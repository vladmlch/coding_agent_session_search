//! Isolate failed Codex files without hiding incomplete scan coverage.
//!
//! Discovery selects the inventory once. Each file is then parsed in its own
//! explicit scope using the real connector and the same integrity guard. This
//! is not a retry loop: changing/unfinished sources are retried on a later scan.

use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

use franken_agent_detection::ScanRoot;
use franken_agent_detection::connectors::codex::rollout_session_path;
use serde::Serialize;

use super::{
    Connector, DiscoveredSourceFile, DiscoveredSourceRole, IncompleteScan, MAX_REJECTION_SAMPLES,
    NormalizedConversation, RejectedSource, Result, ScanAdmission, ScanContext, ScanExclusions,
    ScanLimits, SourceCompletion, SourceScanHooks, scan_with_admission,
};
use crate::connectors::codex::archives;

#[derive(Debug, Serialize)]
struct FailedSource {
    source_path: String,
    reason: &'static str,
}

#[derive(Debug, Serialize)]
struct FailureReport {
    reason: &'static str,
    failed_source_count: usize,
    omitted_source_count: usize,
    failed_sources: Vec<FailedSource>,
}

impl Default for FailureReport {
    fn default() -> Self {
        Self {
            reason: "source_read_failures",
            failed_source_count: 0,
            omitted_source_count: 0,
            failed_sources: Vec::new(),
        }
    }
}

impl fmt::Display for FailureReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Codex scan incomplete: {}",
            serde_json::to_string(self).map_err(|_| fmt::Error)?
        )
    }
}

#[derive(Default)]
struct Failures {
    report: FailureReport,
    budgets: IncompleteScan,
    first_error: Option<anyhow::Error>,
}

impl Failures {
    fn record(&mut self, source: &DiscoveredSourceFile, error: anyhow::Error) {
        let reason = if let Some(budget) = error.downcast_ref::<IncompleteScan>() {
            self.budgets.rejected_source_count = self
                .budgets
                .rejected_source_count
                .saturating_add(budget.rejected_source_count);
            let available = MAX_REJECTION_SAMPLES - self.budgets.rejected_sources.len();
            let configured_limit = self.budgets.limit_bytes;
            self.budgets.rejected_sources.extend(
                budget
                    .rejected_sources
                    .iter()
                    .take(available)
                    .map(|sample| RejectedSource {
                        source_path: sample.source_path.clone(),
                        observed_bytes: sample.observed_bytes,
                        limit_bytes: {
                            let actual = sample.limit_bytes.unwrap_or(budget.limit_bytes);
                            (actual != configured_limit).then_some(actual)
                        },
                    }),
            );
            self.budgets.omitted_source_count = self
                .budgets
                .rejected_source_count
                .saturating_sub(self.budgets.rejected_sources.len());
            "read_budget_exceeded"
        } else {
            let reason = match error.downcast_ref::<io::Error>().map(io::Error::kind) {
                Some(io::ErrorKind::UnexpectedEof) => "unfinished_source",
                Some(io::ErrorKind::Interrupted) => "source_changed",
                Some(io::ErrorKind::NotFound) => "source_missing",
                Some(io::ErrorKind::PermissionDenied) => "source_unreadable",
                Some(io::ErrorKind::InvalidData) => "invalid_source_data",
                // A compressed rollout whose decoded text passed the budget
                // without declaring its length up front (GH #513).
                Some(io::ErrorKind::FileTooLarge) => "read_budget_exceeded",
                _ => "source_read_failed",
            };
            // Retain one original error for typed downcasts and its cause chain.
            // The bounded summary never contains session bodies or parser text.
            if self.first_error.is_none() {
                self.first_error = Some(error);
            }
            reason
        };
        self.report.failed_source_count = self.report.failed_source_count.saturating_add(1);
        if self.report.failed_sources.len() < MAX_REJECTION_SAMPLES {
            self.report.failed_sources.push(FailedSource {
                source_path: source.source_path.to_string_lossy().into_owned(),
                reason,
            });
        } else {
            self.report.omitted_source_count = self.report.omitted_source_count.saturating_add(1);
        }
    }

    fn finish(self) -> Result<()> {
        if let Some(error) = self.first_error {
            return Err(error.context(self.report));
        }
        // Preserve the existing budget-only diagnostic schema and error type.
        if self.budgets.rejected_source_count > 0 {
            return Err(self.budgets.into());
        }
        Ok(())
    }
}

fn scoped_context(ctx: &ScanContext, source: &DiscoveredSourceFile) -> ScanContext {
    let root = ctx
        .scan_roots
        .iter()
        .find(|root| {
            root.origin == source.origin
                && (source.scan_root.starts_with(&root.path)
                    || root.path.starts_with(&source.scan_root))
        })
        .map_or_else(
            || {
                ScanRoot::remote(
                    source.source_path.clone(),
                    source.origin.clone(),
                    source.platform,
                )
            },
            |root| root.with_path(source.source_path.clone()),
        );
    // Discovery already applied the incremental cutoff. Applying it twice
    // could silently skip a selected file after its metadata changes.
    let mut scoped = ScanContext::with_roots(ctx.data_dir.clone(), vec![root], None);
    scoped.progress_tick.clone_from(&ctx.progress_tick);
    // Preserve the parent's local-default stance (FAD 0.3.11's hermeticity
    // flag, issue #174) — the codex connector never consults it, but a
    // scoped recovery context must not silently flip the scan's semantics.
    scoped.allow_local_default_stores = ctx.allow_local_default_stores;
    scoped
}

fn original_identity(
    mut observed: DiscoveredSourceFile,
    inventory: &DiscoveredSourceFile,
) -> DiscoveredSourceFile {
    // Explicit-file scanning is an implementation detail. Durable ledgers must
    // see the original discovery identity, with fresh pre-parse size/mtime.
    observed.scan_root.clone_from(&inventory.scan_root);
    observed.origin.clone_from(&inventory.origin);
    observed.platform = inventory.platform;
    observed
}

fn directory_session_id(source: &DiscoveredSourceFile) -> Option<String> {
    // FAD's directory scan derives the ID relative to root/sessions when that
    // is the selected subtree, otherwise relative to the root itself. Its
    // explicit-file API instead finds the nearest sessions ancestor. Preserve
    // the original directory contract for dated subroots and custom mirrors;
    // changing the scope for recovery must not create duplicate stored sessions.
    source
        .source_path
        .strip_prefix(source.scan_root.join("sessions"))
        .or_else(|_| source.source_path.strip_prefix(&source.scan_root))
        .ok()
        .and_then(|relative| rollout_session_path(relative).to_str().map(str::to_owned))
        .or_else(|| {
            rollout_session_path(Path::new(source.source_path.file_name()?))
                .to_str()
                .map(str::to_owned)
        })
}

#[allow(clippy::too_many_arguments)]
fn scan_source(
    inner: &dyn Connector,
    ctx: &ScanContext,
    source: &DiscoveredSourceFile,
    hooks: &mut SourceScanHooks<'_>,
    on_conversation: &mut dyn FnMut(NormalizedConversation) -> Result<()>,
    admission: &ScanAdmission,
    enrich: &mut dyn FnMut(&mut NormalizedConversation) -> Result<()>,
    inventory: &HashMap<PathBuf, DiscoveredSourceFile>,
    covered: &mut HashSet<PathBuf>,
) -> (Result<()>, bool) {
    let consumer_failed = Cell::new(false);
    let visited = Cell::new(false);
    let scope_changed = Cell::new(false);
    let SourceScanHooks {
        should_scan_source,
        on_source_complete,
    } = hooks;
    let mut before = |observed: &DiscoveredSourceFile| {
        if observed.source_path != source.source_path {
            // FAD expands a scoped segment to its whole thread. Thread siblings
            // are discovered inventory; anything else (e.g. a file that became a
            // directory hiding undiscovered children) is a scope change.
            if !inventory.contains_key(&observed.source_path) {
                scope_changed.set(true);
                return false;
            }
        } else {
            visited.set(true);
        }
        let identity = inventory.get(&observed.source_path).unwrap_or(source);
        let observed = original_identity(observed.clone(), identity);
        should_scan_source
            .as_mut()
            .is_none_or(|predicate| predicate(&observed))
    };
    let mut complete = |done: &SourceCompletion| {
        let source_identity = inventory.get(&done.source.source_path).unwrap_or(source);
        let done = SourceCompletion {
            source: original_identity(done.source.clone(), source_identity),
            required_sidecars: done
                .required_sidecars
                .iter()
                .map(|sidecar| {
                    let identity = inventory.get(&sidecar.source_path).unwrap_or(source);
                    original_identity(sidecar.clone(), identity)
                })
                .collect(),
            conversations_emitted: done.conversations_emitted,
        };
        on_source_complete
            .as_mut()
            .map_or(Ok(()), |sink| sink(&done))
            .inspect_err(|_| consumer_failed.set(true))
    };
    let mut deliver = |mut conversation: NormalizedConversation| {
        // Bind the directory identity to the conversation's identity segment (the
        // merged thread's original), not to the scoped continuation.
        let identity = inventory.get(&conversation.source_path).unwrap_or(source);
        let directory_id = archives::session_id(ctx, identity).map(Some).or_else(|| {
            (identity.scan_root != identity.source_path).then(|| directory_session_id(identity))
        });
        if let Some(id) = &directory_id {
            conversation.external_id.clone_from(id);
        }
        covered.insert(conversation.source_path.clone());
        if let Some(segments) = conversation
            .metadata
            .get("segments")
            .and_then(|value| value.as_array())
        {
            for segment in segments {
                if let Some(path) = segment.get("path").and_then(|path| path.as_str()) {
                    covered.insert(PathBuf::from(path));
                }
            }
        }
        on_conversation(conversation).inspect_err(|_| consumer_failed.set(true))
    };
    let mut guarded = SourceScanHooks {
        should_scan_source: Some(&mut before),
        on_source_complete: Some(&mut complete),
    };
    let result = scan_with_admission(
        inner,
        &scoped_context(ctx, source),
        &mut guarded,
        &mut deliver,
        admission,
        enrich,
    )
    .and_then(|()| {
        if scope_changed.get() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "Codex source scope changed after discovery; retry this source",
            )
            .into());
        }
        if !visited.get() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Codex source disappeared after discovery; retry this source",
            )
            .into());
        }
        Ok(())
    });
    (result, consumer_failed.get())
}

pub(super) fn scan(
    inner: &dyn Connector,
    ctx: &ScanContext,
    hooks: &mut SourceScanHooks<'_>,
    on_conversation: &mut dyn FnMut(NormalizedConversation) -> Result<()>,
    mut enrich: impl FnMut(&mut NormalizedConversation) -> Result<()>,
) -> Result<()> {
    anyhow::ensure!(
        inner.supports_source_boundaries(),
        "Codex recovery requires source boundaries"
    );
    // Resolve once, before discovery or callbacks can perform any work.
    let limits = ScanLimits::from_env()?;
    // Since franken-agent-detection 0.3.1 the primary rollout reader applies
    // its own admission budget (100 MiB by default) and refused every larger
    // rollout as invalid data after this policy had admitted it. Hand it the
    // same policy.
    franken_agent_detection::connectors::codex::set_codex_rollout_byte_budget(limits.jsonl_bytes);
    let admission = ScanAdmission {
        exclusions: ScanExclusions::from_env(),
        limits,
    };
    let (sources, discovery_result) = archives::discover_for_scan(ctx)?.into_parts();
    let inventory: HashMap<PathBuf, DiscoveredSourceFile> = sources
        .iter()
        .map(|source| (source.source_path.clone(), source.clone()))
        .collect();
    let mut covered: HashSet<PathBuf> = HashSet::new();
    let mut failures = Failures {
        budgets: limits.incomplete(),
        ..Failures::default()
    };
    for source in sources {
        // A merged thread is indexed once, under its identity; its other segments
        // are covered by that delivery and must not be re-attempted.
        if covered.contains(&source.source_path) {
            continue;
        }
        if admission.exclusions.excludes(&source.source_path) {
            continue;
        }
        anyhow::ensure!(
            source.provider_slug == "codex"
                && source.role == DiscoveredSourceRole::PrimarySessionLog,
            "Codex recovery requires independent rollout sources"
        );
        let (result, consumer_failed) = scan_source(
            inner,
            ctx,
            &source,
            hooks,
            on_conversation,
            &admission,
            &mut enrich,
            &inventory,
            &mut covered,
        );
        if let Err(error) = result {
            // Classify by where the error originated, never its type or text:
            // a storage/cancellation callback can return the same I/O error as
            // an unfinished file and must still abort immediately.
            if consumer_failed {
                return Err(error);
            }
            failures.record(&source, error);
        }
    }
    match (discovery_result, failures.finish()) {
        (Err(discovery_error), Err(source_error)) => {
            // Retain discovery as the typed cause; this mixed failure must
            // never be mistaken for a budget-only refusal by its caller.
            Err(discovery_error.context(source_error))
        }
        (Err(error), Ok(())) => Err(error),
        (Ok(()), result) => result,
    }
}

#[cfg(test)]
mod contract_tests;
#[cfg(test)]
mod resume_tests;
#[cfg(test)]
mod tests;
