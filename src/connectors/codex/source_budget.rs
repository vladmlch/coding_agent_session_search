//! Guard source integrity and contain explicitly typed rollout-size rejection.
//!
//! Each guarded attempt fails immediately on parse, I/O, or callback errors.
//! The recovery driver isolates attempts by discovered file: healthy sources
//! continue after source-local failures, but consumer errors still abort the
//! entire scan. Any incomplete coverage returns an error, keeping watermarks
//! conservative. Diagnostics are bounded and source files are never modified.
//!
//! GH #489: CASS_CODEX_MAX_SOURCE_BYTES explicitly raises the admission budget
//! for modern .jsonl rollouts, up to 1 GiB (default 100 MiB). Legacy .json keeps
//! the published parser's 100 MiB ceiling. This is a source-byte admission
//! limit, not a process RSS limit or a deadline for the upstream primary scan.

mod empty_source;
mod recovery;
mod snapshot;

use std::cell::RefCell;
use std::fmt;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use franken_agent_detection::DiscoveredSourceRole;
use franken_agent_detection::connectors::codex;
use franken_agent_detection::connectors::{SourceCompletion, SourceScanHooks};
use serde::Serialize;

use super::exclusions::ScanExclusions;
use super::{
    Connector, DiscoveredSourceFile, MAX_AUGMENT_ROLLOUT_BYTES, NormalizedConversation, ScanContext,
};
use snapshot::SourceSnapshot;

const MAX_REJECTION_SAMPLES: usize = 32;
const SOURCE_LIMIT_ENV: &str = "CASS_CODEX_MAX_SOURCE_BYTES";
const MAX_CONFIGURED_SOURCE_BYTES: u64 = 1024 * 1024 * 1024;

/// One immutable policy for discovery, every source attempt and both passes.
#[derive(Debug, Clone, Copy)]
struct ScanLimits {
    jsonl_bytes: u64,
}

impl Default for ScanLimits {
    fn default() -> Self {
        Self {
            jsonl_bytes: MAX_AUGMENT_ROLLOUT_BYTES,
        }
    }
}

impl ScanLimits {
    fn invalid() -> anyhow::Error {
        anyhow::anyhow!(
            "{SOURCE_LIMIT_ENV} must be a decimal byte count from 1 through {MAX_CONFIGURED_SOURCE_BYTES}; unset it for the 100 MiB default"
        )
    }

    fn parse(value: &str) -> Result<Self> {
        let value = value.trim();
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(Self::invalid());
        }
        let jsonl_bytes = value.parse::<u64>().map_err(|_| Self::invalid())?;
        if !(1..=MAX_CONFIGURED_SOURCE_BYTES).contains(&jsonl_bytes) {
            return Err(Self::invalid());
        }
        Ok(Self { jsonl_bytes })
    }

    fn from_env() -> Result<Self> {
        match dotenvy::var(SOURCE_LIMIT_ENV) {
            Ok(value) => Self::parse(&value),
            Err(dotenvy::Error::EnvVar(std::env::VarError::NotPresent)) => Ok(Self::default()),
            // Do not echo malformed configuration or treat non-Unicode as unset.
            Err(_) => Err(Self::invalid()),
        }
    }

    fn for_path(self, path: &Path) -> u64 {
        // FAD 0.3.0 selects its streaming parser only for lowercase jsonl.
        // Its other branch uses read_capped; raising that ceiling here would
        // silently certify skipped legacy data as successfully consumed.
        // FAD 0.3.6 streams a compressed .jsonl.zst too, holding its decoded
        // text to this same budget (GH #513).
        if path
            .extension()
            .is_some_and(|extension| extension == "jsonl")
            || codex::is_compressed_rollout(path)
        {
            self.jsonl_bytes
        } else {
            self.jsonl_bytes.min(MAX_AUGMENT_ROLLOUT_BYTES)
        }
    }

    fn incomplete(self) -> IncompleteScan {
        IncompleteScan {
            limit_bytes: self.jsonl_bytes,
            ..IncompleteScan::default()
        }
    }
}

struct ScanAdmission {
    exclusions: ScanExclusions,
    limits: ScanLimits,
}

/// A precise rejection type; never identify a recoverable error by its text or
/// by the broad `InvalidData` kind (which also covers corruption/invalid UTF-8).
#[derive(Debug, thiserror::Error)]
#[error("Codex enrichment source exceeds its admitted read budget ({observed_bytes} bytes)")]
pub(super) struct EnrichmentBudgetExceeded {
    pub(super) observed_bytes: u64,
}

#[derive(Debug, Serialize)]
struct RejectedSource {
    source_path: String,
    observed_bytes: u64,
    /// Present only when this format has a lower limit than the scan policy.
    #[serde(skip_serializing_if = "Option::is_none")]
    limit_bytes: Option<u64>,
}

/// Bounded, source-specific diagnostics carried by the final scan error. Only
/// provenance and byte counts are recorded, never rollout text or JSON lines.
#[derive(Debug, Serialize)]
pub(super) struct IncompleteScan {
    reason: &'static str,
    limit_bytes: u64,
    rejected_source_count: usize,
    omitted_source_count: usize,
    rejected_sources: Vec<RejectedSource>,
}

impl Default for IncompleteScan {
    fn default() -> Self {
        Self {
            reason: "enrichment_read_budget_exceeded",
            limit_bytes: MAX_AUGMENT_ROLLOUT_BYTES,
            rejected_source_count: 0,
            omitted_source_count: 0,
            rejected_sources: Vec::new(),
        }
    }
}

impl fmt::Display for IncompleteScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // JSON escaping keeps unusual source names from injecting log lines.
        write!(
            f,
            "Codex scan incomplete: {}",
            serde_json::to_string(self).map_err(|_| fmt::Error)?
        )
    }
}

impl std::error::Error for IncompleteScan {}

/// The rollouts a Codex scan skipped for exceeding the per-source read budget.
pub(crate) struct OverBudgetSources {
    /// Sampled rejected sources (at most [`MAX_REJECTION_SAMPLES`]).
    pub(crate) sampled_paths: Vec<PathBuf>,
}

/// The error a Codex scan returns when these rollouts exceeded the per-source
/// read budget, for tests of the modules that diagnose it.
#[cfg(test)]
pub(crate) fn over_budget_scan_error(rejected: &[(&str, u64)]) -> anyhow::Error {
    anyhow::Error::from(IncompleteScan {
        rejected_source_count: rejected.len(),
        rejected_sources: rejected
            .iter()
            .map(|&(source_path, observed_bytes)| RejectedSource {
                source_path: source_path.to_string(),
                observed_bytes,
                limit_bytes: None,
            })
            .collect(),
        ..IncompleteScan::default()
    })
}

/// The over-budget rollouts behind a Codex scan error, when that is all it is.
/// A scan that also failed for other reasons keeps its generic diagnostic.
pub(crate) fn over_budget_sources(error: &anyhow::Error) -> Option<OverBudgetSources> {
    let incomplete = error.downcast_ref::<IncompleteScan>()?;
    Some(OverBudgetSources {
        sampled_paths: incomplete
            .rejected_sources
            .iter()
            .map(|source| PathBuf::from(&source.source_path))
            .collect(),
    })
}

/// One offered segment of the pending Codex thread. FAD offers a merged
/// thread's continuations first and its identity last, then emits a single
/// conversation and a single completion for the whole thread. Admission
/// therefore tracks every offered segment, not just the most recent one: a
/// ledger-reused original still belongs to the thread even though the host
/// predicate declined to reparse it.
struct SourceOffer {
    discovered: DiscoveredSourceFile,
    snapshot: Option<SourceSnapshot>,
    admitted: bool,
    over_budget: bool,
    reconciled: bool,
}

#[derive(Default)]
struct ScanState {
    limits: ScanLimits,
    offers: Vec<SourceOffer>,
    preflight_error: Option<anyhow::Error>,
    incomplete: IncompleteScan,
}

impl ScanState {
    fn offer_index(&self, path: &Path) -> Option<usize> {
        self.offers
            .iter()
            .position(|offer| offer.discovered.source_path == path)
    }

    /// Prompt integrity guard: every admitted, not-yet-reconciled segment must
    /// still match its pre-parse observation before more work is admitted.
    fn validate_pending_integrity(&self) -> Result<()> {
        for offer in &self.offers {
            if offer.reconciled {
                continue;
            }
            if let Some(snapshot) = &offer.snapshot {
                snapshot.validate(&offer.discovered.source_path)?;
            }
        }
        Ok(())
    }

    /// Validate an admitted segment that produced no conversation of its own.
    /// A merged thread's segments are reconciled by the thread conversation, so
    /// only a genuinely zero-output source reaches the empty-input check.
    fn reconcile_offer(
        &mut self,
        index: usize,
        progress_tick: Option<&(dyn Fn() + Send + Sync)>,
    ) -> Result<()> {
        if self.offers[index].reconciled {
            return Ok(());
        }
        let path = self.offers[index].discovered.source_path.clone();
        let limit = self.limits.for_path(&path);
        let offer = &mut self.offers[index];
        offer.reconciled = true;
        if !offer.admitted {
            return Ok(());
        }
        let snapshot = offer
            .snapshot
            .as_ref()
            .context("Codex source was not admitted for parsing")?;
        snapshot.validate(&path)?;
        empty_source::validate_with_limit(&path, progress_tick, limit)?;
        snapshot.validate(&path)
    }

    fn mark_thread_reconciled(&mut self, thread_paths: &[PathBuf]) {
        for offer in &mut self.offers {
            if thread_paths.contains(&offer.discovered.source_path) {
                offer.reconciled = true;
            }
        }
    }

    /// A late enrichment budget rejection withdraws the WHOLE thread, never a
    /// fragment: every segment's offer is marked over budget so no segment's
    /// completion can be certified, and the refusal is recorded against the
    /// segment whose bytes were refused. The conversation was already withheld
    /// by the caller; this also stops the completion hook from certifying it.
    fn withdraw_thread(&mut self, thread_paths: &[PathBuf], rejected: &Path, observed_bytes: u64) {
        self.reject(rejected, observed_bytes);
        for offer in &mut self.offers {
            if thread_paths.contains(&offer.discovered.source_path) {
                offer.over_budget = true;
                offer.reconciled = true;
            }
        }
    }

    fn reject(&mut self, source: &Path, observed_bytes: u64) {
        self.incomplete.rejected_source_count =
            self.incomplete.rejected_source_count.saturating_add(1);
        if self.incomplete.rejected_sources.len() < MAX_REJECTION_SAMPLES {
            self.incomplete.rejected_sources.push(RejectedSource {
                source_path: source.to_string_lossy().into_owned(),
                observed_bytes,
                limit_bytes: (self.limits.for_path(source) != self.incomplete.limit_bytes)
                    .then_some(self.limits.for_path(source)),
            });
        } else {
            self.incomplete.omitted_source_count =
                self.incomplete.omitted_source_count.saturating_add(1);
        }
    }
}

#[cfg(test)]
fn observed_over_limit(source: &DiscoveredSourceFile) -> Option<u64> {
    observed_over_limit_with_limit(source, MAX_AUGMENT_ROLLOUT_BYTES)
}

fn observed_over_limit_with_limit(source: &DiscoveredSourceFile, limit: u64) -> Option<u64> {
    let compressed = codex::is_compressed_rollout(&source.source_path);
    if source.provider_slug != "codex"
        || source.role != DiscoveredSourceRole::PrimarySessionLog
        || !(compressed
            || source
                .source_path
                .extension()
                .and_then(|extension| extension.to_str())
                .is_some_and(|extension| {
                    extension.eq_ignore_ascii_case("jsonl")
                        || extension.eq_ignore_ascii_case("json")
                }))
    {
        return None;
    }
    // Refuse before primary parsing; never emit a truncated prefix as complete.
    let metadata = std::fs::metadata(&source.source_path).ok()?;
    if !metadata.is_file() {
        return None;
    }
    let observed = if compressed {
        std::fs::File::open(&source.source_path).map_or(metadata.len(), |file| {
            text_len(&source.source_path, &file, metadata.len())
        })
    } else {
        metadata.len()
    };
    (observed > limit).then_some(observed)
}

/// The length of a rollout's text: its file length or, for a compressed
/// rollout, the decoded length its zstd frame header declares. Codex records
/// that length when it compresses a rollout; one that declares none is sized
/// by its file (FAD still holds its decoded text to the budget).
fn text_len(path: &Path, file: &std::fs::File, file_len: u64) -> u64 {
    if !codex::is_compressed_rollout(path) {
        return file_len;
    }
    let mut source = file;
    codex::compressed_rollout_declared_len(&mut source)
        .ok()
        .flatten()
        .unwrap_or(file_len)
}

pub(super) fn scan(
    inner: &dyn Connector,
    ctx: &ScanContext,
    hooks: &mut SourceScanHooks<'_>,
    on_conversation: &mut dyn FnMut(NormalizedConversation) -> Result<()>,
    enrich: impl FnMut(&mut NormalizedConversation) -> Result<()>,
) -> Result<()> {
    recovery::scan(inner, ctx, hooks, on_conversation, enrich)
}

#[cfg(test)]
fn scan_guarded(
    inner: &dyn Connector,
    ctx: &ScanContext,
    hooks: &mut SourceScanHooks<'_>,
    on_conversation: &mut dyn FnMut(NormalizedConversation) -> Result<()>,
    enrich: impl FnMut(&mut NormalizedConversation) -> Result<()>,
) -> Result<()> {
    let admission = ScanAdmission {
        exclusions: ScanExclusions::from_env(),
        limits: ScanLimits::default(),
    };
    scan_with_admission(inner, ctx, hooks, on_conversation, &admission, enrich)
}

fn scan_with_admission(
    inner: &dyn Connector,
    ctx: &ScanContext,
    hooks: &mut SourceScanHooks<'_>,
    on_conversation: &mut dyn FnMut(NormalizedConversation) -> Result<()>,
    admission: &ScanAdmission,
    mut enrich: impl FnMut(&mut NormalizedConversation) -> Result<()>,
) -> Result<()> {
    let state = RefCell::new(ScanState {
        limits: admission.limits,
        incomplete: admission.limits.incomplete(),
        ..ScanState::default()
    });
    let SourceScanHooks {
        should_scan_source,
        on_source_complete,
    } = hooks;
    let mut should_scan = |source: &DiscoveredSourceFile| {
        {
            let mut state = state.borrow_mut();
            // The upstream predicate cannot return an error. Stop admitting
            // work after a source fails, then return that error from scan().
            if state.preflight_error.is_some() {
                return false;
            }
            if let Err(error) = state.validate_pending_integrity() {
                state.preflight_error = Some(error);
                return false;
            }
        }
        // GH #486: filter before host hooks, budget rejection, or either parse.
        // Explicit-file roots pass through this hook too. An excluded oversized
        // file must not turn an otherwise successful scan into IncompleteScan.
        if admission.exclusions.excludes(&source.source_path) {
            return false;
        }
        // Preserve durable reuse, operator filters, pending batch flushes and
        // cancellation decisions. A ledger-reused segment is still part of its
        // thread: record the offer so the merged conversation can validate and
        // enrich it, but do not parse-admit it.
        if !should_scan_source
            .as_mut()
            .is_none_or(|predicate| predicate(source))
        {
            state.borrow_mut().offers.push(SourceOffer {
                discovered: source.clone(),
                snapshot: None,
                admitted: false,
                over_budget: false,
                reconciled: false,
            });
            return false;
        }
        let limit = admission.limits.for_path(&source.source_path);
        if let Some(size) = observed_over_limit_with_limit(source, limit) {
            let mut state = state.borrow_mut();
            state.reject(&source.source_path, size);
            state.offers.push(SourceOffer {
                discovered: source.clone(),
                snapshot: None,
                admitted: false,
                over_budget: true,
                reconciled: false,
            });
            return false;
        }
        // One observation spans BOTH primary parsing and CASS enrichment.
        // Per-pass checks alone allow a rewrite between the two reads to mix
        // old primary messages with new enrichment in the same conversation.
        match SourceSnapshot::capture_with_limit(&source.source_path, limit) {
            Ok(snapshot) => state.borrow_mut().offers.push(SourceOffer {
                discovered: source.clone(),
                snapshot: Some(snapshot),
                admitted: true,
                over_budget: false,
                reconciled: false,
            }),
            Err(error) => {
                let mut state = state.borrow_mut();
                // Capture rechecks the opened object's size after the cheap
                // path-based preflight, closing the intervening growth race.
                if let Some(rejection) = error.downcast_ref::<EnrichmentBudgetExceeded>() {
                    state.reject(&source.source_path, rejection.observed_bytes);
                    state.offers.push(SourceOffer {
                        discovered: source.clone(),
                        snapshot: None,
                        admitted: false,
                        over_budget: true,
                        reconciled: false,
                    });
                } else {
                    state.preflight_error = Some(error);
                }
                return false;
            }
        }
        true
    };
    let mut complete = |completion: &SourceCompletion| {
        {
            let state = state.borrow();
            let mismatch = || {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Codex conversation or completion does not match its admitted source",
                )
                .into()
            };
            let Some(source_index) = state.offer_index(&completion.source.source_path) else {
                return Err(mismatch());
            };
            // A budget-refused thread is never certified.
            if state.offers[source_index].over_budget {
                return Ok(());
            }
            let mut sidecar_indices = Vec::new();
            for sidecar in &completion.required_sidecars {
                let Some(index) = state.offer_index(&sidecar.source_path) else {
                    return Err(mismatch());
                };
                if state.offers[index].over_budget {
                    return Ok(());
                }
                sidecar_indices.push(index);
            }
            state.offers[source_index]
                .snapshot
                .as_ref()
                .context("Codex source was not admitted for parsing")?
                .validate(&completion.source.source_path)?;
            for (sidecar, index) in completion.required_sidecars.iter().zip(sidecar_indices) {
                state.offers[index]
                    .snapshot
                    .as_ref()
                    .context("Codex source was not admitted for parsing")?
                    .validate(&sidecar.source_path)?;
            }
        }
        on_source_complete
            .as_mut()
            .map_or(Ok(()), |sink| sink(completion))?;
        // The thread is certified; its offers no longer guard a later source.
        state.borrow_mut().offers.clear();
        Ok(())
    };
    let mut forward = |mut conversation: NormalizedConversation| {
        // The merged thread's segment list is authoritative (FAD metadata). A
        // single-file thread has no segments and keeps the strict one-source
        // path. Keying on metadata, never on filename patterns.
        let segment_paths: Option<Vec<PathBuf>> = conversation
            .metadata
            .get("segments")
            .and_then(|value| value.as_array())
            .map(|array| {
                array
                    .iter()
                    .filter_map(|segment| segment.get("path").and_then(|path| path.as_str()))
                    .map(PathBuf::from)
                    .collect()
            });
        let merged = segment_paths
            .as_ref()
            .is_some_and(|paths| !paths.is_empty());
        let thread_paths: Vec<PathBuf> = segment_paths
            .filter(|paths| !paths.is_empty())
            .unwrap_or_else(|| vec![conversation.source_path.clone()]);
        {
            let mut state = state.borrow_mut();
            // Reconcile any earlier zero-output source before delivering this
            // conversation, so a swallowed parse failure still stops the scan.
            for index in 0..state.offers.len() {
                if state.offers[index].reconciled
                    || thread_paths.contains(&state.offers[index].discovered.source_path)
                {
                    continue;
                }
                state.reconcile_offer(index, ctx.progress_tick.as_deref())?;
            }
            // Every segment must have been offered. A budget-refused segment
            // withdraws the whole thread: admitting the rest would publish bytes
            // this guard never observed (no silent truncation).
            let mut refused = false;
            for path in &thread_paths {
                match state.offer_index(path) {
                    None => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Codex thread segment was not offered for admission",
                        )
                        .into());
                    }
                    Some(index) => {
                        if state.offers[index].over_budget {
                            refused = true;
                        }
                        if !merged && !state.offers[index].admitted {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "Codex conversation or completion does not match its admitted source",
                            )
                            .into());
                        }
                    }
                }
            }
            if refused {
                state.mark_thread_reconciled(&thread_paths);
                return Ok(());
            }
            // Capture snapshots for ledger-reused segments lazily. Their
            // pre-parse observation must still match before enrichment reads.
            for path in &thread_paths {
                let index = state.offer_index(path).unwrap();
                if state.offers[index].snapshot.is_some() {
                    continue;
                }
                if state.offers[index].discovered.fs_metadata_changed() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        "Codex source changed across parsing or enrichment; retry this source",
                    )
                    .into());
                }
                let limit = admission.limits.for_path(path);
                match SourceSnapshot::capture_with_limit(path, limit) {
                    Ok(snapshot) => state.offers[index].snapshot = Some(snapshot),
                    Err(error) => {
                        if let Some(rejection) = error.downcast_ref::<EnrichmentBudgetExceeded>() {
                            state.reject(path, rejection.observed_bytes);
                            state.offers[index].over_budget = true;
                            state.mark_thread_reconciled(&thread_paths);
                            return Ok(());
                        }
                        return Err(error);
                    }
                }
            }
            for path in &thread_paths {
                let offer = &state.offers[state.offer_index(path).unwrap()];
                offer
                    .snapshot
                    .as_ref()
                    .context("Codex source was not admitted for parsing")?
                    .validate(path)?;
            }
        }
        // The segment whose enrichment refused the bytes, when a merged thread
        // fails mid-enrichment; a single-file source is its own rejected path.
        let mut rejected_segment: Option<PathBuf> = None;
        let enriched = if merged {
            enrich_merged_thread(
                &mut conversation,
                &thread_paths,
                &state,
                admission,
                ctx.progress_tick.as_deref(),
                &mut rejected_segment,
            )
        } else {
            enrich(&mut conversation).or_else(|error| {
                let state = state.borrow();
                let index = state.offer_index(&conversation.source_path).context(
                    "Codex conversation or completion does not match its admitted source",
                )?;
                let snapshot = state.offers[index]
                    .snapshot
                    .as_ref()
                    .context("Codex source was not admitted for enrichment")?;
                // The existing enrichment entry point refuses >100 MiB BEFORE
                // parsing. Only that precise refusal can use the operator's
                // larger budget, through the retained source handle.
                if error.is::<EnrichmentBudgetExceeded>()
                    && snapshot.len() > MAX_AUGMENT_ROLLOUT_BYTES
                    && admission.limits.for_path(&conversation.source_path)
                        > MAX_AUGMENT_ROLLOUT_BYTES
                {
                    snapshot.validate(&conversation.source_path)?;
                    snapshot.enrich(&mut conversation, ctx.progress_tick.as_deref())
                } else {
                    Err(error)
                }
            })
        };
        if let Err(error) = enriched {
            // The file may cross the cap after preflight. Contain that specific
            // owned enrichment error only when the enclosing source is known;
            // interpose on completion so FAD cannot certify the discarded data.
            if let Some(rejection) = error.downcast_ref::<EnrichmentBudgetExceeded>() {
                let mut state = state.borrow_mut();
                if state.offer_index(&conversation.source_path).is_some() {
                    let rejected =
                        rejected_segment.unwrap_or_else(|| conversation.source_path.clone());
                    state.withdraw_thread(&thread_paths, &rejected, rejection.observed_bytes);
                    return Ok(());
                }
            }
            return Err(error);
        }
        // Enrichment must not reassign the conversation to a foreign source.
        if !thread_paths.contains(&conversation.source_path) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Codex conversation or completion does not match its admitted source",
            )
            .into());
        }
        {
            let state = state.borrow();
            for path in &thread_paths {
                let offer = &state.offers[state.offer_index(path).unwrap()];
                offer
                    .snapshot
                    .as_ref()
                    .context("Codex source was not admitted for parsing")?
                    .validate(path)?;
            }
        }
        // Deliberately outside the enrichment error match: sink/storage errors
        // must abort even when a caller returns the same concrete error type.
        on_conversation(conversation)?;
        let mut state = state.borrow_mut();
        state.mark_thread_reconciled(&thread_paths);
        // A sink can mutate the source too. Delivery cannot be rolled back
        // here, but the scan must fail and must not certify its fingerprint.
        for path in &thread_paths {
            let offer = &state.offers[state.offer_index(path).unwrap()];
            offer
                .snapshot
                .as_ref()
                .context("Codex source was not admitted for parsing")?
                .validate(path)?;
        }
        Ok(())
    };
    let mut guarded = SourceScanHooks {
        should_scan_source: Some(&mut should_scan),
        on_source_complete: Some(&mut complete),
    };
    inner.scan_with_source_boundaries(ctx, &mut guarded, &mut forward)?;
    let mut state = state.into_inner();
    if let Some(error) = state.preflight_error.take() {
        return Err(error);
    }
    for index in 0..state.offers.len() {
        state.reconcile_offer(index, ctx.progress_tick.as_deref())?;
    }
    let incomplete = state.incomplete;
    if incomplete.rejected_source_count > 0 {
        return Err(incomplete.into());
    }
    Ok(())
}

/// Enrich a merged thread by reading EVERY segment's bytes, not only the
/// identity snapshot. Each segment reuses the existing per-file augmentation
/// (compressed/legacy handling and its own observable-snapshot guards); a
/// segment over the default cap but within the operator's raised budget falls
/// back to its retained admitted handle.
fn enrich_merged_thread(
    conversation: &mut NormalizedConversation,
    thread_paths: &[PathBuf],
    state: &RefCell<ScanState>,
    admission: &ScanAdmission,
    progress_tick: Option<&(dyn Fn() + Send + Sync)>,
    rejected_segment: &mut Option<PathBuf>,
) -> Result<()> {
    for path in thread_paths {
        match super::augment_modern_codex_source(conversation, path, progress_tick) {
            Ok(()) => {}
            Err(error) => {
                *rejected_segment = Some(path.clone());
                let state = state.borrow();
                let index = state
                    .offer_index(path)
                    .context("Codex thread segment was not offered for admission")?;
                let snapshot = state.offers[index]
                    .snapshot
                    .as_ref()
                    .context("Codex source was not admitted for enrichment")?;
                if error.is::<EnrichmentBudgetExceeded>()
                    && snapshot.len() > MAX_AUGMENT_ROLLOUT_BYTES
                    && admission.limits.for_path(path) > MAX_AUGMENT_ROLLOUT_BYTES
                {
                    snapshot.validate(path)?;
                    snapshot.enrich_path(conversation, path, progress_tick)?;
                } else {
                    return Err(error);
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod coverage_tests {
    use super::scan_guarded as scan;
    include!("source_budget/coverage_tests.rs");
}
#[cfg(test)]
mod tests {
    use super::scan_guarded as scan;
    include!("source_budget/tests.rs");
}

#[cfg(test)]
mod limit_tests;
