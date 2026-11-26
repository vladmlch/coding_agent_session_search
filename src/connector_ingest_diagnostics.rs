//! Provider-specific connector-ingest diagnostic taxonomy.
//!
//! Bead: coding_agent_session_search-cass-fleet-resilience-20260608-uojcg.15.1
//! ("Harden connector ingest diagnostics and per-provider failure fixtures").
//!
//! Connectors are the front door for 20 local agents, so a connector failure
//! becomes lost, merged, duplicated, or misleading search history. The report
//! named concrete cases: an Aider external-id collision that merges distinct
//! project histories, malformed JSONL that could panic/abort a whole file, a
//! Cursor `state.vscdb` locked by the live app, ChatGPT encrypted stores that
//! are undecipherable on Linux/headless hosts, and Amp filename-prefix
//! assumptions that break on non-ASCII / future schemes.
//!
//! The failure these share is *silence*: a provider-specific limitation gets
//! turned into a fake zero-message import, or two unrelated conversations get
//! merged under one id. This module makes the failure **explicit,
//! provider-specific, retry-aware, and testable**: an [`IngestFailureKind`]
//! classifies what went wrong, [`classify`] turns it into a
//! [`ConnectorIngestDiagnostic`] carrying provider/source/provenance/line/id
//! context plus severity, retryability, the per-source
//! [`SourceIngestDisposition`], and a safe next action, and
//! [`ProviderIngestSummary`] distinguishes discovered / skipped / quarantined /
//! partially-indexed / locked / unsupported-encrypted / successfully-indexed
//! sources instead of collapsing them into one count.
//!
//! It also carries the one concrete behavioural fix that is pure logic:
//! [`disambiguated_external_id`] folds workspace/source identity into a
//! connector external id so two distinct project histories can never collide
//! and merge.
//!
//! The classification helpers remain pure. [`ConnectorIngestRun`] wires them to
//! live discovery and parsing, applies the Aider id hardening before persistence,
//! and records privacy-safe hashes for malformed lines while the surrounding
//! valid records continue through normal ingest.

use std::collections::BTreeMap;
use std::io::BufRead;
use std::path::{Path, PathBuf};

use franken_agent_detection::connectors::sqlite_sync::{
    ConnectionExt as FadConnectionExt, SqliteError as FadSqliteError, open_external_strict_in_place,
};
use serde::{Deserialize, Serialize};

use crate::connectors::{DiscoveredSourceFile, NormalizedConversation, ScanContext};

/// Stable schema version for the connector-ingest diagnostic wire format.
pub const CONNECTOR_INGEST_DIAGNOSTIC_SCHEMA_VERSION: u32 = 1;

/// How serious a single ingest finding is. Ordered low→high so a per-source or
/// per-run rollup can take the worst with `max`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IngestSeverity {
    /// Informational; ingestion proceeded normally.
    Info,
    /// A limitation or a quarantined record; the rest of the source still
    /// indexed. Not a hard failure.
    Warning,
    /// The source could not be ingested at all (and it is not merely an absent
    /// or encrypted-but-known limitation).
    Error,
}

impl IngestSeverity {
    /// Stable kebab-case wire value.
    pub const fn as_str(self) -> &'static str {
        match self {
            IngestSeverity::Info => "info",
            IngestSeverity::Warning => "warning",
            IngestSeverity::Error => "error",
        }
    }
}

/// What specifically went wrong while ingesting a connector source. Each kind is
/// the report's concrete connector failure mode, generalised across providers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IngestFailureKind {
    /// A connector external id would collide across distinct workspaces/sources
    /// and merge unrelated histories (the Aider case) unless disambiguated.
    ExternalIdCollisionRisk,
    /// A single JSONL line/record was malformed; valid records around it are
    /// preserved and this one is quarantined (never a whole-file abort/panic).
    MalformedJsonLine,
    /// A session file was truncated mid-stream; records up to the truncation are
    /// preserved.
    TruncatedSession,
    /// The source is locked by the live app (Cursor `state.vscdb`); retryable.
    SourceLocked,
    /// The source is encrypted and cannot be decrypted on this host
    /// (ChatGPT v2/v3 on Linux/headless); legacy unencrypted data still indexes.
    EncryptedUnsupported,
    /// A required keychain/credential to decrypt is unavailable (headless host).
    KeychainUnavailable,
    /// A filename/path assumption did not hold (Amp prefix scheme, non-ASCII, or
    /// a future-looking name); the source is skipped rather than mis-parsed.
    FilenameAssumptionViolated,
    /// The source path exists but could not be read (permissions / I/O).
    UnreadableSource,
    /// The connector failed on content it could not interpret (an unsupported
    /// record shape or value); no I/O error caused it, so permissions and
    /// retries do not help (GH #511).
    UnparseableSource,
    /// The source is larger than the connector's per-source read budget, so it
    /// was not indexed; raising the budget admits it on the next run.
    SourceOverReadBudget,
    /// The store uses a layout or format version this build's connector does
    /// not read (a newer Pi durable schema, a Pi Session Store V2 sidecar).
    /// It was not indexed from that store, and the store is left untouched.
    UnsupportedStoreFormat,
}

impl IngestFailureKind {
    /// Stable kebab-case wire value.
    pub const fn as_str(self) -> &'static str {
        match self {
            IngestFailureKind::ExternalIdCollisionRisk => "external-id-collision-risk",
            IngestFailureKind::MalformedJsonLine => "malformed-json-line",
            IngestFailureKind::TruncatedSession => "truncated-session",
            IngestFailureKind::SourceLocked => "source-locked",
            IngestFailureKind::EncryptedUnsupported => "encrypted-unsupported",
            IngestFailureKind::KeychainUnavailable => "keychain-unavailable",
            IngestFailureKind::FilenameAssumptionViolated => "filename-assumption-violated",
            IngestFailureKind::UnreadableSource => "unreadable-source",
            IngestFailureKind::UnparseableSource => "unparseable-source",
            IngestFailureKind::SourceOverReadBudget => "source-over-read-budget",
            IngestFailureKind::UnsupportedStoreFormat => "unsupported-store-format",
        }
    }
}

/// The per-source outcome an ingest run reports, so a limitation is never
/// silently rendered as "indexed 0 messages". Ordered roughly best→worst.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SourceIngestDisposition {
    /// Fully ingested; every record indexed.
    Indexed,
    /// Discovered but not yet processed (enumerated, pending).
    Discovered,
    /// Some records indexed, others quarantined/truncated.
    PartiallyIndexed,
    /// One or more records quarantined; the source itself is otherwise indexed.
    Quarantined,
    /// Skipped (filename/path assumption violated, or deliberately excluded).
    Skipped,
    /// Locked by the live app; retry later (no data indexed this run).
    Locked,
    /// Encrypted and unsupported on this host (a known, explicit limitation —
    /// never a fake zero-message success).
    UnsupportedEncrypted,
}

impl SourceIngestDisposition {
    /// Stable kebab-case wire value.
    pub const fn as_str(self) -> &'static str {
        match self {
            SourceIngestDisposition::Indexed => "indexed",
            SourceIngestDisposition::Discovered => "discovered",
            SourceIngestDisposition::PartiallyIndexed => "partially-indexed",
            SourceIngestDisposition::Quarantined => "quarantined",
            SourceIngestDisposition::Skipped => "skipped",
            SourceIngestDisposition::Locked => "locked",
            SourceIngestDisposition::UnsupportedEncrypted => "unsupported-encrypted",
        }
    }

    /// Whether this disposition reflects an explicit, non-silent limitation
    /// (locked / unsupported-encrypted / skipped) — the thing that must never be
    /// reported as a successful zero-message import.
    pub const fn is_explicit_limitation(self) -> bool {
        matches!(
            self,
            SourceIngestDisposition::Locked
                | SourceIngestDisposition::UnsupportedEncrypted
                | SourceIngestDisposition::Skipped
        )
    }
}

/// A single connector-ingest diagnostic. Stable snake_case JSON with an embedded
/// [`schema_version`](Self::schema_version).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorIngestDiagnostic {
    /// Mirrors [`CONNECTOR_INGEST_DIAGNOSTIC_SCHEMA_VERSION`].
    pub schema_version: u32,
    /// The provider/connector (e.g. `"aider"`, `"cursor"`, `"chatgpt"`).
    pub provider: String,
    /// The source path or root the finding came from.
    pub source_path: String,
    /// Workspace / source provenance identity, when known (distinguishes the
    /// same base id across projects/machines).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
    /// Line/row identifier when the finding is record-scoped (e.g. a malformed
    /// JSONL line number).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line_or_row: Option<u64>,
    /// The connector external id in context, when relevant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_id: Option<String>,
    /// The canonical (post-disambiguation) id, when computed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_id: Option<String>,
    /// What went wrong.
    pub failure_kind: IngestFailureKind,
    /// The connector's own explanation, when it gives one (e.g. the format
    /// version it found and the one it reads). Never message content.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// How serious it is.
    pub severity: IngestSeverity,
    /// Whether retrying later may succeed (e.g. a transient lock).
    pub retryable: bool,
    /// The per-source disposition this finding implies.
    pub disposition: SourceIngestDisposition,
    /// A single, safe next action for an operator/agent. Always read-only or a
    /// safe re-run; never a destructive cleanup.
    pub safe_next_action: String,
}

/// The classification of a failure kind for a provider: severity, retryability,
/// the disposition it implies, and the safe next action. Pure.
fn classify_kind(
    provider: &str,
    kind: IngestFailureKind,
) -> (IngestSeverity, bool, SourceIngestDisposition, String) {
    match kind {
        IngestFailureKind::ExternalIdCollisionRisk => (
            IngestSeverity::Warning,
            false,
            SourceIngestDisposition::Indexed,
            format!(
                "{provider} external ids are disambiguated by workspace/source identity so distinct \
                 project histories cannot merge; verify the canonical id includes provenance"
            ),
        ),
        IngestFailureKind::MalformedJsonLine => (
            IngestSeverity::Warning,
            false,
            SourceIngestDisposition::PartiallyIndexed,
            "the malformed line is quarantined and the surrounding valid records are indexed; \
             inspect the quarantined line — the whole file is never aborted"
                .to_string(),
        ),
        IngestFailureKind::TruncatedSession => (
            IngestSeverity::Warning,
            false,
            SourceIngestDisposition::PartiallyIndexed,
            "the session was truncated; records up to the truncation are indexed — re-export the \
             session if the tail is needed"
                .to_string(),
        ),
        IngestFailureKind::SourceLocked => (
            IngestSeverity::Warning,
            true, // retryable: the lock is transient
            SourceIngestDisposition::Locked,
            format!(
                "{provider} source is locked by the live app (e.g. an open Cursor); this is \
                 reported as lock-busy, NOT a zero-message import — retry after closing the app or \
                 on the next sync"
            ),
        ),
        IngestFailureKind::EncryptedUnsupported => (
            IngestSeverity::Warning,
            false,
            SourceIngestDisposition::UnsupportedEncrypted,
            format!(
                "{provider} store is encrypted and unsupported on this host; legacy unencrypted \
                 conversations still index — this is an explicit limitation, never a fake success"
            ),
        ),
        IngestFailureKind::KeychainUnavailable => (
            IngestSeverity::Warning,
            true, // a keychain may become available on a non-headless run
            SourceIngestDisposition::UnsupportedEncrypted,
            format!(
                "{provider} decryption needs a keychain/credential unavailable on this headless \
                 host; index from an interactive session where the keychain is unlocked"
            ),
        ),
        IngestFailureKind::FilenameAssumptionViolated => (
            IngestSeverity::Warning,
            false,
            SourceIngestDisposition::Skipped,
            format!(
                "{provider} filename did not match the expected scheme (non-ASCII or a future \
                 naming variant); skipped rather than mis-parsed — file a fixture for the variant"
            ),
        ),
        IngestFailureKind::UnreadableSource => (
            IngestSeverity::Error,
            true, // permissions/IO may be fixed and retried
            SourceIngestDisposition::Skipped,
            format!("{provider} source path could not be read; check permissions, then re-index"),
        ),
        IngestFailureKind::UnparseableSource => (
            IngestSeverity::Error,
            false, // the same bytes fail the same way
            SourceIngestDisposition::Skipped,
            format!(
                "{provider} could not interpret a source's content (not a permissions or I/O \
                 problem); indexing_stats.connectors[].error names the record. Report its shape \
                 so the connector can parse it, then re-index"
            ),
        ),
        IngestFailureKind::SourceOverReadBudget => (
            IngestSeverity::Error,
            true, // a larger budget admits it on the next run
            SourceIngestDisposition::Skipped,
            format!(
                "{provider} session file is larger than the per-source read budget and was not \
                 indexed; raise the budget (Codex: CASS_CODEX_MAX_SOURCE_BYTES, up to 1073741824 \
                 bytes), then re-index"
            ),
        ),
        IngestFailureKind::UnsupportedStoreFormat => (
            IngestSeverity::Warning,
            false, // the same build reads the same store the same way
            SourceIngestDisposition::Skipped,
            format!(
                "{provider} found a store in a format or location this cass build does not \
                 read; nothing was indexed from that store and it was not modified. detail names \
                 the format and what admits it (a cass release whose connector reads it, or \
                 indexing on the store's own host), then re-index"
            ),
        ),
    }
}

/// Build a full [`ConnectorIngestDiagnostic`] for a provider failure. Pure.
pub fn classify(
    provider: &str,
    source_path: &str,
    kind: IngestFailureKind,
) -> ConnectorIngestDiagnostic {
    let (severity, retryable, disposition, safe_next_action) = classify_kind(provider, kind);
    ConnectorIngestDiagnostic {
        schema_version: CONNECTOR_INGEST_DIAGNOSTIC_SCHEMA_VERSION,
        provider: provider.to_string(),
        source_path: source_path.to_string(),
        workspace: None,
        line_or_row: None,
        external_id: None,
        canonical_id: None,
        failure_kind: kind,
        detail: None,
        severity,
        retryable,
        disposition,
        safe_next_action,
    }
}

fn classify_path(
    provider: &str,
    source_path: &Path,
    kind: IngestFailureKind,
) -> ConnectorIngestDiagnostic {
    classify(provider, source_path.to_string_lossy().as_ref(), kind)
}

/// Recognize backend error messages, not arbitrary occurrences of lock words
/// in source paths or parser context. Typed causes take precedence below.
fn is_database_lock_message(message: &str) -> bool {
    let lower = message.trim().to_ascii_lowercase();
    [
        "database is locked",
        "database is busy",
        "database table is locked",
        "database schema is locked",
        "sqlite_busy",
        "sqlite_busy_recovery",
        "sqlite_busy_snapshot",
        "sqlite_busy_timeout",
        "sqlite_locked",
        "sqlite_locked_sharedcache",
        "sqlite_locked_vtab",
    ]
    .iter()
    .any(|&prefix| {
        lower.strip_prefix(prefix).is_some_and(|suffix| {
            let suffix = suffix.trim_start();
            suffix.is_empty() || suffix.starts_with(':') || suffix.starts_with('(')
        })
    })
}

fn is_rusqlite_lock_error(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code: rusqlite::ffi::ErrorCode::DatabaseBusy
                    | rusqlite::ffi::ErrorCode::DatabaseLocked
                    | rusqlite::ffi::ErrorCode::FileLockingProtocolFailed,
                ..
            },
            _,
        )
    )
}

fn connector_scan_failure_kind(error: &anyhow::Error) -> IngestFailureKind {
    use crate::franken_sync::FrankenError;

    if let Some(io) = error.chain().find_map(|cause| {
        cause.downcast_ref::<std::io::Error>().or_else(|| {
            cause
                .downcast_ref::<FadSqliteError>()
                .and_then(|sqlite_err| match sqlite_err {
                    FadSqliteError::Io(io) => Some(io),
                    _ => None,
                })
        })
    }) {
        return if matches!(
            io.kind(),
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::ResourceBusy
        ) {
            IngestFailureKind::SourceLocked
        } else {
            IngestFailureKind::UnreadableSource
        };
    }
    // A parser may quote source content that itself names a lock. Its type
    // proves this is still a content failure, regardless of that message.
    if error
        .chain()
        .any(|cause| cause.downcast_ref::<serde_json::Error>().is_some())
    {
        return IngestFailureKind::UnparseableSource;
    }
    let database_locked = error.chain().any(|cause| {
        cause.downcast_ref::<FrankenError>().is_some_and(|error| {
            matches!(
                error,
                FrankenError::Busy
                    | FrankenError::BusyRecovery
                    | FrankenError::BusySnapshot { .. }
                    | FrankenError::DatabaseLocked { .. }
                    | FrankenError::LockFailed { .. }
                    | FrankenError::WriteConflict { .. }
                    | FrankenError::SerializationFailure { .. }
            )
        }) || cause.downcast_ref::<FadSqliteError>().is_some_and(|error| {
            matches!(error, FadSqliteError::BusyRecovery)
                || matches!(error, FadSqliteError::Sqlite(inner) if is_rusqlite_lock_error(inner))
        }) || cause
            .downcast_ref::<rusqlite::Error>()
            .is_some_and(is_rusqlite_lock_error)
    });
    if database_locked || is_database_lock_message(&error.root_cause().to_string()) {
        IngestFailureKind::SourceLocked
    } else {
        IngestFailureKind::UnparseableSource
    }
}

impl ConnectorIngestDiagnostic {
    /// Attach workspace/source provenance.
    pub fn with_workspace(mut self, workspace: impl Into<String>) -> Self {
        self.workspace = Some(workspace.into());
        self
    }

    /// Attach a record line/row identifier.
    pub fn with_line(mut self, line: u64) -> Self {
        self.line_or_row = Some(line);
        self
    }

    /// Attach the external id and its disambiguated canonical id.
    pub fn with_ids(
        mut self,
        external_id: impl Into<String>,
        canonical_id: impl Into<String>,
    ) -> Self {
        self.external_id = Some(external_id.into());
        self.canonical_id = Some(canonical_id.into());
        self
    }

    /// Attach the connector's own explanation.
    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }
}

/// Fold workspace/source identity into a connector external id so that two
/// distinct project histories sharing the same base id can never collide and
/// merge (the Aider external-id collision). Deterministic: the same
/// `(base_id, workspace)` always yields the same canonical id, and different
/// workspaces always diverge. An empty workspace returns the base id unchanged
/// (nothing to disambiguate by).
pub fn disambiguated_external_id(base_id: &str, workspace: &str) -> String {
    if workspace.is_empty() {
        return base_id.to_string();
    }
    let digest = blake3::hash(workspace.as_bytes());
    let short: String = digest.to_hex().to_string().chars().take(12).collect();
    format!("{base_id}@{short}")
}

/// A per-provider rollup of source dispositions for a run. Replaces a single
/// "indexed N" count with the full breakdown so a locked/encrypted source is
/// never invisible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ProviderIngestSummary {
    pub discovered: u64,
    pub indexed: u64,
    pub partially_indexed: u64,
    pub quarantined: u64,
    pub skipped: u64,
    pub locked: u64,
    pub unsupported_encrypted: u64,
}

impl ProviderIngestSummary {
    /// Record one source's disposition.
    pub fn record(&mut self, disposition: SourceIngestDisposition) {
        match disposition {
            SourceIngestDisposition::Discovered => self.discovered += 1,
            SourceIngestDisposition::Indexed => self.indexed += 1,
            SourceIngestDisposition::PartiallyIndexed => self.partially_indexed += 1,
            SourceIngestDisposition::Quarantined => self.quarantined += 1,
            SourceIngestDisposition::Skipped => self.skipped += 1,
            SourceIngestDisposition::Locked => self.locked += 1,
            SourceIngestDisposition::UnsupportedEncrypted => self.unsupported_encrypted += 1,
        }
    }

    /// Total sources accounted for.
    pub fn total(&self) -> u64 {
        self.discovered
            + self.indexed
            + self.partially_indexed
            + self.quarantined
            + self.skipped
            + self.locked
            + self.unsupported_encrypted
    }

    /// Sources that produced at least some searchable content (indexed or
    /// partially indexed). Used to assert a run is not a silent total miss.
    pub fn with_content(&self) -> u64 {
        self.indexed + self.partially_indexed
    }

    /// Merge another scan scope for the same provider into this run rollup.
    pub fn merge(&mut self, other: Self) {
        self.discovered = self.discovered.saturating_add(other.discovered);
        self.indexed = self.indexed.saturating_add(other.indexed);
        self.partially_indexed = self
            .partially_indexed
            .saturating_add(other.partially_indexed);
        self.quarantined = self.quarantined.saturating_add(other.quarantined);
        self.skipped = self.skipped.saturating_add(other.skipped);
        self.locked = self.locked.saturating_add(other.locked);
        self.unsupported_encrypted = self
            .unsupported_encrypted
            .saturating_add(other.unsupported_encrypted);
    }
}

/// One completed provider scan scope, ready to merge into `IndexingStats`.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ConnectorIngestReport {
    pub provider: String,
    pub summary: ProviderIngestSummary,
    pub diagnostics: Vec<ConnectorIngestDiagnostic>,
}

#[derive(Debug, Clone, Copy)]
struct ObservedSource {
    disposition: SourceIngestDisposition,
    malformed: bool,
}

/// Mutable observation of one connector scan scope. It consumes the same
/// discovery results used by raw-mirror capture, avoiding a second filesystem
/// walk, and contains no conversation payloads.
pub struct ConnectorIngestRun {
    provider: String,
    sources: BTreeMap<PathBuf, ObservedSource>,
    diagnostics: Vec<ConnectorIngestDiagnostic>,
}

impl ConnectorIngestRun {
    /// Inspect discovered sources for failure modes that upstream connectors
    /// otherwise log-and-skip: malformed JSONL, locked Cursor databases, and
    /// encrypted ChatGPT directories unavailable to this scan.
    pub fn begin(
        provider: &str,
        data_dir: &Path,
        ctx: &ScanContext,
        sources: &[DiscoveredSourceFile],
    ) -> Self {
        let mut run = Self {
            provider: provider.to_string(),
            sources: sources
                .iter()
                .map(|source| {
                    (
                        source.source_path.clone(),
                        ObservedSource {
                            disposition: SourceIngestDisposition::Discovered,
                            malformed: false,
                        },
                    )
                })
                .collect(),
            diagnostics: Vec::new(),
        };
        for source in sources {
            run.inspect_jsonl(data_dir, &source.source_path);
            if provider == "cursor" {
                run.inspect_cursor_database(&source.source_path);
            }
        }
        if provider == "chatgpt" {
            run.inspect_chatgpt_encrypted_directories(ctx, sources);
        }
        run
    }

    /// Sources the scan skipped because the source ledger shows them
    /// unchanged since they were indexed. They count as indexed, and
    /// [`Self::finish`] does not re-read them.
    pub fn observe_reused_sources(&mut self, paths: impl IntoIterator<Item = PathBuf>) {
        for path in paths {
            // Self-contained connectors skip the redundant discovery
            // inventory, so a reused source may not be listed yet.
            let source = self.sources.entry(path).or_insert(ObservedSource {
                disposition: SourceIngestDisposition::Discovered,
                malformed: false,
            });
            if source.disposition == SourceIngestDisposition::Discovered {
                source.disposition = SourceIngestDisposition::Indexed;
            }
        }
    }

    /// Record and harden one successfully parsed conversation.
    pub fn observe_conversation(&mut self, conversation: &mut NormalizedConversation) {
        let state = self
            .sources
            .entry(conversation.source_path.clone())
            .or_insert(ObservedSource {
                disposition: SourceIngestDisposition::Discovered,
                malformed: false,
            });
        state.disposition = if state.malformed {
            SourceIngestDisposition::PartiallyIndexed
        } else {
            SourceIngestDisposition::Indexed
        };

        if self.provider == "aider"
            && let (Some(base_id), Some(workspace)) = (
                conversation.external_id.clone(),
                conversation
                    .workspace
                    .as_deref()
                    .or_else(|| conversation.source_path.parent()),
            )
        {
            let workspace = workspace.display().to_string();
            let canonical_id = disambiguated_external_id(&base_id, &workspace);
            conversation.external_id = Some(canonical_id.clone());
            self.diagnostics.push(
                classify(
                    "aider",
                    &conversation.source_path.display().to_string(),
                    IngestFailureKind::ExternalIdCollisionRisk,
                )
                .with_workspace(workspace)
                .with_ids(base_id, canonical_id),
            );
        }
    }

    /// Attach a scan-level error without converting it into a fake success.
    pub fn observe_scan_error(&mut self, source_path: &Path, error: impl std::fmt::Display) {
        let kind = if is_database_lock_message(&error.to_string()) {
            IngestFailureKind::SourceLocked
        } else {
            IngestFailureKind::UnreadableSource
        };
        let diagnostic = classify_path(&self.provider, source_path, kind);
        self.sources.insert(
            source_path.to_path_buf(),
            ObservedSource {
                disposition: diagnostic.disposition,
                malformed: false,
            },
        );
        self.diagnostics.push(diagnostic);
    }

    /// Record a failed connector scan. A Codex scan that stopped only because
    /// rollouts exceeded the per-source read budget is reported per rejected
    /// file, with the setting that admits it; it used to surface as one
    /// "unreadable, check permissions" entry naming the cass data directory.
    pub fn observe_connector_scan_error(&mut self, fallback_path: &Path, error: &anyhow::Error) {
        if let Some(over) = crate::connectors::codex::over_budget_sources(error) {
            for path in &over.sampled_paths {
                let diagnostic = classify_path(
                    &self.provider,
                    path,
                    IngestFailureKind::SourceOverReadBudget,
                );
                self.sources.insert(
                    path.clone(),
                    ObservedSource {
                        disposition: diagnostic.disposition,
                        malformed: false,
                    },
                );
                self.diagnostics.push(diagnostic);
            }
            return;
        }
        // GH #511: FAD's top message includes the failing path. Classify the
        // cause, so a project called "busy" cannot turn a parse or permission
        // failure into a retryable database lock.
        let diagnostic = classify_path(
            &self.provider,
            fallback_path,
            connector_scan_failure_kind(error),
        );
        self.sources.insert(
            fallback_path.to_path_buf(),
            ObservedSource {
                disposition: diagnostic.disposition,
                malformed: false,
            },
        );
        self.diagnostics.push(diagnostic);
    }

    /// Close the scope and summarize discovered sources.
    #[must_use]
    pub fn finish(self) -> ConnectorIngestReport {
        let mut summary = ProviderIngestSummary::default();
        for source in self.sources.values() {
            summary.record(source.disposition);
        }
        ConnectorIngestReport {
            provider: self.provider,
            summary,
            diagnostics: self.diagnostics,
        }
    }

    fn inspect_jsonl(&mut self, data_dir: &Path, path: &Path) {
        let is_jsonl = path
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case("jsonl"));
        if !is_jsonl {
            return;
        }
        let file = match std::fs::File::open(path) {
            Ok(file) => file,
            Err(error) => {
                self.observe_scan_error(path, error.to_string());
                return;
            }
        };
        let mut reader = std::io::BufReader::new(file);
        let mut raw_line = Vec::new();
        let mut line_number = 0u64;
        let source_path = path.display().to_string();
        let mut observations: Vec<crate::indexer::quarantine::ConnectorLineObservation> =
            Vec::new();
        loop {
            raw_line.clear();
            let bytes_read = match reader.read_until(b'\n', &mut raw_line) {
                Ok(bytes_read) => bytes_read,
                Err(error) => {
                    self.observe_scan_error(path, error);
                    break;
                }
            };
            if bytes_read == 0 {
                break;
            }
            line_number = line_number.saturating_add(1);
            let had_newline = raw_line.last() == Some(&b'\n');
            if had_newline {
                raw_line.pop();
                if raw_line.last() == Some(&b'\r') {
                    raw_line.pop();
                }
            }
            let line = raw_line.as_slice();
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            if serde_json::from_slice::<serde_json::Value>(line).is_ok() {
                continue;
            }
            let kind = if !had_newline {
                IngestFailureKind::TruncatedSession
            } else {
                IngestFailureKind::MalformedJsonLine
            };
            self.diagnostics
                .push(classify(&self.provider, &source_path, kind).with_line(line_number));
            self.sources.entry(path.to_path_buf()).and_modify(|source| {
                source.disposition = SourceIngestDisposition::Quarantined;
                source.malformed = true;
            });
            observations.push(
                crate::indexer::quarantine::ConnectorLineObservation::observe(
                    line_number,
                    line,
                    kind.as_str(),
                ),
            );
        }
        // One ledger publication for the whole source file, not one per
        // malformed line: a re-scan that observes nothing new writes nothing.
        if let Err(error) = crate::indexer::quarantine::record_connector_line_batch(
            data_dir,
            &self.provider,
            path,
            &observations,
        ) {
            tracing::warn!(
                provider = %self.provider,
                source_path = %path.display(),
                malformed_lines = observations.len(),
                error = %error,
                "failed to persist connector line quarantine records"
            );
        }
    }

    fn inspect_cursor_database(&mut self, path: &Path) {
        if path.file_name().and_then(|name| name.to_str()) != Some("state.vscdb") {
            return;
        }
        // Avoid doubling the cost of every cold Cursor DB open. A live Cursor
        // writer uses SQLite sidecars; only those potentially contended stores
        // need the explicit lock probe before the connector's normal read.
        let wal_path = path.with_file_name("state.vscdb-wal");
        let journal_path = path.with_file_name("state.vscdb-journal");
        if !wal_path.exists() && !journal_path.exists() {
            return;
        }
        // Strict read-only (F17 doctrine): this probe may never write or
        // create any sidecar of the user's store. A plain read-only open on
        // a WAL database creates or rebuilds sidecars in place, which would
        // mutate the store during `cass index`; `open_external_strict_in_place`
        // instead validates the container and sidecars up front and refuses
        // every state needing recovery with `SqliteError::BusyRecovery`, which
        // `connector_scan_failure_kind` maps to the transient `SourceLocked`
        // warning. The connector's own read still goes through FAD's strict
        // open + private-snapshot fallback (F9/F17), so a store this probe
        // refuses is still indexed.
        let probe = open_external_strict_in_place(path).and_then(|connection| {
            FadConnectionExt::query_row_map(
                &connection,
                "SELECT 1 FROM sqlite_master LIMIT 1",
                &[],
                |_row| Ok(()),
            )
        });
        if let Err(error) = probe {
            let error = anyhow::Error::new(error);
            if connector_scan_failure_kind(&error) == IngestFailureKind::SourceLocked {
                self.observe_connector_scan_error(path, &error);
            }
        }
    }

    fn inspect_chatgpt_encrypted_directories(
        &mut self,
        ctx: &ScanContext,
        sources: &[DiscoveredSourceFile],
    ) {
        let mut pending = if ctx.scan_roots.is_empty() {
            vec![(ctx.data_dir.clone(), 0usize)]
        } else {
            ctx.scan_roots
                .iter()
                .map(|root| (root.path.clone(), 0usize))
                .collect()
        };
        while let Some((path, depth)) = pending.pop() {
            let encrypted = path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.starts_with("conversations-v2-") || name.starts_with("conversations-v3-")
                });
            if encrypted {
                let key_was_available = sources
                    .iter()
                    .any(|source| source.source_path.starts_with(&path));
                if !key_was_available {
                    let kind = if cfg!(target_os = "macos") {
                        IngestFailureKind::KeychainUnavailable
                    } else {
                        IngestFailureKind::EncryptedUnsupported
                    };
                    let diagnostic = classify_path("chatgpt", &path, kind);
                    self.sources.insert(
                        path,
                        ObservedSource {
                            disposition: diagnostic.disposition,
                            malformed: false,
                        },
                    );
                    self.diagnostics.push(diagnostic);
                }
                continue;
            }
            if depth >= 5 {
                continue;
            }
            if let Ok(entries) = std::fs::read_dir(&path) {
                pending.extend(
                    entries
                        .flatten()
                        .filter(|entry| entry.path().is_dir())
                        .map(|entry| (entry.path(), depth + 1)),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests;
