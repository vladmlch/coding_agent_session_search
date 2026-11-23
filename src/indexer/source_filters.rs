use super::*;

/// Type alias for batch classification map:
/// (ConnectorKind, SourceKind, SourceId, Host, Path) -> (ScanRoot, MinTS, MaxTS).
///
/// Provenance is part of the key because two configured sources may point at
/// the same local mirror path while still representing distinct archives.
pub(in crate::indexer) type BatchClassificationMap = HashMap<
    (ConnectorKind, SourceKind, String, Option<String>, PathBuf),
    (ScanRoot, Option<i64>, Option<i64>),
>;

pub(in crate::indexer) const LEXICAL_REBUILD_PACKET_VERSION: u32 = CONVERSATION_PACKET_VERSION;
pub(in crate::indexer) const PREPARSE_PRIMARY_SOURCE_CAPTURE_LIMIT: usize = 256;
pub(in crate::indexer) const WATCH_INGEST_DEFAULT_CHUNK_SIZE: usize = 32;
pub(in crate::indexer) const WATCH_INGEST_CHUNK_SIZE_MAX: usize = 512;
pub(in crate::indexer) const NON_WATCH_INGEST_DEFAULT_CHUNK_SIZE: usize =
    DEFAULT_STREAMING_BATCH_LIMITS.max_conversations;
pub(in crate::indexer) const NON_WATCH_INGEST_CHUNK_SIZE_MAX: usize = 128;
pub(in crate::indexer) static ROBOT_TRACE_INGEST_ENABLED: AtomicBool = AtomicBool::new(false);
pub(in crate::indexer) static ROBOT_TRACE_INGEST_BATCH_N: AtomicU64 = AtomicU64::new(0);
pub(in crate::indexer) static ACTIVE_SESSION_SOURCE_SKIP_OBSERVED: AtomicBool =
    AtomicBool::new(false);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(in crate::indexer) struct SourceFileId {
    pub(in crate::indexer) dev: u64,
    pub(in crate::indexer) ino: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::indexer) enum ActiveSessionSourceReason {
    TestOverride,
    WritableFileDescriptor,
    AdvisoryLock,
    RecentlyModified,
}

#[derive(Debug, Default)]
pub(in crate::indexer) struct ActiveSessionSourceFilter {
    pub(in crate::indexer) writer_probe: SourceWriterProbe,
    pub(in crate::indexer) recent_write_window: Option<Duration>,
    pub(in crate::indexer) deferred_sources: Mutex<BTreeSet<PathBuf>>,
}

impl ActiveSessionSourceFilter {
    pub(in crate::indexer) fn new(enable_recent_write_window: bool) -> Self {
        Self {
            writer_probe: SourceWriterProbe::capture(is_session_log_source_candidate),
            recent_write_window: active_session_recent_write_window(enable_recent_write_window),
            deferred_sources: Mutex::default(),
        }
    }

    #[cfg(test)]
    pub(in crate::indexer) fn with_recent_write_window_for_test(
        recent_write_window: Option<Duration>,
    ) -> Self {
        Self {
            writer_probe: SourceWriterProbe::default(),
            recent_write_window,
            deferred_sources: Mutex::default(),
        }
    }

    pub(in crate::indexer) fn take_deferred_sources(&self) -> BTreeSet<PathBuf> {
        std::mem::take(
            &mut *self
                .deferred_sources
                .lock()
                .unwrap_or_else(|error| error.into_inner()),
        )
    }

    pub(in crate::indexer) fn active_writer_reason(
        &self,
        path: &Path,
    ) -> Option<ActiveSessionSourceReason> {
        if !is_session_log_source_candidate(path) {
            return None;
        }
        if test_active_session_source_path_matches(path) {
            return Some(ActiveSessionSourceReason::TestOverride);
        }
        // Cheap mtime check first: a single `stat` is dramatically cheaper than
        // walking `/proc/*/fd` (Linux) or shelling out to `lsof` (macOS), and it
        // is the only signal that catches macOS Claude Code's buffered-append
        // writers (which hold no writable fd advertised through lsof and do not
        // take advisory locks). False positives just defer a just-finished file
        // to the next watch cycle (acceptable). See #251 — the v0.5.1 data-loss
        // regression — for the original root cause.
        if self
            .recent_write_window
            .is_some_and(|window| source_file_recently_modified(path, window))
        {
            tracing::debug!(
                source_path = %path.display(),
                "active-source filter: mtime fallback fired"
            );
            return Some(ActiveSessionSourceReason::RecentlyModified);
        }
        if source_file_id(path)
            .is_some_and(|file_id| self.writer_probe.should_defer((file_id.dev, file_id.ino)))
        {
            return Some(ActiveSessionSourceReason::WritableFileDescriptor);
        }
        if source_file_has_active_advisory_lock(path) {
            return Some(ActiveSessionSourceReason::AdvisoryLock);
        }
        None
    }
}

pub(in crate::indexer) fn active_session_recent_write_window(
    enable_recent_write_window: bool,
) -> Option<Duration> {
    if !enable_recent_write_window {
        return None;
    }
    let seconds = dotenvy::var("CASS_ACTIVE_SESSION_RECENT_WRITE_WINDOW_SECS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(120)
        .min(3_600);
    (seconds > 0).then_some(Duration::from_secs(seconds))
}

pub(in crate::indexer) fn source_file_recently_modified(path: &Path, window: Duration) -> bool {
    let Ok(metadata) = fs::metadata(path) else {
        return false;
    };
    let Ok(modified_at) = metadata.modified() else {
        return false;
    };
    match SystemTime::now().duration_since(modified_at) {
        Ok(age) => age <= window,
        Err(_) => true,
    }
}

pub(in crate::indexer) fn is_session_log_source_candidate(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| {
            ext.eq_ignore_ascii_case("jsonl")
                || ext.eq_ignore_ascii_case("json")
                || ext.eq_ignore_ascii_case("claude")
        })
}

#[cfg(test)]
pub(in crate::indexer) fn test_active_session_source_path_matches(path: &Path) -> bool {
    let Ok(paths) = dotenvy::var("CASS_TEST_ACTIVE_SESSION_SOURCE_PATHS") else {
        return false;
    };
    std::env::split_paths(&paths).any(|candidate| candidate == path)
}

#[cfg(not(test))]
pub(in crate::indexer) fn test_active_session_source_path_matches(_path: &Path) -> bool {
    false
}

pub(in crate::indexer) fn should_skip_active_session_source(
    active_source_filter: &ActiveSessionSourceFilter,
    source_kind: SourceKind,
    source_path: &Path,
) -> bool {
    if source_kind != SourceKind::Local {
        return false;
    }
    let Some(reason) = active_source_filter.active_writer_reason(source_path) else {
        return false;
    };
    ACTIVE_SESSION_SOURCE_SKIP_OBSERVED.store(true, Ordering::Relaxed);
    active_source_filter
        .deferred_sources
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .insert(source_path.to_path_buf());
    tracing::info!(
        source_path = %source_path.display(),
        ?reason,
        "skipping session source that appears to be actively written"
    );
    true
}

/// GH #486 / bead 0f1k0: `CASS_EXCLUDE_PATHS` covers every connector. The
/// Claude Code, Codex, Codebuff and Pi readers skip an excluded file before
/// parsing it, but the other connectors read every source they find, so each
/// conversation is checked here before it is counted, stored or indexed. The
/// policy is the one raw-mirror capture applies: whole path components, a
/// relative entry taken from the working directory, aliases of an existing
/// directory resolved on both sides, and an unusable policy excluding
/// everything. Scan watermarks are already held while exclusions are set
/// (`scan_watermark_preservation_active`), so a skipped source is picked up
/// once its exclusion is removed.
pub(in crate::indexer) fn should_skip_excluded_source(source_path: &Path) -> bool {
    let excluded =
        crate::connectors::codex::path_policy::ScanExclusions::from_env().excludes(source_path);
    if excluded {
        tracing::debug!(
            source_path = %source_path.display(),
            "skipping a conversation whose source CASS_EXCLUDE_PATHS excludes"
        );
    }
    excluded
}

/// Prospective workspace exclusions (GH #537) are a completed ingest policy,
/// unlike an unreadable or deferred source. They never hold scan watermarks.
/// Bind the policy to reuse evidence so removing a glob revisits old sources.
#[derive(Debug, Clone, Default)]
pub(in crate::indexer) struct WorkspaceExclusions {
    pub(in crate::indexer) patterns: Vec<glob::Pattern>,
    pub(in crate::indexer) signature: String,
}

impl WorkspaceExclusions {
    pub(in crate::indexer) fn parse(value: &str) -> Result<Self> {
        let entries: BTreeSet<&str> = value
            .split([',', '\n'])
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .collect();
        let patterns = entries
            .iter()
            .map(|entry| {
                glob::Pattern::new(entry).with_context(|| {
                    format!(
                        "invalid CASS_EXCLUDE_WORKSPACES glob {entry:?}; use valid workspace \
                         globs separated by commas or newlines"
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            patterns,
            signature: entries.into_iter().collect::<Vec<_>>().join("\n"),
        })
    }

    pub(in crate::indexer) fn from_env() -> Result<Self> {
        match dotenvy::var("CASS_EXCLUDE_WORKSPACES") {
            Ok(value) => Self::parse(&value),
            Err(dotenvy::Error::EnvVar(std::env::VarError::NotPresent)) => Ok(Self::default()),
            Err(error) => Err(error).context("CASS_EXCLUDE_WORKSPACES must be valid Unicode"),
        }
    }

    pub(in crate::indexer) fn excludes(&self, workspace: Option<&Path>) -> bool {
        workspace.is_some_and(|path| {
            self.patterns
                .iter()
                .any(|pattern| pattern.matches_path(path))
        })
    }

    pub(in crate::indexer) fn bind_contract(&self, contract: &str) -> String {
        if self.patterns.is_empty() {
            contract.to_string()
        } else {
            format!(
                "{contract}:workspace-exclusions-v1:{}",
                blake3::hash(self.signature.as_bytes()).to_hex()
            )
        }
    }
}

pub(in crate::indexer) const WORKSPACE_EXCLUSION_SCAN_PREFIX: &str =
    "workspace_exclusions:connector:";

pub(in crate::indexer) fn workspace_exclusion_scan_policies(
    storage: &FrankenStorage,
) -> Result<HashMap<String, String>> {
    Ok(storage
        .raw()
        .query_map_collect(
            "SELECT key, value FROM meta WHERE key >= 'workspace_exclusions:connector:' \
         AND key < 'workspace_exclusions:connector;'",
            &[],
            |row| Ok((row.get_typed::<String>(0)?, row.get_typed::<String>(1)?)),
        )?
        .into_iter()
        .collect())
}

pub(in crate::indexer) fn record_connector_scan_watermark(
    storage: &FrankenStorage,
    connector_name: &str,
    scan_start_ts: i64,
) -> Result<()> {
    let policy = WorkspaceExclusions::from_env()?;
    storage.set_connector_last_scan_ts(connector_name, scan_start_ts)?;
    // Write the policy after the watermark. If this write fails, a subsequent
    // run conservatively scans again; it cannot bless a partial policy change.
    let key = format!(
        "{WORKSPACE_EXCLUSION_SCAN_PREFIX}{}",
        connector_name.trim().to_ascii_lowercase()
    );
    storage.raw().execute_compat(
        "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
        &[
            ParamValue::from(key.as_str()),
            ParamValue::from(policy.signature.as_str()),
        ],
    )?;
    Ok(())
}

/// Whether `CASS_SKIP_SUBAGENTS` is enabled (#292). Subagent transcripts
/// (Claude Code's `<project>/<session>/subagents/agent-*.jsonl`) are ephemeral
/// one-task scratchpads whose conclusions already live in the parent session;
/// on a subagent-heavy corpus they dominate the corpus by count and are the
/// main source of false-positive OOM quarantines, so operators can exclude the
/// whole class from indexing with one toggle. `0`/`false`/empty disables.
pub(in crate::indexer) fn skip_subagents_active() -> bool {
    dotenvy::var("CASS_SKIP_SUBAGENTS")
        .ok()
        .map(|raw| {
            let value = raw.trim();
            !(value.is_empty() || value == "0" || value.eq_ignore_ascii_case("false"))
        })
        .unwrap_or(false)
}

/// True when `source_path` is a subagent transcript — it lives directly inside a
/// `subagents/` directory (e.g. `.../<session>/subagents/agent-1234.jsonl`).
pub(in crate::indexer) fn conversation_source_is_subagent(source_path: &Path) -> bool {
    source_path
        .parent()
        .and_then(|parent| parent.file_name())
        .is_some_and(|name| name == "subagents")
}

/// Combined scan-time skip for the `CASS_SKIP_SUBAGENTS` toggle (#292): drop
/// subagent-class transcripts before they are ingested, so they never enter the
/// canonical DB / lexical index / raw-mirror and never trigger the OOM→quarantine
/// path. Applied at every connector scan callback.
pub(in crate::indexer) fn conversation_is_subagent(conversation: &NormalizedConversation) -> bool {
    conversation_source_is_subagent(&conversation.source_path)
        || (conversation.agent_slug == "shelley"
            && conversation.metadata["source"] == "shelley"
            && conversation
                .metadata
                .pointer("/shelley/parent_conversation_id")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|parent| !parent.trim().is_empty()))
}

pub(in crate::indexer) fn should_skip_subagent(conversation: &NormalizedConversation) -> bool {
    skip_subagents_active() && conversation_is_subagent(conversation)
}

#[cfg(test)]
pub(in crate::indexer) mod subagent_skip_tests {
    use super::{
        NormalizedConversation, conversation_is_subagent, conversation_source_is_subagent,
    };
    use std::path::Path;

    #[test]
    fn detects_subagent_transcripts_by_parent_dir() {
        // Claude Code subagent transcript: file directly inside a subagents/ dir.
        assert!(conversation_source_is_subagent(Path::new(
            "/home/u/.claude/projects/proj/session-1/subagents/agent-abc.jsonl"
        )));
        // A top-level session transcript is not a subagent.
        assert!(!conversation_source_is_subagent(Path::new(
            "/home/u/.claude/projects/proj/session-1.jsonl"
        )));
        // A file merely named like an agent but not under subagents/ is kept.
        assert!(!conversation_source_is_subagent(Path::new(
            "/home/u/.codex/sessions/agent-notes.jsonl"
        )));
        // A subagents/ dir higher up does not match a file nested deeper under it.
        assert!(!conversation_source_is_subagent(Path::new(
            "/home/u/subagents/archive/session.jsonl"
        )));
    }

    #[test]
    fn gh415_shelley_subagent_requires_authoritative_parent_metadata() {
        let mut conversation = NormalizedConversation {
            agent_slug: "shelley".into(),
            external_id: Some("child".into()),
            title: None,
            workspace: None,
            source_path: "/source/sessions.sqlite3".into(),
            started_at: None,
            ended_at: None,
            messages: Vec::new(),
            metadata: serde_json::json!({"source":"shelley", "shelley":{"parent_conversation_id":"parent"}}),
        };
        assert!(conversation_is_subagent(&conversation));
        for parent in [
            serde_json::Value::Null,
            serde_json::json!(""),
            serde_json::json!("  "),
            serde_json::json!(123),
        ] {
            conversation.metadata["shelley"]["parent_conversation_id"] = parent;
            assert!(!conversation_is_subagent(&conversation));
        }
        conversation.metadata["shelley"]["parent_conversation_id"] = serde_json::json!("parent");
        conversation.metadata["source"] = serde_json::json!("other");
        assert!(!conversation_is_subagent(&conversation));
        conversation.metadata["source"] = serde_json::json!("shelley");
        conversation.agent_slug = "codex".into();
        assert!(!conversation_is_subagent(&conversation));
    }
}

#[cfg(unix)]
pub(in crate::indexer) fn source_file_id(path: &Path) -> Option<SourceFileId> {
    use std::os::unix::fs::MetadataExt;

    fs::metadata(path).ok().map(|metadata| SourceFileId {
        dev: metadata.dev(),
        ino: metadata.ino(),
    })
}

#[cfg(not(unix))]
pub(in crate::indexer) fn source_file_id(_path: &Path) -> Option<SourceFileId> {
    None
}

pub(in crate::indexer) fn source_file_has_active_advisory_lock(path: &Path) -> bool {
    let Ok(file) = OpenOptions::new().read(true).open(path) else {
        return false;
    };
    // std's File::try_lock reports another holder as WouldBlock on every
    // platform; with fs2, Windows contention was a raw ERROR_LOCK_VIOLATION
    // that matched nothing here, so a file a writer held looked unlocked
    // (2l1b0.74).
    match file.try_lock() {
        Ok(()) => {
            let _ = file.unlock();
            false
        }
        Err(std::fs::TryLockError::WouldBlock) => true,
        Err(std::fs::TryLockError::Error(error)) => error.kind() == std::io::ErrorKind::Interrupted,
    }
}

#[cfg(target_os = "linux")]
pub(in crate::indexer) mod linux_publish_swap {
    use std::ffi::{c_char, c_int, c_uint};

    pub const AT_FDCWD: c_int = -100;
    pub const RENAME_EXCHANGE: c_uint = 0x2;
    /// Linux's `<errno.h>` definition (`EINVAL = 22`). We don't depend on
    /// `libc` directly — `errno.h` has guaranteed this value on every
    /// glibc/musl architecture since the kernel was renumbered, and
    /// pulling in `libc` for one constant inflates compile time.
    /// Cross-checked against `man 2 renameat2` (returns EINVAL when the
    /// underlying filesystem doesn't support `RENAME_EXCHANGE`).
    pub const EINVAL: i32 = 22;

    // SAFETY: a raw libc symbol declaration; the only caller is
    // `atomic_exchange_paths`, which documents its argument invariants.
    #[allow(unsafe_code)]
    unsafe extern "C" {
        pub fn renameat2(
            olddirfd: c_int,
            oldpath: *const c_char,
            newdirfd: c_int,
            newpath: *const c_char,
            flags: c_uint,
        ) -> c_int;
    }
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::indexer) enum LexicalRebuildPacketSource {
    CanonicalReplay,
    NormalizedConversation,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::indexer) enum LexicalRebuildPacketProvenanceMode {
    SourceMapLookup,
    ConversationFields,
    MetadataFields,
    HostFallback,
    LocalDefault,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::indexer) struct LexicalRebuildPacketDiagnostics {
    pub(in crate::indexer) version: u32,
    pub(in crate::indexer) source: LexicalRebuildPacketSource,
    pub(in crate::indexer) provenance_mode: LexicalRebuildPacketProvenanceMode,
    pub(in crate::indexer) missing_conversation_id: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::indexer) struct LexicalRebuildPacketIdentity {
    pub(in crate::indexer) conversation_id: Option<i64>,
    pub(in crate::indexer) external_id: Option<String>,
    pub(in crate::indexer) agent: String,
    pub(in crate::indexer) workspace: Option<String>,
    pub(in crate::indexer) source_path: String,
    pub(in crate::indexer) title: Option<String>,
    pub(in crate::indexer) started_at: Option<i64>,
    pub(in crate::indexer) ended_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::indexer) struct LexicalRebuildPacketProvenance {
    pub(in crate::indexer) source_id: String,
    pub(in crate::indexer) origin_kind: String,
    pub(in crate::indexer) origin_host: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::indexer) struct LexicalRebuildConversationPacket {
    pub(in crate::indexer) diagnostics: LexicalRebuildPacketDiagnostics,
    pub(in crate::indexer) identity: LexicalRebuildPacketIdentity,
    pub(in crate::indexer) provenance: LexicalRebuildPacketProvenance,
    pub(in crate::indexer) contract_hashes: ConversationPacketHashes,
    pub(in crate::indexer) contract_projections: ConversationPacketSinkProjections,
    pub(in crate::indexer) messages: crate::storage::sqlite::LexicalRebuildGroupedMessageRows,
    pub(in crate::indexer) message_count: usize,
    pub(in crate::indexer) message_bytes: usize,
    pub(in crate::indexer) flow_reservation_bytes: usize,
    pub(in crate::indexer) last_message_id: Option<i64>,
    /// Set when this packet carries one message-range chunk of a conversation
    /// too long to hold whole (bgn6s); `None` for an ordinary whole-conversation
    /// packet.
    pub(in crate::indexer) chunk: Option<LexicalRebuildChunk>,
}

/// One message-range chunk of a long conversation (bgn6s). Chunks of one
/// conversation arrive in order; only the last finishes the conversation, so
/// progress and commits still happen at conversation boundaries.
#[derive(Clone)]
pub(in crate::indexer) struct LexicalRebuildChunk {
    pub(in crate::indexer) is_final: bool,
    /// Released once the consumer has flushed this chunk into the index, so
    /// the page-prep worker reads the next chunk only then.
    pub(in crate::indexer) gate: Arc<LexicalRebuildChunkGate>,
}

impl std::fmt::Debug for LexicalRebuildChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LexicalRebuildChunk")
            .field("is_final", &self.is_final)
            .finish_non_exhaustive()
    }
}

impl PartialEq for LexicalRebuildChunk {
    fn eq(&self, other: &Self) -> bool {
        self.is_final == other.is_final
    }
}

impl Eq for LexicalRebuildChunk {}

/// Flow control for streaming one long conversation from a page-prep worker
/// to the consumer: at most one flushed-but-unacknowledged chunk is in flight.
#[derive(Debug, Default)]
pub(in crate::indexer) struct LexicalRebuildChunkGate {
    pub(in crate::indexer) outstanding: Mutex<usize>,
    pub(in crate::indexer) cv: Condvar,
}

impl LexicalRebuildChunkGate {
    pub(in crate::indexer) fn sent(&self) {
        let mut outstanding = self.outstanding.lock().unwrap_or_else(|e| e.into_inner());
        *outstanding = outstanding.saturating_add(1);
    }

    pub(in crate::indexer) fn flushed(&self) {
        let mut outstanding = self.outstanding.lock().unwrap_or_else(|e| e.into_inner());
        *outstanding = outstanding.saturating_sub(1);
        self.cv.notify_all();
    }

    /// Wait until every sent chunk has been flushed, or fail once the pipeline
    /// is torn down (the consumer will never flush again). A failing consumer
    /// closes the flow limiter; a failing producer or worker closes the
    /// reservation order.
    pub(in crate::indexer) fn wait_until_flushed(
        &self,
        reservation_order: &LexicalRebuildReservationOrder,
        flow_limiter: &StreamingByteLimiter,
    ) -> Result<()> {
        let mut outstanding = self.outstanding.lock().unwrap_or_else(|e| e.into_inner());
        while *outstanding > 0 {
            if reservation_order.is_closed() || flow_limiter.is_closed() {
                anyhow::bail!(
                    "lexical rebuild pipeline closed while a long conversation was streaming"
                );
            }
            outstanding = self
                .cv
                .wait_timeout(outstanding, Duration::from_millis(200))
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LexicalRebuildPacketFingerprintInput<'a> {
    pub version: u32,
    pub agent: &'a str,
    pub external_id: Option<&'a str>,
    pub workspace: Option<&'a str>,
    pub source_path: &'a str,
    pub title: Option<&'a str>,
    pub started_at: Option<i64>,
    pub ended_at: Option<i64>,
    pub source_id: &'a str,
    pub origin_kind: &'a str,
    pub origin_host: Option<&'a str>,
    pub lexical_projected_content_bytes: usize,
    pub messages: &'a crate::storage::sqlite::LexicalRebuildGroupedMessageRows,
    pub message_count: usize,
    pub message_bytes: usize,
}

#[cfg(test)]
pub(in crate::indexer) fn message_id_from_db(raw: i64) -> Option<u64> {
    u64::try_from(raw).ok()
}

#[cfg(test)]
pub(in crate::indexer) fn saturating_u32_from_i64(raw: i64) -> u32 {
    match u32::try_from(raw) {
        Ok(value) => value,
        Err(_) if raw.is_negative() => 0,
        Err(_) => u32::MAX,
    }
}
