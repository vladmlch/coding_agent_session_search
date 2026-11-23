use super::*;

#[derive(Debug, Clone)]
pub enum ReindexCommand {
    Full,
}

#[derive(Debug)]
pub enum IndexerEvent {
    Notify(Vec<PathBuf>),
    Command(ReindexCommand),
}

// =============================================================================
// Stale Detection (Issue #54)
// =============================================================================

/// Action to take when watch daemon detects stale state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StaleAction {
    /// Log warning but take no automatic action.
    #[default]
    Warn,
    /// Automatically trigger full rebuild.
    Rebuild,
    /// Disable stale detection entirely.
    None,
}

impl StaleAction {
    /// Parse from environment variable value.
    pub(in crate::indexer) fn from_env_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "rebuild" | "auto" | "fix" => Self::Rebuild,
            "none" | "off" | "disabled" | "0" | "false" => Self::None,
            _ => Self::Warn, // Default: warn, log
        }
    }
}

/// Configuration for stale detection.
#[derive(Debug, Clone)]
pub struct StaleConfig {
    /// Hours without successful ingests before considering stale (default: 24).
    pub threshold_hours: u64,
    /// Action to take when stale detected.
    pub action: StaleAction,
    /// Minutes between stale checks (default: 60).
    pub check_interval_mins: u64,
    /// Minimum scans with 0 conversations before triggering (default: 10).
    pub min_zero_scans: u64,
}

impl Default for StaleConfig {
    fn default() -> Self {
        Self {
            threshold_hours: 24,
            action: StaleAction::Warn,
            check_interval_mins: 60,
            min_zero_scans: 10,
        }
    }
}

impl StaleConfig {
    /// Load configuration from environment variables.
    pub fn from_env() -> Self {
        let mut cfg = Self::default();

        if let Some(hours) = env_u64("CASS_WATCH_STALE_THRESHOLD_HOURS") {
            cfg.threshold_hours = hours;
        }

        if let Ok(val) = dotenvy::var("CASS_WATCH_STALE_ACTION") {
            cfg.action = StaleAction::from_env_str(&val);
        }

        if let Some(mins) = env_u64("CASS_WATCH_STALE_CHECK_INTERVAL_MINS") {
            cfg.check_interval_mins = mins;
        }

        if let Some(count) = env_u64("CASS_WATCH_STALE_MIN_ZERO_SCANS") {
            cfg.min_zero_scans = count;
        }

        cfg
    }

    /// Check if stale detection is enabled.
    pub fn is_enabled(&self) -> bool {
        self.action != StaleAction::None
    }
}

pub(in crate::indexer) fn env_u64(key: &str) -> Option<u64> {
    dotenvy::var(key).ok()?.parse().ok()
}

/// Tracks indexing activity to detect stuck/stale states.
///
/// The watch daemon can get into a state where it runs but fails to index
/// new conversations (e.g., due to connector parsing issues). This detector
/// monitors ingest activity and triggers recovery when appropriate.
#[derive(Debug)]
pub struct StaleDetector {
    pub(in crate::indexer) config: StaleConfig,
    /// Timestamp of last successful ingest (>0 conversations).
    pub(in crate::indexer) last_successful_ingest: Mutex<Option<Instant>>,
    /// Count of consecutive scans with 0 conversations.
    pub(in crate::indexer) consecutive_zero_scans: std::sync::atomic::AtomicU64,
    /// Whether stale warning has been emitted (to avoid spam).
    pub(in crate::indexer) warning_emitted: AtomicBool,
    /// Last stale check timestamp.
    pub(in crate::indexer) last_check: Mutex<Instant>,
    /// Total successful ingests since start.
    pub(in crate::indexer) total_ingests: std::sync::atomic::AtomicU64,
    /// Time when the detector was created.
    pub(in crate::indexer) start_time: Instant,
}

impl StaleDetector {
    /// Create a new stale detector with given configuration.
    pub fn new(config: StaleConfig) -> Self {
        Self {
            config,
            last_successful_ingest: Mutex::new(None),
            consecutive_zero_scans: std::sync::atomic::AtomicU64::new(0),
            warning_emitted: AtomicBool::new(false),
            last_check: Mutex::new(Instant::now()),
            total_ingests: std::sync::atomic::AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    /// Create with configuration loaded from environment.
    pub fn from_env() -> Self {
        Self::new(StaleConfig::from_env())
    }

    /// Record a scan result.
    ///
    /// Called after each watch scan cycle with the number of conversations indexed.
    pub fn record_scan(&self, conversations_indexed: usize) {
        if conversations_indexed > 0 {
            // Successful ingest
            {
                let mut guard = self
                    .last_successful_ingest
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                *guard = Some(Instant::now());
            }
            self.consecutive_zero_scans.store(0, Ordering::Relaxed);
            self.warning_emitted.store(false, Ordering::Relaxed);
            self.total_ingests.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(
                conversations = conversations_indexed,
                "stale_detector: successful ingest recorded"
            );
        } else {
            // Zero-conversation scan
            let count = self.consecutive_zero_scans.fetch_add(1, Ordering::Relaxed) + 1;
            tracing::trace!(
                consecutive_zero_scans = count,
                "stale_detector: zero-conversation scan"
            );
        }
    }

    /// Check if the indexer appears to be in a stale state.
    ///
    /// Returns `Some(StaleAction)` if stale and action should be taken.
    pub fn check_stale(&self) -> Option<StaleAction> {
        if !self.config.is_enabled() {
            return None;
        }

        // Check if enough time has passed since last check
        let now = Instant::now();
        {
            let mut last_check = self.last_check.lock().unwrap_or_else(|e| e.into_inner());
            let check_interval = Duration::from_secs(self.config.check_interval_mins * 60);
            if now.duration_since(*last_check) < check_interval {
                return None;
            }
            *last_check = now;
        }

        // Check consecutive zero scans threshold
        let zero_count = self.consecutive_zero_scans.load(Ordering::Relaxed);
        if zero_count < self.config.min_zero_scans {
            return None;
        }

        // Check time since last successful ingest
        let threshold = Duration::from_secs(self.config.threshold_hours * 3600);
        let is_stale = match self
            .last_successful_ingest
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            Some(last) => now.duration_since(*last) > threshold,
            // No successful ingests ever - check if we've been running long enough
            None => now.duration_since(self.start_time) > threshold,
        };

        if is_stale {
            let already_warned = self.warning_emitted.swap(true, Ordering::Relaxed);
            if !already_warned || self.config.action == StaleAction::Rebuild {
                return Some(self.config.action);
            }
        }

        None
    }

    /// Get statistics for logging/debugging.
    pub fn stats(&self) -> StaleStats {
        let last_ingest = *self
            .last_successful_ingest
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        StaleStats {
            consecutive_zero_scans: self.consecutive_zero_scans.load(Ordering::Relaxed),
            total_ingests: self.total_ingests.load(Ordering::Relaxed),
            seconds_since_last_ingest: last_ingest.map(|t| t.elapsed().as_secs()),
            warning_emitted: self.warning_emitted.load(Ordering::Relaxed),
            config_action: format!("{:?}", self.config.action),
            config_threshold_hours: self.config.threshold_hours,
        }
    }

    /// Reset the detector state (e.g., after a full rebuild).
    pub fn reset(&self) {
        {
            let mut guard = self
                .last_successful_ingest
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            *guard = Some(Instant::now());
        }
        self.consecutive_zero_scans.store(0, Ordering::Relaxed);
        self.warning_emitted.store(false, Ordering::Relaxed);
    }
}

/// Statistics from the stale detector.
#[derive(Debug, Clone, serde::Serialize)]
pub struct StaleStats {
    pub consecutive_zero_scans: u64,
    pub total_ingests: u64,
    pub seconds_since_last_ingest: Option<u64>,
    pub warning_emitted: bool,
    pub config_action: String,
    pub config_threshold_hours: u64,
}

/// Per-connector statistics for structured logging (T7.4).
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct ConnectorStats {
    pub name: String,
    pub conversations: usize,
    pub messages: usize,
    pub scan_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Structured lexical repair metadata for JSON output.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct LexicalRepairStats {
    pub kind: String,
    pub reason: String,
    pub canonical_conversations: usize,
    pub canonical_messages: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_tantivy_docs: Option<usize>,
}

/// Structured proof for `cass index --semantic --watch-once <path>`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SemanticWatchOnceStats {
    pub published: bool,
    pub selected_docs: usize,
    pub embedded_docs: usize,
    pub tier: String,
    pub vector_index_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest_before_db_fingerprint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest_after_db_fingerprint: Option<String>,
    pub reason: String,
}

/// Incremental extension-version re-projection accounting, reported under
/// `indexing_stats.region_reproject` (see [`run_region_reprojection_lane`]).
///
/// * `stale_sessions` — sessions detected below the current
///   extractor/region/projection versions this run.
/// * `reprojected` — sessions the lane re-projected from the PERSISTED
///   archive: a full coherent generation (facts/projection rows, search
///   regions and the region mark, and the receipt) republished in one
///   savepoint, whether or not the row sets came out unchanged. This is
///   the drain signal — every one of these carries a fresh
///   DOCS_PENDING region mark for the post-lane drain.
/// * `skipped_unchanged` — sessions the lane visited without
///   republishing anything: the receipt-and-markers skip key already
///   saw them current (a receipt current while a row drifted). The
///   steady re-run marker.
/// * `failed` — sessions whose re-projection failed and rolled back to
///   their savepoint; each keeps a durable `failed` receipt naming the
///   reason and is retried by the next run.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct RegionReprojectStats {
    pub stale_sessions: usize,
    pub reprojected: usize,
    pub skipped_unchanged: usize,
    pub failed: usize,
}

/// Aggregate indexing statistics for JSON output (T7.4).
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct IndexingStats {
    /// Time spent in scanning phase (ms)
    pub scan_ms: u64,
    /// Time spent in indexing phase (ms)
    pub index_ms: u64,
    /// Linux process block-write counter delta across the index run, including
    /// final checkpointing. This is physical I/O accounting, not logical source
    /// bytes or an attribution to one storage layer. Omitted when unavailable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_written: Option<u64>,
    /// True when any attempted source/connector scan failed. Successful
    /// source commits are retained, but this run must not report complete
    /// coverage or cache a successful command result (GH #484).
    pub scan_had_errors: bool,
    /// Per-connector breakdown
    pub connectors: Vec<ConnectorStats>,
    /// Per-provider source outcomes; unlike conversation counts this preserves
    /// locked, encrypted, malformed, skipped, and discovered-only sources.
    pub connector_summary: BTreeMap<String, ProviderIngestSummary>,
    /// Actionable connector findings. These contain provenance and hashes, but
    /// never message or malformed-line payload content.
    pub connector_diagnostics: Vec<ConnectorIngestDiagnostic>,
    /// Agents discovered during scan
    pub agents_discovered: Vec<String>,
    /// Total conversations indexed
    pub total_conversations: usize,
    /// Total messages indexed
    pub total_messages: usize,
    #[serde(skip_serializing)]
    pub total_counts_exact: bool,
    /// Chosen lexical population strategy for this run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lexical_strategy: Option<String>,
    /// Why the lexical population strategy was chosen.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lexical_strategy_reason: Option<String>,
    /// Automatic lexical repair/catch-up performed before normal indexing work.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lexical_repair: Option<LexicalRepairStats>,
    /// Targeted semantic watch-once publish proof.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub semantic_watch_once: Option<SemanticWatchOnceStats>,
    /// Conversations skipped by ingest quarantine during this run.
    pub quarantined_conversations: usize,
    /// True when SQLite ingest succeeded but inline lexical updates were
    /// deferred and the caller must rebuild lexical assets from the archive.
    pub lexical_update_deferred: bool,
    /// Local session files this run did not read because a process was still
    /// writing them (an open writable descriptor, an advisory lock or, in
    /// watch mode, a recent write). Their scan watermark is held, so a later
    /// run reads them once the writer lets go; until then their newest
    /// messages are not searchable, and a run that exits 0 says so here
    /// (GH #530).
    pub deferred_active_sources: usize,
    /// The first `DEFERRED_ACTIVE_SOURCE_SAMPLE` of those paths, sorted.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub deferred_active_source_sample: Vec<String>,
    /// Extension-version re-projection accounting: the incremental pass
    /// that replays stale sessions (marks/facts/projection below the
    /// current versions) from the PERSISTED archive through the
    /// per-conversation backfill lane and converges their search-region
    /// docs. Present only on non-watch runs of archives carrying the
    /// extension profile; omitted when the lane could not run (watch
    /// mode, env disabled, no marks table) or when its accounting itself
    /// failed after a successful run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region_reproject: Option<RegionReprojectStats>,
    #[serde(skip)]
    pub(in crate::indexer) deferred_active_source_paths: BTreeSet<PathBuf>,
}

pub(in crate::indexer) const DEFERRED_ACTIVE_SOURCE_SAMPLE: usize = 20;

pub(in crate::indexer) fn record_deferred_active_sources(
    progress: Option<&Arc<IndexingProgress>>,
    sources: &BTreeSet<PathBuf>,
) {
    let Some(progress) = progress else {
        return;
    };
    if sources.is_empty() {
        return;
    }
    let Ok(mut stats) = progress.stats.lock() else {
        return;
    };
    stats
        .deferred_active_source_paths
        .extend(sources.iter().cloned());
    stats.deferred_active_sources = stats.deferred_active_source_paths.len();
    stats.deferred_active_source_sample = stats
        .deferred_active_source_paths
        .iter()
        .take(DEFERRED_ACTIVE_SOURCE_SAMPLE)
        .map(|path| path.display().to_string())
        .collect();
}

pub(in crate::indexer) fn record_connector_ingest_report(
    progress: Option<&Arc<IndexingProgress>>,
    report: ConnectorIngestReport,
) {
    let Some(progress) = progress else {
        return;
    };
    let Ok(mut stats) = progress.stats.lock() else {
        return;
    };
    stats
        .connector_summary
        .entry(report.provider)
        .or_default()
        .merge(report.summary);
    stats.connector_diagnostics.extend(report.diagnostics);
    stats.connector_diagnostics.sort_by(|left, right| {
        left.provider
            .cmp(&right.provider)
            .then_with(|| left.source_path.cmp(&right.source_path))
            .then_with(|| left.line_or_row.cmp(&right.line_or_row))
            .then_with(|| left.failure_kind.cmp(&right.failure_kind))
    });
    stats.connector_diagnostics.dedup();
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::indexer) struct CanonicalMutationCounts {
    pub(in crate::indexer) inserted_conversations: usize,
    pub(in crate::indexer) inserted_messages: usize,
    pub(in crate::indexer) updated_messages: usize,
}

impl CanonicalMutationCounts {
    pub(in crate::indexer) fn accumulate(self, other: Self) -> Self {
        Self {
            inserted_conversations: self
                .inserted_conversations
                .saturating_add(other.inserted_conversations),
            inserted_messages: self
                .inserted_messages
                .saturating_add(other.inserted_messages),
            updated_messages: self.updated_messages.saturating_add(other.updated_messages),
        }
    }

    pub(in crate::indexer) fn changed(self) -> bool {
        self.inserted_conversations > 0 || self.inserted_messages > 0 || self.updated_messages > 0
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(in crate::indexer) struct NonWatchIngestOutcome {
    pub(in crate::indexer) canonical_mutations: CanonicalMutationCounts,
    pub(in crate::indexer) quarantined_conversations: usize,
    pub(in crate::indexer) lexical_update_deferred: bool,
    pub(in crate::indexer) scanned_connectors: BTreeSet<String>,
    pub(in crate::indexer) scan_had_errors: bool,
    pub(in crate::indexer) deferred_sources: BTreeSet<PathBuf>,
}

impl NonWatchIngestOutcome {
    pub(in crate::indexer) fn accumulate(self, other: Self) -> Self {
        let mut scanned_connectors = self.scanned_connectors;
        scanned_connectors.extend(other.scanned_connectors);
        let mut deferred_sources = self.deferred_sources;
        deferred_sources.extend(other.deferred_sources);
        Self {
            canonical_mutations: self
                .canonical_mutations
                .accumulate(other.canonical_mutations),
            quarantined_conversations: self
                .quarantined_conversations
                .saturating_add(other.quarantined_conversations),
            lexical_update_deferred: self.lexical_update_deferred || other.lexical_update_deferred,
            scanned_connectors,
            scan_had_errors: self.scan_had_errors || other.scan_had_errors,
            deferred_sources,
        }
    }
}

impl std::ops::Deref for NonWatchIngestOutcome {
    type Target = CanonicalMutationCounts;

    fn deref(&self) -> &Self::Target {
        &self.canonical_mutations
    }
}

impl PartialEq<CanonicalMutationCounts> for NonWatchIngestOutcome {
    fn eq(&self, other: &CanonicalMutationCounts) -> bool {
        self.canonical_mutations == *other
    }
}

pub(crate) const INDEX_PHASE_PREPARING: usize = 0;
pub(crate) const INDEX_PHASE_SCANNING: usize = 1;
pub(crate) const INDEX_PHASE_LEXICAL_INDEXING: usize = 2;
pub(crate) const INDEX_PHASE_SEMANTIC_INITIALIZE: usize = 3;
pub(crate) const INDEX_PHASE_SEMANTIC_REPLAY: usize = 4;
pub(crate) const INDEX_PHASE_SEMANTIC_EMBEDDING: usize = 5;
pub(crate) const INDEX_PHASE_SEMANTIC_VECTOR_PUBLISH: usize = 6;
pub(crate) const INDEX_PHASE_SEMANTIC_HNSW: usize = 7;
pub(crate) const INDEX_PHASE_SEMANTIC_MANIFEST: usize = 8;
pub(crate) const INDEX_PHASE_SEMANTIC_FINALIZE: usize = 9;
pub(crate) const INDEX_PHASE_ANALYTICS_REBUILD: usize = 10;

#[derive(Debug, Default)]
pub struct IndexingProgress {
    pub stop_requested: AtomicBool,
    /// Cancellation of an admitted preparation query is relayed to the
    /// engine's operation context. Its owner still drains the query and
    /// rollback before releasing storage or the index-run lock.
    storage_cancellation: crate::franken_sync::CancellationHandle,
    pub total: AtomicUsize,
    pub current: AtomicUsize,
    /// #332: monotonic work-liveness tick. Producer threads bump it once per
    /// parsed conversation (BEFORE batch publication) and the streaming
    /// consumer bumps it per received/persisted batch, so the stall watchdog
    /// can tell "actively parsing a large source artifact between coarse
    /// batch publications" apart from a genuine wedge even while
    /// `current`/`total` sit still.
    pub activity: std::sync::atomic::AtomicU64,
    /// #332: true when `total` is a final denominator for the current phase
    /// (known connector count, known conversation total, exact semantic
    /// counters); false while `total` is still "discovered so far" and can
    /// expand (streaming ingest accumulation). Snapshot consumers suppress
    /// ETA while the denominator is not final, because an expanding total
    /// makes ETA regress by hours and useless for scheduling.
    pub total_is_final: AtomicBool,
    // Phase indicator. Values 0-2 cover canonical/lexical work; 3-9 identify
    // the independently measured semantic pipeline stages above.
    pub phase: AtomicUsize,
    /// True only while the watch owner is waiting for events or its cooldown.
    /// A failed attempt can leave cumulative `current < total` after a later
    /// successful retry; those counters must not pretend the failed attempt
    /// completed just to distinguish an idle watcher from a blocked syscall.
    pub watch_idle: AtomicBool,
    pub is_rebuilding: AtomicBool,
    /// Set by `run_index` while it is inside the post-publish finalize window —
    /// specifically `close_storage_after_index`, which runs the (deferred,
    /// possibly multi-GB) bulk-ingest WAL checkpoint via a synchronous,
    /// `!Send` frankensqlite `conn.close()` + `wal_checkpoint(TRUNCATE)` on the
    /// indexer thread. That checkpoint cannot report progress and, on a large
    /// corpus (esp. macOS, where Darwin fsync/flock is slow), legitimately
    /// takes minutes. The stall watchdog reads this flag so it does not misread
    /// that active checkpoint as a #297 finalize wedge and kill the process
    /// (exit 70) mid-write — which strands the un-truncated WAL and leaves the
    /// canonical DB malformed to stock SQLite (#296/#321). Liveness is still
    /// bounded: see `index_finalize_abort_threshold` (#319).
    pub finalizing: AtomicBool,
    /// gh373 third variant (bead oeu5a): true while the streaming consumer is
    /// inside `persist::persist_conversations_batched*` for one ingest batch.
    /// A single giant conversation (observed: 60-90MB codex rollouts, ~40k
    /// messages) makes that call run for many minutes — allocator-heavy
    /// redaction in `map_to_internal_with_redactor` before the first WAL
    /// write — during which `current` cannot move and, before the
    /// per-message heartbeat existed, `activity` did not either. The stall
    /// watchdog grants this window the finalize-class abort grace as defense
    /// in depth (the per-message heartbeat is the primary liveness signal).
    pub persist_in_progress: AtomicBool,
    /// Number of coding agents discovered so far during scanning
    pub discovered_agents: AtomicUsize,
    /// Names of discovered agents (protected by mutex for concurrent access)
    pub discovered_agent_names: Mutex<Vec<String>>,
    /// Last error message from background indexer, if any
    pub last_error: Mutex<Option<String>>,
    /// Structured stats for JSON output (T7.4)
    pub stats: Mutex<IndexingStats>,
    /// Live authoritative rebuild queue depth for same-process progress output.
    pub rebuild_pipeline_queue_depth: AtomicUsize,
    /// Live authoritative rebuild in-flight byte budget usage.
    pub rebuild_pipeline_inflight_message_bytes: AtomicUsize,
    /// Conversations currently buffered in the sink-side pending batch.
    pub rebuild_pipeline_pending_batch_conversations: AtomicUsize,
    /// Message bytes currently buffered in the sink-side pending batch.
    pub rebuild_pipeline_pending_batch_message_bytes: AtomicUsize,
    /// Configured producer-side page-prep worker count for the active rebuild.
    pub rebuild_pipeline_page_prep_workers: AtomicUsize,
    /// Producer-side page-prep jobs currently queued or running in workers.
    pub rebuild_pipeline_active_page_prep_jobs: AtomicUsize,
    /// Prepared pages waiting behind the ordered handoff barrier.
    pub rebuild_pipeline_ordered_buffered_pages: AtomicUsize,
    /// Runtime budget generation observed by the rebuild pipeline.
    pub rebuild_pipeline_budget_generation: AtomicUsize,
    /// Producer-side waits on the in-flight byte budget.
    pub rebuild_pipeline_producer_budget_wait_count: AtomicUsize,
    /// Producer-side milliseconds spent waiting on the in-flight byte budget.
    pub rebuild_pipeline_producer_budget_wait_ms: AtomicUsize,
    /// Current park site of the rebuild page-production stage (#288):
    /// `listing_db` / `waiting_result` / `waiting_turn` / `waiting_budget` /
    /// `handoff_send` / `idle`. Empty when no rebuild pipeline is active.
    pub rebuild_pipeline_producer_state: Mutex<String>,
    /// Next page sequence the ordered reservation barrier will admit (#288).
    pub rebuild_pipeline_reservation_next_sequence: AtomicU64,
    /// Threads currently parked waiting on the in-flight byte budget (#288).
    pub rebuild_pipeline_producer_budget_active_waiters: AtomicUsize,
    /// Producer-side waits on bounded sink handoff.
    pub rebuild_pipeline_producer_handoff_wait_count: AtomicUsize,
    /// Producer-side milliseconds spent waiting on bounded sink handoff.
    pub rebuild_pipeline_producer_handoff_wait_ms: AtomicUsize,
    /// Sampled host 1-minute load average while the rebuild is active, in milli-loadavg units.
    pub rebuild_pipeline_host_loadavg_1m_milli: Mutex<Option<u32>>,
    /// Sampled host MemAvailable while the rebuild is active.
    pub rebuild_pipeline_host_available_memory_bytes: Mutex<Option<u64>>,
    /// Sampled process resident set size while the rebuild is active.
    pub rebuild_pipeline_process_rss_bytes: Mutex<Option<u64>>,
    /// Human-readable runtime controller mode for the active rebuild.
    pub rebuild_pipeline_controller_mode: Mutex<String>,
    /// Human-readable reason explaining the current controller mode.
    pub rebuild_pipeline_controller_reason: Mutex<String>,
    /// Maximum staged eager-merge workers available for the active rebuild.
    pub rebuild_pipeline_staged_merge_workers_max: AtomicUsize,
    /// Current staged eager-merge concurrency budget after adaptive throttling.
    pub rebuild_pipeline_staged_merge_allowed_jobs: AtomicUsize,
    /// Staged eager-merge jobs currently running.
    pub rebuild_pipeline_staged_merge_active_jobs: AtomicUsize,
    /// Ready staged merge artifacts waiting in the reduction frontier.
    pub rebuild_pipeline_staged_merge_ready_artifacts: AtomicUsize,
    /// Full staged merge groups currently ready to run.
    pub rebuild_pipeline_staged_merge_ready_groups: AtomicUsize,
    /// Human-readable reason explaining the current staged merge budget.
    pub rebuild_pipeline_staged_merge_controller_reason: Mutex<String>,
    /// Maximum staged shard-build workers available for the active rebuild.
    pub rebuild_pipeline_staged_shard_build_workers_max: AtomicUsize,
    /// Current staged shard-build concurrency budget after adaptive throttling.
    pub rebuild_pipeline_staged_shard_build_allowed_jobs: AtomicUsize,
    /// Staged shard-build jobs currently running.
    pub rebuild_pipeline_staged_shard_build_active_jobs: AtomicUsize,
    /// Staged shard-build jobs buffered behind the adaptive dispatch gate.
    pub rebuild_pipeline_staged_shard_build_pending_jobs: AtomicUsize,
    /// Human-readable reason explaining the current staged shard-build budget.
    pub rebuild_pipeline_staged_shard_build_controller_reason: Mutex<String>,
    /// Host memory reserve below which new staged shard builds are throttled.
    pub rebuild_pipeline_staged_shard_build_memory_reserve_bytes: AtomicUsize,
    /// Host memory reserve below which new staged shard builds are paused.
    pub rebuild_pipeline_staged_shard_build_emergency_memory_reserve_bytes: AtomicUsize,
    /// Completed staged shard-build jobs in the active rebuild.
    pub rebuild_pipeline_staged_shard_build_completed_jobs: AtomicUsize,
    /// Last completed staged shard index.
    pub rebuild_pipeline_staged_shard_build_last_shard_index: Mutex<Option<usize>>,
    /// Raw message bytes in the last completed staged shard.
    pub rebuild_pipeline_staged_shard_build_last_message_bytes: AtomicUsize,
    /// On-disk bytes in the last completed staged shard index.
    pub rebuild_pipeline_staged_shard_build_last_index_size_bytes: AtomicU64,
    /// Wall-clock milliseconds spent building the last staged shard.
    pub rebuild_pipeline_staged_shard_build_last_duration_ms: AtomicU64,
    /// Last observed index-size/raw-message-byte amplification in milli-units.
    pub rebuild_pipeline_staged_shard_build_last_amplification_milli: Mutex<Option<u64>>,
    /// Conservative observed amplification used by staged shard admission.
    pub rebuild_pipeline_staged_shard_build_observed_amplification_milli: Mutex<Option<u64>>,
}

#[derive(Debug)]
pub struct IndexInterrupted;

impl std::fmt::Display for IndexInterrupted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("indexing interrupted after committed batch; resume with cass index")
    }
}

impl std::error::Error for IndexInterrupted {}

/// A connector rejected a source after any successfully parsed neighbors were
/// committed. Keep the original cause without confusing its path with an
/// archive storage failure at the CLI boundary.
#[derive(Debug, thiserror::Error)]
#[error("one or more watch source scans failed; completed conversations were retained")]
pub(crate) struct ConnectorScanFailure(#[source] pub(in crate::indexer) anyhow::Error);

impl IndexingProgress {
    pub fn request_stop(&self) {
        self.stop_requested.store(true, Ordering::Release);
        self.storage_cancellation.cancel();
    }

    pub fn stop_requested(&self) -> bool {
        self.stop_requested.load(Ordering::Acquire)
    }

    pub(in crate::indexer) fn check_stop(&self) -> Result<()> {
        if self.stop_requested() {
            return Err(anyhow::Error::new(IndexInterrupted));
        }
        Ok(())
    }
    pub(crate) fn phase_label_for(phase: usize) -> &'static str {
        match phase {
            INDEX_PHASE_PREPARING => "preparing",
            INDEX_PHASE_SCANNING => "scanning",
            INDEX_PHASE_LEXICAL_INDEXING => "indexing",
            INDEX_PHASE_SEMANTIC_INITIALIZE => "semantic_initialize",
            INDEX_PHASE_SEMANTIC_REPLAY => "semantic_replay",
            INDEX_PHASE_SEMANTIC_EMBEDDING => "semantic_embedding",
            INDEX_PHASE_SEMANTIC_VECTOR_PUBLISH => "semantic_vector_publish",
            INDEX_PHASE_SEMANTIC_HNSW => "semantic_hnsw",
            INDEX_PHASE_SEMANTIC_MANIFEST => "semantic_manifest",
            INDEX_PHASE_SEMANTIC_FINALIZE => "semantic_finalize",
            INDEX_PHASE_ANALYTICS_REBUILD => "analytics_rebuild",
            _ => "preparing",
        }
    }

    pub(crate) fn phase_unit_for(phase: usize) -> &'static str {
        match phase {
            INDEX_PHASE_SCANNING => "connectors",
            INDEX_PHASE_LEXICAL_INDEXING | INDEX_PHASE_SEMANTIC_REPLAY => "conversations",
            INDEX_PHASE_SEMANTIC_EMBEDDING | INDEX_PHASE_ANALYTICS_REBUILD => "messages",
            INDEX_PHASE_SEMANTIC_VECTOR_PUBLISH | INDEX_PHASE_SEMANTIC_HNSW => "vectors",
            INDEX_PHASE_SEMANTIC_MANIFEST | INDEX_PHASE_SEMANTIC_FINALIZE => "steps",
            _ => "items",
        }
    }

    pub(crate) fn set_phase_progress(&self, phase: usize, current: usize, total: usize) {
        // Publish counters before the phase code so a polling observer never
        // sees a new phase paired with stale lexical totals.
        self.total.store(total, Ordering::Relaxed);
        self.current.store(current, Ordering::Relaxed);
        // Semantic pipeline stages publish exact denominators.
        self.total_is_final.store(true, Ordering::Relaxed);
        self.phase.store(phase, Ordering::Relaxed);
    }

    pub(crate) fn update_phase_progress(&self, current: usize, total: usize) {
        self.total.store(total, Ordering::Relaxed);
        self.current.store(current, Ordering::Relaxed);
    }

    /// #332: record one unit of real work (a parsed conversation, a received
    /// or persisted batch) without touching the published `current`/`total`
    /// counters. The stall watchdog treats an advancing tick as forward
    /// progress.
    pub fn tick_activity(&self) {
        self.activity
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Human-readable label for the current phase.
    /// See the `INDEX_PHASE_*` constants for the canonical phase taxonomy.
    pub fn phase_label(&self) -> &'static str {
        Self::phase_label_for(self.phase.load(Ordering::Relaxed))
    }

    /// True when the authoritative lexical rebuild pipeline holds no
    /// in-flight, pending, buffered, or parked work — i.e. nothing is left
    /// to do and nothing is actively trying to make progress. The stall
    /// watchdog uses this to distinguish a genuinely wedged post-publish
    /// finalize (#297: everything idle, yet the process never exits) from a
    /// legitimately slow-but-active sort/rebuild (#294), which must never be
    /// aborted.
    pub fn rebuild_pipeline_is_quiescent(&self) -> bool {
        let no_active_work = self.rebuild_pipeline_queue_depth.load(Ordering::Relaxed) == 0
            && self
                .rebuild_pipeline_inflight_message_bytes
                .load(Ordering::Relaxed)
                == 0
            && self
                .rebuild_pipeline_pending_batch_conversations
                .load(Ordering::Relaxed)
                == 0
            && self
                .rebuild_pipeline_pending_batch_message_bytes
                .load(Ordering::Relaxed)
                == 0
            && self
                .rebuild_pipeline_active_page_prep_jobs
                .load(Ordering::Relaxed)
                == 0
            && self
                .rebuild_pipeline_ordered_buffered_pages
                .load(Ordering::Relaxed)
                == 0
            && self
                .rebuild_pipeline_producer_budget_active_waiters
                .load(Ordering::Relaxed)
                == 0
            && self
                .rebuild_pipeline_staged_merge_active_jobs
                .load(Ordering::Relaxed)
                == 0
            && self
                .rebuild_pipeline_staged_merge_ready_artifacts
                .load(Ordering::Relaxed)
                == 0
            && self
                .rebuild_pipeline_staged_merge_ready_groups
                .load(Ordering::Relaxed)
                == 0
            && self
                .rebuild_pipeline_staged_shard_build_active_jobs
                .load(Ordering::Relaxed)
                == 0
            && self
                .rebuild_pipeline_staged_shard_build_pending_jobs
                .load(Ordering::Relaxed)
                == 0;
        if !no_active_work {
            return false;
        }
        // An empty producer state means no rebuild is active; "idle" means
        // the producer parked with nothing queued. Any other park site
        // (listing_db / waiting_result / waiting_turn / waiting_budget /
        // handoff_send) means it is still trying to make progress.
        self.rebuild_pipeline_producer_state
            .lock()
            .map(|state| state.is_empty() || state.as_str() == "idle")
            .unwrap_or(false)
    }

    /// Capture a JSON snapshot of the current progress state, suitable for
    /// NDJSON event streaming. `elapsed_ms` is the wall-clock elapsed since
    /// the index command started (caller's responsibility to track).
    pub fn snapshot_json(&self, elapsed_ms: u128) -> serde_json::Value {
        let phase = self.phase.load(Ordering::Relaxed);
        let total = self.total.load(Ordering::Relaxed);
        let current = self.current.load(Ordering::Relaxed);
        let agents = self.discovered_agents.load(Ordering::Relaxed);
        let is_rebuilding = self.is_rebuilding.load(Ordering::Relaxed);
        let finalizing = self.finalizing.load(Ordering::Relaxed);
        let persist_in_progress = self.persist_in_progress.load(Ordering::Relaxed);
        let agent_names: Vec<String> = self
            .discovered_agent_names
            .lock()
            .map(|g| g.clone())
            .unwrap_or_default();
        let last_error: Option<String> = self.last_error.lock().ok().and_then(|g| g.clone());
        let rebuild_pipeline_queue_depth =
            self.rebuild_pipeline_queue_depth.load(Ordering::Relaxed);
        let rebuild_pipeline_inflight_message_bytes = self
            .rebuild_pipeline_inflight_message_bytes
            .load(Ordering::Relaxed);
        let rebuild_pipeline_pending_batch_conversations = self
            .rebuild_pipeline_pending_batch_conversations
            .load(Ordering::Relaxed);
        let rebuild_pipeline_pending_batch_message_bytes = self
            .rebuild_pipeline_pending_batch_message_bytes
            .load(Ordering::Relaxed);
        let rebuild_pipeline_page_prep_workers = self
            .rebuild_pipeline_page_prep_workers
            .load(Ordering::Relaxed);
        let rebuild_pipeline_active_page_prep_jobs = self
            .rebuild_pipeline_active_page_prep_jobs
            .load(Ordering::Relaxed);
        let rebuild_pipeline_ordered_buffered_pages = self
            .rebuild_pipeline_ordered_buffered_pages
            .load(Ordering::Relaxed);
        let rebuild_pipeline_budget_generation = self
            .rebuild_pipeline_budget_generation
            .load(Ordering::Relaxed);
        let rebuild_pipeline_producer_budget_wait_count = self
            .rebuild_pipeline_producer_budget_wait_count
            .load(Ordering::Relaxed);
        let rebuild_pipeline_producer_budget_wait_ms = self
            .rebuild_pipeline_producer_budget_wait_ms
            .load(Ordering::Relaxed);
        let rebuild_pipeline_producer_state = self
            .rebuild_pipeline_producer_state
            .lock()
            .map(|value| value.clone())
            .unwrap_or_default();
        let rebuild_pipeline_reservation_next_sequence = self
            .rebuild_pipeline_reservation_next_sequence
            .load(Ordering::Relaxed);
        let rebuild_pipeline_producer_budget_active_waiters = self
            .rebuild_pipeline_producer_budget_active_waiters
            .load(Ordering::Relaxed);
        let rebuild_pipeline_producer_handoff_wait_count = self
            .rebuild_pipeline_producer_handoff_wait_count
            .load(Ordering::Relaxed);
        let rebuild_pipeline_producer_handoff_wait_ms = self
            .rebuild_pipeline_producer_handoff_wait_ms
            .load(Ordering::Relaxed);
        let rebuild_pipeline_host_loadavg_1m_milli = self
            .rebuild_pipeline_host_loadavg_1m_milli
            .lock()
            .ok()
            .and_then(|value| *value);
        let rebuild_pipeline_host_available_memory_bytes = self
            .rebuild_pipeline_host_available_memory_bytes
            .lock()
            .ok()
            .and_then(|value| *value);
        let rebuild_pipeline_process_rss_bytes = self
            .rebuild_pipeline_process_rss_bytes
            .lock()
            .ok()
            .and_then(|value| *value);
        let rebuild_pipeline_controller_mode = self
            .rebuild_pipeline_controller_mode
            .lock()
            .map(|value| value.clone())
            .unwrap_or_default();
        let rebuild_pipeline_controller_reason = self
            .rebuild_pipeline_controller_reason
            .lock()
            .map(|value| value.clone())
            .unwrap_or_default();
        let rebuild_pipeline_staged_merge_workers_max = self
            .rebuild_pipeline_staged_merge_workers_max
            .load(Ordering::Relaxed);
        let rebuild_pipeline_staged_merge_allowed_jobs = self
            .rebuild_pipeline_staged_merge_allowed_jobs
            .load(Ordering::Relaxed);
        let rebuild_pipeline_staged_merge_active_jobs = self
            .rebuild_pipeline_staged_merge_active_jobs
            .load(Ordering::Relaxed);
        let rebuild_pipeline_staged_merge_ready_artifacts = self
            .rebuild_pipeline_staged_merge_ready_artifacts
            .load(Ordering::Relaxed);
        let rebuild_pipeline_staged_merge_ready_groups = self
            .rebuild_pipeline_staged_merge_ready_groups
            .load(Ordering::Relaxed);
        let rebuild_pipeline_staged_merge_controller_reason = self
            .rebuild_pipeline_staged_merge_controller_reason
            .lock()
            .map(|value| value.clone())
            .unwrap_or_default();
        let rebuild_pipeline_staged_shard_build_workers_max = self
            .rebuild_pipeline_staged_shard_build_workers_max
            .load(Ordering::Relaxed);
        let rebuild_pipeline_staged_shard_build_allowed_jobs = self
            .rebuild_pipeline_staged_shard_build_allowed_jobs
            .load(Ordering::Relaxed);
        let rebuild_pipeline_staged_shard_build_active_jobs = self
            .rebuild_pipeline_staged_shard_build_active_jobs
            .load(Ordering::Relaxed);
        let rebuild_pipeline_staged_shard_build_pending_jobs = self
            .rebuild_pipeline_staged_shard_build_pending_jobs
            .load(Ordering::Relaxed);
        let rebuild_pipeline_staged_shard_build_controller_reason = self
            .rebuild_pipeline_staged_shard_build_controller_reason
            .lock()
            .map(|value| value.clone())
            .unwrap_or_default();
        let rebuild_pipeline_staged_shard_build_memory_reserve_bytes = self
            .rebuild_pipeline_staged_shard_build_memory_reserve_bytes
            .load(Ordering::Relaxed);
        let rebuild_pipeline_staged_shard_build_emergency_memory_reserve_bytes = self
            .rebuild_pipeline_staged_shard_build_emergency_memory_reserve_bytes
            .load(Ordering::Relaxed);
        let rebuild_pipeline_staged_shard_build_completed_jobs = self
            .rebuild_pipeline_staged_shard_build_completed_jobs
            .load(Ordering::Relaxed);
        let rebuild_pipeline_staged_shard_build_last_shard_index = self
            .rebuild_pipeline_staged_shard_build_last_shard_index
            .lock()
            .ok()
            .and_then(|value| *value);
        let rebuild_pipeline_staged_shard_build_last_message_bytes = self
            .rebuild_pipeline_staged_shard_build_last_message_bytes
            .load(Ordering::Relaxed);
        let rebuild_pipeline_staged_shard_build_last_index_size_bytes = self
            .rebuild_pipeline_staged_shard_build_last_index_size_bytes
            .load(Ordering::Relaxed);
        let rebuild_pipeline_staged_shard_build_last_duration_ms = self
            .rebuild_pipeline_staged_shard_build_last_duration_ms
            .load(Ordering::Relaxed);
        let rebuild_pipeline_staged_shard_build_last_amplification_milli = self
            .rebuild_pipeline_staged_shard_build_last_amplification_milli
            .lock()
            .ok()
            .and_then(|value| *value);
        let rebuild_pipeline_staged_shard_build_observed_amplification_milli = self
            .rebuild_pipeline_staged_shard_build_observed_amplification_milli
            .lock()
            .ok()
            .and_then(|value| *value);
        let (quarantined_conversations, lexical_update_deferred) = self
            .stats
            .lock()
            .map(|stats| {
                (
                    stats.quarantined_conversations,
                    stats.lexical_update_deferred,
                )
            })
            .unwrap_or_default();

        let total_is_final = self.total_is_final.load(Ordering::Relaxed);
        let activity = self.activity.load(std::sync::atomic::Ordering::Relaxed);

        // Derived rate + ETA for the indexing phase. Guard against divide-by-zero
        // and bogus values when `total` isn't set yet. #332: ETA is only
        // published against a FINAL denominator — while `total` is still
        // "discovered so far" an ETA computed from it regresses by hours as
        // discovery expands, so it is suppressed (rate stays available).
        let (rate_per_sec, eta_seconds) = if phase == 2 && elapsed_ms > 0 && current > 0 {
            let secs = (elapsed_ms as f64) / 1000.0;
            let rate = (current as f64) / secs.max(0.001);
            let remaining = total.saturating_sub(current) as f64;
            let eta = if rate > 0.0 && total > 0 && total_is_final {
                Some(remaining / rate)
            } else {
                None
            };
            (Some(rate), eta)
        } else {
            (None, None)
        };

        serde_json::json!({
            "phase": Self::phase_label_for(phase),
            "phase_code": phase,
            // #332: the unit `current`/`total` count in this phase, so
            // dashboards never mix source artifacts, conversations, and
            // vectors under one unlabeled shape.
            "unit": Self::phase_unit_for(phase),
            "total": total,
            // #332: false while `total` is a lazily expanding "discovered so
            // far" count; true once it is a stable denominator.
            "total_is_final": total_is_final,
            "current": current,
            // #332: monotonic work-liveness tick (parsed conversations +
            // received/persisted batches); advances during long parses even
            // when `current` cannot.
            "activity": activity,
            "discovered_agents": agents,
            "agent_names": agent_names,
            "is_rebuilding": is_rebuilding,
            // #319: true while the indexer is inside the post-publish finalize
            // WAL checkpoint (close_storage_after_index). A quiescent, phase-0,
            // current==total snapshot with `finalizing: true` is an active
            // final checkpoint, NOT a #297 wedge.
            "finalizing": finalizing,
            // gh373/oeu5a: true while one ingest batch is inside batched
            // persistence (redaction/map + writer transaction). A phase-2
            // snapshot with frozen counters but `persist_in_progress: true`
            // is an active giant-batch persist, not a wedge.
            "persist_in_progress": persist_in_progress,
            "elapsed_ms": elapsed_ms,
            "rate_per_sec": rate_per_sec,
            "eta_seconds": eta_seconds,
            "last_error": last_error,
            "quarantined_conversations": quarantined_conversations,
            "lexical_update_deferred": lexical_update_deferred,
            "rebuild_pipeline": {
                "queue_depth": rebuild_pipeline_queue_depth,
                "inflight_message_bytes": rebuild_pipeline_inflight_message_bytes,
                "pending_batch_conversations": rebuild_pipeline_pending_batch_conversations,
                "pending_batch_message_bytes": rebuild_pipeline_pending_batch_message_bytes,
                "page_prep_workers": rebuild_pipeline_page_prep_workers,
                "active_page_prep_jobs": rebuild_pipeline_active_page_prep_jobs,
                "ordered_buffered_pages": rebuild_pipeline_ordered_buffered_pages,
                "budget_generation": rebuild_pipeline_budget_generation,
                "producer_budget_wait_count": rebuild_pipeline_producer_budget_wait_count,
                "producer_budget_wait_ms": rebuild_pipeline_producer_budget_wait_ms,
                // #288 park-site telemetry: where the page-production stage is
                // currently blocked, which page sequence the ordered
                // reservation barrier admits next, and how many threads are
                // parked on the byte budget right now (the cumulative
                // producer_budget_wait_* counters above only advance AFTER a
                // wait succeeds, so they cannot identify a live park).
                "producer_state": non_empty_json_string(rebuild_pipeline_producer_state),
                "reservation_next_sequence": rebuild_pipeline_reservation_next_sequence,
                "producer_budget_active_waiters": rebuild_pipeline_producer_budget_active_waiters,
                "producer_handoff_wait_count": rebuild_pipeline_producer_handoff_wait_count,
                "producer_handoff_wait_ms": rebuild_pipeline_producer_handoff_wait_ms,
                "host_loadavg_1m": rebuild_pipeline_host_loadavg_1m_milli.map(|value| {
                    f64::from(value) / 1000.0
                }),
                "host_available_memory_bytes": rebuild_pipeline_host_available_memory_bytes,
                "process_rss_bytes": rebuild_pipeline_process_rss_bytes,
                "controller_mode": non_empty_json_string(rebuild_pipeline_controller_mode),
                "controller_reason": non_empty_json_string(rebuild_pipeline_controller_reason),
                // The staged-merge / staged-shard-build controllers
                // only run during the lexical rebuild pipeline. Outside
                // that window (incremental scan, post-rebuild small-N
                // pass, idle indexer) `capture_lexical_rebuild_pipeline_runtime`
                // emits its initialization defaults — i.e. literal zeros —
                // for every staged_* counter. Surfacing those zeros to
                // operators looks identical to the #196-class "broken to
                // zero workers_max" semaphore bug and triages exactly
                // wrong (see #201). Emit `null` instead when the rebuild
                // pipeline isn't the source of these values; consumers
                // can keep their existing "is the field set?" check
                // and the JSON shape remains stable.
                "staged_merge_workers_max": staged_field_or_null(is_rebuilding, rebuild_pipeline_staged_merge_workers_max),
                "staged_merge_allowed_jobs": staged_field_or_null(is_rebuilding, rebuild_pipeline_staged_merge_allowed_jobs),
                "staged_merge_active_jobs": staged_field_or_null(is_rebuilding, rebuild_pipeline_staged_merge_active_jobs),
                "staged_merge_ready_artifacts": staged_field_or_null(is_rebuilding, rebuild_pipeline_staged_merge_ready_artifacts),
                "staged_merge_ready_groups": staged_field_or_null(is_rebuilding, rebuild_pipeline_staged_merge_ready_groups),
                "staged_merge_controller_reason": active_rebuild_json_string(is_rebuilding, rebuild_pipeline_staged_merge_controller_reason),
                "staged_shard_build_workers_max": staged_field_or_null(is_rebuilding, rebuild_pipeline_staged_shard_build_workers_max),
                "staged_shard_build_allowed_jobs": staged_field_or_null(is_rebuilding, rebuild_pipeline_staged_shard_build_allowed_jobs),
                "staged_shard_build_active_jobs": staged_field_or_null(is_rebuilding, rebuild_pipeline_staged_shard_build_active_jobs),
                "staged_shard_build_pending_jobs": staged_field_or_null(is_rebuilding, rebuild_pipeline_staged_shard_build_pending_jobs),
                "staged_shard_build_controller_reason": active_rebuild_json_string(is_rebuilding, rebuild_pipeline_staged_shard_build_controller_reason),
                "staged_shard_build_memory_reserve_bytes": staged_field_or_null(is_rebuilding, rebuild_pipeline_staged_shard_build_memory_reserve_bytes),
                "staged_shard_build_emergency_memory_reserve_bytes": staged_field_or_null(is_rebuilding, rebuild_pipeline_staged_shard_build_emergency_memory_reserve_bytes),
                "staged_shard_build_completed_jobs": staged_field_or_null(is_rebuilding, rebuild_pipeline_staged_shard_build_completed_jobs),
                "staged_shard_build_last_shard_index": active_rebuild_json_optional_usize(is_rebuilding, rebuild_pipeline_staged_shard_build_last_shard_index),
                "staged_shard_build_last_message_bytes": staged_field_or_null(is_rebuilding, rebuild_pipeline_staged_shard_build_last_message_bytes),
                "staged_shard_build_last_index_size_bytes": staged_u64_field_or_null(is_rebuilding, rebuild_pipeline_staged_shard_build_last_index_size_bytes),
                "staged_shard_build_last_duration_ms": staged_u64_field_or_null(is_rebuilding, rebuild_pipeline_staged_shard_build_last_duration_ms),
                "staged_shard_build_last_amplification_milli": active_rebuild_json_optional_u64(is_rebuilding, rebuild_pipeline_staged_shard_build_last_amplification_milli),
                "staged_shard_build_observed_amplification_milli": active_rebuild_json_optional_u64(is_rebuilding, rebuild_pipeline_staged_shard_build_observed_amplification_milli),
            },
        })
    }
}

pub(in crate::indexer) fn check_legacy_omp_analytics_stop(
    progress: Option<&Arc<IndexingProgress>>,
) -> Result<()> {
    if progress.is_some_and(|progress| progress.stop_requested()) {
        return Err(anyhow::Error::new(IndexInterrupted));
    }
    Ok(())
}

pub(in crate::indexer) fn report_legacy_omp_analytics_progress(
    progress: Option<&Arc<IndexingProgress>>,
    processed: i64,
    total: i64,
) {
    if let Some(progress) = progress {
        let processed = usize::try_from(processed.max(0)).unwrap_or(usize::MAX);
        let total = usize::try_from(total.max(0)).unwrap_or(usize::MAX);
        progress.set_phase_progress(INDEX_PHASE_ANALYTICS_REBUILD, processed, total);
        if processed == 0 && total == 0 {
            // The aggregate-only phases do not expose a message denominator.
            // Do not leave the preceding message pass looking 100% complete.
            progress.total_is_final.store(false, Ordering::Relaxed);
        }
    }
}

/// Helper for the JSON snapshot above. Returns the staged-pipeline
/// counter as a JSON number when the lexical rebuild pipeline is
/// actually running, and `null` otherwise — so a stalled incremental
/// scan no longer surfaces the post-init zeros and trick operators
/// into chasing a non-existent worker-pool sizing bug (issue #201).
pub(in crate::indexer) fn staged_field_or_null(
    is_rebuilding: bool,
    value: usize,
) -> serde_json::Value {
    if is_rebuilding {
        serde_json::json!(value)
    } else {
        serde_json::Value::Null
    }
}

pub(in crate::indexer) fn staged_u64_field_or_null(
    is_rebuilding: bool,
    value: u64,
) -> serde_json::Value {
    if is_rebuilding {
        serde_json::json!(value)
    } else {
        serde_json::Value::Null
    }
}

pub(in crate::indexer) fn non_empty_json_string(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

pub(in crate::indexer) fn active_rebuild_json_string(
    is_rebuilding: bool,
    value: String,
) -> Option<String> {
    (is_rebuilding && !value.is_empty()).then_some(value)
}

pub(in crate::indexer) fn active_rebuild_json_optional_usize(
    is_rebuilding: bool,
    value: Option<usize>,
) -> serde_json::Value {
    if is_rebuilding {
        serde_json::json!(value)
    } else {
        serde_json::Value::Null
    }
}

pub(in crate::indexer) fn active_rebuild_json_optional_u64(
    is_rebuilding: bool,
    value: Option<u64>,
) -> serde_json::Value {
    if is_rebuilding {
        serde_json::json!(value)
    } else {
        serde_json::Value::Null
    }
}

#[derive(Clone)]
pub struct IndexOptions {
    pub full: bool,
    pub force_rebuild: bool,
    pub watch: bool,
    /// One-shot watch hook: when set, `watch_sources` will bypass notify and invoke reindex for these paths once.
    pub watch_once_paths: Option<Vec<PathBuf>>,
    pub db_path: PathBuf,
    pub data_dir: PathBuf,
    /// Build semantic vector index after text indexing.
    pub semantic: bool,
    /// Build HNSW index for approximate nearest neighbor search (requires semantic).
    pub build_hnsw: bool,
    /// Embedder ID to use for semantic indexing (hash, fastembed).
    pub embedder: String,
    pub progress: Option<Arc<IndexingProgress>>,
    /// Minimum interval (in seconds) between watch scan cycles. Prevents tight-loop
    /// CPU burn when filesystem events arrive continuously. Default: 30.
    pub watch_interval_secs: u64,
}

pub fn set_robot_trace_ingest_enabled(enabled: bool) -> bool {
    let previous = ROBOT_TRACE_INGEST_ENABLED.swap(enabled, Ordering::Relaxed);
    if enabled {
        ROBOT_TRACE_INGEST_BATCH_N.store(0, Ordering::Relaxed);
    }
    let _ = crate::storage::sqlite::set_message_lookup_trace_enabled(enabled);
    previous
}

#[derive(Debug)]
pub(in crate::indexer) struct RobotIngestTraceSpan {
    pub(in crate::indexer) batch_n: u64,
    pub(in crate::indexer) stage: &'static str,
    pub(in crate::indexer) lexical_strategy: &'static str,
    pub(in crate::indexer) defer_checkpoints: bool,
    pub(in crate::indexer) batch_conversations: usize,
    pub(in crate::indexer) batch_msgs: usize,
    pub(in crate::indexer) started: Instant,
    pub(in crate::indexer) lookup_before: crate::storage::sqlite::MessageLookupTraceCounters,
}

pub(in crate::indexer) fn robot_trace_ingest_start(
    stage: &'static str,
    convs: &[NormalizedConversation],
    lexical_strategy: LexicalPopulationStrategy,
    defer_checkpoints: bool,
) -> Option<RobotIngestTraceSpan> {
    if !ROBOT_TRACE_INGEST_ENABLED.load(Ordering::Relaxed) {
        return None;
    }
    let batch_n = ROBOT_TRACE_INGEST_BATCH_N
        .fetch_add(1, Ordering::Relaxed)
        .saturating_add(1);
    Some(RobotIngestTraceSpan {
        batch_n,
        stage,
        lexical_strategy: lexical_strategy.as_str(),
        defer_checkpoints,
        batch_conversations: convs.len(),
        batch_msgs: convs.iter().map(|conv| conv.messages.len()).sum(),
        started: Instant::now(),
        lookup_before: crate::storage::sqlite::message_lookup_trace_snapshot(),
    })
}

pub(in crate::indexer) fn robot_trace_ingest_finish(
    span: Option<RobotIngestTraceSpan>,
    status: &str,
    inserted_conversations: usize,
    inserted_messages: usize,
    error: Option<&anyhow::Error>,
) {
    let Some(span) = span else {
        return;
    };
    let lookup_delta =
        crate::storage::sqlite::message_lookup_trace_snapshot().saturating_sub(span.lookup_before);
    let mut payload = serde_json::json!({
        "event": "ingest_batch",
        "ts_ms": chrono::Utc::now().timestamp_millis(),
        "batch_n": span.batch_n,
        "stage": span.stage,
        "status": status,
        "batch_conversations": span.batch_conversations,
        "batch_msgs": span.batch_msgs,
        "inserted_conversations": inserted_conversations,
        "inserted_messages": inserted_messages,
        "wall_ms": span.started.elapsed().as_millis() as u64,
        "lexical_strategy": span.lexical_strategy,
        "defer_checkpoints": span.defer_checkpoints,
        "lookups_against_global": lookup_delta.lookups_against_global(),
        "lookup_trace": lookup_delta,
    });
    if let Some(error) = error
        && let serde_json::Value::Object(ref mut map) = payload
    {
        map.insert("error".to_string(), serde_json::json!(error.to_string()));
    }
    if let Ok(line) = serde_json::to_string(&payload) {
        eprintln!("{line}");
    }
}

impl IndexingProgress {
    pub(in crate::indexer) fn enter_storage_cancellation(
        &self,
    ) -> crate::franken_sync::CancellationScope {
        self.storage_cancellation.enter()
    }
}
