use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LexicalPopulationStrategy {
    IncrementalInline,
    InlineRebuildFromScan,
    DeferredAuthoritativeDbRebuild,
}

pub(in crate::indexer) const DEFAULT_INCREMENTAL_AUTHORITATIVE_LEXICAL_REPAIR_MAX_DB_BYTES: u64 =
    1024 * 1024 * 1024;
pub(in crate::indexer) const DEFAULT_INCREMENTAL_MISSING_WATERMARK_FULL_SCAN_MAX_DB_BYTES: u64 =
    1024 * 1024 * 1024;
pub(in crate::indexer) const DEFERRED_LARGE_INCREMENTAL_LEXICAL_REPAIR_REASON: &str = "large_populated_incremental_index_defers_authoritative_lexical_repair_until_explicit_full_or_force_rebuild";
pub(in crate::indexer) const BOOTSTRAP_LARGE_INCREMENTAL_MISSING_WATERMARK_REASON: &str =
    "large_populated_incremental_index_bootstraps_missing_scan_watermark";
pub(in crate::indexer) const EMPTY_ARCHIVE_LEXICAL_REBUILD_REASON: &str =
    "empty_archive_defers_lexical_rebuild_to_authoritative_db_rebuild";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::indexer) struct DeferredIncrementalCanonicalLexicalRepair {
    pub(in crate::indexer) canonical_conversations: usize,
    pub(in crate::indexer) db_size_bytes: u64,
    pub(in crate::indexer) max_automatic_repair_db_size_bytes: u64,
    pub(in crate::indexer) observed_tantivy_docs: Option<usize>,
    pub(in crate::indexer) reason: &'static str,
}

impl LexicalPopulationStrategy {
    pub(in crate::indexer) fn as_str(self) -> &'static str {
        match self {
            Self::IncrementalInline => "incremental_inline",
            Self::InlineRebuildFromScan => "inline_rebuild_from_scan",
            Self::DeferredAuthoritativeDbRebuild => "deferred_authoritative_db_rebuild",
        }
    }
}

pub(in crate::indexer) fn select_lexical_population_strategy(
    needs_rebuild: bool,
    defer_to_authoritative_db_rebuild: bool,
) -> LexicalPopulationStrategy {
    if defer_to_authoritative_db_rebuild {
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild
    } else if needs_rebuild {
        LexicalPopulationStrategy::InlineRebuildFromScan
    } else {
        LexicalPopulationStrategy::IncrementalInline
    }
}

pub(in crate::indexer) fn resolve_lexical_population_strategy(
    needs_rebuild: bool,
    full_refresh: bool,
    salvage_messages_imported: usize,
) -> (LexicalPopulationStrategy, &'static str) {
    let defer_to_authoritative_db_rebuild = full_refresh || salvage_messages_imported > 0;
    let strategy =
        select_lexical_population_strategy(needs_rebuild, defer_to_authoritative_db_rebuild);
    let reason = if salvage_messages_imported > 0 {
        "historical_salvage_imported_messages_require_authoritative_db_rebuild"
    } else if full_refresh {
        "full_refresh_defers_inline_lexical_writes_to_authoritative_db_rebuild"
    } else if needs_rebuild {
        "lexical_index_needs_rebuild_so_scan_results_repopulate_tantivy_directly"
    } else {
        "incremental_scan_applies_inline_lexical_updates_only_for_new_messages"
    };
    (strategy, reason)
}

pub(in crate::indexer) fn lexical_population_strategy_requires_inline_tantivy(
    strategy: LexicalPopulationStrategy,
) -> bool {
    !matches!(
        strategy,
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild
    )
}

pub(in crate::indexer) fn non_watch_ingest_chunk_size() -> usize {
    match dotenvy::var("CASS_NON_WATCH_INGEST_CHUNK_SIZE")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
    {
        Some(0) => NON_WATCH_INGEST_DEFAULT_CHUNK_SIZE,
        Some(value) if value > NON_WATCH_INGEST_CHUNK_SIZE_MAX => {
            tracing::warn!(
                env_var = "CASS_NON_WATCH_INGEST_CHUNK_SIZE",
                requested = value,
                cap = NON_WATCH_INGEST_CHUNK_SIZE_MAX,
                "non-watch ingest chunk size exceeds safe cap; clamping"
            );
            NON_WATCH_INGEST_CHUNK_SIZE_MAX
        }
        Some(value) => value,
        None => NON_WATCH_INGEST_DEFAULT_CHUNK_SIZE,
    }
}

pub(crate) fn incremental_authoritative_lexical_repair_max_db_bytes() -> u64 {
    dotenvy::var("CASS_INCREMENTAL_AUTHORITATIVE_LEXICAL_REPAIR_MAX_DB_BYTES")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_INCREMENTAL_AUTHORITATIVE_LEXICAL_REPAIR_MAX_DB_BYTES)
}

pub(in crate::indexer) fn force_incremental_authoritative_lexical_repair_enabled() -> bool {
    dotenvy_truthy("CASS_INCREMENTAL_AUTHORITATIVE_LEXICAL_REPAIR")
}

pub(in crate::indexer) fn incremental_missing_watermark_full_scan_max_db_bytes() -> u64 {
    dotenvy::var("CASS_INCREMENTAL_MISSING_WATERMARK_FULL_SCAN_MAX_DB_BYTES")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_INCREMENTAL_MISSING_WATERMARK_FULL_SCAN_MAX_DB_BYTES)
}

pub(in crate::indexer) fn force_incremental_missing_watermark_full_scan_enabled() -> bool {
    dotenvy_truthy("CASS_INCREMENTAL_MISSING_WATERMARK_FULL_SCAN")
}

pub(in crate::indexer) fn is_plain_populated_incremental_index_run(
    opts: &IndexOptions,
    canonical_conversations: usize,
) -> bool {
    canonical_conversations > 0
        && !opts.full
        && !opts.force_rebuild
        && !opts.watch
        && !opts.semantic
        && !opts.build_hnsw
        && opts
            .watch_once_paths
            .as_ref()
            .is_none_or(|paths| paths.is_empty())
}

pub(crate) fn db_size_bytes_for_incremental_lexical_repair_policy(db_path: &Path) -> u64 {
    std::fs::metadata(db_path)
        .map(|metadata| metadata.len())
        .unwrap_or(0)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::indexer) struct BootstrappedIncrementalScanWatermark {
    pub(in crate::indexer) canonical_conversations: usize,
    pub(in crate::indexer) db_size_bytes: u64,
    pub(in crate::indexer) max_automatic_full_scan_db_size_bytes: u64,
    pub(in crate::indexer) reason: &'static str,
}

pub(in crate::indexer) fn should_bootstrap_missing_incremental_scan_watermark(
    opts: &IndexOptions,
    canonical_storage_rebuilt: bool,
    canonical_conversations: usize,
    needs_rebuild: bool,
    retry_stale_index_ingest_quarantine: bool,
    last_scan_ts: Option<i64>,
) -> Option<BootstrappedIncrementalScanWatermark> {
    if canonical_storage_rebuilt
        || needs_rebuild
        || retry_stale_index_ingest_quarantine
        || last_scan_ts.is_some()
        || !is_plain_populated_incremental_index_run(opts, canonical_conversations)
        || force_incremental_missing_watermark_full_scan_enabled()
    {
        return None;
    }

    let db_size_bytes = db_size_bytes_for_incremental_lexical_repair_policy(&opts.db_path);
    let max_automatic_full_scan_db_size_bytes =
        incremental_missing_watermark_full_scan_max_db_bytes();
    if db_size_bytes <= max_automatic_full_scan_db_size_bytes {
        return None;
    }

    Some(BootstrappedIncrementalScanWatermark {
        canonical_conversations,
        db_size_bytes,
        max_automatic_full_scan_db_size_bytes,
        reason: BOOTSTRAP_LARGE_INCREMENTAL_MISSING_WATERMARK_REASON,
    })
}

pub(in crate::indexer) fn should_defer_incremental_authoritative_lexical_repair(
    opts: &IndexOptions,
    canonical_storage_rebuilt: bool,
    canonical_conversations: usize,
    authoritative_rebuild_required: bool,
    observed_tantivy_docs: Option<usize>,
) -> Option<DeferredIncrementalCanonicalLexicalRepair> {
    if canonical_storage_rebuilt
        || !authoritative_rebuild_required
        || !is_plain_populated_incremental_index_run(opts, canonical_conversations)
        || force_incremental_authoritative_lexical_repair_enabled()
    {
        return None;
    }

    let db_size_bytes = db_size_bytes_for_incremental_lexical_repair_policy(&opts.db_path);
    let max_automatic_repair_db_size_bytes =
        incremental_authoritative_lexical_repair_max_db_bytes();
    if db_size_bytes <= max_automatic_repair_db_size_bytes {
        return None;
    }

    Some(DeferredIncrementalCanonicalLexicalRepair {
        canonical_conversations,
        db_size_bytes,
        max_automatic_repair_db_size_bytes,
        observed_tantivy_docs,
        reason: DEFERRED_LARGE_INCREMENTAL_LEXICAL_REPAIR_REASON,
    })
}

pub(in crate::indexer) fn record_lexical_population_strategy(
    progress: Option<&Arc<IndexingProgress>>,
    strategy: LexicalPopulationStrategy,
    reason: &str,
) {
    let Some(progress) = progress else {
        return;
    };
    if let Ok(mut stats) = progress.stats.lock() {
        stats.lexical_strategy = Some(strategy.as_str().to_string());
        stats.lexical_strategy_reason = Some(reason.to_string());
    }
}

pub(in crate::indexer) fn record_lexical_population_strategy_if_unset(
    progress: Option<&Arc<IndexingProgress>>,
    strategy: LexicalPopulationStrategy,
    reason: &str,
) {
    let Some(progress) = progress else {
        return;
    };
    if let Ok(mut stats) = progress.stats.lock()
        && stats.lexical_strategy.is_none()
    {
        stats.lexical_strategy = Some(strategy.as_str().to_string());
        stats.lexical_strategy_reason = Some(reason.to_string());
    }
}

pub(in crate::indexer) fn record_semantic_watch_once_stats(
    progress: Option<&Arc<IndexingProgress>>,
    stats: SemanticWatchOnceStats,
) {
    let Some(progress) = progress else {
        return;
    };
    if let Ok(mut indexing_stats) = progress.stats.lock() {
        indexing_stats.semantic_watch_once = Some(stats);
    }
}

pub(in crate::indexer) fn record_incremental_canonical_lexical_repair(
    progress: Option<&Arc<IndexingProgress>>,
    plan: &IncrementalCanonicalLexicalRepairPlan,
    canonical_conversations: usize,
) {
    let Some(progress) = progress else {
        return;
    };
    if let Ok(mut stats) = progress.stats.lock() {
        stats.lexical_repair = Some(LexicalRepairStats {
            kind: "authoritative_canonical_db_rebuild".to_string(),
            reason: plan.reason.to_string(),
            canonical_conversations,
            canonical_messages: plan.canonical_messages,
            observed_tantivy_docs: plan.observed_tantivy_docs,
        });
    }
}

pub(in crate::indexer) fn record_deferred_incremental_canonical_lexical_repair(
    progress: Option<&Arc<IndexingProgress>>,
    deferred: &DeferredIncrementalCanonicalLexicalRepair,
) {
    let Some(progress) = progress else {
        return;
    };
    if let Ok(mut stats) = progress.stats.lock() {
        stats.lexical_repair = Some(LexicalRepairStats {
            kind: "deferred_authoritative_canonical_db_rebuild".to_string(),
            reason: deferred.reason.to_string(),
            canonical_conversations: deferred.canonical_conversations,
            canonical_messages: 0,
            observed_tantivy_docs: deferred.observed_tantivy_docs,
        });
        stats.lexical_update_deferred = true;
    }
}

pub(in crate::indexer) fn reset_progress_to_idle(progress: Option<&Arc<IndexingProgress>>) {
    let Some(progress) = progress else {
        return;
    };

    progress.phase.store(0, Ordering::Relaxed);
    progress.is_rebuilding.store(false, Ordering::Relaxed);
    progress
        .rebuild_pipeline_queue_depth
        .store(0, Ordering::Relaxed);
    progress
        .rebuild_pipeline_inflight_message_bytes
        .store(0, Ordering::Relaxed);
    progress
        .rebuild_pipeline_pending_batch_conversations
        .store(0, Ordering::Relaxed);
    progress
        .rebuild_pipeline_pending_batch_message_bytes
        .store(0, Ordering::Relaxed);
    progress
        .rebuild_pipeline_page_prep_workers
        .store(0, Ordering::Relaxed);
    progress
        .rebuild_pipeline_active_page_prep_jobs
        .store(0, Ordering::Relaxed);
    progress
        .rebuild_pipeline_ordered_buffered_pages
        .store(0, Ordering::Relaxed);
    progress
        .rebuild_pipeline_budget_generation
        .store(0, Ordering::Relaxed);
    progress
        .rebuild_pipeline_producer_budget_wait_count
        .store(0, Ordering::Relaxed);
    progress
        .rebuild_pipeline_producer_budget_wait_ms
        .store(0, Ordering::Relaxed);
    if let Ok(mut producer_state) = progress.rebuild_pipeline_producer_state.lock() {
        producer_state.clear();
    }
    progress
        .rebuild_pipeline_reservation_next_sequence
        .store(0, Ordering::Relaxed);
    progress
        .rebuild_pipeline_producer_budget_active_waiters
        .store(0, Ordering::Relaxed);
    progress
        .rebuild_pipeline_producer_handoff_wait_count
        .store(0, Ordering::Relaxed);
    progress
        .rebuild_pipeline_producer_handoff_wait_ms
        .store(0, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_merge_workers_max
        .store(0, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_merge_allowed_jobs
        .store(0, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_merge_active_jobs
        .store(0, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_merge_ready_artifacts
        .store(0, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_merge_ready_groups
        .store(0, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_shard_build_workers_max
        .store(0, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_shard_build_allowed_jobs
        .store(0, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_shard_build_active_jobs
        .store(0, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_shard_build_pending_jobs
        .store(0, Ordering::Relaxed);
    if let Ok(mut host_loadavg) = progress.rebuild_pipeline_host_loadavg_1m_milli.lock() {
        *host_loadavg = None;
    }
    if let Ok(mut controller_mode) = progress.rebuild_pipeline_controller_mode.lock() {
        controller_mode.clear();
    }
    if let Ok(mut controller_reason) = progress.rebuild_pipeline_controller_reason.lock() {
        controller_reason.clear();
    }
    if let Ok(mut staged_merge_reason) = progress
        .rebuild_pipeline_staged_merge_controller_reason
        .lock()
    {
        staged_merge_reason.clear();
    }
    if let Ok(mut staged_shard_build_reason) = progress
        .rebuild_pipeline_staged_shard_build_controller_reason
        .lock()
    {
        staged_shard_build_reason.clear();
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::indexer) struct LexicalRebuildPipelineSinkRuntimeSnapshot {
    pub(in crate::indexer) queue_depth: usize,
    pub(in crate::indexer) pending_batch_conversations: usize,
    pub(in crate::indexer) pending_batch_message_bytes: usize,
}

impl LexicalRebuildPipelineSinkRuntimeSnapshot {
    pub(in crate::indexer) fn new(
        queue_depth: usize,
        pending_batch_conversations: usize,
        pending_batch_message_bytes: usize,
    ) -> Self {
        Self {
            queue_depth,
            pending_batch_conversations,
            pending_batch_message_bytes,
        }
    }
}

pub(in crate::indexer) fn capture_lexical_rebuild_pipeline_runtime(
    flow_limiter: &StreamingByteLimiter,
    producer_telemetry: Option<&LexicalRebuildProducerTelemetry>,
    responsiveness_controller: Option<&LexicalRebuildResponsivenessController>,
    budget_generation: usize,
    sink_runtime: LexicalRebuildPipelineSinkRuntimeSnapshot,
) -> LexicalRebuildPipelineRuntimeSnapshot {
    let producer_snapshot = producer_telemetry
        .map(LexicalRebuildProducerTelemetry::snapshot)
        .unwrap_or_default();
    let (controller_mode, controller_reason) = responsiveness_controller
        .map(|controller| {
            (
                controller.mode().to_string(),
                controller.reason().to_string(),
            )
        })
        .unwrap_or_default();
    LexicalRebuildPipelineRuntimeSnapshot {
        queue_depth: sink_runtime.queue_depth,
        inflight_message_bytes: flow_limiter.bytes_in_flight(),
        max_message_bytes_in_flight: flow_limiter.max_bytes_in_flight(),
        pending_batch_conversations: sink_runtime.pending_batch_conversations,
        pending_batch_message_bytes: sink_runtime.pending_batch_message_bytes,
        page_prep_workers: producer_snapshot.page_prep_workers,
        active_page_prep_jobs: producer_snapshot.active_page_prep_jobs,
        ordered_buffered_pages: producer_snapshot.ordered_buffered_pages,
        budget_generation,
        producer_budget_wait_count: producer_snapshot.budget_wait_count,
        producer_budget_wait_ms: producer_snapshot.budget_wait_ms,
        producer_budget_waiter_count: flow_limiter.active_waiter_count(),
        producer_state: producer_telemetry
            .map(|_| {
                LexicalRebuildProducerParkSite::label_for(producer_snapshot.producer_state)
                    .to_string()
            })
            .unwrap_or_default(),
        reservation_next_sequence: producer_snapshot.reservation_next_sequence,
        producer_handoff_wait_count: producer_snapshot.handoff_wait_count,
        producer_handoff_wait_ms: producer_snapshot.handoff_wait_ms,
        host_loadavg_1m_milli: lexical_rebuild_host_loadavg_1m_milli(),
        host_available_memory_bytes: responsiveness::available_memory_bytes(),
        process_rss_bytes: responsiveness::process_resident_memory_bytes(),
        controller_mode,
        controller_reason,
        staged_merge_workers_max: 0,
        staged_merge_allowed_jobs: 0,
        staged_merge_active_jobs: 0,
        staged_merge_ready_artifacts: 0,
        staged_merge_ready_groups: 0,
        staged_merge_controller_reason: String::new(),
        staged_shard_build_workers_max: 0,
        staged_shard_build_allowed_jobs: 0,
        staged_shard_build_active_jobs: 0,
        staged_shard_build_pending_jobs: 0,
        staged_shard_build_controller_reason: String::new(),
        staged_shard_build_memory_reserve_bytes: 0,
        staged_shard_build_emergency_memory_reserve_bytes: 0,
        staged_shard_build_completed_jobs: 0,
        staged_shard_build_last_shard_index: None,
        staged_shard_build_last_message_bytes: 0,
        staged_shard_build_last_index_size_bytes: 0,
        staged_shard_build_last_duration_ms: 0,
        staged_shard_build_last_amplification_milli: None,
        staged_shard_build_observed_amplification_milli: None,
        updated_at_ms: FrankenStorage::now_millis(),
    }
}

/// #366: a producer park-site transition counts as forward progress only
/// when at least one side is a working site. Flapping between the three
/// waiting sites (`waiting_result`/`waiting_turn`/`waiting_budget`) is
/// scheduler churn a livelocked pipeline can sustain forever; it must not
/// keep resetting the stall clock. Real progress out of a waiting state
/// (a result arriving, a handoff, a new listing pass) also moves a job or
/// queue counter, which the caller checks independently.
pub(in crate::indexer) fn producer_state_transition_is_work(previous: &str, current: &str) -> bool {
    previous != current && !(previous.starts_with("waiting_") && current.starts_with("waiting_"))
}

/// GH #446: the stall watchdog's liveness signal, shaped for the Quill sink
/// (`quill_bridge::EngineLivenessProbe`). `None` when the run has no progress
/// tracker, in which case the sink stays silent as before.
pub(in crate::indexer) fn lexical_index_heartbeat(
    progress: Option<&Arc<IndexingProgress>>,
) -> Option<crate::search::quill_bridge::EngineHeartbeat> {
    progress.map(|progress| {
        let progress = Arc::clone(progress);
        Arc::new(move || progress.tick_activity()) as crate::search::quill_bridge::EngineHeartbeat
    })
}

pub(in crate::indexer) fn install_lexical_index_heartbeat(
    t_index: &mut TantivyIndex,
    progress: Option<&Arc<IndexingProgress>>,
) {
    t_index.set_heartbeat(lexical_index_heartbeat(progress));
}

/// Open the live lexical index with the stall watchdog's liveness signal
/// already routed into its sink (GH #446).
pub(in crate::indexer) fn open_lexical_index_with_heartbeat(
    index_path: &Path,
    progress: Option<&Arc<IndexingProgress>>,
) -> Result<TantivyIndex> {
    let mut t_index = TantivyIndex::open_or_create(index_path)?;
    install_lexical_index_heartbeat(&mut t_index, progress);
    Ok(t_index)
}

/// #366: true when the pipeline runtime snapshot moved on a field that only
/// changes with forward progress — pages flowing, prep/build/merge jobs
/// starting or finishing, the producer moving between park sites. Pure wait
/// counters/durations, host metrics, controller mode/reason, budget
/// generation, and waiting↔waiting park flaps are deliberately excluded:
/// they keep changing while the pipeline is genuinely wedged and must not
/// defeat the stall watchdog.
pub(in crate::indexer) fn lexical_rebuild_pipeline_work_advanced(
    previous: &LexicalRebuildPipelineRuntimeSnapshot,
    current: &LexicalRebuildPipelineRuntimeSnapshot,
) -> bool {
    previous.queue_depth != current.queue_depth
        || previous.inflight_message_bytes != current.inflight_message_bytes
        || previous.pending_batch_conversations != current.pending_batch_conversations
        || previous.pending_batch_message_bytes != current.pending_batch_message_bytes
        || previous.active_page_prep_jobs != current.active_page_prep_jobs
        || previous.ordered_buffered_pages != current.ordered_buffered_pages
        || previous.reservation_next_sequence != current.reservation_next_sequence
        || producer_state_transition_is_work(&previous.producer_state, &current.producer_state)
        || previous.staged_merge_active_jobs != current.staged_merge_active_jobs
        || previous.staged_merge_ready_artifacts != current.staged_merge_ready_artifacts
        || previous.staged_merge_ready_groups != current.staged_merge_ready_groups
        || previous.staged_shard_build_active_jobs != current.staged_shard_build_active_jobs
        || previous.staged_shard_build_pending_jobs != current.staged_shard_build_pending_jobs
        || previous.staged_shard_build_completed_jobs != current.staged_shard_build_completed_jobs
        || previous.staged_shard_build_last_shard_index
            != current.staged_shard_build_last_shard_index
}

pub(in crate::indexer) fn refresh_lexical_rebuild_pipeline_runtime(
    latest_runtime: &mut LexicalRebuildPipelineRuntimeSnapshot,
    progress: Option<&Arc<IndexingProgress>>,
    flow_limiter: &StreamingByteLimiter,
    producer_telemetry: Option<&LexicalRebuildProducerTelemetry>,
    responsiveness_controller: Option<&LexicalRebuildResponsivenessController>,
    budget_generation: usize,
    sink_runtime: LexicalRebuildPipelineSinkRuntimeSnapshot,
) {
    let previous_runtime = std::mem::replace(
        latest_runtime,
        capture_lexical_rebuild_pipeline_runtime(
            flow_limiter,
            producer_telemetry,
            responsiveness_controller,
            budget_generation,
            sink_runtime,
        ),
    );
    let Some(progress) = progress else {
        return;
    };
    // #366: pipeline stage progress (producer park-site transitions, page-prep
    // and shard-build/merge job movement) is real forward progress, but none
    // of it advances `phase`/`current`, so the stall watchdog used to abort a
    // healthy ingest→staged-shard-build hand-off after 300 s. Tick the #332
    // activity channel whenever the work-bearing snapshot fields move.
    if lexical_rebuild_pipeline_work_advanced(&previous_runtime, latest_runtime) {
        progress.tick_activity();
    }
    progress
        .rebuild_pipeline_queue_depth
        .store(latest_runtime.queue_depth, Ordering::Relaxed);
    progress
        .rebuild_pipeline_inflight_message_bytes
        .store(latest_runtime.inflight_message_bytes, Ordering::Relaxed);
    progress.rebuild_pipeline_pending_batch_conversations.store(
        latest_runtime.pending_batch_conversations,
        Ordering::Relaxed,
    );
    progress.rebuild_pipeline_pending_batch_message_bytes.store(
        latest_runtime.pending_batch_message_bytes,
        Ordering::Relaxed,
    );
    progress
        .rebuild_pipeline_page_prep_workers
        .store(latest_runtime.page_prep_workers, Ordering::Relaxed);
    progress
        .rebuild_pipeline_active_page_prep_jobs
        .store(latest_runtime.active_page_prep_jobs, Ordering::Relaxed);
    progress
        .rebuild_pipeline_ordered_buffered_pages
        .store(latest_runtime.ordered_buffered_pages, Ordering::Relaxed);
    progress
        .rebuild_pipeline_budget_generation
        .store(latest_runtime.budget_generation, Ordering::Relaxed);
    progress
        .rebuild_pipeline_producer_budget_wait_count
        .store(latest_runtime.producer_budget_wait_count, Ordering::Relaxed);
    progress
        .rebuild_pipeline_producer_budget_wait_ms
        .store(latest_runtime.producer_budget_wait_ms, Ordering::Relaxed);
    if let Ok(mut producer_state) = progress.rebuild_pipeline_producer_state.lock() {
        *producer_state = latest_runtime.producer_state.clone();
    }
    progress
        .rebuild_pipeline_reservation_next_sequence
        .store(latest_runtime.reservation_next_sequence, Ordering::Relaxed);
    progress
        .rebuild_pipeline_producer_budget_active_waiters
        .store(
            latest_runtime.producer_budget_waiter_count,
            Ordering::Relaxed,
        );
    progress.rebuild_pipeline_producer_handoff_wait_count.store(
        latest_runtime.producer_handoff_wait_count,
        Ordering::Relaxed,
    );
    progress
        .rebuild_pipeline_producer_handoff_wait_ms
        .store(latest_runtime.producer_handoff_wait_ms, Ordering::Relaxed);
    if let Ok(mut host_loadavg) = progress.rebuild_pipeline_host_loadavg_1m_milli.lock() {
        *host_loadavg = latest_runtime.host_loadavg_1m_milli;
    }
    if let Ok(mut host_available_memory) =
        progress.rebuild_pipeline_host_available_memory_bytes.lock()
    {
        *host_available_memory = latest_runtime.host_available_memory_bytes;
    }
    if let Ok(mut process_rss) = progress.rebuild_pipeline_process_rss_bytes.lock() {
        *process_rss = latest_runtime.process_rss_bytes;
    }
    if let Ok(mut controller_mode) = progress.rebuild_pipeline_controller_mode.lock() {
        *controller_mode = latest_runtime.controller_mode.clone();
    }
    if let Ok(mut controller_reason) = progress.rebuild_pipeline_controller_reason.lock() {
        *controller_reason = latest_runtime.controller_reason.clone();
    }
    progress
        .rebuild_pipeline_staged_merge_workers_max
        .store(latest_runtime.staged_merge_workers_max, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_merge_allowed_jobs
        .store(latest_runtime.staged_merge_allowed_jobs, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_merge_active_jobs
        .store(latest_runtime.staged_merge_active_jobs, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_merge_ready_artifacts
        .store(
            latest_runtime.staged_merge_ready_artifacts,
            Ordering::Relaxed,
        );
    progress
        .rebuild_pipeline_staged_merge_ready_groups
        .store(latest_runtime.staged_merge_ready_groups, Ordering::Relaxed);
    if let Ok(mut staged_merge_reason) = progress
        .rebuild_pipeline_staged_merge_controller_reason
        .lock()
    {
        *staged_merge_reason = latest_runtime.staged_merge_controller_reason.clone();
    }
    progress
        .rebuild_pipeline_staged_shard_build_workers_max
        .store(
            latest_runtime.staged_shard_build_workers_max,
            Ordering::Relaxed,
        );
    progress
        .rebuild_pipeline_staged_shard_build_allowed_jobs
        .store(
            latest_runtime.staged_shard_build_allowed_jobs,
            Ordering::Relaxed,
        );
    progress
        .rebuild_pipeline_staged_shard_build_active_jobs
        .store(
            latest_runtime.staged_shard_build_active_jobs,
            Ordering::Relaxed,
        );
    progress
        .rebuild_pipeline_staged_shard_build_pending_jobs
        .store(
            latest_runtime.staged_shard_build_pending_jobs,
            Ordering::Relaxed,
        );
    if let Ok(mut staged_shard_build_reason) = progress
        .rebuild_pipeline_staged_shard_build_controller_reason
        .lock()
    {
        *staged_shard_build_reason = latest_runtime.staged_shard_build_controller_reason.clone();
    }
    progress
        .rebuild_pipeline_staged_shard_build_memory_reserve_bytes
        .store(
            latest_runtime.staged_shard_build_memory_reserve_bytes,
            Ordering::Relaxed,
        );
    progress
        .rebuild_pipeline_staged_shard_build_emergency_memory_reserve_bytes
        .store(
            latest_runtime.staged_shard_build_emergency_memory_reserve_bytes,
            Ordering::Relaxed,
        );
    progress
        .rebuild_pipeline_staged_shard_build_completed_jobs
        .store(
            latest_runtime.staged_shard_build_completed_jobs,
            Ordering::Relaxed,
        );
    if let Ok(mut last_shard_index) = progress
        .rebuild_pipeline_staged_shard_build_last_shard_index
        .lock()
    {
        *last_shard_index = latest_runtime.staged_shard_build_last_shard_index;
    }
    progress
        .rebuild_pipeline_staged_shard_build_last_message_bytes
        .store(
            latest_runtime.staged_shard_build_last_message_bytes,
            Ordering::Relaxed,
        );
    progress
        .rebuild_pipeline_staged_shard_build_last_index_size_bytes
        .store(
            latest_runtime.staged_shard_build_last_index_size_bytes,
            Ordering::Relaxed,
        );
    progress
        .rebuild_pipeline_staged_shard_build_last_duration_ms
        .store(
            latest_runtime.staged_shard_build_last_duration_ms,
            Ordering::Relaxed,
        );
    if let Ok(mut last_amplification) = progress
        .rebuild_pipeline_staged_shard_build_last_amplification_milli
        .lock()
    {
        *last_amplification = latest_runtime.staged_shard_build_last_amplification_milli;
    }
    if let Ok(mut observed_amplification) = progress
        .rebuild_pipeline_staged_shard_build_observed_amplification_milli
        .lock()
    {
        *observed_amplification = latest_runtime.staged_shard_build_observed_amplification_milli;
    }
}

#[allow(clippy::too_many_arguments)]
pub(in crate::indexer) fn refresh_and_maybe_apply_lexical_rebuild_pipeline_runtime(
    latest_runtime: &mut LexicalRebuildPipelineRuntimeSnapshot,
    progress: Option<&Arc<IndexingProgress>>,
    flow_limiter: &StreamingByteLimiter,
    producer_telemetry: Option<&LexicalRebuildProducerTelemetry>,
    responsiveness_controller: &mut LexicalRebuildResponsivenessController,
    pipeline_budget_controller: &LexicalRebuildPipelineBudgetController,
    current_batch_conversation_limit: &mut usize,
    active_commit_intervals: Option<(&mut usize, &mut usize, &mut usize)>,
    sink_runtime: LexicalRebuildPipelineSinkRuntimeSnapshot,
) {
    refresh_lexical_rebuild_pipeline_runtime(
        latest_runtime,
        progress,
        flow_limiter,
        producer_telemetry,
        Some(responsiveness_controller),
        pipeline_budget_controller.generation(),
        sink_runtime,
    );

    if let Some(transition) = responsiveness_controller.observe_runtime(latest_runtime) {
        apply_lexical_rebuild_budget_transition(
            transition,
            flow_limiter,
            pipeline_budget_controller,
            current_batch_conversation_limit,
            active_commit_intervals,
        );
        refresh_lexical_rebuild_pipeline_runtime(
            latest_runtime,
            progress,
            flow_limiter,
            producer_telemetry,
            Some(responsiveness_controller),
            pipeline_budget_controller.generation(),
            sink_runtime,
        );
    }
}

pub(in crate::indexer) fn exact_total_counts_from_progress(
    progress: Option<&Arc<IndexingProgress>>,
) -> Option<(usize, usize)> {
    let progress = progress?;
    let stats = progress.stats.lock().ok()?;
    if !stats.total_counts_exact {
        return None;
    }
    Some((stats.total_conversations, stats.total_messages))
}

pub(in crate::indexer) fn record_exact_total_counts_in_progress(
    progress: Option<&Arc<IndexingProgress>>,
    total_conversations: usize,
    total_messages: usize,
) {
    if let Some(progress) = progress
        && let Ok(mut stats) = progress.stats.lock()
    {
        stats.total_conversations = total_conversations;
        stats.total_messages = total_messages;
        stats.total_counts_exact = true;
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(in crate::indexer) struct MatchingLexicalRebuildStateStatus {
    pub(in crate::indexer) has_pending_resume: bool,
    pub(in crate::indexer) has_completed_checkpoint: bool,
    pub(in crate::indexer) completed_indexed_docs: Option<usize>,
    pub(in crate::indexer) completed_exact_totals: Option<(usize, usize)>,
    pub(in crate::indexer) completed_storage_fingerprint: Option<String>,
}

#[cfg(test)]
pub(in crate::indexer) fn matching_lexical_rebuild_state_status(
    index_path: &Path,
    db_state: &LexicalRebuildDbState,
) -> Result<MatchingLexicalRebuildStateStatus> {
    let Some(state) = load_lexical_rebuild_state(index_path)? else {
        return Ok(MatchingLexicalRebuildStateStatus::default());
    };
    Ok(matching_lexical_rebuild_state_status_for_loaded_state(
        state, db_state,
    ))
}

pub(in crate::indexer) fn matching_lexical_rebuild_state_status_if_present(
    index_path: &Path,
    load_current_db_state: impl FnOnce() -> Result<LexicalRebuildDbState>,
) -> Result<MatchingLexicalRebuildStateStatus> {
    let Some(state) = load_lexical_rebuild_state(index_path)? else {
        return Ok(MatchingLexicalRebuildStateStatus::default());
    };
    let db_state = load_current_db_state()?;
    Ok(matching_lexical_rebuild_state_status_for_loaded_state(
        state, &db_state,
    ))
}

pub(in crate::indexer) fn matching_completed_lexical_rebuild_state_status_without_fingerprint(
    index_path: &Path,
    db_path: &Path,
    total_conversations: usize,
) -> Result<Option<MatchingLexicalRebuildStateStatus>> {
    let Some(state) = load_lexical_rebuild_state(index_path)? else {
        return Ok(Some(MatchingLexicalRebuildStateStatus::default()));
    };
    if state.is_incomplete() {
        return Ok(None);
    }

    let normalized_db_path = crate::normalize_path_identity(db_path)
        .to_string_lossy()
        .into_owned();
    if state.version != LEXICAL_REBUILD_STATE_VERSION
        || state.schema_hash != crate::search::tantivy::SCHEMA_HASH
        || state.db.total_conversations != total_conversations
        || !lexical_rebuild_db_paths_match(&state.db.db_path, &normalized_db_path)
        || !lexical_rebuild_page_size_is_compatible(state.page_size)
    {
        return Ok(Some(MatchingLexicalRebuildStateStatus::default()));
    }

    let has_completed_checkpoint = state.completed
        && state.pending.is_none()
        && state.effective_execution_mode() == LexicalRebuildExecutionMode::SharedWriter
        && !state.runtime.is_observed();
    Ok(Some(MatchingLexicalRebuildStateStatus {
        has_pending_resume: false,
        has_completed_checkpoint,
        completed_indexed_docs: has_completed_checkpoint.then_some(state.indexed_docs),
        // Task 5: `indexed_docs` includes converged search-region
        // documents; the exact message total is its canonical share.
        completed_exact_totals: has_completed_checkpoint.then_some((
            state.db.total_conversations,
            state.indexed_docs.saturating_sub(state.indexed_region_docs),
        )),
        completed_storage_fingerprint: has_completed_checkpoint
            .then_some(state.db.storage_fingerprint),
    }))
}

pub(in crate::indexer) fn matching_lexical_rebuild_state_status_for_loaded_state(
    state: LexicalRebuildState,
    db_state: &LexicalRebuildDbState,
) -> MatchingLexicalRebuildStateStatus {
    if !state.matches_run(db_state, LEXICAL_REBUILD_PAGE_SIZE) {
        return MatchingLexicalRebuildStateStatus::default();
    }

    let has_completed_checkpoint = state.completed
        && state.pending.is_none()
        && state.effective_execution_mode() == LexicalRebuildExecutionMode::SharedWriter
        && !state.runtime.is_observed();

    MatchingLexicalRebuildStateStatus {
        has_pending_resume: state.is_incomplete(),
        has_completed_checkpoint,
        completed_indexed_docs: has_completed_checkpoint.then_some(state.indexed_docs),
        // Task 5: `indexed_docs` includes converged search-region
        // documents; the exact message total is its canonical share.
        completed_exact_totals: has_completed_checkpoint.then_some((
            state.db.total_conversations,
            state.indexed_docs.saturating_sub(state.indexed_region_docs),
        )),
        completed_storage_fingerprint: has_completed_checkpoint
            .then_some(state.db.storage_fingerprint),
    }
}

pub(in crate::indexer) fn nonresumable_pending_lexical_rebuild_status_without_fingerprint(
    index_path: &Path,
    db_path: &Path,
    total_conversations: usize,
) -> Result<Option<MatchingLexicalRebuildStateStatus>> {
    let Some(state) = nonresumable_pending_lexical_rebuild_state_for_db(index_path, db_path)?
    else {
        return Ok(None);
    };
    if state.effective_execution_mode() != LexicalRebuildExecutionMode::CanonicalMetadataRepair
        && state.db.total_conversations != total_conversations
    {
        return Ok(None);
    }

    Ok(Some(MatchingLexicalRebuildStateStatus {
        has_pending_resume: true,
        ..MatchingLexicalRebuildStateStatus::default()
    }))
}

/// Load and cheaply classify a restart-from-zero checkpoint before touching
/// the canonical database. In particular, path identity, schema, execution
/// mode, and checkpoint shape are all available in the small JSON sidecar and
/// must be rejected before an exact conversation count can hydrate a very
/// large SQLite archive (GH #413).
pub(in crate::indexer) fn nonresumable_pending_lexical_rebuild_state_for_db(
    index_path: &Path,
    db_path: &Path,
) -> Result<Option<LexicalRebuildState>> {
    let Some(state) = load_lexical_rebuild_state(index_path)? else {
        return Ok(None);
    };
    if !state.is_incomplete()
        || !state.requires_restart_from_zero_on_resume()
        || state.version != LEXICAL_REBUILD_STATE_VERSION
        || state.schema_hash != crate::search::tantivy::SCHEMA_HASH
        || !lexical_rebuild_page_size_is_compatible(state.page_size)
    {
        return Ok(None);
    }

    let normalized_db_path = crate::normalize_path_identity(db_path)
        .to_string_lossy()
        .into_owned();
    if !lexical_rebuild_db_paths_match(&state.db.db_path, &normalized_db_path) {
        return Ok(None);
    }

    Ok(Some(state))
}

#[cfg(test)]
pub(in crate::indexer) fn nonresumable_pending_lexical_rebuild_status_from_readonly_db(
    index_path: &Path,
    db_path: &Path,
) -> Result<Option<(MatchingLexicalRebuildStateStatus, usize)>> {
    let Some(state) = nonresumable_pending_lexical_rebuild_state_for_db(index_path, db_path)?
    else {
        return Ok(None);
    };
    confirm_nonresumable_pending_lexical_rebuild_state_from_readonly_db(&state, db_path)
}

#[cfg(test)]
pub(in crate::indexer) fn confirm_nonresumable_pending_lexical_rebuild_state_from_readonly_db(
    state: &LexicalRebuildState,
    db_path: &Path,
) -> Result<Option<(MatchingLexicalRebuildStateStatus, usize)>> {
    let mut storage = FrankenStorage::open_readonly(db_path).with_context(|| {
        format!(
            "opening readonly storage to classify pending lexical rebuild checkpoint: {}",
            db_path.display()
        )
    })?;
    let total_conversations = count_total_conversations_exact(&storage)?;
    storage.close_best_effort_in_place();

    if state.effective_execution_mode() != LexicalRebuildExecutionMode::CanonicalMetadataRepair
        && state.db.total_conversations != total_conversations
    {
        return Ok(None);
    }
    Ok(Some((
        MatchingLexicalRebuildStateStatus {
            has_pending_resume: true,
            ..MatchingLexicalRebuildStateStatus::default()
        },
        total_conversations,
    )))
}

/// Runs under the index writer lock, before spawning readonly rebuild workers.
/// Only the optional fallback shadow and its repair markers may change here;
/// canonical conversations and messages remain the rebuild authority.
pub(in crate::indexer) fn preflight_fts_shadow_before_lexical_readers(
    db_path: &Path,
) -> Result<()> {
    let storage = FrankenStorage::open_deferred_fts5_for_repair(db_path)
        .with_context(|| format!("opening deferred FTS5 preflight for {}", db_path.display()))?;
    let result = (|| -> Result<()> {
        if let crate::storage::sqlite::FtsShadowViability::NotViable {
            corpus_messages,
            bound_messages,
        } = storage.fts_shadow_viability()?
        {
            // Already retired by an earlier run: nothing to drop, and
            // rewriting the markers would only churn the archive's WAL.
            if storage.fts_shadow_retirement_is_recorded()? {
                return Ok(());
            }
            let detail = crate::storage::sqlite::fts_shadow_not_viable_detail(
                corpus_messages,
                bound_messages,
            );
            tracing::warn!(
                db_path = %db_path.display(),
                corpus_messages,
                bound_messages,
                "dropping oversized fallback FTS shadow before lexical readers (GH #413); canonical rows and Quill search are preserved"
            );
            storage.drop_fts_shadow_as_not_viable(&detail)?;
        }
        Ok(())
    })();
    let close = storage
        .close_without_checkpoint()
        .with_context(|| format!("closing deferred FTS5 preflight for {}", db_path.display()));
    if result.is_err()
        && let Err(error) = &close
    {
        tracing::warn!(%error, "FTS shadow preflight also failed to close");
    }
    result.and(close)
}

pub(in crate::indexer) fn should_try_readonly_nonresumable_lexical_resume(
    opts: &IndexOptions,
) -> bool {
    !opts.full
        && !opts.force_rebuild
        && !opts.watch
        && !opts.semantic
        && opts
            .watch_once_paths
            .as_ref()
            .is_none_or(|paths| paths.is_empty())
}

pub(in crate::indexer) fn should_try_readonly_canonical_force_rebuild(opts: &IndexOptions) -> bool {
    opts.force_rebuild
        && !opts.full
        && !opts.watch
        && !opts.semantic
        && !opts.build_hnsw
        && opts
            .watch_once_paths
            .as_ref()
            .is_none_or(|paths| paths.is_empty())
        && opts.db_path.exists()
}

/// The guided repair adapter authorizes only an existing archive's lexical
/// rebuild. Keep ordinary first-run `--force-rebuild` behavior, but enforce
/// this production scope constraint under the index lock so an archive that
/// disappears or becomes empty cannot turn the repair into source discovery.
pub(in crate::indexer) fn canonical_rebuild_required_from_env() -> Result<bool> {
    match dotenvy::var("CASS_INDEX_REQUIRE_CANONICAL_REBUILD") {
        Ok(value) => match value.trim() {
            "1" => Ok(true),
            "0" => Ok(false),
            _ => anyhow::bail!("CASS_INDEX_REQUIRE_CANONICAL_REBUILD must be 0 or 1"),
        },
        Err(dotenvy::Error::EnvVar(std::env::VarError::NotPresent)) => Ok(false),
        Err(error) => {
            Err(error).context("CASS_INDEX_REQUIRE_CANONICAL_REBUILD must be valid Unicode")
        }
    }
}

pub(in crate::indexer) fn validate_canonical_rebuild_scope(
    opts: &IndexOptions,
    required: bool,
) -> Result<()> {
    if required && (!should_try_readonly_canonical_force_rebuild(opts) || !opts.db_path.is_file()) {
        anyhow::bail!(
            "canonical-only lexical repair requires an existing archive and --force-rebuild without source-scan, watch or semantic options; no sources were scanned"
        );
    }
    Ok(())
}

pub(in crate::indexer) enum ReadonlyRebuildBoundary {
    Rebuild,
    ArchiveWrite,
}

/// Only these typed failures identify derived state that the ordinary reader
/// is allowed to repair. In particular, a malformed archive must still fail a
/// search-triggered probe before it creates maintenance artifacts (GH #436).
pub(in crate::indexer) fn readonly_rebuild_needs_archive_recovery(
    db_path: &Path,
    error: &anyhow::Error,
) -> bool {
    use crate::franken_sync::FrankenError;

    let Some(error) = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<FrankenError>())
    else {
        return false;
    };
    match error {
        FrankenError::BusyRecovery => true,
        FrankenError::DatabaseCorrupt { detail } => {
            detail.contains("conflicting virtual-table entries") && detail.contains("fts_messages")
        }
        FrankenError::CannotOpen { .. } => {
            let Ok(canonical_db_path) = archive_lease::canonical_database_path(db_path) else {
                return false;
            };
            if !canonical_db_path.is_file() {
                return false;
            }
            let mut wal_path = canonical_db_path.into_os_string();
            wal_path.push("-wal");
            fs::symlink_metadata(Path::new(&wal_path))
                .is_ok_and(|metadata| metadata.file_type().is_file() && metadata.len() > 32)
        }
        _ => false,
    }
}

pub(in crate::indexer) fn open_readonly_rebuild_storage(
    db_path: &Path,
    before_recovery: impl FnOnce() -> Result<()>,
) -> Result<FrankenStorage> {
    // This first attempt is an observation, not another 30-second recovery
    // wait. A recovery-capable open can proceed only after writer admission.
    match FrankenStorage::open_strict_readonly_with_timeout(db_path, Duration::ZERO) {
        Ok(storage) => Ok(storage),
        Err(error) if readonly_rebuild_needs_archive_recovery(db_path, &error) => {
            before_recovery()?;
            let canonical_db_path = archive_lease::canonical_database_path(db_path)?;
            FrankenStorage::open_readonly_recovering_wal_index_in_place(&canonical_db_path)
                .with_context(|| {
                format!(
                    "opening canonical rebuild reader after admitted recovery for {} (strict read: {error:#})",
                    db_path.display()
                )
            })
        }
        Err(error) => Err(error),
    }
}

pub(in crate::indexer) fn try_readonly_canonical_force_rebuild(
    opts: &IndexOptions,
    progress_bump: &Arc<AtomicI64>,
    require_canonical_rebuild: bool,
    mut before_boundary: impl FnMut(ReadonlyRebuildBoundary) -> Result<()>,
) -> Result<bool> {
    validate_canonical_rebuild_scope(opts, require_canonical_rebuild)?;
    if !should_try_readonly_canonical_force_rebuild(opts) {
        return Ok(false);
    }

    let storage = open_readonly_rebuild_storage(&opts.db_path, || {
        before_boundary(ReadonlyRebuildBoundary::ArchiveWrite)
    })
    .with_context(|| {
        format!(
            "opening canonical database read-only for force rebuild: {}",
            opts.db_path.display()
        )
    })?;
    let total_conversations = count_total_conversations_exact(&storage)?;
    if total_conversations == 0 {
        storage.close_without_checkpoint().with_context(|| {
            format!(
                "closing empty canonical database after read-only force rebuild preflight: {}",
                opts.db_path.display()
            )
        })?;
        if require_canonical_rebuild {
            anyhow::bail!(
                "canonical-only lexical repair requires a populated archive; the archive is empty and no sources were scanned"
            );
        }
        return Ok(false);
    }
    let total_messages = count_total_messages_exact(&storage)?;
    storage.close_without_checkpoint().with_context(|| {
        format!(
            "closing canonical database before read-only force rebuild: {}",
            opts.db_path.display()
        )
    })?;

    tracing::info!(
        db_path = %opts.db_path.display(),
        conversations = total_conversations,
        messages = total_messages,
        "running force rebuild from populated canonical database without writable storage preflight"
    );
    record_lexical_population_strategy(
        opts.progress.as_ref(),
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
        "force_rebuild_uses_readonly_authoritative_canonical_db_rebuild_only",
    );
    tracing::info!(
        strategy = LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild.as_str(),
        reason = "force_rebuild_uses_readonly_authoritative_canonical_db_rebuild_only",
        "selected_lexical_population_strategy"
    );

    ensure_authoritative_lexical_rebuild_storage_headroom(&opts.data_dir, &opts.db_path)?;
    before_boundary(ReadonlyRebuildBoundary::Rebuild)?;
    let rebuild_start = Instant::now();
    let rebuild = rebuild_tantivy_from_db_deferred_startup_with_progress_bump(
        &opts.db_path,
        &opts.data_dir,
        total_conversations,
        opts.progress.clone(),
        Arc::clone(progress_bump),
    )?;
    if let Some(p) = &opts.progress
        && let Ok(mut stats) = p.stats.lock()
    {
        stats.scan_ms = 0;
        stats.index_ms = rebuild_start.elapsed().as_millis() as u64;
        stats.total_conversations = total_conversations;
        stats.total_messages = total_messages;
        stats.total_counts_exact = true;
    }
    if let Some(observed_messages) = rebuild.observed_messages {
        record_exact_total_counts_in_progress(
            opts.progress.as_ref(),
            total_conversations,
            observed_messages,
        );
    }

    // Force-rebuild deliberately skips the writable index path (readonly DB →
    // Tantivy only). Without an explicit watermark write, `last_indexed_at`
    // stays frozen from the previous incremental run and `cass status` /
    // `cass health` report stale forever even though the lexical index is
    // freshly rebuilt. performed_scan=false so last_scan_ts is preserved.
    let now_ms = FrankenStorage::now_millis();
    // The rebuild above is read-only and remains useful with an unwritable
    // archive directory. Its optional watermark write needs the same archive
    // lease as every other writer, including when another data dir owns it.
    match before_boundary(ReadonlyRebuildBoundary::ArchiveWrite)
        .and_then(|()| FrankenStorage::open_writer(&opts.db_path))
    {
        Ok(writer) => {
            let write_result = writer.set_last_indexed_at(now_ms);
            match writer.close() {
                Ok(()) => {
                    if let Err(err) = write_result {
                        tracing::warn!(
                            db_path = %opts.db_path.display(),
                            now_ms,
                            error = %format!("{err:#}"),
                            "force rebuild succeeded but failed to update last_indexed_at; \
                             status/health may report stale until the next index run"
                        );
                    } else {
                        tracing::info!(
                            now_ms,
                            "updated last_indexed_at for status display after force rebuild"
                        );
                    }
                }
                Err(close_err) => {
                    tracing::warn!(
                        db_path = %opts.db_path.display(),
                        now_ms,
                        write_ok = write_result.is_ok(),
                        error = %format!("{close_err:#}"),
                        "force rebuild metadata writer close failed after last_indexed_at update attempt"
                    );
                }
            }
        }
        Err(err) => {
            tracing::warn!(
                db_path = %opts.db_path.display(),
                now_ms,
                error = %format!("{err:#}"),
                "force rebuild succeeded but could not open writer to update last_indexed_at; \
                 status/health may report stale until the next index run"
            );
        }
    }

    Ok(true)
}

pub(in crate::indexer) fn should_preserve_matching_completed_lexical_checkpoint_during_full_scan(
    full_rebuild: bool,
    resume_lexical_rebuild: bool,
    canonical_only_full_rebuild: bool,
    initial_checkpoint_status: &MatchingLexicalRebuildStateStatus,
) -> bool {
    full_rebuild
        && !resume_lexical_rebuild
        && !canonical_only_full_rebuild
        && initial_checkpoint_status.has_completed_checkpoint
}

pub(in crate::indexer) fn should_preflight_existing_tantivy_reader(
    resume_lexical_rebuild: bool,
    full_rebuild: bool,
) -> bool {
    !resume_lexical_rebuild && !full_rebuild
}

pub(in crate::indexer) fn should_probe_live_tantivy_docs_for_post_full_scan_skip(
    full_rebuild: bool,
    rebuild_was_required: bool,
    salvage_messages_imported: usize,
    initial_checkpoint_status: &MatchingLexicalRebuildStateStatus,
    scan_canonical_mutations: CanonicalMutationCounts,
    observed_tantivy_docs: Option<usize>,
) -> bool {
    full_rebuild
        && !rebuild_was_required
        && salvage_messages_imported == 0
        && !scan_canonical_mutations.changed()
        && initial_checkpoint_status.has_completed_checkpoint
        && observed_tantivy_docs.is_none()
}

pub(in crate::indexer) fn observed_tantivy_docs_for_post_full_scan_skip(
    index_path: &Path,
    full_rebuild: bool,
    rebuild_was_required: bool,
    salvage_messages_imported: usize,
    initial_checkpoint_status: &MatchingLexicalRebuildStateStatus,
    scan_canonical_mutations: CanonicalMutationCounts,
    observed_tantivy_docs: Option<usize>,
) -> Result<Option<usize>> {
    if should_probe_live_tantivy_docs_for_post_full_scan_skip(
        full_rebuild,
        rebuild_was_required,
        salvage_messages_imported,
        initial_checkpoint_status,
        scan_canonical_mutations,
        observed_tantivy_docs,
    ) {
        live_tantivy_doc_count(index_path)
    } else {
        Ok(observed_tantivy_docs)
    }
}

pub(in crate::indexer) fn should_force_authoritative_rebuild(
    canonical_storage_rebuilt: bool,
    tantivy_requires_rebuild: bool,
) -> bool {
    canonical_storage_rebuilt || tantivy_requires_rebuild
}

pub(in crate::indexer) fn should_skip_noop_final_lexical_checkpoint_refresh(
    full_rebuild: bool,
    rebuild_was_required: bool,
    initial_checkpoint_status: &MatchingLexicalRebuildStateStatus,
    exact_total_counts: Option<(usize, usize)>,
    canonical_mutations: CanonicalMutationCounts,
    completed_checkpoint_present_at_run_end: bool,
) -> bool {
    !full_rebuild
        && !rebuild_was_required
        && exact_total_counts.is_none()
        && !canonical_mutations.changed()
        && initial_checkpoint_status.has_completed_checkpoint
        // #289: the run-start status can go stale before the end of the run —
        // the federated-bundle materialization that happens on the next writer
        // open used to wipe the live directory's sidecars. Only skip the final
        // refresh when the checkpoint file still exists at run end; if it went
        // missing mid-run, fall through and re-persist it.
        && completed_checkpoint_present_at_run_end
}

/// GH #457 follow-on: must this run re-derive its exact canonical totals and
/// re-persist the lexical checkpoint after a pre-scan authoritative repair?
///
/// The pre-scan repair (`choose_incremental_canonical_lexical_repair_plan`)
/// rebuilds the lexical index from the authoritative database and persists an
/// exact completed checkpoint, and the run *then* continues into the source
/// scan. Even a scan that skips every unchanged source overwrites the progress
/// counts while leaving `total_counts_exact` set. Recount after any such scan
/// so reports and the final checkpoint describe the archive, not just the
/// discovered delta. Resume-only and canonical-only rebuilds perform no scan;
/// healthy scans without a preceding repair retain their existing fast path.
pub(in crate::indexer) fn should_redrive_final_lexical_checkpoint_refresh_after_pre_scan_repair(
    exact_completed_checkpoint_predates_scan: bool,
    performed_scan: bool,
) -> bool {
    exact_completed_checkpoint_predates_scan && performed_scan
}

pub(in crate::indexer) fn should_skip_post_full_scan_authoritative_rebuild(
    full_rebuild: bool,
    rebuild_was_required: bool,
    salvage_messages_imported: usize,
    initial_checkpoint_status: &MatchingLexicalRebuildStateStatus,
    scan_canonical_mutations: CanonicalMutationCounts,
    observed_tantivy_docs: Option<usize>,
) -> bool {
    full_rebuild
        && !rebuild_was_required
        && salvage_messages_imported == 0
        && !scan_canonical_mutations.changed()
        && initial_checkpoint_status.has_completed_checkpoint
        && initial_checkpoint_status.completed_indexed_docs == observed_tantivy_docs
}

pub(in crate::indexer) struct RunIndexProgressReset {
    pub(in crate::indexer) progress: Option<Arc<IndexingProgress>>,
    pub(in crate::indexer) initial_write_bytes: Option<u64>,
}

impl RunIndexProgressReset {
    pub(in crate::indexer) fn new(progress: Option<Arc<IndexingProgress>>) -> Self {
        Self {
            initial_write_bytes: progress.as_ref().and_then(|_| process_index_write_bytes()),
            progress,
        }
    }
}

impl Drop for RunIndexProgressReset {
    fn drop(&mut self) {
        if let Some(progress) = self.progress.as_ref()
            && let Ok(mut stats) = progress.stats.lock()
        {
            stats.bytes_written = self
                .initial_write_bytes
                .zip(process_index_write_bytes())
                .and_then(|(before, after)| after.checked_sub(before));
        }
        reset_progress_to_idle(self.progress.as_ref());
    }
}

#[cfg(any(target_os = "linux", test))]
pub(in crate::indexer) fn parse_process_index_write_bytes(text: &str) -> Option<u64> {
    text.lines()
        .find_map(|line| line.strip_prefix("write_bytes:"))?
        .trim()
        .parse()
        .ok()
}

pub(in crate::indexer) fn process_index_write_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        parse_process_index_write_bytes(&fs::read_to_string("/proc/self/io").ok()?)
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}
