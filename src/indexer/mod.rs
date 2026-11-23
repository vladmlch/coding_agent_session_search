mod archive_lease;
pub mod background_refresh;
mod checkpoint_deadline;
// Local fix (review 2026-11-02, I1): unreadable forgotten source stays forgotten.
mod forgotten_source_admission;
// Local fix (review 2026-11-02, I2): typed final-checkpoint report for analytics.
pub(crate) mod lexical_generation;
mod lexical_publish;
pub mod lexical_reconcile;
pub(crate) mod memoization;
pub(crate) mod parallel_wal_shadow;
pub mod quarantine;
pub mod quarantine_retry;
mod raw_source_exclusions;
pub mod redact_secrets;
pub mod refresh_ledger;
pub(crate) mod responsiveness;
pub mod semantic;
pub mod semantic_progress;
#[cfg(test)]
mod source_ledger_tests;
mod source_writer_probe;
pub(crate) mod staging_reclaim;
#[cfg(test)]
mod wal_checkpoint_report;
#[cfg(test)]
mod watch_ingestion_tests;
mod watch_source_cache;
mod watch_source_ledger;
mod watch_source_stability;

mod archive_fingerprint;
mod expected_lexical_docs;
mod fallback_fts_repair;
mod index_dispatch;
mod ingest_oom;
mod ingest_quarantine;
mod lexical_checkpoint_io;
mod lexical_packet_prep;
mod lexical_policy;
mod lexical_publish_ops;
mod lexical_rebuild_controller;
mod lexical_rebuild_once;
mod lexical_rebuild_producer;
mod lexical_rebuild_settings;
mod lexical_rebuild_staged;
mod lexical_rebuild_state;
mod lexical_shard_planner;
mod lexical_shard_workers;
pub mod persist;
mod progress_and_stats;
mod raw_mirror_capture;
mod run_index_finalize;
mod run_index_lexical;
mod run_index_preflight;
mod run_lock;
mod semantic_watch_once;
mod source_filters;
mod storage_preflight;
mod streaming_limiter;
mod streaming_pipeline;
#[cfg(test)]
mod tests;
mod wal_close;
mod watch_loop;
mod watch_reindex;
mod watch_replay_debt;
mod watch_spool;
mod watch_state;

pub(in crate::indexer) use self::quarantine::QuarantineKey;
pub(in crate::indexer) use self::quarantine::QuarantineState;
pub(in crate::indexer) use self::raw_source_exclusions::RawSourceExclusions;
pub(in crate::indexer) use self::refresh_ledger::EquivalenceArtifacts as RefreshEquivalenceArtifacts;
pub(in crate::indexer) use self::refresh_ledger::PhaseRecord;
pub(in crate::indexer) use self::refresh_ledger::RefreshLedger;
pub(in crate::indexer) use self::refresh_ledger::RefreshLedgerEvidence;
pub(in crate::indexer) use self::refresh_ledger::RefreshPhase;
pub(in crate::indexer) use self::source_writer_probe::SourceWriterProbe;
pub(in crate::indexer) use crate::connector_ingest_diagnostics::ConnectorIngestDiagnostic;
pub(in crate::indexer) use crate::connector_ingest_diagnostics::ConnectorIngestReport;
pub(in crate::indexer) use crate::connector_ingest_diagnostics::ConnectorIngestRun;
pub(in crate::indexer) use crate::connector_ingest_diagnostics::ProviderIngestSummary;
pub(in crate::indexer) use crate::connectors::Connector;
pub(in crate::indexer) use crate::connectors::DiscoveredSourceFile;
pub(in crate::indexer) use crate::connectors::NormalizedConversation;
pub(in crate::indexer) use crate::connectors::NormalizedMessage;
pub(in crate::indexer) use crate::connectors::NormalizedSnippet;
pub(in crate::indexer) use crate::connectors::ScanContext;
pub(in crate::indexer) use crate::connectors::ScanRoot;
pub(in crate::indexer) use crate::connectors::antigravity::AntigravityConnector;
pub(in crate::indexer) use crate::connectors::chatgpt::ChatGptConnector;
pub(in crate::indexer) use crate::connectors::claude_code::ClaudeCodeConnector;
pub(in crate::indexer) use crate::connectors::codex::CodexConnector;
pub(in crate::indexer) use crate::connectors::cursor::CursorConnector;
pub(in crate::indexer) use crate::connectors::gemini::GeminiConnector;
pub(in crate::indexer) use crate::connectors::opencode::OpenCodeConnector;
pub(in crate::indexer) use crate::connectors::qwen::QwenConnector;
pub(in crate::indexer) use crate::franken_sync::compat::ConnectionExt;
pub(in crate::indexer) use crate::franken_sync::compat::ParamValue;
pub(in crate::indexer) use crate::franken_sync::compat::RowExt;
pub(in crate::indexer) use crate::model::conversation_packet::CONVERSATION_PACKET_VERSION;
pub(in crate::indexer) use crate::model::conversation_packet::ConversationPacket;
pub(in crate::indexer) use crate::model::conversation_packet::ConversationPacketHashes;
pub(in crate::indexer) use crate::model::conversation_packet::ConversationPacketProvenance;
pub(in crate::indexer) use crate::model::conversation_packet::ConversationPacketSinkProjections;
pub(in crate::indexer) use crate::search::asset_state::SearchMaintenanceJobKind;
pub(in crate::indexer) use crate::search::asset_state::SearchMaintenanceMode;
pub(in crate::indexer) use crate::search::canonicalize::is_hard_message_noise;
pub(in crate::indexer) use crate::search::policy::CHUNKING_STRATEGY_VERSION;
pub(in crate::indexer) use crate::search::policy::SEMANTIC_SCHEMA_VERSION;
pub(in crate::indexer) use crate::search::semantic_manifest::ArtifactRecord;
pub(in crate::indexer) use crate::search::semantic_manifest::SemanticManifest;
pub(in crate::indexer) use crate::search::semantic_manifest::TierKind as SemanticTierKind;
pub(in crate::indexer) use crate::search::tantivy::SearchableIndexSummary;
pub(in crate::indexer) use crate::search::tantivy::TantivyIndex;
pub(in crate::indexer) use crate::search::tantivy::index_dir;
pub(in crate::indexer) use crate::search::tantivy::schema_hash_matches;
pub(in crate::indexer) use crate::search::vector_index::ROLE_ASSISTANT;
pub(in crate::indexer) use crate::search::vector_index::ROLE_SYSTEM;
pub(in crate::indexer) use crate::search::vector_index::ROLE_TOOL;
pub(in crate::indexer) use crate::search::vector_index::ROLE_USER;
pub(in crate::indexer) use crate::search::vector_index::vector_index_path;
pub(in crate::indexer) use crate::sources::config::Platform;
pub(in crate::indexer) use crate::sources::config::SourcesConfig;
pub(in crate::indexer) use crate::sources::provenance::LOCAL_SOURCE_ID;
pub(in crate::indexer) use crate::sources::provenance::Origin;
pub(in crate::indexer) use crate::sources::provenance::Source;
pub(in crate::indexer) use crate::sources::provenance::SourceKind;
pub(in crate::indexer) use crate::sources::provenance::path_to_safe_dirname;
pub(in crate::indexer) use crate::storage::sqlite::FrankenStorage;
pub(in crate::indexer) use crate::storage::sqlite::FtsConsistencyRepair;
pub(in crate::indexer) use crate::storage::sqlite::HistoricalSalvageOutcome;
pub(in crate::indexer) use crate::storage::sqlite::LEXICAL_REBUILD_PLANNER_ESTIMATED_BYTES_PER_MESSAGE;
pub(in crate::indexer) use crate::storage::sqlite::SemanticIdentityTier;
pub(in crate::indexer) use crate::storage::sqlite::incompatible_legacy_fts_shadow_ddl;
pub(in crate::indexer) use crate::storage::sqlite::seed_canonical_from_best_historical_bundle;
pub(in crate::indexer) use anyhow::Context;
pub(in crate::indexer) use anyhow::Result;
pub(in crate::indexer) use crossbeam_channel::Receiver;
pub(in crate::indexer) use crossbeam_channel::Sender;
pub(in crate::indexer) use crossbeam_channel::TrySendError;
pub(in crate::indexer) use crossbeam_channel::bounded;
pub(in crate::indexer) use crossbeam_channel::never;
pub(in crate::indexer) use crossbeam_channel::select;
pub(in crate::indexer) use frankensearch::index::VectorIndex as FsVectorIndex;
pub(in crate::indexer) use notify::RecursiveMode;
pub(in crate::indexer) use notify::Watcher;
pub(in crate::indexer) use notify::event::AccessKind;
pub(in crate::indexer) use notify::event::AccessMode;
pub(in crate::indexer) use notify::event::MetadataKind;
pub(in crate::indexer) use notify::event::ModifyKind;
pub(in crate::indexer) use notify::recommended_watcher;
pub(in crate::indexer) use rayon::ThreadPool;
pub(in crate::indexer) use rayon::ThreadPoolBuilder;
pub(in crate::indexer) use semantic::EmbeddingInput;
pub(in crate::indexer) use semantic::SemanticIndexer;
pub(in crate::indexer) use semantic::packet_embedding_inputs_from_storage_since;
pub(in crate::indexer) use semantic::semantic_doc_id_for_input;
pub(in crate::indexer) use semantic::visit_packet_embedding_inputs_from_storage;
pub(in crate::indexer) use std::any::Any;
pub(in crate::indexer) use std::collections::BTreeMap;
pub(in crate::indexer) use std::collections::BTreeSet;
pub(in crate::indexer) use std::collections::HashMap;
pub(in crate::indexer) use std::collections::HashSet;
pub(in crate::indexer) use std::collections::VecDeque;
pub(in crate::indexer) use std::fs;
pub(in crate::indexer) use std::fs::File;
pub(in crate::indexer) use std::fs::OpenOptions;
pub(in crate::indexer) use std::io::BufReader;
pub(in crate::indexer) use std::io::BufWriter;
pub(in crate::indexer) use std::io::Read;
pub(in crate::indexer) use std::io::Seek;
pub(in crate::indexer) use std::io::SeekFrom;
pub(in crate::indexer) use std::io::Write;
pub(in crate::indexer) use std::path::Path;
pub(in crate::indexer) use std::path::PathBuf;
pub(in crate::indexer) use std::rc::Rc;
pub(in crate::indexer) use std::sync::Arc;
pub(in crate::indexer) use std::sync::Condvar;
pub(in crate::indexer) use std::sync::Mutex;
pub(in crate::indexer) use std::sync::atomic::AtomicBool;
pub(in crate::indexer) use std::sync::atomic::AtomicI64;
pub(in crate::indexer) use std::sync::atomic::AtomicU8;
pub(in crate::indexer) use std::sync::atomic::AtomicU64;
pub(in crate::indexer) use std::sync::atomic::AtomicUsize;
pub(in crate::indexer) use std::sync::atomic::Ordering;
pub(in crate::indexer) use std::thread;
pub(in crate::indexer) use std::thread::JoinHandle;
pub(in crate::indexer) use std::time::Duration;
pub(in crate::indexer) use std::time::Instant;
pub(in crate::indexer) use std::time::SystemTime;
pub(in crate::indexer) use std::time::UNIX_EPOCH;
pub(in crate::indexer) use tempfile::Builder as TempDirBuilder;

pub use crate::connectors::get_connector_factories;
pub use index_dispatch::{run_index, run_index_with_local_connector_roots};
pub use ingest_quarantine::{
    ConversationIngestQuarantineSummary, conversation_ingest_quarantine_summary,
    plan_quarantine_retry, run_quarantine_retry,
};
pub use lexical_rebuild_settings::{
    LexicalRebuildProfile, lexical_rebuild_profile, lexical_rebuild_profile_report,
    set_lexical_rebuild_profile,
};
pub use lexical_rebuild_state::IndexRunBusy;
pub use progress_and_stats::{
    ConnectorStats, IndexInterrupted, IndexOptions, IndexerEvent, IndexingProgress, IndexingStats,
    LexicalRepairStats, RegionReprojectStats, ReindexCommand, SemanticWatchOnceStats, StaleAction,
    StaleConfig, StaleDetector, StaleStats, set_robot_trace_ingest_enabled,
};
pub use raw_mirror_capture::{apply_workspace_rewrite, build_scan_roots};
pub use storage_preflight::{
    LexicalSegmentGcReport, background_migration_repair_pending_bytes, run_lexical_segment_gc,
};
pub use streaming_limiter::IndexMessage;
pub use streaming_pipeline::streaming_index_enabled;
pub use wal_close::best_effort_abort_wal_checkpoint;

#[cfg(test)]
pub(crate) use archive_fingerprint::load_lexical_rebuild_snapshot;
pub(crate) use archive_fingerprint::{
    ARCHIVE_FINGERPRINT_CACHE_FILE, lexical_storage_fingerprint_for_db,
    lexical_storage_fingerprint_for_db_cached, lexical_storage_fingerprint_for_db_cached_readonly,
    lexical_storage_fingerprint_for_db_strict, lexical_storage_fingerprint_for_storage,
    load_active_lexical_rebuild_pipeline_runtime, load_lexical_rebuild_checkpoint,
    prime_archive_fingerprint_cache_from_completed_checkpoint,
    write_archive_fingerprint_cache_best_effort,
};
pub(crate) use expected_lexical_docs::{
    ExpectedLexicalDocsScanPosition, LexicalRebuildOutcome, expected_lexical_docs_scan_position,
};
pub(crate) use fallback_fts_repair::LexicalRebuildCheckpoint;
#[cfg(test)]
pub(crate) use fallback_fts_repair::LexicalRebuildSnapshot;
pub(crate) use index_dispatch::{mirrored_root_owners, run_cli_index};
pub(crate) use ingest_oom::{WATCH_NOMEM_DEFERRALS_FILE, WatchNomemDeferral, WatchNomemDeferrals};
#[cfg(any(unix, test))]
pub(crate) use ingest_quarantine::try_acquire_semantic_backfill_lock;
pub(crate) use ingest_quarantine::{
    SearchMaintenanceMutationGuard, acquire_quarantine_mutation_lock,
    acquire_search_asset_mutation_lock, acquire_search_maintenance_mutation_lock,
    acquire_semantic_backfill_lock, clear_operator_quarantine_poison_records,
    operator_quarantine_poison_records, quarantine_source_missing_ids,
};
pub(crate) use lexical_checkpoint_io::{
    LexicalDocCountMismatch, completed_lexical_storage_fingerprint_matches_total,
    has_usable_lexical_publication_receipt, verify_published_lexical_doc_count,
};
pub(crate) use lexical_packet_prep::{
    LexicalRebuildEquivalenceEvidence, LexicalRebuildEquivalenceGoldenHit,
};
pub(crate) use lexical_policy::{
    LexicalPopulationStrategy, db_size_bytes_for_incremental_lexical_repair_policy,
    incremental_authoritative_lexical_repair_max_db_bytes,
};
#[cfg(target_os = "linux")]
pub(crate) use lexical_publish_ops::{atomic_exchange_paths, linux_atomic_exchange_is_unsupported};
pub(crate) use lexical_rebuild_controller::lexical_rebuild_page_fetch_message_bytes;
pub(crate) use lexical_rebuild_producer::{
    SearchLexicalRepairOutcome, rebuild_tantivy_from_db,
    refresh_completed_lexical_rebuild_checkpoint_from_live_index,
    repair_lexical_index_from_canonical_db_for_search,
};
pub(crate) use lexical_rebuild_settings::{
    LexicalRebuildPipelineSettingsSnapshot, lexical_rebuild_pipeline_settings_snapshot,
    lexical_rebuild_pipeline_settings_snapshot_passive,
};
pub(crate) use lexical_rebuild_state::LexicalRebuildPipelineRuntimeSnapshot;
pub(crate) use lexical_shard_planner::{
    LexicalShardPlan, LexicalShardPlanShard, LexicalShardPlannerBudgets,
    LexicalShardPlannerConversation, plan_lexical_rebuild_shards,
};
pub(crate) use progress_and_stats::{
    ConnectorScanFailure, INDEX_PHASE_ANALYTICS_REBUILD, INDEX_PHASE_LEXICAL_INDEXING,
    INDEX_PHASE_PREPARING, INDEX_PHASE_SCANNING, INDEX_PHASE_SEMANTIC_EMBEDDING,
    INDEX_PHASE_SEMANTIC_FINALIZE, INDEX_PHASE_SEMANTIC_HNSW, INDEX_PHASE_SEMANTIC_INITIALIZE,
    INDEX_PHASE_SEMANTIC_MANIFEST, INDEX_PHASE_SEMANTIC_REPLAY,
    INDEX_PHASE_SEMANTIC_VECTOR_PUBLISH,
};
pub(crate) use raw_mirror_capture::inject_provenance;
pub(crate) use run_lock::{
    IndexRunLockGuard, LEXICAL_REBUILD_PAGE_SIZE_PUBLIC, lexical_rebuild_page_size_is_compatible,
};
pub(crate) use source_filters::LexicalRebuildPacketFingerprintInput;
pub(crate) use storage_preflight::{
    FULL_REBUILD_HEADROOM_FORMULA, FullRebuildHeadroomProjection, LEXICAL_REBUILD_DISK_STOP_MARKER,
    LexicalIndexFootprint, existing_headroom_probe_paths, full_rebuild_headroom_projection,
    index_disk_headroom_check_disabled, lexical_index_footprint,
    lexical_rebuild_free_space_verdict,
};
pub(crate) use wal_close::{
    CASS_TEST_WAL_CHECKPOINT_PARK_MS_ENV, checkpoint_wal_truncate_with_deadline,
    close_storage_with_wal_checkpoint,
};

pub(in crate::indexer) use archive_fingerprint::{
    grant_final_checkpoint_refresh_grace, persist_connector_scan_watermarks,
    persist_final_index_run_metadata, published_lexical_index_validated_for_current_data,
    refresh_completed_lexical_rebuild_checkpoint,
    refresh_completed_lexical_rebuild_checkpoint_for_final_state,
    refresh_completed_watch_lexical_checkpoint, repair_daily_stats_if_drifted,
    should_repair_daily_stats_after_historical_salvage,
};
pub(in crate::indexer) use expected_lexical_docs::{
    IncrementalCanonicalLexicalRepairContext, IncrementalCanonicalLexicalRepairPlan,
    count_total_messages_exact, expected_live_lexical_doc_count_cached,
    expected_live_lexical_doc_count_cached_with_scan_policy, maintain_requested_hnsw,
    max_conversation_id_exact, publish_direct_semantic_artifact,
    semantic_identity_tier_for_requested_embedder, semantic_index_has_current_contract,
    semantic_indexing_now_ms, semantic_model_revision_for_embedder_id,
    semantic_tier_for_embedder_id, semantic_vector_matches_current_contract,
    storage_semantic_identity_tier, system_time_to_epoch_millis,
};
pub(in crate::indexer) use fallback_fts_repair::{
    DailyStatsRepairOutcome, FTS_REPAIR_FAILURE_ESCALATION_RUNS, FallbackFtsRepairOutcome,
    LexicalRebuildStartupOptions, can_skip_absent_explicit_watch_once_index_run,
    can_skip_unchanged_explicit_watch_once_index_run,
    choose_incremental_canonical_lexical_repair_plan, clear_fts_repair_failure_streak,
    deferred_lexical_rebuild_db_state, fts_repair_failure_streak_path,
    full_rebuild_requires_historical_restart, lexical_rebuild_db_state_with_exact_totals,
    lexical_rebuild_db_state_with_total_conversations, record_fts_repair_failure_streak,
    repair_fallback_fts_after_full_index_run, should_evaluate_incremental_canonical_lexical_repair,
    should_probe_pending_historical_bundles, should_repair_fallback_fts_after_full_index_run,
    should_run_targeted_watch_once_only, should_salvage_historical_databases,
    should_skip_broad_scan_after_watch_once_authoritative_repair,
    should_skip_unchanged_explicit_watch_once_paths,
};
pub(in crate::indexer) use index_dispatch::{
    LocalConnectorRootsOverride, colon_separated_exclusion_warning, configured_connector_factories,
    detect_for_local_scan, explicit_scan_root_since_ts, filter_disabled_connector_factories,
    mirrored_root_may_hold, non_watch_scan_since_ts, record_scan_contract_with_watermark_held,
    run_batch_index, run_region_reprojection_lane, run_streaming_index_with_connector_factories,
};
pub(in crate::indexer) use ingest_oom::{
    WatchOomIngestMode, conversation_payload_watermark_ms, error_is_out_of_memory,
    format_error_chain, ingest_batch_detailed_with_source_completion,
    ingest_non_watch_batch_with_oom_split, ingest_non_watch_oom_retry_or_quarantine,
    ingest_watch_batch_with_oom_split, ingest_watch_batch_with_oom_split_inner,
    save_watch_state_watermark, watch_conversation_in_nomem_backoff,
};
pub(in crate::indexer) use ingest_quarantine::{
    INDEX_INGEST_POISON_FILE, PoisonMemoryAttribution, WATCH_INGEST_POISON_FILE,
    clear_poison_conversations_after_successful_ingest, current_cass_version,
    mark_stale_index_ingest_quarantine_retry_attempted, record_index_poison_conversation,
    record_watch_poison_conversation, should_force_watch_solo_retry_oom,
    should_inject_non_watch_ingest_test_oom, should_inject_watch_ingest_test_oom,
    stale_index_ingest_quarantine_version_retry,
};
pub(in crate::indexer) use lexical_checkpoint_io::{
    ResumedLexicalCandidateRetained, clear_lexical_rebuild_state,
    completed_lexical_checkpoint_conversations, count_total_conversations_exact,
    index_meta_fingerprint, legacy_lexical_resume_lost_its_generation,
    lexical_rebuild_bound_candidate_is_missing, lexical_rebuild_content_fingerprint,
    lexical_rebuild_content_fingerprint_value, lexical_rebuild_deferred_content_fingerprint,
    lexical_rebuild_resume_content_path, lexical_rebuild_storage_fingerprint,
    live_tantivy_doc_count, load_lexical_rebuild_state,
    maybe_persist_staged_lexical_rebuild_progress, normalize_lexical_rebuild_state_for_current_run,
    persist_lexical_publication_receipt, persist_lexical_rebuild_state,
    persist_lexical_rebuild_state_for_active_run_start, persist_pending_lexical_rebuild_progress,
    prepare_lexical_rebuild_state_for_active_run, reconcile_pending_lexical_commit,
    retain_missing_lexical_candidate_checkpoint, retain_uncertifiable_resumed_candidate,
    sync_parent_directory, upgrade_lexical_rebuild_state_resume_cursor_if_needed,
    validate_complete_lexical_rebuild_shard_artifacts, validate_lexical_rebuild_shard_build_result,
    verify_lexical_generation_not_hollow_after_run, write_json_pretty_atomically,
};
pub(in crate::indexer) use lexical_packet_prep::{
    LexicalRebuildEquivalenceAccumulator, assign_lexical_rebuild_flow_reservation_bytes,
    build_lexical_rebuild_shard_index_summary_with_writer_parallelism,
    flush_streamed_lexical_rebuild_batch, lexical_rebuild_contract_from_canonical_messages,
    lexical_rebuild_noise_role, lexical_rebuild_packet_provenance_from_canonical,
    prepare_lexical_rebuild_packet_batch,
};
pub(in crate::indexer) use lexical_policy::{
    DeferredIncrementalCanonicalLexicalRepair, EMPTY_ARCHIVE_LEXICAL_REBUILD_REASON,
    LexicalRebuildPipelineSinkRuntimeSnapshot, MatchingLexicalRebuildStateStatus,
    ReadonlyRebuildBoundary, RunIndexProgressReset, canonical_rebuild_required_from_env,
    exact_total_counts_from_progress, install_lexical_index_heartbeat, lexical_index_heartbeat,
    lexical_population_strategy_requires_inline_tantivy,
    matching_completed_lexical_rebuild_state_status_without_fingerprint,
    matching_lexical_rebuild_state_status_if_present, non_watch_ingest_chunk_size,
    nonresumable_pending_lexical_rebuild_state_for_db,
    nonresumable_pending_lexical_rebuild_status_without_fingerprint,
    observed_tantivy_docs_for_post_full_scan_skip, open_lexical_index_with_heartbeat,
    open_readonly_rebuild_storage, preflight_fts_shadow_before_lexical_readers,
    record_deferred_incremental_canonical_lexical_repair, record_exact_total_counts_in_progress,
    record_incremental_canonical_lexical_repair, record_lexical_population_strategy,
    record_lexical_population_strategy_if_unset, record_semantic_watch_once_stats,
    refresh_and_maybe_apply_lexical_rebuild_pipeline_runtime, reset_progress_to_idle,
    resolve_lexical_population_strategy, should_bootstrap_missing_incremental_scan_watermark,
    should_defer_incremental_authoritative_lexical_repair, should_force_authoritative_rebuild,
    should_preflight_existing_tantivy_reader,
    should_preserve_matching_completed_lexical_checkpoint_during_full_scan,
    should_redrive_final_lexical_checkpoint_refresh_after_pre_scan_repair,
    should_skip_noop_final_lexical_checkpoint_refresh,
    should_skip_post_full_scan_authoritative_rebuild, should_try_readonly_canonical_force_rebuild,
    should_try_readonly_nonresumable_lexical_resume, try_readonly_canonical_force_rebuild,
    validate_canonical_rebuild_scope,
};
pub(in crate::indexer) use lexical_publish_ops::{
    directory_size_bytes_best_effort,
    finalize_staged_lexical_rebuild_publish_artifact_from_artifacts,
    lexical_post_run_maintenance_skipped_for_test,
    maybe_pause_lexical_rebuild_after_commit_for_kill, publish_staged_lexical_index,
    recover_or_finalize_interrupted_lexical_publish_backup, resume_reconcile_upsert_budget,
    staged_lexical_rebuild_scratch_path,
};
pub(in crate::indexer) use lexical_rebuild_controller::{
    LEXICAL_PIPELINE_BLOCKED_WAIT_LOG_INTERVAL, LexicalRebuildFirstBudgetPromotionWait,
    LexicalRebuildPipelineBudgetController, LexicalRebuildPipelineBudgetSnapshot,
    LexicalRebuildProducerParkSite, LexicalRebuildProducerTelemetry,
    LexicalRebuildResponsivenessController, apply_lexical_rebuild_budget_transition,
    format_lexical_rebuild_loadavg_1m_milli, lexical_pipeline_trace_enabled,
    lexical_rebuild_content_bounded_page_conversation_limit,
    lexical_rebuild_first_budget_promotion_wait, lexical_rebuild_page_prep_worker_parallelism,
    lexical_rebuild_page_prep_worker_parallelism_configured, lexical_rebuild_pipeline_channel_size,
    lexical_rebuild_pipeline_max_message_bytes_in_flight,
    lexical_rebuild_runtime_pipeline_budget_snapshot,
    pending_lexical_rebuild_batch_reached_runtime_limit, should_commit_lexical_rebuild,
    should_persist_lexical_rebuild_progress,
};
pub(in crate::indexer) use lexical_rebuild_once::rebuild_tantivy_from_db_with_options;
pub(in crate::indexer) use lexical_rebuild_producer::{
    LexicalRebuildPipelineMessage, LexicalRebuildPreparedPage, StreamingByteReservation,
    lexical_rebuild_conversation_chunks, lexical_rebuild_prepared_page_reserved_bytes,
    rebuild_tantivy_from_db_deferred_startup_with_options,
    rebuild_tantivy_from_db_deferred_startup_with_progress_bump,
    rebuild_tantivy_from_db_with_progress_bump, spawn_lexical_rebuild_packet_producer,
};
pub(in crate::indexer) use lexical_rebuild_settings::{
    LexicalRebuildResponsivenessPolicy, build_lexical_rebuild_worker_pool,
    lexical_rebuild_batch_fetch_conversation_limit, lexical_rebuild_commit_interval_conversations,
    lexical_rebuild_commit_interval_message_bytes, lexical_rebuild_commit_interval_messages,
    lexical_rebuild_commit_intervals_for_state,
    lexical_rebuild_controller_loadavg_high_watermark_1m_milli_for_available_and_reserved,
    lexical_rebuild_controller_loadavg_low_watermark_1m_milli_from_high,
    lexical_rebuild_controller_restore_clear_samples, lexical_rebuild_controller_restore_hold,
    lexical_rebuild_host_loadavg_1m_milli, lexical_rebuild_initial_batch_fetch_conversation_limit,
    lexical_rebuild_initial_commit_interval_conversations,
    lexical_rebuild_initial_commit_interval_message_bytes,
    lexical_rebuild_initial_commit_interval_messages, lexical_rebuild_progress_heartbeat_interval,
    lexical_rebuild_progress_heartbeat_interval_conversations,
    lexical_rebuild_responsiveness_policy, lexical_rebuild_setting,
    lexical_rebuild_worker_parallelism,
};
pub(in crate::indexer) use lexical_rebuild_staged::rebuild_tantivy_from_db_via_staged_shards;
pub(in crate::indexer) use lexical_rebuild_state::{
    ArchiveAdmission, AuthoritativeLexicalRefreshLedgerInput, LexicalRebuildDbState,
    LexicalRebuildExecutionMode, LexicalRebuildState, acquire_archive_write_lease,
    acquire_index_run_lock_with_archive_admission, acquire_index_run_lock_with_job_kind,
    build_authoritative_lexical_refresh_ledger, lexical_rebuild_state_path,
    log_lexical_generation_manifest_published, log_lexical_refresh_ledger_published,
    persist_lexical_rebuild_generation_artifacts, persist_lexical_refresh_ledger,
};
pub(in crate::indexer) use lexical_shard_planner::{
    LEXICAL_REBUILD_STAGED_SHARD_BUILD_AMPLIFICATION_FLOOR_MILLI,
    LEXICAL_REBUILD_STAGED_SHARD_BUILD_AMPLIFICATION_HEADROOM_MILLI,
    LEXICAL_REBUILD_STAGED_SHARD_BUILD_MIN_ESTIMATED_BYTES, LexicalRebuildPlannedShardCursor,
    lexical_rebuild_pending_shard_build_max_jobs,
    lexical_rebuild_pending_shard_build_max_message_bytes,
    lexical_rebuild_staged_merge_worker_parallelism,
    lexical_rebuild_staged_merge_worker_parallelism_configured,
    lexical_rebuild_staged_shard_build_emergency_memory_reserve_bytes,
    lexical_rebuild_staged_shard_build_memory_reserve_bytes,
    lexical_rebuild_staged_shard_builder_parallelism,
    lexical_rebuild_staged_shard_builder_parallelism_configured,
    lexical_rebuild_staged_shard_builder_settings,
    lexical_rebuild_staged_shard_builder_writer_parallelism_for_dispatch,
    lexical_rebuild_staged_shard_merge_settings, lexical_shard_message_count_is_known,
    usize_from_u64_saturating,
};
pub(in crate::indexer) use lexical_shard_workers::{
    LexicalRebuildPerfProfile, LexicalRebuildShardBuildMessage, LexicalRebuildShardBuildResult,
    LexicalRebuildShardBuildTelemetry, LexicalRebuildShardBuildWork,
    LexicalRebuildShardMergeArtifact, LexicalRebuildShardMergeCoordinator,
    LexicalRebuildShardMergeJob, LexicalRebuildShardMergeMessage, LexicalRebuildShardMergeWork,
    LexicalRebuildStagedMergeController, LexicalRebuildStagedShardBuildController,
    apply_staged_merge_runtime_snapshot, apply_staged_shard_build_runtime_snapshot,
    apply_staged_shard_build_telemetry_snapshot, commit_lexical_rebuild_progress,
    flush_streamed_lexical_rebuild_batch_for_planned_shard_boundary,
    log_lexical_rebuild_prep_profile_step, maintenance_job_kind_for_mode,
    reduce_staged_lexical_final_merge_frontier_via_workers,
    should_pause_staged_lexical_pipeline_handoff, should_reduce_staged_lexical_final_frontier,
    spawn_lexical_rebuild_shard_builder_workers, spawn_lexical_rebuild_shard_merge_workers,
};
pub(in crate::indexer) use progress_and_stats::{
    CanonicalMutationCounts, NonWatchIngestOutcome, check_legacy_omp_analytics_stop,
    record_connector_ingest_report, record_deferred_active_sources,
    report_legacy_omp_analytics_progress, robot_trace_ingest_finish, robot_trace_ingest_start,
};
pub(in crate::indexer) use raw_mirror_capture::{
    build_scan_roots_with_missing, capture_connector_sources_before_parse,
    capture_prepared_conversation, prepare_conversation_for_ingest,
    prepare_conversation_metadata_for_ingest,
};
pub(in crate::indexer) use run_index_finalize::run_index_with_storage_ownership;
pub(in crate::indexer) use run_index_lexical::{
    RunIndexLexicalOutcome, run_index_lexical_and_scan,
};
pub(in crate::indexer) use run_index_preflight::{
    RunIndexPreflightOutcome, run_index_preflight_and_open,
};
pub(in crate::indexer) use run_lock::{
    IndexRunLockHeartbeat, LEXICAL_REBUILD_PAGE_SIZE, LEXICAL_REBUILD_STATE_VERSION,
    PreflightTimeoutPolicy, WATCH_STARTUP_SUB_PHASE_TAXONOMY, WatchStartupPreflightState,
    WatchStartupPreflightWatchdog, bump_index_run_lock_progress_atomic,
    bump_index_run_lock_progress_if_present, dotenvy_truthy, index_run_lock_heartbeat_interval,
    lexical_rebuild_db_paths_match, lexical_rebuild_db_state_matches,
    lexical_rebuild_db_state_matches_legacy, preflight_breadcrumb_visible,
    preflight_cleanup_orphan_fk_rows_enabled, preflight_historical_salvage_discovery_enabled,
    preflight_skip, preflight_validate_fts_messages_enabled, watch_startup_preflight,
    watch_startup_step_idx,
};
pub(in crate::indexer) use semantic_watch_once::{
    WatchSemanticDelta, WatchSemanticMaintenance, finalize_one_shot_semantic_delta,
    incremental_semantic_embed, prepare_progress_for_semantic_build,
    run_targeted_semantic_watch_once_publish, semantic_artifact_for_tier,
    semantic_artifact_index_path, semantic_role_name, set_semantic_progress_phase,
    should_optimize_tantivy_after_watch_once_ingest, should_run_targeted_semantic_watch_once,
    update_semantic_progress,
};
#[cfg(target_os = "linux")]
pub(in crate::indexer) use source_filters::linux_publish_swap;
pub(in crate::indexer) use source_filters::{
    ACTIVE_SESSION_SOURCE_SKIP_OBSERVED, ActiveSessionSourceFilter, BatchClassificationMap,
    LEXICAL_REBUILD_PACKET_VERSION, LexicalRebuildChunk, LexicalRebuildChunkGate,
    LexicalRebuildConversationPacket, LexicalRebuildPacketDiagnostics,
    LexicalRebuildPacketIdentity, LexicalRebuildPacketProvenance,
    LexicalRebuildPacketProvenanceMode, LexicalRebuildPacketSource,
    NON_WATCH_INGEST_CHUNK_SIZE_MAX, NON_WATCH_INGEST_DEFAULT_CHUNK_SIZE,
    PREPARSE_PRIMARY_SOURCE_CAPTURE_LIMIT, ROBOT_TRACE_INGEST_BATCH_N, ROBOT_TRACE_INGEST_ENABLED,
    SourceFileId, WATCH_INGEST_CHUNK_SIZE_MAX, WATCH_INGEST_DEFAULT_CHUNK_SIZE,
    WORKSPACE_EXCLUSION_SCAN_PREFIX, WorkspaceExclusions, record_connector_scan_watermark,
    should_skip_active_session_source, should_skip_excluded_source, should_skip_subagent,
    skip_subagents_active, source_file_id, workspace_exclusion_scan_policies,
};
pub(in crate::indexer) use storage_preflight::{
    INDEX_MIN_FREE_SPACE_BYTES, LEXICAL_REBUILD_STAGING_SUFFIX, SLOW_STORAGE_OPEN_WARN_THRESHOLD,
    canonical_archive_unhealthy_for_index_error, database_bundle_size_bytes,
    database_path_with_suffix, database_sidecar_paths,
    ensure_authoritative_lexical_rebuild_storage_headroom, ensure_index_startup_storage_headroom,
    ensure_lexical_rebuild_free_space, full_rebuild_existing_archive_integrity_preflight,
    lexical_rebuild_fold_shortfall, maybe_seed_empty_canonical_from_historical_bundle,
    open_storage_for_index, orphan_fk_cleanup_failed_index_error, storage_open_liveness_targets,
    targeted_watch_once_fts_parity_problem, targeted_watch_once_unhealthy_fts_error,
};
pub(in crate::indexer) use streaming_limiter::{
    DEFAULT_STREAMING_BATCH_LIMITS, LexicalRebuildReservationOrder, STREAMING_CHANNEL_SIZE,
    STREAMING_MAX_BYTES_IN_FLIGHT, StreamingBatchSender, StreamingByteLimiter,
    acquire_ordered_lexical_rebuild_page_budget, conversation_batch_footprint,
    lexical_rebuild_page_working_set_reservation_bytes, remember_discovered_connector,
    scan_with_durable_source_boundaries, source_file_observation, source_folder_is_dependency,
    source_ledger_key, source_ledger_matches_with_contract, source_observations_after_scan,
};
pub(in crate::indexer) use streaming_pipeline::{
    StreamingConsumerDisconnected, StreamingProducerConfig, panic_payload_message,
    run_streaming_consumer, run_streaming_index, scan_path_exclusions_active,
    scan_watermark_preservation_active, spawn_connector_producer,
};
pub(in crate::indexer) use wal_close::{
    best_effort_abort_wal_checkpoint_for_db, close_storage_after_index,
    close_watch_storage_for_recycle, release_watch_storage_after_index,
    reopen_watch_storage_if_pending, restore_watch_steady_state_checkpoint_policy,
};
pub(in crate::indexer) use watch_loop::{
    explicit_watch_once_file_may_be_database, is_database_watch_root, watch_scan_lower_bound,
    watch_sources,
};
pub(in crate::indexer) use watch_reindex::{
    WatchReindexOutcome, explicit_watch_once_root_unchanged_after_last_index,
    reindex_full_watch_roots, reindex_paths_with_semantic_delta,
};
pub(in crate::indexer) use watch_replay_debt::{
    WatchAbsentSourceRecovery, WatchLexicalReplayDebt, WatchRecoverySourceCoverage,
    clear_watch_lexical_replay_debt, load_watch_lexical_replay_debt,
    persist_watch_lexical_replay_debt, plan_remote_mirror_scan_skips,
    promote_missing_watch_replay_paths, recover_absent_watch_roots,
    recover_absent_watch_source_publications, watch_lexical_replay_debt_for_root,
    watch_lexical_replay_sources,
};
pub(in crate::indexer) use watch_spool::{
    WatchConversationSpool, WatchEventBacklog, WatchLoopEvent, WatchNotificationMailbox,
    build_watch_roots, collect_watch_notifications, dispatch_pending_watch_callback,
    enqueue_watch_notification, observe_index_raw_mirror_io, wait_watch_loop_event,
    watch_ingest_chunk_size,
};
pub(in crate::indexer) use watch_state::{
    ConnectorKind, additional_scan_roots_for_scan_or_watch, classify_paths,
    create_new_atomic_sidecar_file, expand_local_scan_root_path,
    finalize_watch_once_reindex_result, finalize_watch_reindex_result, load_watch_state,
    replace_file_from_temp, save_watch_state, set_progress_last_error, state_path,
    unique_atomic_sidecar_path, unique_atomic_temp_path, unique_failed_seed_backup_root,
    warn_unclaimed_explicit_watch_once_paths, watch_event_should_trigger_reindex,
};

#[cfg(test)]
pub(in crate::indexer) use archive_fingerprint::{
    lexical_storage_fingerprint_for_db_disk_cached, lexical_storage_physical_identity,
    persist_final_index_run_metadata_with_writer, rebuild_daily_stats_from_conversation_packets,
};
#[cfg(test)]
pub(in crate::indexer) use expected_lexical_docs::{
    EXPECTED_LEXICAL_DOCS_MAX_DELTA_GENERATIONS, EXPECTED_LEXICAL_DOCS_PARTIAL_FILE,
    ExpectedLexicalDocsCache, ExpectedLexicalDocsPartial, expected_lexical_docs_cache_path,
    expected_lexical_docs_identity, expected_live_lexical_doc_count,
    expected_live_lexical_doc_count_resumable_with,
};
#[cfg(test)]
pub(in crate::indexer) use fallback_fts_repair::{
    lexical_rebuild_db_state, nonfatal_fallback_fts_repair_outcome,
    should_skip_absent_explicit_watch_once_paths,
};
#[cfg(test)]
pub(in crate::indexer) use index_dispatch::{
    ConnectorFactory, connector_activity_time_lower_bound,
    connector_local_scan_since_ts_from_state, connector_local_scan_since_ts_map,
    run_batch_index_with_connector_factories,
};
#[cfg(test)]
pub(in crate::indexer) use ingest_oom::{
    WATCH_LEXICAL_TEST_FAULT, WatchLexicalTestFault, ingest_batch,
    inject_watch_lexical_test_failure, sort_watch_conversations_for_watermark,
    watch_nomem_deferral_backoff_ms,
};
#[cfg(test)]
pub(in crate::indexer) use ingest_quarantine::{
    INCREMENTAL_LEXICAL_UPDATE_TEST_OOM, NON_WATCH_INGEST_TEST_OOM_MIN_CONVS,
    poison_conversation_id, poison_record_cass_version, record_poison_conversation,
};
#[cfg(test)]
pub(in crate::indexer) use lexical_checkpoint_io::{
    LEXICAL_PUBLISHED_STATE_FILE, LEXICAL_PUBLISHED_STATE_MAX_BYTES,
    persist_pending_lexical_rebuild_progress_with_base_meta_fingerprint,
    resolve_legacy_lexical_rebuild_conversation_id_from_offset,
};
#[cfg(test)]
pub(in crate::indexer) use lexical_packet_prep::{
    LEXICAL_REBUILD_EQUIVALENCE_DEFAULT_PROBES, StreamingBatchFlowReservation,
    build_lexical_rebuild_shard_index,
};
#[cfg(test)]
pub(in crate::indexer) use lexical_policy::{
    BOOTSTRAP_LARGE_INCREMENTAL_MISSING_WATERMARK_REASON,
    DEFERRED_LARGE_INCREMENTAL_LEXICAL_REPAIR_REASON, matching_lexical_rebuild_state_status,
    nonresumable_pending_lexical_rebuild_status_from_readonly_db, parse_process_index_write_bytes,
    process_index_write_bytes, producer_state_transition_is_work,
    readonly_rebuild_needs_archive_recovery, select_lexical_population_strategy,
    should_probe_live_tantivy_docs_for_post_full_scan_skip,
};
#[cfg(test)]
pub(in crate::indexer) use lexical_publish_ops::{
    LEXICAL_TERMINAL_FAILPOINT, LexicalPublishRenameSite, RESUME_RECONCILE_SLACK_DOCS,
    finalize_staged_lexical_rebuild_publish_artifact, inject_lexical_publish_rename_failure_once,
    lexical_publish_backups_dir, lexical_publish_in_progress_backup_path,
    lexical_terminal_failpoint, merge_lexical_rebuild_shard_index_tree,
};
#[cfg(test)]
pub(in crate::indexer) use lexical_rebuild_controller::{
    LexicalRebuildBudgetTransition, bounded_lexical_rebuild_page_reservation_limits,
    lexical_rebuild_default_page_prep_worker_parallelism_for_workers,
};
#[cfg(test)]
pub(in crate::indexer) use lexical_rebuild_producer::{
    LEXICAL_REBUILD_PAGE_PREP_INJECTED_PANIC, LEXICAL_REBUILD_SHARD_BUILD_INJECTED_PANIC,
    LEXICAL_REBUILD_SHARD_MERGE_INJECTED_PANIC, LexicalRebuildPagePrepResult,
    LexicalRebuildPagePrepWork, ScopedPanicInjection, lexical_rebuild_page_prep_error,
    lexical_rebuild_shard_build_injected_panic_hook,
    lexical_rebuild_shard_merge_injected_panic_hook, rebuild_tantivy_from_db_deferred_startup,
    release_lexical_rebuild_prepared_page_reservation, send_lexical_rebuild_pipeline_error,
    spawn_lexical_rebuild_page_prep_workers,
};
#[cfg(test)]
pub(in crate::indexer) use lexical_rebuild_settings::{
    LexicalRebuildSettingSource, lexical_rebuild_available_parallelism,
    lexical_rebuild_configured_worker_parallelism_for_available,
    lexical_rebuild_default_batch_fetch_conversation_limit,
    lexical_rebuild_default_reserved_cores_for_available,
    lexical_rebuild_default_worker_parallelism_for_available,
    parse_lexical_rebuild_loadavg_1m_milli, resolve_lexical_rebuild_setting,
};
#[cfg(test)]
pub(in crate::indexer) use lexical_rebuild_state::{
    PendingLexicalCommit, acquire_index_run_lock, lexical_rebuild_equivalence_evidence_path,
    lexical_refresh_evidence_path, lexical_refresh_ledger_path,
};
#[cfg(test)]
pub(in crate::indexer) use lexical_shard_planner::{
    LEXICAL_SHARD_PLAN_VERSION, LEXICAL_SHARD_UNKNOWN_MESSAGE_COUNT,
    LexicalRebuildShardBuilderSettings, LexicalRebuildShardMergeSettings,
    lexical_rebuild_default_shard_budget, lexical_rebuild_default_shard_planner_budgets_for_totals,
    lexical_rebuild_default_staged_merge_worker_parallelism_for_workers,
    lexical_rebuild_default_staged_shard_builder_parallelism_for_workers,
    lexical_rebuild_default_staged_shard_builder_parallelism_for_workers_and_memory,
    lexical_rebuild_default_staged_shard_max_message_bytes_for_available_memory,
    lexical_rebuild_shard_planner_conversations_from_storage, lexical_rebuild_target_shard_count,
    lexical_shard_conversation_ids_fingerprint,
    plan_lexical_rebuild_shards_from_storage_with_settings,
};
#[cfg(test)]
pub(in crate::indexer) use lexical_shard_workers::{
    LEXICAL_REBUILD_FINAL_FRONTIER_FEDERATED_SHARD_LIMIT, LexicalRebuildStagedMergeRuntimeSnapshot,
    lexical_rebuild_amplification_milli,
};
#[cfg(test)]
pub(in crate::indexer) use progress_and_stats::staged_field_or_null;
#[cfg(test)]
pub(in crate::indexer) use raw_mirror_capture::{
    attach_raw_mirror_capture, canonicalize_claude_external_id,
    capture_discovered_source_file_before_parse, capture_scan_root_file_before_parse,
    capture_scan_sources_before_parse, should_skip_raw_mirror_capture_for_logical_source,
};
#[cfg(test)]
pub(in crate::indexer) use run_lock::{
    heartbeat_index_run_lock, heartbeat_index_run_lock_with_lock,
    heartbeat_index_run_lock_with_lock_and_progress, write_index_run_lock_heartbeat_in_place,
};
#[cfg(test)]
pub(in crate::indexer) use semantic_watch_once::{
    CanonicalSemanticDocuments, embed_incremental_semantic_inputs, inspect_semantic_artifact,
    update_incremental_semantic_watermark,
};
#[cfg(test)]
pub(in crate::indexer) use source_filters::{
    ActiveSessionSourceReason, message_id_from_db, saturating_u32_from_i64,
    source_file_has_active_advisory_lock,
};
#[cfg(test)]
pub(in crate::indexer) use storage_preflight::{
    IndexStorageHeadroomRequirement, LEXICAL_INDEX_ROOT_DIR, LEXICAL_PUBLISH_BACKUPS_DIR_NAME,
    anyhow_chain_indicates_retryable_storage_contention,
    background_migration_repair_pending_bytes_with_max_bytes, current_schema_fast_probe,
    ensure_index_storage_headroom, env_value_truthy,
    full_rebuild_existing_archive_integrity_preflight_with_max_bytes,
    full_rebuild_existing_storage_integrity_problem, full_rebuild_headroom_projection_from,
    full_rebuild_refusal_terms, index_startup_storage_headroom_requirement,
    lexical_rebuild_fold_fits, quarantine_failed_seed_bundle, raw_mirror_headroom_note,
    required_index_headroom_bytes, should_run_engine_backed_archive_integrity_preflight,
};
#[cfg(test)]
pub(in crate::indexer) use streaming_limiter::{
    StreamingBatchLimits, next_streaming_batch, source_ledger_matches, source_parent_observation,
};
#[cfg(test)]
pub(in crate::indexer) use streaming_pipeline::{
    STREAMING_COMMIT_MAX_PACED_INTERVAL, StreamingCommitPacer, is_streaming_consumer_disconnected,
    paced_streaming_commit_interval, scan_path_exclusions_value_active, send_conversation_batches,
    streaming_combine_enabled, streaming_combine_max_messages,
};
#[cfg(test)]
pub(in crate::indexer) use wal_close::{
    AbortWalCheckpointAttempt, FinalWalCheckpointOutcome, classify_final_wal_checkpoint,
    prepare_storage_for_final_checkpoint, run_bounded_abort_wal_checkpoint,
};
#[cfg(test)]
pub(in crate::indexer) use watch_loop::{database_root_poller, refresh_watch_root_registrations};
#[cfg(test)]
pub(in crate::indexer) use watch_reindex::{reindex_paths, reset_storage};
#[cfg(test)]
pub(in crate::indexer) use watch_replay_debt::{
    WATCH_LEXICAL_REPLAY_DEBT_PREFIX, WatchReplayRootStatus, watch_replay_root_status,
};
#[cfg(test)]
pub(in crate::indexer) use watch_spool::{
    WATCH_PENDING_MAX_PATHS, WatchSpoolReader, dispatch_watch_callback,
};
#[cfg(test)]
pub(in crate::indexer) use watch_state::unclaimed_explicit_watch_once_paths;
