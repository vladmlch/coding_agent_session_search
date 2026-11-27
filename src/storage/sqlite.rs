//! `SQLite` backend: schema, pragmas, and migrations.

/// Frankensqlite parameter list builder.
/// Kept before all `mod` declarations so every `sqlite::*` and `sqlite::tests::*`
/// submodule inherits `fparams!` lexically without path imports.
macro_rules! fparams {
    () => {
        &[] as &[$crate::franken_sync::compat::ParamValue]
    };
    ($($val:expr),+ $(,)?) => {
        &[$($crate::franken_sync::compat::ParamValue::from($val)),+]
            as &[$crate::franken_sync::compat::ParamValue]
    };
}

// Local fix (review 2026-11-02, S1): read the TRUNCATE checkpoint status row.
mod wal_checkpoint_status;
// Local fix (review 2026-11-02, S3): live index hint for lexical-rebuild tail.
mod footprint_tail_sql;
// Local fix (review 2026-11-02, U1): deduplicate multi-record Claude reply usage.
mod claude_usage_dedup;

// Production submodules:
mod admin_and_omp;
mod aggregators_and_pricing;
mod analytics_rebuild;
mod backup_and_integrity;
mod batch_ingest;
mod connection_pool;
mod conversation_tail_state;
mod conversation_tree_insert;
mod embeddings_and_daily_stats;
mod forget_and_dedup;
mod fts_parity_and_repair;
mod fts_rebuild;
mod lexical_rebuild;
mod omp_and_semantic_meta;
mod open;
pub(crate) mod open_and_locks;
mod orphan_fk_cleanup;
mod reconcile_and_codebuff;
mod schema_v1;
mod source_and_historical_import;
mod storage_core;
mod tx_analytics_rollups;
mod tx_conversation_insert;
mod tx_message_insert;

pub use open::SchemaRebuildRequired;
pub use schema_v1::BASELINE_SCHEMA_V1_SQL;

pub use connection_pool::{
    ConnectionManagerConfig, FrankenConnectionManager, FrankenOwnerConnection, LazyDbError,
    LazyFrankenDb, LazyFrankenDbGuard, WriterGuard,
};

#[cfg(test)]
pub(crate) use open_and_locks::retry_transient_storage_op;
pub(crate) use open_and_locks::{
    DeferAnalyticsUpdatesGuard, DoctorMutationDbOpenBypassGuard, MessageLookupTraceCounters,
    defer_analytics_updates_guard, enter_doctor_mutation_db_open_bypass,
    index_engine_migration_is_complete, message_lookup_trace_snapshot,
    open_current_schema_storage_with_timeout, open_franken_async_readonly_connection_with_timeout,
    open_franken_async_strict_readonly_connection_with_timeout,
    open_franken_owner_readonly_connection_with_timeout,
    open_franken_owner_strict_readonly_connection_with_timeout,
    open_franken_raw_connection_with_timeout, open_franken_raw_readonly_connection_with_timeout,
    open_franken_readonly_storage_with_timeout, open_franken_storage_with_timeout,
    retryable_franken_anyhow, retryable_franken_error, retryable_storage_error_message,
    set_message_lookup_trace_enabled, sleep_with_franken_retry_backoff,
};

pub use backup_and_integrity::{
    ConnectorScanState, DatabaseBundleMoveResult, FTS_MESSAGES_CORRUPTION_RECOVERY_HINT,
    FTS_MESSAGES_INTEGRITY_PROBE_SQL, FTS_MESSAGES_REQUIRED_SHADOW_TABLES, FTS5_DELETE_ALL_SQL,
    FTS5_REGISTER_SQL, FtsMessagesIntegrityError, MigrationError, RecoveryConversationRow,
    cleanup_old_backups, connector_scan_contract, create_backup,
    fts_messages_integrity_error_from_message, is_user_data_file,
    validate_fts_messages_integrity_for_connection,
};
pub(crate) use backup_and_integrity::{
    FTS_INDEXABLE_MESSAGE_COUNT_SQL, FtsConsistencyRepair, FtsDryRunParity, FtsShadowParity,
    FtsShadowParityStatus, database_sidecar_path, move_database_bundle,
    validate_fts_messages_integrity_for_async_connection,
};
#[cfg(test)]
pub(crate) use backup_and_integrity::{
    ensure_fts_consistency_via_rusqlite, rebuild_fts_via_rusqlite,
    recovery_conversation_row_from_lenient_columns, remove_database_files,
};

#[cfg(test)]
pub(crate) use backup_and_integrity::discover_historical_database_bundles;
pub use source_and_historical_import::HistoricalSalvageOutcome;
pub(crate) use source_and_historical_import::{
    message_extra_to_source_jsonl_line, seed_canonical_from_best_historical_bundle,
};

#[cfg(test)]
pub(crate) use omp_and_semantic_meta::{
    LEGACY_OMP_ANALYTICS_CURSOR_CONTEXT_META_KEY, LEGACY_OMP_ANALYTICS_CURSOR_LAST_ID_META_KEY,
    LEGACY_OMP_ANALYTICS_CURSOR_PROCESSED_META_KEY, legacy_omp_analytics_pending,
};
pub use omp_and_semantic_meta::{CURRENT_SCHEMA_VERSION, LegacyOmpReclassificationResult};
pub(crate) use omp_and_semantic_meta::{
    MIN_IN_PLACE_MIGRATION_SCHEMA_VERSION, SemanticIdentityTier,
    legacy_omp_analytics_pending_with_query,
};
pub use open_and_locks::SchemaCheck;

pub(crate) use lexical_rebuild::LEXICAL_REBUILD_PLANNER_ESTIMATED_BYTES_PER_MESSAGE;
pub use lexical_rebuild::{
    EmbeddingJobRow, LEXICAL_MAX_MESSAGE_CONTENT_BYTES_DEFAULT,
    LexicalRebuildConversationFootprintRow, LexicalRebuildConversationRow,
    LexicalRebuildGroupedMessageRow, LexicalRebuildGroupedMessageRows, LexicalRebuildMessageRow,
    lexical_max_message_content_bytes,
};

#[cfg(test)]
pub(crate) use storage_core::close_franken_in_place_with_busy_retry;
pub(crate) use storage_core::error_message_indicates_populated_fts_shadow_without_rowid_reload;
pub use storage_core::{FrankenStorage, SqliteStorage};

pub use omp_and_semantic_meta::active_schema_migration_versions;

pub use reconcile_and_codebuff::{InsertOutcome, SourceIngestLedgerEntry};

pub use conversation_tail_state::MessageForEmbedding;

pub(crate) use tx_conversation_insert::{
    FtsShadowViability, error_message_indicates_fts_shadow_not_viable,
    explicit_analytics_rebuild_deferred, fts_shadow_max_messages, fts_shadow_not_viable_detail,
    incompatible_legacy_fts_shadow_ddl,
};

pub use aggregators_and_pricing::{
    AnalyticsRollupAggregator, IndexingCache, IndexingCacheStorage, MessageMetricsEntry,
    PricingDiagnostics, PricingEntry, PricingTable, StatsAggregator, StatsDelta,
    TokenStatsAggregator, TokenStatsDelta, TokenUsageEntry, UsageRollupDelta,
};

pub use analytics_rebuild::{AnalyticsRebuildResult, DailyCount, DailyStatsRebuildResult};

pub use forget_and_dedup::{
    AgentArchivePurgeResult, ExternalIdDuplicateCandidate, ExternalIdDuplicateCleanupResult,
    ForgetConversationsFilter, ForgetConversationsResult, ForgetExclusionList,
    ForgetExclusionSelector, ForgetSessionSample, ForgottenConversationExclusion,
    ForgottenConversationKey, ForgottenSourceExclusion, MirrorContainerOwners,
    MisattributedMirrorCleanupResult, MisattributedMirrorRow, ReincludeForgetExclusionsResult,
    RetiredCodexSegmentRow, SourceFileStamp,
};

pub use embeddings_and_daily_stats::DailyStatsHealth;

pub use fts_rebuild::FtsEntry;

// =============================================================================
// Tests (bead yln.4)
// =============================================================================

#[cfg(test)]
pub(crate) mod tests;
