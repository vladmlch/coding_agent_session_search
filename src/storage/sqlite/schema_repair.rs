use super::analytics_rebuild::is_out_of_memory_error;
use super::backup_and_integrity::database_sidecar_path;
use super::schema_migrations::{
    MIGRATION_V14, MIGRATION_V15_TAIL_STATE_TABLE, MIGRATION_V16, MIGRATION_V17, MIGRATION_V18,
    MIGRATION_V19, MIGRATION_V20, MIGRATION_V21, MIGRATION_V22, MIGRATION_V23,
};
use crate::franken_sync::{
    Connection as FrankenConnection, Row as FrankenRow, SqliteValue,
    compat::{
        ConnectionExt as FrankenConnectionExt, ParamValue, RowExt as FrankenRowExt,
        Transaction as FrankenTransaction, TransactionExt as FrankenTransactionExt,
    },
    migrate::MigrationRunner,
};
use anyhow::{Context, Result, anyhow};
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::time::Instant;
use thiserror::Error;
use tracing::info;

// -------------------------------------------------------------------------
// Frankensqlite migration helpers
// -------------------------------------------------------------------------

/// Pending-migration steps applied before the V15 tail-state migration.
///
/// Uses the single combined V13 migration that creates the complete final
/// schema in one step. This avoids the V5 `DROP TABLE conversations`
/// operation which triggers a known frankensqlite limitation: autoindex
/// entries in sqlite_master are not properly cleaned up during DROP TABLE,
/// causing "sqlite_master entry not found" errors.
///
/// Each step runs through its own single-migration [`MigrationRunner`]
/// invocation so a failure is attributed to the exact version and name
/// instead of an opaque "base schema migrations" context (GH #349). Every
/// migration already runs inside its own transaction inside the runner, so
/// splitting the runner is behavior-preserving.
pub(super) const BASE_MIGRATION_STEPS: &[(i64, &str, &str)] = &[
    (13, "full_schema_v13", MIGRATION_FRESH_SCHEMA),
    (14, "fts_contentless", MIGRATION_V14),
];

/// Pending-migration steps applied after the V15 tail-state migration, run
/// one-per-runner for failure attribution (see [`BASE_MIGRATION_STEPS`]).
pub(super) const POST_TAIL_CACHE_MIGRATION_STEPS: &[(i64, &str, &str)] = &[
    (16, "drop_redundant_message_conv_idx", MIGRATION_V16),
    (17, "drop_message_created_idx", MIGRATION_V17),
    (18, "conversation_tail_state_hot_table", MIGRATION_V18),
    (19, "conversation_external_lookup", MIGRATION_V19),
    (20, "conversation_external_tail_lookup", MIGRATION_V20),
    (21, "conversation_context_index", MIGRATION_V21),
    (22, "forgotten_sources", MIGRATION_V22),
    (23, "forgotten_conversations", MIGRATION_V23),
];

/// Migration versions that ordinary storage open may apply when their ledger
/// rows are absent. Restores must retain every admitted step, including V15's
/// separately implemented migration; a matching maximum alone is insufficient.
pub fn active_schema_migration_versions() -> impl Iterator<Item = i64> {
    BASE_MIGRATION_STEPS
        .iter()
        .map(|&(version, _, _)| version)
        .chain(std::iter::once(15))
        .chain(
            POST_TAIL_CACHE_MIGRATION_STEPS
                .iter()
                .map(|&(version, _, _)| version),
        )
}

/// Run each pending migration through its own single-migration runner so an
/// error names the exact failing step. Returns the applied versions plus the
/// first step's `was_fresh` signal — the same value the combined-runner form
/// this replaces used to report.
pub(super) fn run_attributed_migration_steps(
    conn: &FrankenConnection,
    steps: &[(i64, &'static str, &'static str)],
) -> Result<(Vec<i64>, bool)> {
    let mut applied = Vec::new();
    let mut was_fresh = false;
    for (index, &(version, name, sql)) in steps.iter().enumerate() {
        let result = MigrationRunner::new()
            .add(version, name, sql)
            .run(conn)
            .with_context(|| format!("running schema migration v{version} ({name})"))?;
        if index == 0 {
            was_fresh = result.was_fresh;
        }
        applied.extend(result.applied);
    }
    Ok((applied, was_fresh))
}

/// Environment override for the GH #349 legacy-FTS teardown refusal budget,
/// in bytes of on-disk archive family (main db + sidecars). `0` disables the
/// refusal entirely and forces the unbounded teardown attempt.
pub(super) const LEGACY_FTS_TEARDOWN_BUDGET_ENV: &str =
    "CASS_MIGRATION_LEGACY_FTS_DROP_BUDGET_BYTES";

/// Default refusal budget: archives above this size never attempt the V14
/// legacy `fts_messages` teardown automatically.
const DEFAULT_LEGACY_FTS_TEARDOWN_BUDGET_BYTES: u64 = 512 * 1024 * 1024;

/// Typed refusal of the V14 `fts_contentless` teardown of a large legacy
/// `fts_messages` FTS5 shadow (GH #349).
///
/// The drop is unbounded inside frankensqlite (WITHOUT-ROWID shadow teardown)
/// and aborts with `out of memory` after minutes and multi-GiB RSS on
/// multi-GB archives. Refusing up front keeps the archive untouched, names
/// the failing migration, and carries the safe next actions.
#[derive(Debug, Error)]
#[error(
    "migration v14 (fts_contentless) refused: this archive still carries a legacy fts_messages \
     FTS5 shadow whose teardown is unbounded in frankensqlite (GH #345/#349: the drop aborts \
     with 'out of memory' after minutes on multi-GB archives); archive family is \
     {archive_bytes} bytes, above the {budget_bytes}-byte refusal budget. The archive was not \
     modified. Next actions: run 'cass doctor check --json' for a bounded read-only diagnosis; \
     upgrade to a cass build carrying the frankensqlite bounded WITHOUT-ROWID teardown fix and \
     retry 'cass index --full'; or back up the archive and rebuild a fresh one from provider \
     session logs. To force the unbounded attempt anyway, set \
     CASS_MIGRATION_LEGACY_FTS_DROP_BUDGET_BYTES to a byte value above the archive size \
     (0 disables the refusal entirely)."
)]
pub(crate) struct LegacyFtsTeardownRefusal {
    pub(crate) archive_bytes: u64,
    pub(crate) budget_bytes: u64,
}

/// Total on-disk size of the archive family: the main db plus its WAL, SHM,
/// and journal sidecars. Missing members contribute zero.
pub(super) fn archive_family_bytes(db_path: &Path) -> u64 {
    ["", "-wal", "-shm", "-journal"]
        .into_iter()
        .filter_map(|suffix| {
            std::fs::symlink_metadata(database_sidecar_path(db_path, suffix))
                .ok()
                .filter(|meta| meta.is_file())
                .map(|meta| meta.len())
        })
        .sum()
}

/// Resolve the legacy-FTS teardown refusal budget. `None` means the operator
/// explicitly disabled the refusal via a `0` budget; an unparsable value
/// falls back to the default with a warning rather than silently disabling
/// the gate.
pub(super) fn legacy_fts_teardown_budget_bytes() -> Option<u64> {
    match std::env::var(LEGACY_FTS_TEARDOWN_BUDGET_ENV) {
        Ok(raw) => match raw.trim().parse::<u64>() {
            Ok(0) => None,
            Ok(bytes) => Some(bytes),
            Err(_) => {
                tracing::warn!(
                    value = %raw,
                    env = LEGACY_FTS_TEARDOWN_BUDGET_ENV,
                    default = DEFAULT_LEGACY_FTS_TEARDOWN_BUDGET_BYTES,
                    "unparsable refusal budget; using the default"
                );
                Some(DEFAULT_LEGACY_FTS_TEARDOWN_BUDGET_BYTES)
            }
        },
        Err(_) => Some(DEFAULT_LEGACY_FTS_TEARDOWN_BUDGET_BYTES),
    }
}

pub(super) fn schema_migration_is_applied(conn: &FrankenConnection, version: i64) -> Result<bool> {
    let rows = conn
        .query_with_params(
            "SELECT 1 FROM _schema_migrations WHERE version = ?1 LIMIT 1;",
            &[SqliteValue::from(version)],
        )
        .with_context(|| format!("checking schema migration version {version}"))?;
    Ok(!rows.is_empty())
}

pub(super) fn apply_conversation_tail_state_cache_migration(
    conn: &FrankenConnection,
) -> Result<bool> {
    conn.execute("BEGIN IMMEDIATE;")
        .with_context(|| "starting v15 conversation tail-state migration transaction")?;

    let result = (|| -> Result<bool> {
        if schema_migration_is_applied(conn, 15)? {
            conn.execute("COMMIT;")
                .with_context(|| "committing already-applied v15 migration transaction")?;
            return Ok(false);
        }

        let started = Instant::now();
        let conversation_columns = franken_table_column_names(conn, "conversations")
            .with_context(|| "inspecting conversations columns before v15 migration")?;
        if !conversation_columns.contains("last_message_idx") {
            conn.execute("ALTER TABLE conversations ADD COLUMN last_message_idx INTEGER;")
                .with_context(|| "adding v15 conversations.last_message_idx column")?;
        }
        if !conversation_columns.contains("last_message_created_at") {
            conn.execute("ALTER TABLE conversations ADD COLUMN last_message_created_at INTEGER;")
                .with_context(|| "adding v15 conversations.last_message_created_at column")?;
        }
        conn.execute_batch(MIGRATION_V15_TAIL_STATE_TABLE)
            .with_context(|| "applying v15 conversation tail-state table schema")?;
        conn.execute_compat(
            "INSERT INTO _schema_migrations (version, name) VALUES (?1, ?2);",
            fparams![15_i64, "conversation_tail_state_cache"],
        )
        .with_context(|| "recording v15 conversation tail-state migration")?;
        conn.execute("COMMIT;")
            .with_context(|| "committing v15 conversation tail-state migration")?;
        info!(
            elapsed_ms = started.elapsed().as_millis(),
            "applied v15 conversation tail-state cache migration"
        );
        Ok(true)
    })();

    if result.is_err() {
        let _ = conn.execute("ROLLBACK;");
    }

    result
}

pub(super) fn franken_table_column_names(
    conn: &FrankenConnection,
    table_name: &str,
) -> Result<HashSet<String>> {
    if !table_name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(anyhow!(
            "unsafe table name for PRAGMA table_info: {table_name}"
        ));
    }

    conn.query_map_collect(
        &format!("PRAGMA table_info({table_name})"),
        fparams![],
        |row: &FrankenRow| row.get_typed::<String>(1),
    )
    .with_context(|| format!("reading PRAGMA table_info({table_name})"))
    .map(|columns| columns.into_iter().collect())
}

/// Combined V13 schema for fresh databases.
///
/// Creates the complete final schema in a single migration, avoiding the
/// incremental V5 `DROP TABLE conversations` which triggers a frankensqlite
/// autoindex limitation. All columns from V1-V13 are included in their
/// respective CREATE TABLE statements.
///
/// Table creation order respects foreign key references:
/// sources → agents/workspaces → conversations → messages → snippets, etc.
pub(super) const MIGRATION_FRESH_SCHEMA: &str = r"
-- Core tables (V1)
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS agents (
    id INTEGER PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    version TEXT,
    kind TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS workspaces (
    id INTEGER PRIMARY KEY,
    path TEXT NOT NULL UNIQUE,
    display_name TEXT
);

-- Sources (V4)
CREATE TABLE IF NOT EXISTS sources (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    host_label TEXT,
    machine_id TEXT,
    platform TEXT,
    config_json TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

INSERT OR IGNORE INTO sources (id, kind, host_label, created_at, updated_at)
VALUES ('local', 'local', NULL, strftime('%s','now')*1000, strftime('%s','now')*1000);

-- Conversations: V1 base + V5 provenance + V7 metadata_bin + V10 token summary
CREATE TABLE IF NOT EXISTS conversations (
    id INTEGER PRIMARY KEY,
    agent_id INTEGER NOT NULL REFERENCES agents(id),
    workspace_id INTEGER REFERENCES workspaces(id),
    source_id TEXT NOT NULL DEFAULT 'local' REFERENCES sources(id),
    external_id TEXT,
    title TEXT,
    source_path TEXT NOT NULL,
    started_at INTEGER,
    ended_at INTEGER,
    approx_tokens INTEGER,
    metadata_json TEXT,
    origin_host TEXT,
    metadata_bin BLOB,
    total_input_tokens INTEGER,
    total_output_tokens INTEGER,
    total_cache_read_tokens INTEGER,
    total_cache_creation_tokens INTEGER,
    grand_total_tokens INTEGER,
    estimated_cost_usd REAL,
    primary_model TEXT,
    api_call_count INTEGER,
    tool_call_count INTEGER,
    user_message_count INTEGER,
    assistant_message_count INTEGER,
    -- V15 columns are included in the fresh schema so fresh DB creation does
    -- not need ALTER TABLE on conversations. That ALTER path can duplicate
    -- provenance autoindex state in frankensqlite when the named unique
    -- provenance index already exists.
    last_message_idx INTEGER,
    last_message_created_at INTEGER
);

-- Named unique index avoids autoindex issues if table is ever recreated
CREATE UNIQUE INDEX IF NOT EXISTS idx_conversations_provenance
    ON conversations(source_id, agent_id, external_id);

-- Messages: V1 base + V7 extra_bin
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY,
    conversation_id INTEGER NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    idx INTEGER NOT NULL,
    role TEXT NOT NULL,
    author TEXT,
    created_at INTEGER,
    content TEXT NOT NULL,
    extra_json TEXT,
    extra_bin BLOB,
    UNIQUE(conversation_id, idx)
);

CREATE TABLE IF NOT EXISTS snippets (
    id INTEGER PRIMARY KEY,
    message_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    file_path TEXT,
    start_line INTEGER,
    end_line INTEGER,
    language TEXT,
    snippet_text TEXT
);

CREATE TABLE IF NOT EXISTS tags (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS conversation_tags (
    conversation_id INTEGER NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (conversation_id, tag_id)
);

-- Daily stats (V8)
CREATE TABLE IF NOT EXISTS daily_stats (
    day_id INTEGER NOT NULL,
    agent_slug TEXT NOT NULL,
    source_id TEXT NOT NULL DEFAULT 'all',
    session_count INTEGER NOT NULL DEFAULT 0,
    message_count INTEGER NOT NULL DEFAULT 0,
    total_chars INTEGER NOT NULL DEFAULT 0,
    last_updated INTEGER NOT NULL,
    PRIMARY KEY (day_id, agent_slug, source_id)
);

-- Embedding jobs (V9)
CREATE TABLE IF NOT EXISTS embedding_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    db_path TEXT NOT NULL,
    model_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    total_docs INTEGER NOT NULL DEFAULT 0,
    completed_docs INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    started_at TEXT,
    completed_at TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_embedding_jobs_active
ON embedding_jobs(db_path, model_id)
WHERE status IN ('pending', 'running');

-- Token usage ledger (V10)
CREATE TABLE IF NOT EXISTS token_usage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    conversation_id INTEGER NOT NULL,
    agent_id INTEGER NOT NULL,
    workspace_id INTEGER,
    source_id TEXT NOT NULL DEFAULT 'local',
    timestamp_ms INTEGER NOT NULL,
    day_id INTEGER NOT NULL,
    model_name TEXT,
    model_family TEXT,
    model_tier TEXT,
    service_tier TEXT,
    provider TEXT,
    input_tokens INTEGER,
    output_tokens INTEGER,
    cache_read_tokens INTEGER,
    cache_creation_tokens INTEGER,
    thinking_tokens INTEGER,
    total_tokens INTEGER,
    estimated_cost_usd REAL,
    role TEXT NOT NULL,
    content_chars INTEGER NOT NULL,
    has_tool_calls INTEGER NOT NULL DEFAULT 0,
    tool_call_count INTEGER NOT NULL DEFAULT 0,
    data_source TEXT NOT NULL DEFAULT 'api',
    UNIQUE(message_id)
);

-- Token daily stats (V10)
CREATE TABLE IF NOT EXISTS token_daily_stats (
    day_id INTEGER NOT NULL,
    agent_slug TEXT NOT NULL,
    source_id TEXT NOT NULL DEFAULT 'all',
    model_family TEXT NOT NULL DEFAULT 'all',
    api_call_count INTEGER NOT NULL DEFAULT 0,
    user_message_count INTEGER NOT NULL DEFAULT 0,
    assistant_message_count INTEGER NOT NULL DEFAULT 0,
    tool_message_count INTEGER NOT NULL DEFAULT 0,
    total_input_tokens INTEGER NOT NULL DEFAULT 0,
    total_output_tokens INTEGER NOT NULL DEFAULT 0,
    total_cache_read_tokens INTEGER NOT NULL DEFAULT 0,
    total_cache_creation_tokens INTEGER NOT NULL DEFAULT 0,
    total_thinking_tokens INTEGER NOT NULL DEFAULT 0,
    grand_total_tokens INTEGER NOT NULL DEFAULT 0,
    total_content_chars INTEGER NOT NULL DEFAULT 0,
    total_tool_calls INTEGER NOT NULL DEFAULT 0,
    estimated_cost_usd REAL NOT NULL DEFAULT 0.0,
    session_count INTEGER NOT NULL DEFAULT 0,
    last_updated INTEGER NOT NULL,
    PRIMARY KEY (day_id, agent_slug, source_id, model_family)
);

-- Model pricing (V10)
CREATE TABLE IF NOT EXISTS model_pricing (
    model_pattern TEXT NOT NULL,
    provider TEXT NOT NULL,
    input_cost_per_mtok REAL NOT NULL,
    output_cost_per_mtok REAL NOT NULL,
    cache_read_cost_per_mtok REAL,
    cache_creation_cost_per_mtok REAL,
    effective_date TEXT NOT NULL,
    PRIMARY KEY (model_pattern, effective_date)
);

INSERT OR IGNORE INTO model_pricing VALUES
    ('claude-opus-4%', 'anthropic', 15.0, 75.0, 1.5, 18.75, '2025-10-01'),
    ('claude-sonnet-4%', 'anthropic', 3.0, 15.0, 0.3, 3.75, '2025-10-01'),
    ('claude-haiku-4%', 'anthropic', 0.80, 4.0, 0.08, 1.0, '2025-10-01'),
    ('gpt-4o%', 'openai', 2.50, 10.0, NULL, NULL, '2025-01-01'),
    ('gpt-4-turbo%', 'openai', 10.0, 30.0, NULL, NULL, '2024-04-01'),
    ('gpt-4.1%', 'openai', 2.0, 8.0, NULL, NULL, '2025-04-01'),
    ('o3%', 'openai', 2.0, 8.0, NULL, NULL, '2025-04-01'),
    ('o4-mini%', 'openai', 1.10, 4.40, NULL, NULL, '2025-04-01'),
    ('gemini-2%flash%', 'google', 0.075, 0.30, NULL, NULL, '2025-01-01'),
    ('gemini-2%pro%', 'google', 1.25, 10.0, NULL, NULL, '2025-01-01');

-- Message metrics: V11 base + V12 model dimensions
CREATE TABLE IF NOT EXISTS message_metrics (
    message_id INTEGER PRIMARY KEY REFERENCES messages(id) ON DELETE CASCADE,
    created_at_ms INTEGER NOT NULL,
    hour_id INTEGER NOT NULL,
    day_id INTEGER NOT NULL,
    agent_slug TEXT NOT NULL,
    workspace_id INTEGER NOT NULL DEFAULT 0,
    source_id TEXT NOT NULL DEFAULT 'local',
    role TEXT NOT NULL,
    content_chars INTEGER NOT NULL,
    content_tokens_est INTEGER NOT NULL,
    api_input_tokens INTEGER,
    api_output_tokens INTEGER,
    api_cache_read_tokens INTEGER,
    api_cache_creation_tokens INTEGER,
    api_thinking_tokens INTEGER,
    api_service_tier TEXT,
    api_data_source TEXT NOT NULL DEFAULT 'estimated',
    tool_call_count INTEGER NOT NULL DEFAULT 0,
    has_tool_calls INTEGER NOT NULL DEFAULT 0,
    has_plan INTEGER NOT NULL DEFAULT 0,
    model_name TEXT,
    model_family TEXT NOT NULL DEFAULT 'unknown',
    model_tier TEXT NOT NULL DEFAULT 'unknown',
    provider TEXT NOT NULL DEFAULT 'unknown'
);

-- Hourly rollups: V11 base + V13 plan columns
CREATE TABLE IF NOT EXISTS usage_hourly (
    hour_id INTEGER NOT NULL,
    agent_slug TEXT NOT NULL,
    workspace_id INTEGER NOT NULL DEFAULT 0,
    source_id TEXT NOT NULL DEFAULT 'local',
    message_count INTEGER NOT NULL DEFAULT 0,
    user_message_count INTEGER NOT NULL DEFAULT 0,
    assistant_message_count INTEGER NOT NULL DEFAULT 0,
    tool_call_count INTEGER NOT NULL DEFAULT 0,
    plan_message_count INTEGER NOT NULL DEFAULT 0,
    api_coverage_message_count INTEGER NOT NULL DEFAULT 0,
    content_tokens_est_total INTEGER NOT NULL DEFAULT 0,
    content_tokens_est_user INTEGER NOT NULL DEFAULT 0,
    content_tokens_est_assistant INTEGER NOT NULL DEFAULT 0,
    api_tokens_total INTEGER NOT NULL DEFAULT 0,
    api_input_tokens_total INTEGER NOT NULL DEFAULT 0,
    api_output_tokens_total INTEGER NOT NULL DEFAULT 0,
    api_cache_read_tokens_total INTEGER NOT NULL DEFAULT 0,
    api_cache_creation_tokens_total INTEGER NOT NULL DEFAULT 0,
    api_thinking_tokens_total INTEGER NOT NULL DEFAULT 0,
    last_updated INTEGER NOT NULL DEFAULT 0,
    plan_content_tokens_est_total INTEGER NOT NULL DEFAULT 0,
    plan_api_tokens_total INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (hour_id, agent_slug, workspace_id, source_id)
);

-- Daily rollups: V11 base + V13 plan columns
CREATE TABLE IF NOT EXISTS usage_daily (
    day_id INTEGER NOT NULL,
    agent_slug TEXT NOT NULL,
    workspace_id INTEGER NOT NULL DEFAULT 0,
    source_id TEXT NOT NULL DEFAULT 'local',
    message_count INTEGER NOT NULL DEFAULT 0,
    user_message_count INTEGER NOT NULL DEFAULT 0,
    assistant_message_count INTEGER NOT NULL DEFAULT 0,
    tool_call_count INTEGER NOT NULL DEFAULT 0,
    plan_message_count INTEGER NOT NULL DEFAULT 0,
    api_coverage_message_count INTEGER NOT NULL DEFAULT 0,
    content_tokens_est_total INTEGER NOT NULL DEFAULT 0,
    content_tokens_est_user INTEGER NOT NULL DEFAULT 0,
    content_tokens_est_assistant INTEGER NOT NULL DEFAULT 0,
    api_tokens_total INTEGER NOT NULL DEFAULT 0,
    api_input_tokens_total INTEGER NOT NULL DEFAULT 0,
    api_output_tokens_total INTEGER NOT NULL DEFAULT 0,
    api_cache_read_tokens_total INTEGER NOT NULL DEFAULT 0,
    api_cache_creation_tokens_total INTEGER NOT NULL DEFAULT 0,
    api_thinking_tokens_total INTEGER NOT NULL DEFAULT 0,
    last_updated INTEGER NOT NULL DEFAULT 0,
    plan_content_tokens_est_total INTEGER NOT NULL DEFAULT 0,
    plan_api_tokens_total INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (day_id, agent_slug, workspace_id, source_id)
);

-- Model daily rollups (V12)
CREATE TABLE IF NOT EXISTS usage_models_daily (
    day_id INTEGER NOT NULL,
    agent_slug TEXT NOT NULL,
    workspace_id INTEGER NOT NULL DEFAULT 0,
    source_id TEXT NOT NULL DEFAULT 'local',
    model_family TEXT NOT NULL DEFAULT 'unknown',
    model_tier TEXT NOT NULL DEFAULT 'unknown',
    message_count INTEGER NOT NULL DEFAULT 0,
    user_message_count INTEGER NOT NULL DEFAULT 0,
    assistant_message_count INTEGER NOT NULL DEFAULT 0,
    tool_call_count INTEGER NOT NULL DEFAULT 0,
    plan_message_count INTEGER NOT NULL DEFAULT 0,
    api_coverage_message_count INTEGER NOT NULL DEFAULT 0,
    content_tokens_est_total INTEGER NOT NULL DEFAULT 0,
    content_tokens_est_user INTEGER NOT NULL DEFAULT 0,
    content_tokens_est_assistant INTEGER NOT NULL DEFAULT 0,
    api_tokens_total INTEGER NOT NULL DEFAULT 0,
    api_input_tokens_total INTEGER NOT NULL DEFAULT 0,
    api_output_tokens_total INTEGER NOT NULL DEFAULT 0,
    api_cache_read_tokens_total INTEGER NOT NULL DEFAULT 0,
    api_cache_creation_tokens_total INTEGER NOT NULL DEFAULT 0,
    api_thinking_tokens_total INTEGER NOT NULL DEFAULT 0,
    last_updated INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (day_id, agent_slug, workspace_id, source_id, model_family, model_tier)
);

-- All indexes
CREATE INDEX IF NOT EXISTS idx_conversations_agent_started ON conversations(agent_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_conversations_source_id ON conversations(source_id);
CREATE INDEX IF NOT EXISTS idx_conversations_source_path ON conversations(source_path);
CREATE INDEX IF NOT EXISTS idx_daily_stats_agent ON daily_stats(agent_slug, day_id);
CREATE INDEX IF NOT EXISTS idx_daily_stats_source ON daily_stats(source_id, day_id);
CREATE INDEX IF NOT EXISTS idx_token_usage_day ON token_usage(day_id, agent_id);
CREATE INDEX IF NOT EXISTS idx_token_usage_conv ON token_usage(conversation_id);
CREATE INDEX IF NOT EXISTS idx_token_usage_model ON token_usage(model_family, day_id);
CREATE INDEX IF NOT EXISTS idx_token_usage_workspace ON token_usage(workspace_id, day_id);
CREATE INDEX IF NOT EXISTS idx_token_usage_timestamp ON token_usage(timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_token_daily_stats_agent ON token_daily_stats(agent_slug, day_id);
CREATE INDEX IF NOT EXISTS idx_token_daily_stats_model ON token_daily_stats(model_family, day_id);
CREATE INDEX IF NOT EXISTS idx_mm_hour ON message_metrics(hour_id);
CREATE INDEX IF NOT EXISTS idx_mm_day ON message_metrics(day_id);
CREATE INDEX IF NOT EXISTS idx_mm_agent_hour ON message_metrics(agent_slug, hour_id);
CREATE INDEX IF NOT EXISTS idx_mm_agent_day ON message_metrics(agent_slug, day_id);
CREATE INDEX IF NOT EXISTS idx_mm_workspace_hour ON message_metrics(workspace_id, hour_id);
CREATE INDEX IF NOT EXISTS idx_mm_source_hour ON message_metrics(source_id, hour_id);
CREATE INDEX IF NOT EXISTS idx_mm_model_family_day ON message_metrics(model_family, day_id);
CREATE INDEX IF NOT EXISTS idx_mm_provider_day ON message_metrics(provider, day_id);
CREATE INDEX IF NOT EXISTS idx_uh_agent ON usage_hourly(agent_slug, hour_id);
CREATE INDEX IF NOT EXISTS idx_uh_workspace ON usage_hourly(workspace_id, hour_id);
CREATE INDEX IF NOT EXISTS idx_uh_source ON usage_hourly(source_id, hour_id);
CREATE INDEX IF NOT EXISTS idx_ud_agent ON usage_daily(agent_slug, day_id);
CREATE INDEX IF NOT EXISTS idx_ud_workspace ON usage_daily(workspace_id, day_id);
CREATE INDEX IF NOT EXISTS idx_ud_source ON usage_daily(source_id, day_id);
CREATE INDEX IF NOT EXISTS idx_umd_model_day ON usage_models_daily(model_family, day_id);
CREATE INDEX IF NOT EXISTS idx_umd_agent_day ON usage_models_daily(agent_slug, day_id);
CREATE INDEX IF NOT EXISTS idx_umd_workspace_day ON usage_models_daily(workspace_id, day_id);
CREATE INDEX IF NOT EXISTS idx_umd_source_day ON usage_models_daily(source_id, day_id);
";

#[derive(Clone, Copy)]
pub(super) struct SchemaRepairBatch {
    pub(super) name: &'static str,
    pub(super) tables: &'static [&'static str],
    pub(super) sql: &'static str,
}

const CURRENT_SCHEMA_REPAIR_SOURCES_SQL: &str = r"
CREATE TABLE IF NOT EXISTS sources (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    host_label TEXT,
    machine_id TEXT,
    platform TEXT,
    config_json TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

INSERT OR IGNORE INTO sources (id, kind, host_label, created_at, updated_at)
VALUES ('local', 'local', NULL, strftime('%s','now')*1000, strftime('%s','now')*1000);
";

const CURRENT_SCHEMA_REPAIR_DAILY_STATS_SQL: &str = r"
CREATE TABLE IF NOT EXISTS daily_stats (
    day_id INTEGER NOT NULL,
    agent_slug TEXT NOT NULL,
    source_id TEXT NOT NULL DEFAULT 'all',
    session_count INTEGER NOT NULL DEFAULT 0,
    message_count INTEGER NOT NULL DEFAULT 0,
    total_chars INTEGER NOT NULL DEFAULT 0,
    last_updated INTEGER NOT NULL,
    PRIMARY KEY (day_id, agent_slug, source_id)
);

CREATE INDEX IF NOT EXISTS idx_daily_stats_agent ON daily_stats(agent_slug, day_id);
CREATE INDEX IF NOT EXISTS idx_daily_stats_source ON daily_stats(source_id, day_id);
";

const CURRENT_SCHEMA_REPAIR_CONVERSATION_EXTERNAL_LOOKUP_SQL: &str = r"
CREATE TABLE IF NOT EXISTS conversation_external_lookup (
    lookup_key TEXT PRIMARY KEY,
    conversation_id INTEGER NOT NULL
);

INSERT OR REPLACE INTO conversation_external_lookup (lookup_key, conversation_id)
SELECT
    CAST(length(source_id) AS TEXT) || ':' || source_id || ':' ||
    CAST(agent_id AS TEXT) || ':' ||
    CAST(length(external_id) AS TEXT) || ':' || external_id,
    id
FROM conversations
WHERE external_id IS NOT NULL;
";

const CURRENT_SCHEMA_REPAIR_CONVERSATION_EXTERNAL_TAIL_LOOKUP_SQL: &str = r"
CREATE TABLE IF NOT EXISTS conversation_tail_state (
    conversation_id INTEGER PRIMARY KEY,
    ended_at INTEGER,
    last_message_idx INTEGER,
    last_message_created_at INTEGER
);

CREATE TABLE IF NOT EXISTS conversation_external_tail_lookup (
    lookup_key TEXT PRIMARY KEY,
    conversation_id INTEGER NOT NULL,
    ended_at INTEGER,
    last_message_idx INTEGER,
    last_message_created_at INTEGER
);

INSERT OR REPLACE INTO conversation_external_tail_lookup (
    lookup_key,
    conversation_id,
    ended_at,
    last_message_idx,
    last_message_created_at
)
SELECT
    CAST(length(c.source_id) AS TEXT) || ':' || c.source_id || ':' ||
    CAST(c.agent_id AS TEXT) || ':' ||
    CAST(length(c.external_id) AS TEXT) || ':' || c.external_id,
    c.id,
    ts.ended_at,
    ts.last_message_idx,
    ts.last_message_created_at
FROM conversations c
LEFT JOIN conversation_tail_state ts ON ts.conversation_id = c.id
WHERE c.external_id IS NOT NULL;
";

const CURRENT_SCHEMA_REPAIR_EMBEDDING_JOBS_SQL: &str = r"
CREATE TABLE IF NOT EXISTS embedding_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    db_path TEXT NOT NULL,
    model_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    total_docs INTEGER NOT NULL DEFAULT 0,
    completed_docs INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    started_at TEXT,
    completed_at TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_embedding_jobs_active
ON embedding_jobs(db_path, model_id)
WHERE status IN ('pending', 'running');
";

const CURRENT_SCHEMA_REPAIR_TOKEN_ANALYTICS_SQL: &str = r"
CREATE TABLE IF NOT EXISTS token_usage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    conversation_id INTEGER NOT NULL,
    agent_id INTEGER NOT NULL,
    workspace_id INTEGER,
    source_id TEXT NOT NULL DEFAULT 'local',
    timestamp_ms INTEGER NOT NULL,
    day_id INTEGER NOT NULL,
    model_name TEXT,
    model_family TEXT,
    model_tier TEXT,
    service_tier TEXT,
    provider TEXT,
    input_tokens INTEGER,
    output_tokens INTEGER,
    cache_read_tokens INTEGER,
    cache_creation_tokens INTEGER,
    thinking_tokens INTEGER,
    total_tokens INTEGER,
    estimated_cost_usd REAL,
    role TEXT NOT NULL,
    content_chars INTEGER NOT NULL,
    has_tool_calls INTEGER NOT NULL DEFAULT 0,
    tool_call_count INTEGER NOT NULL DEFAULT 0,
    data_source TEXT NOT NULL DEFAULT 'api',
    UNIQUE(message_id)
);

CREATE INDEX IF NOT EXISTS idx_token_usage_day ON token_usage(day_id, agent_id);
CREATE INDEX IF NOT EXISTS idx_token_usage_conv ON token_usage(conversation_id);
CREATE INDEX IF NOT EXISTS idx_token_usage_model ON token_usage(model_family, day_id);
CREATE INDEX IF NOT EXISTS idx_token_usage_workspace ON token_usage(workspace_id, day_id);
CREATE INDEX IF NOT EXISTS idx_token_usage_timestamp ON token_usage(timestamp_ms);

CREATE TABLE IF NOT EXISTS token_daily_stats (
    day_id INTEGER NOT NULL,
    agent_slug TEXT NOT NULL,
    source_id TEXT NOT NULL DEFAULT 'all',
    model_family TEXT NOT NULL DEFAULT 'all',
    api_call_count INTEGER NOT NULL DEFAULT 0,
    user_message_count INTEGER NOT NULL DEFAULT 0,
    assistant_message_count INTEGER NOT NULL DEFAULT 0,
    tool_message_count INTEGER NOT NULL DEFAULT 0,
    total_input_tokens INTEGER NOT NULL DEFAULT 0,
    total_output_tokens INTEGER NOT NULL DEFAULT 0,
    total_cache_read_tokens INTEGER NOT NULL DEFAULT 0,
    total_cache_creation_tokens INTEGER NOT NULL DEFAULT 0,
    total_thinking_tokens INTEGER NOT NULL DEFAULT 0,
    grand_total_tokens INTEGER NOT NULL DEFAULT 0,
    total_content_chars INTEGER NOT NULL DEFAULT 0,
    total_tool_calls INTEGER NOT NULL DEFAULT 0,
    estimated_cost_usd REAL NOT NULL DEFAULT 0.0,
    session_count INTEGER NOT NULL DEFAULT 0,
    last_updated INTEGER NOT NULL,
    PRIMARY KEY (day_id, agent_slug, source_id, model_family)
);

CREATE INDEX IF NOT EXISTS idx_token_daily_stats_agent ON token_daily_stats(agent_slug, day_id);
CREATE INDEX IF NOT EXISTS idx_token_daily_stats_model ON token_daily_stats(model_family, day_id);

CREATE TABLE IF NOT EXISTS model_pricing (
    model_pattern TEXT NOT NULL,
    provider TEXT NOT NULL,
    input_cost_per_mtok REAL NOT NULL,
    output_cost_per_mtok REAL NOT NULL,
    cache_read_cost_per_mtok REAL,
    cache_creation_cost_per_mtok REAL,
    effective_date TEXT NOT NULL,
    PRIMARY KEY (model_pattern, effective_date)
);

INSERT OR IGNORE INTO model_pricing VALUES
    ('claude-opus-4%', 'anthropic', 15.0, 75.0, 1.5, 18.75, '2025-10-01'),
    ('claude-sonnet-4%', 'anthropic', 3.0, 15.0, 0.3, 3.75, '2025-10-01'),
    ('claude-haiku-4%', 'anthropic', 0.80, 4.0, 0.08, 1.0, '2025-10-01'),
    ('gpt-4o%', 'openai', 2.50, 10.0, NULL, NULL, '2025-01-01'),
    ('gpt-4-turbo%', 'openai', 10.0, 30.0, NULL, NULL, '2024-04-01'),
    ('gpt-4.1%', 'openai', 2.0, 8.0, NULL, NULL, '2025-04-01'),
    ('o3%', 'openai', 2.0, 8.0, NULL, NULL, '2025-04-01'),
    ('o4-mini%', 'openai', 1.10, 4.40, NULL, NULL, '2025-04-01'),
    ('gemini-2%flash%', 'google', 0.075, 0.30, NULL, NULL, '2025-01-01'),
    ('gemini-2%pro%', 'google', 1.25, 10.0, NULL, NULL, '2025-01-01');
";

const CURRENT_SCHEMA_REPAIR_MESSAGE_METRICS_SQL: &str = r"
CREATE TABLE IF NOT EXISTS message_metrics (
    message_id INTEGER PRIMARY KEY REFERENCES messages(id) ON DELETE CASCADE,
    created_at_ms INTEGER NOT NULL,
    hour_id INTEGER NOT NULL,
    day_id INTEGER NOT NULL,
    agent_slug TEXT NOT NULL,
    workspace_id INTEGER NOT NULL DEFAULT 0,
    source_id TEXT NOT NULL DEFAULT 'local',
    role TEXT NOT NULL,
    content_chars INTEGER NOT NULL,
    content_tokens_est INTEGER NOT NULL,
    api_input_tokens INTEGER,
    api_output_tokens INTEGER,
    api_cache_read_tokens INTEGER,
    api_cache_creation_tokens INTEGER,
    api_thinking_tokens INTEGER,
    api_service_tier TEXT,
    api_data_source TEXT NOT NULL DEFAULT 'estimated',
    tool_call_count INTEGER NOT NULL DEFAULT 0,
    has_tool_calls INTEGER NOT NULL DEFAULT 0,
    has_plan INTEGER NOT NULL DEFAULT 0,
    model_name TEXT,
    model_family TEXT NOT NULL DEFAULT 'unknown',
    model_tier TEXT NOT NULL DEFAULT 'unknown',
    provider TEXT NOT NULL DEFAULT 'unknown'
);

CREATE TABLE IF NOT EXISTS usage_hourly (
    hour_id INTEGER NOT NULL,
    agent_slug TEXT NOT NULL,
    workspace_id INTEGER NOT NULL DEFAULT 0,
    source_id TEXT NOT NULL DEFAULT 'local',
    message_count INTEGER NOT NULL DEFAULT 0,
    user_message_count INTEGER NOT NULL DEFAULT 0,
    assistant_message_count INTEGER NOT NULL DEFAULT 0,
    tool_call_count INTEGER NOT NULL DEFAULT 0,
    plan_message_count INTEGER NOT NULL DEFAULT 0,
    api_coverage_message_count INTEGER NOT NULL DEFAULT 0,
    content_tokens_est_total INTEGER NOT NULL DEFAULT 0,
    content_tokens_est_user INTEGER NOT NULL DEFAULT 0,
    content_tokens_est_assistant INTEGER NOT NULL DEFAULT 0,
    api_tokens_total INTEGER NOT NULL DEFAULT 0,
    api_input_tokens_total INTEGER NOT NULL DEFAULT 0,
    api_output_tokens_total INTEGER NOT NULL DEFAULT 0,
    api_cache_read_tokens_total INTEGER NOT NULL DEFAULT 0,
    api_cache_creation_tokens_total INTEGER NOT NULL DEFAULT 0,
    api_thinking_tokens_total INTEGER NOT NULL DEFAULT 0,
    last_updated INTEGER NOT NULL DEFAULT 0,
    plan_content_tokens_est_total INTEGER NOT NULL DEFAULT 0,
    plan_api_tokens_total INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (hour_id, agent_slug, workspace_id, source_id)
);

CREATE TABLE IF NOT EXISTS usage_daily (
    day_id INTEGER NOT NULL,
    agent_slug TEXT NOT NULL,
    workspace_id INTEGER NOT NULL DEFAULT 0,
    source_id TEXT NOT NULL DEFAULT 'local',
    message_count INTEGER NOT NULL DEFAULT 0,
    user_message_count INTEGER NOT NULL DEFAULT 0,
    assistant_message_count INTEGER NOT NULL DEFAULT 0,
    tool_call_count INTEGER NOT NULL DEFAULT 0,
    plan_message_count INTEGER NOT NULL DEFAULT 0,
    api_coverage_message_count INTEGER NOT NULL DEFAULT 0,
    content_tokens_est_total INTEGER NOT NULL DEFAULT 0,
    content_tokens_est_user INTEGER NOT NULL DEFAULT 0,
    content_tokens_est_assistant INTEGER NOT NULL DEFAULT 0,
    api_tokens_total INTEGER NOT NULL DEFAULT 0,
    api_input_tokens_total INTEGER NOT NULL DEFAULT 0,
    api_output_tokens_total INTEGER NOT NULL DEFAULT 0,
    api_cache_read_tokens_total INTEGER NOT NULL DEFAULT 0,
    api_cache_creation_tokens_total INTEGER NOT NULL DEFAULT 0,
    api_thinking_tokens_total INTEGER NOT NULL DEFAULT 0,
    last_updated INTEGER NOT NULL DEFAULT 0,
    plan_content_tokens_est_total INTEGER NOT NULL DEFAULT 0,
    plan_api_tokens_total INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (day_id, agent_slug, workspace_id, source_id)
);

CREATE TABLE IF NOT EXISTS usage_models_daily (
    day_id INTEGER NOT NULL,
    agent_slug TEXT NOT NULL,
    workspace_id INTEGER NOT NULL DEFAULT 0,
    source_id TEXT NOT NULL DEFAULT 'local',
    model_family TEXT NOT NULL DEFAULT 'unknown',
    model_tier TEXT NOT NULL DEFAULT 'unknown',
    message_count INTEGER NOT NULL DEFAULT 0,
    user_message_count INTEGER NOT NULL DEFAULT 0,
    assistant_message_count INTEGER NOT NULL DEFAULT 0,
    tool_call_count INTEGER NOT NULL DEFAULT 0,
    plan_message_count INTEGER NOT NULL DEFAULT 0,
    api_coverage_message_count INTEGER NOT NULL DEFAULT 0,
    content_tokens_est_total INTEGER NOT NULL DEFAULT 0,
    content_tokens_est_user INTEGER NOT NULL DEFAULT 0,
    content_tokens_est_assistant INTEGER NOT NULL DEFAULT 0,
    api_tokens_total INTEGER NOT NULL DEFAULT 0,
    api_input_tokens_total INTEGER NOT NULL DEFAULT 0,
    api_output_tokens_total INTEGER NOT NULL DEFAULT 0,
    api_cache_read_tokens_total INTEGER NOT NULL DEFAULT 0,
    api_cache_creation_tokens_total INTEGER NOT NULL DEFAULT 0,
    api_thinking_tokens_total INTEGER NOT NULL DEFAULT 0,
    last_updated INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (day_id, agent_slug, workspace_id, source_id, model_family, model_tier)
);

CREATE INDEX IF NOT EXISTS idx_mm_hour ON message_metrics(hour_id);
CREATE INDEX IF NOT EXISTS idx_mm_day ON message_metrics(day_id);
CREATE INDEX IF NOT EXISTS idx_mm_agent_hour ON message_metrics(agent_slug, hour_id);
CREATE INDEX IF NOT EXISTS idx_mm_agent_day ON message_metrics(agent_slug, day_id);
CREATE INDEX IF NOT EXISTS idx_mm_workspace_hour ON message_metrics(workspace_id, hour_id);
CREATE INDEX IF NOT EXISTS idx_mm_source_hour ON message_metrics(source_id, hour_id);
CREATE INDEX IF NOT EXISTS idx_mm_model_family_day ON message_metrics(model_family, day_id);
CREATE INDEX IF NOT EXISTS idx_mm_provider_day ON message_metrics(provider, day_id);
CREATE INDEX IF NOT EXISTS idx_uh_agent ON usage_hourly(agent_slug, hour_id);
CREATE INDEX IF NOT EXISTS idx_uh_workspace ON usage_hourly(workspace_id, hour_id);
CREATE INDEX IF NOT EXISTS idx_uh_source ON usage_hourly(source_id, hour_id);
CREATE INDEX IF NOT EXISTS idx_ud_agent ON usage_daily(agent_slug, day_id);
CREATE INDEX IF NOT EXISTS idx_ud_workspace ON usage_daily(workspace_id, day_id);
CREATE INDEX IF NOT EXISTS idx_ud_source ON usage_daily(source_id, day_id);
CREATE INDEX IF NOT EXISTS idx_umd_model_day ON usage_models_daily(model_family, day_id);
CREATE INDEX IF NOT EXISTS idx_umd_agent_day ON usage_models_daily(agent_slug, day_id);
CREATE INDEX IF NOT EXISTS idx_umd_workspace_day ON usage_models_daily(workspace_id, day_id);
CREATE INDEX IF NOT EXISTS idx_umd_source_day ON usage_models_daily(source_id, day_id);
";

pub(super) const CURRENT_SCHEMA_REPAIR_BATCHES: &[SchemaRepairBatch] = &[
    SchemaRepairBatch {
        name: "sources",
        tables: &["sources"],
        sql: CURRENT_SCHEMA_REPAIR_SOURCES_SQL,
    },
    SchemaRepairBatch {
        name: "daily_stats",
        tables: &["daily_stats"],
        sql: CURRENT_SCHEMA_REPAIR_DAILY_STATS_SQL,
    },
    SchemaRepairBatch {
        name: "conversation_external_lookup",
        tables: &["conversation_external_lookup"],
        sql: CURRENT_SCHEMA_REPAIR_CONVERSATION_EXTERNAL_LOOKUP_SQL,
    },
    SchemaRepairBatch {
        name: "conversation_external_tail_lookup",
        tables: &[
            "conversation_tail_state",
            "conversation_external_tail_lookup",
        ],
        sql: CURRENT_SCHEMA_REPAIR_CONVERSATION_EXTERNAL_TAIL_LOOKUP_SQL,
    },
    SchemaRepairBatch {
        name: "embedding_jobs",
        tables: &["embedding_jobs"],
        sql: CURRENT_SCHEMA_REPAIR_EMBEDDING_JOBS_SQL,
    },
    SchemaRepairBatch {
        name: "token_analytics",
        tables: &["token_usage", "token_daily_stats", "model_pricing"],
        sql: CURRENT_SCHEMA_REPAIR_TOKEN_ANALYTICS_SQL,
    },
    SchemaRepairBatch {
        name: "message_rollups",
        tables: &[
            "message_metrics",
            "usage_hourly",
            "usage_daily",
            "usage_models_daily",
        ],
        sql: CURRENT_SCHEMA_REPAIR_MESSAGE_METRICS_SQL,
    },
    SchemaRepairBatch {
        name: "forgotten_sources",
        tables: &["forgotten_sources"],
        sql: MIGRATION_V22,
    },
    SchemaRepairBatch {
        name: "forgotten_conversations",
        tables: &["forgotten_conversations"],
        sql: MIGRATION_V23,
    },
];

pub(super) fn current_schema_repair_batches_for_missing_tables(
    missing_tables: &[&'static str],
) -> Result<Vec<&'static SchemaRepairBatch>> {
    let missing_set: HashSet<&'static str> = missing_tables.iter().copied().collect();
    let mut selected_batches = Vec::new();
    let mut covered_tables = HashSet::new();

    for batch in CURRENT_SCHEMA_REPAIR_BATCHES {
        if !batch
            .tables
            .iter()
            .any(|table_name| missing_set.contains(table_name))
        {
            continue;
        }
        selected_batches.push(batch);
        covered_tables.extend(batch.tables.iter().copied());
    }

    for &table_name in missing_tables {
        if !covered_tables.contains(table_name) {
            return Err(anyhow!(
                "no current-schema repair batch registered for missing table {table_name}"
            ));
        }
    }

    Ok(selected_batches)
}

/// Migration name lookup for backfilling `_schema_migrations` during transition.
const MIGRATION_NAMES: [(i64, &str); 23] = [
    (1, "core_tables"),
    (2, "fts_messages"),
    (3, "fts_messages_rebuild"),
    (4, "sources"),
    (5, "provenance_columns"),
    (6, "source_path_index"),
    (7, "msgpack_columns"),
    (8, "daily_stats"),
    (9, "embedding_jobs"),
    (10, "token_analytics"),
    (11, "message_metrics"),
    (12, "model_dimensions"),
    (13, "plan_token_rollups"),
    (14, "fts_contentless"),
    (15, "conversation_tail_state_cache"),
    (16, "drop_redundant_message_conv_idx"),
    (17, "drop_message_created_idx"),
    (18, "conversation_tail_state_hot_table"),
    (19, "conversation_external_lookup"),
    (20, "conversation_external_tail_lookup"),
    (21, "conversation_context_index"),
    (22, "forgotten_sources"),
    (23, "forgotten_conversations"),
];

/// Transitions an existing database from `meta` table schema versioning to the
/// `_schema_migrations` table used by `MigrationRunner`.
///
/// The existing `SqliteStorage` tracks schema version as a string value in
/// `meta WHERE key = 'schema_version'`. The bead spec references
/// `PRAGMA user_version`, but the actual cass code uses the `meta` table.
/// This function handles the real code path.
///
/// Behavior:
/// - If `_schema_migrations` already exists → skip (already transitioned)
/// - If `meta` table has `schema_version > 0` → create `_schema_migrations`
///   and backfill entries for versions `1..=current_version`
/// - Legacy V10-V12 databases are represented as V13 in `_schema_migrations`
///   because frankensqlite uses one combined V13 base migration instead of
///   replaying the old incremental V11-V13 steps.
/// - If `meta` table missing or `schema_version = 0` with no tables → fresh DB,
///   let `MigrationRunner` handle it
/// - If `schema_version = 0` but tables exist → corrupted state, log warning
pub(super) fn transition_from_meta_version(conn: &FrankenConnection) -> Result<()> {
    // Avoid sqlite_master enumeration here. Databases with FTS virtual tables
    // can trigger frankensqlite parse-recovery on sqlite_master reads, which is
    // enough to break the transition on otherwise-healthy legacy cass DBs.
    if conn
        .query("SELECT version FROM \"_schema_migrations\";")
        .is_ok()
    {
        return Ok(());
    }

    // Check if the meta table exists.
    if conn.query("SELECT key FROM meta;").is_err() {
        // No meta table → fresh database, let MigrationRunner handle it.
        return Ok(());
    }

    // Read the current schema version from the meta table.
    let rows = conn
        .query("SELECT value FROM meta WHERE key = 'schema_version';")
        .with_context(|| "reading schema_version from meta")?;

    let current_version: i64 = rows
        .first()
        .and_then(|row| row.get_typed::<String>(0).ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    if current_version == 0 {
        // Check if tables actually exist (corrupted state: tables present but version=0).
        if conn.query("SELECT id FROM conversations LIMIT 1;").is_err() {
            // Truly fresh DB (meta table exists but empty/reset). Let MigrationRunner handle it.
            return Ok(());
        }

        // Tables exist but version=0: corrupted state. Log and skip transition;
        // MigrationRunner will fail on "table already exists" and surface the error.
        info!("meta.schema_version=0 but tables exist; skipping transition (corrupted state)");
        return Ok(());
    }

    // Create _schema_migrations and backfill entries for all applied versions.
    info!(
        current_version,
        "transitioning schema tracking from meta table to _schema_migrations"
    );

    conn.execute(
        "CREATE TABLE IF NOT EXISTS _schema_migrations (\
            version INTEGER PRIMARY KEY, \
            name TEXT NOT NULL, \
            applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))\
        );",
    )
    .with_context(|| "creating _schema_migrations table for transition")?;

    let backfill_through_version = if (10..13).contains(&current_version) {
        13
    } else {
        current_version
    };

    for &(version, name) in &MIGRATION_NAMES {
        if version > backfill_through_version {
            break;
        }
        conn.execute_compat(
            "INSERT INTO _schema_migrations (version, name) VALUES (?1, ?2);",
            &[ParamValue::from(version), ParamValue::from(name)],
        )
        .with_context(|| format!("backfilling _schema_migrations version {version}"))?;
    }

    info!(
        current_version,
        backfill_through_version,
        "schema version transition complete: backfilled legacy meta schema versions"
    );

    Ok(())
}

pub(super) const REQUIRED_CURRENT_SCHEMA_TABLE_PROBES: &[(&str, &str)] = &[
    ("sources", "SELECT id FROM sources LIMIT 1;"),
    ("daily_stats", "SELECT day_id FROM daily_stats LIMIT 1;"),
    (
        "conversation_external_lookup",
        "SELECT lookup_key FROM conversation_external_lookup LIMIT 1;",
    ),
    (
        "conversation_tail_state",
        "SELECT conversation_id FROM conversation_tail_state LIMIT 1;",
    ),
    (
        "conversation_external_tail_lookup",
        "SELECT lookup_key, last_message_idx FROM conversation_external_tail_lookup LIMIT 1;",
    ),
    ("embedding_jobs", "SELECT id FROM embedding_jobs LIMIT 1;"),
    ("token_usage", "SELECT id FROM token_usage LIMIT 1;"),
    (
        "token_daily_stats",
        "SELECT day_id FROM token_daily_stats LIMIT 1;",
    ),
    (
        "model_pricing",
        "SELECT model_pattern FROM model_pricing LIMIT 1;",
    ),
    (
        "message_metrics",
        "SELECT message_id FROM message_metrics LIMIT 1;",
    ),
    ("usage_hourly", "SELECT hour_id FROM usage_hourly LIMIT 1;"),
    ("usage_daily", "SELECT day_id FROM usage_daily LIMIT 1;"),
    (
        "usage_models_daily",
        "SELECT day_id FROM usage_models_daily LIMIT 1;",
    ),
    (
        "forgotten_sources",
        "SELECT source_path FROM forgotten_sources LIMIT 1;",
    ),
    (
        "forgotten_conversations",
        "SELECT source_id, agent_slug, source_path, external_id FROM forgotten_conversations LIMIT 1;",
    ),
];

pub(super) const REQUIRED_CONVERSATION_TOKEN_COLUMNS: &[(&str, &str)] = &[
    ("total_input_tokens", "INTEGER"),
    ("total_output_tokens", "INTEGER"),
    ("total_cache_read_tokens", "INTEGER"),
    ("total_cache_creation_tokens", "INTEGER"),
    ("grand_total_tokens", "INTEGER"),
    ("estimated_cost_usd", "REAL"),
    ("primary_model", "TEXT"),
    ("api_call_count", "INTEGER"),
    ("tool_call_count", "INTEGER"),
    ("user_message_count", "INTEGER"),
    ("assistant_message_count", "INTEGER"),
];

pub(super) fn error_indicates_missing_table(err: &impl std::fmt::Display) -> bool {
    err.to_string()
        .to_ascii_lowercase()
        .contains("no such table")
}

pub(super) fn error_indicates_missing_column(err: &impl std::fmt::Display) -> bool {
    err.to_string()
        .to_ascii_lowercase()
        .contains("no such column")
}

pub(super) const ORPHAN_FK_ID_CHUNK_SIZE: usize = 256;

pub(super) fn collect_orphan_message_ids(conn: &FrankenConnection) -> Result<Vec<i64>> {
    conn.query_map_collect(
        "SELECT m.id
         FROM messages AS m
         WHERE NOT EXISTS (
             SELECT 1
             FROM conversations AS c
             WHERE c.id = m.conversation_id
         )",
        fparams![],
        |row| row.get_typed(0),
    )
    .context("listing orphan message ids for orphan FK cleanup")
}

pub(super) fn delete_rows_by_i64_chunks(
    tx: &FrankenTransaction<'_>,
    delete_many_sql_prefix: &'static str,
    ids: &[i64],
) -> Result<usize> {
    if ids.is_empty() {
        return Ok(0);
    }

    let full_chunk_sql = delete_rows_by_i64_sql(delete_many_sql_prefix, ORPHAN_FK_ID_CHUNK_SIZE);
    let tail_len = ids.len() % ORPHAN_FK_ID_CHUNK_SIZE;
    let tail_sql =
        (tail_len != 0).then(|| delete_rows_by_i64_sql(delete_many_sql_prefix, tail_len));

    let mut deleted = 0;
    for chunk in ids.chunks(ORPHAN_FK_ID_CHUNK_SIZE) {
        let sql = if chunk.len() == ORPHAN_FK_ID_CHUNK_SIZE {
            &full_chunk_sql
        } else {
            tail_sql.as_ref().unwrap_or(&full_chunk_sql)
        };
        let params = chunk
            .iter()
            .map(|id| SqliteValue::from(*id))
            .collect::<Vec<_>>();
        deleted += tx.execute_with_params(sql, &params)?;
    }
    Ok(deleted)
}

fn delete_rows_by_i64_sql(delete_many_sql_prefix: &'static str, count: usize) -> String {
    let placeholders = sql_placeholders(count);
    format!("{delete_many_sql_prefix} ({placeholders})")
}

pub(super) fn sql_placeholders(count: usize) -> String {
    vec!["?"; count].join(", ")
}

pub(super) fn delete_orphan_message_ids_bisecting_oom(
    conn: &FrankenConnection,
    ids: &[i64],
) -> Result<usize> {
    let mut deleted = 0usize;
    for chunk in ids.chunks(ORPHAN_FK_ID_CHUNK_SIZE) {
        deleted = deleted.saturating_add(delete_orphan_message_id_chunk(conn, chunk)?);
    }
    Ok(deleted)
}

fn delete_orphan_message_id_chunk(conn: &FrankenConnection, ids: &[i64]) -> Result<usize> {
    if ids.is_empty() {
        return Ok(0);
    }

    match delete_orphan_message_id_chunk_once(conn, ids) {
        Ok(deleted) => Ok(deleted),
        Err(err) if is_out_of_memory_error(&err) && ids.len() > 1 => {
            let split_at = ids.len() / 2;
            tracing::warn!(
                target: "cass::fk_repair",
                rows = ids.len(),
                left = split_at,
                right = ids.len().saturating_sub(split_at),
                error = %err,
                "orphan-message cleanup ran out of memory; retrying as smaller batches"
            );
            let left = delete_orphan_message_id_chunk(conn, &ids[..split_at])?;
            let right = delete_orphan_message_id_chunk(conn, &ids[split_at..])?;
            Ok(left.saturating_add(right))
        }
        Err(err) => Err(err),
    }
}

fn delete_orphan_message_id_chunk_once(conn: &FrankenConnection, ids: &[i64]) -> Result<usize> {
    let mut tx = conn.transaction()?;
    let mut deleted = 0usize;
    for entry in ORPHAN_MESSAGE_DEPENDENT_TABLES {
        match delete_rows_by_i64_chunks(&tx, entry.delete_many_sql_prefix, ids) {
            Ok(count) => {
                deleted = deleted.saturating_add(count);
            }
            Err(err) if error_indicates_missing_table(&err) => {
                tracing::debug!(
                    target: "cass::fk_repair",
                    child_table = entry.child_table,
                    error = %err,
                    "skipping orphan-message dependent cleanup (table unavailable)"
                );
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!(
                        "deleting rows from {} that depend on orphan messages",
                        entry.child_table
                    )
                });
            }
        }
    }
    deleted = deleted.saturating_add(
        delete_rows_by_i64_chunks(&tx, "DELETE FROM messages WHERE id IN", ids)
            .context("deleting orphan rows from messages")?,
    );
    tx.commit()?;
    Ok(deleted)
}

pub(super) fn collect_direct_orphan_id_page(
    conn: &FrankenConnection,
    entry: &'static OrphanFkTable,
) -> Result<Vec<i64>> {
    Ok(conn.query_map_collect(
        entry.orphan_id_page_sql,
        fparams![i64::try_from(ORPHAN_FK_ID_CHUNK_SIZE).unwrap_or(i64::MAX)],
        |row| row.get_typed(0),
    )?)
}

pub(super) fn delete_direct_orphan_ids_bisecting_oom(
    conn: &FrankenConnection,
    entry: &'static OrphanFkTable,
    ids: &[i64],
) -> Result<usize> {
    let mut deleted = 0usize;
    for chunk in ids.chunks(ORPHAN_FK_ID_CHUNK_SIZE) {
        deleted = deleted.saturating_add(delete_direct_orphan_id_chunk(conn, entry, chunk)?);
    }
    Ok(deleted)
}

fn delete_direct_orphan_id_chunk(
    conn: &FrankenConnection,
    entry: &'static OrphanFkTable,
    ids: &[i64],
) -> Result<usize> {
    if ids.is_empty() {
        return Ok(0);
    }

    match delete_direct_orphan_id_chunk_once(conn, entry, ids) {
        Ok(deleted) => Ok(deleted),
        Err(err) if is_out_of_memory_error(&err) && ids.len() > 1 => {
            let split_at = ids.len() / 2;
            tracing::warn!(
                target: "cass::fk_repair",
                child_table = entry.child_table,
                rows = ids.len(),
                left = split_at,
                right = ids.len().saturating_sub(split_at),
                error = %err,
                "direct orphan cleanup ran out of memory; retrying as smaller batches"
            );
            let left = delete_direct_orphan_id_chunk(conn, entry, &ids[..split_at])?;
            let right = delete_direct_orphan_id_chunk(conn, entry, &ids[split_at..])?;
            Ok(left.saturating_add(right))
        }
        Err(err) => Err(err),
    }
}

fn delete_direct_orphan_id_chunk_once(
    conn: &FrankenConnection,
    entry: &'static OrphanFkTable,
    ids: &[i64],
) -> Result<usize> {
    let mut tx = conn.transaction()?;
    let deleted = delete_rows_by_i64_chunks(&tx, entry.delete_many_sql_prefix, ids)?;
    tx.commit()?;
    Ok(deleted)
}

/// Tables whose FK parent rows can go missing when an index transaction is
/// dropped mid-flight. The select and delete SQL strings are intentionally
/// static (no dynamic table names) so they can be audited at a glance and so
/// they cannot be subverted by injected identifiers. The select statement
/// yields the integer FK key used by the matching chunked delete.
pub(super) struct OrphanFkTable {
    pub(super) child_table: &'static str,
    pub(super) orphan_id_page_sql: &'static str,
    pub(super) delete_many_sql_prefix: &'static str,
}

pub(super) const ORPHAN_DIRECT_CHILD_TABLES: &[OrphanFkTable] = &[
    OrphanFkTable {
        child_table: "message_metrics",
        orphan_id_page_sql: "SELECT message_id FROM message_metrics \
                             WHERE NOT EXISTS (\
                                 SELECT 1 FROM messages \
                                 WHERE messages.id = message_metrics.message_id\
                             ) \
                             ORDER BY message_id \
                             LIMIT ?1",
        delete_many_sql_prefix: "DELETE FROM message_metrics WHERE message_id IN",
    },
    OrphanFkTable {
        child_table: "token_usage",
        orphan_id_page_sql: "SELECT message_id FROM token_usage \
                             WHERE NOT EXISTS (\
                                 SELECT 1 FROM messages \
                                 WHERE messages.id = token_usage.message_id\
                             ) \
                             ORDER BY message_id \
                             LIMIT ?1",
        delete_many_sql_prefix: "DELETE FROM token_usage WHERE message_id IN",
    },
    OrphanFkTable {
        child_table: "snippets",
        orphan_id_page_sql: "SELECT message_id FROM snippets \
                             WHERE NOT EXISTS (\
                                 SELECT 1 FROM messages \
                                 WHERE messages.id = snippets.message_id\
                             ) \
                             ORDER BY message_id \
                             LIMIT ?1",
        delete_many_sql_prefix: "DELETE FROM snippets WHERE message_id IN",
    },
    OrphanFkTable {
        child_table: "conversation_tags",
        orphan_id_page_sql: "SELECT conversation_id FROM conversation_tags \
                             WHERE NOT EXISTS (\
                                 SELECT 1 FROM conversations \
                                 WHERE conversations.id = conversation_tags.conversation_id\
                             ) \
                             ORDER BY conversation_id \
                             LIMIT ?1",
        delete_many_sql_prefix: "DELETE FROM conversation_tags WHERE conversation_id IN",
    },
];

pub(super) struct OrphanMessageDependentTable {
    pub(super) child_table: &'static str,
    pub(super) delete_many_sql_prefix: &'static str,
}

const ORPHAN_MESSAGE_DEPENDENT_TABLES: &[OrphanMessageDependentTable] = &[
    OrphanMessageDependentTable {
        child_table: "message_metrics",
        delete_many_sql_prefix: "DELETE FROM message_metrics WHERE message_id IN",
    },
    OrphanMessageDependentTable {
        child_table: "token_usage",
        delete_many_sql_prefix: "DELETE FROM token_usage WHERE message_id IN",
    },
    OrphanMessageDependentTable {
        child_table: "snippets",
        delete_many_sql_prefix: "DELETE FROM snippets WHERE message_id IN",
    },
];

/// Summary of orphan rows detected and removed by `cleanup_orphan_fk_rows`.
///
/// Message-root counts come from the probe phase, while direct child counts
/// come from bounded page deletes. Under the function's intended use — a single
/// indexer-startup pass holding the index run lock — no concurrent writers
/// exist, so these counts match the primary orphan roots identified and
/// removed during cleanup. Dependent rows below an orphan message
/// (`message_metrics` / `token_usage` / `snippets`) are an expected consequence
/// of removing that root orphan and are *not* separately counted in `total` or
/// `per_table`.
#[derive(Debug, Default, Clone)]
pub(crate) struct OrphanFkCleanupReport {
    pub total: i64,
    pub per_table: Vec<(&'static str, i64)>,
}

impl OrphanFkCleanupReport {
    pub(super) fn record(&mut self, child_table: &'static str, count: i64) {
        if let Some((_, existing)) = self
            .per_table
            .iter_mut()
            .find(|(table, _)| *table == child_table)
        {
            *existing = existing.saturating_add(count);
        } else {
            self.per_table.push((child_table, count));
        }
        self.total = self.total.saturating_add(count);
    }
}
