//! Canonical baseline schema for the bundled-rusqlite `agent_search.db`
//! archive (Task C3).
//!
//! [`BASELINE_SCHEMA_V1_SQL`] is the complete schema a fresh archive is
//! initialized with inside a single `BEGIN IMMEDIATE` transaction (see
//! `crate::storage::sqlite::open`). It consolidates the former combined v13
//! base schema plus every additive post-v13 migration (v14 FTS teardown,
//! v15/v18 conversation tail state, v19/v20 external lookups, v21 context
//! index, v22 forgotten sources, v23 forgotten conversations) into one
//! idempotent baseline: every statement is `IF NOT EXISTS`/`INSERT OR IGNORE`,
//! so re-application on an already-initialized archive is a no-op.
//!
//! Backfill `INSERT .. SELECT` statements from the old v18-v20 migrations are
//! deliberately absent: a fresh archive has no pre-existing `conversations`
//! rows, so the lookup tables simply start empty and are maintained by the
//! same writer paths that populate the canonical rows.

/// `PRAGMA application_id` stamp for a canonical cass index archive.
///
/// Reads as the ASCII tag `CaSS` (`0x43 0x41 0x53 0x53`) in the big-endian
/// 32-bit header field at offset 68, so `file`/`xxd` and every archive
/// inspection agree on the identity without a lookup table.
pub const CASS_APPLICATION_ID: i64 = 0x4341_5353;

/// `PRAGMA user_version` value stamped on a fully initialized schema-v1
/// archive (header field at offset 60).
pub const SCHEMA_V1_USER_VERSION: i64 = 1;

/// Complete canonical schema for a schema-v1 archive.
///
/// Object inventory (tables, unique constraints, indexes, triggers):
///
/// * `meta`, `agents`, `workspaces`, `sources` (+ the `local` source row)
/// * `conversations` — including the v15 tail columns
///   (`last_message_idx`, `last_message_created_at`) so fresh creation needs
///   no `ALTER TABLE`
/// * `messages` — `UNIQUE(conversation_id, idx)` produces
///   `sqlite_autoindex_messages_1`, which a large body of existing SQL pins
///   with `INDEXED BY`; the explicit named
///   `idx_messages_conv_idx` unique index is kept alongside it
/// * `snippets` (+ `idx_snippets_message_id`), `tags`, `conversation_tags`
/// * `daily_stats`, `embedding_jobs` (+ active-job partial unique index)
/// * `token_usage`, `token_daily_stats`, `model_pricing` (+ reference rows)
/// * `message_metrics`, `usage_hourly`, `usage_daily`, `usage_models_daily`
/// * `conversation_tail_state` (v15/v18), `conversation_external_lookup`
///   (v19), `conversation_external_tail_lookup` (v20)
/// * `forgotten_sources` (v22), `forgotten_conversations` (v23)
/// * `viewer_search_regions` + `viewer_search_region_marks` with their region
///   indexes (additive viewer extension profile)
/// * `fts_messages` — the canonical three-column contentless FTS5 shadow
///   (`content`, `title`, `porter unicode61`), with the `AFTER DELETE ON
///   messages` trigger issuing the FTS5 `delete` command
/// * `meta.schema_version = '1'` as the final statement
pub const BASELINE_SCHEMA_V1_SQL: &str = r"
-- Core bookkeeping
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

-- Conversations: base columns + provenance + metadata_bin + token summary +
-- v15 tail columns (folded in so fresh creation needs no ALTER TABLE).
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
    last_message_idx INTEGER,
    last_message_created_at INTEGER
);

-- Named unique index avoids autoindex issues if the table is ever recreated.
CREATE UNIQUE INDEX IF NOT EXISTS idx_conversations_provenance
    ON conversations(source_id, agent_id, external_id);

CREATE INDEX IF NOT EXISTS idx_conversations_agent_started
    ON conversations(agent_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_conversations_source_id ON conversations(source_id);
CREATE INDEX IF NOT EXISTS idx_conversations_source_path ON conversations(source_path);

-- v21: context reads this covering index before hydrating limited result
-- sets; wide metadata records stay out of candidate selection (GH #463).
CREATE INDEX IF NOT EXISTS idx_conversations_context
    ON conversations(started_at DESC, workspace_id, agent_id);

-- Messages: base columns + extra_bin.
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

-- The table-level UNIQUE already produces sqlite_autoindex_messages_1, which
-- existing query plans pin with INDEXED BY; this named unique index is kept
-- per the schema-v1 contract so the object also exists under a stable name.
CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_conv_idx
    ON messages(conversation_id, idx);

CREATE TABLE IF NOT EXISTS snippets (
    id INTEGER PRIMARY KEY,
    message_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    file_path TEXT,
    start_line INTEGER,
    end_line INTEGER,
    language TEXT,
    snippet_text TEXT
);

CREATE INDEX IF NOT EXISTS idx_snippets_message_id ON snippets(message_id);

CREATE TABLE IF NOT EXISTS tags (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS conversation_tags (
    conversation_id INTEGER NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (conversation_id, tag_id)
);

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

CREATE INDEX IF NOT EXISTS idx_mm_hour ON message_metrics(hour_id);
CREATE INDEX IF NOT EXISTS idx_mm_day ON message_metrics(day_id);
CREATE INDEX IF NOT EXISTS idx_mm_agent_hour ON message_metrics(agent_slug, hour_id);
CREATE INDEX IF NOT EXISTS idx_mm_agent_day ON message_metrics(agent_slug, day_id);
CREATE INDEX IF NOT EXISTS idx_mm_workspace_hour ON message_metrics(workspace_id, hour_id);
CREATE INDEX IF NOT EXISTS idx_mm_source_hour ON message_metrics(source_id, hour_id);
CREATE INDEX IF NOT EXISTS idx_mm_model_family_day ON message_metrics(model_family, day_id);
CREATE INDEX IF NOT EXISTS idx_mm_provider_day ON message_metrics(provider, day_id);

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

CREATE INDEX IF NOT EXISTS idx_uh_agent ON usage_hourly(agent_slug, hour_id);
CREATE INDEX IF NOT EXISTS idx_uh_workspace ON usage_hourly(workspace_id, hour_id);
CREATE INDEX IF NOT EXISTS idx_uh_source ON usage_hourly(source_id, hour_id);

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

CREATE INDEX IF NOT EXISTS idx_ud_agent ON usage_daily(agent_slug, day_id);
CREATE INDEX IF NOT EXISTS idx_ud_workspace ON usage_daily(workspace_id, day_id);
CREATE INDEX IF NOT EXISTS idx_ud_source ON usage_daily(source_id, day_id);

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

CREATE INDEX IF NOT EXISTS idx_umd_model_day ON usage_models_daily(model_family, day_id);
CREATE INDEX IF NOT EXISTS idx_umd_agent_day ON usage_models_daily(agent_slug, day_id);
CREATE INDEX IF NOT EXISTS idx_umd_workspace_day ON usage_models_daily(workspace_id, day_id);
CREATE INDEX IF NOT EXISTS idx_umd_source_day ON usage_models_daily(source_id, day_id);

-- v15/v18: append-tail state lives outside the wide, indexed conversations
-- row so the hot append path updates a tiny rowid table instead.
-- Deliberately no FOREIGN KEY (kept off the direct rowid update path).
CREATE TABLE IF NOT EXISTS conversation_tail_state (
    conversation_id INTEGER PRIMARY KEY,
    ended_at INTEGER,
    last_message_idx INTEGER,
    last_message_created_at INTEGER
);

-- v19: materialized external conversation provenance as one lookup key.
CREATE TABLE IF NOT EXISTS conversation_external_lookup (
    lookup_key TEXT PRIMARY KEY,
    conversation_id INTEGER NOT NULL
);

-- v20: external lookup fused with append-tail state.
CREATE TABLE IF NOT EXISTS conversation_external_tail_lookup (
    lookup_key TEXT PRIMARY KEY,
    conversation_id INTEGER NOT NULL,
    ended_at INTEGER,
    last_message_idx INTEGER,
    last_message_created_at INTEGER
);

-- v22: `cass forget --apply` source tombstones.
CREATE TABLE IF NOT EXISTS forgotten_sources (
    source_path TEXT PRIMARY KEY,
    size_bytes INTEGER,
    mtime_ms INTEGER,
    forgotten_at_ms INTEGER NOT NULL
);

-- v23: forgotten session identities independent of the store's file stamp.
CREATE TABLE IF NOT EXISTS forgotten_conversations (
    source_id TEXT NOT NULL,
    agent_slug TEXT NOT NULL,
    source_path TEXT NOT NULL,
    external_id TEXT NOT NULL,
    forgotten_at_ms INTEGER NOT NULL,
    PRIMARY KEY (source_id, agent_slug, source_path, external_id)
);

-- Additive viewer extension profile: persisted search-region inventory and
-- the per-session publication marks (viewer_extensions/schema.rs v7 shape,
-- folded into the baseline so the canonical archive is complete at birth).
CREATE TABLE IF NOT EXISTS viewer_search_regions (
    conversation_id INTEGER NOT NULL
        REFERENCES conversations(id) ON DELETE CASCADE,
    region_ordinal INTEGER NOT NULL,
    region_key TEXT NOT NULL,
    scope TEXT NOT NULL,
    scope_known INTEGER NOT NULL DEFAULT 1,
    quill_idx INTEGER NOT NULL,
    message_idx INTEGER,
    message_id INTEGER,
    step_index INTEGER,
    turn_index INTEGER,
    tool_name TEXT,
    file_path TEXT,
    exit_code INTEGER,
    has_error INTEGER,
    error_type TEXT,
    text_source TEXT NOT NULL DEFAULT 'derived',
    indexed_text TEXT,
    source_revision INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (conversation_id, region_ordinal)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_viewer_search_regions_quill
    ON viewer_search_regions(conversation_id, quill_idx);
CREATE UNIQUE INDEX IF NOT EXISTS idx_viewer_search_regions_key
    ON viewer_search_regions(conversation_id, region_key);
CREATE INDEX IF NOT EXISTS idx_viewer_search_regions_scope
    ON viewer_search_regions(conversation_id, scope);
CREATE INDEX IF NOT EXISTS idx_viewer_search_regions_tool
    ON viewer_search_regions(tool_name);
CREATE INDEX IF NOT EXISTS idx_viewer_search_regions_file
    ON viewer_search_regions(file_path);

CREATE TABLE IF NOT EXISTS viewer_search_region_marks (
    conversation_id INTEGER PRIMARY KEY
        REFERENCES conversations(id) ON DELETE CASCADE,
    region_version INTEGER NOT NULL,
    source_revision INTEGER NOT NULL DEFAULT 0,
    region_count INTEGER NOT NULL DEFAULT 0,
    docs_state TEXT NOT NULL DEFAULT 'pending',
    pending_doc_deletes TEXT NOT NULL DEFAULT '[]',
    published_at_ms INTEGER
);
CREATE INDEX IF NOT EXISTS idx_viewer_search_region_marks_state
    ON viewer_search_region_marks(docs_state);

-- Derived contentless FTS5 shadow: canonical schema-v1 shape (three-column
-- registration: the two indexed columns plus the implicit rowid).
CREATE VIRTUAL TABLE IF NOT EXISTS fts_messages USING fts5(
    content,
    title,
    content='', contentless_delete=1, tokenize='porter unicode61'
);

-- Row removal propagates into the contentless shadow via the FTS5 'delete'
-- command so deletes never leave stale rowids behind.
CREATE TRIGGER IF NOT EXISTS messages_fts_delete
AFTER DELETE ON messages
BEGIN
    INSERT INTO fts_messages(fts_messages, rowid) VALUES('delete', old.id);
END;

-- Durable schema marker read by status/doctor/logical-archive surfaces.
INSERT OR REPLACE INTO meta (key, value) VALUES ('schema_version', '1');
";

#[cfg(test)]
mod tests {
    use super::*;

    /// Every statement the bootstrap must emit, pinned so a later edit
    /// cannot silently drop a canonical object or drift the stamps.
    #[test]
    fn baseline_contains_all_canonical_objects() {
        for needle in [
            "CREATE TABLE IF NOT EXISTS meta",
            "CREATE TABLE IF NOT EXISTS agents",
            "CREATE TABLE IF NOT EXISTS workspaces",
            "CREATE TABLE IF NOT EXISTS sources",
            "CREATE TABLE IF NOT EXISTS conversations",
            "CREATE TABLE IF NOT EXISTS messages",
            "CREATE TABLE IF NOT EXISTS snippets",
            "CREATE TABLE IF NOT EXISTS tags",
            "CREATE TABLE IF NOT EXISTS conversation_tags",
            "CREATE TABLE IF NOT EXISTS daily_stats",
            "CREATE TABLE IF NOT EXISTS embedding_jobs",
            "CREATE TABLE IF NOT EXISTS token_usage",
            "CREATE TABLE IF NOT EXISTS token_daily_stats",
            "CREATE TABLE IF NOT EXISTS model_pricing",
            "CREATE TABLE IF NOT EXISTS message_metrics",
            "CREATE TABLE IF NOT EXISTS usage_hourly",
            "CREATE TABLE IF NOT EXISTS usage_daily",
            "CREATE TABLE IF NOT EXISTS usage_models_daily",
            "CREATE TABLE IF NOT EXISTS conversation_tail_state",
            "CREATE TABLE IF NOT EXISTS conversation_external_lookup",
            "CREATE TABLE IF NOT EXISTS conversation_external_tail_lookup",
            "CREATE TABLE IF NOT EXISTS forgotten_sources",
            "CREATE TABLE IF NOT EXISTS forgotten_conversations",
            "CREATE TABLE IF NOT EXISTS viewer_search_regions",
            "CREATE TABLE IF NOT EXISTS viewer_search_region_marks",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_conversations_provenance",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_conv_idx",
            "CREATE INDEX IF NOT EXISTS idx_snippets_message_id",
            "CREATE INDEX IF NOT EXISTS idx_conversations_context",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_embedding_jobs_active",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_viewer_search_regions_quill",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_viewer_search_regions_key",
            "CREATE INDEX IF NOT EXISTS idx_viewer_search_regions_scope",
            "CREATE INDEX IF NOT EXISTS idx_viewer_search_regions_tool",
            "CREATE INDEX IF NOT EXISTS idx_viewer_search_regions_file",
            "CREATE INDEX IF NOT EXISTS idx_viewer_search_region_marks_state",
            "ON DELETE CASCADE",
            "tokenize='porter unicode61'",
            "contentless_delete=1",
            "AFTER DELETE ON messages",
            "INSERT INTO fts_messages(fts_messages, rowid) VALUES('delete', old.id)",
            "VALUES ('schema_version', '1')",
        ] {
            assert!(
                BASELINE_SCHEMA_V1_SQL.contains(needle),
                "baseline schema missing: {needle}"
            );
        }
        // Exactly one idx_messages_conv_idx creation site.
        assert_eq!(
            BASELINE_SCHEMA_V1_SQL
                .matches("idx_messages_conv_idx")
                .count(),
            2, // index name + its comment mention
            "idx_messages_conv_idx must be declared exactly once"
        );
    }
}
