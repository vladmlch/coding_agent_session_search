pub(super) use super::omp_and_semantic_meta::{
    LEGACY_OMP_ANALYTICS_CURSOR_CONTEXT_META_KEY, LEGACY_OMP_ANALYTICS_CURSOR_LAST_ID_META_KEY,
    LEGACY_OMP_ANALYTICS_CURSOR_PROCESSED_META_KEY, LEGACY_OMP_ANALYTICS_REBUILT_META_KEY,
    LEGACY_OMP_LEXICAL_PUBLISHED_META_KEY, LEGACY_OMP_RECLASSIFICATION_META_KEY,
    LegacyOmpReclassificationResult, PREVIOUS_LEGACY_OMP_RECLASSIFICATION_META_KEY,
    SemanticIdentityTier,
};

#[cfg(test)]
pub(super) const MIGRATION_V1: &str = r"
PRAGMA foreign_keys = ON;

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

CREATE TABLE IF NOT EXISTS conversations (
    id INTEGER PRIMARY KEY,
    agent_id INTEGER NOT NULL REFERENCES agents(id),
    workspace_id INTEGER REFERENCES workspaces(id),
    external_id TEXT,
    title TEXT,
    source_path TEXT NOT NULL,
    started_at INTEGER,
    ended_at INTEGER,
    approx_tokens INTEGER,
    metadata_json TEXT,
    UNIQUE(agent_id, external_id)
);

CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY,
    conversation_id INTEGER NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    idx INTEGER NOT NULL,
    role TEXT NOT NULL,
    author TEXT,
    created_at INTEGER,
    content TEXT NOT NULL,
    extra_json TEXT,
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

CREATE INDEX IF NOT EXISTS idx_conversations_agent_started
    ON conversations(agent_id, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_messages_conv_idx
    ON messages(conversation_id, idx);

";

#[cfg(test)]
pub(super) const MIGRATION_V2: &str = r"
CREATE VIRTUAL TABLE IF NOT EXISTS fts_messages USING fts5(
    content,
    title,
    agent,
    workspace,
    source_path,
    created_at UNINDEXED,
    message_id UNINDEXED,
    tokenize='porter'
);
INSERT INTO fts_messages(content, title, agent, workspace, source_path, created_at, message_id)
SELECT
    m.content,
    c.title,
    a.slug,
    w.path,
    c.source_path,
    m.created_at,
    m.id
FROM messages m
JOIN conversations c ON m.conversation_id = c.id
JOIN agents a ON c.agent_id = a.id
LEFT JOIN workspaces w ON c.workspace_id = w.id;
";

#[cfg(test)]
#[allow(dead_code)]
const MIGRATION_V3: &str = r"
DROP TABLE IF EXISTS fts_messages;
CREATE VIRTUAL TABLE fts_messages USING fts5(
    content,
    title,
    agent,
    workspace,
    source_path,
    created_at UNINDEXED,
    message_id UNINDEXED,
    tokenize='porter'
);
INSERT INTO fts_messages(content, title, agent, workspace, source_path, created_at, message_id)
SELECT
    m.content,
    c.title,
    a.slug,
    w.path,
    c.source_path,
    m.created_at,
    m.id
FROM messages m
JOIN conversations c ON m.conversation_id = c.id
JOIN agents a ON c.agent_id = a.id
LEFT JOIN workspaces w ON c.workspace_id = w.id;
";

#[cfg(test)]
pub(super) const MIGRATION_V4: &str = r"
-- Sources table for tracking where conversations come from
CREATE TABLE IF NOT EXISTS sources (
    id TEXT PRIMARY KEY,           -- source_id (e.g., 'local', 'work-laptop')
    kind TEXT NOT NULL,            -- 'local', 'ssh', etc.
    host_label TEXT,               -- display label
    machine_id TEXT,               -- optional stable machine id
    platform TEXT,                 -- 'macos', 'linux', 'windows'
    config_json TEXT,              -- JSON blob for extra config (SSH params, path rewrites)
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

-- Bootstrap: Insert the default 'local' source
INSERT OR IGNORE INTO sources (id, kind, host_label, created_at, updated_at)
VALUES ('local', 'local', NULL, strftime('%s','now')*1000, strftime('%s','now')*1000);
";

#[cfg(test)]
pub(super) const MIGRATION_V5: &str = r"
-- Add provenance columns to conversations table
-- SQLite cannot alter unique constraints, so we need to recreate the table

-- Create new table with provenance columns and updated unique constraint
CREATE TABLE conversations_new (
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
    UNIQUE(source_id, agent_id, external_id)
);

-- Copy data from old table (all existing conversations get source_id='local')
INSERT INTO conversations_new (id, agent_id, workspace_id, source_id, external_id, title,
                               source_path, started_at, ended_at, approx_tokens, metadata_json, origin_host)
SELECT id, agent_id, workspace_id, 'local', external_id, title,
       source_path, started_at, ended_at, approx_tokens, metadata_json, NULL
FROM conversations;

-- Drop old table and rename new
DROP TABLE conversations;
ALTER TABLE conversations_new RENAME TO conversations;

-- Recreate indexes
CREATE INDEX IF NOT EXISTS idx_conversations_agent_started ON conversations(agent_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_conversations_source_id ON conversations(source_id);
";

#[cfg(test)]
pub(super) const MIGRATION_V6: &str = r"
-- Optimize lookup by source_path (used by TUI detail view)
CREATE INDEX IF NOT EXISTS idx_conversations_source_path ON conversations(source_path);
";

#[cfg(test)]
pub(super) const MIGRATION_V7: &str = r"
-- Add binary columns for MessagePack serialization (Opt 3.1)
-- Binary format is 50-70% smaller than JSON and faster to parse
ALTER TABLE conversations ADD COLUMN metadata_bin BLOB;
ALTER TABLE messages ADD COLUMN extra_bin BLOB;
";

#[cfg(test)]
pub(super) const MIGRATION_V8: &str = r"
-- Opt 3.2: Daily stats materialized table for O(1) time-range histograms
-- Provides fast aggregated queries for stats/dashboard without full table scans

CREATE TABLE IF NOT EXISTS daily_stats (
    day_id INTEGER NOT NULL,              -- Days since 2020-01-01 (Unix epoch + offset)
    agent_slug TEXT NOT NULL,             -- 'all' for totals, or specific agent slug
    source_id TEXT NOT NULL DEFAULT 'all', -- 'all' for totals, or specific source
    session_count INTEGER NOT NULL DEFAULT 0,
    message_count INTEGER NOT NULL DEFAULT 0,
    total_chars INTEGER NOT NULL DEFAULT 0,
    last_updated INTEGER NOT NULL,
    PRIMARY KEY (day_id, agent_slug, source_id)
);

CREATE INDEX IF NOT EXISTS idx_daily_stats_agent ON daily_stats(agent_slug, day_id);
CREATE INDEX IF NOT EXISTS idx_daily_stats_source ON daily_stats(source_id, day_id);
";

#[cfg(test)]
pub(super) const MIGRATION_V9: &str = r"
-- Background embedding jobs tracking table
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

-- Only one pending or running job per (db_path, model_id) at a time.
-- Multiple completed/failed/cancelled jobs are allowed for history.
CREATE UNIQUE INDEX IF NOT EXISTS idx_embedding_jobs_active
ON embedding_jobs(db_path, model_id)
WHERE status IN ('pending', 'running');
";

#[cfg(test)]
pub(super) const MIGRATION_V10: &str = r"
-- Token analytics: per-message token usage ledger
CREATE TABLE IF NOT EXISTS token_usage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    conversation_id INTEGER NOT NULL,
    agent_id INTEGER NOT NULL,
    workspace_id INTEGER,
    source_id TEXT NOT NULL DEFAULT 'local',

    -- Timing
    timestamp_ms INTEGER NOT NULL,
    day_id INTEGER NOT NULL,

    -- Model identification
    model_name TEXT,
    model_family TEXT,
    model_tier TEXT,
    service_tier TEXT,
    provider TEXT,

    -- Token counts (nullable — not all agents provide all fields)
    input_tokens INTEGER,
    output_tokens INTEGER,
    cache_read_tokens INTEGER,
    cache_creation_tokens INTEGER,
    thinking_tokens INTEGER,
    total_tokens INTEGER,

    -- Cost estimation
    estimated_cost_usd REAL,

    -- Message context
    role TEXT NOT NULL,
    content_chars INTEGER NOT NULL,
    has_tool_calls INTEGER NOT NULL DEFAULT 0,
    tool_call_count INTEGER NOT NULL DEFAULT 0,

    -- Data quality
    data_source TEXT NOT NULL DEFAULT 'api',

    UNIQUE(message_id)
);

CREATE INDEX IF NOT EXISTS idx_token_usage_day ON token_usage(day_id, agent_id);
CREATE INDEX IF NOT EXISTS idx_token_usage_conv ON token_usage(conversation_id);
CREATE INDEX IF NOT EXISTS idx_token_usage_model ON token_usage(model_family, day_id);
CREATE INDEX IF NOT EXISTS idx_token_usage_workspace ON token_usage(workspace_id, day_id);
CREATE INDEX IF NOT EXISTS idx_token_usage_timestamp ON token_usage(timestamp_ms);

-- Token analytics: pre-aggregated daily rollups
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

-- Model pricing lookup table
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

-- Seed with current pricing (as of 2026-02)
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

-- Extend conversations table with token summary columns
ALTER TABLE conversations ADD COLUMN total_input_tokens INTEGER;
ALTER TABLE conversations ADD COLUMN total_output_tokens INTEGER;
ALTER TABLE conversations ADD COLUMN total_cache_read_tokens INTEGER;
ALTER TABLE conversations ADD COLUMN total_cache_creation_tokens INTEGER;
ALTER TABLE conversations ADD COLUMN grand_total_tokens INTEGER;
ALTER TABLE conversations ADD COLUMN estimated_cost_usd REAL;
ALTER TABLE conversations ADD COLUMN primary_model TEXT;
ALTER TABLE conversations ADD COLUMN api_call_count INTEGER;
ALTER TABLE conversations ADD COLUMN tool_call_count INTEGER;
ALTER TABLE conversations ADD COLUMN user_message_count INTEGER;
ALTER TABLE conversations ADD COLUMN assistant_message_count INTEGER;
";

pub(super) const MIGRATION_V14: &str = r"
-- Switch FTS5 from internal-content to contentless mode (CASS #163).
-- Drop the old V13 internal-content fts_messages first so that
-- sqlite_schema does not contain two conflicting CREATE VIRTUAL TABLE
-- entries, which makes the database completely unreadable.
-- The current contentless table is recreated lazily after open() only when the
-- frankensqlite FTS consistency check finds it missing or malformed.
DROP TABLE IF EXISTS fts_messages;
";

pub(super) const MIGRATION_V15_TAIL_STATE_TABLE: &str = r"
CREATE TABLE IF NOT EXISTS conversation_tail_state (
    -- Deliberately no FOREIGN KEY: this hot row is maintained by insert/append
    -- paths, and FK metadata keeps frankensqlite off the direct rowid update path.
    conversation_id INTEGER PRIMARY KEY,
    ended_at INTEGER,
    last_message_idx INTEGER,
    last_message_created_at INTEGER
);
";

pub(super) const MIGRATION_V16: &str = r"
-- UNIQUE(conversation_id, idx) already creates sqlite_autoindex_messages_1,
-- which covers the same lookup/order key as idx_messages_conv_idx. Keeping both
-- doubles message insert index maintenance on the hot indexing path.
DROP INDEX IF EXISTS idx_messages_conv_idx;
";

pub(super) const MIGRATION_V17: &str = r"
-- Drop the global messages(created_at) secondary index from the ingest hot
-- path. Search/time filters are served by the derived search layer and
-- conversation/analytics indexes, while this index is maintained on every
-- message insert.
DROP INDEX IF EXISTS idx_messages_created;
";

pub(super) const MIGRATION_V18: &str = r"
-- Move append-tail state out of the wide, indexed conversations row. The hot
-- append path updates this cache for every appended conversation; keeping it in
-- a tiny rowid table avoids rewriting the large conversation record.
CREATE TABLE IF NOT EXISTS conversation_tail_state (
    -- Deliberately no FOREIGN KEY: this hot row is maintained by insert/append
    -- paths, and FK metadata keeps frankensqlite off the direct rowid update path.
    conversation_id INTEGER PRIMARY KEY,
    ended_at INTEGER,
    last_message_idx INTEGER,
    last_message_created_at INTEGER
);

INSERT OR REPLACE INTO conversation_tail_state (
    conversation_id, ended_at, last_message_idx, last_message_created_at
)
SELECT id, ended_at, last_message_idx, last_message_created_at
FROM conversations
WHERE ended_at IS NOT NULL
   OR last_message_idx IS NOT NULL
   OR last_message_created_at IS NOT NULL;
";

pub(super) const MIGRATION_V19: &str = r"
-- Materialize external conversation provenance into one compact lookup key.
-- This keeps the hot append/new-conversation probe on a single primary-key
-- lookup instead of a composite conversations-table predicate.
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

pub(super) const MIGRATION_V20: &str = r"
-- Fuse external conversation lookup with append-tail state. Append-heavy
-- workloads can resolve both the conversation id and tail plan from one
-- primary-key probe.
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
    (SELECT ts.ended_at
     FROM conversation_tail_state ts
     WHERE ts.conversation_id = c.id),
    (SELECT ts.last_message_idx
     FROM conversation_tail_state ts
     WHERE ts.conversation_id = c.id),
    (SELECT ts.last_message_created_at
     FROM conversation_tail_state ts
     WHERE ts.conversation_id = c.id)
FROM conversations c
WHERE c.external_id IS NOT NULL;
";

pub(super) const MIGRATION_V21: &str = r"
-- Context reads this covering index before hydrating its limited result sets.
-- Keep wide metadata records out of candidate selection (GH #463). The rowid
-- stored in each index entry supplies conversations.id without a table read.
CREATE INDEX IF NOT EXISTS idx_conversations_context
ON conversations(started_at DESC, workspace_id, agent_id);
";

pub(super) const MIGRATION_V22: &str = r"
-- `cass forget --apply` tombstones (2l1b0.50): the forgotten source file's size
-- and modification time. Scans skip an unchanged source; a changed one is
-- ingested again and its tombstone is removed.
CREATE TABLE IF NOT EXISTS forgotten_sources (
    source_path TEXT PRIMARY KEY,
    size_bytes INTEGER,
    mtime_ms INTEGER,
    forgotten_at_ms INTEGER NOT NULL
);
";

pub(super) const MIGRATION_V23: &str = r"
-- Shared stores change whenever any session changes. Keep forgotten session
-- identities independently of the store's file stamp (GH #537). These values
-- deliberately do not reference rows that forgetting or agent removal deletes.
CREATE TABLE IF NOT EXISTS forgotten_conversations (
    source_id TEXT NOT NULL,
    agent_slug TEXT NOT NULL,
    source_path TEXT NOT NULL,
    external_id TEXT NOT NULL,
    forgotten_at_ms INTEGER NOT NULL,
    PRIMARY KEY (source_id, agent_slug, source_path, external_id)
);
";
