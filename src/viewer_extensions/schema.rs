//! Additive `viewer_*` schema for the CASS-owned Viewer backend.
//!
//! All objects are `CREATE ... IF NOT EXISTS` inside the canonical archive;
//! nothing here alters, drops or inserts into upstream schema-22 tables.
//! The engine dialect is deliberately conservative: no generated columns,
//! no expression indexes, no CHECK constraints, no JSON operators in the
//! DDL itself — only constructs the canonical schema already relies on
//! (`REFERENCES`, composite `PRIMARY KEY`, `AUTOINCREMENT`, `INSERT OR
//! IGNORE`). `tests/viewer_bridge_extensions.rs` probes and pins the actual
//! FrankenSQLite behavior (FK declaration visibility, enforcement,
//! `EXPLAIN QUERY PLAN`, optional JSON functions).
//!
//! `viewer_change_log` is provisioned empty now (bounded replay journal for
//! the Task-11 change stream); its writer semantics are owned by that task.
//! If its final shape differs, `EXTENSION_SCHEMA_VERSION` bumps and this
//! module adds a follow-up additive step.

use anyhow::{Context, Result, bail};

use crate::franken_sync::Connection;
use crate::franken_sync::compat::RowExt;

/// Version of the additive extension profile. Bumped only when a later task
/// needs objects that `IF NOT EXISTS` DDL cannot express (e.g. a column
/// addition); each bump adds a new idempotent step, never a rewrite of
/// existing objects.
///
/// v2: the Task-8 structural projection tables (`viewer_session_projection`,
/// `viewer_step_links`, `viewer_turn_links`, `viewer_tool_calls`).
/// v3: ingest-generation support — `viewer_session_facts.parent_external_id`
/// (durable parent evidence for the child-before-parent fixup) plus the
/// `viewer_ingest_receipt_cleanup` delete trigger so a removed canonical
/// session can never leave a `viewer_meta` ingest receipt claiming coverage.
/// v4: the `viewer_session_facts_parent_cleanup` delete trigger so a
/// removed canonical parent cannot FK-refuse its own deletion or leave a
/// child row pointing at a gone conversation.
pub const EXTENSION_SCHEMA_VERSION: i64 = 4;

/// Additive viewer schema. Every statement is idempotent so re-running the
/// profile on an initialized archive is a no-op.
const VIEWER_SCHEMA_V1: &str = r"
-- Extension profile bookkeeping (upstream `meta` is never touched).
CREATE TABLE IF NOT EXISTS viewer_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- User edits only: pin state, a custom display title and free-form notes.
-- No canonical text is copied. `revision` is the per-row annotation
-- revision consumed by patch_annotations' expected-revision check.
CREATE TABLE IF NOT EXISTS viewer_annotations (
    conversation_id INTEGER PRIMARY KEY
        REFERENCES conversations(id) ON DELETE CASCADE,
    is_pinned INTEGER NOT NULL DEFAULT 0,
    custom_title TEXT,
    notes TEXT,
    revision INTEGER NOT NULL DEFAULT 0,
    updated_at_ms INTEGER
);

-- Flat user tags (conversation, tag). Canonical `tags`/`conversation_tags`
-- stay owned by upstream ingest; this lane keeps metadata edits out of the
-- canonical revision.
CREATE TABLE IF NOT EXISTS viewer_tags (
    conversation_id INTEGER NOT NULL
        REFERENCES conversations(id) ON DELETE CASCADE,
    tag TEXT NOT NULL,
    created_at_ms INTEGER,
    PRIMARY KEY (conversation_id, tag)
);
CREATE INDEX IF NOT EXISTS idx_viewer_tags_tag ON viewer_tags(tag);

-- Derived session facts persisted at ingestion. Every metric column is
-- nullable: unknown stays NULL, never a fabricated 0. No column duplicates
-- an already-measured canonical field (tokens, cost, api/tool/message
-- counts, timestamps all remain canonical-only).
CREATE TABLE IF NOT EXISTS viewer_session_facts (
    conversation_id INTEGER PRIMARY KEY
        REFERENCES conversations(id) ON DELETE CASCADE,
    extractor_version INTEGER NOT NULL,
    source_revision INTEGER NOT NULL DEFAULT 0,
    coverage_state TEXT NOT NULL DEFAULT 'unknown',
    coverage_detail_json TEXT,
    is_subagent INTEGER,
    parent_conversation_id INTEGER REFERENCES conversations(id),
    -- Schema v3: provider parent evidence (`parentSessionId`/`parent_id`)
    -- persisted durably so the child-before-parent fixup can resolve the
    -- link when the parent lands on a later ingest. Pre-v3 archives gain
    -- this column through the guarded ALTER in migrate_facts_parent_evidence.
    parent_external_id TEXT,
    task_category TEXT,
    turn_count INTEGER,
    step_count INTEGER,
    thinking_block_count INTEGER,
    artifact_count INTEGER,
    subagent_count INTEGER,
    tool_error_count INTEGER,
    tool_calls_with_exit_status INTEGER,
    file_fact_count INTEGER,
    compaction_count INTEGER,
    model_set_json TEXT,
    first_prompt TEXT,
    extracted_at_ms INTEGER
);
CREATE INDEX IF NOT EXISTS idx_viewer_session_facts_parent
    ON viewer_session_facts(parent_conversation_id);
-- `list_sessions` category filter (`f.task_category = ?`) can drive from
-- this index instead of scanning facts; NULLs/unknowns stay unindexed.
CREATE INDEX IF NOT EXISTS idx_viewer_session_facts_category
    ON viewer_session_facts(task_category);

-- Singleton logical-revision row (id is always 1 by writer convention).
-- Physical WAL/data_version changes refresh readers; these counters are
-- the logical revisions bound into response cursors. Metadata edits bump
-- annotation_revision only; canonical_revision and lexical_generation stay
-- independent (spec decision 12).
CREATE TABLE IF NOT EXISTS viewer_revisions (
    id INTEGER PRIMARY KEY,
    canonical_revision INTEGER NOT NULL DEFAULT 0,
    annotation_revision INTEGER NOT NULL DEFAULT 0,
    projection_revision INTEGER NOT NULL DEFAULT 0,
    lexical_generation INTEGER NOT NULL DEFAULT 0,
    updated_at_ms INTEGER
);
INSERT OR IGNORE INTO viewer_revisions (id) VALUES (1);

-- Bounded change journal (Task 11 change stream). Canonical observer
-- triggers append session_* rows inside the writers' commits; extension
-- triggers append annotations_updated; index_ready/reset_required arrive
-- via the explicit append lane. `changes_since` pages by the rowid `seq`
-- (`WHERE seq > ? ORDER BY seq LIMIT ?` — EXPLAIN QUERY PLAN: SCAN, no
-- temp b-tree; pinned in tests/viewer_change_stream.rs). The notify
-- dedupe GROUP BY sorts the bounded committed tail once per post-commit
-- pass — cheaper than maintaining an index on every trigger-append
-- during bulk ingest — so no secondary index is justified on this capped
-- append-only journal.
CREATE TABLE IF NOT EXISTS viewer_change_log (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    conversation_id INTEGER,
    revision INTEGER NOT NULL,
    payload_json TEXT,
    created_at_ms INTEGER
);

";

/// Schema v2 — the Task-8 structural projection (spec decision 4/5: narrow
/// derived links anchored to canonical messages, never a second transcript,
/// never denormalized message bodies).
///
/// `viewer_step_links` carries two row shapes distinguished by `scope`:
/// provenance members (`member`/`result`) recording which canonical
/// messages produced or were absorbed into the step, and content parts
/// (`text`/`thinking`/`tool_call`/`tool_result`) recording the step's
/// derived block inventory. `source_idx` is the canonical `messages.idx`
/// coordinate (stable across id churn); `message_id`/`source_key` resolve
/// to `messages(id)` at publish time.
///
/// `viewer_tool_calls` stores verified call→result links only (ambiguous/
/// orphan records stay documented in `viewer_session_facts` coverage, never
/// positional-guessed). `result_block_index` is the recorded-result ordinal
/// `get_tool_output` needs inside multi-output messages.
const VIEWER_SCHEMA_V2: &str = r"
-- Per-session projection marker: `(projection_version, extractor_version,
-- source_revision)` is the staleness key — a later extractor/projection
-- change triggers targeted recomputation on the next ingest.
CREATE TABLE IF NOT EXISTS viewer_session_projection (
    conversation_id INTEGER PRIMARY KEY
        REFERENCES conversations(id) ON DELETE CASCADE,
    projection_version INTEGER NOT NULL,
    extractor_version INTEGER NOT NULL,
    source_revision INTEGER NOT NULL DEFAULT 0,
    step_count INTEGER,
    turn_count INTEGER,
    tool_call_count INTEGER,
    projected_at_ms INTEGER
);

CREATE TABLE IF NOT EXISTS viewer_step_links (
    conversation_id INTEGER NOT NULL
        REFERENCES conversations(id) ON DELETE CASCADE,
    step_index INTEGER NOT NULL,
    turn_index INTEGER,
    step_kind TEXT NOT NULL,
    link_index INTEGER NOT NULL,
    scope TEXT NOT NULL,
    block_index INTEGER,
    call_id TEXT,
    source_idx INTEGER NOT NULL,
    source_key TEXT,
    message_id INTEGER,
    part_key TEXT,
    PRIMARY KEY (conversation_id, step_index, link_index)
);
-- Message → step lookup and step-range probes both drive from this index.
CREATE INDEX IF NOT EXISTS idx_viewer_step_links_message
    ON viewer_step_links(conversation_id, source_idx);
CREATE INDEX IF NOT EXISTS idx_viewer_step_links_call
    ON viewer_step_links(conversation_id, call_id);

CREATE TABLE IF NOT EXISTS viewer_turn_links (
    conversation_id INTEGER NOT NULL
        REFERENCES conversations(id) ON DELETE CASCADE,
    turn_index INTEGER NOT NULL,
    turn_kind TEXT NOT NULL,
    first_step_index INTEGER NOT NULL,
    step_count INTEGER NOT NULL,
    source_idx INTEGER,
    message_id INTEGER,
    source_key TEXT,
    PRIMARY KEY (conversation_id, turn_index)
);

CREATE TABLE IF NOT EXISTS viewer_tool_calls (
    conversation_id INTEGER NOT NULL
        REFERENCES conversations(id) ON DELETE CASCADE,
    call_id TEXT NOT NULL,
    raw_call_id TEXT NOT NULL,
    name TEXT NOT NULL,
    step_index INTEGER,
    turn_index INTEGER,
    has_result INTEGER NOT NULL DEFAULT 0,
    is_error INTEGER,
    exit_code INTEGER,
    file_path TEXT,
    call_idx INTEGER NOT NULL,
    call_message_id INTEGER,
    result_idx INTEGER,
    result_message_id INTEGER,
    result_block_index INTEGER,
    ordinal INTEGER NOT NULL,
    PRIMARY KEY (conversation_id, call_id)
);
CREATE INDEX IF NOT EXISTS idx_viewer_tool_calls_step
    ON viewer_tool_calls(conversation_id, step_index);
CREATE INDEX IF NOT EXISTS idx_viewer_tool_calls_file
    ON viewer_tool_calls(conversation_id, file_path);

INSERT OR REPLACE INTO viewer_meta (key, value)
VALUES ('extension_schema_version', '2');
";

/// Schema v3 — ingest-generation support.
///
/// `viewer_session_facts.parent_external_id` persists the provider parent
/// evidence (`parentSessionId`/`parent_id`) alongside the resolved
/// `parent_conversation_id`. A subagent transcript ingested BEFORE its
/// parent keeps the evidence durably so the targeted fixup can resolve the
/// link when the parent arrives on a later ingest — no catalog rescan, no
/// evidence re-parse. The column itself lands via a guarded `ALTER TABLE`
/// (see [`migrate_facts_parent_evidence`]); this batch carries the index,
/// the receipt-cleanup trigger and the version marker.
///
/// `viewer_ingest_receipt_cleanup` is the explicit removal half of the
/// ingest-receipt protocol: extension row tables cascade on
/// `conversations(id)` already, but `viewer_meta` receipts are keyed
/// strings, not FK rows — without this trigger a `cass forget` would leave
/// a durable `complete` receipt claiming coverage for a deleted session.
/// It is a schema object (not a `viewer_rev_*` observer): it journals
/// nothing and bumps no revision.
const VIEWER_SCHEMA_V3: &str = r"
CREATE INDEX IF NOT EXISTS idx_viewer_session_facts_parent_ext
    ON viewer_session_facts(parent_external_id);

DROP TRIGGER IF EXISTS viewer_ingest_receipt_cleanup;
CREATE TRIGGER viewer_ingest_receipt_cleanup AFTER DELETE ON conversations
BEGIN
    DELETE FROM viewer_meta WHERE key = 'ingest_receipt:' || OLD.id;
END;

INSERT OR REPLACE INTO viewer_meta (key, value)
VALUES ('extension_schema_version', '3');
";

/// Schema v4 — parent-removal repair lane.
///
/// `viewer_session_facts_parent_cleanup` mirrors
/// `viewer_ingest_receipt_cleanup` for the parent link: the fixup creates
/// `parent_conversation_id` references in the child-first direction, so a
/// `cass forget` of a resolved parent would otherwise be refused by the
/// `REFERENCES conversations(id)` constraint (or, under a writer without
/// FK enforcement, leave a child pointing at a gone conversation). The
/// BEFORE DELETE trigger NULLs the link inside the same statement — the
/// child's own rows stay, and a later matching parent can re-resolve the
/// still-durable `parent_external_id` evidence.
const VIEWER_SCHEMA_V4: &str = r"
DROP TRIGGER IF EXISTS viewer_session_facts_parent_cleanup;
CREATE TRIGGER viewer_session_facts_parent_cleanup BEFORE DELETE ON conversations
BEGIN
    UPDATE viewer_session_facts
    SET parent_conversation_id = NULL
    WHERE parent_conversation_id = OLD.id;
END;

INSERT OR REPLACE INTO viewer_meta (key, value)
VALUES ('extension_schema_version', '4');
";

/// Guarded v3 column migration: `parent_external_id` cannot ride the
/// `CREATE TABLE` statement (existing v2 archives keep their table), so it
/// lands as an `ALTER TABLE` only when the column is absent — inside the
/// same schema transaction as the rest of the profile.
fn migrate_facts_parent_evidence(connection: &Connection) -> Result<()> {
    let present = connection
        .query("PRAGMA table_info(viewer_session_facts)")?
        .iter()
        .any(|row| {
            row.get_typed::<String>(1)
                .map(|name| name == "parent_external_id")
                .unwrap_or(false)
        });
    if !present {
        connection
            .execute(
                "ALTER TABLE viewer_session_facts \
                 ADD COLUMN parent_external_id TEXT",
            )
            .context("adding viewer_session_facts.parent_external_id (schema v3)")?;
    }
    // Pre-v3 rows recorded the same parent evidence inside the coverage
    // manifest (`parent.evidence`) that the column now persists, so the
    // guarded migration can recover it — without this backfill those rows
    // would keep parent_external_id NULL forever: the row-level
    // idempotence key and the ingest receipt both skip republishing them,
    // and the child-before-parent fixup only scans rows that recorded the
    // column. `json_valid` keeps a malformed manifest from aborting the
    // schema transaction; rows without recorded evidence simply stay NULL.
    connection
        .execute(
            "UPDATE viewer_session_facts \
             SET parent_external_id = \
                 json_extract(coverage_detail_json, '$.parent.evidence') \
             WHERE parent_external_id IS NULL \
               AND json_valid(coverage_detail_json)",
        )
        .context("backfilling viewer_session_facts.parent_external_id (schema v3)")?;
    Ok(())
}

/// The ingest hook's cheap profile guard: `true` when the extension
/// profile is installed at the current version, installing/upgrading it
/// when missing or stale. One indexed `viewer_meta` read on the hot path;
/// the full DDL batch only runs once per archive (and once more whenever
/// `EXTENSION_SCHEMA_VERSION` moves).
///
/// This is the same `ensure_schema` the authorized `--init-extensions`
/// pass runs — additive `IF NOT EXISTS`/guarded-`ALTER` DDL inside the
/// canonical archive, never a sidecar, never a canonical-table write.
/// Canonical observer triggers stay owned by the authorized maintenance
/// lane (`revisions::ensure_observers`); ingest publication itself does
/// not depend on them.
pub fn ensure_profile_for_ingest(connection: &Connection) -> Result<bool> {
    match installed_version(connection)? {
        Some(version) if version >= EXTENSION_SCHEMA_VERSION => Ok(true),
        _ => {
            ensure_schema(connection)?;
            Ok(true)
        }
    }
}

/// Initialize (or verify) the additive `viewer_*` extension profile inside
/// the canonical archive.
///
/// Requirements and guarantees:
/// * The canonical schema must already exist — this refuses on a database
///   without the `conversations` table so the profile is always installed
///   by an authorized CASS writer before `StrictArchive::open`, never on a
///   bare file.
/// * Idempotent: every statement uses `IF NOT EXISTS` / `INSERT OR IGNORE`,
///   and the singleton revision row and meta marker are preserved.
/// * Atomic: all DDL runs in one transaction; a refusal leaves no partial
///   `viewer_*` objects behind.
/// * Non-invasive: no upstream table is altered, dropped or written.
pub fn ensure_schema(connection: &Connection) -> Result<()> {
    require_canonical_base(connection)?;
    connection
        .execute("PRAGMA foreign_keys = ON")
        .context("enabling foreign key enforcement for viewer extension schema")?;
    connection
        .execute("BEGIN")
        .context("starting viewer extension schema transaction")?;
    let applied = (|| -> Result<()> {
        connection.execute_batch(VIEWER_SCHEMA_V1)?;
        connection.execute_batch(VIEWER_SCHEMA_V2)?;
        migrate_facts_parent_evidence(connection)?;
        connection.execute_batch(VIEWER_SCHEMA_V3)?;
        connection.execute_batch(VIEWER_SCHEMA_V4)?;
        Ok(())
    })();
    match applied {
        Ok(()) => {
            connection
                .execute("COMMIT")
                .context("committing viewer extension schema")?;
            Ok(())
        }
        Err(error) => {
            // Best-effort rollback; the DDL error is the one to report.
            let _ = connection.execute("ROLLBACK");
            Err(error).context("applying viewer extension schema DDL")
        }
    }
}

/// The installed extension profile version, if `viewer_meta` exists.
pub fn installed_version(connection: &Connection) -> Result<Option<i64>> {
    let present = connection
        .query("SELECT 1 FROM sqlite_master WHERE type='table' AND name='viewer_meta' LIMIT 1")?;
    if present.is_empty() {
        return Ok(None);
    }
    let value: Option<String> = connection
        .query_row("SELECT value FROM viewer_meta WHERE key='extension_schema_version'")?
        .get_typed(0)?;
    match value {
        Some(text) => text
            .parse::<i64>()
            .map(Some)
            .context("viewer_meta.extension_schema_version is not an integer"),
        None => Ok(None),
    }
}

/// The extension profile is anchored to the canonical schema: it must never
/// be installed on a database that lacks upstream `conversations`.
fn require_canonical_base(connection: &Connection) -> Result<()> {
    let rows = connection
        .query("SELECT 1 FROM sqlite_master WHERE type='table' AND name='conversations' LIMIT 1")?;
    if rows.is_empty() {
        bail!(
            "viewer extension schema requires the canonical schema \
             (table 'conversations' missing); run cass storage migrations first"
        );
    }
    Ok(())
}
