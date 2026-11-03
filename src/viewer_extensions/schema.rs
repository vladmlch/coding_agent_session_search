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
/// v5: `viewer_session_sort` — persisted scalar browse sort keys (pin,
/// knownness, sort value, verified public tie key) plus the single-table
/// filter mirrors, the seven per-sort composite order indexes, and the
/// `viewer_sort_*` maintenance triggers keeping every scalar column
/// transactionally in step with its source table.
/// v6: `viewer_sort_agents_del` — the missing agents-side dimension
/// invalidation counterpart to `viewer_sort_sources_del`: an `agents`
/// DELETE leaves a dangling `agent_id` whose stored tie key was verified
/// against a dimension row that no longer exists; the trigger returns
/// those rows to pending so browse refuses honestly until re-resolved.
pub const EXTENSION_SCHEMA_VERSION: i64 = 6;

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

/// Schema v5 — persisted browse sort keys.
///
/// `viewer_session_sort` carries, beside the existing extension facts, the
/// missing scalar sort facts the browse ORDER BY needs: pin state, the
/// per-axis knownness flags, each sort value (effective activity timestamp
/// under tail-state precedence, created timestamp, workspace path, step and
/// artifact counts), the verified public tie key, and the single-table
/// filter mirrors (agent/source/workspace/classification) so a browse page
/// is served by ONE indexed scan on this table — the engine's join sorter
/// never sees the candidate set.
///
/// Column maintenance is split by what SQL can compute:
/// * the `viewer_sort_*` triggers recompute every canonical/annotation/
///   facts/workspace mirror transactionally inside whatever write produced
///   them (any connection, any lane) and invalidate `tie_key` when
///   identity-bearing canonical inputs move;
/// * `tie_key` itself needs provider-native-ID recomputation over canonical
///   rows plus `agents`/`sources` lookups — the ingest generation
///   transaction writes it per publication and the
///   `--init-extensions`/`--backfill-viewer-extensions` maintenance lanes
///   converge any NULL remainder (`viewer_extensions::sort`).
///
/// The migration INSERT..SELECT below backfills every pre-v5 conversation
/// inside the same schema transaction, so a successfully applied v5 means
/// full sort-row coverage by construction.
const VIEWER_SCHEMA_V5: &str = r"
CREATE TABLE IF NOT EXISTS viewer_session_sort (
    conversation_id INTEGER PRIMARY KEY
        REFERENCES conversations(id) ON DELETE CASCADE,
    is_pinned INTEGER NOT NULL DEFAULT 0,
    activity_known INTEGER NOT NULL DEFAULT 0,
    activity_ms INTEGER,
    created_known INTEGER NOT NULL DEFAULT 0,
    created_ms INTEGER,
    project_known INTEGER NOT NULL DEFAULT 0,
    project_path TEXT,
    steps_known INTEGER NOT NULL DEFAULT 0,
    step_count INTEGER,
    artifacts_known INTEGER NOT NULL DEFAULT 0,
    artifact_count INTEGER,
    -- Verified public tie key ({provider}:{native}); NULL + NULL error is
    -- 'pending' (the maintenance lanes resolve it), NULL + recorded kind is
    -- the typed refusal browse surfaces instead of guessing an order.
    tie_key TEXT,
    tie_error TEXT,
    -- Single-table filter mirrors (provider/project/tag stay resolvable:
    -- agent_id+workspace_id for dimension predicates, tag via EXISTS).
    agent_id INTEGER,
    source_id TEXT,
    workspace_id INTEGER,
    is_subagent INTEGER,
    task_category TEXT,
    coverage_state TEXT
);

-- Per-sort order indexes: (pin DESC, knownness DESC, value, public tie key
-- ASC) with the implicit rowid as the final stabilizer — the exact oracle
-- tuple (pins first, known values ordered, unknowns last, public-ID tie
-- order). `*_known` flags are 1=known, so DESC puts known rows first.
CREATE INDEX IF NOT EXISTS idx_viewer_sort_activity_desc
    ON viewer_session_sort(is_pinned DESC, activity_known DESC, activity_ms DESC, tie_key);
CREATE INDEX IF NOT EXISTS idx_viewer_sort_activity_asc
    ON viewer_session_sort(is_pinned DESC, activity_known DESC, activity_ms ASC, tie_key);
CREATE INDEX IF NOT EXISTS idx_viewer_sort_created_desc
    ON viewer_session_sort(is_pinned DESC, created_known DESC, created_ms DESC, tie_key);
CREATE INDEX IF NOT EXISTS idx_viewer_sort_created_asc
    ON viewer_session_sort(is_pinned DESC, created_known DESC, created_ms ASC, tie_key);
CREATE INDEX IF NOT EXISTS idx_viewer_sort_project_asc
    ON viewer_session_sort(is_pinned DESC, project_known DESC, project_path ASC, tie_key);
CREATE INDEX IF NOT EXISTS idx_viewer_sort_steps_desc
    ON viewer_session_sort(is_pinned DESC, steps_known DESC, step_count DESC, tie_key);
CREATE INDEX IF NOT EXISTS idx_viewer_sort_artifacts_desc
    ON viewer_session_sort(is_pinned DESC, artifacts_known DESC, artifact_count DESC, tie_key);
-- NULL-tie probe: pending/conflicted identities seek this range only.
CREATE INDEX IF NOT EXISTS idx_viewer_sort_tie_pending
    ON viewer_session_sort(tie_key);

-- Backfill one sort row per conversation that predates the profile (the
-- conversations-INSERT trigger below covers everything written after).
INSERT INTO viewer_session_sort (
    conversation_id, is_pinned,
    activity_known, activity_ms, created_known, created_ms,
    project_known, project_path,
    steps_known, step_count, artifacts_known, artifact_count,
    agent_id, source_id, workspace_id,
    is_subagent, task_category, coverage_state
)
SELECT c.id, COALESCE(va.is_pinned, 0),
    (CASE WHEN t.conversation_id IS NOT NULL
          THEN COALESCE(t.last_message_created_at, t.ended_at, c.started_at)
          ELSE COALESCE(c.last_message_created_at, c.ended_at, c.started_at)
     END) IS NOT NULL,
    CASE WHEN t.conversation_id IS NOT NULL
         THEN COALESCE(t.last_message_created_at, t.ended_at, c.started_at)
         ELSE COALESCE(c.last_message_created_at, c.ended_at, c.started_at)
    END,
    c.started_at IS NOT NULL,
    c.started_at,
    w.path IS NOT NULL,
    w.path,
    f.step_count IS NOT NULL,
    f.step_count,
    f.artifact_count IS NOT NULL,
    f.artifact_count,
    c.agent_id, c.source_id, c.workspace_id,
    f.is_subagent, f.task_category, f.coverage_state
FROM conversations c
LEFT JOIN conversation_tail_state t ON t.conversation_id = c.id
LEFT JOIN viewer_annotations va ON va.conversation_id = c.id
LEFT JOIN viewer_session_facts f ON f.conversation_id = c.id
LEFT JOIN workspaces w ON w.id = c.workspace_id
WHERE NOT EXISTS (
    SELECT 1 FROM viewer_session_sort vs WHERE vs.conversation_id = c.id
);

-- --- conversations lifecycle ---------------------------------------------
DROP TRIGGER IF EXISTS viewer_sort_conversations_ins;
CREATE TRIGGER viewer_sort_conversations_ins AFTER INSERT ON conversations BEGIN
    INSERT OR REPLACE INTO viewer_session_sort (
        conversation_id, is_pinned,
        activity_known, activity_ms, created_known, created_ms,
        project_known, project_path,
        agent_id, source_id, workspace_id
    ) VALUES (
        NEW.id, 0,
        COALESCE(NEW.last_message_created_at, NEW.ended_at, NEW.started_at) IS NOT NULL,
        COALESCE(NEW.last_message_created_at, NEW.ended_at, NEW.started_at),
        NEW.started_at IS NOT NULL,
        NEW.started_at,
        (SELECT w.path FROM workspaces w WHERE w.id = NEW.workspace_id) IS NOT NULL,
        (SELECT w.path FROM workspaces w WHERE w.id = NEW.workspace_id),
        NEW.agent_id, NEW.source_id, NEW.workspace_id
    );
END;

DROP TRIGGER IF EXISTS viewer_sort_conversations_upd;
CREATE TRIGGER viewer_sort_conversations_upd AFTER UPDATE ON conversations BEGIN
    UPDATE viewer_session_sort SET
        activity_known = (
            CASE WHEN (SELECT t.conversation_id FROM conversation_tail_state t
                       WHERE t.conversation_id = NEW.id LIMIT 1) IS NOT NULL
                 THEN COALESCE(
                     (SELECT t.last_message_created_at FROM conversation_tail_state t
                      WHERE t.conversation_id = NEW.id),
                     (SELECT t.ended_at FROM conversation_tail_state t
                      WHERE t.conversation_id = NEW.id),
                     NEW.started_at)
                 ELSE COALESCE(NEW.last_message_created_at, NEW.ended_at, NEW.started_at)
            END) IS NOT NULL,
        activity_ms =
            CASE WHEN (SELECT t.conversation_id FROM conversation_tail_state t
                       WHERE t.conversation_id = NEW.id LIMIT 1) IS NOT NULL
                 THEN COALESCE(
                     (SELECT t.last_message_created_at FROM conversation_tail_state t
                      WHERE t.conversation_id = NEW.id),
                     (SELECT t.ended_at FROM conversation_tail_state t
                      WHERE t.conversation_id = NEW.id),
                     NEW.started_at)
                 ELSE COALESCE(NEW.last_message_created_at, NEW.ended_at, NEW.started_at)
            END,
        created_known = NEW.started_at IS NOT NULL,
        created_ms = NEW.started_at,
        project_known = (SELECT w.path FROM workspaces w WHERE w.id = NEW.workspace_id) IS NOT NULL,
        project_path = (SELECT w.path FROM workspaces w WHERE w.id = NEW.workspace_id),
        agent_id = NEW.agent_id,
        source_id = NEW.source_id,
        workspace_id = NEW.workspace_id
    WHERE conversation_id = NEW.id;
    -- Identity-bearing inputs moved: the stored public tie key is no longer
    -- a verified identity for this row; the maintenance lanes re-resolve.
    UPDATE viewer_session_sort SET tie_key = NULL, tie_error = NULL
    WHERE conversation_id = NEW.id
      AND (OLD.external_id IS NOT NEW.external_id
           OR OLD.source_path IS NOT NEW.source_path
           OR OLD.source_id IS NOT NEW.source_id
           OR OLD.origin_host IS NOT NEW.origin_host
           OR OLD.metadata_json IS NOT NEW.metadata_json
           OR OLD.metadata_bin IS NOT NEW.metadata_bin
           OR OLD.agent_id IS NOT NEW.agent_id);
END;

DROP TRIGGER IF EXISTS viewer_sort_conversations_del;
CREATE TRIGGER viewer_sort_conversations_del AFTER DELETE ON conversations BEGIN
    DELETE FROM viewer_session_sort WHERE conversation_id = OLD.id;
END;

-- --- tail-state precedence over the denormalized activity columns ---------
DROP TRIGGER IF EXISTS viewer_sort_tail_ins;
CREATE TRIGGER viewer_sort_tail_ins AFTER INSERT ON conversation_tail_state BEGIN
    UPDATE viewer_session_sort SET
        activity_known = COALESCE(
            NEW.last_message_created_at, NEW.ended_at,
            (SELECT c.started_at FROM conversations c WHERE c.id = NEW.conversation_id)
        ) IS NOT NULL,
        activity_ms = COALESCE(
            NEW.last_message_created_at, NEW.ended_at,
            (SELECT c.started_at FROM conversations c WHERE c.id = NEW.conversation_id)
        )
    WHERE conversation_id = NEW.conversation_id;
END;

DROP TRIGGER IF EXISTS viewer_sort_tail_upd;
CREATE TRIGGER viewer_sort_tail_upd AFTER UPDATE ON conversation_tail_state BEGIN
    UPDATE viewer_session_sort SET
        activity_known = COALESCE(
            NEW.last_message_created_at, NEW.ended_at,
            (SELECT c.started_at FROM conversations c WHERE c.id = NEW.conversation_id)
        ) IS NOT NULL,
        activity_ms = COALESCE(
            NEW.last_message_created_at, NEW.ended_at,
            (SELECT c.started_at FROM conversations c WHERE c.id = NEW.conversation_id)
        )
    WHERE conversation_id = NEW.conversation_id;
END;

DROP TRIGGER IF EXISTS viewer_sort_tail_del;
CREATE TRIGGER viewer_sort_tail_del AFTER DELETE ON conversation_tail_state BEGIN
    UPDATE viewer_session_sort SET
        activity_known = COALESCE(
            (SELECT c.last_message_created_at FROM conversations c WHERE c.id = OLD.conversation_id),
            (SELECT c.ended_at FROM conversations c WHERE c.id = OLD.conversation_id),
            (SELECT c.started_at FROM conversations c WHERE c.id = OLD.conversation_id)
        ) IS NOT NULL,
        activity_ms = COALESCE(
            (SELECT c.last_message_created_at FROM conversations c WHERE c.id = OLD.conversation_id),
            (SELECT c.ended_at FROM conversations c WHERE c.id = OLD.conversation_id),
            (SELECT c.started_at FROM conversations c WHERE c.id = OLD.conversation_id)
        )
    WHERE conversation_id = OLD.conversation_id;
END;

-- --- annotation pin mirror -------------------------------------------------
DROP TRIGGER IF EXISTS viewer_sort_annotations_ins;
CREATE TRIGGER viewer_sort_annotations_ins AFTER INSERT ON viewer_annotations BEGIN
    UPDATE viewer_session_sort SET is_pinned = NEW.is_pinned
    WHERE conversation_id = NEW.conversation_id;
END;

DROP TRIGGER IF EXISTS viewer_sort_annotations_upd;
CREATE TRIGGER viewer_sort_annotations_upd AFTER UPDATE ON viewer_annotations BEGIN
    UPDATE viewer_session_sort SET is_pinned = NEW.is_pinned
    WHERE conversation_id = NEW.conversation_id;
END;

DROP TRIGGER IF EXISTS viewer_sort_annotations_del;
CREATE TRIGGER viewer_sort_annotations_del AFTER DELETE ON viewer_annotations BEGIN
    UPDATE viewer_session_sort SET is_pinned = 0
    WHERE conversation_id = OLD.conversation_id;
END;

-- --- facts mirrors (classification + count sort values) -------------------
DROP TRIGGER IF EXISTS viewer_sort_facts_ins;
CREATE TRIGGER viewer_sort_facts_ins AFTER INSERT ON viewer_session_facts BEGIN
    UPDATE viewer_session_sort SET
        is_subagent = NEW.is_subagent,
        task_category = NEW.task_category,
        coverage_state = NEW.coverage_state,
        steps_known = NEW.step_count IS NOT NULL,
        step_count = NEW.step_count,
        artifacts_known = NEW.artifact_count IS NOT NULL,
        artifact_count = NEW.artifact_count
    WHERE conversation_id = NEW.conversation_id;
END;

DROP TRIGGER IF EXISTS viewer_sort_facts_upd;
CREATE TRIGGER viewer_sort_facts_upd AFTER UPDATE ON viewer_session_facts BEGIN
    UPDATE viewer_session_sort SET
        is_subagent = NEW.is_subagent,
        task_category = NEW.task_category,
        coverage_state = NEW.coverage_state,
        steps_known = NEW.step_count IS NOT NULL,
        step_count = NEW.step_count,
        artifacts_known = NEW.artifact_count IS NOT NULL,
        artifact_count = NEW.artifact_count
    WHERE conversation_id = NEW.conversation_id;
END;

DROP TRIGGER IF EXISTS viewer_sort_facts_del;
CREATE TRIGGER viewer_sort_facts_del AFTER DELETE ON viewer_session_facts BEGIN
    UPDATE viewer_session_sort SET
        is_subagent = NULL,
        task_category = NULL,
        coverage_state = NULL,
        steps_known = 0,
        step_count = NULL,
        artifacts_known = 0,
        artifact_count = NULL
    WHERE conversation_id = OLD.conversation_id;
END;

-- --- workspace project mirror ---------------------------------------------
DROP TRIGGER IF EXISTS viewer_sort_workspaces_upd;
CREATE TRIGGER viewer_sort_workspaces_upd AFTER UPDATE ON workspaces BEGIN
    UPDATE viewer_session_sort SET
        project_known = NEW.path IS NOT NULL,
        project_path = NEW.path
    WHERE workspace_id = NEW.id
      AND (OLD.path IS NOT NEW.path OR OLD.display_name IS NOT NEW.display_name);
END;

DROP TRIGGER IF EXISTS viewer_sort_workspaces_del;
CREATE TRIGGER viewer_sort_workspaces_del AFTER DELETE ON workspaces BEGIN
    UPDATE viewer_session_sort SET project_known = 0, project_path = NULL
    WHERE workspace_id = OLD.id;
END;

-- --- tie-key invalidation on dimension edits --------------------------------
DROP TRIGGER IF EXISTS viewer_sort_agents_upd;
CREATE TRIGGER viewer_sort_agents_upd AFTER UPDATE ON agents BEGIN
    UPDATE viewer_session_sort SET tie_key = NULL, tie_error = NULL
    WHERE agent_id = NEW.id
      AND (OLD.slug IS NOT NEW.slug);
END;

DROP TRIGGER IF EXISTS viewer_sort_sources_upd;
CREATE TRIGGER viewer_sort_sources_upd AFTER UPDATE ON sources BEGIN
    UPDATE viewer_session_sort SET tie_key = NULL, tie_error = NULL
    WHERE source_id = OLD.id
      AND (OLD.kind IS NOT NEW.kind
           OR OLD.host_label IS NOT NEW.host_label
           OR OLD.id IS NOT NEW.id);
END;

DROP TRIGGER IF EXISTS viewer_sort_sources_del;
CREATE TRIGGER viewer_sort_sources_del AFTER DELETE ON sources BEGIN
    UPDATE viewer_session_sort SET tie_key = NULL, tie_error = NULL
    WHERE source_id = OLD.id;
END;

INSERT OR REPLACE INTO viewer_meta (key, value)
VALUES ('extension_schema_version', '5');
";

/// Schema v6 — symmetric dimension-delete invalidation.
///
/// `viewer_sort_sources_del` (v5) already returns a sort row's verified tie
/// key to pending when its `sources` row is deleted; the `agents`
/// counterpart was missing, so an `agents` DELETE left persisted keys
/// verified against a gone dimension row trusted forever. Mirrors the
/// sources trigger exactly: the stored key is no longer a verified
/// identity, so the row returns to pending for the maintenance/ingest
/// convergence lanes (`viewer_extensions::sort`).
const VIEWER_SCHEMA_V6: &str = r"
DROP TRIGGER IF EXISTS viewer_sort_agents_del;
CREATE TRIGGER viewer_sort_agents_del AFTER DELETE ON agents BEGIN
    UPDATE viewer_session_sort SET tie_key = NULL, tie_error = NULL
    WHERE agent_id = OLD.id;
END;

INSERT OR REPLACE INTO viewer_meta (key, value)
VALUES ('extension_schema_version', '6');
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
///
/// After the guard, one bounded `idx_viewer_sort_tie_pending` probe checks
/// for unresolved browse tie keys. The schema migrations and the
/// dimension/canonical invalidation triggers can only ever leave `tie_key`
/// NULL — resolution needs canonical-identity recomputation no trigger can
/// express — so the ingest lane converges the pending set itself, in
/// bounded per-chunk transactions (`sort::repair_sort_keys`), instead of
/// stranding an archive-wide `facts_pending` browse refusal on a
/// maintenance command an operator running only `cass index` never sees.
/// Once converged the probe is the only steady-state cost. Must NOT be
/// called with a transaction already open on this connection (the repair
/// commits in its own bounded transactions).
pub fn ensure_profile_for_ingest(connection: &Connection) -> Result<bool> {
    let ensured = match installed_version(connection)? {
        Some(version) if version >= EXTENSION_SCHEMA_VERSION => true,
        _ => {
            ensure_schema(connection)?;
            true
        }
    };
    if ensured && super::sort::sort_ties_pending(connection)? {
        super::sort::repair_sort_keys(connection)
            .context("converging pending browse sort tie keys")?;
    }
    Ok(ensured)
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
        connection.execute_batch(VIEWER_SCHEMA_V5)?;
        connection.execute_batch(VIEWER_SCHEMA_V6)?;
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
