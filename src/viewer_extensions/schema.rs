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

use crate::franken_sync::compat::RowExt;
use crate::franken_sync::{Connection, FrankenError};

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
/// v7: `viewer_search_regions` + `viewer_search_region_marks` — the Task-5
/// persisted search-region inventory (stable region keys, scope/knownness,
/// canonical message + structural anchors, predicate fields, bounded
/// `indexed_text` fragments for derived/meta lanes only) plus the
/// per-session publication mark and stale-document reconcile queue that
/// keeps `complete` ingest receipts honest about index coverage.
/// v8: `viewer_lexical_publication` — the durable singleton
/// `PublicationReceipt` the Task-8 certification lane writes inside the
/// same transaction that bumps `lexical_generation` and journals
/// `index_ready`, so the announced generation always names the committed
/// Quill manifest it certifies.
/// v9: `viewer_session_facts.parent_lookup_key` + its index — the
/// normalized form of `parent_external_id` (ASCII-lowercased, with
/// wildcard-bearing evidence collapsed onto a constant sentinel) that
/// lets the child-before-parent fixup probe candidates by equality
/// instead of an archive-wide `LIKE` scan per published conversation.
/// Replaces the v3 `idx_viewer_session_facts_parent_ext` index, which no
/// query could ever seek (all its readers match suffix `LIKE` patterns).
pub const EXTENSION_SCHEMA_VERSION: i64 = 9;

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
    -- Schema v9: normalized parent-evidence lookup key (ASCII-lowercased
    -- evidence; evidence bearing a LIKE wildcard gets the constant
    -- sentinel) so the fixup can probe candidates by equality through
    -- idx_viewer_session_facts_parent_lookup instead of a per-conversation
    -- LIKE scan. Pre-v9 archives gain this column through the guarded
    -- ALTER in migrate_facts_parent_lookup_key.
    parent_lookup_key TEXT,
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

/// Schema v7 — the Task-5 persisted search-region inventory.
///
/// `viewer_search_regions` holds one row per DSL-addressable region: the
/// stable `region_key`, scope + knownness, canonical message coordinates
/// (`message_idx`/`message_id`), structural anchors (`step_index`/
/// `turn_index`), the predicate fields (`tool_name`/`file_path`/
/// `exit_code`/`has_error`/`error_type`), the hydration lane
/// (`text_source`: `chat` re-reads the canonical `messages.content` via
/// `message_id`; `derived`/`meta` read the bounded `indexed_text`
/// fragment persisted here — the ONLY added text storage, justified
/// because re-deriving it would re-open the corpus scan this task
/// removes) and the generation's `source_revision`. `quill_idx` is the
/// region document's `msg_idx` slot: the canonical `messages.idx` for
/// chat regions (mapping straight onto the message document), or
/// `2^40 + region_ordinal` — a synthetic slot `decode_hit` accepts but no
/// real message index ever occupies — for every other region so one
/// Quill hit maps back to exactly one region key.
///
/// `viewer_search_region_marks` is the per-session coherency marker the
/// `complete` ingest receipt binds: `(region_version, source_revision,
/// region_count)`, the index-lane `docs_state` (`pending` until the
/// region documents commit; `meta_pending` after targeted meta
/// replacement) and `pending_doc_deletes` — the JSON list of stale
/// document identities the Quill reconcile lane removes before
/// re-stamping `published`.
const VIEWER_SCHEMA_V7: &str = r"
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
-- Quill hit → region lookup and keyed refresh both drive from this.
CREATE UNIQUE INDEX IF NOT EXISTS idx_viewer_search_regions_quill
    ON viewer_search_regions(conversation_id, quill_idx);
CREATE UNIQUE INDEX IF NOT EXISTS idx_viewer_search_regions_key
    ON viewer_search_regions(conversation_id, region_key);
-- Block-domain predicates seek by scope/tool/file inside a candidate set.
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

INSERT OR REPLACE INTO viewer_meta (key, value)
VALUES ('extension_schema_version', '7');
";

/// Schema v8 — durable lexical publication receipt (Task 8).
///
/// `viewer_lexical_publication` is a singleton row (`id` is always 1 by
/// writer convention) holding the versioned `PublicationReceipt` of the
/// most recent certified Quill publication: the committed MANIFEST
/// `quill_generation`, its `live_doc_count`, and the covered
/// canonical/annotation/projection revisions plus the `lexical_generation`
/// ordinal minted for it. The row, the `viewer_revisions` bump and the
/// `index_ready` journal event commit atomically in
/// `changes::certify_lexical_publication`, so a crash can never leave a
/// generation the journal cannot explain or a journal claim no receipt
/// backs. Only the newest certification matters — the journal replay
/// coalesces `index_ready` the same way.
const VIEWER_SCHEMA_V8: &str = r"
CREATE TABLE IF NOT EXISTS viewer_lexical_publication (
    id INTEGER PRIMARY KEY,
    v INTEGER NOT NULL,
    quill_generation INTEGER NOT NULL,
    live_doc_count INTEGER NOT NULL,
    canonical_revision INTEGER NOT NULL,
    annotation_revision INTEGER NOT NULL,
    projection_revision INTEGER NOT NULL,
    lexical_generation INTEGER NOT NULL,
    certified_at_ms INTEGER NOT NULL,
    payload_json TEXT NOT NULL
);

INSERT OR REPLACE INTO viewer_meta (key, value)
VALUES ('extension_schema_version', '8');
";

/// Schema v9 — the sargable parent-evidence lookup.
///
/// `viewer_session_facts.parent_lookup_key` is the normalized form of
/// `parent_external_id` that [`super::facts::fixup_pending_parent_links`]
/// probes with pure equality (nothing but the index term in the WHERE, so
/// the engine seeks `idx_viewer_session_facts_parent_lookup` instead of
/// scanning the archive once per published conversation). The column
/// itself lands via a guarded `ALTER TABLE` backfilled in the same schema
/// transaction (see [`migrate_facts_parent_lookup_key`]); this batch
/// carries the index, the retirement of the v3
/// `idx_viewer_session_facts_parent_ext` (every reader it was built for
/// matches suffix `LIKE` patterns no B-tree index can serve, so after v9
/// nothing can ever seek it) and the version marker.
const VIEWER_SCHEMA_V9: &str = r"
DROP INDEX IF EXISTS idx_viewer_session_facts_parent_ext;
CREATE INDEX IF NOT EXISTS idx_viewer_session_facts_parent_lookup
    ON viewer_session_facts(parent_lookup_key);

INSERT OR REPLACE INTO viewer_meta (key, value)
VALUES ('extension_schema_version', '9');
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

/// Guarded v9 column migration: `parent_lookup_key` cannot ride the
/// `CREATE TABLE` statement (existing v8 archives keep their table), so it
/// lands as an `ALTER TABLE` only when the column is absent — inside the
/// same schema transaction as the rest of the profile.
///
/// The backfill mirrors [`super::facts::parent_lookup_key`] exactly:
/// evidence bearing a `LIKE` wildcard (`%`/`_`) collapses onto the constant
/// sentinel `char(1)` (such evidence is not enumerable from a parent id's
/// suffixes, so the fixup fetches those rows through one sentinel probe and
/// lets the engine's own `LIKE` decide), everything else is
/// ASCII-lowercased (`lower` here and `to_ascii_lowercase` in Rust fold
/// exactly the ASCII range). NULL evidence keeps its NULL key (an equality
/// probe never fetches it); empty evidence keeps its empty key (no probe
/// key is ever empty, and the fixup's candidacy filter excludes it), so
/// rows that were never claimable stay exactly as unclaimable as before.
fn migrate_facts_parent_lookup_key(connection: &Connection) -> Result<()> {
    let present = connection
        .query("PRAGMA table_info(viewer_session_facts)")?
        .iter()
        .any(|row| {
            row.get_typed::<String>(1)
                .map(|name| name == "parent_lookup_key")
                .unwrap_or(false)
        });
    if !present {
        connection
            .execute(
                "ALTER TABLE viewer_session_facts \
                 ADD COLUMN parent_lookup_key TEXT",
            )
            .context("adding viewer_session_facts.parent_lookup_key (schema v9)")?;
    }
    // Pre-v9 rows keep their evidence durably in parent_external_id;
    // without this backfill the equality lookup would never probe them
    // and the fixup would silently miss children recorded before the
    // upgrade. The `parent_lookup_key IS NULL` guard keeps the backfill
    // idempotent and heals any straggler a crashed publish left behind.
    connection
        .execute(
            "UPDATE viewer_session_facts \
             SET parent_lookup_key = CASE \
                     WHEN parent_external_id LIKE '%\\%%' ESCAPE '\\' \
                          OR parent_external_id LIKE '%\\_%' ESCAPE '\\' \
                     THEN char(1) \
                     ELSE lower(parent_external_id) \
                 END \
             WHERE parent_lookup_key IS NULL \
               AND parent_external_id IS NOT NULL",
        )
        .context("backfilling viewer_session_facts.parent_lookup_key (schema v9)")?;
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
        connection.execute_batch(VIEWER_SCHEMA_V7)?;
        connection.execute_batch(VIEWER_SCHEMA_V8)?;
        migrate_facts_parent_lookup_key(connection)?;
        connection.execute_batch(VIEWER_SCHEMA_V9)?;
        Ok(())
    })();
    // A failed COMMIT can leave this function's own transaction ACTIVE in
    // the engine (concurrent-mode snapshot conflict, deferred-FK recheck),
    // and a body error can leave it open too, so every post-BEGIN failure
    // rolls back while the transaction is still open. Only this function's
    // transaction can be open here: a pre-existing caller transaction
    // fails BEGIN above, and a failed BEGIN returns without cleanup. The
    // initiating error stays the cause; a rollback failure is reported
    // with it, never swallowed — a possibly-open transaction must not be
    // claimed clean.
    let cleanup = |error: anyhow::Error| -> anyhow::Error {
        match rollback_owned_transaction(connection) {
            Ok(()) => error,
            Err(rollback) => error.context(format!(
                "rolling back the failed schema transaction also failed ({rollback}); \
                 the connection may still hold it open"
            )),
        }
    };
    match applied {
        Ok(()) => match connection
            .execute("COMMIT")
            .context("committing viewer extension schema")
        {
            Ok(_) => Ok(()),
            Err(error) => Err(cleanup(error)),
        },
        Err(error) => Err(cleanup(error).context("applying viewer extension schema DDL")),
    }
}

/// Roll back the transaction an owned `BEGIN` site began, when it is still
/// open. `Ok(())` certifies the connection is back in autocommit — either
/// the engine already closed the transaction (some COMMIT-time failures
/// unwind it themselves) or the rollback succeeded. `Err` means the
/// rollback itself failed; callers must surface that alongside the
/// initiating error instead of claiming a clean connection.
///
/// Only a transaction the call site itself began is ever rolled back: a
/// pre-existing caller transaction fails `BEGIN` before this runs.
pub(crate) fn rollback_owned_transaction(
    connection: &Connection,
) -> std::result::Result<(), FrankenError> {
    if !connection.as_async().in_transaction() {
        return Ok(());
    }
    connection.execute("ROLLBACK").map(|_| ())
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

#[cfg(test)]
mod tests {
    //! Transaction-ownership regression lane for [`ensure_schema`]: real
    //! COMMIT failures injected through the engine's deferred-FK recheck,
    //! plus caller-transaction preservation. The wider profile surface is
    //! exercised by `tests/viewer_bridge_extensions.rs`.

    use super::*;

    use crate::storage::sqlite::FrankenStorage;

    fn fixture() -> (tempfile::TempDir, std::path::PathBuf, FrankenStorage) {
        let temp = tempfile::tempdir().expect("tempdir");
        let db = temp.path().join("archive.db");
        let storage = FrankenStorage::open(&db).expect("canonical schema");
        (temp, db, storage)
    }

    /// Real COMMIT-failure injection: pre-create `viewer_meta` with the
    /// shape the profile declares — `CREATE TABLE IF NOT EXISTS` then
    /// keeps it, trigger included — and arm an `AFTER INSERT` trigger
    /// that plants a `DEFERRABLE INITIALLY DEFERRED` foreign-key
    /// violation inside the install transaction. The engine defers the
    /// check to COMMIT, where the surviving violation fails the COMMIT
    /// but — matching SQLite — leaves the transaction ACTIVE: exactly
    /// the ownership hole this test guards.
    fn arm_commit_failure_on_meta_writes(connection: &Connection) {
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS viewer_meta (
                     key TEXT PRIMARY KEY,
                     value TEXT NOT NULL
                 );
                 CREATE TABLE commit_poison (
                     id INTEGER PRIMARY KEY,
                     parent INTEGER REFERENCES conversations(id)
                         DEFERRABLE INITIALLY DEFERRED
                 );
                 CREATE TRIGGER poison_meta_append
                 AFTER INSERT ON viewer_meta BEGIN
                     INSERT INTO commit_poison(parent) VALUES (999999);
                 END;",
            )
            .expect("commit-failure injection trigger");
    }

    /// A COMMIT that fails inside `ensure_schema` must roll back the
    /// transaction it began: the initiating error stays the cause,
    /// `in_transaction()` proves the connection clean, and a second
    /// install on the same connection still succeeds.
    #[test]
    fn ensure_schema_commit_failure_rolls_back_owned_transaction() -> Result<()> {
        let (_temp, _db, storage) = fixture();
        let connection = storage.raw();
        arm_commit_failure_on_meta_writes(connection);

        let error = ensure_schema(connection)
            .expect_err("a schema COMMIT the deferred-FK recheck fails must surface");
        assert!(
            format!("{error:#}").contains("committing viewer extension schema"),
            "the initiating COMMIT failure must stay the cause: {error:#}"
        );
        assert!(
            !connection.as_async().in_transaction(),
            "the failed install must not leave its own transaction open"
        );

        // The same connection stays usable, and the profile still
        // installs once the injection is gone.
        connection.execute_batch("DROP TRIGGER poison_meta_append; DROP TABLE commit_poison;")?;
        ensure_schema(connection)?;
        assert_eq!(
            installed_version(connection)?,
            Some(EXTENSION_SCHEMA_VERSION),
            "the same connection must still install the extension profile"
        );
        Ok(())
    }

    /// An `ensure_schema` invoked while the CALLER already holds a
    /// transaction fails without touching it — the ownership rule only
    /// ever rolls back what the function itself began.
    #[test]
    fn ensure_schema_preserves_caller_owned_transaction() -> Result<()> {
        let (_temp, _db, storage) = fixture();
        let connection = storage.raw();
        connection.execute("BEGIN")?;
        let error =
            ensure_schema(connection).expect_err("install inside a caller transaction must fail");
        assert!(
            format!("{error:#}").contains("viewer extension schema"),
            "the refused install must still name the schema it was installing: {error:#}"
        );
        assert!(
            connection.as_async().in_transaction(),
            "a failed install must not roll back the caller's transaction"
        );
        connection.execute("CREATE TABLE caller_probe (id INTEGER)")?;
        connection.execute("COMMIT")?;
        assert_eq!(
            connection
                .query("SELECT 1 FROM sqlite_master WHERE name = 'caller_probe'")?
                .len(),
            1,
            "the caller's transaction must still commit"
        );
        Ok(())
    }
}
