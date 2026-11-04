//! Logical revision helpers over the `viewer_revisions` singleton plus the
//! additive canonical-observer triggers that keep `canonical_revision`
//! honest for EVERY writer of the canonical tables (spec decision 12: one
//! SQLite file is not one atomic Quill commit; metadata edits stay out of
//! `canonical_revision`, lexical readiness stays independent).
//!
//! ## Why triggers, not a write hook
//!
//! SQLite update/commit hooks observe only their own connection; the strict
//! reader, the authorized writer and stock CASS ingest are three different
//! connections across processes. AFTER triggers are schema objects — any
//! connection's committed INSERT/UPDATE/DELETE on an observed table fires
//! them inside that same transaction, so an external commit cannot produce
//! a canonical change the revision cannot see. `update_hook` would be
//! blind to everything but the one connection that registered it.
//!
//! ## Change journal (Task 11)
//!
//! The same observer bodies also append one `viewer_change_log` row per
//! firing, AFTER the revision bump, stamped with the post-bump
//! `canonical_revision` — the canonical event is recorded by the same
//! commit that mutates canonical rows, so a journal event is never
//! observed ahead of (or without) the change it announces, and a writer
//! that crashes after commit but before notify left a fully replayable
//! journal. `viewer_annotations`/`viewer_tags` carry their own
//! journal-only triggers (`annotations_updated` stamped with the per-row
//! annotation CAS revision); the `annotation_revision` bump itself stays
//! an explicit `revisions::bump` in `patch_annotations`' transaction.
//! Post-commit coalescing and retention are the notify pass in
//! `viewer_extensions::changes`.
//!
//! ## False-positive suppression
//!
//! INSERT/DELETE observers bump unconditionally (a row that appears or
//! disappears is real content change). UPDATE observers carry their
//! `OLD.x IS NOT NEW.x` guard in the bump's WHERE clause rather than the
//! trigger WHEN clause: both forms bind OLD/NEW on the pinned engine, but
//! the body-WHERE form keeps working even if WHEN evaluation ever
//! regressed. The journal append repeats the same guard as the SELECT's
//! WHERE, so a same-values rewrite — e.g. `UPDATE messages SET
//! content=content` or `INSERT OR REPLACE` replays on tail state — is
//! neither a false content update nor a false journal event.
//!
//! `conversation_tail_state` intentionally carries ONLY the guarded UPDATE
//! trigger: CASS ingest writes tail rows via `INSERT OR REPLACE`, which
//! fires DELETE+INSERT trigger pairs and would false-positive on identical
//! replayed values. Genuine tail movement always accompanies a canonical
//! `conversations`/`messages` write, so those tables' observers cover it.
//!
//! Qualified against pinned FrankenSQLite 0.4.12 in
//! `tests/viewer_bridge_extensions.rs::revisions_engine_trigger_semantics_probe`
//! and `tests/viewer_change_stream.rs::journal_engine_insert_select_guard_probe`.

use anyhow::Result;

use coding_agent_search::franken_sync::Connection;
use coding_agent_search::franken_sync::compat::RowExt;

use super::protocol::Generation;

// The observed canonical surface is the served v1 projection:
// `conversations`, `messages` and `conversation_tail_state`, plus the
// dimension tables (`agents`, `workspaces`, `sources`) whose rows the
// served projection joins — a rename/delete there changes served data
// and must invalidate live clients. Dimension observers journal a coarse
// `session_updated` with `conversation_id = NULL` (the served row is not
// addressable by one conversation). Non-served tables (`snippets`,
// `*_tail_lookup`, analytics) stay out of v1 scope; dimension INSERTs are
// likewise uncovered — an unreferenced row serves nothing, and the
// conversation INSERT that later references it already journals
// `session_created`.

const CONVERSATION_GUARD: &str = "\
    OLD.agent_id IS NOT NEW.agent_id OR OLD.workspace_id IS NOT NEW.workspace_id OR \
    OLD.source_id IS NOT NEW.source_id OR OLD.external_id IS NOT NEW.external_id OR \
    OLD.title IS NOT NEW.title OR OLD.source_path IS NOT NEW.source_path OR \
    OLD.started_at IS NOT NEW.started_at OR OLD.ended_at IS NOT NEW.ended_at OR \
    OLD.approx_tokens IS NOT NEW.approx_tokens OR \
    OLD.metadata_json IS NOT NEW.metadata_json OR \
    OLD.origin_host IS NOT NEW.origin_host OR OLD.metadata_bin IS NOT NEW.metadata_bin OR \
    OLD.total_input_tokens IS NOT NEW.total_input_tokens OR \
    OLD.total_output_tokens IS NOT NEW.total_output_tokens OR \
    OLD.total_cache_read_tokens IS NOT NEW.total_cache_read_tokens OR \
    OLD.total_cache_creation_tokens IS NOT NEW.total_cache_creation_tokens OR \
    OLD.grand_total_tokens IS NOT NEW.grand_total_tokens OR \
    OLD.estimated_cost_usd IS NOT NEW.estimated_cost_usd OR \
    OLD.primary_model IS NOT NEW.primary_model OR \
    OLD.api_call_count IS NOT NEW.api_call_count OR \
    OLD.tool_call_count IS NOT NEW.tool_call_count OR \
    OLD.user_message_count IS NOT NEW.user_message_count OR \
    OLD.assistant_message_count IS NOT NEW.assistant_message_count OR \
    OLD.last_message_idx IS NOT NEW.last_message_idx OR \
    OLD.last_message_created_at IS NOT NEW.last_message_created_at";

const MESSAGE_GUARD: &str = "\
    OLD.conversation_id IS NOT NEW.conversation_id OR OLD.idx IS NOT NEW.idx OR \
    OLD.role IS NOT NEW.role OR OLD.author IS NOT NEW.author OR \
    OLD.created_at IS NOT NEW.created_at OR OLD.content IS NOT NEW.content OR \
    OLD.extra_json IS NOT NEW.extra_json OR OLD.extra_bin IS NOT NEW.extra_bin";

const TAIL_GUARD: &str = "\
    OLD.ended_at IS NOT NEW.ended_at OR \
    OLD.last_message_idx IS NOT NEW.last_message_idx OR \
    OLD.last_message_created_at IS NOT NEW.last_message_created_at";

const ANNOTATION_GUARD: &str = "\
    OLD.is_pinned IS NOT NEW.is_pinned OR OLD.custom_title IS NOT NEW.custom_title OR \
    OLD.notes IS NOT NEW.notes OR OLD.revision IS NOT NEW.revision";

/// Dimension guards: the served/identity columns only, so a same-values
/// upsert (`ensure_agent`/`ensure_workspace`/`ensure_source` replay) is
/// neither a false canonical bump nor a false journal row. `updated_at`
/// is bookkeeping, never a served change.
const AGENT_GUARD: &str = "\
    OLD.slug IS NOT NEW.slug OR OLD.name IS NOT NEW.name OR \
    OLD.version IS NOT NEW.version OR OLD.kind IS NOT NEW.kind";

const WORKSPACE_GUARD: &str = "\
    OLD.path IS NOT NEW.path OR OLD.display_name IS NOT NEW.display_name";

const SOURCE_GUARD: &str = "\
    OLD.id IS NOT NEW.id OR OLD.kind IS NOT NEW.kind OR \
    OLD.host_label IS NOT NEW.host_label OR OLD.machine_id IS NOT NEW.machine_id OR \
    OLD.platform IS NOT NEW.platform OR OLD.config_json IS NOT NEW.config_json";

/// Journal `revision` stamped on canonical events: the post-bump
/// `canonical_revision` (the bump runs first in the same trigger body).
/// `COALESCE` keeps a missing singleton row from aborting the writer's
/// statement — a journal event with revision 0 is still replayable.
const CANONICAL_STAMP: &str =
    "COALESCE((SELECT canonical_revision FROM viewer_revisions WHERE id = 1), 0)";

/// Journal `revision` stamped on `viewer_tags` events: the owning
/// annotation row's CAS revision at fire time (a cascade delete reads 0 —
/// the `session_deleted` event carries the truth then).
const TAG_STAMP: &str =
    "COALESCE((SELECT revision FROM viewer_annotations WHERE conversation_id = {cid}), 0)";

/// One `viewer_change_log` append, optionally guarded by a body-WHERE (the
/// same `OLD.x IS NOT NEW.x` form the revision bump uses — an INSERT..
/// SELECT..WHERE suppresses same-values rewrites identically).
fn journal_append(kind: &str, cid: &str, rev: &str, guard: Option<&str>) -> String {
    let where_clause = guard.map_or_else(String::new, |g| format!(" WHERE ({g})"));
    format!(
        "INSERT INTO viewer_change_log (kind, conversation_id, revision, created_at_ms) \
         SELECT '{kind}', {cid}, {rev}, strftime('%s','now')*1000{where_clause};"
    )
}

/// Canonical observer: revision bump THEN journal append — the event is
/// stamped post-bump inside the same firing, and both ride the writer's
/// commit. `guard` sits in the bump's AND-clause and the append's WHERE.
fn canonical_trigger(
    name: &str,
    timing: &str,
    table: &str,
    kind: &str,
    cid: &str,
    guard: Option<&str>,
) -> String {
    let bump_where = guard.map_or_else(String::new, |g| format!(" AND ({g})"));
    format!(
        "DROP TRIGGER IF EXISTS {name}; \
         CREATE TRIGGER {name} {timing} ON {table} BEGIN \
             UPDATE viewer_revisions SET canonical_revision = canonical_revision + 1 \
             WHERE id = 1{bump_where}; \
             {append} \
         END;",
        append = journal_append(kind, cid, CANONICAL_STAMP, guard),
    )
}

/// Extension-table journal trigger: appends `annotations_updated` and
/// never bumps `annotation_revision` — the per-patch bump stays an
/// explicit `revisions::bump` inside `patch_annotations`' transaction so
/// one patch counts exactly once.
fn annotation_trigger(
    name: &str,
    timing: &str,
    table: &str,
    cid: &str,
    rev: &str,
    guard: Option<&str>,
) -> String {
    format!(
        "DROP TRIGGER IF EXISTS {name}; \
         CREATE TRIGGER {name} {timing} ON {table} BEGIN \
             {append} \
         END;",
        append = journal_append("annotations_updated", cid, rev, guard),
    )
}

/// Every observer object the profile installs; presence of ALL names is the
/// `canonical_observers` capability signal.
pub const OBSERVER_TRIGGERS: &[&str] = &[
    "viewer_rev_conversations_ins",
    "viewer_rev_conversations_del",
    "viewer_rev_conversations_upd",
    "viewer_rev_messages_ins",
    "viewer_rev_messages_del",
    "viewer_rev_messages_upd",
    "viewer_rev_tail_upd",
    "viewer_rev_annotations_ins",
    "viewer_rev_annotations_del",
    "viewer_rev_annotations_upd",
    "viewer_rev_tags_ins",
    "viewer_rev_tags_del",
    "viewer_rev_agents_upd",
    "viewer_rev_agents_del",
    "viewer_rev_workspaces_upd",
    "viewer_rev_workspaces_del",
    "viewer_rev_sources_upd",
    "viewer_rev_sources_del",
];

fn observer_ddl() -> String {
    let mut ddl = String::new();
    // conversations: create/update/delete are the session lifecycle.
    ddl.push_str(&canonical_trigger(
        "viewer_rev_conversations_ins",
        "AFTER INSERT",
        "conversations",
        "session_created",
        "NEW.id",
        None,
    ));
    ddl.push_str(&canonical_trigger(
        "viewer_rev_conversations_del",
        "AFTER DELETE",
        "conversations",
        "session_deleted",
        "OLD.id",
        None,
    ));
    ddl.push_str(&canonical_trigger(
        "viewer_rev_conversations_upd",
        "AFTER UPDATE",
        "conversations",
        "session_updated",
        "NEW.id",
        Some(CONVERSATION_GUARD),
    ));
    // messages: any real change is a session content update, keyed by the
    // owning conversation.
    ddl.push_str(&canonical_trigger(
        "viewer_rev_messages_ins",
        "AFTER INSERT",
        "messages",
        "session_updated",
        "NEW.conversation_id",
        None,
    ));
    ddl.push_str(&canonical_trigger(
        "viewer_rev_messages_del",
        "AFTER DELETE",
        "messages",
        "session_updated",
        "OLD.conversation_id",
        None,
    ));
    ddl.push_str(&canonical_trigger(
        "viewer_rev_messages_upd",
        "AFTER UPDATE",
        "messages",
        "session_updated",
        "NEW.conversation_id",
        Some(MESSAGE_GUARD),
    ));
    ddl.push_str(&canonical_trigger(
        "viewer_rev_tail_upd",
        "AFTER UPDATE",
        "conversation_tail_state",
        "session_updated",
        "NEW.conversation_id",
        Some(TAIL_GUARD),
    ));
    // Extension tables: journal-only triggers — a tag-set replacement or
    // annotation upsert from ANY writer's transaction is observed.
    ddl.push_str(&annotation_trigger(
        "viewer_rev_annotations_ins",
        "AFTER INSERT",
        "viewer_annotations",
        "NEW.conversation_id",
        "NEW.revision",
        None,
    ));
    ddl.push_str(&annotation_trigger(
        "viewer_rev_annotations_del",
        "AFTER DELETE",
        "viewer_annotations",
        "OLD.conversation_id",
        "OLD.revision",
        None,
    ));
    ddl.push_str(&annotation_trigger(
        "viewer_rev_annotations_upd",
        "AFTER UPDATE",
        "viewer_annotations",
        "NEW.conversation_id",
        "NEW.revision",
        Some(ANNOTATION_GUARD),
    ));
    ddl.push_str(&annotation_trigger(
        "viewer_rev_tags_ins",
        "AFTER INSERT",
        "viewer_tags",
        "NEW.conversation_id",
        &TAG_STAMP.replace("{cid}", "NEW.conversation_id"),
        None,
    ));
    ddl.push_str(&annotation_trigger(
        "viewer_rev_tags_del",
        "AFTER DELETE",
        "viewer_tags",
        "OLD.conversation_id",
        &TAG_STAMP.replace("{cid}", "OLD.conversation_id"),
        None,
    ));
    // Dimension tables: a rename or delete changes served rows but is
    // not addressable by one conversation — journal a coarse
    // `session_updated` with `conversation_id = NULL`. INSERTs stay
    // unobserved by design (see the scope note at the top of this file).
    for (name, timing, table, guard) in [
        (
            "viewer_rev_agents_upd",
            "AFTER UPDATE",
            "agents",
            Some(AGENT_GUARD),
        ),
        ("viewer_rev_agents_del", "AFTER DELETE", "agents", None),
        (
            "viewer_rev_workspaces_upd",
            "AFTER UPDATE",
            "workspaces",
            Some(WORKSPACE_GUARD),
        ),
        (
            "viewer_rev_workspaces_del",
            "AFTER DELETE",
            "workspaces",
            None,
        ),
        (
            "viewer_rev_sources_upd",
            "AFTER UPDATE",
            "sources",
            Some(SOURCE_GUARD),
        ),
        ("viewer_rev_sources_del", "AFTER DELETE", "sources", None),
    ] {
        ddl.push_str(&canonical_trigger(
            name,
            timing,
            table,
            "session_updated",
            "NULL",
            guard,
        ));
    }
    ddl
}

/// `true` when the extension profile is installed in this archive.
pub fn extensions_installed(connection: &Connection) -> Result<bool> {
    let rows = connection.query(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='viewer_revisions' LIMIT 1",
    )?;
    Ok(!rows.is_empty())
}

/// `true` when every canonical observer trigger is present. A partial set
/// reports `false` so consumers never trust a silently missing observer.
pub fn observers_installed(connection: &Connection) -> Result<bool> {
    let mut present = 0_usize;
    for row in connection
        .query("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'viewer_rev_%'")?
    {
        let name: String = row.get_typed(0)?;
        if OBSERVER_TRIGGERS.contains(&name.as_str()) {
            present += 1;
        }
    }
    Ok(present == OBSERVER_TRIGGERS.len())
}

/// Install the canonical and extension-table observers. Idempotent and
/// self-upgrading: each trigger is dropped and recreated so archives whose
/// observers predate the journal-append bodies pick them up on the next
/// authorized run (`observers_installed` reports a partial set as `false`,
/// so a mid-DDL failure can never masquerade as a complete install).
/// Requires the extension profile to already exist — caller orders
/// `ensure_schema` first. Runs as one batch so one engine refusal reports
/// the exact object.
pub fn ensure_observers(connection: &Connection) -> Result<()> {
    anyhow::ensure!(
        extensions_installed(connection)?,
        "canonical observers require the viewer extension profile \
         (run --init-extensions, which applies ensure_schema first)"
    );
    // The singleton row is the triggers' bump target; tolerate archives
    // whose row was never seeded or was removed.
    connection.execute("INSERT OR IGNORE INTO viewer_revisions (id) VALUES (1)")?;
    connection.execute_batch(&observer_ddl())?;
    Ok(())
}

/// The committed logical generation, or `None` on unextended archives.
pub fn generation(connection: &Connection) -> Result<Option<Generation>> {
    if !extensions_installed(connection)? {
        return Ok(None);
    }
    let rows = connection.query(
        "SELECT canonical_revision,annotation_revision,projection_revision,lexical_generation \
         FROM viewer_revisions WHERE id = 1 LIMIT 2",
    )?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    Ok(Some(Generation {
        canonical_revision: row.get_typed(0)?,
        annotation_revision: row.get_typed(1)?,
        projection_revision: row.get_typed(2)?,
        lexical_generation: row.get_typed(3)?,
    }))
}

/// Named logical counters a writer may bump. Each bump commits in the SAME
/// transaction as its payload mutation so a revision is never observed
/// ahead of the change it announces. Exported for the projection/lexical
/// lanes that land in later tasks; only `Annotation` has a writer today.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)] // Canonical/Projection/Lexical are the shared bump API.
pub enum Counter {
    Canonical,
    Annotation,
    Projection,
    Lexical,
}

impl Counter {
    fn column(self) -> &'static str {
        match self {
            Self::Canonical => "canonical_revision",
            Self::Annotation => "annotation_revision",
            Self::Projection => "projection_revision",
            Self::Lexical => "lexical_generation",
        }
    }
}

/// Bump one logical counter inside the caller's open transaction and return
/// the new committed value. `now_ms` stamps `updated_at_ms`.
pub fn bump(connection: &Connection, counter: Counter, now_ms: i64) -> Result<i64> {
    connection.execute_with_params(
        &format!(
            "UPDATE viewer_revisions SET {column} = {column} + 1, updated_at_ms = ?1 \
             WHERE id = 1",
            column = counter.column()
        ),
        &[coding_agent_search::franken_sync::SqliteValue::Integer(
            now_ms,
        )],
    )?;
    let value: i64 = connection
        .query_row(&format!(
            "SELECT {column} FROM viewer_revisions WHERE id = 1",
            column = counter.column()
        ))?
        .get_typed(0)?;
    Ok(value)
}
