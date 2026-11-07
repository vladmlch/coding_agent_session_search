//! Task 2: additive `viewer_*` extension schema living inside the SAME
//! canonical `agent_search.db`. No sidecar database, no upstream-table
//! mutation, and the strict schema-22 bridge profile must still open.
#[path = "viewer_bridge_support/mod.rs"]
mod support;
// `revisions` (the bin-side canonical-observer installer) is `#[path]`-
// included further down with the other bridge modules; the Task-14 backfill
// tests reuse it to run the exact ensure_schema + ensure_observers pair
// --init-extensions runs.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Result, ensure};
use coding_agent_search::franken_sync::compat::RowExt;
use coding_agent_search::franken_sync::compat::param_slice_to_values;
use coding_agent_search::franken_sync::{Connection, SqliteValue, params};
use coding_agent_search::model::types::{
    Agent, AgentKind, Conversation, Message, MessageRole, Snippet,
};
use coding_agent_search::storage::sqlite::{CURRENT_SCHEMA_VERSION, FrankenStorage};
use coding_agent_search::viewer_extensions::{backfill, facts, schema};
use serde_json::{Value, json};
use support::{BridgeHarness, FixtureConversation, SeedConversation, SeedMessage};

fn db_family_bytes(db: &Path) -> Result<u64> {
    let mut total = 0_u64;
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let path = if suffix.is_empty() {
            db.to_path_buf()
        } else {
            Path::new(&format!("{}{}", db.display(), suffix)).to_path_buf()
        };
        if let Ok(meta) = std::fs::metadata(&path) {
            total += meta.len();
        }
    }
    Ok(total)
}

fn viewer_objects(conn: &Connection) -> Result<BTreeSet<(String, String)>> {
    let mut objects = BTreeSet::new();
    for row in conn.query(
        "SELECT type,name FROM sqlite_master \
         WHERE name LIKE '%viewer%' OR name='sqlite_sequence' ORDER BY type,name",
    )? {
        let kind: String = row.get_typed(0)?;
        let name: String = row.get_typed(1)?;
        objects.insert((kind, name));
    }
    Ok(objects)
}

fn sidecar_dbs(data_dir: &Path) -> Result<Vec<String>> {
    let mut stray = Vec::new();
    for entry in std::fs::read_dir(data_dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        // WAL/SHM/journal companions of the canonical archive are SQLite
        // internals of the SAME physical database, not a second corpus.
        if name.ends_with(".db") && name != "archive.db" {
            stray.push(name);
        }
    }
    Ok(stray)
}

#[test]
fn schema_objects_live_inside_the_same_canonical_db() -> Result<()> {
    let mut bridge = BridgeHarness::with_conversations(&[FixtureConversation::repeated(
        1,
        "local",
        "fixture text",
        2,
    )])?;
    let bytes_before = db_family_bytes(&bridge.db)?;

    // Authorized writer path: initialize the extension profile before the
    // strict read-only archive is ever opened (bridge child is spawned but
    // has not touched the DB yet — the first exchange opens it lazily).
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    schema::ensure_schema(&writer)?;

    let objects = viewer_objects(&writer)?;
    for table in [
        "viewer_annotations",
        "viewer_tags",
        "viewer_session_facts",
        "viewer_revisions",
    ] {
        ensure!(
            objects.contains(&("table".to_owned(), table.to_owned())),
            "missing extension table {table}; found {objects:?}"
        );
    }
    ensure!(
        sidecar_dbs(&bridge.data_dir)?.is_empty(),
        "extension profile created a second physical database: {:?}",
        sidecar_dbs(&bridge.data_dir)?
    );

    // Idempotent re-initialization: same object set, no error.
    schema::ensure_schema(&writer)?;
    let again = viewer_objects(&writer)?;
    ensure!(
        again == objects,
        "re-init changed the object set: {again:?}"
    );

    // Upstream canonical tables untouched: row counts and schema marker.
    let conv_count: i64 = writer
        .query_row("SELECT COUNT(*) FROM conversations")?
        .get_typed(0)?;
    ensure!(conv_count == 1, "canonical conversations mutated");
    let version: String = writer
        .query_row("SELECT value FROM meta WHERE key='schema_version'")?
        .get_typed(0)?;
    ensure!(
        version == CURRENT_SCHEMA_VERSION.to_string(),
        "canonical schema marker moved: {version}"
    );

    drop(writer);
    let bytes_after = db_family_bytes(&bridge.db)?;
    let delta = bytes_after.saturating_sub(bytes_before);
    eprintln!(
        "viewer_* footprint: before={bytes_before} after={bytes_after} delta={delta} bytes; \
         objects={objects:?}"
    );
    ensure!(
        delta < 1024 * 1024,
        "empty extension schema added {delta} bytes to the canonical archive"
    );

    // The strict pinned-schema profile still opens with the extensions present.
    let hello = bridge.exchange(json!({"op":"hello","id":1}))?;
    ensure!(
        hello["ok"] == true,
        "strict bridge refused extended archive: {hello}"
    );
    ensure!(
        hello["result"]["db_schema_version"] == CURRENT_SCHEMA_VERSION,
        "strict profile drifted"
    );
    bridge.shutdown()
}

#[test]
fn schema_revisions_singleton_and_extension_version() -> Result<()> {
    let mut bridge = BridgeHarness::empty()?;
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    schema::ensure_schema(&writer)?;

    let rows = writer.query(
        "SELECT id,canonical_revision,annotation_revision,projection_revision,lexical_generation \
         FROM viewer_revisions",
    )?;
    ensure!(
        rows.len() == 1,
        "viewer_revisions must hold exactly one row"
    );
    let row = &rows[0];
    ensure!(row.get_typed::<i64>(0)? == 1, "singleton id must be 1");
    for idx in 1..=4 {
        ensure!(
            row.get_typed::<i64>(idx)? == 0,
            "revision counter {idx} must start at 0"
        );
    }

    // Re-init must not duplicate or reset the singleton.
    writer.execute("UPDATE viewer_revisions SET annotation_revision=7 WHERE id=1")?;
    schema::ensure_schema(&writer)?;
    let rows = writer.query("SELECT id,annotation_revision FROM viewer_revisions")?;
    ensure!(rows.len() == 1, "re-init duplicated the singleton row");
    ensure!(
        rows[0].get_typed::<i64>(1)? == 7,
        "re-init must preserve existing revision state"
    );

    let installed = schema::installed_version(&writer)?;
    ensure!(
        installed == Some(schema::EXTENSION_SCHEMA_VERSION),
        "extension version marker mismatch: {installed:?}"
    );
    drop(writer);
    bridge.shutdown()
}

#[test]
fn schema_annotations_tags_facts_roundtrip_nullable_typed() -> Result<()> {
    let mut bridge = BridgeHarness::with_conversations(&[FixtureConversation::repeated(
        1, "local", "fixture", 3,
    )])?;
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    schema::ensure_schema(&writer)?;

    writer.execute_with_params(
        "INSERT INTO viewer_annotations \
         (conversation_id,is_pinned,custom_title,notes,revision,updated_at_ms) \
         VALUES (?1,?2,?3,?4,?5,?6)",
        &param_slice_to_values(params![
            1_i64,
            1_i64,
            "renamed",
            "note body",
            3_i64,
            1_700_000_000_000_i64
        ]),
    )?;
    writer.execute_with_params(
        "INSERT INTO viewer_tags (conversation_id,tag,created_at_ms) VALUES (?1,?2,?3)",
        &param_slice_to_values(params![1_i64, "release-blocker", 1_700_000_000_001_i64]),
    )?;
    // Nullable typed facts: unknown metrics stay NULL (unknown != 0).
    writer.execute_with_params(
        "INSERT INTO viewer_session_facts \
         (conversation_id,extractor_version,source_revision,coverage_state,is_subagent,\
          task_category,turn_count,thinking_block_count) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
        &param_slice_to_values(params![
            1_i64,
            1_i64,
            42_i64,
            "partial",
            0_i64,
            "bugfix",
            7_i64,
            Option::<i64>::None
        ]),
    )?;

    let ann = writer.query_row(
        "SELECT is_pinned,custom_title,notes,revision FROM viewer_annotations \
         WHERE conversation_id=1",
    )?;
    ensure!(ann.get_typed::<i64>(0)? == 1);
    ensure!(ann.get_typed::<Option<String>>(1)?.as_deref() == Some("renamed"));
    ensure!(ann.get_typed::<Option<String>>(2)?.as_deref() == Some("note body"));
    ensure!(ann.get_typed::<i64>(3)? == 3);

    let tags = writer.query("SELECT tag FROM viewer_tags WHERE conversation_id=1")?;
    ensure!(tags.len() == 1);
    ensure!(tags[0].get_typed::<String>(0)? == "release-blocker");

    let facts = writer.query_row(
        "SELECT source_revision,coverage_state,is_subagent,task_category,turn_count,\
         thinking_block_count,artifact_count FROM viewer_session_facts \
         WHERE conversation_id=1",
    )?;
    ensure!(facts.get_typed::<i64>(0)? == 42);
    ensure!(facts.get_typed::<String>(1)? == "partial");
    ensure!(facts.get_typed::<Option<i64>>(2)? == Some(0));
    ensure!(facts.get_typed::<Option<String>>(3)?.as_deref() == Some("bugfix"));
    ensure!(facts.get_typed::<Option<i64>>(4)? == Some(7));
    ensure!(
        facts.get_typed::<Option<i64>>(5)?.is_none(),
        "unknown metric must round-trip as NULL, never 0"
    );
    ensure!(
        facts.get_typed::<Option<i64>>(6)?.is_none(),
        "never-inserted metric must stay NULL"
    );
    drop(writer);
    bridge.shutdown()
}

#[test]
fn schema_engine_capability_probe() -> Result<()> {
    let mut bridge = BridgeHarness::empty()?;
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    schema::ensure_schema(&writer)?;

    // FK declaration must be visible in the catalog even if the engine does
    // not enforce it (enforcement is probed separately below).
    let fk_rows = writer.query("PRAGMA foreign_key_list(viewer_annotations)")?;
    ensure!(
        !fk_rows.is_empty(),
        "viewer_annotations must declare REFERENCES conversations(id)"
    );
    let fk_enabled: Option<i64> = writer
        .query("PRAGMA foreign_keys")
        .ok()
        .and_then(|rows| rows.first().and_then(|row| row.get_typed(0).ok()));
    let enforced = writer
        .execute("INSERT INTO viewer_annotations (conversation_id) VALUES (424242)")
        .is_err();
    eprintln!(
        "engine probe: foreign_keys pragma={fk_enabled:?} \
         orphan-insert-refused={enforced}"
    );

    // Query-plan introspection must be available for later index audits.
    let plan =
        writer.query("EXPLAIN QUERY PLAN SELECT conversation_id FROM viewer_tags WHERE tag='x'")?;
    ensure!(!plan.is_empty(), "EXPLAIN QUERY PLAN must return a plan");

    // JSON support is OPTIONAL: report it for later tasks, do not rely on it.
    let json_probe = writer
        .query("SELECT json_extract('{\"a\":1}','$.a')")
        .ok()
        .and_then(|rows| rows.first().map(|row| row.get_typed::<i64>(0)));
    eprintln!("engine probe: json_extract={json_probe:?}");
    drop(writer);
    bridge.shutdown()
}

#[test]
fn schema_requires_canonical_base_and_rolls_back_cleanly() -> Result<()> {
    // An archive without the canonical schema must refuse instead of
    // creating orphan extension tables.
    let temp = tempfile::tempdir()?;
    let db = temp.path().join("empty.db");
    let writer = Connection::open(db.to_string_lossy().into_owned())?;
    let refused = schema::ensure_schema(&writer);
    ensure!(
        refused.is_err(),
        "ensure_schema must require canonical schema"
    );
    let objects = viewer_objects(&writer)?;
    ensure!(
        objects.is_empty(),
        "refused init must leave no viewer_* objects: {objects:?}"
    );
    drop(writer);
    Ok(())
}

/// Review fix (Important #1): rows published before the v3
/// `parent_external_id` column existed keep their evidence only inside
/// the coverage manifest (`parent.evidence`) — the guarded migration must
/// recover it into the durable column so the child-before-parent fixup
/// can ever see them. Rows without recorded evidence must stay NULL.
#[test]
fn facts_parent_evidence_backfill_for_pre_v3_rows() -> Result<()> {
    let mut bridge = BridgeHarness::with_conversations(&[
        FixtureConversation::repeated(1, "local", "fixture", 2),
        FixtureConversation::repeated(2, "local", "fixture", 2),
    ])?;
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;

    // Simulate a pre-v3 archive honestly: the v2 table shape (no
    // `parent_external_id` column), a v2 version marker, and a facts row
    // whose only parent evidence lives in the coverage manifest.
    writer.execute_batch(
        "CREATE TABLE viewer_session_facts (\
            conversation_id INTEGER PRIMARY KEY \
                REFERENCES conversations(id) ON DELETE CASCADE, \
            extractor_version INTEGER NOT NULL, \
            source_revision INTEGER NOT NULL DEFAULT 0, \
            coverage_state TEXT NOT NULL DEFAULT 'unknown', \
            coverage_detail_json TEXT, \
            is_subagent INTEGER, \
            parent_conversation_id INTEGER REFERENCES conversations(id), \
            task_category TEXT, \
            turn_count INTEGER, \
            step_count INTEGER, \
            thinking_block_count INTEGER, \
            artifact_count INTEGER, \
            subagent_count INTEGER, \
            tool_error_count INTEGER, \
            tool_calls_with_exit_status INTEGER, \
            file_fact_count INTEGER, \
            compaction_count INTEGER, \
            model_set_json TEXT, \
            first_prompt TEXT, \
            extracted_at_ms INTEGER\
        );\
        CREATE TABLE IF NOT EXISTS viewer_meta (\
            key TEXT PRIMARY KEY, \
            value TEXT NOT NULL\
        );\
        INSERT INTO viewer_meta (key, value) \
        VALUES ('extension_schema_version', '2');",
    )?;
    writer.execute_with_params(
        "INSERT INTO viewer_session_facts \
         (conversation_id,extractor_version,source_revision,coverage_state,\
          coverage_detail_json,is_subagent) \
         VALUES (?1,?2,?3,?4,?5,?6)",
        &param_slice_to_values(params![
            1_i64,
            1_i64,
            42_i64,
            "partial",
            r#"{"extractor":"claude-facts-v1","parent":{"evidence":"parent-sess-abc","resolution":"unresolved"}}"#,
            1_i64
        ]),
    )?;
    writer.execute_with_params(
        "INSERT INTO viewer_session_facts \
         (conversation_id,extractor_version,source_revision,coverage_state,\
          coverage_detail_json,is_subagent) \
         VALUES (?1,?2,?3,?4,?5,?6)",
        &param_slice_to_values(params![
            2_i64,
            1_i64,
            43_i64,
            "partial",
            r#"{"extractor":"claude-facts-v1","parent":{"evidence":null,"resolution":"no_evidence"}}"#,
            0_i64
        ]),
    )?;

    // The upgrade path runs the guarded ALTER plus the evidence backfill
    // inside the same schema transaction.
    schema::ensure_schema(&writer)?;

    let row = writer.query_row(
        "SELECT parent_external_id,parent_conversation_id \
         FROM viewer_session_facts WHERE conversation_id=1",
    )?;
    ensure!(
        row.get_typed::<Option<String>>(0)?.as_deref() == Some("parent-sess-abc"),
        "a pre-v3 row must recover durable parent evidence from the manifest"
    );
    ensure!(
        row.get_typed::<Option<i64>>(1)?.is_none(),
        "the migration recovers evidence only — resolution waits for the parent"
    );
    let no_evidence: Option<String> = writer
        .query_row(
            "SELECT parent_external_id FROM viewer_session_facts \
             WHERE conversation_id=2",
        )?
        .get_typed(0)?;
    ensure!(
        no_evidence.is_none(),
        "a row without recorded evidence must stay NULL, never fabricated"
    );
    ensure!(
        schema::installed_version(&writer)? == Some(schema::EXTENSION_SCHEMA_VERSION),
        "the upgrade must land on the current extension profile"
    );
    drop(writer);
    bridge.shutdown()
}

/// Review fix (Minor #4): the fixup keeps the forward rule's
/// self-exclusion — a corrupt row naming its OWN external_id as parent
/// evidence must never self-parent, while ordinary matching children are
/// still claimed.
#[test]
fn facts_fixup_never_self_parents() -> Result<()> {
    let mut bridge = BridgeHarness::with_conversations(&[
        FixtureConversation::repeated(1, "local", "fixture", 2),
        FixtureConversation::repeated(2, "local", "fixture", 2),
    ])?;
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    schema::ensure_schema(&writer)?;

    // Conversation 1's own external_id is "fixture-1" — its facts row
    // corruptly names itself as the parent evidence; conversation 2
    // legitimately names fixture-1 as its parent. The key column is
    // seeded through the real derivation, exactly like the publish path.
    for conversation_id in [1_i64, 2_i64] {
        writer.execute_with_params(
            "INSERT INTO viewer_session_facts \
             (conversation_id,extractor_version,source_revision,coverage_state,\
              coverage_detail_json,parent_external_id,parent_lookup_key) \
             VALUES (?1,?2,?3,?4,?5,?6,?7)",
            &param_slice_to_values(params![
                conversation_id,
                1_i64,
                42_i64,
                "partial",
                r#"{"parent":{"evidence":"fixture-1","resolution":"unresolved"}}"#,
                "fixture-1",
                facts::parent_lookup_key("fixture-1")
            ]),
        )?;
    }

    let claimed = facts::fixup_pending_parent_links(&writer, 1, "codex", "local", "fixture-1")?;
    ensure!(
        claimed == 1,
        "only the real child may be claimed, never the corrupt self-row: {claimed}"
    );
    let self_link: Option<i64> = writer
        .query_row(
            "SELECT parent_conversation_id FROM viewer_session_facts \
             WHERE conversation_id=1",
        )?
        .get_typed(0)?;
    ensure!(
        self_link.is_none(),
        "a row naming its own external_id must never self-parent"
    );
    let child_link: Option<i64> = writer
        .query_row(
            "SELECT parent_conversation_id FROM viewer_session_facts \
             WHERE conversation_id=2",
        )?
        .get_typed(0)?;
    ensure!(
        child_link == Some(1),
        "the legitimate child still resolves to the parent"
    );
    drop(writer);
    bridge.shutdown()
}

/// Review fix (Important #3, engine level): the
/// `viewer_session_facts_parent_cleanup` trigger is load-bearing — pinned
/// here by deleting a resolved parent with the trigger present (clean
/// NULL) and probing the pre-fix failure mode with the trigger dropped
/// (refused or dangling, never clean).
#[test]
fn facts_parent_delete_trigger_repairs_resolved_child_link() -> Result<()> {
    let mut bridge = BridgeHarness::with_conversations(&[
        FixtureConversation::repeated(1, "local", "fixture", 2),
        FixtureConversation::repeated(2, "local", "fixture", 2),
        FixtureConversation::repeated(3, "local", "fixture", 2),
        FixtureConversation::repeated(4, "local", "fixture", 2),
    ])?;
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    schema::ensure_schema(&writer)?;
    // Resolved links: child 2 -> parent 1, child 4 -> parent 3.
    for (child_id, parent_id) in [(2_i64, 1_i64), (4_i64, 3_i64)] {
        writer.execute_with_params(
            "INSERT INTO viewer_session_facts \
             (conversation_id,extractor_version,source_revision,coverage_state,\
              parent_conversation_id) VALUES (?1,?2,?3,?4,?5)",
            &param_slice_to_values(params![child_id, 1_i64, 42_i64, "partial", parent_id]),
        )?;
    }

    // With the v4 trigger installed the parent delete lands inside one
    // statement and the child's link is NULLed — never refused, never
    // dangling.
    writer.execute("DELETE FROM conversations WHERE id=1")?;
    let link: Option<i64> = writer
        .query_row(
            "SELECT parent_conversation_id FROM viewer_session_facts \
             WHERE conversation_id=2",
        )?
        .get_typed(0)?;
    ensure!(
        link.is_none(),
        "the delete trigger must NULL the child's parent link"
    );

    // The pre-fix failure mode the trigger repairs: without it the NO
    // ACTION reference either refuses the delete outright or leaves a
    // dangling reference — never a clean NULL.
    writer.execute("DROP TRIGGER viewer_session_facts_parent_cleanup")?;
    match writer.execute("DELETE FROM conversations WHERE id=3") {
        Err(error) => {
            eprintln!("engine probe: delete-side FK refused without the trigger: {error}")
        }
        Ok(_) => {
            let dangling: Option<i64> = writer
                .query_row(
                    "SELECT parent_conversation_id FROM viewer_session_facts \
                     WHERE conversation_id=4",
                )?
                .get_typed(0)?;
            ensure!(
                dangling == Some(3),
                "without the trigger the link must refuse or dangle, never self-clean"
            );
            eprintln!(
                "engine probe: delete-side FK unenforced; the trigger is the \
                 only cleanup on this build"
            );
        }
    }
    drop(writer);
    bridge.shutdown()
}

// ---------------------------------------------------------------------------
// Parent-link fixup: indexed candidate lookup (differential / cost / v9
// migration). The truth table these tests pin lives in
// .scratch/glm/2026-11-07-parent-links-sargable/report.md §1.
// ---------------------------------------------------------------------------

/// Canonical base only — exactly the upstream tables/columns the viewer
/// profile's DDL and v5 sort triggers reference, with NO viewer profile
/// installed yet. The migration test builds a simulated pre-v9 archive on
/// this base; every other fixup fixture wraps it in `ensure_schema`. The
/// caller owns the db path (a test-scoped tempdir join).
fn fixup_base_db(db_path: &Path) -> Result<Connection> {
    let conn = Connection::open(db_path.to_string_lossy().into_owned())?;
    conn.execute_batch(
        "CREATE TABLE agents (id INTEGER PRIMARY KEY, slug TEXT NOT NULL UNIQUE);
         CREATE TABLE sources (id TEXT PRIMARY KEY, kind TEXT NOT NULL, host_label TEXT);
         CREATE TABLE workspaces (id INTEGER PRIMARY KEY, path TEXT NOT NULL UNIQUE,
             display_name TEXT);
         CREATE TABLE conversations (
             id INTEGER PRIMARY KEY,
             agent_id INTEGER,
             workspace_id INTEGER,
             source_id TEXT,
             external_id TEXT,
             title TEXT,
             source_path TEXT,
             origin_host TEXT,
             started_at INTEGER,
             ended_at INTEGER,
             last_message_created_at INTEGER,
             last_message_idx INTEGER,
             metadata_json TEXT,
             metadata_bin BLOB
         );
         CREATE TABLE conversation_tail_state (
             conversation_id INTEGER PRIMARY KEY,
             last_message_created_at INTEGER,
             ended_at INTEGER
         );
         INSERT INTO agents (id, slug) VALUES (1, 'codex');
         INSERT INTO agents (id, slug) VALUES (2, 'claude_code');
         INSERT INTO sources (id, kind) VALUES ('local', 'local');
         INSERT INTO sources (id, kind) VALUES ('remote', 'remote');",
    )?;
    Ok(conn)
}

/// The fixup-level fixture: canonical base plus the current viewer
/// extension profile (`ensure_schema`), without the bridge child or a full
/// FrankenStorage seed.
fn fixup_fixture_db(db_path: &Path) -> Result<Connection> {
    let conn = fixup_base_db(db_path)?;
    schema::ensure_schema(&conn)?;
    Ok(conn)
}

/// Cost gate for the fixup candidate lookup: with ~50k pending-evidence
/// rows, the indexed lookup must beat the pre-change scan shape by a wide
/// margin. The legacy arm times the verbatim pre-change candidate SELECT
/// (the exact statement facts.rs ran per conversation); the new arm times
/// the real `fixup_pending_parent_links`. Self-relative (both timings in
/// one process) so a loaded machine shifts both arms equally.
#[test]
fn fixup_parent_links_lookup_cost_is_index_served() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let conn = fixup_fixture_db(&temp.path().join("cost.db"))?;

    const ROWS: i64 = 50_000;
    // Pre-v9 archives have no `parent_lookup_key` column; detect it so this
    // test also compiles and runs RED against today's code (where the fixup
    // is the archive-wide scan) instead of dying in fixture setup.
    let has_lookup_key = conn
        .query("PRAGMA table_info(viewer_session_facts)")?
        .iter()
        .any(|row| matches!(row.get_typed::<String>(1), Ok(name) if name == "parent_lookup_key"));
    // Every row carries DISTINCT pending parent evidence (the archive-wide
    // worst case for the old scan); the parent call below matches exactly
    // one child (`evid-7`).
    conn.execute("BEGIN")?;
    // 100k rows seed in this one transaction, so the per-row strings are
    // formatted into preallocated reusable buffers rather than fresh
    // allocations every iteration (`fmt::Write` into a String is
    // infallible, hence the discarded results).
    let mut path = String::with_capacity(32);
    let mut evidence = String::with_capacity(16);
    for i in 0..ROWS {
        path.clear();
        let _ = write!(path, "/cost/dir/evid-{i}.jsonl");
        evidence.clear();
        let _ = write!(evidence, "evid-{i}");
        conn.execute_with_params(
            "INSERT INTO conversations \
             (id, agent_id, source_id, external_id, source_path) \
             VALUES (?1, 1, 'local', ?2, ?3)",
            &param_slice_to_values(params![i, path.as_str(), path.as_str()]),
        )?;
        // `evid-{i}` is lowercase ASCII with no LIKE wildcards, so it is
        // its own lookup key — exactly what facts::parent_lookup_key
        // returns for it (inlined here so this test also compiles before
        // the v9 column/function exist). The pre-v9 branch omits the
        // column entirely.
        if has_lookup_key {
            conn.execute_with_params(
                "INSERT INTO viewer_session_facts \
                 (conversation_id, extractor_version, source_revision, coverage_state, \
                  parent_external_id, parent_lookup_key) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                &param_slice_to_values(params![
                    i,
                    1_i64,
                    42_i64,
                    "partial",
                    evidence.as_str(),
                    evidence.as_str()
                ]),
            )?;
        } else {
            conn.execute_with_params(
                "INSERT INTO viewer_session_facts \
                 (conversation_id, extractor_version, source_revision, coverage_state, \
                  parent_external_id) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                &param_slice_to_values(params![i, 1_i64, 42_i64, "partial", evidence.as_str()]),
            )?;
        }
    }
    conn.execute("COMMIT")?;

    // The facts table's `parent_conversation_id` carries a real FK to
    // conversations(id) and ensure_schema enables FK enforcement, so the
    // parent conversations the fixup links to must exist.
    for (id, external_id, source_path) in [
        (
            900_001_i64,
            "cost-parent-1",
            "/cost/dir/cost-parent-1.jsonl",
        ),
        (
            900_002_i64,
            "cost-parent-2",
            "/cost/dir/cost-parent-2.jsonl",
        ),
    ] {
        conn.execute_with_params(
            "INSERT INTO conversations \
             (id, agent_id, source_id, external_id, source_path) \
             VALUES (?1, 1, 'local', ?2, ?3)",
            &param_slice_to_values(params![id, external_id, source_path]),
        )?;
    }

    let parent_external_id = "/cost/dir/evid-7.jsonl";
    // The fixup must still find the one real child (correctness gate for
    // the timed path — a vacuous probe-fest would pass the timing alone).
    let claimed =
        facts::fixup_pending_parent_links(&conn, 900_001, "codex", "local", parent_external_id)?;
    ensure!(
        claimed == 1,
        "the fixup must link its one matching child: {claimed}"
    );
    let link: Option<i64> = conn
        .query_row(
            "SELECT parent_conversation_id FROM viewer_session_facts \
             WHERE conversation_id = 7",
        )?
        .get_typed(0)?;
    ensure!(
        link == Some(900_001),
        "the matching child must carry the link: {link:?}"
    );

    let legacy_sql = "\
         SELECT f.conversation_id, f.coverage_detail_json \
         FROM viewer_session_facts f \
         JOIN conversations c ON c.id = f.conversation_id \
         JOIN agents a ON a.id = c.agent_id \
         WHERE f.parent_conversation_id IS NULL \
           AND f.parent_external_id IS NOT NULL \
           AND f.parent_external_id != '' \
           AND f.conversation_id != ?4 \
           AND a.slug = ?1 AND c.source_id = ?2 \
           AND (?3 = f.parent_external_id \
                OR ?3 LIKE '%/' || f.parent_external_id \
                OR ?3 LIKE '%' || f.parent_external_id || '.jsonl') \
         ORDER BY f.conversation_id";
    let legacy_params =
        param_slice_to_values(params!["codex", "local", parent_external_id, 900_001_i64]);

    // Warm both paths (statement cache, page cache) before timing.
    let _ = conn.query_with_params(legacy_sql, &legacy_params)?;
    let _ =
        facts::fixup_pending_parent_links(&conn, 900_002, "codex", "local", parent_external_id)?;

    const LEGACY_CALLS: usize = 6;
    const NEW_CALLS: usize = 30;
    let start = std::time::Instant::now();
    for _ in 0..LEGACY_CALLS {
        let _ = conn.query_with_params(legacy_sql, &legacy_params)?;
    }
    let legacy_elapsed = start.elapsed();
    let start = std::time::Instant::now();
    for _ in 0..NEW_CALLS {
        let _ = facts::fixup_pending_parent_links(
            &conn,
            900_002,
            "codex",
            "local",
            parent_external_id,
        )?;
    }
    let new_elapsed = start.elapsed();
    let legacy_avg_ms = legacy_elapsed.as_secs_f64() * 1000.0 / LEGACY_CALLS as f64;
    let new_avg_ms = new_elapsed.as_secs_f64() * 1000.0 / NEW_CALLS as f64;
    eprintln!(
        "fixup cost: legacy scan {legacy_avg_ms:.3} ms/call, indexed {new_avg_ms:.3} ms/call \
         over {ROWS} pending rows"
    );
    ensure!(
        legacy_avg_ms >= 5.0 * new_avg_ms,
        "the fixup lookup must be index-served: legacy scan {legacy_avg_ms:.3} ms/call \
         vs new {new_avg_ms:.3} ms/call over {ROWS} rows"
    );
    Ok(())
}

/// Seed one canonical conversation row with the caller-chosen live scope
/// (agent id + source) — the child's scope the fixup joins against. The
/// id-derived strings are written into preallocated buffers (`fmt::Write`
/// into a String is infallible) so the seeding loops stay allocation-lean.
fn fixup_seed_conversation(
    conn: &Connection,
    id: i64,
    agent_id: i64,
    source_id: &str,
) -> Result<()> {
    let mut external_id = String::with_capacity(16);
    let _ = write!(external_id, "child-{id}");
    let mut source_path = String::with_capacity(32);
    let _ = write!(source_path, "/seed/child-{id}.jsonl");
    conn.execute_with_params(
        "INSERT INTO conversations \
         (id, agent_id, source_id, external_id, source_path) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        &param_slice_to_values(params![
            id,
            agent_id,
            source_id,
            external_id.as_str(),
            source_path.as_str()
        ]),
    )?;
    Ok(())
}

/// Seed one facts row exactly the way the publish pipeline does: durable
/// evidence, its lookup key (the real derivation) and a caller-chosen
/// manifest / pre-resolved link.
fn fixup_seed_fact(
    conn: &Connection,
    conversation_id: i64,
    evidence: Option<&str>,
    manifest: Option<&str>,
    parent_conversation_id: Option<i64>,
) -> Result<()> {
    let lookup_key = evidence.map(facts::parent_lookup_key);
    conn.execute_with_params(
        "INSERT INTO viewer_session_facts \
         (conversation_id, extractor_version, source_revision, coverage_state, \
          coverage_detail_json, parent_external_id, parent_lookup_key, \
          parent_conversation_id) \
         VALUES (?1, 1, 42, 'partial', ?2, ?3, ?4, ?5)",
        &param_slice_to_values(params![
            conversation_id,
            manifest,
            evidence,
            lookup_key,
            parent_conversation_id
        ]),
    )?;
    Ok(())
}

/// The engine's own match predicate (the old query's arms, evaluated as a
/// scalar): byte-exact equality or the two LIKE suffix shapes.
fn fixup_engine_evidence_matches(
    conn: &Connection,
    parent_external_id: &str,
    evidence: &str,
) -> Result<bool> {
    let matched = conn.query_with_params(
        "SELECT (?1 = ?2) \
             OR (?1 LIKE '%/' || ?2) \
             OR (?1 LIKE '%' || ?2 || '.jsonl')",
        &param_slice_to_values(params![parent_external_id, evidence]),
    )?;
    Ok(matches!(
        matched.first().map(|row| row.get_typed::<i64>(0)),
        Some(Ok(1))
    ))
}

/// The pre-change candidate query + update loop, verbatim (only the row
/// plumbing adapted to a bare Connection): the differential oracle is the
/// exact old behavior, not a paraphrase of it.
#[allow(clippy::too_many_lines)]
fn fixup_legacy_pending_parent_links(
    conn: &Connection,
    parent_conversation_id: i64,
    parent_provider: &str,
    parent_source_id: &str,
    parent_external_id: &str,
) -> Result<usize> {
    let candidates: Vec<(i64, Option<String>)> = conn
        .query_with_params(
            "SELECT f.conversation_id, f.coverage_detail_json \
             FROM viewer_session_facts f \
             JOIN conversations c ON c.id = f.conversation_id \
             JOIN agents a ON a.id = c.agent_id \
             WHERE f.parent_conversation_id IS NULL \
               AND f.parent_external_id IS NOT NULL \
               AND f.parent_external_id != '' \
               AND f.conversation_id != ?4 \
               AND a.slug = ?1 AND c.source_id = ?2 \
               AND (?3 = f.parent_external_id \
                    OR ?3 LIKE '%/' || f.parent_external_id \
                    OR ?3 LIKE '%' || f.parent_external_id || '.jsonl') \
             ORDER BY f.conversation_id",
            &param_slice_to_values(params![
                parent_provider,
                parent_source_id,
                parent_external_id,
                parent_conversation_id
            ]),
        )?
        .iter()
        .map(|row| -> Result<(i64, Option<String>)> { Ok((row.get_typed(0)?, row.get_typed(1)?)) })
        .collect::<Result<Vec<_>>>()?;
    let mut resolved = 0_usize;
    for (child_id, detail) in candidates {
        let parsed = detail
            .as_deref()
            .and_then(|text| serde_json::from_str::<Value>(text).ok());
        if parsed
            .as_ref()
            .and_then(|value| value.pointer("/parent/resolution"))
            .and_then(Value::as_str)
            == Some("ambiguous")
        {
            continue;
        }
        resolved += 1;
        let patched = match (detail, parsed) {
            (Some(text), Some(mut value)) => {
                if let Some(slot) = value.pointer_mut("/parent/resolution") {
                    *slot = json!("resolved");
                }
                Some(serde_json::to_string(&value).unwrap_or(text))
            }
            _ => None,
        };
        match patched {
            Some(detail) => conn.execute_with_params(
                "UPDATE viewer_session_facts \
                 SET parent_conversation_id = ?1, coverage_detail_json = ?2 \
                 WHERE conversation_id = ?3",
                &param_slice_to_values(params![parent_conversation_id, detail, child_id]),
            )?,
            None => conn.execute_with_params(
                "UPDATE viewer_session_facts \
                 SET parent_conversation_id = ?1 \
                 WHERE conversation_id = ?2",
                &param_slice_to_values(params![parent_conversation_id, child_id]),
            )?,
        };
    }
    Ok(resolved)
}

/// Deterministic LCG for the differential sweep (no ambient randomness).
struct FixupLcg(u64);

impl FixupLcg {
    fn below(&mut self, bound: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 % bound
    }
}

/// Differential oracle for the fixup rewrite: the legacy query (verbatim)
/// and the real fixup must produce IDENTICAL per-call counts and IDENTICAL
/// final row state over a fixture built from the report's truth table —
/// nested path suffixes, the `a/b` vs `xa/b` suffix collision, empty/NULL
/// evidence, pre-resolved/self/wrong-scope rows, wildcard evidence,
/// case-sensitivity, non-ASCII ids, ambiguous and unparseable manifests,
/// several candidates — plus a deterministic 120-pair (P, E) sweep whose
/// alphabet stresses suffix boundaries, wildcards, case and multi-byte
/// characters. The no-miss check pins the index design directly: any pair
/// the ENGINE matches must be reachable by a probe key.
#[test]
fn fixup_parent_links_differential_matches_legacy_query() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let legacy = fixup_fixture_db(&temp.path().join("legacy.db"))?;
    let indexed = fixup_fixture_db(&temp.path().join("indexed.db"))?;

    const UNRESOLVED: Option<&str> = Some(r#"{"parent":{"resolution":"unresolved"}}"#);
    const AMBIGUOUS: Option<&str> = Some(r#"{"parent":{"resolution":"ambiguous"}}"#);
    const NO_SLOT: Option<&str> = Some(r#"{"extractor":"fixture"}"#);
    const INVALID: Option<&str> = Some("not json{{");

    // Truth-table rows: (conversation_id, evidence, manifest, pre-set link).
    type FixupRow<'a> = (i64, Option<&'a str>, Option<&'a str>, Option<i64>);
    let rows: &[FixupRow] = &[
        // P1 = /deep/nest/ed/rollout-x.jsonl: nested suffixes, all three
        // arms, and every way a row can fall out of candidacy.
        (1, Some("rollout-x"), UNRESOLVED, None),    // arm C
        (2, Some("rollout-x.jsonl"), NO_SLOT, None), // arm B, manifest without the slot
        (3, Some("ed/rollout-x"), UNRESOLVED, None), // arm C, nested suffix
        (4, Some("nest/ed/rollout-x"), UNRESOLVED, None), // arm C, nested suffix
        (5, Some("deep/nest/ed/rollout-x"), UNRESOLVED, None), // arm C, whole core
        (6, Some("ROLLOUT-X"), UNRESOLVED, None),    // arm C, ASCII-ci
        (7, Some("rollout_x"), UNRESOLVED, None),    // '_' wildcard (sentinel key)
        (8, Some("%t-x"), UNRESOLVED, None),         // '%' wildcard (sentinel key)
        (9, Some("rollout-x"), AMBIGUOUS, None),     // candidate, never claimed
        (10, Some("rollout-x"), UNRESOLVED, Some(555)), // already resolved
        (12, Some("rollout-x"), UNRESOLVED, None),   // wrong agent scope
        (13, Some("rollout-x"), UNRESOLVED, None),   // wrong source scope
        (16, Some("rollout-xj"), UNRESOLVED, None),  // suffix boundary
        (17, Some("x.jsonl"), UNRESOLVED, None),     // arm B boundary ('-' before)
        (18, Some("rollout-x.jsonl.jsonl"), UNRESOLVED, None), // arm C boundary
        (19, Some("rollout-x"), INVALID, None),      // unparseable manifest
        (20, Some("rollout-x"), None, None),         // NULL manifest
        // P2 = /root/xa/b: the a/b vs xa/b suffix collision.
        (21, Some("a/b"), UNRESOLVED, None),  // NOT a candidate
        (22, Some("b"), UNRESOLVED, None),    // arm B
        (23, Some("xa/b"), UNRESOLVED, None), // arm B
        (24, Some("root/xa/b"), UNRESOLVED, None), // arm B, whole core
        // P3 = abc: arm A only (no '/', no .jsonl).
        (25, Some("abc"), UNRESOLVED, None), // arm A
        (26, Some("ABC"), UNRESOLVED, None), // A is case-SENSITIVE: no match
        (27, Some("a_c"), UNRESOLVED, None), // no arm reaches it
        // P4 = '' claims nothing (E='%' only matches ids containing '/').
        (28, Some("%"), UNRESOLVED, None), // wildcard sentinel row
        // P5 = /p/sess-É.jsonl (claude_code scope, non-ASCII).
        (29, Some("sess-É"), UNRESOLVED, None), // arm C, ASCII part folds
        (30, Some("sess-é"), UNRESOLVED, None), // non-ASCII case: no match
        (31, Some("SESS-É"), UNRESOLVED, None), // arm C
        (32, Some("sess-É"), UNRESOLVED, None), // wrong agent scope for P5
        // P6 = /logs/s_2026/roll.jsonl: wildcard-heavy id, several
        // candidates sharing one evidence.
        (33, Some("s_2026/roll"), UNRESOLVED, None), // sentinel, arm C
        (34, Some("%"), UNRESOLVED, None),           // sentinel, arm B (contains '/')
        (35, Some("roll"), UNRESOLVED, None),        // several candidates...
        (36, Some("roll"), UNRESOLVED, None),        // ...sharing one evidence...
        (37, Some("roll"), UNRESOLVED, None),        // ...and one ambiguous skip
        (38, Some("roll"), AMBIGUOUS, None),
        (39, Some("r%ll"), UNRESOLVED, None), // sentinel, arm C
        (40, Some("sX2026/roll"), UNRESOLVED, None), // '_' literal vs X
        (42, Some("rollout-x"), UNRESOLVED, None), // agent row missing
        // The corrupt self-row for P1: conversation 900 IS P1's parent id.
        (900, Some("rollout-x"), UNRESOLVED, None),
        // Empty/NULL evidence never matches anything.
        (14, Some(""), UNRESOLVED, None),
        (15, None, UNRESOLVED, None),
    ];

    for id in 1..=40_i64 {
        let (agent_id, source) = match id {
            12 => (2, "local"),      // claude_code row: P1/P6 are codex
            13 => (1, "remote"),     // wrong source
            29..=31 => (2, "local"), // claude_code rows for P5
            _ => (1, "local"),
        };
        fixup_seed_conversation(&legacy, id, agent_id, source)?;
        fixup_seed_conversation(&indexed, id, agent_id, source)?;
    }
    for (id, agent_id, source) in [
        (42_i64, 999, "local"), // agent row 999 does not exist
        (900_i64, 1, "local"),  // doubles as P1's parent conversation
        (555_i64, 1, "local"),  // row 10's pre-set link target
        (901_i64, 1, "local"),
        (902_i64, 1, "local"),
        (903_i64, 1, "local"),
        (904_i64, 1, "local"),
        (905_i64, 1, "local"),
    ] {
        fixup_seed_conversation(&legacy, id, agent_id, source)?;
        fixup_seed_conversation(&indexed, id, agent_id, source)?;
    }
    for &(id, evidence, manifest, parent) in rows {
        fixup_seed_fact(&legacy, id, evidence, manifest, parent)?;
        fixup_seed_fact(&indexed, id, evidence, manifest, parent)?;
    }

    let parent_calls: &[(i64, &str, &str, &str)] = &[
        (900, "codex", "local", "/deep/nest/ed/rollout-x.jsonl"),
        (901, "codex", "local", "/root/xa/b"),
        (902, "codex", "local", "abc"),
        (903, "codex", "local", ""),
        (904, "claude_code", "local", "/p/sess-É.jsonl"),
        (905, "codex", "local", "/logs/s_2026/roll.jsonl"),
    ];
    for &(parent_id, provider, source, parent_external_id) in parent_calls {
        let legacy_claimed = fixup_legacy_pending_parent_links(
            &legacy,
            parent_id,
            provider,
            source,
            parent_external_id,
        )?;
        let indexed_claimed = facts::fixup_pending_parent_links(
            &indexed,
            parent_id,
            provider,
            source,
            parent_external_id,
        )?;
        ensure!(
            legacy_claimed == indexed_claimed,
            "parent {parent_id} ({parent_external_id}): legacy claimed \
             {legacy_claimed}, indexed claimed {indexed_claimed}"
        );
    }

    // Truth-table spot pins: the full state is compared below; these make
    // a mismatch legible instead of a dump diff.
    let link = |conn: &Connection, id: i64| -> Result<Option<i64>> {
        Ok(conn
            .query_row_with_params(
                "SELECT parent_conversation_id FROM viewer_session_facts \
                 WHERE conversation_id = ?1",
                &[SqliteValue::Integer(id)],
            )?
            .get_typed(0)?)
    };
    for (id, expected) in [
        (1_i64, Some(900_i64)),
        (900, None),     // corrupt self-row never self-parents
        (9, None),       // ambiguous never claimed
        (10, Some(555)), // already-resolved link untouched
        (12, None),      // wrong agent scope
        (13, None),      // wrong source scope
        (21, None),      // a/b vs xa/b suffix collision
        (25, Some(902)), // case-sensitive arm A
        (26, None),      // 'ABC' is not 'abc'
        (29, Some(904)), // ASCII part of sess-É folds
        (30, None),      // non-ASCII case does not fold
        (32, None),      // codex row invisible to the claude_code parent
        (42, None),      // missing agent row
        (38, None),      // ambiguous among several candidates
    ] {
        let got = link(&indexed, id)?;
        ensure!(
            got == expected,
            "conversation {id}: expected link {expected:?}, got {got:?}"
        );
    }
    let manifest_of = |conn: &Connection, id: i64| -> Result<Option<String>> {
        Ok(conn
            .query_row_with_params(
                "SELECT coverage_detail_json FROM viewer_session_facts \
                 WHERE conversation_id = ?1",
                &[SqliteValue::Integer(id)],
            )?
            .get_typed(0)?)
    };
    ensure!(
        manifest_of(&indexed, 9)?.as_deref() == Some(r#"{"parent":{"resolution":"ambiguous"}}"#),
        "the never-claimed ambiguous row keeps its recorded resolution"
    );
    ensure!(
        manifest_of(&indexed, 19)?.as_deref() == Some("not json{{"),
        "an unparseable manifest must be left untouched by the link-only update"
    );
    ensure!(
        manifest_of(&indexed, 1)?.as_deref() == Some(r#"{"parent":{"resolution":"resolved"}}"#),
        "a claimed row's manifest must record the resolution"
    );

    // Deterministic (P, E) sweep: 120 generated parent/evidence pairs over
    // an alphabet stressing suffix boundaries, wildcards, case and
    // multi-byte characters, interleaved into the archive one pair at a
    // time so both sides see identical incremental growth.
    let mut rng = FixupLcg(0x5EED_2026_1110_0007);
    let alphabet = ['a', 'A', '/', '.', 'x', 'j', '_', '%', 'é', '9'];
    for i in 0..120_i64 {
        let parent_len = 3 + rng.below(10);
        let evidence_len = 1 + rng.below(8);
        let mut parent_external_id = String::new();
        for _ in 0..parent_len {
            parent_external_id.push(alphabet[rng.below(alphabet.len() as u64) as usize]);
        }
        let mut evidence = String::new();
        for _ in 0..evidence_len {
            evidence.push(alphabet[rng.below(alphabet.len() as u64) as usize]);
        }
        let (child_id, parent_id) = (2000 + i, 30_000 + i);
        for conn in [&legacy, &indexed] {
            fixup_seed_conversation(conn, child_id, 1, "local")?;
            fixup_seed_conversation(conn, parent_id, 1, "local")?;
            fixup_seed_fact(conn, child_id, Some(evidence.as_str()), UNRESOLVED, None)?;
        }
        let legacy_claimed = fixup_legacy_pending_parent_links(
            &legacy,
            parent_id,
            "codex",
            "local",
            &parent_external_id,
        )?;
        let indexed_claimed = facts::fixup_pending_parent_links(
            &indexed,
            parent_id,
            "codex",
            "local",
            &parent_external_id,
        )?;
        ensure!(
            legacy_claimed == indexed_claimed,
            "sweep pair {i} ({parent_external_id:?} vs {evidence:?}): \
             legacy claimed {legacy_claimed}, indexed claimed {indexed_claimed}"
        );
        // No-miss property, checked directly against the engine: a pair
        // the engine matches must be reachable by a probe key.
        if fixup_engine_evidence_matches(&legacy, &parent_external_id, &evidence)? {
            let key = facts::parent_lookup_key(&evidence);
            let probes = facts::parent_fixup_probe_keys(&parent_external_id);
            ensure!(
                probes.contains(&key) || key == facts::PARENT_LOOKUP_WILDCARD_SENTINEL,
                "sweep pair {i}: engine match {parent_external_id:?} vs \
                 {evidence:?} has no covering probe key"
            );
        }
    }
    // The handcrafted pairs get the same direct no-miss check. Empty
    // evidence is skipped: the old query's `parent_external_id != ''`
    // candidacy filter runs BEFORE the match predicate, so an empty
    // evidence row is never a candidate even though the bare engine
    // predicate can match it (arm B's `%/'`-shaped pattern with E=`''`
    // degenerates to "contains a slash") — the fixup filters empties in
    // Rust, exactly like the old SQL did.
    for &(_, evidence, _, _) in rows {
        let Some(evidence) = evidence else { continue };
        if evidence.is_empty() {
            continue;
        }
        for &(_, _, _, parent_external_id) in parent_calls {
            if fixup_engine_evidence_matches(&legacy, parent_external_id, evidence)? {
                let key = facts::parent_lookup_key(evidence);
                let probes = facts::parent_fixup_probe_keys(parent_external_id);
                ensure!(
                    probes.contains(&key) || key == facts::PARENT_LOOKUP_WILDCARD_SENTINEL,
                    "engine match {parent_external_id:?} vs {evidence:?} has \
                     no covering probe key"
                );
            }
        }
    }

    // Identical final state, row for row.
    type DumpRow = (i64, Option<i64>, Option<String>);
    let dump = |conn: &Connection| -> Result<Vec<DumpRow>> {
        conn.query(
            "SELECT conversation_id, parent_conversation_id, coverage_detail_json \
             FROM viewer_session_facts ORDER BY conversation_id",
        )?
        .iter()
        .map(|row| -> Result<DumpRow> {
            Ok((row.get_typed(0)?, row.get_typed(1)?, row.get_typed(2)?))
        })
        .collect()
    };
    let (legacy_state, indexed_state) = (dump(&legacy)?, dump(&indexed)?);
    ensure!(
        legacy_state == indexed_state,
        "final facts state diverged: {} legacy rows vs {} indexed rows",
        legacy_state.len(),
        indexed_state.len()
    );
    Ok(())
}

/// The pre-v9 `viewer_session_facts` shape — the v1 DDL exactly as the v8
/// profile shipped it, without the v9 lookup-key column — for simulating an
/// existing archive before the upgrade.
const FIXUP_PRE_V9_FACTS_DDL: &str = "CREATE TABLE viewer_session_facts (\
    conversation_id INTEGER PRIMARY KEY \
        REFERENCES conversations(id) ON DELETE CASCADE, \
    extractor_version INTEGER NOT NULL, \
    source_revision INTEGER NOT NULL DEFAULT 0, \
    coverage_state TEXT NOT NULL DEFAULT 'unknown', \
    coverage_detail_json TEXT, \
    is_subagent INTEGER, \
    parent_conversation_id INTEGER REFERENCES conversations(id), \
    parent_external_id TEXT, \
    task_category TEXT, \
    turn_count INTEGER, \
    step_count INTEGER, \
    thinking_block_count INTEGER, \
    artifact_count INTEGER, \
    subagent_count INTEGER, \
    tool_error_count INTEGER, \
    tool_calls_with_exit_status INTEGER, \
    file_fact_count INTEGER, \
    compaction_count INTEGER, \
    model_set_json TEXT, \
    first_prompt TEXT, \
    extracted_at_ms INTEGER \
)";

/// v9 migration: an archive whose profile predates the lookup-key column
/// must upgrade in place — guarded ALTER, a backfill matching the Rust
/// derivation exactly (clean ASCII-lowercase, the wildcard sentinel for
/// `%`/`_` evidence, untouched NULL for missing evidence), the index, and
/// pre-migration rows must stay claimable through the new path.
#[test]
fn facts_parent_lookup_key_migration_for_pre_v9_archives() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let conn = fixup_base_db(&temp.path().join("migrate.db"))?;

    // Simulate the existing v8 archive: the pre-v9 facts table, the meta
    // marker at 8, and pending rows whose evidence predates the key.
    conn.execute_batch(&format!(
        "{FIXUP_PRE_V9_FACTS_DDL}; \
         CREATE TABLE viewer_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL); \
         INSERT INTO viewer_meta (key, value) VALUES ('extension_schema_version', '8');"
    ))?;
    for id in 1..=5_i64 {
        fixup_seed_conversation(&conn, id, 1, "local")?;
    }
    for (id, external_id, source_path) in [
        (800_i64, "migrate-parent-a", "/arch/migrate-parent-a.jsonl"),
        (801_i64, "migrate-parent-b", "/arch/migrate-parent-b.jsonl"),
    ] {
        conn.execute_with_params(
            "INSERT INTO conversations \
             (id, agent_id, source_id, external_id, source_path) \
             VALUES (?1, 1, 'local', ?2, ?3)",
            &param_slice_to_values(params![id, external_id, source_path]),
        )?;
    }
    // (id, evidence) — the keys are backfilled by the migration, never
    // seeded here.
    let rows: &[(i64, Option<&str>)] = &[
        (1, Some("migrate-clean")),
        (2, Some("migrate_wild")), // '_' is a LIKE wildcard: sentinel key
        (3, Some("")),             // empty evidence stays unclaimable
        (4, None),                 // no evidence stays NULL
        (5, Some("MIXED-Case")),   // ASCII-lowercase key
    ];
    for &(id, evidence) in rows {
        conn.execute_with_params(
            "INSERT INTO viewer_session_facts \
             (conversation_id, extractor_version, source_revision, coverage_state, \
              coverage_detail_json, parent_external_id) \
             VALUES (?1, 1, 42, 'partial', ?2, ?3)",
            &param_slice_to_values(params![
                id,
                r#"{"parent":{"resolution":"unresolved"}}"#,
                evidence
            ]),
        )?;
    }
    ensure!(
        schema::installed_version(&conn)? == Some(8),
        "fixture must stand at schema v8"
    );

    // The upgrade: guarded ALTER + backfill + index, one schema
    // transaction.
    schema::ensure_schema(&conn)?;

    ensure!(
        schema::installed_version(&conn)? == Some(schema::EXTENSION_SCHEMA_VERSION),
        "the upgrade must land on the current extension profile"
    );
    let index = conn.query(
        "SELECT 1 FROM sqlite_master \
         WHERE type='index' AND name='idx_viewer_session_facts_parent_lookup'",
    )?;
    ensure!(
        !index.is_empty(),
        "the v9 batch must create the lookup index"
    );

    // Backfilled keys must equal the Rust derivation exactly — the SQL
    // CASE (lower/char(1)/LIKE...ESCAPE) mirrors facts::parent_lookup_key.
    for &(id, evidence) in rows {
        let expected = evidence.map(facts::parent_lookup_key);
        let actual: Option<String> = conn
            .query_row_with_params(
                "SELECT parent_lookup_key FROM viewer_session_facts \
                 WHERE conversation_id = ?1",
                &[SqliteValue::Integer(id)],
            )?
            .get_typed(0)?;
        ensure!(
            actual == expected,
            "conversation {id}: backfilled key {actual:?} != derivation {expected:?}"
        );
    }

    // Idempotence: a second ensure_schema keeps the backfilled keys.
    schema::ensure_schema(&conn)?;
    let stable: Option<String> = conn
        .query_row_with_params(
            "SELECT parent_lookup_key FROM viewer_session_facts \
             WHERE conversation_id = 1",
            &[SqliteValue::Integer(1)],
        )?
        .get_typed(0)?;
    ensure!(
        stable.as_deref() == Some("migrate-clean"),
        "a re-run must not touch backfilled keys: {stable:?}"
    );

    // End-to-end: the fixup links PRE-MIGRATION rows through the new
    // index — both a clean key and the sentinel-served wildcard row.
    let clean = facts::fixup_pending_parent_links(
        &conn,
        800,
        "codex",
        "local",
        "/arch/migrate-clean.jsonl",
    )?;
    ensure!(
        clean == 1,
        "the migrated clean row must be claimable: {clean}"
    );
    let wildcard = facts::fixup_pending_parent_links(
        &conn,
        801,
        "codex",
        "local",
        "/arch/migrate_wild.jsonl",
    )?;
    ensure!(
        wildcard == 1,
        "the migrated wildcard row must be claimable through the sentinel \
         probe: {wildcard}"
    );
    for (id, expected) in [
        (1_i64, Some(800_i64)),
        (2, Some(801)),
        (3, None),
        (4, None),
        (5, None),
    ] {
        let got: Option<i64> = conn
            .query_row_with_params(
                "SELECT parent_conversation_id FROM viewer_session_facts \
                 WHERE conversation_id = ?1",
                &[SqliteValue::Integer(id)],
            )?
            .get_typed(0)?;
        ensure!(
            got == expected,
            "conversation {id}: expected link {expected:?}, got {got:?}"
        );
    }
    Ok(())
}

#[test]
fn schema_init_extensions_flag_uses_authorized_writer_then_exits() -> Result<()> {
    // The bridge binary's one-shot writer path initializes the extension
    // profile on an existing canonical archive, then exits without serving.
    let temp = tempfile::tempdir()?;
    let data_dir = temp.path().join("data");
    std::fs::create_dir(&data_dir)?;
    let db = data_dir.join("archive.db");
    let storage = FrankenStorage::open(&db)?;
    drop(storage);

    let status = Command::new(env!("CARGO_BIN_EXE_cass-viewer-bridge"))
        .arg("--stdio")
        .arg("--init-extensions")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--db")
        .arg(&db)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .status()?;
    ensure!(status.success(), "--init-extensions failed: {status}");

    let reader = Connection::open(db.to_string_lossy().into_owned())?;
    let objects = viewer_objects(&reader)?;
    ensure!(
        objects.contains(&("table".to_owned(), "viewer_annotations".to_owned())),
        "--init-extensions did not install the extension profile: {objects:?}"
    );
    drop(reader);
    Ok(())
}

// ---------------------------------------------------------------------------
// Task 4: explicit bridge capabilities, write ownership and logical revisions
// ---------------------------------------------------------------------------

/// Run the authorized one-shot writer lane against a harness fixture:
/// installs the `viewer_*` profile AND the canonical-observer triggers, then
/// exits without serving.
fn init_extensions(bridge: &BridgeHarness) -> Result<()> {
    let status = Command::new(env!("CARGO_BIN_EXE_cass-viewer-bridge"))
        .arg("--stdio")
        .arg("--init-extensions")
        .arg("--data-dir")
        .arg(&bridge.data_dir)
        .arg("--db")
        .arg(&bridge.db)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .status()?;
    ensure!(status.success(), "--init-extensions failed: {status}");
    Ok(())
}

fn generation_of(reply: &serde_json::Value) -> serde_json::Value {
    reply["result"]["generation"].clone()
}

#[test]
fn revisions_strict_lane_advertises_capability_without_write_privilege() -> Result<()> {
    // Read-only bridge over an extended archive: the capability is
    // advertised, the generation names all four logical revisions, but the
    // annotations_write privilege stays denied without the explicit local
    // opt-in flag.
    let mut bridge = BridgeHarness::with_conversations(&[FixtureConversation::repeated(
        1,
        "local",
        "needle body",
        2,
    )])?;
    init_extensions(&bridge)?;

    let hello = bridge.exchange(json!({"op":"hello","id":1}))?;
    ensure!(hello["ok"] == true, "hello refused: {hello}");
    let capability = &hello["result"]["capabilities"]["cass_owned_queries_v1"];
    ensure!(
        capability["available"] == true,
        "capability not advertised: {hello}"
    );
    ensure!(
        capability["canonical_observers"] == true,
        "canonical observers not reported: {hello}"
    );
    ensure!(
        capability["privileges"]["annotations_write"] == false,
        "write privilege must stay denied without --enable-viewer-writes: {hello}"
    );
    let generation = generation_of(&hello);
    let mut keys = generation
        .as_object()
        .expect("generation object")
        .keys()
        .collect::<Vec<_>>();
    keys.sort();
    ensure!(
        keys == [
            "annotation_revision",
            "canonical_revision",
            "lexical_generation",
            "projection_revision"
        ],
        "generation must name the four spec-12 revisions: {generation}"
    );
    for key in &keys {
        ensure!(
            generation[key].as_i64() == Some(0),
            "fresh generation counter {key} must be 0: {generation}"
        );
    }

    // Mutating RPC without local authorization: typed refusal, never a write.
    let refused = bridge.exchange(json!({
        "op":"patch_annotations","id":2,"conversation_id":1,
        "expected_revision":0,"patch":{"is_pinned":true}
    }))?;
    ensure!(refused["ok"] == false);
    ensure!(
        refused["error"]["kind"] == "capability_not_granted",
        "mutating RPC must be refused without write opt-in: {refused}"
    );

    // Annotation reads through the strict lane are legal when extensions
    // exist; an absent row is reported, not fabricated.
    let annotations =
        bridge.exchange(json!({"op":"get_annotations","id":3,"conversation_id":1}))?;
    ensure!(
        annotations["ok"] == true,
        "get_annotations refused: {annotations}"
    );
    ensure!(annotations["result"]["installed"] == true);
    ensure!(annotations["result"]["annotation"].is_null());
    ensure!(annotations["result"]["generation"]["canonical_revision"] == 0);
    bridge.shutdown()
}

#[test]
fn revisions_observer_triggers_catch_external_canonical_writers() -> Result<()> {
    // A stock-style writer through a *different* connection must bump the
    // logical canonical revision: the observer triggers fire inside the
    // external commit, while the read-only bridge only ever reports.
    let mut bridge = BridgeHarness::with_conversations(&[FixtureConversation::repeated(
        1,
        "local",
        "original text",
        2,
    )])?;
    init_extensions(&bridge)?;
    let hello = bridge.exchange(json!({"op":"hello","id":1}))?;
    ensure!(hello["result"]["generation"]["canonical_revision"] == 0);

    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    writer
        .execute("UPDATE messages SET content='mutated text' WHERE conversation_id=1 AND idx=0")?;
    drop(writer);

    let status = bridge.exchange(json!({"op":"status","id":2}))?;
    let generation = generation_of(&status);
    ensure!(
        generation["canonical_revision"].as_i64() == Some(1),
        "external UPDATE must bump canonical_revision exactly once: {generation}"
    );
    ensure!(
        generation["annotation_revision"].as_i64() == Some(0)
            && generation["projection_revision"].as_i64() == Some(0)
            && generation["lexical_generation"].as_i64() == Some(0),
        "canonical writes must not move the other logical revisions: {generation}"
    );

    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    writer.execute(
        "INSERT INTO messages (conversation_id,idx,role,content) VALUES (1,2,'agent','appended')",
    )?;
    writer.execute("DELETE FROM messages WHERE conversation_id=1 AND idx=2")?;
    drop(writer);
    let status = bridge.exchange(json!({"op":"status","id":3}))?;
    ensure!(
        status["result"]["generation"]["canonical_revision"].as_i64() == Some(3),
        "external INSERT and DELETE must each bump canonical_revision: {status}"
    );
    bridge.shutdown()
}

#[test]
fn revisions_same_values_rewrite_is_not_a_content_update() -> Result<()> {
    // Unchanged ingest writes the same values back: conditional UPDATE
    // triggers must suppress the false content update while genuine changes
    // still bump.
    let mut bridge = BridgeHarness::with_conversations(&[FixtureConversation::repeated(
        1,
        "local",
        "stable text",
        2,
    )])?;
    init_extensions(&bridge)?;
    ensure!(
        bridge.exchange(json!({"op":"hello","id":1}))?["result"]["generation"]["canonical_revision"]
            == 0
    );

    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    writer.execute("UPDATE messages SET content=content WHERE conversation_id=1")?;
    writer
        .execute("UPDATE conversations SET title=title,metadata_json=metadata_json WHERE id=1")?;
    let status = bridge.exchange(json!({"op":"status","id":2}))?;
    ensure!(
        status["result"]["generation"]["canonical_revision"].as_i64() == Some(0),
        "same-values UPDATE must not bump canonical_revision: {status}"
    );

    // Re-ingesting the same conversation is quiet at steady state. The
    // FIRST replay legitimately bumps: it backfills derived `ended_at`
    // columns the original insert left NULL — real stored-byte change.
    // Every later identical replay must produce no false content update.
    let agent_id: i64 = writer
        .query_row("SELECT agent_id FROM conversations WHERE id=1")?
        .get_typed(0)?;
    let source_id: String = writer
        .query_row("SELECT source_id FROM conversations WHERE id=1")?
        .get_typed(0)?;
    drop(writer);
    let replay = || coding_agent_search::model::types::Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: None,
        external_id: Some("fixture-1".into()),
        title: Some("fixture".into()),
        source_path: "/missing/source-1.jsonl".into(),
        started_at: None,
        ended_at: None,
        approx_tokens: None,
        metadata_json: json!({"fixture":true}),
        source_id: source_id.clone(),
        origin_host: None,
        messages: (0..2_i64)
            .map(|idx| coding_agent_search::model::types::Message {
                id: None,
                idx,
                role: coding_agent_search::model::types::MessageRole::Agent,
                author: None,
                created_at: Some(1_700_000_000_000 + idx),
                content: "stable text".into(),
                extra_json: json!({"fixture":idx}),
                snippets: Vec::new(),
            })
            .collect(),
    };
    for round in 0..2 {
        let storage = FrankenStorage::open(&bridge.db)?;
        let outcome = storage.insert_conversation_tree(agent_id, None, &replay())?;
        drop(storage);
        ensure!(outcome.conversation_id == 1);
        let status = bridge.exchange(json!({"op":"status","id":10 + round}))?;
        let canonical = status["result"]["generation"]["canonical_revision"].as_i64();
        if round == 0 {
            ensure!(
                canonical == Some(2),
                "first replay must bump once per real backfill write: {status}"
            );
        } else {
            ensure!(
                canonical == Some(2),
                "steady-state identical re-ingest produced a false content update: {status}"
            );
        }
    }

    // A genuinely changed served column still bumps through the same lane.
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    writer.execute("UPDATE conversations SET title='new title' WHERE id=1")?;
    drop(writer);
    let status = bridge.exchange(json!({"op":"status","id":4}))?;
    ensure!(
        status["result"]["generation"]["canonical_revision"].as_i64() == Some(3),
        "real canonical update must bump canonical_revision: {status}"
    );
    bridge.shutdown()
}

#[test]
fn revisions_authorized_writer_accepts_only_narrow_annotation_ops() -> Result<()> {
    // With the explicit local opt-in the bridge owns one writable extension
    // connection behind a serialized boundary: narrow annotation ops work,
    // arbitrary SQL is still refused at parse time.
    let mut bridge = BridgeHarness::writable_with_conversations(&[FixtureConversation::repeated(
        1,
        "local",
        "fixture body",
        2,
    )])?;
    init_extensions(&bridge)?;
    let hello = bridge.exchange(json!({"op":"hello","id":1}))?;
    ensure!(
        hello["result"]["capabilities"]["cass_owned_queries_v1"]["privileges"]["annotations_write"]
            == true,
        "write privilege must be advertised under the opt-in: {hello}"
    );

    let patch = bridge.exchange(json!({
        "op":"patch_annotations","id":2,"conversation_id":1,"expected_revision":0,
        "patch":{"is_pinned":true,"custom_title":"renamed","notes":"first note"}
    }))?;
    ensure!(patch["ok"] == true, "authorized patch refused: {patch}");
    ensure!(patch["result"]["annotation"]["revision"] == 1);
    ensure!(patch["result"]["annotation"]["is_pinned"] == true);
    ensure!(patch["result"]["annotation"]["custom_title"] == "renamed");
    ensure!(patch["result"]["annotation"]["notes"] == "first note");
    ensure!(
        patch["result"]["generation"]["annotation_revision"] == 1
            && patch["result"]["generation"]["canonical_revision"] == 0
            && patch["result"]["generation"]["lexical_generation"] == 0,
        "annotation write must bump only annotation_revision: {patch}"
    );

    // Compare-and-swap: a stale expected revision is a typed conflict.
    let stale = bridge.exchange(json!({
        "op":"patch_annotations","id":3,"conversation_id":1,"expected_revision":0,
        "patch":{"is_pinned":false}
    }))?;
    ensure!(stale["ok"] == false);
    ensure!(
        stale["error"]["kind"] == "revision_conflict",
        "stale expected revision must be a typed conflict: {stale}"
    );

    // Explicit clear (null) differs from an absent field.
    let cleared = bridge.exchange(json!({
        "op":"patch_annotations","id":4,"conversation_id":1,"expected_revision":1,
        "patch":{"notes":null,"custom_title":"second"}
    }))?;
    ensure!(cleared["ok"] == true, "{cleared}");
    ensure!(cleared["result"]["annotation"]["revision"] == 2);
    ensure!(cleared["result"]["annotation"]["notes"].is_null());
    ensure!(cleared["result"]["annotation"]["custom_title"] == "second");
    ensure!(cleared["result"]["annotation"]["is_pinned"] == true);

    // Narrow-only is enforced: no arbitrary SQL and bounded patch fields.
    for (id, request, kind) in [
        (
            5,
            json!({"op":"arbitrary_sql","id":5,"sql":"DELETE FROM viewer_annotations"}),
            "invalid_request",
        ),
        (
            6,
            json!({"op":"patch_annotations","id":6,"conversation_id":1,"expected_revision":-1,
                   "patch":{"is_pinned":true}}),
            "invalid_request",
        ),
        (
            7,
            json!({"op":"patch_annotations","id":7,"conversation_id":1,"expected_revision":2,
                   "patch":{}}),
            "invalid_request",
        ),
        (
            8,
            json!({"op":"patch_annotations","id":8,"conversation_id":1,"expected_revision":2,
                   "patch":{"notes":"x".repeat(40_000)}}),
            "invalid_request",
        ),
        (
            9,
            json!({"op":"patch_annotations","id":9,"conversation_id":99,"expected_revision":0,
                   "patch":{"is_pinned":true}}),
            "unknown_conversation",
        ),
    ] {
        let reply = bridge.exchange(request)?;
        ensure!(
            reply["ok"] == false,
            "request {id} unexpectedly succeeded: {reply}"
        );
        ensure!(
            reply["error"]["kind"] == kind,
            "request {id} wrong refusal kind: {reply}"
        );
    }

    // The persisted state matches the last accepted patch, and exactly one
    // annotation_revision bump committed per accepted patch.
    let reader = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    let row = reader.query_row(
        "SELECT is_pinned,custom_title,notes,revision FROM viewer_annotations WHERE conversation_id=1",
    )?;
    ensure!(row.get_typed::<i64>(0)? == 1);
    ensure!(row.get_typed::<Option<String>>(1)?.as_deref() == Some("second"));
    ensure!(row.get_typed::<Option<String>>(2)?.is_none());
    ensure!(row.get_typed::<i64>(3)? == 2);
    let revisions = reader.query_row(
        "SELECT canonical_revision,annotation_revision,projection_revision,lexical_generation \
         FROM viewer_revisions WHERE id=1",
    )?;
    ensure!(
        revisions.get_typed::<i64>(0)? == 0,
        "annotation writes touched canonical_revision"
    );
    ensure!(
        revisions.get_typed::<i64>(1)? == 2,
        "two accepted patches must bump twice"
    );
    ensure!(revisions.get_typed::<i64>(2)? == 0);
    // Task 8: `lexical_generation` counts CERTIFIED Quill publications —
    // each accepted patch's post-edit converge committed a real index
    // generation, so the counter must be positive, never still 0 (canonical
    // progress alone would leave it untouched).
    ensure!(
        revisions.get_typed::<i64>(3)? >= 1,
        "post-edit converge committed real publications; lexical_generation must certify them"
    );
    drop(reader);
    bridge.shutdown()
}

#[test]
fn revisions_annotation_write_never_invalidates_canonical_reads() -> Result<()> {
    // Metadata edits move the physical WAL bytes but not the logical
    // canonical generation: an in-flight raw enumeration must survive an
    // annotation write, and canonical pages keep identical content.
    // 300 matching docs keep the raw enumeration cursor alive past the
    // first PAGE_LIMIT page so the mid-enumeration patch is exercised.
    let mut bridge = BridgeHarness::writable_with_conversations(&[FixtureConversation::repeated(
        1,
        "local",
        "searchable needle",
        300,
    )])?;
    init_extensions(&bridge)?;
    let before = bridge.exchange(json!({"op":"session","conversation_id":1}))?;
    ensure!(before["ok"] == true);

    let begin = bridge.exchange(json!({"op":"rank_begin","query":"needle"}))?;
    ensure!(begin["ok"] == true, "rank_begin refused: {begin}");
    let cursor = begin["result"]["next_cursor"].as_str().map(str::to_owned);

    let patch = bridge.exchange(json!({
        "op":"patch_annotations","conversation_id":1,"expected_revision":0,
        "patch":{"custom_title":"annotated mid-enumeration"}
    }))?;
    ensure!(patch["ok"] == true, "{patch}");

    if let Some(cursor) = cursor {
        let next = bridge.exchange(json!({"op":"rank_next","cursor":cursor,"max_hits":64}))?;
        ensure!(
            next["ok"] == true,
            "annotation write must not invalidate a raw enumeration cursor: {next}"
        );
    }
    let after = bridge.exchange(json!({"op":"session","conversation_id":1}))?;
    ensure!(after["ok"] == true, "{after}");
    ensure!(
        after["result"]["row"] == before["result"]["row"],
        "annotation write changed canonical session bytes"
    );
    ensure!(
        after["result"]["generation"]["canonical_revision"] == 0
            && after["result"]["generation"]["annotation_revision"] == 1,
        "logical generations mixed annotation and canonical revisions: {after}"
    );
    bridge.shutdown()
}

#[test]
fn revisions_external_write_can_never_serve_stale_canonical() -> Result<()> {
    // An unobserved external writer changes the physical snapshot; the next
    // canonical read must return the NEW row and a new physical token, never
    // silently stale bytes.
    let mut bridge = BridgeHarness::with_conversations(&[FixtureConversation::repeated(
        1,
        "local",
        "v1 content",
        2,
    )])?;
    init_extensions(&bridge)?;
    let first = bridge.exchange(json!({"op":"session","conversation_id":1}))?;
    ensure!(first["result"]["row"]["title"] == "fixture");
    let token = first["result"]["catalog_revision"].clone();

    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    writer.execute("UPDATE conversations SET title='externally mutated' WHERE id=1")?;
    drop(writer);

    let second = bridge.exchange(json!({"op":"session","conversation_id":1}))?;
    ensure!(second["ok"] == true, "{second}");
    ensure!(
        second["result"]["row"]["title"] == "externally mutated",
        "reader served stale canonical bytes after an external commit: {second}"
    );
    ensure!(
        second["result"]["catalog_revision"] != token,
        "external commit must invalidate the physical snapshot token"
    );
    ensure!(
        second["result"]["generation"]["canonical_revision"] == 1,
        "external commit must bump the logical canonical revision: {second}"
    );
    bridge.shutdown()
}

#[test]
fn revisions_writer_requires_installed_extension_profile() -> Result<()> {
    // The write opt-in alone is not enough: without the extension profile
    // the writer boundary refuses with a typed error instead of creating
    // viewer_* objects on the serving lane.
    let mut bridge = BridgeHarness::writable_with_conversations(&[FixtureConversation::repeated(
        1, "local", "fixture", 1,
    )])?;
    let hello = bridge.exchange(json!({"op":"hello","id":1}))?;
    ensure!(
        hello["result"]["capabilities"]["cass_owned_queries_v1"]["available"] == false,
        "capability must be unavailable without the extension profile: {hello}"
    );
    ensure!(hello["result"]["generation"].is_null());
    let refused = bridge.exchange(json!({
        "op":"patch_annotations","id":2,"conversation_id":1,"expected_revision":0,
        "patch":{"is_pinned":true}
    }))?;
    ensure!(refused["ok"] == false);
    ensure!(
        refused["error"]["kind"] == "viewer_extensions_missing",
        "writes must require the installed extension profile: {refused}"
    );
    let annotations =
        bridge.exchange(json!({"op":"get_annotations","id":3,"conversation_id":1}))?;
    ensure!(annotations["ok"] == true, "{annotations}");
    ensure!(annotations["result"]["installed"] == false);
    bridge.shutdown()
}

#[test]
fn revisions_engine_trigger_semantics_probe() -> Result<()> {
    // Qualify the chosen revision mechanism against the pinned FrankenSQLite
    // build: AFTER INSERT/DELETE fire unconditionally, and an UPDATE trigger
    // whose bump carries `WHERE OLD.x IS NOT NEW.x` suppresses same-values
    // rewrites while genuine column changes still bump — across connections.
    let temp = tempfile::tempdir()?;
    let db = temp.path().join("probe.db");
    let writer = Connection::open(db.to_string_lossy().into_owned())?;
    writer.execute_batch(
        "CREATE TABLE base (id INTEGER PRIMARY KEY, a TEXT, b INTEGER);
         CREATE TABLE bumps (n INTEGER NOT NULL);
         INSERT INTO bumps VALUES (0);
         CREATE TRIGGER probe_ins AFTER INSERT ON base BEGIN
             UPDATE bumps SET n = n + 1;
         END;
         CREATE TRIGGER probe_del AFTER DELETE ON base BEGIN
             UPDATE bumps SET n = n + 1;
         END;
         CREATE TRIGGER probe_upd AFTER UPDATE ON base BEGIN
             UPDATE bumps SET n = n + 1
                 WHERE OLD.a IS NOT NEW.a OR OLD.b IS NOT NEW.b;
         END;",
    )?;
    let bumped = |conn: &Connection| -> Result<i64> {
        Ok(conn.query_row("SELECT n FROM bumps")?.get_typed(0)?)
    };
    ensure!(bumped(&writer)? == 0);
    writer.execute("INSERT INTO base (a,b) VALUES ('x',1)")?;
    ensure!(bumped(&writer)? == 1, "AFTER INSERT must fire");
    writer.execute("UPDATE base SET a=a, b=b WHERE id=1")?;
    ensure!(
        bumped(&writer)? == 1,
        "same-values UPDATE must be suppressed by the OLD/NEW guard"
    );
    writer.execute("UPDATE base SET b=2 WHERE id=1")?;
    ensure!(
        bumped(&writer)? == 2,
        "guarded UPDATE must fire on real change"
    );
    writer.execute("DELETE FROM base WHERE id=1")?;
    ensure!(bumped(&writer)? == 3, "AFTER DELETE must fire");
    drop(writer);
    Ok(())
}

// ---------------------------------------------------------------------------
// Task 5: first-class indexed list (`list_sessions`) and annotations
// ---------------------------------------------------------------------------
//
// The bridge modules are compiled into the `cass-viewer-bridge` binary, not
// the library; for the SQL-plan audit we include the pure modules directly
// under their original names so `super::` paths resolve (same pattern as
// tests/viewer_bridge_identity.rs).
#[path = "../src/viewer_bridge/annotations.rs"]
#[allow(dead_code)]
mod annotations;
#[path = "../src/viewer_bridge/extra.rs"]
#[allow(dead_code)]
mod extra;
#[path = "../src/viewer_bridge/facts_bag.rs"]
#[allow(dead_code)]
mod facts_bag;
#[path = "../src/viewer_bridge/identity.rs"]
#[allow(dead_code)]
mod identity;
#[path = "../src/viewer_bridge/protocol.rs"]
#[allow(dead_code)]
mod protocol;
#[path = "../src/viewer_bridge/query.rs"]
#[allow(dead_code)]
mod query;
#[path = "../src/viewer_bridge/revisions.rs"]
#[allow(dead_code)]
mod revisions;
// The strict reader's open-time capability probe is exercised below; it
// needs the same `super::` sibling set the reader itself names.
#[path = "../src/viewer_bridge/catalog.rs"]
#[allow(dead_code)]
mod catalog;
#[path = "../src/viewer_bridge/reader.rs"]
#[allow(dead_code)]
mod reader;

/// Five seeded conversations, canonical IDs = insertion order but activity
/// deliberately out of order, two providers, NULL timestamps, a workspace
/// and (after setup) a pinned + tagged row:
///
///   id provider     activity                            workspace
///   1  gemini       lmca 1000 (started 50)
///   2  claude_code  lmca 5000 → tail UPDATE to 2500       (started 900)
///   3  gemini       all NULL                              (started NULL)
///   4  claude_code  lmca 3000, pinned + tagged            (started 30)
///   5  gemini       lmca 4000 (started 400)               /data/projects/alpha
///
/// Expected default browse (pinned first, activity desc, NULLs last, id
/// tie-break): [4, 5, 2, 1, 3]. The stale `conversations.last_message_
/// created_at` = 5000 for row 2 makes the tail-state precedence
/// distinguishable: a tail-blind implementation orders [4, 2, 5, 1, 3].
fn list_fixture_a() -> Vec<SeedConversation> {
    vec![
        SeedConversation::new(1, "gemini", "/missing/fixture-1.jsonl", "fixture-1")
            .started_at(Some(50))
            .agent_messages(Some(1_000), 1),
        SeedConversation::new(2, "claude_code", "/missing/BBB.jsonl", "BBB")
            .started_at(Some(900))
            .agent_messages(Some(5_000), 1),
        SeedConversation::new(3, "gemini", "/missing/fixture-3.jsonl", "fixture-3")
            .agent_messages(None, 1),
        SeedConversation::new(4, "claude_code", "/missing/CCC.jsonl", "CCC")
            .started_at(Some(30))
            .agent_messages(Some(3_000), 1),
        SeedConversation::new(5, "gemini", "/missing/fixture-5.jsonl", "fixture-5")
            .workspace("/data/projects/alpha")
            .started_at(Some(400))
            .agent_messages(Some(4_000), 1),
    ]
}

/// Seed annotation + tag for conv 4 and move conv 2's tail timestamp.
fn seed_annotations(bridge: &BridgeHarness) -> Result<()> {
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    writer.execute(
        "INSERT INTO viewer_annotations (conversation_id,is_pinned,revision) VALUES (4,1,1)",
    )?;
    writer.execute(
        "INSERT INTO viewer_tags (conversation_id,tag,created_at_ms) \
         VALUES (4,'release-blocker',1700000000001)",
    )?;
    writer.execute(
        "UPDATE conversation_tail_state SET last_message_created_at = 2500 \
         WHERE conversation_id = 2",
    )?;
    drop(writer);
    Ok(())
}

fn item_ids(page: &Value) -> Vec<i64> {
    page["result"]["items"]
        .as_array()
        .expect("items array")
        .iter()
        .map(|item| item["conversation_id"].as_i64().expect("conversation_id"))
        .collect()
}

fn item(page: &Value, index: usize) -> &Value {
    &page["result"]["items"].as_array().expect("items")[index]
}

#[test]
fn list_sessions_default_order_stable_pagination_tail_precedence() -> Result<()> {
    let mut bridge = BridgeHarness::with_seed(&list_fixture_a(), &[])?;
    init_extensions(&bridge)?;
    seed_annotations(&bridge)?;

    let mut seen = Vec::new();
    let mut cursor = Value::Null;
    for page_no in 0..3 {
        let request = match cursor.as_str() {
            Some(cursor) => json!({"op":"list_sessions","limit":2,"cursor":cursor}),
            None => json!({"op":"list_sessions","limit":2}),
        };
        let page = bridge.exchange(request)?;
        ensure!(page["ok"] == true, "page {page_no} refused: {page}");
        ensure!(
            page["result"]["complete"] == true,
            "browse must be complete: {page}"
        );
        ensure!(
            page["result"]["generation"]["canonical_revision"].is_i64(),
            "page must bind the logical generation: {page}"
        );
        ensure!(
            page["result"]["catalog_revision"].is_string(),
            "page must bind the physical snapshot token: {page}"
        );
        seen.extend(item_ids(&page));
        cursor = page["result"]["next_cursor"].clone();
        if page_no == 0 {
            let first = item(&page, 0);
            ensure!(first["public_id"] == "claude:CCC", "{first}");
            ensure!(first["provider"] == "claude");
            ensure!(first["is_pinned"] == true);
            ensure!(first["activity_ms"] == 3_000);
            ensure!(first["tags"] == json!(["release-blocker"]), "{first}");
            let second = item(&page, 1);
            ensure!(second["public_id"] == "gemini:fixture-5", "{second}");
            ensure!(second["workspace_path"] == "/data/projects/alpha");
            ensure!(second["project"] == "alpha");
        }
        if page_no == 1 {
            // Tail precedence proof: row 2's tail lmca (2500) wins over the
            // stale denormalized conversations.last_message_created_at
            // (5000) — it must sort below conv 5 (4000).
            let row2 = item(&page, 0);
            ensure!(row2["public_id"] == "claude:BBB", "{row2}");
            ensure!(row2["activity_ms"] == 2_500, "{row2}");
        }
        if page_no == 2 {
            // NULL activity sorts last and carries an honest NULL.
            let last = item(&page, 0);
            ensure!(last["public_id"] == "gemini:fixture-3", "{last}");
            ensure!(last["activity_ms"].is_null(), "{last}");
        }
    }
    ensure!(
        seen == [4, 5, 2, 1, 3],
        "paginated order must be pinned-first, activity desc, NULLs last: {seen:?}"
    );
    ensure!(cursor.is_null(), "last page must end the cursor: {cursor}");
    bridge.shutdown()
}

#[test]
fn list_sessions_filters_and_typed_refusals() -> Result<()> {
    let mut bridge = BridgeHarness::with_seed(&list_fixture_a(), &[])?;
    init_extensions(&bridge)?;
    seed_annotations(&bridge)?;

    // --- supported filters -------------------------------------------------
    let claude = bridge.exchange(json!({
        "op":"list_sessions","filters":{"provider":"claude"},"limit":10}))?;
    ensure!(claude["ok"] == true, "{claude}");
    ensure!(item_ids(&claude) == [4, 2], "{claude}");

    let gemini = bridge.exchange(json!({
        "op":"list_sessions","filters":{"provider":"gemini"},"limit":10}))?;
    ensure!(gemini["ok"] == true, "{gemini}");
    ensure!(item_ids(&gemini) == [5, 1, 3], "{gemini}");

    let all_providers = bridge.exchange(json!({
        "op":"list_sessions","filters":{"provider":"all_providers"},"limit":10}))?;
    ensure!(item_ids(&all_providers) == [4, 5, 2, 1, 3]);

    for filters in [
        json!({"project":"/data/projects/alpha"}),
        json!({"project":"alpha"}), // unique display name resolves
    ] {
        let page = bridge.exchange(json!({"op":"list_sessions","filters":filters,"limit":10}))?;
        ensure!(page["ok"] == true, "{page}");
        ensure!(item_ids(&page) == [5], "{page}");
    }

    let pinned = bridge.exchange(json!({
        "op":"list_sessions","filters":{"pinned":true},"limit":10}))?;
    ensure!(item_ids(&pinned) == [4]);
    let unpinned = bridge.exchange(json!({
        "op":"list_sessions","filters":{"pinned":false},"limit":10}))?;
    ensure!(item_ids(&unpinned) == [5, 2, 1, 3]);

    let tagged = bridge.exchange(json!({
        "op":"list_sessions","filters":{"tag":"release-blocker"},"limit":10}))?;
    ensure!(item_ids(&tagged) == [4]);
    // Raw filter text is sanitized like a write ("#Release-Blocker").
    let sanitized = bridge.exchange(json!({
        "op":"list_sessions","filters":{"tag":"#Release-Blocker"},"limit":10}))?;
    ensure!(item_ids(&sanitized) == [4], "{sanitized}");

    let local = bridge.exchange(json!({
        "op":"list_sessions","filters":{"source_id":"local"},"limit":10}))?;
    ensure!(item_ids(&local) == [4, 5, 2, 1, 3]);
    let remote = bridge.exchange(json!({
        "op":"list_sessions","filters":{"source_id":"ssh:nope"},"limit":10}))?;
    ensure!(
        remote["ok"] == true && item_ids(&remote).is_empty(),
        "{remote}"
    );

    // Activity window scoped to fully-known candidates: claude rows 4
    // (3000) and 2 (2500) inside [2400, 4500).
    let windowed = bridge.exchange(json!({
        "op":"list_sessions","limit":10,
        "filters":{"provider":"claude","activity_from_ms":2_400,"activity_to_ms":4_500}}))?;
    ensure!(item_ids(&windowed) == [4, 2], "{windowed}");

    // --- honest browse warning for unknown classification ------------------
    ensure!(
        claude["result"]["warnings"]
            .as_array()
            .is_some_and(|w| w.iter().any(|v| v == "classification_pending")),
        "browse must warn on unknown classification: {claude}"
    );

    // --- typed refusals, never silent wrong results ------------------------
    for (request, kind) in [
        // Unknown facts on an explicitly filtered classification.
        (
            json!({"op":"list_sessions","filters":{"category":"bugfix"}}),
            "facts_pending",
        ),
        (
            json!({"op":"list_sessions","filters":{"is_subagent":true}}),
            "facts_pending",
        ),
        // NULL activity under an activity window.
        (
            json!({"op":"list_sessions","filters":{"activity_from_ms":1}}),
            "data_unavailable",
        ),
        (
            json!({"op":"list_sessions","filters":{"updated_after_ms":1}}),
            "data_unavailable",
        ),
        // NULL started_at under a created window.
        (
            json!({"op":"list_sessions","filters":{"created_from_ms":0}}),
            "data_unavailable",
        ),
        // Unsupported sorts / search-lane ordering.
        (
            json!({"op":"list_sessions","sort":"relevance"}),
            "unsupported_filter",
        ),
        (
            json!({"op":"list_sessions","sort":"name_asc"}),
            "unsupported_filter",
        ),
        // Tag that normalizes to nothing.
        (
            json!({"op":"list_sessions","filters":{"tag":"###"}}),
            "unsupported_filter",
        ),
        // Malformed values.
        (
            json!({"op":"list_sessions","filters":{"provider":"%%%"}}),
            "invalid_request",
        ),
        (
            json!({"op":"list_sessions","filters":{"activity_from_ms":5,"activity_to_ms":5}}),
            "invalid_request",
        ),
        (
            json!({"op":"list_sessions","filters":{"updated_after_ms":1,"activity_from_ms":1}}),
            "invalid_request",
        ),
        // Unknown filter keys never pass silently.
        (
            json!({"op":"list_sessions","filters":{"scopes":["chat"]}}),
            "invalid_request",
        ),
        // Bad cursors.
        (
            json!({"op":"list_sessions","cursor":"bogus"}),
            "cursor_expired",
        ),
        (
            json!({"op":"list_sessions","cursor":"{}"}),
            "cursor_expired",
        ),
    ] {
        let reply = bridge.exchange(request)?;
        ensure!(reply["ok"] == false, "unexpected success: {reply}");
        ensure!(
            reply["error"]["kind"] == kind,
            "expected {kind}, got {reply}"
        );
    }

    // Unextended archives refuse the capability op explicitly.
    let mut bare = BridgeHarness::with_seed(&list_fixture_a(), &[])?;
    let refused = bare.exchange(json!({"op":"list_sessions","limit":2}))?;
    ensure!(refused["ok"] == false);
    ensure!(
        refused["error"]["kind"] == "viewer_extensions_missing",
        "{refused}"
    );
    bare.shutdown()?;
    bridge.shutdown()
}

#[test]
fn list_sessions_sorts_and_cursor_integrity() -> Result<()> {
    let mut bridge = BridgeHarness::with_seed(&list_fixture_a(), &[])?;
    init_extensions(&bridge)?;
    seed_annotations(&bridge)?;

    // created_desc: pinned first, then started_at desc, NULL last.
    let created =
        bridge.exchange(json!({"op":"list_sessions","sort":"created_desc","limit":10}))?;
    ensure!(created["ok"] == true, "{created}");
    ensure!(item_ids(&created) == [4, 2, 5, 1, 3], "{created}");

    // project_asc: pinned first, then workspace path asc, NULL last —
    // the NULL group orders by the verified public tie key, so
    // claude:BBB(2) precedes gemini:fixture-1(1) and gemini:fixture-3(3).
    let project = bridge.exchange(json!({"op":"list_sessions","sort":"project_asc","limit":10}))?;
    ensure!(project["ok"] == true, "{project}");
    ensure!(item_ids(&project) == [4, 5, 2, 1, 3], "{project}");

    // pin_first=false drops the pin ordering key.
    let flat = bridge.exchange(json!({
        "op":"list_sessions","sort":"updated_desc","pin_first":false,"limit":10}))?;
    ensure!(flat["ok"] == true, "{flat}");
    ensure!(item_ids(&flat) == [5, 4, 2, 1, 3], "{flat}");

    // A cursor only continues the sort/pin profile it was minted under.
    let first = bridge.exchange(json!({"op":"list_sessions","limit":1}))?;
    let cursor = first["result"]["next_cursor"].as_str().unwrap().to_owned();
    for (request, kind) in [
        (
            json!({"op":"list_sessions","limit":1,"sort":"created_desc","cursor":cursor}),
            "cursor_expired",
        ),
        (
            json!({"op":"list_sessions","limit":1,"pin_first":false,"cursor":cursor}),
            "cursor_expired",
        ),
    ] {
        let reply = bridge.exchange(request)?;
        ensure!(
            reply["ok"] == false && reply["error"]["kind"] == kind,
            "{reply}"
        );
    }
    // Same sort, continued: resumes exactly after conv 4.
    let second = bridge.exchange(json!({"op":"list_sessions","limit":10,"cursor":cursor}))?;
    ensure!(item_ids(&second) == [5, 2, 1, 3], "{second}");
    bridge.shutdown()
}

#[test]
fn list_sessions_classification_filters() -> Result<()> {
    // Every row classified: conv2 is a proven subagent; conv5's facts row
    // is pending with NULL task_category; conv4 has coverage 'unavailable'.
    let fixtures = (1..=5_i64)
        .map(|id| {
            SeedConversation::new(
                id,
                "gemini",
                &format!("/missing/fixture-{id}.jsonl"),
                &format!("fixture-{id}"),
            )
            .agent_messages(Some(id * 1_000), 1)
        })
        .collect::<Vec<_>>();
    let mut bridge = BridgeHarness::with_seed(&fixtures, &[])?;
    init_extensions(&bridge)?;
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    for (id, is_subagent, category, coverage) in [
        (1_i64, 0_i64, Some("bugfix"), "complete"),
        (2, 1, Some("refactor"), "complete"),
        (3, 0, Some("docs"), "complete"),
        (4, 0, Some("bugfix"), "unavailable"),
        (5, 0, None, "pending"),
    ] {
        writer.execute_with_params(
            "INSERT INTO viewer_session_facts \
             (conversation_id,extractor_version,source_revision,coverage_state,is_subagent,task_category) \
             VALUES (?1,1,1,?2,?3,?4)",
            &param_slice_to_values(params![id, coverage, is_subagent, category]),
        )?;
    }
    writer.execute(
        "INSERT INTO viewer_annotations (conversation_id,is_pinned,revision) \
         VALUES (1,1,1),(4,1,1)",
    )?;
    drop(writer);

    // Default browse: pinned rows first (4 then 1 by activity), then
    // unpinned by activity desc; only the proven subagent (conv2) drops out.
    let browse = bridge.exchange(json!({"op":"list_sessions","limit":10}))?;
    ensure!(browse["ok"] == true, "{browse}");
    ensure!(item_ids(&browse) == [4, 1, 5, 3], "{browse}");

    let included = bridge.exchange(json!({
        "op":"list_sessions","filters":{"include_subagents":true},"limit":10}))?;
    ensure!(item_ids(&included) == [4, 1, 5, 3, 2], "{included}");

    let subagents = bridge.exchange(json!({
        "op":"list_sessions","filters":{"is_subagent":true},"limit":10}))?;
    ensure!(item_ids(&subagents) == [2], "{subagents}");

    let non_subagents = bridge.exchange(json!({
        "op":"list_sessions","filters":{"is_subagent":false},"limit":10}))?;
    ensure!(item_ids(&non_subagents) == [4, 1, 5, 3], "{non_subagents}");

    // Category over fully-classified candidates (pin narrows the set to
    // rows 1 and 4, both known "bugfix").
    let category = bridge.exchange(json!({
        "op":"list_sessions","limit":10,
        "filters":{"pinned":true,"category":"bugfix"}}))?;
    ensure!(category["ok"] == true, "{category}");
    ensure!(item_ids(&category) == [4, 1], "{category}");

    // Determinable empty: pinned rows are all "bugfix"; "docs" matches
    // nothing without tripping on conv5's unknown category (excluded by
    // the pinned filter before classification ever runs).
    let none = bridge.exchange(json!({
        "op":"list_sessions","limit":10,
        "filters":{"pinned":true,"category":"docs"}}))?;
    ensure!(none["ok"] == true && item_ids(&none).is_empty(), "{none}");

    // Unknown category under an explicit filter is a typed refusal, never
    // silent exclusion: conv5's pending extraction is `facts_pending`
    // (retryable), not `data_unavailable`.
    let pending = bridge.exchange(json!({
        "op":"list_sessions","filters":{"category":"bugfix"}}))?;
    ensure!(pending["ok"] == false, "{pending}");
    ensure!(pending["error"]["kind"] == "facts_pending", "{pending}");
    bridge.shutdown()
}

#[test]
fn list_sessions_items_carry_persisted_facts_and_parent_public_id() -> Result<()> {
    // Task 7: the list page must emit every fact `viewer_session_facts`
    // already persisted plus the measured `viewer_session_metrics` fields —
    // the Python mapper can only surface what the wire actually carries.
    let fixtures = (1..=3_i64)
        .map(|id| {
            SeedConversation::new(
                id,
                "claude_code",
                &format!("/missing/fixture-{id}.jsonl"),
                &format!("fixture-{id}"),
            )
            .started_at(Some(id * 1_000))
            .agent_messages(Some(id * 2_000), 1)
        })
        .collect::<Vec<_>>();
    let mut bridge = BridgeHarness::with_seed(&fixtures, &[])?;
    init_extensions(&bridge)?;
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    // conv2 is the proven child of conv1; conv3 keeps only a facts row with
    // a known-zero payload (a measured zero must stay a zero on the wire).
    writer.execute_with_params(
        "INSERT INTO viewer_session_facts \
         (conversation_id,extractor_version,source_revision,coverage_state,is_subagent,\
          task_category,turn_count,step_count,thinking_block_count,artifact_count,\
          subagent_count,tool_error_count,tool_calls_with_exit_status,file_fact_count,\
          compaction_count,model_set_json,first_prompt,parent_conversation_id) \
         VALUES (?1,1,1,'complete',?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
        &param_slice_to_values(params![
            1_i64,
            0_i64,
            "feature",
            4_i64,
            9_i64,
            2_i64,
            3_i64,
            1_i64,
            2_i64,
            6_i64,
            5_i64,
            1_i64,
            "[\"claude-opus\",\"claude-sonnet\"]",
            "fix the flaky test",
            Option::<i64>::None,
        ]),
    )?;
    writer.execute_with_params(
        "INSERT INTO viewer_session_facts \
         (conversation_id,extractor_version,source_revision,coverage_state,is_subagent,\
          task_category,turn_count,step_count,artifact_count,subagent_count,\
          tool_error_count,model_set_json,first_prompt,parent_conversation_id) \
         VALUES (?1,1,1,'complete',?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
        &param_slice_to_values(params![
            2_i64,
            1_i64,
            "delegation",
            2_i64,
            4_i64,
            0_i64,
            0_i64,
            1_i64,
            "[\"claude-opus\"]",
            "nested task",
            1_i64,
        ]),
    )?;
    writer.execute_with_params(
        "INSERT INTO viewer_session_facts \
         (conversation_id,extractor_version,source_revision,coverage_state,is_subagent,\
          task_category,turn_count,step_count,artifact_count,subagent_count,\
          tool_error_count) \
         VALUES (?1,1,1,'complete',?2,?3,?4,?5,?6,?7,?8)",
        &param_slice_to_values(params![
            3_i64,
            0_i64,
            Option::<&str>::None,
            0_i64,
            0_i64,
            0_i64,
            0_i64,
            0_i64,
        ]),
    )?;
    coding_agent_search::viewer_extensions::facts::ensure_metrics_schema(&writer)?;
    writer.execute_with_params(
        "INSERT INTO viewer_session_metrics \
         (conversation_id,extractor_version,source_revision,ttfe_seconds,one_shot_rate,\
          retries_count,self_corrections_count,user_corrections_count,tools_used_json,\
          health_score,health_grade,health_issues_count,potential_waste_tokens,\
          potential_savings_usd,computed_at_ms) \
         VALUES (?1,1,1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,1700000000000)",
        &param_slice_to_values(params![
            1_i64,
            12.5_f64,
            80.0_f64,
            1_i64,
            0_i64,
            0_i64,
            "[\"Bash\",\"Read\"]",
            82_i64,
            "B",
            1_i64,
            400_i64,
            0.02_f64,
        ]),
    )?;
    writer.execute(
        "UPDATE conversations SET total_input_tokens=800,total_output_tokens=150,\
         total_cache_read_tokens=50,total_cache_creation_tokens=25,grand_total_tokens=1025 \
         WHERE id=1",
    )?;
    drop(writer);

    let page = bridge.exchange(json!({
        "op":"list_sessions","filters":{"include_subagents":true},
        "sort":"created_asc","pin_first":false,"limit":10}))?;
    ensure!(page["ok"] == true, "{page}");
    let first = item(&page, 0);
    // Canonical token subfields ride the same row — never dropped.
    ensure!(first["total_input_tokens"] == 800, "{first}");
    ensure!(first["total_output_tokens"] == 150, "{first}");
    ensure!(first["total_cache_read_tokens"] == 50, "{first}");
    ensure!(first["total_cache_creation_tokens"] == 25, "{first}");
    // The persisted fact/metrics bag is the delivery channel.
    let metrics = &first["metrics"];
    ensure!(
        metrics["turn_count"] == 4 && metrics["step_count"] == 9,
        "{metrics}"
    );
    ensure!(
        metrics["thinking_block_count"] == 2 && metrics["artifact_count"] == 3,
        "{metrics}"
    );
    ensure!(
        metrics["subagent_count"] == 1 && metrics["tool_error_count"] == 2,
        "{metrics}"
    );
    ensure!(
        metrics["tool_calls_with_exit_status"] == 6 && metrics["file_fact_count"] == 5,
        "{metrics}"
    );
    ensure!(metrics["compaction_count"] == 1, "{metrics}");
    ensure!(metrics["first_prompt"] == "fix the flaky test", "{metrics}");
    ensure!(
        metrics["model_set"] == json!(["claude-opus", "claude-sonnet"]),
        "{metrics}"
    );
    ensure!(
        metrics["tools_used"] == json!(["Bash", "Read"]),
        "{metrics}"
    );
    ensure!(
        metrics["health_score"] == 82 && metrics["health_grade"] == "B",
        "{metrics}"
    );
    ensure!(metrics["health_issues_count"] == 1, "{metrics}");
    ensure!(metrics["potential_waste_tokens"] == 400, "{metrics}");
    ensure!(metrics["potential_savings_usd"] == 0.02, "{metrics}");
    ensure!(
        metrics["ttfe_seconds"] == 12.5 && metrics["one_shot_rate"] == 80.0,
        "{metrics}"
    );
    ensure!(metrics["retries_count"] == 1, "{metrics}");

    // The proven child's persisted parent link resolves to the parent's
    // verified public id inside the same snapshot.
    let child = item(&page, 1);
    ensure!(child["is_subagent"] == true, "{child}");
    ensure!(child["parent_conversation_id"] == 1, "{child}");
    ensure!(child["parent_public_id"] == "claude:fixture-1", "{child}");
    ensure!(child["metrics"]["first_prompt"] == "nested task", "{child}");

    // A persisted known-zero payload stays zero — unknown would be NULL.
    let zeros = item(&page, 2);
    ensure!(zeros["metrics"]["step_count"] == 0, "{zeros}");
    ensure!(zeros["metrics"]["subagent_count"] == 0, "{zeros}");
    ensure!(zeros["metrics"]["tool_error_count"] == 0, "{zeros}");
    bridge.shutdown()
}

#[test]
fn list_sessions_items_without_facts_emit_null_metrics() -> Result<()> {
    // No facts row → `metrics: null` (unknown), never an all-zero object.
    let fixtures = (1..=2_i64)
        .map(|id| {
            SeedConversation::new(
                id,
                "claude_code",
                &format!("/missing/fixture-{id}.jsonl"),
                &format!("fixture-{id}"),
            )
            .agent_messages(Some(id * 1_000), 1)
        })
        .collect::<Vec<_>>();
    let mut bridge = BridgeHarness::with_seed(&fixtures, &[])?;
    init_extensions(&bridge)?;
    let page = bridge.exchange(json!({"op":"list_sessions","limit":10}))?;
    ensure!(page["ok"] == true, "{page}");
    ensure!(item(&page, 0)["metrics"].is_null(), "{page}");
    ensure!(
        item(&page, 0)["classification_coverage"].is_null(),
        "{page}"
    );
    bridge.shutdown()
}

#[test]
fn list_sessions_tag_patch_reflected_without_reindex() -> Result<()> {
    let mut bridge = BridgeHarness::with_seed(&list_fixture_a(), &["--enable-viewer-writes"])?;
    init_extensions(&bridge)?;
    seed_annotations(&bridge)?;

    // Tag set replacement through the write lane (patch is the only way
    // tags change; sanitization mirrors the writer).
    let patch = bridge.exchange(json!({
        "op":"patch_annotations","conversation_id":1,"expected_revision":0,
        "patch":{"tags":["urgent","My Tag!","urgent"]}}))?;
    ensure!(patch["ok"] == true, "{patch}");
    ensure!(
        patch["result"]["annotation"]["tags"] == json!(["my-tag", "urgent"]),
        "tags must be sanitized + deduped + canonically ordered: {patch}"
    );
    ensure!(patch["result"]["annotation"]["revision"] == 1);

    // The list path sees it immediately — same snapshot, no reindex.
    for tag in ["urgent", "my-tag"] {
        let page = bridge.exchange(json!({
            "op":"list_sessions","filters":{"tag":tag},"limit":10}))?;
        ensure!(item_ids(&page) == [1], "tag {tag}: {page}");
    }
    let still_tagged = bridge.exchange(json!({
        "op":"list_sessions","filters":{"tag":"release-blocker"},"limit":10}))?;
    ensure!(item_ids(&still_tagged) == [4]);

    // get_annotations reports the tag set through the read lane.
    let annotations = bridge.exchange(json!({
        "op":"get_annotations","conversation_id":1}))?;
    ensure!(
        annotations["result"]["annotation"]["tags"] == json!(["my-tag", "urgent"]),
        "{annotations}"
    );

    // public_id addressing: resolve → patch → read back.
    let patched = bridge.exchange(json!({
        "op":"patch_annotations","public_id":"gemini:fixture-1","expected_revision":1,
        "patch":{"notes":"via public id"}}))?;
    ensure!(patched["ok"] == true, "{patched}");
    ensure!(patched["result"]["annotation"]["conversation_id"] == 1);
    ensure!(patched["result"]["annotation"]["notes"] == "via public id");
    // Tags survive an unrelated field patch (absent key = untouched).
    ensure!(patched["result"]["annotation"]["tags"] == json!(["my-tag", "urgent"]));

    let by_public = bridge.exchange(json!({
        "op":"get_annotations","public_id":"gemini:fixture-1"}))?;
    ensure!(by_public["ok"] == true, "{by_public}");
    ensure!(by_public["result"]["annotation"]["notes"] == "via public id");

    // Neither-both addressing and CAS conflicts stay typed.
    for (request, kind) in [
        (json!({"op":"get_annotations"}), "invalid_request"),
        (
            json!({"op":"get_annotations","conversation_id":1,"public_id":"gemini:fixture-1"}),
            "invalid_request",
        ),
        (
            json!({"op":"patch_annotations","public_id":"gemini:nope","expected_revision":0,
                "patch":{"is_pinned":true}}),
            "unknown_conversation",
        ),
        (
            json!({"op":"patch_annotations","conversation_id":1,"expected_revision":0,
                "patch":{"is_pinned":true}}),
            "revision_conflict",
        ),
    ] {
        let reply = bridge.exchange(request)?;
        ensure!(
            reply["ok"] == false && reply["error"]["kind"] == kind,
            "{reply}"
        );
    }

    // Clearing the tag set removes it from the list filter immediately.
    let cleared = bridge.exchange(json!({
        "op":"patch_annotations","conversation_id":1,"expected_revision":2,
        "patch":{"tags":[]}}))?;
    ensure!(cleared["ok"] == true, "{cleared}");
    let gone = bridge.exchange(json!({
        "op":"list_sessions","filters":{"tag":"urgent"},"limit":10}))?;
    ensure!(item_ids(&gone).is_empty(), "{gone}");
    bridge.shutdown()
}

/// Opcode-level plan audit: `EXPLAIN <sql>` returns the VDBE program, so a
/// page whose order comes from a sorter shows `SorterOpen` (and `TEMP
/// B-TREE` in the query plan). The persisted sort tuple exists precisely
/// so neither appears on the browse path — this is the evidence that the
/// page is index-bounded rather than a full-candidate sort.
fn assert_index_bounded_plan(
    connection: &Connection,
    label: &str,
    sql: &str,
    expect_index: &str,
) -> Result<()> {
    let opcodes: Vec<String> = connection
        .query(&format!("EXPLAIN {sql}"))?
        .iter()
        .map(|row| row.get_typed::<String>(1).unwrap_or_default())
        .collect();
    eprintln!("OPCODES [{label}]: {opcodes:?}");
    ensure!(
        !opcodes.iter().any(|op| op == "SorterOpen"),
        "order scan must be index-satisfied, never a sorter pass [{label}]: {opcodes:?}"
    );
    let plan: Vec<String> = connection
        .query(&format!("EXPLAIN QUERY PLAN {sql}"))?
        .iter()
        .map(|row| row.get_typed::<String>(3).unwrap_or_default())
        .collect();
    let joined = plan.join(" | ");
    eprintln!("EQP [{label}]: {joined}");
    ensure!(
        !joined.to_uppercase().contains("TEMP B-TREE"),
        "no materialized sorter table on the order scan [{label}]: {joined}"
    );
    ensure!(
        joined.contains(expect_index),
        "order scan must walk the persisted sort index [{label}]: {joined}"
    );
    Ok(())
}

/// `EXPLAIN QUERY PLAN` detail lines for the hydration probe, joined for
/// assertion/error reporting.
fn hydrate_sql_plan(connection: &Connection, sql: &str) -> Result<String> {
    let plan: Vec<String> = connection
        .query(&format!("EXPLAIN QUERY PLAN {sql}"))?
        .iter()
        .map(|row| row.get_typed::<String>(3).unwrap_or_default())
        .collect();
    Ok(plan.join(" | "))
}

#[test]
fn list_sessions_query_plan_audit() -> Result<()> {
    // The browse page is index-bounded: the order scan walks the persisted
    // `viewer_session_sort` composite index (opcode-level proof: no
    // `SorterOpen`, no TEMP B-TREE) and the payload hydration joins
    // conversations for exactly the page's ids — never `messages`.
    let mut bridge = BridgeHarness::with_seed(&list_fixture_a(), &[])?;
    init_extensions(&bridge)?;
    seed_annotations(&bridge)?;

    let connection = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    for (label, filters, sort, pin_first) in [
        (
            "default browse",
            protocol::ListFilters::default(),
            None,
            Some(true),
        ),
        (
            "provider+window",
            serde_json::from_value::<protocol::ListFilters>(json!({
                "provider":"gemini","activity_from_ms":2_000
            }))?,
            None,
            Some(true),
        ),
        (
            "category",
            serde_json::from_value::<protocol::ListFilters>(json!({
                "category":"bugfix"
            }))?,
            None,
            Some(true),
        ),
        (
            "project_asc",
            protocol::ListFilters::default(),
            Some("project_asc"),
            Some(true),
        ),
        (
            "flat pin_first=false",
            protocol::ListFilters::default(),
            None,
            Some(false),
        ),
        (
            "step_desc",
            protocol::ListFilters::default(),
            Some("step_desc"),
            Some(true),
        ),
        (
            "artifact_desc",
            protocol::ListFilters::default(),
            Some("artifact_desc"),
            Some(true),
        ),
        (
            "created_asc",
            protocol::ListFilters::default(),
            Some("created_asc"),
            Some(true),
        ),
    ] {
        let planned = query::planned_page_sql(&connection, &filters, sort, pin_first, None, 10)?;
        let expected_index = match sort {
            Some("project_asc") => "idx_viewer_sort_project_asc",
            Some("step_desc") => "idx_viewer_sort_steps_desc",
            Some("artifact_desc") => "idx_viewer_sort_artifacts_desc",
            Some("created_asc") => "idx_viewer_sort_created_asc",
            _ => "idx_viewer_sort_activity_desc",
        };
        for (i, (sql, params)) in planned.order_queries.iter().enumerate() {
            eprintln!("ORDER SQL [{label}#{i}] ({} params): {sql}", params.len());
            assert_index_bounded_plan(
                &connection,
                &format!("{label} order#{i}"),
                sql,
                expected_index,
            )?;
        }
        // Hydration is a bounded `WHERE c.id IN (page ids)` probe —
        // FrankenSQLite resolves a standalone-table IN list to repeated
        // `SeekRowid` lookups (scratch probe `in-list`), and the audit
        // pins that probe shape plus the no-messages guarantee.
        ensure!(
            planned
                .hydrate_query
                .contains("conversations c WHERE c.id IN ("),
            "hydration must be a bounded primary-key IN probe: {}",
            planned.hydrate_query
        );
        ensure!(
            !planned.hydrate_query.to_uppercase().contains("MESSAGES"),
            "browse hydration must never touch the messages table"
        );
        // The probe shape with a real page-id list: repeated rowid seeks.
        let hydrated_probe = planned.hydrate_query.replace("?1..?N", "?1,?2");
        let joined = hydrate_sql_plan(&connection, &hydrated_probe)?;
        eprintln!("EQP [{label} hydrate]: {joined}");
        ensure!(
            !joined.to_uppercase().contains("MESSAGES"),
            "browse hydration must never touch the messages table: {joined}"
        );
    }
    drop(connection);

    // Rough timing at fixture scale: the whole op is a handful of small
    // statements; report, don't gate.
    let start = std::time::Instant::now();
    for _ in 0..20 {
        let page = bridge.exchange(json!({"op":"list_sessions","limit":2}))?;
        ensure!(page["ok"] == true);
    }
    eprintln!(
        "list_sessions limit=2 over 5 rows: {:?}/page (20 pages)",
        start.elapsed() / 20
    );
    bridge.shutdown()
}

// ---------------------------------------------------------------------------
// Task 4: index-bounded browse at realistic scale (50k exact-order walk)
// ---------------------------------------------------------------------------

/// Mirror of one `viewer_session_sort` row for the test-side reference
/// oracle: the small fixtures pin the shared order semantics against
/// literal id sequences; this walk proves the SQL index scan returns
/// exactly that declared order over a 50k candidate set — no drift, no
/// duplicates, no gaps, cursor-resumable.
#[derive(Debug)]
struct ScaleSortRow {
    id: i64,
    pin: i64,
    activity_known: i64,
    activity_ms: Option<i64>,
    created_known: i64,
    created_ms: Option<i64>,
    project_known: i64,
    project_path: Option<String>,
    steps_known: i64,
    step_count: Option<i64>,
    artifacts_known: i64,
    artifact_count: Option<i64>,
    tie: String,
    is_subagent: Option<i64>,
}

/// Bulk-seed a 50k-scale canonical archive directly through SQL. Every row
/// is engineered to stress the order tuple: interleaved providers make the
/// public tie key disagree with internal insertion order, `started_at` and
/// `last_message_created_at` collide hard (`i % 97`/`i % 131`) so equal
/// values exercise `(tie, id)` tails on every page, every 11th row falls
/// back to `ended_at`/`started_at` activity, every 23rd is fully unknown,
/// and a third of the rows carry no workspace (project unknown).
fn seed_sort_scale_canonical(db: &Path, count: usize) -> Result<()> {
    let conn = Connection::open(db.to_string_lossy().into_owned())?;
    conn.execute("BEGIN")?;
    conn.execute(
        "INSERT OR IGNORE INTO sources (id,kind,created_at,updated_at) \
         VALUES ('local','local',0,0)",
    )?;
    for slug in ["claude_code", "gemini"] {
        conn.execute_with_params(
            "INSERT INTO agents (slug,name,kind,created_at,updated_at) \
             VALUES (?1,?1,'cli',0,0)",
            &param_slice_to_values(params![slug]),
        )?;
    }
    for i in 0..8_i64 {
        conn.execute_with_params(
            "INSERT INTO workspaces (path,display_name) VALUES (?1,?2)",
            &param_slice_to_values(params![format!("/scale/proj-{i}"), format!("proj-{i}")]),
        )?;
    }
    // One prepared insert: at 50k rows the per-statement parse dominates.
    let insert = conn.prepare(
        "INSERT INTO conversations \
         (id,agent_id,workspace_id,source_id,external_id,title,source_path,\
          started_at,ended_at,metadata_json,last_message_idx,last_message_created_at) \
         VALUES (?1,?2,?3,'local',?4,?5,?6,?7,?8,'{}',?9,?10)",
    )?;
    for i in 0..count as i64 {
        let external = format!("sess-{i:06}");
        let fully_unknown = i % 23 == 0;
        let (started, ended, lmca) = if fully_unknown {
            (None, None, None)
        } else {
            (
                Some(1_000_000 + (i % 97)),
                (i % 29 == 0).then_some(3_000_000 + (i % 53)),
                if i % 11 == 0 {
                    None
                } else {
                    Some(2_000_000 + (i % 131))
                },
            )
        };
        insert.execute_with_params(&param_slice_to_values(params![
            i + 1,
            if i % 2 == 0 { 1_i64 } else { 2_i64 },
            if i % 3 == 0 {
                SqliteValue::Null
            } else {
                SqliteValue::Integer((i % 8) + 1)
            },
            external,
            format!("scale-{i}"),
            format!("/scale/sess-{i:06}.jsonl"),
            match started {
                Some(v) => SqliteValue::Integer(v),
                None => SqliteValue::Null,
            },
            match ended {
                Some(v) => SqliteValue::Integer(v),
                None => SqliteValue::Null,
            },
            match lmca {
                Some(_) => SqliteValue::Integer(0),
                None => SqliteValue::Null,
            },
            match lmca {
                Some(v) => SqliteValue::Integer(v),
                None => SqliteValue::Null,
            },
        ]))?;
    }
    conn.execute("COMMIT")?;
    drop(conn);
    Ok(())
}

/// Post-init facts/annotation writes at scale: the v5 triggers must mirror
/// classification and count sort values into `viewer_session_sort` for
/// every row, and the annotation trigger must flip `is_pinned` — this is
/// the maintenance lane exercised at scale (the migration backfill covers
/// the canonical columns; these writes cover the trigger path).
fn seed_sort_scale_extensions(db: &Path, count: usize) -> Result<()> {
    let conn = Connection::open(db.to_string_lossy().into_owned())?;
    conn.execute("BEGIN")?;
    let insert = conn.prepare(
        "INSERT INTO viewer_session_facts \
         (conversation_id,extractor_version,source_revision,coverage_state,\
          is_subagent,task_category,step_count,artifact_count) \
         VALUES (?1,1,1,'complete',?2,'chore',?3,?4)",
    )?;
    for i in 0..count as i64 {
        if !(i % 5 == 0 || i % 7 == 0 || i % 13 == 0 || i % 37 == 0) {
            continue;
        }
        insert.execute_with_params(&param_slice_to_values(params![
            i + 1,
            (i % 37 == 0) as i64,
            if i % 5 == 0 {
                SqliteValue::Integer(i % 41)
            } else {
                SqliteValue::Null
            },
            if i % 7 == 0 {
                SqliteValue::Integer(i % 17)
            } else {
                SqliteValue::Null
            },
        ]))?;
    }
    for id in [100_i64, 25_001, 49_999] {
        conn.execute_with_params(
            "INSERT INTO viewer_annotations (conversation_id,is_pinned,revision) \
             VALUES (?1,1,1)",
            &param_slice_to_values(params![id]),
        )?;
    }
    conn.execute("COMMIT")?;
    drop(conn);
    Ok(())
}

fn load_scale_sort_rows(db: &Path) -> Result<Vec<ScaleSortRow>> {
    let conn = Connection::open(db.to_string_lossy().into_owned())?;
    let mut rows = Vec::new();
    for row in conn.query(
        "SELECT conversation_id, is_pinned, \
         activity_known, activity_ms, created_known, created_ms, \
         project_known, project_path, steps_known, step_count, \
         artifacts_known, artifact_count, tie_key, is_subagent \
         FROM viewer_session_sort",
    )? {
        rows.push(ScaleSortRow {
            id: row.get_typed(0)?,
            pin: row.get_typed(1)?,
            activity_known: row.get_typed(2)?,
            activity_ms: row.get_typed(3)?,
            created_known: row.get_typed(4)?,
            created_ms: row.get_typed(5)?,
            project_known: row.get_typed(6)?,
            project_path: row.get_typed(7)?,
            steps_known: row.get_typed(8)?,
            step_count: row.get_typed(9)?,
            artifacts_known: row.get_typed(10)?,
            artifact_count: row.get_typed(11)?,
            tie: row.get_typed::<Option<String>>(12)?.unwrap_or_default(),
            is_subagent: row.get_typed(13)?,
        });
    }
    drop(conn);
    Ok(rows)
}

/// Sortable order value across the INTEGER and TEXT sort axes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum ScaleValue {
    Int(i64),
    Text(String),
    Null,
}

/// The oracle's explicit order: `(pin, knownness DESC, value <dir>, public
/// tie ASC, conversation_id ASC)` over the filtered candidate set
/// (default browse excludes `is_subagent = 1`).
fn expected_scale_order(rows: &[ScaleSortRow], sort: &str, pin_first: bool) -> Vec<i64> {
    let key_of = |row: &ScaleSortRow| -> (i64, ScaleValue) {
        match sort {
            "updated_asc" => (
                row.activity_known,
                row.activity_ms
                    .map(ScaleValue::Int)
                    .unwrap_or(ScaleValue::Null),
            ),
            "created_desc" => (
                row.created_known,
                row.created_ms
                    .map(ScaleValue::Int)
                    .unwrap_or(ScaleValue::Null),
            ),
            "created_asc" => (
                row.created_known,
                row.created_ms
                    .map(ScaleValue::Int)
                    .unwrap_or(ScaleValue::Null),
            ),
            "project_asc" => (
                row.project_known,
                row.project_path
                    .clone()
                    .map(ScaleValue::Text)
                    .unwrap_or(ScaleValue::Null),
            ),
            "step_desc" => (
                row.steps_known,
                row.step_count
                    .map(ScaleValue::Int)
                    .unwrap_or(ScaleValue::Null),
            ),
            "artifact_desc" => (
                row.artifacts_known,
                row.artifact_count
                    .map(ScaleValue::Int)
                    .unwrap_or(ScaleValue::Null),
            ),
            _ => (
                row.activity_known,
                row.activity_ms
                    .map(ScaleValue::Int)
                    .unwrap_or(ScaleValue::Null),
            ),
        }
    };
    let desc = matches!(
        sort,
        "created_desc" | "step_desc" | "artifact_desc" | "updated_desc"
    );
    let mut ordered: Vec<&ScaleSortRow> = rows
        .iter()
        .filter(|row| row.is_subagent.unwrap_or(0) == 0)
        .collect();
    ordered.sort_by(|a, b| {
        if pin_first {
            match b.pin.cmp(&a.pin) {
                std::cmp::Ordering::Equal => {}
                other => return other,
            }
        }
        let (ka, va) = key_of(a);
        let (kb, vb) = key_of(b);
        match kb.cmp(&ka) {
            std::cmp::Ordering::Equal => {}
            other => return other,
        }
        let directed = if desc { vb.cmp(&va) } else { va.cmp(&vb) };
        match directed {
            std::cmp::Ordering::Equal => {}
            other => return other,
        }
        match a.tie.cmp(&b.tie) {
            std::cmp::Ordering::Equal => a.id.cmp(&b.id),
            other => other,
        }
    });
    ordered.iter().map(|row| row.id).collect()
}

/// Diff two id sequences for diagnostics: report the first position they
/// diverge plus the surrounding slice instead of dumping 50k ids.
fn first_divergence(expected: &[i64], seen: &[i64]) -> String {
    let len = expected.len().min(seen.len());
    for i in 0..len {
        if expected[i] != seen[i] {
            let lo = i.saturating_sub(3);
            return format!(
                "first divergence at position {i}: expected[{lo}..]={:?} \
                 seen[{lo}..]={:?} (expected len {}, seen len {})",
                &expected[lo..(i + 4).min(expected.len())],
                &seen[lo..(i + 4).min(seen.len())],
                expected.len(),
                seen.len()
            );
        }
    }
    format!(
        "prefixes match over {len}; expected len {}, seen len {}",
        expected.len(),
        seen.len()
    )
}

#[test]
fn list_sessions_sort_index_50k_exact_order_and_bounded_work() -> Result<()> {
    const COUNT: usize = 50_000;
    let mut bridge = BridgeHarness::with_seed(&[], &[])?;
    seed_sort_scale_canonical(&bridge.db, COUNT)?;

    // The maintenance lane at scale: one v5 migration backfill plus the
    // repair pass over 50k pending tie keys.
    let init_start = std::time::Instant::now();
    init_extensions(&bridge)?;
    let init_ms = init_start.elapsed().as_millis();

    seed_sort_scale_extensions(&bridge.db, COUNT)?;
    let rows = load_scale_sort_rows(&bridge.db)?;
    ensure!(rows.len() == COUNT, "sort rows: {} != {COUNT}", rows.len());
    ensure!(
        rows.iter().all(|row| !row.tie.is_empty()),
        "every sort row must carry a resolved public tie key"
    );

    // ---- plan audit at scale: every supported sort walks its index ------
    let connection = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    for (sort, expected_index) in [
        (None, "idx_viewer_sort_activity_desc"),
        (Some("updated_asc"), "idx_viewer_sort_activity_asc"),
        (Some("created_desc"), "idx_viewer_sort_created_desc"),
        (Some("created_asc"), "idx_viewer_sort_created_asc"),
        (Some("project_asc"), "idx_viewer_sort_project_asc"),
        (Some("step_desc"), "idx_viewer_sort_steps_desc"),
        (Some("artifact_desc"), "idx_viewer_sort_artifacts_desc"),
    ] {
        let planned = query::planned_page_sql(
            &connection,
            &protocol::ListFilters::default(),
            sort,
            Some(true),
            None,
            200,
        )?;
        for (i, (sql, _params)) in planned.order_queries.iter().enumerate() {
            assert_index_bounded_plan(
                &connection,
                &format!("50k {sort:?}#{i}"),
                sql,
                expected_index,
            )?;
        }
    }

    // ---- full cursor traversal of the default sort ---------------------
    let expected = expected_scale_order(&rows, "updated_desc", true);
    eprintln!("50k candidates: {}", expected.len());
    let mut cursor: Option<String> = None;
    let mut seen: Vec<i64> = Vec::with_capacity(expected.len());
    let mut pages = 0_usize;
    let mut first_ms = None;
    let mut last_ms: std::time::Duration;
    let mut slowest = std::time::Duration::ZERO;
    loop {
        let request = match &cursor {
            Some(c) => json!({"op":"list_sessions","limit":200,"cursor":c}),
            None => json!({"op":"list_sessions","limit":200}),
        };
        let start = std::time::Instant::now();
        let page = bridge.exchange(request)?;
        let elapsed = start.elapsed();
        ensure!(page["ok"] == true, "walk page {pages}: {page}");
        if pages == 0 {
            first_ms = Some(elapsed);
            ensure!(
                page["result"]["warnings"]
                    .as_array()
                    .is_some_and(|w| w.iter().any(|v| v == "classification_pending")),
                "unclassified rows must surface the browse warning: {page}"
            );
        }
        seen.extend(item_ids(&page));
        slowest = slowest.max(elapsed);
        last_ms = elapsed;
        match page["result"]["next_cursor"].as_str() {
            Some(next) => cursor = Some(next.to_owned()),
            None => break,
        }
        pages += 1;
    }
    eprintln!(
        "50k updated_desc walk: {pages} pages, {} rows, page1 {:?}, last page {:?}, slowest {:?}",
        seen.len(),
        first_ms.unwrap_or_default(),
        last_ms,
        slowest
    );
    ensure!(
        seen == expected,
        "50k walk must return the oracle order exactly: {}",
        first_divergence(&expected, &seen)
    );

    // ---- deep keyset walks: EVERY supported explicit sort to page 10 ---
    // Page size 20, ten pages per sort: the page-10 slice must equal the
    // oracle's `expected[180..200]` exactly, proving the persisted keyset
    // cursor stays faithful to the oracle order at depth — not just in
    // the cheap head pages — for every supported sort, including the two
    // ascending directions the old spot-walk never exercised
    // (`updated_asc`, `created_asc`).
    for (sort, pin_first, label) in [
        (Some("updated_desc"), true, "explicit updated_desc"),
        (Some("updated_asc"), true, "updated_asc"),
        (Some("created_desc"), true, "created_desc"),
        (Some("created_asc"), true, "created_asc"),
        (Some("project_asc"), true, "project_asc"),
        (Some("step_desc"), true, "step_desc"),
        (Some("artifact_desc"), true, "artifact_desc"),
        (None, false, "flat pin_first=false"),
    ] {
        let expected = expected_scale_order(&rows, sort.unwrap_or("updated_desc"), pin_first);
        let mut seen: Vec<i64> = Vec::with_capacity(200);
        let mut cursor: Option<String> = None;
        let mut pages = 0_usize;
        let mut page10: Vec<i64> = Vec::new();
        loop {
            let mut request = json!({
                "op":"list_sessions","limit":20,"pin_first":pin_first});
            if let Some(sort) = sort {
                request["sort"] = json!(sort);
            }
            if let Some(c) = &cursor {
                request["cursor"] = json!(c);
            }
            let page = bridge.exchange(request)?;
            ensure!(page["ok"] == true, "{label} page {}: {page}", pages + 1);
            let ids = item_ids(&page);
            seen.extend_from_slice(&ids);
            if pages == 9 {
                page10 = ids;
            }
            pages += 1;
            cursor = page["result"]["next_cursor"].as_str().map(str::to_owned);
            if pages == 10 || cursor.is_none() {
                break;
            }
        }
        ensure!(
            pages == 10,
            "{label} must reach page-10 depth over the scale set ({pages} pages)"
        );
        let oracle_slice = &expected[180..200];
        ensure!(
            page10 == oracle_slice,
            "{label} page 10 must equal expected[180..200] exactly: \
             page10={page10:?} oracle={oracle_slice:?}"
        );
        let head: &[i64] = &expected[..200];
        ensure!(
            seen == head,
            "{label} pages 1-10 must match the oracle prefix exactly: {}",
            first_divergence(head, &seen)
        );
    }

    // ---- a mid-archive keyset resume: mint a cursor, continue exactly --
    let first = bridge.exchange(json!({"op":"list_sessions","limit":200}))?;
    let cursor = first["result"]["next_cursor"].as_str().map(str::to_owned);
    let start = std::time::Instant::now();
    let page = bridge.exchange(json!({
        "op":"list_sessions","limit":200,
        "cursor":cursor.as_deref().expect("page-1 cursor")
    }))?;
    let second_ms = start.elapsed();
    ensure!(page["ok"] == true, "keyset page: {page}");
    let head: Vec<i64> = expected[200..400.min(expected.len())].to_vec();
    ensure!(
        item_ids(&page) == head,
        "the cursor resume must continue the oracle order exactly: {}",
        first_divergence(&head, &item_ids(&page))
    );
    eprintln!("50k walk: init {init_ms}ms, keyset page latency {second_ms:?}");

    // ---- keyset order scan at scale stays on the composite index -------
    let connection2 = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    let planned = query::planned_page_sql(
        &connection2,
        &protocol::ListFilters::default(),
        None,
        Some(true),
        cursor.as_deref(),
        200,
    )?;
    for (i, (sql, _)) in planned.order_queries.iter().enumerate() {
        assert_index_bounded_plan(
            &connection2,
            &format!("50k keyset#{i}"),
            sql,
            "idx_viewer_sort_activity_desc",
        )?;
    }
    drop(connection2);
    drop(connection);
    bridge.shutdown()
}

/// Smaller-scale engine-work evidence for the scaling comparison the
/// brief asks for (1k/10k here, 50k in the exact-order test above — its
/// per-page latencies print to stderr under `--nocapture`). Each scale
/// asserts the same composite-index plan and records seed/init/page
/// timings so the report can compare observed work honestly.
#[test]
fn list_sessions_sort_index_scaling_evidence() -> Result<()> {
    for count in [1_000_usize, 10_000] {
        let mut bridge = BridgeHarness::with_seed(&[], &[])?;
        let seed_start = std::time::Instant::now();
        seed_sort_scale_canonical(&bridge.db, count)?;
        let seed_ms = seed_start.elapsed().as_millis();
        let init_start = std::time::Instant::now();
        init_extensions(&bridge)?;
        let init_ms = init_start.elapsed().as_millis();
        seed_sort_scale_extensions(&bridge.db, count)?;

        let connection = Connection::open(bridge.db.to_string_lossy().into_owned())?;
        let planned = query::planned_page_sql(
            &connection,
            &protocol::ListFilters::default(),
            None,
            Some(true),
            None,
            200,
        )?;
        for (i, (sql, _)) in planned.order_queries.iter().enumerate() {
            assert_index_bounded_plan(
                &connection,
                &format!("{count}k#{i}"),
                sql,
                "idx_viewer_sort_activity_desc",
            )?;
        }
        drop(connection);

        // Two pages through the live bridge — page latency is the
        // scale-invariant unit of work the index bounds.
        let start = std::time::Instant::now();
        let first = bridge.exchange(json!({"op":"list_sessions","limit":200}))?;
        let first_ms = start.elapsed();
        ensure!(first["ok"] == true, "{count} page 1: {first}");
        let cursor = first["result"]["next_cursor"].as_str().map(str::to_owned);
        ensure!(cursor.is_some(), "{count} must have a second page");
        let start = std::time::Instant::now();
        let second = bridge.exchange(json!({
            "op":"list_sessions","limit":200,
            "cursor":cursor.as_deref().expect("page-1 cursor")
        }))?;
        let second_ms = start.elapsed();
        ensure!(second["ok"] == true, "{count} page 2: {second}");
        ensure!(
            item_ids(&second).len() == 200,
            "{count} page 2 must be full: {}",
            item_ids(&second).len()
        );
        eprintln!(
            "scale {count}: seed {seed_ms}ms, init {init_ms}ms, \
             page1 {first_ms:?}, page2 {second_ms:?}"
        );
        bridge.shutdown()?;
    }
    Ok(())
}

#[test]
fn list_sessions_browse_refuses_unresolved_tie_keys() -> Result<()> {
    // The oracle's explicit order needs a verified public tie key for every
    // candidate: a pending key is `facts_pending`, a persisted resolver
    // refusal surfaces its own kind, and a filtered-out candidate cannot
    // refuse for rows it never ordered.
    let mut bridge = BridgeHarness::with_seed(&list_fixture_a(), &[])?;
    init_extensions(&bridge)?;
    seed_annotations(&bridge)?;

    let conn = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    conn.execute(
        "UPDATE viewer_session_sort SET tie_key = NULL, tie_error = NULL \
         WHERE conversation_id = 3",
    )?;
    let refused = bridge.exchange(json!({"op":"list_sessions","limit":10}))?;
    ensure!(refused["ok"] == false);
    ensure!(
        refused["error"]["kind"] == "facts_pending",
        "pending tie key must refuse facts_pending: {refused}"
    );

    // A provider filter that excludes the pending row still serves —
    // the refusal scope is the candidate set, not the table.
    let claude = bridge.exchange(json!({
        "op":"list_sessions","filters":{"provider":"claude"},"limit":10}))?;
    ensure!(
        claude["ok"] == true,
        "filtered-out tie must not refuse: {claude}"
    );
    ensure!(item_ids(&claude) == [4, 2], "{claude}");

    // A persisted resolver refusal surfaces its own kind.
    conn.execute(
        "UPDATE viewer_session_sort SET tie_error = 'identity_conflict' \
         WHERE conversation_id = 3",
    )?;
    let refused = bridge.exchange(json!({"op":"list_sessions","limit":10}))?;
    ensure!(refused["ok"] == false);
    ensure!(
        refused["error"]["kind"] == "identity_conflict",
        "persisted resolver kind must surface: {refused}"
    );

    // The maintenance lane repairs the row; browse serves again.
    conn.execute(
        "UPDATE viewer_session_sort \
         SET tie_key = 'gemini:fixture-3', tie_error = NULL \
         WHERE conversation_id = 3",
    )?;
    let page = bridge.exchange(json!({"op":"list_sessions","limit":10}))?;
    ensure!(page["ok"] == true, "repaired tie must serve: {page}");
    ensure!(item_ids(&page) == [4, 5, 2, 1, 3], "{page}");
    drop(conn);
    bridge.shutdown()
}

#[test]
fn list_sessions_sort_mirrors_pin_canonical_and_facts_changes() -> Result<()> {
    // Trigger maintenance across every mirror source: an annotation pin
    // moves the row to the pin partition immediately, a canonical
    // started_at update moves it inside created_desc, a facts write moves
    // it inside step_desc, and an identity-bearing canonical change
    // invalidates the tie key until the repair lane runs.
    let mut bridge = BridgeHarness::with_seed(&list_fixture_a(), &["--enable-viewer-writes"])?;
    init_extensions(&bridge)?;
    seed_annotations(&bridge)?;

    // Pin conv 1 through the writer lane: the annotation trigger must flip
    // the persisted sort key so the next browse page leads with it.
    let patched = bridge.exchange(json!({
        "op":"patch_annotations","conversation_id":1,"expected_revision":0,
        "patch":{"is_pinned":true}
    }))?;
    ensure!(patched["ok"] == true, "pin patch refused: {patched}");
    let page = bridge.exchange(json!({"op":"list_sessions","limit":10}))?;
    ensure!(item_ids(&page) == [4, 1, 5, 2, 3], "pin must lead: {page}");

    // Unpin through the same lane: the row re-enters the unpinned
    // partition at its activity position.
    let patched = bridge.exchange(json!({
        "op":"patch_annotations","conversation_id":1,"expected_revision":1,
        "patch":{"is_pinned":false}
    }))?;
    ensure!(patched["ok"] == true, "unpin patch refused: {patched}");
    let page = bridge.exchange(json!({"op":"list_sessions","limit":10}))?;
    ensure!(
        item_ids(&page) == [4, 5, 2, 1, 3],
        "unpin must restore: {page}"
    );

    // Canonical timestamp edit: the conversations trigger moves the row
    // inside created_desc (and never touches the verified tie).
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    writer.execute("UPDATE conversations SET started_at = 9_999 WHERE id = 3")?;
    let page = bridge.exchange(json!({
        "op":"list_sessions","sort":"created_desc","limit":10}))?;
    ensure!(page["ok"] == true, "{page}");
    ensure!(item_ids(&page) == [4, 3, 2, 5, 1], "created mirror: {page}");

    // Facts update through the fact mirror: conv 2 gains the largest step
    // count and leads step_desc among unpinned rows.
    writer.execute(
        "INSERT INTO viewer_session_facts \
         (conversation_id,extractor_version,source_revision,coverage_state,step_count) \
         VALUES (2,1,1,'complete',9_000) \
         ON CONFLICT(conversation_id) DO UPDATE SET step_count = 9_000",
    )?;
    let page = bridge.exchange(json!({
        "op":"list_sessions","sort":"step_desc","limit":10}))?;
    ensure!(page["ok"] == true, "{page}");
    // Pinned conv4 still leads its partition; conv2's mirrored step count
    // puts it at the head of the unpinned partition.
    let ids = item_ids(&page);
    ensure!(ids[0] == 4 && ids[1] == 2, "step mirror order: {page}");

    // Identity-bearing canonical edit: the stored tie is invalidated,
    // browse refuses until the repair lane re-resolves it.
    writer.execute("UPDATE conversations SET external_id = 'renamed' WHERE id = 3")?;
    drop(writer);
    let refused = bridge.exchange(json!({"op":"list_sessions","limit":10}))?;
    ensure!(refused["ok"] == false, "{refused}");
    ensure!(
        refused["error"]["kind"] == "facts_pending",
        "invalidated tie must refuse facts_pending: {refused}"
    );
    init_extensions(&bridge)?;
    let page = bridge.exchange(json!({"op":"list_sessions","limit":10}))?;
    ensure!(page["ok"] == true, "repair must restore browse: {page}");
    // 'renamed' re-resolves as gemini:renamed, and the earlier
    // started_at=9999 edit flows through tail-state precedence into
    // activity — conv 3 now leads the unpinned partition.
    ensure!(item_ids(&page) == [4, 3, 5, 2, 1], "{page}");
    bridge.shutdown()
}

#[test]
fn list_sessions_stale_extension_profile_refused() -> Result<()> {
    // A pre-v5 extension profile is stale for the sort-key lane: browse
    // must refuse with the same typed kind as a missing profile instead of
    // silently degrading to a full-candidate sort.
    let mut bridge = BridgeHarness::with_seed(&list_fixture_a(), &[])?;
    init_extensions(&bridge)?;
    let conn = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    conn.execute("UPDATE viewer_meta SET value = '4' WHERE key = 'extension_schema_version'")?;
    drop(conn);
    let refused = bridge.exchange(json!({"op":"list_sessions","limit":10}))?;
    ensure!(refused["ok"] == false);
    ensure!(
        refused["error"]["kind"] == "viewer_extensions_missing",
        "stale extension profile must refuse explicitly: {refused}"
    );
    bridge.shutdown()
}

/// Review fix (Important #4, v6): `viewer_sort_agents_del` — the
/// dimension-delete counterpart to `viewer_sort_sources_del`. Deleting an
/// `agents` row leaves a dangling `agent_id` whose stored tie key was
/// verified against a dimension row that no longer exists; without the
/// trigger those keys stayed trusted forever. With it, the affected rows
/// return to pending and browse refuses honestly until the convergence
/// lanes re-resolve them — where the now-missing agent row surfaces the
/// typed `canonical_coordinates_invalid` refusal.
#[test]
fn list_sessions_agent_delete_invalidates_tie_keys() -> Result<()> {
    let mut bridge = BridgeHarness::with_seed(&list_fixture_a(), &[])?;
    init_extensions(&bridge)?;
    seed_annotations(&bridge)?;

    // Baseline: every sort row carries a verified public tie key.
    let page = bridge.exchange(json!({"op":"list_sessions","limit":10}))?;
    ensure!(page["ok"] == true, "baseline browse must serve: {page}");
    ensure!(item_ids(&page) == [4, 5, 2, 1, 3], "{page}");

    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    match writer.execute("DELETE FROM agents WHERE slug = 'gemini'") {
        Ok(_) => {
            // The v6 trigger returned every gemini-owned row's persisted
            // tie to pending — the keys were verified against a dimension
            // row that no longer exists, so they must not stay trusted.
            let pending: i64 = writer
                .query_row(
                    "SELECT COUNT(*) FROM viewer_session_sort \
                     WHERE tie_key IS NULL AND tie_error IS NULL",
                )?
                .get_typed(0)?;
            ensure!(
                pending == 3,
                "agents delete must invalidate every owned tie key: {pending} pending"
            );
            let refused = bridge.exchange(json!({"op":"list_sessions","limit":10}))?;
            ensure!(refused["ok"] == false, "{refused}");
            ensure!(
                refused["error"]["kind"] == "facts_pending",
                "deleted-agent rows must refuse pending, never keep stale keys: {refused}"
            );
            // A filter that excludes the deleted agent's rows still
            // serves — the refusal scope is the candidate set.
            let claude = bridge.exchange(json!({
                "op":"list_sessions","filters":{"provider":"claude"},"limit":10}))?;
            ensure!(
                claude["ok"] == true,
                "unaffected claude rows must keep serving: {claude}"
            );
            ensure!(item_ids(&claude) == [4, 2], "{claude}");

            // The maintenance lane re-resolves: the missing agents row is
            // itself a refusal input, persisted as the typed kind browse
            // then surfaces — honest, never a guessed order.
            init_extensions(&bridge)?;
            let refused = bridge.exchange(json!({"op":"list_sessions","limit":10}))?;
            ensure!(refused["ok"] == false, "{refused}");
            ensure!(
                refused["error"]["kind"] == "canonical_coordinates_invalid",
                "dangling agent_id must persist its own refusal kind: {refused}"
            );
        }
        Err(error) => {
            eprintln!("engine probe: delete-side FK refused the agents delete: {error}");
            // Enforcement refused the write before the trigger could fire —
            // pin the trigger's presence so the counterpart is never lost
            // on builds where delete-side FKs go unenforced.
            let triggers = writer.query(
                "SELECT name FROM sqlite_master WHERE type = 'trigger' \
                 AND name = 'viewer_sort_agents_del'",
            )?;
            ensure!(
                !triggers.is_empty(),
                "viewer_sort_agents_del must exist beside viewer_sort_sources_del"
            );
        }
    }
    drop(writer);
    bridge.shutdown()
}

/// Review fix (Important #2, unit level): the ingest lane's own profile
/// guard must converge the pending tie set a schema install/upgrade (or a
/// dimension invalidation) can only ever leave. `ensure_schema` alone
/// backfills sort rows with NULL ties; `ensure_profile_for_ingest` — the
/// call `cass index`'s publish hook reaches — resolves them in bounded
/// batches, so a user who only indexes is never stranded on an
/// archive-wide `facts_pending` browse refusal.
#[test]
fn ingest_profile_guard_converges_pending_sort_ties() -> Result<()> {
    let mut bridge = BridgeHarness::with_seed(&list_fixture_a(), &[])?;
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;

    // The pre-fix upgrade state honestly: the profile installs (sort rows
    // backfilled, every tie pending — triggers cannot compute a verified
    // public identity) and nothing else runs.
    schema::ensure_schema(&writer)?;
    let pending: i64 = writer
        .query_row(
            "SELECT COUNT(*) FROM viewer_session_sort \
             WHERE tie_key IS NULL AND tie_error IS NULL",
        )?
        .get_typed(0)?;
    ensure!(
        pending == 5,
        "schema install alone must leave every tie pending: {pending}"
    );

    // The exact call the ingest hook makes: probe + bounded repair, not a
    // no-op guard that strands the archive on a maintenance command the
    // plain-`cass index` operator never sees.
    ensure!(schema::ensure_profile_for_ingest(&writer)?);
    let pending: i64 = writer
        .query_row(
            "SELECT COUNT(*) FROM viewer_session_sort \
             WHERE tie_key IS NULL AND tie_error IS NULL",
        )?
        .get_typed(0)?;
    ensure!(
        pending == 0,
        "the ingest profile guard must converge pending ties: {pending} remain"
    );
    let keys: Vec<String> = writer
        .query("SELECT tie_key FROM viewer_session_sort ORDER BY conversation_id")?
        .iter()
        .map(|row| row.get_typed::<String>(0))
        .collect::<Result<Vec<_>, _>>()?;
    ensure!(
        keys == [
            "gemini:fixture-1",
            "claude:BBB",
            "gemini:fixture-3",
            "claude:CCC",
            "gemini:fixture-5"
        ],
        "converged keys must be the verified public identities: {keys:?}"
    );

    // Idempotent: a second guard call is the cheap indexed probe only —
    // pending stays zero and the keys are unmoved.
    ensure!(schema::ensure_profile_for_ingest(&writer)?);
    let pending: i64 = writer
        .query_row(
            "SELECT COUNT(*) FROM viewer_session_sort \
             WHERE tie_key IS NULL AND tie_error IS NULL",
        )?
        .get_typed(0)?;
    ensure!(pending == 0, "the converged set must stay converged");
    drop(writer);

    // And the read lane serves immediately — no facts_pending in sight
    // (the fixture's pin/tail writes ride the same installed triggers).
    seed_annotations(&bridge)?;
    let page = bridge.exchange(json!({"op":"list_sessions","limit":10}))?;
    ensure!(
        page["ok"] == true,
        "browse must serve right after ingest-lane convergence: {page}"
    );
    ensure!(item_ids(&page) == [4, 5, 2, 1, 3], "{page}");
    bridge.shutdown()
}

// ---------------------------------------------------------------------------
// Task 6: bridge summary/detail mapped from canonical messages
// ---------------------------------------------------------------------------

/// conv 1: a rich local Claude session — user turn, assistant record with
/// thinking + redacted_thinking + tool_use blocks, a normalized tool_result
/// message, an attachment reference whose file does not exist, and a final
/// text message. conv 2: an unrelated remote codex session whose marker
/// content must never appear in conv-1's detail (bound reads).
fn detail_fixture() -> Vec<SeedConversation> {
    let mut rich = SeedConversation::new(1, "claude_code", "/sessions/AAA.jsonl", "AAA")
        .workspace("/data/projects/alpha")
        .started_at(Some(1_000))
        .ended_at(Some(9_000));
    rich.messages = vec![
        SeedMessage {
            idx: 0,
            created_at: Some(1_000),
            role: MessageRole::User,
            content: "fix the flaky test".into(),
        },
        SeedMessage {
            idx: 1,
            created_at: Some(2_000),
            role: MessageRole::Agent,
            content: "running the test now".into(),
        },
        SeedMessage {
            idx: 2,
            created_at: Some(3_000),
            role: MessageRole::Tool,
            content: "test failed: left != right".into(),
        },
        SeedMessage {
            idx: 3,
            created_at: Some(4_000),
            role: MessageRole::Agent,
            content: "see attached evidence".into(),
        },
        SeedMessage {
            idx: 4,
            created_at: Some(5_000),
            role: MessageRole::Agent,
            content: "done".into(),
        },
    ];
    let mut remote = SeedConversation::new(
        2,
        "codex",
        "/codex/sessions/2026/01/02/rollout-2026-01-02T03-04-05-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee.jsonl",
        "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
    );
    remote.source_id = "ssh:builder".into();
    remote.origin_host = Some("builder".into());
    remote.metadata = json!({"cass":{"origin":{"kind":"remote"}}});
    remote.messages = vec![SeedMessage {
        idx: 0,
        created_at: Some(7_000),
        role: MessageRole::Agent,
        content: "REMOTE_SESSION_MARKER_UNRELATED".into(),
    }];
    vec![rich, remote]
}

/// Replace the canned `{"fixture":idx}` extras on conv 1 with the recorded
/// provider record shapes this task exercises. Seeded BEFORE
/// `--init-extensions` so the canonical-observer triggers stay quiet.
fn seed_detail_extras(bridge: &BridgeHarness) -> Result<()> {
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    for (idx, extra) in [
        (
            0_i64,
            json!({"type":"user","uuid":"u-0","message":{"role":"user",
            "content":[{"type":"text","text":"fix the flaky test"}]}}),
        ),
        (
            1,
            json!({"type":"assistant","sessionId":"AAA","uuid":"u-1","requestId":"req-1",
            "message":{"role":"assistant","model":"claude-test","id":"msg-1","content":[
                {"type":"thinking","thinking":"inspect the test first"},
                {"type":"redacted_thinking","data":"REDACTED_BLOCK"},
                {"type":"tool_use","id":"call-1","name":"Bash","input":{"command":"cargo test -q"}},
                {"type":"text","text":"running the test now"}]}}),
        ),
        (
            2,
            json!({"source":"tool_result","tool_use_id":"call-1","is_error":false}),
        ),
        (
            3,
            json!({"type":"attachment","file_path":"/missing/evidence.png",
            "name":"evidence.png","uuid":"u-3"}),
        ),
        (
            4,
            json!({"type":"assistant","uuid":"u-4","message":{"role":"assistant",
            "content":[{"type":"text","text":"done"}]}}),
        ),
    ] {
        writer.execute_with_params(
            "UPDATE messages SET extra_bin = NULL, extra_json = ?1 \
             WHERE conversation_id = 1 AND idx = ?2",
            &param_slice_to_values(params![extra.to_string(), idx]),
        )?;
    }
    drop(writer);
    Ok(())
}

/// Facts / annotation / tag / snippet overlays for conv 1 (after the
/// extension profile exists). `viewer_*` and `snippets` writes are not
/// canonical-observed, so `canonical_revision` stays at its seeded value.
fn seed_detail_overlays(bridge: &BridgeHarness) -> Result<()> {
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    writer.execute_with_params(
        "INSERT INTO viewer_session_facts \
         (conversation_id,extractor_version,source_revision,coverage_state,coverage_detail_json,\
          is_subagent,task_category,turn_count,step_count,thinking_block_count,compaction_count,\
          model_set_json) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
        &param_slice_to_values(params![
            1_i64,
            1_i64,
            77_i64,
            "partial",
            "{\"extractor\":\"claude-facts-v1\",\"fields\":{\
             \"thinking\":{\"state\":\"unavailable\",\"reasons\":[\"thinking_redacted\"]},\
             \"tools\":{\"state\":\"partial\",\"reasons\":[\"tool_exit_not_recorded\"]},\
             \"chat\":{\"state\":\"complete\",\"reasons\":[]}}}",
            0_i64,
            "bugfix",
            3_i64,
            5_i64,
            1_i64,
            0_i64,
            "[\"claude-test\"]"
        ]),
    )?;
    writer.execute(
        "INSERT INTO viewer_annotations \
         (conversation_id,is_pinned,custom_title,notes,revision,updated_at_ms) \
         VALUES (1,1,'detail-title','detail note',2,1700000000002)",
    )?;
    writer.execute(
        "INSERT INTO viewer_tags (conversation_id,tag,created_at_ms) \
         VALUES (1,'detail-tag',1700000000003)",
    )?;
    let message_id: i64 = writer
        .query_row("SELECT id FROM messages WHERE conversation_id=1 AND idx=1")?
        .get_typed(0)?;
    writer.execute_with_params(
        "INSERT INTO snippets (message_id,file_path,start_line,end_line,language,snippet_text) \
         VALUES (?1,'/src/lib.rs',10,14,'rust','fn broken() {}')",
        &param_slice_to_values(params![message_id]),
    )?;
    drop(writer);
    Ok(())
}

#[test]
fn details_rich_session_bound_reads() -> Result<()> {
    let mut bridge = BridgeHarness::with_seed(&detail_fixture(), &[])?;
    seed_detail_extras(&bridge)?;
    init_extensions(&bridge)?;
    seed_detail_overlays(&bridge)?;

    let detail = bridge.exchange(json!({
        "op":"get_session_details","public_id":"claude:AAA","limit":10,
        "sections":["summary","messages","raw","facts","availability","annotations",
                    "attachments","snippets","steps","tools","thinking"],
    }))?;
    ensure!(
        detail["ok"] == true,
        "get_session_details refused: {detail}"
    );
    let result = &detail["result"];
    ensure!(result["conversation_id"] == 1, "{result}");
    ensure!(result["public_id"] == "claude:AAA");
    ensure!(result["provider"] == "claude");
    ensure!(result["source_id"] == "local");
    ensure!(result["complete"] == true);
    ensure!(result["generation"]["canonical_revision"].is_i64());
    ensure!(result["catalog_revision"].is_string());
    ensure!(result["archive_stamp"].is_object());

    let sections = &result["sections"];
    // --- summary: canonical columns + overlays, unknown != 0
    let summary = &sections["summary"];
    ensure!(summary["public_id"] == "claude:AAA", "{summary}");
    ensure!(summary["provider"] == "claude");
    ensure!(summary["source_kind"] == "local");
    ensure!(summary["workspace_path"] == "/data/projects/alpha");
    ensure!(summary["project"] == "alpha");
    ensure!(summary["activity_ms"] == 5_000, "{summary}");
    ensure!(summary["is_pinned"] == true);
    ensure!(summary["custom_title"] == "detail-title");
    ensure!(summary["notes"] == "detail note");
    ensure!(summary["tags"] == json!(["detail-tag"]), "{summary}");
    ensure!(summary["is_subagent"] == false);
    ensure!(summary["task_category"] == "bugfix");
    ensure!(summary["classification_coverage"] == "partial");
    ensure!(summary["metrics"]["step_count"] == 5, "{summary}");
    ensure!(summary["metrics"]["turn_count"] == 3);
    ensure!(summary["metrics"]["thinking_block_count"] == 1);
    ensure!(summary["metrics"]["artifact_count"].is_null());
    ensure!(summary["metrics"]["first_prompt"].is_null(), "{summary}");
    ensure!(summary["metric_quality"]["step_count"] == "known");
    ensure!(summary["metric_quality"]["artifact_count"] == "unknown");
    ensure!(summary["metric_quality"]["first_prompt"] == "unknown");
    ensure!(summary["metric_quality"]["health_grade"] == "unknown");

    // --- messages: stable canonical source order, verbatim extra evidence
    let messages = &sections["messages"];
    let items = messages["items"].as_array().expect("messages items");
    ensure!(items.len() == 5, "{messages}");
    ensure!(
        items
            .iter()
            .map(|m| m["idx"].as_i64().unwrap())
            .collect::<Vec<_>>()
            == [0, 1, 2, 3, 4],
        "messages must stay in canonical source order"
    );
    ensure!(items.iter().all(|m| m["conversation_id"] == 1));
    ensure!(items[1]["role"] == "agent");
    ensure!(
        items[1]["extra"]["value"]["message"]["content"][1]["type"] == "redacted_thinking",
        "redacted thinking must survive verbatim: {}",
        items[1]
    );
    ensure!(items[3]["extra"]["value"]["type"] == "attachment");
    ensure!(messages["next_idx"].is_null(), "{messages}");

    // --- raw: canonical_raw explicitly labelled; source_raw unavailable
    let raw = &sections["raw"];
    ensure!(raw["origin"] == "canonical_raw", "{raw}");
    ensure!(raw["state"] == "available");
    ensure!(raw["source_raw"]["state"] == "unavailable", "{raw}");
    ensure!(raw["records"].as_array().unwrap().len() == 5);
    ensure!(raw["records"][0]["extra_origin"] == "json");

    // --- facts: persisted coverage manifest carried through
    let facts = &sections["facts"];
    ensure!(facts["state"] == "partial", "{facts}");
    ensure!(facts["extractor_version"] == 1);
    ensure!(facts["source_revision"] == 77);
    ensure!(facts["coverage"]["fields"]["thinking"]["state"] == "unavailable");
    ensure!(
        facts["coverage"]["fields"]["thinking"]["reasons"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r == "thinking_redacted"),
        "{facts}"
    );

    // --- availability: typed states, nothing fabricated
    let availability = &sections["availability"];
    ensure!(availability["canonical_raw"] == "available");
    ensure!(availability["source_raw"] == "unavailable");
    ensure!(availability["thinking"] == "unavailable", "{availability}");
    ensure!(availability["tools"] == "partial");
    ensure!(availability["artifacts"] == "unavailable");
    ensure!(availability["memory"] == "unavailable");

    // --- annotations + attachments + snippets
    ensure!(sections["annotations"]["is_pinned"] == true);
    ensure!(sections["annotations"]["tags"] == json!(["detail-tag"]));
    let attachments = &sections["attachments"];
    ensure!(attachments["state"] == "partial", "{attachments}");
    let evidence = attachments["evidence"].as_array().unwrap();
    ensure!(evidence.len() == 1, "{attachments}");
    ensure!(evidence[0]["path"] == "/missing/evidence.png");
    ensure!(evidence[0]["state"] == "evidence_missing");
    let snippets = &sections["snippets"];
    ensure!(
        snippets["items"][0]["file_path"] == "/src/lib.rs",
        "{snippets}"
    );

    // --- pending structural sections are typed unavailable, never dropped
    for name in ["steps", "tools", "thinking"] {
        ensure!(
            sections[name]["state"] == "unavailable",
            "{name}: {sections}"
        );
        ensure!(sections[name]["reason"].is_string());
    }
    ensure!(result["next_cursor"].is_null());

    // --- bound reads: the unrelated remote session never leaks in
    let blob = serde_json::to_string(&result["sections"])?;
    ensure!(
        !blob.contains("REMOTE_SESSION_MARKER_UNRELATED"),
        "conv-1 detail fetched unrelated session rows"
    );

    // The remote row is independently addressable with its own provenance.
    let remote = bridge.exchange(json!({
        "op":"get_session_details","conversation_id":2,"sections":["summary","messages"]}))?;
    ensure!(remote["ok"] == true, "{remote}");
    let remote_summary = &remote["result"]["sections"]["summary"];
    ensure!(remote_summary["source_kind"] == "ssh", "{remote_summary}");
    ensure!(remote_summary["origin_host"] == "builder");
    ensure!(
        remote_summary["public_id"]
            .as_str()
            .unwrap()
            .starts_with("codex:cassremote:"),
        "{remote_summary}"
    );
    let remote_items = remote["result"]["sections"]["messages"]["items"]
        .as_array()
        .unwrap();
    ensure!(remote_items.len() == 1 && remote_items[0]["conversation_id"] == 2);
    bridge.shutdown()
}

#[test]
fn details_summary_carries_metrics_half_and_null_without_facts() -> Result<()> {
    // Task 7 review: the detail summary must serve the identical merged
    // fact+metrics bag the list/recent lanes emit — a persisted
    // `viewer_session_metrics` row surfaces on `summary.metrics` with
    // `viewer_session_metrics.*` provenance — and a fact-less session
    // reports `metrics: null`, never an all-null object.
    let mut bridge = BridgeHarness::with_seed(&detail_fixture(), &[])?;
    seed_detail_extras(&bridge)?;
    init_extensions(&bridge)?;
    seed_detail_overlays(&bridge)?;
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    coding_agent_search::viewer_extensions::facts::ensure_metrics_schema(&writer)?;
    writer.execute_with_params(
        "INSERT INTO viewer_session_metrics \
         (conversation_id,extractor_version,source_revision,ttfe_seconds,one_shot_rate,\
          retries_count,self_corrections_count,user_corrections_count,tools_used_json,\
          task_breakdown_json,health_score,health_grade,health_issues_count,\
          potential_waste_tokens,potential_savings_usd,computed_at_ms) \
         VALUES (?1,1,77,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,1700000000000)",
        &param_slice_to_values(params![
            1_i64,
            12.5_f64,
            80.0_f64,
            1_i64,
            0_i64,
            0_i64,
            "[\"Bash\",\"Read\"]",
            "{\"bugfix\":3,\"reworked_files\":{\"/src/lib.rs\":2}}",
            82_i64,
            "B",
            1_i64,
            400_i64,
            0.02_f64,
        ]),
    )?;
    drop(writer);

    let detail = bridge.exchange(json!({
        "op":"get_session_details","conversation_id":1,"sections":["summary"]}))?;
    ensure!(detail["ok"] == true, "{detail}");
    let summary = &detail["result"]["sections"]["summary"];
    let metrics = &summary["metrics"];
    // Facts half — `viewer_session_facts` columns.
    ensure!(
        metrics["turn_count"] == 3 && metrics["step_count"] == 5,
        "{metrics}"
    );
    ensure!(metrics["thinking_block_count"] == 1, "{metrics}");
    ensure!(metrics["compaction_count"] == 0, "{metrics}");
    ensure!(metrics["model_set"] == json!(["claude-test"]), "{metrics}");
    // Metrics half — the columns only `viewer_session_metrics` carries;
    // measured zeros stay zero, never degrade to unknown.
    ensure!(metrics["ttfe_seconds"] == 12.5, "{metrics}");
    ensure!(metrics["one_shot_rate"] == 80.0, "{metrics}");
    ensure!(metrics["retries_count"] == 1, "{metrics}");
    ensure!(metrics["self_corrections_count"] == 0, "{metrics}");
    ensure!(metrics["user_corrections_count"] == 0, "{metrics}");
    ensure!(
        metrics["tools_used"] == json!(["Bash", "Read"]),
        "{metrics}"
    );
    ensure!(
        metrics["task_breakdown"]["reworked_files"]["/src/lib.rs"] == 2,
        "{metrics}"
    );
    ensure!(
        metrics["health_score"] == 82 && metrics["health_grade"] == "B",
        "{metrics}"
    );
    ensure!(metrics["health_issues_count"] == 1, "{metrics}");
    ensure!(metrics["potential_waste_tokens"] == 400, "{metrics}");
    ensure!(metrics["potential_savings_usd"] == 0.02, "{metrics}");

    // Provenance names the serving column on both halves.
    let quality = &summary["metric_quality"];
    let sources = &summary["metric_sources"];
    ensure!(quality["turn_count"] == "known", "{quality}");
    ensure!(
        sources["turn_count"] == "viewer_session_facts.turn_count",
        "{sources}"
    );
    ensure!(quality["health_score"] == "known", "{quality}");
    ensure!(
        sources["health_score"] == "viewer_session_metrics.health_score",
        "{sources}"
    );
    ensure!(
        sources["health_grade"] == "viewer_session_metrics.health_grade",
        "{sources}"
    );
    ensure!(
        sources["tools_used"] == "viewer_session_metrics.tools_used_json",
        "{sources}"
    );
    ensure!(
        sources["potential_waste_tokens"] == "viewer_session_metrics.potential_waste_tokens",
        "{sources}"
    );
    ensure!(
        sources["potential_savings_usd"] == "viewer_session_metrics.potential_savings_usd",
        "{sources}"
    );
    // The seeded facts row left first_prompt NULL → known-provenance unknown.
    ensure!(quality["first_prompt"] == "unknown", "{quality}");
    ensure!(
        sources["first_prompt"] == "viewer_session_facts.first_prompt",
        "{sources}"
    );

    // The fact-less remote conversation reports `metrics: null`, never an
    // all-null object — the same verified unknown contract list/recent use.
    let remote = bridge.exchange(json!({
        "op":"get_session_details","conversation_id":2,"sections":["summary"]}))?;
    ensure!(remote["ok"] == true, "{remote}");
    let remote_summary = &remote["result"]["sections"]["summary"];
    ensure!(remote_summary["metrics"].is_null(), "{remote_summary}");
    ensure!(
        remote_summary["metric_quality"]["turn_count"] == "unknown",
        "{remote_summary}"
    );
    ensure!(
        remote_summary["metric_sources"]["turn_count"] == "unavailable:facts_not_persisted",
        "{remote_summary}"
    );
    ensure!(
        remote_summary["metric_sources"]["health_score"] == "unavailable:facts_not_persisted",
        "{remote_summary}"
    );
    bridge.shutdown()
}

#[test]
fn details_pagination_and_typed_refusals() -> Result<()> {
    let mut bridge = BridgeHarness::with_seed(&detail_fixture(), &[])?;
    seed_detail_extras(&bridge)?;
    init_extensions(&bridge)?;

    let idxs = |reply: &Value| -> Vec<i64> {
        reply["result"]["sections"]["messages"]["items"]
            .as_array()
            .expect("items")
            .iter()
            .map(|m| m["idx"].as_i64().unwrap())
            .collect()
    };
    let first = bridge.exchange(json!({
        "op":"get_session_details","conversation_id":1,"sections":["messages"],"limit":2}))?;
    ensure!(first["ok"] == true, "{first}");
    ensure!(idxs(&first) == [0, 1]);
    let cursor = first["result"]["next_cursor"]
        .as_str()
        .expect("cursor")
        .to_owned();
    let second = bridge.exchange(json!({
        "op":"get_session_details","conversation_id":1,"sections":["messages"],
        "limit":2,"cursor":cursor}))?;
    ensure!(idxs(&second) == [2, 3]);
    let cursor2 = second["result"]["next_cursor"]
        .as_str()
        .expect("cursor")
        .to_owned();
    let third = bridge.exchange(json!({
        "op":"get_session_details","conversation_id":1,"sections":["messages"],
        "limit":2,"cursor":cursor2}))?;
    ensure!(idxs(&third) == [4]);
    ensure!(third["result"]["next_cursor"].is_null());

    for (request, kind) in [
        (
            json!({"op":"get_session_details","conversation_id":1,"sections":["bogus"]}),
            "unsupported_filter",
        ),
        (
            json!({"op":"get_session_details","sections":["summary"]}),
            "invalid_request",
        ),
        (
            json!({"op":"get_session_details","conversation_id":1,"public_id":"claude:AAA"}),
            "invalid_request",
        ),
        (
            json!({"op":"get_session_details","conversation_id":0}),
            "invalid_request",
        ),
        (
            json!({"op":"get_session_details","conversation_id":99}),
            "session_not_found",
        ),
        (
            json!({"op":"get_session_details","public_id":"claude:nope"}),
            "session_not_found",
        ),
        (
            json!({"op":"get_session_details","conversation_id":1,"sections":["messages"],"cursor":"bogus"}),
            "cursor_expired",
        ),
        (
            json!({"op":"get_session_details","conversation_id":1,"sections":["messages"],"cursor":"{\"m\":-1}"}),
            "cursor_expired",
        ),
        (
            json!({"op":"get_session_details","conversation_id":1,"sections":["summary"],"cursor":"{\"m\":0}"}),
            "invalid_request",
        ),
        (
            json!({"op":"get_session_details","conversation_id":1,"limit":0}),
            "invalid_request",
        ),
    ] {
        let reply = bridge.exchange(request)?;
        ensure!(reply["ok"] == false, "unexpected success: {reply}");
        ensure!(reply["error"]["kind"] == kind, "expected {kind}: {reply}");
    }

    // Unextended archive: the capability op refuses explicitly.
    let mut bare = BridgeHarness::with_seed(&detail_fixture(), &[])?;
    let refused = bare.exchange(json!({"op":"get_session_details","conversation_id":1}))?;
    ensure!(
        refused["ok"] == false && refused["error"]["kind"] == "viewer_extensions_missing",
        "{refused}"
    );
    bare.shutdown()?;
    bridge.shutdown()
}

#[test]
fn details_tool_output_revision_and_evidence() -> Result<()> {
    let mut bridge = BridgeHarness::with_seed(&detail_fixture(), &[])?;
    seed_detail_extras(&bridge)?;
    init_extensions(&bridge)?;

    let detail = bridge.exchange(json!({
        "op":"get_session_details","conversation_id":1,"sections":["messages"]}))?;
    ensure!(detail["ok"] == true, "{detail}");
    let revision = detail["result"]["generation"]["canonical_revision"]
        .as_i64()
        .unwrap();
    let message_id = detail["result"]["sections"]["messages"]["items"]
        .as_array()
        .unwrap()[2]["id"]
        .as_i64()
        .unwrap();

    // Recorded normalized tool output at a verified message anchor.
    let output = bridge.exchange(json!({
        "op":"get_tool_output","conversation_id":1,"expected_revision":revision,
        "step_anchor":{"kind":"message","idx":2}}))?;
    ensure!(output["ok"] == true, "{output}");
    let resource = &output["result"];
    ensure!(
        resource["content"] == "test failed: left != right",
        "{resource}"
    );
    ensure!(resource["media_type"] == "text/plain; charset=utf-8");
    ensure!(resource["origin"] == "canonical_raw");
    ensure!(resource["availability"] == "available");
    ensure!(resource["tool_use_id"] == "call-1");
    ensure!(resource["is_error"] == false);
    ensure!(resource["canonical_revision"] == revision);
    ensure!(resource["catalog_revision"].is_string());
    ensure!(resource["anchor"]["message_id"] == message_id);
    ensure!(resource["anchor"]["idx"] == 2);

    // public_id addressing resolves the same anchor.
    let by_public = bridge.exchange(json!({
        "op":"get_tool_output","public_id":"claude:AAA","expected_revision":revision,
        "step_anchor":{"kind":"message","message_id":message_id}}))?;
    ensure!(
        by_public["ok"] == true && by_public["result"]["content"] == resource["content"],
        "{by_public}"
    );

    for (request, kind) in [
        // Stale generation: the anchor belongs to a different revision.
        (
            json!({"op":"get_tool_output","conversation_id":1,"expected_revision":revision + 9,
                "step_anchor":{"kind":"message","idx":2}}),
            "detail_changed",
        ),
        // Structural anchors stay unverifiable until the Task-8 projection.
        (
            json!({"op":"get_tool_output","conversation_id":1,"expected_revision":revision,
                "step_anchor":{"kind":"step","idx":2}}),
            "anchor_unsupported",
        ),
        // Messages recording no tool output: typed unavailable evidence.
        (
            json!({"op":"get_tool_output","conversation_id":1,"expected_revision":revision,
                "step_anchor":{"kind":"message","idx":0}}),
            "resource_unavailable",
        ),
        (
            json!({"op":"get_tool_output","conversation_id":1,"expected_revision":revision,
                "step_anchor":{"kind":"message","idx":1}}),
            "resource_unavailable",
        ),
        // Missing message / conversation / malformed coordinates.
        (
            json!({"op":"get_tool_output","conversation_id":1,"expected_revision":revision,
                "step_anchor":{"kind":"message","idx":99}}),
            "resource_unavailable",
        ),
        (
            json!({"op":"get_tool_output","conversation_id":99,"expected_revision":revision,
                "step_anchor":{"kind":"message","idx":0}}),
            "session_not_found",
        ),
        (
            json!({"op":"get_tool_output","conversation_id":1,"expected_revision":-1,
                "step_anchor":{"kind":"message","idx":2}}),
            "invalid_request",
        ),
        (
            json!({"op":"get_tool_output","conversation_id":1,"expected_revision":revision,
                "step_anchor":{"kind":"message"}}),
            "invalid_request",
        ),
        (
            json!({"op":"get_tool_output","conversation_id":1,"expected_revision":revision,
                "step_anchor":{"kind":"message","message_id":message_id,"idx":2}}),
            "invalid_request",
        ),
    ] {
        let reply = bridge.exchange(request)?;
        ensure!(reply["ok"] == false, "unexpected success: {reply}");
        ensure!(reply["error"]["kind"] == kind, "expected {kind}: {reply}");
    }

    // A canonical write invalidates the previously bound revision; the new
    // generation serves the new bytes, never the stale coordinate.
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    writer.execute(
        "UPDATE messages SET content='mutated output' WHERE conversation_id=1 AND idx=2",
    )?;
    drop(writer);
    let stale = bridge.exchange(json!({
        "op":"get_tool_output","conversation_id":1,"expected_revision":revision,
        "step_anchor":{"kind":"message","idx":2}}))?;
    ensure!(
        stale["ok"] == false && stale["error"]["kind"] == "detail_changed",
        "{stale}"
    );
    let redetail = bridge.exchange(json!({
        "op":"get_session_details","conversation_id":1,"sections":["summary"]}))?;
    let new_revision = redetail["result"]["generation"]["canonical_revision"]
        .as_i64()
        .unwrap();
    ensure!(new_revision > revision, "{redetail}");
    let fresh = bridge.exchange(json!({
        "op":"get_tool_output","conversation_id":1,"expected_revision":new_revision,
        "step_anchor":{"kind":"message","idx":2}}))?;
    ensure!(
        fresh["ok"] == true && fresh["result"]["content"] == "mutated output",
        "{fresh}"
    );
    bridge.shutdown()
}

/// The chunk contract (`byte_offset`/`next_offset`/UTF-8 snap) exercised
/// against the REAL ingest + serve lanes: a tool output larger than
/// `TOOL_OUTPUT_BYTES` (512 KiB) is seeded through
/// `insert_conversation_tree`, then `next_offset` is walked to the tail and
/// the reassembly must equal the recorded bytes exactly. Mid-string and
/// past-end `byte_offset`s are typed `invalid_request` probes.
#[test]
fn details_tool_output_chunked_walk_is_byte_exact() -> Result<()> {
    let mut fixture = detail_fixture();
    // >512 KiB with a multibyte character straddling EACH chunk boundary:
    // the serve lane must snap `end` back to a UTF-8 boundary twice.
    let mut big = "x".repeat(512 * 1024 - 1); // 524287 bytes
    big.push('é'); // bytes 524287..524289 straddle the 512 KiB boundary
    big.push_str(&"y".repeat(1_048_574 - big.len()));
    big.push('é'); // bytes 1048574..1048576 straddle the second boundary
    big.push_str(&"z".repeat(128 * 1024));
    big.push_str("— needle tail —");
    fixture[0].messages[2].content = big.clone();
    let mut bridge = BridgeHarness::with_seed(&fixture, &[])?;
    seed_detail_extras(&bridge)?;
    init_extensions(&bridge)?;

    // The guarded messages page cannot serve the oversized row whole; it
    // degrades this one record honestly instead of failing the page.
    let detail = bridge.exchange(json!({
        "op":"get_session_details","conversation_id":1,"sections":["summary","messages"]}))?;
    ensure!(detail["ok"] == true, "{detail}");
    let revision = detail["result"]["generation"]["canonical_revision"]
        .as_i64()
        .unwrap();
    let row = &detail["result"]["sections"]["messages"]["items"][2];
    ensure!(
        row["wire_suppressed"] == true && row["content"].is_null(),
        "oversized row must be suppressed on the guarded page: {row}"
    );

    // Walk `next_offset` to the tail: every chunk stays at or under the
    // 512 KiB budget, lands on a UTF-8 boundary, is contiguous with the
    // bytes actually served, and reassembles byte-exact.
    let mut reassembled = String::new();
    let mut offset: i64 = 0;
    let mut continued: Vec<i64> = Vec::new();
    for chunks in 1..=8 {
        let output = bridge.exchange(json!({
            "op":"get_tool_output","conversation_id":1,"expected_revision":revision,
            "step_anchor":{"kind":"message","idx":2},"byte_offset":offset}))?;
        ensure!(output["ok"] == true, "{output}");
        let resource = &output["result"];
        let content = resource["content"]
            .as_str()
            .expect("content must be a string");
        ensure!(
            content.len() <= 512 * 1024,
            "chunk {} exceeds the 512 KiB budget",
            content.len()
        );
        ensure!(
            resource["content_bytes"].as_i64() == Some(big.len() as i64),
            "content_bytes drifted: {resource}"
        );
        ensure!(
            resource["canonical_revision"].as_i64() == Some(revision),
            "revision drifted mid-walk: {resource}"
        );
        ensure!(resource["availability"] == "available", "{resource}");
        ensure!(resource["tool_use_id"] == "call-1", "{resource}");
        reassembled.push_str(content);
        match resource["next_offset"].as_i64() {
            Some(next) => {
                ensure!(next > offset, "cursor did not advance at {offset}");
                ensure!(
                    next == offset + content.len() as i64,
                    "non-contiguous chunk: next={next} offset={offset} len={}",
                    content.len()
                );
                ensure!(
                    big.is_char_boundary(next as usize),
                    "next_offset {next} splits a character"
                );
                continued.push(next);
                offset = next;
            }
            None => break,
        }
        ensure!(chunks < 8, "chunk walk did not terminate: {continued:?}");
    }
    ensure!(
        reassembled == big,
        "reassembled output diverged from the recorded bytes"
    );
    // The two straddled boundaries snap back below the naive +512 KiB ends.
    ensure!(
        continued == [512 * 1024 - 1, 1_048_574],
        "UTF-8 snap produced unexpected offsets: {continued:?}"
    );

    // Probes: mid-string and past-end offsets are typed invalid_request;
    // the exact tail is the legal empty final chunk.
    for probe in [524_288_i64, 1_048_575, -1, big.len() as i64 + 1] {
        let reply = bridge.exchange(json!({
            "op":"get_tool_output","conversation_id":1,"expected_revision":revision,
            "step_anchor":{"kind":"message","idx":2},"byte_offset":probe}))?;
        ensure!(
            reply["ok"] == false && reply["error"]["kind"] == "invalid_request",
            "expected invalid_request at byte_offset {probe}: {reply}"
        );
    }
    let tail = bridge.exchange(json!({
        "op":"get_tool_output","conversation_id":1,"expected_revision":revision,
        "step_anchor":{"kind":"message","idx":2},"byte_offset":big.len() as i64}))?;
    ensure!(
        tail["ok"] == true
            && tail["result"]["content"] == ""
            && tail["result"]["next_offset"].is_null(),
        "exact-end offset must serve the empty tail: {tail}"
    );
    bridge.shutdown()
}

// ---------------------------------------------------------------------------
// Task 10: SQL analytics — projects / dashboard / health
// ---------------------------------------------------------------------------

/// Engine qualification probe (Task-10 Step 3): pin the JSON1 + strftime
/// surface the dashboard explosions rely on BEFORE it is used — and record
/// which paths the serving code qualified at runtime.
#[test]
fn analytics_engine_json1_strftime_probe() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let db = temp.path().join("probe.db");
    let conn = Connection::open(db.to_string_lossy().into_owned())?;
    let json_each = conn
        .query("SELECT j.key, j.value FROM json_each('{\"a\":2}') j")
        .ok()
        .map(|rows| {
            rows.iter()
                .map(|r| {
                    (
                        r.get_typed::<String>(0).unwrap_or_default(),
                        r.get_typed::<i64>(1).unwrap_or(-1),
                    )
                })
                .collect::<Vec<_>>()
        });
    eprintln!("engine probe: json_each={json_each:?}");
    let guards = conn
        .query("SELECT json_valid('{\"a\":1}'), json_type('{\"a\":1}'), json_extract('{\"a\":1}','$.a')")
        .ok()
        .map(|rows| rows.first().map(|r| (
            r.get_typed::<i64>(0).unwrap_or(-1),
            r.get_typed::<String>(1).unwrap_or_default(),
            r.get_typed::<i64>(2).unwrap_or(-1),
        )));
    eprintln!("engine probe: json guards={guards:?}");
    let strf = conn
        .query("SELECT strftime('%w', datetime(1700000000,'unixepoch','localtime')), strftime('%H', datetime(1700000000,'unixepoch','localtime'))")
        .ok()
        .map(|rows| rows.first().map(|r| (
            r.get_typed::<String>(0).unwrap_or_default(),
            r.get_typed::<String>(1).unwrap_or_default(),
        )));
    eprintln!("engine probe: strftime localtime={strf:?}");
    ensure!(
        json_each.is_some(),
        "json_each must be qualified for analytics SQL"
    );
    ensure!(
        strf.is_some(),
        "strftime localtime must be qualified for punchcard"
    );
    drop(conn);
    Ok(())
}

/// Analytics fixture: six seeded conversations across two providers and two
/// workspaces; canonical metric columns written via UPDATE (the observer
/// triggers tolerate it), facts/metrics rows inserted directly.
///
///   id provider     workspace   activity   notes
///   1  claude_code  projA dir   1000       full metrics, breakdown, churn a.rs:7
///   2  claude_code  projA dir   3000       NULL cost; breakdown NULL → fallback category
///   3  gemini       projB dir   9000       is_subagent=1 (excluded by default)
///   4  gemini       projB dir   NULL       unknown activity
///   5  claude_code  projA dir   90004000   NO facts row (pending) — unknown metrics
///   6  codex        projB dir   190008000  retries=5, churn a.rs:2 + b.rs:6
fn analytics_fixture(bridge: &BridgeHarness) -> Result<()> {
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    for (id, started, ended, activity) in [
        (1_i64, Some(500_i64), Some(8_000_i64), 1_000_i64),
        (2, Some(2_000), Some(6_000), 3_000),
        (3, Some(8_500), Some(9_500), 9_000),
        (4, None, None, -1_i64),                    // NULL activity marker
        (5, Some(3_500), Some(4_500), 90_004_000),  // +1 day+hour: own punchcard cell
        (6, Some(7_000), Some(9_000), 190_008_000), // +2 days+hours: own cell
    ] {
        writer.execute_with_params(
            "UPDATE conversations SET started_at=?2, ended_at=?3 WHERE id=?1",
            &param_slice_to_values(params![id, started, ended]),
        )?;
        if activity >= 0 {
            writer.execute_with_params(
                "UPDATE conversation_tail_state SET last_message_created_at=?2 WHERE conversation_id=?1",
                &param_slice_to_values(params![id, activity]),
            )?;
        }
    }
    // Canonical metric columns: known + deliberately NULL inputs.
    for (id, input, output, cache_r, cache_w, total, cost, api, tools, user, assistant, model) in [
        (
            1_i64,
            Some(40_000_i64),
            Some(2_000_i64),
            Some(5_000_i64),
            Some(500_i64),
            Some(60_000_i64),
            Some(1.5_f64),
            Some(3_i64),
            Some(10_i64),
            Some(5_i64),
            Some(5_i64),
            Some("claude-test"),
        ),
        (
            2,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None::<&str>,
        ),
        (
            3,
            Some(1_000),
            Some(500),
            Some(100),
            Some(50),
            Some(1_650),
            Some(0.2),
            Some(1),
            Some(2),
            Some(1),
            Some(1),
            Some("gemini-x"),
        ),
        (
            4,
            None,
            None,
            None,
            None,
            None,
            Some(0.4),
            None,
            None,
            None,
            None,
            Some("gemini-x"),
        ),
        (
            5,
            Some(2_000),
            Some(1_000),
            Some(300),
            Some(60),
            Some(3_360),
            Some(0.3),
            Some(2),
            Some(4),
            Some(2),
            Some(2),
            Some("claude-test"),
        ),
        (
            6,
            Some(5_000),
            Some(2_500),
            Some(200),
            Some(80),
            Some(7_780),
            Some(0.6),
            Some(4),
            Some(12),
            Some(3),
            Some(3),
            Some("codex-x"),
        ),
    ] {
        writer.execute_with_params(
            "UPDATE conversations SET total_input_tokens=?2,total_output_tokens=?3,\
             total_cache_read_tokens=?4,total_cache_creation_tokens=?5,grand_total_tokens=?6,\
             estimated_cost_usd=?7,api_call_count=?8,tool_call_count=?9,user_message_count=?10,\
             assistant_message_count=?11,primary_model=?12 WHERE id=?1",
            &param_slice_to_values(params![
                id, input, output, cache_r, cache_w, total, cost, api, tools, user, assistant,
                model
            ]),
        )?;
    }
    // Facts rows: conv5 stays pending (no row) — unknown, never zero.
    for (id, is_subagent, category, turns, steps) in [
        (1_i64, 0_i64, Some("debugging"), Some(4_i64), Some(8_i64)),
        (2, 0, Some("exploration"), Some(2), Some(3)),
        (3, 1, Some("delegation"), Some(1), Some(2)),
        (4, 0, Some("general"), Some(1), Some(1)),
        (6, 0, Some("refactoring"), Some(2), Some(5)),
    ] {
        writer.execute_with_params(
            "INSERT INTO viewer_session_facts \
             (conversation_id,extractor_version,source_revision,coverage_state,is_subagent,task_category,turn_count,step_count) \
             VALUES (?1,1,1,'complete',?2,?3,?4,?5)",
            &param_slice_to_values(params![id, is_subagent, category, turns, steps]),
        )?;
    }
    coding_agent_search::viewer_extensions::facts::ensure_metrics_schema(&writer)?;
    for (id, ttfe, one_shot, retries, selfc, userc, breakdown, tools_used, hs, waste, savings) in [
        (
            1_i64,
            Some(12.5_f64),
            Some(100.0_f64),
            Some(0_i64),
            Some(0_i64),
            Some(0_i64),
            Some("{\"debugging\":3,\"testing\":1,\"reworked_files\":{\"/src/a.rs\":7}}"),
            Some("[\"Bash\",\"Read\",\"Edit\"]"),
            Some(85_i64),
            Some(3_000_i64),
            Some(0.03_f64),
        ),
        (
            2,
            Some(30.0),
            Some(50.0),
            Some(2),
            Some(0),
            Some(1),
            None::<&str>,
            Some("[\"Bash\"]"),
            Some(95),
            Some(0),
            Some(0.0),
        ),
        (
            3,
            Some(4.0),
            Some(100.0),
            Some(0),
            Some(0),
            Some(0),
            Some("{\"delegation\":1,\"reworked_files\":{}}"),
            Some("[\"Task\"]"),
            Some(90),
            Some(0),
            Some(0.0),
        ),
        (
            4, None, None, None, None, None, None, None, None, None, None,
        ),
        (
            6,
            Some(7.0),
            Some(60.0),
            Some(5),
            Some(1),
            Some(1),
            Some("{\"refactoring\":2,\"reworked_files\":{\"/src/a.rs\":2,\"/src/b.rs\":6}}"),
            Some("[\"Edit\",\"Bash\"]"),
            Some(70),
            Some(4_000),
            Some(0.05),
        ),
    ] {
        writer.execute_with_params(
            "INSERT INTO viewer_session_metrics \
             (conversation_id,extractor_version,source_revision,ttfe_seconds,one_shot_rate,\
              retries_count,self_corrections_count,user_corrections_count,task_breakdown_json,\
              tools_used_json,health_score,health_grade,health_issues_count,\
              potential_waste_tokens,potential_savings_usd,computed_at_ms) \
             VALUES (?1,1,1,?2,?3,?4,?5,?6,?7,?8,?9,'B',0,?10,?11,1700000000000)",
            &param_slice_to_values(params![
                id, ttfe, one_shot, retries, selfc, userc, breakdown, tools_used, hs, waste,
                savings
            ]),
        )?;
    }
    drop(writer);
    Ok(())
}

fn analytics_seeds(workspace_a: &str, workspace_b: &str) -> Vec<SeedConversation> {
    let mk = |id: i64, agent: &str, ws: Option<&str>| {
        let seed = SeedConversation::new(
            id,
            agent,
            &format!("/missing/fixture-{id}.jsonl"),
            &format!("fixture-{id}"),
        );
        match ws {
            Some(path) => seed.workspace(path).agent_messages(None, 1),
            None => seed.agent_messages(None, 1),
        }
    };
    vec![
        mk(1, "claude_code", Some(workspace_a)),
        mk(2, "claude_code", Some(workspace_a)),
        mk(3, "gemini", Some(workspace_b)),
        mk(4, "gemini", Some(workspace_b)),
        mk(5, "claude_code", Some(workspace_a)),
        // Codex identity requires a native rollout filename.
        SeedConversation::new(
            6,
            "codex",
            "/codex/sessions/rollout-2026-01-02T03-04-05-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee.jsonl",
            "rollout-2026-01-02T03-04-05-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        )
        .workspace(workspace_b)
        .agent_messages(None, 1),
    ]
}

#[test]
fn analytics_dashboard_aggregates_and_nullable_unknowns() -> Result<()> {
    let proj = tempfile::tempdir()?;
    let workspace_a = proj.path().join("alpha").to_string_lossy().into_owned();
    std::fs::create_dir(&workspace_a)?;
    let workspace_b = proj.path().join("beta").to_string_lossy().into_owned();
    std::fs::create_dir(&workspace_b)?;
    let mut bridge = BridgeHarness::with_seed(&analytics_seeds(&workspace_a, &workspace_b), &[])?;
    init_extensions(&bridge)?;
    analytics_fixture(&bridge)?;

    let reply = bridge.exchange(json!({"op":"dashboard"}))?;
    ensure!(reply["ok"] == true, "dashboard refused: {reply}");
    let result = &reply["result"];
    ensure!(result["generation"]["canonical_revision"].is_i64());
    ensure!(result["catalog_revision"].is_string());

    // Scope applied once: subagent conv3 excluded; conv4 kept (NULL
    // activity only fails window filters).
    ensure!(result["total_sessions"] == 5, "{result}");

    let summary = &result["summary"];
    ensure!(summary["total_steps"] == 17, "{summary}"); // 8+3+1+5 (conv5 NULL → skip)
    ensure!(summary["total_turns"] == 9, "{summary}"); // 4+2+1+2
    ensure!(summary["total_tool_calls"] == 26, "{summary}"); // 10+2+4+12... conv2 NULL,conv4 NULL,conv5 4? no conv5 tools=4
    ensure!(summary["total_user_messages"] == 10, "{summary}"); // 5+2+3
    ensure!(summary["total_assistant_messages"] == 10, "{summary}");
    ensure!(summary["total_duration_seconds"] == 14.5, "{summary}"); // 7.5+4+1+2

    let financials = &result["financials"];
    // costs: 1.5 + NULL + 0.4 + 0.3 + 0.6 = 2.8; avg divides by ALL 5.
    ensure!(financials["total_cost_usd"] == 2.8, "{financials}");
    ensure!(financials["avg_cost_per_session"] == 0.56, "{financials}");
    ensure!(financials["total_tokens"] == 71_140, "{financials}"); // 60000+3360+7780
    ensure!(financials["total_input_tokens"] == 47_000, "{financials}"); // 40000+2000+5000
    // cache: input=47000, read=5500 → 5500/52500*100 = 10.476 → 10.5
    ensure!(financials["cache_hit_rate"] == 10.5, "{financials}");

    let quality = &result["quality"];
    ensure!(quality["median_ttfe_seconds"] == 12.5, "{quality}"); // [7,12.5,30] → 12.5
    ensure!(quality["total_retries"] == 7, "{quality}"); // 2 + 5 (conv3 excluded)
    ensure!(quality["avg_one_shot_rate"] == 70.0, "{quality}"); // (100+50+60)/3
    ensure!(quality["total_self_corrections"] == 1, "{quality}");
    ensure!(quality["total_user_corrections"] == 2, "{quality}");

    // Punchcard: 168 cells, bucketed by localtime; verify via the same
    // engine expression — the sums/membership are the independent part.
    let cells = result["punchcard"].as_array().expect("punchcard");
    ensure!(cells.len() == 168, "{result}");
    let total_sessions_in_cells: i64 = cells
        .iter()
        .map(|c| c["session_count"].as_i64().unwrap())
        .sum();
    ensure!(
        total_sessions_in_cells == 4,
        "NULL activity excluded: {cells:?}"
    );
    let conn = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    // conv1+conv2 share one bucket (same local hour); conv5/6 land in
    // their own buckets. Bucket identity is probed through the same
    // strftime expression — the session/cost/token sums are independent.
    for (activity_ms, expected_sessions, expected_cost) in [
        (1_000_i64, 2_i64, 1.5_f64), // conv1 + conv2 (NULL cost → 0.0)
        (90_004_000, 1, 0.3),
        (190_008_000, 1, 0.6),
    ] {
        let probe = conn.query_row(&format!(
            "SELECT (CAST(strftime('%w', datetime({activity_ms}/1000.0,'unixepoch','localtime')) AS INTEGER)+6)%7, \
             CAST(strftime('%H', datetime({activity_ms}/1000.0,'unixepoch','localtime')) AS INTEGER)"
        ))?;
        let (w, h) = (probe.get_typed::<i64>(0)?, probe.get_typed::<i64>(1)?);
        let cell = &cells[(w * 24 + h) as usize];
        ensure!(
            cell["session_count"] == expected_sessions && cell["cost_usd"] == expected_cost,
            "cell ({w},{h}) wrong: {cell} (activity {activity_ms})"
        );
    }
    drop(conn);

    // File churn: /src/a.rs 9 edits / 2 sessions; /src/b.rs 6 / 1.
    let churn = result["file_churn"].as_array().expect("file_churn");
    ensure!(churn.len() == 2, "{churn:?}");
    ensure!(
        churn[0]["file_path"] == "/src/a.rs"
            && churn[0]["edits_count"] == 9
            && churn[0]["sessions_count"] == 2,
        "{churn:?}"
    );
    ensure!(
        churn[1]["file_path"] == "/src/b.rs" && churn[1]["edits_count"] == 6,
        "{churn:?}"
    );

    // Category distribution: conv1 breakdown pro-rates cost; conv2 falls
    // back to task_category; conv5 (no facts) falls back to "general".
    let cats = result["categories"].as_array().expect("categories");
    let cat = |name: &str| {
        cats.iter()
            .find(|c| c["category"] == name)
            .cloned()
            .unwrap_or_default()
    };
    ensure!(
        cat("debugging")["turn_count"] == 3
            && cat("debugging")["label"] == "Debugging & Bug Fixing",
        "{cats:?}"
    );
    ensure!(cat("debugging")["cost_usd"] == 1.13, "{cats:?}"); // 1.5*3/4
    ensure!(
        cat("testing")["turn_count"] == 1 && cat("testing")["cost_usd"] == 0.38,
        "{cats:?}"
    );
    ensure!(
        cat("refactoring")["turn_count"] == 2 && cat("refactoring")["cost_usd"] == 0.6,
        "{cats:?}"
    );
    ensure!(cat("exploration")["turn_count"] == 3, "{cats:?}"); // fallback: max(1,step=3)
    ensure!(cat("general")["turn_count"] == 2, "{cats:?}"); // conv4 fallback + conv5 no-facts
    ensure!(cat("general")["cost_usd"] == 0.7, "{cats:?}"); // conv4 0.4 + conv5 0.3
    ensure!(cat("debugging")["one_shot_rate"] == 100.0, "{cats:?}");
    ensure!(cat("exploration")["one_shot_rate"] == 50.0, "{cats:?}");
    ensure!(cat("general")["one_shot_rate"].is_null(), "{cats:?}");

    // Tools frequency = per-TOOL SESSION counts.
    ensure!(
        result["tools_frequency"] == json!({"Bash":3,"Edit":2,"Read":1}),
        "{result}"
    );
    ensure!(
        result["tools_used"] == json!(["Bash", "Edit", "Read"]),
        "{result}"
    );

    // Provider/model distributions over the same scope.
    ensure!(
        result["providers_distribution"] == json!({"claude":3,"codex":1,"gemini":1}),
        "{result}"
    );
    ensure!(
        result["model_distribution"] == json!({"claude-test":2,"codex-x":1,"gemini-x":1}),
        "{result}"
    );
    ensure!(result["unknown_model_count"] == 1, "{result}"); // conv2 NULL model

    // Health audit: score averages MEASURED health_scores only.
    let audit = &result["health_audit"];
    ensure!(audit["score"] == 83 && audit["grade"] == "B", "{audit}"); // int(250/3)
    ensure!(audit["total_wasted_tokens"] == 7_000, "{audit}");
    ensure!(audit["total_savings_usd"] == 0.08, "{audit}");
    let finding_ids: Vec<&str> = audit["findings"]
        .as_array()
        .expect("findings")
        .iter()
        .map(|f| f["id"].as_str().unwrap())
        .collect();
    ensure!(
        finding_ids == ["tool-edit-retries", "high-file-churn", "low-prompt-cache"],
        "{finding_ids:?}"
    );
    let retries_finding = &audit["findings"][0];
    ensure!(
        retries_finding["estimated_tokens_wasted"] == 5_600
            && retries_finding["estimated_savings_usd"] == 0.04,
        "{retries_finding}"
    );
    let churn_finding = &audit["findings"][1];
    ensure!(
        churn_finding["estimated_tokens_wasted"] == 10_800
            && churn_finding["estimated_savings_usd"] == 0.07,
        "{churn_finding}"
    );
    let cache_finding = &audit["findings"][2];
    ensure!(
        cache_finding["estimated_tokens_wasted"] == 11_750,
        "{cache_finding}"
    ); // input//4

    // Recents: activity DESC, NULL last — conv4 sinks to the tail.
    let recents = result["recent_sessions"].as_array().expect("recents");
    let recent_ids: Vec<i64> = recents
        .iter()
        .map(|s| s["conversation_id"].as_i64().unwrap())
        .collect();
    ensure!(recent_ids == [6, 5, 2, 1, 4], "{recent_ids:?}");

    // Metric coverage: known/partial/unknown — conv2/4 NULLs, conv5 no row.
    let coverage = &result["metric_coverage"];
    ensure!(
        coverage["cost"]["quality"] == "partial" && coverage["cost"]["unknown_count"] == 1,
        "{coverage}"
    );
    ensure!(
        coverage["health_score"]["quality"] == "partial"
            && coverage["health_score"]["known_count"] == 3,
        "{coverage}"
    );
    ensure!(coverage["input"]["quality"] == "partial", "{coverage}");

    bridge.shutdown()
}

#[test]
fn analytics_scope_filters_applied_once() -> Result<()> {
    let proj = tempfile::tempdir()?;
    let workspace_a = proj.path().join("alpha").to_string_lossy().into_owned();
    std::fs::create_dir(&workspace_a)?;
    let workspace_b = proj.path().join("beta").to_string_lossy().into_owned();
    std::fs::create_dir(&workspace_b)?;
    let mut bridge = BridgeHarness::with_seed(&analytics_seeds(&workspace_a, &workspace_b), &[])?;
    init_extensions(&bridge)?;
    analytics_fixture(&bridge)?;

    // Provider scope narrows every aggregate identically.
    let claude = bridge.exchange(json!({"op":"dashboard","scope":{"provider":"claude"}}))?;
    ensure!(claude["ok"] == true, "{claude}");
    ensure!(claude["result"]["total_sessions"] == 3, "{claude}");
    ensure!(claude["result"]["providers_distribution"] == json!({"claude":3}));
    ensure!(
        claude["result"]["financials"]["total_cost_usd"] == 1.8,
        "{claude}"
    ); // 1.5+0.3

    // Project scope by display name and by path are the same filter.
    for project in ["alpha", &workspace_a] {
        let scoped = bridge.exchange(json!({"op":"dashboard","scope":{"project":project}}))?;
        ensure!(scoped["ok"] == true, "{scoped}");
        ensure!(scoped["result"]["total_sessions"] == 3, "{scoped}");
        ensure!(scoped["result"]["project_path"] == workspace_a, "{scoped}");
    }

    // Activity window: NULL-activity conv4 fails the bound silently
    // (unknown ≠ included); conv6 (8000) drops out under the upper bound.
    let window = bridge
        .exchange(json!({"op":"dashboard","scope":{"since_ms":2_500,"until_ms":95_000_000}}))?;
    ensure!(window["ok"] == true, "{window}");
    ensure!(window["result"]["total_sessions"] == 2, "{window}"); // conv2 + conv5
    ensure!(window["result"]["summary"]["total_steps"] == 3, "{window}"); // conv5 has no facts

    // Subagent inclusion flips the count exactly once.
    let with_subs =
        bridge.exchange(json!({"op":"dashboard","scope":{"include_subagents":true}}))?;
    ensure!(with_subs["result"]["total_sessions"] == 6, "{with_subs}");
    ensure!(
        with_subs["result"]["providers_distribution"] == json!({"claude":3,"codex":1,"gemini":2})
    );

    // Period aliases: a rolling window that excludes everything seeded in
    // 1970 must still answer honestly (empty universe, null aggregates).
    let period = bridge.exchange(json!({"op":"dashboard","scope":{"period":"7d"}}))?;
    ensure!(period["ok"] == true, "{period}");
    ensure!(period["result"]["total_sessions"] == 0, "{period}");
    ensure!(
        period["result"]["financials"]["total_cost_usd"].is_null(),
        "{period}"
    );
    ensure!(
        period["result"]["health_audit"]["score"].is_null(),
        "{period}"
    );

    // Typed refusals, never silent wrong results.
    for (request, kind) in [
        (
            json!({"op":"dashboard","scope":{"period":"fortnight"}}),
            "unsupported_filter",
        ),
        (
            json!({"op":"dashboard","scope":{"since_ms":10,"until_ms":10}}),
            "unsupported_filter",
        ),
        (
            json!({"op":"dashboard","scope":{"provider":"%%%"}}),
            "unsupported_filter",
        ),
        (
            json!({"op":"dashboard","scope":{"bogus":1}}),
            "invalid_request",
        ),
        (
            json!({"op":"projects","scope":{"period":"all","bogus":1}}),
            "invalid_request",
        ),
    ] {
        let reply = bridge.exchange(request)?;
        ensure!(reply["ok"] == false, "unexpected success: {reply}");
        ensure!(reply["error"]["kind"] == kind, "expected {kind}: {reply}");
    }

    // Unextended archive: the capability op refuses explicitly.
    let mut bare = BridgeHarness::with_seed(&analytics_seeds(&workspace_a, &workspace_b), &[])?;
    let refused = bare.exchange(json!({"op":"dashboard"}))?;
    ensure!(
        refused["ok"] == false && refused["error"]["kind"] == "viewer_extensions_missing",
        "{refused}"
    );
    bare.shutdown()?;
    bridge.shutdown()
}

#[test]
fn analytics_projects_group_by_workspace() -> Result<()> {
    let proj = tempfile::tempdir()?;
    let workspace_a = proj.path().join("alpha").to_string_lossy().into_owned();
    std::fs::create_dir(&workspace_a)?;
    let workspace_b = proj.path().join("beta").to_string_lossy().into_owned();
    std::fs::create_dir(&workspace_b)?;
    let mut bridge = BridgeHarness::with_seed(&analytics_seeds(&workspace_a, &workspace_b), &[])?;
    init_extensions(&bridge)?;
    analytics_fixture(&bridge)?;

    let reply = bridge.exchange(json!({"op":"projects"}))?;
    ensure!(reply["ok"] == true, "{reply}");
    let projects = reply["result"]["projects"].as_array().expect("projects");
    ensure!(projects.len() == 2, "{projects:?}");
    let alpha = &projects[0];
    ensure!(
        alpha["project_path"] == workspace_a && alpha["project_name"] == "alpha",
        "{alpha}"
    );
    ensure!(alpha["session_count"] == 3, "{alpha}"); // convs 1,2,5
    ensure!(alpha["last_activity_ms"] == 90_004_000, "{alpha}");
    let beta = &projects[1];
    ensure!(beta["session_count"] == 2, "{beta}"); // convs 4,6
    ensure!(beta["last_activity_ms"] == 190_008_000, "{beta}");

    // OptionalAggregate coverage per project (CASS lane semantics).
    let alpha_cost = &alpha["metrics"]["estimated_cost_usd"];
    ensure!(
        alpha_cost["total"] == 1.8
            && alpha_cost["known_count"] == 2
            && alpha_cost["unknown_count"] == 1
            && alpha_cost["quality"] == "partial",
        "{alpha_cost}"
    );
    let alpha_duration = &alpha["metrics"]["duration_seconds"];
    ensure!(
        alpha_duration["total"] == 12.5 && alpha_duration["quality"] == "known",
        "{alpha_duration}"
    );
    let beta_in = &beta["metrics"]["total_input_tokens"];
    ensure!(
        beta_in["total"].as_f64() == Some(5_000.0)
            && beta_in["known_count"] == 1
            && beta_in["quality"] == "partial",
        "{beta_in}"
    );

    // Scope applies once here too.
    let claude = bridge.exchange(json!({"op":"projects","scope":{"provider":"claude"}}))?;
    ensure!(claude["ok"] == true, "{claude}");
    let projects = claude["result"]["projects"].as_array().unwrap();
    ensure!(
        projects.len() == 1 && projects[0]["project_name"] == "alpha",
        "{projects:?}"
    );
    bridge.shutdown()
}

#[test]
fn analytics_classification_reports_measured_coverage() -> Result<()> {
    // Task 7: `classification` is derived from the scoped eligible-session
    // facts coverage — `complete` only when every selected row has a
    // non-unknown classification, never a hardcoded marker.
    //
    // The analytics fixture seeds complete-coverage facts for convs
    // 1,2,3,4,6 and NO row for conv5: the default dashboard scope (conv3
    // excluded as a proven subagent) therefore leaves exactly conv5
    // uncertain.
    let proj = tempfile::tempdir()?;
    let workspace_a = proj.path().join("alpha").to_string_lossy().into_owned();
    std::fs::create_dir(&workspace_a)?;
    let workspace_b = proj.path().join("beta").to_string_lossy().into_owned();
    std::fs::create_dir(&workspace_b)?;
    let mut bridge = BridgeHarness::with_seed(&analytics_seeds(&workspace_a, &workspace_b), &[])?;
    init_extensions(&bridge)?;
    analytics_fixture(&bridge)?;

    let reply = bridge.exchange(json!({"op":"dashboard"}))?;
    ensure!(reply["ok"] == true, "{reply}");
    let classification = &reply["result"]["classification"];
    ensure!(classification["complete"] == false, "{classification}");
    ensure!(classification["uncertain_count"] == 1, "{classification}");

    // The beta workspace's selected rows are fully classified — a scoped
    // dashboard reports complete coverage for the same universe.
    let beta = bridge.exchange(json!({"op":"dashboard","scope":{"project":"beta"}}))?;
    ensure!(beta["ok"] == true, "{beta}");
    ensure!(
        beta["result"]["classification"] == json!({"complete": true, "uncertain_count": 0}),
        "{beta}"
    );

    // Projects carry the same per-scope report: alpha keeps conv5's
    // unknown classification, beta is fully covered.
    let projects = bridge.exchange(json!({"op":"projects"}))?;
    ensure!(projects["ok"] == true, "{projects}");
    let items = projects["result"]["projects"].as_array().expect("projects");
    let alpha = items
        .iter()
        .find(|item| item["project_name"] == "alpha")
        .expect("alpha project");
    ensure!(
        alpha["classification"] == json!({"complete": false, "uncertain_count": 1}),
        "{alpha}"
    );
    let beta_proj = items
        .iter()
        .find(|item| item["project_name"] == "beta")
        .expect("beta project");
    ensure!(
        beta_proj["classification"] == json!({"complete": true, "uncertain_count": 0}),
        "{beta_proj}"
    );
    bridge.shutdown()
}

#[test]
fn analytics_recent_sessions_carry_persisted_facts() -> Result<()> {
    // The recent-sessions slice is the dashboard's per-session surface —
    // it must carry the persisted fact/metrics bag, the persisted parent
    // link's public id and the canonical token subfields the summary row
    // needs, not just the headline aggregates.
    let proj = tempfile::tempdir()?;
    let workspace_a = proj.path().join("alpha").to_string_lossy().into_owned();
    std::fs::create_dir(&workspace_a)?;
    let workspace_b = proj.path().join("beta").to_string_lossy().into_owned();
    std::fs::create_dir(&workspace_b)?;
    let mut bridge = BridgeHarness::with_seed(&analytics_seeds(&workspace_a, &workspace_b), &[])?;
    init_extensions(&bridge)?;
    analytics_fixture(&bridge)?;

    let reply = bridge.exchange(json!({"op":"dashboard"}))?;
    ensure!(reply["ok"] == true, "{reply}");
    let recents = reply["result"]["recent_sessions"]
        .as_array()
        .expect("recent_sessions");
    ensure!(!recents.is_empty(), "{recents:?}");
    let conv1 = recents
        .iter()
        .find(|item| item["conversation_id"] == 1)
        .expect("conv1 recent row");
    ensure!(conv1["total_input_tokens"] == 40_000, "{conv1}");
    ensure!(conv1["metrics"]["step_count"] == 8, "{conv1}");
    ensure!(conv1["metrics"]["turn_count"] == 4, "{conv1}");
    ensure!(
        conv1["metrics"]["tools_used"] == json!(["Bash", "Read", "Edit"]),
        "{conv1}"
    );
    ensure!(conv1["metrics"]["health_score"] == 85, "{conv1}");
    ensure!(conv1["classification_coverage"] == "complete", "{conv1}");
    ensure!(conv1["is_subagent"] == false, "{conv1}");
    // conv5 has no facts row: `metrics` stays null, coverage stays unknown.
    let conv5 = recents
        .iter()
        .find(|item| item["conversation_id"] == 5)
        .expect("conv5 recent row");
    ensure!(conv5["metrics"].is_null(), "{conv5}");
    bridge.shutdown()
}

#[test]
fn analytics_health_dynamic_boundary_and_findings() -> Result<()> {
    let proj = tempfile::tempdir()?;
    let workspace_a = proj.path().join("alpha").to_string_lossy().into_owned();
    std::fs::create_dir(&workspace_a)?;
    // Dynamic resource evidence: a real CLAUDE.md inside the workspace.
    std::fs::write(
        std::path::Path::new(&workspace_a).join("CLAUDE.md"),
        "line one\nline two\nline three\n",
    )?;
    let workspace_b = proj.path().join("beta").to_string_lossy().into_owned();
    std::fs::create_dir(&workspace_b)?;
    let mut bridge = BridgeHarness::with_seed(&analytics_seeds(&workspace_a, &workspace_b), &[])?;
    init_extensions(&bridge)?;
    analytics_fixture(&bridge)?;

    let reply = bridge.exchange(json!({"op":"health"}))?;
    ensure!(reply["ok"] == true, "{reply}");
    let result = &reply["result"];
    ensure!(result["score"] == 83 && result["grade"] == "B", "{result}"); // score_to_grade band
    ensure!(
        result["scores_known"] == 3 && result["scores_unknown"] == 2,
        "{result}"
    );
    ensure!(result["total_wasted_tokens"] == 7_000, "{result}");
    let ids: Vec<&str> = result["findings"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["id"].as_str().unwrap())
        .collect();
    ensure!(
        ids == ["tool-edit-retries", "high-file-churn", "low-prompt-cache"],
        "{ids:?}"
    );

    // Dynamic resources are clearly separated: present, provenance-bound,
    // and NEVER folded into score (folded_into_score=false).
    let dynamic = &result["dynamic_resources"];
    ensure!(dynamic["folded_into_score"] == false, "{dynamic}");
    let files = dynamic["rules_files"].as_array().expect("rules_files");
    ensure!(files.len() == 4, "{files:?}"); // 2 projects × CLAUDE.md/AGENTS.md
    let claude_md = files
        .iter()
        .find(|f| f["name"] == "CLAUDE.md" && f["state"] == "available")
        .cloned()
        .unwrap_or_default();
    ensure!(
        claude_md["lines"] == 3 && claude_md["provenance"] == "filesystem",
        "{files:?}"
    );
    ensure!(files.iter().any(|f| f["state"] == "missing"), "{files:?}");

    // Scoped health sees only the alpha project's rules file.
    let scoped = bridge.exchange(json!({"op":"health","scope":{"project":"alpha"}}))?;
    ensure!(scoped["ok"] == true, "{scoped}");
    ensure!(scoped["result"]["total_sessions"] == 3, "{scoped}");
    let files = scoped["result"]["dynamic_resources"]["rules_files"]
        .as_array()
        .unwrap();
    ensure!(files.len() == 2, "{files:?}");
    bridge.shutdown()
}

/// Plan audit for the Task-10 heavy aggregates (pinned like
/// `list_sessions_query_plan_audit`): every aggregate statement must come
/// back with a real plan; the output is the fixture-scale evidence for the
/// Task-10 report.
#[test]
fn analytics_query_plan_audit() -> Result<()> {
    let proj = tempfile::tempdir()?;
    let workspace_a = proj.path().join("alpha").to_string_lossy().into_owned();
    std::fs::create_dir(&workspace_a)?;
    let workspace_b = proj.path().join("beta").to_string_lossy().into_owned();
    std::fs::create_dir(&workspace_b)?;
    let mut bridge = BridgeHarness::with_seed(&analytics_seeds(&workspace_a, &workspace_b), &[])?;
    init_extensions(&bridge)?;
    analytics_fixture(&bridge)?;
    let connection = Connection::open(bridge.db.to_string_lossy().into_owned())?;

    const FROM: &str = "FROM conversations c \
        LEFT JOIN conversation_tail_state t ON t.conversation_id = c.id \
        LEFT JOIN viewer_session_facts f ON f.conversation_id = c.id \
        LEFT JOIN viewer_annotations va ON va.conversation_id = c.id \
        LEFT JOIN viewer_session_metrics m ON m.conversation_id = c.id \
        LEFT JOIN workspaces w ON w.id = c.workspace_id \
        LEFT JOIN agents a ON a.id = c.agent_id \
        LEFT JOIN sources s ON s.id = c.source_id";
    const ACTIVITY: &str = "CASE WHEN t.conversation_id IS NOT NULL \
        THEN COALESCE(t.last_message_created_at, t.ended_at, c.started_at) \
        ELSE COALESCE(c.last_message_created_at, c.ended_at, c.started_at) END";
    let statements: &[(&str, String)] = &[
        (
            "core_aggregate",
            format!(
                "SELECT COUNT(*), SUM(c.estimated_cost_usd), COUNT(c.estimated_cost_usd), \
             SUM(m.health_score), COUNT(m.health_score) {FROM} \
             WHERE COALESCE(f.is_subagent,0) = 0"
            ),
        ),
        (
            "punchcard",
            format!(
                "SELECT (CAST(strftime('%w', datetime({ACTIVITY}/1000.0,'unixepoch','localtime')) AS INTEGER)+6)%7, \
             COUNT(*), COALESCE(SUM(c.estimated_cost_usd),0.0) {FROM} \
             WHERE {ACTIVITY} IS NOT NULL GROUP BY 1, 2"
            ),
        ),
        (
            "file_churn_json_each",
            format!(
                "SELECT j.key, COUNT(DISTINCT c.id), SUM(CAST(j.value AS INTEGER)) \
             {FROM}, json_each(json_extract(m.task_breakdown_json,'$.reworked_files')) j \
             WHERE j.key IS NOT NULL AND j.key != '' GROUP BY j.key ORDER BY 3 DESC, 2 DESC LIMIT 10"
            ),
        ),
        (
            "projects_group_by",
            format!(
                "SELECT w.path, COUNT(*), MAX({ACTIVITY}), SUM(c.estimated_cost_usd) {FROM} \
             WHERE w.path IS NOT NULL AND w.path != '' GROUP BY c.workspace_id ORDER BY w.path"
            ),
        ),
        (
            "ttfe_sorted",
            format!(
                "SELECT m.ttfe_seconds {FROM} WHERE m.ttfe_seconds IS NOT NULL \
             AND m.ttfe_seconds >= 0 ORDER BY m.ttfe_seconds LIMIT 100001"
            ),
        ),
        (
            "recent_sessions",
            format!(
                "SELECT c.id, {ACTIVITY} {FROM} \
             ORDER BY ({ACTIVITY} IS NULL), {ACTIVITY} DESC, c.id DESC LIMIT 10"
            ),
        ),
    ];
    for (label, sql) in statements {
        let plan = connection.query(&format!("EXPLAIN QUERY PLAN {sql}"))?;
        let joined = plan
            .iter()
            .map(|row| row.get_typed::<String>(3).unwrap_or_default())
            .collect::<Vec<_>>()
            .join(" | ");
        eprintln!("PLAN [{label}]: {joined}");
        ensure!(!plan.is_empty(), "{label} must produce a query plan");
    }
    drop(connection);

    // Fixture-scale dashboard latency: p50/p95 over repeated exchanges.
    let mut times: Vec<u128> = Vec::new();
    for _ in 0..50 {
        let start = std::time::Instant::now();
        let reply = bridge.exchange(json!({"op":"dashboard"}))?;
        times.push(start.elapsed().as_micros());
        ensure!(reply["ok"] == true, "{reply}");
    }
    times.sort();
    let p50 = times[times.len() / 2] / 1000;
    let p95 = times[times.len() * 95 / 100] / 1000;
    eprintln!("dashboard fixture-scale: p50={p50}ms p95={p95}ms (n=50)");
    bridge.shutdown()
}

/// The Task-10 metrics row lands next to the facts row: `publish_session_facts`
/// creates `viewer_session_metrics` lazily inside the same transaction and
/// persists the quality/health columns with unknowns as NULL, never zero.
#[test]
fn analytics_metrics_publish_nullable_and_atomic() -> Result<()> {
    let workspace = std::env::temp_dir()
        .join(format!("analytics-metrics-{}", std::process::id()))
        .to_string_lossy()
        .into_owned();
    let mut bridge =
        BridgeHarness::with_seed(&analytics_seeds(&workspace, "/data/projects/beta"), &[])?;
    init_extensions(&bridge)?;
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;

    use coding_agent_search::viewer_extensions::facts::{
        CoverageEntry, FactPublication, SessionHealth,
    };
    let publication = FactPublication {
        extractor: "claude-facts-v1".into(),
        extractor_version: 1,
        source_revision: 42,
        provider: "claude_code".into(),
        source_id: "fixture".into(),
        coverage: vec![CoverageEntry {
            field: "tools".into(),
            state: "complete".into(),
            reasons: vec![],
        }],
        coverage_state: "complete".into(),
        is_subagent: Some(false),
        parent_external_id: None,
        task_category: Some("debugging".into()),
        turn_count: Some(2),
        step_count: Some(4),
        thinking_block_count: None,
        artifact_count: None,
        subagent_count: None,
        tool_error_count: Some(1),
        tool_calls_with_exit_status: None,
        file_fact_count: Some(1),
        compaction_count: None,
        model_set: vec!["claude-test".into()],
        first_prompt: None,
        tools: vec![
            coding_agent_search::viewer_extensions::facts::ToolCallFact {
                call_id: "c1".into(),
                raw_call_id: "c1".into(),
                name: "Bash".into(),
                has_result: true,
                exit_code: Some(0),
                is_error: Some(false),
                file_path: Some("/proj/node_modules/pkg/index.js".into()),
                source_idx: 0,
                source_message_id: None,
            },
        ],
        quality: Some(
            coding_agent_search::viewer_extensions::facts::SessionQualityMetrics {
                user_turn_count: 2,
                retries_count: 1,
                self_corrections_count: 1,
                user_corrections_count: 0,
                one_shot_rate: Some(71.4),
                ttfe_seconds: Some(12.34),
                category_distribution: [("debugging".to_string(), 2_i64)].into_iter().collect(),
                tools_frequency: vec![("Bash".to_string(), 3_i64), ("Edit".to_string(), 1)],
                reworked_files: [("/src/a.rs".to_string(), 4_i64)].into_iter().collect(),
            },
        ),
        health: Some(SessionHealth {
            score: 85,
            grade: "B".into(),
            issues_count: 1,
            potential_waste_tokens: 3_000,
            potential_savings_usd: 0.009,
        }),
    };
    coding_agent_search::viewer_extensions::facts::publish_session_facts(&writer, 1, &publication)?;

    let row = writer.query_row(
        "SELECT ttfe_seconds, one_shot_rate, retries_count, self_corrections_count, \
         user_corrections_count, task_breakdown_json, tools_used_json, health_score, \
         health_grade, health_issues_count, potential_waste_tokens, potential_savings_usd \
         FROM viewer_session_metrics WHERE conversation_id = 1",
    )?;
    ensure!(row.get_typed::<f64>(0)? == 12.34, "{row:?}");
    ensure!(row.get_typed::<f64>(1)? == 71.4, "{row:?}");
    ensure!(row.get_typed::<i64>(2)? == 1, "{row:?}");
    ensure!(row.get_typed::<i64>(3)? == 1, "{row:?}");
    ensure!(row.get_typed::<i64>(4)? == 0, "{row:?}");
    let breakdown: String = row.get_typed(5)?;
    ensure!(breakdown.contains("\"debugging\":2"), "{breakdown}");
    ensure!(
        breakdown.contains("\"reworked_files\":{\"/src/a.rs\":4}"),
        "{breakdown}"
    );
    ensure!(
        row.get_typed::<String>(6)? == "[\"Bash\",\"Edit\"]",
        "{row:?}"
    );
    ensure!(row.get_typed::<i64>(7)? == 85, "{row:?}");
    ensure!(row.get_typed::<String>(8)? == "B", "{row:?}");
    ensure!(row.get_typed::<i64>(9)? == 1, "{row:?}");
    ensure!(row.get_typed::<i64>(10)? == 3_000, "{row:?}");

    // A second publication with the same (extractor_version, source_revision)
    // skips both rows — the idempotence rule applies to the metrics row too.
    let outcome = coding_agent_search::viewer_extensions::facts::publish_session_facts(
        &writer,
        1,
        &publication,
    )?;
    ensure!(
        matches!(
            outcome,
            coding_agent_search::viewer_extensions::facts::FactPublishOutcome::SkippedUnchanged
        ),
        "{outcome:?}"
    );

    // An extractor-unavailable publication leaves every metric NULL — the
    // row exists (same key) but no axis is fabricated.
    let unavailable = FactPublication {
        extractor: "extractor_unavailable".into(),
        source_revision: 43,
        coverage: vec![],
        coverage_state: "unknown".into(),
        is_subagent: None,
        task_category: None,
        turn_count: None,
        step_count: None,
        thinking_block_count: None,
        artifact_count: None,
        subagent_count: None,
        tool_error_count: None,
        tool_calls_with_exit_status: None,
        file_fact_count: None,
        compaction_count: None,
        model_set: vec![],
        first_prompt: None,
        tools: vec![],
        quality: None,
        health: None,
        ..publication.clone()
    };
    coding_agent_search::viewer_extensions::facts::publish_session_facts(&writer, 2, &unavailable)?;
    let row = writer.query_row(
        "SELECT ttfe_seconds, health_score, task_breakdown_json, tools_used_json \
         FROM viewer_session_metrics WHERE conversation_id = 2",
    )?;
    ensure!(row.get_typed::<Option<f64>>(0)?.is_none(), "{row:?}");
    ensure!(row.get_typed::<Option<i64>>(1)?.is_none(), "{row:?}");
    ensure!(row.get_typed::<Option<String>>(2)?.is_none(), "{row:?}");
    ensure!(row.get_typed::<Option<String>>(3)?.is_none(), "{row:?}");

    drop(writer);
    bridge.shutdown()
}

// ---------------------------------------------------------------------------
// Task 14: extension backfill for archives indexed before --init-extensions
//
// Facts and the structural projection are populated inside the ingest pass;
// an archive indexed BEFORE the authorized profile install keeps empty
// extension tables forever (search_sessions/list_sessions honestly refuse
// facts_pending/data_unavailable). The `--backfill-viewer-extensions`
// one-shot writer replays committed canonical rows through the SAME
// packet reconstruction the ingest hooks use — bounded per-batch
// transactions, idempotent on (extractor_version|projection_version,
// source_revision), with one end-of-run journal notify.
// ---------------------------------------------------------------------------

/// Insert one conversation through the canonical storage writer (the same
/// `insert_conversation_tree` the indexer hooks ride). Snippet-bearing
/// messages matter: snippets feed `semantic_hash`, so a replay that skipped
/// them would key extension rows on a different `source_revision` than the
/// ingest pass would have produced.
fn insert_backfill_conversation(
    storage: &FrankenStorage,
    agent_id: i64,
    conv: &Conversation,
) -> Result<i64> {
    Ok(storage
        .insert_conversation_tree(agent_id, None, conv)?
        .conversation_id)
}

/// Claude-shaped canonical conversation: user prompt, assistant reply with
/// thinking + tool_use, tool result — plus a snippet on the user message so
/// snippet hydration is pinned by the source-revision equality assert.
fn backfill_claude_conversation() -> Conversation {
    let user_extra = json!({
        "type": "user", "uuid": "u1", "sessionId": "sess-bf",
        "message": {"role": "user", "content": [{"type": "text", "text": "please fix the flaky test"}]}
    });
    let assistant_extra = json!({
        "type": "assistant", "uuid": "a1", "sessionId": "sess-bf", "requestId": "r1",
        "message": {"id": "m1", "role": "assistant", "model": "claude-opus-5",
                    "content": [
                        {"type": "thinking", "thinking": "plan the fix"},
                        {"type": "tool_use", "id": "toolu_1", "name": "Read",
                         "input": {"file_path": "/src/a.rs"}}
                    ],
                    "usage": {"input_tokens": 10, "output_tokens": 7}}
    });
    Conversation {
        id: None,
        agent_slug: "claude_code".into(),
        workspace: None,
        external_id: Some("bf-claude".into()),
        title: Some("backfill claude".into()),
        source_path: "/missing/bf-claude.jsonl".into(),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_010),
        approx_tokens: None,
        metadata_json: json!({"fixture": true, "source": "claude_code"}),
        source_id: "local".into(),
        origin_host: None,
        messages: vec![
            Message {
                id: None,
                idx: 0,
                role: MessageRole::User,
                author: None,
                created_at: Some(1_700_000_000_000),
                content: "please fix the flaky test".into(),
                extra_json: user_extra,
                snippets: vec![Snippet {
                    id: None,
                    file_path: Some(PathBuf::from("src/lib.rs")),
                    start_line: Some(3),
                    end_line: Some(9),
                    language: Some("rust".into()),
                    snippet_text: Some("fn flaky() {}".into()),
                }],
            },
            Message {
                id: None,
                idx: 1,
                role: MessageRole::Agent,
                author: None,
                created_at: Some(1_700_000_000_001),
                content: "assistant reply".into(),
                extra_json: assistant_extra,
                snippets: Vec::new(),
            },
            Message {
                id: None,
                idx: 2,
                role: MessageRole::Tool,
                author: None,
                created_at: Some(1_700_000_000_002),
                content: "file body".into(),
                extra_json: json!({"source": "tool_result", "tool_use_id": "toolu_1",
                                   "is_error": false, "_text": "file body"}),
                snippets: Vec::new(),
            },
        ],
    }
}

fn backfill_codex_conversation(external_id: &str, text: &str, count: i64) -> Conversation {
    Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: None,
        external_id: Some(external_id.into()),
        title: Some(format!("backfill {external_id}")),
        source_path: format!("/missing/{external_id}.jsonl").into(),
        started_at: Some(1_700_000_100_000),
        ended_at: Some(1_700_000_100_010),
        approx_tokens: None,
        metadata_json: json!({"fixture": true}),
        source_id: "local".into(),
        origin_host: None,
        messages: (0..count)
            .map(|idx| Message {
                id: None,
                idx,
                role: if idx == 0 {
                    MessageRole::User
                } else {
                    MessageRole::Agent
                },
                author: None,
                created_at: Some(1_700_000_100_000 + idx),
                content: text.into(),
                extra_json: json!({"fixture": idx}),
                snippets: Vec::new(),
            })
            .collect(),
    }
}

struct BackfillFixture {
    _temp: tempfile::TempDir,
    db: PathBuf,
    /// Canonical `Conversation` as passed to `insert_conversation_tree` —
    /// the ingest-time input the replay must reproduce bit-for-bit at the
    /// packet-hash level (ids never enter the hash).
    claude: Conversation,
}

/// Seed canonical rows WITHOUT the extension profile — the archive state an
/// operator has before ever running --init-extensions.
fn backfill_fixture() -> Result<BackfillFixture> {
    let temp = tempfile::tempdir()?;
    let db = temp.path().join("archive.db");
    let storage = FrankenStorage::open(&db)?;
    let claude_agent = storage.ensure_agent(&Agent {
        id: None,
        slug: "claude_code".into(),
        name: "claude_code".into(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    let codex_agent = storage.ensure_agent(&Agent {
        id: None,
        slug: "codex".into(),
        name: "codex".into(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    let claude = backfill_claude_conversation();
    let codex = backfill_codex_conversation("bf-codex", "codex fixture text", 3);
    let claude_id = insert_backfill_conversation(&storage, claude_agent, &claude)?;
    let codex_id = insert_backfill_conversation(&storage, codex_agent, &codex)?;
    ensure!(
        claude_id == 1 && codex_id == 2,
        "fixture ids: {claude_id}/{codex_id}"
    );
    drop(storage);
    Ok(BackfillFixture {
        _temp: temp,
        db,
        claude,
    })
}

/// The `--init-extensions` authorized pass, in-process: additive schema then
/// canonical observers — exactly `viewer_bridge::init_extensions`.
fn install_extensions(db: &std::path::Path) -> Result<Connection> {
    let connection = Connection::open(db.to_string_lossy().into_owned())?;
    schema::ensure_schema(&connection)?;
    revisions::ensure_observers(&connection)?;
    Ok(connection)
}

fn count(connection: &Connection, sql: &str) -> Result<i64> {
    Ok(connection.query_row(sql)?.get_typed(0)?)
}

#[test]
fn backfill_replays_committed_canonical_into_extension_tables() -> Result<()> {
    let fixture = backfill_fixture()?;
    let connection = install_extensions(&fixture.db)?;

    // The Task-14 gap: canonical rows committed, extension tables empty —
    // schema install alone cannot populate them.
    ensure!(
        count(&connection, "SELECT COUNT(*) FROM viewer_session_facts")? == 0
            && count(
                &connection,
                "SELECT COUNT(*) FROM viewer_session_projection"
            )? == 0,
        "gap fixture must start without extension rows"
    );

    let mut progress_calls = 0_usize;
    let stats = backfill::backfill_viewer_extensions(
        &connection,
        &backfill::BackfillOptions::default(),
        |_| progress_calls += 1,
    )?;
    ensure!(stats.conversations_scanned == 2, "{stats:?}");
    ensure!(stats.facts_published == 2, "{stats:?}");
    ensure!(stats.structure_published == 2, "{stats:?}");
    ensure!(stats.failures == 0, "{stats:?}");
    ensure!(progress_calls >= 1, "progress must be reported per batch");

    // Correctness: the replayed publication must be identical to what the
    // ingest hook would have produced from the same committed bytes —
    // source_revision is semantic_hash-derived, so equality pins snippet,
    // extra, role and provenance hydration all at once.
    let expected_facts = facts::facts_for_canonical(&fixture.claude);
    let row = connection.query_row(
        "SELECT extractor_version, source_revision, coverage_state, \
         thinking_block_count, first_prompt \
         FROM viewer_session_facts WHERE conversation_id = 1",
    )?;
    ensure!(row.get_typed::<i64>(0)? == facts::FACTS_EXTRACTOR_VERSION);
    ensure!(
        row.get_typed::<i64>(1)? == expected_facts.source_revision,
        "backfill replay must produce the ingest-identical source_revision"
    );
    ensure!(
        row.get_typed::<Option<i64>>(3)? == expected_facts.thinking_block_count,
        "thinking_block_count must match the ingest-time publication"
    );
    ensure!(
        row.get_typed::<Option<String>>(4)? == expected_facts.first_prompt,
        "first_prompt must match the ingest-time publication"
    );

    let projection = connection.query_row(
        "SELECT projection_version, extractor_version, source_revision, \
         step_count, turn_count, tool_call_count \
         FROM viewer_session_projection WHERE conversation_id = 1",
    )?;
    ensure!(projection.get_typed::<i64>(0)? == 1, "projection_version");
    ensure!(projection.get_typed::<i64>(1)? == facts::FACTS_EXTRACTOR_VERSION);
    ensure!(
        projection.get_typed::<i64>(2)? == expected_facts.source_revision,
        "projection must key on the same replay revision"
    );
    ensure!(projection.get_typed::<Option<i64>>(4)?.is_some());
    ensure!(
        count(
            &connection,
            "SELECT COUNT(*) FROM viewer_tool_calls WHERE conversation_id = 1"
        )? == 1,
        "the toolu_1 call→result link must be projected"
    );
    ensure!(
        count(
            &connection,
            "SELECT COUNT(*) FROM viewer_step_links WHERE conversation_id = 1"
        )? > 0,
        "step links must be projected"
    );

    // Backfill writes extension tables only: canonical_revision must not
    // move. projection_revision bumps once per published conversation.
    let revisions = connection.query_row(
        "SELECT canonical_revision, projection_revision FROM viewer_revisions WHERE id = 1",
    )?;
    ensure!(
        revisions.get_typed::<i64>(0)? == 0,
        "backfill must not move canonical_revision"
    );
    ensure!(revisions.get_typed::<i64>(1)? == 2, "one bump per publish");

    // Idempotent re-run: every outcome SkippedUnchanged, no journal events —
    // extension tables carry no journal triggers and the single end-of-run
    // notify pass emits only when committed events exist.
    let again = backfill::backfill_viewer_extensions(
        &connection,
        &backfill::BackfillOptions::default(),
        |_| {},
    )?;
    ensure!(
        again.facts_published == 0 && again.structure_published == 0,
        "{again:?}"
    );
    ensure!(
        again.facts_skipped_unchanged == 2 && again.structure_skipped_unchanged == 2,
        "{again:?}"
    );
    let still =
        connection.query_row("SELECT projection_revision FROM viewer_revisions WHERE id = 1")?;
    ensure!(still.get_typed::<i64>(0)? == 2, "skip must not bump");
    ensure!(
        count(&connection, "SELECT COUNT(*) FROM viewer_change_log")? == 0,
        "backfill publishes no journal rows; the single end-of-run notify \
         observes no committed events and stamps nothing"
    );

    // A committed canonical change re-derives on the next run: the appended
    // message changes semantic_hash → both publications republish for that
    // conversation only.
    connection.execute(
        "INSERT INTO messages (conversation_id,idx,role,content) \
         VALUES (1,99,'agent','appended tail')",
    )?;
    let moved = backfill::backfill_viewer_extensions(
        &connection,
        &backfill::BackfillOptions::default(),
        |_| {},
    )?;
    ensure!(
        moved.facts_published == 1 && moved.structure_published == 1,
        "{moved:?}"
    );
    ensure!(
        moved.facts_skipped_unchanged == 1 && moved.structure_skipped_unchanged == 1,
        "{moved:?}"
    );
    let canon: i64 = connection
        .query_row("SELECT canonical_revision FROM viewer_revisions WHERE id = 1")?
        .get_typed(0)?;
    ensure!(
        canon == 1,
        "only the canonical INSERT moves canonical_revision: {canon}"
    );
    Ok(())
}

#[test]
fn backfill_batches_and_limit_are_bounded_and_resumable() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let db = temp.path().join("archive.db");
    let storage = FrankenStorage::open(&db)?;
    let agent = storage.ensure_agent(&Agent {
        id: None,
        slug: "codex".into(),
        name: "codex".into(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    for id in 1..=3_i64 {
        let conv = backfill_codex_conversation(&format!("bf-batched-{id}"), "batched text", 2);
        ensure!(insert_backfill_conversation(&storage, agent, &conv)? == id);
    }
    drop(storage);
    let connection = install_extensions(&db)?;

    // One-conversation batches: three transactions, three progress reports.
    let options = backfill::BackfillOptions {
        batch_size: 1,
        limit: None,
    };
    let mut batches = 0_usize;
    let stats = backfill::backfill_viewer_extensions(&connection, &options, |_| batches += 1)?;
    ensure!(
        stats.conversations_scanned == 3 && batches == 3,
        "{stats:?} batches={batches}"
    );

    // --backfill-limit N equivalent: a capped run stops mid-archive and a
    // later uncapped run finishes it (idempotent resume, no cursor table).
    let limited = backfill::BackfillOptions {
        batch_size: 256,
        limit: Some(1),
    };
    // Reset so the limit path re-derives on a clean state.
    connection.execute("DELETE FROM viewer_session_facts")?;
    connection.execute("DELETE FROM viewer_session_projection")?;
    connection.execute("DELETE FROM viewer_step_links")?;
    connection.execute("DELETE FROM viewer_turn_links")?;
    connection.execute("DELETE FROM viewer_tool_calls")?;
    let first = backfill::backfill_viewer_extensions(&connection, &limited, |_| {})?;
    ensure!(
        first.conversations_scanned == 1 && first.limit_reached,
        "{first:?}"
    );
    ensure!(
        count(&connection, "SELECT COUNT(*) FROM viewer_session_facts")? == 1,
        "limit must bound the committed batch"
    );
    let resumed = backfill::backfill_viewer_extensions(
        &connection,
        &backfill::BackfillOptions::default(),
        |_| {},
    )?;
    ensure!(resumed.conversations_scanned == 3, "{resumed:?}");
    ensure!(
        resumed.facts_published == 2 && resumed.facts_skipped_unchanged == 1,
        "{resumed:?}"
    );
    ensure!(
        count(&connection, "SELECT COUNT(*) FROM viewer_session_facts")? == 3
            && count(
                &connection,
                "SELECT COUNT(*) FROM viewer_session_projection"
            )? == 3,
        "resume must finish the archive"
    );
    Ok(())
}

#[test]
fn backfill_viewer_extensions_flag_repairs_preexisting_archive() -> Result<()> {
    // Binary-level: --init-extensions installs schema only (the gap), then
    // --backfill-viewer-extensions replays the committed canonical rows.
    let mut bridge = BridgeHarness::with_seed(
        &[
            SeedConversation::new(1, "codex", "/missing/a.jsonl", "ext-a")
                .agent_messages(Some(1_700_000_000_000), 3),
            SeedConversation::new(2, "codex", "/missing/b.jsonl", "ext-b")
                .agent_messages(Some(1_700_000_100_000), 2),
        ],
        &[],
    )?;
    init_extensions(&bridge)?;
    let reader = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    ensure!(
        count(&reader, "SELECT COUNT(*) FROM viewer_session_facts")? == 0,
        "--init-extensions alone must leave extension tables empty (the gap)"
    );
    drop(reader);

    let status = Command::new(env!("CARGO_BIN_EXE_cass-viewer-bridge"))
        .arg("--stdio")
        .arg("--backfill-viewer-extensions")
        .arg("--data-dir")
        .arg(&bridge.data_dir)
        .arg("--db")
        .arg(&bridge.db)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .status()?;
    ensure!(
        status.success(),
        "--backfill-viewer-extensions failed: {status}"
    );

    let reader = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    ensure!(
        count(&reader, "SELECT COUNT(*) FROM viewer_session_facts")? == 2,
        "backfill must populate facts for every canonical conversation"
    );
    ensure!(
        count(&reader, "SELECT COUNT(*) FROM viewer_session_projection")? == 2,
        "backfill must populate the structural projection"
    );
    let canon: i64 = reader
        .query_row("SELECT canonical_revision FROM viewer_revisions WHERE id = 1")?
        .get_typed(0)?;
    ensure!(canon == 0, "backfill must not move canonical_revision");
    drop(reader);

    // Idempotent second run exits cleanly and changes nothing.
    let status = Command::new(env!("CARGO_BIN_EXE_cass-viewer-bridge"))
        .arg("--stdio")
        .arg("--backfill-viewer-extensions")
        .arg("--data-dir")
        .arg(&bridge.data_dir)
        .arg("--db")
        .arg(&bridge.db)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .status()?;
    ensure!(status.success(), "idempotent re-run failed: {status}");
    let reader = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    let canon: i64 = reader
        .query_row("SELECT canonical_revision FROM viewer_revisions WHERE id = 1")?
        .get_typed(0)?;
    ensure!(canon == 0);
    drop(reader);

    bridge.shutdown()
}

// ===========================================================================
// Ingest-lane extension generation publication. Production `cass index`
// over isolated fixture sources must publish facts AND the structural
// projection as ONE coherent extension generation — same
// `source_revision`, durably receipted in `viewer_meta` — or honestly
// record `pending`/`failed`. `complete` may only stand when both sides
// committed; a partial or interrupted publication is never advertised
// complete and is reconciled only by the explicit resumable backfill.
// ===========================================================================

/// Isolated ingest sandbox: HOME/XDG/CODEX_HOME/CLAUDE_CONFIG_DIR all under
/// one tempdir so the production `cass` and `cass-viewer-bridge` binaries
/// only ever see the fixture sources seeded here.
struct IngestFixture {
    _temp: tempfile::TempDir,
    home: PathBuf,
    data_dir: PathBuf,
    db: PathBuf,
    codex_home: PathBuf,
    claude_config: PathBuf,
}

fn ingest_fixture() -> Result<IngestFixture> {
    let temp = tempfile::tempdir()?;
    let home = temp.path().join("home");
    let data_dir = temp.path().join("data");
    let codex_home = home.join(".codex");
    let claude_config = home.join(".claude");
    for dir in [&home, &data_dir, &codex_home, &claude_config] {
        std::fs::create_dir_all(dir)?;
    }
    Ok(IngestFixture {
        db: data_dir.join("agent_search.db"),
        _temp: temp,
        home,
        data_dir,
        codex_home,
        claude_config,
    })
}

/// Shared environment for every production `cass` call in this section:
/// pinned HOME/XDG/CODEX/CLAUDE roots plus the usual e2e quieting flags.
fn cass_cmd(fixture: &IngestFixture) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_cass"));
    cmd.env("HOME", &fixture.home)
        .env("XDG_DATA_HOME", fixture.home.join(".local/share"))
        .env("XDG_CONFIG_HOME", fixture.home.join(".config"))
        .env("CODEX_HOME", &fixture.codex_home)
        .env("CLAUDE_CONFIG_DIR", &fixture.claude_config)
        .env("CASS_DATA_DIR", &fixture.data_dir)
        .env("CASS_IGNORE_SOURCES_CONFIG", "1")
        .env("CASS_AUTO_REFRESH", "0")
        .env("CODING_AGENT_SEARCH_NO_UPDATE_PROMPT", "1")
        .env_remove("CASS_EXCLUDE_PATHS")
        .env_remove("CASS_EXCLUDE_WORKSPACES")
        .current_dir(&fixture.home);
    cmd
}

/// Production ingest: `cass index --full` (a full connector scan so every
/// persist path re-runs its extension publication hook).
fn run_cass_index(fixture: &IngestFixture, extra_env: &[(&str, &str)]) -> Result<()> {
    let mut cmd = cass_cmd(fixture);
    cmd.arg("index")
        .arg("--json")
        .arg("--no-progress-events")
        .arg("--full");
    for (key, value) in extra_env {
        cmd.env(key, value);
    }
    let output = cmd.stdin(Stdio::null()).output()?;
    ensure!(
        output.status.success(),
        "cass index failed: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

/// Production `--init-extensions` writer pass (schema + canonical
/// observers), the same flag an operator runs.
fn run_init_extensions(fixture: &IngestFixture) -> Result<()> {
    let status = Command::new(env!("CARGO_BIN_EXE_cass-viewer-bridge"))
        .arg("--stdio")
        .arg("--init-extensions")
        .arg("--data-dir")
        .arg(&fixture.data_dir)
        .arg("--db")
        .arg(&fixture.db)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .status()?;
    ensure!(status.success(), "--init-extensions failed: {status}");
    Ok(())
}

/// Production `--backfill-viewer-extensions` repair lane (the explicit,
/// resumable reconcile path for pending/failed generations).
fn run_backfill(fixture: &IngestFixture) -> Result<()> {
    let status = Command::new(env!("CARGO_BIN_EXE_cass-viewer-bridge"))
        .arg("--stdio")
        .arg("--backfill-viewer-extensions")
        .arg("--data-dir")
        .arg(&fixture.data_dir)
        .arg("--db")
        .arg(&fixture.db)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .status()?;
    ensure!(
        status.success(),
        "--backfill-viewer-extensions failed: {status}"
    );
    Ok(())
}

/// A real Codex rollout under `<codex_home>/sessions/2026/04/23/` — the
/// connector only reads `rollout-*.jsonl`.
fn seed_codex_rollout(fixture: &IngestFixture, name: &str, user_text: &str) -> Result<PathBuf> {
    let sessions = fixture.codex_home.join("sessions/2026/04/23");
    std::fs::create_dir_all(&sessions)?;
    let path = sessions.join(format!("rollout-2026-04-23T12-00-00-{name}.jsonl"));
    let lines = [
        format!(
            r#"{{"timestamp":"2026-04-23T12:00:00.000Z","type":"session_meta","payload":{{"id":"{name}","cwd":"/work/ingest-test","cli_version":"0.42.0"}}}}"#
        ),
        format!(
            r#"{{"timestamp":"2026-04-23T12:00:01.000Z","type":"response_item","payload":{{"type":"message","role":"user","content":[{{"type":"input_text","text":"{user_text}"}}]}}}}"#
        ),
        format!(
            r#"{{"timestamp":"2026-04-23T12:00:02.000Z","type":"response_item","payload":{{"type":"message","role":"assistant","content":[{{"type":"text","text":"acknowledged {user_text}"}}]}}}}"#
        ),
    ];
    std::fs::write(&path, lines.join("\n") + "\n")?;
    Ok(path)
}

/// A real Claude Code transcript: `<claude_config>/projects/<proj>/<name>.jsonl`.
fn seed_claude_session(
    fixture: &IngestFixture,
    rel_path: &str,
    session_id: &str,
    user_text: &str,
) -> Result<PathBuf> {
    let path = fixture.claude_config.join("projects").join(rel_path);
    std::fs::create_dir_all(path.parent().expect("session dir"))?;
    let lines = [
        format!(
            r#"{{"type":"user","cwd":"/work/proj","sessionId":"{session_id}","gitBranch":"main","message":{{"role":"user","content":"{user_text}"}},"timestamp":"2026-04-23T12:00:00.000Z"}}"#
        ),
        format!(
            r#"{{"type":"assistant","sessionId":"{session_id}","message":{{"role":"assistant","model":"claude-opus-4","content":[{{"type":"text","text":"ready {user_text}"}}]}},"timestamp":"2026-04-23T12:00:05.000Z"}}"#
        ),
    ];
    std::fs::write(&path, lines.join("\n") + "\n")?;
    Ok(path)
}

fn ingest_conn(fixture: &IngestFixture) -> Result<Connection> {
    Ok(Connection::open(fixture.db.to_string_lossy().into_owned())?)
}

/// The canonical id of the fixture conversation — resolved from committed
/// rows (never assumed to be `1`).
fn canonical_id(conn: &Connection, source_path_match: &str) -> Result<i64> {
    let rows = conn.query_with_params(
        "SELECT id FROM conversations WHERE source_path LIKE ?1",
        &param_slice_to_values(params![format!("%{source_path_match}%")]),
    )?;
    ensure!(
        rows.len() == 1,
        "expected one conversation matching {source_path_match}: {} rows",
        rows.len()
    );
    Ok(rows[0].get_typed(0)?)
}

/// `viewer_meta` receipt payload for one session, decoded as JSON.
fn ingest_receipt(conn: &Connection, conversation_id: i64) -> Result<Option<Value>> {
    let rows = conn.query_with_params(
        "SELECT value FROM viewer_meta WHERE key = ?1",
        &param_slice_to_values(params![format!("ingest_receipt:{conversation_id}")]),
    )?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let value: String = row.get_typed(0)?;
    Ok(Some(serde_json::from_str(&value)?))
}

/// The verbatim generation-coherence assertion from the task brief: facts
/// and projection must carry the SAME canonical source revision.
fn assert_generation_coherent(conn: &Connection, conversation_id: i64) -> Result<()> {
    let row = conn.query_row(&format!(
        "SELECT f.source_revision,p.source_revision \
         FROM viewer_session_facts f JOIN viewer_session_projection p \
         ON p.conversation_id=f.conversation_id WHERE f.conversation_id={conversation_id}"
    ))?;
    ensure!(row.get_typed::<i64>(0)? == row.get_typed::<i64>(1)?);
    Ok(())
}

/// `f.source_revision` of one session's facts row.
fn facts_source_revision(conn: &Connection, conversation_id: i64) -> Result<i64> {
    Ok(conn
        .query_row(&format!(
            "SELECT source_revision FROM viewer_session_facts WHERE conversation_id={conversation_id}"
        ))?
        .get_typed(0)?)
}

fn projection_revision(conn: &Connection) -> Result<i64> {
    Ok(conn
        .query_row("SELECT projection_revision FROM viewer_revisions WHERE id = 1")?
        .get_typed(0)?)
}

/// No extra physical SQLite databases may appear next to the canonical
/// archive — the one-db rule (WAL/SHM/journal companions are internals of
/// the SAME `agent_search.db`, and the lexical index dir is not SQLite).
fn assert_no_sidecar_db(fixture: &IngestFixture) -> Result<()> {
    let mut stray = Vec::new();
    for entry in std::fs::read_dir(&fixture.data_dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.ends_with(".db") && name != "agent_search.db" {
            stray.push(name);
        }
    }
    ensure!(
        stray.is_empty(),
        "a second physical database appeared: {stray:?}"
    );
    Ok(())
}

/// Extension-row presence for one canonical session, all lanes.
fn extension_row_counts(conn: &Connection, conversation_id: i64) -> Result<(i64, i64, i64)> {
    let facts_n: i64 = conn
        .query_row(&format!(
            "SELECT COUNT(*) FROM viewer_session_facts WHERE conversation_id={conversation_id}"
        ))?
        .get_typed(0)?;
    let projection_n: i64 = conn
        .query_row(&format!(
            "SELECT COUNT(*) FROM viewer_session_projection WHERE conversation_id={conversation_id}"
        ))?
        .get_typed(0)?;
    let steps_n: i64 = conn
        .query_row(&format!(
            "SELECT COUNT(*) FROM viewer_step_links WHERE conversation_id={conversation_id}"
        ))?
        .get_typed(0)?;
    Ok((facts_n, projection_n, steps_n))
}

/// First ingest publishes the whole generation: same `source_revision` on
/// both sides, structural rows present, and a durable `complete` receipt in
/// `viewer_meta` naming the extractor/projection versions.
#[test]
fn ingest_publishes_coherent_extension_generation() -> Result<()> {
    let fixture = ingest_fixture()?;
    seed_codex_rollout(&fixture, "gen-codex-1", "build the extension lane")?;

    run_cass_index(&fixture, &[])?;
    run_init_extensions(&fixture)?;

    let conn = ingest_conn(&fixture)?;
    let conversation_id = canonical_id(&conn, "rollout-")?;

    // Exact assertion from the task brief (the fixture's real canonical id
    // resolved from committed rows, never assumed).
    let row = conn.query_row(&format!(
        "SELECT f.source_revision,p.source_revision \
         FROM viewer_session_facts f JOIN viewer_session_projection p \
         ON p.conversation_id=f.conversation_id WHERE f.conversation_id={conversation_id}"
    ))?;
    ensure!(row.get_typed::<i64>(0)? == row.get_typed::<i64>(1)?);

    // Structural projection rows actually landed.
    let (_, projection_n, steps_n) = extension_row_counts(&conn, conversation_id)?;
    ensure!(
        projection_n == 1,
        "projection marker missing: {projection_n}"
    );
    ensure!(
        steps_n > 0,
        "no step links projected for the fixture session"
    );

    // Durable publication receipt: complete, versioned, no failure reason.
    let receipt =
        ingest_receipt(&conn, conversation_id)?.expect("ingest publication receipt in viewer_meta");
    ensure!(
        receipt["state"] == "complete",
        "receipt must be complete after a clean ingest: {receipt}"
    );
    ensure!(
        receipt["source_revision"].as_i64() == Some(facts_source_revision(&conn, conversation_id)?),
        "receipt must bind the same source_revision as the rows: {receipt}"
    );
    ensure!(
        receipt["extractor_version"].as_i64() == Some(facts::FACTS_EXTRACTOR_VERSION)
            && receipt["projection_version"].as_i64()
                == Some(coding_agent_search::viewer_extensions::projection::PROJECTION_VERSION),
        "receipt must name the current extractor/projection versions: {receipt}"
    );
    ensure!(
        receipt["reason"].is_null(),
        "clean ingest records no reason: {receipt}"
    );

    ensure!(
        projection_revision(&conn)? >= 1,
        "a committed generation bumps projection_revision"
    );
    assert_no_sidecar_db(&fixture)?;
    drop(conn);
    Ok(())
}

/// Unchanged reingest must not churn the generation: identical
/// `source_revision` on both sides, `projection_revision` unmoved, receipt
/// row untouched.
#[test]
fn ingest_unchanged_reingest_keeps_generation_and_revisions() -> Result<()> {
    let fixture = ingest_fixture()?;
    seed_codex_rollout(&fixture, "gen-codex-2", "idempotent reingest")?;

    run_cass_index(&fixture, &[])?;
    run_init_extensions(&fixture)?;

    let conn = ingest_conn(&fixture)?;
    let conversation_id = canonical_id(&conn, "rollout-")?;
    let revision_before = facts_source_revision(&conn, conversation_id)?;
    let projection_revision_before = projection_revision(&conn)?;
    let receipt_before = ingest_receipt(&conn, conversation_id)?.expect("receipt");
    ensure!(receipt_before["state"] == "complete", "{receipt_before}");
    drop(conn);

    // Reingest the same sources — unchanged canonical content republishes
    // nothing and touches no revision counter.
    run_cass_index(&fixture, &[])?;
    let conn = ingest_conn(&fixture)?;
    assert_generation_coherent(&conn, conversation_id)?;
    ensure!(
        facts_source_revision(&conn, conversation_id)? == revision_before,
        "unchanged content must keep the same source_revision"
    );
    ensure!(
        projection_revision(&conn)? == projection_revision_before,
        "unchanged reingest must not bump projection_revision: \
         {projection_revision_before} -> {}",
        projection_revision(&conn)?
    );
    let receipt_after = ingest_receipt(&conn, conversation_id)?.expect("receipt");
    ensure!(
        receipt_after == receipt_before,
        "unchanged reingest must not rewrite the receipt: {receipt_after} vs {receipt_before}"
    );
    drop(conn);
    Ok(())
}

/// Changed canonical content republishes BOTH sides at the new
/// `source_revision` — never facts-new/projection-stale.
#[test]
fn ingest_changed_content_republishes_same_revision_on_both_sides() -> Result<()> {
    let fixture = ingest_fixture()?;
    let session_path = seed_codex_rollout(&fixture, "gen-codex-3", "first body")?;

    run_cass_index(&fixture, &[])?;
    run_init_extensions(&fixture)?;

    let conn = ingest_conn(&fixture)?;
    let conversation_id = canonical_id(&conn, "rollout-")?;
    let revision_before = facts_source_revision(&conn, conversation_id)?;
    let projection_revision_before = projection_revision(&conn)?;
    drop(conn);

    // Append a new assistant message — the canonical merge changes
    // semantic_hash, so a NEW generation must land on both sides.
    let appended = format!(
        "{}\n",
        r#"{"timestamp":"2026-04-23T12:00:03.000Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"text","text":"second body"}]}}"#
    );
    std::fs::write(
        &session_path,
        format!("{}{appended}", std::fs::read_to_string(&session_path)?),
    )?;
    run_cass_index(&fixture, &[])?;

    let conn = ingest_conn(&fixture)?;
    assert_generation_coherent(&conn, conversation_id)?;
    let revision_after = facts_source_revision(&conn, conversation_id)?;
    ensure!(
        revision_after != revision_before,
        "changed content must produce a new source_revision"
    );
    ensure!(
        projection_revision(&conn)? == projection_revision_before + 1,
        "changed generation bumps projection_revision exactly once"
    );
    let receipt = ingest_receipt(&conn, conversation_id)?.expect("receipt");
    ensure!(
        receipt["state"] == "complete"
            && receipt["source_revision"].as_i64() == Some(revision_after),
        "receipt must move to the new generation complete: {receipt}"
    );
    drop(conn);
    Ok(())
}

/// Canonical removal removes the whole extension generation with the
/// session: facts, projection, structural links and the receipt — no
/// orphan rows, no orphan receipt claiming coverage.
#[test]
fn ingest_removal_drops_extension_rows_and_receipt() -> Result<()> {
    let fixture = ingest_fixture()?;
    seed_codex_rollout(&fixture, "gen-codex-4", "session to forget")?;

    run_cass_index(&fixture, &[])?;
    run_init_extensions(&fixture)?;

    let conn = ingest_conn(&fixture)?;
    let conversation_id = canonical_id(&conn, "rollout-")?;
    let (facts_n, projection_n, steps_n) = extension_row_counts(&conn, conversation_id)?;
    ensure!(
        facts_n == 1 && projection_n == 1 && steps_n > 0,
        "fixture must have extension rows before removal"
    );
    ensure!(ingest_receipt(&conn, conversation_id)?.is_some());
    drop(conn);

    // Production removal: `cass forget --apply` deletes the canonical row.
    let mut cmd = cass_cmd(&fixture);
    cmd.arg("forget")
        .arg("--json")
        .arg("--source-glob")
        .arg("*rollout*")
        .arg("--apply");
    let output = cmd.stdin(Stdio::null()).output()?;
    ensure!(
        output.status.success(),
        "cass forget failed: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let conn = ingest_conn(&fixture)?;
    ensure!(
        count(&conn, "SELECT COUNT(*) FROM conversations")? == 0,
        "canonical row must be removed"
    );
    let (facts_n, projection_n, steps_n) = extension_row_counts(&conn, conversation_id)?;
    ensure!(
        facts_n == 0 && projection_n == 0 && steps_n == 0,
        "removal must drop every extension row: {facts_n}/{projection_n}/{steps_n}"
    );
    ensure!(
        ingest_receipt(&conn, conversation_id)?.is_none(),
        "removal must drop the publication receipt — a deleted session \
         can never claim coverage"
    );
    drop(conn);
    Ok(())
}

/// Interrupted publication: the receipt rides the generation transaction,
/// so a crash before that commit leaves the pre-attempt state — NO receipt
/// and NO rows, never a falsely complete archive (nothing claims coverage
/// the rows do not have). The explicit resumable backfill converges it to
/// `complete` with no loss or duplication.
#[test]
fn ingest_interrupted_publication_leaves_no_false_state_and_backfill_completes() -> Result<()> {
    let fixture = ingest_fixture()?;
    seed_codex_rollout(&fixture, "gen-codex-5", "crash mid publication")?;

    // Crash injection: the process "dies" before the generation transaction
    // commits — with the receipt folded in, that means nothing durable was
    // written this attempt.
    run_cass_index(&fixture, &[("CASS_TEST_VIEWER_GENERATION_CRASH", "1")])?;
    run_init_extensions(&fixture)?;

    let conn = ingest_conn(&fixture)?;
    let conversation_id = canonical_id(&conn, "rollout-")?;
    let (facts_n, projection_n, steps_n) = extension_row_counts(&conn, conversation_id)?;
    ensure!(
        facts_n == 0 && projection_n == 0 && steps_n == 0,
        "an interrupted publication must leave no partial rows: \
         {facts_n}/{projection_n}/{steps_n}"
    );
    ensure!(
        ingest_receipt(&conn, conversation_id)?.is_none(),
        "a crash before the generation commit must leave NO receipt — \
         the receipt only becomes durable with the rows it certifies"
    );
    ensure!(
        facts::list_incomplete_ingest_receipts(&conn)?.is_empty(),
        "a receipt-less crash must not surface as an incomplete receipt"
    );
    drop(conn);

    // Restart + explicit resumable backfill converges without loss or
    // duplication.
    run_backfill(&fixture)?;

    let conn = ingest_conn(&fixture)?;
    assert_generation_coherent(&conn, conversation_id)?;
    let (facts_n, projection_n, steps_n) = extension_row_counts(&conn, conversation_id)?;
    ensure!(
        facts_n == 1 && projection_n == 1 && steps_n > 0,
        "backfill must complete the generation exactly once: \
         {facts_n}/{projection_n}/{steps_n}"
    );
    let receipt = ingest_receipt(&conn, conversation_id)?.expect("receipt");
    ensure!(
        receipt["state"] == "complete",
        "backfill must mark the receipt complete: {receipt}"
    );
    ensure!(
        facts::list_incomplete_ingest_receipts(&conn)?.is_empty(),
        "no incomplete receipts may survive the reconcile"
    );
    drop(conn);
    Ok(())
}

/// Failed publication: an error inside the generation transaction rolls
/// the whole conversation back — no facts row without its structure — and
/// the durable receipt records `failed` with the exact reason. A plain
/// re-ingest of unchanged content does NOT silently repair it; only the
/// explicit backfill converges it.
#[test]
fn ingest_failed_publication_records_reason_and_stays_incomplete() -> Result<()> {
    let fixture = ingest_fixture()?;
    seed_codex_rollout(&fixture, "gen-codex-6", "fails mid transaction")?;

    run_cass_index(&fixture, &[("CASS_TEST_VIEWER_GENERATION_FAIL", "1")])?;
    run_init_extensions(&fixture)?;

    let conn = ingest_conn(&fixture)?;
    let conversation_id = canonical_id(&conn, "rollout-")?;
    let (facts_n, projection_n, steps_n) = extension_row_counts(&conn, conversation_id)?;
    ensure!(
        facts_n == 0 && projection_n == 0 && steps_n == 0,
        "a failed generation must roll back atomically — no facts row \
         without structure: {facts_n}/{projection_n}/{steps_n}"
    );
    let receipt = ingest_receipt(&conn, conversation_id)?.expect("failure receipt");
    ensure!(
        receipt["state"] == "failed",
        "a rolled-back publication must be marked failed: {receipt}"
    );
    ensure!(
        receipt["reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("CASS_TEST_VIEWER_GENERATION_FAIL")),
        "the receipt must record the exact failure reason: {receipt}"
    );
    drop(conn);

    // An unchanged re-ingest is the same generation the receipt already
    // marks attempted — it retries the publication but this injected fault
    // is deterministic, so it stays failed and never advertises complete.
    run_cass_index(&fixture, &[("CASS_TEST_VIEWER_GENERATION_FAIL", "1")])?;
    let conn = ingest_conn(&fixture)?;
    let receipt = ingest_receipt(&conn, conversation_id)?.expect("receipt");
    ensure!(
        receipt["state"] == "failed",
        "the deterministic fault must keep the receipt failed: {receipt}"
    );
    let (facts_n, _, _) = extension_row_counts(&conn, conversation_id)?;
    ensure!(facts_n == 0, "still no partial coverage may be committed");
    drop(conn);

    // Explicit resumable backfill with the fault cleared converges.
    run_backfill(&fixture)?;
    let conn = ingest_conn(&fixture)?;
    assert_generation_coherent(&conn, conversation_id)?;
    let receipt = ingest_receipt(&conn, conversation_id)?.expect("receipt");
    ensure!(receipt["state"] == "complete", "{receipt}");
    ensure!(
        facts::list_incomplete_ingest_receipts(&conn)?.is_empty(),
        "reconcile must drain every incomplete receipt"
    );
    drop(conn);
    Ok(())
}

/// Child-before-parent: a subagent transcript ingested before its parent
/// records the parent evidence but leaves `parent_conversation_id` NULL
/// (honest unresolved). When the parent session arrives on a later
/// `cass index`, the targeted fixup resolves the child's parent link —
/// same-transaction, no full-catalog rescan.
#[test]
fn ingest_child_before_parent_resolves_on_parent_arrival() -> Result<()> {
    let fixture = ingest_fixture()?;
    let parent_session = "parent-sess-abc";
    // Child first: `projects/proj/<parent>/subagents/agent-child.jsonl` —
    // the connector derives `parentSessionId` = "<parent>" from the path.
    seed_claude_session(
        &fixture,
        &format!("proj/{parent_session}/subagents/agent-child.jsonl"),
        "child-sess-xyz",
        "child prompt",
    )?;
    run_cass_index(&fixture, &[])?;
    run_init_extensions(&fixture)?;

    let conn = ingest_conn(&fixture)?;
    let child_id = canonical_id(&conn, "agent-child")?;
    let row = conn.query_row(&format!(
        "SELECT parent_conversation_id, parent_external_id FROM viewer_session_facts \
         WHERE conversation_id={child_id}"
    ))?;
    ensure!(
        row.get_typed::<Option<i64>>(0)?.is_none(),
        "child published before parent must leave parent_conversation_id NULL"
    );
    ensure!(
        row.get_typed::<Option<String>>(1)?.as_deref() == Some(parent_session),
        "the recorded parent evidence must be the path-derived session id"
    );
    let detail: String = conn
        .query_row(&format!(
            "SELECT coverage_detail_json FROM viewer_session_facts \
             WHERE conversation_id={child_id}"
        ))?
        .get_typed(0)?;
    let detail: Value = serde_json::from_str(&detail)?;
    ensure!(
        detail["parent"]["resolution"] == "unresolved",
        "unresolved evidence stays honest: {detail}"
    );
    drop(conn);

    // Parent arrives on a later ingest — the targeted fixup resolves the
    // child's parent_conversation_id in the parent's own publication
    // transaction (no child republish needed).
    seed_claude_session(
        &fixture,
        &format!("proj/{parent_session}.jsonl"),
        parent_session,
        "parent prompt",
    )?;
    run_cass_index(&fixture, &[])?;

    let conn = ingest_conn(&fixture)?;
    let parent_id = canonical_id(&conn, &format!("{parent_session}.jsonl"))?;
    let resolved: Option<i64> = conn
        .query_row(&format!(
            "SELECT parent_conversation_id FROM viewer_session_facts \
             WHERE conversation_id={child_id}"
        ))?
        .get_typed(0)?;
    ensure!(
        resolved == Some(parent_id),
        "parent arrival must resolve the child's parent link: \
         resolved={resolved:?} parent_id={parent_id}"
    );
    let detail: String = conn
        .query_row(&format!(
            "SELECT coverage_detail_json FROM viewer_session_facts \
             WHERE conversation_id={child_id}"
        ))?
        .get_typed(0)?;
    let detail: Value = serde_json::from_str(&detail)?;
    ensure!(
        detail["parent"]["resolution"] == "resolved",
        "fixup must refresh the coverage manifest honestly: {detail}"
    );
    assert_generation_coherent(&conn, child_id)?;
    assert_generation_coherent(&conn, parent_id)?;
    drop(conn);
    Ok(())
}

/// Review fix (Important #2): a child whose forward resolution was
/// `ambiguous` — its evidence matched several parents at publish time —
/// must NEVER be claimed by a later matching parent. The fixup leaves the
/// link NULL and keeps the recorded resolution instead of rewriting the
/// manifest to `resolved` around an arbitrary winner.
#[test]
fn ingest_ambiguous_child_is_never_claimed_by_later_parent() -> Result<()> {
    let fixture = ingest_fixture()?;
    let parent_session = "parent-sess-amb";
    // Two distinct parents whose path-derived external_ids BOTH match the
    // child's recorded evidence (`%parent-sess-amb.jsonl`) — the forward
    // resolution must stay ambiguous. They publish BEFORE the child so the
    // ambiguity is deterministic, not ingest-order dependent.
    seed_claude_session(
        &fixture,
        &format!("projA/{parent_session}.jsonl"),
        parent_session,
        "parent A",
    )?;
    seed_claude_session(
        &fixture,
        &format!("projB/{parent_session}.jsonl"),
        parent_session,
        "parent B",
    )?;
    run_cass_index(&fixture, &[])?;
    run_init_extensions(&fixture)?;

    // The subagent transcript arrives later and records its parent
    // evidence durably — but two canonical parents already match it.
    seed_claude_session(
        &fixture,
        &format!("projX/{parent_session}/subagents/agent-amb.jsonl"),
        "child-amb-1",
        "ambiguous child prompt",
    )?;
    run_cass_index(&fixture, &[])?;

    let conn = ingest_conn(&fixture)?;
    let child_id = canonical_id(&conn, "agent-amb")?;
    let row = conn.query_row(&format!(
        "SELECT parent_conversation_id, parent_external_id, coverage_detail_json \
         FROM viewer_session_facts WHERE conversation_id={child_id}"
    ))?;
    ensure!(
        row.get_typed::<Option<i64>>(0)?.is_none(),
        "ambiguous evidence must leave the link NULL at publish"
    );
    ensure!(
        row.get_typed::<Option<String>>(1)?.as_deref() == Some(parent_session),
        "the recorded parent evidence must be the path-derived session id"
    );
    let detail: Value = serde_json::from_str(&row.get_typed::<String>(2)?)?;
    ensure!(
        detail["parent"]["resolution"] == "ambiguous",
        "two matching parents at publish time record an ambiguous resolution: {detail}"
    );
    drop(conn);

    // A THIRD matching parent arrives later — its own publication fixup
    // must not claim the child the forward pass deliberately left
    // ambiguous.
    seed_claude_session(
        &fixture,
        &format!("projC/{parent_session}.jsonl"),
        parent_session,
        "parent C",
    )?;
    run_cass_index(&fixture, &[])?;

    let conn = ingest_conn(&fixture)?;
    let row = conn.query_row(&format!(
        "SELECT parent_conversation_id, coverage_detail_json \
         FROM viewer_session_facts WHERE conversation_id={child_id}"
    ))?;
    ensure!(
        row.get_typed::<Option<i64>>(0)?.is_none(),
        "a later matching parent must not claim an ambiguous child"
    );
    let detail: Value = serde_json::from_str(&row.get_typed::<String>(1)?)?;
    ensure!(
        detail["parent"]["resolution"] == "ambiguous",
        "the manifest keeps its recorded ambiguous resolution: {detail}"
    );
    drop(conn);
    Ok(())
}

/// Review fix (Important #3): `cass forget` of a resolved parent must not
/// be refused by the `parent_conversation_id` reference its children
/// hold — the `viewer_session_facts_parent_cleanup` trigger NULLs the
/// link inside the delete, so the child keeps its own rows and its
/// durable evidence can re-resolve on a later matching parent.
#[test]
fn ingest_forget_parent_nulls_resolved_child_link() -> Result<()> {
    let fixture = ingest_fixture()?;
    let parent_session = "parent-sess-del";
    // Child first, then parent — the same resolved-link setup the
    // child-before-parent test pins.
    seed_claude_session(
        &fixture,
        &format!("proj/{parent_session}/subagents/agent-del.jsonl"),
        "child-del-1",
        "child prompt",
    )?;
    run_cass_index(&fixture, &[])?;
    run_init_extensions(&fixture)?;
    seed_claude_session(
        &fixture,
        &format!("proj/{parent_session}.jsonl"),
        parent_session,
        "parent prompt",
    )?;
    run_cass_index(&fixture, &[])?;

    let conn = ingest_conn(&fixture)?;
    let child_id = canonical_id(&conn, "agent-del")?;
    let parent_id = canonical_id(&conn, &format!("{parent_session}.jsonl"))?;
    let resolved: Option<i64> = conn
        .query_row(&format!(
            "SELECT parent_conversation_id FROM viewer_session_facts \
             WHERE conversation_id={child_id}"
        ))?
        .get_typed(0)?;
    ensure!(
        resolved == Some(parent_id),
        "setup: the child's parent link must resolve before the forget"
    );
    drop(conn);

    // Production removal of the parent only — the glob anchors on the
    // trailing filename so the child's `subagents/` path cannot match.
    let mut cmd = cass_cmd(&fixture);
    cmd.arg("forget")
        .arg("--json")
        .arg("--source-glob")
        .arg(format!("*{parent_session}.jsonl"))
        .arg("--apply");
    let output = cmd.stdin(Stdio::null()).output()?;
    ensure!(
        output.status.success(),
        "cass forget of a resolved parent must not be FK-refused: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let conn = ingest_conn(&fixture)?;
    ensure!(
        count(
            &conn,
            &format!("SELECT COUNT(*) FROM conversations WHERE id={parent_id}")
        )? == 0,
        "the parent conversation must be removed"
    );
    let row = conn.query_row(&format!(
        "SELECT parent_conversation_id, parent_external_id \
         FROM viewer_session_facts WHERE conversation_id={child_id}"
    ))?;
    ensure!(
        row.get_typed::<Option<i64>>(0)?.is_none(),
        "the delete trigger must NULL the orphaned parent link"
    );
    ensure!(
        row.get_typed::<Option<String>>(1)?.as_deref() == Some(parent_session),
        "the durable evidence stays so a later matching parent can re-resolve"
    );
    ensure!(
        count(
            &conn,
            &format!("SELECT COUNT(*) FROM conversations WHERE id={child_id}")
        )? == 1,
        "the child conversation itself is untouched by the parent's removal"
    );
    drop(conn);
    Ok(())
}

/// Review fix (Important #2, end to end): a user running ONLY `cass index`
/// must not be stranded on an archive-wide `facts_pending` browse refusal.
/// The ingest publication hook's profile guard probes the pending tie set
/// and converges it in bounded batches — no `--init-extensions`, no
/// `--backfill-viewer-extensions`, no maintenance command anywhere.
#[test]
fn ingest_index_converges_pending_sort_ties() -> Result<()> {
    let fixture = ingest_fixture()?;
    seed_claude_session(&fixture, "proj/pending-a.jsonl", "pending-a", "first body")?;

    // First index: the publish hook installs the profile AND resolves the
    // session's tie inside the same plain `cass index` run.
    run_cass_index(&fixture, &[])?;
    let conn = ingest_conn(&fixture)?;
    let pending = count(
        &conn,
        "SELECT COUNT(*) FROM viewer_session_sort \
         WHERE tie_key IS NULL AND tie_error IS NULL",
    )?;
    ensure!(
        pending == 0,
        "first index must leave nothing pending: {pending}"
    );

    // The upgrade window: every sort row's verified tie is returned to
    // pending — the state a schema migration or dimension invalidation
    // can only produce (the trigger layer cannot compute a verified
    // public identity).
    conn.execute("UPDATE viewer_session_sort SET tie_key = NULL, tie_error = NULL")?;
    let pending = count(
        &conn,
        "SELECT COUNT(*) FROM viewer_session_sort \
         WHERE tie_key IS NULL AND tie_error IS NULL",
    )?;
    ensure!(
        pending == 1,
        "setup must leave the whole set pending: {pending}"
    );
    drop(conn);

    // A second session gives the next plain index run real persist work;
    // its publication hook converges the whole pending set, not only its
    // own row.
    seed_claude_session(&fixture, "proj/pending-b.jsonl", "pending-b", "second body")?;
    run_cass_index(&fixture, &[])?;

    let conn = ingest_conn(&fixture)?;
    let pending = count(
        &conn,
        "SELECT COUNT(*) FROM viewer_session_sort \
         WHERE tie_key IS NULL AND tie_error IS NULL",
    )?;
    ensure!(
        pending == 0,
        "plain cass index must converge pending ties in bounded batches: {pending} remain"
    );
    // And the persisted values are the verified public identities — never
    // a guessed or stale key left trusted.
    let keys: Vec<String> = conn
        .query("SELECT tie_key FROM viewer_session_sort ORDER BY conversation_id")?
        .iter()
        .map(|row| row.get_typed::<String>(0))
        .collect::<Result<Vec<_>, _>>()?;
    ensure!(
        keys == ["claude:pending-a", "claude:pending-b"],
        "converged keys must be verified public identities: {keys:?}"
    );
    drop(conn);
    Ok(())
}

/// Review fix (Important #3): the tie resolve used to run before the
/// generation transaction while its write ran inside it — a canonical
/// identity change in the gap NULLed the stored tie via the conversations
/// trigger and the stale pre-transaction key overwrote it, never to be
/// re-invalidated. The evidence-guarded write re-reads the identity
/// columns inside the transaction: a raced basis refuses, the attempt
/// rolls back, and the retry re-resolves against the moved row.
/// `CASS_TEST_VIEWER_TIE_RACE_SQL` injects the mutation at exactly the
/// old race window — deterministically, every run.
#[test]
fn ingest_tie_publish_guards_against_racing_identity_change() -> Result<()> {
    let fixture = ingest_fixture()?;
    seed_claude_session(&fixture, "proj/race-a.jsonl", "race-a", "race prompt")?;

    // The probe mutates the canonical identity between the resolve probes
    // and the generation transaction — the exact interleaving the finding
    // describes. source_path+external_id move together so the post-race
    // identity still verifies to ONE new public id.
    run_cass_index(
        &fixture,
        &[(
            "CASS_TEST_VIEWER_TIE_RACE_SQL",
            "UPDATE conversations \
             SET external_id='racedAAA', source_path='/raced/racedAAA.jsonl'",
        )],
    )?;

    let conn = ingest_conn(&fixture)?;
    let conversation_id = canonical_id(&conn, "racedAAA")?;
    let row = conn.query_row(&format!(
        "SELECT tie_key, tie_error FROM viewer_session_sort \
         WHERE conversation_id={conversation_id}"
    ))?;
    ensure!(
        row.get_typed::<Option<String>>(0)?.as_deref() == Some("claude:racedAAA"),
        "the guarded write must persist the post-mutation identity, \
         never the stale pre-resolve key: {row:?}"
    );
    ensure!(
        row.get_typed::<Option<String>>(1)?.is_none(),
        "a re-resolved race must not record a refusal: {row:?}"
    );
    // The mutation provably landed — the guard re-resolved against the
    // moved canonical row inside its own attempt window.
    let external: String = conn
        .query_row(&format!(
            "SELECT external_id FROM conversations WHERE id={conversation_id}"
        ))?
        .get_typed(0)?;
    ensure!(
        external == "racedAAA",
        "the injected identity change must be visible: {external}"
    );
    // Generation publication itself survived the raced attempt cleanly.
    let receipt = ingest_receipt(&conn, conversation_id)?.expect("receipt");
    ensure!(
        receipt["state"] == "complete",
        "the retry must still land a complete receipt: {receipt}"
    );
    drop(conn);
    Ok(())
}

/// Review fix-round-2 (Important, repair lane): `repair_sort_keys` used to
/// resolve committed state outside its chunk transaction and then blindly
/// UPDATE inside it — the same stale-tie race the ingest lane's evidence
/// guard was built for. `CASS_TEST_VIEWER_REPAIR_RACE_SQL` injects a
/// canonical identity mutation after a repair chunk's resolve probes and
/// before its write transaction opens (the exact interleaving of a
/// cross-connection canonical write racing the pass): the guarded write
/// must refuse the stale basis so the row stays pending or keys to the
/// moved identity — never a stale `claude:repair-race` left trusted.
/// The probe is scoped to the seeded row's canonical `external_id` (the
/// project-relative session path) so it is a no-op on the later passes
/// that re-resolve the moved row.
#[test]
fn repair_sort_tie_publish_guards_against_racing_identity_change() -> Result<()> {
    let fixture = ingest_fixture()?;
    seed_claude_session(
        &fixture,
        "proj/repair-race.jsonl",
        "repair-race",
        "race body",
    )?;
    run_cass_index(&fixture, &[])?;

    // Force the pending state a migration or dimension invalidation
    // leaves: the trigger layer can only NULL the tie, never recompute
    // a verified public identity.
    let conn = ingest_conn(&fixture)?;
    conn.execute("UPDATE viewer_session_sort SET tie_key = NULL, tie_error = NULL")?;
    let pending = count(
        &conn,
        "SELECT COUNT(*) FROM viewer_session_sort \
         WHERE tie_key IS NULL AND tie_error IS NULL",
    )?;
    ensure!(pending == 1, "setup must leave the tie pending: {pending}");
    drop(conn);

    // A second session gives the index run publish work; its profile
    // guard drives the repair pass that races the injected mutation.
    seed_claude_session(&fixture, "proj/repair-b.jsonl", "repair-b", "other body")?;
    run_cass_index(
        &fixture,
        &[(
            "CASS_TEST_VIEWER_REPAIR_RACE_SQL",
            "UPDATE conversations \
             SET external_id='repairRACED', source_path='/raced/repairRACED.jsonl' \
             WHERE external_id='proj/repair-race.jsonl'",
        )],
    )?;

    let conn = ingest_conn(&fixture)?;
    let conversation_id = canonical_id(&conn, "repairRACED")?;
    let row = conn.query_row(&format!(
        "SELECT tie_key, tie_error FROM viewer_session_sort \
         WHERE conversation_id={conversation_id}"
    ))?;
    let tie_key: Option<String> = row.get_typed(0)?;
    let tie_error: Option<String> = row.get_typed(1)?;
    ensure!(
        tie_key.as_deref() != Some("claude:repair-race"),
        "the stale pre-race key must never persist: {row:?}"
    );
    ensure!(
        tie_key.is_none() || tie_key.as_deref() == Some("claude:repairRACED"),
        "the raced row must be pending or keyed to the moved identity: {row:?}"
    );
    ensure!(
        tie_error.is_none(),
        "a guard-refused row stays pending — never a fabricated refusal: {row:?}"
    );
    // The mutation provably landed inside the old race window.
    let external: String = conn
        .query_row(&format!(
            "SELECT external_id FROM conversations WHERE id={conversation_id}"
        ))?
        .get_typed(0)?;
    ensure!(
        external == "repairRACED",
        "the injected identity change must be visible: {external}"
    );
    drop(conn);

    // The claimed contract — refused rows stay pending for the NEXT
    // pass: a further publish converges the row to the post-mutation
    // identity (the scoped probe is a no-op once the row has moved).
    seed_claude_session(&fixture, "proj/repair-c.jsonl", "repair-c", "third body")?;
    run_cass_index(
        &fixture,
        &[(
            "CASS_TEST_VIEWER_REPAIR_RACE_SQL",
            "UPDATE conversations \
             SET external_id='repairRACED', source_path='/raced/repairRACED.jsonl' \
             WHERE external_id='proj/repair-race.jsonl'",
        )],
    )?;
    let conn = ingest_conn(&fixture)?;
    let row = conn.query_row(&format!(
        "SELECT tie_key, tie_error FROM viewer_session_sort \
         WHERE conversation_id={conversation_id}"
    ))?;
    ensure!(
        row.get_typed::<Option<String>>(0)?.as_deref() == Some("claude:repairRACED"),
        "the next pass must converge the row to the moved identity: {row:?}"
    );
    ensure!(
        row.get_typed::<Option<String>>(1)?.is_none(),
        "converged row carries no refusal: {row:?}"
    );
    drop(conn);
    Ok(())
}

/// Review fix-round-2 (adjacent, repair lane): a conversation deleted
/// between the repair pass's pending-list read and its chunk write used
/// to regain a bare `viewer_session_sort` row through the unguarded
/// `INSERT OR IGNORE` — with FK enforcement off on the ingest writer
/// that is a permanent `session_not_found` wedge. The injected delete
/// lands in the old race window: the `WHERE EXISTS` row-ensure must
/// leave nothing dangling behind.
#[test]
fn repair_sort_tie_delete_race_leaves_no_dangling_row() -> Result<()> {
    let fixture = ingest_fixture()?;
    seed_claude_session(&fixture, "proj/repair-del.jsonl", "repair-del", "delete me")?;
    run_cass_index(&fixture, &[])?;

    let conn = ingest_conn(&fixture)?;
    let conversation_id = canonical_id(&conn, "repair-del")?;
    conn.execute("UPDATE viewer_session_sort SET tie_key = NULL, tie_error = NULL")?;
    drop(conn);

    // The delete races the repair chunk that carries the pending row.
    seed_claude_session(&fixture, "proj/repair-del-b.jsonl", "repair-del-b", "kept")?;
    run_cass_index(
        &fixture,
        &[(
            "CASS_TEST_VIEWER_REPAIR_RACE_SQL",
            "DELETE FROM conversations WHERE external_id='proj/repair-del.jsonl'",
        )],
    )?;

    let conn = ingest_conn(&fixture)?;
    ensure!(
        count(
            &conn,
            &format!("SELECT COUNT(*) FROM conversations WHERE id={conversation_id}")
        )? == 0,
        "the injected delete must have landed"
    );
    ensure!(
        count(
            &conn,
            &format!(
                "SELECT COUNT(*) FROM viewer_session_sort \
                 WHERE conversation_id={conversation_id}"
            )
        )? == 0,
        "a deleted conversation must not regain a dangling sort row \
         (the conversations-del trigger removed it; no guarded insert may resurrect it)"
    );
    ensure!(
        count(
            &conn,
            "SELECT COUNT(*) FROM viewer_session_sort \
             WHERE tie_key IS NULL AND tie_error IS NULL"
        )? == 0,
        "nothing may stay pending on a row that no longer exists"
    );

    // The un-raced sibling converged through the same pass normally.
    let kept_id = canonical_id(&conn, "repair-del-b")?;
    let row = conn.query_row(&format!(
        "SELECT tie_key FROM viewer_session_sort WHERE conversation_id={kept_id}"
    ))?;
    ensure!(
        row.get_typed::<Option<String>>(0)?.as_deref() == Some("claude:repair-del-b"),
        "the surviving conversation must carry its verified key: {row:?}"
    );
    drop(conn);
    Ok(())
}

// ---------------------------------------------------------------------------
// Engine capability probe (Task 4 audit evidence): pins the FrankenSQLite
// plan/codegen behaviors the indexed browse design depends on — a
// multi-column ORDER BY (ASC/DESC mix) satisfied from a composite index
// with no SorterOpen, IN-list probes compiled to repeated SeekRowid, the
// JOIN/IN codegen limitation that forced the per-table hydration shape,
// and the trailing-rowid ORDER BY term that would force a sorter. Purely
// diagnostic output; the hard assertions live in
// list_sessions_query_plan_audit.
// ---------------------------------------------------------------------------

#[test]
fn fsqlite_sort_index_capability_probe() -> Result<()> {
    let dir = std::env::temp_dir().join(format!("sort-probe-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let db = dir.join("probe.db");
    let conn = Connection::open(db.to_string_lossy().into_owned())?;
    conn.execute(
        "CREATE TABLE vs (
            conversation_id INTEGER PRIMARY KEY,
            is_pinned INTEGER NOT NULL DEFAULT 0,
            a_known INTEGER NOT NULL DEFAULT 1,
            activity_ms INTEGER,
            tie_key TEXT,
            provider TEXT,
            step_count INTEGER
        )",
    )?;
    conn.execute(
        "CREATE INDEX idx_act_desc ON vs(is_pinned DESC, a_known ASC, activity_ms DESC, tie_key ASC)",
    )?;
    conn.execute(
        "CREATE INDEX idx_act_asc ON vs(is_pinned DESC, a_known ASC, activity_ms ASC, tie_key ASC)",
    )?;
    conn.execute("CREATE TABLE vt (conversation_id INTEGER, tag TEXT)")?;
    conn.execute("CREATE INDEX idx_vt ON vt(conversation_id, tag)")?;
    for i in 1..=200_i64 {
        conn.execute(&format!(
            "INSERT INTO vs (conversation_id, is_pinned, a_known, activity_ms, tie_key, provider, step_count) \
             VALUES ({i}, {}, {}, {}, 'pid-{:05}', '{}', {i})",
            i64::from(i % 7 == 0),
            i64::from(i % 11 != 0),
            if i % 11 == 0 { "NULL".to_owned() } else { (i % 13).to_string() },
            (i * 7919) % 200,
            if i % 2 == 0 { "claude" } else { "gemini" }
        ))?;
    }
    conn.execute("INSERT INTO vt VALUES (3, 'release-blocker'), (17, 'release-blocker')")?;

    // Probe 1: ORDER BY naming only the 4 index columns (rowid implicit tail).
    let sql = "SELECT conversation_id FROM vs \
               ORDER BY is_pinned DESC, a_known ASC, activity_ms DESC, tie_key ASC \
               LIMIT 21";
    let opcodes: Vec<String> = conn
        .query(&format!("EXPLAIN {sql}"))?
        .iter()
        .map(|row| row.get_typed::<String>(1).unwrap_or_default())
        .collect();
    eprintln!("probe1 opcodes: {opcodes:?}");
    let plan: Vec<String> = conn
        .query(&format!("EXPLAIN QUERY PLAN {sql}"))?
        .iter()
        .map(|row| row.get_typed::<String>(3).unwrap_or_default())
        .collect();
    eprintln!("probe1 eqp: {plan:?}");

    // Probe 2: same order, with residual predicates + IN-subquery.
    let sql2 = "SELECT conversation_id FROM vs \
                WHERE provider = 'claude' \
                  AND conversation_id IN (SELECT conversation_id FROM vt WHERE tag = 'release-blocker') \
                ORDER BY is_pinned DESC, a_known ASC, activity_ms DESC, tie_key ASC \
                LIMIT 21";
    let opcodes2: Vec<String> = conn
        .query(&format!("EXPLAIN {sql2}"))?
        .iter()
        .map(|row| row.get_typed::<String>(1).unwrap_or_default())
        .collect();
    eprintln!("probe2 opcodes: {opcodes2:?}");
    let plan2: Vec<String> = conn
        .query(&format!("EXPLAIN QUERY PLAN {sql2}"))?
        .iter()
        .map(|row| row.get_typed::<String>(3).unwrap_or_default())
        .collect();
    eprintln!("probe2 eqp: {plan2:?}");

    // Probe 3: pin equality + order (bounded prefix scan: SeekGE expected).
    let sql3 = "SELECT conversation_id FROM vs \
                WHERE is_pinned = 1 AND provider IN ('claude','gemini') \
                ORDER BY is_pinned DESC, a_known ASC, activity_ms DESC, tie_key ASC \
                LIMIT 21";
    let plan3: Vec<String> = conn
        .query(&format!("EXPLAIN QUERY PLAN {sql3}"))?
        .iter()
        .map(|row| row.get_typed::<String>(3).unwrap_or_default())
        .collect();
    let opcodes3: Vec<String> = conn
        .query(&format!("EXPLAIN {sql3}"))?
        .iter()
        .map(|row| row.get_typed::<String>(1).unwrap_or_default())
        .collect();
    eprintln!("probe3 eqp: {plan3:?} opcodes: {opcodes3:?}");

    // Probe 4: keyset-style WHERE clause + order (residual skip-walk).
    let sql4 = "SELECT conversation_id FROM vs \
                WHERE is_pinned = 0 AND (a_known = 1 OR activity_ms < 5 \
                  OR (activity_ms = 5 AND tie_key > 'pid-00042') \
                  OR (activity_ms = 5 AND tie_key = 'pid-00042' AND conversation_id > 50)) \
                ORDER BY is_pinned DESC, a_known ASC, activity_ms DESC, tie_key ASC \
                LIMIT 21";
    let plan4: Vec<String> = conn
        .query(&format!("EXPLAIN QUERY PLAN {sql4}"))?
        .iter()
        .map(|row| row.get_typed::<String>(3).unwrap_or_default())
        .collect();
    let opcodes4: Vec<String> = conn
        .query(&format!("EXPLAIN {sql4}"))?
        .iter()
        .map(|row| row.get_typed::<String>(1).unwrap_or_default())
        .collect();
    eprintln!("probe4 eqp: {plan4:?} opcodes: {opcodes4:?}");
    let rows4 = conn.query(sql4)?;
    eprintln!(
        "probe4 first ids: {:?}",
        rows4
            .iter()
            .take(10)
            .map(|r| r.get_typed::<i64>(0).unwrap_or(0))
            .collect::<Vec<_>>()
    );

    // Probe 5: INDEXED BY (no ORDER BY) — stored-order walk per the codegen.
    let sql5 = "SELECT conversation_id FROM vs INDEXED BY idx_act_desc \
                WHERE is_pinned = 1 LIMIT 21";
    let opcodes5: Vec<String> = conn
        .query(&format!("EXPLAIN {sql5}"))?
        .iter()
        .map(|row| row.get_typed::<String>(1).unwrap_or_default())
        .collect();
    let rows5 = conn.query(sql5)?;
    eprintln!(
        "probe5 opcodes {opcodes5:?} pinned rows in order: {:?}",
        rows5
            .iter()
            .map(|r| r.get_typed::<i64>(0).unwrap_or(0))
            .collect::<Vec<_>>()
    );

    // Probe 6: ORDER BY with equality on non-index column (residual + walk).
    let sql6 = "SELECT conversation_id FROM vs \
                WHERE step_count > 100 \
                ORDER BY is_pinned DESC, a_known ASC, activity_ms DESC, tie_key ASC \
                LIMIT 21";
    let opcodes6: Vec<String> = conn
        .query(&format!("EXPLAIN {sql6}"))?
        .iter()
        .map(|row| row.get_typed::<String>(1).unwrap_or_default())
        .collect();
    eprintln!("probe6 opcodes: {opcodes6:?}");

    // Probe 7 (production shape): ORDER BY with the trailing rowid-column
    // term `conversation_id ASC` — does the index plan still satisfy the
    // order, or does the extra term force a sorter?
    let sql7 = "SELECT conversation_id, is_pinned, a_known, activity_ms, tie_key \
                FROM vs \
                ORDER BY is_pinned DESC, a_known ASC, activity_ms DESC, \
                         tie_key ASC, conversation_id ASC \
                LIMIT 21";
    let opcodes7: Vec<String> = conn
        .query(&format!("EXPLAIN {sql7}"))?
        .iter()
        .map(|row| row.get_typed::<String>(1).unwrap_or_default())
        .collect();
    eprintln!("probe7 opcodes (rowid tail): {opcodes7:?}");
    let rows7 = conn.query(sql7)?;
    eprintln!(
        "probe7 first ids: {:?}",
        rows7
            .iter()
            .take(8)
            .map(|r| r.get_typed::<i64>(0).unwrap_or(0))
            .collect::<Vec<_>>()
    );

    // Probe 8: trailing rowid term + a residual filter + keyset predicate.
    let sql8 = "SELECT conversation_id, is_pinned, a_known, activity_ms, tie_key \
                FROM vs \
                WHERE provider = 'claude' \
                  AND (a_known = 0 OR activity_ms < 5 \
                    OR (activity_ms = 5 AND tie_key > 'pid-00042') \
                    OR (activity_ms = 5 AND tie_key = 'pid-00042' AND conversation_id > 50)) \
                ORDER BY is_pinned DESC, a_known ASC, activity_ms DESC, \
                         tie_key ASC, conversation_id ASC \
                LIMIT 21";
    let opcodes8: Vec<String> = conn
        .query(&format!("EXPLAIN {sql8}"))?
        .iter()
        .map(|row| row.get_typed::<String>(1).unwrap_or_default())
        .collect();
    eprintln!("probe8 opcodes (residual+keyset+rowid tail): {opcodes8:?}");

    // Probe 9: hydration shapes on the rowid PK — IN-list vs OR-equalities
    // vs UNION ALL arms. `SCAN` vs `SEARCH` decides the bounded-work claim.
    for (label, sql9) in [
        (
            "in-list",
            "SELECT conversation_id, provider FROM vs \
                     WHERE conversation_id IN (3, 17, 42)"
                .to_owned(),
        ),
        (
            "or-eq",
            "SELECT conversation_id, provider FROM vs \
                  WHERE conversation_id = 3 OR conversation_id = 17 \
                     OR conversation_id = 42"
                .to_owned(),
        ),
        (
            "union-arms",
            "SELECT conversation_id, provider FROM vs \
                       WHERE conversation_id = 3 UNION ALL \
                       SELECT conversation_id, provider FROM vs \
                       WHERE conversation_id = 17 UNION ALL \
                       SELECT conversation_id, provider FROM vs \
                       WHERE conversation_id = 42"
                .to_owned(),
        ),
        // Joined hydration shapes — LEFT JOIN goes through the interpreted
        // path (VDBE codegen refuses it), so EQP detail is the available
        // evidence for whether the page-id set seeks or scans.
        (
            "join-in",
            "SELECT vs.conversation_id FROM vs \
                     LEFT JOIN vt ON vt.conversation_id = vs.conversation_id \
                     WHERE vs.conversation_id IN (3, 17, 42)"
                .to_owned(),
        ),
        (
            "join-eq",
            "SELECT vs.conversation_id FROM vs \
                     LEFT JOIN vt ON vt.conversation_id = vs.conversation_id \
                     WHERE vs.conversation_id = 3"
                .to_owned(),
        ),
        (
            "join-or",
            "SELECT vs.conversation_id FROM vs \
                     LEFT JOIN vt ON vt.conversation_id = vs.conversation_id \
                     WHERE vs.conversation_id = 3 OR vs.conversation_id = 17 \
                        OR vs.conversation_id = 42"
                .to_owned(),
        ),
    ] {
        let eqp: Vec<String> = conn
            .query(&format!("EXPLAIN QUERY PLAN {sql9}"))?
            .iter()
            .map(|row| row.get_typed::<String>(3).unwrap_or_default())
            .collect();
        let ops: Vec<String> = conn
            .query(&format!("EXPLAIN {sql9}"))
            .map(|rows| {
                rows.iter()
                    .map(|row| row.get_typed::<String>(1).unwrap_or_default())
                    .collect::<Vec<String>>()
            })
            .unwrap_or_else(|e| vec![format!("EXPLAIN-FAILED: {e}")]);
        eprintln!("probe9 [{label}] eqp: {eqp:?} ops: {ops:?}");
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Strict-reader metadata-token capability probe
// ---------------------------------------------------------------------------

/// The open-time capability probe earns the metadata-token fast path only
/// on evidence: on this filesystem the simulated rewrite + `set_modified`
/// mtime restore must be caught (a real inode change-time moves the hashed
/// tuple); on a path that cannot exist or whose data directory refuses the
/// probe write, the wrapper retires the token instead of ever risking a
/// false "unchanged".
#[cfg(unix)]
#[test]
fn meta_token_probe_earns_and_denies_the_fast_path() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempfile::tempdir()?;
    // Behavioral half: rewrite content, restore mtime — only a real ctime
    // still proves the write. A capable filesystem returns Some(true).
    let probe = temp.path().join("probe.bin");
    ensure!(
        reader::meta_token_probe(&probe) == Some(true),
        "local filesystem must report an unforgeable inode change-time"
    );

    // Capability wrapper: a real archive path in a writable directory earns
    // the token; the probe file is cleaned up behind it.
    let archive = temp.path().join("archive.db");
    std::fs::write(&archive, b"x")?;
    ensure!(
        reader::meta_token_trusted(&archive),
        "a local writable data dir must keep the metadata fast path"
    );
    ensure!(
        std::fs::read_dir(temp.path())?
            .filter_map(|entry| entry.ok())
            .all(|entry| !entry
                .file_name()
                .to_string_lossy()
                .contains("meta-token-probe")),
        "the probe must not leak files into the data directory"
    );

    // Fallback: a path that cannot exist fails the probe → distrust.
    let missing = temp.path().join("no-such-dir").join("archive.db");
    ensure!(
        !reader::meta_token_trusted(&missing),
        "an unresolvable archive path must retire the metadata token"
    );

    // Fallback: a data directory the probe cannot write to retires the
    // token too — the failure is never mistaken for capability.
    let sealed = temp.path().join("sealed");
    std::fs::create_dir(&sealed)?;
    let inner = sealed.join("archive.db");
    std::fs::write(&inner, b"x")?;
    std::fs::set_permissions(&sealed, std::fs::Permissions::from_mode(0o555))?;
    ensure!(
        !reader::meta_token_trusted(&inner),
        "an unwritable data dir must retire the metadata token"
    );
    std::fs::set_permissions(&sealed, std::fs::Permissions::from_mode(0o755))?;
    Ok(())
}
