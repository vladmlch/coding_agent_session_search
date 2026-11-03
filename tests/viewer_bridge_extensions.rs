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
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Result, ensure};
use coding_agent_search::franken_sync::compat::RowExt;
use coding_agent_search::franken_sync::compat::param_slice_to_values;
use coding_agent_search::franken_sync::{Connection, params};
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
    ensure!(revisions.get_typed::<i64>(3)? == 0);
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

    // project_asc: pinned first, then workspace path asc, NULL last.
    let project = bridge.exchange(json!({"op":"list_sessions","sort":"project_asc","limit":10}))?;
    ensure!(project["ok"] == true, "{project}");
    ensure!(item_ids(&project) == [4, 5, 1, 2, 3], "{project}");

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

#[test]
fn list_sessions_query_plan_audit() -> Result<()> {
    // Report the real plan for the default sort + a selective filter shape.
    // Assertions pin what MUST NOT appear: no messages table on the browse
    // path, no whole-catalog dependent scan that ignores LIMIT pushdown
    // (the TEMP B-TREE for the computed activity sort is expected and
    // reported, not hidden).
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
    ] {
        let (sql, params) =
            query::planned_page_sql(&connection, &filters, sort, pin_first, None, 10)?;
        let plan = connection.query(&format!("EXPLAIN QUERY PLAN {sql}"))?;
        let mut lines = Vec::new();
        for row in &plan {
            let detail: String = row.get_typed(3)?;
            lines.push(detail);
        }
        let joined = lines.join(" | ");
        eprintln!("EXPLAIN [{label}] ({} params): {joined}", params.len());
        ensure!(
            !joined.to_uppercase().contains("MESSAGES"),
            "browse path must never touch the messages table: {joined}"
        );
        ensure!(
            joined.contains("conversations"),
            "plan must scan/search conversations: {joined}"
        );
        if label == "category" {
            eprintln!("category-filter plan detail: {joined}");
        }
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
    ensure!(claude_id == 1 && codex_id == 2, "fixture ids: {claude_id}/{codex_id}");
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
            && count(&connection, "SELECT COUNT(*) FROM viewer_session_projection")? == 0,
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
    let revisions = connection
        .query_row("SELECT canonical_revision, projection_revision FROM viewer_revisions WHERE id = 1")?;
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
    ensure!(again.facts_published == 0 && again.structure_published == 0, "{again:?}");
    ensure!(again.facts_skipped_unchanged == 2 && again.structure_skipped_unchanged == 2, "{again:?}");
    let still = connection
        .query_row("SELECT projection_revision FROM viewer_revisions WHERE id = 1")?;
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
    ensure!(moved.facts_published == 1 && moved.structure_published == 1, "{moved:?}");
    ensure!(moved.facts_skipped_unchanged == 1 && moved.structure_skipped_unchanged == 1, "{moved:?}");
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
    ensure!(stats.conversations_scanned == 3 && batches == 3, "{stats:?} batches={batches}");

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
    ensure!(first.conversations_scanned == 1 && first.limit_reached, "{first:?}");
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
    ensure!(resumed.facts_published == 2 && resumed.facts_skipped_unchanged == 1, "{resumed:?}");
    ensure!(
        count(&connection, "SELECT COUNT(*) FROM viewer_session_facts")? == 3
            && count(&connection, "SELECT COUNT(*) FROM viewer_session_projection")? == 3,
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
    ensure!(status.success(), "--backfill-viewer-extensions failed: {status}");

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
        receipt["extractor_version"].as_i64() == Some(1)
            && receipt["projection_version"].as_i64() == Some(1),
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

/// Interrupted publication: a crash after the pending receipt commits but
/// before the generation transaction lands leaves a durable `pending`
/// receipt and NO rows — never a falsely complete archive. The explicit
/// resumable backfill converges it to `complete` with no loss or
/// duplication.
#[test]
fn ingest_interrupted_publication_leaves_pending_and_backfill_completes() -> Result<()> {
    let fixture = ingest_fixture()?;
    seed_codex_rollout(&fixture, "gen-codex-5", "crash mid publication")?;

    // Crash injection: pending receipt committed, generation transaction
    // never attempted (process "dies" between the two adjacent commits).
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
    let receipt = ingest_receipt(&conn, conversation_id)?.expect("crash receipt");
    ensure!(
        receipt["state"] == "pending",
        "interrupted publication must leave a pending receipt: {receipt}"
    );
    ensure!(
        facts::list_incomplete_ingest_receipts(&conn)?
            .iter()
            .any(|receipt| receipt.conversation_id == conversation_id),
        "the pending receipt must surface in the incomplete-receipt set"
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
