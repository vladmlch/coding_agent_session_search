//! Task 11: revisioned change journal inside the canonical SQLite archive.
//!
//! `viewer_change_log` is the truth (spec decision 7): canonical observer
//! triggers append `session_*` rows inside the SAME commit that mutates
//! canonical tables, extension-table triggers append `annotations_updated`
//! inside the annotation write transaction, and `notify_committed_change`
//! runs AFTER a successful commit to coalesce the journal, stamp a
//! `canonical_ready` marker and enforce bounded retention. Task 8 reserves
//! `index_ready` for a certified real Quill publication carrying the
//! durable `PublicationReceipt` coverage. Local IPC is only a wakeup; a
//! writer that crashes after commit but before notify still left a fully
//! replayable journal.
//!
//! The bridge modules are compiled into the `cass-viewer-bridge` binary,
//! not the library; the read-side surface is included under its original
//! module names so `#[path]` wiring matches `tests/viewer_bridge_extensions.rs`
//! (Task 12 owns `mod changes;` + the RPC binding).
#[path = "../src/viewer_bridge/changes.rs"]
#[allow(dead_code, unused_imports)]
mod changes;
#[path = "../src/viewer_bridge/protocol.rs"]
#[allow(dead_code)]
mod protocol;
#[path = "../src/viewer_bridge/revisions.rs"]
#[allow(dead_code)]
mod revisions;
#[path = "viewer_bridge_support/mod.rs"]
mod support;

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Result, ensure};
use coding_agent_search::franken_sync::compat::{RowExt, param_slice_to_values};
use coding_agent_search::franken_sync::{Connection, params};
use coding_agent_search::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use coding_agent_search::storage::sqlite::FrankenStorage;
use coding_agent_search::viewer_extensions::changes as journal;
use serde_json::{Value, json};
use support::{BridgeHarness, SeedConversation};

// ---------------------------------------------------------------------------
// Fixture: canonical archive + the authorized extension install (profile AND
// the canonical-observer/annotation journal triggers) applied in-process —
// the exact `schema::ensure_schema` + `revisions::ensure_observers` pair the
// `--init-extensions` one-shot writer runs (the subprocess path itself is
// pinned by tests/viewer_bridge_extensions.rs). No serving bridge needed —
// the journal is a database object, not a wire protocol.
// ---------------------------------------------------------------------------

struct JournalFixture {
    _temp: tempfile::TempDir,
    db: PathBuf,
}

fn fixture() -> Result<JournalFixture> {
    let temp = tempfile::tempdir()?;
    let data_dir = temp.path().join("data");
    std::fs::create_dir(&data_dir)?;
    let db = data_dir.join("archive.db");
    let storage = FrankenStorage::open(&db)?;
    drop(storage);
    let installer = open(&db)?;
    coding_agent_search::viewer_extensions::schema::ensure_schema(&installer)?;
    revisions::ensure_observers(&installer)?;
    drop(installer);
    Ok(JournalFixture { _temp: temp, db })
}

fn canned_conversation(id: i64, text: &str, count: i64) -> Conversation {
    Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: None,
        external_id: Some(format!("fixture-{id}")),
        title: Some(format!("fixture-{id}")),
        source_path: format!("/missing/source-{id}.jsonl").into(),
        started_at: None,
        ended_at: None,
        approx_tokens: None,
        metadata_json: json!({"fixture":true}),
        source_id: "local".into(),
        origin_host: None,
        messages: (0..count)
            .map(|idx| Message {
                id: None,
                idx,
                role: MessageRole::Agent,
                author: None,
                created_at: Some(1_700_000_000_000 + idx),
                content: text.into(),
                extra_json: json!({"fixture":idx}),
                snippets: Vec::new(),
            })
            .collect(),
    }
}

fn ensure_agent(storage: &FrankenStorage) -> Result<i64> {
    Ok(storage.ensure_agent(&Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: None,
        kind: AgentKind::Cli,
    })?)
}

/// Insert a conversation through a fresh `FrankenStorage` connection — a
/// different writer connection from the journal reader, like a real stock
/// indexer process.
fn insert_conversation(db: &Path, conv: &Conversation) -> Result<i64> {
    let storage = FrankenStorage::open(db)?;
    let agent = ensure_agent(&storage)?;
    let outcome = storage.insert_conversation_tree(agent, None, conv)?;
    drop(storage);
    Ok(outcome.conversation_id)
}

fn open(db: &Path) -> Result<Connection> {
    Ok(Connection::open(db.to_string_lossy().into_owned())?)
}

fn kinds<'a>(events: &'a [journal::ChangeEvent]) -> Vec<&'a str> {
    events.iter().map(|e| e.kind.as_str()).collect()
}

fn assert_monotonic(events: &[journal::ChangeEvent], floor: i64) -> Result<()> {
    let mut previous = floor;
    for event in events {
        let seq = event.seq.expect("durable events carry seq");
        ensure!(
            seq > previous,
            "journal seqs must be strictly increasing: {seq} <= {previous}"
        );
        previous = seq;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Engine qualification (mirrors revisions_engine_trigger_semantics_probe):
// multi-statement trigger bodies and INSERT..SELECT..WHERE with OLD/NEW
// bindings are the chosen journal-append mechanism — pin them first.
// ---------------------------------------------------------------------------

#[test]
fn journal_engine_insert_select_guard_probe() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let db = temp.path().join("probe.db");
    let writer = open(&db)?;
    writer.execute_batch(
        "CREATE TABLE base (id INTEGER PRIMARY KEY, a TEXT);
         CREATE TABLE j (seq INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT, cid INTEGER);
         CREATE TRIGGER probe_ins AFTER INSERT ON base BEGIN
             INSERT INTO j (kind,cid) SELECT 'created', NEW.id;
         END;
         CREATE TRIGGER probe_upd AFTER UPDATE ON base BEGIN
             INSERT INTO j (kind,cid) SELECT 'updated', NEW.id
                 WHERE OLD.a IS NOT NEW.a;
         END;",
    )?;
    writer.execute("INSERT INTO base (a) VALUES ('x')")?;
    writer.execute("UPDATE base SET a=a WHERE id=1")?;
    writer.execute("UPDATE base SET a='y' WHERE id=1")?;
    let rows = writer.query("SELECT kind,cid FROM j ORDER BY seq")?;
    let got: Vec<(String, i64)> = rows
        .iter()
        .map(|r| Ok((r.get_typed::<String>(0)?, r.get_typed::<i64>(1)?)))
        .collect::<Result<_>>()?;
    ensure!(
        got == [("created".to_owned(), 1), ("updated".to_owned(), 1)],
        "INSERT..SELECT..WHERE must bind OLD/NEW and suppress same-values: {got:?}"
    );
    drop(writer);
    Ok(())
}

// ---------------------------------------------------------------------------
// Ordered monotonic events across canonical insert/update/delete and an
// annotation write; journal rows commit atomically with the canonical write.
// ---------------------------------------------------------------------------

#[test]
fn journal_canonical_and_annotation_writes_yield_monotonic_events() -> Result<()> {
    let fixture = fixture()?;
    insert_conversation(&fixture.db, &canned_conversation(1, "alpha", 3))?;

    let reader = open(&fixture.db)?;
    let page = journal::changes_since(&reader, 0, 256)?;
    ensure!(!page.truncated, "fresh cursor must not truncate: {page:?}");
    ensure!(
        kinds(&page.events).contains(&journal::kind::SESSION_CREATED),
        "canonical insert must journal session_created: {:?}",
        page.events
    );
    ensure!(
        page.events.iter().all(|e| e.conversation_id == Some(1)),
        "all seeded events address conversation 1: {:?}",
        page.events
    );
    assert_monotonic(&page.events, 0)?;
    ensure!(
        page.current_revisions
            .is_some_and(|g| g.canonical_revision > 0),
        "canonical_revision must move with committed canonical rows: {:?}",
        page.current_revisions
    );

    // Resume from the last seq: a guarded canonical update is one event.
    let update = open(&fixture.db)?;
    update.execute("UPDATE conversations SET title='renamed' WHERE id=1")?;
    drop(update);
    let page = journal::changes_since(&reader, page.next_seq, 256)?;
    ensure!(
        kinds(&page.events) == [journal::kind::SESSION_UPDATED],
        "guarded title update must journal exactly session_updated: {:?}",
        page.events
    );
    ensure!(page.events[0].conversation_id == Some(1));

    // Annotation write lane: row write + the explicit revision bump live in
    // the same transaction in patch_annotations; the journal append rides
    // the extension-table trigger so ANY writer's commit is observed.
    let update = open(&fixture.db)?;
    update.execute("BEGIN")?;
    update.execute_with_params(
        "INSERT INTO viewer_annotations \
         (conversation_id,is_pinned,custom_title,notes,revision,updated_at_ms) \
         VALUES (?1,?2,?3,?4,?5,?6)",
        &param_slice_to_values(params![1_i64, 1_i64, "renamed", "note", 1_i64, 1_i64]),
    )?;
    update.execute_with_params(
        "INSERT INTO viewer_tags (conversation_id,tag,created_at_ms) VALUES (?1,?2,?3)",
        &param_slice_to_values(params![1_i64, "release-blocker", 1_i64]),
    )?;
    revisions::bump(&update, revisions::Counter::Annotation, 1)?;
    update.execute("COMMIT")?;
    drop(update);
    let page = journal::changes_since(&reader, page.next_seq, 256)?;
    ensure!(
        kinds(&page.events).contains(&journal::kind::ANNOTATIONS_UPDATED),
        "annotation row write must journal annotations_updated: {:?}",
        page.events
    );
    ensure!(
        page.events.iter().all(|e| e.conversation_id == Some(1)),
        "annotation events address the patched conversation: {:?}",
        page.events
    );

    // Deleting the conversation journals session_deleted (and cascades the
    // annotation row — still annotation-scoped events, never canonical ones).
    let update = open(&fixture.db)?;
    update.execute("PRAGMA foreign_keys = ON")?;
    update.execute("DELETE FROM conversations WHERE id=1")?;
    drop(update);
    let page = journal::changes_since(&reader, page.next_seq, 256)?;
    ensure!(
        kinds(&page.events).contains(&journal::kind::SESSION_DELETED),
        "conversation delete must journal session_deleted: {:?}",
        page.events
    );
    assert_monotonic(&page.events, 0)?;
    drop(reader);
    Ok(())
}

// ---------------------------------------------------------------------------
// Rollback: an aborted transaction must leave no visible journal event and
// must not move seqs.
// ---------------------------------------------------------------------------

#[test]
fn journal_rollback_leaves_no_visible_event() -> Result<()> {
    let fixture = fixture()?;
    insert_conversation(&fixture.db, &canned_conversation(1, "base", 2))?;
    let reader = open(&fixture.db)?;
    let baseline = journal::changes_since(&reader, 0, 256)?;

    let writer = open(&fixture.db)?;
    writer.execute("BEGIN")?;
    writer.execute(
        "INSERT INTO messages (conversation_id,idx,role,content) VALUES (1,99,'agent','vanished')",
    )?;
    writer.execute("ROLLBACK")?;
    drop(writer);

    let page = journal::changes_since(&reader, baseline.next_seq, 256)?;
    ensure!(
        page.events.is_empty() && page.next_seq == baseline.next_seq && !page.truncated,
        "rolled-back write must produce no journal event and no seq drift: {page:?}"
    );
    ensure!(
        page.current_revisions == baseline.current_revisions,
        "rollback must not move logical revisions: {:?} vs {:?}",
        page.current_revisions,
        baseline.current_revisions
    );
    drop(reader);
    Ok(())
}

// ---------------------------------------------------------------------------
// Same-values UPDATEs and steady-state idempotent re-ingest stay silent.
// ---------------------------------------------------------------------------

#[test]
fn journal_same_values_update_and_idempotent_reingest_are_quiet() -> Result<()> {
    let fixture = fixture()?;
    insert_conversation(&fixture.db, &canned_conversation(1, "stable text", 2))?;
    let reader = open(&fixture.db)?;
    let mut cursor = journal::changes_since(&reader, 0, 256)?.next_seq;

    // Guarded UPDATE triggers suppress same-values rewrites entirely.
    let writer = open(&fixture.db)?;
    writer.execute("UPDATE messages SET content=content WHERE conversation_id=1")?;
    writer
        .execute("UPDATE conversations SET title=title,metadata_json=metadata_json WHERE id=1")?;
    drop(writer);
    let page = journal::changes_since(&reader, cursor, 256)?;
    ensure!(
        page.events.is_empty(),
        "same-values UPDATE must not journal a semantic event: {:?}",
        page.events
    );

    // First identical replay legitimately backfills derived columns (real
    // stored-byte change); every LATER identical replay must be quiet.
    let writer = open(&fixture.db)?;
    let agent_id: i64 = writer
        .query_row("SELECT agent_id FROM conversations WHERE id=1")?
        .get_typed(0)?;
    drop(writer);
    for round in 0..2 {
        let storage = FrankenStorage::open(&fixture.db)?;
        let outcome = storage.insert_conversation_tree(
            agent_id,
            None,
            &canned_conversation(1, "stable text", 2),
        )?;
        drop(storage);
        ensure!(outcome.conversation_id == 1);
        let page = journal::changes_since(&reader, cursor, 256)?;
        if round == 0 {
            // Consume the honest backfill events, then run the notify pass
            // that compacts them — the quiet-journal precondition for the
            // idempotent replay below.
            cursor = page.next_seq;
            journal::notify_committed_change_best_effort(&reader);
            cursor = journal::changes_since(&reader, cursor, 256)?.next_seq;
        } else {
            ensure!(
                page.events.is_empty(),
                "steady-state identical re-ingest journaled a false event: {:?}",
                page.events
            );
        }
    }

    // A notify pass on a quiet journal writes no semantic event either —
    // canonical_ready is only stamped when a pass actually coalesced
    // canonical changes (and index_ready is never stamped by notify at all).
    journal::notify_committed_change_best_effort(&reader);
    let page = journal::changes_since(&reader, cursor, 256)?;
    ensure!(
        page.events.is_empty(),
        "notify on a quiet journal must not emit canonical_ready: {:?}",
        page.events
    );
    drop(reader);
    Ok(())
}

// ---------------------------------------------------------------------------
// Crash window: the canonical commit already contains the journal rows —
// notify only compacts them. No notify ⇒ events still replay.
// ---------------------------------------------------------------------------

#[test]
fn journal_commit_without_notify_still_replays_then_compacts() -> Result<()> {
    let fixture = fixture()?;
    insert_conversation(&fixture.db, &canned_conversation(1, "bulk", 4))?;
    let reader = open(&fixture.db)?;
    let cursor = journal::changes_since(&reader, 0, 256)?.next_seq;

    // A bulk append (writer crash after commit before notify): raw per-row
    // events are already durable inside the same commit.
    let writer = open(&fixture.db)?;
    writer.execute("BEGIN")?;
    for idx in 4..12 {
        writer.execute_with_params(
            "INSERT INTO messages (conversation_id,idx,role,content) VALUES (1,?1,'agent','appended')",
            &param_slice_to_values(params![idx]),
        )?;
    }
    writer.execute("COMMIT")?;
    drop(writer);

    let page = journal::changes_since(&reader, cursor, 256)?;
    ensure!(
        page.events
            .iter()
            .all(|e| e.kind == journal::kind::SESSION_UPDATED)
            && page.events.iter().all(|e| e.conversation_id == Some(1)),
        "pre-notify journal must replay the committed change: {:?}",
        page.events
    );
    ensure!(
        !page.events.is_empty() && page.events.len() <= 8,
        "read-side coalescing keeps at most the latest per key: {:?}",
        page.events
    );

    // The post-commit notify coalesces the bulk burst and stamps
    // canonical_ready bound to the canonical revision it certifies.
    // Task 8: notify marks canonical progress only — index_ready is
    // reserved for a certified Quill publication and must NOT appear here.
    journal::notify_committed_change_best_effort(&reader);
    let replayed = journal::changes_since(&reader, cursor, 256)?;
    let updated: Vec<_> = replayed
        .events
        .iter()
        .filter(|e| e.kind == journal::kind::SESSION_UPDATED && e.conversation_id == Some(1))
        .collect();
    ensure!(
        updated.len() == 1,
        "notify must coalesce the per-row burst to one event per key: {:?}",
        replayed.events
    );
    ensure!(
        replayed
            .events
            .iter()
            .any(|e| e.kind == journal::kind::CANONICAL_READY),
        "a notify pass over real canonical changes must stamp canonical_ready: {:?}",
        replayed.events
    );
    ensure!(
        replayed
            .events
            .iter()
            .all(|e| e.kind != journal::kind::INDEX_READY),
        "canonical notify must never fabricate index_ready — only a real \
         Quill publication certifies it: {:?}",
        replayed.events
    );
    let ready = replayed
        .events
        .iter()
        .find(|e| e.kind == journal::kind::CANONICAL_READY)
        .expect("canonical_ready event");
    ensure!(
        Some(ready.revision) == replayed.current_revisions.map(|g| g.canonical_revision),
        "canonical_ready must bind the canonical revision it certifies: {:?} vs {:?}",
        ready.revision,
        replayed.current_revisions
    );

    // A second quiet notify appends nothing at all.
    journal::notify_committed_change_best_effort(&reader);
    let quiet = journal::changes_since(&reader, replayed.next_seq, 256)?;
    ensure!(
        quiet.events.is_empty(),
        "repeated quiet notify must append no events: {:?}",
        quiet.events
    );
    drop(reader);
    Ok(())
}

// ---------------------------------------------------------------------------
// Explicit append lane (index_ready / reset_required / annotation helper
// used by the writer boundary): commits atomically in the caller's
// transaction and is visible on the next read.
// ---------------------------------------------------------------------------

#[test]
fn journal_explicit_append_rides_callers_transaction() -> Result<()> {
    let fixture = fixture()?;
    insert_conversation(&fixture.db, &canned_conversation(1, "x", 1))?;
    let reader = open(&fixture.db)?;
    let cursor = journal::changes_since(&reader, 0, 256)?.next_seq;

    let writer = open(&fixture.db)?;
    writer.execute("BEGIN")?;
    journal::append_change_event(
        &writer,
        journal::kind::RESET_REQUIRED,
        None,
        0,
        Some(json!({"reason":"admin reindex"}).to_string()),
    )?;
    writer.execute("ROLLBACK")?;
    drop(writer);
    let page = journal::changes_since(&reader, cursor, 256)?;
    ensure!(
        page.events.is_empty(),
        "appended event must roll back with the caller transaction: {:?}",
        page.events
    );

    let writer = open(&fixture.db)?;
    writer.execute("BEGIN")?;
    let seq = journal::append_change_event(
        &writer,
        journal::kind::RESET_REQUIRED,
        None,
        0,
        Some(json!({"reason":"admin reindex"}).to_string()),
    )?;
    writer.execute("COMMIT")?;
    drop(writer);
    let page = journal::changes_since(&reader, cursor, 256)?;
    ensure!(
        page.events.len() == 1
            && page.events[0].kind == journal::kind::RESET_REQUIRED
            && page.events[0].seq == Some(seq)
            && page.events[0]
                .payload_json
                .as_deref()
                .is_some_and(|p| p.contains("reindex")),
        "committed append must replay with its payload: {:?}",
        page.events
    );
    drop(reader);
    Ok(())
}

// ---------------------------------------------------------------------------
// Bounded retention: an expired cursor is answered with `truncated` + a
// reset_required event, never silently partial replay.
// ---------------------------------------------------------------------------

#[test]
fn journal_retention_expired_cursor_requests_reset() -> Result<()> {
    let fixture = fixture()?;
    for id in 1..=3 {
        insert_conversation(&fixture.db, &canned_conversation(id, "row", 1))?;
    }
    let reader = open(&fixture.db)?;
    let first = journal::changes_since(&reader, 0, 256)?;

    // Generate a burst of distinct per-key events, then compact+prune with
    // a tiny retention bound so the earliest seqs expire.
    let writer = open(&fixture.db)?;
    for round in 0..6 {
        for id in 1..=3_i64 {
            writer.execute_with_params(
                "UPDATE conversations SET title=?1 WHERE id=?2",
                &param_slice_to_values(params![format!("t{round}-{id}"), id]),
            )?;
        }
    }
    drop(writer);
    journal::notify_committed_change_with_retention(&reader, 0, 3)?;

    let expired = journal::changes_since(&reader, first.next_seq.min(1_000), 256)?;
    ensure!(
        expired.truncated,
        "expired cursor must report truncated, not a silent partial replay: {expired:?}"
    );
    ensure!(
        expired
            .events
            .iter()
            .any(|e| e.kind == journal::kind::RESET_REQUIRED),
        "expired cursor must be answered with reset_required: {:?}",
        expired.events
    );
    // The reset token resumes cleanly at the retained head.
    let resumed = journal::changes_since(&reader, expired.next_seq, 256)?;
    ensure!(
        !resumed.truncated,
        "post-reset cursor must resume without truncation: {resumed:?}"
    );
    drop(reader);
    Ok(())
}

// ---------------------------------------------------------------------------
// Archive replacement / seq regression: a cursor ahead of the journal's
// high-water mark can only mean the archive was replaced — reset, never
// quietly "no events".
// ---------------------------------------------------------------------------

#[test]
fn journal_seq_regression_requests_reset() -> Result<()> {
    let fixture = fixture()?;
    insert_conversation(&fixture.db, &canned_conversation(1, "x", 1))?;
    let reader = open(&fixture.db)?;
    let page = journal::changes_since(&reader, 0, 256)?;
    ensure!(page.next_seq > 0);

    let regressed = journal::changes_since(&reader, page.next_seq + 100_000, 256)?;
    ensure!(
        regressed.truncated
            && regressed
                .events
                .iter()
                .any(|e| e.kind == journal::kind::RESET_REQUIRED),
        "a cursor beyond the journal high-water mark must reset: {regressed:?}"
    );
    drop(reader);
    Ok(())
}

// ---------------------------------------------------------------------------
// Unexplained canonical bump: a revision that moved without a matching
// journal event (manual revision write, a writer whose observers predate
// the journal bodies) must surface as a coarse refetch signal — never a
// silent gap.
// ---------------------------------------------------------------------------

#[test]
fn journal_unexplained_canonical_bump_is_coarse_signal() -> Result<()> {
    let fixture = fixture()?;
    insert_conversation(&fixture.db, &canned_conversation(1, "x", 1))?;
    let reader = open(&fixture.db)?;
    let cursor = journal::changes_since(&reader, 0, 256)?.next_seq;

    // Simulate an unjournaled canonical revision bump (e.g. an archive whose
    // observers predate the journal bodies, or an admin counter write).
    let writer = open(&fixture.db)?;
    writer.execute(
        "UPDATE viewer_revisions SET canonical_revision = canonical_revision + 3 WHERE id = 1",
    )?;
    drop(writer);

    let page = journal::changes_since(&reader, cursor, 256)?;
    ensure!(
        page.events
            .iter()
            .any(|e| e.kind == journal::kind::SESSION_UPDATED
                && e.conversation_id.is_none()
                && e.revision
                    == page
                        .current_revisions
                        .map(|g| g.canonical_revision)
                        .unwrap_or(-1)),
        "unexplained canonical revision must yield a coarse session_updated: {:?}",
        page.events
    );
    drop(reader);
    Ok(())
}

// ---------------------------------------------------------------------------
// Unextended archive: changes_since stays a typed empty page, never an
// opaque SQL error.
// ---------------------------------------------------------------------------

#[test]
fn journal_unextended_archive_reports_empty() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let db = temp.path().join("archive.db");
    let storage = FrankenStorage::open(&db)?;
    drop(storage);
    let reader = open(&db)?;
    let page = journal::changes_since(&reader, 0, 256)?;
    ensure!(
        page.events.is_empty() && !page.truncated && page.current_revisions.is_none(),
        "unextended archive must report an empty, revisionless page: {page:?}"
    );
    drop(reader);
    Ok(())
}

// ---------------------------------------------------------------------------
// Journal read plan: `WHERE seq > ? ORDER BY seq` drives off the rowid
// primary key — pin EXPLAIN so a future change can't silently regress the
// bounded-page plan.
// ---------------------------------------------------------------------------

#[test]
fn journal_changes_since_plan_uses_seq_ordering() -> Result<()> {
    let fixture = fixture()?;
    insert_conversation(&fixture.db, &canned_conversation(1, "x", 1))?;
    let reader = open(&fixture.db)?;
    // EXPLAIN evidence for the schema.rs no-index decision: the READ path
    // walks the rowid `seq` with no temp b-tree. The notify dedupe GROUP BY
    // sorts the bounded committed tail once per post-commit pass — a sort
    // there beats index maintenance on every trigger-append during bulk
    // ingest, so no secondary index is justified.
    for (sql, allow_sort) in [
        (
            "SELECT seq,kind,conversation_id,revision,payload_json \
             FROM viewer_change_log WHERE seq > 0 ORDER BY seq LIMIT 10",
            false,
        ),
        (
            "SELECT MAX(revision) FROM viewer_change_log \
             WHERE kind IN ('session_created','session_updated','session_deleted')",
            false,
        ),
        (
            "SELECT MAX(seq) FROM viewer_change_log WHERE seq > 0 \
             GROUP BY kind, conversation_id",
            true,
        ),
        ("DELETE FROM viewer_change_log WHERE seq <= 5", false),
    ] {
        let plan: Vec<String> = reader
            .query(&format!("EXPLAIN QUERY PLAN {sql}"))?
            .iter()
            .map(|row| row.get_typed::<String>(3))
            .collect::<Result<_, _>>()?;
        eprintln!("plan {sql}\n  -> {plan:?}");
        ensure!(
            allow_sort || plan.iter().all(|line| !line.contains("TEMP B-TREE")),
            "read-path journal query must not need a temp b-tree: {plan:?}"
        );
    }
    drop(reader);
    Ok(())
}

// ===========================================================================
// Task 12: independent push channel (`--enable-viewer-events`), atomic
// reconnect/replay, honest index_ready gating and bounded-queue backpressure.
//
// Transport under test: a loopback TCP listener owned by the bridge process,
// discovered through `<db>.viewer_events.addr` (and advertised in `hello`),
// speaking one JSONL frame per line. The SQLite journal is the only truth:
// the socket carries wakeup (`{"op":"notify"}`) and replay delivery, never
// event durability. These tests exercise it through the real
// `cass-viewer-bridge` binary so the serialized stdio FIFO and the event
// channel are proven independent.
// ===========================================================================

const EVENT_ARGS: &[&str] = &["--enable-viewer-events", "--event-poll-ms", "25"];

/// Run the authorized one-shot writer lane against a harness fixture so the
/// `viewer_*` profile AND the journal observers exist before subscribers
/// attach (mirrors the Task-4 helper in tests/viewer_bridge_extensions.rs).
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

fn events_endpoint(hello: &Value) -> Result<String> {
    ensure!(
        hello["ok"] == true && hello["result"]["event_stream"]["enabled"] == true,
        "event stream must be advertised enabled: {hello}"
    );
    let endpoint = hello["result"]["event_stream"]["endpoint"]
        .as_str()
        .expect("event endpoint")
        .to_owned();
    ensure!(
        endpoint.parse::<std::net::SocketAddr>().is_ok(),
        "endpoint must be a numeric loopback socket address: {endpoint}"
    );
    Ok(endpoint)
}

/// Spawn a bridge with the event channel enabled over seeded canonical rows,
/// install the extension profile, and return `(bridge, endpoint)`.
fn events_bridge(seeds: &[SeedConversation]) -> Result<(BridgeHarness, String)> {
    let mut bridge = BridgeHarness::with_seed(seeds, EVENT_ARGS)?;
    init_extensions(&bridge)?;
    let hello = bridge.exchange(json!({"op":"hello","id":1}))?;
    let endpoint = events_endpoint(&hello)?;
    Ok((bridge, endpoint))
}

struct Subscriber {
    reader: BufReader<TcpStream>,
}

/// Connect to the bridge event socket and send the subscribe handshake.
/// `after_seq` is the caller's last consumed durable journal seq (the
/// watermark side of the atomic reconnect protocol).
fn subscribe(endpoint: &str, after_seq: Option<i64>) -> Result<Subscriber> {
    let stream = TcpStream::connect(endpoint)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_nodelay(true).ok();
    let mut writer = stream.try_clone()?;
    let mut frame = json!({"op":"subscribe"});
    if let Some(seq) = after_seq {
        frame["after_seq"] = Value::from(seq);
    }
    writer.write_all(frame.to_string().as_bytes())?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(Subscriber {
        reader: BufReader::new(stream),
    })
}

/// One event-socket frame, or `None` on clean EOF.
fn read_event(subscriber: &mut Subscriber) -> Result<Option<Value>> {
    let mut line = String::new();
    let read = subscriber.reader.read_line(&mut line)?;
    if read == 0 {
        return Ok(None);
    }
    Ok(Some(serde_json::from_str(&line)?))
}

/// Read frames until `budget` elapses, timing each blocking read so a
/// silent pump fails the test instead of hanging it. Returns whatever
/// complete frames arrived in the window.
fn collect_events(subscriber: &mut Subscriber, budget: Duration) -> Result<Vec<Value>> {
    let end = Instant::now() + budget;
    let mut frames = Vec::new();
    loop {
        let remaining = end.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(frames);
        }
        subscriber
            .reader
            .get_ref()
            .set_read_timeout(Some(remaining.min(Duration::from_secs(1))))?;
        let mut line = String::new();
        match subscriber.reader.read_line(&mut line) {
            Ok(0) => return Ok(frames),
            Ok(_) => frames.push(serde_json::from_str(&line)?),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                return Ok(frames);
            }
            Err(error) => return Err(error.into()),
        }
    }
}

/// Read frames until `predicate` matches one or `budget` elapses; returns
/// every frame seen so callers can assert on the whole prefix.
fn collect_until(
    subscriber: &mut Subscriber,
    budget: Duration,
    predicate: impl Fn(&Value) -> bool,
) -> Result<(Vec<Value>, bool)> {
    let mut frames = Vec::new();
    let mut found = false;
    for frame in collect_events(subscriber, budget)?.into_iter() {
        found |= predicate(&frame);
        frames.push(frame);
        if found {
            // Keep draining briefly so order/extra-event assertions see the
            // full emitted prefix, then stop.
            let rest = collect_events(subscriber, Duration::from_millis(150))?;
            frames.extend(rest);
            return Ok((frames, true));
        }
    }
    Ok((frames, false))
}

// ---------------------------------------------------------------------------
// Happy path: a committed canonical write on a DIFFERENT connection (the same
// mechanism a separate stock indexer process uses — schema triggers, never
// shared memory) arrives as one pushed event. The subscriber never asks for
// the catalog.
// ---------------------------------------------------------------------------

#[test]
fn subscribe_event_stream_delivers_committed_change() -> Result<()> {
    let (mut bridge, endpoint) =
        events_bridge(&[SeedConversation::new(1, "codex", "/seed-a", "seed-1")
            .agent_messages(Some(1_700_000_000_000), 1)])?;
    let mut subscriber = subscribe(&endpoint, None)?;

    let ack = read_event(&mut subscriber)?.expect("subscribed ack");
    ensure!(
        ack["type"] == "subscribed" && ack["current_revisions"].is_object(),
        "first frame must be the subscribed watermark ack: {ack}"
    );

    // Commit a canonical write on a second connection — the same mechanism
    // a separate stock indexer process uses (observer triggers are schema
    // objects; the IPC wakeup below is the hint, the journal is the truth).
    let conversation = insert_conversation(&bridge.db, &canned_conversation(2, "pushed", 2))?;
    journal::send_bridge_wakeup_best_effort(&bridge.db);

    let (frames, found) = collect_until(&mut subscriber, Duration::from_secs(3), |frame| {
        frame["type"] == journal::kind::SESSION_CREATED
    })?;
    ensure!(found, "no session_created event arrived: {frames:?}");
    let event = frames
        .iter()
        .find(|frame| frame["type"] == journal::kind::SESSION_CREATED)
        .expect("session_created frame");
    ensure!(
        event["conversation_id"].as_i64() == Some(conversation)
            && event["sequence"].as_i64().is_some_and(|seq| seq > 0)
            && event["canonical_revision"]
                .as_i64()
                .is_some_and(|rev| rev > 0),
        "pushed event must carry the spec-12 wire shape: {event}"
    );
    // One committed update arrived without any catalog/list fetch — the
    // event frame itself is the whole notification.
    ensure!(
        event["lexical_ready"].is_null() || event["lexical_ready"] == false,
        "no event may claim unverified lexical readiness: {event}"
    );
    drop(subscriber);
    let _ = bridge.shutdown();
    Ok(())
}

// ---------------------------------------------------------------------------
// Atomic reconnect: read watermark → subscribe → replay. Events committed
// while the receiver was down replay in seq order with no missed window,
// and the stdio `changes_since` RPC agrees with the stream.
// ---------------------------------------------------------------------------

#[test]
fn subscribe_reconnect_replays_missed_changes() -> Result<()> {
    let (mut bridge, endpoint) =
        events_bridge(&[SeedConversation::new(1, "codex", "/seed-b", "seed-1")
            .agent_messages(Some(1_700_000_000_000), 1)])?;

    // First attachment establishes the client's watermark.
    let watermark = {
        let mut subscriber = subscribe(&endpoint, None)?;
        let ack = read_event(&mut subscriber)?.expect("subscribed ack");
        ensure!(ack["type"] == "subscribed", "ack expected: {ack}");
        ack["high_water"].as_i64().unwrap_or(0)
    }; // subscriber drops here — the missed window opens.

    // Missed commits: a canonical insert AND an annotation edit.
    let conversation = insert_conversation(&bridge.db, &canned_conversation(2, "missed", 1))?;
    let writer = open(&bridge.db)?;
    writer.execute("BEGIN")?;
    writer.execute_with_params(
        "INSERT INTO viewer_annotations \
         (conversation_id,is_pinned,custom_title,notes,revision,updated_at_ms) \
         VALUES (?1,?2,?3,?4,?5,?6)",
        &param_slice_to_values(params![1_i64, 1_i64, "pinned-offline", "n", 1_i64, 1_i64]),
    )?;
    revisions::bump(&writer, revisions::Counter::Annotation, 1)?;
    writer.execute("COMMIT")?;
    drop(writer);

    // Reconnect with the watermark: every missed event replays in order.
    let mut subscriber = subscribe(&endpoint, Some(watermark))?;
    let ack = read_event(&mut subscriber)?.expect("subscribed ack");
    ensure!(
        ack["type"] == "subscribed" && ack["truncated"] == false,
        "{ack}"
    );
    let (frames, found) = collect_until(&mut subscriber, Duration::from_secs(3), |frame| {
        frame["type"] == journal::kind::ANNOTATIONS_UPDATED
    })?;
    ensure!(found, "missed-window events did not replay: {frames:?}");
    // The missed window replays in commit order: the insert's
    // session_created + session_updated (conversations and messages rows
    // both fire canonical observers) land before the annotation edit. A
    // leading coarse session_updated (NULL conversation_id) is also
    // admissible: the fixture's ensure_agent upsert legitimately renames
    // the seeded agent row, and dimension observers journal that rename.
    let events: Vec<&Value> = frames
        .iter()
        .filter(|frame| frame["type"] != "heartbeat")
        .collect();
    let kinds: Vec<&str> = events
        .iter()
        .filter_map(|frame| frame["type"].as_str())
        .collect();
    ensure!(
        kinds.last() == Some(&journal::kind::ANNOTATIONS_UPDATED)
            && kinds.iter().all(|kind| [
                journal::kind::SESSION_CREATED,
                journal::kind::SESSION_UPDATED,
                journal::kind::ANNOTATIONS_UPDATED
            ]
            .contains(kind)),
        "replay must deliver every missed event in commit order: {kinds:?}"
    );
    ensure!(
        events
            .iter()
            .any(|frame| frame["type"] == journal::kind::SESSION_CREATED
                && frame["conversation_id"].as_i64() == Some(conversation))
            && events[events.len() - 1]["conversation_id"].as_i64() == Some(1),
        "replayed events must keep their addresses: {events:?}"
    );
    let seqs: Vec<i64> = frames
        .iter()
        .filter_map(|frame| frame["sequence"].as_i64())
        .collect();
    ensure!(
        seqs.windows(2).all(|pair| pair[0] < pair[1]) && seqs[0] > watermark,
        "replayed sequences must be strictly above the watermark: {seqs:?} vs {watermark}"
    );

    // The stdio `changes_since` RPC — the refetch fallback — agrees.
    let page = bridge.exchange(json!({
        "op": "changes_since", "id": 77, "after_seq": watermark
    }))?;
    ensure!(page["ok"] == true, "changes_since refused: {page}");
    ensure!(
        page["result"]["next_seq"].as_i64() >= seqs.last().copied()
            && page["result"]["truncated"] == false
            && page["result"]["current_revisions"].is_object(),
        "RPC replay must cover the same committed window: {page}"
    );
    drop(subscriber);
    let _ = bridge.shutdown();
    Ok(())
}

// ---------------------------------------------------------------------------
// Task 8 readiness split: an annotation-only commit must NEVER emit either
// readiness kind; a canonical pass stamps `canonical_ready` (canonical
// progress only — never `index_ready`); and `index_ready` arrives only from
// a REAL Quill publication, carrying `lexical_ready:true` plus the durable
// `PublicationReceipt` coverage it certifies.
// ---------------------------------------------------------------------------

#[test]
fn subscribe_readiness_events_split_canonical_from_lexical() -> Result<()> {
    let (mut bridge, endpoint) =
        events_bridge(&[SeedConversation::new(1, "codex", "/seed-c", "seed-1")
            .agent_messages(Some(1_700_000_000_000), 1)])?;
    let mut subscriber = subscribe(&endpoint, None)?;
    let ack = read_event(&mut subscriber)?.expect("subscribed ack");
    ensure!(ack["type"] == "subscribed", "{ack}");
    ensure!(
        ack["lexical_tracking"] == "bound",
        "the subscribe ack must negotiate publication-bound lexical tracking: {ack}"
    );

    // Metadata-only edit on a second connection.
    let writer = open(&bridge.db)?;
    writer.execute("BEGIN")?;
    writer.execute_with_params(
        "INSERT INTO viewer_annotations \
         (conversation_id,is_pinned,custom_title,notes,revision,updated_at_ms) \
         VALUES (?1,?2,?3,?4,?5,?6)",
        &param_slice_to_values(params![1_i64, 1_i64, "meta", "n", 1_i64, 1_i64]),
    )?;
    revisions::bump(&writer, revisions::Counter::Annotation, 1)?;
    writer.execute("COMMIT")?;
    drop(writer);
    journal::send_bridge_wakeup_best_effort(&bridge.db);

    let (frames, found) = collect_until(&mut subscriber, Duration::from_secs(3), |frame| {
        frame["type"] == journal::kind::ANNOTATIONS_UPDATED
    })?;
    ensure!(found, "annotations_updated did not arrive: {frames:?}");
    ensure!(
        frames.iter().all(|frame| frame["type"] != journal::kind::INDEX_READY
            && frame["type"] != journal::kind::CANONICAL_READY),
        "a metadata edit must never claim either readiness kind: {frames:?}"
    );

    // A canonical commit + post-commit notify stamps `canonical_ready` —
    // canonical progress only; NO Quill publication happened, so
    // `index_ready` must stay absent.
    insert_conversation(&bridge.db, &canned_conversation(2, "content", 1))?;
    let maintainer = open(&bridge.db)?;
    journal::notify_committed_change_best_effort(&maintainer);
    drop(maintainer);
    journal::send_bridge_wakeup_best_effort(&bridge.db);

    let (frames, found) = collect_until(&mut subscriber, Duration::from_secs(3), |frame| {
        frame["type"] == journal::kind::CANONICAL_READY
    })?;
    ensure!(
        found,
        "canonical_ready did not arrive after a canonical pass: {frames:?}"
    );
    ensure!(
        frames
            .iter()
            .all(|frame| frame["type"] != journal::kind::INDEX_READY),
        "canonical progress must never masquerade as index_ready: {frames:?}"
    );
    let canonical_ready = frames
        .iter()
        .find(|frame| frame["type"] == journal::kind::CANONICAL_READY)
        .expect("canonical_ready frame");
    ensure!(
        canonical_ready["canonical_revision"].as_i64().unwrap_or(0) > 0,
        "canonical_ready must bind the canonical revision it certifies: {canonical_ready}"
    );

    // A REAL Quill publication — the authorized write lane's post-edit
    // region convergence commits the published index and certifies the
    // generation it sealed. Only now may `index_ready` appear, bound to
    // the durable PublicationReceipt coverage.
    let second_temp = tempfile::tempdir()?;
    let mut second = BridgeHarness::spawn_fixture(
        second_temp,
        bridge.data_dir.clone(),
        bridge.db.clone(),
        &["--enable-viewer-writes"],
    )?;
    let patched = second.exchange(json!({
        "op": "patch_annotations",
        "conversation_id": 1,
        "expected_revision": 1,
        "patch": {"is_pinned": false, "custom_title": "certified"}
    }))?;
    ensure!(patched["ok"] == true, "patch_annotations refused: {patched}");
    let edited = patched["result"]["generation"]["annotation_revision"]
        .as_i64()
        .expect("annotation_revision in patch receipt");

    let (frames, found) = collect_until(&mut subscriber, Duration::from_secs(3), |frame| {
        frame["type"] == journal::kind::INDEX_READY
    })?;
    ensure!(
        found,
        "a real Quill publication must emit index_ready: {frames:?}"
    );
    let ready = frames
        .iter()
        .find(|frame| frame["type"] == journal::kind::INDEX_READY)
        .expect("index_ready frame");
    ensure!(
        ready["lexical_ready"] == true,
        "a certified publication must report lexical_ready:true: {ready}"
    );
    ensure!(
        ready["annotation_revision"].as_i64().unwrap_or(0) >= edited,
        "index_ready coverage must reach the just-committed annotation revision \
         ({edited}): {ready}"
    );
    let receipt = &ready["payload"];
    ensure!(
        receipt["v"].as_i64() == Some(1)
            && receipt["lexical_generation"].as_i64() == ready["lexical_generation"].as_i64()
            && receipt["quill_generation"].as_i64().unwrap_or(0) >= 1
            && receipt["canonical_revision"].as_i64().is_some(),
        "index_ready must carry the durable PublicationReceipt: {ready}"
    );

    // The announced lexical generation IS the committed logical generation
    // the bridge reports.
    let hello = bridge.exchange(json!({"op":"hello","id":5}))?;
    let served_generation =
        hello["result"]["generation"]["lexical_generation"].as_i64();
    ensure!(
        served_generation == ready["lexical_generation"].as_i64(),
        "the served generation must equal the one index_ready announced: \
         {served_generation:?} vs {ready}"
    );
    drop(subscriber);
    let _ = second.shutdown();
    let _ = bridge.shutdown();
    Ok(())
}

// ---------------------------------------------------------------------------
// Crash/restart truth: `canonical_ready` is canonical progress only (it must
// never advance `lexical_generation`); a certified publication is a durable
// journal row + durable `viewer_revisions` bump — replayed identically by a
// restarted bridge that lost every in-memory watermark.
// ---------------------------------------------------------------------------

#[test]
fn certified_publication_replays_after_bridge_restart() -> Result<()> {
    let (mut bridge, _endpoint) =
        events_bridge(&[SeedConversation::new(1, "codex", "/seed-d", "seed-1")
            .agent_messages(Some(1_700_000_000_000), 1)])?;
    let data_dir = bridge.data_dir.clone();
    let db = bridge.db.clone();

    // Canonical progress alone must NOT mint a lexical generation.
    insert_conversation(&db, &canned_conversation(2, "pre-publish", 1))?;
    let maintainer = open(&db)?;
    journal::notify_committed_change_best_effort(&maintainer);
    drop(maintainer);
    let hello = bridge.exchange(json!({"op":"hello","id":9}))?;
    ensure!(
        hello["result"]["generation"]["lexical_generation"].as_i64() == Some(0),
        "canonical progress minted a lexical generation it did not publish: {hello}"
    );
    let page = journal::changes_since(&open(&db)?, 0, 256)?;
    ensure!(
        page.events.iter().any(|e| e.kind == journal::kind::CANONICAL_READY)
            && page.events.iter().all(|e| e.kind != journal::kind::INDEX_READY),
        "canonical pass must journal canonical_ready only: {:?}",
        page.events
    );

    // A REAL Quill publication on the served index: commit + region-stamp
    // path certifies the generation it sealed.
    let index_dir =
        coding_agent_search::search::tantivy::expected_index_dir(&data_dir);
    let mut index =
        coding_agent_search::search::tantivy::TantivyIndex::open_or_create(&index_dir)?;
    let writer = open(&db)?;
    index.commit_with_region_doc_stamps(&writer)?;
    drop(writer);
    journal::send_bridge_wakeup_best_effort(&db);

    // The certification is durable in the journal and in viewer_revisions.
    let generation = revisions::generation(&open(&db)?)?.expect("generation");
    ensure!(
        generation.lexical_generation == 1,
        "first certified publication must mint lexical_generation 1: {generation:?}"
    );
    let _ = bridge.shutdown();

    // A restarted bridge replays the durable index_ready — sequence,
    // receipt payload and lexical_ready from the journal alone.
    let restart_temp = tempfile::tempdir()?;
    let mut restarted =
        BridgeHarness::spawn_fixture(restart_temp, data_dir.clone(), db.clone(), EVENT_ARGS)?;
    let hello = restarted.exchange(json!({"op":"hello","id":1}))?;
    let restarted_endpoint = events_endpoint(&hello)?;
    let mut subscriber = subscribe(&restarted_endpoint, None)?;
    let ack = read_event(&mut subscriber)?.expect("subscribed ack");
    ensure!(
        ack["type"] == "subscribed" && ack["lexical_tracking"] == "bound",
        "{ack}"
    );
    let (frames, found) = collect_until(&mut subscriber, Duration::from_secs(3), |frame| {
        frame["type"] == journal::kind::INDEX_READY
    })?;
    ensure!(
        found,
        "a restarted bridge must replay the certified index_ready: {frames:?}"
    );
    let ready = frames
        .iter()
        .find(|frame| frame["type"] == journal::kind::INDEX_READY)
        .expect("index_ready frame");
    ensure!(
        ready["lexical_ready"] == true
            && ready["lexical_generation"].as_i64() == Some(1)
            && ready["payload"]["quill_generation"].as_i64().is_some()
            && ready["payload"]["v"].as_i64() == Some(1),
        "replayed index_ready must carry its durable receipt: {ready}"
    );
    let hello = restarted.exchange(json!({"op":"hello","id":2}))?;
    ensure!(
        hello["result"]["generation"]["lexical_generation"].as_i64() == Some(1)
            && hello["result"]["event_stream"]["lexical_tracking"] == "bound",
        "hello must report the certified generation + bound tracking: {hello}"
    );
    drop(subscriber);
    let _ = restarted.shutdown();
    Ok(())
}

// ---------------------------------------------------------------------------
// Deferred certification (rebuild ordering): a candidate manifest that is
// sealed but NEVER published — validation refused it — must leave no durable
// certification: no `lexical_generation` bump, no receipt row, no
// `index_ready` journal event. Only the manifest verified at the PUBLISHED
// path may certify.
// ---------------------------------------------------------------------------

#[test]
fn deferred_certification_binds_published_manifest_never_a_refused_candidate() -> Result<()> {
    let fixture = fixture()?;
    let index_root = fixture.db.parent().expect("data dir").join("lexical");
    let candidate_dir = index_root.join("candidate");
    let published_dir = index_root.join("published");

    // A candidate seals a manifest at its build path. Validation then
    // refuses it (count mismatch / unreadable): the writer's armed floor
    // is disarmed and NO certification call is ever made for it.
    let writer = open(&fixture.db)?;
    let mut candidate =
        coding_agent_search::search::tantivy::TantivyIndex::open_or_create(&candidate_dir)?;
    candidate.capture_publication_floor(&writer)?;
    candidate.commit()?;
    let armed_floor = candidate.take_publication_floor();
    ensure!(
        armed_floor.is_some(),
        "the extended archive must arm a coverage floor"
    );
    drop(candidate);

    // The refusal path: certification is only ever invoked for the
    // PUBLISHED path — a directory without a durable manifest must no-op.
    coding_agent_search::search::tantivy::TantivyIndex::certify_published_manifest(
        &writer,
        &published_dir,
        armed_floor,
    );
    let generation = revisions::generation(&writer)?.expect("generation");
    ensure!(
        generation.lexical_generation == 0,
        "a refused candidate must not mint a lexical generation: {generation:?}"
    );
    ensure!(
        journal::latest_lexical_publication(&writer)?.is_none(),
        "a refused candidate must leave no durable receipt"
    );
    let page = journal::changes_since(&writer, 0, 256)?;
    ensure!(
        page.events.iter().all(|e| e.kind != journal::kind::INDEX_READY),
        "a refused candidate must not journal index_ready: {:?}",
        page.events
    );

    // Publish for real: a manifest sealed at the PUBLISHED path certifies
    // against the armed floor.
    let mut live =
        coding_agent_search::search::tantivy::TantivyIndex::open_or_create(&published_dir)?;
    live.commit()?;
    coding_agent_search::search::tantivy::TantivyIndex::certify_published_manifest(
        &writer,
        &published_dir,
        armed_floor,
    );
    let receipt = journal::latest_lexical_publication(&writer)?.expect("receipt");
    ensure!(
        receipt.lexical_generation == 1
            && receipt.quill_generation >= 1
            && receipt.v == journal::PUBLICATION_RECEIPT_VERSION,
        "the published manifest must certify once, bound to its receipt: {receipt:?}"
    );
    let page = journal::changes_since(&writer, 0, 256)?;
    ensure!(
        page.events.iter().any(|e| e.kind == journal::kind::INDEX_READY),
        "the published certification must journal index_ready: {:?}",
        page.events
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Build-start coverage floor: a canonical commit landing DURING the build
// lands above the armed floor, so the deferred certification's receipt
// covers only the revisions the build could have indexed — never the
// broader current-at-certify snapshot.
// ---------------------------------------------------------------------------

#[test]
fn build_start_floor_bounds_receipt_coverage() -> Result<()> {
    let fixture = fixture()?;
    let index_dir = fixture.db.parent().expect("data dir").join("lexical");
    let writer = open(&fixture.db)?;

    // Build start: the rebuild captures the committed revisions BEFORE
    // any document is staged and arms the writer's floor with them.
    let build_floor = journal::current_journal_revisions(&writer)?.expect("revisions");
    let mut index =
        coding_agent_search::search::tantivy::TantivyIndex::open_or_create(&index_dir)?;
    index.arm_publication_floor(Some(build_floor));

    // A canonical commit lands mid-build — its content was never staged.
    insert_conversation(&fixture.db, &canned_conversation(1, "mid-build", 1))?;
    let mid_build = journal::current_journal_revisions(&writer)?.expect("revisions");
    ensure!(
        mid_build.canonical_revision > build_floor.canonical_revision,
        "the mid-build commit must land above the floor: {mid_build:?} vs {build_floor:?}"
    );

    // The build seals, validates, publishes — certification binds the
    // armed build-start floor, not the wider current revisions.
    index.commit()?;
    let armed = index.take_publication_floor();
    coding_agent_search::search::tantivy::TantivyIndex::certify_published_manifest(
        &writer, &index_dir, armed,
    );
    let receipt = journal::latest_lexical_publication(&writer)?.expect("receipt");
    ensure!(
        receipt.canonical_revision == build_floor.canonical_revision,
        "receipt coverage must bind the build-start floor, not the \
         mid-build revision: {receipt:?} vs {build_floor:?}"
    );
    ensure!(
        receipt.canonical_revision < mid_build.canonical_revision,
        "the mid-build commit must NOT be claimed covered: {receipt:?}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Receipt-version gate: `index_ready` claims `lexical_ready:true` ONLY for a
// version-1 PublicationReceipt payload. Retained pre-Task-8 rows and
// malformed receipts (`v` as bool/string/missing) report
// `lexical_ready:false` — never a readiness claim.
// ---------------------------------------------------------------------------

#[test]
fn index_ready_frame_requires_v1_receipt_payload() -> Result<()> {
    let (mut bridge, endpoint) =
        events_bridge(&[SeedConversation::new(1, "codex", "/seed-g", "seed-1")
            .agent_messages(Some(1_700_000_000_000), 1)])?;
    let mut subscriber = subscribe(&endpoint, None)?;
    let ack = read_event(&mut subscriber)?.expect("subscribed ack");
    ensure!(ack["type"] == "subscribed", "{ack}");

    // Retained legacy/malformed index_ready payloads — each injected and
    // framed individually (same-key journal rows coalesce inside a page).
    for payload in [
        None,
        Some(json!({"note":"pre-task-8 marker"}).to_string()),
        Some(json!({"v":2,"quill_generation":9}).to_string()),
        Some(json!({"v":true,"quill_generation":9}).to_string()),
        Some(json!({"v":"1","quill_generation":9}).to_string()),
    ] {
        let writer = open(&bridge.db)?;
        journal::append_change_event(
            &writer,
            journal::kind::INDEX_READY,
            None,
            0,
            payload.clone(),
        )?;
        drop(writer);
        journal::send_bridge_wakeup_best_effort(&bridge.db);
        let (frames, found) = collect_until(&mut subscriber, Duration::from_secs(3), |frame| {
            frame["type"] == journal::kind::INDEX_READY
        })?;
        ensure!(found, "index_ready did not arrive: {frames:?}");
        let ready = frames
            .iter()
            .find(|frame| frame["type"] == journal::kind::INDEX_READY)
            .expect("index_ready frame");
        ensure!(
            ready["lexical_ready"] == false,
            "receipt payload {payload:?} must never claim lexical readiness: {ready}"
        );
    }
    drop(subscriber);
    let _ = bridge.shutdown();
    Ok(())
}

// ---------------------------------------------------------------------------
// Dimension invalidation: a rename on `agents`/`workspaces`/`sources` is a
// canonical change the served projection joins — it must bump
// `canonical_revision` and journal a coarse `session_updated` with a NULL
// conversation_id so live clients invalidate. A same-values rewrite stays
// silent.
// ---------------------------------------------------------------------------

#[test]
fn dimension_rename_journals_coarse_session_updated() -> Result<()> {
    let fixture = fixture()?;
    insert_conversation(&fixture.db, &canned_conversation(1, "dim", 1))?;
    let cursor = journal::changes_since(&open(&fixture.db)?, 0, 256)?.next_seq;

    let agent_id: i64 = open(&fixture.db)?
        .query_row("SELECT id FROM agents WHERE slug = 'codex' LIMIT 1")?
        .get_typed(0)?;
    let writer = open(&fixture.db)?;
    writer.execute_with_params(
        "UPDATE agents SET name = ?1 WHERE id = ?2",
        &param_slice_to_values(params![String::from("Codex Renamed"), agent_id]),
    )?;
    drop(writer);

    let reader = open(&fixture.db)?;
    let page = journal::changes_since(&reader, cursor, 256)?;
    let coarse = page
        .events
        .iter()
        .filter(|e| e.kind == journal::kind::SESSION_UPDATED)
        .collect::<Vec<_>>();
    ensure!(
        coarse.len() == 1 && coarse[0].conversation_id.is_none(),
        "a dimension rename must journal one coarse session_updated with \
         NULL conversation_id: {:?}",
        page.events
    );
    let generation = revisions::generation(&reader)?.expect("generation");
    ensure!(
        generation.canonical_revision > 0,
        "a dimension rename must bump canonical_revision: {generation:?}"
    );

    // Same-values rewrite: neither a bump nor a journal row.
    let cursor = page.next_seq;
    let before = generation.canonical_revision;
    let writer = open(&fixture.db)?;
    writer.execute_with_params(
        "UPDATE agents SET name = ?1 WHERE id = ?2",
        &param_slice_to_values(params![String::from("Codex Renamed"), agent_id]),
    )?;
    drop(writer);
    let reader = open(&fixture.db)?;
    let page = journal::changes_since(&reader, cursor, 256)?;
    ensure!(
        page.events.is_empty(),
        "a same-values dimension rewrite must journal nothing: {:?}",
        page.events
    );
    ensure!(
        revisions::generation(&reader)?.expect("generation").canonical_revision == before,
        "a same-values dimension rewrite must not bump canonical_revision"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Schema-v7 self-upgrade: an archive whose extension profile predates v8
// (no `viewer_lexical_publication`) self-upgrades when the authorized write
// lane opens — `OwnedWriter::open` runs `ensure_schema` before observers —
// so certification can never silently no-op under `lexical_tracking: bound`.
// ---------------------------------------------------------------------------

#[test]
fn schema_v7_archive_upgrades_when_owned_writer_opens() -> Result<()> {
    let mut bridge = BridgeHarness::with_seed(
        &[SeedConversation::new(1, "codex", "/seed-v7", "seed-1")
            .agent_messages(Some(1_700_000_000_000), 1)],
        &["--enable-viewer-writes"],
    )?;
    init_extensions(&bridge)?;

    // Rewind the profile to v7: V8 is purely additive (one table + the
    // version marker), so dropping both restores the v7 shape exactly.
    let installer = open(&bridge.db)?;
    installer.execute("DROP TABLE IF EXISTS viewer_lexical_publication")?;
    installer.execute(
        "INSERT OR REPLACE INTO viewer_meta (key, value) \
         VALUES ('extension_schema_version', '7')",
    )?;
    ensure!(
        coding_agent_search::viewer_extensions::schema::installed_version(&installer)?
            == Some(7),
        "fixture must stand at schema v7"
    );
    drop(installer);

    // The write lane's first mutation opens `OwnedWriter` → ensure_schema
    // upgrades the archive in place.
    let patched = bridge.exchange(json!({
        "op": "patch_annotations",
        "conversation_id": 1,
        "expected_revision": 0,
        "patch": {"is_pinned": true}
    }))?;
    ensure!(patched["ok"] == true, "patch_annotations refused: {patched}");

    let reader = open(&bridge.db)?;
    ensure!(
        coding_agent_search::viewer_extensions::schema::installed_version(&reader)?
            == Some(8),
        "OwnedWriter::open must self-upgrade the v7 profile to v8"
    );
    // Certification is live on the upgraded archive, not a silent no-op.
    let index_dir =
        coding_agent_search::search::tantivy::expected_index_dir(&bridge.data_dir);
    let mut index =
        coding_agent_search::search::tantivy::TantivyIndex::open_or_create(&index_dir)?;
    index.commit_with_region_doc_stamps(&reader)?;
    ensure!(
        journal::latest_lexical_publication(&reader)?.is_some(),
        "the upgraded archive must certify real publications"
    );
    let _ = bridge.shutdown();
    Ok(())
}

// ---------------------------------------------------------------------------
// Backpressure: a missed window larger than the bounded pending queue is
// answered with `reset_required` + close — the client falls back to
// `changes_since` refetch instead of receiving an unbounded flood or a
// silently truncated stream. Resume at the high-water works afterwards.
// ---------------------------------------------------------------------------

#[test]
fn subscribe_queue_overflow_requests_reset_required() -> Result<()> {
    // Seed MORE distinct journal events than the pending-queue bound: every
    // conversation commits its own session_created, and read-side
    // coalescing keeps one event per (kind, conversation_id) — 300 distinct
    // conversations are 300 replayed events.
    let temp = tempfile::tempdir()?;
    let data_dir = temp.path().join("data");
    std::fs::create_dir(&data_dir)?;
    let db = data_dir.join("archive.db");
    let storage = FrankenStorage::open(&db)?;
    drop(storage);
    let installer = open(&db)?;
    coding_agent_search::viewer_extensions::schema::ensure_schema(&installer)?;
    revisions::ensure_observers(&installer)?;
    drop(installer);
    insert_conversation(&db, &canned_conversation(1, "bulk", 1))?;
    let writer = open(&db)?;
    let agent_id: i64 = writer
        .query_row("SELECT agent_id FROM conversations WHERE id = 1")?
        .get_typed(0)?;
    writer.execute("BEGIN")?;
    for id in 2..=300_i64 {
        writer.execute_with_params(
            "INSERT INTO conversations (agent_id, source_id, external_id, source_path) \
             VALUES (?1, 'local', ?2, ?3)",
            &param_slice_to_values(params![
                agent_id,
                format!("bulk-{id}"),
                format!("/bulk-{id}")
            ]),
        )?;
    }
    writer.execute("COMMIT")?;
    drop(writer);
    let high_water = {
        let reader = open(&db)?;
        let seq = journal::journal_high_water(&reader)?;
        drop(reader);
        seq
    };
    ensure!(
        high_water > 256,
        "fixture must exceed the pending bound: {high_water}"
    );

    let mut bridge = BridgeHarness::spawn_fixture(temp, data_dir, db, EVENT_ARGS)?;
    let hello = bridge.exchange(json!({"op":"hello","id":1}))?;
    let endpoint = events_endpoint(&hello)?;

    let mut subscriber = subscribe(&endpoint, None)?;
    let ack = read_event(&mut subscriber)?.expect("subscribed ack");
    ensure!(ack["type"] == "subscribed", "{ack}");
    let mut saw_reset = false;
    let mut saw_eof = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !saw_eof {
        match read_event(&mut subscriber)? {
            Some(frame) if frame["type"] == journal::kind::RESET_REQUIRED => saw_reset = true,
            Some(_) => {}
            None => saw_eof = true,
        }
        if saw_reset && saw_eof {
            break;
        }
    }
    ensure!(saw_reset, "overflow must be answered reset_required");
    ensure!(saw_eof, "overflowed subscriber must be closed");

    // The reset fallback: refetch through changes_since and resume at the
    // high-water — the channel then serves normally again.
    let mut subscriber = subscribe(&endpoint, Some(high_water))?;
    let ack = read_event(&mut subscriber)?.expect("subscribed ack");
    ensure!(
        ack["type"] == "subscribed" && ack["truncated"] == false,
        "resume at high-water must subscribe cleanly: {ack}"
    );
    let (frames, heartbeat) = collect_until(&mut subscriber, Duration::from_secs(2), |frame| {
        frame["type"] == "heartbeat"
    })?;
    ensure!(
        heartbeat
            && frames
                .iter()
                .all(|frame| frame["type"] != journal::kind::RESET_REQUIRED),
        "post-reset subscriber must get liveness, not resets: {frames:?}"
    );
    drop(subscriber);
    let _ = bridge.shutdown();
    Ok(())
}

// ---------------------------------------------------------------------------
// Opt-in boundary: without --enable-viewer-events the bridge opens no
// listener and writes no endpoint sidecar; the `changes_since` RPC stays an
// ordinary read op either way.
// ---------------------------------------------------------------------------

#[test]
fn subscribe_channel_is_opt_in_and_rpc_stays_read_only() -> Result<()> {
    let mut bridge = BridgeHarness::with_seed(
        &[SeedConversation::new(1, "codex", "/seed-d", "seed-1")
            .agent_messages(Some(1_700_000_000_000), 1)],
        &[],
    )?;
    let hello = bridge.exchange(json!({"op":"hello","id":1}))?;
    ensure!(hello["ok"] == true, "hello refused: {hello}");
    ensure!(
        hello["result"]["event_stream"]["enabled"] == false
            && hello["result"]["event_stream"]["endpoint"].is_null(),
        "event channel must stay closed without the explicit opt-in: {hello}"
    );
    ensure!(
        !journal::event_endpoint_sidecar_path(&bridge.db).exists(),
        "no endpoint sidecar may be written without the opt-in"
    );

    // changes_since works without the push channel — an unextended archive
    // reports an honest empty page, never an opaque error.
    let page = bridge.exchange(json!({"op":"changes_since","id":7,"after_seq":0}))?;
    ensure!(page["ok"] == true, "changes_since refused: {page}");
    ensure!(
        page["result"]["events"]
            .as_array()
            .is_some_and(Vec::is_empty)
            && page["result"]["truncated"] == false,
        "unextended archive must report an empty page: {page}"
    );
    let _ = bridge.shutdown();
    Ok(())
}

// ---------------------------------------------------------------------------
// changes_since RPC contract: bounded replay, resume token, expired-cursor
// reset and input validation over the serialized stdio FIFO.
// ---------------------------------------------------------------------------

#[test]
fn changes_since_rpc_replay_resume_and_bounds() -> Result<()> {
    let mut bridge = BridgeHarness::with_seed(
        &[SeedConversation::new(1, "codex", "/seed-e", "seed-1")
            .agent_messages(Some(1_700_000_000_000), 1)],
        &[],
    )?;
    init_extensions(&bridge)?;
    bridge.exchange(json!({"op":"hello","id":1}))?;

    insert_conversation(&bridge.db, &canned_conversation(2, "first", 1))?;
    let first = bridge.exchange(json!({"op":"changes_since","id":2,"after_seq":0}))?;
    ensure!(first["ok"] == true, "changes_since refused: {first}");
    let next_seq = first["result"]["next_seq"].as_i64().expect("next_seq");
    ensure!(
        first["result"]["events"]
            .as_array()
            .is_some_and(|events| events
                .iter()
                .any(|event| event["kind"] == journal::kind::SESSION_CREATED)),
        "first page must contain the committed insert: {first}"
    );

    // Resume from the token: a second commit yields only the new events.
    insert_conversation(&bridge.db, &canned_conversation(3, "second", 1))?;
    let resumed = bridge.exchange(json!({
        "op": "changes_since", "id": 3, "after_seq": next_seq
    }))?;
    ensure!(
        resumed["result"]["events"]
            .as_array()
            .is_some_and(|events| events.iter().all(|event| event["conversation_id"] == 3)),
        "resumed page must contain only the second commit: {resumed}"
    );

    // Expired/regressed cursor: truncated + reset_required, never silence.
    let regressed = bridge.exchange(json!({
        "op": "changes_since", "id": 4, "after_seq": 9_999_999_i64
    }))?;
    ensure!(
        regressed["ok"] == true
            && regressed["result"]["truncated"] == true
            && regressed["result"]["events"]
                .as_array()
                .is_some_and(|events| events
                    .iter()
                    .any(|event| event["kind"] == journal::kind::RESET_REQUIRED)),
        "a cursor past the high-water must reset: {regressed}"
    );

    // Input validation stays typed.
    for (id, body) in [
        (5, json!({"op":"changes_since","id":5,"after_seq":-1})),
        (6, json!({"op":"changes_since","id":6,"limit":0})),
        (7, json!({"op":"changes_since","id":7,"limit":2048})),
    ] {
        let refused = bridge.exchange(body)?;
        ensure!(
            refused["ok"] == false && refused["error"]["kind"] == "invalid_request",
            "request {id} must be a typed invalid_request refusal: {refused}"
        );
    }
    let _ = bridge.shutdown();
    Ok(())
}

// ---------------------------------------------------------------------------
// Literal second process: a stock writer (a second bridge holding only the
// write opt-in) commits an annotation edit; the FIRST bridge's subscriber
// is notified. The commit's wakeup rides the endpoint sidecar; the journal
// remains the truth either way.
// ---------------------------------------------------------------------------

#[test]
fn subscribe_second_process_write_notifies_first_bridge() -> Result<()> {
    let (mut first, endpoint) =
        events_bridge(&[SeedConversation::new(1, "codex", "/seed-f", "seed-1")
            .agent_messages(Some(1_700_000_000_000), 1)])?;
    let mut subscriber = subscribe(&endpoint, None)?;
    let ack = read_event(&mut subscriber)?.expect("subscribed ack");
    ensure!(ack["type"] == "subscribed", "{ack}");

    // A second bridge process on the SAME archive, authorized for writes
    // only — it never opens the event listener; its post-commit wakeup
    // still reaches the first bridge through the sidecar file.
    let second_temp = tempfile::tempdir()?;
    let mut second = BridgeHarness::spawn_fixture(
        second_temp,
        first.data_dir.clone(),
        first.db.clone(),
        &["--enable-viewer-writes"],
    )?;
    let patched = second.exchange(json!({
        "op": "patch_annotations",
        "id": 9,
        "conversation_id": 1,
        "expected_revision": 0,
        "patch": {"is_pinned": true}
    }))?;
    ensure!(
        patched["ok"] == true,
        "second-process write refused: {patched}"
    );

    let (frames, found) = collect_until(&mut subscriber, Duration::from_secs(3), |frame| {
        frame["type"] == journal::kind::ANNOTATIONS_UPDATED
    })?;
    ensure!(
        found
            && frames
                .iter()
                .any(|frame| frame["conversation_id"].as_i64() == Some(1)),
        "the first bridge must publish the second process's committed write: {frames:?}"
    );
    drop(subscriber);
    let _ = second.shutdown();
    let _ = first.shutdown();
    Ok(())
}

// ---------------------------------------------------------------------------
// Task 14 stress — journal overflow during bulk ingest: a burst of separate
// ingest commits overflows any single page; paged `changes_since` replay
// must still cover every committed conversation with strictly increasing
// seqs across page boundaries and a clean resume token at the tail.
// ---------------------------------------------------------------------------

#[test]
fn changes_since_bulk_ingest_paged_replay_covers_every_commit() -> Result<()> {
    let mut bridge = BridgeHarness::with_seed(&[], &[])?;
    init_extensions(&bridge)?;
    bridge.exchange(json!({"op":"hello","id":1}))?;

    // Bulk ingest: 24 independent commits (each journals session_created
    // plus per-message session_updated rows) — far beyond one page.
    const BURST: i64 = 24;
    for id in 1..=BURST {
        insert_conversation(&bridge.db, &canned_conversation(id, "burst", 2))?;
    }

    let mut created = std::collections::BTreeSet::new();
    let mut last_seq = 0_i64;
    let mut after = 0_i64;
    let mut pages = 0_usize;
    loop {
        let page = bridge.exchange(json!({
            "op": "changes_since", "after_seq": after, "limit": 4
        }))?;
        ensure!(page["ok"] == true, "changes_since page refused: {page}");
        ensure!(
            page["result"]["truncated"] == false,
            "a live cursor must never be truncated mid-replay: {page}"
        );
        let events = page["result"]["events"].as_array().expect("events");
        for event in events {
            if let Some(seq) = event["seq"].as_i64() {
                ensure!(
                    seq > last_seq,
                    "journal seqs must increase across page boundaries: {seq} <= {last_seq}"
                );
                last_seq = seq;
            }
            if event["kind"] == journal::kind::SESSION_CREATED
                && let Some(cid) = event["conversation_id"].as_i64()
            {
                created.insert(cid);
            }
        }
        let next_seq = page["result"]["next_seq"].as_i64().expect("next_seq");
        pages += 1;
        if events.is_empty() || next_seq <= after {
            break;
        }
        after = next_seq;
        ensure!(pages < 100, "paged replay must terminate: {page}");
    }

    ensure!(
        pages > 1,
        "a {BURST}-commit burst must span multiple pages at limit=4"
    );
    ensure!(
        created.len() == BURST as usize && created.iter().copied().eq(1..=BURST),
        "paged replay must surface every committed session_created: {created:?}"
    );
    let _ = bridge.shutdown();
    Ok(())
}

// ---------------------------------------------------------------------------
// Task 14 stress — deep pagination under concurrent writes: `list_sessions`
// keyset cursors are position-bound, not generation-bound, so a second
// connection's inserts between pages must never duplicate or lose a row
// that predates the walk (new rows join only by keyset position).
// ---------------------------------------------------------------------------

#[test]
fn list_sessions_deep_pagination_survives_concurrent_inserts() -> Result<()> {
    // Eight seeded rows, deterministic created_asc order. `claude_code`
    // session-file identities verify through `identity::resolve_public_ids`
    // (a bare codex fixture id would refuse `identity_conflict` — only
    // rollout filenames or UUID external ids are admissible there).
    let seeds: Vec<SeedConversation> = (1..=8_i64)
        .map(|id| {
            SeedConversation::new(
                id,
                "claude_code",
                &format!("/sessions/seed-{id}.jsonl"),
                &format!("seed-{id}"),
            )
            .started_at(Some(1_000 * id))
            .agent_messages(Some(1_000 * id), 1)
        })
        .collect();
    let mut bridge = BridgeHarness::with_seed(&seeds, &[])?;
    init_extensions(&bridge)?;
    bridge.exchange(json!({"op":"hello","id":1}))?;

    let mut seen: Vec<i64> = Vec::new();
    let mut cursor: Option<String> = None;
    let mut inserted = false;
    loop {
        let mut request = json!({"op":"list_sessions","sort":"created_asc","limit":3});
        if let Some(cursor) = &cursor {
            request["cursor"] = json!(cursor);
        }
        let page = bridge.exchange(request)?;
        ensure!(page["ok"] == true, "list_sessions page refused: {page}");
        for item in page["result"]["items"].as_array().expect("items") {
            seen.push(item["conversation_id"].as_i64().expect("conversation_id"));
        }
        // A stock indexer commits mid-walk, between page 1 and page 2 —
        // the canonical commit AND its post-commit extension publication
        // (the `publish_ingest_generations_batch_best_effort` call the
        // indexer itself runs): the conversations-INSERT trigger leaves
        // each new sort row's public tie key pending, and the generation
        // transaction resolves it. A browse page that cannot verify a
        // candidate's public identity refuses `facts_pending` rather than
        // guess the order, so the walker must see the finished story.
        if !inserted {
            let storage = FrankenStorage::open(&bridge.db)?;
            let agent = storage.ensure_agent(&Agent {
                id: None,
                slug: "claude_code".into(),
                name: "claude_code".into(),
                version: None,
                kind: AgentKind::Cli,
            })?;
            let mut published: Vec<(Conversation, i64)> = Vec::new();
            for id in 9..=11_i64 {
                let mut conv = canned_conversation(id, "mid-walk", 1);
                conv.agent_slug = "claude_code".into();
                conv.external_id = Some(format!("mid-{id}"));
                conv.source_path = format!("/sessions/mid-{id}.jsonl").into();
                let outcome = storage.insert_conversation_tree(agent, None, &conv)?;
                published.push((conv, outcome.conversation_id));
            }
            drop(storage);
            let conn = Connection::open(bridge.db.to_string_lossy().into_owned())?;
            coding_agent_search::viewer_extensions::projection::publish_ingest_generations_batch_best_effort(
                &conn,
                published.iter().map(|(conv, id)| (conv, *id)),
            );
            drop(conn);
            inserted = true;
        }
        match page["result"]["next_cursor"].as_str() {
            Some(next) => cursor = Some(next.to_owned()),
            None => break,
        }
        ensure!(seen.len() < 64, "deep pagination must terminate: {page}");
    }

    let originals: Vec<i64> = seen.iter().copied().filter(|id| *id <= 8).collect();
    ensure!(
        originals == (1..=8).collect::<Vec<_>>(),
        "every pre-walk row must appear exactly once, in key order: {seen:?}"
    );
    let mut dedup = seen.clone();
    dedup.sort_unstable();
    dedup.dedup();
    ensure!(
        dedup.len() == seen.len(),
        "concurrent inserts must never duplicate a keyset position: {seen:?}"
    );
    let _ = bridge.shutdown();
    Ok(())
}
