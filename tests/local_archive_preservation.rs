//! Characterization test for the local-only migration (Task 0 baseline;
//! Task 11 is the behavioral owner).
//!
//! The archive must keep serving rows whose remote source vanished: with no
//! `sources.toml` at all (and therefore nothing that names `work-laptop`),
//! `cass index --full` must not drop or rewrite any canonical conversation,
//! and the read-only CLI surface (`search --source`, `view`, `expand`,
//! `export`, `export-html`, `stats --source`) must keep answering from the
//! archived snapshot alone — the original session files are gone.
//!
//! Fixture conversations, written through the project's own storage API:
//!   * `source_id = "local"`, no origin host — an ordinary local row.
//!   * `source_id = "work-laptop"`, `origin_host` set — an SSH-source row;
//!     the storage layer auto-registers `work-laptop` with `kind = "ssh"`.
//!   * `source_id = ""`, `origin_host` set — a pre-normalization legacy row,
//!     produced by an explicit UPDATE after insert; the `sources` table row
//!     the insert auto-created is then deleted so nothing describes the
//!     source at all.
//!
//! If this test goes red after remote-sync removal, the regression is real —
//! every check reads the same archive a user keeps. A pre-change failure is
//! characterization too: per the Task 0 ruling it is committed `#[ignore]`d
//! and closed in Task 11.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use coding_agent_search::franken_sync::compat::{ConnectionExt, RowExt};
use coding_agent_search::franken_sync::{Connection as FrankenConnection, params};
use coding_agent_search::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use coding_agent_search::storage::sqlite::SqliteStorage;
use serde_json::Value;

mod util;
use util::cass_bin;

// Unique markers that cannot collide with tokenizer noise or other fixtures.
const MARK_LOCAL: &str = "zzlocalmarker7f3a";
const MARK_REMOTE: &str = "zzremotemarker9b2c";
const MARK_LEGACY: &str = "zzlegacymarker4d8e";

const WORK_SOURCE_ID: &str = "work-laptop";
const WORK_ORIGIN_HOST: &str = "alice@work-laptop.example";
const LEGACY_ORIGIN_HOST: &str = "legacy-remote-host";

/// One canonical row as the test cares about it: identity, provenance and the
/// message count. `index --full` must leave the whole set bit-identical.
type ConversationRow = (i64, String, Option<String>, i64);

fn snapshot_conversations(db_path: &Path) -> Vec<ConversationRow> {
    let conn = FrankenConnection::open(db_path.to_string_lossy().into_owned())
        .expect("open archive for snapshot");
    let rows = conn
        .query_map_collect(
            "SELECT c.id, c.source_id, c.origin_host, \
                    (SELECT COUNT(*) FROM messages m WHERE m.conversation_id = c.id) \
             FROM conversations c ORDER BY c.id",
            &[],
            |r| {
                Ok((
                    r.get_typed::<i64>(0)?,
                    r.get_typed::<Option<String>>(1)?.unwrap_or_default(),
                    r.get_typed::<Option<String>>(2)?,
                    r.get_typed::<i64>(3)?,
                ))
            },
        )
        .expect("snapshot conversations");
    conn.close().expect("close snapshot connection");
    rows
}

fn source_rows(db_path: &Path) -> Vec<(String, String, Option<String>)> {
    let conn = FrankenConnection::open(db_path.to_string_lossy().into_owned())
        .expect("open archive for sources snapshot");
    let rows = conn
        .query_map_collect(
            "SELECT id, kind, host_label FROM sources ORDER BY id",
            &[],
            |r| {
                Ok((
                    r.get_typed::<String>(0)?,
                    r.get_typed::<Option<String>>(1)?.unwrap_or_default(),
                    r.get_typed::<Option<String>>(2)?,
                ))
            },
        )
        .expect("snapshot sources");
    conn.close().expect("close sources snapshot connection");
    rows
}

fn message(idx: i64, content: &str) -> Message {
    Message {
        id: None,
        idx,
        role: if idx % 2 == 0 {
            MessageRole::User
        } else {
            MessageRole::Agent
        },
        author: Some(if idx % 2 == 0 { "user" } else { "agent" }.into()),
        created_at: Some(1_700_000_000_000 + idx * 1_000),
        content: content.to_string(),
        extra_json: serde_json::json!({}),
        snippets: vec![],
    }
}

fn conversation(source_id: &str, origin_host: Option<&str>, external_id: &str) -> Conversation {
    Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/fixture/workspace")),
        external_id: Some(external_id.to_string()),
        title: Some(format!("fixture {external_id}")),
        source_path: PathBuf::from(format!(
            "/remote/{}/.codex/sessions/rollout-{}.jsonl",
            external_id, external_id
        )),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_010_000),
        approx_tokens: Some(64),
        metadata_json: serde_json::json!({"fixture": true}),
        messages: Vec::new(),
        source_id: source_id.to_string(),
        origin_host: origin_host.map(str::to_string),
    }
}

struct Fixture {
    _tmp: tempfile::TempDir,
    home: PathBuf,
    config: PathBuf,
    xdg_data: PathBuf,
    data_dir: PathBuf,
    db_path: PathBuf,
}

impl Fixture {
    fn cass(&self, args: &[&str]) -> Output {
        Command::new(cass_bin())
            .args(args)
            .current_dir(&self.home)
            // Hermetic environment: no inherited CASS_*/XDG/HOME. Note that
            // CASS_IGNORE_SOURCES_CONFIG is deliberately absent — the
            // config-load (and its DB fallback) is part of what is being
            // characterized.
            .env_clear()
            .env("PATH", "/usr/bin:/bin:/usr/local/bin")
            .env("TERM", "dumb")
            .env("HOME", &self.home)
            .env("XDG_CONFIG_HOME", &self.config)
            .env("XDG_DATA_HOME", &self.xdg_data)
            .env("CASS_DATA_DIR", &self.data_dir)
            .env("CASS_AUTO_REFRESH", "0")
            .env("CODING_AGENT_SEARCH_NO_UPDATE_PROMPT", "1")
            .output()
            .expect("spawn cass")
    }

    fn cass_ok(&self, args: &[&str]) -> Output {
        let out = self.cass(args);
        assert!(
            out.status.success(),
            "cass {:?} failed: status={:?}\nstdout={}\nstderr={}",
            args,
            out.status.code(),
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        out
    }
}

/// Build the fixture archive: one local conversation, one ssh-source
/// conversation, one pre-normalization legacy row (empty `source_id`,
/// `origin_host` set, no `sources` table entry).
fn build_fixture() -> (Fixture, i64) {
    let tmp = tempfile::tempdir().expect("fixture tempdir");
    let home = tmp.path().join("home");
    let config = tmp.path().join("config");
    let xdg_data = tmp.path().join("xdg-data");
    let data_dir = tmp.path().join("data");
    for dir in [&home, &config, &xdg_data, &data_dir] {
        fs::create_dir_all(dir).expect("create fixture dir");
    }
    let db_path = data_dir.join("agent_search.db");

    let (work_conv_id, legacy_conv_id);
    {
        let storage = SqliteStorage::open(&db_path).expect("create fixture archive db");
        let agent_id = storage
            .ensure_agent(&Agent {
                id: None,
                slug: "codex".into(),
                name: "Codex".into(),
                version: Some("fixture".into()),
                kind: AgentKind::Cli,
            })
            .expect("ensure fixture agent");
        let workspace_id = storage
            .ensure_workspace(&PathBuf::from("/fixture/workspace"), Some("workspace"))
            .expect("ensure fixture workspace");

        // Local row.
        let mut local = conversation("local", None, "local-session");
        local.messages = vec![
            message(0, &format!("local question {MARK_LOCAL}")),
            message(1, "local answer"),
        ];
        storage
            .insert_conversation_tree(agent_id, Some(workspace_id), &local)
            .expect("insert local conversation");

        // SSH-source row: source_id names the source, origin_host the box it
        // came from. `ensure_source_for_conversation` registers
        // `work-laptop` with kind = "ssh".
        let mut work = conversation(WORK_SOURCE_ID, Some(WORK_ORIGIN_HOST), "work-session");
        work.messages = vec![
            message(0, &format!("remote question {MARK_REMOTE}")),
            message(1, "remote answer one"),
            message(2, "remote answer two"),
        ];
        let outcome = storage
            .insert_conversation_tree(agent_id, Some(workspace_id), &work)
            .expect("insert work-laptop conversation");
        work_conv_id = outcome.conversation_id;

        // Legacy row: storage normalization rewrites `source_id = ""` into the
        // origin-host label, so the insert goes in as `legacy-remote-host`
        // and is then patched back to the true pre-normalization shape.
        let mut legacy = conversation("", Some(LEGACY_ORIGIN_HOST), "legacy-session");
        legacy.messages = vec![
            message(0, &format!("legacy question {MARK_LEGACY}")),
            message(1, "legacy answer"),
        ];
        let outcome = storage
            .insert_conversation_tree(agent_id, Some(workspace_id), &legacy)
            .expect("insert legacy conversation");
        legacy_conv_id = outcome.conversation_id;
    }

    // The storage layer normalizes `source_id = ""` into the origin-host
    // label and registers that label in `sources`; a legacy archive written
    // before that normalization (and before `REFERENCES sources(id)` was
    // enforced) keeps the true `''` value with nothing describing it. Patch
    // the row back through a raw connection with FK enforcement off.
    {
        let conn = FrankenConnection::open(db_path.to_string_lossy().into_owned())
            .expect("open fixture archive for legacy patch");
        conn.execute("PRAGMA foreign_keys = OFF")
            .expect("disable foreign key enforcement for legacy patch");
        conn.execute_compat(
            "UPDATE conversations SET source_id = '' WHERE id = ?1",
            params![legacy_conv_id],
        )
        .expect("restore legacy empty source_id");
        conn.execute_compat(
            "DELETE FROM sources WHERE id = ?1",
            params![LEGACY_ORIGIN_HOST],
        )
        .expect("drop auto-registered legacy source row");
        conn.close().expect("close legacy patch connection");
    }

    let fixture = Fixture {
        _tmp: tmp,
        home,
        config,
        xdg_data,
        data_dir,
        db_path,
    };

    // Fixture sanity: the DB really is in the state this test characterizes.
    let rows = snapshot_conversations(&fixture.db_path);
    let legacy_row = rows
        .iter()
        .find(|row| row.0 == legacy_conv_id)
        .expect("legacy row present");
    assert_eq!(legacy_row.1, "", "legacy row must keep an empty source_id");
    assert_eq!(
        legacy_row.2.as_deref(),
        Some(LEGACY_ORIGIN_HOST),
        "legacy row must keep its origin_host"
    );
    assert!(
        source_rows(&fixture.db_path)
            .iter()
            .all(|(id, _, _)| id != LEGACY_ORIGIN_HOST),
        "no sources-table entry may describe the legacy row"
    );

    (fixture, work_conv_id)
}

#[test]
fn full_index_preserves_rows_whose_remote_source_vanished() {
    let (fixture, work_conv_id) = build_fixture();
    let data_dir = fixture.data_dir.to_string_lossy().into_owned();
    let work_source_path = format!(
        "/remote/{}/.codex/sessions/rollout-{0}.jsonl",
        "work-session"
    );

    let before = snapshot_conversations(&fixture.db_path);
    assert_eq!(before.len(), 3, "fixture must hold three conversations");
    let sources_before = source_rows(&fixture.db_path);

    // Action: a full reindex with no sources.toml anywhere.
    fixture.cass_ok(&["index", "--full", "--data-dir", &data_dir]);

    // All rows on place, unchanged: id, source_id, origin_host, message count.
    let after = snapshot_conversations(&fixture.db_path);
    let sources_after = source_rows(&fixture.db_path);
    assert_eq!(
        after, before,
        "index --full must not drop or rewrite archived conversations"
    );
    assert_eq!(
        sources_after, sources_before,
        "index --full must not add, drop or rewrite sources-table rows"
    );

    // `work-laptop` must still be a registered ssh source in the DB (its
    // config entry is gone; the archive is what remembers it).
    let work = sources_after
        .iter()
        .find(|(id, _, _)| id == WORK_SOURCE_ID)
        .expect("work-laptop source row must survive");
    assert_eq!(work.1, "ssh");
    assert_eq!(work.2.as_deref(), Some(WORK_ORIGIN_HOST));

    // The lexical index must contain the remote records: a source-filtered
    // search finds the work-laptop session even though its origin is gone.
    let search = fixture.cass_ok(&[
        "search",
        MARK_REMOTE,
        "--source",
        WORK_SOURCE_ID,
        "--mode",
        "lexical",
        "--json",
        "--data-dir",
        &data_dir,
    ]);
    let payload: Value = serde_json::from_slice(&search.stdout).expect("search json");
    let hits = payload["hits"].as_array().expect("hits array");
    assert!(
        hits.iter()
            .any(|hit| hit["source_id"] == WORK_SOURCE_ID
                && hit["conversation_id"] == work_conv_id),
        "search --source work-laptop must return the archived remote session: {payload}"
    );

    // Unfiltered search finds all three archived conversations, including the
    // legacy row with the empty source_id.
    for marker in [MARK_LOCAL, MARK_REMOTE, MARK_LEGACY] {
        let all = fixture.cass_ok(&[
            "search",
            marker,
            "--mode",
            "lexical",
            "--json",
            "--data-dir",
            &data_dir,
        ]);
        let payload: Value = serde_json::from_slice(&all.stdout).expect("search json");
        let hits = payload["hits"].as_array().expect("hits array");
        assert!(
            !hits.is_empty(),
            "lexical index must contain the record carrying {marker}: {payload}"
        );
    }

    // Read-only archive surface on the vanished remote session.
    let conv_id = work_conv_id.to_string();
    fixture.cass_ok(&[
        "view",
        &work_source_path,
        "--source",
        WORK_SOURCE_ID,
        "--conversation-id",
        &conv_id,
        "--message-index",
        "1",
        "--json",
    ]);
    fixture.cass_ok(&[
        "expand",
        &work_source_path,
        "--source",
        WORK_SOURCE_ID,
        "--conversation-id",
        &conv_id,
        "--message-index",
        "1",
        "--json",
    ]);
    let export = fixture.cass_ok(&[
        "export",
        &work_source_path,
        "--source",
        WORK_SOURCE_ID,
        "--conversation-id",
        &conv_id,
    ]);
    assert!(
        String::from_utf8_lossy(&export.stdout).contains(MARK_REMOTE),
        "export must emit the archived remote message"
    );
    let html_dir = fixture.data_dir.join("html-out");
    fixture.cass_ok(&[
        "export-html",
        &work_source_path,
        "--source",
        WORK_SOURCE_ID,
        "--conversation-id",
        &conv_id,
        "--output-dir",
        &html_dir.to_string_lossy(),
        "--json",
    ]);
    assert!(
        fs::read_dir(&html_dir)
            .expect("export-html output dir")
            .count()
            > 0,
        "export-html must write a file for the archived remote session"
    );
    let stats = fixture.cass_ok(&[
        "stats",
        "--source",
        WORK_SOURCE_ID,
        "--json",
        "--data-dir",
        &data_dir,
    ]);
    let stats: Value = serde_json::from_slice(&stats.stdout).expect("stats json");
    assert_eq!(
        stats["conversations"], 1,
        "stats --source work-laptop must count the archived remote session: {stats}"
    );
}
