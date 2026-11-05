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
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use coding_agent_search::franken_sync::compat::{ConnectionExt, RowExt};
use coding_agent_search::franken_sync::{Connection as FrankenConnection, params};
use coding_agent_search::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use coding_agent_search::sources::provenance::path_to_safe_dirname;
use coding_agent_search::storage::sqlite::SqliteStorage;
use serde_json::Value;

mod util;
use util::cass_bin;

// Unique markers that cannot collide with tokenizer noise or other fixtures.
const MARK_LOCAL: &str = "zzlocalmarker7f3a";
const MARK_REMOTE: &str = "zzremotemarker9b2c";
const MARK_LEGACY: &str = "zzlegacymarker4d8e";
const MARK_MIRROR: &str = "zzmirrormarker5c1d";

const WORK_SOURCE_ID: &str = "work-laptop";
const WORK_ORIGIN_HOST: &str = "alice@work-laptop.example";
const LEGACY_ORIGIN_HOST: &str = "legacy-remote-host";

/// The configured remote path a pre-removal sync would have mirrored. The
/// mirror directory the indexer still scans lives at
/// `<data-dir>/remotes/<name>/mirror/<path_to_safe_dirname(path)>`.
const REMOTE_VIBE_PATH: &str = "~/.vibe/logs/session";

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
    fn cass_command(&self) -> Command {
        let mut cmd = Command::new(cass_bin());
        cmd.current_dir(&self.home)
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
            .env("CASS_AUTO_REFRESH", "0");
        cmd
    }

    fn cass(&self, args: &[&str]) -> Output {
        self.cass_command().args(args).output().expect("spawn cass")
    }

    /// Run cass with a piped stdin (for `--password-stdin` surfaces).
    fn cass_stdin(&self, args: &[&str], stdin_text: &str) -> Output {
        let mut child = self
            .cass_command()
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn cass");
        child
            .stdin
            .take()
            .expect("stdin pipe")
            .write_all(stdin_text.as_bytes())
            .expect("write cass stdin");
        child.wait_with_output().expect("cass output")
    }

    fn expect_ok(out: &Output, args: &[&str]) {
        assert!(
            out.status.success(),
            "cass {:?} failed: status={:?}\nstdout={}\nstderr={}",
            args,
            out.status.code(),
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }

    fn cass_ok(&self, args: &[&str]) -> Output {
        let out = self.cass(args);
        Self::expect_ok(&out, args);
        out
    }

    fn cass_stdin_ok(&self, args: &[&str], stdin_text: &str) -> Output {
        let out = self.cass_stdin(args, stdin_text);
        Self::expect_ok(&out, args);
        out
    }

    /// Lexical search hits for `query` (the archived-remote assertions all
    /// run against the same envelope).
    fn search_hits(&self, args: &[&str]) -> Vec<Value> {
        let out = self.cass_ok(args);
        let payload: Value = serde_json::from_slice(&out.stdout).expect("search json");
        payload["hits"].as_array().expect("hits array").clone()
    }

    fn search_marker(&self, marker: &str, data_dir: &str) -> Vec<Value> {
        self.search_hits(&[
            "search",
            marker,
            "--mode",
            "lexical",
            "--json",
            "--data-dir",
            data_dir,
        ])
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

/// A `sources.toml` still carrying a legacy `type = "ssh"` entry — the shape
/// an old config has on disk today.
fn write_ssh_source_config(config_dir: &Path, name: &str, host: &str, remote_path: &str) {
    let cass_dir = config_dir.join("cass");
    fs::create_dir_all(&cass_dir).expect("create cass config dir");
    fs::write(
        cass_dir.join("sources.toml"),
        format!(
            "[[sources]]\nname = {name:?}\ntype = \"ssh\"\nhost = {host:?}\npaths = [{remote_path:?}]\n"
        ),
    )
    .expect("write sources.toml");
}

/// A valid `sources.toml` whose remote entry was removed — the DB fallback in
/// `build_scan_roots_with_missing` is what this exercises.
fn write_empty_sources_config(config_dir: &Path) {
    let cass_dir = config_dir.join("cass");
    fs::create_dir_all(&cass_dir).expect("create cass config dir");
    fs::write(
        cass_dir.join("sources.toml"),
        "# all remote entries removed\n",
    )
    .expect("write emptied sources.toml");
}

/// Drop a transcript into the historical mirror layout
/// `<data-dir>/remotes/<source>/mirror/<safe-name>/<basename>/<session>/messages.jsonl`,
/// the way a pre-removal rsync/scp of `~/.vibe/logs/session` laid it out: the
/// transport container holds the remote path's basename (`session/`) and the
/// session store lives below it. The connector's `mirrored_store` resolver
/// only accepts that basename-preserving shape for a path without a trailing
/// slash.
fn write_vibe_mirror_file(data_dir: &Path, source_id: &str, remote_path: &str) -> PathBuf {
    let basename = Path::new(remote_path)
        .file_name()
        .expect("remote path must name a store basename");
    let file = data_dir
        .join("remotes")
        .join(source_id)
        .join("mirror")
        .join(path_to_safe_dirname(remote_path))
        .join(basename)
        .join("fixture-session")
        .join("messages.jsonl");
    fs::create_dir_all(file.parent().expect("mirror parent")).expect("create mirror directory");
    fs::write(
        &file,
        format!(
            "{{\"role\":\"user\",\"content\":\"mirror question {MARK_MIRROR}\",\"timestamp\":1700000000000}}\n\
             {{\"role\":\"assistant\",\"content\":\"mirror answer\",\"timestamp\":1700000001000}}\n"
        ),
    )
    .expect("write mirror transcript");
    file
}

/// A legacy `type = "ssh"` config entry whose mirror directory exists keeps
/// producing a scan root: the full index ingests the archived mirror under
/// the configured source, and later losing the mirror — or losing the config
/// entry itself — never prunes the rows already archived.
#[test]
fn configured_mirror_indexes_then_survives_mirror_and_config_loss() {
    let (fixture, _work_conv_id) = build_fixture();
    let data_dir = fixture.data_dir.to_string_lossy().into_owned();
    write_ssh_source_config(
        &fixture.config,
        WORK_SOURCE_ID,
        WORK_ORIGIN_HOST,
        REMOTE_VIBE_PATH,
    );
    write_vibe_mirror_file(&fixture.data_dir, WORK_SOURCE_ID, REMOTE_VIBE_PATH);

    // Phase 1 — mirror present: `index --full` must pick the mirrored
    // transcript up under the `work-laptop` source.
    fixture.cass_ok(&["index", "--full", "--data-dir", &data_dir]);
    let with_mirror = snapshot_conversations(&fixture.db_path);
    assert_eq!(
        with_mirror.len(),
        4,
        "configured mirror transcript must be indexed: {with_mirror:?}"
    );
    let mirror_rows: Vec<_> = with_mirror
        .iter()
        .filter(|row| row.1 == WORK_SOURCE_ID)
        .collect();
    assert_eq!(
        mirror_rows.len(),
        2,
        "work-laptop must hold the fixture row plus the mirrored transcript: {with_mirror:?}"
    );
    let hits = fixture.search_marker(MARK_MIRROR, &data_dir);
    assert!(
        hits.iter().any(|hit| hit["source_id"] == WORK_SOURCE_ID),
        "mirror transcript must be searchable under work-laptop: {hits:?}"
    );

    // Phase 2 — the config entry is removed (empty sources.toml) while the
    // mirror still exists: the DB fallback builds roots from the stored
    // `sources` row, and nothing may be pruned.
    write_empty_sources_config(&fixture.config);
    fixture.cass_ok(&["index", "--full", "--data-dir", &data_dir]);
    assert_eq!(
        snapshot_conversations(&fixture.db_path),
        with_mirror,
        "removing the remote entry from sources.toml must not prune archived rows"
    );
    assert!(
        !fixture.search_marker(MARK_MIRROR, &data_dir).is_empty(),
        "mirror transcript must stay searchable with the config entry gone"
    );

    // Phase 3 — the mirror directory is deleted by hand: the vanished scan
    // root is not grounds for deleting anything.
    fs::remove_dir_all(fixture.data_dir.join("remotes").join(WORK_SOURCE_ID))
        .expect("delete mirror directory");
    fixture.cass_ok(&["index", "--full", "--data-dir", &data_dir]);
    assert_eq!(
        snapshot_conversations(&fixture.db_path),
        with_mirror,
        "a manually deleted mirror directory must not prune archived rows"
    );
    let sources_after = source_rows(&fixture.db_path);
    assert_eq!(
        sources_after
            .iter()
            .filter(|(id, _, _)| id == WORK_SOURCE_ID)
            .count(),
        1,
        "the sources-table row for work-laptop must survive the mirror loss: {sources_after:?}"
    );

    // Phase 4 — `--force-rebuild` on the populated archive is a
    // canonical-only lexical rebuild: same rows, remote provenance kept.
    fixture.cass_ok(&["index", "--force-rebuild", "--data-dir", &data_dir]);
    assert_eq!(
        snapshot_conversations(&fixture.db_path),
        with_mirror,
        "index --force-rebuild must leave the archived rows identical"
    );
    for marker in [MARK_REMOTE, MARK_MIRROR] {
        let hits = fixture.search_marker(marker, &data_dir);
        assert!(
            hits.iter().any(|hit| hit["origin_kind"] == "remote"),
            "{marker} hits must keep remote provenance after --force-rebuild: {hits:?}"
        );
    }
}

/// A conversation whose remote `source_id` has no row in `sources` at all —
/// the hardest archived shape — must survive `--force-rebuild` and keep its
/// remote `origin_kind` in the rebuilt lexical index.
#[test]
fn force_rebuild_preserves_remote_rows_without_sources_table_entry() {
    let (fixture, work_conv_id) = build_fixture();
    let data_dir = fixture.data_dir.to_string_lossy().into_owned();

    // Remove the auto-registered `work-laptop` row from `sources` so its
    // conversation is a remote record nothing describes.
    {
        let conn = FrankenConnection::open(fixture.db_path.to_string_lossy().into_owned())
            .expect("open archive for sources patch");
        conn.execute("PRAGMA foreign_keys = OFF")
            .expect("disable foreign key enforcement for sources patch");
        conn.execute_compat("DELETE FROM sources WHERE id = ?1", params![WORK_SOURCE_ID])
            .expect("drop work-laptop sources row");
        conn.close().expect("close sources patch connection");
    }
    assert!(
        source_rows(&fixture.db_path)
            .iter()
            .all(|(id, _, _)| id != WORK_SOURCE_ID),
        "work-laptop must have no sources-table entry for this scenario"
    );

    let before = snapshot_conversations(&fixture.db_path);

    // `--full` and `--force-rebuild` must report identical conversation sets.
    fixture.cass_ok(&["index", "--full", "--data-dir", &data_dir]);
    let after_full = snapshot_conversations(&fixture.db_path);
    assert_eq!(
        after_full, before,
        "index --full must not drop the source-less remote row"
    );
    fixture.cass_ok(&["index", "--force-rebuild", "--data-dir", &data_dir]);
    assert_eq!(
        snapshot_conversations(&fixture.db_path),
        after_full,
        "index --force-rebuild must produce the same conversation set as --full"
    );

    // The rebuilt lexical doc for a remote row with no `sources` entry still
    // carries remote provenance: origin_kind `remote`, the stored source_id,
    // and the recorded origin host.
    let hits = fixture.search_hits(&[
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
    assert!(
        hits.iter().any(|hit| {
            hit["conversation_id"] == work_conv_id
                && hit["source_id"] == WORK_SOURCE_ID
                && hit["origin_kind"] == "remote"
                && hit["origin_host"] == WORK_ORIGIN_HOST
        }),
        "work-laptop hit must keep remote provenance after --force-rebuild: {hits:?}"
    );

    // The legacy pre-normalization row (empty source_id, origin_host only)
    // resolves to the remote host identity too.
    let legacy_hits = fixture.search_marker(MARK_LEGACY, &data_dir);
    assert!(
        legacy_hits.iter().any(|hit| hit["origin_kind"] == "remote"),
        "legacy remote hit must keep remote origin_kind: {legacy_hits:?}"
    );
}

/// `cass forget` is the explicit deletion path: once a remote record is
/// forgotten it must stay hidden through `index --full` — the tombstone, not
/// the vanished source file, decides admission.
#[test]
fn forget_on_remote_record_stays_hidden_after_full_index() {
    let (fixture, _work_conv_id) = build_fixture();
    let data_dir = fixture.data_dir.to_string_lossy().into_owned();
    let db = fixture.db_path.to_string_lossy().into_owned();

    // Preview, then apply: only the work-laptop row may be selected.
    let preview = fixture.cass_ok(&[
        "forget",
        "--source-glob",
        "/remote/work-session/**",
        "--json",
        "--db",
        &db,
    ]);
    let preview: Value = serde_json::from_slice(&preview.stdout).expect("forget preview json");
    assert_eq!(
        preview["conversations_matched"], 1,
        "forget preview must select exactly the work-laptop session: {preview}"
    );
    let applied = fixture.cass_ok(&[
        "forget",
        "--source-glob",
        "/remote/work-session/**",
        "--apply",
        "--json",
        "--db",
        &db,
    ]);
    let applied: Value = serde_json::from_slice(&applied.stdout).expect("forget apply json");
    assert_eq!(
        applied["conversations_deleted"], 1,
        "forget --apply must delete the selected remote conversation: {applied}"
    );

    let after_forget = snapshot_conversations(&fixture.db_path);
    assert_eq!(after_forget.len(), 2, "the remote row is tombstoned away");

    // A full reindex must not resurrect the tombstoned row.
    fixture.cass_ok(&["index", "--full", "--data-dir", &data_dir]);
    assert_eq!(
        snapshot_conversations(&fixture.db_path),
        after_forget,
        "index --full must not resurrect a forgotten remote conversation"
    );
    assert!(
        fixture.search_marker(MARK_REMOTE, &data_dir).is_empty(),
        "forgotten remote session must stay out of search after index --full"
    );
    for marker in [MARK_LOCAL, MARK_LEGACY] {
        assert!(
            !fixture.search_marker(marker, &data_dir).is_empty(),
            "unrelated archived rows must stay searchable: {marker}"
        );
    }
}

/// The same fixture exercises the read surface: every command that answers
/// from the archive must keep serving the vanished remote's rows — including
/// `models backfill`, which must embed remote messages like any other.
#[test]
fn archived_remote_rows_survive_across_read_surfaces() {
    let (fixture, work_conv_id) = build_fixture();
    let data_dir = fixture.data_dir.to_string_lossy().into_owned();
    let work_source_path = "/remote/work-session/.codex/sessions/rollout-work-session.jsonl";
    let conv_id = work_conv_id.to_string();

    fixture.cass_ok(&["index", "--full", "--data-dir", &data_dir]);

    // `view` resolves the archived remote session by its identity key
    // (source path + source_id + conversation id) — the source file itself
    // is never required to exist.
    let view = fixture.cass_ok(&[
        "view",
        work_source_path,
        "--source",
        WORK_SOURCE_ID,
        "--conversation-id",
        &conv_id,
        "--json",
    ]);
    let view: Value = serde_json::from_slice(&view.stdout).expect("view json");
    assert_eq!(
        view["archive_only"], true,
        "view must serve the archived remote session without its source file: {view}"
    );
    let view_text = view.to_string();
    assert!(
        view_text.contains(MARK_REMOTE),
        "view must render the archived remote messages: {view}"
    );

    // `expand` returns the neighborhood of a canonical message index.
    let expand = fixture.cass_ok(&[
        "expand",
        work_source_path,
        "--source",
        WORK_SOURCE_ID,
        "--message-index",
        "1",
        "--conversation-id",
        &conv_id,
        "--json",
    ]);
    let expand: Value = serde_json::from_slice(&expand.stdout).expect("expand json");
    let expand_text = expand.to_string();
    assert!(
        expand_text.contains(MARK_REMOTE),
        "expand must read the archived remote messages: {expand}"
    );

    // `export` prints the archived remote session as markdown on stdout.
    let export = fixture.cass_ok(&[
        "export",
        work_source_path,
        "--source",
        WORK_SOURCE_ID,
        "--conversation-id",
        &conv_id,
    ]);
    let export_text = String::from_utf8_lossy(&export.stdout);
    assert!(
        export_text.contains(MARK_REMOTE),
        "export must carry the archived remote session text: {export_text}"
    );

    // Hybrid search degrades to the lexical hits when no model is installed;
    // the remote row must be returned either way.
    let hybrid = fixture.search_hits(&[
        "search",
        MARK_REMOTE,
        "--mode",
        "hybrid",
        "--json",
        "--data-dir",
        &data_dir,
    ]);
    assert!(
        hybrid
            .iter()
            .any(|hit| hit["conversation_id"] == work_conv_id),
        "hybrid search must surface the archived remote session: {hybrid:?}"
    );

    // Encrypted HTML export of the archived remote session.
    let html_dir = fixture.data_dir.join("html-encrypted");
    let encrypted = fixture.cass_stdin_ok(
        &[
            "export-html",
            work_source_path,
            "--source",
            WORK_SOURCE_ID,
            "--conversation-id",
            &conv_id,
            "--output-dir",
            &html_dir.to_string_lossy(),
            "--encrypt",
            "--password-stdin",
            "--json",
        ],
        "archive-preservation passphrase\n",
    );
    let receipt: Value = serde_json::from_slice(&encrypted.stdout).expect("export-html json");
    assert_eq!(
        receipt["exported"]["encrypted"], true,
        "export-html --encrypt must emit an encrypted file: {receipt}"
    );

    // Pages static export of the whole archive: `--export-only` writes a
    // single staged database file, not an output directory.
    let pages_export = fixture.data_dir.join("pages-export.db");
    fixture.cass_ok(&[
        "pages",
        "--export-only",
        &pages_export.to_string_lossy(),
        "--json",
    ]);
    let pages_meta = fs::metadata(&pages_export).expect("pages export file");
    assert!(
        pages_meta.is_file() && pages_meta.len() > 0,
        "pages --export-only must write a non-empty database for the archive"
    );

    // Timeline over the fixture window counts the remote session.
    let timeline = fixture.cass_ok(&[
        "timeline",
        "--since",
        "2023-11-01",
        "--source",
        "remote",
        "--json",
        "--data-dir",
        &data_dir,
    ]);
    let timeline: Value = serde_json::from_slice(&timeline.stdout).expect("timeline json");
    let total = timeline["total_sessions"]
        .as_u64()
        .unwrap_or_else(|| panic!("timeline must report total_sessions: {timeline}"));
    assert_eq!(
        total, 2,
        "timeline --source remote must count both remote sessions: {timeline}"
    );

    // Analytics rollups tolerate remote rows (they may be empty of token
    // data, but the command must answer).
    fixture.cass_ok(&[
        "analytics",
        "tokens",
        "--source",
        "remote",
        "--json",
        "--data-dir",
        &data_dir,
    ]);

    // `models backfill` with the hash embedder must place remote messages
    // into the semantic index — no model download needed.
    fixture.cass_ok(&[
        "models",
        "backfill",
        "--tier",
        "fast",
        "--embedder",
        "hash",
        "--batch-conversations",
        "8",
        "--data-dir",
        &data_dir,
        "--json",
    ]);
    let semantic = fixture.search_hits(&[
        "search",
        MARK_REMOTE,
        "--mode",
        "semantic",
        "--model",
        "hash",
        "--json",
        "--data-dir",
        &data_dir,
    ]);
    assert!(
        semantic
            .iter()
            .any(|hit| hit["conversation_id"] == work_conv_id),
        "semantic search must surface the archived remote session after hash backfill: {semantic:?}"
    );

    // After all of that, the canonical rows are still exactly what the
    // fixture wrote.
    assert_eq!(
        snapshot_conversations(&fixture.db_path).len(),
        3,
        "read surfaces must not mutate the canonical archive"
    );
}
