//! Index-first export contract tests.
//!
//! Pins the NEW `cass export` / `cass export-html` contract from
//! `docs/superpowers/plans/2026-11-02-index-first-export.md`:
//!
//!   * The canonical indexed conversation is the **sole** source of truth.
//!     The supplied source path is an opaque identity key — never opened,
//!     sniffed, decompressed, or re-parsed at export time.
//!   * Missing/renamed/changed source files must not change exported bytes.
//!   * Failure taxonomy (proposed names): `IndexUnavailable`,
//!     `ConversationNotIndexed`, `AmbiguousIdentity`, `IndexReadFailure`,
//!     `NoRenderableMessages`. **No fallback to source files on any error.**
//!   * `--include-tools` gates *indexed* tool payloads; missing index-time
//!     tool detail is reported, never fabricated from source files.
//!
//! Baseline (pre-refactor) behaviour in `src/lib.rs`:
//!   * `prefers_direct_jsonl_file` / `should_defer_indexed_lookup_for_direct_export`
//!     prefer a live `.jsonl` file over the archive whenever it exists for a
//!     local source, so scenarios "changed file" and "source exists" are RED.
//!   * `detect_opencode_sqlite_session` treats ANY `<dir>/*.db/<id>` path as an
//!     OpenCode session, so a Devin `sessions.db/<id>` identity collides and
//!     export routes into the OpenCode reader instead of the index (RED).
//!   * `load_conversation*` silently picks the newest row on ambiguity
//!     (`ORDER BY started_at DESC LIMIT 1`), so the ambiguity tests are RED.
//!
//! Seeding uses `FrankenStorage::open` + `insert_conversation_tree` (same
//! approach as the GH#519 CLI tests in `tests/cli_dispatch_coverage.rs`), so
//! the archive is a real canonical DB the `cass` subprocess opens read-only.
//!
//! Do not weaken these tests to match legacy file-first behaviour: Wave 2
//! owns flipping the implementation; a RED row here is the intended signal.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::time::Duration;

use assert_cmd::Command;
use coding_agent_search::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use coding_agent_search::sources::provenance::{Source, SourceKind};
use coding_agent_search::storage::sqlite::FrankenStorage;
use serde_json::{Value, json};
use tempfile::TempDir;

const CMD_TIMEOUT: Duration = Duration::from_secs(60);

// -------------------------------------------------------------------------
// Command + environment helpers
// -------------------------------------------------------------------------

/// Isolated-environment `cass` command. `CASS_OUTPUT_FORMAT=json` makes the
/// process emit machine-checkable `{"error": {code, kind, ...}}` envelopes on
/// stderr for every non-robot-native failure without touching stdout payloads.
fn base_cmd(home: &Path) -> Command {
    let mut cmd = Command::cargo_bin("cass").expect("cass binary");
    cmd.env("CODING_AGENT_SEARCH_NO_UPDATE_PROMPT", "1")
        .env("NO_COLOR", "1")
        .env("HOME", home)
        .env("USERPROFILE", home)
        .env("XDG_DATA_HOME", home.join(".local/share"))
        .env("XDG_CONFIG_HOME", home.join(".config"))
        .env(
            "CASS_DATA_DIR",
            home.join(".local/share/coding-agent-search"),
        )
        .env("LOCALAPPDATA", home.join("AppData/Local"))
        .env("APPDATA", home.join("AppData/Roaming"))
        .env("CODEX_HOME", home.join(".codex"))
        .env("CASS_AUTO_REFRESH", "0")
        .env("CASS_OUTPUT_FORMAT", "json")
        // Keep any stray OpenCode store discovery out of the sandbox.
        .env("OPENCODE_STORAGE_ROOT", home.join("empty-opencode-storage"))
        .timeout(CMD_TIMEOUT);
    cmd
}

/// `cass export <session> --db <archive> <extra...>`.
fn run_export(home: &Path, archive: &Path, session: &Path, extra: &[&str]) -> Output {
    let mut cmd = base_cmd(home);
    cmd.arg("export")
        .arg(session)
        .arg("--db")
        .arg(archive)
        .args(extra);
    cmd.output().expect("spawn cass export")
}

/// `cass export-html <session> --output-dir <dir> --filename <name> --json --db <archive>`.
fn run_export_html(
    home: &Path,
    archive: &Path,
    session: &Path,
    output_dir: &Path,
    filename: &str,
    extra: &[&str],
) -> Output {
    let mut cmd = base_cmd(home);
    cmd.arg("export-html")
        .arg(session)
        .arg("--output-dir")
        .arg(output_dir)
        .arg("--filename")
        .arg(filename)
        .arg("--json")
        .arg("--db")
        .arg(archive)
        .args(extra);
    cmd.output().expect("spawn cass export-html")
}

fn stdout_string(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr_string(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// Extract the `{"error": {...}}` envelope cass prints on stderr in robot mode
/// (or `{"success": false, "error": {...}}` on stdout for `export-html --json`).
fn error_kind(output: &Output) -> Option<String> {
    let find_envelope = |stream: &str| -> Option<String> {
        stream.lines().rev().find_map(|line| {
            let line = line.trim();
            if !line.starts_with('{') {
                return None;
            }
            let value: Value = serde_json::from_str(line).ok()?;
            value
                .get("error")
                .and_then(|err| err.get("kind"))
                .and_then(Value::as_str)
                .map(str::to_string)
        })
    };
    find_envelope(&stderr_string(output)).or_else(|| find_envelope(&stdout_string(output)))
}

// -------------------------------------------------------------------------
// Archive seeding
// -------------------------------------------------------------------------

fn agent(slug: &str) -> Agent {
    Agent {
        id: None,
        slug: slug.to_string(),
        name: slug.to_string(),
        version: None,
        kind: AgentKind::Cli,
    }
}

fn message(idx: i64, role: MessageRole, created_at: i64, content: &str) -> Message {
    Message {
        id: None,
        idx,
        role,
        author: None,
        created_at: Some(created_at),
        content: content.to_string(),
        extra_json: json!({}),
        snippets: Vec::new(),
    }
}

fn conversation(
    agent_slug: &str,
    source_path: &Path,
    external_id: &str,
    title: &str,
    started_at: i64,
    messages: Vec<Message>,
) -> Conversation {
    Conversation {
        id: None,
        agent_slug: agent_slug.to_string(),
        workspace: None,
        external_id: Some(external_id.to_string()),
        title: Some(title.to_string()),
        source_path: source_path.to_path_buf(),
        started_at: Some(started_at),
        ended_at: messages.last().and_then(|msg| msg.created_at),
        approx_tokens: None,
        metadata_json: json!({}),
        messages,
        source_id: "local".to_string(),
        origin_host: None,
    }
}

/// Seed a canonical archive: schema + agents + source rows + conversation
/// trees, then close so the WAL is checkpointed before the `cass` subprocess
/// opens the file read-only. Returns the inserted conversation row ids.
fn seed_archive(archive: &Path, convos: &[Conversation]) -> Vec<i64> {
    let storage = FrankenStorage::open(archive).expect("open seed archive");
    let mut agent_ids = std::collections::HashMap::<String, i64>::new();
    let mut ensured_remote = std::collections::HashSet::<String>::new();
    let mut ids = Vec::with_capacity(convos.len());
    for conv in convos {
        let agent_id = *agent_ids.entry(conv.agent_slug.clone()).or_insert_with(|| {
            storage
                .ensure_agent(&agent(&conv.agent_slug))
                .expect("ensure agent")
        });
        if conv.source_id != "local" && ensured_remote.insert(conv.source_id.clone()) {
            let host = conv.origin_host.as_deref().unwrap_or(&conv.source_id);
            storage
                .upsert_source(&Source {
                    id: conv.source_id.clone(),
                    kind: SourceKind::Ssh,
                    host_label: Some(host.to_string()),
                    machine_id: None,
                    platform: None,
                    config_json: None,
                    created_at: None,
                    updated_at: None,
                })
                .expect("upsert remote source");
        }
        let outcome = storage
            .insert_conversation_tree(agent_id, None, conv)
            .expect("insert conversation tree");
        ids.push(outcome.conversation_id);
    }
    storage.close().expect("close seed archive");
    ids
}

/// Minimal user+assistant indexed dialogue with distinctive markers.
fn indexed_dialogue(idx_base: i64, user_marker: &str, assistant_marker: &str) -> Vec<Message> {
    vec![
        message(idx_base, MessageRole::User, 1_700_000_000_000, user_marker),
        message(
            idx_base + 1,
            MessageRole::Agent,
            1_700_000_001_000,
            assistant_marker,
        ),
    ]
}

fn write_jsonl(path: &Path, lines: &[(&str, &str)]) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create session parent dir");
    }
    let mut body = String::new();
    for (role, content) in lines {
        body.push_str(&json!({"role": role, "content": content}).to_string());
        body.push('\n');
    }
    fs::write(path, body).expect("write jsonl session");
}

/// A real SQLite file that is deliberately NOT an OpenCode store (Devin's
/// `sessions.db`/`message_nodes` layout). Any `<dir>/*.db/<id>` path below it
/// triggers the legacy `detect_opencode_sqlite_session` collision.
fn write_devin_sessions_db(path: &Path) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create devin db parent");
    }
    // rusqlite is a dev-dependency sanctioned for interop fixture building.
    let conn = rusqlite::Connection::open(path).expect("open devin sessions.db");
    conn.execute_batch(
        "CREATE TABLE devin_sessions (
             session_id TEXT PRIMARY KEY,
             title TEXT,
             created_at INTEGER
         );
         CREATE TABLE message_nodes (
             node_id TEXT PRIMARY KEY,
             session_id TEXT NOT NULL,
             role TEXT NOT NULL,
             body TEXT NOT NULL,
             created_at INTEGER
         );
         INSERT INTO devin_sessions VALUES
             ('sess-devin-001', 'Devin store fixture', 1700000000000);
         INSERT INTO message_nodes VALUES
             ('n1', 'sess-devin-001', 'user',
              'FILE-ONLY-UTTERANCE that must never appear in export',
              1700000000000);",
    )
    .expect("populate devin sessions.db");
}

// -------------------------------------------------------------------------
// Assertion helpers
// -------------------------------------------------------------------------

/// The new contract requires an explicit, index-flavoured failure: non-zero
/// exit and a diagnostic that names the index (not silent file fallback, not
/// a provider parse error). None of the fixture paths this suite creates
/// contain the substring "index" (`cass-archive.db`, `no-such-archive.db`,
/// `live-session.jsonl`, `sessions.db/...`), so a bare `contains("index")`
/// on the combined diagnostic can only match an index-taxonomy `kind` or
/// message — never the echoed archive/session filename.
fn assert_index_failure(output: &Output, label: &str) {
    let stdout = stdout_string(output);
    let stderr = stderr_string(output);
    assert!(
        !output.status.success(),
        "{label}: export must fail explicitly under the index-first contract\nstdout: {stdout}\nstderr: {stderr}"
    );
    let diagnostic = format!("{stderr}\n{stdout}").to_lowercase();
    assert!(
        diagnostic.contains("index"),
        "{label}: failure must name the index (kind or message)\nstdout: {stdout}\nstderr: {stderr}"
    );
}

fn assert_stdout_has_indexed_only(
    output: &Output,
    indexed: &[&str],
    file_only: &[&str],
    label: &str,
) {
    let stdout = stdout_string(output);
    assert!(
        output.status.success(),
        "{label}: export from the indexed snapshot must succeed\nstdout: {stdout}\nstderr: {}",
        stderr_string(output)
    );
    for marker in indexed {
        assert!(
            stdout.contains(marker),
            "{label}: stdout must contain indexed content {marker:?}\nstdout: {stdout}"
        );
    }
    for marker in file_only {
        assert!(
            !stdout.contains(marker),
            "{label}: stdout must not contain live file content {marker:?}\nstdout: {stdout}"
        );
    }
}

// =========================================================================
// 1. Source file CONTENT CHANGED after indexing -> indexed snapshot wins.
// =========================================================================

const CHANGED_USER: &str = "CHANGED-FILE-UTTERANCE-OMEGA";
const CHANGED_ASSISTANT: &str = "CHANGED-FILE-REPLY-PSI";
const INDEXED_USER: &str = "INDEXED-SNAPSHOT-UTTERANCE-ALPHA";
const INDEXED_ASSISTANT: &str = "INDEXED-SNAPSHOT-REPLY-BETA";

/// Fixture shared by the changed-source tests: archive holds the indexed
/// snapshot; the on-disk `.jsonl` carries different (newer) bytes.
fn changed_source_fixture(tmp: &TempDir) -> (PathBuf, PathBuf) {
    let archive = tmp.path().join("cass-archive.db");
    let session = tmp.path().join("sessions").join("changed-session.jsonl");

    let conv = conversation(
        "claude_code",
        &session,
        "changed-ext-1",
        "Indexed Snapshot Session",
        1_700_000_000_000,
        indexed_dialogue(0, INDEXED_USER, INDEXED_ASSISTANT),
    );
    seed_archive(&archive, &[conv]);

    write_jsonl(
        &session,
        &[("user", CHANGED_USER), ("assistant", CHANGED_ASSISTANT)],
    );
    (archive, session)
}

#[test]
fn changed_source_markdown_uses_indexed_snapshot() {
    let tmp = TempDir::new().unwrap();
    let (archive, session) = changed_source_fixture(&tmp);

    let out = run_export(tmp.path(), &archive, &session, &["--format", "markdown"]);
    assert_stdout_has_indexed_only(
        &out,
        &[INDEXED_USER, INDEXED_ASSISTANT],
        &[CHANGED_USER, CHANGED_ASSISTANT],
        "markdown export",
    );
}

#[test]
fn changed_source_json_uses_indexed_snapshot() {
    let tmp = TempDir::new().unwrap();
    let (archive, session) = changed_source_fixture(&tmp);

    let out = run_export(tmp.path(), &archive, &session, &["--format", "json"]);
    assert!(
        out.status.success(),
        "json export must succeed\nstderr: {}",
        stderr_string(&out)
    );
    let stdout = stdout_string(&out);
    let messages: Vec<Value> =
        serde_json::from_str(stdout.trim()).expect("export --format json is a message array");
    let contents: Vec<&str> = messages
        .iter()
        .filter_map(|m| m.get("content").and_then(Value::as_str))
        .collect();
    assert_eq!(
        contents,
        vec![INDEXED_USER, INDEXED_ASSISTANT],
        "json export must emit exactly the indexed snapshot, in indexed order"
    );
    let roles: Vec<&str> = messages
        .iter()
        .filter_map(|m| m.get("role").and_then(Value::as_str))
        .collect();
    assert_eq!(roles, vec!["user", "assistant"]);
}

#[test]
fn changed_source_html_uses_indexed_snapshot() {
    let tmp = TempDir::new().unwrap();
    let (archive, session) = changed_source_fixture(&tmp);

    let out = run_export(tmp.path(), &archive, &session, &["--format", "html"]);
    assert_stdout_has_indexed_only(
        &out,
        &[INDEXED_USER, INDEXED_ASSISTANT],
        &[CHANGED_USER, CHANGED_ASSISTANT],
        "html export",
    );
}

#[test]
fn changed_source_export_html_uses_indexed_snapshot() {
    let tmp = TempDir::new().unwrap();
    let (archive, session) = changed_source_fixture(&tmp);
    let out_dir = tmp.path().join("html-out");

    let out = run_export_html(tmp.path(), &archive, &session, &out_dir, "export.html", &[]);
    assert!(
        out.status.success(),
        "export-html must succeed\nstdout: {}\nstderr: {}",
        stdout_string(&out),
        stderr_string(&out)
    );
    let html = fs::read_to_string(out_dir.join("export.html")).expect("read exported html");
    for marker in [INDEXED_USER, INDEXED_ASSISTANT] {
        assert!(
            html.contains(marker),
            "export-html must contain indexed content {marker:?}"
        );
    }
    for marker in [CHANGED_USER, CHANGED_ASSISTANT] {
        assert!(
            !html.contains(marker),
            "export-html must not contain live file content {marker:?}"
        );
    }
}

// =========================================================================
// 2. Source file DELETED/missing -> export still succeeds from the index.
//    (Guard: this path is already index-backed at baseline.)
// =========================================================================

#[test]
fn missing_source_markdown_uses_indexed_snapshot() {
    let tmp = TempDir::new().unwrap();
    let archive = tmp.path().join("cass-archive.db");
    // Never created: parent dir "ghost" does not exist at all.
    let session = tmp.path().join("ghost").join("missing-session.jsonl");
    let conv = conversation(
        "claude_code",
        &session,
        "ghost-ext-1",
        "Indexed Ghost Title",
        1_700_000_000_000,
        indexed_dialogue(0, INDEXED_USER, INDEXED_ASSISTANT),
    );
    seed_archive(&archive, &[conv]);

    let out = run_export(tmp.path(), &archive, &session, &["--format", "markdown"]);
    let stdout = stdout_string(&out);
    assert!(
        out.status.success(),
        "missing source file must not break indexed export\nstdout: {stdout}\nstderr: {}",
        stderr_string(&out)
    );
    for marker in [INDEXED_USER, INDEXED_ASSISTANT] {
        assert!(stdout.contains(marker), "missing indexed marker {marker:?}");
    }
    let user_pos = stdout.find(INDEXED_USER).unwrap();
    let assistant_pos = stdout.find(INDEXED_ASSISTANT).unwrap();
    assert!(user_pos < assistant_pos, "indexed message order preserved");
    assert!(stdout.contains("Indexed Ghost Title"), "indexed title used");
}

#[test]
fn missing_source_text_uses_indexed_snapshot() {
    let tmp = TempDir::new().unwrap();
    let archive = tmp.path().join("cass-archive.db");
    let session = tmp.path().join("ghost").join("missing-session.jsonl");
    let conv = conversation(
        "codex",
        &session,
        "ghost-text-1",
        "Indexed Ghost Text",
        1_700_000_000_000,
        indexed_dialogue(0, INDEXED_USER, INDEXED_ASSISTANT),
    );
    seed_archive(&archive, &[conv]);

    let out = run_export(tmp.path(), &archive, &session, &["--format", "text"]);
    let stdout = stdout_string(&out);
    assert!(
        out.status.success(),
        "text export must succeed from index\nstderr: {}",
        stderr_string(&out)
    );
    assert!(stdout.contains("=== USER ==="), "text user header");
    assert!(
        stdout.contains("=== ASSISTANT ==="),
        "text assistant header"
    );
    assert!(stdout.contains(INDEXED_USER));
    assert!(stdout.contains(INDEXED_ASSISTANT));
}

#[test]
fn missing_source_export_html_uses_indexed_snapshot() {
    let tmp = TempDir::new().unwrap();
    let archive = tmp.path().join("cass-archive.db");
    let session = tmp.path().join("ghost").join("missing-session.jsonl");
    let conv = conversation(
        "claude_code",
        &session,
        "ghost-html-1",
        "Indexed Ghost Html",
        1_700_000_000_000,
        indexed_dialogue(0, INDEXED_USER, INDEXED_ASSISTANT),
    );
    seed_archive(&archive, &[conv]);

    let out_dir = tmp.path().join("html-out");
    let out = run_export_html(tmp.path(), &archive, &session, &out_dir, "export.html", &[]);
    assert!(
        out.status.success(),
        "export-html must succeed for a missing source file\nstdout: {}\nstderr: {}",
        stdout_string(&out),
        stderr_string(&out)
    );
    let html = fs::read_to_string(out_dir.join("export.html")).expect("read exported html");
    assert!(html.contains(INDEXED_USER));
    assert!(html.contains(INDEXED_ASSISTANT));
}

// =========================================================================
// 3. `--db` points at a NONEXISTENT archive + live file -> explicit
//    IndexUnavailable-style failure; never create the archive, never fall
//    back to the file.
// =========================================================================

#[test]
fn nonexistent_archive_fails_instead_of_file_fallback() {
    let tmp = TempDir::new().unwrap();
    let missing_archive = tmp.path().join("no-such-archive.db");
    let session = tmp.path().join("sessions").join("live-session.jsonl");
    write_jsonl(
        &session,
        &[("user", CHANGED_USER), ("assistant", CHANGED_ASSISTANT)],
    );

    let out = run_export(
        tmp.path(),
        &missing_archive,
        &session,
        &["--format", "markdown"],
    );
    assert_index_failure(&out, "missing archive");
    let stdout = stdout_string(&out);
    assert!(
        !stdout.contains(CHANGED_USER),
        "no file-fallback export may be emitted when the index is unavailable\nstdout: {stdout}"
    );
    assert!(
        !missing_archive.exists(),
        "export must not create/repair the archive it was asked to read"
    );
}

// =========================================================================
// 4. Source file exists & parses, but NO indexed conversation ->
//    ConversationNotIndexed-style failure; never fall back to the file.
// =========================================================================

#[test]
fn unindexed_session_fails_instead_of_file_fallback() {
    let tmp = TempDir::new().unwrap();
    let archive = tmp.path().join("cass-archive.db");
    // A valid, non-empty archive whose conversations point elsewhere.
    let unrelated = conversation(
        "claude_code",
        &tmp.path().join("other").join("unrelated.jsonl"),
        "unrelated-ext-1",
        "Unrelated Conversation",
        1_700_000_000_000,
        indexed_dialogue(0, "UNRELATED-USER-MARKER", "UNRELATED-REPLY-MARKER"),
    );
    seed_archive(&archive, &[unrelated]);

    let session = tmp.path().join("sessions").join("live-session.jsonl");
    write_jsonl(
        &session,
        &[("user", CHANGED_USER), ("assistant", CHANGED_ASSISTANT)],
    );

    let out = run_export(tmp.path(), &archive, &session, &["--format", "markdown"]);
    assert_index_failure(&out, "conversation not indexed");
    let stdout = stdout_string(&out);
    assert!(
        !stdout.contains(CHANGED_USER),
        "live file content must never be emitted when the conversation is not indexed\nstdout: {stdout}"
    );
}

// =========================================================================
// 5. TWO indexed conversations share one source_path -> explicit
//    AmbiguousIdentity failure naming the candidates; never silently pick
//    the newest row.
// =========================================================================

#[test]
fn ambiguous_same_path_conversations_fail_explicitly() {
    let tmp = TempDir::new().unwrap();
    let archive = tmp.path().join("cass-archive.db");
    // Shared identity key; deliberately absent on disk so the archive is the
    // only place identity can resolve.
    let session = tmp.path().join("shared").join("amb-session.jsonl");

    let older = conversation(
        "claude_code",
        &session,
        "amb-alpha-ext",
        "amb-alpha-title",
        1_700_000_000_000,
        indexed_dialogue(0, "AMB-ALPHA-USER", "AMB-ALPHA-REPLY"),
    );
    let newer = conversation(
        "claude_code",
        &session,
        "amb-beta-ext",
        "amb-omega-title",
        1_700_100_000_000,
        indexed_dialogue(0, "AMB-OMEGA-USER", "AMB-OMEGA-REPLY"),
    );
    seed_archive(&archive, &[older, newer]);

    let out = run_export(tmp.path(), &archive, &session, &["--format", "markdown"]);
    let stdout = stdout_string(&out);
    let stderr = stderr_string(&out);
    assert!(
        !out.status.success(),
        "ambiguous identity must fail instead of silently picking a row\nstdout: {stdout}\nstderr: {stderr}"
    );
    let diagnostic = format!("{stderr}\n{stdout}").to_lowercase();
    assert!(
        diagnostic.contains("ambigu"),
        "the failure must surface ambiguity\nstdout: {stdout}\nstderr: {stderr}"
    );
    // Candidates must be NAMED, not counted: both distinctive titles are
    // required — a vague "ambiguous (2 matches)" that names nothing fails
    // this check, as does any single-digit coincidence in the output.
    assert!(
        stderr.contains("amb-alpha-title") && stderr.contains("amb-omega-title"),
        "the ambiguity diagnostic must list both candidate identities\nstdout: {stdout}\nstderr: {stderr}"
    );
    // And it must not emit a silently-chosen conversation's content.
    assert!(
        !stdout.contains("AMB-ALPHA-REPLY") && !stdout.contains("AMB-OMEGA-REPLY"),
        "no ambiguous winner may be exported\nstdout: {stdout}"
    );
}

#[test]
fn ambiguous_same_path_same_remote_source_fails_explicitly() {
    let tmp = TempDir::new().unwrap();
    let archive = tmp.path().join("cass-archive.db");
    let session = tmp.path().join("shared").join("amb-remote.jsonl");

    let mut older = conversation(
        "codex",
        &session,
        "amb-remote-alpha",
        "amb-remote-alpha-title",
        1_700_000_000_000,
        indexed_dialogue(0, "AMB-REMOTE-ALPHA-USER", "AMB-REMOTE-ALPHA-REPLY"),
    );
    older.source_id = "work-laptop".to_string();
    older.origin_host = Some("work-laptop".to_string());
    let mut newer = conversation(
        "codex",
        &session,
        "amb-remote-beta",
        "amb-remote-omega-title",
        1_700_100_000_000,
        indexed_dialogue(0, "AMB-REMOTE-OMEGA-USER", "AMB-REMOTE-OMEGA-REPLY"),
    );
    newer.source_id = "work-laptop".to_string();
    newer.origin_host = Some("work-laptop".to_string());
    seed_archive(&archive, &[older, newer]);

    let out = run_export(
        tmp.path(),
        &archive,
        &session,
        &["--format", "markdown", "--source", "work-laptop"],
    );
    let stdout = stdout_string(&out);
    let stderr = stderr_string(&out);
    assert!(
        !out.status.success(),
        "ambiguous identity within one source must fail\nstdout: {stdout}\nstderr: {stderr}"
    );
    let diagnostic = format!("{stderr}\n{stdout}").to_lowercase();
    assert!(
        diagnostic.contains("ambigu"),
        "the failure must surface ambiguity\nstdout: {stdout}\nstderr: {stderr}"
    );
    // Both distinctive candidate titles must be listed for disambiguation.
    assert!(
        stderr.contains("amb-remote-alpha-title") && stderr.contains("amb-remote-omega-title"),
        "the ambiguity diagnostic must list both candidate identities\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        !stdout.contains("AMB-REMOTE-ALPHA-REPLY") && !stdout.contains("AMB-REMOTE-OMEGA-REPLY"),
        "no ambiguous winner may be exported\nstdout: {stdout}"
    );
}

// -------------------------------------------------------------------------
// 5b. `--conversation-id` disambiguates a shared source_path.
//     RED-pending: the flag exists on `cass view`/`cass expand`
//     (`--conversation-id`, alias `conversation_id`, i64 >= 1) but is being
//     added to `cass export`/`cass export-html` by a sibling task. Until it
//     lands, clap rejects the flag (exit 2) — which still proves export did
//     not silently pick a same-path row, so the RED signal is valid either
//     way.
// -------------------------------------------------------------------------

#[test]
fn conversation_id_disambiguates_shared_source_path() {
    let tmp = TempDir::new().unwrap();
    let archive = tmp.path().join("cass-archive.db");
    let session = tmp.path().join("shared").join("convid-session.jsonl");

    let alpha = conversation(
        "claude_code",
        &session,
        "convid-alpha-ext",
        "convid-alpha-title",
        1_700_000_000_000,
        indexed_dialogue(0, "CONVID-ALPHA-USER", "CONVID-ALPHA-REPLY"),
    );
    let bravo = conversation(
        "claude_code",
        &session,
        "convid-bravo-ext",
        "convid-bravo-title",
        1_700_100_000_000,
        indexed_dialogue(0, "CONVID-BRAVO-USER", "CONVID-BRAVO-REPLY"),
    );
    let ids = seed_archive(&archive, &[alpha, bravo]);
    let bravo_id = ids[1];
    let flag = format!("--conversation-id={bravo_id}");

    let out = run_export(
        tmp.path(),
        &archive,
        &session,
        &["--format", "markdown", flag.as_str()],
    );
    // Exact identity beats ambiguity: B's indexed content, never A's.
    assert_stdout_has_indexed_only(
        &out,
        &["CONVID-BRAVO-USER", "CONVID-BRAVO-REPLY"],
        &["CONVID-ALPHA-USER", "CONVID-ALPHA-REPLY"],
        "--conversation-id export",
    );
}

// =========================================================================
// 6. Devin-style virtual path: indexed `agent_slug=devin`,
//    `source_path = <tmp>/sessions.db/<session-id>` with a REAL non-OpenCode
//    `sessions.db` on disk. Export must read the index, never the OpenCode
//    reader; a not-indexed variant must fail as not-indexed, not as an
//    OpenCode parse.
// =========================================================================

const DEVIN_INDEXED_USER: &str = "DEVIN-INDEXED-USER-UTTERANCE";
const DEVIN_INDEXED_ASSISTANT: &str = "DEVIN-INDEXED-ASSISTANT-REPLY";

fn devin_fixture(tmp: &TempDir, session_id: &str) -> (PathBuf, PathBuf) {
    let archive = tmp.path().join("cass-archive.db");
    let db_file = tmp.path().join("sessions.db");
    write_devin_sessions_db(&db_file);
    let virtual_path = db_file.join(session_id);

    let conv = conversation(
        "devin",
        &virtual_path,
        session_id,
        "Devin Indexed Session",
        1_700_000_000_000,
        indexed_dialogue(0, DEVIN_INDEXED_USER, DEVIN_INDEXED_ASSISTANT),
    );
    seed_archive(&archive, &[conv]);
    (archive, virtual_path)
}

#[test]
fn devin_virtual_db_path_uses_indexed_snapshot() {
    let tmp = TempDir::new().unwrap();
    let (archive, virtual_path) = devin_fixture(&tmp, "sess-devin-001");

    let out = run_export(
        tmp.path(),
        &archive,
        &virtual_path,
        &["--format", "markdown"],
    );
    assert_stdout_has_indexed_only(
        &out,
        &[DEVIN_INDEXED_USER, DEVIN_INDEXED_ASSISTANT],
        &["FILE-ONLY-UTTERANCE"],
        "devin virtual path export",
    );
    // The OpenCode reader must not be involved at all.
    let stderr = stderr_string(&out).to_lowercase();
    assert!(
        !stderr.contains("opencode"),
        "export of an indexed Devin session must not invoke the OpenCode reader\nstderr: {stderr}"
    );
}

#[test]
fn unindexed_devin_virtual_db_path_fails_as_not_indexed() {
    let tmp = TempDir::new().unwrap();
    // Archive exists and is non-empty, but holds no row for this virtual
    // path. (devin_fixture seeds `sess-devin-001` into the same archive.)
    let (archive, _indexed_virtual) = devin_fixture(&tmp, "sess-devin-001");
    // Deliberately no "index"/"indexed" substring in the unknown id, so the
    // index-diagnostic check below cannot self-satisfy on the echoed path.
    let unknown_virtual = tmp.path().join("sessions.db").join("sess-missing-999");

    let out = run_export(
        tmp.path(),
        &archive,
        &unknown_virtual,
        &["--format", "markdown"],
    );
    let stdout = stdout_string(&out);
    let stderr = stderr_string(&out);
    assert!(
        !out.status.success(),
        "an unindexed sessions.db/<id> identity must fail explicitly\nstdout: {stdout}\nstderr: {stderr}"
    );
    let diagnostic = format!("{stderr}\n{stdout}").to_lowercase();
    assert!(
        diagnostic.contains("index"),
        "failure must be a not-indexed diagnostic\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        !diagnostic.contains("opencode"),
        "failure must not be shaped as an OpenCode parse error\nstdout: {stdout}\nstderr: {stderr}"
    );
    // When the CLI emits a structured kind it must come from the index-first
    // taxonomy, never the OpenCode reader.
    if let Some(kind) = error_kind(&out) {
        assert_ne!(
            kind, "opencode-sqlite-parse",
            "err.kind must come from the index-first taxonomy, not the OpenCode reader"
        );
        assert_ne!(
            kind, "opencode-parse",
            "err.kind must come from the index-first taxonomy, not the OpenCode reader"
        );
    }
}

// =========================================================================
// 7. `.jsonl.zst` source_path exports from the index without any zstd decode
//    at export time (a deliberately truncated file must not break it).
// =========================================================================

#[test]
fn truncated_zst_source_uses_indexed_snapshot() {
    let tmp = TempDir::new().unwrap();
    let archive = tmp.path().join("cass-archive.db");
    let session = tmp
        .path()
        .join("rollouts")
        .join("rollout-sess-zst.jsonl.zst");
    let conv = conversation(
        "codex",
        &session,
        "zst-ext-1",
        "Zst Indexed Session",
        1_700_000_000_000,
        indexed_dialogue(0, "ZST-INDEXED-USER-UTTERANCE", "ZST-INDEXED-REPLY"),
    );
    seed_archive(&archive, &[conv]);

    fs::create_dir_all(session.parent().unwrap()).unwrap();
    // zstd magic bytes followed by a cut frame: undecodable on purpose.
    fs::write(&session, [0x28_u8, 0xB5, 0x2F, 0xFD, 0x04, 0x00, 0x19]).unwrap();

    let out = run_export(tmp.path(), &archive, &session, &["--format", "json"]);
    assert!(
        out.status.success(),
        "export must not decode the .zst at export time\nstdout: {}\nstderr: {}",
        stdout_string(&out),
        stderr_string(&out)
    );
    let stdout = stdout_string(&out);
    assert!(stdout.contains("ZST-INDEXED-USER-UTTERANCE"));
    assert!(stdout.contains("ZST-INDEXED-REPLY"));
}

// =========================================================================
// 8. `--source` binding: a conversation indexed under a remote source_id is
//    exported with indexed REMOTE content even when a same-path local file
//    exists.
// =========================================================================

#[test]
fn remote_source_binding_uses_indexed_remote_content() {
    let tmp = TempDir::new().unwrap();
    let archive = tmp.path().join("cass-archive.db");
    let session = tmp.path().join("shared").join("shared-path.jsonl");

    // Same path indexed locally AND remotely — a real provenance collision.
    let local = conversation(
        "codex",
        &session,
        "local-ext-1",
        "Local Indexed Title",
        1_700_000_000_000,
        indexed_dialogue(0, "LOCAL-INDEXED-UTTERANCE", "LOCAL-INDEXED-REPLY"),
    );
    let mut remote = conversation(
        "codex",
        &session,
        "remote-ext-1",
        "Remote Indexed Title",
        1_700_000_500_000,
        indexed_dialogue(0, "REMOTE-INDEXED-UTTERANCE", "REMOTE-INDEXED-REPLY"),
    );
    remote.source_id = "work-laptop".to_string();
    remote.origin_host = Some("work-laptop".to_string());
    seed_archive(&archive, &[local, remote]);

    write_jsonl(
        &session,
        &[
            ("user", "LOCAL-FILE-UTTERANCE"),
            ("assistant", "LOCAL-FILE-REPLY"),
        ],
    );

    let out = run_export(
        tmp.path(),
        &archive,
        &session,
        &["--format", "markdown", "--source", "work-laptop"],
    );
    assert_stdout_has_indexed_only(
        &out,
        &["REMOTE-INDEXED-UTTERANCE", "REMOTE-INDEXED-REPLY"],
        &[
            "LOCAL-FILE-UTTERANCE",
            "LOCAL-FILE-REPLY",
            "LOCAL-INDEXED-UTTERANCE",
            "LOCAL-INDEXED-REPLY",
        ],
        "remote --source export",
    );
}

// =========================================================================
// 9. `--include-tools`: tool payload inclusion follows *indexed* data.
// =========================================================================

fn tools_fixture(tmp: &TempDir) -> (PathBuf, PathBuf) {
    let archive = tmp.path().join("cass-archive.db");
    let session = tmp.path().join("ghost").join("tools-session.jsonl");
    let conv = conversation(
        "claude_code",
        &session,
        "tools-ext-1",
        "Tools Indexed Session",
        1_700_000_000_000,
        vec![
            message(
                0,
                MessageRole::User,
                1_700_000_000_000,
                "TOOLS-USER-UTTERANCE",
            ),
            message(
                1,
                MessageRole::Agent,
                1_700_000_001_000,
                "TOOLS-ASSISTANT-UTTERANCE",
            ),
            message(
                2,
                MessageRole::Tool,
                1_700_000_002_000,
                "TOOL-PAYLOAD-MARKER read_file output body",
            ),
        ],
    );
    seed_archive(&archive, &[conv]);
    (archive, session)
}

#[test]
fn include_tools_renders_indexed_tool_payload() {
    let tmp = TempDir::new().unwrap();
    let (archive, session) = tools_fixture(&tmp);

    let out = run_export(
        tmp.path(),
        &archive,
        &session,
        &["--format", "markdown", "--include-tools"],
    );
    let stdout = stdout_string(&out);
    assert!(
        out.status.success(),
        "--include-tools export must succeed\nstdout: {stdout}\nstderr: {}",
        stderr_string(&out)
    );
    assert!(stdout.contains("TOOLS-USER-UTTERANCE"));
    assert!(stdout.contains("TOOLS-ASSISTANT-UTTERANCE"));
    assert!(
        stdout.contains("TOOL-PAYLOAD-MARKER"),
        "--include-tools must surface the indexed tool payload\nstdout: {stdout}"
    );
}

#[test]
fn without_include_tools_indexed_tool_payload_is_hidden() {
    let tmp = TempDir::new().unwrap();
    let (archive, session) = tools_fixture(&tmp);

    let out = run_export(tmp.path(), &archive, &session, &["--format", "markdown"]);
    let stdout = stdout_string(&out);
    assert!(
        out.status.success(),
        "default export must succeed\nstdout: {stdout}\nstderr: {}",
        stderr_string(&out)
    );
    assert!(stdout.contains("TOOLS-USER-UTTERANCE"));
    assert!(stdout.contains("TOOLS-ASSISTANT-UTTERANCE"));
    // New contract: tool payloads follow --include-tools even when the index
    // retains them. Baseline gap (documented in the task report): an indexed
    // MessageRole::Tool renders unconditionally today because
    // format_as_markdown keeps role "tool" regardless of the flag.
    assert!(
        !stdout.contains("TOOL-PAYLOAD-MARKER"),
        "without --include-tools the indexed tool payload must stay hidden\nstdout: {stdout}"
    );
}

// =========================================================================
// 10. Failure paths must never leave a partial `-o` output file behind.
// =========================================================================

#[test]
fn unindexed_remote_source_leaves_no_output_file() {
    let tmp = TempDir::new().unwrap();
    let archive = tmp.path().join("cass-archive.db");
    // Archive holds a remote conversation under a DIFFERENT path, so this
    // export must fail rather than read the live file or a wrong identity.
    let mut remote = conversation(
        "codex",
        &tmp.path().join("other").join("elsewhere.jsonl"),
        "remote-elsewhere",
        "Remote Elsewhere",
        1_700_000_000_000,
        indexed_dialogue(0, "ELSEWHERE-USER", "ELSEWHERE-REPLY"),
    );
    remote.source_id = "work-laptop".to_string();
    remote.origin_host = Some("work-laptop".to_string());
    seed_archive(&archive, &[remote]);

    let session = tmp.path().join("sessions").join("live-session.jsonl");
    write_jsonl(&session, &[("user", CHANGED_USER)]);
    let out_file = tmp.path().join("partial-out.md");

    let out = run_export(
        tmp.path(),
        &archive,
        &session,
        &[
            "--format",
            "markdown",
            "--source",
            "work-laptop",
            "-o",
            out_file.to_str().unwrap(),
        ],
    );
    assert!(
        !out.status.success(),
        "not-indexed-under-remote-source must fail\nstdout: {}\nstderr: {}",
        stdout_string(&out),
        stderr_string(&out)
    );
    assert!(
        !out_file.exists(),
        "a failed export must not leave a partial output file"
    );
}

#[test]
fn unindexed_source_with_output_flag_writes_no_file() {
    let tmp = TempDir::new().unwrap();
    let archive = tmp.path().join("cass-archive.db");
    let unrelated = conversation(
        "claude_code",
        &tmp.path().join("other").join("unrelated.jsonl"),
        "unrelated-ext-1",
        "Unrelated Conversation",
        1_700_000_000_000,
        indexed_dialogue(0, "UNRELATED-USER", "UNRELATED-REPLY"),
    );
    seed_archive(&archive, &[unrelated]);

    let session = tmp.path().join("sessions").join("live-session.jsonl");
    write_jsonl(&session, &[("user", CHANGED_USER)]);
    let out_file = tmp.path().join("partial-out.md");

    let out = run_export(
        tmp.path(),
        &archive,
        &session,
        &["--format", "markdown", "-o", out_file.to_str().unwrap()],
    );
    assert!(
        !out.status.success(),
        "export of an unindexed conversation must fail even though the file parses\nstdout: {}\nstderr: {}",
        stdout_string(&out),
        stderr_string(&out)
    );
    assert!(
        !out_file.exists(),
        "a failed export must not leave a partial output file"
    );
}

#[test]
fn failed_virtual_db_path_leaves_no_output_file() {
    // Guard: this identity fails at baseline too (legacy OpenCode collision),
    // and it will still fail under the new contract (not-indexed) — the
    // no-partial-file invariant must hold across both failure shapes.
    let tmp = TempDir::new().unwrap();
    let archive = tmp.path().join("cass-archive.db");
    let _ = devin_fixture(&tmp, "sess-devin-001");
    let db_file = tmp.path().join("sessions.db");
    let unknown_virtual = db_file.join("sess-missing-999");
    let out_file = tmp.path().join("partial-out.md");

    let out = run_export(
        tmp.path(),
        &archive,
        &unknown_virtual,
        &["--format", "markdown", "-o", out_file.to_str().unwrap()],
    );
    assert!(
        !out.status.success(),
        "unindexed virtual db identity must fail\nstdout: {}\nstderr: {}",
        stdout_string(&out),
        stderr_string(&out)
    );
    assert!(
        !out_file.exists(),
        "a failed export must not leave a partial output file"
    );
}

// =========================================================================
// 11. Privacy guard: indexed content is redacted by default even when the
//     source-of-truth is the archive. `password=<value>` assignments hit
//     GENERIC_SECRET_ASSIGNMENT_PATTERN (src/indexer/redact_secrets.rs) and
//     render as `[REDACTED]`; `--include-secrets` opts back into raw output.
// =========================================================================

// ubs:ignore — synthetic redaction fixture, not a credential.
const SYNTHETIC_SECRET: &str = "password=hunter2-synthetic-export-9d8f2";
const SYNTHETIC_SECRET_VALUE: &str = "hunter2-synthetic-export-9d8f2";

#[test]
fn export_redacts_indexed_secret_unless_include_secrets() {
    let tmp = TempDir::new().unwrap();
    let archive = tmp.path().join("cass-archive.db");
    let session = tmp.path().join("ghost").join("secret-session.jsonl");
    let conv = conversation(
        "claude_code",
        &session,
        "secret-ext-1",
        "Secret Indexed Session",
        1_700_000_000_000,
        vec![
            message(
                0,
                MessageRole::User,
                1_700_000_000_000,
                &format!("REDACT-USER-UTTERANCE with {SYNTHETIC_SECRET} inline"),
            ),
            message(
                1,
                MessageRole::Agent,
                1_700_000_001_000,
                "REDACT-ASSISTANT-REPLY",
            ),
        ],
    );
    seed_archive(&archive, &[conv]);

    // Default: the archived secret value must never reach stdout.
    let redacted = run_export(tmp.path(), &archive, &session, &["--format", "markdown"]);
    let stdout = stdout_string(&redacted);
    assert!(
        redacted.status.success(),
        "default export must succeed\nstdout: {stdout}\nstderr: {}",
        stderr_string(&redacted)
    );
    assert!(
        !stdout.contains(SYNTHETIC_SECRET_VALUE),
        "default export must redact the indexed secret\nstdout: {stdout}"
    );
    assert!(
        stdout.contains("[REDACTED]"),
        "redaction marker should appear in place of the secret\nstdout: {stdout}"
    );
    assert!(stdout.contains("REDACT-ASSISTANT-REPLY"));

    // Explicit opt-in: --include-secrets reveals the archived value.
    let revealed = run_export(
        tmp.path(),
        &archive,
        &session,
        &["--format", "markdown", "--include-secrets"],
    );
    let stdout = stdout_string(&revealed);
    assert!(
        revealed.status.success(),
        "--include-secrets export must succeed\nstdout: {stdout}\nstderr: {}",
        stderr_string(&revealed)
    );
    assert!(
        stdout.contains(SYNTHETIC_SECRET),
        "--include-secrets must emit the archived secret value\nstdout: {stdout}"
    );
}
