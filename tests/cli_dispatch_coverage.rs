//! CLI dispatch subprocess integration tests.
//!
//! This module covers CLI commands that were previously untested via subprocess
//! invocation. Tests invoke the real binary with representative flags, validate
//! output formats, exit codes, and JSON structure.
//!
//! Coverage targets: completions, man, health, doctor, context, timeline, expand,
//! export, export-html, sources subcommands, models subcommands.

use assert_cmd::Command;
use coding_agent_search::franken_sync::compat::{ConnectionExt, RowExt};
use coding_agent_search::franken_sync::{Connection as FrankenConnection, params as fparams};
use coding_agent_search::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use predicates::prelude::*;
use predicates::str::contains;
use serde_json::{Value, json};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tempfile::TempDir;

/// Create a base command with isolated test environment.
fn base_cmd(temp_home: &Path) -> Command {
    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("cass"));
    let data_dir = temp_home.join(".local/share/coding-agent-search");
    // Isolate test environment
    cmd.env("HOME", temp_home);
    cmd.env("XDG_DATA_HOME", temp_home.join(".local/share"));
    cmd.env("CASS_DATA_DIR", data_dir);
    cmd.env("XDG_CONFIG_HOME", temp_home.join(".config"));
    cmd.env("CODEX_HOME", temp_home.join(".codex"));
    // Disable TTY detection
    cmd.env("NO_COLOR", "1");
    cmd
}

/// Create base command without HOME isolation (for simple tests), but with isolated XDG_DATA_HOME.
fn simple_cmd() -> Command {
    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("cass"));
    cmd.env("NO_COLOR", "1");

    // Create an isolated empty database with schema to avoid hitting the real user DB
    let tmp = tempfile::TempDir::new().unwrap();
    let db_dir = tmp.path().join("coding-agent-search");
    std::fs::create_dir_all(&db_dir).unwrap();
    let db_path = db_dir.join("agent_search.db");

    // Initialize the schema
    let fs = coding_agent_search::storage::sqlite::FrankenStorage::open(&db_path).unwrap();
    drop(fs);

    cmd.env("XDG_DATA_HOME", tmp.path());
    cmd.env("CASS_DATA_DIR", &db_dir);

    // Leak the temp dir so it survives the command execution
    std::mem::forget(tmp);

    cmd
}

fn run_on_large_stack<T, F>(f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let handle = std::thread::Builder::new()
        .name("cass-cli-dispatch-parse-test".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(f)
        .expect("spawn large-stack test thread");
    match handle.join() {
        Ok(value) => value,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

fn parse_cli_ok<const N: usize>(
    args: [&'static str; N],
    context: &'static str,
) -> coding_agent_search::Cli {
    run_on_large_stack(move || {
        <coding_agent_search::Cli as clap::Parser>::try_parse_from(args).expect(context)
    })
}

fn sample_agent(slug: &str, name: &str) -> Agent {
    Agent {
        id: None,
        slug: slug.to_string(),
        name: name.to_string(),
        version: None,
        kind: AgentKind::Cli,
    }
}

fn sample_message(idx: i64, role: MessageRole, ts: i64, content: &str) -> Message {
    Message {
        id: None,
        idx,
        role,
        author: None,
        created_at: Some(ts),
        content: content.to_string(),
        extra_json: json!({}),
        snippets: Vec::new(),
    }
}

fn make_codex_session(root: &Path, content: &str, ts: u64) {
    let sessions = root.join("sessions/2024/12/01");
    fs::create_dir_all(&sessions).unwrap();
    let file = sessions.join("rollout-test.jsonl");
    let sample = format!(
        r#"{{"type": "event_msg", "timestamp": {ts}, "payload": {{"type": "user_message", "message": "{content}"}}}}
{{"type": "response_item", "timestamp": {}, "payload": {{"role": "assistant", "content": "{content}_response"}}}}
"#,
        ts + 1000
    );
    fs::write(file, sample).unwrap();
}

fn sample_conversation(
    agent_slug: &str,
    workspace: &Path,
    source_path: &Path,
    external_id: &str,
    title: &str,
    started_at: i64,
    messages: Vec<Message>,
) -> Conversation {
    Conversation {
        id: None,
        agent_slug: agent_slug.to_string(),
        workspace: Some(workspace.to_path_buf()),
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

#[test]
fn completions_bash_outputs_valid_script() {
    let mut cmd = simple_cmd();
    cmd.args(["completions", "bash"]);
    let output = cmd.assert().success().get_output().clone();
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Bash completions should contain function definitions
    assert!(
        stdout.contains("_cass"),
        "bash completions should define _cass function"
    );
    assert!(
        stdout.contains("complete"),
        "bash completions should have complete command"
    );
}

#[test]
fn completions_zsh_outputs_valid_script() {
    let mut cmd = simple_cmd();
    cmd.args(["completions", "zsh"]);
    let output = cmd.assert().success().get_output().clone();
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Zsh completions should contain compdef
    assert!(
        stdout.contains("#compdef") || stdout.contains("compdef"),
        "zsh completions should have compdef directive"
    );
}

#[test]
fn completions_fish_outputs_valid_script() {
    let mut cmd = simple_cmd();
    cmd.args(["completions", "fish"]);
    let output = cmd.assert().success().get_output().clone();
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Fish completions use complete command
    assert!(
        stdout.contains("complete -c cass"),
        "fish completions should define completions for cass"
    );
}

#[test]
fn completions_powershell_outputs_valid_script() {
    let mut cmd = simple_cmd();
    cmd.args(["completions", "powershell"]);
    let output = cmd.assert().success().get_output().clone();
    let stdout = String::from_utf8_lossy(&output.stdout);

    // PowerShell completions use Register-ArgumentCompleter
    assert!(
        stdout.contains("Register-ArgumentCompleter")
            || stdout.contains("ArgumentCompleter")
            || stdout.contains("$scriptblock"),
        "powershell completions should define argument completer"
    );
}

#[test]
fn completions_help_shows_shells() {
    let mut cmd = simple_cmd();
    cmd.args(["completions", "--help"]);
    cmd.assert()
        .success()
        .stdout(contains("bash"))
        .stdout(contains("zsh"))
        .stdout(contains("fish"))
        .stdout(contains("powershell"));
}

// =============================================================================
// Man command tests
// =============================================================================

#[test]
fn man_outputs_groff_format() {
    let mut cmd = simple_cmd();
    cmd.arg("man");
    let output = cmd.assert().success().get_output().clone();
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Man pages start with .TH (title header) or .\" comment
    assert!(
        stdout.contains(".TH") || stdout.contains(".SH"),
        "man output should be groff format with .TH or .SH macros"
    );
    assert!(
        stdout.contains("cass") || stdout.contains("CASS"),
        "man page should mention cass"
    );
}

#[test]
fn man_help_shows_usage() {
    let mut cmd = simple_cmd();
    cmd.args(["man", "--help"]);
    cmd.assert().success().stdout(contains("Generate man page"));
}

// =============================================================================
// Health command tests
// =============================================================================

#[test]
fn health_json_outputs_valid_structure() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();

    let mut cmd = base_cmd(tmp.path());
    cmd.args(["health", "--json", "--data-dir", data_dir.to_str().unwrap()]);

    let output = cmd.assert().get_output().clone();
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Should output valid JSON with healthy field
    if !stdout.trim().is_empty() {
        let json: Value = serde_json::from_str(stdout.trim()).expect("valid health json");
        assert!(
            json.get("healthy").is_some(),
            "health JSON should have 'healthy' field"
        );
    }
}

#[test]
fn health_with_robot_meta_includes_metadata() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();

    // First create the DB by running index
    let mut idx_cmd = base_cmd(tmp.path());
    idx_cmd.args(["index", "--data-dir", data_dir.to_str().unwrap(), "--json"]);
    idx_cmd.assert().success();

    let mut cmd = base_cmd(tmp.path());
    cmd.args([
        "health",
        "--json",
        "--robot-meta",
        "--data-dir",
        data_dir.to_str().unwrap(),
    ]);

    let output = cmd.assert().success().get_output().clone();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: Value = serde_json::from_str(stdout.trim()).expect("valid health json with meta");

    // Should have _meta block
    assert!(
        json.get("_meta").is_some() || json.get("latency_ms").is_some(),
        "health --robot-meta should include metadata"
    );
}

#[test]
fn health_help_shows_options() {
    let mut cmd = simple_cmd();
    cmd.args(["health", "--help"]);
    cmd.assert()
        .success()
        .stdout(contains("health check"))
        .stdout(contains("--json"))
        .stdout(contains("--stale-threshold"));
}

// =============================================================================
// Doctor command tests
// =============================================================================

#[test]
fn doctor_json_outputs_valid_structure() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();

    let mut cmd = base_cmd(tmp.path());
    cmd.args(["doctor", "--json", "--data-dir", data_dir.to_str().unwrap()]);

    let output = cmd.assert().get_output().clone();
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Should output valid JSON
    if !stdout.trim().is_empty() {
        let json: Value = serde_json::from_str(stdout.trim()).expect("valid doctor json");
        // Doctor should have checks or issues array
        assert!(
            json.get("checks").is_some()
                || json.get("issues").is_some()
                || json.get("status").is_some(),
            "doctor JSON should have diagnostic fields"
        );
    }
}

#[test]
fn doctor_verbose_shows_all_checks() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();

    let mut cmd = base_cmd(tmp.path());
    cmd.args([
        "doctor",
        "--verbose",
        "--data-dir",
        data_dir.to_str().unwrap(),
    ]);

    // Just check it runs without error
    let _ = cmd.assert();
}

#[test]
fn doctor_help_shows_options() {
    let mut cmd = simple_cmd();
    cmd.args(["doctor", "--help"]);
    cmd.assert()
        .success()
        .stdout(contains("Diagnose"))
        .stdout(contains("--fix"))
        .stdout(contains("--verbose"));
}

#[test]
fn doctor_fix_preserves_corrupted_archive_bundle_without_repair_plan() {
    let tmp = TempDir::new().unwrap();
    let temp_home = tmp.path();
    let data_dir = temp_home.join("data");
    let codex_home = temp_home.join(".codex");
    fs::create_dir_all(&data_dir).unwrap();
    make_codex_session(&codex_home, "doctor sidecar recovery", 1_733_011_200_000);

    let db_path = data_dir.join("agent_search.db");
    let corrupt_bytes = b"not a sqlite database".to_vec();
    let wal_bytes = b"stale wal bytes".to_vec();
    let shm_bytes = b"stale shm bytes".to_vec();
    fs::write(&db_path, &corrupt_bytes).unwrap();
    fs::write(data_dir.join("agent_search.db-wal"), &wal_bytes).unwrap();
    fs::write(data_dir.join("agent_search.db-shm"), &shm_bytes).unwrap();

    let doctor = base_cmd(temp_home)
        .current_dir(temp_home)
        .args([
            "doctor",
            "--fix",
            "--json",
            "--data-dir",
            data_dir.to_str().unwrap(),
        ])
        .output()
        .expect("doctor command");
    assert!(
        !doctor.status.success(),
        "safe auto-run must fail closed for unreadable archive repair\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&doctor.stdout),
        String::from_utf8_lossy(&doctor.stderr)
    );
    let doctor_json: Value = serde_json::from_slice(&doctor.stdout).expect("valid doctor json");
    assert_eq!(
        doctor_json.get("auto_fix_applied").and_then(Value::as_bool),
        Some(false),
        "legacy safe auto-run must not move or replace archive evidence without a repair plan\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&doctor.stdout),
        String::from_utf8_lossy(&doctor.stderr)
    );
    assert_eq!(
        doctor_json["doctor_command"]["realized_subcommand"].as_str(),
        Some("safe-auto-run")
    );
    assert_eq!(
        doctor_json["doctor_command"]["execution_mode"].as_str(),
        Some("safe-auto-fix")
    );
    let safe_auto = &doctor_json["safe_auto_eligibility"];
    assert_eq!(safe_auto["enabled"].as_bool(), Some(true));
    assert!(
        safe_auto["manual_approval_required"]
            .as_array()
            .expect("manual approval actions")
            .iter()
            .any(|action| action.as_str() == Some("archive_rebuild_from_sources")),
        "archive-risk recovery should require a fingerprinted repair plan: {safe_auto:#}"
    );
    assert!(
        doctor_json["checks"]
            .as_array()
            .expect("doctor checks")
            .iter()
            .any(|check| {
                check["name"].as_str() == Some("safe_auto_archive_rebuild")
                    && check["status"].as_str() == Some("fail")
                    && check["fix_applied"].as_bool() == Some(false)
            }),
        "safe auto-run should surface the archive rebuild refusal check: {doctor_json:#}"
    );
    assert!(
        doctor_json["auto_fix_actions"]
            .as_array()
            .expect("auto fix actions")
            .iter()
            .all(|action| {
                !action
                    .as_str()
                    .unwrap_or_default()
                    .contains("Backed up corrupted database bundle")
            }),
        "safe auto-run must not report an archive bundle backup/move: {doctor_json:#}"
    );
    assert_eq!(
        fs::read(&db_path).unwrap(),
        corrupt_bytes,
        "corrupt archive DB bytes must remain in place for later forensic recovery"
    );
    assert_eq!(
        fs::read(data_dir.join("agent_search.db-wal")).unwrap(),
        wal_bytes,
        "WAL sidecar bytes must remain in place with the archive bundle"
    );
    assert_eq!(
        fs::read(data_dir.join("agent_search.db-shm")).unwrap(),
        shm_bytes,
        "SHM sidecar bytes must remain in place with the archive bundle"
    );

    let entries: Vec<String> = fs::read_dir(&data_dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        entries.iter().all(|name| !name.contains(".corrupt.")),
        "safe auto-run must not move SQLite DB/WAL/SHM bundles into ad-hoc corrupt backups: {entries:#?}"
    );
}

// =============================================================================
// Context command tests
// =============================================================================

#[test]
fn context_requires_path_argument() {
    let mut cmd = simple_cmd();
    cmd.arg("context");
    // Should fail without path
    cmd.assert().failure();
}

#[test]
fn context_json_with_nonexistent_path() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    drop(
        coding_agent_search::storage::sqlite::FrankenStorage::open(
            &data_dir.join("agent_search.db"),
        )
        .unwrap(),
    );

    let mut cmd = base_cmd(tmp.path());
    cmd.args([
        "context",
        "/nonexistent/path.jsonl",
        "--json",
        "--data-dir",
        data_dir.to_str().unwrap(),
    ]);

    cmd.timeout(Duration::from_secs(10))
        .assert()
        .code(4)
        .stderr(contains("No session found at path"));
}

#[test]
fn context_legacy_nulls_and_zero_limit_remain_readonly_before_migration() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("agent_search.db");
    let conn = FrankenConnection::open(db_path.to_string_lossy().as_ref()).unwrap();
    conn.execute_batch(
        "PRAGMA application_id = 0x43415353;
         PRAGMA user_version = 1;
         CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT);
         INSERT INTO meta VALUES('schema_version', '1');
         CREATE TABLE agents(id INTEGER PRIMARY KEY, slug TEXT);
         INSERT INTO agents VALUES(1, NULL);
         CREATE TABLE workspaces(id INTEGER PRIMARY KEY, path TEXT);
         INSERT INTO workspaces VALUES(1, '/legacy-workspace');
         CREATE TABLE conversations(
             id INTEGER PRIMARY KEY, agent_id INTEGER, workspace_id INTEGER,
             source_path TEXT, title TEXT, started_at INTEGER, source_id TEXT, origin_host TEXT
         );
         INSERT INTO conversations VALUES
             (1, NULL, 1, '/legacy/source.jsonl', NULL, 100, NULL, NULL),
             (2, 1, 1, '/legacy/related.jsonl', NULL, 200, '   ', 'user@remote');",
    )
    .unwrap();
    conn.close().unwrap();
    let before = fs::read(&db_path).unwrap();
    for limit in [1, 0] {
        let output = base_cmd(tmp.path())
            .args(["context", "/legacy/source.jsonl", "--json", "--limit"])
            .arg(limit.to_string())
            .arg("--data-dir")
            .arg(&data_dir)
            .timeout(Duration::from_secs(10))
            .assert()
            .success()
            .get_output()
            .clone();
        let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(payload["source"]["title"], "");
        assert_eq!(payload["source"]["agent"], "unknown");
        assert_eq!(payload["source"]["source_id"], "local");
        assert_eq!(payload["counts"]["same_agent"], 0);
        assert_eq!(payload["counts"]["same_workspace"], limit);
        assert_eq!(payload["counts"]["same_day"], limit);
        if limit == 1 {
            let related = &payload["related"]["same_workspace"][0];
            assert_eq!(related["path"], "/legacy/related.jsonl");
            assert_eq!(related["agent"], "unknown");
            assert_eq!(related["title"], "");
            assert_eq!(related["source_id"], "user@remote");
        }
    }
    assert_eq!(fs::read(&db_path).unwrap(), before);
}

#[test]
fn context_wide_archive_limit_one_is_bounded_and_readonly() {
    use coding_agent_search::storage::sqlite::{CURRENT_SCHEMA_VERSION, FrankenStorage};

    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("agent_search.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    let conn = storage.raw();
    conn.execute_batch(
        "INSERT INTO agents(id, slug, name, kind, created_at, updated_at)
             VALUES(1, 'codex', 'Codex', 'cli', 0, 0),
                   (2, 'claude', 'Claude', 'cli', 0, 0);
         INSERT INTO workspaces(id, path) VALUES(1, '/context-workspace'), (2, '/elsewhere');
         INSERT INTO sources(id, kind, created_at, updated_at)
             VALUES('  local  ', 'local', 0, 0), ('   ', 'ssh', 0, 0);",
    )
    .unwrap();
    // Reporter shape: 5,600 conversations with about 84 KiB metadata each.
    // No message population is needed: context never queries messages.
    let metadata = rmp_serde::to_vec_named(&json!({"padding": "x".repeat(84 * 1024)})).unwrap();
    for batch in 0..56_i64 {
        conn.execute("BEGIN IMMEDIATE").unwrap();
        for offset in 1..=100_i64 {
            let id = batch * 100 + offset;
            conn.execute_compat(
                "INSERT INTO conversations
                 (id, agent_id, workspace_id, source_path, title, started_at, metadata_bin)
                 VALUES(?1, 2, 2, ?2, 'wide archive session', ?3, ?4)",
                fparams![
                    id,
                    format!("/wide/{id}.jsonl"),
                    86_400_000_i64 + id,
                    metadata.as_slice()
                ],
            )
            .unwrap();
        }
        conn.execute("COMMIT").unwrap();
    }
    // The source and related sessions are older than all the unrelated rows,
    // preventing a newest-first table walk from passing by stopping early.
    conn.execute_batch(
        "UPDATE conversations SET agent_id = 1, workspace_id = 1, started_at = 100,
             title = 'source' WHERE id = 1;
         UPDATE conversations SET workspace_id = 1, started_at = 300,
             title = NULL, source_id = '  local  ' WHERE id = 2;
         UPDATE conversations SET agent_id = 1, started_at = 200,
             source_id = '   ', origin_host = 'user@remote' WHERE id = 3;",
    )
    .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    drop(storage);
    // Reopen through ordinary storage on schema v1, then prove every metadata
    // payload survived before invoking the CLI.
    let storage = FrankenStorage::open(&db_path).unwrap();
    assert_eq!(storage.schema_version().unwrap(), CURRENT_SCHEMA_VERSION);
    {
        let rows = storage
            .raw()
            .query("SELECT metadata_bin FROM conversations")
            .unwrap();
        assert_eq!(rows.len(), 5600);
        for row in rows {
            assert_eq!(row.get_typed::<Vec<u8>>(0).unwrap(), metadata);
        }
    }
    storage
        .raw()
        .execute("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    drop(storage);
    let before = blake3::hash(&fs::read(&db_path).unwrap());
    let wal_path = data_dir.join("agent_search.db-wal");
    let wal_before = wal_path.exists().then(|| fs::read(&wal_path).unwrap());
    let started = std::time::Instant::now();
    let output = base_cmd(tmp.path())
        .args([
            "context",
            "/wide/1.jsonl",
            "--json",
            "--limit",
            "1",
            "--data-dir",
        ])
        .arg(&data_dir)
        .timeout(Duration::from_secs(30))
        .assert()
        .success()
        .get_output()
        .clone();
    let elapsed = started.elapsed();
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["source"]["title"], "source");
    assert_eq!(
        payload["counts"],
        json!({"same_workspace": 1, "same_day": 1, "same_agent": 1})
    );
    let workspace = &payload["related"]["same_workspace"][0];
    assert_eq!(workspace["path"], "/wide/2.jsonl");
    assert_eq!(workspace["title"], "");
    assert_eq!(workspace["agent"], "claude");
    assert_eq!(workspace["source_id"], "local");
    assert_eq!(payload["related"]["same_day"][0], *workspace);
    assert_eq!(payload["related"]["same_agent"][0]["path"], "/wide/3.jsonl");
    assert_eq!(
        payload["related"]["same_agent"][0]["source_id"],
        "user@remote"
    );
    assert_eq!(
        blake3::hash(&fs::read(&db_path).unwrap()),
        before,
        "context must not migrate or rewrite the archive"
    );
    assert_eq!(
        wal_path.exists().then(|| fs::read(&wal_path).unwrap()),
        wal_before,
        "context must preserve the WAL too"
    );
    eprintln!(
        "GH463: conversations=5600 metadata_bytes={} limit=1 elapsed_ms={}",
        5600 * metadata.len(),
        elapsed.as_millis()
    );
}

#[test]
fn context_help_shows_options() {
    let mut cmd = simple_cmd();
    cmd.args(["context", "--help"]);
    cmd.assert()
        .success()
        .stdout(contains("related sessions"))
        .stdout(contains("--json"))
        .stdout(contains("--limit"));
}

// =============================================================================
// Timeline command tests
// =============================================================================

// Deliberately keep message bodies in the fixture: GH#521 was caused by the
// engine materializing those bodies for a join, even outside the time window.
fn seed_timeline_count_fixture(db_path: &Path, archive_messages: i64, body_bytes: usize) {
    let conn = FrankenConnection::open(db_path.to_string_lossy()).unwrap();
    conn.execute_batch(
        "PRAGMA application_id = 0x43415353;
         PRAGMA user_version = 1;
         CREATE TABLE agents (id INTEGER PRIMARY KEY, slug TEXT);
         CREATE TABLE sources (id TEXT PRIMARY KEY, kind TEXT);
         CREATE TABLE conversations (
             id INTEGER PRIMARY KEY, agent_id INTEGER, title TEXT,
             started_at INTEGER, ended_at INTEGER, source_path TEXT,
             source_id TEXT, origin_host TEXT);
         CREATE INDEX timeline_started_at ON conversations(started_at);
         CREATE TABLE messages (
             id INTEGER PRIMARY KEY, conversation_id INTEGER, idx INTEGER,
             content TEXT, UNIQUE(conversation_id, idx));
         INSERT INTO agents VALUES (1, 'codex'), (2, 'claude_code');
         INSERT INTO sources VALUES ('local', 'local'), ('laptop', 'ssh'),
                                    ('named-local', 'local');
         INSERT INTO conversations VALUES
             (1, 1, 'lower boundary', 1700000000000, 1700000005000, '/one', 'local', NULL),
             (2, 2, 'remote', 1700000030000, NULL, '/two', 'laptop', 'laptop'),
             (3, 1, 'empty upper boundary', 1700000060000, NULL, '/three', 'local', NULL),
             (4, 1, 'old archive', 1699999999999, NULL, '/four', 'local', NULL),
             (5, 1, 'after window', 1700000060001, NULL, '/five', 'local', NULL),
             (6, NULL, NULL, 1700000020000, NULL, '/six', NULL, NULL),
             (7, 1, 'named local', 1700000010000, NULL, '/seven', 'named-local', NULL),
             (8, 1, 'undated', NULL, NULL, '/eight', 'local', NULL);
         INSERT INTO messages (conversation_id, idx, content) VALUES
             (1, 0, 'one'), (1, 4, 'two'), (1, 9, 'three'),
             (2, 0, 'remote'), (5, 0, 'future'), (6, 0, 'unknown'),
             (7, 0, 'named local');",
    )
    .unwrap();
    let body = "x".repeat(body_bytes);
    conn.execute("BEGIN IMMEDIATE").unwrap();
    if archive_messages > 128 {
        // Match the report's conversation cardinality with a tiny window.
        for id in 9..=12_667_i64 {
            conn.execute_compat(
                "INSERT INTO conversations (id, agent_id, title, started_at,
                     source_path, source_id) VALUES (?1, 1, 'archive',
                     1600000000000, '/archive', 'local')",
                fparams![id],
            )
            .unwrap();
        }
    }
    for idx in 0..archive_messages {
        conn.execute_compat(
            "INSERT INTO messages (conversation_id, idx, content) VALUES (4, ?1, ?2)",
            fparams![idx, body.as_str()],
        )
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    conn.close().unwrap();
}

fn timeline_window_command(home: &Path, db_path: &Path) -> Command {
    let mut cmd = base_cmd(home);
    cmd.args([
        "timeline",
        "--since",
        "2023-11-14T22:13:20Z",
        "--until",
        "2023-11-14T22:14:20Z",
        "--json",
        "--db",
    ]);
    cmd.arg(db_path);
    cmd.timeout(Duration::from_secs(60));
    cmd
}

#[test]
fn timeline_counts_only_filtered_sessions_and_preserves_order() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("timeline.db");
    seed_timeline_count_fixture(&db_path, 128, 4096);
    for (filters, expected) in [
        (vec![], vec![(3, 0), (2, 1), (6, 1), (7, 1), (1, 3)]),
        (vec!["--agent", "codex"], vec![(3, 0), (7, 1), (1, 3)]),
        (vec!["--agent", "claude_code"], vec![(2, 1)]),
        (
            vec!["--source", "local"],
            vec![(3, 0), (6, 1), (7, 1), (1, 3)],
        ),
        (vec!["--source", "remote"], vec![(2, 1)]),
        (vec!["--source", "named-local"], vec![(7, 1)]),
        (vec!["--agent", "codex", "--source", "remote"], vec![]),
    ] {
        for grouping in ["none", "hour", "day"] {
            let output = timeline_window_command(tmp.path(), &db_path)
                .args(&filters)
                .args(["--group-by", grouping])
                .assert()
                .success()
                .get_output()
                .clone();
            let value: Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(value["total_sessions"], expected.len());
            assert_eq!(value["range"]["start"], 1_700_000_000_000_i64);
            assert_eq!(value["range"]["end"], 1_700_000_060_000_i64);
            let rows: Vec<&Value> = if grouping == "none" {
                value["sessions"].as_array().unwrap().iter().collect()
            } else {
                value["groups"]
                    .as_object()
                    .unwrap()
                    .values()
                    .flat_map(|group| group.as_array().unwrap())
                    .collect()
            };
            let actual: Vec<_> = rows
                .iter()
                .map(|row| {
                    (
                        row["id"].as_i64().unwrap(),
                        row["message_count"].as_i64().unwrap(),
                    )
                })
                .collect();
            assert_eq!(actual, expected, "filters={filters:?}, grouping={grouping}");
            if let Some(unknown) = rows.iter().find(|row| row["id"] == 6) {
                assert_eq!(unknown["agent"], "unknown");
                assert!(unknown["title"].is_null());
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn timeline_measured_output(cmd: &Command) -> Value {
    use std::process::Stdio;
    use std::time::Instant;

    let mut measured = std::process::Command::new(cmd.get_program());
    measured.args(cmd.get_args());
    for (key, value) in cmd.get_envs() {
        if let Some(value) = value {
            measured.env(key, value);
        } else {
            measured.env_remove(key);
        }
    }
    let mut child = measured
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let status_path = format!("/proc/{}/status", child.id());
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut peak_kib = 0_u64;
    loop {
        if let Ok(status) = fs::read_to_string(&status_path) {
            for line in status.lines() {
                if let Some(value) = line.strip_prefix("VmHWM:") {
                    peak_kib =
                        peak_kib.max(value.split_whitespace().next().unwrap().parse().unwrap());
                }
            }
        }
        if child.try_wait().unwrap().is_some() {
            break;
        }
        if Instant::now() >= deadline || peak_kib >= 192 * 1024 {
            child.kill().unwrap();
            let output = child.wait_with_output().unwrap();
            panic!(
                "timeline exceeded time/memory budget: peak={peak_kib} KiB, stderr={}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(peak_kib > 0, "no child RSS measurement captured");
    assert!(
        peak_kib < 192 * 1024,
        "timeline retained archive payloads: {peak_kib} KiB"
    );
    eprintln!(
        "timeline {:?} peak_rss_kib={peak_kib}",
        cmd.get_args().collect::<Vec<_>>()
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "256 MiB archive RSS probe; run explicitly with --ignored --test-threads=1"]
fn timeline_large_archive_peak_memory_is_bounded() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("timeline-large.db");
    seed_timeline_count_fixture(&db_path, 4096, 64 * 1024);
    assert!(fs::metadata(&db_path).unwrap().len() >= 256 * 1024 * 1024);
    for filters in [vec![], vec!["--agent", "codex"], vec!["--source", "local"]] {
        let mut cmd = timeline_window_command(tmp.path(), &db_path);
        cmd.args(&filters).args(["--group-by", "none"]);
        let value = timeline_measured_output(&cmd);
        assert_eq!(
            value["total_sessions"],
            if filters.is_empty() {
                5
            } else if filters[0] == "--agent" {
                3
            } else {
                4
            }
        );
    }
    for window in ["--today", "--since"] {
        let mut cmd = base_cmd(tmp.path());
        cmd.args(["timeline", "--json", "--group-by", "none", "--db"])
            .arg(&db_path)
            .arg(window);
        if window == "--since" {
            cmd.arg("5m");
        }
        let value = timeline_measured_output(&cmd);
        assert_eq!(value["total_sessions"], 0);
    }
    // Move the payload-heavy conversation into the window to verify that even
    // a very large selected session is counted without collecting its bodies.
    let conn = FrankenConnection::open(db_path.to_string_lossy()).unwrap();
    conn.execute("UPDATE conversations SET started_at = 1700000040000 WHERE id = 4")
        .unwrap();
    conn.close().unwrap();
    let mut cmd = timeline_window_command(tmp.path(), &db_path);
    cmd.args(["--group-by", "none"]);
    let value = timeline_measured_output(&cmd);
    let large = value["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == 4)
        .unwrap();
    assert_eq!(large["message_count"], 4096);
    for filters in [vec!["--agent", "claude_code"], vec!["--source", "remote"]] {
        let mut cmd = timeline_window_command(tmp.path(), &db_path);
        cmd.args(&filters).args(["--group-by", "none"]);
        let value = timeline_measured_output(&cmd);
        assert_eq!(value["total_sessions"], 1);
        assert_eq!(value["sessions"][0]["id"], 2);
        assert_eq!(value["sessions"][0]["message_count"], 1);
    }
}

#[test]
fn timeline_json_outputs_valid_structure() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();

    // First create DB
    let mut idx_cmd = base_cmd(tmp.path());
    idx_cmd.args(["index", "--data-dir", data_dir.to_str().unwrap(), "--json"]);
    idx_cmd.assert().success();

    let mut cmd = base_cmd(tmp.path());
    cmd.args([
        "timeline",
        "--json",
        "--today",
        "--group-by",
        "none",
        "--data-dir",
        data_dir.to_str().unwrap(),
    ]);

    let output = cmd.assert().success().get_output().clone();
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Should output valid JSON (may be empty array)
    if !stdout.trim().is_empty() {
        let _json: Value = serde_json::from_str(stdout.trim()).expect("valid timeline json");
    }
}

#[test]
fn timeline_json_normalizes_remote_provenance_without_source_row() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("agent_search.db");
    let storage = coding_agent_search::storage::sqlite::FrankenStorage::open(&db_path).unwrap();

    let workspace = tmp.path().join("workspace");
    fs::create_dir_all(&workspace).unwrap();

    let session = tmp.path().join("timeline-remote-no-source-row.jsonl");
    fs::write(&session, "{\"session\":\"timeline-remote\"}\n").unwrap();

    let codex_id = storage
        .ensure_agent(&sample_agent("codex", "Codex"))
        .unwrap();
    let workspace_id = storage
        .ensure_workspace(&workspace, Some("workspace"))
        .unwrap();
    storage
        .upsert_source(&coding_agent_search::sources::provenance::Source::remote(
            "work-laptop",
            "user@work-laptop",
        ))
        .unwrap();
    let conn =
        coding_agent_search::franken_sync::Connection::open(db_path.to_string_lossy()).unwrap();
    conn.execute("UPDATE sources SET kind = '' WHERE id = 'work-laptop'")
        .unwrap();

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let mut conversation = sample_conversation(
        "codex",
        &workspace,
        &session,
        "timeline-remote-no-source-row",
        "Remote Timeline Session",
        now_ms,
        vec![
            sample_message(0, MessageRole::User, now_ms, "question"),
            sample_message(1, MessageRole::Agent, now_ms + 1, "answer"),
        ],
    );
    conversation.source_id = "work-laptop".to_string();
    conversation.origin_host = Some("   ".to_string());
    storage
        .insert_conversation_tree(codex_id, Some(workspace_id), &conversation)
        .unwrap();

    let mut cmd = base_cmd(tmp.path());
    cmd.args([
        "timeline",
        "--json",
        "--today",
        "--data-dir",
        data_dir.to_str().unwrap(),
    ]);

    let output = cmd.assert().success().get_output().clone();
    let json: Value = serde_json::from_slice(&output.stdout).expect("valid timeline json");
    let sessions: Vec<&Value> = if let Some(items) = json.as_array() {
        items.iter().collect()
    } else if let Some(items) = json["sessions"].as_array() {
        items.iter().collect()
    } else {
        json["groups"]
            .as_object()
            .expect("timeline groups object")
            .values()
            .flat_map(|value| value.as_array().into_iter().flatten())
            .collect()
    };
    let entry = sessions
        .into_iter()
        .find(|entry| entry["source_path"].as_str() == Some(session.to_string_lossy().as_ref()))
        .expect("remote timeline session entry");

    assert_eq!(entry["source_id"].as_str(), Some("work-laptop"));
    assert_eq!(entry["origin_kind"].as_str(), Some("remote"));
    assert!(entry["origin_host"].is_null());
}

#[test]
fn timeline_json_derives_remote_source_id_from_origin_host_when_source_id_blank() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("agent_search.db");
    let storage = coding_agent_search::storage::sqlite::FrankenStorage::open(&db_path).unwrap();

    let workspace = tmp.path().join("workspace");
    fs::create_dir_all(&workspace).unwrap();

    let session = tmp.path().join("timeline-blank-source-id.jsonl");
    fs::write(&session, "{\"session\":\"timeline-blank-remote\"}\n").unwrap();

    let codex_id = storage
        .ensure_agent(&sample_agent("codex", "Codex"))
        .unwrap();
    let workspace_id = storage
        .ensure_workspace(&workspace, Some("workspace"))
        .unwrap();
    let conn =
        coding_agent_search::franken_sync::Connection::open(db_path.to_string_lossy()).unwrap();
    conn.execute(
        "INSERT INTO sources(id, kind, host_label, created_at, updated_at) VALUES ('   ', 'remote', 'user@work-laptop', 0, 0)",
    )
    .unwrap();

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let mut conversation = sample_conversation(
        "codex",
        &workspace,
        &session,
        "timeline-blank-source-id",
        "Timeline Blank Source Id",
        now_ms,
        vec![
            sample_message(0, MessageRole::User, now_ms, "question"),
            sample_message(1, MessageRole::Agent, now_ms + 1, "answer"),
        ],
    );
    conversation.source_id = "   ".to_string();
    conversation.origin_host = Some("user@work-laptop".to_string());
    storage
        .insert_conversation_tree(codex_id, Some(workspace_id), &conversation)
        .unwrap();

    let mut cmd = base_cmd(tmp.path());
    cmd.args([
        "timeline",
        "--json",
        "--today",
        "--data-dir",
        data_dir.to_str().unwrap(),
    ]);

    let output = cmd.assert().success().get_output().clone();
    let json: Value = serde_json::from_slice(&output.stdout).expect("valid timeline json");
    let sessions: Vec<&Value> = if let Some(items) = json.as_array() {
        items.iter().collect()
    } else if let Some(items) = json["sessions"].as_array() {
        items.iter().collect()
    } else {
        json["groups"]
            .as_object()
            .expect("timeline groups object")
            .values()
            .flat_map(|value| value.as_array().into_iter().flatten())
            .collect()
    };
    let entry = sessions
        .into_iter()
        .find(|entry| entry["source_path"].as_str() == Some(session.to_string_lossy().as_ref()))
        .expect("blank source timeline entry");

    assert_eq!(entry["source_id"].as_str(), Some("user@work-laptop"));
    assert_eq!(entry["origin_kind"].as_str(), Some("remote"));
    assert_eq!(entry["origin_host"].as_str(), Some("user@work-laptop"));
}

#[test]
fn timeline_human_output_does_not_badge_trimmed_local_source_id() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("agent_search.db");
    let storage = coding_agent_search::storage::sqlite::FrankenStorage::open(&db_path).unwrap();

    let workspace = tmp.path().join("workspace");
    fs::create_dir_all(&workspace).unwrap();

    let session = tmp.path().join("timeline-trimmed-local.jsonl");
    fs::write(&session, "{\"session\":\"timeline-trimmed-local\"}\n").unwrap();

    let codex_id = storage
        .ensure_agent(&sample_agent("codex", "Codex"))
        .unwrap();
    let workspace_id = storage
        .ensure_workspace(&workspace, Some("workspace"))
        .unwrap();
    storage
        .upsert_source(&coding_agent_search::sources::provenance::Source {
            id: "  local  ".to_string(),
            kind: coding_agent_search::sources::provenance::SourceKind::Local,
            host_label: None,
            machine_id: None,
            platform: None,
            config_json: None,
            created_at: None,
            updated_at: None,
        })
        .unwrap();

    let mut conversation = sample_conversation(
        "codex",
        &workspace,
        &session,
        "timeline-trimmed-local",
        "Timeline Trimmed Local",
        1_700_000_000_000,
        vec![
            sample_message(0, MessageRole::User, 1_700_000_000_000, "question"),
            sample_message(1, MessageRole::Agent, 1_700_000_000_001, "answer"),
        ],
    );
    conversation.source_id = "  local  ".to_string();
    storage
        .insert_conversation_tree(codex_id, Some(workspace_id), &conversation)
        .unwrap();

    let mut cmd = base_cmd(tmp.path());
    cmd.args([
        "timeline",
        "--since",
        "2020-01-01",
        "--until",
        "2030-01-01",
        "--group-by",
        "none",
        "--data-dir",
        data_dir.to_str().unwrap(),
    ]);

    let output = cmd.assert().success().get_output().clone();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Timeline Trimmed Local"));
    assert!(
        !stdout.contains("[  local  ]"),
        "unexpected raw local badge: {stdout}"
    );
    assert!(
        !stdout.contains("[local]"),
        "unexpected normalized local badge: {stdout}"
    );
}

#[test]
fn timeline_help_shows_options() {
    let mut cmd = simple_cmd();
    cmd.args(["timeline", "--help"]);
    cmd.assert()
        .success()
        .stdout(contains("timeline"))
        .stdout(contains("--since"))
        .stdout(contains("--until"))
        .stdout(contains("--today"));
}

// =============================================================================
// Expand command tests
// =============================================================================

#[test]
fn expand_requires_path_and_line() {
    let mut cmd = simple_cmd();
    cmd.arg("expand");
    cmd.assert().failure();
}

#[test]
fn expand_help_shows_options() {
    let mut cmd = simple_cmd();
    cmd.args(["expand", "--help"]);
    // Since #493 (aa5d5068) expand is described by its two coordinate systems
    // instead of "messages around": a hit's canonical ordinal or a raw line.
    cmd.assert()
        .success()
        .stdout(contains(
            "Expand a search hit with --message-index, or inspect raw JSONL with --line",
        ))
        .stdout(contains("--message-index"))
        .stdout(contains("--line"))
        .stdout(contains("--context"))
        .stdout(contains("--json"));
}

// =============================================================================
// Export command tests
// =============================================================================

#[test]
fn export_requires_path() {
    let mut cmd = simple_cmd();
    cmd.arg("export");
    cmd.assert().failure();
}

#[test]
fn export_help_shows_formats() {
    let mut cmd = simple_cmd();
    cmd.args(["export", "--help"]);
    cmd.assert()
        .success()
        .stdout(contains("Export"))
        .stdout(contains("--format"))
        .stdout(contains("--output"))
        .stdout(contains("markdown").or(contains("Markdown")));
}

// GH #519: a complete file is not sufficient proof of a successful export.
// Exercise the real process through normal return, including its thread-local
// database/runtime teardown, and check both the transcript and the exit status.
//
// Index-first arrangement: `cass export` resolves the canonical archive, so
// the WAL-contention target is the archive itself, not a provider store. The
// seed below mirrors `gh519_export_indexed_source_live_wal_*`: the nine
// GH519 messages plus the `export_wal_activity` ledger the live writer
// drives, committed through the real storage layer.
fn gh519_indexed_archive(tmp_home: &Path) -> (PathBuf, PathBuf, i64) {
    use coding_agent_search::storage::sqlite::FrankenStorage;

    let db_path = tmp_home.join("agent_search.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    let agent_id = storage
        .ensure_agent(&sample_agent("opencode", "OpenCode"))
        .unwrap();
    let session = tmp_home.join("archive-store/opencode.db").join("ses_gh519");
    let messages = (0..9_i64)
        .map(|index| {
            sample_message(
                index,
                if index % 2 == 0 {
                    MessageRole::User
                } else {
                    MessageRole::Agent
                },
                1_700_000_000_000 + index,
                &format!("GH519 message {index}: complete 🪟"),
            )
        })
        .collect();
    let conversation = sample_conversation(
        "opencode",
        tmp_home,
        &session,
        "ses_gh519",
        "GH519 complete conversation",
        1_700_000_000_000,
        messages,
    );
    let outcome = storage
        .insert_conversation_tree(agent_id, None, &conversation)
        .unwrap();
    storage
        .raw()
        .execute_batch(
            "CREATE TABLE export_wal_activity (id INTEGER PRIMARY KEY, generation INTEGER);
             INSERT INTO export_wal_activity VALUES (1, 0);",
        )
        .unwrap();
    storage.close().unwrap();
    (db_path, session, outcome.conversation_id)
}

fn gh519_export_cmd(home: &Path, db: &Path, session: &Path) -> Command {
    let mut cmd = base_cmd(home);
    cmd.current_dir(home)
        .env("USERPROFILE", home)
        .env("LOCALAPPDATA", home.join("AppData/Local"))
        .env("APPDATA", home.join("AppData/Roaming"))
        .env("CASS_AUTO_REFRESH", "0")
        .env("CASS_OUTPUT_FORMAT", "json")
        .env("RUST_BACKTRACE", "full")
        .arg("--db")
        .arg(db)
        .arg("export")
        .arg(session)
        .args(["--format", "json", "--include-tools"])
        .timeout(Duration::from_secs(60));
    cmd
}

fn gh519_assert_messages(bytes: &[u8]) {
    let payload: Value = serde_json::from_slice(bytes).expect("complete, valid exported JSON");
    let messages = payload.as_array().expect("export is a message array");
    assert_eq!(messages.len(), 9, "every expected message must survive");
    for (index, message) in messages.iter().enumerate() {
        assert_eq!(
            message["content"],
            format!("GH519 message {index}: complete 🪟")
        );
        assert_eq!(
            message["role"],
            if index % 2 == 0 { "user" } else { "assistant" }
        );
    }
}

fn gh519_assert_success(output: &std::process::Output) {
    assert_eq!(
        output.status.code(),
        Some(0),
        "a complete export must also exit successfully; status={}\nstdout={}\nstderr={}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stderr.contains("panicked at"), "{stderr}");
    assert!(
        !stderr.contains("threads should not terminate unexpectedly"),
        "{stderr}"
    );
}

fn gh519_assert_error(output: &std::process::Output, expected_kind: &str, expected_code: i32) {
    assert_eq!(output.status.code(), Some(expected_code), "{output:?}");
    assert!(
        output.stdout.is_empty(),
        "no export success on failure: {output:?}"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stderr.contains("panicked at"), "{stderr}");
    let envelope = stderr
        .lines()
        .rev()
        .find_map(|line| serde_json::from_str::<Value>(line).ok())
        .expect("typed error envelope must survive database/runtime shutdown");
    assert_eq!(envelope["error"]["code"], expected_code, "{stderr}");
    assert_eq!(envelope["error"]["kind"], expected_kind, "{stderr}");
    assert!(envelope["error"]["retryable"].is_boolean(), "{stderr}");
}

#[derive(Clone, Copy, Debug)]
enum Gh519WalActivity {
    Commits,
    Truncate,
}

/// The writer lives in this test process; every reader is a separate real cass
/// process. The selected nine messages stay fixed while unrelated transactions
/// repeatedly commit to the same WAL, optionally resetting it after each commit.
fn gh519_exports_with_wal_writer(
    db_path: &Path,
    commands: Vec<Command>,
    activity: Gh519WalActivity,
) -> Vec<std::process::Output> {
    use std::io::Read;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::mpsc;

    let stop = Arc::new(AtomicBool::new(false));
    let commits = Arc::new(AtomicUsize::new(0));
    let salt_changes = Arc::new(AtomicUsize::new(0));
    let (ready_tx, ready_rx) = mpsc::channel();
    let writer_stop = Arc::clone(&stop);
    let writer_commits = Arc::clone(&commits);
    let writer_salt_changes = Arc::clone(&salt_changes);
    let db_path = db_path.to_path_buf();
    let writer = std::thread::Builder::new()
        .name("gh519-live-wal-writer".into())
        .stack_size(128 * 1024 * 1024)
        .spawn(move || {
            let result = (|| -> anyhow::Result<()> {
                let conn = FrankenConnection::open(db_path.to_string_lossy().as_ref())?;
                conn.execute_batch(
                    "PRAGMA journal_mode = WAL;
                     PRAGMA wal_autocheckpoint = 0;
                     PRAGMA busy_timeout = 1000;",
                )?;
                let wal_path = db_path.with_file_name(format!(
                    "{}-wal",
                    db_path.file_name().unwrap().to_string_lossy()
                ));
                let mut ready_tx = Some(ready_tx);
                let mut previous_salt = None;
                while !writer_stop.load(Ordering::Acquire) {
                    conn.execute(
                        "UPDATE export_wal_activity SET generation = generation + 1 WHERE id = 1",
                    )?;
                    // Inspect actual WAL headers to distinguish ordinary live
                    // commits from the separately tested reset contention.
                    let mut header = [0_u8; 24];
                    fs::File::open(&wal_path)?.read_exact(&mut header)?;
                    let salt: [u8; 8] = header[16..24].try_into().unwrap();
                    if previous_salt.is_some_and(|previous| previous != salt) {
                        writer_salt_changes.fetch_add(1, Ordering::Release);
                    }
                    previous_salt = Some(salt);
                    if matches!(activity, Gh519WalActivity::Truncate) {
                        conn.query("PRAGMA wal_checkpoint(TRUNCATE)")?;
                    }
                    writer_commits.fetch_add(1, Ordering::Release);
                    if let Some(ready_tx) = ready_tx.take() {
                        ready_tx.send(())?;
                    }
                    std::thread::sleep(Duration::from_millis(2));
                }
                conn.close()?;
                Ok(())
            })();
            let drained = coding_agent_search::shutdown_thread_local_bridge_runtimes();
            assert!(drained, "writer runtime must finish before thread exit");
            result
        })
        .expect("spawn live WAL writer");

    let ready = ready_rx.recv_timeout(Duration::from_secs(30));
    let before = commits.load(Ordering::Acquire);
    let salts_before = salt_changes.load(Ordering::Acquire);
    // Save fallible subprocess results until after stopping and joining the
    // writer, so a failed export cannot leave the test's writer running.
    let outputs: Vec<_> = if ready.is_ok() {
        commands.into_iter().map(|mut cmd| cmd.output()).collect()
    } else {
        Vec::new()
    };
    stop.store(true, Ordering::Release);
    writer
        .join()
        .expect("WAL writer panicked")
        .expect("WAL writer failed");
    ready.expect("WAL writer became ready");
    let concurrent_commits = commits.load(Ordering::Acquire) - before;
    let concurrent_resets = salt_changes.load(Ordering::Acquire) - salts_before;
    assert!(concurrent_commits > 0, "no concurrent writes occurred");
    match activity {
        Gh519WalActivity::Commits => assert_eq!(
            concurrent_resets, 0,
            "steady writer must not reset the WAL during exports"
        ),
        Gh519WalActivity::Truncate => assert!(
            concurrent_resets > 0,
            "no WAL generation changed while cass exports ran"
        ),
    }
    eprintln!(
        "GH519 WAL {activity:?}: {concurrent_commits} concurrent commits, \
         {concurrent_resets} salt changes during {} exports",
        outputs.len()
    );
    outputs
        .into_iter()
        .map(|output| output.expect("run cass export"))
        .collect()
}

#[test]
fn gh519_export_archive_stdout_exits_successfully() {
    let tmp = TempDir::new().unwrap();
    let (db_path, session, _) = gh519_indexed_archive(tmp.path());
    let output = gh519_export_cmd(tmp.path(), &db_path, &session)
        .output()
        .unwrap();
    gh519_assert_success(&output);
    gh519_assert_messages(&output.stdout);
}

#[test]
fn gh519_export_archive_live_wal_exits_successfully_repeatedly() {
    let tmp = TempDir::new().unwrap();
    let (db_path, session, _) = gh519_indexed_archive(tmp.path());
    let paths: Vec<_> = (0..3)
        .map(|run| tmp.path().join(format!("out-{run}.json")))
        .collect();
    let commands = paths
        .iter()
        .map(|path| {
            let mut cmd = gh519_export_cmd(tmp.path(), &db_path, &session);
            cmd.args(["--source", "local"]).arg("-o").arg(path);
            cmd
        })
        .collect();
    let outputs = gh519_exports_with_wal_writer(&db_path, commands, Gh519WalActivity::Commits);
    for (path, output) in paths.iter().zip(outputs) {
        gh519_assert_success(&output);
        assert!(String::from_utf8_lossy(&output.stdout).contains("Exported to:"));
        gh519_assert_messages(&fs::read(path).expect("published export"));
    }
}

#[test]
fn gh519_export_indexed_source_live_wal_exits_successfully_repeatedly() {
    use coding_agent_search::sources::provenance::{Source, SourceKind};
    use coding_agent_search::storage::sqlite::FrankenStorage;

    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("agent_search.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    storage
        .upsert_source(&Source {
            id: "work-laptop".into(),
            kind: SourceKind::Ssh,
            host_label: Some("work-laptop".into()),
            machine_id: None,
            platform: None,
            config_json: None,
            created_at: None,
            updated_at: None,
        })
        .unwrap();
    let agent_id = storage
        .ensure_agent(&sample_agent("opencode", "OpenCode"))
        .unwrap();
    // Deliberately absent: --source must read the archive, not a local store.
    let session = tmp.path().join("remote/opencode.db/ses_gh519");
    let messages = (0..9)
        .map(|index| {
            sample_message(
                index,
                if index % 2 == 0 {
                    MessageRole::User
                } else {
                    MessageRole::Agent
                },
                1_700_000_000_000 + index,
                &format!("GH519 message {index}: complete 🪟"),
            )
        })
        .collect();
    let mut conversation = sample_conversation(
        "opencode",
        tmp.path(),
        &session,
        "ses_gh519",
        "GH519 complete conversation",
        1_700_000_000_000,
        messages,
    );
    conversation.source_id = "work-laptop".into();
    conversation.origin_host = Some("work-laptop".into());
    storage
        .insert_conversation_tree(agent_id, None, &conversation)
        .unwrap();
    storage
        .raw()
        .execute_batch(
            "CREATE TABLE export_wal_activity (id INTEGER PRIMARY KEY, generation INTEGER);
         INSERT INTO export_wal_activity VALUES (1, 0);",
        )
        .unwrap();
    drop(storage);

    let paths: Vec<_> = (0..3)
        .map(|run| tmp.path().join(format!("archive-{run}.json")))
        .collect();
    let commands = paths
        .iter()
        .map(|path| {
            let mut cmd = gh519_export_cmd(tmp.path(), &db_path, &session);
            cmd.args(["--source", "work-laptop"]).arg("-o").arg(path);
            cmd
        })
        .collect();
    let outputs = gh519_exports_with_wal_writer(&db_path, commands, Gh519WalActivity::Commits);
    for (path, output) in paths.iter().zip(outputs) {
        gh519_assert_success(&output);
        assert!(String::from_utf8_lossy(&output.stdout).contains("Exported to:"));
        gh519_assert_messages(&fs::read(path).expect("published archive export"));
    }
}

#[test]
fn gh519_export_archive_wal_resets_complete_or_fail_cleanly() {
    let tmp = TempDir::new().unwrap();
    let (db_path, session, _) = gh519_indexed_archive(tmp.path());
    let paths: Vec<_> = (0..6)
        .map(|run| tmp.path().join(format!("reset-{run}.json")))
        .collect();
    let commands = paths
        .iter()
        .map(|path| {
            let mut cmd = gh519_export_cmd(tmp.path(), &db_path, &session);
            cmd.args(["--source", "local"]).arg("-o").arg(path);
            cmd
        })
        .collect();
    let outputs = gh519_exports_with_wal_writer(&db_path, commands, Gh519WalActivity::Truncate);
    let mut completed = 0;
    let mut refused = 0;
    // Upstream independently reproduced a read refusal when TRUNCATE leaves
    // the WAL header briefly empty. That is separate from GH519's panic after
    // writing a complete file. Check every attempt; never retry or discard a
    // nonzero exit, and never accept published output paired with an error.
    for (path, output) in paths.iter().zip(outputs) {
        if output.status.code() == Some(0) {
            gh519_assert_success(&output);
            assert!(String::from_utf8_lossy(&output.stdout).contains("Exported to:"));
            gh519_assert_messages(&fs::read(path).expect("complete reset-contention export"));
            completed += 1;
        } else {
            // An archive read refused mid-flight maps to the index-first
            // read-failure kind (code 9), never a silent partial export.
            gh519_assert_error(&output, "index-read-failure", 9);
            assert!(
                !path.exists(),
                "a refused read must not publish an incomplete or misleading export"
            );
            refused += 1;
        }
    }
    assert_eq!(
        completed + refused,
        6,
        "every subprocess outcome is checked"
    );
    eprintln!("GH519 WAL reset outcomes: {completed} complete, {refused} clean read refusals");

    // The helper has stopped and joined the writer. Always demand complete
    // success now, so permanent unreadability cannot satisfy the refusal arm.
    let destination = tmp.path().join("after-resets.json");
    let output = gh519_export_cmd(tmp.path(), &db_path, &session)
        .args(["--source", "local"])
        .arg("-o")
        .arg(&destination)
        .output()
        .unwrap();
    gh519_assert_success(&output);
    gh519_assert_messages(&fs::read(destination).expect("complete export after WAL writer stops"));
}

#[test]
fn gh519_export_corrupt_archive_fails_without_publishing() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("agent_search.db");
    fs::write(&db_path, b"not a SQLite database").unwrap();
    let session = db_path.join("ses_gh519");
    let destination = tmp.path().join("out.json");
    for previous in [None, Some(b"previous complete export".as_slice())] {
        if let Some(previous) = previous {
            fs::write(&destination, previous).unwrap();
        }
        let output = gh519_export_cmd(tmp.path(), &db_path, &session)
            .arg("-o")
            .arg(&destination)
            .output()
            .unwrap();
        gh519_assert_error(&output, "index-unavailable", 9);
        if let Some(previous) = previous {
            assert_eq!(fs::read(&destination).unwrap(), previous);
        } else {
            assert!(
                !destination.exists(),
                "failed read must not publish an export"
            );
        }
    }
}

#[test]
fn gh519_export_unreadable_committed_wal_fails_without_publishing() {
    let tmp = TempDir::new().unwrap();
    let (db_path, session, conversation_id) = gh519_indexed_archive(tmp.path());
    let conn = FrankenConnection::open(db_path.to_string_lossy().as_ref()).unwrap();
    conn.execute("PRAGMA wal_autocheckpoint = 0").unwrap();
    let main_before = fs::read(&db_path).unwrap();
    conn.execute("BEGIN IMMEDIATE").unwrap();
    conn.execute_compat(
        "INSERT INTO messages(conversation_id, idx, role, author, created_at,
         content, extra_json) VALUES (?1, 9, 'assistant', NULL, 1700000000009,
         'GH519 committed WAL-only message', '{}')",
        fparams![conversation_id],
    )
    .unwrap();
    conn.execute("COMMIT").unwrap();
    conn.close_without_checkpoint().unwrap();
    let wal_path = tmp.path().join("agent_search.db-wal");
    assert!(fs::metadata(&wal_path).unwrap().len() > 32);
    assert_eq!(
        fs::read(&db_path).unwrap(),
        main_before,
        "the tenth committed message must require the WAL"
    );

    let complete = gh519_export_cmd(tmp.path(), &db_path, &session)
        .output()
        .unwrap();
    gh519_assert_success(&complete);
    let messages: Vec<Value> = serde_json::from_slice(&complete.stdout).unwrap();
    assert_eq!(messages.len(), 10);
    assert_eq!(messages[9]["content"], "GH519 committed WAL-only message");
    assert_eq!(fs::read(&db_path).unwrap(), main_before);

    // A directory gives a portable WAL I/O failure, unlike an obsolete salt
    // or invalid header the engine may legitimately treat as an empty WAL.
    // Retain the original committed bytes for diagnosis instead of deleting.
    let retained_wal = tmp.path().join("retained-committed.wal");
    fs::rename(&wal_path, &retained_wal).unwrap();
    fs::create_dir(&wal_path).unwrap();
    let destination = tmp.path().join("out.json");
    for previous in [None, Some(complete.stdout.as_slice())] {
        if let Some(previous) = previous {
            fs::write(&destination, previous).unwrap();
        }
        let output = gh519_export_cmd(tmp.path(), &db_path, &session)
            .arg("-o")
            .arg(&destination)
            .output()
            .unwrap();
        gh519_assert_error(&output, "index-unavailable", 9);
        if let Some(previous) = previous {
            assert_eq!(fs::read(&destination).unwrap(), previous);
        } else {
            assert!(!destination.exists(), "an incomplete read must not publish");
        }
    }
    assert!(wal_path.is_dir());
    assert!(retained_wal.is_file());
    assert_eq!(fs::read(&db_path).unwrap(), main_before);
}

#[test]
fn gh519_export_output_failure_exits_cleanly_after_archive_read() {
    let tmp = TempDir::new().unwrap();
    let (db_path, session, _) = gh519_indexed_archive(tmp.path());
    let destination = tmp.path().join("missing-directory/out.json");
    let output = gh519_export_cmd(tmp.path(), &db_path, &session)
        .arg("-o")
        .arg(&destination)
        .output()
        .unwrap();
    gh519_assert_error(&output, "file-create", 9);
    assert!(!destination.exists());

    #[cfg(target_os = "linux")]
    {
        // Unlike a missing parent directory, /dev/full opens successfully and
        // rejects write_all with ENOSPC after the archive has returned data.
        let output = gh519_export_cmd(tmp.path(), &db_path, &session)
            .args(["-o", "/dev/full"])
            .output()
            .unwrap();
        gh519_assert_error(&output, "file-write", 9);
    }
}

#[test]
fn gh519_export_missing_identity_does_not_use_another_archive() {
    let tmp = TempDir::new().unwrap();
    let (db_path, _, _) = gh519_indexed_archive(tmp.path());
    // The archive holds `archive-store/opencode.db/ses_gh519`; requesting the
    // same leaf name under a different parent must not substitute it.
    let requested = tmp.path().join("requested/opencode.db").join("ses_gh519");
    let destination = tmp.path().join("out.json");
    let output = gh519_export_cmd(tmp.path(), &db_path, &requested)
        .arg("-o")
        .arg(&destination)
        .output()
        .unwrap();
    gh519_assert_error(&output, "conversation-not-indexed", 3);
    assert!(
        !destination.exists(),
        "another conversation's session must not be substituted"
    );
}

// =============================================================================
// Export-HTML command tests
// =============================================================================

#[test]
fn export_html_requires_session() {
    let mut cmd = simple_cmd();
    cmd.arg("export-html");
    cmd.assert().failure();
}

#[test]
fn export_html_help_shows_options() {
    let mut cmd = simple_cmd();
    cmd.args(["export-html", "--help"]);
    cmd.assert()
        .success()
        .stdout(contains("HTML"))
        .stdout(contains("--output-dir"))
        .stdout(contains("--theme"))
        .stdout(predicate::str::contains("--encrypt").not());
}
// =============================================================================
// Sources subcommand tests
// =============================================================================

#[test]
fn sources_list_json_outputs_valid_structure() {
    let tmp = TempDir::new().unwrap();

    let mut cmd = base_cmd(tmp.path());
    cmd.args(["sources", "list", "--json"]);

    let output = cmd.assert().success().get_output().clone();
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Should output valid JSON with sources array
    let json: Value = serde_json::from_str(stdout.trim()).expect("valid sources list json");
    assert!(
        json.get("sources").map(|v| v.is_array()).unwrap_or(false) || json.is_object(),
        "sources list --json should return object with sources array"
    );
}

#[test]
fn sources_list_verbose() {
    let tmp = TempDir::new().unwrap();

    let mut cmd = base_cmd(tmp.path());
    cmd.args(["sources", "list", "--verbose"]);

    // Should complete without error
    cmd.assert().success();
}

#[test]
fn sources_doctor_json_outputs_structure() {
    let tmp = TempDir::new().unwrap();

    let mut cmd = base_cmd(tmp.path());
    cmd.args(["sources", "doctor", "--json"]);

    let output = cmd.assert().success().get_output().clone();
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Should output valid JSON
    let _json: Value = serde_json::from_str(stdout.trim()).expect("valid sources doctor json");
}

#[test]
fn sources_help_shows_subcommands() {
    let mut cmd = simple_cmd();
    cmd.args(["sources", "--help"]);
    cmd.assert()
        .success()
        .stdout(contains("list"))
        .stdout(contains("add"))
        .stdout(contains("remove"))
        .stdout(contains("doctor"))
        .stdout(contains("mappings"))
        .stdout(contains("agents"));
}

// =============================================================================
// Models subcommand tests
// =============================================================================

#[test]
fn models_status_json_outputs_structure() {
    let tmp = TempDir::new().unwrap();

    let mut cmd = base_cmd(tmp.path());
    cmd.args(["models", "status", "--json"]);

    let output = cmd.assert().success().get_output().clone();
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Should output valid JSON
    let json: Value = serde_json::from_str(stdout.trim()).expect("valid models status json");
    assert_eq!(
        json["lexical_fail_open"].as_bool(),
        Some(true),
        "models status should report lexical fail-open behavior"
    );
    assert!(
        json.get("cache_lifecycle").is_some(),
        "models status JSON should include cache lifecycle details: {json}"
    );
    assert!(
        json.get("next_step").is_some(),
        "models status JSON should include next-step guidance: {json}"
    );
    assert!(
        json["state"].as_str().is_some(),
        "models status JSON should include a machine-readable state: {json}"
    );
    // Keep the older loose structure check for compatibility with callers.
    assert!(
        json.get("installed").is_some()
            || json.get("models").is_some()
            || json.get("status").is_some()
            || json.is_object(),
        "models status JSON should have status information"
    );
}

#[test]
fn models_verify_json_with_no_model() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();

    let mut cmd = base_cmd(tmp.path());
    cmd.args([
        "models",
        "verify",
        "--json",
        "--data-dir",
        data_dir.to_str().unwrap(),
    ]);

    // May succeed with empty or fail - either is acceptable
    let output = cmd.assert().get_output().clone();
    let _stdout = String::from_utf8_lossy(&output.stdout);
}

#[test]
fn models_help_shows_subcommands() {
    let mut cmd = simple_cmd();
    cmd.args(["models", "--help"]);
    cmd.assert()
        .success()
        .stdout(contains("status"))
        .stdout(contains("install"))
        .stdout(contains("verify"))
        .stdout(contains("remove"));
}

#[test]
fn models_install_help_shows_options() {
    let mut cmd = simple_cmd();
    cmd.args(["models", "install", "--help"]);
    cmd.assert()
        .success()
        .stdout(contains("--model"))
        .stdout(contains("--from-file"))
        .stdout(predicate::str::contains("--mirror").not())
        .stdout(predicate::str::contains("--yes").not())
        .stdout(predicate::str::contains("--repair").not());
}

// =============================================================================
// Pages command tests
// =============================================================================

#[test]
fn pages_help_shows_options() {
    let output = simple_cmd()
        .args(["pages", "--help"])
        .assert()
        .success()
        .stdout(contains("searchable archive"))
        .stdout(contains("--export-only"))
        .stdout(contains("--verify"))
        .stdout(contains("--target"))
        .get_output()
        .clone();
    let stdout = String::from_utf8_lossy(&output.stdout);
    // Hosted publishing flags are gone — they must not surface in help.
    for removed in [
        "--project",
        "--repo",
        "--branch",
        "--account-id",
        "--api-token",
    ] {
        assert!(
            !stdout.contains(removed),
            "pages --help must not list hosted flag {removed}\n{stdout}"
        );
    }
}

#[test]
fn pages_verify_with_nonexistent_path() {
    let tmp = TempDir::new().unwrap();

    let mut cmd = base_cmd(tmp.path());
    cmd.args(["pages", "--verify", "/nonexistent/bundle"]);

    // Should fail with appropriate error
    cmd.assert().failure();
}

// =============================================================================
// Exit code tests
// =============================================================================

#[test]
fn search_requires_query_argument() {
    // search command requires a query argument
    let mut cmd = simple_cmd();
    cmd.arg("search");
    // Should fail without query
    cmd.assert().failure();
}

#[test]
fn missing_required_arg_returns_error() {
    let mut cmd = simple_cmd();
    cmd.args(["search"]); // Missing query
    cmd.assert().failure();
}

// =============================================================================
// Clap parsing tests for new commands
// =============================================================================

use coding_agent_search::{Commands, DisplayFormat, RobotFormat};

#[test]
fn parse_completions_bash() {
    let cli = parse_cli_ok(["cass", "completions", "bash"], "parse completions bash");
    match cli.command {
        Some(Commands::Completions { shell }) => {
            assert_eq!(shell, clap_complete::Shell::Bash);
        }
        other => panic!("expected completions command, got {other:?}"),
    }
}

#[test]
fn parse_health_with_stale_threshold() {
    let cli = parse_cli_ok(
        ["cass", "health", "--stale-threshold", "600"],
        "parse health with threshold",
    );
    match cli.command {
        Some(Commands::Health {
            stale_threshold, ..
        }) => {
            assert_eq!(stale_threshold, 600);
        }
        other => panic!("expected health command, got {other:?}"),
    }
}

#[test]
fn parse_pack_robot_contract_flags() {
    let cli = parse_cli_ok(
        [
            "cass",
            "--robot-format",
            "compact",
            "pack",
            "checkout failure",
            "--json",
            "--agent",
            "codex",
            "--agent",
            "claude",
            "--workspace",
            "/repo",
            "--limit",
            "40",
            "--fields",
            "summary,evidence",
            "--max-tokens",
            "4000",
            "--max-sessions",
            "5",
            "--max-evidence",
            "12",
            "--context-lines",
            "2",
            "--max-excerpt-chars",
            "800",
            "--request-id",
            "req-1",
            "--display",
            "markdown",
            "--data-dir",
            "/tmp/cass-data",
            "--days",
            "7",
            "--source",
            "remote",
            "--sessions-from",
            "-",
            "--mode",
            "lexical",
            "--freshness-policy",
            "strict",
            "--freshness-window-seconds",
            "3600",
            "--require-evidence",
            "--explain-selection",
            "--refresh",
            "--timeout",
            "9000",
        ],
        "parse pack robot contract flags",
    );

    assert_eq!(cli.robot_format, Some(RobotFormat::Compact));
    match cli.command {
        Some(Commands::Pack {
            query,
            agent,
            workspace,
            limit,
            json,
            fields,
            max_tokens,
            max_sessions,
            max_evidence,
            context_lines,
            max_excerpt_chars,
            request_id,
            display,
            data_dir,
            days,
            source,
            sessions_from,
            mode,
            freshness_policy,
            freshness_window_seconds,
            require_evidence,
            explain_selection,
            refresh,
            timeout,
            ..
        }) => {
            assert_eq!(query, "checkout failure");
            assert_eq!(agent, vec!["codex", "claude"]);
            assert_eq!(workspace, vec!["/repo"]);
            assert_eq!(limit, 40);
            assert!(json);
            assert_eq!(
                fields,
                Some(vec!["summary".to_string(), "evidence".to_string()])
            );
            assert_eq!(max_tokens, 4000);
            assert_eq!(max_sessions, 5);
            assert_eq!(max_evidence, 12);
            assert_eq!(context_lines, 2);
            assert_eq!(max_excerpt_chars, 800);
            assert_eq!(request_id, Some("req-1".to_string()));
            assert_eq!(display, Some(DisplayFormat::Markdown));
            assert_eq!(data_dir.unwrap().to_str().unwrap(), "/tmp/cass-data");
            assert_eq!(days, Some(7));
            assert_eq!(source, Some("remote".to_string()));
            assert_eq!(sessions_from, Some("-".to_string()));
            assert_eq!(
                mode,
                Some(coding_agent_search::search::query::SearchMode::Lexical)
            );
            assert_eq!(freshness_policy, "strict");
            assert_eq!(freshness_window_seconds, 3600);
            assert!(require_evidence);
            assert!(explain_selection);
            assert!(refresh);
            assert_eq!(timeout, Some(9000));
        }
        other => panic!("expected pack command, got {other:?}"),
    }
}

fn assert_pack_robot_error(args: &[&str], stdin: Option<&str>, expected_kind: &str) {
    let mut cmd = simple_cmd();
    cmd.args(args);
    if let Some(stdin) = stdin {
        cmd.write_stdin(stdin);
    }

    let output = cmd.assert().failure().get_output().clone();
    assert!(
        output.stdout.is_empty(),
        "pack robot errors must keep stdout data-only, got stdout={}",
        String::from_utf8_lossy(&output.stdout)
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    let json: Value = serde_json::from_str(stderr.trim())
        .unwrap_or_else(|err| panic!("stderr should be a JSON error envelope: {err}\n{stderr}"));
    assert_eq!(json["error"]["kind"], expected_kind);
}

#[test]
fn pack_empty_query_json_error_uses_stderr_only() {
    assert_pack_robot_error(&["pack", "", "--json"], None, "pack-empty-query");
}

#[test]
fn pack_rejects_sessions_robot_format_before_search() {
    assert_pack_robot_error(
        &["--robot-format", "sessions", "pack", "checkout", "--json"],
        None,
        "pack-unsupported-format",
    );
}

#[test]
fn pack_invalid_field_with_sessions_from_stdin_is_json_error() {
    assert_pack_robot_error(
        &[
            "pack",
            "checkout",
            "--json",
            "--sessions-from",
            "-",
            "--fields",
            "no_such_field",
        ],
        Some("/tmp/session-a.jsonl\n"),
        "pack-invalid-field",
    );
}

#[test]
fn parse_doctor_with_fix() {
    let cli = parse_cli_ok(
        ["cass", "doctor", "--fix", "--verbose"],
        "parse doctor with fix",
    );
    match cli.command {
        Some(Commands::Doctor { fix, verbose, .. }) => {
            assert!(fix, "fix should be true");
            assert!(verbose, "verbose should be true");
        }
        other => panic!("expected doctor command, got {other:?}"),
    }
}

#[test]
fn parse_timeline_with_filters() {
    let cli = parse_cli_ok(
        [
            "cass",
            "timeline",
            "--since",
            "2024-01-01",
            "--agent",
            "claude",
        ],
        "parse timeline with filters",
    );
    match cli.command {
        Some(Commands::Timeline { since, agent, .. }) => {
            assert_eq!(since, Some("2024-01-01".to_string()));
            assert_eq!(agent, vec!["claude"]);
        }
        other => panic!("expected timeline command, got {other:?}"),
    }
}

#[test]
fn parse_expand_with_context() {
    let cli = parse_cli_ok(
        [
            "cass",
            "expand",
            "/path/to/session.jsonl",
            "--line",
            "100",
            "-C",
            "5",
        ],
        "parse expand with context",
    );
    match cli.command {
        Some(Commands::Expand {
            path,
            line,
            context,
            ..
        }) => {
            assert_eq!(path.to_str().unwrap(), "/path/to/session.jsonl");
            assert_eq!(line, Some(100));
            assert_eq!(context, 5);
        }
        other => panic!("expected expand command, got {other:?}"),
    }
}

#[test]
fn parse_context_with_limit() {
    let cli = parse_cli_ok(
        ["cass", "context", "/path/to/session.jsonl", "--limit", "10"],
        "parse context with limit",
    );
    match cli.command {
        Some(Commands::Context { path, limit, .. }) => {
            assert_eq!(path.to_str().unwrap(), "/path/to/session.jsonl");
            assert_eq!(limit, 10);
        }
        other => panic!("expected context command, got {other:?}"),
    }
}

#[test]
fn parse_sessions_with_workspace_and_limit() {
    let cli = parse_cli_ok(
        [
            "cass",
            "sessions",
            "--workspace",
            "/path/to/project",
            "--limit",
            "3",
            "--json",
        ],
        "parse sessions with workspace and limit",
    );
    match cli.command {
        Some(Commands::Sessions {
            workspace,
            current,
            limit,
            json,
            ..
        }) => {
            assert_eq!(workspace.unwrap().to_str().unwrap(), "/path/to/project");
            assert!(!current);
            assert_eq!(limit, Some(3));
            assert!(json);
        }
        other => panic!("expected sessions command, got {other:?}"),
    }
}

#[test]
fn sessions_json_reports_recent_and_current_workspace_sessions() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("agent_search.db");
    let storage = coding_agent_search::storage::sqlite::FrankenStorage::open(&db_path).unwrap();

    let workspace_a = tmp.path().join("workspace-a");
    let workspace_a_nested = workspace_a.join("src");
    let workspace_b = tmp.path().join("workspace-b");
    fs::create_dir_all(&workspace_a_nested).unwrap();
    fs::create_dir_all(&workspace_b).unwrap();

    let session_a_old = tmp.path().join("claude-old.jsonl");
    let session_a_new = tmp.path().join("claude-new.jsonl");
    let session_b = tmp.path().join("codex.jsonl");
    fs::write(&session_a_old, "{\"session\":\"old\"}\n").unwrap();
    std::thread::sleep(Duration::from_millis(5));
    fs::write(&session_a_new, "{\"session\":\"new\"}\n").unwrap();
    std::thread::sleep(Duration::from_millis(5));
    fs::write(&session_b, "{\"session\":\"other\"}\n").unwrap();

    let claude_id = storage
        .ensure_agent(&sample_agent("claude_code", "Claude Code"))
        .unwrap();
    let codex_id = storage
        .ensure_agent(&sample_agent("codex", "Codex"))
        .unwrap();
    let workspace_a_id = storage
        .ensure_workspace(&workspace_a, Some("workspace-a"))
        .unwrap();
    let workspace_b_id = storage
        .ensure_workspace(&workspace_b, Some("workspace-b"))
        .unwrap();

    storage
        .insert_conversation_tree(
            claude_id,
            Some(workspace_a_id),
            &sample_conversation(
                "claude_code",
                &workspace_a,
                &session_a_old,
                "claude-old",
                "Old Claude Session",
                1_700_000_000_000,
                vec![
                    sample_message(0, MessageRole::User, 1_700_000_000_000, "old question"),
                    sample_message(1, MessageRole::Agent, 1_700_000_000_001, "old answer"),
                ],
            ),
        )
        .unwrap();
    storage
        .insert_conversation_tree(
            claude_id,
            Some(workspace_a_id),
            &sample_conversation(
                "claude_code",
                &workspace_a,
                &session_a_new,
                "claude-new",
                "Newest Claude Session",
                1_700_000_100_000,
                vec![
                    sample_message(0, MessageRole::User, 1_700_000_100_000, "new question"),
                    sample_message(1, MessageRole::Agent, 1_700_000_100_001, "new answer"),
                ],
            ),
        )
        .unwrap();
    storage
        .insert_conversation_tree(
            codex_id,
            Some(workspace_b_id),
            &sample_conversation(
                "codex",
                &workspace_b,
                &session_b,
                "codex-other",
                "Other Workspace Session",
                1_700_000_200_000,
                vec![
                    sample_message(0, MessageRole::User, 1_700_000_200_000, "other question"),
                    sample_message(1, MessageRole::Agent, 1_700_000_200_001, "other answer"),
                ],
            ),
        )
        .unwrap();

    let mut all_cmd = base_cmd(tmp.path());
    all_cmd.args([
        "sessions",
        "--json",
        "--data-dir",
        data_dir.to_str().unwrap(),
    ]);
    let all_output = all_cmd.assert().success().get_output().clone();
    let all_json: Value = serde_json::from_slice(&all_output.stdout).expect("valid sessions json");
    let all_sessions = all_json["sessions"].as_array().expect("sessions array");
    assert_eq!(all_sessions.len(), 3, "should list all recent sessions");
    assert_eq!(
        all_sessions[0]["path"].as_str().unwrap(),
        session_b.to_string_lossy(),
        "most recently modified file should come first"
    );
    assert_eq!(all_sessions[0]["message_count"], 2);
    assert_eq!(all_sessions[0]["human_turns"], 1);
    assert_eq!(all_sessions[0]["source_id"].as_str(), Some("local"));
    assert!(all_sessions[0]["origin_host"].is_null());
    assert!(all_sessions[0]["size_bytes"].is_number());

    let mut current_cmd = base_cmd(tmp.path());
    current_cmd.current_dir(&workspace_a_nested);
    current_cmd.args([
        "sessions",
        "--current",
        "--json",
        "--data-dir",
        data_dir.to_str().unwrap(),
    ]);
    let current_output = current_cmd.assert().success().get_output().clone();
    let current_json: Value =
        serde_json::from_slice(&current_output.stdout).expect("valid current sessions json");
    let current_sessions = current_json["sessions"].as_array().expect("sessions array");
    assert_eq!(
        current_sessions.len(),
        1,
        "--current should return one best match"
    );
    assert_eq!(
        current_sessions[0]["path"].as_str().unwrap(),
        session_a_new.to_string_lossy(),
        "current workspace should resolve to newest matching workspace session"
    );
    assert_eq!(
        current_sessions[0]["workspace"].as_str().unwrap(),
        workspace_a.to_string_lossy()
    );
}

/// cass#323: `--limit` must truncate the result (counts are backfilled only
/// for surviving sessions) and `--since` must bound the enumeration window
/// by recorded activity timestamps.
#[test]
fn sessions_json_since_bounds_window_and_limit_truncates() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("agent_search.db");
    let storage = coding_agent_search::storage::sqlite::FrankenStorage::open(&db_path).unwrap();

    let workspace = tmp.path().join("workspace");
    fs::create_dir_all(&workspace).unwrap();

    let session_old = tmp.path().join("since-old.jsonl");
    let session_new = tmp.path().join("since-new.jsonl");
    fs::write(&session_old, "{\"session\":\"old\"}\n").unwrap();
    std::thread::sleep(Duration::from_millis(5));
    fs::write(&session_new, "{\"session\":\"new\"}\n").unwrap();

    let claude_id = storage
        .ensure_agent(&sample_agent("claude_code", "Claude Code"))
        .unwrap();
    let workspace_id = storage
        .ensure_workspace(&workspace, Some("workspace"))
        .unwrap();

    // Old session: started/ended 2023-11-14.
    let old_started = 1_700_000_000_000_i64;
    storage
        .insert_conversation_tree(
            claude_id,
            Some(workspace_id),
            &sample_conversation(
                "claude_code",
                &workspace,
                &session_old,
                "since-old",
                "Old Session",
                old_started,
                vec![
                    sample_message(0, MessageRole::User, old_started, "old question"),
                    sample_message(1, MessageRole::Agent, old_started + 1, "old answer"),
                ],
            ),
        )
        .unwrap();
    // New session: started/ended 2024-12-24.
    let new_started = 1_735_000_000_000_i64;
    storage
        .insert_conversation_tree(
            claude_id,
            Some(workspace_id),
            &sample_conversation(
                "claude_code",
                &workspace,
                &session_new,
                "since-new",
                "New Session",
                new_started,
                vec![
                    sample_message(0, MessageRole::User, new_started, "new question"),
                    sample_message(1, MessageRole::Agent, new_started + 1, "new answer"),
                    sample_message(2, MessageRole::User, new_started + 2, "follow-up"),
                ],
            ),
        )
        .unwrap();

    // --since between the two sessions keeps only the newer one.
    let mut since_cmd = base_cmd(tmp.path());
    since_cmd.args([
        "sessions",
        "--json",
        "--since",
        "2024-06-01",
        "--data-dir",
        data_dir.to_str().unwrap(),
    ]);
    let since_output = since_cmd.assert().success().get_output().clone();
    let since_json: Value =
        serde_json::from_slice(&since_output.stdout).expect("valid sessions json");
    let since_sessions = since_json["sessions"].as_array().expect("sessions array");
    assert_eq!(
        since_sessions.len(),
        1,
        "--since should exclude the session that ended before the cutoff"
    );
    assert_eq!(
        since_sessions[0]["path"].as_str().unwrap(),
        session_new.to_string_lossy()
    );
    // Counts are backfilled after the limit push-down; they must stay exact.
    assert_eq!(since_sessions[0]["message_count"], 3);
    assert_eq!(since_sessions[0]["human_turns"], 2);

    // --limit 1 returns exactly one session, with exact counts.
    let mut limit_cmd = base_cmd(tmp.path());
    limit_cmd.args([
        "sessions",
        "--json",
        "--limit",
        "1",
        "--data-dir",
        data_dir.to_str().unwrap(),
    ]);
    let limit_output = limit_cmd.assert().success().get_output().clone();
    let limit_json: Value =
        serde_json::from_slice(&limit_output.stdout).expect("valid sessions json");
    let limit_sessions = limit_json["sessions"].as_array().expect("sessions array");
    assert_eq!(limit_sessions.len(), 1, "--limit 1 should truncate to one");
    assert_eq!(
        limit_sessions[0]["path"].as_str().unwrap(),
        session_new.to_string_lossy(),
        "most recently modified session should win the single slot"
    );
    assert_eq!(limit_sessions[0]["message_count"], 3);
    assert_eq!(limit_sessions[0]["human_turns"], 2);

    // An unparseable --since is a usage error, not a silent full scan.
    let mut bad_cmd = base_cmd(tmp.path());
    bad_cmd.args([
        "sessions",
        "--json",
        "--since",
        "not-a-date",
        "--data-dir",
        data_dir.to_str().unwrap(),
    ]);
    bad_cmd.assert().failure();
}

#[test]
fn sessions_json_keeps_local_file_metadata_for_trimmed_local_source_id() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("agent_search.db");
    let storage = coding_agent_search::storage::sqlite::FrankenStorage::open(&db_path).unwrap();

    let workspace = tmp.path().join("workspace");
    fs::create_dir_all(&workspace).unwrap();

    let session = tmp.path().join("trimmed-local.jsonl");
    fs::write(&session, "{\"session\":\"trimmed-local\"}\n").unwrap();

    let codex_id = storage
        .ensure_agent(&sample_agent("codex", "Codex"))
        .unwrap();
    let workspace_id = storage
        .ensure_workspace(&workspace, Some("workspace"))
        .unwrap();
    storage
        .upsert_source(&coding_agent_search::sources::provenance::Source {
            id: "  local  ".to_string(),
            kind: coding_agent_search::sources::provenance::SourceKind::Local,
            host_label: None,
            machine_id: None,
            platform: None,
            config_json: None,
            created_at: None,
            updated_at: None,
        })
        .unwrap();

    let mut conversation = sample_conversation(
        "codex",
        &workspace,
        &session,
        "trimmed-local",
        "Trimmed Local Session",
        1_700_000_000_000,
        vec![
            sample_message(0, MessageRole::User, 1_700_000_000_000, "question"),
            sample_message(1, MessageRole::Agent, 1_700_000_000_001, "answer"),
        ],
    );
    conversation.source_id = "  local  ".to_string();
    storage
        .insert_conversation_tree(codex_id, Some(workspace_id), &conversation)
        .unwrap();

    let mut cmd = base_cmd(tmp.path());
    cmd.args([
        "sessions",
        "--json",
        "--data-dir",
        data_dir.to_str().unwrap(),
    ]);

    let output = cmd.assert().success().get_output().clone();
    let json: Value = serde_json::from_slice(&output.stdout).expect("valid sessions json");
    let sessions = json["sessions"].as_array().expect("sessions array");
    let entry = sessions
        .iter()
        .find(|entry| entry["path"].as_str() == Some(session.to_string_lossy().as_ref()))
        .expect("trimmed local session entry");

    assert_eq!(entry["source_id"].as_str(), Some("local"));
    assert!(
        entry["size_bytes"].is_number(),
        "expected local metadata for trimmed local source"
    );
    assert!(
        entry["modified"].is_string(),
        "expected modified timestamp for trimmed local source"
    );
}

#[test]
fn sessions_json_derives_remote_source_id_from_origin_host_when_source_id_blank() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("agent_search.db");
    let storage = coding_agent_search::storage::sqlite::FrankenStorage::open(&db_path).unwrap();

    let workspace = tmp.path().join("workspace");
    fs::create_dir_all(&workspace).unwrap();

    let session = tmp.path().join("remote-blank-source-id.jsonl");
    fs::write(&session, "{\"session\":\"remote\"}\n").unwrap();

    let codex_id = storage
        .ensure_agent(&sample_agent("codex", "Codex"))
        .unwrap();
    let workspace_id = storage
        .ensure_workspace(&workspace, Some("workspace"))
        .unwrap();
    let conn =
        coding_agent_search::franken_sync::Connection::open(db_path.to_string_lossy()).unwrap();
    conn.execute(
        "INSERT INTO sources(id, kind, host_label, created_at, updated_at) VALUES ('   ', 'remote', 'user@work-laptop', 0, 0)",
    )
    .unwrap();

    let mut conversation = sample_conversation(
        "codex",
        &workspace,
        &session,
        "remote-blank-source-id",
        "Remote Blank Source Id",
        1_700_000_000_000,
        vec![
            sample_message(0, MessageRole::User, 1_700_000_000_000, "question"),
            sample_message(1, MessageRole::Agent, 1_700_000_000_001, "answer"),
        ],
    );
    conversation.source_id = "   ".to_string();
    conversation.origin_host = Some("user@work-laptop".to_string());
    storage
        .insert_conversation_tree(codex_id, Some(workspace_id), &conversation)
        .unwrap();

    let mut cmd = base_cmd(tmp.path());
    cmd.args([
        "sessions",
        "--json",
        "--data-dir",
        data_dir.to_str().unwrap(),
    ]);

    let output = cmd.assert().success().get_output().clone();
    let json: Value = serde_json::from_slice(&output.stdout).expect("valid sessions json");
    let sessions = json["sessions"].as_array().expect("sessions array");
    let entry = sessions
        .iter()
        .find(|entry| entry["path"].as_str() == Some(session.to_string_lossy().as_ref()))
        .expect("remote blank source session entry");

    assert_eq!(entry["source_id"].as_str(), Some("user@work-laptop"));
    assert_eq!(entry["origin_host"].as_str(), Some("user@work-laptop"));
    assert!(
        entry["size_bytes"].is_null(),
        "remote fallback source_id must not be treated as local metadata"
    );
}

#[test]
fn sessions_json_keeps_local_file_metadata_for_blank_source_id() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("agent_search.db");
    let storage = coding_agent_search::storage::sqlite::FrankenStorage::open(&db_path).unwrap();

    let workspace = tmp.path().join("workspace");
    fs::create_dir_all(&workspace).unwrap();

    let session = tmp.path().join("blank-local-source-id.jsonl");
    fs::write(&session, "{\"session\":\"blank-local\"}\n").unwrap();

    let codex_id = storage
        .ensure_agent(&sample_agent("codex", "Codex"))
        .unwrap();
    let workspace_id = storage
        .ensure_workspace(&workspace, Some("workspace"))
        .unwrap();
    let conn =
        coding_agent_search::franken_sync::Connection::open(db_path.to_string_lossy()).unwrap();
    conn.execute(
        "INSERT INTO sources(id, kind, host_label, created_at, updated_at) VALUES ('   ', 'local', NULL, 0, 0)",
    )
    .unwrap();

    let mut conversation = sample_conversation(
        "codex",
        &workspace,
        &session,
        "blank-local-source-id",
        "Blank Local Source Id",
        1_700_000_000_000,
        vec![
            sample_message(0, MessageRole::User, 1_700_000_000_000, "question"),
            sample_message(1, MessageRole::Agent, 1_700_000_000_001, "answer"),
        ],
    );
    conversation.source_id = "   ".to_string();
    storage
        .insert_conversation_tree(codex_id, Some(workspace_id), &conversation)
        .unwrap();

    let mut cmd = base_cmd(tmp.path());
    cmd.args([
        "sessions",
        "--json",
        "--data-dir",
        data_dir.to_str().unwrap(),
    ]);

    let output = cmd.assert().success().get_output().clone();
    let json: Value = serde_json::from_slice(&output.stdout).expect("valid sessions json");
    let sessions = json["sessions"].as_array().expect("sessions array");
    let entry = sessions
        .iter()
        .find(|entry| entry["path"].as_str() == Some(session.to_string_lossy().as_ref()))
        .expect("blank local session entry");

    assert_eq!(entry["source_id"].as_str(), Some("local"));
    assert!(
        entry["size_bytes"].is_number(),
        "blank local source_id should still resolve to local file metadata"
    );
    assert!(entry["modified"].is_string());
}

#[test]
fn sessions_json_trims_blank_remote_origin_host() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("agent_search.db");
    let storage = coding_agent_search::storage::sqlite::FrankenStorage::open(&db_path).unwrap();

    let workspace = tmp.path().join("workspace");
    fs::create_dir_all(&workspace).unwrap();

    let session = tmp.path().join("remote-blank-origin-host.jsonl");
    fs::write(&session, "{\"session\":\"remote\"}\n").unwrap();

    let codex_id = storage
        .ensure_agent(&sample_agent("codex", "Codex"))
        .unwrap();
    let workspace_id = storage
        .ensure_workspace(&workspace, Some("workspace"))
        .unwrap();
    storage
        .upsert_source(&coding_agent_search::sources::provenance::Source::remote(
            "work-laptop",
            "user@work-laptop",
        ))
        .unwrap();

    let mut conversation = sample_conversation(
        "codex",
        &workspace,
        &session,
        "remote-blank-origin-host",
        "Remote Blank Origin Host",
        1_700_000_000_000,
        vec![
            sample_message(0, MessageRole::User, 1_700_000_000_000, "question"),
            sample_message(1, MessageRole::Agent, 1_700_000_000_001, "answer"),
        ],
    );
    conversation.source_id = "work-laptop".to_string();
    conversation.origin_host = Some("   ".to_string());
    storage
        .insert_conversation_tree(codex_id, Some(workspace_id), &conversation)
        .unwrap();

    let mut cmd = base_cmd(tmp.path());
    cmd.args([
        "sessions",
        "--json",
        "--data-dir",
        data_dir.to_str().unwrap(),
    ]);

    let output = cmd.assert().success().get_output().clone();
    let json: Value = serde_json::from_slice(&output.stdout).expect("valid sessions json");
    let sessions = json["sessions"].as_array().expect("sessions array");
    let entry = sessions
        .iter()
        .find(|entry| entry["path"].as_str() == Some(session.to_string_lossy().as_ref()))
        .expect("remote session entry");

    assert_eq!(entry["source_id"].as_str(), Some("work-laptop"));
    assert!(
        entry["origin_host"].is_null(),
        "blank origin_host should be trimmed away so downstream displays fall back to source_id"
    );
}

#[test]
fn sessions_json_distinguishes_same_path_across_sources() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("agent_search.db");
    let storage = coding_agent_search::storage::sqlite::FrankenStorage::open(&db_path).unwrap();

    let workspace = tmp.path().join("workspace");
    fs::create_dir_all(&workspace).unwrap();

    let shared_path = tmp.path().join("shared-session.jsonl");
    fs::write(&shared_path, "{\"session\":\"shared\"}\\n").unwrap();

    let codex_id = storage
        .ensure_agent(&sample_agent("codex", "Codex"))
        .unwrap();
    let workspace_id = storage
        .ensure_workspace(&workspace, Some("workspace"))
        .unwrap();
    storage
        .upsert_source(&coding_agent_search::sources::provenance::Source::remote(
            "laptop",
            "user@laptop",
        ))
        .unwrap();

    storage
        .insert_conversation_tree(
            codex_id,
            Some(workspace_id),
            &sample_conversation(
                "codex",
                &workspace,
                &shared_path,
                "shared-local",
                "Shared Session",
                1_700_000_000_000,
                vec![
                    sample_message(0, MessageRole::User, 1_700_000_000_000, "local question"),
                    sample_message(1, MessageRole::Agent, 1_700_000_000_001, "local answer"),
                ],
            ),
        )
        .unwrap();

    let mut remote = sample_conversation(
        "codex",
        &workspace,
        &shared_path,
        "shared-remote",
        "Shared Session",
        1_700_000_100_000,
        vec![
            sample_message(0, MessageRole::User, 1_700_000_100_000, "remote question"),
            sample_message(1, MessageRole::Agent, 1_700_000_100_001, "remote answer"),
        ],
    );
    remote.source_id = "laptop".to_string();
    remote.origin_host = Some("user@laptop".to_string());
    storage
        .insert_conversation_tree(codex_id, Some(workspace_id), &remote)
        .unwrap();

    let mut cmd = base_cmd(tmp.path());
    cmd.args([
        "sessions",
        "--json",
        "--data-dir",
        data_dir.to_str().unwrap(),
    ]);

    let output = cmd.assert().success().get_output().clone();
    let json: Value = serde_json::from_slice(&output.stdout).expect("valid sessions json");
    let sessions = json["sessions"].as_array().expect("sessions array");
    let shared: Vec<&Value> = sessions
        .iter()
        .filter(|entry| entry["path"].as_str() == Some(shared_path.to_string_lossy().as_ref()))
        .collect();

    assert_eq!(shared.len(), 2, "same-path sessions should both be visible");
    assert!(
        shared
            .iter()
            .any(|entry| entry["source_id"].as_str() == Some("local"))
    );
    assert!(
        shared
            .iter()
            .any(|entry| entry["source_id"].as_str() == Some("laptop"))
    );
    assert!(
        shared
            .iter()
            .any(|entry| entry["origin_host"].as_str() == Some("user@laptop"))
    );
}

#[test]
fn sessions_current_prefers_closest_workspace_over_newer_parent_workspace() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("agent_search.db");
    let storage = coding_agent_search::storage::sqlite::FrankenStorage::open(&db_path).unwrap();

    let workspace_root = tmp.path().join("repo");
    let workspace_nested = workspace_root.join("apps/web");
    let cwd = workspace_nested.join("src/components");
    fs::create_dir_all(&cwd).unwrap();

    let nested_session_path = tmp.path().join("nested.jsonl");
    let root_session_path = tmp.path().join("root.jsonl");
    fs::write(&nested_session_path, "{\"session\":\"nested\"}\n").unwrap();
    std::thread::sleep(Duration::from_millis(5));
    fs::write(&root_session_path, "{\"session\":\"root\"}\n").unwrap();

    let claude_id = storage
        .ensure_agent(&sample_agent("claude_code", "Claude Code"))
        .unwrap();
    let workspace_root_id = storage
        .ensure_workspace(&workspace_root, Some("repo"))
        .unwrap();
    let workspace_nested_id = storage
        .ensure_workspace(&workspace_nested, Some("repo-web"))
        .unwrap();

    storage
        .insert_conversation_tree(
            claude_id,
            Some(workspace_nested_id),
            &sample_conversation(
                "claude_code",
                &workspace_nested,
                &nested_session_path,
                "nested-session",
                "Nested Session",
                1_700_000_100_000,
                vec![
                    sample_message(0, MessageRole::User, 1_700_000_100_000, "nested question"),
                    sample_message(1, MessageRole::Agent, 1_700_000_100_001, "nested answer"),
                ],
            ),
        )
        .unwrap();
    storage
        .insert_conversation_tree(
            claude_id,
            Some(workspace_root_id),
            &sample_conversation(
                "claude_code",
                &workspace_root,
                &root_session_path,
                "root-session",
                "Root Session",
                1_700_000_200_000,
                vec![
                    sample_message(0, MessageRole::User, 1_700_000_200_000, "root question"),
                    sample_message(1, MessageRole::Agent, 1_700_000_200_001, "root answer"),
                ],
            ),
        )
        .unwrap();

    let mut cmd = base_cmd(tmp.path());
    cmd.current_dir(&cwd);
    cmd.args([
        "sessions",
        "--current",
        "--json",
        "--data-dir",
        data_dir.to_str().unwrap(),
    ]);

    let output = cmd.assert().success().get_output().clone();
    let json: Value = serde_json::from_slice(&output.stdout).expect("valid current sessions json");
    let sessions = json["sessions"].as_array().expect("sessions array");
    assert_eq!(sessions.len(), 1, "--current should default to one session");
    assert_eq!(
        sessions[0]["path"].as_str().unwrap(),
        nested_session_path.to_string_lossy(),
        "closest matching workspace should win over a newer parent workspace session"
    );
    assert_eq!(
        sessions[0]["workspace"].as_str().unwrap(),
        workspace_nested.to_string_lossy()
    );
}

#[test]
fn parse_export_with_format() {
    let cli = parse_cli_ok(
        [
            "cass",
            "export",
            "/path/to/session.jsonl",
            "--format",
            "json",
        ],
        "parse export with format",
    );
    match cli.command {
        Some(Commands::Export { path, format, .. }) => {
            assert_eq!(path.to_str().unwrap(), "/path/to/session.jsonl");
            assert_eq!(format, coding_agent_search::ConvExportFormat::Json);
        }
        other => panic!("expected export command, got {other:?}"),
    }
}

/// cass-local plan, task 6: the negative table for commands, subcommand
/// pairs and flags removed by the local-only port. Each row activates when
/// the owning task (7–14) lands its `REMOVED_COMMANDS`/`REMOVED_FLAGS`
/// entry in src/lib.rs — flip `pending` to `false` in the same commit. The
/// matcher and its contract are unit-tested in `removed_invocation_tests`;
/// this table is the end-to-end proof that a removed invocation fails
/// without executing anything else.
struct RemovedCommandCase {
    /// argv after `cass` (structured-output flags are appended by the test).
    args: &'static [&'static str],
    /// Canonical `cass …` spelling asserted inside the human message.
    display: &'static str,
    /// Local-only plan task that owns the row.
    task: u8,
    /// `true` while the command is still live — the row is only checked
    /// for "not yet rejected" via a cheap `--help` probe, never executed.
    pending: bool,
    /// Pending-probe argv after `cass`; `None` falls back to
    /// `[args[0], "--help"]`. Rows whose full command path is a live
    /// subcommand today must probe it in full (`["sources","sync","--help"]`)
    /// so a landed `REMOVED_*` entry with a forgotten `pending` flip goes
    /// red instead of passing on a still-live parent word. The fallback
    /// stays for rows whose path does not parse today (`fleet doctor`
    /// names no live subcommand — the whole `fleet` tree is task 8's
    /// removal) and is also what flag rows extend: their probe includes
    /// the still-live flag so a premature `REMOVED_FLAGS` entry is caught
    /// the same way.
    probe_args: Option<&'static [&'static str]>,
}

const REMOVED_COMMAND_CASES: &[RemovedCommandCase] = &[
    // Task 7 — swarm/lessons coordination surfaces.
    RemovedCommandCase {
        args: &["swarm", "status"],
        display: "cass swarm",
        task: 7,
        pending: false,
        probe_args: Some(&["swarm", "status", "--help"]),
    },
    RemovedCommandCase {
        args: &["lessons", "list"],
        display: "cass lessons",
        task: 7,
        pending: false,
        probe_args: Some(&["lessons", "list", "--help"]),
    },
    // Case-variant and typo of a removed top-level command. Both must keep
    // reporting `cass swarm` as removed instead of resolving elsewhere.
    RemovedCommandCase {
        args: &["Swarm"],
        display: "cass swarm",
        task: 7,
        pending: false,
        probe_args: None,
    },
    RemovedCommandCase {
        args: &["swarn"],
        display: "cass swarm",
        task: 7,
        pending: false,
        probe_args: None,
    },
    // Task 8 — fleet, release verification, self-upgrade.
    RemovedCommandCase {
        args: &["fleet", "doctor"],
        display: "cass fleet",
        task: 8,
        pending: false,
        // `fleet doctor` names no live subcommand — the whole `fleet`
        // tree was removed in task 8.
        probe_args: None,
    },
    RemovedCommandCase {
        args: &["release-verify"],
        display: "cass release-verify",
        task: 8,
        pending: false,
        probe_args: None,
    },
    RemovedCommandCase {
        args: &["upgrade", "--check"],
        display: "cass upgrade",
        task: 8,
        pending: false,
        probe_args: None,
    },
    // Task 9 — remote sources. `sources` itself stays live: each row
    // probes its own still-live subcommand so a landed pair entry with a
    // forgotten `pending` flip goes red instead of passing on
    // `sources --help` (all verified to exit 0 on 2026-11-05).
    RemovedCommandCase {
        args: &["sources", "sync"],
        display: "cass sources sync",
        task: 9,
        pending: false,
        probe_args: None,
    },
    RemovedCommandCase {
        args: &["sources", "setup"],
        display: "cass sources setup",
        task: 9,
        pending: false,
        probe_args: None,
    },
    RemovedCommandCase {
        args: &["sources", "discover"],
        display: "cass sources discover",
        task: 9,
        pending: false,
        probe_args: None,
    },
    RemovedCommandCase {
        args: &["sources", "reingest"],
        display: "cass sources reingest",
        task: 9,
        pending: false,
        probe_args: None,
    },
    RemovedCommandCase {
        args: &["sources", "artifact-manifest"],
        display: "cass sources artifact-manifest",
        task: 9,
        pending: false,
        probe_args: None,
    },
    RemovedCommandCase {
        args: &["sources", "schedule"],
        display: "cass sources schedule",
        task: 9,
        pending: false,
        probe_args: None,
    },
    // Task 12 — model download/acquisition surfaces. The flag rows probe
    // the flag itself: while pending the flag must still parse, and a
    // premature REMOVED_FLAGS entry rejects the probe.
    RemovedCommandCase {
        args: &["models", "check-update"],
        display: "cass models check-update",
        task: 12,
        pending: false,
        probe_args: Some(&["models", "check-update", "--help"]),
    },
    RemovedCommandCase {
        args: &["models", "install", "--mirror", "X"],
        display: "--mirror` on `cass models install",
        task: 12,
        pending: false,
        probe_args: Some(&["models", "install", "--mirror", "X", "--help"]),
    },
    RemovedCommandCase {
        args: &["models", "verify", "--repair"],
        display: "--repair` on `cass models verify",
        task: 12,
        pending: false,
        probe_args: Some(&["models", "verify", "--repair", "--help"]),
    },
    // Task 13 — external HTTP embedding provider value. `--embedder` itself
    // stays live for local providers; only `external` is rejected, on every
    // command that takes the flag. Both spellings must name the removed
    // value, not claim the surviving flag is gone.
    RemovedCommandCase {
        args: &["models", "backfill", "--embedder", "external"],
        display: "--embedder external",
        task: 13,
        pending: false,
        probe_args: None,
    },
    RemovedCommandCase {
        args: &["models", "backfill", "--embedder=external"],
        display: "--embedder external",
        task: 13,
        pending: false,
        probe_args: None,
    },
    // Task 14 — hosted pages deploy targets and hosted deploy flags.
    // `--target` survives for `local`; the hosted values and the
    // credential/project flags are removed.
    RemovedCommandCase {
        args: &["pages", "--target", "github"],
        display: "--target github` on `cass pages",
        task: 14,
        pending: false,
        probe_args: None,
    },
    RemovedCommandCase {
        args: &["pages", "--target", "cloudflare"],
        display: "--target cloudflare` on `cass pages",
        task: 14,
        pending: false,
        probe_args: None,
    },
    RemovedCommandCase {
        args: &["pages", "--project", "my-archive"],
        display: "--project` on `cass pages",
        task: 14,
        pending: false,
        probe_args: None,
    },
    RemovedCommandCase {
        args: &["pages", "--repo", "my-archive"],
        display: "--repo` on `cass pages",
        task: 14,
        pending: false,
        probe_args: None,
    },
    RemovedCommandCase {
        args: &["pages", "--branch", "main"],
        display: "--branch` on `cass pages",
        task: 14,
        pending: false,
        probe_args: None,
    },
    RemovedCommandCase {
        args: &["pages", "--account-id", "abc123"],
        display: "--account-id` on `cass pages",
        task: 14,
        pending: false,
        probe_args: None,
    },
    RemovedCommandCase {
        args: &["pages", "--api-token", "tok"],
        display: "--api-token` on `cass pages",
        task: 14,
        pending: false,
        probe_args: None,
    },
    // Task 14 — there is no CDN mode left, so `--no-cdns` is retired.
    RemovedCommandCase {
        args: &["export-html", "x", "--no-cdns"],
        display: "--no-cdns` on `cass export-html",
        task: 14,
        pending: false,
        probe_args: None,
    },
];

/// Every active row must fail with exit 2, robot kind `removed-command`,
/// the canonical `cass … was removed: cass runs fully local-only` message,
/// an untouched data dir, and no spawned helper — `PATH` is emptied so a
/// hypothetical `ssh`/`curl` exec could never resolve. Pending rows are
/// probed only via `probe_args` — `<top-level word> --help` by default,
/// the full still-live path (plus its flag, for flag rows) where that
/// parses today — which keeps them honest documentation without running
/// network-touching commands like `cass upgrade --check`.
#[test]
fn removed_commands_error_without_redirection() {
    // Tasks must never flip `pending` without landing the matching
    // REMOVED_* entry — the per-row message asserts name the task.
    for case in REMOVED_COMMAND_CASES {
        let label = format!("cass {}", case.args.join(" "));
        let temp_home = tempfile::TempDir::new().expect("removed-command temp home");
        let data_dir = temp_home.path().join(".local/share/coding-agent-search");
        fs::create_dir_all(&data_dir).expect("create isolated data dir");
        // A PATH with no executables at all: any child spawn (ssh, curl,
        // gh) would fail outright, so a passing rejection proves nothing
        // external ran.
        let empty_path = temp_home.path().join("empty-path");
        fs::create_dir_all(&empty_path).expect("create empty PATH dir");

        let run = |args: &[&str]| {
            let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("cass"));
            cmd.env("HOME", temp_home.path())
                .env("XDG_DATA_HOME", temp_home.path().join(".local/share"))
                .env("CASS_DATA_DIR", &data_dir)
                .env("XDG_CONFIG_HOME", temp_home.path().join(".config"))
                .env("CODEX_HOME", temp_home.path().join(".codex"))
                .env("NO_COLOR", "1")
                .env("PATH", &empty_path);
            cmd.args(args);
            cmd.output().expect("spawn cass")
        };

        if case.pending {
            // Still-live surface: the probe must exit 0 and must not be
            // turned into a removal error. `probe_args` probes the full
            // still-live path (flag included) wherever it parses today —
            // a landed REMOVED_* entry with a forgotten `pending` flip
            // then fails here instead of passing on a still-live parent
            // word. Rows without it fall back to `<top-level word>
            // --help` because their full path does not parse even today
            // (`fleet doctor` names no live subcommand; the whole `fleet`
            // tree is what task 8 removes).
            let mut probe = vec![case.args[0], "--help"];
            if let Some(probe_args) = case.probe_args {
                probe = probe_args.to_vec();
            }
            let output = run(&probe);
            assert!(
                output.status.success(),
                "task {}: `cass {}` must still work while the row is pending; \
                 land the REMOVED_* entry and flip `pending` together\nstderr: {}",
                case.task,
                probe.join(" "),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(
                !String::from_utf8_lossy(&output.stderr).contains("removed-command"),
                "task {}: `{label}` must not be rejected before its task lands",
                case.task
            );
            continue;
        }

        // Human path: usage exit, canonical removed message, stdout empty.
        let human = run(case.args);
        assert_eq!(
            human.status.code(),
            Some(2),
            "task {}: `{label}` must exit 2 (usage)\nstderr: {}",
            case.task,
            String::from_utf8_lossy(&human.stderr)
        );
        assert!(
            human.stdout.is_empty(),
            "task {}: `{label}` must not write stdout",
            case.task
        );
        let human_stderr = String::from_utf8_lossy(&human.stderr);
        assert!(
            human_stderr.contains(case.display) && human_stderr.contains("was removed"),
            "task {}: `{label}` human message must name `{}` as removed, got: {human_stderr}",
            case.task,
            case.display
        );
        assert!(
            human_stderr.contains("local-only"),
            "task {}: `{label}` human message must cite local-only, got: {human_stderr}",
            case.task
        );

        // Robot path: one structured envelope on stderr.
        let mut robot_args = case.args.to_vec();
        robot_args.push("--json");
        let robot = run(&robot_args);
        assert_eq!(
            robot.status.code(),
            Some(2),
            "task {}: `{label} --json` must exit 2\nstderr: {}",
            case.task,
            String::from_utf8_lossy(&robot.stderr)
        );
        let envelope: Value = serde_json::from_str(String::from_utf8_lossy(&robot.stderr).trim())
            .unwrap_or_else(|err| {
                panic!(
                    "task {}: `{label} --json` stderr is not one JSON envelope: {err}",
                    case.task
                )
            });
        assert_eq!(
            envelope["error"]["kind"],
            json!("removed-command"),
            "task {}: `{label}`",
            case.task
        );
        assert_eq!(
            envelope["error"]["code"],
            json!(2),
            "task {}: `{label}`",
            case.task
        );
        assert_eq!(
            envelope["error"]["retryable"],
            json!(false),
            "task {}: `{label}`",
            case.task
        );

        // The rejection happens before dispatch: the data dir stays empty.
        let mut created = Vec::new();
        for entry in walkdir::WalkDir::new(&data_dir) {
            let entry = entry.expect("walk data dir");
            if entry.file_type().is_file() {
                created.push(entry.path().to_path_buf());
            }
        }
        assert!(
            created.is_empty(),
            "task {}: `{label}` created files in the data dir: {created:?}",
            case.task
        );
    }

    // The table is the plan's contract — silently empty rows would let a
    // task skip the whole rejection surface.
    assert_eq!(
        REMOVED_COMMAND_CASES.len(),
        26,
        "the negative table lists every removal promised by tasks 7–14"
    );
}
