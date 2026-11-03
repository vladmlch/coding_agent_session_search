//! Provider end-to-end contracts for index-first export — SDD Task 6.
//!
//! Plan: `docs/superpowers/plans/2026-11-02-index-first-export.md`
//! Brief: `.superpowers/sdd/2026-11-02-index-first-export/task-6-brief.md`
//!
//! The contract under test: `cass export` and `cass export-html` resolve the
//! supplied path as an *opaque identity* against the canonical archive and
//! render only archived, provider-neutral messages (role, content, timestamp,
//! author, retained metadata). Export must never re-read the source file,
//! invoke a provider connector, decompress `.zst` data, or decode a
//! provider-native store, and must keep working — byte-for-byte — after the
//! original source is renamed or deleted.
//!
//! Each provider is indexed hermetically through
//! `run_index_with_local_connector_roots` into an isolated `TempDir` archive,
//! then exported through the real `cass` binary (`--db` pointed at that
//! archive). Assertions check actual roles, body text, and message order —
//! never merely exit status — and record which contract points the current
//! implementation violates (see task-6-report.md for the RED/GREEN baseline).

use assert_cmd::Command;
use coding_agent_search::indexer::{IndexOptions, run_index_with_local_connector_roots};
use coding_agent_search::model::types::{Message, MessageRole};
use coding_agent_search::storage::sqlite::SqliteStorage;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Output;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// New Task-6 fixtures (unique names under `export_index_providers/`).
const CLAUDE_FIXTURE: &str =
    "tests/fixtures/export_index_providers/claude/projects/-e2e-p6/claude-export-p6.jsonl";
const CODEX_MODERN_FIXTURE: &str =
    "tests/fixtures/export_index_providers/codex/sessions/2026/11/02/rollout-e2e-p6-modern.jsonl";
const CODEX_LEGACY_FIXTURE: &str =
    "tests/fixtures/export_index_providers/codex/sessions/2026/11/02/rollout-e2e-p6-legacy.json";

/// Existing Antigravity transcript fixtures, reused verbatim.
const AGY_MAIN_DIR: &str = "tests/fixtures/antigravity/brain/aaaa1111-bbbb-2222-cccc-333344445555";
const AGY_CALLS_DIR: &str =
    "tests/fixtures/antigravity_planner_calls/brain/bbbb2222-cccc-3333-dddd-444455556666";
const AGY_MAIN_UUID: &str = "aaaa1111-bbbb-2222-cccc-333344445555";
const AGY_CALLS_UUID: &str = "bbbb2222-cccc-3333-dddd-444455556666";

// ---------------------------------------------------------------------------
// Indexing helpers
// ---------------------------------------------------------------------------

/// Run a closed-world index into `tmp/cass-data/agent_search.db`, scanning
/// exactly the per-connector roots given. Returns the db path.
fn run_isolated_index(tmp: &TempDir, roots: HashMap<String, Vec<PathBuf>>) -> PathBuf {
    let data_dir = tmp.path().join("cass-data");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("agent_search.db");
    let opts = IndexOptions {
        full: true,
        force_rebuild: false,
        watch: false,
        watch_once_paths: None,
        db_path: db_path.clone(),
        data_dir,
        semantic: false,
        build_hnsw: false,
        embedder: "hash".to_string(),
        progress: None,
        watch_interval_secs: 30,
    };
    run_index_with_local_connector_roots(opts, roots, None)
        .expect("isolated index run should succeed");
    db_path
}

#[derive(Debug)]
struct ArchivedConvo {
    agent_slug: String,
    source_path: String,
    title: Option<String>,
    messages: Vec<Message>,
}

/// Read back every archived conversation *with its messages* directly from the
/// canonical archive. This is the index-side evidence: what actually got
/// retained, independent of any export code path.
fn archived_convos(db_path: &Path) -> Vec<ArchivedConvo> {
    let storage =
        SqliteStorage::open_readonly(db_path).expect("open archive read-only for inspection");
    let convs = storage
        .list_conversations(1000, 0)
        .expect("list archived conversations");
    let mut out = Vec::with_capacity(convs.len());
    for c in &convs {
        let id = c.id.expect("archived conversation id");
        let messages = storage.fetch_messages(id).expect("fetch archived messages");
        out.push(ArchivedConvo {
            agent_slug: c.agent_slug.clone(),
            source_path: c.source_path.to_string_lossy().into_owned(),
            title: c.title.clone(),
            messages,
        });
    }
    storage.close().expect("close readonly archive");
    out
}

fn find_convo<'a>(convos: &'a [ArchivedConvo], needle: &str) -> &'a ArchivedConvo {
    convos
        .iter()
        .find(|c| c.source_path.contains(needle))
        .unwrap_or_else(|| {
            panic!(
                "no archived conversation with source_path containing '{needle}'; got: {:?}",
                convos
                    .iter()
                    .map(|c| (c.agent_slug.clone(), c.source_path.clone()))
                    .collect::<Vec<_>>()
            )
        })
}

fn role_name(role: &MessageRole) -> String {
    match role {
        MessageRole::User => "user".to_string(),
        MessageRole::Agent => "assistant".to_string(),
        MessageRole::Tool => "tool".to_string(),
        MessageRole::System => "system".to_string(),
        MessageRole::Other(s) => s.clone(),
    }
}

fn archived_roles(convo: &ArchivedConvo) -> Vec<String> {
    convo.messages.iter().map(|m| role_name(&m.role)).collect()
}

// ---------------------------------------------------------------------------
// `cass` subprocess helpers
// ---------------------------------------------------------------------------

/// A `cass` invocation with an isolated HOME and no inherited index state, so
/// the only archive it can see is the `--db` we pass. `current_dir(home)`
/// keeps the repo's `.env` out of the child.
fn cass_cmd(home: &Path) -> Command {
    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("cass"));
    cmd.current_dir(home)
        .env("CODING_AGENT_SEARCH_NO_UPDATE_PROMPT", "1")
        .env("CASS_AUTO_REFRESH", "0")
        .env("HOME", home)
        .env("XDG_CONFIG_HOME", home.join("xdg-config"))
        .env("XDG_DATA_HOME", home.join("xdg-data"))
        .env_remove("CASS_DB_PATH")
        .env_remove("CASS_DATA_DIR")
        .env_remove("CASS_OUTPUT_FORMAT")
        .env_remove("CASS_ROBOT_FORMAT")
        .env_remove("CASS_EXCLUDE_PATHS")
        .env_remove("CASS_EXCLUDE_WORKSPACES")
        .env_remove("CLAUDE_CONFIG_DIR")
        .env_remove("CODEX_HOME")
        .env_remove("CASS_ANTIGRAVITY_DATA_ROOT")
        .env_remove("OPENCODE_SQLITE_DB");
    cmd
}

fn cass_export(
    db: &Path,
    identity: &str,
    format: &str,
    include_tools: bool,
    home: &Path,
) -> Output {
    let mut cmd = cass_cmd(home);
    cmd.arg("--db")
        .arg(db)
        .arg("export")
        .arg(identity)
        .arg("--format")
        .arg(format);
    if include_tools {
        cmd.arg("--include-tools");
    }
    cmd.output().expect("spawn cass export")
}

/// `cass export-html --json`, returning the output plus the parsed
/// `exported.output_path` when the run succeeded.
fn cass_export_html(
    db: &Path,
    identity: &str,
    out_dir: &Path,
    filename: &str,
    include_tools: bool,
    home: &Path,
) -> (Output, Option<PathBuf>) {
    let mut cmd = cass_cmd(home);
    cmd.arg("--db")
        .arg(db)
        .arg("export-html")
        .arg(identity)
        .arg("--output-dir")
        .arg(out_dir)
        .arg("--filename")
        .arg(filename)
        .arg(format!("--include-tools={include_tools}"))
        .arg("--json");
    let output = cmd.output().expect("spawn cass export-html");
    if !output.status.success() {
        return (output, None);
    }
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let envelope: Option<PathBuf> = serde_json::from_str::<Value>(stdout.trim())
        .ok()
        .and_then(|v| v["exported"]["output_path"].as_str().map(PathBuf::from));
    (output, envelope)
}

// ---------------------------------------------------------------------------
// Assertion helpers
// ---------------------------------------------------------------------------

fn stdout_of(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr_of(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn expect_success(output: &Output) -> String {
    assert!(
        output.status.success(),
        "expected exit 0, got {:?}; stderr:\n{}",
        output.status.code(),
        stderr_of(output)
    );
    stdout_of(output)
}

/// Assert that `markers` appear in `body` in exactly this order.
fn assert_order(body: &str, markers: &[&str]) {
    let mut offset = 0usize;
    for marker in markers {
        let found = body[offset..]
            .find(marker)
            .unwrap_or_else(|| panic!("marker '{marker}' missing or out of order;\nbody:\n{body}"));
        offset += found + marker.len();
    }
}

fn json_messages(stdout: &str) -> Vec<Value> {
    let value: Value = serde_json::from_str(stdout.trim())
        .expect("`--format json` stdout must be a JSON array of messages");
    value
        .as_array()
        .expect("`--format json` stdout must be a JSON array")
        .clone()
}

fn json_roles(stdout: &str) -> Vec<String> {
    json_messages(stdout)
        .iter()
        .map(|m| {
            m["role"]
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| panic!("exported message missing `role`: {m}"))
        })
        .collect()
}

fn count_occurrences(haystack: &str, needle: &str) -> usize {
    haystack.matches(needle).count()
}

// ---------------------------------------------------------------------------
// Claude Code
// ---------------------------------------------------------------------------

/// Build the claude fixture tree inside `tmp`, index it, and return
/// `(db_path, source_path, ArchivedConvo-snapshot)`. The snapshot captures the
/// archived messages so tests can cite index-side evidence.
fn index_claude(tmp: &TempDir) -> (PathBuf, PathBuf, Vec<ArchivedConvo>) {
    let claude_home = tmp.path().join("claude-home");
    let session_dir = claude_home.join("projects").join("-e2e-p6");
    fs::create_dir_all(&session_dir).unwrap();
    let session_file = session_dir.join("claude-export-p6.jsonl");
    fs::copy(CLAUDE_FIXTURE, &session_file).expect("copy claude fixture");

    let db = run_isolated_index(
        tmp,
        HashMap::from([("claude".to_string(), vec![claude_home])]),
    );
    let convos = archived_convos(&db);
    assert_eq!(
        convos.len(),
        1,
        "exactly one claude conversation must be archived, got {:?}",
        convos.iter().map(|c| &c.source_path).collect::<Vec<_>>()
    );
    assert_eq!(convos[0].agent_slug, "claude_code");
    (db, session_file, convos)
}

#[test]
fn claude_export_uses_indexed_conversation_for_all_formats() {
    let tmp = TempDir::new().unwrap();
    let home = tmp.path().join("home");
    fs::create_dir_all(&home).unwrap();
    let (db, _session_file, convos) = index_claude(&tmp);
    let convo = &convos[0];

    // Index-side evidence: the source events normalize to
    // user → assistant(text+tool_use) → tool(tool_result) → assistant.
    assert_eq!(
        archived_roles(convo),
        ["user", "assistant", "tool", "assistant"],
        "archived role/order for claude fixture"
    );
    assert!(convo.messages[0].content.contains("P6CLAUDEUSER"));
    assert!(convo.messages[1].content.contains("P6CLAUDEASSIST"));
    // The assistant's tool_use block must survive in retained metadata so a
    // projection can render the call without re-reading the source file.
    assert!(
        convo.messages[1]
            .extra_json
            .to_string()
            .contains("toolu_p6_read"),
        "assistant tool_use must be retained in extra_json: {}",
        convo.messages[1].extra_json
    );
    assert_eq!(
        convo.messages[2].extra_json["source"].as_str(),
        Some("tool_result"),
        "tool-result provenance must be retained in extra_json"
    );
    assert_eq!(
        convo.messages[2].extra_json["tool_use_id"].as_str(),
        Some("toolu_p6_read")
    );
    assert!(convo.messages[3].content.contains("P6CLAUDEFINAL"));

    let identity = convo.source_path.clone();

    // --- Markdown (default: tools excluded) ---
    let out = cass_export(&db, &identity, "markdown", false, &home);
    let md = expect_success(&out);
    assert!(
        md.contains('#'),
        "markdown export must not be empty/header-only"
    );
    assert!(md.contains("P6CLAUDEUSER"), "user body missing:\n{md}");
    assert!(
        md.contains("P6CLAUDEASSIST"),
        "assistant body missing:\n{md}"
    );
    assert!(md.contains("P6CLAUDEFINAL"), "final body missing:\n{md}");
    assert_eq!(
        count_occurrences(&md, "## 👤 User"),
        1,
        "exactly one user turn may render — a tool_result record is not a user turn:\n{md}"
    );
    assert!(
        !md.contains("P6CLAUDETOOLRESULT"),
        "tool-result body must be excluded without --include-tools:\n{md}"
    );
    assert!(
        !md.contains("## tool"),
        "tool-role section must be excluded without --include-tools:\n{md}"
    );
    assert!(
        !md.contains("P6CLAUDESUMMARY"),
        "session summary records are metadata, not timeline messages:\n{md}"
    );
    assert_order(&md, &["P6CLAUDEUSER", "P6CLAUDEASSIST", "P6CLAUDEFINAL"]);

    // --- Markdown with --include-tools ---
    let out = cass_export(&db, &identity, "markdown", true, &home);
    let md = expect_success(&out);
    assert!(
        md.contains("P6CLAUDETOOLRESULT"),
        "--include-tools must render the indexed tool message:\n{md}"
    );
    assert!(
        md.contains("## tool"),
        "indexed tool message must render under a tool role section:\n{md}"
    );
    assert_order(
        &md,
        &[
            "P6CLAUDEUSER",
            "P6CLAUDEASSIST",
            "P6CLAUDETOOLRESULT",
            "P6CLAUDEFINAL",
        ],
    );

    // --- JSON ---
    let out = cass_export(&db, &identity, "json", true, &home);
    let json = expect_success(&out);
    assert_eq!(
        json_roles(&json),
        ["user", "assistant", "tool", "assistant"],
        "JSON export must emit the archived roles in order: {json}"
    );
    let msgs = json_messages(&json);
    assert!(
        msgs[0]["content"]
            .as_str()
            .unwrap_or("")
            .contains("P6CLAUDEUSER")
    );
    assert!(
        msgs[2]["content"]
            .as_str()
            .unwrap_or("")
            .contains("P6CLAUDETOOLRESULT")
    );

    let out = cass_export(&db, &identity, "json", false, &home);
    let json = expect_success(&out);
    assert_eq!(
        json_roles(&json),
        ["user", "assistant", "assistant"],
        "tool-role messages must be excluded without --include-tools: {json}"
    );
    assert!(
        !json.contains("P6CLAUDETOOLRESULT"),
        "tool body must not leak into default JSON export: {json}"
    );

    // --- HTML via export-html (defaults include tools) ---
    let out_dir = tmp.path().join("html-out");
    fs::create_dir_all(&out_dir).unwrap();
    let (out, output_path) = cass_export_html(&db, &identity, &out_dir, "claude-all", true, &home);
    assert!(
        out.status.success(),
        "export-html failed: {}",
        stderr_of(&out)
    );
    let path = output_path.expect("export-html should report exported.output_path");
    let html = fs::read_to_string(&path).expect("read exported HTML");
    assert!(html.contains("P6CLAUDEUSER"), "user body missing from HTML");
    assert!(
        html.contains("P6CLAUDEASSIST"),
        "assistant body missing from HTML"
    );
    assert!(
        html.contains("P6CLAUDETOOLRESULT"),
        "tool result must render in HTML when tools are included"
    );
    assert!(
        !html.contains("P6CLAUDESUMMARY"),
        "summary records must not render"
    );

    let (out, output_path) =
        cass_export_html(&db, &identity, &out_dir, "claude-notools", false, &home);
    assert!(
        out.status.success(),
        "export-html failed: {}",
        stderr_of(&out)
    );
    let path = output_path.expect("exported.output_path");
    let html = fs::read_to_string(&path).expect("read exported HTML");
    assert!(html.contains("P6CLAUDEUSER"));
    assert!(
        !html.contains("P6CLAUDETOOLRESULT"),
        "tool result must be omitted with --include-tools=false"
    );
}

#[test]
fn claude_export_is_byte_stable_after_source_rename() {
    let tmp = TempDir::new().unwrap();
    let home = tmp.path().join("home");
    fs::create_dir_all(&home).unwrap();
    let (db, session_file, convos) = index_claude(&tmp);
    let identity = convos[0].source_path.clone();

    // Export while the source file still exists.
    let live = cass_export(&db, &identity, "markdown", true, &home);
    let live_md = expect_success(&live);

    // Rename the source away (non-destructive: the bytes still exist under a
    // different name, which is exactly the "source moved/renamed" case).
    let moved = session_file.with_extension("jsonl.moved");
    fs::rename(&session_file, &moved).expect("rename source file");
    assert!(!session_file.exists());

    let after = cass_export(&db, &identity, "markdown", true, &home);
    let after_md = expect_success(&after);

    assert_eq!(
        after_md, live_md,
        "export must be byte-stable whether or not the original source file exists"
    );
    assert!(after_md.contains("P6CLAUDEUSER"));
    assert!(after_md.contains("P6CLAUDETOOLRESULT"));

    // JSON surface must also be stable.
    let live_json = expect_success(&cass_export(&db, &identity, "json", true, &home));
    let after_json = expect_success(&cass_export(&db, &identity, "json", true, &home));
    assert_eq!(
        after_json, live_json,
        "JSON export must be source-independent"
    );
    assert_eq!(
        json_roles(&after_json),
        ["user", "assistant", "tool", "assistant"]
    );
}

// ---------------------------------------------------------------------------
// Codex
// ---------------------------------------------------------------------------

fn index_codex(tmp: &TempDir, files: &[(&str, &[u8])]) -> (PathBuf, PathBuf, Vec<ArchivedConvo>) {
    let codex_home = tmp.path().join("codex-home");
    let day_dir = codex_home
        .join("sessions")
        .join("2026")
        .join("11")
        .join("02");
    fs::create_dir_all(&day_dir).unwrap();
    for (name, bytes) in files {
        fs::write(day_dir.join(name), bytes).expect("write codex fixture");
    }
    let db = run_isolated_index(
        tmp,
        HashMap::from([("codex".to_string(), vec![codex_home])]),
    );
    let convos = archived_convos(&db);
    for c in &convos {
        assert_eq!(c.agent_slug, "codex");
    }
    (db, day_dir, convos)
}

fn codex_zst_bytes() -> Vec<u8> {
    let plain = fs::read(CODEX_MODERN_FIXTURE).expect("read modern fixture");
    zstd::stream::encode_all(&plain[..], 0).expect("compress rollout")
}

#[test]
fn codex_export_uses_indexed_conversation_for_all_formats() {
    let tmp = TempDir::new().unwrap();
    let home = tmp.path().join("home");
    fs::create_dir_all(&home).unwrap();
    let modern_bytes = fs::read(CODEX_MODERN_FIXTURE).unwrap();
    let (db, _day_dir, convos) = index_codex(
        &tmp,
        &[("rollout-e2e-p6-modern.jsonl", modern_bytes.as_slice())],
    );
    assert_eq!(convos.len(), 1);
    let convo = &convos[0];

    // Index-side evidence: session_meta dropped; user → reasoning(assistant)
    // → tool_call(assistant `[Tool: bash]`) → tool(output) → assistant.
    assert_eq!(
        archived_roles(convo),
        ["user", "assistant", "assistant", "tool", "assistant"],
        "archived role/order for codex modern rollout"
    );
    assert_eq!(
        convo.messages[1].author.as_deref(),
        Some("reasoning"),
        "agent_reasoning events index as assistant messages authored 'reasoning'"
    );
    assert!(convo.messages[1].content.contains("P6CODEXREASON"));
    assert!(
        convo.messages[2].content.contains("[Tool: bash]"),
        "tool_call event must index as an assistant tool-call marker: {}",
        convo.messages[2].content
    );
    assert!(
        convo.messages[2]
            .extra_json
            .to_string()
            .contains("call_p6_bash"),
        "tool_call linkage must be retained in extra_json"
    );
    assert!(convo.messages[3].content.contains("P6CODEXTOOLOUT"));

    let identity = convo.source_path.clone();

    // --- Markdown ---
    let md = expect_success(&cass_export(&db, &identity, "markdown", true, &home));
    assert!(md.contains("P6CODEXUSER"), "user body missing:\n{md}");
    assert!(
        md.contains("P6CODEXASSIST"),
        "assistant body missing:\n{md}"
    );
    assert!(
        md.contains("P6CODEXREASON"),
        "indexed reasoning message must be exported:\n{md}"
    );
    assert!(
        md.contains("P6CODEXTOOLOUT"),
        "tool output must render with --include-tools:\n{md}"
    );
    assert_order(
        &md,
        &[
            "P6CODEXUSER",
            "P6CODEXREASON",
            "P6CODEXTOOLOUT",
            "P6CODEXASSIST",
        ],
    );

    let md = expect_success(&cass_export(&db, &identity, "markdown", false, &home));
    assert!(md.contains("P6CODEXUSER"));
    assert!(
        !md.contains("P6CODEXTOOLOUT"),
        "tool output must be excluded without --include-tools:\n{md}"
    );
    assert!(
        !md.contains("## tool"),
        "no tool-role section without --include-tools:\n{md}"
    );

    // --- JSON ---
    let json = expect_success(&cass_export(&db, &identity, "json", true, &home));
    assert_eq!(
        json_roles(&json),
        ["user", "assistant", "assistant", "tool", "assistant"],
        "JSON export must emit archived roles in order: {json}"
    );
    let msgs = json_messages(&json);
    assert_eq!(
        msgs[1]["author"].as_str(),
        Some("reasoning"),
        "reasoning author must reach the JSON export"
    );

    let json = expect_success(&cass_export(&db, &identity, "json", false, &home));
    assert_eq!(
        json_roles(&json),
        ["user", "assistant", "assistant", "assistant"],
        "default JSON must exclude the tool-role message: {json}"
    );

    // --- HTML ---
    let out_dir = tmp.path().join("html-out");
    fs::create_dir_all(&out_dir).unwrap();
    let (out, output_path) = cass_export_html(&db, &identity, &out_dir, "codex-all", true, &home);
    assert!(
        out.status.success(),
        "export-html failed: {}",
        stderr_of(&out)
    );
    let path = output_path.expect("exported.output_path");
    let html = fs::read_to_string(&path).expect("read exported HTML");
    assert!(html.contains("P6CODEXUSER"));
    assert!(html.contains("P6CODEXASSIST"));
    assert!(
        html.contains("P6CODEXTOOLOUT"),
        "tool output must render in HTML when tools are included"
    );

    let (out, output_path) =
        cass_export_html(&db, &identity, &out_dir, "codex-notools", false, &home);
    assert!(
        out.status.success(),
        "export-html failed: {}",
        stderr_of(&out)
    );
    let path = output_path.expect("exported.output_path");
    let html = fs::read_to_string(&path).expect("read exported HTML");
    assert!(html.contains("P6CODEXUSER"));
    assert!(
        !html.contains("P6CODEXTOOLOUT"),
        "tool output must be omitted with --include-tools=false"
    );
}

#[test]
fn codex_export_is_byte_stable_after_source_removal() {
    let tmp = TempDir::new().unwrap();
    let home = tmp.path().join("home");
    fs::create_dir_all(&home).unwrap();
    let modern_bytes = fs::read(CODEX_MODERN_FIXTURE).unwrap();
    let (db, day_dir, convos) = index_codex(
        &tmp,
        &[("rollout-e2e-p6-modern.jsonl", modern_bytes.as_slice())],
    );
    let convo = find_convo(&convos, "rollout-e2e-p6-modern");
    let identity = convo.source_path.clone();

    let live_md = expect_success(&cass_export(&db, &identity, "markdown", true, &home));

    let moved = day_dir.join("rollout-e2e-p6-modern.jsonl.moved");
    fs::rename(day_dir.join("rollout-e2e-p6-modern.jsonl"), &moved).expect("rename rollout");

    let after_md = expect_success(&cass_export(&db, &identity, "markdown", true, &home));
    assert_eq!(
        after_md, live_md,
        "export must be byte-stable once the source is gone"
    );
    assert!(after_md.contains("P6CODEXUSER"));
    assert!(after_md.contains("P6CODEXTOOLOUT"));
}

#[test]
fn codex_rollout_variants_export_equivalent_from_archive() {
    // The same dialogue stored as modern JSONL, a `.jsonl.zst` copy, and a
    // legacy single-document rollout must produce equivalent exports from the
    // archive — with no decompression or provider-format parsing at export
    // time. The legacy schema has no reasoning/tool events, so it is asserted
    // on the dialogue subset it can express.
    let tmp = TempDir::new().unwrap();
    let home = tmp.path().join("home");
    fs::create_dir_all(&home).unwrap();
    let modern_bytes = fs::read(CODEX_MODERN_FIXTURE).unwrap();
    let zst_bytes = codex_zst_bytes();
    let legacy_bytes = fs::read(CODEX_LEGACY_FIXTURE).unwrap();
    let (db, day_dir, convos) = index_codex(
        &tmp,
        &[
            ("rollout-e2e-p6-modern.jsonl", modern_bytes.as_slice()),
            ("rollout-e2e-p6-zst.jsonl.zst", zst_bytes.as_slice()),
            ("rollout-e2e-p6-legacy.json", legacy_bytes.as_slice()),
        ],
    );
    assert_eq!(
        convos.len(),
        3,
        "all three rollout representations must be archived: {:?}",
        convos.iter().map(|c| &c.source_path).collect::<Vec<_>>()
    );

    let modern = find_convo(&convos, "rollout-e2e-p6-modern");
    let zst = find_convo(&convos, "rollout-e2e-p6-zst");
    let legacy = find_convo(&convos, "rollout-e2e-p6-legacy");

    // The archive must already carry the same normalized dialogue for the
    // modern and compressed representations.
    assert_eq!(
        archived_roles(modern),
        archived_roles(zst),
        "modern vs zst must archive to the same role sequence"
    );
    assert_eq!(
        archived_roles(legacy),
        ["user", "assistant"],
        "legacy schema archives the dialogue subset"
    );

    // Remove every source representation; exports must still be equivalent.
    fs::rename(
        day_dir.join("rollout-e2e-p6-modern.jsonl"),
        day_dir.join("rollout-e2e-p6-modern.jsonl.moved"),
    )
    .unwrap();
    fs::rename(
        day_dir.join("rollout-e2e-p6-zst.jsonl.zst"),
        day_dir.join("rollout-e2e-p6-zst.jsonl.zst.moved"),
    )
    .unwrap();
    fs::rename(
        day_dir.join("rollout-e2e-p6-legacy.json"),
        day_dir.join("rollout-e2e-p6-legacy.json.moved"),
    )
    .unwrap();

    let modern_md = expect_success(&cass_export(
        &db,
        &modern.source_path,
        "markdown",
        true,
        &home,
    ));
    let zst_md = expect_success(&cass_export(&db, &zst.source_path, "markdown", true, &home));
    assert_eq!(
        modern_md, zst_md,
        "compressed and plain representations of the same dialogue must export identically"
    );

    let modern_json = expect_success(&cass_export(&db, &modern.source_path, "json", true, &home));
    let zst_json = expect_success(&cass_export(&db, &zst.source_path, "json", true, &home));
    assert_eq!(
        modern_json, zst_json,
        "JSON export must be representation-independent"
    );

    let legacy_md = expect_success(&cass_export(
        &db,
        &legacy.source_path,
        "markdown",
        true,
        &home,
    ));
    assert!(legacy_md.contains("P6CODEXUSER"));
    assert!(legacy_md.contains("P6CODEXASSIST"));
}

// ---------------------------------------------------------------------------
// Devin
// ---------------------------------------------------------------------------

/// A devin-style `sessions.db` containing:
///  * `devin-alpha` — main chain user → assistant(tool_call) → tool → assistant,
///    plus a system node (not indexed) and an abandoned branch node
///  * `devin-beta` — an unrelated two-message chain
///  * `devin-hidden` — `hidden=1`, never indexed
///  * OpenCode-shaped `session`/`session_message` decoy tables holding sessions
///    with the SAME ids `devin-alpha`/`devin-beta` but clearly different
///    `P6OPENCODEDECOY` bodies, to prove which store export actually consulted.
fn write_devin_fixture(db_file: &Path) {
    let conn = rusqlite::Connection::open(db_file).expect("open devin fixture db");
    conn.execute_batch(
        "CREATE TABLE sessions (
            id TEXT PRIMARY KEY,
            title TEXT,
            working_directory TEXT,
            model TEXT,
            agent_mode TEXT,
            created_at INTEGER,
            last_activity_at INTEGER,
            main_chain_id INTEGER,
            hidden INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE message_nodes (
            node_id INTEGER PRIMARY KEY,
            session_id TEXT NOT NULL,
            parent_node_id INTEGER,
            chat_message TEXT,
            created_at INTEGER
        );
        -- OpenCode 2.x-shaped decoy tables (same file, different store).
        CREATE TABLE session (
            id TEXT PRIMARY KEY,
            title TEXT,
            directory TEXT,
            project_id TEXT,
            time_created INTEGER,
            time_updated INTEGER
        );
        CREATE TABLE session_message (
            id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            type TEXT NOT NULL,
            seq INTEGER NOT NULL,
            time_created INTEGER NOT NULL,
            time_updated INTEGER NOT NULL,
            data TEXT NOT NULL
        );",
    )
    .expect("create devin fixture schema");

    let chat = |v: Value| v.to_string();
    let ins = |session: &str, node: i64, parent: Option<i64>, msg: Value| {
        conn.execute(
            "INSERT INTO message_nodes (node_id, session_id, parent_node_id, chat_message, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![node, session, parent, chat(msg), 1_762_003_200i64 + node],
        )
        .expect("insert message node");
    };

    // devin-alpha main chain: nodes 1..5; abandoned sibling node 6 hangs off 2.
    conn.execute(
        "INSERT INTO sessions (id, title, working_directory, model, agent_mode, created_at, last_activity_at, main_chain_id, hidden)
         VALUES ('devin-alpha', 'Alpha probe', '/work/alpha', 'devin-model', 'default', 1762003200, 1762003260, 5, 0)",
        [],
    )
    .unwrap();
    ins(
        "devin-alpha",
        1,
        None,
        serde_json::json!({"role": "system", "content": "P6DEVINSYS system node must never be indexed"}),
    );
    ins(
        "devin-alpha",
        2,
        Some(1),
        serde_json::json!({"role": "user", "content": "P6DEVINALPHAUSER fix the wheel check", "metadata": {"is_user_input": true}}),
    );
    ins(
        "devin-alpha",
        3,
        Some(2),
        serde_json::json!({
            "role": "assistant",
            "content": "P6DEVINALPHATHINK reading the wheel check",
            "thinking": {"thinking": "P6DEVINPRIVATE internal reasoning note"},
            "tool_calls": [{"id": "call_a1", "kind": "function", "name": "read_file", "arguments": {"path": "wheel.py"}}]
        }),
    );
    ins(
        "devin-alpha",
        4,
        Some(3),
        serde_json::json!({"role": "tool", "content": "P6DEVINALPHATOOLOUT wheel check passes", "tool_call_id": "call_a1"}),
    );
    ins(
        "devin-alpha",
        5,
        Some(4),
        serde_json::json!({"role": "assistant", "content": "P6DEVINALPHAFINAL the wheel check passes now"}),
    );
    ins(
        "devin-alpha",
        6,
        Some(2),
        serde_json::json!({"role": "user", "content": "P6DEVINABANDONED branch text must never export"}),
    );

    // devin-beta: unrelated minimal chain.
    conn.execute(
        "INSERT INTO sessions (id, title, working_directory, model, agent_mode, created_at, last_activity_at, main_chain_id, hidden)
         VALUES ('devin-beta', 'Beta probe', '/work/beta', 'devin-model', 'default', 1762003300, 1762003330, 11, 0)",
        [],
    )
    .unwrap();
    ins(
        "devin-beta",
        10,
        None,
        serde_json::json!({"role": "user", "content": "P6DEVINBETAUSER unrelated beta question"}),
    );
    ins(
        "devin-beta",
        11,
        Some(10),
        serde_json::json!({"role": "assistant", "content": "P6DEVINBETAFINAL unrelated beta answer"}),
    );

    // devin-hidden: hidden sessions stay out of the archive entirely.
    conn.execute(
        "INSERT INTO sessions (id, title, working_directory, model, agent_mode, created_at, last_activity_at, main_chain_id, hidden)
         VALUES ('devin-hidden', 'Hidden probe', '/work/hidden', 'devin-model', 'default', 1762003400, 1762003410, 20, 1)",
        [],
    )
    .unwrap();
    ins(
        "devin-hidden",
        20,
        None,
        serde_json::json!({"role": "user", "content": "P6DEVINHIDDENUSER must not be indexed"}),
    );

    // OpenCode decoy: same session ids, unmistakable different content.
    for (id, tag) in [("devin-alpha", "alpha"), ("devin-beta", "beta")] {
        conn.execute(
            "INSERT INTO session (id, title, directory, project_id, time_created, time_updated)
             VALUES (?1, ?2, '/decoy', 'proj-decoy', 1762003200000, 1762003260000)",
            rusqlite::params![id, format!("OPENCODE DECOY {tag}")],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
             VALUES (?1, ?2, 'user', 1, 1762003200000, 1762003200000, ?3)",
            rusqlite::params![
                format!("decoy-msg-{tag}-1"),
                id,
                serde_json::json!({"text": format!("P6OPENCODEDECOY {tag} decoy user text")}).to_string()
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
             VALUES (?1, ?2, 'assistant', 2, 1762003201000, 1762003201000, ?3)",
            rusqlite::params![
                format!("decoy-msg-{tag}-2"),
                id,
                serde_json::json!({"content": [{"type": "text", "text": format!("P6OPENCODEDECOY {tag} decoy assistant text")}]}).to_string()
            ],
        )
        .unwrap();
    }
    conn.close().expect("close devin fixture db");
}

fn index_devin(tmp: &TempDir) -> (PathBuf, PathBuf, Vec<ArchivedConvo>) {
    let devin_root = tmp.path().join("devin-home");
    fs::create_dir_all(&devin_root).unwrap();
    let db_file = devin_root.join("sessions.db");
    write_devin_fixture(&db_file);
    let db = run_isolated_index(
        tmp,
        HashMap::from([("devin".to_string(), vec![devin_root])]),
    );
    let convos = archived_convos(&db);
    (db, db_file, convos)
}

#[test]
fn devin_export_resolves_virtual_identity_not_opencode_decoy() {
    let tmp = TempDir::new().unwrap();
    let home = tmp.path().join("home");
    fs::create_dir_all(&home).unwrap();
    let (db, _db_file, convos) = index_devin(&tmp);

    assert_eq!(
        convos.len(),
        2,
        "only non-hidden devin sessions may be archived: {:?}",
        convos.iter().map(|c| &c.source_path).collect::<Vec<_>>()
    );
    for c in &convos {
        assert_eq!(c.agent_slug, "devin");
    }
    let alpha = find_convo(&convos, "devin-alpha");
    let beta = find_convo(&convos, "devin-beta");
    assert_eq!(
        alpha.title.as_deref(),
        Some("Alpha probe"),
        "session title must survive indexing"
    );
    assert!(
        alpha.source_path.ends_with("sessions.db/devin-alpha"),
        "devin identity must be the virtual sessions.db/<id> path: {}",
        alpha.source_path
    );

    // Index-side evidence: the main chain exports in root-to-tip order; the
    // system node and the abandoned branch are never archived.
    assert_eq!(
        archived_roles(alpha),
        ["user", "assistant", "tool", "assistant"],
        "devin alpha main chain must archive in order"
    );
    assert!(alpha.messages[0].content.contains("P6DEVINALPHAUSER"));
    assert!(
        alpha.messages[1].content.contains("[tool_call: read_file]"),
        "assistant tool_call marker must be archived: {}",
        alpha.messages[1].content
    );
    assert!(alpha.messages[2].content.contains("P6DEVINALPHATOOLOUT"));
    assert_eq!(
        alpha.messages[2].extra_json["tool_call_id"].as_str(),
        Some("call_a1"),
        "tool_call_id linkage must be retained"
    );
    assert!(
        alpha.messages[1].extra_json["thinking"]
            .as_str()
            .is_some_and(|t| t.contains("P6DEVINPRIVATE")),
        "assistant thinking must be retained in extra_json, not silently dropped"
    );

    // --- Export alpha while the SQLite source still exists ---
    let md = expect_success(&cass_export(
        &db,
        &alpha.source_path,
        "markdown",
        true,
        &home,
    ));
    assert!(
        md.contains("P6DEVINALPHAUSER"),
        "alpha user body missing:\n{md}"
    );
    assert!(
        md.contains("P6DEVINALPHAFINAL"),
        "alpha final body missing:\n{md}"
    );
    assert!(
        md.contains("P6DEVINALPHATOOLOUT"),
        "alpha tool output must render with --include-tools:\n{md}"
    );
    assert!(
        !md.contains("P6OPENCODEDECOY"),
        "export must never consult the OpenCode-shaped tables in the same db:\n{md}"
    );
    assert!(
        !md.contains("P6DEVINBETAUSER"),
        "exporting alpha must not leak beta's conversation:\n{md}"
    );
    assert!(
        !md.contains("P6DEVINABANDONED"),
        "abandoned branch text must not export:\n{md}"
    );
    assert!(
        !md.contains("P6DEVINHIDDENUSER"),
        "hidden session text must not export:\n{md}"
    );
    assert!(
        !md.contains("P6DEVINPRIVATE"),
        "thinking is retained metadata and must not silently render:\n{md}"
    );

    // --- Export beta: exact identity, not the decoy, not alpha ---
    let md = expect_success(&cass_export(
        &db,
        &beta.source_path,
        "markdown",
        true,
        &home,
    ));
    assert!(
        md.contains("P6DEVINBETAUSER"),
        "beta user body missing:\n{md}"
    );
    assert!(
        md.contains("P6DEVINBETAFINAL"),
        "beta final body missing:\n{md}"
    );
    assert!(
        !md.contains("P6OPENCODEDECOY"),
        "beta export must not consult the OpenCode decoy tables:\n{md}"
    );
    assert!(
        !md.contains("P6DEVINALPHAUSER"),
        "beta export must not leak alpha's conversation:\n{md}"
    );

    // --- JSON on alpha: archived roles in order ---
    let json = expect_success(&cass_export(&db, &alpha.source_path, "json", true, &home));
    assert_eq!(
        json_roles(&json),
        ["user", "assistant", "tool", "assistant"],
        "devin JSON export must emit archived roles: {json}"
    );
    let json = expect_success(&cass_export(&db, &alpha.source_path, "json", false, &home));
    assert_eq!(
        json_roles(&json),
        ["user", "assistant", "assistant"],
        "default JSON must exclude the tool node: {json}"
    );

    // --- HTML on alpha ---
    let out_dir = tmp.path().join("html-out");
    fs::create_dir_all(&out_dir).unwrap();
    let (out, output_path) = cass_export_html(
        &db,
        &alpha.source_path,
        &out_dir,
        "devin-alpha",
        true,
        &home,
    );
    assert!(
        out.status.success(),
        "export-html failed: {}",
        stderr_of(&out)
    );
    let path = output_path.expect("exported.output_path");
    let html = fs::read_to_string(&path).expect("read exported HTML");
    assert!(
        html.contains("P6DEVINALPHAUSER"),
        "alpha user missing from HTML"
    );
    assert!(
        html.contains("P6DEVINALPHAFINAL"),
        "alpha final missing from HTML"
    );
    assert!(
        html.contains("P6DEVINALPHATOOLOUT"),
        "alpha tool output missing"
    );
    assert!(
        !html.contains("P6OPENCODEDECOY"),
        "HTML export must not consult the decoy tables"
    );
    assert!(!html.contains("P6DEVINBETAUSER"), "HTML must not leak beta");
}

#[test]
fn devin_export_is_byte_stable_after_db_rename() {
    let tmp = TempDir::new().unwrap();
    let home = tmp.path().join("home");
    fs::create_dir_all(&home).unwrap();
    let (db, db_file, convos) = index_devin(&tmp);
    let beta = find_convo(&convos, "devin-beta");

    let live_md = expect_success(&cass_export(
        &db,
        &beta.source_path,
        "markdown",
        true,
        &home,
    ));

    let moved = db_file.with_extension("db.moved");
    fs::rename(&db_file, &moved).expect("rename sessions.db");
    assert!(!db_file.exists());

    let after_md = expect_success(&cass_export(
        &db,
        &beta.source_path,
        "markdown",
        true,
        &home,
    ));
    assert_eq!(
        after_md, live_md,
        "devin export must be byte-stable after the backing store is renamed"
    );
    assert!(after_md.contains("P6DEVINBETAUSER"));
    assert!(
        !after_md.contains("P6OPENCODEDECOY"),
        "the renamed-store export must still come from the archive"
    );
}

#[test]
fn devin_export_rejects_unindexed_session_id() {
    let tmp = TempDir::new().unwrap();
    let home = tmp.path().join("home");
    fs::create_dir_all(&home).unwrap();
    let (db, db_file, _convos) = index_devin(&tmp);

    // `devin-hidden` exists in the source store (hidden=1) but was never
    // indexed. Export must fail explicitly — never substitute another
    // conversation and never fall back to the OpenCode decoder.
    let hidden_identity = format!("{}/devin-hidden", db_file.display());
    let out = cass_export(&db, &hidden_identity, "markdown", true, &home);
    assert!(
        !out.status.success(),
        "exporting an unindexed virtual id must fail, got stdout:\n{}",
        stdout_of(&out)
    );
    assert!(
        !stdout_of(&out).contains("P6DEVINHIDDENUSER"),
        "failure must not leak hidden session content"
    );
    assert!(
        !stdout_of(&out).contains("P6OPENCODEDECOY"),
        "failure must not consult the decoy tables"
    );

    // A completely unknown id fails the same way.
    let unknown_identity = format!("{}/devin-does-not-exist", db_file.display());
    let out = cass_export(&db, &unknown_identity, "markdown", true, &home);
    assert!(
        !out.status.success(),
        "exporting an unknown virtual id must fail explicitly"
    );
}

// ---------------------------------------------------------------------------
// Antigravity
// ---------------------------------------------------------------------------

fn copy_dir_all(src: &Path, dst: &Path) {
    fs::create_dir_all(dst).unwrap();
    for entry in WalkDirEntries::new(src) {
        let rel = entry.strip_prefix(src).unwrap();
        let target = dst.join(rel);
        if entry.is_dir() {
            fs::create_dir_all(&target).unwrap();
        } else {
            fs::copy(&entry, &target).unwrap();
        }
    }
}

/// Minimal recursive walker avoiding an extra dev-dependency.
struct WalkDirEntries {
    stack: Vec<PathBuf>,
}

impl WalkDirEntries {
    fn new(root: &Path) -> Self {
        Self {
            stack: vec![root.to_path_buf()],
        }
    }
}

impl Iterator for WalkDirEntries {
    type Item = PathBuf;

    fn next(&mut self) -> Option<Self::Item> {
        let path = self.stack.pop()?;
        if path.is_dir()
            && let Ok(read) = fs::read_dir(&path)
        {
            for entry in read.flatten() {
                self.stack.push(entry.path());
            }
        }
        Some(path)
    }
}

fn index_antigravity(tmp: &TempDir) -> (PathBuf, Vec<ArchivedConvo>) {
    let agy_home = tmp.path().join("agy-home");
    copy_dir_all(
        Path::new(AGY_MAIN_DIR),
        &agy_home.join("brain").join(AGY_MAIN_UUID),
    );
    copy_dir_all(
        Path::new(AGY_CALLS_DIR),
        &agy_home.join("brain").join(AGY_CALLS_UUID),
    );
    let db = run_isolated_index(
        tmp,
        HashMap::from([("antigravity".to_string(), vec![agy_home])]),
    );
    let convos = archived_convos(&db);
    (db, convos)
}

#[test]
fn antigravity_export_uses_indexed_transcript_for_all_formats() {
    let tmp = TempDir::new().unwrap();
    let home = tmp.path().join("home");
    fs::create_dir_all(&home).unwrap();
    let (db, convos) = index_antigravity(&tmp);
    assert_eq!(
        convos.len(),
        2,
        "both antigravity transcripts must be archived: {:?}",
        convos.iter().map(|c| &c.source_path).collect::<Vec<_>>()
    );
    for c in &convos {
        assert_eq!(c.agent_slug, "antigravity");
    }

    // --- Main transcript: roles and retention ---
    let main = find_convo(&convos, AGY_MAIN_UUID);
    assert_eq!(
        archived_roles(main),
        [
            "user",
            "system",
            "assistant",
            "tool",
            "tool",
            "assistant",
            "system"
        ],
        "archived role/order for the main antigravity transcript"
    );
    assert!(main.messages[0].content.contains("FLYWHEEL"));
    assert_eq!(main.messages[1].author.as_deref(), Some("ephemeral"));
    assert_eq!(main.messages[3].author.as_deref(), Some("view_file"));
    assert_eq!(main.messages[4].author.as_deref(), Some("run_command"));
    assert_eq!(main.messages[6].author.as_deref(), Some("system"));
    assert!(
        main.messages
            .iter()
            .all(|m| !m.content.contains("CONVERSATION_HISTORY")),
        "CONVERSATION_HISTORY records must not be archived as messages"
    );

    // --- Planner-calls transcript: invocations retained in extra_json ---
    let calls = find_convo(&convos, AGY_CALLS_UUID);
    assert_eq!(
        archived_roles(calls),
        ["user", "assistant", "tool", "assistant"],
        "archived role/order for the planner-calls transcript"
    );
    assert!(
        calls.messages[1].content.contains("[Tool: view_file]"),
        "planner tool-call body must be archived: {}",
        calls.messages[1].content
    );
    assert!(
        calls.messages[1].extra_json["cass"]["invocations"]
            .as_array()
            .is_some_and(|v| v.iter().any(|i| i["name"].as_str() == Some("view_file"))),
        "planner invocation must be durable in extra_json.cass.invocations: {}",
        calls.messages[1].extra_json
    );
    assert!(calls.messages[2].content.contains("fn p2b_unique_body()"));

    // --- Markdown on the main transcript ---
    let md = expect_success(&cass_export(
        &db,
        &main.source_path,
        "markdown",
        true,
        &home,
    ));
    assert!(
        md.len() > "# Conversation Export".len() + 10,
        "export must not be a bare header:\n{md}"
    );
    assert!(
        md.contains("Search the codebase for the FLYWHEEL marker"),
        "user request must survive the <USER_REQUEST> envelope extraction:\n{md}"
    );
    assert!(
        md.contains("I'll grep for the FLYWHEEL marker now."),
        "first planner response missing:\n{md}"
    );
    assert!(
        md.contains("Found the FLYWHEEL marker in notes.txt; the associated magic number is 42."),
        "second planner response missing:\n{md}"
    );
    assert!(
        md.contains("notes.txt:1:The FLYWHEEL marker"),
        "tool step output must render with --include-tools:\n{md}"
    );

    let md = expect_success(&cass_export(
        &db,
        &main.source_path,
        "markdown",
        false,
        &home,
    ));
    assert!(md.contains("FLYWHEEL"));
    assert!(
        !md.contains("notes.txt:1:The FLYWHEEL marker"),
        "tool steps must be excluded without --include-tools:\n{md}"
    );

    // --- JSON on the main transcript ---
    let json = expect_success(&cass_export(&db, &main.source_path, "json", true, &home));
    assert_eq!(
        json_roles(&json),
        [
            "user",
            "system",
            "assistant",
            "tool",
            "tool",
            "assistant",
            "system"
        ],
        "antigravity JSON export must emit archived roles: {json}"
    );
    let msgs = json_messages(&json);
    assert_eq!(msgs[1]["author"].as_str(), Some("ephemeral"));
    assert_eq!(msgs[3]["author"].as_str(), Some("view_file"));

    // --- JSON on planner-calls: exact dialogue + tool result ---
    let json = expect_success(&cass_export(&db, &calls.source_path, "json", true, &home));
    assert_eq!(
        json_roles(&json),
        ["user", "assistant", "tool", "assistant"]
    );
    let msgs = json_messages(&json);
    assert!(
        msgs[0]["content"]
            .as_str()
            .unwrap_or("")
            .contains("read a.rs")
    );
    assert!(
        msgs[2]["content"]
            .as_str()
            .unwrap_or("")
            .contains("fn p2b_unique_body()")
    );
    assert!(msgs[3]["content"].as_str().unwrap_or("").contains("Done."));

    // --- HTML on the main transcript ---
    let out_dir = tmp.path().join("html-out");
    fs::create_dir_all(&out_dir).unwrap();
    let (out, output_path) =
        cass_export_html(&db, &main.source_path, &out_dir, "agy-main", true, &home);
    assert!(
        out.status.success(),
        "export-html failed: {}",
        stderr_of(&out)
    );
    let path = output_path.expect("exported.output_path");
    let html = fs::read_to_string(&path).expect("read exported HTML");
    assert!(
        html.contains("FLYWHEEL"),
        "antigravity transcript content must reach HTML export"
    );
    assert!(html.contains("magic number is 42"));
    assert!(
        html.contains("notes.txt:1:The FLYWHEEL marker"),
        "tool step must render in HTML when tools are included"
    );
}

#[test]
fn antigravity_export_is_byte_stable_after_transcript_rename() {
    let tmp = TempDir::new().unwrap();
    let home = tmp.path().join("home");
    fs::create_dir_all(&home).unwrap();
    let (db, convos) = index_antigravity(&tmp);
    let main = find_convo(&convos, AGY_MAIN_UUID);

    let live_md = expect_success(&cass_export(
        &db,
        &main.source_path,
        "markdown",
        true,
        &home,
    ));

    let transcript = PathBuf::from(&main.source_path);
    let moved = transcript.with_extension("jsonl.moved");
    fs::rename(&transcript, &moved).expect("rename transcript");
    assert!(!transcript.exists());

    let after_md = expect_success(&cass_export(
        &db,
        &main.source_path,
        "markdown",
        true,
        &home,
    ));
    assert_eq!(
        after_md, live_md,
        "antigravity export must be byte-stable after the transcript is renamed"
    );
    assert!(after_md.contains("FLYWHEEL"));
}
