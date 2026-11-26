//! E2E tests for multi-connector scenarios.
//!
//! These tests verify that multiple connectors work together correctly:
//! - Multiple connectors can be indexed in a single run
//! - Search returns results from all indexed connectors
//! - Agent filtering correctly isolates connector results
//! - Results are properly attributed to their source connector

use std::fs;
use std::path::Path;

mod util;
use util::e2e_log::{E2eError, E2eErrorContext, E2ePerformanceMetrics, PhaseTracker};

fn tracker_for(test_name: &str) -> PhaseTracker {
    PhaseTracker::new("e2e_multi_connector", test_name)
}

/// `cass` that sees only the isolated home and data dir: `env_clear` leaves
/// every provider root override unset, so discovery starts from the home.
fn isolated_cass(home: &Path, data: &Path) -> assert_cmd::Command {
    let mut command = assert_cmd::Command::new(assert_cmd::cargo::cargo_bin!("cass"));
    command
        .env_clear()
        .env("HOME", home)
        .env("USERPROFILE", home)
        .env("CASS_DATA_DIR", data)
        .env("CASS_IGNORE_SOURCES_CONFIG", "1")
        .env("CASS_AUTO_REFRESH", "0")
        .env("RUST_MIN_STACK", "134217728")
        .current_dir(home)
        .timeout(std::time::Duration::from_secs(180));
    if let Ok(system_root) = dotenvy::var("SystemRoot") {
        command.env("SystemRoot", system_root);
    }
    command
}

/// A Codex rollout holding one question with `needle` and its answer.
fn gh513_rollout_text(needle: &str) -> String {
    [
        serde_json::json!({"timestamp": "2026-08-01T10:00:00Z", "type": "session_meta",
            "payload": {"cwd": "/synthetic/gh513"}}),
        serde_json::json!({"timestamp": "2026-08-01T10:00:01Z", "type": "response_item",
            "payload": {"type": "message", "role": "user",
                "content": [{"type": "input_text", "text": format!("question {needle}")}]}}),
        serde_json::json!({"timestamp": "2026-08-01T10:00:02Z", "type": "response_item",
            "payload": {"type": "message", "role": "assistant",
                "content": [{"type": "output_text", "text": "the answer"}]}}),
    ]
    .iter()
    .map(|record| format!("{record}\n"))
    .collect()
}

/// A session as Codex's `local_thread_store_compression` leaves it:
/// `rollout-*.jsonl.zst`, one zstd frame that records the decoded length,
/// with the plain rollout's modification time (here 30 days ago).
fn gh513_compressed_rollout(home: &Path, name: &str, text: &str) -> std::path::PathBuf {
    let day = home.join(".codex/sessions/2026/08/01");
    fs::create_dir_all(&day).unwrap();
    let path = day.join(format!("rollout-2026-08-01T10-00-00-{name}.jsonl.zst"));
    fs::write(&path, zstd::bulk::compress(text.as_bytes(), 3).unwrap()).unwrap();
    let old = std::time::SystemTime::now() - std::time::Duration::from_secs(30 * 24 * 3600);
    fs::File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_modified(old)
        .unwrap();
    path
}

/// Codex hits for `needle`, as `(source_path, conversation_id)`.
fn gh513_hits(home: &Path, data: &Path, needle: &str) -> Vec<(String, serde_json::Value)> {
    let output = isolated_cass(home, data)
        .args([
            "search",
            needle,
            "--agent",
            "codex",
            "--mode",
            "lexical",
            "--json",
            "--no-maintenance",
            "--timeout",
            "10000",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let result: serde_json::Value = serde_json::from_slice(&output).unwrap();
    result["hits"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|hit| {
            (
                hit["source_path"].as_str().unwrap_or_default().to_owned(),
                hit["conversation_id"].clone(),
            )
        })
        .collect()
}

fn gh513_codex_conversations(home: &Path, data: &Path) -> serde_json::Value {
    let output = isolated_cass(home, data)
        .args(["stats", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let stats: serde_json::Value = serde_json::from_slice(&output).unwrap();
    stats["by_agent"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|row| row["agent"] == "codex")
        .map_or(serde_json::Value::Null, |row| row["count"].clone())
}

/// GH #513: both Codex passes (franken-agent-detection's parse and cass's
/// enrichment) read a compressed rollout's decoded text, so the connector
/// gives the same conversation, id included, for either form of one rollout.
#[test]
fn gh513_a_compressed_rollout_scans_and_enriches_like_its_plain_form() {
    use coding_agent_search::connectors::{
        Connector, ScanContext, ScanRoot, codex::CodexConnector,
    };
    let response = |payload: serde_json::Value| {
        serde_json::json!({"type": "response_item", "timestamp": "2026-08-01T10:00:05.000Z",
            "payload": payload})
    };
    let text = [
        response(serde_json::json!({"type": "message", "role": "user",
            "content": [{"type": "input_text", "text": "compressed parity question"}]})),
        response(
            serde_json::json!({"type": "custom_tool_call", "name": "apply_patch",
            "call_id": "patch-1", "input": "*** Begin Patch\n+compressed_parity\n*** End Patch\n"}),
        ),
        response(
            serde_json::json!({"type": "function_call_output", "call_id": "patch-1",
            "output": "patch applied"}),
        ),
    ]
    .iter()
    .map(|record| format!("{record}\n"))
    .collect::<String>();
    let scan = |compressed: bool, enrich: bool| -> Vec<serde_json::Value> {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join(".codex");
        let day = home.join("sessions/2026/08/01");
        fs::create_dir_all(&day).unwrap();
        let plain = day.join("rollout-2026-08-01T10-00-00-parity.jsonl");
        let written = if compressed {
            plain.with_extension("jsonl.zst")
        } else {
            plain
        };
        if compressed {
            fs::write(&written, zstd::bulk::compress(text.as_bytes(), 3).unwrap()).unwrap();
        } else {
            fs::write(&written, &text).unwrap();
        }
        let ctx = ScanContext::with_roots(
            dir.path().join("cass-data"),
            vec![ScanRoot::local(home)],
            None,
        )
        .without_local_default_stores();
        let conversations = if enrich {
            CodexConnector::new().scan(&ctx).unwrap()
        } else {
            franken_agent_detection::CodexConnector::new()
                .scan(&ctx)
                .unwrap()
        };
        conversations
            .into_iter()
            .map(|conversation| {
                assert_eq!(conversation.source_path, written);
                let mut value = serde_json::to_value(conversation).unwrap();
                value.as_object_mut().unwrap().remove("source_path");
                value
            })
            .collect()
    };
    let plain = scan(false, true);
    assert_eq!(plain.len(), 1);
    assert_eq!(
        plain[0]["external_id"],
        "2026/08/01/rollout-2026-08-01T10-00-00-parity"
    );
    // The fixture needs the enrichment pass: without it the messages differ.
    assert_ne!(plain[0]["messages"], scan(false, false)[0]["messages"]);
    assert_eq!(scan(true, true), plain);
}

/// GH #513 (bead 3u22z): a Codex home holding only compressed sessions (the
/// reporter's 1,599 of 1,609) indexes every one of them, searchably.
#[test]
fn gh513_codex_sessions_compressed_to_jsonl_zst_are_indexed() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let data = tmp.path().join("data");
    fs::create_dir_all(&data).unwrap();
    let first = gh513_compressed_rollout(&home, "first", &gh513_rollout_text("gh513firstneedle"));
    gh513_compressed_rollout(&home, "second", &gh513_rollout_text("gh513secondneedle"));
    isolated_cass(&home, &data)
        .args(["index", "--full", "--json", "--no-progress-events"])
        .assert()
        .success();
    assert_eq!(gh513_codex_conversations(&home, &data), 2);
    let hits = gh513_hits(&home, &data, "gh513firstneedle");
    assert!(
        !hits.is_empty(),
        "the compressed session's text is searchable"
    );
    let name = first.file_name().unwrap();
    assert!(
        hits.iter()
            .all(|(path, _)| Path::new(path).file_name() == Some(name)),
        "{hits:?}"
    );
}

/// GH #513: a session indexed as `rollout-*.jsonl` that Codex then compresses
/// (plain file removed, modification time kept) is the same session, not a
/// second one.
#[test]
fn gh513_a_session_compressed_after_indexing_is_not_duplicated() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let data = tmp.path().join("data");
    fs::create_dir_all(&data).unwrap();
    let text = gh513_rollout_text("gh513sameneedle");
    let packed = gh513_compressed_rollout(&home, "same", &text);
    let plain = packed.with_extension("");
    let mtime = fs::metadata(&packed).unwrap().modified().unwrap();
    fs::rename(&packed, packed.with_extension("parked")).unwrap();
    fs::write(&plain, &text).unwrap();
    isolated_cass(&home, &data)
        .args(["index", "--full", "--json", "--no-progress-events"])
        .assert()
        .success();
    let before = gh513_hits(&home, &data, "gh513sameneedle");
    assert!(!before.is_empty());
    // The comparison below needs a real id, not two missing ones.
    assert!(before[0].1.is_i64(), "{before:?}");

    // Codex compresses it: the compressed file keeps the plain file's mtime.
    fs::remove_file(&plain).unwrap();
    fs::rename(packed.with_extension("parked"), &packed).unwrap();
    fs::File::options()
        .write(true)
        .open(&packed)
        .unwrap()
        .set_modified(mtime)
        .unwrap();
    isolated_cass(&home, &data)
        .args(["index", "--full", "--json", "--no-progress-events"])
        .assert()
        .success();
    assert_eq!(gh513_codex_conversations(&home, &data), 1);
    let after = gh513_hits(&home, &data, "gh513sameneedle");
    assert!(
        after.iter().all(|(_, id)| *id == before[0].1),
        "before {before:?} after {after:?}"
    );
}

/// GH #513: an archive whose Codex watermark an older build recorded picks
/// up compressed sessions older than that watermark on its next plain
/// `cass index`, in both ingest modes, and records the scan contract so the
/// run after it has a cutoff again.
#[test]
fn gh513_an_older_codex_watermark_does_not_hide_old_compressed_sessions() {
    use coding_agent_search::storage::sqlite::FrankenStorage;
    for streaming in ["0", "1"] {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path().join("home");
        let data = tmp.path().join("data");
        fs::create_dir_all(&data).unwrap();
        let plain_day = home.join(".codex/sessions/2026/10/01");
        fs::create_dir_all(&plain_day).unwrap();
        fs::write(
            plain_day.join("rollout-2026-10-01T10-00-00-plain.jsonl"),
            gh513_rollout_text("gh513plainneedle"),
        )
        .unwrap();
        isolated_cass(&home, &data)
            .env("CASS_STREAMING_INDEX", streaming)
            .args(["index", "--full", "--json", "--no-progress-events"])
            .assert()
            .success();
        let db = data.join("agent_search.db");
        {
            // Leave the archive as an older build would: a Codex watermark
            // recorded without the scan contract.
            let storage = FrankenStorage::open(&db).unwrap();
            let state = storage.connector_scan_states(&["codex"]).unwrap()["codex"];
            assert!(state.last_scan_ts.is_some(), "streaming={streaming}");
            assert!(!state.scan_contract_pending, "streaming={streaming}");
            storage
                .raw()
                .execute("DELETE FROM meta WHERE key = 'scan_contract:connector:codex'")
                .unwrap();
            assert!(
                storage.connector_scan_states(&["codex"]).unwrap()["codex"].scan_contract_pending
            );
        }

        // Compressed long ago, so older than the watermark.
        let packed =
            gh513_compressed_rollout(&home, "backlog", &gh513_rollout_text("gh513backlogneedle"));
        isolated_cass(&home, &data)
            .env("CASS_STREAMING_INDEX", streaming)
            .args(["index", "--json", "--no-progress-events"])
            .assert()
            .success();
        let hits = gh513_hits(&home, &data, "gh513backlogneedle");
        assert!(
            hits.iter()
                .any(|(path, _)| Path::new(path).file_name() == packed.file_name()),
            "streaming={streaming}: {hits:?}"
        );
        let storage = FrankenStorage::open_readonly(&db).unwrap();
        assert!(
            !storage.connector_scan_states(&["codex"]).unwrap()["codex"].scan_contract_pending,
            "streaming={streaming}: the scan recorded the contract"
        );
    }
}

fn truncate_output(bytes: &[u8], max_len: usize) -> String {
    let s = String::from_utf8_lossy(bytes);
    if s.len() > max_len {
        format!(
            "{}... [truncated {} bytes]",
            &s[..max_len],
            s.len() - max_len
        )
    } else {
        s.to_string()
    }
}

fn make_codex_fixture(root: &Path) {
    let sessions = root.join("sessions/2025/11/21");
    fs::create_dir_all(&sessions).unwrap();
    let file = sessions.join("rollout-1.jsonl");
    // Modern Codex JSONL format (envelope)
    let sample = r#"{"type": "event_msg", "timestamp": 1700000000000, "payload": {"type": "user_message", "message": "codex_user"}}
{"type": "response_item", "timestamp": 1700000001000, "payload": {"role": "assistant", "content": "codex_assistant"}}
"#;
    fs::write(file, sample).unwrap();
}

fn make_claude_fixture(root: &Path) {
    let project = root.join("projects/test-project");
    fs::create_dir_all(&project).unwrap();
    let file = project.join("session.jsonl");
    // Claude Code format
    let sample = r#"{"type": "user", "timestamp": "2023-11-21T10:00:00Z", "message": {"role": "user", "content": "claude_user"}}
{"type": "assistant", "timestamp": "2023-11-21T10:00:05Z", "message": {"role": "assistant", "content": "claude_assistant"}}
"#;
    fs::write(file, sample).unwrap();
}

fn make_gemini_fixture(root: &Path) {
    let project_hash = root.join("tmp/hash123/chats");
    fs::create_dir_all(&project_hash).unwrap();
    let file = project_hash.join("session-1.json"); // Must start with session-
    // Gemini CLI format
    let sample = r#"{
  "messages": [
    {"role": "user", "timestamp": 1700000000000, "content": "gemini_user"},
    {"role": "model", "timestamp": 1700000001000, "content": "gemini_assistant"}
  ]
}"#;
    fs::write(file, sample).unwrap();
}

fn make_qwen_fixture(root: &Path) {
    let chats = root.join("tmp/hash456/chats");
    fs::create_dir_all(&chats).unwrap();
    let file = chats.join("session-1.json"); // Legacy layout: session-*.json
    // Qwen Code format
    let sample = r#"{
  "sessionId": "qwen-session-1",
  "projectHash": "hash456",
  "messages": [
    {"type": "user", "timestamp": "2023-11-21T10:00:00Z", "content": "qwen_user"},
    {"type": "qwen", "timestamp": "2023-11-21T10:00:05Z", "content": "qwen_assistant"}
  ]
}"#;
    fs::write(file, sample).unwrap();
}

fn make_opencode_fixture(xdg_data: &Path) {
    let storage = xdg_data.join("opencode/storage");
    let session_dir = storage.join("session/proj-1");
    let message_dir = storage.join("message/sess-1");
    let part_user_dir = storage.join("part/msg-1");
    let part_assistant_dir = storage.join("part/msg-2");
    for dir in [
        &session_dir,
        &message_dir,
        &part_user_dir,
        &part_assistant_dir,
    ] {
        fs::create_dir_all(dir).unwrap();
    }
    fs::write(
        session_dir.join("sess-1.json"),
        r#"{"id":"sess-1","projectID":"proj-1","directory":"/work/opencode-demo","time":{"created":1700000000000,"updated":1700000001000}}"#,
    )
    .unwrap();
    fs::write(
        message_dir.join("msg-1.json"),
        r#"{"id":"msg-1","sessionID":"sess-1","role":"user","time":{"created":1700000000000}}"#,
    )
    .unwrap();
    fs::write(
        part_user_dir.join("part-1.json"),
        r#"{"id":"part-1","messageID":"msg-1","type":"text","text":"opencode_user"}"#,
    )
    .unwrap();
    fs::write(
        message_dir.join("msg-2.json"),
        r#"{"id":"msg-2","sessionID":"sess-1","role":"assistant","time":{"created":1700000001000}}"#,
    )
    .unwrap();
    fs::write(
        part_assistant_dir.join("part-2.json"),
        r#"{"id":"part-2","messageID":"msg-2","type":"text","text":"opencode_assistant"}"#,
    )
    .unwrap();
}

#[test]
#[cfg_attr(
    not(target_os = "linux"),
    ignore = "Linux-specific test (XDG_DATA_HOME paths)"
)]
fn multi_connector_pipeline() {
    let tracker = tracker_for("multi_connector_pipeline");
    let tmp = tempfile::TempDir::new().unwrap();
    let home = tmp.path();
    let xdg_data = home.join("xdg_data");

    fs::create_dir_all(&xdg_data).unwrap();

    // Setup fixture roots
    let dot_codex = home.join(".codex");
    let dot_claude = home.join(".claude");
    let dot_gemini = home.join(".gemini");
    let dot_qwen = home.join(".qwen");

    let command_env = tracker
        .command_environment()
        .with_home(home)
        .with_codex_home(&dot_codex);

    // Phase: Create fixtures for all connectors
    let phase_start = tracker.start("setup_fixtures", Some("Create fixtures for 5 connectors"));
    make_codex_fixture(&dot_codex);
    make_claude_fixture(&dot_claude);
    make_gemini_fixture(&dot_gemini);
    make_qwen_fixture(&dot_qwen);
    make_opencode_fixture(&xdg_data);
    let data_dir = home.join("cass_data");
    fs::create_dir_all(&data_dir).unwrap();
    tracker.end(
        "setup_fixtures",
        Some("Create fixtures for 5 connectors"),
        phase_start,
    );

    // Phase: Full index
    let phase_start = tracker.start(
        "run_index_full",
        Some("Run full index across all connectors"),
    );
    let idx_output = command_env
        .cass_assert_command()
        .arg("index")
        .arg("--full")
        .arg("--data-dir")
        .arg(&data_dir)
        .env("HOME", home.to_string_lossy().as_ref())
        .env("XDG_DATA_HOME", xdg_data.to_string_lossy().as_ref())
        .env(
            "OPENCODE_STORAGE_ROOT",
            xdg_data.join("opencode/storage").to_string_lossy().as_ref(),
        )
        .env("CODEX_HOME", dot_codex.to_string_lossy().as_ref())
        .env("GEMINI_HOME", dot_gemini.to_string_lossy().as_ref())
        .output()
        .expect("failed to spawn cass index --full");
    if !idx_output.status.success() {
        let ctx = E2eErrorContext::new()
            .with_command("cass index --full (multi_connector_pipeline)")
            .capture_cwd()
            .add_state("exit_code", serde_json::json!(idx_output.status.code()))
            .add_state(
                "stdout_tail",
                serde_json::json!(truncate_output(&idx_output.stdout, 1000)),
            )
            .add_state(
                "stderr_tail",
                serde_json::json!(truncate_output(&idx_output.stderr, 1000)),
            );
        tracker.fail(
            E2eError::with_type("cass index --full failed", "COMMAND_FAILED").with_context(ctx),
        );
        panic!(
            "cass index --full failed (exit {:?}): {}",
            idx_output.status.code(),
            truncate_output(&idx_output.stderr, 500)
        );
    }
    tracker.end(
        "run_index_full",
        Some("Run full index across all connectors"),
        phase_start,
    );

    // Phase: Search all connectors
    let phase_start = tracker.start(
        "search_all_connectors",
        Some("Search and verify all 5 connector results"),
    );
    let search_start = std::time::Instant::now();
    let output = command_env
        .cass_assert_command()
        .arg("search")
        .arg("user")
        .arg("--robot")
        .arg("--data-dir")
        .arg(&data_dir)
        .env("HOME", home.to_string_lossy().as_ref())
        .env("XDG_DATA_HOME", xdg_data.to_string_lossy().as_ref())
        .output()
        .expect("failed to execute search");
    let search_duration = search_start.elapsed().as_millis() as u64;

    if !output.status.success() {
        let ctx = E2eErrorContext::new()
            .with_command("cass search user --robot")
            .capture_cwd()
            .add_state("exit_code", serde_json::json!(output.status.code()))
            .add_state(
                "stdout_tail",
                serde_json::json!(truncate_output(&output.stdout, 1000)),
            )
            .add_state(
                "stderr_tail",
                serde_json::json!(truncate_output(&output.stderr, 1000)),
            );
        tracker.fail(E2eError::with_type("cass search failed", "COMMAND_FAILED").with_context(ctx));
        panic!(
            "cass search failed (exit {:?}): {}",
            output.status.code(),
            truncate_output(&output.stderr, 500)
        );
    }
    let json_out: serde_json::Value = serde_json::from_slice(&output.stdout).expect("valid json");
    let hits = json_out
        .get("hits")
        .and_then(|h| h.as_array())
        .expect("hits array");

    let found_agents: std::collections::HashSet<&str> = hits
        .iter()
        .filter_map(|h| h.get("agent").and_then(|s| s.as_str()))
        .collect();

    assert!(
        found_agents.contains("codex"),
        "Missing codex hit. Found: {found_agents:?}"
    );
    assert!(
        found_agents.contains("claude_code"),
        "Missing claude hit. Found: {found_agents:?}"
    );
    assert!(
        found_agents.contains("gemini"),
        "Missing gemini hit. Found: {found_agents:?}"
    );
    assert!(
        found_agents.contains("qwen"),
        "Missing qwen hit. Found: {found_agents:?}"
    );
    assert!(
        found_agents.contains("opencode"),
        "Missing opencode hit. Found: {found_agents:?}"
    );
    tracker.end(
        "search_all_connectors",
        Some("Search and verify all 5 connector results"),
        phase_start,
    );

    tracker.metrics(
        "search_all_connectors",
        &E2ePerformanceMetrics::new()
            .with_duration(search_duration)
            .with_custom("hit_count", serde_json::json!(hits.len()))
            .with_custom("agent_count", serde_json::json!(found_agents.len())),
    );

    // Phase: Incremental index test
    let phase_start = tracker.start(
        "incremental_index",
        Some("Add new file and verify incremental index"),
    );
    std::thread::sleep(std::time::Duration::from_secs(2));

    let sessions = dot_codex.join("sessions/2025/11/22");
    fs::create_dir_all(&sessions).unwrap();

    let now_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    let content = format!(
        r#"{{"type": "event_msg", "timestamp": {now_ts}, "payload": {{"type": "user_message", "message": "codex_new"}}}}"#
    );
    fs::write(sessions.join("rollout-2.jsonl"), content).unwrap();

    let incr_idx_output = command_env
        .cass_assert_command()
        .arg("index")
        .arg("--data-dir")
        .arg(&data_dir)
        .env("HOME", home.to_string_lossy().as_ref())
        .env("XDG_DATA_HOME", xdg_data.to_string_lossy().as_ref())
        .env(
            "OPENCODE_STORAGE_ROOT",
            xdg_data.join("opencode/storage").to_string_lossy().as_ref(),
        )
        .env("CODEX_HOME", dot_codex.to_string_lossy().as_ref())
        .env("GEMINI_HOME", dot_gemini.to_string_lossy().as_ref())
        .output()
        .expect("failed to spawn cass index (incremental)");
    if !incr_idx_output.status.success() {
        let ctx = E2eErrorContext::new()
            .with_command("cass index (incremental)")
            .capture_cwd()
            .add_state(
                "exit_code",
                serde_json::json!(incr_idx_output.status.code()),
            )
            .add_state(
                "stdout_tail",
                serde_json::json!(truncate_output(&incr_idx_output.stdout, 1000)),
            )
            .add_state(
                "stderr_tail",
                serde_json::json!(truncate_output(&incr_idx_output.stderr, 1000)),
            );
        tracker.fail(
            E2eError::with_type("cass index (incremental) failed", "COMMAND_FAILED")
                .with_context(ctx),
        );
        panic!(
            "cass index (incremental) failed (exit {:?}): {}",
            incr_idx_output.status.code(),
            truncate_output(&incr_idx_output.stderr, 500)
        );
    }

    let output_inc = command_env
        .cass_assert_command()
        .arg("search")
        .arg("codex_new")
        .arg("--robot")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("failed to execute search");

    let json_inc: serde_json::Value =
        serde_json::from_slice(&output_inc.stdout).expect("valid json");
    let hits_inc = json_inc
        .get("hits")
        .and_then(|h| h.as_array())
        .expect("hits array");
    assert!(
        !hits_inc.is_empty(),
        "Incremental index failed to pick up new file"
    );
    assert_eq!(hits_inc[0]["content"], "codex_new");
    tracker.end(
        "incremental_index",
        Some("Add new file and verify incremental index"),
        phase_start,
    );

    // Phase: Agent filter test
    let phase_start = tracker.start(
        "test_agent_filter",
        Some("Verify agent filter isolates results"),
    );
    let filter_start = std::time::Instant::now();
    let output_filter = command_env
        .cass_assert_command()
        .arg("search")
        .arg("user")
        .arg("--agent")
        .arg("claude_code")
        .arg("--robot")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("failed to execute search");
    let filter_duration = filter_start.elapsed().as_millis() as u64;

    let json_filter: serde_json::Value =
        serde_json::from_slice(&output_filter.stdout).expect("valid json");
    let hits_filter = json_filter
        .get("hits")
        .and_then(|h| h.as_array())
        .expect("hits array");

    for hit in hits_filter {
        assert_eq!(hit["agent"], "claude_code");
    }
    assert!(!hits_filter.is_empty());
    tracker.end(
        "test_agent_filter",
        Some("Verify agent filter isolates results"),
        phase_start,
    );

    tracker.metrics(
        "agent_filter_query",
        &E2ePerformanceMetrics::new()
            .with_duration(filter_duration)
            .with_custom("filtered_hit_count", serde_json::json!(hits_filter.len())),
    );

    tracker.complete();
}

// ============================================================================
// Cross-platform multi-connector tests (work on macOS and Linux)
// These tests use Codex and Claude Code which rely on HOME env var
// ============================================================================

/// Creates a Codex session with specific date and content.
fn make_codex_session(
    codex_home: &Path,
    date_path: &str,
    filename: &str,
    content: &str,
    ts_millis: u64,
) {
    let sessions = codex_home.join(format!("sessions/{date_path}"));
    fs::create_dir_all(&sessions).unwrap();
    let file = sessions.join(filename);
    let sample = format!(
        r#"{{"type": "event_msg", "timestamp": {ts_millis}, "payload": {{"type": "user_message", "message": "{content}"}}}}
{{"type": "response_item", "timestamp": {}, "payload": {{"role": "assistant", "content": "{content}_response"}}}}"#,
        ts_millis + 1000
    );
    fs::write(file, sample).unwrap();
}

/// Creates a Claude Code session with specific content.
fn make_claude_session(
    claude_home: &Path,
    project_name: &str,
    filename: &str,
    content: &str,
    ts_iso: &str,
) {
    let project = claude_home.join(format!("projects/{project_name}"));
    fs::create_dir_all(&project).unwrap();
    let file = project.join(filename);
    let sample = format!(
        r#"{{"type": "user", "timestamp": "{ts_iso}", "message": {{"role": "user", "content": "{content}"}}}}
{{"type": "assistant", "timestamp": "{ts_iso}", "message": {{"role": "assistant", "content": "{content}_response"}}}}"#
    );
    fs::write(file, sample).unwrap();
}

/// Test: Multiple connectors can be indexed and searched together
#[test]
fn multi_connector_codex_and_claude() {
    let tracker = tracker_for("multi_connector_codex_and_claude");
    let tmp = tempfile::TempDir::new().unwrap();
    let home = tmp.path();
    let codex_home = home.join(".codex");
    let claude_home = home.join(".claude");
    let data_dir = home.join("cass_data");
    fs::create_dir_all(&data_dir).unwrap();

    let command_env = tracker
        .command_environment()
        .with_home(home)
        .with_codex_home(&codex_home);

    // Phase: Create fixtures
    let phase_start = tracker.start("setup_fixtures", Some("Create Codex and Claude sessions"));
    make_codex_session(
        &codex_home,
        "2024/11/20",
        "rollout-multi.jsonl",
        "multitest codex_unique_content",
        1732118400000,
    );
    make_claude_session(
        &claude_home,
        "multi-project",
        "session-multi.jsonl",
        "multitest claude_unique_content",
        "2024-11-20T10:00:00Z",
    );
    tracker.end(
        "setup_fixtures",
        Some("Create Codex and Claude sessions"),
        phase_start,
    );

    // Phase: Index
    let phase_start = tracker.start("run_index", Some("Run full index"));
    let idx_output = command_env
        .cass_assert_command()
        .args(["index", "--full", "--data-dir"])
        .arg(&data_dir)
        .env("CODEX_HOME", &codex_home)
        .env("HOME", home)
        .output()
        .expect("failed to spawn cass index --full");
    if !idx_output.status.success() {
        let ctx = E2eErrorContext::new()
            .with_command("cass index --full (codex_and_claude)")
            .capture_cwd()
            .add_state("exit_code", serde_json::json!(idx_output.status.code()))
            .add_state(
                "stdout_tail",
                serde_json::json!(truncate_output(&idx_output.stdout, 1000)),
            )
            .add_state(
                "stderr_tail",
                serde_json::json!(truncate_output(&idx_output.stderr, 1000)),
            );
        tracker.fail(
            E2eError::with_type("cass index --full failed", "COMMAND_FAILED").with_context(ctx),
        );
        panic!(
            "cass index --full failed (exit {:?}): {}",
            idx_output.status.code(),
            truncate_output(&idx_output.stderr, 500)
        );
    }
    tracker.end("run_index", Some("Run full index"), phase_start);

    // Phase: Search and verify
    let phase_start = tracker.start(
        "search_multi_connector",
        Some("Search shared term across connectors"),
    );
    let search_start = std::time::Instant::now();
    let output = command_env
        .cass_assert_command()
        .args(["search", "multitest", "--robot", "--data-dir"])
        .arg(&data_dir)
        .env("HOME", home)
        .output()
        .expect("search command");
    let search_duration = search_start.elapsed().as_millis() as u64;

    if !output.status.success() {
        let ctx = E2eErrorContext::new()
            .with_command("cass search multitest --robot")
            .capture_cwd()
            .add_state("exit_code", serde_json::json!(output.status.code()))
            .add_state(
                "stdout_tail",
                serde_json::json!(truncate_output(&output.stdout, 1000)),
            )
            .add_state(
                "stderr_tail",
                serde_json::json!(truncate_output(&output.stderr, 1000)),
            );
        tracker.fail(E2eError::with_type("cass search failed", "COMMAND_FAILED").with_context(ctx));
        panic!(
            "cass search failed (exit {:?}): {}",
            output.status.code(),
            truncate_output(&output.stderr, 500)
        );
    }
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("valid json");
    let hits = json
        .get("hits")
        .and_then(|h| h.as_array())
        .expect("hits array");

    let agents: std::collections::HashSet<_> =
        hits.iter().filter_map(|h| h["agent"].as_str()).collect();

    assert!(
        agents.contains("codex"),
        "Should find codex results. Agents found: {agents:?}"
    );
    assert!(
        agents.contains("claude_code"),
        "Should find claude_code results. Agents found: {agents:?}"
    );
    assert!(
        hits.len() >= 2,
        "Should have at least 2 hits from different connectors"
    );
    tracker.end(
        "search_multi_connector",
        Some("Search shared term across connectors"),
        phase_start,
    );

    tracker.metrics(
        "search_multi_connector",
        &E2ePerformanceMetrics::new()
            .with_duration(search_duration)
            .with_custom("hit_count", serde_json::json!(hits.len()))
            .with_custom("agent_count", serde_json::json!(agents.len())),
    );

    tracker.complete();
}

/// Test: Agent filter isolates results to specific connector
#[test]
fn multi_connector_agent_filter_isolation() {
    let tracker = tracker_for("multi_connector_agent_filter_isolation");
    let tmp = tempfile::TempDir::new().unwrap();
    let home = tmp.path();
    let codex_home = home.join(".codex");
    let claude_home = home.join(".claude");
    let data_dir = home.join("cass_data");
    fs::create_dir_all(&data_dir).unwrap();

    let command_env = tracker
        .command_environment()
        .with_home(home)
        .with_codex_home(&codex_home);

    // Phase: Setup
    let phase_start = tracker.start(
        "setup_fixtures",
        Some("Create sessions with shared search term"),
    );
    make_codex_session(
        &codex_home,
        "2024/11/20",
        "rollout-iso.jsonl",
        "isolationtest codex_data",
        1732118400000,
    );
    make_claude_session(
        &claude_home,
        "iso-project",
        "session-iso.jsonl",
        "isolationtest claude_data",
        "2024-11-20T10:00:00Z",
    );
    tracker.end(
        "setup_fixtures",
        Some("Create sessions with shared search term"),
        phase_start,
    );

    // Phase: Index
    let phase_start = tracker.start("run_index", Some("Run full index"));
    let idx_output = command_env
        .cass_assert_command()
        .args(["index", "--full", "--data-dir"])
        .arg(&data_dir)
        .env("CODEX_HOME", &codex_home)
        .env("HOME", home)
        .output()
        .expect("failed to spawn cass index --full");
    if !idx_output.status.success() {
        let ctx = E2eErrorContext::new()
            .with_command("cass index --full (agent_filter_isolation)")
            .capture_cwd()
            .add_state("exit_code", serde_json::json!(idx_output.status.code()))
            .add_state(
                "stdout_tail",
                serde_json::json!(truncate_output(&idx_output.stdout, 1000)),
            )
            .add_state(
                "stderr_tail",
                serde_json::json!(truncate_output(&idx_output.stderr, 1000)),
            );
        tracker.fail(
            E2eError::with_type("cass index --full failed", "COMMAND_FAILED").with_context(ctx),
        );
        panic!(
            "cass index --full failed (exit {:?}): {}",
            idx_output.status.code(),
            truncate_output(&idx_output.stderr, 500)
        );
    }
    tracker.end("run_index", Some("Run full index"), phase_start);

    // Phase: Filter by codex
    let phase_start = tracker.start("filter_codex", Some("Search with agent=codex filter"));
    let codex_start = std::time::Instant::now();
    let codex_output = command_env
        .cass_assert_command()
        .args([
            "search",
            "isolationtest",
            "--agent",
            "codex",
            "--robot",
            "--data-dir",
        ])
        .arg(&data_dir)
        .env("HOME", home)
        .output()
        .expect("search command");
    let codex_duration = codex_start.elapsed().as_millis() as u64;

    if !codex_output.status.success() {
        let ctx = E2eErrorContext::new()
            .with_command("cass search isolationtest --agent codex --robot")
            .capture_cwd()
            .add_state("exit_code", serde_json::json!(codex_output.status.code()))
            .add_state(
                "stdout_tail",
                serde_json::json!(truncate_output(&codex_output.stdout, 1000)),
            )
            .add_state(
                "stderr_tail",
                serde_json::json!(truncate_output(&codex_output.stderr, 1000)),
            );
        tracker.fail(
            E2eError::with_type("cass search --agent codex failed", "COMMAND_FAILED")
                .with_context(ctx),
        );
        panic!(
            "cass search --agent codex failed (exit {:?}): {}",
            codex_output.status.code(),
            truncate_output(&codex_output.stderr, 500)
        );
    }
    let codex_json: serde_json::Value =
        serde_json::from_slice(&codex_output.stdout).expect("valid json");
    let codex_hits = codex_json
        .get("hits")
        .and_then(|h| h.as_array())
        .expect("hits array");

    assert!(!codex_hits.is_empty(), "Should find codex hits");
    for hit in codex_hits {
        assert_eq!(
            hit["agent"], "codex",
            "All hits should be from codex when filtering"
        );
    }
    tracker.end(
        "filter_codex",
        Some("Search with agent=codex filter"),
        phase_start,
    );

    tracker.metrics(
        "filter_codex",
        &E2ePerformanceMetrics::new()
            .with_duration(codex_duration)
            .with_custom("hit_count", serde_json::json!(codex_hits.len())),
    );

    // Phase: Filter by claude_code
    let phase_start = tracker.start(
        "filter_claude",
        Some("Search with agent=claude_code filter"),
    );
    let claude_start = std::time::Instant::now();
    let claude_output = command_env
        .cass_assert_command()
        .args([
            "search",
            "isolationtest",
            "--agent",
            "claude_code",
            "--robot",
            "--data-dir",
        ])
        .arg(&data_dir)
        .env("HOME", home)
        .output()
        .expect("search command");
    let claude_duration = claude_start.elapsed().as_millis() as u64;

    if !claude_output.status.success() {
        let ctx = E2eErrorContext::new()
            .with_command("cass search isolationtest --agent claude_code --robot")
            .capture_cwd()
            .add_state("exit_code", serde_json::json!(claude_output.status.code()))
            .add_state(
                "stdout_tail",
                serde_json::json!(truncate_output(&claude_output.stdout, 1000)),
            )
            .add_state(
                "stderr_tail",
                serde_json::json!(truncate_output(&claude_output.stderr, 1000)),
            );
        tracker.fail(
            E2eError::with_type("cass search --agent claude_code failed", "COMMAND_FAILED")
                .with_context(ctx),
        );
        panic!(
            "cass search --agent claude_code failed (exit {:?}): {}",
            claude_output.status.code(),
            truncate_output(&claude_output.stderr, 500)
        );
    }
    let claude_json: serde_json::Value =
        serde_json::from_slice(&claude_output.stdout).expect("valid json");
    let claude_hits = claude_json
        .get("hits")
        .and_then(|h| h.as_array())
        .expect("hits array");

    assert!(!claude_hits.is_empty(), "Should find claude_code hits");
    for hit in claude_hits {
        assert_eq!(
            hit["agent"], "claude_code",
            "All hits should be from claude_code when filtering"
        );
    }
    tracker.end(
        "filter_claude",
        Some("Search with agent=claude_code filter"),
        phase_start,
    );

    tracker.metrics(
        "filter_claude",
        &E2ePerformanceMetrics::new()
            .with_duration(claude_duration)
            .with_custom("hit_count", serde_json::json!(claude_hits.len())),
    );

    tracker.complete();
}

/// Test: Each connector's unique content is properly indexed
#[test]
fn multi_connector_unique_content() {
    let tracker = tracker_for("multi_connector_unique_content");
    let tmp = tempfile::TempDir::new().unwrap();
    let home = tmp.path();
    let codex_home = home.join(".codex");
    let claude_home = home.join(".claude");
    let data_dir = home.join("cass_data");
    fs::create_dir_all(&data_dir).unwrap();

    let command_env = tracker
        .command_environment()
        .with_home(home)
        .with_codex_home(&codex_home);

    // Phase: Setup
    let phase_start = tracker.start(
        "setup_fixtures",
        Some("Create sessions with unique content per connector"),
    );
    make_codex_session(
        &codex_home,
        "2024/11/20",
        "rollout-unique.jsonl",
        "codexonly_xyzzy uniqueterm",
        1732118400000,
    );
    make_claude_session(
        &claude_home,
        "unique-project",
        "session-unique.jsonl",
        "claudeonly_plugh uniqueterm",
        "2024-11-20T10:00:00Z",
    );
    tracker.end(
        "setup_fixtures",
        Some("Create sessions with unique content per connector"),
        phase_start,
    );

    // Phase: Index
    let phase_start = tracker.start("run_index", Some("Run full index"));
    let idx_output = command_env
        .cass_assert_command()
        .args(["index", "--full", "--data-dir"])
        .arg(&data_dir)
        .env("CODEX_HOME", &codex_home)
        .env("HOME", home)
        .output()
        .expect("failed to spawn cass index --full");
    if !idx_output.status.success() {
        let ctx = E2eErrorContext::new()
            .with_command("cass index --full (unique_content)")
            .capture_cwd()
            .add_state("exit_code", serde_json::json!(idx_output.status.code()))
            .add_state(
                "stdout_tail",
                serde_json::json!(truncate_output(&idx_output.stdout, 1000)),
            )
            .add_state(
                "stderr_tail",
                serde_json::json!(truncate_output(&idx_output.stderr, 1000)),
            );
        tracker.fail(
            E2eError::with_type("cass index --full failed", "COMMAND_FAILED").with_context(ctx),
        );
        panic!(
            "cass index --full failed (exit {:?}): {}",
            idx_output.status.code(),
            truncate_output(&idx_output.stderr, 500)
        );
    }
    tracker.end("run_index", Some("Run full index"), phase_start);

    // Phase: Search codex-specific content
    let phase_start = tracker.start(
        "search_codex_unique",
        Some("Search for codex-specific term"),
    );
    let codex_output = command_env
        .cass_assert_command()
        .args(["search", "codexonly_xyzzy", "--robot", "--data-dir"])
        .arg(&data_dir)
        .env("HOME", home)
        .output()
        .expect("search command");

    if !codex_output.status.success() {
        let ctx = E2eErrorContext::new()
            .with_command("cass search codexonly_xyzzy --robot")
            .capture_cwd()
            .add_state("exit_code", serde_json::json!(codex_output.status.code()))
            .add_state(
                "stdout_tail",
                serde_json::json!(truncate_output(&codex_output.stdout, 1000)),
            )
            .add_state(
                "stderr_tail",
                serde_json::json!(truncate_output(&codex_output.stderr, 1000)),
            );
        tracker.fail(
            E2eError::with_type("cass search codexonly_xyzzy failed", "COMMAND_FAILED")
                .with_context(ctx),
        );
        panic!(
            "cass search failed (exit {:?}): {}",
            codex_output.status.code(),
            truncate_output(&codex_output.stderr, 500)
        );
    }
    let codex_json: serde_json::Value =
        serde_json::from_slice(&codex_output.stdout).expect("valid json");
    let codex_hits = codex_json
        .get("hits")
        .and_then(|h| h.as_array())
        .expect("hits array");

    assert!(!codex_hits.is_empty(), "Should find codex-specific content");
    assert!(
        codex_hits.iter().all(|h| h["agent"] == "codex"),
        "Codex-specific search should only return codex results"
    );
    tracker.end(
        "search_codex_unique",
        Some("Search for codex-specific term"),
        phase_start,
    );

    // Phase: Search claude-specific content
    let phase_start = tracker.start(
        "search_claude_unique",
        Some("Search for claude-specific term"),
    );
    let claude_output = command_env
        .cass_assert_command()
        .args(["search", "claudeonly_plugh", "--robot", "--data-dir"])
        .arg(&data_dir)
        .env("HOME", home)
        .output()
        .expect("search command");

    if !claude_output.status.success() {
        let ctx = E2eErrorContext::new()
            .with_command("cass search claudeonly_plugh --robot")
            .capture_cwd()
            .add_state("exit_code", serde_json::json!(claude_output.status.code()))
            .add_state(
                "stdout_tail",
                serde_json::json!(truncate_output(&claude_output.stdout, 1000)),
            )
            .add_state(
                "stderr_tail",
                serde_json::json!(truncate_output(&claude_output.stderr, 1000)),
            );
        tracker.fail(
            E2eError::with_type("cass search claudeonly_plugh failed", "COMMAND_FAILED")
                .with_context(ctx),
        );
        panic!(
            "cass search failed (exit {:?}): {}",
            claude_output.status.code(),
            truncate_output(&claude_output.stderr, 500)
        );
    }
    let claude_json: serde_json::Value =
        serde_json::from_slice(&claude_output.stdout).expect("valid json");
    let claude_hits = claude_json
        .get("hits")
        .and_then(|h| h.as_array())
        .expect("hits array");

    assert!(
        !claude_hits.is_empty(),
        "Should find claude-specific content"
    );
    assert!(
        claude_hits.iter().all(|h| h["agent"] == "claude_code"),
        "Claude-specific search should only return claude_code results"
    );
    tracker.end(
        "search_claude_unique",
        Some("Search for claude-specific term"),
        phase_start,
    );

    tracker.complete();
}

/// Test: Aggregation by agent works with multiple connectors
#[test]
fn multi_connector_aggregation() {
    let tracker = tracker_for("multi_connector_aggregation");
    let tmp = tempfile::TempDir::new().unwrap();
    let home = tmp.path();
    let codex_home = home.join(".codex");
    let claude_home = home.join(".claude");
    let data_dir = home.join("cass_data");
    fs::create_dir_all(&data_dir).unwrap();

    let command_env = tracker
        .command_environment()
        .with_home(home)
        .with_codex_home(&codex_home);

    // Phase: Setup
    let phase_start = tracker.start(
        "setup_fixtures",
        Some("Create multiple sessions per connector"),
    );
    make_codex_session(
        &codex_home,
        "2024/11/20",
        "rollout-agg1.jsonl",
        "aggtest codex_first",
        1732118400000,
    );
    make_codex_session(
        &codex_home,
        "2024/11/21",
        "rollout-agg2.jsonl",
        "aggtest codex_second",
        1732204800000,
    );
    make_claude_session(
        &claude_home,
        "agg-project1",
        "session-agg1.jsonl",
        "aggtest claude_first",
        "2024-11-20T10:00:00Z",
    );
    make_claude_session(
        &claude_home,
        "agg-project2",
        "session-agg2.jsonl",
        "aggtest claude_second",
        "2024-11-21T10:00:00Z",
    );
    tracker.end(
        "setup_fixtures",
        Some("Create multiple sessions per connector"),
        phase_start,
    );

    // Phase: Index
    let phase_start = tracker.start("run_index", Some("Run full index"));
    let idx_output = command_env
        .cass_assert_command()
        .args(["index", "--full", "--data-dir"])
        .arg(&data_dir)
        .env("CODEX_HOME", &codex_home)
        .env("HOME", home)
        .output()
        .expect("failed to spawn cass index --full");
    if !idx_output.status.success() {
        let ctx = E2eErrorContext::new()
            .with_command("cass index --full (aggregation)")
            .capture_cwd()
            .add_state("exit_code", serde_json::json!(idx_output.status.code()))
            .add_state(
                "stdout_tail",
                serde_json::json!(truncate_output(&idx_output.stdout, 1000)),
            )
            .add_state(
                "stderr_tail",
                serde_json::json!(truncate_output(&idx_output.stderr, 1000)),
            );
        tracker.fail(
            E2eError::with_type("cass index --full failed", "COMMAND_FAILED").with_context(ctx),
        );
        panic!(
            "cass index --full failed (exit {:?}): {}",
            idx_output.status.code(),
            truncate_output(&idx_output.stderr, 500)
        );
    }
    tracker.end("run_index", Some("Run full index"), phase_start);

    // Phase: Aggregation search
    let phase_start = tracker.start("search_aggregate", Some("Search with agent aggregation"));
    let agg_start = std::time::Instant::now();
    let output = command_env
        .cass_assert_command()
        .args([
            "search",
            "aggtest",
            "--aggregate",
            "agent",
            "--robot",
            "--data-dir",
        ])
        .arg(&data_dir)
        .env("HOME", home)
        .output()
        .expect("search command");
    let agg_duration = agg_start.elapsed().as_millis() as u64;

    if !output.status.success() {
        let ctx = E2eErrorContext::new()
            .with_command("cass search aggtest --aggregate agent --robot")
            .capture_cwd()
            .add_state("exit_code", serde_json::json!(output.status.code()))
            .add_state(
                "stdout_tail",
                serde_json::json!(truncate_output(&output.stdout, 1000)),
            )
            .add_state(
                "stderr_tail",
                serde_json::json!(truncate_output(&output.stderr, 1000)),
            );
        tracker.fail(
            E2eError::with_type("cass search --aggregate failed", "COMMAND_FAILED")
                .with_context(ctx),
        );
        panic!(
            "cass search --aggregate failed (exit {:?}): {}",
            output.status.code(),
            truncate_output(&output.stderr, 500)
        );
    }
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("valid json");

    let aggregations = json.get("aggregations").and_then(|a| a.as_object());
    assert!(
        aggregations.is_some(),
        "Should have aggregations in response"
    );

    let aggs = aggregations.unwrap();
    let agent_agg = aggs.get("agent").and_then(|a| a.as_object());
    assert!(agent_agg.is_some(), "Should have agent aggregation");

    let buckets = agent_agg
        .unwrap()
        .get("buckets")
        .and_then(|b| b.as_array())
        .expect("Should have buckets array");

    let agent_keys: std::collections::HashSet<_> = buckets
        .iter()
        .filter_map(|b| b.get("key").and_then(|k| k.as_str()))
        .collect();

    assert!(
        agent_keys.contains("codex"),
        "Agent aggregation should include codex. Keys: {agent_keys:?}"
    );
    assert!(
        agent_keys.contains("claude_code"),
        "Agent aggregation should include claude_code. Keys: {agent_keys:?}"
    );
    tracker.end(
        "search_aggregate",
        Some("Search with agent aggregation"),
        phase_start,
    );

    tracker.metrics(
        "aggregation_query",
        &E2ePerformanceMetrics::new()
            .with_duration(agg_duration)
            .with_custom("bucket_count", serde_json::json!(buckets.len())),
    );

    tracker.complete();
}

/// Test: Incremental indexing works across multiple connectors
#[test]
fn multi_connector_incremental_index() {
    let tracker = tracker_for("multi_connector_incremental_index");
    let tmp = tempfile::TempDir::new().unwrap();
    let home = tmp.path();
    let codex_home = home.join(".codex");
    let claude_home = home.join(".claude");
    let data_dir = home.join("cass_data");
    fs::create_dir_all(&data_dir).unwrap();

    let command_env = tracker
        .command_environment()
        .with_home(home)
        .with_codex_home(&codex_home);

    // Phase: Create initial sessions
    let phase_start = tracker.start(
        "setup_initial_fixtures",
        Some("Create initial sessions for both connectors"),
    );
    make_codex_session(
        &codex_home,
        "2024/11/20",
        "rollout-incr1.jsonl",
        "incrtest initial_codex",
        1732118400000,
    );
    make_claude_session(
        &claude_home,
        "incr-project1",
        "session-incr1.jsonl",
        "incrtest initial_claude",
        "2024-11-20T10:00:00Z",
    );
    tracker.end(
        "setup_initial_fixtures",
        Some("Create initial sessions for both connectors"),
        phase_start,
    );

    // Phase: Full index
    let phase_start = tracker.start("run_full_index", Some("Run full index"));
    let idx_output = command_env
        .cass_assert_command()
        .args(["index", "--full", "--data-dir"])
        .arg(&data_dir)
        .env("CODEX_HOME", &codex_home)
        .env("HOME", home)
        .output()
        .expect("failed to spawn cass index --full");
    if !idx_output.status.success() {
        let ctx = E2eErrorContext::new()
            .with_command("cass index --full (incremental_index)")
            .capture_cwd()
            .add_state("exit_code", serde_json::json!(idx_output.status.code()))
            .add_state(
                "stdout_tail",
                serde_json::json!(truncate_output(&idx_output.stdout, 1000)),
            )
            .add_state(
                "stderr_tail",
                serde_json::json!(truncate_output(&idx_output.stderr, 1000)),
            );
        tracker.fail(
            E2eError::with_type("cass index --full failed", "COMMAND_FAILED").with_context(ctx),
        );
        panic!(
            "cass index --full failed (exit {:?}): {}",
            idx_output.status.code(),
            truncate_output(&idx_output.stderr, 500)
        );
    }
    tracker.end("run_full_index", Some("Run full index"), phase_start);

    // Phase: Verify initial index
    let phase_start = tracker.start(
        "verify_initial_index",
        Some("Verify initial sessions indexed"),
    );
    let output1 = command_env
        .cass_assert_command()
        .args(["search", "incrtest", "--robot", "--data-dir"])
        .arg(&data_dir)
        .env("HOME", home)
        .output()
        .expect("search command");

    let json1: serde_json::Value = serde_json::from_slice(&output1.stdout).expect("valid json");
    let hits1 = json1
        .get("hits")
        .and_then(|h| h.as_array())
        .expect("hits array");
    assert!(hits1.len() >= 2, "Should have initial sessions indexed");
    tracker.end(
        "verify_initial_index",
        Some("Verify initial sessions indexed"),
        phase_start,
    );

    // Phase: Add new sessions and run incremental index
    let phase_start = tracker.start(
        "incremental_index",
        Some("Add new sessions and run incremental index"),
    );
    std::thread::sleep(std::time::Duration::from_secs(2));

    let now_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let now_iso = chrono::Utc::now().to_rfc3339();

    make_codex_session(
        &codex_home,
        "2024/11/21",
        "rollout-incr2.jsonl",
        "incrtest new_codex",
        now_ts,
    );
    make_claude_session(
        &claude_home,
        "incr-project2",
        "session-incr2.jsonl",
        "incrtest new_claude",
        &now_iso,
    );

    let incr_start = std::time::Instant::now();
    let incr_idx_output = command_env
        .cass_assert_command()
        .args(["index", "--data-dir"])
        .arg(&data_dir)
        .env("CODEX_HOME", &codex_home)
        .env("HOME", home)
        .output()
        .expect("failed to spawn cass index (incremental)");
    if !incr_idx_output.status.success() {
        let ctx = E2eErrorContext::new()
            .with_command("cass index (incremental)")
            .capture_cwd()
            .add_state(
                "exit_code",
                serde_json::json!(incr_idx_output.status.code()),
            )
            .add_state(
                "stdout_tail",
                serde_json::json!(truncate_output(&incr_idx_output.stdout, 1000)),
            )
            .add_state(
                "stderr_tail",
                serde_json::json!(truncate_output(&incr_idx_output.stderr, 1000)),
            );
        tracker.fail(
            E2eError::with_type("cass index (incremental) failed", "COMMAND_FAILED")
                .with_context(ctx),
        );
        panic!(
            "cass index (incremental) failed (exit {:?}): {}",
            incr_idx_output.status.code(),
            truncate_output(&incr_idx_output.stderr, 500)
        );
    }
    let incr_duration = incr_start.elapsed().as_millis() as u64;
    tracker.end(
        "incremental_index",
        Some("Add new sessions and run incremental index"),
        phase_start,
    );

    tracker.metrics(
        "incremental_index",
        &E2ePerformanceMetrics::new().with_duration(incr_duration),
    );

    // Phase: Verify incremental results
    let phase_start = tracker.start(
        "verify_incremental",
        Some("Verify all sessions indexed after incremental"),
    );
    let output2 = command_env
        .cass_assert_command()
        .args(["search", "incrtest", "--robot", "--data-dir"])
        .arg(&data_dir)
        .env("HOME", home)
        .output()
        .expect("search command");

    let json2: serde_json::Value = serde_json::from_slice(&output2.stdout).expect("valid json");
    let hits2 = json2
        .get("hits")
        .and_then(|h| h.as_array())
        .expect("hits array");

    assert!(
        hits2.len() > hits1.len(),
        "Incremental index should add new sessions. hits1={}, hits2={}",
        hits1.len(),
        hits2.len()
    );

    let has_initial = hits2
        .iter()
        .any(|h| h["content"].as_str().unwrap_or("").contains("initial"));
    let has_new = hits2
        .iter()
        .any(|h| h["content"].as_str().unwrap_or("").contains("new"));

    assert!(
        has_initial,
        "Should still have initial sessions after incremental index"
    );
    assert!(has_new, "Should have new sessions after incremental index");
    tracker.end(
        "verify_incremental",
        Some("Verify all sessions indexed after incremental"),
        phase_start,
    );

    tracker.metrics(
        "incremental_results",
        &E2ePerformanceMetrics::new()
            .with_custom("initial_hit_count", serde_json::json!(hits1.len()))
            .with_custom("final_hit_count", serde_json::json!(hits2.len())),
    );

    tracker.complete();
}

/// Test: Multiple agent filter works correctly
#[test]
fn multi_connector_multiple_agent_filter() {
    let tracker = tracker_for("multi_connector_multiple_agent_filter");
    let tmp = tempfile::TempDir::new().unwrap();
    let home = tmp.path();
    let codex_home = home.join(".codex");
    let claude_home = home.join(".claude");
    let data_dir = home.join("cass_data");
    fs::create_dir_all(&data_dir).unwrap();

    let command_env = tracker
        .command_environment()
        .with_home(home)
        .with_codex_home(&codex_home);

    // Phase: Setup
    let phase_start = tracker.start(
        "setup_fixtures",
        Some("Create sessions for both connectors"),
    );
    make_codex_session(
        &codex_home,
        "2024/11/20",
        "rollout-maf.jsonl",
        "multiagent codex_content",
        1732118400000,
    );
    make_claude_session(
        &claude_home,
        "multi-agent-project",
        "session-maf.jsonl",
        "multiagent claude_content",
        "2024-11-20T10:00:00Z",
    );
    tracker.end(
        "setup_fixtures",
        Some("Create sessions for both connectors"),
        phase_start,
    );

    // Phase: Index
    let phase_start = tracker.start("run_index", Some("Run full index"));
    let idx_output = command_env
        .cass_assert_command()
        .args(["index", "--full", "--data-dir"])
        .arg(&data_dir)
        .env("CODEX_HOME", &codex_home)
        .env("HOME", home)
        .output()
        .expect("failed to spawn cass index --full");
    if !idx_output.status.success() {
        let ctx = E2eErrorContext::new()
            .with_command("cass index --full (multiple_agent_filter)")
            .capture_cwd()
            .add_state("exit_code", serde_json::json!(idx_output.status.code()))
            .add_state(
                "stdout_tail",
                serde_json::json!(truncate_output(&idx_output.stdout, 1000)),
            )
            .add_state(
                "stderr_tail",
                serde_json::json!(truncate_output(&idx_output.stderr, 1000)),
            );
        tracker.fail(
            E2eError::with_type("cass index --full failed", "COMMAND_FAILED").with_context(ctx),
        );
        panic!(
            "cass index --full failed (exit {:?}): {}",
            idx_output.status.code(),
            truncate_output(&idx_output.stderr, 500)
        );
    }
    tracker.end("run_index", Some("Run full index"), phase_start);

    // Phase: Multi-agent filter search
    let phase_start = tracker.start(
        "search_multi_agent_filter",
        Some("Search with multiple --agent filters"),
    );
    let search_start = std::time::Instant::now();
    let output = command_env
        .cass_assert_command()
        .args([
            "search",
            "multiagent",
            "--agent",
            "codex",
            "--agent",
            "claude_code",
            "--robot",
            "--data-dir",
        ])
        .arg(&data_dir)
        .env("HOME", home)
        .output()
        .expect("search command");
    let search_duration = search_start.elapsed().as_millis() as u64;

    if !output.status.success() {
        let ctx = E2eErrorContext::new()
            .with_command("cass search multiagent --agent codex --agent claude_code --robot")
            .capture_cwd()
            .add_state("exit_code", serde_json::json!(output.status.code()))
            .add_state(
                "stdout_tail",
                serde_json::json!(truncate_output(&output.stdout, 1000)),
            )
            .add_state(
                "stderr_tail",
                serde_json::json!(truncate_output(&output.stderr, 1000)),
            );
        tracker.fail(
            E2eError::with_type("cass search --agent multi failed", "COMMAND_FAILED")
                .with_context(ctx),
        );
        panic!(
            "cass search --agent multi failed (exit {:?}): {}",
            output.status.code(),
            truncate_output(&output.stderr, 500)
        );
    }
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("valid json");
    let hits = json
        .get("hits")
        .and_then(|h| h.as_array())
        .expect("hits array");

    let agents: std::collections::HashSet<_> =
        hits.iter().filter_map(|h| h["agent"].as_str()).collect();

    assert!(
        agents.contains("codex") && agents.contains("claude_code"),
        "Should find results from both specified agents. Found: {agents:?}"
    );
    tracker.end(
        "search_multi_agent_filter",
        Some("Search with multiple --agent filters"),
        phase_start,
    );

    tracker.metrics(
        "multi_agent_filter_query",
        &E2ePerformanceMetrics::new()
            .with_duration(search_duration)
            .with_custom("hit_count", serde_json::json!(hits.len()))
            .with_custom("agent_count", serde_json::json!(agents.len())),
    );

    tracker.complete();
}

/// Test: Empty connector doesn't break indexing of other connectors
#[test]
fn multi_connector_empty_connector() {
    let tracker = tracker_for("multi_connector_empty_connector");
    let tmp = tempfile::TempDir::new().unwrap();
    let home = tmp.path();
    let codex_home = home.join(".codex");
    let data_dir = home.join("cass_data");

    fs::create_dir_all(&data_dir).unwrap();

    let command_env = tracker
        .command_environment()
        .with_home(home)
        .with_codex_home(&codex_home);

    // Phase: Setup (only codex, no claude)
    let phase_start = tracker.start(
        "setup_fixtures",
        Some("Create only Codex session, no Claude"),
    );
    make_codex_session(
        &codex_home,
        "2024/11/20",
        "rollout-only.jsonl",
        "singleconnector codex_only",
        1732118400000,
    );
    tracker.end(
        "setup_fixtures",
        Some("Create only Codex session, no Claude"),
        phase_start,
    );

    // Phase: Index with missing connector
    let phase_start = tracker.start("run_index", Some("Index with non-existent claude_home"));
    let idx_output = command_env
        .cass_assert_command()
        .args(["index", "--full", "--data-dir"])
        .arg(&data_dir)
        .env("CODEX_HOME", &codex_home)
        .env("HOME", home)
        .output()
        .expect("failed to spawn cass index --full");
    if !idx_output.status.success() {
        let ctx = E2eErrorContext::new()
            .with_command("cass index --full (empty_connector)")
            .capture_cwd()
            .add_state("exit_code", serde_json::json!(idx_output.status.code()))
            .add_state(
                "stdout_tail",
                serde_json::json!(truncate_output(&idx_output.stdout, 1000)),
            )
            .add_state(
                "stderr_tail",
                serde_json::json!(truncate_output(&idx_output.stderr, 1000)),
            );
        tracker.fail(
            E2eError::with_type("cass index --full failed", "COMMAND_FAILED").with_context(ctx),
        );
        panic!(
            "cass index --full failed (exit {:?}): {}",
            idx_output.status.code(),
            truncate_output(&idx_output.stderr, 500)
        );
    }
    tracker.end(
        "run_index",
        Some("Index with non-existent claude_home"),
        phase_start,
    );

    // Phase: Search and verify
    let phase_start = tracker.start(
        "verify_results",
        Some("Search and verify codex-only results"),
    );
    let output = command_env
        .cass_assert_command()
        .args(["search", "singleconnector", "--robot", "--data-dir"])
        .arg(&data_dir)
        .env("HOME", home)
        .output()
        .expect("search command");

    if !output.status.success() {
        let ctx = E2eErrorContext::new()
            .with_command("cass search singleconnector --robot")
            .capture_cwd()
            .add_state("exit_code", serde_json::json!(output.status.code()))
            .add_state(
                "stdout_tail",
                serde_json::json!(truncate_output(&output.stdout, 1000)),
            )
            .add_state(
                "stderr_tail",
                serde_json::json!(truncate_output(&output.stderr, 1000)),
            );
        tracker.fail(
            E2eError::with_type("cass search singleconnector failed", "COMMAND_FAILED")
                .with_context(ctx),
        );
        panic!(
            "cass search failed (exit {:?}): {}",
            output.status.code(),
            truncate_output(&output.stderr, 500)
        );
    }
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("valid json");
    let hits = json
        .get("hits")
        .and_then(|h| h.as_array())
        .expect("hits array");

    assert!(!hits.is_empty(), "Should find codex results");
    assert!(
        hits.iter().all(|h| h["agent"] == "codex"),
        "All results should be from codex"
    );
    tracker.end(
        "verify_results",
        Some("Search and verify codex-only results"),
        phase_start,
    );

    tracker.complete();
}
