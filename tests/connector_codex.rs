use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;

use coding_agent_search::connectors::{Connector, ScanContext, ScanRoot, codex::CodexConnector};
use serde_json::Value;

fn codex_real_fixture_home() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/codex_real")
}

#[test]
fn codex_connector_reads_modern_envelope_jsonl() {
    let dir = TempDir::new().unwrap();
    let sessions = dir.path().join("sessions/2025/11/21");
    fs::create_dir_all(&sessions).unwrap();
    let file = sessions.join("rollout-1.jsonl");

    // Modern envelope format with {type, timestamp, payload}
    let sample = r#"{"timestamp":"2025-09-30T15:42:34.559Z","type":"session_meta","payload":{"id":"test-id","cwd":"/test/workspace","cli_version":"0.42.0"}}
{"timestamp":"2025-09-30T15:42:36.190Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"write a hello program"}]}}
{"timestamp":"2025-09-30T15:42:43.000Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"text","text":"here is code"}]}}
"#;
    fs::write(&file, sample).unwrap();

    let connector = CodexConnector::new();
    // qu81y: the tempdir codex home is passed as an EXPLICIT scan root
    // instead of mutating process-global CODEX_HOME.
    let ctx = ScanContext::with_roots(
        dir.path().to_path_buf(),
        vec![ScanRoot::local(dir.path().to_path_buf())],
        None,
    );
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);
    let c = &convs[0];
    assert_eq!(c.agent_slug, "codex");
    assert_eq!(c.messages.len(), 2);
    assert!(c.title.as_ref().unwrap().contains("write a hello program"));
    // Verify workspace was extracted from session_meta
    assert_eq!(c.workspace, Some(PathBuf::from("/test/workspace")));
    // Bead 7k7pl: pin timestamps parsed from the 2025-09-30 ISO-8601
    // fixture — both must be plausible ms-epoch values (>= 2020) and
    // started must be strictly before ended. A regression that
    // dropped ISO parsing (emitting 0 / default) would slip past
    // `.is_some()`.
    let started = c
        .started_at
        .expect("started_at must be parsed from ISO-8601");
    let ended = c.ended_at.expect("ended_at must be parsed from ISO-8601");
    // ms-epoch floor: 2020-01-01 (1_577_836_800_000 ms). Fixture is
    // 2025 so this is well below the real value but catches 0/MIN.
    assert!(
        started >= 1_577_836_800_000,
        "started_at must be parsed from the 2025 fixture (>= 2020); got {started}"
    );
    assert!(
        started < ended,
        "started_at must be strictly before ended_at; got started={started}, ended={ended}"
    );
}

#[test]
fn codex_connector_includes_agent_reasoning() {
    let dir = TempDir::new().unwrap();
    let sessions = dir.path().join("sessions/2025/11/22");
    fs::create_dir_all(&sessions).unwrap();
    let file = sessions.join("rollout-reasoning.jsonl");

    let sample = r#"{"timestamp":"2025-09-30T15:42:34.559Z","type":"session_meta","payload":{"id":"test-id","cwd":"/test"}}
{"timestamp":"2025-09-30T15:42:36.190Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"solve this problem"}]}}
{"timestamp":"2025-09-30T15:42:40.000Z","type":"event_msg","payload":{"type":"agent_reasoning","text":"Let me think about this carefully..."}}
{"timestamp":"2025-09-30T15:42:43.000Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"text","text":"here is solution"}]}}
{"timestamp":"2025-09-30T15:42:45.000Z","type":"event_msg","payload":{"type":"token_count","input_tokens":100,"output_tokens":200}}
"#;
    fs::write(&file, sample).unwrap();

    let connector = CodexConnector::new();
    // qu81y: the tempdir codex home is passed as an EXPLICIT scan root
    // instead of mutating process-global CODEX_HOME.
    let ctx = ScanContext::with_roots(
        dir.path().to_path_buf(),
        vec![ScanRoot::local(dir.path().to_path_buf())],
        None,
    );
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);
    let c = &convs[0];

    // Should have 3 messages: user, reasoning, assistant
    // (token_count does not create a synthetic message)
    assert_eq!(c.messages.len(), 3);

    // Check reasoning is included with correct author tag
    let reasoning = c
        .messages
        .iter()
        .find(|m| m.author.as_deref() == Some("reasoning"));
    assert!(reasoning.is_some());
    assert!(
        reasoning
            .unwrap()
            .content
            .contains("think about this carefully")
    );

    let assistant = c.messages.iter().find(|m| {
        m.role == "assistant" && m.author.is_none() && m.content.contains("here is solution")
    });
    assert!(assistant.is_some());
    let assistant = assistant.unwrap();
    assert_eq!(
        assistant
            .extra
            .pointer("/cass/token_usage/input_tokens")
            .and_then(|v| v.as_i64()),
        Some(100)
    );
    assert_eq!(
        assistant
            .extra
            .pointer("/cass/token_usage/output_tokens")
            .and_then(|v| v.as_i64()),
        Some(200)
    );
}

#[test]
fn codex_connector_parses_real_tool_call_fixture() {
    let fixture_home = codex_real_fixture_home();
    let expected_path = fixture_home.join("sessions/2025/11/26/rollout-tool-call.jsonl");

    let connector = CodexConnector::new();
    // qu81y: committed fixture home passed as an explicit scan root.
    let ctx = ScanContext::with_roots(
        fixture_home.clone(),
        vec![ScanRoot::local(fixture_home.clone())],
        None,
    );
    let convs = connector.scan(&ctx).unwrap();
    let conv = convs
        .into_iter()
        .find(|conv| conv.source_path == expected_path)
        .expect("real Codex tool-call fixture should be discoverable");

    assert_eq!(conv.agent_slug, "codex");
    assert_eq!(conv.workspace, Some(PathBuf::from("/test/soldier/project")));
    assert_eq!(
        conv.external_id,
        Some("2025/11/26/rollout-tool-call".to_string())
    );
    assert_eq!(
        conv.title,
        Some(
            "Please trace the tool_call branch in the Codex connector and confirm invocation extraction."
                .to_string()
        )
    );
    assert_eq!(conv.messages.len(), 3);

    let tool_msg = &conv.messages[1];
    assert_eq!(tool_msg.idx, 1);
    assert_eq!(tool_msg.role, "assistant");
    assert_eq!(tool_msg.author, None);
    assert_eq!(tool_msg.content, "[Tool: bash]");
    assert_eq!(tool_msg.invocations.len(), 1);
    assert!(tool_msg.extra.get("payload").is_some());

    let invocation = &tool_msg.invocations[0];
    assert_eq!(invocation.kind, "tool");
    assert_eq!(invocation.name, "bash");
    assert_eq!(invocation.call_id.as_deref(), Some("call_codex_tool_001"));
    assert_eq!(
        invocation
            .arguments
            .as_ref()
            .and_then(|args| args.get("cmd"))
            .and_then(|value| value.as_str()),
        Some("rg -n tool_call src/connectors/codex.rs")
    );
    assert!(
        tool_msg.extra.pointer("/cass/token_usage").is_none(),
        "token usage should attach to the later assistant turn, not the tool_call message"
    );

    let assistant = &conv.messages[2];
    assert_eq!(assistant.idx, 2);
    assert_eq!(assistant.role, "assistant");
    assert!(assistant.content.contains("invocation is emitted"));
    assert_eq!(
        assistant
            .extra
            .pointer("/cass/token_usage/input_tokens")
            .and_then(|v| v.as_i64()),
        Some(120)
    );
    assert_eq!(
        assistant
            .extra
            .pointer("/cass/token_usage/output_tokens")
            .and_then(|v| v.as_i64()),
        Some(45)
    );
}

#[test]
fn codex_connector_indexes_modern_response_items() {
    let dir = TempDir::new().unwrap();
    let sessions = dir.path().join("sessions/2026/05/08");
    fs::create_dir_all(&sessions).unwrap();
    let file = sessions.join("rollout-modern.jsonl");

    let sample = r#"{"timestamp":"2026-05-08T23:09:00.000Z","type":"session_meta","payload":{"id":"modern-id","cwd":"/data/projects/ntm","cli_version":"0.49.0"}}
{"timestamp":"2026-05-08T23:09:01.000Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"investigate cass"}]}}
{"timestamp":"2026-05-08T23:09:02.000Z","type":"response_item","payload":{"type":"function_call","name":"exec_command","arguments":"{\"cmd\":\"git log --grep='bd-2mb03'\"}","call_id":"call-modern-1"}}
{"timestamp":"2026-05-08T23:09:03.000Z","type":"response_item","payload":{"type":"function_call_output","call_id":"call-modern-1","output":"Output:\ncommit abc123 bd-2mb03.6.5 add CASS-backed handoff context enrichment\n"}}
{"timestamp":"2026-05-08T23:09:04.000Z","type":"event_msg","payload":{"type":"agent_message","message":"The raw session contains bd-2mb03."}}
{"timestamp":"2026-05-08T23:09:04.000Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"The raw session contains bd-2mb03."}],"phase":"commentary"}}
"#;
    fs::write(&file, sample).unwrap();

    let connector = CodexConnector::new();
    // qu81y: the tempdir codex home is passed as an EXPLICIT scan root
    // instead of mutating process-global CODEX_HOME.
    let ctx = ScanContext::with_roots(
        dir.path().to_path_buf(),
        vec![ScanRoot::local(dir.path().to_path_buf())],
        None,
    );
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);

    let conv = &convs[0];
    let tool_calls: Vec<_> = conv
        .messages
        .iter()
        .filter(|message| {
            message
                .invocations
                .iter()
                .any(|invocation| invocation.call_id.as_deref() == Some("call-modern-1"))
        })
        .collect();
    assert_eq!(
        tool_calls.len(),
        1,
        "function_call should not be duplicated"
    );
    let tool_call = tool_calls[0];
    assert_eq!(
        tool_call.invocations.len(),
        1,
        "function_call should produce exactly one invocation"
    );
    assert!(
        tool_call.content.contains("git log --grep='bd-2mb03'"),
        "function_call arguments should be searchable"
    );
    let invocation = &tool_call.invocations[0];
    assert_eq!(invocation.name, "exec_command");
    assert_eq!(invocation.call_id.as_deref(), Some("call-modern-1"));
    assert_eq!(
        invocation
            .arguments
            .as_ref()
            .and_then(|arguments| arguments.get("cmd"))
            .and_then(Value::as_str),
        Some("git log --grep='bd-2mb03'")
    );
    assert!(
        conv.messages
            .iter()
            .any(|message| message.role == "tool" && message.content.contains("bd-2mb03.6.5")),
        "function_call_output text should be searchable"
    );
    assert_eq!(
        conv.messages
            .iter()
            .filter(|message| message.content == "The raw session contains bd-2mb03.")
            .count(),
        1,
        "agent_message and output_text duplicates should collapse to one searchable message"
    );
}

#[test]
fn codex_connector_scans_explicit_rollout_file_root() {
    let dir = TempDir::new().unwrap();
    let sessions = dir.path().join(".codex/sessions/2026/05/08");
    fs::create_dir_all(&sessions).unwrap();
    let file = sessions.join("rollout-explicit.jsonl");

    let sample = r#"{"timestamp":"2026-05-08T23:09:00.000Z","type":"session_meta","payload":{"id":"explicit-id","cwd":"/data/projects/ntm","cli_version":"0.49.0"}}
{"timestamp":"2026-05-08T23:09:01.000Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"explicit watch once bd-2mb03"}]}}
{"timestamp":"2026-05-08T23:09:02.000Z","type":"response_item","payload":{"type":"function_call_output","call_id":"call-explicit","output":"bd-2mb03 direct file root output\n"}}
"#;
    fs::write(&file, sample).unwrap();

    let connector = CodexConnector::new();
    let ctx = ScanContext {
        data_dir: dir.path().join(".codex"),
        scan_roots: vec![ScanRoot::local(file.clone())],
        since_ts: None,
        progress_tick: None,
        allow_local_default_stores: false,
    };
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);

    let conv = &convs[0];
    assert_eq!(conv.source_path, file);
    assert_eq!(conv.workspace, Some(PathBuf::from("/data/projects/ntm")));
    assert!(
        conv.messages
            .iter()
            .any(|message| message.content.contains("bd-2mb03 direct file root")),
        "explicit file-root scans must index modern response items"
    );
}

#[test]
fn codex_connector_ignores_unmatched_token_count() {
    let dir = TempDir::new().unwrap();
    let sessions = dir.path().join("sessions/2025/11/23");
    fs::create_dir_all(&sessions).unwrap();
    let file = sessions.join("rollout-filter.jsonl");

    let sample = r#"{"timestamp":"2025-09-30T15:42:34.559Z","type":"session_meta","payload":{"id":"test-id","cwd":"/test"}}
{"timestamp":"2025-09-30T15:42:36.190Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}}
{"timestamp":"2025-09-30T15:42:37.000Z","type":"event_msg","payload":{"type":"token_count","input_tokens":10,"output_tokens":20}}
{"timestamp":"2025-09-30T15:42:38.000Z","type":"turn_context","payload":{"turn":1}}
{"timestamp":"2025-09-30T15:42:39.000Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"text","text":"world"}]}}
"#;
    fs::write(&file, sample).unwrap();

    let connector = CodexConnector::new();
    // qu81y: the tempdir codex home is passed as an EXPLICIT scan root
    // instead of mutating process-global CODEX_HOME.
    let ctx = ScanContext::with_roots(
        dir.path().to_path_buf(),
        vec![ScanRoot::local(dir.path().to_path_buf())],
        None,
    );
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);
    let c = &convs[0];

    // Should only have 2 messages (user, assistant).
    // token_count and turn_context do not create searchable messages.
    assert_eq!(c.messages.len(), 2);

    for msg in &c.messages {
        assert!(!msg.content.contains("token_count"));
        assert!(!msg.content.contains("turn_context"));
        assert!(!msg.content.trim().is_empty());
    }

    // token_count occurs before the first assistant turn and must not attach forward.
    let assistant = c.messages.iter().find(|m| m.role == "assistant").unwrap();
    assert!(
        assistant.extra.pointer("/cass/token_usage").is_none(),
        "unmatched token_count should be ignored"
    );
}

/// Test that since_ts uses FILE-LEVEL filtering, not message-level.
///
/// NOTE: We intentionally removed message-level timestamp filtering because
/// it caused data loss during incremental re-indexing. When a file is modified,
/// ALL messages in that file are ingested, regardless of individual timestamps.
/// The since_ts is ONLY used to decide whether to process the file at all
/// (based on file mtime vs since_ts).
#[test]
fn codex_connector_respects_since_ts_at_file_level_only() {
    let dir = TempDir::new().unwrap();
    let sessions = dir.path().join("sessions/2025/11/24");
    fs::create_dir_all(&sessions).unwrap();
    let file = sessions.join("rollout-since.jsonl");

    // Two messages with different timestamps - both should be included
    // since since_ts filtering happens at the FILE level, not message level.
    let sample = r#"{"timestamp":"2025-09-30T15:42:34.000Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"old msg"}]}}
{"timestamp":1700000100000,"type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"text","text":"new msg"}]}}
"#;
    fs::write(&file, sample).unwrap();

    let connector = CodexConnector::new();
    // since_ts does NOT filter individual messages anymore - only whole files
    let ctx = ScanContext::with_roots(
        dir.path().to_path_buf(),
        vec![ScanRoot::local(dir.path().to_path_buf())],
        Some(1_700_000_000_000),
    );
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);
    let c = &convs[0];

    // BOTH messages should be present - we don't filter by message timestamp
    assert_eq!(
        c.messages.len(),
        2,
        "file-level filtering means all messages in a processed file are included"
    );
    // Messages should have correct roles
    assert_eq!(c.messages[0].role, "user");
    assert!(c.messages[0].content.contains("old msg"));
    assert_eq!(c.messages[1].role, "assistant");
    assert!(c.messages[1].content.contains("new msg"));
}

/// Test legacy .json format parsing
#[test]
fn codex_connector_reads_legacy_json_format() {
    let dir = TempDir::new().unwrap();
    let sessions = dir.path().join("sessions/2025/11/25");
    fs::create_dir_all(&sessions).unwrap();
    let file = sessions.join("rollout-legacy.json");

    // Legacy format: single JSON object with session and items
    let sample = r#"{
        "session": {
            "id": "legacy-session",
            "cwd": "/legacy/workspace"
        },
        "items": [
            {
                "role": "user",
                "timestamp": "2025-09-30T15:42:36.190Z",
                "content": "legacy user message"
            },
            {
                "role": "assistant",
                "timestamp": "2025-09-30T15:42:43.000Z",
                "content": "legacy assistant response"
            }
        ]
    }"#;
    fs::write(&file, sample).unwrap();

    let connector = CodexConnector::new();
    // qu81y: the tempdir codex home is passed as an EXPLICIT scan root
    // instead of mutating process-global CODEX_HOME.
    let ctx = ScanContext::with_roots(
        dir.path().to_path_buf(),
        vec![ScanRoot::local(dir.path().to_path_buf())],
        None,
    );
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);

    let c = &convs[0];
    assert_eq!(c.agent_slug, "codex");
    assert_eq!(c.messages.len(), 2);
    assert_eq!(c.workspace, Some(PathBuf::from("/legacy/workspace")));

    // Verify metadata indicates legacy format
    assert_eq!(
        c.metadata.get("source").and_then(|v| v.as_str()),
        Some("rollout_json")
    );

    // Check messages
    assert_eq!(c.messages[0].role, "user");
    assert!(c.messages[0].content.contains("legacy user message"));
    assert_eq!(c.messages[1].role, "assistant");
}

/// Test detection with existing sessions directory
#[test]
fn codex_detect_with_sessions_dir() {
    let dir = TempDir::new().unwrap();
    let sessions = dir.path().join("sessions");
    fs::create_dir_all(&sessions).unwrap();

    // qu81y: detection is exercised through FAD's explicit root-override
    // input (the same root CODEX_HOME=<dir> used to resolve to, i.e.
    // <dir>/sessions) instead of mutating process-global env.
    let report = franken_agent_detection::detect_installed_agents(
        &franken_agent_detection::AgentDetectOptions {
            only_connectors: Some(vec!["codex".to_string()]),
            include_undetected: true,
            root_overrides: vec![franken_agent_detection::AgentDetectRootOverride {
                slug: "codex".to_string(),
                root: sessions,
            }],
        },
    )
    .expect("codex detection report");
    let entry = &report.installed_agents[0];
    assert!(entry.detected);
    assert!(!entry.evidence.is_empty());
}

/// Test detection without sessions directory
#[test]
fn codex_detect_without_sessions_dir() {
    let dir = TempDir::new().unwrap();
    // Don't create the sessions directory: the overridden probe root does
    // not exist, so detection must report not-found.
    let report = franken_agent_detection::detect_installed_agents(
        &franken_agent_detection::AgentDetectOptions {
            only_connectors: Some(vec!["codex".to_string()]),
            include_undetected: true,
            root_overrides: vec![franken_agent_detection::AgentDetectRootOverride {
                slug: "codex".to_string(),
                root: dir.path().join("sessions"),
            }],
        },
    )
    .expect("codex detection report");
    let entry = &report.installed_agents[0];
    assert!(!entry.detected);
}

/// Test `user_message` event type
#[test]
fn codex_connector_handles_user_message_event() {
    let dir = TempDir::new().unwrap();
    let sessions = dir.path().join("sessions/2025/11/26");
    fs::create_dir_all(&sessions).unwrap();
    let file = sessions.join("rollout-user-event.jsonl");

    let sample = r#"{"timestamp":"2025-09-30T15:42:34.559Z","type":"session_meta","payload":{"id":"test-id","cwd":"/test"}}
{"timestamp":"2025-09-30T15:42:36.190Z","type":"event_msg","payload":{"type":"user_message","message":"user event message"}}
{"timestamp":"2025-09-30T15:42:43.000Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"text","text":"assistant reply"}]}}
"#;
    fs::write(&file, sample).unwrap();

    let connector = CodexConnector::new();
    // qu81y: the tempdir codex home is passed as an EXPLICIT scan root
    // instead of mutating process-global CODEX_HOME.
    let ctx = ScanContext::with_roots(
        dir.path().to_path_buf(),
        vec![ScanRoot::local(dir.path().to_path_buf())],
        None,
    );
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);

    let c = &convs[0];
    assert_eq!(c.messages.len(), 2);

    // First message should be the user event
    assert_eq!(c.messages[0].role, "user");
    assert!(c.messages[0].content.contains("user event message"));
}

/// Test malformed JSONL lines are skipped gracefully
#[test]
fn codex_connector_skips_malformed_lines() {
    let dir = TempDir::new().unwrap();
    let sessions = dir.path().join("sessions/2025/11/27");
    fs::create_dir_all(&sessions).unwrap();
    let file = sessions.join("rollout-malformed.jsonl");

    let sample = r#"{"timestamp":"2025-09-30T15:42:34.559Z","type":"session_meta","payload":{"id":"test-id","cwd":"/test"}}
{ this is not valid json
{"timestamp":"2025-09-30T15:42:36.190Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"valid message"}]}}
also not valid
{"timestamp":"2025-09-30T15:42:43.000Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"text","text":"valid response"}]}}
"#;
    fs::write(&file, sample).unwrap();

    let connector = CodexConnector::new();
    // qu81y: the tempdir codex home is passed as an EXPLICIT scan root
    // instead of mutating process-global CODEX_HOME.
    let ctx = ScanContext::with_roots(
        dir.path().to_path_buf(),
        vec![ScanRoot::local(dir.path().to_path_buf())],
        None,
    );
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);

    let c = &convs[0];
    // Should have 2 valid messages, malformed lines skipped
    assert_eq!(c.messages.len(), 2);
}

/// Test multiple sessions in separate files
#[test]
fn codex_connector_handles_multiple_sessions() {
    let dir = TempDir::new().unwrap();
    let sessions = dir.path().join("sessions/2025/11/28");
    fs::create_dir_all(&sessions).unwrap();

    for i in 1..=3 {
        let file = sessions.join(format!("rollout-{i}.jsonl"));
        let sample = format!(
            r#"{{"timestamp":"2025-09-30T15:42:34.559Z","type":"session_meta","payload":{{"id":"session-{i}","cwd":"/test/{i}"}}}}
{{"timestamp":"2025-09-30T15:42:36.190Z","type":"response_item","payload":{{"type":"message","role":"user","content":[{{"type":"input_text","text":"message {i}"}}]}}}}
"#
        );
        fs::write(&file, sample).unwrap();
    }

    let connector = CodexConnector::new();
    // qu81y: the tempdir codex home is passed as an EXPLICIT scan root
    // instead of mutating process-global CODEX_HOME.
    let ctx = ScanContext::with_roots(
        dir.path().to_path_buf(),
        vec![ScanRoot::local(dir.path().to_path_buf())],
        None,
    );
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 3);
}

/// Test empty content messages are filtered
///
/// (qu81y: this suite no longer mutates CODEX_HOME — every test passes its
/// tempdir as an explicit ScanContext scan root, so parallel scheduling is
/// safe without serial_test.)
#[test]
fn codex_connector_filters_empty_content() {
    let dir = TempDir::new().unwrap();
    let sessions = dir.path().join("sessions/2025/11/29");
    fs::create_dir_all(&sessions).unwrap();
    let file = sessions.join("rollout-empty.jsonl");

    let sample = r#"{"timestamp":"2025-09-30T15:42:34.559Z","type":"session_meta","payload":{"id":"test-id","cwd":"/test"}}
{"timestamp":"2025-09-30T15:42:36.190Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"   "}]}}
{"timestamp":"2025-09-30T15:42:37.000Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"valid content"}]}}
{"timestamp":"2025-09-30T15:42:43.000Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[]}}
"#;
    fs::write(&file, sample).unwrap();

    let connector = CodexConnector::new();
    // qu81y: the tempdir codex home is passed as an EXPLICIT scan root
    // instead of mutating process-global CODEX_HOME.
    let ctx = ScanContext::with_roots(
        dir.path().to_path_buf(),
        vec![ScanRoot::local(dir.path().to_path_buf())],
        None,
    );
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);

    let c = &convs[0];
    // Only the message with "valid content" should be included
    assert_eq!(c.messages.len(), 1);
    assert!(c.messages[0].content.contains("valid content"));
}

/// Test title extraction from first user message
///
#[test]
fn codex_connector_extracts_title() {
    let dir = TempDir::new().unwrap();
    let sessions = dir.path().join("sessions/2025/11/30");
    fs::create_dir_all(&sessions).unwrap();
    let file = sessions.join("rollout-title.jsonl");

    let sample = r#"{"timestamp":"2025-09-30T15:42:34.559Z","type":"session_meta","payload":{"id":"test-id","cwd":"/test"}}
{"timestamp":"2025-09-30T15:42:35.000Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"text","text":"assistant first"}]}}
{"timestamp":"2025-09-30T15:42:36.190Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"This is the user's question\nWith a second line"}]}}
"#;
    fs::write(&file, sample).unwrap();

    let connector = CodexConnector::new();
    // qu81y: the tempdir codex home is passed as an EXPLICIT scan root
    // instead of mutating process-global CODEX_HOME.
    let ctx = ScanContext::with_roots(
        dir.path().to_path_buf(),
        vec![ScanRoot::local(dir.path().to_path_buf())],
        None,
    );
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);

    let c = &convs[0];
    // Title should be first line of first user message
    assert_eq!(c.title, Some("This is the user's question".to_string()));
}

/// Test sequential index assignment
#[test]
fn codex_connector_assigns_sequential_indices() {
    let dir = TempDir::new().unwrap();
    let sessions = dir.path().join("sessions/2025/12/01");
    fs::create_dir_all(&sessions).unwrap();
    let file = sessions.join("rollout-idx.jsonl");

    let sample = r#"{"timestamp":"2025-09-30T15:42:34.559Z","type":"session_meta","payload":{"id":"test-id","cwd":"/test"}}
{"timestamp":"2025-09-30T15:42:36.190Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"first"}]}}
{"timestamp":"2025-09-30T15:42:37.000Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"text","text":"second"}]}}
{"timestamp":"2025-09-30T15:42:38.000Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"third"}]}}
"#;
    fs::write(&file, sample).unwrap();

    let connector = CodexConnector::new();
    // qu81y: the tempdir codex home is passed as an EXPLICIT scan root
    // instead of mutating process-global CODEX_HOME.
    let ctx = ScanContext::with_roots(
        dir.path().to_path_buf(),
        vec![ScanRoot::local(dir.path().to_path_buf())],
        None,
    );
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);

    let c = &convs[0];
    assert_eq!(c.messages.len(), 3);
    assert_eq!(c.messages[0].idx, 0);
    assert_eq!(c.messages[1].idx, 1);
    assert_eq!(c.messages[2].idx, 2);
}

/// Test `external_id` comes from filename
#[test]
fn codex_connector_sets_external_id_from_filename() {
    let dir = TempDir::new().unwrap();
    let sessions = dir.path().join("sessions/2025/12/02");
    fs::create_dir_all(&sessions).unwrap();
    let file = sessions.join("rollout-unique-id-123.jsonl");

    let sample = r#"{"timestamp":"2025-09-30T15:42:34.559Z","type":"session_meta","payload":{"id":"test-id","cwd":"/test"}}
{"timestamp":"2025-09-30T15:42:36.190Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"test"}]}}
"#;
    fs::write(&file, sample).unwrap();

    let connector = CodexConnector::new();
    // qu81y: the tempdir codex home is passed as an EXPLICIT scan root
    // instead of mutating process-global CODEX_HOME.
    let ctx = ScanContext::with_roots(
        dir.path().to_path_buf(),
        vec![ScanRoot::local(dir.path().to_path_buf())],
        None,
    );
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);

    let c = &convs[0];
    // external_id is now the relative path from sessions dir for uniqueness across directories
    assert_eq!(
        c.external_id,
        Some("2025/12/02/rollout-unique-id-123".to_string())
    );
}

/// Test empty sessions directory returns no conversations
#[test]
fn codex_connector_handles_empty_sessions() {
    let dir = TempDir::new().unwrap();
    let sessions = dir.path().join("sessions");
    fs::create_dir_all(&sessions).unwrap();
    // No files in sessions

    let connector = CodexConnector::new();
    // qu81y: the tempdir codex home is passed as an EXPLICIT scan root
    // instead of mutating process-global CODEX_HOME.
    let ctx = ScanContext::with_roots(
        dir.path().to_path_buf(),
        vec![ScanRoot::local(dir.path().to_path_buf())],
        None,
    );
    let convs = connector.scan(&ctx).unwrap();
    assert!(convs.is_empty());
}

/// Test integer (milliseconds) timestamp format
#[test]
fn codex_connector_parses_millis_timestamp() {
    let dir = TempDir::new().unwrap();
    let sessions = dir.path().join("sessions/2025/12/03");
    fs::create_dir_all(&sessions).unwrap();
    let file = sessions.join("rollout-millis.jsonl");

    // Timestamps as i64 milliseconds instead of ISO-8601 strings
    let sample = r#"{"timestamp":1700000000000,"type":"session_meta","payload":{"id":"millis-test","cwd":"/millis"}}
{"timestamp":1700000001000,"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"millis timestamp test"}]}}
{"timestamp":1700000002000,"type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"text","text":"response with millis"}]}}
"#;
    fs::write(&file, sample).unwrap();

    let connector = CodexConnector::new();
    // qu81y: the tempdir codex home is passed as an EXPLICIT scan root
    // instead of mutating process-global CODEX_HOME.
    let ctx = ScanContext::with_roots(
        dir.path().to_path_buf(),
        vec![ScanRoot::local(dir.path().to_path_buf())],
        None,
    );
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);

    let c = &convs[0];
    assert_eq!(c.messages.len(), 2);
    // Verify timestamps were parsed from i64 millis
    // started_at comes from session_meta timestamp (1700000000000)
    assert_eq!(c.started_at, Some(1700000000000));
    // ended_at comes from the last message timestamp (1700000002000)
    assert_eq!(c.ended_at, Some(1700000002000));
}

/// Test `tool_use` blocks in content are flattened properly
#[test]
fn codex_connector_flattens_tool_use_blocks() {
    let dir = TempDir::new().unwrap();
    let sessions = dir.path().join("sessions/2025/12/04");
    fs::create_dir_all(&sessions).unwrap();
    let file = sessions.join("rollout-tools.jsonl");

    let sample = r#"{"timestamp":"2025-09-30T15:42:34.559Z","type":"session_meta","payload":{"id":"tool-test","cwd":"/tools"}}
{"timestamp":"2025-09-30T15:42:36.190Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"read a file"}]}}
{"timestamp":"2025-09-30T15:42:43.000Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"text","text":"Let me read that file"},{"type":"tool_use","name":"Read","input":{"file_path":"/test/file.rs"}}]}}
"#;
    fs::write(&file, sample).unwrap();

    let connector = CodexConnector::new();
    // qu81y: the tempdir codex home is passed as an EXPLICIT scan root
    // instead of mutating process-global CODEX_HOME.
    let ctx = ScanContext::with_roots(
        dir.path().to_path_buf(),
        vec![ScanRoot::local(dir.path().to_path_buf())],
        None,
    );
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);

    let c = &convs[0];
    assert_eq!(c.messages.len(), 2);

    // Assistant message should contain flattened tool_use
    let assistant = &c.messages[1];
    assert!(assistant.content.contains("Let me read that file"));
    assert!(assistant.content.contains("[Tool: Read"));
    assert!(assistant.content.contains("/test/file.rs"));
}

/// Test missing cwd in `session_meta` results in None workspace
#[test]
fn codex_connector_handles_missing_cwd() {
    let dir = TempDir::new().unwrap();
    let sessions = dir.path().join("sessions/2025/12/05");
    fs::create_dir_all(&sessions).unwrap();
    let file = sessions.join("rollout-no-cwd.jsonl");

    // session_meta without cwd field
    let sample = r#"{"timestamp":"2025-09-30T15:42:34.559Z","type":"session_meta","payload":{"id":"no-cwd","cli_version":"0.42.0"}}
{"timestamp":"2025-09-30T15:42:36.190Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"test without cwd"}]}}
"#;
    fs::write(&file, sample).unwrap();

    let connector = CodexConnector::new();
    // qu81y: the tempdir codex home is passed as an EXPLICIT scan root
    // instead of mutating process-global CODEX_HOME.
    let ctx = ScanContext::with_roots(
        dir.path().to_path_buf(),
        vec![ScanRoot::local(dir.path().to_path_buf())],
        None,
    );
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);

    let c = &convs[0];
    assert!(
        c.workspace.is_none(),
        "workspace should be None when cwd missing"
    );
}

/// Test files without rollout- prefix are ignored
#[test]
fn codex_connector_ignores_non_rollout_files() {
    let dir = TempDir::new().unwrap();
    let sessions = dir.path().join("sessions/2025/12/06");
    fs::create_dir_all(&sessions).unwrap();

    // Valid rollout file
    let rollout = sessions.join("rollout-valid.jsonl");
    let sample = r#"{"timestamp":"2025-09-30T15:42:34.559Z","type":"session_meta","payload":{"id":"valid","cwd":"/test"}}
{"timestamp":"2025-09-30T15:42:36.190Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"valid"}]}}
"#;
    fs::write(&rollout, sample).unwrap();

    // Non-rollout files that should be ignored
    let other1 = sessions.join("session-123.jsonl");
    let other2 = sessions.join("backup.json");
    let other3 = sessions.join("config.jsonl");
    fs::write(&other1, sample).unwrap();
    fs::write(&other2, sample).unwrap();
    fs::write(&other3, sample).unwrap();

    let connector = CodexConnector::new();
    // qu81y: the tempdir codex home is passed as an EXPLICIT scan root
    // instead of mutating process-global CODEX_HOME.
    let ctx = ScanContext::with_roots(
        dir.path().to_path_buf(),
        vec![ScanRoot::local(dir.path().to_path_buf())],
        None,
    );
    let convs = connector.scan(&ctx).unwrap();
    // Only the rollout- prefixed file should be processed
    assert_eq!(convs.len(), 1);
    // external_id is now the relative path from sessions dir for uniqueness across directories
    assert_eq!(
        convs[0].external_id,
        Some("2025/12/06/rollout-valid".to_string())
    );
}

/// Test legacy JSON with missing optional fields
#[test]
fn codex_connector_handles_legacy_json_missing_session() {
    let dir = TempDir::new().unwrap();
    let sessions = dir.path().join("sessions/2025/12/07");
    fs::create_dir_all(&sessions).unwrap();
    let file = sessions.join("rollout-minimal.json");

    // Minimal legacy format without session object
    let sample = r#"{
        "items": [
            {
                "role": "user",
                "content": "minimal legacy message"
            }
        ]
    }"#;
    fs::write(&file, sample).unwrap();

    let connector = CodexConnector::new();
    // qu81y: the tempdir codex home is passed as an EXPLICIT scan root
    // instead of mutating process-global CODEX_HOME.
    let ctx = ScanContext::with_roots(
        dir.path().to_path_buf(),
        vec![ScanRoot::local(dir.path().to_path_buf())],
        None,
    );
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);

    let c = &convs[0];
    assert!(c.workspace.is_none());
    assert_eq!(c.messages.len(), 1);
    assert!(c.messages[0].content.contains("minimal legacy message"));
}

/// Test title fallback to first message when no user message exists
#[test]
fn codex_connector_title_fallback_to_first_message() {
    let dir = TempDir::new().unwrap();
    let sessions = dir.path().join("sessions/2025/12/08");
    fs::create_dir_all(&sessions).unwrap();
    let file = sessions.join("rollout-no-user.jsonl");

    // Only assistant messages, no user message
    let sample = r#"{"timestamp":"2025-09-30T15:42:34.559Z","type":"session_meta","payload":{"id":"no-user","cwd":"/test"}}
{"timestamp":"2025-09-30T15:42:36.190Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"text","text":"Assistant first line\nSecond line"}]}}
"#;
    fs::write(&file, sample).unwrap();

    let connector = CodexConnector::new();
    // qu81y: the tempdir codex home is passed as an EXPLICIT scan root
    // instead of mutating process-global CODEX_HOME.
    let ctx = ScanContext::with_roots(
        dir.path().to_path_buf(),
        vec![ScanRoot::local(dir.path().to_path_buf())],
        None,
    );
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);

    let c = &convs[0];
    // Title should fallback to first line of first message
    assert_eq!(c.title, Some("Assistant first line".to_string()));
}

/// Test deeply nested directory structure
#[test]
fn codex_connector_handles_nested_directories() {
    let dir = TempDir::new().unwrap();
    let deep_sessions = dir.path().join("sessions/2025/12/09/sub1/sub2");
    fs::create_dir_all(&deep_sessions).unwrap();
    let file = deep_sessions.join("rollout-nested.jsonl");

    let sample = r#"{"timestamp":"2025-09-30T15:42:34.559Z","type":"session_meta","payload":{"id":"nested","cwd":"/nested"}}
{"timestamp":"2025-09-30T15:42:36.190Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"deeply nested"}]}}
"#;
    fs::write(&file, sample).unwrap();

    let connector = CodexConnector::new();
    // qu81y: the tempdir codex home is passed as an EXPLICIT scan root
    // instead of mutating process-global CODEX_HOME.
    let ctx = ScanContext::with_roots(
        dir.path().to_path_buf(),
        vec![ScanRoot::local(dir.path().to_path_buf())],
        None,
    );
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);
    assert!(convs[0].source_path.to_string_lossy().contains("sub2"));
}

/// Test `turn_aborted` event is filtered out
#[test]
fn codex_connector_filters_turn_aborted() {
    let dir = TempDir::new().unwrap();
    let sessions = dir.path().join("sessions/2025/12/10");
    fs::create_dir_all(&sessions).unwrap();
    let file = sessions.join("rollout-aborted.jsonl");

    let sample = r#"{"timestamp":"2025-09-30T15:42:34.559Z","type":"session_meta","payload":{"id":"test-id","cwd":"/test"}}
{"timestamp":"2025-09-30T15:42:36.190Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"test"}]}}
{"timestamp":"2025-09-30T15:42:37.000Z","type":"event_msg","payload":{"type":"turn_aborted","reason":"user cancelled"}}
{"timestamp":"2025-09-30T15:42:38.000Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"text","text":"response"}]}}
"#;
    fs::write(&file, sample).unwrap();

    let connector = CodexConnector::new();
    // qu81y: the tempdir codex home is passed as an EXPLICIT scan root
    // instead of mutating process-global CODEX_HOME.
    let ctx = ScanContext::with_roots(
        dir.path().to_path_buf(),
        vec![ScanRoot::local(dir.path().to_path_buf())],
        None,
    );
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);

    let c = &convs[0];
    // Should only have 2 messages (user, assistant) - turn_aborted filtered
    assert_eq!(c.messages.len(), 2);
    for msg in &c.messages {
        assert!(!msg.content.contains("turn_aborted"));
    }
}

/// Test long title is truncated to 100 chars
#[test]
fn codex_connector_truncates_long_title() {
    let dir = TempDir::new().unwrap();
    let sessions = dir.path().join("sessions/2025/12/11");
    fs::create_dir_all(&sessions).unwrap();
    let file = sessions.join("rollout-long-title.jsonl");

    let long_text = "A".repeat(200);
    let sample = format!(
        r#"{{"timestamp":"2025-09-30T15:42:34.559Z","type":"session_meta","payload":{{"id":"long","cwd":"/test"}}}}
{{"timestamp":"2025-09-30T15:42:36.190Z","type":"response_item","payload":{{"type":"message","role":"user","content":[{{"type":"input_text","text":"{long_text}"}}]}}}}
"#
    );
    fs::write(&file, sample).unwrap();

    let connector = CodexConnector::new();
    // qu81y: the tempdir codex home is passed as an EXPLICIT scan root
    // instead of mutating process-global CODEX_HOME.
    let ctx = ScanContext::with_roots(
        dir.path().to_path_buf(),
        vec![ScanRoot::local(dir.path().to_path_buf())],
        None,
    );
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);

    let c = &convs[0];
    // Bead 7k7pl: collapse `.is_some()` + `.unwrap().len() == 100`
    // into a single pin on the title's length (truncation contract)
    // plus a content pin — the truncated title must still be 100 x
    // 'A' (no bytes lost to multi-byte boundary confusion).
    assert_eq!(
        c.title.as_ref().map(|t| t.len()),
        Some(100),
        "title must be truncated to exactly 100 chars; got {:?}",
        c.title
    );
    assert_eq!(
        c.title.as_deref(),
        Some("A".repeat(100).as_str()),
        "title must be 100 'A's from the truncation of 200; got {:?}",
        c.title
    );
}

/// Test `source_path` matches actual file path
#[test]
fn codex_connector_sets_source_path() {
    let dir = TempDir::new().unwrap();
    let sessions = dir.path().join("sessions/2025/12/12");
    fs::create_dir_all(&sessions).unwrap();
    let file = sessions.join("rollout-source-path.jsonl");

    let sample = r#"{"timestamp":"2025-09-30T15:42:34.559Z","type":"session_meta","payload":{"id":"path-test","cwd":"/test"}}
{"timestamp":"2025-09-30T15:42:36.190Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"test source path"}]}}
"#;
    fs::write(&file, sample).unwrap();

    let connector = CodexConnector::new();
    // qu81y: the tempdir codex home is passed as an EXPLICIT scan root
    // instead of mutating process-global CODEX_HOME.
    let ctx = ScanContext::with_roots(
        dir.path().to_path_buf(),
        vec![ScanRoot::local(dir.path().to_path_buf())],
        None,
    );
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);

    let c = &convs[0];
    assert_eq!(c.source_path, file);
}

/// Test metadata indicates correct source format
#[test]
fn codex_connector_metadata_indicates_format() {
    let dir = TempDir::new().unwrap();
    let sessions = dir.path().join("sessions/2025/12/13");
    fs::create_dir_all(&sessions).unwrap();

    // Create both JSONL and JSON files
    let jsonl_file = sessions.join("rollout-jsonl.jsonl");
    let json_file = sessions.join("rollout-json.json");

    let jsonl_sample = r#"{"timestamp":"2025-09-30T15:42:34.559Z","type":"session_meta","payload":{"id":"jsonl","cwd":"/test"}}
{"timestamp":"2025-09-30T15:42:36.190Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"jsonl format"}]}}
"#;
    fs::write(&jsonl_file, jsonl_sample).unwrap();

    let json_sample = r#"{"session":{"id":"json","cwd":"/test"},"items":[{"role":"user","content":"json format"}]}"#;
    fs::write(&json_file, json_sample).unwrap();

    let connector = CodexConnector::new();
    // qu81y: the tempdir codex home is passed as an EXPLICIT scan root
    // instead of mutating process-global CODEX_HOME.
    let ctx = ScanContext::with_roots(
        dir.path().to_path_buf(),
        vec![ScanRoot::local(dir.path().to_path_buf())],
        None,
    );
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 2);

    // Find each conversation and verify metadata
    let jsonl_conv = convs.iter().find(|c| c.source_path == jsonl_file).unwrap();
    let json_conv = convs.iter().find(|c| c.source_path == json_file).unwrap();

    assert_eq!(
        jsonl_conv.metadata.get("source").and_then(|v| v.as_str()),
        Some("rollout")
    );
    assert_eq!(
        json_conv.metadata.get("source").and_then(|v| v.as_str()),
        Some("rollout_json")
    );
}

// review 2026-11-02 F6 through the CASS wrapper (incl. its enrichment pass).
#[test]
fn codex_sums_distinct_request_token_counts_through_cass_wrapper() {
    let dir = TempDir::new().unwrap();
    let sessions = dir.path().join("sessions/2025/12/01");
    fs::create_dir_all(&sessions).unwrap();
    let sample = r#"{"timestamp":"2025-12-01T10:00:00.000Z","type":"session_meta","payload":{"id":"p2c","cwd":"/test/workspace","cli_version":"0.42.0"}}
{"timestamp":"2025-12-01T10:00:00.500Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"question"}]}}
{"timestamp":"2025-12-01T10:00:01.000Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"text","text":"answer"}]}}
{"timestamp":"2025-12-01T10:00:02.000Z","type":"event_msg","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":5,"output_tokens":10},"total_token_usage":{"total_tokens":15}}}}
{"timestamp":"2025-12-01T10:00:03.000Z","type":"event_msg","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":5,"output_tokens":10},"total_token_usage":{"total_tokens":15}}}}
{"timestamp":"2025-12-01T10:00:04.000Z","type":"event_msg","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":7,"output_tokens":14},"total_token_usage":{"total_tokens":36}}}}
"#;
    fs::write(sessions.join("rollout-p2c.jsonl"), sample).unwrap();
    let ctx = ScanContext::with_roots(
        dir.path().to_path_buf(),
        vec![ScanRoot::local(dir.path().to_path_buf())],
        None,
    );
    let convs = CodexConnector::new().scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);
    let assistant = convs[0]
        .messages
        .iter()
        .find(|m| m.role == "assistant" && m.content.contains("answer"))
        .expect("assistant message");
    let usage = assistant
        .extra
        .pointer("/cass/token_usage")
        .expect("token usage");
    assert_eq!(usage["input_tokens"].as_i64(), Some(12));
    assert_eq!(usage["output_tokens"].as_i64(), Some(24));
}

// --- Codex paginated threads (FAD 18e2ffaf merge) through the real cass path ---
// Synthetic fixtures only: invented UUIDs/prompts, no personal data. One Codex
// thread is an original rollout plus a continuation; the bumped FAD merges them
// into one conversation. These pin that cass's admission, enrichment and
// recovery layers deliver the merged thread on full AND incremental scans.

const PAG_THREAD_A: &str = "01a1d1b4-5529-7cd3-97db-d52326514bc2";
const PAG_SEGMENT_B: &str = "01a1d1ba-a4da-7b00-92b0-0368cfada0b2";

/// Write one paginated thread (original + one continuation). The continuation
/// optionally carries a token_count event so enrichment coverage is observable.
fn paginated_thread_fixture(
    continuation_token_count: bool,
) -> (TempDir, PathBuf, PathBuf, ScanContext) {
    let dir = TempDir::new().unwrap();
    let sessions = dir.path().join("sessions/2026/11/12");
    fs::create_dir_all(&sessions).unwrap();
    let cwd = dir.path().join("workspace");
    fs::create_dir_all(&cwd).unwrap();
    let cwd_str = cwd.to_str().unwrap();
    let a = PAG_THREAD_A;
    let b = PAG_SEGMENT_B;
    let original = format!(
        r#"{{"type":"session_meta","timestamp":"2026-11-12T21:40:18Z","payload":{{"id":"{a}","session_id":"{a}","cwd":"{cwd_str}","history_mode":"paginated","originator":"codex-tui","source":"cli"}}}}
{{"type":"response_item","timestamp":"2026-11-12T21:40:20Z","payload":{{"type":"message","role":"user","content":"first question"}}}}
{{"type":"response_item","timestamp":"2026-11-12T21:41:00Z","payload":{{"type":"message","role":"assistant","content":"first answer"}}}}
"#
    );
    let mut continuation = format!(
        r#"{{"type":"session_meta","timestamp":"2026-11-12T21:47:12Z","payload":{{"id":"{a}","session_id":"{a}","cwd":"{cwd_str}","history_mode":"paginated","originator":"codex-tui","source":"cli"}}}}
{{"type":"response_item","timestamp":"2026-11-12T21:47:20Z","payload":{{"type":"message","role":"user","content":"second question"}}}}
{{"type":"response_item","timestamp":"2026-11-12T21:48:00Z","payload":{{"type":"message","role":"assistant","content":"second answer"}}}}
"#
    );
    if continuation_token_count {
        continuation.push_str(
            r#"{"type":"event_msg","timestamp":"2026-11-12T21:48:05Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":7,"output_tokens":14},"total_token_usage":{"total_tokens":21}}}}
"#,
        );
    }
    let original_path = sessions.join(format!("rollout-2026-11-12T22-40-18-{a}.jsonl"));
    let continuation_path = sessions.join(format!("rollout-2026-11-12T22-47-12-{a}_{b}.jsonl"));
    fs::write(&original_path, original).unwrap();
    fs::write(&continuation_path, continuation).unwrap();
    let ctx = ScanContext::with_roots(
        dir.path().to_path_buf(),
        vec![ScanRoot::local(dir.path().to_path_buf())],
        None,
    );
    (dir, original_path, continuation_path, ctx)
}

#[test]
fn codex_paginated_thread_merges_through_cass_scan() {
    let (_dir, _original, _continuation, ctx) = paginated_thread_fixture(false);
    let convs = CodexConnector::new()
        .scan(&ctx)
        .expect("merged-thread full scan must succeed");
    assert_eq!(
        convs.len(),
        1,
        "one conversation per thread, got {}",
        convs.len()
    );
    let texts: Vec<&str> = convs[0]
        .messages
        .iter()
        .map(|m| m.content.as_str())
        .collect();
    assert!(
        texts.iter().any(|t| t.contains("first question")),
        "{texts:?}"
    );
    assert!(
        texts.iter().any(|t| t.contains("second question")),
        "{texts:?}"
    );
    assert_eq!(
        convs[0].metadata["segments"].as_array().map(|a| a.len()),
        Some(2),
        "merged metadata must list both segments: {:?}",
        convs[0].metadata
    );
}

/// §7.1: an incremental scan that reuses the original (durable ledger) and
/// admits a new continuation must still index the merged conversation. The
/// host predicate models the ledger by refusing the original's path.
#[test]
fn codex_paginated_incremental_reused_original_admits_continuation() {
    let (_dir, original, continuation, ctx) = paginated_thread_fixture(false);
    let mut admitted = Vec::new();
    let mut predicate = |source: &franken_agent_detection::DiscoveredSourceFile| {
        let reuse = source.source_path == original;
        if !reuse {
            admitted.push(source.source_path.clone());
        }
        !reuse
    };
    let mut completed = Vec::new();
    let mut complete = |done: &franken_agent_detection::SourceCompletion| {
        completed.push(done.source.source_path.clone());
        Ok(())
    };
    let mut hooks = franken_agent_detection::SourceScanHooks {
        should_scan_source: Some(&mut predicate),
        on_source_complete: Some(&mut complete),
    };
    let mut delivered = Vec::new();
    CodexConnector::new()
        .scan_with_source_boundaries(&ctx, &mut hooks, &mut |conversation| {
            delivered.push(conversation);
            Ok(())
        })
        .expect("incremental merged-thread scan must succeed when the original is ledger-reused");
    assert_eq!(
        delivered.len(),
        1,
        "the merged thread is indexed exactly once"
    );
    let texts: Vec<&str> = delivered[0]
        .messages
        .iter()
        .map(|m| m.content.as_str())
        .collect();
    assert!(
        texts.iter().any(|t| t.contains("first question")),
        "{texts:?}"
    );
    assert!(
        texts.iter().any(|t| t.contains("second question")),
        "{texts:?}"
    );
    assert_eq!(
        delivered[0].source_path, original,
        "identity stays the original segment"
    );
    assert_eq!(
        admitted,
        [continuation],
        "only the new continuation is admitted"
    );
    assert_eq!(
        completed,
        [original],
        "completion certifies the thread identity once"
    );
}

/// §7.2: enrichment must read every segment's bytes. The token_count event
/// lives only in the continuation, so the merged conversation's usage proves
/// the continuation was enriched, not just the identity snapshot.
#[test]
fn codex_paginated_enrichment_covers_continuation_bytes() {
    let (_dir, _original, _continuation, ctx) = paginated_thread_fixture(true);
    let convs = CodexConnector::new()
        .scan(&ctx)
        .expect("merged-thread scan with continuation token_count must succeed");
    assert_eq!(convs.len(), 1);
    let assistant = convs[0]
        .messages
        .iter()
        .find(|m| m.role == "assistant" && m.content.contains("second answer"))
        .expect("continuation assistant message");
    let usage = assistant
        .extra
        .pointer("/cass/token_usage")
        .expect("continuation token usage must be enriched onto the merged conversation");
    assert_eq!(usage["input_tokens"].as_i64(), Some(7));
    assert_eq!(usage["output_tokens"].as_i64(), Some(14));
}
