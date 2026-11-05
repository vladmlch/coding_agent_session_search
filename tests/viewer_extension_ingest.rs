//! Task 7: provider-specific session facts extracted inside the CASS ingest
//! pass (spec decisions 4/5). Behavioral oracle: the Viewer's
//! `viewer/backends/cass/facts/{claude,codex}.py` — ported semantics, not code.
//!
//! These tests pin: exact tool call IDs, tool outcomes, source anchors,
//! thinking counts, missing-vs-null (unrecorded stays NULL/unknown),
//! conflicting IDs never merging, parent/category/first_prompt, and
//! deterministic idempotent publication keyed on
//! (extractor_version, source_revision).

use anyhow::{Result, ensure};
use coding_agent_search::connectors::{
    NormalizedConversation, NormalizedMessage, NormalizedSnippet,
};
use coding_agent_search::franken_sync::compat::RowExt;
use coding_agent_search::model::conversation_packet::{
    ConversationPacket, ConversationPacketProvenance,
};
use coding_agent_search::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use coding_agent_search::storage::sqlite::FrankenStorage;
use coding_agent_search::viewer_extensions::facts::{
    FactPublishOutcome, extract_session_facts, publish_session_facts,
};
use coding_agent_search::viewer_extensions::schema;
use serde_json::{Value, json};
use std::collections::BTreeSet;

fn msg(idx: i64, role: &str, content: &str, extra: Value) -> NormalizedMessage {
    NormalizedMessage {
        idx,
        role: role.to_string(),
        author: None,
        created_at: Some(1_700_000_000_000 + idx),
        content: content.to_string(),
        extra,
        invocations: Vec::new(),
        snippets: Vec::<NormalizedSnippet>::new(),
    }
}

fn packet(
    agent_slug: &str,
    metadata: Value,
    messages: Vec<NormalizedMessage>,
) -> ConversationPacket {
    let conv = NormalizedConversation {
        agent_slug: agent_slug.to_string(),
        external_id: Some("fixture-ext".to_string()),
        title: Some("fixture".to_string()),
        workspace: None,
        source_path: "/tmp/fixture.jsonl".into(),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_100),
        metadata,
        messages,
    };
    ConversationPacket::from_normalized_conversation(&conv, ConversationPacketProvenance::local())
}

fn coverage<'a>(
    publication: &'a coding_agent_search::viewer_extensions::facts::FactPublication,
    field: &str,
) -> Option<&'a coding_agent_search::viewer_extensions::facts::CoverageEntry> {
    publication
        .coverage
        .iter()
        .find(|entry| entry.field == field)
}

// ---------------------------------------------------------------------------
// Claude fixture: tool_use/tool_result pairing, streaming dedup, thinking,
// conflicting uuid events, conflicting call IDs, orphan result.
// ---------------------------------------------------------------------------

fn claude_packet() -> ConversationPacket {
    let extra_user = |uuid: &str| {
        json!({
            "type": "user", "uuid": uuid, "sessionId": "sess-1",
            "message": {"role": "user", "content": [{"type": "text", "text": "please fix the flaky test"}]}
        })
    };
    // One logical assistant reply split across two physical records (same
    // requestId + message.id): identical blocks must dedup, thinking counted once.
    let extra_asst = |uuid: &str, blocks: Value| {
        json!({
            "type": "assistant", "uuid": uuid, "sessionId": "sess-1", "requestId": "r1",
            "message": {"id": "m1", "role": "assistant", "model": "claude-opus-5",
                        "content": blocks, "usage": {"input_tokens": 10, "output_tokens": 7}}
        })
    };
    let tool_result = |call: &str, is_error: bool, text: &str| json!({"source": "tool_result", "tool_use_id": call, "is_error": is_error, "_text": text});
    packet(
        "claude_code",
        json!({"source": "claude_code", "sidechain": true, "parentSessionId": "parent-sess"}),
        vec![
            msg(0, "user", "please fix the flaky test", extra_user("u1")),
            // Duplicate uuid with IDENTICAL semantic content: dedups to one event.
            msg(1, "user", "please fix the flaky test", extra_user("u1")),
            // Conflicting uuid pair: same uuid, different record -> unknown, no merge.
            msg(2, "assistant", "conflict A", {
                let mut e = extra_asst("uc", json!([{"type": "text", "text": "conflict A"}]));
                e["requestId"] = json!("rc1");
                e
            }),
            msg(10, "assistant", "conflict B", {
                let mut e = extra_asst("uc", json!([{"type": "text", "text": "conflict B"}]));
                e["requestId"] = json!("rc2");
                e
            }),
            msg(
                3,
                "assistant",
                "thinking text",
                extra_asst(
                    "a1",
                    json!([
                        {"type": "thinking", "thinking": "plan the fix"},
                        {"type": "tool_use", "id": "toolu_1", "name": "Read",
                         "input": {"file_path": "/src/a.rs"}}
                    ]),
                ),
            ),
            // Same logical reply (r1/m1), repeated text block: dedup'd.
            msg(
                4,
                "assistant",
                "thinking text",
                extra_asst(
                    "a2",
                    json!([
                        {"type": "thinking", "thinking": "plan the fix"},
                        {"type": "tool_use", "id": "toolu_1", "name": "Read",
                         "input": {"file_path": "/src/a.rs"}},
                        {"type": "tool_use", "id": "toolu_2", "name": "Bash",
                         "input": {"command": "cargo test"}}
                    ]),
                ),
            ),
            msg(
                5,
                "tool",
                "file body",
                tool_result("toolu_1", false, "file body"),
            ),
            msg(
                6,
                "tool",
                "1 failed",
                tool_result("toolu_2", true, "1 failed"),
            ),
            // Orphan result: no recorded call for toolu_orphan.
            msg(
                7,
                "tool",
                "lost",
                tool_result("toolu_orphan", false, "lost"),
            ),
            // Conflicting call IDs: same id, different inputs -> ambiguous, not merged.
            msg(
                8,
                "assistant",
                "x",
                extra_asst(
                    "a3",
                    json!([{"type": "tool_use", "id": "toolu_dup", "name": "Read",
                            "input": {"file_path": "/one.rs"}}]),
                ),
            ),
            msg(
                9,
                "assistant",
                "y",
                extra_asst(
                    "a4",
                    json!([{"type": "tool_use", "id": "toolu_dup", "name": "Read",
                            "input": {"file_path": "/two.rs"}}]),
                ),
            ),
        ],
    )
}

#[test]
fn claude_facts_pair_tools_dedup_and_coverage() {
    let publication = extract_session_facts(&claude_packet(), "claude_code");

    // Exact call IDs: toolu_1/toolu_2 paired, toolu_orphan unpaired,
    // toolu_dup ambiguous and NOT merged (absent from tools).
    let ids: Vec<&str> = publication
        .tools
        .iter()
        .map(|t| t.call_id.as_str())
        .collect();
    assert!(ids.contains(&"toolu_1"), "tools: {ids:?}");
    assert!(ids.contains(&"toolu_2"), "tools: {ids:?}");
    assert!(
        !ids.contains(&"toolu_dup"),
        "conflicting call ids must not merge: {ids:?}"
    );

    let read = publication
        .tools
        .iter()
        .find(|t| t.call_id == "toolu_1")
        .expect("toolu_1");
    assert_eq!(read.name, "Read");
    assert_eq!(read.is_error, Some(false));
    assert_eq!(read.file_path.as_deref(), Some("/src/a.rs"));
    assert!(read.has_result, "toolu_1 must pair its tool_result");
    // Source anchor: the call came from packet message idx 3 (or 4 after dedup).
    assert!(read.source_idx == 3 || read.source_idx == 4);

    let bash = publication
        .tools
        .iter()
        .find(|t| t.call_id == "toolu_2")
        .expect("toolu_2");
    assert_eq!(bash.is_error, Some(true));

    // Streaming dedup: one thinking block even though the reply is split.
    assert_eq!(publication.thinking_block_count, Some(1));
    assert_eq!(publication.tool_error_count, Some(1));
    // No exit codes recorded for Claude tool blocks.
    assert_eq!(publication.tool_calls_with_exit_status, Some(0));
    assert_eq!(publication.file_fact_count, Some(1));

    // Parent/subagent evidence from connector-verified metadata_json.
    assert_eq!(publication.is_subagent, Some(true));
    assert_eq!(
        publication.parent_external_id.as_deref(),
        Some("parent-sess")
    );

    // first_prompt / turn / category. Two identical u1 records dedup to one
    // user turn; the conflicting u1 record is excluded as unknown evidence.
    assert_eq!(
        publication.first_prompt.as_deref(),
        Some("please fix the flaky test")
    );
    assert_eq!(publication.turn_count, Some(1));
    assert_eq!(publication.task_category.as_deref(), Some("testing"));

    assert_eq!(publication.model_set, vec!["claude-opus-5".to_string()]);

    // Coverage: conflicting events + orphan result must surface reasons.
    let tools = coverage(&publication, "tools").expect("tools coverage");
    assert_ne!(tools.state, "complete");
    let reasons = tools.reasons.join(",");
    assert!(reasons.contains("orphan_tool_result"), "tools: {reasons}");
    assert!(reasons.contains("conflicting_event"), "tools: {reasons}");
    let exit = coverage(&publication, "tool_exit").expect("tool_exit");
    assert_eq!(exit.state, "unavailable", "exit: {exit:?}");
}

// ---------------------------------------------------------------------------
// Codex fixture: function_call/output pairing, compaction-delimited call-ID
// scoping, response-id dedup, reasoning->thinking, control records.
// ---------------------------------------------------------------------------

fn codex_packet() -> ConversationPacket {
    let response_item = |payload: Value| json!({"type": "response_item", "payload": payload});
    let event_msg = |payload: Value| json!({"type": "event_msg", "payload": payload});
    packet(
        "codex",
        json!({"source": "rollout"}),
        vec![
            // Control records never become facts but must not be unknown.
            msg(
                0,
                "user",
                "x",
                json!({"type": "session_meta", "payload": {"id": "sess-c"}}),
            ),
            msg(
                1,
                "user",
                "list the files",
                event_msg(json!({
                    "type": "user_message", "message": "list the files"
                })),
            ),
            msg(
                2,
                "assistant",
                "[Tool: exec_command]",
                response_item(json!({
                    "type": "function_call", "call_id": "call_1", "name": "exec_command",
                    "arguments": "{\"cmd\": \"ls\"}", "id": "resp-1"
                })),
            ),
            msg(
                3,
                "tool",
                "file1\nfile2",
                response_item(json!({
                    "type": "function_call_output", "call_id": "call_1",
                    "output": "file1\nfile2", "id": "resp-2"
                })),
            ),
            msg(
                4,
                "assistant",
                "reasoning text",
                response_item(json!({
                    "type": "reasoning",
                    "summary": [{"type": "summary_text", "text": "reasoning text"}],
                    "id": "resp-3"
                })),
            ),
            // Compaction boundary: call IDs after it are segment-scoped.
            msg(
                5,
                "system",
                "",
                json!({"type": "compacted", "payload": {"type": "context_compacted"}}),
            ),
            // Same raw call_id AFTER the boundary: must NOT merge with seg-0 call.
            msg(
                6,
                "assistant",
                "[Tool: exec_command]",
                response_item(json!({
                    "type": "function_call", "call_id": "call_1", "name": "exec_command",
                    "arguments": "{\"cmd\": \"pwd\"}", "id": "resp-4"
                })),
            ),
            msg(
                7,
                "tool",
                "/work",
                response_item(json!({
                    "type": "function_call_output", "call_id": "call_1",
                    "output": "/work", "id": "resp-5"
                })),
            ),
            // Response-id dedup: identical record replayed post-boundary.
            msg(
                8,
                "tool",
                "/work",
                response_item(json!({
                    "type": "function_call_output", "call_id": "call_1",
                    "output": "/work", "id": "resp-5"
                })),
            ),
            // agent_reasoning event_msg counts as thinking (matrix-mandated arm).
            msg(
                9,
                "assistant",
                "chain",
                event_msg(json!({"type": "agent_reasoning", "text": "chain"})),
            ),
            // Encrypted reasoning: recorded but unreadable -> unavailable.
            msg(
                10,
                "assistant",
                "",
                response_item(json!({
                    "type": "reasoning", "encrypted_content": "cafe", "id": "resp-6"
                })),
            ),
        ],
    )
}

#[test]
fn codex_facts_segment_scoped_call_ids_and_coverage() {
    let publication = extract_session_facts(&codex_packet(), "codex");

    // Pre-boundary call stays unqualified; post-boundary is segment-qualified
    // so the same raw call_id never merges across a compaction.
    let pre = publication
        .tools
        .iter()
        .find(|t| t.call_id == "call_1")
        .expect("pre-boundary call_1");
    assert_eq!(pre.raw_call_id, "call_1");
    assert_eq!(pre.name, "exec_command");
    assert!(pre.has_result);

    let post = publication
        .tools
        .iter()
        .find(|t| t.call_id != "call_1")
        .expect("post-boundary qualified call");
    assert!(
        post.call_id.starts_with("codex-segment:"),
        "post-boundary call must be qualified: {}",
        post.call_id
    );
    assert_eq!(post.raw_call_id, "call_1");
    assert!(post.has_result, "post-boundary result must pair");

    // Response-id replay deduped: exactly one result for the post call.
    assert_eq!(publication.tools.len(), 2, "tools: {:?}", publication.tools);

    assert_eq!(publication.compaction_count, Some(1));
    // reasoning summary + agent_reasoning event = 2 thinking blocks; the
    // encrypted reasoning is counted unavailable, not as a text block.
    assert_eq!(publication.thinking_block_count, Some(2));
    assert_eq!(publication.tool_error_count, Some(0));
    assert_eq!(publication.first_prompt.as_deref(), Some("list the files"));
    assert_eq!(publication.turn_count, Some(1));

    // is_subagent has no evidence for codex (session_meta carries no emitted
    // parent evidence in normalized input) -> stays unknown, never fabricated.
    assert_eq!(publication.is_subagent, None);

    let thinking = coverage(&publication, "thinking").expect("thinking coverage");
    assert_ne!(thinking.state, "complete");
    assert!(
        thinking.reasons.iter().any(|r| r.contains("reasoning")),
        "thinking reasons: {:?}",
        thinking.reasons
    );

    // Coverage manifest is never a blanket "complete" with open reasons.
    assert!(publication.coverage_state != "unknown");
}

// ---------------------------------------------------------------------------
// Antigravity fixture: `agy_type` envelopes, `cass.invocations` calls,
// TOOL_OUTPUT/VIEW_FILE/RUN_COMMAND results, explicit exit codes only where
// recorded, unlinked outputs, thinking hints, unknown-kind events.
// ---------------------------------------------------------------------------

fn antigravity_packet() -> ConversationPacket {
    packet(
        "antigravity",
        json!({"source": "antigravity", "model": "Gemini 3.1 Pro (High)"}),
        vec![
            // FAD USER_INPUT: the connector emits no agy_type for user turns;
            // model detection is lifted to conversation metadata.
            msg(
                0,
                "user",
                "read data.txt then run the tests",
                json!({"settings_change": "set model to Gemini 3.1 Pro (High)"}),
            ),
            // PLANNER_RESPONSE: thinking plus normalized `cass.invocations`
            // calls (the F3 envelope the pinned connector emits).
            msg(
                1,
                "assistant",
                "[Tool: view_file]\n[Tool: run_command]",
                json!({
                    "thinking": "plan the read then the test run",
                    "cass": {"invocations": [
                        {"kind": "tool", "name": "view_file", "call_id": "call_view_1",
                         "arguments": {"TargetFile": "/data.txt"}},
                        {"kind": "tool", "name": "run_command", "call_id": "call_run_1",
                         "arguments": {"CommandLine": "cargo test"}}
                    ]}
                }),
            ),
            // Paired output with an explicit recorded exit code.
            msg(
                2,
                "tool",
                "3 passed",
                json!({"agy_type": "TOOL_OUTPUT", "call_id": "call_run_1",
                       "output": "3 passed", "exit_code": 0, "is_error": false}),
            ),
            // Paired output without status evidence: unknown, not zero.
            msg(
                3,
                "tool",
                "file body",
                json!({"agy_type": "VIEW_FILE", "call_id": "call_view_1",
                       "content": "file body"}),
            ),
            // Output with no recorded identifier: never positionally linked.
            msg(
                4,
                "tool",
                "lost output",
                json!({"agy_type": "RUN_COMMAND", "content": "lost output"}),
            ),
            // Explicit error record kind.
            msg(
                5,
                "system",
                "the run blew up",
                json!({"agy_type": "ERROR_MESSAGE", "content": "the run blew up"}),
            ),
            // Unverifiable event (no agy/type/kind evidence): unknown arm.
            msg(6, "assistant", "trailing text", json!({})),
        ],
    )
}

#[test]
fn antigravity_facts_agy_tools_exit_codes_and_coverage() {
    let publication = extract_session_facts(&antigravity_packet(), "antigravity");

    // Exact recorded call IDs: both planner calls recorded, run_command
    // paired with its exit-coded output; view_file paired with its output.
    let run = publication
        .tools
        .iter()
        .find(|t| t.call_id == "call_run_1")
        .expect("call_run_1");
    assert_eq!(run.raw_call_id, "call_run_1");
    assert_eq!(run.name, "run_command");
    assert!(run.has_result);
    assert_eq!(run.exit_code, Some(0));
    assert_eq!(run.is_error, Some(false));

    let view = publication
        .tools
        .iter()
        .find(|t| t.call_id == "call_view_1")
        .expect("call_view_1");
    assert_eq!(view.name, "view_file");
    assert!(view.has_result);
    // No status evidence recorded: NULL, never a fabricated success/zero.
    assert_eq!(view.exit_code, None);
    assert_eq!(view.is_error, None);
    assert_eq!(view.file_path.as_deref(), Some("/data.txt"));

    assert_eq!(publication.tools.len(), 2, "tools: {:?}", publication.tools);
    assert_eq!(publication.thinking_block_count, Some(1));
    assert_eq!(publication.tool_error_count, Some(0));
    assert_eq!(publication.tool_calls_with_exit_status, Some(1));
    assert_eq!(publication.file_fact_count, Some(1));

    assert_eq!(publication.turn_count, Some(1));
    assert_eq!(
        publication.first_prompt.as_deref(),
        Some("read data.txt then run the tests")
    );
    assert_eq!(publication.task_category.as_deref(), Some("testing"));
    assert_eq!(publication.model_set, vec!["Gemini 3.1 Pro (High)"]);

    // No relation evidence for antigravity.
    assert_eq!(publication.is_subagent, None);
    assert_eq!(publication.parent_external_id, None);
    assert_eq!(publication.compaction_count, None);

    // Coverage: the unlinked output, the canonical agy envelope and the
    // unverifiable event must surface exact reasons.
    let tools = coverage(&publication, "tools").expect("tools coverage");
    assert_ne!(tools.state, "complete");
    let reasons = tools.reasons.join(",");
    assert!(
        reasons.contains("agy_tool_result_identity_missing"),
        "tools: {reasons}"
    );
    let exit = coverage(&publication, "tool_exit").expect("tool_exit");
    assert_ne!(exit.state, "complete", "exit: {exit:?}");
    let relation = coverage(&publication, "parent_relation").expect("parent_relation");
    assert_eq!(relation.state, "unavailable");
    assert!(
        publication
            .coverage
            .iter()
            .any(|entry| entry.reasons.iter().any(|r| r == "agy_record_unknown")),
        "coverage: {:?}",
        publication.coverage
    );
}

// ---------------------------------------------------------------------------
// Devin fixture: FAD sparse extras ({message_id, thinking, tool_call_id}) plus
// the oracle graph contract (kind/payload/node_id) — same raw call id under
// two graph roots must yield devin-call:* composite ids, never merged.
// ---------------------------------------------------------------------------

fn devin_packet() -> ConversationPacket {
    packet(
        "devin",
        json!({
            "source": "sqlite", "session_id": "bald-ketch", "model": "swe-1-7",
            "agent_mode": "bypass", "main_chain_id": 5, "main_chain_nodes": 5,
            "off_chain_nodes": 1
        }),
        vec![
            // Sparse FAD extras — the pinned connector's real output.
            msg(
                0,
                "user",
                "Fix the broken checks.",
                json!({"message_id": "m2", "is_user_input": true}),
            ),
            msg(
                1,
                "assistant",
                "Reading the file.\n[tool_call: read_file]",
                json!({"message_id": "m3", "thinking": "I should read it first."}),
            ),
            // Result with a recorded tool_call_id but no normalized call:
            // orphan result, never positionally linked.
            msg(
                2,
                "tool",
                "print(\"hello\")",
                json!({"message_id": "m4", "tool_call_id": "call_1"}),
            ),
            msg(
                3,
                "assistant",
                "The check passes now.",
                json!({"message_id": "m5"}),
            ),
            // Oracle contract arm: CLI-role record carrying payload.tool_calls.
            msg(
                4,
                "assistant",
                "",
                json!({"payload": {"role": "assistant", "content": "",
                        "tool_calls": [{"id": "cli_9", "name": "run_terminal_command",
                                        "arguments": {"command": "make check"}}]}}),
            ),
            // cli_tool result with explicit exit status pairs with cli_9.
            msg(
                5,
                "tool",
                "check failed",
                json!({"payload": {"role": "tool", "content": "check failed",
                        "tool_call_id": "cli_9", "exit_code": 3}}),
            ),
            // ACP tool_call under graph root 20: execute -> exec.
            msg(
                6,
                "assistant",
                "",
                json!({"kind": "tool_call", "node_id": 20, "parent_node_id": null,
                       "payload": {"content": {"kind": "execute", "title": "Ran ls",
                            "toolCallId": "dup_1", "rawInput": {"cmd": "ls"},
                            "rawOutput": "a.txt"}}}),
            ),
            // Same raw identifier under a second graph root -> composite ids.
            msg(
                7,
                "assistant",
                "",
                json!({"kind": "tool_call", "node_id": 30, "parent_node_id": null,
                       "payload": {"content": {"kind": "read", "toolCallId": "dup_1",
                            "rawInput": {"file_path": "/b.rs"}, "rawOutput": "b body"}}}),
            ),
            // cli_system boundary prefix: recorded compaction evidence.
            msg(
                8,
                "system",
                "You are continuing a previous Devin session",
                json!({"payload": {"role": "system", "content":
                        "You are continuing a previous Devin session"}}),
            ),
        ],
    )
}

#[test]
fn devin_facts_graph_linking_and_sparse_coverage() {
    let publication = extract_session_facts(&devin_packet(), "devin");

    // cli_9: payload.tool_calls call paired with its exit-coded result.
    let cli = publication
        .tools
        .iter()
        .find(|t| t.call_id == "cli_9")
        .expect("cli_9");
    assert_eq!(cli.raw_call_id, "cli_9");
    assert_eq!(cli.name, "run_terminal_command");
    assert!(cli.has_result);
    assert_eq!(cli.exit_code, Some(3));
    assert_eq!(cli.is_error, Some(true));

    // dup_1 under two graph roots: two composite ids, never merged.
    let composites: Vec<&coding_agent_search::viewer_extensions::facts::ToolCallFact> = publication
        .tools
        .iter()
        .filter(|t| t.raw_call_id == "dup_1")
        .collect();
    assert_eq!(composites.len(), 2, "tools: {:?}", publication.tools);
    assert!(
        composites
            .iter()
            .all(|t| t.call_id.starts_with("devin-call:") && t.call_id != "dup_1"),
        "multi-root raw ids must be composite: {composites:?}"
    );
    assert_ne!(composites[0].call_id, composites[1].call_id);
    let names: BTreeSet<&str> = composites.iter().map(|t| t.name.as_str()).collect();
    assert!(
        names.contains("exec") && names.contains("read"),
        "tool-name normalization: {names:?}"
    );
    assert!(composites.iter().all(|t| t.has_result));
    assert!(
        composites
            .iter()
            .any(|t| t.file_path.as_deref() == Some("/b.rs"))
    );

    // call_1: result recorded by the sparse arm but the call's args/ID were
    // dropped by normalization -> orphan result, absent from tools.
    assert!(
        !publication.tools.iter().any(|t| t.call_id == "call_1"),
        "orphan result must not fabricate a call: {:?}",
        publication.tools
    );
    let tools = coverage(&publication, "tools").expect("tools coverage");
    let reasons = tools.reasons.join(",");
    assert!(reasons.contains("orphan_tool_result"), "tools: {reasons}");

    assert_eq!(publication.tools.len(), 3);
    assert_eq!(publication.thinking_block_count, Some(1));
    assert_eq!(publication.tool_error_count, Some(1));
    assert_eq!(publication.tool_calls_with_exit_status, Some(1));
    // One compaction boundary record (cli_system prefix).
    assert_eq!(publication.compaction_count, Some(1));
    assert_eq!(
        publication.first_prompt.as_deref(),
        Some("Fix the broken checks.")
    );
    assert_eq!(publication.turn_count, Some(1));
    assert_eq!(publication.task_category.as_deref(), Some("debugging"));
    assert_eq!(publication.model_set, vec!["swe-1-7"]);
    assert_eq!(publication.is_subagent, None);
}

// ---------------------------------------------------------------------------
// Cursor fixture: harvested bubble extras — toolFormerCalls with recorded
// callIds and inline outputs, allThinkingBlocks, no exit statuses ever,
// conflicting callIds never merged, empty-extra message stays partial.
// ---------------------------------------------------------------------------

fn cursor_packet() -> ConversationPacket {
    packet(
        "cursor",
        json!({"source": "cursor", "model": "claude-4.5"}),
        vec![
            msg(
                0,
                "user",
                "fix the failing test",
                json!({"type": 1, "rawText": "fix the failing test",
                       "bubbleId": "b1", "createdAt": "2026-01-01T00:00:00Z"}),
            ),
            msg(
                1,
                "assistant",
                "Working on it",
                json!({
                "type": 2, "bubbleId": "b2", "modelType": "claude-4.5",
                "allThinkingBlocks": [{"text": "plan a"}, {"text": "plan b"}],
                "toolFormerCalls": [
                    {"name": "read_file_v2", "callId": "c1",
                     "args": {"path": "/README.md"}, "output": "contents"},
                    {"name": "edit_file_v2", "callId": "c2",
                     "args": {"path": "/README.md"}, "output": "rejected",
                     "isError": true},
                    {"name": "run_terminal_command_v2", "callId": "c3",
                     "args": {"command": "cargo test"}, "result": "3 failed",
                     "rejected": true}
                ]}),
            ),
            msg(
                2,
                "assistant",
                "final check",
                json!({"type": 2, "bubbleId": "b3", "thinking": {"text": "final check"}}),
            ),
            // Conflicting recorded callId across bubbles: same id, different
            // args -> ambiguous, never merged.
            msg(
                3,
                "assistant",
                "dup",
                json!({"type": 2, "bubbleId": "b4",
                       "toolFormerCalls": [{"name": "grep_search", "callId": "c_dup",
                                            "args": {"q": "a"}, "output": "1"}]}),
            ),
            msg(
                4,
                "assistant",
                "dup",
                json!({"type": 2, "bubbleId": "b5",
                       "toolFormerCalls": [{"name": "grep_search", "callId": "c_dup",
                                            "args": {"q": "b"}, "output": "2"}]}),
            ),
            // Legacy path bubble with no extra evidence at all.
            msg(5, "assistant", "tail", json!({})),
        ],
    )
}

#[test]
fn cursor_facts_bubble_tools_thinking_and_coverage() {
    let publication = extract_session_facts(&cursor_packet(), "cursor");

    // Tool-name normalization (CURSOR_TOOL_NAME_MAP): read_file_v2 -> read_file,
    // edit_file_v2 -> edit_file, run_terminal_command_v2 -> bash.
    let read = publication
        .tools
        .iter()
        .find(|t| t.call_id == "c1")
        .expect("c1");
    assert_eq!(read.name, "read_file");
    assert!(read.has_result);
    assert_eq!(read.file_path.as_deref(), Some("/README.md"));

    let edit = publication
        .tools
        .iter()
        .find(|t| t.call_id == "c2")
        .expect("c2");
    assert_eq!(edit.name, "edit_file");
    assert_eq!(edit.is_error, Some(true));

    let bash = publication
        .tools
        .iter()
        .find(|t| t.call_id == "c3")
        .expect("c3");
    assert_eq!(bash.name, "bash");
    assert_eq!(bash.is_error, Some(true));

    // Cursor bubbles never record exit codes: unknown, not zero.
    assert_eq!(publication.tool_calls_with_exit_status, Some(0));
    assert_eq!(publication.tool_error_count, Some(2));
    assert_eq!(publication.file_fact_count, Some(2));

    // Conflicting callId records: c_dup is ambiguous and must not appear.
    assert!(
        !publication.tools.iter().any(|t| t.raw_call_id == "c_dup"),
        "conflicting call ids must not merge: {:?}",
        publication.tools
    );
    assert_eq!(publication.tools.len(), 3);

    // allThinkingBlocks + thinking.text = 3 thinking blocks.
    assert_eq!(publication.thinking_block_count, Some(3));

    assert_eq!(
        publication.first_prompt.as_deref(),
        Some("fix the failing test")
    );
    assert_eq!(publication.turn_count, Some(1));
    assert_eq!(publication.task_category.as_deref(), Some("testing"));
    assert_eq!(publication.model_set, vec!["claude-4.5"]);
    assert_eq!(publication.is_subagent, None);
    assert_eq!(publication.compaction_count, None);

    let tools = coverage(&publication, "tools").expect("tools coverage");
    assert!(
        tools.reasons.iter().any(|r| r == "conflicting_event"),
        "tools: {:?}",
        tools.reasons
    );
    let exit = coverage(&publication, "tool_exit").expect("tool_exit");
    assert_eq!(exit.state, "unavailable");
}

// ---------------------------------------------------------------------------
// Publication: idempotent upsert keyed on (extractor_version, source_revision)
// inside the canonical archive.
// ---------------------------------------------------------------------------

struct Db {
    _temp: tempfile::TempDir,
    storage: FrankenStorage,
    agent_id: i64,
}

fn db() -> Result<Db> {
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("archive.db");
    let storage = FrankenStorage::open(&path)?;
    // Extension profile requires the canonical schema first.
    schema::ensure_schema(storage.raw())?;
    let agent_id = storage.ensure_agent(&Agent {
        id: None,
        slug: "claude_code".into(),
        name: "claude_code".into(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    Ok(Db {
        _temp: temp,
        storage,
        agent_id,
    })
}

fn canonical_conversation(
    agent_slug: &str,
    external_id: &str,
    metadata: Value,
    messages: Vec<Message>,
) -> Conversation {
    Conversation {
        id: None,
        agent_slug: agent_slug.to_string(),
        workspace: None,
        external_id: Some(external_id.to_string()),
        title: Some("t".into()),
        source_path: format!("/tmp/{external_id}.jsonl").into(),
        started_at: Some(1),
        ended_at: Some(2),
        approx_tokens: None,
        metadata_json: metadata,
        source_id: "local".into(),
        origin_host: None,
        messages,
    }
}

fn can_msg(idx: i64, role: MessageRole, content: &str, extra: Value) -> Message {
    Message {
        id: None,
        idx,
        role,
        author: None,
        created_at: Some(idx),
        content: content.to_string(),
        extra_json: extra,
        snippets: Vec::new(),
    }
}

#[allow(clippy::type_complexity)]
fn fact_row(
    db: &Db,
    conversation_id: i64,
) -> Result<(
    i64,
    i64,
    String,
    Option<i64>,
    Option<i64>,
    Option<String>,
    Option<String>,
)> {
    let row = db.storage.raw().query_row(&format!(
        "SELECT extractor_version, source_revision, coverage_state, \
         parent_conversation_id, thinking_block_count, task_category, first_prompt \
         FROM viewer_session_facts WHERE conversation_id = {conversation_id}"
    ))?;
    Ok((
        row.get_typed(0)?,
        row.get_typed(1)?,
        row.get_typed(2)?,
        row.get_typed(3)?,
        row.get_typed(4)?,
        row.get_typed(5)?,
        row.get_typed(6)?,
    ))
}

#[test]
fn publish_is_idempotent_replace_and_resolves_parent() -> Result<()> {
    let db = db()?;

    // Parent session: external_id is the path-derived identity the Claude
    // connector emits (relative path under the projects root).
    let parent = canonical_conversation(
        "claude_code",
        "-work/proj/parent-sess.jsonl",
        json!({"source": "claude_code"}),
        vec![can_msg(0, MessageRole::User, "hi", json!({}))],
    );
    let parent_id = db
        .storage
        .insert_conversation_tree(db.agent_id, None, &parent)?
        .conversation_id;

    let packet = claude_packet();
    let publication = extract_session_facts(&packet, "claude_code");

    let child = canonical_conversation(
        "claude_code",
        "-work/proj/parent-sess/subagents/agent-1.jsonl",
        json!({"source": "claude_code", "sidechain": true, "parentSessionId": "parent-sess"}),
        packet
            .payload
            .messages
            .iter()
            .map(|m| {
                can_msg(
                    m.idx,
                    match m.role.as_str() {
                        "user" => MessageRole::User,
                        "tool" => MessageRole::Tool,
                        "system" => MessageRole::System,
                        _ => MessageRole::Agent,
                    },
                    &m.content,
                    m.extra_json.clone(),
                )
            })
            .collect(),
    );
    let child_id = db
        .storage
        .insert_conversation_tree(db.agent_id, None, &child)?
        .conversation_id;

    let outcome = publish_session_facts(db.storage.raw(), child_id, &publication)?;
    ensure!(outcome == FactPublishOutcome::Published, "first publish");

    let (version, revision, state, parent, thinking, category, prompt) = fact_row(&db, child_id)?;
    ensure!(version == 1, "extractor_version");
    ensure!(revision == publication.source_revision, "source_revision");
    ensure!(state == publication.coverage_state, "coverage_state");
    ensure!(
        parent == Some(parent_id),
        "parent resolved to {parent_id:?}, got {parent:?}"
    );
    ensure!(thinking == Some(1), "thinking_block_count");
    ensure!(category.as_deref() == Some("testing"), "task_category");
    ensure!(
        prompt.as_deref() == Some("please fix the flaky test"),
        "first_prompt"
    );

    // Unchanged content: skip, row untouched (same revision, no rewrite).
    let outcome = publish_session_facts(db.storage.raw(), child_id, &publication)?;
    ensure!(
        outcome == FactPublishOutcome::SkippedUnchanged,
        "unchanged skip"
    );

    // Changed content: deterministic replace with the new source revision.
    let mut changed = packet;
    changed.payload.messages.push(
        coding_agent_search::model::conversation_packet::ConversationPacketMessage {
            message_id: None,
            idx: 10,
            role: "user".to_string(),
            author: None,
            created_at: Some(1_700_000_000_010),
            content: "and reformat".to_string(),
            extra_json: json!({"type": "user", "uuid": "u9"}),
            snippets: Vec::new(),
        },
    );
    changed.hashes.semantic_hash = format!("f{}", &changed.hashes.semantic_hash[1..]);
    let changed_pub = extract_session_facts(&changed, "claude_code");
    ensure!(
        changed_pub.source_revision != publication.source_revision,
        "content change must move source_revision"
    );
    let outcome = publish_session_facts(db.storage.raw(), child_id, &changed_pub)?;
    ensure!(outcome == FactPublishOutcome::Published, "changed replace");
    let (_, new_revision, _, _, _, _, _) = fact_row(&db, child_id)?;
    ensure!(
        new_revision == changed_pub.source_revision,
        "replaced revision"
    );
    Ok(())
}

#[test]
fn publish_reports_missing_extension_profile() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("bare.db");
    let storage = FrankenStorage::open(&path)?;
    let publication = extract_session_facts(&claude_packet(), "claude_code");
    let outcome = publish_session_facts(storage.raw(), 1, &publication)?;
    ensure!(
        outcome == FactPublishOutcome::ExtensionNotInstalled,
        "unextended archive must report, not fail"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Task 8: stable steps/turns/tools projection anchored to canonical messages
// (spec decision 4: no duplicate whole-trajectory storage; structural records
// anchored to canonical messages with proven offsets).
// ---------------------------------------------------------------------------

use coding_agent_search::viewer_extensions::projection::{
    ProjectionPublishOutcome, project_session_structure, publish_session_structure,
    publish_structure_for_canonical_best_effort,
};

fn members(
    structure: &coding_agent_search::viewer_extensions::projection::StructuralPublication,
    step_index: i64,
) -> Vec<(i64, &str)> {
    structure
        .steps
        .iter()
        .filter(|l| l.step_index == step_index && matches!(l.scope.as_str(), "member" | "result"))
        .map(|l| (l.source_idx, l.scope.as_str()))
        .collect()
}

fn parts(
    structure: &coding_agent_search::viewer_extensions::projection::StructuralPublication,
    step_index: i64,
) -> Vec<(&str, Option<&str>, i64)> {
    structure
        .steps
        .iter()
        .filter(|l| l.step_index == step_index && !matches!(l.scope.as_str(), "member" | "result"))
        .map(|l| (l.scope.as_str(), l.call_id.as_deref(), l.source_idx))
        .collect()
}

#[test]
fn projection_claude_merges_stream_and_splits_blocks() {
    let packet = claude_packet();
    let facts = extract_session_facts(&packet, "claude_code");
    let structure = project_session_structure(&packet, &facts);

    // Two surviving steps: the user turn and ONE merged logical reply
    // (a1..a4 share requestId/message.id despite interleaved tool results).
    let step_indices: BTreeSet<i64> = structure.steps.iter().map(|l| l.step_index).collect();
    assert_eq!(
        step_indices,
        BTreeSet::from([1, 2]),
        "{:?}",
        structure.steps
    );

    // Step 1: uuid-dedup folds the identical u1 records — both physical
    // messages remain member provenance of the same step.
    let step1 = members(&structure, 1);
    assert_eq!(
        step1,
        vec![(0, "member"), (1, "member")],
        "merged uuid provenance: {step1:?}"
    );
    let step1_parts = parts(&structure, 1);
    assert_eq!(step1_parts, vec![("text", None, 0)], "{step1_parts:?}");

    // Step 2: merged reply — members 3,4,8,9 plus absorbed verified
    // results 5,6; the orphan result (idx 7) is never absorbed.
    let step2 = members(&structure, 2);
    assert_eq!(
        step2,
        vec![
            (3, "member"),
            (4, "member"),
            (8, "member"),
            (9, "member"),
            (5, "result"),
            (6, "result"),
        ],
        "merged step provenance: {step2:?}"
    );

    // Parts: thinking dedup'd once across the merge, tool_use dedup by id
    // (toolu_1 appears twice), the conflicting toolu_dup stays recorded
    // (unverified — no viewer_tool_calls row claims it).
    let step2_parts = parts(&structure, 2);
    let kinds: Vec<&str> = step2_parts.iter().map(|p| p.0).collect();
    assert_eq!(
        kinds,
        vec!["thinking", "tool_call", "tool_call", "tool_call"],
        "{step2_parts:?}"
    );
    let calls: Vec<Option<&str>> = step2_parts.iter().map(|p| p.1).collect();
    assert_eq!(
        calls,
        vec![None, Some("toolu_1"), Some("toolu_2"), Some("toolu_dup")]
    );

    // Conflicting uuid pair produced no step and is reported.
    assert!(
        structure
            .warnings
            .iter()
            .any(|w| w.contains("conflicting_event")),
        "warnings: {:?}",
        structure.warnings
    );

    // Verified links carry exact anchors: the call's recording message and
    // the claimed result record (never positional).
    let t1 = structure
        .tool_calls
        .iter()
        .find(|t| t.call_id == "toolu_1")
        .expect("toolu_1 link");
    assert_eq!(t1.step_index, Some(2));
    assert_eq!(t1.call_idx, 3);
    assert_eq!(t1.result_idx, Some(5));
    let t2 = structure
        .tool_calls
        .iter()
        .find(|t| t.call_id == "toolu_2")
        .expect("toolu_2 link");
    assert_eq!(t2.result_idx, Some(6));

    // Turns: user turn then one ai turn.
    let turn_kinds: Vec<&str> = structure
        .turns
        .iter()
        .map(|t| t.turn_kind.as_str())
        .collect();
    assert_eq!(turn_kinds, vec!["user", "ai"], "{:?}", structure.turns);
    assert_eq!(structure.turns[0].step_count, 1);
    assert_eq!(structure.turns[1].step_count, 1);

    // Stable across re-computation (reindex determinism).
    let again = project_session_structure(&packet, &facts);
    assert_eq!(structure.steps, again.steps);
    assert_eq!(structure.turns, again.turns);
    assert_eq!(structure.tool_calls, again.tool_calls);
}

#[test]
fn projection_codex_compaction_boundary_and_result_claims() {
    let packet = codex_packet();
    let facts = extract_session_facts(&packet, "codex");
    let structure = project_session_structure(&packet, &facts);

    // Steps: user@1, call@2, reasoning@4, boundary@5, call@6, reasoning@9.
    // session_meta(idx0) is noise; outputs idx3/7/8 produce no step;
    // encrypted reasoning idx10 produces no step.
    let steps: Vec<(i64, i64, &str)> = structure
        .steps
        .iter()
        .filter(|l| l.scope == "member")
        .map(|l| (l.step_index, l.source_idx, l.step_kind.as_str()))
        .collect();
    assert_eq!(
        steps,
        vec![
            (1, 1, "user_message"),
            (2, 2, "turn"),
            (3, 4, "turn"),
            (4, 5, "compact_boundary"),
            (5, 6, "turn"),
            (6, 9, "turn"),
        ],
        "{steps:?}"
    );

    // Turns: user / ai(steps 2-3) / boundary / ai(steps 5-6).
    let turns: Vec<(i64, &str, i64, i64)> = structure
        .turns
        .iter()
        .map(|t| {
            (
                t.turn_index,
                t.turn_kind.as_str(),
                t.first_step_index,
                t.step_count,
            )
        })
        .collect();
    assert_eq!(
        turns,
        vec![
            (0, "user", 1, 1),
            (1, "ai", 2, 2),
            (2, "compact_boundary", 4, 1),
            (3, "ai", 5, 2),
        ],
        "{turns:?}"
    );

    // The pre-boundary call claims idx3; the segment-qualified post call
    // claims idx7 — never the replayed identical result at idx8.
    let pre = structure
        .tool_calls
        .iter()
        .find(|t| t.call_id == "call_1")
        .expect("pre call");
    assert_eq!(pre.result_idx, Some(3));
    assert_eq!(pre.step_index, Some(2));
    let post = structure
        .tool_calls
        .iter()
        .find(|t| t.call_id.starts_with("codex-segment:"))
        .expect("post call");
    assert_eq!(
        post.result_idx,
        Some(7),
        "post result must be idx7 not the replay idx8"
    );
    assert_eq!(post.step_index, Some(5));
    assert_eq!(post.turn_index, Some(3));

    // Reasoning steps carry thinking parts; encrypted reasoning none.
    assert!(
        parts(&structure, 3).iter().any(|p| p.0 == "thinking"),
        "reasoning step part: {:?}",
        parts(&structure, 3)
    );
    assert!(
        parts(&structure, 6).iter().any(|p| p.0 == "thinking"),
        "agent_reasoning step part"
    );
}

#[test]
fn projection_sparse_index_and_opaque_remote_source() {
    let provenance = ConversationPacketProvenance {
        source_id: "mirror-1".to_string(),
        origin_kind: "remote".to_string(),
        origin_host: Some("builder".to_string()),
    };
    let conv = NormalizedConversation {
        agent_slug: "antigravity".to_string(),
        external_id: Some("remote-ext".to_string()),
        title: Some("r".to_string()),
        workspace: None,
        source_path: "/remote/sess.ag".into(),
        started_at: Some(1),
        ended_at: Some(2),
        metadata: json!({"source": "antigravity"}),
        messages: vec![
            msg(0, "user", "remote ask", json!({})),
            msg(40, "assistant", "remote reply", json!({"thinking": "t"})),
            msg(90, "assistant", "tail", json!({})),
        ],
    };
    let packet = ConversationPacket::from_normalized_conversation(&conv, provenance);
    let facts = extract_session_facts(&packet, "antigravity");
    let structure = project_session_structure(&packet, &facts);

    // Sparse idx: steps stay sequential; anchors keep the recorded idx
    // and fall back to `i<idx>` source keys (no canonical ids in a
    // normalized packet).
    let steps: Vec<(i64, i64, &str)> = structure
        .steps
        .iter()
        .filter(|l| l.scope == "member")
        .map(|l| (l.step_index, l.source_idx, l.source_key.as_str()))
        .collect();
    assert_eq!(
        steps,
        vec![(1, 0, "i0"), (2, 40, "i40"), (3, 90, "i90")],
        "{steps:?}"
    );
    assert!(
        structure.steps.iter().all(|l| l.message_id.is_none()),
        "normalized packet carries no canonical ids"
    );
    let turn_kinds: Vec<&str> = structure
        .turns
        .iter()
        .map(|t| t.turn_kind.as_str())
        .collect();
    assert_eq!(turn_kinds, vec!["user", "ai"]);
}

#[test]
fn projection_publish_is_idempotent_replace_with_anchors() -> Result<()> {
    let db = db()?;
    let packet = claude_packet();
    let conv = canonical_conversation(
        "claude_code",
        "proj-ext",
        json!({"source": "claude_code", "sidechain": true, "parentSessionId": "p"}),
        packet
            .payload
            .messages
            .iter()
            .map(|m| {
                can_msg(
                    m.idx,
                    match m.role.as_str() {
                        "user" => MessageRole::User,
                        "tool" => MessageRole::Tool,
                        "system" => MessageRole::System,
                        _ => MessageRole::Agent,
                    },
                    &m.content,
                    m.extra_json.clone(),
                )
            })
            .collect(),
    );
    let conversation_id = db
        .storage
        .insert_conversation_tree(db.agent_id, None, &conv)?
        .conversation_id;

    let facts = extract_session_facts(&packet, "claude_code");
    let structure = project_session_structure(&packet, &facts);

    let db_size = |db: &Db| -> u64 {
        ["archive.db", "archive.db-wal", "archive.db-shm"]
            .iter()
            .map(|name| {
                std::fs::metadata(db._temp.path().join(name))
                    .map(|m| m.len())
                    .unwrap_or(0)
            })
            .sum()
    };
    let _ = db.storage.raw().execute("PRAGMA wal_checkpoint(TRUNCATE)");
    let size_before = db_size(&db);
    let outcome = publish_session_structure(db.storage.raw(), conversation_id, &structure)?;
    ensure!(
        outcome == ProjectionPublishOutcome::Published,
        "first publish"
    );
    let _ = db.storage.raw().execute("PRAGMA wal_checkpoint(TRUNCATE)");
    let size_after = db_size(&db);
    let body_bytes: usize = conv.messages.iter().map(|m| m.content.len()).sum();
    eprintln!(
        "projection disk overhead: {} link rows for {} messages \
         ({} message-body bytes): db+wal {} -> {} bytes (+{})",
        structure.steps.len() + structure.turns.len() + structure.tool_calls.len(),
        conv.messages.len(),
        body_bytes,
        size_before,
        size_after,
        size_after.saturating_sub(size_before)
    );

    // Anchors resolved through the canonical idx→id join; source_key
    // flips to `m<id>` form once ids exist.
    let rows = db.storage.raw().query(
        "SELECT step_index,scope,source_idx,message_id,source_key \
         FROM viewer_step_links WHERE conversation_id = 1 ORDER BY step_index, link_index",
    )?;
    ensure!(!rows.is_empty(), "step links persisted");
    let merged_members: Vec<i64> = rows
        .iter()
        .filter(|r| {
            r.get_typed::<i64>(0).unwrap() == 2 && r.get_typed::<String>(1).unwrap() == "member"
        })
        .map(|r| r.get_typed(2).unwrap())
        .collect();
    ensure!(
        merged_members == vec![3, 4, 8, 9],
        "merged member idxs: {merged_members:?}"
    );
    ensure!(
        rows.iter()
            .all(|r| r.get_typed::<Option<i64>>(3).unwrap().is_some()),
        "every link resolves a canonical message_id"
    );
    let key: String = rows[0].get_typed(4)?;
    ensure!(key.starts_with('m'), "resolved source_key: {key}");

    let tool_rows = db.storage.raw().query(
        "SELECT call_id,result_idx FROM viewer_tool_calls WHERE conversation_id = 1 ORDER BY ordinal",
    )?;
    let tool_map: Vec<(String, Option<i64>)> = tool_rows
        .iter()
        .map(|r| (r.get_typed(0).unwrap(), r.get_typed(1).unwrap()))
        .collect();
    ensure!(
        tool_map
            == vec![
                ("toolu_1".to_string(), Some(5)),
                ("toolu_2".to_string(), Some(6))
            ],
        "tool links: {tool_map:?}"
    );

    // Marker + logical projection revision.
    let marker = db.storage.raw().query_row(
        "SELECT projection_version, source_revision FROM viewer_session_projection \
         WHERE conversation_id = 1",
    )?;
    ensure!(marker.get_typed::<i64>(0)? == 1, "projection_version");
    let revision: i64 = db
        .storage
        .raw()
        .query_row("SELECT projection_revision FROM viewer_revisions WHERE id = 1")?
        .get_typed(0)?;
    ensure!(revision == 1, "projection_revision bumped once: {revision}");

    // Unchanged content: skip, no rewrite, no second bump.
    let outcome = publish_session_structure(db.storage.raw(), conversation_id, &structure)?;
    ensure!(outcome == ProjectionPublishOutcome::SkippedUnchanged);
    let revision: i64 = db
        .storage
        .raw()
        .query_row("SELECT projection_revision FROM viewer_revisions WHERE id = 1")?
        .get_typed(0)?;
    ensure!(revision == 1, "skip must not bump: {revision}");

    // Changed content: deterministic replace.
    let mut changed = packet;
    changed.payload.messages.push(
        coding_agent_search::model::conversation_packet::ConversationPacketMessage {
            message_id: None,
            idx: 11,
            role: "user".to_string(),
            author: None,
            created_at: Some(1_700_000_000_011),
            content: "and reformat".to_string(),
            extra_json: json!({"type": "user", "uuid": "u9"}),
            snippets: Vec::new(),
        },
    );
    changed.hashes.semantic_hash = format!("f{}", &changed.hashes.semantic_hash[1..]);
    let changed_facts = extract_session_facts(&changed, "claude_code");
    let changed_structure = project_session_structure(&changed, &changed_facts);
    let outcome = publish_session_structure(db.storage.raw(), conversation_id, &changed_structure)?;
    ensure!(outcome == ProjectionPublishOutcome::Published, "replace");
    let steps: Vec<i64> = db
        .storage
        .raw()
        .query("SELECT DISTINCT step_index FROM viewer_step_links WHERE conversation_id = 1 ORDER BY step_index")?
        .iter()
        .map(|r| r.get_typed(0).unwrap())
        .collect();
    ensure!(steps == vec![1, 2, 3], "replaced steps: {steps:?}");

    // Best-effort ingest path: builds packet + facts itself.
    publish_structure_for_canonical_best_effort(db.storage.raw(), &conv, conversation_id);
    let count: i64 = db
        .storage
        .raw()
        .query_row("SELECT COUNT(*) FROM viewer_step_links WHERE conversation_id = 1")?
        .get_typed(0)?;
    ensure!(count > 0, "best-effort publish persists links");
    Ok(())
}

#[test]
fn projection_publish_reports_missing_extension_profile() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("bare-projection.db");
    let storage = FrankenStorage::open(&path)?;
    let packet = claude_packet();
    let facts = extract_session_facts(&packet, "claude_code");
    let structure = project_session_structure(&packet, &facts);
    let outcome = publish_session_structure(storage.raw(), 1, &structure)?;
    ensure!(
        outcome == ProjectionPublishOutcome::ExtensionNotInstalled,
        "unextended archive must report, not fail"
    );
    Ok(())
}
