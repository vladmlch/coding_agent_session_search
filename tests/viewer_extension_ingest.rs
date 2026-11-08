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
    FACTS_EXTRACTOR_VERSION, FactPublishOutcome, extract_session_facts, is_source_absent_gap,
    publish_session_facts,
};
use coding_agent_search::viewer_extensions::schema;
use coding_agent_search::viewer_extensions::search_regions::derive_message_regions;
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
// Codex agent_message items: `response_item` inter-agent handoffs carry the
// text in content[].input_text parts (the `event_msg` form keeps a bare
// `message` string); a record with no usable text stays malformed.
// ---------------------------------------------------------------------------

fn codex_agent_message_packet(record: Value) -> ConversationPacket {
    packet(
        "codex",
        json!({"source": "rollout"}),
        vec![msg(0, "assistant", "handoff body", record)],
    )
}

#[test]
fn codex_agent_message_response_item_reads_content_parts() {
    let publication = extract_session_facts(
        &codex_agent_message_packet(json!({
            "type": "response_item",
            "payload": {
                "type": "agent_message",
                "role": "agent",
                "content": [{"type": "input_text", "text": "subagent handoff body"}]
            }
        })),
        "codex",
    );
    let chat = coverage(&publication, "chat").expect("chat coverage");
    assert_eq!(chat.state, "complete", "chat: {chat:?}");
    assert!(
        chat.reasons.is_empty(),
        "agent_message content parts must not be malformed: {:?}",
        chat.reasons
    );
}

#[test]
fn codex_agent_message_event_msg_keeps_bare_message_string() {
    let publication = extract_session_facts(
        &codex_agent_message_packet(json!({
            "type": "event_msg",
            "payload": {"type": "agent_message", "message": "inter-agent note"}
        })),
        "codex",
    );
    let chat = coverage(&publication, "chat").expect("chat coverage");
    assert_eq!(chat.state, "complete", "chat: {chat:?}");
}

#[test]
fn codex_agent_message_without_usable_text_stays_malformed() {
    let publication = extract_session_facts(
        &codex_agent_message_packet(json!({
            "type": "response_item",
            "payload": {
                "type": "agent_message",
                "role": "agent",
                "content": [{"type": "input_text"}]
            }
        })),
        "codex",
    );
    let chat = coverage(&publication, "chat").expect("chat coverage");
    assert_eq!(chat.state, "unavailable", "chat: {chat:?}");
    assert!(
        chat.reasons.iter().any(|r| r == "codex_message_malformed"),
        "chat reasons: {:?}",
        chat.reasons
    );
}

// ---------------------------------------------------------------------------
// Codex agent_message encrypted payloads. Inter-agent handoffs (codex
// protocol `AgentMessageInputContent`) arrive either as a plaintext
// `input_text` part or as an envelope-header `input_text` plus the actual
// payload in an `encrypted_content` part — a Fernet ciphertext the model
// provider produced and the sending agent hands over already encrypted, so
// the rollout never held the plaintext. A known, deliberately unreadable
// shape: the payload marks the text inventory unavailable, the readable
// header still counts as chat evidence, and no field claims ignorance.
// ---------------------------------------------------------------------------

#[test]
fn codex_agent_message_encrypted_payload_is_known_unreadable() {
    let publication = extract_session_facts(
        &codex_agent_message_packet(json!({
            "type": "response_item",
            "payload": {
                "type": "agent_message",
                "id": "amsg_1",
                "author": "/root/worker",
                "recipient": "/root",
                "content": [
                    {"type": "input_text",
                     "text": "Message Type: MESSAGE\nTask name: /root\nSender: /root/worker\nPayload:\n"},
                    {"type": "encrypted_content", "encrypted_content": "gAAAAAB-fixture-ciphertext"}
                ]
            }
        })),
        "codex",
    );

    // The payload's text is provider-encrypted: a known shape, unreadable
    // text — the same verdict redacted thinking earns, never "unknown".
    let text = coverage(&publication, "text_inventory").expect("text_inventory");
    assert_eq!(text.state, "unavailable", "text_inventory: {text:?}");
    assert!(
        text.reasons.iter().any(|r| r == "agent_message_encrypted"),
        "text_inventory reasons: {:?}",
        text.reasons
    );
    assert!(
        !text
            .reasons
            .iter()
            .any(|r| r == "codex_message_part_unknown"),
        "an encrypted part is not an unknown shape: {:?}",
        text.reasons
    );

    // The readable envelope header is still chat evidence (partial, not
    // absent), and a message part never touches the tool scopes.
    let chat = coverage(&publication, "chat").expect("chat coverage");
    assert_eq!(chat.state, "partial", "chat: {chat:?}");
    let tools = coverage(&publication, "tools").expect("tools coverage");
    assert_eq!(tools.state, "complete", "tools: {tools:?}");
}

// ---------------------------------------------------------------------------
// Codex extra-compaction envelopes. Past the compaction threshold (cass
// ≥16 MiB, FAD ≥32 MiB) per-message extras are reduced to the `{"cass":{…}}`
// skeleton or its empty `{}` product; the message text itself stays in
// `content`. The connector only ever emits `response_item`/`event_msg`
// records, so an unwrappable codex record IS the envelope: the honest
// verdict is the partial `extra_compacted`, never an unknown-shape claim.
// Legacy-format rollout items (`{"role":…,"content":…}`) are not envelopes
// and keep the unknown-shape verdict.
// ---------------------------------------------------------------------------

#[test]
fn codex_compacted_extra_envelope_is_a_known_shape() {
    let publication = extract_session_facts(
        &packet(
            "codex",
            json!({"source": "rollout"}),
            vec![
                // A plain record anchors the session and parses normally.
                msg(
                    0,
                    "user",
                    "run the sweep",
                    json!({"type": "event_msg",
                           "payload": {"type": "user_message", "message": "run the sweep"}}),
                ),
                // `{"cass":{…}}` skeleton — a tool-call record whose
                // invocation survived the compaction merge.
                msg(
                    1,
                    "assistant",
                    "[Tool: exec]",
                    json!({"cass": {"invocations": [
                        {"kind": "tool", "name": "exec", "call_id": "call_9",
                         "arguments": "{\"command\": \"ls\"}"}]}}),
                ),
                // The empty `{}` product of the same compaction.
                msg(2, "tool", "file-a\nfile-b", json!({})),
                // Model-bearing skeleton variant.
                msg(
                    3,
                    "assistant",
                    "done",
                    json!({"cass": {"model": "gpt-fixture"}}),
                ),
            ],
        ),
        "codex",
    );

    let text = coverage(&publication, "text_inventory").expect("text_inventory");
    assert_eq!(text.state, "partial", "text_inventory: {text:?}");
    assert!(
        text.reasons.iter().any(|r| r == "extra_compacted"),
        "text_inventory reasons: {:?}",
        text.reasons
    );
    assert!(
        !text.reasons.iter().any(|r| r == "codex_shape_unknown"),
        "the compaction envelope is a known shape: {:?}",
        text.reasons
    );
    let tools = coverage(&publication, "tools").expect("tools coverage");
    assert_eq!(tools.state, "partial", "tools: {tools:?}");
    assert!(
        tools.reasons.iter().any(|r| r == "extra_compacted"),
        "tools reasons: {:?}",
        tools.reasons
    );
}

#[test]
fn codex_unwrapped_non_envelope_record_stays_shape_unknown() {
    // A legacy-format rollout item carries role/content keys and no `type`:
    // not a compaction product, so the unknown-shape verdict must survive.
    let publication = extract_session_facts(
        &packet(
            "codex",
            json!({"source": "rollout"}),
            vec![msg(
                0,
                "user",
                "legacy body",
                json!({"role": "user", "content": "legacy body"}),
            )],
        ),
        "codex",
    );
    let text = coverage(&publication, "text_inventory").expect("text_inventory");
    assert_eq!(text.state, "unavailable", "text_inventory: {text:?}");
    assert!(
        text.reasons.iter().any(|r| r == "codex_shape_unknown"),
        "text_inventory reasons: {:?}",
        text.reasons
    );
}

// ---------------------------------------------------------------------------
// Unpaired tool calls. FAD's claude_code connector drops `tool_result`
// content with no `text` part (ToolSearch `tool_reference` results, Read
// `image` results), so for Claude an unpaired call never means session text
// was lost: `tools` stays partial, the inventories stay complete. Codex
// keeps the strict verdict — there the result record itself is missing
// from storage, so its text really is gone.
// ---------------------------------------------------------------------------

fn claude_unpaired_call_packet() -> ConversationPacket {
    let user = json!({
        "type": "user", "uuid": "u1", "sessionId": "sess-2",
        "message": {"role": "user", "content": [{"type": "text", "text": "search the tools"}]}
    });
    // A ToolSearch answer carries a `tool_reference` part with no text: FAD
    // drops the tool_result record, so the call stays unpaired in the packet.
    let call = json!({
        "type": "assistant", "uuid": "a1", "sessionId": "sess-2",
        "message": {"id": "m1", "role": "assistant", "model": "claude-opus-5",
                    "content": [{"type": "tool_use", "id": "toolu_noresult",
                                 "name": "ToolSearch", "input": {"query": "walrus"}}]}
    });
    packet(
        "claude_code",
        json!({"source": "claude_code"}),
        vec![
            msg(0, "user", "search the tools", user),
            msg(1, "assistant", "", call),
        ],
    )
}

#[test]
fn claude_unpaired_call_keeps_text_inventory_complete() {
    let publication = extract_session_facts(&claude_unpaired_call_packet(), "claude_code");

    // The pairing gap is real and stays visible on `tools`.
    let tools = coverage(&publication, "tools").expect("tools coverage");
    assert_eq!(tools.state, "partial", "tools: {tools:?}");
    assert!(
        tools.reasons.iter().any(|r| r == "tool_result_missing"),
        "tools reasons: {:?}",
        tools.reasons
    );

    // ...but no session text is missing: the dropped result had no text part.
    let text = coverage(&publication, "text_inventory").expect("text_inventory");
    assert_eq!(text.state, "complete", "text_inventory: {text:?}");
    assert!(
        !text.reasons.iter().any(|r| r == "tool_result_missing"),
        "text_inventory reasons: {:?}",
        text.reasons
    );
    let blocks = coverage(&publication, "block_inventory").expect("block_inventory");
    assert_eq!(blocks.state, "complete", "block_inventory: {blocks:?}");
}

fn codex_unpaired_call_packet() -> ConversationPacket {
    packet(
        "codex",
        json!({"source": "rollout"}),
        vec![
            msg(
                0,
                "user",
                "run the probe",
                json!({"type": "event_msg",
                       "payload": {"type": "user_message", "message": "run the probe"}}),
            ),
            // No function_call_output follows: the result record is missing
            // from storage, so its text really is lost.
            msg(
                1,
                "assistant",
                "[Tool: shell]",
                json!({"type": "response_item",
                       "payload": {"type": "function_call", "call_id": "call_unpaired",
                                   "name": "shell",
                                   "arguments": "{\"command\": \"probe\"}",
                                   "id": "resp-1"}}),
            ),
        ],
    )
}

#[test]
fn codex_unpaired_call_still_marks_text_inventory_partial() {
    let publication = extract_session_facts(&codex_unpaired_call_packet(), "codex");

    let tools = coverage(&publication, "tools").expect("tools coverage");
    assert_eq!(tools.state, "partial", "tools: {tools:?}");
    assert!(
        tools.reasons.iter().any(|r| r == "tool_result_missing"),
        "tools reasons: {:?}",
        tools.reasons
    );

    // Codex storage really loses the output record: session text is missing.
    let text = coverage(&publication, "text_inventory").expect("text_inventory");
    assert_eq!(text.state, "partial", "text_inventory: {text:?}");
    assert!(
        text.reasons.iter().any(|r| r == "tool_result_missing"),
        "text_inventory reasons: {:?}",
        text.reasons
    );
}

// ---------------------------------------------------------------------------
// Claude known non-block records. FAD lifts queued_command attachment
// prompts into user message content (cass GH #500), so their text is never
// missing; compacted `{"cass":{…}}` extras already carry their partial
// verdict through mark_compacted. Neither is an unknown shape.
// ---------------------------------------------------------------------------

fn claude_queued_command_packet() -> ConversationPacket {
    // Verbatim FAD shape: the raw attachment record plus the cass provenance
    // `queued_command_message` inserts; the prompt is the message content.
    let attachment = json!({
        "type": "attachment", "uuid": "q1", "sessionId": "sess-3",
        "timestamp": "2026-09-24T10:00:12.000Z",
        "attachment": {
            "type": "queued_command", "prompt": "also check the walrus path",
            "source_uuid": "src1", "commandMode": "prompt",
            "origin": {"kind": "human"}
        },
        "cass": {"queued_command": {"command_mode": "prompt",
                                    "authorship": "human", "source_uuid": "src1"}}
    });
    let reply = json!({
        "type": "assistant", "uuid": "a1", "sessionId": "sess-3",
        "message": {"id": "m1", "role": "assistant", "model": "claude-opus-5",
                    "content": [{"type": "text", "text": "checked the walrus path"}]}
    });
    packet(
        "claude_code",
        json!({"source": "claude_code"}),
        vec![
            msg(0, "user", "also check the walrus path", attachment),
            msg(1, "assistant", "checked the walrus path", reply),
        ],
    )
}

#[test]
fn claude_queued_command_attachment_is_known_non_block() {
    let publication = extract_session_facts(&claude_queued_command_packet(), "claude_code");

    // The prompt text lives in the message content, so no scope may report
    // an unknown shape for the attachment record.
    for entry in &publication.coverage {
        assert!(
            !entry.reasons.iter().any(|r| r == "claude_shape_unknown"),
            "{} must not be shape-unknown: {:?}",
            entry.field,
            entry.reasons
        );
    }
    let text = coverage(&publication, "text_inventory").expect("text_inventory");
    assert_eq!(text.state, "complete", "text_inventory: {text:?}");
    let blocks = coverage(&publication, "block_inventory").expect("block_inventory");
    assert_eq!(blocks.state, "complete", "block_inventory: {blocks:?}");

    // The queued command is a real user turn, not a dropped record.
    assert_eq!(
        publication.first_prompt.as_deref(),
        Some("also check the walrus path")
    );
    assert_eq!(publication.turn_count, Some(1));
}

fn claude_compacted_extra_packet() -> ConversationPacket {
    packet(
        "claude_code",
        json!({"source": "claude_code"}),
        vec![msg(
            0,
            "user",
            "the recorded structure was compacted away",
            // Large-session compaction reduces extra to the `{"cass":{…}}`
            // skeleton; the flattened text stays in the message content.
            json!({"cass": {"model": "claude-opus-5"}}),
        )],
    )
}

#[test]
fn claude_compacted_extra_stays_partial_not_unknown() {
    let publication = extract_session_facts(&claude_compacted_extra_packet(), "claude_code");

    // mark_compacted already recorded the partial verdict on every core
    // scope; the skeleton shape must not raise it to unavailable.
    for field in ["text_inventory", "block_inventory", "chat", "tools"] {
        let entry = coverage(&publication, field).expect(field);
        assert_eq!(entry.state, "partial", "{field}: {entry:?}");
        assert!(
            entry.reasons.iter().any(|r| r == "extra_compacted"),
            "{field} reasons: {:?}",
            entry.reasons
        );
        assert!(
            !entry.reasons.iter().any(|r| r == "claude_shape_unknown"),
            "{field} must not be shape-unknown: {:?}",
            entry.reasons
        );
    }
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
            // Connector drift: a record outside the verified FAD output
            // contract (role `agent` with an unmapped extra key) keeps the
            // unknown-record arm. A bare `assistant` message with an empty
            // extra is a REAL planner shape (planner response with neither
            // thinking nor calls) and must not land here.
            msg(
                6,
                "assistant",
                "trailing text",
                json!({"unmapped_future_key": "made-up drift value"}),
            ),
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
// Antigravity REAL-shape fixture: the exact extra/role shapes the pinned FAD
// connector emits for live transcripts (verified message-by-message against
// 96,722 persisted antigravity messages, synthetic text only):
//   - USER_INPUT  -> role user,    extra ⊆ {settings_change, additional_metadata}
//   - PLANNER_RESPONSE -> role agent/assistant, extra ⊆ {cass, thinking},
//     with `cass.invocations[]` carrying {kind:"tool", name, call_id: null,
//     arguments} — call ids are NEVER recorded by the connector.
//   - MODEL steps (GENERIC …) -> role tool, bare `agy_type` envelope, no
//     call identity and no status (the raw `status` field is dropped).
//   - SYSTEM_MESSAGE/CHECKPOINT -> role system, `agy_type` envelope.
// ---------------------------------------------------------------------------

fn antigravity_real_shape_packet_with_slug(slug: &str) -> ConversationPacket {
    packet(
        slug,
        json!({"source": "antigravity", "model": "MadeUp Model"}),
        vec![
            msg(
                0,
                "user",
                "made-up user request text",
                json!({
                    "settings_change": "made-up settings change text",
                    "additional_metadata": "made-up additional metadata text"
                }),
            ),
            msg(
                1,
                "assistant",
                "made-up planner narration",
                json!({
                    "thinking": "made-up thinking text",
                    "cass": {"invocations": [
                        {"kind": "tool", "name": "view_file", "call_id": null,
                         "arguments": {"TargetFile": "/made/up/target.rs"}},
                        {"kind": "tool", "name": "run_command",
                         "arguments": {"CommandLine": "made-up-run --flag"}}
                    ]}
                }),
            ),
            msg(
                2,
                "tool",
                "made-up model step output",
                json!({"agy_type": "GENERIC"}),
            ),
            msg(
                3,
                "system",
                "made-up system message text",
                json!({"agy_type": "SYSTEM_MESSAGE"}),
            ),
            msg(
                4,
                "system",
                "made-up checkpoint state",
                json!({"agy_type": "CHECKPOINT", "agy_source": "made-up source"}),
            ),
            msg(
                5,
                "system",
                "made-up ephemeral notice text",
                json!({"agy_type": "EPHEMERAL_MESSAGE"}),
            ),
        ],
    )
}

fn antigravity_real_shape_packet() -> ConversationPacket {
    antigravity_real_shape_packet_with_slug("antigravity")
}

#[test]
fn antigravity_facts_pre_f10_packet_text_scopes_complete_status_unavailable() {
    // The pre-F10 connector shape (no `truncated_fields`/`status`/`error`/
    // `media` extras — the packet the v2 extractor was written against).
    // The blanket connector-drop mark is gone, so the text scopes certify
    // complete for a session with no truncation markers, and with no
    // lifecycle `status` in the packet the exit/error verdicts stay
    // unavailable-by-connector-drop (the pre-F10 branch of the status
    // gate).
    let publication = extract_session_facts(&antigravity_real_shape_packet(), "antigravity");

    // Every real FAD output shape classifies as known: no unknown-record
    // degradation anywhere.
    assert!(
        !publication
            .coverage
            .iter()
            .any(|entry| entry.reasons.iter().any(|r| r == "agy_record_unknown")),
        "coverage: {:?}",
        publication.coverage
    );

    // No truncation markers and no dropped-record signal in the packet:
    // the text scopes are complete — the v2 blanket mark
    // (`agy_connector_drops_source_text`) is retired.
    for field in ["chat", "thinking", "text_inventory", "files"] {
        let entry = coverage(&publication, field).unwrap_or_else(|| panic!("{field}"));
        assert_eq!(entry.state, "complete", "{field}: {entry:?}");
        assert!(entry.reasons.is_empty(), "{field}: {entry:?}");
    }

    // The canonical envelope loses raw record structure — block inventory
    // only. Its text IS the message content and lands in chat regions, so
    // text_inventory must not carry the envelope reason.
    let block = coverage(&publication, "block_inventory").expect("block_inventory");
    assert_eq!(block.state, "partial", "{block:?}");
    assert!(
        block.reasons.iter().any(|r| r == "agy_canonical_envelope"),
        "{block:?}"
    );
    let text_inventory = coverage(&publication, "text_inventory").expect("text_inventory");
    assert!(
        !text_inventory
            .reasons
            .iter()
            .any(|r| r == "agy_canonical_envelope"),
        "{text_inventory:?}"
    );

    // Planner calls without recorded ids stay uncollected, never fabricated;
    // MODEL-step records without identity keep the result-side reason too.
    assert!(publication.tools.is_empty(), "{:?}", publication.tools);
    let tools = coverage(&publication, "tools").expect("tools");
    assert_eq!(tools.state, "partial", "{tools:?}");
    assert!(
        tools
            .reasons
            .iter()
            .any(|r| r == "agy_tool_call_identity_missing"),
        "{tools:?}"
    );
    assert!(
        tools
            .reasons
            .iter()
            .any(|r| r == "agy_tool_result_identity_missing"),
        "{tools:?}"
    );

    // Pre-F10 packet: the connector dropped the lifecycle `status`, so
    // exit/error evidence is unavailable — never silently complete, and
    // never the v3 `agy_status_unpaired` verdict (that one requires a
    // status the packet can actually see).
    for field in ["tool_exit", "tool_error"] {
        let entry = coverage(&publication, field).unwrap_or_else(|| panic!("{field}"));
        assert_eq!(entry.state, "unavailable", "{field}: {entry:?}");
        assert!(
            entry
                .reasons
                .iter()
                .any(|r| r == "agy_status_dropped_by_connector"),
            "{field}: {entry:?}"
        );
        assert!(
            !entry.reasons.iter().any(|r| r == "agy_status_unpaired"),
            "{field}: {entry:?}"
        );
    }

    // The format has no mcp surface at all (verified against the raw
    // transcripts): no mcp evidence can be missing, so mcp stays complete.
    let mcp = coverage(&publication, "mcp").expect("mcp");
    assert_eq!(mcp.state, "complete", "{mcp:?}");

    assert_eq!(publication.thinking_block_count, Some(1));
}

#[test]
fn antigravity_facts_unknown_future_shape_stays_unknown() {
    // Connector drift: a record outside the verified FAD antigravity
    // contract (role `agent` with an unmapped extra key) must keep the
    // unknown-record degradation — the gate never silently absorbs new
    // shapes as known.
    let drift = packet(
        "antigravity",
        json!({"source": "antigravity"}),
        vec![
            msg(0, "user", "made-up request", json!({})),
            msg(
                1,
                "assistant",
                "made-up drift record",
                json!({"unmapped_future_key": "made-up value"}),
            ),
        ],
    );
    let publication = extract_session_facts(&drift, "antigravity");
    for field in ["chat", "tools", "text_inventory", "block_inventory"] {
        let entry = coverage(&publication, field).unwrap_or_else(|| panic!("{field}"));
        assert_eq!(entry.state, "partial", "{field}: {entry:?}");
        assert!(
            entry.reasons.iter().any(|r| r == "agy_record_unknown"),
            "{field}: {entry:?}"
        );
    }
}

#[test]
fn antigravity_facts_ephemeral_message_shape_is_known() {
    // The connector maps EPHEMERAL_MESSAGE records to role `system` with
    // `agy_type: "EPHEMERAL_MESSAGE"` and the record text as the message
    // content (verified against the pinned FAD source and the persisted
    // archive: 11 records across 5 sessions). The label must classify as a
    // known envelope — an unknown kind here would leave real sessions
    // degraded for no reason.
    let ephemeral = packet(
        "antigravity",
        json!({"source": "antigravity"}),
        vec![
            msg(0, "user", "made-up request", json!({})),
            msg(
                1,
                "system",
                "made-up ephemeral notice text",
                json!({"agy_type": "EPHEMERAL_MESSAGE"}),
            ),
        ],
    );
    let publication = extract_session_facts(&ephemeral, "antigravity");

    assert!(
        !publication
            .coverage
            .iter()
            .any(|entry| entry.reasons.iter().any(|r| r == "agy_record_unknown")),
        "coverage: {:?}",
        publication.coverage
    );
    let block = coverage(&publication, "block_inventory").expect("block_inventory");
    assert_eq!(block.state, "partial", "{block:?}");
    assert!(
        block.reasons.iter().any(|r| r == "agy_canonical_envelope"),
        "{block:?}"
    );
}

#[test]
fn antigravity_facts_zero_thinking_session_proves_thinking_complete() {
    // Verified against the raw transcripts: only PLANNER_RESPONSE records
    // ever carry `thinking`, the connector never drops that record type,
    // and no record has thinking truncated to empty/absent at the source.
    // So a packet with zero thinking blocks provably had zero thinking text
    // at the source, and `thinking` keeps the claude-style complete empty
    // inventory — the connector-contract mark must not over-mark it.
    let zero_thinking = packet(
        "antigravity",
        json!({"source": "antigravity", "model": "MadeUp Model"}),
        vec![
            msg(0, "user", "made-up user request text", json!({})),
            msg(
                1,
                "assistant",
                "made-up planner narration without thinking",
                json!({"cass": {"invocations": [
                    {"kind": "tool", "name": "view_file", "call_id": null,
                     "arguments": {"TargetFile": "/made/up/target.rs"}}
                ]}}),
            ),
            msg(
                2,
                "tool",
                "made-up model step output",
                json!({"agy_type": "GENERIC"}),
            ),
        ],
    );
    let publication = extract_session_facts(&zero_thinking, "antigravity");
    assert_eq!(publication.thinking_block_count, Some(0));

    let thinking = coverage(&publication, "thinking").expect("thinking coverage");
    assert_eq!(thinking.state, "complete", "{thinking:?}");
    assert!(thinking.reasons.is_empty(), "{thinking:?}");

    // No truncation markers anywhere in this packet: the whole text
    // inventory certifies complete — the blanket mark that kept chat/
    // text_inventory/files partial for every antigravity session is gone.
    for field in ["chat", "text_inventory", "files"] {
        let entry = coverage(&publication, field).unwrap_or_else(|| panic!("{field}"));
        assert_eq!(entry.state, "complete", "{field}: {entry:?}");
        assert!(entry.reasons.is_empty(), "{field}: {entry:?}");
    }

    // This pre-F10 packet has no lifecycle `status` in the extras, so the
    // exit/error scopes keep the connector-drop verdict.
    for field in ["tool_exit", "tool_error"] {
        let entry = coverage(&publication, field).unwrap_or_else(|| panic!("{field}"));
        assert_eq!(entry.state, "unavailable", "{field}: {entry:?}");
        assert!(
            entry
                .reasons
                .iter()
                .any(|r| r == "agy_status_dropped_by_connector"),
            "{field}: {entry:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Antigravity F10 shapes: the pinned FAD connector (rev 3851a3a) passes the
// source fields through into message extras — `truncated_fields` (the
// source-side truncation markers), `status` (the record lifecycle
// DONE/RUNNING/ERROR/INVALID), `error` (the failure text), `media`
// ({mime_type, uri} attachment refs) — and emits error-only ERROR_MESSAGE
// records (the `error` text as content) plus text-bearing
// CONVERSATION_HISTORY records. Synthetic text only; real key/type
// structure only.
// ---------------------------------------------------------------------------

/// A clean F10 session: every record carries its lifecycle `status`, no
/// truncation markers, no media — the dominant real-archive shape.
fn antigravity_f10_clean_packet() -> ConversationPacket {
    packet(
        "antigravity",
        json!({"source": "antigravity", "model": "MadeUp Model"}),
        vec![
            msg(
                0,
                "user",
                "made-up clean request text",
                json!({
                    "additional_metadata": "made-up local time note",
                    "status": "DONE"
                }),
            ),
            msg(
                1,
                "assistant",
                "made-up clean planner narration",
                json!({
                    "thinking": "made-up clean thinking text",
                    "cass": {"invocations": [
                        {"kind": "tool", "name": "view_file", "call_id": null,
                         "arguments": {"TargetFile": "/made/up/clean/target.rs"}}
                    ]},
                    "status": "DONE"
                }),
            ),
            msg(
                2,
                "tool",
                "made-up clean step output",
                json!({
                    "agy_type": "GENERIC", "status": "DONE"
                }),
            ),
        ],
    )
}

#[test]
fn antigravity_f10_clean_session_text_scopes_complete() {
    let publication = extract_session_facts(&antigravity_f10_clean_packet(), "antigravity");

    // The blanket connector-drop mark is retired: a session whose records
    // carry no truncation markers certifies its text scopes complete.
    assert!(
        !publication.coverage.iter().any(|entry| entry
            .reasons
            .iter()
            .any(|r| r == "agy_connector_drops_source_text")),
        "coverage: {:?}",
        publication.coverage
    );
    // The F10 passthrough keys are part of the connector's verified output
    // contract: every record classifies as a known shape, no drift.
    assert!(
        !publication
            .coverage
            .iter()
            .any(|entry| entry.reasons.iter().any(|r| r == "agy_record_unknown")),
        "coverage: {:?}",
        publication.coverage
    );
    for field in ["chat", "thinking", "text_inventory", "files", "mcp"] {
        let entry = coverage(&publication, field).unwrap_or_else(|| panic!("{field}"));
        assert_eq!(entry.state, "complete", "{field}: {entry:?}");
        assert!(entry.reasons.is_empty(), "{field}: {entry:?}");
    }

    // Tool scopes keep their honest marks: the connector records no call
    // ids (identity), and the F10 lifecycle `status` — now visible in the
    // packet — never pairs with the planner call it belongs to, so
    // per-call exit/error stays unprovable: partial, never silently
    // complete, and never the retired connector-drop verdict.
    for field in ["tool_exit", "tool_error"] {
        let entry = coverage(&publication, field).unwrap_or_else(|| panic!("{field}"));
        assert_eq!(entry.state, "partial", "{field}: {entry:?}");
        assert!(
            entry.reasons.iter().any(|r| r == "agy_status_unpaired"),
            "{field}: {entry:?}"
        );
        assert!(
            !entry
                .reasons
                .iter()
                .any(|r| r == "agy_status_dropped_by_connector"),
            "{field}: {entry:?}"
        );
    }
    let tools = coverage(&publication, "tools").expect("tools");
    assert_eq!(tools.state, "partial", "{tools:?}");
    assert!(
        tools
            .reasons
            .iter()
            .any(|r| r == "agy_tool_call_identity_missing"),
        "{tools:?}"
    );
    assert!(
        tools
            .reasons
            .iter()
            .any(|r| r == "agy_tool_result_identity_missing"),
        "{tools:?}"
    );

    // The canonical envelope still loses raw record structure — block
    // inventory only.
    let block = coverage(&publication, "block_inventory").expect("block_inventory");
    assert_eq!(block.state, "partial", "{block:?}");
    assert!(
        block.reasons.iter().any(|r| r == "agy_canonical_envelope"),
        "{block:?}"
    );
}

#[test]
fn antigravity_f10_truncated_fields_mark_scopes_source_absent() {
    // A session whose records carry the source-side truncation markers:
    // the app cut the named fields before persisting, so the cut text
    // never existed in any local record — the affected scopes are partial
    // with `agy_source_truncated` only, the source-absent reason the
    // strict-search allowlist names.
    let truncated = packet(
        "antigravity",
        json!({"source": "antigravity"}),
        vec![
            // USER_INPUT with the request body truncated at the source.
            msg(
                0,
                "user",
                "made-up shortened request",
                json!({
                    "status": "DONE",
                    "truncated_fields": ["content"]
                }),
            ),
            // PLANNER_RESPONSE with thinking and tool_calls truncated.
            msg(
                1,
                "assistant",
                "made-up planner narration",
                json!({
                    "thinking": "made-up partial thinking",
                    "cass": {"invocations": [
                        {"kind": "tool", "name": "view_file", "call_id": null,
                         "arguments": {"TargetFile": "/made/up/truncated-target.rs"}}
                    ]},
                    "status": "DONE",
                    "truncated_fields": ["thinking", "tool_calls"]
                }),
            ),
            msg(
                2,
                "tool",
                "made-up step output",
                json!({
                    "agy_type": "GENERIC", "status": "DONE"
                }),
            ),
        ],
    );
    let publication = extract_session_facts(&truncated, "antigravity");

    // Content truncation degrades chat + the text inventory, and nothing
    // else piles onto those scopes.
    let chat = coverage(&publication, "chat").expect("chat");
    assert_eq!(chat.state, "partial", "{chat:?}");
    assert_eq!(
        chat.reasons,
        vec!["agy_source_truncated".to_string()],
        "{chat:?}"
    );
    let thinking = coverage(&publication, "thinking").expect("thinking");
    assert_eq!(thinking.state, "partial", "{thinking:?}");
    assert_eq!(
        thinking.reasons,
        vec!["agy_source_truncated".to_string()],
        "{thinking:?}"
    );
    let text_inventory = coverage(&publication, "text_inventory").expect("text_inventory");
    assert_eq!(text_inventory.state, "partial", "{text_inventory:?}");
    assert!(
        text_inventory
            .reasons
            .iter()
            .all(|r| r == "agy_source_truncated"),
        "{text_inventory:?}"
    );

    // The marker reason is on the source-absent allowlist: pure marker
    // gaps do not block strict search (the bridge suite pins the
    // end-to-end annotation).
    assert!(is_source_absent_gap(chat.state.as_str(), &chat.reasons));
    assert!(is_source_absent_gap(
        text_inventory.state.as_str(),
        &text_inventory.reasons
    ));

    // tool_calls truncation marks the tools scope, but the unrecorded call
    // identities keep their own not-read reason there — a mixed reason
    // set stays a NOT-READ gap exactly as before.
    let tools = coverage(&publication, "tools").expect("tools");
    assert_eq!(tools.state, "partial", "{tools:?}");
    assert!(
        tools.reasons.iter().any(|r| r == "agy_source_truncated"),
        "{tools:?}"
    );
    assert!(
        tools
            .reasons
            .iter()
            .any(|r| r == "agy_tool_call_identity_missing"),
        "{tools:?}"
    );
    assert!(!is_source_absent_gap(tools.state.as_str(), &tools.reasons));

    // tool_calls truncation also cuts call arguments — file paths ride in
    // those arguments, so the files scope degrades with the same marker-
    // only reason (a pure source-absent gap).
    let files = coverage(&publication, "files").expect("files");
    assert_eq!(files.state, "partial", "{files:?}");
    assert_eq!(
        files.reasons,
        vec!["agy_source_truncated".to_string()],
        "{files:?}"
    );
}

#[test]
fn antigravity_f10_truncated_fields_unknown_media_and_malformed() {
    // An unrecognized marker field name is still a real source-side
    // truncation: some field was cut, and if it held text that text is
    // absent at the source — text_inventory only, conservatively.
    let unknown_field = packet(
        "antigravity",
        json!({"source": "antigravity"}),
        vec![msg(
            0,
            "user",
            "made-up request",
            json!({
                "status": "DONE",
                "truncated_fields": ["made_up_future_field"]
            }),
        )],
    );
    let publication = extract_session_facts(&unknown_field, "antigravity");
    let text_inventory = coverage(&publication, "text_inventory").expect("text_inventory");
    assert_eq!(text_inventory.state, "partial", "{text_inventory:?}");
    assert_eq!(
        text_inventory.reasons,
        vec!["agy_source_truncated".to_string()],
        "{text_inventory:?}"
    );
    let chat = coverage(&publication, "chat").expect("chat");
    assert_eq!(chat.state, "complete", "{chat:?}");

    // `media` carries no text: a media truncation marker marks nothing.
    let media_only = packet(
        "antigravity",
        json!({"source": "antigravity"}),
        vec![msg(
            0,
            "user",
            "made-up request",
            json!({
                "status": "DONE",
                "truncated_fields": ["media"]
            }),
        )],
    );
    let publication = extract_session_facts(&media_only, "antigravity");
    for field in ["chat", "text_inventory", "thinking", "files"] {
        let entry = coverage(&publication, field).unwrap_or_else(|| panic!("{field}"));
        assert_eq!(entry.state, "complete", "{field}: {entry:?}");
        assert!(entry.reasons.is_empty(), "{field}: {entry:?}");
    }

    // A marker that fails to parse is NOT provable source truncation: a
    // separate not-read reason, never the allowlisted one.
    let malformed = packet(
        "antigravity",
        json!({"source": "antigravity"}),
        vec![
            msg(
                0,
                "user",
                "made-up request",
                json!({
                    "status": "DONE",
                    "truncated_fields": "content"
                }),
            ),
            msg(
                1,
                "assistant",
                "made-up narration",
                json!({
                    "status": "DONE",
                    "truncated_fields": ["content", 7]
                }),
            ),
        ],
    );
    let publication = extract_session_facts(&malformed, "antigravity");
    let text_inventory = coverage(&publication, "text_inventory").expect("text_inventory");
    assert_eq!(text_inventory.state, "partial", "{text_inventory:?}");
    assert!(
        text_inventory
            .reasons
            .iter()
            .any(|r| r == "agy_truncated_fields_malformed"),
        "{text_inventory:?}"
    );
    assert!(
        !text_inventory
            .reasons
            .iter()
            .any(|r| r == "agy_source_truncated"),
        "{text_inventory:?}"
    );
    assert!(!is_source_absent_gap(
        text_inventory.state.as_str(),
        &["agy_truncated_fields_malformed".to_string()]
    ));
}

#[test]
fn antigravity_f10_error_only_and_history_text_reach_chat_regions() {
    // F10 emits error-only ERROR_MESSAGE records (the `error` text as the
    // message content) and text-bearing CONVERSATION_HISTORY records: both
    // are known shapes, and their content is indexed as chat-scope region
    // text instead of vanishing with the record.
    let p = packet(
        "antigravity",
        json!({"source": "antigravity"}),
        vec![
            msg(0, "user", "made-up request", json!({"status": "DONE"})),
            msg(
                1,
                "system",
                "made-up widget pipeline failure",
                json!({
                    "agy_type": "ERROR_MESSAGE", "agy_source": "SYSTEM",
                    "error": "made-up widget pipeline failure", "status": "DONE"
                }),
            ),
            msg(
                2,
                "system",
                "made-up folded history note",
                json!({
                    "agy_type": "CONVERSATION_HISTORY", "agy_source": "SYSTEM",
                    "status": "DONE"
                }),
            ),
        ],
    );
    let facts = extract_session_facts(&p, "antigravity");
    assert!(
        !facts
            .coverage
            .iter()
            .any(|entry| entry.reasons.iter().any(|r| r == "agy_record_unknown")),
        "coverage: {:?}",
        facts.coverage
    );
    // The two records carry text the packet can fully account for: no
    // text-scope mark at all.
    for field in ["chat", "text_inventory"] {
        let entry = coverage(&facts, field).unwrap_or_else(|| panic!("{field}"));
        assert_eq!(entry.state, "complete", "{field}: {entry:?}");
        assert!(entry.reasons.is_empty(), "{field}: {entry:?}");
    }

    // Region derivation: each record's content lands as a chat-scope
    // region bound to its message (the m{idx}:chat region's document IS
    // the message), so the error text and the history text are searchable
    // chat text.
    let structure = project_session_structure(&p, &facts);
    let regions = derive_message_regions(1, &p, &structure);
    for idx in [1, 2] {
        let region = regions
            .iter()
            .find(|r| r.region_key == format!("m{idx}:chat"))
            .unwrap_or_else(|| panic!("m{idx}:chat region missing: {regions:?}"));
        assert_eq!(region.scope, "chat", "{region:?}");
        assert_eq!(region.message_idx, Some(idx), "{region:?}");
    }

    // The error-only record carries the error text AS its content — the
    // chat region IS its index. No separate error region may double-index
    // the same text: the derivation emits an error region only when the
    // string differs from the content.
    assert!(
        !regions.iter().any(|r| r.region_key == "m1:error"),
        "error == content must not grow a second region: {regions:?}"
    );
}

#[test]
fn antigravity_f10_error_field_indexes_as_searchable_regions() {
    // The `error` passthrough on a record whose message content is NOT the
    // same text: the failure text is packet-visible AND local, so it
    // indexes as its own searchable region instead of keeping the retired
    // `agy_error_field_not_indexed` not-read gap. Scope follows where the
    // record's own text indexes for every provider: a tool-role MODEL
    // step is an executed step, so its failure text is tool error text —
    // tools scope with the recorded error flags, exactly like a
    // tool_result carrying `is_error`; a system ERROR_MESSAGE is message
    // text the connector itself emits as chat content for error-only
    // records, so its differing error text stays chat. Error-only
    // ERROR_MESSAGE records carry the text AS their content (the
    // previous test pins that they never double-index).
    let messages = || {
        vec![
            msg(0, "user", "made-up request", json!({"status": "DONE"})),
            msg(
                1,
                "tool",
                "made-up step narration content",
                json!({
                    "agy_type": "GENERIC", "status": "ERROR",
                    "error": "made-up gyrocopter failure detail"
                }),
            ),
            msg(
                2,
                "system",
                "made-up widget failure summary",
                json!({
                    "agy_type": "ERROR_MESSAGE", "agy_source": "SYSTEM",
                    "status": "ERROR",
                    "error": "made-up wobbledrive failure detail"
                }),
            ),
        ]
    };
    let p = packet("antigravity", json!({"source": "antigravity"}), messages());
    let publication = extract_session_facts(&p, "antigravity");
    assert!(
        !publication
            .coverage
            .iter()
            .any(|entry| entry.reasons.iter().any(|r| r == "agy_record_unknown")),
        "coverage: {:?}",
        publication.coverage
    );
    // The error text is indexed: no coverage entry keeps the retired
    // reason, and the text scopes certify complete — no not-read gap
    // from the error field.
    assert!(
        !publication.coverage.iter().any(|entry| entry
            .reasons
            .iter()
            .any(|r| r == "agy_error_field_not_indexed")),
        "the error-field mark is retired: {:?}",
        publication.coverage
    );
    for field in ["chat", "text_inventory"] {
        let entry = coverage(&publication, field).unwrap_or_else(|| panic!("{field}"));
        assert_eq!(entry.state, "complete", "{field}: {entry:?}");
        assert!(entry.reasons.is_empty(), "{field}: {entry:?}");
    }

    // Region derivation: each differing error string becomes its own
    // region — the tool-step failure text under the tools scope with the
    // recorded error flags, the system announcement's detail as chat.
    let structure = project_session_structure(&p, &publication);
    let regions = derive_message_regions(1, &p, &structure);
    let tool_error = regions
        .iter()
        .find(|r| r.region_key == "m1:error")
        .unwrap_or_else(|| panic!("m1:error region missing: {regions:?}"));
    assert_eq!(tool_error.scope, "tools", "{tool_error:?}");
    assert_eq!(tool_error.message_idx, Some(1), "{tool_error:?}");
    assert_eq!(tool_error.has_error, Some(true), "{tool_error:?}");
    assert_eq!(
        tool_error.error_type.as_deref(),
        Some("tool"),
        "{tool_error:?}"
    );
    assert!(
        tool_error
            .indexed_text
            .as_deref()
            .is_some_and(|text| text.contains("gyrocopter")),
        "{tool_error:?}"
    );
    let chat_error = regions
        .iter()
        .find(|r| r.region_key == "m2:error")
        .unwrap_or_else(|| panic!("m2:error region missing: {regions:?}"));
    assert_eq!(chat_error.scope, "chat", "{chat_error:?}");
    assert_eq!(chat_error.has_error, None, "{chat_error:?}");
    assert_eq!(chat_error.error_type, None, "{chat_error:?}");
    assert!(
        chat_error
            .indexed_text
            .as_deref()
            .is_some_and(|text| text.contains("wobbledrive")),
        "{chat_error:?}"
    );
    // A record without an error field grows no error region.
    assert!(
        !regions.iter().any(|r| r.region_key == "m0:error"),
        "no error field on the user record: {regions:?}"
    );

    // The provider gate: the same shapes under a foreign slug must not
    // grow error regions (the F10 passthrough contract is
    // antigravity's; other providers' `error` keys are not it).
    let foreign = packet("pi_agent", json!({"source": "antigravity"}), messages());
    let foreign_facts = extract_session_facts(&foreign, "pi_agent");
    let foreign_structure = project_session_structure(&foreign, &foreign_facts);
    let foreign_regions = derive_message_regions(1, &foreign, &foreign_structure);
    assert!(
        !foreign_regions
            .iter()
            .any(|r| r.region_key.ends_with(":error")),
        "foreign slug must not derive error regions: {foreign_regions:?}"
    );
}

#[test]
fn antigravity_f10_media_refs_stay_known_without_text_marks() {
    // `media` attachment references ({mime_type, uri}) ride on USER_INPUT
    // and MODEL-step records. Attachments carry no text: media must not
    // degrade any text scope, and the records must classify as known
    // shapes (the key is part of the F10 contract, not drift).
    let p = packet(
        "antigravity",
        json!({"source": "antigravity"}),
        vec![
            msg(
                0,
                "user",
                "made-up request with attachment",
                json!({
                    "status": "DONE",
                    "media": [
                        {"mime_type": "image/png", "uri": "file:///tmp/made-up-doodle.png"}
                    ]
                }),
            ),
            msg(
                1,
                "tool",
                "made-up step output",
                json!({
                    "agy_type": "GENERIC", "status": "DONE",
                    "media": [
                        {"mime_type": "image/png", "uri": "file:///tmp/made-up-render.png"}
                    ]
                }),
            ),
        ],
    );
    let publication = extract_session_facts(&p, "antigravity");
    assert!(
        !publication
            .coverage
            .iter()
            .any(|entry| entry.reasons.iter().any(|r| r == "agy_record_unknown")),
        "coverage: {:?}",
        publication.coverage
    );
    for field in ["chat", "thinking", "text_inventory", "files"] {
        let entry = coverage(&publication, field).unwrap_or_else(|| panic!("{field}"));
        assert_eq!(entry.state, "complete", "{field}: {entry:?}");
        assert!(entry.reasons.is_empty(), "{field}: {entry:?}");
    }
}

#[test]
fn antigravity_f10_status_done_records_success_for_paired_results() {
    // A raw-shape packet whose TOOL_KINDS record carries call identity
    // (the oracle-port arm the extractor keeps for raw extras; the pinned
    // connector itself never records call ids, so real packets stay
    // unpaired — the clean test above pins that verdict). Paired with its
    // planner call, the lifecycle status becomes recorded exit/error
    // evidence: DONE is the recorded success verdict — an explicit error
    // flag, never a fabricated exit code — so the error scope certifies
    // complete while the exit-code scope honestly reports nothing
    // recorded.
    let p = packet(
        "antigravity",
        json!({"source": "antigravity"}),
        vec![
            msg(0, "user", "made-up request", json!({"status": "DONE"})),
            msg(
                1,
                "assistant",
                "made-up narration",
                json!({
                    "cass": {"invocations": [
                        {"kind": "tool", "name": "view_file", "call_id": "call_made_up_1",
                         "arguments": {"TargetFile": "/made/up/status-target.rs"}}
                    ]},
                    "status": "DONE"
                }),
            ),
            msg(
                2,
                "tool",
                "made-up file body",
                json!({
                    "agy_type": "VIEW_FILE", "call_id": "call_made_up_1",
                    "content": "made-up file body", "status": "DONE"
                }),
            ),
        ],
    );
    let publication = extract_session_facts(&p, "antigravity");

    let view = publication
        .tools
        .iter()
        .find(|t| t.call_id == "call_made_up_1")
        .expect("call_made_up_1");
    assert!(view.has_result);
    assert_eq!(view.is_error, Some(false));
    assert_eq!(view.exit_code, None, "no exit code is ever invented");
    assert_eq!(view.file_path.as_deref(), Some("/made/up/status-target.rs"));

    // Paired status is recorded error evidence: the error scope completes
    // and neither connector-drop nor unpaired verdict appears.
    let error = coverage(&publication, "tool_error").expect("tool_error");
    assert_eq!(error.state, "complete", "{error:?}");
    assert!(
        !error
            .reasons
            .iter()
            .any(|r| r == "agy_status_unpaired" || r == "agy_status_dropped_by_connector"),
        "{error:?}"
    );
    // The format records no numeric exit codes for the step: the exit
    // scope honestly stays not-recorded (collector convention).
    let exit = coverage(&publication, "tool_exit").expect("tool_exit");
    assert_eq!(exit.state, "unavailable", "{exit:?}");
    assert!(
        exit.reasons.iter().any(|r| r == "tool_exit_not_recorded"),
        "{exit:?}"
    );
    // The fully paired call set certifies the tools scope.
    let tools = coverage(&publication, "tools").expect("tools");
    assert_eq!(tools.state, "complete", "{tools:?}");
}

#[test]
fn antigravity_regions_index_planner_arguments_and_user_wrappers() {
    let packet = antigravity_real_shape_packet();
    let facts = extract_session_facts(&packet, "antigravity");
    let structure = project_session_structure(&packet, &facts);
    let regions = derive_message_regions(1, &packet, &structure);

    // Planner invocation arguments are packet text that must reach the
    // tools scope as searchable regions (the connector records no call
    // ids, so the region set is the only place the arguments live).
    let tools_regions: Vec<_> = regions.iter().filter(|r| r.scope == "tools").collect();
    let view = tools_regions
        .iter()
        .find(|r| r.tool_name.as_deref() == Some("view_file"))
        .expect("view_file call region");
    assert!(
        view.indexed_text
            .as_deref()
            .is_some_and(|text| text.contains("/made/up/target.rs")),
        "view_file region: {view:?}"
    );
    assert_eq!(view.file_path.as_deref(), Some("/made/up/target.rs"));
    let run = tools_regions
        .iter()
        .find(|r| r.tool_name.as_deref() == Some("run_command"))
        .expect("run_command call region");
    assert!(
        run.indexed_text
            .as_deref()
            .is_some_and(|text| text.contains("made-up-run --flag")),
        "run_command region: {run:?}"
    );

    // The MODEL-step fallback keeps its content in the tools scope.
    let fallback = tools_regions
        .iter()
        .find(|r| r.region_key.ends_with(":role:2"))
        .expect("role fallback region");
    assert!(
        fallback
            .indexed_text
            .as_deref()
            .is_some_and(|text| text.contains("made-up model step output")),
        "fallback region: {fallback:?}"
    );

    // USER_INPUT wrapper blocks the connector lifted to extra are user
    // turn text: chat-scope regions, deterministically keyed.
    for (key, needle) in [
        ("m0:user_settings_change", "made-up settings change text"),
        (
            "m0:user_additional_metadata",
            "made-up additional metadata text",
        ),
    ] {
        let region = regions
            .iter()
            .find(|r| r.region_key == key)
            .unwrap_or_else(|| panic!("{key} missing: {regions:?}"));
        assert_eq!(region.scope, "chat", "{region:?}");
        assert!(
            region
                .indexed_text
                .as_deref()
                .is_some_and(|text| text.contains(needle)),
            "{region:?}"
        );
    }

    // The provider gate: the same message shapes under a foreign slug must
    // not grow antigravity regions (no invocation regions, no wrappers).
    let foreign = antigravity_real_shape_packet_with_slug("pi_agent");
    let foreign_facts = extract_session_facts(&foreign, "pi_agent");
    let foreign_structure = project_session_structure(&foreign, &foreign_facts);
    let foreign_regions = derive_message_regions(1, &foreign, &foreign_structure);
    assert!(
        !foreign_regions
            .iter()
            .any(|r| r.tool_name.as_deref() == Some("view_file")
                || r.tool_name.as_deref() == Some("run_command")),
        "foreign slug must not derive invocation regions: {foreign_regions:?}"
    );
    assert!(
        !foreign_regions
            .iter()
            .any(|r| r.region_key.contains("user_settings_change")
                || r.region_key.contains("user_additional_metadata")),
        "foreign slug must not derive wrapper regions: {foreign_regions:?}"
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
// conflicting callIds never merged; the trailing empty-extra message is a
// no-record shape (see the no-record tests below).
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
// Cursor no-record shapes: the pinned connector persists `extra = {}` for
// agent-transcript messages and composer draft fallback messages — no raw
// record — while delivering every text-bearing field into `content` and
// lifting every tool_use block into `invocations` (archived by the ingest
// merge under `cass.invocations`). Those events' text/block inventories
// are complete by construction; only thinking stays unverifiable (a
// dropped thinking block leaves no archive evidence). Unknown shapes — an
// empty extra WITHOUT content, or a non-object extra — keep the full
// four-scope mark.
// ---------------------------------------------------------------------------

#[test]
fn cursor_facts_no_record_shapes_text_inventory_complete() {
    let publication = extract_session_facts(
        &packet(
            "cursor",
            json!({"source": "cursor"}),
            vec![
                // Composer draft fallback: one user message, no bubble record.
                msg(
                    0,
                    "user",
                    "synthetic draft prompt about wiring tests",
                    json!({}),
                ),
                // Agent-transcript text message: flattened record text, no raw record.
                msg(1, "assistant", "synthetic transcript reply", json!({})),
                // Agent-transcript tool message: invocation archived under
                // cass.invocations, flattened rendering in content.
                msg(
                    2,
                    "assistant",
                    "[Tool: read_file - /tmp/synthetic.txt]",
                    json!({"cass": {"invocations": [
                        {"kind": "tool", "call_id": "call_synth_1", "name": "read_file",
                         "input": {"file_path": "/tmp/synthetic.txt"}}
                    ]}}),
                ),
            ],
        ),
        "cursor",
    );

    // The delivered text of every no-record event is fully in `content`,
    // and the projection derives its block inventory from content: the
    // text and block inventories are complete.
    for field in ["text_inventory", "block_inventory"] {
        let entry = coverage(&publication, field).expect(field);
        assert_eq!(entry.state, "complete", "{field}: {entry:?}");
    }

    // Thinking stays honestly unverifiable: the absent record could have
    // carried thinking blocks — per-event partial, session-level
    // unavailable, reason naming the absent record.
    let thinking = coverage(&publication, "thinking").expect("thinking");
    assert_eq!(thinking.state, "unavailable");
    assert!(
        thinking.reasons.iter().any(|r| r == "cursor_extra_absent"),
        "thinking reasons: {:?}",
        thinking.reasons
    );

    // No recorded tool records exist for these events (a `{}` extra means
    // the connector lifted no invocations; the cass-merged invocation is
    // archived structure, not recorded call/result evidence): tools is
    // complete the way a session without tools is.
    let tools = coverage(&publication, "tools").expect("tools");
    assert_eq!(tools.state, "complete", "tools: {tools:?}");
}

#[test]
fn cursor_facts_unknown_shapes_stay_fully_marked() {
    let publication = extract_session_facts(
        &packet(
            "cursor",
            json!({"source": "cursor"}),
            vec![
                // Not a known connector shape: every `{}` producer the
                // pinned connector has delivers non-empty content.
                msg(0, "assistant", "", json!({})),
                // Non-object extra: unknown entirely.
                msg(1, "assistant", "text beside a null extra", Value::Null),
            ],
        ),
        "cursor",
    );
    for field in ["tools", "block_inventory", "text_inventory"] {
        let entry = coverage(&publication, field).expect(field);
        assert_eq!(entry.state, "partial", "{field}: {entry:?}");
        assert!(
            entry.reasons.iter().any(|r| r == "cursor_extra_absent"),
            "{field}: {entry:?}"
        );
    }
    // Thinking still carries the per-event absent-record mark (visible in
    // the reason set) under the session-level unverifiable-thinking state.
    let thinking = coverage(&publication, "thinking").expect("thinking");
    assert_eq!(thinking.state, "unavailable", "thinking: {thinking:?}");
    assert!(
        thinking.reasons.iter().any(|r| r == "cursor_extra_absent"),
        "thinking: {thinking:?}"
    );
}

// ---------------------------------------------------------------------------
// Gemini facts: the pinned FAD connector archives the raw gemini record
// verbatim in `extra` (content parts, `thoughts`, `toolCalls` with inline
// results) and inlines every text-bearing field into the message content —
// tool args are the one text class that stays out of `content`, and they
// index as tool regions (search_regions.rs, REGION_VERSION 3). Real key
// structure from the pinned connector; synthetic text only.
// ---------------------------------------------------------------------------

fn gemini_messages() -> Vec<NormalizedMessage> {
    vec![
            // User record: raw content parts, all text delivered.
            msg(
                0,
                "user",
                "made-up prompt about widget calibration",
                json!({
                    "type": "user", "id": "u1",
                    "timestamp": "2026-01-01T00:00:00Z",
                    "content": [
                        {"type": "text", "text": "made-up prompt about widget calibration"}
                    ]
                }),
            ),
            // Assistant record: thoughts + toolCalls with inline results
            // (the connector renders `[Thinking] …`/`[Tool: …]`/
            // `[Tool result: …]` into the content it delivers).
            msg(
                1,
                "agent",
                "[Thinking] made-up thought subject made-up thought description\n\
                 [Tool: glob_widget_files]\n\
                 [Tool result: glob_widget_files] made-up glob output\n\
                 [Tool: write_widget]\n\
                 [Tool result: write_widget] [Error] made-up write failure",
                json!({
                    "type": "gemini", "id": "m1",
                    "timestamp": "2026-01-01T00:00:01Z",
                    "model": "made-up-gemini-model",
                    "tokens": {"input": 120, "output": 60},
                    "thoughts": [
                        {"subject": "made-up thought subject",
                         "description": "made-up thought description",
                         "timestamp": "2026-01-01T00:00:01Z"}
                    ],
                    "toolCalls": [
                        {"id": "tc1", "name": "glob_widget_files",
                         "args": {"pattern": "**/*.widget"},
                         "status": "success",
                         "timestamp": "2026-01-01T00:00:01Z",
                         "description": "made-up description template",
                         "displayName": "made-up display name",
                         "resultDisplay": "made-up rendered display",
                         "renderOutputAsMarkdown": true,
                         "result": [
                             {"functionResponse": {"name": "glob_widget_files",
                                                   "response": {"output": "made-up glob output"}}}
                         ]},
                        {"id": "tc2", "name": "write_widget",
                         "args": {"file_path": "/tmp/synthetic/widget.md",
                                  "content": "made-up file body"},
                         "status": "error",
                         "timestamp": "2026-01-01T00:00:02Z",
                         "result": [
                             {"functionResponse": {"response":
                                 {"error": "made-up write failure"}}}
                         ]}
                    ],
                    "cass": {"invocations": [
                        {"kind": "tool", "name": "glob_widget_files", "call_id": "tc1",
                         "arguments": {"pattern": "**/*.widget"}},
                        {"kind": "tool", "name": "write_widget", "call_id": "tc2",
                         "arguments": {"file_path": "/tmp/synthetic/widget.md",
                                        "content": "made-up file body"}}
                    ]}
                }),
            ),
    ]
}

fn gemini_packet() -> ConversationPacket {
    packet("gemini", json!({"source": "gemini"}), gemini_messages())
}

#[test]
fn gemini_facts_raw_records_tools_thinking_and_coverage() {
    let publication = extract_session_facts(&gemini_packet(), "gemini");

    // Recorded calls pair with their inline results by call id.
    let glob = publication
        .tools
        .iter()
        .find(|t| t.call_id == "tc1")
        .expect("tc1");
    assert_eq!(glob.name, "glob_widget_files");
    assert!(glob.has_result);
    // status == "success" is the recorded success verdict; never an exit code.
    assert_eq!(glob.is_error, Some(false));
    assert_eq!(glob.exit_code, None);
    assert_eq!(glob.file_path, None);

    let write = publication
        .tools
        .iter()
        .find(|t| t.call_id == "tc2")
        .expect("tc2");
    assert_eq!(write.name, "write_widget");
    assert!(write.has_result);
    // response.error forces the error verdict.
    assert_eq!(write.is_error, Some(true));
    assert_eq!(
        write.file_path.as_deref(),
        Some("/tmp/synthetic/widget.md")
    );

    assert_eq!(publication.tools.len(), 2);
    assert_eq!(publication.tool_error_count, Some(1));
    assert_eq!(publication.tool_calls_with_exit_status, Some(0));
    assert_eq!(publication.file_fact_count, Some(1));
    assert_eq!(publication.thinking_block_count, Some(1));
    assert_eq!(publication.model_set, vec!["made-up-gemini-model"]);
    assert_eq!(publication.first_prompt.as_deref(),
               Some("made-up prompt about widget calibration"));
    assert_eq!(publication.turn_count, Some(1));
    assert_eq!(publication.step_count, Some(1));
    // The pinned gemini connector normalizes no compaction boundary records.
    assert_eq!(publication.compaction_count, None);

    // The raw record is archived verbatim, so the delivered text scopes
    // certify complete: content parts, thoughts and tool results are all
    // recorded, and the tool args index as regions (test below).
    for field in ["chat", "tools", "thinking", "tool_error", "files",
                  "block_inventory", "text_inventory"] {
        let entry = coverage(&publication, field)
            .unwrap_or_else(|| panic!("{field}"));
        assert_eq!(entry.state, "complete", "{field}: {entry:?}");
        assert!(entry.reasons.is_empty(), "{field}: {entry:?}");
    }
    // Gemini records explicit status/error verdicts but never exit codes.
    let exit = coverage(&publication, "tool_exit").expect("tool_exit");
    assert_eq!(exit.state, "unavailable");
    assert!(
        exit.reasons.iter().any(|r| r == "tool_exit_not_recorded"),
        "tool_exit: {exit:?}"
    );
    // Boundary records are not normalized by this connector.
    let compaction = coverage(&publication, "compaction").expect("compaction");
    assert_eq!(compaction.state, "unavailable");
    assert!(
        compaction
            .reasons
            .iter()
            .any(|r| r == "boundary_records_not_normalized"),
        "compaction: {compaction:?}"
    );
}

#[test]
fn gemini_facts_unpaired_call_keeps_text_inventory_complete() {
    // A raw toolCalls entry with no `result` key: the record is archived
    // verbatim, so the absence proves no result ever existed at the source
    // (an interrupted call) — no session text is missing.
    let publication = extract_session_facts(
        &packet(
            "gemini",
            json!({"source": "gemini"}),
            vec![
                msg(0, "user", "made-up prompt", json!({
                    "type": "user", "id": "u1", "timestamp": "t0",
                    "content": [{"type": "text", "text": "made-up prompt"}]
                })),
                msg(1, "agent", "[Tool: read_widget]", json!({
                    "type": "gemini", "id": "m1", "timestamp": "t1",
                    "toolCalls": [
                        {"id": "tc9", "name": "read_widget",
                         "args": {"file_path": "/tmp/synthetic/w.txt"},
                         "status": "success", "timestamp": "t1"}
                    ]
                })),
            ],
        ),
        "gemini",
    );
    let read = publication
        .tools
        .iter()
        .find(|t| t.call_id == "tc9")
        .expect("tc9");
    assert!(!read.has_result);
    let tools = coverage(&publication, "tools").expect("tools");
    assert_eq!(tools.state, "partial");
    assert!(
        tools.reasons.iter().any(|r| r == "tool_result_missing"),
        "tools: {tools:?}"
    );
    let text = coverage(&publication, "text_inventory")
        .expect("text_inventory");
    assert_eq!(text.state, "complete", "text_inventory: {text:?}");
}

#[test]
fn gemini_facts_compacted_extra_and_drift_shapes() {
    // compact_message_extra (>=32 MiB session files) replaces the raw record
    // with {model, tokens, attachments} before the ingest seam archives the
    // connector's invocations under cass.invocations: evidence survives,
    // the raw record does not — partial, not unknown.
    let compacted = extract_session_facts(
        &packet(
            "gemini",
            json!({"source": "gemini"}),
            vec![
                msg(0, "agent", "[Tool: read_widget]", json!({
                    "model": "made-up-gemini-model",
                    "tokens": {"input": 10, "output": 5},
                    "attachments": [{"path": "/tmp/synthetic/attach.bin"}],
                    "cass": {"invocations": [
                        {"kind": "tool", "name": "read_widget", "call_id": "tc7",
                         "arguments": {"file_path": "/tmp/synthetic/w.txt"}}
                    ]}
                })),
            ],
        ),
        "gemini",
    );
    let call = compacted
        .tools
        .iter()
        .find(|t| t.call_id == "tc7")
        .expect("tc7 from the archived invocation");
    assert_eq!(call.name, "read_widget");
    let tools = coverage(&compacted, "tools").expect("tools");
    assert_eq!(tools.state, "partial");
    assert!(
        tools.reasons.iter().any(|r| r == "extra_compacted"),
        "tools: {tools:?}"
    );
    assert!(
        compacted.model_set == vec!["made-up-gemini-model"],
        "models: {:?}",
        compacted.model_set
    );

    // A record with keys outside the connector's verified contract is
    // drift, never an unknown-shape guess.
    let drift = extract_session_facts(
        &packet(
            "gemini",
            json!({"source": "gemini"}),
            vec![
                msg(0, "user", "made-up prompt", json!({
                    "type": "user", "id": "u1", "timestamp": "t0",
                    "futureField": {"opaque": true},
                    "content": [{"type": "text", "text": "made-up prompt"}]
                })),
            ],
        ),
        "gemini",
    );
    let entry = coverage(&drift, "text_inventory").expect("text_inventory");
    assert_eq!(entry.state, "partial");
    assert!(
        entry.reasons.iter().any(|r| r == "gemini_record_unknown"),
        "text_inventory: {entry:?}"
    );

    // A non-object extra is unknown entirely.
    let missing = extract_session_facts(
        &packet(
            "gemini",
            json!({"source": "gemini"}),
            vec![msg(0, "user", "made-up text beside a null extra", Value::Null)],
        ),
        "gemini",
    );
    let entry = coverage(&missing, "chat").expect("chat");
    assert_eq!(entry.state, "unavailable");
    assert!(
        entry.reasons.iter().any(|r| r == "gemini_extra_missing"),
        "chat: {entry:?}"
    );
}

#[test]
fn gemini_regions_index_toolcall_args_and_thoughts() {
    let p = gemini_packet();
    let publication = extract_session_facts(&p, "gemini");
    let structure = project_session_structure(&p, &publication);
    let regions = derive_message_regions(1, &p, &structure);

    // Tool args are the one text class the connector keeps out of content:
    // each raw toolCall indexes as a tools region carrying its args and its
    // inline result text, with the recorded error verdicts.
    let glob = regions
        .iter()
        .find(|r| r.region_key == "tool:0:tc1")
        .unwrap_or_else(|| panic!("tc1 tool region missing: {regions:?}"));
    assert_eq!(glob.scope, "tools");
    assert_eq!(glob.tool_name.as_deref(), Some("glob_widget_files"));
    assert_eq!(glob.has_error, Some(false), "{glob:?}");
    assert!(
        glob.indexed_text
            .as_deref()
            .is_some_and(|text| text.contains("**/*.widget")),
        "call args text: {glob:?}"
    );
    assert!(
        glob.indexed_text
            .as_deref()
            .is_some_and(|text| text.contains("made-up glob output")),
        "result text: {glob:?}"
    );

    let write = regions
        .iter()
        .find(|r| r.region_key == "tool:1:tc2")
        .unwrap_or_else(|| panic!("tc2 tool region missing: {regions:?}"));
    assert_eq!(write.tool_name.as_deref(), Some("write_widget"));
    assert_eq!(write.has_error, Some(true), "{write:?}");
    assert_eq!(
        write.file_path.as_deref(),
        Some("/tmp/synthetic/widget.md")
    );
    assert!(
        write.indexed_text
            .as_deref()
            .is_some_and(|text| text.contains("made-up write failure")),
        "error text: {write:?}"
    );

    // Thoughts index as a thinking region (the generic reader cannot see
    // them: they are neither content blocks nor an extra string).
    let thinking = regions
        .iter()
        .find(|r| r.region_key == "m1:thinking")
        .unwrap_or_else(|| panic!("m1:thinking region missing: {regions:?}"));
    assert_eq!(thinking.scope, "thinking");
    assert!(
        thinking.indexed_text
            .as_deref()
            .is_some_and(|text| text.contains("made-up thought subject")
                && text.contains("made-up thought description")),
        "{thinking:?}"
    );

    // The provider gate: the same shapes under a foreign slug must not
    // grow gemini tool regions (the raw-toolCalls contract is gemini's).
    // `junie` stays a stub extractor, so this exercises the region gate
    // alone.
    let foreign = packet("junie", json!({"source": "gemini"}), gemini_messages());
    let foreign_facts = extract_session_facts(&foreign, "junie");
    let foreign_structure = project_session_structure(&foreign, &foreign_facts);
    let foreign_regions = derive_message_regions(1, &foreign, &foreign_structure);
    assert!(
        !foreign_regions
            .iter()
            .any(|r| r.tool_name.as_deref() == Some("glob_widget_files")),
        "foreign slug must not derive gemini tool regions: {foreign_regions:?}"
    );
}

// ---------------------------------------------------------------------------
// Pi Agent facts: the pinned FAD connector archives every emitted tree
// entry verbatim in `extra` and inserts a `cass` provenance object
// ({entry_kind, source_role, entry_id, parent_id, source_line,
// on_active_branch, in_active_context, model?, provider?, invocations?}).
// Message content blocks are flattened into the delivered content
// (`[Thinking] …`/`[Tool: name] key=value…`; tool args stay out of the
// block facts and index as regions). Real key structure from the pinned
// connector; synthetic text only.
// ---------------------------------------------------------------------------

fn pi_messages() -> Vec<NormalizedMessage> {
    vec![
        // User entry: bare-string message content (simple user messages).
        msg(
            0,
            "user",
            "made-up pi prompt about widget calibration",
            json!({
                "type": "message", "id": "e1", "parentId": null,
                "timestamp": "2026-01-02T00:00:00Z",
                "message": {
                    "role": "user", "timestamp": "2026-01-02T00:00:00Z",
                    "content": "made-up pi prompt about widget calibration"
                },
                "cass": {
                    "entry_kind": "message", "source_role": "user",
                    "entry_id": "e1", "parent_id": null, "source_line": 1,
                    "on_active_branch": true, "in_active_context": true
                }
            }),
        ),
        // Assistant entry: text + thinking + toolCall blocks (the connector
        // flattens all three into the delivered content and lifts the
        // toolCall into cass.invocations).
        msg(
            1,
            "agent",
            "made-up pi answer\n[Thinking] made-up pi reasoning\n\
             [Tool: read_pi_widget] file_path=/tmp/synthetic/pi.txt",
            json!({
                "type": "message", "id": "e2", "parentId": "e1",
                "timestamp": "2026-01-02T00:00:01Z",
                "message": {
                    "role": "assistant", "model": "made-up-pi-model",
                    "provider": "made-up-pi-provider",
                    "timestamp": "2026-01-02T00:00:01Z",
                    "api": "made-up-api", "responseId": "made-up-resp-1",
                    "stopReason": "made-up-stop", "rawStopReason": "made-up-raw-stop",
                    "usage": {"input": 30, "output": 12},
                    "content": [
                        {"type": "text", "text": "made-up pi answer"},
                        {"type": "thinking", "thinking": "made-up pi reasoning",
                         "thinkingSignature": "made-up-signature"},
                        {"type": "toolCall", "id": "pc1", "name": "read_pi_widget",
                         "arguments": {"file_path": "/tmp/synthetic/pi.txt"}}
                    ]
                },
                "cass": {
                    "entry_kind": "message", "source_role": "assistant",
                    "entry_id": "e2", "parent_id": "e1", "source_line": 2,
                    "on_active_branch": true, "in_active_context": true,
                    "model": "made-up-pi-model", "provider": "made-up-pi-provider",
                    "invocations": [
                        {"kind": "tool", "name": "read_pi_widget", "call_id": "pc1",
                         "arguments": {"file_path": "/tmp/synthetic/pi.txt"}}
                    ]
                }
            }),
        ),
        // Tool result entry: raw role `toolResult` maps to packet role
        // `tool`; the message carries the correlation id and error flag.
        msg(
            2,
            "tool",
            "made-up pi tool output",
            json!({
                "type": "message", "id": "e3", "parentId": "e2",
                "timestamp": "2026-01-02T00:00:02Z",
                "message": {
                    "role": "toolResult", "timestamp": "2026-01-02T00:00:02Z",
                    "content": "made-up pi tool output",
                    "isError": false,
                    "toolCallId": "pc1", "toolName": "read_pi_widget"
                },
                "cass": {
                    "entry_kind": "message", "source_role": "toolResult",
                    "entry_id": "e3", "parent_id": "e2", "source_line": 3,
                    "on_active_branch": true, "in_active_context": true
                }
            }),
        ),
    ]
}

#[test]
fn pi_agent_facts_raw_entries_tools_thinking_and_coverage() {
    let publication = extract_session_facts(
        &packet("pi_agent", json!({"source": "pi_agent"}), pi_messages()),
        "pi_agent",
    );

    // The toolCall block pairs with its toolResult entry by toolCallId.
    let read = publication
        .tools
        .iter()
        .find(|t| t.call_id == "pc1")
        .expect("pc1");
    assert_eq!(read.name, "read_pi_widget");
    assert!(read.has_result);
    // `isError` is the recorded verdict; pi records no exit codes.
    assert_eq!(read.is_error, Some(false));
    assert_eq!(read.exit_code, None);
    assert_eq!(
        read.file_path.as_deref(),
        Some("/tmp/synthetic/pi.txt")
    );

    assert_eq!(publication.tools.len(), 1);
    assert_eq!(publication.tool_error_count, Some(0));
    assert_eq!(publication.tool_calls_with_exit_status, Some(0));
    assert_eq!(publication.file_fact_count, Some(1));
    assert_eq!(publication.thinking_block_count, Some(1));
    assert_eq!(publication.model_set, vec!["made-up-pi-model"]);
    assert_eq!(
        publication.first_prompt.as_deref(),
        Some("made-up pi prompt about widget calibration")
    );
    assert_eq!(publication.turn_count, Some(1));
    assert_eq!(publication.step_count, Some(2));
    // This session has no compaction entries, and a zero count cannot
    // certify absence (the connector drops empty-summary compactions).
    assert_eq!(publication.compaction_count, None);

    // Every emitted entry is archived verbatim with enumerable blocks, so
    // the delivered-text scopes certify complete.
    for field in ["chat", "tools", "thinking", "tool_error", "files",
                  "block_inventory", "text_inventory"] {
        let entry = coverage(&publication, field)
            .unwrap_or_else(|| panic!("{field}"));
        assert_eq!(entry.state, "complete", "{field}: {entry:?}");
        assert!(entry.reasons.is_empty(), "{field}: {entry:?}");
    }
    // Pi records error verdicts (isError) but never exit codes.
    let exit = coverage(&publication, "tool_exit").expect("tool_exit");
    assert_eq!(exit.state, "unavailable");
    assert!(
        exit.reasons.iter().any(|r| r == "tool_exit_not_recorded"),
        "tool_exit: {exit:?}"
    );
    let compaction = coverage(&publication, "compaction").expect("compaction");
    assert_eq!(compaction.state, "unavailable");
    assert!(
        compaction
            .reasons
            .iter()
            .any(|r| r == "pi_compaction_count_unverifiable"),
        "compaction: {compaction:?}"
    );
}

#[test]
fn pi_agent_facts_error_result_and_unpaired_call() {
    // `isError: true` records the error verdict on the paired call.
    let publication = extract_session_facts(
        &packet(
            "pi_agent",
            json!({"source": "pi_agent"}),
            vec![
                msg(0, "user", "made-up pi prompt", json!({
                    "type": "message", "id": "e1", "parentId": null,
                    "timestamp": "t0",
                    "message": {"role": "user", "timestamp": "t0",
                                "content": "made-up pi prompt"},
                    "cass": {"entry_kind": "message", "source_role": "user",
                             "entry_id": "e1", "parent_id": null, "source_line": 1,
                             "on_active_branch": true, "in_active_context": true}
                })),
                msg(1, "agent", "[Tool: write_pi_widget] file_path=/tmp/synthetic/pi.txt", json!({
                    "type": "message", "id": "e2", "parentId": "e1",
                    "timestamp": "t1",
                    "message": {
                        "role": "assistant", "model": "made-up-pi-model",
                        "timestamp": "t1",
                        "content": [
                            {"type": "toolCall", "id": "pc2",
                             "name": "write_pi_widget",
                             "arguments": {"file_path": "/tmp/synthetic/pi.txt"}}
                        ]
                    },
                    "cass": {"entry_kind": "message", "source_role": "assistant",
                             "entry_id": "e2", "parent_id": "e1", "source_line": 2,
                             "on_active_branch": true, "in_active_context": true,
                             "model": "made-up-pi-model"}
                })),
                msg(2, "tool", "made-up pi failure output", json!({
                    "type": "message", "id": "e3", "parentId": "e2",
                    "timestamp": "t2",
                    "message": {"role": "toolResult", "timestamp": "t2",
                                "content": "made-up pi failure output",
                                "isError": true,
                                "toolCallId": "pc2",
                                "toolName": "write_pi_widget"},
                    "cass": {"entry_kind": "message", "source_role": "toolResult",
                             "entry_id": "e3", "parent_id": "e2", "source_line": 3,
                             "on_active_branch": true, "in_active_context": true}
                })),
            ],
        ),
        "pi_agent",
    );
    let write = publication
        .tools
        .iter()
        .find(|t| t.call_id == "pc2")
        .expect("pc2");
    assert_eq!(write.is_error, Some(true));
    assert_eq!(publication.tool_error_count, Some(1));

    // An unpaired call keeps the text inventory complete: the connector
    // drops empty-content entries, so the missing result entry carried no
    // text (either it never existed or it was dropped for being empty).
    let unpaired = extract_session_facts(
        &packet(
            "pi_agent",
            json!({"source": "pi_agent"}),
            vec![
                msg(0, "user", "made-up pi prompt", json!({
                    "type": "message", "id": "e1", "parentId": null,
                    "timestamp": "t0",
                    "message": {"role": "user", "timestamp": "t0",
                                "content": "made-up pi prompt"},
                    "cass": {"entry_kind": "message", "source_role": "user",
                             "entry_id": "e1", "parent_id": null, "source_line": 1,
                             "on_active_branch": true, "in_active_context": true}
                })),
                msg(1, "agent", "[Tool: read_pi_widget] file_path=/tmp/synthetic/pi.txt", json!({
                    "type": "message", "id": "e2", "parentId": "e1",
                    "timestamp": "t1",
                    "message": {
                        "role": "assistant", "timestamp": "t1",
                        "content": [
                            {"type": "toolCall", "id": "pc3",
                             "name": "read_pi_widget",
                             "arguments": {"file_path": "/tmp/synthetic/pi.txt"}}
                        ]
                    },
                    "cass": {"entry_kind": "message", "source_role": "assistant",
                             "entry_id": "e2", "parent_id": "e1", "source_line": 2,
                             "on_active_branch": true, "in_active_context": true}
                })),
            ],
        ),
        "pi_agent",
    );
    let read = unpaired
        .tools
        .iter()
        .find(|t| t.call_id == "pc3")
        .expect("pc3");
    assert!(!read.has_result);
    let tools = coverage(&unpaired, "tools").expect("tools");
    assert_eq!(tools.state, "partial");
    assert!(
        tools.reasons.iter().any(|r| r == "tool_result_missing"),
        "tools: {tools:?}"
    );
    let text = coverage(&unpaired, "text_inventory").expect("text_inventory");
    assert_eq!(text.state, "complete", "text_inventory: {text:?}");
}

#[test]
fn pi_agent_facts_compaction_branch_and_drift() {
    // Compaction entries are normalized (system role, "[compaction] …"
    // content, systemMessage stripped) and count; branch summaries are
    // boundary records that do not.
    let publication = extract_session_facts(
        &packet(
            "pi_agent",
            json!({"source": "pi_agent"}),
            vec![
                msg(0, "user", "made-up pi prompt", json!({
                    "type": "message", "id": "e1", "parentId": null,
                    "timestamp": "t0",
                    "message": {"role": "user", "timestamp": "t0",
                                "content": "made-up pi prompt"},
                    "cass": {"entry_kind": "message", "source_role": "user",
                             "entry_id": "e1", "parent_id": null, "source_line": 1,
                             "on_active_branch": true, "in_active_context": true}
                })),
                msg(1, "system", "[compaction] made-up compaction summary", json!({
                    "type": "compaction", "id": "e2", "parentId": "e1",
                    "timestamp": "t1",
                    "summary": "made-up compaction summary",
                    "firstKeptEntryId": "e3", "tokensBefore": 50000,
                    "fromHook": false,
                    "cass": {"entry_kind": "compaction",
                             "source_role": "compaction",
                             "entry_id": "e2", "parent_id": "e1", "source_line": 2,
                             "on_active_branch": true, "in_active_context": true,
                             "first_kept_entry_id": "e3",
                             "tokens_before": 50000, "from_hook": false,
                             "system_checkpoint": true}
                })),
                msg(2, "system", "[branch summary] made-up branch summary", json!({
                    "type": "branch_summary", "id": "e3", "parentId": "e1",
                    "timestamp": "t2",
                    "summary": "made-up branch summary",
                    "fromId": "e0", "fromHook": true,
                    "cass": {"entry_kind": "branch_summary",
                             "source_role": "branch_summary",
                             "entry_id": "e3", "parent_id": "e1", "source_line": 3,
                             "on_active_branch": false, "in_active_context": false,
                             "from_id": "e0", "from_hook": true}
                })),
            ],
        ),
        "pi_agent",
    );
    assert_eq!(publication.compaction_count, Some(1));
    // Like claude, an observed compaction reports through the count; the
    // coverage entry exists only when the field is degraded (the
    // zero-count case asserts the `pi_compaction_count_unverifiable`
    // unavailable entry in the first test).
    assert!(
        coverage(&publication, "compaction").is_none(),
        "compaction: {:?}",
        coverage(&publication, "compaction")
    );
    // Both boundary summaries are delivered as system-role content.
    let text = coverage(&publication, "text_inventory").expect("text_inventory");
    assert_eq!(text.state, "complete", "text_inventory: {text:?}");

    // pi's role union is open (extensions may add roles via declaration
    // merging): an unmapped role keeps its raw role and is drift, never a
    // shape guess.
    let drift = extract_session_facts(
        &packet(
            "pi_agent",
            json!({"source": "pi_agent"}),
            vec![msg(0, "extensionWidget", "made-up extension text", json!({
                "type": "message", "id": "e1", "parentId": null,
                "timestamp": "t0",
                "message": {"role": "extensionWidget", "timestamp": "t0",
                            "content": [
                                {"type": "text", "text": "made-up extension text"}
                            ]},
                "cass": {"entry_kind": "message", "source_role": "extensionWidget",
                         "entry_id": "e1", "parent_id": null, "source_line": 1,
                         "on_active_branch": true, "in_active_context": true}
            }))],
        ),
        "pi_agent",
    );
    let entry = coverage(&drift, "text_inventory").expect("text_inventory");
    assert_eq!(entry.state, "partial");
    assert!(
        entry.reasons.iter().any(|r| r == "pi_record_unknown"),
        "text_inventory: {entry:?}"
    );

    // An unknown content block type is block-inventory drift; image blocks
    // are known no-text blocks (the connector skips them when flattening).
    let blocks = extract_session_facts(
        &packet(
            "pi_agent",
            json!({"source": "pi_agent"}),
            vec![msg(0, "agent", "made-up pi answer", json!({
                "type": "message", "id": "e1", "parentId": null,
                "timestamp": "t0",
                "message": {"role": "assistant", "timestamp": "t0",
                            "content": [
                                {"type": "futureBlock", "opaque": true},
                                {"type": "text", "text": "made-up pi answer"}
                            ]},
                "cass": {"entry_kind": "message", "source_role": "assistant",
                         "entry_id": "e1", "parent_id": null, "source_line": 1,
                         "on_active_branch": true, "in_active_context": true}
            }))],
        ),
        "pi_agent",
    );
    let inventory = coverage(&blocks, "block_inventory").expect("block_inventory");
    assert_eq!(inventory.state, "partial");
    assert!(
        inventory.reasons.iter().any(|r| r == "pi_block_unknown"),
        "block_inventory: {inventory:?}"
    );

    let image = extract_session_facts(
        &packet(
            "pi_agent",
            json!({"source": "pi_agent"}),
            vec![msg(0, "agent", "made-up pi answer", json!({
                "type": "message", "id": "e1", "parentId": null,
                "timestamp": "t0",
                "message": {"role": "assistant", "timestamp": "t0",
                            "content": [
                                {"type": "image", "source": "made-up-image"},
                                {"type": "text", "text": "made-up pi answer"}
                            ]},
                "cass": {"entry_kind": "message", "source_role": "assistant",
                         "entry_id": "e1", "parent_id": null, "source_line": 1,
                         "on_active_branch": true, "in_active_context": true}
            }))],
        ),
        "pi_agent",
    );
    let inventory = coverage(&image, "block_inventory").expect("block_inventory");
    assert_eq!(inventory.state, "complete", "block_inventory: {inventory:?}");

    // A non-object extra is unknown entirely.
    let missing = extract_session_facts(
        &packet(
            "pi_agent",
            json!({"source": "pi_agent"}),
            vec![msg(0, "user", "made-up text beside a null extra", Value::Null)],
        ),
        "pi_agent",
    );
    let entry = coverage(&missing, "chat").expect("chat");
    assert_eq!(entry.state, "unavailable");
    assert!(
        entry.reasons.iter().any(|r| r == "pi_extra_missing"),
        "chat: {entry:?}"
    );
}

#[test]
fn pi_agent_regions_index_toolcall_blocks_and_results() {
    let p = packet("pi_agent", json!({"source": "pi_agent"}), pi_messages());
    let publication = extract_session_facts(&p, "pi_agent");
    let structure = project_session_structure(&p, &publication);
    let regions = derive_message_regions(1, &p, &structure);

    // The toolCall block indexes as a tool region carrying its arguments
    // and the paired toolResult content, with the recorded error verdict.
    let call = regions
        .iter()
        .find(|r| r.region_key == "tool:0:pc1")
        .unwrap_or_else(|| panic!("pc1 tool region missing: {regions:?}"));
    assert_eq!(call.scope, "tools");
    assert_eq!(call.tool_name.as_deref(), Some("read_pi_widget"));
    assert_eq!(
        call.file_path.as_deref(),
        Some("/tmp/synthetic/pi.txt")
    );
    assert_eq!(call.has_error, Some(false), "{call:?}");
    assert!(
        call.indexed_text
            .as_deref()
            .is_some_and(|text| text.contains("/tmp/synthetic/pi.txt")
                && text.contains("made-up pi tool output")),
        "region text: {call:?}"
    );

    // The generic thinking reader sees pi thinking blocks
    // (/message/content, type "thinking" with a "thinking" string).
    let thinking = regions
        .iter()
        .find(|r| r.region_key == "m1:thinking")
        .unwrap_or_else(|| panic!("m1:thinking region missing: {regions:?}"));
    assert_eq!(thinking.scope, "thinking");
    assert!(
        thinking.indexed_text
            .as_deref()
            .is_some_and(|text| text.contains("made-up pi reasoning")),
        "{thinking:?}"
    );

    // The paired toolResult record must not ALSO grow the role-fallback
    // name:"unknown" call region (its result is correlated with pc1).
    assert!(
        !regions.iter().any(|r| r.tool_name.as_deref() == Some("unknown")),
        "role fallback must stay suppressed: {regions:?}"
    );

    // The provider gate: the same shapes under a foreign slug must not
    // grow pi tool regions — and the generic role fallback keeps the
    // tool-role content searchable there.
    let foreign = packet("junie", json!({"source": "pi_agent"}), pi_messages());
    let foreign_facts = extract_session_facts(&foreign, "junie");
    let foreign_structure = project_session_structure(&foreign, &foreign_facts);
    let foreign_regions = derive_message_regions(1, &foreign, &foreign_structure);
    assert!(
        !foreign_regions
            .iter()
            .any(|r| r.tool_name.as_deref() == Some("read_pi_widget")),
        "foreign slug must not derive pi tool regions: {foreign_regions:?}"
    );
    assert!(
        foreign_regions
            .iter()
            .any(|r| r.tool_name.as_deref() == Some("unknown")),
        "generic role fallback must keep the tool-role text: {foreign_regions:?}"
    );
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
    ensure!(version == FACTS_EXTRACTOR_VERSION, "extractor_version");
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
    PROJECTION_VERSION, ProjectionPublishOutcome, project_session_structure,
    publish_session_structure, publish_structure_for_canonical_best_effort,
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
fn cursor_projection_cass_invocations_are_tool_call_parts() {
    // Cursor transcript tool messages archive their tool_use blocks under
    // `cass.invocations` (the connector lifts every block; the ingest merge
    // writes the envelope). For cursor that single-cass-key envelope is
    // NOT a compaction skeleton — the invocations are the recorded calls —
    // so the derived block inventory carries one tool_call part per
    // invocation, call ids preserved, instead of degrading the record to
    // its flattened text.
    let packet = packet(
        "cursor",
        json!({"source": "cursor"}),
        vec![
            msg(
                0,
                "user",
                "synthetic prompt",
                json!({"type": 1, "rawText": "synthetic prompt", "bubbleId": "b1"}),
            ),
            msg(
                1,
                "assistant",
                "[Tool: read_file - /tmp/synthetic.txt] [Tool: bash - ls /tmp]",
                json!({"cass": {"invocations": [
                    {"kind": "tool", "call_id": "call_synth_1", "name": "read_file",
                     "input": {"file_path": "/tmp/synthetic.txt"}},
                    {"kind": "tool", "call_id": "call_synth_2", "name": "bash",
                     "input": {"command": "ls /tmp"}},
                ]}}),
            ),
        ],
    );
    let facts = extract_session_facts(&packet, "cursor");
    let structure = project_session_structure(&packet, &facts);

    // Step 2 is the assistant turn: exactly one tool_call part per
    // invocation, in recorded order; the flattened content is not
    // double-counted as a text part beside them.
    assert_eq!(
        parts(&structure, 2),
        vec![
            ("tool_call", Some("call_synth_1"), 1),
            ("tool_call", Some("call_synth_2"), 1),
        ],
        "{:?}",
        structure.steps
    );
}

#[test]
fn codex_projection_compacted_skeleton_stays_text_only() {
    // Codex compaction skeletons keep the flattened-text-only projection:
    // for codex the single-cass-key envelope means the raw response record
    // was compacted away, so only the flattened text remains projectable.
    let packet = packet(
        "codex",
        json!({"source": "codex"}),
        vec![msg(
            0,
            "assistant",
            "synthetic compacted codex reply",
            json!({"cass": {"model": "synthetic-model"}}),
        )],
    );
    let facts = extract_session_facts(&packet, "codex");
    let structure = project_session_structure(&packet, &facts);

    assert_eq!(
        parts(&structure, 1),
        vec![("text", None, 0)],
        "{:?}",
        structure.steps
    );
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
    ensure!(
        marker.get_typed::<i64>(0)? == PROJECTION_VERSION,
        "projection_version"
    );
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
