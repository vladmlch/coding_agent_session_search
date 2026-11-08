//! Pi Agent session facts from raw tree-entry extras. The pinned FAD
//! connector (pi_wire) archives every emitted tree entry verbatim in
//! `extra_json` and inserts a `cass` provenance object
//! ({entry_kind, source_role, entry_id, parent_id, source_line,
//! on_active_branch, in_active_context, model?, provider?, invocations?})
//! — verified against the 10 persisted pi_agent messages (key/type
//! structure only here, synthetic text in tests).
//!
//! Connector contract (pinned rev ad5fb23b):
//!   - emitted entry kinds: `message` (roles user/bashExecution/custom/
//!     hookMessage → user, assistant → agent, toolResult → tool,
//!     branchSummary/compactionSummary → system; raw `system` messages
//!     dropped), `custom_message` (→ user), `compaction` (→ system,
//!     "[compaction] {summary}", systemMessage stripped as
//!     context-boundary metadata), `branch_summary` (→ system);
//!     `title` entries and unknown kinds are dropped before the packet;
//!   - empty-content entries are dropped, so an unpaired call means the
//!     result entry carried no text or never existed — the text inventory
//!     stays complete (`unpaired_results_carry_no_text`);
//!   - message content blocks: `text`, `thinking` (flattened as
//!     "[Thinking] …"), `toolCall` (flattened as "[Tool: name] k=v…" —
//!     the block facts stay in extra), `image` (known no-text);
//!   - `toolResult` messages carry `toolCallId`/`toolName`/`isError` —
//!     the correlation id and the recorded error verdict; pi records no
//!     exit codes;
//!   - bashExecution runs are user-role text ("$ {command}\n{output}");
//!     their `exitCode` provenance is the shell run's own record with no
//!     call identity, so it is not a tool fact and never invents one.
//!
//! pi's message union is OPEN (the connector keeps unmapped roles as-is
//! for declaration-merged extensions), so there is no strict key gate:
//! drift is an unmapped role or an unknown content block type.

use std::collections::BTreeSet;

use serde_json::Value;

use crate::model::conversation_packet::ConversationPacket;

use super::classification::{self, ClassifyCall};
use super::{
    CORE_FIELDS, CoverageBuilder, Event, ProviderExtraction, RecordedCall, RecordedResult,
    ToolCollector, events, text_id,
};

const EXTRACTOR_REVISION: &str = "pi-agent-facts-v1";

/// Message roles the connector maps to a fixed packet role
/// (`normalized_role`). Raw `system` messages are dropped and never reach
/// the packet; anything else is an extension role — drift.
const KNOWN_MESSAGE_ROLES: &[&str] = &[
    "user",
    "bashExecution",
    "custom",
    "hookMessage",
    "assistant",
    "toolResult",
    "branchSummary",
    "compactionSummary",
];

/// One `toolCall` content block: the call identity is the block `id`.
fn record_tool_call(
    event: &Event,
    block: &Value,
    coverage: &mut CoverageBuilder,
    tools: &mut ToolCollector,
) {
    let Some(name) = text_id(block.get("name")) else {
        coverage.mark(&["tools"], "pi_tool_name_missing", "partial");
        return;
    };
    coverage.evidence("tools");
    let Some(call_id) = text_id(block.get("id")) else {
        coverage.mark(
            &["tools", "tool_exit", "tool_error"],
            "pi_tool_call_identity_missing",
            "partial",
        );
        return;
    };
    tools.call(RecordedCall {
        event_idx: event.idx,
        message_id: event.message_id,
        raw_call_id: call_id.clone(),
        call_id,
        name,
        arguments: block.get("arguments").cloned(),
    });
}

/// Classify one record's content blocks and record their evidence
/// (thinking counts, tool calls).
fn record_blocks(
    event: &Event,
    blocks: &[Value],
    coverage: &mut CoverageBuilder,
    tools: &mut ToolCollector,
    thinking: &mut i64,
) {
    for block in blocks {
        let kind = block.get("type").and_then(Value::as_str).unwrap_or("");
        match kind {
            "text" => {}
            "thinking" => {
                // The flattener renders "[Thinking] {text}"; a block with
                // no thinking string delivers nothing.
                if block
                    .get("thinking")
                    .and_then(Value::as_str)
                    .is_some_and(|text| !text.trim().is_empty())
                {
                    *thinking += 1;
                    coverage.evidence("thinking");
                } else {
                    coverage.mark(&["thinking", "text_inventory"], "pi_block_malformed", "partial");
                }
            }
            "toolCall" => record_tool_call(event, block, coverage, tools),
            "image" => {}
            _ => coverage.mark(&["block_inventory"], "pi_block_unknown", "partial"),
        }
    }
}

pub(crate) fn extract(packet: &ConversationPacket) -> ProviderExtraction {
    let input = events(packet);
    let mut coverage = CoverageBuilder::new();
    // Empty-content entries are dropped by the connector, so an unpaired
    // call loses no session text.
    let mut tools = ToolCollector::new().unpaired_results_carry_no_text();
    let mut thinking = 0_i64;
    let mut compactions = 0_i64;
    let mut models: BTreeSet<String> = BTreeSet::new();
    let mut deduped: Vec<(i64, &str, &str)> = Vec::new();

    for event in &input {
        deduped.push((event.idx, event.role, event.content));
        let extra = event.extra;
        if !extra.is_object() {
            coverage.unknown("pi_extra_missing");
            continue;
        }
        if !event.content.is_empty() {
            coverage.evidence("chat");
        }
        // Assistant attribution rides the message and the cass provenance.
        if let Some(model) = extra
            .pointer("/message/model")
            .or_else(|| extra.pointer("/cass/model"))
            .and_then(Value::as_str)
            .filter(|model| !model.is_empty())
        {
            models.insert(model.to_string());
        }
        match extra.get("type").and_then(Value::as_str).unwrap_or("") {
            "message" => {
                let Some(message) = extra.get("message").filter(|m| m.is_object()) else {
                    coverage.mark(CORE_FIELDS, "pi_record_unknown", "partial");
                    continue;
                };
                let role = message.get("role").and_then(Value::as_str).unwrap_or("");
                if !KNOWN_MESSAGE_ROLES.contains(&role) {
                    // pi's role union is open (declaration-merged
                    // extensions keep their raw role) — drift, never a
                    // shape guess.
                    coverage.mark(CORE_FIELDS, "pi_record_unknown", "partial");
                }
                if role == "toolResult" {
                    // The result record: correlation id + recorded error
                    // verdict; the delivered content is the result text.
                    // A missing toolCallId keeps the result orphaned
                    // (honest `orphan_tool_result` partial).
                    let call_id =
                        text_id(message.get("toolCallId")).unwrap_or_default();
                    let is_error = message.get("isError").and_then(Value::as_bool);
                    tools.result(RecordedResult {
                        event_idx: event.idx,
                        call_id,
                        text: (!event.content.is_empty()).then(|| event.content.to_owned()),
                        exit_code: None,
                        is_error,
                    });
                }
                if let Some(blocks) = message.get("content").and_then(Value::as_array) {
                    record_blocks(event, blocks, &mut coverage, &mut tools, &mut thinking);
                }
            }
            "custom_message" => {
                // Extension-injected user text: the entry's own content
                // blocks (flattened by the connector).
                if let Some(blocks) = extra.get("content").and_then(Value::as_array) {
                    record_blocks(event, blocks, &mut coverage, &mut tools, &mut thinking);
                }
            }
            // Boundary records, normalized by the connector with their
            // summary delivered as system-role content.
            "compaction" => {
                compactions += 1;
            }
            "branch_summary" => {}
            // The connector drops unknown kinds before the packet, so this
            // is defensive drift handling for future contract changes.
            _ => coverage.mark(CORE_FIELDS, "pi_record_unknown", "partial"),
        }
    }

    if let Some(model) = packet
        .payload
        .metadata_json
        .get("model")
        .and_then(Value::as_str)
        .filter(|model| !model.is_empty())
    {
        models.insert(model.to_string());
    }

    let classify_calls: Vec<ClassifyCall> = tools
        .calls
        .values()
        .flatten()
        .map(|call| ClassifyCall {
            event_idx: call.event_idx,
            name: call.name.clone(),
            arguments: call.arguments.clone(),
        })
        .collect();
    let (turn_count, step_count, first_prompt, task_category) =
        classification::session_turn_stats(&deduped, &classify_calls);
    let event_ts: std::collections::BTreeMap<i64, Option<i64>> = input
        .iter()
        .map(|event| (event.idx, event.created_at))
        .collect();
    let quality_events: Vec<classification::QualityEvent> = deduped
        .iter()
        .map(|&(idx, role, content)| classification::QualityEvent {
            idx,
            created_at: event_ts.get(&idx).copied().flatten(),
            role,
            content,
        })
        .collect();
    let quality = classification::session_quality_metrics(&quality_events, &classify_calls);

    let (tools, file_facts) = tools.finish(&mut coverage);
    // A zero count cannot certify the source had no compactions: the
    // connector drops empty-summary compaction entries (no text lost, but
    // the count cannot see them). Observed compactions are exact.
    let compaction_count = if compactions > 0 {
        Some(compactions)
    } else {
        coverage.mark(
            &["compaction"],
            "pi_compaction_count_unverifiable",
            "unavailable",
        );
        None
    };
    ProviderExtraction {
        extractor: EXTRACTOR_REVISION,
        coverage,
        tools,
        file_fact_count: Some(file_facts as i64),
        thinking_block_count: Some(thinking),
        compaction_count,
        model_set: models.into_iter().collect(),
        is_subagent: None,
        parent_external_id: None,
        turn_count,
        step_count,
        first_prompt,
        task_category,
        quality: Some(quality),
    }
}
