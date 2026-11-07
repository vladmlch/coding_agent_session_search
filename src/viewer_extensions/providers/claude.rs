//! Claude session facts: `tool_use`/`tool_result` pairing, streaming reply
//! dedup (`requestId`/`message.id` + `uuid` physical dedup), thinking blocks
//! and explicit coverage. Behavioral port of the oracle `facts/claude.py`;
//! message ancestry never establishes session ancestry.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use serde_json::{Value, json};

use crate::model::conversation_packet::ConversationPacket;

use super::classification::{self, ClassifyCall};
use super::{
    CoverageBuilder, Event, ProviderExtraction, RecordedCall, RecordedResult, ToolCollector,
    events, explicit_status, is_compacted_extra, json_text, recorded_text, text_id,
};

const EXTRACTOR_REVISION: &str = "claude-facts-v1";
const BLOCK_TYPES: &[&str] = &[
    "text",
    "thinking",
    "redacted_thinking",
    "tool_use",
    "tool_result",
];

/// Repeated `usage`/`costUSD` is accounting, not a rewrite of the record.
fn semantic_record(record: &Value) -> String {
    let mut stripped = serde_json::Map::new();
    for (key, value) in record.as_object().into_iter().flatten() {
        if key == "usage" || key == "costUSD" {
            continue;
        }
        let mut value = value.clone();
        if key == "message"
            && let Some(message) = value.as_object_mut()
        {
            message.remove("usage");
        }
        stripped.insert(key.clone(), value);
    }
    json_text(&Value::Object(stripped))
}

/// Physical-record dedup by `uuid`: a conflicting uuid is unknown (never
/// first-wins merged). Returns surviving events in `(idx, key)` order plus
/// their occurrence count.
fn unique_events<'a>(input: &'a [Event<'a>], coverage: &mut CoverageBuilder) -> Vec<&'a Event<'a>> {
    let mut groups: BTreeMap<String, Vec<&Event>> = BTreeMap::new();
    for event in input {
        let group = text_id(event.extra.get("uuid"))
            .map(|uuid| format!("uuid:{uuid}"))
            .unwrap_or_else(|| format!("key:{}", event.key));
        groups.entry(group).or_default().push(event);
    }
    let mut result = Vec::new();
    for candidates in groups.values() {
        let signatures: HashSet<String> = candidates
            .iter()
            .map(|event| {
                if event.extra.is_object() {
                    semantic_record(event.extra)
                } else {
                    json_text(&json!([event.role, event.content, event.extra]))
                }
            })
            .collect();
        if signatures.len() != 1 {
            for _ in candidates {
                coverage.unknown("conflicting_event");
            }
            continue;
        }
        result.push(candidates[0]);
    }
    result.sort_by(|a, b| (a.idx, &a.key).cmp(&(b.idx, &b.key)));
    result
}

fn is_boundary(record: &Value) -> bool {
    matches!(
        record.get("type").and_then(Value::as_str),
        Some("summary" | "compact_boundary")
    ) || record.get("subtype").and_then(Value::as_str) == Some("compact_boundary")
        || record.get("isCompactSummary").and_then(Value::as_bool) == Some(true)
}

/// `(requestId or message.id, message.id)` — the logical reply key.
fn logical_key(record: &Value) -> Option<String> {
    let message = record.get("message")?.as_object()?;
    let is_assistant = record.get("type").and_then(Value::as_str) == Some("assistant")
        || message.get("role").and_then(Value::as_str) == Some("assistant");
    if !is_assistant {
        return None;
    }
    let message_id = message.get("id");
    let group_id = record
        .get("requestId")
        .filter(|v| !v.is_null())
        .or(message_id)?;
    let scalar = |v: &Value| matches!(v, Value::String(_) | Value::Number(_)) && !v.is_null();
    if !scalar(group_id) {
        return None;
    }
    if !message_id.is_none_or(scalar) {
        return None;
    }
    Some(json_text(&json!([group_id, message_id])))
}

/// Content blocks of a record: `message.content[]`, bare block records,
/// `tool_use`/`tool_result` dicts, or `toolResults[]`.
fn contents(record: &Value) -> Option<Vec<Value>> {
    if let Some(message) = record.get("message").and_then(Value::as_object)
        && let Some(content) = message.get("content")
    {
        return Some(vec_or_single(content));
    }
    if BLOCK_TYPES.contains(&record.get("type").and_then(Value::as_str).unwrap_or("")) {
        return Some(vec![record.clone()]);
    }
    if let Some(content) = record.get("content") {
        return Some(vec_or_single(content));
    }
    for field in ["tool_use", "tool_result"] {
        if let Some(value) = record.get(field).filter(|v| v.is_object()) {
            let mut block = value.clone();
            block["type"] = json!(field);
            return Some(vec![block]);
        }
    }
    if let Some(results) = record.get("toolResults").and_then(Value::as_array) {
        return Some(
            results
                .iter()
                .map(|value| {
                    if value.is_object() {
                        json!({
                            "type": "tool_result",
                            "tool_use_id": value.get("tool_use_id").or(value.get("toolUseId")),
                            "content": value.get("content").or(value.get("output")),
                        })
                    } else {
                        value.clone()
                    }
                })
                .collect(),
        );
    }
    None
}

fn vec_or_single(content: &Value) -> Vec<Value> {
    match content {
        Value::Array(items) => items.clone(),
        other => vec![other.clone()],
    }
}

/// Top-level `toolUseResult` + `sourceToolUseID` result arm.
fn top_level_result(
    event: &Event,
    record: &Value,
    coverage: &mut CoverageBuilder,
    tools: &mut ToolCollector,
) -> bool {
    if !record
        .as_object()
        .is_some_and(|m| m.contains_key("toolUseResult"))
    {
        return false;
    }
    let Some(call_id) = text_id(record.get("sourceToolUseID")) else {
        coverage.unknown("claude_tool_identity_missing");
        return true;
    };
    let value = &record["toolUseResult"];
    let (text, complete, exit_code, is_error) = if value.is_object() {
        let output = value.get("output").or(value.get("content"));
        let (text, complete) = recorded_text(output.unwrap_or(&Value::Null));
        let (exit, error) = explicit_status(value);
        (text, complete, exit, error)
    } else {
        let (text, complete) = recorded_text(value);
        (text, complete, None, None)
    };
    if !complete || text.is_none() {
        coverage.mark(
            &["tools", "text_inventory"],
            "tool_output_not_recorded",
            "partial",
        );
    }
    tools.result(RecordedResult {
        event_idx: event.idx,
        call_id,
        text,
        exit_code,
        is_error,
    });
    true
}

/// Canonical FAD tool-result message `{"source":"tool_result",…}`: the
/// connector lifts each result block into its own `role:"tool"` message.
fn normalized_result(
    event: &Event,
    record: &Value,
    coverage: &mut CoverageBuilder,
    tools: &mut ToolCollector,
) -> bool {
    if record.get("source").and_then(Value::as_str) != Some("tool_result") {
        return false;
    }
    let Some(call_id) = text_id(record.get("tool_use_id")) else {
        coverage.unknown("claude_tool_identity_missing");
        return true;
    };
    let text = (!event.content.is_empty()).then(|| event.content.to_string());
    if text.is_none() {
        coverage.mark(
            &["tools", "text_inventory"],
            "tool_output_not_recorded",
            "partial",
        );
    }
    tools.result(RecordedResult {
        event_idx: event.idx,
        call_id,
        text,
        exit_code: None,
        is_error: record.get("is_error").and_then(Value::as_bool),
    });
    true
}

fn use_block(
    event: &Event,
    block: &Value,
    coverage: &mut CoverageBuilder,
    tools: &mut ToolCollector,
) {
    let call_id = text_id(block.get("id"));
    let name = text_id(block.get("name"));
    let (Some(call_id), Some(name)) = (call_id, name) else {
        coverage.unknown("claude_tool_identity_missing");
        return;
    };
    if block.as_object().is_some_and(|m| !m.contains_key("input")) {
        coverage.mark(
            &["tools", "files", "mcp", "text_inventory"],
            "tool_input_missing",
            "partial",
        );
    }
    coverage.evidence("tools");
    tools.call(RecordedCall {
        event_idx: event.idx,
        message_id: event.message_id,
        call_id: call_id.clone(),
        raw_call_id: call_id,
        name,
        arguments: block.get("input").cloned(),
    });
}

fn result_block(
    event: &Event,
    block: &Value,
    coverage: &mut CoverageBuilder,
    tools: &mut ToolCollector,
) {
    let Some(call_id) = text_id(block.get("tool_use_id")) else {
        coverage.unknown("claude_tool_identity_missing");
        return;
    };
    let (output, complete) = recorded_text(block.get("content").unwrap_or(&Value::Null));
    if !complete
        || block
            .as_object()
            .is_some_and(|m| !m.contains_key("content"))
    {
        coverage.mark(
            &["tools", "text_inventory"],
            "tool_output_not_recorded",
            "partial",
        );
    }
    let (exit_code, is_error) = explicit_status(block);
    coverage.evidence("tools");
    tools.result(RecordedResult {
        event_idx: event.idx,
        call_id,
        text: output,
        exit_code,
        is_error,
    });
}

#[allow(clippy::too_many_arguments)]
fn part(
    event: &Event,
    block: &Value,
    coverage: &mut CoverageBuilder,
    tools: &mut ToolCollector,
    thinking: &mut i64,
) {
    match block {
        Value::String(_) => coverage.evidence("chat"),
        Value::Object(map) => match map.get("type").and_then(Value::as_str) {
            Some("tool_use") => use_block(event, block, coverage, tools),
            Some("tool_result") => result_block(event, block, coverage, tools),
            Some("text") if map.get("text").and_then(Value::as_str).is_some() => {
                coverage.evidence("chat")
            }
            Some("thinking") if map.get("thinking").and_then(Value::as_str).is_some() => {
                *thinking += 1;
                coverage.evidence("thinking");
            }
            Some("redacted_thinking") => {
                coverage.mark(
                    &["thinking", "text_inventory"],
                    "thinking_redacted",
                    "unavailable",
                );
            }
            Some("thinking") => {
                coverage.mark(
                    &["thinking", "text_inventory"],
                    "thinking_not_recorded",
                    "unavailable",
                );
            }
            // Non-text data is explicitly described by the source; no fake label.
            Some("image") => {}
            _ => coverage.unknown("claude_content_unknown"),
        },
        _ => coverage.unknown("claude_content_malformed"),
    }
}

pub(crate) fn extract(packet: &ConversationPacket) -> ProviderExtraction {
    let input = events(packet);
    let mut coverage = CoverageBuilder::new();
    // The claude_code connector drops tool_result content with no text part,
    // so an unpaired call never means session text was lost.
    let mut tools = ToolCollector::new().unpaired_results_carry_no_text();
    let mut seen_blocks: HashSet<(u64, String, String)> = HashSet::new();
    let mut segment = 0_u64;
    let mut current_session: Option<String> = None;
    let mut thinking = 0_i64;
    let mut compactions = 0_i64;
    let mut models: BTreeSet<String> = BTreeSet::new();
    let mut deduped: Vec<(i64, &str, &str)> = Vec::new();

    for event in unique_events(&input, &mut coverage) {
        let record = event.extra;
        if !record.is_object() {
            coverage.unknown("claude_extra_missing");
            deduped.push((event.idx, event.role, event.content));
            continue;
        }
        if is_compacted_extra(record) {
            coverage.mark_compacted("extra_compacted");
        }
        if let Some(model) = record
            .pointer("/message/model")
            .or_else(|| record.get("model"))
            .or_else(|| record.pointer("/cass/model"))
            .and_then(Value::as_str)
            .filter(|m| !m.is_empty())
        {
            models.insert(model.to_string());
        }
        if let Some(session_id) = text_id(record.get("sessionId")) {
            if current_session
                .as_ref()
                .is_some_and(|current| *current != session_id)
            {
                segment += 1;
            }
            current_session = Some(session_id);
        }
        if is_boundary(record) {
            segment += 1;
            compactions += 1;
            continue;
        }
        deduped.push((event.idx, event.role, event.content));
        let Some(items) = contents(record) else {
            if top_level_result(event, record, &mut coverage, &mut tools)
                || normalized_result(event, record, &mut coverage, &mut tools)
            {
                continue;
            }
            coverage.unknown("claude_shape_unknown");
            continue;
        };
        if !matches!(items.as_slice(), [Value::String(_)] | [])
            && !items
                .iter()
                .any(|item| item.get("type").and_then(Value::as_str) == Some("tool_result"))
        {
            // A bare toolUseResult accompanies content that carries no
            // tool_result block (oracle order).
            top_level_result(event, record, &mut coverage, &mut tools);
        }
        if items
            .iter()
            .any(|item| !item.is_string() && !item.is_object())
        {
            coverage.unknown("claude_content_malformed");
            continue;
        }
        let group = logical_key(record);
        for (index, value) in items.iter().enumerate() {
            if group.is_some()
                && value.is_object()
                && matches!(
                    value.get("type").and_then(Value::as_str),
                    Some("text" | "thinking")
                )
            {
                let signature = (segment, group.clone().unwrap_or_default(), json_text(value));
                if !seen_blocks.insert(signature) {
                    continue;
                }
            }
            let _ = index;
            part(event, value, &mut coverage, &mut tools, &mut thinking);
        }
    }

    let metadata = &packet.payload.metadata_json;
    let is_subagent = metadata
        .get("is_subagent")
        .or_else(|| metadata.get("sidechain"))
        .and_then(Value::as_bool);
    let parent_external_id = metadata
        .get("parentSessionId")
        .or_else(|| metadata.get("parent_id"))
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .map(str::to_string);
    // A recorded sidechain flag means the relation itself is known.
    let is_subagent = is_subagent.or(parent_external_id.as_ref().map(|_| true));

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
    let compaction_count = if compactions > 0 {
        Some(compactions)
    } else {
        coverage.mark(
            &["compaction"],
            "boundary_records_not_normalized",
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
        is_subagent,
        parent_external_id,
        turn_count,
        step_count,
        first_prompt,
        task_category,
        quality: Some(quality),
    }
}
