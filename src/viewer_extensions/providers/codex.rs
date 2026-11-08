//! Codex session facts from discriminated recorded events, with explicit
//! compaction epochs. Behavioral port of the oracle `facts/codex.py`:
//! `function_call`/`custom_tool_call` → output pairing, response-id dedup,
//! and call IDs qualified by the compaction segment that recorded them —
//! the same raw call ID never merges across a boundary.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use serde_json::{Value, json};
use sha2::Digest;

use crate::model::conversation_packet::ConversationPacket;

use super::classification::{self, ClassifyCall};
use super::{
    CoverageBuilder, Event, ProviderExtraction, RecordedCall, RecordedResult, ToolCollector,
    decode_json_str, events, explicit_status, is_compacted_extra, json_text, recorded_text,
    text_id,
};

const EXTRACTOR_REVISION: &str = "codex-facts-v1";
const CALLS: &[&str] = &["function_call", "custom_tool_call"];
const RESULTS: &[&str] = &[
    "function_call_output",
    "custom_tool_call_output",
    "tool_output",
    "tool_result",
    "exec_command_end",
];
const CONTROLS: &[&str] = &[
    "session_meta",
    "turn_meta",
    "turn_context",
    "model_turn",
    "token_count",
    "turn_started",
    "turn_complete",
    "task_started",
    "task_complete",
];

fn decode(value: &Value) -> (Value, bool) {
    match value {
        Value::String(text) => match decode_json_str(text) {
            Some(decoded) => (decoded, true),
            None => (value.clone(), false),
        },
        other => (other.clone(), true),
    }
}

/// Call IDs are scoped to the compaction segment that recorded them; the
/// qualified ID is digest-stable, the raw provider ID is retained verbatim.
fn qualified_call(segment: Option<&str>, call_id: &str) -> String {
    if segment.is_none() && !call_id.starts_with("codex-segment:") {
        return call_id.to_string();
    }
    let digest = hex::encode(sha2::Sha256::digest(
        json_text(&json!([segment, call_id])).as_bytes(),
    ));
    format!("codex-segment:{digest}")
}

/// `(payload, wrapped)`: response_item/event_msg carry a payload dict;
/// event_msg with a bare `message` string reads as the record itself.
fn payload_of(record: &Value) -> (Value, bool) {
    if matches!(
        record.get("type").and_then(Value::as_str),
        Some("response_item" | "event_msg")
    ) {
        if let Some(payload) = record.get("payload").filter(|p| p.is_object()) {
            return (payload.clone(), true);
        }
        if record.get("type").and_then(Value::as_str) == Some("event_msg")
            && record.get("message").and_then(Value::as_str).is_some()
        {
            return (record.clone(), true);
        }
        return (record.clone(), false);
    }
    (record.clone(), false)
}

fn is_boundary(record: &Value, payload: &Value) -> bool {
    matches!(
        record.get("type").and_then(Value::as_str),
        Some("compacted" | "compact_boundary" | "fork_boundary")
    ) || (record.get("type").and_then(Value::as_str) == Some("event_msg")
        && matches!(
            payload.get("type").and_then(Value::as_str),
            Some("context_compacted" | "compacted")
        ))
}

/// Only explicit source identity deduplicates a response; equal text never
/// does. Returns `(event, segment)` pairs in `(idx, key)` order — `segment`
/// is the boundary event's source key, or None pre-boundary.
fn prepared_events<'a>(
    input: &'a [Event<'a>],
    coverage: &mut CoverageBuilder,
) -> Vec<(&'a Event<'a>, Option<String>)> {
    let mut groups: BTreeMap<(Option<String>, String), Vec<&Event>> = BTreeMap::new();
    let mut segment: Option<String> = None;
    for event in input {
        let record = event.extra;
        let mut response_id = None;
        if record.is_object() {
            let (payload, _) = payload_of(record);
            if is_boundary(record, &payload) {
                segment = Some(event.key.clone());
            }
            if record.get("type").and_then(Value::as_str) == Some("response_item") {
                response_id = text_id(payload.get("id"));
            }
        }
        let key = response_id
            .map(|id| {
                format!(
                    "response:{}",
                    json_text(&json!([record.pointer("/payload/type"), id]))
                )
            })
            .unwrap_or_else(|| format!("source:{}", event.key));
        groups
            .entry((segment.clone(), key))
            .or_default()
            .push(event);
    }
    let mut result = Vec::new();
    for ((epoch, _), candidates) in &groups {
        let mut signatures = HashSet::new();
        for event in candidates {
            let raw = event.extra;
            if raw.is_object() && raw.get("type").and_then(Value::as_str) == Some("response_item") {
                signatures.insert(json_text(raw.get("payload").unwrap_or(&Value::Null)));
            } else {
                signatures.insert(json_text(&json!([event.role, event.content, raw])));
            }
        }
        if signatures.len() != 1 {
            for _ in candidates {
                coverage.unknown("conflicting_event");
            }
            continue;
        }
        result.push((candidates[0], epoch.clone()));
    }
    result.sort_by(|a, b| (a.0.idx, &a.0.key).cmp(&(b.0.idx, &b.0.key)));
    result
}

fn call(
    event: &Event,
    payload: &Value,
    segment: Option<&str>,
    coverage: &mut CoverageBuilder,
    tools: &mut ToolCollector,
) {
    let call_id = text_id(payload.get("call_id").or(payload.get("id")));
    let name = text_id(payload.get("name"));
    let (Some(call_id), Some(name)) = (call_id, name) else {
        coverage.unknown("codex_tool_identity_missing");
        return;
    };
    let (arguments, valid) =
        if payload.get("type").and_then(Value::as_str) == Some("custom_tool_call") {
            // CASS preserves even JSON-looking freeform input verbatim.
            (
                payload.get("input").cloned(),
                payload.get("input").and_then(Value::as_str).is_some(),
            )
        } else {
            let has_field = payload
                .as_object()
                .is_some_and(|m| m.contains_key("arguments") || m.contains_key("args"));
            let (args, ok) = decode(
                payload
                    .get("arguments")
                    .or(payload.get("args"))
                    .unwrap_or(&Value::Null),
            );
            (
                if has_field || ok { Some(args) } else { None },
                ok && has_field,
            )
        };
    if !valid {
        coverage.mark(&["files", "mcp"], "tool_arguments_unparsed", "partial");
        if arguments.is_none() {
            coverage.mark(
                &["tools", "text_inventory"],
                "tool_input_missing",
                "partial",
            );
        }
    }
    coverage.evidence("tools");
    tools.call(RecordedCall {
        event_idx: event.idx,
        message_id: event.message_id,
        call_id: qualified_call(segment, &call_id),
        raw_call_id: call_id,
        name,
        arguments,
    });
}

/// `(text, complete, exit_code, is_error)` from a tool-output payload,
/// including the `{output, metadata}` envelope Native recognizes.
fn output_payload(payload: &Value) -> (Option<String>, bool, Option<i64>, Option<bool>) {
    let mut value: Option<&Value> = None;
    for field in [
        "output",
        "result",
        "aggregated_output",
        "stdout",
        "stderr",
        "formatted_output",
    ] {
        if let Some(candidate) = payload.get(field) {
            value = Some(candidate);
            break;
        }
    }
    let (mut exit_code, mut is_error) = explicit_status(payload);
    let mut value = value.cloned().unwrap_or(Value::Null);
    if let Value::String(text) = &value
        && text.trim_start().starts_with('{')
        && let Some(decoded) = decode_json_str(text)
        && decoded.is_object()
        && decoded
            .as_object()
            .is_some_and(|m| m.contains_key("output"))
    {
        if let Some(metadata) = decoded.get("metadata").filter(|m| m.is_object()) {
            let (nested_exit, nested_error) = explicit_status(metadata);
            exit_code = exit_code.or(nested_exit);
            if is_error.is_none() {
                is_error = nested_error;
            }
        }
        value = decoded["output"].clone();
    }
    let (text, complete) = recorded_text(&value);
    (text, complete && !value.is_null(), exit_code, is_error)
}

fn result(
    event: &Event,
    payload: &Value,
    segment: Option<&str>,
    coverage: &mut CoverageBuilder,
    tools: &mut ToolCollector,
) {
    let Some(call_id) = text_id(payload.get("call_id").or(payload.get("id"))) else {
        coverage.unknown("codex_tool_identity_missing");
        return;
    };
    let (text, complete, exit_code, is_error) = output_payload(payload);
    if !complete {
        coverage.mark(
            &["tools", "text_inventory"],
            "tool_output_not_recorded",
            "partial",
        );
    }
    coverage.evidence("tools");
    tools.result(RecordedResult {
        event_idx: event.idx,
        call_id: qualified_call(segment, &call_id),
        text,
        exit_code,
        is_error,
    });
}

fn message(payload: &Value, coverage: &mut CoverageBuilder) {
    let known_role = matches!(
        payload.get("role").and_then(Value::as_str),
        Some("user" | "assistant" | "system" | "developer")
    );
    if !known_role {
        coverage.mark(
            &["chat", "tools", "thinking"],
            "codex_message_role_unknown",
            "unavailable",
        );
    }
    message_content(payload, coverage);
}

/// Content-part parsing shared by `message` and `agent_message_text`: a
/// string body or text-typed parts with their `text` are chat evidence,
/// images are known non-text, anything else is malformed.
fn message_content(payload: &Value, coverage: &mut CoverageBuilder) {
    match payload.get("content") {
        Some(Value::String(_)) => coverage.evidence("chat"),
        Some(Value::Array(parts)) => {
            for part in parts {
                if part.is_string() {
                    coverage.evidence("chat");
                } else if part.is_object()
                    && matches!(
                        part.get("type").and_then(Value::as_str),
                        Some("input_text" | "output_text" | "text")
                    )
                {
                    if part.get("text").and_then(Value::as_str).is_some() {
                        coverage.evidence("chat");
                    } else {
                        coverage.unknown("codex_message_malformed");
                    }
                } else if part.is_object()
                    && matches!(
                        part.get("type").and_then(Value::as_str),
                        Some("image" | "input_image")
                    )
                {
                    continue;
                } else if part.is_object()
                    && part.get("type").and_then(Value::as_str) == Some("encrypted_content")
                {
                    // Provider-encrypted inter-agent payload (codex protocol
                    // `AgentMessageInputContent::EncryptedContent`): the model
                    // hands the handoff over already encrypted, so the rollout
                    // never held the plaintext — only the sibling `input_text`
                    // envelope header is readable. A known shape whose text is
                    // deliberately unreadable earns the redacted-thinking
                    // verdict, never an unknown-part claim.
                    coverage.mark(
                        &["chat", "text_inventory"],
                        "agent_message_encrypted",
                        "unavailable",
                    );
                } else {
                    coverage.unknown("codex_message_part_unknown");
                }
            }
        }
        _ => coverage.unknown("codex_message_malformed"),
    }
}

/// `agent_message` payloads — inter-agent handoff messages. `event_msg`
/// records carry a bare `message`/`text` string; `response_item` records
/// carry the text in `content[].input_text` parts (the same shapes the
/// connector indexes). No role check: the sender is an agent, not a chat
/// role — a record with no usable text in either shape stays malformed.
fn agent_message_text(payload: &Value, coverage: &mut CoverageBuilder) {
    if payload
        .get("message")
        .or_else(|| payload.get("text"))
        .and_then(Value::as_str)
        .is_some()
    {
        coverage.evidence("chat");
        return;
    }
    message_content(payload, coverage);
}

fn reasoning(payload: &Value, coverage: &mut CoverageBuilder, thinking: &mut i64) {
    let mut readable = false;
    for field in ["content", "summary"] {
        let Some(value) = payload.get(field) else {
            continue;
        };
        let parts: Vec<&Value> = match value {
            Value::Array(items) => items.iter().collect(),
            other => vec![other],
        };
        for part in parts {
            let text = if part.is_string() {
                part.as_str()
            } else {
                part.get("text").and_then(Value::as_str)
            };
            match text {
                Some(text) if !text.is_empty() => {
                    readable = true;
                    *thinking += 1;
                    coverage.evidence("thinking");
                }
                Some(_) => {}
                None => {
                    coverage.mark(
                        &["thinking", "text_inventory"],
                        "reasoning_part_unknown",
                        "partial",
                    );
                }
            }
        }
    }
    if payload.get("encrypted_content").is_some() {
        coverage.mark(
            &["thinking", "text_inventory"],
            "reasoning_encrypted",
            "unavailable",
        );
    } else if !readable {
        coverage.mark(
            &["thinking", "text_inventory"],
            "reasoning_not_recorded",
            "unavailable",
        );
    }
}

/// `event_msg` `agent_reasoning` records — the matrix's
/// "`agent_reasoning` → thinking" arm (the oracle leaves it unknown; the
/// recorded text is real thinking evidence).
fn agent_reasoning(payload: &Value, coverage: &mut CoverageBuilder, thinking: &mut i64) {
    let text = payload
        .get("text")
        .or_else(|| payload.get("message"))
        .and_then(Value::as_str);
    match text {
        Some(text) if !text.is_empty() => {
            *thinking += 1;
            coverage.evidence("thinking");
        }
        _ => coverage.mark(
            &["thinking", "text_inventory"],
            "reasoning_not_recorded",
            "unavailable",
        ),
    }
}

fn event_text(payload: &Value, coverage: &mut CoverageBuilder) {
    let value = payload.get("message").or_else(|| payload.get("text"));
    if value.and_then(Value::as_str).is_some() {
        coverage.evidence("chat");
    } else {
        coverage.unknown("codex_message_malformed");
    }
}

pub(crate) fn extract(packet: &ConversationPacket) -> ProviderExtraction {
    let input = events(packet);
    let mut coverage = CoverageBuilder::new();
    let mut tools = ToolCollector::new();
    let mut thinking = 0_i64;
    let mut compactions = 0_i64;
    let mut models: BTreeSet<String> = BTreeSet::new();
    let mut deduped: Vec<(i64, &str, &str)> = Vec::new();

    for (event, segment) in prepared_events(&input, &mut coverage) {
        let record = event.extra;
        if !record.is_object() {
            coverage.unknown("codex_extra_missing");
            deduped.push((event.idx, event.role, event.content));
            continue;
        }
        if let Some(model) = record
            .get("model")
            .or_else(|| record.pointer("/response/model"))
            .or_else(|| record.pointer("/cass/model"))
            .and_then(Value::as_str)
            .filter(|m| !m.is_empty())
        {
            models.insert(model.to_string());
        }
        let (payload, wrapped) = payload_of(record);
        let kind = payload.get("type").and_then(Value::as_str);
        if is_boundary(record, &payload) {
            compactions += 1;
            continue;
        }
        if CONTROLS.contains(&record.get("type").and_then(Value::as_str).unwrap_or("")) {
            continue;
        }
        if !wrapped {
            // An unwrappable codex record is the extra-compaction envelope:
            // archives indexed while the compaction layers were live (cass
            // ≥16 MiB / FAD ≥32 MiB, removed 2026-11-08) carry per-message
            // extras reduced to the `{"cass":{…}}` skeleton or its empty
            // `{}` product — the connector only ever emits
            // `response_item`/`event_msg` records, and both require a payload
            // object to become messages, so a record without that shape IS
            // the skeleton (the message text itself stayed in `content`).
            // The envelope is a known shape: the partial `extra_compacted`
            // verdict is the truthful one, never an unknown-shape claim.
            // Legacy-format rollout items (`{"role":…,"content":…}`) are not
            // envelopes and keep the unknown verdict.
            if is_compacted_extra(record) || record.as_object().is_some_and(|map| map.is_empty()) {
                coverage.mark_compacted("extra_compacted");
            } else {
                coverage.unknown("codex_shape_unknown");
            }
            deduped.push((event.idx, event.role, event.content));
            continue;
        }
        deduped.push((event.idx, event.role, event.content));
        if CALLS.contains(&kind.unwrap_or("")) {
            call(
                event,
                &payload,
                segment.as_deref(),
                &mut coverage,
                &mut tools,
            );
        } else if RESULTS.contains(&kind.unwrap_or("")) {
            result(
                event,
                &payload,
                segment.as_deref(),
                &mut coverage,
                &mut tools,
            );
        } else if kind == Some("reasoning") {
            reasoning(&payload, &mut coverage, &mut thinking);
        } else if kind == Some("agent_reasoning") {
            agent_reasoning(&payload, &mut coverage, &mut thinking);
        } else if (kind == Some("message") || kind.is_none())
            && record.get("type").and_then(Value::as_str) == Some("response_item")
        {
            message(&payload, &mut coverage);
        } else if kind == Some("agent_message") {
            agent_message_text(&payload, &mut coverage);
        } else if matches!(kind, Some("user_message" | "error" | "event_msg")) {
            event_text(&payload, &mut coverage);
        } else if CONTROLS.contains(&kind.unwrap_or("")) {
            continue;
        } else {
            coverage.unknown("codex_payload_unknown");
        }
    }

    // Codex subagent evidence lives in `session_meta.payload.parent_thread_id`
    // / `source.subagent.thread_spawn`; normalization drops `session_meta`
    // records, so the canonical packet carries no parent evidence today
    // (packet-contract gap — the relation stays unknown, never fabricated).
    let is_subagent = packet
        .payload
        .metadata_json
        .get("is_subagent")
        .and_then(Value::as_bool);
    let parent_external_id = packet
        .payload
        .metadata_json
        .get("parent_id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .map(str::to_string);

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
        // FAD's codex connector emits no boundary records; a zero would be
        // fabricated evidence, so the count stays NULL with the reason.
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
