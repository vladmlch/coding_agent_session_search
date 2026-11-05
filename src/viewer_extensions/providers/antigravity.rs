//! Antigravity session facts: `agy_type` envelopes, `cass.invocations`
//! normalized calls, `TOOL_OUTPUT`/`VIEW_FILE`/`RUN_COMMAND` results and
//! recorded-only exit/error evidence. Behavioral port of the oracle
//! `facts/antigravity.py` over the normalized CASS packet.
//!
//! The pinned FAD connector emits sparse extras — `agy_type`, `thinking`,
//! `settings_change`, `cass.invocations` (PLANNER_RESPONSE calls) — while raw
//! transcript `tool_calls`/`tool_outputs`/`call_id`/`exit_code` survive only
//! where the connector recorded them. Unlinked outputs are never positionally
//! paired; missing identifiers stay partial, never fabricated.
//!
//! Note: `is_compacted_extra` is deliberately NOT used here — the
//! `{"cass":{"invocations":[..]}}` envelope is real Antigravity evidence, and
//! ingest extra-compaction is codex-only (`should_compact_connector_extra`).

use std::collections::BTreeSet;

use serde_json::Value;

use crate::model::conversation_packet::ConversationPacket;

use super::classification::{self, ClassifyCall};
use super::{
    CORE_FIELDS, CoverageBuilder, Event, ProviderExtraction, RecordedCall, RecordedResult,
    ToolCollector, events, json_text, text_id,
};

const EXTRACTOR_REVISION: &str = "antigravity-facts-v1";
const SUPPORTED_KINDS: &[&str] = &[
    "USER_INPUT",
    "PLANNER_RESPONSE",
    "TOOL_OUTPUT",
    "VIEW_FILE",
    "RUN_COMMAND",
    "ERROR_MESSAGE",
    "error",
    "SYSTEM_MESSAGE",
    "CONVERSATION_HISTORY",
];
const TOOL_KINDS: &[&str] = &["TOOL_OUTPUT", "VIEW_FILE", "RUN_COMMAND"];
const PARTIAL_TOOL_FIELDS: &[&str] = &["tools", "tool_exit", "tool_error"];

/// Recorded record kind: `agy_type` (canonical envelope) or raw `type`/`kind`.
fn agy_event_kind(extra: &Value) -> Option<&str> {
    let kind = extra
        .get("agy_type")
        .or_else(|| extra.get("type"))
        .or_else(|| extra.get("kind"))?
        .as_str()?;
    SUPPORTED_KINDS.contains(&kind).then_some(kind)
}

/// Explicit recorded status (oracle `_error`): an integer exit code can
/// establish an error; `status == "ERROR"` forces error; a missing flag never
/// invents a code.
fn agy_status(value: &Value) -> (Option<i64>, Option<bool>) {
    let exit_code = value.get("exit_code").and_then(Value::as_i64);
    let mut is_error = value.get("is_error").and_then(Value::as_bool);
    if value.get("status").and_then(Value::as_str) == Some("ERROR") {
        is_error = Some(true);
    }
    if let Some(code) = exit_code {
        is_error = Some(is_error == Some(true) || code != 0);
    }
    (exit_code, is_error)
}

/// `output`/`content` as recorded text (oracle `_text`): strings verbatim,
/// other JSON canonicalized, absent stays absent.
fn recorded_output_text(value: &Value) -> Option<String> {
    let candidate = value.get("output").or_else(|| value.get("content"))?;
    match candidate {
        Value::String(text) => Some(text.clone()),
        Value::Null => None,
        other => Some(json_text(other)),
    }
}

/// A recorded call (`tool_calls[]` item or `cass.invocations[]` entry).
fn record_call(
    event: &Event,
    value: &Value,
    coverage: &mut CoverageBuilder,
    tools: &mut ToolCollector,
) {
    let Some(name) = text_id(value.get("name")) else {
        coverage.mark(&["tools"], "agy_tool_name_missing", "partial");
        return;
    };
    coverage.evidence("tools");
    let arguments = value
        .get("args")
        .or_else(|| value.get("arguments"))
        .or_else(|| value.get("input"))
        .cloned();
    let Some(call_id) = text_id(
        value
            .get("call_id")
            .or_else(|| value.get("id"))
            .or_else(|| value.get("tool_use_id")),
    ) else {
        coverage.mark(
            PARTIAL_TOOL_FIELDS,
            "agy_tool_call_identity_missing",
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
        arguments,
    });
}

/// A recorded output (`tool_outputs[]` item or a `agy_type` tool record).
fn record_output(
    event: &Event,
    value: &Value,
    coverage: &mut CoverageBuilder,
    tools: &mut ToolCollector,
) {
    let (exit_code, is_error) = agy_status(value);
    let text = recorded_output_text(value);
    coverage.evidence("tools");
    let Some(call_id) = text_id(value.get("call_id").or_else(|| value.get("tool_use_id"))) else {
        coverage.mark(
            PARTIAL_TOOL_FIELDS,
            "agy_tool_result_identity_missing",
            "partial",
        );
        return;
    };
    tools.result(RecordedResult {
        event_idx: event.idx,
        call_id,
        text,
        exit_code,
        is_error,
    });
}

/// One verified agy event (oracle `_structured_event`). Thinking and
/// `cass.invocations` evidence are handled by [`cass_evidence`] — the FAD
/// records carrying them have no `agy_type` to verify.
fn structured_event(
    event: &Event,
    kind: &str,
    coverage: &mut CoverageBuilder,
    tools: &mut ToolCollector,
) {
    let extra = event.extra;
    // The canonical `agy_type` envelope loses the raw record structure.
    if extra.get("agy_type").is_some() {
        coverage.mark(
            &["block_inventory", "text_inventory", "tools"],
            "agy_canonical_envelope",
            "partial",
        );
    }
    let content = extra
        .get("content")
        .filter(|value| !value.is_null())
        .cloned()
        .unwrap_or_else(|| Value::String(event.content.to_string()));

    if TOOL_KINDS.contains(&kind) {
        let has_output_list = extra.get("tool_outputs").is_some_and(Value::is_array);
        let has_id = text_id(extra.get("call_id"))
            .or_else(|| text_id(extra.get("id")))
            .or_else(|| text_id(extra.get("tool_use_id")))
            .is_some();
        // Oracle: `if content or _call_id(extra) is not None or
        // not isinstance(extra.get("tool_outputs"), list)`.
        let content_present =
            !content.is_null() && content.as_str().is_none_or(|text| !text.is_empty());
        if content_present || has_id || !has_output_list {
            let mut result = extra.clone();
            if let Value::Object(map) = &mut result {
                map.entry("content".to_string())
                    .or_insert_with(|| Value::String(event.content.to_string()));
            }
            record_output(event, &result, coverage, tools);
        }
    } else if !matches!(kind, "CONVERSATION_HISTORY" | "SYSTEM_MESSAGE") {
        match &content {
            Value::String(text) if !text.is_empty() => coverage.evidence("chat"),
            Value::String(_) | Value::Null => {}
            // Non-text content is explicitly described as opaque evidence.
            _ => coverage.mark(
                &[
                    "chat",
                    "tools",
                    "thinking",
                    "block_inventory",
                    "text_inventory",
                ],
                "agy_content_opaque",
                "partial",
            ),
        }
    }

    // Raw-transcript arms: `tool_calls`/`tool_outputs` lists survive only
    // where normalization recorded them verbatim.
    for field in ["tool_calls", "tool_outputs"] {
        let Some(items) = extra.get(field) else {
            continue;
        };
        let Some(items) = items.as_array() else {
            coverage.mark(&["tools"], "agy_tool_field_malformed", "partial");
            continue;
        };
        for item in items {
            if !item.is_object() {
                coverage.mark(&["tools"], "agy_tool_field_malformed", "partial");
            } else if field == "tool_calls" {
                record_call(event, item, coverage, tools);
            } else {
                record_output(event, item, coverage, tools);
            }
        }
    }

    // Artifact descriptors are recorded structure, not extracted content.
    if extra.get("artifacts").is_some_and(Value::is_array) {
        coverage.mark(
            &["artifacts"],
            "artifact_descriptors_not_extracted",
            "partial",
        );
    }
}

/// CASS-normalized evidence consumed regardless of the raw record kind:
/// the connector unwraps `thinking` onto every planner/MODEL record and
/// keeps planner invocations under `extra.cass.invocations` (FAD F3 fix —
/// `NormalizedMessage.invocations` are not part of the packet contract).
/// FAD `PLANNER_RESPONSE`/`USER_INPUT` extras carry no `agy_type`, so these
/// arms must run even when [`agy_event_kind`] cannot verify the record.
fn cass_evidence(
    event: &Event,
    extra: &Value,
    coverage: &mut CoverageBuilder,
    tools: &mut ToolCollector,
    thinking: &mut i64,
) {
    match extra.get("thinking") {
        Some(Value::String(text)) if !text.is_empty() => {
            *thinking += 1;
            coverage.evidence("thinking");
        }
        Some(Value::String(_)) | None => {}
        Some(_) => coverage.mark(
            &["thinking", "text_inventory"],
            "agy_thinking_malformed",
            "partial",
        ),
    }
    if let Some(invocations) = extra.pointer("/cass/invocations") {
        if let Some(items) = invocations.as_array() {
            for item in items {
                if item.is_object() {
                    record_call(event, item, coverage, tools);
                } else {
                    coverage.mark(&["tools"], "agy_tool_field_malformed", "partial");
                }
            }
        } else if !invocations.is_null() {
            coverage.mark(&["tools"], "agy_tool_field_malformed", "partial");
        }
    }
}

pub(crate) fn extract(packet: &ConversationPacket) -> ProviderExtraction {
    let input = events(packet);
    let mut coverage = CoverageBuilder::new();
    let mut tools = ToolCollector::new();
    let mut thinking = 0_i64;
    let mut models: BTreeSet<String> = BTreeSet::new();
    let mut deduped: Vec<(i64, &str, &str)> = Vec::new();

    for event in &input {
        deduped.push((event.idx, event.role, event.content));
        let extra = event.extra;
        if !extra.is_object() {
            coverage.unknown("agy_extra_missing");
            continue;
        }
        cass_evidence(event, extra, &mut coverage, &mut tools, &mut thinking);
        let Some(kind) = agy_event_kind(extra) else {
            // Unverifiable record: real message text but no structured
            // evidence (oracle routes these to the generic fallback, which
            // degrades every scope to partial). FAD USER_INPUT/PLANNER_RESPONSE
            // records land here — their `cass.*` evidence was already consumed.
            coverage.mark(CORE_FIELDS, "agy_record_unknown", "partial");
            continue;
        };
        structured_event(event, kind, &mut coverage, &mut tools);
    }

    // The connector lifts the detected model to conversation metadata.
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
    coverage.mark(
        &["compaction"],
        "boundary_records_not_normalized",
        "unavailable",
    );
    ProviderExtraction {
        extractor: EXTRACTOR_REVISION,
        coverage,
        tools,
        file_fact_count: Some(file_facts as i64),
        thinking_block_count: Some(thinking),
        compaction_count: None,
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
