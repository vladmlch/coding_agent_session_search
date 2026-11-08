//! Gemini session facts from raw-record extras. The pinned FAD connector
//! archives the raw gemini record verbatim in `extra_json` (`content`
//! parts, `thoughts`, `toolCalls` with their inline `result` arrays,
//! `model`, `tokens`) and inlines every text-bearing field into the
//! message content — `[Thinking] subject description`, `[Tool: name]`,
//! `[Tool result: name] output` and `[Error] ` for failed/cancelled calls
//! (`normalize_gemini_message_content`). Tool arguments are the one text
//! class kept out of the delivered content; they index as tool regions
//! (search_regions.rs, REGION_VERSION 3).
//!
//! Verified against the pinned connector's real output (52 persisted
//! gemini messages; key/type structure only here, synthetic text in
//! tests):
//!   - user records: extra keys {content, id, timestamp, type}, content
//!     parts are strings or `{type: "text", text}` objects;
//!   - assistant records (`type` `gemini`/`model`): extra keys
//!     {content, id, model, thoughts, timestamp, tokens, toolCalls, type}
//!     plus `cass` (the ingest seam's invocation merge);
//!   - `toolCalls[]` entries carry `id`, `name`, `args` (objects),
//!     `result[]` of `functionResponse.response.output` strings — or
//!     `response.error` for failed/cancelled calls — plus display
//!     copies (`description`, `displayName`, `resultDisplay`,
//!     `renderOutputAsMarkdown`) that render the args/result and are
//!     never indexed as new text, and a lifecycle `status` (`success`
//!     observed; mapped `error` → error verdict) that is the recorded
//!     success/error flag — an exit code is never fabricated;
//!   - `compact_message_extra` (>=32 MiB session files) replaces the
//!     raw record with {model, tokens, attachments} before the ingest
//!     seam archives the connector's invocations under
//!     `cass.invocations` — evidence survives, the raw record does not,
//!     so those records are partial (`extra_compacted`), never unknown;
//!   - the connector normalizes no compaction boundary records.
//!
//! A raw `toolCalls[]` entry with no `result` proves the result never
//! existed at the source (the record is archived verbatim), so an
//! unpaired call taints `tools` only, never the text inventory.

use std::collections::BTreeSet;

use serde_json::Value;

use crate::model::conversation_packet::ConversationPacket;

use super::classification::{self, ClassifyCall};
use super::{
    CORE_FIELDS, CoverageBuilder, Event, ProviderExtraction, RecordedCall, RecordedResult,
    ToolCollector, events, is_compacted_extra, text_id,
};

const EXTRACTOR_REVISION: &str = "gemini-facts-v1";

/// Record types the connector maps to the assistant role
/// (`normalize_message_role`: `model`/`gemini` → assistant).
const ASSISTANT_TYPES: &[&str] = &["gemini", "model"];

/// Extra keys of a raw user/system record (archived verbatim).
const RAW_USER_KEYS: &[&str] = &["content", "id", "timestamp", "type"];
/// Extra keys of a raw assistant record, plus `cass` (the ingest seam's
/// invocation merge).
const RAW_ASSISTANT_KEYS: &[&str] = &[
    "content", "id", "model", "thoughts", "timestamp", "tokens", "toolCalls", "type", "cass",
];
/// Extra keys after `compact_message_extra` replaced the raw record
/// (>=32 MiB session files); the ingest seam may add `cass.invocations`.
const COMPACTED_KEYS: &[&str] = &["model", "tokens", "attachments", "cass"];

fn keys_known(extra: &Value, allowed: &[&str]) -> bool {
    extra
        .as_object()
        .is_some_and(|map| map.keys().all(|key| allowed.contains(&key.as_str())))
}

/// `true` when `compact_message_extra` replaced the raw record with the
/// compacted skeleton (>=32 MiB session files) — including the
/// post-merge single-`cass`-key envelope `is_compacted_extra` detects.
fn is_gemini_compacted(extra: &Value) -> bool {
    if is_compacted_extra(extra) {
        return true;
    }
    let Some(map) = extra.as_object() else {
        return false;
    };
    !map.contains_key("type")
        && !map.contains_key("content")
        && map.keys().all(|key| COMPACTED_KEYS.contains(&key.as_str()))
}

/// Recorded result text of one `toolCalls[]` entry: `result[]` parts are
/// `functionResponse.response.output` strings, bare strings/`text`
/// values, or `functionResponse.response.error` strings for
/// failed/cancelled calls (the connector prefixes those with `[Error] `
/// in the delivered content). `(text, explicit_error)` — the error text
/// is recorded result text AND the recorded error verdict. Shared with
/// the search-regions gemini arm so the facts and region lanes derive
/// identical text and verdicts.
pub(crate) fn gemini_call_result_text(result: &Value) -> (String, bool) {
    let mut parts: Vec<String> = Vec::new();
    let mut explicit_error = false;
    fn collect(value: &Value, parts: &mut Vec<String>, error: &mut bool) {
        match value {
            Value::Array(items) => {
                for item in items {
                    collect(item, parts, error);
                }
            }
            Value::String(text) => {
                if !text.trim().is_empty() {
                    parts.push(text.clone());
                }
            }
            Value::Object(_) => {
                if let Some(text) = value
                    .pointer("/functionResponse/response/output")
                    .or_else(|| value.get("text"))
                    .and_then(Value::as_str)
                    .filter(|text| !text.trim().is_empty())
                {
                    parts.push(text.to_owned());
                }
                if let Some(text) = value
                    .pointer("/functionResponse/response/error")
                    .and_then(Value::as_str)
                    .filter(|text| !text.trim().is_empty())
                {
                    parts.push(text.to_owned());
                    *error = true;
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
        }
    }
    collect(result, &mut parts, &mut explicit_error);
    (parts.join("\n"), explicit_error)
}

/// The recorded `status` lifecycle verdict: `success` records success,
/// `error` records failure (mirrors the antigravity DONE/ERROR mapping);
/// any other value records nothing. Shared with the search-regions gemini
/// arm (same verdict in facts and regions).
pub(crate) fn gemini_status_verdict(status: Option<&str>) -> Option<bool> {
    match status {
        Some("success") => Some(false),
        Some("error") => Some(true),
        _ => None,
    }
}

/// One raw `toolCalls[]` entry (oracle `_structured_event` tool arm).
fn record_call(
    event: &Event,
    call: &Value,
    coverage: &mut CoverageBuilder,
    tools: &mut ToolCollector,
) {
    let Some(name) = text_id(call.get("name")) else {
        coverage.mark(&["tools"], "gemini_tool_name_missing", "partial");
        return;
    };
    coverage.evidence("tools");
    let Some(call_id) = text_id(call.get("id")) else {
        coverage.mark(
            &["tools", "tool_exit", "tool_error"],
            "gemini_tool_call_identity_missing",
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
        arguments: call.get("args").cloned(),
    });
    // Inline recorded result: absent/null proves no result ever existed
    // (the raw record is archived verbatim) and stays unpaired.
    if let Some(result) = call.get("result").filter(|value| !value.is_null()) {
        let (text, explicit_error) = gemini_call_result_text(result);
        let is_error = if explicit_error {
            Some(true)
        } else {
            gemini_status_verdict(call.get("status").and_then(Value::as_str))
        };
        tools.result(RecordedResult {
            event_idx: event.idx,
            call_id: text_id(call.get("id")).unwrap_or_default(),
            text: (!text.is_empty()).then_some(text),
            exit_code: None,
            is_error,
        });
    }
}

/// One raw `thoughts[]` entry: subject/description strings, at least one
/// non-empty (the connector skips thought records with neither).
fn thought(entry: &Value, coverage: &mut CoverageBuilder) -> bool {
    let counted = entry.is_object()
        && entry.get("subject").is_none_or(Value::is_string)
        && entry.get("description").is_none_or(Value::is_string)
        && [entry.get("subject"), entry.get("description")]
            .into_iter()
            .flatten()
            .any(|value| value.as_str().is_some_and(|text| !text.trim().is_empty()));
    if counted {
        coverage.evidence("thinking");
    } else {
        coverage.mark(
            &["thinking", "text_inventory"],
            "gemini_thought_malformed",
            "partial",
        );
    }
    counted
}

/// The raw content parts of one record: strings and `{type: "text"}`
/// parts are the verified shapes; anything else is drift on the block
/// inventory.
fn content_parts(extra: &Value, coverage: &mut CoverageBuilder) {
    match extra.get("content") {
        Some(Value::String(text)) if !text.trim().is_empty() => {
            coverage.evidence("chat");
        }
        Some(Value::String(_)) | None => {}
        Some(Value::Array(parts)) => {
            for part in parts {
                let known = part.as_str().is_some()
                    || part.get("text").and_then(Value::as_str).is_some();
                if !known {
                    coverage.mark(&["block_inventory"], "gemini_part_unknown", "partial");
                }
            }
        }
        Some(_) => coverage.mark(&["block_inventory"], "gemini_part_unknown", "partial"),
    }
}

/// `cass.invocations[]` evidence on a compacted record (the ingest seam's
/// merge of the connector's lifted invocations) — the only call evidence
/// the compacted skeleton keeps.
fn invocation_evidence(
    event: &Event,
    extra: &Value,
    coverage: &mut CoverageBuilder,
    tools: &mut ToolCollector,
) {
    let Some(items) = extra.pointer("/cass/invocations") else {
        return;
    };
    match items.as_array() {
        Some(items) => {
            for item in items {
                if !item.is_object() {
                    coverage.mark(&["tools"], "gemini_tool_field_malformed", "partial");
                    continue;
                }
                let Some(name) = text_id(item.get("name")) else {
                    coverage.mark(&["tools"], "gemini_tool_name_missing", "partial");
                    continue;
                };
                coverage.evidence("tools");
                let Some(call_id) = text_id(item.get("call_id")) else {
                    coverage.mark(
                        &["tools", "tool_exit", "tool_error"],
                        "gemini_tool_call_identity_missing",
                        "partial",
                    );
                    continue;
                };
                tools.call(RecordedCall {
                    event_idx: event.idx,
                    message_id: event.message_id,
                    raw_call_id: call_id.clone(),
                    call_id,
                    name,
                    arguments: item
                        .get("arguments")
                        .or_else(|| item.get("args"))
                        .cloned(),
                });
            }
        }
        None if items.is_null() => {}
        None => coverage.mark(&["tools"], "gemini_tool_field_malformed", "partial"),
    }
}

pub(crate) fn extract(packet: &ConversationPacket) -> ProviderExtraction {
    let input = events(packet);
    let mut coverage = CoverageBuilder::new();
    // A result absent from the verbatim raw record never existed at the
    // source, so an unpaired call loses no session text.
    let mut tools = ToolCollector::new().unpaired_results_carry_no_text();
    let mut thinking = 0_i64;
    let mut models: BTreeSet<String> = BTreeSet::new();
    let mut deduped: Vec<(i64, &str, &str)> = Vec::new();

    for event in &input {
        deduped.push((event.idx, event.role, event.content));
        let extra = event.extra;
        if !extra.is_object() {
            coverage.unknown("gemini_extra_missing");
            continue;
        }
        if !event.content.is_empty() {
            coverage.evidence("chat");
        }
        if let Some(model) = text_id(extra.get("model")) {
            models.insert(model);
        }
        if is_gemini_compacted(extra) {
            coverage.mark_compacted("extra_compacted");
            invocation_evidence(event, extra, &mut coverage, &mut tools);
            continue;
        }
        let record_type = extra.get("type").and_then(Value::as_str).unwrap_or("");
        let assistant = ASSISTANT_TYPES.contains(&record_type);
        if assistant {
            match extra.get("thoughts") {
                Some(Value::Array(items)) => {
                    for item in items {
                        thinking += u64::from(thought(item, &mut coverage)) as i64;
                    }
                }
                Some(_) => coverage.mark(
                    &["thinking", "text_inventory"],
                    "gemini_thought_malformed",
                    "partial",
                ),
                None => {}
            }
            match extra.get("toolCalls") {
                Some(Value::Array(items)) => {
                    for item in items {
                        if item.is_object() {
                            record_call(event, item, &mut coverage, &mut tools);
                        } else {
                            coverage
                                .mark(&["tools"], "gemini_tool_field_malformed", "partial");
                        }
                    }
                }
                Some(_) => coverage.mark(&["tools"], "gemini_tool_field_malformed", "partial"),
                None => {}
            }
        }
        content_parts(extra, &mut coverage);
        // Connector drift: keys outside the verified contract keep the
        // per-record degradation instead of a shape guess.
        let known = if assistant {
            keys_known(extra, RAW_ASSISTANT_KEYS)
        } else {
            keys_known(extra, RAW_USER_KEYS)
        };
        if !known {
            coverage.mark(CORE_FIELDS, "gemini_record_unknown", "partial");
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
