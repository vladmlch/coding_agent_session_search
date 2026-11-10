//! Cursor session facts from harvested bubble extras. The pinned FAD
//! connector stores the raw bubble verbatim in `extra_json` for composer
//! sources and an empty object for agent-transcript/draft-fallback sources
//! — no-record shapes whose delivered text lives entirely in `content` and
//! whose tool_use blocks the connector lifts into `invocations`, so their
//! text/block inventories are complete and only thinking stays
//! unverifiable (see the `bubble.is_empty()` arm).
//! Behavioral port of the oracle `facts/cursor.py` + the native
//! `providers/cursor/{delta,harvesting}.py` tool/thinking arms: real message
//! shapes only — thinking and exit status that the bubble never recorded
//! stay unknown, never fabricated.
//!
//! `toolFormerCalls` entries pair a call with its inline output on recorded
//! `callId`/`id` evidence alone; conflicting recorded aliases and conflicting
//! same-id signatures are ambiguous and never merge. Exit codes are never
//! recorded by cursor bubbles, so `tool_exit` stays unavailable.

use std::collections::BTreeSet;

use serde_json::Value;

use crate::model::conversation_packet::ConversationPacket;

use super::classification::{self, ClassifyCall};
use super::{
    CoverageBuilder, Event, ProviderExtraction, RecordedCall, RecordedResult, ToolCollector,
    events, json_text, text_id,
};

const EXTRACTOR_REVISION: &str = "cursor-facts-v2";

/// Canonical tool-name map (`providers/cursor/delta.py::CURSOR_TOOL_NAME_MAP`).
fn cursor_tool_name(raw: &str) -> &str {
    match raw {
        "run_terminal_command_v2" | "run_terminal_command" => "bash",
        "read_file_v2" => "read_file",
        "edit_file_v2" => "edit_file",
        "ripgrep_raw_search" => "grep_search",
        "glob_file_search" => "find_by_name",
        "task_v2" => "subagent",
        other => other,
    }
}

/// Recorded call-id aliases (oracle `_identifier` over `_CALL_IDS`): exactly
/// one distinct recorded value is required; zero or conflicting aliases are
/// both unusable, but only the latter is a conflict.
const CALL_ID_KEYS: &[&str] = &[
    "id",
    "callId",
    "call_id",
    "tool_call_id",
    "tool_use_id",
    "toolUseId",
    "toolCallId",
    "tool_id",
];

enum RecordedId {
    Missing,
    One(String),
    Conflicting,
}

fn recorded_call_id(value: &Value) -> RecordedId {
    let mut found: Option<String> = None;
    let mut conflicting = false;
    for key in CALL_ID_KEYS {
        if let Some(candidate) = text_id(value.get(*key)) {
            match &found {
                None => found = Some(candidate),
                Some(existing) if *existing == candidate => {}
                Some(_) => conflicting = true,
            }
        }
    }
    match (found, conflicting) {
        (_, true) => RecordedId::Conflicting,
        (Some(id), false) => RecordedId::One(id),
        (None, false) => RecordedId::Missing,
    }
}

/// Oracle `_text`: recorded output kept verbatim or as canonical JSON.
fn recorded_output_text(call: &Value) -> Option<String> {
    for key in ["output", "result", "toolResult"] {
        if let Some(value) = call.get(key) {
            return match value {
                Value::String(text) => Some(text.clone()),
                Value::Null => None,
                other => Some(json_text(other)),
            };
        }
    }
    None
}

/// Mark a field set unavailable the way `ToolCollector::ambiguous` does —
/// a conflicting event taints every tool-derived scope.
fn mark_conflicting(coverage: &mut CoverageBuilder) {
    coverage.mark(
        &[
            "tools",
            "tool_exit",
            "tool_error",
            "files",
            "mcp",
            "block_inventory",
            "text_inventory",
        ],
        "conflicting_event",
        "unavailable",
    );
}

/// One `toolFormerCalls[]` entry: call plus its inline recorded output.
fn former_call(
    event: &Event,
    call: &Value,
    coverage: &mut CoverageBuilder,
    tools: &mut ToolCollector,
) {
    let raw_name = call
        .get("name")
        .or_else(|| call.get("tool"))
        .and_then(Value::as_str);
    let Some(raw_name) = raw_name.filter(|name| !name.is_empty()) else {
        coverage.mark(&["tools"], "cursor_tool_name_missing", "partial");
        return;
    };
    coverage.evidence("tools");
    let call_id = match recorded_call_id(call) {
        RecordedId::One(id) => id,
        RecordedId::Missing => {
            coverage.mark(
                &["tools", "tool_exit", "tool_error"],
                "cursor_tool_call_identity_missing",
                "partial",
            );
            return;
        }
        RecordedId::Conflicting => {
            mark_conflicting(coverage);
            return;
        }
    };
    let arguments = ["args", "parameters", "params"]
        .iter()
        .find_map(|key| call.get(*key).cloned());
    tools.call(RecordedCall {
        event_idx: event.idx,
        message_id: event.message_id,
        raw_call_id: call_id.clone(),
        call_id: call_id.clone(),
        name: cursor_tool_name(raw_name).to_string(),
        arguments,
    });
    // Inline recorded output: `output`|`result`|`toolResult`, error flag via
    // `isError`|`rejected` — recorded only, never defaulted.
    let output_present = ["output", "result", "toolResult"]
        .iter()
        .any(|key| call.get(*key).is_some_and(|value| !value.is_null()));
    let is_error = call
        .get("isError")
        .or_else(|| call.get("rejected"))
        .and_then(Value::as_bool);
    if output_present || is_error.is_some() {
        tools.result(RecordedResult {
            event_idx: event.idx,
            call_id,
            text: recorded_output_text(call),
            // Cursor bubbles never record exit codes.
            exit_code: None,
            is_error,
        });
    }
}

/// Thinking evidence in real bubbles: `allThinkingBlocks`, `thinking`, and
/// `capabilityType == 30` text. Only recorded text counts.
fn thinking_blocks(bubble: &Value, coverage: &mut CoverageBuilder) -> i64 {
    let mut count = 0_i64;
    let mut unreadable = false;
    if let Some(blocks) = bubble.get("allThinkingBlocks") {
        if let Some(items) = blocks.as_array() {
            for item in items {
                let text = if let Some(text) = item.as_str() {
                    Some(text)
                } else if item.is_object() {
                    item.get("text")
                        .or_else(|| item.get("content"))
                        .or_else(|| item.get("thinking"))
                        .and_then(Value::as_str)
                } else {
                    None
                };
                match text.map(str::trim).filter(|text| !text.is_empty()) {
                    Some(_) => count += 1,
                    None => unreadable = true,
                }
            }
        } else {
            unreadable = true;
        }
    }
    if let Some(thinking) = bubble.get("thinking") {
        let text = if thinking.is_object() {
            thinking
                .get("text")
                .or_else(|| thinking.get("content"))
                .and_then(Value::as_str)
        } else {
            thinking.as_str()
        };
        match text.map(str::trim).filter(|text| !text.is_empty()) {
            Some(_) => count += 1,
            None => unreadable = true,
        }
    }
    if bubble.get("capabilityType").and_then(Value::as_i64) == Some(30) {
        let text = bubble
            .get("text")
            .or_else(|| bubble.get("rawText"))
            .and_then(Value::as_str);
        match text.map(str::trim).filter(|text| !text.is_empty()) {
            Some(_) => count += 1,
            None => unreadable = true,
        }
    }
    if count > 0 {
        coverage.evidence("thinking");
    }
    if unreadable {
        coverage.mark(
            &["thinking", "text_inventory"],
            "cursor_thinking_not_recorded",
            "partial",
        );
    }
    count
}

fn bubble_models(bubble: &Value, models: &mut BTreeSet<String>) {
    for value in [
        bubble.get("modelType"),
        bubble.get("model"),
        bubble.pointer("/modelInfo/modelName"),
    ]
    .into_iter()
    .flatten()
    {
        if let Some(model) = value.as_str().filter(|model| !model.is_empty()) {
            models.insert(model.to_string());
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
        if !event.content.is_empty() {
            coverage.evidence("chat");
        }
        let extra = event.extra;
        let Some(bubble) = extra.as_object() else {
            coverage.mark(
                &["tools", "thinking", "block_inventory", "text_inventory"],
                "cursor_extra_absent",
                "partial",
            );
            continue;
        };
        if bubble.is_empty() {
            // The pinned connector's no-record shapes: agent-transcript
            // messages and composer draft fallback messages persist no raw
            // record but deliver every text-bearing field into `content`
            // (transcripts flatten text blocks and render tool_use blocks)
            // and lift every tool_use block into `invocations`, which the
            // ingest merge archives under `cass.invocations`. A `{}` extra
            // therefore proves the record held no tool_use blocks, and the
            // event's text/block inventories are complete by construction —
            // the projection derives the text part from `content`. Only
            // thinking stays unverifiable: a dropped thinking block leaves
            // no evidence anywhere in the archive.
            if event.content.is_empty() {
                // Unknown shape — every `{}` producer the connector has
                // delivers non-empty content — keeps the full mark.
                coverage.mark(
                    &["tools", "thinking", "block_inventory", "text_inventory"],
                    "cursor_extra_absent",
                    "partial",
                );
            } else {
                coverage.mark(&["thinking"], "cursor_extra_absent", "partial");
            }
            continue;
        }
        bubble_models(extra, &mut models);
        thinking += thinking_blocks(extra, &mut coverage);
        if let Some(calls) = extra.get("toolFormerCalls") {
            if let Some(items) = calls.as_array() {
                for item in items {
                    if item.is_object() {
                        former_call(event, item, &mut coverage, &mut tools);
                    } else {
                        coverage.mark(&["tools"], "cursor_tool_call_malformed", "partial");
                    }
                }
            } else {
                coverage.mark(&["tools"], "cursor_tool_call_malformed", "partial");
            }
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
    let thinking_block_count = if thinking > 0 {
        Some(thinking)
    } else {
        // Cursor bubbles without thinking evidence cannot certify zero.
        coverage.mark(&["thinking"], "cursor_thinking_not_recorded", "unavailable");
        None
    };
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
        thinking_block_count,
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
