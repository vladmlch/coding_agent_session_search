//! Copilot (VS Code) session facts from connector-built extras. The
//! pinned FAD connector (copilot_vscode) builds the extras itself and
//! CASS's ingest seam archives `NormalizedMessage.invocations` under
//! `extra.cass.invocations`, so the persisted shapes are:
//!   - user rows: `{request_id?}`;
//!   - assistant rows: `{request_id?, model_id?, cass}` with
//!     `cass = {token_usage?, token_usage_scope?, tool_call_count?,
//!     invocations?}` — verified against the 6 persisted copilot
//!     messages (key structure only here, synthetic text in tests).
//!
//! Connector contract (pinned rev ad5fb23b, `response_content`):
//!   - assistant content joins the markdown part texts; `thinking`,
//!     `inlineReference`, `textEditGroup`, `codeblockUri`, … parts are
//!     dropped as "presentation/telemetry" (explicit source comment) —
//!     thinking text never reaches the archive, so thinking is
//!     `unavailable` and its block count is not derivable;
//!   - `toolInvocationSerialized` parts become invocations (name =
//!     `toolId` — a part without a toolId is dropped silently — call_id
//!     = `toolCallId`, arguments = `toolSpecificData.rawInput`);
//!   - no tool result is ever archived: results ride the response parts
//!     the connector does not keep, so every call is unpaired and the
//!     missing result text is user-visible text dropped at the
//!     connector — the `tool_result_missing` scopes are never narrowed;
//!   - `model_id` is the turn's model and the only model evidence;
//!   - copilot records no compaction boundaries.
//!
//! The inner `cass` keys are not gated: the envelope is CASS-authored
//! accounting (token usage, the seam's archived invocations) and its
//! keys carry no user-visible text.

use std::collections::BTreeSet;

use serde_json::Value;

use crate::model::conversation_packet::ConversationPacket;

use super::classification::{self, ClassifyCall};
use super::{
    CORE_FIELDS, CoverageBuilder, Event, ProviderExtraction, RecordedCall, ToolCollector, events,
    text_id,
};

const EXTRACTOR_REVISION: &str = "copilot-facts-v1";

/// The connector-built user extra: the turn's request id.
const USER_KEYS: &[&str] = &["request_id"];
/// The connector-built assistant extra: request id, the turn's model,
/// and the token-accounting envelope the ingest seam joins.
const ASSISTANT_KEYS: &[&str] = &["request_id", "model_id", "cass"];

/// `true` when every top-level extra key is one the connector writes.
fn keys_known(extra: &Value, allowed: &[&str]) -> bool {
    extra
        .as_object()
        .is_some_and(|map| map.keys().all(|key| allowed.contains(&key.as_str())))
}

/// Record one archived invocation; mirrors the search-regions invocation
/// arm's `kind == "tool"` filter.
fn record_invocation(
    event: &Event,
    item: &Value,
    coverage: &mut CoverageBuilder,
    tools: &mut ToolCollector,
) {
    if !item.is_object() || item.get("kind").and_then(Value::as_str).unwrap_or("tool") != "tool" {
        return;
    }
    let Some(name) = text_id(item.get("name")) else {
        coverage.mark(&["tools"], "copilot_tool_name_missing", "partial");
        return;
    };
    coverage.evidence("tools");
    let Some(call_id) = text_id(item.get("call_id")) else {
        coverage.mark(
            &["tools", "tool_exit", "tool_error"],
            "copilot_tool_call_identity_missing",
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
        arguments: item
            .get("arguments")
            .or_else(|| item.get("args"))
            .or_else(|| item.get("input"))
            .cloned(),
    });
}

pub(crate) fn extract(packet: &ConversationPacket) -> ProviderExtraction {
    let input = events(packet);
    let mut coverage = CoverageBuilder::new();
    // Tool results are never archived, so an unpaired call means
    // user-visible result text was dropped — the default
    // `tool_result_missing` scopes stand.
    let mut tools = ToolCollector::new();
    let mut models: BTreeSet<String> = BTreeSet::new();
    let mut deduped: Vec<(i64, &str, &str)> = Vec::new();

    for event in &input {
        deduped.push((event.idx, event.role, event.content));
        let extra = event.extra;
        if !extra.is_object() {
            coverage.unknown("copilot_extra_missing");
            continue;
        }
        if !event.content.is_empty() {
            coverage.evidence("chat");
        }
        match event.role {
            "agent" | "assistant" => {
                if !keys_known(extra, ASSISTANT_KEYS) {
                    // A key the connector never writes is a contract
                    // change — drift, never a shape guess.
                    coverage.mark(CORE_FIELDS, "copilot_record_unknown", "partial");
                }
                if let Some(model) = extra
                    .get("model_id")
                    .and_then(Value::as_str)
                    .filter(|model| !model.is_empty())
                {
                    models.insert(model.to_string());
                }
                // Thinking parts are dropped by the connector (explicit
                // source comment): their text never reaches the archive.
                coverage.mark(
                    &["thinking"],
                    "copilot_thinking_dropped_by_connector",
                    "unavailable",
                );
                // The raw response parts are not archived — only the
                // joined text and the invocations survive.
                coverage.mark(
                    &["text_inventory", "block_inventory"],
                    "copilot_response_parts_dropped",
                    "partial",
                );
                for item in extra
                    .pointer("/cass/invocations")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    record_invocation(event, item, &mut coverage, &mut tools);
                }
            }
            "user" => {
                if !keys_known(extra, USER_KEYS) {
                    coverage.mark(CORE_FIELDS, "copilot_record_unknown", "partial");
                }
            }
            // The connector emits only user and assistant records; any
            // other role is a contract change — drift, never a guess.
            _ => coverage.mark(CORE_FIELDS, "copilot_record_unknown", "partial"),
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
    // Copilot records no compaction boundaries.
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
        // Thinking blocks are dropped by the connector, so their count
        // is not derivable.
        thinking_block_count: None,
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
