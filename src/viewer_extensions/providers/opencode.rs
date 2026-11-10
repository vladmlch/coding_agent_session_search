//! OpenCode session facts from archived row extras. The pinned FAD
//! connector renders every opencode session row into a flat content string,
//! archives the row identity keys (`{message_id, session_id, type, seq}`),
//! and — since FAD F16 — archives the raw part records in `extra["parts"]`
//! in delivery order: the v2 assistant `content` items and the 1.x part
//! rows/files, verbatim, including every tool part's id, name, input,
//! status, results and error text.
//!
//! Connector contract (pinned rev bca17f7c + F16):
//!   - `user`/`synthetic` rows deliver `data.text` as user text;
//!   - `system` rows deliver `data.text` as system text;
//!   - `skill` rows deliver "[Skill: {name}]\n{text}" as tool-role text;
//!   - `shell` rows deliver "$ {command}\n{output}" as tool-role text;
//!   - `assistant` rows join their parts: text parts as-is (non-empty
//!     only), reasoning parts as "[Reasoning]\n{reasoning}", tool parts as
//!     "[Tool Output]\n{output}" (the state's text results joined for v2,
//!     the 1.x `state.output` otherwise) and a failed run's error text as
//!     "[Tool Error]\n{error}" — a plain string, or the error object's
//!     `message` (the measured real shape is `{message, type}`);
//!   - 1.x rows carry no `type` key — they dispatch on the packet role
//!     here (user/system render whole; assistant is the parts-bearing row);
//!   - completed `compaction` rows deliver "[Compaction Summary]
//!     \n{summary}" as assistant text; non-completed ones, `idle` rows and
//!     agent/model selection events are dropped before the packet;
//!   - a tool part that renders no text (file-only results, or no result
//!     text at all) keeps its row alive with empty content (F13's codex
//!     rule), so no identity-bearing tool record is dropped;
//!   - the message author is `data.model.id` for assistant and
//!     compaction rows (the model alias), "user" for user rows, absent
//!     otherwise — the only model evidence the connector preserves.
//!
//! Consequences this extractor encodes:
//!   - a row WITH archived parts is verified: every part is checked
//!     against the render contract, every tool part verifies into a
//!     call+result fact pair (an opencode part is both sides of one call),
//!     and the inventory fields stay complete because the archive proves
//!     the inventory — including a provably empty one;
//!   - a row WITHOUT parts predates F16 (rows indexed earlier keep their
//!     stored extras), so the v1 marker heuristics still decide it: a
//!     "[Tool Output]" marker means `partial` (identity and state
//!     dropped), no marker means `unavailable` (presence unverifiable);
//!   - a control part (step-start/step-finish) renders nothing by design —
//!     text it carries anyway is dropped and marked `partial`;
//!   - an unknown part type or a malformed text/state shape is drift:
//!     `partial` on exactly the fields it leaves unverifiable, never a
//!     guess;
//!   - a tool part without a `state` proves the call was recorded but no
//!     result ever persisted — `tool_output_interrupted`, source-absent
//!     (codex precedent), not a not-read gap;
//!   - an error object with no readable `message` is drift unless the
//!     record is provably textless (`opencode_error_text_unverifiable`),
//!     because unknown fields may hold text the render cannot read;
//!   - opencode records no numeric exit codes: every verified call keeps
//!     `tool_exit` `unavailable` (`tool_exit_not_recorded`), while the
//!     state status records error-ness for `tool_error`;
//!   - the delivered text (reasoning, tool output and error text,
//!     compaction summaries) stays fully searchable through the generic
//!     chat regions — this extractor adds no provider arm to
//!     `search_regions`;
//!   - a zero observed compaction count cannot certify the source had
//!     none (non-completed compactions drop pre-packet), so zero reports
//!     `None` with an explicit reason.

use std::collections::BTreeSet;

use serde_json::Value;

use crate::model::conversation_packet::ConversationPacket;

use super::classification::{self, ClassifyCall};
use super::{
    CORE_FIELDS, CoverageBuilder, Event, ProviderExtraction, RecordedCall, RecordedResult,
    ToolCollector, events, text_id,
};

const EXTRACTOR_REVISION: &str = "opencode-facts-v2";

const THINKING_MARKER: &str = "[Reasoning]\n";
const TOOL_MARKER: &str = "[Tool Output]\n";

/// Fields a "[Tool Output]" marker still certifies for a pre-F16 row: the
/// part's output text is delivered inline; its identity, input and state
/// are not.
const TOOL_PART_SCOPES: &[&str] = &["tools", "text_inventory", "block_inventory", "files"];
/// Fields the dropped part state (status/error) makes unverifiable.
const TOOL_STATE_SCOPES: &[&str] = &["tool_exit", "tool_error"];

pub(crate) fn extract(packet: &ConversationPacket) -> ProviderExtraction {
    let input = events(packet);
    let mut coverage = CoverageBuilder::new();
    let mut thinking = 0_i64;
    let mut compactions = 0_i64;
    let mut models: BTreeSet<String> = BTreeSet::new();
    let mut deduped: Vec<(i64, &str, &str)> = Vec::new();
    let mut tools = ToolCollector::new();
    // Rows without an archived parts inventory predating F16: file facts
    // are exact only when every assistant row is verified.
    let mut unverified_rows = false;

    for (event, message) in input.iter().zip(packet.payload.messages.iter()) {
        deduped.push((event.idx, event.role, event.content));
        let extra = event.extra;
        if !extra.is_object() {
            coverage.unknown("opencode_extra_missing");
            continue;
        }
        if !event.content.is_empty() {
            coverage.evidence("chat");
        }
        // The connector lifts `data.model.id` into the message author for
        // assistant and compaction rows ("user" elsewhere) — the only
        // model evidence the extras carry.
        if let Some(model) = message
            .author
            .as_deref()
            .filter(|author| !author.is_empty() && *author != "user")
        {
            models.insert(model.to_string());
        }
        // v2 rows carry their row `type`; 1.x rows do not and dispatch on
        // the packet role (user/system render whole; assistant is the
        // parts-bearing row). Anything else is drift, never a guess.
        let kind = match extra.get("type").and_then(Value::as_str) {
            Some(kind) => kind,
            None => match event.role {
                "assistant" => "assistant",
                "user" => "user",
                "system" => "system",
                _ => "",
            },
        };
        match kind {
            "assistant" => {
                if let Some(parts) = extra.get("parts").and_then(Value::as_array) {
                    verify_archived_parts(event, parts, &mut coverage, &mut tools, &mut thinking);
                } else {
                    unverified_rows = true;
                    // A pre-F16 row (stored extras, no parts archive):
                    // the v1 marker heuristics still decide it.
                    let reasoning = event.content.matches(THINKING_MARKER).count() as i64;
                    if reasoning > 0 {
                        thinking += reasoning;
                        coverage.evidence("thinking");
                    }
                    if event.content.contains(TOOL_MARKER) {
                        // A tool part provably existed and its output text
                        // is delivered inline; the part identity, input and
                        // state are dropped by the connector.
                        coverage.mark(TOOL_PART_SCOPES, "opencode_tool_part_dropped", "partial");
                        coverage.mark(
                            TOOL_STATE_SCOPES,
                            "opencode_tool_state_dropped",
                            "unavailable",
                        );
                    } else {
                        // No marker: a tool part whose state carried no
                        // text renders nothing, so the part's presence —
                        // and with it every tool fact — is unverifiable
                        // for this record.
                        coverage.mark(
                            TOOL_PART_SCOPES,
                            "opencode_tool_part_unverifiable",
                            "unavailable",
                        );
                        coverage.mark(
                            TOOL_STATE_SCOPES,
                            "opencode_tool_part_unverifiable",
                            "unavailable",
                        );
                    }
                }
            }
            // These rows render their whole text field(s) per the pinned
            // connector (data.text; "{name}\n{text}" for skill;
            // "$ {command}\n{output}" for shell) — nothing is dropped.
            "user" | "synthetic" | "system" | "skill" | "shell" => {}
            // Completed compactions render as assistant-role summary
            // content; non-completed ones drop pre-packet, so any
            // compaction-typed record here is completed.
            "compaction" => {
                compactions += 1;
            }
            // Idle rows and selection events drop before the packet; an
            // unknown row type or an unclassifiable 1.x role is a contract
            // change — drift, never a guess.
            _ => coverage.mark(CORE_FIELDS, "opencode_record_unknown", "partial"),
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
    let (tool_facts, file_facts) = tools.finish(&mut coverage);
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

    // A zero count cannot certify the source had no compactions: the
    // connector drops non-completed compaction rows (no summary text is
    // lost — pending compactions carry none). Observed counts are exact.
    let compaction_count = if compactions > 0 {
        Some(compactions)
    } else {
        coverage.mark(
            &["compaction"],
            "opencode_compaction_pending_dropped",
            "unavailable",
        );
        None
    };
    // File facts are exact only when every assistant row carries an
    // archived parts inventory; a pre-F16 row leaves the count a lower
    // bound, so it reports None (file paths then stay underivable, as
    // in v1).
    let file_fact_count = if unverified_rows {
        None
    } else {
        Some(file_facts as i64)
    };
    ProviderExtraction {
        extractor: EXTRACTOR_REVISION,
        coverage,
        tools: tool_facts,
        file_fact_count,
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

/// One assistant row's archived parts (FAD F16): every part is checked
/// against the render contract, and each tool part verifies into a
/// call+result fact pair. The parts array IS the block inventory, so the
/// inventory fields stay complete unless a part's shape is unverifiable.
fn verify_archived_parts(
    event: &Event<'_>,
    parts: &[Value],
    coverage: &mut CoverageBuilder,
    tools: &mut ToolCollector,
    thinking: &mut i64,
) {
    for part in parts {
        if !part.is_object() {
            coverage.mark(
                &["text_inventory", "block_inventory"],
                "opencode_part_malformed",
                "partial",
            );
            continue;
        }
        match part.get("type").and_then(Value::as_str).unwrap_or("") {
            // Text-bearing parts render their text when it is a non-empty
            // string; anything else on them is provably empty.
            "text" | "patch" => {
                if part_text_malformed(part) {
                    coverage.mark(
                        &["text_inventory", "block_inventory"],
                        "opencode_part_text_malformed",
                        "partial",
                    );
                }
            }
            "reasoning" => {
                if part_text_malformed(part) {
                    coverage.mark(
                        &["text_inventory", "block_inventory"],
                        "opencode_part_text_malformed",
                        "partial",
                    );
                } else if part
                    .get("text")
                    .and_then(Value::as_str)
                    .is_some_and(|text| !text.trim().is_empty())
                {
                    *thinking += 1;
                    coverage.evidence("thinking");
                }
            }
            // Control parts render nothing by design; text they carry
            // anyway is dropped by the pinned connector.
            "step-start" | "step-finish" => match part.get("text") {
                Some(Value::String(text)) if !text.trim().is_empty() => {
                    coverage.mark(
                        &["text_inventory"],
                        "opencode_control_part_text_dropped",
                        "partial",
                    );
                }
                None | Some(Value::Null) | Some(Value::String(_)) => {}
                Some(_) => {
                    coverage.mark(
                        &["text_inventory", "block_inventory"],
                        "opencode_part_text_malformed",
                        "partial",
                    );
                }
            },
            "tool" => verify_tool_part(event, part, coverage, tools),
            // The raw record is archived, so the block is inventoried; an
            // unknown part type may still carry text the contract does
            // not render — drift, never a guess.
            _ => coverage.mark(
                &["text_inventory", "block_inventory"],
                "opencode_part_unknown",
                "partial",
            ),
        }
    }
}

/// One archived tool part. Identity (the v2 part id, the 1.x `state.id`),
/// name (the v2 part name, the 1.x `state.title`), input, status, results
/// and error text are all in the raw record, so the part verifies into a
/// call+result fact pair — the part is both sides of one call.
fn verify_tool_part(
    event: &Event<'_>,
    part: &Value,
    coverage: &mut CoverageBuilder,
    tools: &mut ToolCollector,
) {
    let state = part.get("state");
    // A state key that is present but not an object is a not-read gap (the
    // record holds something the contract cannot read); a MISSING state
    // proves no result ever persisted (below).
    let state_obj = match state {
        None => None,
        Some(state) if state.is_object() => Some(state),
        Some(_) => {
            coverage.mark(
                &["tools", "text_inventory", "block_inventory"],
                "opencode_tool_state_malformed",
                "partial",
            );
            None
        }
    };
    let state_id = state_obj.and_then(|state| text_id(state.get("id")));
    let part_id = text_id(part.get("id"));
    // The 1.x tool-call id lives in `state.id`; the v2 part id sits on the
    // item itself. Whichever exists anchors the fact, the other is the raw
    // id; neither is drift on the tool verdicts too (gemini rule).
    let (call_id, raw_call_id) = match (state_id, part_id) {
        (Some(state_id), Some(part_id)) => (state_id, part_id),
        (Some(state_id), None) => (state_id.clone(), state_id),
        (None, Some(part_id)) => (part_id.clone(), part_id),
        (None, None) => {
            coverage.mark(
                &["tools", "tool_exit", "tool_error"],
                "opencode_tool_call_identity_missing",
                "partial",
            );
            return;
        }
    };
    let name = text_id(part.get("name"))
        .or_else(|| state_obj.and_then(|state| text_id(state.get("title"))));
    let Some(name) = name else {
        coverage.mark(&["tools"], "opencode_tool_name_missing", "partial");
        return;
    };
    coverage.evidence("tools");
    tools.call(RecordedCall {
        event_idx: event.idx,
        message_id: event.message_id,
        call_id: call_id.clone(),
        raw_call_id,
        name,
        arguments: state_obj.and_then(|state| state.get("input").cloned()),
    });
    let Some(state) = state_obj else {
        if state.is_none() {
            // No state key in the verbatim record: the call was recorded
            // but no result ever persisted — provably interrupted at the
            // source (codex precedent), a source-absent reason.
            tools.note_interrupted(&call_id);
        }
        // A malformed state keeps the call unpaired as a not-read gap
        // (`tool_result_missing` from `finish()`); no result is guessed.
        return;
    };
    // The result half is the part's own state: the v2 text results joined
    // (file results carry no text by contract), the 1.x `state.output`,
    // plus the error text — everything the render delivers.
    let mut pieces: Vec<String> = Vec::new();
    match state.get("content") {
        Some(Value::Array(items)) => {
            for item in items {
                match item.get("type").and_then(Value::as_str) {
                    Some("text") => match item.get("text") {
                        Some(Value::String(text)) if !text.trim().is_empty() => {
                            pieces.push(text.clone());
                        }
                        None | Some(Value::Null) | Some(Value::String(_)) => {}
                        Some(_) => coverage.mark(
                            &["text_inventory", "block_inventory"],
                            "opencode_result_text_malformed",
                            "partial",
                        ),
                    },
                    // File results carry only a uri/mime — provably
                    // textless per the render contract. Text they carry
                    // anyway is drift the contract drops.
                    Some("file") => {
                        if item
                            .get("text")
                            .and_then(Value::as_str)
                            .is_some_and(|text| !text.trim().is_empty())
                        {
                            coverage.mark(
                                &["text_inventory"],
                                "opencode_result_text_dropped",
                                "partial",
                            );
                        }
                    }
                    _ => coverage.mark(
                        &["text_inventory", "block_inventory"],
                        "opencode_result_unknown",
                        "partial",
                    ),
                }
            }
        }
        // The 1.x result text is `state.output` (no content array).
        None => match state.get("output") {
            Some(Value::String(text)) if !text.trim().is_empty() => pieces.push(text.clone()),
            None | Some(Value::Null) | Some(Value::String(_)) => {}
            Some(_) => coverage.mark(
                &["text_inventory", "block_inventory"],
                "opencode_result_text_malformed",
                "partial",
            ),
        },
        Some(_) => coverage.mark(
            &["text_inventory", "block_inventory"],
            "opencode_tool_state_malformed",
            "partial",
        ),
    }
    match state.get("error") {
        Some(Value::String(text)) if !text.trim().is_empty() => pieces.push(text.clone()),
        None | Some(Value::Null) | Some(Value::String(_)) => {}
        // The measured real failed-run shape: the failure text sits in the
        // error object's `message` and the render delivers it (F16), so
        // the record verifies.
        Some(Value::Object(record)) => {
            let delivered = record
                .get("message")
                .and_then(Value::as_str)
                .filter(|text| !text.trim().is_empty());
            if let Some(text) = delivered {
                pieces.push(text.to_string());
            } else {
                // No readable `message`. The record is provably textless
                // only when the message field is a string or null AND no
                // other field exists; anything else may hold text the
                // render contract cannot read — drift, never a guess.
                let provably_textless = match record.get("message") {
                    None => record.is_empty(),
                    Some(Value::String(_)) | Some(Value::Null) => record.len() <= 1,
                    Some(_) => false,
                };
                if !provably_textless {
                    coverage.mark(
                        &["text_inventory", "block_inventory"],
                        "opencode_error_text_unverifiable",
                        "partial",
                    );
                }
            }
        }
        Some(_) => coverage.mark(
            &["text_inventory", "block_inventory"],
            "opencode_result_text_malformed",
            "partial",
        ),
    }
    let text = pieces.join("\n");
    // Error-ness: an explicit error text records failure; else the state
    // status verdict (`completed` = success, `error` = failure); any other
    // status records nothing and `finish()` marks `tool_error`.
    let is_error = if state_carries_error(state) {
        Some(true)
    } else {
        match state.get("status").and_then(Value::as_str) {
            Some("completed") => Some(false),
            Some("error") => Some(true),
            _ => None,
        }
    };
    tools.result(RecordedResult {
        event_idx: event.idx,
        call_id,
        text: (!text.is_empty()).then_some(text),
        exit_code: None, // opencode records no numeric exit codes
        is_error,
    });
}

/// The state carries an explicit failure record: a non-empty error text —
/// a plain string, or the error object's `message` (F16's real shape).
fn state_carries_error(state: &Value) -> bool {
    match state.get("error") {
        Some(Value::String(text)) => !text.trim().is_empty(),
        Some(Value::Object(record)) => record
            .get("message")
            .and_then(Value::as_str)
            .is_some_and(|text| !text.trim().is_empty()),
        _ => false,
    }
}

/// A text-bearing part's `text` is present but not a string (and not
/// null): a malformed shape the render cannot take — drift, never a guess
/// (mirrors the gemini known-part rule).
fn part_text_malformed(part: &Value) -> bool {
    !matches!(
        part.get("text"),
        None | Some(Value::String(_)) | Some(Value::Null)
    )
}
