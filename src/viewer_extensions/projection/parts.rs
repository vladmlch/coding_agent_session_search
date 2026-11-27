use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde_json::{Value, json};
use sha2::Digest;

use crate::model::conversation_packet::ConversationPacket;
use crate::viewer_extensions::facts::{FactPublication, ToolCallFact};
use crate::viewer_extensions::providers::{is_compacted_extra, json_text, text_id};

use super::classify::{Class, Rec, surviving_records};
use super::{PROJECTION_VERSION, StepLink, StructuralPublication, ToolCallLink, TurnLink};

// ---------------------------------------------------------------------------
// Content parts
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct Part {
    /// `text|thinking|tool_call|tool_result`.
    kind: &'static str,
    call_id: Option<String>,
    /// Dedup signature inside the step (recorded payload identity).
    signature: String,
    /// Contributing packet position.
    pos: usize,
    /// Ordinal among tool parts in this step (`block_index`).
    block_index: Option<i64>,
}

/// Content blocks of a record: `message.content[]`, `content[]`, or a
/// block-typed top-level record (Claude vocabulary).
fn record_blocks(extra: &Value) -> Vec<&Value> {
    let mut blocks = Vec::new();
    if let Some(message) = extra.get("message").and_then(Value::as_object)
        && let Some(content) = message.get("content")
    {
        match content {
            Value::Array(items) => blocks.extend(items.iter()),
            single => blocks.push(single),
        }
        return blocks;
    }
    if let Some("text" | "thinking" | "redacted_thinking" | "tool_use" | "tool_result") =
        extra.get("type").and_then(Value::as_str)
    {
        blocks.push(extra);
        return blocks;
    }
    if let Some(content) = extra.get("content") {
        match content {
            Value::Array(items) => blocks.extend(items.iter()),
            single => blocks.push(single),
        }
    }
    blocks
}

/// Verified-call lookup: `recording idx` -> (`recorded raw id` -> scoped id).
type VerifiedCalls<'a> = HashMap<i64, HashMap<&'a str, &'a str>>;

fn verified_call_map<'a>(facts: &'a FactPublication) -> VerifiedCalls<'a> {
    let mut map = VerifiedCalls::new();
    for tool in &facts.tools {
        map.entry(tool.source_idx)
            .or_default()
            .entry(tool.raw_call_id.as_str())
            .or_insert(tool.call_id.as_str());
    }
    map
}

fn part_signature(kind: &str, payload: &Value) -> String {
    json_text(&json!([kind, payload]))
}

/// Project one surviving record's content parts, in recorded order, with
/// the contributing packet position retained per part.
fn record_parts(
    rec: &Rec,
    msgs: &[crate::model::conversation_packet::ConversationPacketMessage],
    verified: &VerifiedCalls,
    agent_slug: &str,
) -> Vec<Part> {
    let mut parts = Vec::new();
    for &pos in &rec.members {
        let message = &msgs[pos];
        let extra = &message.extra_json;
        // `{"cass":{…}}` single-key skeleton — the recorded structure was
        // compacted by the former ingest layer (codex-only
        // `should_compact_connector_extra`, removed 2026-11-08; rows
        // indexed before that still carry it); only the flattened text
        // remains. The shape is NOT a compaction marker for any other
        // provider: there it is the ingest merge envelope archiving lifted
        // `invocations` (see `merge_invocations_into_extra`), whose
        // harvesting arm below derives the tool_call parts.
        if agent_slug.eq_ignore_ascii_case("codex") && is_compacted_extra(extra) {
            if !message.content.is_empty() {
                parts.push(Part {
                    kind: "text",
                    call_id: None,
                    signature: part_signature("text", &Value::String(message.content.clone())),
                    pos,
                    block_index: None,
                });
            }
            continue;
        }
        let mut structured = false;

        // Claude-style content blocks (message.content/content arrays).
        for block in record_blocks(extra) {
            let Some(map) = block.as_object() else {
                continue;
            };
            match map.get("type").and_then(Value::as_str) {
                Some("text") if map.get("text").and_then(Value::as_str).is_some() => {
                    structured = true;
                    parts.push(Part {
                        kind: "text",
                        call_id: None,
                        signature: part_signature("text", block),
                        pos,
                        block_index: None,
                    });
                }
                Some("thinking" | "redacted_thinking") => {
                    structured = true;
                    parts.push(Part {
                        kind: "thinking",
                        call_id: None,
                        signature: part_signature("thinking", block),
                        pos,
                        block_index: None,
                    });
                }
                Some("tool_use") => {
                    structured = true;
                    let raw_id = text_id(map.get("id")).unwrap_or_default();
                    let call_id = verified
                        .get(&message.idx)
                        .and_then(|by_id| by_id.get(raw_id.as_str()))
                        .map(|scoped| (*scoped).to_string())
                        .or_else(|| (!raw_id.is_empty()).then_some(raw_id));
                    parts.push(Part {
                        kind: "tool_call",
                        call_id,
                        signature: part_signature(
                            "tool_call",
                            map.get("id").unwrap_or(&Value::Null),
                        ),
                        pos,
                        block_index: None,
                    });
                }
                Some("tool_result") => {
                    structured = true;
                    parts.push(Part {
                        kind: "tool_result",
                        call_id: text_id(map.get("tool_use_id")),
                        signature: part_signature(
                            "tool_result",
                            map.get("tool_use_id").unwrap_or(&Value::Null),
                        ),
                        pos,
                        block_index: None,
                    });
                }
                _ => {}
            }
        }

        // Codex `response_item`/`event_msg` payloads.
        match extra.get("type").and_then(Value::as_str) {
            Some("response_item") => {
                if let Some(payload) = extra.get("payload").filter(|p| p.is_object()) {
                    structured = true;
                    let kind = payload.get("type").and_then(Value::as_str).unwrap_or("");
                    match kind {
                        "function_call" | "custom_tool_call" => {
                            let raw_id =
                                text_id(payload.get("call_id").or_else(|| payload.get("id")))
                                    .unwrap_or_default();
                            let call_id = verified
                                .get(&message.idx)
                                .and_then(|by_id| by_id.get(raw_id.as_str()))
                                .map(|scoped| (*scoped).to_string())
                                .or_else(|| (!raw_id.is_empty()).then_some(raw_id.clone()));
                            parts.push(Part {
                                kind: "tool_call",
                                call_id,
                                signature: part_signature("tool_call", &json!([kind, raw_id])),
                                pos,
                                block_index: None,
                            });
                        }
                        "reasoning" => {
                            for field in ["content", "summary"] {
                                if let Some(value) = payload.get(field) {
                                    let items: Vec<&Value> = match value {
                                        Value::Array(list) => list.iter().collect(),
                                        single => vec![single],
                                    };
                                    for item in items {
                                        let text = if item.is_string() {
                                            item.as_str()
                                        } else {
                                            item.get("text").and_then(Value::as_str)
                                        };
                                        if let Some(text) = text
                                            && !text.is_empty()
                                        {
                                            parts.push(Part {
                                                kind: "thinking",
                                                call_id: None,
                                                signature: part_signature(
                                                    "thinking",
                                                    &Value::String(text.to_string()),
                                                ),
                                                pos,
                                                block_index: None,
                                            });
                                        }
                                    }
                                }
                            }
                        }
                        "message" => {
                            if let Some(content) = payload.get("content") {
                                parts.push(Part {
                                    kind: "text",
                                    call_id: None,
                                    signature: part_signature("text", content),
                                    pos,
                                    block_index: None,
                                });
                            }
                        }
                        _ => {}
                    }
                }
            }
            Some("event_msg") => {
                if let Some(payload) = extra.get("payload").filter(|p| p.is_object()) {
                    let kind = payload.get("type").and_then(Value::as_str).unwrap_or("");
                    match kind {
                        "agent_reasoning" | "reasoning" => {
                            structured = true;
                            if let Some(text) = payload
                                .get("text")
                                .or_else(|| payload.get("message"))
                                .and_then(Value::as_str)
                                .filter(|text| !text.is_empty())
                            {
                                parts.push(Part {
                                    kind: "thinking",
                                    call_id: None,
                                    signature: part_signature(
                                        "thinking",
                                        &Value::String(text.to_string()),
                                    ),
                                    pos,
                                    block_index: None,
                                });
                            }
                        }
                        "user_message" | "agent_message" | "error" | "event_msg" => {
                            structured = true;
                            if let Some(text) = payload
                                .get("message")
                                .or_else(|| payload.get("text"))
                                .and_then(Value::as_str)
                                .filter(|text| !text.is_empty())
                            {
                                parts.push(Part {
                                    kind: "text",
                                    call_id: None,
                                    signature: part_signature(
                                        "text",
                                        &Value::String(text.to_string()),
                                    ),
                                    pos,
                                    block_index: None,
                                });
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }

        // Antigravity `cass.invocations` calls + `thinking`; devin
        // `payload.tool_calls`; cursor `toolFormerCalls`; ACP `kind:tool_call`.
        let mut tool_items: Vec<&Value> = Vec::new();
        if let Some(items) = extra.pointer("/cass/invocations").and_then(Value::as_array) {
            structured = true;
            tool_items.extend(items.iter().filter(|item| {
                item.get("kind").and_then(Value::as_str).unwrap_or("tool") == "tool"
            }));
        }
        if let Some(items) = extra.get("tool_calls").and_then(Value::as_array) {
            structured = true;
            tool_items.extend(items.iter());
        }
        if let Some(items) = extra
            .pointer("/payload/tool_calls")
            .and_then(Value::as_array)
        {
            structured = true;
            tool_items.extend(items.iter());
        }
        if let Some(items) = extra.get("toolFormerCalls").and_then(Value::as_array) {
            structured = true;
            tool_items.extend(items.iter());
        }
        if extra.get("kind").and_then(Value::as_str) == Some("tool_call")
            && let Some(content) = extra.pointer("/payload/content").filter(|c| c.is_object())
        {
            structured = true;
            tool_items.push(content);
        }
        for item in tool_items {
            let raw_id = text_id(
                item.get("call_id")
                    .or_else(|| item.get("callId"))
                    .or_else(|| item.get("toolCallId"))
                    .or_else(|| item.get("tool_use_id"))
                    .or_else(|| item.get("id")),
            )
            .unwrap_or_default();
            let call_id = verified
                .get(&message.idx)
                .and_then(|by_id| by_id.get(raw_id.as_str()))
                .map(|scoped| (*scoped).to_string())
                .or_else(|| (!raw_id.is_empty()).then_some(raw_id.clone()));
            parts.push(Part {
                kind: "tool_call",
                call_id,
                signature: part_signature("tool_call", &Value::String(raw_id)),
                pos,
                block_index: None,
            });
        }

        // Thinking recorded outside content blocks.
        if let Some(text) = extra.get("thinking").and_then(Value::as_str)
            && !text.is_empty()
        {
            structured = true;
            parts.push(Part {
                kind: "thinking",
                call_id: None,
                signature: part_signature("thinking", &Value::String(text.to_string())),
                pos,
                block_index: None,
            });
        }
        for field in ["allThinkingBlocks"] {
            if let Some(items) = extra.get(field).and_then(Value::as_array) {
                structured = true;
                for item in items {
                    if let Some(text) = item.get("text").and_then(Value::as_str)
                        && !text.is_empty()
                    {
                        parts.push(Part {
                            kind: "thinking",
                            call_id: None,
                            signature: part_signature("thinking", &Value::String(text.to_string())),
                            pos,
                            block_index: None,
                        });
                    }
                }
            }
        }

        // Cursor `rawText` / flattened content: only when no structured
        // parts described the record (the content column is connector-
        // flattened block text otherwise — never double-counted). This is
        // also the user/system prompt/body arm.
        if !structured && !message.content.is_empty() {
            parts.push(Part {
                kind: "text",
                call_id: None,
                signature: part_signature("text", &Value::String(message.content.clone())),
                pos,
                block_index: None,
            });
        }
    }
    parts
}

// ---------------------------------------------------------------------------
// Result evidence scan + verified claim
// ---------------------------------------------------------------------------

/// Recorded result references in one message: `(raw_call_id, block ordinal)`
/// pairs in recorded order — the same shapes the provider extractors
/// correlate. Only recorded identifiers are read; nothing is invented.
fn result_refs(extra: &Value) -> Vec<String> {
    let mut refs = Vec::new();
    if !extra.is_object() {
        return refs;
    }
    if extra.get("source").and_then(Value::as_str) == Some("tool_result")
        && let Some(id) = text_id(extra.get("tool_use_id"))
    {
        refs.push(id);
    }
    if extra.get("toolUseResult").is_some()
        && let Some(id) = text_id(extra.get("sourceToolUseID"))
    {
        refs.push(id);
    }
    for block in record_blocks(extra) {
        if block.get("type").and_then(Value::as_str) == Some("tool_result")
            && let Some(id) = text_id(
                block
                    .get("tool_use_id")
                    .or_else(|| block.get("toolUseId"))
                    .or_else(|| block.get("id")),
            )
        {
            refs.push(id);
        }
    }
    if let Some(items) = extra.get("toolResults").and_then(Value::as_array) {
        for item in items {
            if let Some(id) = text_id(
                item.get("tool_use_id")
                    .or_else(|| item.get("toolUseId"))
                    .or_else(|| item.get("id")),
            ) {
                refs.push(id);
            }
        }
    }
    match extra.get("type").and_then(Value::as_str) {
        Some("response_item") => {
            let payload = extra.get("payload");
            let kind = payload
                .and_then(|p| p.get("type"))
                .and_then(Value::as_str)
                .unwrap_or("");
            if matches!(
                kind,
                "function_call_output"
                    | "custom_tool_call_output"
                    | "tool_output"
                    | "tool_result"
                    | "exec_command_end"
            ) && let Some(id) = text_id(
                payload
                    .and_then(|p| p.get("call_id"))
                    .or_else(|| payload.and_then(|p| p.get("id"))),
            ) {
                refs.push(id);
            }
        }
        Some("event_msg") => {
            if let Some(payload) = extra.get("payload").filter(|p| p.is_object())
                && payload.get("type").and_then(Value::as_str) == Some("exec_command_end")
                && let Some(id) = text_id(payload.get("call_id"))
            {
                refs.push(id);
            }
        }
        _ => {}
    }
    if matches!(
        extra.get("agy_type").and_then(Value::as_str),
        Some("TOOL_OUTPUT" | "VIEW_FILE" | "RUN_COMMAND")
    ) && let Some(id) = text_id(
        extra
            .get("call_id")
            .or_else(|| extra.get("tool_use_id"))
            .or_else(|| extra.get("id")),
    ) {
        refs.push(id);
    }
    if let Some(items) = extra.get("tool_outputs").and_then(Value::as_array) {
        for item in items {
            if let Some(id) = text_id(
                item.get("call_id")
                    .or_else(|| item.get("tool_use_id"))
                    .or_else(|| item.get("id")),
            ) {
                refs.push(id);
            }
        }
    }
    if extra.pointer("/payload/role").and_then(Value::as_str) == Some("tool")
        && let Some(id) = text_id(
            extra
                .pointer("/payload/tool_call_id")
                .or_else(|| extra.pointer("/payload/toolCallId")),
        )
    {
        refs.push(id);
    }
    // ACP `kind:tool_call` carries `rawOutput` inline — call and result
    // share one anchor.
    if extra.get("kind").and_then(Value::as_str) == Some("tool_call")
        && let Some(content) = extra.pointer("/payload/content").filter(|c| c.is_object())
        && content.get("rawOutput").is_some()
        && let Some(id) = text_id(content.get("toolCallId"))
    {
        refs.push(id);
    }
    // Cursor `toolFormerCalls` carry outputs inline at the same anchor.
    if let Some(items) = extra.get("toolFormerCalls").and_then(Value::as_array) {
        for item in items {
            if (item.get("output").is_some() || item.get("result").is_some())
                && let Some(id) = text_id(item.get("callId").or_else(|| item.get("call_id")))
            {
                refs.push(id);
            }
        }
    }
    // Sparse FAD result records ({message_id, tool_call_id}).
    if let Some(id) = text_id(extra.get("tool_call_id")) {
        refs.push(id);
    }
    refs
}

// ---------------------------------------------------------------------------
// Projection
// ---------------------------------------------------------------------------

fn part_key(part: &Part) -> String {
    let digest = sha2::Sha256::digest(part.signature.as_bytes());
    hex::encode(&digest[..8])
}

pub(super) fn source_key(message_id: Option<i64>, idx: i64) -> String {
    message_id.map_or_else(|| format!("i{idx}"), |id| format!("m{id}"))
}

struct StepAcc {
    step_index: i64,
    step_kind: &'static str,
    turn_index: Option<i64>,
    /// `(pos, scope)` member rows.
    members: Vec<(usize, &'static str)>,
    parts: Vec<Part>,
}

/// Project the packet + verified facts into the structural publication.
pub fn project_session_structure(
    packet: &ConversationPacket,
    facts: &FactPublication,
) -> StructuralPublication {
    let msgs = &packet.payload.messages;
    let mut warnings = Vec::new();
    let records = surviving_records(packet, &mut warnings);
    let verified = verified_call_map(facts);

    // --- steps ------------------------------------------------------------
    let mut steps: Vec<StepAcc> = Vec::new();
    let mut step_of_pos: HashMap<usize, usize> = HashMap::new();
    let mut last_boundary_step = false;
    for rec in &records {
        let kind: &'static str = match rec.class {
            Class::Boundary => "compact_boundary",
            Class::User => "user_message",
            Class::System => "system_message",
            Class::Assistant => "turn",
            Class::Error => "error_message",
            Class::ToolResult | Class::Noise | Class::Other => "",
        };
        if kind.is_empty() {
            continue;
        }
        // A second consecutive boundary folds into the previous one
        // (oracle `_handle_compact_boundary` `already_compacted` arm).
        if rec.class == Class::Boundary && last_boundary_step {
            continue;
        }
        last_boundary_step = rec.class == Class::Boundary;
        let parts = record_parts(rec, msgs, &verified, &packet.payload.identity.agent_slug);
        // Empty assistant steps are dropped by the oracle's normalizer;
        // user/system/boundary/error steps always survive.
        if rec.class == Class::Assistant && parts.is_empty() {
            continue;
        }
        let step_index = (steps.len() + 1) as i64;
        for &pos in &rec.members {
            step_of_pos.insert(pos, steps.len());
        }
        steps.push(StepAcc {
            step_index,
            step_kind: kind,
            turn_index: None,
            members: rec.members.iter().map(|&pos| (pos, "member")).collect(),
            parts,
        });
    }

    // --- part dedup + tool part ordinals ------------------------------------
    for step in &mut steps {
        let keep: Vec<bool> = {
            let mut seen: BTreeSet<&str> = BTreeSet::new();
            step.parts
                .iter()
                .map(|part| seen.insert(part.signature.as_str()))
                .collect()
        };
        let mut keep_iter = keep.into_iter();
        step.parts.retain(|_| keep_iter.next().unwrap_or(false));
        let mut tool_ordinal = 0_i64;
        for part in &mut step.parts {
            if matches!(part.kind, "tool_call" | "tool_result") {
                part.block_index = Some(tool_ordinal);
                tool_ordinal += 1;
            }
        }
    }

    // --- result evidence: claim recorded refs per raw call id ---------------
    // Ordered per-raw-id queues of (pos, block ordinal) refs.
    let mut ref_queues: BTreeMap<String, Vec<(usize, i64)>> = BTreeMap::new();
    for (pos, message) in msgs.iter().enumerate() {
        for (ordinal, raw_id) in result_refs(&message.extra_json).iter().enumerate() {
            ref_queues
                .entry(raw_id.clone())
                .or_default()
                .push((pos, ordinal as i64));
        }
    }
    let mut claimed: BTreeSet<usize> = BTreeSet::new();
    let mut tools: Vec<ToolCallLink> = Vec::new();
    let mut sorted_facts: Vec<&ToolCallFact> = facts.tools.iter().collect();
    sorted_facts.sort_by_key(|tool| tool.source_idx);
    for (ordinal, fact) in sorted_facts.iter().enumerate() {
        let step_index = step_of_pos
            .iter()
            .find(|&(&pos, _)| msgs[pos].idx == fact.source_idx)
            .map(|(_, &step)| steps[step].step_index);
        // Claim the earliest unclaimed recorded ref at or after the call's
        // anchor (inline refs share the anchor; separate records follow it).
        let mut result: Option<(usize, i64)> = None;
        if fact.has_result
            && let Some(queue) = ref_queues.get_mut(&fact.raw_call_id)
            && let Some(slot) = queue
                .iter()
                .position(|&(pos, _)| !claimed.contains(&pos) && msgs[pos].idx >= fact.source_idx)
        {
            let claim = queue.remove(slot);
            claimed.insert(claim.0);
            result = Some(claim);
        }
        tools.push(ToolCallLink {
            call_id: fact.call_id.clone(),
            raw_call_id: fact.raw_call_id.clone(),
            name: fact.name.clone(),
            step_index,
            turn_index: None,
            has_result: fact.has_result,
            is_error: fact.is_error,
            exit_code: fact.exit_code,
            file_path: fact.file_path.clone(),
            call_idx: fact.source_idx,
            call_message_id: fact.source_message_id,
            result_idx: result.map(|(pos, _)| msgs[pos].idx),
            result_message_id: result.and_then(|(pos, _)| msgs[pos].message_id),
            result_block_index: result.map(|(_, ordinal)| ordinal),
            ordinal: ordinal as i64,
        });
        // The claimed result record joins the calling step as an absorbed
        // member (scope `result`).
        if let (Some((pos, _)), Some(step_index)) = (result, step_index)
            && let Some(step) = steps.iter_mut().find(|s| s.step_index == step_index)
        {
            step.members.push((pos, "result"));
        }
    }

    // --- turns --------------------------------------------------------------
    let mut turns: Vec<TurnLink> = Vec::new();
    let mut open_ai: Option<usize> = None;
    for step_index in 0..steps.len() {
        let step_kind = steps[step_index].step_kind;
        let has_text = steps[step_index]
            .parts
            .iter()
            .any(|part| part.kind == "text");
        match step_kind {
            "user_message" => {
                open_ai = None;
                // Empty user prompts emit no turn (oracle
                // `_project_user_turn` returns None).
                if has_text {
                    let turn_index = turns.len() as i64;
                    turns.push(TurnLink {
                        turn_index,
                        turn_kind: "user".to_string(),
                        first_step_index: steps[step_index].step_index,
                        step_count: 1,
                        source_idx: steps[step_index]
                            .members
                            .first()
                            .map(|&(pos, _)| msgs[pos].idx),
                        message_id: steps[step_index]
                            .members
                            .first()
                            .and_then(|&(pos, _)| msgs[pos].message_id),
                        source_key: None,
                    });
                    steps[step_index].turn_index = Some(turn_index);
                }
            }
            "system_message" => {
                open_ai = None;
                if has_text {
                    let turn_index = turns.len() as i64;
                    turns.push(TurnLink {
                        turn_index,
                        turn_kind: "system".to_string(),
                        first_step_index: steps[step_index].step_index,
                        step_count: 1,
                        source_idx: steps[step_index]
                            .members
                            .first()
                            .map(|&(pos, _)| msgs[pos].idx),
                        message_id: steps[step_index]
                            .members
                            .first()
                            .and_then(|&(pos, _)| msgs[pos].message_id),
                        source_key: None,
                    });
                    steps[step_index].turn_index = Some(turn_index);
                }
            }
            "compact_boundary" => {
                open_ai = None;
                let turn_index = turns.len() as i64;
                turns.push(TurnLink {
                    turn_index,
                    turn_kind: "compact_boundary".to_string(),
                    first_step_index: steps[step_index].step_index,
                    step_count: 1,
                    source_idx: steps[step_index]
                        .members
                        .first()
                        .map(|&(pos, _)| msgs[pos].idx),
                    message_id: steps[step_index]
                        .members
                        .first()
                        .and_then(|&(pos, _)| msgs[pos].message_id),
                    source_key: None,
                });
                steps[step_index].turn_index = Some(turn_index);
            }
            _ => {
                // Assistant/error steps accumulate into one `ai` turn until
                // the next user/system/boundary step.
                let turn_index = match open_ai {
                    Some(index) => index,
                    None => {
                        let index = turns.len();
                        turns.push(TurnLink {
                            turn_index: index as i64,
                            turn_kind: "ai".to_string(),
                            first_step_index: steps[step_index].step_index,
                            step_count: 0,
                            source_idx: None,
                            message_id: None,
                            source_key: None,
                        });
                        open_ai = Some(index);
                        index
                    }
                };
                turns[turn_index].step_count += 1;
                steps[step_index].turn_index = Some(turn_index as i64);
            }
        }
    }
    for tool in &mut tools {
        tool.turn_index = tool
            .step_index
            .and_then(|index| steps.iter().find(|s| s.step_index == index))
            .and_then(|step| step.turn_index);
    }

    // --- flatten ------------------------------------------------------------
    let mut step_links: Vec<StepLink> = Vec::new();
    for step in &steps {
        let mut link_index = 0_i64;
        let mut push_link = |pos: usize,
                             scope: &str,
                             block_index: Option<i64>,
                             call_id: Option<String>,
                             part_key: Option<String>| {
            let message = &msgs[pos];
            step_links.push(StepLink {
                step_index: step.step_index,
                turn_index: step.turn_index,
                step_kind: step.step_kind.to_string(),
                link_index,
                scope: scope.to_string(),
                block_index,
                call_id,
                source_idx: message.idx,
                source_key: source_key(message.message_id, message.idx),
                message_id: message.message_id,
                part_key,
            });
            link_index += 1;
        };
        for &(pos, scope) in &step.members {
            push_link(pos, scope, None, None, None);
        }
        for part in &step.parts {
            push_link(
                part.pos,
                part.kind,
                part.block_index,
                part.call_id.clone(),
                Some(part_key(part)),
            );
        }
    }
    for turn in &mut turns {
        if let Some(idx) = turn.source_idx {
            turn.source_key = Some(source_key(turn.message_id, idx));
        }
    }

    StructuralPublication {
        projection_version: PROJECTION_VERSION,
        extractor_version: facts.extractor_version,
        source_revision: facts.source_revision,
        provider: facts.provider.clone(),
        steps: step_links,
        turns,
        tool_calls: tools,
        warnings,
    }
}
