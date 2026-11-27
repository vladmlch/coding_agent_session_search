use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde_json::{Value, json};

use crate::model::conversation_packet::ConversationPacket;
use crate::viewer_extensions::providers::{json_text, text_id};

// ---------------------------------------------------------------------------
// Record classification
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Class {
    User,
    System,
    Assistant,
    Boundary,
    ToolResult,
    Error,
    Noise,
    Other,
}

pub(super) struct Rec {
    /// Packet position of this surviving record.
    pub(super) pos: usize,
    pub(super) class: Class,
    /// Contributing packet positions (uuid-dedup + logical-merge provenance).
    pub(super) members: Vec<usize>,
    /// `true` when this record splits a logical merge group
    /// (compaction/summary), regardless of whether it produces a step.
    merge_boundary: bool,
}

/// Recorded control/noise types that never produce a step
/// (`_is_noise_record` + the codex CONTROL set the oracle consumes for
/// bookkeeping only).
const NOISE_TYPES: &[&str] = &[
    "CHECKPOINT",
    "SYSTEM_MESSAGE",
    "TOOL_OUTPUT",
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
const NOISE_SUBTYPES: &[&str] = &["away_summary", "stop_hook_summary", "turn_duration"];

/// `_is_compact_boundary` / `_is_merge_boundary` over the verbatim extra:
/// returns `(is_step_boundary, is_merge_boundary)`.
fn boundary_flags(extra: &Value) -> (bool, bool) {
    if !extra.is_object() {
        return (false, false);
    }
    let rec_type = extra.get("type").and_then(Value::as_str).unwrap_or("");
    let mut step_boundary = extra
        .get("isCompactSummary")
        .or_else(|| extra.get("is_compact_summary"))
        .and_then(Value::as_bool)
        == Some(true)
        || extra.get("kind").and_then(Value::as_str) == Some("compact_boundary")
        || matches!(
            rec_type,
            "COMPACT_BOUNDARY" | "compact_boundary" | "compacted"
        );
    if rec_type == "event_msg"
        && let Some(payload) = extra.get("payload").filter(|p| p.is_object())
    {
        let p_type = payload.get("type").and_then(Value::as_str);
        let item_type = payload
            .get("item")
            .and_then(Value::as_object)
            .and_then(|item| item.get("type"))
            .and_then(Value::as_str);
        step_boundary = step_boundary
            || p_type == Some("context_compacted")
            || p_type == Some("compacted")
            || (p_type == Some("item_completed") && item_type == Some("ContextCompaction"));
    }
    if matches!(rec_type, "user" | "system")
        && extra.get("source").and_then(Value::as_str) != Some("USER")
        && extra.get("role").and_then(Value::as_str) != Some("user")
    {
        let content = extra
            .pointer("/message/content")
            .and_then(Value::as_str)
            .or_else(|| extra.get("content").and_then(Value::as_str));
        if content.is_some_and(|text| text.trim() == "Conversation compacted") {
            step_boundary = true;
        }
    }
    // Devin `cli_system` continuation marker — the recorded compaction
    // evidence the devin facts arm counts.
    if extra.pointer("/payload/role").and_then(Value::as_str) == Some("system")
        && extra
            .pointer("/payload/content")
            .and_then(Value::as_str)
            .is_some_and(|text| text.starts_with("You are continuing a previous Devin session"))
    {
        step_boundary = true;
    }
    // `summary` records split merge groups but produce no step (oracle
    // `_is_merge_boundary`); a real boundary does both.
    let merge_boundary = step_boundary || rec_type == "summary";
    (step_boundary, merge_boundary)
}

/// `true` when the extra records tool RESULT evidence (the record is a
/// result carrier, not a step of its own).
fn is_tool_result_record(extra: &Value, content: &str) -> bool {
    if !extra.is_object() {
        return false;
    }
    if extra.get("source").and_then(Value::as_str) == Some("tool_result") {
        return true;
    }
    // Claude mapping fold: bare `tool_use_id` without a `type`.
    if text_id(extra.get("tool_use_id")).is_some() && extra.get("type").is_none() {
        return true;
    }
    if extra.get("toolUseResult").is_some()
        && text_id(extra.get("sourceToolUseID")).is_some()
        && content.is_empty()
    {
        return true;
    }
    if extra
        .get("toolResults")
        .and_then(Value::as_array)
        .is_some_and(|items| !items.is_empty())
        && content.is_empty()
    {
        return true;
    }
    match extra.get("type").and_then(Value::as_str) {
        Some("response_item") => {
            let kind = extra
                .pointer("/payload/type")
                .and_then(Value::as_str)
                .unwrap_or("");
            matches!(
                kind,
                "function_call_output"
                    | "custom_tool_call_output"
                    | "tool_output"
                    | "tool_result"
                    | "exec_command_end"
            )
        }
        Some("event_msg") => {
            let payload = extra.get("payload");
            let kind = payload.and_then(|p| p.get("type")).and_then(Value::as_str);
            kind == Some("exec_command_end")
                || payload.is_some_and(|p| {
                    text_id(p.get("call_id")).is_some()
                        && (p.get("exit_code").is_some()
                            || p.get("stdout").is_some()
                            || p.get("aggregated_output").is_some())
                })
        }
        _ => {
            // Antigravity discrete output records; devin cli tool results.
            if matches!(
                extra.get("agy_type").and_then(Value::as_str),
                Some("TOOL_OUTPUT" | "VIEW_FILE" | "RUN_COMMAND")
            ) {
                return true;
            }
            if extra.pointer("/payload/role").and_then(Value::as_str) == Some("tool") {
                return true;
            }
            // Sparse FAD tool result ({tool_call_id} only).
            text_id(extra.get("tool_call_id")).is_some()
        }
    }
}

/// `true` when the record's only content is tool_result blocks
/// (the oracle's pure-tool-result fold producing no user step).
fn pure_tool_result_content(extra: &Value) -> bool {
    for container in [extra.pointer("/message/content"), extra.get("content")]
        .into_iter()
        .flatten()
    {
        if let Value::Array(items) = container
            && !items.is_empty()
            && items.iter().all(|block| {
                block.is_object()
                    && block.get("type").and_then(Value::as_str) == Some("tool_result")
            })
        {
            return true;
        }
    }
    false
}

fn is_error_record(extra: &Value) -> bool {
    let kind = extra
        .get("agy_type")
        .or_else(|| extra.get("type"))
        .or_else(|| extra.get("kind"))
        .and_then(Value::as_str)
        .unwrap_or("");
    matches!(kind, "ERROR_MESSAGE" | "error" | "error_message" | "ERROR")
}

fn is_noise_record(extra: &Value) -> bool {
    let rec_type = extra.get("type").and_then(Value::as_str).unwrap_or("");
    if NOISE_TYPES.contains(&rec_type) {
        return true;
    }
    if NOISE_SUBTYPES.contains(&extra.get("subtype").and_then(Value::as_str).unwrap_or("")) {
        return true;
    }
    // Antigravity envelope kinds that carry no dialogue content.
    matches!(
        extra.get("agy_type").and_then(Value::as_str),
        Some("SYSTEM_MESSAGE" | "CONVERSATION_HISTORY")
    )
}

/// `(class, merge_boundary)` for one packet message.
fn classify(role: &str, content: &str, extra: &Value) -> (Class, bool) {
    let (step_boundary, merge_boundary) = boundary_flags(extra);
    if step_boundary {
        return (Class::Boundary, merge_boundary);
    }
    if is_noise_record(extra) {
        return (Class::Noise, merge_boundary);
    }
    if is_error_record(extra) {
        return (Class::Error, merge_boundary);
    }
    let class = match role {
        "user" => {
            if is_tool_result_record(extra, content) || pure_tool_result_content(extra) {
                Class::ToolResult
            } else {
                Class::User
            }
        }
        "system" => Class::System,
        "agent" | "assistant" => {
            if is_tool_result_record(extra, content) {
                Class::ToolResult
            } else {
                Class::Assistant
            }
        }
        "tool" => {
            // No recorded result evidence → the canonical fallback maps the
            // message to a planner turn rather than dropping it.
            if is_tool_result_record(extra, content) {
                Class::ToolResult
            } else {
                Class::Assistant
            }
        }
        _ => {
            // Unmapped roles keep their own record type in the oracle and
            // produce no step.
            if is_tool_result_record(extra, content) {
                Class::ToolResult
            } else {
                Class::Other
            }
        }
    };
    (class, merge_boundary)
}

// ---------------------------------------------------------------------------
// Dedup + merge passes
// ---------------------------------------------------------------------------

/// Dedup signature ignoring accounting noise (oracle `semantic_record`):
/// top-level `usage`/`costUSD` and `message.usage` never make two records
/// conflict.
fn record_signature(extra: &Value) -> String {
    if let Some(map) = extra.as_object() {
        let mut stripped = serde_json::Map::new();
        for (key, value) in map {
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
    } else {
        json_text(extra)
    }
}

/// Claude logical-response key: `(requestId or message.id, message.id)` on
/// `type == "assistant"` records — the mergeable unit of the streamed reply.
fn assistant_group_key(extra: &Value) -> Option<String> {
    if extra.get("type").and_then(Value::as_str) != Some("assistant") {
        return None;
    }
    let message = extra.get("message")?.as_object()?;
    let message_id = message.get("id");
    let group_id = extra
        .get("requestId")
        .filter(|v| !v.is_null())
        .or(message_id)?;
    let scalar = |v: &Value| matches!(v, Value::String(_) | Value::Number(_)) && !v.is_null();
    if !scalar(group_id) || !message_id.is_none_or(scalar) {
        return None;
    }
    Some(json_text(&json!([group_id, message_id])))
}

/// Build surviving records: uuid physical dedup (conflicting uuids dropped
/// as `conflicting_event`), codex `response_item` payload-id replay dedup,
/// then the Claude logical-group merge. Returned in packet order.
pub(super) fn surviving_records(
    packet: &ConversationPacket,
    warnings: &mut Vec<String>,
) -> Vec<Rec> {
    let msgs = &packet.payload.messages;

    // Pass 1 — physical dedup. Group keys: `uuid:<uuid>` (Claude) or
    // `response:<payload.type>:<payload.id>` (Codex response_item replays).
    let mut groups: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    let mut no_key: Vec<usize> = Vec::new();
    for (pos, message) in msgs.iter().enumerate() {
        let extra = &message.extra_json;
        let key = text_id(extra.get("uuid"))
            .map(|uuid| format!("uuid:{uuid}"))
            .or_else(|| {
                if extra.get("type").and_then(Value::as_str) == Some("response_item") {
                    let kind = extra.pointer("/payload/type").and_then(Value::as_str);
                    text_id(extra.pointer("/payload/id"))
                        .map(|id| format!("response:{}:{id}", kind.unwrap_or("")))
                } else {
                    None
                }
            });
        match key {
            Some(key) => groups.entry(key).or_default().push(pos),
            None => no_key.push(pos),
        }
    }
    let mut kept: Vec<Rec> = Vec::new();
    for positions in groups.values() {
        let signatures: BTreeSet<String> = positions
            .iter()
            .map(|&pos| record_signature(&msgs[pos].extra_json))
            .collect();
        if signatures.len() == 1 {
            let (class, merge_boundary) = classify(
                &msgs[positions[0]].role,
                &msgs[positions[0]].content,
                &msgs[positions[0]].extra_json,
            );
            kept.push(Rec {
                pos: positions[0],
                class,
                members: positions.clone(),
                merge_boundary,
            });
        } else {
            warnings.push(format!(
                "conflicting_event:{}",
                positions
                    .iter()
                    .map(|&pos| msgs[pos].idx.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }
    }
    for pos in no_key {
        let (class, merge_boundary) =
            classify(&msgs[pos].role, &msgs[pos].content, &msgs[pos].extra_json);
        kept.push(Rec {
            pos,
            class,
            members: vec![pos],
            merge_boundary,
        });
    }
    kept.sort_by_key(|rec| rec.pos);

    // Pass 2 — Claude logical-group merge. Members of one
    // `(requestId|message.id, message.id)` key merge at the first member's
    // position; a merge-boundary record or an interleaved foreign
    // `sessionId` between members splits the group into segments.
    let mut segment_no: HashMap<String, usize> = HashMap::new();
    let mut last_member_ord: HashMap<String, usize> = HashMap::new();
    let mut member_session: HashMap<String, String> = HashMap::new();
    let mut session_boundaries: HashMap<String, usize> = HashMap::new();
    let mut last_boundary_ord: Option<usize> = None;
    let mut segment_of_ord: HashMap<usize, (String, usize)> = HashMap::new();

    for (ord, rec) in kept.iter().enumerate() {
        let extra = &msgs[rec.pos].extra_json;
        let session_id = text_id(extra.get("sessionId"));
        if let Some(key) = assistant_group_key(extra) {
            if let Some(&prev_ord) = last_member_ord.get(&key)
                && (last_boundary_ord.is_some_and(|boundary| boundary > prev_ord)
                    || member_session
                        .get(&key)
                        .and_then(|member| session_boundaries.get(member))
                        .is_some_and(|&boundary| boundary > prev_ord))
            {
                *segment_no.entry(key.clone()).or_insert(0) += 1;
            } else {
                segment_no.entry(key.clone()).or_insert(0);
            }
            let segment = segment_no[&key];
            if let Some(session) = &session_id {
                member_session
                    .entry(key.clone())
                    .or_insert_with(|| session.clone());
                if let Some(member) = member_session.get(&key) {
                    session_boundaries.entry(member.clone()).or_insert(0);
                }
            }
            segment_of_ord.insert(ord, (key.clone(), segment));
            last_member_ord.insert(key, ord);
        }
        if let Some(session) = session_id {
            for (tracked, boundary_ord) in session_boundaries.iter_mut() {
                if *tracked != session {
                    *boundary_ord = ord;
                }
            }
        }
        if rec.merge_boundary {
            last_boundary_ord = Some(ord);
        }
    }

    let mut member_ords: BTreeMap<(&str, usize), Vec<usize>> = BTreeMap::new();
    for (ord, (key, segment)) in &segment_of_ord {
        member_ords
            .entry((key.as_str(), *segment))
            .or_default()
            .push(*ord);
    }
    // `segment_of_ord` is a HashMap — member order must follow packet order
    // for deterministic merge position and provenance.
    for members in member_ords.values_mut() {
        members.sort_unstable();
    }
    // Each multi-member segment's later members fold into the first: the
    // merged record keeps the first member's position and unions every
    // member's provenance positions (oracle `_merge_group` +
    // `RECORD_MESSAGE_ID_KEY`).
    let mut merged_into: HashMap<usize, usize> = HashMap::new();
    for members in member_ords.values() {
        if members.len() > 1 {
            for &ord in &members[1..] {
                merged_into.insert(ord, members[0]);
            }
        }
    }
    // Provenance lists per kept-record ordinal (uuid dedup may already
    // have grouped several packet positions into one member list).
    let members_by_ord: Vec<Vec<usize>> = kept.iter().map(|rec| rec.members.clone()).collect();
    let mut out: Vec<Rec> = Vec::new();
    for (ord, mut rec) in kept.into_iter().enumerate() {
        if merged_into.contains_key(&ord) {
            continue;
        }
        if let Some((key, segment)) = segment_of_ord.get(&ord)
            && let Some(members) = member_ords.get(&(key.as_str(), *segment))
            && members.len() > 1
        {
            rec.members = members
                .iter()
                .flat_map(|&member_ord| members_by_ord[member_ord].iter().copied())
                .collect();
        }
        out.push(rec);
    }
    out
}
