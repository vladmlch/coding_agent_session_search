//! Stable steps/turns/tools projection anchored to canonical messages
//! (spec decisions 4/5, plan Task 8).
//!
//! `project_session_structure` folds the normalized packet's per-message
//! `extra_json` evidence (verbatim provider records the connector preserved)
//! plus the verified [`ToolCallFact`] links from the facts pass into a
//! deterministic [`StructuralPublication`]: surviving projection steps with
//! per-message provenance, emitted turns, and call→result links. NO full
//! `SessionTrajectory` blob and no message-body copies are persisted — every
//! row is a structural pointer (`step_index`, `turn_index`, scope, canonical
//! `messages.idx`/`id`) plus derived-only part keys. `get_tool_output`
//! reconstructs output text from the canonical message at the anchor.
//!
//! Oracle alignment (`viewer/domain/passes/normalize/*`,
//! `viewer/backends/cass/canonical/{mappings,projection}.py`):
//!
//! * Physical uuid dedup keeps the first identical record and unions
//!   provenance; a uuid whose records DISAGREE is `conflicting_event` —
//!   dropped entirely (CASS facts semantics: conflict is unknown, never
//!   first-wins merged). Codex `response_item` payload-`id` replays dedup
//!   the same way.
//! * Claude assistant records sharing `(requestId|message.id, message.id)`
//!   merge into ONE logical step regardless of interleaved foreign records;
//!   merge/summary boundary records or a foreign `sessionId` between
//!   members split the group. Identical `text`/`thinking` blocks and
//!   same-`id` `tool_use` blocks dedup inside the merged step.
//! * A pure tool-result record produces NO step of its own; its verified
//!   pairing lands in `viewer_tool_calls.result_*` and the step that
//!   recorded the call gains a `scope='result'` member row — evidence-bound
//!   (the result record literally names the call id), richer than the
//!   oracle's lookahead bookkeeping and still never positional.
//! * Turns follow `pass_project_chat_turns` structure: a user step, a
//!   system step and each boundary/approval step are their own turn;
//!   consecutive assistant/error steps accumulate into one `ai` turn.
//!   `turn_index` is the 0-based emitted ordinal (native `turnIndex`).
//!   Steps that emit no turn (empty user/system prompts) keep `turn_index`
//!   NULL rather than guessing.
//! * Records the canonical mappings leave unmapped (unknown roles,
//!   attachment/control envelopes, `session_meta`/`turn_context`-style
//!   noise) produce no step — never coerced.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::{Context, Result};
use serde_json::{Value, json};
use sha2::Digest;

use crate::franken_sync::Connection;
use crate::franken_sync::compat::{ParamValue, RowExt, TransactionExt};
use crate::model::conversation_packet::{ConversationPacket, ConversationPacketProvenance};
use crate::model::types::Conversation;

use super::facts::{FactPublication, FactsDb, extract_session_facts};
use super::providers::{events, is_compacted_extra, json_text, text_id};

/// Bump when projection semantics change so persisted rows re-derive on
/// the next ingest pass (targeted recomputation via the
/// `viewer_session_projection` marker) instead of silently going stale.
pub const PROJECTION_VERSION: i64 = 1;

// ---------------------------------------------------------------------------
// Publication model
// ---------------------------------------------------------------------------

/// One `viewer_step_links` row: provenance (`scope = member|result`) or a
/// projected content part (`scope = text|thinking|tool_call|tool_result`).
#[derive(Debug, Clone, PartialEq)]
pub struct StepLink {
    /// 1-based surviving step ordinal (pre-filter steps are renumbered the
    /// same way the oracle renumbers survivors).
    pub step_index: i64,
    /// 0-based emitted turn ordinal; `None` when the step produced no turn.
    pub turn_index: Option<i64>,
    /// `user_message|system_message|turn|compact_boundary|error_message`.
    pub step_kind: String,
    /// Emission order of this row inside the step (member rows first, then
    /// absorbed-result members, then content parts).
    pub link_index: i64,
    /// `member` (record provenance), `result` (absorbed verified result
    /// record), or a part kind.
    pub scope: String,
    /// For `tool_call`/`tool_result` part rows: ordinal among that step's
    /// tool parts — the `(step_index, tool_index)` anchor component.
    pub block_index: Option<i64>,
    /// Scoped/verified call id for verified links, else the recorded raw id.
    pub call_id: Option<String>,
    /// Canonical `messages.idx` of the contributing message (stable anchor
    /// even when `message_id` is unresolved).
    pub source_idx: i64,
    /// `m<id>` when a canonical message id is known, else `i<idx>` — the
    /// facts `Event.key` convention.
    pub source_key: String,
    /// Canonical `messages.id` when resolved (packet replay id or the
    /// publish-time idx→id join).
    pub message_id: Option<i64>,
    /// Deterministic digest of the part's recorded identity for
    /// reindex-stability evidence (member/result rows carry none).
    pub part_key: Option<String>,
}

/// One `viewer_turn_links` row.
#[derive(Debug, Clone, PartialEq)]
pub struct TurnLink {
    pub turn_index: i64,
    /// `user|ai|system|compact_boundary|fork_boundary|approval`.
    pub turn_kind: String,
    pub first_step_index: i64,
    pub step_count: i64,
    /// Anchor message of the turn-opening step for singleton turns
    /// (user/system/boundary); `None` for accumulated `ai` turns.
    pub source_idx: Option<i64>,
    pub message_id: Option<i64>,
    pub source_key: Option<String>,
}

/// One `viewer_tool_calls` row — a verified call→result link.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolCallLink {
    /// Verified scoped call id (`codex-segment:*`, `devin-call:*`, raw).
    pub call_id: String,
    /// Provider-recorded id, byte-for-byte.
    pub raw_call_id: String,
    pub name: String,
    /// Step that recorded the call; `None` when its message produced none.
    pub step_index: Option<i64>,
    pub turn_index: Option<i64>,
    pub has_result: bool,
    pub is_error: Option<bool>,
    pub exit_code: Option<i64>,
    pub file_path: Option<String>,
    /// Call anchor: canonical `messages.idx`/`id` of the recording message.
    pub call_idx: i64,
    pub call_message_id: Option<i64>,
    /// Paired result anchor; `None` when `has_result` is false or no
    /// recorded result record claimed the link.
    pub result_idx: Option<i64>,
    pub result_message_id: Option<i64>,
    /// Ordinal among result-recording shapes inside the result message —
    /// the `block_index` `get_tool_output` needs for multi-output records.
    pub result_block_index: Option<i64>,
    pub ordinal: i64,
}

/// Deterministic structural projection of one session.
#[derive(Debug, Clone)]
pub struct StructuralPublication {
    pub projection_version: i64,
    /// Facts extractor version whose verified tools this projection used.
    pub extractor_version: i64,
    /// Content-addressed canonical marker (`semantic_hash` first 64 bits).
    pub source_revision: i64,
    pub provider: String,
    pub steps: Vec<StepLink>,
    pub turns: Vec<TurnLink>,
    pub tool_calls: Vec<ToolCallLink>,
    /// Exact reasons for dropped/unverifiable structure — never silent.
    pub warnings: Vec<String>,
}

/// Outcome of one structural publication attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionPublishOutcome {
    /// Link rows written and the marker updated.
    Published,
    /// `(projection_version, source_revision)` unchanged — write skipped.
    SkippedUnchanged,
    /// The `viewer_*` profile (schema v2) is not installed in this archive.
    ExtensionNotInstalled,
}

// ---------------------------------------------------------------------------
// Record classification
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Class {
    User,
    System,
    Assistant,
    Boundary,
    ToolResult,
    Error,
    Noise,
    Other,
}

struct Rec {
    /// Packet position of this surviving record.
    pos: usize,
    class: Class,
    /// Contributing packet positions (uuid-dedup + logical-merge provenance).
    members: Vec<usize>,
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
fn surviving_records(packet: &ConversationPacket, warnings: &mut Vec<String>) -> Vec<Rec> {
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

    let mut member_ords: BTreeMap<(String, usize), Vec<usize>> = BTreeMap::new();
    for (ord, (key, segment)) in &segment_of_ord {
        member_ords
            .entry((key.clone(), *segment))
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
            && let Some(members) = member_ords.get(&(key.clone(), *segment))
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

/// Verified-call lookup: `(recording idx, recorded raw id)` → scoped id.
type VerifiedCalls<'a> = HashMap<(i64, String), &'a str>;

fn verified_call_map<'a>(facts: &'a FactPublication) -> VerifiedCalls<'a> {
    let mut map = VerifiedCalls::new();
    for tool in &facts.tools {
        map.entry((tool.source_idx, tool.raw_call_id.clone()))
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
) -> Vec<Part> {
    let mut parts = Vec::new();
    for &pos in &rec.members {
        let message = &msgs[pos];
        let extra = &message.extra_json;
        if is_compacted_extra(extra) {
            // `{"cass":{…}}` skeleton — the recorded structure was compacted
            // by normalization; only the flattened text remains.
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
                        .get(&(message.idx, raw_id.clone()))
                        .map(|scoped| scoped.to_string())
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
                                .get(&(message.idx, raw_id.clone()))
                                .map(|scoped| scoped.to_string())
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
                .get(&(message.idx, raw_id.clone()))
                .map(|scoped| scoped.to_string())
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

fn source_key(message_id: Option<i64>, idx: i64) -> String {
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
        let parts = record_parts(rec, msgs, &verified);
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
        let mut seen: BTreeSet<String> = BTreeSet::new();
        step.parts
            .retain(|part| seen.insert(part.signature.clone()));
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
    let mut sorted_facts: Vec<&super::facts::ToolCallFact> = facts.tools.iter().collect();
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

// ---------------------------------------------------------------------------
// Publication
// ---------------------------------------------------------------------------

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

/// Idempotent structural upsert keyed on `(projection_version,
/// source_revision)` in `viewer_session_projection`. A changed projection is
/// a deterministic delete+replace inside the caller's transaction plus a
/// `projection_revision` bump — same logical-revision discipline as the
/// canonical observer triggers (never bumped on a skip).
pub fn publish_session_structure(
    db: &impl FactsDb,
    conversation_id: i64,
    publication: &StructuralPublication,
) -> Result<ProjectionPublishOutcome> {
    if db
        .facts_query_map(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='viewer_session_projection' LIMIT 1",
            &[],
            |_| Ok(()),
        )?
        .is_empty()
    {
        return Ok(ProjectionPublishOutcome::ExtensionNotInstalled);
    }

    let existing = db.facts_query_map(
        "SELECT projection_version, source_revision, extractor_version \
         FROM viewer_session_projection WHERE conversation_id = ?1",
        &[ParamValue::from(conversation_id)],
        |row| {
            Ok((
                row.get_typed::<i64>(0)?,
                row.get_typed::<i64>(1)?,
                row.get_typed::<i64>(2)?,
            ))
        },
    )?;
    if existing
        .first()
        .is_some_and(|&(version, revision, extractor)| {
            version == publication.projection_version
                && revision == publication.source_revision
                && extractor == publication.extractor_version
        })
    {
        return Ok(ProjectionPublishOutcome::SkippedUnchanged);
    }

    // Canonical anchor resolution: packet message ids when replayed,
    // else the idx→id join against committed rows. Unresolved anchors stay
    // NULL (source_idx remains the honest coordinate).
    let mut id_by_idx: HashMap<i64, i64> = HashMap::new();
    for row in db.facts_query_map(
        "SELECT id, idx FROM messages WHERE conversation_id = ?1",
        &[ParamValue::from(conversation_id)],
        |row| Ok((row.get_typed::<i64>(0)?, row.get_typed::<i64>(1)?)),
    )? {
        id_by_idx.entry(row.1).or_insert(row.0);
    }
    let resolve = |packet_id: Option<i64>, idx: i64| -> Option<i64> {
        packet_id.or_else(|| id_by_idx.get(&idx).copied())
    };
    let keyed =
        |packet_id: Option<i64>, idx: i64| -> String { source_key(resolve(packet_id, idx), idx) };

    for table in [
        "viewer_step_links",
        "viewer_turn_links",
        "viewer_tool_calls",
    ] {
        db.facts_execute(
            &format!("DELETE FROM {table} WHERE conversation_id = ?1"),
            &[ParamValue::from(conversation_id)],
        )?;
    }

    for link in &publication.steps {
        let message_id = resolve(link.message_id, link.source_idx);
        db.facts_execute(
            "INSERT INTO viewer_step_links (\
                conversation_id, step_index, turn_index, step_kind, link_index, \
                scope, block_index, call_id, source_idx, source_key, message_id, \
                part_key\
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
            &[
                ParamValue::from(conversation_id),
                ParamValue::from(link.step_index),
                ParamValue::from(link.turn_index),
                ParamValue::from(link.step_kind.as_str()),
                ParamValue::from(link.link_index),
                ParamValue::from(link.scope.as_str()),
                ParamValue::from(link.block_index),
                ParamValue::from(link.call_id.as_deref()),
                ParamValue::from(link.source_idx),
                ParamValue::from(keyed(link.message_id, link.source_idx)),
                ParamValue::from(message_id),
                ParamValue::from(link.part_key.as_deref()),
            ],
        )?;
    }
    for turn in &publication.turns {
        let message_id = turn
            .source_idx
            .and_then(|idx| resolve(turn.message_id, idx));
        db.facts_execute(
            "INSERT INTO viewer_turn_links (\
                conversation_id, turn_index, turn_kind, first_step_index, \
                step_count, source_idx, message_id, source_key\
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
            &[
                ParamValue::from(conversation_id),
                ParamValue::from(turn.turn_index),
                ParamValue::from(turn.turn_kind.as_str()),
                ParamValue::from(turn.first_step_index),
                ParamValue::from(turn.step_count),
                ParamValue::from(turn.source_idx),
                ParamValue::from(message_id),
                ParamValue::from(turn.source_idx.map(|idx| keyed(turn.message_id, idx))),
            ],
        )?;
    }
    for tool in &publication.tool_calls {
        let call_message_id = resolve(tool.call_message_id, tool.call_idx);
        let result_message_id = tool
            .result_idx
            .and_then(|idx| resolve(tool.result_message_id, idx));
        db.facts_execute(
            "INSERT INTO viewer_tool_calls (\
                conversation_id, call_id, raw_call_id, name, step_index, \
                turn_index, has_result, is_error, exit_code, file_path, \
                call_idx, call_message_id, result_idx, result_message_id, \
                result_block_index, ordinal\
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)",
            &[
                ParamValue::from(conversation_id),
                ParamValue::from(tool.call_id.as_str()),
                ParamValue::from(tool.raw_call_id.as_str()),
                ParamValue::from(tool.name.as_str()),
                ParamValue::from(tool.step_index),
                ParamValue::from(tool.turn_index),
                ParamValue::from(i64::from(tool.has_result)),
                ParamValue::from(tool.is_error.map(i64::from)),
                ParamValue::from(tool.exit_code),
                ParamValue::from(tool.file_path.as_deref()),
                ParamValue::from(tool.call_idx),
                ParamValue::from(call_message_id),
                ParamValue::from(tool.result_idx),
                ParamValue::from(result_message_id),
                ParamValue::from(tool.result_block_index),
                ParamValue::from(tool.ordinal),
            ],
        )?;
    }

    db.facts_execute(
        "INSERT OR REPLACE INTO viewer_session_projection (\
            conversation_id, projection_version, extractor_version, \
            source_revision, step_count, turn_count, tool_call_count, \
            projected_at_ms\
         ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
        &[
            ParamValue::from(conversation_id),
            ParamValue::from(publication.projection_version),
            ParamValue::from(publication.extractor_version),
            ParamValue::from(publication.source_revision),
            ParamValue::from(
                publication
                    .steps
                    .iter()
                    .map(|link| link.step_index)
                    .max()
                    .unwrap_or(0),
            ),
            ParamValue::from(publication.turns.len() as i64),
            ParamValue::from(publication.tool_calls.len() as i64),
            ParamValue::from(now_ms()),
        ],
    )?;

    // Logical projection revision: bumps only on a committed content
    // change, inside the same transaction.
    db.facts_execute(
        "INSERT OR IGNORE INTO viewer_revisions (id) VALUES (1)",
        &[],
    )?;
    db.facts_execute(
        "UPDATE viewer_revisions SET projection_revision = projection_revision + 1, \
         updated_at_ms = ?1 WHERE id = 1",
        &[ParamValue::from(now_ms())],
    )?;
    Ok(ProjectionPublishOutcome::Published)
}

fn provenance_of(conv: &Conversation) -> ConversationPacketProvenance {
    let origin_kind = conv
        .metadata_json
        .pointer("/cass/origin/kind")
        .and_then(Value::as_str)
        .unwrap_or("local")
        .to_string();
    ConversationPacketProvenance {
        source_id: conv.source_id.clone(),
        origin_kind,
        origin_host: conv.origin_host.clone(),
    }
}

/// Structural publication for an internal canonical `Conversation` — the
/// same payload bytes that were committed.
pub fn structure_for_canonical(conv: &Conversation) -> StructuralPublication {
    let packet = ConversationPacket::from_canonical_replay(conv, provenance_of(conv));
    let facts = extract_session_facts(&packet, &conv.agent_slug);
    project_session_structure(&packet, &facts)
}

/// Adjacent-transaction structural publication used by the ingest hook:
/// one explicit transaction on the writer connection, idempotent on
/// `(projection_version, source_revision)`. Errors are logged, never
/// fatal — missing links read as `unavailable` sections rather than
/// aborting the canonical ingest commit that already succeeded.
pub fn publish_structure_for_canonical_best_effort(
    connection: &Connection,
    conv: &Conversation,
    conversation_id: i64,
) {
    let result = (|| -> Result<ProjectionPublishOutcome> {
        let mut tx = connection
            .transaction()
            .context("opening viewer structure projection transaction")?;
        let outcome =
            publish_session_structure(&tx, conversation_id, &structure_for_canonical(conv))?;
        tx.commit()
            .context("committing viewer structure projection transaction")?;
        Ok(outcome)
    })();
    if let Err(error) = result {
        tracing::warn!(
            conversation_id,
            error = %error,
            "viewer structure projection publication failed; detail structure remains unavailable"
        );
    }
}

/// Batched sibling of [`publish_structure_for_canonical_best_effort`]: one
/// adjacent transaction covers every `(conversation, conversation_id)`
/// pair so the serial ingest path pays a single commit.
pub fn publish_structure_for_canonical_batch_best_effort<'a>(
    connection: &Connection,
    pairs: impl IntoIterator<Item = (&'a Conversation, i64)>,
) {
    let result = (|| -> Result<()> {
        let mut tx = connection
            .transaction()
            .context("opening viewer structure projection batch transaction")?;
        for (conv, conversation_id) in pairs {
            publish_session_structure(&tx, conversation_id, &structure_for_canonical(conv))?;
        }
        tx.commit()
            .context("committing viewer structure projection batch transaction")?;
        Ok(())
    })();
    if let Err(error) = result {
        tracing::warn!(
            error = %error,
            "viewer structure projection batch publication failed; detail structure remains unavailable"
        );
    }
}

/// `events` re-export guard: keep the provider event normalization the
/// single source of `key/idx/message_id` mapping for future consumers.
#[allow(dead_code)]
fn _packet_events(packet: &ConversationPacket) -> usize {
    events(packet).len()
}

// ---------------------------------------------------------------------------
// Ingest-lane coherent generation publication (completion task)
// ---------------------------------------------------------------------------
//
// Facts and the structural projection are ONE extension generation of a
// canonical source revision: they must be computed from the same replayed
// bytes and committed — with their durable receipt — inside a single
// transaction. A crash before that commit leaves the pre-attempt state
// (the prior receipt — possibly a `complete` at an older revision, honest
// for that revision, since its rows still exist — or no receipt at all);
// a failed generation leaves the exact reason; only rows+receipt
// committed together may ever read `complete`.
//
// Recovery is the explicit `--backfill-viewer-extensions` lane: it scans
// canonical conversations and republishes through THIS same code, so
// `failed`/missing receipts — and stale `complete` receipts at older
// revisions — converge without a startup or request-time reconstruction
// path.

/// Facts + structure computed from ONE canonical replay — the generation
/// unit the ingest lane publishes. `project_session_structure` copies
/// `facts.source_revision`, so the two sides are revision-coherent by
/// construction; the durable [`facts::IngestReceipt`] makes that coherence
/// auditable (the receipt's `source_revision` is the generation's own
/// revision, certified only when the rows commit beside it).
pub struct IngestGeneration {
    pub facts: FactPublication,
    pub structure: StructuralPublication,
    /// The shared replay both lanes derived from — the search-region
    /// publication reuses it so every lane binds the identical committed
    /// bytes and `source_revision`.
    pub packet: crate::model::conversation_packet::ConversationPacket,
}

/// Outcome of one coherent generation publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GenerationPublishOutcome {
    /// Facts, structure, parent fixup and the `complete` receipt committed
    /// in one transaction.
    Published,
    /// A `complete` receipt already binds this exact
    /// `(source_revision, extractor_version, projection_version)` — the
    /// durable idempotence key. Nothing was rewritten, so unchanged
    /// reingest moves no row and no revision counter.
    SkippedUnchanged,
    /// Crash-probe short-circuit (test-only): the generation transaction
    /// never committed — the pre-attempt durable state (the prior receipt
    /// or none) is exactly what a pre-commit kill leaves.
    Interrupted,
}

/// The durable-visibility half of `complete`: a receipt only certifies a
/// generation when BOTH lane markers are committed at the bound
/// `source_revision`. A receipt claiming `complete` over absent or stale
/// rows is divergence, never coverage — callers republish rather than
/// trusting it, so manual/accidental extension-row loss self-heals on the
/// next ingest pass or backfill instead of masquerading as complete.
pub fn generation_markers_visible(
    db: &impl FactsDb,
    conversation_id: i64,
    source_revision: i64,
) -> Result<bool> {
    let facts_present = !db
        .facts_query_map(
            "SELECT 1 FROM viewer_session_facts \
             WHERE conversation_id = ?1 AND source_revision = ?2 LIMIT 1",
            &[
                ParamValue::from(conversation_id),
                ParamValue::from(source_revision),
            ],
            |_| Ok(()),
        )?
        .is_empty();
    if !facts_present {
        return Ok(false);
    }
    let structure_present = !db
        .facts_query_map(
            "SELECT 1 FROM viewer_session_projection \
             WHERE conversation_id = ?1 AND source_revision = ?2 LIMIT 1",
            &[
                ParamValue::from(conversation_id),
                ParamValue::from(source_revision),
            ],
            |_| Ok(()),
        )?
        .is_empty();
    if !structure_present {
        return Ok(false);
    }
    // The Task-5 region lane is the third marker `complete` must bind: a
    // receipt over a missing/stale region inventory is divergence the
    // republish below repairs in the same commit. The marks table only
    // exists at schema v7+; profiles at v6 return false here too (the
    // `sqlite_master` probe keeps the check engine-safe), so an upgraded
    // binary republishes every session once and converges.
    let marks_table = !db
        .facts_query_map(
            "SELECT 1 FROM sqlite_master WHERE type='table' \
             AND name='viewer_search_region_marks' LIMIT 1",
            &[],
            |_| Ok(()),
        )?
        .is_empty();
    if !marks_table {
        return Ok(false);
    }
    Ok(!db
        .facts_query_map(
            "SELECT 1 FROM viewer_search_region_marks \
             WHERE conversation_id = ?1 AND source_revision = ?2 \
             AND region_version = ?3 LIMIT 1",
            &[
                ParamValue::from(conversation_id),
                ParamValue::from(source_revision),
                ParamValue::from(super::search_regions::REGION_VERSION),
            ],
            |_| Ok(()),
        )?
        .is_empty())
}

/// Deterministic crash injection for the receipt protocol's restart path:
/// `CASS_TEST_VIEWER_GENERATION_CRASH` returns early BEFORE the generation
/// transaction writes anything — the durable state any pre-commit kill
/// leaves now that the receipt rides the transaction (no receipt for the
/// attempted revision, no generation rows; the prior receipt, if any,
/// stays honest for its own revision).
fn generation_crash_probe() -> bool {
    #[cfg(test)]
    if test_generation_crash_probe_override() {
        return true;
    }
    dotenvy::var("CASS_TEST_VIEWER_GENERATION_CRASH")
        .map(|value| !value.is_empty() && value != "0")
        .unwrap_or(false)
}

/// Bounded re-resolve attempts for the evidence-guarded tie write. A
/// canonical identity change between the resolve probes and the
/// in-transaction guarded write costs one attempt: three races in a row
/// means something is actively mutating the row mid-index — the durable
/// `failed` receipt then records the race and the backfill lane retries,
/// instead of spinning unbounded inside one index run.
const TIE_EVIDENCE_RACE_ATTEMPTS: usize = 3;

/// Deterministic failure injection inside the generation transaction:
/// `CASS_TEST_VIEWER_GENERATION_FAIL` aborts AFTER both lanes published
/// but BEFORE commit — the whole generation (rows AND the in-tx `complete`
/// receipt) rolls back together, so the archive can never hold a facts row
/// without its structure.
fn generation_fail_probe() -> bool {
    #[cfg(test)]
    if test_generation_fail_probe_override() {
        return true;
    }
    dotenvy::var("CASS_TEST_VIEWER_GENERATION_FAIL")
        .map(|value| !value.is_empty() && value != "0")
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Test-only instrumentation for the publication lane's durable-commit budget.
//
// `TEST_PUBLICATION_COMMIT_CALLS` counts the durable-commit calls
// `publish_ingest_generation` issues on this thread: the generation
// transaction commit, and — on the error path only — the standalone
// `failed` receipt write (an autocommit commit). The publication lane is
// synchronous on the caller's thread, so a thread-local counter keeps
// parallel lib tests from counting each other's commits. Lib tests use it
// to pin the per-conversation commit count without reaching into the
// engine; it compiles away entirely in production builds. The probe
// overrides below are thread-local for the same reason.
// ---------------------------------------------------------------------------

#[cfg(test)]
thread_local! {
    static TEST_PUBLICATION_COMMIT_CALLS: std::cell::Cell<u64> =
        const { std::cell::Cell::new(0) };
}

/// Record one durable-commit call in the publication lane (test builds only).
#[cfg(test)]
fn note_publication_commit_call() {
    TEST_PUBLICATION_COMMIT_CALLS.with(|calls| calls.set(calls.get() + 1));
}

/// Durable-commit calls issued by the publication lane on this thread so far
/// (test builds only).
#[cfg(test)]
pub(crate) fn test_publication_commit_calls() -> u64 {
    TEST_PUBLICATION_COMMIT_CALLS.with(std::cell::Cell::get)
}

/// Reset the publication-lane commit-call counter (test builds only).
#[cfg(test)]
pub(crate) fn test_reset_publication_commit_calls() {
    TEST_PUBLICATION_COMMIT_CALLS.with(|calls| calls.set(0));
}

#[cfg(test)]
thread_local! {
    static TEST_GENERATION_CRASH_PROBE: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
    static TEST_GENERATION_FAIL_PROBE: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

#[cfg(test)]
fn test_generation_crash_probe_override() -> bool {
    TEST_GENERATION_CRASH_PROBE.with(std::cell::Cell::get)
}

#[cfg(test)]
pub(crate) fn test_set_generation_crash_probe(enabled: bool) {
    TEST_GENERATION_CRASH_PROBE.with(|probe| probe.set(enabled));
}

#[cfg(test)]
fn test_generation_fail_probe_override() -> bool {
    TEST_GENERATION_FAIL_PROBE.with(std::cell::Cell::get)
}

#[cfg(test)]
pub(crate) fn test_set_generation_fail_probe(enabled: bool) {
    TEST_GENERATION_FAIL_PROBE.with(|probe| probe.set(enabled));
}

/// Deterministic stale-tie-race injection (test-only):
/// `CASS_TEST_VIEWER_TIE_RACE_SQL` holds SQL executed on this connection
/// ONCE per publication attempt-1 — after the tie evidence/resolve probes
/// and before the generation transaction opens. That is exactly the
/// interleaving of a cross-connection canonical identity write racing the
/// publication: the conversations trigger NULLs `tie_key`, and the
/// evidence-guarded write must refuse the stale key rather than persist it.
fn tie_race_probe() -> Option<String> {
    dotenvy::var("CASS_TEST_VIEWER_TIE_RACE_SQL")
        .ok()
        .filter(|sql| !sql.is_empty())
}

/// Compute the whole extension generation for one committed canonical
/// `Conversation` with a single `ConversationPacket::from_canonical_replay`
/// — the same replay `facts_for_canonical` and `structure_for_canonical`
/// each run on their own today, shared here so both sides bind the
/// identical committed bytes (and therefore the identical
/// `source_revision`). Canonical token/cost totals fill `facts.health`
/// through the same `fill_canonical_health` read
/// `publish_facts_for_canonical` performs.
pub fn generation_for_canonical(
    db: &impl FactsDb,
    conv: &Conversation,
    conversation_id: i64,
) -> Result<IngestGeneration> {
    let packet = ConversationPacket::from_canonical_replay(conv, provenance_of(conv));
    let mut facts = extract_session_facts(&packet, &conv.agent_slug);
    super::facts::fill_canonical_health(db, conversation_id, &mut facts)?;
    let structure = project_session_structure(&packet, &facts);
    Ok(IngestGeneration {
        facts,
        structure,
        packet,
    })
}

/// The generation transaction body — callable on any `FactsDb` (an open
/// transaction, or a savepoint body inside the backfill batch). Publishes
/// facts, then structure, then the targeted child-before-parent fixup,
/// then the `complete` receipt. `receipt` must carry
/// `state == RECEIPT_COMPLETE` bound to `generation`'s source revision.
/// Returns the per-lane publication outcomes so callers (the backfill
/// lane's honest stats) can distinguish writes from idempotence skips.
///
/// Nothing here commits: commit/rollback stays with the caller's
/// transaction boundary, so every piece of the generation — rows AND
/// receipt — lands or vanishes together.
pub fn publish_ingest_generation_in_tx(
    db: &impl FactsDb,
    conv: &Conversation,
    conversation_id: i64,
    generation: &IngestGeneration,
    receipt: &super::facts::IngestReceipt,
) -> Result<(super::facts::FactPublishOutcome, ProjectionPublishOutcome)> {
    let facts_outcome =
        match super::facts::publish_session_facts(db, conversation_id, &generation.facts)? {
            super::facts::FactPublishOutcome::ExtensionNotInstalled => {
                anyhow::bail!("viewer_session_facts missing inside an ensured extension profile");
            }
            outcome => outcome,
        };
    let structure_outcome =
        match publish_session_structure(db, conversation_id, &generation.structure)? {
            ProjectionPublishOutcome::ExtensionNotInstalled => {
                anyhow::bail!(
                    "viewer_session_projection missing inside an ensured extension profile"
                );
            }
            outcome => outcome,
        };
    // Child-before-parent: this generation may BE the parent evidence a
    // previously-ingested child is still waiting on. The fixup is scoped
    // to rows that durably recorded this exact `external_id` — no catalog
    // rescan, no child republish.
    super::facts::fixup_pending_parent_links(
        db,
        conversation_id,
        &generation.facts.provider,
        &conv.source_id,
        conv.external_id.as_deref().unwrap_or_default(),
    )?;
    // Search regions publish AFTER the parent fixup: a child-before-parent
    // landing here may have just linked prior children to this session,
    // and its `subagents` meta regions must see those links in the same
    // generation commit. When this session itself resolved to a parent,
    // the parent's `subagents` regions gain a new descriptor — targeted
    // meta replacement on that parent's rows inside the same transaction
    // (its message-bound region ordinals never renumber).
    super::search_regions::publish_search_regions(
        db,
        conversation_id,
        &generation.packet,
        &generation.structure,
    )?;
    let resolved_parent: Option<i64> = db
        .facts_query_map(
            "SELECT parent_conversation_id FROM viewer_session_facts \
             WHERE conversation_id = ?1",
            &[crate::franken_sync::compat::ParamValue::from(
                conversation_id,
            )],
            |row| row.get_typed::<Option<i64>>(0),
        )?
        .into_iter()
        .next()
        .flatten();
    if let Some(parent_id) = resolved_parent
        && parent_id != conversation_id
    {
        super::search_regions::replace_meta_regions(db, parent_id)?;
    }
    super::facts::write_ingest_receipt(db, receipt)?;
    Ok((facts_outcome, structure_outcome))
}

/// Publish ONE coherent extension generation for a just-committed
/// canonical `Conversation` on a bare writer connection:
///
/// 1. `ensure_profile_for_ingest` — the additive profile exists before
///    any extraction/publication touches it.
/// 2. Compute the generation once (shared replay → shared
///    `source_revision`).
/// 3. Skip when a `complete` receipt already binds this exact generation.
/// 4. Commit the whole generation in ONE durable commit: facts + structure +
///    parent fixup + search/meta regions + sort tie key + the `complete`
///    receipt all ride one transaction. The receipt only becomes durable
///    with the rows it certifies — one fewer durable commit per published
///    conversation than the old standalone `pending` mark.
/// 5. On error, commit a `failed` receipt carrying the exact reason, and
///    return the error (canonical ingest is unaffected — the caller
///    already committed).
///
/// A crash before step 4's commit leaves the pre-attempt state: no receipt
/// for the attempted revision, or the prior receipt — possibly a `complete`
/// at an older revision, honest for that revision since its rows still
/// exist. Both the next ingest run's skip check (revision-keyed: a
/// mismatch never skips) and the backfill lane's full revision-keyed
/// conversation scan republish it. No state here can ever masquerade as
/// `complete` for the current revision.
pub fn publish_ingest_generation(
    connection: &Connection,
    conv: &Conversation,
    conversation_id: i64,
) -> Result<GenerationPublishOutcome> {
    super::schema::ensure_profile_for_ingest(connection)
        .context("ensuring viewer extension profile for ingest publication")?;

    let generation = generation_for_canonical(connection, conv, conversation_id)
        .context("building extension generation from committed canonical bytes")?;
    let source_revision = generation.facts.source_revision;

    let previous = super::facts::ingest_receipt_for(connection, conversation_id)?;
    if let Some(receipt) = &previous
        && receipt.state == super::facts::RECEIPT_COMPLETE
        && receipt.source_revision == source_revision
        && receipt.extractor_version == super::facts::FACTS_EXTRACTOR_VERSION
        && receipt.projection_version == PROJECTION_VERSION
        // `complete` is only honest while both lane markers are durably
        // visible at the bound revision — the task's definition of a
        // completed generation. A receipt over missing rows is divergence:
        // republish and let the same commit rewrite the receipt truthfully.
        && generation_markers_visible(connection, conversation_id, source_revision)?
    {
        return Ok(GenerationPublishOutcome::SkippedUnchanged);
    }

    let mut receipt = super::facts::IngestReceipt {
        conversation_id,
        state: super::facts::RECEIPT_PENDING.to_string(),
        source_revision,
        extractor_version: super::facts::FACTS_EXTRACTOR_VERSION,
        projection_version: PROJECTION_VERSION,
        reason: None,
        attempts: previous.map(|r| r.attempts + 1).unwrap_or(1),
        updated_at_ms: now_ms(),
    };
    // The receipt rides the generation transaction below — it only becomes
    // durable when the rows it certifies do, so a published conversation
    // costs ONE durable commit instead of two. A crash before that commit
    // leaves the pre-attempt state (the prior receipt — a `complete` at an
    // older revision, honest for that revision — or none at all); the
    // revision-keyed skip check and the backfill scan both repair it.
    // Never a false `complete`, never a silently missing row-set.
    if generation_crash_probe() {
        return Ok(GenerationPublishOutcome::Interrupted);
    }

    receipt.state = super::facts::RECEIPT_COMPLETE.to_string();
    let work = (|| -> Result<()> {
        // Recompute the browse tie key (verified public identity) on the
        // just-committed canonical row so its persisted value lands inside
        // the same generation transaction as the facts/structure rows it
        // orders browse alongside. A resolver refusal is durable sort state
        // — `tie_error` — never a reason to abort the publication; engine
        // failures DO abort it so the `failed` receipt stays honest.
        //
        // The resolve runs outside the generation transaction; a
        // cross-connection canonical identity change can land between it
        // and the in-transaction write (the conversations trigger NULLs
        // `tie_key`, then a blind overwrite would persist a stale verified
        // key nothing re-invalidates). The evidence-guarded write re-reads
        // the identity columns inside the transaction and refuses on
        // mismatch — the loop rolls back and re-resolves, so the committed
        // key always verifies against the canonical row it commits beside.
        for attempt in 1..=TIE_EVIDENCE_RACE_ATTEMPTS {
            let (sort_tie, evidence) =
                super::sort::resolve_published_tie_with_evidence(connection, conversation_id)
                    .context("resolving browse sort tie key")?;
            if attempt == 1
                && let Some(race_sql) = tie_race_probe()
            {
                connection
                    .execute_batch(&race_sql)
                    .context("injecting tie-race canonical mutation")?;
            }
            let mut tx = connection
                .transaction()
                .context("opening ingest generation transaction")?;
            publish_ingest_generation_in_tx(&tx, conv, conversation_id, &generation, &receipt)?;
            if super::sort::publish_sort_tie_guarded(
                &tx,
                conversation_id,
                &sort_tie,
                evidence.as_ref(),
            )
            .context("persisting browse sort tie key")?
            {
                if generation_fail_probe() {
                    anyhow::bail!("CASS_TEST_VIEWER_GENERATION_FAIL injected publication fault");
                }
                tx.commit()
                    .context("committing ingest generation transaction")?;
                #[cfg(test)]
                note_publication_commit_call();
                return Ok(());
            }
            // The guard refused: canonical identity moved between the
            // resolve probes and the in-transaction write. Roll the whole
            // generation attempt back (dropping discharges the mandatory
            // rollback before the next statement) and re-resolve against
            // the moved row — never persist the stale key.
            drop(tx);
        }
        anyhow::bail!(
            "canonical identity changed during ingest publication \
             {TIE_EVIDENCE_RACE_ATTEMPTS} times; the durable failed receipt \
             records the race and the backfill lane retries"
        )
    })();

    match work {
        Ok(()) => Ok(GenerationPublishOutcome::Published),
        Err(error) => {
            // The failed mark is a separate durable commit: the generation
            // rolled back, but the archive must remember that THIS source
            // revision's publication was attempted and why it did not land.
            receipt.state = super::facts::RECEIPT_FAILED.to_string();
            receipt.reason = Some(format!("{error:#}"));
            receipt.updated_at_ms = now_ms();
            if let Err(mark_error) = super::facts::write_ingest_receipt(connection, &receipt) {
                tracing::warn!(
                    conversation_id,
                    error = %mark_error,
                    "failed to record the ingest publication failure durably"
                );
            } else {
                #[cfg(test)]
                note_publication_commit_call();
            }
            Err(error)
        }
    }
}

/// Best-effort single-conversation hook: publish the coherent generation,
/// logging any failure without aborting the (already committed) canonical
/// ingest. The durable receipt records the exact reason either way.
pub fn publish_ingest_generation_best_effort(
    connection: &Connection,
    conv: &Conversation,
    conversation_id: i64,
) {
    if let Err(error) = publish_ingest_generation(connection, conv, conversation_id) {
        tracing::warn!(
            conversation_id,
            error = %error,
            "viewer extension generation publication failed; \
             the durable ingest receipt records the reason and the \
             --backfill-viewer-extensions lane reconciles it"
        );
    }
}

/// Best-effort batch hook: the same coherent-generation protocol per
/// conversation, so a single bad row leaves an honest `failed` receipt
/// instead of aborting the batch's other work.
pub fn publish_ingest_generations_batch_best_effort<'a>(
    connection: &Connection,
    pairs: impl IntoIterator<Item = (&'a Conversation, i64)>,
) {
    for (conv, conversation_id) in pairs {
        if let Err(error) = publish_ingest_generation(connection, conv, conversation_id) {
            tracing::warn!(
                conversation_id,
                error = %error,
                "viewer extension generation publication failed; \
                 the durable ingest receipt records the reason and the \
                 --backfill-viewer-extensions lane reconciles it"
            );
        }
    }
}
