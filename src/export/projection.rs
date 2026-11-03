//! Shared provider-neutral export projection.
//!
//! Projects a resolved [`ConversationView`] (indexed data only) into the
//! agent-neutral `ExportConversation`/`ExportMessage` model consumed by the
//! Markdown, text, JSON and HTML renderers. Renderers must stay free of
//! provider-format sniffing (`.db`, `.jsonl`, `.zst`, `response_item`, ...).
//!
//! SDD stub for `docs/superpowers/plans/2026-11-02-index-first-export.md`
//! (Task 3). Implement the bodies below; keep these signatures stable —
//! `run_export`, `run_export_html` and the TUI export tasks are wired
//! against them.
//!
//! ## Projection policy (plan Invariants 4-6)
//!
//! - **Roles:** `User`/`Agent`/`Tool`/`System` map to
//!   `user`/`assistant`/`tool`/`system`. `Other(s)` keeps `s` verbatim in
//!   `role`, records it in `source_role` + `stats.unknown_roles`, and emits a
//!   `tracing::warn!` diagnostic — an explicit policy instead of silently
//!   filtering or rewriting it to `"unknown"`.
//! - **Order:** messages are emitted in `idx` order. The sort is defensive —
//!   callers must not rely on the storage fetch order; ties keep archive
//!   order (stable sort by `(idx, input position)`).
//! - **Tools:** `ExportMessage.tool` carries the *tool-relevant subset* of
//!   the archived source envelope (`extra_json`), decoded through
//!   [`crate::storage::sqlite::message_extra_to_source_jsonl_line`]. It is
//!   `Some` only when the index actually retained tool data *and* the caller
//!   set `include_tools`; absence is exposed honestly (`tool: None`), never
//!   fabricated from source files. Extraction is counted unconditionally in
//!   `stats.tool_payloads_indexed` so `tool_payloads_indexed -
//!   tool_payloads_emitted` reports "captured but suppressed by
//!   `--include-tools=false`". A `MessageRole::Tool` message is itself tool
//!   data (a call or a result), not ordinary conversation text: under
//!   `include_tools=false` it leaves `ExportConversation.messages` entirely
//!   (`stats.tool`/`tool_payloads_indexed` still count it — the counters
//!   describe the index); under `=true` it emits `role: "tool"` with any
//!   archived payload attached.
//!
//!   Recognized envelope shapes (all verbatim subsets, nothing rewritten):
//!   1. `{"type": .., "message": {..}}` — pi-agent/OMP, Claude Code, Cursor.
//!      - `message.role == "toolResult"` (pi-family): the whole inner message
//!        is tool data (`toolCallId`, `toolName`, `content`, `isError`).
//!      - otherwise: `message` reduced to `{role?, content: [tool blocks]}`
//!        where tool blocks are `type` ∈ `tool_use`/`tool_result`/`toolCall`,
//!        plus any flat tool keys present (`tool_use_id`, `toolName`, ...).
//!   2. Top-level `content` array with tool blocks → wrapped under
//!      `{"message": {"content": [blocks]}}` so renderers find
//!      `message.content` where they look.
//!   3. `{"payload": {..}}` (Codex `response_item`): payload exposed whole
//!      when `payload.type` is a tool type (`function_call`,
//!      `function_call_output`, `custom_tool_call`, `local_shell_call`, ...).
//!      No current renderer consumes `payload`; the field is honest
//!      index-retained data available to JSON consumers and future renderers.
//!   4. The event itself is a bare tool block (`{"type":"tool_use",..}`) →
//!      wrapped as `{"message": {"content": [event]}}`.
//!   5. Flat tool records (`{"role":"toolResult",..}`, `{"type":"tool",..}`,
//!      `{"source":"tool_result",..}`, or any event carrying flat tool keys
//!      like `toolCallId`/`tool_use_id`) → the event is exposed whole;
//!      `toolResult` is nested under `message` for renderer detection.
//!   6. `{"cass":{"invocations":[..]}}` — CASS-native Antigravity planner
//!      calls (`NormalizedInvocation` records dual-written into `extra.cass`
//!      because `NormalizedMessage.invocations` is not persisted). Each
//!      named entry projects verbatim as a `tool_use` content block under
//!      `message.content`, with `arguments` additionally aliased to `input`
//!      for the markdown/text renderers; `call_id`/`name`/`kind` pass
//!      through unchanged. Other `cass` keys (`model`, `token_usage`, ...)
//!      are metadata and yield `tool: None`.
//!
//!   In every case the subset may carry `type`, `message` or `payload`; the
//!   legacy raw keys `role`/`content`/`timestamp`/`author` are never emitted
//!   at the top level and never overwritten by `to_raw_messages` merging.
//! - **No false success:** `NoRenderableMessages` fires when the index had
//!   messages but *every* one is either excluded (Tool-role under
//!   `include_tools=false`) or carries a role outside the renderer-emitting
//!   set (`user`/`assistant`/`system`/`tool`/`unknown` — the allowlist used
//!   by `format_as_*` and the consolidated HTML path). An empty *indexed*
//!   conversation is not an error here (callers own that check); a non-empty
//!   one must not project into something every renderer would drop — so a
//!   tool-only archive under `--include-tools=false` errors explicitly
//!   rather than producing a silent empty export.

use crate::model::types::{Message, MessageRole};
use crate::ui::data::ConversationView;
use serde_json::{Map, Value};

/// Agent-neutral projection of one indexed conversation. The metadata fields
/// are consumed by renderers, not by `to_raw_messages`.
#[derive(Debug, Clone)]
pub(crate) struct ExportConversation {
    pub agent_slug: String,
    pub title: Option<String>,
    pub started_at: Option<i64>,
    pub ended_at: Option<i64>,
    pub messages: Vec<ExportMessage>,
    pub stats: ProjectionStats,
}

/// One indexed message after role normalization. Tool payloads come only
/// from index-retained data (e.g. `extra_json` source envelopes); when the
/// index holds none, `tool` stays `None` — the absence is exposed honestly
/// and never fabricated from source files.
#[derive(Debug, Clone)]
pub(crate) struct ExportMessage {
    /// Indexed message order (`idx`).
    pub index: i64,
    /// Normalized role: `user`, `assistant`, `system`, `tool`, or the raw
    /// `Other(..)` string preserved verbatim.
    pub role: String,
    /// Original `MessageRole::Other(..)` payload when `role` is not one of
    /// the four canonical strings; `None` otherwise.
    pub source_role: Option<String>,
    pub content: String,
    pub author: Option<String>,
    pub timestamp: Option<i64>,
    /// Index-retained tool call/result detail, when archived and requested.
    /// Object subset of the source envelope; see module docs for shapes.
    pub tool: Option<serde_json::Value>,
}

/// Completeness counters, so callers can distinguish "not captured during
/// indexing" from "captured but dropped by export". `user`/`assistant`/
/// `system`/`tool` count the indexed [`MessageRole`] variants (not the
/// emitted strings); `unknown_roles` counts `Other(..)` regardless of the
/// verbatim payload.
#[derive(Debug, Clone, Default)]
pub(crate) struct ProjectionStats {
    pub total_indexed: usize,
    pub emitted: usize,
    pub user: usize,
    pub assistant: usize,
    pub system: usize,
    pub tool: usize,
    pub unknown_roles: usize,
    pub tool_payloads_indexed: usize,
    pub tool_payloads_emitted: usize,
}

/// Inclusion options applied at projection time.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ExportOptions {
    pub include_tools: bool,
}

/// Projection failures.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ExportProjectionError {
    /// The indexed conversation has messages but none are renderable —
    /// a non-empty indexed dialogue must never become an empty export.
    #[error("no renderable messages in indexed conversation ({indexed} indexed messages)")]
    NoRenderableMessages { indexed: usize },
}

/// Content block `type` values carrying tool calls/results inside an
/// envelope `content` array — the shapes `format_as_markdown`,
/// `format_as_text`, `format_as_html` and `extract_tool_calls` read.
const TOOL_CONTENT_BLOCK_TYPES: &[&str] = &["tool_use", "tool_result", "toolCall"];

/// `payload.type` values on Codex-style `{"type":"response_item","payload":{..}}`
/// envelopes that carry tool calls/results.
const TOOL_PAYLOAD_TYPES: &[&str] = &[
    "function_call",
    "function_call_output",
    "custom_tool_call",
    "local_shell_call",
    "tool_call",
    "tool_result",
    "toolCall",
    "toolResult",
];

/// Flat event keys that mark tool data when no nested envelope exists
/// (Claude `{"source":"tool_result","tool_use_id":..}` extras, Cursor
/// `{"type":"tool","tool_name":..}` records, codebuff `toolCallId` rows, ...).
const FLAT_TOOL_KEYS: &[&str] = &[
    "tool_use_id",
    "tool_call_id",
    "call_id",
    "toolCallId",
    "callId",
    "toolName",
    "tool_name",
    "tool_input",
    "tool_output",
    "isError",
    "is_error",
];

/// Emitted role strings the renderers will actually render
/// (`format_as_*`/`extract_role`-driven allowlist). `toolResult`/`tool_result`
/// appear verbatim when an archived `Other` role carried the pi-family name —
/// those messages still render, so a toolResult-only archive is renderable.
const RENDERABLE_ROLES: &[&str] = &[
    "user",
    "assistant",
    "system",
    "tool",
    "toolResult",
    "tool_result",
    "unknown",
];

/// Top-level keys owned by the legacy raw-message shape
/// (`{role, content, timestamp?, author?}`); merged tool fields must never
/// clobber them.
const LEGACY_RAW_KEYS: &[&str] = &["role", "content", "timestamp", "author"];

/// Map an indexed role to the export role string + optional verbatim source.
fn export_role_and_source(role: &MessageRole) -> (String, Option<String>) {
    match role {
        MessageRole::User => ("user".to_string(), None),
        MessageRole::Agent => ("assistant".to_string(), None),
        MessageRole::Tool => ("tool".to_string(), None),
        MessageRole::System => ("system".to_string(), None),
        MessageRole::Other(raw) => (raw.clone(), Some(raw.clone())),
    }
}

/// Keep only tool call/result blocks from a `content` array.
fn tool_blocks_of(content: &Value) -> Option<Vec<Value>> {
    let blocks = content.as_array()?;
    let kept: Vec<Value> = blocks
        .iter()
        .filter(|block| {
            block
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|ty| TOOL_CONTENT_BLOCK_TYPES.contains(&ty))
        })
        .cloned()
        .collect();
    (!kept.is_empty()).then_some(kept)
}

fn subset_with_event_type(event: &Value, mut subset: Map<String, Value>) -> Value {
    if let Some(ty) = event.get("type") {
        subset.insert("type".to_string(), ty.clone());
    }
    Value::Object(subset)
}

/// Extract the tool-relevant subset of a decoded source event, or `None`
/// when the event carries no tool data. See module docs for the shapes.
fn tool_subset_of_event(event: &Value) -> Option<Value> {
    // 1. Envelope with a nested `message` object (pi-agent/OMP, Claude Code,
    //    Cursor): `{"type":"message"|"assistant"|"tool","message":{..}}`.
    if let Some(inner) = event.get("message").and_then(Value::as_object) {
        if inner.get("role").and_then(Value::as_str) == Some("toolResult") {
            // pi-family tool result: the whole inner message is tool data —
            // `toolCallId`/`toolName`/`content`/`isError` feed
            // `extract_tool_calls` (result name, output, status, correlation).
            let mut subset = Map::new();
            subset.insert("message".to_string(), Value::Object(inner.clone()));
            return Some(subset_with_event_type(event, subset));
        }
        let mut inner_subset = Map::new();
        if let Some(role) = inner.get("role") {
            inner_subset.insert("role".to_string(), role.clone());
        }
        let mut has_tool = false;
        if let Some(blocks) = inner.get("content").and_then(tool_blocks_of) {
            inner_subset.insert("content".to_string(), Value::Array(blocks));
            has_tool = true;
        }
        for key in FLAT_TOOL_KEYS {
            if let Some(value) = inner.get(*key) {
                inner_subset.insert((*key).to_string(), value.clone());
                has_tool = true;
            }
        }
        if has_tool {
            let mut subset = Map::new();
            subset.insert("message".to_string(), Value::Object(inner_subset));
            return Some(subset_with_event_type(event, subset));
        }
        // Inner message carries no tool data; keep checking other subtrees —
        // an envelope should not be both, but checking is cheap and honest.
    }

    // 2. Flat envelope whose top-level `content` array holds tool blocks.
    //    Wrapped under `message.content` — the key the renderers actually
    //    read — instead of top-level `content`, which the merge would (and
    //    must) refuse to overwrite.
    if let Some(blocks) = event.get("content").and_then(tool_blocks_of) {
        let mut subset = Map::new();
        subset.insert(
            "message".to_string(),
            Value::Object(Map::from_iter([(
                "content".to_string(),
                Value::Array(blocks),
            )])),
        );
        return Some(subset_with_event_type(event, subset));
    }

    // 3. Codex/Responses-API `payload` tool items
    //    (`{"type":"response_item","payload":{"type":"function_call",..}}`).
    if let Some(payload) = event.get("payload").filter(|v| v.is_object())
        && payload
            .get("type")
            .and_then(Value::as_str)
            .is_some_and(|ty| TOOL_PAYLOAD_TYPES.contains(&ty))
    {
        let mut subset = Map::new();
        subset.insert("payload".to_string(), payload.clone());
        return Some(subset_with_event_type(event, subset));
    }

    // 4. The event itself is a bare tool block.
    if event
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|ty| TOOL_CONTENT_BLOCK_TYPES.contains(&ty))
    {
        return Some(subset_with_event_type(
            event,
            Map::from_iter([(
                "message".to_string(),
                Value::Object(Map::from_iter([(
                    "content".to_string(),
                    Value::Array(vec![event.clone()]),
                )])),
            )]),
        ));
    }

    // 5a. Flat `toolResult` record — nested under `message` so
    //    `extract_tool_calls` sees `message.role == "toolResult"`.
    if event.get("role").and_then(Value::as_str) == Some("toolResult") {
        let mut subset = Map::new();
        subset.insert("message".to_string(), event.clone());
        return Some(subset_with_event_type(event, subset));
    }

    // 5b. Flat tool records: Cursor `{"type":"tool",..}` records, Claude
    //    `{"source":"tool_result","tool_use_id":..}` extras, codebuff
    //    `toolCallId` rows — any event with tool-correlation keys. Exposed
    //    whole; the merge keeps legacy fields authoritative.
    if event.get("type").and_then(Value::as_str) == Some("tool")
        || event
            .get("source")
            .and_then(Value::as_str)
            .is_some_and(|s| s.starts_with("tool"))
        || FLAT_TOOL_KEYS.iter().any(|key| event.get(*key).is_some())
    {
        return Some(event.clone());
    }

    // 6. CASS-native `cass.invocations` records — Antigravity planner calls.
    //    FAD dual-writes `NormalizedInvocation` records into `extra.cass`
    //    precisely because `NormalizedMessage.invocations` is not persisted:
    //    `{"cass":{"invocations":[{"kind","name","call_id"?,"arguments"?}]}}`
    //    (other `cass` keys — `model`, `token_usage`, ... — are metadata and
    //    never tool data). Each named entry is emitted as a verbatim
    //    `tool_use` content block under `message.content`, where the
    //    renderers already look: `name`/`call_id` are read as-is
    //    (`extract_tool_correlation_id` accepts `call_id`), and `arguments`
    //    gains an `input` alias because `format_as_markdown`/`format_as_text`
    //    only read `input`. The augmentation is additive — no captured field
    //    is renamed or dropped, and entries without a non-empty `name` are
    //    skipped rather than given an invented one.
    if let Some(invocations) = event
        .get("cass")
        .and_then(|cass| cass.get("invocations"))
        .and_then(Value::as_array)
    {
        let blocks: Vec<Value> = invocations
            .iter()
            .filter(|inv| {
                inv.get("name")
                    .and_then(Value::as_str)
                    .is_some_and(|name| !name.trim().is_empty())
            })
            .map(|inv| {
                let mut block = inv.clone();
                if let Some(obj) = block.as_object_mut() {
                    obj.insert("type".to_string(), Value::from("tool_use"));
                    if !obj.contains_key("input")
                        && let Some(arguments) = obj.get("arguments").cloned()
                    {
                        obj.insert("input".to_string(), arguments);
                    }
                }
                block
            })
            .collect();
        if !blocks.is_empty() {
            let mut subset = Map::new();
            subset.insert(
                "message".to_string(),
                Value::Object(Map::from_iter([(
                    "content".to_string(),
                    Value::Array(blocks),
                )])),
            );
            return Some(subset_with_event_type(event, subset));
        }
    }

    None
}

/// Decode the archived `extra_json` envelope and extract its tool subset.
/// `None` means the index retained no tool data for this message — either no
/// envelope at all (`Null`), a non-decodable one, or a non-tool event.
fn indexed_tool_payload(agent_slug: &str, message_idx: i64, extra_json: &Value) -> Option<Value> {
    let line = crate::storage::sqlite::message_extra_to_source_jsonl_line(extra_json)?;
    let event = match serde_json::from_str::<Value>(&line) {
        Ok(event) => event,
        Err(err) => {
            tracing::warn!(
                agent = %agent_slug,
                message_idx = message_idx,
                error = %err,
                "export projection could not decode archived message envelope"
            );
            return None;
        }
    };
    tool_subset_of_event(&event)
}

/// Project `view` into the export model, preserving indexed message order
/// (`idx`) and timestamps.
///
/// Contract (see plan, Task 3 + Invariants 4-6):
/// - `MessageRole::{User,Agent,Tool,System}` map to `user/assistant/tool/system`;
/// - `MessageRole::Other(s)` keeps `s` verbatim and is counted in stats;
/// - `options.include_tools` gates tool payload emission, never re-parses
///   original source files;
/// - an indexed non-empty dialogue never projects to zero emitted messages
///   without an explicit `NoRenderableMessages` error.
///
/// `NoRenderableMessages` trigger (documented decision): `view.messages` is
/// non-empty AND every message is either excluded from the emitted stream
/// (`MessageRole::Tool` under `include_tools=false` — see Invariant 6 below)
/// or carries a role outside the renderer-emitting set
/// (`user`/`assistant`/`system`/`tool`/`unknown` — the allowlist used by
/// `format_as_*` and the consolidated HTML path). In other words, zero
/// messages could produce visible output — e.g. an archive of only
/// `queue-operation`/`summary` bookkeeping events, or a tool-only archive
/// exported with `--include-tools=false`. A message with a renderable role
/// counts even with empty content (renderers emit its header; content
/// emptiness is their concern). An empty *indexed* conversation is `Ok` —
/// the empty-session diagnostic belongs to the callers.
pub(crate) fn project_indexed_conversation(
    view: &ConversationView,
    options: &ExportOptions,
) -> Result<ExportConversation, ExportProjectionError> {
    // Defensive `idx` ordering — do not rely on the storage fetch order.
    // Keying by (idx, input position) makes the sort deterministic and keeps
    // archive order on duplicate `idx` values.
    let mut ordered: Vec<(usize, &Message)> = view.messages.iter().enumerate().collect();
    ordered.sort_by_key(|(position, msg)| (msg.idx, *position));

    let mut stats = ProjectionStats {
        total_indexed: view.messages.len(),
        ..ProjectionStats::default()
    };
    let mut renderable = 0usize;
    let mut messages = Vec::with_capacity(ordered.len());

    for (_, msg) in ordered {
        let (role, source_role) = export_role_and_source(&msg.role);
        match &msg.role {
            MessageRole::User => stats.user += 1,
            MessageRole::Agent => stats.assistant += 1,
            MessageRole::Tool => stats.tool += 1,
            MessageRole::System => stats.system += 1,
            MessageRole::Other(_) => {}
        }

        // Index-retained tool detail: extract unconditionally so stats can
        // distinguish "not captured during indexing" from "captured but
        // suppressed by --include-tools=false"; attach only when requested.
        let indexed_tool = indexed_tool_payload(&view.convo.agent_slug, msg.idx, &msg.extra_json);
        if indexed_tool.is_some() {
            stats.tool_payloads_indexed += 1;
        }

        // Plan Invariant 6: a `Tool`-role message IS tool data (a call or a
        // result), not ordinary conversation text — under
        // `--include-tools=false` it leaves the emitted stream entirely,
        // under `=true` it is emitted with `role: "tool"` and any archived
        // payload. Exclusion is by the indexed `MessageRole::Tool` variant,
        // not the emitted string: `Other("tool")` is an unknown-role payload
        // and stays emitted verbatim (dropping it could hide user data).
        // `stats.tool`/`tool_payloads_indexed` count it either way.
        if matches!(msg.role, MessageRole::Tool) && !options.include_tools {
            continue;
        }

        if RENDERABLE_ROLES.contains(&role.as_str()) {
            renderable += 1;
        }

        let tool = if options.include_tools {
            indexed_tool
        } else {
            None
        };
        if tool.is_some() {
            stats.tool_payloads_emitted += 1;
        }

        let message = ExportMessage {
            index: msg.idx,
            role,
            source_role,
            content: msg.content.clone(),
            author: msg.author.clone(),
            timestamp: msg.created_at,
            tool,
        };
        if let Some(raw) = &message.source_role {
            stats.unknown_roles += 1;
            tracing::warn!(
                agent = %view.convo.agent_slug,
                message_idx = message.index,
                source_role = %raw,
                "export projection kept a non-canonical message role verbatim"
            );
        }
        stats.emitted += 1;
        messages.push(message);
    }

    if stats.total_indexed > 0 && renderable == 0 {
        return Err(ExportProjectionError::NoRenderableMessages {
            indexed: stats.total_indexed,
        });
    }

    tracing::debug!(
        agent = %view.convo.agent_slug,
        total_indexed = stats.total_indexed,
        emitted = stats.emitted,
        user = stats.user,
        assistant = stats.assistant,
        system = stats.system,
        tool = stats.tool,
        unknown_roles = stats.unknown_roles,
        tool_payloads_indexed = stats.tool_payloads_indexed,
        tool_payloads_emitted = stats.tool_payloads_emitted,
        "export projection complete"
    );

    Ok(ExportConversation {
        agent_slug: view.convo.agent_slug.clone(),
        title: view.convo.title.clone(),
        started_at: view.convo.started_at,
        ended_at: view.convo.ended_at,
        messages,
        stats,
    })
}

/// Render the projection into the legacy `Vec<serde_json::Value>` raw-message
/// shape consumed by `format_as_markdown`/`format_as_text`/`format_as_html`
/// and the `--format json` array envelope. Output must stay
/// backwards-compatible with `conversation_view_to_raw_messages`
/// (`{role, content, timestamp?, author?}`), plus tool detail fields when
/// projected.
///
/// Merge contract: when `message.tool` is `Some` and object-shaped, its
/// entries are copied onto the raw message — `message` (envelope subsets the
/// renderers read: `message.content` block arrays, `message.role ==
/// "toolResult"`, cursor `tool_name`/`tool_input`/`tool_output`), `payload`
/// (Codex items), `type`, and flat tool keys (`tool_use_id`, `toolName`,
/// `is_error`, ...). The legacy keys `role`/`content`/`timestamp`/`author`
/// are never overwritten: `content` stays the normalized indexed text and
/// `role` stays the normalized role, so `extract_role`/`extract_text_content`/
/// `extract_message_timestamp` resolve identically to legacy output.
pub(crate) fn to_raw_messages(conversation: &ExportConversation) -> Vec<serde_json::Value> {
    conversation
        .messages
        .iter()
        .map(|message| {
            let mut obj = Map::new();
            obj.insert("role".to_string(), Value::String(message.role.clone()));
            obj.insert(
                "content".to_string(),
                Value::String(message.content.clone()),
            );
            if let Some(timestamp) = message.timestamp {
                obj.insert("timestamp".to_string(), Value::from(timestamp));
            }
            if let Some(author) = &message.author {
                obj.insert("author".to_string(), Value::String(author.clone()));
            }
            if let Some(tool) = &message.tool
                && let Some(tool_obj) = tool.as_object()
            {
                for (key, value) in tool_obj {
                    if LEGACY_RAW_KEYS.contains(&key.as_str()) {
                        continue;
                    }
                    obj.insert(key.clone(), value.clone());
                }
            }
            Value::Object(obj)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::types::{Conversation, Message, MessageRole};
    use serde_json::json;
    use std::path::PathBuf;

    fn msg(idx: i64, role: MessageRole, content: &str) -> Message {
        Message {
            id: None,
            idx,
            role,
            author: None,
            created_at: None,
            content: content.to_string(),
            extra_json: Value::Null,
            snippets: Vec::new(),
        }
    }

    fn view_with(agent_slug: &str, messages: Vec<Message>) -> ConversationView {
        ConversationView {
            convo: Conversation {
                id: Some(7),
                agent_slug: agent_slug.to_string(),
                workspace: None,
                external_id: Some("ext-7".to_string()),
                title: Some("Projected Session".to_string()),
                source_path: PathBuf::from("/opaque/source.jsonl"),
                started_at: Some(1_700_000_000_000),
                ended_at: Some(1_700_000_060_000),
                approx_tokens: None,
                metadata_json: json!({}),
                messages: vec![],
                source_id: "local".to_string(),
                origin_host: None,
            },
            messages,
            workspace: None,
        }
    }

    fn opts(include_tools: bool) -> ExportOptions {
        ExportOptions { include_tools }
    }

    /// pi-agent/OMP assistant envelope: thinking + text + toolCall block.
    fn pi_assistant_envelope() -> Value {
        json!({
            "type": "message",
            "timestamp": "2026-08-24T12:00:02Z",
            "message": {
                "role": "assistant",
                "content": [
                    { "type": "thinking", "thinking": "envelope-thinking" },
                    { "type": "text", "text": "envelope-text" },
                    {
                        "type": "toolCall",
                        "id": "call-1",
                        "name": "inspect_workspace",
                        "arguments": { "file_path": "/ws/src" }
                    }
                ]
            }
        })
    }

    fn pi_tool_result_envelope() -> Value {
        json!({
            "type": "message",
            "timestamp": "2026-08-24T12:00:03Z",
            "message": {
                "role": "toolResult",
                "toolCallId": "call-1",
                "toolName": "inspect_workspace",
                "content": [{ "type": "text", "text": "result-body" }],
                "isError": false
            }
        })
    }

    fn codex_function_call_envelope() -> Value {
        json!({
            "type": "response_item",
            "timestamp": "2026-01-15T10:00:00.000Z",
            "payload": {
                "type": "function_call",
                "name": "shell",
                "arguments": "{\"cmd\": \"ls\"}",
                "call_id": "call-9"
            }
        })
    }

    #[test]
    fn maps_roles_and_counts_stats() {
        let view = view_with(
            "claude_code",
            vec![
                msg(0, MessageRole::User, "u"),
                msg(1, MessageRole::Agent, "a"),
                msg(2, MessageRole::Tool, "t"),
                msg(3, MessageRole::System, "s"),
                msg(4, MessageRole::Other("queue-operation".to_string()), "q"),
            ],
        );
        // include_tools=true keeps the Tool-role message in the emitted
        // stream so the whole role mapping is visible (exclusion under
        // include_tools=false is covered by its own test).
        let projected = project_indexed_conversation(&view, &opts(true)).expect("project");
        let roles: Vec<&str> = projected.messages.iter().map(|m| m.role.as_str()).collect();
        assert_eq!(
            roles,
            ["user", "assistant", "tool", "system", "queue-operation"]
        );
        assert_eq!(
            projected.messages[4].source_role.as_deref(),
            Some("queue-operation")
        );
        for m in &projected.messages[..4] {
            assert!(
                m.source_role.is_none(),
                "canonical role must not set source_role"
            );
        }
        let stats = &projected.stats;
        assert_eq!(stats.total_indexed, 5);
        assert_eq!(stats.emitted, 5);
        assert_eq!(stats.user, 1);
        assert_eq!(stats.assistant, 1);
        assert_eq!(stats.tool, 1);
        assert_eq!(stats.system, 1);
        assert_eq!(stats.unknown_roles, 1);
    }

    #[test]
    fn preserves_idx_order_for_out_of_order_input() {
        let view = view_with(
            "codex",
            vec![
                msg(2, MessageRole::Agent, "third"),
                msg(0, MessageRole::User, "first"),
                msg(1, MessageRole::Agent, "second"),
            ],
        );
        let projected = project_indexed_conversation(&view, &opts(false)).expect("project");
        let order: Vec<i64> = projected.messages.iter().map(|m| m.index).collect();
        assert_eq!(order, [0, 1, 2]);
        let contents: Vec<&str> = projected
            .messages
            .iter()
            .map(|m| m.content.as_str())
            .collect();
        assert_eq!(contents, ["first", "second", "third"]);
    }

    #[test]
    fn keeps_archive_order_for_duplicate_idx() {
        let view = view_with(
            "codex",
            vec![
                msg(0, MessageRole::User, "a"),
                msg(0, MessageRole::User, "b"),
            ],
        );
        let projected = project_indexed_conversation(&view, &opts(false)).expect("project");
        let contents: Vec<&str> = projected
            .messages
            .iter()
            .map(|m| m.content.as_str())
            .collect();
        assert_eq!(contents, ["a", "b"], "duplicate idx keeps archive order");
    }

    #[test]
    fn carries_conversation_metadata() {
        let view = view_with("devin", vec![msg(0, MessageRole::User, "hi")]);
        let projected = project_indexed_conversation(&view, &opts(false)).expect("project");
        assert_eq!(projected.agent_slug, "devin");
        assert_eq!(projected.title.as_deref(), Some("Projected Session"));
        assert_eq!(projected.started_at, Some(1_700_000_000_000));
        assert_eq!(projected.ended_at, Some(1_700_000_060_000));
    }

    #[test]
    fn carries_author_and_timestamp() {
        let mut m = msg(0, MessageRole::User, "hi");
        m.author = Some("human".to_string());
        m.created_at = Some(1_700_000_000_500);
        let view = view_with("codex", vec![m]);
        let projected = project_indexed_conversation(&view, &opts(false)).expect("project");
        assert_eq!(projected.messages[0].author.as_deref(), Some("human"));
        assert_eq!(projected.messages[0].timestamp, Some(1_700_000_000_500));
    }

    #[test]
    fn include_tools_false_suppresses_indexed_tool_payloads() {
        let mut m = msg(
            0,
            MessageRole::Agent,
            "[Tool: inspect_workspace] file_path=/ws/src",
        );
        m.extra_json = pi_assistant_envelope();
        let view = view_with("omp", vec![m, msg(1, MessageRole::User, "q")]);
        let projected = project_indexed_conversation(&view, &opts(false)).expect("project");
        assert!(projected.messages[0].tool.is_none());
        assert_eq!(projected.stats.tool_payloads_indexed, 1);
        assert_eq!(projected.stats.tool_payloads_emitted, 0);
    }

    #[test]
    fn include_tools_true_emits_pi_tool_call_subset() {
        let mut m = msg(
            0,
            MessageRole::Agent,
            "assistant text\n[Tool: inspect_workspace] file_path=/ws/src",
        );
        m.extra_json = pi_assistant_envelope();
        let view = view_with("pi_agent", vec![m]);
        let projected = project_indexed_conversation(&view, &opts(true)).expect("project");
        let tool = projected.messages[0].tool.as_ref().expect("tool payload");
        let blocks = tool["message"]["content"]
            .as_array()
            .expect("content array");
        assert_eq!(blocks.len(), 1, "only tool blocks, no text/thinking");
        assert_eq!(blocks[0]["type"], "toolCall");
        assert_eq!(blocks[0]["name"], "inspect_workspace");
        assert_eq!(tool["message"]["role"], "assistant");
        assert_eq!(tool["type"], "message");
        assert_eq!(projected.stats.tool_payloads_indexed, 1);
        assert_eq!(projected.stats.tool_payloads_emitted, 1);
    }

    #[test]
    fn tool_role_messages_excluded_without_include_tools() {
        let mut t = msg(1, MessageRole::Tool, "result-body");
        t.extra_json = pi_tool_result_envelope();
        let view = view_with("omp", vec![msg(0, MessageRole::User, "q"), t]);
        let projected = project_indexed_conversation(&view, &opts(false)).expect("project");
        // The Tool-role message leaves the emitted stream entirely — it IS
        // tool data, not ordinary conversation text (Invariant 6).
        assert_eq!(projected.messages.len(), 1);
        assert_eq!(projected.messages[0].role, "user");
        assert_eq!(to_raw_messages(&projected).len(), 1);
        // Stats still describe the index: the tool message existed and held
        // a payload; it was suppressed by the option, not lost in indexing.
        assert_eq!(projected.stats.total_indexed, 2);
        assert_eq!(projected.stats.emitted, 1);
        assert_eq!(projected.stats.tool, 1);
        assert_eq!(projected.stats.tool_payloads_indexed, 1);
        assert_eq!(projected.stats.tool_payloads_emitted, 0);
    }

    #[test]
    fn tool_role_messages_emitted_with_include_tools() {
        let mut t = msg(1, MessageRole::Tool, "result-body");
        t.extra_json = pi_tool_result_envelope();
        let view = view_with("omp", vec![msg(0, MessageRole::User, "q"), t]);
        let projected = project_indexed_conversation(&view, &opts(true)).expect("project");
        assert_eq!(projected.messages.len(), 2);
        assert_eq!(projected.messages[1].role, "tool");
        assert_eq!(
            projected.messages[1]
                .tool
                .as_ref()
                .map(|v| &v["message"]["role"]),
            Some(&json!("toolResult"))
        );
        assert_eq!(projected.stats.emitted, 2);
        assert_eq!(projected.stats.tool, 1);
        assert_eq!(projected.stats.tool_payloads_indexed, 1);
        assert_eq!(projected.stats.tool_payloads_emitted, 1);
        let raw = to_raw_messages(&projected);
        assert_eq!(raw.len(), 2);
        assert_eq!(raw[1]["role"], "tool");
    }

    #[test]
    fn tool_only_archive_errors_without_include_tools() {
        // A tool-only archive under --include-tools=false would emit zero
        // messages — NoRenderableMessages surfaces that explicitly instead
        // of a successful empty export (Invariant 5).
        let mut t = msg(0, MessageRole::Tool, "result-body");
        t.extra_json = pi_tool_result_envelope();
        let view = view_with("omp", vec![t]);
        match project_indexed_conversation(&view, &opts(false)) {
            Err(ExportProjectionError::NoRenderableMessages { indexed }) => {
                assert_eq!(indexed, 1);
            }
            other => panic!("expected NoRenderableMessages, got {other:?}"),
        }
    }

    #[test]
    fn tool_only_archive_projects_with_include_tools() {
        let mut t = msg(0, MessageRole::Tool, "result-body");
        t.extra_json = pi_tool_result_envelope();
        let view = view_with("omp", vec![t]);
        let projected = project_indexed_conversation(&view, &opts(true)).expect("project");
        assert_eq!(projected.stats.emitted, 1);
        assert_eq!(projected.messages[0].role, "tool");
        assert!(projected.messages[0].tool.is_some());
    }

    #[test]
    fn tool_result_envelope_projects_whole_inner_message() {
        let mut m = msg(2, MessageRole::Tool, "result-body");
        m.extra_json = pi_tool_result_envelope();
        let view = view_with(
            "omp",
            vec![
                msg(0, MessageRole::User, "q"),
                msg(1, MessageRole::Agent, "a"),
                m,
            ],
        );
        let projected = project_indexed_conversation(&view, &opts(true)).expect("project");
        let tool = projected.messages[2].tool.as_ref().expect("tool payload");
        let inner = &tool["message"];
        assert_eq!(inner["role"], "toolResult");
        assert_eq!(inner["toolCallId"], "call-1");
        assert_eq!(inner["toolName"], "inspect_workspace");
        assert_eq!(inner["isError"], false);
        assert_eq!(projected.messages[2].role, "tool");
    }

    #[test]
    fn non_tool_envelope_yields_no_tool() {
        let mut m = msg(0, MessageRole::User, "hello");
        m.extra_json = json!({
            "type": "message",
            "message": { "role": "user", "content": "hello" }
        });
        let view = view_with("pi_agent", vec![m]);
        let projected = project_indexed_conversation(&view, &opts(true)).expect("project");
        assert!(projected.messages[0].tool.is_none());
        assert_eq!(projected.stats.tool_payloads_indexed, 0);
    }

    #[test]
    fn malformed_envelope_is_none_not_error() {
        let mut m = msg(0, MessageRole::Agent, "assistant");
        m.extra_json = json!({ "__cass_historical_raw_json__": "{not-valid-json" });
        let view = view_with("omp", vec![m]);
        let projected = project_indexed_conversation(&view, &opts(true)).expect("project");
        assert!(projected.messages[0].tool.is_none());
        assert_eq!(projected.stats.tool_payloads_indexed, 0);
    }

    #[test]
    fn codex_payload_envelope_exposed_honestly() {
        let mut m = msg(0, MessageRole::Agent, "[Tool: shell]");
        m.extra_json = codex_function_call_envelope();
        let view = view_with("codex", vec![m]);
        let projected = project_indexed_conversation(&view, &opts(true)).expect("project");
        let tool = projected.messages[0].tool.as_ref().expect("tool payload");
        assert_eq!(tool["payload"]["type"], "function_call");
        assert_eq!(tool["payload"]["name"], "shell");
        assert_eq!(tool["type"], "response_item");
    }

    #[test]
    fn claude_flat_tool_extra_exposed() {
        let mut m = msg(0, MessageRole::Tool, "file contents");
        m.extra_json = json!({
            "source": "tool_result",
            "tool_use_id": "toolu_1",
            "is_error": false
        });
        let view = view_with("claude_code", vec![m]);
        let projected = project_indexed_conversation(&view, &opts(true)).expect("project");
        let tool = projected.messages[0].tool.as_ref().expect("tool payload");
        assert_eq!(tool["source"], "tool_result");
        assert_eq!(tool["tool_use_id"], "toolu_1");
        assert_eq!(tool["is_error"], false);
    }

    #[test]
    fn claude_assistant_envelope_filters_to_tool_blocks() {
        let mut m = msg(0, MessageRole::Agent, "some text");
        m.extra_json = json!({
            "type": "assistant",
            "message": {
                "role": "assistant",
                "content": [
                    { "type": "text", "text": "some text" },
                    { "type": "tool_use", "id": "toolu_9", "name": "Bash",
                      "input": { "command": "ls" } }
                ]
            }
        });
        let view = view_with("claude_code", vec![m]);
        let projected = project_indexed_conversation(&view, &opts(true)).expect("project");
        let tool = projected.messages[0].tool.as_ref().expect("tool payload");
        let blocks = tool["message"]["content"].as_array().expect("blocks");
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0]["type"], "tool_use");
        assert_eq!(blocks[0]["name"], "Bash");
    }

    fn agy_planner_extra() -> Value {
        // The archived Antigravity PLANNER_RESPONSE extra shape (FAD
        // `planner_calls::invocations_extra` under `extra.cass`): invocation
        // records serialize `NormalizedInvocation{kind,name,call_id,arguments}`.
        json!({
            "thinking": "the user wants the file viewed",
            "cass": {
                "invocations": [
                    {
                        "kind": "tool",
                        "name": "view_file",
                        "call_id": "call-9",
                        "arguments": { "AbsolutePath": "/a.rs" }
                    },
                    {
                        "kind": "tool",
                        "name": "run_command",
                        "arguments": { "CommandLine": "cargo test" }
                    }
                ]
            }
        })
    }

    #[test]
    fn cass_invocations_project_as_tool_use_blocks() {
        let mut m = msg(
            0,
            MessageRole::Agent,
            "[Tool: view_file]\n[Tool: run_command]",
        );
        m.extra_json = agy_planner_extra();
        let view = view_with("antigravity", vec![m]);
        let projected = project_indexed_conversation(&view, &opts(true)).expect("project");
        let tool = projected.messages[0].tool.as_ref().expect("tool payload");
        let blocks = tool["message"]["content"].as_array().expect("blocks");
        assert_eq!(blocks.len(), 2);
        // Verbatim fields pass through; `type`/`input` are the additive
        // renderer-facing aliases.
        assert_eq!(blocks[0]["type"], "tool_use");
        assert_eq!(blocks[0]["name"], "view_file");
        assert_eq!(blocks[0]["call_id"], "call-9");
        assert_eq!(blocks[0]["arguments"]["AbsolutePath"], "/a.rs");
        assert_eq!(blocks[0]["input"]["AbsolutePath"], "/a.rs");
        assert_eq!(blocks[1]["name"], "run_command");
        assert_eq!(blocks[1]["input"]["CommandLine"], "cargo test");
        assert_eq!(projected.stats.tool_payloads_indexed, 1);
        assert_eq!(projected.stats.tool_payloads_emitted, 1);
    }

    #[test]
    fn cass_invocations_suppressed_but_counted_without_include_tools() {
        let mut m = msg(0, MessageRole::Agent, "[Tool: view_file]");
        m.extra_json = agy_planner_extra();
        let view = view_with("antigravity", vec![m]);
        let projected = project_indexed_conversation(&view, &opts(false)).expect("project");
        // The assistant message itself is ordinary dialogue and stays
        // emitted; only the captured tool payload is suppressed.
        assert_eq!(projected.stats.emitted, 1);
        assert!(projected.messages[0].tool.is_none());
        assert_eq!(projected.stats.tool_payloads_indexed, 1);
        assert_eq!(projected.stats.tool_payloads_emitted, 0);
        assert_eq!(to_raw_messages(&projected)[0].get("message"), None);
    }

    #[test]
    fn cass_metadata_without_invocations_yields_no_tool() {
        // `{cass:{model, token_usage, ...}}` extras (Codex ≥16 MiB ingest
        // compaction, Claude ≥32 MiB compaction) and empty/nameless
        // invocations arrays carry no projectable tool data.
        let mut meta = msg(0, MessageRole::Agent, "a");
        meta.extra_json = json!({"cass": {"model": "gpt", "token_usage": {"in": 1}}});
        let mut empty = msg(1, MessageRole::Agent, "b");
        empty.extra_json = json!({"cass": {"invocations": []}});
        let mut nameless = msg(2, MessageRole::Agent, "c");
        nameless.extra_json = json!({"cass": {"invocations": [{"kind": "tool"}]}});
        let view = view_with("antigravity", vec![meta, empty, nameless]);
        let projected = project_indexed_conversation(&view, &opts(true)).expect("project");
        assert!(projected.messages.iter().all(|m| m.tool.is_none()));
        assert_eq!(projected.stats.tool_payloads_indexed, 0);
    }

    #[test]
    fn to_raw_messages_drives_antigravity_invocation_markdown() {
        let mut m = msg(0, MessageRole::Agent, "[Tool: view_file]");
        m.extra_json = agy_planner_extra();
        let view = view_with("antigravity", vec![m]);
        let projected = project_indexed_conversation(&view, &opts(true)).expect("project");
        let raw = to_raw_messages(&projected);
        // The markdown renderer's include-tools path reads
        // `message.content` for `tool_use` blocks — the same key the
        // cass-invocations subset writes.
        let md = crate::format_as_markdown(&raw, &None, None, true);
        assert!(md.contains("**Tool: view_file**"), "markdown: {md}");
        assert!(md.contains("/a.rs"), "markdown: {md}");
        // `call_id` is exposed for HTML correlation via
        // `extract_tool_correlation_id`.
        let calls = crate::extract_tool_calls(&raw[0]);
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].name, "view_file");
        assert_eq!(calls[0].correlation_id.as_deref(), Some("call-9"));
        assert!(calls[0].input.contains("/a.rs"));
    }

    #[test]
    fn no_renderable_messages_when_every_role_would_drop() {
        let view = view_with(
            "claude_code",
            vec![
                msg(0, MessageRole::Other("queue-operation".to_string()), "q"),
                msg(1, MessageRole::Other("summary".to_string()), "s"),
            ],
        );
        match project_indexed_conversation(&view, &opts(false)) {
            Err(ExportProjectionError::NoRenderableMessages { indexed }) => {
                assert_eq!(indexed, 2);
            }
            other => panic!("expected NoRenderableMessages, got {other:?}"),
        }
    }

    #[test]
    fn empty_indexed_conversation_is_not_an_error() {
        let view = view_with("codex", vec![]);
        let projected = project_indexed_conversation(&view, &opts(false)).expect("project");
        assert_eq!(projected.stats.total_indexed, 0);
        assert_eq!(projected.stats.emitted, 0);
        assert!(projected.messages.is_empty());
    }

    #[test]
    fn unknown_role_messages_are_emitted_not_filtered() {
        let view = view_with(
            "claude_code",
            vec![
                msg(0, MessageRole::User, "hi"),
                msg(1, MessageRole::Other("queue-operation".to_string()), "q"),
            ],
        );
        let projected = project_indexed_conversation(&view, &opts(false)).expect("project");
        assert_eq!(projected.stats.emitted, 2);
        assert_eq!(projected.messages[1].role, "queue-operation");
        let raw = to_raw_messages(&projected);
        assert_eq!(raw[1]["role"], "queue-operation");
    }

    #[test]
    fn to_raw_messages_matches_legacy_shape() {
        let mut second = msg(1, MessageRole::Agent, "answer");
        second.author = Some("model".to_string());
        second.created_at = Some(1_700_000_000_500);
        let view = view_with("codex", vec![msg(0, MessageRole::User, "q"), second]);
        let projected = project_indexed_conversation(&view, &opts(false)).expect("project");
        let raw = to_raw_messages(&projected);
        assert_eq!(raw.len(), 2);

        let first = raw[0].as_object().expect("object");
        let keys: Vec<&str> = first.keys().map(String::as_str).collect();
        assert_eq!(keys, ["role", "content"]);
        assert_eq!(raw[0]["role"], "user");
        assert_eq!(raw[0]["content"], "q");

        let second_raw = &raw[1];
        assert_eq!(second_raw["role"], "assistant");
        assert_eq!(second_raw["content"], "answer");
        assert_eq!(second_raw["author"], "model");
        assert_eq!(second_raw["timestamp"], 1_700_000_000_500_i64);
    }

    #[test]
    fn to_raw_messages_merges_tool_fields_without_clobbering_legacy() {
        let mut m = msg(0, MessageRole::Agent, "normalized text");
        m.extra_json = pi_assistant_envelope();
        m.created_at = Some(42);
        let view = view_with("pi_agent", vec![m]);
        let projected = project_indexed_conversation(&view, &opts(true)).expect("project");
        let raw = to_raw_messages(&projected);
        assert_eq!(raw.len(), 1);
        // Legacy fields intact and authoritative.
        assert_eq!(raw[0]["role"], "assistant");
        assert_eq!(raw[0]["content"], "normalized text");
        assert_eq!(raw[0]["timestamp"], 42);
        // Tool subset merged at top level: `message.content` array is where
        // extract_tool_calls and format_as_* look for tool blocks.
        let merged = &raw[0]["message"];
        assert_eq!(merged["role"], "assistant");
        let blocks = merged["content"].as_array().expect("merged blocks");
        assert_eq!(blocks[0]["type"], "toolCall");
        assert_eq!(raw[0]["type"], "message");
    }

    #[test]
    fn to_raw_messages_tool_result_feeds_extract_tool_calls() {
        let mut result = msg(1, MessageRole::Tool, "result-body");
        result.extra_json = pi_tool_result_envelope();
        let mut call = msg(
            0,
            MessageRole::Agent,
            "[Tool: inspect_workspace] file_path=/ws/src",
        );
        call.extra_json = pi_assistant_envelope();
        let view = view_with("pi_agent", vec![call, result]);
        let projected = project_indexed_conversation(&view, &opts(true)).expect("project");
        let raw = to_raw_messages(&projected);

        // The merged messages must drive the existing HTML tool pipeline.
        let calls = crate::extract_tool_calls(&raw[0]);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "inspect_workspace");
        assert!(
            calls[0].input.contains("/ws/src"),
            "tool input should carry the archived arguments: {:?}",
            calls[0].input
        );
        assert_eq!(calls[0].correlation_id.as_deref(), Some("call-1"));

        let results = crate::extract_tool_calls(&raw[1]);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "inspect_workspace");
        assert_eq!(results[0].output.as_deref(), Some("result-body"));
        assert_eq!(results[0].correlation_id.as_deref(), Some("call-1"));
    }

    #[test]
    fn to_raw_messages_drives_claude_tool_use_markdown() {
        let mut m = msg(0, MessageRole::Agent, "running ls");
        m.extra_json = json!({
            "type": "assistant",
            "message": {
                "role": "assistant",
                "content": [
                    { "type": "text", "text": "running ls" },
                    { "type": "tool_use", "id": "toolu_9", "name": "Bash",
                      "input": { "command": "ls -la" } }
                ]
            }
        });
        let view = view_with("claude_code", vec![m]);
        let projected = project_indexed_conversation(&view, &opts(true)).expect("project");
        let raw = to_raw_messages(&projected);
        // The markdown renderer's include-tools path reads
        // `message.content`/`content` arrays for `tool_use` blocks.
        let md = crate::format_as_markdown(&raw, &None, None, true);
        assert!(md.contains("**Tool: Bash**"), "markdown: {md}");
        assert!(md.contains("ls -la"), "markdown: {md}");
        // And the role/content extraction paths stay on the normalized data.
        assert_eq!(crate::extract_role(&raw[0]), "assistant");
        assert_eq!(crate::extract_text_content(&raw[0]), "running ls");
    }

    #[test]
    fn to_raw_messages_tool_never_clobbers_legacy_keys() {
        // A hostile/malformed tool object must not be able to overwrite the
        // legacy fields — the merge is additive only.
        let conversation = ExportConversation {
            agent_slug: "test".to_string(),
            title: None,
            started_at: None,
            ended_at: None,
            stats: ProjectionStats::default(),
            messages: vec![ExportMessage {
                index: 0,
                role: "assistant".to_string(),
                source_role: None,
                content: "normalized".to_string(),
                author: Some("author".to_string()),
                timestamp: Some(7),
                tool: Some(json!({
                    "role": "toolResult",
                    "content": ["should not land"],
                    "timestamp": 999,
                    "author": "impostor",
                    "toolName": "real_tool"
                })),
            }],
        };
        let raw = to_raw_messages(&conversation);
        assert_eq!(raw[0]["role"], "assistant");
        assert_eq!(raw[0]["content"], "normalized");
        assert_eq!(raw[0]["timestamp"], 7);
        assert_eq!(raw[0]["author"], "author");
        assert_eq!(raw[0]["toolName"], "real_tool");
    }
}
