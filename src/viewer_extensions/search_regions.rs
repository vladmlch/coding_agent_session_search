//! Task 5: persisted search regions — the ingest-time region inventory the
//! `search_sessions` DSL evaluates through indexed Quill/SQL algebra instead
//! of query-time corpus reconstruction.
//!
//! One [`publish_search_regions`] call inside the coherent ingest
//! generation (`projection::publish_ingest_generation_in_tx`) derives every
//! region the DSL can address — chat, thinking, tools, artifacts, subagents
//! and metadata (title/notes/tags/child descriptors) — from the SAME
//! `ConversationPacket` and `StructuralPublication` the facts/projection
//! lanes publish against, bound to the same `source_revision`.
//!
//! Storage-byte justification for `indexed_text`: the persisted text
//! fragment exists ONLY for regions whose text cannot be read back from
//! the canonical `messages.content` column — derived extra projections
//! (thinking/tools/artifacts) and metadata overlays (title/notes/tags/
//! child descriptors). Chat regions store NULL and hydrate straight from
//! `messages.id`. Without the fragment the Quill region documents and the
//! query-time verification/snippet lane would have to re-decode every
//! referenced message extra — the exact corpus scan this task removes.
//! Fragments are bounded per region (`MAX_REGION_TEXT`) and no complete
//! trajectory is ever persisted.
//!
//! Region documents in Quill: every non-chat region (and every chat region
//! the noise filter excludes from the message projection) owns one
//! `CassDocument` whose `msg_idx` slot is `REGION_DOC_BASE + region_ordinal`
//! — conversation-scoped identities via `cass_document_identity`, never
//! parsed opaquely. `region_docs` rebuilds that doc set from the persisted
//! rows; [`stage_region_docs`] reconciles it on an uncommitted index
//! writer (delete stale ids, upsert the current set) and
//! [`stamp_region_docs_published`] flips `docs_state='published'` ONLY
//! after the index commit seals those documents — stamping before the
//! commit would certify documents a failed commit or crash never
//! shipped, and `converge_pending` would then never revisit the session.
//! A session whose marks row is missing or not `published` is honest
//! *unseen* evidence at query time — never a silently wrong `False`.

use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use frankensearch::quill::cass::CassDocument;
use serde_json::{Map, Value};

use crate::franken_sync::Row;
use crate::franken_sync::compat::{ParamValue, RowExt};
use crate::model::conversation_packet::ConversationPacket;
use crate::search::quill_bridge::QuillCassIndex;

use super::facts::FactsDb;
use super::projection::{StepLink, StructuralPublication, ToolCallLink};
use super::providers::is_compacted_extra;
use super::providers::{gemini_call_result_text, gemini_status_verdict, scan_copilot_parts};

/// Region schema revision; bumps when the derivation shape changes so the
/// `(region_version, source_revision)` idempotence key republishes rows.
/// v2: the antigravity F10 `extra.error` text (distinct from the record
/// content) indexes as an `m{idx}:error` region — tools scope on tool-role
/// steps, chat scope on every other role.
/// v3: compacted claude/codex extras and every devin record gain their
/// `cass.invocations` call arguments as tools regions (for compacted
/// records the invocations merge is the ONLY surviving copy of the
/// arguments; the devin connector normalizes `tool_calls` away), and the
/// claude Desktop sidecar's user-typed `raw.initialMessage` indexes as an
/// `m{idx}:initial_prompt` chat region.
/// v4: v3 plus gemini raw-record derivations — `thoughts[]` index as
/// `m{idx}:thinking` regions and raw `toolCalls[]` (call args + inline
/// result text, recorded status verdicts) index as tool regions, with
/// `cass.invocations` as the post-compaction fallback; the pinned
/// connector keeps tool arguments out of the delivered content — and
/// pi_agent derivations: `toolCall` content blocks index as tool regions
/// paired with their `toolResult` records by toolCallId (the generic
/// role fallback stays suppressed for those messages).
/// v5: v4 plus copilot F14 derivations — the connector archives the raw
/// `response` part array verbatim in `extra.copilot_response_parts`, so
/// `thinking` part text (values + `generatedTitle`) indexes as
/// `m{idx}:thinking` regions, `mcpServersStarting` server ids index as
/// `m{idx}:mcp` tools regions, and `toolInvocationSerialized` groups
/// (arguments + rendered call messages + `resultDetails` strings, paired
/// by `toolCallId`) index as tool regions; the `cass.invocations`
/// fallback stays suppressed when the parts array is present (the facts
/// lane classifies the same shapes — providers/copilot.rs,
/// `scan_copilot_parts`).
/// v6: v5 plus session×scope documents — every marked conversation also
/// publishes one aggregated lexical document per (scope, chunk) packed from
/// its whole region inventory (`session_scope_docs`), minted at
/// `SESSION_DOC_BASE + (scope_ordinal << SESSION_DOC_SCOPE_SHIFT) +
/// chunk_seq`, so single-token strict plans answer membership from posting
/// sets instead of the capped message-lane enumeration (Stage 1 of the
/// 2026-11-10 strict-common-terms report, §6.1/§6.2). The bump forces one
/// republish of every existing marks row — the fast path refuses to trust
/// session docs stamped at any older version.
pub const REGION_VERSION: i64 = 6;

/// Synthetic `msg_idx` space for region documents: `REGION_DOC_BASE +
/// region_ordinal`. Below `i64::MAX` so `decode_hit` accepts them; far above
/// any real message index so a consumer that treats the slot as a message
/// coordinate misses honestly instead of hydrating the wrong message.
pub const REGION_DOC_BASE: u64 = 1 << 40;

/// Synthetic `msg_idx` space for session×scope documents:
/// `SESSION_DOC_BASE + (scope_ordinal << SESSION_DOC_SCOPE_SHIFT) +
/// chunk_seq`. Above every region slot (region ordinals stay far below
/// `1 << 41`), below `i64::MAX`, and never colliding with
/// [`REGION_DOC_BASE`] slots. Far above any real message index for the
/// same honest-miss reason.
pub const SESSION_DOC_BASE: u64 = 1 << 41;

/// Per-scope chunk width of the session-doc slot space (the report's
/// `(scope_ordinal << 20) + chunk_seq`). A scope packing this many chunks
/// is refused loudly at derivation — a single conversation×scope that big
/// is an archive anomaly, not something to silently mis-slot.
pub const SESSION_DOC_SCOPE_SHIFT: u64 = 20;

/// Hard per-scope chunk ceiling: `chunk_seq < 1 << SESSION_DOC_SCOPE_SHIFT`.
pub const SESSION_DOC_MAX_CHUNKS_PER_SCOPE: u64 = 1 << SESSION_DOC_SCOPE_SHIFT;

/// Session×scope chunks pack WHOLE regions (never mid-region) until the
/// next region would push the chunk past this many CHARACTERS — an
/// oversized region takes a chunk of its own. Chars, not bytes, because
/// the count derivation reads SQL `LENGTH()` (characters for TEXT), so the
/// staging packer and the SQL-side count agree exactly; 65,536 chars is
/// ≤ 256 KiB of UTF-8 for every text (max 4 bytes per scalar).
pub const SESSION_DOC_CHUNK_CHAR_CAP: usize = 64 * 1024;

/// Fixed scope→ordinal mapping for session-doc slots. The six canonical
/// scopes get stable ordinals; any unknown scope string shares the 6
/// bucket so the mapping is total and deterministic from the rows alone.
pub fn session_scope_ordinal(scope: &str) -> u64 {
    match scope {
        "title" => 0,
        "chat" => 1,
        "tools" => 2,
        "thinking" => 3,
        "artifacts" => 4,
        "subagents" => 5,
        _ => 6,
    }
}

/// One region's session-doc text contribution in characters INCLUDING the
/// trailing `'\n'` separator that keeps the last token of one region from
/// merging with the first token of the next (`text.chars().count() + 1`).
/// The SQL-side count derivation must add the same `+ 1` per region.
///
/// The SQL twin of the Rust text selection — `indexed_text` when present
/// (fragment wins: noise-chat and every derived/meta row), else the
/// anchored message content for `chat` rows, else the empty string:
pub const SESSION_REGION_TEXT_SQL: &str = "COALESCE(r.indexed_text, CASE WHEN r.text_source = 'chat' \
     THEN COALESCE(m.content, '') ELSE '' END)";

/// Same bound the query-time derivation applied to one region's text.
pub const MAX_REGION_TEXT: usize = 64 * 1024;

/// `text_source` lanes: `chat` hydrates `messages.content` via
/// `message_id`; `derived`/`meta` carry their fragment in `indexed_text`.
pub const TEXT_SOURCE_CHAT: &str = "chat";
pub const TEXT_SOURCE_DERIVED: &str = "derived";
pub const TEXT_SOURCE_META: &str = "meta";

/// `docs_state` values on `viewer_search_region_marks`.
pub const DOCS_PENDING: &str = "pending";
pub const DOCS_PUBLISHED: &str = "published";
pub const DOCS_META_PENDING: &str = "meta_pending";

// ---------------------------------------------------------------------------
// Row model
// ---------------------------------------------------------------------------

/// One `viewer_search_regions` row — the persisted FactBlock: stable key,
/// scope/knownness, canonical coordinates, structural anchors, predicate
/// fields and the hydration pointer.
#[derive(Debug, Clone, Default)]
pub struct RegionRow {
    pub region_ordinal: i64,
    pub region_key: String,
    pub scope: String,
    pub scope_known: bool,
    pub quill_idx: i64,
    pub message_idx: Option<i64>,
    pub message_id: Option<i64>,
    pub step_index: Option<i64>,
    pub turn_index: Option<i64>,
    pub tool_name: Option<String>,
    pub file_path: Option<String>,
    pub exit_code: Option<i64>,
    pub has_error: Option<bool>,
    pub error_type: Option<String>,
    pub text_source: String,
    /// Whether a persisted fragment exists (`indexed_text IS NOT NULL`) —
    /// carried by the structural universe load, which never materializes
    /// the text itself (Stage 1 lazy fragments).
    pub has_fragment: bool,
    /// Persisted bounded fragment for `derived`/`meta` lanes.
    pub indexed_text: Option<String>,
    pub source_revision: i64,
}

impl RegionRow {
    pub fn is_meta(&self) -> bool {
        self.region_key.starts_with("meta:")
    }
}

// ---------------------------------------------------------------------------
// Derivation helpers (ported from the query-time lane, unchanged semantics)
// ---------------------------------------------------------------------------

fn extra_string<'a>(map: &'a Map<String, Value>, keys: &[&str]) -> Option<&'a str> {
    keys.iter()
        .find_map(|key| map.get(*key).and_then(Value::as_str))
        .filter(|text| !text.is_empty())
}

fn tool_records(extra: &Value) -> (Vec<Value>, Vec<Value>) {
    let mut calls = Vec::new();
    let mut results = Vec::new();
    let kind = extra.get("type").and_then(Value::as_str).unwrap_or("");
    if let Some(content) = extra.pointer("/message/content").and_then(Value::as_array) {
        for part in content {
            let part_kind = part.get("type").and_then(Value::as_str).unwrap_or("");
            if matches!(part_kind, "tool_use" | "tool_call") {
                calls.push(part.clone());
            } else if matches!(part_kind, "tool_result") {
                results.push(part.clone());
            }
        }
    }
    match kind {
        "tool_use" | "tool_call" | "function_call" => calls.push(extra.clone()),
        "tool_result" | "tool_call_output" | "function_call_output" => results.push(extra.clone()),
        _ => {}
    }
    if let Some(item) = extra.get("payload") {
        match item.get("type").and_then(Value::as_str).unwrap_or("") {
            "function_call" | "tool_use" | "tool_call" => calls.push(item.clone()),
            "function_call_output" | "tool_result" | "tool_call_output" => {
                results.push(item.clone())
            }
            _ => {}
        }
    }
    (calls, results)
}

fn call_text(call: &Value) -> String {
    let mut parts = Vec::new();
    if let Some(input) = call.get("input") {
        match input {
            Value::Object(map) => {
                for key in ["command", "file_path", "path", "query", "pattern", "cmd"] {
                    if let Some(text) = map.get(key).and_then(Value::as_str) {
                        parts.push(text.to_owned());
                    }
                }
                if parts.is_empty() {
                    let text = input.to_string();
                    if text.len() <= 8_192 {
                        parts.push(text);
                    }
                }
            }
            Value::String(text) => parts.push(text.clone()),
            _ => {}
        }
    }
    if let Some(args) = call.get("arguments").and_then(Value::as_str) {
        parts.push(args.to_owned());
    }
    parts.join(" ")
}

/// Antigravity planner-invocation arguments as searchable text: named
/// command/path keys first, the canonical JSON otherwise (bounded). Not a
/// `call_text` extension on purpose — its `arguments` arm reads strings
/// only, and widening the shared helper would change other providers'
/// region text. The antigravity connector emits argument OBJECTS.
fn invocation_arguments_text(value: Option<&Value>) -> String {
    let Some(value) = value else {
        return String::new();
    };
    match value {
        Value::Object(map) => {
            let mut parts = Vec::new();
            for key in [
                "command",
                "CommandLine",
                "file_path",
                "TargetFile",
                "path",
                "query",
                "pattern",
                "cmd",
            ] {
                if let Some(text) = map.get(key).and_then(Value::as_str) {
                    parts.push(text.to_owned());
                }
            }
            if parts.is_empty() {
                let text = value.to_string();
                if text.len() <= 8_192 {
                    parts.push(text);
                }
            }
            parts.join(" ")
        }
        Value::String(text) => text.clone(),
        _ => String::new(),
    }
}

fn result_text(result: &Value) -> String {
    for key in ["content", "output", "text", "stdout"] {
        if let Some(value) = result.get(key) {
            match value {
                Value::String(text) => return text.clone(),
                Value::Array(parts) => {
                    return parts
                        .iter()
                        .filter_map(|part| {
                            part.as_str().map(str::to_owned).or_else(|| {
                                part.get("text").and_then(Value::as_str).map(str::to_owned)
                            })
                        })
                        .collect::<Vec<_>>()
                        .join("\n");
                }
                _ => {}
            }
        }
    }
    String::new()
}

fn numeric_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| {
        value
            .as_f64()
            .filter(|v| v.fract() == 0.0)
            .map(|v| v as i64)
    })
}

fn thinking_text(extra: &Value) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(content) = extra.pointer("/message/content").and_then(Value::as_array) {
        for part in content {
            if matches!(
                part.get("type").and_then(Value::as_str),
                Some("thinking" | "reasoning")
            ) && let Some(text) = part
                .get("thinking")
                .or_else(|| part.get("text"))
                .and_then(Value::as_str)
            {
                parts.push(text.to_owned());
            }
        }
    }
    for key in ["thinking", "reasoning", "reasoning_content"] {
        if let Some(text) = extra.get(key).and_then(Value::as_str) {
            parts.push(text.to_owned());
        }
    }
    if matches!(
        extra.get("type").and_then(Value::as_str),
        Some("thinking" | "reasoning" | "agent_reasoning")
    ) {
        for key in ["text", "content", "summary"] {
            if let Some(text) = extra.get(key).and_then(Value::as_str) {
                parts.push(text.to_owned());
            }
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\n"))
    }
}

fn attachment_paths(extra: &Value) -> Vec<String> {
    let mut paths = Vec::new();
    let kind = extra.get("type").and_then(Value::as_str).unwrap_or("");
    if matches!(kind, "attachment" | "artifact" | "file")
        && let Some(path) = extra_string(
            extra.as_object().expect("checked"),
            &["file_path", "path", "name"],
        )
    {
        paths.push(path.to_owned());
    }
    if let Some(list) = extra.get("attachments").and_then(Value::as_array) {
        for element in list {
            if let Some(path) = element.as_str().filter(|p| !p.is_empty()) {
                paths.push(path.to_owned());
            } else if let Some(object) = element.as_object()
                && let Some(path) = extra_string(object, &["file_path", "path", "name"])
            {
                paths.push(path.to_owned());
            }
        }
    }
    paths
}

fn bounded(text: &str) -> String {
    // Byte budget with a UTF-8-safe edge: the bound may split a multibyte
    // scalar — back off to the previous char boundary instead of panicking
    // on the raw byte slice.
    let mut end = text.len().min(MAX_REGION_TEXT);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_owned()
}

// ---------------------------------------------------------------------------
// Region derivation (one deterministic pass; ordinals are derivation order)
// ---------------------------------------------------------------------------

/// O(1) structural anchor lookups for one derivation pass.
///
/// `derive_message_regions` resolves a `(step_index, turn_index)` anchor
/// from the `StructuralPublication` step rows (`scope IN
/// ('text','thinking','tool_call','tool_result')` carry `block_index`
/// parts; the first part row of a message anchors the region) for every
/// message and every extracted call/result. Looking each up by scanning
/// `structure.steps`/`structure.tool_calls` made a large session's
/// publication O(messages·steps + calls·tool_calls) inside the
/// generation transaction; the maps are built once per derivation and
/// keep the original first-match semantics for duplicate `source_idx` /
/// `call_id` rows (`HashMap::entry().or_insert` mirrors `.find()`).
struct AnchorMaps<'a> {
    steps_by_source_idx: HashMap<i64, &'a StepLink>,
    calls_by_call_id: HashMap<&'a str, &'a ToolCallLink>,
}

impl<'a> AnchorMaps<'a> {
    fn build(structure: &'a StructuralPublication) -> Self {
        let mut steps_by_source_idx: HashMap<i64, &'a StepLink> =
            HashMap::with_capacity(structure.steps.len());
        for link in &structure.steps {
            steps_by_source_idx.entry(link.source_idx).or_insert(link);
        }
        let mut calls_by_call_id: HashMap<&'a str, &'a ToolCallLink> =
            HashMap::with_capacity(structure.tool_calls.len());
        for link in &structure.tool_calls {
            calls_by_call_id
                .entry(link.call_id.as_str())
                .or_insert(link);
        }
        Self {
            steps_by_source_idx,
            calls_by_call_id,
        }
    }

    fn anchors_for(&self, source_idx: i64) -> (Option<i64>, Option<i64>) {
        let step = self.steps_by_source_idx.get(&source_idx);
        (
            step.map(|link| link.step_index),
            step.and_then(|link| link.turn_index),
        )
    }

    fn tool_anchor(&self, call_id: &str, fallback_idx: i64) -> (Option<i64>, Option<i64>) {
        if let Some(link) = self.calls_by_call_id.get(call_id) {
            (link.step_index, link.turn_index)
        } else {
            self.anchors_for(fallback_idx)
        }
    }
}

struct PendingCall {
    name: String,
    call_id: String,
    text: String,
    message_idx: i64,
    message_id: Option<i64>,
    file_path: Option<String>,
}

struct PendingResult {
    call_id: Option<String>,
    text: String,
    exit_code: Option<i64>,
    is_error: Option<bool>,
    message_idx: i64,
    message_id: Option<i64>,
}

/// Derive the message-bound region set for one session: chat, thinking,
/// artifacts and correlated tool/result blocks — byte-for-byte the shapes
/// the query-time lane produced, keyed identically so deep links stay
/// stable. Metadata regions are appended by [`derive_meta_regions`].
pub fn derive_message_regions(
    conversation_id: i64,
    packet: &ConversationPacket,
    structure: &StructuralPublication,
) -> Vec<RegionRow> {
    let _ = conversation_id;
    let source_revision = structure.source_revision;
    // Antigravity-scoped derivations below: the pinned FAD connector lifts
    // planner invocations into `extra.cass.invocations` (FAD F3) and
    // USER_INPUT wrapper blocks into `extra.settings_change` /
    // `additional_metadata`. Claude/codex put call arguments there only
    // AFTER extra compaction (non-compacted records keep the raw content
    // blocks AND the merged invocations copy — reading those
    // unconditionally would double-index them), so that lane is gated on
    // the compaction envelope; devin's connector normalizes `tool_calls`
    // away for every record, so its lane is unconditional like
    // antigravity's. The USER_INPUT and sidecar arms stay slug-gated.
    let antigravity = packet.payload.identity.agent_slug == "antigravity";
    let devin = packet.payload.identity.agent_slug == "devin";
    let claude = packet.payload.identity.agent_slug == "claude_code";
    let codex = packet.payload.identity.agent_slug == "codex";
    // Gemini-scoped derivations below: the pinned FAD connector archives
    // the raw gemini record verbatim (FAD gemini connector), so `thoughts[]`
    // and `toolCalls[]` ride in extra — the one text class the connector
    // keeps out of the delivered content is tool arguments. Slug-gated for
    // the same reason as the antigravity arms: no other provider puts raw
    // tool calls in extra under these keys.
    let gemini = packet.payload.identity.agent_slug == "gemini";
    // Pi Agent-scoped derivation below: the connector archives every tree
    // entry verbatim, so `toolCall`/`toolResult` message blocks ride in
    // extra (the generic readers only see `tool_call`/`tool_result` block
    // types; pi's are camelCase). Slug-gated like the antigravity/gemini
    // arms — no other provider puts tool facts under these keys.
    let pi_agent = packet.payload.identity.agent_slug == "pi_agent";
    // Copilot derivations below: the F14 connector archives the raw
    // `response` part array verbatim in `extra.copilot_response_parts`
    // (thinking, tool calls with their results, MCP server starts); on
    // pre-F14 rows only the ingest seam's archived
    // `extra.cass.invocations` survives. Slug-gated like the arms above —
    // no other provider puts these keys in extra.
    let copilot = packet.payload.identity.agent_slug == "copilot";
    let mut regions: Vec<RegionRow> = Vec::new();
    let mut calls: Vec<PendingCall> = Vec::new();
    let mut results: Vec<PendingResult> = Vec::new();
    // One pass over `structure.steps`/`structure.tool_calls` up front —
    // every message/call/result anchor below is then a hash lookup, not a
    // linear scan (previously O(messages·steps + calls·tool_calls)).
    let anchors = AnchorMaps::build(structure);

    for message in &packet.payload.messages {
        let idx = message.idx;
        let message_id = message.message_id;
        let content = &message.content;
        let (step_index, turn_index) = anchors.anchors_for(idx);

        if !content.is_empty() {
            let noise = crate::search::canonicalize::is_hard_message_noise(
                Some(message.role.as_str()),
                content,
            );
            regions.push(RegionRow {
                region_ordinal: regions.len() as i64,
                region_key: format!("m{idx}:chat"),
                scope: "chat".to_owned(),
                scope_known: true,
                // Noise-filtered messages own a synthetic doc slot so their
                // region stays searchable (the message projection skips
                // them); normal chat maps straight onto the message doc.
                quill_idx: if noise {
                    REGION_DOC_BASE as i64 + regions.len() as i64
                } else {
                    idx
                },
                message_idx: Some(idx),
                message_id,
                step_index,
                turn_index,
                text_source: TEXT_SOURCE_CHAT.to_owned(),
                indexed_text: if noise { Some(bounded(content)) } else { None },
                source_revision,
                ..RegionRow::default()
            });
        }

        let extra = &message.extra_json;
        if extra.is_null() {
            continue;
        }

        if let Some(text) = thinking_text(extra) {
            regions.push(RegionRow {
                region_ordinal: regions.len() as i64,
                region_key: format!("m{idx}:thinking"),
                scope: "thinking".to_owned(),
                scope_known: true,
                quill_idx: REGION_DOC_BASE as i64 + regions.len() as i64,
                message_idx: Some(idx),
                message_id,
                step_index,
                turn_index,
                text_source: TEXT_SOURCE_DERIVED.to_owned(),
                indexed_text: Some(bounded(&text)),
                source_revision,
                ..RegionRow::default()
            });
        }
        // Claude Desktop sidecar stubs (FAD
        // `desktop_sidecar_metadata_message`): the connector lifts the
        // metadata lines into the system message content but leaves the
        // user-typed first prompt in `raw.initialMessage` — real
        // conversation text the sidecar carries, so it indexes as its own
        // chat region (the conversation BODY itself is culled at the source
        // per `cass.body_available: false`; nothing else of it exists).
        if claude
            && extra.pointer("/cass/source").and_then(Value::as_str)
                == Some("claude_code_desktop_sidecar")
            && let Some(prompt) = extra
                .pointer("/raw/initialMessage")
                .and_then(Value::as_str)
                .filter(|text| !text.is_empty())
        {
            regions.push(RegionRow {
                region_ordinal: regions.len() as i64,
                region_key: format!("m{idx}:initial_prompt"),
                scope: "chat".to_owned(),
                scope_known: true,
                quill_idx: REGION_DOC_BASE as i64 + regions.len() as i64,
                message_idx: Some(idx),
                message_id,
                step_index,
                turn_index,
                text_source: TEXT_SOURCE_DERIVED.to_owned(),
                indexed_text: Some(bounded(prompt)),
                source_revision,
                ..RegionRow::default()
            });
        }
        // Antigravity USER_INPUT wrapper blocks the connector lifted out of
        // the raw content into extra — at the source they were part of the
        // user turn's text, so they go back into the chat scope here.
        if antigravity && message.role == "user" {
            for (suffix, field) in [
                ("user_settings_change", "settings_change"),
                ("user_additional_metadata", "additional_metadata"),
            ] {
                let Some(text) = extra
                    .get(field)
                    .and_then(Value::as_str)
                    .filter(|text| !text.is_empty())
                else {
                    continue;
                };
                regions.push(RegionRow {
                    region_ordinal: regions.len() as i64,
                    region_key: format!("m{idx}:{suffix}"),
                    scope: "chat".to_owned(),
                    scope_known: true,
                    quill_idx: REGION_DOC_BASE as i64 + regions.len() as i64,
                    message_idx: Some(idx),
                    message_id,
                    step_index,
                    turn_index,
                    text_source: TEXT_SOURCE_DERIVED.to_owned(),
                    indexed_text: Some(bounded(text)),
                    source_revision,
                    ..RegionRow::default()
                });
            }
        }
        // Antigravity F10 `error` passthrough (FAD rev 3851a3a): the
        // source record's failure text rides in `extra.error` while the
        // message content carries the step narration or a summary — the
        // two differ, and until REGION_VERSION 2 the distinct failure text
        // was packet-visible but unindexed (the retired facts mark
        // `agy_error_field_not_indexed` kept that gap honest). Index it
        // where the record's own text indexes: a tool-role MODEL step is
        // an executed step, so its failure text is tool error text and
        // takes the tools scope plus the same has_error/error_type flags a
        // tool_result carrying `is_error` gets; any other role with the
        // field (system ERROR_MESSAGE announcements) is conversation text
        // the connector itself emits as chat content for error-only
        // records, so it stays in the chat scope without error flags.
        // Records whose content IS the error text (error-only
        // ERROR_MESSAGE) are excluded by the equality filter — never
        // double-indexed.
        if antigravity
            && let Some(error) = extra.get("error").and_then(Value::as_str)
            && !error.is_empty()
            && error != content.as_str()
        {
            let tool_step = message.role == "tool";
            regions.push(RegionRow {
                region_ordinal: regions.len() as i64,
                region_key: format!("m{idx}:error"),
                scope: if tool_step { "tools" } else { "chat" }.to_owned(),
                scope_known: true,
                quill_idx: REGION_DOC_BASE as i64 + regions.len() as i64,
                message_idx: Some(idx),
                message_id,
                step_index,
                turn_index,
                has_error: tool_step.then_some(true),
                error_type: tool_step.then_some("tool".to_owned()),
                text_source: TEXT_SOURCE_DERIVED.to_owned(),
                indexed_text: Some(bounded(error)),
                source_revision,
                ..RegionRow::default()
            });
        }
        // Gemini raw-record derivations (REGION_VERSION 3). The pinned
        // connector archives the raw record verbatim in extra and renders
        // its text into the delivered content — with two exceptions:
        //  - `thoughts[]` entries (subject/description) are thinking text
        //    the generic readers cannot see (neither content blocks nor
        //    an extra string), so they index as a `m{idx}:thinking`
        //    region. The `thinking_text(extra).is_none()` guard keeps the
        //    generic arm authoritative and prevents a duplicate region
        //    key if a record ever carried both shapes.
        //  - `toolCalls[]` carry the call arguments the connector keeps
        //    out of content (names and results are inlined, args never
        //    are), plus the inline `result[]` and the recorded lifecycle
        //    status — each indexes as a tool region carrying its args and
        //    result text with the recorded verdict. An absent/null
        //    `result` proves no result ever existed at the source (the
        //    record is archived verbatim) and stays unpaired.
        // Role `tool` never occurs on gemini records (the connector maps
        // model/gemini→assistant, info/error→system, everything else
        // as-is); the guard keeps the generic role fallback authoritative
        // if one ever appears.
        if gemini && message.role != "tool" {
            if thinking_text(extra).is_none() {
                let mut thoughts = Vec::new();
                for thought in extra
                    .get("thoughts")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    for key in ["subject", "description"] {
                        if let Some(text) = thought
                            .get(key)
                            .and_then(Value::as_str)
                            .filter(|text| !text.is_empty())
                        {
                            thoughts.push(text.to_owned());
                        }
                    }
                }
                if !thoughts.is_empty() {
                    let text = thoughts.join("\n");
                    regions.push(RegionRow {
                        region_ordinal: regions.len() as i64,
                        region_key: format!("m{idx}:thinking"),
                        scope: "thinking".to_owned(),
                        scope_known: true,
                        quill_idx: REGION_DOC_BASE as i64 + regions.len() as i64,
                        message_idx: Some(idx),
                        message_id,
                        step_index,
                        turn_index,
                        text_source: TEXT_SOURCE_DERIVED.to_owned(),
                        indexed_text: Some(bounded(&text)),
                        source_revision,
                        ..RegionRow::default()
                    });
                }
            }
            for tool_call in extra
                .get("toolCalls")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if !tool_call.is_object() {
                    continue;
                }
                let call_id = tool_call
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|text| !text.is_empty())
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("anon:{idx}:{}", calls.len()));
                let name = tool_call
                    .get("name")
                    .and_then(Value::as_str)
                    .filter(|text| !text.is_empty())
                    .map(str::to_owned)
                    .unwrap_or_else(|| "unknown".to_owned());
                let arguments = tool_call.get("args");
                let file_path = arguments
                    .and_then(Value::as_object)
                    .and_then(|map| {
                        extra_string(
                            map,
                            &[
                                "file_path",
                                "path",
                                "TargetFile",
                                "file",
                                "filename",
                                "AbsolutePath",
                            ],
                        )
                    })
                    .map(str::to_owned);
                calls.push(PendingCall {
                    name,
                    call_id: call_id.clone(),
                    text: invocation_arguments_text(arguments),
                    message_idx: idx,
                    message_id,
                    file_path,
                });
                if let Some(result) = tool_call.get("result").filter(|value| !value.is_null()) {
                    let (text, explicit_error) = gemini_call_result_text(result);
                    let is_error = if explicit_error {
                        Some(true)
                    } else {
                        gemini_status_verdict(tool_call.get("status").and_then(Value::as_str))
                    };
                    results.push(PendingResult {
                        call_id: Some(call_id),
                        text,
                        exit_code: None,
                        is_error,
                        message_idx: idx,
                        message_id,
                    });
                }
            }
        }
        // Copilot F14 raw-parts derivations (REGION_VERSION 5). The
        // connector archives the serialized `response` array verbatim in
        // `extra.copilot_response_parts` and joins only the markdown
        // values into the delivered content, so the region set mirrors
        // what the facts lane proves (providers/copilot.rs,
        // `scan_copilot_parts` — the SAME classification, so a claim here
        // is never wider than the indexed text):
        //  - `thinking` part text (value strings + `generatedTitle`)
        //    indexes as the message's `m{idx}:thinking` region (guarded
        //    against the generic arm for uniformity);
        //  - `mcpServersStarting` server ids index as an `m{idx}:mcp`
        //    tools region;
        //  - `toolInvocationSerialized` groups (keyed by `toolCallId`,
        //    VS Code's own invocation identity) index as tool regions
        //    carrying the call arguments, the rendered call messages
        //    and the `isConfirmed.reason`, paired with their
        //    `resultDetails` strings; a group without `resultDetails`
        //    stays unpaired (the parts array is the verbatim record —
        //    the result never existed at the source).
        // Role `tool` never occurs on copilot records (the connector
        // emits user/assistant only); the guard keeps the generic role
        // fallback authoritative if one ever appears.
        if copilot
            && message.role != "tool"
            && let Some(parts) = extra
                .get("copilot_response_parts")
                .and_then(Value::as_array)
        {
            let scan = scan_copilot_parts(parts);
            if thinking_text(extra).is_none() {
                let mut texts = Vec::new();
                for part_texts in &scan.thinking_texts {
                    texts.extend(part_texts.iter().cloned());
                }
                if !texts.is_empty() {
                    let text = texts.join("\n");
                    regions.push(RegionRow {
                        region_ordinal: regions.len() as i64,
                        region_key: format!("m{idx}:thinking"),
                        scope: "thinking".to_owned(),
                        scope_known: true,
                        quill_idx: REGION_DOC_BASE as i64 + regions.len() as i64,
                        message_idx: Some(idx),
                        message_id,
                        step_index,
                        turn_index,
                        text_source: TEXT_SOURCE_DERIVED.to_owned(),
                        indexed_text: Some(bounded(&text)),
                        source_revision,
                        ..RegionRow::default()
                    });
                }
            }
            if !scan.mcp_ids.is_empty() {
                let text = scan.mcp_ids.join("\n");
                regions.push(RegionRow {
                    region_ordinal: regions.len() as i64,
                    region_key: format!("m{idx}:mcp"),
                    scope: "tools".to_owned(),
                    scope_known: true,
                    quill_idx: REGION_DOC_BASE as i64 + regions.len() as i64,
                    message_idx: Some(idx),
                    message_id,
                    step_index,
                    turn_index,
                    text_source: TEXT_SOURCE_DERIVED.to_owned(),
                    indexed_text: Some(bounded(&text)),
                    source_revision,
                    ..RegionRow::default()
                });
            }
            for group in &scan.tool_groups {
                // Nameless groups are unrecordable in the facts lane too
                // (`copilot_tool_name_missing`) — their text is marked,
                // never silently indexed here.
                let Some(name) = group
                    .name
                    .as_deref()
                    .filter(|name| !name.is_empty())
                    .map(str::to_owned)
                else {
                    continue;
                };
                let mut text_parts = Vec::new();
                if let Some(arguments) = &group.arguments {
                    let args_text = invocation_arguments_text(Some(arguments));
                    if !args_text.is_empty() {
                        text_parts.push(args_text);
                    }
                }
                text_parts.extend(group.message_texts.iter().cloned());
                let file_path = group
                    .arguments
                    .as_ref()
                    .and_then(Value::as_object)
                    .and_then(|map| {
                        extra_string(
                            map,
                            &[
                                "file_path",
                                "path",
                                "TargetFile",
                                "file",
                                "filename",
                                "AbsolutePath",
                            ],
                        )
                    })
                    .map(str::to_owned);
                calls.push(PendingCall {
                    name,
                    call_id: group.call_id.clone(),
                    text: text_parts.join("\n"),
                    message_idx: idx,
                    message_id,
                    file_path,
                });
                if let Some(result_texts) = &group.result_texts {
                    results.push(PendingResult {
                        call_id: Some(group.call_id.clone()),
                        text: result_texts.join("\n"),
                        exit_code: None,
                        is_error: None,
                        message_idx: idx,
                        message_id,
                    });
                }
            }
        }
        for (ordinal, path) in attachment_paths(extra).into_iter().enumerate() {
            regions.push(RegionRow {
                region_ordinal: regions.len() as i64,
                region_key: format!("m{idx}:artifact:{ordinal}"),
                scope: "artifacts".to_owned(),
                scope_known: true,
                quill_idx: REGION_DOC_BASE as i64 + regions.len() as i64,
                message_idx: Some(idx),
                message_id,
                step_index,
                turn_index,
                file_path: Some(path.clone()),
                text_source: TEXT_SOURCE_DERIVED.to_owned(),
                indexed_text: Some(bounded(&path)),
                source_revision,
                ..RegionRow::default()
            });
        }
        let (call_records, result_records) = tool_records(extra);
        for call in &call_records {
            let name = extra_string(
                call.as_object().unwrap_or(&Map::new()),
                &["name", "tool_name"],
            )
            .map(str::to_owned)
            .unwrap_or_else(|| "unknown".to_owned());
            let call_id = extra_string(
                call.as_object().unwrap_or(&Map::new()),
                &["id", "call_id", "tool_use_id"],
            )
            .map(str::to_owned)
            .unwrap_or_else(|| format!("anon:{idx}:{}", calls.len()));
            let mut text = call_text(call);
            if text.is_empty() {
                text = content.clone();
            }
            let file_path = call
                .get("input")
                .and_then(Value::as_object)
                .and_then(|input| {
                    extra_string(
                        input,
                        &["file_path", "path", "TargetFile", "file", "filename"],
                    )
                })
                .map(str::to_owned);
            calls.push(PendingCall {
                name,
                call_id,
                text,
                message_idx: idx,
                message_id,
                file_path,
            });
        }
        for result in &result_records {
            let call_id = extra_string(
                result.as_object().unwrap_or(&Map::new()),
                &["tool_use_id", "call_id", "id"],
            )
            .map(str::to_owned);
            let exit_code = result
                .get("exit_code")
                .or_else(|| result.get("exitCode"))
                .and_then(numeric_i64);
            let mut is_error = result
                .get("is_error")
                .or_else(|| result.get("isError"))
                .and_then(Value::as_bool);
            if let Some(code) = exit_code {
                is_error = Some(code != 0 || is_error == Some(true));
            }
            let mut text = result_text(result);
            if text.is_empty() {
                text = content.clone();
            }
            results.push(PendingResult {
                call_id,
                text,
                exit_code,
                is_error,
                message_idx: idx,
                message_id,
            });
        }
        // Pi Agent raw-entry derivations: assistant `toolCall` content
        // blocks are the only record of the call arguments (the connector
        // flattens "[Tool: name] k=v…" into content, but the block facts
        // live in extra), and `toolResult` messages are the result records
        // (raw role toolResult, packet role tool) carrying the correlation
        // id and the recorded `isError` verdict. Recording the result as a
        // PendingResult pairs it with its call in the join below, so the
        // role fallback must not ALSO wrap the same content in a
        // name:"unknown" pseudo-call — hence the flag.
        let mut pi_result_recorded = false;
        if pi_agent {
            if let Some(blocks) = extra.pointer("/message/content").and_then(Value::as_array) {
                for block in blocks {
                    if block.get("type").and_then(Value::as_str) != Some("toolCall")
                        || !block.is_object()
                    {
                        continue;
                    }
                    let call_id = block
                        .get("id")
                        .and_then(Value::as_str)
                        .filter(|text| !text.is_empty())
                        .map(str::to_owned)
                        .unwrap_or_else(|| format!("anon:{idx}:{}", calls.len()));
                    let name = block
                        .get("name")
                        .and_then(Value::as_str)
                        .filter(|text| !text.is_empty())
                        .map(str::to_owned)
                        .unwrap_or_else(|| "unknown".to_owned());
                    let arguments = block.get("arguments");
                    let file_path = arguments
                        .and_then(Value::as_object)
                        .and_then(|map| {
                            extra_string(
                                map,
                                &[
                                    "file_path",
                                    "path",
                                    "TargetFile",
                                    "file",
                                    "filename",
                                    "AbsolutePath",
                                ],
                            )
                        })
                        .map(str::to_owned);
                    calls.push(PendingCall {
                        name,
                        call_id,
                        text: invocation_arguments_text(arguments),
                        message_idx: idx,
                        message_id,
                        file_path,
                    });
                }
            }
            if extra.pointer("/message/role").and_then(Value::as_str) == Some("toolResult")
                && !content.is_empty()
            {
                pi_result_recorded = true;
                results.push(PendingResult {
                    call_id: extra
                        .pointer("/message/toolCallId")
                        .and_then(Value::as_str)
                        .filter(|text| !text.is_empty())
                        .map(str::to_owned),
                    text: content.clone(),
                    exit_code: None,
                    is_error: extra.pointer("/message/isError").and_then(Value::as_bool),
                    message_idx: idx,
                    message_id,
                });
            }
        }
        if message.role == "tool"
            && call_records.is_empty()
            && result_records.is_empty()
            && !pi_result_recorded
        {
            calls.push(PendingCall {
                name: "unknown".to_owned(),
                call_id: format!("role:{idx}"),
                text: content.clone(),
                message_idx: idx,
                message_id,
                file_path: None,
            });
        }
        // Planner/agent invocations: `extra.cass.invocations[]` items
        // ({kind:"tool", name, call_id?, arguments}) are the only record of
        // what the planner asked tools to do — the raw `tool_calls` arrays
        // are normalized away by the connector, and the connector records
        // no call ids. The same holds for devin (connector-normalized
        // calls; the rendered content lines carry just the tool name) and
        // for compacted claude/codex extras (the raw tool_use/function_call
        // blocks are gone from the canonical record, so the post-compaction
        // invocations merge is the only surviving copy of the arguments).
        // Non-compacted claude/codex records keep their raw blocks AND the
        // merged invocations copy — the compaction gate keeps those off
        // this lane, so no argument ever indexes twice. Gemini reads the
        // same shape ONLY when `compact_message_extra` (>=32 MiB session
        // files) replaced the raw record — no `toolCalls` array survives,
        // so the ingest seam's archived invocations are the remaining call
        // evidence (a raw record's invocations duplicate its `toolCalls`,
        // already consumed above). Copilot's connector archives tool
        // invocations only through the seam's `cass.invocations` on
        // pre-F14 rows — the F14 `copilot_response_parts` arm above
        // consumed the archived parts already, so this fallback stays
        // suppressed when the parts array is present (reading both would
        // double-record the same call id and taint `conflicting_event`).
        // Mirror the projection's `kind == "tool"` filter. Model-step
        // (role `tool`) records are excluded: their `cass` extras carry
        // name-only synthesized calls with no arguments, and the role
        // fallback above already keeps their content text in the tools
        // scope.
        if (antigravity
            || devin
            || (copilot && extra.get("copilot_response_parts").is_none())
            || ((claude || codex) && is_compacted_extra(extra))
            || (gemini && extra.get("toolCalls").and_then(Value::as_array).is_none()))
            && message.role != "tool"
            && let Some(items) = extra.pointer("/cass/invocations").and_then(Value::as_array)
        {
            for item in items {
                if item.get("kind").and_then(Value::as_str).unwrap_or("tool") != "tool"
                    || !item.is_object()
                {
                    continue;
                }
                let name = item
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_owned();
                let call_id = item
                    .get("call_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("anon:{idx}:{}", calls.len()));
                let arguments = item
                    .get("arguments")
                    .or_else(|| item.get("args"))
                    .or_else(|| item.get("input"));
                let text = invocation_arguments_text(arguments);
                let file_path = arguments
                    .and_then(Value::as_object)
                    .and_then(|map| {
                        extra_string(
                            map,
                            &[
                                "file_path",
                                "path",
                                "TargetFile",
                                "file",
                                "filename",
                                "AbsolutePath",
                            ],
                        )
                    })
                    .map(str::to_owned);
                calls.push(PendingCall {
                    name,
                    call_id,
                    text,
                    message_idx: idx,
                    message_id,
                    file_path,
                });
            }
        }
    }

    let mut results_by_call: HashMap<String, Vec<PendingResult>> = HashMap::new();
    let mut orphan_results: Vec<PendingResult> = Vec::new();
    for result in results {
        match &result.call_id {
            Some(call_id) => results_by_call
                .entry(call_id.clone())
                .or_default()
                .push(result),
            None => orphan_results.push(result),
        }
    }
    for (ordinal, call) in calls.into_iter().enumerate() {
        let linked = results_by_call.remove(&call.call_id).unwrap_or_default();
        let exit_code = linked.iter().find_map(|r| r.exit_code);
        let is_error = linked.iter().find_map(|r| r.is_error);
        let result_text_joined = linked
            .iter()
            .map(|r| r.text.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        let text = [call.text.as_str(), result_text_joined.as_str()]
            .into_iter()
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join("\n");
        let anchor_idx = linked
            .first()
            .map(|r| r.message_idx)
            .unwrap_or(call.message_idx);
        let anchor_id = linked
            .first()
            .map(|r| r.message_id)
            .unwrap_or(call.message_id);
        let (step_index, turn_index) = anchors.tool_anchor(&call.call_id, anchor_idx);
        regions.push(RegionRow {
            region_ordinal: regions.len() as i64,
            region_key: format!("tool:{ordinal}:{}", call.call_id),
            scope: "tools".to_owned(),
            scope_known: true,
            quill_idx: REGION_DOC_BASE as i64 + regions.len() as i64,
            message_idx: Some(anchor_idx),
            message_id: anchor_id,
            step_index,
            turn_index,
            tool_name: Some(call.name),
            file_path: call.file_path,
            exit_code,
            has_error: is_error,
            error_type: is_error.map(|_| "tool".to_owned()),
            text_source: TEXT_SOURCE_DERIVED.to_owned(),
            has_fragment: true,
            indexed_text: Some(bounded(&text)),
            source_revision,
        });
    }
    for (ordinal, result) in orphan_results.into_iter().enumerate() {
        let (step_index, turn_index) = anchors.anchors_for(result.message_idx);
        regions.push(RegionRow {
            region_ordinal: regions.len() as i64,
            region_key: format!("toolresult:{ordinal}:{}", result.message_idx),
            scope: "tools".to_owned(),
            scope_known: true,
            quill_idx: REGION_DOC_BASE as i64 + regions.len() as i64,
            message_idx: Some(result.message_idx),
            message_id: result.message_id,
            step_index,
            turn_index,
            exit_code: result.exit_code,
            has_error: result.is_error,
            error_type: result.is_error.map(|_| "tool".to_owned()),
            text_source: TEXT_SOURCE_DERIVED.to_owned(),
            has_fragment: true,
            indexed_text: Some(bounded(&result.text)),
            source_revision,
            tool_name: None,
            file_path: None,
        });
    }
    regions
}

/// Metadata regions sourced from `viewer_annotations`/`viewer_tags` plus
/// the facts child links — appended LAST so targeted meta replacement never
/// renumbers the message-bound ordinals. Reads the canonical title itself
/// so callers without a packet (annotation edits, child-link fixups) can
/// re-derive identically.
pub fn derive_meta_regions(
    db: &impl FactsDb,
    conversation_id: i64,
    source_revision: i64,
    first_ordinal: i64,
) -> Result<Vec<RegionRow>> {
    let mut regions: Vec<RegionRow> = Vec::new();
    let canonical_title: Option<String> = db
        .facts_query_map(
            "SELECT title FROM conversations WHERE id = ?1",
            &[ParamValue::from(conversation_id)],
            |row| row.get_typed::<Option<String>>(0),
        )?
        .into_iter()
        .next()
        .flatten();
    let annotation = db
        .facts_query_map(
            "SELECT is_pinned, custom_title, notes FROM viewer_annotations \
             WHERE conversation_id = ?1",
            &[ParamValue::from(conversation_id)],
            |row| {
                Ok((
                    row.get_typed::<Option<String>>(1)?,
                    row.get_typed::<Option<String>>(2)?,
                ))
            },
        )?
        .into_iter()
        .next();
    let (custom_title, notes) = annotation.unwrap_or((None, None));
    let effective_title = custom_title.or(canonical_title);
    let mut next = first_ordinal;
    if let Some(title) = effective_title.filter(|t| !t.is_empty()) {
        regions.push(RegionRow {
            region_ordinal: next,
            region_key: "meta:title".to_owned(),
            scope: "title".to_owned(),
            scope_known: true,
            quill_idx: REGION_DOC_BASE as i64 + next,
            text_source: TEXT_SOURCE_META.to_owned(),
            indexed_text: Some(bounded(&title)),
            source_revision,
            ..RegionRow::default()
        });
        next += 1;
    }
    if let Some(notes) = notes.filter(|t| !t.is_empty()) {
        regions.push(RegionRow {
            region_ordinal: next,
            region_key: "meta:notes".to_owned(),
            scope: "chat".to_owned(),
            scope_known: true,
            quill_idx: REGION_DOC_BASE as i64 + next,
            text_source: TEXT_SOURCE_META.to_owned(),
            indexed_text: Some(bounded(&notes)),
            source_revision,
            ..RegionRow::default()
        });
        next += 1;
    }
    let tags = db.facts_query_map(
        "SELECT tag FROM viewer_tags WHERE conversation_id = ?1 ORDER BY tag",
        &[ParamValue::from(conversation_id)],
        |row| row.get_typed::<String>(0),
    )?;
    if !tags.is_empty() {
        regions.push(RegionRow {
            region_ordinal: next,
            region_key: "meta:tags".to_owned(),
            scope: "chat".to_owned(),
            scope_known: true,
            quill_idx: REGION_DOC_BASE as i64 + next,
            text_source: TEXT_SOURCE_META.to_owned(),
            indexed_text: Some(bounded(&tags.join(" "))),
            source_revision,
            ..RegionRow::default()
        });
        next += 1;
    }
    // Subagents scope: resolved child descriptors bound to this parent.
    let children = db.facts_query_map(
        "SELECT c2.id, c2.title, c2.external_id, f2.first_prompt \
         FROM viewer_session_facts f2 \
         JOIN conversations c2 ON c2.id = f2.conversation_id \
         WHERE f2.parent_conversation_id = ?1 ORDER BY c2.id LIMIT 64",
        &[ParamValue::from(conversation_id)],
        |row| {
            Ok((
                row.get_typed::<i64>(0)?,
                row.get_typed::<Option<String>>(1)?,
                row.get_typed::<Option<String>>(2)?,
                row.get_typed::<Option<String>>(3)?,
            ))
        },
    )?;
    for (child_id, title, external, first_prompt) in children {
        let text = [
            title.as_deref(),
            external.as_deref(),
            first_prompt.as_deref(),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join("\n");
        if text.is_empty() {
            continue;
        }
        regions.push(RegionRow {
            region_ordinal: next,
            region_key: format!("meta:child:{child_id}"),
            scope: "subagents".to_owned(),
            scope_known: true,
            quill_idx: REGION_DOC_BASE as i64 + next,
            text_source: TEXT_SOURCE_META.to_owned(),
            indexed_text: Some(bounded(&text)),
            source_revision,
            ..RegionRow::default()
        });
        next += 1;
    }
    Ok(regions)
}

// ---------------------------------------------------------------------------
// Publication
// ---------------------------------------------------------------------------

fn insert_region(db: &impl FactsDb, conversation_id: i64, region: &RegionRow) -> Result<()> {
    db.facts_execute(
        "INSERT INTO viewer_search_regions \
         (conversation_id, region_ordinal, region_key, scope, scope_known, quill_idx, \
          message_idx, message_id, step_index, turn_index, tool_name, file_path, \
          exit_code, has_error, error_type, text_source, indexed_text, source_revision) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18)",
        &[
            ParamValue::from(conversation_id),
            ParamValue::from(region.region_ordinal),
            ParamValue::from(region.region_key.as_str()),
            ParamValue::from(region.scope.as_str()),
            ParamValue::from(region.scope_known as i64),
            ParamValue::from(region.quill_idx),
            ParamValue::from(region.message_idx),
            ParamValue::from(region.message_id),
            ParamValue::from(region.step_index),
            ParamValue::from(region.turn_index),
            ParamValue::from(region.tool_name.as_deref()),
            ParamValue::from(region.file_path.as_deref()),
            ParamValue::from(region.exit_code),
            ParamValue::from(region.has_error.map(|v| v as i64)),
            ParamValue::from(region.error_type.as_deref()),
            ParamValue::from(region.text_source.as_str()),
            ParamValue::from(region.indexed_text.as_deref()),
            ParamValue::from(region.source_revision),
        ],
    )
    .map(|_| ())
    .context("inserting persisted search region")
}

/// The `(source_id, source_path)` that [`region_docs`] mints a session's
/// region documents under: the canonical conversation row, `source_id`
/// defaulting to `local`. `None` when the row is missing or has no path —
/// such a session owns no region documents.
fn region_doc_identity_parts(
    db: &impl FactsDb,
    conversation_id: i64,
) -> Result<Option<(String, String)>> {
    let row = db
        .facts_query_map(
            "SELECT source_id, source_path FROM conversations WHERE id = ?1 LIMIT 1",
            &[ParamValue::from(conversation_id)],
            |row| {
                Ok((
                    row.get_typed::<Option<String>>(0)?,
                    row.get_typed::<Option<String>>(1)?,
                ))
            },
        )?
        .into_iter()
        .next();
    Ok(row.and_then(|(source_id, source_path)| {
        source_path
            .filter(|path| !path.is_empty())
            .map(|path| (source_id.unwrap_or_else(|| "local".to_owned()), path))
    }))
}

fn stale_doc_ids_json(db: &impl FactsDb, conversation_id: i64, keep: &[String]) -> Result<String> {
    // Existing pending deletes ride forward minus ids the new set reclaims.
    let current: Vec<Option<String>> = db.facts_query_map(
        "SELECT pending_doc_deletes FROM viewer_search_region_marks \
         WHERE conversation_id = ?1",
        &[ParamValue::from(conversation_id)],
        |row| row.get_typed::<Option<String>>(0),
    )?;
    let mut pending: Vec<String> = current
        .first()
        .and_then(|raw| raw.as_deref().map(str::to_owned))
        .and_then(|raw| serde_json::from_str::<Vec<String>>(&raw).ok())
        .unwrap_or_default();
    pending.retain(|id| !keep.contains(id));
    Ok(serde_json::to_string(&pending).unwrap_or_else(|_| "[]".to_owned()))
}

/// Publish the session's persisted region inventory inside the coherent
/// ingest generation: same transaction, same `source_revision`, idempotent
/// on `(region_version, source_revision)`.
pub fn publish_search_regions(
    db: &impl FactsDb,
    conversation_id: i64,
    packet: &ConversationPacket,
    structure: &StructuralPublication,
) -> Result<()> {
    let source_revision = structure.source_revision;
    let mut regions = derive_message_regions(conversation_id, packet, structure);
    // `message_id` arrives None whenever the packet replays a pre-commit
    // `Conversation` (ingest's just-inserted rows carry no ids in the
    // struct). Resolve the canonical `(idx → id)` binding in the SAME
    // transaction the packet was committed in — a message-bound region
    // without `message_id` could never hydrate its text.
    if regions
        .iter()
        .any(|r| r.message_idx.is_some() && r.message_id.is_none())
    {
        let ids: HashMap<i64, i64> = db
            .facts_query_map(
                "SELECT idx, id FROM messages WHERE conversation_id = ?1",
                &[ParamValue::from(conversation_id)],
                |row| Ok((row.get_typed::<i64>(0)?, row.get_typed::<i64>(1)?)),
            )?
            .into_iter()
            .collect();
        for region in &mut regions {
            if region.message_id.is_none()
                && let Some(idx) = region.message_idx
            {
                region.message_id = ids.get(&idx).copied();
            }
        }
    }
    let first_meta = regions.len() as i64;
    regions.extend(derive_meta_regions(
        db,
        conversation_id,
        source_revision,
        first_meta,
    )?);

    // Doc-id diff for the index lane: every quill_idx slot the PREVIOUS row
    // set owned that the new set no longer does becomes a pending delete —
    // the Quill publish lane removes exactly those identities, so a shrunk
    // or renumbered inventory never leaves a live stale region document.
    let previous_slots: Vec<i64> = db.facts_query_map(
        "SELECT quill_idx FROM viewer_search_regions WHERE conversation_id = ?1 \
         AND quill_idx >= ?2",
        &[
            ParamValue::from(conversation_id),
            ParamValue::from(REGION_DOC_BASE as i64),
        ],
        |row| row.get_typed::<i64>(0),
    )?;
    // Stage 1: the previous generation's session×scope chunk count per
    // scope is bounded by its region count (region-boundary packing makes
    // at most one chunk per region). The OLD chat chunk count itself is
    // NOT derivable here — a re-ingest already replaced the message rows
    // this transaction reads — so the stale queue conservatively covers
    // every slot under that bound; the post-commit delete ignores
    // identities that never existed, and the keep filter below protects
    // every chunk the new generation owns. Read BEFORE the row DELETE.
    let old_scope_counts: Vec<(String, i64)> = db.facts_query_map(
        "SELECT scope, COUNT(*) FROM viewer_search_regions \
         WHERE conversation_id = ?1 GROUP BY scope",
        &[ParamValue::from(conversation_id)],
        |row| Ok((row.get_typed::<String>(0)?, row.get_typed::<i64>(1)?)),
    )?;
    // Region documents are minted from the canonical conversation row
    // (`region_docs`), so their queued identities must be too. A packet can
    // name the session by another path than the row it merged into; ids
    // built from it never match the live documents, so a reclaimed slot
    // survives the keep filter below and the post-commit delete removes a
    // live document.
    let (doc_source_id, doc_source_path) = region_doc_identity_parts(db, conversation_id)?
        .unwrap_or_else(|| {
            (
                packet.payload.provenance.source_id.clone(),
                packet.payload.identity.source_path.clone(),
            )
        });
    let doc_identity = |slot: i64| {
        frankensearch::quill::cass::cass_document_identity(
            &doc_source_id,
            frankensearch::quill::cass::CassConversationKey {
                source_path: &doc_source_path,
                id: Some(conversation_id),
            },
            slot as u64,
        )
    };
    let keep_ids: Vec<String> = regions
        .iter()
        .filter(|r| r.quill_idx >= REGION_DOC_BASE as i64)
        .map(|r| doc_identity(r.quill_idx))
        .collect();
    let mut pending = stale_doc_ids_json(db, conversation_id, &keep_ids)?;
    {
        let new_slots: std::collections::HashSet<i64> =
            regions.iter().map(|r| r.quill_idx).collect();
        let mut dropped: Vec<String> = previous_slots
            .into_iter()
            .filter(|slot| !new_slots.contains(slot))
            .map(doc_identity)
            .collect();
        let mut list: Vec<String> = serde_json::from_str(&pending).unwrap_or_default();
        list.append(&mut dropped);
        pending = serde_json::to_string(&list).unwrap_or_else(|_| "[]".to_owned());
    }

    db.facts_execute(
        "DELETE FROM viewer_search_regions WHERE conversation_id = ?1",
        &[ParamValue::from(conversation_id)],
    )?;
    for region in &regions {
        insert_region(db, conversation_id, region)?;
    }
    // Stage 1: queue the session×scope stale identities. The keep set is
    // derived from the just-inserted rows through the SAME function the
    // staging lane materializes documents with, so a live keep identity
    // can never be queued. Old slots are conservatively bounded by the
    // pre-delete per-scope region counts (see above); identities that
    // never existed are ignored by the post-commit delete.
    let session_stale =
        session_doc_stale_identities(db, conversation_id, &old_scope_counts, &doc_identity)?;
    if !session_stale.is_empty() {
        let mut list: Vec<String> = serde_json::from_str(&pending).unwrap_or_default();
        list.extend(session_stale);
        pending = serde_json::to_string(&list).unwrap_or_else(|_| "[]".to_owned());
    }
    db.facts_execute(
        "INSERT INTO viewer_search_region_marks \
         (conversation_id, region_version, source_revision, region_count, \
          docs_state, pending_doc_deletes, published_at_ms) \
         VALUES (?1,?2,?3,?4,?5,?6,?7) \
         ON CONFLICT(conversation_id) DO UPDATE SET \
           region_version=excluded.region_version, \
           source_revision=excluded.source_revision, \
           region_count=excluded.region_count, \
           docs_state=excluded.docs_state, \
           pending_doc_deletes=excluded.pending_doc_deletes, \
           published_at_ms=excluded.published_at_ms",
        &[
            ParamValue::from(conversation_id),
            ParamValue::from(REGION_VERSION),
            ParamValue::from(source_revision),
            ParamValue::from(regions.len() as i64),
            ParamValue::from(DOCS_PENDING),
            ParamValue::from(pending.as_str()),
            ParamValue::from(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0),
            ),
        ],
    )?;
    Ok(())
}

/// The conservative session×scope stale-identity queue for one
/// conversation (Stage 1): every session-doc slot under the pre-delete
/// per-scope region-count bound that the CURRENT rows' derivation does not
/// keep. Region-boundary packing makes at most one chunk per region, so
/// the old per-scope region count bounds the previous generation's chunk
/// count; the old chat chunk count itself is not derivable at publication
/// time (a re-ingest already replaced the message rows), which is why the
/// queue is conservative — the post-commit delete ignores identities that
/// never existed, and the keep filter (derived through the same function
/// the staging lane materializes with) protects every live chunk.
fn session_doc_stale_identities(
    db: &impl FactsDb,
    conversation_id: i64,
    old_scope_counts: &[(String, i64)],
    doc_identity: &dyn Fn(i64) -> String,
) -> Result<Vec<String>> {
    let Some(chunks) = session_scope_docs(db, conversation_id)? else {
        return Ok(Vec::new());
    };
    let keep: std::collections::HashSet<String> = chunks
        .iter()
        .map(|chunk| doc_identity(chunk.slot as i64))
        .collect();
    let mut stale = Vec::new();
    for (scope, count) in old_scope_counts {
        let ordinal = session_scope_ordinal(scope);
        for seq in 0..(*count).max(0).min(SESSION_DOC_MAX_CHUNKS_PER_SCOPE as i64) as u64 {
            let slot = SESSION_DOC_BASE + (ordinal << SESSION_DOC_SCOPE_SHIFT) + seq;
            let identity = doc_identity(slot as i64);
            if !keep.contains(&identity) {
                stale.push(identity);
            }
        }
    }
    Ok(stale)
}

/// Targeted metadata-region replacement for annotation/tag edits and
/// child-link fixups: only `meta:*` rows are re-derived, preserving the
/// message-bound ordinals. Marks the session's docs `meta_pending` until
/// the index lane replaces the meta documents. Reads canonical identity
/// itself; a session with no marks row yet simply has no region inventory
/// to patch (its next generation publishes everything).
pub fn replace_meta_regions(db: &impl FactsDb, conversation_id: i64) -> Result<()> {
    let mark = region_mark(db, conversation_id)?;
    let Some((_, source_revision, _, _)) = mark else {
        return Ok(());
    };
    let max_message_ordinal: i64 = db
        .facts_query_map(
            "SELECT COALESCE(MAX(region_ordinal), -1) FROM viewer_search_regions \
             WHERE conversation_id = ?1 AND region_key NOT LIKE 'meta:%'",
            &[ParamValue::from(conversation_id)],
            |row| row.get_typed::<i64>(0),
        )?
        .into_iter()
        .next()
        .unwrap_or(-1);
    let old_meta: Vec<i64> = db.facts_query_map(
        "SELECT quill_idx FROM viewer_search_regions \
         WHERE conversation_id = ?1 AND region_key LIKE 'meta:%'",
        &[ParamValue::from(conversation_id)],
        |row| row.get_typed::<i64>(0),
    )?;
    // Stage 1: the meta replacement rewrites text that rides the
    // session×scope chunks (notes live in the chat scope, children in
    // subagents), so the session-doc stale queue needs the pre-delete
    // per-scope row counts as its conservative old-slot bound — read
    // BEFORE the meta DELETE, exactly like the full republication.
    let old_scope_counts: Vec<(String, i64)> = db.facts_query_map(
        "SELECT scope, COUNT(*) FROM viewer_search_regions \
         WHERE conversation_id = ?1 GROUP BY scope",
        &[ParamValue::from(conversation_id)],
        |row| Ok((row.get_typed::<String>(0)?, row.get_typed::<i64>(1)?)),
    )?;
    let meta = derive_meta_regions(
        db,
        conversation_id,
        source_revision,
        max_message_ordinal + 1,
    )?;
    db.facts_execute(
        "DELETE FROM viewer_search_regions WHERE conversation_id = ?1 \
         AND region_key LIKE 'meta:%'",
        &[ParamValue::from(conversation_id)],
    )?;
    for region in &meta {
        insert_region(db, conversation_id, region)?;
    }
    // Queue stale meta doc identities for the index lane.
    let keep: Vec<i64> = meta.iter().map(|r| r.quill_idx).collect();
    let mut pending: Vec<String> = db
        .facts_query_map(
            "SELECT pending_doc_deletes FROM viewer_search_region_marks \
             WHERE conversation_id = ?1",
            &[ParamValue::from(conversation_id)],
            |row| row.get_typed::<Option<String>>(0),
        )?
        .first()
        .and_then(|raw| raw.as_deref().map(str::to_owned))
        .and_then(|raw| serde_json::from_str::<Vec<String>>(&raw).ok())
        .unwrap_or_default();
    if let Some((source_id, source_path)) = region_doc_identity_parts(db, conversation_id)? {
        let doc_identity = |slot: i64| {
            frankensearch::quill::cass::cass_document_identity(
                &source_id,
                frankensearch::quill::cass::CassConversationKey {
                    source_path: &source_path,
                    id: Some(conversation_id),
                },
                slot as u64,
            )
        };
        // A queued delete for a slot the new meta inventory reclaims would
        // remove a live document after the commit counted it, as
        // `stale_doc_ids_json` already prevents on full republication.
        let keep_ids: Vec<String> = keep.iter().map(|slot| doc_identity(*slot)).collect();
        pending.retain(|id| !keep_ids.contains(id));
        for slot in old_meta.iter().filter(|slot| !keep.contains(slot)) {
            pending.push(doc_identity(*slot));
        }
        // Stage 1: session×scope stale queue — the keep set is derived from
        // the rows the replacement just wrote (the same function the
        // staging lane materializes with), the old slots are bounded by the
        // pre-delete per-scope counts, and identities that never existed
        // are ignored by the post-commit delete.
        let session_stale =
            session_doc_stale_identities(db, conversation_id, &old_scope_counts, &doc_identity)?;
        pending.extend(session_stale);
    }
    db.facts_execute(
        "UPDATE viewer_search_region_marks SET docs_state = ?2, \
         pending_doc_deletes = ?3 WHERE conversation_id = ?1 \
         AND EXISTS (SELECT 1 FROM conversations c WHERE c.id = ?1)",
        &[
            ParamValue::from(conversation_id),
            ParamValue::from(DOCS_META_PENDING),
            ParamValue::from(serde_json::to_string(&pending).unwrap_or_default()),
        ],
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Quill region documents (read-side of the persisted inventory)
// ---------------------------------------------------------------------------

fn row_to_region(row: &Row) -> Result<RegionRow, crate::franken_sync::FrankenError> {
    let indexed_text: Option<String> = row.get_typed(15)?;
    Ok(RegionRow {
        region_ordinal: row.get_typed(0)?,
        region_key: row.get_typed(1)?,
        scope: row.get_typed(2)?,
        scope_known: row.get_typed::<i64>(3)? != 0,
        quill_idx: row.get_typed(4)?,
        message_idx: row.get_typed(5)?,
        message_id: row.get_typed(6)?,
        step_index: row.get_typed(7)?,
        turn_index: row.get_typed(8)?,
        tool_name: row.get_typed(9)?,
        file_path: row.get_typed(10)?,
        exit_code: row.get_typed(11)?,
        has_error: row.get_typed::<Option<i64>>(12)?.map(|v| v != 0),
        error_type: row.get_typed(13)?,
        text_source: row.get_typed(14)?,
        has_fragment: indexed_text.is_some(),
        indexed_text,
        source_revision: row.get_typed(16)?,
    })
}

/// Column-shifted `RegionRow` read for the STRUCTURAL universe load: same
/// column layout as [`region_row_shifted`] except position 16 carries the
/// `indexed_text IS NOT NULL` bit instead of the fragment itself — the
/// universe load never materializes fragment text (Stage 1 lazy fragments)
/// and only records whether a fragment exists to read on demand.
fn region_row_structural(row: &Row) -> Result<RegionRow, crate::franken_sync::FrankenError> {
    Ok(RegionRow {
        region_ordinal: row.get_typed(1)?,
        region_key: row.get_typed(2)?,
        scope: row.get_typed(3)?,
        scope_known: row.get_typed::<i64>(4)? != 0,
        quill_idx: row.get_typed(5)?,
        message_idx: row.get_typed(6)?,
        message_id: row.get_typed(7)?,
        step_index: row.get_typed(8)?,
        turn_index: row.get_typed(9)?,
        tool_name: row.get_typed(10)?,
        file_path: row.get_typed(11)?,
        exit_code: row.get_typed(12)?,
        has_error: row.get_typed::<Option<i64>>(13)?.map(|v| v != 0),
        error_type: row.get_typed(14)?,
        text_source: row.get_typed(15)?,
        has_fragment: row.get_typed::<i64>(16)? != 0,
        indexed_text: None,
        source_revision: row.get_typed(17)?,
    })
}

const REGION_SELECT: &str = "SELECT region_ordinal, region_key, scope, scope_known, quill_idx, \
     message_idx, message_id, step_index, turn_index, tool_name, file_path, \
     exit_code, has_error, error_type, text_source, indexed_text, source_revision \
     FROM viewer_search_regions WHERE conversation_id = ?1 ORDER BY region_ordinal";

/// All persisted region rows for one conversation (ordinal order).
pub fn regions_for_conversation(db: &impl FactsDb, conversation_id: i64) -> Result<Vec<RegionRow>> {
    db.facts_query_map(REGION_SELECT, &[ParamValue::from(conversation_id)], |row| {
        row_to_region(row)
    })
    .context("loading persisted search regions")
}

/// Region rows for a bounded candidate set (one indexed IN-scan).
pub fn regions_for_conversations(
    db: &impl FactsDb,
    conversation_ids: &[i64],
) -> Result<HashMap<i64, Vec<RegionRow>>> {
    let mut out: HashMap<i64, Vec<RegionRow>> = HashMap::new();
    if conversation_ids.is_empty() {
        return Ok(out);
    }
    const CHUNK: usize = 900;
    for chunk in conversation_ids.chunks(CHUNK) {
        let mut sql = String::from(
            "SELECT conversation_id, region_ordinal, region_key, scope, scope_known, \
             quill_idx, message_idx, message_id, step_index, turn_index, tool_name, \
             file_path, exit_code, has_error, error_type, text_source, indexed_text, \
             source_revision FROM viewer_search_regions WHERE conversation_id IN (",
        );
        let mut params: Vec<ParamValue> = Vec::with_capacity(chunk.len());
        for (i, cid) in chunk.iter().enumerate() {
            if i > 0 {
                sql.push(',');
            }
            sql.push_str(&format!("?{}", i + 1));
            params.push(ParamValue::from(*cid));
        }
        sql.push_str(") ORDER BY conversation_id, region_ordinal");
        for (cid, region) in db.facts_query_map(&sql, &params, |row| {
            Ok((row.get_typed::<i64>(0)?, region_row_shifted(row)?))
        })? {
            out.entry(cid).or_default().push(region);
        }
    }
    Ok(out)
}

/// Column-shifted `RegionRow` read for a `conversation_id`-prefixed
/// SELECT (columns 1..=17 of the batch loaders).
fn region_row_shifted(row: &Row) -> Result<RegionRow, crate::franken_sync::FrankenError> {
    let indexed_text: Option<String> = row.get_typed(16)?;
    Ok(RegionRow {
        region_ordinal: row.get_typed(1)?,
        region_key: row.get_typed(2)?,
        scope: row.get_typed(3)?,
        scope_known: row.get_typed::<i64>(4)? != 0,
        quill_idx: row.get_typed(5)?,
        message_idx: row.get_typed(6)?,
        message_id: row.get_typed(7)?,
        step_index: row.get_typed(8)?,
        turn_index: row.get_typed(9)?,
        tool_name: row.get_typed(10)?,
        file_path: row.get_typed(11)?,
        exit_code: row.get_typed(12)?,
        has_error: row.get_typed::<Option<i64>>(13)?.map(|v| v != 0),
        error_type: row.get_typed(14)?,
        text_source: row.get_typed(15)?,
        has_fragment: indexed_text.is_some(),
        indexed_text,
        source_revision: row.get_typed(17)?,
    })
}

/// Region rows for the whole filtered universe in one set-based read,
/// bound by the search lane's `IN (universe subquery)` so archives whose
/// id-list chunking cannot fit the request deadline still load in one
/// statement.
///
/// Stage 1 lazy fragments (§6.3): the load is STRUCTURAL-ONLY —
/// `indexed_text` never rides it. Position 16 carries the
/// `indexed_text IS NOT NULL` bit instead ([`region_row_structural`]), so
/// the universe load stops paying one fragment transfer per region
/// (~6 s on a 762k-region archive); the query lane reads fragments on
/// demand, only for the regions its witnesses and snippets actually need.
///
/// Takes `&Connection`/`&[SqliteValue]` (not `&impl FactsDb`/
/// `&[ParamValue]`) because the caller already holds the ordered universe
/// ids and subquery parameters in that form — no value-type conversion.
pub fn regions_for_universe_subquery(
    connection: &crate::franken_sync::Connection,
    id_subquery: &str,
    params: &[crate::franken_sync::SqliteValue],
) -> Result<HashMap<i64, Vec<RegionRow>>> {
    let sql = format!(
        "SELECT conversation_id, region_ordinal, region_key, scope, scope_known, \
         quill_idx, message_idx, message_id, step_index, turn_index, tool_name, \
         file_path, exit_code, has_error, error_type, text_source, \
         indexed_text IS NOT NULL, source_revision FROM viewer_search_regions \
         WHERE conversation_id IN ({id_subquery}) ORDER BY conversation_id, region_ordinal"
    );
    let rows = connection
        .query_with_params(&sql, params)
        .context("loading universe search regions")?;
    let mut out: HashMap<i64, Vec<RegionRow>> = HashMap::new();
    for row in &rows {
        let cid = row.get_typed::<i64>(0)?;
        let region = region_row_structural(row)?;
        out.entry(cid).or_default().push(region);
    }
    Ok(out)
}

/// Whether this archive carries the extension profile's marks table.
/// Canonical-only stores (minimal test schemas, archives opened before
/// the viewer profile ever installed) legitimately lack it; every
/// marks-listing helper treats that as an empty inventory rather than a
/// query error, matching [`live_region_doc_bound`]'s documented `0`.
fn has_region_marks_table(db: &impl FactsDb) -> Result<bool> {
    Ok(!db
        .facts_query_map(
            "SELECT 1 FROM sqlite_master WHERE type='table' \
             AND name='viewer_search_region_marks' LIMIT 1",
            &[],
            |row| row.get_typed::<i64>(0),
        )?
        .is_empty())
}

/// Upper bound on live NON-canonical documents across the archive: the
/// region documents plus the Stage 1 session×scope documents.
///
/// Region share: only persisted rows that actually own a document count —
/// the synthetic-slot rows (`quill_idx >= REGION_DOC_BASE`) carrying a
/// bounded `indexed_text` fragment, i.e. exactly the rows [`region_docs`]
/// materializes. Chat regions (canonical `quill_idx`, `indexed_text`
/// NULL) map onto the message document and own none, so counting
/// `marks.region_count` here over-bounded the region share and
/// under-counted the observed canonical-message share, breaking watch
/// checkpoint refreshes and the early-exit equality.
///
/// Session×scope share (Stage 1): every (conversation, scope) chunk the
/// current rows pack into under [`pack_region_boundaries`] — the same
/// greedy packer the staging lane materializes documents with, fed the
/// same per-region char contributions (`LENGTH(text) + 1`), so the bound
/// is exact at convergence rather than merely conservative. This streams
/// one row per region and evaluates `LENGTH()` over every chat region's
/// message content — the one deliberate cost of keeping the bound SQL-free
/// of a persisted length column (no DDL is allowed for Stage 1); it runs
/// on indexer-side certification paths only, never on the query lane.
///
/// `pending_doc_deletes` bounds stale identities still awaiting removal
/// (region drops and the conservative session-doc queue alike).
/// `0` on archives without the extension profile (no marks table). The
/// lexical publication receipt uses this to split the total indexed-doc
/// count back into its canonical-message share so the
/// `indexed_docs <= total_messages` certification stays exact.
pub fn live_region_doc_bound(db: &impl FactsDb) -> Result<u64> {
    if !has_region_marks_table(db)? {
        return Ok(0);
    }
    // Set-based form of the per-marks-row count. A correlated scalar
    // `COUNT(*)` subquery is evaluated by frankensqlite's in-memory
    // correlated fallback, which ignores the `(conversation_id, quill_idx)`
    // index and never finished on a 6k-session / 762k-region archive. The
    // JOIN + GROUP BY below yields exactly one row per marks row whose
    // conversation exists (`marks.conversation_id` is the primary key, and
    // `conversations.id` is INTEGER PRIMARY KEY, so the join cannot
    // duplicate it), and the LEFT JOIN keeps marks with no live regions at
    // a count of zero.
    let mut bound = 0_u64;
    for (deletes_json, doc_rows) in db.facts_query_map(
        "SELECT m.pending_doc_deletes, COUNT(r.conversation_id) \
         FROM viewer_search_region_marks m \
         JOIN conversations c ON c.id = m.conversation_id \
         LEFT JOIN viewer_search_regions r \
           ON r.conversation_id = m.conversation_id \
          AND r.quill_idx >= ?1 AND r.indexed_text IS NOT NULL \
         GROUP BY m.conversation_id, m.pending_doc_deletes",
        &[ParamValue::from(REGION_DOC_BASE as i64)],
        |row| Ok((row.get_typed::<String>(0)?, row.get_typed::<i64>(1)?)),
    )? {
        bound = bound.saturating_add(doc_rows.max(0) as u64);
        let deletes: Vec<Value> = serde_json::from_str(&deletes_json).unwrap_or_default();
        bound = bound.saturating_add(deletes.len() as u64);
    }
    // Session×scope share: stream every marked conversation's per-region
    // char contributions (region-ordinal order within each scope) and
    // pack them with the SAME greedy chunker the staging lane uses. The
    // INNER JOIN drops marks rows with no regions (they pack zero
    // chunks); the ORDER BY keeps each (conversation, scope) group
    // contiguous so the fold below never buffers more than one scope.
    let mut session_docs = 0_u64;
    let mut lengths: Vec<u64> = Vec::new();
    let mut current: Option<(i64, String)> = None;
    let flush = |key: &Option<(i64, String)>, lengths: &mut Vec<u64>, session_docs: &mut u64| {
        if key.is_some() {
            *session_docs = session_docs.saturating_add(
                pack_region_boundaries(lengths, SESSION_DOC_CHUNK_CHAR_CAP as u64).len() as u64,
            );
        }
        lengths.clear();
    };
    for (cid, scope, chars) in db.facts_query_map(
        "SELECT r.conversation_id, r.scope, \
                LENGTH(COALESCE(r.indexed_text, CASE WHEN r.text_source = 'chat' \
                 THEN COALESCE(m.content, '') ELSE '' END)) + 1 \
         FROM viewer_search_region_marks mk \
         JOIN conversations c ON c.id = mk.conversation_id \
         JOIN viewer_search_regions r ON r.conversation_id = mk.conversation_id \
         LEFT JOIN messages m ON m.id = r.message_id \
         ORDER BY r.conversation_id, r.scope, r.region_ordinal",
        &[],
        |row| {
            Ok((
                row.get_typed::<i64>(0)?,
                row.get_typed::<String>(1)?,
                row.get_typed::<i64>(2)?,
            ))
        },
    )? {
        let key = (cid, scope);
        if current.as_ref() != Some(&key) {
            flush(&current, &mut lengths, &mut session_docs);
            current = Some(key);
        }
        lengths.push(chars.max(0) as u64);
    }
    flush(&current, &mut lengths, &mut session_docs);
    Ok(bound.saturating_add(session_docs))
}

/// `(region_version, source_revision, region_count, docs_state)` for a
/// conversation's marks row; `None` when no publication has committed.
pub fn region_mark(
    db: &impl FactsDb,
    conversation_id: i64,
) -> Result<Option<(i64, i64, i64, String)>> {
    Ok(db
        .facts_query_map(
            "SELECT region_version, source_revision, region_count, docs_state \
             FROM viewer_search_region_marks WHERE conversation_id = ?1",
            &[ParamValue::from(conversation_id)],
            |row| {
                Ok((
                    row.get_typed::<i64>(0)?,
                    row.get_typed::<i64>(1)?,
                    row.get_typed::<i64>(2)?,
                    row.get_typed::<String>(3)?,
                ))
            },
        )?
        .into_iter()
        .next())
}

/// Marks rows for a bounded candidate set.
pub fn region_marks_for(
    db: &impl FactsDb,
    conversation_ids: &[i64],
) -> Result<HashMap<i64, (i64, i64, i64, String)>> {
    let mut out = HashMap::new();
    if conversation_ids.is_empty() {
        return Ok(out);
    }
    const CHUNK: usize = 900;
    for chunk in conversation_ids.chunks(CHUNK) {
        let mut sql = String::from(
            "SELECT conversation_id, region_version, source_revision, region_count, \
             docs_state FROM viewer_search_region_marks WHERE conversation_id IN (",
        );
        let mut params: Vec<ParamValue> = Vec::with_capacity(chunk.len());
        for (i, cid) in chunk.iter().enumerate() {
            if i > 0 {
                sql.push(',');
            }
            sql.push_str(&format!("?{}", i + 1));
            params.push(ParamValue::from(*cid));
        }
        sql.push(')');
        for row in db.facts_query_map(&sql, &params, |row| {
            Ok((
                row.get_typed::<i64>(0)?,
                (
                    row.get_typed::<i64>(1)?,
                    row.get_typed::<i64>(2)?,
                    row.get_typed::<i64>(3)?,
                    row.get_typed::<String>(4)?,
                ),
            ))
        })? {
            out.insert(row.0, row.1);
        }
    }
    Ok(out)
}

/// `docs_state = 'pending'` sessions — the index lane's dirty set after
/// ingest publication or meta replacement ran without a doc commit yet.
/// Empty on archives without the extension profile (no marks table).
pub fn pending_region_doc_sessions(db: &impl FactsDb, limit: i64) -> Result<Vec<i64>> {
    if !has_region_marks_table(db)? {
        return Ok(Vec::new());
    }
    db.facts_query_map(
        "SELECT conversation_id FROM viewer_search_region_marks m \
         WHERE m.docs_state != ?1 \
         AND EXISTS (SELECT 1 FROM conversations c WHERE c.id = m.conversation_id) \
         ORDER BY m.conversation_id LIMIT ?2",
        &[ParamValue::from(DOCS_PUBLISHED), ParamValue::from(limit)],
        |row| row.get_typed::<i64>(0),
    )
    .context("listing sessions with pending region documents")
}

/// EVERY session carrying a marks row — the authoritative rebuild lane's
/// converge set. A from-scratch index build owns no region documents at
/// all, so `published` marks must be re-materialized too; `pending`/
/// `meta_pending` states reconcile through the same pass. Empty on
/// archives without the extension profile (no marks table).
pub fn marked_region_doc_sessions(db: &impl FactsDb, limit: i64) -> Result<Vec<i64>> {
    if !has_region_marks_table(db)? {
        return Ok(Vec::new());
    }
    db.facts_query_map(
        "SELECT conversation_id FROM viewer_search_region_marks m \
         WHERE EXISTS (SELECT 1 FROM conversations c WHERE c.id = m.conversation_id) \
         ORDER BY m.conversation_id LIMIT ?1",
        &[ParamValue::from(limit)],
        |row| row.get_typed::<i64>(0),
    )
    .context("listing sessions carrying search-region marks")
}

/// The canonical identity fields every synthetic-slot Quill document for a
/// conversation is minted from — shared by the per-region docs
/// ([`region_docs`]) and the session×scope docs ([`session_scope_docs`])
/// so both lanes agree on `source_path`/`source_id`/`title`/`created_at`.
struct RegionDocIdentity {
    source_path: String,
    source_id: String,
    title: Option<String>,
    started_at: Option<i64>,
    agent: String,
    workspace: Option<String>,
}

/// Read the identity a conversation's synthetic-slot documents are minted
/// from. `None` when the conversation row is missing or carries an empty
/// `source_path` — the same honesty guard [`region_docs`] always applied:
/// a session that cannot be named cannot own documents.
fn region_doc_identity(
    db: &impl FactsDb,
    conversation_id: i64,
) -> Result<Option<RegionDocIdentity>> {
    let identity_rows = db.facts_query_map(
        "SELECT c.source_path, c.source_id, c.title, c.started_at, \
                COALESCE((SELECT a.slug FROM agents a WHERE a.id = c.agent_id), 'unknown'), \
                (SELECT w.path FROM workspaces w WHERE w.id = c.workspace_id) \
         FROM conversations c WHERE c.id = ?1 LIMIT 1",
        &[ParamValue::from(conversation_id)],
        |row| {
            Ok((
                row.get_typed::<Option<String>>(0)?,
                row.get_typed::<Option<String>>(1)?,
                row.get_typed::<Option<String>>(2)?,
                row.get_typed::<Option<i64>>(3)?,
                row.get_typed::<Option<String>>(4)?,
                row.get_typed::<Option<String>>(5)?,
            ))
        },
    )?;
    let Some((Some(source_path), source_id_col, title, started_at, agent, workspace)) =
        identity_rows.into_iter().next()
    else {
        return Ok(None);
    };
    if source_path.is_empty() {
        return Ok(None);
    }
    Ok(Some(RegionDocIdentity {
        source_path,
        source_id: source_id_col.unwrap_or_else(|| "local".to_owned()),
        title,
        started_at,
        agent: agent.unwrap_or_else(|| "unknown".to_owned()),
        workspace,
    }))
}

/// One packed session×scope chunk: its synthetic slot and full text.
pub struct SessionScopeDoc {
    pub slot: u64,
    pub text: String,
}

/// Greedy region-boundary packing — the ONE authoritative session-doc
/// chunker, shared by staging ([`session_scope_docs`]), the publication
/// keep set and the live-doc bound, so the three can never disagree on a
/// slot. Whole regions accumulate into a chunk until the next region
/// would push it past `cap` characters; an oversized region takes a chunk
/// of its own. `lengths` are per-region char contributions INCLUDING the
/// `'\n'` separator ([`SESSION_REGION_TEXT_SQL`]'s `+ 1`); the returned
/// `(start, end)` ranges are half-open index ranges over `lengths`.
pub(crate) fn pack_region_boundaries(lengths: &[u64], cap: u64) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    let mut start = 0_usize;
    let mut total = 0_u64;
    for (index, &len) in lengths.iter().enumerate() {
        if index > start && total.saturating_add(len) > cap {
            ranges.push((start, index));
            start = index;
            total = 0;
        }
        total = total.saturating_add(len);
    }
    if start < lengths.len() {
        ranges.push((start, lengths.len()));
    }
    ranges
}

/// The authoritative session×scope doc derivation for one conversation
/// (Stage 1 §6.1/§6.2): reads the committed region inventory, groups the
/// regions by scope in region-ordinal order, and packs each scope's texts
/// into whole-region chunks of at most [`SESSION_DOC_CHUNK_CHAR_CAP`]
/// characters. Every region contributes its text (`indexed_text` when a
/// fragment exists — noise-chat and every derived/meta row — else the
/// anchored message content for `chat` rows, else the empty string) plus
/// one `'\n'` separator, so no token ever merges across a region
/// boundary. The same function runs at staging (documents to upsert) and
/// at publication (the keep set that filters the conservative stale
/// queue), which is what keeps their slot sets identical. `None` when the
/// conversation cannot own documents at all (missing row, empty
/// `source_path`).
pub fn session_scope_docs(
    db: &impl FactsDb,
    conversation_id: i64,
) -> Result<Option<Vec<SessionScopeDoc>>> {
    // The identity guard mirrors `region_docs`: a conversation that cannot
    // own region documents cannot own session documents either.
    if region_doc_identity(db, conversation_id)?.is_none() {
        return Ok(None);
    }
    let regions = regions_for_conversation(db, conversation_id)?;
    // Chat regions hydrate their anchored message content in one read.
    let mut message_texts: HashMap<i64, String> = HashMap::new();
    if regions
        .iter()
        .any(|r| r.text_source == TEXT_SOURCE_CHAT && r.indexed_text.is_none())
    {
        for (id, content) in db.facts_query_map(
            "SELECT id, content FROM messages WHERE conversation_id = ?1",
            &[ParamValue::from(conversation_id)],
            |row| {
                Ok((
                    row.get_typed::<i64>(0)?,
                    row.get_typed::<Option<String>>(1)?,
                ))
            },
        )? {
            if let Some(content) = content {
                message_texts.insert(id, content);
            }
        }
    }
    // Group texts by scope, preserving region-ordinal order.
    let mut scopes: Vec<(String, Vec<(String, u64)>)> = Vec::new();
    for region in &regions {
        let text: String = match region.indexed_text.as_ref() {
            Some(fragment) => fragment.clone(),
            None if region.text_source == TEXT_SOURCE_CHAT => region
                .message_id
                .and_then(|id| message_texts.get(&id).cloned())
                .unwrap_or_default(),
            None => String::new(),
        };
        let chars = (text.chars().count() + 1) as u64; // + '\n' separator
        match scopes.iter_mut().find(|(scope, _)| scope == &region.scope) {
            Some((_, texts)) => texts.push((text, chars)),
            None => scopes.push((region.scope.clone(), vec![(text, chars)])),
        }
    }
    let mut docs = Vec::new();
    for (scope, texts) in &scopes {
        let lengths: Vec<u64> = texts.iter().map(|(_, chars)| *chars).collect();
        let ordinal = session_scope_ordinal(scope);
        let ranges = pack_region_boundaries(&lengths, SESSION_DOC_CHUNK_CHAR_CAP as u64);
        if ranges.len() >= SESSION_DOC_MAX_CHUNKS_PER_SCOPE as usize {
            bail!(
                "conversation {conversation_id} scope {scope:?} packs {} session-doc chunks, \
                 beyond the 2^{} per-scope slot space (SESSION_DOC_BASE)",
                ranges.len(),
                SESSION_DOC_SCOPE_SHIFT
            );
        }
        for (chunk_seq, (start, end)) in ranges.iter().enumerate() {
            let mut text = String::new();
            for (piece, _) in &texts[*start..*end] {
                text.push_str(piece);
                text.push('\n');
            }
            docs.push(SessionScopeDoc {
                slot: SESSION_DOC_BASE + (ordinal << SESSION_DOC_SCOPE_SHIFT) + chunk_seq as u64,
                text,
            });
        }
    }
    Ok(Some(docs))
}

/// Build the Quill `CassDocument` set for one session's session×scope
/// chunks ([`session_scope_docs`]) — the Stage 1 fast path's membership
/// surface. `None` when the conversation cannot own documents.
pub fn session_scope_cass_documents(
    db: &impl FactsDb,
    conversation_id: i64,
) -> Result<Option<Vec<CassDocument>>> {
    let Some(identity) = region_doc_identity(db, conversation_id)? else {
        return Ok(None);
    };
    let Some(chunks) = session_scope_docs(db, conversation_id)? else {
        return Ok(None);
    };
    let docs = chunks
        .into_iter()
        .map(|chunk| CassDocument {
            agent: identity.agent.clone(),
            workspace: identity.workspace.clone(),
            workspace_original: None,
            source_path: identity.source_path.clone(),
            msg_idx: chunk.slot,
            created_at: identity.started_at,
            title: identity.title.clone(),
            content: chunk.text,
            source_id: identity.source_id.clone(),
            origin_kind: "local".to_owned(),
            origin_host: None,
            conversation_id: Some(conversation_id),
        })
        .collect();
    Ok(Some(docs))
}

/// Build the Quill `CassDocument` set for one session's persisted regions:
/// every synthetic-slot region (derived/meta/noise-chat) becomes one doc
/// whose content is the persisted `indexed_text` fragment.
pub fn region_docs(db: &impl FactsDb, conversation_id: i64) -> Result<Vec<CassDocument>> {
    let Some(identity) = region_doc_identity(db, conversation_id)? else {
        return Ok(Vec::new());
    };
    let regions = regions_for_conversation(db, conversation_id)?;
    let mut docs = Vec::new();
    for region in regions
        .iter()
        .filter(|r| r.quill_idx >= REGION_DOC_BASE as i64)
    {
        let Some(content) = region.indexed_text.as_deref() else {
            continue;
        };
        docs.push(CassDocument {
            agent: identity.agent.clone(),
            workspace: identity.workspace.clone(),
            workspace_original: None,
            source_path: identity.source_path.clone(),
            msg_idx: region.quill_idx as u64,
            created_at: identity.started_at,
            title: identity.title.clone(),
            content: content.to_owned(),
            source_id: identity.source_id.clone(),
            origin_kind: "local".to_owned(),
            origin_host: None,
            conversation_id: Some(conversation_id),
        });
    }
    Ok(docs)
}

/// The marks-row snapshot [`stage_region_docs`] read while materializing
/// a session's region documents. [`stamp_region_docs_published`] replays
/// it as a compare-and-swap after the index commit: if a concurrent
/// republish, meta replacement or delete-queue append landed between
/// staging and the commit, the row's `region_version`/`source_revision`/
/// `docs_state`/`pending_doc_deletes` no longer match and the stamp is
/// refused — the session stays honestly pending for the next converge
/// pass instead of certifying documents built against a stale inventory
/// or clobbering a newer pending delete list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionDocStamp {
    pub conversation_id: i64,
    pub region_version: i64,
    pub source_revision: i64,
    pub docs_state: String,
    /// Raw stored JSON — compared byte-for-byte so a republished delete
    /// queue (even a reordered one) is never silently overwritten.
    pub pending_doc_deletes: String,
}

/// Stage one session's region documents on the uncommitted index writer:
/// delete every queued stale identity, upsert the current docs, and
/// return the [`RegionDocStamp`] the caller replays through
/// [`stamp_region_docs_published`] AFTER the index commit lands. The
/// marks row is deliberately untouched here — stamping `published`
/// before the commit would certify documents a failed commit or crash
/// never shipped (a `published` mark is skipped by `converge_pending`,
/// so the absent docs would silently drop literals from search).
/// Returns `(upserted doc count, stamp)`; `(0, None)` when no region
/// inventory was ever published for the session — not a pending one, an
/// absent one — which the search lane treats as honest incomplete
/// evidence.
pub fn stage_region_docs(
    index: &mut QuillCassIndex,
    db: &impl FactsDb,
    conversation_id: i64,
) -> Result<(usize, Option<RegionDocStamp>)> {
    let Some((docs, stamp)) = region_doc_stage(db, conversation_id)? else {
        return Ok((0, None));
    };
    let doc_count = docs.len();
    index
        .upsert_cass_documents(&docs)
        .context("upserting search-region documents")?;
    Ok((doc_count, Some(stamp)))
}

/// Read what [`stage_region_docs`] writes for one session without writing
/// it: the session's current region documents and the marks-row snapshot
/// that certifies them. `None` when the session has no marks row. Callers
/// that converge many sessions upsert the documents of many sessions in one
/// batch: replacing documents a committed candidate already holds costs one
/// publication per upsert call, and one call per session made a resumed
/// terminal converge quadratic in the session count.
pub fn region_doc_stage(
    db: &impl FactsDb,
    conversation_id: i64,
) -> Result<Option<(Vec<CassDocument>, RegionDocStamp)>> {
    let mark = db
        .facts_query_map(
            "SELECT region_version, source_revision, docs_state, pending_doc_deletes \
             FROM viewer_search_region_marks WHERE conversation_id = ?1",
            &[ParamValue::from(conversation_id)],
            |row| {
                Ok((
                    row.get_typed::<i64>(0)?,
                    row.get_typed::<i64>(1)?,
                    row.get_typed::<String>(2)?,
                    row.get_typed::<String>(3)?,
                ))
            },
        )?
        .into_iter()
        .next();
    let Some((region_version, source_revision, docs_state, pending_json)) = mark else {
        return Ok(None);
    };
    // Stale-identity deletes are NOT executed here: `delete_cass_document_ids`
    // requires a committed writer and would commit this writer mid-batch —
    // certifying every document staged so far (canonical AND region) ahead
    // of the real batch boundary. The stamp keeps the exact
    // `pending_doc_deletes` JSON so the post-commit stamp pass can retire
    // them and then compare-and-swap the queue away.
    //
    // Stage 1: the session×scope documents ride the same staging lane, so
    // the bridge writer, the indexer and the test harness all publish them
    // under the same marks discipline.
    let mut docs = region_docs(db, conversation_id)?;
    if let Some(session_docs) = session_scope_cass_documents(db, conversation_id)? {
        docs.extend(session_docs);
    }
    Ok(Some((
        docs,
        RegionDocStamp {
            conversation_id,
            region_version,
            source_revision,
            docs_state,
            pending_doc_deletes: pending_json,
        },
    )))
}

/// True when the session's region documents are already committed in the
/// live snapshot: its marks row is `published` at the current
/// `region_version`, carries no queued stale deletes, and binds the same
/// `source_revision` as its facts generation. An incremental lane skips
/// re-staging such a session — re-upserting an identity already live forces
/// Quill to publish a segment per session for no change. Anything else
/// (pending, meta-pending, a republished revision, a queued delete, or no
/// marks row) must stage. Rebuild lanes that write a fresh index never call
/// this; they re-materialize every marked session.
pub fn region_docs_published_current(db: &impl FactsDb, conversation_id: i64) -> Result<bool> {
    let mark = db
        .facts_query_map(
            "SELECT region_version, source_revision, docs_state, pending_doc_deletes \
             FROM viewer_search_region_marks WHERE conversation_id = ?1",
            &[ParamValue::from(conversation_id)],
            |row| {
                Ok((
                    row.get_typed::<i64>(0)?,
                    row.get_typed::<i64>(1)?,
                    row.get_typed::<String>(2)?,
                    row.get_typed::<String>(3)?,
                ))
            },
        )?
        .into_iter()
        .next();
    let Some((region_version, source_revision, docs_state, pending_json)) = mark else {
        return Ok(false);
    };
    if region_version != REGION_VERSION || docs_state != DOCS_PUBLISHED {
        return Ok(false);
    }
    let no_pending_deletes = serde_json::from_str::<Vec<String>>(&pending_json)
        .map(|ids| ids.is_empty())
        .unwrap_or(false);
    if !no_pending_deletes {
        return Ok(false);
    }
    let facts_revision = db
        .facts_query_map(
            "SELECT source_revision FROM viewer_session_facts \
             WHERE conversation_id = ?1 LIMIT 1",
            &[ParamValue::from(conversation_id)],
            |row| row.get_typed::<i64>(0),
        )?
        .into_iter()
        .next();
    Ok(facts_revision == Some(source_revision))
}

/// Stamp staged sessions `published` — callable ONLY after the index
/// commit that seals their documents. Each stamp replays the stage-time
/// marks snapshot as a compare-and-swap: `region_version`,
/// `source_revision`, `docs_state` and the exact `pending_doc_deletes`
/// JSON must still match or a concurrent republish/meta-edit queued work
/// this stamp never processed — those rows are left pending (returned
/// count excludes them) for the next converge pass. The `WHERE EXISTS`
/// guard shares the sort lane's honesty: a conversation deleted by an
/// FK-off writer mid-converge leaves an orphan marks row that must never
/// be stamped `published`.
pub fn stamp_region_docs_published(db: &impl FactsDb, stamps: &[RegionDocStamp]) -> Result<usize> {
    let mut stamped = 0_usize;
    for stamp in stamps {
        let updated = db.facts_execute(
            "UPDATE viewer_search_region_marks SET docs_state = 'published', \
             pending_doc_deletes = '[]' WHERE conversation_id = ?1 \
             AND region_version = ?2 AND source_revision = ?3 \
             AND docs_state = ?4 AND pending_doc_deletes = ?5 \
             AND EXISTS (SELECT 1 FROM conversations c WHERE c.id = ?1)",
            &[
                ParamValue::from(stamp.conversation_id),
                ParamValue::from(stamp.region_version),
                ParamValue::from(stamp.source_revision),
                ParamValue::from(stamp.docs_state.as_str()),
                ParamValue::from(stamp.pending_doc_deletes.as_str()),
            ],
        )?;
        stamped += usize::from(updated > 0);
    }
    Ok(stamped)
}

/// Hydrate one region's text for verification/snippets — the ONLY message
/// read the indexed query path performs, bounded by the hydrated region
/// set (`text_source='chat'` reads the canonical row it anchors; derived
/// and meta lanes return the persisted fragment).
pub fn hydrate_region_text(db: &impl FactsDb, region: &RegionRow) -> Result<Option<String>> {
    if region.text_source == TEXT_SOURCE_CHAT && region.indexed_text.is_none() {
        let Some(message_id) = region.message_id else {
            return Ok(None);
        };
        let rows = db.facts_query_map(
            "SELECT content FROM messages WHERE id = ?1 LIMIT 1",
            &[ParamValue::from(message_id)],
            |row| row.get_typed::<Option<String>>(0),
        )?;
        return Ok(rows.into_iter().next().flatten().map(|t| bounded(&t)));
    }
    Ok(region.indexed_text.clone())
}

#[cfg(test)]
mod tests {
    //! Equivalence and edge-case coverage for [`live_region_doc_bound`].
    //! The pre-rewrite correlated-subquery statement survives here only as
    //! the oracle the set-based query must match.

    use super::*;
    use crate::model::types::{Agent, AgentKind};
    use crate::storage::sqlite::FrankenStorage;

    /// The original per-marks-row correlated statement, verbatim.
    const CORRELATED_ORACLE_SQL: &str = "SELECT \
           (SELECT COUNT(*) FROM viewer_search_regions r \
            WHERE r.conversation_id = m.conversation_id \
              AND r.quill_idx >= ?1 AND r.indexed_text IS NOT NULL), \
           m.pending_doc_deletes \
         FROM viewer_search_region_marks m \
         WHERE EXISTS (SELECT 1 FROM conversations c WHERE c.id = m.conversation_id)";

    /// Same accumulation as production: doc rows plus the length of the
    /// `pending_doc_deletes` JSON array, unparseable JSON counting as zero,
    /// plus the Stage 1 session×scope chunk count — here in the slow,
    /// obviously-correct per-conversation form (one `session_scope_docs`
    /// call per marked conversation), which the streaming GROUP-BY-fold
    /// query in production must match exactly.
    fn oracle_bound(db: &impl FactsDb) -> Result<u64> {
        let mut bound = 0_u64;
        for (doc_rows, deletes_json) in db.facts_query_map(
            CORRELATED_ORACLE_SQL,
            &[ParamValue::from(REGION_DOC_BASE as i64)],
            |row| Ok((row.get_typed::<i64>(0)?, row.get_typed::<String>(1)?)),
        )? {
            bound = bound.saturating_add(doc_rows.max(0) as u64);
            let deletes: Vec<Value> = serde_json::from_str(&deletes_json).unwrap_or_default();
            bound = bound.saturating_add(deletes.len() as u64);
        }
        for conversation_id in db.facts_query_map(
            "SELECT m.conversation_id FROM viewer_search_region_marks m \
             WHERE EXISTS (SELECT 1 FROM conversations c WHERE c.id = m.conversation_id)",
            &[],
            |row| row.get_typed::<i64>(0),
        )? {
            if let Some(chunks) = session_scope_docs(db, conversation_id)? {
                bound = bound.saturating_add(chunks.len() as u64);
            }
        }
        Ok(bound)
    }

    fn fixture() -> (tempfile::TempDir, FrankenStorage) {
        let temp = tempfile::tempdir().expect("tempdir");
        let storage =
            FrankenStorage::open(&temp.path().join("archive.db")).expect("canonical schema");
        crate::viewer_extensions::schema::ensure_schema(storage.raw()).expect("extension schema");
        // Orphan rows (a conversation deleted under live regions) cannot be
        // written with cascading foreign keys enforced, so the fixture
        // relaxes enforcement; the query itself must still exclude them.
        storage
            .raw()
            .execute("PRAGMA foreign_keys = OFF")
            .expect("relax foreign keys for orphan fixtures");
        (temp, storage)
    }

    fn agent_id(storage: &FrankenStorage) -> i64 {
        storage
            .ensure_agent(&Agent {
                id: None,
                slug: "codex".into(),
                name: "Codex".into(),
                version: None,
                kind: AgentKind::Cli,
            })
            .expect("agent")
    }

    fn insert_conversation(db: &impl FactsDb, id: i64, agent: i64) {
        db.facts_execute(
            "INSERT INTO conversations (id, agent_id, source_path) VALUES (?1, ?2, ?3)",
            &[
                ParamValue::from(id),
                ParamValue::from(agent),
                ParamValue::from(format!("/fixture/{id}.jsonl")),
            ],
        )
        .expect("conversation");
    }

    fn insert_marks(db: &impl FactsDb, conversation_id: i64, pending_doc_deletes: &str) {
        db.facts_execute(
            "INSERT INTO viewer_search_region_marks \
             (conversation_id, region_version, pending_doc_deletes) VALUES (?1, 1, ?2)",
            &[
                ParamValue::from(conversation_id),
                ParamValue::from(pending_doc_deletes),
            ],
        )
        .expect("marks");
    }

    fn insert_region(
        db: &impl FactsDb,
        conversation_id: i64,
        ordinal: i64,
        quill_idx: i64,
        indexed_text: Option<&str>,
    ) {
        db.facts_execute(
            "INSERT INTO viewer_search_regions \
             (conversation_id, region_ordinal, region_key, scope, quill_idx, indexed_text) \
             VALUES (?1, ?2, ?3, 'block', ?4, ?5)",
            &[
                ParamValue::from(conversation_id),
                ParamValue::from(ordinal),
                ParamValue::from(format!("region-{conversation_id}-{ordinal}")),
                ParamValue::from(quill_idx),
                ParamValue::from(indexed_text),
            ],
        )
        .expect("region");
    }

    fn base(offset: i64) -> i64 {
        REGION_DOC_BASE as i64 + offset
    }

    /// The session×scope chunk slot `session_doc_stale_identities`
    /// conservatively covers for `scope`'s `chunk`-th old chunk.
    fn session_slot(scope: &str, chunk: u64) -> i64 {
        (SESSION_DOC_BASE + (session_scope_ordinal(scope) << SESSION_DOC_SCOPE_SHIFT) + chunk)
            as i64
    }

    #[test]
    fn live_region_doc_bound_matches_correlated_oracle_and_hand_count() -> Result<()> {
        let (_temp, storage) = fixture();
        let db = storage.raw();
        let agent = agent_id(&storage);
        // Conversation 5 is deliberately never inserted (it was deleted).
        for id in [1, 2, 3, 4, 6, 7] {
            insert_conversation(db, id, agent);
        }

        // conv 1: two deletes; 3 live doc regions (incl. one exactly at the
        // base); a NULL-text synthetic row, and canonical rows below the
        // base whose text must not count. Hand count: 3 + 2 = 5.
        insert_marks(db, 1, r#"["a","b"]"#);
        insert_region(db, 1, 0, base(0), Some("x"));
        insert_region(db, 1, 1, base(1), Some("y"));
        insert_region(db, 1, 2, base(2), None);
        insert_region(db, 1, 3, 7, None);
        insert_region(db, 1, 4, 8, Some("z"));
        insert_region(db, 1, 5, base(3), Some("b"));

        // conv 2: marks row without any region rows. Hand count: 0.
        insert_marks(db, 2, "[]");

        // conv 3: canonical-only regions (below the base). Hand count: 1
        // (the single pending delete).
        insert_marks(db, 3, r#"["x"]"#);
        insert_region(db, 3, 0, 3, None);
        insert_region(db, 3, 1, 4, Some("c"));

        // conv 4: regions with no marks row are never counted. Hand count: 0.
        insert_region(db, 4, 0, base(0), Some("w"));
        insert_region(db, 4, 1, base(1), Some("v"));

        // conv 5: marks and regions of a deleted conversation (orphans; the
        // conversation-exists filter must drop the whole group). Hand count: 0.
        insert_marks(db, 5, r#"["d1","d2","d3"]"#);
        insert_region(db, 5, 0, base(0), Some("o"));

        // conv 6: a non-array JSON deletes payload contributes zero deletes,
        // but its live doc region still counts. Hand count: 1.
        insert_marks(db, 6, r#"{"not":"an array"}"#);
        insert_region(db, 6, 0, base(0), Some("p"));
        insert_region(db, 6, 1, base(1), None);

        // conv 7: unparseable deletes JSON contributes zero. Hand count: 0.
        insert_marks(db, 7, "not json");

        // Session×scope share (Stage 1): every marked conversation with
        // regions packs its 'block' rows into ONE chunk (all fixture texts
        // are far under the 65,536-char cap) — conv 1 (6 regions), conv 3
        // (2 regions) and conv 6 (2 regions) add one chunk each; conv 2 and
        // conv 7 own no regions and conv 4/conv 5 are excluded exactly as
        // their region share is. Hand count: 7 + 3 = 10.
        let oracle = oracle_bound(db)?;
        let set_based = live_region_doc_bound(db)?;
        assert_eq!(oracle, 10, "oracle drifted from the hand count");
        assert_eq!(set_based, oracle, "set-based bound must equal the oracle");
        assert_eq!(set_based, 10);
        Ok(())
    }

    #[test]
    fn live_region_doc_bound_empty_tables_are_zero() -> Result<()> {
        let (_temp, storage) = fixture();
        assert_eq!(live_region_doc_bound(storage.raw())?, 0);
        assert_eq!(oracle_bound(storage.raw())?, 0);
        Ok(())
    }

    #[test]
    fn live_region_doc_bound_marks_without_regions_and_regions_without_marks() -> Result<()> {
        let (_temp, storage) = fixture();
        let db = storage.raw();
        let agent = agent_id(&storage);
        insert_conversation(db, 1, agent);
        insert_conversation(db, 2, agent);

        // Marks alone: only the deletes count.
        insert_marks(db, 1, r#"["a","b"]"#);
        assert_eq!(live_region_doc_bound(db)?, 2);
        assert_eq!(oracle_bound(db)?, 2);

        // Regions alone (no marks row for conversation 2): nothing counts.
        insert_region(db, 2, 0, base(0), Some("q"));
        assert_eq!(live_region_doc_bound(db)?, 2);
        assert_eq!(oracle_bound(db)?, 2);

        // Only the marks row's own conversation contributes live regions.
        insert_marks(db, 2, "[]");
        // conv 2 now owns its region doc AND one session×scope chunk.
        assert_eq!(live_region_doc_bound(db)?, 4);
        assert_eq!(oracle_bound(db)?, 4);
        Ok(())
    }

    #[test]
    fn live_region_doc_bound_without_marks_table_is_zero() -> Result<()> {
        // Canonical-only archive: no extension profile, so no marks table.
        let temp = tempfile::tempdir().expect("tempdir");
        let storage =
            FrankenStorage::open(&temp.path().join("canonical.db")).expect("canonical schema");
        assert_eq!(live_region_doc_bound(storage.raw())?, 0);
        Ok(())
    }

    fn fixture_doc_identity(conversation_id: i64, slot: i64) -> String {
        let source_path = format!("/fixture/{conversation_id}.jsonl");
        frankensearch::quill::cass::cass_document_identity(
            "local",
            frankensearch::quill::cass::CassConversationKey {
                source_path: &source_path,
                id: Some(conversation_id),
            },
            slot as u64,
        )
    }

    fn pending_deletes(db: &impl FactsDb, conversation_id: i64) -> Result<Vec<String>> {
        let raw: String = db
            .facts_query_map(
                "SELECT pending_doc_deletes FROM viewer_search_region_marks \
                 WHERE conversation_id = ?1",
                &[ParamValue::from(conversation_id)],
                |row| row.get_typed::<String>(0),
            )?
            .into_iter()
            .next()
            .expect("marks row");
        Ok(serde_json::from_str(&raw)?)
    }

    /// A slot freed by an earlier republish and queued for deletion can be
    /// reclaimed by the new meta inventory before the index lane runs. The
    /// queued delete then targets a live document: the post-commit stamp
    /// pass removes it after the commit already counted it, and a fresh
    /// reader serves fewer documents than the rebuild published (GH #457).
    #[test]
    fn replace_meta_regions_drops_pending_deletes_its_new_slots_reclaim() -> Result<()> {
        let (_temp, storage) = fixture();
        let db = storage.raw();
        let agent = agent_id(&storage);
        insert_conversation(db, 1, agent);
        let reclaimed = fixture_doc_identity(1, base(0));
        let still_stale = fixture_doc_identity(1, base(5));
        insert_marks(
            db,
            1,
            &serde_json::to_string(&[reclaimed.as_str(), still_stale.as_str()])?,
        );
        // One tag yields exactly one meta region at ordinal 0: slot base(0).
        db.facts_execute(
            "INSERT INTO viewer_tags (conversation_id, tag) VALUES (1, 'release')",
            &[],
        )?;

        replace_meta_regions(db, 1)?;

        let docs = region_docs(db, 1)?;
        assert_eq!(docs.len(), 1, "the tag must materialize one meta doc");
        assert_eq!(docs[0].msg_idx, base(0) as u64);
        let pending = pending_deletes(db, 1)?;
        assert!(
            !pending.contains(&reclaimed),
            "a slot the new meta inventory owns must leave the delete queue: {pending:?}"
        );
        assert_eq!(
            pending,
            vec![still_stale],
            "unrelated stale ids stay queued"
        );
        Ok(())
    }

    /// A resumed terminal rebuild re-converges every marked session onto a
    /// candidate whose commit already holds their region documents, so each
    /// upsert replaces live documents and publishes a successor. One upsert
    /// per session published one segment per session and validated every
    /// segment each time: quadratic, hours on a real archive. The converge
    /// must batch sessions, and the replacement must still leave exactly one
    /// live document per identity.
    #[test]
    fn converge_all_onto_a_committed_candidate_publishes_per_batch_not_per_session() -> Result<()> {
        let (temp, storage) = fixture();
        let db = storage.raw();
        let agent = agent_id(&storage);
        let sessions = 6_i64;
        for id in 1..=sessions {
            insert_conversation(db, id, agent);
            insert_marks(db, id, "[]");
            insert_region(db, id, 0, base(0), Some("first region"));
            insert_region(db, id, 1, base(1), Some("second region"));
        }
        let index_path = temp.path().join("index");
        let mut index = crate::search::tantivy::TantivyIndex::open_or_create(&index_path)?;
        // Stage 1: each conversation now ships 2 region documents plus 1
        // session×scope chunk (its two 'block' regions pack into one
        // 28-char chunk) — 3 documents per session.
        assert_eq!(
            index.converge_all_search_region_docs(db, i64::MAX)?,
            (sessions as usize, 3 * sessions as usize)
        );
        index.commit()?;
        drop(index);

        // The resumed run opens a new writer on the committed candidate.
        let mut resumed = crate::search::tantivy::TantivyIndex::open_or_create(&index_path)?;
        let segments_before = resumed.segment_count();
        assert_eq!(
            resumed.converge_all_search_region_docs(db, i64::MAX)?,
            (sessions as usize, 3 * sessions as usize)
        );
        resumed.commit()?;
        let published = resumed.segment_count().saturating_sub(segments_before);
        assert!(
            published <= 1,
            "re-converging {sessions} sessions published {published} segments; \
             one batch must publish at most one"
        );
        assert_eq!(
            resumed.doc_count()?,
            3 * sessions as u64,
            "replacement must leave one live document per identity"
        );
        Ok(())
    }

    /// A packet can name a session by another path than the conversation
    /// row it merged into (a Cursor composer re-emitted under a second
    /// path). Region documents are minted from the row, so the delete queue
    /// must be too: ids built from the packet path never match the live
    /// documents, a reclaimed slot survives the keep filter, and the
    /// post-commit delete removes a live document (GH #457).
    #[test]
    fn publish_search_regions_queues_row_identities_when_packet_path_differs() -> Result<()> {
        let (_temp, storage) = fixture();
        let db = storage.raw();
        let agent = agent_id(&storage);
        insert_conversation(db, 1, agent);
        // An earlier republish freed slot base(0) and queued it; base(3) is
        // still owned by the previous inventory and is about to be dropped.
        let reclaimed = fixture_doc_identity(1, base(0));
        let still_stale = fixture_doc_identity(1, base(7));
        insert_marks(
            db,
            1,
            &serde_json::to_string(&[reclaimed.as_str(), still_stale.as_str()])?,
        );
        insert_region(db, 1, 3, base(3), Some("old meta"));
        // One tag and no messages: the new inventory is one meta region at
        // ordinal 0, which reclaims slot base(0).
        db.facts_execute(
            "INSERT INTO viewer_tags (conversation_id, tag) VALUES (1, 'release')",
            &[],
        )?;
        let conv = crate::model::types::Conversation {
            id: None,
            agent_slug: "codex".into(),
            workspace: None,
            external_id: Some("composer-1".into()),
            title: None,
            source_path: "/elsewhere/composer-1.json".into(),
            started_at: None,
            ended_at: None,
            approx_tokens: None,
            metadata_json: Value::Null,
            source_id: "local".into(),
            origin_host: None,
            messages: Vec::new(),
        };
        let generation = super::super::projection::generation_for_canonical(db, &conv, 1)?;
        assert_ne!(
            generation.packet.payload.identity.source_path, "/fixture/1.jsonl",
            "the fixture must exercise a packet path that differs from the row"
        );

        publish_search_regions(db, 1, &generation.packet, &generation.structure)?;

        let docs = region_docs(db, 1)?;
        assert_eq!(docs.len(), 1, "the tag must materialize one meta doc");
        assert_eq!(docs[0].msg_idx, base(0) as u64);
        let pending = pending_deletes(db, 1)?;
        assert_eq!(
            pending,
            vec![
                still_stale,
                fixture_doc_identity(1, base(3)),
                fixture_doc_identity(1, session_slot("block", 0)),
            ],
            "the reclaimed live slot leaves the queue, the dropped slot is \
             queued under the row identity its live document carries, and the \
             old inventory's one 'block' region conservatively bounds one old \
             session-doc chunk (unknown ordinal 6, chunk 0) that the new \
             chat-scope generation no longer keeps — it never held a live \
             document here, and the post-commit delete ignores absent \
             identities"
        );
        Ok(())
    }
}
