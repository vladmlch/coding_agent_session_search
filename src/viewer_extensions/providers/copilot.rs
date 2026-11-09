//! Copilot (VS Code) session facts from connector-built extras. The
//! pinned FAD connector (copilot_vscode) builds the extras itself and
//! CASS's ingest seam archives `NormalizedMessage.invocations` under
//! `extra.cass.invocations`, so the persisted shapes are:
//!   - user rows: `{request_id?}`;
//!   - assistant rows: `{request_id?, model_id?, cass?,
//!     copilot_response_parts?}` with `cass = {token_usage?,
//!     token_usage_scope?, tool_call_count?, invocations?}` and — since
//!     FAD F14 — the raw serialized `response` part array verbatim under
//!     `copilot_response_parts` (non-empty arrays only; verified against
//!     the pre-F14 persisted copilot messages and the F14 census of the
//!     owner's real sessions — key structure only here, synthetic text
//!     in tests).
//!
//! Connector contract (FAD `response_content` + F14):
//!   - assistant content joins the markdown part texts; a markdown value
//!     (bare / `markdownContent.value` / `markdownVuln.content.value`)
//!     the connector joins MUST appear in the delivered content —
//!     verified, and a miss is the `copilot_part_text_not_in_content`
//!     drift mark, never a guess;
//!   - every archived part classifies against the verified VS Code
//!     shapes ([`scan_copilot_parts`]); an unknown kind or key set is
//!     `copilot_part_unknown` — drift keeps the inventories partial;
//!   - `thinking` parts (value strings + `generatedTitle`; `id` and
//!     `metadata` are never-rendered bookkeeping) count as thinking
//!     blocks and index as `m{idx}:thinking` regions
//!     (search_regions.rs, REGION_VERSION 5);
//!   - `toolInvocationSerialized` parts GROUP on `toolCallId` — VS
//!     Code's own invocation identity: a delegation pair (the public
//!     tool plus its `_internal` delegate sharing one call id) is ONE
//!     call named by the first part's `toolId`, with
//!     `toolSpecificData.rawInput` as the arguments,
//!     `invocationMessage`/`pastTenseMessage` values plus the
//!     `isConfirmed.reason` as the rendered call text, and
//!     `resultDetails` as the recorded result — so the two invocations
//!     the pre-F14 seam archived for such a pair can never reach the
//!     `conflicting_event` taint;
//!   - `mcpServersStarting` parts carry `didStartServerIds` (possibly
//!     empty: the part itself is the MCP evidence) and index as
//!     `m{idx}:mcp` tools regions;
//!   - a grouped call without any `resultDetails` part proves the
//!     result never existed at the source — the parts array is the
//!     verbatim record (`unpaired_results_carry_no_text` semantics,
//!     the gemini precedent). The narrowing is only sound while no
//!     pre-F14 row feeds call evidence, so the collector keeps the
//!     default `tool_result_missing` scopes when one does;
//!   - pre-F14 rows (no `copilot_response_parts` key) keep the legacy
//!     verdicts: thinking parts and the raw response parts were
//!     dropped at the connector
//!     (`copilot_thinking_dropped_by_connector`,
//!     `copilot_response_parts_dropped`) and `cass.invocations` is the
//!     only call evidence, with the default `tool_result_missing`
//!     scopes;
//!   - `model_id` is the turn's model and the only model evidence;
//!   - copilot records no compaction boundaries.
//!
//! The inner `cass` keys are not gated: the envelope is CASS-authored
//! accounting (token usage, the seam's archived invocations) and its
//! keys carry no user-visible text.

use std::collections::{BTreeSet, HashMap};

use serde_json::{Map, Value};

use crate::model::conversation_packet::ConversationPacket;

use super::classification::{self, ClassifyCall};
use super::{
    CORE_FIELDS, CoverageBuilder, Event, ProviderExtraction, RecordedCall, RecordedResult,
    ToolCollector, events, text_id,
};

const EXTRACTOR_REVISION: &str = "copilot-facts-v2";

/// The connector-built user extra: the turn's request id.
const USER_KEYS: &[&str] = &["request_id"];
/// The connector-built assistant extra: request id, the turn's model, the
/// token-accounting envelope the ingest seam joins, and (FAD F14) the raw
/// response part array.
const ASSISTANT_KEYS: &[&str] = &["request_id", "model_id", "cass", "copilot_response_parts"];

/// `true` when every top-level extra key is one the connector writes.
fn keys_known(extra: &Value, allowed: &[&str]) -> bool {
    extra
        .as_object()
        .is_some_and(|map| map.keys().all(|key| allowed.contains(&key.as_str())))
}

// ---------------------------------------------------------------------------
// Archived response-part classification — shared with the search-regions
// copilot arm (search_regions.rs) so the completeness claims and the
// indexed surface stay identical. Shapes verified against the F14 census
// of the owner's real Copilot sessions (thinking, mcpServersStarting,
// toolInvocationSerialized delegation pairs with resultDetails, bare
// IMarkdownString parts) plus the VS Code serialized-part types.
// ---------------------------------------------------------------------------

/// Keys of a bare `IMarkdownString` part (markdown serializes without a
/// `kind`: `toExport` unwraps `markdownContent`).
const BARE_PART_KEYS: &[&str] = &[
    "value",
    "isTrusted",
    "supportHtml",
    "supportThemeIcons",
    "baseUri",
    "isUntitled",
];
/// Keys of a `markdownContent` part.
const MARKDOWN_CONTENT_KEYS: &[&str] =
    &["kind", "value", "isTrusted", "supportThemeIcons", "baseUri"];
/// Keys of a `markdownVuln` part.
const MARKDOWN_VULN_KEYS: &[&str] = &["kind", "content", "vulns"];
/// Keys of the `IMarkdownString` nested under a tool part's rendered
/// messages.
const MARKDOWN_STRING_KEYS: &[&str] = &[
    "value",
    "isTrusted",
    "supportAlertSyntax",
    "supportHtml",
    "supportThemeIcons",
    "baseUri",
];
/// Keys of a `thinking` part.
const THINKING_KEYS: &[&str] = &["kind", "id", "value", "metadata", "generatedTitle"];
/// Keys of an `mcpServersStarting` part.
const MCP_STARTING_KEYS: &[&str] = &["kind", "didStartServerIds"];
/// Keys of a `toolInvocationSerialized` part.
const TOOL_PART_KEYS: &[&str] = &[
    "kind",
    "toolCallId",
    "toolId",
    "invocationMessage",
    "pastTenseMessage",
    "isConfirmed",
    "isComplete",
    "presentation",
    "source",
    "toolSpecificData",
    "resultDetails",
];
/// Keys of the `source` sub-object (a tool's origin label — identity, not
/// prose).
const SOURCE_KEYS: &[&str] = &["label", "type"];
/// Keys of the `isConfirmed` sub-object (its `reason` is rendered call
/// text).
const CONFIRMATION_KEYS: &[&str] = &["type", "reason"];
/// Keys of the `toolSpecificData` sub-object.
const TOOL_SPECIFIC_DATA_KEYS: &[&str] = &["kind", "rawInput"];

/// One `toolInvocationSerialized` part, classified (the caller groups on
/// `toolCallId`).
pub(crate) struct CopilotToolPart {
    /// The part's `toolId` (the group's name is the first one in source
    /// order).
    pub tool_id: Option<String>,
    /// The part's `toolCallId` — VS Code's invocation identity.
    pub call_id: Option<String>,
    /// `toolSpecificData.rawInput` (the call arguments).
    pub arguments: Option<Value>,
    /// The rendered call text: `invocationMessage`/`pastTenseMessage`
    /// markdown values plus the `isConfirmed.reason`.
    pub message_texts: Vec<String>,
    /// The `resultDetails` string items (`None` when the part records no
    /// result at all).
    pub result_texts: Option<Vec<String>>,
}

/// One call group: every `toolInvocationSerialized` part sharing a
/// `toolCallId`, in first-seen order.
pub(crate) struct CopilotToolGroup {
    pub call_id: String,
    /// First non-empty `toolId` in source order.
    pub name: Option<String>,
    /// First non-null `toolSpecificData.rawInput` in source order.
    pub arguments: Option<Value>,
    /// Rendered call texts across the group, source order.
    pub message_texts: Vec<String>,
    /// Every `resultDetails` string across the group, source order
    /// (`None` when no part records a result).
    pub result_texts: Option<Vec<String>>,
}

/// One archived response array, classified. Every field the facts lane
/// marks on is also the field the regions lane indexes — the two lanes
/// call the same [`scan_copilot_parts`].
#[derive(Default)]
pub(crate) struct CopilotPartsScan {
    /// Non-whitespace-empty markdown values the connector joins into the
    /// delivered content (containment is the facts lane's check).
    pub content_texts: Vec<String>,
    /// Thinking texts per part, source order (an empty vec is a known
    /// empty block).
    pub thinking_texts: Vec<Vec<String>>,
    /// Every `mcpServersStarting` server id, source order.
    pub mcp_ids: Vec<String>,
    /// `true` when at least one `mcpServersStarting` part exists (the
    /// part itself is MCP evidence, even with no ids).
    pub mcp_seen: bool,
    /// Call groups keyed by `toolCallId`, first-seen order.
    pub tool_groups: Vec<CopilotToolGroup>,
    /// Parts whose shape is outside the verified contract.
    pub unknown_parts: usize,
    /// Tool parts without a `toolCallId` (their text cannot pair).
    pub identity_missing_parts: usize,
}

/// `true` when every key is one of `allowed`.
fn keys_subset(object: &Map<String, Value>, allowed: &[&str]) -> bool {
    object.keys().all(|key| allowed.contains(&key.as_str()))
}

/// A boolean flag that may be absent, nothing else.
fn bool_flag(value: Option<&Value>) -> bool {
    value.is_none_or(Value::is_boolean)
}

/// A non-whitespace-empty string under `key`, or `None`.
fn nonempty_string(object: &Map<String, Value>, key: &str) -> Option<String> {
    object
        .get(key)
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .map(str::to_string)
}

/// The markdown value of an `IMarkdownString` object: outer `None` is a
/// malformed shape, inner `None` carries no text.
fn markdown_value(value: &Value) -> Option<Option<String>> {
    match value {
        Value::String(text) => Some(Some(text.clone()).filter(|t| !t.trim().is_empty())),
        Value::Object(map) => {
            if !keys_subset(map, MARKDOWN_STRING_KEYS) {
                return None;
            }
            match map.get("value") {
                None | Some(Value::Null) => Some(None),
                Some(Value::String(text)) => {
                    Some(Some(text.clone()).filter(|t| !t.trim().is_empty()))
                }
                Some(_) => None,
            }
        }
        Value::Null => Some(None),
        Value::Bool(_) | Value::Number(_) | Value::Array(_) => None,
    }
}

/// The `isConfirmed` confirmation: a bare bool or `{type, reason?}` (the
/// scalar `type` is an enum constant, the `reason` rendered call text).
/// Outer `None` is a malformed shape, inner `None` carries no text.
fn confirmation_reason(value: &Value) -> Option<Option<String>> {
    match value {
        Value::Bool(_) | Value::Null => Some(None),
        Value::Object(map) => {
            if !keys_subset(map, CONFIRMATION_KEYS) {
                return None;
            }
            match map.get("reason") {
                None | Some(Value::Null) => Some(None),
                Some(Value::String(text)) => {
                    Some(Some(text.clone()).filter(|t| !t.trim().is_empty()))
                }
                Some(_) => None,
            }
        }
        Value::String(_) | Value::Number(_) | Value::Array(_) => None,
    }
}

/// The `resultDetails` string items: strings are recorded result text,
/// `$mid` objects are marshalled VS Code references (non-text), scalars
/// are non-text values, and an object without `$mid` is a shape this
/// contract does not know. `None` is malformed.
fn result_detail_texts(value: &Value) -> Option<Vec<String>> {
    let Value::Array(items) = value else {
        return None;
    };
    let mut texts = Vec::new();
    for item in items {
        match item {
            Value::String(text) => {
                if !text.trim().is_empty() {
                    texts.push(text.clone());
                }
            }
            // Marshalled reference (census: a URI); a plain object could
            // carry unknown text — never a shape guess.
            Value::Object(map) => {
                if !map.contains_key("$mid") {
                    return None;
                }
            }
            // A nested array is not a verified result shape.
            Value::Array(_) => return None,
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
        }
    }
    Some(texts)
}

/// One `thinking` part's texts (`value` strings plus `generatedTitle`);
/// `None` is a malformed shape.
fn thinking_part_texts(part: &Map<String, Value>) -> Option<Vec<String>> {
    let mut texts = Vec::new();
    match part.get("value") {
        None | Some(Value::Null) => {}
        Some(Value::String(text)) => {
            if !text.trim().is_empty() {
                texts.push(text.clone());
            }
        }
        Some(Value::Array(items)) => {
            for item in items {
                let text = item.as_str()?;
                if !text.trim().is_empty() {
                    texts.push(text.to_string());
                }
            }
        }
        Some(_) => return None,
    }
    // `id` is an identifier, `metadata` never-rendered telemetry
    // (stopReason/vscodeReasoningDone observed): neither is text.
    if !part.get("id").is_none_or(Value::is_string) {
        return None;
    }
    if let Some(title) = nonempty_string(part, "generatedTitle") {
        texts.push(title);
    }
    Some(texts)
}

/// One `mcpServersStarting` part's server ids; `None` is malformed.
fn mcp_part_ids(part: &Map<String, Value>) -> Option<Vec<String>> {
    match part.get("didStartServerIds") {
        None | Some(Value::Null) => Some(Vec::new()),
        Some(Value::Array(items)) => {
            let mut ids = Vec::new();
            for item in items {
                let id = item.as_str()?;
                if !id.is_empty() {
                    ids.push(id.to_string());
                }
            }
            Some(ids)
        }
        Some(_) => None,
    }
}

/// One `toolInvocationSerialized` part, or `None` when its shape is
/// outside the verified contract.
fn tool_part(part: &Map<String, Value>) -> Option<CopilotToolPart> {
    if !keys_subset(part, TOOL_PART_KEYS) {
        return None;
    }
    let call_id = nonempty_string(part, "toolCallId");
    let tool_id = nonempty_string(part, "toolId");
    if !bool_flag(part.get("isComplete")) {
        return None;
    }
    if !part.get("presentation").is_none_or(Value::is_string) {
        return None;
    }
    let mut message_texts = Vec::new();
    for key in ["invocationMessage", "pastTenseMessage"] {
        match part.get(key) {
            None | Some(Value::Null) => {}
            Some(value) => match markdown_value(value) {
                Some(Some(text)) => message_texts.push(text),
                Some(None) => {}
                None => return None,
            },
        }
    }
    match part.get("isConfirmed") {
        None | Some(Value::Null) => {}
        Some(value) => match confirmation_reason(value) {
            Some(Some(text)) => message_texts.push(text),
            Some(None) => {}
            None => return None,
        },
    }
    match part.get("source") {
        None | Some(Value::Null) => {}
        Some(Value::Object(map)) => {
            if !keys_subset(map, SOURCE_KEYS)
                || !map.get("label").is_none_or(Value::is_string)
                || !map.get("type").is_none_or(Value::is_string)
            {
                return None;
            }
        }
        Some(_) => return None,
    }
    let mut arguments = None;
    match part.get("toolSpecificData") {
        None | Some(Value::Null) => {}
        Some(Value::Object(map)) => {
            if !keys_subset(map, TOOL_SPECIFIC_DATA_KEYS)
                || !map.get("kind").is_none_or(Value::is_string)
            {
                return None;
            }
            arguments = map
                .get("rawInput")
                .cloned()
                .filter(|value| !value.is_null());
        }
        Some(_) => return None,
    }
    let result_texts = match part.get("resultDetails") {
        None | Some(Value::Null) => None,
        Some(value) => Some(result_detail_texts(value)?),
    };
    Some(CopilotToolPart {
        tool_id,
        call_id,
        arguments,
        message_texts,
        result_texts,
    })
}

/// Classify one archived response array against the verified VS Code
/// shapes. Unknown shapes count into `unknown_parts` — the caller keeps
/// its not-read marks; nothing here is ever a guess.
pub(crate) fn scan_copilot_parts(parts: &[Value]) -> CopilotPartsScan {
    let mut scan = CopilotPartsScan::default();
    let mut group_index: HashMap<String, usize> = HashMap::new();
    for part in parts {
        // A `null` slot is a deleted append-log entry: known and empty.
        if part.is_null() {
            continue;
        }
        let Some(object) = part.as_object() else {
            // A bare string part would be joinable text the connector
            // never joined: hand it to the containment check so the miss
            // is marked, never silently dropped.
            if let Some(text) = part
                .as_str()
                .filter(|text| !text.trim().is_empty())
                .map(str::to_string)
            {
                scan.content_texts.push(text);
            } else {
                scan.unknown_parts += 1;
            }
            continue;
        };
        let kind = object.get("kind").and_then(Value::as_str);
        match kind {
            None => {
                if !keys_subset(object, BARE_PART_KEYS)
                    || !bool_flag(object.get("isTrusted"))
                    || !bool_flag(object.get("supportHtml"))
                    || !bool_flag(object.get("supportThemeIcons"))
                    || !bool_flag(object.get("isUntitled"))
                {
                    scan.unknown_parts += 1;
                    continue;
                }
                match object.get("value") {
                    None | Some(Value::Null) => {}
                    Some(Value::String(text)) => {
                        if !text.trim().is_empty() {
                            scan.content_texts.push(text.clone());
                        }
                    }
                    // treeData and friends carry other keys (rejected by
                    // the key set); a non-string `value` is drift.
                    Some(_) => {
                        scan.unknown_parts += 1;
                        continue;
                    }
                }
            }
            Some("markdownContent") => {
                if !keys_subset(object, MARKDOWN_CONTENT_KEYS)
                    || !bool_flag(object.get("isTrusted"))
                    || !bool_flag(object.get("supportThemeIcons"))
                {
                    scan.unknown_parts += 1;
                    continue;
                }
                match object.get("value") {
                    None | Some(Value::Null) => {}
                    Some(Value::String(text)) => {
                        if !text.trim().is_empty() {
                            scan.content_texts.push(text.clone());
                        }
                    }
                    Some(_) => {
                        scan.unknown_parts += 1;
                        continue;
                    }
                }
            }
            Some("markdownVuln") => {
                if !keys_subset(object, MARKDOWN_VULN_KEYS) {
                    scan.unknown_parts += 1;
                    continue;
                }
                match object.get("content") {
                    None | Some(Value::Null) => {}
                    Some(value) => match markdown_value(value) {
                        Some(Some(text)) => scan.content_texts.push(text),
                        Some(None) => {}
                        None => {
                            scan.unknown_parts += 1;
                            continue;
                        }
                    },
                }
                match object.get("vulns") {
                    None | Some(Value::Null) => {}
                    // The annotation text is not indexed: a non-empty
                    // vuln array stays an honest unknown.
                    Some(Value::Array(items)) if items.is_empty() => {}
                    Some(_) => {
                        scan.unknown_parts += 1;
                        continue;
                    }
                }
            }
            Some("thinking") => {
                if !keys_subset(object, THINKING_KEYS) {
                    scan.unknown_parts += 1;
                    continue;
                }
                match thinking_part_texts(object) {
                    Some(texts) => scan.thinking_texts.push(texts),
                    None => {
                        scan.unknown_parts += 1;
                        continue;
                    }
                }
            }
            Some("mcpServersStarting") => {
                if !keys_subset(object, MCP_STARTING_KEYS) {
                    scan.unknown_parts += 1;
                    continue;
                }
                match mcp_part_ids(object) {
                    Some(ids) => {
                        scan.mcp_seen = true;
                        scan.mcp_ids.extend(ids);
                    }
                    None => {
                        scan.unknown_parts += 1;
                        continue;
                    }
                }
            }
            Some("toolInvocationSerialized") => match tool_part(object) {
                Some(tool) => {
                    let Some(call_id) = tool.call_id.clone() else {
                        // Without the invocation identity the part's text
                        // cannot pair — a degradation the facts lane marks.
                        scan.identity_missing_parts += 1;
                        continue;
                    };
                    match group_index.get(&call_id) {
                        Some(&position) => {
                            let group = &mut scan.tool_groups[position];
                            group.name = group.name.clone().or(tool.tool_id);
                            group.arguments = group.arguments.clone().or(tool.arguments);
                            group.message_texts.extend(tool.message_texts);
                            if let Some(texts) = tool.result_texts {
                                group
                                    .result_texts
                                    .get_or_insert_with(Vec::new)
                                    .extend(texts);
                            }
                        }
                        None => {
                            group_index.insert(call_id.clone(), scan.tool_groups.len());
                            scan.tool_groups.push(CopilotToolGroup {
                                call_id,
                                name: tool.tool_id,
                                arguments: tool.arguments,
                                message_texts: tool.message_texts,
                                result_texts: tool.result_texts,
                            });
                        }
                    }
                }
                None => {
                    scan.unknown_parts += 1;
                }
            },
            // inlineReference, textEditGroup, codeblockUri, … and any
            // future kind: outside the verified contract.
            Some(_) => {
                scan.unknown_parts += 1;
            }
        }
    }
    scan
}

// ---------------------------------------------------------------------------
// Facts extraction
// ---------------------------------------------------------------------------

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

/// One F14 assistant row: every archived part is classified, the joined
/// markdown verified against the delivered content, thinking blocks
/// counted, and the `toolCallId` groups recorded as calls with their
/// `resultDetails` results. No connector-drop marks — the source is
/// archived verbatim.
fn record_archived_parts(
    event: &Event,
    parts: &[Value],
    coverage: &mut CoverageBuilder,
    tools: &mut ToolCollector,
    thinking: &mut i64,
) {
    let scan = scan_copilot_parts(parts);
    if scan.unknown_parts > 0 {
        coverage.mark(
            &["block_inventory", "text_inventory"],
            "copilot_part_unknown",
            "partial",
        );
    }
    for text in &scan.content_texts {
        if !event.content.contains(text.as_str()) {
            coverage.mark(
                &["block_inventory", "text_inventory"],
                "copilot_part_text_not_in_content",
                "partial",
            );
        }
    }
    for texts in &scan.thinking_texts {
        if !texts.is_empty() {
            *thinking += 1;
            coverage.evidence("thinking");
        }
    }
    if scan.mcp_seen {
        coverage.evidence("mcp");
    }
    for group in &scan.tool_groups {
        let Some(name) = group.name.clone().filter(|name| !name.is_empty()) else {
            // A nameless group cannot record its call, so its text is
            // unindexable in both lanes.
            coverage.mark(
                &["tools", "text_inventory"],
                "copilot_tool_name_missing",
                "partial",
            );
            continue;
        };
        coverage.evidence("tools");
        tools.call(RecordedCall {
            event_idx: event.idx,
            message_id: event.message_id,
            raw_call_id: group.call_id.clone(),
            call_id: group.call_id.clone(),
            name,
            arguments: group.arguments.clone(),
        });
        if let Some(result_texts) = &group.result_texts {
            let text = result_texts.join("\n");
            tools.result(RecordedResult {
                event_idx: event.idx,
                call_id: group.call_id.clone(),
                text: (!text.is_empty()).then_some(text),
                exit_code: None,
                is_error: None,
            });
        }
    }
    if scan.identity_missing_parts > 0 {
        coverage.mark(
            &["tools", "tool_exit", "tool_error", "text_inventory"],
            "copilot_tool_call_identity_missing",
            "partial",
        );
    }
}

pub(crate) fn extract(packet: &ConversationPacket) -> ProviderExtraction {
    let input = events(packet);
    let mut coverage = CoverageBuilder::new();
    // The parts array is the verbatim source: a grouped call without a
    // `resultDetails` part proves the result never existed, so an unpaired
    // call loses no session text (`unpaired_results_carry_no_text`). The
    // narrowing is only sound while no pre-F14 row feeds call evidence —
    // a seam-invocation row would reintroduce maybe-dropped result text,
    // so those sessions keep the default `tool_result_missing` scopes.
    let parts_authoritative = input.iter().all(|event| {
        !matches!(event.role, "agent" | "assistant")
            || event.extra.get("copilot_response_parts").is_some()
            || event
                .extra
                .pointer("/cass/invocations")
                .and_then(Value::as_array)
                .is_none()
    });
    let mut tools = if parts_authoritative {
        ToolCollector::new().unpaired_results_carry_no_text()
    } else {
        ToolCollector::new()
    };
    let mut thinking: i64 = 0;
    // Thinking blocks are countable exactly when the parts survive; a
    // pre-F14 row keeps the count underivable.
    let mut thinking_derived = false;
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
                match extra.get("copilot_response_parts") {
                    Some(Value::Array(parts)) => {
                        thinking_derived = true;
                        record_archived_parts(
                            event,
                            parts,
                            &mut coverage,
                            &mut tools,
                            &mut thinking,
                        );
                    }
                    // The connector writes arrays only: any other shape
                    // is drift.
                    Some(_) => coverage.mark(
                        &["block_inventory", "text_inventory"],
                        "copilot_part_unknown",
                        "partial",
                    ),
                    None => {
                        // Pre-F14 row: thinking parts and the raw
                        // response parts were dropped at the connector
                        // (explicit source comment) — their text never
                        // reaches the archive.
                        coverage.mark(
                            &["thinking"],
                            "copilot_thinking_dropped_by_connector",
                            "unavailable",
                        );
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
        // Countable exactly when the response parts survive the
        // connector; pre-F14 rows keep the count underivable.
        thinking_block_count: thinking_derived.then_some(thinking),
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
