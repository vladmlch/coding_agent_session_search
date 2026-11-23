use std::collections::HashMap;

use serde_json::{Map, Value};

use crate::model::conversation_packet::ConversationPacket;
use crate::viewer_extensions::projection::{StepLink, StructuralPublication, ToolCallLink};
use crate::viewer_extensions::providers::{
    RENDERED_EXIT_NAMES, gemini_call_result_text, gemini_status_verdict, is_compacted_extra,
    rendered_exit_code, scan_copilot_parts,
};

use super::derive_helpers::{
    attachment_paths, bounded, call_text, extra_string, invocation_arguments_text, numeric_i64,
    result_text, thinking_text, tool_records,
};
use super::{REGION_DOC_BASE, RegionRow, TEXT_SOURCE_CHAT, TEXT_SOURCE_DERIVED};

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
/// stable. Metadata regions are appended by [`super::derive_meta_regions`].
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
                // `call_id` first — the same pairing key the result arm
                // below (`tool_use_id`, `call_id`, `id`) and the
                // projection's codex arm (`projection.rs` reads
                // `payload.call_id` before `payload.id`) already use. A
                // codex `function_call` carries BOTH: `call_id` pairs it
                // with its `function_call_output`, `id` is the
                // response-item id — preferring `id` here left every
                // codex call unpaired, its output falling to the orphan
                // lane with the rendered exit status unreachable by the
                // `exit:`/`has:error` region predicates. Records without
                // `call_id` (claude `tool_use` blocks) keep the `id` they
                // always paired by.
                &["call_id", "id", "tool_use_id"],
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
        let mut exit_code = linked.iter().find_map(|r| r.exit_code);
        let mut is_error = linked.iter().find_map(|r| r.is_error);
        // Follow-up 1 (owner decision, 2026-11-11): a codex renderer
        // tool's output text IS a status record — the census-proven
        // trailer `Process exited with code N`
        // (`providers/codex.rs::RENDERED_EXIT_NAMES`) parses like an
        // explicit field, so the region row carries the same verdict the
        // facts lane extracts and strict `exit:`/`has:error` answer
        // exactly. Slug+name gated: only codex renders this trailer, and
        // an output without it keeps `exit_code` NULL (not-read).
        if codex && exit_code.is_none() && RENDERED_EXIT_NAMES.contains(&call.name.as_str()) {
            for result in &linked {
                let Some(code) = rendered_exit_code(&result.text) else {
                    continue;
                };
                exit_code = Some(code);
                if is_error.is_none() {
                    is_error = Some(code != 0);
                }
                break;
            }
        }
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
