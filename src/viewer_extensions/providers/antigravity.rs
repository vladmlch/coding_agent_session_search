//! Antigravity session facts: `agy_type` envelopes, `cass.invocations`
//! normalized calls, `TOOL_OUTPUT`/`VIEW_FILE`/`RUN_COMMAND` results and
//! recorded-only exit/error evidence. Behavioral port of the oracle
//! `facts/antigravity.py` over the normalized CASS packet, with the record
//! classification re-verified against the pinned connector's real output.
//!
//! The pinned FAD connector (rev 3851a3a) emits sparse extras — `agy_type`,
//! `thinking`, `settings_change`, `cass.invocations` (PLANNER_RESPONSE
//! calls) — and passes the source fields through into extras (F10):
//! `truncated_fields` (the source-side truncation markers), `status`
//! (the record lifecycle DONE/RUNNING/ERROR/INVALID), `error` (the failure
//! text) and `media` ({mime_type, uri} attachment refs). It also emits
//! error-only ERROR_MESSAGE records (the `error` text as the message
//! content) and text-bearing CONVERSATION_HISTORY records. Raw transcript
//! `tool_calls`/`tool_outputs`/`call_id`/`exit_code` survive only where the
//! connector recorded them. Unlinked outputs are never positionally
//! paired; missing identifiers stay partial, never fabricated.
//!
//! Record classification follows the connector's verified output contract
//! (checked message-by-message against 96,722 persisted antigravity
//! messages): every record maps to exactly one of
//!   - role `user` with extra keys ⊆ {settings_change, additional_metadata}
//!     plus the F10 passthrough keys (USER_INPUT; the wrapper blocks' text
//!     is part of the user turn),
//!   - role `agent` with extra keys ⊆ {cass, thinking} plus the F10
//!     passthrough keys (PLANNER_RESPONSE),
//!   - a role `tool`/`system` record carrying an `agy_type` envelope
//!     (MODEL steps GENERIC/VIEW_FILE/…, SYSTEM_MESSAGE, ERROR_MESSAGE,
//!     CONVERSATION_HISTORY, CHECKPOINT, EPHEMERAL_MESSAGE).
//!
//! Anything else is connector drift and stays `agy_record_unknown`.
//!
//! Text-scope coverage is per-session precise (the v2 blanket
//! `agy_connector_drops_source_text` mark is retired):
//!   - a `truncated_fields` marker proves the source cut the named fields
//!     before persisting — `agy_source_truncated` on the affected scopes,
//!     the source-absent allowlist reason, emitted only for that explicit
//!     marker, never for parse failures (a malformed marker keeps
//!     `agy_truncated_fields_malformed`);
//!   - the lifecycle `status` is exit/error evidence: ERROR forces an
//!     error verdict, DONE records success (an explicit flag — an exit
//!     code is never fabricated); status evidence that no collected call
//!     carries degrades the exit/error scopes to `agy_status_unpaired`
//!     partial, and only a packet with no status at all keeps the
//!     `agy_status_dropped_by_connector` unavailable verdict;
//!   - `media` refs are attachment evidence that carries no text: they
//!     mark nothing;
//!   - an `error` string whose content differs from the record's message
//!     text is packet-visible but unindexed (no region derivation reads
//!     extra fields): `agy_error_field_not_indexed` keeps an honest
//!     not-read gap on the text inventory.
//!
//! Residual drops the packet cannot see — the connector's empty-body
//! rules vanish whole records, so no per-session signal exists and no
//! mark is possible (documented residual class, see the ingest report):
//!   - USER_INPUT turns whose extracted request body is empty
//!     (attachment-only turns with `media`/wrapper text);
//!   - MODEL-step records with empty content, no thinking and no calls —
//!     including error-only GENERIC records (their `error` text never
//!     reaches the packet);
//!   - PLANNER_RESPONSE records dropped the same way.
//! A complete text-scope verdict therefore means "complete over every
//! record the connector emitted", never over records it dropped silently.
//!
//! Note: `is_compacted_extra` is deliberately NOT used here — the
//! `{"cass":{"invocations":[..]}}` envelope is real Antigravity evidence, and
//! ingest extra-compaction is codex-only (`should_compact_connector_extra`).

use std::collections::BTreeSet;

use serde_json::Value;

use crate::model::conversation_packet::ConversationPacket;

use super::classification::{self, ClassifyCall};
use super::{
    CORE_FIELDS, CoverageBuilder, Event, ProviderExtraction, RecordedCall, RecordedResult,
    ToolCollector, events, json_text, text_id,
};

const EXTRACTOR_REVISION: &str = "antigravity-facts-v3";
const SUPPORTED_KINDS: &[&str] = &[
    "USER_INPUT",
    "PLANNER_RESPONSE",
    "TOOL_OUTPUT",
    "VIEW_FILE",
    "RUN_COMMAND",
    "ERROR_MESSAGE",
    "error",
    "SYSTEM_MESSAGE",
    "CONVERSATION_HISTORY",
    // MODEL-step and lifecycle envelopes present in the connector's real
    // output (the oracle fixture vocabulary predates them): GENERIC
    // dominates the live transcripts; CHECKPOINT/EPHEMERAL_MESSAGE are
    // lifecycle records; CODE_ACTION/LIST_DIRECTORY are MODEL-step tool
    // kinds. The ephemeral label is EPHEMERAL_MESSAGE in the connector's
    // verified output (11 persisted records) — bare "EPHEMERAL" never
    // occurs in the packet.
    "GENERIC",
    "CHECKPOINT",
    "EPHEMERAL_MESSAGE",
    "CODE_ACTION",
    "LIST_DIRECTORY",
];
const TOOL_KINDS: &[&str] = &["TOOL_OUTPUT", "VIEW_FILE", "RUN_COMMAND"];
const PARTIAL_TOOL_FIELDS: &[&str] = &["tools", "tool_exit", "tool_error"];
/// USER_INPUT extra keys: the connector's wrapper lifts plus the F10
/// passthrough keys (`truncated_fields`, `status`, `error`, `media` —
/// FAD rev 3851a3a inserts each only when the source record carries it).
const USER_KEYS: &[&str] = &[
    "settings_change",
    "additional_metadata",
    "truncated_fields",
    "status",
    "error",
    "media",
];
/// PLANNER_RESPONSE extra keys: the connector's lifts plus the F10
/// passthrough keys.
const PLANNER_KEYS: &[&str] = &[
    "cass",
    "thinking",
    "truncated_fields",
    "status",
    "error",
    "media",
];
/// A `truncated_fields` marker that is not a clean array of strings is
/// not the verified contract shape: it can prove nothing about which text
/// existed, so it keeps this not-read scope only.
const TRUNCATION_MALFORMED_SCOPES: &[&str] = &["text_inventory"];

/// Recorded record kind: `agy_type` (canonical envelope) or raw `type`/`kind`.
fn agy_event_kind(extra: &Value) -> Option<&str> {
    let kind = extra
        .get("agy_type")
        .or_else(|| extra.get("type"))
        .or_else(|| extra.get("kind"))?
        .as_str()?;
    SUPPORTED_KINDS.contains(&kind).then_some(kind)
}

/// One classified record: the pinned connector's verified output shape, or
/// connector drift. See the module header for the contract.
enum RecordShape<'a> {
    /// USER_INPUT: user turn text in the message content; wrapper blocks
    /// lifted to `settings_change`/`additional_metadata` extras.
    User,
    /// PLANNER_RESPONSE: narration content plus `thinking`/`cass.invocations`
    /// evidence consumed by [`cass_evidence`].
    Planner,
    /// Any record carrying a recognized `agy_type` (or legacy `type`/`kind`).
    Envelope(&'a str),
    /// Outside the verified contract: future connector output the
    /// classification has not been taught. Keeps the unknown degradation.
    Drift,
}

/// Classify one packet record against the connector's verified output
/// contract (module header). Envelope evidence wins; role+extra-key-set
/// shapes cover the records the connector emits without `agy_type`.
fn classify_record<'a>(role: &str, extra: &'a Value) -> RecordShape<'a> {
    if let Some(kind) = agy_event_kind(extra) {
        return RecordShape::Envelope(kind);
    }
    let keys_known = |allowed: &[&str]| {
        extra
            .as_object()
            .is_some_and(|map| map.keys().all(|key| allowed.contains(&key.as_str())))
    };
    if role == "user" && keys_known(USER_KEYS) {
        return RecordShape::User;
    }
    if matches!(role, "agent" | "assistant") && keys_known(PLANNER_KEYS) {
        return RecordShape::Planner;
    }
    RecordShape::Drift
}

/// Explicit recorded status (oracle `_error`): an integer exit code can
/// establish an error; `status == "ERROR"` forces error; `status == "DONE"`
/// is the recorded success verdict (F10 passthrough — an explicit flag; a
/// non-final or unknown status never invents evidence, and an exit code is
/// never fabricated).
fn agy_status(value: &Value) -> (Option<i64>, Option<bool>) {
    let exit_code = value.get("exit_code").and_then(Value::as_i64);
    let mut is_error = value.get("is_error").and_then(Value::as_bool);
    match value.get("status").and_then(Value::as_str) {
        Some("ERROR") => is_error = Some(true),
        Some("DONE") if is_error.is_none() => is_error = Some(false),
        _ => {}
    }
    if let Some(code) = exit_code {
        is_error = Some(is_error == Some(true) || code != 0);
    }
    (exit_code, is_error)
}

/// The F10 `truncated_fields` passthrough marker: the source application
/// cut the named fields BEFORE persisting the record, so their text never
/// existed in any local artifact — the cut is provable source absence
/// (`agy_source_truncated`, the source-absent allowlist reason; owner
/// decision, 2026-11-08). Scope mapping by field name:
///   - `content` → chat + text_inventory (the record's message text),
///   - `thinking` → thinking + text_inventory,
///   - `tool_calls`/`tool_outputs` → tools + text_inventory + files
///     (call arguments and result text are indexed region text, and file
///     paths ride in those arguments),
///   - `media` → nothing: attachment references carry no text,
///   - any other name → text_inventory only (some field was cut; if it
///     held text, the inventory is where it would have lived).
/// A marker that is not a clean array of strings is not the verified
/// contract shape and never earns the source-absent reason: the whole
/// marker keeps `agy_truncated_fields_malformed` instead.
fn source_truncation(extra: &Value, coverage: &mut CoverageBuilder) {
    let Some(fields) = extra.get("truncated_fields") else {
        return;
    };
    let Some(items) = fields.as_array() else {
        coverage.mark(
            TRUNCATION_MALFORMED_SCOPES,
            "agy_truncated_fields_malformed",
            "partial",
        );
        return;
    };
    let mut scopes: BTreeSet<&'static str> = BTreeSet::new();
    for item in items {
        let Some(name) = item.as_str() else {
            coverage.mark(
                TRUNCATION_MALFORMED_SCOPES,
                "agy_truncated_fields_malformed",
                "partial",
            );
            return;
        };
        match name {
            "content" => {
                scopes.insert("chat");
                scopes.insert("text_inventory");
            }
            "thinking" => {
                scopes.insert("thinking");
                scopes.insert("text_inventory");
            }
            "tool_calls" | "tool_outputs" => {
                scopes.insert("tools");
                scopes.insert("text_inventory");
                scopes.insert("files");
            }
            // Attachment references carry no text.
            "media" => {}
            _ => {
                scopes.insert("text_inventory");
            }
        }
    }
    if !scopes.is_empty() {
        let scopes: Vec<&str> = scopes.into_iter().collect();
        coverage.mark(&scopes, "agy_source_truncated", "partial");
    }
}

/// The F10 `error` passthrough on a record whose message content is not
/// that same text: the failure text is packet-visible but no region
/// derivation indexes extra fields, so the text inventory honestly keeps
/// a not-read gap (`agy_error_field_not_indexed`). Error-only
/// ERROR_MESSAGE records carry the text AS their content (indexed chat
/// text), so they mark nothing.
fn error_field_gap(event: &Event, extra: &Value, coverage: &mut CoverageBuilder) {
    let Some(error) = extra.get("error").and_then(Value::as_str) else {
        return;
    };
    if !error.is_empty() && error != event.content {
        coverage.mark(
            &["text_inventory"],
            "agy_error_field_not_indexed",
            "partial",
        );
    }
}

/// A recorded call identity on an envelope record (`call_id`/`id`/`tool_use_id`).
fn has_call_identity(extra: &Value) -> bool {
    text_id(extra.get("call_id"))
        .or_else(|| text_id(extra.get("id")))
        .or_else(|| text_id(extra.get("tool_use_id")))
        .is_some()
}

/// `output`/`content` as recorded text (oracle `_text`): strings verbatim,
/// other JSON canonicalized, absent stays absent.
fn recorded_output_text(value: &Value) -> Option<String> {
    let candidate = value.get("output").or_else(|| value.get("content"))?;
    match candidate {
        Value::String(text) => Some(text.clone()),
        Value::Null => None,
        other => Some(json_text(other)),
    }
}

/// A recorded call (`tool_calls[]` item or `cass.invocations[]` entry).
fn record_call(
    event: &Event,
    value: &Value,
    coverage: &mut CoverageBuilder,
    tools: &mut ToolCollector,
) {
    let Some(name) = text_id(value.get("name")) else {
        coverage.mark(&["tools"], "agy_tool_name_missing", "partial");
        return;
    };
    coverage.evidence("tools");
    let arguments = value
        .get("args")
        .or_else(|| value.get("arguments"))
        .or_else(|| value.get("input"))
        .cloned();
    let Some(call_id) = text_id(
        value
            .get("call_id")
            .or_else(|| value.get("id"))
            .or_else(|| value.get("tool_use_id")),
    ) else {
        coverage.mark(
            PARTIAL_TOOL_FIELDS,
            "agy_tool_call_identity_missing",
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
        arguments,
    });
}

/// A recorded output (`tool_outputs[]` item or a `agy_type` tool record).
fn record_output(
    event: &Event,
    value: &Value,
    coverage: &mut CoverageBuilder,
    tools: &mut ToolCollector,
) {
    let (exit_code, is_error) = agy_status(value);
    let text = recorded_output_text(value);
    coverage.evidence("tools");
    let Some(call_id) = text_id(value.get("call_id").or_else(|| value.get("tool_use_id"))) else {
        coverage.mark(
            PARTIAL_TOOL_FIELDS,
            "agy_tool_result_identity_missing",
            "partial",
        );
        return;
    };
    tools.result(RecordedResult {
        event_idx: event.idx,
        call_id,
        text,
        exit_code,
        is_error,
    });
}

/// One verified agy event (oracle `_structured_event`). Thinking and
/// `cass.invocations` evidence are handled by [`cass_evidence`] — the FAD
/// records carrying them have no `agy_type` to verify.
fn structured_event(
    event: &Event,
    kind: &str,
    coverage: &mut CoverageBuilder,
    tools: &mut ToolCollector,
) {
    let extra = event.extra;
    // The canonical `agy_type` envelope loses the raw record structure
    // (field layout, `status`, raw `tool_calls` arrays). The record's TEXT
    // survives as the message content and lands in chat regions, so this
    // taints the block inventory only.
    if extra.get("agy_type").is_some() {
        coverage.mark(&["block_inventory"], "agy_canonical_envelope", "partial");
    }
    let content = extra
        .get("content")
        .filter(|value| !value.is_null())
        .cloned()
        .unwrap_or_else(|| Value::String(event.content.to_string()));

    if TOOL_KINDS.contains(&kind) {
        let has_output_list = extra.get("tool_outputs").is_some_and(Value::is_array);
        let has_id = text_id(extra.get("call_id"))
            .or_else(|| text_id(extra.get("id")))
            .or_else(|| text_id(extra.get("tool_use_id")))
            .is_some();
        // Oracle: `if content or _call_id(extra) is not None or
        // not isinstance(extra.get("tool_outputs"), list)`.
        let content_present =
            !content.is_null() && content.as_str().is_none_or(|text| !text.is_empty());
        if content_present || has_id || !has_output_list {
            let mut result = extra.clone();
            if let Value::Object(map) = &mut result {
                map.entry("content".to_string())
                    .or_insert_with(|| Value::String(event.content.to_string()));
            }
            record_output(event, &result, coverage, tools);
        }
    } else if !matches!(kind, "CONVERSATION_HISTORY" | "SYSTEM_MESSAGE") {
        match &content {
            Value::String(text) if !text.is_empty() => coverage.evidence("chat"),
            Value::String(_) | Value::Null => {}
            // Non-text content is explicitly described as opaque evidence.
            _ => coverage.mark(
                &[
                    "chat",
                    "tools",
                    "thinking",
                    "block_inventory",
                    "text_inventory",
                ],
                "agy_content_opaque",
                "partial",
            ),
        }
    }

    // MODEL-step envelopes outside TOOL_KINDS (GENERIC, CODE_ACTION,
    // LIST_DIRECTORY — the kinds the connector emits for executed steps):
    // the step text is the message content and lands in chat regions, but
    // without a recorded call identity the tools scope cannot verify the
    // step against a call.
    if event.role == "tool" && !TOOL_KINDS.contains(&kind) && !has_call_identity(extra) {
        coverage.mark(&["tools"], "agy_tool_result_identity_missing", "partial");
    }

    // Raw-transcript arms: `tool_calls`/`tool_outputs` lists survive only
    // where normalization recorded them verbatim.
    for field in ["tool_calls", "tool_outputs"] {
        let Some(items) = extra.get(field) else {
            continue;
        };
        let Some(items) = items.as_array() else {
            coverage.mark(&["tools"], "agy_tool_field_malformed", "partial");
            continue;
        };
        for item in items {
            if !item.is_object() {
                coverage.mark(&["tools"], "agy_tool_field_malformed", "partial");
            } else if field == "tool_calls" {
                record_call(event, item, coverage, tools);
            } else {
                record_output(event, item, coverage, tools);
            }
        }
    }

    // Artifact descriptors are recorded structure, not extracted content.
    if extra.get("artifacts").is_some_and(Value::is_array) {
        coverage.mark(
            &["artifacts"],
            "artifact_descriptors_not_extracted",
            "partial",
        );
    }
}

/// CASS-normalized evidence consumed regardless of the raw record kind:
/// the connector unwraps `thinking` onto every planner/MODEL record and
/// keeps planner invocations under `extra.cass.invocations` (FAD F3 fix —
/// `NormalizedMessage.invocations` are not part of the packet contract).
/// FAD `PLANNER_RESPONSE`/`USER_INPUT` extras carry no `agy_type`, so these
/// arms must run even when [`agy_event_kind`] cannot verify the record.
fn cass_evidence(
    event: &Event,
    extra: &Value,
    coverage: &mut CoverageBuilder,
    tools: &mut ToolCollector,
    thinking: &mut i64,
) {
    match extra.get("thinking") {
        Some(Value::String(text)) if !text.is_empty() => {
            *thinking += 1;
            coverage.evidence("thinking");
        }
        Some(Value::String(_)) | None => {}
        Some(_) => coverage.mark(
            &["thinking", "text_inventory"],
            "agy_thinking_malformed",
            "partial",
        ),
    }
    if let Some(invocations) = extra.pointer("/cass/invocations") {
        if let Some(items) = invocations.as_array() {
            for item in items {
                if item.is_object() {
                    record_call(event, item, coverage, tools);
                } else {
                    coverage.mark(&["tools"], "agy_tool_field_malformed", "partial");
                }
            }
        } else if !invocations.is_null() {
            coverage.mark(&["tools"], "agy_tool_field_malformed", "partial");
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

    let mut saw_tool_step = false;
    // Tool-step records carrying a lifecycle `status` (F10 passthrough):
    // status evidence the packet can see, counted before correlation so
    // the post-loop verdict can tell "status present but unpaired" from
    // "no status in the packet at all".
    let mut tool_status_records = 0_usize;
    for event in &input {
        deduped.push((event.idx, event.role, event.content));
        let extra = event.extra;
        if !extra.is_object() {
            coverage.unknown("agy_extra_missing");
            continue;
        }
        cass_evidence(event, extra, &mut coverage, &mut tools, &mut thinking);
        // F10 passthrough evidence is shape-independent: every emitted
        // record can carry the truncation markers and the error text.
        source_truncation(extra, &mut coverage);
        error_field_gap(event, extra, &mut coverage);
        match classify_record(event.role, extra) {
            RecordShape::Envelope(kind) => {
                if event.role == "tool" {
                    saw_tool_step = true;
                    if extra.get("status").is_some_and(|value| !value.is_null()) {
                        tool_status_records += 1;
                    }
                }
                structured_event(event, kind, &mut coverage, &mut tools);
            }
            // Known text-bearing records whose message content IS the
            // record text (USER_INPUT request / planner narration): the
            // content is indexed as the chat region; thinking/invocation
            // evidence was already consumed by `cass_evidence`.
            RecordShape::User | RecordShape::Planner => {
                if !event.content.is_empty() {
                    coverage.evidence("chat");
                }
            }
            // Connector drift (unverifiable record: real message text with
            // no verified shape — the oracle routes these to the generic
            // fallback, which degrades every scope to partial).
            RecordShape::Drift => {
                coverage.mark(CORE_FIELDS, "agy_record_unknown", "partial");
            }
        }
    }

    // The connector lifts the detected model to conversation metadata.
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
    // Source tool steps carry a lifecycle `status` on every record
    // (verified against the raw transcripts: DONE/RUNNING/ERROR/INVALID).
    // The F10 passthrough keeps it in the packet, so the exit/error
    // verdicts are precise:
    //  - status evidence exists but not every carrying record landed on a
    //    collected call/result with a final verdict (the connector records
    //    no call ids for planner invocations, so this is the dominant real
    //    shape) → `agy_status_unpaired` partial: evidence present,
    //    attribution unproven;
    //  - a packet with tool steps but no status at all (pre-F10 shape:
    //    the connector dropped the field) →
    //    `agy_status_dropped_by_connector` unavailable;
    //  - every status landed on a collected verdict → no session-level
    //    mark; the per-call marks above already said exactly which calls
    //    lack one.
    let verdict_calls = tools
        .iter()
        .filter(|tool| tool.exit_code.is_some() || tool.is_error.is_some())
        .count();
    if saw_tool_step && tool_status_records == 0 && verdict_calls == 0 {
        coverage.mark(
            &["tool_exit", "tool_error"],
            "agy_status_dropped_by_connector",
            "unavailable",
        );
    } else if tool_status_records > verdict_calls {
        coverage.mark(
            &["tool_exit", "tool_error"],
            "agy_status_unpaired",
            "partial",
        );
    }
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
        thinking_block_count: Some(thinking),
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
