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
//!     text indexes as its own searchable region (search_regions.rs,
//!     REGION_VERSION 2): tool-role MODEL steps carry it in the tools
//!     scope with the recorded error flags, every other role in chat.
//!     The retired `agy_error_field_not_indexed` mark is gone — the text
//!     is read now. Error-only records carry the string as their content
//!     and index it exactly once.
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
//!
//! A complete text-scope verdict therefore means "complete over every
//! record the connector emitted", never over records it dropped silently.
//!
//! ## F15 raw-record block verification
//!
//! FAD F15 (owner direction 2026-11-08: keep the full source record rather
//! than lossy normalization, the F12 precedent applied to the whole record)
//! archives the complete raw transcript record verbatim under
//! `extra.agy_raw` on every emitted message. When it is present this
//! extractor VERIFIES the block inventory from that record instead of
//! tainting every canonicalized record as structurally unverifiable:
//!   - raw present and every archived field accounted for → the record's
//!     block inventory stays complete (the blanket
//!     `agy_canonical_envelope` mark retires for it);
//!   - raw present but unattributable (not an object, or `type` does not
//!     match the classified record kind) → `agy_raw_unverified` partial;
//!   - raw present with a field the accounting cannot prove indexed — an
//!     unmapped field name, a text field whose packet copy diverged, raw
//!     `tool_calls` that lifted nowhere, USER_INPUT wrapper text the
//!     connector's extraction drops → `agy_raw_field_unaccounted` /
//!     `agy_user_wrapper_unindexed` partial;
//!   - raw absent (pre-F15 packets) → `agy_canonical_envelope` partial on
//!     envelope records, exactly as before: verification is additive and
//!     never loosens a packet the archive already holds.
//!
//! The accounting mirrors the connector's own mapping (census of the real
//! transcripts: exactly 14 field names — `step_index`, `source`, `type`,
//! `status`, `created_at`, `content`, `tool_calls`, `input_tokens`,
//! `cache_read_tokens`, `output_tokens`, `truncated_fields`, `thinking`,
//! `error`, `media`): record identity/lifecycle/tokens are structural
//! provenance that never carries text; `content`/`thinking`/`error` must
//! equal the packet text they became; `truncated_fields` is the allowlisted
//! source-side truncation marker; `media` refs carry no text; `tool_calls`
//! must have lifted into `extra.cass.invocations` with arguments intact,
//! and never on a role `tool` record (the region lane excludes role-tool
//! args). Wrapper extraction is PER TAG — the connector's own
//! `extract_tagged_blocks` scans each tag independently, so a block whose
//! open tag sits inside another block's inner text is still its tag's
//! first block, and the accounting mirrors that scan exactly. Wrapper
//! markup (`<USER_REQUEST>`/`<USER_SETTINGS_CHANGE>`/
//! `<ADDITIONAL_METADATA>`, open or close, matched or not) is structural
//! vocabulary, same as the connector's own body extraction — but text
//! OUTSIDE the connector's keep set (the joined request blocks plus the
//! first settings/metadata block of each kind) that is not markup —
//! loose residual text, a second settings/metadata block's inner text, an
//! unterminated wrapper's remainder — is dropped by the connector's
//! extraction and stays unaccounted.
//!
//! Note: `is_compacted_extra` is deliberately NOT used here — the
//! `{"cass":{"invocations":[..]}}` envelope is real Antigravity evidence, and
//! the ingest extra-compaction that produced skeleton extras was codex-only
//! (`should_compact_connector_extra`, removed 2026-11-08).

use std::collections::BTreeSet;

use serde_json::Value;

use crate::model::conversation_packet::ConversationPacket;

use super::classification::{self, ClassifyCall};
use super::{
    CORE_FIELDS, CoverageBuilder, Event, ProviderExtraction, RecordedCall, RecordedResult,
    ToolCollector, events, json_text, text_id,
};

// v5 (2026-11-08): FAD F15 archives the full raw source record under
// `extra.agy_raw`; the extractor VERIFIES the block inventory from it
// (agy_raw_unverified / agy_raw_field_unaccounted /
// agy_user_wrapper_unindexed) instead of tainting every `agy_type`
// envelope record with `agy_canonical_envelope`. Pre-F15 packets keep the
// old verdict.
// v6 (2026-11-08): the USER_INPUT wrapper accounting scans blocks PER
// TAG, mirroring the connector's `extract_tagged_blocks` exactly — a
// block whose open tag sits inside another block's inner text is still
// its tag's first block. The global earliest-open scan this replaces
// derived a different first-block text for the real archive's
// overlapping-wrapper record and marked it `agy_user_wrapper_unindexed`
// although the connector lifted every byte (an over-strict false
// partial, never an under-strict miss: text the connector actually
// drops keeps the partial).
const EXTRACTOR_REVISION: &str = "antigravity-facts-v6";
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
/// USER_INPUT extra keys: the connector's wrapper lifts, the F10
/// passthrough keys (`truncated_fields`, `status`, `error`, `media` —
/// FAD rev 3851a3a inserts each only when the source record carries it)
/// and the F15 archived raw record (`agy_raw`).
const USER_KEYS: &[&str] = &[
    "settings_change",
    "additional_metadata",
    "truncated_fields",
    "status",
    "error",
    "media",
    "agy_raw",
];
/// PLANNER_RESPONSE extra keys: the connector's lifts, the F10
/// passthrough keys and the F15 archived raw record (`agy_raw`).
const PLANNER_KEYS: &[&str] = &[
    "cass",
    "thinking",
    "truncated_fields",
    "status",
    "error",
    "media",
    "agy_raw",
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
///
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

/// Raw-record fields that never index as text: record identity
/// (`type`/`source`/`step_index`), lifecycle (`status`/`created_at`), token
/// counts and recorded exit/error codes. They ride in `extra.agy_raw` as
/// provenance; the census of the real transcripts shows these plus exactly
/// the accounted fields below (14 names in the archive).
const RAW_STRUCTURAL_FIELDS: &[&str] = &[
    "type",
    "source",
    "status",
    "created_at",
    "step_index",
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "exit_code",
    "error_code",
];

/// F15 raw-record verification: prove every field of the archived source
/// record (`extra.agy_raw`, FAD's verbatim clone of the transcript record)
/// either indexes somewhere in the packet or provably carries no text. The
/// first field that cannot be accounted taints the block inventory — the
/// gate stays strict; a fully verified record retires the blanket
/// `agy_canonical_envelope` taint for itself.
fn verify_raw_blocks(
    event: &Event,
    raw: &Value,
    extra: &Value,
    expected_type: &str,
    coverage: &mut CoverageBuilder,
) {
    let Some(record) = raw.as_object() else {
        // The archived record must be the transcript object itself.
        coverage.mark(&["block_inventory"], "agy_raw_unverified", "partial");
        return;
    };
    if raw.get("type").and_then(Value::as_str) != Some(expected_type) {
        // The archived record does not attribute to the classified record
        // kind — never verify on a guess.
        coverage.mark(&["block_inventory"], "agy_raw_unverified", "partial");
        return;
    }
    let is_user_input = expected_type == "USER_INPUT";
    // Deterministic field order regardless of the map's iteration order.
    let mut keys: Vec<&str> = record.keys().map(String::as_str).collect();
    keys.sort_unstable();
    for key in keys {
        let Some(value) = record.get(key) else {
            continue;
        };
        let accounted = if RAW_STRUCTURAL_FIELDS.contains(&key) {
            true
        } else {
            match key {
                // The allowlisted source-side truncation marker: the text
                // scopes carry `agy_source_truncated` for it; the marker
                // list itself is metadata.
                "truncated_fields" => true,
                // Attachment refs ({mime_type, uri}): evidence with no text.
                "media" => true,
                "content" => {
                    if is_user_input {
                        user_content_accounted(value, event, extra)
                    } else {
                        record_content_accounted(value, event)
                    }
                }
                "thinking" | "error" => raw_text_field_accounted(value, extra.get(key)),
                "tool_calls" => raw_tool_calls_accounted(event, value, extra),
                // Any field outside the verified contract might carry
                // text; unknown names stay unaccounted (never a silent
                // pass).
                _ => false,
            }
        };
        if !accounted {
            let reason = if is_user_input && key == "content" {
                "agy_user_wrapper_unindexed"
            } else {
                "agy_raw_field_unaccounted"
            };
            coverage.mark(&["block_inventory"], reason, "partial");
            return;
        }
    }
}

/// Record `content` accounting outside USER_INPUT: the connector copies
/// the string byte-for-byte into the message content (every arm except
/// the USER_INPUT wrapper extraction and the empty-content error
/// fallbacks, whose raw `content` is empty). Absent/null or blank content
/// carries no text; a non-string value never reached the packet.
fn record_content_accounted(value: &Value, event: &Event) -> bool {
    let Value::String(text) = value else {
        return value.is_null();
    };
    text.trim().is_empty() || event.content == text.as_str()
}

/// `thinking`/`error` accounting: the connector lifts `thinking` trimmed
/// (a blank lift drops the key) and passes `error` through verbatim (F10);
/// either way the packet copy under the same extra key must equal the raw
/// text. Absent/null or blank text carries nothing.
fn raw_text_field_accounted(value: &Value, packet_copy: Option<&Value>) -> bool {
    let Value::String(text) = value else {
        return value.is_null();
    };
    if text.trim().is_empty() {
        return true;
    }
    match packet_copy {
        Some(Value::String(copy)) => copy.as_str() == text || copy.as_str() == text.trim(),
        _ => false,
    }
}

/// One closed `<TAG>…</TAG>` wrapper span in a raw USER_INPUT record.
struct RawWrapperSpan {
    tag: &'static str,
    start: usize,
    end: usize,
    inner: String,
}

/// The wrapper vocabulary of raw USER_INPUT records — fixed structural
/// markup, the same tags the connector's own body extraction strips.
const RAW_USER_WRAPPER_TAGS: &[&str] = &[
    "USER_REQUEST",
    "USER_SETTINGS_CHANGE",
    "ADDITIONAL_METADATA",
];

/// Scan the closed wrapper spans of ONE tag, earliest open first — a
/// direct mirror of the connector's `extract_tagged_blocks`, which scans
/// each tag independently across the whole content. Overlapping shapes
/// occur in the real transcripts (a block's open tag inside another
/// block's inner text), so a global earliest-open scan would derive the
/// wrong first block for the inner tag. An unterminated wrapper of this
/// tag stops ITS scan: the remainder falls to the residual check.
fn raw_wrapper_spans_tag(content: &str, tag: &'static str) -> Vec<RawWrapperSpan> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut spans = Vec::new();
    let mut from = 0;
    while let Some(offset) = content[from..].find(&open) {
        let at = from + offset;
        let after = &content[at + open.len()..];
        let Some(rel) = after.find(&close) else {
            break;
        };
        let end = at + open.len() + rel + close.len();
        spans.push(RawWrapperSpan {
            tag,
            start: at,
            end,
            inner: after[..rel].trim().to_string(),
        });
        from = end;
    }
    spans
}

/// All closed wrapper spans, earliest start first across tags. Same-tag
/// spans are disjoint and ordered, so the sort keeps each tag's
/// first-block position queryable — `wrapper_lift_accounted` and
/// `wrapper_residual_blank` both key on "the FIRST block of a kind".
fn raw_wrapper_spans(content: &str) -> Vec<RawWrapperSpan> {
    let mut spans = Vec::new();
    for tag in RAW_USER_WRAPPER_TAGS {
        spans.extend(raw_wrapper_spans_tag(content, tag));
    }
    spans.sort_by_key(|span| span.start);
    spans
}

/// USER_INPUT `content` accounting — a mirror of the connector's
/// `extract_user_request`: with no closed `<USER_REQUEST>` block the whole
/// record text is the body (everything indexes in chat); otherwise the
/// body is the joined request blocks, the FIRST settings/metadata block
/// of each kind lifts to extras, and any other non-markup text — loose
/// residual text, further settings/metadata blocks, an unterminated
/// wrapper's remainder — never reaches the packet.
fn user_content_accounted(value: &Value, event: &Event, extra: &Value) -> bool {
    let Some(text) = value.as_str() else {
        return value.is_null();
    };
    let spans = raw_wrapper_spans(text);
    let requests: Vec<&str> = spans
        .iter()
        .filter(|span| span.tag == "USER_REQUEST")
        .map(|span| span.inner.as_str())
        .collect();
    let body_matches = if requests.is_empty() {
        event.content == text.trim()
    } else {
        // Mirror of the connector's `requests.join("\n\n")`.
        event.content == requests.join("\n\n")
    };
    if !body_matches {
        return false;
    }
    if requests.is_empty() {
        // Whole-record fallback body: everything indexes in chat.
        return true;
    }
    // Residual: text outside the connector's keep set that is not
    // wrapper markup is dropped text. Byte classification, not a
    // sequential cut — per-tag spans overlap on the real shapes, so
    // "between spans" is not well-defined.
    if !wrapper_residual_blank(text, &spans) {
        return false;
    }
    wrapper_lift_accounted("USER_SETTINGS_CHANGE", &spans, extra.get("settings_change"))
        && wrapper_lift_accounted(
            "ADDITIONAL_METADATA",
            &spans,
            extra.get("additional_metadata"),
        )
}

/// The connector's USER_INPUT keep set is the joined request blocks plus
/// the FIRST settings/metadata block of each kind; everything else in
/// the record is wrapper markup (structural vocabulary, never indexed
/// text) or text the connector's extraction drops. Byte classification:
/// kept spans account their whole extent, every wrapper tag occurrence —
/// open or close, matched or not, even one sitting inside another block's
/// inner text — is markup, and what is left marked is exactly the text
/// the connector never lifted. Blank means no text dropped.
fn wrapper_residual_blank(text: &str, spans: &[RawWrapperSpan]) -> bool {
    let mut dropped = vec![true; text.len()];
    for span in spans {
        let first_of_tag = spans
            .iter()
            .find(|other| other.tag == span.tag)
            .is_some_and(|first| first.start == span.start);
        // Every request block and the first block of each lift tag are
        // kept — their markup is structural, their inner text lifted.
        if span.tag == "USER_REQUEST" || first_of_tag {
            dropped[span.start..span.end].fill(false);
        }
    }
    for tag in RAW_USER_WRAPPER_TAGS {
        for needle in [format!("<{tag}>"), format!("</{tag}>")] {
            let mut from = 0;
            while let Some(offset) = text[from..].find(needle.as_str()) {
                let at = from + offset;
                let to = at + needle.len();
                dropped[at..to].fill(false);
                from = to;
            }
        }
    }
    let residual: Vec<u8> = dropped
        .iter()
        .zip(text.as_bytes())
        .filter(|(dropped, _)| **dropped)
        .map(|(_, byte)| *byte)
        .collect();
    String::from_utf8_lossy(&residual).trim().is_empty()
}

/// The connector lifts only the FIRST wrapper block of each kind; the
/// packet copy must equal it, and any further block must be blank (its
/// text would otherwise drop).
fn wrapper_lift_accounted(
    tag: &str,
    spans: &[RawWrapperSpan],
    packet_copy: Option<&Value>,
) -> bool {
    let blocks: Vec<&str> = spans
        .iter()
        .filter(|span| span.tag == tag)
        .map(|span| span.inner.as_str())
        .collect();
    let Some((first, rest)) = blocks.split_first() else {
        // No block of this kind: nothing lifts (a packet copy without a
        // source block is extra evidence, not a loss).
        return true;
    };
    rest.iter().all(|block| block.trim().is_empty())
        && (first.trim().is_empty()
            || matches!(packet_copy, Some(Value::String(copy)) if copy.as_str() == *first))
}

/// Raw `tool_calls` accounting: every call the archived record held must
/// have lifted into `extra.cass.invocations` (the `NormalizedInvocation`
/// serde shape both FAD's planner dual-write and the ingest merge
/// produce) with its arguments intact — and the record must not be role
/// `tool`, whose args the region lane never indexes. A nameless item
/// lifts nowhere; it is accounted only when it carries no argument text
/// either (a mirror of the connector's nameless-call drop).
fn raw_tool_calls_accounted(event: &Event, raw: &Value, extra: &Value) -> bool {
    let Some(items) = raw.as_array() else {
        return raw.is_null();
    };
    if event.role == "tool" {
        // Role-tool args never index: search_regions.rs gates the
        // `cass.invocations` args lane on role != tool.
        return false;
    }
    // Lifted calls: [{kind, name, arguments?}] — `arguments` serializes
    // only when present (an explicit `null` stays explicit).
    let mut lifted: Vec<(String, Option<Value>)> = extra
        .pointer("/cass/invocations")
        .and_then(Value::as_array)
        .map(|invocations| {
            invocations
                .iter()
                .map(|inv| {
                    (
                        inv.get("name")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_string(),
                        inv.get("arguments").cloned(),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    for item in items {
        let Some(call) = item.as_object() else {
            return false;
        };
        let name = call
            .get("name")
            .or_else(|| call.get("tool_name"))
            .or_else(|| item.pointer("/function/name"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|name| !name.is_empty());
        // The planner lift takes the first present arguments spelling, in
        // this order, cloned verbatim (planner_calls.rs).
        let args = call
            .get("args")
            .or_else(|| call.get("arguments"))
            .or_else(|| call.get("input"))
            .or_else(|| item.pointer("/function/arguments"))
            .cloned();
        match name {
            Some(name) => {
                let found = lifted.iter().position(|(lifted_name, lifted_args)| {
                    lifted_name.as_str() == name && args_equivalent(&args, lifted_args)
                });
                match found {
                    Some(at) => {
                        lifted.remove(at);
                    }
                    None => return false,
                }
            }
            None => {
                // Nameless calls drop whole; accounted only when no
                // argument text drops with them.
                if args
                    .as_ref()
                    .is_some_and(|value| !args_textually_empty(value))
                {
                    return false;
                }
            }
        }
    }
    true
}

/// Raw vs lifted arguments equivalence: absent ≡ explicit `null`; values
/// compared canonically (sorted keys) so object order cannot fake a match.
fn args_equivalent(raw: &Option<Value>, lifted: &Option<Value>) -> bool {
    let normalize = |value: &Option<Value>| {
        value
            .as_ref()
            .filter(|value| !value.is_null())
            .map(json_text)
    };
    normalize(raw) == normalize(lifted)
}

/// True when the value holds no text (null, blank string, empty
/// object/array — numbers and booleans carry none either).
fn args_textually_empty(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::String(text) => text.trim().is_empty(),
        Value::Array(items) => items.is_empty(),
        Value::Object(map) => map.is_empty(),
        Value::Number(_) | Value::Bool(_) => true,
    }
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
    // F15 raw-record verification: with the archived source record the
    // block inventory is proven from it; the pre-F15 canonical-envelope
    // taint applies only when the raw record is absent.
    match extra.get("agy_raw") {
        Some(raw) => verify_raw_blocks(event, raw, extra, kind, coverage),
        None => {
            // The canonical `agy_type` envelope loses the raw record
            // structure (field layout, `status`, raw `tool_calls` arrays).
            // The record's TEXT survives as the message content and lands
            // in chat regions, so this taints the block inventory only.
            if extra.get("agy_type").is_some() {
                coverage.mark(&["block_inventory"], "agy_canonical_envelope", "partial");
            }
        }
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
                // F15: the key-set shapes the connector emits without an
                // `agy_type` envelope verify from the archived raw record
                // the same way. Without it these records carry no block
                // taint (unchanged pre-F15 behavior — verification is
                // additive, never a new blanket mark).
                if let Some(raw) = extra.get("agy_raw") {
                    let expected = if event.role == "user" {
                        "USER_INPUT"
                    } else {
                        "PLANNER_RESPONSE"
                    };
                    verify_raw_blocks(event, raw, extra, expected, &mut coverage);
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
