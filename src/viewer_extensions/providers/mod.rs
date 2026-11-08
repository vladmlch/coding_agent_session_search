//! Provider-specific session-fact extractors plugged into the ingest pass.
//!
//! Each provider module consumes the normalized packet's `extra_json`
//! evidence (raw source records the connector preserved verbatim) and emits
//! a [`ProviderExtraction`]. New providers land by adding a module and one
//! dispatch arm below — junie/qoder are stubs returning
//! `extractor_unavailable` until their wave ships.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::model::conversation_packet::ConversationPacket;

use super::facts::{CoverageEntry, FACTS_EXTRACTOR_VERSION, FactPublication, ToolCallFact};

mod antigravity;
mod claude;
mod codex;
mod cursor;
mod devin;
mod gemini;
mod pi_agent;

/// Gemini tool-result text + status verdict derivation, shared with the
/// search-regions gemini arm so the facts and region lanes stay identical.
pub(crate) use gemini::{gemini_call_result_text, gemini_status_verdict};

pub(crate) mod classification;

/// Bound on decoded text/JSON payloads, matching the oracle's MAX_FACT_TEXT.
const MAX_FACT_TEXT: usize = 1_000_000;

/// Coverage fields every provider reports on (oracle `CORE_FIELDS`).
const CORE_FIELDS: &[&str] = &[
    "chat",
    "tools",
    "thinking",
    "tool_exit",
    "tool_error",
    "block_inventory",
    "text_inventory",
    "files",
    "mcp",
];

fn severity(state: &str) -> u8 {
    match state {
        "complete" => 0,
        "partial" => 1,
        "unavailable" => 2,
        "pending" => 3,
        _ => 4,
    }
}

/// Accumulates per-scope coverage marks; only raises severity.
pub(crate) struct CoverageBuilder {
    states: BTreeMap<String, (String, BTreeSet<String>)>,
    /// Scopes with at least one recorded piece of evidence; an `unavailable`
    /// field that still produced data degrades to `partial`, mirroring the
    /// oracle's `finish()`.
    known: BTreeSet<String>,
}

impl CoverageBuilder {
    pub fn new() -> Self {
        Self {
            states: CORE_FIELDS
                .iter()
                .map(|field| (field.to_string(), ("complete".to_string(), BTreeSet::new())))
                .collect(),
            known: BTreeSet::new(),
        }
    }

    /// Every field reports `unavailable` with the same reason (stub
    /// extractors, empty unverified sources).
    pub fn all_unavailable(&mut self, reason: &str) {
        for field in CORE_FIELDS {
            self.mark(&[field], reason, "unavailable");
        }
    }

    pub fn mark(&mut self, fields: &[&str], reason: &str, state: &str) {
        for field in fields {
            let entry = self
                .states
                .entry((*field).to_string())
                .or_insert_with(|| ("complete".to_string(), BTreeSet::new()));
            if severity(state) > severity(&entry.0) {
                entry.0 = state.to_string();
            }
            entry.1.insert(reason.to_string());
        }
    }

    /// The whole event's evidence was unusable: every core field is unknown.
    pub fn unknown(&mut self, reason: &str) {
        self.mark(CORE_FIELDS, reason, "unavailable");
    }

    /// The record itself was compacted by normalization (`{"cass":{…}}`
    /// envelope): evidence is partial, not absent.
    pub fn mark_compacted(&mut self, reason: &str) {
        self.mark(CORE_FIELDS, reason, "partial");
    }

    pub fn evidence(&mut self, scope: &str) {
        self.known.insert(scope.to_string());
    }

    /// `true` when `reason` was already recorded on any scope — used by
    /// extractors that must not downgrade an explicit conflict.
    #[allow(dead_code)]
    pub fn has_reason(&self, reason: &str) -> bool {
        self.states
            .values()
            .any(|(_, reasons)| reasons.contains(reason))
    }

    /// Fold marks into the persisted manifest, adding the fixed entries the
    /// oracle always reports (overlay-bound scopes, relation, metrics).
    fn finish(
        mut self,
        tools: &[ToolCallFact],
        file_facts: usize,
        relation: (Option<bool>, Option<&str>),
    ) -> Vec<CoverageEntry> {
        // Overlay-bound scopes are never produced by the extractor itself.
        for field in ["title", "artifacts", "subagents"] {
            self.states
                .entry(field.to_string())
                .or_insert_with(|| ("unavailable".to_string(), BTreeSet::new()))
                .1
                .insert("overlay_not_supplied".to_string());
            if let Some(entry) = self.states.get_mut(field) {
                entry.0 = "unavailable".to_string();
            }
        }
        let (is_subagent, parent_external_id) = relation;
        let relation_known = parent_external_id.is_some() || is_subagent == Some(false);
        let relation_state = if relation_known {
            "complete"
        } else if is_subagent == Some(true) {
            "partial"
        } else {
            "unavailable"
        };
        self.states.insert(
            "parent_relation".to_string(),
            (
                relation_state.to_string(),
                if relation_known {
                    BTreeSet::new()
                } else {
                    BTreeSet::from(["relationship_not_supplied".to_string()])
                },
            ),
        );
        self.states.insert(
            "is_subagent".to_string(),
            (
                if is_subagent.is_some() {
                    "complete".to_string()
                } else {
                    "unavailable".to_string()
                },
                if is_subagent.is_some() {
                    BTreeSet::new()
                } else {
                    BTreeSet::from(["relationship_not_supplied".to_string()])
                },
            ),
        );
        // Turn/category metrics are derived, but the full native metric set
        // (TTFE, one-shot rate, retries) is not ported yet.
        self.states.insert(
            "metrics".to_string(),
            (
                "partial".to_string(),
                BTreeSet::from(["native_metrics_partial".to_string()]),
            ),
        );

        let mut coverage = Vec::with_capacity(self.states.len());
        for (field, (mut state, reasons)) in self.states {
            let known = match field.as_str() {
                "tool_exit" => tools.iter().any(|tool| tool.exit_code.is_some()),
                "tool_error" => tools.iter().any(|tool| tool.is_error.is_some()),
                "files" => file_facts > 0,
                _ => self.known.contains(&field),
            };
            if state == "unavailable" && known {
                state = "partial".to_string();
            }
            coverage.push(CoverageEntry {
                field,
                state,
                reasons: reasons.into_iter().collect(),
            });
        }
        coverage
    }
}

/// One normalized packet message as a fact event; `key` binds to the
/// canonical message identity (`m<id>`) or falls back to `idx`.
pub(crate) struct Event<'a> {
    pub key: String,
    pub idx: i64,
    pub role: &'a str,
    pub content: &'a str,
    pub extra: &'a Value,
    pub message_id: Option<i64>,
    /// Recorded message timestamp (ms) — the TTFE turn/edit clock input.
    pub created_at: Option<i64>,
}

pub(crate) fn events(packet: &ConversationPacket) -> Vec<Event<'_>> {
    packet
        .payload
        .messages
        .iter()
        .map(|message| Event {
            key: message
                .message_id
                .map(|id| format!("m{id}"))
                .unwrap_or_else(|| format!("i{}", message.idx)),
            idx: message.idx,
            role: &message.role,
            content: &message.content,
            extra: &message.extra_json,
            message_id: message.message_id,
            created_at: message.created_at,
        })
        .collect()
}

/// Canonical JSON (sorted keys, no whitespace) for dedup signatures and
/// segment digests — identical bytes for semantically identical records.
pub(crate) fn json_text(value: &Value) -> String {
    let mut out = String::new();
    write_json_text(&mut out, value);
    out
}

fn write_json_text(out: &mut String, value: &Value) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(flag) => out.push_str(if *flag { "true" } else { "false" }),
        Value::Number(number) => out.push_str(&number.to_string()),
        Value::String(text) => {
            out.push_str(&serde_json::to_string(text).unwrap_or_else(|_| "null".to_string()));
        }
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_json_text(out, item);
            }
            out.push(']');
        }
        Value::Object(map) => {
            out.push('{');
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            for (index, key) in keys.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::to_string(key).unwrap_or_else(|_| "null".to_string()));
                out.push(':');
                write_json_text(out, &map[*key]);
            }
            out.push('}');
        }
    }
}

/// A non-empty string identifier, or None (never a coerced value).
pub(crate) fn text_id(value: Option<&Value>) -> Option<String> {
    value
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
        .map(str::to_string)
}

/// Explicit recorded status: an integer exit code can establish an error;
/// a generic error flag never invents `exit=1` (oracle `explicit_status`).
pub(crate) fn explicit_status(payload: &Value) -> (Option<i64>, Option<bool>) {
    let exit_code = payload
        .get("exit_code")
        .or_else(|| payload.get("exitCode"))
        .and_then(Value::as_i64);
    let mut is_error = payload
        .get("is_error")
        .or_else(|| payload.get("isError"))
        .and_then(Value::as_bool);
    if let Some(code) = exit_code {
        is_error = Some(code != 0 || is_error == Some(true));
    }
    (exit_code, is_error)
}

/// Read explicit textual content, retaining an unknown-part signal
/// (oracle `recorded_text`): `(text, complete)`.
pub(crate) fn recorded_text(value: &Value) -> (Option<String>, bool) {
    match value {
        Value::String(text) => (Some(text.clone()), true),
        Value::Array(parts) => {
            let mut texts = Vec::new();
            let mut complete = true;
            for part in parts {
                if let Some(text) = part.as_str() {
                    texts.push(text.to_string());
                } else if part.is_object() {
                    let kind = part.get("type").and_then(Value::as_str);
                    if matches!(
                        kind,
                        Some("text" | "input_text" | "output_text" | "summary_text")
                    ) && let Some(text) = part.get("text").and_then(Value::as_str)
                    {
                        texts.push(text.to_string());
                        continue;
                    }
                    if !matches!(kind, Some("image" | "input_image")) {
                        complete = false;
                    }
                } else {
                    complete = false;
                }
            }
            (Some(texts.join("\n")), complete)
        }
        _ => (None, false),
    }
}

/// Parse a bounded JSON string once; malformed or oversized strings stay
/// literal evidence `(None, false)` (oracle `_decode`).
pub(crate) fn decode_json_str(text: &str) -> Option<Value> {
    if text.len() > MAX_FACT_TEXT {
        return None;
    }
    serde_json::from_str(text).ok()
}

/// `true` when normalization replaced the raw record with the
/// `{"cass": {…}}` skeleton (large-session extra compaction).
pub(crate) fn is_compacted_extra(value: &Value) -> bool {
    value
        .as_object()
        .is_some_and(|map| map.len() == 1 && map.contains_key("cass"))
}

/// A recorded call before correlation.
pub(crate) struct RecordedCall {
    pub event_idx: i64,
    pub message_id: Option<i64>,
    pub call_id: String,
    pub raw_call_id: String,
    pub name: String,
    pub arguments: Option<Value>,
}

/// A recorded result before correlation.
pub(crate) struct RecordedResult {
    #[allow(dead_code)] // Source anchor retained for the Task-8 links table.
    pub event_idx: i64,
    pub call_id: String,
    pub text: Option<String>,
    pub exit_code: Option<i64>,
    pub is_error: Option<bool>,
}

/// Target path evidence from call arguments (oracle `_path`).
fn argument_path(arguments: Option<&Value>) -> Option<String> {
    let arguments = arguments?.as_object()?;
    for field in ["file_path", "path", "TargetFile", "file", "filename"] {
        if let Some(value) = arguments.get(field).and_then(Value::as_str)
            && !value.is_empty()
        {
            return Some(value.to_string());
        }
    }
    None
}

/// Strict recorded call/result correlation shared by providers (oracle
/// `ToolCollector`): same-ID calls with distinct signatures are ambiguous
/// and never merged; results without calls are orphans.
pub(crate) struct ToolCollector {
    pub calls: BTreeMap<String, Vec<RecordedCall>>,
    pub results: BTreeMap<String, Vec<RecordedResult>>,
    /// Scopes an unpaired call marks `tool_result_missing` partial. The
    /// default is text-bearing loss (the result record is missing from
    /// storage); claude_code narrows it to `tools` only.
    result_missing_scopes: &'static [&'static str],
}

impl ToolCollector {
    pub fn new() -> Self {
        Self {
            calls: BTreeMap::new(),
            results: BTreeMap::new(),
            result_missing_scopes: &["tools", "text_inventory"],
        }
    }

    /// claude_code only: its connector drops `tool_result` content with no
    /// `text` part (ToolSearch `tool_reference` results, Read `image`
    /// results), so an unpaired call means the result carried no text —
    /// not that session text was lost. `tool_result_missing` then keeps
    /// `tools` partial but leaves the text inventory complete.
    pub fn unpaired_results_carry_no_text(mut self) -> Self {
        self.result_missing_scopes = &["tools"];
        self
    }

    pub fn call(&mut self, call: RecordedCall) {
        let signature = format!(
            "[{},{}]",
            json_text(&Value::String(call.name.clone())),
            json_text(&call.arguments.clone().unwrap_or(Value::Null))
        );
        let candidates = self.calls.entry(call.call_id.clone()).or_default();
        if !candidates.iter().any(|item| {
            format!(
                "[{},{}]",
                json_text(&Value::String(item.name.clone())),
                json_text(&item.arguments.clone().unwrap_or(Value::Null))
            ) == signature
        }) {
            candidates.push(call);
        }
    }

    pub fn result(&mut self, result: RecordedResult) {
        let signature = json_text(&Value::Array(vec![
            result
                .text
                .clone()
                .map(Value::String)
                .unwrap_or(Value::Null),
            result.exit_code.map(Value::from).unwrap_or(Value::Null),
            result.is_error.map(Value::Bool).unwrap_or(Value::Null),
        ]));
        let candidates = self.results.entry(result.call_id.clone()).or_default();
        if !candidates.iter().any(|item| {
            json_text(&Value::Array(vec![
                item.text.clone().map(Value::String).unwrap_or(Value::Null),
                item.exit_code.map(Value::from).unwrap_or(Value::Null),
                item.is_error.map(Value::Bool).unwrap_or(Value::Null),
            ])) == signature
        }) {
            candidates.push(result);
        }
    }

    fn ambiguous(&self, call_id: &str, coverage: &mut CoverageBuilder) {
        let _ = call_id;
        coverage.mark(
            &[
                "tools",
                "tool_exit",
                "tool_error",
                "files",
                "mcp",
                "block_inventory",
                "text_inventory",
            ],
            "conflicting_event",
            "unavailable",
        );
    }

    /// Correlate calls/results into verified links plus a file-fact count.
    pub fn finish(self, coverage: &mut CoverageBuilder) -> (Vec<ToolCallFact>, usize) {
        let mut tools = Vec::new();
        let mut file_facts = 0_usize;
        let keys: BTreeSet<&String> = self.calls.keys().chain(self.results.keys()).collect();
        for call_id in keys {
            let calls = self.calls.get(call_id).map(Vec::as_slice).unwrap_or(&[]);
            let results = self.results.get(call_id).map(Vec::as_slice).unwrap_or(&[]);
            if calls.len() > 1 {
                self.ambiguous(call_id, coverage);
                continue;
            }
            if calls.is_empty() {
                coverage.mark(
                    &["tools", "tool_exit", "tool_error"],
                    "orphan_tool_result",
                    "partial",
                );
                if results.len() > 1 {
                    self.ambiguous(call_id, coverage);
                }
                continue;
            }
            let call = &calls[0];
            if results.len() > 1 {
                self.ambiguous(call_id, coverage);
            }
            let result = if results.len() == 1 {
                Some(&results[0])
            } else {
                None
            };
            if results.is_empty() {
                coverage.mark(self.result_missing_scopes, "tool_result_missing", "partial");
            }
            let exit_code = result.and_then(|r| r.exit_code);
            let is_error = result.and_then(|r| r.is_error);
            if exit_code.is_none() {
                coverage.mark(&["tool_exit"], "tool_exit_not_recorded", "unavailable");
            }
            if is_error.is_none() {
                coverage.mark(&["tool_error"], "tool_error_not_recorded", "unavailable");
            }
            let path = argument_path(call.arguments.as_ref());
            if path.is_some() {
                file_facts += 1;
            }
            coverage.evidence("tools");
            tools.push(ToolCallFact {
                call_id: call.call_id.clone(),
                raw_call_id: call.raw_call_id.clone(),
                name: call.name.clone(),
                has_result: result.is_some(),
                exit_code,
                is_error,
                file_path: path,
                source_idx: call.event_idx,
                source_message_id: call.message_id,
            });
        }
        (tools, file_facts)
    }
}

/// What a provider extractor produced; `facts::extract_session_facts` folds
/// it into the persisted [`FactPublication`].
pub(crate) struct ProviderExtraction {
    pub extractor: &'static str,
    pub coverage: CoverageBuilder,
    pub tools: Vec<ToolCallFact>,
    pub file_fact_count: Option<i64>,
    pub thinking_block_count: Option<i64>,
    /// `Some(n)` when boundary records were observed; `None` when the
    /// provider's normalization drops them (coverage records the reason).
    pub compaction_count: Option<i64>,
    pub model_set: Vec<String>,
    pub is_subagent: Option<bool>,
    pub parent_external_id: Option<String>,
    pub turn_count: Option<i64>,
    pub step_count: Option<i64>,
    pub first_prompt: Option<String>,
    pub task_category: Option<String>,
    /// `SessionMetricsCalculator` metric bundle (Task 10); `None` only for
    /// `extractor_unavailable` — a ran extractor always produces the bundle
    /// (individual fields stay `None` where the oracle leaves them
    /// unmeasured).
    pub quality: Option<classification::SessionQualityMetrics>,
}

impl ProviderExtraction {
    fn unavailable(provider: &str) -> Self {
        let mut coverage = CoverageBuilder::new();
        coverage.all_unavailable("extractor_unavailable");
        let _ = provider;
        Self {
            extractor: "extractor_unavailable",
            coverage,
            tools: Vec::new(),
            file_fact_count: None,
            thinking_block_count: None,
            compaction_count: None,
            model_set: Vec::new(),
            is_subagent: None,
            parent_external_id: None,
            turn_count: None,
            step_count: None,
            first_prompt: None,
            task_category: None,
            quality: None,
        }
    }

    pub fn into_publication(self, packet: &ConversationPacket, provider: &str) -> FactPublication {
        let coverage = self.coverage.finish(
            &self.tools,
            self.file_fact_count.unwrap_or(0).max(0) as usize,
            (self.is_subagent, self.parent_external_id.as_deref()),
        );
        let coverage_state = coverage
            .iter()
            .map(|entry| entry.state.as_str())
            .max_by_key(|state| severity(state))
            .unwrap_or("unknown")
            .to_string();
        let coverage_state = if self.extractor == "extractor_unavailable" {
            "unknown".to_string()
        } else {
            coverage_state
        };
        let tool_error_count = (!self.tools.is_empty()).then(|| {
            self.tools
                .iter()
                .filter(|t| t.is_error == Some(true))
                .count() as i64
        });
        let tool_calls_with_exit_status = (!self.tools.is_empty())
            .then(|| self.tools.iter().filter(|t| t.exit_code.is_some()).count() as i64);
        FactPublication {
            extractor: self.extractor.to_string(),
            extractor_version: FACTS_EXTRACTOR_VERSION,
            source_revision: 0, // stamped by extract_session_facts
            provider: provider.to_string(),
            source_id: packet.payload.provenance.source_id.clone(),
            coverage,
            coverage_state,
            is_subagent: self.is_subagent,
            parent_external_id: self.parent_external_id,
            task_category: self.task_category,
            turn_count: self.turn_count,
            step_count: self.step_count,
            thinking_block_count: self.thinking_block_count,
            artifact_count: None, // artifacts are overlay-bound, not extracted
            subagent_count: None, // child inventory is relation-bound (Task 8+)
            tool_error_count,
            tool_calls_with_exit_status,
            file_fact_count: if self.tools.is_empty() {
                None
            } else {
                self.file_fact_count
            },
            compaction_count: self.compaction_count,
            model_set: self.model_set,
            first_prompt: self.first_prompt,
            tools: self.tools,
            quality: self.quality,
            health: None, // canonical totals live outside the packet; see facts_for_canonical
        }
    }
}

/// Provider dispatch. `provider` is the canonical `agent_slug`.
///
/// `junie` and `qoder` intentionally stay `extractor_unavailable`: the
/// feature matrix (`docs/superpowers/cass/cass-owned-feature-matrix.md`)
/// records that no CASS/FAD connector exists for either, so there is no
/// canonical input to extract from — speculative extractors are forbidden.
pub(crate) fn extract(packet: &ConversationPacket, provider: &str) -> ProviderExtraction {
    match provider.to_ascii_lowercase().as_str() {
        "claude_code" | "claude" => claude::extract(packet),
        "codex" => codex::extract(packet),
        "antigravity" => antigravity::extract(packet),
        "devin" => devin::extract(packet),
        "cursor" => cursor::extract(packet),
        "gemini" => gemini::extract(packet),
        "pi_agent" => pi_agent::extract(packet),
        other => ProviderExtraction::unavailable(other),
    }
}
