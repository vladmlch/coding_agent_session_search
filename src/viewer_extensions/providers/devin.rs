//! Devin session facts with graph-scoped call identity. Behavioral port of
//! the oracle `facts/{devin,devin_records}.py` over the normalized packet.
//!
//! Two input shapes are accepted, matching the oracle's record contract:
//!
//! * Oracle contract records: `extra.kind`/`extra.payload` ACP envelopes
//!   (`tool_call`, `user_message`, `agent_message`, `agent_thought`,
//!   `subagent`) and `payload.role` CLI records (`cli_user`, `cli_assistant`,
//!   `cli_tool`, `cli_system`), optionally `node_id`/`parent_node_id` graph
//!   edges. Call identity is `(graph_root, raw_id)`; a raw id recorded under
//!   more than one root is qualified `devin-call:{sha256([root, id])}` and
//!   the two calls never merge.
//! * FAD sparse extras (what the pinned `devin` connector emits today):
//!   `{message_id, thinking, is_user_input, tool_call_id}` — no `kind`,
//!   `payload`, `role` or `tool_calls`. The canonical message role already
//!   projects `chat_message.role`, so `cli_{role}` handling applies with the
//!   normalized message content as the record content. `tool_calls` arrays
//!   were normalized into `NormalizedMessage.invocations`, which the packet
//!   contract does not carry — those calls surface as
//!   `devin_tool_calls_not_normalized` partial coverage, never as fabricated
//!   links.

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashSet};

use serde_json::{Value, json};
use sha2::Digest;

use crate::model::conversation_packet::ConversationPacket;
use crate::viewer_extensions::facts::ToolCallFact;

use super::classification::{self, ClassifyCall};
use super::{
    CoverageBuilder, Event, ProviderExtraction, RecordedCall, RecordedResult, ToolCollector,
    decode_json_str, events, json_text, text_id,
};

const EXTRACTOR_REVISION: &str = "devin-facts-v2";
const MAX_ARGUMENT_BYTES: usize = 1_000_000;
/// `cli_system` content prefixes that are compaction boundaries, not chat.
const BOUNDARY_PREFIXES: &[&str] = &[
    "You are continuing",
    "You are Devin",
    "You are a subagent of Devin",
    "You are a Summarizer",
];

/// `tool_calls[].arguments` may be a JSON string; decode it once, bounded.
fn arguments(value: &Value) -> (Value, bool) {
    match value {
        Value::String(text) => {
            if text.len() > MAX_ARGUMENT_BYTES {
                (value.clone(), false)
            } else {
                match decode_json_str(text) {
                    Some(decoded) => (decoded, true),
                    None => (value.clone(), false),
                }
            }
        }
        other => (other.clone(), true),
    }
}

/// Result text from recorded content: strings verbatim, parts arrays
/// concatenated; anything else is not recorded text.
fn text_chunks(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Array(items) => {
            let mut parts = Vec::new();
            for item in items {
                if let Some(text) = item.as_str() {
                    parts.push(text.to_string());
                } else {
                    let map = item.as_object()?;
                    let content = map.get("content").unwrap_or(item);
                    if let Some(text) = content.as_str() {
                        parts.push(text.to_string());
                    } else {
                        let text = content.get("text").and_then(Value::as_str)?;
                        parts.push(text.to_string());
                    }
                }
            }
            Some(parts.concat())
        }
        _ => None,
    }
}

/// First non-empty recorded string across the alias keys (oracle
/// `explicit_id`).
fn explicit_id(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| text_id(value.get(*key)))
}

/// Oracle `tool_status`: `_meta.terminal_exit.exit_code` or `exit_code`;
/// explicit `is_error` wins; `status` strings resolve failed/completed;
/// absent evidence stays unknown (never a fabricated false).
fn tool_status(payload: &Value) -> (Option<i64>, Option<bool>) {
    let exit_code = payload
        .pointer("/_meta/terminal_exit/exit_code")
        .or_else(|| payload.get("exit_code"))
        .and_then(Value::as_i64);
    if let Some(error) = payload.get("is_error").and_then(Value::as_bool) {
        return (exit_code, Some(error));
    }
    if let Some(status) = payload.get("status").and_then(Value::as_str) {
        let lower = status.to_lowercase();
        if matches!(lower.as_str(), "failed" | "error") {
            return (exit_code, Some(true));
        }
        if matches!(lower.as_str(), "completed" | "success" | "succeeded") {
            return (exit_code, Some(exit_code.is_some_and(|code| code != 0)));
        }
    }
    (exit_code, exit_code.map(|code| code != 0))
}

/// Oracle `normalize_devin_tool`: `(normalized_name, category)` combining
/// `content.kind`, `cognition.ai/inferenceToolName` and `title`.
fn normalize_devin_tool(
    content_kind: Option<&str>,
    inference_tool: Option<&str>,
    title: Option<&str>,
) -> (String, &'static str) {
    if let Some(inference) = inference_tool
        .map(str::trim)
        .filter(|text| !text.is_empty())
    {
        return match inference {
            "exec" | "terminal" | "bash" | "kill_shell" | "get_output" => {
                ("exec".to_string(), "command")
            }
            "read" | "mcp_read_resource" => ("read".to_string(), "read"),
            "grep" | "find_file_by_name" | "find_code_context" | "search" => {
                ("grep".to_string(), "read")
            }
            "edit" => ("edit".to_string(), "edit"),
            "run_subagent" => ("run_subagent".to_string(), "subagent"),
            "read_subagent" => ("read_subagent".to_string(), "subagent"),
            "ask_user_question" => ("ask_user_question".to_string(), "system"),
            "switch_mode" => ("switch_mode".to_string(), "system"),
            "skill" | "write_plan" | "exit_plan_mode" | "todo_write" => {
                (inference.to_string(), "other")
            }
            _ => {
                let category = match content_kind {
                    Some("execute") => "command",
                    Some("read" | "search") => "read",
                    Some("edit") => "edit",
                    Some("switch_mode") => "system",
                    _ => "other",
                };
                (inference.to_string(), category)
            }
        };
    }
    if let Some(kind) = content_kind.map(str::trim).filter(|text| !text.is_empty()) {
        return match kind {
            "execute" => ("exec".to_string(), "command"),
            "read" => ("read".to_string(), "read"),
            "search" => ("grep".to_string(), "read"),
            "edit" => ("edit".to_string(), "edit"),
            "switch_mode" => ("switch_mode".to_string(), "system"),
            _ => (kind.to_string(), "other"),
        };
    }
    if title.is_some_and(|title| title.starts_with("Ran ")) {
        return ("exec".to_string(), "command");
    }
    ("tool_call".to_string(), "other")
}

/// `node_id`/`parent_node_id` forest → each node's verified root label.
/// Broken ancestry (missing parent, cycle, non-integer ids) resolves to
/// `unverified:{node}` and marks the graph invalid.
fn graph_roots(input: &[Event]) -> (BTreeMap<i64, String>, bool) {
    let mut parents: BTreeMap<i64, Option<i64>> = BTreeMap::new();
    let mut valid = true;
    for event in input {
        let Some(extra) = event.extra.as_object() else {
            continue;
        };
        if !extra.contains_key("node_id") {
            continue;
        }
        let node = extra.get("node_id").and_then(Value::as_i64);
        let parent = extra.get("parent_node_id");
        let parent_id = match parent {
            None | Some(Value::Null) => Some(None),
            Some(value) => value.as_i64().map(Some),
        };
        let (Some(node_id), Some(parent_id)) = (node, parent_id) else {
            valid = false;
            continue;
        };
        if parents
            .get(&node_id)
            .is_some_and(|previous| *previous != parent_id)
        {
            valid = false;
        }
        parents.insert(node_id, parent_id);
    }
    let mut roots = BTreeMap::new();
    for &node in parents.keys() {
        let mut current = node;
        let mut visited = HashSet::new();
        while let Some(Some(parent)) = parents.get(&current) {
            if visited.contains(&current) {
                break;
            }
            visited.insert(current);
            current = *parent;
        }
        if !parents.contains_key(&current) || visited.contains(&current) {
            valid = false;
            roots.insert(node, format!("unverified:{node}"));
        } else {
            roots.insert(node, format!("root:{current}"));
        }
    }
    (roots, valid)
}

/// One accepted devin record (oracle `DevinRecord`).
struct DevinRecord<'a> {
    event: &'a Event<'a>,
    kind: String,
    payload: &'a Value,
    graph_root: String,
    /// `kind` came from the canonical message role (FAD sparse extras), so
    /// record `content`/`thinking` fall back to normalized message fields.
    sparse: bool,
}

fn record_of<'a>(event: &'a Event<'a>, roots: &BTreeMap<i64, String>) -> Option<DevinRecord<'a>> {
    let extra = event.extra.as_object()?;
    let (payload, payload_is_extra) = match extra.get("payload") {
        Some(payload) if payload.is_object() => (payload, false),
        _ => (event.extra, true),
    };
    let (kind, sparse) = match text_id(extra.get("kind")).or_else(|| text_id(payload.get("kind"))) {
        Some(kind) => (kind, false),
        None => {
            let role = payload
                .get("role")
                .and_then(Value::as_str)
                .or(if payload_is_extra {
                    Some(event.role)
                } else {
                    None
                });
            match role {
                Some(role @ ("user" | "assistant" | "tool" | "system")) => (
                    format!("cli_{role}"),
                    payload_is_extra && payload.get("role").is_none(),
                ),
                _ => return None,
            }
        }
    };
    let graph_root = match extra.get("node_id").and_then(Value::as_i64) {
        Some(node) => roots
            .get(&node)
            .cloned()
            .unwrap_or_else(|| "unknown".to_string()),
        None => "acp".to_string(),
    };
    Some(DevinRecord {
        event,
        kind,
        payload,
        graph_root,
        sparse,
    })
}

struct DevinCall {
    name: String,
    value: Value,
    event_idx: i64,
    message_id: Option<i64>,
}

#[derive(PartialEq)]
struct DevinOutput {
    text: Option<String>,
    exit_code: Option<i64>,
    is_error: Option<bool>,
    event_idx: i64,
}

/// Conflicting recorded call/output evidence is a tool-verdict gap, never
/// a text gap: every chat message is still a local indexed packet record,
/// so the marks land on the tool-verdict scopes only and
/// `chat`/`block_inventory`/`text_inventory` stay complete.
fn mark_conflicting(coverage: &mut CoverageBuilder) {
    coverage.mark(
        &["tools", "tool_exit", "tool_error", "files", "mcp"],
        "conflicting_event",
        "unavailable",
    );
}

struct Extraction {
    calls: BTreeMap<(String, String), DevinCall>,
    outputs: BTreeMap<(String, String), DevinOutput>,
    conflicts: HashSet<(String, String)>,
    coverage: CoverageBuilder,
    thinking: i64,
    compactions: i64,
    /// `false` degrades source-driven scopes to partial (oracle `complete`).
    complete: bool,
}

impl Extraction {
    fn call(&mut self, record: &DevinRecord, identifier: String, name: String, value: Value) {
        let (decoded, complete) = arguments(&value);
        self.complete = self.complete && complete;
        let key = (record.graph_root.clone(), identifier);
        match self.calls.entry(key.clone()) {
            Entry::Occupied(entry) => {
                let previous = entry.get();
                if previous.name != name || previous.value != decoded {
                    // A conflicting call is a tool-verdict gap
                    // (mark_conflicting narrows the marks to the tool
                    // scopes); it must not degrade chat/text scopes, so
                    // the oracle `complete` flag stays as-is.
                    self.conflicts.insert(key);
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(DevinCall {
                    name,
                    value: decoded,
                    event_idx: record.event.idx,
                    message_id: record.event.message_id,
                });
            }
        }
    }

    fn output(
        &mut self,
        record: &DevinRecord,
        identifier: String,
        payload: &Value,
        text: Option<String>,
    ) {
        let (exit_code, is_error) = tool_status(payload);
        let key = (record.graph_root.clone(), identifier);
        let value = DevinOutput {
            text,
            exit_code,
            is_error,
            event_idx: record.event.idx,
        };
        match self.outputs.entry(key.clone()) {
            Entry::Occupied(entry) => {
                if *entry.get() != value {
                    // A conflicting output is a tool-verdict gap, never a
                    // text gap — the `complete` flag stays as-is (the
                    // arguments-decode failure and the untyped `tool_call`
                    // shape still flip it).
                    self.conflicts.insert(key);
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(value);
            }
        }
    }

    /// Record content: recorded `payload.content`, or the normalized message
    /// content for sparse extras (the connector's projection of
    /// `chat_message.content`).
    fn content(&self, record: &DevinRecord) -> Option<Value> {
        record.payload.get("content").cloned().or_else(|| {
            record
                .sparse
                .then(|| Value::String(record.event.content.to_string()))
        })
    }

    /// `cli_*` records: boundaries, tool results, chat, thinking, tool_calls.
    fn cli(&mut self, record: &DevinRecord) -> bool {
        let content = self.content(record);
        match record.kind.as_str() {
            "cli_system" => content
                .as_ref()
                .and_then(Value::as_str)
                .is_some_and(|text| {
                    BOUNDARY_PREFIXES
                        .iter()
                        .any(|prefix| text.starts_with(prefix))
                }),
            "cli_tool" => {
                let Some(identifier) = explicit_id(
                    record.payload,
                    &["tool_call_id", "call_id", "toolCallId", "tool_id"],
                ) else {
                    return false;
                };
                let text = content.as_ref().and_then(text_chunks);
                if content.is_some() && text.is_none() {
                    return false;
                }
                self.output(record, identifier, record.payload, text);
                true
            }
            _ => {
                let mut known = true;
                if let Some(content) = &content {
                    if content.is_string() {
                        self.coverage.evidence("chat");
                    } else {
                        known = false;
                    }
                }
                if let Some(thinking) = record.payload.get("thinking") {
                    // Oracle CLI records nest `{signature, thinking}`; FAD
                    // sparse extras already unwrapped it to the string.
                    let recorded = if thinking.is_object() {
                        thinking.get("thinking").and_then(Value::as_str)
                    } else if record.sparse {
                        thinking.as_str()
                    } else {
                        None
                    };
                    match recorded.filter(|text| !text.is_empty()) {
                        Some(_) => {
                            self.thinking += 1;
                            self.coverage.evidence("thinking");
                        }
                        None => known = false,
                    }
                }
                // The connector renders dropped calls as `[tool_call: name]`
                // content lines: evidence the calls existed even though the
                // packet no longer carries their ids/arguments.
                if record.sparse && record.event.content.contains("[tool_call:") {
                    self.coverage.mark(
                        &["tools", "tool_exit", "tool_error", "files"],
                        "devin_tool_calls_not_normalized",
                        "partial",
                    );
                }
                if let Some(calls) = record.payload.get("tool_calls") {
                    let Some(items) = calls.as_array() else {
                        return false;
                    };
                    for tool in items {
                        let Some(tool) = tool.as_object() else {
                            known = false;
                            continue;
                        };
                        let identifier = ["id", "tool_call_id", "call_id", "toolCallId", "tool_id"]
                            .iter()
                            .find_map(|key| text_id(tool.get(*key)));
                        let name = tool.get("name").and_then(Value::as_str);
                        match (identifier, name) {
                            (Some(identifier), Some(name)) if tool.contains_key("arguments") => {
                                let (normalized, _category) =
                                    normalize_devin_tool(None, Some(name), None);
                                self.call(
                                    record,
                                    identifier,
                                    normalized,
                                    tool["arguments"].clone(),
                                );
                            }
                            _ => known = false,
                        }
                    }
                }
                known
            }
        }
    }

    /// ACP `tool_call` records: composite id + inline `rawOutput` result.
    fn acp_tool(&mut self, record: &DevinRecord) -> bool {
        let payload = record.payload;
        let content = payload.get("content");
        let tool = content.filter(|value| value.is_object()).unwrap_or(payload);
        let Some(identifier) = explicit_id(tool, &["toolCallId", "callId", "id"])
            .or_else(|| explicit_id(payload, &["toolCallId", "id"]))
        else {
            return false;
        };
        let meta = tool
            .get("_meta")
            .filter(|value| value.is_object())
            .or_else(|| payload.get("_meta").filter(|value| value.is_object()));
        let inference = meta
            .and_then(|meta| meta.get("cognition.ai/inferenceToolName"))
            .and_then(Value::as_str);
        let kind = tool
            .get("kind")
            .and_then(Value::as_str)
            .filter(|kind| *kind != "tool_call");
        let title = tool.get("title").and_then(Value::as_str);
        let (name, _category) = normalize_devin_tool(kind, inference, title);
        let Some(value) = tool
            .get("rawInput")
            .or_else(|| tool.get("input"))
            .or_else(|| tool.get("arguments"))
        else {
            return false;
        };
        if name == "tool_call" {
            self.coverage
                .mark(&["tools"], "devin_tool_call_untyped", "partial");
            self.complete = false;
            return false;
        }
        self.call(record, identifier.clone(), name, value.clone());
        let raw_output = tool
            .get("rawOutput")
            .or_else(|| tool.get("output"))
            .or_else(|| tool.get("content"));
        let text = raw_output.and_then(text_chunks);
        self.output(record, identifier, tool, text);
        true
    }

    /// `user_message`/`agent_message`/`agent_thought` content records.
    fn message_kind(&mut self, record: &DevinRecord) -> bool {
        let Some(_text) = record.payload.get("content").and_then(text_chunks) else {
            return false;
        };
        if record.kind == "agent_thought" {
            self.thinking += 1;
            self.coverage.evidence("thinking");
        } else {
            self.coverage.evidence("chat");
        }
        true
    }

    /// Explicit `subagent` descriptors: relation evidence only — no public
    /// child CID is ever manufactured (children land with Task 8+).
    fn subagent(&mut self, record: &DevinRecord) -> bool {
        if explicit_id(record.payload, &["agentId"]).is_none() {
            return false;
        }
        self.coverage.mark(
            &["subagents"],
            "devin_subagent_descriptors_not_persisted",
            "partial",
        );
        true
    }

    fn accept(&mut self, record: &DevinRecord) -> bool {
        let accepted = match record.kind.as_str() {
            "cli_user" | "cli_assistant" | "cli_tool" | "cli_system" => self.cli(record),
            "tool_call" => self.acp_tool(record),
            "user_message" | "agent_message" | "agent_thought" => self.message_kind(record),
            "subagent" => self.subagent(record),
            _ => false,
        };
        if accepted && record.kind == "cli_system" {
            self.compactions += 1;
        }
        accepted
    }

    /// Correlate recorded calls/outputs into verified links (oracle
    /// `_Extraction.tools`). Raw identifiers spanning more than one graph
    /// root are qualified `devin-call:{sha256([root, id])}`; conflicting
    /// recorded signatures never merge.
    fn finish_tools(mut self) -> (Vec<ToolCallFact>, usize, CoverageBuilder) {
        let mut raw_counts: BTreeMap<&str, usize> = BTreeMap::new();
        for (_, identifier) in self.calls.keys() {
            *raw_counts.entry(identifier.as_str()).or_default() += 1;
        }
        let qualified = |root: &str, identifier: &str| -> String {
            if raw_counts.get(identifier).copied().unwrap_or(0) > 1 {
                let digest = sha2::Sha256::digest(json_text(&json!([root, identifier])).as_bytes());
                format!("devin-call:{}", hex::encode(digest))
            } else {
                identifier.to_string()
            }
        };
        let mut tools = ToolCollector::new();
        for ((root, identifier), call) in &self.calls {
            if self.conflicts.contains(&(root.clone(), identifier.clone())) {
                continue;
            }
            tools.call(RecordedCall {
                event_idx: call.event_idx,
                message_id: call.message_id,
                call_id: qualified(root, identifier),
                raw_call_id: identifier.clone(),
                name: call.name.clone(),
                arguments: Some(call.value.clone()),
            });
        }
        for ((root, identifier), output) in &self.outputs {
            if self.conflicts.contains(&(root.clone(), identifier.clone())) {
                continue;
            }
            tools.result(RecordedResult {
                event_idx: output.event_idx,
                call_id: qualified(root, identifier),
                text: output.text.clone(),
                exit_code: output.exit_code,
                is_error: output.is_error,
            });
        }
        for key in &self.conflicts {
            let _ = key;
            mark_conflicting(&mut self.coverage);
        }
        if !self.complete {
            self.coverage.mark(
                &["chat", "block_inventory", "text_inventory", "subagents"],
                "devin_source_or_graph_incomplete",
                "partial",
            );
        }
        let (tools, file_facts) = tools.finish(&mut self.coverage);
        (tools, file_facts, self.coverage)
    }
}

pub(crate) fn extract(packet: &ConversationPacket) -> ProviderExtraction {
    let input = events(packet);
    let (roots, graph_valid) = graph_roots(&input);
    let mut extraction = Extraction {
        calls: BTreeMap::new(),
        outputs: BTreeMap::new(),
        conflicts: HashSet::new(),
        coverage: CoverageBuilder::new(),
        thinking: 0,
        compactions: 0,
        complete: graph_valid,
    };
    let mut models: BTreeSet<String> = BTreeSet::new();
    let mut deduped: Vec<(i64, &str, &str)> = Vec::new();

    for event in &input {
        match record_of(event, &roots) {
            Some(record) => {
                let boundary = record.kind == "cli_system";
                if extraction.accept(&record) {
                    if boundary {
                        continue;
                    }
                } else {
                    extraction.coverage.unknown("devin_record_unknown");
                }
            }
            None => extraction.coverage.unknown("devin_record_unknown"),
        }
        deduped.push((event.idx, event.role, event.content));
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

    let classify_calls: Vec<ClassifyCall> = extraction
        .calls
        .values()
        .map(|call| ClassifyCall {
            event_idx: call.event_idx,
            name: call.name.clone(),
            arguments: Some(call.value.clone()),
        })
        .collect();
    let thinking = extraction.thinking;
    let compactions = extraction.compactions;
    let (tools, file_facts, mut coverage) = extraction.finish_tools();

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

    let compaction_count = if compactions > 0 {
        Some(compactions)
    } else {
        coverage.mark(
            &["compaction"],
            "boundary_records_not_normalized",
            "unavailable",
        );
        None
    };
    ProviderExtraction {
        extractor: EXTRACTOR_REVISION,
        coverage,
        tools,
        file_fact_count: Some(file_facts as i64),
        thinking_block_count: Some(thinking),
        compaction_count,
        model_set: models.into_iter().collect(),
        // No session-parent evidence survives normalization for devin; the
        // ACP container parent is locator-bound and lands with Task 8+.
        is_subagent: None,
        parent_external_id: None,
        turn_count,
        step_count,
        first_prompt,
        task_category,
        quality: Some(quality),
    }
}
