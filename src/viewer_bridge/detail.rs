//! `get_session_details` / `get_tool_output` for the `cass_owned_queries_v1`
//! read lane: bounded rich detail mapped from canonical `conversations` +
//! `messages` rows plus the already-persisted `viewer_*` overlays.
//!
//! Contract (spec §4/§5, plan Task 6):
//!
//! * **Targeted reads only.** Every statement below is a primary-key probe
//!   or a `WHERE conversation_id = ?` range on `idx_messages_conv_idx` —
//!   the catalog is never materialized and one session's detail never
//!   touches another session's rows.
//! * **No raw agent-source file access on the canonical path.** A missing
//!   or unreadable source/attachment file cannot invalidate canonical
//!   display: recorded references are listed as `evidence_missing`, and
//!   `source_raw` stays `unavailable` while `canonical_raw` — the bytes the
//!   connector persisted — stays `available`. `source_raw` vs
//!   `canonical_raw` are always labelled, never fabricated.
//! * **unknown ≠ 0.** Persisted facts columns are read as NULL-able values
//!   with `coverage_state` / `coverage_detail_json` carried through
//!   verbatim; an absent facts row reports `pending` (extraction not yet
//!   run), never a fabricated `complete`. Native detail surfaces that
//!   canonical + `viewer_*` data cannot yet express (steps, turns, tool
//!   pairing, thinking blocks, health, files, memory — Task 7/8) are
//!   recognized names answered with typed `unavailable`, never silently
//!   dropped.
//! * **`expected_revision` on `get_tool_output` binds the logical
//!   `canonical_revision` counter** — global, not a per-session digest.
//!   Any committed canonical change invalidates conservatively: an anchor
//!   is never served against a generation the client did not see. A
//!   per-session detail revision lands with the Task-8 projection.

use anyhow::Result;
use coding_agent_search::franken_sync::compat::{RowExt, param_slice_to_values};
use coding_agent_search::franken_sync::{Connection, params};
use serde_json::{Map, Value, json};

use super::annotations;
use super::catalog;
use super::identity;
use super::protocol::{StepAnchor, refuse};
use super::revisions;

const MAX_DETAIL_SECTIONS: usize = 16;
const SECTION_NAME_BYTES: usize = 64;
const DETAIL_CURSOR_BYTES: usize = 256;
const TOOL_OUTPUT_BYTES: usize = 512 * 1024;
const SNIPPET_ROWS: i64 = 256;
const TEXT_GUARD_BYTES: i64 = 512 * 1024;
const MAX_BLOCK_INDEX: i64 = 64;

/// Sections the canonical lane serves today. An absent/empty `sections`
/// request defaults to exactly this set.
const SERVED_SECTIONS: &[&str] = &[
    "summary",
    "messages",
    "raw",
    "facts",
    "availability",
    "annotations",
    "attachments",
    "snippets",
    "steps",
    "turns",
    "tools",
    "thinking",
    "files",
];

/// Sections backed by the shared canonical messages page (`cursor`).
const PAGE_SECTIONS: &[&str] = &["messages", "raw", "attachments"];

/// Native detail surfaces not yet expressible: recognized names with their
/// typed-unavailable reason. Task 10 lands the analytics/dynamic-resource
/// lanes that make them servable; until then they are reported, never
/// dropped and never fabricated. The Task-8 structural sections
/// (`steps`/`turns`/`tools`/`thinking`/`files`) are served from the
/// persisted `viewer_*` projection links and answer typed `unavailable`
/// when no projection was published.
const PENDING_SECTIONS: &[(&str, &str)] = &[
    ("health", "session_audit_pending_task7"),
    ("memory", "memory_resource_lane_pending"),
];

/// Address exactly one of `conversation_id`/`public_id` inside the caller's
/// snapshot so identity resolution and the page share one generation.
pub fn resolve_target(
    connection: &Connection,
    conversation_id: Option<i64>,
    public_id: Option<&str>,
) -> Result<i64> {
    match (conversation_id, public_id) {
        (Some(id), None) => {
            if id <= 0 {
                return Err(refuse(
                    "invalid_request",
                    "conversation_id must be positive",
                ));
            }
            Ok(id)
        }
        (None, Some(public_id)) => identity::resolve_conversation_id(connection, public_id)?
            .ok_or_else(|| {
                refuse(
                    "session_not_found",
                    "No canonical conversation verifies to this public ID",
                )
            }),
        _ => Err(refuse(
            "invalid_request",
            "Address exactly one of conversation_id or public_id",
        )),
    }
}

/// Detail cursor: `{"m": <last emitted messages.idx>}` — the keyset
/// position of the paged sections. Opaque to clients and only meaningful
/// while a paged section was requested.
fn decode_detail_cursor(raw: &str) -> Result<i64> {
    let expired = || refuse("cursor_expired", "Invalid session detail cursor");
    if raw.len() > DETAIL_CURSOR_BYTES || !raw.is_ascii() {
        return Err(expired());
    }
    let value: Value = serde_json::from_str(raw).map_err(|_| expired())?;
    let idx = value
        .as_object()
        .and_then(|map| map.get("m"))
        .and_then(Value::as_i64)
        .ok_or_else(expired)?;
    if idx < 0 {
        return Err(expired());
    }
    Ok(idx)
}

fn resolve_sections(sections: &[String]) -> Result<Vec<String>> {
    if sections.len() > MAX_DETAIL_SECTIONS {
        return Err(refuse(
            "invalid_request",
            "Too many detail sections requested",
        ));
    }
    let mut out: Vec<String> = Vec::new();
    for name in sections {
        if name.is_empty() || name.len() > SECTION_NAME_BYTES || name.trim() != name.as_str() {
            return Err(refuse(
                "invalid_request",
                "Section names must be non-empty, trimmed and within bounds",
            ));
        }
        if !SERVED_SECTIONS.contains(&name.as_str())
            && !PENDING_SECTIONS.iter().any(|(pending, _)| *pending == name)
        {
            return Err(refuse(
                "unsupported_filter",
                format!("Unsupported detail section: {name}"),
            ));
        }
        if !out.iter().any(|existing| existing == name) {
            out.push(name.clone());
        }
    }
    if out.is_empty() {
        out = SERVED_SECTIONS
            .iter()
            .map(|name| name.to_string())
            .collect();
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Persisted session facts (nullable; coverage carried through verbatim)
// ---------------------------------------------------------------------------

struct FactsRow {
    extractor_version: i64,
    source_revision: i64,
    coverage_state: String,
    coverage: Option<Value>,
    is_subagent: Option<i64>,
    parent_conversation_id: Option<i64>,
    task_category: Option<String>,
    turn_count: Option<i64>,
    step_count: Option<i64>,
    thinking_block_count: Option<i64>,
    artifact_count: Option<i64>,
    subagent_count: Option<i64>,
    tool_error_count: Option<i64>,
    tool_calls_with_exit_status: Option<i64>,
    file_fact_count: Option<i64>,
    compaction_count: Option<i64>,
    model_set: Option<Vec<String>>,
    first_prompt: Option<String>,
    /// `false` when the installed extension schema predates the
    /// `first_prompt` column — the value is then reported `unknown`, never
    /// synthesized from messages (native semantics need the projected
    /// steps, Task 8).
    first_prompt_column: bool,
    extracted_at_ms: Option<i64>,
}

fn read_facts(connection: &Connection, conversation_id: i64) -> Result<Option<FactsRow>> {
    // Column-presence probe: `first_prompt` lands with the concurrent schema
    // fix; absent column → NULL + `unavailable:first_prompt_column_pending`.
    let first_prompt_column = connection
        .query("SELECT first_prompt FROM viewer_session_facts LIMIT 0")
        .is_ok();
    let select = format!(
        "SELECT extractor_version,source_revision,coverage_state,coverage_detail_json,\
         is_subagent,parent_conversation_id,task_category,turn_count,step_count,\
         thinking_block_count,artifact_count,subagent_count,tool_error_count,\
         tool_calls_with_exit_status,file_fact_count,compaction_count,model_set_json,\
         extracted_at_ms{} FROM viewer_session_facts WHERE conversation_id = ?1 LIMIT 2",
        if first_prompt_column {
            ",first_prompt"
        } else {
            ",NULL"
        }
    );
    let rows =
        connection.query_with_params(&select, &param_slice_to_values(params![conversation_id]))?;
    if rows.len() > 1 {
        return Err(refuse(
            "canonical_coordinates_invalid",
            "Ambiguous facts row",
        ));
    }
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let coverage: Option<Value> = row
        .get_typed::<Option<String>>(3)?
        .and_then(|text| serde_json::from_str(&text).ok());
    let model_set: Option<Vec<String>> = row
        .get_typed::<Option<String>>(16)?
        .and_then(|text| serde_json::from_str(&text).ok());
    Ok(Some(FactsRow {
        extractor_version: row.get_typed(0)?,
        source_revision: row.get_typed(1)?,
        coverage_state: row.get_typed(2)?,
        coverage,
        is_subagent: row.get_typed(4)?,
        parent_conversation_id: row.get_typed(5)?,
        task_category: row.get_typed(6)?,
        turn_count: row.get_typed(7)?,
        step_count: row.get_typed(8)?,
        thinking_block_count: row.get_typed(9)?,
        artifact_count: row.get_typed(10)?,
        subagent_count: row.get_typed(11)?,
        tool_error_count: row.get_typed(12)?,
        tool_calls_with_exit_status: row.get_typed(13)?,
        file_fact_count: row.get_typed(14)?,
        compaction_count: row.get_typed(15)?,
        model_set,
        first_prompt: if first_prompt_column {
            row.get_typed(18)?
        } else {
            None
        },
        first_prompt_column,
        extracted_at_ms: row.get_typed(17)?,
    }))
}

fn facts_section(facts: Option<&FactsRow>) -> Value {
    let Some(facts) = facts else {
        return json!({
            "state": "pending",
            "reason": "facts_not_persisted",
            "extractor_version": Value::Null,
            "source_revision": Value::Null,
            "extracted_at_ms": Value::Null,
            "is_subagent": Value::Null,
            "parent_conversation_id": Value::Null,
            "task_category": Value::Null,
            "turn_count": Value::Null,
            "step_count": Value::Null,
            "thinking_block_count": Value::Null,
            "artifact_count": Value::Null,
            "subagent_count": Value::Null,
            "tool_error_count": Value::Null,
            "tool_calls_with_exit_status": Value::Null,
            "file_fact_count": Value::Null,
            "compaction_count": Value::Null,
            "first_prompt": Value::Null,
            "model_set": Value::Null,
            "coverage": Value::Null,
        });
    };
    json!({
        "state": facts.coverage_state,
        "extractor_version": facts.extractor_version,
        "source_revision": facts.source_revision,
        "extracted_at_ms": facts.extracted_at_ms,
        "is_subagent": facts.is_subagent.map(|value| value != 0),
        "parent_conversation_id": facts.parent_conversation_id,
        "task_category": facts.task_category,
        "turn_count": facts.turn_count,
        "step_count": facts.step_count,
        "thinking_block_count": facts.thinking_block_count,
        "artifact_count": facts.artifact_count,
        "subagent_count": facts.subagent_count,
        "tool_error_count": facts.tool_error_count,
        "tool_calls_with_exit_status": facts.tool_calls_with_exit_status,
        "file_fact_count": facts.file_fact_count,
        "compaction_count": facts.compaction_count,
        "first_prompt": facts.first_prompt,
        "model_set": facts.model_set,
        "coverage": facts.coverage,
    })
}

fn scope_state<'a>(facts: Option<&'a FactsRow>, field: &str) -> Option<&'a str> {
    facts?
        .coverage
        .as_ref()?
        .pointer(&format!("/fields/{field}/state"))?
        .as_str()
}

/// `ResourceAvailability` for this lane (oracle `detail_models.py`):
/// `canonical_raw` is served; `source_raw`/artifact bytes/memory are never
/// read on the canonical path. `tools`/`thinking` map the persisted
/// coverage states; `artifacts` caps at `partial` — a recorded inventory is
/// listable, byte content is never served here.
fn availability_section(facts: Option<&FactsRow>) -> Value {
    let map = |state: Option<&str>| match state {
        Some("complete") => "available",
        Some("partial") => "partial",
        _ => "unavailable",
    };
    let recorded_inventory = facts.is_some_and(|facts| {
        facts.artifact_count.unwrap_or(0) > 0
            || facts.file_fact_count.unwrap_or(0) > 0
            || matches!(
                scope_state(Some(facts), "artifacts"),
                Some("complete" | "partial")
            )
    });
    json!({
        "canonical_raw": "available",
        "source_raw": "unavailable",
        "tools": map(scope_state(facts, "tools")),
        "thinking": map(scope_state(facts, "thinking")),
        "artifacts": if recorded_inventory { "partial" } else { "unavailable" },
        "memory": "unavailable",
    })
}

// ---------------------------------------------------------------------------
// Sections
// ---------------------------------------------------------------------------

fn pending_section(name: &str) -> Value {
    let reason = PENDING_SECTIONS
        .iter()
        .find(|(candidate, _)| *candidate == name)
        .map(|(_, reason)| *reason)
        .unwrap_or("section_not_implemented");
    json!({"state": "unavailable", "reason": reason})
}

fn messages_section(page: &Value) -> Value {
    json!({
        "items": page["items"],
        "next_idx": page["next_idx"],
    })
}

/// `canonical_raw` resource projection: the recorded canonical payloads for
/// the paged messages, oracle `canonical_message_payload` shape — the
/// `extra` value plus its decode diagnostics as explicit siblings.
/// `source_raw` is labelled unavailable: this lane never opens source
/// journal files.
fn raw_section(page: &Value) -> Result<Value> {
    let items = page["items"].as_array().cloned().unwrap_or_default();
    let mut records = Vec::with_capacity(items.len());
    for item in &items {
        records.push(json!({
            "message_id": item["id"],
            "idx": item["idx"],
            "role": item["role"],
            "author": item["author"],
            "created_at_ms": item["created_at_ms"],
            "content": item["content"],
            "extra": item["extra"]["value"],
            "extra_origin": item["extra"]["origin"],
            "extra_complete": item["extra"]["complete"],
            "extra_issues": item["extra"]["issues"],
        }));
    }
    Ok(json!({
        "state": "available",
        "origin": "canonical_raw",
        "reason": "canonical_projection_is_not_original_raw",
        "records": records,
        "next_idx": page["next_idx"],
        "source_raw": {
            "state": "unavailable",
            "reason": "source_journal_not_served_on_canonical_lane",
        },
    }))
}

/// Recorded attachment references inside the fetched messages page. A
/// reference the canonical lane cannot serve bytes for is
/// `evidence_missing` — the canonical message display is never invalidated
/// by a missing/unreadable artifact file.
fn push_attachment(evidence: &mut Vec<Value>, item: &Value, path: &str) {
    evidence.push(json!({
        "message_id": item["id"],
        "idx": item["idx"],
        "path": path,
        "state": "evidence_missing",
        "reason": "artifact_file_not_served_on_canonical_lane",
    }));
}

fn attachment_evidence(item: &Value, evidence: &mut Vec<Value>) {
    let extra = &item["extra"]["value"];
    let Some(map) = extra.as_object() else { return };
    if matches!(
        map.get("type").and_then(Value::as_str),
        Some("attachment" | "artifact" | "file")
    ) {
        for key in ["file_path", "path", "name"] {
            if let Some(path) = map
                .get(key)
                .and_then(Value::as_str)
                .filter(|p| !p.is_empty())
            {
                push_attachment(evidence, item, path);
                break;
            }
        }
    }
    if let Some(list) = map.get("attachments").and_then(Value::as_array) {
        for element in list {
            if let Some(path) = element.as_str().filter(|p| !p.is_empty()) {
                push_attachment(evidence, item, path);
            } else if let Some(object) = element.as_object() {
                for key in ["file_path", "path", "name"] {
                    if let Some(path) = object
                        .get(key)
                        .and_then(Value::as_str)
                        .filter(|p| !p.is_empty())
                    {
                        push_attachment(evidence, item, path);
                        break;
                    }
                }
            }
        }
    }
}

fn attachments_section(page: &Value) -> Value {
    let mut evidence = Vec::new();
    for item in page["items"].as_array().into_iter().flatten() {
        attachment_evidence(item, &mut evidence);
    }
    if evidence.is_empty() {
        json!({
            "state": "unavailable",
            "reason": "no_recorded_attachment_evidence",
            "scope": "messages_page",
            "evidence": Vec::<Value>::new(),
        })
    } else {
        json!({
            "state": "partial",
            "reason": "artifact_file_content_not_served_on_canonical_lane",
            "scope": "messages_page",
            "evidence": evidence,
        })
    }
}

/// Canonical `snippets` rows joined to this conversation's messages —
/// persisted file fragments, bounded by rows and response bytes.
fn snippets_section(connection: &Connection, conversation_id: i64) -> Result<Value> {
    let rows = connection.query_with_params(
        &format!(
            "SELECT s.message_id,\
             CASE WHEN COALESCE(length(CAST(s.file_path AS BLOB)),0)<={TEXT_GUARD_BYTES} \
                 THEN s.file_path ELSE NULL END,\
             s.start_line,s.end_line,s.language,\
             CASE WHEN COALESCE(length(CAST(s.snippet_text AS BLOB)),0)<={TEXT_GUARD_BYTES} \
                 THEN s.snippet_text ELSE NULL END \
             FROM snippets s JOIN messages m ON m.id = s.message_id \
             WHERE m.conversation_id = ?1 ORDER BY s.id LIMIT ?2"
        ),
        &param_slice_to_values(params![conversation_id, SNIPPET_ROWS + 1]),
    )?;
    let mut items = Vec::new();
    let mut bytes = 0_usize;
    let mut truncated = rows.len() as i64 > SNIPPET_ROWS;
    for row in rows.iter().take(SNIPPET_ROWS as usize) {
        let value = json!({
            "message_id": row.get_typed::<i64>(0)?,
            "file_path": row.get_typed::<Option<String>>(1)?,
            "start_line": row.get_typed::<Option<i64>>(2)?,
            "end_line": row.get_typed::<Option<i64>>(3)?,
            "language": row.get_typed::<Option<String>>(4)?,
            "snippet_text": row.get_typed::<Option<String>>(5)?,
        });
        if !catalog::push_bounded(&mut items, value, &mut bytes)? {
            truncated = true;
            break;
        }
    }
    Ok(json!({
        "state": "available",
        "items": items,
        "truncated": truncated,
    }))
}

// ---------------------------------------------------------------------------
// Task-8 structural sections: persisted projection links joined to canonical
// message coordinates. Anchors only — no message bodies are copied into the
// link tables; the caller resolves content via `messages`/`get_tool_output`.
// ---------------------------------------------------------------------------

/// `None` = no `viewer_session_projection` table (profile predates v2) or
/// no marker for this conversation; `Some(stale)` when the persisted
/// projection predates the current `PROJECTION_VERSION`.
fn projection_marker(connection: &Connection, conversation_id: i64) -> Result<Option<bool>> {
    if connection
        .query("SELECT 1 FROM sqlite_master WHERE type='table' AND name='viewer_session_projection' LIMIT 1")?
        .is_empty()
    {
        return Ok(None);
    }
    let rows = connection.query_with_params(
        "SELECT projection_version FROM viewer_session_projection \
         WHERE conversation_id = ?1 LIMIT 2",
        &param_slice_to_values(params![conversation_id]),
    )?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let version: i64 = row.get_typed(0)?;
    Ok(Some(
        version < coding_agent_search::viewer_extensions::projection::PROJECTION_VERSION,
    ))
}

/// Typed state for structural sections: `available` when a current
/// projection exists, `pending` when a stale one needs recomputation,
/// `unavailable` when none was ever published.
fn projection_state(stale: Option<bool>) -> (&'static str, &'static str) {
    match stale {
        Some(true) => ("pending", "projection_stale_reindex_required"),
        Some(false) => ("available", ""),
        None => ("unavailable", "projection_not_persisted"),
    }
}

fn anchor_json(message_id: Option<i64>, idx: Option<i64>) -> Value {
    json!({"message_id": message_id, "idx": idx})
}

/// `steps`: surviving projection steps with member/part links — the
/// step→message mapping, exact and stable across reindex.
fn steps_section(connection: &Connection, conversation_id: i64) -> Result<Value> {
    let Some(stale) = projection_marker(connection, conversation_id)? else {
        let (state, reason) = projection_state(None);
        return Ok(json!({"state": state, "reason": reason}));
    };
    if stale {
        let (state, reason) = projection_state(Some(true));
        return Ok(json!({"state": state, "reason": reason}));
    }
    let rows = connection.query_with_params(
        "SELECT step_index,turn_index,step_kind,link_index,scope,block_index,\
         call_id,source_idx,source_key,message_id,part_key \
         FROM viewer_step_links WHERE conversation_id = ?1 \
         ORDER BY step_index, link_index",
        &param_slice_to_values(params![conversation_id]),
    )?;
    let mut steps: Vec<Value> = Vec::new();
    let mut members: Vec<Value> = Vec::new();
    let mut parts: Vec<Value> = Vec::new();
    let mut current: Option<i64> = None;
    let flush = |steps: &mut Vec<Value>,
                 current: Option<i64>,
                 kind: &mut Option<String>,
                 turn: &mut Option<i64>,
                 members: &mut Vec<Value>,
                 parts: &mut Vec<Value>| {
        if let Some(step_index) = current {
            steps.push(json!({
                "step_index": step_index,
                "turn_index": turn.take(),
                "kind": kind.take(),
                "members": std::mem::take(members),
                "parts": std::mem::take(parts),
            }));
        }
    };
    let mut step_kind: Option<String> = None;
    let mut step_turn: Option<i64> = None;
    for row in &rows {
        let step_index: i64 = row.get_typed(0)?;
        if current != Some(step_index) {
            flush(
                &mut steps,
                current,
                &mut step_kind,
                &mut step_turn,
                &mut members,
                &mut parts,
            );
            current = Some(step_index);
            step_kind = Some(row.get_typed(2)?);
            step_turn = row.get_typed(1)?;
        }
        let scope: String = row.get_typed(4)?;
        let anchor = anchor_json(row.get_typed(9)?, Some(row.get_typed(7)?));
        match scope.as_str() {
            "member" | "result" => members.push(json!({
                "scope": scope,
                "anchor": anchor,
                "source_key": row.get_typed::<Option<String>>(8)?,
            })),
            _ => parts.push(json!({
                "kind": scope,
                "block_index": row.get_typed::<Option<i64>>(5)?,
                "call_id": row.get_typed::<Option<String>>(6)?,
                "anchor": anchor,
                "part_key": row.get_typed::<Option<String>>(10)?,
            })),
        }
    }
    flush(
        &mut steps,
        current,
        &mut step_kind,
        &mut step_turn,
        &mut members,
        &mut parts,
    );
    let _ = flush;
    Ok(json!({
        "state": "available",
        "items": steps,
        "projection_version": coding_agent_search::viewer_extensions::projection::PROJECTION_VERSION,
    }))
}

/// `turns`: emitted turn list (native `turnIndex` ordinal).
fn turns_section(connection: &Connection, conversation_id: i64) -> Result<Value> {
    let Some(stale) = projection_marker(connection, conversation_id)? else {
        let (state, reason) = projection_state(None);
        return Ok(json!({"state": state, "reason": reason}));
    };
    if stale {
        let (state, reason) = projection_state(Some(true));
        return Ok(json!({"state": state, "reason": reason}));
    }
    let rows = connection.query_with_params(
        "SELECT turn_index,turn_kind,first_step_index,step_count,source_idx,\
         message_id,source_key FROM viewer_turn_links \
         WHERE conversation_id = ?1 ORDER BY turn_index",
        &param_slice_to_values(params![conversation_id]),
    )?;
    let items: Vec<Value> = rows
        .iter()
        .map(|row| {
            Ok(json!({
                "turn_index": row.get_typed::<i64>(0)?,
                "kind": row.get_typed::<String>(1)?,
                "first_step_index": row.get_typed::<i64>(2)?,
                "step_count": row.get_typed::<i64>(3)?,
                "anchor": anchor_json(row.get_typed(5)?, row.get_typed(4)?),
                "source_key": row.get_typed::<Option<String>>(6)?,
            }))
        })
        .collect::<Result<_>>()?;
    Ok(json!({"state": "available", "items": items}))
}

/// `tools`: verified call→result links with canonical anchors.
fn tools_section(connection: &Connection, conversation_id: i64) -> Result<Value> {
    let Some(stale) = projection_marker(connection, conversation_id)? else {
        let (state, reason) = projection_state(None);
        return Ok(json!({"state": state, "reason": reason}));
    };
    if stale {
        let (state, reason) = projection_state(Some(true));
        return Ok(json!({"state": state, "reason": reason}));
    }
    let rows = connection.query_with_params(
        "SELECT call_id,raw_call_id,name,step_index,turn_index,has_result,\
         is_error,exit_code,file_path,call_idx,call_message_id,result_idx,\
         result_message_id,result_block_index FROM viewer_tool_calls \
         WHERE conversation_id = ?1 ORDER BY ordinal",
        &param_slice_to_values(params![conversation_id]),
    )?;
    let items: Vec<Value> = rows
        .iter()
        .map(|row| {
            Ok(json!({
                "call_id": row.get_typed::<String>(0)?,
                "raw_call_id": row.get_typed::<String>(1)?,
                "name": row.get_typed::<String>(2)?,
                "step_index": row.get_typed::<Option<i64>>(3)?,
                "turn_index": row.get_typed::<Option<i64>>(4)?,
                "has_result": row.get_typed::<i64>(5)? != 0,
                "is_error": row.get_typed::<Option<i64>>(6)?.map(|v| v != 0),
                "exit_code": row.get_typed::<Option<i64>>(7)?,
                "file_path": row.get_typed::<Option<String>>(8)?,
                "anchors": {
                    "call": anchor_json(row.get_typed(10)?, Some(row.get_typed(9)?)),
                    "result": json!({
                        "message_id": row.get_typed::<Option<i64>>(12)?,
                        "idx": row.get_typed::<Option<i64>>(11)?,
                        "block_index": row.get_typed::<Option<i64>>(13)?,
                    }),
                },
            }))
        })
        .collect::<Result<_>>()?;
    Ok(json!({"state": "available", "items": items}))
}

/// `thinking`: thinking-part links from the persisted projection.
fn thinking_section(connection: &Connection, conversation_id: i64) -> Result<Value> {
    let Some(stale) = projection_marker(connection, conversation_id)? else {
        let (state, reason) = projection_state(None);
        return Ok(json!({"state": state, "reason": reason}));
    };
    if stale {
        let (state, reason) = projection_state(Some(true));
        return Ok(json!({"state": state, "reason": reason}));
    }
    let rows = connection.query_with_params(
        "SELECT step_index,source_idx,message_id,part_key FROM viewer_step_links \
         WHERE conversation_id = ?1 AND scope = 'thinking' ORDER BY step_index, link_index",
        &param_slice_to_values(params![conversation_id]),
    )?;
    let items: Vec<Value> = rows
        .iter()
        .map(|row| {
            Ok(json!({
                "step_index": row.get_typed::<i64>(0)?,
                "anchor": anchor_json(row.get_typed(2)?, Some(row.get_typed(1)?)),
                "part_key": row.get_typed::<Option<String>>(3)?,
            }))
        })
        .collect::<Result<_>>()?;
    Ok(json!({"state": "available", "items": items}))
}

/// `files`: recorded file-path evidence from verified tool calls. Paths
/// only — artifact bytes are never served on the canonical lane.
fn files_section(connection: &Connection, conversation_id: i64) -> Result<Value> {
    let Some(stale) = projection_marker(connection, conversation_id)? else {
        let (state, reason) = projection_state(None);
        return Ok(json!({"state": state, "reason": reason}));
    };
    if stale {
        let (state, reason) = projection_state(Some(true));
        return Ok(json!({"state": state, "reason": reason}));
    }
    let rows = connection.query_with_params(
        "SELECT file_path,call_id,step_index,call_idx,call_message_id \
         FROM viewer_tool_calls WHERE conversation_id = ?1 \
         AND file_path IS NOT NULL ORDER BY ordinal",
        &param_slice_to_values(params![conversation_id]),
    )?;
    let items: Vec<Value> = rows
        .iter()
        .map(|row| {
            Ok(json!({
                "file_path": row.get_typed::<Option<String>>(0)?,
                "call_id": row.get_typed::<String>(1)?,
                "step_index": row.get_typed::<Option<i64>>(2)?,
                "anchor": anchor_json(row.get_typed(4)?, Some(row.get_typed(3)?)),
            }))
        })
        .collect::<Result<_>>()?;
    if items.is_empty() {
        return Ok(json!({
            "state": "unavailable",
            "reason": "no_recorded_file_evidence",
            "items": items,
        }));
    }
    Ok(json!({
        "state": "partial",
        "reason": "artifact_file_content_not_served_on_canonical_lane",
        "items": items,
    }))
}

/// `metric_quality`/`metric_sources` port of `summary.py`: every emitted
/// metric names its known/unknown quality and its provenance column;
/// metrics canonical + `viewer_*` cannot express keep the oracle's
/// `unavailable:canonical_catalog` marker. Unknown is never a zero.
fn metric_quality(map: &Map<String, Value>, facts: Option<&FactsRow>) -> (Value, Value) {
    let mut quality = Map::new();
    let mut sources = Map::new();
    for key in [
        "title",
        "primary_model",
        "started_at_ms",
        "ended_at_ms",
        "last_message_created_at_ms",
        "last_message_idx",
        "activity_ms",
        "duration_seconds",
        "approx_tokens",
        "total_input_tokens",
        "total_output_tokens",
        "total_cache_read_tokens",
        "total_cache_creation_tokens",
        "grand_total_tokens",
        "estimated_cost_usd",
        "api_call_count",
        "tool_call_count",
        "user_message_count",
        "assistant_message_count",
    ] {
        let known = map.get(key).is_some_and(|value| !value.is_null());
        quality.insert(
            key.to_owned(),
            json!(if known { "known" } else { "unknown" }),
        );
        sources.insert(key.to_owned(), json!(format!("cass.conversations.{key}")));
    }
    let facts_present = facts.is_some();
    for (key, column, value) in [
        ("turn_count", "turn_count", facts.and_then(|f| f.turn_count)),
        ("step_count", "step_count", facts.and_then(|f| f.step_count)),
        (
            "thinking_block_count",
            "thinking_block_count",
            facts.and_then(|f| f.thinking_block_count),
        ),
        (
            "artifact_count",
            "artifact_count",
            facts.and_then(|f| f.artifact_count),
        ),
        (
            "subagent_count",
            "subagent_count",
            facts.and_then(|f| f.subagent_count),
        ),
        (
            "tool_error_count",
            "tool_error_count",
            facts.and_then(|f| f.tool_error_count),
        ),
        (
            "tool_calls_with_exit_status",
            "tool_calls_with_exit_status",
            facts.and_then(|f| f.tool_calls_with_exit_status),
        ),
        (
            "file_fact_count",
            "file_fact_count",
            facts.and_then(|f| f.file_fact_count),
        ),
        (
            "compaction_count",
            "compaction_count",
            facts.and_then(|f| f.compaction_count),
        ),
    ] {
        quality.insert(
            key.to_owned(),
            json!(if value.is_some() { "known" } else { "unknown" }),
        );
        sources.insert(
            key.to_owned(),
            json!(if facts_present {
                format!("viewer_session_facts.{column}")
            } else {
                "unavailable:facts_not_persisted".to_owned()
            }),
        );
    }
    quality.insert(
        "first_prompt".to_owned(),
        json!(if facts.and_then(|f| f.first_prompt.clone()).is_some() {
            "known"
        } else {
            "unknown"
        }),
    );
    sources.insert(
        "first_prompt".to_owned(),
        json!(if !facts_present {
            "unavailable:facts_not_persisted"
        } else if facts.is_some_and(|f| !f.first_prompt_column) {
            "unavailable:first_prompt_column_pending"
        } else {
            "viewer_session_facts.first_prompt"
        }),
    );
    // The oracle's unmeasured set: not expressible from canonical columns
    // and not yet persisted facts — unknown, never zero.
    for key in [
        "message_count",
        "has_subagents",
        "tools_used",
        "has_error",
        "has_security_alert",
        "anomalies_count",
        "cache_hit_rate",
        "health_grade",
        "health_score",
        "health_issues_count",
        "potential_waste_tokens",
        "potential_savings_usd",
    ] {
        quality.entry(key.to_owned()).or_insert(json!("unknown"));
        sources
            .entry(key.to_owned())
            .or_insert(json!("unavailable:canonical_catalog"));
    }
    for (key, value) in [
        ("is_subagent", facts.and_then(|f| f.is_subagent)),
        (
            "task_category",
            facts.and_then(|f| f.task_category.clone()).map(|_| 1_i64),
        ),
    ] {
        quality.insert(
            key.to_owned(),
            json!(if value.is_some() { "known" } else { "unknown" }),
        );
        sources.insert(
            key.to_owned(),
            json!(if facts_present {
                "viewer_session_facts.classification"
            } else {
                "unavailable:facts_not_persisted"
            }),
        );
    }
    (Value::Object(quality), Value::Object(sources))
}

/// `summary` = the protocol-1 canonical session decode plus the verified
/// identity, annotation, classification and facts overlays.
fn summary_section(
    session: &Value,
    verified: &identity::VerifiedIdentity,
    annotation: Option<&Value>,
    facts: Option<&FactsRow>,
) -> Result<Value> {
    let mut map = session.as_object().cloned().unwrap_or_default();
    let id = map
        .remove("id")
        .as_ref()
        .and_then(Value::as_i64)
        .unwrap_or_default();
    map.insert("conversation_id".into(), json!(id));
    map.insert("public_id".into(), json!(verified.public_id));
    map.insert("provider".into(), json!(verified.provider));
    let project = map
        .get("workspace_path")
        .and_then(Value::as_str)
        .map(identity::path_name)
        .filter(|name| !name.is_empty());
    map.insert("project".into(), json!(project));
    let activity = ["last_message_created_at_ms", "ended_at_ms", "started_at_ms"]
        .iter()
        .find_map(|key| map.get(*key).and_then(Value::as_i64));
    map.insert("activity_ms".into(), json!(activity));
    let duration = match (
        map.get("started_at_ms").and_then(Value::as_i64),
        map.get("ended_at_ms").and_then(Value::as_i64),
    ) {
        (Some(started), Some(ended)) if ended >= started => Some((ended - started) as f64 / 1000.0),
        _ => None,
    };
    map.insert("duration_seconds".into(), json!(duration));

    map.insert(
        "is_pinned".into(),
        json!(
            annotation
                .and_then(|a| a["is_pinned"].as_bool())
                .unwrap_or(false)
        ),
    );
    map.insert(
        "custom_title".into(),
        annotation
            .and_then(|a| a.get("custom_title").cloned())
            .unwrap_or(Value::Null),
    );
    map.insert(
        "notes".into(),
        annotation
            .and_then(|a| a.get("notes").cloned())
            .unwrap_or(Value::Null),
    );
    map.insert(
        "tags".into(),
        annotation
            .and_then(|a| a.get("tags").cloned())
            .unwrap_or_else(|| json!([])),
    );

    map.insert(
        "is_subagent".into(),
        json!(facts.and_then(|f| f.is_subagent.map(|value| value != 0))),
    );
    map.insert(
        "task_category".into(),
        json!(facts.and_then(|f| f.task_category.clone())),
    );
    map.insert(
        "parent_conversation_id".into(),
        json!(facts.and_then(|f| f.parent_conversation_id)),
    );
    map.insert(
        "classification_coverage".into(),
        json!(
            facts
                .map(|f| f.coverage_state.as_str())
                .unwrap_or("pending")
        ),
    );

    map.insert(
        "metrics".into(),
        json!({
            "turn_count": facts.and_then(|f| f.turn_count),
            "step_count": facts.and_then(|f| f.step_count),
            "thinking_block_count": facts.and_then(|f| f.thinking_block_count),
            "artifact_count": facts.and_then(|f| f.artifact_count),
            "subagent_count": facts.and_then(|f| f.subagent_count),
            "tool_error_count": facts.and_then(|f| f.tool_error_count),
            "tool_calls_with_exit_status": facts.and_then(|f| f.tool_calls_with_exit_status),
            "file_fact_count": facts.and_then(|f| f.file_fact_count),
            "compaction_count": facts.and_then(|f| f.compaction_count),
            "first_prompt": facts.and_then(|f| f.first_prompt.clone()),
            "model_set": facts.and_then(|f| f.model_set.clone()),
        }),
    );
    let (quality, sources) = metric_quality(&map, facts);
    map.insert("metric_quality".into(), quality);
    map.insert("metric_sources".into(), sources);
    Ok(Value::Object(map))
}

/// `get_session_details(conversation_id, sections, cursor, limit)` — one
/// snapshot-bounded detail read for ONE addressed conversation.
/// `generation` binds the logical revisions the page was read at; the
/// caller stamps `archive_stamp`/`catalog_revision`.
pub fn session_details(
    connection: &Connection,
    conversation_id: i64,
    sections: &[String],
    cursor: Option<&str>,
    limit: usize,
) -> Result<Value> {
    if conversation_id <= 0 {
        return Err(refuse(
            "invalid_request",
            "conversation_id must be positive",
        ));
    }
    if !revisions::extensions_installed(connection)? {
        return Err(refuse(
            "viewer_extensions_missing",
            "The viewer extension profile is not installed; run --init-extensions first",
        ));
    }
    let Some(session) = catalog::read_session(connection, conversation_id)? else {
        return Err(refuse(
            "session_not_found",
            "No canonical conversation carries this ID",
        ));
    };
    let requested = resolve_sections(sections)?;
    let wants_page = requested
        .iter()
        .any(|name| PAGE_SECTIONS.contains(&name.as_str()));
    if cursor.is_some() && !wants_page {
        return Err(refuse(
            "invalid_request",
            "A detail cursor only continues a paged section (messages/raw/attachments)",
        ));
    }
    let after_idx = cursor.map(decode_detail_cursor).transpose()?;
    let page = wants_page
        .then(|| catalog::read_messages_page_body(connection, conversation_id, after_idx, limit))
        .transpose()?;

    let verified = identity::resolve_public_id(connection, conversation_id)?;
    let annotation = annotations::read_annotation(connection, conversation_id)?;
    let facts = read_facts(connection, conversation_id)?;
    let generation = revisions::generation(connection)?;

    let mut out = Map::new();
    for name in &requested {
        let value = match name.as_str() {
            "summary" => summary_section(&session, &verified, annotation.as_ref(), facts.as_ref())?,
            "messages" => messages_section(page.as_ref().expect("messages section paged")),
            "raw" => raw_section(page.as_ref().expect("raw section paged"))?,
            "facts" => facts_section(facts.as_ref()),
            "availability" => availability_section(facts.as_ref()),
            "annotations" => annotation.clone().unwrap_or(Value::Null),
            "attachments" => attachments_section(page.as_ref().expect("attachments section paged")),
            "snippets" => snippets_section(connection, conversation_id)?,
            "steps" => steps_section(connection, conversation_id)?,
            "turns" => turns_section(connection, conversation_id)?,
            "tools" => tools_section(connection, conversation_id)?,
            "thinking" => thinking_section(connection, conversation_id)?,
            "files" => files_section(connection, conversation_id)?,
            other => pending_section(other),
        };
        out.insert(name.clone(), value);
    }
    let next_cursor = page
        .as_ref()
        .and_then(|page| page["next_idx"].as_i64())
        .map(|idx| json!({"m": idx}).to_string());
    Ok(json!({
        "conversation_id": conversation_id,
        "public_id": verified.public_id,
        "provider": verified.provider,
        "source_id": verified.source_id,
        "sections": Value::Object(out),
        "next_cursor": next_cursor.map_or(Value::Null, Value::String),
        "generation": generation,
        "complete": true,
    }))
}

// ---------------------------------------------------------------------------
// get_tool_output: one recorded tool output at a verified message anchor
// ---------------------------------------------------------------------------

/// Oracle `recorded_text` (facts `providers/mod.rs` keeps the same rule in
/// a `pub(crate)` module unreachable from this binary): explicit textual
/// content plus a complete/unknown-part signal.
fn recorded_text(value: &Value) -> (Option<String>, bool) {
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

/// Oracle `explicit_status`: a recorded integer exit code can establish an
/// error; a generic error flag never invents `exit = 1`.
fn explicit_status(record: &Value) -> (Option<i64>, Option<bool>) {
    let exit_code = record
        .get("exit_code")
        .or_else(|| record.get("exitCode"))
        .and_then(Value::as_i64);
    let mut is_error = record
        .get("is_error")
        .or_else(|| record.get("isError"))
        .and_then(Value::as_bool);
    if let Some(code) = exit_code {
        is_error = Some(code != 0 || is_error == Some(true));
    }
    (exit_code, is_error)
}

struct RecordedOutput {
    text: String,
    complete: bool,
    tool_use_id: Option<String>,
    is_error: Option<bool>,
    exit_code: Option<i64>,
}

/// Collect the recorded tool-output candidates inside ONE canonical
/// message's preserved extra, in recorded order, then serve `block_index`.
/// Recognized shapes (all verbatim recorded evidence, never inferred):
/// normalized `{"source":"tool_result"}` rows (canonical `content` is the
/// output), Claude `toolUseResult` + `sourceToolUseID`, `tool_result`
/// content blocks under `message.content`/`content`, bare `tool_result`
/// records and `toolResults[]`.
fn extract_tool_output(item: &Value, block_index: usize) -> Result<RecordedOutput> {
    let extra = &item["extra"];
    let value = &extra["value"];
    let content = item["content"].as_str().unwrap_or("");
    let mut candidates: Vec<(Value, Option<String>, Option<i64>, Option<bool>)> = Vec::new();

    if value.get("source").and_then(Value::as_str) == Some("tool_result") {
        let call_id = value
            .get("tool_use_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let recorded = if content.is_empty() {
            value
                .get("output")
                .or_else(|| value.get("content"))
                .cloned()
                .unwrap_or(Value::Null)
        } else {
            Value::String(content.to_owned())
        };
        let (_, is_error) = explicit_status(value);
        candidates.push((recorded, call_id, None, is_error));
    }
    if let Some(result) = value.get("toolUseResult") {
        let call_id = value
            .get("sourceToolUseID")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let recorded = if result.is_object() {
            result
                .get("output")
                .or_else(|| result.get("content"))
                .cloned()
                .unwrap_or(Value::Null)
        } else {
            result.clone()
        };
        let (exit_code, is_error) = explicit_status(result);
        candidates.push((recorded, call_id, exit_code, is_error));
    }
    if value.get("type").and_then(Value::as_str) == Some("tool_result") {
        let call_id = value
            .get("tool_use_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let (exit_code, is_error) = explicit_status(value);
        candidates.push((
            value.get("content").cloned().unwrap_or(Value::Null),
            call_id,
            exit_code,
            is_error,
        ));
    }
    for container in [value.pointer("/message/content"), value.get("content")]
        .into_iter()
        .flatten()
    {
        let items: Vec<&Value> = match container {
            Value::Array(list) => list.iter().collect(),
            other => vec![other],
        };
        for block in items {
            if block.get("type").and_then(Value::as_str) == Some("tool_result") {
                let call_id = block
                    .get("tool_use_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                let (exit_code, is_error) = explicit_status(block);
                candidates.push((
                    block.get("content").cloned().unwrap_or(Value::Null),
                    call_id,
                    exit_code,
                    is_error,
                ));
            }
        }
    }
    if let Some(results) = value.get("toolResults").and_then(Value::as_array) {
        for result in results {
            let call_id = result
                .get("tool_use_id")
                .or_else(|| result.get("toolUseId"))
                .and_then(Value::as_str)
                .map(str::to_owned);
            candidates.push((
                result
                    .get("content")
                    .or_else(|| result.get("output"))
                    .cloned()
                    .unwrap_or(Value::Null),
                call_id,
                None,
                None,
            ));
        }
    }

    let Some((recorded, tool_use_id, exit_code, is_error)) =
        candidates.into_iter().nth(block_index)
    else {
        return Err(refuse(
            "resource_unavailable",
            "This anchor records no tool output evidence",
        ));
    };
    let (text, complete) = recorded_text(&recorded);
    let Some(text) = text else {
        return Err(refuse(
            "resource_unavailable",
            "Tool output evidence was not recorded",
        ));
    };
    if text.len() > TOOL_OUTPUT_BYTES {
        return Err(refuse(
            "resource_too_large",
            "Recorded tool output exceeds the bounded resource budget",
        ));
    }
    Ok(RecordedOutput {
        text,
        complete,
        tool_use_id,
        is_error,
        exit_code,
    })
}

/// `get_tool_output(conversation_id, step_anchor, expected_revision)` — a
/// bounded text resource at a verified canonical-message anchor. The
/// `expected_revision` must equal the committed logical
/// `canonical_revision`; a stale value is `detail_changed`.
pub fn tool_output(
    connection: &Connection,
    conversation_id: i64,
    anchor: &StepAnchor,
    expected_revision: i64,
) -> Result<Value> {
    if conversation_id <= 0 || expected_revision < 0 {
        return Err(refuse(
            "invalid_request",
            "conversation_id must be positive and expected_revision nonnegative",
        ));
    }
    if !revisions::extensions_installed(connection)? {
        return Err(refuse(
            "viewer_extensions_missing",
            "The viewer extension profile is not installed; run --init-extensions first",
        ));
    }
    let exists = connection.query_with_params(
        "SELECT 1 FROM conversations WHERE id = ?1 LIMIT 1",
        &param_slice_to_values(params![conversation_id]),
    )?;
    if exists.is_empty() {
        return Err(refuse(
            "session_not_found",
            "No canonical conversation carries this ID",
        ));
    }
    let generation = revisions::generation(connection)?.ok_or_else(|| {
        refuse(
            "viewer_extensions_missing",
            "The viewer extension profile is not installed",
        )
    })?;
    if expected_revision != generation.canonical_revision {
        return Err(refuse(
            "detail_changed",
            format!(
                "canonical_revision {expected_revision} is stale; \
                 committed canonical_revision is {}",
                generation.canonical_revision
            ),
        ));
    }
    if anchor.kind != "message" {
        return Err(refuse(
            "anchor_unsupported",
            format!(
                "Anchor kind '{}' cannot be verified on this lane: step/tool \
                 anchors land with the Task-8 projection; only kind=message \
                 is served",
                anchor.kind
            ),
        ));
    }
    let block_index = anchor.block_index.unwrap_or(0);
    if !(0..=MAX_BLOCK_INDEX).contains(&block_index) {
        return Err(refuse("invalid_request", "block_index must be 0..=64"));
    }
    let (column, coordinate) = match (anchor.message_id, anchor.idx) {
        (Some(id), None) if id > 0 => ("id", id),
        (None, Some(idx)) if idx >= 0 => ("idx", idx),
        _ => {
            return Err(refuse(
                "invalid_request",
                "A message anchor needs exactly one of message_id or idx",
            ));
        }
    };
    let rows = connection.query_with_params(
        &format!(
            "SELECT {} FROM messages WHERE conversation_id = ?1 AND {column} = ?2 LIMIT 2",
            catalog::MSG_GUARDED_COLUMNS
        ),
        &param_slice_to_values(params![conversation_id, coordinate]),
    )?;
    if rows.len() > 1 {
        return Err(refuse(
            "canonical_coordinates_invalid",
            "Ambiguous canonical message coordinates",
        ));
    }
    let Some(row) = rows.first() else {
        return Err(refuse(
            "resource_unavailable",
            "No canonical message carries this anchor",
        ));
    };
    let item = catalog::decode_message_row(row)?;
    let output = extract_tool_output(&item, block_index as usize)?;
    let extra_complete = item["extra"]["complete"].as_bool().unwrap_or(false);
    let availability = if extra_complete && output.complete {
        "available"
    } else {
        "partial"
    };
    Ok(json!({
        "content": output.text,
        "media_type": "text/plain; charset=utf-8",
        "availability": availability,
        "origin": "canonical_raw",
        "canonical_revision": generation.canonical_revision,
        "generation": generation,
        "anchor": {
            "kind": "message",
            "message_id": item["id"],
            "idx": item["idx"],
            "block_index": block_index,
        },
        "tool_use_id": output.tool_use_id,
        "is_error": output.is_error,
        "exit_code": output.exit_code,
        "extra_issues": item["extra"]["issues"],
    }))
}
