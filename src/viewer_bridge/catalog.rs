//! Fixed-schema canonical DTOs, including tail state and nullable metrics.
use std::collections::HashMap;

use anyhow::Result;
use coding_agent_search::franken_sync::compat::{RowExt, param_slice_to_values};
use coding_agent_search::franken_sync::{Connection, Row, params};
use serde_json::{Map, Value, json};

use super::extra;
use super::protocol::{RESPONSE_BYTES, page_limit, refuse};
use super::reader::StrictArchive;

const ROW_BYTES: i64 = 512 * 1024;
const CONV_COLUMNS: &str = "id,agent_id,source_id,external_id,source_path,workspace_id,\
    origin_host,title,started_at,ended_at,last_message_idx,last_message_created_at,\
    approx_tokens,total_input_tokens,total_output_tokens,total_cache_read_tokens,\
    total_cache_creation_tokens,grand_total_tokens,estimated_cost_usd,primary_model,\
    api_call_count,tool_call_count,user_message_count,assistant_message_count,\
    metadata_bin,metadata_json";
const CONV_GUARDED_COLUMNS: &str = "id,agent_id,source_id,\
    CASE WHEN COALESCE(length(CAST(external_id AS BLOB)),0)<=524288 THEN external_id ELSE NULL END,\
    CASE WHEN COALESCE(length(CAST(source_path AS BLOB)),0)<=524288 THEN source_path ELSE NULL END,\
    workspace_id,\
    CASE WHEN COALESCE(length(CAST(origin_host AS BLOB)),0)<=524288 THEN origin_host ELSE NULL END,\
    CASE WHEN COALESCE(length(CAST(title AS BLOB)),0)<=524288 THEN title ELSE NULL END,\
    started_at,ended_at,last_message_idx,last_message_created_at,\
    approx_tokens,total_input_tokens,total_output_tokens,total_cache_read_tokens,\
    total_cache_creation_tokens,grand_total_tokens,estimated_cost_usd,\
    CASE WHEN COALESCE(length(CAST(primary_model AS BLOB)),0)<=524288 THEN primary_model ELSE NULL END,\
    api_call_count,tool_call_count,user_message_count,assistant_message_count,\
    CASE WHEN COALESCE(length(metadata_bin),0)<=524288 THEN metadata_bin ELSE NULL END,\
    CASE WHEN COALESCE(length(CAST(metadata_json AS BLOB)),0)<=524288 THEN metadata_json ELSE NULL END,\
    COALESCE(length(CAST(source_id AS BLOB)),0)+\
    COALESCE(length(CAST(external_id AS BLOB)),0)+\
    COALESCE(length(CAST(source_path AS BLOB)),0)+\
    COALESCE(length(CAST(origin_host AS BLOB)),0)+\
    COALESCE(length(CAST(title AS BLOB)),0)+\
    COALESCE(length(CAST(primary_model AS BLOB)),0)+\
    COALESCE(length(metadata_bin),0)+\
    COALESCE(length(CAST(metadata_json AS BLOB)),0)";
const MESSAGE_COLUMNS: &str = "id,conversation_id,idx,role,author,created_at,content,extra_bin,extra_json";
const MSG_GUARDED_COLUMNS: &str = "id,conversation_id,idx,\
    CASE WHEN COALESCE(length(CAST(role AS BLOB)),0)<=524288 THEN role ELSE NULL END,\
    CASE WHEN COALESCE(length(CAST(author AS BLOB)),0)<=524288 THEN author ELSE NULL END,\
    created_at,\
    CASE WHEN COALESCE(length(CAST(content AS BLOB)),0)<=524288 THEN content ELSE NULL END,\
    CASE WHEN COALESCE(length(extra_bin),0)<=524288 THEN extra_bin ELSE NULL END,\
    CASE WHEN COALESCE(length(CAST(extra_json AS BLOB)),0)<=524288 THEN extra_json ELSE NULL END,\
    COALESCE(length(CAST(role AS BLOB)),0)+\
    COALESCE(length(CAST(author AS BLOB)),0)+\
    COALESCE(length(CAST(content AS BLOB)),0)+\
    COALESCE(length(extra_bin),0)+\
    COALESCE(length(CAST(extra_json AS BLOB)),0)";

pub fn check_schema(connection: &Connection) -> Result<()> {
    // These fixed projections verify every required column without reading bodies.
    connection.query(&format!("SELECT {CONV_COLUMNS} FROM conversations WHERE id = 0 LIMIT 0"))?;
    connection.query("SELECT id,slug FROM agents WHERE id = 0 LIMIT 0")?;
    connection.query("SELECT id,path FROM workspaces WHERE id = 0 LIMIT 0")?;
    connection.query("SELECT id,kind,host_label FROM sources WHERE id = '' LIMIT 0")?;
    connection.query(
        "SELECT conversation_id,ended_at,last_message_idx,last_message_created_at          FROM conversation_tail_state WHERE conversation_id = 0 LIMIT 0",
    )?;
    connection.query(&format!("SELECT {MESSAGE_COLUMNS} FROM messages WHERE id = 0 LIMIT 0"))?;
    let migrations = connection.query("PRAGMA table_info(\"_schema_migrations\")")?;
    if !migrations.is_empty() {
        let version: Option<i64> = connection.query_row("SELECT MAX(version) FROM _schema_migrations")?.get_typed(0)?;
        if version != Some(22) {
            return Err(refuse("incompatible_schema", "Canonical schema tracking disagrees with meta version"));
        }
    }
    Ok(())
}

fn required_text(row: &Row, index: usize) -> Result<Value> {
    let value: String = row.get_typed(index)?;
    if value.is_empty() { return Err(refuse("canonical_coordinates_invalid", "Required canonical identity is empty")); }
    Ok(value.into())
}

fn nonnegative_val(value: Option<i64>) -> Result<Value> {
    if value.is_some_and(|n| n < 0) {
        return Err(refuse("canonical_coordinates_invalid", "Negative canonical counter"));
    }
    Ok(json!(value))
}

fn nonnegative(row: &Row, index: usize) -> Result<Value> {
    nonnegative_val(row.get_typed(index)?)
}

type TailTuple = (Option<i64>, Option<i64>, Option<i64>);

#[derive(Default)]
struct SessionLookup {
    agents: HashMap<i64, Option<(String, i64)>>,
    workspaces: HashMap<i64, (Option<String>, i64)>,
    sources: HashMap<String, (Option<String>, Option<String>, i64)>,
    tails: HashMap<i64, TailTuple>,
}

impl SessionLookup {
    fn preload(conn: &Connection, min_id: i64, max_id: i64) -> Result<Self> {
        let mut lookup = Self::default();
        for row in conn.query("SELECT id, slug, COALESCE(length(CAST(slug AS BLOB)),0) FROM agents")? {
            let id: i64 = row.get_typed(0)?;
            let slug: Option<String> = row.get_typed(1)?;
            let len: i64 = row.get_typed(2)?;
            if lookup.agents.insert(id, slug.map(|s| (s, len))).is_some() {
                return Err(refuse("canonical_coordinates_invalid", "Ambiguous canonical row"));
            }
        }
        for row in conn.query("SELECT id, path, COALESCE(length(CAST(path AS BLOB)),0) FROM workspaces")? {
            let id: i64 = row.get_typed(0)?;
            if lookup.workspaces.insert(id, (row.get_typed(1)?, row.get_typed(2)?)).is_some() {
                return Err(refuse("canonical_coordinates_invalid", "Ambiguous canonical row"));
            }
        }
        for row in conn.query(
            "SELECT id, kind, host_label, COALESCE(length(CAST(kind AS BLOB)),0)+COALESCE(length(CAST(host_label AS BLOB)),0) FROM sources",
        )? {
            let id: String = row.get_typed(0)?;
            if lookup.sources.insert(id, (row.get_typed(1)?, row.get_typed(2)?, row.get_typed(3)?)).is_some() {
                return Err(refuse("canonical_coordinates_invalid", "Ambiguous canonical row"));
            }
        }
        for row in conn.query_with_params(
            "SELECT conversation_id,ended_at,last_message_idx,last_message_created_at              FROM conversation_tail_state WHERE conversation_id>=?1 AND conversation_id<=?2",
            &param_slice_to_values(params![min_id, max_id]),
        )? {
            let cid: i64 = row.get_typed(0)?;
            if lookup.tails.insert(cid, (row.get_typed(1)?, row.get_typed(2)?, row.get_typed(3)?)).is_some() {
                return Err(refuse("canonical_coordinates_invalid", "Ambiguous canonical row"));
            }
        }
        Ok(lookup)
    }

    fn agent(&self, id: Option<i64>) -> (Option<String>, i64) {
        id.and_then(|k| self.agents.get(&k)).and_then(Clone::clone).map_or((None, 0), |(s, n)| (Some(s), n))
    }

    fn workspace(&self, id: Option<i64>) -> (Option<String>, i64) {
        id.and_then(|k| self.workspaces.get(&k)).cloned().unwrap_or((None, 0))
    }

    fn source(&self, id: Option<&str>) -> (Option<String>, Option<String>, i64) {
        id.and_then(|k| self.sources.get(k)).cloned().unwrap_or((None, None, 0))
    }
}

fn decode_session_row(row: &Row, lookup: &SessionLookup) -> Result<Value> {
    let row_id: i64 = row.get_typed(0)?;
    if row_id <= 0 { return Err(refuse("canonical_coordinates_invalid", "Nonpositive conversation identity")); }
    let agent_id: Option<i64> = row.get_typed(1)?;
    let source_id_opt: Option<String> = row.get_typed(2)?;
    let workspace_id: Option<i64> = row.get_typed(5)?;
    let conv_bytes: i64 = row.get_typed(26)?;
    let (agent_slug, agent_bytes) = lookup.agent(agent_id);
    let (workspace_path, workspace_bytes) = lookup.workspace(workspace_id);
    let (source_kind, host_label, source_bytes) = lookup.source(source_id_opt.as_deref());
    let total_bytes = conv_bytes + agent_bytes + workspace_bytes + source_bytes;
    if !(0..=ROW_BYTES).contains(&total_bytes) {
        return Err(refuse("canonical_payload_too_large", "Canonical row exceeds the strict bridge byte profile"));
    }
    let slug = agent_slug.ok_or_else(|| anyhow::anyhow!("Missing required agent slug"))?;
    if slug.is_empty() { return Err(refuse("canonical_coordinates_invalid", "Required canonical identity is empty")); }
    let (ended_at, last_message_idx, last_message_created_at) = match lookup.tails.get(&row_id) {
        Some(ts) => *ts,
        None => (row.get_typed::<Option<i64>>(9)?, row.get_typed::<Option<i64>>(10)?, row.get_typed::<Option<i64>>(11)?),
    };
    let mut value = Map::new();
    value.insert("id".into(), json!(row_id));
    value.insert("agent_slug".into(), Value::String(slug));
    value.insert("source_id".into(), required_text(row, 2)?);
    value.insert("source_path".into(), required_text(row, 4)?);
    value.insert("source_kind".into(), json!(source_kind));
    value.insert("external_id".into(), json!(row.get_typed::<Option<String>>(3)?));
    value.insert("workspace_path".into(), json!(workspace_path));
    value.insert("origin_host".into(), json!(row.get_typed::<Option<String>>(6)?.or(host_label)));
    value.insert("title".into(), json!(row.get_typed::<Option<String>>(7)?));
    value.insert("primary_model".into(), json!(row.get_typed::<Option<String>>(19)?));
    value.insert("started_at_ms".into(), json!(row.get_typed::<Option<i64>>(8)?));
    value.insert("ended_at_ms".into(), json!(ended_at));
    value.insert("last_message_created_at_ms".into(), json!(last_message_created_at));
    value.insert("last_message_idx".into(), nonnegative_val(last_message_idx)?);
    for (key, index) in [("approx_tokens",12),("total_input_tokens",13),
        ("total_output_tokens",14),("total_cache_read_tokens",15),("total_cache_creation_tokens",16),
        ("grand_total_tokens",17),("api_call_count",20),("tool_call_count",21),
        ("user_message_count",22),("assistant_message_count",23)] {
        value.insert(key.into(), nonnegative(row, index)?);
    }
    let cost: Option<f64> = row.get_typed(18)?;
    if cost.is_some_and(|cost| !cost.is_finite() || cost < 0.0) {
        return Err(refuse("canonical_coordinates_invalid", "Invalid canonical cost"));
    }
    value.insert("estimated_cost_usd".into(), json!(cost));
    value.insert("metadata".into(), serde_json::to_value(extra::decode(row.get_typed(24)?, row.get_typed(25)?))?);
    Ok(Value::Object(value))
}

fn read_session(connection: &Connection, id: i64) -> Result<Option<Value>> {
    let rows = connection.query_with_params(
        &format!("SELECT {CONV_GUARDED_COLUMNS} FROM conversations WHERE id=?1 LIMIT 2"),
        &param_slice_to_values(params![id]),
    )?;
    if rows.len() > 1 { return Err(refuse("canonical_coordinates_invalid", "Ambiguous canonical row")); }
    let Some(row) = rows.first() else { return Ok(None); };
    let lookup = SessionLookup::preload(connection, id, id)?;
    Ok(Some(decode_session_row(row, &lookup)?))
}

fn decode_message_row(row: &Row) -> Result<Value> {
    let bytes: i64 = row.get_typed(9)?;
    if !(0..=ROW_BYTES).contains(&bytes) {
        return Err(refuse("canonical_payload_too_large", "Canonical row exceeds the strict bridge byte profile"));
    }
    let id: i64 = row.get_typed(0)?;
    let conversation_id: i64 = row.get_typed(1)?;
    let idx: i64 = row.get_typed(2)?;
    if id <= 0 || conversation_id <= 0 || idx < 0 {
        return Err(refuse("canonical_coordinates_invalid", "Invalid canonical message coordinates"));
    }
    Ok(json!({
        "id":id,"conversation_id":conversation_id,"idx":idx,"role":required_text(row,3)?,
        "author":row.get_typed::<Option<String>>(4)?,"created_at_ms":row.get_typed::<Option<i64>>(5)?,
        "content":row.get_typed::<String>(6)?,"extra":extra::decode(row.get_typed(7)?,row.get_typed(8)?),
    }))
}

fn push_bounded(rows: &mut Vec<Value>, row: Value, bytes: &mut usize) -> Result<bool> {
    let row_bytes = serde_json::to_vec(&row)?.len() + 2;
    if *bytes + row_bytes > RESPONSE_BYTES - 8192 {
        if rows.is_empty() {
            return Err(refuse("canonical_payload_too_large", "Page exceeds bridge response budget; request fewer rows"));
        }
        return Ok(false);
    }
    *bytes += row_bytes;
    rows.push(row);
    Ok(true)
}

impl StrictArchive {
    pub fn read_catalog_page(&mut self, after_id: Option<i64>, limit: usize) -> Result<Value> {
        page_limit(limit)?;
        if after_id.is_some_and(|id| id < 1) { return Err(refuse("invalid_request", "after_id must be positive")); }
        let (mut page, revision) = self.snapshot(|connection| {
            let limit_plus_one = (limit + 1) as i64;
            let conv_rows = if let Some(after) = after_id {
                connection.query_with_params(
                    &format!("SELECT {CONV_GUARDED_COLUMNS} FROM conversations WHERE id > ?1 ORDER BY id LIMIT ?2"),
                    &param_slice_to_values(params![after, limit_plus_one]),
                )?
            } else {
                connection.query_with_params(
                    &format!("SELECT {CONV_GUARDED_COLUMNS} FROM conversations ORDER BY id LIMIT ?1"),
                    &param_slice_to_values(params![limit_plus_one]),
                )?
            };
            if conv_rows.is_empty() {
                return Ok(json!({"rows":Vec::<Value>::new(),"next_conversation_id":None::<i64>}));
            }
            let take_count = conv_rows.len().min(limit);
            let min_id: i64 = conv_rows[0].get_typed(0)?;
            let max_id: i64 = conv_rows[take_count - 1].get_typed(0)?;
            let lookup = SessionLookup::preload(connection, min_id, max_id)?;
            let (mut rows, mut bytes, mut last, mut truncated) = (Vec::new(), 0, None, false);
            for row in conv_rows.iter().take(limit) {
                let id: i64 = row.get_typed(0)?;
                let value = decode_session_row(row, &lookup)?;
                if !push_bounded(&mut rows, value, &mut bytes)? {
                    truncated = true;
                    break;
                }
                last = Some(id);
            }
            let has_more = truncated || conv_rows.len() > limit;
            Ok(json!({"rows":rows,"next_conversation_id":if has_more { last } else { None }}))
        })?;
        page["archive_stamp"] = serde_json::to_value(&self.stamp)?;
        page["catalog_revision"] = revision.into();
        Ok(page)
    }

    pub fn session(&mut self, conversation_id: i64) -> Result<Value> {
        if conversation_id <= 0 { return Err(refuse("invalid_request", "conversation_id must be positive")); }
        let (row, revision) = self.snapshot(|connection| read_session(connection, conversation_id))?;
        Ok(json!({"row":row,"archive_stamp":self.stamp,"catalog_revision":revision}))
    }

    pub fn read_messages_page(&mut self, conversation_id: i64, after_idx: Option<i64>, limit: usize) -> Result<Value> {
        page_limit(limit)?;
        if conversation_id <= 0 || after_idx.is_some_and(|idx| idx < 0) {
            return Err(refuse("invalid_request", "Invalid conversation/message coordinates"));
        }
        let (mut page, revision) = self.snapshot(|connection| {
            let limit_plus_one = (limit + 1) as i64;
            let msg_rows = if let Some(after) = after_idx {
                connection.query_with_params(
                    &format!("SELECT {MSG_GUARDED_COLUMNS} FROM messages WHERE conversation_id=?1 AND idx > ?2 ORDER BY idx,id LIMIT ?3"),
                    &param_slice_to_values(params![conversation_id, after, limit_plus_one]),
                )?
            } else {
                connection.query_with_params(
                    &format!("SELECT {MSG_GUARDED_COLUMNS} FROM messages WHERE conversation_id=?1 ORDER BY idx,id LIMIT ?2"),
                    &param_slice_to_values(params![conversation_id, limit_plus_one]),
                )?
            };
            let (mut items, mut bytes, mut last, mut truncated) = (Vec::new(), 0, after_idx.unwrap_or(-1), false);
            for (offset, row) in msg_rows.iter().enumerate() {
                let idx: i64 = row.get_typed(2)?;
                if idx <= last { return Err(refuse("canonical_coordinates_invalid", "Duplicated or unordered message index")); }
                if offset == limit { break; }
                if !push_bounded(&mut items, decode_message_row(row)?, &mut bytes)? {
                    truncated = true;
                    break;
                }
                last = idx;
            }
            let has_more = truncated || msg_rows.len() > limit;
            Ok(json!({"items":items,"next_idx":if has_more { Some(last) } else { None }}))
        })?;
        page["archive_stamp"] = serde_json::to_value(&self.stamp)?;
        page["catalog_revision"] = revision.into();
        Ok(page)
    }
}
