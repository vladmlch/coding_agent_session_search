//! Read/validate helpers for the `viewer_annotations`/`viewer_tags` lane.
//!
//! `sanitize_tag` is a Rust port of the Viewer's
//! `viewer/storage/sanitizer.py::sanitize_tag` (which itself mirrors the
//! Rust `crates/viewer-storage/src/sanitizer.rs` implementation): trim,
//! strip leading `#`, lowercase alnum / keep `_`, fold other characters to
//! `-`, collapse and trim dashes, cap at 40 unicode characters. The writer
//! stores only sanitized tags; the list filter sanitizes the query input so
//! a raw `"My Tag!"` and the stored `"my-tag"` agree.

use std::collections::BTreeMap;

use anyhow::Result;
use coding_agent_search::franken_sync::compat::{RowExt, param_slice_to_values};
use coding_agent_search::franken_sync::{Connection, Row, SqliteValue, params};
use serde_json::{Value, json};

use super::protocol::refuse;

/// `viewer/storage/sanitizer.py::TAG_MAX_CHARS`.
pub const TAG_MAX_CHARS: usize = 40;

/// `viewer/storage/sanitizer.py::sanitize_tag`. Empty results are returned
/// as `""` and must be discarded by callers (mirroring the oracle).
pub fn sanitize_tag(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    let no_hash = trimmed.trim_start_matches('#');
    if no_hash.is_empty() {
        return String::new();
    }
    let mut out = String::new();
    let mut last_was_dash = false;
    for ch in no_hash.chars() {
        if ch.is_alphanumeric() || ch == '_' {
            out.extend(ch.to_lowercase());
            last_was_dash = false;
        } else if !last_was_dash && !out.is_empty() {
            out.push('-');
            last_was_dash = true;
        }
    }
    let cleaned = out.trim_end_matches('-');
    let truncated: String = cleaned.chars().take(TAG_MAX_CHARS).collect();
    truncated.trim_end_matches('-').to_owned()
}

/// `sanitize_tags`: sanitize + deduplicate preserving insertion order.
pub fn sanitize_tags(raw: &[String]) -> Vec<String> {
    let mut seen = std::collections::BTreeSet::new();
    let mut out = Vec::new();
    for tag in raw {
        let clean = sanitize_tag(tag);
        if !clean.is_empty() && seen.insert(clean.clone()) {
            out.push(clean);
        }
    }
    out
}

/// Decode a `viewer_annotations` row to the wire annotation shape. Shared
/// by `get_annotations` and the writer receipt.
pub fn annotation_json(conversation_id: i64, row: &Row) -> Result<Value> {
    Ok(json!({
        "conversation_id": conversation_id,
        "is_pinned": row.get_typed::<i64>(0)? != 0,
        "custom_title": row.get_typed::<Option<String>>(1)?,
        "notes": row.get_typed::<Option<String>>(2)?,
        "revision": row.get_typed::<i64>(3)?,
        "updated_at_ms": row.get_typed::<Option<i64>>(4)?,
    }))
}

/// Tags for a bounded set of conversations (`IN`-list, PK/conversation
/// seek, ordered). Used by `get_annotations` and the `list_sessions`
/// overlay; each page binds at most `PAGE_LIMIT + 1` ids.
pub fn tags_for(
    connection: &Connection,
    conversation_ids: &[i64],
) -> Result<BTreeMap<i64, Vec<String>>> {
    let mut out: BTreeMap<i64, Vec<String>> = BTreeMap::new();
    if conversation_ids.is_empty() {
        return Ok(out);
    }
    let mut params = Vec::with_capacity(conversation_ids.len());
    let mut placeholders = String::new();
    for (index, id) in conversation_ids.iter().enumerate() {
        if index > 0 {
            placeholders.push(',');
        }
        placeholders.push_str(&format!("?{}", index + 1));
        params.push(SqliteValue::Integer(*id));
    }
    let rows = connection.query_with_params(
        &format!(
            "SELECT conversation_id, tag FROM viewer_tags \
             WHERE conversation_id IN ({placeholders}) \
             ORDER BY conversation_id, tag"
        ),
        &params,
    )?;
    for row in rows {
        let conversation_id: i64 = row.get_typed(0)?;
        let tag: String = row.get_typed(1)?;
        out.entry(conversation_id).or_default().push(tag);
    }
    Ok(out)
}

/// Annotation state for one conversation (`viewer_annotations` +
/// `viewer_tags`). `None` when neither an annotation row nor tags exist;
/// a tag-only conversation still reports an annotation-shaped object with
/// `revision: 0` (the CAS token for an absent annotation row).
pub fn read_annotation(connection: &Connection, conversation_id: i64) -> Result<Option<Value>> {
    let rows = connection.query_with_params(
        "SELECT is_pinned,custom_title,notes,revision,updated_at_ms \
         FROM viewer_annotations WHERE conversation_id = ?1 LIMIT 2",
        &param_slice_to_values(params![conversation_id]),
    )?;
    if rows.len() > 1 {
        return Err(refuse(
            "canonical_coordinates_invalid",
            "Ambiguous annotation row",
        ));
    }
    let tags = tags_for(connection, &[conversation_id])?
        .remove(&conversation_id)
        .unwrap_or_default();
    match rows.first() {
        Some(row) => {
            let mut value = annotation_json(conversation_id, row)?;
            value["tags"] = json!(tags);
            Ok(Some(value))
        }
        None if !tags.is_empty() => Ok(Some(json!({
            "conversation_id": conversation_id,
            "is_pinned": false,
            "custom_title": Value::Null,
            "notes": Value::Null,
            "revision": 0,
            "updated_at_ms": Value::Null,
            "tags": tags,
        }))),
        None => Ok(None),
    }
}
