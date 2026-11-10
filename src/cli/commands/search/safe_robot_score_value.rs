use crate::cli::commands::view::role_to_export_string::canonical_followup_source_id;
use tracing::warn;

/// Filter a search hit to only include the requested fields
pub(crate) fn safe_robot_score_value(score: f32) -> serde_json::Value {
    serde_json::Value::from(if score.is_finite() { score as f64 } else { 0.0 })
}

pub(crate) fn normalized_robot_hit_source_id(hit: &crate::search::query::SearchHit) -> String {
    if let Some(source_id) = canonical_followup_source_id(Some(hit.source_id.as_str())) {
        return source_id;
    }
    if hit.origin_kind.trim().eq_ignore_ascii_case("local") {
        return crate::sources::provenance::LOCAL_SOURCE_ID.to_string();
    }
    hit.source_id.trim().to_string()
}

pub(crate) fn normalized_robot_hit_origin_kind(hit: &crate::search::query::SearchHit) -> String {
    let trimmed = hit.origin_kind.trim();
    if !trimmed.is_empty() {
        return trimmed.to_string();
    }
    if normalized_robot_hit_source_id(hit) == crate::sources::provenance::LOCAL_SOURCE_ID {
        return crate::sources::provenance::LOCAL_SOURCE_ID.to_string();
    }
    String::new()
}

pub(crate) fn normalized_robot_hit_origin_host(
    hit: &crate::search::query::SearchHit,
) -> Option<String> {
    hit.origin_host
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub(crate) fn normalized_robot_hit_for_output(
    hit: &crate::search::query::SearchHit,
) -> crate::search::query::SearchHit {
    let mut normalized = hit.clone();
    normalized.source_id = normalized_robot_hit_source_id(hit);
    normalized.origin_kind = normalized_robot_hit_origin_kind(hit);
    normalized.origin_host = normalized_robot_hit_origin_host(hit);
    if !normalized.score.is_finite() {
        normalized.score = 0.0;
    }
    normalized
}

pub(crate) fn projected_hit_field_value(
    hit: &crate::search::query::SearchHit,
    field: &str,
) -> Option<serde_json::Value> {
    match field {
        "score" => Some(safe_robot_score_value(hit.score)),
        "agent" => Some(serde_json::Value::String(hit.agent.clone())),
        "workspace" => Some(serde_json::Value::String(hit.workspace.clone())),
        "source_path" => Some(serde_json::Value::String(hit.source_path.clone())),
        "snippet" => Some(serde_json::Value::String(hit.snippet.clone())),
        "content" => Some(serde_json::Value::String(hit.content.clone())),
        "title" => Some(serde_json::Value::String(hit.title.clone())),
        "created_at" => serde_json::to_value(hit.created_at).ok(),
        "conversation_id" => serde_json::to_value(hit.conversation_id).ok(),
        "line_number" => serde_json::to_value(hit.line_number).ok(),
        "match_type" => serde_json::to_value(hit.match_type).ok(),
        // Provenance fields (P3.4)
        "source_id" => Some(serde_json::Value::String(normalized_robot_hit_source_id(
            hit,
        ))),
        "origin_kind" => Some(serde_json::Value::String(normalized_robot_hit_origin_kind(
            hit,
        ))),
        // Preserve SearchHit serialization semantics:
        // origin_host/workspace_original are omitted entirely when None.
        "origin_host" => normalized_robot_hit_origin_host(hit).map(serde_json::Value::String),
        "workspace_original" => hit
            .workspace_original
            .as_ref()
            .map(|value| serde_json::Value::String(value.clone())),
        _ => None,
    }
}

pub(crate) fn filter_hit_fields(
    hit: &crate::search::query::SearchHit,
    fields: &Option<Vec<String>>,
) -> serde_json::Value {
    // Sanitize NaN/Infinity score before serialization — serde_json rejects non-finite floats.
    let sanitize = |h: &crate::search::query::SearchHit| -> serde_json::Value {
        serde_json::to_value(normalized_robot_hit_for_output(h)).unwrap_or_default()
    };
    match fields {
        None => sanitize(hit),
        Some(field_list) if field_list.is_empty() => sanitize(hit),
        Some(field_list) => {
            let mut filtered = serde_json::Map::new();
            let known_fields = [
                "score",
                "agent",
                "workspace",
                "source_path",
                "snippet",
                "content",
                "title",
                "created_at",
                "conversation_id",
                "line_number",
                "match_type",
                // Provenance fields (P3.4)
                "source_id",
                "origin_kind",
                "origin_host",
                "workspace_original",
            ];

            for field in field_list {
                if let Some(value) = projected_hit_field_value(hit, field) {
                    filtered.insert(field.clone(), value);
                } else if !known_fields.contains(&field.as_str()) {
                    // Warn about unknown fields.
                    warn!(unknown_field = %field, "Unknown field in --fields, ignoring");
                }
            }
            serde_json::Value::Object(filtered)
        }
    }
}

/// Truncate a string to `max_len` characters, UTF-8 safe, with ellipsis.
/// Returns `Some(truncated)` if truncation was needed, `None` otherwise.
///
/// `coding_agent_session_search-nj5eh`: pre-fix this function always
/// walked the full string with `s.chars().count()` to decide whether
/// truncation was needed, then walked it AGAIN with `take()` if it
/// was. For 100KB content with max_len=200 that was ~100K wasted
/// iterations per check. It also always cloned the string in the
/// no-truncation case (`s.to_string()`) even though callers like
/// `apply_content_truncation` only INSERT the result when truncation
/// happened — so for the 1000-hit × 3-field robot path the clone
/// produced ~MB of allocator pressure on the no-truncation hot path.
///
/// Post-fix: short-circuit char counting at `max_len + 1` (bounded
/// by `max_len`, independent of total string length) AND signal
/// "no truncation needed" via `None` so callers skip the wasted
/// allocation entirely. The truncation path captures the cut byte
/// index during the same walk used for short-circuit counting,
/// turning two passes into one.
pub(crate) fn truncate_content(s: &str, max_len: usize) -> Option<String> {
    let truncate_at = max_len.saturating_sub(3);
    let mut chars = s.char_indices();
    // Initialize cut_byte to 0 so the `truncate_at == 0` case (max_len < 3)
    // produces an empty prefix + "..." instead of accidentally using
    // `s.len()` as the cut. The in-loop assignment below overwrites
    // this when the loop reaches the truncate_at index.
    let mut cut_byte: Option<usize> = if truncate_at == 0 { Some(0) } else { None };
    for n in 0..max_len {
        let (byte_idx, _) = chars.next()?;
        if n == truncate_at {
            cut_byte = Some(byte_idx);
        }
    }
    chars.next()?;
    let cut = cut_byte.unwrap_or(s.len());
    let mut out = String::with_capacity(cut + 3);
    out.push_str(&s[..cut]);
    out.push_str("...");
    Some(out)
}
