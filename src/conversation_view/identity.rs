use crate::model::types::{Conversation, Message, Workspace};
use crate::search::query::SearchHit;

// -------------------------------------------------------------------------
// Conversation View
// -------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct ConversationView {
    pub convo: Conversation,
    pub messages: Vec<Message>,
    pub workspace: Option<Workspace>,
}

pub(super) fn normalized_ui_source_identity_sql_expr(
    source_id_column: &str,
    origin_host_column: &str,
) -> String {
    format!(
        "CASE WHEN TRIM(COALESCE({source_id_column}, '')) = '' THEN CASE WHEN TRIM(COALESCE({origin_host_column}, '')) = '' THEN '{local}' ELSE TRIM(COALESCE({origin_host_column}, '')) END \
         WHEN LOWER(TRIM(COALESCE({source_id_column}, ''))) = '{local}' THEN '{local}' \
         ELSE TRIM(COALESCE({source_id_column}, '')) END",
        local = crate::sources::provenance::LOCAL_SOURCE_ID,
    )
}

pub(super) fn normalized_ui_source_origin_kind_sql_expr(
    source_id_column: &str,
    origin_kind_column: &str,
    origin_host_column: &str,
) -> String {
    format!(
        "CASE \
            WHEN LOWER(TRIM(COALESCE({origin_kind_column}, ''))) = '{local}' THEN '{local}' \
            WHEN TRIM(COALESCE({origin_kind_column}, '')) != '' THEN 'remote' \
            WHEN LOWER(TRIM(COALESCE({source_id_column}, ''))) = '{local}' THEN '{local}' \
            WHEN TRIM(COALESCE({source_id_column}, '')) != '' THEN 'remote' \
            WHEN TRIM(COALESCE({origin_host_column}, '')) != '' THEN 'remote' \
            ELSE '{local}' \
         END",
        local = crate::sources::provenance::LOCAL_SOURCE_ID,
    )
}

pub(super) fn normalize_ui_source_id_value(source_id: Option<&str>) -> String {
    let trimmed = source_id.unwrap_or_default().trim();
    if trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case(crate::sources::provenance::LOCAL_SOURCE_ID)
    {
        crate::sources::provenance::LOCAL_SOURCE_ID.to_string()
    } else {
        trimmed.to_string()
    }
}

pub(super) fn normalize_ui_source_id_parts(
    source_id: Option<&str>,
    origin_host: Option<&str>,
) -> String {
    let trimmed_source_id = source_id.unwrap_or_default().trim();
    if !trimmed_source_id.is_empty() {
        return normalize_ui_source_id_value(Some(trimmed_source_id));
    }

    origin_host
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| crate::sources::provenance::LOCAL_SOURCE_ID.to_string())
}

/// Normalized provenance identity of a search hit: `source_id` wins when
/// non-blank, then `origin_host`, then a remote `origin_kind` collapses to
/// `"remote"`, else `local`.
pub(super) fn normalize_ui_hit_source_id(hit: &SearchHit) -> String {
    let trimmed_source_id = hit.source_id.trim();
    if !trimmed_source_id.is_empty() {
        return normalize_ui_source_id_value(Some(trimmed_source_id));
    }

    if let Some(host) = hit
        .origin_host
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return host.to_string();
    }

    if hit.origin_kind.trim().eq_ignore_ascii_case("ssh")
        || hit.origin_kind.trim().eq_ignore_ascii_case("remote")
    {
        return "remote".to_string();
    }

    crate::sources::provenance::LOCAL_SOURCE_ID.to_string()
}
