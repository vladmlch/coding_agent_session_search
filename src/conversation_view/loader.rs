use anyhow::Result;
use std::sync::Arc;

use crate::franken_sync::compat::{ConnectionExt, RowExt};
use crate::franken_sync::{FrankenError, Row};
use crate::model::types::{Conversation, Workspace};
use crate::search::query::SearchHit;
use crate::storage::sqlite::FrankenStorage;

use super::cache::CONVERSATION_CACHE;
use super::identity::{
    ConversationView, normalize_ui_hit_source_id, normalize_ui_source_id_parts,
    normalize_ui_source_id_value, normalized_ui_source_identity_sql_expr,
    normalized_ui_source_origin_kind_sql_expr,
};

pub(super) fn storage_cache_scope(storage: &FrankenStorage) -> Option<String> {
    storage
        .database_path()
        .ok()
        .map(|path| path.to_string_lossy().into_owned())
}

fn ui_conversation_row_parts(
    row: &Row,
) -> std::result::Result<(i64, Conversation, Option<Workspace>), FrankenError> {
    let convo_id: i64 = row.get_typed(0)?;
    let workspace_path = row
        .get_typed::<Option<String>>(3)?
        .map(std::path::PathBuf::from);
    let metadata_json = row
        .get_typed::<Option<String>>(11)?
        .and_then(|s| serde_json::from_str(&s).ok())
        .or_else(|| {
            row.get_typed::<Option<Vec<u8>>>(14)
                .ok()
                .flatten()
                .and_then(|b| rmp_serde::from_slice(&b).ok())
        })
        .unwrap_or_default();
    let convo = Conversation {
        id: Some(convo_id),
        agent_slug: row.get_typed(1)?,
        workspace: workspace_path.clone(),
        external_id: row.get_typed(5)?,
        title: row.get_typed(6)?,
        source_path: std::path::PathBuf::from(row.get_typed::<String>(7)?),
        started_at: row.get_typed(8)?,
        ended_at: row.get_typed(9)?,
        approx_tokens: row.get_typed(10)?,
        metadata_json,
        messages: Vec::new(),
        source_id: normalize_ui_source_id_parts(
            row.get_typed::<Option<String>>(12)?.as_deref(),
            row.get_typed::<Option<String>>(13)?.as_deref(),
        ),
        origin_host: row.get_typed(13)?,
    };
    let workspace = row.get_typed::<Option<i64>>(2)?.map(|id| Workspace {
        id: Some(id),
        path: workspace_path.unwrap_or_default(),
        display_name: row.get_typed(4).ok().flatten(),
    });
    Ok((convo_id, convo, workspace))
}

pub fn load_conversation_by_id_uncached(
    storage: &FrankenStorage,
    conversation_id: i64,
) -> Result<Option<ConversationView>> {
    // LEFT JOIN + COALESCE on agents so conversations with NULL agent_id
    // (legacy V1 schema) still load instead of returning "conversation not
    // found" in the UI.  Consistent with 8a0c547c / e1c08e7c.
    let rows = storage.raw().query_map_collect(
        "SELECT c.id, COALESCE(a.slug, 'unknown'), w.id, w.path, w.display_name, c.external_id, c.title, c.source_path,
                c.started_at, c.ended_at, c.approx_tokens, c.metadata_json, c.source_id, c.origin_host, c.metadata_bin
         FROM conversations c
         LEFT JOIN agents a ON c.agent_id = a.id
         LEFT JOIN workspaces w ON c.workspace_id = w.id
         WHERE c.id = ?1
         LIMIT 1",
        crate::franken_sync::params![conversation_id],
        ui_conversation_row_parts,
    )?;
    if let Some((convo_id, convo, workspace)) = rows.into_iter().next() {
        let messages = storage.fetch_messages(convo_id)?;
        return Ok(Some(ConversationView {
            convo,
            messages,
            workspace,
        }));
    }
    Ok(None)
}

// -------------------------------------------------------------------------
// Load Conversation (with caching)
// -------------------------------------------------------------------------

/// Load a conversation from the database (bypassing cache).
/// Use `load_conversation` or `load_conversation_for_source` for cached access.
fn load_conversation_uncached(
    storage: &FrankenStorage,
    source_id: Option<&str>,
    source_path: &str,
) -> Result<Option<ConversationView>> {
    let normalized_source_sql =
        normalized_ui_source_identity_sql_expr("c.source_id", "c.origin_host");
    let normalized_origin_kind_sql =
        normalized_ui_source_origin_kind_sql_expr("c.source_id", "s.kind", "c.origin_host");
    // LEFT JOIN + COALESCE on agents for the same NULL-agent_id safety as
    // load_conversation_by_id_uncached.
    let (sql, params) = if let Some(source_id) = source_id {
        (
            format!(
                "SELECT c.id, COALESCE(a.slug, 'unknown'), w.id, w.path, w.display_name, c.external_id, c.title, c.source_path,
                        c.started_at, c.ended_at, c.approx_tokens, c.metadata_json, c.source_id, c.origin_host, c.metadata_bin
                 FROM conversations c
                 LEFT JOIN agents a ON c.agent_id = a.id
                 LEFT JOIN workspaces w ON c.workspace_id = w.id
                 LEFT JOIN sources s ON c.source_id = s.id
                 WHERE c.source_path = ?1 AND {normalized_source_sql} = ?2
                 ORDER BY c.started_at DESC LIMIT 1"
            ),
            crate::franken_sync::params![source_path, normalize_ui_source_id_value(Some(source_id))],
        )
    } else {
        (
            format!(
                "SELECT c.id, COALESCE(a.slug, 'unknown'), w.id, w.path, w.display_name, c.external_id, c.title, c.source_path,
                        c.started_at, c.ended_at, c.approx_tokens, c.metadata_json, c.source_id, c.origin_host, c.metadata_bin
                 FROM conversations c
                 LEFT JOIN agents a ON c.agent_id = a.id
                 LEFT JOIN workspaces w ON c.workspace_id = w.id
                 LEFT JOIN sources s ON c.source_id = s.id
                 WHERE c.source_path = ?1
                 ORDER BY CASE WHEN {normalized_origin_kind_sql} = '{local}' THEN 0 ELSE 1 END,
                          c.started_at DESC
                 LIMIT 1",
                local = crate::sources::provenance::LOCAL_SOURCE_ID,
            ),
            crate::franken_sync::params![source_path],
        )
    };
    let rows = storage
        .raw()
        .query_map_collect(&sql, params, ui_conversation_row_parts)?;
    if let Some((convo_id, convo, workspace)) = rows.into_iter().next() {
        let messages = storage.fetch_messages(convo_id)?;
        return Ok(Some(ConversationView {
            convo,
            messages,
            workspace,
        }));
    }
    Ok(None)
}

/// Load a conversation with LRU caching.
///
/// This is the primary function for loading conversations in the TUI.
/// It uses a sharded LRU cache to avoid repeated database queries and
/// JSON parsing for the same conversation.
///
/// Cache behavior:
/// - Hit: Returns cached Arc<ConversationView> (fast path)
/// - Miss: Queries database, parses JSON, caches result
///
/// The cache is keyed by source identity and has a configurable capacity
/// via the CASS_CONV_CACHE_SIZE environment variable (default: 256 per shard,
/// 4096 total entries across 16 shards).
fn cached_conversation_matches_lookup_head(
    storage: &FrankenStorage,
    source_id: Option<&str>,
    source_path: &str,
    cached: &ConversationView,
) -> Result<bool> {
    let Some(cached_id) = cached.convo.id else {
        return Ok(false);
    };

    let normalized_source_sql =
        normalized_ui_source_identity_sql_expr("c.source_id", "c.origin_host");
    let normalized_origin_kind_sql =
        normalized_ui_source_origin_kind_sql_expr("c.source_id", "s.kind", "c.origin_host");
    let (sql, params) = if let Some(source_id) = source_id {
        (
            format!(
                "SELECT c.id, {normalized_source_sql}
                 FROM conversations c
                 LEFT JOIN sources s ON c.source_id = s.id
                 WHERE c.source_path = ?1 AND {normalized_source_sql} = ?2
                 ORDER BY c.started_at DESC LIMIT 1"
            ),
            crate::franken_sync::params![
                source_path,
                normalize_ui_source_id_value(Some(source_id))
            ],
        )
    } else {
        (
            format!(
                "SELECT c.id, {normalized_source_sql}
                 FROM conversations c
                 LEFT JOIN sources s ON c.source_id = s.id
                 WHERE c.source_path = ?1
                 ORDER BY CASE WHEN {normalized_origin_kind_sql} = '{local}' THEN 0 ELSE 1 END,
                          c.started_at DESC LIMIT 1",
                local = crate::sources::provenance::LOCAL_SOURCE_ID,
            ),
            crate::franken_sync::params![source_path],
        )
    };

    let rows = storage.raw().query_map_collect(&sql, params, |row: &Row| {
        Ok((row.get_typed::<i64>(0)?, row.get_typed::<String>(1)?))
    })?;

    Ok(
        matches!(rows.into_iter().next(), Some((latest_id, latest_source_id)) if latest_id == cached_id && latest_source_id == cached.convo.source_id),
    )
}

pub fn load_conversation(
    storage: &FrankenStorage,
    source_path: &str,
) -> Result<Option<ConversationView>> {
    let cache_scope = storage_cache_scope(storage);

    // Fast path: check cache first
    if let Some(scope) = cache_scope.as_deref()
        && let Some(cached) = CONVERSATION_CACHE.get_scoped(scope, None, source_path)
    {
        match cached_conversation_matches_lookup_head(storage, None, source_path, &cached) {
            Ok(true) => {
                // Clone out of Arc for API compatibility
                return Ok(Some((*cached).clone()));
            }
            Ok(false) => {
                CONVERSATION_CACHE.invalidate_scoped(scope, None, source_path);
            }
            Err(_) => {
                return Ok(Some((*cached).clone()));
            }
        }
    }

    // Cache miss: load from database
    let view = load_conversation_uncached(storage, None, source_path)?;

    // Cache the result if found
    if let Some(v) = view {
        if let Some(scope) = cache_scope.as_deref() {
            CONVERSATION_CACHE.insert_scoped(scope, None, source_path, v.clone());
        }
        return Ok(Some(v));
    }

    Ok(None)
}

/// Load a conversation for a specific source with caching.
pub fn load_conversation_for_source(
    storage: &FrankenStorage,
    source_id: &str,
    source_path: &str,
) -> Result<Option<ConversationView>> {
    let cache_scope = storage_cache_scope(storage);

    if let Some(scope) = cache_scope.as_deref()
        && let Some(cached) = CONVERSATION_CACHE.get_scoped(scope, Some(source_id), source_path)
    {
        match cached_conversation_matches_lookup_head(
            storage,
            Some(source_id),
            source_path,
            &cached,
        ) {
            Ok(true) => {
                return Ok(Some((*cached).clone()));
            }
            Ok(false) => {
                CONVERSATION_CACHE.invalidate_scoped(scope, Some(source_id), source_path);
            }
            Err(_) => {
                return Ok(Some((*cached).clone()));
            }
        }
    }

    let view = load_conversation_uncached(storage, Some(source_id), source_path)?;

    if let Some(v) = view {
        if let Some(scope) = cache_scope.as_deref() {
            CONVERSATION_CACHE.insert_scoped(scope, Some(source_id), source_path, v.clone());
        }
        return Ok(Some(v));
    }

    Ok(None)
}

fn search_hit_has_identity_hint(hit: &SearchHit) -> bool {
    let snippet = hit.snippet.trim();
    let snippet_prefix = snippet.strip_suffix("...").unwrap_or(snippet).trim();
    let title = hit.title.trim();
    hit.conversation_id.is_some()
        || hit.line_number.is_some()
        || hit.created_at.is_some()
        || !hit.content.is_empty()
        || !snippet_prefix.is_empty()
        || !title.is_empty()
}

/// Search coordinates are stored indices plus one, never positions in a view.
/// Invalid/overflowing indices have no canonical coordinate to navigate to.
pub(super) fn canonical_message_number(idx: i64) -> Option<usize> {
    usize::try_from(idx).ok()?.checked_add(1)
}

pub(super) fn conversation_view_matches_hit(view: &ConversationView, hit: &SearchHit) -> bool {
    let normalized_hit_source_id = normalize_ui_hit_source_id(hit);
    if normalize_ui_source_id_parts(
        Some(&view.convo.source_id),
        view.convo.origin_host.as_deref(),
    ) != normalized_hit_source_id
        || view.convo.source_path != std::path::Path::new(&hit.source_path)
    {
        return false;
    }

    // A canonical ID is a constraint, not a hint to discard when lookup fails.
    // Check source/path first: an ID from a different archive or source must
    // not authorize an unrelated cached view. Display text can be truncated,
    // but a requested stored ordinal must exist exactly once in that view.
    if let Some(number) = hit.line_number
        && (number == 0
            || view
                .messages
                .iter()
                .filter(|message| canonical_message_number(message.idx) == Some(number))
                .take(2)
                .count()
                != 1)
    {
        return false;
    }
    if let Some(expected_conversation_id) = hit.conversation_id {
        return view.convo.id == Some(expected_conversation_id);
    }

    let snippet = hit.snippet.trim();
    let snippet_prefix = snippet.strip_suffix("...").unwrap_or(snippet).trim();
    let hit_title = hit.title.trim();
    let convo_title = view
        .convo
        .title
        .as_deref()
        .map(str::trim)
        .filter(|title| !title.is_empty());
    let has_identity_hint = search_hit_has_identity_hint(hit);
    let has_strong_message_identity_hint = hit.created_at.is_some() || !hit.content.is_empty();
    if !has_identity_hint {
        return true;
    }

    if !hit_title.is_empty() {
        match convo_title {
            Some(title) if title != hit_title && !has_strong_message_identity_hint => return false,
            None if hit.line_number.is_none()
                && hit.created_at.is_none()
                && hit.content.is_empty()
                && snippet_prefix.is_empty() =>
            {
                return false;
            }
            _ => {}
        }
    }

    view.messages.iter().any(|msg| {
        let line_from_idx = canonical_message_number(msg.idx);

        if let Some(expected_line) = hit.line_number
            && line_from_idx != Some(expected_line)
        {
            return false;
        }

        if let Some(expected_created_at) = hit.created_at {
            let created_matches = msg.created_at == Some(expected_created_at)
                || (msg.created_at.is_none()
                    && view.convo.started_at == Some(expected_created_at)
                    && hit
                        .line_number
                        .is_some_and(|line| Some(line) == line_from_idx));
            if !created_matches {
                return false;
            }

            // A timestamp match is a stronger identity signal than the search-hit payload,
            // which may be truncated or normalized for display.
            return true;
        }

        if !hit.content.is_empty() {
            return msg.content == hit.content;
        }

        if !snippet_prefix.is_empty() {
            return msg.content.contains(snippet_prefix);
        }

        true
    })
}

pub fn load_conversation_for_hit(
    storage: &FrankenStorage,
    hit: &SearchHit,
) -> Result<Option<ConversationView>> {
    let cache_scope = storage_cache_scope(storage);
    // Path-keyed caches cannot prove uniqueness for a legacy hit without a
    // conversation ID. Resolve those hits against all matching conversations.
    if hit.conversation_id.is_some()
        && let Some(scope) = cache_scope.as_deref()
        && let Some(cached) = CONVERSATION_CACHE.get_scoped(
            scope,
            Some(normalize_ui_hit_source_id(hit).as_str()),
            &hit.source_path,
        )
    {
        if conversation_view_matches_hit(&cached, hit) {
            return Ok(Some((*cached).clone()));
        }
        let normalized_hit_source_id = normalize_ui_hit_source_id(hit);
        CONVERSATION_CACHE.invalidate_scoped(
            scope,
            Some(normalized_hit_source_id.as_str()),
            &hit.source_path,
        );
    }

    if let Some(conversation_id) = hit.conversation_id {
        return Ok(load_conversation_by_id_uncached(storage, conversation_id)?
            .filter(|view| conversation_view_matches_hit(view, hit)));
    }
    if !search_hit_has_identity_hint(hit) {
        // Preserve unanchored browsing; only hit-followup promises exactness.
        return load_conversation_uncached(
            storage,
            Some(normalize_ui_hit_source_id(hit).as_str()),
            &hit.source_path,
        );
    }

    let normalized_source_sql =
        normalized_ui_source_identity_sql_expr("c.source_id", "c.origin_host");
    // LEFT JOIN + COALESCE on agents for consistency with the other UI
    // conversation loaders (NULL agent_id rows must still load).
    let sql = format!(
        "SELECT c.id, COALESCE(a.slug, 'unknown'), w.id, w.path, w.display_name, c.external_id, c.title, c.source_path,
                c.started_at, c.ended_at, c.approx_tokens, c.metadata_json, c.source_id, c.origin_host, c.metadata_bin
         FROM conversations c
         LEFT JOIN agents a ON c.agent_id = a.id
         LEFT JOIN workspaces w ON c.workspace_id = w.id
         WHERE c.source_path = ?1 AND {normalized_source_sql} = ?2
         ORDER BY c.started_at DESC"
    );
    let rows = storage.raw().query_map_collect(
        &sql,
        crate::franken_sync::params![hit.source_path.as_str(), normalize_ui_hit_source_id(hit)],
        ui_conversation_row_parts,
    )?;

    let mut matched = None;
    for (convo_id, convo, workspace) in rows {
        let messages = storage.fetch_messages(convo_id)?;
        let view = ConversationView {
            convo,
            messages,
            workspace,
        };
        if conversation_view_matches_hit(&view, hit) {
            if matched.is_some() {
                // Equal text/timestamps in a shared provider file do not make
                // the newest conversation the one selected by search.
                return Ok(None);
            }
            matched = Some(view);
        }
    }

    Ok(matched)
}

/// Load a conversation with caching, returning Arc for efficiency.
///
/// Use this variant when you need to hold the conversation view for
/// an extended period without cloning.
pub fn load_conversation_arc(
    storage: &FrankenStorage,
    source_path: &str,
) -> Result<Option<Arc<ConversationView>>> {
    let cache_scope = storage_cache_scope(storage);

    // Fast path: check cache first
    if let Some(scope) = cache_scope.as_deref()
        && let Some(cached) = CONVERSATION_CACHE.get_scoped(scope, None, source_path)
    {
        match cached_conversation_matches_lookup_head(storage, None, source_path, &cached) {
            Ok(true) => {
                return Ok(Some(cached));
            }
            Ok(false) => {
                CONVERSATION_CACHE.invalidate_scoped(scope, None, source_path);
            }
            Err(_) => {
                return Ok(Some(cached));
            }
        }
    }

    // Cache miss: load from database
    let view = load_conversation_uncached(storage, None, source_path)?;

    // Cache and return the Arc
    if let Some(v) = view {
        if let Some(scope) = cache_scope.as_deref() {
            let arc = CONVERSATION_CACHE.insert_scoped(scope, None, source_path, v);
            return Ok(Some(arc));
        }
        return Ok(Some(Arc::new(v)));
    }

    Ok(None)
}

/// Log conversation cache statistics.
///
/// Outputs cache stats at debug level via tracing.
pub fn log_conversation_cache_stats() {
    let (hits, misses, evictions) = CONVERSATION_CACHE.stats().get();
    let hit_rate = CONVERSATION_CACHE.stats().hit_rate();
    let count = CONVERSATION_CACHE.len();

    tracing::debug!(
        target: "cass::perf::conversation_cache",
        hits = hits,
        misses = misses,
        evictions = evictions,
        hit_rate = format!("{:.1}%", hit_rate * 100.0),
        cached_count = count,
        "Conversation cache statistics"
    );
}
