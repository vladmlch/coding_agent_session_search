use crate::franken_sync::compat::{ConnectionExt, RowExt};
use crate::franken_sync::{FrankenError, Row};
use crate::model::types::{Conversation, Message, Workspace};
use crate::search::query::SearchHit;
use crate::storage::sqlite::FrankenStorage;
use anyhow::Result;
use lru::LruCache;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

// -------------------------------------------------------------------------
// Conversation View
// -------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct ConversationView {
    pub convo: Conversation,
    pub messages: Vec<Message>,
    pub workspace: Option<Workspace>,
}

fn normalized_ui_source_identity_sql_expr(
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

fn normalized_ui_source_origin_kind_sql_expr(
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

fn normalize_ui_source_id_value(source_id: Option<&str>) -> String {
    let trimmed = source_id.unwrap_or_default().trim();
    if trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case(crate::sources::provenance::LOCAL_SOURCE_ID)
    {
        crate::sources::provenance::LOCAL_SOURCE_ID.to_string()
    } else {
        trimmed.to_string()
    }
}

fn normalize_ui_source_id_parts(source_id: Option<&str>, origin_host: Option<&str>) -> String {
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
/// `"remote"`, else `local`. `pub(crate)` so the index-first export
/// (`src/ui/app.rs` `ExportSelector`) scopes by the exact same identity the
/// hit-followup loaders validate against.
pub(crate) fn normalize_ui_hit_source_id(hit: &SearchHit) -> String {
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

// -------------------------------------------------------------------------
// Conversation Cache (P1 Opt 1.3)
// -------------------------------------------------------------------------

/// Cache statistics for monitoring performance.
#[derive(Debug, Default)]
pub struct CacheStats {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub evictions: AtomicU64,
}

impl CacheStats {
    /// Get current stats as a tuple: (hits, misses, evictions).
    pub fn get(&self) -> (u64, u64, u64) {
        (
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
            self.evictions.load(Ordering::Relaxed),
        )
    }

    /// Calculate hit rate as a percentage (0.0 - 1.0).
    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }
}

/// Number of cache shards (must be power of 2 for efficient modulo).
const NUM_SHARDS: usize = 16;

/// Default capacity per shard.
const DEFAULT_CAPACITY_PER_SHARD: usize = 256;

/// Sharded LRU cache for ConversationView to reduce lock contention.
///
/// Caching conversation views avoids:
/// - Database queries (conversation + messages)
/// - JSON parsing (metadata_json, extra_json)
///
/// This is particularly beneficial for:
/// - TUI scrolling (repeated access to same results)
/// - Detail view expansion (view -> expand -> view pattern)
pub struct ConversationCache {
    shards: [RwLock<LruCache<u64, Arc<ConversationView>>>; NUM_SHARDS],
    stats: CacheStats,
}

impl ConversationCache {
    /// Create a new cache with the specified capacity per shard.
    pub fn new(capacity_per_shard: usize) -> Self {
        Self {
            shards: std::array::from_fn(|_| {
                RwLock::new(LruCache::new(
                    NonZeroUsize::new(capacity_per_shard).unwrap_or(NonZeroUsize::MIN),
                ))
            }),
            stats: CacheStats::default(),
        }
    }

    /// Hash a cache scope + source identity to a u64 key using rustc-hash's FxHasher.
    #[inline]
    fn hash_key(cache_scope: Option<&str>, source_id: Option<&str>, source_path: &str) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = rustc_hash::FxHasher::default();
        cache_scope.unwrap_or("").hash(&mut hasher);
        if let Some(source_id) = source_id {
            normalize_ui_source_id_value(Some(source_id)).hash(&mut hasher);
        } else {
            "".hash(&mut hasher);
        }
        source_path.hash(&mut hasher);
        hasher.finish()
    }

    /// Get the shard index for a given hash.
    #[inline]
    fn shard_index(hash: u64) -> usize {
        (hash as usize) % NUM_SHARDS
    }

    /// Get a cached conversation view by source identity.
    pub fn get(&self, source_id: Option<&str>, source_path: &str) -> Option<Arc<ConversationView>> {
        self.get_scoped("", source_id, source_path)
    }

    /// Get a cached conversation view scoped to a specific database identity.
    pub fn get_scoped(
        &self,
        cache_scope: &str,
        source_id: Option<&str>,
        source_path: &str,
    ) -> Option<Arc<ConversationView>> {
        let hash = Self::hash_key(Some(cache_scope), source_id, source_path);
        let shard_idx = Self::shard_index(hash);
        let mut shard = self.shards[shard_idx].write();

        if let Some(cached) = shard.get(&hash) {
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            Some(Arc::clone(cached))
        } else {
            self.stats.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Insert a conversation view into the cache.
    pub fn insert(
        &self,
        source_id: Option<&str>,
        source_path: &str,
        view: ConversationView,
    ) -> Arc<ConversationView> {
        self.insert_scoped("", source_id, source_path, view)
    }

    /// Insert a conversation view into the cache scoped to a specific database identity.
    pub fn insert_scoped(
        &self,
        cache_scope: &str,
        source_id: Option<&str>,
        source_path: &str,
        view: ConversationView,
    ) -> Arc<ConversationView> {
        let hash = Self::hash_key(Some(cache_scope), source_id, source_path);
        let shard_idx = Self::shard_index(hash);
        let arc = Arc::new(view);

        let mut shard = self.shards[shard_idx].write();
        // Only count eviction if shard is full AND key doesn't already exist
        if shard.len() == shard.cap().get() && !shard.contains(&hash) {
            self.stats.evictions.fetch_add(1, Ordering::Relaxed);
        }
        shard.put(hash, Arc::clone(&arc));

        arc
    }

    /// Invalidate a specific cache entry by source identity.
    pub fn invalidate(&self, source_id: Option<&str>, source_path: &str) {
        self.invalidate_scoped("", source_id, source_path)
    }

    /// Invalidate a specific cache entry scoped to a specific database identity.
    pub fn invalidate_scoped(&self, cache_scope: &str, source_id: Option<&str>, source_path: &str) {
        let hash = Self::hash_key(Some(cache_scope), source_id, source_path);
        let shard_idx = Self::shard_index(hash);
        let mut shard = self.shards[shard_idx].write();
        shard.pop(&hash);
    }

    /// Invalidate all cache entries.
    pub fn invalidate_all(&self) {
        for shard in &self.shards {
            shard.write().clear();
        }
    }

    /// Get cache statistics.
    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }

    /// Get total number of cached entries across all shards.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.read().len()).sum()
    }

    /// Check if cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Global conversation cache instance.
pub static CONVERSATION_CACHE: Lazy<ConversationCache> = Lazy::new(|| {
    let capacity = dotenvy::var("CASS_CONV_CACHE_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_CAPACITY_PER_SHARD);
    ConversationCache::new(capacity)
});

fn storage_cache_scope(storage: &FrankenStorage) -> Option<String> {
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

pub(crate) fn load_conversation_by_id_uncached(
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
pub(crate) fn load_conversation_uncached(
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

pub(crate) fn search_hit_has_identity_hint(hit: &SearchHit) -> bool {
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
pub(crate) fn canonical_message_number(idx: i64) -> Option<usize> {
    usize::try_from(idx).ok()?.checked_add(1)
}

pub(crate) fn conversation_view_matches_hit(view: &ConversationView, hit: &SearchHit) -> bool {
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


// -------------------------------------------------------------------------
// Unit Tests
// -------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::query::MatchType;
    use crate::model::types::MessageRole;
    use std::path::PathBuf;
    use tempfile::tempdir;

    fn make_test_view(id: i64) -> ConversationView {
        ConversationView {
            convo: Conversation {
                id: Some(id),
                agent_slug: "claude".to_string(),
                workspace: Some(PathBuf::from("/test/workspace")),
                external_id: Some(format!("ext-{}", id)),
                title: Some(format!("Test Conversation {}", id)),
                source_path: PathBuf::from(format!("/test/path/{}.jsonl", id)),
                started_at: Some(1704067200 + id),
                ended_at: None,
                approx_tokens: Some(1000),
                metadata_json: serde_json::json!({"test": true}),
                messages: Vec::new(),
                source_id: "local".to_string(),
                origin_host: None,
            },
            messages: vec![Message {
                id: Some(1),
                idx: 0,
                role: MessageRole::User,
                author: None,
                created_at: Some(1704067200),
                content: "Test message".to_string(),
                extra_json: serde_json::json!({}),
                snippets: Vec::new(),
            }],
            workspace: Some(Workspace {
                id: Some(1),
                path: PathBuf::from("/test/workspace"),
                display_name: None,
            }),
        }
    }

    #[test]
    fn test_cache_insert_and_get() {
        let cache = ConversationCache::new(10);
        let view = make_test_view(1);
        let source_path = "/test/path/1.jsonl";

        // Insert into cache
        let arc = cache.insert(None, source_path, view.clone());
        assert_eq!(arc.convo.id, Some(1));

        // Get from cache
        let cached = cache.get(None, source_path);
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().convo.id, Some(1));

        // Check stats
        let (hits, misses, _) = cache.stats().get();
        assert_eq!(hits, 1);
        assert_eq!(misses, 0);
    }

    #[test]
    fn test_cache_distinguishes_same_path_across_sources() {
        let cache = ConversationCache::new(10);
        let source_path = "/test/shared/session.jsonl";

        let mut local = make_test_view(1);
        local.convo.source_path = PathBuf::from(source_path);
        local.convo.source_id = "local".to_string();
        let mut remote = make_test_view(2);
        remote.convo.source_path = PathBuf::from(source_path);
        remote.convo.source_id = "work-laptop".to_string();

        cache.insert(Some("local"), source_path, local);
        cache.insert(Some("work-laptop"), source_path, remote);

        let local_cached = cache.get(Some("local"), source_path).expect("local cached");
        let remote_cached = cache
            .get(Some("work-laptop"), source_path)
            .expect("remote cached");

        assert_eq!(local_cached.convo.source_id, "local");
        assert_eq!(remote_cached.convo.source_id, "work-laptop");
        assert_ne!(local_cached.convo.id, remote_cached.convo.id);
    }

    #[test]
    fn load_conversation_cache_is_scoped_by_database_path() {
        use crate::storage::sqlite::FrankenStorage;

        let shared_path = "/shared/cross-db-session.jsonl";
        let tmp_a = tempfile::TempDir::new().expect("tempdir a");
        let db_path_a = tmp_a.path().join("cass-a.db");
        let storage_a = FrankenStorage::open(&db_path_a).expect("open storage a");
        let conn_a = storage_a.raw();
        let scope_a =
            storage_cache_scope(&storage_a).unwrap_or_else(|| db_path_a.display().to_string());

        let tmp_b = tempfile::TempDir::new().expect("tempdir b");
        let db_path_b = tmp_b.path().join("cass-b.db");
        let storage_b = FrankenStorage::open(&db_path_b).expect("open storage b");
        let conn_b = storage_b.raw();
        let scope_b =
            storage_cache_scope(&storage_b).unwrap_or_else(|| db_path_b.display().to_string());

        CONVERSATION_CACHE.invalidate_scoped(&scope_a, None, shared_path);
        CONVERSATION_CACHE.invalidate_scoped(&scope_b, None, shared_path);

        for conn in [&conn_a, &conn_b] {
            conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'claude_code', 'Claude Code', 'local', 0, 0)")
                .expect("insert agent");
        }

        {
            use crate::franken_sync::compat::{ParamValue, param_slice_to_values};
            let p = [ParamValue::from(shared_path.to_string())];
            conn_a.execute_with_params(
                "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'db-a', 'DB A Session', ?1, 'local', 100)",
                &param_slice_to_values(&p),
            )
            .expect("insert db a conversation");
            conn_b.execute_with_params(
                "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'db-b', 'DB B Session', ?1, 'local', 100)",
                &param_slice_to_values(&p),
            )
            .expect("insert db b conversation");
        }
        conn_a.execute(
            "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (1, 1, 0, 'user', 'db a body')",
        )
        .expect("insert db a message");
        conn_b.execute(
            "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (1, 1, 0, 'user', 'db b body')",
        )
        .expect("insert db b message");

        let from_a = load_conversation(&storage_a, shared_path)
            .expect("load from db a")
            .expect("db a conversation present");
        assert_eq!(from_a.convo.title.as_deref(), Some("DB A Session"));
        assert_eq!(from_a.messages[0].content, "db a body");

        let from_b = load_conversation(&storage_b, shared_path)
            .expect("load from db b")
            .expect("db b conversation present");
        assert_eq!(from_b.convo.title.as_deref(), Some("DB B Session"));
        assert_eq!(from_b.messages[0].content, "db b body");

        CONVERSATION_CACHE.invalidate_scoped(&scope_a, None, shared_path);
        CONVERSATION_CACHE.invalidate_scoped(&scope_b, None, shared_path);
    }

    #[test]
    fn load_conversation_for_source_selects_blank_remote_source_id_via_origin_host() {
        use crate::storage::sqlite::FrankenStorage;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let db_path = tmp.path().join("cass.db");
        let storage = FrankenStorage::open(&db_path).expect("open storage");
        let conn = storage.raw();
        let shared_path = "/shared/session.jsonl";

        conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'claude_code', 'Claude Code', 'local', 0, 0)")
            .expect("insert agent");
        conn.execute(
            "INSERT INTO sources (id, kind, host_label, created_at, updated_at) VALUES ('   ', 'ssh', 'user@laptop', 0, 0)",
        )
        .expect("insert blank-id remote source");
        {
            use crate::franken_sync::compat::{ParamValue, param_slice_to_values};
            let p = [ParamValue::from(shared_path.to_string())];
            conn.execute_with_params(
                "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, origin_host, started_at) VALUES (1, 1, 'remote-ext', 'Remote Session', ?1, '   ', 'user@laptop', 200)",
                &param_slice_to_values(&p),
            )
            .expect("insert remote conversation");
        }
        conn.execute(
            "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (1, 1, 0, 'user', 'remote body')",
        )
        .expect("insert remote message");

        let loaded = load_conversation_for_source(&storage, "user@laptop", shared_path)
            .expect("load conversation")
            .expect("conversation present");

        assert_eq!(loaded.convo.source_id, "user@laptop");
        assert_eq!(loaded.convo.origin_host.as_deref(), Some("user@laptop"));
        assert_eq!(loaded.convo.title.as_deref(), Some("Remote Session"));
        assert_eq!(loaded.messages[0].content, "remote body");
    }

    #[test]
    fn load_conversation_for_source_selects_exact_source_id() {
        use crate::storage::sqlite::FrankenStorage;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let db_path = tmp.path().join("cass.db");
        let storage = FrankenStorage::open(&db_path).expect("open storage");
        let conn = storage.raw();
        let shared_path = "/shared/session.jsonl";

        conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'claude_code', 'Claude Code', 'local', 0, 0)")
            .expect("insert agent");
        conn.execute(
            "INSERT INTO sources (id, kind, host_label, created_at, updated_at) VALUES ('  local  ', 'local', 'local', 0, 0)",
        )
        .expect("insert local source");
        conn.execute(
            "INSERT INTO sources (id, kind, host_label, created_at, updated_at) VALUES ('work-laptop', 'ssh', 'work-laptop', 0, 0)",
        )
        .expect("insert source");
        {
            use crate::franken_sync::compat::{ParamValue, param_slice_to_values};
            let p = [ParamValue::from(shared_path.to_string())];
            conn.execute_with_params(
                "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'local-ext', 'Local Session', ?1, '  local  ', 200)",
                &param_slice_to_values(&p),
            )
            .expect("insert local conversation");
            conn.execute_with_params(
                "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (2, 1, 'remote-ext', 'Remote Session', ?1, 'work-laptop', 100)",
                &param_slice_to_values(&p),
            )
            .expect("insert remote conversation");
        }
        conn.execute(
            "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (1, 1, 0, 'user', 'local body')",
        )
        .expect("insert local message");
        conn.execute(
            "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (2, 2, 0, 'user', 'remote body')",
        )
        .expect("insert remote message");

        let local = load_conversation_for_source(&storage, "local", shared_path)
            .expect("load local")
            .expect("local conversation");
        let remote = load_conversation_for_source(&storage, "work-laptop", shared_path)
            .expect("load remote")
            .expect("remote conversation");

        assert_eq!(local.convo.source_id, "local");
        assert_eq!(local.convo.title.as_deref(), Some("Local Session"));
        assert_eq!(local.messages[0].content, "local body");

        assert_eq!(remote.convo.source_id, "work-laptop");
        assert_eq!(remote.convo.title.as_deref(), Some("Remote Session"));
        assert_eq!(remote.messages[0].content, "remote body");
    }

    #[test]
    fn load_conversation_for_source_invalidates_cache_when_newer_conversation_arrives() {
        use crate::storage::sqlite::FrankenStorage;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let db_path = tmp.path().join("cass.db");
        let storage = FrankenStorage::open(&db_path).expect("open storage");
        let conn = storage.raw();
        let shared_path = "/shared/source-specific-session.jsonl";

        CONVERSATION_CACHE.invalidate(Some("local"), shared_path);

        conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'claude_code', 'Claude Code', 'local', 0, 0)")
            .expect("insert agent");
        {
            use crate::franken_sync::compat::{ParamValue, param_slice_to_values};
            let p = [ParamValue::from(shared_path.to_string())];
            conn.execute_with_params(
                "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'old-ext', 'Old Session', ?1, 'local', 100)",
                &param_slice_to_values(&p),
            )
            .expect("insert old conversation");
        }
        conn.execute(
            "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (1, 1, 0, 'user', 'old body')",
        )
        .expect("insert old message");

        let first = load_conversation_for_source(&storage, "local", shared_path)
            .expect("load old conversation")
            .expect("old conversation present");
        assert_eq!(first.convo.title.as_deref(), Some("Old Session"));
        assert_eq!(first.messages[0].content, "old body");

        {
            use crate::franken_sync::compat::{ParamValue, param_slice_to_values};
            let p = [ParamValue::from(shared_path.to_string())];
            conn.execute_with_params(
                "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (2, 1, 'new-ext', 'New Session', ?1, 'local', 200)",
                &param_slice_to_values(&p),
            )
            .expect("insert new conversation");
        }
        conn.execute(
            "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (2, 2, 0, 'user', 'new body')",
        )
        .expect("insert new message");

        let second = load_conversation_for_source(&storage, "local", shared_path)
            .expect("load new conversation")
            .expect("new conversation present");

        assert_eq!(second.convo.title.as_deref(), Some("New Session"));
        assert_eq!(second.messages[0].content, "new body");

        CONVERSATION_CACHE.invalidate(Some("local"), shared_path);
    }

    #[test]
    fn load_conversation_prefers_named_local_source_for_shared_path() {
        use crate::storage::sqlite::FrankenStorage;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let db_path = tmp.path().join("cass.db");
        let storage = FrankenStorage::open(&db_path).expect("open storage");
        let conn = storage.raw();
        let shared_path = "/shared/session.jsonl";

        conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'claude_code', 'Claude Code', 'local', 0, 0)")
            .expect("insert agent");
        conn.execute(
            "INSERT INTO sources (id, kind, host_label, created_at, updated_at) VALUES ('backup-local', 'local', NULL, 0, 0)",
        )
        .expect("insert named local source");
        conn.execute(
            "INSERT INTO sources (id, kind, host_label, created_at, updated_at) VALUES ('work-laptop', 'ssh', 'work-laptop', 0, 0)",
        )
        .expect("insert source");
        {
            use crate::franken_sync::compat::{ParamValue, param_slice_to_values};
            let p = [ParamValue::from(shared_path.to_string())];
            conn.execute_with_params(
                "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'local-ext', 'Local Session', ?1, 'backup-local', 100)",
                &param_slice_to_values(&p),
            )
            .expect("insert local conversation");
            conn.execute_with_params(
                "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (2, 1, 'remote-ext', 'Remote Session', ?1, 'work-laptop', 200)",
                &param_slice_to_values(&p),
            )
            .expect("insert remote conversation");
        }
        conn.execute(
            "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (1, 1, 0, 'user', 'local body')",
        )
        .expect("insert local message");
        conn.execute(
            "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (2, 2, 0, 'user', 'remote body')",
        )
        .expect("insert remote message");

        let loaded = load_conversation(&storage, shared_path)
            .expect("load conversation")
            .expect("conversation present");

        assert_eq!(loaded.convo.source_id, "backup-local");
        assert_eq!(loaded.convo.title.as_deref(), Some("Local Session"));
        assert_eq!(loaded.messages[0].content, "local body");
    }

    #[test]
    fn load_conversation_uses_cached_value_when_validation_query_fails() {
        use crate::storage::sqlite::FrankenStorage;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let db_path = tmp.path().join("cass.db");
        let storage = FrankenStorage::open(&db_path).expect("open storage");
        let conn = storage.raw();
        let shared_path = "/shared/cached-when-db-breaks.jsonl";

        CONVERSATION_CACHE.invalidate(None, shared_path);
        CONVERSATION_CACHE.invalidate(Some("local"), shared_path);

        conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'claude_code', 'Claude Code', 'local', 0, 0)")
            .expect("insert agent");
        {
            use crate::franken_sync::compat::{ParamValue, param_slice_to_values};
            let p = [ParamValue::from(shared_path.to_string())];
            conn.execute_with_params(
                "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'local-ext', 'Cached Session', ?1, 'local', 100)",
                &param_slice_to_values(&p),
            )
            .expect("insert local conversation");
        }
        conn.execute(
            "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (1, 1, 0, 'user', 'cached body')",
        )
        .expect("insert local message");

        let cached = load_conversation(&storage, shared_path)
            .expect("load initial conversation")
            .expect("conversation present");
        assert_eq!(cached.convo.title.as_deref(), Some("Cached Session"));
        assert_eq!(cached.messages[0].content, "cached body");

        conn.execute("DROP TABLE conversations")
            .expect("drop conversations to force validation failure");

        let still_cached = load_conversation(&storage, shared_path)
            .expect("use cached conversation after validation failure")
            .expect("cached conversation still present");

        assert_eq!(still_cached.convo.title.as_deref(), Some("Cached Session"));
        assert_eq!(still_cached.messages[0].content, "cached body");

        CONVERSATION_CACHE.invalidate(None, shared_path);
        CONVERSATION_CACHE.invalidate(Some("local"), shared_path);
    }

    #[test]
    fn gh493_canonical_message_numbers_reject_invalid_indices_without_position_fallback() {
        assert_eq!(canonical_message_number(-1), None);
        assert_eq!(canonical_message_number(i64::MIN), None);
        assert_eq!(canonical_message_number(0), Some(1));
        assert_eq!(canonical_message_number(128), Some(129));
        assert_eq!(canonical_message_number(540), Some(541));
        assert_eq!(
            canonical_message_number(i64::MAX),
            usize::try_from(i64::MAX)
                .ok()
                .and_then(|idx| idx.checked_add(1))
        );
    }

    fn gh493_sparse_view_fixture() -> (tempfile::TempDir, FrankenStorage, SearchHit) {
        let root = tempdir().expect("tempdir");
        let storage = FrankenStorage::open(&root.path().join("cass.db")).expect("archive");
        storage.raw().execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'codex', 'Codex', 'local', 0, 0)").unwrap();
        storage.raw().execute("INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'sparse', 'Sparse', '/gh493/shared.db', 'local', 100)").unwrap();
        for (id, idx) in [(1_i64, 1_i64), (2, 16), (3, 89)] {
            storage.raw().execute_compat(
                "INSERT INTO messages (id, conversation_id, idx, role, created_at, content) VALUES (?1, 1, ?2, 'user', 101, 'identical repeated body')",
                crate::franken_sync::params![id, idx],
            ).unwrap();
        }
        let hit = SearchHit {
            title: "Sparse".into(),
            snippet: "identical repeated body".into(),
            content: "identical repeated body".into(),
            content_hash: 0,
            conversation_id: Some(1),
            score: 1.0,
            agent: "codex".into(),
            source_path: "/gh493/shared.db".into(),
            source_id: "local".into(),
            origin_kind: "local".into(),
            origin_host: None,
            workspace: String::new(),
            workspace_original: None,
            created_at: Some(101),
            line_number: Some(17),
            match_type: MatchType::Exact,
        };
        (root, storage, hit)
    }

    #[test]
    fn gh493_hit_lookup_never_uses_dense_positions_or_ignores_source_identity() {
        let (_root, storage, hit) = gh493_sparse_view_fixture();
        let view = load_conversation_for_hit(&storage, &hit).unwrap().unwrap();
        for conversation_id in [Some(1), None] {
            for (number, valid) in [
                (0, false),
                (1, false),
                (2, true),
                (3, false),
                (17, true),
                (90, true),
                (91, false),
            ] {
                let mut request = hit.clone();
                request.conversation_id = conversation_id;
                request.line_number = Some(number);
                assert_eq!(
                    conversation_view_matches_hit(&view, &request),
                    valid,
                    "stored ordinal {number}, conversation {conversation_id:?}"
                );
                assert_eq!(
                    load_conversation_for_hit(&storage, &request)
                        .unwrap()
                        .is_some(),
                    valid
                );
            }
        }
        for (source, path) in [("remote", "/gh493/shared.db"), ("local", "/gh493/other.db")] {
            let mut request = hit.clone();
            request.source_id = source.into();
            request.source_path = path.into();
            assert!(!conversation_view_matches_hit(&view, &request));
            assert!(
                load_conversation_for_hit(&storage, &request)
                    .unwrap()
                    .is_none()
            );
        }
        let mut duplicate_view = view.clone();
        duplicate_view.messages.push(view.messages[1].clone());
        assert!(!conversation_view_matches_hit(&duplicate_view, &hit));
        let mut truncated = hit;
        truncated.title = "old display title".into();
        truncated.content = "truncated display payload".into();
        assert!(
            conversation_view_matches_hit(&view, &truncated),
            "exact source/path/conversation/ordinal is independent of display text"
        );
    }

    #[test]
    fn gh493_legacy_hit_ambiguity_cannot_be_hidden_by_a_path_cache() {
        let (_root, storage, mut hit) = gh493_sparse_view_fixture();
        // Populate the ordinary path cache before another session appears.
        load_conversation_for_source(&storage, "local", &hit.source_path)
            .unwrap()
            .unwrap();
        storage.raw().execute("INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (2, 1, 'twin', 'Sparse', '/gh493/shared.db', 'local', 200)").unwrap();
        storage.raw().execute("INSERT INTO messages (id, conversation_id, idx, role, created_at, content) VALUES (4, 2, 16, 'user', 101, 'identical repeated body')").unwrap();
        hit.conversation_id = None;
        assert!(
            load_conversation_for_hit(&storage, &hit).unwrap().is_none(),
            "matching text in two sessions is ambiguous, even with a warm cache"
        );
        for id in [1, 2] {
            hit.conversation_id = Some(id);
            assert_eq!(
                load_conversation_for_hit(&storage, &hit)
                    .unwrap()
                    .unwrap()
                    .convo
                    .id,
                Some(id)
            );
        }
        hit.conversation_id = Some(999);
        assert!(load_conversation_for_hit(&storage, &hit).unwrap().is_none());
    }

    #[test]
    fn conversation_view_matches_hit_normalizes_blank_remote_source_id_via_origin_host() {
        let view = ConversationView {
            convo: Conversation {
                id: Some(1),
                agent_slug: "claude_code".to_string(),
                workspace: None,
                external_id: Some("ext-1".to_string()),
                title: Some("Session".to_string()),
                source_path: std::path::PathBuf::from("/shared/session.jsonl"),
                started_at: Some(100),
                ended_at: None,
                approx_tokens: None,
                metadata_json: serde_json::Value::Null,
                messages: Vec::new(),
                source_id: "user@laptop".to_string(),
                origin_host: Some("user@laptop".to_string()),
            },
            messages: vec![Message {
                id: Some(1),
                idx: 0,
                role: MessageRole::User,
                author: None,
                created_at: Some(101),
                content: "body".to_string(),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            }],
            workspace: None,
        };

        let hit = SearchHit {
            title: "Session".to_string(),
            snippet: String::new(),
            content: String::new(),
            content_hash: 0,
            score: 0.0,
            conversation_id: None,
            source_path: "/shared/session.jsonl".to_string(),
            agent: "claude_code".to_string(),
            workspace: String::new(),
            workspace_original: None,
            created_at: None,
            line_number: None,
            match_type: Default::default(),
            source_id: "   ".to_string(),
            origin_kind: "remote".to_string(),
            origin_host: Some("user@laptop".to_string()),
        };

        assert!(conversation_view_matches_hit(&view, &hit));
    }

    #[test]
    fn conversation_view_matches_hit_normalizes_local_source_id_variants() {
        let view = ConversationView {
            convo: Conversation {
                id: Some(1),
                agent_slug: "claude_code".to_string(),
                workspace: None,
                external_id: Some("ext-1".to_string()),
                title: Some("Session".to_string()),
                source_path: std::path::PathBuf::from("/shared/session.jsonl"),
                started_at: Some(100),
                ended_at: None,
                approx_tokens: None,
                metadata_json: serde_json::Value::Null,
                messages: Vec::new(),
                source_id: "local".to_string(),
                origin_host: None,
            },
            messages: vec![Message {
                id: Some(1),
                idx: 0,
                role: MessageRole::User,
                author: None,
                created_at: Some(101),
                content: "body".to_string(),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            }],
            workspace: None,
        };

        let hit = SearchHit {
            title: "Session".to_string(),
            snippet: String::new(),
            content: String::new(),
            content_hash: 0,
            score: 0.0,
            conversation_id: None,
            source_path: "/shared/session.jsonl".to_string(),
            agent: "claude_code".to_string(),
            workspace: String::new(),
            workspace_original: None,
            created_at: None,
            line_number: None,
            match_type: Default::default(),
            source_id: "  LOCAL  ".to_string(),
            origin_kind: "local".to_string(),
            origin_host: None,
        };

        assert!(conversation_view_matches_hit(&view, &hit));
    }

    #[test]
    fn conversation_view_matches_hit_refuses_stale_conversation_id_even_with_matching_hints() {
        let view = ConversationView {
            convo: Conversation {
                id: Some(1),
                agent_slug: "claude_code".to_string(),
                workspace: None,
                external_id: Some("ext-1".to_string()),
                title: Some("Session".to_string()),
                source_path: std::path::PathBuf::from("/shared/session.jsonl"),
                started_at: Some(100),
                ended_at: None,
                approx_tokens: None,
                metadata_json: serde_json::Value::Null,
                messages: Vec::new(),
                source_id: "local".to_string(),
                origin_host: None,
            },
            messages: vec![Message {
                id: Some(1),
                idx: 0,
                role: MessageRole::User,
                author: None,
                created_at: Some(101),
                content: "body".to_string(),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            }],
            workspace: None,
        };

        let hit = SearchHit {
            title: "Session".to_string(),
            snippet: String::new(),
            content: "body".to_string(),
            content_hash: 0,
            score: 0.0,
            conversation_id: Some(999),
            source_path: "/shared/session.jsonl".to_string(),
            agent: "claude_code".to_string(),
            workspace: String::new(),
            workspace_original: None,
            created_at: Some(101),
            line_number: Some(1),
            match_type: Default::default(),
            source_id: "local".to_string(),
            origin_kind: "local".to_string(),
            origin_host: None,
        };

        assert!(!conversation_view_matches_hit(&view, &hit));
        let mut exact = hit.clone();
        exact.conversation_id = view.convo.id;
        assert!(conversation_view_matches_hit(&view, &exact));
        let mut legacy = hit;
        legacy.conversation_id = None;
        assert!(conversation_view_matches_hit(&view, &legacy));
    }

    #[test]
    fn conversation_view_matches_hit_rejects_stale_conversation_id_without_other_hints() {
        let view = ConversationView {
            convo: Conversation {
                id: Some(1),
                agent_slug: "claude_code".to_string(),
                workspace: None,
                external_id: Some("ext-1".to_string()),
                title: Some("Session".to_string()),
                source_path: std::path::PathBuf::from("/shared/session.jsonl"),
                started_at: Some(100),
                ended_at: None,
                approx_tokens: None,
                metadata_json: serde_json::Value::Null,
                messages: vec![],
                source_id: "local".to_string(),
                origin_host: None,
            },
            messages: vec![Message {
                id: Some(1),
                idx: 0,
                role: MessageRole::User,
                author: None,
                created_at: Some(101),
                content: "body".to_string(),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            }],
            workspace: None,
        };

        let hit = SearchHit {
            title: String::new(),
            snippet: String::new(),
            content: String::new(),
            content_hash: 0,
            score: 0.0,
            conversation_id: Some(999),
            source_path: "/shared/session.jsonl".to_string(),
            agent: "claude_code".to_string(),
            workspace: String::new(),
            workspace_original: None,
            created_at: None,
            line_number: None,
            match_type: Default::default(),
            source_id: "local".to_string(),
            origin_kind: "local".to_string(),
            origin_host: None,
        };

        assert!(!conversation_view_matches_hit(&view, &hit));
    }

    #[test]
    fn load_conversation_for_source_uses_cached_value_when_validation_query_fails() {
        use crate::storage::sqlite::FrankenStorage;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let db_path = tmp.path().join("cass.db");
        let storage = FrankenStorage::open(&db_path).expect("open storage");
        let conn = storage.raw();
        let shared_path = "/shared/source-cache-when-db-breaks.jsonl";

        CONVERSATION_CACHE.invalidate(Some("local"), shared_path);

        conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'claude_code', 'Claude Code', 'local', 0, 0)")
            .expect("insert agent");
        {
            use crate::franken_sync::compat::{ParamValue, param_slice_to_values};
            let p = [ParamValue::from(shared_path.to_string())];
            conn.execute_with_params(
                "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'local-ext', 'Cached Session', ?1, 'local', 100)",
                &param_slice_to_values(&p),
            )
            .expect("insert local conversation");
        }
        conn.execute(
            "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (1, 1, 0, 'user', 'cached body')",
        )
        .expect("insert local message");

        let cached = load_conversation_for_source(&storage, "local", shared_path)
            .expect("load initial conversation")
            .expect("conversation present");
        assert_eq!(cached.convo.title.as_deref(), Some("Cached Session"));
        assert_eq!(cached.messages[0].content, "cached body");

        conn.execute("DROP TABLE conversations")
            .expect("drop conversations to force validation failure");

        let still_cached = load_conversation_for_source(&storage, "  LOCAL  ", shared_path)
            .expect("use cached conversation after validation failure")
            .expect("cached conversation still present");

        assert_eq!(still_cached.convo.title.as_deref(), Some("Cached Session"));
        assert_eq!(still_cached.messages[0].content, "cached body");

        CONVERSATION_CACHE.invalidate(Some("local"), shared_path);
    }

    #[test]
    fn load_conversation_invalidates_path_only_cache_when_local_source_appears() {
        use crate::storage::sqlite::FrankenStorage;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let db_path = tmp.path().join("cass.db");
        let storage = FrankenStorage::open(&db_path).expect("open storage");
        let conn = storage.raw();
        let shared_path = "/shared/late-local-session.jsonl";

        CONVERSATION_CACHE.invalidate(None, shared_path);
        CONVERSATION_CACHE.invalidate(Some("local"), shared_path);
        CONVERSATION_CACHE.invalidate(Some("work-laptop"), shared_path);

        conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'claude_code', 'Claude Code', 'local', 0, 0)")
            .expect("insert agent");
        conn.execute(
            "INSERT INTO sources (id, kind, host_label, created_at, updated_at) VALUES ('work-laptop', 'ssh', 'work-laptop', 0, 0)",
        )
        .expect("insert source");
        {
            use crate::franken_sync::compat::{ParamValue, param_slice_to_values};
            let p = [ParamValue::from(shared_path.to_string())];
            conn.execute_with_params(
                "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'remote-ext', 'Remote Session', ?1, 'work-laptop', 200)",
                &param_slice_to_values(&p),
            )
            .expect("insert remote conversation");
        }
        conn.execute(
            "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (1, 1, 0, 'user', 'remote body')",
        )
        .expect("insert remote message");

        let first = load_conversation(&storage, shared_path)
            .expect("load remote conversation")
            .expect("remote conversation present");
        assert_eq!(first.convo.source_id, "work-laptop");
        assert_eq!(first.messages[0].content, "remote body");

        {
            use crate::franken_sync::compat::{ParamValue, param_slice_to_values};
            let p = [ParamValue::from(shared_path.to_string())];
            conn.execute_with_params(
                "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (2, 1, 'local-ext', 'Local Session', ?1, 'local', 100)",
                &param_slice_to_values(&p),
            )
            .expect("insert local conversation");
        }
        conn.execute(
            "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (2, 2, 0, 'user', 'local body')",
        )
        .expect("insert local message");

        let second = load_conversation(&storage, shared_path)
            .expect("load local conversation")
            .expect("local conversation present");

        assert_eq!(second.convo.source_id, "local");
        assert_eq!(second.convo.title.as_deref(), Some("Local Session"));
        assert_eq!(second.messages[0].content, "local body");

        CONVERSATION_CACHE.invalidate(None, shared_path);
        CONVERSATION_CACHE.invalidate(Some("local"), shared_path);
        CONVERSATION_CACHE.invalidate(Some("work-laptop"), shared_path);
    }

    #[test]
    fn load_conversation_for_hit_selects_exact_conversation_within_same_source_and_path() {
        use crate::storage::sqlite::FrankenStorage;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let db_path = tmp.path().join("cass.db");
        let storage = FrankenStorage::open(&db_path).expect("open storage");
        let conn = storage.raw();
        let shared_path = "/shared/cursor.sqlite";

        conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'cursor', 'Cursor', 'local', 0, 0)")
            .expect("insert agent");
        conn.execute(
            "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'old-ext', 'Old Session', '/shared/cursor.sqlite', 'local', 100)",
        )
        .expect("insert old conversation");
        conn.execute(
            "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (2, 1, 'new-ext', 'New Session', '/shared/cursor.sqlite', 'local', 200)",
        )
        .expect("insert new conversation");
        conn.execute(
            "INSERT INTO messages (id, conversation_id, idx, role, created_at, content) VALUES (1, 1, 0, 'user', 101, 'old conversation body')",
        )
        .expect("insert old message");
        conn.execute(
            "INSERT INTO messages (id, conversation_id, idx, role, created_at, content) VALUES (2, 2, 0, 'user', 201, 'new conversation body')",
        )
        .expect("insert new message");

        let hit = SearchHit {
            title: "New Session".to_string(),
            snippet: "new conversation body".to_string(),
            content: "new conversation body".to_string(),
            content_hash: 0,
            conversation_id: None,
            score: 0.0,
            source_path: shared_path.to_string(),
            agent: "cursor".to_string(),
            workspace: String::new(),
            workspace_original: None,
            created_at: Some(201),
            line_number: Some(1),
            match_type: Default::default(),
            source_id: "local".to_string(),
            origin_kind: "local".to_string(),
            origin_host: None,
        };

        let loaded = load_conversation_for_hit(&storage, &hit)
            .expect("load exact conversation")
            .expect("matching conversation");

        assert_eq!(loaded.convo.external_id.as_deref(), Some("new-ext"));
        assert_eq!(loaded.convo.title.as_deref(), Some("New Session"));
        assert_eq!(loaded.messages[0].content, "new conversation body");
    }

    #[test]
    fn load_conversation_for_hit_accepts_matching_timestamp_even_when_hit_content_is_stale() {
        use crate::storage::sqlite::FrankenStorage;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let db_path = tmp.path().join("cass.db");
        let storage = FrankenStorage::open(&db_path).expect("open storage");
        let conn = storage.raw();
        let shared_path = "/shared/cursor.sqlite";

        conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'cursor', 'Cursor', 'local', 0, 0)")
            .expect("insert agent");
        conn.execute(
            "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'new-ext', 'New Session', '/shared/cursor.sqlite', 'local', 200)",
        )
        .expect("insert conversation");
        conn.execute(
            "INSERT INTO messages (id, conversation_id, idx, role, created_at, content) VALUES (1, 1, 0, 'user', 201, 'new conversation body')",
        )
        .expect("insert message");

        let hit = SearchHit {
            title: "New Session".to_string(),
            snippet: "rendered fragment".to_string(),
            content: "stale search fragment".to_string(),
            content_hash: 0,
            conversation_id: None,
            score: 0.0,
            source_path: shared_path.to_string(),
            agent: "cursor".to_string(),
            workspace: String::new(),
            workspace_original: None,
            created_at: Some(201),
            line_number: None,
            match_type: Default::default(),
            source_id: "local".to_string(),
            origin_kind: "local".to_string(),
            origin_host: None,
        };

        let loaded = load_conversation_for_hit(&storage, &hit)
            .expect("load exact conversation")
            .expect("matching conversation");

        assert_eq!(loaded.convo.external_id.as_deref(), Some("new-ext"));
        assert_eq!(loaded.messages[0].content, "new conversation body");
    }

    #[test]
    fn load_conversation_for_hit_refuses_stale_id_without_substituting_matching_content() {
        let tmp = tempdir().expect("tempdir");
        let db_path = tmp.path().join("cass.db");
        let storage = FrankenStorage::open(&db_path).expect("open db");
        let conn = storage.raw();
        conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'claude_code', 'Claude Code', 'local', 0, 0)")
            .expect("insert agent");
        conn.execute(
            "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'exact-ext', 'Database Title', '/shared/cursor.sqlite', 'local', 200)",
        )
        .expect("insert conversation");
        conn.execute(
            "INSERT INTO messages (id, conversation_id, idx, role, created_at, content) VALUES (1, 1, 0, 'user', 201, 'db body')",
        )
        .expect("insert message");

        let hit = SearchHit {
            title: "Database Title".to_string(),
            snippet: "db body".to_string(),
            content: "db body".to_string(),
            content_hash: 0,
            conversation_id: Some(999),
            score: 1.0,
            source_path: "/shared/cursor.sqlite".to_string(),
            agent: "claude_code".to_string(),
            workspace: String::new(),
            workspace_original: None,
            created_at: Some(201),
            line_number: Some(1),
            match_type: MatchType::Exact,
            source_id: "local".to_string(),
            origin_kind: "local".to_string(),
            origin_host: None,
        };
        assert!(
            load_conversation_for_hit(&storage, &hit)
                .expect("load attempt succeeds")
                .is_none()
        );

        // The same uniquely identifying legacy hit remains usable when no ID
        // was supplied. An explicit stale ID is never silently erased.
        let mut legacy = hit;
        legacy.conversation_id = None;
        let loaded = load_conversation_for_hit(&storage, &legacy)
            .expect("load legacy hit")
            .expect("unique message remains discoverable");

        assert_eq!(loaded.convo.id, Some(1));
        assert_eq!(
            loaded.convo.source_path,
            std::path::Path::new("/shared/cursor.sqlite")
        );
    }

    #[test]
    fn load_conversation_for_hit_uses_origin_host_when_db_source_id_is_blank_remote() {
        use crate::storage::sqlite::FrankenStorage;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let db_path = tmp.path().join("cass.db");
        let storage = FrankenStorage::open(&db_path).expect("open storage");
        let conn = storage.raw();
        let shared_path = "/shared/remote.sqlite";

        conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'cursor', 'Cursor', 'local', 0, 0)")
            .expect("insert agent");
        conn.execute(
            "INSERT INTO sources (id, kind, host_label, created_at, updated_at) VALUES ('   ', 'ssh', 'user@laptop', 0, 0)",
        )
        .expect("insert blank-id remote source");
        conn.execute(
            "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, origin_host, started_at) VALUES (1, 1, 'remote-ext', 'Remote Session', '/shared/remote.sqlite', '   ', 'user@laptop', 200)",
        )
        .expect("insert conversation");
        conn.execute(
            "INSERT INTO messages (id, conversation_id, idx, role, created_at, content) VALUES (1, 1, 0, 'user', 201, 'db body')",
        )
        .expect("insert message");

        let hit = SearchHit {
            title: "Remote Session".to_string(),
            snippet: String::new(),
            content: String::new(),
            content_hash: 0,
            conversation_id: Some(1),
            score: 0.0,
            source_path: shared_path.to_string(),
            agent: "cursor".to_string(),
            workspace: String::new(),
            workspace_original: None,
            created_at: None,
            line_number: None,
            match_type: Default::default(),
            source_id: "   ".to_string(),
            origin_kind: "remote".to_string(),
            origin_host: Some("user@laptop".to_string()),
        };

        let loaded = load_conversation_for_hit(&storage, &hit)
            .expect("load exact conversation")
            .expect("matching conversation");

        assert_eq!(loaded.convo.id, Some(1));
        assert_eq!(loaded.convo.source_id, "user@laptop");
        assert_eq!(loaded.convo.origin_host.as_deref(), Some("user@laptop"));
    }

    #[test]
    fn load_conversation_for_hit_rejects_conflicting_path_and_source_despite_matching_id() {
        use crate::storage::sqlite::FrankenStorage;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let db_path = tmp.path().join("cass.db");
        let storage = FrankenStorage::open(&db_path).expect("open storage");
        let conn = storage.raw();

        conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'cursor', 'Cursor', 'local', 0, 0)")
            .expect("insert agent");
        conn.execute(
            "INSERT INTO sources (id, kind, host_label, created_at, updated_at) VALUES ('  local  ', 'local', 'local', 0, 0)",
        )
        .expect("insert local source");
        conn.execute(
            "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'exact-ext', 'Database Title', '/db/real/path.sqlite', '  local  ', 200)",
        )
        .expect("insert conversation");
        conn.execute(
            "INSERT INTO messages (id, conversation_id, idx, role, created_at, content) VALUES (1, 1, 0, 'user', 201, 'db body')",
        )
        .expect("insert message");

        let hit = SearchHit {
            title: "Stale Indexed Title".to_string(),
            snippet: String::new(),
            content: String::new(),
            content_hash: 0,
            conversation_id: Some(1),
            score: 0.0,
            source_path: "/stale/index/path.sqlite".to_string(),
            agent: "cursor".to_string(),
            workspace: String::new(),
            workspace_original: None,
            created_at: None,
            line_number: None,
            match_type: Default::default(),
            source_id: "remote-laptop".to_string(),
            origin_kind: "remote".to_string(),
            origin_host: Some("dev@laptop".to_string()),
        };

        assert!(load_conversation_for_hit(&storage, &hit).unwrap().is_none());
        let mut exact = hit;
        exact.source_path = "/db/real/path.sqlite".into();
        exact.source_id = "  LOCAL  ".into();
        exact.origin_kind = "local".into();
        exact.origin_host = None;
        let loaded = load_conversation_for_hit(&storage, &exact)
            .expect("load exact source/path/conversation")
            .expect("matching conversation");

        assert_eq!(loaded.convo.id, Some(1));
        assert_eq!(
            loaded.convo.source_path.to_string_lossy(),
            "/db/real/path.sqlite"
        );
        assert_eq!(loaded.convo.source_id, "local");
    }

    #[test]
    fn load_conversation_for_hit_prefers_exact_conversation_id_over_stale_title() {
        use crate::storage::sqlite::FrankenStorage;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let db_path = tmp.path().join("cass.db");
        let storage = FrankenStorage::open(&db_path).expect("open storage");
        let conn = storage.raw();
        let shared_path = "/shared/cursor.sqlite";

        conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'cursor', 'Cursor', 'local', 0, 0)")
            .expect("insert agent");
        conn.execute(
            "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'exact-ext', 'Database Title', '/shared/cursor.sqlite', 'local', 200)",
        )
        .expect("insert conversation");
        conn.execute(
            "INSERT INTO messages (id, conversation_id, idx, role, created_at, content) VALUES (1, 1, 0, 'user', 201, 'db body')",
        )
        .expect("insert message");

        let hit = SearchHit {
            title: "Stale Indexed Title".to_string(),
            snippet: String::new(),
            content: String::new(),
            content_hash: 0,
            conversation_id: Some(1),
            score: 0.0,
            source_path: shared_path.to_string(),
            agent: "cursor".to_string(),
            workspace: String::new(),
            workspace_original: None,
            created_at: None,
            line_number: None,
            match_type: Default::default(),
            source_id: "local".to_string(),
            origin_kind: "local".to_string(),
            origin_host: None,
        };

        let loaded = load_conversation_for_hit(&storage, &hit)
            .expect("load exact conversation")
            .expect("matching conversation");

        assert_eq!(loaded.convo.id, Some(1));
        assert_eq!(loaded.convo.title.as_deref(), Some("Database Title"));
    }

    #[test]
    fn load_conversation_for_hit_ignores_stale_title_when_exact_content_identifies_match() {
        use crate::storage::sqlite::FrankenStorage;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let db_path = tmp.path().join("cass.db");
        let storage = FrankenStorage::open(&db_path).expect("open storage");
        let conn = storage.raw();
        let shared_path = "/shared/cursor.sqlite";

        conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'cursor', 'Cursor', 'local', 0, 0)")
            .expect("insert agent");
        conn.execute(
            "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'old-ext', 'Old Session', '/shared/cursor.sqlite', 'local', 100)",
        )
        .expect("insert old conversation");
        conn.execute(
            "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (2, 1, 'new-ext', 'New Session', '/shared/cursor.sqlite', 'local', 200)",
        )
        .expect("insert new conversation");
        conn.execute(
            "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (1, 1, 0, 'user', 'old conversation body')",
        )
        .expect("insert old message");
        conn.execute(
            "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (2, 2, 0, 'user', 'new conversation body')",
        )
        .expect("insert new message");

        let hit = SearchHit {
            title: "Stale Indexed Title".to_string(),
            snippet: "new conversation body".to_string(),
            content: "new conversation body".to_string(),
            content_hash: 0,
            conversation_id: None,
            score: 0.0,
            source_path: shared_path.to_string(),
            agent: "cursor".to_string(),
            workspace: String::new(),
            workspace_original: None,
            created_at: None,
            line_number: Some(1),
            match_type: Default::default(),
            source_id: "local".to_string(),
            origin_kind: "local".to_string(),
            origin_host: None,
        };

        let loaded = load_conversation_for_hit(&storage, &hit)
            .expect("load exact conversation")
            .expect("matching conversation");

        assert_eq!(loaded.convo.external_id.as_deref(), Some("new-ext"));
        assert_eq!(loaded.convo.title.as_deref(), Some("New Session"));
        assert_eq!(loaded.messages[0].content, "new conversation body");
    }

    #[test]
    fn load_conversation_for_hit_uses_title_only_identity_hint() {
        use crate::storage::sqlite::FrankenStorage;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let db_path = tmp.path().join("cass.db");
        let storage = FrankenStorage::open(&db_path).expect("open storage");
        let conn = storage.raw();
        let shared_path = "/shared/cursor.sqlite";

        conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'cursor', 'Cursor', 'local', 0, 0)")
            .expect("insert agent");
        conn.execute(
            "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'old-ext', 'Old Session', '/shared/cursor.sqlite', 'local', 100)",
        )
        .expect("insert old conversation");
        conn.execute(
            "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (2, 1, 'new-ext', 'New Session', '/shared/cursor.sqlite', 'local', 200)",
        )
        .expect("insert new conversation");
        conn.execute(
            "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (1, 1, 0, 'user', 'old conversation body')",
        )
        .expect("insert old message");
        conn.execute(
            "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (2, 2, 0, 'user', 'new conversation body')",
        )
        .expect("insert new message");

        let hit = SearchHit {
            title: "Old Session".to_string(),
            snippet: String::new(),
            content: String::new(),
            content_hash: 0,
            conversation_id: None,
            score: 0.0,
            source_path: shared_path.to_string(),
            agent: "cursor".to_string(),
            workspace: String::new(),
            workspace_original: None,
            created_at: None,
            line_number: None,
            match_type: Default::default(),
            source_id: "local".to_string(),
            origin_kind: "local".to_string(),
            origin_host: None,
        };

        let loaded = load_conversation_for_hit(&storage, &hit)
            .expect("load attempt succeeds")
            .expect("matching conversation");

        assert_eq!(loaded.convo.external_id.as_deref(), Some("old-ext"));
        assert_eq!(loaded.convo.title.as_deref(), Some("Old Session"));
    }

    #[test]
    fn load_conversation_for_hit_does_not_fall_back_to_wrong_conversation_when_identity_misses() {
        use crate::storage::sqlite::FrankenStorage;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let db_path = tmp.path().join("cass.db");
        let storage = FrankenStorage::open(&db_path).expect("open storage");
        let conn = storage.raw();
        let shared_path = "/shared/cursor.sqlite";

        conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'cursor', 'Cursor', 'local', 0, 0)")
            .expect("insert agent");
        conn.execute(
            "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'old-ext', 'Old Session', '/shared/cursor.sqlite', 'local', 100)",
        )
        .expect("insert old conversation");
        conn.execute(
            "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (2, 1, 'new-ext', 'New Session', '/shared/cursor.sqlite', 'local', 200)",
        )
        .expect("insert new conversation");
        conn.execute(
            "INSERT INTO messages (id, conversation_id, idx, role, created_at, content) VALUES (1, 1, 0, 'user', 101, 'old conversation body')",
        )
        .expect("insert old message");
        conn.execute(
            "INSERT INTO messages (id, conversation_id, idx, role, created_at, content) VALUES (2, 2, 0, 'user', 201, 'new conversation body')",
        )
        .expect("insert new message");

        let hit = SearchHit {
            title: "Missing Session".to_string(),
            snippet: "missing conversation body".to_string(),
            content: "missing conversation body".to_string(),
            content_hash: 0,
            conversation_id: None,
            score: 0.0,
            source_path: shared_path.to_string(),
            agent: "cursor".to_string(),
            workspace: String::new(),
            workspace_original: None,
            created_at: Some(999),
            line_number: Some(1),
            match_type: Default::default(),
            source_id: "local".to_string(),
            origin_kind: "local".to_string(),
            origin_host: None,
        };

        let loaded = load_conversation_for_hit(&storage, &hit).expect("load attempt succeeds");
        assert!(
            loaded.is_none(),
            "identity-mismatched hits must not fall back to an arbitrary conversation"
        );
    }

    #[test]
    fn test_cache_miss() {
        let cache = ConversationCache::new(10);

        // Get from empty cache
        let cached = cache.get(None, "/nonexistent/path.jsonl");
        assert!(cached.is_none());

        // Check stats
        let (hits, misses, _) = cache.stats().get();
        assert_eq!(hits, 0);
        assert_eq!(misses, 1);
    }

    #[test]
    fn test_cache_invalidation() {
        let cache = ConversationCache::new(10);
        let view = make_test_view(1);
        let source_path = "/test/path/1.jsonl";

        // Insert and verify
        cache.insert(None, source_path, view);
        assert!(cache.get(None, source_path).is_some());

        // Invalidate
        cache.invalidate(None, source_path);
        assert!(cache.get(None, source_path).is_none());
    }

    #[test]
    fn test_cache_invalidate_all() {
        let cache = ConversationCache::new(10);

        // Insert multiple entries
        for i in 0..5 {
            let view = make_test_view(i);
            let source_path = format!("/test/path/{}.jsonl", i);
            cache.insert(None, &source_path, view);
        }

        assert_eq!(cache.len(), 5);

        // Invalidate all
        cache.invalidate_all();
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_cache_lru_eviction() {
        let cache = ConversationCache::new(2); // 2 per shard, 32 total

        // Insert more entries than a single shard can hold
        // All entries go to same shard by using paths that hash to same shard
        // (in practice, FxHasher distributes well, so we insert many entries)
        for i in 0..100 {
            let view = make_test_view(i);
            let source_path = format!("/test/path/{}.jsonl", i);
            cache.insert(None, &source_path, view);
        }

        // Some early entries should have been evicted
        let (_, _, evictions) = cache.stats().get();
        assert!(evictions > 0, "Expected some evictions with small capacity");
    }

    #[test]
    fn test_cache_hit_rate() {
        let cache = ConversationCache::new(10);
        let view = make_test_view(1);
        let source_path = "/test/path/1.jsonl";

        // Initial hit rate is 0
        assert_eq!(cache.stats().hit_rate(), 0.0);

        // Insert and access twice (1 miss on insert lookup, then 2 hits)
        cache.insert(None, source_path, view);
        let _ = cache.get(None, source_path);
        let _ = cache.get(None, source_path);

        // Hit rate should be positive (2 hits / 2 total)
        let hit_rate = cache.stats().hit_rate();
        assert!(
            hit_rate > 0.5,
            "Expected >50% hit rate, got {:.1}%",
            hit_rate * 100.0
        );
    }

    #[test]
    fn test_cache_shard_distribution() {
        let cache = ConversationCache::new(100);

        // Insert 1000 entries
        for i in 0..1000 {
            let view = make_test_view(i);
            let source_path = format!("/various/paths/{}/session.jsonl", i);
            cache.insert(None, &source_path, view);
        }

        // All entries should be cached
        assert_eq!(cache.len(), 1000);
    }

    #[test]
    fn test_cache_concurrent_access() {
        use std::thread;

        let cache = Arc::new(ConversationCache::new(100));
        let mut handles = vec![];

        // Spawn writers
        for t in 0..4 {
            let cache = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for i in 0..250 {
                    let id = t * 250 + i;
                    let view = make_test_view(id);
                    let source_path = format!("/test/path/{}.jsonl", id);
                    cache.insert(None, &source_path, view);
                }
            }));
        }

        // Spawn readers
        for _ in 0..4 {
            let cache = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for i in 0..1000 {
                    let source_path = format!("/test/path/{}.jsonl", i);
                    let _ = cache.get(None, &source_path);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Verify cache is consistent
        let (hits, misses, _) = cache.stats().get();
        assert!(hits + misses > 0, "Expected some cache operations");
    }

}
