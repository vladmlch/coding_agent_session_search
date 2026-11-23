use anyhow::{Result, anyhow, bail};
use frankensearch::{
    InMemoryTwoTierIndex as FsInMemoryTwoTierIndex, InMemoryVectorIndex as FsInMemoryVectorIndex,
    TwoTierIndex as FsTwoTierIndex, VectorHit as FsVectorHit,
    core::filter::SearchFilter as FsSearchFilter,
    index::{HnswIndex as FsHnswIndex, VectorIndex as FsVectorIndex},
};
use lru::LruCache;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::search::canonicalize::canonicalize_for_embedding;
use crate::search::embedder::Embedder;
use crate::search::vector_index::{
    SemanticAnnUnavailableReason, SemanticFilter, SemanticFilterMaps, SemanticIndexArtifact,
    SemanticProgressiveUnavailableReason, VectorSearchResult, parse_semantic_doc_id,
};

use super::cache::QueryCache;
use super::parsing::ensure_query_nesting_depth;
use super::session_scope::{
    SEMANTIC_SESSION_SCOPE_MAX_MESSAGES, SessionScopedSemanticFilter,
    load_semantic_session_message_ids,
};
use super::sqlite_hydration::SEMANTIC_EXACT_CHUNK_OVERFETCH_MULTIPLIER;
use super::types::{
    FieldMask, PROGRESSIVE_EMBEDDING_CACHE_CAPACITY, SearchFilters, SearchHit, SemanticTierMode,
    effective_field_mask, no_limit_result_cap,
};
use super::{SearchClient, ann_shards, message_stream, message_topk};

/// Intersect the original scope with message-level refill constraints before
/// vector top-k. Hash-only admission cannot prove either message constraint.
pub(in crate::search::query) struct ExactMessageRefillFilter<'a> {
    pub(in crate::search::query) base: Option<&'a dyn FsSearchFilter>,
    pub(in crate::search::query) excluded: &'a HashSet<u64>,
    pub(in crate::search::query) ceiling: Option<u64>,
}

impl FsSearchFilter for ExactMessageRefillFilter<'_> {
    fn matches(&self, doc_id: &str, metadata: Option<&serde_json::Value>) -> bool {
        let Some(rest) = doc_id.strip_prefix("m|") else {
            return false;
        };
        let mut parts = rest.splitn(3, '|');
        let Some(message_id) = parts.next().and_then(|value| value.parse::<u64>().ok()) else {
            return false;
        };
        if parts
            .next()
            .and_then(|value| value.parse::<u8>().ok())
            .is_none()
            || self.excluded.contains(&message_id)
            || self.ceiling.is_some_and(|ceiling| message_id >= ceiling)
        {
            return false;
        }
        crate::search::vector_index::parse_semantic_doc_id_filter_view(doc_id).is_some()
            && self.base.is_none_or(|base| base.matches(doc_id, metadata))
    }

    fn matches_doc_id_hash(
        &self,
        _hash: u64,
        _metadata: Option<&serde_json::Value>,
    ) -> Option<bool> {
        None
    }

    fn name(&self) -> &str {
        "cass_exact_message_refill_filter"
    }
}

/// Returns `Some(&filter)` when the filter has at least one active constraint,
/// `None` when unrestricted (skip filtering for performance).
pub(in crate::search::query) fn semantic_filter_as_search_filter(
    filter: &SemanticFilter,
) -> Option<&dyn FsSearchFilter> {
    let unrestricted = filter.agents.is_none()
        && filter.workspaces.is_none()
        && filter.sources.is_none()
        && filter.roles.is_none()
        && filter.created_from.is_none()
        && filter.created_to.is_none();
    if unrestricted { None } else { Some(filter) }
}

#[derive(Debug)]
pub(in crate::search::query) struct SemanticAnnOpenFailure {
    pub(in crate::search::query) reason: SemanticAnnUnavailableReason,
    pub(in crate::search::query) diagnostic: &'static str,
}

pub(in crate::search::query) fn open_fs_semantic_ann_index(
    fs_index: &FsVectorIndex,
    ann_path: &Path,
) -> std::result::Result<FsHnswIndex, SemanticAnnOpenFailure> {
    match std::fs::symlink_metadata(ann_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(SemanticAnnOpenFailure {
                reason: SemanticAnnUnavailableReason::SidecarMissing,
                diagnostic: "selected_metadata_missing",
            });
        }
        Err(_) => {
            return Err(SemanticAnnOpenFailure {
                reason: SemanticAnnUnavailableReason::SidecarOpenFailed,
                diagnostic: "selected_metadata_stat_failed",
            });
        }
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(SemanticAnnOpenFailure {
                reason: SemanticAnnUnavailableReason::SidecarOpenFailed,
                diagnostic: "selected_metadata_symlink",
            });
        }
        Ok(metadata) if !metadata.file_type().is_file() => {
            return Err(SemanticAnnOpenFailure {
                reason: SemanticAnnUnavailableReason::SidecarOpenFailed,
                diagnostic: "selected_metadata_not_regular_file",
            });
        }
        Ok(_) => {}
    }

    match FsHnswIndex::try_load_native(ann_path, fs_index) {
        Ok(Some(ann)) => Ok(ann),
        Ok(None) => Err(SemanticAnnOpenFailure {
            reason: SemanticAnnUnavailableReason::SidecarNotNative,
            diagnostic: "selected_graph_not_native",
        }),
        Err(_) => Err(SemanticAnnOpenFailure {
            reason: SemanticAnnUnavailableReason::SidecarOpenFailed,
            diagnostic: "selected_graph_open_failed",
        }),
    }
}

pub(in crate::search::query) struct SemanticSearchState {
    pub(in crate::search::query) context_token: Arc<()>,
    pub(in crate::search::query) embedder: Arc<dyn Embedder>,
    pub(in crate::search::query) artifacts: Arc<Vec<SemanticIndexArtifact>>,
    pub(in crate::search::query) quality_artifact: Option<SemanticIndexArtifact>,
    pub(in crate::search::query) fs_ann_index: Option<Arc<ann_shards::SemanticAnnShardSet>>,
    pub(in crate::search::query) fs_ann_unavailable: Option<SemanticAnnUnavailableReason>,
    pub(in crate::search::query) fs_ann_fallback_reported: bool,
    pub(in crate::search::query) fs_in_memory_two_tier_index: Option<Arc<FsInMemoryTwoTierIndex>>,
    pub(in crate::search::query) in_memory_two_tier_unavailable: InMemoryTwoTierUnavailable,
    pub(in crate::search::query) progressive_context: Option<Arc<ProgressiveTwoTierContext>>,
    pub(in crate::search::query) progressive_context_unavailable:
        Option<SemanticProgressiveUnavailableReason>,
    pub(in crate::search::query) filter_maps: SemanticFilterMaps,
    pub(in crate::search::query) roles: Option<HashSet<u8>>,
    pub(in crate::search::query) query_cache: QueryCache,
}

#[derive(Debug, Clone, Copy, Default)]
pub(in crate::search::query) struct InMemoryTwoTierUnavailable {
    pub(in crate::search::query) fast_only: Option<SemanticProgressiveUnavailableReason>,
    pub(in crate::search::query) quality: Option<SemanticProgressiveUnavailableReason>,
}

impl InMemoryTwoTierUnavailable {
    pub(in crate::search::query) fn reason(
        self,
        tier_mode: SemanticTierMode,
    ) -> Option<SemanticProgressiveUnavailableReason> {
        match tier_mode {
            SemanticTierMode::Single => None,
            SemanticTierMode::FastOnly => self.fast_only,
            SemanticTierMode::Progressive | SemanticTierMode::QualityOnly => self.quality,
        }
    }

    pub(in crate::search::query) fn mark_unavailable(
        &mut self,
        tier_mode: SemanticTierMode,
        reason: SemanticProgressiveUnavailableReason,
    ) {
        match tier_mode {
            SemanticTierMode::Single => {}
            SemanticTierMode::FastOnly => {
                self.fast_only = Some(reason);
            }
            SemanticTierMode::Progressive | SemanticTierMode::QualityOnly => {
                self.quality = Some(reason);
            }
        }
    }

    pub(in crate::search::query) fn clear_available(&mut self, tier_mode: SemanticTierMode) {
        match tier_mode {
            SemanticTierMode::Single => {}
            SemanticTierMode::FastOnly => self.fast_only = None,
            SemanticTierMode::Progressive | SemanticTierMode::QualityOnly => self.quality = None,
        }
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::search::query) struct ProgressiveTwoTierContext {
    pub(in crate::search::query) context_token: Arc<()>,
    pub(in crate::search::query) index: Arc<FsTwoTierIndex>,
    pub(in crate::search::query) fast_embedder: Arc<dyn frankensearch::Embedder>,
    pub(in crate::search::query) quality_embedder: Option<Arc<dyn frankensearch::Embedder>>,
}

#[derive(Clone)]
pub(in crate::search::query) struct SemanticCandidateContext {
    pub(in crate::search::query) artifacts: Arc<Vec<SemanticIndexArtifact>>,
    pub(in crate::search::query) filter_maps: SemanticFilterMaps,
    pub(in crate::search::query) roles: Option<HashSet<u8>>,
}

pub(in crate::search::query) struct SemanticCandidateSearchRequest<'a> {
    pub(in crate::search::query) fetch_limit: usize,
    pub(in crate::search::query) approximate: bool,
    pub(in crate::search::query) tier_mode: SemanticTierMode,
    pub(in crate::search::query) in_memory_two_tier_index: Option<&'a Arc<FsInMemoryTwoTierIndex>>,
    pub(in crate::search::query) ann_index: Option<&'a Arc<ann_shards::SemanticAnnShardSet>>,
}

#[derive(Debug, Clone, Copy, Default)]
pub(in crate::search::query) struct SemanticCandidateRetryState {
    pub(in crate::search::query) has_more_candidates: bool,
    pub(in crate::search::query) exact_window_may_omit_competitor: bool,
}

pub(in crate::search::query) struct SemanticQueryEmbedding {
    pub(in crate::search::query) context_token: Arc<()>,
    pub(in crate::search::query) vector: Vec<f32>,
}

pub(in crate::search::query) struct SharedCassSyncEmbedder {
    pub(in crate::search::query) inner: Arc<dyn Embedder>,
    pub(in crate::search::query) cache: Mutex<LruCache<String, Vec<f32>>>,
}

impl SharedCassSyncEmbedder {
    pub(in crate::search::query) fn new(inner: Arc<dyn Embedder>) -> Self {
        let cache_capacity =
            NonZeroUsize::new(PROGRESSIVE_EMBEDDING_CACHE_CAPACITY).expect("cache capacity > 0");
        Self {
            inner,
            cache: Mutex::new(LruCache::new(cache_capacity)),
        }
    }
}

impl Embedder for SharedCassSyncEmbedder {
    fn embed_sync(&self, text: &str) -> crate::search::embedder::EmbedderResult<Vec<f32>> {
        if let Ok(mut cache) = self.cache.lock()
            && let Some(embedding) = cache.get(text).cloned()
        {
            return Ok(embedding);
        }

        let embedding = self.inner.embed_sync(text)?;
        if let Ok(mut cache) = self.cache.lock() {
            cache.put(text.to_owned(), embedding.clone());
        }
        Ok(embedding)
    }

    fn embed_batch_sync(
        &self,
        texts: &[&str],
    ) -> crate::search::embedder::EmbedderResult<Vec<Vec<f32>>> {
        self.inner.embed_batch_sync(texts)
    }

    fn dimension(&self) -> usize {
        self.inner.dimension()
    }

    fn id(&self) -> &str {
        self.inner.id()
    }

    fn model_name(&self) -> &str {
        self.inner.model_name()
    }

    fn is_ready(&self) -> bool {
        self.inner.is_ready()
    }

    fn is_semantic(&self) -> bool {
        self.inner.is_semantic()
    }

    fn category(&self) -> frankensearch::ModelCategory {
        self.inner.category()
    }

    fn tier(&self) -> frankensearch::ModelTier {
        self.inner.tier()
    }

    fn supports_mrl(&self) -> bool {
        self.inner.supports_mrl()
    }
}

pub(in crate::search::query) fn build_in_memory_two_tier_index(
    fast_path: &Path,
    quality_path: Option<&Path>,
    tier_mode: SemanticTierMode,
) -> std::result::Result<Arc<FsInMemoryTwoTierIndex>, SemanticProgressiveUnavailableReason> {
    let fast = match FsInMemoryVectorIndex::from_fsvi(fast_path) {
        Ok(index) => index,
        Err(err) => {
            tracing::debug!(
                path = %fast_path.display(),
                error = %err,
                "fast semantic artifact load failed"
            );
            return Err(SemanticProgressiveUnavailableReason::ExactArtifactOpenFailed);
        }
    };

    let quality = if matches!(
        tier_mode,
        SemanticTierMode::Progressive | SemanticTierMode::QualityOnly
    ) {
        let quality_path =
            quality_path.ok_or(SemanticProgressiveUnavailableReason::QualityArtifactMissing)?;
        match same_file::is_same_file(fast_path, quality_path) {
            Ok(true) => {
                return Err(SemanticProgressiveUnavailableReason::ExactArtifactRoleAlias);
            }
            Ok(false) => {}
            Err(err) => {
                tracing::debug!(
                    fast_path = %fast_path.display(),
                    quality_path = %quality_path.display(),
                    error = %err,
                    "compare exact semantic artifact roles failed"
                );
                return Err(SemanticProgressiveUnavailableReason::ExactArtifactOpenFailed);
            }
        }
        match FsInMemoryVectorIndex::from_fsvi(quality_path) {
            Ok(index) => Some(index),
            Err(err) => {
                tracing::debug!(
                    path = %quality_path.display(),
                    error = %err,
                    "quality semantic artifact load failed"
                );
                return Err(SemanticProgressiveUnavailableReason::ExactArtifactOpenFailed);
            }
        }
    } else {
        None
    };

    Ok(Arc::new(FsInMemoryTwoTierIndex::new(fast, quality)))
}

pub(in crate::search::query) fn two_tier_index_supports_mode(
    index: &FsInMemoryTwoTierIndex,
    tier_mode: SemanticTierMode,
) -> bool {
    !matches!(
        tier_mode,
        SemanticTierMode::Progressive | SemanticTierMode::QualityOnly
    ) || index.has_quality_index()
}

impl SearchClient {
    pub fn set_semantic_context(
        &self,
        embedder: Arc<dyn Embedder>,
        artifact: SemanticIndexArtifact,
        quality_artifact: Option<SemanticIndexArtifact>,
        filter_maps: SemanticFilterMaps,
        roles: Option<HashSet<u8>>,
    ) -> Result<()> {
        self.set_semantic_artifacts_context(
            embedder,
            vec![artifact],
            quality_artifact,
            filter_maps,
            roles,
        )
    }

    pub fn set_semantic_artifacts_context(
        &self,
        embedder: Arc<dyn Embedder>,
        artifacts: Vec<SemanticIndexArtifact>,
        quality_artifact: Option<SemanticIndexArtifact>,
        filter_maps: SemanticFilterMaps,
        roles: Option<HashSet<u8>>,
    ) -> Result<()> {
        if artifacts.is_empty() {
            bail!("semantic context requires at least one vector index");
        }

        for artifact in &artifacts {
            let index = artifact.index();
            let embedder_id = index.embedder_id();
            let dimension = index.dimension();
            if embedder_id != embedder.id() {
                bail!(
                    "embedder mismatch: index uses {}, embedder is {}",
                    embedder_id,
                    embedder.id()
                );
            }
            if dimension != embedder.dimension() {
                bail!(
                    "embedder dimension mismatch: index uses {}, embedder is {}",
                    dimension,
                    embedder.dimension()
                );
            }
        }
        let embedder_id = artifacts[0].index().embedder_id().to_string();
        let dimension = artifacts[0].index().dimension();
        let shard_count = artifacts.len();
        let fs_ann_unavailable = ann_shards::SemanticAnnShardSet::unavailable_reason(&artifacts);
        let artifacts = Arc::new(artifacts);

        let capacity = NonZeroUsize::new(100).ok_or_else(|| anyhow!("invalid cache size"))?;
        let context_token = Arc::new(());
        let mut state_guard = self
            .semantic
            .lock()
            .map_err(|_| anyhow!("semantic lock poisoned"))?;
        *state_guard = Some(SemanticSearchState {
            context_token,
            embedder,
            artifacts,
            quality_artifact,
            fs_ann_index: None,
            fs_ann_unavailable,
            fs_ann_fallback_reported: false,
            fs_in_memory_two_tier_index: None,
            in_memory_two_tier_unavailable: InMemoryTwoTierUnavailable::default(),
            progressive_context: None,
            progressive_context_unavailable: None,
            filter_maps,
            roles,
            query_cache: QueryCache::new(embedder_id.as_str(), capacity),
        });
        if shard_count > 1 {
            tracing::info!(
                shard_count,
                dimension,
                embedder = embedder_id,
                "semantic search context loaded sharded vector generation"
            );
        }
        Ok(())
    }

    pub fn clear_semantic_context(&self) -> Result<()> {
        let mut guard = self
            .semantic
            .lock()
            .map_err(|_| anyhow!("semantic lock poisoned"))?;
        *guard = None;
        Ok(())
    }

    pub(in crate::search::query) fn semantic_context_matches(
        &self,
        context_token: &Arc<()>,
    ) -> Result<bool> {
        let guard = self
            .semantic
            .lock()
            .map_err(|_| anyhow!("semantic lock poisoned"))?;
        Ok(guard
            .as_ref()
            .is_some_and(|state| Arc::ptr_eq(&state.context_token, context_token)))
    }

    pub(in crate::search::query) fn semantic_query_embedding(
        &self,
        canonical: &str,
    ) -> Result<SemanticQueryEmbedding> {
        loop {
            let (embedder, context_token) = {
                let mut guard = self
                    .semantic
                    .lock()
                    .map_err(|_| anyhow!("semantic lock poisoned"))?;
                let state = guard.as_mut().ok_or_else(|| {
                    anyhow!("semantic search unavailable (no embedder or vector index)")
                })?;
                if let Some(hit) = state
                    .query_cache
                    .get_cached(state.embedder.as_ref(), canonical)
                {
                    return Ok(SemanticQueryEmbedding {
                        context_token: Arc::clone(&state.context_token),
                        vector: hit,
                    });
                }
                (
                    Arc::clone(&state.embedder),
                    Arc::clone(&state.context_token),
                )
            };

            let embedding = embedder
                .embed_sync(canonical)
                .map_err(|e| anyhow!("embedding failed: {e}"))?;

            let mut guard = self
                .semantic
                .lock()
                .map_err(|_| anyhow!("semantic lock poisoned"))?;
            let state = guard.as_mut().ok_or_else(|| {
                anyhow!("semantic search unavailable (no embedder or vector index)")
            })?;
            if !Arc::ptr_eq(&state.context_token, &context_token) {
                continue;
            }
            if let Some(hit) = state
                .query_cache
                .get_cached(state.embedder.as_ref(), canonical)
            {
                return Ok(SemanticQueryEmbedding {
                    context_token,
                    vector: hit,
                });
            }
            state
                .query_cache
                .store(state.embedder.as_ref(), canonical, embedding.clone());
            return Ok(SemanticQueryEmbedding {
                context_token,
                vector: embedding,
            });
        }
    }

    pub(in crate::search::query) fn in_memory_two_tier_index(
        &self,
        tier_mode: SemanticTierMode,
    ) -> Result<Option<Arc<FsInMemoryTwoTierIndex>>> {
        loop {
            let (fast_path, quality_path, context_token) = {
                let mut guard = self
                    .semantic
                    .lock()
                    .map_err(|_| anyhow!("semantic lock poisoned"))?;
                let state = guard.as_mut().ok_or_else(|| {
                    anyhow!("semantic search unavailable (no embedder or vector index)")
                })?;
                let fast_owner_backed = state
                    .artifacts
                    .iter()
                    .all(SemanticIndexArtifact::has_owner_backed_progressive_reader);
                let quality_owner_backed = !matches!(
                    tier_mode,
                    SemanticTierMode::Progressive | SemanticTierMode::QualityOnly
                ) || state
                    .quality_artifact
                    .as_ref()
                    .is_some_and(SemanticIndexArtifact::has_owner_backed_progressive_reader);
                if !fast_owner_backed || !quality_owner_backed {
                    let reason = SemanticProgressiveUnavailableReason::OwnerBackedReaderRequired;
                    tracing::debug!(
                        reason = reason.code(),
                        ?tier_mode,
                        "two-tier semantic unavailable"
                    );
                    state
                        .in_memory_two_tier_unavailable
                        .mark_unavailable(tier_mode, reason);
                    return Ok(None);
                }
                if let Some(index) = state.fs_in_memory_two_tier_index.as_ref()
                    && two_tier_index_supports_mode(index.as_ref(), tier_mode)
                {
                    return Ok(Some(Arc::clone(index)));
                }
                if let Some(reason) = state.in_memory_two_tier_unavailable.reason(tier_mode) {
                    tracing::debug!(
                        reason = reason.code(),
                        ?tier_mode,
                        "two-tier semantic remains unavailable"
                    );
                    return Ok(None);
                }
                if state.artifacts.len() != 1 {
                    let reason = SemanticProgressiveUnavailableReason::MultipleExactShards;
                    tracing::debug!(
                        reason = reason.code(),
                        shard_count = state.artifacts.len(),
                        "two-tier semantic unavailable"
                    );
                    state
                        .in_memory_two_tier_unavailable
                        .mark_unavailable(tier_mode, reason);
                    return Ok(None);
                }
                (
                    state.artifacts[0].fsvi_path().to_path_buf(),
                    state
                        .quality_artifact
                        .as_ref()
                        .map(|artifact| artifact.fsvi_path().to_path_buf()),
                    Arc::clone(&state.context_token),
                )
            };

            let index =
                build_in_memory_two_tier_index(&fast_path, quality_path.as_deref(), tier_mode);

            let mut guard = self
                .semantic
                .lock()
                .map_err(|_| anyhow!("semantic lock poisoned"))?;
            let state = guard.as_mut().ok_or_else(|| {
                anyhow!("semantic search unavailable (no embedder or vector index)")
            })?;
            if let Some(existing) = state.fs_in_memory_two_tier_index.as_ref()
                && two_tier_index_supports_mode(existing.as_ref(), tier_mode)
            {
                return Ok(Some(Arc::clone(existing)));
            }
            if !Arc::ptr_eq(&state.context_token, &context_token) {
                continue;
            }
            let index = match index {
                Ok(index) => index,
                Err(reason) => {
                    tracing::debug!(
                        reason = reason.code(),
                        ?tier_mode,
                        "two-tier semantic unavailable"
                    );
                    state
                        .in_memory_two_tier_unavailable
                        .mark_unavailable(tier_mode, reason);
                    return Ok(None);
                }
            };
            if !two_tier_index_supports_mode(index.as_ref(), tier_mode) {
                let reason = SemanticProgressiveUnavailableReason::QualityArtifactMissing;
                state
                    .in_memory_two_tier_unavailable
                    .mark_unavailable(tier_mode, reason);
                return Ok(None);
            }
            state.fs_in_memory_two_tier_index = Some(Arc::clone(&index));
            if index.has_quality_index() {
                state.in_memory_two_tier_unavailable = InMemoryTwoTierUnavailable::default();
            } else {
                state
                    .in_memory_two_tier_unavailable
                    .clear_available(SemanticTierMode::FastOnly);
            }
            return Ok(Some(index));
        }
    }

    /// Resolve and return the stable reason an in-memory two-tier mode is
    /// unavailable. `None` means that mode is ready.
    pub fn two_tier_unavailability_reason(
        &self,
        tier_mode: SemanticTierMode,
    ) -> Result<Option<SemanticProgressiveUnavailableReason>> {
        let resolution = self.in_memory_two_tier_index(tier_mode);
        let reason = self
            .semantic
            .lock()
            .map_err(|_| anyhow!("semantic lock poisoned"))?
            .as_ref()
            .and_then(|state| state.in_memory_two_tier_unavailable.reason(tier_mode));
        match (resolution, reason) {
            (_, Some(reason)) => Ok(Some(reason)),
            (Ok(_), None) => Ok(None),
            (Err(error), None) => Err(error),
        }
    }

    pub(in crate::search::query) fn ann_index(
        &self,
    ) -> Result<Option<Arc<ann_shards::SemanticAnnShardSet>>> {
        loop {
            let (artifacts, context_token) = {
                let mut guard = self
                    .semantic
                    .lock()
                    .map_err(|_| anyhow!("semantic lock poisoned"))?;
                let state = guard.as_mut().ok_or_else(|| {
                    anyhow!("semantic search unavailable (no embedder or vector index)")
                })?;
                if let Some(index) = state.fs_ann_index.as_ref() {
                    return Ok(Some(Arc::clone(index)));
                }
                if let Some(reason) = state.fs_ann_unavailable {
                    if !state.fs_ann_fallback_reported {
                        tracing::warn!(
                            reason_code = reason.code(),
                            "approximate semantic index unavailable; using exact FSVI search"
                        );
                        state.fs_ann_fallback_reported = true;
                    }
                    return Ok(None);
                }
                (
                    Arc::clone(&state.artifacts),
                    Arc::clone(&state.context_token),
                )
            };

            let opened = ann_shards::SemanticAnnShardSet::open(artifacts);

            let mut guard = self
                .semantic
                .lock()
                .map_err(|_| anyhow!("semantic lock poisoned"))?;
            let state = guard.as_mut().ok_or_else(|| {
                anyhow!("semantic search unavailable (no embedder or vector index)")
            })?;
            if let Some(existing) = state.fs_ann_index.as_ref() {
                return Ok(Some(Arc::clone(existing)));
            }
            if !Arc::ptr_eq(&state.context_token, &context_token) {
                continue;
            }
            match opened {
                Ok(ann) => {
                    let ann = Arc::new(ann);
                    state.fs_ann_unavailable = None;
                    state.fs_ann_fallback_reported = false;
                    state.fs_ann_index = Some(Arc::clone(&ann));
                    return Ok(Some(ann));
                }
                Err(failure) => {
                    tracing::warn!(
                        reason_code = failure.reason.code(),
                        diagnostic = failure.diagnostic,
                        "approximate semantic index unavailable; using exact FSVI search"
                    );
                    state.fs_ann_unavailable = Some(failure.reason);
                    state.fs_ann_fallback_reported = true;
                    return Ok(None);
                }
            }
        }
    }

    /// Resolve and return the stable reason ANN acceleration is unavailable.
    ///
    /// `None` means the explicitly paired native ANN artifact is ready.
    pub fn ann_unavailability_reason(&self) -> Result<Option<SemanticAnnUnavailableReason>> {
        let resolution = self.ann_index();
        let reason = self
            .semantic
            .lock()
            .map_err(|_| anyhow!("semantic lock poisoned"))?
            .as_ref()
            .and_then(|state| state.fs_ann_unavailable);
        match (resolution, reason) {
            (_, Some(reason)) => Ok(Some(reason)),
            (Ok(_), None) => Ok(None),
            (Err(error), None) => Err(error),
        }
    }

    pub(in crate::search::query) fn collapse_semantic_results(
        best_by_message: HashMap<u64, VectorSearchResult>,
        fetch_limit: usize,
    ) -> Vec<VectorSearchResult> {
        let mut collapsed: Vec<VectorSearchResult> = best_by_message.into_values().collect();
        collapsed.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.message_id.cmp(&b.message_id))
        });
        if collapsed.len() > fetch_limit {
            collapsed.truncate(fetch_limit);
        }
        collapsed
    }

    pub(in crate::search::query) fn semantic_exact_candidate_limit(
        fetch_limit: usize,
        record_count: usize,
    ) -> usize {
        fetch_limit
            .saturating_mul(SEMANTIC_EXACT_CHUNK_OVERFETCH_MULTIPLIER)
            .max(fetch_limit)
            .min(record_count)
    }

    pub(in crate::search::query) fn semantic_window_may_omit_competitor(
        collapsed: &[VectorSearchResult],
        fetch_limit: usize,
        max_omitted_score: Option<f32>,
    ) -> bool {
        if fetch_limit == 0 {
            return false;
        }
        let Some(max_omitted_score) = max_omitted_score else {
            return false;
        };
        if collapsed.len() < fetch_limit {
            return true;
        }
        let Some(last_in_requested_window) = collapsed.get(fetch_limit - 1) else {
            return true;
        };
        !last_in_requested_window
            .score
            .total_cmp(&max_omitted_score)
            .is_gt()
    }

    pub(in crate::search::query) fn record_fs_semantic_hit(
        best_by_message: &mut HashMap<u64, VectorSearchResult>,
        hit: &FsVectorHit,
    ) {
        let Some(parsed) = parse_semantic_doc_id(&hit.doc_id) else {
            return;
        };
        best_by_message
            .entry(parsed.message_id)
            .and_modify(|entry| {
                if hit.score > entry.score {
                    entry.score = hit.score;
                    entry.chunk_idx = parsed.chunk_idx;
                }
            })
            .or_insert(VectorSearchResult {
                message_id: parsed.message_id,
                chunk_idx: parsed.chunk_idx,
                score: hit.score,
            });
    }

    /// Preserve the inexpensive first window when its score bound proves the
    /// requested message ranking. Chunk-dominated windows need a bounded exact
    /// refill, not one larger window that can still contain the same message.
    pub(in crate::search::query) fn search_exact_semantic_indexes(
        context: &SemanticCandidateContext,
        embedding: &[f32],
        fetch_limit: usize,
        fs_filter: Option<&dyn FsSearchFilter>,
    ) -> Result<(Vec<VectorSearchResult>, SemanticCandidateRetryState)> {
        Self::search_exact_semantic_indexes_with_refinement(
            context,
            embedding,
            fetch_limit,
            fs_filter,
            true,
        )
    }

    // Keep the incumbent for unsupported no-WAL widths and same-invocation
    // comparisons. The pinned backend repairs WAL-shadowed raw windows itself;
    // a retained delta alone is not a reason to bypass its proven first window.
    pub(in crate::search::query) fn search_exact_semantic_indexes_with_refinement(
        context: &SemanticCandidateContext,
        embedding: &[f32],
        fetch_limit: usize,
        fs_filter: Option<&dyn FsSearchFilter>,
        streaming: bool,
    ) -> Result<(Vec<VectorSearchResult>, SemanticCandidateRetryState)> {
        let (initial, retry) = Self::search_exact_semantic_indexes_initial_window(
            context,
            embedding,
            fetch_limit,
            fs_filter,
        )?;
        if !retry.exact_window_may_omit_competitor {
            return Ok((initial, retry));
        }
        let record_count = context.artifacts.iter().fold(0usize, |total, artifact| {
            total
                .saturating_add(artifact.index().record_count())
                .saturating_add(artifact.index().wal_record_count())
        });
        let return_limit = Self::semantic_exact_candidate_limit(fetch_limit, record_count);
        if streaming
            && let Some(hits) = message_stream::try_collect_retained_messages(
                &context.artifacts,
                embedding,
                return_limit,
                fs_filter,
            )?
        {
            let has_more_candidates = hits.len() >= return_limit && return_limit < record_count;
            return Ok((
                hits,
                SemanticCandidateRetryState {
                    has_more_candidates,
                    exact_window_may_omit_competitor: false,
                },
            ));
        }
        let mut refills_left = message_topk::MAX_EXACT_MESSAGE_REFILLS;
        let mut rounds = 0usize;
        let mut best_by_message = HashMap::<u64, VectorSearchResult>::new();
        for artifact in context.artifacts.iter() {
            let index = artifact.index();
            let physical_count = index
                .record_count()
                .saturating_add(index.wal_record_count());
            let target = return_limit.min(physical_count);
            // Retain the existing message-overfetch allowance for hydration.
            // Bound each raw window independently of the total archive size;
            // the extra lookahead proves strict score cutoffs without a refill.
            let window = target
                .saturating_mul(4)
                .saturating_add(1)
                .min(physical_count);
            let selection = message_topk::collect_exact_messages(
                target,
                window,
                refills_left + 1,
                |excluded, ceiling, window| {
                    let filter = ExactMessageRefillFilter {
                        base: fs_filter,
                        excluded,
                        ceiling,
                    };
                    let hits = index
                        .search_top_k(embedding, window.min(physical_count), Some(&filter))
                        .map_err(|error| anyhow!("exact semantic refill failed: {error}"))?;
                    hits.into_iter()
                        .map(|hit| {
                            let parsed = parse_semantic_doc_id(&hit.doc_id).ok_or_else(|| {
                                anyhow!(
                                    "exact semantic refill returned an invalid message identity"
                                )
                            })?;
                            Ok(VectorSearchResult {
                                message_id: parsed.message_id,
                                chunk_idx: parsed.chunk_idx,
                                score: hit.score,
                            })
                        })
                        .collect::<Result<Vec<_>>>()
                },
            )
            .map_err(|error| match error {
                message_topk::RefillError::Backend(error) => error,
                other => anyhow!("{other}"),
            })?;
            rounds = rounds.saturating_add(selection.rounds);
            refills_left -= selection.rounds.saturating_sub(1);
            for hit in selection.hits {
                best_by_message
                    .entry(hit.message_id)
                    .and_modify(|best| {
                        if hit.score.total_cmp(&best.score).is_gt() {
                            best.score = hit.score;
                            best.chunk_idx = hit.chunk_idx;
                        }
                    })
                    .or_insert(hit);
            }
            // Per-shard top-k messages suffice for global top-k. Prune after
            // each merge instead of retaining k messages times all shard count.
            best_by_message = Self::collapse_semantic_results(best_by_message, return_limit)
                .into_iter()
                .map(|hit| (hit.message_id, hit))
                .collect();
        }
        let hits = Self::collapse_semantic_results(best_by_message, return_limit);
        let has_more_candidates = hits.len() >= return_limit && return_limit < record_count;
        tracing::debug!(
            shard_count = context.artifacts.len(),
            rounds,
            returned = hits.len(),
            "exact semantic message refinement complete"
        );
        Ok((
            hits,
            SemanticCandidateRetryState {
                has_more_candidates,
                exact_window_may_omit_competitor: false,
            },
        ))
    }

    pub(in crate::search::query) fn search_exact_semantic_indexes_initial_window(
        context: &SemanticCandidateContext,
        embedding: &[f32],
        fetch_limit: usize,
        fs_filter: Option<&dyn FsSearchFilter>,
    ) -> Result<(Vec<VectorSearchResult>, SemanticCandidateRetryState)> {
        if context.artifacts.len() == 1 {
            let index = context.artifacts[0].index();
            // The retained query view includes WAL additions and replacements.
            // Main rows plus retained WAL rows bound physical candidates, not
            // distinct live messages; only the backend resolves replacements.
            let record_count = index
                .record_count()
                .saturating_add(index.wal_record_count());
            let candidate_limit = Self::semantic_exact_candidate_limit(fetch_limit, record_count);
            let fs_hits = index
                .search_top_k(embedding, candidate_limit, fs_filter)
                .map_err(|err| anyhow!("frankensearch semantic search failed: {err}"))?;
            let mut best_by_message = HashMap::with_capacity(fs_hits.len());
            for hit in &fs_hits {
                Self::record_fs_semantic_hit(&mut best_by_message, hit);
            }
            let collapsed = Self::collapse_semantic_results(best_by_message, candidate_limit);
            // FSVI deduplicates document IDs after raw top-k. A short nonempty
            // batch can therefore hide later messages even before our first
            // refill. Only an empty batch or a full-record window proves that
            // no candidates remain; retain the score bound in every other case.
            let has_more_candidates = !fs_hits.is_empty() && candidate_limit < record_count;
            let max_omitted_score = if has_more_candidates {
                fs_hits.last().map(|hit| hit.score)
            } else {
                None
            };
            let exact_window_may_omit_competitor = Self::semantic_window_may_omit_competitor(
                &collapsed,
                fetch_limit,
                max_omitted_score,
            );
            return Ok((
                collapsed,
                SemanticCandidateRetryState {
                    has_more_candidates,
                    exact_window_may_omit_competitor,
                },
            ));
        }

        let mut best_by_message = HashMap::new();
        let mut raw_hits = 0usize;
        let mut max_omitted_score: Option<f32> = None;
        let mut has_more_candidates = false;
        for artifact in context.artifacts.iter() {
            let index = artifact.index();
            // A main-empty shard can still have searchable durable WAL rows.
            let shard_record_count = index
                .record_count()
                .saturating_add(index.wal_record_count());
            // Search chunks, then collapse by message. A message can have many
            // high-scoring chunks, so per-shard top-k chunks alone is not a
            // proof of per-message top-k. Use a bounded overfetch window and
            // retry only when the omitted-score bound can still beat the last
            // collapsed message in the requested window.
            let shard_limit = Self::semantic_exact_candidate_limit(fetch_limit, shard_record_count);
            if shard_limit == 0 {
                continue;
            }
            let fs_hits = index
                .search_top_k(embedding, shard_limit, fs_filter)
                .map_err(|err| anyhow!("frankensearch sharded semantic search failed: {err}"))?;
            if shard_limit < shard_record_count
                && let Some(last_hit) = fs_hits.last()
            {
                has_more_candidates = true;
                max_omitted_score = Some(
                    max_omitted_score
                        .map(|current| current.max(last_hit.score))
                        .unwrap_or(last_hit.score),
                );
            }
            raw_hits = raw_hits.saturating_add(fs_hits.len());
            best_by_message.reserve(fs_hits.len());
            for hit in &fs_hits {
                Self::record_fs_semantic_hit(&mut best_by_message, hit);
            }
            // Max-score collapse distributes over bounded top-k merge. Keep
            // only the eventual return window after each shard, rather than
            // retaining a full candidate page for every opened shard.
            let keep = Self::semantic_exact_candidate_limit(fetch_limit, raw_hits);
            // Exhausting each physical shard does not exhaust the merged
            // candidate page when bounded global retention discards messages.
            has_more_candidates |= best_by_message.len() > keep;
            best_by_message = Self::collapse_semantic_results(best_by_message, keep)
                .into_iter()
                .map(|hit| (hit.message_id, hit))
                .collect();
        }
        let candidate_return_limit = Self::semantic_exact_candidate_limit(fetch_limit, raw_hits);
        let collapsed = Self::collapse_semantic_results(best_by_message, candidate_return_limit);
        let exact_window_may_omit_competitor =
            Self::semantic_window_may_omit_competitor(&collapsed, fetch_limit, max_omitted_score);
        tracing::debug!(
            shard_count = context.artifacts.len(),
            raw_hits,
            returned = collapsed.len(),
            "semantic sharded exact merge complete"
        );
        Ok((
            collapsed,
            SemanticCandidateRetryState {
                has_more_candidates,
                exact_window_may_omit_competitor,
            },
        ))
    }

    pub(in crate::search::query) fn search_semantic_candidates(
        &self,
        context: &SemanticCandidateContext,
        embedding: &[f32],
        filters: &SearchFilters,
        request: SemanticCandidateSearchRequest<'_>,
    ) -> Result<(
        Vec<VectorSearchResult>,
        SemanticCandidateRetryState,
        Option<crate::search::ann_index::AnnSearchStats>,
    )> {
        let mut semantic_filter =
            SemanticFilter::from_search_filters(filters, &context.filter_maps)?;
        if let Some(roles) = context.roles.clone() {
            semantic_filter = semantic_filter.with_roles(Some(roles));
        }

        if !filters.session_paths.is_empty() {
            let message_ids = {
                let sqlite_guard = self.sqlite_guard()?;
                let conn = sqlite_guard.as_ref().ok_or_else(|| {
                    anyhow!("session-scoped semantic search requires database connection")
                })?;
                load_semantic_session_message_ids(
                    conn,
                    &filters.session_paths,
                    SEMANTIC_SESSION_SCOPE_MAX_MESSAGES,
                )?
            };
            // No selected archive messages means no matching vector records.
            // In particular, never interpret an empty allowlist as unrestricted.
            if message_ids.is_empty() {
                return Ok((
                    Vec::new(),
                    SemanticCandidateRetryState {
                        has_more_candidates: false,
                        exact_window_may_omit_competitor: false,
                    },
                    None,
                ));
            }
            let scoped_filter = SessionScopedSemanticFilter {
                metadata: &semantic_filter,
                message_ids: &message_ids,
            };
            // The native ANN lane selects a global window before filtering.
            // It cannot guarantee recall for a narrow session selection; use
            // the retained exact owners with the filter inside top-k instead.
            // None here means no ANN statistics: this was an exact search.
            let (results, retry_state) = Self::search_exact_semantic_indexes(
                context,
                embedding,
                request.fetch_limit,
                Some(&scoped_filter),
            )?;
            return Ok((results, retry_state, None));
        }

        if request.tier_mode.wants_two_tier() && !request.approximate {
            tracing::debug!(
                tier_mode = ?request.tier_mode,
                owner_backed_index_available = request.in_memory_two_tier_index.is_some(),
                "per-tier bound query embeddings unavailable; falling back to exact single-tier search"
            );

            let fs_filter = semantic_filter_as_search_filter(&semantic_filter);
            let (results, truncated) = Self::search_exact_semantic_indexes(
                context,
                embedding,
                request.fetch_limit,
                fs_filter,
            )?;
            return Ok((results, truncated, None));
        }

        if request.approximate {
            if request.tier_mode.wants_two_tier() {
                tracing::debug!(
                    tier_mode = ?request.tier_mode,
                    "approximate search requested; bypassing two-tier mode"
                );
            }
            let ann = request
                .ann_index
                .ok_or_else(|| anyhow!("HNSW cohort failed to initialize"))?;
            return ann.search_with_exact_fallback(
                context,
                embedding,
                request.fetch_limit,
                semantic_filter_as_search_filter(&semantic_filter),
            );
        }

        let fs_filter = semantic_filter_as_search_filter(&semantic_filter);
        let (results, truncated) = Self::search_exact_semantic_indexes(
            context,
            embedding,
            request.fetch_limit,
            fs_filter,
        )?;
        Ok((results, truncated, None))
    }

    /// Semantic search result containing hits and optional ANN statistics.
    pub fn search_semantic(
        &self,
        query: &str,
        filters: SearchFilters,
        limit: usize,
        offset: usize,
        field_mask: FieldMask,
        approximate: bool,
    ) -> Result<(
        Vec<SearchHit>,
        Option<crate::search::ann_index::AnnSearchStats>,
    )> {
        self.search_semantic_with_tier(
            query,
            filters,
            limit,
            offset,
            field_mask,
            approximate,
            SemanticTierMode::Single,
        )
    }

    /// Semantic search with optional progressive two-tier execution strategy.
    #[allow(clippy::too_many_arguments)]
    pub fn search_semantic_with_tier(
        &self,
        query: &str,
        filters: SearchFilters,
        limit: usize,
        offset: usize,
        field_mask: FieldMask,
        approximate: bool,
        tier_mode: SemanticTierMode,
    ) -> Result<(
        Vec<SearchHit>,
        Option<crate::search::ann_index::AnnSearchStats>,
    )> {
        // Same bound as the lexical entry: the semantic lane never parses
        // the boolean grammar (it embeds the query), but a too-deep query
        // must get the same typed verdict in every mode, not an answer
        // here and a refusal there.
        ensure_query_nesting_depth(query)?;
        let field_mask = effective_field_mask(field_mask);
        let canonical = canonicalize_for_embedding(query);
        if canonical.trim().is_empty() {
            return Ok((Vec::new(), None));
        }
        loop {
            let (
                embedding,
                candidate_context,
                in_memory_two_tier_index,
                ann_index,
                effective_approximate,
                effective_tier_mode,
                context_token,
            ) = loop {
                let embedding = self.semantic_query_embedding(&canonical)?;
                let (candidate_context, context_token) = {
                    let guard = self
                        .semantic
                        .lock()
                        .map_err(|_| anyhow!("semantic lock poisoned"))?;
                    let state = guard.as_ref().ok_or_else(|| {
                        anyhow!("semantic search unavailable (no embedder or vector index)")
                    })?;
                    (
                        SemanticCandidateContext {
                            artifacts: Arc::clone(&state.artifacts),
                            filter_maps: state.filter_maps.clone(),
                            roles: state.roles.clone(),
                        },
                        Arc::clone(&state.context_token),
                    )
                };
                if !Arc::ptr_eq(&embedding.context_token, &context_token) {
                    continue;
                }
                let in_memory_two_tier_index = if tier_mode.wants_two_tier()
                    && !approximate
                    && filters.session_paths.is_empty()
                {
                    self.in_memory_two_tier_index(tier_mode)?
                } else {
                    None
                };
                // Session-scoped selection uses exact filtered vectors. Do not
                // load the global ANN graph for a query that cannot use it.
                let ann_index = if approximate && filters.session_paths.is_empty() {
                    self.ann_index()?
                } else {
                    None
                };
                let effective_approximate = approximate && ann_index.is_some();
                let effective_tier_mode = if approximate {
                    SemanticTierMode::Single
                } else {
                    tier_mode
                };

                let guard = self
                    .semantic
                    .lock()
                    .map_err(|_| anyhow!("semantic lock poisoned"))?;
                let state = guard.as_ref().ok_or_else(|| {
                    anyhow!("semantic search unavailable (no embedder or vector index)")
                })?;
                if !Arc::ptr_eq(&state.context_token, &context_token) {
                    continue;
                }
                break (
                    embedding.vector,
                    candidate_context,
                    in_memory_two_tier_index,
                    ann_index,
                    effective_approximate,
                    effective_tier_mode,
                    context_token,
                );
            };

            // A semantic-only client deliberately has no lexical reader. Use
            // the complete retained vector view for an unlimited request's
            // cap: durable WAL additions are searchable before consolidation,
            // including when a shard has no main records. Main rows plus WAL
            // rows safely bound live messages even when updates replace rows.
            let limit = if limit == 0 {
                candidate_context
                    .artifacts
                    .iter()
                    .fold(0usize, |count, artifact| {
                        count
                            .saturating_add(artifact.index().record_count())
                            .saturating_add(artifact.index().wal_record_count())
                    })
                    .min(no_limit_result_cap())
                    .max(1)
            } else {
                limit
            };
            let target_hits = limit.saturating_add(offset);
            let initial_fetch_limit = target_hits;
            let fallback_fetch_limit = target_hits.saturating_mul(3);
            let finalize_hits =
                |results: &[VectorSearchResult]| -> Result<(usize, Vec<SearchHit>)> {
                    let hits = self.hydrate_semantic_hits(results, field_mask)?;
                    Ok(self.postprocess_hits_page(hits, query, &filters, limit, offset))
                };

            let (results, retry_state, mut ann_stats) = self.search_semantic_candidates(
                &candidate_context,
                &embedding,
                &filters,
                SemanticCandidateSearchRequest {
                    fetch_limit: initial_fetch_limit,
                    approximate: effective_approximate,
                    tier_mode: effective_tier_mode,
                    in_memory_two_tier_index: in_memory_two_tier_index.as_ref(),
                    ann_index: ann_index.as_ref(),
                },
            )?;
            if !self.semantic_context_matches(&context_token)? {
                tracing::debug!("semantic context changed during candidate search; retrying");
                continue;
            }
            let (mut available_hits, mut paged_hits) = finalize_hits(&results)?;

            let needs_retry = initial_fetch_limit < fallback_fetch_limit
                && ((available_hits < target_hits && retry_state.has_more_candidates)
                    || retry_state.exact_window_may_omit_competitor);

            if needs_retry {
                tracing::debug!(
                    query = canonical,
                    target_hits,
                    available_hits,
                    initial_fetch_limit,
                    fallback_fetch_limit,
                    "retrying semantic fetch due to candidate-window shortfall"
                );
                let (retry_results, _, retry_ann_stats) = self.search_semantic_candidates(
                    &candidate_context,
                    &embedding,
                    &filters,
                    SemanticCandidateSearchRequest {
                        fetch_limit: fallback_fetch_limit,
                        approximate: effective_approximate,
                        tier_mode: effective_tier_mode,
                        in_memory_two_tier_index: in_memory_two_tier_index.as_ref(),
                        ann_index: ann_index.as_ref(),
                    },
                )?;
                if !self.semantic_context_matches(&context_token)? {
                    tracing::debug!("semantic context changed during retry fetch; retrying");
                    continue;
                }
                (available_hits, paged_hits) = finalize_hits(&retry_results)?;
                ann_stats = retry_ann_stats;
            }

            tracing::trace!(
                query = canonical,
                target_hits,
                available_hits,
                returned = paged_hits.len(),
                "semantic fetch complete"
            );

            return Ok((paged_hits, ann_stats));
        }
    }

    pub(in crate::search::query) fn hydrate_semantic_hits(
        &self,
        results: &[VectorSearchResult],
        field_mask: FieldMask,
    ) -> Result<Vec<SearchHit>> {
        self.hydrate_semantic_hits_with_ids(results, field_mask)
            .map(|rows| rows.into_iter().map(|(_, hit)| hit).collect())
    }
}
