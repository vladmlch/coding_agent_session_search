use anyhow::{Context, Result, anyhow, bail};
use frankensearch::{
    Cx as FsCx, ScoreSource as FsScoreSource, ScoredResult as FsScoredResult,
    SearchError as FsSearchError, SearchFuture as FsSearchFuture, SearchPhase as FsSearchPhase,
    SyncEmbedderAdapter as FsSyncEmbedderAdapter, TwoTierIndex as FsTwoTierIndex,
    TwoTierIndexPaths as FsTwoTierIndexPaths, TwoTierSearcher as FsTwoTierSearcher,
    core::LexicalRead as FsLexicalRead,
};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::franken_sync::compat::{ParamValue, RowExt};
use crate::search::canonicalize::canonicalize_for_embedding;
use crate::search::canonicalize::content_hash;
use crate::search::embedder::Embedder;
use crate::search::vector_index::{
    ROLE_USER, SemanticDocId, SemanticIndexArtifact, SemanticProgressiveUnavailableReason,
    VectorSearchResult, parse_semantic_doc_id, role_code_from_str,
};

use super::SearchClient;
use super::fusion::{
    normalize_search_source_filter_value, normalized_search_hit_origin_kind,
    normalized_search_hit_source_id, normalized_search_hit_source_id_parts,
    normalized_search_source_id_sql_expr, snippet_from_content, stable_hit_hash,
};
use super::parsing::QuerySuggestion;
use super::semantic::{ProgressiveTwoTierContext, SharedCassSyncEmbedder};
use super::sqlite_hydration::{
    SearchReadTransaction, SearchSqliteConnectionExt, semantic_doc_component_id_from_db,
    semantic_message_id_from_db, sql_rowid_literal_list,
};
use super::types::{
    FieldMask, MatchType, ProgressivePhaseKind, ProgressiveSearchEvent, ProgressiveSearchRequest,
    SearchFilters, SearchHit, SearchMode, SearchResult, effective_field_mask,
    frankensearch_two_tier_config, progressive_phase_fetch_limit,
};

#[derive(Debug, Clone)]
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::search::query) struct ResolvedSemanticDocId {
    pub(in crate::search::query) message_id: u64,
    pub(in crate::search::query) doc_id: String,
}

/// Engine-agnostic input to the production SQLite message projector.
/// Semantic search and the CASS Layer-B seam both adapt into this shape.
#[derive(Debug, Clone, Copy)]
pub(in crate::search::query) struct RankedMessageHydrationCandidate {
    pub(in crate::search::query) message_id: u64,
    pub(in crate::search::query) score: f32,
}

#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::search::query) type ProgressiveLookupKey =
    (String, String, Option<i64>, String, i64, Option<i64>, u64);
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::search::query) type ProgressiveExactQueryKey = (i64, i64);
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::search::query) type ProgressiveFallbackQueryKey = (String, String, i64);
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::search::query) type ResolvedSemanticLookupRow =
    Option<(ProgressiveLookupKey, ResolvedSemanticDocId)>;

#[derive(Debug, Clone)]
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::search::query) struct ProgressiveLexicalHit {
    pub(in crate::search::query) title: String,
    pub(in crate::search::query) snippet: String,
    pub(in crate::search::query) content: String,
    pub(in crate::search::query) content_hash: u64,
    pub(in crate::search::query) conversation_id: Option<i64>,
    pub(in crate::search::query) source_path: String,
    pub(in crate::search::query) agent: String,
    pub(in crate::search::query) workspace: String,
    pub(in crate::search::query) workspace_original: Option<String>,
    pub(in crate::search::query) created_at: Option<i64>,
    pub(in crate::search::query) match_type: MatchType,
    pub(in crate::search::query) line_number: Option<usize>,
    pub(in crate::search::query) source_id: String,
    pub(in crate::search::query) origin_kind: String,
    pub(in crate::search::query) origin_host: Option<String>,
}

#[cfg_attr(not(test), allow(dead_code))]
impl ProgressiveLexicalHit {
    pub(in crate::search::query) fn from_search_hit(
        hit: &SearchHit,
        field_mask: FieldMask,
    ) -> Self {
        Self {
            title: if field_mask.wants_title() {
                hit.title.clone()
            } else {
                String::new()
            },
            snippet: if field_mask.wants_snippet() {
                hit.snippet.clone()
            } else {
                String::new()
            },
            content: if field_mask.needs_content() {
                hit.content.clone()
            } else {
                String::new()
            },
            content_hash: hit.content_hash,
            conversation_id: hit.conversation_id,
            source_path: hit.source_path.clone(),
            agent: hit.agent.clone(),
            workspace: hit.workspace.clone(),
            workspace_original: hit.workspace_original.clone(),
            created_at: hit.created_at,
            match_type: hit.match_type,
            line_number: hit.line_number,
            source_id: hit.source_id.clone(),
            origin_kind: hit.origin_kind.clone(),
            origin_host: hit.origin_host.clone(),
        }
    }

    pub(in crate::search::query) fn to_search_hit(&self, score: f32) -> SearchHit {
        SearchHit {
            title: self.title.clone(),
            snippet: self.snippet.clone(),
            content: self.content.clone(),
            content_hash: self.content_hash,
            conversation_id: self.conversation_id,
            score,
            source_path: self.source_path.clone(),
            agent: self.agent.clone(),
            workspace: self.workspace.clone(),
            workspace_original: self.workspace_original.clone(),
            created_at: self.created_at,
            line_number: self.line_number,
            match_type: self.match_type,
            source_id: self.source_id.clone(),
            origin_kind: self.origin_kind.clone(),
            origin_host: self.origin_host.clone(),
        }
    }
}

#[derive(Debug, Default)]
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::search::query) struct ProgressiveLexicalCache {
    pub(in crate::search::query) hits_by_message: HashMap<u64, ProgressiveLexicalHit>,
    pub(in crate::search::query) wildcard_fallback: bool,
    pub(in crate::search::query) suggestions: Vec<QuerySuggestion>,
}

#[derive(Clone, Copy)]
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::search::query) struct ProgressivePhaseContext<'a> {
    pub(in crate::search::query) query: &'a str,
    pub(in crate::search::query) filters: &'a SearchFilters,
    pub(in crate::search::query) field_mask: FieldMask,
    pub(in crate::search::query) lexical_cache: Option<&'a ProgressiveLexicalCache>,
    pub(in crate::search::query) limit: usize,
    pub(in crate::search::query) fetch_limit: usize,
}

#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::search::query) type ProgressiveLexicalSnapshot = Arc<ProgressiveLexicalCache>;

#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::search::query) struct CassProgressiveLexicalAdapter {
    pub(in crate::search::query) client: Arc<SearchClient>,
    pub(in crate::search::query) filters: SearchFilters,
    pub(in crate::search::query) field_mask: FieldMask,
    pub(in crate::search::query) sparse_threshold: usize,
    pub(in crate::search::query) shared: Arc<Mutex<ProgressiveLexicalSnapshot>>,
}

#[cfg_attr(not(test), allow(dead_code))]
impl CassProgressiveLexicalAdapter {
    pub(in crate::search::query) fn new(
        client: Arc<SearchClient>,
        filters: SearchFilters,
        field_mask: FieldMask,
        sparse_threshold: usize,
        shared: Arc<Mutex<ProgressiveLexicalSnapshot>>,
    ) -> Self {
        Self {
            client,
            filters,
            field_mask,
            sparse_threshold,
            shared,
        }
    }
}

impl FsLexicalRead for CassProgressiveLexicalAdapter {
    fn search<'a>(
        &'a self,
        cx: &'a FsCx,
        query: &'a str,
        limit: usize,
    ) -> FsSearchFuture<'a, Vec<FsScoredResult>> {
        Box::pin(async move {
            if cx.is_cancel_requested() {
                return Err(FsSearchError::Cancelled {
                    phase: "lexical".to_string(),
                    reason: "cancel requested".to_string(),
                });
            }

            let result = self
                .client
                .search_with_fallback(
                    query,
                    self.filters.clone(),
                    limit,
                    0,
                    self.sparse_threshold,
                    self.field_mask,
                )
                .map_err(|err| FsSearchError::SubsystemError {
                    subsystem: "cass_lexical_adapter",
                    source: Box::new(std::io::Error::other(err.to_string())),
                })?;

            let resolved = self
                .client
                .resolve_semantic_doc_ids_for_hits(&result.hits)
                .map_err(|err| FsSearchError::SubsystemError {
                    subsystem: "cass_lexical_adapter",
                    source: Box::new(std::io::Error::other(err.to_string())),
                })?;

            let mut scored = Vec::with_capacity(result.hits.len());
            let mut hits_by_message = HashMap::with_capacity(result.hits.len());

            for (hit, resolved_doc) in result.hits.iter().zip(resolved) {
                let Some(resolved_doc) = resolved_doc else {
                    continue;
                };
                hits_by_message
                    .entry(resolved_doc.message_id)
                    .or_insert_with(|| {
                        ProgressiveLexicalHit::from_search_hit(hit, self.field_mask)
                    });
                scored.push(FsScoredResult {
                    doc_id: resolved_doc.doc_id.into(),
                    score: hit.score,
                    source: FsScoreSource::Lexical,
                    index: None,
                    fast_score: None,
                    quality_score: None,
                    lexical_score: Some(hit.score),
                    rerank_score: None,
                    explanation: None,
                    metadata: None,
                });
            }

            if let Ok(mut guard) = self.shared.lock() {
                *guard = Arc::new(ProgressiveLexicalCache {
                    hits_by_message,
                    wildcard_fallback: result.wildcard_fallback,
                    suggestions: result.suggestions,
                });
            }

            Ok(scored)
        })
    }

    fn doc_count(&self) -> Result<usize, FsSearchError> {
        Ok(self.client.total_docs())
    }
}

impl SearchClient {
    pub fn can_progressively_refine(&self) -> bool {
        self.progressive_context()
            .map(|context| {
                context.as_ref().is_some_and(|ctx| {
                    ctx.quality_embedder.is_some() && ctx.index.has_quality_index()
                })
            })
            .unwrap_or(false)
    }

    /// Resolve and return the stable reason progressive serving is unavailable.
    ///
    /// `None` means an exact fast+quality pair is ready. The method performs
    /// the same lazy resolution used by search so diagnostics and execution
    /// cannot disagree about artifact topology.
    pub fn progressive_unavailability_reason(
        &self,
    ) -> Result<Option<SemanticProgressiveUnavailableReason>> {
        let resolution = self.progressive_context();
        let reason = self
            .semantic
            .lock()
            .map_err(|_| anyhow!("semantic lock poisoned"))?
            .as_ref()
            .and_then(|state| state.progressive_context_unavailable);
        match (resolution, reason) {
            (_, Some(reason)) => Ok(Some(reason)),
            (Ok(_), None) => Ok(None),
            (Err(error), None) => Err(error),
        }
    }

    pub(in crate::search::query) fn progressive_context(
        &self,
    ) -> Result<Option<Arc<ProgressiveTwoTierContext>>> {
        loop {
            let (fast_artifact, quality_artifact, embedder, context_token) = {
                let mut guard = self
                    .semantic
                    .lock()
                    .map_err(|_| anyhow!("semantic lock poisoned"))?;
                let state = guard.as_mut().ok_or_else(|| {
                    anyhow!("semantic search unavailable (no embedder or vector index)")
                })?;
                let owner_backed = state
                    .artifacts
                    .iter()
                    .all(SemanticIndexArtifact::has_owner_backed_progressive_reader)
                    && state
                        .quality_artifact
                        .as_ref()
                        .is_some_and(SemanticIndexArtifact::has_owner_backed_progressive_reader);
                if !owner_backed {
                    let reason = SemanticProgressiveUnavailableReason::OwnerBackedReaderRequired;
                    tracing::debug!(reason = reason.code(), "progressive semantic unavailable");
                    state.progressive_context_unavailable = Some(reason);
                    return Ok(None);
                }
                if let Some(context) = state.progressive_context.as_ref() {
                    return Ok(Some(Arc::clone(context)));
                }
                if let Some(reason) = state.progressive_context_unavailable {
                    tracing::debug!(
                        reason = reason.code(),
                        "progressive semantic remains unavailable"
                    );
                    return Ok(None);
                }
                if state.artifacts.len() != 1 {
                    let reason = SemanticProgressiveUnavailableReason::MultipleExactShards;
                    tracing::debug!(
                        reason = reason.code(),
                        shard_count = state.artifacts.len(),
                        "progressive semantic unavailable"
                    );
                    state.progressive_context_unavailable = Some(reason);
                    return Ok(None);
                }
                let Some(quality_artifact) = state.quality_artifact.clone() else {
                    let reason = SemanticProgressiveUnavailableReason::QualityArtifactMissing;
                    tracing::debug!(reason = reason.code(), "progressive semantic unavailable");
                    state.progressive_context_unavailable = Some(reason);
                    return Ok(None);
                };
                (
                    state.artifacts[0].clone(),
                    quality_artifact,
                    Arc::clone(&state.embedder),
                    Arc::clone(&state.context_token),
                )
            };

            let pair_reason = match same_file::is_same_file(
                fast_artifact.fsvi_path(),
                quality_artifact.fsvi_path(),
            ) {
                Ok(true) => Some(SemanticProgressiveUnavailableReason::ExactArtifactRoleAlias),
                Ok(false) => None,
                Err(err) => {
                    tracing::debug!(
                        fast_path = %fast_artifact.fsvi_path().display(),
                        quality_path = %quality_artifact.fsvi_path().display(),
                        error = %err,
                        "compare progressive semantic artifact roles failed"
                    );
                    Some(SemanticProgressiveUnavailableReason::ExactArtifactOpenFailed)
                }
            };
            if let Some(reason) = pair_reason {
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
                state.progressive_context_unavailable = Some(reason);
                bail!(
                    "progressive semantic artifact contract rejected: {}",
                    reason.code()
                );
            }

            let context = match self.build_progressive_context(
                &fast_artifact,
                &quality_artifact,
                embedder,
                Arc::clone(&context_token),
            ) {
                Ok(context) => context,
                Err(err) => {
                    let mut guard = self
                        .semantic
                        .lock()
                        .map_err(|_| anyhow!("semantic lock poisoned"))?;
                    let state = guard.as_mut().ok_or_else(|| {
                        anyhow!("semantic search unavailable (no embedder or vector index)")
                    })?;
                    if let Some(existing) = state.progressive_context.as_ref() {
                        return Ok(Some(Arc::clone(existing)));
                    }
                    if !Arc::ptr_eq(&state.context_token, &context_token) {
                        continue;
                    }
                    state.progressive_context_unavailable =
                        Some(SemanticProgressiveUnavailableReason::ExactArtifactOpenFailed);
                    return Err(err);
                }
            };

            let mut guard = self
                .semantic
                .lock()
                .map_err(|_| anyhow!("semantic lock poisoned"))?;
            let state = guard.as_mut().ok_or_else(|| {
                anyhow!("semantic search unavailable (no embedder or vector index)")
            })?;
            if let Some(existing) = state.progressive_context.as_ref() {
                return Ok(Some(Arc::clone(existing)));
            }
            if !Arc::ptr_eq(&state.context_token, &context_token) {
                continue;
            }
            state.progressive_context_unavailable = None;
            state.progressive_context = Some(Arc::clone(&context));
            return Ok(Some(context));
        }
    }

    pub(in crate::search::query) fn build_progressive_context(
        &self,
        fast_artifact: &SemanticIndexArtifact,
        quality_artifact: &SemanticIndexArtifact,
        embedder: Arc<dyn Embedder>,
        context_token: Arc<()>,
    ) -> Result<Arc<ProgressiveTwoTierContext>> {
        let fast_index = fast_artifact.index();
        let quality_index = quality_artifact.index();
        let paths = FsTwoTierIndexPaths::new(fast_artifact.fsvi_path())
            .with_quality_index(quality_artifact.fsvi_path());
        let index = Arc::new(
            FsTwoTierIndex::open_with_paths(&paths, frankensearch_two_tier_config())
                .map_err(|err| anyhow!("open progressive two-tier index failed: {err}"))?,
        );

        let fast_embedder = self.load_embedder_for_progressive_id(
            &embedder,
            fast_index.embedder_id(),
            fast_index.dimension(),
        )?;
        let fast_embedder: Arc<dyn frankensearch::Embedder> = Arc::new(FsSyncEmbedderAdapter(
            SharedCassSyncEmbedder::new(fast_embedder),
        ));
        let quality_embedder = Some(self.load_embedder_for_progressive_id(
            &embedder,
            quality_index.embedder_id(),
            quality_index.dimension(),
        )?);
        let quality_embedder = quality_embedder.map(|embedder| {
            Arc::new(FsSyncEmbedderAdapter(SharedCassSyncEmbedder::new(embedder)))
                as Arc<dyn frankensearch::Embedder>
        });

        Ok(Arc::new(ProgressiveTwoTierContext {
            context_token,
            index,
            fast_embedder,
            quality_embedder,
        }))
    }

    pub(in crate::search::query) fn load_embedder_for_progressive_id(
        &self,
        current_embedder: &Arc<dyn Embedder>,
        embedder_id: &str,
        dimension: usize,
    ) -> Result<Arc<dyn Embedder>> {
        if current_embedder.id() == embedder_id {
            return Ok(Arc::clone(current_embedder));
        }

        if let Some(dim) = embedder_id.strip_prefix("fnv1a-")
            && let Ok(parsed) = dim.parse::<usize>()
        {
            if parsed != dimension {
                bail!(
                    "progressive hash embedder dimension mismatch: identity {} declares {}, index has {}",
                    embedder_id,
                    parsed,
                    dimension
                );
            }
            return Ok(Arc::new(crate::search::hash_embedder::HashEmbedder::new(
                parsed,
            )));
        }

        if let Some(embedder_name) =
            crate::search::fastembed_embedder::FastEmbedder::canonical_name(embedder_id)
        {
            let data_dir = self
                .sqlite_path
                .as_ref()
                .and_then(|path| path.parent())
                .ok_or_else(|| anyhow!("cannot resolve data dir for progressive embedder load"))?;
            let embedder = crate::search::fastembed_embedder::FastEmbedder::load_by_name(
                data_dir,
                embedder_name,
            )
            .with_context(|| format!("loading FastEmbed model for {embedder_name}"))?;
            if embedder.dimension() != dimension {
                bail!(
                    "progressive embedder dimension mismatch: {} index expects {}, model has {}",
                    embedder_id,
                    dimension,
                    embedder.dimension()
                );
            }
            return Ok(Arc::new(embedder));
        }

        bail!("unsupported progressive embedder id: {embedder_id}");
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::search::query) fn resolve_semantic_doc_ids_for_hits(
        &self,
        hits: &[SearchHit],
    ) -> Result<Vec<Option<ResolvedSemanticDocId>>> {
        if hits.is_empty() {
            return Ok(Vec::new());
        }

        let lookup_keys: Vec<Option<ProgressiveLookupKey>> = hits
            .iter()
            .map(|hit| {
                let idx = hit
                    .line_number
                    .and_then(|line| line.checked_sub(1))
                    .map(i64::try_from)
                    .transpose()
                    .ok()
                    .flatten()?;
                Some((
                    normalized_search_hit_source_id(hit),
                    hit.source_path.clone(),
                    hit.conversation_id,
                    hit.title.trim().to_string(),
                    idx,
                    hit.created_at,
                    hit.content_hash,
                ))
            })
            .collect();

        let mut seen_exact = HashSet::new();
        let mut exact_query_keys = Vec::new();
        let mut seen_fallback = HashSet::new();
        let mut fallback_query_keys = Vec::new();
        for (source_id, source_path, conversation_id, _title, idx, _created_at, _content_hash) in
            lookup_keys.iter().flatten()
        {
            if let Some(conversation_id) = conversation_id {
                let query_key: ProgressiveExactQueryKey = (*conversation_id, *idx);
                if seen_exact.insert(query_key) {
                    exact_query_keys.push(query_key);
                }
            } else {
                let query_key: ProgressiveFallbackQueryKey =
                    (source_id.clone(), source_path.clone(), *idx);
                if seen_fallback.insert(query_key.clone()) {
                    fallback_query_keys.push(query_key);
                }
            }
        }

        if exact_query_keys.is_empty() && fallback_query_keys.is_empty() {
            return Ok(vec![None; hits.len()]);
        }

        let sqlite_guard = self.sqlite_guard()?;
        let conn = sqlite_guard
            .as_ref()
            .ok_or_else(|| anyhow!("progressive search requires database connection"))?;

        let mut resolved_by_key = HashMap::new();
        let normalized_source_sql =
            normalized_search_source_id_sql_expr("c.source_id", "s.kind", "c.origin_host");

        const CHUNK_SIZE: usize = 300;
        for chunk in exact_query_keys.chunks(CHUNK_SIZE) {
            let mut sql = String::from("SELECT c.id, ");
            sql.push_str(&normalized_source_sql);
            sql.push_str(
                ", c.source_path, m.idx, m.id, c.agent_id, c.workspace_id, m.role, m.created_at, m.content, c.title
                 FROM messages m
                 JOIN conversations c ON m.conversation_id = c.id
                 LEFT JOIN sources s ON c.source_id = s.id
                 WHERE ",
            );
            let mut params = Vec::with_capacity(chunk.len().saturating_mul(2));
            for (idx, (conversation_id, line_idx)) in chunk.iter().enumerate() {
                if idx > 0 {
                    sql.push_str(" OR ");
                }
                sql.push_str("(c.id = ? AND m.idx = ?)");
                params.push(ParamValue::from(*conversation_id));
                params.push(ParamValue::from(*line_idx));
            }

            let chunk_rows: Vec<ResolvedSemanticLookupRow> =
                conn.query_map_collect(&sql, &params, |row: &crate::franken_sync::Row| {
                    let conversation_id: i64 = row.get_typed(0)?;
                    let source_id: String = row.get_typed(1)?;
                    let source_path: String = row.get_typed(2)?;
                    let idx: i64 = row.get_typed(3)?;
                    let message_id_raw: i64 = row.get_typed(4)?;
                    // agent_id is nullable for legacy V1 conversations; treat
                    // NULL the same as the negative-sentinel branch below (0).
                    let agent_id_raw: Option<i64> = row.get_typed(5)?;
                    let workspace_id_raw: Option<i64> = row.get_typed(6)?;
                    let role_raw: String = row.get_typed(7)?;
                    let created_at_ms: Option<i64> = row.get_typed(8)?;
                    let content: String = row.get_typed(9)?;
                    let title: Option<String> = row.get_typed(10)?;

                    let canonical = canonicalize_for_embedding(&content);
                    if canonical.is_empty() {
                        return Ok(None);
                    }

                    let message_id = u64::try_from(message_id_raw).map_err(|_| {
                        std::io::Error::other("message id out of range for progressive doc_id")
                    })?;
                    let agent_id = semantic_doc_component_id_from_db(agent_id_raw);
                    let workspace_id = semantic_doc_component_id_from_db(workspace_id_raw);
                    let role = role_code_from_str(&role_raw).unwrap_or(ROLE_USER);
                    let doc_id = SemanticDocId {
                        message_id,
                        chunk_idx: 0,
                        agent_id,
                        workspace_id,
                        source_id: crc32fast::hash(source_id.as_bytes()),
                        role,
                        created_at_ms: created_at_ms.unwrap_or(0),
                        content_hash: Some(content_hash(&canonical)),
                    }
                    .to_doc_id_string();
                    let line_number = usize::try_from(idx).ok().map(|line| line.saturating_add(1));
                    let lookup_key = (
                        source_id,
                        source_path.clone(),
                        Some(conversation_id),
                        title.unwrap_or_default().trim().to_string(),
                        idx,
                        created_at_ms,
                        stable_hit_hash(&content, &source_path, line_number, created_at_ms),
                    );

                    Ok(Some((
                        lookup_key,
                        ResolvedSemanticDocId { message_id, doc_id },
                    )))
                })?;

            for row in chunk_rows.into_iter().flatten() {
                resolved_by_key.insert(row.0, row.1);
            }
        }

        for chunk in fallback_query_keys.chunks(CHUNK_SIZE) {
            let mut sql = String::from("SELECT ");
            sql.push_str(&normalized_source_sql);
            sql.push_str(
                ", c.source_path, m.idx, m.id, c.agent_id, c.workspace_id, m.role, m.created_at, m.content, c.title
                 FROM messages m
                 JOIN conversations c ON m.conversation_id = c.id
                 LEFT JOIN sources s ON c.source_id = s.id
                 WHERE ",
            );
            let mut params = Vec::with_capacity(chunk.len().saturating_mul(3));
            for (idx, (source_id, source_path, line_idx)) in chunk.iter().enumerate() {
                if idx > 0 {
                    sql.push_str(" OR ");
                }
                sql.push_str(&format!(
                    "({normalized_source_sql} = ? AND c.source_path = ? AND m.idx = ?)"
                ));
                params.push(ParamValue::from(normalize_search_source_filter_value(
                    source_id,
                )));
                params.push(ParamValue::from(source_path.clone()));
                params.push(ParamValue::from(*line_idx));
            }

            let chunk_rows: Vec<ResolvedSemanticLookupRow> =
                conn.query_map_collect(&sql, &params, |row: &crate::franken_sync::Row| {
                    let source_id: String = row.get_typed(0)?;
                    let source_path: String = row.get_typed(1)?;
                    let idx: i64 = row.get_typed(2)?;
                    let message_id_raw: i64 = row.get_typed(3)?;
                    // agent_id is nullable for legacy V1 conversations; treat
                    // NULL the same as the negative-sentinel branch below (0).
                    let agent_id_raw: Option<i64> = row.get_typed(4)?;
                    let workspace_id_raw: Option<i64> = row.get_typed(5)?;
                    let role_raw: String = row.get_typed(6)?;
                    let created_at_ms: Option<i64> = row.get_typed(7)?;
                    let content: String = row.get_typed(8)?;
                    let title: Option<String> = row.get_typed(9)?;

                    let canonical = canonicalize_for_embedding(&content);
                    if canonical.is_empty() {
                        return Ok(None);
                    }

                    let message_id = u64::try_from(message_id_raw).map_err(|_| {
                        std::io::Error::other("message id out of range for progressive doc_id")
                    })?;
                    let agent_id = semantic_doc_component_id_from_db(agent_id_raw);
                    let workspace_id = semantic_doc_component_id_from_db(workspace_id_raw);
                    let role = role_code_from_str(&role_raw).unwrap_or(ROLE_USER);
                    let doc_id = SemanticDocId {
                        message_id,
                        chunk_idx: 0,
                        agent_id,
                        workspace_id,
                        source_id: crc32fast::hash(source_id.as_bytes()),
                        role,
                        created_at_ms: created_at_ms.unwrap_or(0),
                        content_hash: Some(content_hash(&canonical)),
                    }
                    .to_doc_id_string();
                    let line_number = usize::try_from(idx).ok().map(|line| line.saturating_add(1));
                    let lookup_key = (
                        source_id,
                        source_path.clone(),
                        None,
                        title.unwrap_or_default().trim().to_string(),
                        idx,
                        created_at_ms,
                        stable_hit_hash(&content, &source_path, line_number, created_at_ms),
                    );

                    Ok(Some((
                        lookup_key,
                        ResolvedSemanticDocId { message_id, doc_id },
                    )))
                })?;

            for row in chunk_rows.into_iter().flatten() {
                resolved_by_key.insert(row.0, row.1);
            }
        }

        Ok(lookup_keys
            .into_iter()
            .map(|key| key.and_then(|lookup| resolved_by_key.get(&lookup).cloned()))
            .collect())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::search::query) fn load_message_text_by_id(
        &self,
        message_id: u64,
    ) -> Result<Option<String>> {
        let sqlite_guard = self.sqlite_guard()?;
        let conn = sqlite_guard
            .as_ref()
            .ok_or_else(|| anyhow!("progressive search requires database connection"))?;
        let rows: Vec<String> = conn.query_map_collect(
            "SELECT content FROM messages WHERE id = ?",
            &[ParamValue::from(i64::try_from(message_id)?)],
            |row: &crate::franken_sync::Row| row.get_typed(0),
        )?;
        Ok(rows.into_iter().next())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::search::query) fn collapse_progressive_scored_results(
        &self,
        results: &[FsScoredResult],
        fetch_limit: usize,
    ) -> Vec<VectorSearchResult> {
        let fetch = fetch_limit.max(1);
        let mut best_by_message: HashMap<u64, VectorSearchResult> =
            HashMap::with_capacity(results.len());
        for hit in results {
            let Some(parsed) = parse_semantic_doc_id(&hit.doc_id) else {
                continue;
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
        let mut collapsed: Vec<VectorSearchResult> = best_by_message.into_values().collect();
        collapsed.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.message_id.cmp(&b.message_id))
        });
        if collapsed.len() > fetch {
            collapsed.truncate(fetch);
        }
        collapsed
    }

    pub(in crate::search::query) fn hydrate_ranked_message_hits_in_transaction(
        &self,
        transaction: &SearchReadTransaction<'_>,
        results: &[RankedMessageHydrationCandidate],
        field_mask: FieldMask,
        match_type: MatchType,
    ) -> Result<Vec<(u64, SearchHit)>> {
        if results.is_empty() {
            return Ok(Vec::new());
        }

        #[derive(Debug)]
        struct MessageHydrationRow {
            message_id: u64,
            conversation_id: i64,
            full_content: String,
            msg_created_at: Option<i64>,
            idx: Option<i64>,
        }

        #[derive(Debug)]
        struct ConversationHydrationRow {
            title: Option<String>,
            source_path: String,
            source_id: String,
            origin_host: Option<String>,
            agent: String,
            workspace: Option<String>,
            origin_kind: Option<String>,
            started_at: Option<i64>,
        }

        let mut unique_message_ids = Vec::with_capacity(results.len());
        let mut seen_message_ids = HashSet::with_capacity(results.len());
        for result in results {
            if seen_message_ids.insert(result.message_id) {
                unique_message_ids.push(result.message_id);
            }
        }

        // Retain the literal rowid path: see `sql_rowid_literal_list`
        // (avoids parameterized IN-list scans on older query plans).
        let message_rowids = unique_message_ids
            .iter()
            .map(|message_id| i64::try_from(*message_id))
            .collect::<Result<Vec<i64>, _>>()?;
        let message_sql = format!(
            "SELECT id, conversation_id, content, created_at, idx
             FROM messages
             WHERE id IN ({})",
            sql_rowid_literal_list(&message_rowids)
        );

        let message_rows: Vec<MessageHydrationRow> = transaction.query_map_collect(
            &message_sql,
            &[],
            |row: &crate::franken_sync::Row| {
                let message_id: i64 = row.get_typed(0)?;
                Ok(MessageHydrationRow {
                    message_id: semantic_message_id_from_db(message_id)?,
                    conversation_id: row.get_typed(1)?,
                    full_content: row.get_typed(2)?,
                    msg_created_at: row.get_typed(3)?,
                    idx: row.get_typed(4)?,
                })
            },
        )?;
        if message_rows.is_empty() {
            return Ok(Vec::new());
        }

        let title_expr = if field_mask.wants_title() {
            "c.title"
        } else {
            "''"
        };
        let normalized_source_sql =
            normalized_search_source_id_sql_expr("c.source_id", "s.kind", "c.origin_host");
        let mut conversation_ids = Vec::with_capacity(message_rows.len());
        let mut seen_conversation_ids = HashSet::with_capacity(message_rows.len());
        for row in &message_rows {
            if seen_conversation_ids.insert(row.conversation_id) {
                conversation_ids.push(row.conversation_id);
            }
        }
        // Literal rowid list for the same reason as the message step above.
        let conversation_rowids = sql_rowid_literal_list(&conversation_ids);
        // LEFT JOIN + COALESCE on agents so search hits for conversations
        // with NULL agent_id (legacy V1 schema) still surface instead of
        // being silently dropped from results.  Consistent with the fts/
        // lexical rebuild paths (8a0c547c, e1c08e7c).
        let sql = format!(
            "SELECT c.id, {title_expr}, c.source_path, {normalized_source_sql}, c.origin_host, COALESCE(a.slug, 'unknown'), w.path, s.kind, c.started_at
             FROM conversations c
             LEFT JOIN agents a ON c.agent_id = a.id
             LEFT JOIN workspaces w ON c.workspace_id = w.id
             LEFT JOIN sources s ON c.source_id = s.id
             WHERE c.id IN ({conversation_rowids})"
        );

        let conversation_rows: Vec<(i64, ConversationHydrationRow)> = transaction
            .query_map_collect(&sql, &[], |row: &crate::franken_sync::Row| {
                let conversation_id: i64 = row.get_typed(0)?;
                let title: Option<String> = if field_mask.wants_title() {
                    row.get_typed(1)?
                } else {
                    None
                };
                Ok((
                    conversation_id,
                    ConversationHydrationRow {
                        title,
                        source_path: row.get_typed(2)?,
                        source_id: row.get_typed(3)?,
                        origin_host: row.get_typed(4)?,
                        agent: row.get_typed(5)?,
                        workspace: row.get_typed(6)?,
                        origin_kind: row.get_typed(7)?,
                        started_at: row.get_typed(8)?,
                    },
                ))
            })?;

        let conversations_by_id: HashMap<i64, ConversationHydrationRow> =
            conversation_rows.into_iter().collect();

        let rows: Vec<(u64, SearchHit)> = message_rows
            .into_iter()
            .filter_map(|message| {
                let conversation = conversations_by_id.get(&message.conversation_id)?;

                let created_at = message.msg_created_at.or(conversation.started_at);
                let line_number = message
                    .idx
                    .and_then(|i| usize::try_from(i).ok())
                    .map(|i| i.saturating_add(1));
                let snippet = if field_mask.wants_snippet() {
                    snippet_from_content(&message.full_content)
                } else {
                    String::new()
                };
                let content = if field_mask.needs_content() {
                    message.full_content.clone()
                } else {
                    String::new()
                };
                let content_hash = stable_hit_hash(
                    &message.full_content,
                    &conversation.source_path,
                    line_number,
                    created_at,
                );
                let source_id = normalized_search_hit_source_id_parts(
                    conversation.source_id.as_str(),
                    conversation.origin_kind.as_deref().unwrap_or_default(),
                    conversation.origin_host.as_deref(),
                );
                let origin_kind = normalized_search_hit_origin_kind(
                    &source_id,
                    conversation.origin_kind.as_deref(),
                );

                let hit = SearchHit {
                    title: if field_mask.wants_title() {
                        conversation.title.clone().unwrap_or_default()
                    } else {
                        String::new()
                    },
                    snippet,
                    content,
                    content_hash,
                    conversation_id: Some(message.conversation_id),
                    score: 0.0,
                    source_path: conversation.source_path.clone(),
                    agent: conversation.agent.clone(),
                    workspace: conversation.workspace.clone().unwrap_or_default(),
                    workspace_original: None,
                    created_at,
                    line_number,
                    match_type,
                    source_id,
                    origin_kind,
                    origin_host: conversation.origin_host.clone(),
                };

                Some((message.message_id, hit))
            })
            .collect();

        let mut hits_by_id = HashMap::new();
        for (id, hit) in rows {
            hits_by_id.insert(id, hit);
        }

        let mut ordered = Vec::new();
        for result in results {
            if let Some(mut hit) = hits_by_id.remove(&result.message_id) {
                hit.score = result.score;
                ordered.push((result.message_id, hit));
            }
        }

        Ok(ordered)
    }

    pub(in crate::search::query) fn hydrate_semantic_hits_with_ids(
        &self,
        results: &[VectorSearchResult],
        field_mask: FieldMask,
    ) -> Result<Vec<(u64, SearchHit)>> {
        if results.is_empty() {
            return Ok(Vec::new());
        }
        let candidates = results
            .iter()
            .map(|result| RankedMessageHydrationCandidate {
                message_id: result.message_id,
                score: result.score,
            })
            .collect::<Vec<_>>();
        let sqlite_guard = self.sqlite_guard()?;
        let conn = sqlite_guard
            .as_ref()
            .ok_or_else(|| anyhow!("semantic search requires database connection"))?;
        let mut transaction = SearchReadTransaction::begin(conn)?;
        let hits = self.hydrate_ranked_message_hits_in_transaction(
            &transaction,
            &candidates,
            field_mask,
            MatchType::Exact,
        )?;
        transaction.rollback()?;
        Ok(hits)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::search::query) fn overlay_progressive_lexical_hit(
        &self,
        hit: &mut SearchHit,
        lexical: &ProgressiveLexicalHit,
        field_mask: FieldMask,
    ) {
        if field_mask.wants_title() && !lexical.title.is_empty() {
            hit.title = lexical.title.clone();
        }
        if field_mask.wants_snippet() && !lexical.snippet.is_empty() {
            hit.snippet = lexical.snippet.clone();
        }
        if field_mask.needs_content() && !lexical.content.is_empty() {
            hit.content = lexical.content.clone();
        }
        hit.match_type = lexical.match_type;
        hit.line_number = lexical.line_number.or(hit.line_number);
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::search::query) fn progressive_phase_to_result(
        &self,
        results: &[FsScoredResult],
        ctx: ProgressivePhaseContext<'_>,
    ) -> Result<SearchResult> {
        let collapsed = self.collapse_progressive_scored_results(results, ctx.fetch_limit);
        let missing: Vec<VectorSearchResult> = collapsed
            .iter()
            .filter(|result| {
                ctx.lexical_cache
                    .and_then(|cache| cache.hits_by_message.get(&result.message_id))
                    .is_none()
            })
            .map(|result| VectorSearchResult {
                message_id: result.message_id,
                chunk_idx: result.chunk_idx,
                score: result.score,
            })
            .collect();
        let mut hydrated_by_id: HashMap<u64, SearchHit> = self
            .hydrate_semantic_hits_with_ids(&missing, ctx.field_mask)?
            .into_iter()
            .collect();

        let mut hydrated: Vec<(u64, SearchHit)> = Vec::with_capacity(collapsed.len());
        for result in &collapsed {
            if let Some(cache) = ctx.lexical_cache
                && let Some(lexical) = cache.hits_by_message.get(&result.message_id)
            {
                hydrated.push((result.message_id, lexical.to_search_hit(result.score)));
                continue;
            }
            if let Some(mut hit) = hydrated_by_id.remove(&result.message_id) {
                if let Some(cache) = ctx.lexical_cache
                    && let Some(lexical) = cache.hits_by_message.get(&result.message_id)
                {
                    self.overlay_progressive_lexical_hit(&mut hit, lexical, ctx.field_mask);
                }
                hydrated.push((result.message_id, hit));
            }
        }

        let mut hits: Vec<SearchHit> = hydrated.into_iter().map(|(_, hit)| hit).collect();
        (_, hits) = self.postprocess_hits_page(hits, ctx.query, ctx.filters, ctx.limit, 0);

        let (wildcard_fallback, suggestions) = ctx
            .lexical_cache
            .map(|cache| {
                let suggestions = if hits.is_empty() {
                    cache.suggestions.clone()
                } else {
                    Vec::new()
                };
                (cache.wildcard_fallback, suggestions)
            })
            .unwrap_or((false, Vec::new()));

        Ok(SearchResult {
            hits,
            wildcard_fallback,
            cache_stats: self.cache_stats(),
            suggestions,
            ann_stats: None,
            ann_unavailable_reason: None,
            total_count: None,
        })
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn search_progressive_with_callback(
        self: &Arc<Self>,
        request: ProgressiveSearchRequest<'_>,
        mut on_event: impl FnMut(ProgressiveSearchEvent) + Send,
    ) -> Result<()> {
        let ProgressiveSearchRequest {
            cx,
            query,
            filters,
            limit,
            sparse_threshold,
            field_mask,
            mode,
        } = request;
        let field_mask = effective_field_mask(field_mask);
        let limit = limit.max(1);
        let fetch_limit = progressive_phase_fetch_limit(limit);

        match mode {
            SearchMode::Lexical => {
                let started = Instant::now();
                let result = self.search_with_fallback(
                    query,
                    filters,
                    limit,
                    0,
                    sparse_threshold,
                    field_mask,
                )?;
                on_event(ProgressiveSearchEvent::Phase {
                    kind: ProgressivePhaseKind::Initial,
                    elapsed_ms: started.elapsed().as_millis(),
                    result,
                });
                return Ok(());
            }
            SearchMode::Semantic | SearchMode::Hybrid => {}
        }

        let progressive_context = {
            self.progressive_context()?
                .ok_or_else(|| anyhow!("progressive two-tier context unavailable"))?
        };
        let progressive_context_token = Arc::clone(&progressive_context.context_token);

        let lexical_cache: Arc<Mutex<ProgressiveLexicalSnapshot>> =
            Arc::new(Mutex::new(Arc::new(ProgressiveLexicalCache::default())));
        let text_cache: Arc<Mutex<HashMap<u64, String>>> = Arc::new(Mutex::new(HashMap::new()));
        let text_client = Arc::clone(self);
        let text_cache_for_lookup = Arc::clone(&text_cache);
        let text_fn = move |doc_id: &str| -> Option<String> {
            let parsed = parse_semantic_doc_id(doc_id)?;
            if let Ok(cache) = text_cache_for_lookup.lock()
                && let Some(text) = cache.get(&parsed.message_id)
            {
                return Some(text.clone());
            }
            let loaded = text_client
                .load_message_text_by_id(parsed.message_id)
                .ok()
                .flatten()?;
            if let Ok(mut cache) = text_cache_for_lookup.lock() {
                cache.insert(parsed.message_id, loaded.clone());
            }
            Some(loaded)
        };

        let mut searcher = FsTwoTierSearcher::new(
            Arc::clone(&progressive_context.index),
            Arc::clone(&progressive_context.fast_embedder),
            frankensearch_two_tier_config(),
        );

        if let Some(quality_embedder) = progressive_context.quality_embedder.as_ref() {
            searcher = searcher.with_quality_embedder(Arc::clone(quality_embedder));
        }

        if matches!(mode, SearchMode::Hybrid) {
            let lexical = Arc::new(CassProgressiveLexicalAdapter::new(
                Arc::clone(self),
                filters.clone(),
                field_mask,
                sparse_threshold,
                Arc::clone(&lexical_cache),
            ));
            searcher = searcher.with_lexical(lexical);
        }

        let phase_client = Arc::clone(self);
        let phase_filters = filters.clone();
        let phase_cache = Arc::clone(&lexical_cache);
        let mut phase_error: Option<anyhow::Error> = None;

        let search_result = searcher
            .search(cx, query, fetch_limit, text_fn, |phase| {
                if phase_error.is_some() {
                    return;
                }
                match phase_client.semantic_context_matches(&progressive_context_token) {
                    Ok(true) => {}
                    Ok(false) => {
                        phase_error = Some(anyhow!(
                            "progressive search aborted: semantic context changed"
                        ));
                        cx.set_cancel_requested(true);
                        return;
                    }
                    Err(err) => {
                        phase_error = Some(err);
                        cx.set_cancel_requested(true);
                        return;
                    }
                }
                let lexical_snapshot = phase_cache.lock().ok().map(|guard| Arc::clone(&guard));
                let event_result = match phase {
                    FsSearchPhase::Initial {
                        results, latency, ..
                    } => phase_client
                        .progressive_phase_to_result(
                            &results,
                            ProgressivePhaseContext {
                                query,
                                filters: &phase_filters,
                                field_mask,
                                lexical_cache: lexical_snapshot.as_deref(),
                                limit,
                                fetch_limit,
                            },
                        )
                        .map(|result| ProgressiveSearchEvent::Phase {
                            kind: ProgressivePhaseKind::Initial,
                            elapsed_ms: latency.as_millis(),
                            result,
                        }),
                    FsSearchPhase::Refined {
                        results, latency, ..
                    } => phase_client
                        .progressive_phase_to_result(
                            &results,
                            ProgressivePhaseContext {
                                query,
                                filters: &phase_filters,
                                field_mask,
                                lexical_cache: lexical_snapshot.as_deref(),
                                limit,
                                fetch_limit,
                            },
                        )
                        .map(|result| ProgressiveSearchEvent::Phase {
                            kind: ProgressivePhaseKind::Refined,
                            elapsed_ms: latency.as_millis(),
                            result,
                        }),
                    // frankensearch may emit a final reranked phase after the
                    // quality-refined pass. cass's progressive consumers only
                    // distinguish fast initial results from a better upgraded
                    // replacement set, so reranked results flow through the
                    // existing refined/upgrade path.
                    FsSearchPhase::Reranked {
                        results, latency, ..
                    } => phase_client
                        .progressive_phase_to_result(
                            &results,
                            ProgressivePhaseContext {
                                query,
                                filters: &phase_filters,
                                field_mask,
                                lexical_cache: lexical_snapshot.as_deref(),
                                limit,
                                fetch_limit,
                            },
                        )
                        .map(|result| ProgressiveSearchEvent::Phase {
                            kind: ProgressivePhaseKind::Refined,
                            elapsed_ms: latency.as_millis(),
                            result,
                        }),
                    FsSearchPhase::RefinementFailed { error, latency, .. } => {
                        Ok(ProgressiveSearchEvent::RefinementFailed {
                            latency_ms: latency.as_millis(),
                            error: error.to_string(),
                        })
                    }
                };

                match event_result {
                    Ok(event) => on_event(event),
                    Err(err) => {
                        phase_error = Some(err);
                        cx.set_cancel_requested(true);
                    }
                }
            })
            .await;

        if let Some(err) = phase_error {
            return Err(err);
        }

        search_result
            .map(|_| ())
            .map_err(|err| anyhow!("progressive search failed: {err}"))
    }
}
