use anyhow::Result;
#[cfg(any(test, feature = "cass-layer-b-conformance"))]
use anyhow::anyhow;
use frankensearch::quill::query::{
    CassQueryFilters as FsCassQueryFilters, CassSourceFilter as FsCassSourceFilter,
};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use crate::franken_sync::compat::ParamValue;
use crate::franken_sync::compat::RowExt;
use crate::sources::provenance::SourceFilter;

use super::SearchClient;
use super::cache::{
    FEDERATED_SEARCH_READERS, FederatedIndexReader, cached_prefix_snippet, is_prefix_only,
    quick_prefix_snippet, reload_index_readers_bounded,
};
use super::fusion::{
    FederatedRankedHit, federated_rrf_score, merge_federated_ranked_hits,
    normalize_search_source_filter_value, normalized_search_hit_origin_kind,
    normalized_search_hit_source_id_parts, remote_source_filter_is_post_applied,
    session_paths_filter_is_post_applied, snippet_from_preview_without_full_content,
    snippet_terms_for_query, stable_hit_hash, stored_preview_is_complete_content,
};
#[cfg(any(test, feature = "cass-layer-b-conformance"))]
use super::fusion::{SearchHitKey, postprocess_hits_page_core};
use super::parsing::dominant_match_type;
#[cfg(any(test, feature = "cass-layer-b-conformance"))]
use super::progressive::RankedMessageHydrationCandidate;
use super::sqlite_hydration::sql_placeholders;
#[cfg(any(test, feature = "cass-layer-b-conformance"))]
use super::sqlite_hydration::{
    SQLITE_MAX_VARIABLE_NUMBER, SearchReadTransaction, nfc_sanitize_query,
};
use super::sqlite_hydration::{
    TantivyContentExactKey, TantivyContentFallbackKey, TantivyHydratedContentMaps,
    franken_query_map_collect_retry, hydrate_message_content_by_conversation,
};
#[cfg(any(test, feature = "cass-layer-b-conformance"))]
use super::types::{
    CASS_LEXICAL_LAYER_B_CANDIDATE_MAX, CassLexicalLayerBError, CassLexicalLayerBHit,
    CassLexicalLayerBNoClaim, CassLexicalLayerBPage, CassLexicalLayerBRequest,
};
use super::types::{FieldMask, SearchFilters, SearchHit, execute_query_with_bounded_exact_count};

impl SearchClient {
    /// Project an engine-ranked candidate stream through the production CASS
    /// SQLite hydrator and result reducer.
    ///
    /// This is an in-memory conformance seam, not an artifact or admission
    /// API. The caller owns the Layer-A query/filter execution and must provide
    /// exact `(conversation_id, message_index)` identities from the engine's
    /// stored fields. CASS resolves those pairs fail-closed, then applies the
    /// same SQLite message projector used by shipping semantic/progressive
    /// hydration and the same noise filter, deduplicator, `session_paths`
    /// filter, and page reducer used across shipping search paths. The output
    /// carries an explicit no-claim for Tantivy's separate stored-document
    /// materializer; this bounded seam does not prove those two projectors are
    /// equivalent.
    ///
    /// # Errors
    ///
    /// Returns a typed validation error for malformed or ambiguous candidate
    /// streams and a wrapped hydration error for SQLite failures.
    #[cfg(any(test, feature = "cass-layer-b-conformance"))]
    pub fn project_cass_lexical_layer_b_page(
        &self,
        request: CassLexicalLayerBRequest<'_>,
    ) -> std::result::Result<CassLexicalLayerBPage, CassLexicalLayerBError> {
        if request.candidates.len() > CASS_LEXICAL_LAYER_B_CANDIDATE_MAX {
            return Err(CassLexicalLayerBError::CandidateBoundExceeded {
                actual: request.candidates.len(),
                maximum: CASS_LEXICAL_LAYER_B_CANDIDATE_MAX,
            });
        }
        if request.limit == 0 {
            return Err(CassLexicalLayerBError::ZeroLimit);
        }
        request
            .offset
            .checked_add(request.limit)
            .ok_or(CassLexicalLayerBError::PaginationOverflow)?;
        if request.field_mask.wants_snippet() {
            return Err(CassLexicalLayerBError::SnippetNotSupported);
        }
        if request.field_mask.preview_content_limit().is_some() {
            return Err(CassLexicalLayerBError::BoundedPreviewNotSupported);
        }

        let mut seen_identities = HashSet::with_capacity(request.candidates.len());
        let mut indices_by_conversation: HashMap<i64, Vec<i64>> = HashMap::new();
        for (position, candidate) in request.candidates.iter().enumerate() {
            if candidate.rank != position {
                return Err(CassLexicalLayerBError::NonCanonicalRank {
                    position,
                    actual_rank: candidate.rank,
                });
            }
            if candidate.conversation_id <= 0 {
                return Err(CassLexicalLayerBError::InvalidConversationId {
                    rank: candidate.rank,
                    conversation_id: candidate.conversation_id,
                });
            }
            let message_index = i64::try_from(candidate.message_index).map_err(|_| {
                CassLexicalLayerBError::MessageIndexOutOfRange {
                    rank: candidate.rank,
                }
            })?;
            if !f32::from_bits(candidate.score_bits).is_finite() {
                return Err(CassLexicalLayerBError::NonFiniteScore {
                    rank: candidate.rank,
                });
            }
            if !seen_identities.insert((candidate.conversation_id, candidate.message_index)) {
                return Err(CassLexicalLayerBError::DuplicateIdentity {
                    conversation_id: candidate.conversation_id,
                    message_index: candidate.message_index,
                });
            }
            indices_by_conversation
                .entry(candidate.conversation_id)
                .or_default()
                .push(message_index);
        }

        let sanitized_query = nfc_sanitize_query(request.raw_query);
        let match_type = dominant_match_type(&sanitized_query);
        let (resolved_message_ids, hydrated) = {
            let sqlite_guard = self
                .sqlite_guard()
                .map_err(CassLexicalLayerBError::Hydration)?;
            let conn = sqlite_guard.as_ref().ok_or_else(|| {
                CassLexicalLayerBError::Hydration(anyhow!(
                    "CASS Layer-B projection requires a database connection"
                ))
            })?;
            let mut transaction = SearchReadTransaction::begin(conn)
                .map_err(|error| CassLexicalLayerBError::Hydration(anyhow::Error::new(error)))?;
            let mut conversation_ids = indices_by_conversation.keys().copied().collect::<Vec<_>>();
            conversation_ids.sort_unstable();
            let mut resolved_by_identity = HashMap::with_capacity(request.candidates.len());

            for conversation_id in conversation_ids {
                let Some(message_indices) = indices_by_conversation.get_mut(&conversation_id)
                else {
                    continue;
                };
                message_indices.sort_unstable();
                for chunk in message_indices.chunks(SQLITE_MAX_VARIABLE_NUMBER - 1) {
                    let placeholders = sql_placeholders(chunk.len());
                    let sql = format!(
                        "SELECT id, conversation_id, idx
                         FROM messages
                         WHERE conversation_id = ? AND idx IN ({placeholders})
                         ORDER BY idx, id"
                    );
                    let mut params = Vec::with_capacity(chunk.len() + 1);
                    params.push(ParamValue::from(conversation_id));
                    params.extend(chunk.iter().copied().map(ParamValue::from));
                    let rows: Vec<(i64, i64, i64)> = transaction
                        .query_map_collect(&sql, &params, |row| {
                            Ok((row.get_typed(0)?, row.get_typed(1)?, row.get_typed(2)?))
                        })
                        .map_err(|error| {
                            CassLexicalLayerBError::Hydration(anyhow::Error::new(error))
                        })?;
                    for (message_id, row_conversation_id, message_index) in rows {
                        let message_id = u64::try_from(message_id).map_err(|_| {
                            CassLexicalLayerBError::Hydration(anyhow!(
                                "CASS Layer-B resolved a negative message id"
                            ))
                        })?;
                        if message_id == 0 {
                            return Err(CassLexicalLayerBError::Hydration(anyhow!(
                                "CASS Layer-B resolved invalid message id 0"
                            )));
                        }
                        let public_message_index = u64::try_from(message_index).map_err(|_| {
                            CassLexicalLayerBError::Hydration(anyhow!(
                                "CASS Layer-B resolved a negative message index"
                            ))
                        })?;
                        let key = (row_conversation_id, message_index);
                        if resolved_by_identity.insert(key, message_id).is_some() {
                            return Err(CassLexicalLayerBError::UnresolvedIdentity {
                                conversation_id: row_conversation_id,
                                message_index: public_message_index,
                            });
                        }
                    }
                }
            }

            let resolved_message_ids = request
                .candidates
                .iter()
                .map(|candidate| {
                    let message_index = i64::try_from(candidate.message_index).map_err(|_| {
                        CassLexicalLayerBError::MessageIndexOutOfRange {
                            rank: candidate.rank,
                        }
                    })?;
                    resolved_by_identity
                        .get(&(candidate.conversation_id, message_index))
                        .copied()
                        .ok_or(CassLexicalLayerBError::UnresolvedIdentity {
                            conversation_id: candidate.conversation_id,
                            message_index: candidate.message_index,
                        })
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let ranked_messages = request
                .candidates
                .iter()
                .zip(resolved_message_ids.iter().copied())
                .map(|(candidate, message_id)| RankedMessageHydrationCandidate {
                    message_id,
                    score: f32::from_bits(candidate.score_bits),
                })
                .collect::<Vec<_>>();
            let hydrated = self
                .hydrate_ranked_message_hits_in_transaction(
                    &transaction,
                    &ranked_messages,
                    request.field_mask,
                    match_type,
                )
                .map_err(CassLexicalLayerBError::Hydration)?;
            transaction
                .rollback()
                .map_err(|error| CassLexicalLayerBError::Hydration(anyhow::Error::new(error)))?;
            (resolved_message_ids, hydrated)
        };

        let hydrated_message_ids = hydrated
            .iter()
            .map(|(message_id, _)| *message_id)
            .collect::<HashSet<_>>();
        for message_id in &resolved_message_ids {
            if !hydrated_message_ids.contains(message_id) {
                return Err(CassLexicalLayerBError::HydratedCandidateMissing {
                    message_id: *message_id,
                });
            }
        }

        let candidate_by_message_id = resolved_message_ids
            .iter()
            .copied()
            .zip(request.candidates.iter().copied())
            .collect::<HashMap<_, _>>();
        // The exact identity resolver has already rejected duplicate
        // `(conversation_id, message_index)` rows and candidate identities.
        // Since hydration derives `conversation_id` and `line_number` from
        // that same transaction snapshot, two valid candidates cannot share a
        // `SearchHitKey`. Keep the occupied-entry handling defensive so a
        // future projector change still binds the reducer's higher-score
        // winner to the same message and input rank it retained.
        let mut binding_by_hit_key: HashMap<SearchHitKey, (u64, usize, f32)> = HashMap::new();
        for (message_id, hit) in &hydrated {
            let candidate = candidate_by_message_id
                .get(message_id)
                .ok_or(CassLexicalLayerBError::LostCandidateBinding)?;
            let key = SearchHitKey::from_hit(hit);
            match binding_by_hit_key.entry(key) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert((*message_id, candidate.rank, hit.score));
                }
                std::collections::hash_map::Entry::Occupied(mut entry)
                    if entry.get().2 < hit.score =>
                {
                    entry.insert((*message_id, candidate.rank, hit.score));
                }
                std::collections::hash_map::Entry::Occupied(_) => {}
            }
        }
        let hydrated_candidate_count = hydrated.len();
        let (available_hits, hits) = postprocess_hits_page_core(
            hydrated.into_iter().map(|(_, hit)| hit).collect(),
            &sanitized_query,
            request.session_paths,
            request.limit,
            request.offset,
        );
        let hits = hits
            .into_iter()
            .map(|hit| {
                let key = SearchHitKey::from_hit(&hit);
                let (message_id, input_rank, _) = binding_by_hit_key
                    .remove(&key)
                    .ok_or(CassLexicalLayerBError::LostCandidateBinding)?;
                Ok(CassLexicalLayerBHit {
                    message_id,
                    input_rank,
                    hit,
                })
            })
            .collect::<std::result::Result<Vec<_>, CassLexicalLayerBError>>()?;

        Ok(CassLexicalLayerBPage {
            input_candidate_count: request.candidates.len(),
            hydrated_candidate_count,
            available_hits,
            hits,
            no_claims: [
                CassLexicalLayerBNoClaim::AdaptiveFetchRetry,
                CassLexicalLayerBNoClaim::AutomaticWildcardFallback,
                CassLexicalLayerBNoClaim::BoundedPreviewProjection,
                CassLexicalLayerBNoClaim::QueryAwareSnippet,
                CassLexicalLayerBNoClaim::ShippingTantivyMaterializer,
                CassLexicalLayerBNoClaim::TotalCountPrecision,
                CassLexicalLayerBNoClaim::WorkspaceOriginal,
            ],
        })
    }

    pub(in crate::search::query) fn federated_readers(
        &self,
    ) -> Option<Arc<Vec<FederatedIndexReader>>> {
        FEDERATED_SEARCH_READERS
            .read()
            .get(&self.cache_namespace)
            .cloned()
    }

    pub(in crate::search::query) fn maybe_reload_federated_readers(
        &self,
        readers: &[FederatedIndexReader],
    ) -> Result<Option<u64>> {
        if !self.reload_on_search || readers.is_empty() {
            return Ok(None);
        }
        const MIN_RELOAD_INTERVAL: Duration = Duration::from_millis(300);
        let now = Instant::now();
        let mut guard = self.last_reload.lock().unwrap_or_else(|e| e.into_inner());
        if guard
            .map(|t| now.duration_since(t) < MIN_RELOAD_INTERVAL)
            .unwrap_or(false)
        {
            let signature = self.federated_generation_signature(readers);
            return Ok(Some(signature));
        }

        let reload_started = Instant::now();
        let cached_generation = self.federated_generation_signature(readers);
        let republished = match reload_index_readers_bounded(
            readers.iter().map(|shard| shard.reader.clone()).collect(),
            Arc::clone(&self.metrics.reload_in_flight),
        ) {
            Ok(republished) => republished,
            Err(error) => {
                self.metrics
                    .record_cache_lookup(crate::daemon_runtime_state::CacheLookup {
                        cache_hit: true,
                        cached_generation: Some(self.federated_generation_signature(readers)),
                        current_generation: self.federated_generation_signature(readers),
                        reload_attempted: true,
                        reload_succeeded: false,
                        served_fallback: false,
                    });
                return Err(error);
            }
        };
        if republished {
            self.reopen_republished_lexical_index()?;
            *guard = Some(Instant::now());
            return Ok(self
                .federated_readers()
                .map(|reopened| self.federated_generation_signature(&reopened)));
        }
        let elapsed = reload_started.elapsed();
        // Rate-limit from completion, not start. A slow successful reload must
        // not become immediately eligible for another reload merely because it
        // consumed the whole minimum interval.
        *guard = Some(Instant::now());
        let epoch = self.reload_epoch.fetch_add(1, Ordering::SeqCst) + 1;
        self.metrics.record_reload(elapsed);
        let current_generation = self.federated_generation_signature(readers);
        if cached_generation != current_generation {
            self.metrics
                .record_cache_lookup(crate::daemon_runtime_state::CacheLookup {
                    cache_hit: true,
                    cached_generation: Some(cached_generation),
                    current_generation,
                    reload_attempted: true,
                    reload_succeeded: true,
                    served_fallback: false,
                });
        }
        tracing::debug!(
            duration_ms = elapsed.as_millis() as u64,
            reload_epoch = epoch,
            shards = readers.len(),
            "tantivy_reader_reload_federated"
        );
        Ok(Some(self.federated_generation_signature(readers)))
    }

    pub(in crate::search::query) fn federated_generation_signature(
        &self,
        readers: &[FederatedIndexReader],
    ) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        readers.len().hash(&mut hasher);
        for shard in readers {
            // Quill publishes a monotonic keeper generation, which is the
            // direct analogue of Tantivy's searcher generation id.
            shard.reader.keeper_generation().hash(&mut hasher);
        }
        hasher.finish()
    }

    pub(in crate::search::query) fn track_generation(&self, generation: u64) {
        let mut guard = self
            .last_generation
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(prev) = *guard
            && prev != generation
        {
            self.metrics
                .record_cache_lookup(crate::daemon_runtime_state::CacheLookup {
                    cache_hit: true,
                    cached_generation: Some(prev),
                    current_generation: generation,
                    reload_attempted: false,
                    reload_succeeded: false,
                    served_fallback: false,
                });
            if let Ok(mut cache) = self.prefix_cache.lock() {
                // A search already using the prior generation must not refill
                // this cache after its invalidation.
                self.reload_epoch.fetch_add(1, Ordering::SeqCst);
                cache.clear();
            }
        }
        *guard = Some(generation);
    }

    pub(in crate::search::query) fn hydrate_tantivy_hit_contents(
        &self,
        exact_keys: &[TantivyContentExactKey],
        fallback_keys: &[TantivyContentFallbackKey],
    ) -> Result<TantivyHydratedContentMaps> {
        if exact_keys.is_empty() && fallback_keys.is_empty() {
            return Ok((HashMap::new(), HashMap::new()));
        }

        let sqlite_guard = match self.sqlite_guard() {
            Ok(guard) => guard,
            Err(_) => return Ok((HashMap::new(), HashMap::new())),
        };
        let Some(conn) = sqlite_guard.as_ref() else {
            return Ok((HashMap::new(), HashMap::new()));
        };

        let mut hydrated_exact = HashMap::new();
        let mut hydrated_fallback = HashMap::new();
        const CHUNK_SIZE: usize = 300;

        if !exact_keys.is_empty() {
            let mut unique_exact_keys = Vec::with_capacity(exact_keys.len());
            let mut seen = HashSet::with_capacity(exact_keys.len());
            for key in exact_keys {
                if seen.insert(*key) {
                    unique_exact_keys.push(*key);
                }
            }

            hydrated_exact.extend(hydrate_message_content_by_conversation(
                conn,
                &unique_exact_keys,
            )?);
        }

        if !fallback_keys.is_empty() {
            let mut unique_fallback_keys = Vec::with_capacity(fallback_keys.len());
            let mut seen = HashSet::with_capacity(fallback_keys.len());
            for key in fallback_keys {
                if seen.insert(key.clone()) {
                    unique_fallback_keys.push(key.clone());
                }
            }

            let mut unique_source_paths = Vec::with_capacity(unique_fallback_keys.len());
            let mut seen_source_paths = HashSet::with_capacity(unique_fallback_keys.len());
            for (_, source_path, _) in &unique_fallback_keys {
                if seen_source_paths.insert(source_path.clone()) {
                    unique_source_paths.push(source_path.clone());
                }
            }

            let mut conversations_by_key: HashMap<(String, String), Vec<i64>> = HashMap::new();
            for chunk in unique_source_paths.chunks(CHUNK_SIZE) {
                let placeholders = sql_placeholders(chunk.len());
                let sql = format!(
                    "SELECT c.id,
                            c.source_path,
                            COALESCE(c.source_id, ''),
                            COALESCE(c.origin_host, ''),
                            COALESCE(s.kind, '')
                     FROM conversations c
                     LEFT JOIN sources s ON c.source_id = s.id
                     WHERE c.source_path IN ({placeholders})
                     ORDER BY c.id"
                );
                let params = chunk
                    .iter()
                    .map(|source_path| ParamValue::from(source_path.clone()))
                    .collect::<Vec<_>>();
                let rows: Vec<(i64, String, String, String, String)> =
                    franken_query_map_collect_retry(conn, &sql, &params, |row| {
                        Ok((
                            row.get_typed(0)?,
                            row.get_typed(1)?,
                            row.get_typed(2)?,
                            row.get_typed(3)?,
                            row.get_typed(4)?,
                        ))
                    })?;

                for (conversation_id, source_path, raw_source_id, origin_host, origin_kind) in rows
                {
                    let normalized_source_id = normalized_search_hit_source_id_parts(
                        &raw_source_id,
                        &origin_kind,
                        (!origin_host.trim().is_empty()).then_some(origin_host.as_str()),
                    );
                    conversations_by_key
                        .entry((normalized_source_id, source_path))
                        .or_default()
                        .push(conversation_id);
                }
            }

            let mut message_requests = Vec::new();
            let mut fallback_keys_by_exact: HashMap<
                TantivyContentExactKey,
                Vec<TantivyContentFallbackKey>,
            > = HashMap::new();
            let mut seen_message_requests = HashSet::new();
            for (source_id, source_path, line_idx) in &unique_fallback_keys {
                let key = (source_id.clone(), source_path.clone());
                let Some(conversation_ids) = conversations_by_key.get(&key) else {
                    continue;
                };
                for &conversation_id in conversation_ids {
                    let exact_key = (conversation_id, *line_idx);
                    if seen_message_requests.insert(exact_key) {
                        message_requests.push(exact_key);
                    }
                    fallback_keys_by_exact.entry(exact_key).or_default().push((
                        source_id.clone(),
                        source_path.clone(),
                        *line_idx,
                    ));
                }
            }

            for ((conversation_id, line_idx), content) in
                hydrate_message_content_by_conversation(conn, &message_requests)?
            {
                if let Some(fallback_keys) =
                    fallback_keys_by_exact.get(&(conversation_id, line_idx))
                {
                    for fallback_key in fallback_keys {
                        hydrated_fallback.insert(fallback_key.clone(), content.clone());
                    }
                }
            }
        }

        Ok((hydrated_exact, hydrated_fallback))
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::search::query) fn search_tantivy(
        &self,
        reader: &frankensearch::quill::QuillSearchIndex,
        fields: &crate::search::quill_bridge::QuillCassFields,
        raw_query: &str,
        sanitized_query: &str,
        filters: SearchFilters,
        limit: usize,
        offset: usize,
        field_mask: FieldMask,
    ) -> Result<(Vec<SearchHit>, Option<usize>)> {
        struct PendingTantivyHit {
            score: f32,
            title: String,
            stored_content: String,
            stored_preview: String,
            agent: String,
            source_path: String,
            workspace: String,
            workspace_original: Option<String>,
            created_at: Option<i64>,
            line_number: Option<usize>,
            stored_preview_snippet: Option<String>,
            source_id: String,
            conversation_id: Option<i64>,
            raw_origin_kind: Option<String>,
            origin_host: Option<String>,
        }

        let wants_snippet = field_mask.wants_snippet();
        let needs_content = field_mask.needs_content() || wants_snippet;

        // Delegate cass-compatible query parsing + Tantivy clause construction to frankensearch.
        // cass retains ownership of paging/fallback orchestration and stored-field hydration.
        let remote_post_filter = remote_source_filter_is_post_applied(&filters.source_filter);
        let session_paths_post_filter =
            session_paths_filter_is_post_applied(&filters.session_paths);
        let fs_filters = FsCassQueryFilters {
            agents: filters.agents.into_iter().collect(),
            workspaces: filters.workspaces.into_iter().collect(),
            created_from: filters.created_from,
            created_to: filters.created_to,
            source_filter: match filters.source_filter {
                SourceFilter::All => FsCassSourceFilter::All,
                SourceFilter::Local => FsCassSourceFilter::Local,
                // The engine's `Remote` clause matches the `origin_kind` term
                // `ssh`, but cass indexes remote provenance as `remote` (see
                // `normalized_index_origin_kind`), so the engine clause can
                // never match a cass document. Fetch unfiltered and select
                // remote hits in `postprocess_hits_page`, which already owns
                // the other stored-but-not-indexed filter (`session_paths`)
                // and its over-fetch/retry accounting (bead 5bf29).
                SourceFilter::Remote => FsCassSourceFilter::All,
                SourceFilter::SourceId(id) => {
                    FsCassSourceFilter::SourceId(normalize_search_source_filter_value(&id))
                }
            },
        };

        // NOTE: session_paths filtering is applied post-search since source_path
        // is STORED but not indexed. See apply_session_paths_filter().
        // Quill parses CASS's query language natively; the parsed tree is the
        // engine-neutral form its executor takes.
        let parser = frankensearch::quill::query::CassQueryParser::new(
            frankensearch::quill::schema::CASS_SEMANTIC_SCHEMA,
        )
        .map_err(|error| anyhow::anyhow!("building the CASS query parser: {error}"))?;
        let parsed = parser.parse(raw_query, &fs_filters);
        // Plain lexical hits are canonical messages only. Persisted
        // search-region documents share this index in the synthetic
        // `msg_idx >= REGION_DOC_BASE` space (Task 5); the guard keeps
        // them out of hits, pages, and exact totals while leaving the
        // session-search gate (`raw_page`/`decode_hit`) untouched.
        let q = crate::search::quill_bridge::canonical_message_query(parsed.query);

        let prefix_only = is_prefix_only(sanitized_query);
        let top_docs = execute_query_with_bounded_exact_count(reader, &q, limit, offset)?;
        // With the remote selection applied after hydration the engine count
        // covers every origin, so it is no longer an exact total for the page.
        // The same holds for the session_paths scope (review 2026-11-02 Q1).
        let tantivy_total_count = if remote_post_filter || session_paths_post_filter {
            None
        } else {
            top_docs.total_count
        };
        let query_match_type = dominant_match_type(sanitized_query);
        let mut pending_hits = Vec::with_capacity(top_docs.hits.len());
        let mut missing_exact_content_keys = Vec::new();
        let mut missing_fallback_content_keys = Vec::new();

        for ranked_hit in top_docs.hits {
            let score = ranked_hit.bm25_score;
            let global_docid = ranked_hit.global_docid;
            let title = if field_mask.wants_title() {
                crate::search::quill_bridge::stored_text(reader, fields.title, global_docid)?
                    .unwrap_or_default()
            } else {
                String::new()
            };
            let stored_content =
                crate::search::quill_bridge::stored_text(reader, fields.content, global_docid)?
                    .unwrap_or_default();
            let stored_preview =
                crate::search::quill_bridge::stored_text(reader, fields.preview, global_docid)?
                    .unwrap_or_default();
            let stored_preview_snippet = snippet_from_preview_without_full_content(
                field_mask,
                &stored_preview,
                sanitized_query,
            );
            let agent =
                crate::search::quill_bridge::stored_text(reader, fields.agent, global_docid)?
                    .unwrap_or_default();
            let workspace =
                crate::search::quill_bridge::stored_text(reader, fields.workspace, global_docid)?
                    .unwrap_or_default();
            let workspace_original = crate::search::quill_bridge::stored_text(
                reader,
                fields.workspace_original,
                global_docid,
            )?
            .filter(|value| !value.is_empty());
            let created_at =
                crate::search::quill_bridge::stored_i64(reader, fields.created_at, global_docid)?;
            let line_number =
                crate::search::quill_bridge::stored_u64(reader, fields.msg_idx, global_docid)?
                    .and_then(|value| usize::try_from(value).ok())
                    .map(|value| value.saturating_add(1));
            let raw_source_id =
                crate::search::quill_bridge::stored_text(reader, fields.source_id, global_docid)?
                    .unwrap_or_default();
            // The compiled CASS schema always carries conversation_id, so the
            // incumbent's Option<Field> lookup collapses to a direct read.
            let conversation_id = crate::search::quill_bridge::stored_i64(
                reader,
                fields.conversation_id,
                global_docid,
            )?;
            let source_path =
                crate::search::quill_bridge::stored_text(reader, fields.source_path, global_docid)?
                    .unwrap_or_default();
            let raw_origin_kind =
                crate::search::quill_bridge::stored_text(reader, fields.origin_kind, global_docid)?;
            let origin_host =
                crate::search::quill_bridge::stored_text(reader, fields.origin_host, global_docid)?
                    .filter(|value| !value.is_empty());
            let source_id = normalized_search_hit_source_id_parts(
                raw_source_id.as_str(),
                raw_origin_kind.as_deref().unwrap_or_default(),
                origin_host.as_deref(),
            );

            let preview_satisfies_bounded_content =
                field_mask.preview_content_limit().is_some() && !stored_preview.is_empty();
            let preview_satisfies_full_content = field_mask.needs_content()
                && field_mask.preview_content_limit().is_none()
                && stored_preview_is_complete_content(&stored_preview);
            if needs_content
                && let Some(line_idx) = line_number
                    .and_then(|line| line.checked_sub(1))
                    .and_then(|line| i64::try_from(line).ok())
                && stored_content.is_empty()
                && !preview_satisfies_bounded_content
                && !preview_satisfies_full_content
                && stored_preview_snippet.is_none()
            {
                if let Some(conversation_id) = conversation_id {
                    missing_exact_content_keys.push((conversation_id, line_idx));
                } else {
                    missing_fallback_content_keys.push((
                        source_id.clone(),
                        source_path.clone(),
                        line_idx,
                    ));
                }
            }

            pending_hits.push(PendingTantivyHit {
                score,
                title,
                stored_content,
                stored_preview,
                agent,
                source_path,
                workspace,
                workspace_original,
                created_at,
                line_number,
                stored_preview_snippet,
                source_id,
                conversation_id,
                raw_origin_kind,
                origin_host,
            });
        }

        let (hydrated_contents, hydrated_fallback_contents) = if needs_content
            && (!missing_exact_content_keys.is_empty() || !missing_fallback_content_keys.is_empty())
        {
            self.hydrate_tantivy_hit_contents(
                &missing_exact_content_keys,
                &missing_fallback_content_keys,
            )?
        } else {
            (HashMap::new(), HashMap::new())
        };
        let needs_tantivy_snippet_generator = wants_snippet
            && !prefix_only
            && pending_hits
                .iter()
                .any(|pending| pending.stored_preview_snippet.is_none());
        let mut snippet_generator = if needs_tantivy_snippet_generator {
            let snippet_cfg = frankensearch::quill::SnippetConfig {
                max_chars: 160,
                highlight_prefix: "<b>".to_string(),
                highlight_postfix: "</b>".to_string(),
            };
            // Quill compiles a generator from terms and renders against source
            // text, so the query's terms drive it directly.
            let terms = snippet_terms_for_query(sanitized_query);
            Some(crate::search::quill_bridge::content_snippet_generator(
                &terms,
                snippet_cfg,
            ))
        } else {
            None
        };
        let mut hits = Vec::with_capacity(pending_hits.len());
        for pending in pending_hits {
            let hydrated_content = pending
                .line_number
                .and_then(|line| line.checked_sub(1))
                .and_then(|line| i64::try_from(line).ok())
                .and_then(|line_idx| {
                    if let Some(conversation_id) = pending.conversation_id {
                        hydrated_contents.get(&(conversation_id, line_idx)).cloned()
                    } else {
                        hydrated_fallback_contents
                            .get(&(
                                pending.source_id.clone(),
                                pending.source_path.clone(),
                                line_idx,
                            ))
                            .cloned()
                    }
                });
            let preview_satisfies_effective_content = !pending.stored_preview.is_empty()
                && (field_mask.preview_content_limit().is_some()
                    || (field_mask.needs_content()
                        && field_mask.preview_content_limit().is_none()
                        && stored_preview_is_complete_content(&pending.stored_preview)));
            let effective_content = if !pending.stored_content.is_empty() {
                pending.stored_content.clone()
            } else if preview_satisfies_effective_content {
                pending.stored_preview.clone()
            } else if let Some(content) = hydrated_content {
                content
            } else {
                pending.stored_preview.clone()
            };
            let snippet = if wants_snippet {
                if let Some(snippet) = pending.stored_preview_snippet.clone() {
                    snippet
                } else if let Some(generator) = snippet_generator.as_mut() {
                    // Borrowed mutably rather than cloned per hit: `snippet()`
                    // takes `&mut self` only for the analyzer's internal
                    // buffers — it reads the config and term weights and keeps
                    // no state between calls — so one generator serves every
                    // hit and the per-hit clone was pure allocation.
                    let rendered = if !pending.stored_content.is_empty() {
                        generator.snippet(&pending.stored_content)
                    } else if !effective_content.is_empty() {
                        generator.snippet(&effective_content)
                    } else {
                        None
                    };
                    rendered
                        .map(|html| html.replace("<b>", "**").replace("</b>", "**"))
                        .or_else(|| cached_prefix_snippet(&effective_content, sanitized_query, 160))
                        .unwrap_or_else(|| {
                            quick_prefix_snippet(&effective_content, sanitized_query, 160)
                        })
                } else if let Some(sn) =
                    cached_prefix_snippet(&effective_content, sanitized_query, 160)
                {
                    sn
                } else {
                    quick_prefix_snippet(&effective_content, sanitized_query, 160)
                }
            } else {
                String::new()
            };
            let content = if field_mask.needs_content() {
                effective_content.clone()
            } else {
                String::new()
            };
            let content_hash = stable_hit_hash(
                &effective_content,
                &pending.source_path,
                pending.line_number,
                pending.created_at,
            );
            let origin_kind = normalized_search_hit_origin_kind(
                &pending.source_id,
                pending.raw_origin_kind.as_deref(),
            )
            .to_string();
            hits.push(SearchHit {
                title: pending.title,
                snippet,
                content,
                content_hash,
                conversation_id: pending.conversation_id,
                score: pending.score,
                source_path: pending.source_path,
                agent: pending.agent,
                workspace: pending.workspace,
                workspace_original: pending.workspace_original,
                created_at: pending.created_at,
                line_number: pending.line_number,
                match_type: query_match_type,
                source_id: pending.source_id,
                origin_kind,
                origin_host: pending.origin_host,
            });
        }
        Ok((hits, tantivy_total_count))
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::search::query) fn search_tantivy_federated(
        &self,
        readers: &[FederatedIndexReader],
        raw_query: &str,
        sanitized_query: &str,
        filters: SearchFilters,
        limit: usize,
        field_mask: FieldMask,
    ) -> Result<(Vec<SearchHit>, Option<usize>)> {
        let mut ranked_hits = Vec::new();
        let mut total_count = Some(0usize);

        for (shard_index, shard) in readers.iter().enumerate() {
            let (shard_hits, shard_total_count) = self.search_tantivy(
                &shard.reader,
                &shard.fields,
                raw_query,
                sanitized_query,
                filters.clone(),
                limit,
                0,
                field_mask,
            )?;
            total_count = match (total_count, shard_total_count) {
                (Some(total), Some(shard_total)) => Some(total.saturating_add(shard_total)),
                _ => None,
            };
            for (shard_rank, hit) in shard_hits.into_iter().enumerate() {
                ranked_hits.push(FederatedRankedHit {
                    hit,
                    shard_index,
                    shard_rank,
                    fused_score: federated_rrf_score(shard_rank),
                });
            }
        }

        let raw_hit_count = ranked_hits.len();
        let generation_signature = self.federated_generation_signature(readers);
        self.track_generation(generation_signature);
        let combined_hits = merge_federated_ranked_hits(ranked_hits);
        tracing::debug!(
            generation_signature,
            shard_count = readers.len(),
            total_count,
            raw_hit_count,
            returned_hit_count = combined_hits.len(),
            merge_policy = "rrf_rank_then_stable_hit_key",
            "federated lexical search merged shard results"
        );

        Ok((combined_hits, total_count))
    }
}
