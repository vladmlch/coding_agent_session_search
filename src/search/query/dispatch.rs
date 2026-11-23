use anyhow::{Result, anyhow};
use frankensearch::lexical_tantivy::{
    CASS_SCHEMA_HASH as FS_CASS_SCHEMA_HASH,
    cass_has_boolean_operators as fs_cass_has_boolean_operators,
};
use frankensqlite::AsyncConnection as SearchSqliteConnection;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::franken_sync::compat::RowExt;
use crate::search::vector_index::SemanticAnnUnavailableReason;

use super::cache::{
    CACHE_BYTE_CAP, CACHE_KEY_VERSION, CACHE_TOTAL_CAP, CacheShards, FEDERATED_SEARCH_READERS,
    FederatedIndexReader, LexicalReaderSlot, Metrics, SEARCH_CLIENT_INSTANCE_COUNTER,
    maybe_spawn_warm_worker,
};
use super::fusion::{
    postprocess_hits_page_core, remote_source_filter_is_post_applied, rrf_fuse_hits,
    should_skip_automatic_wildcard_fallback_for_long_zero_hit_query, should_try_wildcard_fallback,
};
use super::parsing::{QuerySuggestion, ensure_query_nesting_depth};
use super::sqlite_hydration::{
    SearchSqliteConnectionExt, nfc_sanitize_query, open_search_hydration_sqlite,
};
use super::types::{
    FieldMask, MatchType, SearchFilters, SearchHit, SearchResult, SemanticTierMode,
    automatic_wildcard_fallback_max_docs, effective_field_mask, hybrid_candidate_budget,
    no_limit_result_cap, should_allow_automatic_wildcard_fallback,
};
use super::{
    LEXICAL_DEGRADE_QUERY_FUEL_EXHAUSTED, LEXICAL_RETRY_QUERY_FUEL_EXHAUSTED, SearchClient,
    SearchClientOptions, WILDCARD_FALLBACK_SKIPPED_DISABLED, WILDCARD_FALLBACK_SKIPPED_LARGE_INDEX,
    WILDCARD_FALLBACK_SKIPPED_LONG_TERM, WILDCARD_FALLBACK_SKIPPED_QUERY_FUEL_EXHAUSTED,
};

/// Calculate Levenshtein edit distance between two strings.
/// Used for typo detection in did-you-mean suggestions.
pub(in crate::search::query) fn levenshtein_distance(a: &str, b: &str) -> usize {
    let a_chars: Vec<char> = a.chars().collect();
    let b_chars: Vec<char> = b.chars().collect();
    let a_len = a_chars.len();
    let b_len = b_chars.len();

    if a_len == 0 {
        return b_len;
    }
    if b_len == 0 {
        return a_len;
    }

    // Use two rows for space efficiency
    let mut prev_row: Vec<usize> = (0..=b_len).collect();
    let mut curr_row: Vec<usize> = vec![0; b_len + 1];

    for (i, a_char) in a_chars.iter().enumerate() {
        curr_row[0] = i + 1;
        for (j, b_char) in b_chars.iter().enumerate() {
            let cost = usize::from(a_char != b_char);
            curr_row[j + 1] = (prev_row[j + 1] + 1) // deletion
                .min(curr_row[j] + 1) // insertion
                .min(prev_row[j] + cost); // substitution
        }
        std::mem::swap(&mut prev_row, &mut curr_row);
    }

    prev_row[b_len]
}

impl SearchClient {
    /// Create a database-backed semantic client without opening a lexical index.
    ///
    /// The archive connection and semantic assets remain lazy. Callers must
    /// attach a validated semantic context before querying; this constructor
    /// performs no index repair, model loading, or background work.
    pub fn open_semantic(db_path: &Path, options: SearchClientOptions) -> Result<Option<Self>> {
        Self::from_opened_lexical_index(
            crate::search::tantivy::OpenedLexicalIndex {
                path: db_path.to_path_buf(),
                reader: None,
                federated_readers: None,
            },
            Some(db_path),
            options,
        )
    }

    pub fn open(index_path: &Path, db_path: Option<&Path>) -> Result<Option<Self>> {
        Self::open_with_options(index_path, db_path, SearchClientOptions::default())
    }

    pub fn open_with_options(
        index_path: &Path,
        db_path: Option<&Path>,
        options: SearchClientOptions,
    ) -> Result<Option<Self>> {
        let tantivy = crate::search::quill_bridge::open_cass_reader(index_path)
            .map(|reader| {
                (
                    reader,
                    crate::search::quill_bridge::QuillCassFields::compiled(),
                )
            })
            .ok();
        let federated_readers = if tantivy.is_none() {
            crate::search::tantivy::open_federated_search_readers(index_path)
                .ok()
                .flatten()
                .filter(|readers| !readers.is_empty())
        } else {
            None
        };

        if tantivy.is_none() && federated_readers.is_none() && db_path.is_some_and(Path::exists) {
            tracing::warn!(
                index_path = %index_path.display(),
                "Tantivy search index not found or incompatible. \
                 Search results will be degraded. \
                 Run `cass index --full` to rebuild the index."
            );
        }

        Self::from_opened_lexical_index(
            crate::search::tantivy::OpenedLexicalIndex {
                path: index_path.to_path_buf(),
                reader: tantivy,
                federated_readers,
            },
            db_path,
            options,
        )
    }

    /// Consume the reader used by lexical admission without opening its path
    /// again. This preserves the admitted snapshot even if publication changes
    /// between validation and constructing the client.
    pub(crate) fn from_opened_lexical_index(
        index: crate::search::tantivy::OpenedLexicalIndex,
        db_path: Option<&Path>,
        options: SearchClientOptions,
    ) -> Result<Option<Self>> {
        let crate::search::tantivy::OpenedLexicalIndex {
            path: index_path,
            reader: tantivy,
            federated_readers,
        } = index;
        let federated_readers =
            federated_readers
                .filter(|readers| !readers.is_empty())
                .map(|readers| {
                    Arc::new(
                        readers
                            .into_iter()
                            .map(|(reader, fields)| FederatedIndexReader { reader, fields })
                            .collect::<Vec<_>>(),
                    )
                });
        let client_id = SEARCH_CLIENT_INSTANCE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let cache_namespace = format!(
            "v{}|schema:{}|client:{}|index:{}",
            CACHE_KEY_VERSION,
            FS_CASS_SCHEMA_HASH,
            client_id,
            index_path.display()
        );

        let sqlite_path = db_path.map(Path::to_path_buf).filter(|path| path.exists());

        if tantivy.is_none() && federated_readers.is_none() && sqlite_path.is_none() {
            return Ok(None);
        }

        let reload_epoch = Arc::new(AtomicU64::new(0));
        let metrics = Metrics::default();

        let warm_pair = if options.enable_warm
            && let Some((reader, fields)) = &tantivy
        {
            maybe_spawn_warm_worker(
                reader.clone(),
                *fields,
                reload_epoch.clone(),
                metrics.clone(),
            )
        } else {
            None
        };

        if let Some(readers) = &federated_readers {
            FEDERATED_SEARCH_READERS
                .write()
                .insert(cache_namespace.clone(), Arc::clone(readers));
        } else {
            FEDERATED_SEARCH_READERS.write().remove(&cache_namespace);
        }

        Ok(Some(Self {
            reader: LexicalReaderSlot::new(index_path, tantivy),
            sqlite: Mutex::new(None),
            sqlite_path,
            strict_read_only: options.strict_read_only,
            prefix_cache: Mutex::new(CacheShards::new(*CACHE_TOTAL_CAP, *CACHE_BYTE_CAP)),
            reload_on_search: options.enable_reload,
            last_reload: Mutex::new(None),
            last_generation: Mutex::new(None),
            reload_epoch,
            warm_tx: warm_pair.as_ref().map(|(tx, _)| tx.clone()),
            _warm_handle: warm_pair.map(|(_, h)| h),
            metrics,
            cache_namespace,
            semantic: Mutex::new(None),
            last_tantivy_total_count: Mutex::new(None),
            last_lexical_degrade_reason: Mutex::new(None),
            last_wildcard_fallback_skip: Mutex::new(None),
        }))
    }

    pub(in crate::search::query) fn sqlite_guard(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, Option<SearchSqliteConnection>>> {
        let mut guard = self
            .sqlite
            .lock()
            .map_err(|_| anyhow!("sqlite lock poisoned"))?;

        if guard.is_none()
            && let Some(path) = &self.sqlite_path
        {
            let conn = open_search_hydration_sqlite(
                path,
                std::time::Duration::from_secs(1),
                self.strict_read_only,
            )
            .map_err(|error| {
                // A configured archive that failed to open is not an absent
                // optional backend. Keep the storage cause in the displayed
                // message as well as the error chain: CLI callers generally
                // render only the outermost error.
                let context = format!(
                    "opening configured search archive {} failed: {error}",
                    path.display()
                );
                error.context(context)
            })?;
            *guard = Some(conn);
        }

        Ok(guard)
    }

    pub fn search(
        &self,
        query: &str,
        filters: SearchFilters,
        limit: usize,
        offset: usize,
        field_mask: FieldMask,
    ) -> Result<Vec<SearchHit>> {
        // A query nested deeper than MAX_QUERY_NESTING_DEPTH is refused
        // before any lane runs: the recursive boolean stages behind this
        // entry (and the renderers and AST teardown downstream of them)
        // must never see it, and the caller gets the limit in a typed
        // error rather than a lane-specific degradation.
        ensure_query_nesting_depth(query)?;
        // This is per-search diagnostics, never a sticky property of a reader.
        self.record_lexical_degrade_reason(None);
        // NFC-normalize early so every downstream consumer (Tantivy query
        // builder, sanitizer, FTS5 fallback) sees consistent Unicode form
        // matching the NFC-indexed content.
        use unicode_normalization::UnicodeNormalization;
        let query: String = query.nfc().collect();
        let query: &str = &query;
        let sanitized = nfc_sanitize_query(query);
        let field_mask = effective_field_mask(field_mask);
        // Only lexical readers provide a publication generation with which to
        // invalidate results. A SQLite-only client can observe new canonical
        // rows between calls and must query its archive again.
        let can_use_cache = self.has_tantivy()
            && field_mask.allows_cache()
            && (field_mask.needs_content() || field_mask.wants_snippet());

        // Admit the current generation before using either its cached results
        // or its document count to size this request.
        self.refresh_lexical_generation()?;

        // Resolve an unlimited request against the generation just admitted,
        // not the older reader's count from before the reload. SQLite-only
        // clients have no lexical document count, so retain the shared result
        // cap instead of mistaking an unavailable count for a one-hit archive.
        let limit = if limit == 0 {
            let cap = no_limit_result_cap();
            if self.has_tantivy() {
                self.total_docs().min(cap).max(1)
            } else {
                cap
            }
        } else {
            limit
        };

        // A prior query's top-k is not the top-k of an extended query, even
        // when every cached hit still matches: scores and omitted competitors
        // can change. Reuse only the exact NFC query and field projection.
        // Prefixes remain useful solely as hints for posting-list prewarming.
        let cache_epoch = self.reload_epoch.load(Ordering::SeqCst);
        if can_use_cache
            && offset == 0
            && !query.contains('*')
            && !fs_cass_has_boolean_operators(query)
        {
            self.maybe_schedule_adaptive_query_prewarm(query, &filters, field_mask);
            if let Some(cached) = self.cached_query_hits(query, &filters, field_mask) {
                if cached.len() >= limit {
                    self.metrics.inc_cache_hits();
                    self.maybe_log_cache_metrics("hit");
                    if let Ok(mut tc) = self.last_tantivy_total_count.lock() {
                        *tc = None;
                    }
                    return Ok(cached
                        .into_iter()
                        .take(limit)
                        .map(|cached| cached.hit)
                        .collect());
                }
                // Cache had entries but not enough to satisfy limit - shortfall, not miss
                self.metrics.inc_cache_shortfall();
                self.maybe_log_cache_metrics("shortfall");
            } else {
                // No result for this exact query and projection.
                self.metrics.inc_cache_miss();
                self.maybe_log_cache_metrics("miss");
            }
        }

        // Adaptive fetch sizing: start at 2x target to reduce common-case work,
        // retry at 3x only when deduplication causes shortfall.
        // We always fetch from 0 to preserve global deduplication correctness.
        let target_hits = offset.saturating_add(limit);
        let initial_fetch_limit = if target_hits <= 16 {
            target_hits.saturating_mul(2)
        } else {
            // Larger pages benefit from a lower first-pass over-fetch.
            // Retry logic below preserves correctness on duplicate-heavy corpora.
            target_hits.saturating_mul(3).div_ceil(2)
        };
        // Filters applied after hydration (not in the engine clause) can
        // discard most of a page, so their retry fetch must reach deep.
        let session_path_filter_active = !filters.session_paths.is_empty()
            || remote_source_filter_is_post_applied(&filters.source_filter);
        let fallback_fetch_limit = if session_path_filter_active {
            self.total_docs()
                .min(no_limit_result_cap())
                .max(target_hits.saturating_mul(3))
                .max(1)
        } else {
            target_hits.saturating_mul(3)
        };

        // Quill is the primary high-performance engine.
        if let Some((reader, fields)) = self.reader.get() {
            tracing::info!(
                backend = "quill",
                query = sanitized,
                limit = initial_fetch_limit,
                offset = 0,
                "search_start"
            );
            let (hits, tantivy_total_count) = self.search_tantivy(
                &reader,
                &fields,
                query,
                &sanitized,
                filters.clone(),
                initial_fetch_limit,
                0, // Always fetch from 0 for global dedup
                field_mask,
            )?;
            if let Ok(mut tc) = self.last_tantivy_total_count.lock() {
                *tc = tantivy_total_count;
            }
            if !hits.is_empty() {
                let initial_hit_count = hits.len();
                let page_hits = |raw_hits: Vec<SearchHit>| {
                    self.postprocess_hits_page(raw_hits, &sanitized, &filters, limit, offset)
                };

                let (mut deduped_len, mut paged_hits) = page_hits(hits);

                let needs_retry = deduped_len < target_hits
                    && initial_hit_count == initial_fetch_limit
                    && initial_fetch_limit < fallback_fetch_limit;
                let mut retry_budget_exhausted = false;

                if needs_retry {
                    tracing::debug!(
                        query = sanitized,
                        target_hits,
                        deduped_len,
                        initial_fetch_limit,
                        fallback_fetch_limit,
                        session_path_filter_active,
                        "retrying lexical fetch due to dedup or session-path shortfall"
                    );
                    let attempt = self.search_tantivy(
                        &reader,
                        &fields,
                        query,
                        &sanitized,
                        filters.clone(),
                        fallback_fetch_limit,
                        0,
                        field_mask,
                    );
                    match self.settle_lexical_retry(attempt, paged_hits.len())? {
                        Some((retry_hits, retry_total_count)) => {
                            if let Ok(mut tc) = self.last_tantivy_total_count.lock() {
                                *tc = retry_total_count;
                            }
                            if !retry_hits.is_empty() {
                                (deduped_len, paged_hits) = page_hits(retry_hits);
                            }
                        }
                        None => retry_budget_exhausted = true,
                    }
                }

                tracing::trace!(
                    query = sanitized,
                    target_hits,
                    deduped_len,
                    returned = paged_hits.len(),
                    "lexical fetch complete"
                );

                // Never promote a budget-shortened page into a complete cache hit.
                if can_use_cache && offset == 0 && !retry_budget_exhausted {
                    self.put_cache(query, &filters, field_mask, cache_epoch, &paged_hits);
                }
                return Ok(paged_hits);
            }
            tracing::debug!(
                query = sanitized,
                "quill returned zero hits; skipping sqlite fallback because quill is authoritative when available"
            );
            return Ok(Vec::new());
        } else if let Some(readers) = self.federated_readers() {
            tracing::info!(
                backend = "quill-federated",
                query = sanitized,
                limit = initial_fetch_limit,
                offset = 0,
                shards = readers.len(),
                "search_start"
            );
            let (hits, tantivy_total_count) = self.search_tantivy_federated(
                readers.as_ref(),
                query,
                &sanitized,
                filters.clone(),
                initial_fetch_limit,
                field_mask,
            )?;
            if let Ok(mut tc) = self.last_tantivy_total_count.lock() {
                *tc = tantivy_total_count;
            }
            if !hits.is_empty() {
                let initial_hit_count = hits.len();
                let page_hits = |raw_hits: Vec<SearchHit>| {
                    self.postprocess_hits_page(raw_hits, &sanitized, &filters, limit, offset)
                };

                let (mut deduped_len, mut paged_hits) = page_hits(hits);
                let expected_federated_capacity = initial_fetch_limit.saturating_mul(readers.len());
                let federated_initial_capacity_reached = if session_path_filter_active {
                    initial_hit_count >= initial_fetch_limit.min(expected_federated_capacity)
                } else {
                    initial_hit_count == expected_federated_capacity
                };
                let needs_retry = deduped_len < target_hits
                    && federated_initial_capacity_reached
                    && initial_fetch_limit < fallback_fetch_limit;
                let mut retry_budget_exhausted = false;

                if needs_retry {
                    tracing::debug!(
                        query = sanitized,
                        target_hits,
                        deduped_len,
                        initial_fetch_limit,
                        fallback_fetch_limit,
                        shards = readers.len(),
                        session_path_filter_active,
                        "retrying federated lexical fetch due to dedup or session-path shortfall"
                    );
                    let attempt = self.search_tantivy_federated(
                        readers.as_ref(),
                        query,
                        &sanitized,
                        filters.clone(),
                        fallback_fetch_limit,
                        field_mask,
                    );
                    match self.settle_lexical_retry(attempt, paged_hits.len())? {
                        Some((retry_hits, retry_total_count)) => {
                            if let Ok(mut tc) = self.last_tantivy_total_count.lock() {
                                *tc = retry_total_count;
                            }
                            if !retry_hits.is_empty() {
                                (deduped_len, paged_hits) = page_hits(retry_hits);
                            }
                        }
                        None => retry_budget_exhausted = true,
                    }
                }

                tracing::trace!(
                    query = sanitized,
                    target_hits,
                    deduped_len,
                    returned = paged_hits.len(),
                    shards = readers.len(),
                    "federated lexical fetch complete"
                );

                if can_use_cache && offset == 0 && !retry_budget_exhausted {
                    self.put_cache(query, &filters, field_mask, cache_epoch, &paged_hits);
                }
                return Ok(paged_hits);
            }
            tracing::debug!(
                query = sanitized,
                shards = readers.len(),
                "federated quill returned zero hits; skipping sqlite fallback because quill is authoritative when available"
            );
            return Ok(Vec::new());
        }

        // 1t79z: leading/internal wildcards ("*handler", "f*o") are not
        // representable in FTS5, but the primary index supports them, so they
        // must not become a false-empty result here. `transpile_to_fts5`
        // returns None for them and the SQLite lane routes such queries to the
        // bounded source-table scan, which honours the patterns on token
        // boundaries.

        let has_sqlite_backend = {
            let sqlite_guard = self
                .sqlite
                .lock()
                .map_err(|_| anyhow!("sqlite lock poisoned"))?;
            sqlite_guard.is_some() || self.sqlite_path.is_some()
        };

        if has_sqlite_backend {
            tracing::info!(
                // This lane may use FTS5 or a source-table scan.
                backend = "frankensqlite",
                query = sanitized,
                limit = fallback_fetch_limit,
                offset = 0,
                "search_start"
            );
            let hits = self.search_sqlite_fts5(
                self.sqlite_path
                    .as_deref()
                    .unwrap_or_else(|| Path::new(":memory:")),
                query,
                filters.clone(),
                fallback_fetch_limit,
                0, // Always fetch from 0 for global dedup
                field_mask,
            )?;
            let (_, paged_hits) =
                self.postprocess_hits_page(hits, &sanitized, &filters, limit, offset);

            if can_use_cache && offset == 0 {
                self.put_cache(query, &filters, field_mask, cache_epoch, &paged_hits);
            }
            return Ok(paged_hits);
        }

        tracing::info!(backend = "none", query = query, "search_start");
        Ok(Vec::new())
    }
}

impl SearchClient {
    pub(in crate::search::query) fn postprocess_hits_page(
        &self,
        mut hits: Vec<SearchHit>,
        query: &str,
        filters: &SearchFilters,
        limit: usize,
        offset: usize,
    ) -> (usize, Vec<SearchHit>) {
        if remote_source_filter_is_post_applied(&filters.source_filter) {
            hits.retain(|hit| hit.origin_kind != crate::sources::provenance::LOCAL_SOURCE_ID);
        }
        postprocess_hits_page_core(hits, query, &filters.session_paths, limit, offset)
    }

    /// Search with automatic wildcard fallback for sparse results.
    /// If the initial search returns fewer than `sparse_threshold` results and the query
    /// doesn't already contain wildcards, automatically retry with substring wildcards (*term*).
    pub fn search_with_fallback(
        &self,
        query: &str,
        filters: SearchFilters,
        limit: usize,
        offset: usize,
        sparse_threshold: usize,
        field_mask: FieldMask,
    ) -> Result<SearchResult> {
        self.record_wildcard_fallback_skip(None);
        // First, try the normal search
        let hits = self.search(query, filters.clone(), limit, offset, field_mask)?;
        let baseline_stats = self.cache_stats();
        // Capture the exact Tantivy total when the query path could collect it cheaply.
        let tantivy_total = self
            .last_tantivy_total_count
            .lock()
            .ok()
            .and_then(|guard| *guard);

        if self.lexical_degrade_reason() == Some(LEXICAL_RETRY_QUERY_FUEL_EXHAUSTED) {
            // A larger native fetch already spent its budget. Do not turn its
            // retained short page into another automatic dictionary expansion.
            self.record_wildcard_fallback_skip(Some(
                WILDCARD_FALLBACK_SKIPPED_QUERY_FUEL_EXHAUSTED,
            ));
            return Ok(SearchResult {
                hits,
                wildcard_fallback: false,
                cache_stats: baseline_stats,
                suggestions: Vec::new(),
                ann_stats: None,
                ann_unavailable_reason: None,
                total_count: None,
            });
        }

        // Check if we should try wildcard fallback
        let query_has_wildcards = query.contains('*');
        let has_boolean_or_phrase = fs_cass_has_boolean_operators(query);
        let is_sparse = should_try_wildcard_fallback(hits.len(), limit, offset, sparse_threshold);
        let total_docs = self.total_docs();
        let automatic_wildcard_allowed = should_allow_automatic_wildcard_fallback(
            total_docs,
            automatic_wildcard_fallback_max_docs(),
        );

        if !is_sparse
            || query_has_wildcards
            || has_boolean_or_phrase
            || query.trim().is_empty()
            || !automatic_wildcard_allowed
        {
            // Either we have enough results, query already has wildcards,
            // query uses boolean/phrases, or query is empty.
            if is_sparse
                && !automatic_wildcard_allowed
                && !query_has_wildcards
                && !has_boolean_or_phrase
                && !query.trim().is_empty()
            {
                tracing::debug!(
                    query,
                    returned_hits = hits.len(),
                    total_docs,
                    automatic_wildcard_max_docs = automatic_wildcard_fallback_max_docs(),
                    "skipping automatic wildcard fallback on large index"
                );
                self.record_wildcard_fallback_skip(Some(
                    if automatic_wildcard_fallback_max_docs() == 0 {
                        WILDCARD_FALLBACK_SKIPPED_DISABLED
                    } else {
                        WILDCARD_FALLBACK_SKIPPED_LARGE_INDEX
                    },
                ));
            }
            // Generate suggestions only if truly zero hits
            let suggestions = if hits.is_empty() && !query.trim().is_empty() {
                self.generate_suggestions(query, &filters)
            } else {
                Vec::new()
            };
            return Ok(SearchResult {
                hits,
                wildcard_fallback: false,
                cache_stats: baseline_stats,
                suggestions,
                ann_stats: None,
                ann_unavailable_reason: None,
                total_count: tantivy_total,
            });
        }

        if should_skip_automatic_wildcard_fallback_for_long_zero_hit_query(query, hits.len()) {
            self.record_wildcard_fallback_skip(Some(WILDCARD_FALLBACK_SKIPPED_LONG_TERM));
            let suggestions = if hits.is_empty() {
                self.generate_suggestions(query, &filters)
            } else {
                Vec::new()
            };
            return Ok(SearchResult {
                hits,
                wildcard_fallback: false,
                cache_stats: baseline_stats,
                suggestions,
                ann_stats: None,
                ann_unavailable_reason: None,
                total_count: tantivy_total,
            });
        }

        // Try wildcard fallback: wrap each term in *term*
        let wildcard_query = query
            .split_whitespace()
            .map(|term| format!("*{}*", term.trim_matches('*')))
            .collect::<Vec<_>>()
            .join(" ");

        tracing::info!(
            original_query = query,
            wildcard_query = wildcard_query,
            original_count = hits.len(),
            "wildcard_fallback"
        );

        let mut fallback_hits =
            match self.search(&wildcard_query, filters.clone(), limit, offset, field_mask) {
                Ok(fallback_hits) => fallback_hits,
                // The retry only refines a search that already succeeded. A
                // `*term*` dictionary walk can exhaust the query fuel the plain
                // query fit in (GH #518); that refusal must not discard the
                // plain query's hits (coding_agent_session_search-7hvlg).
                // Other failures still fail the search.
                Err(error) if crate::search::quill_bridge::is_query_fuel_exhausted(&error) => {
                    tracing::warn!(
                        original_query = query,
                        wildcard_query,
                        original_count = hits.len(),
                        error = %format_args!("{error:#}"),
                        "wildcard_fallback ran out of query fuel; returning the original results"
                    );
                    self.record_wildcard_fallback_skip(Some(
                        WILDCARD_FALLBACK_SKIPPED_QUERY_FUEL_EXHAUSTED,
                    ));
                    // The original page, not the unsuccessful wildcard request,
                    // owns the diagnostics exposed with this response.
                    self.record_lexical_degrade_reason(None);
                    if let Ok(mut total) = self.last_tantivy_total_count.lock() {
                        *total = tantivy_total;
                    }
                    let suggestions = if hits.is_empty() {
                        self.generate_suggestions(query, &filters)
                    } else {
                        Vec::new()
                    };
                    return Ok(SearchResult {
                        hits,
                        wildcard_fallback: false,
                        cache_stats: baseline_stats,
                        suggestions,
                        ann_stats: None,
                        ann_unavailable_reason: None,
                        total_count: tantivy_total,
                    });
                }
                Err(error) => return Err(error),
            };
        let fallback_stats = self.cache_stats();
        // Re-capture total_count after wildcard search (may have changed)
        let fallback_tantivy_total = self
            .last_tantivy_total_count
            .lock()
            .ok()
            .and_then(|guard| *guard);

        // Use fallback results if they're better
        if fallback_hits.len() > hits.len() {
            // Mark all hits as ImplicitWildcard since we auto-added wildcards
            for hit in &mut fallback_hits {
                hit.match_type = MatchType::ImplicitWildcard;
            }
            // Generate suggestions if still zero hits after fallback
            let suggestions = if fallback_hits.is_empty() {
                self.generate_suggestions(query, &filters)
            } else {
                Vec::new()
            };
            Ok(SearchResult {
                hits: fallback_hits,
                wildcard_fallback: true,
                cache_stats: fallback_stats,
                suggestions,
                ann_stats: None,
                ann_unavailable_reason: None,
                total_count: fallback_tantivy_total,
            })
        } else {
            // Keep original results even if sparse. A shorter wildcard page
            // must not leave its retry-refusal metadata on the selected page.
            self.record_lexical_degrade_reason(None);
            if let Ok(mut total) = self.last_tantivy_total_count.lock() {
                *total = tantivy_total;
            }
            // Generate suggestions if zero hits
            let suggestions = if hits.is_empty() {
                self.generate_suggestions(query, &filters)
            } else {
                Vec::new()
            };
            Ok(SearchResult {
                hits,
                wildcard_fallback: false,
                cache_stats: baseline_stats,
                suggestions,
                ann_stats: None,
                ann_unavailable_reason: None,
                total_count: tantivy_total,
            })
        }
    }

    /// Hybrid search that fuses lexical + semantic results with RRF.
    #[allow(clippy::too_many_arguments)]
    pub fn search_hybrid(
        &self,
        lexical_query: &str,
        semantic_query: &str,
        filters: SearchFilters,
        limit: usize,
        offset: usize,
        sparse_threshold: usize,
        field_mask: FieldMask,
        approximate: bool,
    ) -> Result<SearchResult> {
        self.search_hybrid_with_tier(
            lexical_query,
            semantic_query,
            filters,
            limit,
            offset,
            sparse_threshold,
            field_mask,
            approximate,
            SemanticTierMode::Single,
        )
    }

    /// Hybrid search that fuses lexical + semantic results with optional
    /// progressive two-tier semantic execution.
    #[allow(clippy::too_many_arguments)]
    pub fn search_hybrid_with_tier(
        &self,
        lexical_query: &str,
        semantic_query: &str,
        filters: SearchFilters,
        limit: usize,
        offset: usize,
        sparse_threshold: usize,
        field_mask: FieldMask,
        approximate: bool,
        semantic_tier_mode: SemanticTierMode,
    ) -> Result<SearchResult> {
        // The lexical leg parses the boolean grammar (its own entry gates
        // too, via `search_with_fallback` → `search`); refusing here keeps
        // the hybrid verdict typed and first, before any generation
        // refresh or candidate budgeting runs.
        ensure_query_nesting_depth(lexical_query)?;
        if semantic_query.trim().is_empty() {
            return self.search_with_fallback(
                lexical_query,
                filters,
                limit,
                offset,
                sparse_threshold,
                field_mask,
            );
        }

        self.refresh_lexical_generation()?;
        let requested_limit = limit;
        // A missing lexical reader says nothing about the size of the
        // canonical or semantic archive. Keep both candidate legs bounded by
        // the shared cap instead of silently restricting each to one hit.
        let total_docs = if self.has_tantivy() {
            self.total_docs().max(1)
        } else {
            no_limit_result_cap()
        };
        let limit = if requested_limit == 0 {
            total_docs.min(no_limit_result_cap()).max(1)
        } else {
            requested_limit
        };
        let fetch = limit.saturating_add(offset);
        if fetch == 0 {
            return Ok(SearchResult {
                hits: Vec::new(),
                wildcard_fallback: false,
                cache_stats: self.cache_stats(),
                suggestions: Vec::new(),
                ann_stats: None,
                ann_unavailable_reason: None,
                total_count: None,
            });
        }

        let budget =
            hybrid_candidate_budget(semantic_query, requested_limit, limit, offset, total_docs);
        self.record_lexical_degrade_reason(None);
        let lexical = match self.search_with_fallback(
            lexical_query,
            filters.clone(),
            budget.lexical_candidates,
            0,
            sparse_threshold,
            field_mask,
        ) {
            Ok(lexical) => lexical,
            // GH #441: Quill's per-query fuel ceiling is a work bound, not an
            // index fault. A stopword-heavy natural-language query on a
            // segment-heavy archive can hit it while the semantic leg is
            // perfectly able to answer. Degrade to semantic-only and say so
            // in the robot metadata instead of failing the whole search.
            Err(err) if crate::search::quill_bridge::is_query_fuel_exhausted(&err) => {
                tracing::warn!(
                    error = %err,
                    "lexical leg of hybrid search exhausted its Quill query fuel; \
                     continuing with the semantic leg only (GH #441)"
                );
                self.record_lexical_degrade_reason(Some(LEXICAL_DEGRADE_QUERY_FUEL_EXHAUSTED));
                SearchResult {
                    hits: Vec::new(),
                    wildcard_fallback: false,
                    cache_stats: self.cache_stats(),
                    suggestions: Vec::new(),
                    ann_stats: None,
                    ann_unavailable_reason: None,
                    total_count: None,
                }
            }
            Err(err) => return Err(err),
        };
        let session_scoped = !filters.session_paths.is_empty();
        let (semantic_hits, semantic_ann_stats) = self.search_semantic_with_tier(
            semantic_query,
            filters,
            budget.semantic_candidates,
            0,
            field_mask,
            approximate,
            semantic_tier_mode,
        )?;
        let semantic_ann_unavailable_reason = if approximate && session_scoped {
            Some(SemanticAnnUnavailableReason::SessionScopeRequiresExact)
        } else if approximate {
            self.ann_unavailability_reason()?
        } else {
            None
        };
        let fused = rrf_fuse_hits(&lexical.hits, &semantic_hits, semantic_query, limit, offset);
        let suggestions = if fused.is_empty() {
            lexical.suggestions.clone()
        } else {
            Vec::new()
        };
        Ok(SearchResult {
            hits: fused,
            wildcard_fallback: lexical.wildcard_fallback,
            cache_stats: lexical.cache_stats,
            suggestions,
            ann_stats: semantic_ann_stats,
            ann_unavailable_reason: semantic_ann_unavailable_reason,
            total_count: None,
        })
    }

    /// Generate "did-you-mean" suggestions for zero-hit queries.
    pub(in crate::search::query) fn generate_suggestions(
        &self,
        query: &str,
        filters: &SearchFilters,
    ) -> Vec<QuerySuggestion> {
        let mut suggestions = Vec::new();
        let query_lower = query.to_lowercase();

        // 1. Suggest wildcard search if query doesn't have wildcards
        if !query.contains('*') && query.len() >= 2 {
            suggestions.push(QuerySuggestion::wildcard(query).with_shortcut(1));
        }

        // 2. Suggest removing agent filter if one is set
        if !filters.agents.is_empty() {
            let agents: Vec<&str> = filters
                .agents
                .iter()
                .map(std::string::String::as_str)
                .collect();
            let agent_str = agents.join(", ");
            suggestions
                .push(QuerySuggestion::remove_agent_filter(&agent_str, filters).with_shortcut(2));
        }

        // 3. Suggest common agent names if query looks like a typo of one
        let known_agents = [
            "codex",
            "claude",
            "claude_code",
            "cline",
            "gemini",
            "amp",
            "opencode",
        ];
        for agent in &known_agents {
            if levenshtein_distance(&query_lower, agent) <= 2 && query_lower != *agent {
                suggestions.push(
                    QuerySuggestion::spelling(query, agent)
                        .with_shortcut(suggestions.len().min(2) as u8 + 1),
                );
                break; // Only suggest one spelling fix
            }
        }

        // 4. Suggest alternative agents if SQLite is already open and no agent
        // filter is set. Avoid lazy-opening storage solely for no-hit advice:
        // large read-only frankensqlite opens can dominate fast lexical misses.
        if filters.agents.is_empty()
            && let Ok(sqlite_guard) = self.sqlite.lock()
            && let Some(conn) = sqlite_guard.as_ref()
            && let Ok(rows) = conn.query_map_collect(
                "SELECT a.slug
                 FROM conversations c
                 JOIN agents a ON c.agent_id = a.id
                 GROUP BY a.slug
                 ORDER BY MAX(c.id) DESC
                 LIMIT 3",
                &[],
                |row: &crate::franken_sync::Row| row.get_typed::<String>(0),
            )
        {
            for row in rows {
                if suggestions.len() < 3 {
                    suggestions.push(
                        QuerySuggestion::try_agent(&row)
                            .with_shortcut(suggestions.len().min(2) as u8 + 1),
                    );
                }
            }
        }

        // Ensure we have at most 3 suggestions with shortcuts 1, 2, 3
        suggestions.truncate(3);
        for (i, sugg) in suggestions.iter_mut().enumerate() {
            sugg.shortcut = Some((i + 1) as u8);
        }

        suggestions
    }
}
