#[cfg(test)]
pub(in crate::search::query) use anyhow::Context;
pub(in crate::search::query) use anyhow::{Result, anyhow, bail};
pub(in crate::search::query) use crossbeam_channel as mpsc;
pub(in crate::search::query) use frankensearch::core::filter::SearchFilter as FsSearchFilter;
pub(in crate::search::query) use frankensearch::index::{
    HNSW_DEFAULT_EF_SEARCH as FS_HNSW_DEFAULT_EF_SEARCH, HnswIndex as FsHnswIndex,
    VectorIndex as FsVectorIndex,
};
#[cfg(test)]
pub(in crate::search::query) use frankensearch::lexical_tantivy::{
    CassQueryToken as FsCassQueryToken, CassWildcardPattern as FsCassWildcardPattern,
    cass_has_boolean_operators as fs_cass_has_boolean_operators,
    cass_parse_boolean_query as fs_cass_parse_boolean_query,
    cass_sanitize_query as fs_cass_sanitize_query,
};
#[cfg(test)]
pub(in crate::search::query) use frankensearch::quill::query::CassQueryFilters as FsCassQueryFilters;
pub(in crate::search::query) use frankensqlite::AsyncConnection as SearchSqliteConnection;
pub(in crate::search::query) use std::collections::{HashMap, HashSet};
#[cfg(test)]
pub(in crate::search::query) use std::path::Path;
pub(in crate::search::query) use std::path::PathBuf;
pub(in crate::search::query) use std::sync::atomic::AtomicU64;
#[cfg(test)]
pub(in crate::search::query) use std::sync::atomic::Ordering;
pub(in crate::search::query) use std::sync::{Arc, Mutex};
#[cfg(test)]
pub(in crate::search::query) use std::time::Duration;
pub(in crate::search::query) use std::time::Instant;

pub(in crate::search::query) use crate::franken_sync::compat::{ParamValue, RowExt};
#[cfg(test)]
pub(in crate::search::query) use crate::franken_sync::params;
#[cfg(test)]
pub(in crate::search::query) use crate::search::canonicalize::content_hash;
pub(in crate::search::query) use crate::search::vector_index::{
    SemanticAnnUnavailableReason, SemanticFilter, SemanticIndexArtifact, VectorSearchResult,
    parse_semantic_doc_id,
};
#[cfg(test)]
pub(in crate::search::query) use crate::search::vector_index::{
    SemanticDocId, SemanticFilterMaps, SemanticProgressiveUnavailableReason,
};
#[cfg(test)]
pub(in crate::search::query) use crate::sources::provenance::SourceFilter;

#[path = "query/retry_budget.rs"]
mod retry_budget;
pub use retry_budget::LEXICAL_RETRY_QUERY_FUEL_EXHAUSTED;

mod ann_shards;
#[cfg(test)]
mod ann_shards_integration;
mod message_stream;
mod message_topk;
#[cfg(test)]
mod message_topk_integration;
mod session_scope;

mod cache;
mod dispatch;
mod fusion;
mod lexical;
mod parsing;
mod progressive;
mod semantic;
mod sqlite_fts;
mod sqlite_hydration;
mod sqlite_scan;
mod types;

pub use cache::CacheStats;
#[cfg(test)]
pub(crate) use fusion::deduplicate_hits;
pub use fusion::rrf_fuse_hits;
#[cfg(test)]
pub(crate) use fusion::{
    deduplicate_hits_with_query, is_tool_invocation_noise, stable_content_hash,
};
#[doc(hidden)]
pub use parsing::fuzz_transpile_to_fts5;
pub use parsing::{
    FiltersSummary, IndexStrategy, MAX_QUERY_NESTING_DEPTH, ParsedQuery, ParsedSubTerm, ParsedTerm,
    QueryCost, QueryDepthExceeded, QueryExplanation, QueryReading, QuerySuggestion, QueryType,
    SuggestionKind, ensure_query_nesting_depth, read_query,
};
pub use sqlite_hydration::{sql_placeholders, sql_rowid_literal_list};
#[cfg(test)]
pub(crate) use types::ProgressiveSearchRequest;
#[cfg(any(test, feature = "cass-layer-b-conformance"))]
pub use types::{
    CASS_LEXICAL_LAYER_B_CANDIDATE_MAX, CassLexicalLayerBCandidate, CassLexicalLayerBError,
    CassLexicalLayerBHit, CassLexicalLayerBNoClaim, CassLexicalLayerBPage,
    CassLexicalLayerBRequest,
};
pub use types::{
    FieldMask, MatchType, NO_LIMIT_RESULT_MAX, NO_LIMIT_RESULT_MIN, ProgressivePhaseKind,
    ProgressiveSearchEvent, SearchFilters, SearchHit, SearchMode, SearchResult, SemanticTierMode,
    StringInterner,
};

// Internal re-exports consumed by existing `src/search/query/*.rs` siblings (`ann_shards*`,
// `message_stream*`, `message_topk*`, `session_scope`, `retry_budget`).
pub(in crate::search::query) use cache::{
    CacheShards, FEDERATED_SEARCH_READERS, LexicalReaderSlot, Metrics, WarmJob,
};
pub(in crate::search::query) use semantic::{
    SemanticAnnOpenFailure, SemanticCandidateContext, SemanticCandidateRetryState,
    SemanticSearchState, open_fs_semantic_ann_index,
};
pub(in crate::search::query) use sqlite_hydration::{
    SearchReadTransaction, semantic_message_id_from_db,
};
pub(in crate::search::query) use types::ANN_CANDIDATE_MULTIPLIER;

#[cfg(test)]
pub(in crate::search::query) use cache::{
    CACHE_BYTE_CAP, CACHE_KEY_VERSION, CACHE_TOTAL_CAP, CacheEvictionPolicy,
    DEFAULT_CACHE_BYTE_CAP_CEILING, DEFAULT_CACHE_BYTE_CAP_FALLBACK, QueryCache,
    S3_FIFO_LARGE_ENTRY_FRACTION_DENOMINATOR, SEARCH_CLIENT_INSTANCE_COUNTER,
    cache_byte_cap_from_env_value, cache_eviction_policy_from_env_value, cached_hit_from,
    default_cache_byte_cap_for_available, filters_fingerprint, run_reload_bounded,
};
#[cfg(test)]
pub(in crate::search::query) use dispatch::levenshtein_distance;
#[cfg(test)]
pub(in crate::search::query) use fusion::{
    FederatedRankedHit, FusedHit, HybridScore, SearchHitKey, cmp_fused_hit_desc,
    federated_rrf_score, hit_is_noise, merge_federated_ranked_hits, search_hit_key_doc_id,
    should_try_wildcard_fallback, snippet_from_content, snippet_from_preview_without_full_content,
    snippet_terms_for_query, stable_hit_hash, top_k_fused,
};
#[cfg(test)]
pub(in crate::search::query) use parsing::{
    CassBoolToken, cass_bool_tokens, dominant_match_type, normalize_term_parts, transpile_to_fts5,
};
#[cfg(test)]
pub(in crate::search::query) use progressive::{
    ProgressiveLexicalCache, ProgressiveLexicalHit, ProgressivePhaseContext,
};
#[cfg(test)]
pub(in crate::search::query) use semantic::{
    SemanticCandidateSearchRequest, semantic_filter_as_search_filter,
};
#[cfg(test)]
pub(in crate::search::query) use session_scope::SessionScopedSemanticFilter;
#[cfg(test)]
pub(in crate::search::query) use sqlite_fts::SqliteFtsMatchMode;
#[cfg(test)]
pub(in crate::search::query) use sqlite_hydration::{
    SEARCH_SQLITE_HYDRATION_CACHE_KIB, SQLITE_FTS5_HYDRATE_PARAM_CHUNK, SQLITE_MAX_VARIABLE_NUMBER,
    SearchSqliteFixture, franken_query_map_collect_retry, hydrate_message_content_by_conversation,
    nfc_sanitize_query, open_search_hydration_sqlite, semantic_doc_component_id_from_db,
};
#[cfg(test)]
pub(in crate::search::query) use sqlite_scan::{
    SqliteMessageScanQuery, SqliteMessageScanRequest, glob_token_matches,
};
#[cfg(test)]
pub(in crate::search::query) use types::{
    AVG_HIT_BYTES, HYBRID_NO_LIMIT_SEMANTIC_CAP, NO_LIMIT_BYTES_CEILING, NO_LIMIT_BYTES_FLOOR,
    bounded_exact_count_page, compute_no_limit_result_cap_from,
    execute_query_with_bounded_exact_count, hybrid_candidate_budget, no_limit_budget_bytes,
    no_limit_result_cap, should_allow_automatic_wildcard_fallback,
    should_collect_exact_total_count,
};

pub struct SearchClient {
    pub(in crate::search::query) reader: LexicalReaderSlot,
    pub(in crate::search::query) sqlite: Mutex<Option<SearchSqliteConnection>>,
    pub(in crate::search::query) sqlite_path: Option<PathBuf>,
    pub(in crate::search::query) strict_read_only: bool,
    pub(in crate::search::query) prefix_cache: Mutex<CacheShards>,
    pub(in crate::search::query) reload_on_search: bool,
    pub(in crate::search::query) last_reload: Mutex<Option<Instant>>,
    pub(in crate::search::query) last_generation: Mutex<Option<u64>>,
    pub(in crate::search::query) reload_epoch: Arc<AtomicU64>,
    pub(in crate::search::query) warm_tx: Option<mpsc::Sender<WarmJob>>,
    pub(in crate::search::query) _warm_handle: Option<std::thread::JoinHandle<()>>,
    pub(in crate::search::query) metrics: Metrics,
    pub(in crate::search::query) cache_namespace: String,
    pub(in crate::search::query) semantic: Mutex<Option<SemanticSearchState>>,
    /// Exact total from the most recent Tantivy query when collecting it was
    /// cheap enough. Large saturated pages leave this as `None` so robot output
    /// can truthfully report lower-bound count precision without blocking the
    /// top-N result path.
    pub(in crate::search::query) last_tantivy_total_count: Mutex<Option<usize>>,
    /// GH #441: why the most recent hybrid search dropped its lexical leg
    /// (`None` when lexical ran normally). Robot metadata surfaces it as
    /// `_meta.lexical_degrade_reason` so an agent can tell "no lexical hits"
    /// from "lexical was skipped because the engine ran out of query fuel".
    pub(in crate::search::query) last_lexical_degrade_reason: Mutex<Option<&'static str>>,
    /// Why the most recent `search_with_fallback` did not run the automatic
    /// `*term*` retry its sparse result would otherwise get (`None` when the
    /// retry ran or did not apply). Robot metadata surfaces it as
    /// `_meta.wildcard_fallback_skipped` (2l1b0.68): above 10,000 documents
    /// the retry used to turn off without a trace.
    pub(in crate::search::query) last_wildcard_fallback_skip: Mutex<Option<&'static str>>,
}

/// `_meta.lexical_degrade_reason` value when Quill's query-fuel ceiling was
/// hit on the lexical leg of a hybrid search (GH #441).
pub const LEXICAL_DEGRADE_QUERY_FUEL_EXHAUSTED: &str = "query_fuel_exhausted";

/// `_meta.wildcard_fallback_skipped` value when the index holds more documents
/// than `CASS_AUTOMATIC_WILDCARD_FALLBACK_MAX_DOCS` (default 10,000).
pub const WILDCARD_FALLBACK_SKIPPED_LARGE_INDEX: &str = "index_over_automatic_limit";

/// `_meta.wildcard_fallback_skipped` value when
/// `CASS_AUTOMATIC_WILDCARD_FALLBACK_MAX_DOCS=0` turned the retry off.
pub const WILDCARD_FALLBACK_SKIPPED_DISABLED: &str = "automatic_retry_disabled";

/// `_meta.wildcard_fallback_skipped` value when a zero-hit query has a term
/// longer than the automatic retry accepts.
pub const WILDCARD_FALLBACK_SKIPPED_LONG_TERM: &str = "long_query_term";

/// `_meta.wildcard_fallback_skipped` value when the automatic retry ran out of
/// Quill query fuel; the plain query's hits are returned
/// (coding_agent_session_search-7hvlg).
pub const WILDCARD_FALLBACK_SKIPPED_QUERY_FUEL_EXHAUSTED: &str = "query_fuel_exhausted";

#[derive(Debug, Clone, Copy)]
pub struct SearchClientOptions {
    pub enable_reload: bool,
    pub enable_warm: bool,
    /// Refuse every storage recovery write while hydrating search results.
    /// The default reader may repair a dirty WAL or duplicate FTS schema; a
    /// strict reader surfaces those conditions instead.
    pub strict_read_only: bool,
}

impl Default for SearchClientOptions {
    fn default() -> Self {
        Self {
            enable_reload: true,
            enable_warm: true,
            strict_read_only: false,
        }
    }
}

impl Drop for SearchClient {
    fn drop(&mut self) {
        let sqlite = match self.sqlite.get_mut() {
            Ok(sqlite) => sqlite,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(mut conn) = sqlite.take()
            && let Err(error) = conn.close_without_checkpoint_sync()
        {
            tracing::warn!(%error, "failed to close search sqlite owner");
        }
        FEDERATED_SEARCH_READERS
            .write()
            .remove(&self.cache_namespace);
    }
}

#[cfg(test)]
mod tests;
