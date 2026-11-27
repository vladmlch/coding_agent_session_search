use anyhow::Result;
use frankensearch::{
    Cx as FsCx, QueryClass as FsQueryClass, TwoTierConfig as FsTwoTierConfig,
    candidate_count as fs_candidate_count,
};
use lru::LruCache;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;

use crate::search::vector_index::SemanticAnnUnavailableReason;
use crate::sources::provenance::SourceFilter;

use super::cache::CacheStats;
use super::parsing::QuerySuggestion;
#[cfg(any(test, feature = "cass-layer-b-conformance"))]
use super::sqlite_hydration::SQLITE_MAX_VARIABLE_NUMBER;

// ============================================================================
// String Interner for Cache Keys (Opt 2.3)
// ============================================================================
//
// Reduces memory usage and allocation overhead for repeated cache key patterns.
// Uses LRU eviction to bound memory, Arc<str> for cheap cloning.

/// Thread-safe string interner with bounded memory via LRU eviction.
/// Uses `LruCache<Arc<str>, Arc<str>>` where key and value are the same `Arc`,
/// enabling O(1) lookup via `Borrow<str>` trait while preserving LRU semantics.
pub struct StringInterner {
    cache: RwLock<LruCache<Arc<str>, Arc<str>>>,
}

impl StringInterner {
    /// Create a new interner with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: RwLock::new(LruCache::new(
                NonZeroUsize::new(capacity).expect("capacity must be > 0"),
            )),
        }
    }

    /// Intern a string, returning a shared `Arc<str>`.
    /// If the string is already interned, returns the existing Arc.
    /// Otherwise, creates a new Arc and caches it.
    ///
    /// Performance: O(1) lookup via LruCache's internal HashMap.
    pub fn intern(&self, s: &str) -> Arc<str> {
        // Fast path: read-only check for existing entry (O(1) lookup)
        {
            let cache = self.cache.read();
            // LruCache::peek allows O(1) lookup without updating LRU order
            // Arc<str>: Borrow<str> enables lookup by &str
            if let Some(arc) = cache.peek(s) {
                return Arc::clone(arc);
            }
        }

        // Slow path: acquire write lock and insert
        let mut cache = self.cache.write();

        // Double-check after acquiring write lock (another thread may have inserted)
        // Use get() here to update LRU order since we're about to use this entry
        if let Some(arc) = cache.get(s) {
            return Arc::clone(arc);
        }

        // Create new Arc<str> and insert (same Arc as key and value)
        let arc: Arc<str> = Arc::from(s);
        cache.put(Arc::clone(&arc), Arc::clone(&arc));
        arc
    }

    /// Get the current number of interned strings.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.cache.read().len()
    }

    /// Check if the interner is empty.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.cache.read().is_empty()
    }
}

/// Global cache key interner with 10K entry limit (~1MB for typical keys).
/// Uses Lazy initialization for thread-safe singleton.
pub(in crate::search::query) static CACHE_KEY_INTERNER: Lazy<StringInterner> =
    Lazy::new(|| StringInterner::new(10_000));

/// Intern a cache key string, returning a shared Arc<str>.
#[inline]
pub(in crate::search::query) fn intern_cache_key(s: &str) -> Arc<str> {
    CACHE_KEY_INTERNER.intern(s)
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct SearchFilters {
    pub agents: HashSet<String>,
    pub workspaces: HashSet<String>,
    pub created_from: Option<i64>,
    pub created_to: Option<i64>,
    /// Filter by conversation source (local, remote, or specific source ID)
    #[serde(skip_serializing_if = "SourceFilter::is_all")]
    pub source_filter: SourceFilter,
    /// Filter to specific session source paths (for chained searches)
    #[serde(skip_serializing_if = "HashSet::is_empty")]
    pub session_paths: HashSet<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum SearchMode {
    /// Lexical (BM25) search - keyword matching
    Lexical,
    /// Semantic search - embedding similarity
    Semantic,
    /// Hybrid-preferred search - RRF fusion of lexical and semantic when available
    #[default]
    Hybrid,
}

impl SearchMode {
    pub fn next(self) -> Self {
        match self {
            SearchMode::Lexical => SearchMode::Semantic,
            SearchMode::Semantic => SearchMode::Hybrid,
            SearchMode::Hybrid => SearchMode::Lexical,
        }
    }
}

/// Execution strategy for semantic search.
///
/// `Single` preserves existing exact vector behavior.
/// The other modes request two-tier semantics, but this synchronous candidate
/// path currently carries only one CASS query embedding. It therefore falls
/// back to exact single-tier search until it can supply independently
/// identity-bound fast and quality embeddings; the owner-backed progressive
/// API remains the safe two-tier execution path.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticTierMode {
    #[default]
    Single,
    Progressive,
    FastOnly,
    QualityOnly,
}

impl SemanticTierMode {
    pub(in crate::search::query) const fn wants_two_tier(self) -> bool {
        !matches!(self, Self::Single)
    }
}

pub(in crate::search::query) const PROGRESSIVE_EMBEDDING_CACHE_CAPACITY: usize = 64;
pub(in crate::search::query) const ANN_CANDIDATE_MULTIPLIER: usize = 4;
pub(in crate::search::query) const HYBRID_NO_LIMIT_PLANNING_WINDOW: usize = 64;
pub(in crate::search::query) const HYBRID_NO_LIMIT_SEMANTIC_CAP: usize = 2048;
pub(in crate::search::query) const AUTOMATIC_WILDCARD_FALLBACK_MAX_TOKEN_CHARS: usize = 16;

/// Upper bound on how many documents a `limit == 0` ("no limit") search is
/// allowed to materialize. Each `SearchHit` carries the full message
/// `content` string (roughly 80 KB p99 in real corpora), so an unlimited
/// search on a ~500k-row user history can easily allocate tens of
/// gigabytes of heap AND drive sustained multi-GB/s reads off the Tantivy
/// `.store` file and SQLite rows, crushing the whole machine.
///
/// The cap is computed dynamically from `/proc/meminfo` `MemAvailable`
/// (Linux) so a dev box with 512 GB of RAM is allowed to return ~200k
/// rows while a 2 GB laptop stops at the floor. The cap translates
/// directly into an upper bound on disk-I/O per query because the
/// per-hit hydration loop in `fs_load_doc()` / `hydrate_tantivy_hit_contents`
/// does ~11 `.store` field reads per hit plus up to one SQLite row
/// fetch — bounding hits bounds bytes read.
///
/// Override with `CASS_SEARCH_NO_LIMIT_CAP=<hits>` or
/// `CASS_SEARCH_NO_LIMIT_BYTES=<bytes>`. Both overrides are still
/// clamped to `[NO_LIMIT_RESULT_MIN, NO_LIMIT_RESULT_MAX]` on the way
/// out — an unclamped override would re-open the same "crush the
/// machine" hole this cap exists to close.
pub const NO_LIMIT_RESULT_MIN: usize = 1_000;
pub const NO_LIMIT_RESULT_MAX: usize = 1_000_000;

/// Approximate on-heap size per `SearchHit` used to translate a
/// memory budget into a hit-count cap. Kept conservatively high
/// (p99-ish message content + metadata strings) so real workloads
/// stay well under the computed bytes budget.
pub(in crate::search::query) const AVG_HIT_BYTES: u64 = 80 * 1024;

/// Absolute ceiling on the memory budget for a single "no limit"
/// search, regardless of how much RAM is free. 16 GiB keeps sustained
/// disk reads on a single query bounded to <10 s on a 2 GB/s NVMe —
/// long enough for a power user to wait, short enough not to block
/// other workloads on a shared box.
pub(in crate::search::query) const NO_LIMIT_BYTES_CEILING: u64 = 16 * 1024 * 1024 * 1024;

/// Floor on the memory budget. On a 2 GB laptop we still let a
/// single "no limit" query use ~256 MiB — small enough to survive,
/// large enough to be useful.
pub(in crate::search::query) const NO_LIMIT_BYTES_FLOOR: u64 = 256 * 1024 * 1024;

/// Fraction of `MemAvailable` we're willing to spend on a single
/// "no limit" search response. 1/16 leaves 93% of RAM for everything
/// else on the box.
pub(in crate::search::query) const NO_LIMIT_RAM_DIVISOR: u64 = 16;

/// Above this corpus size, exact total counting is not part of the default
/// top-N path, and a saturated page reports `limit + 1` as a lower bound.
///
/// The cap was 50,000 when the lexical engine was Tantivy, whose `Count` over a
/// common term on a multi-million-document index could dominate the query. On
/// Quill the count is cheap: on a 1,034,219-document archive (paired runs,
/// `--limit 10`), exact totals added 0.00-0.11 s CPU to a ~0.8 s search even for
/// "AGENTS.md" (867,087 matches) and "the" (439,461). Meanwhile the capped
/// answer was wrong by up to five orders of magnitude ("stale lock": 11 against
/// 11,915), and agents read `total_matches` as a count. The cap now sits at five
/// times that archive; `CASS_SEARCH_EXACT_TOTAL_COUNT_MAX_DOCS` still overrides it.
pub(in crate::search::query) const DEFAULT_EXACT_TOTAL_COUNT_MAX_DOCS: usize = 5_000_000;
pub(in crate::search::query) const DEFAULT_AUTOMATIC_WILDCARD_FALLBACK_MAX_DOCS: usize = 10_000;

pub(in crate::search::query) fn exact_total_count_max_docs() -> usize {
    static MAX_DOCS: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *MAX_DOCS.get_or_init(|| {
        dotenvy::var("CASS_SEARCH_EXACT_TOTAL_COUNT_MAX_DOCS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(DEFAULT_EXACT_TOTAL_COUNT_MAX_DOCS)
    })
}

pub(in crate::search::query) fn should_collect_exact_total_count(
    index_doc_count: usize,
    max_docs_for_exact_count: usize,
) -> bool {
    max_docs_for_exact_count > 0 && index_doc_count <= max_docs_for_exact_count
}

pub(in crate::search::query) fn automatic_wildcard_fallback_max_docs() -> usize {
    static MAX_DOCS: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *MAX_DOCS.get_or_init(|| {
        dotenvy::var("CASS_AUTOMATIC_WILDCARD_FALLBACK_MAX_DOCS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(DEFAULT_AUTOMATIC_WILDCARD_FALLBACK_MAX_DOCS)
    })
}

pub(in crate::search::query) fn should_allow_automatic_wildcard_fallback(
    index_doc_count: usize,
    max_docs_for_automatic_wildcard: usize,
) -> bool {
    max_docs_for_automatic_wildcard > 0 && index_doc_count <= max_docs_for_automatic_wildcard
}

pub(in crate::search::query) fn available_memory_bytes() -> Option<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in meminfo.lines() {
        if let Some(rest) = line.strip_prefix("MemAvailable:") {
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb.saturating_mul(1024));
        }
    }
    None
}

pub(in crate::search::query) fn no_limit_result_cap() -> usize {
    static CAP: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CAP.get_or_init(|| {
        compute_no_limit_result_cap_from(
            dotenvy::var("CASS_SEARCH_NO_LIMIT_CAP").ok(),
            dotenvy::var("CASS_SEARCH_NO_LIMIT_BYTES").ok(),
            available_memory_bytes(),
        )
    })
}

/// Pure version of the cap-computation, with env + `/proc/meminfo`
/// passed in as arguments. Kept pure so unit tests can drive it
/// deterministically without mutating the process-global env (which
/// would race with every other parallel test that reads env, including
/// the search-query pipeline tests that transitively hit
/// `no_limit_result_cap()`).
pub(in crate::search::query) fn compute_no_limit_result_cap_from(
    cap_env: Option<String>,
    bytes_env: Option<String>,
    available_bytes: Option<u64>,
) -> usize {
    // Explicit hit-count override takes priority, but is still clamped
    // to `[MIN, MAX]` so a typo like `CASS_SEARCH_NO_LIMIT_CAP=10000000000`
    // can't reopen the unbounded-result bug this cap closes.
    if let Some(hits) = cap_env
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
    {
        return hits.clamp(NO_LIMIT_RESULT_MIN, NO_LIMIT_RESULT_MAX);
    }

    let budget_bytes = no_limit_budget_bytes(bytes_env, available_bytes);
    let hits = (budget_bytes / AVG_HIT_BYTES) as usize;
    hits.clamp(NO_LIMIT_RESULT_MIN, NO_LIMIT_RESULT_MAX)
}

pub(in crate::search::query) fn no_limit_budget_bytes(
    bytes_env: Option<String>,
    available_bytes: Option<u64>,
) -> u64 {
    bytes_env
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .or_else(|| no_limit_available_memory_budget(available_bytes))
        .unwrap_or(NO_LIMIT_BYTES_FLOOR)
}

pub(in crate::search::query) fn no_limit_available_memory_budget(
    available_bytes: Option<u64>,
) -> Option<u64> {
    available_bytes.map(|avail| {
        (avail / NO_LIMIT_RAM_DIVISOR).clamp(NO_LIMIT_BYTES_FLOOR, NO_LIMIT_BYTES_CEILING)
    })
}

pub(in crate::search::query) static FRANKENSEARCH_TWO_TIER_CONFIG: Lazy<FsTwoTierConfig> =
    Lazy::new(|| FsTwoTierConfig::optimized().with_env_overrides());

pub(in crate::search::query) fn frankensearch_two_tier_config() -> FsTwoTierConfig {
    FRANKENSEARCH_TWO_TIER_CONFIG.clone()
}

#[inline]
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::search::query) const fn progressive_phase_fetch_limit(limit: usize) -> usize {
    let limit = if limit == 0 { 1 } else { limit };
    limit.saturating_mul(3)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::search::query) struct HybridCandidateBudget {
    pub(in crate::search::query) lexical_candidates: usize,
    pub(in crate::search::query) semantic_candidates: usize,
}

#[inline]
pub(in crate::search::query) const fn hybrid_stage_multipliers(
    query_class: FsQueryClass,
) -> (usize, usize) {
    match query_class {
        // Identifier-heavy queries: prioritize lexical precision.
        FsQueryClass::Identifier => (6, 2),
        // Keyword queries: balanced lexical/semantic retrieval.
        FsQueryClass::ShortKeyword => (4, 4),
        // Natural language queries: prioritize semantic retrieval.
        FsQueryClass::NaturalLanguage => (2, 8),
        // Empty query should short-circuit before budgeting.
        FsQueryClass::Empty => (0, 0),
    }
}

#[inline]
pub(in crate::search::query) fn hybrid_candidate_budget(
    query: &str,
    requested_limit: usize,
    effective_limit: usize,
    offset: usize,
    total_docs: usize,
) -> HybridCandidateBudget {
    let query_class = FsQueryClass::classify(query);
    let (lex_mult, sem_mult) = hybrid_stage_multipliers(query_class);
    let total_docs = total_docs.max(1);

    // When no explicit limit is requested, keep "no limit" output semantics,
    // but bound semantic fanout so hybrid doesn't try to score the entire corpus.
    if requested_limit == 0 {
        let planning_window = HYBRID_NO_LIMIT_PLANNING_WINDOW.max(offset.saturating_add(1));
        // Cap the lexical fanout — without a ceiling a "no limit" hybrid
        // query on a ~500k-row corpus asks Tantivy to materialize a
        // `Vec<SearchHit>` the size of the entire index, which is the
        // unboundedness fixed by `no_limit_result_cap()`.
        let lexical = effective_limit.min(total_docs).min(no_limit_result_cap());
        // Semantic fan-out can be wide in principle, but must never
        // exceed the lexical cap — the pipeline fuses lexical+semantic
        // candidates and returning more semantic candidates than
        // lexical is both wasteful (semantic is the expensive tier)
        // and breaks the pre-cap invariant that `semantic ≤ lexical`.
        // On tiny boxes where `no_limit_result_cap()` hits the floor,
        // this pulls semantic down with it.
        let semantic = fs_candidate_count(planning_window, 0, sem_mult)
            .max(planning_window)
            .min(HYBRID_NO_LIMIT_SEMANTIC_CAP.max(offset.saturating_add(planning_window)))
            .min(total_docs)
            .min(lexical);
        return HybridCandidateBudget {
            lexical_candidates: lexical,
            semantic_candidates: semantic,
        };
    }

    let lexical = fs_candidate_count(requested_limit, offset, lex_mult.max(1))
        .max(requested_limit.saturating_add(offset))
        .min(total_docs);
    let semantic = fs_candidate_count(requested_limit, offset, sem_mult.max(1))
        .max(requested_limit.saturating_add(offset))
        .min(total_docs);

    HybridCandidateBudget {
        lexical_candidates: lexical,
        semantic_candidates: semantic,
    }
}

/// Indicates how a search result matched the query.
/// Used for ranking: exact matches rank higher than wildcard matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MatchType {
    /// No wildcards - matched via exact term or edge n-gram prefix
    #[default]
    Exact,
    /// Matched via trailing wildcard (foo*)
    Prefix,
    /// Matched via leading wildcard (*foo) - uses regex
    Suffix,
    /// Matched via both wildcards (*foo*) - uses regex
    Substring,
    /// Matched via complex wildcard (e.g. f*o) - uses regex
    Wildcard,
    /// Matched via automatic wildcard fallback when exact search was sparse
    ImplicitWildcard,
}

impl MatchType {
    /// Returns a quality factor for ranking (1.0 = best, lower = less precise match)
    pub fn quality_factor(self) -> f32 {
        match self {
            MatchType::Exact => 1.0,
            MatchType::Prefix => 0.9,
            MatchType::Suffix => 0.8,
            MatchType::Substring => 0.7,
            MatchType::Wildcard => 0.65,
            MatchType::ImplicitWildcard => 0.6,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FieldMask {
    pub(in crate::search::query) flags: u8,
    pub(in crate::search::query) preview_content_chars: Option<usize>,
}

impl FieldMask {
    const CONTENT: u8 = 1 << 0;
    const SNIPPET: u8 = 1 << 1;
    const TITLE: u8 = 1 << 2;
    const CACHE: u8 = 1 << 3;

    pub const FULL: Self = Self {
        flags: Self::CONTENT | Self::SNIPPET | Self::TITLE | Self::CACHE,
        preview_content_chars: None,
    };

    pub fn new(
        wants_content: bool,
        wants_snippet: bool,
        wants_title: bool,
        allows_cache: bool,
    ) -> Self {
        let mut flags = 0;
        if wants_content {
            flags |= Self::CONTENT;
        }
        if wants_snippet {
            flags |= Self::SNIPPET;
        }
        if wants_title {
            flags |= Self::TITLE;
        }
        if allows_cache {
            flags |= Self::CACHE;
        }
        Self {
            flags,
            preview_content_chars: None,
        }
    }

    pub fn with_preview_content_limit(mut self, max_chars: Option<usize>) -> Self {
        self.preview_content_chars = max_chars;
        if max_chars.is_some() {
            self.flags &= !Self::CACHE;
        }
        self
    }

    pub fn needs_content(self) -> bool {
        self.flags & Self::CONTENT != 0
    }

    pub fn wants_snippet(self) -> bool {
        self.flags & Self::SNIPPET != 0
    }

    pub fn wants_title(self) -> bool {
        self.flags & Self::TITLE != 0
    }

    pub fn allows_cache(self) -> bool {
        self.flags & Self::CACHE != 0
    }

    pub fn preview_content_limit(self) -> Option<usize> {
        self.preview_content_chars
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SearchHit {
    pub title: String,
    pub snippet: String,
    pub content: String,
    #[serde(skip_serializing)]
    pub content_hash: u64,
    /// Canonical conversation identity in the archive used by search.
    /// Keep this with source_id, source_path and line_number for exact follow-ups.
    /// Null means no canonical conversation identity is available; never invent one.
    pub conversation_id: Option<i64>,
    pub score: f32,
    pub source_path: String,
    pub agent: String,
    pub workspace: String,
    /// Original workspace path before rewriting (P6.2)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_original: Option<String>,
    pub created_at: Option<i64>,
    /// Canonical message ordinal (`messages.idx + 1`), NOT a physical file line.
    /// Pass unchanged to view/expand --message-index, with source_id and conversation_id.
    pub line_number: Option<usize>,
    /// How this result matched the query (exact, prefix wildcard, etc.)
    #[serde(default)]
    pub match_type: MatchType,
    // Provenance fields (P3.3)
    /// Source identifier (e.g., "local", "work-laptop")
    #[serde(default = "default_source_id")]
    pub source_id: String,
    /// Origin kind ("local" or "remote"; legacy "ssh" normalizes to "remote")
    #[serde(default = "default_source_id")]
    pub origin_kind: String,
    /// Origin host label for remote sources
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin_host: Option<String>,
}

/// One engine-ranked message admitted to the CASS lexical Layer-B hydrator.
///
/// This in-memory conformance input deliberately uses the exact
/// `(conversation_id, message_index)` pair stored by both lexical engines. The
/// schema's historical `source_id#msg_idx` rendering is not a safe hydration
/// key for real CASS data: `source_id` is provenance (commonly `local`) and
/// `msg_idx` restarts in every conversation. The Layer-B seam resolves the
/// exact pair to one globally unique database message and rejects a missing or
/// ambiguous mapping instead of falling back to source/path heuristics.
#[cfg(any(test, feature = "cass-layer-b-conformance"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CassLexicalLayerBCandidate {
    /// Database conversation identity stored by both lexical engines.
    pub conversation_id: i64,
    /// Zero-based message index within `conversation_id`.
    pub message_index: u64,
    /// Exact engine score representation. It is converted to `f32` only while
    /// the production `SearchHit` is materialized.
    pub score_bits: u32,
    /// Zero-based rank in the engine-owned candidate stream.
    pub rank: usize,
}

/// Largest candidate stream the bounded Layer-B projector can hydrate in one
/// transaction without exceeding FrankenSQLite's bind-variable ceiling.
#[cfg(any(test, feature = "cass-layer-b-conformance"))]
pub const CASS_LEXICAL_LAYER_B_CANDIDATE_MAX: usize = SQLITE_MAX_VARIABLE_NUMBER;

/// Same-input request for the bounded CASS Layer-B projection seam.
#[cfg(any(test, feature = "cass-layer-b-conformance"))]
#[derive(Debug, Clone, Copy)]
pub struct CassLexicalLayerBRequest<'a> {
    pub candidates: &'a [CassLexicalLayerBCandidate],
    pub raw_query: &'a str,
    /// The only filter owned by Layer B. Agent, workspace, time, and source
    /// filters must already have been realized by the engine-owned Layer A.
    pub session_paths: &'a HashSet<String>,
    pub limit: usize,
    pub offset: usize,
    pub field_mask: FieldMask,
}

/// One CASS-visible result after the production hydrator and post-filter.
#[cfg(any(test, feature = "cass-layer-b-conformance"))]
#[derive(Debug, Clone)]
pub struct CassLexicalLayerBHit {
    pub message_id: u64,
    pub input_rank: usize,
    pub hit: SearchHit,
}

/// Semantics intentionally outside the first bounded Layer-B extraction.
///
/// Keeping these gaps typed prevents a successful projection test from being
/// promoted into a claim about the controller's retry or count behavior.
#[cfg(any(test, feature = "cass-layer-b-conformance"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CassLexicalLayerBNoClaim {
    AdaptiveFetchRetry,
    AutomaticWildcardFallback,
    BoundedPreviewProjection,
    QueryAwareSnippet,
    ShippingTantivyMaterializer,
    TotalCountPrecision,
    WorkspaceOriginal,
}

/// Bounded result of one same-input CASS Layer-B projection.
#[cfg(any(test, feature = "cass-layer-b-conformance"))]
#[derive(Debug, Clone)]
pub struct CassLexicalLayerBPage {
    pub input_candidate_count: usize,
    pub hydrated_candidate_count: usize,
    /// Count after noise filtering, deduplication, and `session_paths`, before
    /// applying `offset` and `limit`.
    pub available_hits: usize,
    pub hits: Vec<CassLexicalLayerBHit>,
    pub no_claims: [CassLexicalLayerBNoClaim; 7],
}

/// Fail-closed validation and hydration errors for the Layer-B seam.
#[cfg(any(test, feature = "cass-layer-b-conformance"))]
#[derive(Debug, thiserror::Error)]
pub enum CassLexicalLayerBError {
    #[error("Layer-B candidate count {actual} exceeds the bound {maximum}")]
    CandidateBoundExceeded { actual: usize, maximum: usize },
    #[error("Layer-B candidate at position {position} declared rank {actual_rank}")]
    NonCanonicalRank { position: usize, actual_rank: usize },
    #[error("Layer-B candidate at rank {rank} has invalid conversation_id {conversation_id}")]
    InvalidConversationId { rank: usize, conversation_id: i64 },
    #[error("Layer-B candidate at rank {rank} has a message index that does not fit i64")]
    MessageIndexOutOfRange { rank: usize },
    #[error(
        "Layer-B candidate identity ({conversation_id}, {message_index}) appears more than once"
    )]
    DuplicateIdentity {
        conversation_id: i64,
        message_index: u64,
    },
    #[error("Layer-B candidate at rank {rank} has a non-finite score")]
    NonFiniteScore { rank: usize },
    #[error("Layer-B projection requires a non-zero page limit")]
    ZeroLimit,
    #[error("Layer-B offset plus limit overflows usize")]
    PaginationOverflow,
    #[error("Layer-B query-aware snippets are outside this contract slice")]
    SnippetNotSupported,
    #[error("Layer-B bounded preview projection is outside this contract slice")]
    BoundedPreviewNotSupported,
    #[error(
        "Layer-B candidate identity ({conversation_id}, {message_index}) did not resolve uniquely"
    )]
    UnresolvedIdentity {
        conversation_id: i64,
        message_index: u64,
    },
    #[error("Layer-B message_id {message_id} disappeared during hydration")]
    HydratedCandidateMissing { message_id: u64 },
    #[error("Layer-B SQLite hydration failed")]
    Hydration(#[source] anyhow::Error),
    #[error("Layer-B post-filter output lost its message/rank binding")]
    LostCandidateBinding,
}

pub(in crate::search::query) static LAZY_FIELDS_ENABLED: Lazy<bool> = Lazy::new(|| {
    dotenvy::var("CASS_LAZY_FIELDS")
        .ok()
        .map(|v| !(v == "0" || v.eq_ignore_ascii_case("false")))
        .unwrap_or(true)
});

pub(in crate::search::query) fn default_source_id() -> String {
    "local".to_string()
}

pub(in crate::search::query) fn effective_field_mask(field_mask: FieldMask) -> FieldMask {
    if *LAZY_FIELDS_ENABLED {
        field_mask
    } else {
        FieldMask::FULL
    }
}

pub(in crate::search::query) struct CassLexicalSearchResult {
    pub(in crate::search::query) hits: Vec<crate::search::quill_bridge::QuillLexicalDocHit>,
    pub(in crate::search::query) total_count: Option<usize>,
}

/// Execute one parsed query, computing an exact total only when it is affordable.
///
/// The exact-count rule is preserved from the Tantivy incumbent: a total is
/// reported when the page is not saturated (it is then simply `offset + len`),
/// and an exact count is computed only when the page IS saturated and the index
/// is small enough for a full scan to be worth it. Quill takes that decision as
/// an up-front flag rather than a second query, so saturation is established
/// with one cheap pass and the exact count is requested only when the rule says
/// to — the same two-pass cost the incumbent paid, in the same circumstances.
pub(in crate::search::query) fn execute_query_with_bounded_exact_count(
    reader: &frankensearch::quill::QuillSearchIndex,
    query: &frankensearch::quill::query::Query,
    limit: usize,
    offset: usize,
) -> Result<CassLexicalSearchResult> {
    bounded_exact_count_page(limit, offset, exact_total_count_max_docs(), |exact_count| {
        crate::search::quill_bridge::search_paginated(reader, query, limit, offset, exact_count)
    })
}

/// The policy of [`execute_query_with_bounded_exact_count`], with the engine
/// call passed in as `run(exact_count)`.
///
/// The ranked page is the result; the exact total is an optional extra. The
/// counting pass visits every match (the engine's block-max pruning is off for
/// it), so on a fragmented or match-heavy index it can exhaust the query fuel
/// the ranked page fit in. That refusal costs only the total, which is then
/// reported as unknown exactly as on a large index (GH #518). Every other
/// failure of the counting pass, cancellation included, still fails the query.
pub(in crate::search::query) fn bounded_exact_count_page(
    limit: usize,
    offset: usize,
    exact_count_max_docs: usize,
    mut run: impl FnMut(bool) -> Result<crate::search::quill_bridge::QuillLexicalPage>,
) -> Result<CassLexicalSearchResult> {
    let page = run(false)?;
    // `limit == 0` must not read as saturated: an empty page would equal an
    // empty limit and trigger the expensive exact-count pass for a caller that
    // asked for no rows at all.
    let page_saturated = limit > 0 && page.hits.len() == limit;
    let index_doc_count = page.doc_count;
    let total_count = if page_saturated {
        if should_collect_exact_total_count(index_doc_count, exact_count_max_docs) {
            match run(true) {
                Ok(counted) => counted.total_count,
                Err(error) if crate::search::quill_bridge::is_query_fuel_exhausted(&error) => {
                    tracing::warn!(
                        index_doc_count,
                        limit,
                        offset,
                        error = %format_args!("{error:#}"),
                        "exact lexical count ran out of query fuel; keeping the ranked page \
                         and reporting its total as unknown"
                    );
                    None
                }
                Err(error) => return Err(error),
            }
        } else {
            tracing::debug!(
                index_doc_count,
                exact_count_max_docs,
                limit,
                offset,
                "skipping exact lexical count on large saturated result page"
            );
            None
        }
    } else if offset > 0 && page.hits.is_empty() {
        None
    } else {
        Some(offset.saturating_add(page.hits.len()))
    };
    Ok(CassLexicalSearchResult {
        hits: page.hits,
        total_count,
    })
}

/// Result of a search operation with metadata about how matches were found
#[derive(Debug, Clone)]
pub struct SearchResult {
    /// The search results
    pub hits: Vec<SearchHit>,
    /// Whether wildcard fallback was used (query had no/few exact matches)
    pub wildcard_fallback: bool,
    /// Cache metrics snapshot for observability/debug
    pub cache_stats: CacheStats,
    /// Did-you-mean suggestions when hits are empty or sparse
    pub suggestions: Vec<QuerySuggestion>,
    /// ANN search statistics (present when --approximate was used)
    pub ann_stats: Option<crate::search::ann_index::AnnSearchStats>,
    /// Stable reason an approximate request fell back to exact FSVI search.
    pub ann_unavailable_reason: Option<SemanticAnnUnavailableReason>,
    /// True total matching documents from the search engine when that is cheap
    /// and available. Large saturated lexical pages intentionally leave this as
    /// `None`; robot output then reports `total_matches` as a lower bound
    /// instead of forcing an expensive exact recount.
    pub total_count: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressivePhaseKind {
    Initial,
    Refined,
}

// Phase events intentionally carry a complete SearchResult so consumers can
// react without reloading auxiliary state or keeping cross-event caches.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum ProgressiveSearchEvent {
    Phase {
        kind: ProgressivePhaseKind,
        result: SearchResult,
        elapsed_ms: u128,
    },
    RefinementFailed {
        latency_ms: u128,
        error: String,
    },
}

#[derive(Debug, Clone)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct ProgressiveSearchRequest<'a> {
    pub(crate) cx: &'a FsCx,
    pub(crate) query: &'a str,
    pub(crate) filters: SearchFilters,
    pub(crate) limit: usize,
    pub(crate) sparse_threshold: usize,
    pub(crate) field_mask: FieldMask,
    pub(crate) mode: SearchMode,
}
