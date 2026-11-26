use frankensearch::{
    RrfConfig as FsRrfConfig, ScoreSource as FsScoreSource, ScoredResult as FsScoredResult,
    VectorHit as FsVectorHit, rrf_fuse as fs_rrf_fuse,
};
use std::cmp::Ordering as CmpOrdering;
use std::collections::{HashMap, HashSet};

use crate::search::canonicalize::is_search_noise_text;
use crate::sources::provenance::SourceFilter;

use super::cache::cached_prefix_snippet;
use super::parsing::normalize_phrase_terms;
use super::types::{AUTOMATIC_WILDCARD_FALLBACK_MAX_TOKEN_CHARS, FieldMask, SearchHit};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(in crate::search::query) struct SearchHitKey {
    pub(in crate::search::query) source_id: String,
    pub(in crate::search::query) source_path: String,
    pub(in crate::search::query) conversation_id: Option<i64>,
    pub(in crate::search::query) title: String,
    pub(in crate::search::query) line_number: Option<usize>,
    pub(in crate::search::query) created_at: Option<i64>,
    pub(in crate::search::query) content_hash: u64,
}

pub(in crate::search::query) fn normalized_search_source_id_sql_expr(
    source_id_column: &str,
    origin_kind_column: &str,
    origin_host_column: &str,
) -> String {
    format!(
        "CASE \
            WHEN TRIM(COALESCE({source_id_column}, '')) != '' THEN \
                CASE \
                    WHEN LOWER(TRIM(COALESCE({source_id_column}, ''))) = '{local}' THEN '{local}' \
                    ELSE TRIM(COALESCE({source_id_column}, '')) \
                END \
            WHEN LOWER(TRIM(COALESCE({origin_kind_column}, ''))) IN ('ssh', 'remote') THEN \
                CASE \
                    WHEN TRIM(COALESCE({origin_host_column}, '')) = '' THEN 'remote' \
                    ELSE TRIM(COALESCE({origin_host_column}, '')) \
                END \
            WHEN LOWER(TRIM(COALESCE({origin_kind_column}, ''))) = '{local}' THEN '{local}' \
            WHEN TRIM(COALESCE({origin_host_column}, '')) != '' THEN TRIM(COALESCE({origin_host_column}, '')) \
            ELSE '{local}' \
         END",
        local = crate::sources::provenance::LOCAL_SOURCE_ID,
    )
}

/// SQL twin of [`normalized_search_hit_origin_kind`]: the normalized origin
/// kind (`local` / `remote` / other lowercase kind) of a conversation row,
/// from its `sources.kind` with the same `source_id` / `origin_host` fallback
/// the hit derivation uses. Lets the SQLite lane's `--source local|remote`
/// select by kind instead of by `source_id == 'local'` (bead 5bf29).
pub(in crate::search::query) fn normalized_search_origin_kind_sql_expr(
    source_id_column: &str,
    origin_kind_column: &str,
    origin_host_column: &str,
) -> String {
    format!(
        "CASE \
            WHEN LOWER(TRIM(COALESCE({origin_kind_column}, ''))) = '{local}' THEN '{local}' \
            WHEN LOWER(TRIM(COALESCE({origin_kind_column}, ''))) IN ('ssh', 'remote') THEN 'remote' \
            WHEN TRIM(COALESCE({origin_kind_column}, '')) != '' THEN LOWER(TRIM({origin_kind_column})) \
            WHEN LOWER(TRIM(COALESCE({source_id_column}, ''))) = '{local}' THEN '{local}' \
            WHEN TRIM(COALESCE({source_id_column}, '')) != '' THEN 'remote' \
            WHEN TRIM(COALESCE({origin_host_column}, '')) != '' THEN 'remote' \
            ELSE '{local}' \
         END",
        local = crate::sources::provenance::LOCAL_SOURCE_ID,
    )
}

pub(in crate::search::query) fn normalize_search_source_filter_value(source_id: &str) -> String {
    let trimmed = source_id.trim();
    if trimmed.eq_ignore_ascii_case(crate::sources::provenance::LOCAL_SOURCE_ID) {
        crate::sources::provenance::LOCAL_SOURCE_ID.to_string()
    } else {
        trimmed.to_string()
    }
}

pub(in crate::search::query) fn normalized_search_hit_source_id_parts(
    source_id: &str,
    origin_kind: &str,
    origin_host: Option<&str>,
) -> String {
    let trimmed_source_id = source_id.trim();
    if !trimmed_source_id.is_empty() {
        if trimmed_source_id.eq_ignore_ascii_case(crate::sources::provenance::LOCAL_SOURCE_ID) {
            return crate::sources::provenance::LOCAL_SOURCE_ID.to_string();
        }
        return trimmed_source_id.to_string();
    }

    let trimmed_origin_host = origin_host.map(str::trim).filter(|value| !value.is_empty());
    let trimmed_origin_kind = origin_kind.trim();
    if trimmed_origin_kind.eq_ignore_ascii_case("ssh")
        || trimmed_origin_kind.eq_ignore_ascii_case("remote")
    {
        return trimmed_origin_host.unwrap_or("remote").to_string();
    }
    if let Some(origin_host) = trimmed_origin_host {
        return origin_host.to_string();
    }

    crate::sources::provenance::LOCAL_SOURCE_ID.to_string()
}

pub(in crate::search::query) fn normalized_search_hit_origin_kind(
    source_id: &str,
    origin_kind: Option<&str>,
) -> String {
    if let Some(kind) = origin_kind.map(str::trim).filter(|value| !value.is_empty()) {
        if kind.eq_ignore_ascii_case("local") {
            return crate::sources::provenance::LOCAL_SOURCE_ID.to_string();
        }
        if kind.eq_ignore_ascii_case("ssh") || kind.eq_ignore_ascii_case("remote") {
            return "remote".to_string();
        }
        return kind.to_ascii_lowercase();
    }

    if source_id == crate::sources::provenance::LOCAL_SOURCE_ID {
        crate::sources::provenance::LOCAL_SOURCE_ID.to_string()
    } else {
        "remote".to_string()
    }
}

pub(in crate::search::query) fn normalized_search_hit_source_id(hit: &SearchHit) -> String {
    normalized_search_hit_source_id_parts(
        hit.source_id.as_str(),
        hit.origin_kind.as_str(),
        hit.origin_host.as_deref(),
    )
}

impl SearchHitKey {
    pub(in crate::search::query) fn from_hit(hit: &SearchHit) -> Self {
        Self {
            source_id: normalized_search_hit_source_id(hit),
            source_path: hit.source_path.clone(),
            conversation_id: hit.conversation_id,
            title: if hit.conversation_id.is_some() {
                String::new()
            } else {
                hit.title.trim().to_string()
            },
            line_number: hit.line_number,
            created_at: hit.created_at,
            content_hash: hit.content_hash,
        }
    }
}

impl Ord for SearchHitKey {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.source_id
            .cmp(&other.source_id)
            .then_with(|| self.source_path.cmp(&other.source_path))
            .then_with(|| self.conversation_id.cmp(&other.conversation_id))
            .then_with(|| self.title.cmp(&other.title))
            .then_with(|| self.line_number.cmp(&other.line_number))
            .then_with(|| self.created_at.cmp(&other.created_at))
            .then_with(|| self.content_hash.cmp(&other.content_hash))
    }
}

impl PartialOrd for SearchHitKey {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

pub(in crate::search::query) const FEDERATED_RRF_K: f32 = 60.0;

#[derive(Debug)]
pub(in crate::search::query) struct FederatedRankedHit {
    pub(in crate::search::query) hit: SearchHit,
    pub(in crate::search::query) shard_index: usize,
    pub(in crate::search::query) shard_rank: usize,
    pub(in crate::search::query) fused_score: f32,
}

pub(in crate::search::query) fn federated_rrf_score(shard_rank: usize) -> f32 {
    1.0 / (FEDERATED_RRF_K + shard_rank as f32 + 1.0)
}

pub(in crate::search::query) fn merge_federated_ranked_hits(
    mut ranked_hits: Vec<FederatedRankedHit>,
) -> Vec<SearchHit> {
    ranked_hits.sort_by(|a, b| {
        b.fused_score
            .total_cmp(&a.fused_score)
            .then_with(|| a.shard_rank.cmp(&b.shard_rank))
            .then_with(|| SearchHitKey::from_hit(&a.hit).cmp(&SearchHitKey::from_hit(&b.hit)))
            .then_with(|| a.shard_index.cmp(&b.shard_index))
    });
    ranked_hits
        .into_iter()
        .map(|mut ranked| {
            ranked.hit.score = ranked.fused_score;
            ranked.hit
        })
        .collect()
}

#[cfg(test)]
#[allow(dead_code)]
#[derive(Debug, Default, Clone)]
pub(in crate::search::query) struct HybridScore {
    pub(in crate::search::query) rrf: f32,
    pub(in crate::search::query) lexical_rank: Option<usize>,
    pub(in crate::search::query) semantic_rank: Option<usize>,
    pub(in crate::search::query) lexical_score: Option<f32>,
    pub(in crate::search::query) semantic_score: Option<f32>,
}

#[cfg(test)]
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(in crate::search::query) struct FusedHit {
    pub(in crate::search::query) key: SearchHitKey,
    pub(in crate::search::query) score: HybridScore,
    pub(in crate::search::query) hit: SearchHit,
}

/// Whitespace-invariant content hash used for search-hit dedup.
///
/// Uses xxhash3-64 (via `xxhash-rust`) for ~4-10x throughput over the prior
/// hand-rolled FNV-1a byte loop on the 1-2 KB tool-output bodies that
/// dominate the corpus. The hash value is in-memory only (dedup keys), never
/// persisted, so switching algorithms requires no migration. The canonical
/// byte stream fed to the hasher is: each whitespace-separated token
/// followed by a single 0x20 space between tokens — identical tokenization
/// rules as the former FNV implementation, so dedup semantics are preserved.
pub(crate) fn stable_content_hash(content: &str) -> u64 {
    use xxhash_rust::xxh3::Xxh3;
    let mut hasher = Xxh3::new();
    let mut first = true;
    for token in content.split_whitespace() {
        if !first {
            hasher.update(b" ");
        }
        hasher.update(token.as_bytes());
        first = false;
    }
    hasher.digest()
}

pub(in crate::search::query) fn stable_hit_hash(
    content: &str,
    source_path: &str,
    line_number: Option<usize>,
    created_at: Option<i64>,
) -> u64 {
    use xxhash_rust::xxh3::Xxh3;
    let mut hasher = Xxh3::new();
    // Seed with the whitespace-normalized content hash for empty-body
    // stability (matches the former FNV_OFFSET fallback).
    if !content.is_empty() {
        hasher.update(&stable_content_hash(content).to_le_bytes());
    }
    hasher.update(b"|");
    hasher.update(source_path.as_bytes());
    hasher.update(b"|");
    if let Some(line) = line_number {
        let mut buf = itoa::Buffer::new();
        hasher.update(buf.format(line).as_bytes());
    }
    hasher.update(b"|");
    if let Some(ts) = created_at {
        let mut buf = itoa::Buffer::new();
        hasher.update(buf.format(ts).as_bytes());
    }
    hasher.digest()
}

pub(in crate::search::query) fn search_hit_key_doc_id(key: &SearchHitKey) -> String {
    // Unit Separator (0x1F) is extremely unlikely in filesystem paths/ids.
    // Bead num7z: build the stable dedup key directly into a pre-sized
    // String, branching on each Option instead of allocating throwaway
    // per-field Strings via `.map(|v| v.to_string())`. Output must stay
    // byte-identical to the prior `format!`-based implementation: empty
    // string for `None` optional fields, the integer's `Display` rendering
    // otherwise, all joined by 0x1F.
    use std::fmt::Write as _;
    const SEP: char = '\u{1f}';
    // 20 bytes covers the decimal rendering of any i64/usize/u64.
    let capacity = key.source_id.len()
        + key.source_path.len()
        + key.title.len()
        + 6 // six separators
        + 3 * 20 // three possibly-empty i64/usize fields
        + 20; // content_hash u64
    let mut out = String::with_capacity(capacity);
    out.push_str(&key.source_id);
    out.push(SEP);
    out.push_str(&key.source_path);
    out.push(SEP);
    if let Some(v) = key.conversation_id {
        let _ = write!(out, "{v}");
    }
    out.push(SEP);
    out.push_str(&key.title);
    out.push(SEP);
    if let Some(v) = key.line_number {
        let _ = write!(out, "{v}");
    }
    out.push(SEP);
    if let Some(v) = key.created_at {
        let _ = write!(out, "{v}");
    }
    out.push(SEP);
    let _ = write!(out, "{}", key.content_hash);
    out
}

pub(in crate::search::query) fn search_hit_doc_id(hit: &SearchHit) -> String {
    search_hit_key_doc_id(&SearchHitKey::from_hit(hit))
}

/// Comparator for FusedHit: descending RRF score, prefer dual-source, then key for determinism.
#[cfg(test)]
pub(in crate::search::query) fn cmp_fused_hit_desc(a: &FusedHit, b: &FusedHit) -> CmpOrdering {
    b.score
        .rrf
        .total_cmp(&a.score.rrf)
        .then_with(|| {
            let a_both = a.score.lexical_rank.is_some() && a.score.semantic_rank.is_some();
            let b_both = b.score.lexical_rank.is_some() && b.score.semantic_rank.is_some();
            match (b_both, a_both) {
                (true, false) => CmpOrdering::Greater,
                (false, true) => CmpOrdering::Less,
                _ => CmpOrdering::Equal,
            }
        })
        .then_with(|| a.key.cmp(&b.key))
}

/// Threshold below which full sort is faster than quickselect + partial sort.
#[cfg(test)]
#[allow(dead_code)]
pub(in crate::search::query) const QUICKSELECT_THRESHOLD: usize = 64;

/// Partition fused hits to get top-k in O(N + k log k) instead of O(N log N).
///
/// For k << N, this is significantly faster than sorting all N elements.
/// Uses `select_nth_unstable_by` for O(N) average-case partitioning,
/// then sorts only the top-k elements.
///
/// Note: Currently only used for tests. Production code uses full sort for
/// content deduplication which requires seeing all elements.
#[cfg(test)]
#[allow(dead_code)]
pub(in crate::search::query) fn top_k_fused(mut hits: Vec<FusedHit>, k: usize) -> Vec<FusedHit> {
    let n = hits.len();

    // Edge cases: nothing to do or k >= n
    if n == 0 || k == 0 {
        return Vec::new();
    }
    if k >= n {
        hits.sort_by(cmp_fused_hit_desc);
        return hits;
    }

    // For small N, full sort has less overhead than quickselect
    if n < QUICKSELECT_THRESHOLD {
        hits.sort_by(cmp_fused_hit_desc);
        hits.truncate(k);
        return hits;
    }

    // Partition: move top-k elements to the front (unordered) in O(N)
    hits.select_nth_unstable_by(k - 1, cmp_fused_hit_desc);

    // Truncate to just the top-k elements
    hits.truncate(k);

    // Sort just the top-k in O(k log k)
    hits.sort_by(cmp_fused_hit_desc);

    hits
}

/// Fuse lexical + semantic hits using Reciprocal Rank Fusion (RRF).
/// Applies deterministic tie-breaking and returns the requested page slice.
pub fn rrf_fuse_hits(
    lexical: &[SearchHit],
    semantic: &[SearchHit],
    query: &str,
    limit: usize,
    offset: usize,
) -> Vec<SearchHit> {
    if limit == 0 {
        return Vec::new();
    }
    let total_candidates = lexical.len().saturating_add(semantic.len());
    if total_candidates == 0 {
        return Vec::new();
    }

    let mut lexical_scored = Vec::with_capacity(lexical.len());
    let mut semantic_scored = Vec::with_capacity(semantic.len());
    let mut hit_by_doc_id: HashMap<String, SearchHit> = HashMap::with_capacity(total_candidates);

    for hit in lexical {
        let doc_id = search_hit_doc_id(hit);
        // Prefer lexical hit details (snippets highlight query terms).
        hit_by_doc_id.insert(doc_id.clone(), hit.clone());
        lexical_scored.push(FsScoredResult {
            doc_id: doc_id.into(),
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

    for (idx, hit) in semantic.iter().enumerate() {
        let doc_id = search_hit_doc_id(hit);
        hit_by_doc_id
            .entry(doc_id.clone())
            .or_insert_with(|| hit.clone());
        semantic_scored.push(FsVectorHit {
            index: u32::try_from(idx).unwrap_or(u32::MAX),
            score: hit.score,
            doc_id: doc_id.into(),
        });
    }

    // Ask frankensearch for full fused ordering so we can preserve cass's
    // content-level deduplication/pagination semantics afterward.
    let fused = fs_rrf_fuse(
        &lexical_scored,
        &semantic_scored,
        total_candidates,
        0,
        &FsRrfConfig::default(),
    );

    // Dedup by (source_id, source_path, conversation_id-or-title, line_number,
    // created_at, content_hash) while preserving RRF order. When a real
    // conversation_id is present, it is the authoritative session key and title
    // drift must not split the same conversation.
    #[derive(Clone, Copy)]
    struct CompatSlot {
        index: usize,
        conversation_id: Option<i64>,
        ambiguous: bool,
    }

    let mut source_ids: HashMap<String, u32> = HashMap::new();
    let mut path_ids: HashMap<String, u32> = HashMap::new();
    let mut title_ids: HashMap<String, u32> = HashMap::new();
    let mut next_source_id: u32 = 0;
    let mut next_path_id: u32 = 0;
    let mut next_title_id: u32 = 0;
    type CompatExactKey = (
        u32,
        u32,
        Option<i64>,
        Option<u32>,
        Option<usize>,
        Option<i64>,
        u64,
    );
    type CompatFallbackKey = (u32, u32, u32, Option<usize>, Option<i64>, u64);

    let mut exact_seen: HashMap<CompatExactKey, usize> = HashMap::with_capacity(fused.len());
    let mut fallback_seen: HashMap<CompatFallbackKey, CompatSlot> =
        HashMap::with_capacity(fused.len());
    let mut unique_hits: Vec<SearchHit> = Vec::with_capacity(fused.len());

    let update_slot = |slot: &mut CompatSlot, conversation_id: Option<i64>| {
        if slot.ambiguous {
            return;
        }
        match (slot.conversation_id, conversation_id) {
            (Some(existing), Some(current)) if existing != current => slot.ambiguous = true,
            (None, Some(current)) => slot.conversation_id = Some(current),
            _ => {}
        }
    };

    for fused_hit in fused {
        let mut hit = match hit_by_doc_id.remove(fused_hit.doc_id.as_str()) {
            Some(hit) => hit,
            None => continue,
        };
        if hit_is_noise(&hit, query) {
            continue;
        }

        let normalized_source_id = normalized_search_hit_source_id(&hit);
        let source_key = if let Some(id) = source_ids.get(normalized_source_id.as_str()) {
            *id
        } else {
            let id = next_source_id;
            next_source_id = next_source_id.saturating_add(1);
            source_ids.insert(normalized_source_id, id);
            id
        };
        let path_key = if let Some(id) = path_ids.get(hit.source_path.as_str()) {
            *id
        } else {
            let id = next_path_id;
            next_path_id = next_path_id.saturating_add(1);
            path_ids.insert(hit.source_path.clone(), id);
            id
        };
        let normalized_title = hit.title.trim();
        let fallback_title_key = if let Some(id) = title_ids.get(normalized_title) {
            *id
        } else {
            let id = next_title_id;
            next_title_id = next_title_id.saturating_add(1);
            title_ids.insert(normalized_title.to_string(), id);
            id
        };
        let exact_title_key = if hit.conversation_id.is_some() {
            None
        } else {
            Some(fallback_title_key)
        };
        let exact_key = (
            source_key,
            path_key,
            hit.conversation_id,
            exact_title_key,
            hit.line_number,
            hit.created_at,
            hit.content_hash,
        );
        let fallback_key = (
            source_key,
            path_key,
            fallback_title_key,
            hit.line_number,
            hit.created_at,
            hit.content_hash,
        );

        let merged_idx = exact_seen.get(&exact_key).copied().or_else(|| {
            fallback_seen.get(&fallback_key).and_then(|slot| {
                if slot.ambiguous {
                    return None;
                }
                match (slot.conversation_id, hit.conversation_id) {
                    (Some(existing), Some(current)) if existing != current => None,
                    _ => Some(slot.index),
                }
            })
        });

        if let Some(existing_idx) = merged_idx {
            exact_seen.insert(exact_key, existing_idx);
            let slot = fallback_seen.entry(fallback_key).or_insert(CompatSlot {
                index: existing_idx,
                conversation_id: hit.conversation_id,
                ambiguous: false,
            });
            update_slot(slot, hit.conversation_id);
            if unique_hits[existing_idx].conversation_id.is_none() && hit.conversation_id.is_some()
            {
                unique_hits[existing_idx].conversation_id = hit.conversation_id;
            }
            unique_hits[existing_idx].score += fused_hit.rrf_score as f32;
            continue;
        }

        hit.score = fused_hit.rrf_score as f32;
        let index = unique_hits.len();
        unique_hits.push(hit);
        exact_seen.insert(exact_key, index);
        match fallback_seen.get_mut(&fallback_key) {
            Some(slot) => update_slot(slot, unique_hits[index].conversation_id),
            None => {
                fallback_seen.insert(
                    fallback_key,
                    CompatSlot {
                        index,
                        conversation_id: unique_hits[index].conversation_id,
                        ambiguous: false,
                    },
                );
            }
        }
    }

    unique_hits.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then_with(|| SearchHitKey::from_hit(a).cmp(&SearchHitKey::from_hit(b)))
    });

    let start = offset.min(unique_hits.len());
    unique_hits.into_iter().skip(start).take(limit).collect()
}

/// Check if content is primarily a tool invocation (noise that shouldn't appear in search results).
/// Tool invocations like "[Tool: Bash - Check status]" are not informative search results.
pub(crate) fn is_tool_invocation_noise(content: &str) -> bool {
    let trimmed = content.trim();

    // Direct tool invocations that are just "[Tool: X - description]" or "[Tool: X] args"
    if trimmed.starts_with("[Tool:") {
        // Find closing bracket
        if let Some(close_idx) = trimmed.find(']') {
            // Check for content after closing bracket (Pi-Agent style: "[Tool: name] args")
            let after = &trimmed[close_idx + 1..];
            if !after.trim().is_empty() {
                return false; // Has args/content after -> Keep
            }

            // No content after bracket. Check for description inside.
            // Format: "[Tool: Name - Desc]" (useful) vs "[Tool: Name]" (previously noise, now kept)
            // We now keep "[Tool: Name]" because users might search for "Tool: Bash" to find usage.
            // Only "[Tool:]" or "[Tool: ]" (empty name) is considered noise.
            let inner = &trimmed[6..close_idx]; // Skip "[Tool:"
            return inner.trim().is_empty();
        }
        // No closing bracket? Malformed, treat as noise
        return true;
    }

    // Also filter very short content that's just tool names or markers
    if trimmed.len() < 20 {
        let lower = trimmed.to_lowercase();
        if lower.starts_with("[tool") || lower.starts_with("tool:") {
            return true;
        }
    }

    false
}

pub(in crate::search::query) fn hit_content_for_noise_check(hit: &SearchHit) -> &str {
    if hit.content.is_empty() {
        &hit.snippet
    } else {
        &hit.content
    }
}

pub(in crate::search::query) fn hit_is_noise(hit: &SearchHit, query: &str) -> bool {
    let content_to_check = hit_content_for_noise_check(hit);
    // When both `content` and `snippet` are empty, it usually means the caller
    // explicitly asked for a projection (`--fields minimal` / `summary`) that
    // excludes both fields — NOT that the underlying row was empty. Treating
    // the hit as noise in that case silently drops every real match and makes
    // `cass search --fields minimal` return zero results even when matches
    // exist (reality-check bead q6xf9). The noise classifier cannot make a
    // correctness-preserving decision without text to inspect, so default to
    // "not noise" in that case and let the hit through; downstream projection
    // will apply the requested field subset.
    if content_to_check.is_empty() {
        return false;
    }
    is_search_noise_text(content_to_check, query) || is_tool_invocation_noise(content_to_check)
}

pub(in crate::search::query) fn snippet_from_content(content: &str) -> String {
    let trimmed = content.trim();
    let mut chars = trimmed.chars();
    let preview: String = chars.by_ref().take(200).collect();
    if chars.next().is_some() {
        format!("{preview}...")
    } else {
        preview
    }
}

/// Build a bounded snippet from `messages.content` with `**...**` match
/// highlighting for the query terms extracted by [`snippet_terms_for_query`].
///
/// Falls back to [`snippet_from_content`] when the query carries no highlightable
/// terms or none appear verbatim in `content`.
pub(in crate::search::query) fn snippet_from_content_for_query(
    content: &str,
    query: &str,
) -> String {
    const MAX_WINDOW_CHARS: usize = 200;
    const LEAD_CONTEXT_CHARS: usize = 30;

    let trimmed = content.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    let raw_terms = snippet_terms_for_query(query);
    let terms: Vec<Vec<char>> = raw_terms
        .into_iter()
        .map(|term| term.trim_end_matches('*').to_lowercase())
        .filter(|term| !term.is_empty())
        .map(|term| term.chars().collect::<Vec<char>>())
        .collect();
    if terms.is_empty() {
        return snippet_from_content(content);
    }

    let orig_chars: Vec<char> = trimmed.chars().collect();
    let mut lower_chars: Vec<char> = Vec::with_capacity(orig_chars.len());
    let mut lower_to_orig_char: Vec<usize> = Vec::with_capacity(orig_chars.len());
    for (orig_idx, &ch) in orig_chars.iter().enumerate() {
        for lc in ch.to_lowercase() {
            lower_chars.push(lc);
            lower_to_orig_char.push(orig_idx);
        }
    }

    // Collect non-overlapping (start_orig_char, end_orig_char) match spans.
    let mut match_spans: Vec<(usize, usize)> = Vec::new();
    let mut lower_pos = 0usize;
    while lower_pos < lower_chars.len() {
        let best_len = terms
            .iter()
            .filter(|term| {
                lower_pos + term.len() <= lower_chars.len()
                    && lower_chars[lower_pos..lower_pos + term.len()] == term[..]
            })
            .map(Vec::len)
            .max();
        if let Some(len) = best_len {
            let start_orig = lower_to_orig_char[lower_pos];
            let end_orig = lower_to_orig_char[lower_pos + len - 1] + 1;
            if match_spans
                .last()
                .is_none_or(|&(_, prev_end)| start_orig >= prev_end)
            {
                match_spans.push((start_orig, end_orig));
            }
            lower_pos += len;
        } else {
            lower_pos += 1;
        }
    }

    let Some(&(first_start, _)) = match_spans.first() else {
        return snippet_from_content(content);
    };

    let window_start = if first_start <= LEAD_CONTEXT_CHARS + 10 {
        0
    } else {
        first_start.saturating_sub(LEAD_CONTEXT_CHARS)
    };
    let window_end = (window_start + MAX_WINDOW_CHARS).min(orig_chars.len());

    let mut out = String::new();
    if window_start > 0 {
        out.push_str("...");
    }
    let mut cursor = window_start;
    for &(span_start, span_end) in &match_spans {
        if span_end <= window_start || span_start >= window_end {
            continue;
        }
        let clipped_start = span_start.max(window_start);
        let clipped_end = span_end.min(window_end);
        if cursor < clipped_start {
            out.extend(&orig_chars[cursor..clipped_start]);
        }
        if clipped_start < clipped_end {
            out.push_str("**");
            out.extend(&orig_chars[clipped_start..clipped_end]);
            out.push_str("**");
        }
        cursor = clipped_end;
    }
    if cursor < window_end {
        out.extend(&orig_chars[cursor..window_end]);
    }
    if window_end < orig_chars.len() {
        out.push_str("...");
    }
    out
}

/// Deduplicate search hits by message-level provenance and content, keeping
/// only the highest-scored hit for each unique matched message.
///
/// This respects source boundaries (P2.3): the same content from different sources
/// appears as separate results, since they represent distinct conversations.
///
/// Also filters out tool invocation noise that isn't useful for search results.
#[cfg(test)]
pub(crate) fn deduplicate_hits(hits: Vec<SearchHit>) -> Vec<SearchHit> {
    deduplicate_hits_with_query(hits, "")
}

pub(crate) fn deduplicate_hits_with_query(hits: Vec<SearchHit>, query: &str) -> Vec<SearchHit> {
    // Key: (source_numeric_id, source_path_numeric_id, conversation_id-or-title,
    //       line_number, created_at, content_hash) -> index in deduped.
    // Include message-level identity so repeated identical content in the same
    // session remains visible as distinct hits when it came from different messages.
    // When conversation_id exists, it is authoritative and title drift must not
    // split or merge hits incorrectly.
    let mut source_ids: HashMap<String, u32> = HashMap::new();
    let mut path_ids: HashMap<String, u32> = HashMap::new();
    let mut title_ids: HashMap<String, u32> = HashMap::new();
    let mut next_source_id: u32 = 0;
    let mut next_path_id: u32 = 0;
    let mut next_title_id: u32 = 0;
    type DedupKey = (
        u32,
        u32,
        Option<i64>,
        Option<u32>,
        Option<usize>,
        Option<i64>,
        u64,
    );

    let mut seen: HashMap<DedupKey, usize> = HashMap::new();
    let mut deduped: Vec<SearchHit> = Vec::new();

    for hit in hits {
        if hit_is_noise(&hit, query) {
            continue;
        }

        // Include normalized source identity AND source_path in the key so different
        // sessions keep their results while local provenance drift still coalesces.
        let normalized_source_id = normalized_search_hit_source_id(&hit);
        let source_key = if let Some(id) = source_ids.get(normalized_source_id.as_str()) {
            *id
        } else {
            let id = next_source_id;
            next_source_id = next_source_id.saturating_add(1);
            source_ids.insert(normalized_source_id, id);
            id
        };
        let path_key = if let Some(id) = path_ids.get(hit.source_path.as_str()) {
            *id
        } else {
            let id = next_path_id;
            next_path_id = next_path_id.saturating_add(1);
            path_ids.insert(hit.source_path.clone(), id);
            id
        };
        let title_key = if hit.conversation_id.is_some() {
            None
        } else {
            let normalized_title = hit.title.trim();
            Some(if let Some(id) = title_ids.get(normalized_title) {
                *id
            } else {
                let id = next_title_id;
                next_title_id = next_title_id.saturating_add(1);
                title_ids.insert(normalized_title.to_string(), id);
                id
            })
        };
        let key = (
            source_key,
            path_key,
            hit.conversation_id,
            title_key,
            hit.line_number,
            hit.created_at,
            hit.content_hash,
        );

        if let Some(&existing_idx) = seen.get(&key) {
            // If existing hit has lower score, replace it
            if deduped[existing_idx].score < hit.score {
                deduped[existing_idx] = hit;
            }
            // Otherwise keep existing (higher score)
        } else {
            seen.insert(key, deduped.len());
            deduped.push(hit);
        }
    }

    deduped
}

/// `--source remote` is selected on hydrated hits rather than in the engine
/// clause (see the lexical filter mapping in `search_tantivy`): a hit is
/// remote iff its normalized `origin_kind` is not the local kind.
pub(in crate::search::query) fn remote_source_filter_is_post_applied(
    filter: &SourceFilter,
) -> bool {
    matches!(filter, SourceFilter::Remote)
}

/// Local fix (review 2026-11-02, Q1). `session_paths` is matched against the
/// stored (not indexed) `source_path` after hydration, exactly like the
/// remote filter, so the engine's hit count covers sessions outside the scope.
pub(in crate::search::query) fn session_paths_filter_is_post_applied(
    session_paths: &HashSet<String>,
) -> bool {
    !session_paths.is_empty()
}

/// Production CASS result reducer shared by lexical, semantic, progressive,
/// and feature-gated conformance callers.
pub(in crate::search::query) fn postprocess_hits_page_core(
    hits: Vec<SearchHit>,
    query: &str,
    session_paths: &HashSet<String>,
    limit: usize,
    offset: usize,
) -> (usize, Vec<SearchHit>) {
    let mut hits = deduplicate_hits_with_query(hits, query);
    if !session_paths.is_empty() {
        hits.retain(|hit| session_paths.contains(&hit.source_path));
    }
    let available_hits = hits.len();
    let paged_hits = hits.into_iter().skip(offset).take(limit).collect();
    (available_hits, paged_hits)
}

pub(in crate::search::query) fn should_try_wildcard_fallback(
    returned_hits: usize,
    limit: usize,
    offset: usize,
    sparse_threshold: usize,
) -> bool {
    if offset != 0 {
        return false;
    }

    let effective_sparse_threshold = if limit == 0 {
        sparse_threshold
    } else {
        sparse_threshold.min(limit)
    };

    returned_hits < effective_sparse_threshold
}

pub(in crate::search::query) fn should_skip_automatic_wildcard_fallback_for_long_zero_hit_query(
    query: &str,
    returned_hits: usize,
) -> bool {
    if returned_hits != 0 {
        return false;
    }

    for token in normalize_phrase_terms(query) {
        if token.chars().count() > AUTOMATIC_WILDCARD_FALLBACK_MAX_TOKEN_CHARS {
            return true;
        }
    }

    false
}

/// Words from `query` to highlight in a snippet.
///
/// The generator analyzes the SOURCE text and looks each token up in this set,
/// so a term that never occurs in the source is inert — a stray boolean
/// operator costs nothing but a map lookup. What is NOT inert is a
/// `field:value` token: matched whole it would highlight nothing, so the value
/// half is taken. Quoted phrases contribute their words individually, matching
/// what the incumbent's snippet did.
pub(in crate::search::query) fn snippet_terms_for_query(query: &str) -> Vec<String> {
    query
        .split_whitespace()
        .map(|word| word.trim_matches(|c: char| c == '"' || c == '\''))
        .filter(|word| !matches!(*word, "AND" | "OR" | "NOT"))
        .map(|word| word.rsplit(':').next().unwrap_or(word))
        .filter(|word| !word.is_empty())
        .map(str::to_owned)
        .collect()
}

pub(in crate::search::query) fn snippet_from_preview_without_full_content(
    field_mask: FieldMask,
    stored_preview: &str,
    query: &str,
) -> Option<String> {
    if field_mask.needs_content() || !field_mask.wants_snippet() || stored_preview.is_empty() {
        return None;
    }

    cached_prefix_snippet(stored_preview, query, 160)
}

pub(in crate::search::query) fn stored_preview_is_complete_content(stored_preview: &str) -> bool {
    // The preview builder appends U+2026 only when truncating. A real message
    // ending with that character becomes a conservative false negative here.
    !stored_preview.is_empty() && !stored_preview.ends_with('…')
}
