use anyhow::{Result, anyhow};
use crossbeam_channel as mpsc;
use lru::LruCache;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet, VecDeque};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::search::embedder::Embedder;
use crate::sources::provenance::SourceFilter;

use super::SearchClient;
use super::types::{FieldMask, SearchFilters, SearchHit, available_memory_bytes, intern_cache_key};

pub(in crate::search::query) struct QueryCache {
    pub(in crate::search::query) embedder_id: String,
    pub(in crate::search::query) embeddings: LruCache<String, Vec<f32>>,
}

impl QueryCache {
    pub(in crate::search::query) fn new(embedder_id: &str, capacity: NonZeroUsize) -> Self {
        Self {
            embedder_id: embedder_id.to_string(),
            embeddings: LruCache::new(capacity),
        }
    }

    pub(in crate::search::query) fn align_embedder(&mut self, embedder: &dyn Embedder) {
        if self.embedder_id != embedder.id() {
            self.embedder_id = embedder.id().to_string();
            self.embeddings.clear();
        }
    }

    pub(in crate::search::query) fn get_cached(
        &mut self,
        embedder: &dyn Embedder,
        canonical: &str,
    ) -> Option<Vec<f32>> {
        self.align_embedder(embedder);
        self.embeddings.get(canonical).cloned()
    }

    pub(in crate::search::query) fn store(
        &mut self,
        embedder: &dyn Embedder,
        canonical: &str,
        embedding: Vec<f32>,
    ) {
        self.align_embedder(embedder);
        self.embeddings.put(canonical.to_string(), embedding);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheStats {
    pub cache_hits: u64,
    pub cache_miss: u64,
    pub cache_shortfall: u64,
    /// Outcome taxonomy for generation-aware searcher/cache behavior. Unlike
    /// the legacy aggregate fields, this keeps forced reloads, stale
    /// generations, reload failures, and degraded fallback distinct.
    pub searcher_cache: crate::daemon_runtime_state::SearcherCacheMetrics,
    pub reloads: u64,
    pub reload_ms_total: u128,
    pub total_cap: usize,
    pub total_cost: usize,
    /// Total evictions since client creation
    pub eviction_count: u64,
    /// Approximate bytes used by cache (rough estimate)
    pub approx_bytes: usize,
    /// Effective byte cap for cached hits (0 = disabled by explicit operator override)
    pub byte_cap: usize,
    /// Active eviction/admission policy for prefix result cache
    pub eviction_policy: &'static str,
    /// Number of S3-FIFO ghost entries retained for adaptive admission
    pub ghost_entries: usize,
    /// Number of cache insertions rejected by adaptive admission
    pub admission_rejects: u64,
    /// Number of adaptive query prewarm jobs scheduled from hot prefix-cache state.
    pub prewarm_scheduled: u64,
    /// Number of adaptive query prewarm jobs skipped because cache pressure was high.
    pub prewarm_skipped_pressure: u64,
    /// Last observed Tantivy reader generation signature for cursor continuity metadata.
    pub reader_generation: Option<u64>,
}

impl Default for CacheStats {
    fn default() -> Self {
        Self {
            cache_hits: 0,
            cache_miss: 0,
            cache_shortfall: 0,
            searcher_cache: crate::daemon_runtime_state::SearcherCacheMetrics::default(),
            reloads: 0,
            reload_ms_total: 0,
            total_cap: 0,
            total_cost: 0,
            eviction_count: 0,
            approx_bytes: 0,
            byte_cap: 0,
            eviction_policy: "unknown",
            ghost_entries: 0,
            admission_rejects: 0,
            prewarm_scheduled: 0,
            prewarm_skipped_pressure: 0,
            reader_generation: None,
        }
    }
}

// Cache tuning: read from env to allow runtime override without recompiling.
// CASS_CACHE_SHARD_CAP controls per-shard entries; default 256.
pub(in crate::search::query) static CACHE_SHARD_CAP: Lazy<usize> = Lazy::new(|| {
    dotenvy::var("CASS_CACHE_SHARD_CAP")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(256)
});

// Total cache cost across all shards; approximate "~2k entries" default.
pub(in crate::search::query) static CACHE_TOTAL_CAP: Lazy<usize> = Lazy::new(|| {
    dotenvy::var("CASS_CACHE_TOTAL_CAP")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(2048)
});

pub(in crate::search::query) static CACHE_DEBUG_ENABLED: Lazy<bool> = Lazy::new(|| {
    dotenvy::var("CASS_DEBUG_CACHE_METRICS")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
});

// Byte-based cap for cache memory. Unset defaults to a memory-proportional cap;
// explicit CASS_CACHE_BYTE_CAP=0 disables the byte guard.
pub(in crate::search::query) static CACHE_BYTE_CAP: Lazy<usize> =
    Lazy::new(|| match dotenvy::var("CASS_CACHE_BYTE_CAP") {
        Ok(value) => cache_byte_cap_from_env_value(Some(&value), available_memory_bytes()),
        Err(_) => default_cache_byte_cap(),
    });

pub(in crate::search::query) static CACHE_EVICTION_POLICY: Lazy<CacheEvictionPolicy> =
    Lazy::new(|| {
        cache_eviction_policy_from_env_value(
            dotenvy::var("CASS_CACHE_EVICTION_POLICY").ok().as_deref(),
        )
    });

pub(in crate::search::query) const DEFAULT_CACHE_BYTE_CAP_FALLBACK: usize = 64 * 1024 * 1024;
pub(in crate::search::query) const DEFAULT_CACHE_BYTE_CAP_MEMORY_FRACTION_DENOMINATOR: u64 = 128;
pub(in crate::search::query) const DEFAULT_CACHE_BYTE_CAP_CEILING: u64 = 2 * 1024 * 1024 * 1024;
pub(in crate::search::query) const S3_FIFO_GHOST_CAP_MULTIPLIER: usize = 2;
pub(in crate::search::query) const S3_FIFO_LARGE_ENTRY_FRACTION_DENOMINATOR: usize = 4;
pub(in crate::search::query) const PREWARM_ENTRY_PRESSURE_NUMERATOR: usize = 9;
pub(in crate::search::query) const PREWARM_ENTRY_PRESSURE_DENOMINATOR: usize = 10;
pub(in crate::search::query) const PREWARM_BYTE_PRESSURE_NUMERATOR: usize = 4;
pub(in crate::search::query) const PREWARM_BYTE_PRESSURE_DENOMINATOR: usize = 5;

pub(in crate::search::query) const CACHE_KEY_VERSION: &str = "1";

// Warm debounce (ms) for background reload/warm jobs; default 120ms.
pub(in crate::search::query) static WARM_DEBOUNCE_MS: Lazy<u64> = Lazy::new(|| {
    dotenvy::var("CASS_WARM_DEBOUNCE_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(120)
});

pub(in crate::search::query) fn default_cache_byte_cap() -> usize {
    default_cache_byte_cap_for_available(available_memory_bytes())
}

pub(in crate::search::query) fn cache_byte_cap_from_env_value(
    value: Option<&str>,
    available_bytes: Option<u64>,
) -> usize {
    let Some(raw) = value else {
        return default_cache_byte_cap_for_available(available_bytes);
    };
    raw.parse::<usize>()
        .unwrap_or_else(|_| default_cache_byte_cap_for_available(available_bytes))
}

pub(in crate::search::query) fn default_cache_byte_cap_for_available(
    available_bytes: Option<u64>,
) -> usize {
    let Some(available_bytes) = available_bytes else {
        return DEFAULT_CACHE_BYTE_CAP_FALLBACK;
    };
    let ceiling = usize::try_from(DEFAULT_CACHE_BYTE_CAP_CEILING).unwrap_or(usize::MAX);
    let budget = available_bytes / DEFAULT_CACHE_BYTE_CAP_MEMORY_FRACTION_DENOMINATOR;
    let budget = budget.min(DEFAULT_CACHE_BYTE_CAP_CEILING);
    let budget = usize::try_from(budget).unwrap_or(ceiling);
    budget.clamp(DEFAULT_CACHE_BYTE_CAP_FALLBACK, ceiling)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::search::query) enum CacheEvictionPolicy {
    Lru,
    S3Fifo,
}

impl CacheEvictionPolicy {
    pub(in crate::search::query) fn label(self) -> &'static str {
        match self {
            CacheEvictionPolicy::Lru => "lru",
            CacheEvictionPolicy::S3Fifo => "s3-fifo",
        }
    }
}

pub(in crate::search::query) fn cache_eviction_policy_from_env_value(
    value: Option<&str>,
) -> CacheEvictionPolicy {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        Some(value) if value.eq_ignore_ascii_case("s3-fifo") => CacheEvictionPolicy::S3Fifo,
        Some(value) if value.eq_ignore_ascii_case("s3fifo") => CacheEvictionPolicy::S3Fifo,
        Some(value) if value.eq_ignore_ascii_case("s3_fifo") => CacheEvictionPolicy::S3Fifo,
        _ => CacheEvictionPolicy::Lru,
    }
}

#[derive(Clone)]
pub(in crate::search::query) struct CachedHit {
    pub(in crate::search::query) hit: SearchHit,
}

impl CachedHit {
    /// Approximate byte size of this cached hit (rough estimate for memory guardrails).
    /// Includes the `SearchHit` and its owned strings.
    pub(in crate::search::query) fn approx_bytes(&self) -> usize {
        // Base struct overhead
        let base = std::mem::size_of::<Self>();
        // SearchHit string fields (title, snippet, content, source_path, agent, workspace)
        let hit_strings = self.hit.title.len()
            + self.hit.snippet.len()
            + self.hit.content.len()
            + self.hit.source_path.len()
            + self.hit.agent.len()
            + self.hit.workspace.len()
            + self
                .hit
                .workspace_original
                .as_ref()
                .map_or(0, std::string::String::len)
            + self.hit.source_id.len()
            + self.hit.origin_kind.len()
            + self
                .hit
                .origin_host
                .as_ref()
                .map_or(0, std::string::String::len);
        base + hit_strings
    }
}

pub(in crate::search::query) struct CacheShards {
    // Optimization 2.3: Use Arc<str> for cache keys to reduce memory via interning
    pub(in crate::search::query) shards: HashMap<Arc<str>, LruCache<Arc<str>, Vec<CachedHit>>>,
    pub(in crate::search::query) total_cap: usize,
    pub(in crate::search::query) total_cost: usize,
    /// Running count of evictions (for diagnostics)
    pub(in crate::search::query) eviction_count: u64,
    /// Approximate bytes used by all cached hits
    pub(in crate::search::query) total_bytes: usize,
    /// Byte cap (0 = disabled)
    pub(in crate::search::query) byte_cap: usize,
    /// Active cache admission/eviction policy.
    pub(in crate::search::query) policy: CacheEvictionPolicy,
    /// Ghost queue used by S3-FIFO-style adaptive admission.
    pub(in crate::search::query) ghost_keys: VecDeque<Arc<str>>,
    pub(in crate::search::query) ghost_set: HashSet<Arc<str>>,
    pub(in crate::search::query) admission_rejects: u64,
}

impl CacheShards {
    pub(in crate::search::query) fn new(total_cap: usize, byte_cap: usize) -> Self {
        Self::new_with_policy(total_cap, byte_cap, *CACHE_EVICTION_POLICY)
    }

    pub(in crate::search::query) fn new_with_policy(
        total_cap: usize,
        byte_cap: usize,
        policy: CacheEvictionPolicy,
    ) -> Self {
        Self {
            shards: HashMap::new(),
            total_cap: total_cap.max(1),
            total_cost: 0,
            eviction_count: 0,
            total_bytes: 0,
            byte_cap,
            policy,
            ghost_keys: VecDeque::new(),
            ghost_set: HashSet::new(),
            admission_rejects: 0,
        }
    }

    pub(in crate::search::query) fn shard_mut(
        &mut self,
        name: &str,
    ) -> &mut LruCache<Arc<str>, Vec<CachedHit>> {
        // Use interned shard names to reduce memory for repeated lookups
        let interned_name = intern_cache_key(name);
        self.shards
            .entry(interned_name)
            .or_insert_with(|| LruCache::new(NonZeroUsize::new(*CACHE_SHARD_CAP).unwrap()))
    }

    pub(in crate::search::query) fn shard_opt(
        &self,
        name: &str,
    ) -> Option<&LruCache<Arc<str>, Vec<CachedHit>>> {
        // HashMap<Arc<str>, _> can be queried with &str via Borrow trait
        self.shards.get(name)
    }

    pub(in crate::search::query) fn put(
        &mut self,
        shard_name: &str,
        key: Arc<str>,
        value: Vec<CachedHit>,
    ) {
        let new_cost = value.len();
        let new_bytes: usize = value.iter().map(CachedHit::approx_bytes).sum();
        let replacing = self
            .shard_opt(shard_name)
            .is_some_and(|shard| shard.contains(&key));

        if !replacing && !self.should_admit(&key, new_cost, new_bytes) {
            self.admission_rejects += 1;
            self.record_ghost(key);
            return;
        }

        self.remove_ghost(&key);

        let shard = self.shard_mut(shard_name);
        let old_val = shard.put(key, value);
        let (old_cost, old_bytes) = old_val.as_ref().map_or((0, 0), |v| {
            (v.len(), v.iter().map(CachedHit::approx_bytes).sum())
        });

        self.total_cost = self
            .total_cost
            .saturating_add(new_cost)
            .saturating_sub(old_cost);
        self.total_bytes = self
            .total_bytes
            .saturating_add(new_bytes)
            .saturating_sub(old_bytes);
        self.evict_until_within_cap();
    }

    pub(in crate::search::query) fn evict_until_within_cap(&mut self) {
        // Evict if over entry cap OR over byte cap (when byte_cap > 0)
        while self.total_cost > self.total_cap
            || (self.byte_cap > 0 && self.total_bytes > self.byte_cap)
        {
            // Under byte pressure, target the byte-heaviest shard. Otherwise,
            // target the shard with the most cached items. This avoids
            // evicting many small useful entries before a single oversized
            // result set is finally removed.
            let byte_pressure = self.byte_cap > 0 && self.total_bytes > self.byte_cap;
            let mut largest_shard_key = None;
            let mut max_score = 0usize;
            for (k, v) in self.shards.iter() {
                let score = if byte_pressure {
                    shard_cached_bytes(v)
                } else {
                    v.len()
                };
                if score > max_score {
                    max_score = score;
                    largest_shard_key = Some(k.clone());
                }
            }

            if let Some(key) = largest_shard_key {
                if let Some(shard) = self.shards.get_mut(&key)
                    && let Some((evicted_key, v)) = shard.pop_lru()
                {
                    let evicted_bytes: usize = v.iter().map(CachedHit::approx_bytes).sum();
                    self.total_cost = self.total_cost.saturating_sub(v.len());
                    self.total_bytes = self.total_bytes.saturating_sub(evicted_bytes);
                    self.eviction_count += 1;
                    self.record_ghost(evicted_key);
                }
            } else {
                break; // All shards are empty
            }
        }
    }

    pub(in crate::search::query) fn should_admit(
        &self,
        key: &Arc<str>,
        cost: usize,
        bytes: usize,
    ) -> bool {
        if self.policy == CacheEvictionPolicy::Lru || self.ghost_set.contains(key) {
            return true;
        }
        !self.is_s3_fifo_large_candidate(cost, bytes)
    }

    pub(in crate::search::query) fn is_s3_fifo_large_candidate(
        &self,
        cost: usize,
        bytes: usize,
    ) -> bool {
        let entry_heavy = cost
            > self
                .total_cap
                .div_ceil(S3_FIFO_LARGE_ENTRY_FRACTION_DENOMINATOR);
        let byte_heavy = self.byte_cap > 0
            && bytes
                > self
                    .byte_cap
                    .div_ceil(S3_FIFO_LARGE_ENTRY_FRACTION_DENOMINATOR);
        entry_heavy || byte_heavy
    }

    pub(in crate::search::query) fn record_ghost(&mut self, key: Arc<str>) {
        if self.policy != CacheEvictionPolicy::S3Fifo {
            return;
        }
        if self.ghost_set.insert(key.clone()) {
            self.ghost_keys.push_back(key);
        }
        let cap = self
            .total_cap
            .saturating_mul(S3_FIFO_GHOST_CAP_MULTIPLIER)
            .max(1);
        while self.ghost_set.len() > cap {
            if let Some(old) = self.ghost_keys.pop_front() {
                self.ghost_set.remove(&old);
            } else {
                break;
            }
        }
    }

    pub(in crate::search::query) fn remove_ghost(&mut self, key: &Arc<str>) {
        self.ghost_set.remove(key);
        self.ghost_keys.retain(|candidate| candidate != key);
    }

    pub(in crate::search::query) fn clear(&mut self) {
        self.shards.clear();
        self.total_cost = 0;
        self.total_bytes = 0;
        self.ghost_keys.clear();
        self.ghost_set.clear();
        // Note: eviction_count preserved for lifetime stats
    }

    pub(in crate::search::query) fn total_cost(&self) -> usize {
        self.total_cost
    }

    pub(in crate::search::query) fn total_cap(&self) -> usize {
        self.total_cap
    }

    pub(in crate::search::query) fn eviction_count(&self) -> u64 {
        self.eviction_count
    }

    pub(in crate::search::query) fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    pub(in crate::search::query) fn byte_cap(&self) -> usize {
        self.byte_cap
    }

    pub(in crate::search::query) fn policy_label(&self) -> &'static str {
        self.policy.label()
    }

    pub(in crate::search::query) fn ghost_entries(&self) -> usize {
        self.ghost_set.len()
    }

    pub(in crate::search::query) fn admission_rejects(&self) -> u64 {
        self.admission_rejects
    }

    pub(in crate::search::query) fn prewarm_pressure(&self) -> bool {
        let entry_pressure = self
            .total_cost
            .saturating_mul(PREWARM_ENTRY_PRESSURE_DENOMINATOR)
            >= self
                .total_cap
                .saturating_mul(PREWARM_ENTRY_PRESSURE_NUMERATOR);
        let byte_pressure = self.byte_cap > 0
            && self
                .total_bytes
                .saturating_mul(PREWARM_BYTE_PRESSURE_DENOMINATOR)
                >= self
                    .byte_cap
                    .saturating_mul(PREWARM_BYTE_PRESSURE_NUMERATOR);
        entry_pressure || byte_pressure
    }
}

pub(in crate::search::query) fn shard_cached_bytes(
    shard: &LruCache<Arc<str>, Vec<CachedHit>>,
) -> usize {
    shard
        .iter()
        .map(|(_key, hits)| hits.iter().map(CachedHit::approx_bytes).sum::<usize>())
        .sum()
}

#[derive(Clone)]
pub(in crate::search::query) struct WarmJob {
    pub(in crate::search::query) query: String,
    pub(in crate::search::query) filters_fingerprint: String,
    pub(in crate::search::query) shard_name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::search::query) enum AdaptivePrewarmDecision {
    Schedule,
    SkipCold,
    SkipPressure,
}

#[derive(Clone)]
pub(in crate::search::query) struct FederatedIndexReader {
    pub(in crate::search::query) reader: frankensearch::quill::QuillSearchIndex,
    pub(in crate::search::query) fields: crate::search::quill_bridge::QuillCassFields,
}

pub(in crate::search::query) static FEDERATED_SEARCH_READERS: Lazy<
    RwLock<HashMap<String, Arc<Vec<FederatedIndexReader>>>>,
> = Lazy::new(|| RwLock::new(HashMap::new()));

/// The single-directory lexical reader and the path it was opened from.
///
/// A lexical rebuild publishes a new index lineage by exchanging the whole
/// directory, which in-place catch-up cannot follow. The slot lets a
/// reloading client (the TUI) open the path again and swap the reader in
/// instead of failing every later search.
#[derive(Default)]
pub(in crate::search::query) struct LexicalReaderSlot {
    pub(in crate::search::query) index_path: Option<PathBuf>,
    pub(in crate::search::query) current: RwLock<
        Option<(
            frankensearch::quill::QuillSearchIndex,
            crate::search::quill_bridge::QuillCassFields,
        )>,
    >,
}

impl LexicalReaderSlot {
    pub(in crate::search::query) fn new(
        index_path: PathBuf,
        reader: Option<(
            frankensearch::quill::QuillSearchIndex,
            crate::search::quill_bridge::QuillCassFields,
        )>,
    ) -> Self {
        Self {
            index_path: Some(index_path),
            current: RwLock::new(reader),
        }
    }

    pub(in crate::search::query) fn get(
        &self,
    ) -> Option<(
        frankensearch::quill::QuillSearchIndex,
        crate::search::quill_bridge::QuillCassFields,
    )> {
        self.current.read().clone()
    }

    pub(in crate::search::query) fn is_some(&self) -> bool {
        self.current.read().is_some()
    }
}
pub(in crate::search::query) static SEARCH_CLIENT_INSTANCE_COUNTER: AtomicU64 = AtomicU64::new(1);

pub(in crate::search::query) static SEARCHER_RELOAD_TIMEOUT: Lazy<Duration> = Lazy::new(|| {
    dotenvy::var("CASS_SEARCHER_RELOAD_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_secs(1))
});

pub(in crate::search::query) struct ReloadInFlightGuard(Arc<AtomicBool>);

impl Drop for ReloadInFlightGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Refresh `readers` in place on a bounded worker. `Ok(true)` when a
/// directory was republished as a new index lineage and must be opened again
/// (see [`crate::search::quill_bridge::refresh_reader_or_detect_republish`]).
pub(in crate::search::query) fn reload_index_readers_bounded(
    readers: Vec<frankensearch::quill::QuillSearchIndex>,
    reload_in_flight: Arc<AtomicBool>,
) -> Result<bool> {
    let republished = Arc::new(AtomicBool::new(false));
    let worker_republished = Arc::clone(&republished);
    run_reload_bounded(
        move || {
            for reader in &readers {
                match crate::search::quill_bridge::refresh_reader_or_detect_republish(reader) {
                    Ok(None) => {}
                    Ok(Some(reason)) => {
                        tracing::info!(
                            index = %reader.path().display(),
                            reason,
                            "lexical index was republished; reopening it"
                        );
                        worker_republished.store(true, Ordering::SeqCst);
                    }
                    Err(error) => return Err(error.to_string()),
                }
            }
            Ok(())
        },
        reload_in_flight,
        *SEARCHER_RELOAD_TIMEOUT,
    )?;
    Ok(republished.load(Ordering::SeqCst))
}

pub(in crate::search::query) fn run_reload_bounded<F>(
    reload: F,
    reload_in_flight: Arc<AtomicBool>,
    timeout: Duration,
) -> Result<()>
where
    F: FnOnce() -> std::result::Result<(), String> + Send + 'static,
{
    reload_in_flight
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .map_err(|_| anyhow!("a bounded searcher reload is already in flight"))?;
    let (tx, rx) = mpsc::bounded(1);
    let worker_reload_in_flight = Arc::clone(&reload_in_flight);
    if let Err(error) = std::thread::Builder::new()
        .name("cass-searcher-reload".to_string())
        .spawn(move || {
            let in_flight = ReloadInFlightGuard(worker_reload_in_flight);
            let result = reload();
            // Publish completion only after clearing the single-flight flag.
            // Otherwise the receiver can return success, immediately start a
            // subsequent reload, and spuriously observe the completed worker
            // as still in flight.
            drop(in_flight);
            let _ = tx.send(result);
        })
    {
        // The worker did not take ownership, so clear the single-flight flag.
        // This path is rare, but without it every future search would report a
        // phantom reload in progress.
        reload_in_flight.store(false, Ordering::Release);
        return Err(anyhow!("failed to start bounded searcher reload: {error}"));
    }

    match rx.recv_timeout(timeout) {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(anyhow!("searcher reload failed: {error}")),
        Err(mpsc::RecvTimeoutError::Timeout) => Err(anyhow!(
            "searcher reload exceeded {} ms",
            timeout.as_millis()
        )),
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            Err(anyhow!("searcher reload worker disconnected"))
        }
    }
}

#[derive(Default, Clone)]
pub(in crate::search::query) struct Metrics {
    pub(in crate::search::query) cache_hits: Arc<AtomicU64>,
    pub(in crate::search::query) cache_outcomes:
        Arc<Mutex<crate::daemon_runtime_state::SearcherCacheMetrics>>,
    pub(in crate::search::query) cache_shortfall: Arc<AtomicU64>,
    pub(in crate::search::query) reloads: Arc<AtomicU64>,
    pub(in crate::search::query) reload_ms_total: Arc<AtomicU64>,
    pub(in crate::search::query) reload_in_flight: Arc<AtomicBool>,
    pub(in crate::search::query) prewarm_scheduled: Arc<AtomicU64>,
    pub(in crate::search::query) prewarm_skipped_pressure: Arc<AtomicU64>,
}

impl Metrics {
    pub(in crate::search::query) fn inc_cache_hits(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
        self.record_cache_lookup(crate::daemon_runtime_state::CacheLookup {
            cache_hit: true,
            cached_generation: Some(0),
            current_generation: 0,
            reload_attempted: false,
            reload_succeeded: false,
            served_fallback: false,
        });
    }
    pub(in crate::search::query) fn inc_cache_miss(&self) {
        self.record_cache_lookup(crate::daemon_runtime_state::CacheLookup {
            cache_hit: false,
            cached_generation: None,
            current_generation: 0,
            reload_attempted: false,
            reload_succeeded: false,
            served_fallback: false,
        });
    }
    pub(in crate::search::query) fn record_cache_lookup(
        &self,
        lookup: crate::daemon_runtime_state::CacheLookup,
    ) -> crate::daemon_runtime_state::SearcherCacheOutcome {
        let outcome = crate::daemon_runtime_state::classify_cache_outcome(&lookup);
        if let Ok(mut metrics) = self.cache_outcomes.lock() {
            metrics.record(outcome);
        }
        outcome
    }
    pub(in crate::search::query) fn inc_cache_shortfall(&self) {
        self.cache_shortfall.fetch_add(1, Ordering::Relaxed);
    }
    pub(in crate::search::query) fn inc_prewarm_scheduled(&self) {
        self.prewarm_scheduled.fetch_add(1, Ordering::Relaxed);
    }
    pub(in crate::search::query) fn inc_prewarm_skipped_pressure(&self) {
        self.prewarm_skipped_pressure
            .fetch_add(1, Ordering::Relaxed);
    }
    pub(in crate::search::query) fn inc_reload(&self) {
        self.reloads.fetch_add(1, Ordering::Relaxed);
    }
    pub(in crate::search::query) fn record_reload(&self, duration: Duration) {
        self.inc_reload();
        self.reload_ms_total
            .fetch_add(duration.as_millis() as u64, Ordering::Relaxed);
    }

    pub(in crate::search::query) fn snapshot_all(
        &self,
    ) -> (
        u64,
        crate::daemon_runtime_state::SearcherCacheMetrics,
        u64,
        u64,
        u128,
    ) {
        let cache_outcomes = self
            .cache_outcomes
            .lock()
            .map(|metrics| *metrics)
            .unwrap_or_default();
        (
            self.cache_hits.load(Ordering::Relaxed),
            cache_outcomes,
            self.cache_shortfall.load(Ordering::Relaxed),
            self.reloads.load(Ordering::Relaxed),
            self.reload_ms_total.load(Ordering::Relaxed) as u128,
        )
    }

    pub(in crate::search::query) fn snapshot_prewarm(&self) -> (u64, u64) {
        (
            self.prewarm_scheduled.load(Ordering::Relaxed),
            self.prewarm_skipped_pressure.load(Ordering::Relaxed),
        )
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(in crate::search::query) fn reset(&self) {
        self.cache_hits.store(0, Ordering::Relaxed);
        if let Ok(mut metrics) = self.cache_outcomes.lock() {
            *metrics = crate::daemon_runtime_state::SearcherCacheMetrics::default();
        }
        self.cache_shortfall.store(0, Ordering::Relaxed);
        self.reloads.store(0, Ordering::Relaxed);
        self.reload_ms_total.store(0, Ordering::Relaxed);
        self.reload_in_flight.store(false, Ordering::Relaxed);
        self.prewarm_scheduled.store(0, Ordering::Relaxed);
        self.prewarm_skipped_pressure.store(0, Ordering::Relaxed);
    }
}

pub(in crate::search::query) fn maybe_spawn_warm_worker(
    reader: frankensearch::quill::QuillSearchIndex,
    _fields: crate::search::quill_bridge::QuillCassFields,
    reload_epoch: Arc<AtomicU64>,
    metrics: Metrics,
) -> Option<(mpsc::Sender<WarmJob>, std::thread::JoinHandle<()>)> {
    let (tx, rx) = mpsc::unbounded::<WarmJob>();
    let handle = std::thread::Builder::new()
        .name("cass-warm-worker".into())
        .spawn(move || {
            // Built once for the worker's lifetime: the CASS schema is fixed, so
            // re-parsing it per warm job was pure overhead on a loop whose entire
            // purpose is to be cheap.
            let warm_parser = match frankensearch::quill::query::CassQueryParser::new(
                frankensearch::quill::schema::CASS_SEMANTIC_SCHEMA,
            ) {
                Ok(parser) => parser,
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        "warm worker could not build the CASS query parser; warm searches disabled"
                    );
                    return;
                }
            };
            // Simple debounce: process at most one warmup every WARM_DEBOUNCE_MS.
            let mut last_run = Instant::now();
            while let Ok(job) = rx.recv() {
                let now = Instant::now();
                if now.duration_since(last_run) < Duration::from_millis(*WARM_DEBOUNCE_MS) {
                    continue;
                }
                last_run = now;
                let reload_started = Instant::now();
                match crate::search::quill_bridge::refresh_reader_or_detect_republish(&reader) {
                    Ok(None) => {}
                    Ok(Some(reason)) => {
                        // Its clone of the reader belongs to the old lineage;
                        // warming it would only warm files that are gone.
                        tracing::debug!(
                            reason,
                            "warm worker stops: its lexical index was republished"
                        );
                        return;
                    }
                    Err(err) => {
                        tracing::warn!(error = ?err, "warm_worker_reload_failed");
                        continue;
                    }
                }
                let elapsed = reload_started.elapsed();
                let epoch = reload_epoch.fetch_add(1, Ordering::SeqCst) + 1;
                metrics.record_reload(elapsed);
                tracing::debug!(
                    duration_ms = elapsed.as_millis() as u64,
                    reload_epoch = epoch,
                    filters = %job.filters_fingerprint,
                    shard = %job.shard_name,
                    "warm_worker_reload"
                );
                // Run a tiny warm search to prefill OS cache without
                // allocating full result sets. The CASS parser already lowers
                // a bare term list to the same title/content disjunction the
                // incumbent hand-built here, so this warms the identical
                // posting lists. Limit 1 doc.
                if !job.query.trim().is_empty() {
                    let parsed = warm_parser.parse(
                        &job.query,
                        &frankensearch::quill::query::CassQueryFilters::default(),
                    );
                    // A failed warm search is not an error for the caller — the
                    // real query path will surface anything that matters — but
                    // swallowing it entirely makes a persistently broken warm
                    // path invisible. Log at debug so it is diagnosable without
                    // being noisy.
                    if let Err(err) = crate::search::quill_bridge::search_paginated(
                        &reader,
                        &parsed.query,
                        1,
                        0,
                        false,
                    ) {
                        tracing::debug!(
                            error = %err,
                            query = %job.query,
                            "warm search failed; cache prefill skipped for this job"
                        );
                    }
                }
            }
        })
        .ok()?;
    Some((tx, handle))
}

pub(in crate::search::query) fn cached_hit_from(hit: &SearchHit) -> CachedHit {
    CachedHit { hit: hit.clone() }
}

pub(in crate::search::query) fn is_prefix_only(query: &str) -> bool {
    let tokens: Vec<&str> = query.split_whitespace().collect();
    // Only strictly optimize single-term prefix queries.
    // Multi-term queries benefit from Tantivy's snippet generation (highlighting both terms).
    if tokens.len() != 1 {
        return false;
    }
    tokens[0].chars().all(char::is_alphanumeric)
}

pub(in crate::search::query) fn quick_prefix_snippet(
    content: &str,
    query: &str,
    max_chars: usize,
) -> String {
    // Handle empty query case first
    if query.is_empty() {
        let mut chars = content.chars();
        let snippet: String = chars.by_ref().take(max_chars).collect();
        return if chars.next().is_some() {
            format!("{snippet}…")
        } else {
            snippet
        };
    }

    let lc_content = content.to_lowercase();
    let lc_query = query.to_lowercase();

    if let Some(pos) = lc_content.find(&lc_query) {
        // Convert byte index in the lowercased string to a character index.
        let match_start_char_idx = lc_content[..pos].chars().count();
        let query_char_len = lc_query.chars().count();

        // Determine where to start the snippet (aim for 15 chars before match)
        let start_char = match_start_char_idx.saturating_sub(15);
        let mut chars_iter = content.chars().skip(start_char);
        let mut snippet = String::new();
        let mut chars_taken = 0;
        let mut current_idx = start_char;

        while chars_taken < max_chars {
            if current_idx == match_start_char_idx {
                snippet.push_str("**");
                for _ in 0..query_char_len {
                    if let Some(ch) = chars_iter.next() {
                        snippet.push(ch);
                        chars_taken += 1;
                        current_idx += 1;
                    }
                }
                snippet.push_str("**");
                if chars_taken >= max_chars {
                    break;
                }
                continue;
            }

            if let Some(ch) = chars_iter.next() {
                snippet.push(ch);
                chars_taken += 1;
                current_idx += 1;
            } else {
                break;
            }
        }

        if chars_iter.next().is_some() {
            format!("{snippet}…")
        } else {
            snippet
        }
    } else {
        let mut chars = content.chars();
        let snippet: String = chars.by_ref().take(max_chars).collect();
        if chars.next().is_some() {
            format!("{snippet}…")
        } else {
            snippet
        }
    }
}

pub(in crate::search::query) fn cached_prefix_snippet(
    content: &str,
    query: &str,
    max_chars: usize,
) -> Option<String> {
    if query.trim().is_empty() {
        return None;
    }
    let lc_content = content.to_lowercase();
    let lc_query = query.to_lowercase();
    lc_content.find(&lc_query).map(|pos| {
        let match_start_char_idx = lc_content[..pos].chars().count();
        let query_char_len = lc_query.chars().count();

        let start_char = match_start_char_idx.saturating_sub(15);
        let mut chars_iter = content.chars().skip(start_char);
        let mut snippet = String::new();
        let mut chars_taken = 0;
        let mut current_idx = start_char;

        while chars_taken < max_chars {
            if current_idx == match_start_char_idx {
                snippet.push_str("**");
                for _ in 0..query_char_len {
                    if let Some(ch) = chars_iter.next() {
                        snippet.push(ch);
                        chars_taken += 1;
                        current_idx += 1;
                    }
                }
                snippet.push_str("**");
                if chars_taken >= max_chars {
                    break;
                }
                continue;
            }

            if let Some(ch) = chars_iter.next() {
                snippet.push(ch);
                chars_taken += 1;
                current_idx += 1;
            } else {
                break;
            }
        }

        if chars_iter.next().is_some() {
            format!("{snippet}…")
        } else {
            snippet
        }
    })
}

pub(in crate::search::query) fn filters_fingerprint(filters: &SearchFilters) -> String {
    let mut parts = Vec::new();
    if !filters.agents.is_empty() {
        let mut v: Vec<_> = filters.agents.iter().cloned().collect();
        v.sort();
        parts.push(format!("a:{v:?}"));
    }
    if !filters.workspaces.is_empty() {
        let mut v: Vec<_> = filters.workspaces.iter().cloned().collect();
        v.sort();
        parts.push(format!("w:{v:?}"));
    }
    if let Some(f) = filters.created_from {
        parts.push(format!("from:{f}"));
    }
    if let Some(t) = filters.created_to {
        parts.push(format!("to:{t}"));
    }
    // Include source_filter in cache key (P3.1)
    if !matches!(
        filters.source_filter,
        crate::sources::provenance::SourceFilter::All
    ) {
        parts.push(format!("src:{:?}", filters.source_filter));
    }
    // Include session_paths in cache key (for chained searches)
    if !filters.session_paths.is_empty() {
        let mut v: Vec<_> = filters.session_paths.iter().cloned().collect();
        v.sort();
        parts.push(format!("sp:{v:?}"));
    }
    parts.join("|")
}

impl SearchClient {
    /// Return the total number of indexed Tantivy documents.
    pub fn total_docs(&self) -> usize {
        if let Some((reader, _)) = self.reader.get() {
            return usize::try_from(reader.doc_count().unwrap_or(0)).unwrap_or(usize::MAX);
        }
        self.federated_readers()
            .map(|readers| {
                readers
                    .iter()
                    .map(|shard| {
                        usize::try_from(shard.reader.doc_count().unwrap_or(0)).unwrap_or(usize::MAX)
                    })
                    .sum()
            })
            .unwrap_or(0)
    }

    /// Returns `true` if the Tantivy search index is available.
    pub fn has_tantivy(&self) -> bool {
        self.reader.is_some() || self.federated_readers().is_some()
    }

    pub(in crate::search::query) fn refresh_lexical_generation(&self) -> Result<()> {
        if let Some((reader, _)) = self.reader.get() {
            // A reload can replace the reader after a generation directory
            // exchange, so track the reader that is current afterwards.
            self.maybe_reload_reader(&reader)?;
            if let Some((reader, _)) = self.reader.get() {
                self.track_generation(reader.keeper_generation());
            }
        } else if let Some(readers) = self.federated_readers()
            && let Some(signature) = self.maybe_reload_federated_readers(readers.as_ref())?
        {
            self.track_generation(signature);
        }
        Ok(())
    }

    pub(in crate::search::query) fn maybe_reload_reader(
        &self,
        reader: &frankensearch::quill::QuillSearchIndex,
    ) -> Result<()> {
        if !self.reload_on_search {
            return Ok(());
        }
        const MIN_RELOAD_INTERVAL: Duration = Duration::from_millis(300);
        let now = Instant::now();
        let mut guard = self.last_reload.lock().unwrap_or_else(|e| e.into_inner());
        if guard
            .map(|t| now.duration_since(t) >= MIN_RELOAD_INTERVAL)
            .unwrap_or(true)
        {
            let reload_started = Instant::now();
            let cached_generation = reader.keeper_generation();
            let republished = match reload_index_readers_bounded(
                vec![reader.clone()],
                Arc::clone(&self.metrics.reload_in_flight),
            ) {
                Ok(republished) => republished,
                Err(error) => {
                    self.metrics
                        .record_cache_lookup(crate::daemon_runtime_state::CacheLookup {
                            cache_hit: true,
                            cached_generation: Some(cached_generation),
                            current_generation: cached_generation,
                            reload_attempted: true,
                            reload_succeeded: false,
                            served_fallback: false,
                        });
                    return Err(error);
                }
            };
            if republished {
                self.reopen_republished_lexical_index()?;
            }
            let elapsed = reload_started.elapsed();
            // Rate-limit from completion, not start. This keeps a slow reload
            // from becoming immediately eligible for another refresh.
            *guard = Some(Instant::now());
            let epoch = self.reload_epoch.fetch_add(1, Ordering::SeqCst) + 1;
            self.metrics.record_reload(elapsed);
            let current_generation = reader.keeper_generation();
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
                "tantivy_reader_reload"
            );
        }
        Ok(())
    }

    /// Open the index path again after it was republished as a new lineage
    /// (a rebuild's directory exchange), the way `open_with_options` opens
    /// it, and swap the readers in. The rebuild can reuse the old generation
    /// number, so the prefix cache is cleared here rather than left to
    /// `track_generation`.
    pub(in crate::search::query) fn reopen_republished_lexical_index(&self) -> Result<()> {
        let Some(index_path) = self.reader.index_path.clone() else {
            return Err(anyhow!(
                "the lexical index was republished and this search client has no path to reopen it from"
            ));
        };
        type Opened = (
            Option<frankensearch::quill::QuillSearchIndex>,
            Option<Vec<FederatedIndexReader>>,
        );
        let opened: Arc<Mutex<Option<Opened>>> = Arc::new(Mutex::new(None));
        let worker_opened = Arc::clone(&opened);
        run_reload_bounded(
            move || {
                let single = crate::search::quill_bridge::open_cass_reader(&index_path).ok();
                let federated = if single.is_none() {
                    crate::search::tantivy::open_federated_search_readers(&index_path)
                        .ok()
                        .flatten()
                        .filter(|readers| !readers.is_empty())
                        .map(|readers| {
                            readers
                                .into_iter()
                                .map(|(reader, fields)| FederatedIndexReader { reader, fields })
                                .collect::<Vec<_>>()
                        })
                } else {
                    None
                };
                if single.is_none() && federated.is_none() {
                    return Err(format!(
                        "reopening the republished lexical index at {}: no readable index",
                        index_path.display()
                    ));
                }
                *worker_opened.lock().unwrap_or_else(|e| e.into_inner()) =
                    Some((single, federated));
                Ok(())
            },
            Arc::clone(&self.metrics.reload_in_flight),
            *SEARCHER_RELOAD_TIMEOUT,
        )?;
        let (single, federated) = opened
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .ok_or_else(|| anyhow!("the lexical reopen worker returned no readers"))?;
        *self.reader.current.write() = single.map(|reader| {
            (
                reader,
                crate::search::quill_bridge::QuillCassFields::compiled(),
            )
        });
        match federated {
            Some(readers) => {
                FEDERATED_SEARCH_READERS
                    .write()
                    .insert(self.cache_namespace.clone(), Arc::new(readers));
            }
            None => {
                FEDERATED_SEARCH_READERS
                    .write()
                    .remove(&self.cache_namespace);
            }
        }
        if let Ok(mut cache) = self.prefix_cache.lock() {
            self.reload_epoch.fetch_add(1, Ordering::SeqCst);
            cache.clear();
        }
        *self
            .last_generation
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = None;
        Ok(())
    }

    pub(in crate::search::query) fn maybe_log_cache_metrics(&self, event: &str) {
        if !*CACHE_DEBUG_ENABLED {
            return;
        }
        let stats = self.cache_stats();
        tracing::debug!(
            event = event,
            hits = stats.cache_hits,
            miss = stats.cache_miss,
            shortfall = stats.cache_shortfall,
            stale_generation_miss = stats.searcher_cache.stale_generation_miss,
            forced_reload = stats.searcher_cache.forced_reload,
            reload_failure = stats.searcher_cache.reload_failure,
            fallback = stats.searcher_cache.fallback,
            reloads = stats.reloads,
            reload_ms_total = stats.reload_ms_total,
            total_cap = stats.total_cap,
            total_cost = stats.total_cost,
            evictions = stats.eviction_count,
            approx_bytes = stats.approx_bytes,
            byte_cap = stats.byte_cap,
            eviction_policy = stats.eviction_policy,
            ghost_entries = stats.ghost_entries,
            admission_rejects = stats.admission_rejects,
            "cache_metrics"
        );
    }

    /// Generate an interned result key for the exact query, filters and fields.
    /// Returns Arc<str> to enable memory sharing for repeated queries.
    pub(in crate::search::query) fn cache_key(
        &self,
        query: &str,
        filters: &SearchFilters,
        field_mask: FieldMask,
    ) -> Arc<str> {
        let key_str = format!(
            "{}|{query:?}::{}|fields:{}:{:?}",
            self.cache_namespace,
            filters_fingerprint(filters),
            field_mask.flags,
            field_mask.preview_content_chars,
        );
        intern_cache_key(&key_str)
    }

    pub(in crate::search::query) fn shard_name(&self, filters: &SearchFilters) -> String {
        if filters.agents.len() == 1 {
            format!(
                "agent:{}",
                filters
                    .agents
                    .iter()
                    .next()
                    .cloned()
                    .unwrap_or_else(|| "global".into())
            )
        } else if filters.workspaces.len() == 1 {
            format!(
                "workspace:{}",
                filters
                    .workspaces
                    .iter()
                    .next()
                    .cloned()
                    .unwrap_or_else(|| "global".into())
            )
        } else {
            "global".into()
        }
    }
    pub(in crate::search::query) fn cached_prefix_key_exists_in_shard(
        &self,
        shard: &LruCache<Arc<str>, Vec<CachedHit>>,
        query: &str,
        filters: &SearchFilters,
        field_mask: FieldMask,
    ) -> bool {
        let mut byte_indices: Vec<usize> = query.char_indices().map(|(i, _)| i).collect();
        byte_indices.push(query.len());
        let query_len = query.len();
        for &end in byte_indices.iter().rev() {
            if end == 0 || end == query_len {
                continue;
            }
            let key = self.cache_key(&query[..end], filters, field_mask);
            if shard.contains(&key) {
                return true;
            }
        }
        false
    }

    pub(in crate::search::query) fn maybe_schedule_adaptive_query_prewarm(
        &self,
        query: &str,
        filters: &SearchFilters,
        field_mask: FieldMask,
    ) {
        if query.is_empty() {
            return;
        }
        let Some(tx) = &self.warm_tx else {
            return;
        };

        let shard_name = self.shard_name(filters);
        let decision = match self.prefix_cache.lock() {
            Ok(cache) => {
                let hot_prefix = cache.shard_opt(&shard_name).is_some_and(|shard| {
                    self.cached_prefix_key_exists_in_shard(shard, query, filters, field_mask)
                });
                if !hot_prefix {
                    AdaptivePrewarmDecision::SkipCold
                } else if cache.prewarm_pressure() {
                    AdaptivePrewarmDecision::SkipPressure
                } else {
                    AdaptivePrewarmDecision::Schedule
                }
            }
            Err(_) => return,
        };

        if decision == AdaptivePrewarmDecision::SkipPressure {
            self.metrics.inc_prewarm_skipped_pressure();
            return;
        }
        if decision == AdaptivePrewarmDecision::SkipCold {
            return;
        }

        if tx
            .send(WarmJob {
                query: query.to_string(),
                filters_fingerprint: filters_fingerprint(filters),
                shard_name,
            })
            .is_ok()
        {
            self.metrics.inc_prewarm_scheduled();
        }
    }

    pub(in crate::search::query) fn cached_query_hits(
        &self,
        query: &str,
        filters: &SearchFilters,
        field_mask: FieldMask,
    ) -> Option<Vec<CachedHit>> {
        if query.is_empty() {
            return None;
        }
        let cache = self.prefix_cache.lock().ok()?;
        let shard_name = self.shard_name(filters);
        let shard = cache.shard_opt(&shard_name)?;
        let key = self.cache_key(query, filters, field_mask);
        shard.peek(&key).cloned()
    }

    pub(in crate::search::query) fn put_cache(
        &self,
        query: &str,
        filters: &SearchFilters,
        field_mask: FieldMask,
        expected_epoch: u64,
        hits: &[SearchHit],
    ) {
        if query.is_empty() || hits.is_empty() {
            return;
        }
        if let Ok(mut cache) = self.prefix_cache.lock() {
            if self.reload_epoch.load(Ordering::SeqCst) != expected_epoch {
                return;
            }
            let shard_name = self.shard_name(filters);
            let key = self.cache_key(query, filters, field_mask);
            let cached_hits: Vec<CachedHit> = hits.iter().map(cached_hit_from).collect();
            cache.put(&shard_name, key, cached_hits);
        }
    }

    pub(in crate::search::query) fn record_lexical_degrade_reason(
        &self,
        reason: Option<&'static str>,
    ) {
        if let Ok(mut slot) = self.last_lexical_degrade_reason.lock() {
            *slot = reason;
        }
    }

    /// Why the most recent lexical request degraded, if it did.
    /// [`LEXICAL_DEGRADE_QUERY_FUEL_EXHAUSTED`] means a hybrid lexical leg was lost;
    /// [`LEXICAL_RETRY_QUERY_FUEL_EXHAUSTED`] means a usable but incomplete native
    /// page survived a larger-fetch refusal. Neither code denotes an exact total.
    #[must_use]
    pub fn lexical_degrade_reason(&self) -> Option<&'static str> {
        self.last_lexical_degrade_reason
            .lock()
            .ok()
            .and_then(|slot| *slot)
    }

    pub(in crate::search::query) fn record_wildcard_fallback_skip(
        &self,
        reason: Option<&'static str>,
    ) {
        if let Ok(mut slot) = self.last_wildcard_fallback_skip.lock() {
            *slot = reason;
        }
    }

    /// Why the most recent `search_with_fallback` skipped the automatic
    /// wildcard retry a sparse result would otherwise get, if it did. See
    /// [`WILDCARD_FALLBACK_SKIPPED_LARGE_INDEX`],
    /// [`WILDCARD_FALLBACK_SKIPPED_DISABLED`] and
    /// [`WILDCARD_FALLBACK_SKIPPED_LONG_TERM`] (2l1b0.68).
    #[must_use]
    pub fn wildcard_fallback_skipped_reason(&self) -> Option<&'static str> {
        self.last_wildcard_fallback_skip
            .lock()
            .ok()
            .and_then(|slot| *slot)
    }

    pub fn cache_stats(&self) -> CacheStats {
        let (hits, searcher_cache, shortfall, reloads, reload_ms_total) =
            self.metrics.snapshot_all();
        let (prewarm_scheduled, prewarm_skipped_pressure) = self.metrics.snapshot_prewarm();
        let reader_generation = self.last_generation.lock().ok().and_then(|guard| *guard);
        let (
            total_cap,
            total_cost,
            eviction_count,
            approx_bytes,
            byte_cap,
            eviction_policy,
            ghost_entries,
            admission_rejects,
        ) = if let Ok(cache) = self.prefix_cache.lock() {
            (
                cache.total_cap(),
                cache.total_cost(),
                cache.eviction_count(),
                cache.total_bytes(),
                cache.byte_cap(),
                cache.policy_label(),
                cache.ghost_entries(),
                cache.admission_rejects(),
            )
        } else {
            (0, 0, 0, 0, 0, "unknown", 0, 0)
        };
        CacheStats {
            cache_hits: hits,
            cache_miss: searcher_cache
                .cold_miss
                .saturating_add(searcher_cache.stale_generation_miss),
            cache_shortfall: shortfall,
            searcher_cache,
            reloads,
            reload_ms_total,
            total_cap,
            total_cost,
            eviction_count,
            approx_bytes,
            byte_cap,
            eviction_policy,
            ghost_entries,
            admission_rejects,
            prewarm_scheduled,
            prewarm_skipped_pressure,
            reader_generation,
        }
    }
}
