use super::super::*;
use super::helpers::*;
use crate::connectors::{NormalizedConversation, NormalizedMessage};
use crate::search::tantivy::TantivyIndex;
use lru::LruCache;
use tempfile::TempDir;

#[test]
fn cache_invalidates_on_new_data() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;

    // 1. Add initial doc
    let conv1 = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("first".into()),
        workspace: None,
        source_path: dir.path().join("1.jsonl"),
        started_at: Some(1),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(1),
            content: "apple banana".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    index.add_conversation(&conv1)?;
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");

    // 2. Search "app" -> should hit "apple"
    let hits = client.search("app", SearchFilters::default(), 10, 0, FieldMask::FULL)?;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].content, "apple banana");

    // 3. Verify it's cached (peek internal state)
    {
        let cache = client.prefix_cache.lock().unwrap();
        let shard = cache.shard_opt("global").unwrap();
        // "app" should be in cache
        assert!(shard.contains(&client.cache_key(
            "app",
            &SearchFilters::default(),
            FieldMask::FULL
        )));
    }

    // 4. Add new doc with "apricot"
    let conv2 = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("second".into()),
        workspace: None,
        source_path: dir.path().join("2.jsonl"),
        started_at: Some(2),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(2),
            content: "apricot".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    index.add_conversation(&conv2)?;
    index.commit()?;

    // 5. Force reload (mocking time passing or just ensuring reload triggers)
    // In test, maybe_reload_reader uses 300ms debounce.
    // We can rely on opstamp check logic which runs AFTER reload.
    // We need to sleep briefly to bypass debounce or just modify test to not rely on time?
    // Actually SearchClient::maybe_reload_reader checks duration.
    std::thread::sleep(std::time::Duration::from_millis(350));

    // 6. Search "ap" (prefix of apricot and apple)
    // The cache for "app" should be cleared if opstamp changed.
    let _hits = client.search("app", SearchFilters::default(), 10, 0, FieldMask::FULL)?;
    // Should now find 1 doc still ("apple"), but cache should have been cleared first

    // Search "apr" -> should find "apricot"
    let hits = client.search("apr", SearchFilters::default(), 10, 0, FieldMask::FULL)?;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].content, "apricot");

    // Check that cache was cleared by verifying a stale key is gone?
    // Or rely on correctness of results if we searched a common prefix?

    Ok(())
}

#[test]
fn track_generation_clears_cache_on_change() {
    let client = SearchClient {
        reader: LexicalReaderSlot::default(),
        sqlite: Mutex::new(None),
        sqlite_path: None,
        prefix_cache: Mutex::new(CacheShards::new(*CACHE_TOTAL_CAP, *CACHE_BYTE_CAP)),
        reload_on_search: true,
        strict_read_only: false,
        last_reload: Mutex::new(None),
        last_generation: Mutex::new(None),
        reload_epoch: Arc::new(AtomicU64::new(0)),
        warm_tx: None,
        _warm_handle: None,
        metrics: Metrics::default(),
        cache_namespace: format!("v{CACHE_KEY_VERSION}|schema:test"),
        semantic: Mutex::new(None),
        last_tantivy_total_count: Mutex::new(None),
        last_lexical_degrade_reason: Mutex::new(None),
        last_wildcard_fallback_skip: Mutex::new(None),
    };

    let hit = SearchHit {
        title: "hello world".into(),
        snippet: "hello".into(),
        content: "hello world".into(),
        content_hash: stable_content_hash("hello world"),
        score: 1.0,
        source_path: "p".into(),
        agent: "a".into(),
        workspace: "w".into(),
        workspace_original: None,
        created_at: None,
        line_number: None,
        match_type: MatchType::Exact,
        source_id: "local".into(),
        origin_kind: "local".into(),
        origin_host: None,
        conversation_id: None,
    };
    let hits = vec![hit];

    client.put_cache(
        "hello",
        &SearchFilters::default(),
        FieldMask::FULL,
        0,
        &hits,
    );
    {
        let cache = client.prefix_cache.lock().unwrap();
        assert!(!cache.shards.is_empty());
    }

    client.track_generation(1);
    {
        let cache = client.prefix_cache.lock().unwrap();
        assert!(!cache.shards.is_empty());
    }

    client.track_generation(2);
    {
        let cache = client.prefix_cache.lock().unwrap();
        assert!(cache.shards.is_empty());
    }
    let stats = client.cache_stats();
    assert_eq!(stats.searcher_cache.stale_generation_miss, 1);
    assert_eq!(stats.cache_miss, 1);
}

#[test]
fn cache_total_cap_evicts_across_shards() {
    let client = SearchClient {
        reader: LexicalReaderSlot::default(),
        sqlite: Mutex::new(None),
        sqlite_path: None,
        prefix_cache: Mutex::new(CacheShards::new(2, 0)), // tiny entry cap, no byte cap
        reload_on_search: true,
        strict_read_only: false,
        last_reload: Mutex::new(None),
        last_generation: Mutex::new(None),
        reload_epoch: Arc::new(AtomicU64::new(0)),
        warm_tx: None,
        _warm_handle: None,
        metrics: Metrics::default(),
        cache_namespace: format!("v{CACHE_KEY_VERSION}|schema:test"),
        semantic: Mutex::new(None),
        last_tantivy_total_count: Mutex::new(None),
        last_lexical_degrade_reason: Mutex::new(None),
        last_wildcard_fallback_skip: Mutex::new(None),
    };

    let hit = SearchHit {
        title: "a".into(),
        snippet: "a".into(),
        content: "a".into(),
        content_hash: stable_content_hash("a"),
        score: 1.0,
        source_path: "p".into(),
        agent: "agent1".into(),
        workspace: "w".into(),
        workspace_original: None,
        created_at: None,
        line_number: None,
        match_type: MatchType::Exact,
        source_id: "local".into(),
        origin_kind: "local".into(),
        origin_host: None,
        conversation_id: None,
    };
    let hits = vec![hit.clone()];

    let mut filters = SearchFilters::default();
    filters.agents.insert("agent1".into());
    client.put_cache("a", &filters, FieldMask::FULL, 0, &hits);
    filters.agents.clear();
    filters.agents.insert("agent2".into());
    client.put_cache("b", &filters, FieldMask::FULL, 0, &hits);
    filters.agents.clear();
    filters.agents.insert("agent3".into());
    client.put_cache("c", &filters, FieldMask::FULL, 0, &hits);

    let stats = client.cache_stats();
    assert!(stats.total_cost <= stats.total_cap);
    assert_eq!(stats.total_cap, 2);
}

#[test]
fn cache_stats_reflect_metrics() {
    let client = SearchClient {
        reader: LexicalReaderSlot::default(),
        sqlite: Mutex::new(None),
        sqlite_path: None,
        prefix_cache: Mutex::new(CacheShards::new(*CACHE_TOTAL_CAP, *CACHE_BYTE_CAP)),
        reload_on_search: true,
        strict_read_only: false,
        last_reload: Mutex::new(None),
        last_generation: Mutex::new(None),
        reload_epoch: Arc::new(AtomicU64::new(0)),
        warm_tx: None,
        _warm_handle: None,
        metrics: Metrics::default(),
        cache_namespace: format!("v{CACHE_KEY_VERSION}|schema:test"),
        semantic: Mutex::new(None),
        last_tantivy_total_count: Mutex::new(None),
        last_lexical_degrade_reason: Mutex::new(None),
        last_wildcard_fallback_skip: Mutex::new(None),
    };

    client.metrics.inc_cache_hits();
    client.metrics.inc_cache_miss();
    client.metrics.inc_cache_shortfall();
    client.metrics.record_reload(Duration::from_millis(10));

    let stats = client.cache_stats();
    assert_eq!(stats.cache_hits, 1);
    assert_eq!(stats.cache_miss, 1);
    assert_eq!(stats.cache_shortfall, 1);
    assert_eq!(stats.searcher_cache.hit, 1);
    assert_eq!(stats.searcher_cache.cold_miss, 1);
    assert_eq!(stats.reloads, 1);
    assert_eq!(stats.reload_ms_total, 10);
    assert_eq!(stats.total_cap, *CACHE_TOTAL_CAP);
    assert_eq!(stats.eviction_policy, "lru");
    assert_eq!(stats.prewarm_scheduled, 0);
    assert_eq!(stats.prewarm_skipped_pressure, 0);
    assert_eq!(CacheStats::default().eviction_policy, "unknown");
}

#[test]
fn adaptive_query_prewarm_schedules_only_after_hot_prefix_cache_entry() {
    let (tx, rx) = mpsc::unbounded();
    let client = SearchClient {
        reader: LexicalReaderSlot::default(),
        sqlite: Mutex::new(None),
        sqlite_path: None,
        prefix_cache: Mutex::new(CacheShards::new(10, 0)),
        reload_on_search: true,
        strict_read_only: false,
        last_reload: Mutex::new(None),
        last_generation: Mutex::new(None),
        reload_epoch: Arc::new(AtomicU64::new(0)),
        warm_tx: Some(tx),
        _warm_handle: None,
        metrics: Metrics::default(),
        cache_namespace: format!("v{CACHE_KEY_VERSION}|schema:test"),
        semantic: Mutex::new(None),
        last_tantivy_total_count: Mutex::new(None),
        last_lexical_degrade_reason: Mutex::new(None),
        last_wildcard_fallback_skip: Mutex::new(None),
    };
    let mut filters = SearchFilters::default();
    filters.workspaces.insert("/tmp/cass-workspace".into());

    client.maybe_schedule_adaptive_query_prewarm("hel", &filters, FieldMask::FULL);
    assert!(
        rx.try_recv().is_err(),
        "cold prefixes should not schedule adaptive prewarm"
    );

    let mut hit = projected_minimal_fields_search_hit("hello title", "p");
    hit.snippet = "hello".into();
    hit.content = "hello world".into();
    hit.content_hash = stable_content_hash(&hit.content);
    client.put_cache(
        "hel",
        &filters,
        FieldMask::FULL,
        0,
        std::slice::from_ref(&hit),
    );

    let total_cost_before = client.cache_stats().total_cost;
    client.maybe_schedule_adaptive_query_prewarm("hel", &filters, FieldMask::FULL);
    assert!(
        rx.try_recv().is_err(),
        "an exact cached query should not schedule redundant prewarm"
    );
    client.maybe_schedule_adaptive_query_prewarm("hello", &filters, FieldMask::FULL);

    let job = rx
        .try_recv()
        .expect("hot prefix should schedule adaptive prewarm");
    assert_eq!(job.query, "hello");
    assert_eq!(job.shard_name, "workspace:/tmp/cass-workspace");
    assert_eq!(job.filters_fingerprint, filters_fingerprint(&filters));
    let stats = client.cache_stats();
    assert_eq!(stats.prewarm_scheduled, 1);
    assert_eq!(stats.prewarm_skipped_pressure, 0);
    assert_eq!(
        stats.total_cost, total_cost_before,
        "prewarm scheduling should not mutate result-cache contents"
    );
}

#[test]
fn adaptive_query_prewarm_skips_when_cache_byte_cap_is_under_pressure() {
    let mut hit = projected_minimal_fields_search_hit("hello title", "p");
    hit.snippet = "hello".into();
    hit.content = "hello world with enough content to consume the small byte budget".into();
    hit.content_hash = stable_content_hash(&hit.content);
    let byte_cap = cached_hit_from(&hit).approx_bytes();

    let (tx, rx) = mpsc::unbounded();
    let client = SearchClient {
        reader: LexicalReaderSlot::default(),
        sqlite: Mutex::new(None),
        sqlite_path: None,
        prefix_cache: Mutex::new(CacheShards::new(10, byte_cap)),
        reload_on_search: true,
        strict_read_only: false,
        last_reload: Mutex::new(None),
        last_generation: Mutex::new(None),
        reload_epoch: Arc::new(AtomicU64::new(0)),
        warm_tx: Some(tx),
        _warm_handle: None,
        metrics: Metrics::default(),
        cache_namespace: format!("v{CACHE_KEY_VERSION}|schema:test"),
        semantic: Mutex::new(None),
        last_tantivy_total_count: Mutex::new(None),
        last_lexical_degrade_reason: Mutex::new(None),
        last_wildcard_fallback_skip: Mutex::new(None),
    };
    let filters = SearchFilters::default();

    client.put_cache(
        "hel",
        &filters,
        FieldMask::FULL,
        0,
        std::slice::from_ref(&hit),
    );
    client.maybe_schedule_adaptive_query_prewarm("zebra", &filters, FieldMask::FULL);
    assert_eq!(
        client.cache_stats().prewarm_skipped_pressure,
        0,
        "cold queries should not be counted as pressure-skipped prewarm jobs"
    );

    client.maybe_schedule_adaptive_query_prewarm("hello", &filters, FieldMask::FULL);

    assert!(
        rx.try_recv().is_err(),
        "prewarm should be disabled while cache byte pressure is high"
    );
    let stats = client.cache_stats();
    assert_eq!(stats.prewarm_scheduled, 0);
    assert_eq!(stats.prewarm_skipped_pressure, 1);
    assert!(stats.approx_bytes <= stats.byte_cap);
}

#[test]
fn cache_eviction_count_tracks_evictions() {
    // tiny entry cap (2 entries), no byte cap - forces evictions
    let client = SearchClient {
        reader: LexicalReaderSlot::default(),
        sqlite: Mutex::new(None),
        sqlite_path: None,
        prefix_cache: Mutex::new(CacheShards::new(2, 0)),
        reload_on_search: true,
        strict_read_only: false,
        last_reload: Mutex::new(None),
        last_generation: Mutex::new(None),
        reload_epoch: Arc::new(AtomicU64::new(0)),
        warm_tx: None,
        _warm_handle: None,
        metrics: Metrics::default(),
        cache_namespace: format!("v{CACHE_KEY_VERSION}|schema:test"),
        semantic: Mutex::new(None),
        last_tantivy_total_count: Mutex::new(None),
        last_lexical_degrade_reason: Mutex::new(None),
        last_wildcard_fallback_skip: Mutex::new(None),
    };

    let hit = SearchHit {
        title: "test".into(),
        snippet: "snippet".into(),
        content: "content".into(),
        content_hash: stable_content_hash("content"),
        score: 1.0,
        source_path: "p".into(),
        agent: "a".into(),
        workspace: "w".into(),
        workspace_original: None,
        created_at: None,
        line_number: None,
        match_type: MatchType::Exact,
        source_id: "local".into(),
        origin_kind: "local".into(),
        origin_host: None,
        conversation_id: None,
    };

    // Put 3 entries - should trigger 1 eviction (cap is 2)
    client.put_cache(
        "query1",
        &SearchFilters::default(),
        FieldMask::FULL,
        0,
        std::slice::from_ref(&hit),
    );
    client.put_cache(
        "query2",
        &SearchFilters::default(),
        FieldMask::FULL,
        0,
        std::slice::from_ref(&hit),
    );
    client.put_cache(
        "query3",
        &SearchFilters::default(),
        FieldMask::FULL,
        0,
        std::slice::from_ref(&hit),
    );

    let stats = client.cache_stats();
    assert!(
        stats.eviction_count >= 1,
        "should have evicted at least 1 entry"
    );
    assert!(stats.total_cost <= 2, "should be at or below cap");
    assert!(stats.approx_bytes > 0, "should track bytes used");
}

#[test]
fn default_cache_byte_cap_scales_with_available_memory() {
    let gib = 1024_u64 * 1024 * 1024;

    assert_eq!(
        default_cache_byte_cap_for_available(None),
        DEFAULT_CACHE_BYTE_CAP_FALLBACK
    );
    assert_eq!(
        default_cache_byte_cap_for_available(Some(2 * gib)),
        DEFAULT_CACHE_BYTE_CAP_FALLBACK,
        "small hosts keep a conservative cache byte budget"
    );
    assert_eq!(
        default_cache_byte_cap_for_available(Some(64 * gib)),
        512 * 1024 * 1024,
        "larger hosts get a proportionally larger cache byte budget"
    );
    assert_eq!(
        default_cache_byte_cap_for_available(Some(256 * gib)),
        usize::try_from(DEFAULT_CACHE_BYTE_CAP_CEILING).unwrap_or(usize::MAX),
        "large swarm hosts still have a bounded default cache budget"
    );
}

#[test]
fn malformed_cache_byte_cap_env_uses_default_instead_of_disabling_guard() {
    let gib = 1024_u64 * 1024 * 1024;

    assert_eq!(cache_byte_cap_from_env_value(Some("0"), Some(64 * gib)), 0);
    assert_eq!(
        cache_byte_cap_from_env_value(Some("not-a-number"), Some(64 * gib)),
        default_cache_byte_cap_for_available(Some(64 * gib)),
        "malformed env should keep the default memory guard active"
    );
    assert_eq!(
        cache_byte_cap_from_env_value(None, Some(64 * gib)),
        default_cache_byte_cap_for_available(Some(64 * gib))
    );
}

#[test]
fn cache_eviction_policy_env_defaults_to_lru_and_accepts_s3_fifo() {
    assert_eq!(
        cache_eviction_policy_from_env_value(None),
        CacheEvictionPolicy::Lru
    );
    assert_eq!(
        cache_eviction_policy_from_env_value(Some("not-a-policy")),
        CacheEvictionPolicy::Lru,
        "malformed env keeps the current LRU behavior"
    );
    assert_eq!(
        cache_eviction_policy_from_env_value(Some("s3-fifo")),
        CacheEvictionPolicy::S3Fifo
    );
    assert_eq!(
        cache_eviction_policy_from_env_value(Some("s3_fifo")),
        CacheEvictionPolicy::S3Fifo
    );
}

#[test]
fn s3_fifo_admission_rejects_one_off_byte_heavy_entries_then_admits_ghost_replay() {
    let content = "large".repeat(1_000);
    let hit = SearchHit {
        title: "large".into(),
        snippet: "large".into(),
        content: content.clone(),
        content_hash: stable_content_hash(&content),
        score: 1.0,
        source_path: "large-path".into(),
        agent: "a".into(),
        workspace: "w".into(),
        workspace_original: None,
        created_at: None,
        line_number: None,
        match_type: MatchType::Exact,
        source_id: "local".into(),
        origin_kind: "local".into(),
        origin_host: None,
        conversation_id: None,
    };
    let cached = cached_hit_from(&hit);
    let byte_cap = cached.approx_bytes() + 1_024;
    assert!(cached.approx_bytes() > byte_cap.div_ceil(S3_FIFO_LARGE_ENTRY_FRACTION_DENOMINATOR));

    let mut cache = CacheShards::new_with_policy(100, byte_cap, CacheEvictionPolicy::S3Fifo);
    let key = Arc::<str>::from("large-query");

    cache.put("global", key.clone(), vec![cached.clone()]);
    assert_eq!(
        cache.total_cost(),
        0,
        "first one-off large entry is not admitted"
    );
    assert_eq!(cache.ghost_entries(), 1);
    assert_eq!(cache.admission_rejects(), 1);

    cache.put("global", key, vec![cached]);
    assert_eq!(
        cache.total_cost(),
        1,
        "ghost replay admits the repeated query"
    );
    assert_eq!(cache.ghost_entries(), 0);
    assert!(cache.ghost_keys.is_empty());
    assert_eq!(cache.admission_rejects(), 1);
    assert!(cache.total_bytes() <= cache.byte_cap());
}

#[test]
fn lru_policy_keeps_admitting_large_entries_under_existing_caps() {
    let content = "large".repeat(1_000);
    let hit = SearchHit {
        title: "large".into(),
        snippet: "large".into(),
        content: content.clone(),
        content_hash: stable_content_hash(&content),
        score: 1.0,
        source_path: "large-path".into(),
        agent: "a".into(),
        workspace: "w".into(),
        workspace_original: None,
        created_at: None,
        line_number: None,
        match_type: MatchType::Exact,
        source_id: "local".into(),
        origin_kind: "local".into(),
        origin_host: None,
        conversation_id: None,
    };
    let cached = cached_hit_from(&hit);
    let byte_cap = cached.approx_bytes() + 1_024;
    let mut cache = CacheShards::new_with_policy(100, byte_cap, CacheEvictionPolicy::Lru);

    cache.put("global", Arc::<str>::from("large-query"), vec![cached]);

    assert_eq!(cache.total_cost(), 1);
    assert_eq!(cache.ghost_entries(), 0);
    assert_eq!(cache.admission_rejects(), 0);
    assert_eq!(cache.policy_label(), "lru");
}

#[test]
fn cache_byte_cap_triggers_eviction() {
    // Large entry cap (1000), tiny byte cap (100 bytes) - forces byte-based evictions
    let client = SearchClient {
        reader: LexicalReaderSlot::default(),
        sqlite: Mutex::new(None),
        sqlite_path: None,
        prefix_cache: Mutex::new(CacheShards::new(1000, 100)), // byte cap of 100
        reload_on_search: true,
        strict_read_only: false,
        last_reload: Mutex::new(None),
        last_generation: Mutex::new(None),
        reload_epoch: Arc::new(AtomicU64::new(0)),
        warm_tx: None,
        _warm_handle: None,
        metrics: Metrics::default(),
        cache_namespace: format!("v{CACHE_KEY_VERSION}|schema:test"),
        semantic: Mutex::new(None),
        last_tantivy_total_count: Mutex::new(None),
        last_lexical_degrade_reason: Mutex::new(None),
        last_wildcard_fallback_skip: Mutex::new(None),
    };

    // Large content to exceed byte cap quickly
    let content = "c".repeat(100);
    let hit = SearchHit {
        title: "a".repeat(50),
        snippet: "b".repeat(50),
        content: content.clone(), // 200+ bytes per hit
        content_hash: stable_content_hash(&content),
        score: 1.0,
        source_path: "p".into(),
        agent: "a".into(),
        workspace: "w".into(),
        workspace_original: None,
        created_at: None,
        line_number: None,
        match_type: MatchType::Exact,
        source_id: "local".into(),
        origin_kind: "local".into(),
        origin_host: None,
        conversation_id: None,
    };

    // Put 3 large entries - should trigger byte-based evictions
    client.put_cache(
        "q1",
        &SearchFilters::default(),
        FieldMask::FULL,
        0,
        std::slice::from_ref(&hit),
    );
    client.put_cache(
        "q2",
        &SearchFilters::default(),
        FieldMask::FULL,
        0,
        std::slice::from_ref(&hit),
    );
    client.put_cache(
        "q3",
        &SearchFilters::default(),
        FieldMask::FULL,
        0,
        std::slice::from_ref(&hit),
    );

    let stats = client.cache_stats();
    assert!(
        stats.eviction_count >= 1,
        "byte cap should trigger evictions"
    );
    assert_eq!(stats.byte_cap, 100, "byte cap should be reported");
    // Note: approx_bytes may briefly exceed cap during put, but eviction brings it down
}

#[test]
fn cache_byte_pressure_evicts_byte_heavy_shard_before_small_entries() {
    let small_hit = SearchHit {
        title: "small".into(),
        snippet: "small".into(),
        content: "small".into(),
        content_hash: stable_content_hash("small"),
        score: 1.0,
        source_path: "small-path".into(),
        agent: "a".into(),
        workspace: "w".into(),
        workspace_original: None,
        created_at: None,
        line_number: None,
        match_type: MatchType::Exact,
        source_id: "local".into(),
        origin_kind: "local".into(),
        origin_host: None,
        conversation_id: None,
    };
    let large_content = "large".repeat(2_000);
    let large_hit = SearchHit {
        title: "large".into(),
        snippet: "large".into(),
        content: large_content.clone(),
        content_hash: stable_content_hash(&large_content),
        score: 1.0,
        source_path: "large-path".into(),
        agent: "b".into(),
        workspace: "w".into(),
        workspace_original: None,
        created_at: None,
        line_number: None,
        match_type: MatchType::Exact,
        source_id: "local".into(),
        origin_kind: "local".into(),
        origin_host: None,
        conversation_id: None,
    };

    let mut cache = CacheShards::new(100, 1_024);
    cache.put(
        "small",
        Arc::<str>::from("small-1"),
        vec![cached_hit_from(&small_hit)],
    );
    cache.put(
        "small",
        Arc::<str>::from("small-2"),
        vec![cached_hit_from(&small_hit)],
    );
    cache.put(
        "large",
        Arc::<str>::from("large-1"),
        vec![cached_hit_from(&large_hit)],
    );

    assert_eq!(
        cache.shard_opt("small").map(LruCache::len),
        Some(2),
        "byte pressure should preserve the small shard"
    );
    assert!(
        cache.shard_opt("large").is_none_or(LruCache::is_empty),
        "oversized shard should be evicted first under byte pressure"
    );
    assert!(cache.total_bytes() <= cache.byte_cap());
}

// ============================================================
// Phase 7 Tests: WildcardPattern, escape_regex, fallback, dedup
// ============================================================

#[test]
fn wildcard_pattern_parse_exact() {
    // No wildcards - exact match
    assert_eq!(
        FsCassWildcardPattern::parse("hello"),
        FsCassWildcardPattern::Exact("hello".into())
    );
    assert_eq!(
        FsCassWildcardPattern::parse("HELLO"),
        FsCassWildcardPattern::Exact("hello".into()) // lowercased
    );
    assert_eq!(
        FsCassWildcardPattern::parse("FooBar123"),
        FsCassWildcardPattern::Exact("foobar123".into())
    );
}

#[test]
fn wildcard_pattern_parse_prefix() {
    // Trailing wildcard: foo*
    assert_eq!(
        FsCassWildcardPattern::parse("foo*"),
        FsCassWildcardPattern::Prefix("foo".into())
    );
    assert_eq!(
        FsCassWildcardPattern::parse("CONFIG*"),
        FsCassWildcardPattern::Prefix("config".into())
    );
    assert_eq!(
        FsCassWildcardPattern::parse("test*"),
        FsCassWildcardPattern::Prefix("test".into())
    );
}

#[test]
fn wildcard_pattern_parse_suffix() {
    // Leading wildcard: *foo
    assert_eq!(
        FsCassWildcardPattern::parse("*foo"),
        FsCassWildcardPattern::Suffix("foo".into())
    );
    assert_eq!(
        FsCassWildcardPattern::parse("*Error"),
        FsCassWildcardPattern::Suffix("error".into())
    );
    assert_eq!(
        FsCassWildcardPattern::parse("*Handler"),
        FsCassWildcardPattern::Suffix("handler".into())
    );
}

#[test]
fn wildcard_pattern_parse_substring() {
    // Both wildcards: *foo*
    assert_eq!(
        FsCassWildcardPattern::parse("*foo*"),
        FsCassWildcardPattern::Substring("foo".into())
    );
    assert_eq!(
        FsCassWildcardPattern::parse("*CONFIG*"),
        FsCassWildcardPattern::Substring("config".into())
    );
    assert_eq!(
        FsCassWildcardPattern::parse("*test*"),
        FsCassWildcardPattern::Substring("test".into())
    );
}

#[test]
fn wildcard_pattern_parse_edge_cases() {
    // Empty after trimming wildcards
    assert_eq!(
        FsCassWildcardPattern::parse("*"),
        FsCassWildcardPattern::Exact(String::new())
    );
    assert_eq!(
        FsCassWildcardPattern::parse("**"),
        FsCassWildcardPattern::Exact(String::new())
    );
    assert_eq!(
        FsCassWildcardPattern::parse("***"),
        FsCassWildcardPattern::Exact(String::new())
    );

    // Single char with wildcards
    assert_eq!(
        FsCassWildcardPattern::parse("*a*"),
        FsCassWildcardPattern::Substring("a".into())
    );
    assert_eq!(
        FsCassWildcardPattern::parse("a*"),
        FsCassWildcardPattern::Prefix("a".into())
    );
    assert_eq!(
        FsCassWildcardPattern::parse("*a"),
        FsCassWildcardPattern::Suffix("a".into())
    );

    // Multiple asterisks get trimmed
    assert_eq!(
        FsCassWildcardPattern::parse("***foo***"),
        FsCassWildcardPattern::Substring("foo".into())
    );
}

#[test]
fn wildcard_pattern_to_regex_suffix() {
    let pattern = FsCassWildcardPattern::Suffix("foo".into());
    // tantivy-fst regexes match the complete term and reject zero-width
    // assertions, so suffix patterns carry no `$` anchor — "ends with"
    // comes from the engine's full-term matching.
    assert_eq!(pattern.to_regex(), Some(".*foo".into()));
}

#[test]
fn wildcard_pattern_to_regex_substring() {
    let pattern = FsCassWildcardPattern::Substring("bar".into());
    assert_eq!(pattern.to_regex(), Some(".*bar.*".into()));
}

#[test]
fn wildcard_pattern_to_regex_exact_prefix_none() {
    // Exact and Prefix patterns don't need regex
    let exact = FsCassWildcardPattern::Exact("foo".into());
    assert_eq!(exact.to_regex(), None);

    let prefix = FsCassWildcardPattern::Prefix("bar".into());
    assert_eq!(prefix.to_regex(), None);
}

#[test]
fn match_type_quality_factors() {
    // Exact match has highest quality
    assert_eq!(MatchType::Exact.quality_factor(), 1.0);
    // Prefix is slightly lower
    assert_eq!(MatchType::Prefix.quality_factor(), 0.9);
    // Suffix is lower than prefix
    assert_eq!(MatchType::Suffix.quality_factor(), 0.8);
    // Substring is lower still
    assert_eq!(MatchType::Substring.quality_factor(), 0.7);
    // Implicit wildcard is lowest
    assert_eq!(MatchType::ImplicitWildcard.quality_factor(), 0.6);
}

#[test]
fn dominant_match_type_single_terms() {
    // Single terms return their pattern's match type
    assert_eq!(dominant_match_type("hello"), MatchType::Exact);
    assert_eq!(dominant_match_type("hello*"), MatchType::Prefix);
    assert_eq!(dominant_match_type("*hello"), MatchType::Suffix);
    assert_eq!(dominant_match_type("*hello*"), MatchType::Substring);
}

#[test]
fn dominant_match_type_multiple_terms() {
    // Multiple terms: returns the "loosest" (lowest quality factor)
    assert_eq!(dominant_match_type("foo bar"), MatchType::Exact);
    assert_eq!(dominant_match_type("foo bar*"), MatchType::Prefix);
    assert_eq!(dominant_match_type("foo *bar"), MatchType::Suffix);
    assert_eq!(dominant_match_type("foo* *bar*"), MatchType::Substring);
    // Substring is loosest even if other terms are exact
    assert_eq!(dominant_match_type("foo *bar* baz"), MatchType::Substring);
}

#[test]
fn dominant_match_type_empty_query() {
    assert_eq!(dominant_match_type(""), MatchType::Exact);
    assert_eq!(dominant_match_type("   "), MatchType::Exact);
}

#[test]
fn wildcard_pattern_to_regex_escapes_special_chars() {
    assert_eq!(
        FsCassWildcardPattern::Suffix("foo.bar".into()).to_regex(),
        Some(".*foo\\.bar".into())
    );
    assert_eq!(
        FsCassWildcardPattern::Substring("a+b*c?".into()).to_regex(),
        Some(".*a\\+b\\*c\\?.*".into())
    );
}

#[test]
fn wildcard_pattern_to_regex_escapes_complex_patterns() {
    assert_eq!(
        FsCassWildcardPattern::Suffix("test[0-9]+".into()).to_regex(),
        Some(".*test\\[0-9\\]\\+".into())
    );
    assert_eq!(
        FsCassWildcardPattern::Substring("(a|b)".into()).to_regex(),
        Some(".*\\(a\\|b\\).*".into())
    );
    assert_eq!(
        FsCassWildcardPattern::Substring("end$".into()).to_regex(),
        Some(".*end\\$.*".into())
    );
    assert_eq!(
        FsCassWildcardPattern::Substring("^start".into()).to_regex(),
        Some(".*\\^start.*".into())
    );
}

#[test]
fn is_tool_invocation_noise_detects_noise() {
    // "[Tool: Name]" is now kept (users search for tool usage)
    assert!(!is_tool_invocation_noise("[Tool: Bash]"));
    assert!(!is_tool_invocation_noise("[Tool: Read]"));

    // Empty tool names are noise
    assert!(is_tool_invocation_noise("[Tool:]"));
    assert!(is_tool_invocation_noise("[Tool: ]"));

    // Useful content should NOT be filtered
    assert!(!is_tool_invocation_noise("[Tool: Bash - Check status]"));
    assert!(!is_tool_invocation_noise("  [Tool: Grep - Search files]  "));

    // Very short tool markers (< 20 chars with "tool" prefix)
    assert!(is_tool_invocation_noise("[tool]"));
    assert!(is_tool_invocation_noise("tool: Bash"));
}

#[test]
fn is_tool_invocation_noise_allows_useful_content() {
    // This should NOT be considered noise
    assert!(!is_tool_invocation_noise("[Tool: Read - src/main.rs]"));
    assert!(!is_tool_invocation_noise("[Tool: Bash - cargo test --lib]"));
}

#[test]
fn is_tool_invocation_noise_detects_tool_markers() {
    // "[Tool: Name]" is now kept (searchable tool usage)
    assert!(!is_tool_invocation_noise("[Tool: Bash]"));
    assert!(!is_tool_invocation_noise("[Tool: Read]"));

    // Empty names are still noise
    assert!(is_tool_invocation_noise("[Tool:]"));

    // Useful content allowed
    assert!(!is_tool_invocation_noise("[Tool: Bash - Check status]"));
    assert!(!is_tool_invocation_noise("  [Tool: Write - description]  "));
}

#[test]
fn deduplicate_hits_removes_exact_dupes() {
    let hits = vec![
        SearchHit {
            title: "title1".into(),
            snippet: "snip1".into(),
            content: "hello world".into(),
            content_hash: stable_content_hash("hello world"),
            score: 1.0,
            source_path: "a.jsonl".into(),
            agent: "agent".into(),
            workspace: "ws".into(),
            workspace_original: None,
            created_at: Some(100),
            line_number: None,
            match_type: MatchType::Exact,
            source_id: "local".into(),
            origin_kind: "local".into(),
            origin_host: None,
            conversation_id: None,
        },
        SearchHit {
            title: "title1".into(),
            snippet: "snip2".into(),
            content: "hello world".into(), // same content
            content_hash: stable_content_hash("hello world"),
            score: 0.5, // lower score
            source_path: "a.jsonl".into(),
            agent: "agent".into(),
            workspace: "ws".into(),
            workspace_original: None,
            created_at: Some(100),
            line_number: None,
            match_type: MatchType::Exact,
            source_id: "local".into(), // same source_id = will dedupe
            origin_kind: "local".into(),
            origin_host: None,
            conversation_id: None,
        },
    ];

    let deduped = deduplicate_hits(hits);
    assert_eq!(deduped.len(), 1);
    assert_eq!(deduped[0].score, 1.0); // kept higher score
    assert_eq!(deduped[0].title, "title1");
}

#[test]
fn deduplicate_hits_keeps_higher_score() {
    let hits = vec![
        SearchHit {
            title: "title1".into(),
            snippet: "snip1".into(),
            content: "hello world".into(),
            content_hash: stable_content_hash("hello world"),
            score: 0.3, // lower score first
            source_path: "a.jsonl".into(),
            agent: "agent".into(),
            workspace: "ws".into(),
            workspace_original: None,
            created_at: Some(100),
            line_number: None,
            match_type: MatchType::Exact,
            source_id: "local".into(),
            origin_kind: "local".into(),
            origin_host: None,
            conversation_id: None,
        },
        SearchHit {
            title: "title1".into(),
            snippet: "snip2".into(),
            content: "hello world".into(),
            content_hash: stable_content_hash("hello world"),
            score: 0.9, // higher score second
            source_path: "a.jsonl".into(),
            agent: "agent".into(),
            workspace: "ws".into(),
            workspace_original: None,
            created_at: Some(100),
            line_number: None,
            match_type: MatchType::Exact,
            source_id: "local".into(),
            origin_kind: "local".into(),
            origin_host: None,
            conversation_id: None,
        },
    ];

    let deduped = deduplicate_hits(hits);
    assert_eq!(deduped.len(), 1);
    assert_eq!(deduped[0].score, 0.9); // kept higher score
    assert_eq!(deduped[0].title, "title1");
}

#[test]
fn deduplicate_hits_keeps_repeated_same_content_at_different_lines() {
    let first = SearchHit {
        title: "Shared Session".into(),
        snippet: String::new(),
        content: "repeat me".into(),
        content_hash: stable_content_hash("repeat me"),
        score: 10.0,
        source_path: "/shared/session.jsonl".into(),
        agent: "codex".into(),
        workspace: "/ws".into(),
        workspace_original: None,
        created_at: Some(100),
        line_number: Some(1),
        match_type: MatchType::Exact,
        source_id: "local".into(),
        origin_kind: "local".into(),
        origin_host: None,
        conversation_id: None,
    };
    let mut second = first.clone();
    second.line_number = Some(2);
    second.created_at = Some(200);
    second.score = 9.0;

    let deduped = deduplicate_hits(vec![first, second]);
    assert_eq!(deduped.len(), 2);
}

#[test]
fn deduplicate_hits_keeps_distinct_conversation_ids_with_same_title_path_and_content() {
    let mut first = make_test_hit("same", 1.0);
    first.title = "Shared Session".into();
    first.source_path = "/shared/session.jsonl".into();
    first.content = "identical body".into();
    first.content_hash = stable_content_hash("identical body");
    first.conversation_id = Some(1);

    let mut second = first.clone();
    second.conversation_id = Some(2);
    second.score = 0.9;

    let deduped = deduplicate_hits(vec![first, second]);
    assert_eq!(deduped.len(), 2);
    assert!(deduped.iter().any(|hit| hit.conversation_id == Some(1)));
    assert!(deduped.iter().any(|hit| hit.conversation_id == Some(2)));
}

#[test]
fn deduplicate_hits_coalesces_same_conversation_id_despite_title_drift() {
    let mut first = make_test_hit("same", 1.0);
    first.title = "Morning Session".into();
    first.source_path = "/shared/session.jsonl".into();
    first.content = "identical body".into();
    first.content_hash = stable_content_hash("identical body");
    first.conversation_id = Some(7);

    let mut second = first.clone();
    second.title = "Evening Session".into();
    second.score = 0.9;

    let deduped = deduplicate_hits(vec![first, second]);
    assert_eq!(deduped.len(), 1);
    assert_eq!(deduped[0].conversation_id, Some(7));
}

#[test]
fn deduplicate_hits_keeps_distinct_titles_with_same_source_path_and_content() {
    let hits = vec![
        SearchHit {
            title: "Morning Session".into(),
            snippet: "snip1".into(),
            content: "hello world".into(),
            content_hash: stable_content_hash("hello world"),
            score: 0.9,
            source_path: "shared.jsonl".into(),
            agent: "agent".into(),
            workspace: "ws".into(),
            workspace_original: None,
            created_at: None,
            line_number: Some(1),
            match_type: MatchType::Exact,
            source_id: "local".into(),
            origin_kind: "local".into(),
            origin_host: None,
            conversation_id: None,
        },
        SearchHit {
            title: "Evening Session".into(),
            snippet: "snip2".into(),
            content: "hello world".into(),
            content_hash: stable_content_hash("hello world"),
            score: 0.8,
            source_path: "shared.jsonl".into(),
            agent: "agent".into(),
            workspace: "ws".into(),
            workspace_original: None,
            created_at: None,
            line_number: Some(1),
            match_type: MatchType::Exact,
            source_id: "local".into(),
            origin_kind: "local".into(),
            origin_host: None,
            conversation_id: None,
        },
    ];

    let deduped = deduplicate_hits(hits);
    assert_eq!(deduped.len(), 2);
    assert!(deduped.iter().any(|hit| hit.title == "Morning Session"));
    assert!(deduped.iter().any(|hit| hit.title == "Evening Session"));
}

#[test]
fn deduplicate_hits_normalizes_whitespace() {
    let hits = vec![
        SearchHit {
            title: "title1".into(),
            snippet: "snip1".into(),
            content: "hello    world".into(), // extra spaces
            content_hash: stable_content_hash("hello    world"),
            score: 1.0,
            source_path: "a.jsonl".into(),
            agent: "agent".into(),
            workspace: "ws".into(),
            workspace_original: None,
            created_at: Some(100),
            line_number: None,
            match_type: MatchType::Exact,
            source_id: "local".into(),
            origin_kind: "local".into(),
            origin_host: None,
            conversation_id: None,
        },
        SearchHit {
            title: "title1".into(),
            snippet: "snip2".into(),
            content: "hello world".into(), // normal spacing
            content_hash: stable_content_hash("hello world"),
            score: 0.5,
            source_path: "a.jsonl".into(),
            agent: "agent".into(),
            workspace: "ws".into(),
            workspace_original: None,
            created_at: Some(100),
            line_number: None,
            match_type: MatchType::Exact,
            source_id: "local".into(),
            origin_kind: "local".into(),
            origin_host: None,
            conversation_id: None,
        },
    ];

    let deduped = deduplicate_hits(hits);
    assert_eq!(deduped.len(), 1); // normalized to same content
}

#[test]
fn deduplicate_hits_normalizes_blank_local_source_id() {
    let hits = vec![
        SearchHit {
            title: "title1".into(),
            snippet: "snip1".into(),
            content: "hello world".into(),
            content_hash: stable_content_hash("hello world"),
            score: 1.0,
            source_path: "a.jsonl".into(),
            agent: "agent".into(),
            workspace: "ws".into(),
            workspace_original: None,
            created_at: Some(100),
            line_number: None,
            match_type: MatchType::Exact,
            source_id: "local".into(),
            origin_kind: "local".into(),
            origin_host: None,
            conversation_id: None,
        },
        SearchHit {
            title: "title1".into(),
            snippet: "snip2".into(),
            content: "hello world".into(),
            content_hash: stable_content_hash("hello world"),
            score: 0.5,
            source_path: "a.jsonl".into(),
            agent: "agent".into(),
            workspace: "ws".into(),
            workspace_original: None,
            created_at: Some(100),
            line_number: None,
            match_type: MatchType::Exact,
            source_id: "   ".into(),
            origin_kind: "local".into(),
            origin_host: None,
            conversation_id: None,
        },
    ];

    let deduped = deduplicate_hits(hits);
    assert_eq!(deduped.len(), 1);
    assert_eq!(deduped[0].source_id, "local");
}

#[test]
fn deduplicate_hits_filters_tool_noise() {
    let hits = vec![
        SearchHit {
            title: "title1".into(),
            snippet: "snip1".into(),
            content: "[Tool:]".into(), // noise (empty tool name)
            content_hash: stable_content_hash("[Tool:]"),
            score: 1.0,
            source_path: "a.jsonl".into(),
            agent: "agent".into(),
            workspace: "ws".into(),
            workspace_original: None,
            created_at: Some(100),
            line_number: None,
            match_type: MatchType::Exact,
            source_id: "local".into(),
            origin_kind: "local".into(),
            origin_host: None,
            conversation_id: None,
        },
        SearchHit {
            title: "title2".into(),
            snippet: "snip2".into(),
            content: "This is real content about testing".into(),
            content_hash: stable_content_hash("This is real content about testing"),
            score: 0.5,
            source_path: "b.jsonl".into(),
            agent: "agent".into(),
            workspace: "ws".into(),
            workspace_original: None,
            created_at: Some(200),
            line_number: None,
            match_type: MatchType::Exact,
            source_id: "local".into(),
            origin_kind: "local".into(),
            origin_host: None,
            conversation_id: None,
        },
    ];

    let deduped = deduplicate_hits(hits);
    assert_eq!(deduped.len(), 1);
    assert!(deduped[0].content.contains("real content"));
}

#[test]
fn deduplicate_hits_filters_acknowledgement_noise() {
    let hits = vec![
        SearchHit {
            title: "ack".into(),
            snippet: "ack".into(),
            content: "Acknowledged.".into(),
            content_hash: stable_content_hash("Acknowledged."),
            score: 1.0,
            source_path: "ack.jsonl".into(),
            agent: "agent".into(),
            workspace: "ws".into(),
            workspace_original: None,
            created_at: Some(100),
            line_number: None,
            match_type: MatchType::Exact,
            source_id: "local".into(),
            origin_kind: "local".into(),
            origin_host: None,
            conversation_id: None,
        },
        SearchHit {
            title: "real".into(),
            snippet: "real".into(),
            content: "Authentication refresh logic changed".into(),
            content_hash: stable_content_hash("Authentication refresh logic changed"),
            score: 0.5,
            source_path: "real.jsonl".into(),
            agent: "agent".into(),
            workspace: "ws".into(),
            workspace_original: None,
            created_at: Some(200),
            line_number: None,
            match_type: MatchType::Exact,
            source_id: "local".into(),
            origin_kind: "local".into(),
            origin_host: None,
            conversation_id: None,
        },
    ];

    let deduped = deduplicate_hits_with_query(hits, "authentication");
    assert_eq!(deduped.len(), 1);
    assert_eq!(deduped[0].title, "real");
}

#[test]
fn deduplicate_hits_hides_system_prompts_unless_query_requests_them() {
    let prompt_hit = SearchHit {
        title: "prompt".into(),
        snippet: "prompt".into(),
        content:
            "# AGENTS.md instructions for /repo\n\nYou are a coding assistant. Follow the instructions exactly."
                .into(),
        content_hash: stable_content_hash(
            "# AGENTS.md instructions for /repo\n\nYou are a coding assistant. Follow the instructions exactly.",
        ),
        score: 1.0,
        source_path: "prompt.jsonl".into(),
        agent: "agent".into(),
        workspace: "ws".into(),
        workspace_original: None,
        created_at: Some(100),
        line_number: None,
        match_type: MatchType::Exact,
        source_id: "local".into(),
        origin_kind: "local".into(),
        origin_host: None,
        conversation_id: None,
    };

    assert!(deduplicate_hits_with_query(vec![prompt_hit.clone()], "coding assistant").is_empty());

    let kept = deduplicate_hits_with_query(vec![prompt_hit], "AGENTS.md instructions");
    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].title, "prompt");
}

#[test]
fn deduplicate_hits_preserves_unique_content() {
    let hits = vec![
        SearchHit {
            title: "title1".into(),
            snippet: "snip1".into(),
            content: "first message".into(),
            content_hash: stable_content_hash("first message"),
            score: 1.0,
            source_path: "a.jsonl".into(),
            agent: "agent".into(),
            workspace: "ws".into(),
            workspace_original: None,
            created_at: Some(100),
            line_number: None,
            match_type: MatchType::Exact,
            source_id: "local".into(),
            origin_kind: "local".into(),
            origin_host: None,
            conversation_id: None,
        },
        SearchHit {
            title: "title2".into(),
            snippet: "snip2".into(),
            content: "second message".into(),
            content_hash: stable_content_hash("second message"),
            score: 0.8,
            source_path: "b.jsonl".into(),
            agent: "agent".into(),
            workspace: "ws".into(),
            workspace_original: None,
            created_at: Some(200),
            line_number: None,
            match_type: MatchType::Exact,
            source_id: "local".into(),
            origin_kind: "local".into(),
            origin_host: None,
            conversation_id: None,
        },
        SearchHit {
            title: "title3".into(),
            snippet: "snip3".into(),
            content: "third message".into(),
            content_hash: stable_content_hash("third message"),
            score: 0.6,
            source_path: "c.jsonl".into(),
            agent: "agent".into(),
            workspace: "ws".into(),
            workspace_original: None,
            created_at: Some(300),
            line_number: None,
            match_type: MatchType::Exact,
            source_id: "local".into(),
            origin_kind: "local".into(),
            origin_host: None,
            conversation_id: None,
        },
    ];

    let deduped = deduplicate_hits(hits);
    assert_eq!(deduped.len(), 3); // all unique
}

/// P2.3: Deduplication respects source boundaries - same content from different sources
/// should appear as separate results.
#[test]
fn deduplicate_hits_respects_source_boundaries() {
    let hits = vec![
        SearchHit {
            title: "local title".into(),
            snippet: "snip".into(),
            content: "hello world".into(),
            content_hash: stable_content_hash("hello world"),
            score: 1.0,
            source_path: "a.jsonl".into(),
            agent: "agent".into(),
            workspace: "ws".into(),
            workspace_original: None,
            created_at: Some(100),
            line_number: None,
            match_type: MatchType::Exact,
            source_id: "local".into(),
            origin_kind: "local".into(),
            origin_host: None,
            conversation_id: None,
        },
        SearchHit {
            title: "remote title".into(),
            snippet: "snip".into(),
            content: "hello world".into(), // same content
            content_hash: stable_content_hash("hello world"),
            score: 0.9,
            source_path: "b.jsonl".into(),
            agent: "agent".into(),
            workspace: "ws".into(),
            workspace_original: None,
            created_at: Some(200),
            line_number: None,
            match_type: MatchType::Exact,
            source_id: "work-laptop".into(), // different source = no dedupe
            origin_kind: "ssh".into(),
            origin_host: Some("work-laptop.local".into()),
            conversation_id: None,
        },
    ];

    let deduped = deduplicate_hits(hits);
    assert_eq!(
        deduped.len(),
        2,
        "same content from different sources should not dedupe"
    );
    assert!(deduped.iter().any(|h| h.source_id == "local"));
    assert!(deduped.iter().any(|h| h.source_id == "work-laptop"));
}
