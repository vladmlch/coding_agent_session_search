use super::super::*;
use super::helpers::*;
use crate::connectors::{NormalizedConversation, NormalizedMessage};
use crate::search::tantivy::TantivyIndex;
use frankensearch::{ScoreSource as FsScoreSource, ScoredResult as FsScoredResult};
use serde_json::json;
use std::sync::atomic::AtomicBool;
use tempfile::TempDir;

#[test]
fn result_cache_unlimited_search_uses_reloaded_document_count() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;
    let conversation = |id: i64| NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: Some(format!("reload-session-{id}")),
        title: Some(format!("Reload fixture {id}")),
        workspace: Some("/cache-fixture".into()),
        source_path: dir.path().join(format!("reload-session-{id}.jsonl")),
        started_at: Some(1_700_000_000_000 + id),
        ended_at: None,
        metadata: json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(1_700_000_000_000 + id),
            content: format!("cacheword newly visible message {id}"),
            extra: json!({}),
            snippets: vec![],
            invocations: vec![],
        }],
    };
    index.add_conversation(&conversation(0))?;
    index.commit()?;
    let warm = open_cache_test_client(dir.path())?;
    let initial = warm.search("cacheword", SearchFilters::default(), 0, 0, FieldMask::FULL)?;
    assert_eq!(initial.len(), 1);

    for id in 1..3 {
        index.add_conversation(&conversation(id))?;
    }
    index.commit()?;
    assert_eq!(
        warm.total_docs(),
        1,
        "the retained reader has not reloaded yet"
    );

    // Admit the next reload deterministically. The old ordering computed
    // an unlimited request's cap from the one-document reader before this
    // reload and silently truncated the newly admitted generation to one.
    *warm.last_reload.lock().unwrap() = None;
    let actual = warm.search("cacheword", SearchFilters::default(), 0, 0, FieldMask::FULL)?;
    assert_eq!(
        actual.len(),
        3,
        "all newly published messages must be returned"
    );
    let cold = open_cache_test_client(dir.path())?;
    let expected = cold.search("cacheword", SearchFilters::default(), 0, 0, FieldMask::FULL)?;
    assert_eq!(
        serde_json::to_value(&actual)?,
        serde_json::to_value(&expected)?
    );
    Ok(())
}

#[test]
fn result_cache_query_extension_matches_cold_ranking_and_repeats_exactly() -> Result<()> {
    let dir = lexical_cache_fixture()?;
    let warm = open_cache_test_client(dir.path())?;
    let cold = open_cache_test_client(dir.path())?;
    let prefix = warm.search("cacheword", SearchFilters::default(), 1, 0, FieldMask::FULL)?;
    assert_eq!(prefix.len(), 1);
    assert!(
        prefix[0].content.contains("rareword"),
        "cached hit survives the extended query"
    );
    let expected = cold.search(
        "cacheword rareword",
        SearchFilters::default(),
        1,
        0,
        FieldMask::FULL,
    )?;
    assert_eq!(expected.len(), 1);
    assert_ne!(
        prefix[0].source_path, expected[0].source_path,
        "the new term changes the winner"
    );
    let actual = warm.search(
        "cacheword rareword",
        SearchFilters::default(),
        1,
        0,
        FieldMask::FULL,
    )?;
    assert_eq!(
        serde_json::to_value(&actual)?,
        serde_json::to_value(&expected)?
    );
    let hits_before = warm.cache_stats().cache_hits;
    let repeated = warm.search(
        "cacheword rareword",
        SearchFilters::default(),
        1,
        0,
        FieldMask::FULL,
    )?;
    assert_eq!(
        serde_json::to_value(&repeated)?,
        serde_json::to_value(&expected)?
    );
    assert_eq!(warm.cache_stats().cache_hits, hits_before + 1);

    // A short cached page cannot certify a larger request or its offset.
    for (limit, offset) in [(2, 0), (1, 1)] {
        let expected = cold.search(
            "cacheword rareword",
            SearchFilters::default(),
            limit,
            offset,
            FieldMask::new(true, true, true, false),
        )?;
        let actual = warm.search(
            "cacheword rareword",
            SearchFilters::default(),
            limit,
            offset,
            FieldMask::FULL,
        )?;
        assert_eq!(
            serde_json::to_value(&actual)?,
            serde_json::to_value(&expected)?
        );
    }
    Ok(())
}

#[test]
fn result_cache_keeps_field_projections_independent_in_both_orders() -> Result<()> {
    let dir = lexical_cache_fixture()?;
    let projected = FieldMask::new(false, true, false, true);
    for masks in [[projected, FieldMask::FULL], [FieldMask::FULL, projected]] {
        let warm = open_cache_test_client(dir.path())?;
        for mask in masks {
            let cold = open_cache_test_client(dir.path())?;
            let expected = cold.search("cacheword", SearchFilters::default(), 1, 0, mask)?;
            let actual = warm.search("cacheword", SearchFilters::default(), 1, 0, mask)?;
            assert_eq!(actual.len(), 1);
            assert_eq!(actual[0].content.is_empty(), !mask.needs_content());
            assert_eq!(actual[0].title.is_empty(), !mask.wants_title());
            assert_eq!(
                serde_json::to_value(&actual)?,
                serde_json::to_value(&expected)?
            );
            let hits_before = warm.cache_stats().cache_hits;
            let repeated = warm.search("cacheword", SearchFilters::default(), 1, 0, mask)?;
            assert_eq!(
                serde_json::to_value(&repeated)?,
                serde_json::to_value(&expected)?
            );
            assert_eq!(warm.cache_stats().cache_hits, hits_before + 1);
        }
    }
    Ok(())
}

#[test]
fn result_cache_rejects_late_publication_from_an_invalidated_generation() -> Result<()> {
    let dir = lexical_cache_fixture()?;
    let client = open_cache_test_client(dir.path())?;
    let hits = client.search("cacheword", SearchFilters::default(), 1, 0, FieldMask::FULL)?;
    let epoch = client.reload_epoch.load(Ordering::SeqCst);
    let generation = client
        .cache_stats()
        .reader_generation
        .expect("tracked generation");
    client.track_generation(generation + 1);
    client.put_cache(
        "cacheword",
        &SearchFilters::default(),
        FieldMask::FULL,
        epoch,
        &hits,
    );
    assert!(
        client
            .cached_query_hits("cacheword", &SearchFilters::default(), FieldMask::FULL)
            .is_none()
    );
    Ok(())
}

#[test]
fn cache_skips_complex_queries() -> Result<()> {
    let dir = lexical_cache_fixture()?;
    let client = open_cache_test_client(dir.path())?;

    // Wildcard query should skip cache logic entirely (no miss recorded)
    let _ = client.search("foo*", SearchFilters::default(), 10, 0, FieldMask::FULL);
    let stats = client.cache_stats();
    assert_eq!(
        stats.cache_miss, 0,
        "Wildcard query should not trigger cache miss"
    );

    // Boolean query should skip cache
    let _ = client.search(
        "foo OR bar",
        SearchFilters::default(),
        10,
        0,
        FieldMask::FULL,
    );
    let stats = client.cache_stats();
    assert_eq!(
        stats.cache_miss, 0,
        "Boolean query should not trigger cache miss"
    );

    // Simple query should trigger miss
    let _ = client.search("simple", SearchFilters::default(), 10, 0, FieldMask::FULL);
    let stats = client.cache_stats();
    assert_eq!(
        stats.cache_miss, 1,
        "Simple query should trigger cache miss"
    );
    Ok(())
}

#[test]
fn cache_query_lookup_preserves_utf8_and_requires_exact_text() {
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

    let hits = vec![SearchHit {
        title: "こんにちは".into(),
        snippet: String::new(),
        content: "こんにちは 世界".into(),
        content_hash: stable_content_hash("こんにちは 世界"),
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
    }];

    client.put_cache("こん", &SearchFilters::default(), FieldMask::FULL, 0, &hits);

    let cached = client
        .cached_query_hits("こん", &SearchFilters::default(), FieldMask::FULL)
        .unwrap();
    assert_eq!(cached.len(), 1);
    assert_eq!(cached[0].hit.title, "こんにちは");
    assert!(
        client
            .cached_query_hits("こんにちは", &SearchFilters::default(), FieldMask::FULL)
            .is_none()
    );
}

#[test]
fn cache_metrics_count_each_outcome() {
    let metrics = Metrics::default();
    metrics.inc_cache_hits();
    metrics.inc_cache_miss();
    metrics.inc_cache_shortfall();
    metrics.inc_reload();
    let (hits, outcomes, shortfall, reloads, _) = metrics.snapshot_all();
    assert_eq!((hits, outcomes.cold_miss, shortfall, reloads), (1, 1, 1, 1));
}

#[test]
fn b7tb0_bounded_reload_times_out_without_starting_parallel_reloads() {
    let reload_in_flight = Arc::new(AtomicBool::new(false));
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let started = Instant::now();
    let error = run_reload_bounded(
        move || {
            release_rx.recv().map_err(|error| error.to_string())?;
            Ok(())
        },
        Arc::clone(&reload_in_flight),
        Duration::from_millis(20),
    )
    .expect_err("blocked reload must hit its deadline");
    assert!(error.to_string().contains("exceeded 20 ms"));
    assert!(started.elapsed() < Duration::from_millis(250));
    assert!(reload_in_flight.load(Ordering::Acquire));

    let parallel = run_reload_bounded(
        || Ok(()),
        Arc::clone(&reload_in_flight),
        Duration::from_millis(20),
    )
    .expect_err("single-flight guard must reject a parallel reload");
    assert!(parallel.to_string().contains("already in flight"));

    release_tx.send(()).expect("release reload worker");
    let deadline = Instant::now() + Duration::from_secs(1);
    while reload_in_flight.load(Ordering::Acquire) && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let successful_reload = run_reload_bounded(
        || Ok(()),
        Arc::clone(&reload_in_flight),
        Duration::from_secs(1),
    );
    assert!(
        successful_reload.is_ok() && !reload_in_flight.load(Ordering::Acquire),
        "a successful return must mean the worker no longer owns the single-flight slot"
    );
}

#[test]
fn b7tb0_metrics_keep_stale_reload_failure_and_fallback_distinct() {
    let metrics = Metrics::default();
    for lookup in [
        crate::daemon_runtime_state::CacheLookup {
            cache_hit: true,
            cached_generation: Some(1),
            current_generation: 2,
            reload_attempted: false,
            reload_succeeded: false,
            served_fallback: false,
        },
        crate::daemon_runtime_state::CacheLookup {
            cache_hit: true,
            cached_generation: Some(1),
            current_generation: 2,
            reload_attempted: true,
            reload_succeeded: false,
            served_fallback: false,
        },
        crate::daemon_runtime_state::CacheLookup {
            cache_hit: false,
            cached_generation: None,
            current_generation: 2,
            reload_attempted: false,
            reload_succeeded: false,
            served_fallback: true,
        },
        crate::daemon_runtime_state::CacheLookup {
            cache_hit: true,
            cached_generation: Some(1),
            current_generation: 2,
            reload_attempted: true,
            reload_succeeded: true,
            served_fallback: false,
        },
    ] {
        metrics.record_cache_lookup(lookup);
    }

    let (_, outcomes, _, _, _) = metrics.snapshot_all();
    assert_eq!(outcomes.stale_generation_miss, 1);
    assert_eq!(outcomes.reload_failure, 1);
    assert_eq!(outcomes.fallback, 1);
    assert_eq!(outcomes.forced_reload, 1);
    assert_eq!(outcomes.total(), 4);
}

#[test]
fn progressive_lexical_hit_omits_unused_content() {
    let hit = SearchHit {
        title: "hello world".into(),
        snippet: "hello **world**".into(),
        content: "hello world from a much larger conversation body".into(),
        content_hash: stable_content_hash("hello world from a much larger conversation body"),
        score: 1.0,
        source_path: "p".into(),
        agent: "a".into(),
        workspace: "w".into(),
        workspace_original: None,
        created_at: None,
        line_number: Some(3),
        match_type: MatchType::Exact,
        source_id: "local".into(),
        origin_kind: "local".into(),
        origin_host: None,
        conversation_id: None,
    };

    let snippet_only =
        ProgressiveLexicalHit::from_search_hit(&hit, FieldMask::new(false, true, true, true));
    assert_eq!(snippet_only.title, hit.title);
    assert_eq!(snippet_only.snippet, hit.snippet);
    assert!(
        snippet_only.content.is_empty(),
        "snippet-only progressive cache should not retain full content"
    );
    assert_eq!(snippet_only.match_type, hit.match_type);
    assert_eq!(snippet_only.line_number, hit.line_number);
    assert_eq!(snippet_only.source_path, hit.source_path);
    assert_eq!(snippet_only.agent, hit.agent);
    assert_eq!(snippet_only.workspace, hit.workspace);

    let full = ProgressiveLexicalHit::from_search_hit(&hit, FieldMask::new(true, true, true, true));
    assert_eq!(full.content, hit.content);
}

#[test]
fn progressive_phase_reuses_lexical_cache_without_db_hydration() -> Result<()> {
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
    let field_mask = FieldMask::new(false, true, true, true);
    let lexical_hit = SearchHit {
        title: "lexical title".into(),
        snippet: "lexical snippet".into(),
        content: "full lexical body".into(),
        content_hash: stable_content_hash("full lexical body"),
        score: 0.0,
        source_path: "/tmp/session.jsonl".into(),
        agent: "codex".into(),
        workspace: "/tmp".into(),
        workspace_original: Some("/original".into()),
        created_at: Some(1_700_000_000_000),
        line_number: Some(7),
        match_type: MatchType::Exact,
        source_id: "local".into(),
        origin_kind: "local".into(),
        origin_host: None,
        conversation_id: None,
    };
    let mut lexical_cache = ProgressiveLexicalCache::default();
    lexical_cache.hits_by_message.insert(
        42,
        ProgressiveLexicalHit::from_search_hit(&lexical_hit, field_mask),
    );

    let hash_hex = "00".repeat(32);
    let results = vec![FsScoredResult {
        doc_id: format!("m|42|0|1|1|1|1|1700000000000|{hash_hex}").into(),
        score: 0.91,
        source: FsScoreSource::Lexical,
        index: None,
        fast_score: None,
        quality_score: None,
        lexical_score: Some(0.91),
        rerank_score: None,
        explanation: None,
        metadata: None,
    }];

    let result = client.progressive_phase_to_result(
        &results,
        ProgressivePhaseContext {
            query: "merged title",
            filters: &SearchFilters::default(),
            field_mask,
            lexical_cache: Some(&lexical_cache),
            limit: 1,
            fetch_limit: 1,
        },
    )?;

    assert_eq!(result.hits.len(), 1);
    assert_eq!(result.hits[0].title, lexical_hit.title);
    assert_eq!(result.hits[0].snippet, lexical_hit.snippet);
    assert!(
        result.hits[0].content.is_empty(),
        "masked lexical cache should still avoid carrying full content"
    );
    assert_eq!(result.hits[0].source_path, lexical_hit.source_path);
    assert_eq!(result.hits[0].score, 0.91);

    Ok(())
}
