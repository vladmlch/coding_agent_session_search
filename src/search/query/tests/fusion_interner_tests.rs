use super::super::*;
use super::helpers::*;
use crate::connectors::{NormalizedConversation, NormalizedMessage};
use crate::search::tantivy::TantivyIndex;
use frankensearch::Cx as FsCx;
use std::cmp::Ordering as CmpOrdering;
use tempfile::TempDir;

#[test]
#[ignore = "profiling harness for live hybrid progressive search"]
fn progressive_hybrid_profile_harness() -> Result<()> {
    let fixture = build_progressive_hybrid_fixture()?;
    let runtime = asupersync::runtime::RuntimeBuilder::current_thread()
        .build()
        .map_err(|err| anyhow!("build test runtime failed: {err}"))?;
    let iterations = 24usize;

    runtime.block_on(async {
        let cx = FsCx::for_request();
        fixture
            .client
            .search_progressive_with_callback(
                ProgressiveSearchRequest {
                    cx: &cx,
                    query: &fixture.query,
                    filters: SearchFilters::default(),
                    limit: 16,
                    sparse_threshold: 0,
                    field_mask: FieldMask::new(false, true, true, true),
                    mode: SearchMode::Hybrid,
                },
                |_| {},
            )
            .await
    })?;

    let mut initial_events = 0usize;
    let mut refined_events = 0usize;
    let mut total_hits = 0usize;
    for _ in 0..iterations {
        let mut refinement_error = None;
        runtime.block_on(async {
            let cx = FsCx::for_request();
            fixture
                .client
                .search_progressive_with_callback(
                    ProgressiveSearchRequest {
                        cx: &cx,
                        query: &fixture.query,
                        filters: SearchFilters::default(),
                        limit: 16,
                        sparse_threshold: 0,
                        field_mask: FieldMask::new(false, true, true, true),
                        mode: SearchMode::Hybrid,
                    },
                    |event| match event {
                        ProgressiveSearchEvent::Phase { kind, result, .. } => {
                            assert!(
                                !result.hits.is_empty(),
                                "progressive harness expects non-empty hits for each phase"
                            );
                            total_hits += result.hits.len();
                            match kind {
                                ProgressivePhaseKind::Initial => initial_events += 1,
                                ProgressivePhaseKind::Refined => refined_events += 1,
                            }
                        }
                        ProgressiveSearchEvent::RefinementFailed { error, .. } => {
                            refinement_error = Some(error);
                        }
                    },
                )
                .await
        })?;
        if let Some(error) = refinement_error {
            bail!("progressive harness refinement failed: {error}");
        }
    }

    assert_eq!(initial_events, iterations);
    assert_eq!(refined_events, iterations);
    assert!(
        total_hits >= iterations.saturating_mul(16),
        "harness should observe a full page for each phase"
    );

    Ok(())
}

// ==========================================================================
// StringInterner Tests (Opt 2.3)
// ==========================================================================

#[test]
fn interner_returns_same_arc_for_same_string() {
    let interner = StringInterner::new(100);

    let s1 = interner.intern("test_query");
    let s2 = interner.intern("test_query");

    // Should be the exact same Arc (pointer equality)
    assert!(Arc::ptr_eq(&s1, &s2));
    assert_eq!(&*s1, "test_query");
}

#[test]
fn interner_different_strings_return_different_arcs() {
    let interner = StringInterner::new(100);

    let s1 = interner.intern("query1");
    let s2 = interner.intern("query2");

    assert!(!Arc::ptr_eq(&s1, &s2));
    assert_eq!(&*s1, "query1");
    assert_eq!(&*s2, "query2");
}

#[test]
fn interner_handles_empty_string() {
    let interner = StringInterner::new(100);

    let s1 = interner.intern("");
    let s2 = interner.intern("");

    assert!(Arc::ptr_eq(&s1, &s2));
    assert_eq!(&*s1, "");
}

#[test]
fn interner_handles_unicode() {
    let interner = StringInterner::new(100);

    let s1 = interner.intern("测试查询");
    let s2 = interner.intern("测试查询");
    let s3 = interner.intern("emoji 🔍 search");

    assert!(Arc::ptr_eq(&s1, &s2));
    assert_eq!(&*s3, "emoji 🔍 search");
}

#[test]
fn interner_respects_lru_eviction() {
    let interner = StringInterner::new(3);

    let _s1 = interner.intern("query1");
    let _s2 = interner.intern("query2");
    let _s3 = interner.intern("query3");

    assert_eq!(interner.len(), 3);

    // This should evict query1 (LRU)
    let _s4 = interner.intern("query4");

    assert_eq!(interner.len(), 3);

    // query1 should now get a NEW Arc (was evicted)
    let s1_new = interner.intern("query1");
    assert_eq!(&*s1_new, "query1");
}

#[test]
fn interner_concurrent_access() {
    use std::thread;

    let interner = Arc::new(StringInterner::new(1000));
    let queries: Vec<String> = (0..100).map(|i| format!("query_{}", i)).collect();

    let handles: Vec<_> = (0..4)
        .map(|_| {
            let interner = Arc::clone(&interner);
            let queries = queries.clone();

            thread::spawn(move || {
                for _ in 0..10 {
                    for query in &queries {
                        let _ = interner.intern(query);
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify all queries are interned correctly
    for query in &queries {
        let s1 = interner.intern(query);
        let s2 = interner.intern(query);
        assert!(Arc::ptr_eq(&s1, &s2));
    }
}

#[test]
fn federated_merge_orders_equal_rank_hits_by_stable_hit_key() {
    let merged = merge_federated_ranked_hits(vec![
        make_federated_ranked_hit(2, 0, "zeta"),
        make_federated_ranked_hit(0, 0, "bravo"),
        make_federated_ranked_hit(1, 0, "alpha"),
    ]);

    let paths = merged
        .iter()
        .map(|hit| hit.source_path.as_str())
        .collect::<Vec<_>>();
    assert_eq!(paths, vec!["alpha.jsonl", "bravo.jsonl", "zeta.jsonl"]);
    assert!(
        merged
            .iter()
            .all(|hit| (hit.score - federated_rrf_score(0)).abs() < f32::EPSILON),
        "equal per-shard rank should produce equal RRF scores"
    );
}

#[test]
fn federated_merge_keeps_rrf_rank_ahead_of_stable_key() {
    let merged = merge_federated_ranked_hits(vec![
        make_federated_ranked_hit(0, 1, "alpha"),
        make_federated_ranked_hit(1, 0, "zeta"),
    ]);

    let paths = merged
        .iter()
        .map(|hit| hit.source_path.as_str())
        .collect::<Vec<_>>();
    assert_eq!(paths, vec!["zeta.jsonl", "alpha.jsonl"]);
    assert!(merged[0].score > merged[1].score);
}

#[test]
fn federated_merge_uses_shard_index_as_duplicate_final_tiebreak() {
    let merged = merge_federated_ranked_hits(vec![
        FederatedRankedHit {
            hit: make_federated_merge_hit("same", "shard-2"),
            shard_index: 2,
            shard_rank: 0,
            fused_score: federated_rrf_score(0),
        },
        FederatedRankedHit {
            hit: make_federated_merge_hit("same", "shard-0"),
            shard_index: 0,
            shard_rank: 0,
            fused_score: federated_rrf_score(0),
        },
    ]);

    assert_eq!(merged[0].agent, "shard-0");
    assert_eq!(merged[1].agent, "shard-2");
}

#[test]
fn top_k_fused_basic() {
    let hits = vec![
        make_fused_hit("a", 1.0, Some(0), None),
        make_fused_hit("b", 3.0, Some(1), None),
        make_fused_hit("c", 2.0, Some(2), None),
        make_fused_hit("d", 5.0, Some(3), None),
        make_fused_hit("e", 4.0, Some(4), None),
    ];

    let top = top_k_fused(hits, 3);

    assert_eq!(top.len(), 3);
    assert_eq!(top[0].key.source_path, "d"); // 5.0
    assert_eq!(top[1].key.source_path, "e"); // 4.0
    assert_eq!(top[2].key.source_path, "b"); // 3.0
}

#[test]
fn top_k_fused_empty() {
    let hits: Vec<FusedHit> = vec![];
    let top = top_k_fused(hits, 10);
    assert!(top.is_empty());
}

#[test]
fn top_k_fused_k_zero() {
    let hits = vec![
        make_fused_hit("a", 1.0, Some(0), None),
        make_fused_hit("b", 2.0, Some(1), None),
    ];
    let top = top_k_fused(hits, 0);
    assert!(top.is_empty());
}

#[test]
fn top_k_fused_k_larger_than_n() {
    let hits = vec![
        make_fused_hit("a", 1.0, Some(0), None),
        make_fused_hit("b", 2.0, Some(1), None),
    ];

    let top = top_k_fused(hits, 10);

    assert_eq!(top.len(), 2);
    assert_eq!(top[0].key.source_path, "b"); // 2.0
    assert_eq!(top[1].key.source_path, "a"); // 1.0
}

#[test]
fn top_k_fused_k_equals_n() {
    let hits = vec![
        make_fused_hit("a", 3.0, Some(0), None),
        make_fused_hit("b", 1.0, Some(1), None),
        make_fused_hit("c", 2.0, Some(2), None),
    ];

    let top = top_k_fused(hits, 3);

    assert_eq!(top.len(), 3);
    assert_eq!(top[0].key.source_path, "a"); // 3.0
    assert_eq!(top[1].key.source_path, "c"); // 2.0
    assert_eq!(top[2].key.source_path, "b"); // 1.0
}

#[test]
fn top_k_fused_k_one() {
    let hits = vec![
        make_fused_hit("a", 1.0, Some(0), None),
        make_fused_hit("b", 3.0, Some(1), None),
        make_fused_hit("c", 2.0, Some(2), None),
    ];

    let top = top_k_fused(hits, 1);

    assert_eq!(top.len(), 1);
    assert_eq!(top[0].key.source_path, "b");
    assert_eq!(top[0].score.rrf, 3.0);
}

#[test]
fn top_k_fused_duplicate_scores() {
    let hits = vec![
        make_fused_hit("a", 2.0, Some(0), None),
        make_fused_hit("b", 2.0, Some(1), None),
        make_fused_hit("c", 2.0, Some(2), None),
        make_fused_hit("d", 1.0, Some(3), None),
    ];

    let top = top_k_fused(hits, 2);

    assert_eq!(top.len(), 2);
    // All have same score, so order is by key (deterministic tie-breaking)
    assert_eq!(top[0].score.rrf, 2.0);
    assert_eq!(top[1].score.rrf, 2.0);
}

#[test]
fn top_k_fused_dual_source_tiebreaker() {
    // Hits with same RRF score, but some have both lexical and semantic ranks
    let hits = vec![
        make_fused_hit("a", 2.0, Some(0), None),    // lexical only
        make_fused_hit("b", 2.0, Some(1), Some(0)), // both sources
        make_fused_hit("c", 2.0, None, Some(1)),    // semantic only
    ];

    let top = top_k_fused(hits, 3);

    assert_eq!(top.len(), 3);
    // Dual-source hit should come first
    assert_eq!(top[0].key.source_path, "b");
}

#[test]
fn top_k_fused_large_input_uses_quickselect() {
    // Create input larger than QUICKSELECT_THRESHOLD to trigger quickselect path
    let hits: Vec<FusedHit> = (0..100)
        .map(|i| make_fused_hit(&format!("hit_{}", i), i as f32, Some(i), None))
        .collect();

    let top = top_k_fused(hits, 10);

    assert_eq!(top.len(), 10);
    // Should be sorted descending: hit_99, hit_98, ... hit_90
    for (i, hit) in top.iter().enumerate() {
        assert_eq!(hit.key.source_path, format!("hit_{}", 99 - i));
        assert_eq!(hit.score.rrf, (99 - i) as f32);
    }
}

#[test]
fn top_k_fused_equivalence_with_full_sort() {
    // Verify quickselect produces same results as full sort
    for n in [10, 50, 100, 200] {
        for k in [1, 5, 10, 25] {
            if k > n {
                continue;
            }

            let hits: Vec<FusedHit> = (0..n)
                .map(|i| {
                    // Pseudo-random scores using simple hash
                    let score = ((i * 17 + 7) % 1000) as f32;
                    make_fused_hit(&format!("hit_{}", i), score, Some(i), None)
                })
                .collect();

            // Baseline: full sort
            let mut baseline = hits.clone();
            baseline.sort_by(cmp_fused_hit_desc);
            baseline.truncate(k);

            // Quickselect
            let quickselect = top_k_fused(hits, k);

            // Verify same length
            assert_eq!(quickselect.len(), baseline.len(), "n={}, k={}", n, k);

            // Verify same elements in same order
            for (q, b) in quickselect.iter().zip(baseline.iter()) {
                assert_eq!(
                    q.key.source_path, b.key.source_path,
                    "n={}, k={}: mismatch",
                    n, k
                );
                assert_eq!(q.score.rrf, b.score.rrf, "n={}, k={}: score mismatch", n, k);
            }
        }
    }
}

#[test]
fn cmp_fused_hit_desc_basic_ordering() {
    let a = make_fused_hit("a", 2.0, Some(0), None);
    let b = make_fused_hit("b", 3.0, Some(1), None);

    // Higher score should come first (compare returns Less)
    assert_eq!(cmp_fused_hit_desc(&a, &b), CmpOrdering::Greater);
    assert_eq!(cmp_fused_hit_desc(&b, &a), CmpOrdering::Less);
    assert_eq!(cmp_fused_hit_desc(&a, &a), CmpOrdering::Equal);
}

// ==========================================================================
// Original Tests
// ==========================================================================

#[test]
fn search_deduplication_across_pages_repro() {
    // Distinct sessions with identical content should remain visible across
    // pages. Global pagination still has to happen after deduplication, but
    // dedup itself only coalesces hits that share message-level provenance.

    let dir = TempDir::new().unwrap();
    let index_path = dir.path();
    let mut index = TantivyIndex::open_or_create(index_path).unwrap();

    // Add two documents with IDENTICAL content but distinct other fields.
    // Tantivy scores them. If query matches both equally, one comes first.
    // We'll use different source paths to ensure they are distinct hits initially.
    let msg1 = NormalizedMessage {
        idx: 0,
        role: "user".into(),
        author: None,
        created_at: Some(1000),
        content: "duplicate content".into(),
        extra: serde_json::json!({}),
        snippets: Vec::new(),
        invocations: Vec::new(),
    };
    let conv1 = NormalizedConversation {
        agent_slug: "agent1".into(),
        external_id: None,
        title: None,
        workspace: None,
        source_path: "path/1".into(),
        started_at: None,
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![msg1],
    };

    let msg2 = NormalizedMessage {
        idx: 0,
        role: "user".into(),
        author: None,
        created_at: Some(2000),              // Different timestamp
        content: "duplicate content".into(), // SAME content
        extra: serde_json::json!({}),
        snippets: Vec::new(),
        invocations: Vec::new(),
    };
    let conv2 = NormalizedConversation {
        agent_slug: "agent1".into(),
        external_id: None,
        title: None,
        workspace: None,
        source_path: "path/2".into(), // Different source path
        started_at: None,
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![msg2],
    };

    index.add_conversation(&conv1).unwrap();
    index.add_conversation(&conv2).unwrap();
    index.commit().unwrap();

    let client = SearchClient::open(index_path, None).unwrap().unwrap();

    // Search page 1: limit 1, offset 0
    let page1 = client
        .search("duplicate", SearchFilters::default(), 1, 0, FieldMask::FULL)
        .unwrap();
    assert_eq!(page1.len(), 1);

    // Search page 2: limit 1, offset 1
    let page2 = client
        .search("duplicate", SearchFilters::default(), 1, 1, FieldMask::FULL)
        .unwrap();

    assert_eq!(page2.len(), 1);
    assert_ne!(page1[0].source_path, page2[0].source_path);
}
