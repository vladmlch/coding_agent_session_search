use super::super::*;
use super::helpers::*;
use crate::connectors::{NormalizedConversation, NormalizedMessage};
use crate::search::tantivy::TantivyIndex;
use crate::search::vector_index::VectorIndex;
use frankensearch::{ScoreSource as FsScoreSource, ScoredResult as FsScoredResult};
use serde_json::json;
use tempfile::TempDir;

// =========================================================================
// Session paths filter tests (chained searches)
// =========================================================================

#[test]
fn search_session_paths_filter() -> Result<()> {
    // Test filtering by specific session source paths (for chained searches)
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;

    // Create 3 conversations with different source paths
    let paths = [
        dir.path().join("session-a.jsonl"),
        dir.path().join("session-b.jsonl"),
        dir.path().join("session-c.jsonl"),
    ];

    for (i, path) in paths.iter().enumerate() {
        let conv = NormalizedConversation {
            agent_slug: "claude".into(),
            external_id: None,
            title: Some(format!("session-{}", i)),
            workspace: Some(std::path::PathBuf::from("/ws")),
            source_path: path.clone(),
            started_at: Some(100 + i as i64),
            ended_at: None,
            metadata: serde_json::json!({}),
            messages: vec![NormalizedMessage {
                idx: 0,
                role: "user".into(),
                author: None,
                created_at: Some(100 + i as i64),
                content: format!("needle content for session {}", i),
                extra: serde_json::json!({}),
                snippets: vec![],
                invocations: Vec::new(),
            }],
        };
        index.add_conversation(&conv)?;
    }
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");

    // First, search without filter - should get all 3
    let hits_all = client.search("needle", SearchFilters::default(), 10, 0, FieldMask::FULL)?;
    assert_eq!(hits_all.len(), 3, "Should find all 3 sessions");

    // Now filter to only sessions A and C
    let mut filters = SearchFilters::default();
    filters
        .session_paths
        .insert(paths[0].to_string_lossy().to_string());
    filters
        .session_paths
        .insert(paths[2].to_string_lossy().to_string());

    let hits_filtered = client.search("needle", filters, 10, 0, FieldMask::FULL)?;
    assert_eq!(
        hits_filtered.len(),
        2,
        "Should find only 2 sessions (A and C)"
    );

    // Verify the correct sessions are returned
    let filtered_paths: HashSet<&str> = hits_filtered
        .iter()
        .map(|h| h.source_path.as_str())
        .collect();
    assert!(filtered_paths.contains(paths[0].to_string_lossy().as_ref()));
    assert!(filtered_paths.contains(paths[2].to_string_lossy().as_ref()));
    assert!(!filtered_paths.contains(paths[1].to_string_lossy().as_ref()));

    Ok(())
}

// review 2026-11-02 Q1: the engine counts every match before the
// session_paths post-filter, so its count is not an exact total.
#[test]
fn session_paths_filter_does_not_publish_engine_count_as_exact() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;
    let paths: Vec<_> = (0..3)
        .map(|i| dir.path().join(format!("q1-session-{i}.jsonl")))
        .collect();
    for (i, path) in paths.iter().enumerate() {
        index.add_conversation(&NormalizedConversation {
            agent_slug: "claude".into(),
            external_id: None,
            title: Some(format!("q1-{i}")),
            workspace: Some(std::path::PathBuf::from("/ws")),
            source_path: path.clone(),
            started_at: Some(100 + i as i64),
            ended_at: None,
            metadata: serde_json::json!({}),
            messages: vec![NormalizedMessage {
                idx: 0,
                role: "user".into(),
                author: None,
                created_at: Some(100 + i as i64),
                content: format!("q1needle unique body {i}"),
                extra: serde_json::json!({}),
                snippets: vec![],
                invocations: Vec::new(),
            }],
        })?;
    }
    index.commit()?;
    let client = SearchClient::open(dir.path(), None)?.expect("index present");

    let unscoped = client.search_with_fallback(
        "q1needle",
        SearchFilters::default(),
        10,
        0,
        0,
        FieldMask::FULL,
    )?;
    assert_eq!(unscoped.hits.len(), 3);
    assert_eq!(unscoped.total_count, Some(3), "unscoped count stays exact");

    let mut scoped_filters = SearchFilters::default();
    scoped_filters
        .session_paths
        .insert(paths[1].to_string_lossy().to_string());
    let scoped =
        client.search_with_fallback("q1needle", scoped_filters, 10, 0, 0, FieldMask::FULL)?;
    assert_eq!(scoped.hits.len(), 1);
    assert_eq!(
        scoped.total_count, None,
        "a pre-filter engine count must not be published as the scoped total"
    );
    Ok(())
}

#[test]
fn lexical_session_paths_filter_retries_past_initial_page() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;
    let requested_path = dir.path().join("requested-session.jsonl");

    for i in 0..4 {
        let conv = NormalizedConversation {
            agent_slug: "claude".into(),
            external_id: None,
            title: Some(format!("distractor-{i}")),
            workspace: Some(std::path::PathBuf::from("/ws")),
            source_path: dir.path().join(format!("distractor-{i}.jsonl")),
            started_at: Some(100 + i as i64),
            ended_at: None,
            metadata: serde_json::json!({}),
            messages: vec![NormalizedMessage {
                idx: 0,
                role: "user".into(),
                author: None,
                created_at: Some(100 + i as i64),
                content: "needle needle needle high ranking distractor".into(),
                extra: serde_json::json!({}),
                snippets: vec![],
                invocations: Vec::new(),
            }],
        };
        index.add_conversation(&conv)?;
    }

    let requested = NormalizedConversation {
        agent_slug: "claude".into(),
        external_id: None,
        title: Some("requested".into()),
        workspace: Some(std::path::PathBuf::from("/ws")),
        source_path: requested_path.clone(),
        started_at: Some(200),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(200),
            content: "needle requested session should survive post-filter paging".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    index.add_conversation(&requested)?;
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");
    let mut filters = SearchFilters::default();
    filters
        .session_paths
        .insert(requested_path.to_string_lossy().to_string());

    let hits = client.search("needle", filters, 1, 0, FieldMask::FULL)?;

    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].source_path, requested_path.to_string_lossy());

    Ok(())
}

#[test]
fn search_session_paths_empty_filter_returns_all() -> Result<()> {
    // Empty session_paths filter should not restrict results
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;

    let conv = NormalizedConversation {
        agent_slug: "claude".into(),
        external_id: None,
        title: Some("test".into()),
        workspace: Some(std::path::PathBuf::from("/ws")),
        source_path: dir.path().join("test.jsonl"),
        started_at: Some(100),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(100),
            content: "needle content".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    index.add_conversation(&conv)?;
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");

    // Empty session_paths should not filter
    let filters = SearchFilters::default();
    assert!(filters.session_paths.is_empty());

    let hits = client.search("needle", filters, 10, 0, FieldMask::FULL)?;
    assert_eq!(hits.len(), 1);

    Ok(())
}

#[test]
// Builds its corpus by publishing separate shard directories into a
// federated bundle, which is the Tantivy-only staged-shard machinery
// disabled for the Quill backend (see the `staged_shard_plan` binding in
// indexer/mod.rs). The federated READ path itself is ported and covered by
// `open_federated_search_readers`; what is unreachable here is the shard
// *construction* this fixture depends on.
#[ignore = "Tantivy-only shard publishing; unreachable on the Quill backend"]
fn search_client_reads_federated_lexical_bundle_as_one_corpus() -> Result<()> {
    let root = TempDir::new()?;
    let shard_a = root.path().join("shard-a");
    let shard_b = root.path().join("shard-b");
    let published = root.path().join("published");

    let mut shard_a_index = TantivyIndex::open_or_create(&shard_a)?;
    let mut shard_b_index = TantivyIndex::open_or_create(&shard_b)?;

    let make_conv =
        |external_id: &str, title: &str, source_path: &str, tag: &str| NormalizedConversation {
            agent_slug: "codex".into(),
            external_id: Some(external_id.into()),
            title: Some(title.into()),
            workspace: Some(std::path::PathBuf::from("/ws")),
            source_path: std::path::PathBuf::from(source_path),
            started_at: Some(1_700_000_100_000),
            ended_at: Some(1_700_000_100_100),
            metadata: json!({}),
            messages: vec![
                NormalizedMessage {
                    idx: 0,
                    role: "user".into(),
                    author: None,
                    created_at: Some(1_700_000_100_010),
                    content: format!("shared federated needle {tag} user"),
                    extra: json!({}),
                    snippets: vec![],
                    invocations: Vec::new(),
                },
                NormalizedMessage {
                    idx: 1,
                    role: "assistant".into(),
                    author: None,
                    created_at: Some(1_700_000_100_020),
                    content: format!("shared federated needle {tag} assistant"),
                    extra: json!({}),
                    snippets: vec![],
                    invocations: Vec::new(),
                },
            ],
        };

    let conv_a = make_conv(
        "fed-query-a",
        "Fed Query A",
        "/tmp/fed-query-a.jsonl",
        "alpha",
    );
    let conv_b = make_conv(
        "fed-query-b",
        "Fed Query B",
        "/tmp/fed-query-b.jsonl",
        "beta",
    );

    shard_a_index.add_conversation(&conv_a)?;
    shard_b_index.add_conversation(&conv_b)?;
    shard_a_index.commit()?;
    shard_b_index.commit()?;
    drop(shard_a_index);
    drop(shard_b_index);

    crate::search::tantivy::publish_federated_searchable_index_directories(
        &published,
        &[&shard_a, &shard_b],
    )?;

    let client = SearchClient::open(&published, None)?.expect("federated index present");
    assert!(client.has_tantivy());
    assert_eq!(client.total_docs(), 4);

    let hits = client.search(
        "shared federated needle",
        SearchFilters::default(),
        10,
        0,
        FieldMask::FULL,
    )?;
    assert_eq!(hits.len(), 4);
    let observed_order = hits
        .iter()
        .map(|hit| {
            (
                hit.source_path.clone(),
                hit.line_number,
                hit.content.clone(),
                hit.score.to_bits(),
            )
        })
        .collect::<Vec<_>>();
    let hit_paths = hits
        .iter()
        .map(|hit| hit.source_path.as_str())
        .collect::<std::collections::HashSet<_>>();
    assert!(hit_paths.contains("/tmp/fed-query-a.jsonl"));
    assert!(hit_paths.contains("/tmp/fed-query-b.jsonl"));

    for attempt in 0..3 {
        let repeated = client.search(
            "shared federated needle",
            SearchFilters::default(),
            10,
            0,
            FieldMask::FULL,
        )?;
        let repeated_order = repeated
            .iter()
            .map(|hit| {
                (
                    hit.source_path.clone(),
                    hit.line_number,
                    hit.content.clone(),
                    hit.score.to_bits(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            repeated_order, observed_order,
            "federated lexical query order drifted on repeated attempt {attempt}"
        );
    }

    Ok(())
}

#[test]
fn gh452_semantic_no_limit_uses_vector_count_without_a_lexical_reader() -> Result<()> {
    for sharded in [false, true] {
        let fixture =
            build_semantic_test_fixture_with_options(sharded, SemanticAnnFixtureMode::Missing)?;
        assert!(!fixture.client.has_tantivy());
        let (hits, _) = fixture.client.search_semantic(
            "semantic fixture query",
            SearchFilters::default(),
            0,
            0,
            FieldMask::FULL,
            false,
        )?;
        assert_eq!(hits.len(), 3);
        let (page, _) = fixture.client.search_semantic(
            "semantic fixture query",
            SearchFilters::default(),
            0,
            1,
            FieldMask::FULL,
            false,
        )?;
        assert_eq!(page.len(), 2);
        assert_eq!(page[0].source_path, hits[1].source_path);
        assert_eq!(page[1].source_path, hits[2].source_path);
    }
    Ok(())
}

#[test]
fn semantic_no_limit_includes_retained_wal_messages_and_offset_suffixes() -> Result<()> {
    let identities = |hits: &[SearchHit]| {
        hits.iter()
            .map(|hit| {
                (
                    hit.source_path.clone(),
                    hit.conversation_id,
                    hit.line_number,
                )
            })
            .collect::<Vec<_>>()
    };
    for sharded in [false, true] {
        let fixture =
            build_semantic_test_fixture_with_options(sharded, SemanticAnnFixtureMode::Missing)?;
        assert!(!fixture.client.has_tantivy());
        let (expected, _) = fixture.client.search_semantic(
            "semantic fixture query",
            SearchFilters::default(),
            fixture.doc_ids.len(),
            0,
            FieldMask::FULL,
            false,
        )?;
        assert_eq!(expected.len(), 3);
        let (embedder, filter_maps, records) = {
            let guard = fixture.client.semantic.lock().unwrap();
            let state = guard.as_ref().expect("fixture semantic context");
            let mut records = Vec::new();
            for artifact in state.artifacts.iter() {
                let index = artifact.index();
                for row in 0..index.record_count() {
                    records.push((index.doc_id_at(row)?.to_owned(), index.vector_at_f32(row)?));
                }
            }
            (
                Arc::clone(&state.embedder),
                state.filter_maps.clone(),
                records,
            )
        };

        // Exercise both a wholly WAL-backed generation and a mixed
        // main/WAL generation, with the delta in a separate shard too.
        for main_rows_per_shard in [0_usize, 1] {
            let mut artifacts = Vec::new();
            let shard_size = if sharded { 2 } else { records.len() };
            for (shard, rows) in records.chunks(shard_size).enumerate() {
                let path = fixture.vector_dir.join(format!(
                    "unlimited-wal-{main_rows_per_shard}-shard-{shard}.fsvi"
                ));
                let main_rows = main_rows_per_shard.min(rows.len());
                let mut writer = VectorIndex::create_with_revision(
                    &path,
                    embedder.id(),
                    "rev-1",
                    embedder.dimension(),
                    frankensearch::index::Quantization::F16,
                )?;
                for (doc_id, vector) in &rows[..main_rows] {
                    writer.write_record(doc_id, vector)?;
                }
                writer.finish()?;
                if main_rows < rows.len() {
                    let mut writer = VectorIndex::open_writer(&path)?;
                    writer.append_batch(&rows[main_rows..])?;
                    assert!(writer.wal_record_count() > 0);
                }
                artifacts.push(SemanticIndexArtifact::open(&path, None)?);
            }
            let main_count: usize = artifacts
                .iter()
                .map(|artifact| artifact.index().record_count())
                .sum();
            let wal_count: usize = artifacts
                .iter()
                .map(|artifact| artifact.index().wal_record_count())
                .sum();
            assert!(wal_count > 0, "the reopened delta must remain in WAL");
            assert_eq!(main_count + wal_count, expected.len());
            assert!(main_count < expected.len());
            fixture.client.set_semantic_artifacts_context(
                Arc::clone(&embedder),
                artifacts,
                None,
                filter_maps.clone(),
                None,
            )?;
            let before = semantic_artifact_tree_snapshot(&fixture.vector_dir)?;
            for approximate in [false, true] {
                let (hits, stats) = fixture.client.search_semantic(
                    "semantic fixture query",
                    SearchFilters::default(),
                    0,
                    0,
                    FieldMask::FULL,
                    approximate,
                )?;
                assert_eq!(
                    identities(&hits),
                    identities(&expected),
                    "unlimited search lost WAL messages: sharded={sharded}, \
                     main_rows_per_shard={main_rows_per_shard}, approximate={approximate}"
                );
                assert!(stats.is_none(), "the fixture has no admitted ANN graph");
                let (page, _) = fixture.client.search_semantic(
                    "semantic fixture query",
                    SearchFilters::default(),
                    0,
                    1,
                    FieldMask::FULL,
                    approximate,
                )?;
                assert_eq!(identities(&page), identities(&expected[1..]));
            }
            assert_eq!(
                semantic_artifact_tree_snapshot(&fixture.vector_dir)?,
                before
            );
        }
    }
    Ok(())
}

#[test]
fn semantic_search_session_paths_filter_retries_past_initial_candidates() -> Result<()> {
    let fixture = build_semantic_test_fixture()?;
    let mut filters = SearchFilters::default();
    filters
        .session_paths
        .insert(fixture.source_paths[2].clone());

    let (hits, ann_stats) = fixture.client.search_semantic(
        "semantic fixture query",
        filters,
        1,
        0,
        FieldMask::FULL,
        false,
    )?;

    assert!(
        ann_stats.is_none(),
        "exact search should not emit ANN stats"
    );
    assert_eq!(
        hits.len(),
        1,
        "filtered semantic search should still return a hit"
    );
    assert_eq!(
        hits[0].source_path, fixture.source_paths[2],
        "semantic search should keep searching until it finds the requested session path"
    );

    Ok(())
}

#[test]
fn semantic_search_offsets_after_session_paths_filtering() -> Result<()> {
    let fixture = build_semantic_test_fixture()?;
    let mut filters = SearchFilters::default();
    filters
        .session_paths
        .insert(fixture.source_paths[1].clone());
    filters
        .session_paths
        .insert(fixture.source_paths[2].clone());

    let (hits, _) = fixture.client.search_semantic(
        "semantic fixture query",
        filters,
        1,
        1,
        FieldMask::FULL,
        false,
    )?;

    assert_eq!(
        hits.len(),
        1,
        "second filtered page should still return one hit"
    );
    assert_eq!(
        hits[0].source_path, fixture.source_paths[2],
        "offset must apply after semantic deduplication and session path filtering"
    );

    Ok(())
}

#[test]
fn exact_artifact_contract_native_ann_searches_without_mutating_generation() -> Result<()> {
    let fixture = build_semantic_test_fixture_with_ann(SemanticAnnFixtureMode::Native)?;
    let artifacts_before = semantic_artifact_tree_snapshot(&fixture.vector_dir)?;

    let (hits, ann_stats) = fixture.client.search_semantic(
        "semantic fixture query",
        SearchFilters::default(),
        1,
        0,
        FieldMask::FULL,
        true,
    )?;

    assert_eq!(hits.len(), 1);
    assert_eq!(
        hits[0].source_path, fixture.source_paths[0],
        "the selected native ANN must preserve the exact source ranking"
    );
    assert!(
        ann_stats.is_some(),
        "a native explicitly paired ANN must report ANN execution stats"
    );
    assert_eq!(fixture.client.ann_unavailability_reason()?, None);
    assert_eq!(
        semantic_artifact_tree_snapshot(&fixture.vector_dir)?,
        artifacts_before,
        "native ANN serving must preserve bytes, mtimes, permissions, and inventory"
    );

    Ok(())
}

#[cfg(unix)]
#[test]
fn exact_artifact_contract_path_replacement_keeps_exact_owner_and_disables_progressive()
-> Result<()> {
    let fixture = build_semantic_test_fixture()?;
    let selected_path = fixture.vector_dir.join("index-test-fixed-2d.fsvi");
    let selected_bytes = std::fs::read(&selected_path)?;

    let (initial_hits, initial_ann_stats) = fixture.client.search_semantic(
        "semantic fixture query",
        SearchFilters::default(),
        1,
        0,
        FieldMask::FULL,
        false,
    )?;
    assert!(initial_ann_stats.is_none());
    assert_eq!(initial_hits.len(), 1);
    assert_eq!(initial_hits[0].source_path, fixture.source_paths[0]);

    let replacement_path = fixture.vector_dir.join("replacement.fsvi");
    let mut replacement = VectorIndex::create_with_revision(
        &replacement_path,
        "test-fixed-2d",
        "replacement-rev",
        2,
        frankensearch::index::Quantization::F16,
    )?;
    for (doc_id, vector) in
        fixture
            .doc_ids
            .iter()
            .zip([[0.0_f32, 1.0_f32], [0.1_f32, 0.9_f32], [1.0_f32, 0.0_f32]])
    {
        replacement.write_record(doc_id, &vector)?;
    }
    replacement.finish()?;
    std::fs::rename(&replacement_path, &selected_path)?;

    let replacement_bytes = std::fs::read(&selected_path)?;
    assert_ne!(
        replacement_bytes, selected_bytes,
        "the selected pathname must now name a different FSVI image"
    );
    let replacement_index = VectorIndex::open(&selected_path)?;
    let replacement_hits = replacement_index.search_top_k(&[1.0, 0.0], 1, None)?;
    assert_eq!(replacement_hits.len(), 1);
    assert_eq!(
        replacement_hits[0].doc_id.as_str(),
        fixture.doc_ids[2],
        "the replacement path must realize a different winner"
    );
    let post_replace_snapshot = semantic_artifact_tree_snapshot(&fixture.vector_dir)?;

    let (exact_hits, exact_ann_stats) = fixture.client.search_semantic(
        "semantic fixture query",
        SearchFilters::default(),
        1,
        0,
        FieldMask::FULL,
        false,
    )?;
    assert!(exact_ann_stats.is_none());
    assert_eq!(exact_hits.len(), 1);
    assert_eq!(
        exact_hits[0].source_path, fixture.source_paths[0],
        "exact search must stay on the constructor-retained owner"
    );

    let (fast_only_hits, fast_only_ann_stats) = fixture.client.search_semantic_with_tier(
        "semantic fixture query",
        SearchFilters::default(),
        1,
        0,
        FieldMask::FULL,
        false,
        SemanticTierMode::FastOnly,
    )?;
    assert!(fast_only_ann_stats.is_none());
    assert_eq!(fast_only_hits.len(), 1);
    assert_eq!(
        fast_only_hits[0].source_path, fixture.source_paths[0],
        "fast-only requests must fall back to the retained exact owner"
    );
    assert_eq!(
        fixture
            .client
            .two_tier_unavailability_reason(SemanticTierMode::FastOnly)?,
        Some(SemanticProgressiveUnavailableReason::OwnerBackedReaderRequired)
    );
    assert!(fixture.client.progressive_context()?.is_none());
    assert_eq!(
        fixture.client.progressive_unavailability_reason()?,
        Some(SemanticProgressiveUnavailableReason::OwnerBackedReaderRequired)
    );
    assert_eq!(
        semantic_artifact_tree_snapshot(&fixture.vector_dir)?,
        post_replace_snapshot,
        "exact fallback and progressive diagnostics must not mutate the replacement tree"
    );

    Ok(())
}

#[test]
fn exact_artifact_contract_stale_ann_with_same_doc_ids_falls_back_exact_read_only() -> Result<()> {
    let fixture = build_semantic_test_fixture_with_ann(SemanticAnnFixtureMode::StaleSameDocIds)?;
    let artifacts_before = semantic_artifact_tree_snapshot(&fixture.vector_dir)?;

    let (hits, ann_stats) = fixture.client.search_semantic(
        "semantic fixture query",
        SearchFilters::default(),
        1,
        0,
        FieldMask::FULL,
        true,
    )?;

    assert_eq!(hits.len(), 1);
    assert_eq!(
        hits[0].source_path, fixture.source_paths[0],
        "a stale same-doc-id ANN must not influence the exact fallback ranking"
    );
    assert!(
        ann_stats.is_none(),
        "a rejected stale ANN must not claim approximate execution"
    );
    assert_eq!(
        fixture.client.ann_unavailability_reason()?,
        Some(SemanticAnnUnavailableReason::SidecarNotNative)
    );
    assert_eq!(
        semantic_artifact_tree_snapshot(&fixture.vector_dir)?,
        artifacts_before,
        "stale ANN rejection must preserve bytes, mtimes, permissions, and inventory"
    );

    Ok(())
}

#[test]
fn exact_artifact_contract_invalid_optional_ann_falls_back_exact_read_only() -> Result<()> {
    let modes = [
        SemanticAnnFixtureMode::CorruptMetadata,
        SemanticAnnFixtureMode::CrossRoleAlias,
        #[cfg(unix)]
        SemanticAnnFixtureMode::FinalComponentSymlink,
    ];

    for mode in modes {
        let fixture = build_semantic_test_fixture_with_ann(mode)?;
        let artifacts_before = semantic_artifact_tree_snapshot(&fixture.vector_dir)?;

        let (hits, ann_stats) = fixture.client.search_semantic(
            "semantic fixture query",
            SearchFilters::default(),
            1,
            0,
            FieldMask::FULL,
            true,
        )?;

        assert_eq!(hits.len(), 1, "exact fallback failed for {mode:?}");
        assert_eq!(
            hits[0].source_path, fixture.source_paths[0],
            "an invalid optional ANN must not influence exact ranking for {mode:?}"
        );
        assert!(
            ann_stats.is_none(),
            "an invalid optional ANN must not claim approximate execution for {mode:?}"
        );
        assert_eq!(
            fixture.client.ann_unavailability_reason()?,
            Some(SemanticAnnUnavailableReason::SidecarOpenFailed),
            "invalid optional ANN reason disagreed for {mode:?}"
        );
        assert_eq!(
            semantic_artifact_tree_snapshot(&fixture.vector_dir)?,
            artifacts_before,
            "invalid ANN fallback must preserve bytes, mtimes, permissions, and inventory for {mode:?}"
        );
    }

    Ok(())
}

#[test]
fn exact_artifact_contract_missing_ann_falls_back_exact_with_typed_reason() -> Result<()> {
    let fixture = build_semantic_test_fixture()?;
    let artifacts_before = semantic_artifact_tree_snapshot(&fixture.vector_dir)?;

    let (hits, ann_stats) = fixture.client.search_semantic(
        "semantic fixture query",
        SearchFilters::default(),
        1,
        0,
        FieldMask::FULL,
        true,
    )?;

    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].source_path, fixture.source_paths[0]);
    assert!(
        ann_stats.is_none(),
        "an exact fallback must not emit ANN stats"
    );
    assert_eq!(
        fixture.client.ann_unavailability_reason()?,
        Some(SemanticAnnUnavailableReason::SidecarMissing)
    );
    assert_eq!(
        semantic_artifact_tree_snapshot(&fixture.vector_dir)?,
        artifacts_before,
        "missing ANN fallback must not synthesize or rewrite artifacts"
    );

    Ok(())
}

#[test]
fn exact_artifact_contract_sharded_ann_request_falls_back_exact_with_typed_reason() -> Result<()> {
    let fixture = build_sharded_semantic_test_fixture()?;
    let artifacts_before = semantic_artifact_tree_snapshot(&fixture.vector_dir)?;

    let (hits, ann_stats) = fixture.client.search_semantic(
        "semantic fixture query",
        SearchFilters::default(),
        3,
        0,
        FieldMask::FULL,
        true,
    )?;

    assert!(ann_stats.is_none());
    assert_eq!(hits.len(), 3);
    assert_eq!(hits[0].source_path, fixture.source_paths[0]);
    assert_eq!(hits[1].source_path, fixture.source_paths[1]);
    assert_eq!(hits[2].source_path, fixture.source_paths[2]);
    assert_eq!(
        fixture.client.ann_unavailability_reason()?,
        Some(SemanticAnnUnavailableReason::SidecarMissing)
    );
    assert_eq!(
        semantic_artifact_tree_snapshot(&fixture.vector_dir)?,
        artifacts_before,
        "sharded ANN fallback must preserve the exact artifact set"
    );

    Ok(())
}

#[test]
fn exact_artifact_contract_sharded_exact_search_has_typed_progressive_fallback() -> Result<()> {
    let fixture = build_sharded_semantic_test_fixture()?;
    let (hits, ann_stats) = fixture.client.search_semantic(
        "semantic fixture query",
        SearchFilters::default(),
        3,
        0,
        FieldMask::FULL,
        false,
    )?;

    assert!(
        ann_stats.is_none(),
        "sharded exact search should not emit ANN stats"
    );
    assert_eq!(hits.len(), 3);
    assert_eq!(hits[0].source_path, fixture.source_paths[0]);
    assert_eq!(hits[1].source_path, fixture.source_paths[1]);
    assert_eq!(hits[2].source_path, fixture.source_paths[2]);
    assert!(
        fixture
            .client
            .in_memory_two_tier_index(SemanticTierMode::FastOnly)?
            .is_none(),
        "multi-shard exact serving must not synthesize an unsharded two-tier index"
    );
    assert!(
        fixture.client.progressive_context()?.is_none(),
        "multi-shard exact serving must report progressive refinement unavailable"
    );
    assert_eq!(
        fixture
            .client
            .two_tier_unavailability_reason(SemanticTierMode::FastOnly)?,
        Some(SemanticProgressiveUnavailableReason::OwnerBackedReaderRequired)
    );
    assert_eq!(
        fixture.client.progressive_unavailability_reason()?,
        Some(SemanticProgressiveUnavailableReason::OwnerBackedReaderRequired)
    );

    Ok(())
}

#[test]
fn progressive_phase_overfetches_before_session_paths_filtering() -> Result<()> {
    let fixture = build_semantic_test_fixture()?;
    let mut filters = SearchFilters::default();
    filters
        .session_paths
        .insert(fixture.source_paths[2].clone());

    let results = vec![
        FsScoredResult {
            doc_id: fixture.doc_ids[0].clone().into(),
            score: 1.0,
            source: FsScoreSource::SemanticFast,
            index: None,
            fast_score: Some(1.0),
            quality_score: None,
            lexical_score: None,
            rerank_score: None,
            explanation: None,
            metadata: None,
        },
        FsScoredResult {
            doc_id: fixture.doc_ids[1].clone().into(),
            score: 0.9,
            source: FsScoreSource::SemanticFast,
            index: None,
            fast_score: Some(0.9),
            quality_score: None,
            lexical_score: None,
            rerank_score: None,
            explanation: None,
            metadata: None,
        },
        FsScoredResult {
            doc_id: fixture.doc_ids[2].clone().into(),
            score: 0.8,
            source: FsScoreSource::SemanticFast,
            index: None,
            fast_score: Some(0.8),
            quality_score: None,
            lexical_score: None,
            rerank_score: None,
            explanation: None,
            metadata: None,
        },
    ];

    let result = fixture.client.progressive_phase_to_result(
        &results,
        ProgressivePhaseContext {
            query: "session path filter",
            filters: &filters,
            field_mask: FieldMask::FULL,
            lexical_cache: None,
            limit: 1,
            fetch_limit: 3,
        },
    )?;

    assert_eq!(
        result.hits.len(),
        1,
        "progressive phase should retain enough overfetched hits to satisfy post-search session path filtering"
    );
    assert_eq!(
        result.hits[0].source_path, fixture.source_paths[2],
        "progressive phase should page after session path filtering"
    );

    Ok(())
}

// =============================================================================
// SQL Placeholder Builder Tests (Opt 4.5: Pre-sized String Buffers)
// =============================================================================

#[test]
fn sql_placeholders_empty() {
    assert_eq!(sql_placeholders(0), "");
}

#[test]
fn sql_rowid_literal_list_renders_integers_only() {
    assert_eq!(sql_rowid_literal_list(&[]), "");
    assert_eq!(sql_rowid_literal_list(&[7]), "7");
    assert_eq!(sql_rowid_literal_list(&[1, 22, 333]), "1,22,333");
    assert_eq!(
        sql_rowid_literal_list(&[i64::MIN, -1, 0, i64::MAX]),
        "-9223372036854775808,-1,0,9223372036854775807"
    );
    let rendered = sql_rowid_literal_list(&[12_966_472, 5]);
    assert!(
        rendered
            .chars()
            .all(|c| c.is_ascii_digit() || c == ',' || c == '-'),
        "a rowid list can only ever contain digits, commas and minus signs: {rendered}"
    );
}

#[test]
fn sql_placeholders_single() {
    assert_eq!(sql_placeholders(1), "?");
}

#[test]
fn sql_placeholders_multiple() {
    assert_eq!(sql_placeholders(3), "?,?,?");
    assert_eq!(sql_placeholders(5), "?,?,?,?,?");
}

#[test]
fn sql_placeholders_capacity_efficient() {
    // For count=3, capacity should be exactly 2*3-1=5 ("?,?,?" = 5 chars)
    let result = sql_placeholders(3);
    assert_eq!(result.len(), 5);
    assert!(result.capacity() >= 5); // Should have allocated at least 5

    // For count=10, capacity should be exactly 2*10-1=19
    let result = sql_placeholders(10);
    assert_eq!(result.len(), 19);
    assert!(result.capacity() >= 19);
}

#[test]
fn sql_placeholders_large_count() {
    // Test with a large count to ensure no off-by-one errors
    let result = sql_placeholders(100);
    assert_eq!(result.len(), 199); // 100 "?" + 99 ","
    assert_eq!(result.chars().filter(|c| *c == '?').count(), 100);
    assert_eq!(result.chars().filter(|c| *c == ',').count(), 99);
}

#[test]
fn hybrid_budget_identifier_biases_lexical() {
    let budget = hybrid_candidate_budget("src/main.rs", 20, 20, 5, 10_000);
    assert!(
        budget.lexical_candidates > budget.semantic_candidates,
        "identifier queries should allocate more lexical than semantic fanout"
    );
    assert!(budget.lexical_candidates >= 25);
}

#[test]
fn hybrid_budget_natural_language_biases_semantic() {
    let budget = hybrid_candidate_budget(
        "how do we fix authentication middleware latency",
        20,
        20,
        5,
        10_000,
    );
    assert!(
        budget.semantic_candidates > budget.lexical_candidates,
        "natural language queries should allocate more semantic than lexical fanout"
    );
}

#[test]
fn hybrid_budget_no_limit_caps_both_lexical_and_semantic() {
    // Regression: a "no limit" hybrid search on a large corpus used to
    // set `lexical_candidates = total_docs`, which let a single
    // `cass search` request grow to tens of GB of RAM on a ~500k-row
    // user history and saturate disk IO. Both lexical and semantic
    // fanout are now bounded, lexical against the RAM-proportional
    // `no_limit_result_cap()` ceiling and semantic against the narrower
    // `HYBRID_NO_LIMIT_SEMANTIC_CAP` ceiling.
    let total_docs = 2_000_000;
    let budget = hybrid_candidate_budget("authentication middleware", 0, total_docs, 0, total_docs);
    let cap = no_limit_result_cap();
    assert!(
        budget.lexical_candidates <= cap,
        "lexical fanout must respect no_limit_result_cap() = {cap}; got {}",
        budget.lexical_candidates
    );
    assert!(
        budget.lexical_candidates <= NO_LIMIT_RESULT_MAX,
        "lexical fanout must respect the absolute NO_LIMIT_RESULT_MAX; got {}",
        budget.lexical_candidates
    );
    assert!(budget.semantic_candidates <= HYBRID_NO_LIMIT_SEMANTIC_CAP);
    // Invariant preserved by the `.min(lexical)` clamp inside
    // hybrid_candidate_budget: semantic fanout never exceeds
    // lexical fanout. On typical hosts lexical >> semantic, but
    // the cheaper `<=` assertion also holds on edge-case tiny
    // boxes where the overall cap pulls lexical down to the
    // planning window.
    assert!(
        budget.semantic_candidates <= budget.lexical_candidates,
        "semantic ({}) must not exceed lexical ({}) fanout",
        budget.semantic_candidates,
        budget.lexical_candidates
    );
}

#[test]
fn compute_no_limit_result_cap_clamps_explicit_over_ceiling_env_override() {
    // A naively large explicit override must still be clamped. The
    // old implementation returned the env value unclamped, which
    // reintroduced the unbounded-result failure mode. Driven via
    // the pure `*_from` helper so we can't race with other
    // concurrent tests that read the real env.
    let cap = compute_no_limit_result_cap_from(Some("999999999999".to_string()), None, None);
    assert!(
        cap <= NO_LIMIT_RESULT_MAX,
        "explicit override must still clamp to ceiling; got {cap} > {NO_LIMIT_RESULT_MAX}"
    );
    assert!(cap >= NO_LIMIT_RESULT_MIN);
}

#[test]
fn compute_no_limit_result_cap_clamps_tiny_explicit_override_up_to_floor() {
    // Mirror case: an explicit override under the floor is lifted.
    let cap = compute_no_limit_result_cap_from(Some("1".to_string()), None, None);
    assert_eq!(cap, NO_LIMIT_RESULT_MIN);
}

#[test]
fn exact_total_count_policy_allows_small_indexes_only() {
    assert!(should_collect_exact_total_count(49_999, 50_000));
    assert!(should_collect_exact_total_count(50_000, 50_000));
    assert!(!should_collect_exact_total_count(50_001, 50_000));
}

#[test]
fn exact_total_count_policy_zero_limit_disables_recount() {
    assert!(!should_collect_exact_total_count(0, 0));
    assert!(!should_collect_exact_total_count(1, 0));
    assert!(!should_collect_exact_total_count(usize::MAX, 0));
}

fn gh518_page(
    hits: usize,
    total_count: Option<usize>,
    doc_count: usize,
) -> crate::search::quill_bridge::QuillLexicalPage {
    crate::search::quill_bridge::QuillLexicalPage {
        hits: (0..hits)
            .map(|rank| crate::search::quill_bridge::QuillLexicalDocHit {
                bm25_score: 1.0,
                rank,
                global_docid: u32::try_from(rank).expect("fixture rank fits u32"),
                document_id: format!("gh518-{rank}"),
            })
            .collect(),
        total_count,
        doc_count,
    }
}

/// An error carrying the engine's own Display text, wrapped the way
/// `quill_bridge::search_paginated` wraps it.
fn gh518_engine_error(error: frankensearch::quill::argus::ArgusError) -> anyhow::Error {
    anyhow!(
        "executing a Quill lexical query: {}",
        frankensearch::quill::QuillIndexError::from(error)
    )
}

fn gh518_fuel_exhausted() -> anyhow::Error {
    gh518_engine_error(
        frankensearch::quill::argus::ArgusError::QueryFuelExhausted {
            budget: 100,
            consumed: 100,
            segments_touched: 1,
            dictionary_blocks: 2,
            posting_blocks: 97,
            position_docs: 0,
        },
    )
}

/// GH #518: the exact total is optional. When only the counting pass runs
/// out of query fuel, the ranked page is returned and its total is unknown.
#[test]
fn bounded_exact_count_keeps_the_page_when_only_the_count_runs_out_of_fuel() -> Result<()> {
    let mut passes = Vec::new();
    let result = bounded_exact_count_page(4, 0, 1_000, |exact_count| {
        passes.push(exact_count);
        if exact_count {
            Err(gh518_fuel_exhausted())
        } else {
            Ok(gh518_page(4, None, 500))
        }
    })?;
    assert_eq!(
        passes,
        [false, true],
        "a saturated page on a small index asks for the exact count"
    );
    assert_eq!(result.hits, gh518_page(4, None, 500).hits);
    assert_eq!(result.total_count, None);
    Ok(())
}

/// The degrade is keyed to fuel exhaustion of the counting pass alone: a
/// cancelled count still fails the query, and the ranked pass is never
/// optional (its fuel refusal fails the query before any count is tried).
#[test]
fn bounded_exact_count_still_fails_on_other_count_errors_and_on_page_fuel() {
    let cancelled = bounded_exact_count_page(4, 0, 1_000, |exact_count| {
        if exact_count {
            Err(gh518_engine_error(
                frankensearch::quill::argus::ArgusError::QueryCancelled { phase: "collect" },
            ))
        } else {
            Ok(gh518_page(4, None, 500))
        }
    });
    let error = cancelled
        .err()
        .expect("a cancelled counting pass fails the query");
    assert!(
        !crate::search::quill_bridge::is_query_fuel_exhausted(&error),
        "{error:#}"
    );

    let mut passes = Vec::new();
    let refused = bounded_exact_count_page(4, 0, 1_000, |exact_count| {
        passes.push(exact_count);
        Err(gh518_fuel_exhausted())
    });
    let error = refused
        .err()
        .expect("a fuel-exhausted ranked pass fails the query");
    assert!(
        crate::search::quill_bridge::is_query_fuel_exhausted(&error),
        "{error:#}"
    );
    assert_eq!(passes, [false], "no count is attempted without a page");
}

/// Controls for the unchanged policy: an affordable count is reported, an
/// unsaturated page is its own total, and an index over the exact-count
/// ceiling is never recounted.
#[test]
fn bounded_exact_count_reports_affordable_totals_and_skips_unneeded_counts() -> Result<()> {
    let counted = bounded_exact_count_page(4, 0, 1_000, |exact_count| {
        Ok(gh518_page(4, exact_count.then_some(37), 500))
    })?;
    assert_eq!(counted.total_count, Some(37));

    let mut passes = Vec::new();
    let unsaturated = bounded_exact_count_page(4, 10, 1_000, |exact_count| {
        passes.push(exact_count);
        Ok(gh518_page(3, None, 500))
    })?;
    assert_eq!(passes, [false]);
    assert_eq!(unsaturated.total_count, Some(13));

    let mut passes = Vec::new();
    let over_ceiling = bounded_exact_count_page(4, 0, 499, |exact_count| {
        passes.push(exact_count);
        Ok(gh518_page(4, None, 500))
    })?;
    assert_eq!(passes, [false]);
    assert_eq!(over_ceiling.total_count, None);
    Ok(())
}

/// GH #518 on the real engine: the ranked page comes from Quill, the
/// counting call gets Quill's own typed fuel refusal, and the policy keeps
/// the page with an unknown total. With fuel for both, the same call
/// reports the exact total and ranks the same page.
///
/// The two calls use separate readers, ample fuel for the page and one
/// unit for the count, because a count-only refusal under one shared
/// budget could not be produced on frankensearch-quill 0.4.1. For CASS's
/// parsed queries the counting pass always fit the smallest budget the
/// ranked page fit, whether the matches sat in one segment past the first
/// 4,096-document scoring window or spread over one segment per ingest
/// shard, so no budget fit the page but not the count. The policy stays
/// because the total is optional either way; this test pins its wiring to
/// real engine errors, and the injected-error tests above pin its
/// decisions.
#[test]
fn bounded_exact_count_survives_a_real_count_fuel_refusal() -> Result<()> {
    const STRONG: usize = 4;
    const WEAK: usize = 60;
    let dir = TempDir::new()?;
    // Same length, so only the term frequency ranks the strong ones first.
    let strong = ["needle"; 24].join(" ");
    let weak = std::iter::once("needle")
        .chain(std::iter::repeat_n("filler", 23))
        .collect::<Vec<_>>()
        .join(" ");
    let mut documents = Vec::with_capacity(STRONG + WEAK);
    for ordinal in 0..STRONG + WEAK {
        documents.push(frankensearch::quill::cass::CassDocument {
            agent: "codex".into(),
            workspace: Some("/ws".into()),
            workspace_original: None,
            source_path: format!("/gh518/{ordinal:03}.jsonl"),
            msg_idx: 0,
            created_at: Some(100),
            title: Some("fixture".into()),
            content: if ordinal < STRONG {
                strong.clone()
            } else {
                weak.clone()
            },
            source_id: "local".into(),
            origin_kind: "local".into(),
            origin_host: None,
            conversation_id: Some(i64::try_from(ordinal)? + 1),
        });
    }
    let mut writer = crate::search::quill_bridge::QuillCassIndex::open_or_create(dir.path())?;
    writer.add_cass_documents(&documents)?;
    writer.commit()?;
    drop(writer);

    let query = frankensearch::quill::query::CassQueryParser::new(
        frankensearch::quill::schema::CASS_SEMANTIC_SCHEMA,
    )
    .map_err(|error| anyhow!("building the CASS query parser: {error}"))?
    .parse("needle", &FsCassQueryFilters::default())
    .query;
    let open = |budget: u64| {
        crate::search::quill_bridge::open_cass_reader_with_query_fuel_budget(dir.path(), budget)
    };
    let ample = open(crate::search::quill_bridge::cass_quill_config().query_fuel_budget)?;
    let starved = open(1)?;
    // Precondition: the starved reader really refuses the counting call
    // with the engine's typed fuel error, not some other failure.
    let refusal =
        match crate::search::quill_bridge::search_paginated(&starved, &query, STRONG, 0, true) {
            Ok(page) => bail!(
                "a one-unit reader must refuse the count, but it returned {} hits and total {:?}",
                page.hits.len(),
                page.total_count
            ),
            Err(error) => error,
        };
    assert!(
        matches!(
            refusal.downcast_ref::<frankensearch::quill::QuillIndexError>(),
            Some(frankensearch::quill::QuillIndexError::QueryFuelExhausted { .. })
        ),
        "{refusal:#}"
    );

    let mut passes = Vec::new();
    let degraded = bounded_exact_count_page(STRONG, 0, 1_000_000, |exact_count| {
        passes.push(exact_count);
        let reader = if exact_count { &starved } else { &ample };
        crate::search::quill_bridge::search_paginated(reader, &query, STRONG, 0, exact_count)
    })?;
    assert_eq!(
        passes,
        [false, true],
        "a saturated small page asks for its count"
    );
    assert_eq!(degraded.hits.len(), STRONG, "the ranked page survives");
    assert_eq!(degraded.total_count, None, "its total is unknown");

    // Control: with fuel for both passes the production entry point
    // reports the exact total and ranks the same page.
    let counted = execute_query_with_bounded_exact_count(&ample, &query, STRONG, 0)?;
    let direct = crate::search::quill_bridge::search_paginated(&ample, &query, STRONG, 0, true)?;
    assert_eq!(counted.total_count, direct.total_count);
    assert_eq!(
        counted.total_count,
        Some(STRONG + WEAK),
        "every document matches"
    );
    assert_eq!(counted.hits, degraded.hits);
    Ok(())
}

#[test]
fn automatic_wildcard_fallback_policy_allows_small_indexes_only() {
    assert!(should_allow_automatic_wildcard_fallback(9_999, 10_000));
    assert!(should_allow_automatic_wildcard_fallback(10_000, 10_000));
    assert!(!should_allow_automatic_wildcard_fallback(10_001, 10_000));
}

#[test]
fn automatic_wildcard_fallback_policy_zero_limit_disables_fallback() {
    assert!(!should_allow_automatic_wildcard_fallback(0, 0));
    assert!(!should_allow_automatic_wildcard_fallback(1, 0));
    assert!(!should_allow_automatic_wildcard_fallback(usize::MAX, 0));
}

#[test]
fn compute_no_limit_result_cap_uses_meminfo_when_no_env_override() {
    // 128 GiB available → 128 / 16 = 8 GiB budget (under the 16 GiB
    // ceiling, above the 256 MiB floor) → 8 GiB / 80 KiB ≈ 104k
    // hits. That lands inside [MIN, MAX] and above floor.
    let cap = compute_no_limit_result_cap_from(None, None, Some(128u64 * 1024 * 1024 * 1024));
    assert!(cap >= NO_LIMIT_RESULT_MIN, "cap {cap} below floor");
    assert!(cap <= NO_LIMIT_RESULT_MAX, "cap {cap} above ceiling");
    // Sanity: 128 GiB / 16 / 80 KiB is nowhere near 1k.
    assert!(cap > NO_LIMIT_RESULT_MIN * 10);
}

#[test]
fn compute_no_limit_result_cap_falls_back_to_floor_when_meminfo_unavailable() {
    // Simulates non-Linux (no /proc/meminfo): must still produce a
    // finite, in-envelope cap. The floor budget (256 MiB) / 80 KiB
    // ≈ 3276 hits — above MIN, below MAX.
    let cap = compute_no_limit_result_cap_from(None, None, None);
    assert!(cap >= NO_LIMIT_RESULT_MIN);
    assert!(cap <= NO_LIMIT_RESULT_MAX);
}

#[test]
fn compute_no_limit_result_cap_bytes_env_takes_priority_over_meminfo() {
    // Explicit bytes override wins over MemAvailable. 4 GiB bytes
    // / 80 KiB ≈ 52k hits, distinct from what a large MemAvailable
    // hint would otherwise produce (which would hit the 16 GiB
    // ceiling → ~209k hits).
    let four_gib = (4u64 * 1024 * 1024 * 1024).to_string();
    let cap = compute_no_limit_result_cap_from(
        None,
        Some(four_gib),
        Some(1024u64 * 1024 * 1024 * 1024), // 1 TiB (would ceiling otherwise)
    );
    let expected_hits = ((4u64 * 1024 * 1024 * 1024) / AVG_HIT_BYTES) as usize;
    let expected = expected_hits.clamp(NO_LIMIT_RESULT_MIN, NO_LIMIT_RESULT_MAX);
    assert_eq!(cap, expected, "bytes env must win over meminfo");
}

#[test]
fn no_limit_budget_bytes_preserves_fallback_priority() {
    let huge_meminfo = Some(1024u64 * 1024 * 1024 * 1024);
    let four_gib = 4u64 * 1024 * 1024 * 1024;

    assert_eq!(
        no_limit_budget_bytes(Some(four_gib.to_string()), huge_meminfo),
        four_gib
    );
    assert_eq!(
        no_limit_budget_bytes(Some("0".to_string()), huge_meminfo),
        NO_LIMIT_BYTES_CEILING
    );
    assert_eq!(no_limit_budget_bytes(None, None), NO_LIMIT_BYTES_FLOOR);
}

#[test]
fn compute_no_limit_result_cap_ignores_malformed_env() {
    // Garbage or zero values fall back to meminfo / floor, not crash.
    for bad in ["", "abc", "0", "-1"] {
        let cap =
            compute_no_limit_result_cap_from(Some(bad.to_string()), Some(bad.to_string()), None);
        assert!(cap >= NO_LIMIT_RESULT_MIN, "bad={bad:?} cap={cap}");
        assert!(cap <= NO_LIMIT_RESULT_MAX, "bad={bad:?} cap={cap}");
    }
}
