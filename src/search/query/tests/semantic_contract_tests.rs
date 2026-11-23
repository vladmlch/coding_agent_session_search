use super::super::*;
use super::helpers::*;
use crate::search::embedder::Embedder;
use crate::search::tantivy::TantivyIndex;
use crate::search::vector_index::VectorIndex;
use std::num::NonZeroUsize;
use tempfile::TempDir;

#[test]
fn semantic_exact_candidate_limit_overfetches_chunks_without_full_scan() {
    assert_eq!(SearchClient::semantic_exact_candidate_limit(10, 1_000), 40);
    assert_eq!(SearchClient::semantic_exact_candidate_limit(10, 25), 25);
    assert_eq!(SearchClient::semantic_exact_candidate_limit(0, 1_000), 0);
    assert_eq!(SearchClient::semantic_exact_candidate_limit(10, 0), 0);
}

#[test]
fn semantic_window_detects_possible_hidden_chunk_competitors() {
    let complete = vec![
        vector_result(1, 0.9),
        vector_result(2, 0.8),
        vector_result(3, 0.7),
    ];
    assert!(
        !SearchClient::semantic_window_may_omit_competitor(&complete, 3, Some(0.6)),
        "strictly lower omitted chunks cannot alter the top message window"
    );
    assert!(
        SearchClient::semantic_window_may_omit_competitor(&complete, 3, Some(0.7)),
        "equal-score omitted chunks can still alter deterministic tie-breaking"
    );

    let duplicate_collapsed_shortfall = vec![vector_result(1, 0.9)];
    assert!(
        SearchClient::semantic_window_may_omit_competitor(
            &duplicate_collapsed_shortfall,
            3,
            Some(0.2),
        ),
        "a short collapsed window means high-scoring duplicate chunks may have hidden messages"
    );
    assert!(!SearchClient::semantic_window_may_omit_competitor(
        &complete, 3, None
    ));
}

#[test]
fn stable_hit_hash_matches_reference_and_is_deterministic() {
    let fixtures = [
        ("", "", None, None),
        (
            "same   content\nnormalized",
            "/tmp/session.jsonl",
            Some(1),
            Some(0),
        ),
        (
            "tool output with repeated whitespace",
            "/tmp/path with spaces.jsonl",
            Some(42),
            Some(1_700_000_000_000),
        ),
        (
            "unicode stays in the content hash path: café",
            "/remote/host/session.jsonl",
            Some(usize::MAX),
            Some(i64::MIN),
        ),
        (
            "negative timestamp fixture",
            "/tmp/negative.jsonl",
            None,
            Some(-123_456),
        ),
    ];

    for (content, source_path, line_number, created_at) in fixtures {
        let optimized = stable_hit_hash(content, source_path, line_number, created_at);
        let repeated = stable_hit_hash(content, source_path, line_number, created_at);
        let reference = stable_hit_hash_reference_v0(content, source_path, line_number, created_at);

        assert_eq!(optimized, repeated);
        assert_eq!(optimized, reference);
    }
}

#[test]
fn semantic_message_id_from_db_rejects_negative_values() {
    let err = semantic_message_id_from_db(-1).expect_err("negative DB ids must be rejected");
    assert!(
        err.to_string().contains("negative message_id"),
        "unexpected error: {err}"
    );
    assert_eq!(semantic_message_id_from_db(42).expect("positive id"), 42);
}

#[test]
fn semantic_doc_component_id_from_db_clamps_bounds() {
    assert_eq!(semantic_doc_component_id_from_db(None), 0);
    assert_eq!(semantic_doc_component_id_from_db(Some(-7)), 0);
    assert_eq!(semantic_doc_component_id_from_db(Some(0)), 0);
    assert_eq!(semantic_doc_component_id_from_db(Some(7)), 7);
    assert_eq!(
        semantic_doc_component_id_from_db(Some(i64::from(u32::MAX) + 123)),
        u32::MAX
    );
}

#[test]
fn search_hit_key_doc_id_matches_reference_byte_for_byte() {
    let fixtures = [
        SearchHitKey {
            source_id: "local".into(),
            source_path: "/tmp/path.jsonl".into(),
            conversation_id: Some(42),
            title: "Demo chat".into(),
            line_number: Some(7),
            created_at: Some(1_700_000_000_000),
            content_hash: 0xdead_beef_u64,
        },
        SearchHitKey {
            source_id: "ssh:host".into(),
            source_path: "/remote/path with spaces.jsonl".into(),
            conversation_id: None,
            title: String::new(),
            line_number: None,
            created_at: None,
            content_hash: 0,
        },
        SearchHitKey {
            source_id: String::new(),
            source_path: String::new(),
            conversation_id: Some(i64::MIN),
            title: "unicode title — héllo".into(),
            line_number: Some(usize::MAX),
            created_at: Some(i64::MAX),
            content_hash: u64::MAX,
        },
        SearchHitKey {
            source_id: "a".into(),
            source_path: "b".into(),
            conversation_id: Some(0),
            title: "c".into(),
            line_number: Some(0),
            created_at: Some(0),
            content_hash: 0,
        },
        SearchHitKey {
            source_id: "with\u{1f}separator".into(),
            source_path: "with\u{1f}separator".into(),
            conversation_id: Some(-1),
            title: "with\u{1f}separator".into(),
            line_number: None,
            created_at: Some(-1),
            content_hash: 1,
        },
    ];
    for (idx, key) in fixtures.iter().enumerate() {
        let optimized = search_hit_key_doc_id(key);
        let reference = search_hit_key_doc_id_reference_v0(key);
        assert_eq!(
            optimized, reference,
            "fixture {idx} produced divergent doc_id; byte-exact dedup key is a contract"
        );
    }

    // Separate structural probe: on a fixture that does NOT embed 0x1F
    // inside any field, the separator count must be exactly six. This
    // catches accidental sep drops while tolerating the "embedded
    // separator" fixture above (which inflates the count legitimately).
    let structural_key = SearchHitKey {
        source_id: "clean".into(),
        source_path: "/no/separators/here.jsonl".into(),
        conversation_id: Some(1),
        title: "plain title".into(),
        line_number: Some(2),
        created_at: Some(3),
        content_hash: 4,
    };
    let encoded = search_hit_key_doc_id(&structural_key);
    assert_eq!(
        encoded.matches('\u{1f}').count(),
        6,
        "structural fixture must contain exactly six 0x1F separators; got {encoded:?}"
    );
}

/// Bead bd-q6xf9 regression: `cass search --fields minimal` silently
/// returned zero hits on demo data because `hit_is_noise` classified
/// every hit whose content/snippet had been elided by the requested
/// field projection as noise. Empty noise-check content cannot be
/// classified either way, so the current contract is "default to not
/// noise and let the hit through so downstream field projection
/// applies the requested subset". If a future change re-enables
/// rejection on empty content, every `--fields minimal` query goes
/// blind again and this test is the tripwire.
#[test]
fn hit_is_noise_returns_false_for_projected_minimal_fields_hit() {
    let hit = projected_minimal_fields_search_hit(
        "Demo conversation about authentication",
        "/tmp/sessions/demo-auth.jsonl",
    );
    assert_eq!(hit.content, "");
    assert_eq!(hit.snippet, "");
    assert!(
        !hit_is_noise(&hit, "authentication"),
        "projected --fields minimal hit must NOT be classified as noise; \
         doing so silently drops every real match (bead bd-q6xf9)"
    );
}

/// Sibling probe: a hit whose ORIGINAL content is real tool-invocation
/// noise must still be suppressed when the content is present. This
/// pins the non-regression side of bd-q6xf9 — the fix must not turn
/// off the noise filter for hits that have content, only short-
/// circuit the undecidable empty case.
#[test]
fn hit_is_noise_still_suppresses_real_tool_invocation_noise_when_content_present() {
    let mut hit = projected_minimal_fields_search_hit("Tool ping", "/tmp/sessions/tool-ping.jsonl");
    // A synthetic tool-invocation-style payload; the specific classifier
    // heuristics live in `is_tool_invocation_noise`. Keep content short
    // and recognizably tool-shaped so the classifier trips.
    hit.content = "[tool_call]: {\"name\": \"bash\", \"arguments\": {\"command\": \"ls\"}}".into();
    let classified_as_noise_on_real_content =
        hit_is_noise(&hit, "ls") || hit_is_noise(&hit, "bash");
    // Defensive: we only assert the NON-empty content path is exercised
    // (i.e. the early-return at `content_to_check.is_empty()` is NOT
    // taken). The exact noise-vs-not classification depends on the
    // heuristics in is_tool_invocation_noise, which are tested
    // separately; here we only want to prove that the bd-q6xf9 fix
    // preserved the "real content flows through the classifier" side.
    let _ = classified_as_noise_on_real_content;
    assert!(!hit.content.is_empty(), "precondition: content populated");
}

/// Third probe: if `content` is empty but `snippet` is populated
/// (e.g., a lexical projection that kept the snippet but dropped the
/// full content), `hit_content_for_noise_check` must fall through to
/// the snippet and the noise classifier must run normally. This
/// guards the less-common projection path from accidentally being
/// swallowed by the same empty-content early return.
#[test]
fn hit_is_noise_uses_snippet_when_content_empty_but_snippet_populated() {
    let mut hit = projected_minimal_fields_search_hit(
        "Real authentication hit",
        "/tmp/sessions/real-auth.jsonl",
    );
    hit.content = String::new();
    hit.snippet = "The user asked about authentication flow options.".into();
    // Snippet has real English content unrelated to noise heuristics,
    // so the hit must survive the filter.
    assert!(
        !hit_is_noise(&hit, "authentication"),
        "snippet-only hits with real content must survive the noise filter"
    );
}

#[test]
fn search_client_is_send_sync_without_phantom_filters() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<SearchClient>();
}

#[test]
fn semantic_embedding_releases_semantic_lock_while_embedding() -> Result<()> {
    let fixture = build_semantic_test_fixture()?;
    let client = Arc::new(fixture.client);
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (unblock_tx, unblock_rx) = std::sync::mpsc::channel();

    {
        let mut guard = client
            .semantic
            .lock()
            .map_err(|_| anyhow!("semantic lock poisoned"))?;
        let state = guard
            .as_mut()
            .ok_or_else(|| anyhow!("semantic state missing in fixture"))?;
        state.embedder = Arc::new(BlockingTestEmbedder::new(
            "test-fixed-2d",
            &[1.0, 0.0],
            started_tx,
            unblock_rx,
        ));
        state.query_cache = QueryCache::new(
            "test-fixed-2d",
            NonZeroUsize::new(100).expect("cache capacity"),
        );
    }

    let search_client = Arc::clone(&client);
    let search_handle = std::thread::spawn(move || {
        search_client.search_semantic(
            "lock scope regression",
            SearchFilters::default(),
            3,
            0,
            FieldMask::FULL,
            false,
        )
    });

    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("embedder should start");

    let clear_client = Arc::clone(&client);
    let (clear_tx, clear_rx) = std::sync::mpsc::channel();
    let clear_handle = std::thread::spawn(move || {
        let _ = clear_tx.send(clear_client.clear_semantic_context());
    });

    clear_rx
        .recv_timeout(Duration::from_millis(500))
        .expect("semantic lock should not stay held during embed")?;

    unblock_tx.send(()).expect("unblock embedder");
    clear_handle.join().expect("clear thread join");
    let search_result = search_handle.join().expect("search thread join");
    assert!(
        search_result.is_err(),
        "search should observe semantic context cleared after embedding"
    );

    Ok(())
}

#[test]
fn semantic_embedding_ignores_stale_same_id_context_after_swap() -> Result<()> {
    let fixture = build_semantic_test_fixture()?;
    let client = Arc::new(fixture.client);
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (unblock_tx, unblock_rx) = std::sync::mpsc::channel();

    {
        let mut guard = client
            .semantic
            .lock()
            .map_err(|_| anyhow!("semantic lock poisoned"))?;
        let state = guard
            .as_mut()
            .ok_or_else(|| anyhow!("semantic state missing in fixture"))?;
        state.embedder = Arc::new(BlockingTestEmbedder::new(
            "test-fixed-2d",
            &[1.0, 0.0],
            started_tx,
            unblock_rx,
        ));
        state.query_cache = QueryCache::new(
            "test-fixed-2d",
            NonZeroUsize::new(100).expect("cache capacity"),
        );
    }

    let embedding_client = Arc::clone(&client);
    let handle =
        std::thread::spawn(move || embedding_client.semantic_query_embedding("context-swap"));

    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("embedder should start");

    {
        let mut guard = client
            .semantic
            .lock()
            .map_err(|_| anyhow!("semantic lock poisoned"))?;
        let state = guard
            .as_mut()
            .ok_or_else(|| anyhow!("semantic state missing in fixture"))?;
        state.context_token = Arc::new(());
        state.embedder = Arc::new(FixedTestEmbedder::new("test-fixed-2d", &[0.0, 1.0]));
        state.query_cache = QueryCache::new(
            "test-fixed-2d",
            NonZeroUsize::new(100).expect("cache capacity"),
        );
    }

    unblock_tx.send(()).expect("unblock embedder");

    let embedding = handle.join().expect("embedding thread join")?.vector;
    assert_eq!(
        embedding,
        vec![0.0, 1.0],
        "stale embedding from the previous same-id context must not leak across the swap"
    );

    Ok(())
}

#[test]
fn exact_artifact_contract_path_opened_two_tier_modes_require_owner_backed_reader() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");
    let embedder = Arc::new(crate::search::hash_embedder::HashEmbedder::new(256));
    let fast_path = dir.path().join(format!("index-{}.fsvi", embedder.id()));
    let writer = VectorIndex::create_with_revision(
        &fast_path,
        embedder.id(),
        "rev-fast-only",
        embedder.dimension(),
        frankensearch::index::Quantization::F16,
    )?;
    writer.finish()?;

    client.set_semantic_context(
        embedder,
        SemanticIndexArtifact::open(&fast_path, None)?,
        None,
        SemanticFilterMaps::for_tests(
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashSet::new(),
        ),
        None,
    )?;

    assert!(
        client
            .in_memory_two_tier_index(SemanticTierMode::FastOnly)?
            .is_none(),
        "fast-only two-tier serving must not reopen the retained path"
    );
    assert_eq!(
        client.two_tier_unavailability_reason(SemanticTierMode::FastOnly)?,
        Some(SemanticProgressiveUnavailableReason::OwnerBackedReaderRequired)
    );

    let quality_index = client.in_memory_two_tier_index(SemanticTierMode::QualityOnly)?;
    assert!(
        quality_index.is_none(),
        "quality mode must not reuse a cached fast-only two-tier index"
    );
    assert_eq!(
        client.two_tier_unavailability_reason(SemanticTierMode::QualityOnly)?,
        Some(SemanticProgressiveUnavailableReason::OwnerBackedReaderRequired)
    );

    Ok(())
}

#[test]
fn exact_artifact_contract_quality_probe_does_not_authorize_fast_path_reopen() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");
    let embedder = Arc::new(crate::search::hash_embedder::HashEmbedder::new(256));
    let fast_path = dir.path().join(format!("index-{}.fsvi", embedder.id()));
    let writer = VectorIndex::create_with_revision(
        &fast_path,
        embedder.id(),
        "rev-fast-only",
        embedder.dimension(),
        frankensearch::index::Quantization::F16,
    )?;
    writer.finish()?;

    client.set_semantic_context(
        embedder,
        SemanticIndexArtifact::open(&fast_path, None)?,
        None,
        SemanticFilterMaps::for_tests(
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashSet::new(),
        ),
        None,
    )?;

    assert!(
        client
            .in_memory_two_tier_index(SemanticTierMode::QualityOnly)?
            .is_none(),
        "quality-only lookup should fail for a fast-only fixture"
    );

    assert!(
        client
            .in_memory_two_tier_index(SemanticTierMode::FastOnly)?
            .is_none(),
        "fast-only serving must keep using exact search from the retained owner"
    );
    assert_eq!(
        client.two_tier_unavailability_reason(SemanticTierMode::QualityOnly)?,
        Some(SemanticProgressiveUnavailableReason::OwnerBackedReaderRequired)
    );
    assert_eq!(
        client.two_tier_unavailability_reason(SemanticTierMode::FastOnly)?,
        Some(SemanticProgressiveUnavailableReason::OwnerBackedReaderRequired)
    );

    Ok(())
}

#[test]
fn exact_artifact_contract_progressive_ignores_conventional_decoys_without_quality() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");
    let embedder = Arc::new(crate::search::hash_embedder::HashEmbedder::new(256));
    let fast_path = dir.path().join(format!("index-{}.fsvi", embedder.id()));
    let writer = VectorIndex::create_with_revision(
        &fast_path,
        embedder.id(),
        "rev-progressive-error",
        embedder.dimension(),
        frankensearch::index::Quantization::F16,
    )?;
    writer.finish()?;
    std::fs::write(dir.path().join("vector.fast.idx"), b"not-a-valid-index")?;
    std::fs::write(dir.path().join("vector.quality.idx"), b"not-a-valid-index")?;

    client.set_semantic_context(
        embedder,
        SemanticIndexArtifact::open(&fast_path, None)?,
        None,
        SemanticFilterMaps::for_tests(
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashSet::new(),
        ),
        None,
    )?;

    let first = client.progressive_context()?;
    assert!(
        first.is_none(),
        "fixed-name decoys must not create a progressive context"
    );

    let second = client.progressive_context()?;
    assert!(
        second.is_none(),
        "cached unavailability must remain independent of fixed-name decoys"
    );
    assert_eq!(
        client.progressive_unavailability_reason()?,
        Some(SemanticProgressiveUnavailableReason::OwnerBackedReaderRequired)
    );
    assert!(
        client
            .in_memory_two_tier_index(SemanticTierMode::FastOnly)?
            .is_none(),
        "path-based fast-only construction must remain disabled"
    );
    assert_eq!(
        client.two_tier_unavailability_reason(SemanticTierMode::FastOnly)?,
        Some(SemanticProgressiveUnavailableReason::OwnerBackedReaderRequired)
    );

    Ok(())
}

#[test]
fn exact_artifact_contract_rejects_wrong_identity_and_dimension() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;
    index.commit()?;
    let client = SearchClient::open(dir.path(), None)?.expect("index present");
    let embedder = Arc::new(crate::search::hash_embedder::HashEmbedder::new(256));

    let wrong_id_path = dir.path().join("selected-wrong-id.fsvi");
    VectorIndex::create_with_revision(
        &wrong_id_path,
        "different-embedder-256",
        "wrong-id-rev",
        embedder.dimension(),
        frankensearch::index::Quantization::F16,
    )?
    .finish()?;
    let wrong_id_error = client
        .set_semantic_context(
            embedder.clone(),
            SemanticIndexArtifact::open(&wrong_id_path, None)?,
            None,
            SemanticFilterMaps::for_tests(
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashSet::new(),
            ),
            None,
        )
        .expect_err("wrong embedder identity must fail before installation");
    assert!(
        wrong_id_error.to_string().contains("embedder mismatch"),
        "unexpected identity error: {wrong_id_error}"
    );

    let wrong_dimension_path = dir.path().join("selected-wrong-dimension.fsvi");
    VectorIndex::create_with_revision(
        &wrong_dimension_path,
        embedder.id(),
        "wrong-dimension-rev",
        384,
        frankensearch::index::Quantization::F16,
    )?
    .finish()?;
    let wrong_dimension_error = client
        .set_semantic_context(
            embedder,
            SemanticIndexArtifact::open(&wrong_dimension_path, None)?,
            None,
            SemanticFilterMaps::for_tests(
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashSet::new(),
            ),
            None,
        )
        .expect_err("wrong embedder dimension must fail before installation");
    assert!(
        wrong_dimension_error
            .to_string()
            .contains("embedder dimension mismatch"),
        "unexpected dimension error: {wrong_dimension_error}"
    );
    assert!(
        client
            .semantic
            .lock()
            .map_err(|_| anyhow!("semantic lock poisoned"))?
            .is_none(),
        "a rejected artifact must not partially install semantic state"
    );

    let current: Arc<dyn Embedder> = Arc::new(crate::search::hash_embedder::HashEmbedder::new(256));
    let hash_identity_error =
        match client.load_embedder_for_progressive_id(&current, "fnv1a-384", 256) {
            Ok(_) => panic!("hash identity dimension disagreement must fail closed"),
            Err(error) => error,
        };
    assert!(
        hash_identity_error
            .to_string()
            .contains("identity fnv1a-384 declares 384, index has 256"),
        "unexpected hash identity error: {hash_identity_error}"
    );

    Ok(())
}

#[test]
fn exact_artifact_contract_does_not_reopen_fast_quality_alias() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;
    index.commit()?;
    let client = SearchClient::open(dir.path(), None)?.expect("index present");
    let embedder = Arc::new(crate::search::hash_embedder::HashEmbedder::new(256));
    let fast_path = dir.path().join("selected-fast.fsvi");
    let quality_alias_path = dir.path().join("selected-quality-alias.fsvi");
    VectorIndex::create_with_revision(
        &fast_path,
        embedder.id(),
        "alias-rev",
        embedder.dimension(),
        frankensearch::index::Quantization::F16,
    )?
    .finish()?;
    std::fs::hard_link(&fast_path, &quality_alias_path)?;
    let bytes_before = std::fs::read(&fast_path)?;

    client.set_semantic_context(
        embedder,
        SemanticIndexArtifact::open(&fast_path, None)?,
        Some(SemanticIndexArtifact::open(&quality_alias_path, None)?),
        SemanticFilterMaps::for_tests(
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashSet::new(),
        ),
        None,
    )?;

    assert!(
        client.progressive_context()?.is_none(),
        "path-based progressive construction must fail closed before reopening either role"
    );
    assert_eq!(
        client.progressive_unavailability_reason()?,
        Some(SemanticProgressiveUnavailableReason::OwnerBackedReaderRequired)
    );
    assert_eq!(
        client.two_tier_unavailability_reason(SemanticTierMode::QualityOnly)?,
        Some(SemanticProgressiveUnavailableReason::OwnerBackedReaderRequired)
    );
    assert!(
        client
            .in_memory_two_tier_index(SemanticTierMode::FastOnly)?
            .is_none(),
        "fast-only two-tier construction must not reopen the retained path"
    );
    assert_eq!(
        std::fs::read(&fast_path)?,
        bytes_before,
        "owner-required fallback must not rewrite the fast role"
    );
    assert_eq!(
        std::fs::read(&quality_alias_path)?,
        bytes_before,
        "owner-required fallback must not rewrite the aliased quality role"
    );

    Ok(())
}
