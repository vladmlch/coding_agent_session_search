use super::super::*;
use super::*;

#[test]
fn semantic_identity_marker_overrides_ready_asset_status_per_tier() {
    let temp = tempfile::tempdir().expect("tempdir");
    let db_path = temp.path().join("cass.db");
    let storage = FrankenStorage::open(&db_path).expect("create cass db");
    storage
        .mark_semantic_identity_rebuild_required(SemanticIdentityTier::Fast)
        .expect("mark fast semantic identity stale");
    drop(storage);

    let stale = apply_semantic_identity_invalidation(
        &db_path,
        SemanticAvailability::HashFallback,
        SemanticPreference::HashFallback,
    );
    assert!(
        stale.is_index_stale(),
        "status must not advertise a Pi-era fast-tier artifact as ready: {stale:?}"
    );

    let storage = FrankenStorage::open(&db_path).expect("reopen cass db");
    storage
        .complete_semantic_identity_rebuild(SemanticIdentityTier::Fast)
        .expect("complete fast semantic identity rebuild");
    drop(storage);
    let ready = apply_semantic_identity_invalidation(
        &db_path,
        SemanticAvailability::HashFallback,
        SemanticPreference::HashFallback,
    );
    assert!(ready.is_hash_fallback());
}

#[test]
fn inspect_search_assets_preserves_semantic_database_unavailable_signal() {
    let temp = tempfile::tempdir().expect("tempdir");
    let index_path = temp.path().join("index").join("v4");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");

    let db_path = temp.path().join("agent_search.db");
    std::fs::create_dir_all(&db_path).expect("create unopenable db path");

    let vector_path = vector_index_path(temp.path(), HashEmbedder::default().id());
    std::fs::create_dir_all(vector_path.parent().expect("vector parent"))
        .expect("create vector dir");
    std::fs::write(&vector_path, b"index").expect("write vector index");

    let snapshot = inspect_search_assets(InspectSearchAssetsInput {
        data_dir: temp.path(),
        db_path: &db_path,
        stale_threshold: 60,
        last_indexed_at_ms: Some(1_733_000_000_000),
        now_ms: 1_733_000_001_000,
        maintenance: SearchMaintenanceSnapshot::default(),
        semantic_preference: SemanticPreference::HashFallback,
        db_available: false,
        compute_lexical_fingerprint: false,
        inspect_semantic: true,
    })
    .expect("asset inspection should not fail when db availability is already known");

    assert_ne!(snapshot.lexical.status, "error");
    assert_eq!(snapshot.semantic.status, "error");
    assert_eq!(snapshot.semantic.availability, "database_unavailable");
    assert_eq!(snapshot.semantic.fallback_mode, Some("lexical"));
    assert!(snapshot.semantic.summary.contains("db unavailable"));
}

#[test]
fn inspect_search_assets_can_skip_semantic_db_open_for_fast_paths() {
    let temp = tempfile::tempdir().expect("tempdir");
    let index_path = temp.path().join("index").join("v4");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");

    let db_path = temp.path().join("agent_search.db");
    std::fs::create_dir_all(&db_path).expect("create unopenable db path");

    let vector_path = vector_index_path(temp.path(), HashEmbedder::default().id());
    std::fs::create_dir_all(vector_path.parent().expect("vector parent"))
        .expect("create vector dir");
    std::fs::write(&vector_path, b"index").expect("write vector index");

    let snapshot = inspect_search_assets(InspectSearchAssetsInput {
        data_dir: temp.path(),
        db_path: &db_path,
        stale_threshold: 60,
        last_indexed_at_ms: Some(1_733_000_000_000),
        now_ms: 1_733_000_001_000,
        maintenance: SearchMaintenanceSnapshot::default(),
        semantic_preference: SemanticPreference::HashFallback,
        db_available: false,
        compute_lexical_fingerprint: false,
        inspect_semantic: false,
    })
    .expect("asset inspection should not open semantic DB when semantic inspection is skipped");

    assert_ne!(snapshot.lexical.status, "error");
    assert_eq!(snapshot.semantic.status, "not_inspected");
    assert_eq!(snapshot.semantic.availability, "not_inspected");
    assert_eq!(snapshot.semantic.fallback_mode, Some("lexical"));
}

#[test]
fn inspect_search_assets_fails_semantic_closed_when_identity_marker_is_unreadable() {
    let temp = tempfile::tempdir().expect("tempdir");
    let index_path = temp.path().join("index").join("v4");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");

    let db_path = temp.path().join("agent_search.db");
    std::fs::create_dir_all(&db_path).expect("create unopenable db path");

    // The availability probe validates the artifact it finds (it no
    // longer trusts bare file existence), so the fixture must be a real
    // hash vector index. `db_available` only records an earlier probe: an
    // unreadable identity marker must still disable semantic search.
    let embedder = HashEmbedder::default();
    let vector_path = vector_index_path(temp.path(), embedder.id());
    std::fs::create_dir_all(vector_path.parent().expect("vector parent"))
        .expect("create vector dir");
    let mut writer = crate::search::vector_index::VectorIndex::create_with_revision(
        &vector_path,
        embedder.id(),
        crate::search_defaults::HASH_VECTOR_SPACE_REVISION,
        embedder.dimension(),
        frankensearch::index::Quantization::F16,
    )
    .expect("create hash vector index");
    let mut vector = vec![0.0_f32; embedder.dimension()];
    vector[0] = 1.0;
    writer
        .write_record("doc-0", &vector)
        .expect("write hash vector record");
    writer.finish().expect("finish hash vector index");

    let snapshot = inspect_search_assets(InspectSearchAssetsInput {
        data_dir: temp.path(),
        db_path: &db_path,
        stale_threshold: 60,
        last_indexed_at_ms: Some(1_733_000_000_000),
        now_ms: 1_733_000_001_000,
        maintenance: SearchMaintenanceSnapshot::default(),
        semantic_preference: SemanticPreference::HashFallback,
        db_available: true,
        compute_lexical_fingerprint: false,
        inspect_semantic: true,
    })
    .expect("semantic metadata inspection should degrade without failing the whole snapshot");

    assert_eq!(snapshot.semantic.status, "error");
    assert_eq!(snapshot.semantic.availability, "load_failed");
    assert!(!snapshot.semantic.can_search);
    assert_eq!(snapshot.semantic.fallback_mode, Some("lexical"));
    assert!(
        snapshot
            .semantic
            .summary
            .contains("checking semantic identity invalidation marker")
    );
}

#[test]
fn semantic_state_reports_hash_fallback_as_searchable() {
    let state = semantic_state_from_availability(
        Path::new("/tmp/cass"),
        &SemanticAvailability::HashFallback,
        SemanticPreference::HashFallback,
        None,
    );

    assert_eq!(state.status, "hash_fallback");
    assert_eq!(state.availability, "hash_fallback");
    assert!(state.available);
    assert!(state.can_search);
    assert_eq!(state.fallback_mode, None);
}

#[test]
fn semantic_state_preserves_missing_model_identity_and_paths() {
    let temp = tempfile::tempdir().expect("tempdir");
    let model_dir = temp.path().join("models/multilingual-minilm");
    let state = semantic_state_from_availability(
        temp.path(),
        &SemanticAvailability::ModelMissing {
            embedder_id: "multilingual-minilm-384".to_string(),
            model_dir: model_dir.clone(),
            missing_files: vec!["model.safetensors".to_string()],
        },
        SemanticPreference::DefaultModel,
        None,
    );

    assert_eq!(
        state.embedder_id.as_deref(),
        Some("multilingual-minilm-384")
    );
    assert_eq!(state.model_dir.as_deref(), Some(model_dir.as_path()));
    assert_eq!(
        state.vector_index_path.as_deref(),
        Some(vector_index_path(temp.path(), "multilingual-minilm-384").as_path())
    );
}

#[test]
fn semantic_preference_surface_preserves_backend_and_model_dir_projection() {
    let data_dir = Path::new("/tmp/cass");
    let cases = [
        (
            SemanticPreference::DefaultModel,
            "fastembed",
            Some(FastEmbedder::default_model_dir(data_dir)),
        ),
        (SemanticPreference::HashFallback, "hash", None),
    ];

    for (preference, expected_backend, expected_model_dir) in cases {
        let surface = semantic_preference_surface(data_dir, preference);

        assert_eq!(surface.preferred_backend, expected_backend);
        assert_eq!(surface.model_dir, expected_model_dir);
    }
}

#[test]
fn exact_artifact_contract_status_ignores_fixed_name_progressive_decoys() {
    let temp = tempfile::tempdir().expect("tempdir");
    let vector_dir = temp.path().join(VECTOR_INDEX_DIR);
    std::fs::create_dir_all(&vector_dir).expect("create vector dir");
    std::fs::write(vector_dir.join("vector.fast.idx"), b"fast").expect("write fast tier");
    std::fs::write(vector_dir.join("vector.quality.idx"), b"quality").expect("write quality tier");
    let hnsw_path = hnsw_index_path(temp.path(), FastEmbedder::embedder_id_static());
    std::fs::write(&hnsw_path, b"hnsw").expect("write hnsw");

    let state = semantic_state_from_availability(
        temp.path(),
        &SemanticAvailability::Ready {
            embedder_id: FastEmbedder::embedder_id_static().to_string(),
        },
        SemanticPreference::DefaultModel,
        None,
    );

    assert_eq!(state.status, "ready");
    assert!(
        !state.progressive_ready,
        "obsolete fixed-name files must not claim progressive readiness"
    );
    assert_eq!(
        state.progressive_reason_code,
        Some(SemanticProgressiveUnavailableReason::OwnerBackedReaderRequired.code())
    );
    assert!(
        state.summary.contains("owner_backed_reader_required"),
        "human summary and JSON reason code must agree: {}",
        state.summary
    );
    assert!(state.hnsw_present);
    assert_eq!(state.hnsw_state, "present_unverified");
    assert!(
        !state.hnsw_ready,
        "arbitrary bytes cannot prove native admission"
    );
    assert_eq!(
        state.embedder_id.as_deref(),
        Some(FastEmbedder::embedder_id_static())
    );
}

#[test]
fn bounded_ann_presence_does_not_read_or_modify_sidecar_contents() {
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("ann.chsw");
    assert_eq!(bounded_ann_presence(None), (false, "absent"));
    assert_eq!(bounded_ann_presence(Some(&path)), (false, "absent"));
    std::fs::write(&path, b"not native ANN metadata").expect("write decoy");
    let before = std::fs::metadata(&path).expect("metadata before");
    assert_eq!(
        bounded_ann_presence(Some(&path)),
        (true, "present_unverified")
    );
    let after = std::fs::metadata(&path).expect("metadata after");
    assert_eq!(before.len(), after.len());
    assert_eq!(before.modified().unwrap(), after.modified().unwrap());
    assert_eq!(before.permissions(), after.permissions());
    assert_eq!(std::fs::read(&path).unwrap(), b"not native ANN metadata");
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 1);
}

#[cfg(unix)]
#[test]
fn bounded_ann_presence_reports_dangling_symlink_without_following_it() {
    let temp = tempfile::tempdir().expect("tempdir");
    let target = temp.path().join("absent-target");
    let link = temp.path().join("ann.chsw");
    std::os::unix::fs::symlink(&target, &link).expect("create symlink");
    assert_eq!(
        bounded_ann_presence(Some(&link)),
        (true, "present_unverified")
    );
    assert_eq!(std::fs::read_link(&link).unwrap(), target);
    assert!(!target.exists());
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 1);
}

#[test]
fn semantic_state_reports_backfill_when_manifest_only_has_stale_assets() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut manifest = SemanticManifest {
        fast_tier: Some(ArtifactRecord {
            tier: crate::search::semantic_manifest::TierKind::Fast,
            embedder_id: HashEmbedder::default().id().to_string(),
            model_revision: "hash".to_string(),
            schema_version: crate::search::policy::SEMANTIC_SCHEMA_VERSION,
            chunking_version: crate::search::policy::CHUNKING_STRATEGY_VERSION,
            dimension: 256,
            doc_count: 12,
            conversation_count: 3,
            db_fingerprint: "stale-db".to_string(),
            index_path: "vector_index/vector.fast.idx".to_string(),
            size_bytes: 4096,
            started_at_ms: 1_733_100_000_000,
            completed_at_ms: 1_733_100_100_000,
            ready: true,
        }),
        backlog: crate::search::semantic_manifest::BacklogLedger {
            total_conversations: 20,
            fast_tier_processed: 3,
            quality_tier_processed: 0,
            db_fingerprint: "current-db".to_string(),
            computed_at_ms: 1_733_100_200_000,
        },
        checkpoint: Some(BuildCheckpoint {
            tier: crate::search::semantic_manifest::TierKind::Fast,
            embedder_id: HashEmbedder::default().id().to_string(),
            last_offset: 77,
            docs_embedded: 66,
            conversations_processed: 3,
            total_conversations: 20,
            db_fingerprint: "current-db".to_string(),
            schema_version: crate::search::policy::SEMANTIC_SCHEMA_VERSION,
            chunking_version: crate::search::policy::CHUNKING_STRATEGY_VERSION,
            saved_at_ms: 1_733_100_300_000,
            last_message_id: None,
            cursor_exhausted: false,
        }),
        ..Default::default()
    };
    manifest.save(temp.path()).expect("save semantic manifest");

    let state = semantic_state_from_availability(
        temp.path(),
        &SemanticAvailability::NotInstalled,
        SemanticPreference::DefaultModel,
        Some("current-db"),
    );

    assert_eq!(state.status, "building");
    assert_eq!(state.availability, "index_building");
    assert!(!state.can_search);
    assert_eq!(state.fallback_mode, Some("lexical"));
    assert!(state.summary.contains("backfill"));
    assert!(
        state
            .hint
            .as_deref()
            .is_some_and(|hint| hint.contains("finish backfilling"))
    );
}

#[test]
fn semantic_state_prefers_current_hash_tier_over_missing_model() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut manifest = SemanticManifest {
        fast_tier: Some(ArtifactRecord {
            tier: crate::search::semantic_manifest::TierKind::Fast,
            embedder_id: HashEmbedder::default().id().to_string(),
            model_revision: "hash".to_string(),
            schema_version: crate::search::policy::SEMANTIC_SCHEMA_VERSION,
            chunking_version: crate::search::policy::CHUNKING_STRATEGY_VERSION,
            dimension: 256,
            doc_count: 12,
            conversation_count: 3,
            db_fingerprint: "current-db".to_string(),
            index_path: "vector_index/vector.fast.idx".to_string(),
            size_bytes: 4096,
            started_at_ms: 1_733_100_000_000,
            completed_at_ms: 1_733_100_100_000,
            ready: true,
        }),
        ..Default::default()
    };
    manifest.save(temp.path()).expect("save semantic manifest");
    let vector_path = vector_index_path(temp.path(), HashEmbedder::default().id());
    std::fs::create_dir_all(vector_path.parent().expect("vector parent"))
        .expect("create vector dir");
    std::fs::write(&vector_path, b"fast").expect("write fast vector index");

    let state = semantic_state_from_availability(
        temp.path(),
        &SemanticAvailability::NotInstalled,
        SemanticPreference::DefaultModel,
        Some("current-db"),
    );

    assert_eq!(state.status, "ready");
    assert_eq!(state.availability, "ready");
    assert!(state.can_search);
    assert_eq!(state.fallback_mode, None);
    assert_eq!(
        state.embedder_id.as_deref(),
        Some(HashEmbedder::default().id())
    );
    assert_eq!(state.model_dir, None);
    assert_eq!(
        state.vector_index_path.as_deref(),
        Some(vector_path.as_path())
    );
    assert_eq!(state.hint, None);
}

#[test]
fn semantic_state_treats_ready_quality_tier_with_unknown_db_match_as_queryable() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut manifest = SemanticManifest {
        quality_tier: Some(ArtifactRecord {
            tier: crate::search::semantic_manifest::TierKind::Quality,
            embedder_id: HashEmbedder::default().id().to_string(),
            model_revision: "hash".to_string(),
            schema_version: crate::search::policy::SEMANTIC_SCHEMA_VERSION,
            chunking_version: crate::search::policy::CHUNKING_STRATEGY_VERSION,
            dimension: 256,
            doc_count: 249,
            conversation_count: 21,
            db_fingerprint: "boxed-db".to_string(),
            index_path: "vector_index/vector.quality.idx".to_string(),
            size_bytes: 221_824,
            started_at_ms: 1_733_100_000_000,
            completed_at_ms: 1_733_100_100_000,
            ready: true,
        }),
        ..Default::default()
    };
    manifest.save(temp.path()).expect("save semantic manifest");

    let state = semantic_state_from_availability(
        temp.path(),
        &SemanticAvailability::NotInstalled,
        SemanticPreference::DefaultModel,
        None,
    );

    assert_eq!(state.quality_tier.current_db_matches, None);
    assert!(
        state.quality_tier_published,
        "a ready quality tier with unknown DB match should remain visible as published in boxed data-dir status"
    );
    assert!(
        state.semantic_only_search_available,
        "semantic-only search can still run when the quality tier is ready and DB match is unknown"
    );
    assert!(state.can_search);
    assert_eq!(state.fallback_mode, None);
}

#[test]
fn semantic_state_promotes_complete_current_shard_generation() {
    let temp = tempfile::tempdir().expect("tempdir");
    let embedder_id = HashEmbedder::default().id().to_string();
    let mut records = Vec::new();
    for shard_index in 0..2_u32 {
        let relative_path = format!("vector_index/shards/fast-hash/shard-{shard_index}.fsvi");
        let path = temp.path().join(&relative_path);
        std::fs::create_dir_all(path.parent().expect("shard parent")).expect("create shard parent");
        std::fs::write(&path, b"fsvi").expect("write shard placeholder");
        records.push(SemanticShardRecord {
            tier: TierKind::Fast,
            embedder_id: embedder_id.clone(),
            model_revision: "hash".to_string(),
            schema_version: SEMANTIC_SCHEMA_VERSION,
            chunking_version: CHUNKING_STRATEGY_VERSION,
            dimension: HashEmbedder::default().dimension(),
            shard_index,
            shard_count: 2,
            doc_count: 10 + u64::from(shard_index),
            total_conversations: 7,
            db_fingerprint: "current-db".to_string(),
            index_path: relative_path,
            quantization: "f16".to_string(),
            mmap_ready: true,
            ann_index_path: None,
            ann_size_bytes: 0,
            ann_ready: false,
            size_bytes: 100 + u64::from(shard_index),
            started_at_ms: 1_733_100_000_000,
            completed_at_ms: 1_733_100_000_000 + i64::from(shard_index),
            ready: true,
        });
    }
    let mut shards = SemanticShardManifest {
        shards: records,
        ..Default::default()
    };
    shards.save(temp.path()).expect("save shard manifest");

    let state = semantic_state_from_availability(
        temp.path(),
        &SemanticAvailability::IndexMissing {
            index_path: vector_index_path(temp.path(), &embedder_id),
        },
        SemanticPreference::HashFallback,
        Some("current-db"),
    );

    assert_eq!(state.status, "ready");
    assert_eq!(state.availability, "ready");
    assert!(state.can_search);
    assert_eq!(state.fallback_mode, None);
    assert_eq!(state.fast_tier.doc_count, Some(21));
    assert_eq!(state.fast_tier.shard_count, Some(2));
    assert!(!state.progressive_ready);
    assert_eq!(
        state.progressive_reason_code,
        Some(SemanticProgressiveUnavailableReason::OwnerBackedReaderRequired.code())
    );
    assert!(
        state.summary.contains("owner_backed_reader_required"),
        "human summary and JSON reason code must agree: {}",
        state.summary
    );
    let expected_path = temp
        .path()
        .join("vector_index/shards/fast-hash/shard-0.fsvi");
    assert_eq!(
        state.vector_index_path.as_deref(),
        Some(expected_path.as_path())
    );
}

#[test]
fn semantic_state_rejects_complete_shard_generation_with_unsafe_path() {
    let temp = tempfile::tempdir().expect("tempdir");
    let outside = tempfile::tempdir().expect("outside tempdir");
    let outside_path = outside.path().join("outside.fsvi");
    std::fs::write(&outside_path, b"fsvi").expect("write outside placeholder");
    let embedder_id = HashEmbedder::default().id().to_string();
    let mut shards = SemanticShardManifest {
        shards: vec![SemanticShardRecord {
            tier: TierKind::Fast,
            embedder_id: embedder_id.clone(),
            model_revision: "hash".to_string(),
            schema_version: SEMANTIC_SCHEMA_VERSION,
            chunking_version: CHUNKING_STRATEGY_VERSION,
            dimension: HashEmbedder::default().dimension(),
            shard_index: 0,
            shard_count: 1,
            doc_count: 10,
            total_conversations: 7,
            db_fingerprint: "current-db".to_string(),
            index_path: outside_path.to_string_lossy().to_string(),
            quantization: "f16".to_string(),
            mmap_ready: true,
            ann_index_path: None,
            ann_size_bytes: 0,
            ann_ready: false,
            size_bytes: 100,
            started_at_ms: 1_733_100_000_000,
            completed_at_ms: 1_733_100_000_001,
            ready: true,
        }],
        ..Default::default()
    };
    shards.save(temp.path()).expect("save shard manifest");

    let base_vector_path = vector_index_path(temp.path(), &embedder_id);
    let state = semantic_state_from_availability(
        temp.path(),
        &SemanticAvailability::IndexMissing {
            index_path: base_vector_path.clone(),
        },
        SemanticPreference::HashFallback,
        Some("current-db"),
    );

    assert_ne!(state.status, "ready");
    assert!(!state.can_search);
    assert_eq!(
        state.vector_index_path.as_deref(),
        Some(base_vector_path.as_path())
    );
    assert_ne!(
        state.vector_index_path.as_deref(),
        Some(outside_path.as_path())
    );
}
