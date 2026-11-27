use super::*;
use tempfile::tempdir;

type AvailabilityTuiCase = (
    SemanticAvailability,
    &'static str,
    fn(&SemanticAvailability) -> bool,
);

#[test]
fn test_semantic_availability_ready() {
    let ready = SemanticAvailability::Ready {
        embedder_id: "test-123".into(),
    };
    assert!(ready.summary().contains("semantic ready"));
    assert!(ready.is_ready());
    assert!(ready.can_search());
    assert_eq!(ready.status_label(), "SEM");
}

#[test]
fn semantic_sidecar_path_rejects_paths_outside_data_dir() {
    let tmp = tempdir().unwrap();
    let safe = semantic_sidecar_path(tmp.path(), "vector_index/shards/hash/shard-0.fsvi")
        .expect("safe relative shard path");
    assert_eq!(
        safe,
        tmp.path().join("vector_index/shards/hash/shard-0.fsvi")
    );

    for unsafe_path in [
        tmp.path()
            .join("outside.fsvi")
            .to_string_lossy()
            .to_string(),
        "../outside.fsvi".to_string(),
        "vector_index/../outside.fsvi".to_string(),
        "./vector_index/shards/hash/shard-0.fsvi".to_string(),
    ] {
        assert!(
            semantic_sidecar_path(tmp.path(), &unsafe_path).is_none(),
            "unsafe semantic sidecar path should be rejected: {unsafe_path}"
        );
    }
}

#[test]
fn cache_state_preserves_selected_embedder_identity() {
    let model_dir = Path::new("/tmp/cass/models/multilingual-minilm");
    let missing = semantic_availability_from_cache_state(
        "multilingual-minilm-384",
        model_dir,
        &ModelCacheState::NotAcquired {
            missing_files: vec!["model.safetensors".to_string()],
        },
    )
    .expect("missing cache state");
    assert!(matches!(
        missing,
        SemanticAvailability::ModelMissing { embedder_id, model_dir: actual_dir, .. }
            if embedder_id == "multilingual-minilm-384" && actual_dir == model_dir
    ));

    // A stale marker revision no longer blocks loading: with no update
    // path the files (already checksum-verified upstream) are usable.
    let usable = semantic_availability_from_cache_state(
        "multilingual-minilm-384",
        model_dir,
        &ModelCacheState::IncompatibleVersion {
            current_revision: "old".to_string(),
            expected_revision: "new".to_string(),
        },
    );
    assert!(usable.is_none());
}

#[test]
fn test_semantic_availability_index_building() {
    let building = SemanticAvailability::IndexBuilding {
        embedder_id: "test".into(),
        progress_pct: Some(45),
        items_indexed: 100,
        total_items: 200,
    };
    assert!(building.summary().contains("building index"));
    assert!(building.summary().contains("45%"));
    assert!(building.is_building());
    assert_eq!(building.status_label(), "IDX...");

    let (pct, done, total) = building.index_progress().unwrap();
    assert_eq!(pct, Some(45));
    assert_eq!(done, 100);
    assert_eq!(total, 200);
}

#[test]
fn test_semantic_availability_tui_states() {
    let cases: &[AvailabilityTuiCase] = &[
        (
            SemanticAvailability::NotInstalled,
            "LEX",
            SemanticAvailability::is_not_installed,
        ),
        (SemanticAvailability::HashFallback, "SEM*", |state| {
            state.is_hash_fallback() && state.can_search()
        }),
        (
            SemanticAvailability::Disabled {
                reason: "offline mode".into(),
            },
            "OFF",
            |state| state.is_disabled() && state.summary().contains("offline"),
        ),
    ];

    for (state, expected_label, predicate) in cases {
        assert_eq!(state.status_label(), *expected_label, "{state:?}");
        assert!(predicate(state), "{state:?}");
    }
}

#[test]
fn test_semantic_availability_error_states() {
    let load_failed = SemanticAvailability::LoadFailed {
        context: "test error".into(),
    };
    assert!(load_failed.is_error());
    assert_eq!(load_failed.status_label(), "ERR");

    let db_unavail = SemanticAvailability::DatabaseUnavailable {
        db_path: PathBuf::from("/test"),
        error: "locked".into(),
    };
    assert!(db_unavail.is_error());
    assert_eq!(db_unavail.status_label(), "NODB");
}

#[test]
fn test_needs_index_rebuild_no_index() {
    let tmp = tempdir().unwrap();
    assert!(!needs_index_rebuild(tmp.path()));
}

#[test]
fn test_delete_vector_index_no_file() {
    let tmp = tempdir().unwrap();
    let result = delete_vector_index_for_rebuild(tmp.path());
    assert!(result.is_ok());
    assert!(!result.unwrap());
}

fn write_hash_vector_index(path: &Path, record_count: usize) {
    let embedder = HashEmbedder::default();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create vector index parent");
    }
    let mut writer = VectorIndex::create_with_revision(
        path,
        embedder.id(),
        crate::search_defaults::HASH_VECTOR_SPACE_REVISION,
        embedder.dimension(),
        frankensearch::index::Quantization::F16,
    )
    .expect("create hash vector index");
    let mut vector = vec![0.0_f32; embedder.dimension()];
    vector[0] = 1.0;
    for idx in 0..record_count {
        writer
            .write_record(&format!("doc-{idx}"), &vector)
            .expect("write hash vector record");
    }
    writer.finish().expect("finish hash vector index");
}

#[test]
fn exact_artifact_contract_pairs_only_existing_consumer_owned_ann() {
    let tmp = tempdir().expect("hash context fixture");
    let db_path = tmp.path().join("cass.db");
    let storage = FrankenStorage::open(&db_path).expect("create cass db");
    drop(storage);

    let embedder = HashEmbedder::default();
    let fsvi_path = vector_index_path(tmp.path(), embedder.id());
    write_hash_vector_index(&fsvi_path, 1);

    let without_ann = load_hash_semantic_context(tmp.path(), &db_path)
        .context
        .expect("hash context without ANN");
    assert_eq!(without_ann.artifacts.len(), 1);
    assert_eq!(without_ann.artifacts[0].fsvi_path(), fsvi_path);
    assert_eq!(
        without_ann.artifacts[0].ann_path(),
        None,
        "a synthesized-but-missing ANN filename must not become a serving artifact"
    );

    let ann_path = hnsw_index_path(tmp.path(), embedder.id());
    std::fs::write(&ann_path, b"selected CASS ANN sidecar fixture")
        .expect("write selected ANN sidecar");
    let with_ann = load_hash_semantic_context(tmp.path(), &db_path)
        .context
        .expect("hash context with ANN");
    assert_eq!(
        with_ann.artifacts[0].ann_path(),
        Some(ann_path.as_path()),
        "an existing CASS-owned ANN path must stay paired with its exact FSVI"
    );
}

/// GH #404: a vector index that parses but whose manifest record was
/// built against a different database fingerprint must not be served —
/// `cass status` already reports `can_search:false` for it, and the
/// query path must agree instead of returning k arbitrary neighbours.
#[test]
fn stale_manifest_tier_is_refused_even_when_the_artifact_loads() {
    use crate::search::semantic_manifest::{SemanticManifest, TierKind};

    let tmp = tempdir().expect("stale context fixture");
    let db_path = tmp.path().join("cass.db");
    let storage = FrankenStorage::open(&db_path).expect("create cass db");
    drop(storage);

    let embedder = HashEmbedder::default();
    let fsvi_path = vector_index_path(tmp.path(), embedder.id());
    write_hash_vector_index(&fsvi_path, 1);
    let ann_path = hnsw_index_path(tmp.path(), embedder.id());
    std::fs::write(&ann_path, b"selected CASS ANN sidecar fixture")
        .expect("write selected ANN sidecar");

    // Control: with no manifest at all, the legacy manifest-less artifact
    // still serves (no verdict contradicts it).
    assert!(
        load_hash_semantic_context(tmp.path(), &db_path)
            .context
            .is_some(),
        "manifest-less artifacts keep serving"
    );

    // Canonical identity rewrites (such as Pi -> OMP reclassification)
    // do not change the count/max-id fingerprint. The durable storage
    // marker must therefore override even the legacy manifest-less
    // serving allowance until a semantic republish acknowledges it.
    let storage = FrankenStorage::open(&db_path).expect("open identity marker fixture");
    storage
        .mark_semantic_identity_rebuild_required(SemanticIdentityTier::Fast)
        .expect("mark semantic identity stale");
    drop(storage);
    let identity_stale = load_hash_semantic_context(tmp.path(), &db_path);
    assert!(identity_stale.context.is_none());
    assert!(
        identity_stale.availability.is_index_stale(),
        "identity-invalidated semantic assets must fail closed: {:?}",
        identity_stale.availability
    );
    let storage = FrankenStorage::open(&db_path).expect("reopen identity marker fixture");
    storage
        .complete_semantic_identity_rebuild(SemanticIdentityTier::Fast)
        .expect("complete semantic identity rebuild");
    drop(storage);
    assert!(
        load_hash_semantic_context(tmp.path(), &db_path)
            .context
            .is_some(),
        "an acknowledged semantic republish should restore manifest-less serving"
    );

    // A manifest record whose fingerprint belongs to some other database.
    let mut manifest = SemanticManifest::load_or_default(tmp.path()).expect("manifest");
    assert!(manifest.adopt_legacy_artifact(
        TierKind::Fast,
        embedder.id(),
        "hash",
        embedder.dimension(),
        1,
        1,
        "content-v1:999999:999999:999999",
        "vector_index/index-fnv1a-384.fsvi",
        1,
    ));
    manifest.save(tmp.path()).expect("save stale manifest");

    let setup = load_hash_semantic_context(tmp.path(), &db_path);
    assert!(
        setup.context.is_none(),
        "stale tier must not produce a serving context"
    );
    assert!(
        setup.availability.is_index_stale(),
        "expected IndexStale, got {:?}",
        setup.availability
    );
    assert!(
        !setup.availability.can_search(),
        "stale assets must report can_search=false to the query path"
    );

    // Once the record matches the live database again, serving resumes.
    let current =
        crate::storage::archive_fingerprint::lexical_storage_fingerprint_for_db(&db_path)
            .expect("current fingerprint");
    let mut manifest = SemanticManifest::load_or_default(tmp.path()).expect("manifest");
    assert!(manifest.adopt_legacy_artifact(
        TierKind::Fast,
        embedder.id(),
        "hash",
        embedder.dimension(),
        1,
        1,
        &current,
        "vector_index/index-fnv1a-384.fsvi",
        1,
    ));
    manifest.save(tmp.path()).expect("save current manifest");
    assert!(
        load_hash_semantic_context(tmp.path(), &db_path)
            .context
            .is_some(),
        "a current tier record must serve again"
    );
}

#[cfg(unix)]
#[test]
fn readiness_and_serving_reject_the_same_final_component_fsvi_symlink() {
    use std::os::unix::fs::symlink;

    let tmp = tempdir().expect("FSVI symlink fixture");
    let db_path = tmp.path().join("cass.db");
    let storage = FrankenStorage::open(&db_path).expect("create cass db");
    drop(storage);

    let embedder = HashEmbedder::default();
    let selected_path = vector_index_path(tmp.path(), embedder.id());
    let target_path = selected_path.with_file_name("unselected-target.fsvi");
    write_hash_vector_index(&target_path, 1);
    let target_bytes = std::fs::read(&target_path).expect("read target FSVI before probes");
    symlink(&target_path, &selected_path).expect("create final-component FSVI symlink");

    let serving_error = SemanticIndexArtifact::open(&selected_path, None)
        .expect_err("serving must reject a final-component FSVI symlink");
    assert!(serving_error.to_string().contains("symlink"));

    let readiness = probe_hash_semantic_availability(tmp.path());
    let readiness_context = match readiness {
        SemanticAvailability::LoadFailed { context } => context,
        other => panic!("readiness must reject the same FSVI symlink: {other:?}"),
    };
    let serving = load_hash_semantic_context(tmp.path(), &db_path);
    let serving_context = match serving.availability {
        SemanticAvailability::LoadFailed { context } => context,
        other => panic!("semantic serving must reject the same FSVI symlink: {other:?}"),
    };

    assert_eq!(
        readiness_context, serving_context,
        "readiness and serving must use one exact-artifact admission policy"
    );
    assert!(readiness_context.contains("symlink"));
    assert!(
        serving.context.is_none(),
        "a rejected exact artifact must never produce a serving context"
    );
    assert_eq!(
        std::fs::read_link(&selected_path).expect("read selected FSVI symlink"),
        target_path
    );
    assert_eq!(
        std::fs::read(&target_path).expect("read target FSVI after probes"),
        target_bytes,
        "readiness and serving probes must not mutate the symlink target"
    );

    let broken = tempdir().expect("broken FSVI symlink fixture");
    let broken_db_path = broken.path().join("cass.db");
    let storage = FrankenStorage::open(&broken_db_path).expect("create broken fixture db");
    drop(storage);
    let broken_path = vector_index_path(broken.path(), embedder.id());
    std::fs::create_dir_all(
        broken_path
            .parent()
            .expect("broken selected FSVI must have a parent"),
    )
    .expect("create broken selected FSVI parent");
    symlink(broken.path().join("absent-target.fsvi"), &broken_path)
        .expect("create broken final-component FSVI symlink");

    let broken_readiness = probe_hash_semantic_availability(broken.path());
    let broken_serving = load_hash_semantic_context(broken.path(), &broken_db_path);
    let broken_readiness_context = match broken_readiness {
        SemanticAvailability::LoadFailed { context } => context,
        other => panic!("broken FSVI symlink must be rejected by readiness: {other:?}"),
    };
    let broken_serving_context = match broken_serving.availability {
        SemanticAvailability::LoadFailed { context } => context,
        other => panic!("broken FSVI symlink must be rejected by serving: {other:?}"),
    };
    assert_eq!(broken_readiness_context, broken_serving_context);
    assert!(broken_readiness_context.contains("symlink"));
    assert!(broken_serving.context.is_none());

    let non_file = tempdir().expect("non-file FSVI fixture");
    let non_file_db_path = non_file.path().join("cass.db");
    let storage = FrankenStorage::open(&non_file_db_path).expect("create non-file fixture db");
    drop(storage);
    let non_file_path = vector_index_path(non_file.path(), embedder.id());
    std::fs::create_dir_all(
        non_file_path
            .parent()
            .expect("non-file selected FSVI must have a parent"),
    )
    .expect("create non-file selected FSVI parent");
    std::fs::create_dir(&non_file_path).expect("create directory at selected FSVI path");

    let non_file_readiness = probe_hash_semantic_availability(non_file.path());
    let non_file_serving = load_hash_semantic_context(non_file.path(), &non_file_db_path);
    let non_file_readiness_context = match non_file_readiness {
        SemanticAvailability::LoadFailed { context } => context,
        other => panic!("non-file FSVI must be rejected by readiness: {other:?}"),
    };
    let non_file_serving_context = match non_file_serving.availability {
        SemanticAvailability::LoadFailed { context } => context,
        other => panic!("non-file FSVI must be rejected by serving: {other:?}"),
    };
    assert_eq!(non_file_readiness_context, non_file_serving_context);
    assert!(
        non_file_readiness_context.contains("open semantic FSVI"),
        "non-file selection must reach the exact-artifact opener: {non_file_readiness_context}"
    );
    assert!(non_file_serving.context.is_none());
}

#[test]
fn exact_artifact_contract_ann_selection_preserves_existing_invalid_artifacts() {
    let tmp = tempdir().expect("ANN selection fixture");
    let missing = tmp.path().join("missing.chsw");
    assert_eq!(select_existing_ann_candidate(missing), None);

    let regular = tmp.path().join("regular.chsw");
    std::fs::write(&regular, b"not necessarily valid ANN metadata").expect("write ANN candidate");
    assert_eq!(
        select_existing_ann_candidate(regular.clone()),
        Some(regular)
    );

    let directory = tmp.path().join("directory.chsw");
    std::fs::create_dir(&directory).expect("create invalid ANN directory");
    assert_eq!(
        select_existing_ann_candidate(directory.clone()),
        Some(directory),
        "an existing invalid candidate must reach bounded rejection instead of being mislabeled missing"
    );

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;

        let broken_symlink = tmp.path().join("broken-symlink.chsw");
        symlink(tmp.path().join("absent-target"), &broken_symlink)
            .expect("create broken ANN symlink");
        assert_eq!(
            select_existing_ann_candidate(broken_symlink.clone()),
            Some(broken_symlink),
            "a selected final-component symlink must reach explicit rejection"
        );
    }
}

#[test]
fn exact_artifact_contract_pointer_selected_generation_requires_retained_owner() {
    let tmp = tempdir().expect("pointer-selected fixture");
    let db_path = tmp.path().join("cass.db");
    let storage = FrankenStorage::open(&db_path).expect("create cass db");
    drop(storage);

    let embedder = HashEmbedder::default();
    let legacy_path = vector_index_path(tmp.path(), embedder.id());
    write_hash_vector_index(&legacy_path, 1);
    let legacy_bytes = std::fs::read(&legacy_path).expect("read legacy FSVI");
    let legacy_mtime = std::fs::metadata(&legacy_path)
        .and_then(|metadata| metadata.modified())
        .expect("legacy FSVI mtime");

    let pointer = SemanticCurrentPointerV1 {
        schema_version: 1,
        generation_id: "gen-selected-owner-contract".to_string(),
        manifest_sha256: "a".repeat(64),
        selection_epoch: 1,
        selected_at_ms: 1_733_100_000_000,
    };
    let pointer_path = SemanticCurrentPointerV1::path(tmp.path());
    std::fs::create_dir_all(pointer_path.parent().expect("pointer parent"))
        .expect("create pointer parent");
    std::fs::write(
        &pointer_path,
        serde_json::to_vec(&pointer).expect("serialize selected pointer"),
    )
    .expect("write selected pointer");
    let mut entries_before = std::fs::read_dir(tmp.path())
        .expect("list fixture before load")
        .map(|entry| entry.expect("fixture entry").file_name())
        .collect::<Vec<_>>();
    entries_before.sort();

    for availability in [
        probe_hash_semantic_availability(tmp.path()),
        probe_semantic_availability(tmp.path()),
        load_hash_semantic_context(tmp.path(), &db_path).availability,
        load_semantic_context(tmp.path(), &db_path).availability,
    ] {
        assert!(
            matches!(
                &availability,
                SemanticAvailability::LoadFailed { context }
                    if context
                        == SemanticProgressiveUnavailableReason::OwnerBackedReaderRequired
                            .code()
            ),
            "pointer-selected generation must fail closed with the stable owner reason: {availability:?}"
        );
    }
    assert!(
        load_hash_semantic_context(tmp.path(), &db_path)
            .context
            .is_none(),
        "a valid legacy file must not bypass the selected-generation owner requirement"
    );
    assert_eq!(
        std::fs::read(&legacy_path).expect("read legacy FSVI after rejected load"),
        legacy_bytes
    );
    assert_eq!(
        std::fs::metadata(&legacy_path)
            .and_then(|metadata| metadata.modified())
            .expect("legacy FSVI mtime after rejected load"),
        legacy_mtime
    );
    let mut entries_after = std::fs::read_dir(tmp.path())
        .expect("list fixture after load")
        .map(|entry| entry.expect("fixture entry").file_name())
        .collect::<Vec<_>>();
    entries_after.sort();
    assert_eq!(
        entries_after, entries_before,
        "fail-closed pointer handling must not create serving aliases"
    );
}

#[test]
fn same_id_and_dimension_with_legacy_revision_requires_rebuild() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let index_path = vector_index_path(tmp.path(), FastEmbedder::embedder_id_static());
    let Some(parent) = index_path.parent() else {
        anyhow::bail!("vector index path has no parent");
    };
    std::fs::create_dir_all(parent)?;

    for legacy in [
        "1.0",
        "native-minilm-v1:c9745ed1d9f207416be6d2e6f8de32d1f16199bf",
    ] {
        let writer = VectorIndex::create_with_revision(
            &index_path,
            FastEmbedder::embedder_id_static(),
            legacy,
            384,
            frankensearch::index::Quantization::F16,
        )?;
        writer.finish()?;
        assert!(needs_index_rebuild(tmp.path()));
    }

    let expected_revision = expected_vector_space_revision(FastEmbedder::embedder_id_static())
        .ok_or_else(|| anyhow::anyhow!("MiniLM vector-space revision is not registered"))?;
    let writer = VectorIndex::create_with_revision(
        &index_path,
        FastEmbedder::embedder_id_static(),
        expected_revision,
        384,
        frankensearch::index::Quantization::F16,
    )?;
    writer.finish()?;
    assert!(!needs_index_rebuild(tmp.path()));
    Ok(())
}

fn semantic_shard_record(
    tier: TierKind,
    embedder_id: &str,
    db_fingerprint: &str,
    shard_index: u32,
    shard_count: u32,
) -> SemanticShardRecord {
    SemanticShardRecord {
        tier,
        embedder_id: embedder_id.to_string(),
        model_revision: "test-revision".to_string(),
        schema_version: crate::search::policy::SEMANTIC_SCHEMA_VERSION,
        chunking_version: crate::search::policy::CHUNKING_STRATEGY_VERSION,
        dimension: 384,
        shard_index,
        shard_count,
        doc_count: 1,
        total_conversations: 1,
        db_fingerprint: db_fingerprint.to_string(),
        index_path: format!("vector_index/shards/{embedder_id}/shard-{shard_index}.fsvi"),
        quantization: "f16".to_string(),
        mmap_ready: true,
        ann_index_path: None,
        ann_size_bytes: 0,
        ann_ready: false,
        size_bytes: 128,
        started_at_ms: 1_733_100_000_000,
        completed_at_ms: 1_733_100_000_000 + i64::from(shard_index),
        ready: true,
    }
}

#[test]
fn shard_candidate_probe_is_false_without_manifest() {
    let tmp = tempdir().unwrap();
    assert!(
        !complete_shard_generation_candidate_exists(tmp.path(), "fnv1a-384"),
        "missing shard manifest must not trigger a current-DB fingerprint"
    );
    assert!(
        load_complete_shard_artifacts_with_fingerprint(
            tmp.path(),
            "fnv1a-384",
            384,
            "test",
            || panic!("missing shards must not open the archive for fingerprinting"),
        )
        .is_none()
    );
}

#[test]
fn shard_candidate_probe_is_false_for_unreadable_manifest() {
    let tmp = tempdir().unwrap();
    let path = SemanticShardManifest::path(tmp.path());
    std::fs::create_dir_all(path.parent().expect("manifest parent"))
        .expect("create shard manifest dir");
    std::fs::write(&path, b"not json").expect("write invalid shard manifest");

    assert!(
        !complete_shard_generation_candidate_exists(tmp.path(), "fnv1a-384"),
        "corrupt shard metadata must not trigger a query-time current-DB fingerprint"
    );
    assert!(
        load_complete_shard_artifacts_with_fingerprint(
            tmp.path(),
            "fnv1a-384",
            384,
            "test",
            || panic!("invalid shards must not open the archive for fingerprinting"),
        )
        .is_none()
    );
}

#[test]
fn shard_candidate_probe_ignores_other_or_incomplete_generations() {
    let tmp = tempdir().unwrap();
    let mut manifest = SemanticShardManifest {
        shards: vec![
            semantic_shard_record(TierKind::Fast, "other-384", "fp-other", 0, 1),
            semantic_shard_record(TierKind::Fast, "fnv1a-384", "fp-partial", 0, 2),
        ],
        ..Default::default()
    };
    manifest.save(tmp.path()).expect("save shard manifest");

    assert!(
        !complete_shard_generation_candidate_exists(tmp.path(), "fnv1a-384"),
        "incomplete or unrelated shard generations must not trigger a current-DB fingerprint"
    );
    assert!(
        load_complete_shard_artifacts_with_fingerprint(
            tmp.path(),
            "fnv1a-384",
            384,
            "test",
            || panic!("incomplete or unrelated shards must not fingerprint the archive"),
        )
        .is_none()
    );
}

#[test]
fn shard_candidate_probe_detects_complete_generation_for_embedder() {
    let tmp = tempdir().unwrap();
    let mut manifest = SemanticShardManifest {
        shards: vec![
            semantic_shard_record(TierKind::Fast, "fnv1a-384", "fp-current", 0, 2),
            semantic_shard_record(TierKind::Fast, "fnv1a-384", "fp-current", 1, 2),
        ],
        ..Default::default()
    };
    manifest.save(tmp.path()).expect("save shard manifest");

    assert!(
        complete_shard_generation_candidate_exists(tmp.path(), "fnv1a-384"),
        "complete candidate generations should allow the current-DB fingerprint check"
    );
    let fingerprint_calls = std::cell::Cell::new(0);
    assert!(
        load_complete_shard_artifacts_with_fingerprint(
            tmp.path(),
            "fnv1a-384",
            384,
            "test",
            || {
                fingerprint_calls.set(fingerprint_calls.get() + 1);
                Ok("fp-current".to_string())
            },
        )
        .is_none(),
        "metadata completeness alone must not admit missing shard artifacts"
    );
    assert_eq!(
        fingerprint_calls.get(),
        1,
        "a complete candidate must still fingerprint the current archive exactly once"
    );
}

#[test]
fn load_hash_context_prefers_current_complete_shards_over_monolithic_file() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("cass.db");
    let storage = FrankenStorage::open(&db_path).expect("create cass db");
    drop(storage);
    let db_fingerprint =
        crate::storage::archive_fingerprint::lexical_storage_fingerprint_for_db(&db_path)
            .expect("fingerprint cass db");

    let embedder = HashEmbedder::default();
    write_hash_vector_index(&vector_index_path(tmp.path(), embedder.id()), 1);

    let mut records = Vec::new();
    for shard_index in 0..2_u32 {
        let relative_path = format!("vector_index/shards/hash/shard-{shard_index}.fsvi");
        let shard_path = tmp.path().join(&relative_path);
        write_hash_vector_index(&shard_path, 1);
        records.push(SemanticShardRecord {
            tier: TierKind::Fast,
            embedder_id: embedder.id().to_string(),
            model_revision: "hash".to_string(),
            schema_version: crate::search::policy::SEMANTIC_SCHEMA_VERSION,
            chunking_version: crate::search::policy::CHUNKING_STRATEGY_VERSION,
            dimension: embedder.dimension(),
            shard_index,
            shard_count: 2,
            doc_count: 1,
            total_conversations: 1,
            db_fingerprint: db_fingerprint.clone(),
            index_path: relative_path,
            quantization: "f16".to_string(),
            mmap_ready: true,
            ann_index_path: None,
            ann_size_bytes: 0,
            ann_ready: false,
            size_bytes: std::fs::metadata(&shard_path)
                .expect("stat hash shard")
                .len(),
            started_at_ms: 1_733_100_000_000,
            completed_at_ms: 1_733_100_000_000 + i64::from(shard_index),
            ready: true,
        });
    }
    let mut manifest = SemanticShardManifest {
        shards: records,
        ..Default::default()
    };
    manifest.save(tmp.path()).expect("save shard manifest");

    let setup = load_hash_semantic_context(tmp.path(), &db_path);
    assert!(
        matches!(setup.availability, SemanticAvailability::HashFallback),
        "hash semantic availability should remain ready: {:?}",
        setup.availability
    );
    let context = setup
        .context
        .expect("complete current shards should load a semantic context");
    assert_eq!(
        context.artifacts.len(),
        2,
        "complete current shards must not be shadowed by an older monolithic vector file"
    );
    let loaded_records = context
        .artifacts
        .iter()
        .map(|artifact| artifact.index().record_count())
        .sum::<usize>();
    assert_eq!(loaded_records, 2);
}
