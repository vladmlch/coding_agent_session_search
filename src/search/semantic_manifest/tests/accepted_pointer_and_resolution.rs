use super::*;

#[test]
fn accepted_pointer_publish_is_locked_compare_and_swap_under_concurrency() {
    use std::sync::{Arc, Barrier};

    let temp = tempfile::tempdir().unwrap();
    let corpus = test_corpus_identity();
    let mut manifests = Vec::new();
    for (suffix, bytes) in [("a", b"vector-a".to_vec()), ("b", b"vector-b".to_vec())] {
        let artifact = complete_generation_artifact(
            SemanticArtifactRole::FastVector,
            "fast/primary.fsvi",
            &bytes,
            &corpus,
        );
        let manifest = generation_with_id(
            &format!("gen-concurrent-{suffix}"),
            SemanticGenerationTopology::FastOnly,
            vec![artifact],
        );
        seal_generation(
            temp.path(),
            &manifest,
            &BTreeMap::from([(SemanticArtifactRole::FastVector, bytes)]),
        );
        manifests.push(manifest);
    }

    let barrier = Arc::new(Barrier::new(3));
    let data_dir = Arc::new(temp.path().to_path_buf());
    let handles = manifests
        .iter()
        .cloned()
        .map(|manifest| {
            let barrier = Arc::clone(&barrier);
            let data_dir = Arc::clone(&data_dir);
            std::thread::spawn(move || {
                let pointer =
                    SemanticCurrentPointerV1::for_manifest(&manifest, manifest.sealed_at_ms + 1, 1)
                        .unwrap();
                barrier.wait();
                pointer.publish_atomic(&data_dir, &SemanticExpectedCurrentV1::Absent)
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();
    let results = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(
                result,
                Err(SemanticGenerationError::ConcurrentPublishConflict { .. })
            ))
            .count(),
        1
    );
    let loaded = load_current_semantic_generation(temp.path(), Some(&corpus)).unwrap();
    assert!(
        manifests
            .iter()
            .any(|manifest| manifest.generation_id == loaded.manifest.generation_id)
    );
}

#[test]
fn accepted_pointer_faults_select_exactly_old_or_new_generation() {
    let checkpoints = [
        (PointerPublishCheckpoint::TemporaryBytesWritten, false),
        (PointerPublishCheckpoint::TemporaryFileSynced, false),
        (PointerPublishCheckpoint::PointerReplaced, true),
        (PointerPublishCheckpoint::ParentDirectorySynced, true),
    ];
    for (fault, selects_new) in checkpoints {
        let temp = tempfile::tempdir().unwrap();
        let corpus = test_corpus_identity();
        let mut manifests = Vec::new();
        for (id, bytes) in [
            ("gen-pointer-old", b"old-vector".to_vec()),
            ("gen-pointer-new", b"new-vector".to_vec()),
        ] {
            let artifact = complete_generation_artifact(
                SemanticArtifactRole::FastVector,
                "fast/primary.fsvi",
                &bytes,
                &corpus,
            );
            let manifest =
                generation_with_id(id, SemanticGenerationTopology::FastOnly, vec![artifact]);
            seal_generation(
                temp.path(),
                &manifest,
                &BTreeMap::from([(SemanticArtifactRole::FastVector, bytes)]),
            );
            manifests.push(manifest);
        }
        let old_pointer =
            SemanticCurrentPointerV1::for_manifest(&manifests[0], 1_700_000_000_400, 1).unwrap();
        old_pointer
            .publish_atomic(temp.path(), &SemanticExpectedCurrentV1::Absent)
            .unwrap();
        let new_pointer =
            SemanticCurrentPointerV1::for_manifest(&manifests[1], 1_700_000_000_500, 2).unwrap();
        let error = new_pointer
            .publish_atomic_with_hook(
                temp.path(),
                &SemanticExpectedCurrentV1::exact(&old_pointer),
                |observed| {
                    if observed == fault {
                        return Err(SemanticGenerationError::PointerIo {
                            source: "injected pointer fault".to_owned(),
                        });
                    }
                    Ok(())
                },
            )
            .expect_err("injected pointer checkpoint");
        assert!(matches!(error, SemanticGenerationError::PointerIo { .. }));
        let loaded = load_current_semantic_generation(temp.path(), Some(&corpus)).unwrap();
        assert_eq!(
            loaded.manifest.generation_id,
            manifests[usize::from(selects_new)].generation_id
        );
    }
}

#[test]
fn accepted_pointer_selection_timestamp_is_independent_and_supports_republication() {
    let temp = tempfile::tempdir().unwrap();
    let corpus = test_corpus_identity();
    let bytes = b"republish-vector".to_vec();
    let artifact = complete_generation_artifact(
        SemanticArtifactRole::FastVector,
        "fast/primary.fsvi",
        &bytes,
        &corpus,
    );
    let manifest = generation_with_id(
        "gen-republish",
        SemanticGenerationTopology::FastOnly,
        vec![artifact],
    );
    seal_generation(
        temp.path(),
        &manifest,
        &BTreeMap::from([(SemanticArtifactRole::FastVector, bytes)]),
    );
    assert!(
        SemanticCurrentPointerV1::for_manifest(&manifest, manifest.sealed_at_ms - 1, 1).is_err()
    );
    assert!(matches!(
        SemanticCurrentPointerV1::for_manifest(&manifest, manifest.sealed_at_ms + 1, 0),
        Err(SemanticGenerationError::InvalidPointer { reason })
            if reason == "selection_epoch must be positive"
    ));

    let first =
        SemanticCurrentPointerV1::for_manifest(&manifest, manifest.sealed_at_ms + 100, 1).unwrap();
    first
        .publish_atomic(temp.path(), &SemanticExpectedCurrentV1::Absent)
        .unwrap();
    let second =
        SemanticCurrentPointerV1::for_manifest(&manifest, manifest.sealed_at_ms + 200, 2).unwrap();
    second
        .publish_atomic(temp.path(), &SemanticExpectedCurrentV1::exact(&first))
        .unwrap();
    let loaded = load_current_semantic_generation(temp.path(), None).unwrap();
    assert_eq!(
        loaded.manifest.sha256().unwrap(),
        manifest.sha256().unwrap()
    );
    assert_eq!(loaded.pointer.selected_at_ms, manifest.sealed_at_ms + 200);
    assert_eq!(loaded.manifest.sealed_at_ms, manifest.sealed_at_ms);
}

#[test]
fn accepted_pointer_cas_rejects_aba_and_requires_a_strictly_newer_selection_epoch() {
    let temp = tempfile::tempdir().unwrap();
    let corpus = test_corpus_identity();
    let mut manifests = Vec::new();
    for (generation_id, bytes) in [
        ("gen-aba-a", b"aba-vector-a".to_vec()),
        ("gen-aba-b", b"aba-vector-b".to_vec()),
    ] {
        let artifact = complete_generation_artifact(
            SemanticArtifactRole::FastVector,
            "fast/primary.fsvi",
            &bytes,
            &corpus,
        );
        let manifest = generation_with_id(
            generation_id,
            SemanticGenerationTopology::FastOnly,
            vec![artifact],
        );
        seal_generation(
            temp.path(),
            &manifest,
            &BTreeMap::from([(SemanticArtifactRole::FastVector, bytes)]),
        );
        manifests.push(manifest);
    }

    let selected_a_first =
        SemanticCurrentPointerV1::for_manifest(&manifests[0], 1_700_000_000_400, 1).unwrap();
    selected_a_first
        .publish_atomic(temp.path(), &SemanticExpectedCurrentV1::Absent)
        .unwrap();
    let stale_a_expectation = SemanticExpectedCurrentV1::exact(&selected_a_first);

    let selected_b =
        SemanticCurrentPointerV1::for_manifest(&manifests[1], 1_700_000_000_500, 2).unwrap();
    selected_b
        .publish_atomic(
            temp.path(),
            &SemanticExpectedCurrentV1::exact(&selected_a_first),
        )
        .unwrap();
    let selected_a_again =
        SemanticCurrentPointerV1::for_manifest(&manifests[0], 1_700_000_000_600, 3).unwrap();
    selected_a_again
        .publish_atomic(temp.path(), &SemanticExpectedCurrentV1::exact(&selected_b))
        .unwrap();

    let stale_publisher_target =
        SemanticCurrentPointerV1::for_manifest(&manifests[1], 1_700_000_000_700, 4).unwrap();
    assert!(matches!(
        stale_publisher_target.publish_atomic(temp.path(), &stale_a_expectation),
        Err(SemanticGenerationError::ConcurrentPublishConflict { .. })
    ));
    let after_stale_attempt = load_current_semantic_generation(temp.path(), None).unwrap();
    assert_eq!(after_stale_attempt.pointer, selected_a_again);

    let equal_epoch = SemanticCurrentPointerV1::for_manifest(
        &manifests[1],
        selected_a_again.selected_at_ms,
        selected_a_again.selection_epoch,
    )
    .unwrap();
    let error = equal_epoch
        .publish_atomic(
            temp.path(),
            &SemanticExpectedCurrentV1::exact(&selected_a_again),
        )
        .expect_err("selection epochs must advance strictly");
    assert!(matches!(
        &error,
        SemanticGenerationError::SelectionEpochNotAdvanced { .. }
    ));
    assert_eq!(
        error.recovery_action(),
        SemanticGenerationRecoveryAction::RetryPublish
    );
}

#[cfg(unix)]
#[test]
fn accepted_path_checks_reject_symlinks_hardlink_aliases_and_portable_aliases() {
    use std::os::unix::fs::symlink;

    let corpus = test_corpus_identity();
    for path in [
        "../escape.fsvi",
        "CON/index.fsvi",
        "fast/trailing.",
        "fast/stream:payload.fsvi",
        "fast//duplicate.fsvi",
        "fast/naïve.fsvi",
    ] {
        let artifact = complete_generation_artifact(
            SemanticArtifactRole::FastVector,
            path,
            b"path-vector",
            &corpus,
        );
        let error = test_generation_manifest(SemanticGenerationTopology::FastOnly, vec![artifact])
            .validate()
            .expect_err("portable alias path");
        assert_eq!(error.reason_code(), "semantic_manifest_path_invalid");
    }

    let base = complete_generation_artifact(
        SemanticArtifactRole::FastVector,
        "fast/primary.fsvi",
        b"case-base",
        &corpus,
    );
    let mut ann = bind_ann_to_base(
        complete_generation_artifact(
            SemanticArtifactRole::FastAnn,
            "FAST/PRIMARY.FSVI",
            b"case-ann",
            &corpus,
        ),
        &base,
    );
    ann.ann_base.as_mut().unwrap().parameters_sha256 = ann.artifact_parameters_sha256.clone();
    let collision = test_generation_manifest(SemanticGenerationTopology::FastOnly, vec![base, ann]);
    assert_eq!(
        collision.validate().unwrap_err().reason_code(),
        "semantic_manifest_path_invalid"
    );

    let temp = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let artifact = complete_generation_artifact(
        SemanticArtifactRole::FastVector,
        "fast/primary.fsvi",
        b"symlink-vector",
        &corpus,
    );
    let manifest = generation_with_id(
        "gen-symlink",
        SemanticGenerationTopology::FastOnly,
        vec![artifact],
    );
    let generation_dir = manifest.generation_dir(temp.path()).unwrap();
    fs::create_dir_all(generation_dir.join("fast")).unwrap();
    let outside_file = outside.path().join("outside.fsvi");
    fs::write(&outside_file, b"symlink-vector").unwrap();
    symlink(&outside_file, generation_dir.join("fast/primary.fsvi")).unwrap();
    assert!(matches!(
        manifest.write_immutable(temp.path()),
        Err(SemanticGenerationError::ArtifactIo { .. })
            | Err(SemanticGenerationError::ManifestIo { .. })
    ));
    assert!(!manifest.manifest_path(temp.path()).unwrap().exists());

    let alias_temp = tempfile::tempdir().unwrap();
    let base = complete_generation_artifact(
        SemanticArtifactRole::FastVector,
        "fast/primary.fsvi",
        b"same-physical-file",
        &corpus,
    );
    let ann = bind_ann_to_base(
        complete_generation_artifact(
            SemanticArtifactRole::FastAnn,
            "fast/search.hnsw",
            b"same-physical-file",
            &corpus,
        ),
        &base,
    );
    let alias_manifest = generation_with_id(
        "gen-hardlink",
        SemanticGenerationTopology::FastOnly,
        vec![base, ann],
    );
    let generation_dir = alias_manifest.generation_dir(alias_temp.path()).unwrap();
    fs::create_dir_all(generation_dir.join("fast")).unwrap();
    let vector_path = generation_dir.join("fast/primary.fsvi");
    fs::write(&vector_path, b"same-physical-file").unwrap();
    fs::hard_link(&vector_path, generation_dir.join("fast/search.hnsw")).unwrap();
    assert!(matches!(
        alias_manifest.write_immutable(alias_temp.path()),
        Err(SemanticGenerationError::InvalidManifest {
            class: SemanticManifestInvariantClass::Path,
            ..
        })
    ));

    let external_link_temp = tempfile::tempdir().unwrap();
    let external_link_target = tempfile::tempdir().unwrap();
    let external_artifact = complete_generation_artifact(
        SemanticArtifactRole::FastVector,
        "fast/primary.fsvi",
        b"externally-linked",
        &corpus,
    );
    let external_manifest = generation_with_id(
        "gen-external-hardlink",
        SemanticGenerationTopology::FastOnly,
        vec![external_artifact],
    );
    write_generation_artifacts(
        external_link_temp.path(),
        &external_manifest,
        &BTreeMap::from([(
            SemanticArtifactRole::FastVector,
            b"externally-linked".to_vec(),
        )]),
    );
    let external_artifact_path = external_manifest
        .generation_dir(external_link_temp.path())
        .unwrap()
        .join(&external_manifest.artifacts[0].relative_path);
    fs::hard_link(
        &external_artifact_path,
        external_link_target.path().join("alias.fsvi"),
    )
    .unwrap();
    assert!(matches!(
        external_manifest.write_immutable(external_link_temp.path()),
        Err(SemanticGenerationError::InvalidManifest {
            class: SemanticManifestInvariantClass::Path,
            ..
        })
    ));

    let swap_temp = tempfile::tempdir().unwrap();
    let selected_path = swap_temp.path().join("selected.fsvi");
    fs::write(&selected_path, b"owner-a").unwrap();
    let selected_file = fs::File::open(&selected_path).unwrap();
    let selected_metadata = selected_file.metadata().unwrap();
    let selected_identity = portable_file_identity(&selected_file, &selected_metadata).unwrap();
    verify_path_still_names_open_file(&selected_path, selected_identity).unwrap();
    fs::rename(&selected_path, swap_temp.path().join("displaced.fsvi")).unwrap();
    fs::write(&selected_path, b"owner-b").unwrap();
    assert!(
        verify_path_still_names_open_file(&selected_path, selected_identity).is_err(),
        "post-open path replacement must not retain the prior verified identity"
    );
}

#[cfg(windows)]
#[test]
fn accepted_windows_publication_uses_write_through_replace_and_no_clobber() {
    let temp = tempfile::tempdir().unwrap();
    let final_path = temp.path().join("current.json");
    fs::write(&final_path, b"old-pointer").unwrap();

    let mut replacement = tempfile::Builder::new()
        .prefix(".current.write-through.")
        .tempfile_in(temp.path())
        .unwrap();
    replacement.write_all(b"new-pointer").unwrap();
    replacement.as_file().sync_all().unwrap();
    let persisted = persist_named_temp(replacement, &final_path, true).unwrap();
    persisted.sync_all().unwrap();
    assert_eq!(fs::read(&final_path).unwrap(), b"new-pointer");

    let mut conflicting = tempfile::Builder::new()
        .prefix(".manifest.no-clobber.")
        .tempfile_in(temp.path())
        .unwrap();
    conflicting.write_all(b"conflicting-manifest").unwrap();
    conflicting.as_file().sync_all().unwrap();
    let error = persist_named_temp(conflicting, &final_path, false)
        .expect_err("no-clobber publication must preserve an existing final");
    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    assert_eq!(fs::read(&final_path).unwrap(), b"new-pointer");
}

#[test]
fn accepted_two_stage_schema_dispatch_classifies_future_envelopes_as_upgrade_required() {
    let pointer = serde_json::json!({
        "schema_version": u64::from(SEMANTIC_CURRENT_POINTER_SCHEMA_VERSION) + 1,
        "generation_id": "gen-future-pointer",
        "manifest_sha256": test_sha256("future-pointer"),
        "selection_epoch": 1,
        "selected_at_ms": 1_700_000_000_400_i64,
        "future_selection_epoch": 7,
    });
    let error = parse_current_pointer_bytes(&serde_json::to_vec(&pointer).unwrap())
        .expect_err("future pointer with added fields must dispatch before strict V1 parsing");
    assert!(matches!(
        &error,
        SemanticGenerationError::UnsupportedPointerSchema { .. }
    ));
    assert_eq!(
        error.recovery_action(),
        SemanticGenerationRecoveryAction::UpgradeRequired
    );

    let corpus = test_corpus_identity();
    let artifact = complete_generation_artifact(
        SemanticArtifactRole::FastVector,
        "fast/primary.fsvi",
        b"future-schema-vector",
        &corpus,
    );
    let manifest = test_generation_manifest(SemanticGenerationTopology::FastOnly, vec![artifact]);
    let mut future_manifest = serde_json::to_value(&manifest).unwrap();
    future_manifest["schema_version"] =
        serde_json::json!(u64::from(SEMANTIC_SHARDED_GENERATION_MANIFEST_SCHEMA_VERSION) + 1);
    future_manifest.as_object_mut().unwrap().insert(
        "future_provenance".to_owned(),
        serde_json::json!({"epoch": 2}),
    );
    let error = parse_generation_manifest_bytes(
        &serde_json::to_vec(&future_manifest).unwrap(),
        &manifest.generation_id,
    )
    .expect_err("future manifest with added fields must dispatch before strict V1 parsing");
    assert!(matches!(
        &error,
        SemanticGenerationError::UnsupportedManifestSchema { .. }
    ));
    assert_eq!(
        error.recovery_action(),
        SemanticGenerationRecoveryAction::UpgradeRequired
    );

    for (component, supported) in [
        ("space", u64::from(EMBEDDING_SPACE_IDENTITY_SCHEMA_V1)),
        (
            "producer",
            u64::from(EMBEDDING_PRODUCER_ATTESTATION_SCHEMA_V1),
        ),
        ("input", u64::from(EMBEDDING_INPUT_CONTRACT_SCHEMA_V1)),
        ("storage", u64::from(VECTOR_STORAGE_IDENTITY_SCHEMA_V1)),
    ] {
        let mut future_identity = serde_json::to_value(&manifest).unwrap();
        future_identity["artifacts"][0]["embedding_identity"]["identity"][component]["schema_version"] =
            serde_json::json!(supported + 1);
        future_identity["artifacts"][0]["embedding_identity"]["identity"][component]
            .as_object_mut()
            .unwrap()
            .insert(
                "future_component_contract".to_owned(),
                serde_json::json!({"epoch": 2}),
            );
        let error = parse_generation_manifest_bytes(
            &serde_json::to_vec(&future_identity).unwrap(),
            &manifest.generation_id,
        )
        .expect_err("future embedding identity must classify as upgrade-required");
        assert!(matches!(
            &error,
            SemanticGenerationError::UnsupportedEmbeddingIdentitySchema {
                component: actual,
                ..
            } if actual == component
        ));
        assert_eq!(
            error.recovery_action(),
            SemanticGenerationRecoveryAction::UpgradeRequired
        );
    }

    let mut current_unknown = serde_json::to_value(&manifest).unwrap();
    current_unknown
        .as_object_mut()
        .unwrap()
        .insert("unversioned_unknown".to_owned(), serde_json::json!(true));
    assert!(matches!(
        parse_generation_manifest_bytes(
            &serde_json::to_vec(&current_unknown).unwrap(),
            &manifest.generation_id,
        ),
        Err(SemanticGenerationError::ManifestParse { .. })
    ));
}

#[test]
fn accepted_disk_resolution_is_exhaustive_with_robot_and_human_goldens() {
    // Rich step-by-step remediation plans remain the ds7uy.4 surface.
    // This minimal contract still has a distinct, stable state/reason/action
    // for every authoritative on-disk condition so callers never infer
    // recovery from free-form text.
    let absent = tempfile::tempdir().unwrap();
    assert_eq!(
        resolve_semantic_generation_disk_state(absent.path()).unwrap(),
        SemanticGenerationDiskStateV1::Absent
    );
    assert_eq!(
        serde_json::to_string_pretty(&SemanticGenerationDiskStateV1::Absent.robot_projection())
            .unwrap(),
        r#"{
  "schema_version": 1,
  "state": {
    "state": "absent"
  },
  "reason_code": "semantic_generation_absent",
  "recovery_action": "provision_and_index"
}"#
    );

    let legacy_root = tempfile::tempdir().unwrap();
    let legacy_vector_root = semantic_vector_root(legacy_root.path());
    fs::create_dir_all(&legacy_vector_root).unwrap();
    fs::write(
        legacy_vector_root.join("semantic_manifest.json"),
        b"legacy-ledger",
    )
    .unwrap();
    fs::write(legacy_vector_root.join("vector.fast.idx"), b"legacy-vector").unwrap();
    let legacy = resolve_semantic_generation_disk_state(legacy_root.path()).unwrap();
    assert_eq!(
        legacy,
        SemanticGenerationDiskStateV1::LegacyUnidentified { artifact_count: 2 }
    );
    assert_eq!(
        serde_json::to_string_pretty(&legacy.robot_projection()).unwrap(),
        r#"{
  "schema_version": 1,
  "state": {
    "state": "legacy_unidentified",
    "artifact_count": 2
  },
  "reason_code": "semantic_generation_legacy_unidentified",
  "recovery_action": "reindex_required"
}"#
    );

    let pointer_missing_root = tempfile::tempdir().unwrap();
    let pointer_missing_vector_root = semantic_vector_root(pointer_missing_root.path());
    fs::create_dir_all(
        pointer_missing_vector_root
            .join(SEMANTIC_GENERATIONS_DIRNAME)
            .join("gen-unselected"),
    )
    .unwrap();
    fs::write(
        pointer_missing_vector_root.join("vector.fast.idx"),
        b"legacy-vector",
    )
    .unwrap();
    let pointer_missing =
        resolve_semantic_generation_disk_state(pointer_missing_root.path()).unwrap();
    assert_eq!(
        pointer_missing,
        SemanticGenerationDiskStateV1::PointerMissingWithGenerationDirectories {
            generation_directory_count: 1,
            legacy_artifact_count: 1,
        }
    );
    assert_eq!(
        serde_json::to_string_pretty(&pointer_missing.robot_projection()).unwrap(),
        r#"{
  "schema_version": 1,
  "state": {
    "state": "pointer_missing_with_generation_directories",
    "generation_directory_count": 1,
    "legacy_artifact_count": 1
  },
  "reason_code": "semantic_pointer_missing_with_generation_directories",
  "recovery_action": "rollback_or_reindex"
}"#
    );

    let building_root = tempfile::tempdir().unwrap();
    let building_vector_root = semantic_vector_root(building_root.path());
    fs::create_dir_all(
        building_vector_root
            .join(SEMANTIC_STAGING_DIRNAME)
            .join("build-in-progress"),
    )
    .unwrap();
    fs::create_dir_all(
        building_vector_root
            .join(SEMANTIC_GENERATIONS_DIRNAME)
            .join("gen-unselected"),
    )
    .unwrap();
    fs::write(
        building_vector_root.join("vector.fast.idx"),
        b"legacy-vector",
    )
    .unwrap();
    let building = resolve_semantic_generation_disk_state(building_root.path()).unwrap();
    assert_eq!(
        building,
        SemanticGenerationDiskStateV1::Building {
            build_count: 1,
            unselected_generation_directory_count: 1,
            legacy_artifact_count: 1,
        }
    );
    assert_eq!(
        serde_json::to_string_pretty(&building.robot_projection()).unwrap(),
        r#"{
  "schema_version": 1,
  "state": {
    "state": "building",
    "build_count": 1,
    "unselected_generation_directory_count": 1,
    "legacy_artifact_count": 1
  },
  "reason_code": "semantic_generation_building",
  "recovery_action": "wait_for_build"
}"#
    );

    let interrupted_root = tempfile::tempdir().unwrap();
    let interrupted_vector_root = semantic_vector_root(interrupted_root.path());
    fs::create_dir_all(
        interrupted_vector_root
            .join(SEMANTIC_STAGING_DIRNAME)
            .join("build-in-progress"),
    )
    .unwrap();
    fs::create_dir_all(
        interrupted_vector_root
            .join(SEMANTIC_GENERATIONS_DIRNAME)
            .join("gen-unselected"),
    )
    .unwrap();
    fs::write(
        interrupted_vector_root.join("vector.fast.idx"),
        b"legacy-vector",
    )
    .unwrap();
    fs::write(
        interrupted_vector_root.join(".current.publish.A1b2C3"),
        b"torn-pointer-publication",
    )
    .unwrap();
    let interrupted = resolve_semantic_generation_disk_state(interrupted_root.path()).unwrap();
    assert_eq!(
        interrupted,
        SemanticGenerationDiskStateV1::InterruptedPublish {
            temporary_entry_count: 1,
            build_count: 1,
            unselected_generation_directory_count: 1,
            legacy_artifact_count: 1,
        }
    );
    assert_eq!(
        serde_json::to_string_pretty(&interrupted.robot_projection()).unwrap(),
        r#"{
  "schema_version": 1,
  "state": {
    "state": "interrupted_publish",
    "temporary_entry_count": 1,
    "build_count": 1,
    "unselected_generation_directory_count": 1,
    "legacy_artifact_count": 1
  },
  "reason_code": "semantic_pointer_publish_interrupted",
  "recovery_action": "retry_publish"
}"#
    );

    assert!(is_private_pointer_publish_temp_name(OsStr::new(
        ".current.publish.A1b2C3"
    )));
    for lookalike in [
        ".current.publish.",
        ".current.publish.contains-dash",
        ".current.publish.contains.dot",
        ".current.publish.non_ascii_é",
        "current.publish.A1b2C3",
    ] {
        assert!(!is_private_pointer_publish_temp_name(OsStr::new(lookalike)));
    }
    let directory_lookalike_root = tempfile::tempdir().unwrap();
    fs::create_dir_all(
        semantic_vector_root(directory_lookalike_root.path()).join(".current.publish.A1b2C3"),
    )
    .unwrap();
    assert_eq!(
        resolve_semantic_generation_disk_state(directory_lookalike_root.path()).unwrap(),
        SemanticGenerationDiskStateV1::LegacyUnidentified { artifact_count: 1 }
    );
    let malformed_generation_root = tempfile::tempdir().unwrap();
    let malformed_generations = semantic_generations_root(malformed_generation_root.path());
    fs::create_dir_all(&malformed_generations).unwrap();
    fs::write(
        malformed_generations.join("not-a-generation-directory"),
        b"untrusted-entry",
    )
    .unwrap();
    assert!(matches!(
        resolve_semantic_generation_disk_state(malformed_generation_root.path()),
        Err(SemanticGenerationError::InvalidPointer { reason })
            if reason == "semantic generations root contains a non-directory entry"
    ));

    let ready_digest = test_sha256("ready-manifest");
    let ready = SemanticGenerationDiskStateV1::Ready {
        generation_id: "gen-ready-1234567890abcdef".to_owned(),
        manifest_sha256: ready_digest.clone(),
        selection_epoch: 7,
        selected_at_ms: 1_700_000_000_700,
        requested_topology: SemanticGenerationTopology::FullProgressive,
        realized_topology: SemanticGenerationTopology::FastOnly,
    };
    assert_eq!(
        serde_json::to_string_pretty(&ready.robot_projection()).unwrap(),
        format!(
            r#"{{
  "schema_version": 1,
  "state": {{
    "state": "ready",
    "generation_id": "gen-ready-1234567890abcdef",
    "manifest_sha256": "{ready_digest}",
    "selection_epoch": 7,
    "selected_at_ms": 1700000000700,
    "requested_topology": "full_progressive",
    "realized_topology": "fast_only"
  }},
  "reason_code": null,
  "recovery_action": null
}}"#
        )
    );
    assert_eq!(
        SemanticGenerationDiskStateV1::Absent.to_string(),
        "no semantic generation exists; provision an embedder and build the initial index"
    );
    assert_eq!(
        legacy.to_string(),
        "legacy semantic artifacts without authoritative identity: 2"
    );
    assert_eq!(
        pointer_missing.to_string(),
        "current semantic pointer is missing; unselected generation candidate directory count: 1; legacy artifact count: 1; validate a candidate fully before rollback, otherwise reindex"
    );
    assert_eq!(
        building.to_string(),
        "staged semantic build count: 1; unselected generation candidate directory count: 1; unselected legacy artifact count: 1"
    );
    assert_eq!(
        interrupted.to_string(),
        "interrupted pointer publication temp count: 1; staged build count: 1; unselected generation candidate directory count: 1; legacy artifact count: 1"
    );
    let ready_summary = ready.to_string();
    assert!(ready_summary.contains("gen-ready-12"));
    assert!(!ready_summary.contains(&ready_digest));
}

#[test]
fn accepted_errors_are_typed_redacted_and_do_not_misclassify_read_io_as_publish_retry() {
    let expected = "a".repeat(64);
    let actual = "b".repeat(64);
    let digest_error = SemanticGenerationError::ManifestDigestMismatch {
        expected: expected.clone(),
        actual: actual.clone(),
    };
    let rendered = digest_error.to_string();
    assert!(!rendered.contains(&expected));
    assert!(!rendered.contains(&actual));
    assert!(rendered.contains(&expected[..12]));
    assert_eq!(
        SemanticGenerationError::ArtifactIo {
            role: SemanticArtifactRole::FastVector,
            source: "/private/path".to_owned(),
        }
        .recovery_action(),
        SemanticGenerationRecoveryAction::RollbackOrReindex
    );

    let corpus = test_corpus_identity();
    let mut partial = test_generation_artifact(
        SemanticArtifactRole::QualityVector,
        "quality/partial.fsvi",
        b"partial",
        &corpus,
        1,
    );
    partial.covered_content_sha256 = corpus.content_sha256.clone();
    partial.validation.covered_content_sha256 = corpus.content_sha256.clone();
    let error = test_generation_manifest(SemanticGenerationTopology::QualityOnly, vec![partial])
        .validate()
        .unwrap_err();
    assert!(matches!(
        error,
        SemanticGenerationError::InvalidManifest {
            class: SemanticManifestInvariantClass::PartialCoverage,
            ..
        }
    ));

    let mut building = test_generation_manifest(
        SemanticGenerationTopology::FastOnly,
        vec![complete_generation_artifact(
            SemanticArtifactRole::FastVector,
            "fast/primary.fsvi",
            b"building",
            &corpus,
        )],
    );
    building.status = SemanticGenerationStatus::Building;
    let error = building.validate().unwrap_err();
    assert_eq!(error.reason_code(), "semantic_generation_building");
    assert_eq!(
        error.recovery_action(),
        SemanticGenerationRecoveryAction::WaitForBuild
    );
}
