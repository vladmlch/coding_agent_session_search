use super::*;

// ── Superseded immutable generation manifest and pointer ───────────

fn mutated_generation(
    base: &SemanticGenerationManifestV1,
    mutate: impl FnOnce(&mut SemanticGenerationManifestV1),
) -> SemanticGenerationManifestV1 {
    let mut candidate = base.clone();
    mutate(&mut candidate);
    candidate
}

#[test]
fn immutable_generation_canonical_serialization_and_authoritative_fields_are_hashed() {
    let corpus = test_corpus_identity();
    let artifact = test_generation_artifact(
        SemanticArtifactRole::FastVector,
        "fast/vector.fsvi",
        b"fast-vector",
        &corpus,
    );
    let base = test_generation_manifest(SemanticGenerationTopology::FastOnly, vec![artifact]);
    base.validate().unwrap();
    let canonical = base.canonical_bytes().unwrap();
    assert_eq!(
        canonical,
        base.canonical_bytes().unwrap(),
        "canonical serialization must be byte deterministic"
    );
    let canonical_text = std::str::from_utf8(&canonical).unwrap();
    let ordered_root_keys = [
        "\"schema_version\"",
        "\"generation_id\"",
        "\"build_id\"",
        "\"request_id\"",
        "\"previous_generation_id\"",
        "\"created_at_ms\"",
        "\"completed_at_ms\"",
        "\"published_at_ms\"",
        "\"requested_topology\"",
        "\"realized_topology\"",
        "\"status\"",
        "\"corpus\"",
        "\"producer\"",
        "\"artifacts\"",
    ];
    let positions = ordered_root_keys.map(|key| canonical_text.find(key).expect("canonical key"));
    assert!(
        positions.windows(2).all(|window| window[0] < window[1]),
        "canonical root fields must remain in schema order: {positions:?}"
    );
    let ordered_embedding_keys = [
        "\"embedder_id\"",
        "\"embedder_revision\"",
        "\"dimension\"",
        "\"model_manifest_sha256\"",
        "\"model_files_sha256\"",
        "\"input_contract_sha256\"",
    ];
    let positions =
        ordered_embedding_keys.map(|key| canonical_text.find(key).expect("embedding key"));
    assert!(
        positions.windows(2).all(|window| window[0] < window[1]),
        "canonical embedding identity fields must remain in schema order: {positions:?}"
    );
    let decoded: SemanticGenerationManifestV1 = serde_json::from_slice(&canonical).unwrap();
    assert_eq!(decoded, base);

    let mutations: Vec<(&str, SemanticGenerationManifestV1)> = vec![
        (
            "schema_version",
            mutated_generation(&base, |value| value.schema_version += 1),
        ),
        (
            "generation_id",
            mutated_generation(&base, |value| value.generation_id.push('x')),
        ),
        (
            "build_id",
            mutated_generation(&base, |value| value.build_id.push('x')),
        ),
        (
            "request_id",
            mutated_generation(&base, |value| value.request_id = None),
        ),
        (
            "previous_generation_id",
            mutated_generation(&base, |value| value.previous_generation_id = None),
        ),
        (
            "created_at_ms",
            mutated_generation(&base, |value| value.created_at_ms += 1),
        ),
        (
            "completed_at_ms",
            mutated_generation(&base, |value| value.completed_at_ms += 1),
        ),
        (
            "published_at_ms",
            mutated_generation(&base, |value| value.published_at_ms += 1),
        ),
        (
            "requested_topology",
            mutated_generation(&base, |value| {
                value.requested_topology = SemanticGenerationTopology::FastOnly;
            }),
        ),
        (
            "realized_topology",
            mutated_generation(&base, |value| {
                value.realized_topology = SemanticGenerationTopology::QualityOnly;
            }),
        ),
        (
            "database_uuid",
            mutated_generation(&base, |value| value.corpus.database_uuid.push('x')),
        ),
        (
            "source_change_sequence",
            mutated_generation(&base, |value| value.corpus.source_change_sequence += 1),
        ),
        (
            "schema_contract_sha256",
            mutated_generation(&base, |value| {
                value.corpus.schema_contract_sha256 = test_sha256("changed-schema");
            }),
        ),
        (
            "content_selection_sha256",
            mutated_generation(&base, |value| {
                value.corpus.content_selection_sha256 = test_sha256("changed-selection");
            }),
        ),
        (
            "canonicalization_sha256",
            mutated_generation(&base, |value| {
                value.corpus.canonicalization_sha256 = test_sha256("changed-canonicalization");
            }),
        ),
        (
            "chunking_sha256",
            mutated_generation(&base, |value| {
                value.corpus.chunking_sha256 = test_sha256("changed-chunking");
            }),
        ),
        (
            "document_id_contract_sha256",
            mutated_generation(&base, |value| {
                value.corpus.document_id_contract_sha256 = test_sha256("changed-document-id");
            }),
        ),
        (
            "config_sha256",
            mutated_generation(&base, |value| {
                value.corpus.config_sha256 = test_sha256("changed-config");
            }),
        ),
        (
            "corpus_sha256",
            mutated_generation(&base, |value| {
                value.corpus.corpus_sha256 = test_sha256("changed-corpus");
            }),
        ),
        (
            "ordered_live_docset_sha256",
            mutated_generation(&base, |value| {
                value.corpus.ordered_live_docset_sha256 = test_sha256("changed-docset");
            }),
        ),
        (
            "content_sha256",
            mutated_generation(&base, |value| {
                value.corpus.content_sha256 = test_sha256("changed-content");
            }),
        ),
        (
            "corpus_document_count",
            mutated_generation(&base, |value| value.corpus.document_count += 1),
        ),
        (
            "cass_git_commit",
            mutated_generation(&base, |value| value.producer.cass_git_commit.push('x')),
        ),
        (
            "frankensearch_git_commit",
            mutated_generation(&base, |value| {
                value.producer.frankensearch_git_commit.push('x');
            }),
        ),
        (
            "cargo_lock_sha256",
            mutated_generation(&base, |value| {
                value.producer.cargo_lock_sha256 = test_sha256("changed-lock");
            }),
        ),
        (
            "rust_toolchain",
            mutated_generation(&base, |value| value.producer.rust_toolchain.push('x')),
        ),
        (
            "enabled_features",
            mutated_generation(&base, |value| {
                value.producer.enabled_features.push("zstd".to_owned());
            }),
        ),
        (
            "build_profile",
            mutated_generation(&base, |value| value.producer.build_profile.push('x')),
        ),
        (
            "binary_elf_sha256",
            mutated_generation(&base, |value| {
                value.producer.binary_elf_sha256 = test_sha256("changed-binary");
            }),
        ),
        (
            "document_count",
            mutated_generation(&base, |value| value.document_count += 1),
        ),
        (
            "vector_count",
            mutated_generation(&base, |value| value.vector_count += 1),
        ),
        (
            "live_vector_count",
            mutated_generation(&base, |value| value.live_vector_count -= 1),
        ),
        (
            "tombstone_count",
            mutated_generation(&base, |value| value.tombstone_count += 1),
        ),
        (
            "wal_entry_count",
            mutated_generation(&base, |value| value.wal_entry_count += 1),
        ),
        (
            "artifact_role",
            mutated_generation(&base, |value| {
                value.artifacts[0].role = SemanticArtifactRole::QualityVector;
            }),
        ),
        (
            "artifact_path",
            mutated_generation(&base, |value| {
                value.artifacts[0].relative_path.push('x');
            }),
        ),
        (
            "artifact_sha256",
            mutated_generation(&base, |value| {
                value.artifacts[0].artifact_sha256 = test_sha256("changed-artifact");
            }),
        ),
        (
            "artifact_size",
            mutated_generation(&base, |value| value.artifacts[0].size_bytes += 1),
        ),
        (
            "storage_format",
            mutated_generation(&base, |value| {
                value.artifacts[0].storage_format.push('x');
            }),
        ),
        (
            "storage_format_version",
            mutated_generation(&base, |value| {
                value.artifacts[0].storage_format_version += 1;
            }),
        ),
        (
            "quantization",
            mutated_generation(&base, |value| {
                value.artifacts[0].quantization.push('x');
            }),
        ),
        (
            "embedder_id",
            mutated_generation(&base, |value| {
                value.artifacts[0].embedding.embedder_id.push('x');
            }),
        ),
        (
            "embedder_revision",
            mutated_generation(&base, |value| {
                value.artifacts[0].embedding.embedder_revision.push('x');
            }),
        ),
        (
            "dimension",
            mutated_generation(&base, |value| {
                value.artifacts[0].embedding.dimension += 1;
            }),
        ),
        (
            "model_manifest_sha256",
            mutated_generation(&base, |value| {
                value.artifacts[0].embedding.model_manifest_sha256 =
                    test_sha256("changed-model-manifest");
            }),
        ),
        (
            "model_files_sha256",
            mutated_generation(&base, |value| {
                value.artifacts[0].embedding.model_files_sha256 =
                    test_sha256("changed-model-files");
            }),
        ),
        (
            "input_contract_sha256",
            mutated_generation(&base, |value| {
                value.artifacts[0].embedding.input_contract_sha256 = test_sha256("changed-input");
            }),
        ),
        (
            "artifact_document_count",
            mutated_generation(&base, |value| value.artifacts[0].document_count += 1),
        ),
        (
            "artifact_vector_count",
            mutated_generation(&base, |value| value.artifacts[0].vector_count += 1),
        ),
        (
            "artifact_live_count",
            mutated_generation(&base, |value| value.artifacts[0].live_vector_count -= 1),
        ),
        (
            "artifact_tombstone_count",
            mutated_generation(&base, |value| value.artifacts[0].tombstone_count += 1),
        ),
        (
            "artifact_wal_count",
            mutated_generation(&base, |value| value.artifacts[0].wal_entry_count += 1),
        ),
        (
            "coverage_numerator",
            mutated_generation(&base, |value| value.artifacts[0].coverage_numerator -= 1),
        ),
        (
            "coverage_denominator",
            mutated_generation(&base, |value| value.artifacts[0].coverage_denominator += 1),
        ),
        (
            "artifact_generation_sha256",
            mutated_generation(&base, |value| {
                value.artifacts[0].generation_sha256 = test_sha256("changed-generation");
            }),
        ),
        (
            "artifact_docset_sha256",
            mutated_generation(&base, |value| {
                value.artifacts[0].ordered_live_docset_sha256 =
                    test_sha256("changed-artifact-docset");
            }),
        ),
        (
            "artifact_content_sha256",
            mutated_generation(&base, |value| {
                value.artifacts[0].content_sha256 = test_sha256("changed-artifact-content");
            }),
        ),
        (
            "validator_id",
            mutated_generation(&base, |value| {
                value.artifacts[0].validation.validator_id.push('x');
            }),
        ),
        (
            "validated_at_ms",
            mutated_generation(&base, |value| {
                value.artifacts[0].validation.validated_at_ms += 1;
            }),
        ),
        (
            "validation_artifact_sha256",
            mutated_generation(&base, |value| {
                value.artifacts[0].validation.artifact_sha256 =
                    test_sha256("changed-validation-artifact");
            }),
        ),
        (
            "validation_size",
            mutated_generation(&base, |value| {
                value.artifacts[0].validation.size_bytes += 1;
            }),
        ),
        (
            "validation_document_count",
            mutated_generation(&base, |value| {
                value.artifacts[0].validation.document_count += 1;
            }),
        ),
        (
            "validation_vector_count",
            mutated_generation(&base, |value| {
                value.artifacts[0].validation.vector_count += 1;
            }),
        ),
        (
            "validation_live_count",
            mutated_generation(&base, |value| {
                value.artifacts[0].validation.live_vector_count -= 1;
            }),
        ),
        (
            "validation_tombstone_count",
            mutated_generation(&base, |value| {
                value.artifacts[0].validation.tombstone_count += 1;
            }),
        ),
        (
            "validation_wal_count",
            mutated_generation(&base, |value| {
                value.artifacts[0].validation.wal_entry_count += 1;
            }),
        ),
        (
            "validation_generation_sha256",
            mutated_generation(&base, |value| {
                value.artifacts[0].validation.generation_sha256 =
                    test_sha256("changed-validation-generation");
            }),
        ),
        (
            "validation_docset_sha256",
            mutated_generation(&base, |value| {
                value.artifacts[0].validation.ordered_live_docset_sha256 =
                    test_sha256("changed-validation-docset");
            }),
        ),
        (
            "validation_content_sha256",
            mutated_generation(&base, |value| {
                value.artifacts[0].validation.content_sha256 =
                    test_sha256("changed-validation-content");
            }),
        ),
    ];
    let base_digest = base.sha256().unwrap();
    for (label, mutation) in mutations {
        assert_ne!(
            mutation.sha256().unwrap(),
            base_digest,
            "authoritative field {label} must affect canonical digest"
        );
    }

    let mut json = serde_json::to_value(&base).unwrap();
    json["status"] = serde_json::Value::String("building".to_owned());
    assert!(
        serde_json::from_value::<SemanticGenerationManifestV1>(json).is_err(),
        "mutable/building status must not decode as an immutable generation"
    );
}

#[test]
fn immutable_generation_validates_fast_quality_full_and_partial_topologies() {
    let corpus = test_corpus_identity();
    let fast = test_generation_artifact(
        SemanticArtifactRole::FastVector,
        "fast/vector.fsvi",
        b"fast-vector",
        &corpus,
    );
    let quality = test_generation_artifact(
        SemanticArtifactRole::QualityVector,
        "quality/vector.fsvi",
        b"quality-vector",
        &corpus,
    );

    test_generation_manifest(SemanticGenerationTopology::FastOnly, vec![fast.clone()])
        .validate()
        .unwrap();
    test_generation_manifest(
        SemanticGenerationTopology::QualityOnly,
        vec![quality.clone()],
    )
    .validate()
    .unwrap();
    test_generation_manifest(
        SemanticGenerationTopology::FullProgressive,
        vec![fast.clone(), quality.clone()],
    )
    .validate()
    .unwrap();

    let partial =
        test_generation_manifest(SemanticGenerationTopology::FastOnly, vec![fast.clone()]);
    assert_eq!(
        partial.requested_topology,
        SemanticGenerationTopology::FullProgressive
    );
    partial.validate().unwrap();

    let mut contradictory = partial.clone();
    contradictory.requested_topology = SemanticGenerationTopology::QualityOnly;
    assert!(matches!(
        contradictory.validate(),
        Err(SemanticGenerationError::InvalidManifest { .. })
    ));

    let mut missing = test_generation_manifest(
        SemanticGenerationTopology::FullProgressive,
        vec![fast.clone(), quality.clone()],
    );
    missing.artifacts.pop();
    missing.vector_count = fast.vector_count;
    missing.live_vector_count = fast.live_vector_count;
    assert!(matches!(
        missing.validate(),
        Err(SemanticGenerationError::InvalidManifest { .. })
    ));

    let duplicate = test_generation_manifest(
        SemanticGenerationTopology::FastOnly,
        vec![fast.clone(), fast],
    );
    assert!(matches!(
        duplicate.validate(),
        Err(SemanticGenerationError::InvalidManifest { .. })
    ));
}

#[test]
fn immutable_generation_rejects_count_evidence_ann_and_path_inconsistency() {
    let corpus = test_corpus_identity();
    let fast_bytes = b"fast-vector";
    let fast = test_generation_artifact(
        SemanticArtifactRole::FastVector,
        "fast/vector.fsvi",
        fast_bytes,
        &corpus,
    );
    let mut ann = test_generation_artifact(
        SemanticArtifactRole::FastAnn,
        "fast/vector.hnsw",
        b"fast-ann",
        &corpus,
    );
    let valid = test_generation_manifest(
        SemanticGenerationTopology::FastOnly,
        vec![fast.clone(), ann.clone()],
    );
    valid.validate().unwrap();

    let mut tombstoned = fast.clone();
    tombstoned.vector_count += 2;
    tombstoned.tombstone_count = 2;
    tombstoned.validation.vector_count = tombstoned.vector_count;
    tombstoned.validation.tombstone_count = tombstoned.tombstone_count;
    test_generation_manifest(SemanticGenerationTopology::FastOnly, vec![tombstoned])
        .validate()
        .unwrap();

    ann.embedding.dimension += 1;
    ann.validation.artifact_sha256 = ann.artifact_sha256.clone();
    let bad_ann = test_generation_manifest(
        SemanticGenerationTopology::FastOnly,
        vec![fast.clone(), ann],
    );
    assert!(matches!(
        bad_ann.validate(),
        Err(SemanticGenerationError::InvalidManifest { .. })
    ));

    let mut bad_evidence = valid.clone();
    bad_evidence.artifacts[0].validation.vector_count += 1;
    assert!(matches!(
        bad_evidence.validate(),
        Err(SemanticGenerationError::InvalidManifest { .. })
    ));

    let mut bad_count = valid.clone();
    bad_count.vector_count += 1;
    assert!(matches!(
        bad_count.validate(),
        Err(SemanticGenerationError::InvalidManifest { .. })
    ));

    for unsafe_path in [
        "/absolute/vector.fsvi",
        "../escape.fsvi",
        "fast/../escape.fsvi",
        "./fast/vector.fsvi",
        "fast\\vector.fsvi",
        "fast//vector.fsvi",
        "fast/vector.fsvi/",
        "fast/\0vector.fsvi",
        " manifest.fsvi",
        "manifest.json",
    ] {
        let mut candidate = test_generation_manifest(
            SemanticGenerationTopology::FastOnly,
            vec![test_generation_artifact(
                SemanticArtifactRole::FastVector,
                unsafe_path,
                fast_bytes,
                &corpus,
            )],
        );
        candidate.artifacts[0].relative_path = unsafe_path.to_owned();
        assert!(
            matches!(
                candidate.validate(),
                Err(SemanticGenerationError::InvalidManifest { .. })
            ),
            "unsafe path must fail closed: {unsafe_path}"
        );
    }
}

#[test]
fn immutable_generation_rejects_weak_provenance_and_embedding_identity() {
    let corpus = test_corpus_identity();
    let artifact = test_generation_artifact(
        SemanticArtifactRole::FastVector,
        "fast/vector.fsvi",
        b"fast-vector",
        &corpus,
    );
    let base =
        test_generation_manifest(SemanticGenerationTopology::FastOnly, vec![artifact.clone()]);

    let mut weak_commit = base.clone();
    weak_commit.producer.cass_git_commit = "main".to_owned();
    assert!(matches!(
        weak_commit.validate(),
        Err(SemanticGenerationError::InvalidManifest { .. })
    ));

    let mut unsorted_features = base.clone();
    unsorted_features.producer.enabled_features = vec!["semantic".to_owned(), "native".to_owned()];
    assert!(matches!(
        unsorted_features.validate(),
        Err(SemanticGenerationError::InvalidManifest { .. })
    ));

    let mut missing_model_identity = base.clone();
    missing_model_identity.artifacts[0]
        .embedding
        .model_files_sha256 = "unverified".to_owned();
    assert!(matches!(
        missing_model_identity.validate(),
        Err(SemanticGenerationError::InvalidManifest { .. })
    ));

    let mut impossible_dimension = base.clone();
    impossible_dimension.artifacts[0].embedding.dimension =
        MAX_EMBEDDING_DIMENSION.saturating_add(1);
    assert!(matches!(
        impossible_dimension.validate(),
        Err(SemanticGenerationError::InvalidManifest { .. })
    ));

    let mut wrong_format = base.clone();
    wrong_format.artifacts[0].storage_format = "hnsw".to_owned();
    assert!(matches!(
        wrong_format.validate(),
        Err(SemanticGenerationError::InvalidManifest { .. })
    ));

    let mut incomplete_live_set = artifact;
    incomplete_live_set.live_vector_count -= 1;
    incomplete_live_set.tombstone_count += 1;
    incomplete_live_set.validation.live_vector_count = incomplete_live_set.live_vector_count;
    incomplete_live_set.validation.tombstone_count = incomplete_live_set.tombstone_count;
    let incomplete = test_generation_manifest(
        SemanticGenerationTopology::FastOnly,
        vec![incomplete_live_set],
    );
    assert!(matches!(
        incomplete.validate(),
        Err(SemanticGenerationError::InvalidManifest { .. })
    ));
}

#[test]
fn immutable_generation_publish_and_restart_load_use_only_pointer_and_manifest() {
    let temp = tempfile::tempdir().unwrap();
    let corpus = test_corpus_identity();
    let fast_bytes = b"fast-vector".to_vec();
    let quality_bytes = b"quality-vector".to_vec();
    let fast = test_generation_artifact(
        SemanticArtifactRole::FastVector,
        "fast/vector.fsvi",
        &fast_bytes,
        &corpus,
    );
    let quality = test_generation_artifact(
        SemanticArtifactRole::QualityVector,
        "quality/vector.fsvi",
        &quality_bytes,
        &corpus,
    );
    let manifest = test_generation_manifest(
        SemanticGenerationTopology::FullProgressive,
        vec![fast, quality],
    );
    let contents = BTreeMap::from([
        (SemanticArtifactRole::FastVector, fast_bytes),
        (SemanticArtifactRole::QualityVector, quality_bytes),
    ]);
    write_generation_artifacts(temp.path(), &manifest, &contents);

    let digest = manifest.write_immutable(temp.path()).unwrap();
    assert_eq!(
        manifest.write_immutable(temp.path()).unwrap(),
        digest,
        "identical immutable write retry must be idempotent"
    );
    let pointer = SemanticCurrentPointerV1::for_manifest(&manifest).unwrap();
    assert_eq!(pointer.manifest_sha256, digest);
    pointer.publish_atomic(temp.path()).unwrap();

    let loaded = load_current_semantic_generation(temp.path(), Some(&manifest.corpus)).unwrap();
    assert_eq!(loaded.pointer, pointer);
    assert_eq!(loaded.manifest, manifest);
    assert_eq!(loaded.artifact_paths.len(), 2);
    assert!(
        !temp.path().join("vector_index/vector.fast.idx").exists(),
        "resolution must not depend on a conventional filename"
    );

    let mut conflicting = manifest.clone();
    conflicting.build_id.push_str("-different");
    assert!(matches!(
        conflicting.write_immutable(temp.path()),
        Err(SemanticGenerationError::ImmutableManifestConflict { .. })
    ));
}

#[test]
fn immutable_generation_resolution_distinguishes_missing_corrupt_future_and_stale() {
    let missing = tempfile::tempdir().unwrap();
    let error = load_current_semantic_generation(missing.path(), None).unwrap_err();
    assert!(matches!(error, SemanticGenerationError::MissingPointer));
    assert_eq!(
        error.recovery_action(),
        SemanticGenerationRecoveryAction::ReindexRequired
    );

    let corrupt = tempfile::tempdir().unwrap();
    let pointer_path = SemanticCurrentPointerV1::path(corrupt.path());
    fs::create_dir_all(pointer_path.parent().unwrap()).unwrap();
    fs::write(&pointer_path, b"not-json").unwrap();
    let error = load_current_semantic_generation(corrupt.path(), None).unwrap_err();
    assert!(matches!(
        error,
        SemanticGenerationError::PointerParse { .. }
    ));

    let future = tempfile::tempdir().unwrap();
    let pointer_path = SemanticCurrentPointerV1::path(future.path());
    fs::create_dir_all(pointer_path.parent().unwrap()).unwrap();
    let future_pointer = SemanticCurrentPointerV1 {
        schema_version: SEMANTIC_CURRENT_POINTER_SCHEMA_VERSION + 1,
        generation_id: "gen-future".to_owned(),
        manifest_sha256: test_sha256("future"),
        published_at_ms: 1,
    };
    fs::write(&pointer_path, serde_json::to_vec(&future_pointer).unwrap()).unwrap();
    let error = load_current_semantic_generation(future.path(), None).unwrap_err();
    assert!(matches!(
        error,
        SemanticGenerationError::UnsupportedPointerSchema { .. }
    ));
    assert_eq!(
        error.recovery_action(),
        SemanticGenerationRecoveryAction::UpgradeRequired
    );

    let stale = tempfile::tempdir().unwrap();
    let corpus = test_corpus_identity();
    let bytes = b"fast-vector".to_vec();
    let artifact = test_generation_artifact(
        SemanticArtifactRole::FastVector,
        "fast/vector.fsvi",
        &bytes,
        &corpus,
    );
    let manifest = test_generation_manifest(SemanticGenerationTopology::FastOnly, vec![artifact]);
    write_generation_artifacts(
        stale.path(),
        &manifest,
        &BTreeMap::from([(SemanticArtifactRole::FastVector, bytes)]),
    );
    manifest.write_immutable(stale.path()).unwrap();
    SemanticCurrentPointerV1::for_manifest(&manifest)
        .unwrap()
        .publish_atomic(stale.path())
        .unwrap();
    let mut expected = manifest.corpus.clone();
    expected.source_change_sequence += 1;
    expected.corpus_sha256 = test_sha256("newer-corpus");
    let error = load_current_semantic_generation(stale.path(), Some(&expected)).unwrap_err();
    assert!(matches!(error, SemanticGenerationError::StaleCorpus { .. }));
}

#[test]
fn immutable_generation_detects_manifest_artifact_hash_and_size_substitution() {
    let temp = tempfile::tempdir().unwrap();
    let corpus = test_corpus_identity();
    let bytes = b"fast-vector".to_vec();
    let artifact = test_generation_artifact(
        SemanticArtifactRole::FastVector,
        "fast/vector.fsvi",
        &bytes,
        &corpus,
    );
    let manifest = test_generation_manifest(SemanticGenerationTopology::FastOnly, vec![artifact]);
    write_generation_artifacts(
        temp.path(),
        &manifest,
        &BTreeMap::from([(SemanticArtifactRole::FastVector, bytes.clone())]),
    );
    manifest.write_immutable(temp.path()).unwrap();
    let pointer = SemanticCurrentPointerV1::for_manifest(&manifest).unwrap();
    pointer.publish_atomic(temp.path()).unwrap();

    let artifact_path = manifest
        .generation_dir(temp.path())
        .unwrap()
        .join("fast/vector.fsvi");
    fs::write(&artifact_path, b"same-length!").unwrap();
    let error = load_current_semantic_generation(temp.path(), None).unwrap_err();
    assert!(matches!(
        &error,
        SemanticGenerationError::ArtifactDigestMismatch { .. }
            | SemanticGenerationError::ArtifactSizeMismatch { .. }
    ));
    assert_eq!(
        error.recovery_action(),
        SemanticGenerationRecoveryAction::RollbackOrReindex
    );

    fs::write(&artifact_path, &bytes).unwrap();
    let manifest_path = manifest.manifest_path(temp.path()).unwrap();
    let mut manifest_bytes = fs::read(&manifest_path).unwrap();
    manifest_bytes.push(b' ');
    fs::write(&manifest_path, manifest_bytes).unwrap();
    let error = load_current_semantic_generation(temp.path(), None).unwrap_err();
    assert!(matches!(
        &error,
        SemanticGenerationError::ManifestDigestMismatch { .. }
    ));
    assert_eq!(
        error.recovery_action(),
        SemanticGenerationRecoveryAction::RollbackOrReindex
    );
}

#[test]
fn immutable_pointer_publish_faults_never_expose_a_mixed_generation() {
    let checkpoints = [
        (
            PointerPublishCheckpoint::TemporaryBytesWritten,
            "gen-20260726-old",
        ),
        (
            PointerPublishCheckpoint::TemporaryFileSynced,
            "gen-20260726-old",
        ),
        (
            PointerPublishCheckpoint::PointerReplaced,
            "gen-20260726-new",
        ),
        (
            PointerPublishCheckpoint::ParentDirectorySynced,
            "gen-20260726-new",
        ),
    ];

    for (fault, expected_generation_id) in checkpoints {
        let temp = tempfile::tempdir().unwrap();
        let corpus = test_corpus_identity();
        let old_bytes = b"old-vector".to_vec();
        let new_bytes = b"new-vector".to_vec();
        let old_artifact = test_generation_artifact(
            SemanticArtifactRole::FastVector,
            "fast/vector.fsvi",
            &old_bytes,
            &corpus,
        );
        let new_artifact = test_generation_artifact(
            SemanticArtifactRole::FastVector,
            "fast/vector.fsvi",
            &new_bytes,
            &corpus,
        );
        let mut old_manifest =
            test_generation_manifest(SemanticGenerationTopology::FastOnly, vec![old_artifact]);
        old_manifest.generation_id = "gen-20260726-old".to_owned();
        old_manifest.build_id = "build-old".to_owned();
        old_manifest.previous_generation_id = None;
        let mut new_manifest =
            test_generation_manifest(SemanticGenerationTopology::FastOnly, vec![new_artifact]);
        new_manifest.generation_id = "gen-20260726-new".to_owned();
        new_manifest.build_id = "build-new".to_owned();
        new_manifest.previous_generation_id = Some(old_manifest.generation_id.clone());
        new_manifest.created_at_ms += 1_000;
        new_manifest.completed_at_ms += 1_000;
        new_manifest.published_at_ms += 1_000;

        write_generation_artifacts(
            temp.path(),
            &old_manifest,
            &BTreeMap::from([(SemanticArtifactRole::FastVector, old_bytes.clone())]),
        );
        write_generation_artifacts(
            temp.path(),
            &new_manifest,
            &BTreeMap::from([(SemanticArtifactRole::FastVector, new_bytes.clone())]),
        );
        old_manifest.write_immutable(temp.path()).unwrap();
        new_manifest.write_immutable(temp.path()).unwrap();
        SemanticCurrentPointerV1::for_manifest(&old_manifest)
            .unwrap()
            .publish_atomic(temp.path())
            .unwrap();

        let new_pointer = SemanticCurrentPointerV1::for_manifest(&new_manifest).unwrap();
        let mut injected = false;
        let error = new_pointer
            .publish_atomic_with_hook(temp.path(), |checkpoint| {
                if checkpoint == fault {
                    injected = true;
                    return Err(SemanticGenerationError::PointerIo {
                        source: format!("injected failure after {fault:?}"),
                    });
                }
                Ok(())
            })
            .unwrap_err();
        assert!(injected, "fault checkpoint {fault:?} must be reached");
        assert!(matches!(error, SemanticGenerationError::PointerIo { .. }));

        let loaded = load_current_semantic_generation(temp.path(), Some(&corpus)).unwrap();
        assert_eq!(
            loaded.manifest.generation_id, expected_generation_id,
            "failure after {fault:?} must expose exactly the old or new sealed generation"
        );
        let expected_bytes = if expected_generation_id == old_manifest.generation_id {
            &old_bytes
        } else {
            &new_bytes
        };
        let selected_artifact = loaded
            .artifact_paths
            .get(&SemanticArtifactRole::FastVector)
            .unwrap();
        assert_eq!(
            fs::read(selected_artifact).unwrap(),
            *expected_bytes,
            "pointer and artifact bytes must always come from the same generation"
        );
    }
}

#[test]
fn immutable_generation_rejects_unknown_fields_missing_manifest_and_future_manifest() {
    let corpus = test_corpus_identity();
    let bytes = b"fast-vector".to_vec();
    let artifact = test_generation_artifact(
        SemanticArtifactRole::FastVector,
        "fast/vector.fsvi",
        &bytes,
        &corpus,
    );
    let manifest = test_generation_manifest(SemanticGenerationTopology::FastOnly, vec![artifact]);

    let mut manifest_json: serde_json::Value =
        serde_json::from_slice(&manifest.canonical_bytes().unwrap()).unwrap();
    manifest_json
        .as_object_mut()
        .unwrap()
        .insert("guessed_ready".to_owned(), serde_json::Value::Bool(true));
    assert!(
        serde_json::from_value::<SemanticGenerationManifestV1>(manifest_json).is_err(),
        "unknown manifest fields must fail closed"
    );

    let missing = tempfile::tempdir().unwrap();
    let pointer = SemanticCurrentPointerV1 {
        schema_version: SEMANTIC_CURRENT_POINTER_SCHEMA_VERSION,
        generation_id: manifest.generation_id.clone(),
        manifest_sha256: manifest.sha256().unwrap(),
        published_at_ms: manifest.published_at_ms,
    };
    let pointer_path = SemanticCurrentPointerV1::path(missing.path());
    fs::create_dir_all(pointer_path.parent().unwrap()).unwrap();
    fs::write(&pointer_path, pointer.canonical_bytes().unwrap()).unwrap();
    assert!(matches!(
        load_current_semantic_generation(missing.path(), None),
        Err(SemanticGenerationError::MissingManifest { .. })
    ));

    let future = tempfile::tempdir().unwrap();
    let mut future_manifest = manifest;
    future_manifest.schema_version = SEMANTIC_SHARDED_GENERATION_MANIFEST_SCHEMA_VERSION + 1;
    write_generation_artifacts(
        future.path(),
        &future_manifest,
        &BTreeMap::from([(SemanticArtifactRole::FastVector, bytes)]),
    );
    let manifest_path = future_manifest.manifest_path(future.path()).unwrap();
    let manifest_bytes = future_manifest.canonical_bytes().unwrap();
    fs::write(&manifest_path, &manifest_bytes).unwrap();
    let future_pointer = SemanticCurrentPointerV1 {
        schema_version: SEMANTIC_CURRENT_POINTER_SCHEMA_VERSION,
        generation_id: future_manifest.generation_id.clone(),
        manifest_sha256: sha256_hex(&manifest_bytes),
        published_at_ms: future_manifest.published_at_ms,
    };
    let pointer_path = SemanticCurrentPointerV1::path(future.path());
    fs::write(&pointer_path, future_pointer.canonical_bytes().unwrap()).unwrap();
    let error = load_current_semantic_generation(future.path(), None).unwrap_err();
    assert!(matches!(
        error,
        SemanticGenerationError::UnsupportedManifestSchema { .. }
    ));
    assert_eq!(
        error.recovery_action(),
        SemanticGenerationRecoveryAction::UpgradeRequired
    );
}

proptest! {
#[test]
fn immutable_generation_property_rejects_parent_alias_paths(
    directory in "[A-Za-z0-9_-]{1,64}",
    filename in "[A-Za-z0-9_-]{1,64}\\.fsvi",
) {
    let corpus = test_corpus_identity();
    let bytes = b"property-vector";
    let traversal = format!("{directory}/../{filename}");
    let artifact = test_generation_artifact(
        SemanticArtifactRole::FastVector,
        &traversal,
        bytes,
        &corpus,
    );
    let candidate =
        test_generation_manifest(SemanticGenerationTopology::FastOnly, vec![artifact]);
    prop_assert!(matches!(
        candidate.validate(),
        Err(SemanticGenerationError::InvalidManifest { .. })
    ), "parent traversal path unexpectedly validated");
}

#[test]
fn immutable_generation_property_digest_is_stable_and_generation_sensitive(
    suffix in "[A-Za-z0-9_-]{1,32}",
) {
    let corpus = test_corpus_identity();
    let artifact = test_generation_artifact(
        SemanticArtifactRole::FastVector,
        "fast/vector.fsvi",
        b"property-vector",
        &corpus,
    );
    let base =
        test_generation_manifest(SemanticGenerationTopology::FastOnly, vec![artifact]);
    let mut changed = base.clone();
    changed.generation_id = format!("gen-{suffix}");
    prop_assert_eq!(base.sha256().unwrap(), base.sha256().unwrap());
    prop_assert_ne!(base.sha256().unwrap(), changed.sha256().unwrap());
}
}

#[cfg(unix)]
#[test]
fn immutable_generation_rejects_symlink_escape() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let outside_artifact = outside.path().join("outside.fsvi");
    fs::write(&outside_artifact, b"fast-vector").unwrap();
    let corpus = test_corpus_identity();
    let artifact = test_generation_artifact(
        SemanticArtifactRole::FastVector,
        "fast/vector.fsvi",
        b"fast-vector",
        &corpus,
    );
    let manifest = test_generation_manifest(SemanticGenerationTopology::FastOnly, vec![artifact]);
    let generation_dir = manifest.generation_dir(temp.path()).unwrap();
    fs::create_dir_all(generation_dir.join("fast")).unwrap();
    symlink(&outside_artifact, generation_dir.join("fast/vector.fsvi")).unwrap();
    manifest.write_immutable(temp.path()).unwrap();
    let error = SemanticCurrentPointerV1::for_manifest(&manifest)
        .unwrap()
        .publish_atomic(temp.path())
        .unwrap_err();
    assert!(matches!(error, SemanticGenerationError::ArtifactIo { .. }));

    let pointer_escape = tempfile::tempdir().unwrap();
    let outside_pointer = outside.path().join("outside-current.json");
    fs::write(
        &outside_pointer,
        SemanticCurrentPointerV1::for_manifest(&manifest)
            .unwrap()
            .canonical_bytes()
            .unwrap(),
    )
    .unwrap();
    let pointer_path = SemanticCurrentPointerV1::path(pointer_escape.path());
    fs::create_dir_all(pointer_path.parent().unwrap()).unwrap();
    symlink(&outside_pointer, &pointer_path).unwrap();
    assert!(matches!(
        load_current_semantic_generation(pointer_escape.path(), None),
        Err(SemanticGenerationError::InvalidPointer { .. })
    ));
}
