use super::*;

#[test]
fn accepted_manifest_uses_frankensearch_identity_and_rejects_hash_controls() {
    use frankensearch::core::EmbeddingIdentityBundleV1;

    let corpus = test_corpus_identity();
    let mut artifact = complete_generation_artifact(
        SemanticArtifactRole::FastVector,
        "fast/primary.fsvi",
        b"fast-vector",
        &corpus,
    );
    let manifest =
        test_generation_manifest(SemanticGenerationTopology::FastOnly, vec![artifact.clone()]);
    manifest.validate().expect("canonical semantic identity");

    artifact.embedding_identity =
        EmbeddingIdentityBundleV1::explicit_test_model("hash-control", 256)
            .freeze()
            .expect("explicit hash control");
    artifact.artifact_format = artifact.embedding_identity.identity.storage.format.clone();
    let hash_control =
        test_generation_manifest(SemanticGenerationTopology::FastOnly, vec![artifact]);
    let error = hash_control
        .validate()
        .expect_err("hash control must fail closed");
    assert_eq!(error.reason_code(), "semantic_manifest_identity_invalid");

    let mut noncanonical = manifest;
    noncanonical.artifacts[0]
        .embedding_identity
        .identity
        .space
        .pooling
        .push_str("-drift");
    assert!(matches!(
        noncanonical.validate(),
        Err(SemanticGenerationError::InvalidManifest {
            class: SemanticManifestInvariantClass::Identity,
            ..
        })
    ));
}

#[test]
fn accepted_manifest_represents_partial_quality_coverage_and_across_tier_totals() {
    let corpus = test_corpus_identity();
    let fast = complete_generation_artifact(
        SemanticArtifactRole::FastVector,
        "fast/primary.fsvi",
        b"fast-vector",
        &corpus,
    );
    let quality = test_generation_artifact(
        SemanticArtifactRole::QualityVector,
        "quality/partial.fsvi",
        b"quality-vector",
        &corpus,
        1,
    );
    let manifest = test_generation_manifest(
        SemanticGenerationTopology::FullProgressive,
        vec![fast.clone(), quality.clone()],
    );
    manifest.validate().expect("partial quality is truthful");
    assert_eq!(manifest.selected_document_count, 3);
    assert_eq!(manifest.total_live_vector_count_across_tiers, 4);
    assert_eq!(manifest.total_vector_slot_count_across_tiers, 4);
    assert_eq!(quality.covered_document_count, 1);
    assert_eq!(quality.selected_document_count, 3);
    assert_ne!(
        quality.covered_live_docset_sha256,
        corpus.ordered_live_docset_sha256
    );

    let quality_only = test_generation_manifest(
        SemanticGenerationTopology::QualityOnly,
        vec![complete_generation_artifact(
            SemanticArtifactRole::QualityVector,
            "quality/primary.fsvi",
            b"quality-only",
            &corpus,
        )],
    );
    quality_only.validate().expect("quality-only topology");

    let mut dishonest = manifest.clone();
    dishonest.artifacts[1].covered_live_docset_sha256 = corpus.ordered_live_docset_sha256.clone();
    dishonest.artifacts[1].validation.covered_live_docset_sha256 =
        corpus.ordered_live_docset_sha256.clone();
    let error = dishonest
        .validate()
        .expect_err("partial tier cannot claim full docset identity");
    assert_eq!(
        error.reason_code(),
        "semantic_manifest_partial_coverage_invalid"
    );

    let mut wrong_total = manifest;
    wrong_total.total_live_vector_count_across_tiers += 1;
    assert_eq!(
        wrong_total.validate().unwrap_err().reason_code(),
        "semantic_manifest_count_invalid"
    );
}

#[test]
fn accepted_immutable_generation_rejects_declared_or_on_disk_wal_state() {
    let corpus = test_corpus_identity();
    let bytes = b"wal-free-vector".to_vec();
    let artifact = complete_generation_artifact(
        SemanticArtifactRole::FastVector,
        "fast/primary.fsvi",
        &bytes,
        &corpus,
    );
    let manifest = generation_with_id(
        "gen-wal-free",
        SemanticGenerationTopology::FastOnly,
        vec![artifact],
    );

    let mut declared_wal = manifest.clone();
    declared_wal.artifacts[0].wal_entry_count = 1;
    declared_wal.artifacts[0].validation.wal_entry_count = 1;
    let error = declared_wal
        .validate()
        .expect_err("sealed generation cannot declare live WAL entries");
    assert_eq!(error.reason_code(), "semantic_manifest_not_durably_sealed");

    let seal_temp = tempfile::tempdir().unwrap();
    write_generation_artifacts(
        seal_temp.path(),
        &manifest,
        &BTreeMap::from([(SemanticArtifactRole::FastVector, bytes.clone())]),
    );
    let artifact_path = manifest
        .generation_dir(seal_temp.path())
        .unwrap()
        .join(&manifest.artifacts[0].relative_path);
    fs::write(wal_path_for(&artifact_path), b"injected-sidecar").unwrap();
    assert!(matches!(
        manifest.write_immutable(seal_temp.path()),
        Err(SemanticGenerationError::ArtifactIo { .. })
    ));

    let load_temp = tempfile::tempdir().unwrap();
    seal_generation(
        load_temp.path(),
        &manifest,
        &BTreeMap::from([(SemanticArtifactRole::FastVector, bytes)]),
    );
    let pointer =
        SemanticCurrentPointerV1::for_manifest(&manifest, manifest.sealed_at_ms + 1, 1).unwrap();
    pointer
        .publish_atomic(load_temp.path(), &SemanticExpectedCurrentV1::Absent)
        .unwrap();
    let artifact_path = manifest
        .generation_dir(load_temp.path())
        .unwrap()
        .join(&manifest.artifacts[0].relative_path);
    fs::write(wal_path_for(&artifact_path), b"post-seal-sidecar").unwrap();
    assert!(matches!(
        load_current_semantic_generation(load_temp.path(), Some(&corpus)),
        Err(SemanticGenerationError::ArtifactIo { .. })
    ));
}

#[test]
fn accepted_ann_binding_covers_exact_base_bytes_storage_and_parameters() {
    let corpus = test_corpus_identity();
    let base = complete_generation_artifact(
        SemanticArtifactRole::FastVector,
        "fast/primary.fsvi",
        b"base-vector",
        &corpus,
    );
    let ann_template = complete_generation_artifact(
        SemanticArtifactRole::FastAnn,
        "fast/search.hnsw",
        b"ann-index",
        &corpus,
    );
    let mut base_with_wal = base.clone();
    base_with_wal.wal_entry_count = 7;
    base_with_wal.validation.wal_entry_count = 7;
    let ann_from_live_base = bind_ann_to_base(ann_template.clone(), &base_with_wal);
    assert_eq!(ann_from_live_base.wal_entry_count, 0);
    assert_eq!(ann_from_live_base.validation.wal_entry_count, 0);

    let ann = bind_ann_to_base(ann_template, &base);
    let manifest = test_generation_manifest(
        SemanticGenerationTopology::FastOnly,
        vec![base.clone(), ann],
    );
    manifest.validate().expect("exact ANN base binding");

    for mutation in 0..3 {
        let mut invalid = manifest.clone();
        let binding = invalid.artifacts[1].ann_base.as_mut().expect("ANN binding");
        match mutation {
            0 => binding.base_artifact_sha256 = test_sha256("wrong-base-bytes"),
            1 => binding.base_storage_fingerprint = test_sha256("wrong-base-storage"),
            _ => binding.base_artifact_parameters_sha256 = test_sha256("wrong-base-parameters"),
        }
        let error = invalid.validate().expect_err("ANN drift must fail");
        assert_eq!(error.reason_code(), "semantic_manifest_ann_binding_invalid");
    }

    let mut live_ann_wal = manifest.clone();
    live_ann_wal.artifacts[1].wal_entry_count = 1;
    live_ann_wal.artifacts[1].validation.wal_entry_count = 1;
    let error = live_ann_wal
        .validate()
        .expect_err("immutable ANN artifact must reject a live WAL");
    assert_eq!(error.reason_code(), "semantic_manifest_not_durably_sealed");

    let mut wrong_space = manifest;
    wrong_space.artifacts[1].embedding_identity =
        test_semantic_identity("different-fast-space", 256, "fsvi-v1");
    let error = wrong_space
        .validate()
        .expect_err("ANN identity mismatch must be typed");
    assert!(matches!(
        &error,
        SemanticGenerationError::EmbeddingIdentityMismatch { .. }
    ));
    assert_eq!(error.reason_code(), "semantic_embedding_identity_mismatch");
}

#[test]
fn accepted_manifest_storage_order_is_stable_but_digest_is_json_independent() {
    let corpus = test_corpus_identity();
    let base_vector = complete_generation_artifact(
        SemanticArtifactRole::FastVector,
        "fast/primary.fsvi",
        b"fast-vector",
        &corpus,
    );
    let ann = bind_ann_to_base(
        complete_generation_artifact(
            SemanticArtifactRole::FastAnn,
            "fast/search.hnsw",
            b"fast-ann",
            &corpus,
        ),
        &base_vector,
    );
    let base =
        test_generation_manifest(SemanticGenerationTopology::FastOnly, vec![base_vector, ann]);
    base.validate().unwrap();
    let canonical = base.canonical_bytes().unwrap();
    assert_eq!(canonical, base.canonical_bytes().unwrap());
    let canonical_text = std::str::from_utf8(&canonical).unwrap();

    let root_keys = [
        "\"schema_version\"",
        "\"generation_id\"",
        "\"build_id\"",
        "\"request_id\"",
        "\"previous_generation_id\"",
        "\"created_at_ms\"",
        "\"completed_at_ms\"",
        "\"sealed_at_ms\"",
        "\"requested_topology\"",
        "\"realized_topology\"",
        "\"status\"",
        "\"corpus\"",
        "\"producer\"",
        "\"selected_document_count\"",
        "\"total_vector_slot_count_across_tiers\"",
        "\"total_live_vector_count_across_tiers\"",
        "\"total_tombstone_vector_count_across_tiers\"",
        "\"total_wal_entry_count_across_tiers\"",
        "\"artifacts\"",
    ];
    let root_positions = root_keys.map(|key| canonical_text.find(key).expect("root manifest key"));
    assert!(
        root_positions
            .windows(2)
            .all(|window| window[0] < window[1]),
        "stored root fields must remain in schema order: {root_positions:?}"
    );

    let corpus_json = canonical_text
        .split_once("\"corpus\":")
        .expect("corpus JSON")
        .1;
    let corpus_keys = [
        "\"database_uuid\"",
        "\"source_change_sequence\"",
        "\"schema_contract_sha256\"",
        "\"content_selection_sha256\"",
        "\"canonicalization_sha256\"",
        "\"chunking_sha256\"",
        "\"document_id_contract_sha256\"",
        "\"config_sha256\"",
        "\"corpus_sha256\"",
        "\"ordered_live_docset_sha256\"",
        "\"content_sha256\"",
        "\"document_count\"",
    ];
    let corpus_positions = corpus_keys.map(|key| corpus_json.find(key).expect("corpus key"));
    assert!(
        corpus_positions
            .windows(2)
            .all(|window| window[0] < window[1]),
        "stored corpus fields must remain in schema order: {corpus_positions:?}"
    );

    let producer_json = canonical_text
        .split_once("\"producer\":")
        .expect("producer JSON")
        .1;
    let producer_keys = [
        "\"cass_git_commit\"",
        "\"frankensearch_git_commit\"",
        "\"cargo_lock_sha256\"",
        "\"rust_toolchain\"",
        "\"enabled_features\"",
        "\"build_profile\"",
        "\"binary_elf_sha256\"",
    ];
    let producer_positions =
        producer_keys.map(|key| producer_json.find(key).expect("producer key"));
    assert!(
        producer_positions
            .windows(2)
            .all(|window| window[0] < window[1]),
        "stored producer fields must remain in schema order: {producer_positions:?}"
    );

    let artifacts_json = canonical_text
        .split_once("\"artifacts\":")
        .expect("artifact JSON")
        .1;
    let artifact_keys = [
        "\"role\"",
        "\"relative_path\"",
        "\"artifact_sha256\"",
        "\"size_bytes\"",
        "\"artifact_format\"",
        "\"artifact_parameters_sha256\"",
        "\"embedding_identity\"",
        "\"ann_base\"",
        "\"selected_document_count\"",
        "\"covered_document_count\"",
        "\"vector_slot_count\"",
        "\"live_vector_count\"",
        "\"tombstone_vector_count\"",
        "\"wal_entry_count\"",
        "\"generation_corpus_sha256\"",
        "\"covered_live_docset_sha256\"",
        "\"covered_content_sha256\"",
        "\"validation\"",
    ];
    let artifact_positions =
        artifact_keys.map(|key| artifacts_json.find(key).expect("artifact key"));
    assert!(
        artifact_positions
            .windows(2)
            .all(|window| window[0] < window[1]),
        "stored artifact fields must remain in schema order: {artifact_positions:?}"
    );
    let embedding_json = artifacts_json
        .split_once("\"embedding_identity\":")
        .expect("frozen embedding identity JSON")
        .1;
    let frozen_keys = ["\"identity\"", "\"canonical_bytes\"", "\"fingerprint\""];
    let frozen_positions =
        frozen_keys.map(|key| embedding_json.find(key).expect("frozen identity key"));
    assert!(
        frozen_positions
            .windows(2)
            .all(|window| window[0] < window[1]),
        "stored frozen identity fields must remain in schema order: {frozen_positions:?}"
    );
    let identity_json = embedding_json
        .split_once("\"identity\":")
        .expect("embedding bundle JSON")
        .1;
    let identity_keys = ["\"space\"", "\"producer\"", "\"input\"", "\"storage\""];
    let identity_positions =
        identity_keys.map(|key| identity_json.find(key).expect("identity component key"));
    assert!(
        identity_positions
            .windows(2)
            .all(|window| window[0] < window[1]),
        "stored identity components must remain in schema order: {identity_positions:?}"
    );
    let validation_json = artifacts_json
        .split_once("\"validation\":")
        .expect("validation JSON")
        .1;
    let validation_keys = [
        "\"validator_id\"",
        "\"validated_at_ms\"",
        "\"artifact_sha256\"",
        "\"size_bytes\"",
        "\"selected_document_count\"",
        "\"covered_document_count\"",
        "\"vector_slot_count\"",
        "\"live_vector_count\"",
        "\"tombstone_vector_count\"",
        "\"wal_entry_count\"",
        "\"generation_corpus_sha256\"",
        "\"covered_live_docset_sha256\"",
        "\"covered_content_sha256\"",
    ];
    let validation_positions =
        validation_keys.map(|key| validation_json.find(key).expect("validation key"));
    assert!(
        validation_positions
            .windows(2)
            .all(|window| window[0] < window[1]),
        "stored validation fields must remain in schema order: {validation_positions:?}"
    );
    for key in [
        "\"base_role\"",
        "\"base_artifact_sha256\"",
        "\"base_storage_fingerprint\"",
        "\"base_artifact_parameters_sha256\"",
        "\"algorithm\"",
        "\"metric\"",
        "\"parameters_sha256\"",
    ] {
        assert!(
            artifacts_json.contains(key),
            "missing ANN binding key {key}"
        );
    }

    let digest = base.sha256().unwrap();
    let pretty = serde_json::to_vec_pretty(&base).unwrap();
    let decoded: SemanticGenerationManifestV1 = serde_json::from_slice(&pretty).unwrap();
    assert_eq!(decoded.sha256().unwrap(), digest);
    assert_ne!(
        digest,
        sha256_hex(&canonical),
        "manifest fingerprint must be a domain-separated semantic transcript, not raw JSON"
    );
}

#[test]
fn accepted_manifest_digest_covers_every_authoritative_field() {
    let corpus = test_corpus_identity();
    let artifact = complete_generation_artifact(
        SemanticArtifactRole::FastVector,
        "fast/primary.fsvi",
        b"fast-vector",
        &corpus,
    );
    let base = test_generation_manifest(SemanticGenerationTopology::FastOnly, vec![artifact]);
    base.validate().unwrap();
    let base_digest = base.sha256().unwrap();

    macro_rules! assert_digest_field {
        ($label:literal, $mutate:expr) => {{
            let candidate = accepted_mutated_generation(&base, $mutate);
            assert_ne!(
                candidate.sha256().unwrap(),
                base_digest,
                "authoritative field {} must affect the semantic digest",
                $label
            );
        }};
    }

    assert_digest_field!("schema_version", |value| value.schema_version += 1);
    assert_digest_field!("generation_id", |value| value.generation_id.push('x'));
    assert_digest_field!("build_id", |value| value.build_id.push('x'));
    assert_digest_field!("request_id", |value| value.request_id = None);
    assert_digest_field!("previous_generation_id", |value| {
        value.previous_generation_id = None;
    });
    assert_digest_field!("created_at_ms", |value| value.created_at_ms += 1);
    assert_digest_field!("completed_at_ms", |value| value.completed_at_ms += 1);
    assert_digest_field!("sealed_at_ms", |value| value.sealed_at_ms += 1);
    assert_digest_field!("requested_topology", |value| {
        value.requested_topology = SemanticGenerationTopology::FastOnly;
    });
    assert_digest_field!("realized_topology", |value| {
        value.realized_topology = SemanticGenerationTopology::QualityOnly;
    });
    assert_digest_field!("status", |value| {
        value.status = SemanticGenerationStatus::Building;
    });

    assert_digest_field!("corpus.database_uuid", |value| {
        value.corpus.database_uuid.push('x');
    });
    assert_digest_field!("corpus.source_change_sequence", |value| {
        value.corpus.source_change_sequence += 1;
    });
    assert_digest_field!("corpus.schema_contract_sha256", |value| {
        value.corpus.schema_contract_sha256 = test_sha256("changed-schema");
    });
    assert_digest_field!("corpus.content_selection_sha256", |value| {
        value.corpus.content_selection_sha256 = test_sha256("changed-selection");
    });
    assert_digest_field!("corpus.canonicalization_sha256", |value| {
        value.corpus.canonicalization_sha256 = test_sha256("changed-canonicalization");
    });
    assert_digest_field!("corpus.chunking_sha256", |value| {
        value.corpus.chunking_sha256 = test_sha256("changed-chunking");
    });
    assert_digest_field!("corpus.document_id_contract_sha256", |value| {
        value.corpus.document_id_contract_sha256 = test_sha256("changed-document-id");
    });
    assert_digest_field!("corpus.config_sha256", |value| {
        value.corpus.config_sha256 = test_sha256("changed-config");
    });
    assert_digest_field!("corpus.corpus_sha256", |value| {
        value.corpus.corpus_sha256 = test_sha256("changed-corpus");
    });
    assert_digest_field!("corpus.ordered_live_docset_sha256", |value| {
        value.corpus.ordered_live_docset_sha256 = test_sha256("changed-docset");
    });
    assert_digest_field!("corpus.content_sha256", |value| {
        value.corpus.content_sha256 = test_sha256("changed-content");
    });
    assert_digest_field!("corpus.document_count", |value| {
        value.corpus.document_count += 1;
    });

    assert_digest_field!("producer.cass_git_commit", |value| {
        value.producer.cass_git_commit.push('c');
    });
    assert_digest_field!("producer.frankensearch_git_commit", |value| {
        value.producer.frankensearch_git_commit.push('c');
    });
    assert_digest_field!("producer.cargo_lock_sha256", |value| {
        value.producer.cargo_lock_sha256 = test_sha256("changed-lock");
    });
    assert_digest_field!("producer.rust_toolchain", |value| {
        value.producer.rust_toolchain.push('x');
    });
    assert_digest_field!("producer.enabled_features", |value| {
        value.producer.enabled_features.push("zstd".to_owned());
    });
    assert_digest_field!("producer.build_profile", |value| {
        value.producer.build_profile.push('x');
    });
    assert_digest_field!("producer.binary_elf_sha256", |value| {
        value.producer.binary_elf_sha256 = test_sha256("changed-binary");
    });

    assert_digest_field!("selected_document_count", |value| {
        value.selected_document_count += 1;
    });
    assert_digest_field!("total_vector_slot_count_across_tiers", |value| {
        value.total_vector_slot_count_across_tiers += 1;
    });
    assert_digest_field!("total_live_vector_count_across_tiers", |value| {
        value.total_live_vector_count_across_tiers += 1;
    });
    assert_digest_field!("total_tombstone_vector_count_across_tiers", |value| {
        value.total_tombstone_vector_count_across_tiers += 1;
    });
    assert_digest_field!("total_wal_entry_count_across_tiers", |value| {
        value.total_wal_entry_count_across_tiers += 1;
    });
    assert_digest_field!("artifacts length", |value| value.artifacts.clear());

    assert_digest_field!("artifact.role", |value| {
        value.artifacts[0].role = SemanticArtifactRole::QualityVector;
    });
    assert_digest_field!("artifact.relative_path", |value| {
        value.artifacts[0].relative_path.push('x');
    });
    assert_digest_field!("artifact.artifact_sha256", |value| {
        value.artifacts[0].artifact_sha256 = test_sha256("changed-artifact");
    });
    assert_digest_field!("artifact.size_bytes", |value| {
        value.artifacts[0].size_bytes += 1;
    });
    assert_digest_field!("artifact.artifact_format", |value| {
        value.artifacts[0].artifact_format.push('x');
    });
    assert_digest_field!("artifact.artifact_parameters_sha256", |value| {
        value.artifacts[0].artifact_parameters_sha256 = test_sha256("changed-parameters");
    });
    assert_digest_field!("artifact.embedding_identity", |value| {
        value.artifacts[0].embedding_identity =
            test_semantic_identity("changed-fast", 256, "fsvi-v1");
    });
    assert_digest_field!("artifact.selected_document_count", |value| {
        value.artifacts[0].selected_document_count += 1;
    });
    assert_digest_field!("artifact.covered_document_count", |value| {
        value.artifacts[0].covered_document_count -= 1;
    });
    assert_digest_field!("artifact.vector_slot_count", |value| {
        value.artifacts[0].vector_slot_count += 1;
    });
    assert_digest_field!("artifact.live_vector_count", |value| {
        value.artifacts[0].live_vector_count -= 1;
    });
    assert_digest_field!("artifact.tombstone_vector_count", |value| {
        value.artifacts[0].tombstone_vector_count += 1;
    });
    assert_digest_field!("artifact.wal_entry_count", |value| {
        value.artifacts[0].wal_entry_count += 1;
    });
    assert_digest_field!("artifact.generation_corpus_sha256", |value| {
        value.artifacts[0].generation_corpus_sha256 = test_sha256("changed-generation");
    });
    assert_digest_field!("artifact.covered_live_docset_sha256", |value| {
        value.artifacts[0].covered_live_docset_sha256 = test_sha256("changed-artifact-docset");
    });
    assert_digest_field!("artifact.covered_content_sha256", |value| {
        value.artifacts[0].covered_content_sha256 = test_sha256("changed-artifact-content");
    });

    assert_digest_field!("validation.validator_id", |value| {
        value.artifacts[0].validation.validator_id.push('x');
    });
    assert_digest_field!("validation.validated_at_ms", |value| {
        value.artifacts[0].validation.validated_at_ms += 1;
    });
    assert_digest_field!("validation.artifact_sha256", |value| {
        value.artifacts[0].validation.artifact_sha256 = test_sha256("changed-validation-artifact");
    });
    assert_digest_field!("validation.size_bytes", |value| {
        value.artifacts[0].validation.size_bytes += 1;
    });
    assert_digest_field!("validation.selected_document_count", |value| {
        value.artifacts[0].validation.selected_document_count += 1;
    });
    assert_digest_field!("validation.covered_document_count", |value| {
        value.artifacts[0].validation.covered_document_count -= 1;
    });
    assert_digest_field!("validation.vector_slot_count", |value| {
        value.artifacts[0].validation.vector_slot_count += 1;
    });
    assert_digest_field!("validation.live_vector_count", |value| {
        value.artifacts[0].validation.live_vector_count -= 1;
    });
    assert_digest_field!("validation.tombstone_vector_count", |value| {
        value.artifacts[0].validation.tombstone_vector_count += 1;
    });
    assert_digest_field!("validation.wal_entry_count", |value| {
        value.artifacts[0].validation.wal_entry_count += 1;
    });
    assert_digest_field!("validation.generation_corpus_sha256", |value| {
        value.artifacts[0].validation.generation_corpus_sha256 =
            test_sha256("changed-validation-generation");
    });
    assert_digest_field!("validation.covered_live_docset_sha256", |value| {
        value.artifacts[0].validation.covered_live_docset_sha256 =
            test_sha256("changed-validation-docset");
    });
    assert_digest_field!("validation.covered_content_sha256", |value| {
        value.artifacts[0].validation.covered_content_sha256 =
            test_sha256("changed-validation-content");
    });

    let base_vector = base.artifacts[0].clone();
    let ann = bind_ann_to_base(
        complete_generation_artifact(
            SemanticArtifactRole::FastAnn,
            "fast/search.hnsw",
            b"fast-ann",
            &corpus,
        ),
        &base_vector,
    );
    let ann_base =
        test_generation_manifest(SemanticGenerationTopology::FastOnly, vec![base_vector, ann]);
    let ann_digest = ann_base.sha256().unwrap();
    macro_rules! assert_ann_binding_field {
        ($label:literal, $mutate:expr) => {{
            let candidate = accepted_mutated_generation(&ann_base, $mutate);
            assert_ne!(
                candidate.sha256().unwrap(),
                ann_digest,
                "authoritative ANN field {} must affect the semantic digest",
                $label
            );
        }};
    }
    assert_ann_binding_field!("ann_base presence", |value| {
        value.artifacts[1].ann_base = None;
    });
    assert_ann_binding_field!("ann_base.base_role", |value| {
        value.artifacts[1].ann_base.as_mut().unwrap().base_role =
            SemanticArtifactRole::QualityVector;
    });
    assert_ann_binding_field!("ann_base.base_artifact_sha256", |value| {
        value.artifacts[1]
            .ann_base
            .as_mut()
            .unwrap()
            .base_artifact_sha256 = test_sha256("changed-base");
    });
    assert_ann_binding_field!("ann_base.base_storage_fingerprint", |value| {
        value.artifacts[1]
            .ann_base
            .as_mut()
            .unwrap()
            .base_storage_fingerprint = test_sha256("changed-storage");
    });
    assert_ann_binding_field!("ann_base.base_artifact_parameters_sha256", |value| {
        value.artifacts[1]
            .ann_base
            .as_mut()
            .unwrap()
            .base_artifact_parameters_sha256 = test_sha256("changed-base-parameters");
    });
    assert_ann_binding_field!("ann_base.algorithm", |value| {
        value.artifacts[1]
            .ann_base
            .as_mut()
            .unwrap()
            .algorithm
            .push('x');
    });
    assert_ann_binding_field!("ann_base.metric", |value| {
        value.artifacts[1]
            .ann_base
            .as_mut()
            .unwrap()
            .metric
            .push('x');
    });
    assert_ann_binding_field!("ann_base.parameters_sha256", |value| {
        value.artifacts[1]
            .ann_base
            .as_mut()
            .unwrap()
            .parameters_sha256 = test_sha256("changed-ann-parameters");
    });
}

#[test]
fn accepted_manifest_seal_faults_never_leave_torn_final_bytes() {
    let checkpoints = [
        ManifestSealCheckpoint::ArtifactDurabilityEstablished,
        ManifestSealCheckpoint::TemporaryBytesWritten,
        ManifestSealCheckpoint::TemporaryFileSynced,
        ManifestSealCheckpoint::ManifestPublished,
        ManifestSealCheckpoint::ParentDirectorySynced,
    ];
    for checkpoint in checkpoints {
        let temp = tempfile::tempdir().unwrap();
        let corpus = test_corpus_identity();
        let bytes = b"durable-vector".to_vec();
        let artifact = complete_generation_artifact(
            SemanticArtifactRole::FastVector,
            "fast/primary.fsvi",
            &bytes,
            &corpus,
        );
        let manifest = generation_with_id(
            &format!("gen-seal-{checkpoint:?}"),
            SemanticGenerationTopology::FastOnly,
            vec![artifact],
        );
        write_generation_artifacts(
            temp.path(),
            &manifest,
            &BTreeMap::from([(SemanticArtifactRole::FastVector, bytes)]),
        );
        let error = manifest
            .write_immutable_with_hook(temp.path(), |observed| {
                if observed == checkpoint {
                    return Err(SemanticGenerationError::ManifestIo {
                        generation_id: manifest.generation_id.clone(),
                        source: "injected seal fault".to_owned(),
                    });
                }
                Ok(())
            })
            .expect_err("injected checkpoint must fail");
        assert!(matches!(error, SemanticGenerationError::ManifestIo { .. }));

        let manifest_path = manifest.manifest_path(temp.path()).unwrap();
        if matches!(
            checkpoint,
            ManifestSealCheckpoint::ManifestPublished
                | ManifestSealCheckpoint::ParentDirectorySynced
        ) {
            let loaded = load_manifest_file(&manifest_path, &manifest.generation_id).unwrap();
            assert_eq!(loaded, manifest);
            assert_eq!(loaded.sha256().unwrap(), manifest.sha256().unwrap());
        } else {
            assert!(
                !manifest_path.exists(),
                "pre-publication fault exposed a final manifest"
            );
        }
        let generation_dir = manifest.generation_dir(temp.path()).unwrap();
        assert!(
            fs::read_dir(generation_dir)
                .unwrap()
                .filter_map(Result::ok)
                .all(|entry| !entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".manifest.seal.")),
            "temporary seal file survived error return"
        );
    }
}
