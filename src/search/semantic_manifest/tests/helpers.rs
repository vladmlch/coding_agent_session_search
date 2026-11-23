use super::*;

pub(super) fn test_policy() -> SemanticPolicy {
    SemanticPolicy::compiled_defaults()
}

pub(super) fn test_artifact(tier: TierKind, ready: bool) -> ArtifactRecord {
    ArtifactRecord {
        tier,
        embedder_id: match tier {
            TierKind::Fast => "fnv1a-384".to_owned(),
            TierKind::Quality => "minilm-384".to_owned(),
        },
        model_revision: "abc123".to_owned(),
        schema_version: SEMANTIC_SCHEMA_VERSION,
        chunking_version: CHUNKING_STRATEGY_VERSION,
        dimension: 384,
        doc_count: 1000,
        conversation_count: 250,
        db_fingerprint: "fp-1234".to_owned(),
        index_path: format!(
            "vector_index/index-{}.fsvi",
            match tier {
                TierKind::Fast => "fnv1a-384",
                TierKind::Quality => "minilm-384",
            }
        ),
        size_bytes: 150_000,
        started_at_ms: 1_700_000_000_000,
        completed_at_ms: 1_700_000_060_000,
        ready,
    }
}

pub(super) fn test_hnsw() -> HnswRecord {
    HnswRecord {
        base_tier: TierKind::Quality,
        embedder_id: "minilm-384".to_owned(),
        ef_search: 128,
        index_path: "vector_index/hnsw-minilm-384.chsw".to_owned(),
        size_bytes: 50_000,
        built_at_ms: 1_700_000_070_000,
        ready: true,
    }
}

pub(super) fn test_shard(shard_index: u32, shard_count: u32, ready: bool) -> SemanticShardRecord {
    SemanticShardRecord {
        tier: TierKind::Fast,
        embedder_id: "fnv1a-384".to_owned(),
        model_revision: "hash".to_owned(),
        schema_version: SEMANTIC_SCHEMA_VERSION,
        chunking_version: CHUNKING_STRATEGY_VERSION,
        dimension: 384,
        shard_index,
        shard_count,
        doc_count: 25,
        total_conversations: 10,
        db_fingerprint: "fp-sharded".to_owned(),
        index_path: format!("vector_index/shards/fast-fnv1a-384/shard-{shard_index:05}.fsvi"),
        quantization: "f16".to_owned(),
        mmap_ready: true,
        ann_index_path: None,
        ann_size_bytes: 0,
        ann_ready: false,
        size_bytes: 4096,
        started_at_ms: 1_700_000_080_000,
        completed_at_ms: 1_700_000_081_000,
        ready,
    }
}

pub(super) fn test_checkpoint(tier: TierKind) -> BuildCheckpoint {
    BuildCheckpoint {
        tier,
        embedder_id: "minilm-384".to_owned(),
        last_offset: 500,
        docs_embedded: 3000,
        conversations_processed: 500,
        total_conversations: 1000,
        db_fingerprint: "fp-1234".to_owned(),
        schema_version: SEMANTIC_SCHEMA_VERSION,
        chunking_version: CHUNKING_STRATEGY_VERSION,
        saved_at_ms: 1_700_000_030_000,
        last_message_id: None,
        cursor_exhausted: false,
    }
}

pub(super) fn test_sha256(label: &str) -> String {
    sha256_hex(label.as_bytes())
}

pub(super) fn test_semantic_identity(
    label: &str,
    dimension: u32,
    storage_format: &str,
) -> FrozenEmbeddingIdentityBundleV1 {
    use frankensearch::core::{
        EMBEDDING_INPUT_CONTRACT_SCHEMA_V1, EMBEDDING_PRODUCER_ATTESTATION_SCHEMA_V1,
        EMBEDDING_SPACE_IDENTITY_SCHEMA_V1, EmbeddingArtifactIdentityV1, EmbeddingIdentityBundleV1,
        EmbeddingInputContractV1, EmbeddingProducerAttestationV1, EmbeddingSpaceIdentityV1,
        GoldenVectorCertificateV1, QuantizationFormat, VECTOR_STORAGE_IDENTITY_SCHEMA_V1,
        VectorStorageIdentityV1,
    };

    let input = EmbeddingInputContractV1 {
        schema_version: EMBEDDING_INPUT_CONTRACT_SCHEMA_V1,
        canonicalization: "cass-canonicalization-v1".to_owned(),
        content_selection: "cass-selected-message-content-v1".to_owned(),
        chunking: "cass-chunking-v1".to_owned(),
        query_instruction: "query:".to_owned(),
        document_instruction: "document:".to_owned(),
        doc_id_semantics: "cass-stable-document-id-v1".to_owned(),
    };
    let mut space = EmbeddingSpaceIdentityV1 {
        schema_version: EMBEDDING_SPACE_IDENTITY_SCHEMA_V1,
        logical_model_id: format!("{label}-semantic-model"),
        immutable_revision: format!("{label}-revision-v1"),
        kind: EmbeddingSpaceKindV1::Semantic,
        artifact_manifest_fingerprint: test_sha256(&format!("{label}-model-manifest")),
        artifacts: vec![
            EmbeddingArtifactIdentityV1 {
                role: "tokenizer".to_owned(),
                sha256: test_sha256(&format!("{label}-tokenizer")),
                size: 101,
            },
            EmbeddingArtifactIdentityV1 {
                role: "weights".to_owned(),
                sha256: test_sha256(&format!("{label}-weights")),
                size: 10_001,
            },
        ],
        tokenizer_fingerprint: test_sha256(&format!("{label}-tokenizer-contract")),
        vocabulary_fingerprint: test_sha256(&format!("{label}-vocabulary")),
        model_config_fingerprint: test_sha256(&format!("{label}-config")),
        model_preprocessing: "lowercase=false;unicode=nfc".to_owned(),
        sequence_policy: "truncate=tail;max_tokens=512;padding=none".to_owned(),
        query_instruction: "query:".to_owned(),
        document_instruction: "document:".to_owned(),
        pooling: "mean-mask-aware".to_owned(),
        output_normalization: "l2-f32-v1".to_owned(),
        dimension,
        input_contract_fingerprint: input.fingerprint(),
        hash_control: None,
        projection: None,
    };
    let producer = EmbeddingProducerAttestationV1 {
        schema_version: EMBEDDING_PRODUCER_ATTESTATION_SCHEMA_V1,
        backend: "frankensearch-native".to_owned(),
        implementation_revision: "implementation-v1".to_owned(),
        protocol_revision: "in-process-v1".to_owned(),
        numeric_profile: "deterministic-f32-v1".to_owned(),
        provenance_manifest_fingerprint: test_sha256(&format!("{label}-provenance-manifest")),
        space_fingerprint: space.fingerprint(),
        golden_vectors: GoldenVectorCertificateV1 {
            corpus_sha256: test_sha256(&format!("{label}-golden-corpus")),
            vectors_sha256: test_sha256(&format!("{label}-golden-vectors")),
            vector_count: 2,
            dimension,
        },
    };
    space.artifact_manifest_fingerprint = test_sha256(&format!("{label}-model-manifest"));
    EmbeddingIdentityBundleV1 {
        space,
        producer,
        input,
        storage: VectorStorageIdentityV1 {
            schema_version: VECTOR_STORAGE_IDENTITY_SCHEMA_V1,
            format: storage_format.to_owned(),
            quantization: QuantizationFormat::F16,
            endianness: "little-endian".to_owned(),
            vector_normalization: "l2-f32-v1".to_owned(),
            dimension,
        },
    }
    .freeze()
    .expect("test semantic identity must satisfy Frankensearch")
}

pub(super) fn test_corpus_identity() -> SemanticCorpusSnapshotIdentity {
    SemanticCorpusSnapshotIdentity {
        database_uuid: "cass-test-db-uuid".to_owned(),
        source_change_sequence: 42,
        schema_contract_sha256: test_sha256("schema-contract"),
        content_selection_sha256: test_sha256("content-selection"),
        canonicalization_sha256: test_sha256("canonicalization"),
        chunking_sha256: test_sha256("chunking"),
        document_id_contract_sha256: test_sha256("document-id-contract"),
        config_sha256: test_sha256("config"),
        corpus_sha256: test_sha256("generation"),
        ordered_live_docset_sha256: test_sha256("ordered-docset"),
        content_sha256: test_sha256("content"),
        document_count: 3,
    }
}

fn test_producer_attestation() -> SemanticProducerAttestation {
    SemanticProducerAttestation {
        cass_git_commit: "a".repeat(40),
        frankensearch_git_commit: "b".repeat(40),
        cargo_lock_sha256: test_sha256("cargo-lock"),
        rust_toolchain: "stable-1.90.0".to_owned(),
        enabled_features: vec!["native".to_owned(), "semantic".to_owned()],
        build_profile: "release".to_owned(),
        binary_elf_sha256: test_sha256("binary"),
    }
}

pub(super) fn test_generation_artifact(
    role: SemanticArtifactRole,
    relative_path: &str,
    bytes: &[u8],
    corpus: &SemanticCorpusSnapshotIdentity,
    covered_document_count: u64,
) -> SemanticGenerationArtifact {
    let size_bytes = u64::try_from(bytes.len()).expect("test artifact length fits u64");
    let quality = matches!(
        role,
        SemanticArtifactRole::QualityVector | SemanticArtifactRole::QualityAnn
    );
    let artifact_sha256 = sha256_hex(bytes);
    let embedding_identity = test_semantic_identity(
        if quality { "quality" } else { "fast" },
        if quality { 384 } else { 256 },
        "fsvi-v1",
    );
    let full_coverage = covered_document_count == corpus.document_count;
    let covered_live_docset_sha256 = if full_coverage {
        corpus.ordered_live_docset_sha256.clone()
    } else {
        test_sha256(if quality {
            "partial-quality-docset"
        } else {
            "partial-fast-docset"
        })
    };
    let covered_content_sha256 = if full_coverage {
        corpus.content_sha256.clone()
    } else {
        test_sha256(if quality {
            "partial-quality-content"
        } else {
            "partial-fast-content"
        })
    };
    let artifact_format = if role.is_vector() {
        "fsvi-v1"
    } else {
        "hnsw-v1"
    };
    let artifact_parameters_sha256 = test_sha256(match role {
        SemanticArtifactRole::FastVector => "fast-fsvi-parameters",
        SemanticArtifactRole::QualityVector => "quality-fsvi-parameters",
        SemanticArtifactRole::FastAnn => "fast-ann-parameters",
        SemanticArtifactRole::QualityAnn => "quality-ann-parameters",
    });
    let validation = SemanticArtifactValidationEvidence {
        validator_id: "frankensearch-fsvi-v1".to_owned(),
        validated_at_ms: 1_700_000_000_250,
        artifact_sha256: artifact_sha256.clone(),
        size_bytes,
        selected_document_count: corpus.document_count,
        covered_document_count,
        vector_slot_count: covered_document_count,
        live_vector_count: covered_document_count,
        tombstone_vector_count: 0,
        wal_entry_count: 0,
        generation_corpus_sha256: corpus.corpus_sha256.clone(),
        covered_live_docset_sha256: covered_live_docset_sha256.clone(),
        covered_content_sha256: covered_content_sha256.clone(),
    };
    SemanticGenerationArtifact {
        shard: None,
        role,
        relative_path: relative_path.to_owned(),
        artifact_sha256,
        size_bytes,
        artifact_format: artifact_format.to_owned(),
        artifact_parameters_sha256,
        embedding_identity,
        ann_base: None,
        selected_document_count: corpus.document_count,
        covered_document_count,
        vector_slot_count: covered_document_count,
        live_vector_count: covered_document_count,
        tombstone_vector_count: 0,
        wal_entry_count: 0,
        generation_corpus_sha256: corpus.corpus_sha256.clone(),
        covered_live_docset_sha256,
        covered_content_sha256,
        validation,
    }
}

pub(super) fn bind_ann_to_base(
    mut ann: SemanticGenerationArtifact,
    base: &SemanticGenerationArtifact,
) -> SemanticGenerationArtifact {
    ann.embedding_identity = base.embedding_identity.clone();
    ann.selected_document_count = base.selected_document_count;
    ann.covered_document_count = base.covered_document_count;
    ann.vector_slot_count = base.vector_slot_count;
    ann.live_vector_count = base.live_vector_count;
    ann.tombstone_vector_count = base.tombstone_vector_count;
    ann.generation_corpus_sha256 = base.generation_corpus_sha256.clone();
    ann.covered_live_docset_sha256 = base.covered_live_docset_sha256.clone();
    ann.covered_content_sha256 = base.covered_content_sha256.clone();
    ann.validation.selected_document_count = ann.selected_document_count;
    ann.validation.covered_document_count = ann.covered_document_count;
    ann.validation.vector_slot_count = ann.vector_slot_count;
    ann.validation.live_vector_count = ann.live_vector_count;
    ann.validation.tombstone_vector_count = ann.tombstone_vector_count;
    ann.validation.generation_corpus_sha256 = ann.generation_corpus_sha256.clone();
    ann.validation.covered_live_docset_sha256 = ann.covered_live_docset_sha256.clone();
    ann.validation.covered_content_sha256 = ann.covered_content_sha256.clone();
    ann.ann_base = Some(SemanticAnnBaseBindingV1 {
        base_role: base.role,
        base_artifact_sha256: base.artifact_sha256.clone(),
        base_storage_fingerprint: base.embedding_identity.identity.storage.fingerprint(),
        base_artifact_parameters_sha256: base.artifact_parameters_sha256.clone(),
        algorithm: "hnsw".to_owned(),
        metric: "cosine".to_owned(),
        parameters_sha256: ann.artifact_parameters_sha256.clone(),
    });
    ann
}

pub(super) fn test_generation_manifest(
    realized_topology: SemanticGenerationTopology,
    artifacts: Vec<SemanticGenerationArtifact>,
) -> SemanticGenerationManifestV1 {
    let vector_slot_count = artifacts
        .iter()
        .filter(|artifact| artifact.role.is_vector())
        .map(|artifact| artifact.vector_slot_count)
        .sum();
    let live_vector_count = artifacts
        .iter()
        .filter(|artifact| artifact.role.is_vector())
        .map(|artifact| artifact.live_vector_count)
        .sum();
    let tombstone_vector_count = artifacts
        .iter()
        .filter(|artifact| artifact.role.is_vector())
        .map(|artifact| artifact.tombstone_vector_count)
        .sum();
    let wal_entry_count = artifacts
        .iter()
        .filter(|artifact| artifact.role.is_vector())
        .map(|artifact| artifact.wal_entry_count)
        .sum();
    SemanticGenerationManifestV1 {
        schema_version: SEMANTIC_GENERATION_MANIFEST_SCHEMA_VERSION,
        generation_id: "gen-20260726-0001".to_owned(),
        build_id: "build-0001".to_owned(),
        request_id: Some("request-0001".to_owned()),
        previous_generation_id: Some("gen-20260725-0001".to_owned()),
        created_at_ms: 1_700_000_000_000,
        completed_at_ms: 1_700_000_000_200,
        sealed_at_ms: 1_700_000_000_300,
        requested_topology: SemanticGenerationTopology::FullProgressive,
        realized_topology,
        status: SemanticGenerationStatus::Sealed,
        corpus: test_corpus_identity(),
        producer: test_producer_attestation(),
        selected_document_count: 3,
        total_vector_slot_count_across_tiers: vector_slot_count,
        total_live_vector_count_across_tiers: live_vector_count,
        total_tombstone_vector_count_across_tiers: tombstone_vector_count,
        total_wal_entry_count_across_tiers: wal_entry_count,
        artifacts,
    }
}

pub(super) fn write_generation_artifacts(
    data_dir: &Path,
    manifest: &SemanticGenerationManifestV1,
    contents: &BTreeMap<SemanticArtifactRole, Vec<u8>>,
) {
    let generation_dir = manifest.generation_dir(data_dir).unwrap();
    fs::create_dir_all(&generation_dir).unwrap();
    for artifact in &manifest.artifacts {
        let path = generation_dir.join(&artifact.relative_path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, contents.get(&artifact.role).unwrap()).unwrap();
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) enum ExpectedTierReadiness {
    Ready,
    Stale,
    Incompatible,
    Building(u8),
}

pub(super) fn no_artifact_mutation(_: &mut ArtifactRecord) {}

pub(super) type TierReadinessCase = (
    &'static str,
    TierKind,
    bool,
    &'static str,
    &'static str,
    fn(&mut ArtifactRecord),
    ExpectedTierReadiness,
);

pub(super) fn set_schema_version_to_zero(artifact: &mut ArtifactRecord) {
    artifact.schema_version = 0;
}

pub(super) fn assert_tier_readiness(
    actual: TierReadiness,
    expected: ExpectedTierReadiness,
    label: &str,
) {
    match expected {
        ExpectedTierReadiness::Ready => {
            assert_eq!(actual, TierReadiness::Ready, "{label}");
        }
        ExpectedTierReadiness::Stale => {
            assert!(
                matches!(actual, TierReadiness::Stale { .. }),
                "{label}: {actual:?}"
            );
        }
        ExpectedTierReadiness::Incompatible => {
            assert!(
                matches!(actual, TierReadiness::Incompatible { .. }),
                "{label}: {actual:?}"
            );
        }
        ExpectedTierReadiness::Building(progress_pct) => {
            assert_eq!(actual, TierReadiness::Building { progress_pct }, "{label}");
        }
    }
}

// ── Accepted immutable generation contract ────────────────────────

pub(super) fn accepted_mutated_generation(
    base: &SemanticGenerationManifestV1,
    mutate: impl FnOnce(&mut SemanticGenerationManifestV1),
) -> SemanticGenerationManifestV1 {
    let mut candidate = base.clone();
    mutate(&mut candidate);
    candidate
}

pub(super) fn complete_generation_artifact(
    role: SemanticArtifactRole,
    relative_path: &str,
    bytes: &[u8],
    corpus: &SemanticCorpusSnapshotIdentity,
) -> SemanticGenerationArtifact {
    test_generation_artifact(role, relative_path, bytes, corpus, corpus.document_count)
}

pub(super) fn generation_with_id(
    generation_id: &str,
    realized_topology: SemanticGenerationTopology,
    artifacts: Vec<SemanticGenerationArtifact>,
) -> SemanticGenerationManifestV1 {
    let mut manifest = test_generation_manifest(realized_topology, artifacts);
    manifest.generation_id = generation_id.to_owned();
    manifest.build_id = format!("build-{generation_id}");
    manifest.previous_generation_id = None;
    manifest
}

pub(super) fn seal_generation(
    data_dir: &Path,
    manifest: &SemanticGenerationManifestV1,
    contents: &BTreeMap<SemanticArtifactRole, Vec<u8>>,
) -> String {
    write_generation_artifacts(data_dir, manifest, contents);
    manifest.write_immutable(data_dir).expect("seal generation")
}
