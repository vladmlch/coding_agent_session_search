use super::generation_types::{
    MAX_EMBEDDING_DIMENSION, MAX_GENERATION_ID_LENGTH, MAX_PRODUCER_FEATURE_BYTES,
    MAX_PRODUCER_FEATURES, MAX_SEMANTIC_ARTIFACT_PATH_BYTES, SEMANTIC_GENERATION_MANIFEST_FILENAME,
    SEMANTIC_GENERATIONS_DIRNAME, SHA256_HEX_LENGTH, SemanticArtifactRole,
    SemanticCorpusSnapshotIdentity, SemanticCurrentPointerV1, SemanticGenerationArtifact,
    SemanticGenerationError, SemanticGenerationManifestV1, SemanticGenerationTopology,
    SemanticManifestInvariantClass, SemanticProducerAttestation,
};
use super::{
    Component, Digest, EmbeddingSpaceKindV1, Instant, IoRead, OpenOptions, Path, PathBuf, Sha256,
    fs,
};

pub(in crate::search::semantic_manifest) fn required_vector_roles(
    topology: SemanticGenerationTopology,
) -> impl Iterator<Item = SemanticArtifactRole> {
    let roles = match topology {
        SemanticGenerationTopology::FastOnly => [Some(SemanticArtifactRole::FastVector), None],
        SemanticGenerationTopology::QualityOnly => {
            [Some(SemanticArtifactRole::QualityVector), None]
        }
        SemanticGenerationTopology::FullProgressive => [
            Some(SemanticArtifactRole::FastVector),
            Some(SemanticArtifactRole::QualityVector),
        ],
    };
    roles.into_iter().flatten()
}

pub(in crate::search::semantic_manifest) fn validate_corpus_identity(
    corpus: &SemanticCorpusSnapshotIdentity,
) -> Result<(), SemanticGenerationError> {
    validate_identifier(
        SemanticManifestInvariantClass::Identity,
        "database_uuid",
        &corpus.database_uuid,
    )?;
    for (name, value) in [
        (
            "schema_contract_sha256",
            corpus.schema_contract_sha256.as_str(),
        ),
        (
            "content_selection_sha256",
            corpus.content_selection_sha256.as_str(),
        ),
        (
            "canonicalization_sha256",
            corpus.canonicalization_sha256.as_str(),
        ),
        ("chunking_sha256", corpus.chunking_sha256.as_str()),
        (
            "document_id_contract_sha256",
            corpus.document_id_contract_sha256.as_str(),
        ),
        ("config_sha256", corpus.config_sha256.as_str()),
        ("corpus_sha256", corpus.corpus_sha256.as_str()),
        (
            "ordered_live_docset_sha256",
            corpus.ordered_live_docset_sha256.as_str(),
        ),
        ("content_sha256", corpus.content_sha256.as_str()),
    ] {
        validate_sha256(name, value).map_err(|reason| {
            SemanticGenerationError::InvalidManifest {
                class: SemanticManifestInvariantClass::Identity,
                reason,
            }
        })?;
    }
    if corpus.document_count == 0 {
        return invalid_manifest(
            SemanticManifestInvariantClass::Count,
            "corpus document_count must be positive",
        );
    }
    Ok(())
}

pub(in crate::search::semantic_manifest) fn corpus_identity_sha256(
    corpus: &SemanticCorpusSnapshotIdentity,
) -> Result<String, SemanticGenerationError> {
    let mut encoder = SemanticCanonicalEncoder::new(b"cass.semantic-corpus-snapshot.v1");
    encode_corpus_identity(&mut encoder, corpus);
    Ok(sha256_hex(&encoder.finish()))
}

pub(in crate::search::semantic_manifest) fn validate_producer_attestation(
    producer: &SemanticProducerAttestation,
) -> Result<(), SemanticGenerationError> {
    validate_git_commit("cass_git_commit", &producer.cass_git_commit)?;
    validate_git_commit(
        "frankensearch_git_commit",
        &producer.frankensearch_git_commit,
    )?;
    for (name, value) in [
        ("rust_toolchain", producer.rust_toolchain.as_str()),
        ("build_profile", producer.build_profile.as_str()),
    ] {
        validate_identifier(SemanticManifestInvariantClass::Provenance, name, value)?;
    }
    for (name, value) in [
        ("cargo_lock_sha256", producer.cargo_lock_sha256.as_str()),
        ("binary_elf_sha256", producer.binary_elf_sha256.as_str()),
    ] {
        validate_sha256(name, value).map_err(|reason| {
            SemanticGenerationError::InvalidManifest {
                class: SemanticManifestInvariantClass::Provenance,
                reason,
            }
        })?;
    }
    if producer.enabled_features.len() > MAX_PRODUCER_FEATURES
        || producer.enabled_features.iter().any(|feature| {
            feature.is_empty()
                || feature.len() > MAX_PRODUCER_FEATURE_BYTES
                || feature.trim() != feature
                || feature.chars().any(char::is_whitespace)
        })
    {
        return invalid_manifest(
            SemanticManifestInvariantClass::Provenance,
            "producer feature names must be non-empty normalized tokens",
        );
    }
    if !producer
        .enabled_features
        .windows(2)
        .all(|window| matches!(window, [left, right] if left < right))
    {
        return invalid_manifest(
            SemanticManifestInvariantClass::Provenance,
            "producer features must be sorted and unique",
        );
    }
    Ok(())
}

fn validate_git_commit(name: &str, value: &str) -> Result<(), SemanticGenerationError> {
    if !matches!(value.len(), 40 | 64)
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return invalid_manifest(
            SemanticManifestInvariantClass::Provenance,
            &format!("{name} must be a full 40- or 64-character lowercase hexadecimal commit ID"),
        );
    }
    Ok(())
}

pub(in crate::search::semantic_manifest) fn validate_generation_artifact(
    artifact: &SemanticGenerationArtifact,
    corpus: &SemanticCorpusSnapshotIdentity,
    created_at_ms: i64,
    completed_at_ms: i64,
    sealed_at_ms: i64,
) -> Result<(), SemanticGenerationError> {
    for (name, value) in [
        ("artifact_sha256", artifact.artifact_sha256.as_str()),
        (
            "artifact_parameters_sha256",
            artifact.artifact_parameters_sha256.as_str(),
        ),
        (
            "generation_corpus_sha256",
            artifact.generation_corpus_sha256.as_str(),
        ),
        (
            "covered_live_docset_sha256",
            artifact.covered_live_docset_sha256.as_str(),
        ),
        (
            "covered_content_sha256",
            artifact.covered_content_sha256.as_str(),
        ),
    ] {
        validate_sha256(name, value).map_err(|reason| {
            SemanticGenerationError::InvalidManifest {
                class: SemanticManifestInvariantClass::Identity,
                reason,
            }
        })?;
    }
    validate_identifier(
        SemanticManifestInvariantClass::Identity,
        "artifact_format",
        &artifact.artifact_format,
    )?;
    validate_identifier(
        SemanticManifestInvariantClass::SealState,
        "validator_id",
        &artifact.validation.validator_id,
    )?;
    artifact.embedding_identity.validate().map_err(|error| {
        SemanticGenerationError::InvalidManifest {
            class: SemanticManifestInvariantClass::Identity,
            reason: format!("canonical Frankensearch embedding identity rejected: {error}"),
        }
    })?;
    if artifact.embedding_identity.identity.space.kind != EmbeddingSpaceKindV1::Semantic {
        return invalid_manifest(
            SemanticManifestInvariantClass::Identity,
            "semantic generations reject explicit hash-control embedding identities",
        );
    }
    let dimension = artifact.embedding_identity.identity.space.dimension;
    if dimension == 0 || dimension > MAX_EMBEDDING_DIMENSION {
        return invalid_manifest(
            SemanticManifestInvariantClass::Identity,
            "canonical embedding dimension must be 1..=65536",
        );
    }
    if artifact.role.is_vector() {
        if artifact.ann_base.is_some() {
            return invalid_manifest(
                SemanticManifestInvariantClass::AnnBinding,
                "vector artifacts must not carry ANN base bindings",
            );
        }
        if artifact.artifact_format != artifact.embedding_identity.identity.storage.format {
            return invalid_manifest(
                SemanticManifestInvariantClass::Identity,
                "vector artifact format must equal canonical Frankensearch storage identity",
            );
        }
    } else if artifact.ann_base.is_none() {
        return invalid_manifest(
            SemanticManifestInvariantClass::AnnBinding,
            "ANN artifacts require an exact base binding",
        );
    }
    if artifact.size_bytes == 0
        || artifact.selected_document_count == 0
        || artifact.covered_document_count == 0
        || artifact.vector_slot_count == 0
    {
        return invalid_manifest(
            SemanticManifestInvariantClass::Count,
            "sealed artifacts must have positive size, selected/covered document, and vector-slot counts",
        );
    }
    if artifact.selected_document_count != corpus.document_count {
        return invalid_manifest(
            SemanticManifestInvariantClass::Count,
            "artifact selected-document count must equal the generation corpus",
        );
    }
    if artifact.covered_document_count > artifact.selected_document_count
        || artifact.live_vector_count != artifact.covered_document_count
    {
        return invalid_manifest(
            SemanticManifestInvariantClass::PartialCoverage,
            "tier coverage must be within the selected corpus with exactly one live vector per covered document",
        );
    }
    if artifact
        .live_vector_count
        .checked_add(artifact.tombstone_vector_count)
        != Some(artifact.vector_slot_count)
    {
        return invalid_manifest(
            SemanticManifestInvariantClass::Count,
            "artifact live plus tombstone counts must equal physical vector-slot count",
        );
    }
    if artifact.wal_entry_count != 0 {
        return invalid_manifest(
            SemanticManifestInvariantClass::SealState,
            "immutable generation artifacts must be compacted and carry zero live WAL entries",
        );
    }
    if artifact.generation_corpus_sha256 != corpus.corpus_sha256 {
        return invalid_manifest(
            SemanticManifestInvariantClass::Identity,
            "artifact generation-corpus identity does not match the selected generation",
        );
    }
    let full_coverage = artifact.covered_document_count == artifact.selected_document_count;
    if full_coverage
        && (artifact.covered_live_docset_sha256 != corpus.ordered_live_docset_sha256
            || artifact.covered_content_sha256 != corpus.content_sha256)
    {
        return invalid_manifest(
            SemanticManifestInvariantClass::PartialCoverage,
            "full tier coverage must bind the generation's exact live-docset and content identities",
        );
    }
    if !full_coverage
        && (artifact.covered_live_docset_sha256 == corpus.ordered_live_docset_sha256
            || artifact.covered_content_sha256 == corpus.content_sha256)
    {
        return invalid_manifest(
            SemanticManifestInvariantClass::PartialCoverage,
            "partial tier coverage must carry distinct covered-subset docset and content identities",
        );
    }
    if let Some(binding) = artifact.ann_base.as_ref() {
        if !matches!(
            binding.base_role,
            SemanticArtifactRole::FastVector | SemanticArtifactRole::QualityVector
        ) {
            return invalid_manifest(
                SemanticManifestInvariantClass::AnnBinding,
                "ANN base role must identify a vector artifact",
            );
        }
        for (name, value) in [
            (
                "ann_base.base_artifact_sha256",
                binding.base_artifact_sha256.as_str(),
            ),
            (
                "ann_base.base_storage_fingerprint",
                binding.base_storage_fingerprint.as_str(),
            ),
            (
                "ann_base.base_artifact_parameters_sha256",
                binding.base_artifact_parameters_sha256.as_str(),
            ),
            (
                "ann_base.parameters_sha256",
                binding.parameters_sha256.as_str(),
            ),
        ] {
            validate_sha256(name, value).map_err(|reason| {
                SemanticGenerationError::InvalidManifest {
                    class: SemanticManifestInvariantClass::AnnBinding,
                    reason,
                }
            })?;
        }
        validate_identifier(
            SemanticManifestInvariantClass::AnnBinding,
            "ann_base.algorithm",
            &binding.algorithm,
        )?;
        validate_identifier(
            SemanticManifestInvariantClass::AnnBinding,
            "ann_base.metric",
            &binding.metric,
        )?;
        if binding.parameters_sha256 != artifact.artifact_parameters_sha256 {
            return invalid_manifest(
                SemanticManifestInvariantClass::AnnBinding,
                "ANN binding parameters do not match artifact parameters",
            );
        }
    }
    let evidence = &artifact.validation;
    if evidence.validated_at_ms < completed_at_ms
        || evidence.validated_at_ms < created_at_ms
        || evidence.validated_at_ms > sealed_at_ms
        || evidence.artifact_sha256 != artifact.artifact_sha256
        || evidence.size_bytes != artifact.size_bytes
        || evidence.selected_document_count != artifact.selected_document_count
        || evidence.covered_document_count != artifact.covered_document_count
        || evidence.vector_slot_count != artifact.vector_slot_count
        || evidence.live_vector_count != artifact.live_vector_count
        || evidence.tombstone_vector_count != artifact.tombstone_vector_count
        || evidence.wal_entry_count != artifact.wal_entry_count
        || evidence.generation_corpus_sha256 != artifact.generation_corpus_sha256
        || evidence.covered_live_docset_sha256 != artifact.covered_live_docset_sha256
        || evidence.covered_content_sha256 != artifact.covered_content_sha256
    {
        return invalid_manifest(
            SemanticManifestInvariantClass::SealState,
            "artifact validation evidence must postdate completion, predate sealing, and attest the descriptor exactly",
        );
    }
    Ok(())
}

pub(in crate::search::semantic_manifest) fn validate_generation_id(
    value: &str,
) -> Result<(), String> {
    if value.is_empty()
        || value.len() > MAX_GENERATION_ID_LENGTH
        || value == "."
        || value == ".."
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        || !value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        || !portable_path_segment_is_unambiguous(value)
    {
        return Err(
            "generation_id must be a portable 1-128 ASCII [A-Za-z0-9._-] segment, start alphanumeric, and avoid reserved/trailing-dot aliases"
                .to_owned(),
        );
    }
    Ok(())
}

pub(in crate::search::semantic_manifest) fn validate_identifier(
    class: SemanticManifestInvariantClass,
    name: &str,
    value: &str,
) -> Result<(), SemanticGenerationError> {
    if value.is_empty()
        || value.trim() != value
        || value.chars().any(char::is_control)
        || value.len() > 512
    {
        return invalid_manifest(
            class,
            &format!("{name} must be a non-empty normalized value of at most 512 bytes"),
        );
    }
    Ok(())
}

pub(in crate::search::semantic_manifest) fn validate_sha256(
    name: &str,
    value: &str,
) -> Result<(), String> {
    if value.len() != SHA256_HEX_LENGTH
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(format!(
            "{name} must be 64 lowercase hexadecimal characters"
        ));
    }
    Ok(())
}

pub(in crate::search::semantic_manifest) fn invalid_manifest<T>(
    class: SemanticManifestInvariantClass,
    reason: &str,
) -> Result<T, SemanticGenerationError> {
    Err(SemanticGenerationError::InvalidManifest {
        class,
        reason: reason.to_owned(),
    })
}

pub(in crate::search::semantic_manifest) fn checked_sum(
    left: u64,
    right: u64,
    label: &str,
) -> Result<u64, SemanticGenerationError> {
    left.checked_add(right)
        .ok_or_else(|| SemanticGenerationError::InvalidManifest {
            class: SemanticManifestInvariantClass::Count,
            reason: format!("{label} overflow"),
        })
}

#[derive(Debug)]
pub(in crate::search::semantic_manifest) struct SemanticCanonicalEncoder {
    bytes: Vec<u8>,
}

impl SemanticCanonicalEncoder {
    pub(in crate::search::semantic_manifest) fn new(domain: &[u8]) -> Self {
        let mut encoder = Self { bytes: Vec::new() };
        encoder.bytes(domain);
        encoder
    }

    pub(in crate::search::semantic_manifest) fn finish(self) -> Vec<u8> {
        self.bytes
    }

    pub(in crate::search::semantic_manifest) fn bytes(&mut self, value: &[u8]) {
        self.u64(u64::try_from(value.len()).unwrap_or(u64::MAX));
        self.bytes.extend_from_slice(value);
    }

    pub(in crate::search::semantic_manifest) fn text(&mut self, value: &str) {
        self.bytes(value.as_bytes());
    }

    pub(in crate::search::semantic_manifest) fn optional_text(&mut self, value: Option<&str>) {
        match value {
            Some(value) => {
                self.u8(1);
                self.text(value);
            }
            None => self.u8(0),
        }
    }

    pub(in crate::search::semantic_manifest) fn len(&mut self, value: usize) {
        self.u64(u64::try_from(value).unwrap_or(u64::MAX));
    }

    pub(in crate::search::semantic_manifest) fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    pub(in crate::search::semantic_manifest) fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub(in crate::search::semantic_manifest) fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub(in crate::search::semantic_manifest) fn i64(&mut self, value: i64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }
}

pub(in crate::search::semantic_manifest) fn topology_tag(
    topology: SemanticGenerationTopology,
) -> u8 {
    match topology {
        SemanticGenerationTopology::FastOnly => 1,
        SemanticGenerationTopology::QualityOnly => 2,
        SemanticGenerationTopology::FullProgressive => 3,
    }
}

fn artifact_role_tag(role: SemanticArtifactRole) -> u8 {
    match role {
        SemanticArtifactRole::FastVector => 1,
        SemanticArtifactRole::QualityVector => 2,
        SemanticArtifactRole::FastAnn => 3,
        SemanticArtifactRole::QualityAnn => 4,
    }
}

pub(in crate::search::semantic_manifest) fn encode_corpus_identity(
    encoder: &mut SemanticCanonicalEncoder,
    corpus: &SemanticCorpusSnapshotIdentity,
) {
    encoder.text(&corpus.database_uuid);
    encoder.u64(corpus.source_change_sequence);
    encoder.text(&corpus.schema_contract_sha256);
    encoder.text(&corpus.content_selection_sha256);
    encoder.text(&corpus.canonicalization_sha256);
    encoder.text(&corpus.chunking_sha256);
    encoder.text(&corpus.document_id_contract_sha256);
    encoder.text(&corpus.config_sha256);
    encoder.text(&corpus.corpus_sha256);
    encoder.text(&corpus.ordered_live_docset_sha256);
    encoder.text(&corpus.content_sha256);
    encoder.u64(corpus.document_count);
}

pub(in crate::search::semantic_manifest) fn encode_producer_attestation(
    encoder: &mut SemanticCanonicalEncoder,
    producer: &SemanticProducerAttestation,
) {
    encoder.text(&producer.cass_git_commit);
    encoder.text(&producer.frankensearch_git_commit);
    encoder.text(&producer.cargo_lock_sha256);
    encoder.text(&producer.rust_toolchain);
    encoder.len(producer.enabled_features.len());
    for feature in &producer.enabled_features {
        encoder.text(feature);
    }
    encoder.text(&producer.build_profile);
    encoder.text(&producer.binary_elf_sha256);
}

pub(in crate::search::semantic_manifest) fn encode_generation_artifact(
    encoder: &mut SemanticCanonicalEncoder,
    artifact: &SemanticGenerationArtifact,
) {
    encoder.u8(artifact_role_tag(artifact.role));
    encoder.text(&artifact.relative_path);
    encoder.text(&artifact.artifact_sha256);
    encoder.u64(artifact.size_bytes);
    encoder.text(&artifact.artifact_format);
    encoder.text(&artifact.artifact_parameters_sha256);
    encoder.text(&artifact.embedding_identity.fingerprint);
    match artifact.ann_base.as_ref() {
        Some(binding) => {
            encoder.u8(1);
            encoder.u8(artifact_role_tag(binding.base_role));
            encoder.text(&binding.base_artifact_sha256);
            encoder.text(&binding.base_storage_fingerprint);
            encoder.text(&binding.base_artifact_parameters_sha256);
            encoder.text(&binding.algorithm);
            encoder.text(&binding.metric);
            encoder.text(&binding.parameters_sha256);
        }
        None => encoder.u8(0),
    }
    encoder.u64(artifact.selected_document_count);
    encoder.u64(artifact.covered_document_count);
    encoder.u64(artifact.vector_slot_count);
    encoder.u64(artifact.live_vector_count);
    encoder.u64(artifact.tombstone_vector_count);
    encoder.u64(artifact.wal_entry_count);
    encoder.text(&artifact.generation_corpus_sha256);
    encoder.text(&artifact.covered_live_docset_sha256);
    encoder.text(&artifact.covered_content_sha256);
    let evidence = &artifact.validation;
    encoder.text(&evidence.validator_id);
    encoder.i64(evidence.validated_at_ms);
    encoder.text(&evidence.artifact_sha256);
    encoder.u64(evidence.size_bytes);
    encoder.u64(evidence.selected_document_count);
    encoder.u64(evidence.covered_document_count);
    encoder.u64(evidence.vector_slot_count);
    encoder.u64(evidence.live_vector_count);
    encoder.u64(evidence.tombstone_vector_count);
    encoder.u64(evidence.wal_entry_count);
    encoder.text(&evidence.generation_corpus_sha256);
    encoder.text(&evidence.covered_live_docset_sha256);
    encoder.text(&evidence.covered_content_sha256);
}

pub(in crate::search::semantic_manifest) fn normalized_manifest_relative_path(
    recorded: &str,
) -> Result<PathBuf, String> {
    if recorded.is_empty()
        || recorded.len() > MAX_SEMANTIC_ARTIFACT_PATH_BYTES
        || !recorded.is_ascii()
        || recorded.trim() != recorded
        || recorded.contains('\\')
        || recorded.contains(':')
        || recorded.chars().any(char::is_control)
        || recorded == SEMANTIC_GENERATION_MANIFEST_FILENAME
    {
        return Err(
            "artifact path must be normalized portable ASCII, manifest-relative, and may not name manifest.json"
                .to_owned(),
        );
    }
    let path = Path::new(recorded);
    if path.is_absolute()
        || !path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
    {
        return Err(
            "artifact path contains an absolute, parent, root, or alias component".to_owned(),
        );
    }
    let normalized = path
        .components()
        .filter_map(|component| match component {
            Component::Normal(segment) => segment.to_str(),
            Component::Prefix(_)
            | Component::RootDir
            | Component::CurDir
            | Component::ParentDir => None,
        })
        .collect::<Vec<_>>()
        .join("/");
    if normalized != recorded {
        return Err("artifact path contains repeated or trailing separators".to_owned());
    }
    for segment in normalized.split('/') {
        if !portable_path_segment_is_unambiguous(segment) {
            return Err(
                "artifact path contains a Windows-reserved, ADS, trailing-dot/space, or alias-prone segment"
                    .to_owned(),
            );
        }
    }
    Ok(path.to_path_buf())
}

fn portable_path_segment_is_unambiguous(segment: &str) -> bool {
    if segment.is_empty()
        || segment.ends_with('.')
        || segment.ends_with(' ')
        || segment
            .bytes()
            .any(|byte| matches!(byte, b'<' | b'>' | b'"' | b'|' | b'?' | b'*' | b':'))
    {
        return false;
    }
    let basename = segment
        .split_once('.')
        .map_or(segment, |(basename, _)| basename);
    let upper = basename.to_ascii_uppercase();
    !matches!(upper.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        && !(upper.len() == 4
            && (upper.starts_with("COM") || upper.starts_with("LPT"))
            && matches!(upper.as_bytes()[3], b'1'..=b'9'))
}

pub(in crate::search::semantic_manifest) fn semantic_vector_root(data_dir: &Path) -> PathBuf {
    data_dir.join("vector_index")
}

pub(in crate::search::semantic_manifest) fn semantic_generations_root(data_dir: &Path) -> PathBuf {
    semantic_vector_root(data_dir).join(SEMANTIC_GENERATIONS_DIRNAME)
}

pub(in crate::search::semantic_manifest) fn prepare_semantic_vector_root(
    data_dir: &Path,
    vector_root: &Path,
) -> Result<(), SemanticGenerationError> {
    ensure_directory_chain_without_links(data_dir, data_dir).map_err(|error| {
        SemanticGenerationError::PointerIo {
            source: error.to_string(),
        }
    })?;
    ensure_directory_chain_without_links(data_dir, vector_root).map_err(|error| {
        SemanticGenerationError::PointerIo {
            source: error.to_string(),
        }
    })?;
    let canonical_data =
        data_dir
            .canonicalize()
            .map_err(|error| SemanticGenerationError::PointerIo {
                source: error.to_string(),
            })?;
    let canonical_vector =
        vector_root
            .canonicalize()
            .map_err(|error| SemanticGenerationError::PointerIo {
                source: error.to_string(),
            })?;
    if !canonical_vector.starts_with(canonical_data) {
        return Err(SemanticGenerationError::InvalidPointer {
            reason: "semantic vector root escapes data directory".to_owned(),
        });
    }
    Ok(())
}

pub(in crate::search::semantic_manifest) fn prepare_generation_directory(
    data_dir: &Path,
    generation_dir: &Path,
) -> Result<(), SemanticGenerationError> {
    let vector_root = semantic_vector_root(data_dir);
    prepare_semantic_vector_root(data_dir, &vector_root)?;
    let generations_root = semantic_generations_root(data_dir);
    let generation_id = generation_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("[invalid]")
        .to_owned();
    ensure_directory_chain_without_links(data_dir, &generations_root).map_err(|error| {
        SemanticGenerationError::ManifestIo {
            generation_id: generation_id.clone(),
            source: error.to_string(),
        }
    })?;
    ensure_directory_chain_without_links(data_dir, generation_dir).map_err(|error| {
        SemanticGenerationError::ManifestIo {
            generation_id,
            source: error.to_string(),
        }
    })?;
    validate_existing_generation_directory(data_dir, generation_dir)
}

pub(in crate::search::semantic_manifest) fn validate_existing_generation_directory(
    data_dir: &Path,
    generation_dir: &Path,
) -> Result<(), SemanticGenerationError> {
    let generation_id = generation_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("[invalid]")
        .to_owned();
    reject_existing_path_links(data_dir, generation_dir).map_err(|error| {
        SemanticGenerationError::ManifestIo {
            generation_id: generation_id.clone(),
            source: error.to_string(),
        }
    })?;
    let metadata = match fs::symlink_metadata(generation_dir) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(SemanticGenerationError::MissingManifest { generation_id });
        }
        Err(error) => {
            return Err(SemanticGenerationError::ManifestIo {
                generation_id,
                source: error.to_string(),
            });
        }
    };
    if !metadata.is_dir() {
        return invalid_manifest(
            SemanticManifestInvariantClass::Path,
            "generation root is not a directory",
        );
    }
    let canonical_data =
        data_dir
            .canonicalize()
            .map_err(|error| SemanticGenerationError::ManifestIo {
                generation_id: generation_id.clone(),
                source: error.to_string(),
            })?;
    let canonical_generation =
        generation_dir
            .canonicalize()
            .map_err(|error| SemanticGenerationError::ManifestIo {
                generation_id: generation_id.clone(),
                source: error.to_string(),
            })?;
    if !canonical_generation.starts_with(canonical_data) {
        return invalid_manifest(
            SemanticManifestInvariantClass::Path,
            "generation directory escapes data directory",
        );
    }
    Ok(())
}

pub(in crate::search::semantic_manifest) fn reject_symlink_entry(
    path: &Path,
    label: &str,
) -> Result<(), SemanticGenerationError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata_is_link_or_reparse(&metadata) => invalid_manifest(
            SemanticManifestInvariantClass::Path,
            &format!("{label} must not be a symlink or reparse point"),
        ),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => invalid_manifest(
            SemanticManifestInvariantClass::Path,
            &format!("failed inspecting {label}: {error}"),
        ),
    }
}

/// Splits `path` into the trusted `base` prefix plus the (possibly empty)
/// relative remainder of plain `Normal` components below it.
///
/// The link checks in this module anchor at `base` instead of walking from
/// the filesystem root: components ABOVE the controlled root are not ours to
/// police and cannot legally be link-free everywhere (macOS keeps every
/// standard temp and data path under `/var` and `/tmp`, both of which are
/// symlinks into `/private`). The controlled region — `base` itself and
/// everything below it — is still fully link-checked.
fn relative_below_base<'p>(base: &Path, path: &'p Path) -> std::io::Result<&'p Path> {
    path.strip_prefix(base).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "path escapes its trusted base directory",
        )
    })
}

fn verify_or_create_dir_component(current: &Path) -> std::io::Result<()> {
    match fs::symlink_metadata(current) {
        Ok(metadata) => {
            if metadata_is_link_or_reparse(&metadata) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "directory chain contains a symlink or reparse point",
                ));
            }
            if !metadata.is_dir() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotADirectory,
                    "directory chain contains a non-directory component",
                ));
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(current)?;
            let metadata = fs::symlink_metadata(current)?;
            if metadata_is_link_or_reparse(&metadata) || !metadata.is_dir() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "new directory component was replaced by an unsafe path entry",
                ));
            }
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn ensure_directory_chain_without_links(base: &Path, path: &Path) -> std::io::Result<()> {
    let relative = relative_below_base(base, path)?;
    let mut current = base.to_path_buf();
    verify_or_create_dir_component(&current)?;
    for component in relative.components() {
        let std::path::Component::Normal(part) = component else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "directory chain contains a non-normal path component",
            ));
        };
        current.push(part);
        verify_or_create_dir_component(&current)?;
    }
    Ok(())
}

pub(in crate::search::semantic_manifest) fn reject_existing_path_links(
    base: &Path,
    path: &Path,
) -> std::io::Result<()> {
    let relative = relative_below_base(base, path)?;
    let mut current = base.to_path_buf();
    match fs::symlink_metadata(&current) {
        Ok(metadata) if metadata_is_link_or_reparse(&metadata) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "path contains a symlink or reparse point",
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    }
    for component in relative.components() {
        let std::path::Component::Normal(part) = component else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "path contains a non-normal path component",
            ));
        };
        current.push(part);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata_is_link_or_reparse(&metadata) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "path contains a symlink or reparse point",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[cfg(not(windows))]
pub(in crate::search::semantic_manifest) fn metadata_is_link_or_reparse(
    metadata: &fs::Metadata,
) -> bool {
    metadata.file_type().is_symlink()
}

#[cfg(windows)]
pub(in crate::search::semantic_manifest) fn metadata_is_link_or_reparse(
    metadata: &fs::Metadata,
) -> bool {
    use std::os::windows::fs::MetadataExt;

    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
    metadata.file_type().is_symlink()
        || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

pub(in crate::search::semantic_manifest) fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

pub(in crate::search::semantic_manifest) fn sha256_reader(
    file: &fs::File,
) -> std::io::Result<String> {
    let mut file = file.try_clone()?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::search::semantic_manifest) struct PortableFileIdentity {
    pub(in crate::search::semantic_manifest) volume: u64,
    pub(in crate::search::semantic_manifest) file: u64,
    pub(in crate::search::semantic_manifest) link_count: u64,
}

#[cfg(unix)]
pub(in crate::search::semantic_manifest) fn portable_file_identity(
    _file: &fs::File,
    metadata: &fs::Metadata,
) -> std::io::Result<Option<PortableFileIdentity>> {
    use std::os::unix::fs::MetadataExt;

    Ok(Some(PortableFileIdentity {
        volume: metadata.dev(),
        file: metadata.ino(),
        link_count: metadata.nlink(),
    }))
}

#[cfg(windows)]
#[allow(unsafe_code)]
pub(in crate::search::semantic_manifest) fn portable_file_identity(
    file: &fs::File,
    _metadata: &fs::Metadata,
) -> std::io::Result<Option<PortableFileIdentity>> {
    use std::mem::MaybeUninit;
    use std::os::windows::io::AsRawHandle;

    #[repr(C)]
    struct ByHandleFileInformation {
        file_attributes: u32,
        creation_time_low: u32,
        creation_time_high: u32,
        last_access_time_low: u32,
        last_access_time_high: u32,
        last_write_time_low: u32,
        last_write_time_high: u32,
        volume_serial_number: u32,
        file_size_high: u32,
        file_size_low: u32,
        number_of_links: u32,
        file_index_high: u32,
        file_index_low: u32,
    }

    // SAFETY: declaration matches the documented kernel32 prototype.
    #[allow(unsafe_code)]
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetFileInformationByHandle(
            file: *mut std::ffi::c_void,
            information: *mut ByHandleFileInformation,
        ) -> i32;
    }

    let mut information = MaybeUninit::<ByHandleFileInformation>::uninit();
    // SAFETY: `file` owns a valid Windows file handle, and `information`
    // points to writable storage of exactly the documented C layout.
    if unsafe { GetFileInformationByHandle(file.as_raw_handle().cast(), information.as_mut_ptr()) }
        == 0
    {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: a successful GetFileInformationByHandle call initializes every
    // field of BY_HANDLE_FILE_INFORMATION.
    let information = unsafe { information.assume_init() };
    Ok(Some(PortableFileIdentity {
        volume: u64::from(information.volume_serial_number),
        file: (u64::from(information.file_index_high) << 32)
            | u64::from(information.file_index_low),
        link_count: u64::from(information.number_of_links),
    }))
}

#[cfg(not(any(unix, windows)))]
pub(in crate::search::semantic_manifest) fn portable_file_identity(
    _file: &fs::File,
    _metadata: &fs::Metadata,
) -> std::io::Result<Option<PortableFileIdentity>> {
    Ok(None)
}

pub(in crate::search::semantic_manifest) fn verify_path_still_names_open_file(
    path: &Path,
    expected: Option<PortableFileIdentity>,
) -> std::io::Result<()> {
    let path_file = OpenOptions::new().read(true).open(path)?;
    let path_metadata = path_file.metadata()?;
    if !path_metadata.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "validated artifact path no longer names a regular file",
        ));
    }
    let actual = portable_file_identity(&path_file, &path_metadata)?;
    if expected.is_some() && actual != expected {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "validated artifact path changed file identity during inspection",
        ));
    }
    if actual.is_some_and(|identity| identity.link_count != 1) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "validated artifact acquired an external hard link during inspection",
        ));
    }
    Ok(())
}

pub(in crate::search::semantic_manifest) fn read_bounded_file(
    path: &Path,
    max_bytes: usize,
) -> std::io::Result<Vec<u8>> {
    let file = fs::File::open(path)?;
    let metadata = file.metadata()?;
    let max_bytes_u64 = u64::try_from(max_bytes).unwrap_or(u64::MAX);
    if metadata.len() > max_bytes_u64 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("file exceeds {max_bytes}-byte limit"),
        ));
    }
    let mut bytes = Vec::with_capacity(max_bytes.min(64 * 1024));
    file.take(max_bytes_u64.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() > max_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("file exceeds {max_bytes}-byte limit"),
        ));
    }
    Ok(bytes)
}

pub(in crate::search::semantic_manifest) fn fingerprint_prefix(value: &str) -> &str {
    value.get(..value.len().min(12)).unwrap_or("[invalid]")
}

pub(in crate::search::semantic_manifest) fn optional_fingerprint_prefix(
    value: Option<&str>,
) -> &str {
    value.map_or("[none]", fingerprint_prefix)
}

pub(in crate::search::semantic_manifest) fn elapsed_ms(started: Instant) -> u128 {
    started.elapsed().as_millis()
}

pub(in crate::search::semantic_manifest) fn log_generation_validation_failure(
    pointer: Option<&SemanticCurrentPointerV1>,
    manifest: Option<&SemanticGenerationManifestV1>,
    error: &SemanticGenerationError,
    started: Instant,
) {
    let schema_version = pointer.map_or(0, |pointer| pointer.schema_version);
    let generation = pointer.map_or("[unresolved]", |pointer| {
        fingerprint_prefix(&pointer.generation_id)
    });
    let manifest_sha256 = pointer.map_or("[unresolved]", |pointer| {
        fingerprint_prefix(&pointer.manifest_sha256)
    });
    let build = manifest.map_or("[unresolved]", |manifest| {
        fingerprint_prefix(&manifest.build_id)
    });
    let request = manifest.map_or("[unresolved]", |manifest| {
        optional_fingerprint_prefix(manifest.request_id.as_deref())
    });
    let requested_topology = manifest.map(|manifest| manifest.requested_topology);
    let realized_topology = manifest.map(|manifest| manifest.realized_topology);
    let role = match error {
        SemanticGenerationError::ArtifactIo { role, .. }
        | SemanticGenerationError::ArtifactSizeMismatch { role, .. }
        | SemanticGenerationError::ArtifactDigestMismatch { role, .. }
        | SemanticGenerationError::EmbeddingIdentityMismatch { role, .. } => Some(*role),
        _ => None,
    };
    let artifact = manifest
        .and_then(|manifest| role.and_then(|role| manifest.artifact(role)))
        .map_or("[unresolved]", |artifact| artifact.relative_path.as_str());
    let tier = role.map_or("[unresolved]", SemanticArtifactRole::as_str);
    let coverage = manifest.map_or_else(
        || "[unresolved]".to_owned(),
        |manifest| {
            manifest
                .artifacts
                .iter()
                .filter(|artifact| artifact.role.is_vector())
                .map(|artifact| {
                    format!(
                        "{}:{}/{}",
                        artifact.role.as_str(),
                        artifact.covered_document_count,
                        artifact.selected_document_count
                    )
                })
                .collect::<Vec<_>>()
                .join(",")
        },
    );
    let (expected_identity, actual_identity) = match error {
        SemanticGenerationError::ManifestDigestMismatch { expected, actual }
        | SemanticGenerationError::StaleCorpus { expected, actual }
        | SemanticGenerationError::ArtifactDigestMismatch {
            expected, actual, ..
        }
        | SemanticGenerationError::EmbeddingIdentityMismatch {
            expected, actual, ..
        }
        | SemanticGenerationError::ConcurrentPublishConflict { expected, actual } => {
            (fingerprint_prefix(expected), fingerprint_prefix(actual))
        }
        _ => ("[unavailable]", "[unavailable]"),
    };
    tracing::warn!(
        event = "semantic.manifest.validate",
        schema_version,
        generation,
        build,
        request,
        requested_topology = ?requested_topology,
        realized_topology = ?realized_topology,
        tier,
        coverage,
        artifact,
        manifest_sha256,
        expected_identity,
        actual_identity,
        reason_code = error.reason_code(),
        recovery_action = ?error.recovery_action(),
        elapsed_ms = elapsed_ms(started),
        "semantic generation validation failed closed"
    );
}
