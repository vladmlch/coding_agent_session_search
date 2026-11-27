use super::atomic_io::{persist_named_temp, sync_directory, sync_parent_directory};
use super::generation_pointer::load_manifest_file;
use super::generation_types::{
    MAX_SEMANTIC_ARTIFACTS, MAX_SEMANTIC_MANIFEST_BYTES, SEMANTIC_GENERATION_MANIFEST_FILENAME,
    SEMANTIC_GENERATION_MANIFEST_SCHEMA_VERSION, SemanticArtifactRole, SemanticGenerationArtifact,
    SemanticGenerationError, SemanticGenerationManifestV1, SemanticGenerationStatus,
    SemanticManifestInvariantClass,
};
use super::generation_validation::{
    SemanticCanonicalEncoder, checked_sum, elapsed_ms, encode_corpus_identity,
    encode_generation_artifact, encode_producer_attestation, fingerprint_prefix, invalid_manifest,
    normalized_manifest_relative_path, optional_fingerprint_prefix, portable_file_identity,
    prepare_generation_directory, reject_existing_path_links, reject_symlink_entry,
    required_vector_roles, semantic_generations_root, sha256_hex, sha256_reader, topology_tag,
    validate_corpus_identity, validate_existing_generation_directory, validate_generation_artifact,
    validate_generation_id, validate_identifier, validate_producer_attestation,
    verify_path_still_names_open_file,
};
use super::shards::{self, SEMANTIC_SHARDED_GENERATION_MANIFEST_SCHEMA_VERSION};
use super::{BTreeMap, BTreeSet, Instant, IoWrite, OpenOptions, Path, PathBuf, fs, wal_path_for};

impl SemanticGenerationManifestV1 {
    /// Deterministic compact JSON used for storage and inspection.
    ///
    /// Integrity never depends on serde's field ordering. [`Self::sha256`]
    /// hashes the domain-separated binary transcript from
    /// `Self::canonical_digest_bytes` instead.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, SemanticGenerationError> {
        let mut bytes =
            serde_json::to_vec(self).map_err(|error| SemanticGenerationError::Serialize {
                source: error.to_string(),
            })?;
        bytes.push(b'\n');
        if bytes.len() > MAX_SEMANTIC_MANIFEST_BYTES {
            return Err(SemanticGenerationError::Serialize {
                source: "semantic generation manifest exceeds its size limit".to_owned(),
            });
        }
        Ok(bytes)
    }

    fn canonical_digest_bytes(&self) -> Vec<u8> {
        let domain: &[u8] =
            if self.schema_version == SEMANTIC_SHARDED_GENERATION_MANIFEST_SCHEMA_VERSION {
                b"cass.semantic-generation-manifest.v2"
            } else {
                b"cass.semantic-generation-manifest.v1"
            };
        let mut encoder = SemanticCanonicalEncoder::new(domain);
        encoder.u32(self.schema_version);
        encoder.text(&self.generation_id);
        encoder.text(&self.build_id);
        encoder.optional_text(self.request_id.as_deref());
        encoder.optional_text(self.previous_generation_id.as_deref());
        encoder.i64(self.created_at_ms);
        encoder.i64(self.completed_at_ms);
        encoder.i64(self.sealed_at_ms);
        encoder.u8(topology_tag(self.requested_topology));
        encoder.u8(topology_tag(self.realized_topology));
        encoder.u8(match self.status {
            SemanticGenerationStatus::Building => 1,
            SemanticGenerationStatus::Sealed => 2,
        });
        encode_corpus_identity(&mut encoder, &self.corpus);
        encode_producer_attestation(&mut encoder, &self.producer);
        encoder.u64(self.selected_document_count);
        encoder.u64(self.total_vector_slot_count_across_tiers);
        encoder.u64(self.total_live_vector_count_across_tiers);
        encoder.u64(self.total_tombstone_vector_count_across_tiers);
        encoder.u64(self.total_wal_entry_count_across_tiers);
        encoder.len(self.artifacts.len());
        for artifact in &self.artifacts {
            encode_generation_artifact(&mut encoder, artifact);
            if self.schema_version == SEMANTIC_SHARDED_GENERATION_MANIFEST_SCHEMA_VERSION {
                shards::encode_shard(&mut encoder, artifact.shard.as_ref());
            }
        }
        encoder.finish()
    }

    pub fn sha256(&self) -> Result<String, SemanticGenerationError> {
        Ok(sha256_hex(&self.canonical_digest_bytes()))
    }

    pub fn generation_dir(&self, data_dir: &Path) -> Result<PathBuf, SemanticGenerationError> {
        validate_generation_id(&self.generation_id).map_err(|reason| {
            SemanticGenerationError::InvalidManifest {
                class: SemanticManifestInvariantClass::Path,
                reason,
            }
        })?;
        Ok(semantic_generations_root(data_dir).join(&self.generation_id))
    }

    pub fn manifest_path(&self, data_dir: &Path) -> Result<PathBuf, SemanticGenerationError> {
        Ok(self
            .generation_dir(data_dir)?
            .join(SEMANTIC_GENERATION_MANIFEST_FILENAME))
    }

    /// Validate every authoritative field without consulting the filesystem.
    pub fn validate(&self) -> Result<(), SemanticGenerationError> {
        if !(SEMANTIC_GENERATION_MANIFEST_SCHEMA_VERSION
            ..=SEMANTIC_SHARDED_GENERATION_MANIFEST_SCHEMA_VERSION)
            .contains(&self.schema_version)
        {
            if self.schema_version > SEMANTIC_SHARDED_GENERATION_MANIFEST_SCHEMA_VERSION {
                return Err(SemanticGenerationError::UnsupportedManifestSchema {
                    found: u64::from(self.schema_version),
                    supported: u64::from(SEMANTIC_SHARDED_GENERATION_MANIFEST_SCHEMA_VERSION),
                });
            }
            return invalid_manifest(
                SemanticManifestInvariantClass::HistoricalSchema,
                &format!(
                    "unsupported historical schema {}; expected {}",
                    self.schema_version, SEMANTIC_GENERATION_MANIFEST_SCHEMA_VERSION
                ),
            );
        }

        validate_generation_id(&self.generation_id).map_err(|reason| {
            SemanticGenerationError::InvalidManifest {
                class: SemanticManifestInvariantClass::Path,
                reason,
            }
        })?;
        if let Some(previous) = self.previous_generation_id.as_deref() {
            validate_generation_id(previous).map_err(|reason| {
                SemanticGenerationError::InvalidManifest {
                    class: SemanticManifestInvariantClass::Path,
                    reason,
                }
            })?;
            if previous == self.generation_id {
                return invalid_manifest(
                    SemanticManifestInvariantClass::Topology,
                    "previous generation must differ from current generation",
                );
            }
        }
        validate_generation_id(&self.build_id).map_err(|reason| {
            SemanticGenerationError::InvalidManifest {
                class: SemanticManifestInvariantClass::Provenance,
                reason: format!("invalid build_id: {reason}"),
            }
        })?;
        if let Some(request_id) = self.request_id.as_deref() {
            validate_identifier(
                SemanticManifestInvariantClass::Provenance,
                "request_id",
                request_id,
            )?;
        }
        if self.created_at_ms < 0
            || self.completed_at_ms < self.created_at_ms
            || self.sealed_at_ms < self.completed_at_ms
        {
            return invalid_manifest(
                SemanticManifestInvariantClass::Timestamp,
                "timestamps must satisfy 0 <= created_at <= completed_at <= sealed_at",
            );
        }
        if !self
            .realized_topology
            .is_valid_realization_of(self.requested_topology)
        {
            return invalid_manifest(
                SemanticManifestInvariantClass::Topology,
                "realized topology is not a subset of requested topology",
            );
        }
        if self.status != SemanticGenerationStatus::Sealed {
            return Err(SemanticGenerationError::BuildingGeneration {
                generation_id: self.generation_id.clone(),
                build_id: self.build_id.clone(),
            });
        }

        validate_corpus_identity(&self.corpus)?;
        validate_producer_attestation(&self.producer)?;
        if self.selected_document_count != self.corpus.document_count {
            return invalid_manifest(
                SemanticManifestInvariantClass::Count,
                "manifest and corpus selected-document counts differ",
            );
        }
        if self.selected_document_count == 0 {
            return invalid_manifest(
                SemanticManifestInvariantClass::Count,
                "sealed generations must select at least one document",
            );
        }
        if self.artifacts.is_empty() {
            return invalid_manifest(
                SemanticManifestInvariantClass::Topology,
                "sealed generations must declare artifacts",
            );
        }
        let artifact_limit = if self.schema_version == SEMANTIC_GENERATION_MANIFEST_SCHEMA_VERSION {
            MAX_SEMANTIC_ARTIFACTS
        } else {
            shards::MAX_SHARDS_PER_TIER * 4
        };
        if self.artifacts.len() > artifact_limit {
            return invalid_manifest(
                SemanticManifestInvariantClass::Topology,
                "semantic generation declares too many artifact roles",
            );
        }

        let mut roles = BTreeSet::new();
        let mut paths = BTreeSet::new();
        let mut portable_paths = BTreeSet::new();
        let mut previous_role = None;
        for artifact in &self.artifacts {
            let key = (
                artifact.role,
                artifact.shard.as_ref().map_or(0, |shard| shard.ordinal),
            );
            if previous_role.is_some_and(|previous| previous >= key) {
                return invalid_manifest(
                    SemanticManifestInvariantClass::Topology,
                    "artifacts must be strictly sorted by role and shard ordinal with no duplicates",
                );
            }
            previous_role = Some(key);
            roles.insert(artifact.role);
            let normalized =
                normalized_manifest_relative_path(&artifact.relative_path).map_err(|reason| {
                    SemanticGenerationError::InvalidManifest {
                        class: SemanticManifestInvariantClass::Path,
                        reason,
                    }
                })?;
            if !paths.insert(normalized) {
                return invalid_manifest(
                    SemanticManifestInvariantClass::Path,
                    "duplicate or lexically aliased artifact path",
                );
            }
            if !portable_paths.insert(artifact.relative_path.to_ascii_lowercase()) {
                return invalid_manifest(
                    SemanticManifestInvariantClass::Path,
                    "artifact paths collide under portable case-insensitive lookup",
                );
            }
            validate_generation_artifact(
                artifact,
                &self.corpus,
                self.created_at_ms,
                self.completed_at_ms,
                self.sealed_at_ms,
            )?;
        }

        shards::validate(self)?;

        for required in required_vector_roles(self.realized_topology) {
            if !roles.contains(&required) {
                return invalid_manifest(
                    SemanticManifestInvariantClass::Topology,
                    &format!(
                        "realized topology is missing required role {}",
                        required.as_str(),
                    ),
                );
            }
        }
        if !self.realized_topology.includes_fast()
            && (roles.contains(&SemanticArtifactRole::FastVector)
                || roles.contains(&SemanticArtifactRole::FastAnn))
        {
            return invalid_manifest(
                SemanticManifestInvariantClass::Topology,
                "fast artifacts contradict realized topology",
            );
        }
        if !self.realized_topology.includes_quality()
            && (roles.contains(&SemanticArtifactRole::QualityVector)
                || roles.contains(&SemanticArtifactRole::QualityAnn))
        {
            return invalid_manifest(
                SemanticManifestInvariantClass::Topology,
                "quality artifacts contradict realized topology",
            );
        }

        for ann in self
            .artifacts
            .iter()
            .filter(|artifact| !artifact.role.is_vector())
        {
            let ann_role = ann.role;
            {
                let Some(base_role) = ann_role.base_vector_role() else {
                    return invalid_manifest(
                        SemanticManifestInvariantClass::AnnBinding,
                        "internal artifact-role contract is inconsistent",
                    );
                };
                let ordinal = ann.shard.as_ref().map_or(0, |shard| shard.ordinal);
                let Some(base) = self.artifact_at(base_role, ordinal) else {
                    return invalid_manifest(
                        SemanticManifestInvariantClass::AnnBinding,
                        "ANN artifact has no manifest-declared base vector",
                    );
                };
                let Some(binding) = ann.ann_base.as_ref() else {
                    return invalid_manifest(
                        SemanticManifestInvariantClass::AnnBinding,
                        "ANN artifact is missing its exact base binding",
                    );
                };
                if ann.embedding_identity != base.embedding_identity {
                    return Err(SemanticGenerationError::EmbeddingIdentityMismatch {
                        role: ann_role,
                        expected: base.embedding_identity.fingerprint.clone(),
                        actual: ann.embedding_identity.fingerprint.clone(),
                        reason: "ANN embedding identity differs from its vector base".to_owned(),
                    });
                }
                if ann.shard != base.shard
                    || binding.base_role != base_role
                    || binding.base_artifact_sha256 != base.artifact_sha256
                    || binding.base_storage_fingerprint
                        != base.embedding_identity.identity.storage.fingerprint()
                    || binding.base_artifact_parameters_sha256 != base.artifact_parameters_sha256
                    || binding.parameters_sha256 != ann.artifact_parameters_sha256
                    || ann.selected_document_count != base.selected_document_count
                    || ann.covered_document_count != base.covered_document_count
                    || ann.vector_slot_count != base.vector_slot_count
                    || ann.live_vector_count != base.live_vector_count
                    || ann.tombstone_vector_count != base.tombstone_vector_count
                    || ann.generation_corpus_sha256 != base.generation_corpus_sha256
                    || ann.covered_live_docset_sha256 != base.covered_live_docset_sha256
                    || ann.covered_content_sha256 != base.covered_content_sha256
                {
                    return invalid_manifest(
                        SemanticManifestInvariantClass::AnnBinding,
                        "ANN artifact does not bind the exact base bytes, storage, parameters, identity, counts, and covered corpus",
                    );
                }
            }
        }

        let mut vector_artifacts = self
            .artifacts
            .iter()
            .filter(|artifact| artifact.role.is_vector());
        let (vector_count, live_count, tombstone_count, wal_count) =
            vector_artifacts.try_fold((0_u64, 0_u64, 0_u64, 0_u64), |totals, artifact| {
                Ok::<_, SemanticGenerationError>((
                    checked_sum(totals.0, artifact.vector_slot_count, "vector slot count")?,
                    checked_sum(totals.1, artifact.live_vector_count, "live vector count")?,
                    checked_sum(
                        totals.2,
                        artifact.tombstone_vector_count,
                        "tombstone vector count",
                    )?,
                    checked_sum(totals.3, artifact.wal_entry_count, "WAL entry count")?,
                ))
            })?;
        if (
            self.total_vector_slot_count_across_tiers,
            self.total_live_vector_count_across_tiers,
            self.total_tombstone_vector_count_across_tiers,
            self.total_wal_entry_count_across_tiers,
        ) != (vector_count, live_count, tombstone_count, wal_count)
        {
            return invalid_manifest(
                SemanticManifestInvariantClass::Count,
                "manifest across-tier aggregate counts do not match vector artifacts",
            );
        }

        Ok(())
    }

    /// Compatibility accessor for a single-artifact role. Never silently choose
    /// one shard from a multi-shard tier; use artifacts_for/artifact_at instead.
    pub fn artifact(&self, role: SemanticArtifactRole) -> Option<&SemanticGenerationArtifact> {
        let mut artifacts = self.artifacts_for(role);
        let first = artifacts.next()?;
        artifacts.next().is_none().then_some(first)
    }

    pub fn artifacts_for(
        &self,
        role: SemanticArtifactRole,
    ) -> impl Iterator<Item = &SemanticGenerationArtifact> {
        self.artifacts
            .iter()
            .filter(move |artifact| artifact.role == role)
    }

    pub fn artifact_at(
        &self,
        role: SemanticArtifactRole,
        ordinal: u32,
    ) -> Option<&SemanticGenerationArtifact> {
        self.artifacts_for(role)
            .find(|artifact| artifact.shard.as_ref().map_or(0, |shard| shard.ordinal) == ordinal)
    }

    /// Durably seal an immutable generation manifest.
    ///
    /// Every artifact and its containing directory is synced first. The
    /// manifest is then written to a same-directory temporary file, synced,
    /// and atomically published without clobbering an existing manifest.
    /// Retrying the exact semantic manifest is idempotent; reusing a generation
    /// ID for different content fails closed.
    pub fn write_immutable(&self, data_dir: &Path) -> Result<String, SemanticGenerationError> {
        self.write_immutable_with_hook(data_dir, |_| Ok(()))
    }

    pub(in crate::search::semantic_manifest) fn write_immutable_with_hook(
        &self,
        data_dir: &Path,
        mut after_checkpoint: impl FnMut(ManifestSealCheckpoint) -> Result<(), SemanticGenerationError>,
    ) -> Result<String, SemanticGenerationError> {
        let started = Instant::now();
        self.validate()?;
        let generation_dir = self.generation_dir(data_dir)?;
        prepare_generation_directory(data_dir, &generation_dir)?;
        self.validate_artifacts_on_disk(data_dir, true)?;
        after_checkpoint(ManifestSealCheckpoint::ArtifactDurabilityEstablished)?;
        let manifest_path = generation_dir.join(SEMANTIC_GENERATION_MANIFEST_FILENAME);
        reject_symlink_entry(&manifest_path, "generation manifest")?;
        let bytes = self.canonical_bytes()?;
        let digest = self.sha256()?;
        let mut temp = tempfile::Builder::new()
            .prefix(".manifest.seal.")
            .tempfile_in(&generation_dir)
            .map_err(|error| SemanticGenerationError::ManifestIo {
                generation_id: self.generation_id.clone(),
                source: error.to_string(),
            })?;
        temp.write_all(&bytes)
            .map_err(|error| SemanticGenerationError::ManifestIo {
                generation_id: self.generation_id.clone(),
                source: error.to_string(),
            })?;
        after_checkpoint(ManifestSealCheckpoint::TemporaryBytesWritten)?;
        temp.as_file()
            .sync_all()
            .map_err(|error| SemanticGenerationError::ManifestIo {
                generation_id: self.generation_id.clone(),
                source: error.to_string(),
            })?;
        after_checkpoint(ManifestSealCheckpoint::TemporaryFileSynced)?;

        match persist_named_temp(temp, &manifest_path, false) {
            Ok(file) => {
                file.sync_all()
                    .map_err(|error| SemanticGenerationError::ManifestIo {
                        generation_id: self.generation_id.clone(),
                        source: error.to_string(),
                    })?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let existing = load_manifest_file(&manifest_path, &self.generation_id)?;
                if existing != *self || existing.sha256()? != digest {
                    return Err(SemanticGenerationError::ImmutableManifestConflict {
                        generation_id: self.generation_id.clone(),
                    });
                }
            }
            Err(error) => {
                return Err(SemanticGenerationError::ManifestIo {
                    generation_id: self.generation_id.clone(),
                    source: error.to_string(),
                });
            }
        };
        after_checkpoint(ManifestSealCheckpoint::ManifestPublished)?;
        sync_parent_directory(&manifest_path).map_err(|error| {
            SemanticGenerationError::ManifestIo {
                generation_id: self.generation_id.clone(),
                source: error.to_string(),
            }
        })?;
        after_checkpoint(ManifestSealCheckpoint::ParentDirectorySynced)?;

        for artifact in &self.artifacts {
            tracing::info!(
                event = "semantic.manifest.artifact.validate",
                schema_version = self.schema_version,
                generation = %fingerprint_prefix(&self.generation_id),
                build = %fingerprint_prefix(&self.build_id),
                request = %optional_fingerprint_prefix(self.request_id.as_deref()),
                requested_topology = ?self.requested_topology,
                realized_topology = ?self.realized_topology,
                tier = artifact.role.as_str(),
                artifact = %artifact.relative_path,
                embedding_fingerprint =
                    %fingerprint_prefix(&artifact.embedding_identity.fingerprint),
                coverage_numerator = artifact.covered_document_count,
                coverage_denominator = artifact.selected_document_count,
                artifact_sha256 = %fingerprint_prefix(&artifact.artifact_sha256),
                "validated and synced manifest-declared semantic artifact"
            );
        }

        tracing::info!(
            event = "semantic.manifest.seal",
            schema_version = self.schema_version,
            generation = %fingerprint_prefix(&self.generation_id),
            build = %fingerprint_prefix(&self.build_id),
            request = %optional_fingerprint_prefix(self.request_id.as_deref()),
            requested_topology = ?self.requested_topology,
            realized_topology = ?self.realized_topology,
            manifest_sha256 = %fingerprint_prefix(&digest),
            selected_document_count = self.selected_document_count,
            total_vector_slot_count_across_tiers =
                self.total_vector_slot_count_across_tiers,
            artifact_count = self.artifacts.len(),
            elapsed_ms = elapsed_ms(started),
            "sealed immutable semantic generation manifest"
        );
        Ok(digest)
    }

    pub(in crate::search::semantic_manifest) fn validate_artifacts_on_disk(
        &self,
        data_dir: &Path,
        sync_for_seal: bool,
    ) -> Result<BTreeMap<SemanticArtifactRole, PathBuf>, SemanticGenerationError> {
        let generation_dir = self.generation_dir(data_dir)?;
        validate_existing_generation_directory(data_dir, &generation_dir)?;
        let canonical_root =
            generation_dir
                .canonicalize()
                .map_err(|error| SemanticGenerationError::ManifestIo {
                    generation_id: self.generation_id.clone(),
                    source: error.to_string(),
                })?;
        let mut canonical_artifacts = BTreeSet::new();
        let mut physical_artifacts = BTreeSet::new();
        let mut synced_parent_directories = BTreeSet::new();
        let mut resolved = BTreeMap::new();

        for artifact in &self.artifacts {
            let relative =
                normalized_manifest_relative_path(&artifact.relative_path).map_err(|reason| {
                    SemanticGenerationError::InvalidManifest {
                        class: SemanticManifestInvariantClass::Path,
                        reason,
                    }
                })?;
            let joined = generation_dir.join(relative);
            reject_existing_path_links(&generation_dir, &joined).map_err(|error| {
                SemanticGenerationError::ArtifactIo {
                    role: artifact.role,
                    source: error.to_string(),
                }
            })?;
            if artifact.role.is_vector() {
                let wal_path = wal_path_for(&joined);
                match fs::symlink_metadata(&wal_path) {
                    Ok(_) => {
                        return Err(SemanticGenerationError::ArtifactIo {
                            role: artifact.role,
                            source: "immutable vector artifact has a WAL sidecar".to_owned(),
                        });
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => {
                        return Err(SemanticGenerationError::ArtifactIo {
                            role: artifact.role,
                            source: format!("failed inspecting immutable WAL sidecar: {error}"),
                        });
                    }
                }
            }
            let file = OpenOptions::new()
                .read(true)
                .write(sync_for_seal)
                .open(&joined)
                .map_err(|error| SemanticGenerationError::ArtifactIo {
                    role: artifact.role,
                    source: error.to_string(),
                })?;
            let metadata =
                file.metadata()
                    .map_err(|error| SemanticGenerationError::ArtifactIo {
                        role: artifact.role,
                        source: error.to_string(),
                    })?;
            if !metadata.is_file() {
                return Err(SemanticGenerationError::ArtifactIo {
                    role: artifact.role,
                    source: "manifest-declared artifact is not a regular file".to_owned(),
                });
            }
            let canonical =
                joined
                    .canonicalize()
                    .map_err(|error| SemanticGenerationError::ArtifactIo {
                        role: artifact.role,
                        source: error.to_string(),
                    })?;
            if !canonical.starts_with(&canonical_root) {
                return Err(SemanticGenerationError::ArtifactIo {
                    role: artifact.role,
                    source: "manifest-declared artifact escapes the generation root".to_owned(),
                });
            }
            let opened_identity = portable_file_identity(&file, &metadata).map_err(|error| {
                SemanticGenerationError::ArtifactIo {
                    role: artifact.role,
                    source: error.to_string(),
                }
            })?;
            if opened_identity.is_some_and(|identity| identity.link_count != 1) {
                return Err(SemanticGenerationError::InvalidManifest {
                    class: SemanticManifestInvariantClass::Path,
                    reason: "manifest-declared artifacts must not have external hard links"
                        .to_owned(),
                });
            }
            verify_path_still_names_open_file(&canonical, opened_identity).map_err(|error| {
                SemanticGenerationError::ArtifactIo {
                    role: artifact.role,
                    source: error.to_string(),
                }
            })?;
            if !canonical_artifacts.insert(canonical.clone()) {
                return Err(SemanticGenerationError::InvalidManifest {
                    class: SemanticManifestInvariantClass::Path,
                    reason: "multiple artifact roles resolve to the same file".to_owned(),
                });
            }
            if let Some(identity) = opened_identity
                && !physical_artifacts.insert(identity)
            {
                return Err(SemanticGenerationError::InvalidManifest {
                    class: SemanticManifestInvariantClass::Path,
                    reason: "multiple artifact roles are hard-link aliases of one file".to_owned(),
                });
            }
            if metadata.len() != artifact.size_bytes {
                return Err(SemanticGenerationError::ArtifactSizeMismatch {
                    role: artifact.role,
                    expected: artifact.size_bytes,
                    actual: metadata.len(),
                });
            }
            let actual_digest =
                sha256_reader(&file).map_err(|error| SemanticGenerationError::ArtifactIo {
                    role: artifact.role,
                    source: error.to_string(),
                })?;
            if actual_digest != artifact.artifact_sha256 {
                return Err(SemanticGenerationError::ArtifactDigestMismatch {
                    role: artifact.role,
                    expected: artifact.artifact_sha256.clone(),
                    actual: actual_digest,
                });
            }
            if sync_for_seal {
                file.sync_all()
                    .map_err(|error| SemanticGenerationError::ArtifactIo {
                        role: artifact.role,
                        source: error.to_string(),
                    })?;
                if let Some(parent) = canonical.parent() {
                    let relative_parent =
                        parent.strip_prefix(&canonical_root).map_err(|error| {
                            SemanticGenerationError::ArtifactIo {
                                role: artifact.role,
                                source: error.to_string(),
                            }
                        })?;
                    let mut directory = canonical_root.clone();
                    if synced_parent_directories.insert(directory.clone()) {
                        sync_directory(&directory).map_err(|error| {
                            SemanticGenerationError::ArtifactIo {
                                role: artifact.role,
                                source: error.to_string(),
                            }
                        })?;
                    }
                    for component in relative_parent.components() {
                        directory.push(component.as_os_str());
                        if synced_parent_directories.insert(directory.clone()) {
                            sync_directory(&directory).map_err(|error| {
                                SemanticGenerationError::ArtifactIo {
                                    role: artifact.role,
                                    source: error.to_string(),
                                }
                            })?;
                        }
                    }
                }
            }
            verify_path_still_names_open_file(&canonical, opened_identity).map_err(|error| {
                SemanticGenerationError::ArtifactIo {
                    role: artifact.role,
                    source: error.to_string(),
                }
            })?;
            // The historical role-keyed map can describe only a singleton.
            // All shards were still validated above; never expose one arbitrary
            // shard through the legacy accessor.
            if self.artifact(artifact.role).is_some() {
                resolved.insert(artifact.role, canonical);
            }
        }
        Ok(resolved)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::search::semantic_manifest) enum ManifestSealCheckpoint {
    ArtifactDurabilityEstablished,
    TemporaryBytesWritten,
    TemporaryFileSynced,
    ManifestPublished,
    ParentDirectorySynced,
}
