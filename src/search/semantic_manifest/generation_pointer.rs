use super::atomic_io::{persist_named_temp, sync_parent_directory};
use super::generation_types::{
    MAX_SEMANTIC_MANIFEST_BYTES, MAX_SEMANTIC_POINTER_BYTES, SEMANTIC_CURRENT_POINTER_FILENAME,
    SEMANTIC_CURRENT_POINTER_LOCK_FILENAME, SEMANTIC_CURRENT_POINTER_SCHEMA_VERSION,
    SEMANTIC_CURRENT_POINTER_TEMP_PREFIX, SEMANTIC_GENERATION_MANIFEST_FILENAME,
    SEMANTIC_GENERATION_MANIFEST_SCHEMA_VERSION, SEMANTIC_GENERATIONS_DIRNAME,
    SEMANTIC_STAGING_DIRNAME, SemanticCorpusSnapshotIdentity, SemanticCurrentPointerV1,
    SemanticExpectedCurrentV1, SemanticGenerationDiskStateV1, SemanticGenerationError,
    SemanticGenerationManifestV1, SemanticManifestInvariantClass, ValidatedSemanticGeneration,
};
use super::generation_validation::{
    elapsed_ms, fingerprint_prefix, invalid_manifest, log_generation_validation_failure,
    metadata_is_link_or_reparse, optional_fingerprint_prefix, prepare_semantic_vector_root,
    read_bounded_file, reject_existing_path_links, reject_symlink_entry, semantic_generations_root,
    semantic_vector_root, validate_existing_generation_directory, validate_generation_id,
    validate_sha256,
};
use super::selection;
use super::shards::SEMANTIC_SHARDED_GENERATION_MANIFEST_SCHEMA_VERSION;
use super::{
    EMBEDDING_INPUT_CONTRACT_SCHEMA_V1, EMBEDDING_PRODUCER_ATTESTATION_SCHEMA_V1,
    EMBEDDING_SPACE_IDENTITY_SCHEMA_V1, FileExt, Instant, IoWrite, OpenOptions, OsStr, Path,
    PathBuf, VECTOR_STORAGE_IDENTITY_SCHEMA_V1, fs,
};

impl SemanticCurrentPointerV1 {
    pub fn path(data_dir: &Path) -> PathBuf {
        semantic_vector_root(data_dir).join(SEMANTIC_CURRENT_POINTER_FILENAME)
    }

    pub fn for_manifest(
        manifest: &SemanticGenerationManifestV1,
        selected_at_ms: i64,
        selection_epoch: u64,
    ) -> Result<Self, SemanticGenerationError> {
        manifest.validate()?;
        if selected_at_ms < manifest.sealed_at_ms {
            return Err(SemanticGenerationError::InvalidPointer {
                reason: "selection timestamp predates immutable generation seal".to_owned(),
            });
        }
        if selection_epoch == 0 {
            return Err(SemanticGenerationError::InvalidPointer {
                reason: "selection_epoch must be positive".to_owned(),
            });
        }
        Ok(Self {
            schema_version: SEMANTIC_CURRENT_POINTER_SCHEMA_VERSION,
            generation_id: manifest.generation_id.clone(),
            manifest_sha256: manifest.sha256()?,
            selection_epoch,
            selected_at_ms,
        })
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>, SemanticGenerationError> {
        let mut bytes =
            serde_json::to_vec(self).map_err(|error| SemanticGenerationError::Serialize {
                source: error.to_string(),
            })?;
        bytes.push(b'\n');
        if bytes.len() > MAX_SEMANTIC_POINTER_BYTES {
            return Err(SemanticGenerationError::Serialize {
                source: "semantic current pointer exceeds its size limit".to_owned(),
            });
        }
        Ok(bytes)
    }

    pub fn validate(&self) -> Result<(), SemanticGenerationError> {
        if self.schema_version != SEMANTIC_CURRENT_POINTER_SCHEMA_VERSION {
            if self.schema_version > SEMANTIC_CURRENT_POINTER_SCHEMA_VERSION {
                return Err(SemanticGenerationError::UnsupportedPointerSchema {
                    found: u64::from(self.schema_version),
                    supported: u64::from(SEMANTIC_CURRENT_POINTER_SCHEMA_VERSION),
                });
            }
            return Err(SemanticGenerationError::InvalidPointer {
                reason: format!(
                    "unsupported historical pointer schema {}; expected {}",
                    self.schema_version, SEMANTIC_CURRENT_POINTER_SCHEMA_VERSION
                ),
            });
        }
        validate_generation_id(&self.generation_id)
            .map_err(|reason| SemanticGenerationError::InvalidPointer { reason })?;
        validate_sha256("manifest_sha256", &self.manifest_sha256)
            .map_err(|reason| SemanticGenerationError::InvalidPointer { reason })?;
        if self.selection_epoch == 0 {
            return Err(SemanticGenerationError::InvalidPointer {
                reason: "selection_epoch must be positive".to_owned(),
            });
        }
        if self.selected_at_ms < 0 {
            return Err(SemanticGenerationError::InvalidPointer {
                reason: "selected_at_ms must be non-negative".to_owned(),
            });
        }
        Ok(())
    }

    /// Atomically select an already-sealed, fully verified generation with a
    /// serialized compare-and-swap precondition.
    ///
    /// `manifest.previous_generation_id` records immutable build lineage; it
    /// is deliberately not the publication precondition because rollback may
    /// select an older generation. `expected` is the sole current-state CAS.
    pub fn publish_atomic(
        &self,
        data_dir: &Path,
        expected: &SemanticExpectedCurrentV1,
    ) -> Result<(), SemanticGenerationError> {
        self.publish_atomic_with_hook(data_dir, expected, |_| Ok(()))
    }

    pub(in crate::search::semantic_manifest) fn publish_atomic_with_hook(
        &self,
        data_dir: &Path,
        expected: &SemanticExpectedCurrentV1,
        mut after_checkpoint: impl FnMut(
            PointerPublishCheckpoint,
        ) -> Result<(), SemanticGenerationError>,
    ) -> Result<(), SemanticGenerationError> {
        let started = Instant::now();
        self.validate()?;
        let root = semantic_vector_root(data_dir);
        prepare_semantic_vector_root(data_dir, &root)?;
        let lock_path = root.join(SEMANTIC_CURRENT_POINTER_LOCK_FILENAME);
        let lock_file = open_semantic_publish_lock(&root, &lock_path)?;
        lock_file
            .lock_exclusive()
            .map_err(|error| SemanticGenerationError::PublishLockIo {
                source: error.to_string(),
            })?;
        let _lock = SemanticPublishLock(lock_file);

        let prior = read_current_pointer_for_cas(data_dir)?;
        verify_publish_precondition(expected, prior.as_ref())?;
        let required_selection_epoch = match prior.as_ref() {
            Some(prior) => prior.selection_epoch.checked_add(1).ok_or_else(|| {
                SemanticGenerationError::InvalidPointer {
                    reason: "selection epoch is exhausted".to_owned(),
                }
            })?,
            None => 1,
        };
        if self.selection_epoch != required_selection_epoch {
            return Err(SemanticGenerationError::SelectionEpochNotAdvanced {
                prior_selection_epoch: prior.as_ref().map_or(0, |pointer| pointer.selection_epoch),
                proposed_selection_epoch: self.selection_epoch,
            });
        }

        let loaded = load_manifest_selected_by_pointer(data_dir, self)?;
        if self.selected_at_ms < loaded.manifest.sealed_at_ms {
            return Err(SemanticGenerationError::InvalidPointer {
                reason: "selection timestamp predates immutable generation seal".to_owned(),
            });
        }
        loaded
            .manifest
            .validate_artifacts_on_disk(data_dir, false)?;
        let pointer_path = Self::path(data_dir);
        reject_symlink_entry(&pointer_path, "semantic current pointer")?;
        let bytes = self.canonical_bytes()?;
        let mut temp = tempfile::Builder::new()
            .prefix(SEMANTIC_CURRENT_POINTER_TEMP_PREFIX)
            .tempfile_in(&root)
            .map_err(|error| SemanticGenerationError::PointerIo {
                source: error.to_string(),
            })?;
        temp.write_all(&bytes)
            .map_err(|error| SemanticGenerationError::PointerIo {
                source: error.to_string(),
            })?;
        after_checkpoint(PointerPublishCheckpoint::TemporaryBytesWritten)?;
        temp.as_file()
            .sync_all()
            .map_err(|error| SemanticGenerationError::PointerIo {
                source: error.to_string(),
            })?;
        after_checkpoint(PointerPublishCheckpoint::TemporaryFileSynced)?;
        let file = persist_named_temp(temp, &pointer_path, true).map_err(|error| {
            SemanticGenerationError::PointerIo {
                source: error.to_string(),
            }
        })?;
        file.sync_all()
            .map_err(|error| SemanticGenerationError::PointerIo {
                source: error.to_string(),
            })?;
        after_checkpoint(PointerPublishCheckpoint::PointerReplaced)?;
        sync_parent_directory(&pointer_path).map_err(|error| {
            SemanticGenerationError::PointerIo {
                source: error.to_string(),
            }
        })?;
        after_checkpoint(PointerPublishCheckpoint::ParentDirectorySynced)?;

        tracing::info!(
            event = "semantic.pointer.swap",
            schema_version = self.schema_version,
            generation = %fingerprint_prefix(&self.generation_id),
            build = %fingerprint_prefix(&loaded.manifest.build_id),
            request = %optional_fingerprint_prefix(loaded.manifest.request_id.as_deref()),
            requested_topology = ?loaded.manifest.requested_topology,
            realized_topology = ?loaded.manifest.realized_topology,
            manifest_sha256 = %fingerprint_prefix(&self.manifest_sha256),
            expected_prior = %expected_pointer_log_value(expected),
            actual_prior = %actual_pointer_log_value(prior.as_ref()),
            selection_epoch = self.selection_epoch,
            selected_at_ms = self.selected_at_ms,
            elapsed_ms = elapsed_ms(started),
            "compare-and-swap selected immutable semantic generation"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::search::semantic_manifest) enum PointerPublishCheckpoint {
    TemporaryBytesWritten,
    TemporaryFileSynced,
    PointerReplaced,
    ParentDirectorySynced,
}

/// Load and verify the selected semantic generation. No conventional filename,
/// model-name, or fallback discovery is attempted.
pub fn load_current_semantic_generation(
    data_dir: &Path,
    expected_corpus: Option<&SemanticCorpusSnapshotIdentity>,
) -> Result<ValidatedSemanticGeneration, SemanticGenerationError> {
    let started = Instant::now();
    let mut pointer_for_log = None;
    let mut manifest_for_log = None;
    let result = (|| {
        let selected = selection::read_observed(
            data_dir,
            expected_corpus,
            &mut pointer_for_log,
            &mut manifest_for_log,
        )?;
        let pointer = selected.pointer;
        let loaded = LoadedManifest {
            manifest: selected.manifest,
            generation_dir: selected.generation_dir,
        };
        let artifact_paths = loaded
            .manifest
            .validate_artifacts_on_disk(data_dir, false)?;

        tracing::info!(
            event = "semantic.manifest.load",
            schema_version = loaded.manifest.schema_version,
            generation = %fingerprint_prefix(&loaded.manifest.generation_id),
            build = %fingerprint_prefix(&loaded.manifest.build_id),
            request = %optional_fingerprint_prefix(loaded.manifest.request_id.as_deref()),
            requested_topology = ?loaded.manifest.requested_topology,
            realized_topology = ?loaded.manifest.realized_topology,
            manifest_sha256 = %fingerprint_prefix(&pointer.manifest_sha256),
            selection_epoch = pointer.selection_epoch,
            selected_document_count = loaded.manifest.selected_document_count,
            total_vector_slot_count_across_tiers =
                loaded.manifest.total_vector_slot_count_across_tiers,
            artifact_count = loaded.manifest.artifacts.len(),
            elapsed_ms = elapsed_ms(started),
            "loaded and verified current semantic generation"
        );

        Ok(ValidatedSemanticGeneration {
            pointer,
            generation_dir: loaded.generation_dir,
            manifest: loaded.manifest,
            artifact_paths,
        })
    })();

    if let Err(error) = &result {
        log_generation_validation_failure(
            pointer_for_log.as_ref(),
            manifest_for_log.as_ref(),
            error,
            started,
        );
    }
    result
}

/// Resolve the coarse authoritative semantic state without guessing artifact
/// identity from legacy filenames.
///
/// A selected pointer receives the full fail-closed load path. Without a
/// pointer, orphaned private pointer-publication tempfiles are reported as
/// `InterruptedPublish`, staging directories as `Building`, unselected
/// generation directories as `PointerMissingWithGenerationDirectories`, and
/// unrecognized root entries as `LegacyUnidentified`. A generation directory
/// is only an unvalidated candidate at this stage; this coarse resolver never
/// calls it rollback-safe. Suspicious tempfile lookalikes remain
/// legacy/unidentified rather than receiving the private-file classification.
pub fn resolve_semantic_generation_disk_state(
    data_dir: &Path,
) -> Result<SemanticGenerationDiskStateV1, SemanticGenerationError> {
    let pointer_path = SemanticCurrentPointerV1::path(data_dir);
    match fs::symlink_metadata(&pointer_path) {
        Ok(_) => {
            let loaded = load_current_semantic_generation(data_dir, None)?;
            return Ok(SemanticGenerationDiskStateV1::Ready {
                generation_id: loaded.pointer.generation_id,
                manifest_sha256: loaded.pointer.manifest_sha256,
                selection_epoch: loaded.pointer.selection_epoch,
                selected_at_ms: loaded.pointer.selected_at_ms,
                requested_topology: loaded.manifest.requested_topology,
                realized_topology: loaded.manifest.realized_topology,
            });
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(SemanticGenerationError::PointerIo {
                source: error.to_string(),
            });
        }
    }

    let root = semantic_vector_root(data_dir);
    match fs::symlink_metadata(&root) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(SemanticGenerationDiskStateV1::Absent);
        }
        Err(error) => {
            return Err(SemanticGenerationError::PointerIo {
                source: error.to_string(),
            });
        }
        Ok(metadata) if metadata_is_link_or_reparse(&metadata) || !metadata.is_dir() => {
            return Err(SemanticGenerationError::InvalidPointer {
                reason: "semantic vector root is not a safe directory".to_owned(),
            });
        }
        Ok(_) => {}
    }

    let staging = root.join(SEMANTIC_STAGING_DIRNAME);
    let build_count = count_safe_directory_entries(&staging, "semantic staging root", false)?;
    let generations = root.join(SEMANTIC_GENERATIONS_DIRNAME);
    let unselected_generation_directory_count =
        count_safe_directory_entries(&generations, "semantic generations root", true)?;
    let mut temporary_entry_count = 0_u64;
    let mut legacy_artifact_count = 0_u64;
    for entry in fs::read_dir(&root).map_err(|error| SemanticGenerationError::PointerIo {
        source: error.to_string(),
    })? {
        let entry = entry.map_err(|error| SemanticGenerationError::PointerIo {
            source: error.to_string(),
        })?;
        let name = entry.file_name();
        if name == OsStr::new(SEMANTIC_CURRENT_POINTER_LOCK_FILENAME)
            || name == OsStr::new(SEMANTIC_GENERATIONS_DIRNAME)
            || name == OsStr::new(SEMANTIC_STAGING_DIRNAME)
        {
            continue;
        }
        if is_private_pointer_publish_temp_name(&name) {
            let metadata = fs::symlink_metadata(entry.path()).map_err(|error| {
                SemanticGenerationError::PointerIo {
                    source: error.to_string(),
                }
            })?;
            if !metadata_is_link_or_reparse(&metadata) && metadata.is_file() {
                temporary_entry_count = temporary_entry_count.saturating_add(1);
                continue;
            }
        }
        legacy_artifact_count = legacy_artifact_count.saturating_add(1);
    }

    if temporary_entry_count > 0 {
        return Ok(SemanticGenerationDiskStateV1::InterruptedPublish {
            temporary_entry_count,
            build_count,
            unselected_generation_directory_count,
            legacy_artifact_count,
        });
    }
    if build_count > 0 {
        return Ok(SemanticGenerationDiskStateV1::Building {
            build_count,
            unselected_generation_directory_count,
            legacy_artifact_count,
        });
    }
    if unselected_generation_directory_count > 0 {
        return Ok(
            SemanticGenerationDiskStateV1::PointerMissingWithGenerationDirectories {
                generation_directory_count: unselected_generation_directory_count,
                legacy_artifact_count,
            },
        );
    }
    if legacy_artifact_count > 0 {
        return Ok(SemanticGenerationDiskStateV1::LegacyUnidentified {
            artifact_count: legacy_artifact_count,
        });
    }
    Ok(SemanticGenerationDiskStateV1::Absent)
}

pub(in crate::search::semantic_manifest) fn is_private_pointer_publish_temp_name(
    name: &OsStr,
) -> bool {
    let Some(name) = name.to_str() else {
        return false;
    };
    let Some(suffix) = name.strip_prefix(SEMANTIC_CURRENT_POINTER_TEMP_PREFIX) else {
        return false;
    };
    !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_alphanumeric())
}

fn count_safe_directory_entries(
    path: &Path,
    root_description: &'static str,
    entries_must_be_directories: bool,
) -> Result<u64, SemanticGenerationError> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => {
            return Err(SemanticGenerationError::PointerIo {
                source: error.to_string(),
            });
        }
        Ok(metadata) if metadata_is_link_or_reparse(&metadata) || !metadata.is_dir() => {
            return Err(SemanticGenerationError::InvalidPointer {
                reason: format!("{root_description} is not a safe directory"),
            });
        }
        Ok(_) => {}
    }
    let mut count = 0_u64;
    for entry in fs::read_dir(path).map_err(|error| SemanticGenerationError::PointerIo {
        source: error.to_string(),
    })? {
        let entry = entry.map_err(|error| SemanticGenerationError::PointerIo {
            source: error.to_string(),
        })?;
        let metadata = fs::symlink_metadata(entry.path()).map_err(|error| {
            SemanticGenerationError::PointerIo {
                source: error.to_string(),
            }
        })?;
        if metadata_is_link_or_reparse(&metadata) {
            return Err(SemanticGenerationError::InvalidPointer {
                reason: format!("{root_description} contains a symlink or reparse point"),
            });
        }
        if entries_must_be_directories && !metadata.is_dir() {
            return Err(SemanticGenerationError::InvalidPointer {
                reason: format!("{root_description} contains a non-directory entry"),
            });
        }
        count = count.saturating_add(1);
    }
    Ok(count)
}

pub(in crate::search::semantic_manifest) struct LoadedManifest {
    pub(in crate::search::semantic_manifest) manifest: SemanticGenerationManifestV1,
    pub(in crate::search::semantic_manifest) generation_dir: PathBuf,
}

pub(in crate::search::semantic_manifest) fn load_manifest_selected_by_pointer(
    data_dir: &Path,
    pointer: &SemanticCurrentPointerV1,
) -> Result<LoadedManifest, SemanticGenerationError> {
    pointer.validate()?;
    let generation_dir = semantic_generations_root(data_dir).join(&pointer.generation_id);
    validate_existing_generation_directory(data_dir, &generation_dir)?;
    let manifest_path = generation_dir.join(SEMANTIC_GENERATION_MANIFEST_FILENAME);
    match fs::symlink_metadata(&manifest_path) {
        Ok(metadata) if metadata_is_link_or_reparse(&metadata) => {
            return Err(SemanticGenerationError::InvalidManifest {
                class: SemanticManifestInvariantClass::Path,
                reason: "generation manifest must not be a symlink or reparse point".to_owned(),
            });
        }
        Ok(metadata) if !metadata.is_file() => {
            return Err(SemanticGenerationError::InvalidManifest {
                class: SemanticManifestInvariantClass::Path,
                reason: "generation manifest is not a regular file".to_owned(),
            });
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(SemanticGenerationError::MissingManifest {
                generation_id: pointer.generation_id.clone(),
            });
        }
        Err(error) => {
            return Err(SemanticGenerationError::ManifestIo {
                generation_id: pointer.generation_id.clone(),
                source: error.to_string(),
            });
        }
    }
    let manifest = load_manifest_file(&manifest_path, &pointer.generation_id)?;
    manifest.validate()?;
    let actual_digest = manifest.sha256()?;
    if actual_digest != pointer.manifest_sha256 {
        return Err(SemanticGenerationError::ManifestDigestMismatch {
            expected: pointer.manifest_sha256.clone(),
            actual: actual_digest,
        });
    }
    if manifest.generation_id != pointer.generation_id {
        return invalid_manifest(
            SemanticManifestInvariantClass::Identity,
            "pointer generation ID and manifest generation ID differ",
        );
    }
    if pointer.selected_at_ms < manifest.sealed_at_ms {
        return invalid_manifest(
            SemanticManifestInvariantClass::Timestamp,
            "pointer selection predates immutable manifest seal",
        );
    }
    Ok(LoadedManifest {
        manifest,
        generation_dir,
    })
}

pub(in crate::search::semantic_manifest) fn load_manifest_file(
    manifest_path: &Path,
    generation_id: &str,
) -> Result<SemanticGenerationManifestV1, SemanticGenerationError> {
    let bytes = read_bounded_file(manifest_path, MAX_SEMANTIC_MANIFEST_BYTES).map_err(|error| {
        if error.kind() == std::io::ErrorKind::InvalidData {
            SemanticGenerationError::ManifestParse {
                generation_id: generation_id.to_owned(),
                source: error.to_string(),
            }
        } else {
            SemanticGenerationError::ManifestIo {
                generation_id: generation_id.to_owned(),
                source: error.to_string(),
            }
        }
    })?;
    parse_generation_manifest_bytes(&bytes, generation_id)
}

pub(in crate::search::semantic_manifest) fn parse_current_pointer_bytes(
    bytes: &[u8],
) -> Result<SemanticCurrentPointerV1, SemanticGenerationError> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|error| SemanticGenerationError::PointerParse {
            source: error.to_string(),
        })?;
    let schema_version = json_schema_version(&value)
        .map_err(|source| SemanticGenerationError::PointerParse { source })?;
    let supported = u64::from(SEMANTIC_CURRENT_POINTER_SCHEMA_VERSION);
    if schema_version > supported {
        return Err(SemanticGenerationError::UnsupportedPointerSchema {
            found: schema_version,
            supported,
        });
    }
    if schema_version < supported {
        return Err(SemanticGenerationError::InvalidPointer {
            reason: format!(
                "unsupported historical pointer schema {schema_version}; expected {supported}"
            ),
        });
    }
    serde_json::from_value(value).map_err(|error| SemanticGenerationError::PointerParse {
        source: error.to_string(),
    })
}

pub(in crate::search::semantic_manifest) fn parse_generation_manifest_bytes(
    bytes: &[u8],
    generation_id: &str,
) -> Result<SemanticGenerationManifestV1, SemanticGenerationError> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|error| SemanticGenerationError::ManifestParse {
            generation_id: generation_id.to_owned(),
            source: error.to_string(),
        })?;
    let schema_version =
        json_schema_version(&value).map_err(|source| SemanticGenerationError::ManifestParse {
            generation_id: generation_id.to_owned(),
            source,
        })?;
    let supported = u64::from(SEMANTIC_SHARDED_GENERATION_MANIFEST_SCHEMA_VERSION);
    if schema_version > supported {
        return Err(SemanticGenerationError::UnsupportedManifestSchema {
            found: schema_version,
            supported,
        });
    }
    if schema_version < u64::from(SEMANTIC_GENERATION_MANIFEST_SCHEMA_VERSION) {
        return invalid_manifest(
            SemanticManifestInvariantClass::HistoricalSchema,
            &format!(
                "unsupported historical generation schema {schema_version}; expected {supported}"
            ),
        );
    }
    preflight_embedding_identity_schemas(&value)?;
    serde_json::from_value(value).map_err(|error| SemanticGenerationError::ManifestParse {
        generation_id: generation_id.to_owned(),
        source: error.to_string(),
    })
}

fn json_schema_version(value: &serde_json::Value) -> Result<u64, String> {
    value
        .as_object()
        .and_then(|object| object.get("schema_version"))
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| "schema_version must be an unsigned integer".to_owned())
}

fn preflight_embedding_identity_schemas(
    manifest: &serde_json::Value,
) -> Result<(), SemanticGenerationError> {
    let Some(artifacts) = manifest
        .as_object()
        .and_then(|object| object.get("artifacts"))
        .and_then(serde_json::Value::as_array)
    else {
        return Ok(());
    };
    let components = [
        ("space", u64::from(EMBEDDING_SPACE_IDENTITY_SCHEMA_V1)),
        (
            "producer",
            u64::from(EMBEDDING_PRODUCER_ATTESTATION_SCHEMA_V1),
        ),
        ("input", u64::from(EMBEDDING_INPUT_CONTRACT_SCHEMA_V1)),
        ("storage", u64::from(VECTOR_STORAGE_IDENTITY_SCHEMA_V1)),
    ];
    for artifact in artifacts {
        let Some(identity) = artifact
            .as_object()
            .and_then(|object| object.get("embedding_identity"))
            .and_then(serde_json::Value::as_object)
            .and_then(|object| object.get("identity"))
            .and_then(serde_json::Value::as_object)
        else {
            continue;
        };
        for &(component, supported) in &components {
            let Some(found) = identity
                .get(component)
                .and_then(serde_json::Value::as_object)
                .and_then(|object| object.get("schema_version"))
                .and_then(serde_json::Value::as_u64)
            else {
                continue;
            };
            if found > supported {
                return Err(
                    SemanticGenerationError::UnsupportedEmbeddingIdentitySchema {
                        component: component.to_owned(),
                        found,
                        supported,
                    },
                );
            }
        }
    }
    Ok(())
}

struct SemanticPublishLock(fs::File);

impl Drop for SemanticPublishLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.0);
    }
}

fn open_semantic_publish_lock(
    vector_root: &Path,
    path: &Path,
) -> Result<fs::File, SemanticGenerationError> {
    reject_existing_path_links(vector_root, path).map_err(|error| {
        SemanticGenerationError::PublishLockIo {
            source: error.to_string(),
        }
    })?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(|error| SemanticGenerationError::PublishLockIo {
            source: error.to_string(),
        })?;
    let metadata = file
        .metadata()
        .map_err(|error| SemanticGenerationError::PublishLockIo {
            source: error.to_string(),
        })?;
    if !metadata.is_file() {
        return Err(SemanticGenerationError::PublishLockIo {
            source: "publication lock is not a regular file".to_owned(),
        });
    }
    Ok(file)
}

fn read_current_pointer_for_cas(
    data_dir: &Path,
) -> Result<Option<SemanticCurrentPointerV1>, SemanticGenerationError> {
    let path = SemanticCurrentPointerV1::path(data_dir);
    match fs::symlink_metadata(&path) {
        Ok(metadata) if metadata_is_link_or_reparse(&metadata) => {
            return Err(SemanticGenerationError::InvalidPointer {
                reason: "current pointer must not be a symlink or reparse point".to_owned(),
            });
        }
        Ok(metadata) if !metadata.is_file() => {
            return Err(SemanticGenerationError::InvalidPointer {
                reason: "current pointer is not a regular file".to_owned(),
            });
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(SemanticGenerationError::PointerIo {
                source: error.to_string(),
            });
        }
    }
    let bytes = read_bounded_file(&path, MAX_SEMANTIC_POINTER_BYTES).map_err(|error| {
        SemanticGenerationError::PointerIo {
            source: error.to_string(),
        }
    })?;
    let pointer = parse_current_pointer_bytes(&bytes)?;
    pointer.validate()?;
    Ok(Some(pointer))
}

fn verify_publish_precondition(
    expected: &SemanticExpectedCurrentV1,
    actual: Option<&SemanticCurrentPointerV1>,
) -> Result<(), SemanticGenerationError> {
    if let SemanticExpectedCurrentV1::Exact(pointer) = expected {
        pointer.validate()?;
    }
    let matches = match (expected, actual) {
        (SemanticExpectedCurrentV1::Absent, None) => true,
        (SemanticExpectedCurrentV1::Exact(expected), Some(actual)) => expected == actual,
        _ => false,
    };
    if matches {
        return Ok(());
    }
    Err(SemanticGenerationError::ConcurrentPublishConflict {
        expected: expected_pointer_log_value(expected),
        actual: actual_pointer_log_value(actual),
    })
}

fn expected_pointer_log_value(expected: &SemanticExpectedCurrentV1) -> String {
    match expected {
        SemanticExpectedCurrentV1::Absent => "[absent]".to_owned(),
        SemanticExpectedCurrentV1::Exact(pointer) => format!(
            "{}:{}#{}",
            fingerprint_prefix(&pointer.generation_id),
            fingerprint_prefix(&pointer.manifest_sha256),
            pointer.selection_epoch
        ),
    }
}

fn actual_pointer_log_value(actual: Option<&SemanticCurrentPointerV1>) -> String {
    actual.map_or_else(
        || "[absent]".to_owned(),
        |pointer| {
            format!(
                "{}:{}#{}",
                fingerprint_prefix(&pointer.generation_id),
                fingerprint_prefix(&pointer.manifest_sha256),
                pointer.selection_epoch
            )
        },
    )
}
