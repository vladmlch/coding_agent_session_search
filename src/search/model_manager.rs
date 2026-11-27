//! Semantic model management (local-only detection).
//!
//! This module wires the pure-Rust native MiniLM embedder into semantic search by:
//! - validating the local model files
//! - loading the vector index
//! - building filter maps from the SQLite database
//! - detecting model version mismatches
//!
//! It does **not** download models. Missing files are surfaced as availability
//! states so the UI can guide the user. Local installs are handled by
//! [`model_download`](super::model_download) (`cass models install --from-file <DIR>`).

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::search_defaults::expected_vector_space_revision;
use crate::search::ann_index::hnsw_index_path;
use crate::search::embedder::Embedder;
use crate::search::fastembed_embedder::{FastEmbedder, LazyFastEmbedder};
use crate::search::hash_embedder::HashEmbedder;
use crate::search::model_download::{
    ModelAcquisitionPolicy, ModelCacheState, ModelManifest, classify_model_cache,
    classify_model_cache_metadata,
};
use crate::search::policy::{CliSemanticOverrides, SemanticPolicy};
use crate::search::semantic_manifest::{
    SemanticCurrentPointerV1, SemanticShardManifest, SemanticShardRecord, TierKind,
    semantic_shard_artifact_path_is_safe,
};
use crate::search::vector_index::{
    ROLE_ASSISTANT, ROLE_USER, SemanticFilterMaps, SemanticIndexArtifact,
    SemanticProgressiveUnavailableReason, VectorIndex, vector_index_path,
};
use crate::storage::sqlite::{FrankenStorage, SemanticIdentityTier};

/// Unified TUI state machine for semantic search availability.
///
/// This enum tracks the full lifecycle of semantic search from the user's perspective:
/// - Model installation flow (NotInstalled/ModelMissing → Ready via
///   `cass models install --from-file <DIR>`; cass has no download lifecycle)
/// - Index building flow (Ready → IndexBuilding → Ready)
/// - Explicit user preferences (HashFallback, Disabled)
/// - Error states (LoadFailed, ModelMissing, etc.)
#[derive(Debug, Clone)]
pub enum SemanticAvailability {
    /// Model is ready for use.
    Ready { embedder_id: String },

    // =========================================================================
    // TUI-centric states for user flow
    // =========================================================================
    /// Model not installed - semantic not available.
    /// TUI should show an option to install from local files or explicitly
    /// choose hash mode.
    NotInstalled,

    /// Index is being built or rebuilt.
    IndexBuilding {
        embedder_id: String,
        /// Optional progress percentage (0-100).
        progress_pct: Option<u8>,
        /// Number of items indexed so far.
        items_indexed: u64,
        /// Total items to index.
        total_items: u64,
    },

    /// Vector assets load, but the semantic manifest says they do not describe
    /// the current database (stale generation, or a backfill that has not yet
    /// published a queryable tier). Serving them would return nearest
    /// neighbours of an outdated corpus, so the query path must not consult
    /// them (GH #404). `reason` carries the manifest-derived verdict.
    IndexStale { embedder_id: String, reason: String },

    /// User explicitly opted for hash-based degraded mode (no ML model).
    HashFallback,

    /// Semantic search disabled by policy or user.
    Disabled { reason: String },

    // =========================================================================
    // Diagnostic states for troubleshooting
    // =========================================================================
    /// Model files are missing.
    ModelMissing {
        embedder_id: String,
        model_dir: PathBuf,
        missing_files: Vec<String>,
    },

    /// Vector index is missing.
    IndexMissing { index_path: PathBuf },

    /// Database is unavailable.
    DatabaseUnavailable { db_path: PathBuf, error: String },

    /// Failed to load semantic context.
    LoadFailed { context: String },
}

impl SemanticAvailability {
    /// Check if semantic search is ready to use.
    pub fn is_ready(&self) -> bool {
        matches!(self, SemanticAvailability::Ready { .. })
    }

    /// Check if the index is being rebuilt.
    pub fn is_building(&self) -> bool {
        matches!(self, SemanticAvailability::IndexBuilding { .. })
    }

    /// Check if loadable vector assets were refused because they do not
    /// describe the current database (GH #404).
    pub fn is_index_stale(&self) -> bool {
        matches!(self, SemanticAvailability::IndexStale { .. })
    }

    /// Check if explicit hash mode is active.
    pub fn is_hash_fallback(&self) -> bool {
        matches!(self, SemanticAvailability::HashFallback)
    }

    /// Check if semantic search is disabled.
    pub fn is_disabled(&self) -> bool {
        matches!(self, SemanticAvailability::Disabled { .. })
    }

    /// Check if the model is not installed.
    pub fn is_not_installed(&self) -> bool {
        matches!(
            self,
            SemanticAvailability::NotInstalled | SemanticAvailability::ModelMissing { .. }
        )
    }

    /// Check if any error state is active.
    pub fn is_error(&self) -> bool {
        matches!(
            self,
            SemanticAvailability::LoadFailed { .. }
                | SemanticAvailability::DatabaseUnavailable { .. }
        )
    }

    /// Check if vector search can be used (native semantic or explicit hash mode).
    pub fn can_search(&self) -> bool {
        matches!(
            self,
            SemanticAvailability::Ready { .. } | SemanticAvailability::HashFallback
        )
    }

    /// Get index building progress if building.
    pub fn index_progress(&self) -> Option<(Option<u8>, u64, u64)> {
        match self {
            SemanticAvailability::IndexBuilding {
                progress_pct,
                items_indexed,
                total_items,
                ..
            } => Some((*progress_pct, *items_indexed, *total_items)),
            _ => None,
        }
    }

    /// Get a short status label for display in status bar.
    pub fn status_label(&self) -> &'static str {
        match self {
            SemanticAvailability::Ready { .. } => "SEM",
            SemanticAvailability::HashFallback => "SEM*",
            SemanticAvailability::NotInstalled => "LEX",
            SemanticAvailability::IndexBuilding { .. } => "IDX...",
            SemanticAvailability::IndexStale { .. } => "STALE",
            SemanticAvailability::Disabled { .. } => "OFF",
            SemanticAvailability::ModelMissing { .. } => "NOMODEL",
            SemanticAvailability::IndexMissing { .. } => "NOIDX",
            SemanticAvailability::DatabaseUnavailable { .. } => "NODB",
            SemanticAvailability::LoadFailed { .. } => "ERR",
        }
    }

    /// Get a detailed summary for display.
    pub fn summary(&self) -> String {
        match self {
            SemanticAvailability::Ready { embedder_id } => {
                format!("semantic ready ({embedder_id})")
            }
            SemanticAvailability::NotInstalled => "model not installed".to_string(),
            SemanticAvailability::IndexBuilding {
                items_indexed,
                total_items,
                progress_pct,
                ..
            } => {
                if let Some(pct) = progress_pct {
                    format!("building index: {pct}% ({items_indexed}/{total_items})")
                } else {
                    format!("building index: {items_indexed}/{total_items}")
                }
            }
            SemanticAvailability::IndexStale { reason, .. } => {
                format!("semantic assets not current: {reason}")
            }
            SemanticAvailability::HashFallback => "using explicit hash mode".to_string(),
            SemanticAvailability::Disabled { reason } => {
                format!("semantic disabled: {reason}")
            }
            SemanticAvailability::ModelMissing { model_dir, .. } => {
                format!("model missing at {}", model_dir.display())
            }
            SemanticAvailability::IndexMissing { index_path } => {
                format!("vector index missing at {}", index_path.display())
            }
            SemanticAvailability::DatabaseUnavailable { error, .. } => {
                format!("db unavailable ({error})")
            }
            SemanticAvailability::LoadFailed { context } => {
                format!("semantic load failed ({context})")
            }
        }
    }
}

pub struct SemanticContext {
    pub embedder: Arc<dyn Embedder>,
    /// These owners may come from the monolithic writer path or the prototype
    /// shard manifest. Exact search consumes the retained owners directly.
    /// Progressive/two-tier serving remains fail-closed for every source,
    /// because its current FrankenSearch constructors reopen paths. Do not
    /// populate these owners by reopening
    /// `ValidatedSemanticGeneration::artifact_paths`: that map is diagnostic
    /// resolution only until the immutable-generation validator can hand
    /// serving code retained FSVI witnesses/owners and FrankenSearch can
    /// consume those owners without reopening.
    pub artifacts: Vec<SemanticIndexArtifact>,
    pub quality_artifact: Option<SemanticIndexArtifact>,
    pub filter_maps: SemanticFilterMaps,
    pub roles: Option<HashSet<u8>>,
}

pub struct SemanticSetup {
    pub availability: SemanticAvailability,
    pub context: Option<SemanticContext>,
}

const SEMANTIC_POINTER_PROBE_FAILED_REASON: &str = "semantic_pointer_probe_failed";

fn selected_generation_owner_requirement(data_dir: &Path) -> Option<SemanticAvailability> {
    match std::fs::symlink_metadata(SemanticCurrentPointerV1::path(data_dir)) {
        Ok(_) => Some(SemanticAvailability::LoadFailed {
            context: SemanticProgressiveUnavailableReason::OwnerBackedReaderRequired
                .code()
                .to_string(),
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(_) => Some(SemanticAvailability::LoadFailed {
            context: SEMANTIC_POINTER_PROBE_FAILED_REASON.to_string(),
        }),
    }
}

fn semantic_sidecar_path(data_dir: &Path, recorded_path: &str) -> Option<PathBuf> {
    // ubs:ignore[rust.security.path-traversal] — guarded by semantic_shard_artifact_path_is_safe
    semantic_shard_artifact_path_is_safe(recorded_path).then(|| data_dir.join(recorded_path))
}

fn validate_vector_index_contract(
    index: &VectorIndex,
    expected_embedder_id: &str,
    expected_dimension: usize,
) -> Result<(), String> {
    let expected_revision =
        expected_vector_space_revision(expected_embedder_id).ok_or_else(|| {
            format!("no current vector-space revision for embedder {expected_embedder_id}")
        })?;
    if index.embedder_id() != expected_embedder_id
        || index.dimension() != expected_dimension
        || index.embedder_revision() != expected_revision
    {
        return Err(format!(
            "incompatible vector index: expected embedder {expected_embedder_id} revision {expected_revision} dimension {expected_dimension}, found embedder {} revision {} dimension {}; rebuild semantic vectors",
            index.embedder_id(),
            index.embedder_revision(),
            index.dimension()
        ));
    }
    Ok(())
}

fn open_validated_semantic_artifact(
    index_path: &Path,
    ann_path: Option<PathBuf>,
    expected_embedder_id: &str,
    expected_dimension: usize,
) -> Result<SemanticIndexArtifact, String> {
    let artifact = SemanticIndexArtifact::open(index_path, ann_path)
        .map_err(|error| format!("vector index: {error}"))?;
    validate_vector_index_contract(artifact.index(), expected_embedder_id, expected_dimension)?;
    Ok(artifact)
}

fn semantic_artifact_candidate_exists(path: &Path) -> Result<bool, String> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(format!(
            "inspect semantic artifact candidate {}: {error}",
            path.display()
        )),
    }
}

fn matching_complete_shard_records(
    data_dir: &Path,
    tier: TierKind,
    embedder_id: &str,
    db_fingerprint: &str,
) -> Result<Option<Vec<SemanticShardRecord>>, String> {
    let manifest = match SemanticShardManifest::load(data_dir) {
        Ok(Some(manifest)) => manifest,
        Ok(None) => return Ok(None),
        Err(err) => return Err(format!("semantic shard manifest: {err}")),
    };
    let summary = manifest.summary(tier, embedder_id, db_fingerprint);
    if !summary.complete {
        return Ok(None);
    }

    let mut records = manifest
        .shards
        .into_iter()
        .filter(|shard| shard.matches_generation(tier, embedder_id, db_fingerprint))
        .collect::<Vec<_>>();
    records.sort_by_key(|shard| shard.shard_index);
    if records.len() != usize::try_from(summary.shard_count).unwrap_or(usize::MAX) {
        return Ok(None);
    }

    let Some(first) = records.first() else {
        return Ok(None);
    };
    for (expected_index, shard) in records.iter().enumerate() {
        if shard.shard_index != u32::try_from(expected_index).unwrap_or(u32::MAX)
            || !shard.ready
            || !shard.mmap_ready
            || shard.model_revision != first.model_revision
            || shard.schema_version != crate::search::policy::SEMANTIC_SCHEMA_VERSION
            || shard.chunking_version != crate::search::policy::CHUNKING_STRATEGY_VERSION
            || shard.dimension == 0
            || shard.dimension != first.dimension
            || shard.total_conversations != first.total_conversations
        {
            return Ok(None);
        }
        let Some(path) = semantic_sidecar_path(data_dir, &shard.index_path) else {
            return Ok(None);
        };
        if !semantic_artifact_candidate_exists(&path)? {
            return Ok(None);
        }
    }

    Ok(Some(records))
}

fn load_complete_shard_artifacts(
    data_dir: &Path,
    embedder_id: &str,
    expected_dimension: usize,
    db_fingerprint: &str,
) -> Result<Option<Vec<SemanticIndexArtifact>>, String> {
    for tier in [TierKind::Quality, TierKind::Fast] {
        let Some(records) =
            matching_complete_shard_records(data_dir, tier, embedder_id, db_fingerprint)?
        else {
            continue;
        };

        let mut artifacts = Vec::with_capacity(records.len());
        for shard in records {
            let Some(path) = semantic_sidecar_path(data_dir, &shard.index_path) else {
                return Ok(None);
            };
            let ann_path = if shard.ann_ready {
                shard
                    .ann_index_path
                    .as_deref()
                    .and_then(|recorded| semantic_sidecar_path(data_dir, recorded))
                    .and_then(|candidate| {
                        let selected = select_existing_ann_candidate(candidate.clone());
                        if selected.is_none() {
                            tracing::warn!(
                                path = %candidate.display(),
                                embedder = embedder_id,
                                shard = shard.shard_index,
                                "semantic shard manifest marks ANN ready but its exact sidecar is missing"
                            );
                        }
                        selected
                    })
            } else {
                None
            };
            let artifact = SemanticIndexArtifact::open(&path, ann_path)
                .map_err(|err| format!("semantic shard vector index {}: {err}", path.display()))?;
            if shard.dimension != expected_dimension {
                return Err(format!(
                    "semantic shard manifest {} declares dimension {}, expected {expected_dimension}",
                    path.display(),
                    shard.dimension
                ));
            }
            validate_vector_index_contract(artifact.index(), embedder_id, expected_dimension)
                .map_err(|error| {
                    format!("semantic shard vector index {}: {error}", path.display())
                })?;
            artifacts.push(artifact);
        }
        if !artifacts.is_empty() {
            tracing::info!(
                tier = tier.as_str(),
                embedder = embedder_id,
                shard_count = artifacts.len(),
                "loaded complete semantic shard generation"
            );
            return Ok(Some(artifacts));
        }
    }

    Ok(None)
}

fn select_existing_ann_candidate(path: PathBuf) -> Option<PathBuf> {
    match std::fs::symlink_metadata(&path) {
        Ok(_) => Some(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(_) => Some(path),
    }
}

fn complete_shard_generation_candidate_exists(data_dir: &Path, embedder_id: &str) -> bool {
    let manifest = match SemanticShardManifest::load(data_dir) {
        Ok(Some(manifest)) => manifest,
        Ok(None) => return false,
        Err(err) => {
            tracing::debug!(
                error = %err,
                embedder = embedder_id,
                "semantic shard candidate probe could not load manifest"
            );
            return false;
        }
    };

    let mut candidates = std::collections::HashSet::new();
    for shard in manifest
        .shards
        .iter()
        .filter(|shard| shard.embedder_id == embedder_id)
    {
        candidates.insert((shard.tier, shard.db_fingerprint.as_str()));
    }

    candidates
        .into_iter()
        .any(|(tier, db_fingerprint)| manifest.summary(tier, embedder_id, db_fingerprint).complete)
}

fn load_complete_shard_artifacts_for_current_db(
    data_dir: &Path,
    db_path: &Path,
    embedder_id: &str,
    expected_dimension: usize,
    context_label: &'static str,
    strict_read_only: bool,
) -> Option<Vec<SemanticIndexArtifact>> {
    load_complete_shard_artifacts_with_fingerprint(
        data_dir,
        embedder_id,
        expected_dimension,
        context_label,
        || {
            if strict_read_only {
                crate::storage::archive_fingerprint::lexical_storage_fingerprint_for_db_strict(
                    db_path,
                )
            } else {
                crate::storage::archive_fingerprint::lexical_storage_fingerprint_for_db(db_path)
            }
        },
    )
}

fn load_complete_shard_artifacts_with_fingerprint(
    data_dir: &Path,
    embedder_id: &str,
    expected_dimension: usize,
    context_label: &'static str,
    fingerprint: impl FnOnce() -> anyhow::Result<String>,
) -> Option<Vec<SemanticIndexArtifact>> {
    // GH #452: a monolithic FSVI is not evidence that shards exist. Probe
    // metadata before opening the archive solely to fingerprint a possible
    // shard generation; the normal staleness/filter-map path opens it later.
    if !complete_shard_generation_candidate_exists(data_dir, embedder_id) {
        return None;
    }
    let db_fingerprint = match fingerprint() {
        Ok(fingerprint) => fingerprint,
        Err(err) => {
            tracing::debug!(
                error = %format!("{err:#}"),
                embedder = embedder_id,
                context = context_label,
                "semantic shard context unavailable: failed to fingerprint current DB"
            );
            return None;
        }
    };

    match load_complete_shard_artifacts(data_dir, embedder_id, expected_dimension, &db_fingerprint)
    {
        Ok(artifacts) => artifacts,
        Err(err) => {
            tracing::debug!(
                error = %err,
                embedder = embedder_id,
                context = context_label,
                "semantic shard context unavailable"
            );
            None
        }
    }
}

/// Load semantic context.
///
/// cass is local-only: there is no update-check path, so an install whose
/// marker revision predates the pinned manifest simply loads (the files were
/// already checksum-verified during cache classification).
pub fn load_semantic_context(data_dir: &Path, db_path: &Path) -> SemanticSetup {
    load_semantic_context_for_embedder(data_dir, db_path, active_policy_embedder_name())
}

/// Strict read-only counterpart used by `search --no-maintenance`.
pub fn load_semantic_context_strict(data_dir: &Path, db_path: &Path) -> SemanticSetup {
    load_semantic_context_inner(
        data_dir,
        db_path,
        active_policy_embedder_name(),
        false,
        true,
    )
}

/// Load the active policy context without initializing its local model yet.
pub fn load_semantic_context_deferred(data_dir: &Path, db_path: &Path) -> SemanticSetup {
    load_semantic_context_for_embedder_deferred(data_dir, db_path, active_policy_embedder_name())
}

/// Strict read-only daemon-first counterpart used by
/// `search --no-maintenance`.
pub fn load_semantic_context_deferred_strict(data_dir: &Path, db_path: &Path) -> SemanticSetup {
    load_semantic_context_inner(data_dir, db_path, active_policy_embedder_name(), true, true)
}

pub fn load_semantic_context_for_embedder(
    data_dir: &Path,
    db_path: &Path,
    embedder_name: &str,
) -> SemanticSetup {
    load_semantic_context_inner(data_dir, db_path, embedder_name, false, false)
}

pub fn load_semantic_context_for_embedder_strict(
    data_dir: &Path,
    db_path: &Path,
    embedder_name: &str,
) -> SemanticSetup {
    load_semantic_context_inner(data_dir, db_path, embedder_name, false, true)
}

/// Load index/filter metadata now while deferring the local model itself.
///
/// This is the daemon-first CLI path: the returned embedder preserves the
/// vector index's stable id/dimension contract, but initializes local inference
/// only if the daemon wrapper needs its fallback (#347).
pub fn load_semantic_context_for_embedder_deferred(
    data_dir: &Path,
    db_path: &Path,
    embedder_name: &str,
) -> SemanticSetup {
    load_semantic_context_inner(data_dir, db_path, embedder_name, true, false)
}

pub fn load_semantic_context_for_embedder_deferred_strict(
    data_dir: &Path,
    db_path: &Path,
    embedder_name: &str,
) -> SemanticSetup {
    load_semantic_context_inner(data_dir, db_path, embedder_name, true, true)
}

/// Probe semantic availability without loading the embedder or DB-backed
/// filter maps. The selected FSVI is opened through the same exact-artifact
/// policy as serving so status cannot advertise an artifact a query rejects.
pub(crate) fn probe_semantic_availability(data_dir: &Path) -> SemanticAvailability {
    probe_semantic_availability_for_embedder(data_dir, active_policy_embedder_name())
}

pub(crate) fn probe_semantic_availability_for_embedder(
    data_dir: &Path,
    embedder_name: &str,
) -> SemanticAvailability {
    if let Some(availability) = selected_generation_owner_requirement(data_dir) {
        return availability;
    }
    let Some(canonical_name) = FastEmbedder::canonical_name(embedder_name) else {
        return SemanticAvailability::LoadFailed {
            context: format!(
                "unsupported semantic embedder: {embedder_name}; supported: minilm, multilingual-minilm"
            ),
        };
    };
    let Some(config) = FastEmbedder::config_for(canonical_name) else {
        return SemanticAvailability::LoadFailed {
            context: format!("unknown semantic embedder: {embedder_name}"),
        };
    };
    let Some(model_dir) = FastEmbedder::runtime_model_dir_for(data_dir, canonical_name) else {
        return SemanticAvailability::LoadFailed {
            context: format!("no model directory mapping for semantic embedder: {embedder_name}"),
        };
    };
    let manifest =
        ModelManifest::for_embedder(canonical_name).unwrap_or_else(ModelManifest::minilm_v2);
    let semantic_policy = SemanticPolicy::resolve(&CliSemanticOverrides::default());
    let acquisition_policy = ModelAcquisitionPolicy::from_semantic_policy(&semantic_policy);
    let cache_report = classify_model_cache_metadata(&model_dir, &manifest, &acquisition_policy);

    if let Some(availability) =
        semantic_availability_from_cache_state(&config.embedder_id, &model_dir, &cache_report.state)
    {
        return availability;
    }

    let index_path = vector_index_path(data_dir, &config.embedder_id);
    match semantic_artifact_candidate_exists(&index_path) {
        Ok(true) => {}
        Ok(false) => return SemanticAvailability::IndexMissing { index_path },
        Err(context) => return SemanticAvailability::LoadFailed { context },
    }

    if let Err(context) =
        open_validated_semantic_artifact(&index_path, None, &config.embedder_id, config.dimension)
    {
        return SemanticAvailability::LoadFailed { context };
    }

    SemanticAvailability::Ready {
        embedder_id: config.embedder_id,
    }
}

/// Probe hash semantic availability without opening the DB. The selected FSVI
/// is admitted through the same exact-artifact policy as serving.
pub(crate) fn probe_hash_semantic_availability(data_dir: &Path) -> SemanticAvailability {
    if let Some(availability) = selected_generation_owner_requirement(data_dir) {
        return availability;
    }
    let embedder = HashEmbedder::default();
    let index_path = vector_index_path(data_dir, embedder.id());
    match semantic_artifact_candidate_exists(&index_path) {
        Ok(false) => SemanticAvailability::IndexMissing { index_path },
        Err(context) => SemanticAvailability::LoadFailed { context },
        Ok(true) => match open_validated_semantic_artifact(
            &index_path,
            None,
            embedder.id(),
            embedder.dimension(),
        ) {
            Ok(_) => SemanticAvailability::HashFallback,
            Err(context) => SemanticAvailability::LoadFailed { context },
        },
    }
}

/// Load hash-based semantic context (no model download required).
pub fn load_hash_semantic_context(data_dir: &Path, db_path: &Path) -> SemanticSetup {
    load_hash_semantic_context_inner(data_dir, db_path, false)
}

pub fn load_hash_semantic_context_strict(data_dir: &Path, db_path: &Path) -> SemanticSetup {
    load_hash_semantic_context_inner(data_dir, db_path, true)
}

fn load_hash_semantic_context_inner(
    data_dir: &Path,
    db_path: &Path,
    strict_read_only: bool,
) -> SemanticSetup {
    if let Some(availability) = selected_generation_owner_requirement(data_dir) {
        return SemanticSetup {
            availability,
            context: None,
        };
    }
    let embedder = HashEmbedder::default();
    let index_path = vector_index_path(data_dir, embedder.id());
    let monolithic_present = match semantic_artifact_candidate_exists(&index_path) {
        Ok(present) => present,
        Err(context) => {
            return SemanticSetup {
                availability: SemanticAvailability::LoadFailed { context },
                context: None,
            };
        }
    };
    let shard_artifacts = load_complete_shard_artifacts_for_current_db(
        data_dir,
        db_path,
        embedder.id(),
        embedder.dimension(),
        "hash semantic",
        strict_read_only,
    );
    if !monolithic_present && shard_artifacts.is_none() {
        return SemanticSetup {
            availability: SemanticAvailability::IndexMissing { index_path },
            context: None,
        };
    }

    let storage = match if strict_read_only {
        FrankenStorage::open_strict_readonly(db_path)
    } else {
        FrankenStorage::open_readonly(db_path)
    } {
        Ok(storage) => storage,
        Err(err) => {
            return SemanticSetup {
                availability: SemanticAvailability::DatabaseUnavailable {
                    db_path: db_path.to_path_buf(),
                    error: err.to_string(),
                },
                context: None,
            };
        }
    };

    if let Some(availability) = refuse_stale_semantic_assets(
        data_dir,
        &storage,
        embedder.id(),
        &SemanticAvailability::HashFallback,
        crate::search::asset_state::SemanticPreference::HashFallback,
    ) {
        return SemanticSetup {
            availability,
            context: None,
        };
    }

    let filter_maps = match SemanticFilterMaps::from_storage(&storage) {
        Ok(maps) => maps,
        Err(err) => {
            return SemanticSetup {
                availability: SemanticAvailability::LoadFailed {
                    context: format!("filter maps: {err}"),
                },
                context: None,
            };
        }
    };

    let artifacts = if let Some(artifacts) = shard_artifacts {
        artifacts
    } else {
        let ann_path = hnsw_index_path(data_dir, embedder.id());
        let ann_path = select_existing_ann_candidate(ann_path);
        match open_validated_semantic_artifact(
            &index_path,
            ann_path,
            embedder.id(),
            embedder.dimension(),
        ) {
            Ok(artifact) => vec![artifact],
            Err(context) => {
                return SemanticSetup {
                    availability: SemanticAvailability::LoadFailed { context },
                    context: None,
                };
            }
        }
    };

    let roles = Some(HashSet::from([ROLE_USER, ROLE_ASSISTANT]));
    let embedder = Arc::new(embedder) as Arc<dyn Embedder>;

    SemanticSetup {
        availability: SemanticAvailability::HashFallback,
        context: Some(SemanticContext {
            embedder,
            artifacts,
            quality_artifact: None,
            filter_maps,
            roles,
        }),
    }
}

/// Load semantic context. Kept for API compatibility with the pre-local-only
/// build; identical to [`load_semantic_context`] now that no update-check
/// path exists.
pub fn load_semantic_context_no_version_check(data_dir: &Path, db_path: &Path) -> SemanticSetup {
    load_semantic_context_inner(
        data_dir,
        db_path,
        active_policy_embedder_name(),
        false,
        false,
    )
}

fn load_semantic_context_inner(
    data_dir: &Path,
    db_path: &Path,
    embedder_name: &str,
    defer_embedder_load: bool,
    strict_read_only: bool,
) -> SemanticSetup {
    if let Some(availability) = selected_generation_owner_requirement(data_dir) {
        return SemanticSetup {
            availability,
            context: None,
        };
    }
    let Some(canonical_name) = FastEmbedder::canonical_name(embedder_name) else {
        return SemanticSetup {
            availability: SemanticAvailability::LoadFailed {
                context: format!(
                    "unsupported semantic embedder: {embedder_name}; supported: minilm, multilingual-minilm"
                ),
            },
            context: None,
        };
    };
    let Some(config) = FastEmbedder::config_for(canonical_name) else {
        return SemanticSetup {
            availability: SemanticAvailability::LoadFailed {
                context: format!("unknown semantic embedder: {embedder_name}"),
            },
            context: None,
        };
    };
    let Some(model_dir) = FastEmbedder::runtime_model_dir_for(data_dir, canonical_name) else {
        return SemanticSetup {
            availability: SemanticAvailability::LoadFailed {
                context: format!(
                    "no model directory mapping for semantic embedder: {embedder_name}"
                ),
            },
            context: None,
        };
    };
    let manifest =
        ModelManifest::for_embedder(canonical_name).unwrap_or_else(ModelManifest::minilm_v2);
    let semantic_policy = SemanticPolicy::resolve(&CliSemanticOverrides::default());
    let acquisition_policy = ModelAcquisitionPolicy::from_semantic_policy(&semantic_policy);
    let cache_report = classify_model_cache(&model_dir, &manifest, &acquisition_policy);

    if let Some(availability) =
        semantic_availability_from_cache_state(&config.embedder_id, &model_dir, &cache_report.state)
    {
        return SemanticSetup {
            availability,
            context: None,
        };
    }

    let index_path = vector_index_path(data_dir, &config.embedder_id);
    let monolithic_present = match semantic_artifact_candidate_exists(&index_path) {
        Ok(present) => present,
        Err(context) => {
            return SemanticSetup {
                availability: SemanticAvailability::LoadFailed { context },
                context: None,
            };
        }
    };
    let shard_artifacts = load_complete_shard_artifacts_for_current_db(
        data_dir,
        db_path,
        &config.embedder_id,
        config.dimension,
        "semantic",
        strict_read_only,
    );
    if !monolithic_present && shard_artifacts.is_none() {
        return SemanticSetup {
            availability: SemanticAvailability::IndexMissing { index_path },
            context: None,
        };
    }

    let storage = match if strict_read_only {
        FrankenStorage::open_strict_readonly(db_path)
    } else {
        FrankenStorage::open_readonly(db_path)
    } {
        Ok(storage) => storage,
        Err(err) => {
            return SemanticSetup {
                availability: SemanticAvailability::DatabaseUnavailable {
                    db_path: db_path.to_path_buf(),
                    error: err.to_string(),
                },
                context: None,
            };
        }
    };

    if let Some(availability) = refuse_stale_semantic_assets(
        data_dir,
        &storage,
        &config.embedder_id,
        &SemanticAvailability::Ready {
            embedder_id: config.embedder_id.clone(),
        },
        crate::search::asset_state::SemanticPreference::DefaultModel,
    ) {
        return SemanticSetup {
            availability,
            context: None,
        };
    }

    let filter_maps = match SemanticFilterMaps::from_storage(&storage) {
        Ok(maps) => maps,
        Err(err) => {
            return SemanticSetup {
                availability: SemanticAvailability::LoadFailed {
                    context: format!("filter maps: {err}"),
                },
                context: None,
            };
        }
    };

    let artifacts = if let Some(artifacts) = shard_artifacts {
        artifacts
    } else {
        let ann_path = hnsw_index_path(data_dir, &config.embedder_id);
        let ann_path = select_existing_ann_candidate(ann_path);
        match open_validated_semantic_artifact(
            &index_path,
            ann_path,
            &config.embedder_id,
            config.dimension,
        ) {
            Ok(artifact) => vec![artifact],
            Err(context) => {
                return SemanticSetup {
                    availability: SemanticAvailability::LoadFailed { context },
                    context: None,
                };
            }
        }
    };

    let embedder: Arc<dyn Embedder> = match if defer_embedder_load {
        LazyFastEmbedder::new(data_dir, canonical_name)
            .map(|embedder| Arc::new(embedder) as Arc<dyn Embedder>)
    } else {
        FastEmbedder::load_by_name(data_dir, canonical_name)
            .map(|embedder| Arc::new(embedder) as Arc<dyn Embedder>)
    } {
        Ok(embedder) => embedder,
        Err(err) => {
            return SemanticSetup {
                availability: SemanticAvailability::LoadFailed {
                    context: format!("model load: {err}"),
                },
                context: None,
            };
        }
    };

    let roles = Some(HashSet::from([ROLE_USER, ROLE_ASSISTANT]));

    SemanticSetup {
        availability: SemanticAvailability::Ready {
            embedder_id: embedder.id().to_string(),
        },
        context: Some(SemanticContext {
            embedder,
            artifacts,
            quality_artifact: None,
            filter_maps,
            roles,
        }),
    }
}

/// GH #404: refuse vector assets that load but do not describe the current
/// database.
///
/// `cass status` already computes a readiness verdict (`can_search`,
/// `fallback_mode`) from the semantic manifest against the live DB
/// fingerprint, but the query path used to admit any artifact that merely
/// parsed. A stale or mid-backfill FSVI/HNSW has no relevance floor, so an
/// out-of-vocabulary query returned `k` arbitrary neighbours while status
/// said `fallback_mode: "lexical"`. This applies the same verdict before a
/// context is handed to `SearchClient`, reusing the read-only handle the
/// caller already opened for filter maps (no second archive open).
///
/// Returns `Some(unavailable)` when serving must be refused; `None` when the
/// assets are current (or when there is no manifest to contradict them, which
/// preserves legacy manifest-less installs).
fn refuse_stale_semantic_assets(
    data_dir: &Path,
    storage: &FrankenStorage,
    served_embedder_id: &str,
    availability_if_served: &SemanticAvailability,
    preference: crate::search::asset_state::SemanticPreference,
) -> Option<SemanticAvailability> {
    let semantic_identity_tier = if served_embedder_id == HashEmbedder::default().id() {
        SemanticIdentityTier::Fast
    } else {
        SemanticIdentityTier::Quality
    };
    match storage.semantic_identity_rebuild_required(semantic_identity_tier) {
        Ok(true) => {
            return Some(SemanticAvailability::IndexStale {
                embedder_id: served_embedder_id.to_string(),
                reason: "canonical agent/source identity changed; run 'cass index --semantic' to rebuild semantic filter metadata"
                    .to_string(),
            });
        }
        Ok(false) => {}
        Err(err) => {
            return Some(SemanticAvailability::LoadFailed {
                context: format!("checking canonical semantic identity invalidation marker: {err}"),
            });
        }
    }
    let db_fingerprint =
        match crate::storage::archive_fingerprint::lexical_storage_fingerprint_for_storage(storage)
        {
        Ok(fingerprint) => fingerprint,
        Err(err) => {
            // The archive answered the open but not the fingerprint queries;
            // the filter-map load right after this will surface the real
            // error, so do not invent a staleness verdict here.
            tracing::debug!(
                error = %format!("{err:#}"),
                embedder = served_embedder_id,
                "semantic staleness gate skipped: could not fingerprint current DB"
            );
            return None;
        }
    };
    let state = crate::search::asset_state::semantic_state_from_availability(
        data_dir,
        availability_if_served,
        preference,
        Some(&db_fingerprint),
    );
    // The tier that actually backs the artifact being served. The overall
    // verdict can be `can_search` on the strength of the *other* tier (e.g.
    // a current hash fast tier while the MiniLM quality tier is stale), which
    // is fine for status but not for handing out this specific artifact.
    let served_tier = if served_embedder_id == HashEmbedder::default().id() {
        &state.fast_tier
    } else {
        &state.quality_tier
    };
    // Only a record for *this* embedder can vouch for or refuse this artifact;
    // a tier record built by some other embedder says nothing about it.
    let served_tier_describes_artifact =
        served_tier.present && served_tier.embedder_id.as_deref() == Some(served_embedder_id);
    let served_tier_not_current = served_tier_describes_artifact
        && (!served_tier.ready || served_tier.current_db_matches == Some(false));
    if state.can_search && !served_tier_not_current {
        return None;
    }
    let reason = match (&state.hint, served_tier_not_current) {
        (_, true) if state.can_search => format!(
            "published {} tier for {served_embedder_id} does not match the current database; run 'cass index --semantic' to refresh it",
            if served_embedder_id == HashEmbedder::default().id() {
                "fast"
            } else {
                "quality"
            }
        ),
        (Some(hint), _) => format!("{}; {hint}", state.summary),
        (None, _) => state.summary.clone(),
    };
    tracing::info!(
        embedder = served_embedder_id,
        status = state.status,
        fallback_mode = state.fallback_mode,
        "refusing semantic assets that do not describe the current database (GH #404)"
    );
    Some(SemanticAvailability::IndexStale {
        embedder_id: served_embedder_id.to_string(),
        reason,
    })
}

fn active_policy_embedder_name() -> &'static str {
    let semantic_policy = SemanticPolicy::resolve(&CliSemanticOverrides::default());
    FastEmbedder::canonical_name(&semantic_policy.quality_tier_embedder).unwrap_or("unsupported")
}

fn semantic_availability_from_cache_state(
    embedder_id: &str,
    model_dir: &Path,
    state: &ModelCacheState,
) -> Option<SemanticAvailability> {
    match state {
        ModelCacheState::Acquired { .. }
        | ModelCacheState::PreseededLocal { .. }
        | ModelCacheState::IncompatibleVersion { .. } => None,
        ModelCacheState::NotAcquired { missing_files } => {
            Some(SemanticAvailability::ModelMissing {
                embedder_id: embedder_id.to_string(),
                model_dir: model_dir.to_path_buf(),
                missing_files: missing_files.clone(),
            })
        }
        ModelCacheState::ChecksumMismatch {
            file,
            expected,
            actual,
        } => Some(SemanticAvailability::LoadFailed {
            context: format!(
                "model checksum mismatch for {file}: expected {expected}, got {actual}"
            ),
        }),
        ModelCacheState::DisabledByPolicy { reason } => Some(SemanticAvailability::Disabled {
            reason: reason.clone(),
        }),
        ModelCacheState::BudgetBlocked {
            required_bytes,
            max_bytes,
        } => Some(SemanticAvailability::Disabled {
            reason: format!(
                "semantic model requires {required_bytes} bytes but policy allows {max_bytes}"
            ),
        }),
        ModelCacheState::QuarantinedCorrupt {
            marker_path,
            reason,
        } => Some(SemanticAvailability::LoadFailed {
            context: format!(
                "model cache quarantined at {}: {reason}",
                marker_path.display()
            ),
        }),
    }
}

/// Check if the vector index needs rebuilding after a model upgrade.
///
/// This compares the embedder ID, vector-space revision, and dimension in the
/// vector index header with the current native MiniLM contract. Shape equality
/// alone cannot prove compatibility across inference-engine generations.
///
/// Returns `true` if rebuild is needed, `false` otherwise.
pub fn needs_index_rebuild(data_dir: &Path) -> bool {
    let index_path = vector_index_path(data_dir, FastEmbedder::embedder_id_static());

    if !index_path.is_file() {
        // Index doesn't exist, so it needs to be built (not rebuilt)
        return false;
    }

    // Try to load the index and check its embedder ID
    match VectorIndex::open(&index_path) {
        Ok(index) => {
            validate_vector_index_contract(&index, FastEmbedder::embedder_id_static(), 384).is_err()
        }
        Err(_) => {
            // Index is corrupted or unreadable, needs rebuild
            true
        }
    }
}

/// Delete the vector index to force a rebuild.
///
/// Call this after a model reinstall when the operator has decided to rebuild
/// the semantic index. The next index run will rebuild from scratch.
///
/// # Returns
///
/// `Ok(true)` if the index was deleted.
/// `Ok(false)` if the index didn't exist.
/// `Err(_)` if deletion failed.
pub fn delete_vector_index_for_rebuild(data_dir: &Path) -> std::io::Result<bool> {
    let index_path = vector_index_path(data_dir, FastEmbedder::embedder_id_static());

    if index_path.is_file() {
        std::fs::remove_file(&index_path)?;
        Ok(true)
    } else {
        Ok(false)
    }
}

/// Get the model directory path for the default MiniLM model.
pub fn default_model_dir(data_dir: &Path) -> PathBuf {
    FastEmbedder::default_model_dir(data_dir)
}

/// Get the model manifest for the default MiniLM model.
pub fn default_model_manifest() -> ModelManifest {
    ModelManifest::minilm_v2()
}

#[cfg(test)]
mod tests;
