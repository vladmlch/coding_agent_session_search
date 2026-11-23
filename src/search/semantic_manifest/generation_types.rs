use super::generation_validation::fingerprint_prefix;
use super::shards::SemanticArtifactShardV2;
use super::{BTreeMap, Deserialize, FrozenEmbeddingIdentityBundleV1, PathBuf, Serialize};

// ─── Immutable semantic generation contract ──────────────────────────────

/// Schema version for immutable semantic generation manifests.
pub const SEMANTIC_GENERATION_MANIFEST_SCHEMA_VERSION: u32 = 1;

/// Schema version for the atomic current-generation pointer.
pub const SEMANTIC_CURRENT_POINTER_SCHEMA_VERSION: u32 = 1;

/// Filename of the atomic current-generation pointer.
pub const SEMANTIC_CURRENT_POINTER_FILENAME: &str = "current.json";

/// Advisory serialization lock for compare-and-swap pointer publication.
pub const SEMANTIC_CURRENT_POINTER_LOCK_FILENAME: &str = "current.lock";

pub(in crate::search::semantic_manifest) const SEMANTIC_CURRENT_POINTER_TEMP_PREFIX: &str =
    ".current.publish.";

/// Directory containing immutable semantic generations.
pub const SEMANTIC_GENERATIONS_DIRNAME: &str = "generations";

/// Directory containing unpublished semantic build state.
pub const SEMANTIC_STAGING_DIRNAME: &str = "staging";

/// Filename of an immutable generation manifest.
pub const SEMANTIC_GENERATION_MANIFEST_FILENAME: &str = "manifest.json";

pub(in crate::search::semantic_manifest) const SHA256_HEX_LENGTH: usize = 64;
pub(in crate::search::semantic_manifest) const MAX_GENERATION_ID_LENGTH: usize = 128;
pub(in crate::search::semantic_manifest) const MAX_SEMANTIC_POINTER_BYTES: usize = 16 * 1024;
pub(in crate::search::semantic_manifest) const MAX_SEMANTIC_MANIFEST_BYTES: usize = 1024 * 1024;
pub(in crate::search::semantic_manifest) const MAX_SEMANTIC_ARTIFACTS: usize = 4;
pub(in crate::search::semantic_manifest) const MAX_SEMANTIC_ARTIFACT_PATH_BYTES: usize = 1024;
pub(in crate::search::semantic_manifest) const MAX_PRODUCER_FEATURES: usize = 256;
pub(in crate::search::semantic_manifest) const MAX_PRODUCER_FEATURE_BYTES: usize = 128;
pub(in crate::search::semantic_manifest) const MAX_EMBEDDING_DIMENSION: u32 = 65_536;

/// Requested or realized semantic tier topology.
///
/// Topology records which tiers exist, not whether each tier covers the whole
/// selected corpus. Per-tier coverage lives on [`SemanticGenerationArtifact`],
/// so a sealed full-progressive generation can truthfully expose a complete
/// fast tier and a partially populated quality tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticGenerationTopology {
    FastOnly,
    QualityOnly,
    FullProgressive,
}

impl SemanticGenerationTopology {
    pub(in crate::search::semantic_manifest) fn includes_fast(self) -> bool {
        matches!(self, Self::FastOnly | Self::FullProgressive)
    }

    pub(in crate::search::semantic_manifest) fn includes_quality(self) -> bool {
        matches!(self, Self::QualityOnly | Self::FullProgressive)
    }

    pub(in crate::search::semantic_manifest) fn is_valid_realization_of(
        self,
        requested: Self,
    ) -> bool {
        match requested {
            Self::FastOnly => self == Self::FastOnly,
            Self::QualityOnly => self == Self::QualityOnly,
            Self::FullProgressive => true,
        }
    }
}

/// Immutable generation seal state.
///
/// Mutable build progress belongs under `vector_index/staging/`; a generation
/// eligible for `current.json` is durably sealed by construction. `Sealed`
/// deliberately does not mean that every tier has full corpus coverage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticGenerationStatus {
    /// Staging-only state. Deserializable for a typed failure, never sealable
    /// or selectable as an immutable generation.
    Building,
    Sealed,
}

/// Role of one manifest-declared semantic artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticArtifactRole {
    FastVector,
    QualityVector,
    FastAnn,
    QualityAnn,
}

impl SemanticArtifactRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FastVector => "fast_vector",
            Self::QualityVector => "quality_vector",
            Self::FastAnn => "fast_ann",
            Self::QualityAnn => "quality_ann",
        }
    }

    pub(in crate::search::semantic_manifest) fn is_vector(self) -> bool {
        matches!(self, Self::FastVector | Self::QualityVector)
    }

    pub(in crate::search::semantic_manifest) fn base_vector_role(self) -> Option<Self> {
        match self {
            Self::FastAnn => Some(Self::FastVector),
            Self::QualityAnn => Some(Self::QualityVector),
            Self::FastVector | Self::QualityVector => None,
        }
    }
}

/// Exact binding from an ANN artifact to its vector base and build parameters.
///
/// The base artifact bytes, canonical Frankensearch storage contract, and
/// producer-supplied vector-layout parameters are all bound independently.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticAnnBaseBindingV1 {
    pub base_role: SemanticArtifactRole,
    pub base_artifact_sha256: String,
    pub base_storage_fingerprint: String,
    pub base_artifact_parameters_sha256: String,
    pub algorithm: String,
    pub metric: String,
    pub parameters_sha256: String,
}

/// Strong identity for the canonical corpus snapshot used by a generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticCorpusSnapshotIdentity {
    pub database_uuid: String,
    pub source_change_sequence: u64,
    pub schema_contract_sha256: String,
    pub content_selection_sha256: String,
    pub canonicalization_sha256: String,
    pub chunking_sha256: String,
    pub document_id_contract_sha256: String,
    pub config_sha256: String,
    pub corpus_sha256: String,
    pub ordered_live_docset_sha256: String,
    pub content_sha256: String,
    pub document_count: u64,
}

/// Reproducible producer provenance for every artifact in a generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticProducerAttestation {
    pub cass_git_commit: String,
    pub frankensearch_git_commit: String,
    pub cargo_lock_sha256: String,
    pub rust_toolchain: String,
    pub enabled_features: Vec<String>,
    pub build_profile: String,
    pub binary_elf_sha256: String,
}

/// Evidence captured when the producer inspected an artifact before sealing
/// the immutable generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticArtifactValidationEvidence {
    pub validator_id: String,
    pub validated_at_ms: i64,
    pub artifact_sha256: String,
    pub size_bytes: u64,
    pub selected_document_count: u64,
    pub covered_document_count: u64,
    pub vector_slot_count: u64,
    pub live_vector_count: u64,
    pub tombstone_vector_count: u64,
    pub wal_entry_count: u64,
    pub generation_corpus_sha256: String,
    pub covered_live_docset_sha256: String,
    pub covered_content_sha256: String,
}

/// Exact description of one immutable vector or ANN artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticGenerationArtifact {
    pub role: SemanticArtifactRole,
    /// Explicit ordered partition binding for schema v2; absent in schema v1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard: Option<SemanticArtifactShardV2>,
    /// Manifest-relative path beneath the selected generation directory.
    pub relative_path: String,
    pub artifact_sha256: String,
    pub size_bytes: u64,
    /// Exact artifact-container format, for example `fsvi-v2` or `hnsw-v1`.
    pub artifact_format: String,
    /// Digest of all format/layout/build parameters not already represented by
    /// the canonical embedding identity.
    pub artifact_parameters_sha256: String,
    /// The accepted Frankensearch identity contract, stored losslessly. CASS
    /// never redefines or weakens embedding-space compatibility.
    pub embedding_identity: FrozenEmbeddingIdentityBundleV1,
    /// Required for ANN roles and forbidden for vector roles.
    pub ann_base: Option<SemanticAnnBaseBindingV1>,
    /// Number of live corpus documents selected for this generation.
    pub selected_document_count: u64,
    /// Number of selected documents represented by a live vector in this tier.
    pub covered_document_count: u64,
    /// Physical slots in the artifact, including tombstones.
    pub vector_slot_count: u64,
    pub live_vector_count: u64,
    pub tombstone_vector_count: u64,
    pub wal_entry_count: u64,
    pub generation_corpus_sha256: String,
    /// Exact ordered identity of the live documents covered by this tier.
    pub covered_live_docset_sha256: String,
    /// Exact content identity of the live documents covered by this tier.
    pub covered_content_sha256: String,
    pub validation: SemanticArtifactValidationEvidence,
}

/// One immutable, self-describing semantic generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticGenerationManifestV1 {
    pub schema_version: u32,
    pub generation_id: String,
    pub build_id: String,
    pub request_id: Option<String>,
    pub previous_generation_id: Option<String>,
    pub created_at_ms: i64,
    pub completed_at_ms: i64,
    /// Time immutable artifacts and this manifest were durably sealed.
    pub sealed_at_ms: i64,
    pub requested_topology: SemanticGenerationTopology,
    pub realized_topology: SemanticGenerationTopology,
    pub status: SemanticGenerationStatus,
    pub corpus: SemanticCorpusSnapshotIdentity,
    pub producer: SemanticProducerAttestation,
    /// Exact selected live-document count; equal to `corpus.document_count`.
    pub selected_document_count: u64,
    /// Sum of physical vector slots across vector tiers. A full two-tier
    /// generation therefore normally counts each covered document twice.
    pub total_vector_slot_count_across_tiers: u64,
    /// Sum of live vectors across vector tiers, not a unique-document count.
    pub total_live_vector_count_across_tiers: u64,
    /// Sum of tombstone slots across vector tiers.
    pub total_tombstone_vector_count_across_tiers: u64,
    /// Sum of WAL entries represented across vector tiers.
    pub total_wal_entry_count_across_tiers: u64,
    /// Sorted by role. Stable ordering is part of canonical serialization.
    pub artifacts: Vec<SemanticGenerationArtifact>,
}

/// Atomic pointer selecting exactly one immutable semantic generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticCurrentPointerV1 {
    pub schema_version: u32,
    pub generation_id: String,
    pub manifest_sha256: String,
    /// Monotone publication epoch allocated under the current-pointer CAS.
    /// Epoch 1 is the first selection; every successful replacement is exactly
    /// the previous epoch plus one.
    pub selection_epoch: u64,
    /// Time this pointer selection occurred. This is not part of the immutable
    /// generation and is informational; CAS ordering uses `selection_epoch`.
    pub selected_at_ms: i64,
}

/// Required compare-and-swap precondition for current-pointer publication.
///
/// There is intentionally no `Any` variant: every publisher must prove either
/// that no current generation exists or that it observed the entire exact
/// pointer state it intends to replace. Capturing the monotone
/// `selection_epoch` closes the A -> B -> A ABA hole that generation and
/// manifest identity alone cannot detect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SemanticExpectedCurrentV1 {
    Absent,
    Exact(SemanticCurrentPointerV1),
}

impl SemanticExpectedCurrentV1 {
    pub fn exact(pointer: &SemanticCurrentPointerV1) -> Self {
        Self::Exact(pointer.clone())
    }
}

/// A current generation whose pointer, manifest, corpus identity, paths, and
/// artifact bytes all passed fail-closed validation.
#[derive(Debug, Clone)]
pub struct ValidatedSemanticGeneration {
    pub pointer: SemanticCurrentPointerV1,
    pub manifest: SemanticGenerationManifestV1,
    pub generation_dir: PathBuf,
    /// Diagnostic resolution only. Serving code must not reopen these paths:
    /// the retained immutable FSVI witness/search owner is gated by
    /// `coding_agent_session_search-cass-fsvi-semantic-witness-xbq0b`.
    pub artifact_paths: BTreeMap<SemanticArtifactRole, PathBuf>,
}

/// Minimal, serializable disk-resolution vocabulary shared with doctor/robot
/// surfaces. Full remediation planning remains downstream, but these states
/// must never collapse into an ambiguous "semantic missing" result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum SemanticGenerationDiskStateV1 {
    Absent,
    Building {
        build_count: u64,
        unselected_generation_directory_count: u64,
        legacy_artifact_count: u64,
    },
    InterruptedPublish {
        temporary_entry_count: u64,
        build_count: u64,
        unselected_generation_directory_count: u64,
        legacy_artifact_count: u64,
    },
    PointerMissingWithGenerationDirectories {
        generation_directory_count: u64,
        legacy_artifact_count: u64,
    },
    LegacyUnidentified {
        artifact_count: u64,
    },
    Ready {
        generation_id: String,
        manifest_sha256: String,
        selection_epoch: u64,
        selected_at_ms: i64,
        requested_topology: SemanticGenerationTopology,
        realized_topology: SemanticGenerationTopology,
    },
}

impl SemanticGenerationDiskStateV1 {
    pub fn reason_code(&self) -> Option<&'static str> {
        match self {
            Self::Absent => Some("semantic_generation_absent"),
            Self::Building { .. } => Some("semantic_generation_building"),
            Self::InterruptedPublish { .. } => Some("semantic_pointer_publish_interrupted"),
            Self::PointerMissingWithGenerationDirectories { .. } => {
                Some("semantic_pointer_missing_with_generation_directories")
            }
            Self::LegacyUnidentified { .. } => Some("semantic_generation_legacy_unidentified"),
            Self::Ready { .. } => None,
        }
    }

    pub fn recovery_action(&self) -> Option<SemanticGenerationRecoveryAction> {
        match self {
            Self::Absent => Some(SemanticGenerationRecoveryAction::ProvisionAndIndex),
            Self::Building { .. } => Some(SemanticGenerationRecoveryAction::WaitForBuild),
            Self::InterruptedPublish { .. } => Some(SemanticGenerationRecoveryAction::RetryPublish),
            Self::PointerMissingWithGenerationDirectories { .. } => {
                Some(SemanticGenerationRecoveryAction::RollbackOrReindex)
            }
            Self::LegacyUnidentified { .. } => {
                Some(SemanticGenerationRecoveryAction::ReindexRequired)
            }
            Self::Ready { .. } => None,
        }
    }

    pub fn robot_projection(&self) -> SemanticGenerationDiskStateRobotV1 {
        SemanticGenerationDiskStateRobotV1 {
            schema_version: 1,
            state: self.clone(),
            reason_code: self.reason_code().map(str::to_owned),
            recovery_action: self.recovery_action(),
        }
    }
}

impl std::fmt::Display for SemanticGenerationDiskStateV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Absent => write!(
                formatter,
                "no semantic generation exists; provision an embedder and build the initial index"
            ),
            Self::Building {
                build_count,
                unselected_generation_directory_count,
                legacy_artifact_count,
            } => write!(
                formatter,
                "staged semantic build count: {build_count}; unselected generation candidate directory count: {unselected_generation_directory_count}; unselected legacy artifact count: {legacy_artifact_count}"
            ),
            Self::InterruptedPublish {
                temporary_entry_count,
                build_count,
                unselected_generation_directory_count,
                legacy_artifact_count,
            } => write!(
                formatter,
                "interrupted pointer publication temp count: {temporary_entry_count}; staged build count: {build_count}; unselected generation candidate directory count: {unselected_generation_directory_count}; legacy artifact count: {legacy_artifact_count}"
            ),
            Self::PointerMissingWithGenerationDirectories {
                generation_directory_count,
                legacy_artifact_count,
            } => write!(
                formatter,
                "current semantic pointer is missing; unselected generation candidate directory count: {generation_directory_count}; legacy artifact count: {legacy_artifact_count}; validate a candidate fully before rollback, otherwise reindex"
            ),
            Self::LegacyUnidentified { artifact_count } => write!(
                formatter,
                "legacy semantic artifacts without authoritative identity: {artifact_count}"
            ),
            Self::Ready {
                generation_id,
                requested_topology,
                realized_topology,
                ..
            } => write!(
                formatter,
                "semantic generation {} is ready ({requested_topology:?} requested, {realized_topology:?} realized)",
                fingerprint_prefix(generation_id)
            ),
        }
    }
}

/// Stable robot envelope for disk-state diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticGenerationDiskStateRobotV1 {
    pub schema_version: u32,
    pub state: SemanticGenerationDiskStateV1,
    pub reason_code: Option<String>,
    pub recovery_action: Option<SemanticGenerationRecoveryAction>,
}

/// Stable action class downstream readiness surfaces can turn into a richer
/// `RecoveryPlan` without guessing from error strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticGenerationRecoveryAction {
    ProvisionAndIndex,
    ReindexRequired,
    RollbackOrReindex,
    UpgradeRequired,
    RetryPublish,
    WaitForBuild,
    InspectCorruption,
}

/// Stable classification for a rejected manifest invariant.
///
/// Downstream diagnostics must branch on this enum/reason code, never parse a
/// human explanation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticManifestInvariantClass {
    Identity,
    Path,
    Topology,
    Count,
    SealState,
    HistoricalSchema,
    PartialCoverage,
    AnnBinding,
    Timestamp,
    Provenance,
}

/// Fail-closed current-generation resolution errors.
#[derive(Debug)]
pub enum SemanticGenerationError {
    MissingPointer,
    BuildingGeneration {
        generation_id: String,
        build_id: String,
    },
    LegacyUnidentified {
        artifact_count: u64,
    },
    PointerIo {
        source: String,
    },
    PointerParse {
        source: String,
    },
    UnsupportedPointerSchema {
        found: u64,
        supported: u64,
    },
    InvalidPointer {
        reason: String,
    },
    MissingManifest {
        generation_id: String,
    },
    ManifestIo {
        generation_id: String,
        source: String,
    },
    ManifestParse {
        generation_id: String,
        source: String,
    },
    UnsupportedManifestSchema {
        found: u64,
        supported: u64,
    },
    UnsupportedEmbeddingIdentitySchema {
        component: String,
        found: u64,
        supported: u64,
    },
    InvalidManifest {
        class: SemanticManifestInvariantClass,
        reason: String,
    },
    ManifestDigestMismatch {
        expected: String,
        actual: String,
    },
    StaleCorpus {
        expected: String,
        actual: String,
    },
    ArtifactIo {
        role: SemanticArtifactRole,
        source: String,
    },
    ArtifactSizeMismatch {
        role: SemanticArtifactRole,
        expected: u64,
        actual: u64,
    },
    ArtifactDigestMismatch {
        role: SemanticArtifactRole,
        expected: String,
        actual: String,
    },
    EmbeddingIdentityMismatch {
        role: SemanticArtifactRole,
        expected: String,
        actual: String,
        reason: String,
    },
    ImmutableManifestConflict {
        generation_id: String,
    },
    ConcurrentPublishConflict {
        expected: String,
        actual: String,
    },
    SelectionEpochNotAdvanced {
        prior_selection_epoch: u64,
        proposed_selection_epoch: u64,
    },
    PublishLockIo {
        source: String,
    },
    Serialize {
        source: String,
    },
}

impl SemanticGenerationError {
    pub fn reason_code(&self) -> &'static str {
        match self {
            Self::MissingPointer => "semantic_pointer_missing",
            Self::BuildingGeneration { .. } => "semantic_generation_building",
            Self::LegacyUnidentified { .. } => "semantic_generation_legacy_unidentified",
            Self::PointerIo { .. } => "semantic_pointer_io",
            Self::PointerParse { .. } => "semantic_pointer_corrupt",
            Self::UnsupportedPointerSchema { .. } => "semantic_pointer_upgrade_required",
            Self::InvalidPointer { .. } => "semantic_pointer_invalid",
            Self::MissingManifest { .. } => "semantic_manifest_missing",
            Self::ManifestIo { .. } => "semantic_manifest_io",
            Self::ManifestParse { .. } => "semantic_manifest_corrupt",
            Self::UnsupportedManifestSchema { .. } => "semantic_manifest_upgrade_required",
            Self::UnsupportedEmbeddingIdentitySchema { .. } => {
                "semantic_embedding_identity_upgrade_required"
            }
            Self::InvalidManifest { class, .. } => match class {
                SemanticManifestInvariantClass::Identity => "semantic_manifest_identity_invalid",
                SemanticManifestInvariantClass::Path => "semantic_manifest_path_invalid",
                SemanticManifestInvariantClass::Topology => "semantic_manifest_topology_invalid",
                SemanticManifestInvariantClass::Count => "semantic_manifest_count_invalid",
                SemanticManifestInvariantClass::SealState => "semantic_manifest_not_durably_sealed",
                SemanticManifestInvariantClass::HistoricalSchema => {
                    "semantic_manifest_historical_schema"
                }
                SemanticManifestInvariantClass::PartialCoverage => {
                    "semantic_manifest_partial_coverage_invalid"
                }
                SemanticManifestInvariantClass::AnnBinding => {
                    "semantic_manifest_ann_binding_invalid"
                }
                SemanticManifestInvariantClass::Timestamp => "semantic_manifest_timestamp_invalid",
                SemanticManifestInvariantClass::Provenance => {
                    "semantic_manifest_provenance_invalid"
                }
            },
            Self::ManifestDigestMismatch { .. } => "semantic_manifest_digest_mismatch",
            Self::StaleCorpus { .. } => "semantic_generation_stale_source",
            Self::ArtifactIo { .. } => "semantic_artifact_io",
            Self::ArtifactSizeMismatch { .. } => "semantic_artifact_size_mismatch",
            Self::ArtifactDigestMismatch { .. } => "semantic_artifact_digest_mismatch",
            Self::EmbeddingIdentityMismatch { .. } => "semantic_embedding_identity_mismatch",
            Self::ImmutableManifestConflict { .. } => "semantic_manifest_immutable_conflict",
            Self::ConcurrentPublishConflict { .. } => "semantic_pointer_cas_conflict",
            Self::SelectionEpochNotAdvanced { .. } => {
                "semantic_pointer_selection_epoch_not_advanced"
            }
            Self::PublishLockIo { .. } => "semantic_pointer_lock_io",
            Self::Serialize { .. } => "semantic_manifest_serialize",
        }
    }

    pub fn recovery_action(&self) -> SemanticGenerationRecoveryAction {
        match self {
            Self::UnsupportedPointerSchema { .. }
            | Self::UnsupportedManifestSchema { .. }
            | Self::UnsupportedEmbeddingIdentitySchema { .. } => {
                SemanticGenerationRecoveryAction::UpgradeRequired
            }
            Self::MissingPointer
            | Self::StaleCorpus { .. }
            | Self::LegacyUnidentified { .. }
            | Self::ImmutableManifestConflict { .. } => {
                SemanticGenerationRecoveryAction::ReindexRequired
            }
            Self::BuildingGeneration { .. } => SemanticGenerationRecoveryAction::WaitForBuild,
            Self::ConcurrentPublishConflict { .. }
            | Self::SelectionEpochNotAdvanced { .. }
            | Self::PublishLockIo { .. } => SemanticGenerationRecoveryAction::RetryPublish,
            Self::PointerParse { .. } | Self::InvalidPointer { .. } | Self::Serialize { .. } => {
                SemanticGenerationRecoveryAction::InspectCorruption
            }
            Self::PointerIo { .. }
            | Self::ManifestIo { .. }
            | Self::MissingManifest { .. }
            | Self::ManifestParse { .. }
            | Self::InvalidManifest { .. }
            | Self::ManifestDigestMismatch { .. }
            | Self::ArtifactIo { .. }
            | Self::ArtifactSizeMismatch { .. }
            | Self::ArtifactDigestMismatch { .. }
            | Self::EmbeddingIdentityMismatch { .. } => {
                SemanticGenerationRecoveryAction::RollbackOrReindex
            }
        }
    }
}

impl std::fmt::Display for SemanticGenerationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingPointer => write!(f, "semantic current pointer is missing"),
            Self::BuildingGeneration {
                generation_id,
                build_id,
            } => write!(
                f,
                "semantic generation {} from build {} is still building",
                fingerprint_prefix(generation_id),
                fingerprint_prefix(build_id)
            ),
            Self::LegacyUnidentified { artifact_count } => write!(
                f,
                "found {artifact_count} legacy semantic artifact entries without authoritative identity"
            ),
            Self::PointerIo { .. } => write!(f, "semantic pointer I/O failure"),
            Self::PointerParse { .. } => write!(f, "semantic pointer parse failure"),
            Self::UnsupportedPointerSchema { found, supported } => write!(
                f,
                "semantic pointer schema {found} is newer than supported schema {supported}"
            ),
            Self::InvalidPointer { .. } => write!(f, "semantic pointer failed validation"),
            Self::MissingManifest { generation_id } => {
                write!(
                    f,
                    "semantic manifest is missing for generation {}",
                    fingerprint_prefix(generation_id)
                )
            }
            Self::ManifestIo { generation_id, .. } => write!(
                f,
                "semantic manifest I/O failure for generation {}",
                fingerprint_prefix(generation_id)
            ),
            Self::ManifestParse { generation_id, .. } => write!(
                f,
                "semantic manifest parse failure for generation {}",
                fingerprint_prefix(generation_id)
            ),
            Self::UnsupportedManifestSchema { found, supported } => write!(
                f,
                "semantic generation schema {found} is newer than supported schema {supported}"
            ),
            Self::UnsupportedEmbeddingIdentitySchema {
                component,
                found,
                supported,
            } => write!(
                f,
                "semantic embedding identity component {component} schema {found} is newer than supported schema {supported}"
            ),
            Self::InvalidManifest { class, .. } => {
                write!(f, "semantic generation failed {class:?} validation")
            }
            Self::ManifestDigestMismatch { expected, actual } => write!(
                f,
                "semantic generation manifest digest mismatch: expected {}, got {}",
                fingerprint_prefix(expected),
                fingerprint_prefix(actual)
            ),
            Self::StaleCorpus { expected, actual } => write!(
                f,
                "semantic generation corpus is stale: expected {}, got {}",
                fingerprint_prefix(expected),
                fingerprint_prefix(actual)
            ),
            Self::ArtifactIo { role, .. } => {
                write!(f, "semantic {} artifact I/O failure", role.as_str())
            }
            Self::ArtifactSizeMismatch {
                role,
                expected,
                actual,
            } => write!(
                f,
                "semantic {} artifact size mismatch: expected {expected}, got {actual}",
                role.as_str()
            ),
            Self::ArtifactDigestMismatch {
                role,
                expected,
                actual,
            } => write!(
                f,
                "semantic {} artifact digest mismatch: expected {}, got {}",
                role.as_str(),
                fingerprint_prefix(expected),
                fingerprint_prefix(actual)
            ),
            Self::EmbeddingIdentityMismatch {
                role,
                expected,
                actual,
                ..
            } => write!(
                f,
                "semantic {} embedding identity mismatch: expected {}, got {}",
                role.as_str(),
                fingerprint_prefix(expected),
                fingerprint_prefix(actual)
            ),
            Self::ImmutableManifestConflict { generation_id } => write!(
                f,
                "immutable semantic manifest conflicts for generation {}",
                fingerprint_prefix(generation_id)
            ),
            Self::ConcurrentPublishConflict { expected, actual } => write!(
                f,
                "semantic pointer compare-and-swap conflict: expected {}, found {}",
                fingerprint_prefix(expected),
                fingerprint_prefix(actual)
            ),
            Self::SelectionEpochNotAdvanced {
                prior_selection_epoch,
                proposed_selection_epoch,
            } => write!(
                f,
                "semantic pointer selection epoch must advance exactly once: prior {prior_selection_epoch}, proposed {proposed_selection_epoch}"
            ),
            Self::PublishLockIo { .. } => {
                write!(f, "semantic pointer publication lock I/O failure")
            }
            Self::Serialize { .. } => {
                write!(f, "semantic generation serialization failure")
            }
        }
    }
}

impl std::error::Error for SemanticGenerationError {}
