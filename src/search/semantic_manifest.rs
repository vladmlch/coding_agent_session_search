//! Durable semantic asset manifest, backlog ledger, and resumable checkpoints.
//!
//! This module contains two deliberately separate contracts:
//!
//! - the historical mutable backfill ledger used to describe work in progress;
//! - the authoritative immutable generation manifest plus compare-and-swap
//!   `current.json` pointer used for runtime selection.
//!
//! Runtime readiness must come only from a fully validated selected generation.
//! Mutable ledger state never implies that an artifact is searchable.
//!
//! # Storage
//!
//! The historical backfill ledger remains at:
//! ```text
//! {data_dir}/vector_index/semantic_manifest.json
//! ```
//!
//! Authoritative immutable generations live at:
//! ```text
//! {data_dir}/vector_index/generations/{generation_id}/manifest.json
//! {data_dir}/vector_index/current.json
//! ```
//!
//! Each generation is sealed only after artifact durability is established.
//! Publication is a serialized compare-and-swap of `current.json`.
//!
//! # Relationship to other modules
//!
//! - **[`policy`]**: Provides the contract (versions, budgets, tier names) that
//!   this manifest fingerprints against.
//! - **[`asset_state`]**: Evaluates coarse readiness from this manifest plus
//!   live file probes.
//! - **[`model_manager`]**: Detects model availability; this module records
//!   which model was used to build each artifact.

mod atomic_io;
mod generation_manifest;
mod generation_pointer;
mod generation_types;
mod generation_validation;
mod legacy;
pub(crate) mod selection;
mod shards;
#[cfg(test)]
mod tests;

pub use atomic_io::ManifestError;
pub use generation_pointer::{
    load_current_semantic_generation, resolve_semantic_generation_disk_state,
};
pub use generation_types::{
    SEMANTIC_CURRENT_POINTER_FILENAME, SEMANTIC_CURRENT_POINTER_LOCK_FILENAME,
    SEMANTIC_CURRENT_POINTER_SCHEMA_VERSION, SEMANTIC_GENERATION_MANIFEST_FILENAME,
    SEMANTIC_GENERATION_MANIFEST_SCHEMA_VERSION, SEMANTIC_GENERATIONS_DIRNAME,
    SEMANTIC_STAGING_DIRNAME, SemanticAnnBaseBindingV1, SemanticArtifactRole,
    SemanticArtifactValidationEvidence, SemanticCorpusSnapshotIdentity, SemanticCurrentPointerV1,
    SemanticExpectedCurrentV1, SemanticGenerationArtifact, SemanticGenerationDiskStateRobotV1,
    SemanticGenerationDiskStateV1, SemanticGenerationError, SemanticGenerationManifestV1,
    SemanticGenerationRecoveryAction, SemanticGenerationStatus, SemanticGenerationTopology,
    SemanticManifestInvariantClass, SemanticProducerAttestation, ValidatedSemanticGeneration,
};
pub(crate) use legacy::semantic_shard_artifact_path_is_safe;
pub use legacy::{
    ArtifactRecord, BacklogLedger, BuildCheckpoint, HnswRecord, LEGACY_EXTERNAL_EMBEDDING_SPACE,
    MANIFEST_FILENAME, MANIFEST_FORMAT_VERSION, MANIFEST_FORMAT_VERSION_PRE_LAST_MESSAGE_CURSOR,
    SHARD_MANIFEST_FILENAME, SemanticManifest, SemanticShardManifest, SemanticShardRecord,
    SemanticShardSummary, TierKind, TierReadiness, is_external_identity,
};
pub use shards::{SEMANTIC_SHARDED_GENERATION_MANIFEST_SCHEMA_VERSION, SemanticArtifactShardV2};

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::fs::{self, OpenOptions};
use std::io::{Read as IoRead, Write as IoWrite};
use std::path::{Component, Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use frankensearch::core::{
    EMBEDDING_INPUT_CONTRACT_SCHEMA_V1, EMBEDDING_PRODUCER_ATTESTATION_SCHEMA_V1,
    EMBEDDING_SPACE_IDENTITY_SCHEMA_V1, EmbeddingSpaceKindV1, FrozenEmbeddingIdentityBundleV1,
    VECTOR_STORAGE_IDENTITY_SCHEMA_V1,
};
use frankensearch::index::wal_path_for;
use fs2::FileExt;

use super::policy::{
    CHUNKING_STRATEGY_VERSION, InvalidationAction, SEMANTIC_SCHEMA_VERSION,
    SemanticAssetManifest as PolicyManifest, SemanticPolicy,
};

#[cfg(test)]
use atomic_io::create_unique_manifest_temp_file;
#[cfg(all(unix, test))]
use atomic_io::replacement_path_entry_exists;
#[cfg(test)]
use generation_manifest::ManifestSealCheckpoint;
#[cfg(test)]
use generation_pointer::{
    PointerPublishCheckpoint, is_private_pointer_publish_temp_name, load_manifest_file,
    parse_generation_manifest_bytes,
};
use generation_pointer::{load_manifest_selected_by_pointer, parse_current_pointer_bytes};
use generation_types::MAX_SEMANTIC_POINTER_BYTES;
use generation_validation::{
    SemanticCanonicalEncoder, corpus_identity_sha256, invalid_manifest,
    log_generation_validation_failure, metadata_is_link_or_reparse,
    normalized_manifest_relative_path, portable_file_identity, read_bounded_file,
    reject_existing_path_links, validate_existing_generation_directory,
    verify_path_still_names_open_file,
};
#[cfg(test)]
use generation_validation::{
    fingerprint_prefix, semantic_generations_root, semantic_vector_root, sha256_hex,
};
