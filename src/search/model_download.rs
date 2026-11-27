//! Model manifest, cache-state classification, and local-file installation.
//!
//! This module handles:
//! - Model manifest with pinned revisions, sizes, and SHA256 checksums
//! - On-disk cache-state classification (`ModelCacheState`)
//! - Installation from a local directory (`ModelInstaller`) with SHA256
//!   verification and atomic publication (staging dir -> rename)
//!
//! cass is fully local-only: **there is no downloader, mirror, resume, or
//! update-check path**. The HuggingFace `repo`/`revision` manifest fields
//! are provenance metadata only — they tell the operator where the pinned
//! file set came from so the files can be obtained outside cass and handed
//! to `cass models install --from-file <DIR>`.
//!
//! **Note**: The core types (`ModelFile`, `ModelManifest`) are
//! structurally identical to those in `frankensearch_embed::model_manifest`.
//! They are kept locally for now due to build-system sync constraints.
//! See frankensearch-embed for the canonical definitions.

use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::search::policy::SemanticPolicy;

/// Installed-model presence on disk.
///
/// cass has no download lifecycle: a model is either absent (or incomplete,
/// which is still absent) or ready. Partial/corrupt states are surfaced
/// through [`ModelCacheState`] instead.
#[derive(Debug, Clone, PartialEq)]
pub enum ModelState {
    /// Model not installed on disk (or incomplete/markerless).
    NotInstalled,
    /// Model ready for use.
    Ready,
}

impl ModelState {
    /// Whether the model is ready for use.
    pub fn is_ready(&self) -> bool {
        matches!(self, ModelState::Ready)
    }

    /// Human-readable summary of the state.
    pub fn summary(&self) -> String {
        match self {
            ModelState::NotInstalled => "not installed".into(),
            ModelState::Ready => "ready".into(),
        }
    }
}

/// Policy inputs that constrain semantic model acquisition.
///
/// This is intentionally local and explicit: callers can construct it from the
/// resolved semantic policy, CLI flags, test fixtures, or future persisted
/// config without hiding why acquisition is blocked. Acquisition always means
/// a local `cass models install --from-file <DIR>` — there is no network path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelAcquisitionPolicy {
    /// Whether semantic model installation is enabled at all.
    pub install_enabled: bool,
    /// Maximum allowed size for this model install.
    pub max_model_bytes: Option<u64>,
    /// Human-readable provenance for the active policy.
    pub config_source: String,
}

impl Default for ModelAcquisitionPolicy {
    fn default() -> Self {
        Self {
            install_enabled: true,
            max_model_bytes: None,
            config_source: "compiled_default".to_string(),
        }
    }
}

impl ModelAcquisitionPolicy {
    /// Build acquisition constraints from the resolved semantic policy.
    pub fn from_semantic_policy(policy: &SemanticPolicy) -> Self {
        const MIB: u64 = 1_048_576;

        Self {
            install_enabled: policy.mode.should_build_semantic(),
            max_model_bytes: Some(policy.max_model_size_mb.saturating_mul(MIB)),
            config_source: "semantic_policy".to_string(),
        }
    }
}

/// Precise on-disk semantic model cache state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum ModelCacheState {
    /// Required files are absent or incomplete.
    NotAcquired {
        /// Missing manifest files, expressed as local filenames.
        missing_files: Vec<String>,
    },
    /// All files are installed, verified, and revision-compatible.
    Acquired { model_dir: PathBuf },
    /// At least one file exists but its checksum does not match the manifest.
    ChecksumMismatch {
        file: String,
        expected: String,
        actual: String,
    },
    /// Files are complete, but the installed revision does not match the manifest.
    IncompatibleVersion {
        current_revision: String,
        expected_revision: String,
    },
    /// Semantic model acquisition is disabled by user or policy.
    DisabledByPolicy { reason: String },
    /// The model exceeds the active byte budget.
    BudgetBlocked { required_bytes: u64, max_bytes: u64 },
    /// A previous corrupt cache has been quarantined.
    QuarantinedCorrupt {
        marker_path: PathBuf,
        reason: String,
    },
    /// Files were preseeded locally and verified without a cass marker.
    PreseededLocal { model_dir: PathBuf },
}

impl ModelCacheState {
    /// Stable machine-readable state code.
    pub fn code(&self) -> &'static str {
        match self {
            Self::NotAcquired { .. } => "not_acquired",
            Self::Acquired { .. } => "acquired",
            Self::ChecksumMismatch { .. } => "checksum_mismatch",
            Self::IncompatibleVersion { .. } => "incompatible_version",
            Self::DisabledByPolicy { .. } => "disabled_by_policy",
            Self::BudgetBlocked { .. } => "budget_blocked",
            Self::QuarantinedCorrupt { .. } => "quarantined_corrupt",
            Self::PreseededLocal { .. } => "preseeded_local",
        }
    }

    /// Human-readable state detail for CLI and robot diagnostics.
    pub fn summary(&self) -> String {
        match self {
            Self::NotAcquired { missing_files } => {
                format!("model not installed; missing {}", missing_files.join(", "))
            }
            Self::Acquired { .. } => "model cache acquired and verified".to_string(),
            Self::ChecksumMismatch {
                file,
                expected,
                actual,
            } => format!("checksum mismatch for {file}: expected {expected}, got {actual}"),
            Self::IncompatibleVersion {
                current_revision,
                expected_revision,
            } => format!("model revision mismatch: {current_revision} != {expected_revision}"),
            Self::DisabledByPolicy { reason } => format!("model acquisition disabled: {reason}"),
            Self::BudgetBlocked {
                required_bytes,
                max_bytes,
            } => {
                format!("model requires {required_bytes} bytes but policy allows {max_bytes} bytes")
            }
            Self::QuarantinedCorrupt { reason, .. } => {
                format!("model cache quarantined: {reason}")
            }
            Self::PreseededLocal { .. } => "preseeded local model cache verified".to_string(),
        }
    }

    /// Suggested next operator action. `model_name` is the canonical registry
    /// name used in the generated `cass models install` command.
    pub fn next_step(&self, model_name: &str) -> Option<String> {
        match self {
            Self::NotAcquired { .. } => Some(format!(
                "cass models install --model {model_name} --from-file <DIR>"
            )),
            Self::Acquired { .. } | Self::PreseededLocal { .. } => None,
            Self::ChecksumMismatch { .. } | Self::QuarantinedCorrupt { .. } => Some(format!(
                "reinstall from verified local files: cass models install --model {model_name} --from-file <DIR>"
            )),
            Self::IncompatibleVersion { .. } => Some(format!(
                "cass models install --model {model_name} --from-file <DIR> to refresh the model cache"
            )),
            Self::DisabledByPolicy { .. } => {
                Some("Use lexical search or re-enable semantic model acquisition in policy.".into())
            }
            Self::BudgetBlocked { .. } => {
                Some("Increase the semantic model budget or keep using lexical search.".into())
            }
        }
    }

    /// Whether the installed files can be used by the embedder immediately.
    pub fn is_usable(&self) -> bool {
        matches!(self, Self::Acquired { .. } | Self::PreseededLocal { .. })
    }
}

/// Machine-readable report for semantic model cache lifecycle decisions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCacheReport {
    pub model_id: String,
    pub model_dir: PathBuf,
    pub state: ModelCacheState,
    pub required_size_bytes: u64,
    pub installed_size_bytes: u64,
    pub policy_source: String,
}

impl ModelCacheReport {
    /// Stable machine-readable state code.
    pub fn state_code(&self) -> &'static str {
        self.state.code()
    }

    /// Whether the model cache can be used by semantic search.
    pub fn is_usable(&self) -> bool {
        self.state.is_usable()
    }
}

/// A file in the model manifest.
///
/// Structurally identical to `frankensearch_embed::ModelFile`.
#[derive(Debug, Clone)]
pub struct ModelFile {
    /// File path relative to repo root (e.g., "model.onnx" or "onnx/model.onnx").
    pub name: String,
    /// Expected SHA256 hash (hex string).
    pub sha256: String,
    /// Expected file size in bytes.
    pub size: u64,
}

impl ModelFile {
    /// Get the local filename (basename) for saving.
    ///
    /// For paths like "onnx/model.onnx", returns "model.onnx".
    /// This handles HuggingFace repos that restructure files into subdirectories.
    pub fn local_name(&self) -> &str {
        self.name.rsplit('/').next().unwrap_or(&self.name)
    }
}

/// Model manifest describing the files required for a local install.
///
/// Structurally compatible with `frankensearch_embed::ModelManifest`
/// (which has additional optional fields: version, display_name, description,
/// dimension, tier, download_size_bytes).
#[derive(Debug, Clone)]
pub struct ModelManifest {
    /// Model identifier (e.g., "all-minilm-l6-v2").
    pub id: String,
    /// HuggingFace repository.
    pub repo: String,
    /// Pinned revision (commit SHA).
    pub revision: String,
    /// Files that must be present in the source directory and installed.
    pub files: Vec<ModelFile>,
    /// License identifier.
    pub license: String,
}

/// Placeholder checksum value used for unverified manifests.
///
/// When a manifest file has this checksum, it means the model has not been
/// obtained and verified yet. The installer will reject such files.
pub const PLACEHOLDER_CHECKSUM: &str = "PLACEHOLDER_VERIFY_AFTER_DOWNLOAD";

impl ModelManifest {
    /// Check if this manifest has verified checksums for all files.
    ///
    /// Returns `false` if any file has the placeholder checksum, indicating
    /// the model files have not been obtained and verified yet.
    pub fn has_verified_checksums(&self) -> bool {
        self.files.iter().all(|f| f.sha256 != PLACEHOLDER_CHECKSUM)
    }

    /// Check if this manifest has a pinned revision (not "main").
    ///
    /// Unpinned revisions ("main") are not reproducible since the content
    /// can change at any time on HuggingFace.
    pub fn has_pinned_revision(&self) -> bool {
        self.revision != "main"
    }

    /// Check if this manifest is production-ready.
    ///
    /// A manifest is production-ready if it has:
    /// - All checksums verified (no placeholders)
    /// - A pinned revision (not "main")
    pub fn is_production_ready(&self) -> bool {
        self.has_verified_checksums() && self.has_pinned_revision()
    }

    /// Get the default MiniLM model manifest (baseline for bake-off).
    ///
    /// The revision and checksums are pinned for reproducibility.
    /// Updated 2026-01-13: HuggingFace restructured the repo - ONNX models moved to onnx/ subdir.
    pub fn minilm_v2() -> Self {
        Self {
            id: "all-minilm-l6-v2".into(),
            repo: "sentence-transformers/all-MiniLM-L6-v2".into(),
            // Pinned revision for reproducibility (updated 2026-01-13 for onnx/ restructuring)
            revision: "c9745ed1d9f207416be6d2e6f8de32d1f16199bf".into(),
            files: vec![
                ModelFile {
                    // cass #308: the pure-Rust native embedder loads safetensors, not
                    // ONNX. Fetch `model.safetensors` (verified at the pinned revision)
                    // so the registry's REQUIRED file check + the embedder's loader find
                    // the weights they now expect.
                    name: "model.safetensors".into(),
                    sha256: "53aa51172d142c89d9012cce15ae4d6cc0ca6895895114379cacb4fab128d9db"
                        .into(),
                    size: 90868376,
                },
                ModelFile {
                    name: "tokenizer.json".into(),
                    sha256: "be50c3628f2bf5bb5e3a7f17b1f74611b2561a3a27eeab05e5aa30f411572037"
                        .into(),
                    size: 466247,
                },
                ModelFile {
                    name: "config.json".into(),
                    sha256: "953f9c0d463486b10a6871cc2fd59f223b2c70184f49815e7efbcab5d8908b41"
                        .into(),
                    size: 612,
                },
                // FastEmbed requires special_tokens_map.json and tokenizer_config.json
                // to construct a tokenizer with correct padding/truncation behavior.
                // We install and verify them alongside the core model files.
                ModelFile {
                    name: "special_tokens_map.json".into(),
                    sha256: "303df45a03609e4ead04bc3dc1536d0ab19b5358db685b6f3da123d05ec200e3"
                        .into(),
                    size: 112,
                },
                ModelFile {
                    name: "tokenizer_config.json".into(),
                    sha256: "acb92769e8195aabd29b7b2137a9e6d6e25c476a4f15aa4355c233426c61576b"
                        .into(),
                    size: 350,
                },
            ],
            license: "Apache-2.0".into(),
        }
    }

    /// Opt-in multilingual MiniLM L12 manifest from the pinned Frankensearch
    /// release. Mapping the canonical manifest prevents checksum, revision, or
    /// file-list drift between CASS acquisition and the native loader.
    pub fn multilingual_minilm_l12_v2() -> Self {
        let canonical =
            frankensearch::embed::model_manifest::ModelManifest::multilingual_minilm_l12_v2();
        Self {
            id: canonical.id,
            repo: canonical.repo,
            revision: canonical.revision,
            files: canonical
                .files
                .into_iter()
                .map(|file| ModelFile {
                    name: file.name,
                    sha256: file.sha256,
                    size: file.size,
                })
                .collect(),
            license: canonical.license,
        }
    }

    // ==================== Bake-off Eligible Models ====================
    // These models were released after 2025-11-01 and are candidates for
    // the CPU-optimized embedding bake-off.
    //
    // Canonical definitions also available via `frankensearch_embed::ModelManifest`.

    /// Snowflake Arctic Embed S manifest.
    ///
    /// Released: 2025-11-10
    /// Dimension: 384
    /// Small, fast model with MiniLM-compatible dimension.
    ///
    /// Verified: 2026-02-02 - All checksums verified from HuggingFace.
    pub fn snowflake_arctic_s() -> Self {
        Self {
            id: "snowflake-arctic-embed-s".into(),
            repo: "Snowflake/snowflake-arctic-embed-s".into(),
            revision: "e596f507467533e48a2e17c007f0e1dacc837b33".into(),
            files: vec![
                ModelFile {
                    name: "onnx/model.onnx".into(),
                    sha256: "579c1f1778a0993eb0d2a1403340ffb491c769247fb46acc4f5cf8ac5b89c1e1"
                        .into(),
                    size: 133_093_492,
                },
                ModelFile {
                    name: "tokenizer.json".into(),
                    sha256: "91f1def9b9391fdabe028cd3f3fcc4efd34e5d1f08c3bf2de513ebb5911a1854"
                        .into(),
                    size: 711_649,
                },
                ModelFile {
                    name: "config.json".into(),
                    sha256: "4e519aa92ec40943356032afe458c8829d70c5766b109e4a57490b82f72dcfb7"
                        .into(),
                    size: 703,
                },
                ModelFile {
                    name: "special_tokens_map.json".into(),
                    sha256: "5d5b662e421ea9fac075174bb0688ee0d9431699900b90662acd44b2a350503a"
                        .into(),
                    size: 695,
                },
                ModelFile {
                    name: "tokenizer_config.json".into(),
                    sha256: "9ca59277519f6e3692c8685e26b94d4afca2d5438deff66483db495e48735810"
                        .into(),
                    size: 1_433,
                },
            ],
            license: "Apache-2.0".into(),
        }
    }

    /// Nomic Embed Text v1.5 manifest.
    ///
    /// Released: 2025-11-05
    /// Dimension: 768
    /// Long context support with Matryoshka embedding capability.
    ///
    /// Verified: 2026-02-02 - All checksums verified from HuggingFace.
    pub fn nomic_embed() -> Self {
        Self {
            id: "nomic-embed-text-v1.5".into(),
            repo: "nomic-ai/nomic-embed-text-v1.5".into(),
            revision: "e5cf08aadaa33385f5990def41f7a23405aec398".into(),
            files: vec![
                ModelFile {
                    name: "onnx/model.onnx".into(),
                    sha256: "147d5aa88c2101237358e17796cf3a227cead1ec304ec34b465bb08e9d952965"
                        .into(),
                    size: 547_310_275,
                },
                ModelFile {
                    name: "tokenizer.json".into(),
                    sha256: "d241a60d5e8f04cc1b2b3e9ef7a4921b27bf526d9f6050ab90f9267a1f9e5c66"
                        .into(),
                    size: 711_396,
                },
                ModelFile {
                    name: "config.json".into(),
                    sha256: "0168e0883705b0bf8f2b381e10f45a9f3e1ef4b13869b43c160e4c8a70ddf442"
                        .into(),
                    size: 2_331,
                },
                ModelFile {
                    name: "special_tokens_map.json".into(),
                    sha256: "5d5b662e421ea9fac075174bb0688ee0d9431699900b90662acd44b2a350503a"
                        .into(),
                    size: 695,
                },
                ModelFile {
                    name: "tokenizer_config.json".into(),
                    sha256: "d7e0000bcc80134debd2222220427e6bf5fa20a669f40a0d0d1409cc18e0a9bc"
                        .into(),
                    size: 1_191,
                },
            ],
            license: "Apache-2.0".into(),
        }
    }

    // ==================== Reranker Models ====================

    /// MS MARCO MiniLM reranker manifest (baseline for bake-off).
    ///
    /// Verified: 2026-02-02 - All checksums verified from HuggingFace.
    /// Note: The HuggingFace repo is ms-marco-MiniLM-L6-v2 (no hyphen between
    /// L and 6), but the on-disk directory name carries an extra hyphen
    /// (ms-marco-MiniLM-L-6-v2) to match `reranker_registry::ReRanker::model_dir`.
    /// Install paths use `id` as the directory; keeping them aligned is what
    /// lets `cass models install --model ms-marco` deliver the files where
    /// `FastEmbedReranker` actually looks for them.
    pub fn msmarco_reranker() -> Self {
        Self {
            id: "ms-marco-MiniLM-L-6-v2".into(),
            repo: "cross-encoder/ms-marco-MiniLM-L6-v2".into(),
            revision: "c5ee24cb16019beea0893ab7796b1df96625c6b8".into(),
            files: vec![
                ModelFile {
                    // cass #308: the pure-Rust NativeReranker loads safetensors, not ONNX.
                    // Standard HF ms-marco model.safetensors (verified at the pinned rev):
                    // F32 weights, bert.-prefixed keys, classifier present, 6/384/12 — exactly
                    // what NativeReranker reimplements.
                    name: "model.safetensors".into(),
                    sha256: "821d1aa69520101d6e0737f78a042ae25b19e5cb9160701909d10434f4aeb0ae"
                        .into(),
                    size: 90_870_598,
                },
                ModelFile {
                    name: "tokenizer.json".into(),
                    sha256: "d241a60d5e8f04cc1b2b3e9ef7a4921b27bf526d9f6050ab90f9267a1f9e5c66"
                        .into(),
                    size: 711_396,
                },
                ModelFile {
                    name: "config.json".into(),
                    sha256: "380e02c93f431831be65d99a4e7e5f67c133985bf2e77d9d4eba46847190bacc"
                        .into(),
                    size: 794,
                },
                ModelFile {
                    name: "special_tokens_map.json".into(),
                    sha256: "3c3507f36dff57bce437223db3b3081d1e2b52ec3e56ee55438193ecb2c94dd6"
                        .into(),
                    size: 132,
                },
                ModelFile {
                    name: "tokenizer_config.json".into(),
                    sha256: "a5c2e5a7b1a29a0702cd28c08a399b5ecc110c263009d17f7e3b415f25905fd8"
                        .into(),
                    size: 1_330,
                },
            ],
            license: "Apache-2.0".into(),
        }
    }

    /// Jina Reranker v1 Turbo EN manifest.
    ///
    /// Released: 2025-11-20
    /// Fast, optimized for English.
    ///
    /// Verified: 2026-02-02 - All checksums verified from HuggingFace.
    pub fn jina_reranker_turbo() -> Self {
        Self {
            id: "jina-reranker-v1-turbo-en".into(),
            repo: "jinaai/jina-reranker-v1-turbo-en".into(),
            revision: "b8c14f4e723d9e0aab4732a7b7b93741eeeb77c2".into(),
            files: vec![
                ModelFile {
                    name: "onnx/model.onnx".into(),
                    sha256: "c1296c66c119de645fa9cdee536d8637740efe85224cfa270281e50f213aa565"
                        .into(),
                    size: 151_296_975,
                },
                ModelFile {
                    name: "tokenizer.json".into(),
                    sha256: "0046da43cc8c424b317f56b092b0512aaaa65c4f925d2f16af9d9eeb4d0ef902"
                        .into(),
                    size: 2_030_772,
                },
                ModelFile {
                    name: "config.json".into(),
                    sha256: "e050ff6a15ae9295e84882fa0e98051bd8754856cd5201395ebf00ce9f2d609b"
                        .into(),
                    size: 1_206,
                },
                ModelFile {
                    name: "special_tokens_map.json".into(),
                    sha256: "06e405a36dfe4b9604f484f6a1e619af1a7f7d09e34a8555eb0b77b66318067f"
                        .into(),
                    size: 280,
                },
                ModelFile {
                    name: "tokenizer_config.json".into(),
                    sha256: "d291c6652d96d56ffdbcf1ea19d9bae5ed79003f7648c627e725a619227ce8fa"
                        .into(),
                    size: 1_215,
                },
            ],
            license: "Apache-2.0".into(),
        }
    }

    // ==================== Lookup Functions ====================

    /// Get manifest by embedder name.
    pub fn for_embedder(name: &str) -> Option<Self> {
        match name {
            "minilm" => Some(Self::minilm_v2()),
            "multilingual-minilm" | "paraphrase-multilingual-minilm-l12-v2" => {
                Some(Self::multilingual_minilm_l12_v2())
            }
            "snowflake-arctic-s" => Some(Self::snowflake_arctic_s()),
            "nomic-embed" => Some(Self::nomic_embed()),
            _ => None,
        }
    }

    /// Get manifest by reranker name.
    pub fn for_reranker(name: &str) -> Option<Self> {
        match name {
            "ms-marco" => Some(Self::msmarco_reranker()),
            "jina-reranker-turbo" => Some(Self::jina_reranker_turbo()),
            _ => None,
        }
    }

    /// Total size of all files in bytes.
    pub fn total_size(&self) -> u64 {
        self.files.iter().map(|f| f.size).sum()
    }
}

/// Model install error types.
#[derive(Debug, Error)]
pub enum InstallError {
    /// File I/O error.
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    /// SHA256 verification failed.
    #[error("verification failed for {file}: expected {expected}, got {actual}")]
    VerificationFailed {
        file: String,
        expected: String,
        actual: String,
    },
    /// A required manifest file is missing from the local source directory.
    #[error("required file '{file}' not found in {source_dir}")]
    MissingSourceFile { file: String, source_dir: String },
    /// The local source directory resolves to the install target itself.
    #[error("source directory {0} is the model install target itself")]
    SourceIsTarget(String),
    /// Manifest has placeholder checksums and is not production-ready.
    ///
    /// This error is returned when attempting to install a bake-off candidate
    /// model that has not yet been verified. The model files need to be:
    /// 1. Obtained locally to compute SHA256 checksums
    /// 2. Revision pinned to a specific commit (not "main")
    #[error(
        "model '{model_id}' is not production-ready: {} file(s) have placeholder checksums{}",
        unverified_files.len(),
        if *revision_unpinned {
            " and revision is not pinned"
        } else {
            ""
        }
    )]
    ManifestNotVerified {
        model_id: String,
        unverified_files: Vec<String>,
        revision_unpinned: bool,
    },
}

/// Local model installer with verification and atomic publication.
///
/// Stages verified files in a `<target>.installing` sibling directory and
/// promotes them with a backup-rename pattern, so a failed install never
/// leaves a partial or clobbered model directory behind.
pub struct ModelInstaller {
    /// Target directory for model files.
    target_dir: PathBuf,
    /// Temporary staging directory.
    temp_dir: PathBuf,
}

impl ModelInstaller {
    /// Create a new model installer targeting `target_dir`.
    pub fn new(target_dir: PathBuf) -> Self {
        // Use parent + modified filename to avoid with_extension() replacing dots in dir names
        // e.g., "model.v2" should become "model.v2.installing", not "model.installing"
        let temp_dir = model_install_temp_dir(&target_dir);
        Self {
            target_dir,
            temp_dir,
        }
    }

    /// Install a model from a local directory of already-obtained files.
    ///
    /// This function:
    /// 1. Validates the manifest is production-ready (no placeholder checksums)
    /// 2. Stages each required file from `source_dir` (by `local_name()`)
    /// 3. Verifies the SHA256 of every staged file
    /// 4. Writes the `.verified` marker into staging
    /// 5. Atomically promotes staging to the target directory
    ///
    /// A checksum or I/O failure discards staging and leaves any existing
    /// installation untouched.
    ///
    /// # Errors
    ///
    /// Returns `InstallError` if a required file is missing, a checksum fails,
    /// the manifest is not production-ready, or the filesystem operation fails.
    pub fn install_from_directory(
        &self,
        source_dir: &Path,
        manifest: &ModelManifest,
    ) -> Result<(), InstallError> {
        // Validate manifest is production-ready before staging. This prevents
        // installing bake-off candidates with placeholder checksums that
        // cannot be verified.
        if !manifest.is_production_ready() {
            let unverified_files: Vec<String> = manifest
                .files
                .iter()
                .filter(|f| f.sha256 == PLACEHOLDER_CHECKSUM)
                .map(|f| f.name.clone())
                .collect();
            return Err(InstallError::ManifestNotVerified {
                model_id: manifest.id.clone(),
                unverified_files,
                revision_unpinned: !manifest.has_pinned_revision(),
            });
        }

        // The source must be a real directory of already-obtained files.
        if !source_dir.is_dir() {
            return Err(InstallError::IoError(std::io::Error::new(
                std::io::ErrorKind::NotADirectory,
                format!(
                    "model install source {} is not a directory",
                    source_dir.display()
                ),
            )));
        }

        // Refuse to stage the target onto itself: copying `model.safetensors`
        // over itself would corrupt the existing install in place.
        if source_dir
            .canonicalize()
            .is_ok_and(|canonical| canonical == self.target_dir.canonicalize().unwrap_or_default())
        {
            return Err(InstallError::SourceIsTarget(
                source_dir.display().to_string(),
            ));
        }

        // Prepare the staging directory. Keeps manifest-matching partials out
        // of the way and removes stale/unsafe entries from interrupted runs.
        self.prepare_temp_dir(manifest)?;

        for file in &manifest.files {
            let local_name = file.local_name();
            if !is_safe_model_local_name(local_name) {
                self.cleanup_temp();
                return Err(InstallError::IoError(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("invalid model manifest filename: {local_name}"),
                )));
            }
            // ubs:ignore[rust.security.path-traversal] — local_name validated by is_safe_model_local_name
            let source_path = source_dir.join(local_name);
            if !source_path.is_file() {
                self.cleanup_temp();
                return Err(InstallError::MissingSourceFile {
                    file: local_name.to_string(),
                    source_dir: source_dir.display().to_string(),
                });
            }

            // ubs:ignore[rust.security.path-traversal] — local_name validated by is_safe_model_local_name
            let staged_path = self.temp_dir.join(local_name);
            if let Err(err) = fs::copy(&source_path, &staged_path)
                .and_then(|_| File::open(&staged_path).and_then(|file| file.sync_all()))
            {
                self.cleanup_temp();
                return Err(err.into());
            }

            // Verify SHA256 on the staged copy: this guards both against bad
            // source content and against a short/corrupt copy.
            let actual_hash = match compute_sha256(&staged_path) {
                Ok(hash) => hash,
                Err(err) => {
                    self.cleanup_temp();
                    return Err(err);
                }
            };
            if actual_hash != file.sha256 {
                self.cleanup_temp();
                return Err(InstallError::VerificationFailed {
                    file: file.name.clone(),
                    expected: file.sha256.clone(),
                    actual: actual_hash,
                });
            }
        }

        // Record the hardened `.verified` marker *inside staging* so marker
        // and files promote atomically with the same operation.
        self.write_verified_marker(source_dir, manifest)?;

        // Atomic install: rename temp -> target
        self.atomic_install()?;

        Ok(())
    }

    fn prepare_temp_dir(&self, manifest: &ModelManifest) -> Result<(), InstallError> {
        ensure_model_install_temp_dir(&self.temp_dir)?;

        // Drop a legacy `<target>.downloading` sibling left behind by the
        // pre-local-only downloader; nothing can acquire through it anymore.
        let legacy_staging = model_staging_dir(&self.target_dir, "downloading");
        if model_dir_is_real_directory(&legacy_staging).unwrap_or(false) {
            let _ = fs::remove_dir_all(&legacy_staging);
        }

        let expected_files: HashSet<String> = manifest
            .files
            .iter()
            .map(|file| file.local_name().to_string())
            .collect();

        for entry in fs::read_dir(&self.temp_dir)? {
            let entry = entry?;
            let entry_type = entry.file_type()?;
            let entry_name = entry.file_name();
            let keep_entry = entry_type.is_file()
                && entry_name
                    .to_str()
                    .is_some_and(|name| expected_files.contains(name));

            if keep_entry {
                continue;
            }

            let entry_path = entry.path();
            if entry_type.is_dir() {
                fs::remove_dir_all(entry_path)?;
            } else {
                fs::remove_file(entry_path)?;
            }
        }

        Ok(())
    }

    /// Atomically install staged files.
    ///
    /// Uses a backup-rename-cleanup pattern to minimize the window where no model exists:
    /// 1. Move existing target to backup (if present)
    /// 2. Rename temp to target
    /// 3. Remove backup on success, or restore on failure
    fn atomic_install(&self) -> Result<(), InstallError> {
        let backup_dir = unique_model_backup_dir(&self.target_dir);
        sync_tree(&self.temp_dir)?;

        // Move existing target to backup (preserves it until new install succeeds)
        let had_existing = if ensure_replaceable_model_dir(&self.target_dir)? {
            fs::rename(&self.target_dir, &backup_dir)?;
            true
        } else {
            false
        };

        // Rename temp to target
        match fs::rename(&self.temp_dir, &self.target_dir) {
            Ok(()) => {
                sync_parent_directory(&self.target_dir)?;
                // Success: remove backup
                if had_existing {
                    let _ = fs::remove_dir_all(&backup_dir);
                    sync_parent_directory(&self.target_dir)?;
                }
            }
            Err(e) => {
                // Failed: try to restore from backup
                if had_existing && backup_dir.exists() {
                    match fs::rename(&backup_dir, &self.target_dir) {
                        Ok(()) => {
                            sync_parent_directory(&self.target_dir)?;
                            return Err(std::io::Error::other(format!(
                                "failed installing {} from {}: {e}; restored original model",
                                self.target_dir.display(),
                                self.temp_dir.display()
                            ))
                            .into());
                        }
                        Err(restore_err) => {
                            return Err(std::io::Error::other(format!(
                                "failed installing {} from {}: {e}; restore error: {restore_err}; temp model retained at {}",
                                self.target_dir.display(),
                                self.temp_dir.display(),
                                self.temp_dir.display()
                            ))
                            .into());
                        }
                    }
                }
                return Err(e.into());
            }
        }

        Ok(())
    }

    /// Write the `.verified` marker into the staging directory.
    ///
    /// The hardened marker records the install source provenance
    /// (`from-file`), the local source path, and the model fingerprint so a
    /// local install is auditable. Byte-compatible with the legacy reader:
    /// `revision=` stays first and unknown lines are ignored (bead
    /// cass-fleet-resilience-20260608-uojcg.5.5).
    fn write_verified_marker(
        &self,
        source_dir: &Path,
        manifest: &ModelManifest,
    ) -> Result<(), InstallError> {
        let marker_path = self
            .temp_dir
            .join(crate::search::model_acquisition::VERIFIED_MARKER_NAME);
        let content = crate::search::model_acquisition::VerifiedMarker::for_from_file(
            manifest,
            source_dir,
            chrono::Utc::now().to_rfc3339(),
        )
        .render();
        let mut file = File::create(&marker_path)?;
        file.write_all(content.as_bytes())?;
        file.sync_all()?;
        Ok(())
    }

    /// Clean up temporary staging directory.
    fn cleanup_temp(&self) {
        if model_dir_is_real_directory(&self.temp_dir).unwrap_or(false) {
            let _ = fs::remove_dir_all(&self.temp_dir);
        }
    }
}

/// Compute SHA256 hash of a file.
pub fn compute_sha256(path: &Path) -> Result<String, InstallError> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut hasher = Sha256::new();

    let mut buffer = [0u8; 8192];
    loop {
        let n = reader.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }

    let hash = hasher.finalize();
    Ok(hex::encode(hash))
}

/// Classify the local semantic model cache without performing network I/O.
///
/// This is the central fail-open lifecycle gate for semantic quality assets:
/// it reports why quality semantic search is unavailable without changing the
/// lexical search path.
pub fn classify_model_cache(
    model_dir: &Path,
    manifest: &ModelManifest,
    policy: &ModelAcquisitionPolicy,
) -> ModelCacheReport {
    classify_model_cache_with_integrity(model_dir, manifest, policy, ModelCacheIntegrity::Full)
}

/// Classify the local semantic model cache using metadata only.
///
/// This is for hot status/health probes. It preserves the same policy,
/// missing-file, staging, quarantine, and revision-marker decisions as
/// `classify_model_cache`, but it does not hash model payloads. Actual model
/// loading and `cass models verify` still use full SHA256 validation.
pub(crate) fn classify_model_cache_metadata(
    model_dir: &Path,
    manifest: &ModelManifest,
    policy: &ModelAcquisitionPolicy,
) -> ModelCacheReport {
    classify_model_cache_with_integrity(model_dir, manifest, policy, ModelCacheIntegrity::Metadata)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ModelCacheIntegrity {
    Full,
    Metadata,
}

fn classify_model_cache_with_integrity(
    model_dir: &Path,
    manifest: &ModelManifest,
    policy: &ModelAcquisitionPolicy,
    integrity: ModelCacheIntegrity,
) -> ModelCacheReport {
    let required_size_bytes = manifest.total_size();
    let installed_size_bytes = installed_manifest_size(model_dir, manifest);
    let missing_files = missing_manifest_files(model_dir, manifest);
    let state = classify_model_cache_state(model_dir, manifest, policy, &missing_files, integrity);

    ModelCacheReport {
        model_id: manifest.id.clone(),
        model_dir: model_dir.to_path_buf(),
        state,
        required_size_bytes,
        installed_size_bytes,
        policy_source: policy.config_source.clone(),
    }
}

fn classify_model_cache_state(
    model_dir: &Path,
    manifest: &ModelManifest,
    policy: &ModelAcquisitionPolicy,
    missing_files: &[String],
    integrity: ModelCacheIntegrity,
) -> ModelCacheState {
    if !policy.install_enabled {
        return ModelCacheState::DisabledByPolicy {
            reason: "semantic model installation disabled by policy".to_string(),
        };
    }

    let quarantine_marker = model_dir.join(".quarantined");
    if quarantine_marker.is_file() {
        let reason = fs::read_to_string(&quarantine_marker)
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "model cache quarantined after integrity failure".to_string());
        return ModelCacheState::QuarantinedCorrupt {
            marker_path: quarantine_marker,
            reason,
        };
    }

    if !missing_files.is_empty() {
        if let Some(max_bytes) = policy.max_model_bytes
            && manifest.total_size() > max_bytes
        {
            return ModelCacheState::BudgetBlocked {
                required_bytes: manifest.total_size(),
                max_bytes,
            };
        }

        return ModelCacheState::NotAcquired {
            missing_files: missing_files.to_vec(),
        };
    }

    if integrity == ModelCacheIntegrity::Full {
        for file in &manifest.files {
            let Some(path) = model_file_path(model_dir, file) else {
                continue;
            };
            match compute_sha256(&path) {
                Ok(actual) if actual == file.sha256 => {}
                Ok(actual) => {
                    return ModelCacheState::ChecksumMismatch {
                        file: file.local_name().to_string(),
                        expected: file.sha256.clone(),
                        actual,
                    };
                }
                Err(err) => {
                    return ModelCacheState::QuarantinedCorrupt {
                        marker_path: path,
                        reason: format!("unable to hash model file {}: {err}", file.local_name()),
                    };
                }
            }
        }
    }

    let verified_marker = model_dir.join(".verified");
    if !verified_marker.is_file() {
        return ModelCacheState::PreseededLocal {
            model_dir: model_dir.to_path_buf(),
        };
    }

    let marker = match fs::read_to_string(&verified_marker) {
        Ok(marker) => marker,
        Err(err) => {
            return ModelCacheState::QuarantinedCorrupt {
                marker_path: verified_marker,
                reason: format!("unable to read verified marker: {err}"),
            };
        }
    };

    let current_revision =
        marker_field(&marker, "revision").unwrap_or_else(|| "<unknown>".to_string());
    if current_revision != manifest.revision {
        return ModelCacheState::IncompatibleVersion {
            current_revision,
            expected_revision: manifest.revision.clone(),
        };
    }

    match marker_field(&marker, "source") {
        Some(source) if source == "preseeded_local" => ModelCacheState::PreseededLocal {
            model_dir: model_dir.to_path_buf(),
        },
        _ => ModelCacheState::Acquired {
            model_dir: model_dir.to_path_buf(),
        },
    }
}

/// Check if a model is installed and verified against the given manifest.
///
/// `coding_agent_session_search-odbnh`: pre-fix this function hardcoded
/// `ModelManifest::minilm_v2()` to enumerate required files, so on a
/// machine with a complete snowflake-arctic-s or nomic-embed install
/// it always returned `NotInstalled` (minilm's filenames aren't a
/// subset of those models' filenames). The caller passes the manifest
/// they already resolved via `ModelManifest::for_embedder(name)` so
/// the file-presence check aligns with the model that was installed.
pub fn check_model_installed(model_dir: &Path, manifest: &ModelManifest) -> ModelState {
    if !model_dir.is_dir() {
        return ModelState::NotInstalled;
    }

    let verified_marker = model_dir.join(".verified");
    if !verified_marker.is_file() {
        return ModelState::NotInstalled;
    }

    // Check if all required files exist. Accept either the canonical repo path
    // (for preseeded HuggingFace layouts) or the flat local name used by the
    // local installer.
    for file in &manifest.files {
        if model_file_path(model_dir, file).is_none() {
            return ModelState::NotInstalled;
        }
    }

    ModelState::Ready
}

fn ensure_replaceable_model_dir(path: &Path) -> Result<bool, InstallError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            ensure_real_model_directory_metadata(
                path,
                &metadata,
                "refusing to install model through symlink",
                "refusing to replace model target because it is not a directory",
            )?;
            Ok(true)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(std::io::Error::new(
            err.kind(),
            format!(
                "failed inspecting model target before install {}: {err}",
                path.display()
            ),
        )
        .into()),
    }
}

fn ensure_model_install_temp_dir(path: &Path) -> Result<(), InstallError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            ensure_real_model_directory_metadata(
                path,
                &metadata,
                "refusing to prepare model install temp dir through symlink",
                "refusing to prepare model install temp dir because it is not a directory",
            )?;
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(path)?;
            let metadata = fs::symlink_metadata(path).map_err(|err| {
                std::io::Error::new(
                    err.kind(),
                    format!(
                        "failed inspecting model install temp dir after create {}: {err}",
                        path.display()
                    ),
                )
            })?;
            ensure_real_model_directory_metadata(
                path,
                &metadata,
                "refusing to prepare model install temp dir through symlink",
                "refusing to prepare model install temp dir because it is not a directory",
            )?;
        }
        Err(err) => {
            return Err(std::io::Error::new(
                err.kind(),
                format!(
                    "failed inspecting model install temp dir before prepare {}: {err}",
                    path.display()
                ),
            )
            .into());
        }
    }
    Ok(())
}

fn model_dir_is_real_directory(path: &Path) -> Result<bool, InstallError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            let file_type = metadata.file_type();
            Ok(file_type.is_dir() && !file_type.is_symlink())
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err.into()),
    }
}

fn ensure_real_model_directory_metadata(
    path: &Path,
    metadata: &fs::Metadata,
    symlink_message: &str,
    non_dir_message: &str,
) -> Result<(), InstallError> {
    let file_type = metadata.file_type();
    if file_type.is_symlink() {
        return Err(std::io::Error::other(format!("{symlink_message}: {}", path.display())).into());
    }
    if !file_type.is_dir() {
        return Err(std::io::Error::other(format!("{non_dir_message}: {}", path.display())).into());
    }
    Ok(())
}

/// Sibling staging directory `<target>.<suffix>` used for in-flight installs.
fn model_staging_dir(target_dir: &Path, suffix: &str) -> PathBuf {
    if let Some(parent) = target_dir.parent() {
        let dir_name = target_dir
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("model");
        parent.join(format!("{dir_name}.{suffix}"))
    } else {
        target_dir.with_extension(suffix)
    }
}

fn model_install_temp_dir(target_dir: &Path) -> PathBuf {
    model_staging_dir(target_dir, "installing")
}

fn is_safe_model_relative_path(path: &str) -> bool {
    !path.is_empty()
        && !Path::new(path).is_absolute()
        && Path::new(path)
            .components()
            .all(|component| matches!(component, std::path::Component::Normal(_)))
}

fn is_safe_model_local_name(name: &str) -> bool {
    let mut components = Path::new(name).components();
    !name.is_empty()
        && matches!(
            (components.next(), components.next()),
            (Some(std::path::Component::Normal(_)), None)
        )
}

/// Resolve a manifest file on disk.
///
/// The installer stores manifest paths by local basename, while preseeded
/// directories can preserve the canonical repo layout.
pub fn model_file_path(model_dir: &Path, file: &ModelFile) -> Option<PathBuf> {
    if is_safe_model_relative_path(&file.name) {
        // ubs:ignore[rust.security.path-traversal] — file.name validated by is_safe_model_relative_path
        let canonical = model_dir.join(&file.name);
        if canonical.is_file() {
            return Some(canonical);
        }
    }

    let local_name = file.local_name();
    if is_safe_model_local_name(local_name) {
        // ubs:ignore[rust.security.path-traversal] — local_name validated by is_safe_model_local_name
        let local = model_dir.join(local_name);
        if local.is_file() {
            return Some(local);
        }
    }

    None
}

fn missing_manifest_files(model_dir: &Path, manifest: &ModelManifest) -> Vec<String> {
    manifest
        .files
        .iter()
        .filter(|file| model_file_path(model_dir, file).is_none())
        .map(|file| file.local_name().to_string())
        .collect()
}

fn installed_manifest_size(model_dir: &Path, manifest: &ModelManifest) -> u64 {
    manifest
        .files
        .iter()
        .filter_map(|file| model_file_path(model_dir, file))
        .filter_map(|path| path.metadata().ok())
        .map(|metadata| metadata.len())
        .sum()
}

fn marker_field(content: &str, field: &str) -> Option<String> {
    let prefix = format!("{field}=");
    content
        .lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn unique_model_backup_dir(path: &Path) -> PathBuf {
    unique_model_sidecar_path(path, "bak", "model")
}

fn unique_model_sidecar_path(path: &Path, suffix: &str, fallback_name: &str) -> PathBuf {
    static NEXT_NONCE: AtomicU64 = AtomicU64::new(0);

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let nonce = NEXT_NONCE.fetch_add(1, Ordering::Relaxed);
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(fallback_name);

    path.with_file_name(format!(
        ".{file_name}.{suffix}.{}.{}.{}",
        std::process::id(),
        timestamp,
        nonce
    ))
}

#[cfg(not(windows))]
fn sync_tree(path: &Path) -> Result<(), InstallError> {
    sync_tree_inner(path)?;
    sync_parent_directory(path)
}

#[cfg(not(windows))]
fn sync_tree_inner(path: &Path) -> Result<(), InstallError> {
    let metadata = fs::metadata(path)?;
    if metadata.is_dir() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            sync_tree_inner(&entry.path())?;
        }
        File::open(path)?.sync_all()?;
    } else if metadata.is_file() {
        File::open(path)?.sync_all()?;
    }
    Ok(())
}

#[cfg(windows)]
fn sync_tree(_path: &Path) -> Result<(), InstallError> {
    Ok(())
}

#[cfg(not(windows))]
fn sync_parent_directory(path: &Path) -> Result<(), InstallError> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    File::open(parent)?.sync_all()?;
    Ok(())
}

#[cfg(windows)]
fn sync_parent_directory(_path: &Path) -> Result<(), InstallError> {
    Ok(())
}

#[cfg(test)]
mod tests;
