use super::atomic_io::{
    ManifestError, create_unique_manifest_temp_file, now_ms, replace_file_from_temp,
    sync_parent_directory,
};
use super::{
    CHUNKING_STRATEGY_VERSION, Component, Deserialize, InvalidationAction, IoWrite, Path, PathBuf,
    PolicyManifest, SEMANTIC_SCHEMA_VERSION, SemanticPolicy, Serialize, fs,
};

// ─── Constants ─────────────────────────────────────────────────────────────

/// Current manifest format version.  Bump when the JSON schema changes in a
/// backwards-incompatible way.
/// Manifest format version.
///
/// History:
/// - **v1** (pre-cass#257): `BuildCheckpoint` resume cursor is the
///   conversation offset only.
/// - **v2** (cass#257 sub-fix 2): `BuildCheckpoint` may additionally
///   carry `last_message_id`, an inclusive canonical message PK
///   advanced by every batch. Resume strictly skips messages ≤ this
///   cursor so an interrupted bounded run never re-embeds work it
///   already staged. Later v2 checkpoints may also carry
///   `cursor_exhausted`, which separates durable completion from
///   count-based progress. Both fields use `#[serde(default)]`; the
///   JSON-on-disk shape only differs when a batch persists the cursor
///   metadata, and older v2 readers ignore the extra field.
///
/// **Compatibility:**
/// - **Old binary reading v2 manifest:** clean `UnsupportedVersion`
///   error from `load()`; operator sees a clear "manifest version
///   $V is newer than max-supported $MAX" message and can upgrade.
/// - **New binary reading v1 manifest:** loads fine. `last_message_id`
///   defaults to `None`; resume falls back to the conversation offset
///   with a one-shot warning that resume granularity is coarser than
///   ideal until the next checkpoint save bumps the on-disk shape.
pub const MANIFEST_FORMAT_VERSION: u32 = 2;

/// Highest manifest-version emitted by pre-cass#257 binaries; loading
/// this is fully supported, but resume granularity will be coarser
/// until a fresh checkpoint is saved. Kept as a named constant so the
/// fallback warning quotes a stable number.
pub const MANIFEST_FORMAT_VERSION_PRE_LAST_MESSAGE_CURSOR: u32 = 1;

/// Filename for the durable manifest.
pub const MANIFEST_FILENAME: &str = "semantic_manifest.json";

/// Filename for the prototype per-shard semantic artifact manifest.
pub const SHARD_MANIFEST_FILENAME: &str = "semantic_shards.json";

/// Display marker for vector spaces published by the removed external HTTP
/// embedding provider. Their artifacts are left untouched on disk and are
/// reported, never loaded or served.
pub const LEGACY_EXTERNAL_EMBEDDING_SPACE: &str = "legacy_external_embedding_space";

/// Whether `embedder_id` is a canonical identity produced by the removed
/// external HTTP embedder (`external-v1-<dimension>-<64 lowercase hex>`).
/// Only recognize canonical identities, never arbitrary path-like strings.
pub fn is_external_identity(id: &str) -> bool {
    const MAX_DIMENSION: usize = 16_384;
    let Some((dimension, digest)) = id
        .strip_prefix("external-v1-")
        .and_then(|rest| rest.split_once('-'))
    else {
        return false;
    };
    dimension
        .parse::<usize>()
        .is_ok_and(|d| (1..=MAX_DIMENSION).contains(&d) && d.to_string() == dimension)
        && digest.len() == 64
        && digest
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

pub fn semantic_shard_artifact_path_is_safe(recorded_path: &str) -> bool {
    let trimmed = recorded_path.trim();
    if trimmed.is_empty() || trimmed != recorded_path {
        return false;
    }
    let path = Path::new(recorded_path);
    !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

// ─── Tier kind ─────────────────────────────────────────────────────────────

/// Which semantic tier an artifact or checkpoint belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TierKind {
    Fast,
    Quality,
}

impl TierKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Fast => "fast",
            Self::Quality => "quality",
        }
    }
}

// ─── Tier readiness ────────────────────────────────────────────────────────

/// Readiness of a single semantic tier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TierReadiness {
    /// Artifact exists, verified, and current with the DB.
    Ready,
    /// Build is in progress (checkpoint present).
    Building { progress_pct: u8 },
    /// Artifact exists but DB or model changed since it was built.
    Stale { reason: String },
    /// No artifact at all for this tier.
    Missing,
    /// Schema or chunking version mismatch — must discard and rebuild.
    Incompatible { reason: String },
}

impl TierReadiness {
    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Ready)
    }

    pub fn is_usable(&self) -> bool {
        matches!(self, Self::Ready | Self::Stale { .. })
    }
}

// ─── Artifact record ───────────────────────────────────────────────────────

/// Durable metadata for a single vector index artifact.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactRecord {
    /// Which tier this artifact belongs to.
    pub tier: TierKind,
    /// Embedder ID that produced these vectors (e.g., "minilm-384", "fnv1a-384").
    pub embedder_id: String,
    /// Model revision hash (HuggingFace commit or "hash" for the hash embedder).
    pub model_revision: String,
    /// Semantic schema version at build time.
    pub schema_version: u32,
    /// Chunking strategy version at build time.
    pub chunking_version: u32,
    /// Output dimension of the embedder.
    pub dimension: usize,
    /// Number of documents (message chunks) embedded.
    pub doc_count: u64,
    /// Number of conversations processed to produce this artifact.
    pub conversation_count: u64,
    /// Storage fingerprint of the canonical DB when this artifact was built.
    pub db_fingerprint: String,
    /// Relative path to the index file (from data_dir).
    pub index_path: String,
    /// File size in bytes.
    pub size_bytes: u64,
    /// Unix timestamp (ms) when the build started.
    pub started_at_ms: i64,
    /// Unix timestamp (ms) when the build completed.
    pub completed_at_ms: i64,
    /// Whether this artifact has been verified and published.
    pub ready: bool,
}

impl ArtifactRecord {
    /// Convert to the policy-level manifest for invalidation checks.
    pub fn to_policy_manifest(&self) -> PolicyManifest {
        PolicyManifest {
            embedder_id: self.embedder_id.clone(),
            model_revision: self.model_revision.clone(),
            schema_version: self.schema_version,
            chunking_version: self.chunking_version,
            doc_count: self.doc_count,
            built_at_ms: self.completed_at_ms,
        }
    }

    /// Evaluate this artifact's readiness against the current policy and DB
    /// fingerprint.
    ///
    /// **Note**: This checks schema/chunking versions, mode, model revision,
    /// and DB fingerprint.  It does NOT detect embedder changes because the
    /// expected embedder ID requires the embedder registry to resolve.
    /// Callers needing embedder-change detection should call
    /// [`SemanticAssetManifest::invalidation_action`](crate::search::policy::SemanticAssetManifest::invalidation_action) directly with the
    /// correct `expected_embedder_id` from the registry.
    pub fn readiness(
        &self,
        policy: &SemanticPolicy,
        current_db_fingerprint: &str,
        current_model_revision: &str,
    ) -> TierReadiness {
        let action = self.to_policy_manifest().invalidation_action(
            policy,
            current_model_revision,
            &self.embedder_id,
        );

        match action {
            InvalidationAction::UpToDate => {
                if self.db_fingerprint != current_db_fingerprint {
                    TierReadiness::Stale {
                        reason: "DB content changed since artifact was built".to_owned(),
                    }
                } else if !self.ready {
                    TierReadiness::Building { progress_pct: 100 }
                } else {
                    TierReadiness::Ready
                }
            }
            InvalidationAction::RebuildInBackground => TierReadiness::Stale {
                reason: "model revision changed; vectors usable until rebuild completes".to_owned(),
            },
            InvalidationAction::DiscardAndRebuild { reason } => {
                TierReadiness::Incompatible { reason }
            }
            InvalidationAction::Evict => TierReadiness::Incompatible {
                reason: "semantic mode set to lexical-only".to_owned(),
            },
        }
    }
}

// ─── HNSW accelerator record ──────────────────────────────────────────────

/// Durable metadata for an HNSW accelerator index.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HnswRecord {
    /// Which base artifact this accelerates.
    pub base_tier: TierKind,
    /// Embedder ID of the base artifact.
    pub embedder_id: String,
    /// ef_search parameter used at build time.
    pub ef_search: usize,
    /// Relative path to the HNSW index file (from data_dir).
    pub index_path: String,
    /// File size in bytes.
    pub size_bytes: u64,
    /// Unix timestamp (ms) when built.
    pub built_at_ms: i64,
    /// Whether this index is ready for use.
    pub ready: bool,
}

// ─── Sharded vector artifact sidecar ─────────────────────────────────────

/// Durable metadata for one mmap-friendly semantic vector shard.
///
/// Shards deliberately live in a sidecar manifest instead of
/// [`SemanticManifest`]. Runtime readiness continues to flow through the
/// existing tier artifact records, so partial shard generations cannot make a
/// semantic tier look ready before a promotion step has merged or selected a
/// complete generation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SemanticShardRecord {
    /// Which semantic tier this shard belongs to.
    pub tier: TierKind,
    /// Embedder ID that produced the vectors.
    pub embedder_id: String,
    /// Model revision hash or "hash" for the hash embedder.
    pub model_revision: String,
    /// Semantic schema version at build time.
    pub schema_version: u32,
    /// Chunking strategy version at build time.
    pub chunking_version: u32,
    /// Output dimension of the embedder.
    pub dimension: usize,
    /// Zero-based shard number within the generation.
    pub shard_index: u32,
    /// Total shards expected for this generation.
    pub shard_count: u32,
    /// Number of documents embedded in this shard.
    pub doc_count: u64,
    /// Total conversations represented by the full shard generation.
    pub total_conversations: u64,
    /// Storage fingerprint of the canonical DB when this shard was built.
    pub db_fingerprint: String,
    /// Relative path to the shard index file from data_dir.
    pub index_path: String,
    /// Vector quantization used by the shard file.
    pub quantization: String,
    /// Whether readers may mmap this artifact directly.
    pub mmap_ready: bool,
    /// Relative path to the shard-local ANN accelerator, when built.
    pub ann_index_path: Option<String>,
    /// File size of the ANN accelerator in bytes.
    pub ann_size_bytes: u64,
    /// Whether the shard-local ANN accelerator is ready for use.
    pub ann_ready: bool,
    /// File size in bytes.
    pub size_bytes: u64,
    /// Unix timestamp (ms) when the shard build started.
    pub started_at_ms: i64,
    /// Unix timestamp (ms) when the shard build completed.
    pub completed_at_ms: i64,
    /// Whether this shard has been verified and published to the sidecar.
    pub ready: bool,
}

impl SemanticShardRecord {
    pub fn to_policy_manifest(&self) -> PolicyManifest {
        PolicyManifest {
            embedder_id: self.embedder_id.clone(),
            model_revision: self.model_revision.clone(),
            schema_version: self.schema_version,
            chunking_version: self.chunking_version,
            doc_count: self.doc_count,
            built_at_ms: self.completed_at_ms,
        }
    }

    pub fn matches_generation(
        &self,
        tier: TierKind,
        embedder_id: &str,
        db_fingerprint: &str,
    ) -> bool {
        self.tier == tier
            && self.embedder_id == embedder_id
            && self.db_fingerprint == db_fingerprint
    }
}

/// Aggregated readiness for a shard generation.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticShardSummary {
    pub shard_count: u32,
    pub ready_shards: u32,
    pub ann_ready_shards: u32,
    pub doc_count: u64,
    pub total_conversations: u64,
    pub size_bytes: u64,
    pub ann_size_bytes: u64,
    pub complete: bool,
}

/// Sidecar manifest for prototype semantic shard generations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SemanticShardManifest {
    pub manifest_version: u32,
    pub shards: Vec<SemanticShardRecord>,
    pub updated_at_ms: i64,
}

impl Default for SemanticShardManifest {
    fn default() -> Self {
        Self {
            manifest_version: MANIFEST_FORMAT_VERSION,
            shards: Vec::new(),
            updated_at_ms: 0,
        }
    }
}

impl SemanticShardManifest {
    pub fn path(data_dir: &Path) -> PathBuf {
        data_dir.join("vector_index").join(SHARD_MANIFEST_FILENAME)
    }

    pub fn load(data_dir: &Path) -> Result<Option<Self>, ManifestError> {
        let path = Self::path(data_dir);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(ManifestError::Io {
                    path,
                    source: e.to_string(),
                });
            }
        };

        let manifest: Self = serde_json::from_slice(&bytes).map_err(|e| ManifestError::Parse {
            path: path.clone(),
            source: e.to_string(),
        })?;

        if manifest.manifest_version > MANIFEST_FORMAT_VERSION {
            return Err(ManifestError::UnsupportedVersion {
                found: manifest.manifest_version,
                max_supported: MANIFEST_FORMAT_VERSION,
            });
        }

        Ok(Some(manifest))
    }

    pub fn load_or_default(data_dir: &Path) -> Result<Self, ManifestError> {
        match Self::load(data_dir) {
            Ok(Some(manifest)) => Ok(manifest),
            Ok(None) => Ok(Self::default()),
            Err(e @ ManifestError::Io { .. }) => Err(e),
            Err(ManifestError::Parse { .. } | ManifestError::UnsupportedVersion { .. }) => {
                Ok(Self::default())
            }
            Err(e) => Err(e),
        }
    }

    pub fn save(&mut self, data_dir: &Path) -> Result<(), ManifestError> {
        let path = Self::path(data_dir);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| ManifestError::Io {
                path: parent.to_path_buf(),
                source: e.to_string(),
            })?;
        }

        self.updated_at_ms = now_ms();
        let json = serde_json::to_string_pretty(self).map_err(|e| ManifestError::Serialize {
            source: e.to_string(),
        })?;
        let (tmp_path, mut file) =
            create_unique_manifest_temp_file(&path).map_err(|e| ManifestError::Io {
                path: path.clone(),
                source: e.to_string(),
            })?;
        file.write_all(json.as_bytes())
            .map_err(|e| ManifestError::Io {
                path: tmp_path.clone(),
                source: e.to_string(),
            })?;
        file.sync_all().map_err(|e| ManifestError::Io {
            path: tmp_path.clone(),
            source: e.to_string(),
        })?;
        replace_file_from_temp(&tmp_path, &path).map_err(|e| ManifestError::Io {
            path: path.clone(),
            source: e.to_string(),
        })?;
        sync_parent_directory(&path).map_err(|e| ManifestError::Io {
            path: path
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| path.clone()),
            source: e.to_string(),
        })?;

        Ok(())
    }

    pub fn replace_shards_for_generation(
        &mut self,
        tier: TierKind,
        embedder_id: &str,
        db_fingerprint: &str,
        mut shards: Vec<SemanticShardRecord>,
    ) {
        self.shards
            .retain(|shard| !shard.matches_generation(tier, embedder_id, db_fingerprint));
        self.shards.append(&mut shards);
        self.shards.sort_by(|a, b| {
            (
                a.tier.as_str(),
                &a.embedder_id,
                &a.db_fingerprint,
                a.shard_index,
            )
                .cmp(&(
                    b.tier.as_str(),
                    &b.embedder_id,
                    &b.db_fingerprint,
                    b.shard_index,
                ))
        });
    }

    /// Stop every shard generation for one embedder from shadowing a freshly
    /// rebuilt monolithic artifact while retaining its provenance record and
    /// files for explicit inspection/GC.
    pub fn mark_shards_stale_for_embedder(&mut self, embedder_id: &str) -> usize {
        let mut changed = 0usize;
        for shard in self
            .shards
            .iter_mut()
            .filter(|shard| shard.embedder_id == embedder_id)
        {
            if shard.ready || shard.ann_ready {
                shard.ready = false;
                shard.ann_ready = false;
                changed = changed.saturating_add(1);
            }
        }
        changed
    }

    pub fn summary(
        &self,
        tier: TierKind,
        embedder_id: &str,
        db_fingerprint: &str,
    ) -> SemanticShardSummary {
        let mut summary = SemanticShardSummary::default();
        let mut ready_indices = std::collections::BTreeSet::new();
        let mut ann_ready_indices = std::collections::BTreeSet::new();
        let mut seen_indices = std::collections::BTreeSet::new();
        let mut seen_index_paths = std::collections::BTreeSet::new();
        let mut seen_ann_index_paths = std::collections::BTreeSet::new();
        let mut expected_shard_count = None;
        let mut expected_generation_metadata: Option<(&str, u32, u32, usize, u64, &str)> = None;
        let mut generation_consistent = true;
        for shard in self
            .shards
            .iter()
            .filter(|shard| shard.matches_generation(tier, embedder_id, db_fingerprint))
        {
            if shard.shard_count == 0 || shard.shard_index >= shard.shard_count {
                generation_consistent = false;
            }
            if !seen_indices.insert(shard.shard_index) {
                generation_consistent = false;
            }
            if !semantic_shard_artifact_path_is_safe(&shard.index_path)
                || !seen_index_paths.insert(&shard.index_path)
            {
                generation_consistent = false;
            }
            match expected_shard_count {
                Some(expected) if expected != shard.shard_count => {
                    generation_consistent = false;
                }
                None => expected_shard_count = Some(shard.shard_count),
                _ => {}
            }
            let generation_metadata = (
                shard.model_revision.as_str(),
                shard.schema_version,
                shard.chunking_version,
                shard.dimension,
                shard.total_conversations,
                shard.quantization.as_str(),
            );
            match expected_generation_metadata {
                Some(expected) if expected != generation_metadata => {
                    generation_consistent = false;
                }
                None => expected_generation_metadata = Some(generation_metadata),
                _ => {}
            }
            if shard.schema_version != SEMANTIC_SCHEMA_VERSION
                || shard.chunking_version != CHUNKING_STRATEGY_VERSION
                || shard.dimension == 0
            {
                generation_consistent = false;
            }
            summary.shard_count = summary.shard_count.max(shard.shard_count);
            summary.doc_count = summary.doc_count.saturating_add(shard.doc_count);
            summary.total_conversations =
                summary.total_conversations.max(shard.total_conversations);
            summary.size_bytes = summary.size_bytes.saturating_add(shard.size_bytes);
            summary.ann_size_bytes = summary.ann_size_bytes.saturating_add(shard.ann_size_bytes);
            if shard.ready && shard.mmap_ready {
                ready_indices.insert(shard.shard_index);
            }
            if shard.ready
                && shard.mmap_ready
                && shard.ann_ready
                && shard.ann_size_bytes > 0
                && let Some(ann_index_path) = shard.ann_index_path.as_deref()
                && semantic_shard_artifact_path_is_safe(ann_index_path)
                && seen_ann_index_paths.insert(ann_index_path)
            {
                ann_ready_indices.insert(shard.shard_index);
            }
        }
        summary.ready_shards = u32::try_from(ready_indices.len()).unwrap_or(u32::MAX);
        summary.ann_ready_shards = u32::try_from(ann_ready_indices.len()).unwrap_or(u32::MAX);
        summary.complete = generation_consistent
            && summary.shard_count > 0
            && summary.ready_shards == summary.shard_count
            && (0..summary.shard_count).all(|index| ready_indices.contains(&index));
        summary
    }

    pub fn invalidate_incompatible(
        &mut self,
        policy: &SemanticPolicy,
        current_model_revision: &str,
    ) -> usize {
        let before = self.shards.len();
        self.shards.retain(|shard| {
            !matches!(
                shard.to_policy_manifest().invalidation_action(
                    policy,
                    current_model_revision,
                    &shard.embedder_id,
                ),
                InvalidationAction::DiscardAndRebuild { .. } | InvalidationAction::Evict
            )
        });
        before.saturating_sub(self.shards.len())
    }

    pub fn total_size_bytes(&self) -> u64 {
        self.shards
            .iter()
            .map(|shard| shard.size_bytes)
            .fold(0, u64::saturating_add)
    }
}

// ─── Build checkpoint ──────────────────────────────────────────────────────

/// Resumable position for an interrupted semantic build.
///
/// Sub-fix 2 for cass#257 added the optional `last_message_id` cursor.
/// Resume strictly advances past this cursor when present, so that a
/// rerun of an interrupted bounded backfill never re-embeds messages
/// that already made it into the staged index. Pre-#257 binaries wrote
/// checkpoints without the field (it deserializes to `None` via
/// `#[serde(default)]`), and modern binaries fall back to the
/// conversation offset when `last_message_id` is absent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildCheckpoint {
    /// Which tier is being built.
    pub tier: TierKind,
    /// Embedder ID for this build.
    pub embedder_id: String,
    /// Last conversation offset processed (for pagination).
    pub last_offset: i64,
    /// Total documents embedded so far in this build.
    pub docs_embedded: u64,
    /// Total conversations processed so far.
    pub conversations_processed: u64,
    /// Total conversations expected (from DB at start of build).
    pub total_conversations: u64,
    /// DB fingerprint when this build started.
    pub db_fingerprint: String,
    /// Schema version for this build.
    pub schema_version: u32,
    /// Chunking version for this build.
    pub chunking_version: u32,
    /// Unix timestamp (ms) when this checkpoint was saved.
    pub saved_at_ms: i64,
    /// Highest canonical message PK embedded in this run so far.
    ///
    /// Added in cass#257 (sub-fix 2). `None` for checkpoints written
    /// by pre-#257 binaries; new code falls back to `last_offset`
    /// (conversation-granularity) when this is absent. New code
    /// strictly resumes past this cursor when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_message_id: Option<i64>,
    /// Whether the canonical selection cursor proved there are no more
    /// eligible conversations after this checkpoint.
    ///
    /// This is distinct from count-based progress: checkpoint caps and
    /// cursor predicates can leave more work even when processed counts
    /// appear to have reached the DB total snapshot.
    #[serde(default, skip_serializing_if = "is_false")]
    pub cursor_exhausted: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

impl BuildCheckpoint {
    /// Progress as a percentage (0–100).
    pub fn progress_pct(&self) -> u8 {
        if self.total_conversations == 0 {
            return 0;
        }
        let pct = (self.conversations_processed as f64 / self.total_conversations as f64) * 100.0;
        let pct = (pct as u8).min(100);
        if pct == 100 && !self.cursor_exhausted {
            99
        } else {
            pct
        }
    }

    /// Whether the build is complete (all conversations processed).
    pub fn is_complete(&self) -> bool {
        self.cursor_exhausted && self.conversations_processed >= self.total_conversations
    }

    /// Whether this checkpoint is still valid against the current DB and policy.
    pub fn is_valid(&self, current_db_fingerprint: &str) -> bool {
        self.db_fingerprint == current_db_fingerprint
            && self.schema_version == SEMANTIC_SCHEMA_VERSION
            && self.chunking_version == CHUNKING_STRATEGY_VERSION
    }
}

// ─── Backlog ledger ────────────────────────────────────────────────────────

/// Tracks what semantic build work remains.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BacklogLedger {
    /// Total conversations in the canonical DB at last check.
    pub total_conversations: u64,
    /// Conversations embedded in the fast tier.
    pub fast_tier_processed: u64,
    /// Conversations embedded in the quality tier.
    pub quality_tier_processed: u64,
    /// DB fingerprint when this ledger was computed.
    pub db_fingerprint: String,
    /// Unix timestamp (ms) when this ledger was computed.
    pub computed_at_ms: i64,
}

impl BacklogLedger {
    /// Remaining conversations for the fast tier.
    pub fn fast_tier_remaining(&self) -> u64 {
        self.total_conversations
            .saturating_sub(self.fast_tier_processed)
    }

    /// Remaining conversations for the quality tier.
    pub fn quality_tier_remaining(&self) -> u64 {
        self.total_conversations
            .saturating_sub(self.quality_tier_processed)
    }

    /// Whether either tier has outstanding work.
    pub fn has_pending_work(&self) -> bool {
        self.fast_tier_remaining() > 0 || self.quality_tier_remaining() > 0
    }

    /// Whether the ledger is current with the given DB fingerprint.
    pub fn is_current(&self, current_db_fingerprint: &str) -> bool {
        self.db_fingerprint == current_db_fingerprint
    }
}

// ─── The top-level manifest ────────────────────────────────────────────────

/// Durable, atomic semantic asset manifest.
///
/// This is the single source of truth for what semantic assets exist, their
/// provenance, and what work remains.  It is loaded/saved as JSON.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SemanticManifest {
    /// Format version — for future migrations.
    pub manifest_version: u32,
    /// Fast-tier vector artifact (hash embedder).
    pub fast_tier: Option<ArtifactRecord>,
    /// Quality-tier vector artifact (ML embedder).
    pub quality_tier: Option<ArtifactRecord>,
    /// HNSW accelerator index.
    pub hnsw: Option<HnswRecord>,
    /// Backlog / progress tracker.
    pub backlog: BacklogLedger,
    /// Active build checkpoint (for resuming interrupted work).
    pub checkpoint: Option<BuildCheckpoint>,
    /// Unix timestamp (ms) when this manifest was last written.
    pub updated_at_ms: i64,
}

impl Default for SemanticManifest {
    fn default() -> Self {
        Self {
            manifest_version: MANIFEST_FORMAT_VERSION,
            fast_tier: None,
            quality_tier: None,
            hnsw: None,
            backlog: BacklogLedger {
                total_conversations: 0,
                fast_tier_processed: 0,
                quality_tier_processed: 0,
                db_fingerprint: String::new(),
                computed_at_ms: 0,
            },
            checkpoint: None,
            updated_at_ms: 0,
        }
    }
}

impl SemanticManifest {
    // ── Path helpers ───────────────────────────────────────────────────

    /// Path to the manifest file.
    pub fn path(data_dir: &Path) -> PathBuf {
        data_dir.join("vector_index").join(MANIFEST_FILENAME)
    }

    // ── Load / Save ───────────────────────────────────────────────────

    /// Load the manifest from disk.  Returns `None` if the file doesn't
    /// exist, `Err` if it exists but is corrupt.
    ///
    /// **Migration notes (cass#257 sub-fix 2):** a v1 manifest written
    /// by a pre-#257 binary loads cleanly — `last_message_id` defaults
    /// to `None` and resume falls back to the conversation offset.
    /// A one-shot warning surfaces on first load so an operator sees
    /// that resume granularity is coarser than ideal until the next
    /// checkpoint save lands and the on-disk shape upgrades.
    pub fn load(data_dir: &Path) -> Result<Option<Self>, ManifestError> {
        let path = Self::path(data_dir);
        let bytes = match fs::read(&path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(ManifestError::Io {
                    path,
                    source: e.to_string(),
                });
            }
        };

        let manifest: Self = serde_json::from_slice(&bytes).map_err(|e| ManifestError::Parse {
            path: path.clone(),
            source: e.to_string(),
        })?;

        // Forward-compatible: reject future manifest versions we can't read.
        if manifest.manifest_version > MANIFEST_FORMAT_VERSION {
            return Err(ManifestError::UnsupportedVersion {
                found: manifest.manifest_version,
                max_supported: MANIFEST_FORMAT_VERSION,
            });
        }

        // Backwards-compatible: a v1 manifest with an active checkpoint
        // means a previous interrupted backfill saved without the
        // `last_message_id` cursor. Warn so an operator monitoring an
        // overnight run knows resume granularity is conversation-coarse
        // until the next checkpoint save bumps the on-disk shape.
        if manifest.manifest_version <= MANIFEST_FORMAT_VERSION_PRE_LAST_MESSAGE_CURSOR
            && manifest
                .checkpoint
                .as_ref()
                .is_some_and(|cp| cp.last_message_id.is_none())
        {
            tracing::warn!(
                manifest_version = manifest.manifest_version,
                supported_version = MANIFEST_FORMAT_VERSION,
                path = %path.display(),
                "semantic checkpoint manifest predates last_message_id cursor (cass#257 sub-fix 2); resume will fall back to conversation offset until the next checkpoint save"
            );
        }

        Ok(Some(manifest))
    }

    /// Load the manifest, returning defaults if absent or corrupt.
    ///
    /// Unlike [`Self::load`], this method treats parse errors and version mismatches
    /// as "manifest absent" — the caller gets a clean default rather than an
    /// error.  This is the right behaviour for runtime code that must always
    /// make progress.
    pub fn load_or_default(data_dir: &Path) -> Result<Self, ManifestError> {
        match Self::load(data_dir) {
            Ok(Some(manifest)) => Ok(manifest),
            Ok(None) => Ok(Self::default()),
            // I/O errors are real failures — propagate the original.
            Err(e @ ManifestError::Io { .. }) => Err(e),
            // Parse or version errors → treat as absent.
            Err(ManifestError::Parse { .. } | ManifestError::UnsupportedVersion { .. }) => {
                Ok(Self::default())
            }
            Err(e) => Err(e),
        }
    }

    /// Atomically save the manifest to disk (write-to-temp then rename).
    pub fn save(&mut self, data_dir: &Path) -> Result<(), ManifestError> {
        let path = Self::path(data_dir);

        // Ensure parent directory exists.
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| ManifestError::Io {
                path: parent.to_path_buf(),
                source: e.to_string(),
            })?;
        }

        self.updated_at_ms = now_ms();

        let json = serde_json::to_string_pretty(self).map_err(|e| ManifestError::Serialize {
            source: e.to_string(),
        })?;

        // Atomic write: temp file → rename.
        let (tmp_path, mut file) =
            create_unique_manifest_temp_file(&path).map_err(|e| ManifestError::Io {
                path: path.clone(),
                source: e.to_string(),
            })?;
        file.write_all(json.as_bytes())
            .map_err(|e| ManifestError::Io {
                path: tmp_path.clone(),
                source: e.to_string(),
            })?;
        file.sync_all().map_err(|e| ManifestError::Io {
            path: tmp_path.clone(),
            source: e.to_string(),
        })?;
        replace_file_from_temp(&tmp_path, &path).map_err(|e| ManifestError::Io {
            path: path.clone(),
            source: e.to_string(),
        })?;
        sync_parent_directory(&path).map_err(|e| ManifestError::Io {
            path: path
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| path.clone()),
            source: e.to_string(),
        })?;

        Ok(())
    }

    // ── Readiness evaluation ──────────────────────────────────────────

    /// Evaluate readiness of the fast tier.
    pub fn fast_tier_readiness(
        &self,
        policy: &SemanticPolicy,
        current_db_fingerprint: &str,
        current_model_revision: &str,
    ) -> TierReadiness {
        match &self.fast_tier {
            Some(artifact) => {
                artifact.readiness(policy, current_db_fingerprint, current_model_revision)
            }
            None => {
                // Check for an active build checkpoint for this tier.
                if let Some(cp) = &self.checkpoint
                    && cp.tier == TierKind::Fast
                    && cp.is_valid(current_db_fingerprint)
                {
                    TierReadiness::Building {
                        progress_pct: cp.progress_pct(),
                    }
                } else {
                    TierReadiness::Missing
                }
            }
        }
    }

    /// Evaluate readiness of the quality tier.
    pub fn quality_tier_readiness(
        &self,
        policy: &SemanticPolicy,
        current_db_fingerprint: &str,
        current_model_revision: &str,
    ) -> TierReadiness {
        match &self.quality_tier {
            Some(artifact) => {
                artifact.readiness(policy, current_db_fingerprint, current_model_revision)
            }
            None => {
                if let Some(cp) = &self.checkpoint
                    && cp.tier == TierKind::Quality
                    && cp.is_valid(current_db_fingerprint)
                {
                    TierReadiness::Building {
                        progress_pct: cp.progress_pct(),
                    }
                } else {
                    TierReadiness::Missing
                }
            }
        }
    }

    /// Whether hybrid refinement can run right now (fast tier usable).
    pub fn can_hybrid_search(
        &self,
        policy: &SemanticPolicy,
        current_db_fingerprint: &str,
        current_model_revision: &str,
    ) -> bool {
        self.fast_tier_readiness(policy, current_db_fingerprint, current_model_revision)
            .is_usable()
    }

    // ── Backlog / checkpoint management ───────────────────────────────

    /// Update the backlog from the canonical DB state.
    pub fn refresh_backlog(&mut self, total_conversations: u64, current_db_fingerprint: &str) {
        let fast_processed = self
            .fast_tier
            .as_ref()
            .filter(|a| a.ready && a.db_fingerprint == current_db_fingerprint)
            .map_or(0, |a| a.conversation_count);
        let quality_processed = self
            .quality_tier
            .as_ref()
            .filter(|a| a.ready && a.db_fingerprint == current_db_fingerprint)
            .map_or(0, |a| a.conversation_count);

        self.backlog = BacklogLedger {
            total_conversations,
            fast_tier_processed: fast_processed,
            quality_tier_processed: quality_processed,
            db_fingerprint: current_db_fingerprint.to_owned(),
            computed_at_ms: now_ms(),
        };
    }

    /// Save a build checkpoint (called periodically during backfill).
    pub fn save_checkpoint(&mut self, checkpoint: BuildCheckpoint) {
        self.checkpoint = Some(checkpoint);
    }

    /// Clear the build checkpoint (called when build finishes or is abandoned).
    pub fn clear_checkpoint(&mut self) {
        self.checkpoint = None;
    }

    /// Record a completed artifact and clear the matching checkpoint.
    pub fn publish_artifact(&mut self, artifact: ArtifactRecord) {
        // Clear checkpoint if it matches this tier.
        if self
            .checkpoint
            .as_ref()
            .is_some_and(|cp| cp.tier == artifact.tier)
        {
            self.checkpoint = None;
        }

        match artifact.tier {
            TierKind::Fast => self.fast_tier = Some(artifact),
            TierKind::Quality => self.quality_tier = Some(artifact),
        }
    }

    /// Record a completed HNSW accelerator.
    pub fn publish_hnsw(&mut self, hnsw: HnswRecord) {
        self.hnsw = Some(hnsw);
    }

    /// Adopt a legacy (pre-manifest) artifact if it is compatible with the
    /// current schema/chunking versions.  Returns `true` if adopted.
    #[allow(clippy::too_many_arguments)]
    pub fn adopt_legacy_artifact(
        &mut self,
        tier: TierKind,
        embedder_id: &str,
        model_revision: &str,
        dimension: usize,
        doc_count: u64,
        conversation_count: u64,
        db_fingerprint: &str,
        index_path: &str,
        size_bytes: u64,
    ) -> bool {
        let record = ArtifactRecord {
            tier,
            embedder_id: embedder_id.to_owned(),
            model_revision: model_revision.to_owned(),
            schema_version: SEMANTIC_SCHEMA_VERSION,
            chunking_version: CHUNKING_STRATEGY_VERSION,
            dimension,
            doc_count,
            conversation_count,
            db_fingerprint: db_fingerprint.to_owned(),
            index_path: index_path.to_owned(),
            size_bytes,
            started_at_ms: 0,
            completed_at_ms: now_ms(),
            ready: true,
        };

        match tier {
            TierKind::Fast => self.fast_tier = Some(record),
            TierKind::Quality => self.quality_tier = Some(record),
        }
        true
    }

    /// Invalidate artifacts that are incompatible with the current policy.
    /// Returns the number of artifacts invalidated.
    ///
    /// **Note**: This detects schema version, chunking version, and mode
    /// incompatibilities.  It does NOT detect embedder changes (e.g., minilm →
    /// snowflake) because the policy stores short names while artifacts store
    /// full registry IDs.  Callers who need embedder-change detection should
    /// compare `artifact.embedder_id` against the expected ID from the
    /// embedder registry.
    pub fn invalidate_incompatible(
        &mut self,
        policy: &SemanticPolicy,
        current_model_revision: &str,
    ) -> usize {
        let mut count = 0;

        if let Some(ref artifact) = self.fast_tier {
            let pm = artifact.to_policy_manifest();
            if matches!(
                pm.invalidation_action(policy, current_model_revision, &artifact.embedder_id),
                InvalidationAction::DiscardAndRebuild { .. } | InvalidationAction::Evict
            ) {
                self.fast_tier = None;
                count += 1;
            }
        }

        if let Some(ref artifact) = self.quality_tier {
            let pm = artifact.to_policy_manifest();
            if matches!(
                pm.invalidation_action(policy, current_model_revision, &artifact.embedder_id),
                InvalidationAction::DiscardAndRebuild { .. } | InvalidationAction::Evict
            ) {
                self.quality_tier = None;
                count += 1;
            }
        }

        // HNSW depends on the base tier — invalidate if base is gone.
        if let Some(ref hnsw) = self.hnsw {
            let base_gone = match hnsw.base_tier {
                TierKind::Fast => self.fast_tier.is_none(),
                TierKind::Quality => self.quality_tier.is_none(),
            };
            if base_gone {
                self.hnsw = None;
                count += 1;
            }
        }

        // Invalidate checkpoint if its schema/chunking is wrong.
        if let Some(ref cp) = self.checkpoint
            && (cp.schema_version != policy.semantic_schema_version
                || cp.chunking_version != policy.chunking_strategy_version)
        {
            self.checkpoint = None;
        }

        count
    }

    /// Total disk usage of all semantic artifacts (bytes).
    pub fn total_size_bytes(&self) -> u64 {
        let fast = self.fast_tier.as_ref().map_or(0, |a| a.size_bytes);
        let quality = self.quality_tier.as_ref().map_or(0, |a| a.size_bytes);
        let hnsw = self.hnsw.as_ref().map_or(0, |h| h.size_bytes);
        fast + quality + hnsw
    }

    /// Total disk usage in megabytes (rounded up).
    pub fn total_size_mb(&self) -> u64 {
        self.total_size_bytes().div_ceil(1_048_576)
    }
}
