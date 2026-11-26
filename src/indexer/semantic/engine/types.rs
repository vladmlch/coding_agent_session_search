#[cfg(unix)]
use std::fs;
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::time::UNIX_EPOCH;

use crate::search::semantic_manifest::{ArtifactRecord, BuildCheckpoint, TierKind};
use crate::search::vector_index::ROLE_USER;
use crate::storage::sqlite::FrankenStorage;

#[derive(Debug, Clone)]
pub struct EmbeddingInput {
    pub message_id: u64,
    pub created_at_ms: i64,
    pub agent_id: u32,
    pub workspace_id: u32,
    pub source_id: u32,
    pub role: u8,
    pub chunk_idx: u8,
    pub content: String,
}

impl EmbeddingInput {
    pub fn new(message_id: u64, content: impl Into<String>) -> Self {
        Self {
            message_id,
            created_at_ms: 0,
            agent_id: 0,
            workspace_id: 0,
            source_id: 0,
            role: ROLE_USER,
            chunk_idx: 0,
            content: content.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EmbeddedMessage {
    pub message_id: u64,
    pub created_at_ms: i64,
    pub agent_id: u32,
    pub workspace_id: u32,
    pub source_id: u32,
    pub role: u8,
    pub chunk_idx: u8,
    pub content_hash: [u8; 32],
    pub embedding: Vec<f32>,
}

#[derive(Debug, Clone)]
pub struct SemanticBackfillBatchPlan {
    pub tier: TierKind,
    pub db_fingerprint: String,
    pub model_revision: String,
    pub total_conversations: u64,
    pub conversations_in_batch: u64,
    pub last_offset: i64,
    pub cursor_exhausted: bool,
}

#[derive(Debug, Clone)]
pub struct SemanticBackfillStoragePlan {
    pub tier: TierKind,
    pub db_fingerprint: String,
    pub model_revision: String,
    pub max_conversations: usize,
}

pub(in crate::indexer::semantic::engine) struct BackfillBatchProgress {
    pub(in crate::indexer::semantic::engine) prior_checkpoint: Option<BuildCheckpoint>,
    pub(in crate::indexer::semantic::engine) embedded_docs: u64,
    pub(in crate::indexer::semantic::engine) last_message_id: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct SemanticBackfillBatchOutcome {
    pub tier: TierKind,
    pub embedder_id: String,
    pub embedded_docs: u64,
    pub conversations_processed: u64,
    pub total_conversations: u64,
    pub last_offset: i64,
    pub checkpoint_saved: bool,
    pub published: bool,
    /// The completed artifact was already current; no scan or publish ran.
    pub unchanged: bool,
    pub index_path: PathBuf,
    pub manifest_path: PathBuf,
}

/// Cache only completed coverage. File identities include ctime on Unix so
/// restoring an old mtime after an in-place edit cannot authorize a no-op.
/// Platforms without that change stamp keep the full reconciliation path.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(in crate::indexer::semantic::engine) struct BackfillFileStamp {
    len: u64,
    modified: (u64, u32),
    #[serde(default, skip_serializing_if = "Option::is_none")]
    changed: Option<(i64, i64)>,
    identity: (u64, u64),
    #[serde(default, skip_serializing_if = "Option::is_none")]
    edge_digest: Option<[u8; 32]>,
}

#[cfg(unix)]
fn is_sqlite_wal_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with("-wal"))
}

#[cfg(unix)]
fn read_wal_edge_digest(
    path: &Path,
    expected_len: u64,
    expected_identity: (u64, u64),
) -> Option<[u8; 32]> {
    use std::io::{Read, Seek, SeekFrom};
    use std::os::unix::fs::MetadataExt;
    const EDGE_SAMPLE_BYTES: u64 = 4096;

    let mut file = fs::File::open(path).ok()?;
    let opened = file.metadata().ok()?;
    if opened.len() != expected_len || (opened.dev(), opened.ino()) != expected_identity {
        return None;
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(&expected_len.to_le_bytes());
    let head_len = usize::try_from(expected_len.min(EDGE_SAMPLE_BYTES)).ok()?;
    let mut head = vec![0_u8; head_len];
    file.read_exact(&mut head).ok()?;
    hasher.update(&head);
    if expected_len > EDGE_SAMPLE_BYTES {
        let tail_start = expected_len
            .saturating_sub(EDGE_SAMPLE_BYTES)
            .max(head_len as u64);
        let tail_len = usize::try_from(expected_len - tail_start).ok()?;
        file.seek(SeekFrom::Start(tail_start)).ok()?;
        let mut tail = vec![0_u8; tail_len];
        file.read_exact(&mut tail).ok()?;
        hasher.update(&tail);
    }
    Some(*hasher.finalize().as_bytes())
}

#[cfg(unix)]
fn backfill_file_stamp_with_ctime(
    path: &Path,
    include_ctime: bool,
) -> Option<Option<BackfillFileStamp>> {
    use std::os::unix::fs::MetadataExt;
    let metadata = match fs::metadata(path) {
        Ok(metadata) if metadata.is_file() => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Some(None),
        _ => return None,
    };
    let modified = metadata.modified().ok()?.duration_since(UNIX_EPOCH).ok()?;
    if !include_ctime && metadata.len() == 0 {
        // An empty 0-byte SQLite -wal carries zero frames, equivalent to absent.
        return Some(None);
    }
    let identity = (metadata.dev(), metadata.ino());
    let edge_digest = if include_ctime {
        None
    } else {
        Some(read_wal_edge_digest(path, metadata.len(), identity)?)
    };
    Some(Some(BackfillFileStamp {
        len: metadata.len(),
        modified: (modified.as_secs(), modified.subsec_nanos()),
        changed: include_ctime.then(|| (metadata.ctime(), metadata.ctime_nsec())),
        identity,
        edge_digest,
    }))
}

#[cfg(unix)]
pub(in crate::indexer::semantic::engine) fn backfill_file_stamp(
    path: &Path,
) -> Option<Option<BackfillFileStamp>> {
    backfill_file_stamp_with_ctime(path, !is_sqlite_wal_path(path))
}

#[cfg(not(unix))]
pub(in crate::indexer::semantic::engine) fn backfill_file_stamp(
    _path: &Path,
) -> Option<Option<BackfillFileStamp>> {
    None
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(in crate::indexer::semantic::engine) struct BackfillFilePair {
    pub(in crate::indexer::semantic::engine) path: PathBuf,
    pub(in crate::indexer::semantic::engine) main: BackfillFileStamp,
    pub(in crate::indexer::semantic::engine) wal: Option<BackfillFileStamp>,
}

impl BackfillFilePair {
    pub(in crate::indexer::semantic::engine) fn capture(path: &Path, wal: &Path) -> Option<Self> {
        Some(Self {
            path: path.canonicalize().ok()?,
            main: backfill_file_stamp(path)??,
            wal: backfill_file_stamp(wal)?,
        })
    }

    #[cfg(unix)]
    pub(in crate::indexer::semantic::engine) fn archive(storage: &FrankenStorage) -> Option<Self> {
        if storage.raw().in_transaction() {
            return None;
        }
        // The VFS resolves symlinks before choosing its WAL. Bind this lookup
        // to the actual open main-file descriptor as well: a pathname can
        // have been replaced since the storage handle was opened.
        let path = storage.database_path().ok()?.canonicalize().ok()?;
        let descriptor = storage
            .raw()
            .file_identity()
            .ok()
            .flatten()
            .or_else(|| storage.raw().snapshot_source_identity())?;
        if crate::franken_sync::FileIdentity::from_path(&path).ok()? != Some(descriptor) {
            return None;
        }
        let wal_path = crate::storage::sqlite::database_sidecar_path(&path, "-wal");
        // Exclude -wal ctime because C SQLite's unixOpen calls robustFchown
        // under euid==0 even on read-only opens, shifting ctime without content writes.
        let pair = Self {
            path: path.clone(),
            main: backfill_file_stamp_with_ctime(&path, true)??,
            wal: backfill_file_stamp_with_ctime(&wal_path, false)?,
        };
        (crate::franken_sync::FileIdentity::from_path(&path).ok()? == Some(descriptor))
            .then_some(pair)
    }

    #[cfg(not(unix))]
    pub(in crate::indexer::semantic::engine) fn archive(_storage: &FrankenStorage) -> Option<Self> {
        None
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(in crate::indexer::semantic::engine) struct CompletedBackfillCache {
    pub(in crate::indexer::semantic::engine) version: u32,
    pub(in crate::indexer::semantic::engine) archive: BackfillFilePair,
    pub(in crate::indexer::semantic::engine) vectors: BackfillFilePair,
    pub(in crate::indexer::semantic::engine) artifact: ArtifactRecord,
    pub(in crate::indexer::semantic::engine) vector_space_revision: String,
    pub(in crate::indexer::semantic::engine) last_offset: i64,
}

impl SemanticBackfillBatchOutcome {
    pub fn progress_pct(&self) -> f64 {
        if self.total_conversations == 0 {
            return if self.published { 100.0 } else { 0.0 };
        }

        let pct = (self.conversations_processed as f64 / self.total_conversations as f64) * 100.0;
        let pct = pct.min(100.0);
        if pct >= 100.0 && !self.published {
            99.0
        } else {
            pct
        }
    }
}

#[derive(Debug, Clone)]
pub struct SemanticShardBuildPlan {
    pub tier: TierKind,
    pub db_fingerprint: String,
    pub model_revision: String,
    pub total_conversations: u64,
    pub max_records_per_shard: usize,
    pub build_ann: bool,
}

#[derive(Debug, Clone)]
pub struct SemanticShardBuildOutcome {
    pub tier: TierKind,
    pub embedder_id: String,
    pub shard_count: u32,
    pub doc_count: u64,
    pub total_conversations: u64,
    pub index_paths: Vec<PathBuf>,
    pub ann_index_paths: Vec<PathBuf>,
    pub shard_manifest_path: PathBuf,
    pub complete: bool,
}
