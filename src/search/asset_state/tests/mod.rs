mod coordination;
mod lexical;
mod semantic;

pub(super) use std::fs::OpenOptions;
pub(super) use std::path::{Path, PathBuf};
pub(super) use std::time::Duration;

pub(super) use crate::search::ann_index::hnsw_index_path;
pub(super) use crate::search::embedder::Embedder;
pub(super) use crate::search::fastembed_embedder::FastEmbedder;
pub(super) use crate::search::hash_embedder::HashEmbedder;
pub(super) use crate::search::model_manager::SemanticAvailability;
pub(super) use crate::search::policy::{CHUNKING_STRATEGY_VERSION, SEMANTIC_SCHEMA_VERSION};
pub(super) use crate::search::semantic_manifest::{
    ArtifactRecord, BuildCheckpoint, SemanticManifest, SemanticShardManifest, SemanticShardRecord,
    TierKind,
};
pub(super) use crate::search::tantivy::SCHEMA_HASH;
pub(super) use crate::search::vector_index::{
    SemanticProgressiveUnavailableReason, VECTOR_INDEX_DIR, vector_index_path,
};
pub(super) use crate::storage::lexical_checkpoint_io::LexicalRebuildCheckpoint;
pub(super) use crate::storage::run_lock::LEXICAL_REBUILD_PAGE_SIZE_PUBLIC;
pub(super) use crate::storage::sqlite::{FrankenStorage, SemanticIdentityTier};

use super::write_index_run_lock_metadata_sidecar;

#[cfg(windows)]
pub(super) fn active_lock_fixture_pid(_non_windows_pid: u32) -> u32 {
    std::process::id()
}

#[cfg(not(windows))]
pub(super) fn active_lock_fixture_pid(non_windows_pid: u32) -> u32 {
    non_windows_pid
}

pub(super) fn write_locked_metadata(
    lock_path: &Path,
    owner: &mut std::fs::File,
    contents: &str,
) -> std::io::Result<()> {
    use std::io::{Seek, SeekFrom, Write};

    owner.set_len(0)?;
    owner.seek(SeekFrom::Start(0))?;
    owner.write_all(contents.as_bytes())?;
    owner.flush()?;
    owner.sync_all()?;
    write_index_run_lock_metadata_sidecar(lock_path, contents).map_err(std::io::Error::other)?;
    Ok(())
}
