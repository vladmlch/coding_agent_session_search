use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use frankensearch::index::{VectorIndex as FsVectorIndex, wal_path_for as fsvi_wal_path_for};

use super::types::{EmbeddedMessage, EmbeddingInput};
use crate::search::canonicalize::{canonicalize_for_embedding, content_hash};
use crate::search::semantic_manifest::{SemanticShardManifest, TierKind};
use crate::search::vector_index::{SemanticDocId, VECTOR_INDEX_DIR};

pub(in crate::indexer::semantic::engine) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

pub(in crate::indexer::semantic::engine) fn hnsw_index_path(
    data_dir: &Path,
    embedder_id: &str,
) -> PathBuf {
    data_dir
        .join(VECTOR_INDEX_DIR)
        .join(format!("hnsw-{embedder_id}.chsw"))
}

pub(in crate::indexer::semantic::engine) fn safe_path_component(raw: &str) -> String {
    raw.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

pub(in crate::indexer::semantic::engine) fn semantic_staging_index_path(
    data_dir: &Path,
    tier: TierKind,
    embedder_id: &str,
    db_fingerprint: &str,
) -> PathBuf {
    let fingerprint_hash = crc32fast::hash(db_fingerprint.as_bytes());
    data_dir.join(VECTOR_INDEX_DIR).join(format!(
        ".staging-{}-{}-{fingerprint_hash:08x}.fsvi",
        tier.as_str(),
        safe_path_component(embedder_id)
    ))
}

/// Content growth may reuse vectors, but identity migrations belong to a
/// different archive identity generation even when row counts are unchanged.
pub(in crate::indexer::semantic::engine) fn semantic_backfill_identity_matches(
    saved: &str,
    current: &str,
) -> bool {
    fn identity(fingerprint: &str) -> Option<&str> {
        if fingerprint.starts_with("content-v1:") {
            Some("")
        } else {
            let rest = fingerprint.strip_prefix("identity-v1:")?;
            let (generation, content) = rest.split_once(':')?;
            (!generation.is_empty() && content.starts_with("content-v1:")).then_some(generation)
        }
    }
    saved == current || matches!((identity(saved), identity(current)), (Some(a), Some(b)) if a == b)
}

/// Revoke shard-sidecar serving authority after a canonical identity rebuild.
///
/// Query loading prefers a complete current-fingerprint shard generation over
/// the monolithic FSVI. Agent/source rewrites do not change that content
/// fingerprint, so leaving old shard records published would resurrect Pi-era
/// filter ids as soon as the durable identity marker was cleared. The shard
/// files themselves remain untouched for explicit quarantine/GC policy.
pub fn invalidate_identity_stale_semantic_shards(
    data_dir: &Path,
    embedder_id: &str,
) -> Result<usize> {
    let Some(mut manifest) = SemanticShardManifest::load(data_dir).map_err(|err| {
        anyhow::anyhow!("loading semantic shard manifest for identity rebuild: {err}")
    })?
    else {
        return Ok(0);
    };
    let invalidated = manifest.mark_shards_stale_for_embedder(embedder_id);
    if invalidated > 0 {
        manifest.save(data_dir).map_err(|err| {
            anyhow::anyhow!("saving semantic shard manifest after identity rebuild: {err}")
        })?;
        tracing::info!(
            embedder = embedder_id,
            invalidated_shard_records = invalidated,
            "revoked identity-stale semantic shard generations"
        );
    }
    Ok(invalidated)
}

fn semantic_generation_fingerprint_component(db_fingerprint: &str) -> String {
    blake3::hash(db_fingerprint.as_bytes())
        .to_hex()
        .chars()
        .take(16)
        .collect()
}

pub(in crate::indexer::semantic::engine) fn create_semantic_shard_generation_dir(
    data_dir: &Path,
    tier: TierKind,
    embedder_id: &str,
    db_fingerprint: &str,
) -> Result<PathBuf> {
    let fingerprint_hash = semantic_generation_fingerprint_component(db_fingerprint);
    let root = data_dir.join(VECTOR_INDEX_DIR).join("shards");
    fs::create_dir_all(&root).context("create semantic shard generation root")?;
    let prefix = format!(
        "{}-{}-{fingerprint_hash}-",
        tier.as_str(),
        safe_path_component(embedder_id),
    );
    // A source fingerprint is not a build identity. Allocate exclusively so
    // repeated/concurrent builds cannot overwrite an installed shard set.
    let generation = tempfile::Builder::new()
        .prefix(&prefix)
        .tempdir_in(&root)
        .context("allocate private semantic shard generation")?
        .keep();
    // Retain failed builds for explicit inspection/GC. Directory existence is
    // not authority: only the atomically installed manifest selects a build.
    sync_parent_directory(&generation)?;
    sync_parent_directory(&root)?;
    sync_parent_directory(&data_dir.join(VECTOR_INDEX_DIR))?;
    Ok(generation)
}

pub(in crate::indexer::semantic::engine) fn semantic_shard_index_path(
    generation_dir: &Path,
    shard_index: u32,
) -> PathBuf {
    generation_dir.join(format!("shard-{shard_index:05}.fsvi"))
}

pub(in crate::indexer::semantic::engine) fn semantic_shard_ann_index_path(
    generation_dir: &Path,
    shard_index: u32,
) -> PathBuf {
    generation_dir.join(format!("shard-{shard_index:05}.chsw"))
}

#[cfg(not(windows))]
pub(in crate::indexer::semantic::engine) fn sync_parent_directory(path: &Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    let directory = fs::File::open(parent)
        .with_context(|| format!("opening parent directory {}", parent.display()))?;
    directory
        .sync_all()
        .with_context(|| format!("syncing parent directory {}", parent.display()))
}

#[cfg(windows)]
pub(in crate::indexer::semantic::engine) fn sync_parent_directory(_path: &Path) -> Result<()> {
    Ok(())
}

pub(in crate::indexer::semantic::engine) fn semantic_doc_id_for_embedded(
    embedded: &EmbeddedMessage,
) -> String {
    SemanticDocId {
        message_id: embedded.message_id,
        chunk_idx: embedded.chunk_idx,
        agent_id: embedded.agent_id,
        workspace_id: embedded.workspace_id,
        source_id: embedded.source_id,
        role: embedded.role,
        created_at_ms: embedded.created_at_ms,
        content_hash: Some(embedded.content_hash),
    }
    .to_doc_id_string()
}

pub(in crate::indexer::semantic::engine) fn validated_semantic_index_ids(
    index: &FsVectorIndex,
) -> Result<HashSet<String>> {
    if index.wal_record_count() != 0 || index.tombstone_count() != 0 {
        bail!("semantic publication requires a compact index without WAL records or tombstones");
    }
    let mut ids = HashSet::with_capacity(index.record_count());
    for record in 0..index.record_count() {
        let id = index.doc_id_at(record)?;
        if !ids.insert(id.to_owned()) {
            bail!("semantic publication contains duplicate document {id}");
        }
        if index
            .vector_at_f32(record)?
            .iter()
            .any(|value| !value.is_finite())
        {
            bail!("semantic publication contains a non-finite vector");
        }
    }
    Ok(ids)
}

/// The destination WAL belongs to the prior live main file, not the staged
/// candidate. Retain it outside the canonical path before the main-file swap
/// rather than relying on unrelated source/destination generations differing.
pub(in crate::indexer::semantic::engine) fn park_semantic_destination_wal(
    index_path: &Path,
) -> Result<Option<PathBuf>> {
    let wal_path = fsvi_wal_path_for(index_path);
    match fs::metadata(&wal_path) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("inspect semantic destination WAL"),
    }
    let parent = index_path
        .parent()
        .context("semantic index has no parent")?;
    let retained_dir = tempfile::Builder::new()
        .prefix(".quarantined-destination-wal-")
        .tempdir_in(parent)?
        .keep();
    let retained_wal = retained_dir.join("destination.wal");
    fs::rename(&wal_path, &retained_wal).context("retain prior semantic destination WAL")?;
    sync_parent_directory(&wal_path)?;
    sync_parent_directory(&retained_wal)?;
    tracing::info!(wal = %wal_path.display(), retained = %retained_wal.display(),
        "retained prior semantic destination WAL before publication");
    Ok(Some(retained_wal))
}

pub fn semantic_doc_id_for_input(input: &EmbeddingInput) -> Option<String> {
    let canonical = canonicalize_for_embedding(&input.content);
    if canonical.is_empty() {
        return None;
    }
    Some(
        SemanticDocId {
            message_id: input.message_id,
            chunk_idx: input.chunk_idx,
            agent_id: input.agent_id,
            workspace_id: input.workspace_id,
            source_id: input.source_id,
            role: input.role,
            created_at_ms: input.created_at_ms,
            content_hash: Some(content_hash(&canonical)),
        }
        .to_doc_id_string(),
    )
}

#[cfg(not(windows))]
pub(in crate::indexer::semantic::engine) fn publish_reconciled_semantic_index(
    staging_path: &Path,
    final_path: &Path,
) -> Result<()> {
    fs::rename(staging_path, final_path).with_context(|| {
        format!(
            "publishing reconciled semantic index {} to {}",
            staging_path.display(),
            final_path.display()
        )
    })?;
    sync_parent_directory(final_path)
}

#[cfg(windows)]
pub(in crate::indexer::semantic::engine) fn publish_reconciled_semantic_index(
    staging_path: &Path,
    final_path: &Path,
) -> Result<()> {
    if !final_path.exists() {
        fs::rename(staging_path, final_path).with_context(|| {
            format!(
                "publishing reconciled semantic index {} to {}",
                staging_path.display(),
                final_path.display()
            )
        })?;
        return sync_parent_directory(final_path);
    }

    let extension = final_path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("fsvi");
    let backup_path = final_path.with_extension(format!(
        "{extension}.reconcile-backup-{}-{}",
        std::process::id(),
        now_ms()
    ));
    fs::rename(final_path, &backup_path).with_context(|| {
        format!(
            "staging existing semantic index {} at {} before replacement",
            final_path.display(),
            backup_path.display()
        )
    })?;
    match fs::rename(staging_path, final_path) {
        Ok(()) => {
            if let Err(err) = fs::remove_file(&backup_path) {
                tracing::warn!(
                    backup_path = %backup_path.display(),
                    error = %err,
                    "reconciled semantic publish left a recoverable prior-live backup"
                );
            }
            sync_parent_directory(final_path)
        }
        Err(publish_err) => match fs::rename(&backup_path, final_path) {
            Ok(()) => {
                sync_parent_directory(final_path)?;
                Err(publish_err).with_context(|| {
                    format!(
                        "failed publishing reconciled semantic index {}; restored prior live artifact",
                        final_path.display()
                    )
                })
            }
            Err(restore_err) => bail!(
                "failed publishing reconciled semantic index {}; also failed to restore prior live artifact from {}: {}; staged candidate remains at {}",
                final_path.display(),
                backup_path.display(),
                restore_err,
                staging_path.display()
            ),
        },
    }
}
