use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::UNIX_EPOCH;

use anyhow::{Context, Result};

pub(crate) use super::lexical_checkpoint_io::load_lexical_rebuild_checkpoint;
use super::lexical_checkpoint_io::{
    count_total_conversations_exact, lexical_rebuild_content_fingerprint,
    lexical_rebuild_storage_fingerprint,
};
use crate::storage::sqlite::{FrankenStorage, database_sidecar_path};

pub(crate) fn lexical_storage_fingerprint_for_db(db_path: &Path) -> Result<String> {
    lexical_rebuild_storage_fingerprint(db_path)
}

/// Compute the lexical fingerprint without invoking any recovery-capable read
/// opener. Used by strict read-only search so a dirty WAL or duplicate schema
/// is surfaced rather than repaired as a side effect of freshness validation.
pub(crate) fn lexical_storage_fingerprint_for_db_strict(db_path: &Path) -> Result<String> {
    let storage = FrankenStorage::open_strict_readonly(db_path).with_context(|| {
        format!(
            "strictly opening readonly storage to compute lexical fingerprint for {}",
            db_path.display()
        )
    })?;
    let total_conversations = count_total_conversations_exact(&storage)?;
    let fingerprint = lexical_rebuild_content_fingerprint(&storage, total_conversations)?;
    storage.close_without_checkpoint().with_context(|| {
        format!(
            "closing strict readonly storage without checkpoint for {}",
            db_path.display()
        )
    })?;
    Ok(fingerprint)
}

/// Sidecar (inside the lexical index directory, next to the rebuild
/// checkpoint) that memoizes the canonical archive fingerprint against the
/// archive's physical identity. It exists for the one-shot CLI: every
/// `cass search --robot` is a fresh process, so an in-process memo alone
/// never hits on the path that matters most.
pub(crate) const ARCHIVE_FINGERPRINT_CACHE_FILE: &str = ".archive-fingerprint-cache.json";

pub(crate) const ARCHIVE_FINGERPRINT_CACHE_SCHEMA_VERSION: u32 = 1;

/// Physical identity of a canonical archive for fingerprint memoization: the
/// size and mtime of the main database file and of its WAL sidecar. Any
/// committed write touches at least one of them (a WAL append changes the WAL
/// length and mtime; a checkpoint changes the main file), so an unchanged key
/// means the strict fingerprint computed last time is still the current one.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub(crate) struct ArchivePhysicalIdentity {
    pub(crate) db_len: u64,
    pub(crate) db_mtime_secs: u64,
    pub(crate) db_mtime_nanos: u32,
    pub(crate) wal_len: u64,
    pub(crate) wal_mtime_secs: u64,
    pub(crate) wal_mtime_nanos: u32,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct ArchiveFingerprintCache {
    pub(crate) schema_version: u32,
    pub(crate) identity: ArchivePhysicalIdentity,
    pub(crate) fingerprint: String,
    pub(crate) computed_at_ms: i64,
}

pub(crate) fn lexical_storage_physical_identity(db_path: &Path) -> Option<ArchivePhysicalIdentity> {
    fn stamp(path: &Path) -> Option<(u64, u64, u32)> {
        match fs::metadata(path) {
            Ok(meta) => {
                let modified = meta
                    .modified()
                    .ok()
                    .and_then(|time| time.duration_since(UNIX_EPOCH).ok())?;
                Some((meta.len(), modified.as_secs(), modified.subsec_nanos()))
            }
            // An absent WAL sidecar is a legitimate, stable state.
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Some((0, 0, 0)),
            Err(_) => None,
        }
    }
    let (db_len, db_mtime_secs, db_mtime_nanos) = stamp(db_path)?;
    if db_len == 0 {
        // No archive (or an empty placeholder): nothing worth memoizing.
        return None;
    }
    let wal_path = database_sidecar_path(db_path, "-wal");
    let (wal_len, wal_mtime_secs, wal_mtime_nanos) = stamp(&wal_path)?;
    Some(ArchivePhysicalIdentity {
        db_len,
        db_mtime_secs,
        db_mtime_nanos,
        wal_len,
        wal_mtime_secs,
        wal_mtime_nanos,
    })
}

pub(crate) fn write_archive_fingerprint_cache_for_identity(
    identity: &ArchivePhysicalIdentity,
    cache_dir: &Path,
    fingerprint: &str,
) {
    if !fingerprint.starts_with("content-v1:") || !cache_dir.is_dir() {
        return;
    }
    // ubs:ignore[rust.security.path-traversal] — joins compile-time ARCHIVE_FINGERPRINT_CACHE_FILE constant
    let cache_path = cache_dir.join(ARCHIVE_FINGERPRINT_CACHE_FILE);
    let payload = ArchiveFingerprintCache {
        schema_version: ARCHIVE_FINGERPRINT_CACHE_SCHEMA_VERSION,
        identity: identity.clone(),
        fingerprint: fingerprint.to_string(),
        computed_at_ms: FrankenStorage::now_millis(),
    };
    // Per-process temp name: concurrent one-shot searches may all miss at once.
    let tmp_path = cache_dir.join(format!(
        "{ARCHIVE_FINGERPRINT_CACHE_FILE}.{}.tmp",
        std::process::id()
    ));
    let written = serde_json::to_vec(&payload)
        .map_err(std::io::Error::other)
        .and_then(|bytes| fs::write(&tmp_path, bytes))
        .and_then(|()| fs::rename(&tmp_path, &cache_path));
    if let Err(err) = written {
        tracing::debug!(
            error = %err,
            cache = %cache_path.display(),
            "archive fingerprint cache write skipped"
        );
        let _ = fs::remove_file(&tmp_path);
    }
}

pub(crate) fn write_archive_fingerprint_cache_best_effort(
    db_path: &Path,
    cache_dir: &Path,
    fingerprint: &str,
) {
    let Some(identity) = lexical_storage_physical_identity(db_path) else {
        return;
    };
    write_archive_fingerprint_cache_for_identity(&identity, cache_dir, fingerprint);
}

pub(crate) fn prime_archive_fingerprint_cache_from_completed_checkpoint(
    db_path: &Path,
    index_path: &Path,
) {
    let Ok(Some(checkpoint)) = load_lexical_rebuild_checkpoint(index_path) else {
        return;
    };
    if checkpoint.completed
        && crate::model::paths::stored_path_identity_matches(&checkpoint.db_path, db_path)
    {
        write_archive_fingerprint_cache_best_effort(
            db_path,
            index_path,
            &checkpoint.storage_fingerprint,
        );
    }
}

/// The on-disk layer of [`lexical_storage_fingerprint_for_db_cached`]: serve
/// the sidecar's fingerprint when it was computed for exactly this physical
/// identity, otherwise compute the strict fingerprint and rewrite the sidecar
/// (best effort, tmp + rename, never fatal). The sidecar is never consulted
/// by the indexer's own checkpoint logic, which always recomputes.
pub(crate) fn lexical_storage_fingerprint_for_db_disk_cached(
    db_path: &Path,
    identity: &ArchivePhysicalIdentity,
    cache_dir: &Path,
) -> Result<String> {
    // ubs:ignore[rust.security.path-traversal] — joins compile-time ARCHIVE_FINGERPRINT_CACHE_FILE constant
    let cache_path = cache_dir.join(ARCHIVE_FINGERPRINT_CACHE_FILE);
    if let Ok(raw) = fs::read(&cache_path)
        && let Ok(cached) = serde_json::from_slice::<ArchiveFingerprintCache>(&raw)
        && cached.schema_version == ARCHIVE_FINGERPRINT_CACHE_SCHEMA_VERSION
        && &cached.identity == identity
        && cached.fingerprint.starts_with("content-v1:")
    {
        tracing::debug!(
            archive_fingerprint_cache = "hit",
            cache = %cache_path.display(),
            "archive fingerprint served from the identity sidecar"
        );
        return Ok(cached.fingerprint);
    }
    tracing::debug!(
        archive_fingerprint_cache = "miss",
        cache = %cache_path.display(),
        "archive fingerprint recomputed from the canonical database"
    );
    let fingerprint = lexical_storage_fingerprint_for_db_strict(db_path)?;
    write_archive_fingerprint_cache_for_identity(identity, cache_dir, &fingerprint);
    Ok(fingerprint)
}

/// [`lexical_storage_fingerprint_for_db_strict`], memoized on the archive's
/// physical identity (`lexical_storage_physical_identity`) and the lexical
/// index it is being validated against — first in this process, then in the
/// on-disk sidecar inside `index_path`.
///
/// The strict variant opens a synchronous read-only handle and replays the
/// WAL; on a large archive that cost every default `cass search` seconds
/// (measured: 201 MB of WAL read in 97k calls, 3.5–4 s) before the engine
/// query even ran, and a one-shot CLI process cannot amortize it in memory.
/// A search-time freshness check only needs the answer to change when the
/// archive does, and any committed write moves the database or WAL
/// size/mtime, so both layers are invalidated exactly when they must be. When
/// the identity cannot be read the strict path runs uncached.
pub(crate) fn lexical_storage_fingerprint_for_db_cached(
    db_path: &Path,
    index_path: &Path,
) -> Result<String> {
    type MemoKey = (PathBuf, PathBuf, ArchivePhysicalIdentity);
    static MEMO: std::sync::OnceLock<Mutex<HashMap<MemoKey, String>>> = std::sync::OnceLock::new();
    let Some(identity) = lexical_storage_physical_identity(db_path) else {
        return lexical_storage_fingerprint_for_db_strict(db_path);
    };
    let key: MemoKey = (
        db_path.to_path_buf(),
        index_path.to_path_buf(),
        identity.clone(),
    );
    let memo = MEMO.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(fingerprint) = memo.lock().ok().and_then(|guard| guard.get(&key).cloned()) {
        return Ok(fingerprint);
    }
    let fingerprint =
        lexical_storage_fingerprint_for_db_disk_cached(db_path, &identity, index_path)?;
    if let Ok(mut guard) = memo.lock() {
        // One entry per archive/index pair: a changed identity replaces the
        // stale one rather than accumulating history.
        guard.retain(|(db, index, _), _| !(db == &key.0 && index == &key.1));
        guard.insert(key, fingerprint.clone());
    }
    Ok(fingerprint)
}

/// GH #353: the sidecar-cached archive fingerprint WITHOUT opening the
/// database. The watermark health lane and other `open_skipped` surfaces
/// promise a mutation-free probe with no archive open (k2k20: a readonly
/// open on a large dirty-WAL archive can take minutes), but they must still
/// compare the SAME storage fingerprint `search` defers repair on. Served
/// from the identity-keyed sidecar only; `None` when the sidecar does not
/// describe the current physical identity — an honest "not computed", never
/// a guess. Search primes that sidecar after every real fingerprint
/// computation, so a status that follows any search sees what search saw.
pub(crate) fn lexical_storage_fingerprint_for_db_cached_readonly(
    db_path: &Path,
    index_path: &Path,
) -> Option<String> {
    let identity = lexical_storage_physical_identity(db_path)?;
    // ubs:ignore[rust.security.path-traversal] — joins compile-time ARCHIVE_FINGERPRINT_CACHE_FILE constant
    let cache_path = index_path.join(ARCHIVE_FINGERPRINT_CACHE_FILE);
    let raw = fs::read(cache_path).ok()?;
    let cached = serde_json::from_slice::<ArchiveFingerprintCache>(&raw).ok()?;
    (cached.schema_version == ARCHIVE_FINGERPRINT_CACHE_SCHEMA_VERSION
        && cached.identity == identity
        && cached.fingerprint.starts_with("content-v1:"))
    .then_some(cached.fingerprint)
}

/// Same fingerprint as [`lexical_storage_fingerprint_for_db`], computed on an
/// already-open read-only handle so callers that have paid the archive open
/// cost (semantic context loading, which opens the DB for filter maps anyway)
/// do not pay it a second time just to learn whether their vector assets
/// still describe the current database (GH #404).
pub(crate) fn lexical_storage_fingerprint_for_storage(storage: &FrankenStorage) -> Result<String> {
    let total_conversations = count_total_conversations_exact(storage)?;
    lexical_rebuild_content_fingerprint(storage, total_conversations)
}
