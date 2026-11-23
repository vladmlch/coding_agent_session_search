#[cfg(test)]
use super::io::{
    BLOB_CAPTURE_CACHE, ensure_private_dir_descendant, private_create_new_file, raw_mirror_root,
    sync_open_file_if_required, sync_parent, unique_temp_path,
};
use super::io::{
    RAW_MIRROR_BLOB_CACHE_MAX_ENTRIES, RAW_MIRROR_MANIFEST_MAX_BYTES, raw_mirror_io,
    raw_mirror_path_has_symlink_below_root, raw_mirror_symlink_metadata, system_time_to_ns,
};
use super::manifest::{
    open_stable_source_file, raw_mirror_blob_relative_path, source_change_time_ns,
    source_identity_token, verify_existing_blob_reference,
};
use super::types::{
    CachedRawMirrorBlobRecord, PersistedRawMirrorBlobCache, RawMirrorBlobCacheKey,
    RawMirrorBlobRecord, RawMirrorBlobSourceKey, RawMirrorCaptureInput, RawMirrorChunkRef,
    RawMirrorStoredBlobFingerprint,
};
use anyhow::{Result, anyhow};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Read;
#[cfg(test)]
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

pub(in crate::raw_mirror) fn raw_mirror_blob_cache_key(
    input: &RawMirrorCaptureInput<'_>,
    source_metadata: &fs::Metadata,
    chunk_threshold_bytes: u64,
    chunk_size_bytes: usize,
) -> RawMirrorBlobCacheKey {
    RawMirrorBlobCacheKey {
        data_dir: input.data_dir.to_path_buf(),
        source_path: input.source_path.to_path_buf(),
        source_identity: source_identity_token(source_metadata),
        source_size_bytes: source_metadata.len(),
        source_mtime_ns: source_metadata.modified().ok().and_then(system_time_to_ns),
        source_change_time_ns: source_change_time_ns(source_metadata),
        source_sidecars_fingerprint: raw_mirror_source_sidecars_fingerprint(input.source_path),
        chunk_threshold_bytes,
        chunk_size_bytes,
    }
}

fn raw_mirror_source_sidecars_fingerprint(source_path: &Path) -> Option<String> {
    let mut hasher = blake3::Hasher::new();
    for suffix in ["-wal", "-shm", "-journal"] {
        let mut path = source_path.as_os_str().to_os_string();
        path.push(suffix);
        hasher.update(suffix.as_bytes());
        match raw_mirror_symlink_metadata(Path::new(&path)) {
            Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                let fingerprint = format!(
                    "{}:{}:{}:{};",
                    source_identity_token(&metadata)?,
                    metadata.len(),
                    metadata.modified().ok().and_then(system_time_to_ns)?,
                    source_change_time_ns(&metadata)?
                );
                hasher.update(fingerprint.as_bytes());
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                hasher.update(b"absent;");
            }
            _ => return None,
        }
    }
    Some(hasher.finalize().to_hex().to_string())
}

pub(in crate::raw_mirror) fn raw_mirror_blob_cache_path(
    root: &Path,
    key: &RawMirrorBlobCacheKey,
) -> Option<PathBuf> {
    let source_key = serde_json::to_vec(&raw_mirror_blob_source_key(key)).ok()?;
    Some(
        root.join("capture-cache")
            .join(format!("{}.json", blake3::hash(&source_key).to_hex())),
    )
}

pub(in crate::raw_mirror) fn read_persisted_raw_mirror_blob_cache(
    root: &Path,
    key: &RawMirrorBlobCacheKey,
) -> Option<CachedRawMirrorBlobRecord> {
    let path = raw_mirror_blob_cache_path(root, key)?;
    if raw_mirror_path_has_symlink_below_root(root, &path) {
        return None;
    }
    let metadata = raw_mirror_symlink_metadata(&path).ok()?;
    if !metadata.is_file() || metadata.len() > RAW_MIRROR_MANIFEST_MAX_BYTES {
        return None;
    }
    let file = open_stable_source_file(&path, &metadata).ok()?;
    let mut bytes = Vec::new();
    raw_mirror_io("read_capture_cache", &path, || {
        file.take(RAW_MIRROR_MANIFEST_MAX_BYTES + 1)
            .read_to_end(&mut bytes)
    })
    .ok()?;
    if bytes.len() as u64 > RAW_MIRROR_MANIFEST_MAX_BYTES {
        return None;
    }
    let persisted: PersistedRawMirrorBlobCache = serde_json::from_slice(&bytes).ok()?;
    if persisted.version != 1 || persisted.entry.cache_key != *key {
        return None;
    }
    let entry_bytes = serde_json::to_vec(&persisted.entry).ok()?;
    if blake3::hash(&entry_bytes).to_hex().as_str() != persisted.entry_blake3 {
        return None;
    }
    Some(persisted.entry)
}

#[cfg(test)]
fn persist_raw_mirror_blob_cache(root: &Path, entry: &CachedRawMirrorBlobRecord) -> Result<()> {
    let path = raw_mirror_blob_cache_path(root, &entry.cache_key)
        .ok_or_else(|| anyhow!("raw mirror cache source path is not serializable"))?;
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("raw mirror cache has no parent"))?;
    ensure_private_dir_descendant(root, parent)?;
    if raw_mirror_path_has_symlink_below_root(root, &path) {
        return Err(anyhow!("raw mirror cache path contains a symlink"));
    }
    let entry_bytes = serde_json::to_vec(entry)?;
    let bytes = serde_json::to_vec(&PersistedRawMirrorBlobCache {
        version: 1,
        entry: entry.clone(),
        entry_blake3: blake3::hash(&entry_bytes).to_hex().to_string(),
    })?;
    if bytes.len() as u64 > RAW_MIRROR_MANIFEST_MAX_BYTES {
        return Err(anyhow!("raw mirror cache entry exceeds its size limit"));
    }
    let temporary = unique_temp_path(parent, "cache");
    let mut file = private_create_new_file(&temporary)?;
    file.write_all(&bytes)?;
    sync_open_file_if_required(&file, &temporary, || {
        "sync raw mirror capture cache".to_string()
    })?;
    drop(file);
    fs::rename(&temporary, &path)?;
    sync_parent(&path)?;
    Ok(())
}

#[cfg(all(test, unix))]
pub(in crate::raw_mirror) fn cached_raw_mirror_blob_record(
    key: &RawMirrorBlobCacheKey,
    root: &Path,
) -> Option<RawMirrorBlobRecord> {
    if !raw_mirror_blob_cache_key_is_strong(key) {
        return None;
    }
    let cache = BLOB_CAPTURE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let source_key = raw_mirror_blob_source_key(key);
    let cached = {
        let guard = cache.lock().ok()?;
        let cached = guard
            .get(&source_key)
            .filter(|entry| entry.cache_key == *key)
            .cloned();
        drop(guard);
        let cached = cached.or_else(|| read_persisted_raw_mirror_blob_cache(root, key))?;
        let mut guard = cache.lock().ok()?;
        if cached.cache_key != *key {
            guard.remove(&source_key);
            return None;
        }
        if raw_mirror_blob_relative_path(&cached.record.blob_blake3).is_none() {
            guard.remove(&source_key);
            return None;
        }
        cached
    };
    let record = cached.record.clone();

    if raw_mirror_stored_blob_fingerprints(root, &record)
        .is_some_and(|current| current == cached.stored_blob_fingerprints)
    {
        if !raw_mirror_blob_cache_source_is_current(key) {
            return None;
        }
        remember_cached_raw_mirror_blob_record(cache, cached);
        return Some(record);
    }

    match cached_raw_mirror_blob_record_is_verified(root, &record) {
        Ok(()) => {
            if !raw_mirror_blob_cache_source_is_current(key) {
                return None;
            }
            cache_raw_mirror_blob_record(key.clone(), record.clone());
            Some(record)
        }
        Err(err) => {
            let blob_relative_path = raw_mirror_blob_relative_path(&record.blob_blake3)?;
            let blob_path = root.join(blob_relative_path);
            tracing::warn!(
                path = %blob_path.display(),
                expected_blake3 = %record.blob_blake3,
                error = %err,
                "discarding raw mirror blob cache entry with mismatched content"
            );
            remove_cached_raw_mirror_blob_record_if_unchanged(cache, key, &record);
            None
        }
    }
}

pub(in crate::raw_mirror) fn raw_mirror_blob_cache_source_is_current(
    key: &RawMirrorBlobCacheKey,
) -> bool {
    raw_mirror_symlink_metadata(&key.source_path)
        .ok()
        .is_some_and(|metadata| {
            !metadata.file_type().is_symlink()
                && metadata.is_file()
                && source_identity_token(&metadata) == key.source_identity
                && metadata.len() == key.source_size_bytes
                && metadata.modified().ok().and_then(system_time_to_ns) == key.source_mtime_ns
                && source_change_time_ns(&metadata) == key.source_change_time_ns
                && raw_mirror_source_sidecars_fingerprint(&key.source_path)
                    == key.source_sidecars_fingerprint
        })
}

pub(in crate::raw_mirror) fn raw_mirror_stored_blob_fingerprints(
    root: &Path,
    record: &RawMirrorBlobRecord,
) -> Option<Vec<RawMirrorStoredBlobFingerprint>> {
    let mut references = vec![RawMirrorChunkRef {
        blob_relative_path: raw_mirror_blob_relative_path(&record.blob_blake3)?,
        blob_blake3: record.blob_blake3.clone(),
        blob_size_bytes: record.blob_size_bytes,
    }];
    if let Some(storage) = &record.content_storage {
        references.extend(storage.chunks.iter().cloned());
    }

    let mut seen = HashSet::new();
    let mut fingerprints = Vec::with_capacity(references.len());
    for reference in references {
        if raw_mirror_blob_relative_path(&reference.blob_blake3).as_deref()
            != Some(reference.blob_relative_path.as_str())
        {
            return None;
        }
        if !seen.insert(reference.blob_relative_path.clone()) {
            continue;
        }
        let path = root.join(&reference.blob_relative_path);
        if raw_mirror_path_has_symlink_below_root(root, &path) {
            return None;
        }
        let metadata = raw_mirror_symlink_metadata(&path).ok()?;
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || metadata.len() != reference.blob_size_bytes
        {
            return None;
        }
        fingerprints.push(RawMirrorStoredBlobFingerprint {
            blob_relative_path: reference.blob_relative_path,
            file_identity: source_identity_token(&metadata)?,
            size_bytes: metadata.len(),
            modified_ns: metadata.modified().ok().and_then(system_time_to_ns)?,
            change_time_ns: source_change_time_ns(&metadata)?,
        });
    }
    Some(fingerprints)
}

pub(in crate::raw_mirror) fn cached_raw_mirror_blob_record_is_verified(
    root: &Path,
    record: &RawMirrorBlobRecord,
) -> Result<()> {
    let descriptor = RawMirrorChunkRef {
        blob_relative_path: raw_mirror_blob_relative_path(&record.blob_blake3)
            .ok_or_else(|| anyhow!("cached raw mirror blob has an invalid digest"))?,
        blob_blake3: record.blob_blake3.clone(),
        blob_size_bytes: record.blob_size_bytes,
    };
    verify_existing_blob_reference(root, &descriptor)?;
    let mut verified_blobs = HashSet::from([descriptor.blob_blake3]);
    if let Some(storage) = &record.content_storage {
        for chunk in &storage.chunks {
            if verified_blobs.insert(chunk.blob_blake3.clone()) {
                verify_existing_blob_reference(root, chunk)?;
            }
        }
    }
    Ok(())
}

pub(in crate::raw_mirror) fn remove_cached_raw_mirror_blob_record_if_unchanged(
    cache: &Mutex<HashMap<RawMirrorBlobSourceKey, CachedRawMirrorBlobRecord>>,
    key: &RawMirrorBlobCacheKey,
    stale_record: &RawMirrorBlobRecord,
) {
    let source_key = raw_mirror_blob_source_key(key);
    if let Ok(mut guard) = cache.lock()
        && guard
            .get(&source_key)
            .is_some_and(|current| current.cache_key == *key && current.record == *stale_record)
    {
        guard.remove(&source_key);
    }
}

#[cfg(test)]
pub(in crate::raw_mirror) fn cache_raw_mirror_blob_record(
    key: RawMirrorBlobCacheKey,
    record: RawMirrorBlobRecord,
) {
    if !raw_mirror_blob_cache_key_is_strong(&key) {
        return;
    }
    let root = raw_mirror_root(&key.data_dir);
    let Some(stored_blob_fingerprints) = raw_mirror_stored_blob_fingerprints(&root, &record) else {
        return;
    };
    let cached = CachedRawMirrorBlobRecord {
        cache_key: key.clone(),
        record,
        stored_blob_fingerprints,
    };
    if let Err(error) = persist_raw_mirror_blob_cache(&root, &cached) {
        tracing::debug!(%error, "raw mirror capture cache was not persisted; capture remains valid");
    }
    let cache = BLOB_CAPTURE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    remember_cached_raw_mirror_blob_record(cache, cached);
}

pub(in crate::raw_mirror) fn remember_cached_raw_mirror_blob_record(
    cache: &Mutex<HashMap<RawMirrorBlobSourceKey, CachedRawMirrorBlobRecord>>,
    cached: CachedRawMirrorBlobRecord,
) {
    if let Ok(mut guard) = cache.lock() {
        let source_key = raw_mirror_blob_source_key(&cached.cache_key);
        if !guard.contains_key(&source_key)
            && guard.len() >= RAW_MIRROR_BLOB_CACHE_MAX_ENTRIES
            && let Some(evicted) = guard.keys().next().cloned()
        {
            guard.remove(&evicted);
        }
        guard.insert(source_key, cached);
    }
}

pub(in crate::raw_mirror) fn raw_mirror_blob_cache_key_is_strong(
    key: &RawMirrorBlobCacheKey,
) -> bool {
    key.source_identity.is_some()
        && key.source_mtime_ns.is_some()
        && key.source_change_time_ns.is_some()
        && key.source_sidecars_fingerprint.is_some()
}

pub(in crate::raw_mirror) fn raw_mirror_blob_source_key(
    key: &RawMirrorBlobCacheKey,
) -> RawMirrorBlobSourceKey {
    RawMirrorBlobSourceKey {
        data_dir: key.data_dir.clone(),
        source_path: key.source_path.clone(),
    }
}
