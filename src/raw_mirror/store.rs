pub(super) use anyhow::{Context, Result, anyhow};
pub(super) use serde::Serialize;
pub(super) use std::collections::{BTreeMap, HashMap, HashSet};
pub(super) use std::fs::{self, File};
pub(super) use std::io::{Read, Write};
pub(super) use std::path::{Path, PathBuf};
pub(super) use std::sync::{Mutex, OnceLock};
pub(super) use std::time::Duration;

#[cfg(test)]
pub(super) use serde_json::{Value, json};
#[cfg(test)]
pub(super) use std::fs::OpenOptions;
#[cfg(test)]
pub(super) use std::sync::Arc;
#[cfg(test)]
pub(super) use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(test)]
pub(super) use std::time::SystemTime;

#[path = "store/cache.rs"]
mod cache;
#[path = "store/io.rs"]
mod io;
#[path = "store/manifest.rs"]
mod manifest;
#[path = "store/prune.rs"]
mod prune;
#[path = "store/summary.rs"]
mod summary;
#[path = "store/types.rs"]
mod types;

#[cfg(test)]
#[path = "store/tests.rs"]
mod tests;

#[cfg(test)]
pub use manifest::capture_source_file;
pub use manifest::merge_manifest_db_links;
pub use prune::prune;
pub use summary::storage_summary;
pub use types::{
    RawMirrorCaptureInput, RawMirrorCaptureRecord, RawMirrorDbLink, RawMirrorPruneEntry,
    RawMirrorPruneOptions, RawMirrorPruneReport, RawMirrorStorageSummary,
};

pub(crate) use io::{RawMirrorIoScope, observe_raw_mirror_io, raw_mirror_io_snapshot};
#[cfg(test)]
pub(crate) use manifest::capture_source_file_with_chunk_policy;
pub(crate) use manifest::{
    read_source_bytes, verify_source_capture, verify_source_capture_observed,
};
pub(crate) use summary::physical_storage_bytes;

#[cfg(test)]
pub(super) use cache::cache_raw_mirror_blob_record;
#[cfg(all(test, unix))]
pub(super) use cache::cached_raw_mirror_blob_record;
pub(super) use cache::{
    cached_raw_mirror_blob_record_is_verified, raw_mirror_blob_cache_key,
    raw_mirror_blob_cache_key_is_strong, raw_mirror_blob_cache_path,
    raw_mirror_blob_cache_source_is_current, raw_mirror_blob_source_key,
    raw_mirror_stored_blob_fingerprints, read_persisted_raw_mirror_blob_cache,
    remember_cached_raw_mirror_blob_record, remove_cached_raw_mirror_blob_record_if_unchanged,
};
pub(super) use io::{
    BLOB_CAPTURE_CACHE, RAW_MIRROR_CHUNK_SIZE_BYTES, RAW_MIRROR_CHUNK_THRESHOLD_BYTES,
    RAW_MIRROR_FIXED_CHUNKS_KIND, RAW_MIRROR_HASH_ALGORITHM, RAW_MIRROR_MANIFEST_KIND,
    RAW_MIRROR_MANIFEST_MAX_BYTES, RAW_MIRROR_SCHEMA_VERSION, acquire_raw_mirror_mutation_lock,
    ensure_private_dir_descendant, ensure_raw_mirror_root, now_ms, private_create_new_file,
    raw_mirror_checkpoint, raw_mirror_io, raw_mirror_path_has_symlink_below_root, raw_mirror_root,
    raw_mirror_symlink_metadata, raw_mirror_tracked_io, redacted_original_path,
    remove_empty_temp_dir_best_effort, remove_temp_best_effort, sync_open_file_if_required,
    sync_parent, system_time_to_ms, try_acquire_index_run_lock_for_prune, unique_capture_temp_dir,
    unique_temp_path,
};
#[cfg(test)]
pub(super) use io::{
    RAW_MIRROR_BLOB_CACHE_MAX_ENTRIES, RAW_MIRROR_MUTATION_LOCK_FILE, RAW_MIRROR_ROOT_DIR,
    RAW_MIRROR_VERSION_DIR, file_blake3, sync_parent_unconditionally,
};
pub(super) use manifest::{
    content_addressed_blob_is_verified, merge_raw_mirror_manifest_db_links,
    open_stable_source_file, publish_content_addressed_temp, publish_manifest_bytes_create_new,
    raw_mirror_blob_relative_path, raw_mirror_manifest_blake3, raw_mirror_manifest_id,
    raw_mirror_manifest_relative_path, raw_mirror_original_path_blake3,
    read_validated_raw_mirror_manifest, replace_raw_mirror_bytes,
    source_file_changed_during_capture, source_path_changed_identity_during_capture,
    unique_db_links,
};
#[cfg(test)]
pub(super) use manifest::{
    prepare_source_content, raw_mirror_manifest_blob_references, read_raw_mirror_manifest,
    read_raw_mirror_manifest_from_open_file, same_source_identity, verify_existing_blob_reference,
};
#[cfg(test)]
pub(super) use prune::remove_prune_entry_file;
pub(super) use prune::{
    apply_prune_entries, collect_prune_manifests, collect_raw_mirror_physical_blobs,
    open_prune_audit_log, pinned_prune_manifest_ids, validate_prune_target_file,
};
pub(super) use types::{
    CachedRawMirrorBlobRecord, CopyToTempResult, PersistedRawMirrorBlobCache,
    PreparedRawMirrorContent, RawMirrorBlobCacheKey, RawMirrorBlobRecord, RawMirrorChunkRef,
    RawMirrorCompressionEnvelope, RawMirrorContentStorage, RawMirrorEncryptionEnvelope,
    RawMirrorManifestFile, RawMirrorVerificationRecord,
};
