use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct RawMirrorCaptureInput<'a> {
    pub data_dir: &'a Path,
    pub provider: &'a str,
    pub source_id: &'a str,
    pub origin_kind: &'a str,
    pub origin_host: Option<&'a str>,
    pub source_path: &'a Path,
    pub db_links: &'a [RawMirrorDbLink],
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RawMirrorCaptureRecord {
    pub manifest_id: String,
    pub manifest_relative_path: String,
    pub blob_relative_path: String,
    pub blob_blake3: String,
    pub blob_size_bytes: u64,
    pub source_content_blake3: String,
    pub source_size_bytes: u64,
    pub storage_kind: String,
    pub chunk_count: usize,
    pub captured_at_ms: i64,
    pub source_mtime_ms: Option<i64>,
    pub already_present: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawMirrorVerifiedCapture {
    pub storage_kind: String,
    pub source_content_blake3: String,
    pub source_size_bytes: u64,
    pub stored_blob_count: usize,
    pub stored_bytes: u64,
    pub stored_blobs: Vec<(String, u64)>,
    pub chunk_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RawMirrorDbLink {
    pub conversation_id: Option<i64>,
    pub message_count: Option<usize>,
    pub source_path: Option<String>,
    pub started_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RawMirrorStorageSummary {
    pub initialized: bool,
    pub root_path: String,
    pub total_storage_bytes: u64,
    pub manifest_count: u64,
    pub manifest_bytes: u64,
    pub unique_blob_count: u64,
    pub total_blob_bytes: u64,
    pub largest_blob_bytes: u64,
    pub orphan_blob_count: u64,
    pub orphan_blob_bytes: u64,
    pub missing_blob_count: u64,
    pub invalid_manifest_count: u64,
    pub oldest_capture_at_ms: Option<i64>,
    pub newest_capture_at_ms: Option<i64>,
    pub oldest_source_mtime_ms: Option<i64>,
    pub newest_source_mtime_ms: Option<i64>,
}

#[derive(Debug, Clone, Default)]
pub struct RawMirrorPruneOptions {
    pub older_than_ms: Option<i64>,
    pub max_size_bytes: Option<u64>,
    pub providers: Vec<String>,
    pub source_path: Option<String>,
    pub keep_tags: Vec<String>,
    /// Retire every capture of a source file except its newest N. Those N
    /// are also out of reach of the age and size rules.
    pub keep_latest: Option<usize>,
    pub safety_hold_down_ms: i64,
    pub apply: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RawMirrorPruneReport {
    pub initialized: bool,
    pub root_path: String,
    pub mode: String,
    pub manifest_count: u64,
    pub unique_blob_count: u64,
    pub current_blob_bytes: u64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub providers: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope_blob_bytes: Option<u64>,
    pub orphan_blob_count: u64,
    pub orphan_blob_bytes: u64,
    pub safety_hold_down_ms: i64,
    pub keep_tags: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keep_latest: Option<usize>,
    pub pinned_manifest_count: u64,
    pub pinned_blob_count: u64,
    pub planned_manifest_count: u64,
    pub planned_blob_count: u64,
    pub planned_reclaim_bytes: u64,
    /// `tmp/capture.*` staging left by captures that were killed or failed
    /// before publishing; reclaimed only by an unscoped prune.
    pub planned_staging_count: u64,
    pub applied_manifest_count: u64,
    pub applied_blob_count: u64,
    pub applied_staging_count: u64,
    pub applied_reclaim_bytes: u64,
    pub audit_log_path: Option<String>,
    /// Preview details omitted from `entries`; aggregate counts still cover the full plan.
    pub omitted_entry_count: u64,
    pub entries: Vec<RawMirrorPruneEntry>,
}

pub(in crate::raw_mirror) const PRUNE_PREVIEW_ENTRY_LIMIT: usize = 1_000;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RawMirrorPruneEntry {
    pub kind: String,
    pub path: String,
    pub blob_blake3: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest_blake3: Option<String>,
    pub size_bytes: u64,
    pub reason: String,
    pub applied: bool,
}

#[derive(Debug, Clone)]
pub(in crate::raw_mirror) struct RawMirrorPruneManifest {
    pub(in crate::raw_mirror) manifest_id: String,
    pub(in crate::raw_mirror) manifest_blake3: String,
    pub(in crate::raw_mirror) blob_blake3: String,
    pub(in crate::raw_mirror) relative_path: String,
    pub(in crate::raw_mirror) size_bytes: u64,
    pub(in crate::raw_mirror) blob_references: Vec<RawMirrorChunkRef>,
    pub(in crate::raw_mirror) captured_at_ms: i64,
    pub(in crate::raw_mirror) provider: String,
    pub(in crate::raw_mirror) source_id: String,
    pub(in crate::raw_mirror) origin_kind: String,
    pub(in crate::raw_mirror) origin_host: Option<String>,
    pub(in crate::raw_mirror) original_path: String,
    pub(in crate::raw_mirror) db_links: Vec<RawMirrorDbLink>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::raw_mirror) struct RawMirrorPhysicalBlob {
    pub(in crate::raw_mirror) relative_path: String,
    pub(in crate::raw_mirror) blob_blake3: String,
    pub(in crate::raw_mirror) size_bytes: u64,
    pub(in crate::raw_mirror) modified_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(in crate::raw_mirror) struct RawMirrorBlobCacheKey {
    pub(in crate::raw_mirror) data_dir: PathBuf,
    pub(in crate::raw_mirror) source_path: PathBuf,
    pub(in crate::raw_mirror) source_identity: Option<String>,
    pub(in crate::raw_mirror) source_size_bytes: u64,
    pub(in crate::raw_mirror) source_mtime_ns: Option<u128>,
    pub(in crate::raw_mirror) source_change_time_ns: Option<u128>,
    pub(in crate::raw_mirror) source_sidecars_fingerprint: Option<String>,
    pub(in crate::raw_mirror) chunk_threshold_bytes: u64,
    pub(in crate::raw_mirror) chunk_size_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(in crate::raw_mirror) struct RawMirrorBlobSourceKey {
    pub(in crate::raw_mirror) data_dir: PathBuf,
    pub(in crate::raw_mirror) source_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(in crate::raw_mirror) struct CachedRawMirrorBlobRecord {
    pub(in crate::raw_mirror) cache_key: RawMirrorBlobCacheKey,
    pub(in crate::raw_mirror) record: RawMirrorBlobRecord,
    pub(in crate::raw_mirror) stored_blob_fingerprints: Vec<RawMirrorStoredBlobFingerprint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(in crate::raw_mirror) struct RawMirrorStoredBlobFingerprint {
    pub(in crate::raw_mirror) blob_relative_path: String,
    pub(in crate::raw_mirror) file_identity: String,
    pub(in crate::raw_mirror) size_bytes: u64,
    pub(in crate::raw_mirror) modified_ns: u128,
    pub(in crate::raw_mirror) change_time_ns: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(in crate::raw_mirror) struct RawMirrorBlobRecord {
    pub(in crate::raw_mirror) blob_blake3: String,
    pub(in crate::raw_mirror) blob_size_bytes: u64,
    pub(in crate::raw_mirror) source_content_blake3: String,
    pub(in crate::raw_mirror) source_size_bytes: u64,
    pub(in crate::raw_mirror) content_storage: Option<RawMirrorContentStorage>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(in crate::raw_mirror) struct PersistedRawMirrorBlobCache {
    pub(in crate::raw_mirror) version: u32,
    pub(in crate::raw_mirror) entry: CachedRawMirrorBlobRecord,
    pub(in crate::raw_mirror) entry_blake3: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(in crate::raw_mirror) struct RawMirrorChunkRef {
    pub(in crate::raw_mirror) blob_relative_path: String,
    pub(in crate::raw_mirror) blob_blake3: String,
    pub(in crate::raw_mirror) blob_size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(in crate::raw_mirror) struct RawMirrorContentStorage {
    pub(in crate::raw_mirror) kind: String,
    pub(in crate::raw_mirror) content_hash_algorithm: String,
    pub(in crate::raw_mirror) content_blake3: String,
    pub(in crate::raw_mirror) content_size_bytes: u64,
    pub(in crate::raw_mirror) chunk_size_bytes: u64,
    pub(in crate::raw_mirror) chunks: Vec<RawMirrorChunkRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(in crate::raw_mirror) struct RawMirrorCompressionEnvelope {
    pub(in crate::raw_mirror) state: String,
    pub(in crate::raw_mirror) algorithm: Option<String>,
    pub(in crate::raw_mirror) uncompressed_size_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(in crate::raw_mirror) struct RawMirrorEncryptionEnvelope {
    pub(in crate::raw_mirror) state: String,
    pub(in crate::raw_mirror) algorithm: Option<String>,
    pub(in crate::raw_mirror) key_id: Option<String>,
    pub(in crate::raw_mirror) envelope_version: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(in crate::raw_mirror) struct RawMirrorVerificationRecord {
    pub(in crate::raw_mirror) status: String,
    pub(in crate::raw_mirror) verifier: String,
    pub(in crate::raw_mirror) content_blake3: Option<String>,
    pub(in crate::raw_mirror) verified_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(in crate::raw_mirror) struct RawMirrorManifestFile {
    pub(in crate::raw_mirror) schema_version: u32,
    pub(in crate::raw_mirror) manifest_kind: String,
    pub(in crate::raw_mirror) manifest_id: String,
    pub(in crate::raw_mirror) blob_hash_algorithm: String,
    pub(in crate::raw_mirror) blob_relative_path: String,
    pub(in crate::raw_mirror) blob_blake3: String,
    pub(in crate::raw_mirror) blob_size_bytes: u64,
    pub(in crate::raw_mirror) provider: String,
    pub(in crate::raw_mirror) source_id: String,
    pub(in crate::raw_mirror) origin_kind: String,
    pub(in crate::raw_mirror) origin_host: Option<String>,
    pub(in crate::raw_mirror) original_path: String,
    pub(in crate::raw_mirror) redacted_original_path: String,
    pub(in crate::raw_mirror) original_path_blake3: String,
    pub(in crate::raw_mirror) captured_at_ms: i64,
    pub(in crate::raw_mirror) source_mtime_ms: Option<i64>,
    pub(in crate::raw_mirror) source_size_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::raw_mirror) content_storage: Option<RawMirrorContentStorage>,
    pub(in crate::raw_mirror) compression: RawMirrorCompressionEnvelope,
    pub(in crate::raw_mirror) encryption: RawMirrorEncryptionEnvelope,
    pub(in crate::raw_mirror) db_links: Vec<RawMirrorDbLink>,
    pub(in crate::raw_mirror) verification: RawMirrorVerificationRecord,
    pub(in crate::raw_mirror) manifest_blake3: Option<String>,
}

pub(in crate::raw_mirror) struct CopyToTempResult {
    pub(in crate::raw_mirror) temp_path: PathBuf,
    pub(in crate::raw_mirror) blob_blake3: String,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "read only by chunk-preparation unit assertions")
    )]
    pub(in crate::raw_mirror) bytes_copied: u64,
}

pub(in crate::raw_mirror) struct PreparedRawMirrorContent {
    pub(in crate::raw_mirror) record: RawMirrorBlobRecord,
    pub(in crate::raw_mirror) files: Vec<CopyToTempResult>,
}
