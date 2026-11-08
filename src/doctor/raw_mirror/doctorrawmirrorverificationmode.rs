use crate::cli::commands::view::role_to_export_string::{
    normalized_provenance_origin_kind, normalized_provenance_source_id,
};
use crate::doctor::forensic::{doctor_canonical_blake3, doctor_redacted_path};
use crate::doctor::fs_safety::doctor_copy_source_path_is_safe::{
    existing_path_has_symlink_below_root, path_has_symlink_below_root,
};
use crate::doctor::policy::DoctorArtifactChecksumStatus;
use crate::doctor::raw_mirror::collect_doctor_raw_mirror_report_with_thresholds_and::collect_doctor_raw_mirror_report_with_thresholds_and_mode;
use crate::doctor::raw_mirror::doctor_raw_mirror_root_dir::{
    CASS_DOCTOR_RAW_MIRROR_FULL_VERIFY, CASS_DOCTOR_RAW_MIRROR_FULL_VERIFY_BYTE_LIMIT,
    CASS_DOCTOR_RAW_MIRROR_FULL_VERIFY_MANIFEST_LIMIT, DOCTOR_RAW_MIRROR_BLOB_EXTENSION,
    DOCTOR_RAW_MIRROR_DEFAULT_FULL_VERIFY_BYTE_LIMIT,
    DOCTOR_RAW_MIRROR_DEFAULT_FULL_VERIFY_MANIFEST_LIMIT, DOCTOR_RAW_MIRROR_DIR_MODE,
    DOCTOR_RAW_MIRROR_FILE_MODE, DOCTOR_RAW_MIRROR_HASH_ALGORITHM, DOCTOR_RAW_MIRROR_MANIFEST_KIND,
    DOCTOR_RAW_MIRROR_ROOT_DIR, DOCTOR_RAW_MIRROR_SCHEMA_VERSION, DOCTOR_RAW_MIRROR_VERSION_DIR,
};
use crate::doctor::raw_mirror::query_doctor_raw_mirror_backfill_candidates::doctor_raw_mirror_manifest_is_verified;
use crate::doctor::repair_state::types::{
    CASS_RAW_MIRROR_SIZE_WARN_THRESHOLD_BYTES, DOCTOR_RAW_MIRROR_AMPLIFICATION_MANIFEST_MAX_BYTES,
    DOCTOR_RAW_MIRROR_BOUNDED_AMPLIFICATION_BYTE_LIMIT,
    DOCTOR_RAW_MIRROR_BOUNDED_AMPLIFICATION_MANIFEST_LIMIT, DOCTOR_RAW_MIRROR_MANIFEST_MAX_BYTES,
    DOCTOR_RAW_MIRROR_SIZE_WARN_THRESHOLD_BYTES_DEFAULT,
    DOCTOR_RAW_MIRROR_SOURCE_AMPLIFICATION_WARN_EXCESS_BYTES_DEFAULT,
    DOCTOR_RAW_MIRROR_SOURCE_AMPLIFICATION_WARN_RATIO_MILLI_DEFAULT,
};
use crate::doctor::report::recommendations::doctor_normalized_provider_slug;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DoctorRawMirrorVerificationMode {
    Full,
    Bounded {
        manifest_limit: usize,
        byte_limit: u64,
    },
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorRawMirrorReport {
    pub(crate) schema_version: u32,
    pub(crate) status: String,
    #[serde(skip_serializing)]
    #[allow(dead_code)]
    pub(crate) root_path: String,
    pub(crate) redacted_root_path: String,
    pub(crate) exists: bool,
    pub(crate) sensitive_paths_included: bool,
    pub(crate) raw_content_included: bool,
    pub(crate) layout: DoctorRawMirrorLayoutReport,
    pub(crate) policy: DoctorRawMirrorPolicyReport,
    pub(crate) summary: DoctorRawMirrorSummary,
    pub(crate) manifests: Vec<DoctorRawMirrorManifestReport>,
    pub(crate) amplified_sources: Vec<DoctorRawMirrorSourceAmplificationReport>,
    pub(crate) warnings: Vec<String>,
    pub(crate) notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorRawMirrorLayoutReport {
    pub(crate) root_relative_path: String,
    pub(crate) manifest_kind: &'static str,
    pub(crate) hash_algorithm: &'static str,
    pub(crate) blob_path_template: String,
    pub(crate) manifest_path_template: String,
    pub(crate) verification_path_template: String,
    pub(crate) temp_path_template: String,
    pub(crate) content_address_scope: String,
    pub(crate) source_identity_scope: String,
    pub(crate) db_link_contract: String,
    pub(crate) case_insensitive_collision_behavior: String,
    pub(crate) migration_contract: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorRawMirrorPolicyReport {
    pub(crate) append_only: bool,
    pub(crate) global_dedup_by_content_hash: bool,
    pub(crate) never_overwrite_different_bytes: bool,
    pub(crate) storage_scope: String,
    pub(crate) local_only_by_default: bool,
    pub(crate) directory_mode_octal: &'static str,
    pub(crate) file_mode_octal: &'static str,
    pub(crate) enforce_private_files: bool,
    pub(crate) atomic_publish: String,
    pub(crate) fsync_required: bool,
    pub(crate) path_traversal_defense: String,
    pub(crate) symlink_defense: String,
    pub(crate) default_report_contract: String,
    pub(crate) sensitive_output_policy: DoctorRawMirrorSensitiveOutputPolicyReport,
    pub(crate) compression_policy: DoctorRawMirrorCodecPolicyReport,
    pub(crate) encryption_policy: DoctorRawMirrorEncryptionPolicyReport,
    pub(crate) backup_policy: DoctorRawMirrorBackupPolicyReport,
    pub(crate) support_bundle_policy: DoctorRawMirrorSupportBundlePolicyReport,
    pub(crate) public_export_policy: DoctorRawMirrorPublicExportPolicyReport,
    pub(crate) compression_contract: String,
    pub(crate) encryption_contract: String,
    pub(crate) support_bundle_redaction_contract: String,
    pub(crate) missing_upstream_semantics: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorRawMirrorSensitiveOutputPolicyReport {
    pub(crate) default_includes_exact_paths: bool,
    pub(crate) default_includes_raw_content: bool,
    pub(crate) safe_metadata_fields: Vec<&'static str>,
    pub(crate) redacted_by_default_fields: Vec<&'static str>,
    pub(crate) opt_in_sensitive_modes: Vec<&'static str>,
    pub(crate) redaction_marker_contract: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorRawMirrorCodecPolicyReport {
    pub(crate) default_state: &'static str,
    pub(crate) optional: bool,
    pub(crate) allowed_states: Vec<&'static str>,
    pub(crate) required_metadata_fields: Vec<&'static str>,
    pub(crate) raw_content_hash_scope: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorRawMirrorEncryptionPolicyReport {
    pub(crate) default_state: &'static str,
    pub(crate) optional: bool,
    pub(crate) allowed_states: Vec<&'static str>,
    pub(crate) required_metadata_fields: Vec<&'static str>,
    pub(crate) key_material_reporting: &'static str,
    pub(crate) integrity_contract: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorRawMirrorBackupPolicyReport {
    pub(crate) included_in_default_backup: bool,
    pub(crate) backup_mode: &'static str,
    pub(crate) backup_contents: Vec<&'static str>,
    pub(crate) restore_precondition: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorRawMirrorSupportBundlePolicyReport {
    pub(crate) default_mode: &'static str,
    pub(crate) include_manifest_metadata: bool,
    pub(crate) include_blob_bytes: bool,
    pub(crate) include_exact_paths: bool,
    pub(crate) sensitive_attachment_gate: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorRawMirrorPublicExportPolicyReport {
    pub(crate) pages_exports_include_raw_mirror: bool,
    pub(crate) html_exports_include_raw_mirror: bool,
    pub(crate) default_logs_include_raw_content: bool,
    pub(crate) default_robot_json_includes_raw_content: bool,
    pub(crate) public_artifact_contract: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorRawMirrorSummary {
    pub(crate) manifest_count: usize,
    pub(crate) verified_blob_count: usize,
    pub(crate) missing_blob_count: usize,
    pub(crate) checksum_mismatch_count: usize,
    pub(crate) manifest_checksum_mismatch_count: usize,
    pub(crate) manifest_checksum_not_recorded_count: usize,
    pub(crate) invalid_manifest_count: usize,
    pub(crate) interrupted_capture_count: usize,
    pub(crate) duplicate_blob_reference_count: usize,
    pub(crate) total_blob_bytes: u64,
    pub(crate) orphan_blob_count: u64,
    pub(crate) orphan_blob_bytes: u64,
    pub(crate) amplified_source_count: usize,
    pub(crate) amplified_source_referenced_bytes: u64,
    pub(crate) amplified_source_excess_bytes: u64,
    pub(crate) max_source_amplification_ratio_milli: Option<u64>,
    /// ub29t (gh#374 issue 4): how many manifests the `manifests` array
    /// actually carries. Equal to `manifest_count` under `--verbose` or when
    /// the archive is small; smaller when healthy entries were elided.
    pub(crate) listed_manifest_count: usize,
    /// Healthy (`verified`, upstream present, no invalid reason) manifests
    /// elided from `manifests` by the bounded default listing. Problem
    /// manifests are never elided.
    pub(crate) omitted_healthy_manifest_count: usize,
    /// `"full"` or `"bounded"`. Surfaces that never apply the bound
    /// (baseline snapshots, archive scans) report `"full"`.
    pub(crate) listing_mode: &'static str,
}

impl Default for DoctorRawMirrorSummary {
    fn default() -> Self {
        Self {
            manifest_count: 0,
            verified_blob_count: 0,
            missing_blob_count: 0,
            checksum_mismatch_count: 0,
            manifest_checksum_mismatch_count: 0,
            manifest_checksum_not_recorded_count: 0,
            invalid_manifest_count: 0,
            interrupted_capture_count: 0,
            duplicate_blob_reference_count: 0,
            total_blob_bytes: 0,
            orphan_blob_count: 0,
            orphan_blob_bytes: 0,
            amplified_source_count: 0,
            amplified_source_referenced_bytes: 0,
            amplified_source_excess_bytes: 0,
            max_source_amplification_ratio_milli: None,
            listed_manifest_count: 0,
            omitted_healthy_manifest_count: 0,
            listing_mode: "full",
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct DoctorRawMirrorSourceAmplificationReport {
    pub(crate) evidence_kind: &'static str,
    pub(crate) providers: Vec<String>,
    pub(crate) source_id: String,
    pub(crate) redacted_original_path: String,
    pub(crate) original_path_blake3: String,
    pub(crate) version_count: usize,
    pub(crate) referenced_blob_bytes: u64,
    pub(crate) current_source_size_bytes: u64,
    pub(crate) excess_bytes: u64,
    pub(crate) amplification_ratio_milli: Option<u64>,
}

#[derive(Debug, Clone)]
pub(crate) struct DoctorRawMirrorSourceAmplificationVersion {
    pub(crate) provider: String,
    pub(crate) source_id: String,
    pub(crate) origin_kind: String,
    pub(crate) origin_host: Option<String>,
    pub(crate) original_path: String,
    pub(crate) redacted_original_path: String,
    pub(crate) original_path_blake3: String,
    pub(crate) snapshot_blake3: String,
    pub(crate) stored_blobs: Vec<(String, u64)>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorRawMirrorManifestReport {
    pub(crate) manifest_id: String,
    #[serde(skip_serializing)]
    pub(crate) manifest_path: String,
    pub(crate) redacted_manifest_path: String,
    pub(crate) blob_relative_path: String,
    #[serde(skip_serializing)]
    #[allow(dead_code)]
    pub(crate) blob_path: String,
    pub(crate) redacted_blob_path: String,
    pub(crate) blob_blake3: String,
    pub(crate) blob_size_bytes: u64,
    pub(crate) storage_kind: String,
    pub(crate) source_content_blake3: String,
    pub(crate) chunk_count: usize,
    #[serde(skip_serializing)]
    pub(crate) verified_stored_blobs: Vec<(String, u64)>,
    pub(crate) provider: String,
    pub(crate) source_id: String,
    pub(crate) origin_kind: String,
    pub(crate) origin_host: Option<String>,
    #[serde(skip_serializing)]
    #[allow(dead_code)]
    pub(crate) original_path: String,
    pub(crate) redacted_original_path: String,
    pub(crate) original_path_blake3: String,
    pub(crate) captured_at_ms: i64,
    pub(crate) source_mtime_ms: Option<i64>,
    pub(crate) source_size_bytes: Option<u64>,
    pub(crate) compression_state: String,
    pub(crate) encryption_state: String,
    pub(crate) compression: DoctorRawMirrorCompressionEnvelope,
    pub(crate) encryption: DoctorRawMirrorEncryptionEnvelope,
    pub(crate) db_link_count: usize,
    #[serde(skip_serializing)]
    pub(crate) db_links: Vec<DoctorRawMirrorDbLink>,
    pub(crate) upstream_path_exists: Option<bool>,
    pub(crate) status: String,
    pub(crate) blob_checksum_status: DoctorArtifactChecksumStatus,
    pub(crate) manifest_checksum_status: DoctorArtifactChecksumStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) invalid_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq, Hash)]
pub(crate) struct DoctorRawMirrorDbLink {
    #[serde(default)]
    pub(crate) conversation_id: Option<i64>,
    #[serde(default)]
    pub(crate) message_count: Option<u64>,
    #[serde(default)]
    pub(crate) source_path: Option<String>,
    #[serde(default)]
    pub(crate) started_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct DoctorRawMirrorCompressionEnvelope {
    #[serde(default = "doctor_raw_mirror_none_state")]
    pub(crate) state: String,
    #[serde(default)]
    pub(crate) algorithm: Option<String>,
    #[serde(default)]
    pub(crate) uncompressed_size_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct DoctorRawMirrorEncryptionEnvelope {
    #[serde(default = "doctor_raw_mirror_none_state")]
    pub(crate) state: String,
    #[serde(default)]
    pub(crate) algorithm: Option<String>,
    #[serde(default)]
    pub(crate) key_id: Option<String>,
    #[serde(default)]
    pub(crate) envelope_version: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct DoctorRawMirrorVerificationRecord {
    #[serde(default)]
    pub(crate) status: String,
    #[serde(default)]
    pub(crate) verifier: String,
    #[serde(default)]
    pub(crate) content_blake3: Option<String>,
    #[serde(default)]
    pub(crate) verified_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct DoctorRawMirrorManifestFile {
    #[serde(default)]
    pub(crate) schema_version: u32,
    #[serde(default)]
    pub(crate) manifest_kind: String,
    #[serde(default)]
    pub(crate) manifest_id: String,
    #[serde(default)]
    pub(crate) blob_hash_algorithm: String,
    #[serde(default)]
    pub(crate) blob_blake3: String,
    #[serde(default)]
    pub(crate) blob_relative_path: String,
    #[serde(default)]
    pub(crate) blob_size_bytes: u64,
    #[serde(default)]
    pub(crate) provider: String,
    #[serde(default)]
    pub(crate) source_id: String,
    #[serde(default)]
    pub(crate) origin_kind: String,
    #[serde(default)]
    pub(crate) origin_host: Option<String>,
    #[serde(default)]
    pub(crate) original_path: String,
    #[serde(default)]
    pub(crate) redacted_original_path: String,
    #[serde(default)]
    pub(crate) original_path_blake3: String,
    #[serde(default)]
    pub(crate) captured_at_ms: i64,
    #[serde(default)]
    pub(crate) source_mtime_ms: Option<i64>,
    #[serde(default)]
    pub(crate) source_size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) content_storage: Option<serde_json::Value>,
    #[serde(default)]
    pub(crate) compression: DoctorRawMirrorCompressionEnvelope,
    #[serde(default)]
    pub(crate) encryption: DoctorRawMirrorEncryptionEnvelope,
    #[serde(default)]
    pub(crate) db_links: Vec<DoctorRawMirrorDbLink>,
    #[serde(default)]
    pub(crate) verification: DoctorRawMirrorVerificationRecord,
    #[serde(default)]
    pub(crate) manifest_blake3: Option<String>,
}

pub(crate) fn doctor_raw_mirror_storage_metadata(
    manifest: &DoctorRawMirrorManifestFile,
) -> (String, String, usize) {
    let Some(storage) = manifest.content_storage.as_ref() else {
        return (
            "whole_blob_v1".to_string(),
            manifest
                .verification
                .content_blake3
                .clone()
                .unwrap_or_else(|| manifest.blob_blake3.clone()),
            1,
        );
    };
    (
        storage
            .get("kind")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("invalid")
            .to_string(),
        storage
            .get("content_blake3")
            .and_then(serde_json::Value::as_str)
            .or(manifest.verification.content_blake3.as_deref())
            .unwrap_or("")
            .to_string(),
        storage
            .get("chunks")
            .and_then(serde_json::Value::as_array)
            .map_or(0, Vec::len),
    )
}

pub(crate) fn doctor_raw_mirror_none_state() -> String {
    "none".to_string()
}

pub(crate) fn doctor_raw_mirror_root(data_dir: &Path) -> PathBuf {
    data_dir
        .join(DOCTOR_RAW_MIRROR_ROOT_DIR)
        .join(DOCTOR_RAW_MIRROR_VERSION_DIR)
}

pub(crate) fn doctor_raw_mirror_root_relative_path() -> String {
    format!("{DOCTOR_RAW_MIRROR_ROOT_DIR}/{DOCTOR_RAW_MIRROR_VERSION_DIR}")
}

pub(crate) fn doctor_raw_mirror_layout_report() -> DoctorRawMirrorLayoutReport {
    DoctorRawMirrorLayoutReport {
        root_relative_path: doctor_raw_mirror_root_relative_path(),
        manifest_kind: DOCTOR_RAW_MIRROR_MANIFEST_KIND,
        hash_algorithm: DOCTOR_RAW_MIRROR_HASH_ALGORITHM,
        blob_path_template: "blobs/blake3/<first-two-hex>/<64-hex>.raw".to_string(),
        manifest_path_template: "manifests/<manifest-id>.json".to_string(),
        verification_path_template: "verification/<manifest-id>.json".to_string(),
        temp_path_template: "tmp/<operation-id>/<file>.tmp".to_string(),
        content_address_scope: "global within the cass data dir; identical whole files, chunk descriptors, and fixed-size source chunks share blobs across versions, providers, and sources".to_string(),
        source_identity_scope: "manifest metadata records provider, source_id, origin_kind, origin_host, original path hash, and db_links back to archive conversations/messages".to_string(),
        db_link_contract: "db_links entries identify archived conversations without embedding raw session content in manifests".to_string(),
        case_insensitive_collision_behavior: "no original path segment is used as a storage directory; path identity is hashed from exact bytes, so case-folding filesystems cannot collide user paths".to_string(),
        migration_contract: "schema_version and raw-mirror/vN keep future layouts side-by-side; v1 manifests without content_storage remain whole-blob captures, while new large-source manifests declare fixed_chunks_v1 metadata".to_string(),
    }
}

pub(crate) fn doctor_raw_mirror_policy_report() -> DoctorRawMirrorPolicyReport {
    DoctorRawMirrorPolicyReport {
        append_only: true,
        global_dedup_by_content_hash: true,
        never_overwrite_different_bytes: true,
        storage_scope: "local cass data directory only; raw mirror bytes are not synced, published, or attached by default".to_string(),
        local_only_by_default: true,
        directory_mode_octal: DOCTOR_RAW_MIRROR_DIR_MODE,
        file_mode_octal: DOCTOR_RAW_MIRROR_FILE_MODE,
        enforce_private_files: true,
        atomic_publish: "write whole-file or fixed-size chunk temps under tmp, verify the stable source, publish immutable content-addressed blobs, then publish the integrity-bound manifest".to_string(),
        fsync_required: false,
        path_traversal_defense: "manifest blob paths must be relative normal components under raw-mirror/v1 and may not contain absolute paths, prefixes, dot-dot, or empty components".to_string(),
        symlink_defense: "doctor verification refuses symlinked blob or manifest paths and never follows symlinks while validating mirror evidence".to_string(),
        default_report_contract: "default doctor reports expose redacted paths, content hashes, sizes, timestamps, provider/source identity, and codec/encryption metadata; exact paths and raw bytes stay internal unless a future explicit sensitive-evidence mode requests them".to_string(),
        sensitive_output_policy: DoctorRawMirrorSensitiveOutputPolicyReport {
            default_includes_exact_paths: false,
            default_includes_raw_content: false,
            safe_metadata_fields: vec![
                "manifest_id",
                "redacted_manifest_path",
                "blob_relative_path",
                "redacted_blob_path",
                "blob_blake3",
                "blob_size_bytes",
                "storage_kind",
                "source_content_blake3",
                "chunk_count",
                "provider",
                "source_id",
                "origin_kind",
                "origin_host",
                "redacted_original_path",
                "original_path_blake3",
                "captured_at_ms",
                "source_mtime_ms",
                "source_size_bytes",
                "compression_state",
                "encryption_state",
                "compression",
                "encryption",
                "db_link_count",
                "upstream_path_exists",
                "status",
                "blob_checksum_status",
                "manifest_checksum_status",
                "invalid_reason",
                "amplified_sources.evidence_kind",
                "amplified_sources.providers",
                "amplified_sources.source_id",
                "amplified_sources.redacted_original_path",
                "amplified_sources.original_path_blake3",
                "amplified_sources.version_count",
                "amplified_sources.referenced_blob_bytes",
                "amplified_sources.current_source_size_bytes",
                "amplified_sources.excess_bytes",
                "amplified_sources.amplification_ratio_milli",
            ],
            redacted_by_default_fields: vec![
                "root_path",
                "manifest_path",
                "blob_path",
                "original_path",
                "db_links.source_path",
                "raw_session_content",
                "attachment_payloads",
                "environment_secrets",
            ],
            opt_in_sensitive_modes: vec![
                "future --include-sensitive-evidence",
                "future support-bundle --include-raw-mirror-bytes",
                "future doctor evidence export with explicit fingerprint",
            ],
            redaction_marker_contract: "cass data dir paths are rendered as [cass-data]/... and external source paths as [external]/<file-name>; raw content is never replaced with a preview or snippet".to_string(),
        },
        compression_policy: DoctorRawMirrorCodecPolicyReport {
            default_state: "none",
            optional: true,
            allowed_states: vec!["none", "compressed"],
            required_metadata_fields: vec![
                "state",
                "algorithm",
                "uncompressed_size_bytes",
            ],
            raw_content_hash_scope: "blob_blake3 always names the stored bytes; compression metadata must preserve the uncompressed byte count for future verification",
        },
        encryption_policy: DoctorRawMirrorEncryptionPolicyReport {
            default_state: "none",
            optional: true,
            allowed_states: vec!["none", "encrypted"],
            required_metadata_fields: vec![
                "state",
                "algorithm",
                "key_id",
                "envelope_version",
            ],
            key_material_reporting: "key material, passphrases, nonces, and decrypted previews must never appear in reports or support bundles",
            integrity_contract: "encryption envelopes are metadata only; manifest identity, blob hash, and verification records must still make tampering detectable without decrypting in default doctor checks".to_string(),
        },
        backup_policy: DoctorRawMirrorBackupPolicyReport {
            included_in_default_backup: true,
            backup_mode: "manifest-and-blob-copy",
            backup_contents: vec![
                "raw-mirror/v1/manifests",
                "raw-mirror/v1/blobs",
                "raw-mirror/v1/verification",
            ],
            restore_precondition: "restore must verify manifests, blob checksums, and sidecar completeness before trusting backed-up raw mirror evidence".to_string(),
        },
        support_bundle_policy: DoctorRawMirrorSupportBundlePolicyReport {
            default_mode: "manifest-only",
            include_manifest_metadata: true,
            include_blob_bytes: false,
            include_exact_paths: false,
            sensitive_attachment_gate: "explicit sensitive-evidence opt-in plus plan fingerprint; not available through default doctor --json",
        },
        public_export_policy: DoctorRawMirrorPublicExportPolicyReport {
            pages_exports_include_raw_mirror: false,
            html_exports_include_raw_mirror: false,
            default_logs_include_raw_content: false,
            default_robot_json_includes_raw_content: false,
            public_artifact_contract: "Pages, HTML exports, robot logs, and default support bundles must not include raw mirror bytes, exact source paths, prompts, attachment payloads, or decrypted/encrypted evidence blobs".to_string(),
        },
        compression_contract: "v1 stores plain bytes by default; large sources use fixed_chunks_v1 content-addressed chunking without compression, while future compression must be declared in the compression envelope and preserve uncompressed size/hash metadata".to_string(),
        encryption_contract: "v1 stores unencrypted local evidence by default; future encryption must be explicit in the encryption envelope and must not weaken manifest integrity checks".to_string(),
        support_bundle_redaction_contract: "support bundles use redacted_original_path and original_path_blake3; raw bytes are not exported unless an operator explicitly asks for evidence export".to_string(),
        missing_upstream_semantics: "missing upstream provider files are distinct from missing cass mirror evidence; a verified mirror blob is preserved archive evidence even if the original source path was pruned".to_string(),
    }
}

pub(crate) fn doctor_raw_mirror_blob_relative_path(blob_blake3: &str) -> Option<String> {
    if blob_blake3.len() != 64 || !blob_blake3.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let lower = blob_blake3.to_ascii_lowercase();
    Some(format!(
        "blobs/{}/{}/{}.{}",
        DOCTOR_RAW_MIRROR_HASH_ALGORITHM,
        &lower[..2],
        lower,
        DOCTOR_RAW_MIRROR_BLOB_EXTENSION
    ))
}

pub(crate) fn doctor_raw_mirror_manifest_relative_path(manifest_id: &str) -> String {
    format!("manifests/{manifest_id}.json")
}

pub(crate) fn doctor_raw_mirror_original_path_blake3(original_path: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"doctor-raw-mirror-original-path-v1");
    hasher.update(&[0]);
    hasher.update(original_path.as_bytes());
    hasher.finalize().to_hex().to_string()
}

pub(crate) fn doctor_raw_mirror_manifest_id(
    provider: &str,
    source_id: &str,
    origin_kind: &str,
    origin_host: Option<&str>,
    original_path_blake3: &str,
    blob_blake3: &str,
) -> String {
    doctor_canonical_blake3(
        "doctor-raw-mirror-manifest-id-v1",
        serde_json::json!({
            "provider": provider,
            "source_id": source_id,
            "origin_kind": origin_kind,
            "origin_host": origin_host,
            "original_path_blake3": original_path_blake3,
            "blob_blake3": blob_blake3,
        }),
    )
}

pub(crate) fn doctor_raw_mirror_manifest_blake3(manifest: &DoctorRawMirrorManifestFile) -> String {
    let mut value = serde_json::to_value(manifest).unwrap_or_default();
    if let serde_json::Value::Object(map) = &mut value {
        map.remove("manifest_blake3");
    }
    doctor_canonical_blake3("doctor-raw-mirror-manifest-v1", value)
}

pub(crate) fn doctor_raw_mirror_validate_relative_path(
    relative: &str,
) -> std::result::Result<PathBuf, String> {
    let path = Path::new(relative);
    if relative.trim().is_empty() {
        return Err("relative path is empty".to_string());
    }
    if path.is_absolute() {
        return Err("relative path must not be absolute".to_string());
    }
    let mut cleaned = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::Normal(part) => cleaned.push(part),
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                return Err("relative path must not contain parent traversal".to_string());
            }
            std::path::Component::RootDir | std::path::Component::Prefix(_) => {
                return Err("relative path must stay under the mirror root".to_string());
            }
        }
    }
    if cleaned.as_os_str().is_empty() {
        return Err("relative path has no normal components".to_string());
    }
    Ok(cleaned)
}

pub(crate) fn doctor_raw_mirror_unique_db_links(
    links: &[DoctorRawMirrorDbLink],
) -> Vec<DoctorRawMirrorDbLink> {
    let mut dedup = links.to_vec();
    dedup.sort_by(|left, right| {
        (
            left.conversation_id,
            left.message_count,
            left.started_at_ms,
            left.source_path.as_deref().unwrap_or(""),
        )
            .cmp(&(
                right.conversation_id,
                right.message_count,
                right.started_at_ms,
                right.source_path.as_deref().unwrap_or(""),
            ))
    });
    dedup.dedup();
    dedup
}

#[cfg(unix)]
pub(crate) fn doctor_same_open_file_identity(
    left: &std::fs::Metadata,
    right: &std::fs::Metadata,
) -> bool {
    use std::os::unix::fs::MetadataExt as _;

    left.is_file() && right.is_file() && left.dev() == right.dev() && left.ino() == right.ino()
}

#[cfg(not(unix))]
pub(crate) fn doctor_same_open_file_identity(
    left: &std::fs::Metadata,
    right: &std::fs::Metadata,
) -> bool {
    left.is_file()
        && right.is_file()
        && left.len() == right.len()
        && left.modified().ok() == right.modified().ok()
}

pub(crate) fn doctor_open_regular_file_for_read(path: &Path) -> io::Result<std::fs::File> {
    let expected_metadata = std::fs::symlink_metadata(path)?;
    if expected_metadata.file_type().is_symlink() || !expected_metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "refusing to open a symlink or non-regular file",
        ));
    }

    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(path)?;
    let opened_metadata = file.metadata()?;
    let current_path_metadata = std::fs::symlink_metadata(path)?;
    if current_path_metadata.file_type().is_symlink()
        || !doctor_same_open_file_identity(&expected_metadata, &opened_metadata)
        || !doctor_same_open_file_identity(&opened_metadata, &current_path_metadata)
    {
        return Err(io::Error::other(
            "file changed identity while it was being opened",
        ));
    }
    Ok(file)
}

pub(crate) fn doctor_file_blake3(path: &Path) -> io::Result<String> {
    let mut file = doctor_open_regular_file_for_read(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = std::io::Read::read(&mut file, &mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

pub(crate) fn doctor_raw_mirror_invalid_manifest_report(
    data_dir: &Path,
    manifest_path: &Path,
    reason: String,
) -> DoctorRawMirrorManifestReport {
    DoctorRawMirrorManifestReport {
        manifest_id: "invalid".to_string(),
        manifest_path: manifest_path.display().to_string(),
        redacted_manifest_path: doctor_redacted_path(
            &manifest_path.display().to_string(),
            data_dir,
        ),
        blob_relative_path: String::new(),
        blob_path: String::new(),
        redacted_blob_path: String::new(),
        blob_blake3: String::new(),
        blob_size_bytes: 0,
        storage_kind: "unknown".to_string(),
        source_content_blake3: String::new(),
        chunk_count: 0,
        verified_stored_blobs: Vec::new(),
        provider: "unknown".to_string(),
        source_id: crate::sources::provenance::LOCAL_SOURCE_ID.to_string(),
        origin_kind: "unknown".to_string(),
        origin_host: None,
        original_path: String::new(),
        redacted_original_path: String::new(),
        original_path_blake3: String::new(),
        captured_at_ms: 0,
        source_mtime_ms: None,
        source_size_bytes: None,
        compression_state: "unknown".to_string(),
        encryption_state: "unknown".to_string(),
        compression: DoctorRawMirrorCompressionEnvelope {
            state: "unknown".to_string(),
            ..DoctorRawMirrorCompressionEnvelope::default()
        },
        encryption: DoctorRawMirrorEncryptionEnvelope {
            state: "unknown".to_string(),
            ..DoctorRawMirrorEncryptionEnvelope::default()
        },
        db_link_count: 0,
        db_links: Vec::new(),
        upstream_path_exists: None,
        status: "invalid_manifest".to_string(),
        blob_checksum_status: DoctorArtifactChecksumStatus::NotRecorded,
        manifest_checksum_status: DoctorArtifactChecksumStatus::Mismatched,
        invalid_reason: Some(reason),
    }
}

pub(crate) fn doctor_raw_mirror_loaded_invalid_manifest_report(
    data_dir: &Path,
    manifest_path: &Path,
    blob_path: &Path,
    manifest: DoctorRawMirrorManifestFile,
    manifest_checksum_status: DoctorArtifactChecksumStatus,
    reason: String,
) -> DoctorRawMirrorManifestReport {
    let manifest_path_string = manifest_path.display().to_string();
    let blob_path_string = blob_path.display().to_string();
    let db_links = doctor_raw_mirror_unique_db_links(&manifest.db_links);
    let (storage_kind, source_content_blake3, chunk_count) =
        doctor_raw_mirror_storage_metadata(&manifest);
    DoctorRawMirrorManifestReport {
        manifest_id: manifest.manifest_id,
        manifest_path: manifest_path_string.clone(),
        redacted_manifest_path: doctor_redacted_path(&manifest_path_string, data_dir),
        blob_relative_path: manifest.blob_relative_path,
        blob_path: blob_path_string.clone(),
        redacted_blob_path: doctor_redacted_path(&blob_path_string, data_dir),
        blob_blake3: manifest.blob_blake3,
        blob_size_bytes: manifest.blob_size_bytes,
        storage_kind,
        source_content_blake3,
        chunk_count,
        verified_stored_blobs: Vec::new(),
        provider: doctor_normalized_provider_slug(&manifest.provider),
        source_id: manifest.source_id,
        origin_kind: manifest.origin_kind,
        origin_host: manifest.origin_host,
        original_path: manifest.original_path.clone(),
        redacted_original_path: doctor_redacted_path(&manifest.original_path, data_dir),
        original_path_blake3: manifest.original_path_blake3,
        captured_at_ms: manifest.captured_at_ms,
        source_mtime_ms: manifest.source_mtime_ms,
        source_size_bytes: manifest.source_size_bytes,
        compression_state: manifest.compression.state.clone(),
        encryption_state: manifest.encryption.state.clone(),
        compression: manifest.compression,
        encryption: manifest.encryption,
        db_link_count: db_links.len(),
        db_links,
        upstream_path_exists: if manifest.original_path.trim().is_empty() {
            None
        } else {
            Some(Path::new(&manifest.original_path).exists())
        },
        status: "invalid_manifest".to_string(),
        blob_checksum_status: DoctorArtifactChecksumStatus::Mismatched,
        manifest_checksum_status,
        invalid_reason: Some(reason),
    }
}

pub(crate) fn doctor_verify_raw_mirror_manifest(
    data_dir: &Path,
    root: &Path,
    manifest_path: &Path,
    manifest: DoctorRawMirrorManifestFile,
) -> DoctorRawMirrorManifestReport {
    let manifest_path_string = manifest_path.display().to_string();
    let invalid =
        |reason: String| doctor_raw_mirror_invalid_manifest_report(data_dir, manifest_path, reason);

    if manifest.schema_version != DOCTOR_RAW_MIRROR_SCHEMA_VERSION {
        return invalid(format!(
            "unsupported raw mirror schema version {}",
            manifest.schema_version
        ));
    }
    if manifest.manifest_kind != DOCTOR_RAW_MIRROR_MANIFEST_KIND {
        return invalid(format!(
            "unsupported manifest kind '{}'",
            manifest.manifest_kind
        ));
    }
    if manifest.blob_hash_algorithm != DOCTOR_RAW_MIRROR_HASH_ALGORITHM {
        return invalid(format!(
            "unsupported blob hash algorithm '{}'",
            manifest.blob_hash_algorithm
        ));
    }
    let expected_blob_relative = match doctor_raw_mirror_blob_relative_path(&manifest.blob_blake3) {
        Some(path) => path,
        None => return invalid("blob_blake3 is not a 64-hex blake3 digest".to_string()),
    };
    if manifest.blob_relative_path != expected_blob_relative {
        return invalid(format!(
            "blob_relative_path '{}' does not match content-addressed path '{}'",
            manifest.blob_relative_path, expected_blob_relative
        ));
    }
    let relative_blob = match doctor_raw_mirror_validate_relative_path(&manifest.blob_relative_path)
    {
        Ok(path) => path,
        Err(reason) => return invalid(reason),
    };
    let blob_path = root.join(relative_blob);
    let blob_path_string = blob_path.display().to_string();
    if std::fs::symlink_metadata(manifest_path)
        .map(|metadata| metadata.file_type().is_symlink())
        .unwrap_or(false)
    {
        return invalid("manifest path is a symlink".to_string());
    }
    if path_has_symlink_below_root(manifest_path, root) {
        return invalid("manifest path contains a symlinked component".to_string());
    }
    let expected_manifest_id = doctor_raw_mirror_manifest_id(
        &manifest.provider,
        &manifest.source_id,
        &manifest.origin_kind,
        manifest.origin_host.as_deref(),
        &manifest.original_path_blake3,
        &manifest.blob_blake3,
    );
    if manifest.manifest_id != expected_manifest_id {
        return invalid("manifest_id does not match canonical source/blob identity".to_string());
    }
    let expected_manifest_relative =
        doctor_raw_mirror_manifest_relative_path(&manifest.manifest_id);
    if let Ok(relative_manifest) = manifest_path.strip_prefix(root)
        && relative_manifest != Path::new(&expected_manifest_relative)
    {
        return invalid(format!(
            "manifest path '{}' does not match canonical path '{}'",
            relative_manifest.display(),
            expected_manifest_relative
        ));
    }

    let computed_manifest_blake3 = doctor_raw_mirror_manifest_blake3(&manifest);
    let manifest_checksum_status = match manifest.manifest_blake3.as_deref() {
        Some(expected) if expected == computed_manifest_blake3 => {
            DoctorArtifactChecksumStatus::Matched
        }
        Some(_) => DoctorArtifactChecksumStatus::Mismatched,
        None => DoctorArtifactChecksumStatus::NotRecorded,
    };
    let (mut storage_kind, mut source_content_blake3, mut chunk_count) =
        doctor_raw_mirror_storage_metadata(&manifest);

    if blob_path
        .parent()
        .is_none_or(|parent| existing_path_has_symlink_below_root(parent, root))
    {
        return doctor_raw_mirror_loaded_invalid_manifest_report(
            data_dir,
            manifest_path,
            &blob_path,
            manifest,
            manifest_checksum_status,
            "blob path contains a symlinked component".to_string(),
        );
    }

    let (mut blob_checksum_status, mut status, mut invalid_reason) = match std::fs::symlink_metadata(
        &blob_path,
    ) {
        Ok(metadata) if metadata.file_type().is_symlink() => (
            DoctorArtifactChecksumStatus::Mismatched,
            "invalid_manifest".to_string(),
            Some("blob path is a symlink".to_string()),
        ),
        Ok(metadata) => {
            let size_matches = metadata.len() == manifest.blob_size_bytes;
            match doctor_file_blake3(&blob_path) {
                Ok(actual_hash) if actual_hash == manifest.blob_blake3 && size_matches => {
                    match manifest_checksum_status {
                        DoctorArtifactChecksumStatus::Mismatched => (
                            DoctorArtifactChecksumStatus::Matched,
                            "manifest_drift".to_string(),
                            Some("manifest_blake3 does not match manifest metadata".to_string()),
                        ),
                        DoctorArtifactChecksumStatus::NotRecorded => (
                            DoctorArtifactChecksumStatus::Matched,
                            "manifest_unverified".to_string(),
                            Some(
                                "manifest_blake3 is not recorded; metadata integrity cannot be verified"
                                    .to_string(),
                            ),
                        ),
                        _ => (
                            DoctorArtifactChecksumStatus::Matched,
                            "verified".to_string(),
                            None,
                        ),
                    }
                }
                Ok(_) => (
                    DoctorArtifactChecksumStatus::Mismatched,
                    "checksum_mismatch".to_string(),
                    Some("blob bytes or size do not match manifest".to_string()),
                ),
                Err(err) => (
                    DoctorArtifactChecksumStatus::Mismatched,
                    "checksum_mismatch".to_string(),
                    Some(format!("failed to hash blob: {err}")),
                ),
            }
        }
        Err(_) => (
            DoctorArtifactChecksumStatus::Missing,
            "missing_blob".to_string(),
            Some("content-addressed blob is missing".to_string()),
        ),
    };
    let mut verified_stored_blobs = if blob_checksum_status == DoctorArtifactChecksumStatus::Matched
    {
        vec![(manifest.blob_blake3.clone(), manifest.blob_size_bytes)]
    } else {
        Vec::new()
    };

    if manifest.content_storage.is_some()
        && manifest_checksum_status == DoctorArtifactChecksumStatus::Matched
    {
        match crate::raw_mirror::verify_source_capture(data_dir, &manifest.manifest_id) {
            Ok(verified) => {
                blob_checksum_status = DoctorArtifactChecksumStatus::Matched;
                status = "verified".to_string();
                invalid_reason = None;
                debug_assert_eq!(verified.stored_blob_count, verified.stored_blobs.len());
                debug_assert_eq!(
                    verified.stored_bytes,
                    verified
                        .stored_blobs
                        .iter()
                        .map(|(_, bytes)| *bytes)
                        .fold(0_u64, u64::saturating_add)
                );
                storage_kind = verified.storage_kind;
                source_content_blake3 = verified.source_content_blake3;
                chunk_count = verified.chunk_count;
                verified_stored_blobs = verified.stored_blobs;
                if manifest.source_size_bytes != Some(verified.source_size_bytes) {
                    blob_checksum_status = DoctorArtifactChecksumStatus::Mismatched;
                    status = "checksum_mismatch".to_string();
                    invalid_reason = Some(
                        "chunked source size disagrees with manifest source_size_bytes".to_string(),
                    );
                    verified_stored_blobs.clear();
                }
            }
            Err(error) => {
                blob_checksum_status = DoctorArtifactChecksumStatus::Mismatched;
                status = "checksum_mismatch".to_string();
                invalid_reason = Some(format!("chunked source verification failed: {error}"));
                verified_stored_blobs.clear();
            }
        }
    }

    let original_path_blake3 = doctor_raw_mirror_original_path_blake3(&manifest.original_path);
    if original_path_blake3 != manifest.original_path_blake3 {
        invalid_reason = Some("original_path_blake3 does not match original_path".to_string());
    }
    if original_path_blake3 != manifest.original_path_blake3 {
        status = "invalid_manifest".to_string();
    }
    if manifest.content_storage.is_none()
        && manifest
            .verification
            .content_blake3
            .as_deref()
            .is_some_and(|verified| verified != manifest.blob_blake3)
    {
        invalid_reason = Some("verification content_blake3 does not match blob_blake3".to_string());
        status = "invalid_manifest".to_string();
    }
    if manifest.content_storage.is_none()
        && manifest.source_size_bytes != Some(manifest.blob_size_bytes)
    {
        invalid_reason =
            Some("whole-blob source_size_bytes does not match blob_size_bytes".to_string());
        status = "invalid_manifest".to_string();
    }

    let db_links = doctor_raw_mirror_unique_db_links(&manifest.db_links);
    DoctorRawMirrorManifestReport {
        manifest_id: manifest.manifest_id,
        manifest_path: manifest_path_string.clone(),
        redacted_manifest_path: doctor_redacted_path(&manifest_path_string, data_dir),
        blob_relative_path: manifest.blob_relative_path,
        blob_path: blob_path_string.clone(),
        redacted_blob_path: doctor_redacted_path(&blob_path_string, data_dir),
        blob_blake3: manifest.blob_blake3,
        blob_size_bytes: manifest.blob_size_bytes,
        storage_kind,
        source_content_blake3,
        chunk_count,
        verified_stored_blobs,
        provider: doctor_normalized_provider_slug(&manifest.provider),
        source_id: manifest.source_id,
        origin_kind: manifest.origin_kind,
        origin_host: manifest.origin_host,
        original_path: manifest.original_path.clone(),
        redacted_original_path: doctor_redacted_path(&manifest.original_path, data_dir),
        original_path_blake3: manifest.original_path_blake3,
        captured_at_ms: manifest.captured_at_ms,
        source_mtime_ms: manifest.source_mtime_ms,
        source_size_bytes: manifest.source_size_bytes,
        compression_state: manifest.compression.state.clone(),
        encryption_state: manifest.encryption.state.clone(),
        compression: manifest.compression,
        encryption: manifest.encryption,
        db_link_count: db_links.len(),
        db_links,
        upstream_path_exists: if manifest.original_path.trim().is_empty() {
            None
        } else {
            Some(Path::new(&manifest.original_path).exists())
        },
        status,
        blob_checksum_status,
        manifest_checksum_status,
        invalid_reason,
    }
}

pub(crate) fn doctor_raw_mirror_count_interrupted_captures(root: &Path) -> usize {
    let tmp_dir = root.join("tmp");
    walkdir::WalkDir::new(tmp_dir)
        .min_depth(1)
        .follow_links(false)
        .into_iter()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().is_file() || entry.file_type().is_symlink())
        .count()
}

pub(crate) fn doctor_raw_mirror_size_warn_threshold_bytes() -> u64 {
    dotenvy::var(CASS_RAW_MIRROR_SIZE_WARN_THRESHOLD_BYTES)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(DOCTOR_RAW_MIRROR_SIZE_WARN_THRESHOLD_BYTES_DEFAULT)
}

pub(crate) fn doctor_raw_mirror_size_warning(
    total_blob_bytes: u64,
    threshold_bytes: u64,
) -> Option<String> {
    (threshold_bytes > 0 && total_blob_bytes >= threshold_bytes).then(|| {
        format!(
            "raw_mirror.size: verified raw-mirror blobs use {total_blob_bytes} bytes, at or above the warn threshold of {threshold_bytes} bytes; inspect `cass mirror prune --keep-latest 1 --json`, which retires superseded captures and keeps the newest capture of every source file, and apply only after reviewing the plan"
        )
    })
}

#[derive(Debug, Default)]
pub(crate) struct DoctorRawMirrorSourceAmplificationAccumulator {
    pub(crate) providers: BTreeSet<String>,
    pub(crate) source_id: String,
    pub(crate) original_path: String,
    pub(crate) redacted_original_path: String,
    pub(crate) original_path_blake3: String,
    pub(crate) snapshots: BTreeSet<String>,
    pub(crate) blobs: BTreeMap<String, u64>,
}

#[derive(Debug)]
pub(crate) struct DoctorRawMirrorBoundedAmplificationScan {
    pub(crate) reports: Vec<DoctorRawMirrorSourceAmplificationReport>,
    pub(crate) scanned_manifest_count: usize,
    pub(crate) truncated: bool,
}

pub(crate) fn doctor_raw_mirror_source_amplification_reports(
    manifests: &[DoctorRawMirrorManifestReport],
    ratio_warn_threshold_milli: u64,
    excess_warn_threshold_bytes: u64,
) -> Vec<DoctorRawMirrorSourceAmplificationReport> {
    let versions = manifests
        .iter()
        .filter(|manifest| doctor_raw_mirror_manifest_is_verified(manifest))
        .map(|manifest| DoctorRawMirrorSourceAmplificationVersion {
            provider: manifest.provider.clone(),
            source_id: manifest.source_id.clone(),
            origin_kind: manifest.origin_kind.clone(),
            origin_host: manifest.origin_host.clone(),
            original_path: manifest.original_path.clone(),
            redacted_original_path: manifest.redacted_original_path.clone(),
            original_path_blake3: manifest.original_path_blake3.clone(),
            snapshot_blake3: manifest.blob_blake3.clone(),
            stored_blobs: manifest.verified_stored_blobs.clone(),
        })
        .collect::<Vec<_>>();
    doctor_raw_mirror_source_amplification_reports_from_versions(
        &versions,
        "full_blob_checksum",
        ratio_warn_threshold_milli,
        excess_warn_threshold_bytes,
    )
}

pub(crate) fn doctor_raw_mirror_source_amplification_reports_from_versions(
    versions: &[DoctorRawMirrorSourceAmplificationVersion],
    evidence_kind: &'static str,
    ratio_warn_threshold_milli: u64,
    excess_warn_threshold_bytes: u64,
) -> Vec<DoctorRawMirrorSourceAmplificationReport> {
    let mut sources: BTreeMap<(String, String), DoctorRawMirrorSourceAmplificationAccumulator> =
        BTreeMap::new();
    for version in versions {
        // `original_path` is meaningful only on the machine that captured a
        // local source. A remote manifest can carry the same path text as an
        // unrelated local file, so comparing its historical bytes with local
        // filesystem metadata would manufacture an amplification warning.
        let normalized_source_id = normalized_provenance_source_id(
            version.source_id.as_str(),
            Some(version.origin_kind.as_str()),
            version.origin_host.as_deref(),
        );
        let normalized_origin_kind = normalized_provenance_origin_kind(
            normalized_source_id.as_str(),
            Some(version.origin_kind.as_str()),
        );
        if normalized_origin_kind != crate::sources::provenance::LOCAL_SOURCE_ID {
            continue;
        }
        let key = (
            version.source_id.clone(),
            version.original_path_blake3.clone(),
        );
        let source =
            sources
                .entry(key)
                .or_insert_with(|| DoctorRawMirrorSourceAmplificationAccumulator {
                    providers: BTreeSet::new(),
                    source_id: version.source_id.clone(),
                    original_path: version.original_path.clone(),
                    redacted_original_path: version.redacted_original_path.clone(),
                    original_path_blake3: version.original_path_blake3.clone(),
                    snapshots: BTreeSet::new(),
                    blobs: BTreeMap::new(),
                });
        source.providers.insert(version.provider.clone());
        source.snapshots.insert(version.snapshot_blake3.clone());
        for (blob_blake3, blob_size_bytes) in &version.stored_blobs {
            source
                .blobs
                .entry(blob_blake3.clone())
                .or_insert(*blob_size_bytes);
        }
    }

    let mut reports = Vec::new();
    for source in sources.into_values() {
        let version_count = source.snapshots.len();
        if version_count < 2 {
            continue;
        }
        let current_source_size_bytes = match std::fs::symlink_metadata(&source.original_path) {
            Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                metadata.len()
            }
            _ => continue,
        };
        let referenced_blob_bytes = source
            .blobs
            .values()
            .copied()
            .fold(0_u64, u64::saturating_add);
        let excess_bytes = referenced_blob_bytes.saturating_sub(current_source_size_bytes);
        let amplification_ratio_milli = (current_source_size_bytes > 0)
            .then(|| referenced_blob_bytes.saturating_mul(1_000) / current_source_size_bytes);
        let ratio_exceeded = amplification_ratio_milli
            .is_some_and(|ratio| ratio >= ratio_warn_threshold_milli)
            || (current_source_size_bytes == 0 && referenced_blob_bytes > 0);
        if !ratio_exceeded || excess_bytes < excess_warn_threshold_bytes {
            continue;
        }
        reports.push(DoctorRawMirrorSourceAmplificationReport {
            evidence_kind,
            providers: source.providers.into_iter().collect(),
            source_id: source.source_id,
            redacted_original_path: source.redacted_original_path,
            original_path_blake3: source.original_path_blake3,
            version_count,
            referenced_blob_bytes,
            current_source_size_bytes,
            excess_bytes,
            amplification_ratio_milli,
        });
    }
    reports.sort_by(|left, right| {
        right
            .excess_bytes
            .cmp(&left.excess_bytes)
            .then_with(|| left.providers.cmp(&right.providers))
            .then_with(|| left.source_id.cmp(&right.source_id))
            .then_with(|| left.original_path_blake3.cmp(&right.original_path_blake3))
    });
    reports
}

/// Read the cheap, integrity-bound subset needed for a storage-amplification
/// warning without hashing the entire raw blob. This is deliberately not a
/// repair-authority proof: callers may use it only for diagnostics, and the
/// serialized report labels the weaker evidence class explicitly.
pub(crate) fn doctor_raw_mirror_amplification_version_from_manifest_path(
    data_dir: &Path,
    root: &Path,
    manifest_path: &Path,
) -> Option<DoctorRawMirrorSourceAmplificationVersion> {
    let metadata = std::fs::symlink_metadata(manifest_path).ok()?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() > DOCTOR_RAW_MIRROR_AMPLIFICATION_MANIFEST_MAX_BYTES
        || path_has_symlink_below_root(manifest_path, root)
    {
        return None;
    }
    let manifest: DoctorRawMirrorManifestFile =
        serde_json::from_slice(&std::fs::read(manifest_path).ok()?).ok()?;
    if manifest.schema_version != DOCTOR_RAW_MIRROR_SCHEMA_VERSION
        || manifest.manifest_kind != DOCTOR_RAW_MIRROR_MANIFEST_KIND
        || manifest.blob_hash_algorithm != DOCTOR_RAW_MIRROR_HASH_ALGORITHM
    {
        return None;
    }

    let expected_blob_relative = doctor_raw_mirror_blob_relative_path(&manifest.blob_blake3)?;
    if manifest.blob_relative_path != expected_blob_relative {
        return None;
    }
    let relative_blob =
        doctor_raw_mirror_validate_relative_path(&manifest.blob_relative_path).ok()?;
    let blob_path = root.join(relative_blob);
    if blob_path
        .parent()
        .is_none_or(|parent| existing_path_has_symlink_below_root(parent, root))
    {
        return None;
    }
    let blob_metadata = std::fs::symlink_metadata(&blob_path).ok()?;
    if blob_metadata.file_type().is_symlink()
        || !blob_metadata.is_file()
        || blob_metadata.len() != manifest.blob_size_bytes
    {
        return None;
    }

    let expected_original_path_blake3 =
        doctor_raw_mirror_original_path_blake3(&manifest.original_path);
    if expected_original_path_blake3 != manifest.original_path_blake3 {
        return None;
    }
    let expected_manifest_id = doctor_raw_mirror_manifest_id(
        &manifest.provider,
        &manifest.source_id,
        &manifest.origin_kind,
        manifest.origin_host.as_deref(),
        &manifest.original_path_blake3,
        &manifest.blob_blake3,
    );
    if manifest.manifest_id != expected_manifest_id {
        return None;
    }
    let expected_manifest_relative =
        doctor_raw_mirror_manifest_relative_path(&manifest.manifest_id);
    if manifest_path.strip_prefix(root).ok()? != Path::new(&expected_manifest_relative) {
        return None;
    }
    let computed_manifest_blake3 = doctor_raw_mirror_manifest_blake3(&manifest);
    if manifest.manifest_blake3.as_deref() != Some(computed_manifest_blake3.as_str()) {
        return None;
    }
    let mut stored_blobs = vec![(manifest.blob_blake3.clone(), manifest.blob_size_bytes)];
    let expected_content_blake3 = if let Some(storage) = manifest.content_storage.as_ref() {
        if storage.get("kind").and_then(serde_json::Value::as_str) != Some("fixed_chunks_v1")
            || storage
                .get("content_hash_algorithm")
                .and_then(serde_json::Value::as_str)
                != Some(DOCTOR_RAW_MIRROR_HASH_ALGORITHM)
        {
            return None;
        }
        let content_blake3 = storage
            .get("content_blake3")
            .and_then(serde_json::Value::as_str)?;
        doctor_raw_mirror_blob_relative_path(content_blake3)?;
        let content_size_bytes = storage
            .get("content_size_bytes")
            .and_then(serde_json::Value::as_u64)?;
        let declared_chunk_size_bytes = storage
            .get("chunk_size_bytes")
            .and_then(serde_json::Value::as_u64)?;
        if declared_chunk_size_bytes == 0 {
            return None;
        }
        if manifest.source_size_bytes != Some(content_size_bytes) {
            return None;
        }
        let chunks = storage
            .get("chunks")
            .and_then(serde_json::Value::as_array)?;
        if chunks.is_empty() {
            return None;
        }
        let mut reconstructed_size_bytes = 0_u64;
        for (index, chunk) in chunks.iter().enumerate() {
            let chunk_blake3 = chunk
                .get("blob_blake3")
                .and_then(serde_json::Value::as_str)?;
            let chunk_relative_path = chunk
                .get("blob_relative_path")
                .and_then(serde_json::Value::as_str)?;
            let chunk_blob_size_bytes = chunk
                .get("blob_size_bytes")
                .and_then(serde_json::Value::as_u64)?;
            let is_last = index + 1 == chunks.len();
            if chunk_blob_size_bytes == 0
                || chunk_blob_size_bytes > declared_chunk_size_bytes
                || (!is_last && chunk_blob_size_bytes != declared_chunk_size_bytes)
            {
                return None;
            }
            reconstructed_size_bytes =
                reconstructed_size_bytes.checked_add(chunk_blob_size_bytes)?;
            if doctor_raw_mirror_blob_relative_path(chunk_blake3)?.as_str() != chunk_relative_path {
                return None;
            }
            let relative_chunk =
                doctor_raw_mirror_validate_relative_path(chunk_relative_path).ok()?;
            let chunk_path = root.join(relative_chunk);
            if chunk_path
                .parent()
                .is_none_or(|parent| existing_path_has_symlink_below_root(parent, root))
            {
                return None;
            }
            let chunk_metadata = std::fs::symlink_metadata(&chunk_path).ok()?;
            if chunk_metadata.file_type().is_symlink()
                || !chunk_metadata.is_file()
                || chunk_metadata.len() != chunk_blob_size_bytes
            {
                return None;
            }
            stored_blobs.push((chunk_blake3.to_string(), chunk_blob_size_bytes));
        }
        if reconstructed_size_bytes != content_size_bytes
            || manifest.verification.content_blake3.as_deref() != Some(content_blake3)
        {
            return None;
        }
        content_blake3
    } else {
        if manifest.source_size_bytes != Some(manifest.blob_size_bytes) {
            return None;
        }
        manifest.blob_blake3.as_str()
    };
    if manifest
        .verification
        .content_blake3
        .as_deref()
        .is_some_and(|verified| verified != expected_content_blake3)
    {
        return None;
    }

    Some(DoctorRawMirrorSourceAmplificationVersion {
        provider: doctor_normalized_provider_slug(&manifest.provider),
        source_id: manifest.source_id,
        origin_kind: manifest.origin_kind,
        origin_host: manifest.origin_host,
        original_path: manifest.original_path.clone(),
        redacted_original_path: doctor_redacted_path(&manifest.original_path, data_dir),
        original_path_blake3: manifest.original_path_blake3,
        snapshot_blake3: manifest.blob_blake3,
        stored_blobs,
    })
}

pub(crate) fn doctor_raw_mirror_bounded_amplification_reports(
    data_dir: &Path,
    root: &Path,
    manifest_entries: &[(PathBuf, bool)],
    inventory_truncated: bool,
    ratio_warn_threshold_milli: u64,
    excess_warn_threshold_bytes: u64,
) -> DoctorRawMirrorBoundedAmplificationScan {
    doctor_raw_mirror_bounded_amplification_reports_with_limits(
        data_dir,
        root,
        manifest_entries,
        inventory_truncated,
        ratio_warn_threshold_milli,
        excess_warn_threshold_bytes,
        DoctorRawMirrorBoundedScanLimits {
            manifest_count: DOCTOR_RAW_MIRROR_BOUNDED_AMPLIFICATION_MANIFEST_LIMIT,
            byte_count: DOCTOR_RAW_MIRROR_BOUNDED_AMPLIFICATION_BYTE_LIMIT,
        },
    )
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DoctorRawMirrorBoundedScanLimits {
    pub(crate) manifest_count: usize,
    pub(crate) byte_count: u64,
}

pub(crate) fn doctor_raw_mirror_bounded_amplification_reports_with_limits(
    data_dir: &Path,
    root: &Path,
    manifest_entries: &[(PathBuf, bool)],
    inventory_truncated: bool,
    ratio_warn_threshold_milli: u64,
    excess_warn_threshold_bytes: u64,
    limits: DoctorRawMirrorBoundedScanLimits,
) -> DoctorRawMirrorBoundedAmplificationScan {
    let mut versions = Vec::new();
    let mut scanned_manifest_count = 0_usize;
    let mut scanned_manifest_bytes = 0_u64;
    let mut truncated = inventory_truncated;
    for (path, is_symlink) in manifest_entries {
        if *is_symlink {
            continue;
        }
        let Ok(metadata) = std::fs::symlink_metadata(path) else {
            continue;
        };
        if !metadata.is_file()
            || metadata.len() > DOCTOR_RAW_MIRROR_AMPLIFICATION_MANIFEST_MAX_BYTES
        {
            continue;
        }
        let Some(next_scanned_bytes) = scanned_manifest_bytes.checked_add(metadata.len()) else {
            truncated = true;
            break;
        };
        if scanned_manifest_count >= limits.manifest_count || next_scanned_bytes > limits.byte_count
        {
            truncated = true;
            break;
        }
        scanned_manifest_count += 1;
        scanned_manifest_bytes = next_scanned_bytes;
        if let Some(version) =
            doctor_raw_mirror_amplification_version_from_manifest_path(data_dir, root, path)
        {
            versions.push(version);
        }
    }
    let reports = doctor_raw_mirror_source_amplification_reports_from_versions(
        &versions,
        "manifest_checksum_and_blob_metadata",
        ratio_warn_threshold_milli,
        excess_warn_threshold_bytes,
    );
    DoctorRawMirrorBoundedAmplificationScan {
        reports,
        scanned_manifest_count,
        truncated,
    }
}

pub(crate) fn doctor_raw_mirror_apply_source_amplification_reports(
    report: &mut DoctorRawMirrorReport,
    amplified_sources: Vec<DoctorRawMirrorSourceAmplificationReport>,
) {
    report.amplified_sources = amplified_sources;
    report.summary.amplified_source_count = report.amplified_sources.len();
    report.summary.amplified_source_referenced_bytes = report
        .amplified_sources
        .iter()
        .map(|source| source.referenced_blob_bytes)
        .fold(0_u64, u64::saturating_add);
    report.summary.amplified_source_excess_bytes = report
        .amplified_sources
        .iter()
        .map(|source| source.excess_bytes)
        .fold(0_u64, u64::saturating_add);
    report.summary.max_source_amplification_ratio_milli = report
        .amplified_sources
        .iter()
        .filter_map(|source| source.amplification_ratio_milli)
        .max();
    if let Some(worst) = report.amplified_sources.first() {
        let ratio = worst
            .amplification_ratio_milli
            .map(|ratio| format!("{:.1}x", ratio as f64 / 1_000.0))
            .unwrap_or_else(|| "unbounded (current source is empty)".to_string());
        report.warnings.push(format!(
            "raw_mirror.source_amplification: {} source path(s) materially exceed their current file size; evidence={} worst={} providers={:?} versions={} referenced_bytes={} current_bytes={} excess_bytes={} ratio={}. Review `amplified_sources` before applying any prune because an older version can be sole-copy evidence after provider rotation.",
            report.summary.amplified_source_count,
            worst.evidence_kind,
            worst.redacted_original_path,
            worst.providers,
            worst.version_count,
            worst.referenced_blob_bytes,
            worst.current_source_size_bytes,
            worst.excess_bytes,
            ratio,
        ));
    }
}

pub(crate) fn doctor_raw_mirror_full_verify_requested() -> bool {
    dotenvy::var(CASS_DOCTOR_RAW_MIRROR_FULL_VERIFY)
        .ok()
        .is_some_and(|raw| {
            matches!(
                raw.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
}

pub(crate) fn doctor_raw_mirror_full_verify_manifest_limit() -> usize {
    dotenvy::var(CASS_DOCTOR_RAW_MIRROR_FULL_VERIFY_MANIFEST_LIMIT)
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|&limit| limit > 0)
        .unwrap_or(DOCTOR_RAW_MIRROR_DEFAULT_FULL_VERIFY_MANIFEST_LIMIT)
}

pub(crate) fn doctor_raw_mirror_full_verify_byte_limit() -> u64 {
    dotenvy::var(CASS_DOCTOR_RAW_MIRROR_FULL_VERIFY_BYTE_LIMIT)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|&limit| limit > 0)
        .unwrap_or(DOCTOR_RAW_MIRROR_DEFAULT_FULL_VERIFY_BYTE_LIMIT)
}

pub(crate) fn doctor_raw_mirror_estimated_verification_bytes(
    manifest_entries: &[(PathBuf, bool)],
) -> u64 {
    let mut estimated_bytes = 0_u64;
    for (path, is_symlink) in manifest_entries {
        if *is_symlink {
            continue;
        }
        let Ok(metadata) = std::fs::symlink_metadata(path) else {
            continue;
        };
        if !metadata.is_file() || metadata.len() > DOCTOR_RAW_MIRROR_MANIFEST_MAX_BYTES {
            continue;
        }
        estimated_bytes = estimated_bytes.saturating_add(metadata.len());
        let Ok(bytes) = std::fs::read(path) else {
            continue;
        };
        let Ok(manifest) = serde_json::from_slice::<DoctorRawMirrorManifestFile>(&bytes) else {
            continue;
        };
        estimated_bytes = estimated_bytes.saturating_add(manifest.blob_size_bytes);
        if manifest.content_storage.is_some() {
            // Chunked verification reads the manifest a second time through the
            // raw-mirror validator. The descriptor is read three times total:
            // once by doctor, then once for its raw checksum and once for typed
            // descriptor parsing. Chunk bytes are streamed once in source order.
            estimated_bytes = estimated_bytes
                .saturating_add(metadata.len())
                .saturating_add(manifest.blob_size_bytes.saturating_mul(2))
                .saturating_add(manifest.source_size_bytes.unwrap_or_default());
        }
    }
    estimated_bytes
}

pub(crate) fn collect_doctor_raw_mirror_report_for_doctor(
    data_dir: &Path,
    mutating_repair_requested: bool,
) -> DoctorRawMirrorReport {
    let mode = if mutating_repair_requested || doctor_raw_mirror_full_verify_requested() {
        DoctorRawMirrorVerificationMode::Full
    } else {
        DoctorRawMirrorVerificationMode::Bounded {
            manifest_limit: doctor_raw_mirror_full_verify_manifest_limit(),
            byte_limit: doctor_raw_mirror_full_verify_byte_limit(),
        }
    };
    collect_doctor_raw_mirror_report_with_threshold_and_mode(
        data_dir,
        doctor_raw_mirror_size_warn_threshold_bytes(),
        mode,
    )
}

pub(crate) fn collect_doctor_raw_mirror_report(data_dir: &Path) -> DoctorRawMirrorReport {
    collect_doctor_raw_mirror_report_with_threshold_and_mode(
        data_dir,
        doctor_raw_mirror_size_warn_threshold_bytes(),
        DoctorRawMirrorVerificationMode::Full,
    )
}

#[cfg(test)]
pub(crate) fn collect_doctor_raw_mirror_report_with_threshold(
    data_dir: &Path,
    size_warn_threshold_bytes: u64,
) -> DoctorRawMirrorReport {
    collect_doctor_raw_mirror_report_with_threshold_and_mode(
        data_dir,
        size_warn_threshold_bytes,
        DoctorRawMirrorVerificationMode::Full,
    )
}

pub(crate) fn collect_doctor_raw_mirror_report_with_threshold_and_mode(
    data_dir: &Path,
    size_warn_threshold_bytes: u64,
    verification_mode: DoctorRawMirrorVerificationMode,
) -> DoctorRawMirrorReport {
    collect_doctor_raw_mirror_report_with_thresholds_and_mode(
        data_dir,
        size_warn_threshold_bytes,
        DOCTOR_RAW_MIRROR_SOURCE_AMPLIFICATION_WARN_RATIO_MILLI_DEFAULT,
        DOCTOR_RAW_MIRROR_SOURCE_AMPLIFICATION_WARN_EXCESS_BYTES_DEFAULT,
        verification_mode,
    )
}

#[cfg(test)]
pub(crate) fn collect_doctor_raw_mirror_report_with_amplification_thresholds(
    data_dir: &Path,
    ratio_warn_threshold_milli: u64,
    excess_warn_threshold_bytes: u64,
) -> DoctorRawMirrorReport {
    collect_doctor_raw_mirror_report_with_thresholds_and_mode(
        data_dir,
        0,
        ratio_warn_threshold_milli,
        excess_warn_threshold_bytes,
        DoctorRawMirrorVerificationMode::Full,
    )
}
