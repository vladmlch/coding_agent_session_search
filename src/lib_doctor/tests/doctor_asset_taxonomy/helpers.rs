#[allow(unused_imports)]
use super::*;
#[allow(unused_imports)]
use crate::franken_sync::compat::{
    ConnectionExt, OpenFlags as FrankenOpenFlags, RowExt,
    open_with_flags as open_franken_with_flags,
};
#[allow(unused_imports)]
use crate::indexer::IndexOptions;
#[allow(unused_imports)]
use crate::model::cli_error_kind::ErrorKind as CliErrorKind;
#[allow(unused_imports)]
use crate::robot_budget_envelope::BudgetSnapshot;
#[allow(unused_imports)]
use crate::{cli_app::*, cli_root::*, lib_doctor::*, *};
#[allow(unused_imports)]
use anyhow::Result;
#[allow(unused_imports)]
use base64::prelude::*;
#[allow(unused_imports)]
use chrono::Utc;
#[allow(unused_imports)]
use clap::{Arg, ArgAction, Command, CommandFactory, Parser, Subcommand, ValueEnum, ValueHint};
#[allow(unused_imports)]
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
#[allow(unused_imports)]
use std::fs::{File, OpenOptions};
#[allow(unused_imports)]
use std::io::{self, BufRead, BufReader, IsTerminal, Read, Seek, SeekFrom, Write};
#[allow(unused_imports)]
use std::path::{Path, PathBuf};
#[allow(unused_imports)]
use std::sync::{Arc, Mutex};
#[allow(unused_imports)]
use std::time::{Duration, Instant};
#[allow(unused_imports)]
use tracing::{info, warn};

pub(crate) const ALL_DOCTOR_ASSET_CLASSES: &[DoctorAssetClass] = &[
    DoctorAssetClass::SourceSessionLog,
    DoctorAssetClass::RawMirrorBlob,
    DoctorAssetClass::CanonicalArchiveDb,
    DoctorAssetClass::ArchiveDbSidecar,
    DoctorAssetClass::SourceCoverageLedger,
    DoctorAssetClass::UserConfig,
    DoctorAssetClass::BookmarkStore,
    DoctorAssetClass::OperationReceipt,
    DoctorAssetClass::EventLog,
    DoctorAssetClass::ForensicBundle,
    DoctorAssetClass::SupportBundle,
    DoctorAssetClass::BackupBundle,
    DoctorAssetClass::DerivedLexicalIndex,
    DoctorAssetClass::DerivedSemanticIndex,
    DoctorAssetClass::ModelCache,
    DoctorAssetClass::MemoCache,
    DoctorAssetClass::RetainedPublishBackup,
    DoctorAssetClass::FailedSeedBundle,
    DoctorAssetClass::QuarantinedLexicalGeneration,
    DoctorAssetClass::QuarantinedLexicalShard,
    DoctorAssetClass::ReclaimableDerivedCache,
    DoctorAssetClass::ExternalUpstreamSource,
    DoctorAssetClass::Unknown,
];

pub(crate) const ALL_DOCTOR_HEALTH_CLASSES: &[DoctorHealth] = &[
    DoctorHealth::Healthy,
    DoctorHealth::DegradedDerivedAssets,
    DoctorHealth::DegradedArchiveRisk,
    DoctorHealth::RepairBlocked,
    DoctorHealth::RepairPreviouslyFailed,
    DoctorHealth::SourceAuthorityUnsafe,
];

pub(crate) const ALL_DOCTOR_ANOMALIES: &[DoctorAnomaly] = &[
    DoctorAnomaly::Healthy,
    DoctorAnomaly::DegradedDerivedAssets,
    DoctorAnomaly::DegradedArchiveRisk,
    DoctorAnomaly::RepairBlocked,
    DoctorAnomaly::RepairPreviouslyFailed,
    DoctorAnomaly::SourceAuthorityUnsafe,
    DoctorAnomaly::ArchiveDbCorrupt,
    DoctorAnomaly::ArchiveDbUnreadable,
    DoctorAnomaly::RawMirrorMissing,
    DoctorAnomaly::RawMirrorBehindSource,
    DoctorAnomaly::UpstreamSourcePruned,
    DoctorAnomaly::DerivedLexicalStale,
    DoctorAnomaly::DerivedSemanticStale,
    DoctorAnomaly::InterruptedRepair,
    DoctorAnomaly::LockContention,
    DoctorAnomaly::StoragePressure,
    DoctorAnomaly::FullRebuildHeadroom,
    DoctorAnomaly::ConfigExclusionRisk,
    DoctorAnomaly::BackupUnverified,
    DoctorAnomaly::BackupStale,
    DoctorAnomaly::PrivacyRedactionRequired,
];

pub(crate) const ALL_DOCTOR_OPERATION_OUTCOMES: &[DoctorOperationOutcomeKind] = &[
    DoctorOperationOutcomeKind::OkNoActionNeeded,
    DoctorOperationOutcomeKind::OkReadOnlyDiagnosed,
    DoctorOperationOutcomeKind::Fixed,
    DoctorOperationOutcomeKind::PartiallyFixed,
    DoctorOperationOutcomeKind::RepairBlocked,
    DoctorOperationOutcomeKind::RepairRefused,
    DoctorOperationOutcomeKind::RepairIncomplete,
    DoctorOperationOutcomeKind::VerificationFailed,
    DoctorOperationOutcomeKind::CleanupDryRunOnly,
    DoctorOperationOutcomeKind::CleanupRefused,
    DoctorOperationOutcomeKind::AutoRunSkipped,
    DoctorOperationOutcomeKind::SupportBundleOnly,
    DoctorOperationOutcomeKind::BaselineDiffOnly,
    DoctorOperationOutcomeKind::RequiresManualReview,
];

pub(crate) const ALL_DOCTOR_SOURCE_AUTHORITIES: &[DoctorSourceAuthorityKind] = &[
    DoctorSourceAuthorityKind::CanonicalArchiveDb,
    DoctorSourceAuthorityKind::VerifiedRawMirror,
    DoctorSourceAuthorityKind::VerifiedBackupBundle,
    DoctorSourceAuthorityKind::VerifiedCandidateArchive,
    DoctorSourceAuthorityKind::LiveUpstreamSource,
    DoctorSourceAuthorityKind::RemoteSyncCopy,
    DoctorSourceAuthorityKind::DerivedLexicalIndex,
    DoctorSourceAuthorityKind::DerivedSemanticIndex,
    DoctorSourceAuthorityKind::SupportBundle,
];

pub(crate) fn test_failure_marker(
    repair_class: &str,
    operation_id: &str,
    failed_at_ms: i64,
) -> DoctorRepairFailureMarker {
    DoctorRepairFailureMarker {
        marker_kind: DOCTOR_REPAIR_FAILURE_MARKER_KIND.to_string(),
        schema_version: DOCTOR_REPAIR_FAILURE_MARKER_SCHEMA_VERSION,
        repair_class: repair_class.to_string(),
        operation_id: operation_id.to_string(),
        command_line_mode: "cass doctor --json --fix".to_string(),
        plan_fingerprint: format!("plan-{operation_id}"),
        affected_artifacts: Vec::new(),
        selected_authorities: vec!["test-authority".to_string()],
        rejected_authorities: Vec::new(),
        preflight_checks: vec!["database:pass".to_string()],
        applied_actions: Vec::new(),
        verification_checks: vec!["rebuild:fail".to_string()],
        failed_checks: vec!["rebuild:repair-previously-failed".to_string()],
        forensic_bundle_path: Some("[cass-data]/doctor/forensics/test".to_string()),
        candidate_path: Some("[cass-data]/doctor/tmp/candidate".to_string()),
        started_at_ms: Some(failed_at_ms - 10),
        failed_at_ms,
        cass_version: env!("CARGO_PKG_VERSION").to_string(),
        platform: "test/test".to_string(),
        user_data_modified: false,
        operation_outcome_kind: "verification-failed".to_string(),
    }
}

pub(crate) fn write_stale_doctor_lock_metadata(data_dir: &Path, heartbeat_ms: i64) -> PathBuf {
    use std::io::Write as _;
    let lock_path = doctor_mutation_lock_path(data_dir);
    std::fs::create_dir_all(lock_path.parent().expect("lock parent"))
        .expect("create doctor lock dir");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&lock_path)
        .expect("open doctor lock");
    write!(
        file,
        "schema_version=1\npid=999999\nstarted_at_ms={heartbeat_ms}\nupdated_at_ms={heartbeat_ms}\nmode=safe_auto_run\ncommand=cass doctor --fix\n"
    )
    .expect("write doctor lock metadata");
    lock_path
}

pub(crate) fn write_aged_failure_marker(
    data_dir: &Path,
    repair_class: &str,
    failed_at_ms: i64,
) -> PathBuf {
    let marker_dir = doctor_repair_failure_marker_dir(data_dir, repair_class);
    std::fs::create_dir_all(&marker_dir).expect("create marker dir");
    let marker = serde_json::json!({
        "marker_kind": DOCTOR_REPAIR_FAILURE_MARKER_KIND,
        "schema_version": DOCTOR_REPAIR_FAILURE_MARKER_SCHEMA_VERSION,
        "repair_class": repair_class,
        "operation_id": "crashed-op",
        "command_line_mode": "cass doctor --json --fix",
        "plan_fingerprint": "plan-crashed-op",
        "affected_artifacts": [],
        "selected_authorities": [],
        "rejected_authorities": [],
        "preflight_checks": [],
        "applied_actions": [],
        "verification_checks": [],
        "failed_checks": ["rebuild:crashed"],
        "forensic_bundle_path": null,
        "candidate_path": null,
        "started_at_ms": failed_at_ms - 10,
        "failed_at_ms": failed_at_ms,
        "cass_version": env!("CARGO_PKG_VERSION"),
        "platform": "test/test",
        "user_data_modified": false,
        "operation_outcome_kind": "repair-failed",
    });
    let path = marker_dir.join(format!("{failed_at_ms}-crashed-op.json"));
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(&marker).expect("encode marker"),
    )
    .expect("write marker");
    path
}

pub(crate) fn test_cleanup_action(
    path: &str,
    planned_reclaimable_bytes: u64,
) -> DiagCleanupApplyAction {
    DiagCleanupApplyAction {
        asset_safety: doctor_asset_safety(DoctorAssetClass::RetainedPublishBackup),
        artifact_kind: "retained_publish_backup".to_string(),
        path: path.to_string(),
        generation_id: None,
        shard_id: None,
        disposition: None,
        reason: "outside retention cap".to_string(),
        planned_reclaimable_bytes,
        reclaimed_bytes: 0,
        applied: false,
        skipped: false,
        skip_reason: None,
        mutation_receipt: None,
    }
}

pub(crate) fn test_doctor_raw_mirror_report_for_candidate(
    data_dir: &Path,
) -> DoctorRawMirrorReport {
    DoctorRawMirrorReport {
        schema_version: DOCTOR_RAW_MIRROR_SCHEMA_VERSION,
        status: "verified".to_string(),
        root_path: doctor_raw_mirror_root(data_dir).display().to_string(),
        redacted_root_path: "[cass-data]/raw-mirror/v1".to_string(),
        exists: true,
        sensitive_paths_included: false,
        raw_content_included: false,
        layout: doctor_raw_mirror_layout_report(),
        policy: doctor_raw_mirror_policy_report(),
        summary: DoctorRawMirrorSummary {
            manifest_count: 2,
            verified_blob_count: 1,
            invalid_manifest_count: 1,
            total_blob_bytes: 12,
            ..DoctorRawMirrorSummary::default()
        },
        manifests: vec![
            DoctorRawMirrorManifestReport {
                manifest_id: "verified-manifest".to_string(),
                manifest_path: doctor_raw_mirror_root(data_dir)
                    .join("manifests")
                    .join("verified.json")
                    .display()
                    .to_string(),
                redacted_manifest_path: "[cass-data]/raw-mirror/v1/manifests/verified.json"
                    .to_string(),
                blob_relative_path: "blobs/blake3/00/verified.raw".to_string(),
                blob_path: doctor_raw_mirror_root(data_dir)
                    .join("blobs/blake3/00/verified.raw")
                    .display()
                    .to_string(),
                redacted_blob_path: "[cass-data]/raw-mirror/v1/blobs/blake3/00/verified.raw"
                    .to_string(),
                blob_blake3: "00".repeat(32),
                blob_size_bytes: 12,
                storage_kind: "whole_blob_v1".to_string(),
                source_content_blake3: "00".repeat(32),
                chunk_count: 1,
                verified_stored_blobs: vec![("00".repeat(32), 12)],
                provider: "codex".to_string(),
                source_id: crate::sources::provenance::LOCAL_SOURCE_ID.to_string(),
                origin_kind: "local".to_string(),
                origin_host: None,
                original_path: "/external/pruned-session.jsonl".to_string(),
                redacted_original_path: "[external]/pruned-session.jsonl".to_string(),
                original_path_blake3: "path-hash".to_string(),
                captured_at_ms: 1_733_000_000_000,
                source_mtime_ms: Some(1_733_000_000_000),
                source_size_bytes: Some(12),
                compression_state: "none".to_string(),
                encryption_state: "none".to_string(),
                compression: DoctorRawMirrorCompressionEnvelope::default(),
                encryption: DoctorRawMirrorEncryptionEnvelope::default(),
                db_link_count: 1,
                db_links: vec![DoctorRawMirrorDbLink {
                    conversation_id: Some(1),
                    message_count: Some(1),
                    source_path: Some("/external/pruned-session.jsonl".to_string()),
                    started_at_ms: Some(1_700_000_000_000),
                }],
                upstream_path_exists: Some(false),
                status: "verified".to_string(),
                blob_checksum_status: DoctorArtifactChecksumStatus::Matched,
                manifest_checksum_status: DoctorArtifactChecksumStatus::Matched,
                invalid_reason: None,
            },
            DoctorRawMirrorManifestReport {
                manifest_id: "invalid-manifest".to_string(),
                manifest_path: doctor_raw_mirror_root(data_dir)
                    .join("manifests")
                    .join("invalid.json")
                    .display()
                    .to_string(),
                redacted_manifest_path: "[cass-data]/raw-mirror/v1/manifests/invalid.json"
                    .to_string(),
                blob_relative_path: "blobs/blake3/ff/missing.raw".to_string(),
                blob_path: doctor_raw_mirror_root(data_dir)
                    .join("blobs/blake3/ff/missing.raw")
                    .display()
                    .to_string(),
                redacted_blob_path: "[cass-data]/raw-mirror/v1/blobs/blake3/ff/missing.raw"
                    .to_string(),
                blob_blake3: "ff".repeat(32),
                blob_size_bytes: 9,
                storage_kind: "whole_blob_v1".to_string(),
                source_content_blake3: "ff".repeat(32),
                chunk_count: 1,
                verified_stored_blobs: Vec::new(),
                provider: "codex".to_string(),
                source_id: crate::sources::provenance::LOCAL_SOURCE_ID.to_string(),
                origin_kind: "local".to_string(),
                origin_host: None,
                original_path: "/external/bad-session.jsonl".to_string(),
                redacted_original_path: "[external]/bad-session.jsonl".to_string(),
                original_path_blake3: "bad-path-hash".to_string(),
                captured_at_ms: 1_733_000_000_100,
                source_mtime_ms: Some(1_733_000_000_100),
                source_size_bytes: Some(9),
                compression_state: "none".to_string(),
                encryption_state: "none".to_string(),
                compression: DoctorRawMirrorCompressionEnvelope::default(),
                encryption: DoctorRawMirrorEncryptionEnvelope::default(),
                db_link_count: 0,
                db_links: Vec::new(),
                upstream_path_exists: Some(false),
                status: "invalid_manifest".to_string(),
                blob_checksum_status: DoctorArtifactChecksumStatus::Missing,
                manifest_checksum_status: DoctorArtifactChecksumStatus::Mismatched,
                invalid_reason: Some("fixture parse error".to_string()),
            },
        ],
        amplified_sources: Vec::new(),
        warnings: Vec::new(),
        notes: Vec::new(),
    }
}

pub(crate) fn seed_candidate_source_db(db_path: &Path) {
    let conn = crate::franken_sync::Connection::open(db_path.to_string_lossy().as_ref())
        .expect("open source db");
    conn.execute("PRAGMA application_id = 1128354643;")
        .expect("set application_id");
    conn.execute("PRAGMA user_version = 1;")
        .expect("set user_version");
    conn.execute("PRAGMA journal_mode = WAL;")
        .expect("set wal mode");
    conn.execute("CREATE TABLE conversations (id INTEGER PRIMARY KEY);")
        .expect("create conversations");
    conn.execute("CREATE TABLE messages (id INTEGER PRIMARY KEY);")
        .expect("create messages");
    conn.execute("INSERT INTO conversations (id) VALUES (1);")
        .expect("insert conversation");
    conn.execute("INSERT INTO messages (id) VALUES (1);")
        .expect("insert message");
}

pub(crate) fn write_candidate_promotion_test_manifest(
    db_path: &Path,
    index_path: &Path,
    candidate_dir: &Path,
    candidate_id: &str,
    candidate_bytes: &[u8],
    promote_allowed: bool,
) -> PathBuf {
    write_candidate_promotion_test_manifest_with_sidecars(
        db_path,
        index_path,
        candidate_dir,
        candidate_id,
        candidate_bytes,
        &[],
        promote_allowed,
    )
}

pub(crate) fn write_candidate_promotion_test_manifest_with_sidecars(
    db_path: &Path,
    index_path: &Path,
    candidate_dir: &Path,
    candidate_id: &str,
    candidate_bytes: &[u8],
    sidecars: &[(&str, &[u8])],
    promote_allowed: bool,
) -> PathBuf {
    std::fs::create_dir_all(candidate_dir.join("database")).expect("create candidate db dir");
    std::fs::create_dir_all(candidate_dir.join("index/lexical"))
        .expect("create candidate lexical dir");
    std::fs::create_dir_all(candidate_dir.join("index/semantic"))
        .expect("create candidate semantic dir");
    let candidate_db = candidate_dir.join("database").join("candidate.db");
    std::fs::write(&candidate_db, candidate_bytes).expect("write candidate db");
    let mut checksum_set = serde_json::Map::new();
    checksum_set.insert(
        "database/candidate.db".to_string(),
        serde_json::Value::String(blake3::hash(candidate_bytes).to_hex().to_string()),
    );
    for (suffix, bytes) in sidecars {
        let sidecar_path =
            doctor_sqlite_sidecar_path(&candidate_db, suffix).expect("candidate sidecar path");
        std::fs::write(&sidecar_path, bytes).expect("write candidate sidecar");
        checksum_set.insert(
            format!("database/candidate.db{suffix}"),
            serde_json::Value::String(blake3::hash(bytes).to_hex().to_string()),
        );
    }
    let lexical_metadata = serde_json::json!({
        "schema_version": 1,
        "metadata_kind": "cass_doctor_candidate_lexical_metadata_v1",
        "candidate_id": candidate_id,
        "candidate_archive_db": "database/candidate.db",
        "not_live_index": true,
    });
    let lexical_metadata_bytes =
        serde_json::to_vec_pretty(&lexical_metadata).expect("lexical metadata json");
    std::fs::write(
        candidate_dir.join("index/lexical/candidate-generation.json"),
        &lexical_metadata_bytes,
    )
    .expect("write candidate lexical metadata");
    checksum_set.insert(
        "index/lexical/candidate-generation.json".to_string(),
        serde_json::Value::String(blake3::hash(&lexical_metadata_bytes).to_hex().to_string()),
    );
    let semantic_metadata = serde_json::json!({
        "schema_version": 1,
        "metadata_kind": "cass_doctor_candidate_semantic_metadata_v1",
        "candidate_id": candidate_id,
        "semantic_vectors_built": false,
        "not_live_index": true,
    });
    let semantic_metadata_bytes =
        serde_json::to_vec_pretty(&semantic_metadata).expect("semantic metadata json");
    std::fs::write(
        candidate_dir.join("index/semantic/metadata.json"),
        &semantic_metadata_bytes,
    )
    .expect("write candidate semantic metadata");
    checksum_set.insert(
        "index/semantic/metadata.json".to_string(),
        serde_json::Value::String(blake3::hash(&semantic_metadata_bytes).to_hex().to_string()),
    );
    let live_inventory = doctor_candidate_live_inventory(db_path, index_path);
    let manifest_path = candidate_dir.join("manifest.json");
    let artifact_count = checksum_set.len();
    let manifest = serde_json::json!({
        "schema_version": DOCTOR_CANDIDATE_SCHEMA_VERSION,
        "manifest_kind": DOCTOR_CANDIDATE_MANIFEST_KIND,
        "candidate_id": candidate_id,
        "lifecycle_status": "completed",
        "created_at_ms": 1_700_000_000_000i64,
        "updated_at_ms": 1_700_000_000_100i64,
        "staging_root": candidate_dir.display().to_string(),
        "manifest_path": manifest_path.display().to_string(),
        "artifact_count": artifact_count,
        "checksum_set": checksum_set,
        "coverage_gate": {
            "schema_version": 1,
            "status": if promote_allowed { "pass" } else { "blocked" },
            "promote_allowed": promote_allowed,
            "safe_to_inspect": true,
            "blocking_reasons": if promote_allowed {
                serde_json::json!([])
            } else {
                serde_json::json!(["fixture blocks promotion"])
            },
        },
        "live_inventory_before": live_inventory,
        "live_inventory_after": live_inventory,
        "live_inventory_unchanged": true,
    });
    std::fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).expect("manifest json"),
    )
    .expect("write candidate manifest");
    manifest_path
}

pub(crate) fn doctor_test_operation_state(
    data_dir: &Path,
    db_path: &Path,
    index_path: &Path,
) -> DoctorOperationStateReport {
    build_doctor_operation_state_report(
        data_dir,
        db_path,
        index_path,
        &crate::search::asset_state::SearchMaintenanceSnapshot::default(),
        &DoctorMutationLockObservation::Absent {
            path: data_dir.join("doctor/locks/doctor-repair.lock"),
        },
    )
}

pub(crate) fn doctor_test_repair_plan_for_candidate_staging(
    data_dir: &Path,
    db_path: &Path,
    index_path: &Path,
    candidate_staging: &DoctorCandidateStagingReport,
    db_ok: bool,
    canonical_candidate_replacement_authorized: bool,
    needs_rebuild: bool,
) -> DoctorRepairPlanPreviewReport {
    doctor_test_repair_plan_for_candidate_staging_with_queryability(
        data_dir,
        db_path,
        index_path,
        candidate_staging,
        db_ok,
        db_ok,
        canonical_candidate_replacement_authorized,
        true,
        needs_rebuild,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn doctor_test_repair_plan_for_candidate_staging_with_queryability(
    data_dir: &Path,
    db_path: &Path,
    index_path: &Path,
    candidate_staging: &DoctorCandidateStagingReport,
    db_ok: bool,
    canonical_archive_queryable_for_non_destructive_derived_rebuild: bool,
    canonical_candidate_replacement_authorized: bool,
    archive_wide_collectors_complete: bool,
    needs_rebuild: bool,
) -> DoctorRepairPlanPreviewReport {
    build_doctor_repair_plan_preview(
        data_dir,
        db_path,
        index_path,
        Some(RobotFormat::Json),
        false,
        false,
        true,
        false,
        None,
        &doctor_test_operation_state(data_dir, db_path, index_path),
        &doctor_failure_marker_absent_report("doctor-repair"),
        needs_rebuild,
        db_ok,
        canonical_archive_queryable_for_non_destructive_derived_rebuild,
        canonical_candidate_replacement_authorized,
        archive_wide_collectors_complete,
        archive_wide_collectors_complete,
        None,
        &DoctorRawMirrorBackfillReport::default(),
        &DoctorCoverageSummary::default(),
        &DoctorCoverageRiskSummary::default(),
        &doctor_test_source_authority_report(),
        candidate_staging,
    )
}

pub(crate) fn doctor_test_source_authority_report() -> DoctorSourceAuthorityReport {
    DoctorSourceAuthorityReport {
        schema_version: 1,
        decision: DoctorSourceAuthorityDecision::CandidateOnly,
        selected_authority: Some(DoctorSourceAuthorityKind::VerifiedRawMirror),
        selected_authorities: Vec::new(),
        rejected_authorities: Vec::new(),
        coverage_delta: DoctorSourceAuthorityCoverageDelta::default(),
        freshness_delta: DoctorSourceAuthorityFreshnessDelta::default(),
        checksum_evidence: DoctorSourceAuthorityChecksumEvidence {
            raw_mirror_status: "verified".to_string(),
            raw_mirror_manifest_count: 1,
            raw_mirror_verified_blob_count: 1,
            raw_mirror_missing_blob_count: 0,
            raw_mirror_checksum_mismatch_count: 0,
            raw_mirror_manifest_checksum_mismatch_count: 0,
            raw_mirror_manifest_checksum_not_recorded_count: 0,
            raw_mirror_invalid_manifest_count: 0,
            summary_status: DoctorArtifactChecksumStatus::Matched,
        },
        matrix: Vec::new(),
        notes: Vec::new(),
    }
}

pub(crate) fn raw_mirror_test_manifest(
    data_dir: &Path,
    provider: &str,
    source_id: &str,
    original_path: &Path,
    bytes: &[u8],
    db_links: Vec<DoctorRawMirrorDbLink>,
) -> DoctorRawMirrorManifestFile {
    raw_mirror_test_manifest_with_origin(
        data_dir,
        provider,
        source_id,
        "local",
        None,
        original_path,
        bytes,
        db_links,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn raw_mirror_test_manifest_with_origin(
    data_dir: &Path,
    provider: &str,
    source_id: &str,
    origin_kind: &str,
    origin_host: Option<&str>,
    original_path: &Path,
    bytes: &[u8],
    db_links: Vec<DoctorRawMirrorDbLink>,
) -> DoctorRawMirrorManifestFile {
    let blob_blake3 = blake3::hash(bytes).to_hex().to_string();
    let original_path = original_path.display().to_string();
    let original_path_blake3 = doctor_raw_mirror_original_path_blake3(&original_path);
    let manifest_id = doctor_raw_mirror_manifest_id(
        provider,
        source_id,
        origin_kind,
        origin_host,
        &original_path_blake3,
        &blob_blake3,
    );
    let mut manifest = DoctorRawMirrorManifestFile {
        schema_version: DOCTOR_RAW_MIRROR_SCHEMA_VERSION,
        manifest_kind: DOCTOR_RAW_MIRROR_MANIFEST_KIND.to_string(),
        manifest_id,
        blob_hash_algorithm: DOCTOR_RAW_MIRROR_HASH_ALGORITHM.to_string(),
        blob_relative_path: doctor_raw_mirror_blob_relative_path(&blob_blake3)
            .expect("valid blob path"),
        blob_size_bytes: bytes.len() as u64,
        blob_blake3,
        provider: provider.to_string(),
        source_id: source_id.to_string(),
        origin_kind: origin_kind.to_string(),
        origin_host: origin_host.map(str::to_string),
        original_path: original_path.clone(),
        redacted_original_path: doctor_redacted_path(&original_path, data_dir),
        original_path_blake3,
        captured_at_ms: 1_733_000_000_000,
        source_mtime_ms: Some(1_733_000_000_000),
        source_size_bytes: Some(bytes.len() as u64),
        content_storage: None,
        compression: DoctorRawMirrorCompressionEnvelope {
            state: "none".to_string(),
            algorithm: None,
            uncompressed_size_bytes: Some(bytes.len() as u64),
        },
        encryption: DoctorRawMirrorEncryptionEnvelope {
            state: "none".to_string(),
            algorithm: None,
            key_id: None,
            envelope_version: None,
        },
        db_links,
        verification: DoctorRawMirrorVerificationRecord {
            status: "captured".to_string(),
            verifier: "test-fixture".to_string(),
            content_blake3: None,
            verified_at_ms: None,
        },
        manifest_blake3: None,
    };
    manifest.manifest_blake3 = Some(doctor_raw_mirror_manifest_blake3(&manifest));
    manifest
}

pub(crate) fn write_raw_mirror_test_manifest(
    data_dir: &Path,
    manifest: &DoctorRawMirrorManifestFile,
    bytes: &[u8],
) -> (PathBuf, PathBuf) {
    let root = doctor_raw_mirror_root(data_dir);
    let blob_path = root.join(&manifest.blob_relative_path);
    std::fs::create_dir_all(blob_path.parent().expect("blob parent")).expect("create blob parent");
    std::fs::write(&blob_path, bytes).expect("write raw mirror blob");
    let manifest_path = root.join(doctor_raw_mirror_manifest_relative_path(
        &manifest.manifest_id,
    ));
    std::fs::create_dir_all(manifest_path.parent().expect("manifest parent"))
        .expect("create manifest parent");
    std::fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(manifest).expect("manifest json"),
    )
    .expect("write raw mirror manifest");
    (blob_path, manifest_path)
}
