use crate::doctor::candidate::build_doctor_reconstruct_candidate::doctor_candidate_promotion_root;
use crate::doctor::forensic::{
    doctor_forensic_create_private_dir_all, doctor_forensic_create_private_file, doctor_now_ms,
    doctor_redacted_path, doctor_sqlite_sidecar_path,
};
use crate::doctor::fs_safety::cleanup_apply::DoctorFsMutationReceipt;
use crate::doctor::fs_safety::doctor_copy_source_path_is_safe::doctor_staged_source_path_is_safe;
use crate::doctor::fs_safety::mutation_executor::{
    DoctorFsMoveMethod, doctor_is_cross_device_rename_error, doctor_rename_file, file_blake3_hex,
    sync_directory, sync_file,
};
use crate::doctor::policy::types::DoctorAssetClass;
use crate::doctor::policy::{
    DoctorActionStatus, DoctorEventDraft, DoctorEventLogMetadata, DoctorRepairMode,
};
use crate::doctor::repair_state::cliheartbeat::doctor_action_status_label;
use crate::doctor::repair_state::doctor_sorted_cleanup_actions::{
    doctor_event_log_from_events, push_doctor_event,
};
use crate::doctor::repair_state::types::CASS_TEST_DOCTOR_CANDIDATE_PROMOTION_FAILPOINT;
use crate::doctor::report::inventory::{
    DoctorCandidateLiveInventory, DoctorCandidatePromotionArtifact, DoctorCandidatePromotionReport,
};
use anyhow::Result;
use serde::Serialize;
use std::io;
use std::path::{Path, PathBuf};

pub(crate) fn doctor_candidate_promotion_id(candidate_id: &str, created_at_ms: i64) -> String {
    let safe_candidate_id = candidate_id
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>();
    format!("{created_at_ms}-{safe_candidate_id}")
}

pub(crate) fn doctor_candidate_promotion_unique_location(
    data_dir: &Path,
    base_promotion_id: &str,
) -> (String, PathBuf) {
    let root = doctor_candidate_promotion_root(data_dir);
    for attempt in 0..1000 {
        let promotion_id = if attempt == 0 {
            base_promotion_id.to_string()
        } else {
            format!("{base_promotion_id}-{attempt}")
        };
        let promotion_dir = root.join(&promotion_id);
        if !promotion_dir.exists() {
            return (promotion_id, promotion_dir);
        }
    }
    let promotion_id = format!("{base_promotion_id}-{}", doctor_now_ms());
    let promotion_dir = root.join(&promotion_id);
    (promotion_id, promotion_dir)
}

pub(crate) fn doctor_candidate_promotion_set_location(
    report: &mut DoctorCandidatePromotionReport,
    data_dir: &Path,
    candidate_id: &str,
    created_at_ms: i64,
) -> PathBuf {
    let base_promotion_id = doctor_candidate_promotion_id(candidate_id, created_at_ms);
    let (promotion_id, promotion_dir) =
        doctor_candidate_promotion_unique_location(data_dir, &base_promotion_id);
    report.promotion_id = promotion_id;
    report.backup_dir = promotion_dir.join("backup").display().to_string();
    report.redacted_backup_dir = doctor_redacted_path(&report.backup_dir, data_dir);
    report.backup_manifest_path = None;
    report.redacted_backup_manifest_path = None;
    report.receipt_path = promotion_dir.join("receipt.json").display().to_string();
    report.redacted_receipt_path = doctor_redacted_path(&report.receipt_path, data_dir);
    promotion_dir
}

pub(crate) fn doctor_candidate_live_inventory_from_value(
    value: Option<&serde_json::Value>,
) -> DoctorCandidateLiveInventory {
    let Some(value) = value else {
        return DoctorCandidateLiveInventory::default();
    };
    DoctorCandidateLiveInventory {
        db_exists: value
            .get("db_exists")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        db_size_bytes: value
            .get("db_size_bytes")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
        db_blake3: value
            .get("db_blake3")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        db_wal_exists: value
            .get("db_wal_exists")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        db_wal_size_bytes: value
            .get("db_wal_size_bytes")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
        db_wal_blake3: value
            .get("db_wal_blake3")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        db_shm_exists: value
            .get("db_shm_exists")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        db_shm_size_bytes: value
            .get("db_shm_size_bytes")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
        db_shm_blake3: value
            .get("db_shm_blake3")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        index_exists: value
            .get("index_exists")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        index_size_bytes: value
            .get("index_size_bytes")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
    }
}

pub(crate) fn doctor_candidate_manifest_checksum<'a>(
    manifest: &'a serde_json::Value,
    relative_path: &str,
) -> Option<&'a str> {
    manifest
        .get("checksum_set")
        .and_then(serde_json::Value::as_object)
        .and_then(|checksums| checksums.get(relative_path))
        .and_then(serde_json::Value::as_str)
}

pub(crate) fn doctor_candidate_manifest_value(
    candidate_dir: &Path,
) -> Result<serde_json::Value, String> {
    let manifest_path = candidate_dir.join("manifest.json");
    let raw = std::fs::read(&manifest_path).map_err(|err| {
        format!(
            "failed to read candidate manifest {}: {err}",
            manifest_path.display()
        )
    })?;
    serde_json::from_slice(&raw).map_err(|err| {
        format!(
            "failed to parse candidate manifest {}: {err}",
            manifest_path.display()
        )
    })
}

pub(crate) struct DoctorCandidatePromotionArtifactSpec<'a> {
    pub(crate) artifact_kind: &'a str,
    pub(crate) asset_class: DoctorAssetClass,
    pub(crate) source_path: Option<&'a Path>,
    pub(crate) backup_path: Option<&'a Path>,
    pub(crate) target_path: &'a Path,
    pub(crate) size_bytes: u64,
    pub(crate) checksum_blake3: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct DoctorCandidatePromotionBundleItem {
    pub(crate) suffix: &'static str,
    pub(crate) candidate_backup_artifact_kind: &'static str,
    pub(crate) live_backup_artifact_kind: &'static str,
    pub(crate) asset_class: DoctorAssetClass,
    pub(crate) source_path: PathBuf,
    pub(crate) target_path: PathBuf,
    pub(crate) candidate_backup_path: PathBuf,
    pub(crate) live_backup_path: PathBuf,
    pub(crate) manifest_relative_path: String,
    pub(crate) candidate_exists: bool,
    pub(crate) live_exists: bool,
    pub(crate) expected_candidate_blake3: Option<String>,
    pub(crate) expected_live_blake3: Option<String>,
    pub(crate) candidate_blake3: Option<String>,
    pub(crate) candidate_bytes: u64,
    pub(crate) live_backup_blake3: Option<String>,
}

pub(crate) fn doctor_candidate_promotion_artifact(
    data_dir: &Path,
    spec: DoctorCandidatePromotionArtifactSpec<'_>,
) -> DoctorCandidatePromotionArtifact {
    DoctorCandidatePromotionArtifact {
        artifact_kind: spec.artifact_kind.to_string(),
        asset_class: spec.asset_class,
        source_path: spec.source_path.map(|path| path.display().to_string()),
        redacted_source_path: spec
            .source_path
            .map(|path| doctor_redacted_path(&path.display().to_string(), data_dir)),
        backup_path: spec.backup_path.map(|path| path.display().to_string()),
        redacted_backup_path: spec
            .backup_path
            .map(|path| doctor_redacted_path(&path.display().to_string(), data_dir)),
        target_path: spec.target_path.display().to_string(),
        redacted_target_path: doctor_redacted_path(
            &spec.target_path.display().to_string(),
            data_dir,
        ),
        size_bytes: spec.size_bytes,
        checksum_blake3: spec.checksum_blake3,
        copied_to_backup: spec.backup_path.is_some(),
        promoted_to_live: false,
    }
}

pub(crate) fn doctor_candidate_live_inventory_component(
    inventory: &DoctorCandidateLiveInventory,
    suffix: &str,
) -> (bool, Option<String>) {
    match suffix {
        "" => (inventory.db_exists, inventory.db_blake3.clone()),
        "-wal" => (inventory.db_wal_exists, inventory.db_wal_blake3.clone()),
        "-shm" => (inventory.db_shm_exists, inventory.db_shm_blake3.clone()),
        _ => (false, None),
    }
}

pub(crate) fn doctor_candidate_promotion_bundle_items(
    db_path: &Path,
    candidate_db_path: &Path,
    backup_dir: &Path,
    manifest: &serde_json::Value,
    live_inventory: &DoctorCandidateLiveInventory,
) -> Vec<DoctorCandidatePromotionBundleItem> {
    [
        (
            "",
            "candidate_archive_db_backup",
            "prior_live_archive_db_backup",
            DoctorAssetClass::CanonicalArchiveDb,
        ),
        (
            "-wal",
            "candidate_archive_wal_backup",
            "prior_live_archive_wal_backup",
            DoctorAssetClass::ArchiveDbSidecar,
        ),
        (
            "-shm",
            "candidate_archive_shm_backup",
            "prior_live_archive_shm_backup",
            DoctorAssetClass::ArchiveDbSidecar,
        ),
    ]
    .into_iter()
    .map(
        |(suffix, candidate_backup_artifact_kind, live_backup_artifact_kind, asset_class)| {
            let source_path = if suffix.is_empty() {
                candidate_db_path.to_path_buf()
            } else {
                doctor_sqlite_sidecar_path(candidate_db_path, suffix).unwrap_or_else(|| {
                    candidate_db_path.with_file_name(format!("candidate.db{suffix}"))
                })
            };
            let target_path = if suffix.is_empty() {
                db_path.to_path_buf()
            } else {
                doctor_sqlite_sidecar_path(db_path, suffix)
                    .unwrap_or_else(|| db_path.with_file_name(format!("agent_search.db{suffix}")))
            };
            let candidate_backup_leaf = source_path
                .file_name()
                .unwrap_or_else(|| std::ffi::OsStr::new("candidate.db"));
            let live_backup_leaf = target_path
                .file_name()
                .unwrap_or_else(|| std::ffi::OsStr::new("agent_search.db"));
            let manifest_relative_path = if suffix.is_empty() {
                "database/candidate.db".to_string()
            } else {
                format!("database/candidate.db{suffix}")
            };
            let (live_exists, expected_live_blake3) =
                doctor_candidate_live_inventory_component(live_inventory, suffix);

            DoctorCandidatePromotionBundleItem {
                suffix,
                candidate_backup_artifact_kind,
                live_backup_artifact_kind,
                asset_class,
                candidate_exists: source_path.exists(),
                live_exists,
                expected_candidate_blake3: doctor_candidate_manifest_checksum(
                    manifest,
                    &manifest_relative_path,
                )
                .map(str::to_string),
                expected_live_blake3,
                candidate_backup_path: backup_dir.join("candidate").join(candidate_backup_leaf),
                live_backup_path: backup_dir.join("live").join(live_backup_leaf),
                manifest_relative_path,
                source_path,
                target_path,
                candidate_blake3: None,
                candidate_bytes: 0,
                live_backup_blake3: None,
            }
        },
    )
    .collect()
}

pub(crate) fn doctor_candidate_promotion_record_metadata_artifact(
    report: &mut DoctorCandidatePromotionReport,
    data_dir: &Path,
    candidate_dir: &Path,
    manifest: &serde_json::Value,
    relative_path: &str,
) {
    let path = candidate_dir.join(relative_path);
    if !path.exists() {
        report.warnings.push(format!(
            "candidate derived metadata artifact is absent: {relative_path}"
        ));
        return;
    }
    if !doctor_staged_source_path_is_safe(&path, candidate_dir, data_dir) {
        report.blocked_reasons.push(format!(
            "candidate derived metadata artifact is not safely confined to staging root: {}",
            path.display()
        ));
        return;
    }
    let hash = match file_blake3_hex(&path) {
        Ok(hash) => hash,
        Err(err) => {
            report.blocked_reasons.push(err);
            return;
        }
    };
    if let Some(expected) = doctor_candidate_manifest_checksum(manifest, relative_path)
        && expected != hash
    {
        report.blocked_reasons.push(format!(
            "candidate derived metadata artifact {relative_path} checksum drifted after staging: expected {expected}, observed {hash}"
        ));
        return;
    }

    let path_text = path.display().to_string();
    match relative_path {
        "index/lexical/candidate-generation.json" => {
            report.candidate_lexical_metadata_path = Some(path_text.clone());
            report.redacted_candidate_lexical_metadata_path =
                Some(doctor_redacted_path(&path_text, data_dir));
            report.candidate_lexical_metadata_blake3 = Some(hash);
        }
        "index/semantic/metadata.json" => {
            report.candidate_semantic_metadata_path = Some(path_text.clone());
            report.redacted_candidate_semantic_metadata_path =
                Some(doctor_redacted_path(&path_text, data_dir));
            report.candidate_semantic_metadata_blake3 = Some(hash);
        }
        _ => {}
    }
}

pub(crate) fn doctor_candidate_promotion_record_derived_metadata(
    report: &mut DoctorCandidatePromotionReport,
    data_dir: &Path,
    candidate_dir: &Path,
    manifest: &serde_json::Value,
) {
    for relative_path in [
        "index/lexical/candidate-generation.json",
        "index/semantic/metadata.json",
    ] {
        doctor_candidate_promotion_record_metadata_artifact(
            report,
            data_dir,
            candidate_dir,
            manifest,
            relative_path,
        );
    }
}

pub(crate) fn doctor_candidate_promotion_derived_status(
    live_inventory_before: &DoctorCandidateLiveInventory,
    candidate_db_blake3: Option<&str>,
    candidate_wal_blake3: Option<&str>,
) -> (String, bool, bool) {
    if !live_inventory_before.index_exists {
        return ("no-live-derived-index-to-promote".to_string(), false, false);
    }
    // Committed rows can live entirely in WAL while the checkpointed DB bytes
    // stay unchanged. SHM only indexes those WAL frames and is rebuildable.
    let same_wal = match (
        live_inventory_before.db_wal_exists,
        live_inventory_before.db_wal_blake3.as_deref(),
        candidate_wal_blake3,
    ) {
        (false, None, None) => true,
        (true, Some(live), Some(candidate)) => live == candidate,
        _ => false,
    };
    if live_inventory_before.db_blake3.as_deref().is_some()
        && live_inventory_before.db_blake3.as_deref() == candidate_db_blake3
        && same_wal
    {
        return (
            "live-derived-assets-reusable-same-archive-identity".to_string(),
            false,
            false,
        );
    }
    (
        "live-derived-assets-stale-after-archive-promotion-rebuild-required".to_string(),
        true,
        true,
    )
}

pub(crate) fn doctor_copy_regular_file_to_private_target(
    source_path: &Path,
    target_path: &Path,
    expected_blake3: Option<&str>,
    label: &str,
) -> Result<(u64, String), String> {
    let source_metadata = std::fs::symlink_metadata(source_path).map_err(|err| {
        format!(
            "failed to inspect {label} source {}: {err}",
            source_path.display()
        )
    })?;
    if !source_metadata.is_file() || source_metadata.file_type().is_symlink() {
        return Err(format!(
            "refusing to copy {label} source that is not a regular non-symlink file: {}",
            source_path.display()
        ));
    }
    let source_blake3 = file_blake3_hex(source_path)?;
    if let Some(expected) = expected_blake3
        && expected != source_blake3
    {
        return Err(format!(
            "refusing to copy {label} source {}: expected blake3 {expected}, observed {source_blake3}",
            source_path.display()
        ));
    }
    let Some(parent) = target_path.parent() else {
        return Err(format!(
            "refusing to copy {label} to target without parent {}",
            target_path.display()
        ));
    };
    doctor_forensic_create_private_dir_all(parent).map_err(|err| {
        format!(
            "failed to create {label} target parent {}: {err}",
            parent.display()
        )
    })?;
    let copied = (|| -> io::Result<u64> {
        let mut source = std::fs::File::open(source_path)?;
        let mut target = doctor_forensic_create_private_file(target_path)?;
        let copied = io::copy(&mut source, &mut target)?;
        use std::io::Write as _;
        target.flush()?;
        target.sync_all()?;
        Ok(copied)
    })()
    .map_err(|err| {
        format!(
            "failed to copy {label} {} to {}: {err}",
            source_path.display(),
            target_path.display()
        )
    })?;
    if copied != source_metadata.len() {
        return Err(format!(
            "{label} copy byte count mismatch for {}: copied {copied}, expected {}",
            source_path.display(),
            source_metadata.len()
        ));
    }
    let target_blake3 = file_blake3_hex(target_path)?;
    if target_blake3 != source_blake3 {
        return Err(format!(
            "{label} copy checksum mismatch for {}: source {source_blake3}, target {target_blake3}",
            source_path.display()
        ));
    }
    sync_directory(parent)?;
    Ok((copied, target_blake3))
}

pub(crate) fn doctor_write_private_json_artifact<T: Serialize + ?Sized>(
    path: &Path,
    value: &T,
    label: &str,
) -> Result<String, String> {
    let Some(parent) = path.parent() else {
        return Err(format!("{label} path has no parent: {}", path.display()));
    };
    doctor_forensic_create_private_dir_all(parent).map_err(|err| {
        format!(
            "failed to create {label} parent {}: {err}",
            parent.display()
        )
    })?;
    let encoded = serde_json::to_vec_pretty(value)
        .map_err(|err| format!("failed to encode {label}: {err}"))?;
    let mut file = doctor_forensic_create_private_file(path)
        .map_err(|err| format!("failed to create {label} {}: {err}", path.display()))?;
    use std::io::Write as _;
    file.write_all(&encoded)
        .map_err(|err| format!("failed to write {label} {}: {err}", path.display()))?;
    file.sync_all()
        .map_err(|err| format!("failed to sync {label} {}: {err}", path.display()))?;
    sync_directory(parent)?;
    file_blake3_hex(path)
}

pub(crate) fn doctor_candidate_promotion_write_receipt(
    report: &DoctorCandidatePromotionReport,
) -> Result<String, String> {
    doctor_write_private_json_artifact(
        &PathBuf::from(&report.receipt_path),
        report,
        "candidate promotion receipt",
    )
}

pub(crate) fn doctor_candidate_promotion_finalize_report(
    report: &mut DoctorCandidatePromotionReport,
) {
    if let Err(err) = doctor_candidate_promotion_write_receipt(report) {
        report.warnings.push(err);
    }
}

pub(crate) fn doctor_candidate_promotion_restore_live_from_backup(
    db_path: &Path,
    backup_db_path: &Path,
    expected_backup_blake3: &str,
    promotion_id: &str,
) -> Result<(), String> {
    let Some(parent) = db_path.parent() else {
        return Err(format!(
            "cannot roll back candidate promotion target without parent: {}",
            db_path.display()
        ));
    };
    let file_name = db_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("agent_search.db");
    let rollback_tmp = parent.join(format!(".{file_name}.{promotion_id}.rollback.tmp"));
    let (_, rollback_hash) = doctor_copy_regular_file_to_private_target(
        backup_db_path,
        &rollback_tmp,
        Some(expected_backup_blake3),
        "candidate promotion rollback",
    )?;
    if rollback_hash != expected_backup_blake3 {
        return Err(format!(
            "candidate promotion rollback temp hash mismatch: expected {expected_backup_blake3}, observed {rollback_hash}"
        ));
    }
    doctor_rename_file(&rollback_tmp, db_path).map_err(|err| {
        format!(
            "failed to atomically restore live archive from rollback temp {} to {}: {err}",
            rollback_tmp.display(),
            db_path.display()
        )
    })?;
    sync_file(db_path, "rolled back live archive")?;
    sync_directory(parent)
}

pub(crate) fn doctor_candidate_promotion_park_new_live_target(
    target_path: &Path,
    backup_dir: &Path,
    promotion_id: &str,
) -> Result<Option<String>, String> {
    if !target_path.exists() {
        return Ok(None);
    }
    let file_name = target_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("promoted-target");
    let parked_path = backup_dir
        .join("rollback-new")
        .join(format!("{promotion_id}-{file_name}"));
    let Some(parked_parent) = parked_path.parent() else {
        return Err(format!(
            "cannot park promoted target without parent: {}",
            parked_path.display()
        ));
    };
    doctor_forensic_create_private_dir_all(parked_parent).map_err(|err| {
        format!(
            "failed to create rollback park directory {}: {err}",
            parked_parent.display()
        )
    })?;
    doctor_rename_file(target_path, &parked_path).map_err(|err| {
        format!(
            "failed to park newly promoted target {} at {} during rollback: {err}",
            target_path.display(),
            parked_path.display()
        )
    })?;
    sync_file(&parked_path, "parked rolled-back promotion artifact")?;
    sync_directory(parked_parent)?;
    if let Some(target_parent) = target_path.parent() {
        sync_directory(target_parent)?;
    }
    Ok(Some(parked_path.display().to_string()))
}

pub(crate) fn doctor_candidate_promotion_restore_live_bundle_from_backups(
    bundle_items: &[DoctorCandidatePromotionBundleItem],
    backup_dir: &Path,
    promotion_id: &str,
) -> Result<Vec<String>, String> {
    let mut rollback_evidence = Vec::new();
    for item in bundle_items {
        if item.live_exists {
            let backup_hash = item.live_backup_blake3.as_deref().ok_or_else(|| {
                format!(
                    "cannot roll back {} without verified live backup hash at {}",
                    item.target_path.display(),
                    item.live_backup_path.display()
                )
            })?;
            doctor_candidate_promotion_restore_live_from_backup(
                &item.target_path,
                &item.live_backup_path,
                backup_hash,
                promotion_id,
            )?;
            rollback_evidence.push(format!(
                "restored-prior-live:{}",
                item.target_path.display()
            ));
        } else if let Some(parked_path) = doctor_candidate_promotion_park_new_live_target(
            &item.target_path,
            backup_dir,
            promotion_id,
        )? {
            rollback_evidence.push(format!("parked-new-live:{parked_path}"));
        }
    }
    Ok(rollback_evidence)
}

pub(crate) fn doctor_remove_candidate_replace_tmp(temp_path: &Path) -> Result<(), String> {
    match std::fs::remove_file(temp_path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(format!(
            "failed to remove partial candidate promotion replace temp {}: {err}",
            temp_path.display()
        )),
    }
}

pub(crate) fn doctor_candidate_promotion_test_failpoint(
    phase: &str,
    path: &Path,
) -> Result<(), String> {
    let Ok(raw) = dotenvy::var(CASS_TEST_DOCTOR_CANDIDATE_PROMOTION_FAILPOINT) else {
        return Ok(());
    };
    let matched = raw
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .any(|value| value == phase || value == "*" || value == "any");
    if matched {
        return Err(format!(
            "injected test candidate promotion failpoint {phase} for {}",
            path.display()
        ));
    }
    Ok(())
}

pub(crate) fn doctor_candidate_promotion_replace_file(
    source_path: &Path,
    target_path: &Path,
    expected_blake3: &str,
    expected_bytes: u64,
    promotion_id: &str,
) -> Result<DoctorFsMoveMethod, String> {
    match doctor_rename_file(source_path, target_path) {
        Ok(()) => return Ok(DoctorFsMoveMethod::AtomicRename),
        Err(err) if !doctor_is_cross_device_rename_error(&err) => {
            return Err(format!(
                "failed to atomically promote sqlite bundle component {} to {}: {err}",
                source_path.display(),
                target_path.display()
            ));
        }
        Err(_) => {}
    }

    let Some(target_parent) = target_path.parent() else {
        return Err(format!(
            "cannot cross-device promote sqlite bundle component without target parent: {}",
            target_path.display()
        ));
    };
    let target_leaf = target_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("promoted-target");
    let temp_path = target_parent.join(format!(
        ".{target_leaf}.{promotion_id}.cross-device-replace.tmp"
    ));

    let (copied_bytes, copied_hash) = match doctor_copy_regular_file_to_private_target(
        source_path,
        &temp_path,
        Some(expected_blake3),
        "candidate promotion cross-device replace temp",
    ) {
        Ok(result) => result,
        Err(err) => {
            if let Err(cleanup_err) = doctor_remove_candidate_replace_tmp(&temp_path) {
                return Err(format!("{err}; {cleanup_err}"));
            }
            return Err(err);
        }
    };
    if copied_bytes != expected_bytes {
        let err = format!(
            "candidate promotion cross-device replace temp copied {copied_bytes} byte(s), expected {expected_bytes}"
        );
        if let Err(cleanup_err) = doctor_remove_candidate_replace_tmp(&temp_path) {
            return Err(format!("{err}; {cleanup_err}"));
        }
        return Err(err);
    }
    if copied_hash != expected_blake3 {
        let err = format!(
            "candidate promotion cross-device replace temp hash mismatch: expected {expected_blake3}, observed {copied_hash}"
        );
        if let Err(cleanup_err) = doctor_remove_candidate_replace_tmp(&temp_path) {
            return Err(format!("{err}; {cleanup_err}"));
        }
        return Err(err);
    }

    doctor_rename_file(&temp_path, target_path).map_err(|err| {
        let cleanup_note = doctor_remove_candidate_replace_tmp(&temp_path)
            .err()
            .map(|cleanup_err| format!("; {cleanup_err}"))
            .unwrap_or_default();
        format!(
            "failed to install cross-device promoted sqlite bundle component {} at {}: {err}{cleanup_note}",
            temp_path.display(),
            target_path.display()
        )
    })?;
    sync_file(target_path, "cross-device promoted sqlite bundle component")?;
    sync_directory(target_parent)?;
    match std::fs::remove_file(source_path) {
        Ok(()) => {
            if let Some(source_parent) = source_path.parent() {
                sync_directory(source_parent)?;
            }
            Ok(DoctorFsMoveMethod::CrossDeviceCopyRemoveSource)
        }
        Err(err) => Err(format!(
            "failed to remove cross-device promoted candidate source {} after verified replacement: {err}",
            source_path.display()
        )),
    }
}

pub(crate) fn doctor_candidate_promotion_event_log(
    report: &DoctorCandidatePromotionReport,
    receipt: &DoctorFsMutationReceipt,
) -> DoctorEventLogMetadata {
    let mut events = Vec::new();
    push_doctor_event(
        &mut events,
        DoctorEventDraft {
            operation_id: report.promotion_id.clone(),
            action_id: None,
            phase: "operation_started".to_string(),
            mode: DoctorRepairMode::ReconstructPromote,
            asset_class: Some(DoctorAssetClass::CanonicalArchiveDb),
            redacted_target_path: None,
            elapsed_ms: None,
            progress_label: "candidate promotion started".to_string(),
            safety_gate_passed: Some(report.blocked_reasons.is_empty()),
            blocked_reasons: report.blocked_reasons.clone(),
            receipt_correlation_id: Some(receipt.action_id.clone()),
            artifact_ids: Vec::new(),
        },
    );
    push_doctor_event(
        &mut events,
        DoctorEventDraft {
            operation_id: report.promotion_id.clone(),
            action_id: Some(receipt.action_id.clone()),
            phase: "backup_bundle_preserved".to_string(),
            mode: DoctorRepairMode::ReconstructPromote,
            asset_class: Some(DoctorAssetClass::BackupBundle),
            redacted_target_path: Some(report.redacted_backup_dir.clone()),
            elapsed_ms: None,
            progress_label: "candidate and prior-live backup bundle preserved".to_string(),
            safety_gate_passed: Some(report.backup_manifest_blake3.is_some()),
            blocked_reasons: Vec::new(),
            receipt_correlation_id: Some(receipt.action_id.clone()),
            artifact_ids: report
                .artifacts
                .iter()
                .filter_map(|artifact| {
                    artifact
                        .checksum_blake3
                        .as_ref()
                        .map(|checksum| format!("{}:{checksum}", artifact.artifact_kind))
                })
                .collect(),
        },
    );
    push_doctor_event(
        &mut events,
        DoctorEventDraft {
            operation_id: report.promotion_id.clone(),
            action_id: Some(receipt.action_id.clone()),
            phase: format!("promotion_{}", doctor_action_status_label(receipt.status)),
            mode: DoctorRepairMode::ReconstructPromote,
            asset_class: Some(DoctorAssetClass::CanonicalArchiveDb),
            redacted_target_path: Some(receipt.redacted_target_path.clone()),
            elapsed_ms: None,
            progress_label: format!("candidate promotion {}", report.status),
            safety_gate_passed: Some(receipt.status == DoctorActionStatus::Applied),
            blocked_reasons: receipt.blocked_reasons.clone(),
            receipt_correlation_id: Some(receipt.action_id.clone()),
            artifact_ids: Vec::new(),
        },
    );
    doctor_event_log_from_events("candidate_promotion_embedded_events", events)
}

pub(crate) fn doctor_candidate_promotion_write_audit_artifacts(
    data_dir: &Path,
    promotion_dir: &Path,
    report: &mut DoctorCandidatePromotionReport,
    receipt: &DoctorFsMutationReceipt,
) {
    let event_log_path = promotion_dir.join("event-log.json");
    let mut event_log = doctor_candidate_promotion_event_log(report, receipt);
    event_log.path = Some(event_log_path.display().to_string());
    match doctor_write_private_json_artifact(
        &event_log_path,
        &event_log,
        "candidate promotion event log",
    ) {
        Ok(_) => report.event_log = event_log,
        Err(err) => report.warnings.push(err),
    }

    let marker_path = promotion_dir.join("promotion-marker.json");
    doctor_candidate_promotion_write_marker(data_dir, &marker_path, report);
}

pub(crate) fn doctor_candidate_promotion_write_marker(
    data_dir: &Path,
    marker_path: &Path,
    report: &mut DoctorCandidatePromotionReport,
) {
    let marker = serde_json::json!({
        "schema_version": 1,
        "manifest_kind": "cass_doctor_candidate_promotion_marker_v1",
        "promotion_id": &report.promotion_id,
        "candidate_id": &report.candidate_id,
        "status": &report.status,
        "plan_fingerprint": &report.plan_fingerprint,
        "coverage_gate_status": &report.coverage_gate_status,
        "coverage_promote_allowed": report.coverage_promote_allowed,
        "sidecar_bundle_complete": report.sidecar_bundle_complete,
        "reader_consistency_guarantee": &report.reader_consistency_guarantee,
        "derived_assets_consistency_status": &report.derived_assets_consistency_status,
        "derived_lexical_rebuild_required": report.derived_lexical_rebuild_required,
        "derived_semantic_rebuild_required": report.derived_semantic_rebuild_required,
        "derived_lexical_followup_status": &report.derived_lexical_followup_status,
        "derived_semantic_followup_status": &report.derived_semantic_followup_status,
        "derived_vector_followup_status": &report.derived_vector_followup_status,
        "derived_memo_followup_status": &report.derived_memo_followup_status,
        "derived_followup_artifact_path": &report.derived_followup_artifact_path,
        "redacted_derived_followup_artifact_path": &report.redacted_derived_followup_artifact_path,
        "derived_followup_artifact_blake3": &report.derived_followup_artifact_blake3,
        "candidate_lexical_metadata_path": &report.candidate_lexical_metadata_path,
        "redacted_candidate_lexical_metadata_path": &report.redacted_candidate_lexical_metadata_path,
        "candidate_lexical_metadata_blake3": &report.candidate_lexical_metadata_blake3,
        "candidate_semantic_metadata_path": &report.candidate_semantic_metadata_path,
        "redacted_candidate_semantic_metadata_path": &report.redacted_candidate_semantic_metadata_path,
        "candidate_semantic_metadata_blake3": &report.candidate_semantic_metadata_blake3,
        "backup_manifest_path": &report.backup_manifest_path,
        "backup_manifest_blake3": &report.backup_manifest_blake3,
        "receipt_path": &report.receipt_path,
        "event_log": &report.event_log,
        "rollback_reference": &report.rollback_reference,
        "rollback_applied": report.rollback_applied,
        "live_inventory_before": &report.live_inventory_before,
        "live_inventory_after": &report.live_inventory_after,
        "blocked_reasons": &report.blocked_reasons,
        "warnings": &report.warnings,
        "notes": &report.notes,
    });
    match doctor_write_private_json_artifact(marker_path, &marker, "candidate promotion marker") {
        Ok(hash) => {
            report.promotion_marker_path = Some(marker_path.display().to_string());
            report.redacted_promotion_marker_path = Some(doctor_redacted_path(
                &marker_path.display().to_string(),
                data_dir,
            ));
            report.promotion_marker_blake3 = Some(hash);
        }
        Err(err) => report.warnings.push(err),
    }
}

pub(crate) fn doctor_candidate_promotion_record_derived_followup_artifact(
    data_dir: &Path,
    report: &mut DoctorCandidatePromotionReport,
    lexical_message_count: usize,
    semantic_fallback_mode: &str,
) {
    let lexical_status_before = report.derived_lexical_followup_status.clone();
    let semantic_status_before = report.derived_semantic_followup_status.clone();
    let vector_status_before = report.derived_vector_followup_status.clone();
    let memo_status_before = report.derived_memo_followup_status.clone();

    if report.derived_lexical_rebuild_required {
        report.derived_lexical_rebuild_required = false;
        report.derived_lexical_followup_status = "rebuild-completed".to_string();
        report.notes.push(format!(
            "Derived lexical index rebuilt from promoted archive DB ({lexical_message_count} messages)."
        ));
    } else if report.derived_lexical_followup_status.is_empty()
        || report.derived_lexical_followup_status == "not-evaluated"
    {
        report.derived_lexical_followup_status = "not-required".to_string();
    }

    if report.derived_semantic_rebuild_required {
        if semantic_fallback_mode == "lexical" {
            report.derived_semantic_rebuild_required = false;
            report.derived_semantic_followup_status =
                "skipped-lexical-fallback-active-no-auto-download".to_string();
            report.derived_vector_followup_status =
                "skipped-lexical-fallback-active-no-auto-download".to_string();
            report.derived_memo_followup_status =
                "not-mutated-rebuildable-cache-does-not-block-archive-recovery".to_string();
            report.notes.push(
                "Derived semantic assets were not rebuilt automatically; lexical fallback remains active and model acquisition stays explicit."
                    .to_string(),
            );
        } else {
            report.derived_semantic_followup_status =
                "rebuild-still-required-no-auto-download".to_string();
            report.derived_vector_followup_status =
                "rebuild-still-required-no-auto-download".to_string();
            report.derived_memo_followup_status =
                "refresh-still-required-after-semantic-rebuild".to_string();
            report.notes.push(format!(
                "Derived semantic assets still need explicit rebuild or fallback handling; doctor did not auto-download models (fallback_mode={semantic_fallback_mode})."
            ));
        }
    } else if report.derived_semantic_followup_status.is_empty()
        || report.derived_semantic_followup_status == "not-evaluated"
    {
        report.derived_semantic_followup_status = "not-required".to_string();
    }
    if report.derived_vector_followup_status.is_empty()
        || report.derived_vector_followup_status == "not-evaluated"
    {
        report.derived_vector_followup_status = "not-required".to_string();
    }
    if report.derived_memo_followup_status.is_empty()
        || report.derived_memo_followup_status == "not-evaluated"
    {
        report.derived_memo_followup_status = "not-required".to_string();
    }

    report.derived_assets_consistency_status = match (
        report.derived_lexical_rebuild_required,
        report.derived_semantic_rebuild_required,
    ) {
        (false, false) => "promoted-archive-derived-followup-completed".to_string(),
        (false, true) => {
            "promoted-archive-lexical-rebuild-completed-semantic-followup-required".to_string()
        }
        (true, false) => "promoted-archive-lexical-followup-required".to_string(),
        (true, true) => "promoted-archive-derived-followup-required".to_string(),
    };

    let promotion_dir = PathBuf::from(&report.receipt_path)
        .parent()
        .map(Path::to_path_buf);
    let Some(promotion_dir) = promotion_dir else {
        report.warnings.push(format!(
            "cannot write candidate promotion derived follow-up artifact without receipt parent: {}",
            report.receipt_path
        ));
        return;
    };
    let artifact_path = promotion_dir.join("derived-followup.json");
    let artifact = serde_json::json!({
        "schema_version": 1,
        "manifest_kind": "cass_doctor_candidate_promotion_derived_followup_v1",
        "promotion_id": &report.promotion_id,
        "candidate_id": &report.candidate_id,
        "derived_assets_consistency_status": &report.derived_assets_consistency_status,
        "derived_lexical_rebuild_required": report.derived_lexical_rebuild_required,
        "derived_semantic_rebuild_required": report.derived_semantic_rebuild_required,
        "derived_lexical_followup_status_before": lexical_status_before,
        "derived_lexical_followup_status_after": &report.derived_lexical_followup_status,
        "derived_semantic_followup_status_before": semantic_status_before,
        "derived_semantic_followup_status_after": &report.derived_semantic_followup_status,
        "derived_vector_followup_status_before": vector_status_before,
        "derived_vector_followup_status_after": &report.derived_vector_followup_status,
        "derived_memo_followup_status_before": memo_status_before,
        "derived_memo_followup_status_after": &report.derived_memo_followup_status,
        "lexical_rebuild_message_count": lexical_message_count,
        "semantic_fallback_mode": semantic_fallback_mode,
        "network_allowed": false,
        "auto_download_attempted": false,
        "derived_asset_actions": {
            "semantic_model": {
                "status": &report.derived_semantic_followup_status,
                "network_allowed": false,
                "auto_download_attempted": false,
                "blocks_archive_recovery": false,
            },
            "semantic_vector_index": {
                "status": &report.derived_vector_followup_status,
                "network_allowed": false,
                "auto_download_attempted": false,
                "blocks_archive_recovery": false,
            },
            "memoization_cache": {
                "status": &report.derived_memo_followup_status,
                "network_allowed": false,
                "auto_download_attempted": false,
                "blocks_archive_recovery": false,
            },
        },
        "notes": &report.notes,
    });
    match doctor_write_private_json_artifact(
        &artifact_path,
        &artifact,
        "candidate promotion derived follow-up artifact",
    ) {
        Ok(hash) => {
            report.derived_followup_artifact_path = Some(artifact_path.display().to_string());
            report.redacted_derived_followup_artifact_path = Some(doctor_redacted_path(
                &artifact_path.display().to_string(),
                data_dir,
            ));
            report.derived_followup_artifact_blake3 = Some(hash);
        }
        Err(err) => report.warnings.push(err),
    }
}
