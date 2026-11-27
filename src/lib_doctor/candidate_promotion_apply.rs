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

fn fail_candidate_promotion_report(
    mut report: DoctorCandidatePromotionReport,
    err: String,
    db_path: &Path,
    index_path: &Path,
) -> DoctorCandidatePromotionReport {
    report.status = "failed".to_string();
    report.blocked_reasons.push(err);
    report.live_inventory_after = doctor_candidate_live_inventory(db_path, index_path);
    doctor_candidate_promotion_finalize_report(&mut report);
    report
}

#[allow(dead_code)]
pub(crate) fn promote_doctor_reconstruct_candidate_bundle(
    data_dir: &Path,
    db_path: &Path,
    index_path: &Path,
    candidate_dir: &Path,
) -> DoctorCandidatePromotionReport {
    let created_at_ms = doctor_now_ms();
    let manifest_path = candidate_dir.join("manifest.json");
    let fallback_candidate_id = candidate_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("unknown-candidate")
        .to_string();
    let mut report = DoctorCandidatePromotionReport {
        schema_version: DOCTOR_CANDIDATE_SCHEMA_VERSION,
        candidate_id: fallback_candidate_id,
        requested: true,
        status: "blocked".to_string(),
        candidate_path: candidate_dir.display().to_string(),
        redacted_candidate_path: doctor_redacted_path(&candidate_dir.display().to_string(), data_dir),
        manifest_path: manifest_path.display().to_string(),
        redacted_manifest_path: doctor_redacted_path(&manifest_path.display().to_string(), data_dir),
        reader_consistency_guarantee: "blocked-before-mutation".to_string(),
        derived_assets_consistency_status: "not-evaluated".to_string(),
        derived_lexical_followup_status: "not-evaluated".to_string(),
        derived_semantic_followup_status: "not-evaluated".to_string(),
        derived_vector_followup_status: "not-evaluated".to_string(),
        derived_memo_followup_status: "not-evaluated".to_string(),
        notes: vec![
            "Promotion preserves a backup copy before replacing the live archive.".to_string(),
            "WAL/SHM sidecars are treated as part of the SQLite bundle; every live sidecar must have a verified candidate replacement before promotion can proceed.".to_string(),
        ],
        ..DoctorCandidatePromotionReport::default()
    };
    let initial_candidate_id = report.candidate_id.clone();
    let mut promotion_dir = doctor_candidate_promotion_set_location(
        &mut report,
        data_dir,
        &initial_candidate_id,
        created_at_ms,
    );

    let manifest = match doctor_candidate_manifest_value(candidate_dir) {
        Ok(manifest) => manifest,
        Err(err) => {
            report.blocked_reasons.push(err);
            report.live_inventory_after = doctor_candidate_live_inventory(db_path, index_path);
            doctor_candidate_promotion_finalize_report(&mut report);
            return report;
        }
    };
    if let Some(candidate_id) = manifest
        .get("candidate_id")
        .and_then(serde_json::Value::as_str)
    {
        report.candidate_id = candidate_id.to_string();
        promotion_dir = doctor_candidate_promotion_set_location(
            &mut report,
            data_dir,
            candidate_id,
            created_at_ms,
        );
    }

    let expected_live_inventory =
        doctor_candidate_live_inventory_from_value(manifest.get("live_inventory_after"));
    report.expected_live_inventory = expected_live_inventory.clone();
    let live_inventory_before = doctor_candidate_live_inventory(db_path, index_path);
    report.live_inventory_before = live_inventory_before.clone();
    report.coverage_gate_status = manifest
        .pointer("/coverage_gate/status")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    report.coverage_promote_allowed = manifest
        .pointer("/coverage_gate/promote_allowed")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    report.plan_fingerprint = doctor_canonical_blake3(
        "doctor-candidate-promotion-plan-v1",
        serde_json::json!({
            "candidate_id": &report.candidate_id,
            "manifest_blake3": file_blake3_hex(&manifest_path).ok(),
            "expected_live_inventory": &expected_live_inventory,
            "current_live_inventory": &live_inventory_before,
            "coverage_gate_status": &report.coverage_gate_status,
            "coverage_promote_allowed": report.coverage_promote_allowed,
        }),
    );

    let lifecycle_status = manifest
        .get("lifecycle_status")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    if lifecycle_status != "completed" {
        report.blocked_reasons.push(format!(
            "candidate lifecycle_status={lifecycle_status} is not completed"
        ));
    }
    if !report.coverage_promote_allowed {
        report
            .blocked_reasons
            .push("candidate coverage gate does not allow promotion".to_string());
    }
    if manifest
        .get("live_inventory_unchanged")
        .and_then(serde_json::Value::as_bool)
        != Some(true)
    {
        report.blocked_reasons.push(
            "candidate manifest did not prove live inventory remained unchanged during staging"
                .to_string(),
        );
    }
    let expected_live_value = serde_json::to_value(&expected_live_inventory).unwrap_or_default();
    let current_live_value = serde_json::to_value(&live_inventory_before).unwrap_or_default();
    if expected_live_value != current_live_value {
        report.blocked_reasons.push(
            "live archive or index inventory changed after candidate staging; rerun doctor before promotion"
                .to_string(),
        );
    }

    let candidate_db_path = candidate_dir.join("database").join("candidate.db");
    let backup_dir = PathBuf::from(&report.backup_dir);
    let mut bundle_items = doctor_candidate_promotion_bundle_items(
        db_path,
        &candidate_db_path,
        &backup_dir,
        &manifest,
        &live_inventory_before,
    );
    let candidate_checksums = manifest
        .get("checksum_set")
        .and_then(serde_json::Value::as_object);
    for item in &mut bundle_items {
        if item.suffix != "-shm" {
            let recorded = candidate_checksums
                .is_some_and(|checksums| checksums.contains_key(&item.manifest_relative_path));
            if recorded && !item.candidate_exists {
                report.blocked_reasons.push(format!(
                    "candidate sqlite bundle component {} is recorded in the completed manifest but is missing",
                    item.manifest_relative_path
                ));
                continue;
            }
            if item.candidate_exists && item.expected_candidate_blake3.is_none() {
                report.blocked_reasons.push(format!(
                    "candidate sqlite bundle component {} has no checksum in the completed manifest",
                    item.manifest_relative_path
                ));
                continue;
            }
        }
        if item.suffix.is_empty() && !item.candidate_exists {
            report.blocked_reasons.push(format!(
                "candidate archive bundle is missing required DB component {}",
                item.source_path.display()
            ));
            continue;
        }
        if item.live_exists && !item.candidate_exists {
            // A live `-shm` without a candidate replacement is legitimate:
            // SQLite's shm lifecycle creates one on any
            // WAL-mode open, but shm is a transient shared-memory index that
            // SQLite rebuilds on the next open — a candidate bundle never
            // needs to ship one. The stale live copy is backed up and then
            // removed after promotion so it cannot pair with the new DB.
            // A live `-wal` without replacement stays fatal: WAL holds data.
            if item.suffix == "-shm" {
                report.warnings.push(format!(
                    "live shm sidecar {} has no candidate replacement; it will be backed up and removed at promotion (shm is rebuilt on next open)",
                    item.target_path.display()
                ));
                continue;
            }
            report.blocked_reasons.push(format!(
                "live sqlite bundle component {} exists but candidate replacement {} is missing",
                item.target_path.display(),
                item.source_path.display()
            ));
            continue;
        }
        if !item.candidate_exists {
            continue;
        }
        if !doctor_staged_source_path_is_safe(&item.source_path, candidate_dir, data_dir) {
            report.blocked_reasons.push(format!(
                "candidate sqlite bundle component is not safely confined to staging root: {}",
                item.source_path.display()
            ));
            continue;
        }
        match std::fs::metadata(&item.source_path) {
            Ok(metadata) => item.candidate_bytes = metadata.len(),
            Err(err) => {
                report.blocked_reasons.push(format!(
                    "failed to inspect candidate sqlite bundle component {}: {err}",
                    item.source_path.display()
                ));
                continue;
            }
        }
        match file_blake3_hex(&item.source_path) {
            Ok(hash) => {
                if let Some(expected) = item.expected_candidate_blake3.as_deref()
                    && expected != hash
                {
                    report.blocked_reasons.push(format!(
                        "candidate sqlite bundle component {} checksum drifted after staging: expected {expected}, observed {hash}",
                        item.manifest_relative_path
                    ));
                }
                item.candidate_blake3 = Some(hash);
            }
            Err(err) => report.blocked_reasons.push(err),
        }
    }
    report.sidecar_bundle_complete = bundle_items
        .iter()
        .filter(|item| item.asset_class == DoctorAssetClass::ArchiveDbSidecar)
        // `-shm` is transient and excluded from the completeness contract
        // (see the per-item warning above); only WAL carries data.
        .filter(|item| item.suffix != "-shm")
        .all(|item| {
            if item.candidate_exists {
                item.candidate_blake3.is_some()
                    && item.candidate_blake3 == item.expected_candidate_blake3
            } else {
                !item.live_exists
                    && !candidate_checksums.is_some_and(|checksums| {
                        checksums.contains_key(&item.manifest_relative_path)
                    })
            }
        });
    if !report.sidecar_bundle_complete {
        report.blocked_reasons.push(
            "sqlite sidecar bundle is incomplete; candidate WAL must match the completed manifest and replace any live WAL"
                .to_string(),
        );
    }
    if !doctor_staged_source_path_is_safe(&candidate_db_path, candidate_dir, data_dir) {
        report.blocked_reasons.push(format!(
            "candidate archive path is not safely confined to staging root: {}",
            candidate_db_path.display()
        ));
    }
    doctor_candidate_promotion_record_derived_metadata(
        &mut report,
        data_dir,
        candidate_dir,
        &manifest,
    );
    if path_has_symlink_below_root(candidate_dir, data_dir) {
        report.blocked_reasons.push(format!(
            "candidate directory has a symlinked ancestor below data dir: {}",
            candidate_dir.display()
        ));
    }
    let Some(db_parent) = db_path.parent() else {
        report.blocked_reasons.push(format!(
            "live archive path has no parent: {}",
            db_path.display()
        ));
        report.live_inventory_after = doctor_candidate_live_inventory(db_path, index_path);
        doctor_candidate_promotion_finalize_report(&mut report);
        return report;
    };
    if !db_parent.is_dir() {
        report.blocked_reasons.push(format!(
            "live archive parent is missing: {}",
            db_parent.display()
        ));
    }
    if !report.blocked_reasons.is_empty() {
        report.live_inventory_after = doctor_candidate_live_inventory(db_path, index_path);
        doctor_candidate_promotion_finalize_report(&mut report);
        return report;
    }

    let source_hash = bundle_items
        .iter()
        .find(|item| item.suffix.is_empty())
        .and_then(|item| item.candidate_blake3.clone())
        .unwrap_or_default();
    let candidate_wal_blake3 = bundle_items
        .iter()
        .find(|item| item.suffix == "-wal")
        .and_then(|item| item.candidate_blake3.as_deref());
    let (derived_status, lexical_rebuild_required, semantic_rebuild_required) =
        doctor_candidate_promotion_derived_status(
            &live_inventory_before,
            Some(&source_hash),
            candidate_wal_blake3,
        );
    report.derived_assets_consistency_status = derived_status;
    report.derived_lexical_rebuild_required = lexical_rebuild_required;
    report.derived_semantic_rebuild_required = semantic_rebuild_required;
    report.derived_lexical_followup_status = if lexical_rebuild_required {
        "rebuild-required".to_string()
    } else {
        "not-required".to_string()
    };
    report.derived_semantic_followup_status = if semantic_rebuild_required {
        "rebuild-or-lexical-fallback-required".to_string()
    } else {
        "not-required".to_string()
    };
    report.derived_vector_followup_status = if semantic_rebuild_required {
        "semantic-vector-rebuild-or-lexical-fallback-required".to_string()
    } else {
        "not-required".to_string()
    };
    report.derived_memo_followup_status = if lexical_rebuild_required || semantic_rebuild_required {
        "not-mutated-rebuildable-cache-does-not-block-archive-recovery".to_string()
    } else {
        "not-required".to_string()
    };

    if let Err(err) = doctor_forensic_create_private_dir_all(&backup_dir) {
        report.blocked_reasons.push(format!(
            "failed to create candidate promotion backup dir {}: {err}",
            backup_dir.display()
        ));
        report.live_inventory_after = doctor_candidate_live_inventory(db_path, index_path);
        doctor_candidate_promotion_finalize_report(&mut report);
        return report;
    }

    for item in &mut bundle_items {
        if item.candidate_exists {
            match doctor_copy_regular_file_to_private_target(
                &item.source_path,
                &item.candidate_backup_path,
                item.candidate_blake3.as_deref(),
                "candidate promotion candidate bundle backup",
            ) {
                Ok((bytes, hash)) => {
                    report.artifacts.push(doctor_candidate_promotion_artifact(
                        data_dir,
                        DoctorCandidatePromotionArtifactSpec {
                            artifact_kind: item.candidate_backup_artifact_kind,
                            asset_class: DoctorAssetClass::BackupBundle,
                            source_path: Some(&item.source_path),
                            backup_path: Some(&item.candidate_backup_path),
                            target_path: &item.target_path,
                            size_bytes: bytes,
                            checksum_blake3: Some(hash),
                        },
                    ));
                }
                Err(err) => {
                    return fail_candidate_promotion_report(report, err, db_path, index_path);
                }
            }
        }

        if item.live_exists {
            match doctor_copy_regular_file_to_private_target(
                &item.target_path,
                &item.live_backup_path,
                item.expected_live_blake3.as_deref(),
                "candidate promotion prior-live bundle backup",
            ) {
                Ok((bytes, hash)) => {
                    item.live_backup_blake3 = Some(hash.clone());
                    report.artifacts.push(doctor_candidate_promotion_artifact(
                        data_dir,
                        DoctorCandidatePromotionArtifactSpec {
                            artifact_kind: item.live_backup_artifact_kind,
                            asset_class: DoctorAssetClass::BackupBundle,
                            source_path: Some(&item.target_path),
                            backup_path: Some(&item.live_backup_path),
                            target_path: &item.target_path,
                            size_bytes: bytes,
                            checksum_blake3: Some(hash),
                        },
                    ));
                }
                Err(err) => {
                    return fail_candidate_promotion_report(report, err, db_path, index_path);
                }
            }
        }
    }
    let backup_manifest_path = report
        .backup_manifest_path
        .as_deref()
        .map(PathBuf::from)
        .unwrap_or_else(|| promotion_dir.join("backup").join("manifest.json"));
    let backup_manifest = serde_json::json!({
        "schema_version": 1,
        "manifest_kind": "cass_doctor_candidate_promotion_backup_manifest_v1",
        "promotion_id": &report.promotion_id,
        "candidate_id": &report.candidate_id,
        "candidate_path": &report.candidate_path,
        "redacted_candidate_path": &report.redacted_candidate_path,
        "backup_dir": &report.backup_dir,
        "redacted_backup_dir": &report.redacted_backup_dir,
        "plan_fingerprint": &report.plan_fingerprint,
        "coverage_gate_status": &report.coverage_gate_status,
        "coverage_promote_allowed": report.coverage_promote_allowed,
        "expected_live_inventory": &report.expected_live_inventory,
        "live_inventory_before": &report.live_inventory_before,
        "derived_assets_consistency_status": &report.derived_assets_consistency_status,
        "derived_lexical_rebuild_required": report.derived_lexical_rebuild_required,
        "derived_semantic_rebuild_required": report.derived_semantic_rebuild_required,
        "candidate_lexical_metadata_path": &report.candidate_lexical_metadata_path,
        "redacted_candidate_lexical_metadata_path": &report.redacted_candidate_lexical_metadata_path,
        "candidate_lexical_metadata_blake3": &report.candidate_lexical_metadata_blake3,
        "candidate_semantic_metadata_path": &report.candidate_semantic_metadata_path,
        "redacted_candidate_semantic_metadata_path": &report.redacted_candidate_semantic_metadata_path,
        "candidate_semantic_metadata_blake3": &report.candidate_semantic_metadata_blake3,
        "artifacts": &report.artifacts,
    });
    match doctor_write_private_json_artifact(
        &backup_manifest_path,
        &backup_manifest,
        "candidate promotion backup manifest",
    ) {
        Ok(hash) => {
            report.backup_manifest_path = Some(backup_manifest_path.display().to_string());
            report.redacted_backup_manifest_path = Some(doctor_redacted_path(
                &backup_manifest_path.display().to_string(),
                data_dir,
            ));
            report.backup_manifest_blake3 = Some(hash);
        }
        Err(err) => {
            report.status = "failed".to_string();
            report.blocked_reasons.push(err);
            report.live_inventory_after = doctor_candidate_live_inventory(db_path, index_path);
            doctor_candidate_promotion_finalize_report(&mut report);
            return report;
        }
    }

    let planned_bytes = bundle_items
        .iter()
        .filter(|item| item.candidate_exists)
        .map(|item| item.candidate_bytes)
        .sum();
    let action_id = doctor_fs_mutation_action_id(
        DoctorFsMutationKind::PromoteStagedFile,
        DoctorRepairMode::ReconstructPromote,
        DoctorAssetClass::CanonicalArchiveDb,
        db_path,
        planned_bytes,
    );
    let mut receipt = doctor_fs_mutation_receipt_for_request(&DoctorFsMutationRequest {
        operation_id: &report.promotion_id,
        action_id: &action_id,
        mutation_kind: DoctorFsMutationKind::PromoteStagedFile,
        mode: DoctorRepairMode::ReconstructPromote,
        asset_class: DoctorAssetClass::CanonicalArchiveDb,
        source_path: Some(&candidate_db_path),
        target_path: db_path,
        data_dir,
        db_path,
        index_path,
        staging_root: Some(candidate_dir),
        expected_source_blake3: Some(&source_hash),
        planned_bytes,
        required_min_age_seconds: None,
    });
    receipt.precondition_checks.extend([
        "candidate_manifest_completed".to_string(),
        "coverage_gate_promote_allowed".to_string(),
        "live_inventory_matched_manifest".to_string(),
        "sqlite_bundle_components_verified".to_string(),
        "prior_live_bundle_backup_preserved".to_string(),
        "candidate_bundle_backup_preserved".to_string(),
    ]);
    receipt.actual_source_blake3 = Some(source_hash.clone());
    receipt.fallback_kind = Some("none".to_string());

    let mut promoted_component_count = 0usize;
    let mut cross_device_fallback = false;
    for item in bundle_items.iter().filter(|item| item.candidate_exists) {
        match doctor_candidate_promotion_replace_file(
            &item.source_path,
            &item.target_path,
            item.candidate_blake3.as_deref().unwrap_or_default(),
            item.candidate_bytes,
            &report.promotion_id,
        ) {
            Ok(move_method) => {
                promoted_component_count += 1;
                if move_method == DoctorFsMoveMethod::CrossDeviceCopyRemoveSource {
                    cross_device_fallback = true;
                }
                receipt.precondition_checks.push(format!(
                    "{}:{}",
                    match move_method {
                        DoctorFsMoveMethod::AtomicRename => "filesystem_replace_completed",
                        DoctorFsMoveMethod::CrossDeviceCopyRemoveSource =>
                            "filesystem_cross_device_copy_replace_completed",
                    },
                    item.suffix
                ));
            }
            Err(err) => {
                receipt.status = DoctorActionStatus::Failed;
                receipt.blocked_reasons.push(err);
                break;
            }
        }

        match (
            file_blake3_hex(&item.target_path),
            item.candidate_blake3.as_deref(),
        ) {
            (Ok(hash), Some(expected)) if hash == expected => {
                if item.suffix.is_empty() {
                    receipt.actual_target_blake3 = Some(hash);
                }
                receipt
                    .precondition_checks
                    .push(format!("target_blake3_matched_source:{}", item.suffix));
                if let Err(err) = doctor_candidate_promotion_test_failpoint(
                    "after-component-replace",
                    &item.target_path,
                ) {
                    receipt.status = DoctorActionStatus::Failed;
                    receipt.blocked_reasons.push(err);
                    break;
                }
            }
            (Ok(hash), Some(expected)) => {
                receipt.status = DoctorActionStatus::Failed;
                receipt.blocked_reasons.push(format!(
                    "promoted sqlite bundle component {} checksum mismatch: expected {expected}, observed {hash}",
                    item.target_path.display()
                ));
                break;
            }
            (Ok(_), None) => {
                receipt.status = DoctorActionStatus::Failed;
                receipt.blocked_reasons.push(format!(
                    "promoted sqlite bundle component {} had no verified candidate checksum",
                    item.target_path.display()
                ));
                break;
            }
            (Err(err), _) => {
                receipt.status = DoctorActionStatus::Failed;
                receipt.blocked_reasons.push(err);
                break;
            }
        }
    }

    if cross_device_fallback {
        receipt.fallback_kind = Some("cross_device_copy_replace".to_string());
    }

    if receipt.status != DoctorActionStatus::Failed {
        receipt.affected_bytes = planned_bytes;
        receipt.precondition_checks.push(format!(
            "sqlite_bundle_component_replace_count={promoted_component_count}"
        ));
        for item in bundle_items.iter().filter(|item| item.candidate_exists) {
            if let Err(err) = sync_file(&item.target_path, "promoted sqlite bundle component") {
                receipt.status = DoctorActionStatus::Failed;
                receipt.blocked_reasons.push(err);
                break;
            }
        }
        // A live `-shm` with no candidate replacement was backed up above;
        // remove the stale copy now so the transient shared-memory index of
        // the REPLACED database cannot pair with the newly promoted one
        // (SQLite rebuilds shm on the next WAL-mode open).
        if receipt.status != DoctorActionStatus::Failed {
            let mut stale_shm_removed = false;
            for item in bundle_items
                .iter()
                .filter(|item| item.suffix == "-shm" && item.live_exists && !item.candidate_exists)
            {
                match std::fs::remove_file(&item.target_path) {
                    Ok(()) => {
                        stale_shm_removed = true;
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(err) => {
                        receipt.status = DoctorActionStatus::Failed;
                        receipt.blocked_reasons.push(format!(
                            "failed to remove stale live shm sidecar {} after backup: {err}",
                            item.target_path.display()
                        ));
                        break;
                    }
                }
            }
            if stale_shm_removed {
                receipt
                    .precondition_checks
                    .push("stale_live_shm_removed_after_backup:-shm".to_string());
            }
        }
        if receipt.status != DoctorActionStatus::Failed
            && let Err(err) =
                doctor_candidate_promotion_test_failpoint("before-parent-sync", db_parent)
        {
            receipt.status = DoctorActionStatus::Failed;
            receipt.blocked_reasons.push(err);
        }
        if receipt.status != DoctorActionStatus::Failed {
            match sync_directory(db_parent) {
                Ok(()) => {
                    receipt.status = DoctorActionStatus::Applied;
                    receipt
                        .precondition_checks
                        .push("target_bundle_parent_sync_completed".to_string());
                }
                Err(err) => {
                    receipt.status = DoctorActionStatus::Failed;
                    receipt.blocked_reasons.push(err);
                }
            }
        }
    }

    if receipt.status == DoctorActionStatus::Failed {
        match doctor_candidate_promotion_restore_live_bundle_from_backups(
            &bundle_items,
            &backup_dir,
            &report.promotion_id,
        ) {
            Ok(rollback_evidence) => {
                report.rollback_applied = true;
                report.rollback_reference = Some(if rollback_evidence.is_empty() {
                    backup_dir.join("live").display().to_string()
                } else {
                    rollback_evidence.join(";")
                });
                receipt
                    .precondition_checks
                    .push("rollback_restored_prior_live_sqlite_bundle".to_string());
            }
            Err(err) => report
                .warnings
                .push(format!("candidate promotion rollback failed: {err}")),
        }
    }

    for artifact in &mut report.artifacts {
        if artifact.artifact_kind.starts_with("candidate_archive_")
            && receipt.status == DoctorActionStatus::Applied
        {
            artifact.promoted_to_live = true;
        }
    }
    report.status = match receipt.status {
        DoctorActionStatus::Applied => {
            report.reader_consistency_guarantee = if bundle_items.iter().any(|item| {
                item.asset_class == DoctorAssetClass::ArchiveDbSidecar && item.candidate_exists
            }) {
                "sqlite-bundle-components-verified-sequentially-promoted-with-rollback-receipts"
                    .to_string()
            } else {
                "single-file-atomic-replace-no-sqlite-sidecars".to_string()
            };
            "applied".to_string()
        }
        DoctorActionStatus::Failed if report.rollback_applied => {
            report.reader_consistency_guarantee =
                "failed-promotion-rolled-back-to-prior-live-bundle-backup".to_string();
            "rolled_back".to_string()
        }
        DoctorActionStatus::Failed => "failed".to_string(),
        _ => "blocked".to_string(),
    };
    report.live_inventory_after = doctor_candidate_live_inventory(db_path, index_path);
    doctor_candidate_promotion_write_audit_artifacts(
        data_dir,
        &promotion_dir,
        &mut report,
        &receipt,
    );
    report.fs_mutation_receipts.push(receipt);
    doctor_candidate_promotion_finalize_report(&mut report);
    report
}
