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

pub(crate) fn execute_doctor_fs_mutation(
    request: DoctorFsMutationRequest<'_>,
) -> DoctorFsMutationReceipt {
    let receipt = doctor_fs_mutation_receipt_for_request(&request);
    match request.mutation_kind {
        DoctorFsMutationKind::PruneCleanupTarget => {
            execute_doctor_prune_cleanup_target(request, receipt)
        }
        DoctorFsMutationKind::RemoveStaleLegacyIndexLock => {
            execute_doctor_remove_stale_legacy_index_lock(request, receipt)
        }
        DoctorFsMutationKind::CopyFileToStaging => {
            execute_doctor_copy_file_to_staging(request, receipt)
        }
        DoctorFsMutationKind::WriteFileToStaging => {
            let mut receipt = receipt;
            receipt.blocked_reasons.push(
                "write file mutations require execute_doctor_fs_write_mutation with an explicit payload"
                    .to_string(),
            );
            receipt
        }
        DoctorFsMutationKind::PromoteStagedFile => {
            execute_doctor_promote_staged_file(request, receipt)
        }
        DoctorFsMutationKind::RestoreStagedFile => {
            execute_doctor_restore_staged_file(request, receipt)
        }
        DoctorFsMutationKind::MoveFileToQuarantine => {
            execute_doctor_move_file_to_quarantine(request, receipt)
        }
    }
}

#[allow(dead_code)]
pub(crate) fn execute_doctor_fs_write_mutation(
    request: DoctorFsWriteMutationRequest<'_>,
) -> DoctorFsMutationReceipt {
    let mut receipt = DoctorFsMutationReceipt {
        schema_version: 1,
        operation_id: request.operation_id.to_string(),
        action_id: request.action_id.to_string(),
        mutation_kind: DoctorFsMutationKind::WriteFileToStaging,
        fallback_kind: None,
        mode: request.mode,
        asset_class: request.asset_class,
        source_path: None,
        redacted_source_path: None,
        target_path: request.target_path.display().to_string(),
        redacted_target_path: doctor_redacted_path(
            &request.target_path.display().to_string(),
            request.data_dir,
        ),
        staging_root: Some(request.staging_root.display().to_string()),
        redacted_staging_root: Some(doctor_redacted_path(
            &request.staging_root.display().to_string(),
            request.data_dir,
        )),
        expected_source_blake3: None,
        actual_source_blake3: None,
        actual_target_blake3: None,
        planned_bytes: request.planned_bytes,
        affected_bytes: 0,
        status: DoctorActionStatus::Blocked,
        blocked_reasons: Vec::new(),
        precondition_checks: vec!["mutation_kind_write_file_to_staging".to_string()],
        forensic_bundle: doctor_forensic_bundle_uncaptured(
            "not_captured_for_non_doctor_mutation_executor_call",
        ),
    };

    if !doctor_repair_mode_allows_asset_operation_mutation(
        request.mode,
        request.asset_class,
        DoctorFsMutationKind::WriteFileToStaging.asset_operation(),
    ) {
        receipt.blocked_reasons.push(format!(
            "refusing to write staging asset class {:?}: mode {:?} does not allow staging write mutation",
            request.asset_class, request.mode
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("mode_allows_asset_operation".to_string());

    let payload_len = request.payload.len() as u64;
    if request.planned_bytes != payload_len {
        receipt.blocked_reasons.push(format!(
            "refusing to write staging target {}: planned_bytes={} does not match payload_bytes={payload_len}",
            request.target_path.display(),
            request.planned_bytes
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("planned_bytes_match_payload".to_string());

    let payload_blake3 = blake3::hash(request.payload).to_hex().to_string();
    if request.expected_payload_blake3 != payload_blake3 {
        receipt.blocked_reasons.push(format!(
            "refusing to write staging target {}: expected payload blake3 {}, observed {payload_blake3}",
            request.target_path.display(),
            request.expected_payload_blake3
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("expected_payload_blake3_matched".to_string());

    let Some(target_parent) = request.target_path.parent() else {
        receipt.blocked_reasons.push(format!(
            "refusing to write staging target without parent {}",
            request.target_path.display()
        ));
        return receipt;
    };
    if !target_parent.is_dir() {
        receipt.blocked_reasons.push(format!(
            "refusing to write staging target with missing parent {}",
            target_parent.display()
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("target_parent_exists".to_string());

    if !doctor_staging_target_path_is_safe(
        request.target_path,
        request.staging_root,
        request.data_dir,
    ) {
        receipt.blocked_reasons.push(format!(
            "refusing to write unsafe staging target {}",
            request.target_path.display()
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("target_path_confined_to_staging_root".to_string());

    match std::fs::symlink_metadata(request.target_path) {
        Ok(_) => {
            receipt.blocked_reasons.push(format!(
                "refusing to write over existing staging target {}",
                request.target_path.display()
            ));
            return receipt;
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => {
            receipt.blocked_reasons.push(format!(
                "refusing to write staging target {}: could not verify target absence: {err}",
                request.target_path.display()
            ));
            return receipt;
        }
    }
    receipt
        .precondition_checks
        .push("target_does_not_exist".to_string());

    let write_result = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(request.target_path)
        .and_then(|mut file| {
            use std::io::Write as _;
            file.write_all(request.payload)
        });
    match write_result {
        Ok(()) => {
            receipt.affected_bytes = payload_len;
            receipt
                .precondition_checks
                .push("filesystem_write_completed".to_string());
        }
        Err(err) => {
            receipt.status = DoctorActionStatus::Failed;
            receipt.blocked_reasons.push(format!(
                "failed to write staging target {}: {err}",
                request.target_path.display()
            ));
            return receipt;
        }
    }

    let target_blake3 = match file_blake3_hex(request.target_path) {
        Ok(hash) => hash,
        Err(err) => {
            receipt.status = DoctorActionStatus::Failed;
            receipt.blocked_reasons.push(err);
            return receipt;
        }
    };
    receipt.actual_target_blake3 = Some(target_blake3.clone());
    if target_blake3 != payload_blake3 {
        receipt.status = DoctorActionStatus::Failed;
        receipt.blocked_reasons.push(format!(
            "written staging target {} hash mismatch: expected payload blake3 {payload_blake3}, observed target blake3 {target_blake3}",
            request.target_path.display()
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("target_blake3_matched_payload".to_string());

    match sync_file(request.target_path, "written staging target") {
        Ok(()) => {
            receipt
                .precondition_checks
                .push("target_file_sync_completed".to_string());
        }
        Err(err) => {
            receipt.status = DoctorActionStatus::Failed;
            receipt.blocked_reasons.push(err);
            return receipt;
        }
    }

    match sync_directory(target_parent) {
        Ok(()) => {
            receipt.status = DoctorActionStatus::Applied;
            receipt
                .precondition_checks
                .push("target_parent_sync_completed".to_string());
        }
        Err(err) => {
            receipt.status = DoctorActionStatus::Failed;
            receipt.blocked_reasons.push(err);
        }
    }
    receipt
}

pub(crate) fn execute_doctor_prune_cleanup_target(
    request: DoctorFsMutationRequest<'_>,
    mut receipt: DoctorFsMutationReceipt,
) -> DoctorFsMutationReceipt {
    receipt
        .precondition_checks
        .push("mutation_kind_prune_cleanup_target".to_string());

    if !doctor_repair_mode_allows_asset_operation_mutation(
        request.mode,
        request.asset_class,
        request.mutation_kind.asset_operation(),
    ) {
        receipt.blocked_reasons.push(format!(
            "refusing to prune asset class {:?}: cleanup_apply mode does not allow this mutation",
            request.asset_class
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("mode_allows_asset_class".to_string());

    if !doctor_asset_safe_to_gc(request.asset_class, true) {
        receipt.blocked_reasons.push(format!(
            "refusing to prune asset class {:?}: taxonomy does not allow automatic reclaim",
            request.asset_class
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("taxonomy_allows_automatic_reclaim".to_string());

    if !cleanup_target_path_is_safe(
        request.target_path,
        request.data_dir,
        request.db_path,
        request.index_path,
    ) {
        receipt.blocked_reasons.push(format!(
            "refusing to prune unsafe cleanup target {}",
            request.target_path.display()
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("path_confined_to_reclaimable_cleanup_target".to_string());

    let reclaimed_bytes = fs_dir_size(request.target_path);
    let remove_result = if request.target_path.is_dir() {
        std::fs::remove_dir_all(request.target_path)
    } else {
        std::fs::remove_file(request.target_path)
    };
    match remove_result {
        Ok(()) => {
            receipt.affected_bytes = reclaimed_bytes;
            receipt.status = DoctorActionStatus::Applied;
            receipt
                .precondition_checks
                .push("filesystem_remove_completed".to_string());
        }
        Err(err) => {
            receipt.status = DoctorActionStatus::Failed;
            receipt.blocked_reasons.push(format!(
                "failed to prune cleanup target {}: {err}",
                request.target_path.display()
            ));
        }
    }
    receipt
}

pub(crate) fn execute_doctor_remove_stale_legacy_index_lock(
    request: DoctorFsMutationRequest<'_>,
    mut receipt: DoctorFsMutationReceipt,
) -> DoctorFsMutationReceipt {
    receipt
        .precondition_checks
        .push("mutation_kind_remove_stale_legacy_index_lock".to_string());

    if !doctor_repair_mode_allows_asset_operation_mutation(
        request.mode,
        request.asset_class,
        request.mutation_kind.asset_operation(),
    ) {
        receipt.blocked_reasons.push(format!(
            "refusing to remove legacy index lock asset class {:?}: mode {:?} does not allow this mutation",
            request.asset_class, request.mode
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("mode_allows_asset_class".to_string());

    if request.asset_class != DoctorAssetClass::ReclaimableDerivedCache {
        receipt.blocked_reasons.push(format!(
            "refusing to remove legacy index lock as asset class {:?}: expected reclaimable derived cache",
            request.asset_class
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("asset_class_is_reclaimable_derived_cache".to_string());

    if !legacy_index_lock_path_is_safe(request.target_path, request.data_dir) {
        receipt.blocked_reasons.push(format!(
            "refusing to remove unsafe legacy index lock target {}",
            request.target_path.display()
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("path_confined_to_legacy_index_lock".to_string());

    if let Some(required_min_age_seconds) = request.required_min_age_seconds {
        let Some(age_seconds) = path_age_seconds(request.target_path) else {
            receipt.blocked_reasons.push(format!(
                "refusing to remove legacy index lock {}: could not verify file age",
                request.target_path.display()
            ));
            return receipt;
        };
        if age_seconds <= required_min_age_seconds {
            receipt.blocked_reasons.push(format!(
                "refusing to remove legacy index lock {}: age_seconds={age_seconds} is not greater than required_min_age_seconds={required_min_age_seconds}",
                request.target_path.display()
            ));
            return receipt;
        }
        receipt.precondition_checks.push(format!(
            "file_age_seconds_exceeds_{required_min_age_seconds}"
        ));
    }

    let reclaimed_bytes = fs_dir_size(request.target_path);
    match std::fs::remove_file(request.target_path) {
        Ok(()) => {
            receipt.affected_bytes = reclaimed_bytes;
            receipt.status = DoctorActionStatus::Applied;
            receipt
                .precondition_checks
                .push("filesystem_remove_completed".to_string());
        }
        Err(err) => {
            receipt.status = DoctorActionStatus::Failed;
            receipt.blocked_reasons.push(format!(
                "failed to remove legacy index lock {}: {err}",
                request.target_path.display()
            ));
        }
    }
    receipt
}

pub(crate) fn execute_doctor_copy_file_to_staging(
    request: DoctorFsMutationRequest<'_>,
    mut receipt: DoctorFsMutationReceipt,
) -> DoctorFsMutationReceipt {
    receipt
        .precondition_checks
        .push("mutation_kind_copy_file_to_staging".to_string());

    if !doctor_repair_mode_allows_asset_operation_mutation(
        request.mode,
        request.asset_class,
        request.mutation_kind.asset_operation(),
    ) {
        receipt.blocked_reasons.push(format!(
            "refusing to copy asset class {:?}: mode {:?} does not allow copy mutation",
            request.asset_class, request.mode
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("mode_allows_asset_operation".to_string());

    let Some(source_path) = request.source_path else {
        receipt
            .blocked_reasons
            .push("refusing to copy without a source path".to_string());
        return receipt;
    };
    let Some(staging_root) = request.staging_root else {
        receipt
            .blocked_reasons
            .push("refusing to copy without an approved staging root".to_string());
        return receipt;
    };

    if !doctor_copy_source_path_is_safe(source_path, request.data_dir) {
        receipt.blocked_reasons.push(format!(
            "refusing to copy unsafe source path {}",
            source_path.display()
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("source_path_confined_to_data_dir".to_string());

    let Some(parent) = request.target_path.parent() else {
        receipt.blocked_reasons.push(format!(
            "refusing to copy staging target without parent {}",
            request.target_path.display()
        ));
        return receipt;
    };
    if !parent.is_dir() {
        receipt.blocked_reasons.push(format!(
            "refusing to copy staging target with missing parent {}",
            parent.display()
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("target_parent_exists".to_string());

    if !doctor_staging_target_path_is_safe(request.target_path, staging_root, request.data_dir) {
        receipt.blocked_reasons.push(format!(
            "refusing to copy to unsafe staging target {}",
            request.target_path.display()
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("target_path_confined_to_staging_root".to_string());

    let source_blake3 = match file_blake3_hex(source_path) {
        Ok(hash) => hash,
        Err(err) => {
            receipt.status = DoctorActionStatus::Failed;
            receipt.blocked_reasons.push(err);
            return receipt;
        }
    };
    receipt.actual_source_blake3 = Some(source_blake3.clone());
    receipt
        .precondition_checks
        .push("source_blake3_recorded".to_string());

    match std::fs::symlink_metadata(request.target_path) {
        Ok(metadata) => {
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                receipt.blocked_reasons.push(format!(
                    "refusing to overwrite existing staging target {} because it is non-regular or a symlink",
                    request.target_path.display()
                ));
                return receipt;
            }
            // Content-addressed staging copies (e.g. raw-mirror blobs named by
            // their blake3) can legitimately collide when two distinct verified
            // manifests reference byte-identical evidence. An existing target
            // whose content already matches the source is an idempotent no-op,
            // not a conflict: record it as Applied so a single benign duplicate
            // does not poison the candidate lifecycle gate (which requires every
            // fs-mutation receipt to be Applied before a verified, promote-allowed
            // candidate can reach `completed`). A genuine content mismatch is still
            // refused as a conflict.
            match file_blake3_hex(request.target_path) {
                Ok(existing_blake3) if existing_blake3 == source_blake3 => {
                    receipt.actual_target_blake3 = Some(existing_blake3);
                    receipt.affected_bytes = 0;
                    receipt.status = DoctorActionStatus::Applied;
                    receipt
                        .precondition_checks
                        .push("existing_staging_target_content_matched_source".to_string());
                    return receipt;
                }
                Ok(existing_blake3) => {
                    receipt.blocked_reasons.push(format!(
                        "refusing to overwrite existing staging target {} with different content: existing blake3 {existing_blake3}, source blake3 {source_blake3}",
                        request.target_path.display()
                    ));
                    return receipt;
                }
                Err(err) => {
                    receipt.blocked_reasons.push(format!(
                        "refusing to overwrite existing staging target {}: could not verify existing content: {err}",
                        request.target_path.display()
                    ));
                    return receipt;
                }
            }
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => {
            receipt.blocked_reasons.push(format!(
                "refusing to copy staging target {}: could not verify target absence: {err}",
                request.target_path.display()
            ));
            return receipt;
        }
    }
    receipt
        .precondition_checks
        .push("target_does_not_exist".to_string());

    if let Some(expected) = request.expected_source_blake3
        && expected != source_blake3
    {
        receipt.blocked_reasons.push(format!(
            "refusing to copy source {}: expected blake3 {expected}, observed {source_blake3}",
            source_path.display()
        ));
        return receipt;
    }
    if request.expected_source_blake3.is_some() {
        receipt
            .precondition_checks
            .push("expected_source_blake3_matched".to_string());
    }

    match std::fs::copy(source_path, request.target_path) {
        Ok(bytes) => {
            receipt.affected_bytes = bytes;
            receipt
                .precondition_checks
                .push("filesystem_copy_completed".to_string());
        }
        Err(err) => {
            receipt.status = DoctorActionStatus::Failed;
            receipt.blocked_reasons.push(format!(
                "failed to copy {} to {}: {err}",
                source_path.display(),
                request.target_path.display()
            ));
            return receipt;
        }
    }

    let target_blake3 = match file_blake3_hex(request.target_path) {
        Ok(hash) => hash,
        Err(err) => {
            receipt.status = DoctorActionStatus::Failed;
            receipt.blocked_reasons.push(err);
            return receipt;
        }
    };
    receipt.actual_target_blake3 = Some(target_blake3.clone());
    if target_blake3 != source_blake3 {
        receipt.status = DoctorActionStatus::Failed;
        receipt.blocked_reasons.push(format!(
            "copied staging target {} hash mismatch: expected source blake3 {source_blake3}, observed target blake3 {target_blake3}",
            request.target_path.display()
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("target_blake3_matched_source".to_string());

    match sync_file(request.target_path, "copied staging target") {
        Ok(()) => {
            receipt
                .precondition_checks
                .push("target_file_sync_completed".to_string());
        }
        Err(err) => {
            receipt.status = DoctorActionStatus::Failed;
            receipt.blocked_reasons.push(err);
            return receipt;
        }
    }

    match sync_directory(parent) {
        Ok(()) => {
            receipt.status = DoctorActionStatus::Applied;
            receipt
                .precondition_checks
                .push("target_parent_sync_completed".to_string());
        }
        Err(err) => {
            receipt.status = DoctorActionStatus::Failed;
            receipt.blocked_reasons.push(err);
        }
    }
    receipt
}

pub(crate) fn execute_doctor_promote_staged_file(
    request: DoctorFsMutationRequest<'_>,
    mut receipt: DoctorFsMutationReceipt,
) -> DoctorFsMutationReceipt {
    receipt
        .precondition_checks
        .push("mutation_kind_promote_staged_file".to_string());

    if !doctor_repair_mode_allows_asset_operation_mutation(
        request.mode,
        request.asset_class,
        request.mutation_kind.asset_operation(),
    ) {
        receipt.blocked_reasons.push(format!(
            "refusing to promote asset class {:?}: mode {:?} does not allow promote mutation",
            request.asset_class, request.mode
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("mode_allows_asset_operation".to_string());

    let Some(source_path) = request.source_path else {
        receipt
            .blocked_reasons
            .push("refusing to promote without a staged source path".to_string());
        return receipt;
    };
    let Some(staging_root) = request.staging_root else {
        receipt
            .blocked_reasons
            .push("refusing to promote without an approved staging root".to_string());
        return receipt;
    };
    let Some(expected_source_blake3) = request.expected_source_blake3 else {
        receipt
            .blocked_reasons
            .push("refusing to promote without expected source blake3".to_string());
        return receipt;
    };

    if !doctor_staged_source_path_is_safe(source_path, staging_root, request.data_dir) {
        receipt.blocked_reasons.push(format!(
            "refusing to promote unsafe staged source path {}",
            source_path.display()
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("source_path_confined_to_staging_root".to_string());

    let Some(source_parent) = source_path.parent() else {
        receipt.blocked_reasons.push(format!(
            "refusing to promote source without parent {}",
            source_path.display()
        ));
        return receipt;
    };
    let Some(target_parent) = request.target_path.parent() else {
        receipt.blocked_reasons.push(format!(
            "refusing to promote target without parent {}",
            request.target_path.display()
        ));
        return receipt;
    };
    if !target_parent.is_dir() {
        receipt.blocked_reasons.push(format!(
            "refusing to promote target with missing parent {}",
            target_parent.display()
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("target_parent_exists".to_string());

    if !doctor_staged_rename_target_path_is_safe(
        request.target_path,
        staging_root,
        request.data_dir,
    ) {
        receipt.blocked_reasons.push(format!(
            "refusing to promote to unsafe target {}",
            request.target_path.display()
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("target_path_confined_to_data_dir".to_string());

    match std::fs::symlink_metadata(request.target_path) {
        Ok(_) => {
            receipt.blocked_reasons.push(format!(
                "refusing to promote over existing target {}",
                request.target_path.display()
            ));
            return receipt;
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => {
            receipt.blocked_reasons.push(format!(
                "refusing to promote target {}: could not verify target absence: {err}",
                request.target_path.display()
            ));
            return receipt;
        }
    }
    receipt
        .precondition_checks
        .push("target_does_not_exist".to_string());

    let source_blake3 = match file_blake3_hex(source_path) {
        Ok(hash) => hash,
        Err(err) => {
            receipt.status = DoctorActionStatus::Failed;
            receipt.blocked_reasons.push(err);
            return receipt;
        }
    };
    receipt.actual_source_blake3 = Some(source_blake3.clone());
    receipt
        .precondition_checks
        .push("source_blake3_recorded".to_string());

    if expected_source_blake3 != source_blake3 {
        receipt.blocked_reasons.push(format!(
            "refusing to promote source {}: expected blake3 {expected_source_blake3}, observed {source_blake3}",
            source_path.display()
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("expected_source_blake3_matched".to_string());

    let source_bytes = std::fs::metadata(source_path)
        .map(|metadata| metadata.len())
        .unwrap_or(request.planned_bytes);
    match doctor_move_file_with_cross_device_fallback(
        source_path,
        request.target_path,
        &source_blake3,
        source_bytes,
        "promote",
        "promoted target",
    ) {
        Ok(move_method) => {
            receipt.affected_bytes = source_bytes;
            receipt.fallback_kind = Some(move_method.receipt_fallback_kind().to_string());
            receipt
                .precondition_checks
                .push(move_method.receipt_precondition_check().to_string());
        }
        Err(err) => {
            receipt.status = DoctorActionStatus::Failed;
            receipt.blocked_reasons.push(err);
            return receipt;
        }
    }

    let target_blake3 = match file_blake3_hex(request.target_path) {
        Ok(hash) => hash,
        Err(err) => {
            receipt.status = DoctorActionStatus::Failed;
            receipt.blocked_reasons.push(err);
            return receipt;
        }
    };
    receipt.actual_target_blake3 = Some(target_blake3.clone());
    if target_blake3 != source_blake3 {
        receipt.status = DoctorActionStatus::Failed;
        receipt.blocked_reasons.push(format!(
            "promoted target {} hash mismatch: expected source blake3 {source_blake3}, observed target blake3 {target_blake3}",
            request.target_path.display()
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("target_blake3_matched_source".to_string());

    match sync_file(request.target_path, "promoted target") {
        Ok(()) => {
            receipt
                .precondition_checks
                .push("target_file_sync_completed".to_string());
        }
        Err(err) => {
            receipt.status = DoctorActionStatus::Failed;
            receipt.blocked_reasons.push(err);
            return receipt;
        }
    }

    match sync_directory(source_parent).and_then(|()| sync_directory(target_parent)) {
        Ok(()) => {
            receipt.status = DoctorActionStatus::Applied;
            receipt
                .precondition_checks
                .push("source_and_target_parent_sync_completed".to_string());
        }
        Err(err) => {
            receipt.status = DoctorActionStatus::Failed;
            receipt.blocked_reasons.push(err);
        }
    }
    receipt
}

pub(crate) fn execute_doctor_restore_staged_file(
    request: DoctorFsMutationRequest<'_>,
    mut receipt: DoctorFsMutationReceipt,
) -> DoctorFsMutationReceipt {
    receipt
        .precondition_checks
        .push("mutation_kind_restore_staged_file".to_string());

    if !doctor_repair_mode_allows_asset_operation_mutation(
        request.mode,
        request.asset_class,
        request.mutation_kind.asset_operation(),
    ) {
        receipt.blocked_reasons.push(format!(
            "refusing to restore asset class {:?}: mode {:?} does not allow restore mutation",
            request.asset_class, request.mode
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("mode_allows_asset_operation".to_string());

    let Some(source_path) = request.source_path else {
        receipt
            .blocked_reasons
            .push("refusing to restore without a staged source path".to_string());
        return receipt;
    };
    let Some(staging_root) = request.staging_root else {
        receipt
            .blocked_reasons
            .push("refusing to restore without an approved staging root".to_string());
        return receipt;
    };
    let Some(expected_source_blake3) = request.expected_source_blake3 else {
        receipt
            .blocked_reasons
            .push("refusing to restore without expected source blake3".to_string());
        return receipt;
    };

    if !doctor_staged_source_path_is_safe(source_path, staging_root, request.data_dir) {
        receipt.blocked_reasons.push(format!(
            "refusing to restore unsafe staged source path {}",
            source_path.display()
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("source_path_confined_to_staging_root".to_string());

    let Some(source_parent) = source_path.parent() else {
        receipt.blocked_reasons.push(format!(
            "refusing to restore source without parent {}",
            source_path.display()
        ));
        return receipt;
    };
    let Some(target_parent) = request.target_path.parent() else {
        receipt.blocked_reasons.push(format!(
            "refusing to restore target without parent {}",
            request.target_path.display()
        ));
        return receipt;
    };
    if !target_parent.is_dir() {
        receipt.blocked_reasons.push(format!(
            "refusing to restore target with missing parent {}",
            target_parent.display()
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("target_parent_exists".to_string());

    if !doctor_staged_rename_target_path_is_safe(
        request.target_path,
        staging_root,
        request.data_dir,
    ) {
        receipt.blocked_reasons.push(format!(
            "refusing to restore to unsafe target {}",
            request.target_path.display()
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("target_path_confined_to_data_dir".to_string());

    match std::fs::symlink_metadata(request.target_path) {
        Ok(_) => {
            receipt.blocked_reasons.push(format!(
                "refusing to restore over existing target {}",
                request.target_path.display()
            ));
            return receipt;
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => {
            receipt.blocked_reasons.push(format!(
                "refusing to restore target {}: could not verify target absence: {err}",
                request.target_path.display()
            ));
            return receipt;
        }
    }
    receipt
        .precondition_checks
        .push("target_does_not_exist".to_string());

    let source_blake3 = match file_blake3_hex(source_path) {
        Ok(hash) => hash,
        Err(err) => {
            receipt.status = DoctorActionStatus::Failed;
            receipt.blocked_reasons.push(err);
            return receipt;
        }
    };
    receipt.actual_source_blake3 = Some(source_blake3.clone());
    receipt
        .precondition_checks
        .push("source_blake3_recorded".to_string());

    if expected_source_blake3 != source_blake3 {
        receipt.blocked_reasons.push(format!(
            "refusing to restore source {}: expected blake3 {expected_source_blake3}, observed {source_blake3}",
            source_path.display()
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("expected_source_blake3_matched".to_string());

    let source_bytes = std::fs::metadata(source_path)
        .map(|metadata| metadata.len())
        .unwrap_or(request.planned_bytes);
    match doctor_move_file_with_cross_device_fallback(
        source_path,
        request.target_path,
        &source_blake3,
        source_bytes,
        "restore",
        "restored target",
    ) {
        Ok(move_method) => {
            receipt.affected_bytes = source_bytes;
            receipt.fallback_kind = Some(move_method.receipt_fallback_kind().to_string());
            receipt
                .precondition_checks
                .push(move_method.receipt_precondition_check().to_string());
        }
        Err(err) => {
            receipt.status = DoctorActionStatus::Failed;
            receipt.blocked_reasons.push(err);
            return receipt;
        }
    }

    let target_blake3 = match file_blake3_hex(request.target_path) {
        Ok(hash) => hash,
        Err(err) => {
            receipt.status = DoctorActionStatus::Failed;
            receipt.blocked_reasons.push(err);
            return receipt;
        }
    };
    receipt.actual_target_blake3 = Some(target_blake3.clone());
    if target_blake3 != source_blake3 {
        receipt.status = DoctorActionStatus::Failed;
        receipt.blocked_reasons.push(format!(
            "restored target {} hash mismatch: expected source blake3 {source_blake3}, observed target blake3 {target_blake3}",
            request.target_path.display()
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("target_blake3_matched_source".to_string());

    match sync_file(request.target_path, "restored target") {
        Ok(()) => {
            receipt
                .precondition_checks
                .push("target_file_sync_completed".to_string());
        }
        Err(err) => {
            receipt.status = DoctorActionStatus::Failed;
            receipt.blocked_reasons.push(err);
            return receipt;
        }
    }

    match sync_directory(source_parent).and_then(|()| sync_directory(target_parent)) {
        Ok(()) => {
            receipt.status = DoctorActionStatus::Applied;
            receipt
                .precondition_checks
                .push("source_and_target_parent_sync_completed".to_string());
        }
        Err(err) => {
            receipt.status = DoctorActionStatus::Failed;
            receipt.blocked_reasons.push(err);
        }
    }
    receipt
}

pub(crate) fn execute_doctor_move_file_to_quarantine(
    request: DoctorFsMutationRequest<'_>,
    mut receipt: DoctorFsMutationReceipt,
) -> DoctorFsMutationReceipt {
    receipt
        .precondition_checks
        .push("mutation_kind_move_file_to_quarantine".to_string());

    if !doctor_repair_mode_allows_asset_operation_mutation(
        request.mode,
        request.asset_class,
        request.mutation_kind.asset_operation(),
    ) {
        receipt.blocked_reasons.push(format!(
            "refusing to quarantine asset class {:?}: mode {:?} does not allow quarantine mutation",
            request.asset_class, request.mode
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("mode_allows_asset_operation".to_string());

    let Some(source_path) = request.source_path else {
        receipt
            .blocked_reasons
            .push("refusing to quarantine without a source path".to_string());
        return receipt;
    };
    let Some(quarantine_root) = request.staging_root else {
        receipt
            .blocked_reasons
            .push("refusing to quarantine without an approved quarantine root".to_string());
        return receipt;
    };
    let Some(expected_source_blake3) = request.expected_source_blake3 else {
        receipt
            .blocked_reasons
            .push("refusing to quarantine without expected source blake3".to_string());
        return receipt;
    };

    if !doctor_quarantine_source_path_is_safe(
        source_path,
        request.data_dir,
        request.db_path,
        request.index_path,
        request.asset_class,
    ) {
        receipt.blocked_reasons.push(format!(
            "refusing to quarantine unsafe source path {}",
            source_path.display()
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("source_path_confined_to_derived_asset".to_string());

    let Some(source_parent) = source_path.parent() else {
        receipt.blocked_reasons.push(format!(
            "refusing to quarantine source without parent {}",
            source_path.display()
        ));
        return receipt;
    };
    let Some(target_parent) = request.target_path.parent() else {
        receipt.blocked_reasons.push(format!(
            "refusing to quarantine target without parent {}",
            request.target_path.display()
        ));
        return receipt;
    };
    if !target_parent.is_dir() {
        receipt.blocked_reasons.push(format!(
            "refusing to quarantine target with missing parent {}",
            target_parent.display()
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("target_parent_exists".to_string());

    if !doctor_staging_target_path_is_safe(request.target_path, quarantine_root, request.data_dir) {
        receipt.blocked_reasons.push(format!(
            "refusing to quarantine to unsafe target {}",
            request.target_path.display()
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("target_path_confined_to_quarantine_root".to_string());

    match std::fs::symlink_metadata(request.target_path) {
        Ok(_) => {
            receipt.blocked_reasons.push(format!(
                "refusing to quarantine over existing target {}",
                request.target_path.display()
            ));
            return receipt;
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => {
            receipt.blocked_reasons.push(format!(
                "refusing to quarantine target {}: could not verify target absence: {err}",
                request.target_path.display()
            ));
            return receipt;
        }
    }
    receipt
        .precondition_checks
        .push("target_does_not_exist".to_string());

    let source_blake3 = match file_blake3_hex(source_path) {
        Ok(hash) => hash,
        Err(err) => {
            receipt.status = DoctorActionStatus::Failed;
            receipt.blocked_reasons.push(err);
            return receipt;
        }
    };
    receipt.actual_source_blake3 = Some(source_blake3.clone());
    receipt
        .precondition_checks
        .push("source_blake3_recorded".to_string());

    if expected_source_blake3 != source_blake3 {
        receipt.blocked_reasons.push(format!(
            "refusing to quarantine source {}: expected blake3 {expected_source_blake3}, observed {source_blake3}",
            source_path.display()
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("expected_source_blake3_matched".to_string());

    let source_bytes = std::fs::metadata(source_path)
        .map(|metadata| metadata.len())
        .unwrap_or(request.planned_bytes);
    match doctor_move_file_with_cross_device_fallback(
        source_path,
        request.target_path,
        &source_blake3,
        source_bytes,
        "quarantine",
        "quarantined target",
    ) {
        Ok(move_method) => {
            receipt.affected_bytes = source_bytes;
            receipt.fallback_kind = Some(move_method.receipt_fallback_kind().to_string());
            receipt
                .precondition_checks
                .push(move_method.receipt_precondition_check().to_string());
        }
        Err(err) => {
            receipt.status = DoctorActionStatus::Failed;
            receipt.blocked_reasons.push(err);
            return receipt;
        }
    }

    let target_blake3 = match file_blake3_hex(request.target_path) {
        Ok(hash) => hash,
        Err(err) => {
            receipt.status = DoctorActionStatus::Failed;
            receipt.blocked_reasons.push(err);
            return receipt;
        }
    };
    receipt.actual_target_blake3 = Some(target_blake3.clone());
    if target_blake3 != source_blake3 {
        receipt.status = DoctorActionStatus::Failed;
        receipt.blocked_reasons.push(format!(
            "quarantined target {} hash mismatch: expected source blake3 {source_blake3}, observed target blake3 {target_blake3}",
            request.target_path.display()
        ));
        return receipt;
    }
    receipt
        .precondition_checks
        .push("target_blake3_matched_source".to_string());

    match sync_file(request.target_path, "quarantined target") {
        Ok(()) => {
            receipt
                .precondition_checks
                .push("target_file_sync_completed".to_string());
        }
        Err(err) => {
            receipt.status = DoctorActionStatus::Failed;
            receipt.blocked_reasons.push(err);
            return receipt;
        }
    }

    match sync_directory(source_parent).and_then(|()| sync_directory(target_parent)) {
        Ok(()) => {
            receipt.status = DoctorActionStatus::Applied;
            receipt
                .precondition_checks
                .push("source_and_target_parent_sync_completed".to_string());
        }
        Err(err) => {
            receipt.status = DoctorActionStatus::Failed;
            receipt.blocked_reasons.push(err);
        }
    }
    receipt
}

pub(crate) fn doctor_fs_mutation_action_id(
    mutation_kind: DoctorFsMutationKind,
    mode: DoctorRepairMode,
    asset_class: DoctorAssetClass,
    target_path: &Path,
    planned_bytes: u64,
) -> String {
    doctor_canonical_blake3(
        "doctor-fs-mutation-action-v1",
        serde_json::json!({
            "mutation_kind": mutation_kind,
            "mode": mode,
            "asset_class": asset_class,
            "target_path": target_path.display().to_string(),
            "planned_bytes": planned_bytes,
        }),
    )
}

pub(crate) fn file_blake3_hex(path: &Path) -> Result<String, String> {
    use std::io::Read as _;

    let mut file = doctor_open_regular_file_for_read(path)
        .map_err(|err| format!("failed to open {} for blake3: {err}", path.display()))?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|err| format!("failed to read {} for blake3: {err}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}
