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

#[test]
fn doctor_fs_mutation_executor_promotes_verified_staged_file_with_receipt() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");

    let db_path = data_dir.join("agent_search.db");
    let staging_root = data_dir.join("doctor-staging").join("promote-op-1");
    let source_path = staging_root.join("candidate").join("agent_search.db");
    let source_bytes = b"verified candidate archive bytes";
    std::fs::create_dir_all(source_path.parent().expect("source parent"))
        .expect("create staged source parent");
    std::fs::write(&source_path, source_bytes).expect("write staged source");
    let expected_source_blake3 = blake3::hash(source_bytes).to_hex().to_string();

    let receipt = execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: "reconstruct-promote-archive",
        action_id: "promote-staged-archive-db",
        mutation_kind: DoctorFsMutationKind::PromoteStagedFile,
        mode: DoctorRepairMode::ReconstructPromote,
        asset_class: DoctorAssetClass::CanonicalArchiveDb,
        source_path: Some(&source_path),
        target_path: &db_path,
        data_dir: &data_dir,
        db_path: &db_path,
        index_path: &index_path,
        staging_root: Some(&staging_root),
        expected_source_blake3: Some(&expected_source_blake3),
        planned_bytes: source_bytes.len() as u64,
        required_min_age_seconds: None,
    });

    assert_eq!(receipt.status, DoctorActionStatus::Applied);
    assert_eq!(
        receipt.mutation_kind,
        DoctorFsMutationKind::PromoteStagedFile
    );
    assert_eq!(receipt.asset_class, DoctorAssetClass::CanonicalArchiveDb);
    assert_eq!(receipt.affected_bytes, source_bytes.len() as u64);
    assert_eq!(
        receipt.actual_source_blake3.as_deref(),
        Some(expected_source_blake3.as_str())
    );
    assert_eq!(
        receipt.actual_target_blake3.as_deref(),
        Some(expected_source_blake3.as_str())
    );
    assert_eq!(
        receipt.redacted_source_path.as_deref(),
        Some("[cass-data]/doctor-staging/promote-op-1/candidate/agent_search.db")
    );
    assert_eq!(receipt.redacted_target_path, "[cass-data]/agent_search.db");
    assert_eq!(receipt.fallback_kind.as_deref(), Some("none"));
    for expected in [
        "mutation_kind_promote_staged_file",
        "mode_allows_asset_operation",
        "source_path_confined_to_staging_root",
        "target_parent_exists",
        "target_path_confined_to_data_dir",
        "target_does_not_exist",
        "source_blake3_recorded",
        "expected_source_blake3_matched",
        "filesystem_rename_completed",
        "target_blake3_matched_source",
        "target_file_sync_completed",
        "source_and_target_parent_sync_completed",
    ] {
        assert!(
            receipt
                .precondition_checks
                .iter()
                .any(|observed| observed == expected),
            "missing promote receipt precondition {expected}: {:?}",
            receipt.precondition_checks
        );
    }
    assert!(
        !source_path.exists(),
        "successful promote should consume the staged candidate"
    );
    assert_eq!(
        std::fs::read(&db_path).expect("read promoted target"),
        source_bytes
    );
}

#[test]
fn doctor_fs_mutation_executor_promote_cross_device_fallback_verifies_and_consumes_source() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");

    let db_path = data_dir.join("agent_search.db");
    let staging_root = data_dir.join("doctor-staging").join("promote-cross-device");
    let source_path = staging_root.join("candidate").join("agent_search.db");
    let source_bytes = b"verified candidate archive bytes";
    std::fs::create_dir_all(source_path.parent().expect("source parent"))
        .expect("create staged source parent");
    std::fs::write(&source_path, source_bytes).expect("write staged source");
    let expected_source_blake3 = blake3::hash(source_bytes).to_hex().to_string();

    doctor_test_inject_next_cross_device_rename_failure();
    let receipt = execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: "reconstruct-promote-archive",
        action_id: "promote-cross-device-fallback",
        mutation_kind: DoctorFsMutationKind::PromoteStagedFile,
        mode: DoctorRepairMode::ReconstructPromote,
        asset_class: DoctorAssetClass::CanonicalArchiveDb,
        source_path: Some(&source_path),
        target_path: &db_path,
        data_dir: &data_dir,
        db_path: &db_path,
        index_path: &index_path,
        staging_root: Some(&staging_root),
        expected_source_blake3: Some(&expected_source_blake3),
        planned_bytes: source_bytes.len() as u64,
        required_min_age_seconds: None,
    });

    assert_eq!(receipt.status, DoctorActionStatus::Applied);
    assert_eq!(receipt.affected_bytes, source_bytes.len() as u64);
    assert_eq!(
        receipt.fallback_kind.as_deref(),
        Some("cross_device_copy_remove_source")
    );
    assert!(
        receipt
            .precondition_checks
            .iter()
            .any(|check| check == "filesystem_cross_device_copy_remove_completed"),
        "cross-device fallback should record copy+verified source removal: {:?}",
        receipt.precondition_checks
    );
    assert!(
        !receipt
            .precondition_checks
            .iter()
            .any(|check| check == "filesystem_rename_completed"),
        "cross-device fallback must not claim an atomic rename completed"
    );
    assert!(
        receipt
            .precondition_checks
            .iter()
            .any(|check| check == "source_and_target_parent_sync_completed"),
        "cross-device fallback should still sync both changed directories"
    );
    assert!(
        !source_path.exists(),
        "verified cross-device promote fallback should consume the staged source"
    );
    assert_eq!(
        std::fs::read(&db_path).expect("read promoted target"),
        source_bytes
    );
}

#[test]
fn doctor_fs_mutation_executor_promote_non_cross_device_rename_failure_does_not_fallback() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");

    let db_path = data_dir.join("agent_search.db");
    let staging_root = data_dir
        .join("doctor-staging")
        .join("promote-permission-fail");
    let source_path = staging_root.join("candidate.db");
    let source_bytes = b"candidate bytes";
    std::fs::create_dir_all(source_path.parent().expect("source parent"))
        .expect("create staged source parent");
    std::fs::write(&source_path, source_bytes).expect("write staged source");
    let expected_source_blake3 = blake3::hash(source_bytes).to_hex().to_string();

    doctor_test_inject_next_permission_denied_rename_failure();
    let receipt = execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: "reconstruct-promote-archive",
        action_id: "promote-permission-denied",
        mutation_kind: DoctorFsMutationKind::PromoteStagedFile,
        mode: DoctorRepairMode::ReconstructPromote,
        asset_class: DoctorAssetClass::CanonicalArchiveDb,
        source_path: Some(&source_path),
        target_path: &db_path,
        data_dir: &data_dir,
        db_path: &db_path,
        index_path: &index_path,
        staging_root: Some(&staging_root),
        expected_source_blake3: Some(&expected_source_blake3),
        planned_bytes: source_bytes.len() as u64,
        required_min_age_seconds: None,
    });

    assert_eq!(receipt.status, DoctorActionStatus::Failed);
    assert!(
        receipt
            .blocked_reasons
            .iter()
            .any(|reason| reason.contains("failed to atomically promote")),
        "non-cross-device rename failures must not take the copy fallback path: {:?}",
        receipt.blocked_reasons
    );
    assert!(
        !receipt
            .precondition_checks
            .iter()
            .any(|check| check == "filesystem_cross_device_copy_remove_completed"),
        "permission-denied rename must not be treated as cross-device fallback"
    );
    assert!(
        source_path.exists(),
        "failed non-cross-device promote must keep staged source"
    );
    assert!(
        !db_path.exists(),
        "failed non-cross-device promote must not create live target"
    );
}

#[test]
fn doctor_fs_mutation_executor_refuses_promote_over_existing_target() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");

    let db_path = data_dir.join("agent_search.db");
    std::fs::write(&db_path, b"existing archive bytes").expect("write existing archive");
    let staging_root = data_dir
        .join("doctor-staging")
        .join("promote-existing-target");
    let source_path = staging_root.join("candidate.db");
    let source_bytes = b"candidate bytes";
    std::fs::create_dir_all(source_path.parent().expect("source parent"))
        .expect("create staged source parent");
    std::fs::write(&source_path, source_bytes).expect("write staged source");
    let expected_source_blake3 = blake3::hash(source_bytes).to_hex().to_string();

    let receipt = execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: "reconstruct-promote-archive",
        action_id: "promote-existing-target",
        mutation_kind: DoctorFsMutationKind::PromoteStagedFile,
        mode: DoctorRepairMode::ReconstructPromote,
        asset_class: DoctorAssetClass::CanonicalArchiveDb,
        source_path: Some(&source_path),
        target_path: &db_path,
        data_dir: &data_dir,
        db_path: &db_path,
        index_path: &index_path,
        staging_root: Some(&staging_root),
        expected_source_blake3: Some(&expected_source_blake3),
        planned_bytes: source_bytes.len() as u64,
        required_min_age_seconds: None,
    });

    assert_eq!(receipt.status, DoctorActionStatus::Blocked);
    assert!(
        receipt
            .blocked_reasons
            .iter()
            .any(|reason| reason.contains("existing target")),
        "existing live targets must be refused unless a prior snapshot operation moved them aside: {:?}",
        receipt.blocked_reasons
    );
    assert!(
        !receipt
            .precondition_checks
            .iter()
            .any(|check| check == "filesystem_rename_completed"),
        "existing target must stop before rename"
    );
    assert_eq!(
        std::fs::read(&db_path).expect("read existing target"),
        b"existing archive bytes"
    );
    assert!(
        source_path.exists(),
        "blocked promote must keep staged source"
    );
}

#[test]
fn doctor_fs_mutation_executor_blocks_promote_without_verified_source_hash() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");

    let db_path = data_dir.join("agent_search.db");
    let staging_root = data_dir.join("doctor-staging").join("promote-missing-hash");
    let source_path = staging_root.join("candidate.db");
    std::fs::create_dir_all(source_path.parent().expect("source parent"))
        .expect("create staged source parent");
    std::fs::write(&source_path, b"candidate bytes").expect("write staged source");

    let receipt = execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: "reconstruct-promote-archive",
        action_id: "promote-without-hash",
        mutation_kind: DoctorFsMutationKind::PromoteStagedFile,
        mode: DoctorRepairMode::ReconstructPromote,
        asset_class: DoctorAssetClass::CanonicalArchiveDb,
        source_path: Some(&source_path),
        target_path: &db_path,
        data_dir: &data_dir,
        db_path: &db_path,
        index_path: &index_path,
        staging_root: Some(&staging_root),
        expected_source_blake3: None,
        planned_bytes: 15,
        required_min_age_seconds: None,
    });

    assert_eq!(receipt.status, DoctorActionStatus::Blocked);
    assert!(
        receipt
            .blocked_reasons
            .iter()
            .any(|reason| reason.contains("expected source blake3")),
        "promote must require a verified source hash: {:?}",
        receipt.blocked_reasons
    );
    assert!(
        source_path.exists(),
        "blocked promote must keep staged source"
    );
    assert!(
        !db_path.exists(),
        "blocked promote must not create live target"
    );
}

#[test]
fn doctor_fs_mutation_executor_blocks_promote_target_inside_staging_root() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");

    let db_path = data_dir.join("agent_search.db");
    let staging_root = data_dir
        .join("doctor-staging")
        .join("promote-target-inside-staging");
    let source_path = staging_root.join("candidate.db");
    let source_bytes = b"candidate bytes";
    std::fs::create_dir_all(&staging_root).expect("create staging root");
    std::fs::write(&source_path, source_bytes).expect("write staged source");
    let expected_source_blake3 = blake3::hash(source_bytes).to_hex().to_string();
    let unsafe_target = staging_root.join("live-target.db");

    let receipt = execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: "reconstruct-promote-archive",
        action_id: "promote-target-inside-staging",
        mutation_kind: DoctorFsMutationKind::PromoteStagedFile,
        mode: DoctorRepairMode::ReconstructPromote,
        asset_class: DoctorAssetClass::CanonicalArchiveDb,
        source_path: Some(&source_path),
        target_path: &unsafe_target,
        data_dir: &data_dir,
        db_path: &db_path,
        index_path: &index_path,
        staging_root: Some(&staging_root),
        expected_source_blake3: Some(&expected_source_blake3),
        planned_bytes: source_bytes.len() as u64,
        required_min_age_seconds: None,
    });

    assert_eq!(receipt.status, DoctorActionStatus::Blocked);
    assert!(
        receipt
            .blocked_reasons
            .iter()
            .any(|reason| reason.contains("unsafe target")),
        "promote target inside staging should be rejected: {:?}",
        receipt.blocked_reasons
    );
    assert!(
        source_path.exists(),
        "blocked promote must keep staged source"
    );
    assert!(
        !unsafe_target.exists(),
        "blocked promote must not create a target inside staging"
    );
}

#[test]
fn doctor_fs_mutation_executor_restores_verified_staged_archive_with_receipt() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");

    let db_path = data_dir.join("agent_search.db");
    let staging_root = data_dir.join("doctor-restore").join("restore-op-1");
    let source_path = staging_root.join("verified-backup").join("agent_search.db");
    let source_bytes = b"verified restored archive bytes";
    std::fs::create_dir_all(source_path.parent().expect("source parent"))
        .expect("create staged restore parent");
    std::fs::write(&source_path, source_bytes).expect("write staged restore source");
    let expected_source_blake3 = blake3::hash(source_bytes).to_hex().to_string();

    let receipt = execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: "restore-apply-archive",
        action_id: "restore-staged-archive-db",
        mutation_kind: DoctorFsMutationKind::RestoreStagedFile,
        mode: DoctorRepairMode::RestoreApply,
        asset_class: DoctorAssetClass::CanonicalArchiveDb,
        source_path: Some(&source_path),
        target_path: &db_path,
        data_dir: &data_dir,
        db_path: &db_path,
        index_path: &index_path,
        staging_root: Some(&staging_root),
        expected_source_blake3: Some(&expected_source_blake3),
        planned_bytes: source_bytes.len() as u64,
        required_min_age_seconds: None,
    });

    assert_eq!(receipt.status, DoctorActionStatus::Applied);
    assert_eq!(
        receipt.mutation_kind,
        DoctorFsMutationKind::RestoreStagedFile
    );
    assert_eq!(receipt.asset_class, DoctorAssetClass::CanonicalArchiveDb);
    assert_eq!(receipt.affected_bytes, source_bytes.len() as u64);
    assert_eq!(
        receipt.actual_source_blake3.as_deref(),
        Some(expected_source_blake3.as_str())
    );
    assert_eq!(
        receipt.actual_target_blake3.as_deref(),
        Some(expected_source_blake3.as_str())
    );
    assert_eq!(
        receipt.redacted_source_path.as_deref(),
        Some("[cass-data]/doctor-restore/restore-op-1/verified-backup/agent_search.db")
    );
    assert_eq!(receipt.redacted_target_path, "[cass-data]/agent_search.db");
    for expected in [
        "mutation_kind_restore_staged_file",
        "mode_allows_asset_operation",
        "source_path_confined_to_staging_root",
        "target_parent_exists",
        "target_path_confined_to_data_dir",
        "target_does_not_exist",
        "source_blake3_recorded",
        "expected_source_blake3_matched",
        "filesystem_rename_completed",
        "target_blake3_matched_source",
        "target_file_sync_completed",
        "source_and_target_parent_sync_completed",
    ] {
        assert!(
            receipt
                .precondition_checks
                .iter()
                .any(|observed| observed == expected),
            "missing restore receipt precondition {expected}: {:?}",
            receipt.precondition_checks
        );
    }
    assert!(
        !source_path.exists(),
        "successful restore should consume the verified staged backup"
    );
    assert_eq!(
        std::fs::read(&db_path).expect("read restored archive"),
        source_bytes
    );
}

#[test]
fn doctor_fs_mutation_executor_refuses_restore_over_existing_archive() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");

    let db_path = data_dir.join("agent_search.db");
    std::fs::write(&db_path, b"existing archive bytes").expect("write existing archive");
    let staging_root = data_dir
        .join("doctor-restore")
        .join("restore-existing-target");
    let source_path = staging_root.join("candidate.db");
    let source_bytes = b"restored archive bytes";
    std::fs::create_dir_all(source_path.parent().expect("source parent"))
        .expect("create staged restore parent");
    std::fs::write(&source_path, source_bytes).expect("write staged restore source");
    let expected_source_blake3 = blake3::hash(source_bytes).to_hex().to_string();

    let receipt = execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: "restore-apply-archive",
        action_id: "restore-existing-target",
        mutation_kind: DoctorFsMutationKind::RestoreStagedFile,
        mode: DoctorRepairMode::RestoreApply,
        asset_class: DoctorAssetClass::CanonicalArchiveDb,
        source_path: Some(&source_path),
        target_path: &db_path,
        data_dir: &data_dir,
        db_path: &db_path,
        index_path: &index_path,
        staging_root: Some(&staging_root),
        expected_source_blake3: Some(&expected_source_blake3),
        planned_bytes: source_bytes.len() as u64,
        required_min_age_seconds: None,
    });

    assert_eq!(receipt.status, DoctorActionStatus::Blocked);
    assert!(
        receipt
            .blocked_reasons
            .iter()
            .any(|reason| reason.contains("existing target")),
        "restore must refuse existing archive targets until a prior snapshot/quarantine operation moves them aside: {:?}",
        receipt.blocked_reasons
    );
    assert!(
        !receipt
            .precondition_checks
            .iter()
            .any(|check| check == "filesystem_rename_completed"),
        "existing restore target must stop before rename"
    );
    assert_eq!(
        std::fs::read(&db_path).expect("read existing archive"),
        b"existing archive bytes"
    );
    assert!(
        source_path.exists(),
        "blocked restore must keep staged source"
    );
}

#[test]
fn doctor_fs_mutation_executor_blocks_restore_without_verified_source_hash() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");

    let db_path = data_dir.join("agent_search.db");
    let staging_root = data_dir.join("doctor-restore").join("restore-missing-hash");
    let source_path = staging_root.join("candidate.db");
    std::fs::create_dir_all(source_path.parent().expect("source parent"))
        .expect("create staged restore parent");
    std::fs::write(&source_path, b"restore candidate bytes").expect("write staged restore source");

    let receipt = execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: "restore-apply-archive",
        action_id: "restore-without-hash",
        mutation_kind: DoctorFsMutationKind::RestoreStagedFile,
        mode: DoctorRepairMode::RestoreApply,
        asset_class: DoctorAssetClass::CanonicalArchiveDb,
        source_path: Some(&source_path),
        target_path: &db_path,
        data_dir: &data_dir,
        db_path: &db_path,
        index_path: &index_path,
        staging_root: Some(&staging_root),
        expected_source_blake3: None,
        planned_bytes: 23,
        required_min_age_seconds: None,
    });

    assert_eq!(receipt.status, DoctorActionStatus::Blocked);
    assert!(
        receipt
            .blocked_reasons
            .iter()
            .any(|reason| reason.contains("expected source blake3")),
        "restore must require a verified source hash: {:?}",
        receipt.blocked_reasons
    );
    assert!(
        source_path.exists(),
        "blocked restore must keep staged source"
    );
    assert!(
        !db_path.exists(),
        "blocked restore must not create live target"
    );
}

#[test]
fn doctor_fs_mutation_executor_reports_restore_file_sync_failure_after_rename() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");

    let db_path = data_dir.join("agent_search.db");
    let staging_root = data_dir
        .join("doctor-restore")
        .join("restore-file-sync-fail");
    let source_path = staging_root.join("candidate.db");
    let source_bytes = b"restore candidate bytes";
    std::fs::create_dir_all(source_path.parent().expect("source parent"))
        .expect("create staged restore parent");
    std::fs::write(&source_path, source_bytes).expect("write staged restore source");
    let expected_source_blake3 = blake3::hash(source_bytes).to_hex().to_string();

    doctor_test_inject_next_file_sync_failure();
    let receipt = execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: "restore-apply-archive",
        action_id: "restore-file-sync-fail",
        mutation_kind: DoctorFsMutationKind::RestoreStagedFile,
        mode: DoctorRepairMode::RestoreApply,
        asset_class: DoctorAssetClass::CanonicalArchiveDb,
        source_path: Some(&source_path),
        target_path: &db_path,
        data_dir: &data_dir,
        db_path: &db_path,
        index_path: &index_path,
        staging_root: Some(&staging_root),
        expected_source_blake3: Some(&expected_source_blake3),
        planned_bytes: source_bytes.len() as u64,
        required_min_age_seconds: None,
    });

    assert_eq!(receipt.status, DoctorActionStatus::Failed);
    assert!(
        receipt
            .blocked_reasons
            .iter()
            .any(|reason| reason.contains("injected test failure syncing restored target")),
        "restore file sync failure should be explicit: {:?}",
        receipt.blocked_reasons
    );
    for expected in [
        "filesystem_rename_completed",
        "target_blake3_matched_source",
    ] {
        assert!(
            receipt
                .precondition_checks
                .iter()
                .any(|observed| observed == expected),
            "restore file-sync failure should record completed step {expected}: {:?}",
            receipt.precondition_checks
        );
    }
    assert!(
        !receipt
            .precondition_checks
            .iter()
            .any(|check| check == "target_file_sync_completed"),
        "restore file-sync failure must not claim target sync completed"
    );
    assert!(
        !source_path.exists(),
        "post-rename sync failure should leave the staged source consumed"
    );
    assert_eq!(
        std::fs::read(&db_path).expect("read restored target after sync failure"),
        source_bytes,
        "failed receipt must still leave a recoverable target for inspection"
    );
}

#[test]
fn doctor_fs_mutation_executor_reports_restore_parent_sync_failure_after_file_sync() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");

    let db_path = data_dir.join("agent_search.db");
    let staging_root = data_dir
        .join("doctor-restore")
        .join("restore-dir-sync-fail");
    let source_path = staging_root.join("candidate.db");
    let source_bytes = b"restore candidate bytes";
    std::fs::create_dir_all(source_path.parent().expect("source parent"))
        .expect("create staged restore parent");
    std::fs::write(&source_path, source_bytes).expect("write staged restore source");
    let expected_source_blake3 = blake3::hash(source_bytes).to_hex().to_string();

    doctor_test_inject_next_directory_sync_failure();
    let receipt = execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: "restore-apply-archive",
        action_id: "restore-parent-sync-fail",
        mutation_kind: DoctorFsMutationKind::RestoreStagedFile,
        mode: DoctorRepairMode::RestoreApply,
        asset_class: DoctorAssetClass::CanonicalArchiveDb,
        source_path: Some(&source_path),
        target_path: &db_path,
        data_dir: &data_dir,
        db_path: &db_path,
        index_path: &index_path,
        staging_root: Some(&staging_root),
        expected_source_blake3: Some(&expected_source_blake3),
        planned_bytes: source_bytes.len() as u64,
        required_min_age_seconds: None,
    });

    assert_eq!(receipt.status, DoctorActionStatus::Failed);
    assert!(
        receipt
            .blocked_reasons
            .iter()
            .any(|reason| reason.contains("injected test failure syncing directory")),
        "restore parent sync failure should be explicit: {:?}",
        receipt.blocked_reasons
    );
    assert!(
        receipt
            .precondition_checks
            .iter()
            .any(|check| check == "target_file_sync_completed"),
        "parent-sync failure should prove the restored target was fsynced first"
    );
    assert!(
        !receipt
            .precondition_checks
            .iter()
            .any(|check| check == "source_and_target_parent_sync_completed"),
        "parent-sync failure must not claim source+target directory sync completed"
    );
    assert!(
        !source_path.exists(),
        "post-rename parent sync failure should leave the staged source consumed"
    );
    assert_eq!(
        std::fs::read(&db_path).expect("read restored target after parent sync failure"),
        source_bytes,
        "failed receipt must leave a recoverable restored target for inspection"
    );
}

#[test]
fn doctor_fs_mutation_executor_restore_cross_device_fallback_rolls_back_target_on_sync_failure() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");

    let db_path = data_dir.join("agent_search.db");
    let staging_root = data_dir
        .join("doctor-restore")
        .join("restore-cross-device-sync-fail");
    let source_path = staging_root.join("candidate.db");
    let source_bytes = b"restore candidate bytes";
    std::fs::create_dir_all(source_path.parent().expect("source parent"))
        .expect("create staged restore parent");
    std::fs::write(&source_path, source_bytes).expect("write staged restore source");
    let expected_source_blake3 = blake3::hash(source_bytes).to_hex().to_string();

    doctor_test_inject_next_cross_device_rename_failure();
    doctor_test_inject_next_file_sync_failure();
    let receipt = execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: "restore-apply-archive",
        action_id: "restore-cross-device-sync-fail",
        mutation_kind: DoctorFsMutationKind::RestoreStagedFile,
        mode: DoctorRepairMode::RestoreApply,
        asset_class: DoctorAssetClass::CanonicalArchiveDb,
        source_path: Some(&source_path),
        target_path: &db_path,
        data_dir: &data_dir,
        db_path: &db_path,
        index_path: &index_path,
        staging_root: Some(&staging_root),
        expected_source_blake3: Some(&expected_source_blake3),
        planned_bytes: source_bytes.len() as u64,
        required_min_age_seconds: None,
    });

    assert_eq!(receipt.status, DoctorActionStatus::Failed);
    assert!(
        receipt.blocked_reasons.iter().any(|reason| {
            reason.contains("failed cross-device restore fallback target sync")
                && reason.contains("rolled back copied target")
        }),
        "cross-device sync failure should report rollback of the copied target: {:?}",
        receipt.blocked_reasons
    );
    assert!(
        !receipt
            .precondition_checks
            .iter()
            .any(|check| check == "filesystem_cross_device_copy_remove_completed"),
        "failed fallback must not claim source removal completed"
    );
    assert!(
        source_path.exists(),
        "failed cross-device restore fallback must keep the verified staged source"
    );
    assert!(
        !db_path.exists(),
        "failed cross-device restore fallback must roll back the copied live target"
    );
}

#[test]
fn doctor_fs_mutation_executor_quarantines_verified_derived_file_with_receipt() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");
    let db_path = data_dir.join("agent_search.db");
    std::fs::write(&db_path, b"precious archive").expect("write archive placeholder");

    let source_path = index_path.join("segment-0001.idx");
    let source_bytes = b"bad derived lexical bytes";
    std::fs::write(&source_path, source_bytes).expect("write derived source");
    let expected_source_blake3 = blake3::hash(source_bytes).to_hex().to_string();

    let quarantine_root = data_dir.join("doctor-quarantine").join("repair-op-1");
    let target_path = quarantine_root.join("index").join("segment-0001.idx");
    std::fs::create_dir_all(target_path.parent().expect("target parent"))
        .expect("create quarantine target parent");

    let receipt = execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: "repair-apply-quarantine-derived-index",
        action_id: "quarantine-derived-segment",
        mutation_kind: DoctorFsMutationKind::MoveFileToQuarantine,
        mode: DoctorRepairMode::RepairApply,
        asset_class: DoctorAssetClass::DerivedLexicalIndex,
        source_path: Some(&source_path),
        target_path: &target_path,
        data_dir: &data_dir,
        db_path: &db_path,
        index_path: &index_path,
        staging_root: Some(&quarantine_root),
        expected_source_blake3: Some(&expected_source_blake3),
        planned_bytes: source_bytes.len() as u64,
        required_min_age_seconds: None,
    });

    assert_eq!(receipt.status, DoctorActionStatus::Applied);
    assert_eq!(
        receipt.mutation_kind,
        DoctorFsMutationKind::MoveFileToQuarantine
    );
    assert_eq!(receipt.asset_class, DoctorAssetClass::DerivedLexicalIndex);
    assert_eq!(receipt.affected_bytes, source_bytes.len() as u64);
    assert_eq!(
        receipt.actual_source_blake3.as_deref(),
        Some(expected_source_blake3.as_str())
    );
    assert_eq!(
        receipt.actual_target_blake3.as_deref(),
        Some(expected_source_blake3.as_str())
    );
    assert_eq!(
        receipt.redacted_source_path.as_deref(),
        Some("[cass-data]/index/live-generation/segment-0001.idx")
    );
    assert_eq!(
        receipt.redacted_target_path,
        "[cass-data]/doctor-quarantine/repair-op-1/index/segment-0001.idx"
    );
    for expected in [
        "mutation_kind_move_file_to_quarantine",
        "mode_allows_asset_operation",
        "source_path_confined_to_derived_asset",
        "target_parent_exists",
        "target_path_confined_to_quarantine_root",
        "target_does_not_exist",
        "source_blake3_recorded",
        "expected_source_blake3_matched",
        "filesystem_rename_completed",
        "target_blake3_matched_source",
        "target_file_sync_completed",
        "source_and_target_parent_sync_completed",
    ] {
        assert!(
            receipt
                .precondition_checks
                .iter()
                .any(|observed| observed == expected),
            "missing quarantine receipt precondition {expected}: {:?}",
            receipt.precondition_checks
        );
    }
    assert!(
        !source_path.exists(),
        "successful quarantine should move the suspect derived file out of live index path"
    );
    assert_eq!(
        std::fs::read(&target_path).expect("read quarantined target"),
        source_bytes
    );
    assert!(
        db_path.exists(),
        "quarantining derived files must not touch the canonical archive DB"
    );
}

#[test]
fn doctor_fs_mutation_executor_refuses_to_quarantine_precious_archive_db() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");
    let db_path = data_dir.join("agent_search.db");
    let archive_bytes = b"precious archive bytes";
    std::fs::write(&db_path, archive_bytes).expect("write archive placeholder");
    let expected_source_blake3 = blake3::hash(archive_bytes).to_hex().to_string();

    let quarantine_root = data_dir.join("doctor-quarantine").join("archive-refusal");
    let target_path = quarantine_root.join("agent_search.db");
    std::fs::create_dir_all(target_path.parent().expect("target parent"))
        .expect("create quarantine target parent");

    let receipt = execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: "repair-apply-quarantine-derived-index",
        action_id: "quarantine-archive-db-refused",
        mutation_kind: DoctorFsMutationKind::MoveFileToQuarantine,
        mode: DoctorRepairMode::RepairApply,
        asset_class: DoctorAssetClass::CanonicalArchiveDb,
        source_path: Some(&db_path),
        target_path: &target_path,
        data_dir: &data_dir,
        db_path: &db_path,
        index_path: &index_path,
        staging_root: Some(&quarantine_root),
        expected_source_blake3: Some(&expected_source_blake3),
        planned_bytes: archive_bytes.len() as u64,
        required_min_age_seconds: None,
    });

    assert_eq!(receipt.status, DoctorActionStatus::Blocked);
    assert!(
        receipt
            .blocked_reasons
            .iter()
            .any(|reason| reason.contains("does not allow quarantine mutation")),
        "precious archive quarantine refusal should name the mode/policy blocker: {:?}",
        receipt.blocked_reasons
    );
    assert!(
        !receipt
            .precondition_checks
            .iter()
            .any(|check| check == "filesystem_rename_completed"),
        "archive DB refusal must stop before rename"
    );
    assert_eq!(
        std::fs::read(&db_path).expect("read archive source"),
        archive_bytes
    );
    assert!(
        !target_path.exists(),
        "blocked archive quarantine must not create a target"
    );
}

#[test]
fn doctor_fs_mutation_executor_blocks_quarantine_without_verified_source_hash() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");
    let db_path = data_dir.join("agent_search.db");
    std::fs::write(&db_path, b"precious archive").expect("write archive placeholder");

    let source_path = index_path.join("segment-0002.idx");
    std::fs::write(&source_path, b"bad derived lexical bytes").expect("write derived source");
    let quarantine_root = data_dir.join("doctor-quarantine").join("missing-hash");
    let target_path = quarantine_root.join("segment-0002.idx");
    std::fs::create_dir_all(target_path.parent().expect("target parent"))
        .expect("create quarantine target parent");

    let receipt = execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: "repair-apply-quarantine-derived-index",
        action_id: "quarantine-without-hash",
        mutation_kind: DoctorFsMutationKind::MoveFileToQuarantine,
        mode: DoctorRepairMode::RepairApply,
        asset_class: DoctorAssetClass::DerivedLexicalIndex,
        source_path: Some(&source_path),
        target_path: &target_path,
        data_dir: &data_dir,
        db_path: &db_path,
        index_path: &index_path,
        staging_root: Some(&quarantine_root),
        expected_source_blake3: None,
        planned_bytes: fs_dir_size(&source_path),
        required_min_age_seconds: None,
    });

    assert_eq!(receipt.status, DoctorActionStatus::Blocked);
    assert!(
        receipt
            .blocked_reasons
            .iter()
            .any(|reason| reason.contains("expected source blake3")),
        "quarantine must require a verified source hash: {:?}",
        receipt.blocked_reasons
    );
    assert!(
        source_path.exists(),
        "blocked quarantine must keep suspect source in place"
    );
    assert!(
        !target_path.exists(),
        "blocked quarantine must not create target"
    );
}

#[test]
fn doctor_fs_mutation_executor_refuses_quarantine_over_existing_target() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");
    let db_path = data_dir.join("agent_search.db");
    std::fs::write(&db_path, b"precious archive").expect("write archive placeholder");

    let source_path = index_path.join("segment-0003.idx");
    let source_bytes = b"bad derived lexical bytes";
    std::fs::write(&source_path, source_bytes).expect("write derived source");
    let expected_source_blake3 = blake3::hash(source_bytes).to_hex().to_string();
    let quarantine_root = data_dir.join("doctor-quarantine").join("existing-target");
    let target_path = quarantine_root.join("segment-0003.idx");
    std::fs::create_dir_all(target_path.parent().expect("target parent"))
        .expect("create quarantine target parent");
    std::fs::write(&target_path, b"existing quarantine bytes").expect("write existing target");

    let receipt = execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: "repair-apply-quarantine-derived-index",
        action_id: "quarantine-existing-target",
        mutation_kind: DoctorFsMutationKind::MoveFileToQuarantine,
        mode: DoctorRepairMode::RepairApply,
        asset_class: DoctorAssetClass::DerivedLexicalIndex,
        source_path: Some(&source_path),
        target_path: &target_path,
        data_dir: &data_dir,
        db_path: &db_path,
        index_path: &index_path,
        staging_root: Some(&quarantine_root),
        expected_source_blake3: Some(&expected_source_blake3),
        planned_bytes: source_bytes.len() as u64,
        required_min_age_seconds: None,
    });

    assert_eq!(receipt.status, DoctorActionStatus::Blocked);
    assert!(
        receipt
            .blocked_reasons
            .iter()
            .any(|reason| reason.contains("existing target")),
        "existing quarantine targets must not be overwritten: {:?}",
        receipt.blocked_reasons
    );
    assert_eq!(
        std::fs::read(&target_path).expect("read existing target"),
        b"existing quarantine bytes"
    );
    assert!(source_path.exists(), "blocked quarantine must keep source");
}

#[test]
fn doctor_fs_mutation_executor_refuses_quarantine_asset_path_mismatch() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");
    let db_path = data_dir.join("agent_search.db");
    std::fs::write(&db_path, b"precious archive").expect("write archive placeholder");

    let cache_root = data_dir.join("cache");
    std::fs::create_dir_all(&cache_root).expect("create cache root");
    let source_path = cache_root.join("memo-cache-entry.bin");
    let source_bytes = b"derived cache bytes";
    std::fs::write(&source_path, source_bytes).expect("write cache source");
    let expected_source_blake3 = blake3::hash(source_bytes).to_hex().to_string();

    let quarantine_root = data_dir
        .join("doctor-quarantine")
        .join("asset-path-mismatch");
    let target_path = quarantine_root.join("memo-cache-entry.bin");
    std::fs::create_dir_all(target_path.parent().expect("target parent"))
        .expect("create quarantine target parent");

    let receipt = execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: "repair-apply-quarantine-derived-index",
        action_id: "quarantine-mislabeled-cache-as-lexical",
        mutation_kind: DoctorFsMutationKind::MoveFileToQuarantine,
        mode: DoctorRepairMode::RepairApply,
        asset_class: DoctorAssetClass::DerivedLexicalIndex,
        source_path: Some(&source_path),
        target_path: &target_path,
        data_dir: &data_dir,
        db_path: &db_path,
        index_path: &index_path,
        staging_root: Some(&quarantine_root),
        expected_source_blake3: Some(&expected_source_blake3),
        planned_bytes: source_bytes.len() as u64,
        required_min_age_seconds: None,
    });

    assert_eq!(receipt.status, DoctorActionStatus::Blocked);
    assert!(
        receipt
            .blocked_reasons
            .iter()
            .any(|reason| reason.contains("unsafe source path")),
        "quarantine must reject a source path that does not match the declared asset class: {:?}",
        receipt.blocked_reasons
    );
    assert!(
        source_path.exists(),
        "blocked quarantine must keep the mismatched source in place"
    );
    assert!(
        !target_path.exists(),
        "blocked quarantine must not create target"
    );
}

fn count_fn_defs(src: &str, fn_name: &str) -> usize {
    let target = format!("fn {fn_name}(");
    src.lines()
        .filter(|line| {
            let trimmed = line.trim_start();
            !trimmed.starts_with("//")
                && (trimmed.starts_with(&target)
                    || trimmed.starts_with(&format!("pub(crate) {target}"))
                    || trimmed.starts_with(&format!("pub(super) {target}"))
                    || trimmed.starts_with(&format!("pub {target}")))
        })
        .count()
}

#[test]
fn doctor_cleanup_apply_cannot_bypass_fs_mutation_executor_for_pruning() {
    let prod_sources = crate::lib_split_production_sources();
    let prod_concat = crate::lib_split_production_concat_source();
    assert_eq!(
        count_fn_defs(&prod_concat, "apply_diag_quarantine_cleanup"),
        1,
        "apply_diag_quarantine_cleanup must have a unique production definition"
    );
    assert_eq!(
        count_fn_defs(&prod_concat, "fill_cleanup_after_summary"),
        1,
        "fill_cleanup_after_summary must have a unique production definition"
    );
    assert_eq!(
        count_fn_defs(&prod_concat, "execute_doctor_fs_mutation"),
        1,
        "execute_doctor_fs_mutation must have a unique production definition"
    );
    assert_eq!(
        count_fn_defs(&prod_concat, "cleanup_target_path_is_safe"),
        1,
        "cleanup_target_path_is_safe must have a unique production definition"
    );

    let quarantine_src = prod_sources
        .iter()
        .find(|(path, _)| {
            path.replace('\\', "/")
                .ends_with("src/lib_doctor/quarantine_and_cleanup.rs")
        })
        .map(|(_, src)| src.as_str())
        .expect("quarantine_and_cleanup.rs production source");
    let cleanup_start = quarantine_src
        .find("fn apply_diag_quarantine_cleanup(")
        .expect("cleanup apply function");
    let cleanup_end = quarantine_src
        .find("fn fill_cleanup_after_summary(")
        .expect("cleanup apply end marker");
    let cleanup_source = &quarantine_src[cleanup_start..cleanup_end];

    assert!(
        cleanup_source.matches("execute_doctor_fs_mutation").count() >= 2,
        "cleanup apply should route both retained publish backup and lexical generation pruning through the audited executor"
    );
    for forbidden in ["remove_dir_all", "remove_file"] {
        assert!(
            !cleanup_source.contains(forbidden),
            "cleanup apply must not call {forbidden} directly; all filesystem pruning goes through execute_doctor_fs_mutation"
        );
    }

    let executor_src = prod_sources
        .iter()
        .find(|(path, _)| {
            path.replace('\\', "/")
                .ends_with("src/lib_doctor/fs_mutation_executor.rs")
        })
        .map(|(_, src)| src.as_str())
        .expect("fs_mutation_executor.rs production source");
    let guards_src = prod_sources
        .iter()
        .find(|(path, _)| {
            path.replace('\\', "/")
                .ends_with("src/lib_doctor/fs_mutation_guards.rs")
        })
        .map(|(_, src)| src.as_str())
        .expect("fs_mutation_guards.rs production source");
    let executor_and_guards = format!("{executor_src}\n{guards_src}");
    let executor_start = executor_and_guards
        .find("fn execute_doctor_fs_mutation(")
        .expect("filesystem mutation executor");
    let executor_end = executor_and_guards
        .find("fn cleanup_target_path_is_safe(")
        .expect("executor end marker");
    let executor_source = &executor_and_guards[executor_start..executor_end];
    assert!(
        executor_source.contains("remove_dir_all")
            && executor_source.contains("remove_file")
            && executor_source.contains("cleanup_target_path_is_safe"),
        "the only cleanup unlink/remove implementation should live inside the executor next to its path guard"
    );
}

#[test]
fn run_doctor_stale_lock_removal_cannot_bypass_fs_mutation_executor() {
    let prod_sources = crate::lib_split_production_sources();
    let prod_concat = crate::lib_split_production_concat_source();
    assert_eq!(
        count_fn_defs(&prod_concat, "run_doctor_impl"),
        1,
        "run_doctor_impl must have a unique production definition"
    );
    let (_, source) = prod_sources
        .iter()
        .find(|(_, src)| count_fn_defs(src, "run_doctor_impl") == 1)
        .expect("production source containing run_doctor_impl");
    let run_doctor_start = source
        .rfind("fn run_doctor_impl(")
        .expect("run_doctor implementation");
    let run_doctor_source = &source[run_doctor_start..];
    let stale_lock_start = run_doctor_source
        .find("// 2. Check for stale lock files")
        .expect("stale lock section");
    let stale_lock_end = run_doctor_source
        .find("// 3. Check database exists and is readable")
        .expect("database section marker");
    let stale_lock_source = &run_doctor_source[stale_lock_start..stale_lock_end];

    assert!(
        stale_lock_source.contains("execute_doctor_fs_mutation")
            && stale_lock_source.contains("RemoveStaleLegacyIndexLock"),
        "run_doctor stale legacy lock cleanup must route through the audited executor"
    );
    assert!(
        !stale_lock_source.contains("remove_file"),
        "run_doctor stale legacy lock cleanup must not call remove_file directly"
    );
}

#[test]
fn doctor_forensic_bundle_captures_db_sidecars_and_metadata_reports() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index").join("tantivy");
    std::fs::create_dir_all(index_path.parent().expect("index parent"))
        .expect("create index parent");
    std::fs::write(&db_path, b"sqlite bytes").expect("write db");
    std::fs::write(data_dir.join("agent_search.db-wal"), b"wal bytes").expect("write wal");
    std::fs::write(data_dir.join("agent_search.db-shm"), b"shm bytes").expect("write shm");
    let manifest_root = doctor_raw_mirror_root(&data_dir).join("manifests");
    std::fs::create_dir_all(&manifest_root).expect("create raw manifest root");
    std::fs::write(manifest_root.join("manifest.json"), br#"{"manifest":true}"#)
        .expect("write raw manifest");

    let bundle = capture_doctor_forensic_bundle(DoctorForensicBundleCaptureInput {
        operation_id: "unit-forensic-capture",
        data_dir: &data_dir,
        db_path: &db_path,
        index_path: &index_path,
        plan: None,
        quarantine_report: None,
        extra_file_artifacts: &[],
    });

    assert_eq!(bundle.status, "captured", "{bundle:#?}");
    assert!(
        bundle
            .path
            .as_deref()
            .is_some_and(|path| path.contains("forensic-bundles"))
    );
    assert!(
        bundle
            .redacted_path
            .as_deref()
            .is_some_and(|path| path.starts_with("[cass-data]/doctor/forensic-bundles/")),
        "bundle metadata should expose redacted bundle paths: {bundle:#?}"
    );
    assert!(bundle.checksum_blake3.is_some());
    assert!(bundle.sidecar_complete);
    assert_eq!(bundle.failed_artifact_count, 0);
    assert!(
        bundle.missing_artifact_count > 0,
        "optional absent configs/bookmarks should be recorded instead of hidden"
    );
    for artifact_kind in [
        "canonical_archive_db",
        "archive_db_wal_sidecar",
        "archive_db_shm_sidecar",
        "raw_mirror_manifest",
        "env_metadata",
        "source_inventory",
        "raw_mirror_report",
        "quarantine_report",
        "forensic_bundle_manifest",
    ] {
        assert!(
            bundle
                .artifacts
                .iter()
                .any(|artifact| artifact.artifact_kind == artifact_kind && artifact.copied),
            "forensic bundle missing copied artifact kind {artifact_kind}: {bundle:#?}"
        );
    }

    let manifest_path = PathBuf::from(bundle.manifest_path.as_deref().expect("manifest path"));
    assert!(
        manifest_path.exists(),
        "bundle manifest should be durable on disk"
    );
    assert_eq!(
        file_blake3_hex(&manifest_path).expect("manifest hash"),
        bundle.checksum_blake3.clone().expect("metadata checksum")
    );
    let env_metadata_path = PathBuf::from(bundle.path.as_deref().expect("bundle path"))
        .join("metadata")
        .join("env-metadata.json");
    let env_metadata: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&env_metadata_path).expect("read env metadata"))
            .expect("parse env metadata");
    assert!(
        env_metadata["redaction_policy"]
            .as_str()
            .unwrap_or_default()
            .contains("allowlist-only"),
        "env metadata must document the secret-safe capture policy"
    );

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let bundle_dir = PathBuf::from(bundle.path.as_deref().expect("bundle path"));
        assert_eq!(
            std::fs::metadata(&bundle_dir)
                .expect("bundle dir metadata")
                .permissions()
                .mode()
                & 0o777,
            0o700,
            "forensic bundle directories contain local archive evidence and must be private"
        );
        for copied_file in [&manifest_path, &env_metadata_path] {
            assert_eq!(
                std::fs::metadata(copied_file)
                    .expect("copied forensic file metadata")
                    .permissions()
                    .mode()
                    & 0o777,
                0o600,
                "forensic bundle files contain local archive evidence and must be private"
            );
        }
    }
}
