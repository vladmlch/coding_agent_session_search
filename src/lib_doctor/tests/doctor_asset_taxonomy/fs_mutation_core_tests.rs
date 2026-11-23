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
fn doctor_plan_fingerprint_is_deterministic_and_action_sensitive() {
    let data_dir = Path::new("/tmp/cass");
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    let action_a = test_cleanup_action("/tmp/cass/index/.lexical-publish-backups/a", 10);
    let action_b = test_cleanup_action("/tmp/cass/index/.lexical-publish-backups/b", 20);
    let result = DiagCleanupApplyResult {
        mode: DoctorRepairMode::CleanupApply,
        approval_requirement: DoctorApprovalRequirement::ApprovalFingerprint,
        approval_fingerprint: "cleanup-v1-test".to_string(),
        before_generation_count: 2,
        before_reclaim_candidate_count: 2,
        before_reclaimable_bytes: 30,
        before_retained_bytes: 0,
        actions: vec![action_a.clone(), action_b.clone()],
        ..DiagCleanupApplyResult::default()
    };
    let reversed = DiagCleanupApplyResult {
        actions: vec![action_b, action_a],
        ..result.clone()
    };

    let plan = build_cleanup_doctor_plan(&result, data_dir, &db_path, &index_path);
    let reversed_plan = build_cleanup_doctor_plan(&reversed, data_dir, &db_path, &index_path);

    assert_eq!(
        plan.plan_fingerprint, reversed_plan.plan_fingerprint,
        "filesystem scan order must not change doctor approval fingerprints"
    );

    let mut changed = result.clone();
    changed.actions[0].planned_reclaimable_bytes = 11;
    let changed_plan = build_cleanup_doctor_plan(&changed, data_dir, &db_path, &index_path);
    assert_ne!(
        plan.plan_fingerprint, changed_plan.plan_fingerprint,
        "fingerprint must change when the action byte contract changes"
    );
}

#[test]
fn doctor_plan_fingerprint_covers_paths_authorities_asset_classes_and_hash_fields() {
    let data_dir = Path::new("/tmp/cass");
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    let result = DiagCleanupApplyResult {
        mode: DoctorRepairMode::CleanupApply,
        approval_requirement: DoctorApprovalRequirement::ApprovalFingerprint,
        approval_fingerprint: "cleanup-v1-approval-a".to_string(),
        before_generation_count: 1,
        before_reclaim_candidate_count: 1,
        before_reclaimable_bytes: 10,
        before_retained_bytes: 0,
        actions: vec![test_cleanup_action(
            "/tmp/cass/index/.lexical-publish-backups/a",
            10,
        )],
        ..DiagCleanupApplyResult::default()
    };

    let baseline = build_cleanup_doctor_plan(&result, data_dir, &db_path, &index_path);

    let mut changed_path = result.clone();
    changed_path.actions[0].path = "/tmp/cass/index/.lexical-publish-backups/b".to_string();
    let changed_path_plan =
        build_cleanup_doctor_plan(&changed_path, data_dir, &db_path, &index_path);
    assert_ne!(
        baseline.plan_fingerprint, changed_path_plan.plan_fingerprint,
        "fingerprint invariant failed: field=actions.target_path \
         expected safe behavior=changing cleanup target path must require a new approval"
    );

    let mut changed_action_class = result.clone();
    changed_action_class.actions[0].artifact_kind = "failed_lexical_generation".to_string();
    let changed_action_class_plan =
        build_cleanup_doctor_plan(&changed_action_class, data_dir, &db_path, &index_path);
    assert_ne!(
        baseline.plan_fingerprint, changed_action_class_plan.plan_fingerprint,
        "fingerprint invariant failed: field=actions.action_kind \
         expected safe behavior=changing cleanup action class must require a new approval"
    );

    let mut changed_asset_class = result.clone();
    changed_asset_class.actions[0].asset_safety =
        doctor_asset_safety(DoctorAssetClass::ReclaimableDerivedCache);
    let changed_asset_class_plan =
        build_cleanup_doctor_plan(&changed_asset_class, data_dir, &db_path, &index_path);
    assert_ne!(
        baseline.plan_fingerprint, changed_asset_class_plan.plan_fingerprint,
        "fingerprint invariant failed: field=actions.asset_class \
         expected safe behavior=changing asset class must require a new approval"
    );

    let mut changed_approval = result.clone();
    changed_approval.approval_fingerprint = "cleanup-v1-approval-b".to_string();
    let changed_approval_plan =
        build_cleanup_doctor_plan(&changed_approval, data_dir, &db_path, &index_path);
    assert_ne!(
        baseline.plan_fingerprint, changed_approval_plan.plan_fingerprint,
        "fingerprint invariant failed: field=approval_fingerprint \
         expected safe behavior=stale cleanup approval fingerprints must not match a new plan"
    );

    let mut changed_outcome = result.clone();
    changed_outcome.outcome_kind = DoctorRepairOutcomeKind::Blocked;
    let changed_outcome_plan =
        build_cleanup_doctor_plan(&changed_outcome, data_dir, &db_path, &index_path);
    assert_ne!(
        baseline.plan_fingerprint, changed_outcome_plan.plan_fingerprint,
        "fingerprint invariant failed: field=outcome_contract \
         expected safe behavior=blocked versus clean plans require distinct approvals"
    );

    let mut changed_artifact_hash = baseline.clone();
    changed_artifact_hash.artifact_manifest.artifacts[0].expected_content_blake3 =
        Some("expected-content-hash-a".to_string());
    let changed_artifact_hash_fingerprint = doctor_cleanup_plan_fingerprint(&changed_artifact_hash);
    assert_ne!(
        baseline.plan_fingerprint, changed_artifact_hash_fingerprint,
        "fingerprint invariant failed: field=artifact_manifest.artifacts.expected_content_blake3 \
         expected safe behavior=artifact checksum/hash drift must require a new approval"
    );
}

#[test]
fn doctor_cleanup_safety_gate_names_mode_taxonomy_and_path_blockers() {
    let data_dir = Path::new("/tmp/cass");
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    let mut action = test_cleanup_action("/tmp/outside/agent_search.db", 10);
    action.artifact_kind = "canonical_archive_db".to_string();
    action.asset_safety = doctor_asset_safety(DoctorAssetClass::CanonicalArchiveDb);

    let gate = doctor_safety_gate_for_cleanup_action(
        &action,
        data_dir,
        &db_path,
        &index_path,
        "cleanup-v1-approval",
    );

    assert_eq!(gate.mode, DoctorRepairMode::CleanupApply);
    assert_eq!(gate.asset_class, DoctorAssetClass::CanonicalArchiveDb);
    assert!(
        !gate.allowed_by_mode,
        "safety invariant failed: operation_kind=cleanup_apply \
         asset_class=canonical_archive_db expected safe behavior=cleanup mode must not mutate canonical archive DB"
    );
    assert!(
        !gate.allowed_by_taxonomy,
        "safety invariant failed: asset_class=canonical_archive_db \
         expected safe behavior=canonical archive DB is precious evidence, not automatic GC"
    );
    assert!(
        !gate.path_safe,
        "safety invariant failed: path_class=outside_data_dir \
         expected safe behavior=cleanup must reject targets outside the cass data dir"
    );
    for expected in [
        "mode_disallows_asset_class",
        "taxonomy_disallows_automatic_reclaim",
        "unsafe_or_missing_path",
    ] {
        assert!(
            gate.blocked_reasons.iter().any(|reason| reason == expected),
            "safety invariant failed: missing blocker={expected} \
             operation_kind=cleanup_apply asset_class=canonical_archive_db path_class=outside_data_dir \
             observed_blockers={:?}",
            gate.blocked_reasons
        );
    }
    assert!(
        !gate.passed,
        "safety invariant failed: cleanup_apply gate must fail closed when mode, taxonomy, and path guards all reject"
    );
}

#[test]
fn doctor_relative_path_guard_rejects_cross_platform_escape_forms() -> Result<(), String> {
    for safe in [
        "doctor/receipts/repair.json",
        "raw-mirror/v1/manifests/session.json",
        "./doctor/forensic-bundles/bundle.json",
        "unicode/session-Resume.json",
    ] {
        assert!(
            doctor_forensic_relative_path_is_safe(Path::new(safe)),
            "portable relative path should be accepted: {safe}"
        );
    }

    for unsafe_path in [
        "",
        "/tmp/cass/escape",
        "../escape",
        "doctor/../escape",
        r"C:\Users\agent\.codex\session.jsonl",
        "C:relative-windows-drive",
        r"doctor\receipts\repair.json",
        "doctor/CON",
        "doctor/NUL.txt",
        "doctor/COM1",
        "doctor/LPT9.log",
        "doctor/trailing-space ",
        "doctor/trailing-dot.",
        "doctor/alternate:stream",
        "doctor/control-\u{0007}",
    ] {
        assert!(
            !doctor_forensic_relative_path_is_safe(Path::new(unsafe_path)),
            "cross-platform unsafe relative path should be rejected: {unsafe_path:?}"
        );
    }

    let Some(portable) = doctor_forensic_portable_child_path(
        "raw-mirror-manifests",
        Path::new("nested/manifest.json"),
    ) else {
        return Err("portable child path should be accepted".to_string());
    };
    if !portable
        .to_string_lossy()
        .as_ref()
        .eq("raw-mirror-manifests/nested/manifest.json")
    {
        return Err("portable child path should use slash-separated archive form".to_string());
    }
    if doctor_forensic_portable_child_path("raw-mirror-manifests", Path::new("../manifest.json"))
        .is_some()
    {
        return Err("portable child paths must not admit traversal".to_string());
    }
    if doctor_forensic_portable_child_path("raw\\mirror", Path::new("manifest.json")).is_some() {
        return Err("portable child path prefixes must stay cross-platform safe".to_string());
    }
    Ok(())
}

#[test]
fn doctor_fs_mutation_executor_prunes_allowed_cleanup_target_with_receipt() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");
    let db_path = data_dir.join("agent_search.db");
    std::fs::write(&db_path, b"sqlite placeholder").expect("write db placeholder");

    let backup_parent = index_path
        .parent()
        .expect("index parent")
        .join(".lexical-publish-backups");
    std::fs::create_dir_all(&backup_parent).expect("create publish backup parent");
    let candidate = backup_parent.join("2026-05-05-prior-live");
    std::fs::create_dir_all(&candidate).expect("create cleanup candidate");
    std::fs::write(candidate.join("segment.idx"), b"reclaim me")
        .expect("write cleanup candidate payload");

    let planned_bytes = fs_dir_size(&candidate);
    let action = test_cleanup_action(&candidate.display().to_string(), planned_bytes);
    let action_id = doctor_cleanup_action_id(&action);
    let receipt = execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: "cleanup-v1-test-operation",
        action_id: &action_id,
        mutation_kind: DoctorFsMutationKind::PruneCleanupTarget,
        mode: DoctorRepairMode::CleanupApply,
        asset_class: DoctorAssetClass::RetainedPublishBackup,
        source_path: None,
        target_path: &candidate,
        data_dir: &data_dir,
        db_path: &db_path,
        index_path: &index_path,
        staging_root: None,
        expected_source_blake3: None,
        planned_bytes,
        required_min_age_seconds: None,
    });

    assert_eq!(receipt.status, DoctorActionStatus::Applied);
    assert_eq!(receipt.operation_id, "cleanup-v1-test-operation");
    assert_eq!(receipt.action_id, action_id);
    assert_eq!(
        receipt.mutation_kind,
        DoctorFsMutationKind::PruneCleanupTarget
    );
    assert_eq!(receipt.asset_class, DoctorAssetClass::RetainedPublishBackup);
    assert_eq!(receipt.planned_bytes, planned_bytes);
    assert_eq!(receipt.affected_bytes, planned_bytes);
    assert!(
        receipt
            .redacted_target_path
            .starts_with("[cass-data]/index/.lexical-publish-backups/"),
        "receipt should carry redacted path for operator diagnostics without leaking the full path: {:?}",
        receipt.redacted_target_path
    );
    for expected in [
        "mutation_kind_prune_cleanup_target",
        "mode_allows_asset_class",
        "taxonomy_allows_automatic_reclaim",
        "path_confined_to_reclaimable_cleanup_target",
        "filesystem_remove_completed",
    ] {
        assert!(
            receipt
                .precondition_checks
                .iter()
                .any(|observed| observed == expected),
            "missing executor receipt precondition {expected}: {:?}",
            receipt.precondition_checks
        );
    }
    assert!(
        !candidate.exists(),
        "allowed derived cleanup target should be removed by the audited executor"
    );
    assert!(
        db_path.exists(),
        "canonical archive DB must remain untouched"
    );
}

#[test]
fn doctor_fs_mutation_executor_refuses_precious_archive_unlink() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");
    let db_path = data_dir.join("agent_search.db");
    std::fs::write(&db_path, b"precious archive").expect("write db placeholder");

    let receipt = execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: "cleanup-v1-precious-refusal",
        action_id: "canonical-db-delete-attempt",
        mutation_kind: DoctorFsMutationKind::PruneCleanupTarget,
        mode: DoctorRepairMode::CleanupApply,
        asset_class: DoctorAssetClass::CanonicalArchiveDb,
        source_path: None,
        target_path: &db_path,
        data_dir: &data_dir,
        db_path: &db_path,
        index_path: &index_path,
        staging_root: None,
        expected_source_blake3: None,
        planned_bytes: fs_dir_size(&db_path),
        required_min_age_seconds: None,
    });

    assert_eq!(receipt.status, DoctorActionStatus::Blocked);
    assert_eq!(receipt.affected_bytes, 0);
    assert!(
        receipt.blocked_reasons.iter().any(|reason| {
            reason.contains("cleanup_apply mode does not allow this mutation")
                || reason.contains("taxonomy does not allow automatic reclaim")
        }),
        "precious archive refusal should name the policy blocker: {:?}",
        receipt.blocked_reasons
    );
    assert!(
        !receipt
            .precondition_checks
            .iter()
            .any(|check| check == "filesystem_remove_completed"),
        "blocked precious evidence must not reach the unlink/remove step"
    );
    assert!(db_path.exists(), "canonical archive DB must not be removed");
}

#[test]
fn doctor_fs_mutation_executor_blocks_missing_cleanup_target() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");
    let db_path = data_dir.join("agent_search.db");
    std::fs::write(&db_path, b"sqlite placeholder").expect("write db placeholder");
    let missing_candidate = index_path
        .parent()
        .expect("index parent")
        .join(".lexical-publish-backups")
        .join("missing-prior-live");

    let receipt = execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: "cleanup-v1-missing-target",
        action_id: "missing-target",
        mutation_kind: DoctorFsMutationKind::PruneCleanupTarget,
        mode: DoctorRepairMode::CleanupApply,
        asset_class: DoctorAssetClass::RetainedPublishBackup,
        source_path: None,
        target_path: &missing_candidate,
        data_dir: &data_dir,
        db_path: &db_path,
        index_path: &index_path,
        staging_root: None,
        expected_source_blake3: None,
        planned_bytes: 1,
        required_min_age_seconds: None,
    });

    assert_eq!(receipt.status, DoctorActionStatus::Blocked);
    assert!(
        receipt
            .blocked_reasons
            .iter()
            .any(|reason| reason.contains("unsafe cleanup target")),
        "missing targets must fail as an unsafe precondition, not as a best-effort deletion: {:?}",
        receipt.blocked_reasons
    );
    assert!(
        !receipt
            .precondition_checks
            .iter()
            .any(|check| check == "filesystem_remove_completed"),
        "missing target must not record a completed filesystem mutation"
    );
}

#[test]
fn doctor_fs_mutation_executor_removes_only_exact_legacy_index_lock() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");
    let db_path = data_dir.join("agent_search.db");
    std::fs::write(&db_path, b"precious archive").expect("write db placeholder");

    let lock_path = data_dir.join(".index.lock");
    std::fs::write(&lock_path, b"legacy lock").expect("write legacy lock");
    let planned_bytes = fs_dir_size(&lock_path);
    let action_id = doctor_fs_mutation_action_id(
        DoctorFsMutationKind::RemoveStaleLegacyIndexLock,
        DoctorRepairMode::SafeAutoRun,
        DoctorAssetClass::ReclaimableDerivedCache,
        &lock_path,
        planned_bytes,
    );
    let receipt = execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: "doctor-fix-stale-legacy-index-lock",
        action_id: &action_id,
        mutation_kind: DoctorFsMutationKind::RemoveStaleLegacyIndexLock,
        mode: DoctorRepairMode::SafeAutoRun,
        asset_class: DoctorAssetClass::ReclaimableDerivedCache,
        source_path: None,
        target_path: &lock_path,
        data_dir: &data_dir,
        db_path: &db_path,
        index_path: &index_path,
        staging_root: None,
        expected_source_blake3: None,
        planned_bytes,
        required_min_age_seconds: None,
    });

    assert_eq!(receipt.status, DoctorActionStatus::Applied);
    assert_eq!(
        receipt.mutation_kind,
        DoctorFsMutationKind::RemoveStaleLegacyIndexLock
    );
    assert_eq!(receipt.action_id, action_id);
    assert_eq!(receipt.affected_bytes, planned_bytes);
    for expected in [
        "mutation_kind_remove_stale_legacy_index_lock",
        "mode_allows_asset_class",
        "asset_class_is_reclaimable_derived_cache",
        "path_confined_to_legacy_index_lock",
        "filesystem_remove_completed",
    ] {
        assert!(
            receipt
                .precondition_checks
                .iter()
                .any(|observed| observed == expected),
            "missing legacy lock receipt precondition {expected}: {:?}",
            receipt.precondition_checks
        );
    }
    assert!(!lock_path.exists(), "exact legacy lock should be removed");
    assert!(db_path.exists(), "canonical DB must remain untouched");

    let near_miss = data_dir.join(".index.lock.bak");
    std::fs::write(&near_miss, b"not the lock").expect("write near miss");
    let receipt = execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: "doctor-fix-stale-legacy-index-lock",
        action_id: "near-miss-lock",
        mutation_kind: DoctorFsMutationKind::RemoveStaleLegacyIndexLock,
        mode: DoctorRepairMode::SafeAutoRun,
        asset_class: DoctorAssetClass::ReclaimableDerivedCache,
        source_path: None,
        target_path: &near_miss,
        data_dir: &data_dir,
        db_path: &db_path,
        index_path: &index_path,
        staging_root: None,
        expected_source_blake3: None,
        planned_bytes: fs_dir_size(&near_miss),
        required_min_age_seconds: None,
    });
    assert_eq!(receipt.status, DoctorActionStatus::Blocked);
    assert!(
        receipt
            .blocked_reasons
            .iter()
            .any(|reason| reason.contains("unsafe legacy index lock target")),
        "near-miss lock name must be rejected by exact-path guard: {:?}",
        receipt.blocked_reasons
    );
    assert!(near_miss.exists(), "near-miss file must not be removed");
}

#[test]
fn doctor_fs_mutation_executor_requires_stale_age_when_requested() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");
    let db_path = data_dir.join("agent_search.db");
    std::fs::write(&db_path, b"precious archive").expect("write db placeholder");
    let lock_path = data_dir.join(".index.lock");
    std::fs::write(&lock_path, b"fresh legacy lock").expect("write legacy lock");

    let receipt = execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: "doctor-fix-stale-legacy-index-lock",
        action_id: "fresh-lock",
        mutation_kind: DoctorFsMutationKind::RemoveStaleLegacyIndexLock,
        mode: DoctorRepairMode::SafeAutoRun,
        asset_class: DoctorAssetClass::ReclaimableDerivedCache,
        source_path: None,
        target_path: &lock_path,
        data_dir: &data_dir,
        db_path: &db_path,
        index_path: &index_path,
        staging_root: None,
        expected_source_blake3: None,
        planned_bytes: fs_dir_size(&lock_path),
        required_min_age_seconds: Some(u64::MAX),
    });

    assert_eq!(receipt.status, DoctorActionStatus::Blocked);
    assert!(
        receipt
            .blocked_reasons
            .iter()
            .any(|reason| reason.contains("required_min_age_seconds")),
        "age-gated legacy lock removal must name the stale-age precondition: {:?}",
        receipt.blocked_reasons
    );
    assert!(
        !receipt
            .precondition_checks
            .iter()
            .any(|check| check == "filesystem_remove_completed"),
        "fresh lock must not reach filesystem removal"
    );
    assert!(lock_path.exists(), "fresh lock must not be removed");
}

#[test]
fn doctor_fs_mutation_executor_copies_verified_source_to_staging_with_receipt() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");
    let db_path = data_dir.join("agent_search.db");
    std::fs::write(&db_path, b"precious archive").expect("write db placeholder");

    let source_path = data_dir
        .join("raw-mirror")
        .join("v1")
        .join("blobs")
        .join("source.raw");
    let source_bytes = b"{\"type\":\"message\",\"content\":\"preserve me\"}\n";
    std::fs::create_dir_all(source_path.parent().expect("source parent"))
        .expect("create source parent");
    std::fs::write(&source_path, source_bytes).expect("write source");
    let expected_source_blake3 = blake3::hash(source_bytes).to_hex().to_string();

    let staging_root = data_dir.join("doctor-staging").join("op-1");
    let target_path = staging_root.join("candidate").join("source.raw");
    std::fs::create_dir_all(target_path.parent().expect("target parent"))
        .expect("create target parent");

    let receipt = execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: "reconstruct-promote-copy-source",
        action_id: "copy-raw-mirror-source-to-staging",
        mutation_kind: DoctorFsMutationKind::CopyFileToStaging,
        mode: DoctorRepairMode::ReconstructPromote,
        asset_class: DoctorAssetClass::RawMirrorBlob,
        source_path: Some(&source_path),
        target_path: &target_path,
        data_dir: &data_dir,
        db_path: &db_path,
        index_path: &index_path,
        staging_root: Some(&staging_root),
        expected_source_blake3: Some(&expected_source_blake3),
        planned_bytes: source_bytes.len() as u64,
        required_min_age_seconds: None,
    });

    assert_eq!(receipt.status, DoctorActionStatus::Applied);
    assert_eq!(receipt.operation_id, "reconstruct-promote-copy-source");
    assert_eq!(receipt.action_id, "copy-raw-mirror-source-to-staging");
    assert_eq!(
        receipt.mutation_kind,
        DoctorFsMutationKind::CopyFileToStaging
    );
    assert_eq!(receipt.asset_class, DoctorAssetClass::RawMirrorBlob);
    assert_eq!(receipt.planned_bytes, source_bytes.len() as u64);
    assert_eq!(receipt.affected_bytes, source_bytes.len() as u64);
    assert_eq!(
        receipt.expected_source_blake3.as_deref(),
        Some(expected_source_blake3.as_str())
    );
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
        Some("[cass-data]/raw-mirror/v1/blobs/source.raw")
    );
    assert_eq!(
        receipt.redacted_target_path,
        "[cass-data]/doctor-staging/op-1/candidate/source.raw"
    );
    assert_eq!(
        receipt.redacted_staging_root.as_deref(),
        Some("[cass-data]/doctor-staging/op-1")
    );
    for expected in [
        "mutation_kind_copy_file_to_staging",
        "mode_allows_asset_operation",
        "source_path_confined_to_data_dir",
        "target_path_confined_to_staging_root",
        "target_does_not_exist",
        "target_parent_exists",
        "source_blake3_recorded",
        "expected_source_blake3_matched",
        "filesystem_copy_completed",
        "target_blake3_matched_source",
        "target_file_sync_completed",
        "target_parent_sync_completed",
    ] {
        assert!(
            receipt
                .precondition_checks
                .iter()
                .any(|observed| observed == expected),
            "missing copy receipt precondition {expected}: {:?}",
            receipt.precondition_checks
        );
    }
    assert_eq!(
        std::fs::read(&target_path).expect("read copied target"),
        source_bytes
    );
    assert!(
        source_path.exists(),
        "copy must preserve the source evidence"
    );
    assert!(
        db_path.exists(),
        "copy must not touch the canonical archive DB"
    );
}

// Regression for #271: content-addressed staging copies (raw-mirror blobs
// named by their blake3) can legitimately collide when two distinct verified
// manifests reference byte-identical evidence. The second copy must be a
// benign idempotent Applied, not Blocked, otherwise a single duplicate poisons
// the candidate lifecycle gate and a verified, promote_allowed, higher-coverage
// candidate is wrongly stamped `blocked` instead of `completed`.
#[test]
fn doctor_fs_mutation_executor_copy_is_idempotent_for_identical_existing_target() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");
    let db_path = data_dir.join("agent_search.db");
    std::fs::write(&db_path, b"precious archive").expect("write db placeholder");

    let source_bytes = b"{\"type\":\"message\",\"content\":\"shared evidence blob\"}\n";
    let expected_source_blake3 = blake3::hash(source_bytes).to_hex().to_string();

    // Two distinct verified manifests can reference byte-identical blobs.
    let first_source = data_dir.join("raw-mirror").join("a").join("source.raw");
    let second_source = data_dir.join("raw-mirror").join("b").join("source.raw");
    for path in [&first_source, &second_source] {
        std::fs::create_dir_all(path.parent().expect("source parent"))
            .expect("create source parent");
        std::fs::write(path, source_bytes).expect("write source");
    }

    let staging_root = data_dir.join("doctor-staging").join("op-1");
    // Content-addressed staging target shared by both manifests.
    let target_path = staging_root
        .join("candidate")
        .join("evidence")
        .join(format!("{expected_source_blake3}.raw"));
    std::fs::create_dir_all(target_path.parent().expect("target parent"))
        .expect("create target parent");

    let first = execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: "reconstruct-promote-copy-source",
        action_id: "copy-raw-mirror-blob-to-candidate",
        mutation_kind: DoctorFsMutationKind::CopyFileToStaging,
        mode: DoctorRepairMode::ReconstructPromote,
        asset_class: DoctorAssetClass::RawMirrorBlob,
        source_path: Some(&first_source),
        target_path: &target_path,
        data_dir: &data_dir,
        db_path: &db_path,
        index_path: &index_path,
        staging_root: Some(&staging_root),
        expected_source_blake3: Some(&expected_source_blake3),
        planned_bytes: source_bytes.len() as u64,
        required_min_age_seconds: None,
    });
    assert_eq!(
        first.status,
        DoctorActionStatus::Applied,
        "first content-addressed copy should apply: {:?}",
        first.blocked_reasons
    );

    // Second copy hits an already-staged byte-identical target: must be a
    // benign idempotent Applied, not Blocked.
    let second = execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: "reconstruct-promote-copy-source",
        action_id: "copy-raw-mirror-blob-to-candidate",
        mutation_kind: DoctorFsMutationKind::CopyFileToStaging,
        mode: DoctorRepairMode::ReconstructPromote,
        asset_class: DoctorAssetClass::RawMirrorBlob,
        source_path: Some(&second_source),
        target_path: &target_path,
        data_dir: &data_dir,
        db_path: &db_path,
        index_path: &index_path,
        staging_root: Some(&staging_root),
        expected_source_blake3: Some(&expected_source_blake3),
        planned_bytes: source_bytes.len() as u64,
        required_min_age_seconds: None,
    });
    assert_eq!(
        second.status,
        DoctorActionStatus::Applied,
        "duplicate content-addressed copy must be idempotent, not blocked: {:?}",
        second.blocked_reasons
    );
    assert_eq!(second.affected_bytes, 0, "idempotent copy writes no bytes");
    assert_eq!(
        second.actual_target_blake3.as_deref(),
        Some(expected_source_blake3.as_str())
    );
    assert!(
        second
            .precondition_checks
            .iter()
            .any(|check| check == "existing_staging_target_content_matched_source"),
        "idempotent copy should record the content-match precondition: {:?}",
        second.precondition_checks
    );
    assert!(
        second.blocked_reasons.is_empty(),
        "idempotent copy must not carry blocking reasons: {:?}",
        second.blocked_reasons
    );

    // A genuine content mismatch at the same target is still refused.
    let conflicting_source = data_dir.join("raw-mirror").join("c").join("source.raw");
    std::fs::create_dir_all(conflicting_source.parent().expect("conflict parent"))
        .expect("create conflict parent");
    let conflict_bytes = b"different content at the same staging path\n";
    std::fs::write(&conflicting_source, conflict_bytes).expect("write conflict source");
    let conflict_blake3 = blake3::hash(conflict_bytes).to_hex().to_string();
    let conflict = execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: "reconstruct-promote-copy-source",
        action_id: "copy-raw-mirror-blob-to-candidate",
        mutation_kind: DoctorFsMutationKind::CopyFileToStaging,
        mode: DoctorRepairMode::ReconstructPromote,
        asset_class: DoctorAssetClass::RawMirrorBlob,
        source_path: Some(&conflicting_source),
        target_path: &target_path,
        data_dir: &data_dir,
        db_path: &db_path,
        index_path: &index_path,
        staging_root: Some(&staging_root),
        expected_source_blake3: Some(&conflict_blake3),
        planned_bytes: conflict_bytes.len() as u64,
        required_min_age_seconds: None,
    });
    assert_eq!(
        conflict.status,
        DoctorActionStatus::Blocked,
        "a real content conflict at the staging target must still be refused"
    );
    assert!(
        conflict
            .blocked_reasons
            .iter()
            .any(|reason| reason.contains("different content")),
        "content conflict should name the divergence: {:?}",
        conflict.blocked_reasons
    );

    // Original staged content is untouched by the refused conflicting copy.
    assert_eq!(
        std::fs::read(&target_path).expect("read staged target"),
        source_bytes
    );
}

#[test]
fn doctor_fs_mutation_executor_reports_copy_parent_sync_failure_after_file_sync() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");
    let db_path = data_dir.join("agent_search.db");
    std::fs::write(&db_path, b"precious archive").expect("write db placeholder");

    let source_path = data_dir.join("raw-mirror").join("source.raw");
    let source_bytes = b"source bytes preserved through copy";
    std::fs::create_dir_all(source_path.parent().expect("source parent"))
        .expect("create source parent");
    std::fs::write(&source_path, source_bytes).expect("write source");
    let expected_source_blake3 = blake3::hash(source_bytes).to_hex().to_string();

    let staging_root = data_dir.join("doctor-staging").join("copy-dir-sync-fail");
    let target_path = staging_root.join("candidate.raw");
    std::fs::create_dir_all(target_path.parent().expect("target parent"))
        .expect("create copy target parent");

    doctor_test_inject_next_directory_sync_failure();
    let receipt = execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: "reconstruct-promote-copy-source",
        action_id: "copy-dir-sync-fail",
        mutation_kind: DoctorFsMutationKind::CopyFileToStaging,
        mode: DoctorRepairMode::ReconstructPromote,
        asset_class: DoctorAssetClass::RawMirrorBlob,
        source_path: Some(&source_path),
        target_path: &target_path,
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
        "copy parent sync failure should be explicit: {:?}",
        receipt.blocked_reasons
    );
    assert!(
        receipt
            .precondition_checks
            .iter()
            .any(|check| check == "target_file_sync_completed"),
        "copy parent sync failure should record that the file fsync completed"
    );
    assert!(
        !receipt
            .precondition_checks
            .iter()
            .any(|check| check == "target_parent_sync_completed"),
        "failed parent fsync must not be reported as completed"
    );
    assert_eq!(
        std::fs::read(&target_path).expect("read copied target after sync failure"),
        source_bytes,
        "failed receipt must leave copied staging bytes available for inspection"
    );
    assert!(
        source_path.exists(),
        "copy parent sync failure must preserve source evidence"
    );
}

#[test]
fn doctor_fs_mutation_executor_snapshots_archive_to_staging_without_deleting_source() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");
    let db_path = data_dir.join("agent_search.db");
    let archive_bytes = b"precious canonical archive bytes";
    std::fs::write(&db_path, archive_bytes).expect("write archive db placeholder");
    let expected_source_blake3 = blake3::hash(archive_bytes).to_hex().to_string();

    let staging_root = data_dir.join("doctor-staging").join("snapshot-op-1");
    let target_path = staging_root.join("before").join("agent_search.db");
    std::fs::create_dir_all(target_path.parent().expect("target parent"))
        .expect("create snapshot target parent");

    let receipt = execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: "reconstruct-promote-snapshot",
        action_id: "snapshot-archive-before-promote",
        mutation_kind: DoctorFsMutationKind::CopyFileToStaging,
        mode: DoctorRepairMode::ReconstructPromote,
        asset_class: DoctorAssetClass::CanonicalArchiveDb,
        source_path: Some(&db_path),
        target_path: &target_path,
        data_dir: &data_dir,
        db_path: &db_path,
        index_path: &index_path,
        staging_root: Some(&staging_root),
        expected_source_blake3: Some(&expected_source_blake3),
        planned_bytes: archive_bytes.len() as u64,
        required_min_age_seconds: None,
    });

    assert_eq!(receipt.status, DoctorActionStatus::Applied);
    assert_eq!(receipt.asset_class, DoctorAssetClass::CanonicalArchiveDb);
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
        Some("[cass-data]/agent_search.db")
    );
    assert_eq!(
        receipt.redacted_target_path,
        "[cass-data]/doctor-staging/snapshot-op-1/before/agent_search.db"
    );
    assert_eq!(
        std::fs::read(&target_path).expect("read snapshot target"),
        archive_bytes
    );
    assert_eq!(
        std::fs::read(&db_path).expect("read original archive"),
        archive_bytes,
        "snapshot copy must preserve the precious source archive"
    );
}

#[test]
fn doctor_fs_write_mutation_writes_verified_payload_to_staging_with_receipt() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");
    let db_path = data_dir.join("agent_search.db");
    std::fs::write(&db_path, b"precious archive").expect("write db placeholder");

    let staging_root = data_dir.join("doctor-staging").join("write-op-1");
    let target_path = staging_root.join("candidate").join("derived.idx");
    std::fs::create_dir_all(target_path.parent().expect("target parent"))
        .expect("create write target parent");
    let payload = b"derived lexical candidate bytes";
    let expected_payload_blake3 = blake3::hash(payload).to_hex().to_string();

    let receipt = execute_doctor_fs_write_mutation(DoctorFsWriteMutationRequest {
        operation_id: "repair-apply-write-derived",
        action_id: "write-derived-candidate",
        mode: DoctorRepairMode::RepairApply,
        asset_class: DoctorAssetClass::DerivedLexicalIndex,
        target_path: &target_path,
        data_dir: &data_dir,
        staging_root: &staging_root,
        payload,
        expected_payload_blake3: &expected_payload_blake3,
        planned_bytes: payload.len() as u64,
    });

    assert_eq!(receipt.status, DoctorActionStatus::Applied);
    assert_eq!(
        receipt.mutation_kind,
        DoctorFsMutationKind::WriteFileToStaging
    );
    assert_eq!(receipt.asset_class, DoctorAssetClass::DerivedLexicalIndex);
    assert_eq!(receipt.planned_bytes, payload.len() as u64);
    assert_eq!(receipt.affected_bytes, payload.len() as u64);
    assert_eq!(
        receipt.actual_target_blake3.as_deref(),
        Some(expected_payload_blake3.as_str())
    );
    assert_eq!(receipt.source_path, None);
    assert_eq!(
        receipt.redacted_target_path,
        "[cass-data]/doctor-staging/write-op-1/candidate/derived.idx"
    );
    assert_eq!(
        receipt.redacted_staging_root.as_deref(),
        Some("[cass-data]/doctor-staging/write-op-1")
    );
    for expected in [
        "mutation_kind_write_file_to_staging",
        "mode_allows_asset_operation",
        "planned_bytes_match_payload",
        "expected_payload_blake3_matched",
        "target_parent_exists",
        "target_path_confined_to_staging_root",
        "target_does_not_exist",
        "filesystem_write_completed",
        "target_blake3_matched_payload",
        "target_file_sync_completed",
        "target_parent_sync_completed",
    ] {
        assert!(
            receipt
                .precondition_checks
                .iter()
                .any(|observed| observed == expected),
            "missing write receipt precondition {expected}: {:?}",
            receipt.precondition_checks
        );
    }
    assert_eq!(
        std::fs::read(&target_path).expect("read written target"),
        payload
    );
    assert!(
        db_path.exists(),
        "staging write must not touch the canonical archive DB"
    );
}

#[test]
fn doctor_fs_write_mutation_refuses_payload_hash_mismatch() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");
    let staging_root = data_dir.join("doctor-staging").join("write-hash-mismatch");
    let target_path = staging_root.join("candidate.idx");
    std::fs::create_dir_all(target_path.parent().expect("target parent"))
        .expect("create write target parent");
    let payload = b"derived lexical candidate bytes";

    let receipt = execute_doctor_fs_write_mutation(DoctorFsWriteMutationRequest {
        operation_id: "repair-apply-write-derived",
        action_id: "write-hash-mismatch",
        mode: DoctorRepairMode::RepairApply,
        asset_class: DoctorAssetClass::DerivedLexicalIndex,
        target_path: &target_path,
        data_dir: &data_dir,
        staging_root: &staging_root,
        payload,
        expected_payload_blake3: "doctor-test-wrong-payload-hash",
        planned_bytes: payload.len() as u64,
    });

    assert_eq!(receipt.status, DoctorActionStatus::Blocked);
    assert!(
        receipt
            .blocked_reasons
            .iter()
            .any(|reason| reason.contains("expected payload blake3")),
        "payload hash mismatch should be a pre-write blocker: {:?}",
        receipt.blocked_reasons
    );
    assert!(
        !target_path.exists(),
        "blocked payload hash mismatch must not create a staging file"
    );
}

#[test]
fn doctor_fs_write_mutation_refuses_planned_byte_mismatch_before_write() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");
    let staging_root = data_dir.join("doctor-staging").join("write-size-mismatch");
    let target_path = staging_root.join("candidate.idx");
    std::fs::create_dir_all(target_path.parent().expect("target parent"))
        .expect("create write target parent");
    let payload = b"derived lexical candidate bytes";
    let expected_payload_blake3 = blake3::hash(payload).to_hex().to_string();

    let receipt = execute_doctor_fs_write_mutation(DoctorFsWriteMutationRequest {
        operation_id: "repair-apply-write-derived",
        action_id: "write-size-mismatch",
        mode: DoctorRepairMode::RepairApply,
        asset_class: DoctorAssetClass::DerivedLexicalIndex,
        target_path: &target_path,
        data_dir: &data_dir,
        staging_root: &staging_root,
        payload,
        expected_payload_blake3: &expected_payload_blake3,
        planned_bytes: payload.len() as u64 + 1,
    });

    assert_eq!(receipt.status, DoctorActionStatus::Blocked);
    assert!(
        receipt
            .blocked_reasons
            .iter()
            .any(|reason| reason.contains("planned_bytes")),
        "planned byte mismatch should be a pre-write blocker: {:?}",
        receipt.blocked_reasons
    );
    assert!(
        !target_path.exists(),
        "blocked planned byte mismatch must not create a staging file"
    );
}

#[test]
fn doctor_fs_write_mutation_refuses_existing_staging_target() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");
    let staging_root = data_dir.join("doctor-staging").join("write-existing");
    let target_path = staging_root.join("candidate.idx");
    std::fs::create_dir_all(target_path.parent().expect("target parent"))
        .expect("create write target parent");
    std::fs::write(&target_path, b"existing staging bytes").expect("write existing target");
    let payload = b"derived lexical candidate bytes";
    let expected_payload_blake3 = blake3::hash(payload).to_hex().to_string();

    let receipt = execute_doctor_fs_write_mutation(DoctorFsWriteMutationRequest {
        operation_id: "repair-apply-write-derived",
        action_id: "write-existing-target",
        mode: DoctorRepairMode::RepairApply,
        asset_class: DoctorAssetClass::DerivedLexicalIndex,
        target_path: &target_path,
        data_dir: &data_dir,
        staging_root: &staging_root,
        payload,
        expected_payload_blake3: &expected_payload_blake3,
        planned_bytes: payload.len() as u64,
    });

    assert_eq!(receipt.status, DoctorActionStatus::Blocked);
    assert!(
        receipt
            .blocked_reasons
            .iter()
            .any(|reason| reason.contains("existing staging target")),
        "staging write must refuse existing targets: {:?}",
        receipt.blocked_reasons
    );
    assert_eq!(
        std::fs::read(&target_path).expect("read existing target"),
        b"existing staging bytes"
    );
}

#[test]
fn doctor_fs_write_mutation_reports_file_sync_failure_after_write() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");
    let staging_root = data_dir.join("doctor-staging").join("write-sync-fail");
    let target_path = staging_root.join("candidate.idx");
    std::fs::create_dir_all(target_path.parent().expect("target parent"))
        .expect("create write target parent");
    let payload = b"derived lexical candidate bytes";
    let expected_payload_blake3 = blake3::hash(payload).to_hex().to_string();

    doctor_test_inject_next_file_sync_failure();
    let receipt = execute_doctor_fs_write_mutation(DoctorFsWriteMutationRequest {
        operation_id: "repair-apply-write-derived",
        action_id: "write-file-sync-fail",
        mode: DoctorRepairMode::RepairApply,
        asset_class: DoctorAssetClass::DerivedLexicalIndex,
        target_path: &target_path,
        data_dir: &data_dir,
        staging_root: &staging_root,
        payload,
        expected_payload_blake3: &expected_payload_blake3,
        planned_bytes: payload.len() as u64,
    });

    assert_eq!(receipt.status, DoctorActionStatus::Failed);
    assert!(
        receipt
            .blocked_reasons
            .iter()
            .any(|reason| reason.contains("injected test failure syncing written staging target")),
        "write sync failure should be explicit: {:?}",
        receipt.blocked_reasons
    );
    assert!(
        receipt
            .precondition_checks
            .iter()
            .any(|check| check == "filesystem_write_completed"),
        "sync failure should record that the write happened"
    );
    assert!(
        !receipt
            .precondition_checks
            .iter()
            .any(|check| check == "target_file_sync_completed"),
        "sync failure must not claim target fsync completed"
    );
    assert_eq!(
        std::fs::read(&target_path).expect("read written target after sync failure"),
        payload,
        "failed receipt must leave the staged bytes available for inspection"
    );
}

#[test]
fn doctor_fs_write_mutation_reports_parent_sync_failure_after_file_sync() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");
    let staging_root = data_dir.join("doctor-staging").join("write-dir-sync-fail");
    let target_path = staging_root.join("candidate.idx");
    std::fs::create_dir_all(target_path.parent().expect("target parent"))
        .expect("create write target parent");
    let payload = b"derived lexical candidate bytes";
    let expected_payload_blake3 = blake3::hash(payload).to_hex().to_string();

    doctor_test_inject_next_directory_sync_failure();
    let receipt = execute_doctor_fs_write_mutation(DoctorFsWriteMutationRequest {
        operation_id: "repair-apply-write-derived",
        action_id: "write-dir-sync-fail",
        mode: DoctorRepairMode::RepairApply,
        asset_class: DoctorAssetClass::DerivedLexicalIndex,
        target_path: &target_path,
        data_dir: &data_dir,
        staging_root: &staging_root,
        payload,
        expected_payload_blake3: &expected_payload_blake3,
        planned_bytes: payload.len() as u64,
    });

    assert_eq!(receipt.status, DoctorActionStatus::Failed);
    assert!(
        receipt
            .blocked_reasons
            .iter()
            .any(|reason| reason.contains("injected test failure syncing directory")),
        "parent directory sync failure should be explicit: {:?}",
        receipt.blocked_reasons
    );
    assert!(
        receipt
            .precondition_checks
            .iter()
            .any(|check| check == "target_file_sync_completed"),
        "parent sync failure should record that file fsync completed"
    );
    assert!(
        !receipt
            .precondition_checks
            .iter()
            .any(|check| check == "target_parent_sync_completed"),
        "parent sync failure must not claim parent fsync completed"
    );
    assert_eq!(
        std::fs::read(&target_path).expect("read written target after parent sync failure"),
        payload,
        "failed receipt must leave the staged bytes available for inspection"
    );
}

#[test]
fn doctor_fs_mutation_executor_refuses_to_overwrite_staging_target() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");
    let db_path = data_dir.join("agent_search.db");
    std::fs::write(&db_path, b"precious archive").expect("write db placeholder");

    let source_path = data_dir.join("raw-mirror").join("source.raw");
    let source_bytes = b"new staging bytes";
    std::fs::create_dir_all(source_path.parent().expect("source parent"))
        .expect("create source parent");
    std::fs::write(&source_path, source_bytes).expect("write source");
    let expected_source_blake3 = blake3::hash(source_bytes).to_hex().to_string();

    let staging_root = data_dir.join("doctor-staging").join("op-overwrite");
    let target_path = staging_root.join("candidate.raw");
    std::fs::create_dir_all(target_path.parent().expect("target parent"))
        .expect("create target parent");
    std::fs::write(&target_path, b"existing staging bytes").expect("write existing target");

    let receipt = execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: "reconstruct-promote-copy-source",
        action_id: "copy-existing-target",
        mutation_kind: DoctorFsMutationKind::CopyFileToStaging,
        mode: DoctorRepairMode::ReconstructPromote,
        asset_class: DoctorAssetClass::RawMirrorBlob,
        source_path: Some(&source_path),
        target_path: &target_path,
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
            .any(|reason| reason.contains("overwrite existing staging target")),
        "overwrite refusal should name the existing target precondition: {:?}",
        receipt.blocked_reasons
    );
    assert!(
        !receipt
            .precondition_checks
            .iter()
            .any(|check| check == "filesystem_copy_completed"),
        "blocked overwrite must not reach filesystem copy"
    );
    assert_eq!(
        std::fs::read(&target_path).expect("read target"),
        b"existing staging bytes"
    );
}

#[test]
fn doctor_fs_mutation_executor_blocks_copy_with_missing_staging_parent() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");
    let db_path = data_dir.join("agent_search.db");
    std::fs::write(&db_path, b"precious archive").expect("write db placeholder");

    let source_path = data_dir.join("raw-mirror").join("source.raw");
    let source_bytes = b"source bytes";
    std::fs::create_dir_all(source_path.parent().expect("source parent"))
        .expect("create source parent");
    std::fs::write(&source_path, source_bytes).expect("write source");
    let expected_source_blake3 = blake3::hash(source_bytes).to_hex().to_string();

    let staging_root = data_dir.join("doctor-staging").join("op-missing-parent");
    std::fs::create_dir_all(&staging_root).expect("create staging root");
    let target_path = staging_root.join("missing").join("candidate.raw");

    let receipt = execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: "reconstruct-promote-copy-source",
        action_id: "copy-missing-parent",
        mutation_kind: DoctorFsMutationKind::CopyFileToStaging,
        mode: DoctorRepairMode::ReconstructPromote,
        asset_class: DoctorAssetClass::RawMirrorBlob,
        source_path: Some(&source_path),
        target_path: &target_path,
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
            .any(|reason| reason.contains("missing parent")),
        "missing staging parents must be refused explicitly before copy: {:?}",
        receipt.blocked_reasons
    );
    assert!(
        !target_path.exists(),
        "blocked copy must not create parents"
    );
}

#[test]
fn doctor_fs_mutation_executor_blocks_copy_when_source_hash_mismatches() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");
    let db_path = data_dir.join("agent_search.db");
    std::fs::write(&db_path, b"precious archive").expect("write db placeholder");

    let source_path = data_dir.join("raw-mirror").join("source.raw");
    let source_bytes = b"source bytes";
    std::fs::create_dir_all(source_path.parent().expect("source parent"))
        .expect("create source parent");
    std::fs::write(&source_path, source_bytes).expect("write source");
    let actual_source_blake3 = blake3::hash(source_bytes).to_hex().to_string();

    let staging_root = data_dir.join("doctor-staging").join("op-hash-mismatch");
    let target_path = staging_root.join("candidate.raw");
    std::fs::create_dir_all(target_path.parent().expect("target parent"))
        .expect("create target parent");

    let receipt = execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: "reconstruct-promote-copy-source",
        action_id: "copy-hash-mismatch",
        mutation_kind: DoctorFsMutationKind::CopyFileToStaging,
        mode: DoctorRepairMode::ReconstructPromote,
        asset_class: DoctorAssetClass::RawMirrorBlob,
        source_path: Some(&source_path),
        target_path: &target_path,
        data_dir: &data_dir,
        db_path: &db_path,
        index_path: &index_path,
        staging_root: Some(&staging_root),
        expected_source_blake3: Some("doctor-test-wrong-source-hash"),
        planned_bytes: source_bytes.len() as u64,
        required_min_age_seconds: None,
    });

    assert_eq!(receipt.status, DoctorActionStatus::Blocked);
    assert_eq!(
        receipt.actual_source_blake3.as_deref(),
        Some(actual_source_blake3.as_str())
    );
    assert!(
        receipt
            .blocked_reasons
            .iter()
            .any(|reason| reason.contains("expected blake3")),
        "source hash mismatch should be visible in the receipt: {:?}",
        receipt.blocked_reasons
    );
    assert!(
        !receipt
            .precondition_checks
            .iter()
            .any(|check| check == "filesystem_copy_completed"),
        "hash mismatch must stop before filesystem copy"
    );
    assert!(
        !target_path.exists(),
        "hash mismatch must not create target"
    );
}

#[test]
fn doctor_fs_mutation_executor_blocks_copy_target_outside_staging_root() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let index_path = data_dir.join("index").join("live-generation");
    std::fs::create_dir_all(&index_path).expect("create live index");
    let db_path = data_dir.join("agent_search.db");
    std::fs::write(&db_path, b"precious archive").expect("write db placeholder");

    let source_path = data_dir.join("raw-mirror").join("source.raw");
    let source_bytes = b"source bytes";
    std::fs::create_dir_all(source_path.parent().expect("source parent"))
        .expect("create source parent");
    std::fs::write(&source_path, source_bytes).expect("write source");
    let expected_source_blake3 = blake3::hash(source_bytes).to_hex().to_string();

    let staging_root = data_dir.join("doctor-staging").join("op-outside");
    std::fs::create_dir_all(&staging_root).expect("create staging root");
    let outside_target_parent = data_dir.join("not-staging");
    std::fs::create_dir_all(&outside_target_parent).expect("create outside parent");
    let outside_target = outside_target_parent.join("candidate.raw");

    let receipt = execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: "reconstruct-promote-copy-source",
        action_id: "copy-outside-staging",
        mutation_kind: DoctorFsMutationKind::CopyFileToStaging,
        mode: DoctorRepairMode::ReconstructPromote,
        asset_class: DoctorAssetClass::RawMirrorBlob,
        source_path: Some(&source_path),
        target_path: &outside_target,
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
            .any(|reason| reason.contains("unsafe staging target")),
        "target outside staging root should be rejected with an explicit blocker: {:?}",
        receipt.blocked_reasons
    );
    assert!(
        !receipt
            .precondition_checks
            .iter()
            .any(|check| check == "filesystem_copy_completed"),
        "unsafe target must not reach filesystem copy"
    );
    assert!(
        !outside_target.exists(),
        "blocked copy must not write outside staging"
    );
}
