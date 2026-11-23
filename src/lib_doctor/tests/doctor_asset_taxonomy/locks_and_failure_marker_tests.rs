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
fn doctor_failure_marker_round_trips_and_preserves_collision_evidence() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path();
    let repair_class = "repair_apply";
    let marker = test_failure_marker(repair_class, "op-same", 1_733_001_111_000);

    let first_path =
        write_doctor_repair_failure_marker(data_dir, &marker).expect("write first marker");
    let second_path =
        write_doctor_repair_failure_marker(data_dir, &marker).expect("write second marker");

    assert_ne!(
        first_path, second_path,
        "colliding marker writes must create a new evidence file"
    );
    assert!(first_path.exists(), "first marker must be preserved");
    assert!(second_path.exists(), "second marker must be preserved");

    let report = collect_doctor_repair_failure_marker(data_dir, repair_class);
    assert!(report.found);
    assert_eq!(report.parse_status, "ok");
    assert_eq!(report.operation_id.as_deref(), Some("op-same"));
    assert_eq!(
        report.failed_checks,
        vec!["rebuild:repair-previously-failed".to_string()]
    );
    assert_eq!(
        report.path.as_deref(),
        Some(second_path.to_string_lossy().as_ref()),
        "newest marker should be the active repeat-repair blocker"
    );
}

#[test]
fn doctor_failure_marker_corrupt_json_fails_closed() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path();
    let repair_class = "repair_apply";
    let marker_dir = doctor_repair_failure_marker_dir(data_dir, repair_class);
    std::fs::create_dir_all(&marker_dir).expect("create marker dir");
    let marker_path = marker_dir.join("1733001111000-corrupt.json");
    std::fs::write(&marker_path, b"{not-json").expect("write corrupt marker");

    let report = collect_doctor_repair_failure_marker(data_dir, repair_class);

    assert!(report.found);
    assert_eq!(report.parse_status, "corrupt");
    assert!(
        doctor_repair_failure_marker_refusal_reason(&report)
            .expect("refusal reason")
            .contains("--allow-repeated-repair"),
        "corrupt markers should still block repeated mutation"
    );
}

#[test]
fn doctor_failure_markers_are_scoped_by_repair_class() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path();
    let repair_marker = test_failure_marker("repair_apply", "op-repair", 1_733_001_111_000);
    let restore_marker = test_failure_marker("restore_apply", "op-restore", 1_733_001_112_000);
    write_doctor_repair_failure_marker(data_dir, &repair_marker).expect("write repair marker");
    write_doctor_repair_failure_marker(data_dir, &restore_marker).expect("write restore marker");

    let repair = collect_doctor_repair_failure_marker(data_dir, "repair_apply");
    let restore = collect_doctor_repair_failure_marker(data_dir, "restore_apply");
    let cleanup = collect_doctor_repair_failure_marker(data_dir, "cleanup_apply");

    assert_eq!(repair.operation_id.as_deref(), Some("op-repair"));
    assert_eq!(restore.operation_id.as_deref(), Some("op-restore"));
    assert!(
        !cleanup.found,
        "unrelated classes must not block each other"
    );
}

#[test]
fn doctor_repeat_failure_marker_check_maps_to_repair_refused() {
    let post_repair_probes = DoctorPostRepairProbeSuiteReport::default();
    let marker_check = doctor_check_report(
        "repair_failure_marker",
        "fail",
        "previous repair failure marker at [cass-data]/doctor/failure-markers/repair_apply/x.json blocks repeated mutation",
        true,
        false,
    );

    let outcome = doctor_top_level_operation_outcome(
        &[marker_check],
        true,
        1,
        0,
        false,
        &post_repair_probes,
        None,
    );

    assert_eq!(outcome.kind, DoctorOperationOutcomeKind::RepairRefused);
    assert_eq!(outcome.exit_code_kind, DoctorExitCodeKind::UsageError);
    assert!(outcome.requires_override);
}

/// cass#368: the crashed-owner shape (dead-owner lock metadata + old
/// marker) must self-heal with a receipt so `--fix` is not blocked
/// forever.
#[test]
fn doctor_self_heal_reclaims_crashed_owner_lock_and_marker() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path();
    let now_ms = doctor_now_ms();
    let nine_days_ago = now_ms - 9 * 24 * 60 * 60 * 1000;
    let lock_path = write_stale_doctor_lock_metadata(data_dir, nine_days_ago);
    let marker_path = write_aged_failure_marker(data_dir, "repair_apply", nine_days_ago);

    let heal = doctor_self_heal_stale_repair_state(data_dir, "repair_apply")
        .expect("crashed-owner shape should self-heal");
    assert_eq!(heal.reclaimed_marker_files, 1);
    assert!(heal.lock_metadata_cleared);
    assert!(heal.receipt_path.exists(), "receipt must exist");
    assert!(
        !marker_path.exists(),
        "marker must be moved (not left) out of the marker dir"
    );
    let reclaim_dir = heal.receipt_path.parent().expect("reclaim dir");
    assert!(
        reclaim_dir
            .join(marker_path.file_name().expect("marker name"))
            .exists(),
        "marker must be preserved inside the reclaim dir"
    );
    let lock_len = std::fs::metadata(&lock_path).expect("lock metadata").len();
    assert_eq!(
        lock_len, 0,
        "stale lock metadata must be truncated in place"
    );

    let receipt: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&heal.receipt_path).expect("read receipt"))
            .expect("receipt JSON");
    assert_eq!(
        receipt["kind"].as_str(),
        Some("cass_doctor_stale_repair_state_reclaim_v1")
    );
    assert_eq!(
        receipt["lock"]["metadata"]["pid"].as_str(),
        Some("999999"),
        "receipt must preserve the reclaimed lock metadata verbatim"
    );

    let marker = collect_doctor_repair_failure_marker(data_dir, "repair_apply");
    assert!(
        !marker.found,
        "failure marker must no longer gate --fix after the reclaim"
    );
}

/// A marker without crashed-owner lock evidence is a deliberate
/// fail-closed record of a completed-but-failed repair; the self-heal
/// must leave it alone (the doctor_fix_refuses_repeated_repair contract).
#[test]
fn doctor_self_heal_leaves_marker_only_state_alone() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path();
    let now_ms = doctor_now_ms();
    let marker_path =
        write_aged_failure_marker(data_dir, "repair_apply", now_ms - 9 * 24 * 60 * 60 * 1000);

    assert!(
        doctor_self_heal_stale_repair_state(data_dir, "repair_apply").is_none(),
        "marker-only state must stay fail-closed"
    );
    assert!(marker_path.exists(), "marker must be preserved");
}

/// A recent crash (heartbeat inside the bound) must not self-heal yet.
#[test]
fn doctor_self_heal_respects_the_generous_bound() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path();
    let now_ms = doctor_now_ms();
    let one_hour_ago = now_ms - 60 * 60 * 1000;
    write_stale_doctor_lock_metadata(data_dir, one_hour_ago);
    let marker_path = write_aged_failure_marker(data_dir, "repair_apply", one_hour_ago);

    assert!(
        doctor_self_heal_stale_repair_state(data_dir, "repair_apply").is_none(),
        "a fresh crash must keep the existing fail-closed behavior"
    );
    assert!(marker_path.exists(), "marker must be preserved");
}

/// A live `--fix` owner holding the flock must never be healed under.
#[test]
fn doctor_self_heal_never_touches_a_live_owner() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path();
    let now_ms = doctor_now_ms();
    let nine_days_ago = now_ms - 9 * 24 * 60 * 60 * 1000;
    let lock_path = write_stale_doctor_lock_metadata(data_dir, nine_days_ago);
    let marker_path = write_aged_failure_marker(data_dir, "repair_apply", nine_days_ago);

    let holder = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&lock_path)
        .expect("open lock as holder");
    fs2::FileExt::try_lock_exclusive(&holder).expect("hold doctor lock");

    assert!(
        doctor_self_heal_stale_repair_state(data_dir, "repair_apply").is_none(),
        "self-heal must back off while the flock is held"
    );
    assert!(marker_path.exists(), "marker must be preserved");
    let lock_len = std::fs::metadata(&lock_path).expect("lock metadata").len();
    assert!(lock_len > 0, "held lock metadata must not be truncated");
}

#[test]
fn doctor_operation_state_blocks_mutation_for_active_index_lock() {
    use std::io::Write as _;

    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path();
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    let lock_path = data_dir.join("index-run.lock");
    let mut lock_file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .expect("open lock file");
    fs2::FileExt::try_lock_exclusive(&lock_file).expect("hold index lock");
    let metadata = format!(
        "pid={}\nstarted_at_ms={}\nupdated_at_ms={}\ndb_path={}\nmode=index\njob_id=lexical-refresh-test\njob_kind=lexical_refresh\nphase=rebuilding\n",
        std::process::id(),
        1_733_001_111_000_i64,
        1_733_001_112_000_i64,
        db_path.display()
    );
    lock_file
        .write_all(metadata.as_bytes())
        .expect("write lock metadata");
    lock_file.flush().expect("flush lock metadata");
    crate::search::asset_state::write_index_run_lock_metadata_sidecar(&lock_path, &metadata)
        .expect("write lock metadata sidecar");

    let snapshot = probe_index_run_lock(data_dir, &db_path, true);
    let doctor_lock = DoctorMutationLockObservation::Absent {
        path: doctor_mutation_lock_path(data_dir),
    };
    let report = build_doctor_operation_state_report(
        data_dir,
        &db_path,
        &index_path,
        &snapshot,
        &doctor_lock,
    );

    assert!(report.read_only_check_allowed);
    assert!(!report.mutating_doctor_allowed);
    assert!(report.active_index_maintenance);
    assert!(report.active_rebuild);
    assert_eq!(report.owner_count, 1);
    let owner = report.owners.first().expect("index owner");
    assert_eq!(owner.lock_kind, DoctorOperationLockKind::IndexRun);
    assert_eq!(owner.mode.as_deref(), Some("index"));
    assert_eq!(
        owner.owner_confidence,
        DoctorOperationOwnerConfidence::CurrentProcess
    );
    assert_eq!(owner.db_path_matches_requested, Some(true));
    assert!(
        report
            .mutation_blocked_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("index/watch")),
        "operation state should explain the lock blocker: {report:#?}"
    );
}

#[test]
fn doctor_operation_state_classifies_interrupted_artifacts_without_gc() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path();
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    let interrupted_plan = data_dir
        .join("doctor")
        .join("tmp")
        .join("interrupted-repair")
        .join("plan.json");
    std::fs::create_dir_all(interrupted_plan.parent().expect("parent"))
        .expect("create interrupted dir");
    std::fs::write(&interrupted_plan, br#"{"state":"interrupted"}"#)
        .expect("write interrupted plan");
    let sidecar =
        doctor_lexical_publish_in_progress_backup_path(&index_path).expect("publish sidecar path");
    std::fs::create_dir_all(sidecar.parent().expect("sidecar parent"))
        .expect("create sidecar parent");
    std::fs::write(&sidecar, b"parked prior live index").expect("write parked sidecar");

    let snapshot = crate::search::asset_state::SearchMaintenanceSnapshot::default();
    let doctor_lock = DoctorMutationLockObservation::Absent {
        path: doctor_mutation_lock_path(data_dir),
    };
    let report = build_doctor_operation_state_report(
        data_dir,
        &db_path,
        &index_path,
        &snapshot,
        &doctor_lock,
    );

    assert!(!report.mutating_doctor_allowed);
    assert!(report.interrupted_state_count >= 2);
    assert!(
        interrupted_plan.exists(),
        "inspection must not delete plans"
    );
    assert!(
        sidecar.exists(),
        "inspection must not delete parked backups"
    );
    assert!(
        report.interrupted_states.iter().any(|state| {
            state.kind == DoctorInterruptedOperationKind::CandidateBuild
                && !state.safe_to_delete_automatically
                && state.blocks_mutation
        }),
        "candidate interrupted repair should block mutation: {report:#?}"
    );
    assert!(
        report.interrupted_states.iter().any(|state| {
            state.kind == DoctorInterruptedOperationKind::ParkedPublishBackup
                && state.disposition == DoctorInterruptedOperationDisposition::RecoverOnNextIndexRun
                && !state.safe_to_delete_automatically
        }),
        "publish sidecar should be classified as recoverable, not deleted: {report:#?}"
    );
}

/// GH #497: an empty interrupted raw-mirror capture dir blocked every
/// mutating repair while doctor only said "inspect", although
/// `--cleanup-interrupted-artifacts --yes` is the supported way to clear
/// it. Doctor must name that command, and the command must actually
/// clear the blocker. The data dir sits under a directory named `backup`
/// so a word-based path classifier cannot be what decides.
#[test]
fn gh497_interrupted_capture_blocker_names_the_cleanup_command_that_clears_it() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("backup").join("cass");
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    let capture = doctor_raw_mirror_capture_staging_root(&data_dir).join("capture.4242.1");
    std::fs::create_dir_all(&capture).expect("create empty interrupted capture dir");
    let snapshot = crate::search::asset_state::SearchMaintenanceSnapshot::default();
    let doctor_lock = DoctorMutationLockObservation::Absent {
        path: doctor_mutation_lock_path(&data_dir),
    };
    let build = || {
        build_doctor_operation_state_report(
            &data_dir,
            &db_path,
            &index_path,
            &snapshot,
            &doctor_lock,
        )
    };

    let blocked = build();
    assert!(!blocked.mutating_doctor_allowed, "{blocked:#?}");
    assert_eq!(
        blocked.next_action, DOCTOR_CLEANUP_INTERRUPTED_ARTIFACTS_NEXT_ACTION,
        "{blocked:#?}"
    );
    assert!(
        blocked
            .next_action
            .contains("cass doctor --cleanup-interrupted-artifacts --yes")
    );
    assert!(
        blocked.interrupted_states.iter().all(|state| state
            .next_action
            .contains("--cleanup-interrupted-artifacts --yes")),
        "{blocked:#?}"
    );

    // A blocker the cleanup command does not cover keeps the inspection
    // instruction next to the cleanup command.
    let foreign = data_dir
        .join("doctor")
        .join("tmp")
        .join("interrupted-repair");
    std::fs::create_dir_all(&foreign).expect("create interrupted doctor tmp dir");
    let mixed = build();
    assert!(
        mixed
            .next_action
            .contains("cass doctor --cleanup-interrupted-artifacts --yes")
            && mixed
                .next_action
                .contains("inspect the remaining operation_state.interrupted_states"),
        "{mixed:#?}"
    );
    assert!(
        mixed
            .interrupted_states
            .iter()
            .filter(|state| !doctor_interrupted_artifact_is_quarantinable(
                &data_dir,
                Path::new(&state.path)
            ))
            .all(|state| !state
                .next_action
                .contains("--cleanup-interrupted-artifacts")),
        "{mixed:#?}"
    );
    std::fs::rename(&foreign, temp.path().join("moved-aside"))
        .expect("move the foreign blocker out of the data dir");

    crate::doctor_recover::run_doctor_cleanup_interrupted_artifacts(
        Some(data_dir.clone()),
        true,
        Some(RobotFormat::Json),
    )
    .expect("the named cleanup command succeeds");
    assert!(!capture.exists(), "the capture dir was quarantined");
    let cleared = build();
    assert!(cleared.mutating_doctor_allowed, "{cleared:#?}");
    assert_eq!(cleared.mutation_blocked_reason, None, "{cleared:#?}");
}

#[test]
fn doctor_lock_diagnostics_report_active_owner_without_delete_advice() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path();
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    let lock_path = doctor_mutation_lock_path(data_dir);
    let mut metadata = BTreeMap::new();
    metadata.insert("schema_version".to_string(), "1".to_string());
    metadata.insert("pid".to_string(), "999999".to_string());
    metadata.insert("started_at_ms".to_string(), "1733001111000".to_string());
    metadata.insert("updated_at_ms".to_string(), "1733001112000".to_string());
    metadata.insert("db_path".to_string(), db_path.display().to_string());
    metadata.insert(
        "command".to_string(),
        format!("cass doctor --fix --data-dir {}", data_dir.display()),
    );
    let report = build_doctor_operation_state_report(
        data_dir,
        &db_path,
        &index_path,
        &crate::search::asset_state::SearchMaintenanceSnapshot::default(),
        &DoctorMutationLockObservation::Active {
            path: lock_path,
            metadata,
        },
    );

    let locks = build_doctor_lock_diagnostics(&report, 1_733_001_113_000);

    assert_eq!(locks.len(), 1);
    let lock = &locks[0];
    assert_eq!(lock.lock_kind, DoctorOperationLockKind::DoctorRepair);
    assert_eq!(
        lock.owner_confidence,
        DoctorOperationOwnerConfidence::ActiveAdvisoryLock
    );
    assert_eq!(lock.retry_policy, "wait-and-retry");
    assert_eq!(lock.wait_duration_ms, Some(30_000));
    assert!(lock.safe_to_wait);
    assert!(!lock.manual_delete_allowed);
    assert!(lock.pid_reuse_ambiguous);
    assert_eq!(
        lock.owner_command.as_deref(),
        Some("cass doctor --fix --data-dir [cass-data]")
    );
}

#[test]
fn doctor_lock_diagnostics_keep_stale_metadata_advisory_only() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path();
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    let mut metadata = BTreeMap::new();
    metadata.insert("schema_version".to_string(), "1".to_string());
    metadata.insert("pid".to_string(), "999999".to_string());
    metadata.insert("started_at_ms".to_string(), "1733000000000".to_string());
    metadata.insert("updated_at_ms".to_string(), "1733000000000".to_string());
    let report = build_doctor_operation_state_report(
        data_dir,
        &db_path,
        &index_path,
        &crate::search::asset_state::SearchMaintenanceSnapshot::default(),
        &DoctorMutationLockObservation::Available {
            path: doctor_mutation_lock_path(data_dir),
            metadata,
        },
    );

    let locks = build_doctor_lock_diagnostics(&report, 1_733_004_000_001);

    assert_eq!(locks.len(), 1);
    assert_eq!(locks[0].retry_policy, "inspect-receipts");
    assert_eq!(locks[0].stale_suspected, Some(true));
    assert!(!locks[0].manual_delete_allowed);
    assert!(
        locks[0].recommended_action.contains("do not delete it")
            && locks[0]
                .recommended_action
                .contains("reclaims it with a receipt"),
        "stale metadata must not become unsafe delete advice, and must name \
         the --fix self-heal path (cass#368): {locks:#?}"
    );
}

#[test]
fn doctor_lock_diagnostics_handle_missing_pid_and_permission_denied() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path();
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");

    let missing_pid_state = build_doctor_operation_state_report(
        data_dir,
        &db_path,
        &index_path,
        &crate::search::asset_state::SearchMaintenanceSnapshot::default(),
        &DoctorMutationLockObservation::Active {
            path: doctor_mutation_lock_path(data_dir),
            metadata: BTreeMap::new(),
        },
    );
    let missing_pid_locks = build_doctor_lock_diagnostics(&missing_pid_state, 1_733_004_000_001);
    assert_eq!(missing_pid_locks[0].pid_metadata_status, "pid-missing");
    assert_eq!(missing_pid_locks[0].stale_suspected, None);

    let permission_state = build_doctor_operation_state_report(
        data_dir,
        &db_path,
        &index_path,
        &crate::search::asset_state::SearchMaintenanceSnapshot::default(),
        &DoctorMutationLockObservation::Unavailable {
            path: doctor_mutation_lock_path(data_dir),
            reason: "failed to open doctor mutation lock for inspection: Permission denied"
                .to_string(),
        },
    );
    let permission_locks = build_doctor_lock_diagnostics(&permission_state, 1_733_004_000_001);
    assert_eq!(permission_locks[0].pid_metadata_status, "permission-denied");
    assert_eq!(permission_locks[0].retry_policy, "inspect-permissions");
}

#[test]
fn doctor_lock_diagnostics_no_lock_reports_retry_allowed_summary() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path();
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    let state = doctor_test_operation_state(data_dir, &db_path, &index_path);
    let locks = build_doctor_lock_diagnostics(&state, 1_733_004_000_001);
    let outcome = doctor_operation_outcome_with_details(
        DoctorOperationOutcomeKind::OkNoActionNeeded,
        "doctor found no lock blockers".to_string(),
        "reported lock state".to_string(),
        "no mutation was attempted".to_string(),
        DoctorDataLossRisk::None,
        Some("cass doctor --json".to_string()),
        None,
    );

    let retry = build_doctor_retry_recommendation(&state, &outcome, &locks, None);

    assert!(locks.is_empty());
    assert_eq!(retry.policy, "retry-allowed");
    assert_eq!(retry.command.as_deref(), Some("cass doctor --json"));
}

#[test]
fn doctor_slow_operation_reports_sort_and_correlate_slow_phases() {
    let spans = vec![
        DoctorTimingSpanReport {
            schema_version: 1,
            name: "raw_mirror_scan".to_string(),
            phase: "raw_mirror".to_string(),
            source: "unit".to_string(),
            elapsed_ms: 900,
            threshold_ms: 500,
            slow: true,
            event_log_correlation_id: "doctor_timing:raw_mirror_scan".to_string(),
            notes: vec![],
        },
        DoctorTimingSpanReport {
            schema_version: 1,
            name: "lexical_probe".to_string(),
            phase: "index".to_string(),
            source: "unit".to_string(),
            elapsed_ms: 30,
            threshold_ms: 500,
            slow: false,
            event_log_correlation_id: "doctor_timing:lexical_probe".to_string(),
            notes: vec![],
        },
        DoctorTimingSpanReport {
            schema_version: 1,
            name: "candidate_staging".to_string(),
            phase: "candidate_staging".to_string(),
            source: "unit".to_string(),
            elapsed_ms: 3_000,
            threshold_ms: 500,
            slow: true,
            event_log_correlation_id: "doctor_timing:candidate_staging".to_string(),
            notes: vec![],
        },
    ];

    let slow = build_doctor_slow_operations(&spans);
    let summary = build_doctor_timing_summary(&spans, 3_200);

    assert_eq!(slow.len(), 2);
    assert_eq!(slow[0].name, "candidate_staging");
    assert_eq!(
        slow[0].event_log_correlation_id,
        "doctor_timing:candidate_staging"
    );
    assert_eq!(summary.measured_operation_count, 3);
    assert_eq!(summary.slow_operation_count, 2);
    assert_eq!(
        summary.slowest_operation.as_deref(),
        Some("candidate_staging")
    );
}

#[test]
fn cleanup_apply_operation_outcome_distinguishes_fixed_partial_refused_and_noop() {
    let applied = DiagCleanupApplyResult {
        outcome_kind: DoctorRepairOutcomeKind::Applied,
        pruned_asset_count: 2,
        reclaimed_bytes: 100,
        ..DiagCleanupApplyResult::default()
    };
    assert_eq!(
        cleanup_apply_operation_outcome(&applied).kind,
        DoctorOperationOutcomeKind::Fixed
    );

    let partial = DiagCleanupApplyResult {
        outcome_kind: DoctorRepairOutcomeKind::Partial,
        pruned_asset_count: 1,
        skipped_asset_count: 1,
        blocked_reasons: vec!["one generation still requires inspection".to_string()],
        ..DiagCleanupApplyResult::default()
    };
    assert_eq!(
        cleanup_apply_operation_outcome(&partial).kind,
        DoctorOperationOutcomeKind::PartiallyFixed
    );

    let refused = DiagCleanupApplyResult {
        outcome_kind: DoctorRepairOutcomeKind::Blocked,
        blocked_reasons: vec!["cleanup target escaped approved root".to_string()],
        ..DiagCleanupApplyResult::default()
    };
    let refused_report = cleanup_apply_operation_outcome(&refused);
    assert_eq!(
        refused_report.kind,
        DoctorOperationOutcomeKind::CleanupRefused
    );
    assert_eq!(
        refused_report.exit_code_kind,
        DoctorExitCodeKind::RepairFailure
    );

    let noop = DiagCleanupApplyResult {
        outcome_kind: DoctorRepairOutcomeKind::NoOp,
        ..DiagCleanupApplyResult::default()
    };
    assert_eq!(
        cleanup_apply_operation_outcome(&noop).kind,
        DoctorOperationOutcomeKind::OkNoActionNeeded
    );
}

#[test]
fn doctor_repair_mode_policy_covers_every_mode_and_asset_class() {
    let policy_modes: HashSet<_> = DOCTOR_REPAIR_MODE_POLICY_TABLE
        .iter()
        .map(|policy| policy.mode)
        .collect();
    let expected_modes: HashSet<_> = DOCTOR_REPAIR_ALL_MODES.iter().copied().collect();
    assert_eq!(
        policy_modes, expected_modes,
        "every advertised doctor repair mode needs a fail-closed policy row"
    );

    for &mode in DOCTOR_REPAIR_ALL_MODES {
        let policy = doctor_repair_mode_policy(mode);
        for &asset_class in ALL_DOCTOR_ASSET_CLASSES {
            let allowed = doctor_repair_mode_allows_asset_mutation(mode, asset_class);
            if allowed {
                assert!(
                    policy.mutates,
                    "{mode:?} cannot allow {asset_class:?} unless the mode is mutating"
                );
                assert!(
                    doctor_asset_safe_to_gc(asset_class, true),
                    "{mode:?} cannot mutate {asset_class:?} unless the asset taxonomy allows safe reclaim"
                );
            }
        }
    }
}

#[test]
fn doctor_repair_mode_policy_is_operation_aware_for_non_prune_mutations() {
    assert!(
        doctor_repair_mode_allows_asset_operation_mutation(
            DoctorRepairMode::RepairApply,
            DoctorAssetClass::DerivedLexicalIndex,
            DoctorAssetOperation::Rebuild,
        ),
        "repair_apply should be able to rebuild derived lexical indexes even though they are not safe_to_gc prune targets"
    );
    assert!(
        !doctor_repair_mode_allows_asset_operation_mutation(
            DoctorRepairMode::RepairApply,
            DoctorAssetClass::DerivedLexicalIndex,
            DoctorAssetOperation::PruneReclaim,
        ),
        "repair_apply must not become a hidden prune path for live derived lexical indexes"
    );
    assert!(
        doctor_repair_mode_allows_asset_operation_mutation(
            DoctorRepairMode::RepairApply,
            DoctorAssetClass::DerivedLexicalIndex,
            DoctorAssetOperation::MoveQuarantine,
        ),
        "repair_apply should be able to move verified bad derived index files into quarantine without deleting them"
    );

    assert!(
        doctor_repair_mode_allows_asset_operation_mutation(
            DoctorRepairMode::ReconstructPromote,
            DoctorAssetClass::RawMirrorBlob,
            DoctorAssetOperation::Copy,
        ),
        "reconstruct_promote should be able to copy verified raw mirror evidence into isolated candidates"
    );
    assert!(
        !doctor_repair_mode_allows_asset_operation_mutation(
            DoctorRepairMode::ReconstructPromote,
            DoctorAssetClass::RawMirrorBlob,
            DoctorAssetOperation::PruneReclaim,
        ),
        "copy-authorized precious evidence must still be unpruneable"
    );

    assert!(
        doctor_repair_mode_allows_asset_operation_mutation(
            DoctorRepairMode::ReconstructPromote,
            DoctorAssetClass::CanonicalArchiveDb,
            DoctorAssetOperation::Promote,
        ),
        "reconstruct_promote should be able to promote a verified staged candidate into a missing canonical archive target"
    );
    assert!(
        !doctor_repair_mode_allows_asset_operation_mutation(
            DoctorRepairMode::ReconstructPromote,
            DoctorAssetClass::RawMirrorBlob,
            DoctorAssetOperation::Promote,
        ),
        "raw mirror evidence may be copied into staging but must not be promoted as the live archive target"
    );

    assert!(
        doctor_repair_mode_allows_asset_operation_mutation(
            DoctorRepairMode::RestoreApply,
            DoctorAssetClass::CanonicalArchiveDb,
            DoctorAssetOperation::Restore,
        ),
        "restore_apply should be able to restore a verified staged archive into a missing canonical archive target"
    );
    assert!(
        !doctor_repair_mode_allows_asset_operation_mutation(
            DoctorRepairMode::RestoreApply,
            DoctorAssetClass::CanonicalArchiveDb,
            DoctorAssetOperation::PruneReclaim,
        ),
        "canonical archives may be restore targets but must never become prune targets"
    );
    assert!(
        doctor_repair_mode_allows_asset_operation_mutation(
            DoctorRepairMode::RestoreApply,
            DoctorAssetClass::BackupBundle,
            DoctorAssetOperation::Restore,
        ),
        "restore_apply should authorize restore operations from verified backup bundles"
    );
    assert!(
        !doctor_repair_mode_allows_asset_operation_mutation(
            DoctorRepairMode::RestoreApply,
            DoctorAssetClass::BackupBundle,
            DoctorAssetOperation::PruneReclaim,
        ),
        "backup bundles are restore authorities, not cleanup targets"
    );

    assert!(
        doctor_repair_mode_allows_asset_mutation(
            DoctorRepairMode::CleanupApply,
            DoctorAssetClass::RetainedPublishBackup,
        ),
        "legacy prune helper should still mean cleanup_apply + prune_reclaim"
    );
}

#[test]
fn default_and_dry_run_modes_are_read_only() {
    for mode in [
        DoctorRepairMode::Check,
        DoctorRepairMode::RepairDryRun,
        DoctorRepairMode::CleanupDryRun,
        DoctorRepairMode::ReconstructDryRun,
        DoctorRepairMode::RestoreRehearsal,
        DoctorRepairMode::SupportBundle,
        DoctorRepairMode::BaselineDiff,
    ] {
        let policy = doctor_repair_mode_policy(mode);
        assert!(!policy.mutates, "{mode:?} must be read-only");
        assert_eq!(policy.approval_requirement, DoctorApprovalRequirement::None);
        for &asset_class in ALL_DOCTOR_ASSET_CLASSES {
            assert!(
                !doctor_repair_mode_allows_asset_mutation(mode, asset_class),
                "{mode:?} unexpectedly allows mutation of {asset_class:?}"
            );
        }
    }
}

#[test]
fn mutating_modes_require_approval_or_fail_closed_refusal() {
    for mode in [
        DoctorRepairMode::RepairApply,
        DoctorRepairMode::CleanupApply,
        DoctorRepairMode::ReconstructPromote,
        DoctorRepairMode::RestoreApply,
        DoctorRepairMode::SafeAutoRun,
        DoctorRepairMode::EmergencyForce,
    ] {
        let policy = doctor_repair_mode_policy(mode);
        assert_ne!(
            policy.approval_requirement,
            DoctorApprovalRequirement::None,
            "{mode:?} must not mutate or override without an explicit gate"
        );
    }

    let emergency = doctor_repair_mode_policy(DoctorRepairMode::EmergencyForce);
    assert!(!emergency.mutates);
    assert!(!emergency.robot_allowed);
    assert_eq!(
        emergency.approval_requirement,
        DoctorApprovalRequirement::Refused
    );
    assert!(emergency.allowed_mutation_asset_classes.is_empty());
}

#[test]
fn cleanup_apply_mode_only_mutates_derived_reclaimable_assets() {
    assert!(doctor_repair_mode_allows_asset_mutation(
        DoctorRepairMode::CleanupApply,
        DoctorAssetClass::RetainedPublishBackup
    ));
    assert!(doctor_repair_mode_allows_asset_mutation(
        DoctorRepairMode::CleanupApply,
        DoctorAssetClass::ReclaimableDerivedCache
    ));

    for asset_class in [
        DoctorAssetClass::SourceSessionLog,
        DoctorAssetClass::RawMirrorBlob,
        DoctorAssetClass::CanonicalArchiveDb,
        DoctorAssetClass::ArchiveDbSidecar,
        DoctorAssetClass::BookmarkStore,
        DoctorAssetClass::UserConfig,
        DoctorAssetClass::OperationReceipt,
        DoctorAssetClass::ForensicBundle,
        DoctorAssetClass::SupportBundle,
        DoctorAssetClass::FailedSeedBundle,
        DoctorAssetClass::QuarantinedLexicalGeneration,
        DoctorAssetClass::QuarantinedLexicalShard,
    ] {
        assert!(
            !doctor_repair_mode_allows_asset_mutation(DoctorRepairMode::CleanupApply, asset_class),
            "cleanup_apply must not mutate precious or quarantined {asset_class:?}"
        );
    }
}

#[test]
fn cleanup_apply_outcome_and_receipt_are_structured() {
    let data_dir = Path::new("/tmp/cass");
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    let mut result = DiagCleanupApplyResult {
        mode: DoctorRepairMode::CleanupApply,
        approval_requirement: DoctorApprovalRequirement::ApprovalFingerprint,
        requested: true,
        operation_started_at_ms: Some(1_700_000_000_000),
        operation_finished_at_ms: Some(1_700_000_000_125),
        approval_fingerprint: "cleanup-v1-test".to_string(),
        pruned_asset_count: 1,
        reclaimed_bytes: 42,
        actions: vec![DiagCleanupApplyAction {
            asset_safety: doctor_asset_safety(DoctorAssetClass::RetainedPublishBackup),
            artifact_kind: "retained_publish_backup".to_string(),
            path: "/tmp/cass/index/.lexical-publish-backups/old".to_string(),
            generation_id: None,
            shard_id: None,
            disposition: None,
            reason: "outside retention cap".to_string(),
            planned_reclaimable_bytes: 42,
            reclaimed_bytes: 42,
            applied: true,
            skipped: false,
            skip_reason: None,
            mutation_receipt: None,
        }],
        ..DiagCleanupApplyResult::default()
    };

    finalize_cleanup_apply_contract(&mut result, data_dir, &db_path, &index_path);

    assert_eq!(result.outcome_kind, DoctorRepairOutcomeKind::Applied);
    assert_eq!(result.retry_safety, DoctorRepairRetrySafety::SafeToRetry);
    assert_eq!(result.planned_actions.len(), 1);
    assert_eq!(
        result
            .plan
            .as_ref()
            .expect("cleanup apply plan")
            .approval_fingerprint,
        "cleanup-v1-test"
    );
    assert_eq!(result.receipt.receipt_kind, "doctor_cleanup_apply_v1");
    assert_eq!(result.receipt.mode, DoctorRepairMode::CleanupApply);
    assert_eq!(
        result.receipt.outcome_kind,
        DoctorRepairOutcomeKind::Applied
    );
    assert_eq!(result.receipt.approval_fingerprint, "cleanup-v1-test");
    assert_eq!(result.receipt.started_at_ms, Some(1_700_000_000_000));
    assert_eq!(result.receipt.finished_at_ms, Some(1_700_000_000_125));
    assert_eq!(result.receipt.duration_ms, Some(125));
    assert_eq!(result.receipt.planned_action_count, 1);
    assert_eq!(result.receipt.applied_action_count, 1);
    assert_eq!(result.receipt.reclaimed_bytes, 42);
    assert_eq!(result.receipt.bytes_planned, 42);
    assert_eq!(result.receipt.bytes_pruned, 42);
    assert_eq!(
        result.receipt.plan_fingerprint,
        result.plan.as_ref().unwrap().plan_fingerprint
    );

    let mut partially_blocked = DiagCleanupApplyResult {
        mode: DoctorRepairMode::CleanupApply,
        approval_requirement: DoctorApprovalRequirement::ApprovalFingerprint,
        requested: true,
        operation_started_at_ms: Some(1_700_000_001_000),
        operation_finished_at_ms: Some(1_700_000_001_010),
        approval_fingerprint: "cleanup-v1-partial".to_string(),
        pruned_asset_count: 1,
        reclaimed_bytes: 42,
        blocked_reasons: vec!["lexical cleanup gate blocked".to_string()],
        actions: result.actions.clone(),
        ..DiagCleanupApplyResult::default()
    };

    finalize_cleanup_apply_contract(&mut partially_blocked, data_dir, &db_path, &index_path);

    assert_eq!(
        partially_blocked.outcome_kind,
        DoctorRepairOutcomeKind::Partial,
        "cleanup receipts must not report full applied success when any planned work was blocked"
    );
    assert_eq!(
        partially_blocked.retry_safety,
        DoctorRepairRetrySafety::RetryAfterSameDryRun
    );
}
