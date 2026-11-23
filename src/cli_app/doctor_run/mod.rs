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
#[allow(unused_imports)]
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

pub(crate) mod archive_probe;
mod db_and_lexical_probes;
mod recovery_evidence;
mod repair_apply;
mod verdict_and_output;

#[allow(unused_imports)]
pub(crate) use archive_probe::*;

#[derive(serde::Serialize)]
pub(super) struct DoctorCheck {
    pub(super) name: String,
    pub(super) status: String,
    pub(super) message: String,
    pub(super) fix_available: bool,
    pub(super) fix_applied: bool,
}

pub(super) struct DoctorRunState {
    pub(super) output_format: Option<RobotFormat>,
    pub(super) fix: bool,
    pub(super) verbose: bool,
    pub(super) force_rebuild: bool,
    pub(super) allow_repeated_repair: bool,
    pub(super) command_surface: doctor::DoctorCommandSurface,
    pub(super) execution_mode: doctor::DoctorExecutionMode,
    pub(super) requested_plan_fingerprint: Option<String>,
    pub(super) wrap: WrapConfig,
    pub(super) start: Instant,
    pub(super) data_dir: PathBuf,
    pub(super) db_path: PathBuf,
    pub(super) index_path: PathBuf,
    pub(super) timing_spans: Vec<DoctorTimingSpanReport>,
    pub(super) maintenance_snapshot: crate::search::asset_state::SearchMaintenanceSnapshot,
    pub(super) rebuild_active: bool,
    pub(super) cleanup_apply_requested: bool,
    pub(super) safe_auto_run_requested: bool,
    pub(super) repair_class: String,
    pub(super) initial_failure_marker: DoctorRepairFailureMarkerReport,
    pub(super) repeat_refusal_reason: Option<String>,
    pub(super) repeat_repair_refused: bool,
    pub(super) override_available: bool,
    pub(super) override_used: bool,
    pub(super) _doctor_lock_guard: Option<DoctorMutationLockGuard>,
    pub(super) _doctor_db_open_bypass_guard:
        Option<crate::storage::sqlite::DoctorMutationDbOpenBypassGuard>,
    pub(super) operation_state: DoctorOperationStateReport,
    pub(super) mutating_lock_acquired: bool,
    pub(super) fix_can_mutate: bool,
    pub(super) not_initialized: bool,
    pub(super) explanation: Option<String>,
    pub(super) recommended_action: Option<String>,
    pub(super) checks: Vec<DoctorCheck>,
    pub(super) needs_rebuild: bool,
    pub(super) db_ok: bool,
    pub(super) db_conversations: Option<usize>,
    pub(super) db_messages: Option<usize>,
    pub(super) storage_db_open_failed: bool,
    pub(super) storage_integrity_failed: bool,
    pub(super) storage_integrity_unverified: bool,
    pub(super) storage_integrity_skipped_reason: Option<String>,
    pub(super) storage_probe_timed_out: bool,
    pub(super) storage_lexical_index_drifted: bool,
    pub(super) storage_attestation_check_depth: Option<&'static str>,
    pub(super) storage_attestation_detail: Option<String>,
    pub(super) dedicated_storage_probe: crate::search::storage_integrity::DedicatedStorageProbe,
    pub(super) matching_cached_integrity_attestation:
        Option<crate::search::storage_integrity::IntegrityAttestation>,
    pub(super) auto_fix_actions: Vec<String>,
    pub(super) auto_fix_applied: bool,
    pub(super) fs_mutation_receipts: Vec<DoctorFsMutationReceipt>,
    pub(super) candidate_promotion_report: Option<DoctorCandidatePromotionReport>,
    pub(super) storage_pressure: DoctorStoragePressureReport,
}

pub(super) struct DoctorDbWalProbeResult {
    pub(super) archive_db_probe_started: Instant,
    pub(super) archive_queryable_for_non_destructive_derived_rebuild: bool,
    pub(super) canonical_candidate_replacement_authorized: bool,
    pub(super) storage_busy_or_locked: bool,
    pub(super) archive_wide_collectors_deferred_reason: Option<String>,
    pub(super) archive_wide_collectors_complete: bool,
}

pub(super) struct DoctorLexicalWorkspaceReport {
    pub(super) archive_queryable_for_non_destructive_derived_rebuild: bool,
    pub(super) canonical_candidate_replacement_authorized: bool,
    pub(super) storage_busy_or_locked: bool,
    pub(super) archive_wide_collectors_deferred_reason: Option<String>,
    pub(super) archive_wide_collectors_complete: bool,
    pub(super) storage_integrity_report: crate::search::storage_integrity::StorageIntegrityReport,
    pub(super) config_exclusion_risks: Vec<DoctorConfigExclusionRiskReport>,
    pub(super) source_inventory: DoctorSourceInventoryReport,
}

pub(super) struct DoctorRecoveryEvidence {
    pub(super) raw_mirror: DoctorRawMirrorReport,
    pub(super) raw_mirror_backfill: DoctorRawMirrorBackfillReport,
    pub(super) sole_copy_warnings: Vec<DoctorSoleCopyWarning>,
    pub(super) coverage_summary: DoctorCoverageSummary,
    pub(super) coverage_risk: DoctorCoverageRiskSummary,
    pub(super) source_authority: DoctorSourceAuthorityReport,
    pub(super) candidate_repair_authority_complete: bool,
    pub(super) isolated_candidate_built: bool,
    pub(super) candidate_staging: DoctorCandidateStagingReport,
    pub(super) readiness_snapshot: serde_json::Value,
    pub(super) fallback_mode: String,
    pub(super) derived_semantic_assets: DoctorDerivedSemanticAssetReport,
}

pub(super) struct DoctorRepairApplyResult {
    pub(super) repair_plan: Option<DoctorRepairPlanPreviewReport>,
    pub(super) archive_queryable_for_non_destructive_derived_rebuild: bool,
    pub(super) cleanup_apply_result: Option<DiagCleanupApplyResult>,
}

/// Internal doctor executor reached through the typed `doctor` module.
/// CRITICAL: This function NEVER deletes user data. It only rebuilds derived data (index, db)
/// from source session files. This is essential because users may have only one copy of their
/// agent session data, and Codex/Claude Code auto-expire older logs.
#[allow(
    clippy::collapsible_if,
    clippy::collapsible_else_if,
    clippy::too_many_arguments
)]
pub(crate) fn run_doctor_impl(
    data_dir_override: &Option<PathBuf>,
    db_override: Option<PathBuf>,
    output_format: Option<RobotFormat>,
    fix: bool,
    verbose: bool,
    force_rebuild: bool,
    allow_repeated_repair: bool,
    command_surface: doctor::DoctorCommandSurface,
    execution_mode: doctor::DoctorExecutionMode,
    requested_plan_fingerprint: Option<String>,
    wrap: WrapConfig,
) -> CliResult<()> {
    let start = Instant::now();
    // siekg: liveness heartbeat on stderr for the whole run; dropped (and
    // joined) when this function returns, after the report is written.
    let _heartbeat = CliHeartbeat::start(if fix {
        "doctor --fix"
    } else {
        "doctor --check"
    });
    let data_dir = resolve_data_dir(data_dir_override, db_override.as_ref());
    let db_path = db_override.unwrap_or_else(|| data_dir.join("agent_search.db"));
    let index_path = crate::search::tantivy::expected_index_dir(&data_dir);
    let lock_path = data_dir.join(".index.lock");
    let mut timing_spans: Vec<DoctorTimingSpanReport> = Vec::new();
    let lock_probe_started = Instant::now();
    let maintenance_snapshot = probe_index_run_lock(&data_dir, &db_path, true);
    let rebuild_active = maintenance_snapshot.active;
    let cleanup_apply_requested = command_surface == doctor::DoctorCommandSurface::Cleanup
        && execution_mode == doctor::DoctorExecutionMode::CleanupApply;
    let safe_auto_run_requested = command_surface == doctor::DoctorCommandSurface::LegacyDoctor
        && execution_mode == doctor::DoctorExecutionMode::SafeAutoFix;
    let repair_class = doctor_repair_class_for_execution(command_surface, execution_mode);
    // cass#368 self-heal: before the failure marker gates this run, reclaim
    // (with a receipt, never deleting) leftovers of a --fix owner that
    // crashed long ago and left the mutation lock + marker blocking every
    // later repair.
    let stale_self_heal = if fix {
        doctor_self_heal_stale_repair_state(&data_dir, &repair_class)
    } else {
        None
    };
    let initial_failure_marker = collect_doctor_repair_failure_marker(&data_dir, &repair_class);
    let repeat_refusal_reason =
        doctor_repair_failure_marker_refusal_reason(&initial_failure_marker);
    let repeat_repair_refused = fix && initial_failure_marker.found && !allow_repeated_repair;
    let override_available = fix && initial_failure_marker.found;
    let override_used = override_available && allow_repeated_repair;
    let mut _doctor_lock_guard: Option<DoctorMutationLockGuard> = None;
    let mut _doctor_db_open_bypass_guard: Option<
        crate::storage::sqlite::DoctorMutationDbOpenBypassGuard,
    > = None;
    let fingerprint_apply_requested =
        execution_mode == doctor::DoctorExecutionMode::FingerprintApply;
    let doctor_lock_observation = if fix && !repeat_repair_refused && fingerprint_apply_requested {
        if doctor_mutation_lock_path(&data_dir).exists() {
            match doctor_acquire_existing_mutation_lock_without_metadata(&data_dir) {
                Ok((guard, observation)) => {
                    _doctor_lock_guard = Some(guard);
                    _doctor_db_open_bypass_guard =
                        Some(crate::storage::sqlite::enter_doctor_mutation_db_open_bypass());
                    observation
                }
                Err(observation) => observation,
            }
        } else {
            doctor_probe_mutation_lock(&data_dir)
        }
    } else if fix && !repeat_repair_refused {
        match doctor_acquire_mutation_lock(&data_dir, &db_path) {
            Ok((guard, observation)) => {
                _doctor_lock_guard = Some(guard);
                _doctor_db_open_bypass_guard =
                    Some(crate::storage::sqlite::enter_doctor_mutation_db_open_bypass());
                observation
            }
            Err(observation) => observation,
        }
    } else {
        doctor_probe_mutation_lock(&data_dir)
    };
    let operation_state = build_doctor_operation_state_report(
        &data_dir,
        &db_path,
        &index_path,
        &maintenance_snapshot,
        &doctor_lock_observation,
    );
    doctor_push_timing_span(
        &mut timing_spans,
        "lock_state_probe",
        "operation_state",
        "operation_state",
        lock_probe_started,
        DOCTOR_SLOW_OPERATION_DEFAULT_THRESHOLD_MS,
        vec!["index-run and doctor repair lock observations captured".to_string()],
    );
    let mutating_lock_acquired = matches!(
        doctor_lock_observation,
        DoctorMutationLockObservation::Acquired { .. }
    );
    let fix_can_mutate = fix
        && operation_state.mutating_doctor_allowed
        && mutating_lock_acquired
        && !repeat_repair_refused
        && !fingerprint_apply_requested;
    let not_initialized = !fix
        && cass_not_initialized(
            db_path.exists(),
            crate::search::tantivy::searchable_index_exists(&index_path),
            rebuild_active,
        );
    let explanation = not_initialized.then(|| cass_not_initialized_explanation(&data_dir));
    let recommended_action = not_initialized.then(cass_not_initialized_recommended_action);

    let mut checks: Vec<DoctorCheck> = Vec::new();
    if let Some(heal) = &stale_self_heal {
        checks.push(DoctorCheck {
            name: "stale_repair_state_self_heal".to_string(),
            status: "pass".to_string(),
            message: format!(
                "reclaimed stale repair state from a crashed doctor --fix owner ({} failure-marker file(s){}) with receipt {}",
                heal.reclaimed_marker_files,
                if heal.lock_metadata_cleared {
                    " + stale mutation-lock metadata"
                } else {
                    ""
                },
                heal.receipt_path.display()
            ),
            fix_available: true,
            fix_applied: true,
        });
    }
    let needs_rebuild = force_rebuild;
    let db_ok = false;
    let db_conversations: Option<usize> = None;
    let db_messages: Option<usize> = None;
    let storage_db_open_failed = false;
    let storage_integrity_failed = false;
    let storage_integrity_unverified = false;
    let storage_integrity_skipped_reason: Option<String> = None;
    let storage_probe_timed_out = false;
    let storage_lexical_index_drifted = false;
    let storage_attestation_check_depth: Option<&'static str> = None;
    let storage_attestation_detail: Option<String> = None;
    let dedicated_storage_probe = crate::search::storage_integrity::probe_dedicated_storage_state(
        &db_path,
        crate::search::storage_integrity::dedicated_storage_probe_timeout(),
    );
    let matching_cached_integrity_attestation =
        crate::search::storage_integrity::load_matching_integrity_attestation(&data_dir, &db_path);
    let mut auto_fix_actions: Vec<String> = Vec::new();
    let mut auto_fix_applied = false;
    let mut fs_mutation_receipts: Vec<DoctorFsMutationReceipt> = Vec::new();
    let candidate_promotion_report: Option<DoctorCandidatePromotionReport> = None;

    macro_rules! add_check {
        ($name:expr, $status:expr, $message:expr, $fix_available:expr) => {
            checks.push(DoctorCheck {
                name: $name.to_string(),
                status: $status.to_string(),
                message: $message.to_string(),
                fix_available: $fix_available,
                fix_applied: false,
            });
        };
    }

    if let Some(reason) = operation_state.mutation_blocked_reason.as_deref() {
        let status = if fix { "fail" } else { "warn" };
        add_check!(
            "operation_state",
            status,
            format!("Doctor mutation blocked: {reason}"),
            false
        );
    } else {
        add_check!(
            "operation_state",
            "pass",
            "Doctor operation state permits read-only inspection and mutating repair",
            false
        );
    }

    if let Some(reason) = repeat_refusal_reason.as_deref() {
        if repeat_repair_refused {
            add_check!("repair_failure_marker", "fail", reason.to_string(), true);
        } else if !override_used {
            add_check!("repair_failure_marker", "warn", reason.to_string(), true);
        }
    }

    // 1. Check data directory exists and is writable
    if !data_dir.exists() && not_initialized {
        add_check!(
            "data_directory",
            "pass",
            format!(
                "Data directory not created yet: {} (it will be created on the first index run)",
                data_dir.display()
            ),
            false
        );
    } else if data_dir.exists() {
        if std::fs::metadata(&data_dir)
            .map(|m| !m.permissions().readonly())
            .unwrap_or(false)
        {
            add_check!(
                "data_directory",
                "pass",
                format!("Data directory exists: {}", data_dir.display()),
                false
            );
        } else {
            add_check!(
                "data_directory",
                "fail",
                format!("Data directory not writable: {}", data_dir.display()),
                false
            );
        }
    } else if fix_can_mutate {
        if std::fs::create_dir_all(&data_dir).is_ok() {
            checks.push(DoctorCheck {
                name: "data_directory".to_string(),
                status: "pass".to_string(),
                message: format!("Data directory created: {}", data_dir.display()),
                fix_available: true,
                fix_applied: true,
            });
            auto_fix_actions.push("Created missing data directory".to_string());
            auto_fix_applied = true;
        } else {
            add_check!(
                "data_directory",
                "fail",
                format!("Cannot create data directory: {}", data_dir.display()),
                true
            );
        }
    } else {
        add_check!(
            "data_directory",
            "fail",
            format!(
                "Data directory missing: {} (run with --fix to create)",
                data_dir.display()
            ),
            true
        );
    }

    let storage_pressure_started = Instant::now();
    let storage_pressure = collect_doctor_storage_pressure(&data_dir, &db_path);
    doctor_push_timing_span(
        &mut timing_spans,
        "storage_pressure",
        "storage_pressure",
        "storage_pressure",
        storage_pressure_started,
        DOCTOR_SLOW_OPERATION_DEFAULT_THRESHOLD_MS,
        vec!["free-space probe completed without mutating archive evidence".to_string()],
    );
    match storage_pressure.status.as_str() {
        "ok" => {
            add_check!(
                "storage_pressure",
                "pass",
                format!(
                    "Storage pressure OK (available bytes above safety floor at {})",
                    storage_pressure.probe_path
                ),
                false
            );
        }
        "warn" => {
            add_check!(
                "storage_pressure",
                "warn",
                format!(
                    "Low free space for safe doctor work (available bytes below safety floor at {})",
                    storage_pressure.probe_path
                ),
                false
            );
        }
        _ => {
            add_check!(
                "storage_pressure",
                "warn",
                storage_pressure
                    .notes
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "Storage pressure was not checked".to_string()),
                false
            );
        }
    }
    // GH#442: answer the predicate `index --full` refuses on, as its own
    // check line so the human output shows it next to the general
    // storage-pressure verdict. A blocked rebuild is a warning, never a
    // failure: incremental indexing and search keep working without it.
    {
        let readiness = &storage_pressure.full_rebuild_readiness;
        let (status, message) = match readiness.status.as_str() {
            // The pass message carries no live free-space figure (it feeds
            // the event-log hash chain in the robot goldens); the number is
            // in storage_pressure.full_rebuild_readiness.available_bytes.
            "ready" => (
                "pass",
                format!(
                    "Full rebuild ready: {} bytes required at {} and free space covers it",
                    readiness.required_bytes, readiness.probe_path
                ),
            ),
            "blocked" => (
                "warn",
                format!(
                    "Full rebuild blocked by disk headroom: {} bytes required, {} bytes available at {} ({} bytes short){}",
                    readiness.required_bytes,
                    readiness.available_bytes.unwrap_or(0),
                    readiness.probe_path,
                    readiness.shortfall_bytes,
                    if readiness.enforced {
                        ""
                    } else {
                        "; CASS_INDEX_SKIP_DISK_HEADROOM_CHECK is set, so the indexer would not enforce this"
                    }
                ),
            ),
            _ => (
                "warn",
                readiness
                    .notes
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "Full-rebuild readiness was not checked".to_string()),
            ),
        };
        add_check!("full_rebuild_readiness", status, message, false);
    }

    // 2. Check for stale lock files
    if lock_path.exists() {
        // Check if lock is stale (older than 1 hour)
        let is_stale = std::fs::metadata(&lock_path)
            .and_then(|m| m.modified())
            .map(|t| t.elapsed().map(|d| d.as_secs() > 3600).unwrap_or(true))
            .unwrap_or(true);

        if is_stale {
            if fix_can_mutate {
                let planned_bytes = fs_dir_size(&lock_path);
                let action_id = doctor_fs_mutation_action_id(
                    DoctorFsMutationKind::RemoveStaleLegacyIndexLock,
                    DoctorRepairMode::SafeAutoRun,
                    DoctorAssetClass::ReclaimableDerivedCache,
                    &lock_path,
                    planned_bytes,
                );
                let request = DoctorFsMutationRequest {
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
                    required_min_age_seconds: Some(3600),
                };
                let stale_lock_artifacts = [DoctorForensicCopySpec {
                    artifact_kind: "stale_legacy_index_lock",
                    source_path: &lock_path,
                    relative_path: Path::new("operation/stale-legacy-index-lock"),
                    required: true,
                    sidecar_group: None,
                }];
                let forensic_bundle =
                    capture_doctor_forensic_bundle(DoctorForensicBundleCaptureInput {
                        operation_id: request.operation_id,
                        data_dir: &data_dir,
                        db_path: &db_path,
                        index_path: &index_path,
                        plan: None,
                        quarantine_report: None,
                        extra_file_artifacts: &stale_lock_artifacts,
                    });
                let mut mutation_receipt = if forensic_bundle.status == "captured" {
                    execute_doctor_fs_mutation(request)
                } else {
                    let mut receipt = doctor_fs_mutation_receipt_for_request(&request);
                    let reason = forensic_bundle
                        .blocked_reason
                        .clone()
                        .unwrap_or_else(|| "forensic bundle capture did not complete".to_string());
                    receipt.blocked_reasons.push(format!(
                        "forensic bundle capture failed before stale lock mutation: {reason}"
                    ));
                    receipt
                };
                mutation_receipt.forensic_bundle = forensic_bundle;
                if mutation_receipt.status == DoctorActionStatus::Applied {
                    checks.push(DoctorCheck {
                        name: "lock_file".to_string(),
                        status: "pass".to_string(),
                        message: "Stale lock file removed".to_string(),
                        fix_available: true,
                        fix_applied: true,
                    });
                    auto_fix_actions.push("Removed stale lock file".to_string());
                    auto_fix_applied = true;
                } else {
                    let reason = if mutation_receipt.blocked_reasons.is_empty() {
                        "no executor refusal reason recorded".to_string()
                    } else {
                        mutation_receipt.blocked_reasons.join("; ")
                    };
                    add_check!(
                        "lock_file",
                        "warn",
                        format!(
                            "Stale lock file found (older than 1 hour) and removal was refused: {reason}"
                        ),
                        true
                    );
                }
                fs_mutation_receipts.push(mutation_receipt);
            } else if fix {
                add_check!(
                    "lock_file",
                    "warn",
                    "Stale lock file found (older than 1 hour) but mutating repair was not allowed",
                    true
                );
            } else {
                add_check!(
                    "lock_file",
                    "warn",
                    "Stale lock file found (older than 1 hour) - run with --fix to remove",
                    true
                );
            }
        } else {
            add_check!(
                "lock_file",
                "warn",
                "Active lock file found - another process may be indexing",
                false
            );
        }
    } else {
        add_check!("lock_file", "pass", "No stale lock files", false);
    }

    let state = DoctorRunState {
        output_format,
        fix,
        verbose,
        force_rebuild,
        allow_repeated_repair,
        command_surface,
        execution_mode,
        requested_plan_fingerprint,
        wrap,
        start,
        data_dir,
        db_path,
        index_path,
        timing_spans,
        maintenance_snapshot,
        rebuild_active,
        cleanup_apply_requested,
        safe_auto_run_requested,
        repair_class,
        initial_failure_marker,
        repeat_refusal_reason,
        repeat_repair_refused,
        override_available,
        override_used,
        _doctor_lock_guard,
        _doctor_db_open_bypass_guard,
        operation_state,
        mutating_lock_acquired,
        fix_can_mutate,
        not_initialized,
        explanation,
        recommended_action,
        checks,
        needs_rebuild,
        db_ok,
        db_conversations,
        db_messages,
        storage_db_open_failed,
        storage_integrity_failed,
        storage_integrity_unverified,
        storage_integrity_skipped_reason,
        storage_probe_timed_out,
        storage_lexical_index_drifted,
        storage_attestation_check_depth,
        storage_attestation_detail,
        dedicated_storage_probe,
        matching_cached_integrity_attestation,
        auto_fix_actions,
        auto_fix_applied,
        fs_mutation_receipts,
        candidate_promotion_report,
        storage_pressure,
    };

    // 3. Check database exists and is readable
    let (state, db_wal) = db_and_lexical_probes::probe_doctor_database_and_wal(state);
    let (state, lexical_ws) =
        db_and_lexical_probes::probe_doctor_lexical_and_workspace(state, db_wal);
    let (state, lexical_ws, evidence) =
        recovery_evidence::collect_doctor_recovery_evidence(state, lexical_ws);
    let (state, evidence, repair_out) =
        repair_apply::execute_doctor_repairs_and_cleanup(state, &lexical_ws, evidence);
    verdict_and_output::finalize_and_render_doctor_report(state, lexical_ws, evidence, repair_out)
}
