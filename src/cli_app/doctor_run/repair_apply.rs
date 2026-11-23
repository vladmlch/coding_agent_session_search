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

use super::{
    DoctorCheck, DoctorLexicalWorkspaceReport, DoctorRecoveryEvidence, DoctorRepairApplyResult,
    DoctorRunState,
};

#[allow(clippy::collapsible_if, clippy::collapsible_else_if)]
pub(super) fn execute_doctor_repairs_and_cleanup(
    state: DoctorRunState,
    lexical_ws: &DoctorLexicalWorkspaceReport,
    evidence: DoctorRecoveryEvidence,
) -> (
    DoctorRunState,
    DoctorRecoveryEvidence,
    DoctorRepairApplyResult,
) {
    #[allow(unused_imports)]
    use colored::*;

    let DoctorRunState {
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
        mut timing_spans,
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
        mut _doctor_lock_guard,
        mut _doctor_db_open_bypass_guard,
        mut operation_state,
        mutating_lock_acquired,
        mut fix_can_mutate,
        not_initialized,
        explanation,
        recommended_action,
        mut checks,
        mut needs_rebuild,
        mut db_ok,
        mut db_conversations,
        mut db_messages,
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
        mut auto_fix_actions,
        mut auto_fix_applied,
        mut fs_mutation_receipts,
        mut candidate_promotion_report,
        storage_pressure,
    } = state;

    let archive_queryable_for_non_destructive_derived_rebuild =
        lexical_ws.archive_queryable_for_non_destructive_derived_rebuild;
    let canonical_candidate_replacement_authorized =
        lexical_ws.canonical_candidate_replacement_authorized;
    let archive_wide_collectors_complete = lexical_ws.archive_wide_collectors_complete;

    let DoctorRecoveryEvidence {
        raw_mirror,
        raw_mirror_backfill,
        sole_copy_warnings,
        coverage_summary,
        coverage_risk,
        source_authority,
        candidate_repair_authority_complete,
        isolated_candidate_built,
        candidate_staging,
        readiness_snapshot,
        fallback_mode,
        derived_semantic_assets,
    } = evidence;

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

    let mut repair_plan = if command_surface == doctor::DoctorCommandSurface::Repair {
        Some(build_doctor_repair_plan_preview(
            &data_dir,
            &db_path,
            &index_path,
            output_format,
            force_rebuild,
            allow_repeated_repair,
            execution_mode == doctor::DoctorExecutionMode::RepairDryRun,
            execution_mode == doctor::DoctorExecutionMode::FingerprintApply,
            requested_plan_fingerprint.clone(),
            &operation_state,
            &initial_failure_marker,
            needs_rebuild,
            db_ok,
            archive_queryable_for_non_destructive_derived_rebuild,
            canonical_candidate_replacement_authorized,
            archive_wide_collectors_complete,
            candidate_repair_authority_complete,
            db_messages,
            &raw_mirror_backfill,
            &coverage_summary,
            &coverage_risk,
            &source_authority,
            &candidate_staging,
        ))
    } else {
        None
    };
    if let Some(plan) = repair_plan.as_mut() {
        if execution_mode == doctor::DoctorExecutionMode::RepairDryRun {
            checks.push(DoctorCheck {
                name: "repair_plan_approval".to_string(),
                status: "pass".to_string(),
                message: format!(
                    "Repair dry-run plan ready; apply with plan fingerprint {}",
                    plan.plan_fingerprint
                ),
                fix_available: plan.planned_action_count > 0,
                fix_applied: false,
            });
        } else if execution_mode == doctor::DoctorExecutionMode::FingerprintApply {
            if plan.approval_status == "matched" {
                if plan.planned_action_count == 0 {
                    plan.apply_authorized = true;
                    plan.will_mutate = false;
                    checks.push(DoctorCheck {
                        name: "repair_plan_approval".to_string(),
                        status: "pass".to_string(),
                        message: format!(
                            "Repair plan fingerprint matched; no repair actions were selected: {}",
                            plan.plan_fingerprint
                        ),
                        fix_available: false,
                        fix_applied: false,
                    });
                } else if mutating_lock_acquired
                    && operation_state.mutating_doctor_allowed
                    && !repeat_repair_refused
                {
                    fix_can_mutate = true;
                    plan.apply_authorized = true;
                    plan.will_mutate = plan.planned_action_count > 0;
                    plan.approval_status = "matched".to_string();
                    checks.push(DoctorCheck {
                        name: "repair_plan_approval".to_string(),
                        status: "pass".to_string(),
                        message: format!(
                            "Repair plan fingerprint matched and mutation lock is held: {}",
                            plan.plan_fingerprint
                        ),
                        fix_available: plan.planned_action_count > 0,
                        fix_applied: false,
                    });
                } else {
                    match doctor_acquire_mutation_lock(&data_dir, &db_path) {
                        Ok((guard, observation)) => {
                            _doctor_lock_guard = Some(guard);
                            _doctor_db_open_bypass_guard = Some(
                                crate::storage::sqlite::enter_doctor_mutation_db_open_bypass(),
                            );
                            operation_state = build_doctor_operation_state_report(
                                &data_dir,
                                &db_path,
                                &index_path,
                                &maintenance_snapshot,
                                &observation,
                            );
                            let lock_acquired = matches!(
                                observation,
                                DoctorMutationLockObservation::Acquired { .. }
                            );
                            if operation_state.mutating_doctor_allowed
                                && lock_acquired
                                && !repeat_repair_refused
                            {
                                fix_can_mutate = true;
                                plan.apply_authorized = true;
                                plan.will_mutate = plan.planned_action_count > 0;
                                plan.approval_status = "matched".to_string();
                                checks.push(DoctorCheck {
                                    name: "repair_plan_approval".to_string(),
                                    status: "pass".to_string(),
                                    message: format!(
                                        "Repair plan fingerprint matched and mutation lock was acquired: {}",
                                        plan.plan_fingerprint
                                    ),
                                    fix_available: plan.planned_action_count > 0,
                                    fix_applied: false,
                                });
                            } else {
                                let reason = operation_state
                                    .mutation_blocked_reason
                                    .clone()
                                    .unwrap_or_else(|| {
                                        "doctor mutation lock was not acquired after plan revalidation"
                                            .to_string()
                                    });
                                plan.apply_authorized = false;
                                plan.will_mutate = false;
                                plan.approval_status = "lock_uncertain".to_string();
                                if !plan
                                    .branchable_blocker_codes
                                    .iter()
                                    .any(|code| code == "lock-uncertain")
                                {
                                    plan.branchable_blocker_codes
                                        .push("lock-uncertain".to_string());
                                }
                                plan.blocked_reasons
                                    .push(format!("lock-uncertain: {reason}"));
                                checks.push(DoctorCheck {
                                    name: "repair_plan_approval".to_string(),
                                    status: "fail".to_string(),
                                    message: format!(
                                        "Repair plan fingerprint matched, but apply was blocked before mutation: {reason}"
                                    ),
                                    fix_available: false,
                                    fix_applied: false,
                                });
                            }
                        }
                        Err(observation) => {
                            operation_state = build_doctor_operation_state_report(
                                &data_dir,
                                &db_path,
                                &index_path,
                                &maintenance_snapshot,
                                &observation,
                            );
                            let reason = operation_state
                                .mutation_blocked_reason
                                .clone()
                                .unwrap_or_else(|| {
                                    "doctor mutation lock could not be acquired after plan revalidation"
                                        .to_string()
                                });
                            plan.apply_authorized = false;
                            plan.will_mutate = false;
                            plan.approval_status = "lock_uncertain".to_string();
                            if !plan
                                .branchable_blocker_codes
                                .iter()
                                .any(|code| code == "lock-uncertain")
                            {
                                plan.branchable_blocker_codes
                                    .push("lock-uncertain".to_string());
                            }
                            plan.blocked_reasons
                                .push(format!("lock-uncertain: {reason}"));
                            checks.push(DoctorCheck {
                                name: "repair_plan_approval".to_string(),
                                status: "fail".to_string(),
                                message: format!(
                                    "Repair plan fingerprint matched, but apply was blocked before mutation: {reason}"
                                ),
                                fix_available: false,
                                fix_applied: false,
                            });
                        }
                    }
                }
            } else {
                let reason = if plan.provided_plan_fingerprint.is_none() {
                    "approval-fingerprint-missing"
                } else if plan.approval_status == "blocked" {
                    "plan-blocked"
                } else {
                    "approval-fingerprint-mismatched"
                };
                if !plan
                    .branchable_blocker_codes
                    .iter()
                    .any(|code| code == reason)
                {
                    plan.branchable_blocker_codes.push(reason.to_string());
                }
                checks.push(DoctorCheck {
                    name: "repair_plan_approval".to_string(),
                    status: "fail".to_string(),
                    message: format!(
                        "Repair apply refused before mutation: {reason}; rerun dry-run and apply the exact current fingerprint"
                    ),
                    fix_available: false,
                    fix_applied: false,
                });
            }
        }
    }

    let candidate_promotion_plan_requested = repair_plan.as_ref().is_some_and(|plan| {
        plan.apply_authorized
            && doctor_repair_plan_has_action(
                plan,
                DOCTOR_REPAIR_ACTION_PROMOTE_RECONSTRUCT_CANDIDATE,
            )
    });
    // Defense in depth: even a malformed/stale plan object must satisfy all
    // live replacement-authority gates again immediately before mutation.
    let candidate_promotion_live_blocker = if archive_queryable_for_non_destructive_derived_rebuild
    {
        Some((
            "canonical-archive-queryable",
            "candidate promotion refused because bounded canonical archive reads succeeded; deferred deep integrity is not replacement authority",
        ))
    } else if !canonical_candidate_replacement_authorized {
        Some((
            "canonical-archive-failure-unproven",
            "candidate promotion refused because no affirmative current integrity failure or absent canonical DB authorizes replacement",
        ))
    } else if !candidate_repair_authority_complete {
        Some((
            "archive-authority-collectors-unchecked",
            "candidate promotion refused because archive-wide coverage and source-authority collectors are unchecked",
        ))
    } else {
        None
    };
    let candidate_promotion_blocked_live =
        candidate_promotion_plan_requested && candidate_promotion_live_blocker.is_some();
    if candidate_promotion_blocked_live {
        fix_can_mutate = false;
        if let Some(plan) = repair_plan.as_mut() {
            plan.apply_authorized = false;
            plan.will_mutate = false;
            plan.approval_status = "blocked".to_string();
            let (code, reason) = candidate_promotion_live_blocker
                .expect("live blocker must exist when candidate promotion is blocked");
            doctor_repair_push_blocker(
                &mut plan.blocked_reasons,
                &mut plan.branchable_blocker_codes,
                code,
                reason.to_string(),
            );
        }
        checks.push(DoctorCheck {
            name: "candidate_promotion".to_string(),
            status: "fail".to_string(),
            message: candidate_promotion_live_blocker
                .map(|(_, reason)| reason)
                .unwrap_or("candidate promotion refused by live safety gates")
                .to_string(),
            fix_available: false,
            fix_applied: false,
        });
    }
    let candidate_promotion_apply_requested =
        candidate_promotion_plan_requested && !candidate_promotion_blocked_live;
    let candidate_promotion_started = Instant::now();
    if candidate_promotion_apply_requested && fix_can_mutate {
        let candidate_promotion_rebuild_planned = repair_plan.as_ref().is_some_and(|plan| {
            doctor_repair_plan_has_action(
                plan,
                DOCTOR_REPAIR_ACTION_REBUILD_FROM_PROMOTED_CANDIDATE,
            )
        });
        let selected_candidate =
            doctor_repair_select_completed_candidate_for_promotion(&candidate_staging);
        if let Some(candidate) = selected_candidate {
            let candidate_dir = PathBuf::from(&candidate.path);
            let promotion = promote_doctor_reconstruct_candidate_bundle(
                &data_dir,
                &db_path,
                &index_path,
                &candidate_dir,
            );
            fs_mutation_receipts.extend(promotion.fs_mutation_receipts.clone());
            match promotion.status.as_str() {
                "applied" => {
                    auto_fix_applied = true;
                    auto_fix_actions.push(format!(
                        "Promoted verified reconstruct candidate {} into the live archive bundle",
                        promotion.candidate_id
                    ));
                    checks.push(DoctorCheck {
                        name: "candidate_promotion".to_string(),
                        status: "pass".to_string(),
                        message: format!(
                            "Promoted reconstruct candidate {} with rollback receipt {}",
                            promotion.candidate_id, promotion.redacted_receipt_path
                        ),
                        fix_available: true,
                        fix_applied: true,
                    });

                    match open_franken_cli_read_db_with_hard_timeout(
                        db_path.to_path_buf(),
                        "doctor promoted candidate archive",
                        Duration::from_secs(30),
                    ) {
                        Ok(conn) => {
                            use crate::franken_sync::compat::RowExt as _;

                            let conv_count: Option<i64> = conn
                                .query_row_map(
                                    "SELECT COUNT(*) FROM conversations",
                                    &[],
                                    |r: &crate::franken_sync::Row| r.get_typed(0),
                                )
                                .ok();
                            let msg_count: Option<i64> = conn
                                .query_row_map(
                                    "SELECT COUNT(*) FROM messages",
                                    &[],
                                    |r: &crate::franken_sync::Row| r.get_typed(0),
                                )
                                .ok();
                            let quick_check_status = doctor_database_quick_check(&conn).ok();

                            if let (Some(conv_count), Some(msg_count), Some(status)) =
                                (conv_count, msg_count, quick_check_status)
                            {
                                if status.trim().eq_ignore_ascii_case("ok") {
                                    db_ok = true;
                                    db_conversations = Some(conv_count.max(0) as usize);
                                    db_messages = Some(msg_count.max(0) as usize);
                                    checks.push(DoctorCheck {
                                        name: "database_after_candidate_promotion".to_string(),
                                        status: "pass".to_string(),
                                        message: format!(
                                            "Promoted archive DB passed frankensqlite quick_check ({} conversations, {} messages)",
                                            conv_count, msg_count
                                        ),
                                        fix_available: false,
                                        fix_applied: false,
                                    });
                                    if candidate_promotion_rebuild_planned {
                                        needs_rebuild = msg_count > 0;
                                    }
                                    if promotion.derived_lexical_rebuild_required && !needs_rebuild
                                    {
                                        needs_rebuild = msg_count > 0;
                                    }
                                } else {
                                    db_ok = false;
                                    fix_can_mutate = false;
                                    needs_rebuild = false;
                                    checks.push(DoctorCheck {
                                        name: "database_after_candidate_promotion".to_string(),
                                        status: "fail".to_string(),
                                        message: format!(
                                            "Promoted archive DB failed frankensqlite quick_check: {}",
                                            status.trim()
                                        ),
                                        fix_available: true,
                                        fix_applied: false,
                                    });
                                }
                            } else {
                                db_ok = false;
                                fix_can_mutate = false;
                                needs_rebuild = false;
                                checks.push(DoctorCheck {
                                    name: "database_after_candidate_promotion".to_string(),
                                    status: "fail".to_string(),
                                    message:
                                        "Promoted archive DB could not report conversation/message counts and quick_check"
                                            .to_string(),
                                    fix_available: true,
                                    fix_applied: false,
                                });
                            }
                            let _ = close_franken_cli_owner_read_db(
                                conn,
                                &db_path,
                                "doctor promoted candidate archive",
                            );
                        }
                        Err(err) => {
                            db_ok = false;
                            fix_can_mutate = false;
                            needs_rebuild = false;
                            checks.push(DoctorCheck {
                                name: "database_after_candidate_promotion".to_string(),
                                status: "fail".to_string(),
                                message: format!(
                                    "Cannot open promoted archive DB after candidate promotion: {}",
                                    err.message
                                ),
                                fix_available: true,
                                fix_applied: false,
                            });
                        }
                    }
                }
                "rolled_back" => {
                    fix_can_mutate = false;
                    needs_rebuild = false;
                    checks.push(DoctorCheck {
                        name: "candidate_promotion".to_string(),
                        status: "fail".to_string(),
                        message: format!(
                            "Candidate promotion failed and rolled back to prior live bundle backup: {}",
                            promotion
                                .rollback_reference
                                .as_deref()
                                .unwrap_or("rollback reference unavailable")
                        ),
                        fix_available: true,
                        fix_applied: false,
                    });
                }
                "blocked" | "failed" => {
                    fix_can_mutate = false;
                    needs_rebuild = false;
                    checks.push(DoctorCheck {
                        name: "candidate_promotion".to_string(),
                        status: "fail".to_string(),
                        message: format!(
                            "Candidate promotion {}: {}",
                            promotion.status,
                            promotion.blocked_reasons.join("; ")
                        ),
                        fix_available: true,
                        fix_applied: false,
                    });
                }
                other => {
                    fix_can_mutate = false;
                    needs_rebuild = false;
                    checks.push(DoctorCheck {
                        name: "candidate_promotion".to_string(),
                        status: "fail".to_string(),
                        message: format!("Candidate promotion returned unexpected status {other}"),
                        fix_available: true,
                        fix_applied: false,
                    });
                }
            }
            candidate_promotion_report = Some(promotion);
        } else {
            fix_can_mutate = false;
            needs_rebuild = false;
            checks.push(DoctorCheck {
                name: "candidate_promotion".to_string(),
                status: "fail".to_string(),
                message: "Candidate promotion apply was planned, but no single completed candidate remained after fingerprint approval".to_string(),
                fix_available: true,
                fix_applied: false,
            });
        }
    }
    doctor_push_timing_span(
        &mut timing_spans,
        "candidate_promotion",
        "candidate_promotion",
        "atomic_promotion",
        candidate_promotion_started,
        DOCTOR_SLOW_OPERATION_DEFAULT_THRESHOLD_MS,
        vec![if candidate_promotion_apply_requested {
            "candidate promotion path was evaluated with fingerprint and lock gates".to_string()
        } else {
            "candidate promotion was not requested for this doctor run".to_string()
        }],
    );

    // Promotion may have replaced and re-probed the live archive after the
    // candidate-planning snapshot above, so refresh this routing predicate
    // before choosing a repair implementation.
    let archive_queryable_for_non_destructive_derived_rebuild =
        doctor_archive_queryable_for_non_destructive_derived_rebuild(
            db_ok,
            db_conversations.is_some() && db_messages.is_some(),
            storage_integrity_skipped_reason.is_some(),
            matching_cached_integrity_attestation
                .as_ref()
                .is_some_and(|attestation| {
                    attestation.verdict
                        == crate::search::storage_integrity::IntegrityAttestationVerdict::Fail
                }),
        );

    // Apply fix: rebuild index if needed (only when --fix is passed)
    let derived_rebuild_started = Instant::now();
    let safe_auto_archive_rebuild_refused = safe_auto_run_requested
        && needs_rebuild
        && fix_can_mutate
        && !archive_queryable_for_non_destructive_derived_rebuild;
    if safe_auto_archive_rebuild_refused {
        checks.push(DoctorCheck {
            name: "safe_auto_archive_rebuild".to_string(),
            status: "fail".to_string(),
            message: "Safe auto-run refused archive DB replacement or source-session rebuild because the canonical cass archive is missing, unreadable, or corrupt; preserve the DB bundle and use a fingerprinted repair dry-run before any archive-risk action".to_string(),
            fix_available: true,
            fix_applied: false,
        });
    }
    let archive_rebuild_authority_refused = needs_rebuild
        && fix_can_mutate
        && !archive_queryable_for_non_destructive_derived_rebuild
        && (!canonical_candidate_replacement_authorized || !candidate_repair_authority_complete);
    if archive_rebuild_authority_refused {
        let reason = if !canonical_candidate_replacement_authorized {
            "canonical archive replacement has no affirmative failure/absence evidence"
        } else {
            "archive-wide coverage and source-authority collectors are unchecked"
        };
        add_check!(
            "archive_rebuild_authority",
            "warn",
            format!(
                "Archive move/replacement and source-session rebuild were refused: {reason}; timeout, busy, unverified, and deferred states preserve canonical bytes"
            ),
            false
        );
    }
    let derived_rebuild_attempted = needs_rebuild
        && fix_can_mutate
        && !safe_auto_archive_rebuild_refused
        && !archive_rebuild_authority_refused;
    if derived_rebuild_attempted {
        let stderr_is_tty = std::io::stderr().is_terminal();
        let is_robot = output_format.is_some();
        let show_progress = !is_robot && stderr_is_tty;
        let show_plain = !is_robot && !stderr_is_tty;

        if !is_robot {
            println!();
            println!(
                "{} Rebuilding index (this may take a moment)...",
                "→".cyan()
            );
        }

        let progress = std::sync::Arc::new(indexer::IndexingProgress::default());
        // A deferred deep integrity probe is not a corruption verdict.  If
        // bounded row counts succeeded and no current cached attestation says
        // FAIL, a derived-only Tantivy rebuild may read the canonical archive;
        // it must never fall through to the branch that moves/replaces it.
        let rebuild_from_db = archive_queryable_for_non_destructive_derived_rebuild;

        if rebuild_from_db {
            let total_convs = db_conversations.unwrap_or(0);
            let rebuild_handle = std::thread::spawn({
                let progress = progress.clone();
                let db_path = db_path.clone();
                let data_dir = data_dir.clone();
                move || rebuild_tantivy_from_db(&db_path, &data_dir, total_convs, Some(progress))
            });

            let rebuild_result = wait_with_progress(
                rebuild_handle,
                progress,
                show_progress,
                show_plain,
                "Rebuilding search index from database...",
            );

            match rebuild_result {
                Ok(message_count) => {
                    needs_rebuild = false;
                    auto_fix_actions.push("Rebuilt search index from database".to_string());
                    auto_fix_applied = true;
                    for check in &mut checks {
                        if check.name == "index" || check.name == "index_sync" {
                            check.status = "pass".to_string();
                            check.fix_applied = true;
                            check.message = "Search index rebuilt from database".to_string();
                        }
                    }
                    checks.push(DoctorCheck {
                        name: "rebuild".to_string(),
                        status: "pass".to_string(),
                        message: format!(
                            "Index rebuilt from database ({} messages)",
                            message_count
                        ),
                        fix_available: true,
                        fix_applied: true,
                    });
                    if let Some(report) = candidate_promotion_report.as_mut()
                        && report.status == "applied"
                        && (report.derived_lexical_rebuild_required
                            || report.derived_semantic_rebuild_required)
                    {
                        doctor_candidate_promotion_record_derived_followup_artifact(
                            &data_dir,
                            report,
                            message_count,
                            &fallback_mode,
                        );
                    }
                }
                Err(e) => {
                    checks.push(DoctorCheck {
                        name: "rebuild".to_string(),
                        status: "fail".to_string(),
                        message: format!("Index rebuild failed: {}", e),
                        fix_available: true,
                        fix_applied: false,
                    });
                }
            }
        } else {
            // Preserve existing DB when possible; rebuild only derived data.
            let mut can_rebuild = true;
            let mut db_backup_done = false;
            if !db_ok {
                let ts = chrono::Utc::now().format("%Y%m%d_%H%M%S");
                let backup_path = db_path.with_extension(format!("corrupt.{ts}"));
                match crate::storage::sqlite::move_database_bundle(&db_path, &backup_path) {
                    Ok(moved) if moved.moved_any() => {
                        let mut components = Vec::new();
                        if moved.database {
                            components.push("db");
                        }
                        if moved.wal {
                            components.push("wal");
                        }
                        if moved.shm {
                            components.push("shm");
                        }
                        let component_note = if components.is_empty() {
                            String::new()
                        } else {
                            format!(" ({})", components.join(", "))
                        };
                        db_backup_done = true;
                        checks.push(DoctorCheck {
                            name: "database_backup".to_string(),
                            status: "pass".to_string(),
                            message: format!(
                                "Backed up corrupted database bundle to {}{}",
                                backup_path.display(),
                                component_note
                            ),
                            fix_available: true,
                            fix_applied: true,
                        });
                        auto_fix_actions.push(format!(
                            "Backed up corrupted database bundle to {}{}",
                            backup_path.display(),
                            component_note
                        ));
                        auto_fix_applied = true;
                    }
                    Ok(_) => {}
                    Err(e) => {
                        checks.push(DoctorCheck {
                            name: "database_backup".to_string(),
                            status: "fail".to_string(),
                            message: format!("Failed to backup corrupted database bundle: {}", e),
                            fix_available: true,
                            fix_applied: false,
                        });
                        can_rebuild = false;
                    }
                }
            }

            if !can_rebuild {
                checks.push(DoctorCheck {
                    name: "rebuild".to_string(),
                    status: "fail".to_string(),
                    message: "Index rebuild skipped because database backup failed".to_string(),
                    fix_available: true,
                    fix_applied: false,
                });
                needs_rebuild = true;
            } else {
                let index_opts = indexer::IndexOptions {
                    // When the database is missing or corrupted, doctor must
                    // rebuild from source sessions using the full path rather
                    // than the incremental UPSERT-based path.
                    full: force_rebuild || !db_ok,
                    force_rebuild,
                    watch: false,
                    watch_once_paths: None,
                    db_path: db_path.clone(),
                    data_dir: data_dir.clone(),
                    semantic: false,
                    build_hnsw: false,
                    embedder: "fastembed".to_string(),
                    progress: Some(progress.clone()),
                    watch_interval_secs: 30,
                };

                let rebuild_handle = std::thread::spawn(move || {
                    indexer::run_index(index_opts, None)
                        .map(|_| 0usize)
                        .map_err(|e| CliError {
                            code: 5,
                            kind: CliErrorKind::Doctor.kind_str(),
                            message: format!("index rebuild failed: {e}"),
                            hint: None,
                            retryable: true,
                        })
                });

                let rebuild_result = wait_with_progress(
                    rebuild_handle,
                    progress,
                    show_progress,
                    show_plain,
                    "Rebuilding index from source sessions...",
                );

                match rebuild_result {
                    Ok(_) => {
                        needs_rebuild = false;
                        let rebuild_note = if db_backup_done {
                            "Rebuilt index from source sessions (new database created)".to_string()
                        } else {
                            "Rebuilt index from source sessions (database preserved)".to_string()
                        };
                        auto_fix_actions.push(rebuild_note.clone());
                        auto_fix_applied = true;
                        for check in &mut checks {
                            if check.name == "index" || check.name == "index_sync" {
                                check.status = "pass".to_string();
                                check.fix_applied = true;
                                check.message = rebuild_note.clone();
                            }
                        }
                        checks.push(DoctorCheck {
                            name: "rebuild".to_string(),
                            status: "pass".to_string(),
                            message: "Index rebuilt successfully".to_string(),
                            fix_available: true,
                            fix_applied: true,
                        });
                    }
                    Err(e) => {
                        checks.push(DoctorCheck {
                            name: "rebuild".to_string(),
                            status: "fail".to_string(),
                            message: format!("Index rebuild failed: {}", e),
                            fix_available: true,
                            fix_applied: false,
                        });
                    }
                }
            }
        }
    }
    doctor_push_timing_span(
        &mut timing_spans,
        "derived_rebuild",
        "rebuild",
        "derived_rebuild",
        derived_rebuild_started,
        DOCTOR_SLOW_OPERATION_DEFAULT_THRESHOLD_MS,
        vec![if derived_rebuild_attempted {
            "derived index rebuild path ran under doctor mutation gates".to_string()
        } else {
            "derived index rebuild was not attempted for this doctor run".to_string()
        }],
    );

    let cleanup_apply_started = Instant::now();
    let cleanup_apply_result = if cleanup_apply_requested && fix_can_mutate {
        // Pass-13 structural refactor: create a per-run journal directory so the
        // FsMutationExecutor's `Applied` receipts also land in `actions.jsonl`.
        // `cass doctor --undo <run-id>` / `--ls` / `--diff` then see real
        // production mutations from the cleanup-apply path, not only mutations
        // produced by `doctor_chokepoint::mutate()` from new fixers.
        //
        // Journaling is best-effort: if the run-dir cannot be created we still
        // run the cleanup (the FsMutationExecutor itself is the safety gate),
        // we just lose the post-hoc observability for this invocation.
        let target_sha = option_env!("VERGEN_GIT_SHA").unwrap_or("unknown");
        let journal_run_id = crate::doctor_runs::RunId::new(target_sha);
        let journal_run_id_str = journal_run_id.as_str().to_string();
        let journal_run_dir = match crate::doctor_runs::create_run_dir(&data_dir, &journal_run_id) {
            Ok(dir) => {
                if let Err(err) = crate::doctor_runs::append_action(
                    &dir,
                    &crate::doctor_runs::ActionRecord::RunStarted {
                        schema_version: crate::doctor_runs::RUN_ARTIFACT_SCHEMA_VERSION,
                        run_id: journal_run_id_str.clone(),
                        target_sha: target_sha.to_string(),
                        mode: "cleanup-apply".to_string(),
                        started_at_ms: doctor_now_ms(),
                    },
                ) {
                    tracing::warn!(
                        target: "cass::doctor::cleanup",
                        run_id = %journal_run_id_str,
                        error = %err,
                        "failed to append RunStarted to cleanup-apply journal; subsequent records will lack a session-opener"
                    );
                }
                Some(dir)
            }
            Err(err) => {
                tracing::warn!(
                    target: "cass::doctor::cleanup",
                    run_id = %journal_run_id_str,
                    error = %err,
                    "failed to create per-run journal directory; cleanup-apply will proceed without journaling"
                );
                None
            }
        };
        let result = apply_diag_quarantine_cleanup(
            &data_dir,
            &db_path,
            &index_path,
            rebuild_active,
            requested_plan_fingerprint.as_deref(),
            journal_run_dir.as_deref(),
            journal_run_dir
                .as_ref()
                .map(|_| journal_run_id_str.as_str()),
        );
        if let Some(run_dir) = journal_run_dir.as_deref() {
            // RunEnded records the cleanup-apply outcome so `cass doctor --diff`
            // and `--ls` distinguish a fully-applied run from a partially-
            // applied run, a no-op, or one blocked by the apply gate. We keep
            // exit_code = 0 because cleanup-apply is not the surface that
            // produces a non-zero process exit — the outer doctor exit is
            // computed separately from cleanup_status further down. The
            // four-branch selection lives in `cleanup_apply_exit_code_kind`
            // so it can be unit-tested without an e2e fixture.
            let exit_code_kind = cleanup_apply_exit_code_kind(
                result.applied,
                result.pruned_asset_count,
                !result.blocked_reasons.is_empty(),
            );
            if let Err(err) = crate::doctor_runs::append_action(
                run_dir,
                &crate::doctor_runs::ActionRecord::RunEnded {
                    run_id: journal_run_id_str.clone(),
                    exit_code: 0,
                    exit_code_kind: exit_code_kind.to_string(),
                    ended_at_ms: doctor_now_ms(),
                },
            ) {
                tracing::warn!(
                    target: "cass::doctor::cleanup",
                    run_id = %journal_run_id_str,
                    exit_code_kind,
                    error = %err,
                    "failed to append RunEnded to cleanup-apply journal; this run will appear as 'in-flight' to crash-recovery"
                );
            }
        }
        let cleanup_status = if result.applied {
            "pass"
        } else if result.before_reclaim_candidate_count > 0
            || result
                .actions
                .iter()
                .any(|action| action.artifact_kind == "retained_publish_backup")
        {
            "warn"
        } else {
            "pass"
        };
        let cleanup_message = if result.applied {
            format!(
                "Pruned {} derivative cleanup artifact(s), reclaimed {} bytes",
                result.pruned_asset_count, result.reclaimed_bytes
            )
        } else if !result.blocked_reasons.is_empty() {
            format!(
                "Derivative cleanup skipped: {}",
                result.blocked_reasons.join("; ")
            )
        } else {
            "No derivative cleanup artifacts were eligible for pruning".to_string()
        };
        checks.push(DoctorCheck {
            name: "derivative_cleanup".to_string(),
            status: cleanup_status.to_string(),
            message: cleanup_message.clone(),
            fix_available: result.before_reclaim_candidate_count > 0
                || result
                    .actions
                    .iter()
                    .any(|action| action.artifact_kind == "retained_publish_backup"),
            fix_applied: result.applied,
        });
        if result.applied {
            auto_fix_actions.push(cleanup_message);
            auto_fix_applied = true;
        }
        Some(result)
    } else {
        None
    };
    doctor_push_timing_span(
        &mut timing_spans,
        "cleanup_apply",
        "derivative_cleanup",
        "cleanup_apply",
        cleanup_apply_started,
        DOCTOR_SLOW_OPERATION_DEFAULT_THRESHOLD_MS,
        vec![if cleanup_apply_requested {
            "cleanup apply path evaluated derivative-only reclamation gates".to_string()
        } else {
            "cleanup apply was not requested for this doctor run".to_string()
        }],
    );

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
    let evidence = DoctorRecoveryEvidence {
        raw_mirror,
        raw_mirror_backfill,
        sole_copy_warnings,
        coverage_summary,
        coverage_risk,
        source_authority,
        candidate_repair_authority_complete,
        isolated_candidate_built,
        candidate_staging,
        readiness_snapshot,
        fallback_mode,
        derived_semantic_assets,
    };
    let repair_out = DoctorRepairApplyResult {
        repair_plan,
        archive_queryable_for_non_destructive_derived_rebuild,
        cleanup_apply_result,
    };
    (state, evidence, repair_out)
}
