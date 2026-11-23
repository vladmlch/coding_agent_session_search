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

use super::{DoctorCheck, DoctorLexicalWorkspaceReport, DoctorRecoveryEvidence, DoctorRunState};

#[allow(clippy::collapsible_if, clippy::collapsible_else_if)]
pub(super) fn collect_doctor_recovery_evidence(
    state: DoctorRunState,
    lexical_ws: DoctorLexicalWorkspaceReport,
) -> (
    DoctorRunState,
    DoctorLexicalWorkspaceReport,
    DoctorRecoveryEvidence,
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
        _doctor_lock_guard,
        _doctor_db_open_bypass_guard,
        operation_state,
        mutating_lock_acquired,
        fix_can_mutate,
        not_initialized,
        explanation,
        recommended_action,
        mut checks,
        mut needs_rebuild,
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
        mut auto_fix_actions,
        mut auto_fix_applied,
        fs_mutation_receipts,
        candidate_promotion_report,
        storage_pressure,
    } = state;

    let DoctorLexicalWorkspaceReport {
        archive_queryable_for_non_destructive_derived_rebuild,
        canonical_candidate_replacement_authorized,
        storage_busy_or_locked,
        archive_wide_collectors_deferred_reason,
        archive_wide_collectors_complete,
        storage_integrity_report,
        config_exclusion_risks,
        source_inventory,
    } = lexical_ws;

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

    let raw_mirror_scan_started = Instant::now();
    let mut raw_mirror = collect_doctor_raw_mirror_report_for_doctor(&data_dir, fix_can_mutate);
    doctor_push_timing_span(
        &mut timing_spans,
        "raw_mirror_scan",
        "raw_mirror",
        "raw_mirror_scan",
        raw_mirror_scan_started,
        DOCTOR_SLOW_OPERATION_DEFAULT_THRESHOLD_MS,
        vec!["raw mirror manifests and blob checksums were summarized".to_string()],
    );
    let raw_mirror_backfill_started = Instant::now();
    let raw_mirror_backfill = collect_doctor_raw_mirror_backfill_report_for_doctor(
        &data_dir,
        &db_path,
        &raw_mirror,
        fix_can_mutate,
        archive_wide_collectors_deferred_reason.as_deref(),
    );
    let raw_mirror_backfill_applied =
        matches!(raw_mirror_backfill.status.as_str(), "applied" | "partial")
            && (raw_mirror_backfill.captured_live_source_count > 0
                || raw_mirror_backfill.existing_raw_manifest_link_count > 0);
    if raw_mirror_backfill_applied {
        auto_fix_applied = true;
        auto_fix_actions.push(format!(
            "Raw mirror backfill captured {} live source file(s) and linked {} existing raw manifest(s)",
            raw_mirror_backfill.captured_live_source_count,
            raw_mirror_backfill.existing_raw_manifest_link_count
        ));
        raw_mirror = collect_doctor_raw_mirror_report_for_doctor(&data_dir, fix_can_mutate);
    }
    doctor_push_timing_span(
        &mut timing_spans,
        "raw_mirror_backfill",
        "raw_mirror_backfill",
        "raw_mirror_backfill",
        raw_mirror_backfill_started,
        DOCTOR_SLOW_OPERATION_DEFAULT_THRESHOLD_MS,
        vec![if !archive_wide_collectors_complete {
            "archive-wide raw mirror backfill scan was explicitly deferred".to_string()
        } else if raw_mirror_backfill.status == "deferred" {
            "raw mirror backfill was explicitly deferred because its evidence prerequisites were not fully verified".to_string()
        } else if raw_mirror_backfill_applied {
            "raw mirror backfill applied and raw mirror summary was refreshed".to_string()
        } else {
            "raw mirror backfill plan was evaluated without provider source mutation".to_string()
        }],
    );
    // ub29t: bound the per-manifest listing (summary counts stay complete;
    // problem manifests are never elided; --verbose restores everything).
    doctor_apply_raw_mirror_listing_bound(&mut raw_mirror, verbose);
    let recovery_evidence_deferred_reason = archive_wide_collectors_deferred_reason
        .clone()
        .or_else(|| {
            (raw_mirror.status == "verification_deferred").then(|| {
                "raw_mirror_verification_deferred: bounded read-only doctor did not establish checksum-verified raw-mirror recovery evidence"
                    .to_string()
            })
        });
    let recovery_evidence_collectors_complete = recovery_evidence_deferred_reason.is_none();
    match raw_mirror.status.as_str() {
        "absent" => {
            add_check!(
                "raw_mirror",
                "pass",
                "Raw mirror layout not initialized yet; future capture will use raw-mirror/v1",
                false
            );
        }
        "empty" => {
            add_check!(
                "raw_mirror",
                "pass",
                "Raw mirror layout exists but contains no manifests yet",
                false
            );
        }
        "verified" => {
            add_check!(
                "raw_mirror",
                "pass",
                format!(
                    "Raw mirror verified ({} manifest(s), {} blob byte(s))",
                    raw_mirror.summary.manifest_count, raw_mirror.summary.total_blob_bytes
                ),
                false
            );
        }
        "verification_deferred" => {
            let amplification_suffix = if raw_mirror.summary.amplified_source_count > 0 {
                format!(
                    "; {} source path(s) materially exceed their current size (see raw_mirror.amplified_sources)",
                    raw_mirror.summary.amplified_source_count
                )
            } else {
                String::new()
            };
            add_check!(
                "raw_mirror",
                "warn",
                format!(
                    "Raw mirror verification deferred for bounded read-only doctor check ({} manifest(s)){amplification_suffix}; set {CASS_DOCTOR_RAW_MIRROR_FULL_VERIFY}=1 to run full blob checksum verification",
                    raw_mirror.summary.manifest_count,
                ),
                false
            );
        }
        _ => {
            add_check!(
                "raw_mirror",
                "warn",
                format!(
                    "Raw mirror needs inspection: {} invalid manifest(s), {} missing blob(s), {} checksum mismatch(es), {} unverified manifest checksum(s), {} interrupted capture artifact(s)",
                    raw_mirror.summary.invalid_manifest_count,
                    raw_mirror.summary.missing_blob_count,
                    raw_mirror.summary.checksum_mismatch_count
                        + raw_mirror.summary.manifest_checksum_mismatch_count,
                    raw_mirror.summary.manifest_checksum_not_recorded_count,
                    raw_mirror.summary.interrupted_capture_count
                ),
                false
            );
        }
    }
    let backfill_fix_available = raw_mirror_backfill.status == "planned";
    let backfill_fix_applied = raw_mirror_backfill_applied;
    let (backfill_check_status, backfill_check_message) = match raw_mirror_backfill.status.as_str()
    {
        "deferred" => (
            "warn",
            raw_mirror_backfill
                .db_query_error
                .clone()
                .unwrap_or_else(|| "archive_wide_collectors_deferred".to_string()),
        ),
        "skipped" => (
            "pass",
            "Raw mirror backfill skipped until archive rows exist".to_string(),
        ),
        "clean" => (
            "pass",
            format!(
                "Raw mirror backfill clean ({} archive conversation(s) already classified)",
                raw_mirror_backfill.total_candidate_count
            ),
        ),
        "applied" => (
            "pass",
            format!(
                "Raw mirror backfill applied: captured {} live source file(s), linked {} existing raw manifest(s), left {} DB-only projection(s)",
                raw_mirror_backfill.captured_live_source_count,
                raw_mirror_backfill.existing_raw_manifest_link_count,
                raw_mirror_backfill.db_projection_only_count
            ),
        ),
        "planned" => (
            "warn",
            format!(
                "Raw mirror backfill available: {} live source file(s) can be captured and {} existing raw manifest(s) can be linked with --fix",
                raw_mirror_backfill.eligible_live_source_count,
                raw_mirror_backfill.existing_raw_manifest_link_count
            ),
        ),
        "partial" => (
            "warn",
            format!(
                "Raw mirror backfill partially applied with {} failure(s); no provider source files were modified",
                raw_mirror_backfill.capture_failure_count
            ),
        ),
        "blocked" => (
            "fail",
            raw_mirror_backfill
                .forensic_bundle
                .blocked_reason
                .as_ref()
                .map(|reason| {
                    format!(
                        "Raw mirror backfill blocked before mutation because forensic bundle capture failed: {reason}"
                    )
                })
                .unwrap_or_else(|| {
                    "Raw mirror backfill blocked before mutation because forensic bundle capture failed".to_string()
                }),
        ),
        _ => (
            "warn",
            format!(
                "Raw mirror backfill found {} DB-only projection(s), including {} missing upstream source(s); cass DB rows may be the sole remaining copy",
                raw_mirror_backfill.db_projection_only_count,
                raw_mirror_backfill.source_missing_count
            ),
        ),
    };
    checks.push(DoctorCheck {
        name: "raw_mirror_backfill".to_string(),
        status: backfill_check_status.to_string(),
        message: backfill_check_message,
        fix_available: backfill_fix_available,
        fix_applied: backfill_fix_applied,
    });
    let sole_copy_warnings = if recovery_evidence_collectors_complete {
        build_doctor_sole_copy_warnings(&raw_mirror_backfill)
    } else {
        Vec::new()
    };
    let coverage_summary = if let Some(reason) = recovery_evidence_deferred_reason.as_deref() {
        doctor_coverage_summary_deferred(
            db_conversations,
            db_messages,
            raw_mirror.summary.manifest_count,
            reason,
        )
    } else {
        build_doctor_coverage_summary(
            &source_inventory,
            &raw_mirror,
            &raw_mirror_backfill,
            &sole_copy_warnings,
        )
    };
    let mut coverage_risk = if let Some(reason) = recovery_evidence_deferred_reason.as_deref() {
        let mut risk = doctor_fast_coverage_risk_unchecked(db_path.exists());
        risk.recommended_action = format!(
            "{reason}. Run process-isolated archive coverage probes before archive repair."
        );
        risk
    } else {
        doctor_coverage_risk_summary(
            &coverage_summary,
            sole_copy_warnings.len(),
            !crate::raw_mirror::capture_enabled(),
        )
    };
    if let Some(reason) = recovery_evidence_deferred_reason.as_deref() {
        add_check!(
            "source_coverage",
            "warn",
            format!("Source coverage is unchecked; {reason}"),
            false
        );
    } else if !sole_copy_warnings.is_empty() {
        add_check!(
            "source_coverage",
            "warn",
            format!(
                "Source coverage ledger found {} sole-copy warning(s); live-source rebuilds that reduce coverage are refused",
                sole_copy_warnings.len()
            ),
            false
        );
    } else if coverage_summary.db_without_raw_mirror_count > 0
        && !crate::raw_mirror::capture_enabled()
    {
        // GH #506: say plainly what the operator's switch gives up; doctor
        // --fix cannot add raw copies while capture is off.
        add_check!(
            "source_coverage",
            "warn",
            format!(
                "Raw mirror capture is off (CASS_RAW_MIRROR): {} DB row(s) have no raw copy, so a session file its provider deletes survives only in the archive DB ({} raw mirror manifest(s) lack DB links)",
                coverage_summary.db_without_raw_mirror_count,
                coverage_summary.mirror_without_db_link_count
            ),
            false
        );
    } else if coverage_summary.db_without_raw_mirror_count > 0
        || coverage_summary.mirror_without_db_link_count > 0
    {
        add_check!(
            "source_coverage",
            "warn",
            format!(
                "Source coverage ledger is partial: {} DB row(s) lack raw-mirror db links and {} raw mirror manifest(s) lack DB links",
                coverage_summary.db_without_raw_mirror_count,
                coverage_summary.mirror_without_db_link_count
            ),
            coverage_summary.db_without_raw_mirror_count > 0
        );
    } else {
        add_check!(
            "source_coverage",
            "pass",
            format!(
                "Source coverage ledger OK ({} archive conversation(s), confidence_tier={})",
                coverage_summary.archive_conversation_count, coverage_summary.confidence_tier
            ),
            false
        );
    }
    let source_authority_started = Instant::now();
    let source_authority = {
        let report = build_doctor_source_authority_report(&db_path, &source_inventory, &raw_mirror);
        if let Some(reason) = recovery_evidence_deferred_reason.as_deref() {
            doctor_refuse_source_authority_when_collectors_deferred(
                report,
                reason,
                storage_integrity_failed,
            )
        } else {
            report
        }
    };
    let candidate_repair_authority_complete = recovery_evidence_collectors_complete
        || (storage_integrity_failed
            && source_authority
                .selected_authorities
                .iter()
                .any(|authority| {
                    matches!(
                        authority.authority,
                        DoctorSourceAuthorityKind::VerifiedRawMirror
                    ) && matches!(
                        authority.decision,
                        DoctorSourceAuthorityDecision::CandidateOnly
                    ) && matches!(
                        authority.checksum_status,
                        DoctorArtifactChecksumStatus::Matched
                    )
                }));
    doctor_push_timing_span(
        &mut timing_spans,
        "source_authority",
        "source_authority",
        "source_authority",
        source_authority_started,
        DOCTOR_SLOW_OPERATION_DEFAULT_THRESHOLD_MS,
        vec!["source-authority matrix selected or rejected repair authorities".to_string()],
    );
    if let Some(reason) = recovery_evidence_deferred_reason.as_deref() {
        if candidate_repair_authority_complete {
            add_check!(
                "source_authority",
                "warn",
                format!(
                    "Archive-wide coverage authority is deferred, but checksum-verified raw mirror evidence may seed an isolated candidate after affirmative canonical corruption; promotion remains independently gated; {reason}"
                ),
                true
            );
        } else {
            add_check!(
                "source_authority",
                "warn",
                format!("No archive repair authority was selected; {reason}"),
                false
            );
        }
    }
    let candidate_staging_started = Instant::now();
    // Set when this run built an isolated reconstruct candidate, which by
    // design leaves every live asset untouched (see the post-repair probes).
    let mut isolated_candidate_built = false;
    let mut candidate_staging =
        collect_doctor_candidate_staging_report(&data_dir, &db_path, &index_path);
    if candidate_repair_authority_complete
        && candidate_staging.total_candidate_count == 0
        && doctor_candidate_build_should_run(
            fix_can_mutate,
            archive_queryable_for_non_destructive_derived_rebuild,
            needs_rebuild,
            &coverage_risk,
            &source_authority,
        )
    {
        let build_report = build_doctor_reconstruct_candidate(
            &data_dir,
            &db_path,
            &index_path,
            &raw_mirror,
            &source_authority,
            &coverage_summary,
        );
        if build_report.status == "completed" {
            auto_fix_applied = true;
            isolated_candidate_built = true;
            if let Some(candidate_id) = build_report.candidate_id.as_deref() {
                auto_fix_actions.push(format!(
                    "Built isolated reconstruct candidate {candidate_id} without changing live archive inventory"
                ));
            }
        }
        let latest_build = build_report;
        candidate_staging =
            collect_doctor_candidate_staging_report(&data_dir, &db_path, &index_path);
        candidate_staging.latest_build = Some(latest_build);
    }
    doctor_push_timing_span(
        &mut timing_spans,
        "candidate_staging",
        "candidate_staging",
        "candidate_build",
        candidate_staging_started,
        DOCTOR_SLOW_OPERATION_DEFAULT_THRESHOLD_MS,
        vec!["candidate staging was summarized and any eligible isolated candidate build was recorded".to_string()],
    );
    let (candidate_check_status, candidate_check_message, candidate_fix_applied) = if let Some(
        build,
    ) =
        candidate_staging.latest_build.as_ref()
    {
        if build.status == "completed" {
            (
                "pass",
                format!(
                    "Candidate staging built {} artifact(s), {} checksum(s), live archive inventory unchanged={}",
                    build.artifact_count, build.checksum_count, build.live_inventory_unchanged
                ),
                true,
            )
        } else {
            (
                "fail",
                format!(
                    "Candidate staging build blocked: {}",
                    build.warnings.join("; ")
                ),
                false,
            )
        }
    } else if candidate_staging.orphaned_candidate_count > 0
        || candidate_staging.interrupted_candidate_count > 0
    {
        (
            "warn",
            format!(
                "Candidate staging found {} orphaned and {} interrupted candidate artifact(s); doctor will not delete them automatically",
                candidate_staging.orphaned_candidate_count,
                candidate_staging.interrupted_candidate_count
            ),
            false,
        )
    } else {
        (
            "pass",
            format!(
                "Candidate staging OK ({} candidate artifact(s), status={})",
                candidate_staging.total_candidate_count, candidate_staging.status
            ),
            false,
        )
    };
    let candidate_buildable = candidate_staging.total_candidate_count == 0
        && candidate_repair_authority_complete
        && doctor_candidate_build_should_run(
            !fix && operation_state.mutating_doctor_allowed,
            archive_queryable_for_non_destructive_derived_rebuild,
            needs_rebuild,
            &coverage_risk,
            &source_authority,
        );
    checks.push(DoctorCheck {
        name: "candidate_staging".to_string(),
        status: candidate_check_status.to_string(),
        message: candidate_check_message,
        fix_available: candidate_buildable,
        fix_applied: candidate_fix_applied,
    });
    if let Some(reason) = recovery_evidence_deferred_reason.as_deref() {
        coverage_risk.recommended_action = doctor_deferred_archive_recommended_action(
            reason,
            candidate_repair_authority_complete && candidate_staging.completed_candidate_count > 0,
            candidate_buildable,
            storage_busy_or_locked,
            storage_probe_timed_out,
            storage_integrity_skipped_reason.is_some(),
        );
    }
    if let Some(build) = candidate_staging.latest_build.as_ref()
        && !build.coverage_gate.promote_allowed
    {
        checks.push(DoctorCheck {
            name: "coverage_comparison_gate".to_string(),
            status: "fail".to_string(),
            message: format!(
                "Candidate promotion refused because coverage would decrease: {}",
                build.coverage_gate.blocking_reasons.join("; ")
            ),
            fix_available: false,
            fix_applied: false,
        });
    }
    let suppress_legacy_rebuild_for_verified_candidate = needs_rebuild
        && fix_can_mutate
        && !archive_queryable_for_non_destructive_derived_rebuild
        && candidate_staging
            .latest_build
            .as_ref()
            .is_some_and(|build| {
                build.status == "completed"
                    && build.confidence == "verified_raw_mirror_reconstruction"
                    && build.candidate_message_count.unwrap_or(0) > 0
            });
    if suppress_legacy_rebuild_for_verified_candidate {
        needs_rebuild = false;
        checks.push(DoctorCheck {
            name: "legacy_archive_rebuild".to_string(),
            status: "warn".to_string(),
            message: "Skipped legacy source rebuild because an isolated verified raw-mirror reconstruct candidate was built; live archive bytes were preserved for explicit promotion/restore handling".to_string(),
            fix_available: true,
            fix_applied: false,
        });
    }

    let semantic_probe_started = Instant::now();
    let readiness_snapshot = state_meta_json_inner(
        &data_dir,
        &db_path,
        DEFAULT_STALE_THRESHOLD_SECS,
        false,
        Some(false),
        true,
        true,
        // passive_db_probe: doctor's readiness probe must stay read-only.
        true,
    );
    doctor_push_timing_span(
        &mut timing_spans,
        "semantic_probe",
        "semantic_model",
        "semantic_probe",
        semantic_probe_started,
        DOCTOR_SLOW_OPERATION_DEFAULT_THRESHOLD_MS,
        vec!["semantic readiness was inspected without downloading models".to_string()],
    );
    let semantic_probe_duration_ms = semantic_probe_started.elapsed().as_millis() as u64;
    let fallback_mode = doctor_fallback_mode_from_state(&readiness_snapshot);
    let derived_semantic_assets = doctor_build_derived_semantic_asset_report(
        &data_dir,
        &readiness_snapshot,
        archive_queryable_for_non_destructive_derived_rebuild,
        semantic_probe_duration_ms,
    );
    let semantic_status = readiness_snapshot
        .pointer("/semantic/status")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    let semantic_availability = readiness_snapshot
        .pointer("/semantic/availability")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    add_check!(
        "semantic_model",
        derived_semantic_assets.doctor_check_status.clone(),
        format!(
            "Semantic/model readiness status={semantic_status}, availability={semantic_availability}, fallback_mode={fallback_mode}, asset_class={}, safe_to_rebuild={}, network_allowed=false, blocks_archive_recovery=false",
            doctor_serde_label(derived_semantic_assets.asset_class),
            derived_semantic_assets.safe_to_rebuild
        ),
        derived_semantic_assets.safe_to_rebuild
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
    let lexical_ws = DoctorLexicalWorkspaceReport {
        archive_queryable_for_non_destructive_derived_rebuild,
        canonical_candidate_replacement_authorized,
        storage_busy_or_locked,
        archive_wide_collectors_deferred_reason,
        archive_wide_collectors_complete,
        storage_integrity_report,
        config_exclusion_risks,
        source_inventory,
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
    (state, lexical_ws, evidence)
}
