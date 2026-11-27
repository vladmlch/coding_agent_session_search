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

pub(crate) fn doctor_severity_rank(severity: DoctorSeverity) -> u8 {
    match severity {
        DoctorSeverity::Info => 0,
        DoctorSeverity::Warn => 1,
        DoctorSeverity::Error => 2,
    }
}

pub(crate) fn doctor_max_severity(left: DoctorSeverity, right: DoctorSeverity) -> DoctorSeverity {
    if doctor_severity_rank(right) > doctor_severity_rank(left) {
        right
    } else {
        left
    }
}

pub(crate) fn doctor_max_data_loss_risk(
    left: DoctorDataLossRisk,
    right: DoctorDataLossRisk,
) -> DoctorDataLossRisk {
    if doctor_data_loss_risk_rank(right) > doctor_data_loss_risk_rank(left) {
        right
    } else {
        left
    }
}

pub(crate) fn doctor_add_unique_string(values: &mut Vec<String>, value: impl Into<String>) {
    let value = value.into();
    if !value.is_empty() && !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
}

pub(crate) fn doctor_add_unique_asset_class(
    values: &mut Vec<DoctorAssetClass>,
    value: DoctorAssetClass,
) {
    if !values.contains(&value) {
        values.push(value);
    }
}

pub(crate) fn doctor_incident_kind_for_check(
    check: &DoctorCheckReport,
    coverage_risk: &DoctorCoverageRiskSummary,
    candidate_staging: &DoctorCandidateStagingReport,
    fallback_mode: &str,
    candidate_promotion_report: Option<&DoctorCandidatePromotionReport>,
) -> DoctorIncidentRootCauseKind {
    match check.anomaly_class {
        DoctorAnomaly::Healthy => DoctorIncidentRootCauseKind::Unknown,
        DoctorAnomaly::DerivedLexicalStale | DoctorAnomaly::DegradedDerivedAssets => {
            if candidate_promotion_report.is_some_and(|report| {
                report.status == "applied"
                    && (report.derived_lexical_rebuild_required
                        || report.derived_semantic_rebuild_required)
            }) {
                DoctorIncidentRootCauseKind::DerivedIndexStaleAfterDbPromotion
            } else {
                DoctorIncidentRootCauseKind::DerivedIndexStale
            }
        }
        DoctorAnomaly::DerivedSemanticStale => {
            if fallback_mode == "lexical" {
                DoctorIncidentRootCauseKind::SemanticModelMissingLexicalFallback
            } else {
                DoctorIncidentRootCauseKind::DerivedIndexStale
            }
        }
        DoctorAnomaly::UpstreamSourcePruned => {
            if coverage_risk.raw_mirror_db_link_count > 0 {
                DoctorIncidentRootCauseKind::SourcePrunedWithMirrorIntact
            } else {
                DoctorIncidentRootCauseKind::MirrorMissingWithDbSoleCopy
            }
        }
        DoctorAnomaly::RawMirrorMissing | DoctorAnomaly::RawMirrorBehindSource => {
            DoctorIncidentRootCauseKind::MirrorMissingWithDbSoleCopy
        }
        DoctorAnomaly::ArchiveDbCorrupt
        | DoctorAnomaly::ArchiveDbUnreadable
        | DoctorAnomaly::DegradedArchiveRisk => {
            if candidate_staging.completed_candidate_count > 0 {
                DoctorIncidentRootCauseKind::ArchiveDbUnreadableWithValidCandidate
            } else {
                DoctorIncidentRootCauseKind::ArchiveDbUnreadable
            }
        }
        DoctorAnomaly::LockContention => DoctorIncidentRootCauseKind::ActiveLockBlockingRepair,
        DoctorAnomaly::InterruptedRepair => DoctorIncidentRootCauseKind::InterruptedRepairState,
        DoctorAnomaly::StoragePressure | DoctorAnomaly::FullRebuildHeadroom => {
            DoctorIncidentRootCauseKind::StoragePressureDerivedCleanupAvailable
        }
        DoctorAnomaly::ConfigExclusionRisk => DoctorIncidentRootCauseKind::BackupExclusionRisk,
        DoctorAnomaly::BackupUnverified | DoctorAnomaly::BackupStale => {
            DoctorIncidentRootCauseKind::BackupUnverified
        }
        DoctorAnomaly::RepairPreviouslyFailed => DoctorIncidentRootCauseKind::PreviousRepairFailed,
        DoctorAnomaly::SourceAuthorityUnsafe | DoctorAnomaly::RepairBlocked => {
            DoctorIncidentRootCauseKind::Unknown
        }
        DoctorAnomaly::PrivacyRedactionRequired => DoctorIncidentRootCauseKind::Unknown,
    }
}

pub(crate) fn doctor_incident_summary(kind: DoctorIncidentRootCauseKind) -> &'static str {
    match kind {
        DoctorIncidentRootCauseKind::DerivedIndexStale => {
            "Derived search assets need rebuild or cleanup; archive evidence is not the failing asset."
        }
        DoctorIncidentRootCauseKind::DerivedIndexStaleAfterDbPromotion => {
            "A promoted archive generation needs derived search assets rebuilt or marked as lexical fallback."
        }
        DoctorIncidentRootCauseKind::SemanticModelMissingLexicalFallback => {
            "Semantic assets are unavailable, but lexical fallback remains the safe search mode."
        }
        DoctorIncidentRootCauseKind::SourcePrunedWithMirrorIntact => {
            "Upstream source logs are missing, but cass has verified raw mirror evidence."
        }
        DoctorIncidentRootCauseKind::MirrorMissingWithDbSoleCopy => {
            "Some archive rows lack visible upstream or raw-mirror evidence; preserve the cass archive before repair."
        }
        DoctorIncidentRootCauseKind::ArchiveDbUnreadableWithValidCandidate => {
            "The archive DB needs candidate-based recovery; a verified candidate is available or being staged."
        }
        DoctorIncidentRootCauseKind::ArchiveDbUnreadable => {
            "The archive DB is unreadable or corrupt; preserve sidecars and plan recovery before mutation."
        }
        DoctorIncidentRootCauseKind::ActiveLockBlockingRepair => {
            "An active or uncertain lock is blocking mutating doctor work."
        }
        DoctorIncidentRootCauseKind::InterruptedRepairState => {
            "An interrupted doctor operation left artifacts that need inspection before retry."
        }
        DoctorIncidentRootCauseKind::StoragePressureDerivedCleanupAvailable => {
            "Low free space can block safe doctor work; reclaim only approved derived bytes or relocate the archive."
        }
        DoctorIncidentRootCauseKind::BackupExclusionRisk => {
            "Backup or config exclusions may leave archive evidence unprotected."
        }
        DoctorIncidentRootCauseKind::BackupUnverified => {
            "Backups need verification before restore, repair, or cleanup decisions."
        }
        DoctorIncidentRootCauseKind::PreviousRepairFailed => {
            "A previous repair failed verification and must be inspected before repeating repair."
        }
        DoctorIncidentRootCauseKind::Unknown => {
            "Doctor found related symptoms but cannot prove one root cause yet."
        }
    }
}

pub(crate) fn doctor_incident_confidence(
    kind: DoctorIncidentRootCauseKind,
    coverage_risk: &DoctorCoverageRiskSummary,
) -> &'static str {
    if kind == DoctorIncidentRootCauseKind::Unknown
        || coverage_risk.confidence_tier == "unchecked"
        || coverage_risk.status == "not_initialized"
    {
        "unknown"
    } else if coverage_risk.confidence_tier.contains("verified")
        || matches!(
            kind,
            DoctorIncidentRootCauseKind::SourcePrunedWithMirrorIntact
                | DoctorIncidentRootCauseKind::ArchiveDbUnreadableWithValidCandidate
        )
    {
        "high"
    } else {
        "medium"
    }
}

pub(crate) fn doctor_incident_blocked_actions(
    check: &DoctorCheckReport,
    kind: DoctorIncidentRootCauseKind,
    blocked_actions: &mut Vec<String>,
) {
    if !check.safe_for_auto_repair && check.status != "pass" {
        doctor_add_unique_string(blocked_actions, "auto-repair");
    }
    if matches!(
        check.data_loss_risk,
        DoctorDataLossRisk::Medium | DoctorDataLossRisk::High | DoctorDataLossRisk::Unknown
    ) {
        doctor_add_unique_string(blocked_actions, "source-only-rebuild");
    }
    match kind {
        DoctorIncidentRootCauseKind::ActiveLockBlockingRepair => {
            doctor_add_unique_string(blocked_actions, "mutating-doctor-repair");
        }
        DoctorIncidentRootCauseKind::StoragePressureDerivedCleanupAvailable => {
            doctor_add_unique_string(blocked_actions, "archive-evidence-cleanup");
        }
        DoctorIncidentRootCauseKind::PreviousRepairFailed => {
            doctor_add_unique_string(blocked_actions, "repeat-repair-without-marker-inspection");
        }
        DoctorIncidentRootCauseKind::ArchiveDbUnreadable
        | DoctorIncidentRootCauseKind::ArchiveDbUnreadableWithValidCandidate
        | DoctorIncidentRootCauseKind::MirrorMissingWithDbSoleCopy => {
            doctor_add_unique_string(blocked_actions, "coverage-reducing-repair");
        }
        _ => {}
    }
}

pub(crate) fn doctor_incident_redacted_paths(
    kind: DoctorIncidentRootCauseKind,
    operation_state: &DoctorOperationStateReport,
    candidate_staging: &DoctorCandidateStagingReport,
) -> Vec<String> {
    let mut paths = Vec::new();
    if matches!(
        kind,
        DoctorIncidentRootCauseKind::ActiveLockBlockingRepair
            | DoctorIncidentRootCauseKind::InterruptedRepairState
    ) {
        for owner in &operation_state.owners {
            doctor_add_unique_string(&mut paths, owner.redacted_lock_path.clone());
        }
        for interrupted in &operation_state.interrupted_states {
            doctor_add_unique_string(&mut paths, interrupted.redacted_path.clone());
        }
    }
    if matches!(
        kind,
        DoctorIncidentRootCauseKind::ArchiveDbUnreadableWithValidCandidate
            | DoctorIncidentRootCauseKind::InterruptedRepairState
    ) {
        doctor_add_unique_string(&mut paths, candidate_staging.redacted_root_path.clone());
        if let Some(build) = candidate_staging.latest_build.as_ref() {
            if let Some(path) = build.redacted_path.as_ref() {
                doctor_add_unique_string(&mut paths, path.clone());
            }
            if let Some(path) = build.redacted_manifest_path.as_ref() {
                doctor_add_unique_string(&mut paths, path.clone());
            }
        }
        for candidate in &candidate_staging.candidates {
            doctor_add_unique_string(&mut paths, candidate.redacted_path.clone());
            if let Some(path) = candidate.redacted_manifest_path.as_ref() {
                doctor_add_unique_string(&mut paths, path.clone());
            }
        }
    }
    paths.sort();
    paths
}

pub(crate) fn build_doctor_root_cause_incidents(
    checks: &[DoctorCheckReport],
    coverage_risk: &DoctorCoverageRiskSummary,
    operation_state: &DoctorOperationStateReport,
    candidate_staging: &DoctorCandidateStagingReport,
    fallback_mode: &str,
    candidate_promotion_report: Option<&DoctorCandidatePromotionReport>,
) -> Vec<DoctorRootCauseIncident> {
    struct IncidentAccumulator {
        kind: DoctorIncidentRootCauseKind,
        severity: DoctorSeverity,
        affected_asset_classes: Vec<DoctorAssetClass>,
        archive_risk_level: DoctorDataLossRisk,
        derived_risk_level: DoctorDataLossRisk,
        evidence_check_ids: Vec<String>,
        blocked_actions: Vec<String>,
        safe_next_actions: Vec<String>,
        stale_or_unknown_fields: Vec<String>,
    }

    let mut groups: BTreeMap<String, IncidentAccumulator> = BTreeMap::new();
    for check in checks.iter().filter(|check| check.status != "pass") {
        let kind = doctor_incident_kind_for_check(
            check,
            coverage_risk,
            candidate_staging,
            fallback_mode,
            candidate_promotion_report,
        );
        let key = doctor_serde_label(kind);
        let entry = groups.entry(key).or_insert_with(|| IncidentAccumulator {
            kind,
            severity: DoctorSeverity::Info,
            affected_asset_classes: Vec::new(),
            archive_risk_level: DoctorDataLossRisk::None,
            derived_risk_level: DoctorDataLossRisk::None,
            evidence_check_ids: Vec::new(),
            blocked_actions: Vec::new(),
            safe_next_actions: Vec::new(),
            stale_or_unknown_fields: Vec::new(),
        });

        entry.severity = doctor_max_severity(entry.severity, check.severity);
        doctor_add_unique_asset_class(
            &mut entry.affected_asset_classes,
            check.affected_asset_class,
        );
        doctor_add_unique_string(&mut entry.evidence_check_ids, check.name.clone());
        if check.recommended_action != "none" {
            doctor_add_unique_string(&mut entry.safe_next_actions, check.recommended_action);
        }
        doctor_incident_blocked_actions(check, kind, &mut entry.blocked_actions);

        let asset_policy = doctor_asset_policy(check.affected_asset_class);
        let derived_risk = if asset_policy.derived
            && check.data_loss_risk == DoctorDataLossRisk::None
            && check.status != "pass"
        {
            DoctorDataLossRisk::Low
        } else {
            check.data_loss_risk
        };
        if asset_policy.derived {
            entry.derived_risk_level =
                doctor_max_data_loss_risk(entry.derived_risk_level, derived_risk);
        } else {
            entry.archive_risk_level =
                doctor_max_data_loss_risk(entry.archive_risk_level, check.data_loss_risk);
        }
        if check.data_loss_risk == DoctorDataLossRisk::Unknown
            || kind == DoctorIncidentRootCauseKind::Unknown
        {
            doctor_add_unique_string(&mut entry.stale_or_unknown_fields, "root_cause_kind");
        }
    }

    if coverage_risk.confidence_tier == "unchecked" {
        for entry in groups.values_mut() {
            doctor_add_unique_string(&mut entry.stale_or_unknown_fields, "coverage_risk");
        }
    }
    if operation_state.mutation_blocked_reason.is_some() {
        for entry in groups.values_mut().filter(|entry| {
            entry.kind == DoctorIncidentRootCauseKind::ActiveLockBlockingRepair
                || entry.kind == DoctorIncidentRootCauseKind::InterruptedRepairState
        }) {
            doctor_add_unique_string(
                &mut entry.safe_next_actions,
                operation_state.next_action.clone(),
            );
        }
    }

    let mut incidents = groups
        .into_values()
        .map(|mut entry| {
            entry
                .affected_asset_classes
                .sort_by_key(|class| doctor_serde_label(*class));
            entry.evidence_check_ids.sort();
            entry.blocked_actions.sort();
            entry.safe_next_actions.sort();
            entry.stale_or_unknown_fields.sort();
            let redacted_evidence_paths =
                doctor_incident_redacted_paths(entry.kind, operation_state, candidate_staging);
            let root_cause_label = doctor_serde_label(entry.kind);
            let incident_id = doctor_canonical_blake3(
                "doctor-incident-v1",
                serde_json::json!({
                    "kind": root_cause_label,
                    "evidence": entry.evidence_check_ids.clone(),
                    "assets": entry
                        .affected_asset_classes
                        .iter()
                        .map(|class| doctor_serde_label(*class))
                        .collect::<Vec<_>>(),
                }),
            );
            DoctorRootCauseIncident {
                schema_version: 1,
                incident_id,
                root_cause_kind: entry.kind,
                severity: entry.severity,
                affected_asset_classes: entry.affected_asset_classes,
                archive_risk_level: entry.archive_risk_level,
                derived_risk_level: entry.derived_risk_level,
                confidence: doctor_incident_confidence(entry.kind, coverage_risk).to_string(),
                evidence_check_ids: entry.evidence_check_ids,
                blocked_actions: entry.blocked_actions,
                safe_next_actions: entry.safe_next_actions,
                stale_or_unknown_fields: entry.stale_or_unknown_fields,
                redacted_evidence_paths,
                summary: doctor_incident_summary(entry.kind).to_string(),
            }
        })
        .collect::<Vec<_>>();

    incidents.sort_by(|left, right| {
        doctor_severity_rank(right.severity)
            .cmp(&doctor_severity_rank(left.severity))
            .then_with(|| {
                doctor_data_loss_risk_rank(right.archive_risk_level)
                    .cmp(&doctor_data_loss_risk_rank(left.archive_risk_level))
            })
            .then_with(|| {
                doctor_data_loss_risk_rank(right.derived_risk_level)
                    .cmp(&doctor_data_loss_risk_rank(left.derived_risk_level))
            })
            .then_with(|| {
                doctor_serde_label(left.root_cause_kind)
                    .cmp(&doctor_serde_label(right.root_cause_kind))
            })
            .then_with(|| left.incident_id.cmp(&right.incident_id))
    });
    incidents
}

pub(crate) fn doctor_health_class_for_checks(checks: &[DoctorCheckReport]) -> DoctorHealth {
    if checks
        .iter()
        .any(|check| check.health_class == DoctorHealth::RepairPreviouslyFailed)
    {
        return DoctorHealth::RepairPreviouslyFailed;
    }
    if checks
        .iter()
        .any(|check| check.health_class == DoctorHealth::DegradedArchiveRisk)
    {
        return DoctorHealth::DegradedArchiveRisk;
    }
    if checks
        .iter()
        .any(|check| check.health_class == DoctorHealth::SourceAuthorityUnsafe)
    {
        return DoctorHealth::SourceAuthorityUnsafe;
    }
    if checks
        .iter()
        .any(|check| check.health_class == DoctorHealth::RepairBlocked)
    {
        return DoctorHealth::RepairBlocked;
    }
    if checks
        .iter()
        .any(|check| check.health_class == DoctorHealth::DegradedDerivedAssets)
    {
        return DoctorHealth::DegradedDerivedAssets;
    }
    DoctorHealth::Healthy
}

pub(crate) fn doctor_data_loss_risk_rank(risk: DoctorDataLossRisk) -> u8 {
    match risk {
        DoctorDataLossRisk::None => 0,
        DoctorDataLossRisk::Low => 1,
        DoctorDataLossRisk::Medium => 2,
        DoctorDataLossRisk::High => 3,
        DoctorDataLossRisk::Unknown => 4,
    }
}

pub(crate) fn doctor_highest_data_loss_risk(checks: &[DoctorCheckReport]) -> DoctorDataLossRisk {
    checks
        .iter()
        .map(|check| check.data_loss_risk)
        .max_by_key(|risk| doctor_data_loss_risk_rank(*risk))
        .unwrap_or(DoctorDataLossRisk::None)
}

pub(crate) fn doctor_top_level_operation_outcome(
    checks: &[DoctorCheckReport],
    fix_requested: bool,
    issues_found: usize,
    issues_fixed: usize,
    not_initialized: bool,
    post_repair_probes: &DoctorPostRepairProbeSuiteReport,
    cleanup_apply_result: Option<&DiagCleanupApplyResult>,
) -> DoctorOperationOutcomeReport {
    if fix_requested && post_repair_probes.blocks_success {
        let failed = post_repair_probes
            .probes
            .iter()
            .filter(|probe| probe.status == "fail")
            .map(|probe| {
                format!(
                    "{}: {}",
                    probe.probe_id,
                    probe
                        .failure_reason
                        .as_deref()
                        .unwrap_or("post-repair probe failed")
                )
            })
            .collect::<Vec<_>>();
        return doctor_operation_outcome_with_details(
            DoctorOperationOutcomeKind::VerificationFailed,
            format!(
                "post-repair verification failed after mutation: {}",
                failed.join("; ")
            ),
            "preserved repair artifacts and recorded post-repair probe failure context".to_string(),
            "doctor did not report repair success because the repaired state was not proven usable"
                .to_string(),
            DoctorDataLossRisk::Medium,
            Some("cass doctor --json".to_string()),
            post_repair_probes.redacted_manifest_path.clone(),
        );
    }

    if let Some(result) = cleanup_apply_result {
        return result.operation_outcome.clone();
    }

    let data_loss_risk = doctor_highest_data_loss_risk(checks);
    if fix_requested
        && checks
            .iter()
            .any(|check| check.anomaly_class == DoctorAnomaly::LockContention)
    {
        return doctor_operation_outcome_with_details(
            DoctorOperationOutcomeKind::RepairBlocked,
            "doctor repair was blocked by an active or unverifiable operation lock".to_string(),
            "recorded lock contention without attempting mutation".to_string(),
            "no repair, rebuild, promotion, restore, or cleanup action was attempted".to_string(),
            data_loss_risk,
            Some("cass doctor --json".to_string()),
            None,
        );
    }

    if fix_requested
        && checks
            .iter()
            .any(|check| check.anomaly_class == DoctorAnomaly::InterruptedRepair)
    {
        return doctor_operation_outcome_with_details(
            DoctorOperationOutcomeKind::RepairBlocked,
            "doctor repair was blocked by interrupted operation artifacts that require inspection"
                .to_string(),
            "reported interrupted operation state without attempting mutation".to_string(),
            "no repair, rebuild, promotion, restore, or cleanup action was attempted".to_string(),
            data_loss_risk,
            Some("cass doctor --json".to_string()),
            None,
        );
    }

    if fix_requested
        && checks
            .iter()
            .any(|check| check.name == "repair_failure_marker" && check.status == "fail")
    {
        return doctor_operation_outcome_with_details(
            DoctorOperationOutcomeKind::RepairRefused,
            "previous doctor repair failure marker blocks repeated mutation".to_string(),
            "reported the durable failure marker and preserved prior evidence".to_string(),
            "no repair, rebuild, promotion, restore, or cleanup action was attempted".to_string(),
            data_loss_risk,
            Some("cass doctor --json".to_string()),
            None,
        );
    }

    if fix_requested
        && checks
            .iter()
            .any(|check| check.name == "repair_plan_approval" && check.status == "fail")
    {
        return doctor_operation_outcome_with_details(
            DoctorOperationOutcomeKind::RepairRefused,
            "repair plan approval failed or drifted before mutation".to_string(),
            "reported the refused fingerprint approval without mutating archive or derived assets"
                .to_string(),
            "no repair, rebuild, promotion, restore, or cleanup action was attempted".to_string(),
            data_loss_risk,
            Some("cass doctor repair --dry-run --json".to_string()),
            None,
        );
    }

    if fix_requested && issues_fixed > 0 {
        let (kind, reason, action_taken, action_not_taken) = if issues_found == 0 {
            (
                DoctorOperationOutcomeKind::Fixed,
                format!("doctor fixed {issues_fixed} issue(s)"),
                "applied every safe repair that was selected".to_string(),
                "no selected safe repair remained blocked".to_string(),
            )
        } else {
            (
                DoctorOperationOutcomeKind::PartiallyFixed,
                format!(
                    "doctor fixed {issues_fixed} issue(s) and left {issues_found} issue(s) for review"
                ),
                "applied the safe repairs that passed all gates".to_string(),
                "left remaining blocked or unsafe issues for operator review".to_string(),
            )
        };
        return doctor_operation_outcome_with_details(
            kind,
            reason,
            action_taken,
            action_not_taken,
            data_loss_risk,
            Some("cass doctor --json".to_string()),
            None,
        );
    }

    if issues_found == 0 {
        return doctor_operation_outcome_with_details(
            DoctorOperationOutcomeKind::OkNoActionNeeded,
            "all doctor checks passed".to_string(),
            "completed diagnostic checks".to_string(),
            "no repair or cleanup was needed".to_string(),
            DoctorDataLossRisk::None,
            None,
            None,
        );
    }

    if !fix_requested {
        // Same routing as the safe-auto-run report's next_exact_command
        // (#374): `--fix` only runs derived cleanup, so a failing archive or
        // source-authority check must not be told to run it. The owner's
        // leaked-page archive got `cass doctor --fix` here while its own
        // check said "reconstruct from verified authority" (2l1b0.73).
        let next_command = if not_initialized {
            Some("cass index --full".to_string())
        } else {
            Some(doctor_read_only_next_command(checks))
        };
        return doctor_operation_outcome_with_details(
            DoctorOperationOutcomeKind::OkReadOnlyDiagnosed,
            format!("doctor diagnosed {issues_found} issue(s) in read-only mode"),
            "completed diagnostic checks without mutation".to_string(),
            "no repair was attempted because --fix was not requested".to_string(),
            data_loss_risk,
            next_command,
            None,
        );
    }

    if checks.iter().any(|check| {
        check.anomaly_class == DoctorAnomaly::RepairPreviouslyFailed && check.status == "fail"
    }) {
        return doctor_operation_outcome_with_details(
            DoctorOperationOutcomeKind::VerificationFailed,
            "previous repair verification failed and must be inspected first".to_string(),
            "refused automatic repeat repair".to_string(),
            "no repair was applied without reviewing the prior failure marker".to_string(),
            data_loss_risk,
            Some("cass doctor --json".to_string()),
            None,
        );
    }

    doctor_operation_outcome_with_details(
        DoctorOperationOutcomeKind::AutoRunSkipped,
        format!("doctor found {issues_found} issue(s), but none were eligible for safe auto-run"),
        "evaluated automatic repair eligibility".to_string(),
        "no repair was applied automatically".to_string(),
        data_loss_risk,
        Some("cass doctor --json".to_string()),
        None,
    )
}

#[cfg(test)]
pub(crate) const DOCTOR_ASSET_ALL_OPERATIONS: &[DoctorAssetOperation] = &[
    DoctorAssetOperation::Read,
    DoctorAssetOperation::Backup,
    DoctorAssetOperation::Copy,
    DoctorAssetOperation::MoveQuarantine,
    DoctorAssetOperation::Promote,
    DoctorAssetOperation::Rebuild,
    DoctorAssetOperation::Normalize,
    DoctorAssetOperation::PruneReclaim,
    DoctorAssetOperation::Restore,
    DoctorAssetOperation::Redact,
    DoctorAssetOperation::Export,
    DoctorAssetOperation::SupportBundle,
];

pub(crate) const DOCTOR_ASSET_PRECIOUS_EVIDENCE_OPS: &[DoctorAssetOperation] = &[
    DoctorAssetOperation::Read,
    DoctorAssetOperation::Backup,
    DoctorAssetOperation::Copy,
    DoctorAssetOperation::MoveQuarantine,
    DoctorAssetOperation::Normalize,
    DoctorAssetOperation::Redact,
    DoctorAssetOperation::Export,
    DoctorAssetOperation::SupportBundle,
];

pub(crate) const DOCTOR_ASSET_ARCHIVE_OPS: &[DoctorAssetOperation] = &[
    DoctorAssetOperation::Read,
    DoctorAssetOperation::Backup,
    DoctorAssetOperation::Copy,
    DoctorAssetOperation::Normalize,
    DoctorAssetOperation::Promote,
    DoctorAssetOperation::Rebuild,
    DoctorAssetOperation::Restore,
    DoctorAssetOperation::Redact,
    DoctorAssetOperation::Export,
    DoctorAssetOperation::SupportBundle,
];

pub(crate) const DOCTOR_ASSET_SIDECAR_OPS: &[DoctorAssetOperation] = &[
    DoctorAssetOperation::Read,
    DoctorAssetOperation::Backup,
    DoctorAssetOperation::Copy,
    DoctorAssetOperation::Promote,
    DoctorAssetOperation::Rebuild,
    DoctorAssetOperation::Restore,
    DoctorAssetOperation::SupportBundle,
];

pub(crate) const DOCTOR_ASSET_USER_STATE_OPS: &[DoctorAssetOperation] = &[
    DoctorAssetOperation::Read,
    DoctorAssetOperation::Backup,
    DoctorAssetOperation::Copy,
    DoctorAssetOperation::Restore,
    DoctorAssetOperation::Redact,
    DoctorAssetOperation::Export,
    DoctorAssetOperation::SupportBundle,
];

pub(crate) const DOCTOR_ASSET_AUDIT_OPS: &[DoctorAssetOperation] = &[
    DoctorAssetOperation::Read,
    DoctorAssetOperation::Backup,
    DoctorAssetOperation::Copy,
    DoctorAssetOperation::Redact,
    DoctorAssetOperation::Export,
    DoctorAssetOperation::SupportBundle,
];

pub(crate) const DOCTOR_ASSET_BACKUP_OPS: &[DoctorAssetOperation] = &[
    DoctorAssetOperation::Read,
    DoctorAssetOperation::Copy,
    DoctorAssetOperation::Restore,
    DoctorAssetOperation::Redact,
    DoctorAssetOperation::Export,
    DoctorAssetOperation::SupportBundle,
];

pub(crate) const DOCTOR_ASSET_DERIVED_REBUILDABLE_OPS: &[DoctorAssetOperation] = &[
    DoctorAssetOperation::Read,
    DoctorAssetOperation::Backup,
    DoctorAssetOperation::Copy,
    DoctorAssetOperation::MoveQuarantine,
    DoctorAssetOperation::Promote,
    DoctorAssetOperation::Rebuild,
    DoctorAssetOperation::SupportBundle,
];

pub(crate) const DOCTOR_ASSET_DERIVED_RECLAIMABLE_OPS: &[DoctorAssetOperation] = &[
    DoctorAssetOperation::Read,
    DoctorAssetOperation::Backup,
    DoctorAssetOperation::Copy,
    DoctorAssetOperation::MoveQuarantine,
    DoctorAssetOperation::Promote,
    DoctorAssetOperation::Rebuild,
    DoctorAssetOperation::PruneReclaim,
    DoctorAssetOperation::SupportBundle,
];

pub(crate) const DOCTOR_ASSET_DIAGNOSTIC_OPS: &[DoctorAssetOperation] = &[
    DoctorAssetOperation::Read,
    DoctorAssetOperation::Backup,
    DoctorAssetOperation::Copy,
    DoctorAssetOperation::Redact,
    DoctorAssetOperation::Export,
    DoctorAssetOperation::SupportBundle,
];

pub(crate) const DOCTOR_ASSET_EXTERNAL_SOURCE_OPS: &[DoctorAssetOperation] = &[
    DoctorAssetOperation::Read,
    DoctorAssetOperation::Copy,
    DoctorAssetOperation::SupportBundle,
];

pub(crate) const DOCTOR_ASSET_NO_OPS: &[DoctorAssetOperation] = &[];

pub(crate) const DOCTOR_ASSET_POLICY_TABLE: &[DoctorAssetPolicy] = &[
    DoctorAssetPolicy {
        asset_class: DoctorAssetClass::SourceSessionLog,
        safety_classification: DoctorAssetSafetyClassification::PreciousEvidence,
        derived: false,
        precious: true,
        auto_delete_allowed: false,
        safe_to_gc_allowed: false,
        allowed_operations: DOCTOR_ASSET_PRECIOUS_EVIDENCE_OPS,
        notes: "Original provider session logs may be the only surviving archive copy.",
    },
    DoctorAssetPolicy {
        asset_class: DoctorAssetClass::RawMirrorBlob,
        safety_classification: DoctorAssetSafetyClassification::PreciousEvidence,
        derived: false,
        precious: true,
        auto_delete_allowed: false,
        safe_to_gc_allowed: false,
        allowed_operations: DOCTOR_ASSET_PRECIOUS_EVIDENCE_OPS,
        notes: "Raw mirrors preserve original bytes for future parser recovery.",
    },
    DoctorAssetPolicy {
        asset_class: DoctorAssetClass::CanonicalArchiveDb,
        safety_classification: DoctorAssetSafetyClassification::CanonicalArchive,
        derived: false,
        precious: true,
        auto_delete_allowed: false,
        safe_to_gc_allowed: false,
        allowed_operations: DOCTOR_ASSET_ARCHIVE_OPS,
        notes: "SQLite archive state is the source of truth for indexed conversations.",
    },
    DoctorAssetPolicy {
        asset_class: DoctorAssetClass::ArchiveDbSidecar,
        safety_classification: DoctorAssetSafetyClassification::RecoverableSidecar,
        derived: false,
        precious: true,
        auto_delete_allowed: false,
        safe_to_gc_allowed: false,
        allowed_operations: DOCTOR_ASSET_SIDECAR_OPS,
        notes: "WAL/SHM and related sidecars can contain uncheckpointed archive state.",
    },
    DoctorAssetPolicy {
        asset_class: DoctorAssetClass::SourceCoverageLedger,
        safety_classification: DoctorAssetSafetyClassification::AuditTrail,
        derived: false,
        precious: true,
        auto_delete_allowed: false,
        safe_to_gc_allowed: false,
        allowed_operations: DOCTOR_ASSET_AUDIT_OPS,
        notes: "Coverage ledgers explain which source paths have been mirrored or indexed.",
    },
    DoctorAssetPolicy {
        asset_class: DoctorAssetClass::UserConfig,
        safety_classification: DoctorAssetSafetyClassification::UserConfiguration,
        derived: false,
        precious: true,
        auto_delete_allowed: false,
        safe_to_gc_allowed: false,
        allowed_operations: DOCTOR_ASSET_USER_STATE_OPS,
        notes: "User configuration must be preserved unless an operator explicitly edits it.",
    },
    DoctorAssetPolicy {
        asset_class: DoctorAssetClass::BookmarkStore,
        safety_classification: DoctorAssetSafetyClassification::UserConfiguration,
        derived: false,
        precious: true,
        auto_delete_allowed: false,
        safe_to_gc_allowed: false,
        allowed_operations: DOCTOR_ASSET_USER_STATE_OPS,
        notes: "Bookmarks are user-authored archive annotations.",
    },
    DoctorAssetPolicy {
        asset_class: DoctorAssetClass::OperationReceipt,
        safety_classification: DoctorAssetSafetyClassification::AuditTrail,
        derived: false,
        precious: true,
        auto_delete_allowed: false,
        safe_to_gc_allowed: false,
        allowed_operations: DOCTOR_ASSET_AUDIT_OPS,
        notes: "Receipts prove what doctor changed or refused to change.",
    },
    DoctorAssetPolicy {
        asset_class: DoctorAssetClass::EventLog,
        safety_classification: DoctorAssetSafetyClassification::AuditTrail,
        derived: false,
        precious: true,
        auto_delete_allowed: false,
        safe_to_gc_allowed: false,
        allowed_operations: DOCTOR_ASSET_AUDIT_OPS,
        notes: "Event logs are forensic breadcrumbs for repair and support.",
    },
    DoctorAssetPolicy {
        asset_class: DoctorAssetClass::ForensicBundle,
        safety_classification: DoctorAssetSafetyClassification::DiagnosticEvidence,
        derived: false,
        precious: true,
        auto_delete_allowed: false,
        safe_to_gc_allowed: false,
        allowed_operations: DOCTOR_ASSET_DIAGNOSTIC_OPS,
        notes: "Forensic bundles are captured evidence, not cleanup candidates.",
    },
    DoctorAssetPolicy {
        asset_class: DoctorAssetClass::SupportBundle,
        safety_classification: DoctorAssetSafetyClassification::DiagnosticEvidence,
        derived: false,
        precious: true,
        auto_delete_allowed: false,
        safe_to_gc_allowed: false,
        allowed_operations: DOCTOR_ASSET_DIAGNOSTIC_OPS,
        notes: "Support bundles can be redacted and exported but never auto-pruned.",
    },
    DoctorAssetPolicy {
        asset_class: DoctorAssetClass::BackupBundle,
        safety_classification: DoctorAssetSafetyClassification::BackupEvidence,
        derived: false,
        precious: true,
        auto_delete_allowed: false,
        safe_to_gc_allowed: false,
        allowed_operations: DOCTOR_ASSET_BACKUP_OPS,
        notes: "Backups may be the only rollback point after a failed repair.",
    },
    DoctorAssetPolicy {
        asset_class: DoctorAssetClass::DerivedLexicalIndex,
        safety_classification: DoctorAssetSafetyClassification::DerivedRebuildable,
        derived: true,
        precious: false,
        auto_delete_allowed: false,
        safe_to_gc_allowed: false,
        allowed_operations: DOCTOR_ASSET_DERIVED_REBUILDABLE_OPS,
        notes: "Live lexical indexes are derived but must be reclassified before pruning.",
    },
    DoctorAssetPolicy {
        asset_class: DoctorAssetClass::DerivedSemanticIndex,
        safety_classification: DoctorAssetSafetyClassification::DerivedRebuildable,
        derived: true,
        precious: false,
        auto_delete_allowed: false,
        safe_to_gc_allowed: false,
        allowed_operations: DOCTOR_ASSET_DERIVED_REBUILDABLE_OPS,
        notes: "Semantic indexes are derived and rebuildable but can be expensive to recreate.",
    },
    DoctorAssetPolicy {
        asset_class: DoctorAssetClass::ModelCache,
        safety_classification: DoctorAssetSafetyClassification::DerivedRebuildable,
        derived: true,
        precious: false,
        auto_delete_allowed: false,
        safe_to_gc_allowed: false,
        allowed_operations: DOCTOR_ASSET_DERIVED_REBUILDABLE_OPS,
        notes: "Model caches are operator-installed files; do not silently delete them.",
    },
    DoctorAssetPolicy {
        asset_class: DoctorAssetClass::MemoCache,
        safety_classification: DoctorAssetSafetyClassification::DerivedReclaimable,
        derived: true,
        precious: false,
        auto_delete_allowed: true,
        safe_to_gc_allowed: true,
        allowed_operations: DOCTOR_ASSET_DERIVED_RECLAIMABLE_OPS,
        notes: "Memoization caches are derived and may be reclaimed when a plan proves safety.",
    },
    DoctorAssetPolicy {
        asset_class: DoctorAssetClass::RetainedPublishBackup,
        safety_classification: DoctorAssetSafetyClassification::DerivedReclaimable,
        derived: true,
        precious: false,
        auto_delete_allowed: true,
        safe_to_gc_allowed: true,
        allowed_operations: DOCTOR_ASSET_DERIVED_RECLAIMABLE_OPS,
        notes: "Bounded old lexical publish backups are derived rollback copies.",
    },
    DoctorAssetPolicy {
        asset_class: DoctorAssetClass::FailedSeedBundle,
        safety_classification: DoctorAssetSafetyClassification::DiagnosticEvidence,
        derived: false,
        precious: true,
        auto_delete_allowed: false,
        safe_to_gc_allowed: false,
        allowed_operations: DOCTOR_ASSET_DIAGNOSTIC_OPS,
        notes: "Failed seed bundles explain initialization failures and require inspection.",
    },
    DoctorAssetPolicy {
        asset_class: DoctorAssetClass::QuarantinedLexicalGeneration,
        safety_classification: DoctorAssetSafetyClassification::DiagnosticEvidence,
        derived: true,
        precious: false,
        auto_delete_allowed: false,
        safe_to_gc_allowed: false,
        allowed_operations: DOCTOR_ASSET_DIAGNOSTIC_OPS,
        notes: "Quarantined generations are retained as evidence until inspection clears them.",
    },
    DoctorAssetPolicy {
        asset_class: DoctorAssetClass::QuarantinedLexicalShard,
        safety_classification: DoctorAssetSafetyClassification::DiagnosticEvidence,
        derived: true,
        precious: false,
        auto_delete_allowed: false,
        safe_to_gc_allowed: false,
        allowed_operations: DOCTOR_ASSET_DIAGNOSTIC_OPS,
        notes: "Quarantined shards are retained as validation-failure evidence.",
    },
    DoctorAssetPolicy {
        asset_class: DoctorAssetClass::ReclaimableDerivedCache,
        safety_classification: DoctorAssetSafetyClassification::DerivedReclaimable,
        derived: true,
        precious: false,
        auto_delete_allowed: true,
        safe_to_gc_allowed: true,
        allowed_operations: DOCTOR_ASSET_DERIVED_RECLAIMABLE_OPS,
        notes: "A repair planner may use this only after proving the artifact is derived and replaceable.",
    },
    DoctorAssetPolicy {
        asset_class: DoctorAssetClass::ExternalUpstreamSource,
        safety_classification: DoctorAssetSafetyClassification::ExternalSource,
        derived: false,
        precious: true,
        auto_delete_allowed: false,
        safe_to_gc_allowed: false,
        allowed_operations: DOCTOR_ASSET_EXTERNAL_SOURCE_OPS,
        notes: "External provider stores are never owned by cass doctor cleanup.",
    },
    DoctorAssetPolicy {
        asset_class: DoctorAssetClass::Unknown,
        safety_classification: DoctorAssetSafetyClassification::Unknown,
        derived: false,
        precious: true,
        auto_delete_allowed: false,
        safe_to_gc_allowed: false,
        allowed_operations: DOCTOR_ASSET_NO_OPS,
        notes: "Unknown assets fail closed until a specific class is added to the taxonomy.",
    },
];

pub(crate) fn doctor_asset_policy(asset_class: DoctorAssetClass) -> &'static DoctorAssetPolicy {
    DOCTOR_ASSET_POLICY_TABLE
        .iter()
        .find(|policy| policy.asset_class == asset_class)
        .expect("doctor asset policy table must cover every class")
}

pub(crate) fn doctor_asset_safety(asset_class: DoctorAssetClass) -> DoctorAssetSafety {
    let policy = doctor_asset_policy(asset_class);
    DoctorAssetSafety {
        asset_class,
        safety_classification: policy.safety_classification,
        derived: policy.derived,
        precious: policy.precious,
        auto_delete_allowed: policy.auto_delete_allowed,
        safe_to_gc_allowed: policy.safe_to_gc_allowed,
    }
}

pub(crate) fn doctor_asset_allows_operation(
    asset_class: DoctorAssetClass,
    operation: DoctorAssetOperation,
) -> bool {
    doctor_asset_policy(asset_class)
        .allowed_operations
        .contains(&operation)
}

pub(crate) fn doctor_asset_safe_to_gc(
    asset_class: DoctorAssetClass,
    candidate_safe_to_gc: bool,
) -> bool {
    let policy = doctor_asset_policy(asset_class);
    candidate_safe_to_gc
        && policy.safe_to_gc_allowed
        && policy.auto_delete_allowed
        && doctor_asset_allows_operation(asset_class, DoctorAssetOperation::PruneReclaim)
}

pub(crate) fn doctor_asset_taxonomy_report() -> Vec<DoctorAssetTaxonomyEntry> {
    DOCTOR_ASSET_POLICY_TABLE
        .iter()
        .map(|policy| DoctorAssetTaxonomyEntry {
            asset_class: policy.asset_class,
            safety_classification: policy.safety_classification,
            derived: policy.derived,
            precious: policy.precious,
            auto_delete_allowed: policy.auto_delete_allowed,
            safe_to_gc_allowed: policy.safe_to_gc_allowed,
            allowed_operations: policy.allowed_operations.to_vec(),
            notes: policy.notes,
        })
        .collect()
}

pub(crate) fn doctor_asset_class_for_cleanup_disposition(
    disposition: crate::indexer::lexical_generation::LexicalCleanupDisposition,
) -> DoctorAssetClass {
    use crate::indexer::lexical_generation::LexicalCleanupDisposition;

    match disposition {
        LexicalCleanupDisposition::SupersededReclaimable
        | LexicalCleanupDisposition::FailedReclaimable => DoctorAssetClass::ReclaimableDerivedCache,
        LexicalCleanupDisposition::QuarantinedRetained => {
            DoctorAssetClass::QuarantinedLexicalGeneration
        }
        _ => DoctorAssetClass::DerivedLexicalIndex,
    }
}
