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

pub(crate) fn doctor_artifact_descriptor_blake3(
    artifact_kind: &str,
    asset_class: DoctorAssetClass,
    path: &str,
    size_bytes: Option<u64>,
) -> String {
    doctor_canonical_blake3(
        "doctor-artifact-descriptor-v1",
        serde_json::json!({
            "artifact_kind": artifact_kind,
            "asset_class": asset_class,
            "path": path,
            "size_bytes": size_bytes,
        }),
    )
}

pub(crate) fn doctor_artifact_from_cleanup_action(
    action: &DiagCleanupApplyAction,
    data_dir: &Path,
) -> DoctorArtifact {
    let exists = Path::new(&action.path).exists();
    let size_bytes = Some(action.planned_reclaimable_bytes);
    let checksum_status = if action.applied {
        DoctorArtifactChecksumStatus::NotRecorded
    } else {
        doctor_artifact_checksum_status(exists, None, None)
    };
    DoctorArtifact {
        artifact_id: doctor_canonical_blake3(
            "doctor-artifact-v1",
            serde_json::json!({
                "artifact_kind": action.artifact_kind,
                "asset_class": action.asset_safety.asset_class,
                "path": action.path,
                "generation_id": action.generation_id,
                "shard_id": action.shard_id,
                "planned_reclaimable_bytes": action.planned_reclaimable_bytes,
            }),
        ),
        artifact_kind: action.artifact_kind.clone(),
        asset_class: action.asset_safety.asset_class,
        path: action.path.clone(),
        redacted_path: doctor_redacted_path(&action.path, data_dir),
        exists,
        size_bytes,
        descriptor_blake3: doctor_artifact_descriptor_blake3(
            &action.artifact_kind,
            action.asset_safety.asset_class,
            &action.path,
            size_bytes,
        ),
        expected_content_blake3: None,
        actual_content_blake3: None,
        checksum_status,
    }
}

pub(crate) fn doctor_manifest_drift_status(
    artifacts: &[DoctorArtifact],
) -> DoctorDriftDetectionStatus {
    if artifacts
        .iter()
        .any(|artifact| artifact.checksum_status == DoctorArtifactChecksumStatus::Missing)
    {
        DoctorDriftDetectionStatus::MissingArtifact
    } else if artifacts
        .iter()
        .any(|artifact| artifact.checksum_status == DoctorArtifactChecksumStatus::Mismatched)
    {
        DoctorDriftDetectionStatus::ChecksumMismatch
    } else if !artifacts.is_empty()
        && artifacts
            .iter()
            .all(|artifact| artifact.checksum_status == DoctorArtifactChecksumStatus::Matched)
    {
        DoctorDriftDetectionStatus::Verified
    } else {
        DoctorDriftDetectionStatus::NotChecked
    }
}

pub(crate) fn doctor_artifact_manifest(artifacts: Vec<DoctorArtifact>) -> DoctorArtifactManifest {
    let drift_detection_status = doctor_manifest_drift_status(&artifacts);
    let manifest_blake3 = doctor_canonical_blake3(
        "doctor-artifact-manifest-v1",
        serde_json::json!({
            "schema_version": 1,
            "artifacts": artifacts,
            "drift_detection_status": drift_detection_status,
        }),
    );
    DoctorArtifactManifest {
        schema_version: 1,
        artifact_count: artifacts.len(),
        manifest_blake3,
        drift_detection_status,
        artifacts,
    }
}

pub(crate) fn doctor_cleanup_coverage_snapshot(
    generation_count: usize,
    reclaim_candidate_count: usize,
    reclaimable_bytes: u64,
    retained_bytes: u64,
    actions: &[DiagCleanupApplyAction],
) -> DoctorCoverageSnapshot {
    let mut covered_asset_classes: Vec<_> = actions
        .iter()
        .map(|action| action.asset_safety.asset_class)
        .collect();
    covered_asset_classes.sort_by_key(|class| format!("{class:?}"));
    covered_asset_classes.dedup();
    DoctorCoverageSnapshot {
        generation_count,
        reclaim_candidate_count,
        reclaimable_bytes,
        retained_bytes,
        artifact_count: actions.len(),
        covered_asset_classes,
    }
}

pub(crate) fn doctor_safety_gate_for_cleanup_action(
    action: &DiagCleanupApplyAction,
    data_dir: &Path,
    db_path: &Path,
    index_path: &Path,
    approval_fingerprint: &str,
) -> DoctorSafetyGate {
    let asset_class = action.asset_safety.asset_class;
    let allowed_by_mode =
        doctor_repair_mode_allows_asset_mutation(DoctorRepairMode::CleanupApply, asset_class);
    let allowed_by_taxonomy = doctor_asset_safe_to_gc(asset_class, true);
    let path_safe = action.applied
        || cleanup_target_path_is_safe(Path::new(&action.path), data_dir, db_path, index_path);
    let mut blocked_reasons = Vec::new();
    if !allowed_by_mode {
        blocked_reasons.push("mode_disallows_asset_class".to_string());
    }
    if !allowed_by_taxonomy {
        blocked_reasons.push("taxonomy_disallows_automatic_reclaim".to_string());
    }
    if !path_safe {
        blocked_reasons.push("unsafe_or_missing_path".to_string());
    }
    if let Some(skip_reason) = action.skip_reason.as_ref() {
        blocked_reasons.push(skip_reason.clone());
    }
    DoctorSafetyGate {
        mode: DoctorRepairMode::CleanupApply,
        asset_class,
        allowed_by_mode,
        allowed_by_taxonomy,
        path_safe,
        approval_requirement: DoctorApprovalRequirement::ApprovalFingerprint,
        approval_fingerprint: approval_fingerprint.to_string(),
        passed: blocked_reasons.is_empty(),
        blocked_reasons,
    }
}

pub(crate) fn doctor_cleanup_action_id(action: &DiagCleanupApplyAction) -> String {
    doctor_canonical_blake3(
        "doctor-action-v1",
        serde_json::json!({
            "action_kind": action.artifact_kind,
            "mode": DoctorRepairMode::CleanupApply,
            "asset_class": action.asset_safety.asset_class,
            "target_path": action.path,
            "generation_id": action.generation_id,
            "shard_id": action.shard_id,
            "planned_bytes": action.planned_reclaimable_bytes,
            "reason": action.reason,
        }),
    )
}

pub(crate) fn doctor_action_from_cleanup_action(
    action: &DiagCleanupApplyAction,
    status: DoctorActionStatus,
    data_dir: &Path,
    db_path: &Path,
    index_path: &Path,
    approval_fingerprint: &str,
) -> DoctorAction {
    let artifact = doctor_artifact_from_cleanup_action(action, data_dir);
    let safety_gate = doctor_safety_gate_for_cleanup_action(
        action,
        data_dir,
        db_path,
        index_path,
        approval_fingerprint,
    );
    let mut remaining_risk = Vec::new();
    if matches!(
        status,
        DoctorActionStatus::Skipped | DoctorActionStatus::Blocked | DoctorActionStatus::Failed
    ) {
        remaining_risk.push(
            action
                .skip_reason
                .clone()
                .unwrap_or_else(|| "planned cleanup did not apply".to_string()),
        );
    }
    let authority_decision = if safety_gate.passed {
        "allowed_by_taxonomy_and_path_gate"
    } else {
        "blocked_by_taxonomy_or_path_gate"
    }
    .to_string();
    let selected_authorities = vec![
        "doctor_asset_taxonomy_v1".to_string(),
        "lexical_cleanup_dry_run_v1".to_string(),
    ];
    let rejected_authorities = if safety_gate.passed {
        Vec::new()
    } else {
        safety_gate.blocked_reasons.clone()
    };
    let verification_outcome = match status {
        DoctorActionStatus::Planned => "pending_apply",
        DoctorActionStatus::Applied => "applied",
        DoctorActionStatus::Skipped => "skipped",
        DoctorActionStatus::Blocked => "blocked",
        DoctorActionStatus::Failed => "failed",
        DoctorActionStatus::Refused => "refused",
    }
    .to_string();
    let bytes_pruned = if status == DoctorActionStatus::Applied {
        action.reclaimed_bytes
    } else {
        0
    };
    let action_id = doctor_cleanup_action_id(action);
    DoctorAction {
        action_id,
        action_kind: action.artifact_kind.clone(),
        status,
        mode: DoctorRepairMode::CleanupApply,
        asset_class: action.asset_safety.asset_class,
        target_path: action.path.clone(),
        redacted_target_path: doctor_redacted_path(&action.path, data_dir),
        reason: action.reason.clone(),
        authority_decision,
        selected_authorities,
        rejected_authorities,
        safety_gate,
        planned_bytes: action.planned_reclaimable_bytes,
        bytes_copied: 0,
        bytes_moved: 0,
        bytes_pruned,
        backup_paths: Vec::new(),
        verification_outcome,
        remaining_risk,
        artifacts: vec![artifact],
    }
}

pub(crate) fn doctor_sorted_cleanup_actions(
    actions: &[DiagCleanupApplyAction],
) -> Vec<DiagCleanupApplyAction> {
    let mut sorted = actions.to_vec();
    sorted.sort_by(|left, right| {
        (
            left.artifact_kind.as_str(),
            left.path.as_str(),
            left.generation_id.as_deref().unwrap_or(""),
            left.shard_id.as_deref().unwrap_or(""),
            format!("{:?}", left.asset_safety.asset_class),
        )
            .cmp(&(
                right.artifact_kind.as_str(),
                right.path.as_str(),
                right.generation_id.as_deref().unwrap_or(""),
                right.shard_id.as_deref().unwrap_or(""),
                format!("{:?}", right.asset_safety.asset_class),
            ))
    });
    sorted
}

pub(crate) fn doctor_cleanup_plan_fingerprint(plan: &DoctorPlan) -> String {
    doctor_canonical_blake3(
        "doctor-plan-v1",
        serde_json::json!({
            "plan_kind": plan.plan_kind,
            "schema_version": plan.schema_version,
            "mode": plan.mode,
            "approval_requirement": plan.approval_requirement,
            "approval_fingerprint": plan.approval_fingerprint,
            "outcome_contract": plan.outcome_contract,
            "coverage_before": plan.coverage_before,
            "safety_gates": plan.safety_gates,
            "actions": plan.actions,
            "artifact_manifest": plan.artifact_manifest,
            "selected_authorities": plan.selected_authorities,
            "rejected_authorities": plan.rejected_authorities,
            "blocked_reasons": plan.blocked_reasons,
            "remaining_risk": plan.remaining_risk,
        }),
    )
}

pub(crate) fn build_cleanup_doctor_plan(
    result: &DiagCleanupApplyResult,
    data_dir: &Path,
    db_path: &Path,
    index_path: &Path,
) -> DoctorPlan {
    let action_source = if result.planned_actions.is_empty() {
        &result.actions
    } else {
        &result.planned_actions
    };
    let sorted_actions = doctor_sorted_cleanup_actions(action_source);
    let actions: Vec<_> = sorted_actions
        .iter()
        .map(|action| {
            doctor_action_from_cleanup_action(
                action,
                DoctorActionStatus::Planned,
                data_dir,
                db_path,
                index_path,
                &result.approval_fingerprint,
            )
        })
        .collect();
    let artifacts = actions
        .iter()
        .flat_map(|action| action.artifacts.clone())
        .collect::<Vec<_>>();
    let safety_gates = actions
        .iter()
        .map(|action| action.safety_gate.clone())
        .collect::<Vec<_>>();
    let selected_authorities = vec![
        "doctor_asset_taxonomy_v1".to_string(),
        "doctor_repair_mode_policy_v1".to_string(),
        "lexical_cleanup_dry_run_v1".to_string(),
    ];
    let remaining_risk = if result.blocked_reasons.is_empty() {
        Vec::new()
    } else {
        result.blocked_reasons.clone()
    };
    let mut plan = DoctorPlan {
        plan_kind: "doctor_cleanup_apply_plan_v1",
        schema_version: 1,
        mode: DoctorRepairMode::CleanupApply,
        approval_requirement: DoctorApprovalRequirement::ApprovalFingerprint,
        approval_fingerprint: result.approval_fingerprint.clone(),
        plan_fingerprint: String::new(),
        fingerprint_algorithm: "blake3-canonical-json-v1",
        outcome_contract: result.outcome_kind,
        coverage_before: doctor_cleanup_coverage_snapshot(
            result.before_generation_count,
            result.before_reclaim_candidate_count,
            result.before_reclaimable_bytes,
            result.before_retained_bytes,
            &sorted_actions,
        ),
        safety_gates,
        actions,
        artifact_manifest: doctor_artifact_manifest(artifacts),
        event_log: DoctorEventLogMetadata::default(),
        forensic_bundle: result.forensic_bundle.clone(),
        selected_authorities,
        rejected_authorities: Vec::new(),
        blocked_reasons: result.blocked_reasons.clone(),
        remaining_risk,
    };
    plan.plan_fingerprint = doctor_cleanup_plan_fingerprint(&plan);
    plan.event_log = doctor_cleanup_event_log_for_actions(
        &plan.plan_fingerprint,
        &plan.actions,
        result.operation_started_at_ms,
        None,
        None,
        "embedded_planned_events",
    );
    plan
}

pub(crate) fn doctor_cleanup_action_status(action: &DiagCleanupApplyAction) -> DoctorActionStatus {
    if action.applied {
        DoctorActionStatus::Applied
    } else if action.skipped {
        DoctorActionStatus::Skipped
    } else {
        DoctorActionStatus::Blocked
    }
}

pub(crate) fn doctor_action_status_counts(actions: &[DoctorAction]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for status in DOCTOR_ACTION_STATUS_VOCABULARY {
        counts.insert(doctor_action_status_label(*status), 0);
    }
    for action in actions {
        *counts
            .entry(doctor_action_status_label(action.status))
            .or_default() += 1;
    }
    counts
}

pub(crate) fn doctor_event_id(event: &DoctorEvent) -> String {
    doctor_canonical_blake3("doctor-event-v1", doctor_event_identity_payload(event))
}

pub(crate) fn doctor_event_log_checksum(events: &[DoctorEvent]) -> String {
    doctor_canonical_blake3(
        "doctor-event-log-v1",
        serde_json::json!({
            "schema_version": 1,
            "events": events
                .iter()
                .map(doctor_event_identity_payload)
                .collect::<Vec<_>>(),
        }),
    )
}

pub(crate) fn doctor_event_identity_payload(event: &DoctorEvent) -> serde_json::Value {
    serde_json::json!({
        "schema_version": event.schema_version,
        "previous_event_hash": event.previous_event_hash,
        "operation_id": event.operation_id,
        "action_id": event.action_id,
        "phase": event.phase,
        "mode": event.mode,
        "asset_class": event.asset_class,
        "redacted_target_path": event.redacted_target_path,
        "progress_label": event.progress_label,
        "safety_gate_passed": event.safety_gate_passed,
        "blocked_reasons": event.blocked_reasons,
        "receipt_correlation_id": event.receipt_correlation_id,
        "artifact_ids": event.artifact_ids,
    })
}

pub(crate) fn doctor_event_log_from_events(
    status: impl Into<String>,
    events: Vec<DoctorEvent>,
) -> DoctorEventLogMetadata {
    let hash_chain_tip = events.last().map(|event| event.event_id.clone());
    let checksum_blake3 = if events.is_empty() {
        None
    } else {
        Some(doctor_event_log_checksum(&events))
    };
    DoctorEventLogMetadata {
        path: None,
        checksum_blake3,
        hash_chain_tip,
        status: status.into(),
        event_count: events.len(),
        events,
    }
}

pub(crate) fn doctor_action_event_draft(
    operation_id: &str,
    mode: DoctorRepairMode,
    action: Option<&DoctorAction>,
    phase: &str,
    elapsed_ms: Option<i64>,
    progress_label: impl Into<String>,
    receipt_correlation_id: Option<&str>,
) -> DoctorEventDraft {
    DoctorEventDraft {
        operation_id: operation_id.to_string(),
        action_id: action.map(|action| action.action_id.clone()),
        phase: phase.to_string(),
        mode: action.map(|action| action.mode).unwrap_or(mode),
        asset_class: action.map(|action| action.asset_class),
        redacted_target_path: action.map(|action| action.redacted_target_path.clone()),
        elapsed_ms,
        progress_label: progress_label.into(),
        safety_gate_passed: action.map(|action| action.safety_gate.passed),
        blocked_reasons: action
            .map(|action| action.safety_gate.blocked_reasons.clone())
            .unwrap_or_default(),
        receipt_correlation_id: receipt_correlation_id.map(str::to_owned),
        artifact_ids: action
            .map(|action| {
                action
                    .artifacts
                    .iter()
                    .map(|artifact| artifact.artifact_id.clone())
                    .collect()
            })
            .unwrap_or_default(),
    }
}

pub(crate) fn doctor_status_event_draft(
    operation_id: &str,
    mode: DoctorRepairMode,
    phase: &str,
    asset_class: Option<DoctorAssetClass>,
    elapsed_ms: Option<i64>,
    progress_label: impl Into<String>,
    blocked_reasons: Vec<String>,
) -> DoctorEventDraft {
    DoctorEventDraft {
        operation_id: operation_id.to_string(),
        action_id: None,
        phase: phase.to_string(),
        mode,
        asset_class,
        redacted_target_path: None,
        elapsed_ms,
        progress_label: progress_label.into(),
        safety_gate_passed: None,
        blocked_reasons,
        receipt_correlation_id: None,
        artifact_ids: Vec::new(),
    }
}

pub(crate) fn push_doctor_event(events: &mut Vec<DoctorEvent>, draft: DoctorEventDraft) {
    let previous_event_hash = events.last().map(|event| event.event_id.clone());
    let mut event = DoctorEvent {
        schema_version: 1,
        event_id: String::new(),
        previous_event_hash,
        operation_id: draft.operation_id,
        action_id: draft.action_id,
        phase: draft.phase,
        mode: draft.mode,
        asset_class: draft.asset_class,
        redacted_target_path: draft.redacted_target_path,
        elapsed_ms: draft.elapsed_ms,
        progress_label: draft.progress_label,
        safety_gate_passed: draft.safety_gate_passed,
        blocked_reasons: draft.blocked_reasons,
        receipt_correlation_id: draft.receipt_correlation_id,
        artifact_ids: draft.artifact_ids,
    };
    event.event_id = doctor_event_id(&event);
    events.push(event);
}

pub(crate) fn doctor_cleanup_event_log_for_actions(
    operation_id: &str,
    actions: &[DoctorAction],
    started_at_ms: Option<i64>,
    finished_at_ms: Option<i64>,
    receipt_correlation_id: Option<&str>,
    status: &str,
) -> DoctorEventLogMetadata {
    let mut events = Vec::new();
    push_doctor_event(
        &mut events,
        doctor_action_event_draft(
            operation_id,
            DoctorRepairMode::CleanupApply,
            None,
            "operation_started",
            Some(0),
            "cleanup operation started",
            receipt_correlation_id,
        ),
    );
    for action in actions {
        push_doctor_event(
            &mut events,
            doctor_action_event_draft(
                operation_id,
                DoctorRepairMode::CleanupApply,
                Some(action),
                &format!("action_{}", doctor_action_status_label(action.status)),
                match (started_at_ms, finished_at_ms) {
                    (Some(started), Some(finished)) => Some(finished.saturating_sub(started)),
                    _ => None,
                },
                format!(
                    "cleanup action {} {}",
                    action.action_kind,
                    doctor_action_status_label(action.status)
                ),
                receipt_correlation_id,
            ),
        );
    }
    push_doctor_event(
        &mut events,
        doctor_action_event_draft(
            operation_id,
            DoctorRepairMode::CleanupApply,
            None,
            "operation_finished",
            match (started_at_ms, finished_at_ms) {
                (Some(started), Some(finished)) => Some(finished.saturating_sub(started)),
                _ => None,
            },
            "cleanup operation finished",
            receipt_correlation_id,
        ),
    );
    doctor_event_log_from_events(status, events)
}

pub(crate) fn doctor_check_event_phase(status: &str) -> &'static str {
    match status {
        "pass" => "check_pass",
        "warn" => "check_warn",
        "fail" => "check_fail",
        _ => "check_unknown",
    }
}

pub(crate) fn doctor_operation_event_log_for_checks(
    operation_id: &str,
    checks: &[DoctorCheckReport],
    fix_requested: bool,
    elapsed_ms: u64,
    status: &str,
) -> DoctorEventLogMetadata {
    let mode = if fix_requested {
        DoctorRepairMode::RepairApply
    } else {
        DoctorRepairMode::Check
    };
    let operation_label = if fix_requested {
        "doctor repair"
    } else {
        "doctor check"
    };
    let mut events = Vec::new();
    push_doctor_event(
        &mut events,
        doctor_status_event_draft(
            operation_id,
            mode,
            "operation_started",
            None,
            Some(0),
            format!("{operation_label} started"),
            Vec::new(),
        ),
    );
    for check in checks {
        let blocked_reasons = if check.status == "pass" {
            Vec::new()
        } else {
            vec![
                format!("check_status:{}", check.status),
                format!("anomaly:{}", doctor_serde_label(check.anomaly_class)),
            ]
        };
        push_doctor_event(
            &mut events,
            doctor_status_event_draft(
                operation_id,
                mode,
                doctor_check_event_phase(&check.status),
                Some(check.affected_asset_class),
                None,
                format!("{operation_label} {} {}", check.name, check.status),
                blocked_reasons,
            ),
        );
    }
    push_doctor_event(
        &mut events,
        doctor_status_event_draft(
            operation_id,
            mode,
            "operation_finished",
            None,
            Some(elapsed_ms.min(i64::MAX as u64) as i64),
            format!("{operation_label} finished"),
            Vec::new(),
        ),
    );
    doctor_event_log_from_events(status, events)
}
