use crate::doctor::policy::assets::{doctor_asset_allows_operation, doctor_asset_safe_to_gc};
use crate::doctor::policy::types::{DoctorAssetClass, DoctorAssetOperation, DoctorDataLossRisk};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DoctorRepairMode {
    #[default]
    Check,
    RepairDryRun,
    RepairApply,
    CleanupDryRun,
    CleanupApply,
    ReconstructDryRun,
    ReconstructPromote,
    RestoreRehearsal,
    RestoreApply,
    SupportBundle,
    BaselineDiff,
    SafeAutoRun,
    EmergencyForce,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DoctorApprovalRequirement {
    #[default]
    None,
    ApprovalFingerprint,
    ExplicitOperatorConfirmation,
    RehearsalReceipt,
    Refused,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DoctorRepairOutcomeKind {
    #[default]
    NoOp,
    Planned,
    Applied,
    Partial,
    Blocked,
    Failed,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum DoctorOperationOutcomeKind {
    #[default]
    OkNoActionNeeded,
    OkReadOnlyDiagnosed,
    Fixed,
    PartiallyFixed,
    RepairBlocked,
    RepairRefused,
    RepairIncomplete,
    VerificationFailed,
    CleanupDryRunOnly,
    CleanupRefused,
    AutoRunSkipped,
    SupportBundleOnly,
    BaselineDiffOnly,
    RequiresManualReview,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum DoctorExitCodeKind {
    #[default]
    Success,
    HealthFailure,
    UsageError,
    LockBusy,
    RepairFailure,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DoctorRepairRetrySafety {
    #[default]
    SafeToRetry,
    RetryAfterSameDryRun,
    RetryAfterInspection,
    DoNotRetryWithoutReview,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DoctorRepairModePolicy {
    pub(crate) mode: DoctorRepairMode,
    pub(crate) mutates: bool,
    pub(crate) robot_allowed: bool,
    pub(crate) approval_requirement: DoctorApprovalRequirement,
    pub(crate) allowed_mutation_asset_classes: &'static [DoctorAssetClass],
    pub(crate) required_fields: &'static [&'static str],
    pub(crate) stdout_contract: &'static str,
    pub(crate) stderr_contract: &'static str,
    pub(crate) aborts_on: &'static [&'static str],
    pub(crate) notes: &'static str,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DoctorOperationOutcomePolicy {
    pub(crate) kind: DoctorOperationOutcomeKind,
    pub(crate) reason: &'static str,
    pub(crate) action_taken: &'static str,
    pub(crate) action_not_taken: &'static str,
    pub(crate) safe_to_retry: bool,
    pub(crate) requires_override: bool,
    pub(crate) data_loss_risk: DoctorDataLossRisk,
    pub(crate) next_command: Option<&'static str>,
    pub(crate) artifact_manifest_path: Option<&'static str>,
    pub(crate) exit_code_kind: DoctorExitCodeKind,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct DoctorOperationOutcomeReport {
    pub(crate) kind: DoctorOperationOutcomeKind,
    pub(crate) reason: String,
    pub(crate) action_taken: String,
    pub(crate) action_not_taken: String,
    pub(crate) safe_to_retry: bool,
    pub(crate) requires_override: bool,
    pub(crate) data_loss_risk: DoctorDataLossRisk,
    pub(crate) next_command: Option<String>,
    pub(crate) artifact_manifest_path: Option<String>,
    pub(crate) exit_code_kind: DoctorExitCodeKind,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorOperationOutcomePolicyReport {
    pub(crate) kind: DoctorOperationOutcomeKind,
    pub(crate) reason: &'static str,
    pub(crate) action_taken: &'static str,
    pub(crate) action_not_taken: &'static str,
    pub(crate) safe_to_retry: bool,
    pub(crate) requires_override: bool,
    pub(crate) data_loss_risk: DoctorDataLossRisk,
    pub(crate) next_command: Option<&'static str>,
    pub(crate) artifact_manifest_path: Option<&'static str>,
    pub(crate) exit_code_kind: DoctorExitCodeKind,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorRepairModePolicyReport {
    pub(crate) mode: DoctorRepairMode,
    pub(crate) mutates: bool,
    pub(crate) robot_allowed: bool,
    pub(crate) approval_requirement: DoctorApprovalRequirement,
    pub(crate) allowed_mutation_asset_classes: Vec<DoctorAssetClass>,
    pub(crate) required_fields: Vec<&'static str>,
    pub(crate) stdout_contract: &'static str,
    pub(crate) stderr_contract: &'static str,
    pub(crate) aborts_on: Vec<&'static str>,
    pub(crate) notes: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorRepairLegacyAliasReport {
    pub(crate) invocation: &'static str,
    pub(crate) mode: DoctorRepairMode,
    pub(crate) notes: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorRepairContractReport {
    pub(crate) default_mode: DoctorRepairMode,
    pub(crate) default_non_destructive: bool,
    pub(crate) fail_closed: bool,
    pub(crate) plan_receipt_schema: DoctorPlanReceiptSchemaReport,
    pub(crate) verification_contract: DoctorVerificationContractReport,
    pub(crate) approval_requirements: Vec<DoctorApprovalRequirement>,
    pub(crate) outcome_kinds: Vec<DoctorRepairOutcomeKind>,
    pub(crate) operation_outcome_kinds: Vec<DoctorOperationOutcomeKind>,
    pub(crate) operation_outcome_contract: Vec<DoctorOperationOutcomePolicyReport>,
    pub(crate) retry_safety_kinds: Vec<DoctorRepairRetrySafety>,
    pub(crate) mode_policies: Vec<DoctorRepairModePolicyReport>,
    pub(crate) legacy_aliases: Vec<DoctorRepairLegacyAliasReport>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct DoctorRepairReceipt {
    pub(crate) receipt_kind: &'static str,
    pub(crate) schema_version: u32,
    pub(crate) mode: DoctorRepairMode,
    pub(crate) outcome_kind: DoctorRepairOutcomeKind,
    pub(crate) approval_fingerprint: String,
    pub(crate) plan_fingerprint: String,
    pub(crate) started_at_ms: Option<i64>,
    pub(crate) finished_at_ms: Option<i64>,
    pub(crate) duration_ms: Option<i64>,
    pub(crate) planned_action_count: usize,
    pub(crate) applied_action_count: usize,
    pub(crate) skipped_action_count: usize,
    pub(crate) failed_action_count: usize,
    pub(crate) bytes_planned: u64,
    pub(crate) bytes_copied: u64,
    pub(crate) bytes_moved: u64,
    pub(crate) bytes_pruned: u64,
    pub(crate) reclaimed_bytes: u64,
    pub(crate) backup_paths: Vec<String>,
    pub(crate) selected_authorities: Vec<String>,
    pub(crate) rejected_authorities: Vec<String>,
    pub(crate) verification_outcomes: Vec<String>,
    pub(crate) remaining_risk: Vec<String>,
    pub(crate) event_log: DoctorEventLogMetadata,
    pub(crate) forensic_bundle: DoctorForensicBundleMetadata,
    pub(crate) artifact_manifest: DoctorArtifactManifest,
    pub(crate) artifact_checksums: Vec<DoctorArtifact>,
    pub(crate) drift_detection_status: DoctorDriftDetectionStatus,
    pub(crate) coverage_before: DoctorCoverageSnapshot,
    pub(crate) coverage_after: DoctorCoverageSnapshot,
    pub(crate) actions: Vec<DoctorAction>,
    pub(crate) action_status_counts: BTreeMap<String, usize>,
    pub(crate) blocked_reasons: Vec<String>,
}

pub(crate) const DOCTOR_REPAIR_FAILURE_MARKER_SCHEMA_VERSION: u32 = 1;

pub(crate) const DOCTOR_REPAIR_FAILURE_MARKER_KIND: &str = "cass_doctor_repair_failure_marker_v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DoctorRepairFailureMarkerArtifact {
    pub(crate) artifact_kind: String,
    pub(crate) asset_class: String,
    pub(crate) path: String,
    pub(crate) redacted_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DoctorRepairFailureMarker {
    pub(crate) marker_kind: String,
    pub(crate) schema_version: u32,
    pub(crate) repair_class: String,
    pub(crate) operation_id: String,
    pub(crate) command_line_mode: String,
    pub(crate) plan_fingerprint: String,
    pub(crate) affected_artifacts: Vec<DoctorRepairFailureMarkerArtifact>,
    pub(crate) selected_authorities: Vec<String>,
    pub(crate) rejected_authorities: Vec<String>,
    pub(crate) preflight_checks: Vec<String>,
    pub(crate) applied_actions: Vec<String>,
    pub(crate) verification_checks: Vec<String>,
    pub(crate) failed_checks: Vec<String>,
    pub(crate) forensic_bundle_path: Option<String>,
    pub(crate) candidate_path: Option<String>,
    pub(crate) started_at_ms: Option<i64>,
    pub(crate) failed_at_ms: i64,
    pub(crate) cass_version: String,
    pub(crate) platform: String,
    pub(crate) user_data_modified: bool,
    pub(crate) operation_outcome_kind: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorRepairFailureMarkerReport {
    pub(crate) schema_version: u32,
    pub(crate) repair_class: String,
    pub(crate) found: bool,
    pub(crate) parse_status: String,
    pub(crate) path: Option<String>,
    pub(crate) redacted_path: Option<String>,
    pub(crate) operation_id: Option<String>,
    pub(crate) plan_fingerprint: Option<String>,
    pub(crate) failed_at_ms: Option<i64>,
    pub(crate) failed_at: Option<String>,
    pub(crate) cass_version: Option<String>,
    pub(crate) platform: Option<String>,
    pub(crate) user_data_modified: Option<bool>,
    pub(crate) failed_checks: Vec<String>,
    pub(crate) corrupt_reason: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DoctorActionStatus {
    #[default]
    Planned,
    Applied,
    Skipped,
    Blocked,
    Failed,
    Refused,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DoctorArtifactChecksumStatus {
    #[default]
    NotRecorded,
    Matched,
    Mismatched,
    Missing,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DoctorDriftDetectionStatus {
    #[default]
    NotChecked,
    Verified,
    ChecksumMismatch,
    MissingArtifact,
    ManifestMismatch,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorPlanReceiptSchemaReport {
    pub(crate) plan_schema_version: u32,
    pub(crate) receipt_schema_version: u32,
    pub(crate) action_schema_version: u32,
    pub(crate) artifact_schema_version: u32,
    pub(crate) fingerprint_algorithm: &'static str,
    pub(crate) plan_fingerprint_includes: Vec<&'static str>,
    pub(crate) receipt_required_fields: Vec<&'static str>,
    pub(crate) action_statuses: Vec<DoctorActionStatus>,
    pub(crate) artifact_checksum_statuses: Vec<DoctorArtifactChecksumStatus>,
    pub(crate) drift_detection_statuses: Vec<DoctorDriftDetectionStatus>,
    pub(crate) redaction_contract: &'static str,
    pub(crate) tamper_evidence_scope: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorVerificationMatrixEntry {
    pub(crate) scenario_id: &'static str,
    pub(crate) feature_area: &'static str,
    pub(crate) proof_layers: Vec<&'static str>,
    pub(crate) representative_commands: Vec<&'static str>,
    pub(crate) required_artifacts: Vec<&'static str>,
    pub(crate) mutation_audit_required: bool,
    pub(crate) redaction_required: bool,
    pub(crate) archive_preservation_assertion: &'static str,
    pub(crate) notes: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorVerificationContractReport {
    pub(crate) schema_version: u32,
    pub(crate) artifact_directory_contract: &'static str,
    pub(crate) manifest_file: &'static str,
    pub(crate) per_step_log_contract: &'static str,
    pub(crate) required_manifest_fields: Vec<&'static str>,
    pub(crate) required_step_log_fields: Vec<&'static str>,
    pub(crate) required_inventory_fields: Vec<&'static str>,
    pub(crate) required_receipt_fields: Vec<&'static str>,
    pub(crate) required_redaction_guarantees: Vec<&'static str>,
    pub(crate) matrix: Vec<DoctorVerificationMatrixEntry>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct DoctorCoverageSnapshot {
    pub(crate) generation_count: usize,
    pub(crate) reclaim_candidate_count: usize,
    pub(crate) reclaimable_bytes: u64,
    pub(crate) retained_bytes: u64,
    pub(crate) artifact_count: usize,
    pub(crate) covered_asset_classes: Vec<DoctorAssetClass>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorSafetyGate {
    pub(crate) mode: DoctorRepairMode,
    pub(crate) asset_class: DoctorAssetClass,
    pub(crate) allowed_by_mode: bool,
    pub(crate) allowed_by_taxonomy: bool,
    pub(crate) path_safe: bool,
    pub(crate) approval_requirement: DoctorApprovalRequirement,
    pub(crate) approval_fingerprint: String,
    pub(crate) passed: bool,
    pub(crate) blocked_reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorArtifact {
    pub(crate) artifact_id: String,
    pub(crate) artifact_kind: String,
    pub(crate) asset_class: DoctorAssetClass,
    pub(crate) path: String,
    pub(crate) redacted_path: String,
    pub(crate) exists: bool,
    pub(crate) size_bytes: Option<u64>,
    pub(crate) descriptor_blake3: String,
    pub(crate) expected_content_blake3: Option<String>,
    pub(crate) actual_content_blake3: Option<String>,
    pub(crate) checksum_status: DoctorArtifactChecksumStatus,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorAction {
    pub(crate) action_id: String,
    pub(crate) action_kind: String,
    pub(crate) status: DoctorActionStatus,
    pub(crate) mode: DoctorRepairMode,
    pub(crate) asset_class: DoctorAssetClass,
    pub(crate) target_path: String,
    pub(crate) redacted_target_path: String,
    pub(crate) reason: String,
    pub(crate) authority_decision: String,
    pub(crate) selected_authorities: Vec<String>,
    pub(crate) rejected_authorities: Vec<String>,
    pub(crate) safety_gate: DoctorSafetyGate,
    pub(crate) planned_bytes: u64,
    pub(crate) bytes_copied: u64,
    pub(crate) bytes_moved: u64,
    pub(crate) bytes_pruned: u64,
    pub(crate) backup_paths: Vec<String>,
    pub(crate) verification_outcome: String,
    pub(crate) remaining_risk: Vec<String>,
    pub(crate) artifacts: Vec<DoctorArtifact>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorEvent {
    pub(crate) schema_version: u32,
    pub(crate) event_id: String,
    pub(crate) previous_event_hash: Option<String>,
    pub(crate) operation_id: String,
    pub(crate) action_id: Option<String>,
    pub(crate) phase: String,
    pub(crate) mode: DoctorRepairMode,
    pub(crate) asset_class: Option<DoctorAssetClass>,
    pub(crate) redacted_target_path: Option<String>,
    pub(crate) elapsed_ms: Option<i64>,
    pub(crate) progress_label: String,
    pub(crate) safety_gate_passed: Option<bool>,
    pub(crate) blocked_reasons: Vec<String>,
    pub(crate) receipt_correlation_id: Option<String>,
    pub(crate) artifact_ids: Vec<String>,
}

pub(crate) struct DoctorEventDraft {
    pub(crate) operation_id: String,
    pub(crate) action_id: Option<String>,
    pub(crate) phase: String,
    pub(crate) mode: DoctorRepairMode,
    pub(crate) asset_class: Option<DoctorAssetClass>,
    pub(crate) redacted_target_path: Option<String>,
    pub(crate) elapsed_ms: Option<i64>,
    pub(crate) progress_label: String,
    pub(crate) safety_gate_passed: Option<bool>,
    pub(crate) blocked_reasons: Vec<String>,
    pub(crate) receipt_correlation_id: Option<String>,
    pub(crate) artifact_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct DoctorEventLogMetadata {
    pub(crate) path: Option<String>,
    pub(crate) checksum_blake3: Option<String>,
    pub(crate) hash_chain_tip: Option<String>,
    pub(crate) status: String,
    pub(crate) event_count: usize,
    pub(crate) events: Vec<DoctorEvent>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct DoctorPostRepairProbeSuiteReport {
    pub(crate) schema_version: u32,
    pub(crate) requested: bool,
    pub(crate) status: String,
    pub(crate) probe_run_id: Option<String>,
    pub(crate) artifact_root: Option<String>,
    pub(crate) redacted_artifact_root: Option<String>,
    pub(crate) manifest_path: Option<String>,
    pub(crate) redacted_manifest_path: Option<String>,
    pub(crate) probe_count: usize,
    pub(crate) passed_count: usize,
    pub(crate) failed_count: usize,
    pub(crate) skipped_count: usize,
    pub(crate) blocks_success: bool,
    pub(crate) probes: Vec<DoctorPostRepairProbeReport>,
    pub(crate) warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorPostRepairProbeReport {
    pub(crate) schema_version: u32,
    pub(crate) probe_id: String,
    pub(crate) target_asset_class: DoctorAssetClass,
    pub(crate) target_path: String,
    pub(crate) redacted_target_path: String,
    pub(crate) generation_identity: Option<String>,
    pub(crate) manifest_identity: Option<String>,
    pub(crate) status: String,
    pub(crate) duration_ms: u64,
    pub(crate) failure_reason: Option<String>,
    pub(crate) blocks_promotion: bool,
    pub(crate) blocks_cleanup: bool,
    pub(crate) receipt_correlation_id: String,
    pub(crate) event_log_correlation_id: String,
    pub(crate) failure_context_path: Option<String>,
    pub(crate) redacted_failure_context_path: Option<String>,
    pub(crate) rollback_confirmed: bool,
    pub(crate) steps: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct DoctorForensicBundleArtifact {
    pub(crate) artifact_kind: String,
    pub(crate) source_path: String,
    pub(crate) redacted_source_path: String,
    pub(crate) bundle_relative_path: Option<String>,
    pub(crate) exists: bool,
    pub(crate) copied: bool,
    pub(crate) required: bool,
    pub(crate) sidecar_group: Option<String>,
    pub(crate) size_bytes: Option<u64>,
    pub(crate) checksum_blake3: Option<String>,
    pub(crate) error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct DoctorForensicBundleMetadata {
    pub(crate) bundle_id: Option<String>,
    pub(crate) path: Option<String>,
    pub(crate) redacted_path: Option<String>,
    pub(crate) manifest_path: Option<String>,
    pub(crate) redacted_manifest_path: Option<String>,
    pub(crate) checksum_blake3: Option<String>,
    pub(crate) status: String,
    pub(crate) created_at_ms: Option<i64>,
    pub(crate) artifact_count: usize,
    pub(crate) copied_artifact_count: usize,
    pub(crate) missing_artifact_count: usize,
    pub(crate) failed_artifact_count: usize,
    pub(crate) sidecar_complete: bool,
    pub(crate) redaction_status: String,
    pub(crate) blocked_reason: Option<String>,
    pub(crate) artifacts: Vec<DoctorForensicBundleArtifact>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct DoctorArtifactManifest {
    pub(crate) schema_version: u32,
    pub(crate) artifact_count: usize,
    pub(crate) manifest_blake3: String,
    pub(crate) drift_detection_status: DoctorDriftDetectionStatus,
    pub(crate) artifacts: Vec<DoctorArtifact>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorPlan {
    pub(crate) plan_kind: &'static str,
    pub(crate) schema_version: u32,
    pub(crate) mode: DoctorRepairMode,
    pub(crate) approval_requirement: DoctorApprovalRequirement,
    pub(crate) approval_fingerprint: String,
    pub(crate) plan_fingerprint: String,
    pub(crate) fingerprint_algorithm: &'static str,
    pub(crate) outcome_contract: DoctorRepairOutcomeKind,
    pub(crate) coverage_before: DoctorCoverageSnapshot,
    pub(crate) safety_gates: Vec<DoctorSafetyGate>,
    pub(crate) actions: Vec<DoctorAction>,
    pub(crate) artifact_manifest: DoctorArtifactManifest,
    pub(crate) event_log: DoctorEventLogMetadata,
    pub(crate) forensic_bundle: DoctorForensicBundleMetadata,
    pub(crate) selected_authorities: Vec<String>,
    pub(crate) rejected_authorities: Vec<String>,
    pub(crate) blocked_reasons: Vec<String>,
    pub(crate) remaining_risk: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorRepairPlanActionPreview {
    pub(crate) action_kind: String,
    pub(crate) mode: DoctorRepairMode,
    pub(crate) asset_class: DoctorAssetClass,
    pub(crate) authority_decision: String,
    pub(crate) reason: String,
    pub(crate) planned_bytes: u64,
    pub(crate) safety_notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorRepairPlanPreviewReport {
    pub(crate) schema_version: u32,
    pub(crate) plan_kind: &'static str,
    pub(crate) mode: DoctorRepairMode,
    pub(crate) dry_run: bool,
    pub(crate) apply_requested: bool,
    pub(crate) approval_required: bool,
    pub(crate) approval_status: String,
    pub(crate) provided_plan_fingerprint: Option<String>,
    pub(crate) plan_fingerprint: String,
    pub(crate) fingerprint_algorithm: &'static str,
    pub(crate) fingerprint_inputs: serde_json::Value,
    pub(crate) exact_apply_command: String,
    pub(crate) apply_argv: Vec<String>,
    pub(crate) will_mutate: bool,
    pub(crate) apply_authorized: bool,
    pub(crate) planned_action_count: usize,
    pub(crate) planned_actions: Vec<DoctorRepairPlanActionPreview>,
    pub(crate) selected_authorities: Vec<String>,
    pub(crate) rejected_authorities: Vec<String>,
    pub(crate) blocked_reasons: Vec<String>,
    pub(crate) branchable_blocker_codes: Vec<String>,
    pub(crate) warnings: Vec<String>,
    pub(crate) freshness_policy: Vec<&'static str>,
    pub(crate) forensic_bundle_required: bool,
    pub(crate) post_repair_probes_required: bool,
    pub(crate) never_prunes_source_evidence: bool,
}

pub(crate) const DOCTOR_REPAIR_ACTION_PROMOTE_RECONSTRUCT_CANDIDATE: &str =
    "promote_reconstruct_candidate_archive_bundle";

pub(crate) const DOCTOR_REPAIR_ACTION_REBUILD_FROM_PROMOTED_CANDIDATE: &str =
    "rebuild_derived_lexical_index_from_promoted_candidate_archive_db";

pub(crate) const DOCTOR_REPAIR_ACTION_REBUILD_FROM_ARCHIVE_DB: &str =
    "rebuild_derived_lexical_index_from_archive_db";

#[cfg(test)]
pub(crate) const DOCTOR_REPAIR_ALL_MODES: &[DoctorRepairMode] = &[
    DoctorRepairMode::Check,
    DoctorRepairMode::RepairDryRun,
    DoctorRepairMode::RepairApply,
    DoctorRepairMode::CleanupDryRun,
    DoctorRepairMode::CleanupApply,
    DoctorRepairMode::ReconstructDryRun,
    DoctorRepairMode::ReconstructPromote,
    DoctorRepairMode::RestoreRehearsal,
    DoctorRepairMode::RestoreApply,
    DoctorRepairMode::SupportBundle,
    DoctorRepairMode::BaselineDiff,
    DoctorRepairMode::SafeAutoRun,
    DoctorRepairMode::EmergencyForce,
];

pub(crate) const DOCTOR_REPAIR_NO_MUTATION_ASSETS: &[DoctorAssetClass] = &[];

pub(crate) const DOCTOR_REPAIR_APPROVAL_REQUIREMENT_VOCABULARY: &[DoctorApprovalRequirement] = &[
    DoctorApprovalRequirement::None,
    DoctorApprovalRequirement::ApprovalFingerprint,
    DoctorApprovalRequirement::ExplicitOperatorConfirmation,
    DoctorApprovalRequirement::RehearsalReceipt,
    DoctorApprovalRequirement::Refused,
];

pub(crate) const DOCTOR_REPAIR_OUTCOME_KIND_VOCABULARY: &[DoctorRepairOutcomeKind] = &[
    DoctorRepairOutcomeKind::NoOp,
    DoctorRepairOutcomeKind::Planned,
    DoctorRepairOutcomeKind::Applied,
    DoctorRepairOutcomeKind::Partial,
    DoctorRepairOutcomeKind::Blocked,
    DoctorRepairOutcomeKind::Failed,
];

pub(crate) const DOCTOR_OPERATION_OUTCOME_KIND_VOCABULARY: &[DoctorOperationOutcomeKind] = &[
    DoctorOperationOutcomeKind::OkNoActionNeeded,
    DoctorOperationOutcomeKind::OkReadOnlyDiagnosed,
    DoctorOperationOutcomeKind::Fixed,
    DoctorOperationOutcomeKind::PartiallyFixed,
    DoctorOperationOutcomeKind::RepairBlocked,
    DoctorOperationOutcomeKind::RepairRefused,
    DoctorOperationOutcomeKind::RepairIncomplete,
    DoctorOperationOutcomeKind::VerificationFailed,
    DoctorOperationOutcomeKind::CleanupDryRunOnly,
    DoctorOperationOutcomeKind::CleanupRefused,
    DoctorOperationOutcomeKind::AutoRunSkipped,
    DoctorOperationOutcomeKind::SupportBundleOnly,
    DoctorOperationOutcomeKind::BaselineDiffOnly,
    DoctorOperationOutcomeKind::RequiresManualReview,
];

pub(crate) const DOCTOR_REPAIR_RETRY_SAFETY_VOCABULARY: &[DoctorRepairRetrySafety] = &[
    DoctorRepairRetrySafety::SafeToRetry,
    DoctorRepairRetrySafety::RetryAfterSameDryRun,
    DoctorRepairRetrySafety::RetryAfterInspection,
    DoctorRepairRetrySafety::DoNotRetryWithoutReview,
];

pub(crate) const DOCTOR_OPERATION_OUTCOME_POLICY_TABLE: &[DoctorOperationOutcomePolicy] = &[
    DoctorOperationOutcomePolicy {
        kind: DoctorOperationOutcomeKind::OkNoActionNeeded,
        reason: "doctor found no work that needs action",
        action_taken: "diagnostics completed",
        action_not_taken: "no repair or cleanup was needed",
        safe_to_retry: true,
        requires_override: false,
        data_loss_risk: DoctorDataLossRisk::None,
        next_command: None,
        artifact_manifest_path: None,
        exit_code_kind: DoctorExitCodeKind::Success,
    },
    DoctorOperationOutcomePolicy {
        kind: DoctorOperationOutcomeKind::OkReadOnlyDiagnosed,
        reason: "doctor diagnosed issues without mutation",
        action_taken: "read-only diagnostics completed",
        action_not_taken: "repair was not attempted in read-only mode",
        safe_to_retry: true,
        requires_override: false,
        data_loss_risk: DoctorDataLossRisk::Low,
        next_command: Some("cass doctor --fix --json"),
        artifact_manifest_path: None,
        exit_code_kind: DoctorExitCodeKind::HealthFailure,
    },
    DoctorOperationOutcomePolicy {
        kind: DoctorOperationOutcomeKind::Fixed,
        reason: "requested operation completed all safe planned work",
        action_taken: "safe repair or cleanup actions were applied",
        action_not_taken: "no planned safe action was skipped",
        safe_to_retry: true,
        requires_override: false,
        data_loss_risk: DoctorDataLossRisk::None,
        next_command: Some("cass doctor --json"),
        artifact_manifest_path: Some("receipt.artifact_manifest"),
        exit_code_kind: DoctorExitCodeKind::Success,
    },
    DoctorOperationOutcomePolicy {
        kind: DoctorOperationOutcomeKind::PartiallyFixed,
        reason: "requested operation applied some work but left blocked work",
        action_taken: "a subset of safe actions was applied",
        action_not_taken: "blocked or skipped actions remain",
        safe_to_retry: true,
        requires_override: false,
        data_loss_risk: DoctorDataLossRisk::Medium,
        next_command: Some("cass doctor --json"),
        artifact_manifest_path: Some("receipt.artifact_manifest"),
        exit_code_kind: DoctorExitCodeKind::RepairFailure,
    },
    DoctorOperationOutcomePolicy {
        kind: DoctorOperationOutcomeKind::RepairBlocked,
        reason: "repair could not proceed because a required precondition was blocked",
        action_taken: "no unsafe mutation was performed",
        action_not_taken: "repair was not attempted until the blocker is resolved",
        safe_to_retry: false,
        requires_override: false,
        data_loss_risk: DoctorDataLossRisk::Unknown,
        next_command: Some("cass doctor --json"),
        artifact_manifest_path: None,
        exit_code_kind: DoctorExitCodeKind::LockBusy,
    },
    DoctorOperationOutcomePolicy {
        kind: DoctorOperationOutcomeKind::RepairRefused,
        reason: "repair request violated the doctor safety contract",
        action_taken: "the unsafe request was refused",
        action_not_taken: "no archive or source evidence was mutated",
        safe_to_retry: false,
        requires_override: true,
        data_loss_risk: DoctorDataLossRisk::High,
        next_command: Some("cass doctor --json"),
        artifact_manifest_path: None,
        exit_code_kind: DoctorExitCodeKind::UsageError,
    },
    DoctorOperationOutcomePolicy {
        kind: DoctorOperationOutcomeKind::RepairIncomplete,
        reason: "repair started but did not reach a verified receipt",
        action_taken: "available receipt or event-log evidence was preserved",
        action_not_taken: "doctor did not claim successful repair",
        safe_to_retry: false,
        requires_override: false,
        data_loss_risk: DoctorDataLossRisk::Medium,
        next_command: Some("cass doctor --json"),
        artifact_manifest_path: Some("receipt.artifact_manifest"),
        exit_code_kind: DoctorExitCodeKind::RepairFailure,
    },
    DoctorOperationOutcomePolicy {
        kind: DoctorOperationOutcomeKind::VerificationFailed,
        reason: "post-repair verification failed",
        action_taken: "failure evidence was retained for inspection",
        action_not_taken: "doctor refused to repeat repair automatically",
        safe_to_retry: false,
        requires_override: false,
        data_loss_risk: DoctorDataLossRisk::High,
        next_command: Some("cass doctor --json"),
        artifact_manifest_path: Some("receipt.artifact_manifest"),
        exit_code_kind: DoctorExitCodeKind::RepairFailure,
    },
    DoctorOperationOutcomePolicy {
        kind: DoctorOperationOutcomeKind::CleanupDryRunOnly,
        reason: "cleanup was planned without mutation",
        action_taken: "cleanup candidates and approval fingerprint were reported",
        action_not_taken: "no cleanup target was pruned",
        safe_to_retry: true,
        requires_override: false,
        data_loss_risk: DoctorDataLossRisk::None,
        next_command: Some("cass doctor --fix --json"),
        artifact_manifest_path: Some("plan.artifact_manifest"),
        exit_code_kind: DoctorExitCodeKind::Success,
    },
    DoctorOperationOutcomePolicy {
        kind: DoctorOperationOutcomeKind::CleanupRefused,
        reason: "cleanup target failed a safety gate",
        action_taken: "cleanup refusal was reported",
        action_not_taken: "no cleanup target was pruned",
        safe_to_retry: false,
        requires_override: false,
        data_loss_risk: DoctorDataLossRisk::Medium,
        next_command: Some("cass diag --json --quarantine"),
        artifact_manifest_path: Some("receipt.artifact_manifest"),
        exit_code_kind: DoctorExitCodeKind::RepairFailure,
    },
    DoctorOperationOutcomePolicy {
        kind: DoctorOperationOutcomeKind::AutoRunSkipped,
        reason: "auto-run found no issue eligible for automatic repair",
        action_taken: "safe auto-run evaluated available actions",
        action_not_taken: "no repair was applied automatically",
        safe_to_retry: true,
        requires_override: false,
        data_loss_risk: DoctorDataLossRisk::Low,
        next_command: Some("cass doctor --json"),
        artifact_manifest_path: None,
        exit_code_kind: DoctorExitCodeKind::HealthFailure,
    },
    DoctorOperationOutcomePolicy {
        kind: DoctorOperationOutcomeKind::SupportBundleOnly,
        reason: "support bundle generation is diagnostic-only",
        action_taken: "support bundle metadata was produced",
        action_not_taken: "no archive repair or cleanup was attempted",
        safe_to_retry: true,
        requires_override: false,
        data_loss_risk: DoctorDataLossRisk::None,
        next_command: None,
        artifact_manifest_path: Some("support_bundle.artifact_manifest"),
        exit_code_kind: DoctorExitCodeKind::Success,
    },
    DoctorOperationOutcomePolicy {
        kind: DoctorOperationOutcomeKind::BaselineDiffOnly,
        reason: "baseline diff is diagnostic-only",
        action_taken: "baseline comparison was produced",
        action_not_taken: "no archive repair or cleanup was attempted",
        safe_to_retry: true,
        requires_override: false,
        data_loss_risk: DoctorDataLossRisk::None,
        next_command: None,
        artifact_manifest_path: Some("baseline_diff.artifact_manifest"),
        exit_code_kind: DoctorExitCodeKind::Success,
    },
    DoctorOperationOutcomePolicy {
        kind: DoctorOperationOutcomeKind::RequiresManualReview,
        reason: "doctor found risk that needs human review",
        action_taken: "risk was classified without mutation",
        action_not_taken: "doctor did not choose an authority or repair path automatically",
        safe_to_retry: false,
        requires_override: false,
        data_loss_risk: DoctorDataLossRisk::High,
        next_command: Some("cass doctor --json"),
        artifact_manifest_path: None,
        exit_code_kind: DoctorExitCodeKind::HealthFailure,
    },
];

pub(crate) const DOCTOR_ACTION_STATUS_VOCABULARY: &[DoctorActionStatus] = &[
    DoctorActionStatus::Planned,
    DoctorActionStatus::Applied,
    DoctorActionStatus::Skipped,
    DoctorActionStatus::Blocked,
    DoctorActionStatus::Failed,
    DoctorActionStatus::Refused,
];

pub(crate) const DOCTOR_ARTIFACT_CHECKSUM_STATUS_VOCABULARY: &[DoctorArtifactChecksumStatus] = &[
    DoctorArtifactChecksumStatus::NotRecorded,
    DoctorArtifactChecksumStatus::Matched,
    DoctorArtifactChecksumStatus::Mismatched,
    DoctorArtifactChecksumStatus::Missing,
];

pub(crate) const DOCTOR_DRIFT_DETECTION_STATUS_VOCABULARY: &[DoctorDriftDetectionStatus] = &[
    DoctorDriftDetectionStatus::NotChecked,
    DoctorDriftDetectionStatus::Verified,
    DoctorDriftDetectionStatus::ChecksumMismatch,
    DoctorDriftDetectionStatus::MissingArtifact,
    DoctorDriftDetectionStatus::ManifestMismatch,
];

pub(crate) const DOCTOR_REPAIR_DERIVED_CLEANUP_ASSETS: &[DoctorAssetClass] = &[
    DoctorAssetClass::RetainedPublishBackup,
    DoctorAssetClass::ReclaimableDerivedCache,
    DoctorAssetClass::MemoCache,
];

pub(crate) const DOCTOR_REPAIR_REBUILD_ASSETS: &[DoctorAssetClass] = &[
    DoctorAssetClass::DerivedLexicalIndex,
    DoctorAssetClass::DerivedSemanticIndex,
    DoctorAssetClass::ReclaimableDerivedCache,
];

pub(crate) const DOCTOR_REPAIR_RESTORE_ASSETS: &[DoctorAssetClass] = &[
    DoctorAssetClass::CanonicalArchiveDb,
    DoctorAssetClass::ArchiveDbSidecar,
    DoctorAssetClass::BackupBundle,
];

pub(crate) const DOCTOR_REPAIR_RECONSTRUCT_ASSETS: &[DoctorAssetClass] = &[
    DoctorAssetClass::RawMirrorBlob,
    DoctorAssetClass::CanonicalArchiveDb,
    DoctorAssetClass::ArchiveDbSidecar,
    DoctorAssetClass::OperationReceipt,
    DoctorAssetClass::DerivedLexicalIndex,
    DoctorAssetClass::DerivedSemanticIndex,
];

pub(crate) const DOCTOR_REPAIR_READ_ONLY_FIELDS: &[&str] = &["mode", "outcome_kind"];

pub(crate) const DOCTOR_REPAIR_MUTATION_FIELDS: &[&str] = &[
    "mode",
    "approval_fingerprint",
    "planned_actions",
    "blocked_reasons",
    "outcome_kind",
    "retry_safety",
    "receipt",
];

pub(crate) const DOCTOR_REPAIR_REHEARSAL_FIELDS: &[&str] =
    &["mode", "planned_actions", "blocked_reasons", "outcome_kind"];

pub(crate) const DOCTOR_PLAN_FINGERPRINT_FIELDS: &[&str] = &[
    "plan_kind",
    "schema_version",
    "mode",
    "approval_requirement",
    "approval_fingerprint",
    "outcome_contract",
    "coverage_before",
    "safety_gates",
    "actions",
    "artifact_manifest",
    "selected_authorities",
    "rejected_authorities",
    "blocked_reasons",
    "remaining_risk",
];

pub(crate) const DOCTOR_RECEIPT_REQUIRED_FIELDS: &[&str] = &[
    "receipt_kind",
    "schema_version",
    "mode",
    "outcome_kind",
    "approval_fingerprint",
    "plan_fingerprint",
    "started_at_ms",
    "finished_at_ms",
    "actions",
    "action_status_counts",
    "artifact_manifest",
    "drift_detection_status",
    "coverage_before",
    "coverage_after",
];

pub(crate) const DOCTOR_VERIFICATION_MANIFEST_FIELDS: &[&str] = &[
    "schema_version",
    "scenario_id",
    "command_line",
    "env",
    "binary_version",
    "started_at_ms",
    "finished_at_ms",
    "temp_data_dir_layout",
    "steps",
    "file_inventory_before",
    "file_inventory_after",
    "inventory_before",
    "inventory_after",
    "checksums",
    "receipts",
    "coverage_deltas",
    "redaction_report",
];

pub(crate) const DOCTOR_VERIFICATION_STEP_LOG_FIELDS: &[&str] = &[
    "step_id",
    "scenario_id",
    "command_line",
    "env",
    "exit_code",
    "stdout_path",
    "stderr_path",
    "parsed_json_path",
    "receipt_paths",
    "inventory_before_path",
    "inventory_after_path",
    "duration_ms",
    "redacted_paths",
];

pub(crate) const DOCTOR_VERIFICATION_INVENTORY_FIELDS: &[&str] = &[
    "path",
    "redacted_path",
    "asset_class",
    "size_bytes",
    "mtime_ms",
    "descriptor_blake3",
    "content_blake3",
    "exists",
];

pub(crate) const DOCTOR_VERIFICATION_REDACTION_GUARANTEES: &[&str] = &[
    "raw session content is never copied into manifests by default",
    "support-bundle paths include redacted_path alongside exact local path fields",
    "environment captures must exclude secrets and token-shaped values",
    "stdout/stderr logs used for artifacts must be scrubbed before export",
];

pub(crate) const DOCTOR_REPAIR_READ_ONLY_ABORTS: &[&str] = &["schema_error", "io_error"];

pub(crate) const DOCTOR_REPAIR_MUTATION_ABORTS: &[&str] = &[
    "active_rebuild_lock",
    "approval_fingerprint_missing",
    "approval_fingerprint_mismatched",
    "unsafe_asset_class",
    "unsafe_path",
    "verification_failed",
];

pub(crate) const DOCTOR_REPAIR_EMERGENCY_ABORTS: &[&str] =
    &["always_refused_until_explicitly_implemented"];

pub(crate) const DOCTOR_REPAIR_MODE_POLICY_TABLE: &[DoctorRepairModePolicy] = &[
    DoctorRepairModePolicy {
        mode: DoctorRepairMode::Check,
        mutates: false,
        robot_allowed: true,
        approval_requirement: DoctorApprovalRequirement::None,
        allowed_mutation_asset_classes: DOCTOR_REPAIR_NO_MUTATION_ASSETS,
        required_fields: DOCTOR_REPAIR_READ_ONLY_FIELDS,
        stdout_contract: "stdout contains data only in robot mode",
        stderr_contract: "stderr contains diagnostics only",
        aborts_on: DOCTOR_REPAIR_READ_ONLY_ABORTS,
        notes: "Default cass doctor mode; never mutates user or derived assets.",
    },
    DoctorRepairModePolicy {
        mode: DoctorRepairMode::RepairDryRun,
        mutates: false,
        robot_allowed: true,
        approval_requirement: DoctorApprovalRequirement::None,
        allowed_mutation_asset_classes: DOCTOR_REPAIR_NO_MUTATION_ASSETS,
        required_fields: DOCTOR_REPAIR_REHEARSAL_FIELDS,
        stdout_contract: "stdout describes the plan and fingerprint only",
        stderr_contract: "stderr contains diagnostics only",
        aborts_on: DOCTOR_REPAIR_READ_ONLY_ABORTS,
        notes: "Plans repair work without changing filesystem or archive state.",
    },
    DoctorRepairModePolicy {
        mode: DoctorRepairMode::RepairApply,
        mutates: true,
        robot_allowed: true,
        approval_requirement: DoctorApprovalRequirement::ApprovalFingerprint,
        allowed_mutation_asset_classes: DOCTOR_REPAIR_REBUILD_ASSETS,
        required_fields: DOCTOR_REPAIR_MUTATION_FIELDS,
        stdout_contract: "stdout emits an apply receipt in robot mode",
        stderr_contract: "stderr contains progress and diagnostics only",
        aborts_on: DOCTOR_REPAIR_MUTATION_ABORTS,
        notes: "Applies verified derived-index repairs after matching the dry-run fingerprint.",
    },
    DoctorRepairModePolicy {
        mode: DoctorRepairMode::CleanupDryRun,
        mutates: false,
        robot_allowed: true,
        approval_requirement: DoctorApprovalRequirement::None,
        allowed_mutation_asset_classes: DOCTOR_REPAIR_NO_MUTATION_ASSETS,
        required_fields: DOCTOR_REPAIR_REHEARSAL_FIELDS,
        stdout_contract: "stdout emits cleanup inventory and approval fingerprint",
        stderr_contract: "stderr contains diagnostics only",
        aborts_on: DOCTOR_REPAIR_READ_ONLY_ABORTS,
        notes: "Inventories reclaimable derived assets without pruning anything.",
    },
    DoctorRepairModePolicy {
        mode: DoctorRepairMode::CleanupApply,
        mutates: true,
        robot_allowed: true,
        approval_requirement: DoctorApprovalRequirement::ApprovalFingerprint,
        allowed_mutation_asset_classes: DOCTOR_REPAIR_DERIVED_CLEANUP_ASSETS,
        required_fields: DOCTOR_REPAIR_MUTATION_FIELDS,
        stdout_contract: "stdout emits cleanup apply actions and receipt in robot mode",
        stderr_contract: "stderr contains progress and diagnostics only",
        aborts_on: DOCTOR_REPAIR_MUTATION_ABORTS,
        notes: "Prunes only derived reclaimable assets after path and taxonomy gates pass.",
    },
    DoctorRepairModePolicy {
        mode: DoctorRepairMode::ReconstructDryRun,
        mutates: false,
        robot_allowed: true,
        approval_requirement: DoctorApprovalRequirement::None,
        allowed_mutation_asset_classes: DOCTOR_REPAIR_NO_MUTATION_ASSETS,
        required_fields: DOCTOR_REPAIR_REHEARSAL_FIELDS,
        stdout_contract: "stdout emits reconstruct candidates and promotion fingerprint",
        stderr_contract: "stderr contains diagnostics only",
        aborts_on: DOCTOR_REPAIR_READ_ONLY_ABORTS,
        notes: "Previews archive reconstruction from preserved raw mirrors.",
    },
    DoctorRepairModePolicy {
        mode: DoctorRepairMode::ReconstructPromote,
        mutates: true,
        robot_allowed: false,
        approval_requirement: DoctorApprovalRequirement::ApprovalFingerprint,
        allowed_mutation_asset_classes: DOCTOR_REPAIR_RECONSTRUCT_ASSETS,
        required_fields: DOCTOR_REPAIR_MUTATION_FIELDS,
        stdout_contract: "stdout emits promotion receipt after verification",
        stderr_contract: "stderr contains progress and diagnostics only",
        aborts_on: DOCTOR_REPAIR_MUTATION_ABORTS,
        notes: "Promotes reconstructed archive state only after preserving evidence and matching a plan.",
    },
    DoctorRepairModePolicy {
        mode: DoctorRepairMode::RestoreRehearsal,
        mutates: false,
        robot_allowed: true,
        approval_requirement: DoctorApprovalRequirement::None,
        allowed_mutation_asset_classes: DOCTOR_REPAIR_NO_MUTATION_ASSETS,
        required_fields: DOCTOR_REPAIR_REHEARSAL_FIELDS,
        stdout_contract: "stdout emits restore preflight and rehearsal receipt",
        stderr_contract: "stderr contains diagnostics only",
        aborts_on: DOCTOR_REPAIR_READ_ONLY_ABORTS,
        notes: "Validates restore inputs without replacing archive state.",
    },
    DoctorRepairModePolicy {
        mode: DoctorRepairMode::RestoreApply,
        mutates: true,
        robot_allowed: false,
        approval_requirement: DoctorApprovalRequirement::RehearsalReceipt,
        allowed_mutation_asset_classes: DOCTOR_REPAIR_RESTORE_ASSETS,
        required_fields: DOCTOR_REPAIR_MUTATION_FIELDS,
        stdout_contract: "stdout emits restore receipt after verification",
        stderr_contract: "stderr contains progress and diagnostics only",
        aborts_on: DOCTOR_REPAIR_MUTATION_ABORTS,
        notes: "Requires a rehearsal receipt before touching canonical archive state.",
    },
    DoctorRepairModePolicy {
        mode: DoctorRepairMode::SupportBundle,
        mutates: false,
        robot_allowed: true,
        approval_requirement: DoctorApprovalRequirement::None,
        allowed_mutation_asset_classes: DOCTOR_REPAIR_NO_MUTATION_ASSETS,
        required_fields: DOCTOR_REPAIR_READ_ONLY_FIELDS,
        stdout_contract: "stdout emits bundle metadata in robot mode",
        stderr_contract: "stderr contains diagnostics only",
        aborts_on: DOCTOR_REPAIR_READ_ONLY_ABORTS,
        notes: "May redact/export evidence but does not prune or rewrite archives.",
    },
    DoctorRepairModePolicy {
        mode: DoctorRepairMode::BaselineDiff,
        mutates: false,
        robot_allowed: true,
        approval_requirement: DoctorApprovalRequirement::None,
        allowed_mutation_asset_classes: DOCTOR_REPAIR_NO_MUTATION_ASSETS,
        required_fields: DOCTOR_REPAIR_READ_ONLY_FIELDS,
        stdout_contract: "stdout emits baseline comparison data in robot mode",
        stderr_contract: "stderr contains diagnostics only",
        aborts_on: DOCTOR_REPAIR_READ_ONLY_ABORTS,
        notes: "Compares current state against baselines without mutation.",
    },
    DoctorRepairModePolicy {
        mode: DoctorRepairMode::SafeAutoRun,
        mutates: true,
        robot_allowed: true,
        approval_requirement: DoctorApprovalRequirement::ApprovalFingerprint,
        allowed_mutation_asset_classes: DOCTOR_REPAIR_DERIVED_CLEANUP_ASSETS,
        required_fields: DOCTOR_REPAIR_MUTATION_FIELDS,
        stdout_contract: "stdout emits every sub-action receipt in robot mode",
        stderr_contract: "stderr contains progress and diagnostics only",
        aborts_on: DOCTOR_REPAIR_MUTATION_ABORTS,
        notes: "Legacy --fix maps here and may only apply individually safe, fingerprinted repairs.",
    },
    DoctorRepairModePolicy {
        mode: DoctorRepairMode::EmergencyForce,
        mutates: false,
        robot_allowed: false,
        approval_requirement: DoctorApprovalRequirement::Refused,
        allowed_mutation_asset_classes: DOCTOR_REPAIR_NO_MUTATION_ASSETS,
        required_fields: DOCTOR_REPAIR_READ_ONLY_FIELDS,
        stdout_contract: "stdout emits a refusal envelope",
        stderr_contract: "stderr explains the unsupported override request",
        aborts_on: DOCTOR_REPAIR_EMERGENCY_ABORTS,
        notes: "Declared explicitly so override-shaped requests fail closed instead of improvising.",
    },
];

pub(crate) fn doctor_repair_mode_policy(mode: DoctorRepairMode) -> &'static DoctorRepairModePolicy {
    DOCTOR_REPAIR_MODE_POLICY_TABLE
        .iter()
        .find(|policy| policy.mode == mode)
        .expect("doctor repair mode policy table must cover every mode")
}

pub(crate) fn doctor_repair_mode_allows_asset_mutation(
    mode: DoctorRepairMode,
    asset_class: DoctorAssetClass,
) -> bool {
    doctor_repair_mode_allows_asset_operation_mutation(
        mode,
        asset_class,
        DoctorAssetOperation::PruneReclaim,
    )
}

pub(crate) fn doctor_repair_mode_allows_asset_operation_mutation(
    mode: DoctorRepairMode,
    asset_class: DoctorAssetClass,
    operation: DoctorAssetOperation,
) -> bool {
    let policy = doctor_repair_mode_policy(mode);
    if !policy.mutates
        || !policy.allowed_mutation_asset_classes.contains(&asset_class)
        || !doctor_asset_allows_operation(asset_class, operation)
    {
        return false;
    }
    if operation == DoctorAssetOperation::PruneReclaim {
        return doctor_asset_safe_to_gc(asset_class, true);
    }
    true
}

pub(crate) fn doctor_repair_mode_policy_report() -> Vec<DoctorRepairModePolicyReport> {
    DOCTOR_REPAIR_MODE_POLICY_TABLE
        .iter()
        .map(|policy| DoctorRepairModePolicyReport {
            mode: policy.mode,
            mutates: policy.mutates,
            robot_allowed: policy.robot_allowed,
            approval_requirement: policy.approval_requirement,
            allowed_mutation_asset_classes: policy.allowed_mutation_asset_classes.to_vec(),
            required_fields: policy.required_fields.to_vec(),
            stdout_contract: policy.stdout_contract,
            stderr_contract: policy.stderr_contract,
            aborts_on: policy.aborts_on.to_vec(),
            notes: policy.notes,
        })
        .collect()
}

pub(crate) fn doctor_operation_outcome_policy(
    kind: DoctorOperationOutcomeKind,
) -> &'static DoctorOperationOutcomePolicy {
    DOCTOR_OPERATION_OUTCOME_POLICY_TABLE
        .iter()
        .find(|policy| policy.kind == kind)
        .expect("doctor operation outcome policy table must cover every kind")
}

pub(crate) fn doctor_operation_outcome_contract_report() -> Vec<DoctorOperationOutcomePolicyReport>
{
    DOCTOR_OPERATION_OUTCOME_POLICY_TABLE
        .iter()
        .map(|policy| DoctorOperationOutcomePolicyReport {
            kind: policy.kind,
            reason: policy.reason,
            action_taken: policy.action_taken,
            action_not_taken: policy.action_not_taken,
            safe_to_retry: policy.safe_to_retry,
            requires_override: policy.requires_override,
            data_loss_risk: policy.data_loss_risk,
            next_command: policy.next_command,
            artifact_manifest_path: policy.artifact_manifest_path,
            exit_code_kind: policy.exit_code_kind,
        })
        .collect()
}

pub(crate) fn doctor_operation_outcome_from_policy(
    kind: DoctorOperationOutcomeKind,
) -> DoctorOperationOutcomeReport {
    let policy = doctor_operation_outcome_policy(kind);
    DoctorOperationOutcomeReport {
        kind,
        reason: policy.reason.to_string(),
        action_taken: policy.action_taken.to_string(),
        action_not_taken: policy.action_not_taken.to_string(),
        safe_to_retry: policy.safe_to_retry,
        requires_override: policy.requires_override,
        data_loss_risk: policy.data_loss_risk,
        next_command: policy.next_command.map(str::to_string),
        artifact_manifest_path: policy.artifact_manifest_path.map(str::to_string),
        exit_code_kind: policy.exit_code_kind,
    }
}

pub(crate) fn doctor_operation_outcome_with_details(
    kind: DoctorOperationOutcomeKind,
    reason: String,
    action_taken: String,
    action_not_taken: String,
    data_loss_risk: DoctorDataLossRisk,
    next_command: Option<String>,
    artifact_manifest_path: Option<String>,
) -> DoctorOperationOutcomeReport {
    let mut report = doctor_operation_outcome_from_policy(kind);
    report.reason = reason;
    report.action_taken = action_taken;
    report.action_not_taken = action_not_taken;
    report.data_loss_risk = data_loss_risk;
    report.next_command = next_command;
    report.artifact_manifest_path = artifact_manifest_path;
    report
}

pub(crate) fn doctor_operation_outcome_kind_label(
    kind: DoctorOperationOutcomeKind,
) -> &'static str {
    match kind {
        DoctorOperationOutcomeKind::OkNoActionNeeded => "ok-no-action-needed",
        DoctorOperationOutcomeKind::OkReadOnlyDiagnosed => "ok-read-only-diagnosed",
        DoctorOperationOutcomeKind::Fixed => "fixed",
        DoctorOperationOutcomeKind::PartiallyFixed => "partially-fixed",
        DoctorOperationOutcomeKind::RepairBlocked => "repair-blocked",
        DoctorOperationOutcomeKind::RepairRefused => "repair-refused",
        DoctorOperationOutcomeKind::RepairIncomplete => "repair-incomplete",
        DoctorOperationOutcomeKind::VerificationFailed => "verification-failed",
        DoctorOperationOutcomeKind::CleanupDryRunOnly => "cleanup-dry-run-only",
        DoctorOperationOutcomeKind::CleanupRefused => "cleanup-refused",
        DoctorOperationOutcomeKind::AutoRunSkipped => "auto-run-skipped",
        DoctorOperationOutcomeKind::SupportBundleOnly => "support-bundle-only",
        DoctorOperationOutcomeKind::BaselineDiffOnly => "baseline-diff-only",
        DoctorOperationOutcomeKind::RequiresManualReview => "requires-manual-review",
    }
}

pub(crate) fn doctor_data_loss_risk_label(risk: DoctorDataLossRisk) -> &'static str {
    match risk {
        DoctorDataLossRisk::None => "none",
        DoctorDataLossRisk::Low => "low",
        DoctorDataLossRisk::Medium => "medium",
        DoctorDataLossRisk::High => "high",
        DoctorDataLossRisk::Unknown => "unknown",
    }
}

pub(crate) mod assets;
pub(crate) mod types;
