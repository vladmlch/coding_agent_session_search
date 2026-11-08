use crate::doctor::fs_safety::cleanup_apply::DoctorFsMutationReceipt;
use crate::doctor::policy::types::{
    DoctorAnomaly, DoctorAssetClass, DoctorAssetSafetyClassification, DoctorDataLossRisk,
    DoctorHealth, DoctorSeverity,
};
use crate::doctor::policy::{DoctorActionStatus, DoctorRepairOutcomeKind};
use crate::doctor::repair_state::cliheartbeat::doctor_serde_label;
use crate::doctor::repair_state::types::DoctorOperationStateReport;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorCheckReport {
    pub(crate) name: String,
    pub(crate) status: String,
    pub(crate) message: String,
    pub(crate) anomaly_class: DoctorAnomaly,
    pub(crate) health_class: DoctorHealth,
    pub(crate) severity: DoctorSeverity,
    pub(crate) affected_asset_class: DoctorAssetClass,
    pub(crate) data_loss_risk: DoctorDataLossRisk,
    pub(crate) recommended_action: &'static str,
    pub(crate) safe_for_auto_repair: bool,
    pub(crate) default_outcome_kind: DoctorRepairOutcomeKind,
    pub(crate) fix_available: bool,
    pub(crate) fix_applied: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DoctorSafeAutoRunDecisionKind {
    Applied,
    Eligible,
    Skipped,
    Blocked,
    ManualApprovalRequired,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorSafeAutoRunFinding {
    pub(crate) finding: String,
    pub(crate) action: String,
    pub(crate) decision: DoctorSafeAutoRunDecisionKind,
    pub(crate) asset_class: DoctorAssetClass,
    pub(crate) risk_class: DoctorDataLossRisk,
    pub(crate) why_safe: Option<String>,
    pub(crate) why_blocked: Option<String>,
    pub(crate) why_manual_approval_required: Option<String>,
    pub(crate) next_exact_command: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorSafeAutoRunReceiptReference {
    pub(crate) receipt_kind: String,
    pub(crate) action_id: String,
    pub(crate) status: DoctorActionStatus,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorSafeAutoRunReport {
    pub(crate) schema_version: u32,
    pub(crate) enabled: bool,
    pub(crate) mode: String,
    pub(crate) evaluated_findings: Vec<DoctorSafeAutoRunFinding>,
    pub(crate) auto_apply_eligibility: bool,
    pub(crate) why_safe: Vec<String>,
    pub(crate) why_blocked: Vec<String>,
    pub(crate) why_manual_approval_required: Vec<String>,
    pub(crate) skipped_due_to_lock_or_unknown: bool,
    pub(crate) next_exact_command: Option<String>,
    pub(crate) applied_actions: Vec<String>,
    pub(crate) blocked_actions: Vec<String>,
    pub(crate) manual_approval_required: Vec<String>,
    pub(crate) receipt_action_ids: Vec<String>,
    pub(crate) receipts: Vec<DoctorSafeAutoRunReceiptReference>,
    pub(crate) event_log_reference: String,
}

pub(crate) struct DoctorSafeAutoRunBuildInput<'a> {
    pub(crate) enabled: bool,
    pub(crate) check_reports: &'a [DoctorCheckReport],
    pub(crate) auto_fix_actions: &'a [String],
    pub(crate) fs_mutation_receipts: &'a [DoctorFsMutationReceipt],
    pub(crate) operation_state: &'a DoctorOperationStateReport,
    pub(crate) needs_rebuild: bool,
    pub(crate) archive_queryable_for_non_destructive_derived_rebuild: bool,
    pub(crate) db_messages: Option<usize>,
    pub(crate) event_log_reference: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorDerivedSemanticComponentReport {
    pub(crate) asset_class: DoctorAssetClass,
    pub(crate) safety_classification: DoctorAssetSafetyClassification,
    pub(crate) status: String,
    pub(crate) safe_to_rebuild: bool,
    pub(crate) safe_for_auto_repair: bool,
    pub(crate) blocks_archive_recovery: bool,
    pub(crate) network_allowed: bool,
    pub(crate) auto_download_attempted: bool,
    pub(crate) path: Option<String>,
    pub(crate) redacted_path: Option<String>,
    pub(crate) current_db_matches: Option<bool>,
    pub(crate) ready: Option<bool>,
    pub(crate) present: Option<bool>,
    pub(crate) note: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorDerivedSemanticAssetReport {
    pub(crate) schema_version: u32,
    pub(crate) status: String,
    pub(crate) doctor_check_status: String,
    pub(crate) asset_class: DoctorAssetClass,
    pub(crate) safety_classification: DoctorAssetSafetyClassification,
    pub(crate) fallback_mode: String,
    pub(crate) semantic_status: String,
    pub(crate) semantic_availability: String,
    pub(crate) can_search: bool,
    pub(crate) lexical_search_unblocked: bool,
    pub(crate) blocks_archive_recovery: bool,
    pub(crate) safe_to_rebuild: bool,
    pub(crate) safe_for_auto_repair: bool,
    pub(crate) network_allowed: bool,
    pub(crate) auto_download_allowed: bool,
    pub(crate) auto_download_attempted: bool,
    pub(crate) skipped_auto_download_reason: Option<String>,
    pub(crate) model_policy: String,
    pub(crate) selected_repair_action: String,
    pub(crate) recommended_action: String,
    pub(crate) probe_duration_ms: u64,
    pub(crate) redaction_status: String,
    pub(crate) receipt_path: Option<String>,
    pub(crate) model_cache: DoctorDerivedSemanticComponentReport,
    pub(crate) vector_index: DoctorDerivedSemanticComponentReport,
    pub(crate) hnsw_index: DoctorDerivedSemanticComponentReport,
    pub(crate) memoization_cache: DoctorDerivedSemanticComponentReport,
    pub(crate) notes: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum DoctorIncidentRootCauseKind {
    DerivedIndexStale,
    DerivedIndexStaleAfterDbPromotion,
    SemanticModelMissingLexicalFallback,
    SourcePrunedWithMirrorIntact,
    MirrorMissingWithDbSoleCopy,
    ArchiveDbUnreadableWithValidCandidate,
    ArchiveDbUnreadable,
    ActiveLockBlockingRepair,
    InterruptedRepairState,
    StoragePressureDerivedCleanupAvailable,
    BackupExclusionRisk,
    BackupUnverified,
    PreviousRepairFailed,
    Unknown,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorRootCauseIncident {
    pub(crate) schema_version: u32,
    pub(crate) incident_id: String,
    pub(crate) root_cause_kind: DoctorIncidentRootCauseKind,
    pub(crate) severity: DoctorSeverity,
    pub(crate) affected_asset_classes: Vec<DoctorAssetClass>,
    pub(crate) archive_risk_level: DoctorDataLossRisk,
    pub(crate) derived_risk_level: DoctorDataLossRisk,
    pub(crate) confidence: String,
    pub(crate) evidence_check_ids: Vec<String>,
    pub(crate) blocked_actions: Vec<String>,
    pub(crate) safe_next_actions: Vec<String>,
    pub(crate) stale_or_unknown_fields: Vec<String>,
    pub(crate) redacted_evidence_paths: Vec<String>,
    pub(crate) summary: String,
}

impl DoctorCheckReport {
    pub(crate) fn anomaly_class_label(&self) -> String {
        doctor_serde_label(self.anomaly_class)
    }
}
