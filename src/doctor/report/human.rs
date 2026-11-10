use crate::cli::error::WrapConfig;
use crate::cli::output::emit_correction_notes::apply_wrap;
use crate::doctor::policy::types::{DoctorAssetClass, DoctorDataLossRisk, DoctorHealth};
use crate::doctor::policy::{
    DOCTOR_ACTION_STATUS_VOCABULARY, DOCTOR_ARTIFACT_CHECKSUM_STATUS_VOCABULARY,
    DOCTOR_DRIFT_DETECTION_STATUS_VOCABULARY, DOCTOR_OPERATION_OUTCOME_KIND_VOCABULARY,
    DOCTOR_PLAN_FINGERPRINT_FIELDS, DOCTOR_RECEIPT_REQUIRED_FIELDS,
    DOCTOR_REPAIR_APPROVAL_REQUIREMENT_VOCABULARY, DOCTOR_REPAIR_OUTCOME_KIND_VOCABULARY,
    DOCTOR_REPAIR_RETRY_SAFETY_VOCABULARY, DOCTOR_VERIFICATION_INVENTORY_FIELDS,
    DOCTOR_VERIFICATION_MANIFEST_FIELDS, DOCTOR_VERIFICATION_REDACTION_GUARANTEES,
    DOCTOR_VERIFICATION_STEP_LOG_FIELDS, DoctorOperationOutcomeReport,
    DoctorPlanReceiptSchemaReport, DoctorRepairContractReport, DoctorRepairLegacyAliasReport,
    DoctorRepairMode, DoctorRepairPlanPreviewReport, DoctorVerificationContractReport,
    DoctorVerificationMatrixEntry, doctor_data_loss_risk_label,
    doctor_operation_outcome_contract_report, doctor_operation_outcome_kind_label,
    doctor_repair_mode_policy_report,
};
use crate::doctor::repair_state::cliheartbeat::doctor_serde_label;
use crate::doctor::repair_state::finalize_cleanup_apply_contract::DiagCleanupApplyResult;
use crate::doctor::report::checks::doctor_data_loss_risk_rank;
use crate::doctor::report::inventory::DoctorSoleCopyWarning;
use crate::doctor::report::types::{DoctorDerivedSemanticAssetReport, DoctorRootCauseIncident};

pub(crate) fn print_doctor_human_line(wrap: WrapConfig, line: impl AsRef<str>) {
    println!("{}", apply_wrap(line.as_ref(), wrap));
}

pub(crate) fn doctor_human_highest_incident_risk(
    incidents: &[DoctorRootCauseIncident],
    extract: impl Fn(&DoctorRootCauseIncident) -> DoctorDataLossRisk,
) -> DoctorDataLossRisk {
    incidents
        .iter()
        .map(extract)
        .max_by_key(|risk| doctor_data_loss_risk_rank(*risk))
        .unwrap_or(DoctorDataLossRisk::None)
}

pub(crate) fn doctor_human_semantic_fallback_summary(
    derived_semantic_assets: &DoctorDerivedSemanticAssetReport,
) -> Option<String> {
    if !matches!(
        derived_semantic_assets.asset_class,
        DoctorAssetClass::ModelCache
            | DoctorAssetClass::DerivedSemanticIndex
            | DoctorAssetClass::MemoCache
    ) {
        return None;
    }
    if derived_semantic_assets.fallback_mode != "lexical"
        || derived_semantic_assets.semantic_status == "ready"
    {
        return None;
    }

    Some(format!(
        "Semantic search: {}. This is derived-search state, not archive damage; cass never acquires models during doctor. Next optional command: cass models install --from-file <dir>, then cass index --semantic --json.",
        derived_semantic_assets.recommended_action
    ))
}

pub(crate) struct DoctorHumanRiskSummaryInput<'a> {
    pub(crate) wrap: WrapConfig,
    pub(crate) doctor_status: &'a str,
    pub(crate) health_class: DoctorHealth,
    pub(crate) risk_level: &'a str,
    pub(crate) recommended_action: Option<&'a String>,
    pub(crate) incidents: &'a [DoctorRootCauseIncident],
    pub(crate) sole_copy_warnings: &'a [DoctorSoleCopyWarning],
    pub(crate) derived_semantic_assets: &'a DoctorDerivedSemanticAssetReport,
    pub(crate) repair_plan: Option<&'a DoctorRepairPlanPreviewReport>,
    pub(crate) cleanup_apply_result: Option<&'a DiagCleanupApplyResult>,
    pub(crate) operation_outcome: &'a DoctorOperationOutcomeReport,
    pub(crate) not_initialized: bool,
}

pub(crate) fn print_doctor_human_risk_summary(input: DoctorHumanRiskSummaryInput<'_>) {
    use colored::Colorize;

    let archive_risk = if input.not_initialized {
        DoctorDataLossRisk::Unknown
    } else {
        doctor_human_highest_incident_risk(input.incidents, |incident| incident.archive_risk_level)
    };
    let derived_risk = if input.not_initialized {
        DoctorDataLossRisk::Unknown
    } else {
        doctor_human_highest_incident_risk(input.incidents, |incident| incident.derived_risk_level)
    };
    let next_action = input
        .operation_outcome
        .next_command
        .as_deref()
        .or(input.recommended_action.map(String::as_str));

    println!();
    print_doctor_human_line(input.wrap, "Risk and next actions:".bold().to_string());
    print_doctor_human_line(
        input.wrap,
        format!(
            "  status: {}; health={}; risk={}; archive_risk={}; derived_index_risk={}",
            input.doctor_status,
            doctor_serde_label(input.health_class),
            input.risk_level,
            doctor_data_loss_risk_label(archive_risk),
            doctor_data_loss_risk_label(derived_risk)
        ),
    );
    print_doctor_human_line(
        input.wrap,
        "  Safety: doctor will not delete source session logs, raw mirrors, archive DBs, SQLite sidecars, backups, receipts, configs, bookmarks, or source evidence automatically.",
    );

    if input.sole_copy_warnings.is_empty() {
        print_doctor_human_line(
            input.wrap,
            "  Sole-copy warning: none identified by the current coverage ledger.",
        );
    } else {
        print_doctor_human_line(
            input.wrap,
            format!(
                "  Sole-copy warning: cass may be the only remaining archival copy for {} conversation(s); back up the cass data directory before any source-only rebuild, cleanup, restore, or repair.",
                input.sole_copy_warnings.len()
            ),
        );
    }

    if !input.not_initialized
        && let Some(summary) = doctor_human_semantic_fallback_summary(input.derived_semantic_assets)
    {
        print_doctor_human_line(input.wrap, format!("  {summary}"));
    }

    if let Some(action) = next_action {
        print_doctor_human_line(input.wrap, format!("  Next safe command: {action}"));
    }

    if let Some(plan) = input.repair_plan {
        print_doctor_human_line(input.wrap, "Repair plan:".bold().to_string());
        print_doctor_human_line(
            input.wrap,
            format!("  plan_fingerprint: {}", plan.plan_fingerprint),
        );
        print_doctor_human_line(
            input.wrap,
            format!("  exact apply command: {}", plan.exact_apply_command),
        );
        print_doctor_human_line(
            input.wrap,
            "  Receipt expectation: apply emits fingerprinted receipts, event-log evidence, and artifact checksums before doctor claims success.",
        );
    }

    if let Some(result) = input.cleanup_apply_result {
        print_doctor_human_line(input.wrap, "Cleanup receipt:".bold().to_string());
        print_doctor_human_line(
            input.wrap,
            format!(
                "  approval_fingerprint: {}; applied={}; reclaimed_bytes={}",
                result.approval_fingerprint, result.applied, result.reclaimed_bytes
            ),
        );
        print_doctor_human_line(
            input.wrap,
            format!(
                "  receipt plan_fingerprint: {}; receipt outcome={}",
                result.receipt.plan_fingerprint,
                doctor_serde_label(result.receipt.outcome_kind)
            ),
        );
    }
}

pub(crate) fn print_doctor_operation_outcome_human(
    outcome: &DoctorOperationOutcomeReport,
    wrap: WrapConfig,
) {
    use colored::Colorize;

    println!();
    print_doctor_human_line(wrap, "Operation outcome:".bold().to_string());
    print_doctor_human_line(
        wrap,
        format!(
            "  kind: {}",
            doctor_operation_outcome_kind_label(outcome.kind).bold()
        ),
    );
    print_doctor_human_line(wrap, format!("  reason: {}", outcome.reason));
    print_doctor_human_line(wrap, format!("  action_taken: {}", outcome.action_taken));
    print_doctor_human_line(
        wrap,
        format!("  action_not_taken: {}", outcome.action_not_taken),
    );
    print_doctor_human_line(
        wrap,
        format!(
            "  data_loss_risk: {}",
            doctor_data_loss_risk_label(outcome.data_loss_risk)
        ),
    );
    if let Some(next_command) = &outcome.next_command {
        print_doctor_human_line(wrap, format!("  next_command: {next_command}"));
    }
    if let Some(artifact_manifest_path) = &outcome.artifact_manifest_path {
        print_doctor_human_line(
            wrap,
            format!("  artifact_manifest_path: {artifact_manifest_path}"),
        );
    }
}

pub(crate) fn doctor_plan_receipt_schema_report() -> DoctorPlanReceiptSchemaReport {
    DoctorPlanReceiptSchemaReport {
        plan_schema_version: 1,
        receipt_schema_version: 1,
        action_schema_version: 1,
        artifact_schema_version: 1,
        fingerprint_algorithm: "blake3-canonical-json-v1",
        plan_fingerprint_includes: DOCTOR_PLAN_FINGERPRINT_FIELDS.to_vec(),
        receipt_required_fields: DOCTOR_RECEIPT_REQUIRED_FIELDS.to_vec(),
        action_statuses: DOCTOR_ACTION_STATUS_VOCABULARY.to_vec(),
        artifact_checksum_statuses: DOCTOR_ARTIFACT_CHECKSUM_STATUS_VOCABULARY.to_vec(),
        drift_detection_statuses: DOCTOR_DRIFT_DETECTION_STATUS_VOCABULARY.to_vec(),
        redaction_contract: "robot receipts may expose existing doctor paths; support bundles must redact before export",
        tamper_evidence_scope: "plan fingerprints, artifact manifests, event logs, and forensic bundles",
    }
}

pub(crate) fn doctor_verification_contract_report() -> DoctorVerificationContractReport {
    DoctorVerificationContractReport {
        schema_version: 1,
        artifact_directory_contract: "each scripted doctor e2e run writes one durable artifact directory named <timestamp>-<scenario_id> with manifest.json at the root and one subdirectory per command step",
        manifest_file: "manifest.json",
        per_step_log_contract: "each step captures command/env metadata, stdout, stderr, parsed JSON, before/after inventories, checksums, receipts, timing, and redaction report paths",
        required_manifest_fields: DOCTOR_VERIFICATION_MANIFEST_FIELDS.to_vec(),
        required_step_log_fields: DOCTOR_VERIFICATION_STEP_LOG_FIELDS.to_vec(),
        required_inventory_fields: DOCTOR_VERIFICATION_INVENTORY_FIELDS.to_vec(),
        required_receipt_fields: DOCTOR_RECEIPT_REQUIRED_FIELDS.to_vec(),
        required_redaction_guarantees: DOCTOR_VERIFICATION_REDACTION_GUARANTEES.to_vec(),
        matrix: vec![
            DoctorVerificationMatrixEntry {
                scenario_id: "no_delete_default_check",
                feature_area: "default non-destructive doctor",
                proof_layers: vec!["unit", "cli", "golden", "scripted_e2e"],
                representative_commands: vec!["cass doctor --json"],
                required_artifacts: vec![
                    "manifest.json",
                    "step-001/stdout.json",
                    "step-001/stderr.txt",
                    "step-001/inventory-before.json",
                    "step-001/inventory-after.json",
                ],
                mutation_audit_required: true,
                redaction_required: true,
                archive_preservation_assertion: "inventory_before and inventory_after must prove no archive, source, or user-state path was removed or rewritten",
                notes: "This is the baseline guardrail: doctor with no repair mode is always read-only.",
            },
            DoctorVerificationMatrixEntry {
                scenario_id: "upstream_pruned_archive_survives",
                feature_area: "archive preservation after provider log pruning",
                proof_layers: vec!["integration", "scripted_e2e"],
                representative_commands: vec![
                    "cass index --full --json",
                    "cass doctor --json",
                    "cass search <known archived text> --json",
                ],
                required_artifacts: vec![
                    "manifest.json",
                    "provider-inventory-before.json",
                    "provider-inventory-after-prune.json",
                    "cass-archive-query.json",
                ],
                mutation_audit_required: true,
                redaction_required: true,
                archive_preservation_assertion: "cass archive DB remains queryable after upstream session files disappear",
                notes: "Proves cass is a durable archive and not merely a live provider-log view.",
            },
            DoctorVerificationMatrixEntry {
                scenario_id: "corrupt_db_repair_plan",
                feature_area: "corruption diagnosis and repair planning",
                proof_layers: vec!["unit", "fault_injection", "scripted_e2e"],
                representative_commands: vec!["cass doctor --json", "cass doctor --fix --json"],
                required_artifacts: vec![
                    "manifest.json",
                    "corrupt-db-before.bin",
                    "doctor-plan.json",
                    "doctor-receipt.json",
                    "inventory-after.json",
                ],
                mutation_audit_required: true,
                redaction_required: true,
                archive_preservation_assertion: "any corrupted archive bundle is backed up or quarantined before derived rebuild work proceeds",
                notes: "Fault-injection proof must be diagnosable without rerunning against the user's real archive.",
            },
            DoctorVerificationMatrixEntry {
                scenario_id: "stale_lock_and_active_rebuild",
                feature_area: "lock ownership and interrupted operation handling",
                proof_layers: vec!["unit", "integration", "scripted_e2e"],
                representative_commands: vec![
                    "cass health --json",
                    "cass doctor --json",
                    "cass doctor --fix --json",
                ],
                required_artifacts: vec![
                    "manifest.json",
                    "lock-state-before.json",
                    "doctor-output.json",
                    "lock-state-after.json",
                ],
                mutation_audit_required: true,
                redaction_required: true,
                archive_preservation_assertion: "active rebuild locks block mutating cleanup and repairs; stale-lock handling records the owner and decision",
                notes: "Prevents two doctor processes from racing repairs or cleanup.",
            },
            DoctorVerificationMatrixEntry {
                scenario_id: "restore_rehearsal_then_apply",
                feature_area: "backup verification and restore",
                proof_layers: vec!["unit", "integration", "scripted_e2e"],
                representative_commands: vec![
                    "cass doctor backups restore <id> --dry-run --json",
                    "cass doctor backups restore <id> --yes --json",
                ],
                required_artifacts: vec![
                    "manifest.json",
                    "restore-rehearsal-receipt.json",
                    "restore-apply-receipt.json",
                    "archive-query-after-restore.json",
                ],
                mutation_audit_required: true,
                redaction_required: true,
                archive_preservation_assertion: "restore apply must require a rehearsal receipt and must verify archive readability after replacement",
                notes: "Restore tests are intentionally separate from cleanup tests because they can touch canonical archive state.",
            },
            DoctorVerificationMatrixEntry {
                scenario_id: "derived_cleanup_fingerprint_apply",
                feature_area: "derived cleanup dry-run/apply",
                proof_layers: vec!["unit", "cli", "golden", "scripted_e2e"],
                representative_commands: vec![
                    "cass doctor --json",
                    "cass doctor --fix --json",
                    "cass diag --json --quarantine",
                ],
                required_artifacts: vec![
                    "manifest.json",
                    "cleanup-plan.json",
                    "cleanup-receipt.json",
                    "inventory-before.json",
                    "inventory-after.json",
                ],
                mutation_audit_required: true,
                redaction_required: true,
                archive_preservation_assertion: "only derived reclaimable assets may disappear; source logs, archive DB, receipts, bundles, and quarantined evidence remain",
                notes: "This scenario owns the before/after filesystem mutation audit contract.",
            },
            DoctorVerificationMatrixEntry {
                scenario_id: "semantic_fallback_no_archive_damage",
                feature_area: "semantic model and vector readiness",
                proof_layers: vec!["unit", "integration", "golden"],
                representative_commands: vec![
                    "cass models status --json",
                    "cass health --json",
                    "cass search <query> --json --robot-meta",
                ],
                required_artifacts: vec![
                    "manifest.json",
                    "models-status.json",
                    "health.json",
                    "search-robot-meta.json",
                ],
                mutation_audit_required: false,
                redaction_required: true,
                archive_preservation_assertion: "missing semantic assets report lexical fallback and never imply archive corruption",
                notes: "Semantic fallback is readiness information, not a destructive repair path.",
            },
            DoctorVerificationMatrixEntry {
                scenario_id: "local_mirror_source_coverage",
                feature_area: "local mirrors and multi-machine archive provenance",
                proof_layers: vec!["integration", "scripted_e2e"],
                representative_commands: vec![
                    "cass sources list --json",
                    "cass sources doctor --json",
                    "cass doctor --json",
                ],
                required_artifacts: vec![
                    "manifest.json",
                    "sources-before.json",
                    "coverage-ledger-after.json",
                    "doctor-output.json",
                ],
                mutation_audit_required: true,
                redaction_required: true,
                archive_preservation_assertion: "local mirror coverage ledgers update without deleting local archive evidence",
                notes: "Remote transports were removed; this keeps configured-remote diagnosis local-only and distinct from local archive safety.",
            },
        ],
    }
}

pub(crate) fn doctor_repair_contract_report() -> DoctorRepairContractReport {
    DoctorRepairContractReport {
        default_mode: DoctorRepairMode::Check,
        default_non_destructive: true,
        fail_closed: true,
        plan_receipt_schema: doctor_plan_receipt_schema_report(),
        verification_contract: doctor_verification_contract_report(),
        approval_requirements: DOCTOR_REPAIR_APPROVAL_REQUIREMENT_VOCABULARY.to_vec(),
        outcome_kinds: DOCTOR_REPAIR_OUTCOME_KIND_VOCABULARY.to_vec(),
        operation_outcome_kinds: DOCTOR_OPERATION_OUTCOME_KIND_VOCABULARY.to_vec(),
        operation_outcome_contract: doctor_operation_outcome_contract_report(),
        retry_safety_kinds: DOCTOR_REPAIR_RETRY_SAFETY_VOCABULARY.to_vec(),
        mode_policies: doctor_repair_mode_policy_report(),
        legacy_aliases: vec![
            DoctorRepairLegacyAliasReport {
                invocation: "cass doctor",
                mode: DoctorRepairMode::Check,
                notes: "default inspection path; no mutation",
            },
            DoctorRepairLegacyAliasReport {
                invocation: "cass doctor --json",
                mode: DoctorRepairMode::Check,
                notes: "robot inspection path; stdout is JSON data only",
            },
            DoctorRepairLegacyAliasReport {
                invocation: "cass doctor --fix",
                mode: DoctorRepairMode::SafeAutoRun,
                notes: "legacy repair entrypoint; each mutating sub-action must still carry its own mode and fingerprint",
            },
            DoctorRepairLegacyAliasReport {
                invocation: "cass doctor --fix --force-rebuild",
                mode: DoctorRepairMode::RepairApply,
                notes: "legacy forced derived-index rebuild request; canonical archives remain protected",
            },
        ],
    }
}
