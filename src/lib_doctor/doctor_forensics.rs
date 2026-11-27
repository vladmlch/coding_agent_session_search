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

pub(crate) fn doctor_now_ms() -> i64 {
    system_time_to_unix_ms(std::time::SystemTime::now()).unwrap_or(0)
}

pub(crate) fn doctor_canonical_json_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.into_iter().map(doctor_canonical_json_value).collect())
        }
        serde_json::Value::Object(map) => {
            let mut entries: Vec<_> = map.into_iter().collect();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            let mut canonical = serde_json::Map::new();
            for (key, value) in entries {
                canonical.insert(key, doctor_canonical_json_value(value));
            }
            serde_json::Value::Object(canonical)
        }
        other => other,
    }
}

pub(crate) fn doctor_canonical_blake3(prefix: &str, value: serde_json::Value) -> String {
    let canonical = doctor_canonical_json_value(value);
    let encoded = serde_json::to_vec(&canonical).unwrap_or_default();
    let mut hasher = blake3::Hasher::new();
    hasher.update(prefix.as_bytes());
    hasher.update(&[0]);
    hasher.update(&encoded);
    format!("{prefix}-{}", hasher.finalize().to_hex())
}

pub(crate) fn doctor_redacted_path(path: &str, data_dir: &Path) -> String {
    fn portable_display(path: &Path) -> String {
        path.display().to_string().replace('\\', "/")
    }

    let path_ref = Path::new(path);
    if let Ok(relative) = path_ref.strip_prefix(data_dir) {
        if relative.as_os_str().is_empty() {
            return "[cass-data]".to_string();
        }
        return format!("[cass-data]/{}", portable_display(relative));
    }
    path_ref
        .file_name()
        .map(|name| format!("[external]/{}", name.to_string_lossy()))
        .unwrap_or_else(|| "[external]".to_string())
}

pub(crate) fn doctor_redacted_text(text: &str, data_dir: &Path) -> String {
    let data_dir_text = data_dir.display().to_string();
    text.replace(&data_dir_text, "[cass-data]")
}

pub(crate) fn doctor_forensic_bundle_uncaptured(status: &str) -> DoctorForensicBundleMetadata {
    DoctorForensicBundleMetadata {
        status: status.to_string(),
        sidecar_complete: true,
        redaction_status: "not_applicable".to_string(),
        ..DoctorForensicBundleMetadata::default()
    }
}

pub(crate) fn doctor_forensic_bundle_failed(
    reason: impl Into<String>,
) -> DoctorForensicBundleMetadata {
    DoctorForensicBundleMetadata {
        status: "failed".to_string(),
        sidecar_complete: false,
        redaction_status: "not_applicable".to_string(),
        blocked_reason: Some(reason.into()),
        ..DoctorForensicBundleMetadata::default()
    }
}

pub(crate) fn doctor_forensic_bundle_root(data_dir: &Path) -> PathBuf {
    data_dir.join("doctor").join("forensic-bundles")
}

pub(crate) fn doctor_forensic_create_private_dir(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;

        let mut builder = std::fs::DirBuilder::new();
        builder.mode(0o700);
        builder.create(path)?;
        let meta = std::fs::symlink_metadata(path)?;
        if meta.file_type().is_symlink() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "refusing to use a symlink as a forensic directory",
            ));
        }
        Ok(())
    }

    #[cfg(not(unix))]
    {
        std::fs::create_dir(path)
    }
}

pub(crate) fn doctor_forensic_create_private_dir_all(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt, PermissionsExt};

        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true).mode(0o700);
        builder.create(path)?;
        let meta = std::fs::symlink_metadata(path)?;
        if meta.file_type().is_symlink() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "refusing to use a symlink as a forensic directory",
            ));
        }
        if meta.permissions().mode() & 0o777 != 0o700 {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
        }
        Ok(())
    }

    #[cfg(not(unix))]
    {
        std::fs::create_dir_all(path)
    }
}

pub(crate) fn doctor_forensic_create_private_file(path: &Path) -> io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.mode(0o600);
        let file = options.open(path)?;
        Ok(file)
    }

    #[cfg(not(unix))]
    {
        options.open(path)
    }
}

pub(crate) fn doctor_forensic_bundle_id(operation_id: &str, created_at_ms: i64) -> String {
    let mut safe_operation: String = operation_id
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .take(24)
        .collect();
    if safe_operation.is_empty() {
        safe_operation.push_str("operation");
    }
    format!("{created_at_ms}-{safe_operation}")
}

pub(crate) fn doctor_forensic_relative_path_is_safe(relative_path: &Path) -> bool {
    let raw = relative_path.as_os_str().to_string_lossy();
    !raw.is_empty()
        && !raw.contains('\\')
        && !raw.contains(':')
        && !relative_path.is_absolute()
        && relative_path.components().all(|component| match component {
            std::path::Component::Normal(name) => doctor_portable_relative_component_is_safe(name),
            std::path::Component::CurDir => true,
            _ => false,
        })
}

pub(crate) fn doctor_forensic_portable_child_path(prefix: &str, child: &Path) -> Option<PathBuf> {
    if prefix.is_empty() || prefix.contains('\\') || prefix.contains(':') {
        return None;
    }
    let mut relative = String::with_capacity(prefix.len() + 64);
    relative.push_str(prefix);
    for component in child.components() {
        match component {
            std::path::Component::Normal(name)
                if doctor_portable_relative_component_is_safe(name) =>
            {
                relative.push('/');
                relative.push_str(name.to_str()?);
            }
            std::path::Component::CurDir => {}
            _ => return None,
        }
    }
    let relative = PathBuf::from(relative);
    doctor_forensic_relative_path_is_safe(&relative).then_some(relative)
}

pub(crate) fn doctor_portable_relative_component_is_safe(name: &std::ffi::OsStr) -> bool {
    let text = name.to_string_lossy();
    if text.is_empty()
        || text.contains('\\')
        || text.contains(':')
        || text.ends_with(' ')
        || text.ends_with('.')
        || text.chars().any(char::is_control)
    {
        return false;
    }
    let stem = text
        .split('.')
        .next()
        .unwrap_or(text.as_ref())
        .trim_end_matches(' ');
    let upper = stem.to_ascii_uppercase();
    !matches!(
        upper.as_str(),
        "CON"
            | "PRN"
            | "AUX"
            | "NUL"
            | "CONIN$"
            | "CONOUT$"
            | "COM1"
            | "COM2"
            | "COM3"
            | "COM4"
            | "COM5"
            | "COM6"
            | "COM7"
            | "COM8"
            | "COM9"
            | "LPT1"
            | "LPT2"
            | "LPT3"
            | "LPT4"
            | "LPT5"
            | "LPT6"
            | "LPT7"
            | "LPT8"
            | "LPT9"
    )
}

pub(crate) fn doctor_forensic_bundle_root_is_safe(
    data_dir: &Path,
    root: &Path,
) -> Result<(), String> {
    let data_dir_metadata = std::fs::symlink_metadata(data_dir).map_err(|err| {
        format!(
            "cannot inspect data directory {} before forensic bundle capture: {err}",
            data_dir.display()
        )
    })?;
    if !data_dir_metadata.is_dir() || data_dir_metadata.file_type().is_symlink() {
        return Err(format!(
            "refusing forensic bundle capture through unsafe data directory {}",
            data_dir.display()
        ));
    }
    if !root.starts_with(data_dir) {
        return Err(format!(
            "refusing forensic bundle root outside data directory: {}",
            root.display()
        ));
    }
    if let Ok(root_metadata) = std::fs::symlink_metadata(root)
        && (!root_metadata.is_dir() || root_metadata.file_type().is_symlink())
    {
        return Err(format!(
            "refusing unsafe forensic bundle root {}",
            root.display()
        ));
    }
    if let Some(parent) = root.parent()
        && existing_path_has_symlink_below_root(parent, data_dir)
    {
        return Err(format!(
            "refusing forensic bundle root with symlinked ancestor {}",
            root.display()
        ));
    }
    Ok(())
}

pub(crate) fn doctor_sqlite_sidecar_path(db_path: &Path, suffix: &str) -> Option<PathBuf> {
    let mut file_name = db_path.file_name()?.to_os_string();
    file_name.push(suffix);
    Some(db_path.with_file_name(file_name))
}

pub(crate) struct DoctorForensicCopySpec<'a> {
    pub(crate) artifact_kind: &'a str,
    pub(crate) source_path: &'a Path,
    pub(crate) relative_path: &'a Path,
    pub(crate) required: bool,
    pub(crate) sidecar_group: Option<&'a str>,
}

pub(crate) fn doctor_forensic_source_artifact(
    data_dir: &Path,
    spec: &DoctorForensicCopySpec<'_>,
) -> DoctorForensicBundleArtifact {
    let source_path_text = spec.source_path.display().to_string();
    DoctorForensicBundleArtifact {
        artifact_kind: spec.artifact_kind.to_string(),
        source_path: source_path_text.clone(),
        redacted_source_path: doctor_redacted_path(&source_path_text, data_dir),
        bundle_relative_path: Some(spec.relative_path.display().to_string()),
        exists: false,
        copied: false,
        required: spec.required,
        sidecar_group: spec.sidecar_group.map(str::to_string),
        size_bytes: None,
        checksum_blake3: None,
        error: None,
    }
}

pub(crate) fn doctor_forensic_generated_artifact(
    artifact_kind: &str,
    relative_path: &Path,
    size_bytes: u64,
    checksum_blake3: String,
) -> DoctorForensicBundleArtifact {
    DoctorForensicBundleArtifact {
        artifact_kind: artifact_kind.to_string(),
        source_path: format!("generated:{artifact_kind}"),
        redacted_source_path: format!("generated:{artifact_kind}"),
        bundle_relative_path: Some(relative_path.display().to_string()),
        exists: true,
        copied: true,
        required: true,
        sidecar_group: None,
        size_bytes: Some(size_bytes),
        checksum_blake3: Some(checksum_blake3),
        error: None,
    }
}

pub(crate) fn doctor_forensic_record_missing_artifact(
    artifacts: &mut Vec<DoctorForensicBundleArtifact>,
    data_dir: &Path,
    artifact_kind: &str,
    source_path: &Path,
    relative_path: &Path,
    required: bool,
    sidecar_group: Option<&str>,
) {
    artifacts.push(doctor_forensic_source_artifact(
        data_dir,
        &DoctorForensicCopySpec {
            artifact_kind,
            source_path,
            relative_path,
            required,
            sidecar_group,
        },
    ));
}

pub(crate) fn doctor_forensic_copy_file_artifact(
    artifacts: &mut Vec<DoctorForensicBundleArtifact>,
    bundle_dir: &Path,
    data_dir: &Path,
    spec: DoctorForensicCopySpec<'_>,
) -> Result<(), String> {
    if !doctor_forensic_relative_path_is_safe(spec.relative_path) {
        let mut artifact = doctor_forensic_source_artifact(data_dir, &spec);
        artifact.error = Some("unsafe bundle relative path".to_string());
        artifacts.push(artifact);
        return Err(format!(
            "refusing unsafe forensic bundle relative path {}",
            spec.relative_path.display()
        ));
    }

    let mut artifact = doctor_forensic_source_artifact(data_dir, &spec);
    let source_metadata = match std::fs::symlink_metadata(spec.source_path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            if spec.required {
                artifact.error = Some("required source artifact is missing".to_string());
                artifacts.push(artifact);
                return Err(format!(
                    "required forensic source artifact is missing: {}",
                    spec.source_path.display()
                ));
            }
            artifacts.push(artifact);
            return Ok(());
        }
        Err(err) => {
            artifact.error = Some(format!("failed to inspect source artifact: {err}"));
            artifacts.push(artifact);
            return Err(format!(
                "failed to inspect forensic source artifact {}: {err}",
                spec.source_path.display()
            ));
        }
    };

    artifact.exists = true;
    artifact.size_bytes = Some(source_metadata.len());
    if source_metadata.file_type().is_symlink() || !source_metadata.is_file() {
        artifact.error = Some("source artifact is not a regular non-symlink file".to_string());
        artifacts.push(artifact);
        return Err(format!(
            "refusing to copy unsafe forensic source artifact {}",
            spec.source_path.display()
        ));
    }
    if spec.source_path.starts_with(data_dir)
        && path_has_symlink_below_root(spec.source_path, data_dir)
    {
        artifact.error = Some("source artifact has a symlinked ancestor".to_string());
        artifacts.push(artifact);
        return Err(format!(
            "refusing to copy forensic source artifact with symlinked ancestor {}",
            spec.source_path.display()
        ));
    }

    let source_hash = match file_blake3_hex(spec.source_path) {
        Ok(hash) => hash,
        Err(err) => {
            artifact.error = Some(err.clone());
            artifacts.push(artifact);
            return Err(err);
        }
    };

    let target_path = bundle_dir.join(spec.relative_path);
    let Some(target_parent) = target_path.parent() else {
        artifact.error = Some("bundle target has no parent".to_string());
        artifacts.push(artifact);
        return Err(format!(
            "forensic bundle target has no parent: {}",
            target_path.display()
        ));
    };
    doctor_forensic_create_private_dir_all(target_parent).map_err(|err| {
        format!(
            "failed to create forensic bundle parent {}: {err}",
            target_parent.display()
        )
    })?;
    if existing_path_has_symlink_below_root(target_parent, bundle_dir) {
        artifact.error = Some("bundle target parent has symlinked ancestor".to_string());
        artifacts.push(artifact);
        return Err(format!(
            "refusing forensic bundle target with symlinked parent {}",
            target_parent.display()
        ));
    }

    let copy_result = (|| -> io::Result<u64> {
        let mut source = std::fs::File::open(spec.source_path)?;
        let mut target = doctor_forensic_create_private_file(&target_path)?;
        let copied = io::copy(&mut source, &mut target)?;
        use std::io::Write as _;
        target.flush()?;
        target.sync_all()?;
        Ok(copied)
    })();
    let copied_bytes = match copy_result {
        Ok(bytes) => bytes,
        Err(err) => {
            artifact.error = Some(format!("copy failed: {err}"));
            artifacts.push(artifact);
            return Err(format!(
                "failed to copy forensic artifact {} to {}: {err}",
                spec.source_path.display(),
                target_path.display()
            ));
        }
    };
    if copied_bytes != source_metadata.len() {
        artifact.error = Some(format!(
            "copy byte count mismatch: copied {copied_bytes}, expected {}",
            source_metadata.len()
        ));
        artifacts.push(artifact);
        return Err(format!(
            "forensic artifact copy byte count mismatch for {}",
            spec.source_path.display()
        ));
    }
    let target_hash = match file_blake3_hex(&target_path) {
        Ok(hash) => hash,
        Err(err) => {
            artifact.error = Some(err.clone());
            artifacts.push(artifact);
            return Err(err);
        }
    };
    if target_hash != source_hash {
        artifact.error = Some(format!(
            "checksum mismatch after copy: source {source_hash}, target {target_hash}"
        ));
        artifacts.push(artifact);
        return Err(format!(
            "forensic artifact checksum mismatch after copying {}",
            spec.source_path.display()
        ));
    }
    artifact.copied = true;
    artifact.checksum_blake3 = Some(source_hash);
    artifacts.push(artifact);
    Ok(())
}

pub(crate) fn doctor_forensic_write_json_artifact(
    artifacts: &mut Vec<DoctorForensicBundleArtifact>,
    bundle_dir: &Path,
    artifact_kind: &str,
    relative_path: &Path,
    value: &serde_json::Value,
) -> Result<(), String> {
    if !doctor_forensic_relative_path_is_safe(relative_path) {
        return Err(format!(
            "refusing unsafe generated forensic bundle relative path {}",
            relative_path.display()
        ));
    }
    // ubs:ignore[rust.security.path-traversal] — relative_path validated by doctor_forensic_relative_path_is_safe above
    let target_path = bundle_dir.join(relative_path);
    let Some(target_parent) = target_path.parent() else {
        return Err(format!(
            "generated forensic artifact target has no parent: {}",
            target_path.display()
        ));
    };
    doctor_forensic_create_private_dir_all(target_parent).map_err(|err| {
        format!(
            "failed to create generated forensic artifact parent {}: {err}",
            target_parent.display()
        )
    })?;
    if existing_path_has_symlink_below_root(target_parent, bundle_dir) {
        return Err(format!(
            "refusing generated forensic artifact with symlinked parent {}",
            target_parent.display()
        ));
    }
    let encoded = serde_json::to_vec_pretty(value)
        .map_err(|err| format!("failed to encode forensic {artifact_kind}: {err}"))?;
    let mut target = doctor_forensic_create_private_file(&target_path).map_err(|err| {
        format!(
            "failed to create generated forensic artifact {}: {err}",
            target_path.display()
        )
    })?;
    use std::io::Write as _;
    target.write_all(&encoded).map_err(|err| {
        format!(
            "failed to write generated forensic artifact {}: {err}",
            target_path.display()
        )
    })?;
    target.flush().map_err(|err| {
        format!(
            "failed to flush generated forensic artifact {}: {err}",
            target_path.display()
        )
    })?;
    target.sync_all().map_err(|err| {
        format!(
            "failed to sync generated forensic artifact {}: {err}",
            target_path.display()
        )
    })?;
    drop(target);
    let checksum = file_blake3_hex(&target_path)?;
    artifacts.push(doctor_forensic_generated_artifact(
        artifact_kind,
        relative_path,
        encoded.len() as u64,
        checksum,
    ));
    Ok(())
}

pub(crate) fn doctor_forensic_safe_command_args() -> Vec<String> {
    let mut redact_next = false;
    let sensitive_terms = ["password", "passwd", "secret", "token", "apikey", "api-key"];
    std::env::args()
        .map(|arg| {
            if redact_next {
                redact_next = false;
                return "[redacted]".to_string();
            }
            let lower = arg.to_ascii_lowercase();
            let sensitive = sensitive_terms.iter().any(|term| lower.contains(term));
            if sensitive {
                if lower.starts_with("--") && !lower.contains('=') {
                    redact_next = true;
                }
                "[redacted-arg]".to_string()
            } else {
                arg
            }
        })
        .collect()
}

pub(crate) fn doctor_forensic_safe_env_metadata() -> serde_json::Value {
    let safe_env_keys = [
        "CASS_OUTPUT_FORMAT",
        "CASS_LEXICAL_PUBLISH_BACKUP_RETENTION",
        "CASS_NO_COLOR",
        "CASS_RESPECT_NO_COLOR",
        "NO_COLOR",
        "TERM",
        "COLORTERM",
    ];
    let mut captured = BTreeMap::new();
    for key in safe_env_keys {
        if let Ok(value) = dotenvy::var(key) {
            captured.insert(key.to_string(), value);
        }
    }
    serde_json::json!({
        "schema_version": 1,
        "captured_env": captured,
        "command_args": doctor_forensic_safe_command_args(),
        "redaction_policy": "allowlist-only; secret/token/password/key-shaped environment variables are not captured",
        "cass_version": env!("CARGO_PKG_VERSION"),
        "platform": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
        }
    })
}

pub(crate) struct DoctorForensicBundleCaptureInput<'a> {
    pub(crate) operation_id: &'a str,
    pub(crate) data_dir: &'a Path,
    pub(crate) db_path: &'a Path,
    pub(crate) index_path: &'a Path,
    pub(crate) plan: Option<&'a DoctorPlan>,
    pub(crate) quarantine_report: Option<&'a DiagQuarantineReport>,
    pub(crate) extra_file_artifacts: &'a [DoctorForensicCopySpec<'a>],
}

pub(crate) fn capture_doctor_forensic_bundle(
    input: DoctorForensicBundleCaptureInput<'_>,
) -> DoctorForensicBundleMetadata {
    let created_at_ms = doctor_now_ms();
    let root = doctor_forensic_bundle_root(input.data_dir);
    if let Err(err) = doctor_forensic_bundle_root_is_safe(input.data_dir, &root) {
        return doctor_forensic_bundle_failed(err);
    }
    if let Err(err) = doctor_forensic_create_private_dir_all(&root) {
        return doctor_forensic_bundle_failed(format!(
            "failed to create forensic bundle root {}: {err}",
            root.display()
        ));
    }
    if let Err(err) = doctor_forensic_bundle_root_is_safe(input.data_dir, &root) {
        return doctor_forensic_bundle_failed(err);
    }
    if let Err(err) = sync_directory(input.data_dir) {
        return doctor_forensic_bundle_failed(err);
    }

    let base_bundle_id = doctor_forensic_bundle_id(input.operation_id, created_at_ms);
    let mut bundle_dir = root.join(&base_bundle_id);
    let mut bundle_id = base_bundle_id.clone();
    let mut bundle_dir_allocated = false;
    for suffix in 0..100 {
        let candidate_id = if suffix == 0 {
            base_bundle_id.clone()
        } else {
            format!("{base_bundle_id}-{suffix}")
        };
        let candidate_dir = root.join(&candidate_id);
        match doctor_forensic_create_private_dir(&candidate_dir) {
            Ok(()) => {
                bundle_id = candidate_id;
                bundle_dir = candidate_dir;
                bundle_dir_allocated = true;
                break;
            }
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(err) => {
                return doctor_forensic_bundle_failed(format!(
                    "failed to create forensic bundle directory {}: {err}",
                    candidate_dir.display()
                ));
            }
        }
    }
    if !bundle_dir_allocated {
        return doctor_forensic_bundle_failed(format!(
            "failed to allocate unique forensic bundle under {}",
            root.display()
        ));
    }

    let mut artifacts = Vec::new();
    let mut first_error: Option<String> = None;
    macro_rules! copy_artifact {
        ($artifact_kind:expr, $source_path:expr, $relative_path:expr, $required:expr, $sidecar_group:expr $(,)?) => {
            if let Err(err) = doctor_forensic_copy_file_artifact(
                &mut artifacts,
                &bundle_dir,
                input.data_dir,
                DoctorForensicCopySpec {
                    artifact_kind: $artifact_kind,
                    source_path: $source_path,
                    relative_path: $relative_path,
                    required: $required,
                    sidecar_group: $sidecar_group,
                },
            ) && first_error.is_none()
            {
                first_error = Some(err);
            }
        };
    }

    copy_artifact!(
        "canonical_archive_db",
        input.db_path,
        Path::new("database/agent_search.db"),
        false,
        Some("sqlite_db"),
    );
    if let Some(wal_path) = doctor_sqlite_sidecar_path(input.db_path, "-wal") {
        copy_artifact!(
            "archive_db_wal_sidecar",
            &wal_path,
            Path::new("database/agent_search.db-wal"),
            false,
            Some("sqlite_db"),
        );
    }
    if let Some(shm_path) = doctor_sqlite_sidecar_path(input.db_path, "-shm") {
        copy_artifact!(
            "archive_db_shm_sidecar",
            &shm_path,
            Path::new("database/agent_search.db-shm"),
            false,
            Some("sqlite_db"),
        );
    }
    for artifact in input.extra_file_artifacts {
        copy_artifact!(
            artifact.artifact_kind,
            artifact.source_path,
            artifact.relative_path,
            artifact.required,
            artifact.sidecar_group,
        );
    }

    for (artifact_kind, path, relative) in [
        (
            "cass_data_config",
            input.data_dir.join("config.toml"),
            PathBuf::from("config/cass-data-config.toml"),
        ),
        (
            "bookmarks_json",
            input.data_dir.join("bookmarks.json"),
            PathBuf::from("bookmarks/bookmarks.json"),
        ),
        (
            "bookmarks_db",
            input.data_dir.join("bookmarks.db"),
            PathBuf::from("bookmarks/bookmarks.db"),
        ),
    ] {
        copy_artifact!(artifact_kind, &path, &relative, false, None);
    }

    let raw_manifest_root = doctor_raw_mirror_root(input.data_dir).join("manifests");
    if raw_manifest_root.exists() {
        for entry in walkdir::WalkDir::new(&raw_manifest_root)
            .follow_links(false)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_file() || entry.file_type().is_symlink())
        {
            let path = entry.path();
            let Ok(relative_to_root) = path.strip_prefix(&raw_manifest_root) else {
                continue;
            };
            let Some(relative) =
                doctor_forensic_portable_child_path("raw-mirror-manifests", relative_to_root)
            else {
                continue;
            };
            copy_artifact!("raw_mirror_manifest", path, &relative, false, None);
        }
    } else {
        doctor_forensic_record_missing_artifact(
            &mut artifacts,
            input.data_dir,
            "raw_mirror_manifest_root",
            &raw_manifest_root,
            Path::new("raw-mirror-manifests"),
            false,
            None,
        );
    }

    let lexical_manifest_root = diag_lexical_manifest_scan_root(input.index_path);
    if lexical_manifest_root.exists() {
        for entry in walkdir::WalkDir::new(&lexical_manifest_root)
            .follow_links(false)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry.file_type().is_file()
                    && entry.file_name().to_str()
                        == Some(
                            crate::indexer::lexical_generation::LEXICAL_GENERATION_MANIFEST_FILE,
                        )
            })
        {
            let path = entry.path();
            let Ok(relative_to_root) = path.strip_prefix(&lexical_manifest_root) else {
                continue;
            };
            let Some(relative) =
                doctor_forensic_portable_child_path("index-manifests", relative_to_root)
            else {
                continue;
            };
            copy_artifact!("lexical_generation_manifest", path, &relative, false, None);
        }
    } else {
        doctor_forensic_record_missing_artifact(
            &mut artifacts,
            input.data_dir,
            "lexical_manifest_root",
            &lexical_manifest_root,
            Path::new("index-manifests"),
            false,
            None,
        );
    }

    let source_inventory = collect_doctor_source_inventory(input.data_dir, input.db_path);
    let raw_mirror_report = collect_doctor_raw_mirror_report(input.data_dir);
    let quarantine_report = input
        .quarantine_report
        .cloned()
        .unwrap_or_else(|| collect_diag_quarantine_report(input.data_dir, input.index_path));

    for (artifact_kind, relative_path, value) in [
        (
            "env_metadata",
            PathBuf::from("metadata/env-metadata.json"),
            doctor_forensic_safe_env_metadata(),
        ),
        (
            "source_inventory",
            PathBuf::from("metadata/source-inventory.json"),
            serde_json::to_value(&source_inventory).unwrap_or_else(|_| serde_json::json!({})),
        ),
        (
            "raw_mirror_report",
            PathBuf::from("metadata/raw-mirror-report.json"),
            serde_json::to_value(&raw_mirror_report).unwrap_or_else(|_| serde_json::json!({})),
        ),
        (
            "quarantine_report",
            PathBuf::from("metadata/quarantine-report.json"),
            serde_json::to_value(&quarantine_report).unwrap_or_else(|_| serde_json::json!({})),
        ),
    ] {
        if let Err(err) = doctor_forensic_write_json_artifact(
            &mut artifacts,
            &bundle_dir,
            artifact_kind,
            &relative_path,
            &value,
        ) && first_error.is_none()
        {
            first_error = Some(err);
        }
    }
    if let Some(plan) = input.plan
        && let Err(err) = doctor_forensic_write_json_artifact(
            &mut artifacts,
            &bundle_dir,
            "doctor_plan",
            Path::new("metadata/doctor-plan.json"),
            &serde_json::to_value(plan).unwrap_or_else(|_| serde_json::json!({})),
        )
        && first_error.is_none()
    {
        first_error = Some(err);
    }

    let manifest_relative_path = Path::new("manifest.json");
    let manifest_path = bundle_dir.join("manifest.json");
    let manifest_value = serde_json::json!({
        "schema_version": 1,
        "bundle_kind": "cass_doctor_forensic_bundle_v1",
        "bundle_id": bundle_id,
        "operation_id": input.operation_id,
        "created_at_ms": created_at_ms,
        "cass_version": env!("CARGO_PKG_VERSION"),
        "data_dir": input.data_dir.display().to_string(),
        "redacted_data_dir": doctor_redacted_path(&input.data_dir.display().to_string(), input.data_dir),
        "db_path": input.db_path.display().to_string(),
        "redacted_db_path": doctor_redacted_path(&input.db_path.display().to_string(), input.data_dir),
        "index_path": input.index_path.display().to_string(),
        "redacted_index_path": doctor_redacted_path(&input.index_path.display().to_string(), input.data_dir),
        "artifacts": artifacts.clone(),
        "redaction_status": "local_forensic_bundle_contains_exact_paths; robot metadata includes redacted path companions",
    });
    if let Err(err) = doctor_forensic_write_json_artifact(
        &mut artifacts,
        &bundle_dir,
        "forensic_bundle_manifest",
        manifest_relative_path,
        &manifest_value,
    ) && first_error.is_none()
    {
        first_error = Some(err);
    }
    if let Err(err) = sync_directory(&bundle_dir)
        && first_error.is_none()
    {
        first_error = Some(err);
    }
    if let Err(err) = sync_directory(&root)
        && first_error.is_none()
    {
        first_error = Some(err);
    }

    let manifest_checksum = file_blake3_hex(&manifest_path).ok();
    let artifact_count = artifacts.len();
    let copied_artifact_count = artifacts.iter().filter(|artifact| artifact.copied).count();
    let missing_artifact_count = artifacts.iter().filter(|artifact| !artifact.exists).count();
    let failed_artifact_count = artifacts
        .iter()
        .filter(|artifact| artifact.error.is_some())
        .count();
    let sidecar_complete = artifacts
        .iter()
        .filter(|artifact| artifact.sidecar_group.is_some() && artifact.exists)
        .all(|artifact| artifact.copied && artifact.error.is_none());
    let blocked_reason = first_error.or_else(|| {
        (failed_artifact_count > 0)
            .then(|| "one or more forensic bundle artifacts failed to copy or verify".to_string())
    });
    DoctorForensicBundleMetadata {
        bundle_id: Some(bundle_id),
        path: Some(bundle_dir.display().to_string()),
        redacted_path: Some(doctor_redacted_path(
            &bundle_dir.display().to_string(),
            input.data_dir,
        )),
        manifest_path: Some(manifest_path.display().to_string()),
        redacted_manifest_path: Some(doctor_redacted_path(
            &manifest_path.display().to_string(),
            input.data_dir,
        )),
        checksum_blake3: manifest_checksum,
        status: if blocked_reason.is_some() {
            "failed".to_string()
        } else {
            "captured".to_string()
        },
        created_at_ms: Some(created_at_ms),
        artifact_count,
        copied_artifact_count,
        missing_artifact_count,
        failed_artifact_count,
        sidecar_complete,
        redaction_status:
            "local_forensic_bundle_contains_exact_paths; robot metadata includes redacted path companions"
                .to_string(),
        blocked_reason,
        artifacts,
    }
}
