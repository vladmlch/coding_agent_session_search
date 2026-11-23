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

pub(super) fn insert_doctor_response_schemas(
    schemas: &mut std::collections::BTreeMap<String, serde_json::Value>,
) {
    use serde_json::json;
    // `coding_agent_session_search-0h4cx`: extend response_schemas to
    // cover every advertised JSON surface from README line 103. The
    // surfaces below — doctor / models-status / models-verify — were
    // absent pre-fix even though their goldens live under
    // tests/golden/robot/. An agent reading response_schemas to drive
    // schema-aware parsing now finds a pinned schema for every surface
    // cass commits to.
    let mut doctor_schema = json!({
        "type": "object",
        "description": "cass doctor --json: diagnostic checks + optional auto-fix audit.",
        "properties": {
            "status": { "type": "string" },
            "health_class": { "type": "string", "description": "Stable kebab-case DoctorHealth value such as healthy, degraded-derived-assets, degraded-archive-risk, repair-blocked, repair-previously-failed, or source-authority-unsafe." },
            "risk_level": { "type": "string", "description": "none | low | medium | high archive/user-data risk summary for first-pass automation." },
            "healthy": { "type": "boolean" },
            "initialized": { "type": "boolean" },
            "explanation": { "type": ["string", "null"] },
            "recommended_action": { "type": ["string", "null"] },
            "fallback_mode": { "type": "string", "description": "Realized fallback tier for default hybrid search, usually lexical when semantic assets are unavailable." },
            "issues_found": { "type": "integer" },
            "issues_fixed": { "type": "integer" },
            "failures": { "type": "integer" },
            "warnings": { "type": "integer" },
            "needs_rebuild": { "type": "boolean" },
            "auto_fix_applied": { "type": "boolean" },
            "auto_fix_actions": { "type": "array", "items": { "type": "string" } },
            "doctor_command": response_schema_opaque_object(),
            "doctor_v2_contract": response_schema_doctor_v2_contract_summary(),
            "check_scope": response_schema_opaque_object(),
            "repair_previously_failed": { "type": "boolean" },
            "failure_marker_path": { "type": ["string", "null"] },
            "repeat_refusal_reason": { "type": ["string", "null"] },
            "override_available": { "type": "boolean" },
            "override_used": { "type": "boolean" },
            "active_repair": response_schema_opaque_object(),
            "post_repair_probes": response_schema_opaque_object(),
            "repair_failure_marker": response_schema_doctor_repair_failure_marker(),
            "failure_marker_write_error": { "type": ["string", "null"] },
            "failure_context": response_schema_opaque_object(),
            "operation_outcome": response_schema_doctor_operation_outcome(),
            "operation_state": response_schema_doctor_operation_state(),
            "locks": response_schema_doctor_locks(),
            "slow_operations": response_schema_doctor_slow_operations(),
            "timing_summary": response_schema_doctor_timing_summary(),
            "retry_recommendation": response_schema_doctor_retry_recommendation(),
            "safe_auto_eligibility": response_schema_opaque_object(),
            "root_cause": response_schema_root_cause_attribution(),
            "primary_incident_id": { "type": ["string", "null"], "description": "incident_id for the highest-priority root-cause incident, or null when no incident was found." },
            "incidents": {
                "type": "array",
                "description": "Root-cause incident groups derived from checks, coverage, lock, and candidate state. Robots should use root_cause_kind and evidence_check_ids instead of scraping check prose.",
                "items": {
                    "type": "object",
                    "properties": {
                        "schema_version": { "type": "integer" },
                        "incident_id": { "type": "string" },
                        "root_cause_kind": { "type": "string" },
                        "severity": { "type": "string" },
                        "affected_asset_classes": { "type": "array", "items": { "type": "string" } },
                        "archive_risk_level": { "type": "string" },
                        "derived_risk_level": { "type": "string" },
                        "confidence": { "type": "string" },
                        "evidence_check_ids": { "type": "array", "items": { "type": "string" } },
                        "blocked_actions": { "type": "array", "items": { "type": "string" } },
                        "safe_next_actions": { "type": "array", "items": { "type": "string" } },
                        "stale_or_unknown_fields": { "type": "array", "items": { "type": "string" } },
                        "redacted_evidence_paths": { "type": "array", "items": { "type": "string" } },
                        "summary": { "type": "string" }
                    },
                    "required": ["schema_version", "incident_id", "root_cause_kind", "severity", "affected_asset_classes", "archive_risk_level", "derived_risk_level", "confidence", "evidence_check_ids", "blocked_actions", "safe_next_actions", "stale_or_unknown_fields", "redacted_evidence_paths", "summary"]
                }
            },
            "event_log": response_schema_doctor_event_log_metadata(),
            "lexical": response_schema_index_state(),
            "semantic": response_schema_semantic_state(),
            "derived_semantic_assets": response_schema_doctor_derived_semantic_assets(),
            "storage_pressure": response_schema_opaque_object(),
            "asset_taxonomy": response_schema_opaque_object_array(),
            "anomaly_taxonomy": response_schema_opaque_object_array(),
            "repair_contract": response_schema_opaque_object(),
            "config_exclusion_risks": {
                "type": "array",
                "items": response_schema_doctor_config_exclusion_risk()
            },
            "source_inventory": response_schema_doctor_source_inventory(),
            "raw_mirror": response_schema_doctor_raw_mirror(),
            "raw_mirror_backfill": response_schema_doctor_raw_mirror_backfill(),
            "coverage_summary": response_schema_doctor_coverage_summary(),
            "sole_copy_warnings": { "type": "array", "items": response_schema_opaque_object() },
            "detail_output": response_schema_doctor_detail_output(),
            "coverage_risk": response_schema_doctor_coverage_risk(),
            "source_authority": response_schema_doctor_source_authority(),
            "candidate_staging": response_schema_doctor_candidate_staging(),
            "quarantine": response_schema_opaque_object(),
            "repair_plan": response_schema_doctor_repair_plan_preview(),
            "cleanup_apply": response_schema_doctor_cleanup_apply(),
            "_meta": response_schema_opaque_object(),
            "checks": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": { "type": "string" },
                        "status": { "type": "string", "description": "pass | warn | fail" },
                        "message": { "type": "string" },
                        "anomaly_class": { "type": "string", "description": "Stable kebab-case DoctorAnomaly value; robots should branch on this instead of message text." },
                        "health_class": { "type": "string", "description": "Stable kebab-case DoctorHealth value derived from anomaly_class." },
                        "severity": { "type": "string", "description": "info | warn | error" },
                        "affected_asset_class": { "type": "string", "description": "Stable DoctorAssetClass value naming the asset class at risk." },
                        "data_loss_risk": { "type": "string", "description": "none | low | medium | high | unknown" },
                        "recommended_action": { "type": "string" },
                        "safe_for_auto_repair": { "type": "boolean" },
                        "default_outcome_kind": { "type": "string" },
                        "fix_available": { "type": "boolean" },
                        "fix_applied": { "type": "boolean" }
                    },
                    "required": ["name", "status", "message", "anomaly_class", "health_class", "severity", "affected_asset_class", "data_loss_risk", "recommended_action", "safe_for_auto_repair", "default_outcome_kind", "fix_available", "fix_applied"]
                }
            }
        },
        "required": ["status", "health_class", "risk_level", "healthy", "initialized", "recommended_action", "fallback_mode", "doctor_command", "check_scope", "repair_previously_failed", "failure_marker_path", "repeat_refusal_reason", "override_available", "override_used", "active_repair", "post_repair_probes", "repair_failure_marker", "operation_outcome", "operation_state", "locks", "slow_operations", "timing_summary", "retry_recommendation", "safe_auto_eligibility", "primary_incident_id", "incidents", "event_log", "lexical", "semantic", "derived_semantic_assets", "storage_pressure", "config_exclusion_risks", "raw_mirror_backfill", "coverage_summary", "sole_copy_warnings", "detail_output", "coverage_risk", "source_authority", "candidate_staging", "checks"]
    });
    let doctor_properties = doctor_schema
        .get_mut("properties")
        .and_then(|value| value.as_object_mut())
        .expect("doctor schema properties object");
    doctor_properties.insert("schema_version".to_string(), json!({ "type": "integer" }));
    doctor_properties.insert(
        "doctor_contract_version".to_string(),
        json!({ "type": "integer" }),
    );
    doctor_properties.insert("capabilities_url".to_string(), json!({ "type": "string" }));
    // `.14.1` storage-integrity block (bead vl1cj). Inserted post-construction
    // (not inside the json! literal) so the giant doctor_schema macro stays
    // under the serde_json recursion limit.
    doctor_properties.insert(
        "storage_integrity".to_string(),
        response_schema_storage_integrity(),
    );
    doctor_properties.insert(
        "daemon_runtime".to_string(),
        response_schema_opaque_object(),
    );
    // #287 degradation taxonomy: `reason_code` is the primary entry of
    // `degraded_reason_codes`; both null/empty on healthy runs. The `budget`
    // envelope mirrors the search/pack budget block (gh352 family).
    doctor_properties.insert(
        "reason_code".to_string(),
        json!({ "type": ["string", "null"] }),
    );
    doctor_properties.insert(
        "degraded_reason_codes".to_string(),
        json!({ "type": "array", "items": { "type": "string" } }),
    );
    doctor_properties.insert("budget".to_string(), response_schema_budget_block());
    doctor_schema
        .get_mut("required")
        .and_then(|value| value.as_array_mut())
        .expect("doctor schema required array")
        .push(json!("daemon_runtime"));
    schemas.insert("doctor".to_string(), doctor_schema);

    schemas.insert(
        "doctor-archive-normalize".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Schema-first contract for archive normalization. Dry-run/apply implementations must report coverage deltas, redaction status, and receipt artifacts before any canonical archive mutation.",
                surface: "archive-normalize",
                mode: "repair-dry-run",
                status: "planned",
                outcome_kind: "planned",
                asset_class: "canonical_archive_db",
                risk_level: "medium",
                fallback_mode: "read-only",
                authority_status: "selected",
                recommended_action: "cass doctor archive-normalize --dry-run --json",
            },
            [
                ("normalization_candidates", response_schema_opaque_object_array()),
                ("repair_plan", response_schema_doctor_repair_plan_preview()),
                ("safety_gates", serde_json::json!({ "type": "array", "items": response_schema_doctor_safety_gate() })),
                ("forensic_bundle", response_schema_doctor_forensic_bundle_metadata()),
                ("detail_output", response_schema_doctor_detail_output()),
            ],
        ),
    );
    schemas.insert(
        "doctor-archive-scan".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Read-only archive scan contract covering canonical DB, raw mirror, current source visibility, sole-copy warnings, and source-authority evidence.",
                surface: "archive-scan",
                mode: "read-only-check",
                status: "ok",
                outcome_kind: "no_op",
                asset_class: "canonical_archive_db",
                risk_level: "low",
                fallback_mode: "read-only",
                authority_status: "read_only",
                recommended_action: "cass doctor check --json",
            },
            [
                ("source_inventory", response_schema_doctor_source_inventory()),
                ("config_exclusion_risks", serde_json::json!({
                    "type": "array",
                    "items": response_schema_doctor_config_exclusion_risk()
                })),
                ("raw_mirror", response_schema_doctor_raw_mirror()),
                ("coverage_summary", response_schema_doctor_coverage_summary()),
                ("sole_copy_warnings", serde_json::json!({ "type": "array", "items": response_schema_opaque_object() })),
                ("detail_output", response_schema_doctor_detail_output()),
                ("source_authority", response_schema_doctor_source_authority()),
            ],
        ),
    );
    schemas.insert(
        "doctor-backups-list".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Read-only backup inventory contract. Backup metadata and redacted paths are data; restore decisions require a separate rehearsal receipt.",
                surface: "backups",
                mode: "read-only-check",
                status: "ok",
                outcome_kind: "no_op",
                asset_class: "backup_bundle",
                risk_level: "low",
                fallback_mode: "read-only",
                authority_status: "read_only",
                recommended_action: "cass doctor backups list --json",
            },
            [
                ("backups", serde_json::json!({ "type": "array", "items": response_schema_opaque_object() })),
                ("retention_policy", response_schema_opaque_object()),
                ("verify_command", serde_json::json!({ "type": "string" })),
                ("restore_rehearsal_command", serde_json::json!({ "type": "string" })),
            ],
        ),
    );
    schemas.insert(
        "doctor-backups-verify".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Backup verification contract with checksum and manifest status for every candidate backup before restore rehearsal.",
                surface: "backups",
                mode: "read-only-check",
                status: "ok",
                outcome_kind: "no_op",
                asset_class: "backup_bundle",
                risk_level: "low",
                fallback_mode: "read-only",
                authority_status: "selected",
                recommended_action: "cass doctor backups verify --json",
            },
            [
                ("backup_verification", response_schema_opaque_object()),
                ("artifact_manifest", response_schema_doctor_artifact_manifest()),
                ("checksum_status_counts", serde_json::json!({ "type": "object", "additionalProperties": { "type": "integer" } })),
                ("restore_rehearsal_allowed", serde_json::json!({ "type": "boolean" })),
            ],
        ),
    );
    schemas.insert(
        "doctor-baseline-diff".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Read-only diagnostic baseline diff. Intended for before/after doctor comparisons with no source or archive mutation.",
                surface: "baseline-diff",
                mode: "baseline-diff",
                status: "unchanged",
                outcome_kind: "no_op",
                asset_class: "operation_receipt",
                risk_level: "low",
                fallback_mode: "not-applicable",
                authority_status: "read_only",
                recommended_action: "cass doctor baseline-diff --json",
            },
            [
                ("baseline_diff", response_schema_opaque_object()),
                ("baseline_id", serde_json::json!({ "type": "string" })),
                ("baseline_path", serde_json::json!({ "type": "string" })),
                ("compared_at", serde_json::json!({ "type": "integer" })),
                ("added_anomalies", serde_json::json!({ "type": "array", "items": { "type": "string" } })),
                ("removed_anomalies", serde_json::json!({ "type": "array", "items": { "type": "string" } })),
                ("changed_assets", serde_json::json!({ "type": "array", "items": response_schema_opaque_object() })),
                ("changed_recommendations", serde_json::json!({ "type": "array", "items": response_schema_opaque_object() })),
                ("perf_delta", response_schema_opaque_object()),
                ("privacy_redaction_status", response_schema_opaque_object()),
                ("event_log_path", serde_json::json!({ "type": ["string", "null"] })),
                ("artifact_manifest_path", serde_json::json!({ "type": "string" })),
                ("baseline_mutated", serde_json::json!({ "type": "boolean" })),
            ],
        ),
    );
    schemas.insert(
        "doctor-baseline-save".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Explicit diagnostic baseline save/update contract. The saved baseline is redacted and is not a backup or restore point.",
                surface: "baseline",
                mode: "baseline-save",
                status: "saved",
                outcome_kind: "applied",
                asset_class: "operation_receipt",
                risk_level: "low",
                fallback_mode: "not-applicable",
                authority_status: "selected",
                recommended_action: "cass doctor baseline diff <id> --json",
            },
            [
                ("baseline_id", serde_json::json!({ "type": "string" })),
                ("baseline_path", serde_json::json!({ "type": "string" })),
                ("redacted_baseline_path", serde_json::json!({ "type": "string" })),
                ("baseline_checksum", serde_json::json!({ "type": ["string", "null"] })),
                ("baseline_mutated", serde_json::json!({ "type": "boolean" })),
                ("baseline_document", response_schema_opaque_object()),
                ("write_error", serde_json::json!({ "type": ["string", "null"] })),
                ("diagnostic_only", serde_json::json!({ "type": "boolean" })),
                ("backup_semantics", serde_json::json!({ "type": "string" })),
            ],
        ),
    );
    schemas.insert(
        "doctor-baseline-update".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Explicit diagnostic baseline update contract. Replacing a baseline is an operator-selected diagnostic snapshot refresh, not a backup or restore point.",
                surface: "baseline",
                mode: "baseline-update",
                status: "updated",
                outcome_kind: "applied",
                asset_class: "operation_receipt",
                risk_level: "low",
                fallback_mode: "not-applicable",
                authority_status: "selected",
                recommended_action: "cass doctor baseline diff <id> --json",
            },
            [
                ("baseline_id", serde_json::json!({ "type": "string" })),
                ("baseline_path", serde_json::json!({ "type": "string" })),
                ("redacted_baseline_path", serde_json::json!({ "type": "string" })),
                ("baseline_checksum", serde_json::json!({ "type": ["string", "null"] })),
                ("baseline_mutated", serde_json::json!({ "type": "boolean" })),
                ("baseline_document", response_schema_opaque_object()),
                ("write_error", serde_json::json!({ "type": ["string", "null"] })),
                ("diagnostic_only", serde_json::json!({ "type": "boolean" })),
                ("backup_semantics", serde_json::json!({ "type": "string" })),
            ],
        ),
    );
    schemas.insert(
        "doctor-check".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Explicit read-only doctor check contract. It is the preferred first call before any repair, cleanup, restore, or reconstruct command.",
                surface: "check",
                mode: "read-only-check",
                status: "ok",
                outcome_kind: "no_op",
                asset_class: "canonical_archive_db",
                risk_level: "low",
                fallback_mode: "lexical",
                authority_status: "read_only",
                recommended_action: "no action needed; inspect checks before repair",
            },
            [
                ("checks", serde_json::json!({ "type": "array", "items": response_schema_opaque_object() })),
                ("doctor_command", response_schema_opaque_object()),
                ("operation_state", response_schema_doctor_operation_state()),
                ("repair_readiness", response_schema_opaque_object()),
                ("safe_auto_eligibility", response_schema_opaque_object()),
            ],
        ),
    );
    schemas.insert(
        "doctor-cleanup-apply".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Fingerprint-approved cleanup apply receipt. It may prune only derived reclaimable assets after safety gates pass and must never delete source session logs or raw mirror evidence.",
                surface: "cleanup",
                mode: "cleanup-apply",
                status: "applied",
                outcome_kind: "applied",
                asset_class: "reclaimable_derived_cache",
                risk_level: "low",
                fallback_mode: "not-applicable",
                authority_status: "selected",
                recommended_action: "cass doctor check --json",
            },
            [
                ("cleanup_apply", response_schema_doctor_cleanup_apply()),
                ("receipt", response_schema_doctor_receipt()),
                ("post_repair_probes", response_schema_opaque_object()),
                ("validation_summary", response_schema_opaque_object()),
            ],
        ),
    );
    schemas.insert(
        "doctor-cleanup-dry-run".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Cleanup dry-run inventory. It reports reclaim candidates, safe_to_gc advice, and a plan fingerprint without mutating the filesystem.",
                surface: "cleanup",
                mode: "cleanup-dry-run",
                status: "planned",
                outcome_kind: "planned",
                asset_class: "reclaimable_derived_cache",
                risk_level: "low",
                fallback_mode: "not-applicable",
                authority_status: "read_only",
                recommended_action: "cass doctor cleanup --yes --plan-fingerprint <fingerprint> --json",
            },
            [
                ("cleanup_plan", response_schema_doctor_repair_plan_preview()),
                ("reclaim_candidates", serde_json::json!({ "type": "array", "items": response_schema_opaque_object() })),
                ("safe_to_gc_count", serde_json::json!({ "type": "integer" })),
                ("planned_reclaim_bytes", serde_json::json!({ "type": "integer" })),
            ],
        ),
    );
    schemas.insert(
        "doctor-error-envelope".to_string(),
        response_schema_doctor_v2_error_envelope(),
    );
    schemas.insert(
        "doctor-failure-context".to_string(),
        response_schema_doctor_failure_context(),
    );
    schemas.insert(
        "doctor-health-summary".to_string(),
        response_schema_doctor_v2_summary("health-summary"),
    );
    schemas.insert(
        "doctor-reconstruct-dry-run".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Reconstruct dry-run contract. Candidate generation writes only isolated doctor/candidates artifacts and reports promotion gates before any canonical archive replacement.",
                surface: "reconstruct",
                mode: "reconstruct-dry-run",
                status: "planned",
                outcome_kind: "planned",
                asset_class: "raw_mirror_blob",
                risk_level: "medium",
                fallback_mode: "read-only",
                authority_status: "candidate_only",
                recommended_action: "inspect candidate_staging before any promote command",
            },
            [
                ("candidate_staging", response_schema_doctor_candidate_staging()),
                ("source_authority", response_schema_doctor_source_authority()),
                ("forensic_bundle", response_schema_doctor_forensic_bundle_metadata()),
                ("promotion_allowed", serde_json::json!({ "type": "boolean" })),
                ("promotion_blockers", serde_json::json!({ "type": "array", "items": { "type": "string" } })),
            ],
        ),
    );
    schemas.insert(
        "doctor-repair-dry-run".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Explicit repair dry-run contract. It returns a canonical plan_fingerprint and exact apply argv while remaining read-only.",
                surface: "repair",
                mode: "repair-dry-run",
                status: "planned",
                outcome_kind: "planned",
                asset_class: "derived_lexical_index",
                risk_level: "low",
                fallback_mode: "lexical",
                authority_status: "selected",
                recommended_action: "cass doctor repair --yes --plan-fingerprint <fingerprint> --json",
            },
            [
                ("repair_plan", response_schema_doctor_repair_plan_preview()),
                ("exact_apply_command", serde_json::json!({ "type": "string" })),
                ("apply_argv", serde_json::json!({ "type": "array", "items": { "type": "string" } })),
                ("safe_auto_eligible", serde_json::json!({ "type": "boolean" })),
                ("post_repair_probes_required", serde_json::json!({ "type": "boolean" })),
            ],
        ),
    );
    schemas.insert(
        "doctor-repair-receipt".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Fingerprint-approved repair receipt contract with event log, artifact manifest, post-repair probe summary, and remaining risk fields.",
                surface: "repair",
                mode: "fingerprint-apply",
                status: "applied",
                outcome_kind: "applied",
                asset_class: "derived_lexical_index",
                risk_level: "low",
                fallback_mode: "lexical",
                authority_status: "selected",
                recommended_action: "cass doctor check --json",
            },
            [
                ("receipt", response_schema_doctor_receipt()),
                ("post_repair_probes", response_schema_opaque_object()),
                ("verification_summary", response_schema_opaque_object()),
                ("remaining_risk", serde_json::json!({ "type": "array", "items": { "type": "string" } })),
            ],
        ),
    );
    schemas.insert(
        "doctor-restore-rehearsal".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Restore rehearsal contract. It validates backup bundles and produces a rehearsal receipt before any restore apply mode can touch canonical archive assets.",
                surface: "restore",
                mode: "restore-rehearsal",
                status: "planned",
                outcome_kind: "planned",
                asset_class: "backup_bundle",
                risk_level: "medium",
                fallback_mode: "read-only",
                authority_status: "selected",
                recommended_action: "inspect rehearsal receipt before restore apply",
            },
            [
                ("restore_rehearsal", response_schema_opaque_object()),
                ("backup_verification", response_schema_opaque_object()),
                ("rehearsal_receipt", response_schema_doctor_receipt()),
                ("restore_apply_allowed", serde_json::json!({ "type": "boolean" })),
            ],
        ),
    );
    schemas.insert(
        "doctor-safe-auto-run".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Safe auto-run contract for legacy cass doctor --fix. It may apply only predeclared safe actions and must emit receipts for every mutation.",
                surface: "legacy-doctor",
                mode: "safe-auto-fix",
                status: "partial",
                outcome_kind: "partial",
                asset_class: "reclaimable_derived_cache",
                risk_level: "low",
                fallback_mode: "lexical",
                authority_status: "selected",
                recommended_action: "cass doctor check --json",
            },
            [
                ("safe_auto_eligibility", response_schema_opaque_object()),
                ("actions", serde_json::json!({ "type": "array", "items": response_schema_doctor_action() })),
                ("receipts", serde_json::json!({ "type": "array", "items": response_schema_doctor_receipt() })),
                ("skipped_actions", serde_json::json!({ "type": "array", "items": response_schema_opaque_object() })),
            ],
        ),
    );
    schemas.insert(
        "doctor-semantic-model-fallback".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Semantic/model fallback contract. Missing model files or vector backlog degrade to lexical and should not block lexical search or archive repair.",
                surface: "semantic-model-fallback",
                mode: "read-only-check",
                status: "warn",
                outcome_kind: "no_op",
                asset_class: "model_cache",
                risk_level: "low",
                fallback_mode: "lexical",
                authority_status: "read_only",
                recommended_action: "cass models status --json",
            },
            [
                ("semantic", response_schema_semantic_state()),
                (
                    "derived_semantic_assets",
                    response_schema_doctor_derived_semantic_assets(),
                ),
                ("model_status", response_schema_opaque_object()),
                ("fallback_reason", serde_json::json!({ "type": ["string", "null"] })),
                ("model_install_command", serde_json::json!({ "type": ["string", "null"] })),
            ],
        ),
    );
    schemas.insert(
        "doctor-status-summary".to_string(),
        response_schema_doctor_v2_summary("status-summary"),
    );
    schemas.insert(
        "doctor-support-bundle".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Support bundle contract. Bundles are scrubbed by default, include manifests/checksums, and require explicit opt-in for sensitive raw attachments.",
                surface: "support-bundle",
                mode: "support-bundle",
                status: "ok",
                outcome_kind: "no_op",
                asset_class: "support_bundle",
                risk_level: "low",
                fallback_mode: "not-applicable",
                authority_status: "read_only",
                recommended_action: "share artifact_manifest_path only after reviewing redaction_status",
            },
            [
                ("support_bundle", response_schema_doctor_forensic_bundle_metadata()),
                ("artifact_manifest", response_schema_doctor_artifact_manifest()),
                ("redaction_report", response_schema_opaque_object()),
                ("sensitive_attachment_policy", response_schema_opaque_object()),
                ("verification_summary", response_schema_opaque_object()),
            ],
        ),
    );

    schemas.insert(
        "models-status".to_string(),
        json!({
            "type": "object",
            "description": "cass models status --json: semantic-model acquisition + cache state.",
            "properties": {
                "policy_quality_tier_embedder": { "type": "string" },
                "active_registry_name": { "type": ["string", "null"] },
                "lexical_fail_open": { "type": "boolean" },
                "models": response_schema_opaque_object_array(),
                "model_id": { "type": "string" },
                "model_dir": { "type": "string" },
                "installed": { "type": "boolean" },
                "state": { "type": "string", "description": "not_acquired | acquired | checksum_mismatch | incompatible_version | disabled_by_policy | budget_blocked | quarantined_corrupt | preseeded_local" },
                "state_detail": { "type": "string" },
                "next_step": { "type": "string" },
                "revision": { "type": "string" },
                "license": { "type": "string" },
                "total_size_bytes": { "type": "integer" },
                "installed_size_bytes": { "type": "integer" },
                "observed_file_bytes": { "type": "integer" },
                "policy_source": { "type": "string" },
                "cache_lifecycle": {
                    "type": "object",
                    "additionalProperties": true,
                    "description": "Opaque lifecycle block describing cache state and missing files."
                },
                "model_acquisition": {
                    "type": "object",
                    "additionalProperties": true,
                    "description": "Hardened acquisition contract: state (absent|partial_install|ready|checksum_mismatch|quarantined_corrupt|budget_blocked|disabled_by_policy|baseline_no_semantic|incompatible_runtime|runtime_load_failed), runtime, source, cost_class, expected_install_bytes, fallback_mode, next_step, next_command, state_detail, rollback_guidance, fingerprint{revision,content_digest,marker_token}, and skipped_network_reason (the typed proof cass never acquires models outside `cass models install --from-file`)."
                },
                "files": response_schema_opaque_object_array()
            },
            "required": ["policy_quality_tier_embedder", "lexical_fail_open", "models"]
        }),
    );

    schemas.insert(
        "models-verify".to_string(),
        json!({
            "type": "object",
            "description": "cass models verify --json: per-file SHA-256 verification of the installed semantic model.",
            "properties": {
                "status": { "type": "string" },
                "state_detail": { "type": "string" },
                "next_step": { "type": "string" },
                "lexical_fail_open": { "type": "boolean" },
                "model_dir": { "type": "string" },
                "all_valid": { "type": "boolean" },
                "cache_lifecycle": response_schema_opaque_object(),
                "model_acquisition": {
                    "type": "object",
                    "additionalProperties": true,
                    "description": "Hardened acquisition contract (same shape as models-status.model_acquisition): state, runtime, source, cost_class, fingerprint, and skipped_network_reason proving verify never auto-downloads."
                },
                "error": { "type": ["string", "null"] }
            },
            "required": ["status", "all_valid", "lexical_fail_open"]
        }),
    );

    schemas.insert(
        "models-build-hnsw".to_string(),
        json!({
            "type": "object",
            "description": "cass models build-hnsw --json (gh#408): builds or verifies (--check) the HNSW accelerator for an already-published semantic artifact without opening the archive or embedding; state_* use the runtime loader's admissibility vocabulary.",
            "properties": {
                "tier": { "type": "string", "description": "fast | quality" },
                "embedder_id": { "type": "string" },
                "vector_count": { "type": "integer" },
                "artifact_path": { "type": "string", "description": "Published .fsvi path relative to data_dir" },
                "hnsw_path": { "type": "string", "description": "Accelerator metadata path relative to data_dir" },
                "state_before": { "type": "string", "description": "missing | invalid_entry | native_valid | stale_or_legacy | unreadable" },
                "state_after": { "type": "string", "description": "Same vocabulary as state_before; equals state_before for check_only" },
                "action": { "type": "string", "description": "check_only | unchanged | rebuilt" },
                "manifest_published": { "type": "boolean", "description": "True when an HnswRecord was written to the semantic manifest in this run" },
                "manifest_recorded": { "type": "boolean", "description": "True when the semantic manifest now carries a ready HnswRecord for this tier/embedder (written this run or already present)" },
                "current": { "type": "boolean", "description": "True when state_after is native_valid (approximate search usable)" },
                "next_command": { "type": ["string", "null"], "description": "cass models build-hnsw when the accelerator is not current" }
            },
            "required": ["tier", "embedder_id", "vector_count", "hnsw_path", "state_before", "state_after", "action", "manifest_published", "current"]
        }),
    );
}
