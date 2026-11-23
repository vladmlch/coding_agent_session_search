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

pub(crate) fn response_schema_doctor_failure_context_repro() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "Safe repro template. It must default to copied fixtures or redacted placeholders, not mutation of a live archive.",
        "properties": {
            "safety": { "type": "string" },
            "mutates_live_archive": { "type": "boolean" },
            "requires_explicit_live_archive": { "type": "boolean" },
            "target": { "type": "string" },
            "working_directory": { "type": ["string", "null"] },
            "command_json": {
                "type": "array",
                "items": { "type": "string" }
            },
            "shell_command": { "type": "string" },
            "notes": {
                "type": "array",
                "items": { "type": "string" }
            }
        },
        "required": [
            "safety",
            "mutates_live_archive",
            "requires_explicit_live_archive",
            "target",
            "command_json",
            "shell_command"
        ]
    })
}

pub(crate) fn response_schema_doctor_failure_context() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "Failure context artifact contract for blocked, refused, incomplete, or verification-failed doctor operations. This artifact is redacted by default and is safe to attach to support reports alongside manifest/commands/events artifacts.",
        "properties": {
            "schema_version": { "type": "integer" },
            "context_kind": { "type": ["string", "null"] },
            "scenario_id": { "type": ["string", "null"] },
            "probe_run_id": { "type": ["string", "null"] },
            "operation_id": { "type": ["string", "null"] },
            "failed_phase": { "type": "string" },
            "failed_check": { "type": "string" },
            "reasons": {
                "type": "array",
                "items": { "type": "string" }
            },
            "failure_reason": { "type": ["string", "null"] },
            "command": response_schema_opaque_object(),
            "command_history": response_schema_opaque_object_array(),
            "platform": response_schema_opaque_object(),
            "fixture": response_schema_opaque_object(),
            "artifacts": response_schema_doctor_failure_context_artifacts(),
            "repro": response_schema_doctor_failure_context_repro(),
            "recent_events": response_schema_opaque_object_array(),
            "captured_logs": response_schema_opaque_object_array(),
            "probe": response_schema_opaque_object(),
            "asset_class": { "type": ["string", "null"] },
            "authority_status": { "type": ["string", "null"] },
            "contract_provenance": { "type": ["string", "null"] },
            "blocked_reasons": {
                "type": "array",
                "items": { "type": "string" }
            },
            "command_id": { "type": ["string", "null"] },
            "exit_code": { "type": ["integer", "null"] },
            "plan_fingerprint": { "type": ["string", "null"] },
            "selected_authority": { "type": ["object", "array", "null"], "additionalProperties": true },
            "rejected_authorities": { "type": ["object", "array", "null"], "additionalProperties": true },
            "active_locks": { "type": ["object", "array", "null"], "additionalProperties": true },
            "coverage_summary": { "type": ["object", "array", "null"], "additionalProperties": true },
            "stdout_tail": { "type": ["string", "null"] },
            "stderr_tail": { "type": ["string", "null"] },
            "redaction_status": { "type": ["string", "null"] }
        },
        "required": [
            "schema_version",
            "failed_phase",
            "failed_check",
            "artifacts",
            "repro"
        ],
        "examples": [{
            "schema_version": 1,
            "context_kind": "cass_doctor_post_repair_probe_failure_context",
            "operation_id": "doctor-post-repair-probe-example",
            "failed_phase": "post_repair_probe",
            "failed_check": "rollback-confirmed",
            "failure_reason": "post-repair verification failed",
            "asset_class": "derived_lexical_index",
            "authority_status": "post_repair_verification_failed",
            "contract_provenance": "contract-example",
            "blocked_reasons": ["blocks_promotion", "blocks_cleanup"],
            "artifacts": {
                "artifact_manifest_path": "[cass-data]/doctor/post-repair-probes/example/manifest.json",
                "failure_context_path": "[cass-data]/doctor/post-repair-probes/example/failure_context.json"
            },
            "captured_logs": [{
                "kind": "failure_reason",
                "entries": ["post-repair verification failed"]
            }],
            "repro": {
                "safety": "read-only-redacted-template",
                "mutates_live_archive": false,
                "requires_explicit_live_archive": true,
                "target": "[cass-data]",
                "command_json": ["cass", "doctor", "check", "--json", "--data-dir", "[cass-data]"],
                "shell_command": "cass doctor check --json --data-dir '[cass-data]'",
                "notes": ["Replace [cass-data] with a copied fixture or explicit test data directory."]
            },
            "recent_events": [],
            "redaction_status": "redacted"
        }]
    })
}

pub(crate) fn response_schema_doctor_coverage_snapshot() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "generation_count": { "type": "integer" },
            "reclaim_candidate_count": { "type": "integer" },
            "reclaimable_bytes": { "type": "integer" },
            "retained_bytes": { "type": "integer" },
            "artifact_count": { "type": "integer" },
            "covered_asset_classes": { "type": "array", "items": { "type": "string" } }
        }
    })
}

pub(crate) fn response_schema_doctor_artifact() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "artifact_id": { "type": "string" },
            "artifact_kind": { "type": "string" },
            "asset_class": { "type": "string" },
            "path": { "type": "string" },
            "redacted_path": { "type": "string" },
            "exists": { "type": "boolean" },
            "size_bytes": { "type": ["integer", "null"] },
            "descriptor_blake3": { "type": "string" },
            "expected_content_blake3": { "type": ["string", "null"] },
            "actual_content_blake3": { "type": ["string", "null"] },
            "checksum_status": {
                "type": "string",
                "description": "not_recorded | matched | mismatched | missing"
            }
        },
        "required": ["artifact_id", "artifact_kind", "asset_class", "path", "redacted_path", "exists", "descriptor_blake3", "checksum_status"]
    })
}

pub(crate) fn response_schema_doctor_artifact_manifest() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "schema_version": { "type": "integer" },
            "artifact_count": { "type": "integer" },
            "manifest_blake3": { "type": "string" },
            "drift_detection_status": {
                "type": "string",
                "description": "not_checked | verified | checksum_mismatch | missing_artifact | manifest_mismatch"
            },
            "artifacts": {
                "type": "array",
                "items": response_schema_doctor_artifact()
            }
        }
    })
}

pub(crate) fn response_schema_doctor_safety_gate() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "mode": { "type": "string" },
            "asset_class": { "type": "string" },
            "allowed_by_mode": { "type": "boolean" },
            "allowed_by_taxonomy": { "type": "boolean" },
            "path_safe": { "type": "boolean" },
            "approval_requirement": { "type": "string" },
            "approval_fingerprint": { "type": "string" },
            "passed": { "type": "boolean" },
            "blocked_reasons": { "type": "array", "items": { "type": "string" } }
        }
    })
}

pub(crate) fn response_schema_doctor_action() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "action_id": { "type": "string" },
            "action_kind": { "type": "string" },
            "status": {
                "type": "string",
                "description": "planned | applied | skipped | blocked | failed | refused"
            },
            "mode": { "type": "string" },
            "asset_class": { "type": "string" },
            "target_path": { "type": "string" },
            "redacted_target_path": { "type": "string" },
            "reason": { "type": "string" },
            "authority_decision": { "type": "string" },
            "selected_authorities": { "type": "array", "items": { "type": "string" } },
            "rejected_authorities": { "type": "array", "items": { "type": "string" } },
            "safety_gate": response_schema_doctor_safety_gate(),
            "planned_bytes": { "type": "integer" },
            "bytes_copied": { "type": "integer" },
            "bytes_moved": { "type": "integer" },
            "bytes_pruned": { "type": "integer" },
            "backup_paths": { "type": "array", "items": { "type": "string" } },
            "verification_outcome": { "type": "string" },
            "remaining_risk": { "type": "array", "items": { "type": "string" } },
            "artifacts": { "type": "array", "items": response_schema_doctor_artifact() }
        }
    })
}

pub(crate) fn response_schema_doctor_event_log_metadata() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "path": { "type": ["string", "null"] },
            "checksum_blake3": { "type": ["string", "null"] },
            "hash_chain_tip": { "type": ["string", "null"] },
            "status": { "type": "string" },
            "event_count": { "type": "integer" },
            "events": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "schema_version": { "type": "integer" },
                        "event_id": { "type": "string" },
                        "previous_event_hash": { "type": ["string", "null"] },
                        "operation_id": { "type": "string" },
                        "action_id": { "type": ["string", "null"] },
                        "phase": { "type": "string" },
                        "mode": { "type": "string" },
                        "asset_class": { "type": ["string", "null"] },
                        "redacted_target_path": { "type": ["string", "null"] },
                        "elapsed_ms": { "type": ["integer", "null"] },
                        "progress_label": { "type": "string" },
                        "safety_gate_passed": { "type": ["boolean", "null"] },
                        "blocked_reasons": { "type": "array", "items": { "type": "string" } },
                        "receipt_correlation_id": { "type": ["string", "null"] },
                        "artifact_ids": { "type": "array", "items": { "type": "string" } }
                    }
                }
            }
        }
    })
}

pub(crate) fn response_schema_doctor_forensic_bundle_metadata() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "bundle_id": { "type": ["string", "null"] },
            "path": { "type": ["string", "null"] },
            "redacted_path": { "type": ["string", "null"] },
            "manifest_path": { "type": ["string", "null"] },
            "redacted_manifest_path": { "type": ["string", "null"] },
            "checksum_blake3": { "type": ["string", "null"] },
            "status": { "type": "string" },
            "created_at_ms": { "type": ["integer", "null"] },
            "artifact_count": { "type": "integer" },
            "copied_artifact_count": { "type": "integer" },
            "missing_artifact_count": { "type": "integer" },
            "failed_artifact_count": { "type": "integer" },
            "sidecar_complete": { "type": "boolean" },
            "redaction_status": { "type": "string" },
            "blocked_reason": { "type": ["string", "null"] },
            "artifacts": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "artifact_kind": { "type": "string" },
                        "source_path": { "type": "string" },
                        "redacted_source_path": { "type": "string" },
                        "bundle_relative_path": { "type": ["string", "null"] },
                        "exists": { "type": "boolean" },
                        "copied": { "type": "boolean" },
                        "required": { "type": "boolean" },
                        "sidecar_group": { "type": ["string", "null"] },
                        "size_bytes": { "type": ["integer", "null"] },
                        "checksum_blake3": { "type": ["string", "null"] },
                        "error": { "type": ["string", "null"] }
                    }
                }
            }
        }
    })
}

pub(crate) fn response_schema_doctor_plan() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "plan_kind": { "type": "string" },
            "schema_version": { "type": "integer" },
            "mode": { "type": "string" },
            "approval_requirement": { "type": "string" },
            "approval_fingerprint": { "type": "string" },
            "plan_fingerprint": { "type": "string" },
            "fingerprint_algorithm": { "type": "string" },
            "outcome_contract": { "type": "string" },
            "coverage_before": response_schema_doctor_coverage_snapshot(),
            "safety_gates": { "type": "array", "items": response_schema_doctor_safety_gate() },
            "actions": { "type": "array", "items": response_schema_doctor_action() },
            "artifact_manifest": response_schema_doctor_artifact_manifest(),
            "event_log": response_schema_doctor_event_log_metadata(),
            "forensic_bundle": response_schema_doctor_forensic_bundle_metadata(),
            "selected_authorities": { "type": "array", "items": { "type": "string" } },
            "rejected_authorities": { "type": "array", "items": { "type": "string" } },
            "blocked_reasons": { "type": "array", "items": { "type": "string" } },
            "remaining_risk": { "type": "array", "items": { "type": "string" } }
        }
    })
}

pub(crate) fn response_schema_doctor_repair_plan_preview() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "cass doctor repair dry-run/apply approval plan. plan_fingerprint is a canonical hash of fingerprint_inputs and must match before apply mutates anything.",
        "properties": {
            "schema_version": { "type": "integer" },
            "plan_kind": { "type": "string" },
            "mode": { "type": "string" },
            "dry_run": { "type": "boolean" },
            "apply_requested": { "type": "boolean" },
            "approval_required": { "type": "boolean" },
            "approval_status": { "type": "string", "description": "dry_run_only | missing_plan_fingerprint | matched | mismatched | blocked | lock_uncertain" },
            "provided_plan_fingerprint": { "type": ["string", "null"] },
            "plan_fingerprint": { "type": "string" },
            "fingerprint_algorithm": { "type": "string" },
            "fingerprint_inputs": response_schema_opaque_object(),
            "exact_apply_command": { "type": "string" },
            "apply_argv": { "type": "array", "items": { "type": "string" } },
            "will_mutate": { "type": "boolean" },
            "apply_authorized": { "type": "boolean" },
            "planned_action_count": { "type": "integer" },
            "planned_actions": { "type": "array", "items": response_schema_opaque_object() },
            "selected_authorities": { "type": "array", "items": { "type": "string" } },
            "rejected_authorities": { "type": "array", "items": { "type": "string" } },
            "blocked_reasons": { "type": "array", "items": { "type": "string" } },
            "branchable_blocker_codes": { "type": "array", "items": { "type": "string" } },
            "warnings": { "type": "array", "items": { "type": "string" } },
            "freshness_policy": { "type": "array", "items": { "type": "string" } },
            "forensic_bundle_required": { "type": "boolean" },
            "post_repair_probes_required": { "type": "boolean" },
            "never_prunes_source_evidence": { "type": "boolean" }
        }
    })
}

pub(crate) fn response_schema_doctor_receipt() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "receipt_kind": { "type": "string" },
            "schema_version": { "type": "integer" },
            "mode": { "type": "string" },
            "outcome_kind": { "type": "string" },
            "approval_fingerprint": { "type": "string" },
            "plan_fingerprint": { "type": "string" },
            "started_at_ms": { "type": ["integer", "null"] },
            "finished_at_ms": { "type": ["integer", "null"] },
            "duration_ms": { "type": ["integer", "null"] },
            "planned_action_count": { "type": "integer" },
            "applied_action_count": { "type": "integer" },
            "skipped_action_count": { "type": "integer" },
            "failed_action_count": { "type": "integer" },
            "bytes_planned": { "type": "integer" },
            "bytes_copied": { "type": "integer" },
            "bytes_moved": { "type": "integer" },
            "bytes_pruned": { "type": "integer" },
            "reclaimed_bytes": { "type": "integer" },
            "backup_paths": { "type": "array", "items": { "type": "string" } },
            "selected_authorities": { "type": "array", "items": { "type": "string" } },
            "rejected_authorities": { "type": "array", "items": { "type": "string" } },
            "verification_outcomes": { "type": "array", "items": { "type": "string" } },
            "remaining_risk": { "type": "array", "items": { "type": "string" } },
            "event_log": response_schema_doctor_event_log_metadata(),
            "forensic_bundle": response_schema_doctor_forensic_bundle_metadata(),
            "artifact_manifest": response_schema_doctor_artifact_manifest(),
            "artifact_checksums": { "type": "array", "items": response_schema_doctor_artifact() },
            "drift_detection_status": { "type": "string" },
            "coverage_before": response_schema_doctor_coverage_snapshot(),
            "coverage_after": response_schema_doctor_coverage_snapshot(),
            "actions": { "type": "array", "items": response_schema_doctor_action() },
            "action_status_counts": { "type": "object", "additionalProperties": { "type": "integer" } },
            "blocked_reasons": { "type": "array", "items": { "type": "string" } }
        }
    })
}

pub(crate) fn response_schema_doctor_operation_outcome() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "Stable doctor operation outcome. Robots should branch on kind before consulting prose fields.",
        "properties": {
            "kind": { "type": "string", "description": "Stable kebab-case outcome kind such as ok-no-action-needed, ok-read-only-diagnosed, fixed, partially-fixed, repair-blocked, repair-refused, repair-incomplete, verification-failed, cleanup-dry-run-only, cleanup-refused, auto-run-skipped, support-bundle-only, baseline-diff-only, or requires-manual-review." },
            "reason": { "type": "string" },
            "action_taken": { "type": "string" },
            "action_not_taken": { "type": "string" },
            "safe_to_retry": { "type": "boolean" },
            "requires_override": { "type": "boolean" },
            "data_loss_risk": { "type": "string", "description": "none | low | medium | high | unknown" },
            "next_command": { "type": ["string", "null"] },
            "artifact_manifest_path": { "type": ["string", "null"] },
            "exit_code_kind": { "type": "string", "description": "success | health-failure | usage-error | lock-busy | repair-failure" }
        },
        "required": ["kind", "reason", "action_taken", "action_not_taken", "safe_to_retry", "requires_override", "data_loss_risk", "next_command", "artifact_manifest_path", "exit_code_kind"]
    })
}

pub(crate) fn response_schema_doctor_repair_failure_marker() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "Durable marker summary for a previous failed mutating doctor repair. Mutating doctor runs refuse the same repair class by default when found=true.",
        "properties": {
            "schema_version": { "type": "integer" },
            "repair_class": { "type": "string" },
            "found": { "type": "boolean" },
            "parse_status": { "type": "string", "description": "absent | ok | corrupt" },
            "path": { "type": ["string", "null"] },
            "redacted_path": { "type": ["string", "null"] },
            "operation_id": { "type": ["string", "null"] },
            "plan_fingerprint": { "type": ["string", "null"] },
            "failed_at_ms": { "type": ["integer", "null"] },
            "failed_at": { "type": ["string", "null"] },
            "cass_version": { "type": ["string", "null"] },
            "platform": { "type": ["string", "null"] },
            "user_data_modified": { "type": ["boolean", "null"] },
            "failed_checks": { "type": "array", "items": { "type": "string" } },
            "corrupt_reason": { "type": ["string", "null"] }
        },
        "required": ["schema_version", "repair_class", "found", "parse_status", "path", "redacted_path", "operation_id", "plan_fingerprint", "failed_at_ms", "failed_at", "cass_version", "platform", "user_data_modified", "failed_checks", "corrupt_reason"]
    })
}

pub(crate) fn response_schema_doctor_operation_state() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "Read-only doctor concurrency and interrupted-operation state. Mutating doctor modes must require mutating_doctor_allowed=true before attempting rebuild, cleanup, restore, or promotion.",
        "properties": {
            "schema_version": { "type": "integer" },
            "read_only_check_allowed": { "type": "boolean" },
            "mutating_doctor_allowed": { "type": "boolean" },
            "active_index_maintenance": { "type": "boolean" },
            "active_rebuild": { "type": "boolean" },
            "active_watch": { "type": "boolean" },
            "active_doctor_repair": { "type": "boolean" },
            "owner_count": { "type": "integer" },
            "stale_owner_count": { "type": "integer" },
            "interrupted_state_count": { "type": "integer" },
            "mutation_blocked_reason": { "type": ["string", "null"] },
            "owners": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "lock_kind": { "type": "string", "description": "index_run | watch_ingestion | doctor_repair" },
                        "lock_path": { "type": "string" },
                        "redacted_lock_path": { "type": "string" },
                        "active": { "type": "boolean" },
                        "owned_by_current_process": { "type": "boolean" },
                        "owner_confidence": { "type": "string", "description": "current_process | active_advisory_lock | active_missing_metadata | stale_metadata_only | unavailable" },
                        "pid": { "type": ["integer", "null"] },
                        "owner_command": { "type": ["string", "null"], "description": "Command recorded by cass-owned lock metadata when safe to report; cass does not scrape arbitrary process command lines." },
                        "started_at_ms": { "type": ["integer", "null"] },
                        "started_at": { "type": ["string", "null"] },
                        "updated_at_ms": { "type": ["integer", "null"] },
                        "updated_at": { "type": ["string", "null"] },
                        "mode": { "type": ["string", "null"] },
                        "job_id": { "type": ["string", "null"] },
                        "job_kind": { "type": ["string", "null"] },
                        "phase": { "type": ["string", "null"] },
                        "db_path": { "type": ["string", "null"] },
                        "db_path_matches_requested": { "type": ["boolean", "null"] },
                        "evidence": { "type": "array", "items": { "type": "string" } },
                        "next_action": { "type": "string" }
                    }
                }
            },
            "interrupted_states": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "kind": { "type": "string" },
                        "path": { "type": "string" },
                        "redacted_path": { "type": "string" },
                        "disposition": { "type": "string" },
                        "blocks_mutation": { "type": "boolean" },
                        "safe_to_delete_automatically": { "type": "boolean" },
                        "evidence": { "type": "array", "items": { "type": "string" } },
                        "next_action": { "type": "string" }
                    }
                }
            },
            "next_action": { "type": "string" },
            "notes": { "type": "array", "items": { "type": "string" } }
        },
        "required": [
            "schema_version",
            "read_only_check_allowed",
            "mutating_doctor_allowed",
            "active_index_maintenance",
            "active_rebuild",
            "active_watch",
            "active_doctor_repair",
            "owner_count",
            "stale_owner_count",
            "interrupted_state_count",
            "mutation_blocked_reason",
            "owners",
            "interrupted_states",
            "next_action",
            "notes"
        ]
    })
}

pub(crate) fn response_schema_doctor_locks() -> serde_json::Value {
    serde_json::json!({
        "type": "array",
        "description": "Structured doctor lock diagnostics. Paths are paired with redacted paths; robots should display redacted_lock_path and never advise deleting lock files manually.",
        "items": {
            "type": "object",
            "properties": {
                "schema_version": { "type": "integer" },
                "lock_kind": { "type": "string", "description": "index_run | watch_ingestion | doctor_repair" },
                "lock_path": { "type": "string" },
                "redacted_lock_path": { "type": "string" },
                "active": { "type": "boolean" },
                "owned_by_current_process": { "type": "boolean" },
                "owner_confidence": { "type": "string", "description": "current_process | active_advisory_lock | active_missing_metadata | stale_metadata_only | unavailable" },
                "pid": { "type": ["integer", "null"] },
                "owner_command": { "type": ["string", "null"] },
                "pid_metadata_status": { "type": "string" },
                "pid_reuse_ambiguous": { "type": "boolean" },
                "age_ms": { "type": ["integer", "null"] },
                "last_heartbeat_age_ms": { "type": ["integer", "null"] },
                "stale_suspected": { "type": ["boolean", "null"] },
                "wait_duration_ms": { "type": ["integer", "null"] },
                "retry_policy": { "type": "string" },
                "safe_to_wait": { "type": "boolean" },
                "manual_delete_allowed": { "type": "boolean" },
                "recommended_action": { "type": "string" },
                "evidence": { "type": "array", "items": { "type": "string" } }
            },
            "required": ["schema_version", "lock_kind", "lock_path", "redacted_lock_path", "active", "owned_by_current_process", "owner_confidence", "pid", "owner_command", "pid_metadata_status", "pid_reuse_ambiguous", "age_ms", "last_heartbeat_age_ms", "stale_suspected", "wait_duration_ms", "retry_policy", "safe_to_wait", "manual_delete_allowed", "recommended_action", "evidence"]
        }
    })
}

pub(crate) fn response_schema_doctor_timing_summary() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "schema_version": { "type": "integer" },
            "elapsed_ms": { "type": "integer" },
            "measured_operation_count": { "type": "integer" },
            "slow_operation_count": { "type": "integer" },
            "default_threshold_ms": { "type": "integer" },
            "slowest_operation": { "type": ["string", "null"] },
            "slowest_elapsed_ms": { "type": ["integer", "null"] },
            "notes": { "type": "array", "items": { "type": "string" } }
        },
        "required": ["schema_version", "elapsed_ms", "measured_operation_count", "slow_operation_count", "default_threshold_ms", "slowest_operation", "slowest_elapsed_ms", "notes"]
    })
}

pub(crate) fn response_schema_doctor_slow_operations() -> serde_json::Value {
    serde_json::json!({
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "schema_version": { "type": "integer" },
                "name": { "type": "string" },
                "phase": { "type": "string" },
                "elapsed_ms": { "type": "integer" },
                "threshold_ms": { "type": "integer" },
                "severity": { "type": "string" },
                "event_log_correlation_id": { "type": "string" },
                "recommended_action": { "type": "string" }
            },
            "required": ["schema_version", "name", "phase", "elapsed_ms", "threshold_ms", "severity", "event_log_correlation_id", "recommended_action"]
        }
    })
}

pub(crate) fn response_schema_doctor_retry_recommendation() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "schema_version": { "type": "integer" },
            "policy": { "type": "string" },
            "safe_to_retry": { "type": "boolean" },
            "retry_after_ms": { "type": ["integer", "null"] },
            "command": { "type": ["string", "null"] },
            "reason": { "type": "string" },
            "notes": { "type": "array", "items": { "type": "string" } }
        },
        "required": ["schema_version", "policy", "safe_to_retry", "retry_after_ms", "command", "reason", "notes"]
    })
}

pub(crate) fn response_schema_doctor_cleanup_apply() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "mode": { "type": "string" },
            "approval_requirement": { "type": "string" },
            "outcome_kind": { "type": "string" },
            "operation_outcome": response_schema_doctor_operation_outcome(),
            "retry_safety": { "type": "string" },
            "approval_fingerprint": { "type": "string" },
            "operation_started_at_ms": { "type": ["integer", "null"] },
            "operation_finished_at_ms": { "type": ["integer", "null"] },
            "planned_actions": { "type": "array", "items": response_schema_opaque_object() },
            "actions": { "type": "array", "items": response_schema_opaque_object() },
            "plan": response_schema_doctor_plan(),
            "receipt": response_schema_doctor_receipt()
        }
    })
}

pub(crate) fn response_schema_doctor_source_inventory() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "Read-only source coverage inventory. Missing upstream provider files are reported as coverage/prune risk, not loss of conversations already archived in cass.",
        "properties": {
            "schema_version": { "type": "integer" },
            "db_available": { "type": "boolean" },
            "db_query_error": { "type": "string" },
            "total_indexed_conversations": { "type": "integer" },
            "provider_counts": { "type": "object", "additionalProperties": { "type": "integer" } },
            "missing_current_source_count": {
                "type": "integer",
                "description": "Local indexed conversations whose original provider source_path is no longer visible on this machine."
            },
            "unknown_mapping_count": { "type": "integer" },
            "remote_source_count": { "type": "integer" },
            "local_source_count": { "type": "integer" },
            "detected_provider_root_count": { "type": "integer" },
            "providers": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "provider": { "type": "string" },
                        "stable_source_id": { "type": "string" },
                        "indexed_conversation_count": { "type": "integer" },
                        "missing_current_source_count": { "type": "integer" },
                        "unknown_mapping_count": { "type": "integer" },
                        "remote_source_count": { "type": "integer" },
                        "local_source_count": { "type": "integer" },
                        "detected_root_count": { "type": "integer" },
                        "detected_roots": { "type": "array", "items": response_schema_opaque_object() },
                        "prune_risk": response_schema_opaque_object(),
                        "notes": { "type": "array", "items": { "type": "string" } }
                    }
                }
            },
            "sources": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "stable_source_id": { "type": "string" },
                        "source_id": { "type": "string" },
                        "origin_kind": { "type": "string" },
                        "origin_host": { "type": ["string", "null"] },
                        "is_remote": { "type": "boolean" },
                        "providers": { "type": "array", "items": { "type": "string" } },
                        "conversation_count": { "type": "integer" },
                        "missing_current_source_count": { "type": "integer" },
                        "unknown_mapping_count": { "type": "integer" }
                    }
                }
            },
            "detected_roots": { "type": "array", "items": response_schema_opaque_object() },
            "notes": { "type": "array", "items": { "type": "string" } }
        },
        "required": [
            "schema_version",
            "db_available",
            "total_indexed_conversations",
            "provider_counts",
            "missing_current_source_count",
            "unknown_mapping_count",
            "providers",
            "sources",
            "detected_roots",
            "notes"
        ]
    })
}

pub(crate) fn response_schema_doctor_config_exclusion_risk() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "Read-only preservation warning for backup, sync, ignore, or cache placement rules that may exclude cass archival evidence. auto_fix_available is always false.",
        "properties": {
            "schema_version": { "type": "integer" },
            "path": { "type": "string" },
            "risk_kind": {
                "type": "string",
                "description": "Stable code such as volatile-cache-location, repo-ignore-excludes-cass-evidence, backup-filter-excludes-cass-evidence, cass-config-excludes-cass-evidence, or sources-config-excludes-cass-evidence."
            },
            "evidence": { "type": "array", "items": { "type": "string" } },
            "confidence": { "type": "string", "description": "high | medium | low, expressing how directly doctor observed the exclusion rule." },
            "recommended_action": { "type": "string" },
            "auto_fix_available": { "type": "boolean" }
        },
        "required": ["schema_version", "path", "risk_kind", "evidence", "confidence", "recommended_action", "auto_fix_available"]
    })
}

pub(crate) fn response_schema_doctor_detail_output() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "Bounds for per-record doctor JSON details. Summary counts and safety decisions always use the complete internal evidence collections.",
        "properties": {
            "mode": { "type": "string", "description": "bounded_summary | full_verbose" },
            "limit_per_collection": { "type": ["integer", "null"] },
            "details_truncated": { "type": "boolean" },
            "raw_mirror_backfill_receipts_total": { "type": "integer" },
            "raw_mirror_backfill_receipts_returned": { "type": "integer" },
            "raw_mirror_backfill_receipts_omitted": { "type": "integer" },
            "sole_copy_warnings_total": { "type": "integer" },
            "sole_copy_warnings_returned": { "type": "integer" },
            "sole_copy_warnings_omitted": { "type": "integer" },
            "full_details_command": { "type": ["string", "null"] },
            "note": { "type": "string" }
        },
        "required": [
            "mode",
            "limit_per_collection",
            "details_truncated",
            "raw_mirror_backfill_receipts_total",
            "raw_mirror_backfill_receipts_returned",
            "raw_mirror_backfill_receipts_omitted",
            "sole_copy_warnings_total",
            "sole_copy_warnings_returned",
            "sole_copy_warnings_omitted",
            "full_details_command",
            "note"
        ]
    })
}

pub(crate) fn response_schema_doctor_raw_mirror_backfill() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "Legacy archive raw-mirror backfill report. Backfill is additive under cass raw-mirror only and never mutates external provider session logs.",
        "properties": {
            "schema_version": { "type": "integer" },
            "backfill_generation": { "type": "integer" },
            "mode": { "type": "string", "description": "check | safe_auto_run" },
            "status": { "type": "string", "description": "skipped | clean | planned | applied | partial | warn | blocked" },
            "db_available": { "type": "boolean" },
            "db_query_error": { "type": "string" },
            "total_candidate_count": { "type": "integer" },
            "already_raw_source_captured_count": { "type": "integer" },
            "eligible_live_source_count": { "type": "integer" },
            "existing_raw_manifest_link_count": { "type": "integer" },
            "captured_live_source_count": { "type": "integer" },
            "already_present_capture_count": { "type": "integer" },
            "source_missing_count": { "type": "integer" },
            "db_projection_only_count": { "type": "integer" },
            "remote_or_unknown_mapping_count": { "type": "integer" },
            "changed_source_hash_count": { "type": "integer" },
            "capture_failure_count": { "type": "integer" },
            "external_source_mutation_count": { "type": "integer" },
            "read_only_external_source_dirs": { "type": "boolean" },
            "forensic_bundle": response_schema_doctor_forensic_bundle_metadata(),
            "receipts": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "stable_record_id": { "type": "string" },
                        "conversation_id": { "type": ["integer", "null"] },
                        "provider": { "type": "string" },
                        "source_id": { "type": "string" },
                        "origin_kind": { "type": "string" },
                        "origin_host": { "type": ["string", "null"] },
                        "redacted_source_path": { "type": ["string", "null"] },
                        "source_path_blake3": { "type": ["string", "null"] },
                        "message_count": { "type": "integer" },
                        "started_at_ms": { "type": ["integer", "null"] },
                        "action": { "type": "string" },
                        "raw_source_captured": { "type": "boolean" },
                        "raw_mirror_db_linked": { "type": "boolean" },
                        "source_missing": { "type": "boolean" },
                        "db_projection_only": { "type": "boolean" },
                        "parse_loss_unknown": { "type": "boolean" },
                        "captured_at_ms": { "type": ["integer", "null"] },
                        "source_stat_snapshot": response_schema_opaque_object(),
                        "raw_mirror_manifest_id": { "type": ["string", "null"] },
                        "raw_mirror_manifest_relative_path": { "type": ["string", "null"] },
                        "raw_mirror_blob_blake3": { "type": ["string", "null"] },
                        "raw_mirror_blob_size_bytes": { "type": ["integer", "null"] },
                        "raw_mirror_source_content_blake3": { "type": ["string", "null"] },
                        "raw_mirror_source_size_bytes": { "type": ["integer", "null"] },
                        "raw_mirror_storage_kind": { "type": ["string", "null"] },
                        "raw_mirror_chunk_count": { "type": ["integer", "null"] },
                        "backfill_generation": { "type": "integer" },
                        "forensic_bundle": response_schema_doctor_forensic_bundle_metadata(),
                        "warnings": { "type": "array", "items": { "type": "string" } }
                    }
                }
            },
            "warnings": { "type": "array", "items": { "type": "string" } },
            "notes": { "type": "array", "items": { "type": "string" } }
        },
        "required": [
            "schema_version",
            "backfill_generation",
            "mode",
            "status",
            "db_available",
            "total_candidate_count",
            "already_raw_source_captured_count",
            "eligible_live_source_count",
            "captured_live_source_count",
            "source_missing_count",
            "db_projection_only_count",
            "external_source_mutation_count",
            "read_only_external_source_dirs",
            "forensic_bundle",
            "receipts",
            "warnings",
            "notes"
        ]
    })
}

pub(crate) fn response_schema_doctor_coverage_summary() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "Source coverage ledger summary comparing archive DB rows, raw mirror db links, and currently visible upstream files.",
        "properties": {
            "schema_version": { "type": "integer" },
            "confidence_tier": { "type": "string" },
            "archive_conversation_count": { "type": "integer" },
            "archived_message_count": { "type": "integer" },
            "provider_count": { "type": "integer" },
            "source_identity_count": { "type": "integer" },
            "visible_current_source_count": { "type": "integer" },
            "visible_current_source_bytes": { "type": "integer" },
            "raw_mirror_manifest_count": { "type": "integer" },
            "raw_mirror_db_link_count": { "type": "integer" },
            "db_without_raw_mirror_count": { "type": "integer" },
            "db_projection_only_count": { "type": "integer" },
            "mirror_without_db_link_count": { "type": "integer" },
            "mirror_alias_of_linked_count": { "type": "integer" },
            "missing_current_source_count": { "type": "integer" },
            "sole_copy_candidate_count": { "type": "integer" },
            "current_source_newer_than_archive_count": { "type": "integer" },
            "remote_source_count": { "type": "integer" },
            "unknown_mapping_count": { "type": "integer" },
            "earliest_started_at_ms": { "type": ["integer", "null"] },
            "latest_started_at_ms": { "type": ["integer", "null"] },
            "coverage_reducing_live_source_rebuild_refused": { "type": "boolean" },
            "recommended_action": { "type": "string" },
            "notes": { "type": "array", "items": { "type": "string" } }
        }
    })
}

pub(crate) fn response_schema_doctor_coverage_risk() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "Concise coverage risk summary suitable for status/health and doctor top-level routing.",
        "properties": {
            "schema_version": { "type": "integer" },
            "status": { "type": "string" },
            "confidence_tier": { "type": "string" },
            "archive_conversation_count": { "type": "integer" },
            "missing_current_source_count": { "type": "integer" },
            "db_without_raw_mirror_count": { "type": "integer" },
            "db_projection_only_count": { "type": "integer" },
            "mirror_without_db_link_count": { "type": "integer" },
            "current_source_newer_than_archive_count": { "type": "integer" },
            "raw_mirror_db_link_count": { "type": "integer" },
            "sole_copy_warning_count": { "type": "integer" },
            "raw_mirror_capture_disabled": {
                "type": "boolean",
                "description": "CASS_RAW_MIRROR turned raw capture off: new session files are not copied into the raw mirror, so archive rows without a raw copy are expected and a file its provider deletes survives only in the archive DB."
            },
            "recommended_action": { "type": "string" }
        }
    })
}

pub(crate) fn response_schema_doctor_candidate_live_inventory() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "db_exists": { "type": "boolean" },
            "db_size_bytes": { "type": "integer" },
            "db_blake3": { "type": ["string", "null"] },
            "db_wal_exists": { "type": "boolean" },
            "db_wal_size_bytes": { "type": "integer" },
            "db_wal_blake3": { "type": ["string", "null"] },
            "db_shm_exists": { "type": "boolean" },
            "db_shm_size_bytes": { "type": "integer" },
            "db_shm_blake3": { "type": ["string", "null"] },
            "index_exists": { "type": "boolean" },
            "index_size_bytes": { "type": "integer" }
        }
    })
}

pub(crate) fn response_schema_doctor_candidate_coverage_snapshot() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "Compact before/after archive coverage point for a reconstruct candidate. It contains counts and confidence metadata only, never raw session text.",
        "properties": {
            "coverage_source": { "type": "string" },
            "conversation_count": { "type": ["integer", "null"] },
            "message_count": { "type": ["integer", "null"] },
            "raw_mirror_manifest_count": { "type": "integer" },
            "raw_mirror_db_link_count": { "type": "integer" },
            "missing_current_source_count": { "type": "integer" },
            "confidence_tier": { "type": "string" }
        }
    })
}

pub(crate) fn response_schema_doctor_coverage_comparison_gate() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "Promotion safety gate comparing a staged candidate against current archive coverage. Negative canonical archive deltas block promotion; derived index deltas are warnings because derived assets are rebuildable.",
        "properties": {
            "schema_version": { "type": "integer" },
            "status": { "type": "string", "description": "pass | warn | blocked" },
            "promote_allowed": { "type": "boolean" },
            "safe_to_inspect": { "type": "boolean" },
            "confidence_tier": { "type": "string" },
            "selected_authority": { "type": "string" },
            "selected_authority_decision": { "type": "string" },
            "archive_conversation_count": { "type": "integer" },
            "candidate_conversation_count": { "type": ["integer", "null"] },
            "conversation_delta": { "type": ["integer", "null"] },
            "archived_message_count": { "type": "integer" },
            "candidate_message_count": { "type": ["integer", "null"] },
            "message_delta": { "type": ["integer", "null"] },
            "candidate_lexical_document_count": { "type": ["integer", "null"] },
            "lexical_document_delta": { "type": ["integer", "null"] },
            "candidate_semantic_vector_count": { "type": ["integer", "null"] },
            "semantic_vector_delta": { "type": ["integer", "null"] },
            "provider_count": { "type": "integer" },
            "source_identity_count": { "type": "integer" },
            "visible_current_source_count": { "type": "integer" },
            "raw_mirror_db_link_count": { "type": "integer" },
            "missing_current_source_count": { "type": "integer" },
            "db_without_raw_mirror_count": { "type": "integer" },
            "db_projection_only_count": { "type": "integer" },
            "mirror_without_db_link_count": { "type": "integer" },
            "sole_copy_candidate_count": { "type": "integer" },
            "current_source_newer_than_archive_count": { "type": "integer" },
            "earliest_started_at_ms": { "type": ["integer", "null"] },
            "latest_started_at_ms": { "type": ["integer", "null"] },
            "blocking_reasons": { "type": "array", "items": { "type": "string" } },
            "warning_reasons": { "type": "array", "items": { "type": "string" } },
            "evidence": { "type": "array", "items": { "type": "string" } },
            "notes": { "type": "array", "items": { "type": "string" } }
        }
    })
}

pub(crate) fn response_schema_doctor_candidate_staging() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "Isolated reconstruct candidate staging report. Candidate builds may write only under doctor/candidates and are retained for inspection; doctor never auto-deletes candidate evidence.",
        "properties": {
            "schema_version": { "type": "integer" },
            "status": { "type": "string", "description": "absent | empty | ready | needs_inspection | blocked_candidates_present | unsafe | warn" },
            "root_path": { "type": "string" },
            "redacted_root_path": { "type": "string" },
            "exists": { "type": "boolean" },
            "total_candidate_count": { "type": "integer" },
            "completed_candidate_count": { "type": "integer" },
            "in_progress_candidate_count": { "type": "integer" },
            "interrupted_candidate_count": { "type": "integer" },
            "blocked_candidate_count": { "type": "integer" },
            "orphaned_candidate_count": { "type": "integer" },
            "latest_build": {
                "type": ["object", "null"],
                "properties": {
                    "requested": { "type": "boolean" },
                    "status": { "type": "string" },
                    "candidate_id": { "type": ["string", "null"] },
                    "path": { "type": ["string", "null"] },
                    "redacted_path": { "type": ["string", "null"] },
                    "manifest_path": { "type": ["string", "null"] },
                    "redacted_manifest_path": { "type": ["string", "null"] },
                    "manifest_blake3": { "type": ["string", "null"] },
                    "selected_authority": { "type": "string" },
                    "selected_authority_decision": { "type": "string" },
                    "selected_authority_evidence": { "type": "array", "items": { "type": "string" } },
                    "evidence_sources": { "type": "array", "items": { "type": "string" } },
                    "coverage_before": response_schema_doctor_candidate_coverage_snapshot(),
                    "coverage_after": response_schema_doctor_candidate_coverage_snapshot(),
                    "confidence": { "type": "string" },
                    "artifact_count": { "type": "integer" },
                    "checksum_count": { "type": "integer" },
                    "skipped_record_count": { "type": "integer" },
                    "parse_error_count": { "type": "integer" },
                    "frankensqlite_open_ok": { "type": "boolean" },
                    "frankensqlite_write_ok": { "type": "boolean" },
                    "candidate_conversation_count": { "type": ["integer", "null"] },
                    "candidate_message_count": { "type": ["integer", "null"] },
                    "coverage_gate": response_schema_doctor_coverage_comparison_gate(),
                    "live_inventory_before": response_schema_doctor_candidate_live_inventory(),
                    "live_inventory_after": response_schema_doctor_candidate_live_inventory(),
                    "live_inventory_unchanged": { "type": "boolean" },
                    "fs_mutation_receipts": { "type": "array", "items": response_schema_opaque_object() },
                    "warnings": { "type": "array", "items": { "type": "string" } },
                    "notes": { "type": "array", "items": { "type": "string" } }
                }
            },
            "candidates": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "candidate_id": { "type": "string" },
                        "lifecycle_status": { "type": "string" },
                        "path": { "type": "string" },
                        "redacted_path": { "type": "string" },
                        "manifest_path": { "type": ["string", "null"] },
                        "redacted_manifest_path": { "type": ["string", "null"] },
                        "manifest_blake3": { "type": ["string", "null"] },
                        "artifact_count": { "type": "integer" },
                        "checksum_count": { "type": "integer" },
                        "selected_authority": { "type": ["string", "null"] },
                        "created_at_ms": { "type": ["integer", "null"] },
                        "updated_at_ms": { "type": ["integer", "null"] },
                        "orphaned": { "type": "boolean" },
                        "interrupted": { "type": "boolean" },
                        "safe_to_delete_automatically": { "type": "boolean" },
                        "evidence": { "type": "array", "items": { "type": "string" } },
                        "next_action": { "type": "string" }
                    }
                }
            },
            "warnings": { "type": "array", "items": { "type": "string" } },
            "notes": { "type": "array", "items": { "type": "string" } }
        },
        "required": [
            "schema_version",
            "status",
            "root_path",
            "redacted_root_path",
            "exists",
            "total_candidate_count",
            "completed_candidate_count",
            "in_progress_candidate_count",
            "interrupted_candidate_count",
            "blocked_candidate_count",
            "orphaned_candidate_count",
            "latest_build",
            "candidates",
            "warnings",
            "notes"
        ]
    })
}

pub(crate) fn response_schema_doctor_raw_mirror() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "Read-only raw session mirror layout and verification report. Raw mirror blobs are precious archive evidence, not derived cleanup candidates.",
        "properties": {
            "schema_version": { "type": "integer" },
            "status": { "type": "string" },
            "redacted_root_path": { "type": "string" },
            "exists": { "type": "boolean" },
            "sensitive_paths_included": { "type": "boolean" },
            "raw_content_included": { "type": "boolean" },
            "layout": {
                "type": "object",
                "properties": {
                    "root_relative_path": { "type": "string" },
                    "manifest_kind": { "type": "string" },
                    "hash_algorithm": { "type": "string" },
                    "blob_path_template": { "type": "string" },
                    "manifest_path_template": { "type": "string" },
                    "verification_path_template": { "type": "string" },
                    "temp_path_template": { "type": "string" },
                    "content_address_scope": { "type": "string" },
                    "source_identity_scope": { "type": "string" },
                    "db_link_contract": { "type": "string" },
                    "case_insensitive_collision_behavior": { "type": "string" },
                    "migration_contract": { "type": "string" }
                }
            },
            "policy": {
                "type": "object",
                "properties": {
                    "append_only": { "type": "boolean" },
                    "global_dedup_by_content_hash": { "type": "boolean" },
                    "never_overwrite_different_bytes": { "type": "boolean" },
                    "storage_scope": { "type": "string" },
                    "local_only_by_default": { "type": "boolean" },
                    "directory_mode_octal": { "type": "string" },
                    "file_mode_octal": { "type": "string" },
                    "enforce_private_files": { "type": "boolean" },
                    "atomic_publish": { "type": "string" },
                    "fsync_required": { "type": "boolean" },
                    "path_traversal_defense": { "type": "string" },
                    "symlink_defense": { "type": "string" },
                    "default_report_contract": { "type": "string" },
                    "sensitive_output_policy": {
                        "type": "object",
                        "properties": {
                            "default_includes_exact_paths": { "type": "boolean" },
                            "default_includes_raw_content": { "type": "boolean" },
                            "safe_metadata_fields": { "type": "array", "items": { "type": "string" } },
                            "redacted_by_default_fields": { "type": "array", "items": { "type": "string" } },
                            "opt_in_sensitive_modes": { "type": "array", "items": { "type": "string" } },
                            "redaction_marker_contract": { "type": "string" }
                        }
                    },
                    "compression_policy": {
                        "type": "object",
                        "properties": {
                            "default_state": { "type": "string" },
                            "optional": { "type": "boolean" },
                            "allowed_states": { "type": "array", "items": { "type": "string" } },
                            "required_metadata_fields": { "type": "array", "items": { "type": "string" } },
                            "raw_content_hash_scope": { "type": "string" }
                        }
                    },
                    "encryption_policy": {
                        "type": "object",
                        "properties": {
                            "default_state": { "type": "string" },
                            "optional": { "type": "boolean" },
                            "allowed_states": { "type": "array", "items": { "type": "string" } },
                            "required_metadata_fields": { "type": "array", "items": { "type": "string" } },
                            "key_material_reporting": { "type": "string" },
                            "integrity_contract": { "type": "string" }
                        }
                    },
                    "backup_policy": {
                        "type": "object",
                        "properties": {
                            "included_in_default_backup": { "type": "boolean" },
                            "backup_mode": { "type": "string" },
                            "backup_contents": { "type": "array", "items": { "type": "string" } },
                            "restore_precondition": { "type": "string" }
                        }
                    },
                    "support_bundle_policy": {
                        "type": "object",
                        "properties": {
                            "default_mode": { "type": "string" },
                            "include_manifest_metadata": { "type": "boolean" },
                            "include_blob_bytes": { "type": "boolean" },
                            "include_exact_paths": { "type": "boolean" },
                            "sensitive_attachment_gate": { "type": "string" }
                        }
                    },
                    "public_export_policy": {
                        "type": "object",
                        "properties": {
                            "pages_exports_include_raw_mirror": { "type": "boolean" },
                            "html_exports_include_raw_mirror": { "type": "boolean" },
                            "default_logs_include_raw_content": { "type": "boolean" },
                            "default_robot_json_includes_raw_content": { "type": "boolean" },
                            "public_artifact_contract": { "type": "string" }
                        }
                    },
                    "compression_contract": { "type": "string" },
                    "encryption_contract": { "type": "string" },
                    "support_bundle_redaction_contract": { "type": "string" },
                    "missing_upstream_semantics": { "type": "string" }
                }
            },
            "summary": {
                "type": "object",
                "properties": {
                    "manifest_count": { "type": "integer" },
                    "verified_blob_count": { "type": "integer" },
                    "missing_blob_count": { "type": "integer" },
                    "checksum_mismatch_count": { "type": "integer" },
                    "manifest_checksum_mismatch_count": { "type": "integer" },
                    "manifest_checksum_not_recorded_count": { "type": "integer" },
                    "invalid_manifest_count": { "type": "integer" },
                    "interrupted_capture_count": { "type": "integer" },
                    "duplicate_blob_reference_count": { "type": "integer" },
                    "total_blob_bytes": { "type": "integer" },
                    "orphan_blob_count": { "type": "integer", "minimum": 0 },
                    "orphan_blob_bytes": { "type": "integer", "minimum": 0 },
                    "amplified_source_count": { "type": "integer" },
                    "amplified_source_referenced_bytes": { "type": "integer" },
                    "amplified_source_excess_bytes": { "type": "integer" },
                    "max_source_amplification_ratio_milli": { "type": ["integer", "null"] },
                    "listed_manifest_count": { "type": "integer" },
                    "omitted_healthy_manifest_count": { "type": "integer" },
                    "listing_mode": { "type": "string", "enum": ["full", "bounded"] }
                }
            },
            "manifests": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "manifest_id": { "type": "string" },
                        "redacted_manifest_path": { "type": "string" },
                        "blob_relative_path": { "type": "string" },
                        "redacted_blob_path": { "type": "string" },
                        "blob_blake3": { "type": "string" },
                        "blob_size_bytes": { "type": "integer" },
                        "storage_kind": { "type": "string" },
                        "source_content_blake3": { "type": "string" },
                        "chunk_count": { "type": "integer" },
                        "provider": { "type": "string" },
                        "source_id": { "type": "string" },
                        "origin_kind": { "type": "string" },
                        "origin_host": { "type": ["string", "null"] },
                        "redacted_original_path": { "type": "string" },
                        "original_path_blake3": { "type": "string" },
                        "captured_at_ms": { "type": "integer" },
                        "source_mtime_ms": { "type": ["integer", "null"] },
                        "source_size_bytes": { "type": ["integer", "null"] },
                        "compression_state": { "type": "string" },
                        "encryption_state": { "type": "string" },
                        "compression": {
                            "type": "object",
                            "properties": {
                                "state": { "type": "string" },
                                "algorithm": { "type": ["string", "null"] },
                                "uncompressed_size_bytes": { "type": ["integer", "null"] }
                            }
                        },
                        "encryption": {
                            "type": "object",
                            "properties": {
                                "state": { "type": "string" },
                                "algorithm": { "type": ["string", "null"] },
                                "key_id": { "type": ["string", "null"] },
                                "envelope_version": { "type": ["integer", "null"] }
                            }
                        },
                        "db_link_count": { "type": "integer" },
                        "upstream_path_exists": { "type": ["boolean", "null"] },
                        "status": { "type": "string" },
                        "blob_checksum_status": { "type": "string" },
                        "manifest_checksum_status": { "type": "string" },
                        "invalid_reason": { "type": ["string", "null"] }
                    }
                }
            },
            "amplified_sources": {
                "type": "array",
                "description": "Source paths whose unique stored versions materially exceed the current source size. evidence_kind distinguishes full blob checksums from bounded manifest-checksum plus blob-metadata diagnostics; neither classification authorizes automatic pruning.",
                "items": {
                    "type": "object",
                    "properties": {
                        "evidence_kind": { "type": "string" },
                        "providers": { "type": "array", "items": { "type": "string" } },
                        "source_id": { "type": "string" },
                        "redacted_original_path": { "type": "string" },
                        "original_path_blake3": { "type": "string" },
                        "version_count": { "type": "integer" },
                        "referenced_blob_bytes": { "type": "integer" },
                        "current_source_size_bytes": { "type": "integer" },
                        "excess_bytes": { "type": "integer" },
                        "amplification_ratio_milli": { "type": ["integer", "null"] }
                    }
                }
            },
            "warnings": { "type": "array", "items": { "type": "string" } },
            "notes": { "type": "array", "items": { "type": "string" } }
        },
        "required": [
            "schema_version",
            "status",
            "redacted_root_path",
            "exists",
            "sensitive_paths_included",
            "raw_content_included",
            "layout",
            "policy",
            "summary",
            "manifests",
            "amplified_sources",
            "warnings",
            "notes"
        ]
    })
}

pub(crate) fn response_schema_doctor_source_authority() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "Fail-closed source-authority precedence report. Robots should use selected_authority, rejected_authorities, coverage_delta, freshness_delta, and checksum_evidence before any repair or reconstruction.",
        "properties": {
            "schema_version": { "type": "integer" },
            "decision": { "type": "string", "description": "read_only | candidate_only | promotable | refused" },
            "selected_authority": { "type": ["string", "null"] },
            "selected_authorities": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "authority": { "type": "string" },
                        "decision": { "type": "string" },
                        "reason": { "type": "string" },
                        "repairs": { "type": "array", "items": { "type": "string" } },
                        "coverage_delta": { "type": "integer" },
                        "freshness_delta_ms": { "type": ["integer", "null"] },
                        "checksum_status": { "type": "string" },
                        "evidence": { "type": "array", "items": { "type": "string" } }
                    }
                }
            },
            "rejected_authorities": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "authority": { "type": "string" },
                        "decision": { "type": "string" },
                        "reason": { "type": "string" },
                        "repairs": { "type": "array", "items": { "type": "string" } },
                        "coverage_delta": { "type": "integer" },
                        "freshness_delta_ms": { "type": ["integer", "null"] },
                        "checksum_status": { "type": "string" },
                        "evidence": { "type": "array", "items": { "type": "string" } }
                    }
                }
            },
            "coverage_delta": {
                "type": "object",
                "properties": {
                    "archive_conversation_count": { "type": "integer" },
                    "visible_local_source_conversation_count": { "type": "integer" },
                    "missing_current_source_count": { "type": "integer" },
                    "remote_source_count": { "type": "integer" },
                    "unknown_mapping_count": { "type": "integer" },
                    "raw_mirror_db_link_count": { "type": "integer" },
                    "visible_local_source_minus_archive": { "type": "integer" },
                    "raw_mirror_links_minus_archive": { "type": "integer" }
                }
            },
            "freshness_delta": {
                "type": "object",
                "properties": {
                    "archive_db_modified_at_ms": { "type": ["integer", "null"] },
                    "newest_raw_mirror_capture_at_ms": { "type": ["integer", "null"] },
                    "newest_raw_mirror_source_mtime_ms": { "type": ["integer", "null"] },
                    "raw_mirror_capture_minus_archive_db_ms": { "type": ["integer", "null"] },
                    "freshness_state": { "type": "string" }
                }
            },
            "checksum_evidence": {
                "type": "object",
                "properties": {
                    "raw_mirror_status": { "type": "string" },
                    "raw_mirror_manifest_count": { "type": "integer" },
                    "raw_mirror_verified_blob_count": { "type": "integer" },
                    "raw_mirror_missing_blob_count": { "type": "integer" },
                    "raw_mirror_checksum_mismatch_count": { "type": "integer" },
                    "raw_mirror_manifest_checksum_mismatch_count": { "type": "integer" },
                    "raw_mirror_manifest_checksum_not_recorded_count": { "type": "integer" },
                    "raw_mirror_invalid_manifest_count": { "type": "integer" },
                    "summary_status": { "type": "string" }
                }
            },
            "matrix": { "type": "array", "items": response_schema_opaque_object() },
            "notes": { "type": "array", "items": { "type": "string" } }
        },
        "required": [
            "schema_version",
            "decision",
            "selected_authority",
            "selected_authorities",
            "rejected_authorities",
            "coverage_delta",
            "freshness_delta",
            "checksum_evidence",
            "matrix",
            "notes"
        ]
    })
}
