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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DoctorBaselineCommandMode {
    Save,
    Update,
    Diff,
}

impl DoctorBaselineCommandMode {
    pub(crate) fn stable_name(self) -> &'static str {
        match self {
            Self::Save => "baseline-save",
            Self::Update => "baseline-update",
            Self::Diff => "baseline-diff",
        }
    }

    pub(crate) fn writes_baseline(self) -> bool {
        matches!(self, Self::Save | Self::Update)
    }
}

pub(crate) fn doctor_baseline_coverage_delta_value() -> serde_json::Value {
    serde_json::json!({
        "status": "unchanged",
        "archive_conversation_count": serde_json::Value::Null,
        "visible_source_conversation_count": serde_json::Value::Null,
        "raw_mirror_manifest_count": serde_json::Value::Null,
        "db_projection_only_count": serde_json::Value::Null,
        "missing_current_source_count": serde_json::Value::Null,
        "conversation_delta": serde_json::Value::Null,
        "message_delta": serde_json::Value::Null,
        "semantic_vector_delta": serde_json::Value::Null,
        "derived_asset_delta": serde_json::Value::Null,
    })
}

pub(crate) fn doctor_baseline_operation_outcome(
    kind: DoctorOperationOutcomeKind,
    reason: impl Into<String>,
    action_taken: impl Into<String>,
    action_not_taken: impl Into<String>,
    data_loss_risk: DoctorDataLossRisk,
    next_command: Option<String>,
    artifact_manifest_path: Option<String>,
) -> serde_json::Value {
    serde_json::to_value(doctor_operation_outcome_with_details(
        kind,
        reason.into(),
        action_taken.into(),
        action_not_taken.into(),
        data_loss_risk,
        next_command,
        artifact_manifest_path,
    ))
    .unwrap_or(serde_json::Value::Null)
}

pub(crate) fn run_doctor_baseline_impl(
    data_dir_override: Option<PathBuf>,
    db_override: Option<PathBuf>,
    output_format: Option<RobotFormat>,
    mode: DoctorBaselineCommandMode,
    baseline_id: Option<String>,
    baseline_path: Option<PathBuf>,
    wrap: WrapConfig,
) -> CliResult<()> {
    let started = Instant::now();
    let data_dir = resolve_data_dir(&data_dir_override, db_override.as_ref());
    let db_path = db_override.unwrap_or_else(|| data_dir.join("agent_search.db"));
    let baseline_id = doctor_baseline_id_or_default(baseline_id.as_deref(), baseline_path.as_ref());
    let baseline_path = doctor_baseline_resolve_path(
        &data_dir,
        &baseline_id,
        baseline_path.as_ref(),
        mode.writes_baseline(),
    )?;
    let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
        if matches!(fmt, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            fmt
        }
    });

    match mode {
        DoctorBaselineCommandMode::Save | DoctorBaselineCommandMode::Update => {
            let snapshot =
                build_doctor_baseline_snapshot(&data_dir, &db_path, &baseline_id, started);
            let baseline_document =
                build_doctor_baseline_document(&data_dir, &baseline_path, &baseline_id, snapshot);
            let write_result = write_doctor_baseline_document(
                &data_dir,
                &baseline_path,
                &baseline_document,
                mode == DoctorBaselineCommandMode::Update,
            );
            let (status, baseline_checksum, write_error) = match write_result {
                Ok(checksum) => (
                    if mode == DoctorBaselineCommandMode::Update {
                        "updated"
                    } else {
                        "saved"
                    },
                    serde_json::Value::String(checksum),
                    serde_json::Value::Null,
                ),
                Err(err) => {
                    if structured_format.is_none() {
                        return Err(CliError {
                            code: 4,
                            kind: "output-not-writable",
                            message: format!("failed to write doctor baseline: {err}"),
                            hint: Some(
                                "Use `cass doctor baseline update <id> --json` to explicitly replace an existing diagnostic baseline."
                                    .to_string(),
                            ),
                            retryable: false,
                        });
                    }
                    (
                        "failed",
                        serde_json::Value::Null,
                        serde_json::Value::String(err),
                    )
                }
            };
            let mode_name = mode.stable_name();
            let status_text = status;
            let saved_or_updated = status_text == "saved" || status_text == "updated";
            let write_failed = status_text == "failed";
            let blocked_reasons = if write_failed {
                vec!["baseline-write-failed"]
            } else {
                Vec::new()
            };
            let artifact_manifest_path = if saved_or_updated {
                serde_json::Value::String(baseline_path.display().to_string())
            } else {
                serde_json::Value::Null
            };
            let recommended_action = if write_failed {
                "Use `cass doctor baseline update <id> --json` only after deciding the previous diagnostic snapshot should be replaced."
            } else {
                "Use `cass doctor baseline diff <id> --json` to compare a future doctor state against this diagnostic snapshot."
            };
            let payload = serde_json::json!({
                "schema_version": 2,
                "surface": "baseline",
                "mode": mode_name,
                "status": status_text,
                "outcome_kind": if saved_or_updated { "applied" } else { "failed" },
                "risk_level": "low",
                "asset_class": "operation_receipt",
                "fallback_mode": "not-applicable",
                "authority_status": if saved_or_updated { "selected" } else { "refused" },
                "coverage_delta": doctor_baseline_coverage_delta_value(),
                "blocked_reasons": blocked_reasons,
                "plan_fingerprint": serde_json::Value::Null,
                "receipt_path": serde_json::Value::Null,
                "event_log_path": serde_json::Value::Null,
                "artifact_manifest_path": artifact_manifest_path,
                "recommended_action": recommended_action,
                "redaction_status": "redacted",
                "contract_provenance": "runtime",
                "operation_outcome": if saved_or_updated {
                    doctor_baseline_operation_outcome(
                        DoctorOperationOutcomeKind::Fixed,
                        format!("doctor baseline {mode_name} completed"),
                        "redacted diagnostic baseline document was written",
                        "no archive DB, SQLite sidecar, provider source log, raw mirror, backup, or derived index was modified",
                        DoctorDataLossRisk::None,
                        Some("cass doctor baseline diff <id> --json".to_string()),
                        Some(baseline_path.display().to_string()),
                    )
                } else {
                    doctor_baseline_operation_outcome(
                        DoctorOperationOutcomeKind::RepairIncomplete,
                        "doctor baseline write failed",
                        "write failure was reported without modifying archive evidence",
                        "no baseline was saved; no archive repair or cleanup was attempted",
                        DoctorDataLossRisk::Low,
                        Some("cass doctor baseline update <id> --json".to_string()),
                        None,
                    )
                },
                "baseline_id": baseline_id,
                "baseline_path": baseline_path.display().to_string(),
                "redacted_baseline_path": doctor_redacted_path(&baseline_path.display().to_string(), &data_dir),
                "baseline_checksum": baseline_checksum,
                "baseline_mutated": saved_or_updated,
                "baseline_document": baseline_document,
                "write_error": write_error,
                "diagnostic_only": true,
                "backup_semantics": "not-a-backup",
                "doctor_command": {
                    "surface": "baseline",
                    "execution_mode": mode_name,
                    "read_only": false,
                    "mutation_allowed": true,
                    "archive_mutation_allowed": false,
                },
                "_meta": {
                    "elapsed_ms": started.elapsed().as_millis() as u64,
                    "data_dir": data_dir.display().to_string(),
                    "db_path": db_path.display().to_string(),
                }
            });
            if let Some(fmt) = structured_format {
                output_structured_value(payload, fmt)?;
            } else {
                print_doctor_human_line(
                    wrap,
                    format!("Doctor baseline {status_text}: {}", baseline_path.display()),
                );
                println!("This is a redacted diagnostic snapshot, not a backup or restore point.");
            }
            if write_failed {
                return Err(CliError {
                    code: 4,
                    kind: "output-not-writable",
                    message: "doctor baseline write failed".to_string(),
                    hint: Some(
                        "Use `cass doctor baseline update <id> --json` to explicitly replace an existing diagnostic baseline."
                            .to_string(),
                    ),
                    retryable: false,
                });
            }
        }
        DoctorBaselineCommandMode::Diff => {
            let baseline_document = read_doctor_baseline_document(&baseline_path)?;
            let current_snapshot =
                build_doctor_baseline_snapshot(&data_dir, &db_path, &baseline_id, started);
            let diff = build_doctor_baseline_diff(
                &data_dir,
                &baseline_path,
                &baseline_id,
                &baseline_document,
                &current_snapshot,
                started,
            );
            if let Some(fmt) = structured_format {
                output_structured_value(diff, fmt)?;
            } else {
                let added = diff["added_anomalies"].as_array().map_or(0, Vec::len);
                let removed = diff["removed_anomalies"].as_array().map_or(0, Vec::len);
                let changed = diff["changed_assets"].as_array().map_or(0, Vec::len);
                print_doctor_human_line(
                    wrap,
                    format!(
                        "Doctor baseline diff: {added} added, {removed} removed, {changed} changed"
                    ),
                );
                if let Some(action) = diff["recommended_action"].as_str() {
                    println!("Next safe action: {action}");
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn doctor_baseline_id_or_default(
    requested: Option<&str>,
    baseline_path: Option<&PathBuf>,
) -> String {
    if let Some(requested) = requested
        && !requested.trim().is_empty()
    {
        return doctor_safe_baseline_id(requested);
    }
    if let Some(path) = baseline_path
        && let Some(stem) = path.file_stem().and_then(|stem| stem.to_str())
        && !stem.trim().is_empty()
    {
        return doctor_safe_baseline_id(stem);
    }
    "default".to_string()
}

pub(crate) fn doctor_safe_baseline_id(raw: &str) -> String {
    let mut safe = String::with_capacity(raw.len());
    for ch in raw.trim().chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
            safe.push(ch);
        } else {
            safe.push('-');
        }
    }
    let safe = safe.trim_matches('-').to_string();
    if safe.is_empty() {
        doctor_canonical_blake3(
            "doctor-baseline-id-v1",
            serde_json::json!({ "raw_id": raw }),
        )
    } else {
        safe
    }
}

pub(crate) fn doctor_baseline_resolve_path(
    data_dir: &Path,
    baseline_id: &str,
    requested_path: Option<&PathBuf>,
    writes_baseline: bool,
) -> CliResult<PathBuf> {
    let path = if let Some(path) = requested_path {
        if path.is_absolute() {
            path.clone()
        } else {
            data_dir.join("doctor").join("baselines").join(path)
        }
    } else {
        data_dir
            .join("doctor")
            .join("baselines")
            .join(format!("{baseline_id}.json"))
    };
    // A relative request must be plain names: on Windows a drive-relative
    // (`C:name`) or rooted (`\name`) path is not absolute, and `join` would
    // let it replace the baselines directory. The resolved path itself may
    // carry a drive prefix: every absolute Windows path does, the data dir's
    // included, so refusing one refused every baseline on Windows
    // (coding_agent_session_search-if17v).
    let relative_request_escapes = requested_path.is_some_and(|requested| {
        !requested.is_absolute()
            && requested.components().any(|component| {
                !matches!(
                    component,
                    std::path::Component::Normal(_) | std::path::Component::CurDir
                )
            })
    });
    if relative_request_escapes
        || path
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err(CliError {
            code: 2,
            kind: "usage",
            message: format!(
                "doctor baseline path is not portable-safe: {}",
                path.display()
            ),
            hint: Some("Use a simple baseline id or an absolute JSON file path.".to_string()),
            retryable: false,
        });
    }
    if writes_baseline && !path.starts_with(data_dir) {
        return Err(CliError {
            code: 2,
            kind: "usage",
            message: format!(
                "doctor baseline save/update may only write under the cass data dir: {}",
                path.display()
            ),
            hint: Some(
                "Use a baseline id, or place --baseline-path under [cass-data]/doctor/baselines/."
                    .to_string(),
            ),
            retryable: false,
        });
    }
    if writes_baseline {
        doctor_baseline_write_path_is_safe(data_dir, &path).map_err(|message| CliError {
            code: 2,
            kind: "usage",
            message,
            hint: Some(
                "Use a normal cass data directory and avoid symlinked baseline parents."
                    .to_string(),
            ),
            retryable: false,
        })?;
    }
    Ok(path)
}

#[cfg(test)]
mod doctor_baseline_path_tests {
    use super::doctor_baseline_resolve_path;
    use std::path::{Path, PathBuf};

    fn refused(data_dir: &Path, requested: &str) -> bool {
        doctor_baseline_resolve_path(data_dir, "id", Some(&PathBuf::from(requested)), false)
            .is_err_and(|error| error.kind == "usage")
    }

    #[test]
    fn baseline_ids_and_plain_relative_paths_resolve_under_the_baselines_dir() {
        let data_dir = std::env::temp_dir().join("cass-baseline-paths");
        let baselines = data_dir.join("doctor").join("baselines");
        assert_eq!(
            doctor_baseline_resolve_path(&data_dir, "known-good", None, false).unwrap(),
            baselines.join("known-good.json")
        );
        assert_eq!(
            doctor_baseline_resolve_path(
                &data_dir,
                "id",
                Some(&PathBuf::from("nested/known-good.json")),
                false
            )
            .unwrap(),
            baselines.join("nested").join("known-good.json")
        );
        assert!(refused(&data_dir, "../escape.json"));
        assert!(refused(&data_dir, "nested/../../escape.json"));
    }

    /// Every absolute Windows path has a drive prefix, the data dir's
    /// included, so refusing any prefix refused every baseline on Windows
    /// (coding_agent_session_search-if17v).
    #[cfg(windows)]
    #[test]
    fn windows_baselines_keep_the_drive_and_refuse_drive_relative_requests() {
        let data_dir = PathBuf::from(r"C:\Users\x\cass-data");
        assert_eq!(
            doctor_baseline_resolve_path(&data_dir, "known-good", None, false).unwrap(),
            PathBuf::from(r"C:\Users\x\cass-data\doctor\baselines\known-good.json")
        );
        assert_eq!(
            doctor_baseline_resolve_path(
                &data_dir,
                "id",
                Some(&PathBuf::from(r"D:\exports\known-good.json")),
                false
            )
            .unwrap(),
            PathBuf::from(r"D:\exports\known-good.json")
        );
        // Not absolute on Windows, and `join` would let each replace the
        // baselines directory.
        assert!(refused(&data_dir, "D:escape.json"));
        assert!(refused(&data_dir, r"\escape.json"));
        assert!(refused(&data_dir, r"D:\exports\..\escape.json"));
    }
}

pub(crate) fn doctor_baseline_write_path_is_safe(
    data_dir: &Path,
    baseline_path: &Path,
) -> Result<(), String> {
    if let Ok(metadata) = std::fs::symlink_metadata(data_dir)
        && (!metadata.is_dir() || metadata.file_type().is_symlink())
    {
        return Err(format!(
            "doctor baseline save/update refuses unsafe data directory root: {}",
            data_dir.display()
        ));
    }
    let parent = baseline_path
        .parent()
        .ok_or_else(|| format!("baseline path has no parent: {}", baseline_path.display()))?;
    if existing_path_has_symlink_below_root(parent, data_dir) {
        return Err(format!(
            "doctor baseline save/update refuses symlinked baseline parent: {}",
            parent.display()
        ));
    }
    Ok(())
}

pub(crate) fn build_doctor_baseline_snapshot(
    data_dir: &Path,
    db_path: &Path,
    baseline_id: &str,
    started: Instant,
) -> serde_json::Value {
    let index_path = crate::search::tantivy::expected_index_dir(data_dir);
    let sources_path = doctor_sources_config_path(data_dir);
    let config_path = data_dir.join("config.toml");
    let source_inventory = collect_doctor_source_inventory(data_dir, db_path);
    let raw_mirror = collect_doctor_raw_mirror_report(data_dir);
    let raw_mirror_backfill =
        collect_doctor_raw_mirror_backfill_report(data_dir, db_path, &raw_mirror, false);
    let sole_copy_warnings = build_doctor_sole_copy_warnings(&raw_mirror_backfill);
    let coverage_summary = build_doctor_coverage_summary(
        &source_inventory,
        &raw_mirror,
        &raw_mirror_backfill,
        &sole_copy_warnings,
    );
    let coverage_risk = doctor_coverage_risk_summary(
        &coverage_summary,
        sole_copy_warnings.len(),
        !crate::raw_mirror::capture_enabled(),
    );
    let source_authority =
        build_doctor_source_authority_report(db_path, &source_inventory, &raw_mirror);
    let storage_pressure = collect_doctor_storage_pressure(data_dir, db_path);
    let config_exclusion_risks =
        collect_doctor_config_exclusion_risks(data_dir, db_path, &config_path, &sources_path);
    let candidate_staging = collect_doctor_candidate_staging_report(data_dir, db_path, &index_path);
    let quarantine = collect_diag_quarantine_report_without_cleanup_plan(data_dir, &index_path);
    let archive_generation = serde_json::json!({
        "db_exists": db_path.exists(),
        "db_size_bytes": if db_path.exists() { fs_dir_size(db_path) } else { 0 },
        "db_blake3": db_path.exists().then(|| file_blake3_hex(db_path).ok()).flatten(),
        "db_path_blake3": doctor_canonical_blake3(
            "doctor-baseline-db-path-v1",
            serde_json::json!({ "db_path": doctor_path_identity_for_fingerprint(db_path) }),
        ),
        "wal_exists": doctor_sqlite_sidecar_path(db_path, "-wal").is_some_and(|path| path.exists()),
        "shm_exists": doctor_sqlite_sidecar_path(db_path, "-shm").is_some_and(|path| path.exists()),
    });
    let derived_generation = serde_json::json!({
        "lexical_index_exists": crate::search::tantivy::searchable_index_exists(&index_path),
        "lexical_index_path_blake3": doctor_canonical_blake3(
            "doctor-baseline-index-path-v1",
            serde_json::json!({ "index_path": doctor_path_identity_for_fingerprint(&index_path) }),
        ),
        "semantic_metadata_exists": data_dir.join("index").join("semantic").join("metadata.json").exists(),
        "memoization_metadata_exists": data_dir.join("semantic-cache").exists(),
    });
    let backup_generation = doctor_baseline_backup_generation(data_dir);
    let redacted_report = serde_json::json!({
        "baseline_id": baseline_id,
        "captured_at_ms": doctor_now_ms(),
        "cass_version": env!("CARGO_PKG_VERSION"),
        "data_dir_identity": {
            "data_dir_blake3": doctor_canonical_blake3(
                "doctor-baseline-data-dir-v1",
                serde_json::json!({ "data_dir": doctor_path_identity_for_fingerprint(data_dir) }),
            ),
            "redacted_data_dir": doctor_redacted_path(&data_dir.display().to_string(), data_dir),
            "redacted_db_path": doctor_redacted_path(&db_path.display().to_string(), data_dir),
        },
        "archive_generation": archive_generation,
        "derived_generation": derived_generation,
        "backup_generation": backup_generation,
        "source_inventory": source_inventory,
        "raw_mirror": raw_mirror,
        "raw_mirror_backfill_summary": {
            "status": raw_mirror_backfill.status,
            "total_candidate_count": raw_mirror_backfill.total_candidate_count,
            "eligible_live_source_count": raw_mirror_backfill.eligible_live_source_count,
            "source_missing_count": raw_mirror_backfill.source_missing_count,
            "db_projection_only_count": raw_mirror_backfill.db_projection_only_count,
        },
        "coverage_summary": coverage_summary,
        "coverage_risk": coverage_risk,
        "source_authority": source_authority,
        "storage_pressure": storage_pressure,
        "config_exclusion_risks": config_exclusion_risks,
        "candidate_staging_summary": {
            "status": candidate_staging.status,
            "total_candidate_count": candidate_staging.total_candidate_count,
            "interrupted_candidate_count": candidate_staging.interrupted_candidate_count,
            "orphaned_candidate_count": candidate_staging.orphaned_candidate_count,
        },
        "quarantine_summary": quarantine,
        "anomaly_classes": doctor_baseline_anomaly_classes_from_parts(
            db_path.exists(),
            &serde_json::to_value(&coverage_risk).unwrap_or(serde_json::Value::Null),
            &serde_json::to_value(&raw_mirror).unwrap_or(serde_json::Value::Null),
            &serde_json::to_value(&storage_pressure).unwrap_or(serde_json::Value::Null),
            &serde_json::to_value(&config_exclusion_risks).unwrap_or(serde_json::Value::Null),
        ),
        "recommendations": doctor_baseline_recommendations_from_parts(
            &serde_json::to_value(&coverage_risk).unwrap_or(serde_json::Value::Null),
            &serde_json::to_value(&storage_pressure).unwrap_or(serde_json::Value::Null),
        ),
        "performance": {
            "elapsed_ms": started.elapsed().as_millis() as u64,
        },
        "redaction_status": "redacted",
    });
    let redacted_report = doctor_baseline_redact_paths(redacted_report, data_dir);
    let checksum = doctor_canonical_blake3(
        "doctor-baseline-redacted-report-v1",
        redacted_report.clone(),
    );
    serde_json::json!({
        "redacted_report": redacted_report,
        "redacted_report_checksum": checksum,
    })
}

pub(crate) fn build_doctor_baseline_document(
    data_dir: &Path,
    baseline_path: &Path,
    baseline_id: &str,
    snapshot: serde_json::Value,
) -> serde_json::Value {
    serde_json::json!({
        "schema_version": 1,
        "baseline_kind": "cass_doctor_diagnostic_baseline_v1",
        "baseline_id": baseline_id,
        "redacted_baseline_path": doctor_redacted_path(&baseline_path.display().to_string(), data_dir),
        "created_at_ms": doctor_now_ms(),
        "diagnostic_only": true,
        "backup_semantics": "not-a-backup",
        "redaction_status": "redacted",
        "redacted_report_checksum": snapshot["redacted_report_checksum"].clone(),
        "redacted_report": snapshot["redacted_report"].clone(),
    })
}

pub(crate) fn write_doctor_baseline_document(
    data_dir: &Path,
    baseline_path: &Path,
    baseline_document: &serde_json::Value,
    replace_existing: bool,
) -> Result<String, String> {
    let parent = baseline_path
        .parent()
        .ok_or_else(|| format!("baseline path has no parent: {}", baseline_path.display()))?;
    doctor_baseline_write_path_is_safe(data_dir, baseline_path)?;
    doctor_forensic_create_private_dir_all(parent).map_err(|err| {
        format!(
            "failed to create doctor baseline parent {}: {err}",
            parent.display()
        )
    })?;
    doctor_baseline_write_path_is_safe(data_dir, baseline_path)?;
    if !replace_existing {
        return doctor_write_private_json_artifact(
            baseline_path,
            baseline_document,
            "doctor diagnostic baseline",
        );
    }
    let file_name = baseline_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("baseline.json");
    let tmp_path = parent.join(format!(".{file_name}.{}.tmp", doctor_now_ms()));
    doctor_write_private_json_artifact(
        &tmp_path,
        baseline_document,
        "doctor diagnostic baseline update temp",
    )?;
    doctor_rename_file(&tmp_path, baseline_path).map_err(|err| {
        format!(
            "failed to replace doctor baseline {} with {}: {err}",
            baseline_path.display(),
            tmp_path.display()
        )
    })?;
    sync_directory(parent)?;
    file_blake3_hex(baseline_path)
}

pub(crate) fn read_doctor_baseline_document(path: &Path) -> CliResult<serde_json::Value> {
    let bytes = std::fs::read(path).map_err(|err| CliError {
        code: 13,
        kind: "not-found",
        message: format!("failed to read doctor baseline {}: {err}", path.display()),
        hint: Some("Run `cass doctor baseline save <id> --json` first.".to_string()),
        retryable: false,
    })?;
    let document: serde_json::Value = serde_json::from_slice(&bytes).map_err(|err| CliError {
        code: 10,
        kind: "config",
        message: format!(
            "doctor baseline is not valid JSON: {} ({err})",
            path.display()
        ),
        hint: Some(
            "Recreate the diagnostic baseline with `cass doctor baseline save <id> --json`."
                .to_string(),
        ),
        retryable: false,
    })?;
    let schema_error = |message: String| CliError {
        code: 10,
        kind: "config",
        message,
        hint: Some(
            "Recreate the diagnostic baseline with `cass doctor baseline save <id> --json`."
                .to_string(),
        ),
        retryable: false,
    };
    if document
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
        != Some(1)
    {
        return Err(schema_error(format!(
            "doctor baseline has an unsupported schema_version: {}",
            path.display()
        )));
    }
    if document
        .get("baseline_kind")
        .and_then(serde_json::Value::as_str)
        != Some("cass_doctor_diagnostic_baseline_v1")
    {
        return Err(schema_error(format!(
            "doctor baseline has an unsupported baseline_kind: {}",
            path.display()
        )));
    }
    if document
        .get("diagnostic_only")
        .and_then(serde_json::Value::as_bool)
        != Some(true)
        || document
            .get("backup_semantics")
            .and_then(serde_json::Value::as_str)
            != Some("not-a-backup")
    {
        return Err(schema_error(format!(
            "doctor baseline is not marked as a diagnostic-only non-backup: {}",
            path.display()
        )));
    }
    if document
        .get("redaction_status")
        .and_then(serde_json::Value::as_str)
        != Some("redacted")
    {
        return Err(schema_error(format!(
            "doctor baseline is not redacted: {}",
            path.display()
        )));
    }
    let redacted_report = document.get("redacted_report").ok_or_else(|| {
        schema_error(format!(
            "doctor baseline is missing redacted_report: {}",
            path.display()
        ))
    })?;
    if !redacted_report.is_object() {
        return Err(schema_error(format!(
            "doctor baseline redacted_report is not an object: {}",
            path.display()
        )));
    }
    let expected_checksum = document
        .get("redacted_report_checksum")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            schema_error(format!(
                "doctor baseline is missing redacted_report_checksum: {}",
                path.display()
            ))
        })?;
    let actual_checksum = doctor_canonical_blake3(
        "doctor-baseline-redacted-report-v1",
        redacted_report.clone(),
    );
    if expected_checksum != actual_checksum {
        return Err(schema_error(format!(
            "doctor baseline redacted_report_checksum mismatch: {}",
            path.display()
        )));
    }
    Ok(document)
}

pub(crate) fn build_doctor_baseline_diff(
    data_dir: &Path,
    baseline_path: &Path,
    baseline_id: &str,
    baseline_document: &serde_json::Value,
    current_snapshot: &serde_json::Value,
    started: Instant,
) -> serde_json::Value {
    let baseline_report = baseline_document
        .get("redacted_report")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let current_report = current_snapshot
        .get("redacted_report")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let baseline_anomalies = doctor_string_set_at(&baseline_report, "/anomaly_classes");
    let current_anomalies = doctor_string_set_at(&current_report, "/anomaly_classes");
    let added_anomalies = doctor_sorted_set_difference(&current_anomalies, &baseline_anomalies);
    let removed_anomalies = doctor_sorted_set_difference(&baseline_anomalies, &current_anomalies);
    let changed_assets = doctor_baseline_changed_assets(&baseline_report, &current_report);
    let changed_recommendations =
        doctor_baseline_changed_recommendations(&baseline_report, &current_report);
    let perf_delta = doctor_baseline_perf_delta(&baseline_report, &current_report);
    let status = if added_anomalies.is_empty()
        && removed_anomalies.is_empty()
        && changed_assets.is_empty()
        && changed_recommendations.is_empty()
    {
        "unchanged"
    } else {
        "changed"
    };
    let recommended_action = if added_anomalies.iter().any(|item| item.contains("archive"))
        || added_anomalies.iter().any(|item| item.contains("source"))
    {
        "Run `cass doctor check --json` and inspect the source-authority and repair plan before any mutation."
    } else if added_anomalies
        .iter()
        .any(|item| item.contains("derived") || item.contains("storage"))
    {
        "Run `cass doctor check --json`; derived-only or storage changes should stay separate from archive repair."
    } else {
        "No archive repair action is implied by this diagnostic baseline diff."
    };
    serde_json::json!({
        "schema_version": 2,
        "surface": "baseline-diff",
        "mode": "baseline-diff",
        "status": status,
        "outcome_kind": "no_op",
        "risk_level": "low",
        "asset_class": "operation_receipt",
        "fallback_mode": "not-applicable",
        "authority_status": "read_only",
        "coverage_delta": doctor_baseline_coverage_delta_value(),
        "blocked_reasons": [],
        "plan_fingerprint": serde_json::Value::Null,
        "receipt_path": serde_json::Value::Null,
        "event_log_path": serde_json::Value::Null,
        "artifact_manifest_path": baseline_path.display().to_string(),
        "recommended_action": recommended_action,
        "redaction_status": "redacted",
        "contract_provenance": "runtime",
        "event_log": {
            "schema_version": 1,
            "event_log_id": doctor_canonical_blake3(
                "doctor-baseline-diff-event-log-v1",
                serde_json::json!({
                    "baseline_id": baseline_id,
                    "baseline_path": doctor_path_identity_for_fingerprint(baseline_path),
                    "status": status,
                }),
            ),
            "path": serde_json::Value::Null,
            "redacted_path": serde_json::Value::Null,
            "events": [
                {
                    "event": "doctor_baseline_diff",
                    "status": status,
                    "baseline_id": baseline_id,
                    "baseline_mutated": false,
                    "changed_asset_count": changed_assets.len(),
                    "added_anomaly_count": added_anomalies.len(),
                    "removed_anomaly_count": removed_anomalies.len(),
                }
            ],
        },
        "operation_outcome": doctor_baseline_operation_outcome(
            DoctorOperationOutcomeKind::BaselineDiffOnly,
            "baseline diff is diagnostic-only",
            "baseline comparison was produced",
            "no archive DB, SQLite sidecar, provider source log, raw mirror, backup, or derived index was modified",
            DoctorDataLossRisk::None,
            None,
            Some(baseline_path.display().to_string()),
        ),
        "baseline_diff": {
            "schema_version": 1,
            "diff_kind": "cass_doctor_diagnostic_baseline_diff_v1",
            "baseline_id": baseline_id,
            "baseline_report_checksum": baseline_document.get("redacted_report_checksum").cloned().unwrap_or(serde_json::Value::Null),
            "current_report_checksum": current_snapshot.get("redacted_report_checksum").cloned().unwrap_or(serde_json::Value::Null),
            "diagnostic_only": true,
        },
        "baseline_path": baseline_path.display().to_string(),
        "redacted_baseline_path": doctor_redacted_path(&baseline_path.display().to_string(), data_dir),
        "baseline_id": baseline_id,
        "compared_at": doctor_now_ms(),
        "added_anomalies": added_anomalies,
        "removed_anomalies": removed_anomalies,
        "changed_assets": changed_assets,
        "changed_recommendations": changed_recommendations,
        "perf_delta": perf_delta,
        "privacy_redaction_status": {
            "baseline": baseline_report.get("redaction_status").cloned().unwrap_or(serde_json::Value::Null),
            "current": current_report.get("redaction_status").cloned().unwrap_or(serde_json::Value::Null),
            "status": if baseline_report.get("redaction_status") == current_report.get("redaction_status") { "unchanged" } else { "changed" },
        },
        "baseline_mutated": false,
        "doctor_command": {
            "surface": "baseline-diff",
            "execution_mode": "baseline-diff",
            "read_only": true,
            "mutation_allowed": false,
            "archive_mutation_allowed": false,
        },
        "_meta": {
            "elapsed_ms": started.elapsed().as_millis() as u64,
            "data_dir": data_dir.display().to_string(),
        }
    })
}

pub(crate) fn doctor_baseline_backup_generation(data_dir: &Path) -> serde_json::Value {
    let backup_root = doctor_candidate_promotion_root(data_dir);
    let mut backup_count = 0usize;
    let mut latest_modified_ms = None;
    if let Ok(entries) = std::fs::read_dir(&backup_root) {
        for entry in entries.flatten() {
            if entry.file_type().map(|ty| ty.is_dir()).unwrap_or(false) {
                backup_count += 1;
                if let Ok(metadata) = entry.metadata()
                    && let Ok(modified) = metadata.modified()
                    && let Some(modified_ms) = system_time_to_unix_ms(modified)
                {
                    latest_modified_ms = Some(latest_modified_ms.unwrap_or(0).max(modified_ms));
                }
            }
        }
    }
    serde_json::json!({
        "backup_root_exists": backup_root.exists(),
        "backup_count": backup_count,
        "latest_modified_ms": latest_modified_ms,
        "redacted_backup_root": doctor_redacted_path(&backup_root.display().to_string(), data_dir),
    })
}

pub(crate) fn doctor_baseline_anomaly_classes_from_parts(
    db_exists: bool,
    coverage_risk: &serde_json::Value,
    raw_mirror: &serde_json::Value,
    storage_pressure: &serde_json::Value,
    config_exclusion_risks: &serde_json::Value,
) -> Vec<String> {
    let mut classes = BTreeSet::new();
    if !db_exists {
        classes.insert("archive-db-missing".to_string());
    }
    if coverage_risk
        .get("missing_current_source_count")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0)
        > 0
    {
        classes.insert("source-coverage-missing-current-source".to_string());
    }
    if coverage_risk
        .get("sole_copy_warning_count")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0)
        > 0
    {
        classes.insert("source-coverage-sole-copy".to_string());
    }
    if coverage_risk
        .get("db_without_raw_mirror_count")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0)
        > 0
    {
        classes.insert("source-coverage-db-without-raw-mirror".to_string());
    }
    let raw_mirror_status = raw_mirror
        .get("status")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    if !matches!(raw_mirror_status, "absent" | "empty" | "verified") {
        classes.insert(format!("raw-mirror-{raw_mirror_status}"));
    }
    let storage_status = storage_pressure
        .get("status")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    if storage_status != "ok" {
        classes.insert(format!("storage-pressure-{storage_status}"));
    }
    if config_exclusion_risks.as_array().map_or(0, Vec::len) > 0 {
        classes.insert("config-exclusion-risk".to_string());
    }
    classes.into_iter().collect()
}

pub(crate) fn doctor_baseline_recommendations_from_parts(
    coverage_risk: &serde_json::Value,
    storage_pressure: &serde_json::Value,
) -> Vec<String> {
    let mut recommendations = BTreeSet::new();
    if let Some(action) = coverage_risk
        .get("recommended_action")
        .and_then(serde_json::Value::as_str)
        && !action.trim().is_empty()
    {
        recommendations.insert(action.to_string());
    }
    if let Some(action) = storage_pressure
        .get("recommended_action")
        .and_then(serde_json::Value::as_str)
        && !action.trim().is_empty()
    {
        recommendations.insert(action.to_string());
    }
    recommendations.into_iter().collect()
}

pub(crate) fn doctor_baseline_redact_paths(
    value: serde_json::Value,
    data_dir: &Path,
) -> serde_json::Value {
    match value {
        serde_json::Value::String(text) => {
            serde_json::Value::String(doctor_redacted_text(&text, data_dir))
        }
        serde_json::Value::Array(items) => serde_json::Value::Array(
            items
                .into_iter()
                .map(|item| doctor_baseline_redact_paths(item, data_dir))
                .collect(),
        ),
        serde_json::Value::Object(map) => {
            let mut redacted = serde_json::Map::new();
            for (key, value) in map {
                if key.starts_with("redacted_") || key.ends_with("_blake3") {
                    redacted.insert(key, doctor_baseline_redact_paths(value, data_dir));
                } else if doctor_baseline_key_looks_like_path(&key) {
                    let next = match value {
                        serde_json::Value::String(path) => {
                            serde_json::Value::String(doctor_redacted_path(&path, data_dir))
                        }
                        serde_json::Value::Array(items) => serde_json::Value::Array(
                            items
                                .into_iter()
                                .map(|item| {
                                    item.as_str()
                                        .map(|path| {
                                            serde_json::Value::String(doctor_redacted_path(
                                                path, data_dir,
                                            ))
                                        })
                                        .unwrap_or_else(|| {
                                            doctor_baseline_redact_paths(item, data_dir)
                                        })
                                })
                                .collect(),
                        ),
                        other => doctor_baseline_redact_paths(other, data_dir),
                    };
                    redacted.insert(key, next);
                } else {
                    redacted.insert(key, doctor_baseline_redact_paths(value, data_dir));
                }
            }
            serde_json::Value::Object(redacted)
        }
        other => other,
    }
}

pub(crate) fn doctor_baseline_key_looks_like_path(key: &str) -> bool {
    key == "path" || key == "paths" || key.ends_with("_path") || key.ends_with("_paths")
}

pub(crate) fn doctor_string_set_at(value: &serde_json::Value, pointer: &str) -> BTreeSet<String> {
    value
        .pointer(pointer)
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .map(str::to_string)
        .collect()
}

pub(crate) fn doctor_sorted_set_difference(
    left: &BTreeSet<String>,
    right: &BTreeSet<String>,
) -> Vec<String> {
    left.difference(right).cloned().collect()
}

pub(crate) fn doctor_baseline_changed_assets(
    baseline_report: &serde_json::Value,
    current_report: &serde_json::Value,
) -> Vec<serde_json::Value> {
    let watched = [
        (
            "data_dir_identity",
            "/data_dir_identity/data_dir_blake3",
            "cass data dir identity",
        ),
        (
            "archive_generation",
            "/archive_generation/db_blake3",
            "canonical archive DB checksum",
        ),
        (
            "archive_generation",
            "/archive_generation/db_size_bytes",
            "canonical archive DB size",
        ),
        (
            "source_coverage",
            "/coverage_summary/archive_conversation_count",
            "archive conversation count",
        ),
        (
            "source_coverage",
            "/coverage_summary/missing_current_source_count",
            "missing current source count",
        ),
        (
            "raw_mirror",
            "/raw_mirror/summary/manifest_count",
            "raw mirror manifest count",
        ),
        (
            "backup_generation",
            "/backup_generation/backup_count",
            "backup generation count",
        ),
        (
            "derived_generation",
            "/derived_generation/lexical_index_exists",
            "lexical derived index presence",
        ),
        (
            "derived_generation",
            "/derived_generation/semantic_metadata_exists",
            "semantic derived metadata presence",
        ),
        (
            "derived_generation",
            "/derived_generation/memoization_metadata_exists",
            "semantic memoization metadata presence",
        ),
        (
            "storage_pressure",
            "/storage_pressure/status",
            "storage pressure status",
        ),
        ("privacy_redaction", "/redaction_status", "redaction status"),
    ];
    watched
        .into_iter()
        .filter_map(|(asset_class, pointer, description)| {
            let before = baseline_report
                .pointer(pointer)
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            let after = current_report
                .pointer(pointer)
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            (before != after).then(|| {
                serde_json::json!({
                    "asset_class": asset_class,
                    "field": pointer.trim_start_matches('/').replace('/', "."),
                    "description": description,
                    "before": before,
                    "after": after,
                })
            })
        })
        .collect()
}

pub(crate) fn doctor_baseline_changed_recommendations(
    baseline_report: &serde_json::Value,
    current_report: &serde_json::Value,
) -> Vec<serde_json::Value> {
    let before = doctor_string_set_at(baseline_report, "/recommendations");
    let after = doctor_string_set_at(current_report, "/recommendations");
    doctor_sorted_set_difference(&after, &before)
        .into_iter()
        .map(|recommendation| {
            serde_json::json!({
                "change": "added",
                "recommendation": recommendation,
            })
        })
        .chain(
            doctor_sorted_set_difference(&before, &after)
                .into_iter()
                .map(|recommendation| {
                    serde_json::json!({
                        "change": "removed",
                        "recommendation": recommendation,
                    })
                }),
        )
        .collect()
}

pub(crate) fn doctor_baseline_perf_delta(
    baseline_report: &serde_json::Value,
    current_report: &serde_json::Value,
) -> serde_json::Value {
    let before = baseline_report
        .pointer("/performance/elapsed_ms")
        .and_then(serde_json::Value::as_u64);
    let after = current_report
        .pointer("/performance/elapsed_ms")
        .and_then(serde_json::Value::as_u64);
    serde_json::json!({
        "baseline_elapsed_ms": before,
        "current_elapsed_ms": after,
        "delta_ms": before.zip(after).map(|(before, after)| after as i64 - before as i64),
    })
}
