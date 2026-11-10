use crate::cli::args_sub::RobotFormat;
use crate::cli::commands::search::fieldbudgets::robot_format_from_env;
use crate::cli::commands::search::toon_encode_options_from_env::output_structured_value;
use crate::cli::error::{CliError, CliResult, WrapConfig};
use crate::cli::paths::{default_data_dir, resolve_data_dir};
use crate::doctor::archive::export::{
    DoctorArchiveExportCommandMode, DoctorArchiveExportRequest,
    doctor_archive_export_collect_assets, doctor_archive_export_copy_assets,
    doctor_archive_export_event_log, doctor_archive_export_event_log_path,
    doctor_archive_export_manifest_path, doctor_archive_export_plan_value,
    doctor_archive_export_receipt_path, doctor_archive_export_target_available_bytes,
    doctor_archive_export_validate_target, doctor_archive_export_verify_target,
};
use crate::doctor::candidate::promotion::doctor_write_private_json_artifact;
use crate::doctor::forensic::{
    doctor_forensic_create_private_dir_all, doctor_now_ms, doctor_redacted_path,
};
use crate::doctor::fs_safety::mutation_executor::sync_directory;
use crate::doctor::report::human::print_doctor_human_line;
use std::path::PathBuf;
use std::time::Instant;

/// World-class-doctor pass-2: `cass doctor --ls`. Read-only; lists per-run
/// artifact directories under `<data_dir>/doctor/runs/`. Output is JSON when
/// `structured_format` is set; otherwise a human-readable table.
pub(crate) fn run_doctor_ls(
    data_dir_override: Option<PathBuf>,
    structured_format: Option<RobotFormat>,
) -> CliResult<()> {
    let data_dir = data_dir_override.unwrap_or_else(default_data_dir);
    let runs = crate::doctor_runs::list_runs(&data_dir).map_err(|e| CliError {
        code: 14,
        kind: "io",
        message: format!("failed to enumerate doctor runs under {data_dir:?}: {e}"),
        hint: Some("Verify the data directory exists and is readable.".to_string()),
        retryable: true,
    })?;
    if structured_format.is_some() {
        let envelope = serde_json::json!({
            "schema_version": 2,
            "doctor_contract_version": 1,
            "runs": runs,
            "data_dir": data_dir.to_string_lossy(),
            "runs_dir": data_dir.join("doctor").join("runs").to_string_lossy(),
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&envelope).map_err(|e| CliError {
                code: 9,
                kind: "internal",
                message: format!("serialize ls envelope: {e}"),
                hint: None,
                retryable: false,
            })?
        );
    } else {
        // Human-readable table on stdout (data); progress on stderr.
        let header_run_id = "RUN_ID";
        let header_status = "STATUS";
        let header_exit = "EXIT";
        let header_actions = "ACTIONS";
        let header_mode = "MODE";
        println!(
            "{header_run_id:<40} {header_status:<13} {header_exit:<6} {header_actions:<11} {header_mode}"
        );
        for run in &runs {
            println!(
                "{:<40} {:<13} {:<6} {:<11} {}",
                run.run_id,
                run.status,
                run.exit_code
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                run.action_count,
                run.mode.clone().unwrap_or_else(|| "-".to_string()),
            );
        }
        if runs.is_empty() {
            eprintln!(
                "(no runs found under {})",
                data_dir.join("doctor").join("runs").display()
            );
        }
    }
    Ok(())
}

/// World-class-doctor pass-2: `cass doctor --undo <run-id>`. Walks
/// `<run-dir>/actions.jsonl` in reverse and restores byte-identically. The
/// undo itself produces a fresh run-id whose actions.jsonl describes what was
/// restored. Refuses with hash-mismatch error if anyone tampered with the
/// post-mutation files (so a partial undo never silently corrupts state).
pub(crate) fn run_doctor_undo(
    data_dir_override: Option<PathBuf>,
    run_id_arg: &str,
    structured_format: Option<RobotFormat>,
) -> CliResult<()> {
    let data_dir = data_dir_override.unwrap_or_else(default_data_dir);
    // Resolve `latest` alias.
    let resolved_run_id = if run_id_arg == "latest" {
        match crate::doctor_runs::find_latest_run(&data_dir).map_err(|e| CliError {
            code: 14,
            kind: "io",
            message: format!("failed to enumerate doctor runs: {e}"),
            hint: Some("Run `cass doctor --ls --json` to inspect available runs.".to_string()),
            retryable: true,
        })? {
            Some(latest) => {
                crate::doctor_runs::RunId::parse(&latest.run_id).ok_or_else(|| CliError {
                    code: 9,
                    kind: "internal",
                    message: format!("latest run id {:?} did not parse as a RunId", latest.run_id),
                    hint: None,
                    retryable: false,
                })?
            }
            None => {
                return Err(CliError {
                    code: 13,
                    kind: "not-found",
                    message: format!("no doctor runs found under {data_dir:?}"),
                    hint: Some(
                        "Run `cass doctor --ls --json` to inspect; no runs means there is nothing to undo."
                            .to_string(),
                    ),
                    retryable: false,
                });
            }
        }
    } else {
        crate::doctor_runs::RunId::parse(run_id_arg).ok_or_else(|| CliError {
            code: 2,
            kind: "usage",
            message: format!(
                "{run_id_arg:?} is not a valid run id (expected `<ISO8601>__<6-hex>` or `latest`)"
            ),
            hint: Some("Use `cass doctor --ls --json` to find a valid run id.".to_string()),
            retryable: false,
        })?
    };

    // Best-effort: identify the cass build sha for the new undo run id. If
    // unavailable (development build), use a sentinel that hashes deterministically.
    let target_sha = option_env!("VERGEN_GIT_SHA").unwrap_or("unknown");

    let receipt = crate::doctor_undo::undo_run(&data_dir, &resolved_run_id, target_sha).map_err(
        |e| match e {
            crate::doctor_undo::UndoError::RunNotFound(id, path) => CliError {
                code: 13,
                kind: "not-found",
                message: format!("run {id} not found at {path:?}"),
                hint: Some("Run `cass doctor --ls --json` to confirm the run id.".to_string()),
                retryable: false,
            },
            crate::doctor_undo::UndoError::AfterHashMismatch { path, expected, actual } => {
                CliError {
                    code: 4,
                    kind: "refused-unsafe",
                    message: format!(
                        "undo refused: post-mutation file {} has hash {} (expected {})",
                        path.display(),
                        actual,
                        expected
                    ),
                    hint: Some(
                        "Someone modified this file after the original `cass doctor --fix`; refusing to overwrite."
                            .to_string(),
                    ),
                    retryable: false,
                }
            }
            // Pass-11 fix: BackupHashMismatch was code 5 ("concurrency-lost"
            // per the documented contract) but actually means "the per-run
            // backup was tampered with". The robot-docs handbook explicitly
            // documents code 3 for "undo failed (hash mismatch on a backup)".
            // Use code 3 + kind "repair-failure" so agents branching on
            // exit_code_kind get a precise signal.
            crate::doctor_undo::UndoError::BackupHashMismatch { path, expected, actual } => CliError {
                code: 3,
                kind: "repair-failure",
                message: format!(
                    "undo backup hash mismatch on {}: expected {expected}, got {actual}",
                    path.display()
                ),
                hint: Some(
                    "The per-run backup itself was tampered with. Inspect the run dir manually; do NOT proceed."
                        .to_string(),
                ),
                retryable: false,
            },
            crate::doctor_undo::UndoError::BackupMissing(path) => CliError {
                code: 13,
                kind: "not-found",
                message: format!("undo backup missing at {}", path.display()),
                hint: Some("The backup file was deleted out-of-band; this run cannot be undone.".to_string()),
                retryable: false,
            },
            crate::doctor_undo::UndoError::ActionsParseError(detail) => CliError {
                code: 5,
                kind: "data-corruption",
                message: format!("actions.jsonl unparseable: {detail}"),
                hint: Some(
                    "Inspect the run dir; refusing to undo a partially-corrupt journal.".to_string(),
                ),
                retryable: false,
            },
            crate::doctor_undo::UndoError::QuarantineDestinationExists(path) => CliError {
                code: 4,
                kind: "refused-unsafe",
                message: format!(
                    "undo refused: quarantine destination already exists at {}",
                    path.display()
                ),
                hint: Some(
                    "The undo quarantine target would be overwritten. Inspect the undo quarantine directory before retrying."
                        .to_string(),
                ),
                retryable: false,
            },
            crate::doctor_undo::UndoError::Io(io) => CliError {
                code: 14,
                kind: "io",
                message: format!("io error during undo: {io}"),
                hint: None,
                retryable: true,
            },
            crate::doctor_undo::UndoError::TargetMissing(p) => CliError {
                code: 13,
                kind: "not-found",
                message: format!("undo target file missing: {}", p.display()),
                hint: None,
                retryable: false,
            },
            // Pass-12 fix: Op::Rename is unsafe to undo because the rename
            // destination isn't recorded in actions.jsonl. Maps to code 4
            // (refused-unsafe). Operator can still inspect via `--explain`
            // and restore manually if needed.
            crate::doctor_undo::UndoError::OpRenameNotReversible { path } => CliError {
                code: 4,
                kind: "refused-unsafe",
                message: format!(
                    "undo refused: Op::Rename on {} is not reversible in the pass-1+ schema (the rename destination is not recorded)",
                    path.display()
                ),
                hint: Some(
                    "Use `cass doctor --explain <run-id> --json` to inspect the rename, then restore manually if needed."
                        .to_string(),
                ),
                retryable: false,
            },
        },
    )?;

    if structured_format.is_some() {
        let envelope = serde_json::json!({
            "schema_version": 2,
            "doctor_contract_version": 1,
            "undo_receipt": receipt,
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&envelope).map_err(|e| CliError {
                code: 9,
                kind: "internal",
                message: format!("serialize undo envelope: {e}"),
                hint: None,
                retryable: false,
            })?
        );
    } else {
        println!(
            "Undo of run {} → new run {}",
            receipt.original_run_id, receipt.undo_run_id
        );
        println!(
            "  steps: total={} succeeded={} skipped={} failed={}",
            receipt.steps_total,
            receipt.steps_succeeded,
            receipt.steps_skipped,
            receipt.steps_failed
        );
    }
    Ok(())
}

/// World-class-doctor pass-3: `cass doctor --robot-triage`. Read-only
/// mega-command that returns ONE envelope with everything an agent needs to
/// make a decision in a single round-trip.
///
/// The envelope is composed from:
/// - `summary`: derived from the read-only doctor check.
/// - `findings`: a slice of the check report's findings (cap 10 for token budget).
/// - `actions_planned`: empty in pass-3 (the planner integration is a pass-4 task).
/// - `recommended_command`: derived from the doctor's
///   `operation_outcome.next_command`, or a fallback `cass doctor --json`.
/// - `capabilities_url`: stable string `cass capabilities --json`.
pub(crate) fn run_doctor_robot_triage(
    data_dir_override: Option<PathBuf>,
    structured_format: Option<RobotFormat>,
) -> CliResult<()> {
    use std::time::Instant;
    let data_dir = data_dir_override.unwrap_or_else(default_data_dir);
    let started = Instant::now();
    // We don't go through run_doctor_impl here — that's a heavyweight invocation.
    // Triage is meant to be fast (<200ms target). We compose a lightweight
    // health snapshot from existing helpers.
    let runs = crate::doctor_runs::list_runs(&data_dir).unwrap_or_default();
    let recent_run = runs.first().cloned();
    let elapsed_ms = started.elapsed().as_millis() as u64;

    // Pass-10: real findings. Surface anything an agent could act on without
    // running the heavyweight doctor pipeline:
    //   1. data_dir missing entirely → recoverable with `cass doctor --json`
    //   2. recent run incomplete → recoverable with `cass doctor --undo`
    //   3. in-flight band on most-recent run → crash-recovery candidate
    //   4. quarantine dir non-empty → operator inspection candidate
    let mut findings: Vec<serde_json::Value> = Vec::new();

    if !data_dir.exists() {
        findings.push(serde_json::json!({
            "severity": "P1",
            "kind": "data-dir-missing",
            "message": format!("data_dir {} does not exist", data_dir.display()),
            "recommended_action": "Run `cass doctor --json` to populate it.",
        }));
    }

    if let Some(run) = recent_run.as_ref()
        && run.status == "incomplete"
    {
        findings.push(serde_json::json!({
            "severity": "P1",
            "kind": "incomplete-run",
            "message": format!(
                "Most-recent run {} did not finish cleanly (no RunEnded event observed)",
                run.run_id
            ),
            "recommended_action": format!("cass doctor --undo {} --json", run.run_id),
        }));
    }

    let in_flight_band = recent_run.as_ref().and_then(|r| {
        crate::doctor_runs::find_in_flight_band(std::path::Path::new(&r.run_dir))
            .ok()
            .flatten()
    });
    if let Some(band) = &in_flight_band {
        findings.push(serde_json::json!({
            "severity": "P0",
            "kind": "in-flight-band",
            "message": format!("Repair band {band} started but never completed — possible crash mid-run"),
            "recommended_action": format!(
                "cass doctor --explain {} --json",
                recent_run.as_ref().map(|r| r.run_id.as_str()).unwrap_or("latest")
            ),
        }));
    }

    let quarantine_dir = data_dir.join("doctor").join("quarantine");
    let quarantine_count: usize = std::fs::read_dir(&quarantine_dir)
        .ok()
        .map(|it| it.filter_map(|e| e.ok()).count())
        .unwrap_or(0);
    if quarantine_count > 0 {
        findings.push(serde_json::json!({
            "severity": "P2",
            "kind": "non-empty-quarantine",
            "message": format!(
                "{quarantine_count} entries in {} — operator review recommended",
                quarantine_dir.display()
            ),
            "recommended_action": "cass doctor --json --quarantine",
        }));
    }

    let highest_severity = findings
        .iter()
        .filter_map(|f| f.get("severity").and_then(|s| s.as_str()))
        .min()
        .map(|s| s.to_string());

    let summary = serde_json::json!({
        "data_dir": data_dir.display().to_string(),
        "elapsed_ms": elapsed_ms,
        "recent_run": recent_run,
        "run_count": runs.len(),
        "findings_count": findings.len(),
        "highest_severity": highest_severity,
    });

    // Pass-10 + Pass-11 fix: recommended_command derives from the
    // *highest-priority* finding's recommended_action, not the first inserted
    // one. We sort by severity ('P0' < 'P1' < 'P2' < 'P3' lexicographically
    // so min() picks the most severe). Falls back to the canonical full-check
    // command when no findings exist.
    let recommended_command = findings
        .iter()
        .min_by_key(|f| {
            f.get("severity")
                .and_then(|s| s.as_str())
                .unwrap_or("P9")
                .to_string()
        })
        .and_then(|f| f.get("recommended_action").and_then(|s| s.as_str()))
        .map(|s| s.to_string())
        .unwrap_or_else(|| "cass doctor --json".to_string());

    let envelope = serde_json::json!({
        "schema_version": 2,
        "doctor_contract_version": 1,
        "kind": "robot-triage",
        "summary": summary,
        "findings": findings,
        "actions_planned": serde_json::Value::Array(Vec::new()),
        "recommended_command": recommended_command,
        "capabilities_url": "cass capabilities --json",
        "robot_docs_url": "cass robot-docs doctor",
        "next_steps": [
            "Run `cass doctor --json` for the full check report.",
            "Run `cass doctor --ls --json` to see prior runs.",
            "Run `cass robot-docs doctor` for the agent handbook."
        ]
    });

    if structured_format.is_some() {
        println!(
            "{}",
            serde_json::to_string_pretty(&envelope).map_err(|e| CliError {
                code: 9,
                kind: "internal",
                message: format!("serialize triage envelope: {e}"),
                hint: None,
                retryable: false,
            })?
        );
    } else {
        println!("cass doctor — robot-triage");
        println!("  data_dir: {}", data_dir.display());
        println!("  prior runs: {}", runs.len());
        println!("  recommended: {recommended_command}");
        println!("  full report: cass doctor --json");
        println!("  agent docs:  cass robot-docs doctor");
    }
    Ok(())
}

/// World-class-doctor pass-3: `cass doctor --diff [<REF>]`. Read-only diff
/// surface. Pass-3 ships a minimal implementation that surfaces the action
/// counts; pass-4 wires it into the actual repair planner.
pub(crate) fn run_doctor_diff(
    data_dir_override: Option<PathBuf>,
    diff_ref: &str,
    structured_format: Option<RobotFormat>,
) -> CliResult<()> {
    let data_dir = data_dir_override.unwrap_or_else(default_data_dir);
    let runs = crate::doctor_runs::list_runs(&data_dir).map_err(|e| CliError {
        code: 14,
        kind: "io",
        message: format!("failed to enumerate doctor runs: {e}"),
        hint: None,
        retryable: true,
    })?;

    // Pass-9 real diff: parse `<a>..<b>` to compare two runs (actions.jsonl
    // contents); otherwise diff the named run against the current data_dir
    // state (do the per-mutation hashes still match? did anyone tamper post-fix?).
    if let Some((a_ref, b_ref)) = diff_ref.split_once("..") {
        return diff_run_to_run(&data_dir, &runs, a_ref, b_ref, structured_format);
    }

    let target_run = if matches!(diff_ref, "" | "current" | "latest") {
        runs.first().cloned()
    } else {
        runs.iter().find(|r| r.run_id == diff_ref).cloned()
    };

    let target = match target_run {
        Some(r) => r,
        None => {
            // Pass-9: surface a clean error envelope instead of the pass-3
            // empty stub when the ref doesn't resolve.
            return Err(CliError {
                code: 13,
                kind: "not-found",
                message: format!("no run matches ref={diff_ref:?}"),
                hint: Some(
                    "Use `cass doctor --ls --json` to inspect available runs, or pass `latest`."
                        .to_string(),
                ),
                retryable: false,
            });
        }
    };

    // Diff: walk the run's actions.jsonl and check whether the post-mutation
    // hashes still match the on-disk files (i.e., did anyone modify the
    // touched files between this run and now?).
    let run_dir = std::path::PathBuf::from(&target.run_dir);
    let (records, _errs) = crate::doctor_runs::read_actions(&run_dir).map_err(|e| CliError {
        code: 14,
        kind: "io",
        message: format!("failed to read actions.jsonl: {e}"),
        hint: None,
        retryable: true,
    })?;

    let mut entries = Vec::new();
    let mut drift_count = 0usize;
    let mut intact_count = 0usize;
    let mut missing_count = 0usize;
    let mut still_absent_count = 0usize;
    let mut appeared_count = 0usize;
    for r in &records {
        if let crate::doctor_runs::ActionRecord::Mutation {
            path,
            op,
            after_blake3,
            ..
        } = r
        {
            let path_buf = std::path::PathBuf::from(path);
            let current_hash = doctor_diff_blake3_of_file(&path_buf);
            let status = match (current_hash.as_deref(), after_blake3.as_deref()) {
                (Some(c), Some(e)) if c == e => {
                    intact_count += 1;
                    "intact"
                }
                (Some(_), Some(_)) => {
                    drift_count += 1;
                    "drifted"
                }
                (None, Some(_)) => {
                    missing_count += 1;
                    "missing"
                }
                (Some(_), None) => {
                    // Post-mutation state was "no file at req.path" (e.g.
                    // Op::Quarantine), but a file is back. Idempotent-undo
                    // territory; not a hard drift.
                    appeared_count += 1;
                    "appeared-after-delete"
                }
                (None, None) => {
                    // Op::Quarantine / Op::Rename canonical case: the file
                    // was moved out of req.path during the mutation and is
                    // still gone. Undo will restore from backup.
                    still_absent_count += 1;
                    "still-absent"
                }
            };
            entries.push(serde_json::json!({
                "path": path,
                "op": op,
                "expected_blake3": after_blake3,
                "current_blake3": current_hash,
                "status": status,
            }));
        }
    }

    // Pass-11 fix: the prior `next_command` logic was upside-down. It
    // suggested `--undo` when drift_count > 0, but undo refuses with
    // AfterHashMismatch on drift (exit 4). Correct semantics:
    //   - drift > 0  → tampering detected; suggest `--explain` for forensics.
    //   - intact + still-absent > 0 (and no drift) → undo will succeed; suggest it.
    //   - everything else → no actionable next step beyond the full check.
    let next_command = if drift_count > 0 {
        format!("cass doctor --explain {} --json", target.run_id)
    } else if intact_count > 0 || still_absent_count > 0 {
        format!("cass doctor --undo {} --json", target.run_id)
    } else {
        "cass doctor --json".to_string()
    };

    let envelope = serde_json::json!({
        "schema_version": 2,
        "doctor_contract_version": 1,
        "kind": "diff",
        "ref": diff_ref,
        "target_run": target,
        "data_dir": data_dir.display().to_string(),
        "summary": {
            "total_mutations": entries.len(),
            "intact": intact_count,
            "drifted": drift_count,
            "missing": missing_count,
            "still_absent": still_absent_count,
            "appeared_after_delete": appeared_count,
        },
        "entries": entries,
        "next_command": next_command,
    });

    if structured_format.is_some() {
        println!(
            "{}",
            serde_json::to_string_pretty(&envelope).map_err(|e| CliError {
                code: 9,
                kind: "internal",
                message: format!("serialize diff envelope: {e}"),
                hint: None,
                retryable: false,
            })?
        );
    } else {
        println!("Diff target: {}", target.run_id);
        println!("  status: {}", target.status);
        println!(
            "  mutations: {} intact, {} drifted, {} missing",
            intact_count, drift_count, missing_count
        );
    }
    Ok(())
}

/// Pass-9 helper: compare two runs by their actions.jsonl contents. Returns
/// added/removed/changed mutations from `a_ref` to `b_ref`. Read-only.
pub(crate) fn diff_run_to_run(
    data_dir: &std::path::Path,
    runs: &[crate::doctor_runs::RunSummary],
    a_ref: &str,
    b_ref: &str,
    structured_format: Option<RobotFormat>,
) -> CliResult<()> {
    let resolve = |r: &str| -> Option<crate::doctor_runs::RunSummary> {
        if matches!(r, "" | "current" | "latest") {
            runs.first().cloned()
        } else {
            runs.iter().find(|x| x.run_id == r).cloned()
        }
    };
    let a = resolve(a_ref).ok_or_else(|| CliError {
        code: 13,
        kind: "not-found",
        message: format!("no run matches ref={a_ref:?}"),
        hint: Some("Use `cass doctor --ls --json` to inspect.".to_string()),
        retryable: false,
    })?;
    let b = resolve(b_ref).ok_or_else(|| CliError {
        code: 13,
        kind: "not-found",
        message: format!("no run matches ref={b_ref:?}"),
        hint: Some("Use `cass doctor --ls --json` to inspect.".to_string()),
        retryable: false,
    })?;

    let load = |run_dir: &str| -> CliResult<std::collections::BTreeMap<String, serde_json::Value>> {
        let path = std::path::Path::new(run_dir);
        let (records, _errs) = crate::doctor_runs::read_actions(path).map_err(|e| CliError {
            code: 14,
            kind: "io",
            message: format!("failed to read actions.jsonl: {e}"),
            hint: None,
            retryable: true,
        })?;
        let mut map = std::collections::BTreeMap::new();
        for r in records {
            if let crate::doctor_runs::ActionRecord::Mutation {
                path,
                op,
                before_blake3,
                after_blake3,
                ..
            } = r
            {
                map.insert(
                    path.clone(),
                    serde_json::json!({
                        "op": op,
                        "before_blake3": before_blake3,
                        "after_blake3": after_blake3,
                    }),
                );
            }
        }
        Ok(map)
    };
    let a_map = load(&a.run_dir)?;
    let b_map = load(&b.run_dir)?;

    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut changed = Vec::new();
    for (path, b_value) in &b_map {
        if let Some(a_value) = a_map.get(path) {
            if a_value != b_value {
                changed.push(serde_json::json!({"path": path, "a": a_value, "b": b_value}));
            }
        } else {
            added.push(serde_json::json!({"path": path, "b": b_value}));
        }
    }
    for (path, a_value) in &a_map {
        if !b_map.contains_key(path) {
            removed.push(serde_json::json!({"path": path, "a": a_value}));
        }
    }

    let envelope = serde_json::json!({
        "schema_version": 2,
        "doctor_contract_version": 1,
        "kind": "diff-run-to-run",
        "a_run": a,
        "b_run": b,
        "data_dir": data_dir.display().to_string(),
        "summary": {
            "added": added.len(),
            "removed": removed.len(),
            "changed": changed.len(),
            "common": a_map.iter().filter(|(p, _)| b_map.contains_key(*p)).count(),
        },
        "added": added,
        "removed": removed,
        "changed": changed,
    });
    if structured_format.is_some() {
        println!(
            "{}",
            serde_json::to_string_pretty(&envelope).map_err(|e| CliError {
                code: 9,
                kind: "internal",
                message: format!("serialize diff-run-to-run envelope: {e}"),
                hint: None,
                retryable: false,
            })?
        );
    } else {
        println!("Run-to-run diff: {} → {}", a.run_id, b.run_id);
        println!(
            "  added={} removed={} changed={}",
            added.len(),
            removed.len(),
            changed.len()
        );
    }
    Ok(())
}

/// Pass-9 helper: blake3 of a file by path. Returns None if the file is
/// missing or unreadable. Used by --diff to detect post-run drift.
pub(crate) fn doctor_diff_blake3_of_file(path: &std::path::Path) -> Option<String> {
    let bytes = std::fs::read(path).ok()?;
    let mut h = blake3::Hasher::new();
    h.update(&bytes);
    Some(h.finalize().to_hex().to_string())
}

/// World-class-doctor pass-3: `cass doctor --gc-before <ISO8601> --yes`.
/// Quarantines runs older than `before_iso` into
/// `<data_dir>/doctor/quarantine/runs/<run-id>/`. Never deletes (per
/// AGENTS.md RULE NUMBER 1).
///
/// Both `--gc-before` and `--yes` are required (the destructive-action gate;
/// invoking with one but not the other refuses with exit 4).
pub(crate) fn run_doctor_gc(
    data_dir_override: Option<PathBuf>,
    before_iso: &str,
    yes: bool,
    structured_format: Option<RobotFormat>,
) -> CliResult<()> {
    let data_dir = data_dir_override.unwrap_or_else(default_data_dir);
    if !yes {
        return Err(CliError {
            code: 4,
            kind: "refused-unsafe",
            message: "`cass doctor --gc-before` requires `--yes` to apply".to_string(),
            hint: Some(
                "Re-run with both `--gc-before <ISO>` and `--yes`. Use `--gc-before <ISO> --json` (without --yes) for a dry run preview is a pass-4 task.".to_string(),
            ),
            retryable: false,
        });
    }
    let cutoff_ms = parse_iso8601_to_unix_ms(before_iso).ok_or_else(|| CliError {
        code: 2,
        kind: "usage",
        message: format!("`--gc-before` value {before_iso:?} is not a valid ISO8601 timestamp"),
        hint: Some(
            "Use a timestamp like `2026-04-01T00:00:00Z`. Sub-second precision is ignored."
                .to_string(),
        ),
        retryable: false,
    })?;
    let runs = crate::doctor_runs::list_runs(&data_dir).map_err(|e| CliError {
        code: 14,
        kind: "io",
        message: format!("failed to enumerate doctor runs: {e}"),
        hint: None,
        retryable: true,
    })?;
    let mut quarantined: Vec<String> = Vec::new();
    let quarantine_root = data_dir.join("doctor").join("quarantine").join("runs");
    std::fs::create_dir_all(&quarantine_root).map_err(|e| CliError {
        code: 14,
        kind: "io",
        message: format!(
            "failed to create quarantine dir {}: {e}",
            quarantine_root.display()
        ),
        hint: None,
        retryable: true,
    })?;
    for run in &runs {
        if let Some(started_at_ms) = run.started_at_ms
            && started_at_ms < cutoff_ms
        {
            let src = std::path::PathBuf::from(&run.run_dir);
            let dst = quarantine_root.join(&run.run_id);
            // Atomic rename: never deletes; if the dest already exists, we
            // append a millisecond suffix.
            let final_dst = if dst.exists() {
                quarantine_root.join(format!("{}.{}", run.run_id, current_unix_ms_for_gc()))
            } else {
                dst
            };
            if let Err(e) = std::fs::rename(&src, &final_dst) {
                return Err(CliError {
                    code: 14,
                    kind: "io",
                    message: format!(
                        "failed to quarantine run {} → {}: {e}",
                        src.display(),
                        final_dst.display()
                    ),
                    hint: Some(
                        "Inspect the run dir manually; the gc operation halted at this run."
                            .to_string(),
                    ),
                    retryable: true,
                });
            }
            quarantined.push(run.run_id.clone());
        }
    }
    let envelope = serde_json::json!({
        "schema_version": 2,
        "doctor_contract_version": 1,
        "kind": "gc",
        "before_iso": before_iso,
        "before_unix_ms": cutoff_ms,
        "quarantined_run_ids": quarantined,
        "quarantined_count": quarantined.len(),
        "quarantine_root": quarantine_root.display().to_string(),
        "data_dir": data_dir.display().to_string(),
        "note": "Runs were renamed into quarantine; cass never deletes. Operator owns final disk reclamation.",
    });
    if structured_format.is_some() {
        println!(
            "{}",
            serde_json::to_string_pretty(&envelope).map_err(|e| CliError {
                code: 9,
                kind: "internal",
                message: format!("serialize gc envelope: {e}"),
                hint: None,
                retryable: false,
            })?
        );
    } else {
        println!(
            "Quarantined {} run(s) into {}",
            quarantined.len(),
            quarantine_root.display()
        );
        for id in &quarantined {
            println!("  • {id}");
        }
    }
    Ok(())
}

pub(crate) fn current_unix_ms_for_gc() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

/// World-class-doctor pass-6: `cass doctor --watch`. Long-running monitor
/// that re-reads `<data_dir>/doctor/runs/` on a poll interval and emits one
/// JSONL event per observed change. Read-only; never mutates.
///
/// The event stream:
///
/// ```jsonl
/// {"schema_version": 2, "kind": "watch-tick", "iteration": 0, "run_count": 3, "elapsed_ms": 1, ...}
/// {"schema_version": 2, "kind": "watch-tick", "iteration": 1, ...}
/// ```
///
/// Each tick reports the current run count + the most recent run id.
/// Pass-7 extends with per-band-progress events when journal events stream
/// in real time.
///
/// Stop conditions:
/// - SIGINT (Ctrl+C) — handled by the OS default signal disposition; the
///   process terminates immediately and the shell reports exit code 130.
///   Each emitted JSONL event is line-flushed via `println!` before the
///   thread sleeps, so SIGINT during the sleep window never loses a tick
///   the operator already saw on stdout.
/// - `--watch-iterations N` reached (0 = run forever)
///
/// Pass-9 deliberately keeps the SIGINT path on the OS default rather than
/// adding a `signal_hook`/`ctrlc` dependency or an `unsafe` libc::signal
/// handler (forbidden by AGENTS.md). Agents reading this code should rely
/// on the documented exit-code-130 contract for clean cancellation.
pub(crate) fn run_doctor_watch(
    data_dir_override: Option<PathBuf>,
    poll_interval_ms: u64,
    iterations: u64,
    structured_format: Option<RobotFormat>,
) -> CliResult<()> {
    let data_dir = data_dir_override.unwrap_or_else(default_data_dir);
    let interval = std::time::Duration::from_millis(poll_interval_ms.clamp(500, 60_000));

    let mut iter = 0u64;
    loop {
        let started = std::time::Instant::now();
        let runs = crate::doctor_runs::list_runs(&data_dir).unwrap_or_default();
        let recent = runs.first();
        let elapsed_ms = started.elapsed().as_millis() as u64;
        let in_flight_band = recent.and_then(|r| {
            crate::doctor_runs::find_in_flight_band(std::path::Path::new(&r.run_dir))
                .ok()
                .flatten()
        });

        let event = serde_json::json!({
            "schema_version": 2,
            "doctor_contract_version": 1,
            "kind": "watch-tick",
            "iteration": iter,
            "wall_clock_ms": current_unix_ms_for_gc(),
            "elapsed_ms": elapsed_ms,
            "data_dir": data_dir.display().to_string(),
            "run_count": runs.len(),
            "most_recent_run_id": recent.map(|r| r.run_id.clone()),
            "most_recent_status": recent.map(|r| r.status.to_string()),
            "in_flight_band": in_flight_band,
        });

        if structured_format.is_some() {
            // JSONL: one event per line. Don't pretty-print — agents pipe to jq.
            println!(
                "{}",
                serde_json::to_string(&event).map_err(|e| CliError {
                    code: 9,
                    kind: "internal",
                    message: format!("serialize watch event: {e}"),
                    hint: None,
                    retryable: false,
                })?
            );
        } else {
            println!(
                "tick {iter}: runs={} recent={} status={} in_flight_band={}",
                runs.len(),
                recent.map(|r| r.run_id.as_str()).unwrap_or("-"),
                recent.map(|r| r.status).unwrap_or("-"),
                in_flight_band.as_deref().unwrap_or("-"),
            );
        }

        iter += 1;
        if iterations != 0 && iter >= iterations {
            break;
        }
        std::thread::sleep(interval);
    }
    Ok(())
}

/// World-class-doctor pass-8: `cass doctor --explain <run-id>`. Returns a
/// single JSON envelope describing every recorded action in the run plus a
/// flattened band timeline. Read-only.
pub(crate) fn run_doctor_explain(
    data_dir_override: Option<PathBuf>,
    run_id_arg: &str,
    structured_format: Option<RobotFormat>,
) -> CliResult<()> {
    let data_dir = data_dir_override.unwrap_or_else(default_data_dir);
    let resolved = if matches!(run_id_arg, "latest" | "current") {
        match crate::doctor_runs::find_latest_run(&data_dir).map_err(|e| CliError {
            code: 14,
            kind: "io",
            message: format!("failed to enumerate doctor runs: {e}"),
            hint: Some("Run `cass doctor --ls --json` to inspect.".to_string()),
            retryable: true,
        })? {
            Some(latest) => latest.run_id.clone(),
            None => {
                return Err(CliError {
                    code: 13,
                    kind: "not-found",
                    message: format!("no doctor runs found under {data_dir:?}"),
                    hint: Some(
                        "Run `cass doctor --ls --json` to inspect; no runs means there is nothing to explain."
                            .to_string(),
                    ),
                    retryable: false,
                });
            }
        }
    } else {
        run_id_arg.to_string()
    };
    let run_id = crate::doctor_runs::RunId::parse(&resolved).ok_or_else(|| CliError {
        code: 2,
        kind: "usage",
        message: format!(
            "{run_id_arg:?} is not a valid run id (expected `<ISO8601>__<6-hex>` or `latest`)"
        ),
        hint: Some("Use `cass doctor --ls --json` to find a valid run id.".to_string()),
        retryable: false,
    })?;
    let run_dir = crate::doctor_runs::run_dir_for(&data_dir, &run_id);
    if !run_dir.exists() {
        return Err(CliError {
            code: 13,
            kind: "not-found",
            message: format!("run {} not found at {}", resolved, run_dir.display()),
            hint: Some("Run `cass doctor --ls --json` to confirm the run id.".to_string()),
            retryable: false,
        });
    }
    let (records, parse_errs) =
        crate::doctor_runs::read_actions(&run_dir).map_err(|e| CliError {
            code: 14,
            kind: "io",
            message: format!("failed to read actions.jsonl: {e}"),
            hint: None,
            retryable: true,
        })?;
    let in_flight_band = crate::doctor_runs::find_in_flight_band(&run_dir)
        .ok()
        .flatten();
    // Categorize records for the human-friendly surface.
    let mut mutation_count = 0usize;
    let mut band_starts = 0usize;
    let mut band_completes = 0usize;
    let mut run_started = false;
    let mut run_ended_code: Option<i32> = None;
    for r in &records {
        match r {
            crate::doctor_runs::ActionRecord::RunStarted { .. } => run_started = true,
            crate::doctor_runs::ActionRecord::RunEnded { exit_code, .. } => {
                run_ended_code = Some(*exit_code);
            }
            crate::doctor_runs::ActionRecord::Mutation { .. } => mutation_count += 1,
            crate::doctor_runs::ActionRecord::BandStarted { .. } => band_starts += 1,
            crate::doctor_runs::ActionRecord::BandCompleted { .. } => band_completes += 1,
        }
    }
    let envelope = serde_json::json!({
        "schema_version": 2,
        "doctor_contract_version": 1,
        "kind": "explain",
        "run_id": resolved,
        "run_dir": run_dir.display().to_string(),
        "summary": {
            "run_started": run_started,
            "run_ended_exit_code": run_ended_code,
            "mutation_count": mutation_count,
            "band_starts": band_starts,
            "band_completes": band_completes,
            "in_flight_band": in_flight_band,
            "actions_jsonl_parse_errors": parse_errs.len(),
        },
        "actions": records,
        // Pass-11 fix (P3): the underlying parse-error tuple stores 0-indexed
        // enumerate offsets. Agents expect 1-indexed file line numbers to match
        // editor + grep conventions, so we add 1 here.
        "actions_jsonl_parse_errors": parse_errs.iter().map(|(idx, msg)| serde_json::json!({"line": idx + 1, "error": msg})).collect::<Vec<_>>(),
    });
    if structured_format.is_some() {
        println!(
            "{}",
            serde_json::to_string_pretty(&envelope).map_err(|e| CliError {
                code: 9,
                kind: "internal",
                message: format!("serialize explain envelope: {e}"),
                hint: None,
                retryable: false,
            })?
        );
    } else {
        println!("Explanation of run {resolved}");
        println!("  run_dir:        {}", run_dir.display());
        println!("  mutations:      {mutation_count}");
        println!("  bands started:  {band_starts}");
        println!("  bands done:     {band_completes}");
        if let Some(code) = run_ended_code {
            println!("  exit_code:      {code}");
        }
        if let Some(b) = &in_flight_band {
            println!("  in-flight band: {b} (run did not complete cleanly)");
        }
        if !parse_errs.is_empty() {
            println!("  jsonl parse errors: {}", parse_errs.len());
        }
    }
    Ok(())
}

/// World-class-doctor pass-8: `cass doctor --emit-capabilities`. Returns the
/// extended capability surface (detectors[], fixers[], exit_codes[],
/// data_paths[], env_vars[]) the agent surface promises. Pass-8 ships a
/// stable enumeration of the canonical surface; future passes refine the
/// per-detector / per-fixer metadata.
pub(crate) fn run_doctor_emit_capabilities(
    structured_format: Option<RobotFormat>,
) -> CliResult<()> {
    let envelope = serde_json::json!({
        "schema_version": 2,
        "doctor_contract_version": 1,
        "kind": "doctor-capabilities",
        "binary_version": env!("CARGO_PKG_VERSION"),
        "detectors": [
            {"name": "operation_state", "kind": "lock-state", "since_pass": 0, "auto_fixable": true},
            {"name": "repair_failure_marker", "kind": "meta", "since_pass": 0, "auto_fixable": false},
            {"name": "data_directory", "kind": "filesystem", "since_pass": 0, "auto_fixable": true},
            {"name": "storage_pressure", "kind": "resource", "since_pass": 0, "auto_fixable": false},
            {"name": "full_rebuild_readiness", "kind": "resource", "since_pass": 0, "auto_fixable": false},
            {"name": "stale_lock", "kind": "lock-state", "since_pass": 0, "auto_fixable": true},
            {"name": "database", "kind": "integrity", "since_pass": 0, "auto_fixable": true},
            {"name": "database_schema", "kind": "integrity", "since_pass": 0, "auto_fixable": true},
            {"name": "lexical_index", "kind": "derived-asset", "since_pass": 0, "auto_fixable": true},
            {"name": "lexical_index_consistency", "kind": "derived-asset", "since_pass": 0, "auto_fixable": true},
            {"name": "semantic_index", "kind": "derived-asset", "since_pass": 0, "auto_fixable": true},
            {"name": "archive_sources", "kind": "external-source", "since_pass": 0, "auto_fixable": false},
            {"name": "config_exclusion_risk", "kind": "policy", "since_pass": 0, "auto_fixable": false},
            {"name": "safe_auto_run", "kind": "meta", "since_pass": 0, "auto_fixable": true}
        ],
        "fixers": [
            {"name": "create_data_directory", "op_kind": "create-dir", "since_pass": 0},
            {"name": "remove_stale_legacy_index_lock", "op_kind": "rename", "since_pass": 0},
            {"name": "rebuild_lexical_index", "op_kind": "atomic-swap", "since_pass": 0},
            {"name": "restore_semantic_model", "op_kind": "rename", "since_pass": 0},
            {"name": "resync_raw_mirrors", "op_kind": "write", "since_pass": 0},
            {"name": "normalize_archive_layout", "op_kind": "rename", "since_pass": 0},
            {"name": "promote_candidate_archive", "op_kind": "atomic-swap", "since_pass": 0},
            {"name": "restore_from_backup", "op_kind": "atomic-swap", "since_pass": 0}
        ],
        // Exactly the (code, kind) pairs doctor and its subcommands return
        // (2l1b0.58). Findings without a failed check exit 0; the payload's
        // operation_outcome.exit_code_kind names them. Failed checks exit 5
        // `doctor`; a repair blocked by an operation lock exits 7 `index-busy`.
        "exit_codes": [
            {"code": 0, "kind": "success", "retryable_via_kind_branch": false},
            {"code": 2, "kind": "usage", "retryable_via_kind_branch": false},
            {"code": 3, "kind": "repair-failure", "retryable_via_kind_branch": false},
            {"code": 4, "kind": "refused-unsafe", "retryable_via_kind_branch": false},
            {"code": 4, "kind": "output-not-writable", "retryable_via_kind_branch": false},
            {"code": 5, "kind": "doctor", "retryable_via_kind_branch": true},
            {"code": 5, "kind": "data-corruption", "retryable_via_kind_branch": false},
            {"code": 7, "kind": "index-busy", "retryable_via_kind_branch": true},
            {"code": 9, "kind": "internal", "retryable_via_kind_branch": false},
            {"code": 10, "kind": "config", "retryable_via_kind_branch": false},
            {"code": 13, "kind": "not-found", "retryable_via_kind_branch": false},
            {"code": 14, "kind": "io", "retryable_via_kind_branch": true}
        ],
        "data_paths": [
            {"path_kind": "data_dir", "default": "~/.local/share/coding-agent-search/", "writable_by_doctor": true},
            {"path_kind": "lock_dir", "default": "<data_dir>/doctor/locks/", "writable_by_doctor": true},
            {"path_kind": "runs_dir", "default": "<data_dir>/doctor/runs/", "writable_by_doctor": true},
            {"path_kind": "quarantine_dir", "default": "<data_dir>/doctor/quarantine/", "writable_by_doctor": true},
            {"path_kind": "events_dir", "default": "<data_dir>/doctor/events/", "writable_by_doctor": true},
            {"path_kind": "agent_search_db", "default": "<data_dir>/agent_search.db", "writable_by_doctor": true},
            {"path_kind": "lexical_index", "default": "<data_dir>/index/", "writable_by_doctor": true},
            {"path_kind": "semantic_models", "default": "<data_dir>/models/", "writable_by_doctor": true},
            {"path_kind": "vector_index", "default": "<data_dir>/vector_index/", "writable_by_doctor": true},
            {"path_kind": "user_config", "default": "~/.config/cass/", "writable_by_doctor": false}
        ],
        "env_vars": [
            {"name": "CASS_IGNORE_SOURCES_CONFIG", "purpose": "test isolation"},
            {"name": "CASS_LEXICAL_PUBLISH_BACKUP_RETENTION", "purpose": "lexical backup retention limit"},
            {"name": "CASS_SEMANTIC_EMBEDDER", "purpose": "force local semantic embedder: hash, minilm or multilingual-minilm"},
            {"name": "CASS_TEST_DOCTOR_CANDIDATE_PROMOTION_FAILPOINT", "purpose": "test failpoint"},
            {"name": "CASS_TEST_DOCTOR_RENAME_FAILURE", "purpose": "test failpoint"}
            // Pass-11 fix: removed the previously-advertised
            // CASS_DOCTOR_LOCK_HEARTBEAT_STALE_AFTER_MS env var. The const at
            // src/lib.rs:24317 was never wired to read it — false advertising
            // to agents. Future passes may add the env override; until then
            // the threshold is compile-time fixed at 5min.
        ],
        // Coordination-surface env vars retired by the local-only conversion.
        // They are still *recognized* here (as ignored) so agents get explicit
        // migration guidance instead of silently dereferencing stale config —
        // the "diagnose only" policy: report, never execute.
        "deprecated_env_vars": [
            {"name": "CASS_SWARM_AGENT_MAIL_URL", "status": "deprecated-ignored", "note": "Agent Mail was removed with the swarm coordination layer; the value is ignored. Local handoffs: use `cass search`/`cass pack`."},
            {"name": "CASS_SWARM_AGENT_MAIL_TOKEN", "status": "deprecated-ignored", "note": "Agent Mail was removed with the swarm coordination layer; the value is ignored."},
            {"name": "CASS_FLEET_BUDGET_MS", "status": "deprecated-ignored", "note": "Fleet commands were removed by the local-only conversion; the value is ignored."},
            {"name": "CASS_FLEET_PER_HOST_BUDGET_MS", "status": "deprecated-ignored", "note": "Fleet commands were removed by the local-only conversion; the value is ignored."},
            {"name": "CASS_SKIP_UPDATE", "status": "deprecated-ignored", "note": "Update checks were removed by the local-only conversion; cass never phones home, so no opt-out is needed."},
            {"name": "CODING_AGENT_SEARCH_NO_UPDATE_PROMPT", "status": "deprecated-ignored", "note": "Update checks were removed by the local-only conversion; the prompt no longer exists, so the value is ignored."},
            {"name": "CASS_UPDATE_API_BASE_URL", "status": "deprecated-ignored", "note": "Update checks were removed by the local-only conversion; cass never contacts a release API, so the value is ignored."},
            {"name": "CASS_SSH_CONFIG", "status": "deprecated-ignored", "note": "Remote synchronization was removed by the local-only conversion; no SSH config is consulted, so the value is ignored."},
            {"name": "CASS_SEMANTIC_DOWNLOAD_POLICY", "status": "deprecated-ignored", "note": "Model downloads were removed by the local-only conversion; models install only via `cass models install --from-file <DIR>`, so the value is ignored."},
            {"name": "CASS_EXTERNAL_EMBEDDINGS", "status": "deprecated-ignored", "note": "The external HTTP embedding provider was removed by the local-only conversion; its consent flag is ignored and no endpoint is contacted. Local providers: `--embedder minilm|multilingual-minilm|hash`."},
            {"name": "CASS_EXTERNAL_EMBEDDING_URL", "status": "deprecated-ignored", "note": "The external HTTP embedding provider was removed by the local-only conversion; the endpoint URL is never dereferenced."},
            {"name": "CASS_EXTERNAL_EMBEDDING_MODEL", "status": "deprecated-ignored", "note": "The external HTTP embedding provider was removed by the local-only conversion; the remote model name is ignored. Local models install via `cass models install --from-file <DIR>`."},
            {"name": "CASS_EXTERNAL_EMBEDDING_DIMENSION", "status": "deprecated-ignored", "note": "The external HTTP embedding provider was removed by the local-only conversion; the remote dimension is ignored."},
            {"name": "CASS_EXTERNAL_EMBEDDING_REVISION", "status": "deprecated-ignored", "note": "The external HTTP embedding provider was removed by the local-only conversion; the remote revision is ignored."},
            {"name": "CASS_EXTERNAL_EMBEDDING_TIMEOUT_MS", "status": "deprecated-ignored", "note": "The external HTTP embedding provider was removed by the local-only conversion; there are no embedding HTTP requests to time out."},
            {"name": "CASS_EXTERNAL_EMBEDDING_BATCH_SIZE", "status": "deprecated-ignored", "note": "The external HTTP embedding provider was removed by the local-only conversion; the value is ignored. Tune local batches with `--batch-conversations`."},
            {"name": "CASS_EXTERNAL_EMBEDDING_MAX_REQUEST_BYTES", "status": "deprecated-ignored", "note": "The external HTTP embedding provider was removed by the local-only conversion; no embedding request is ever built or sent."},
            {"name": "CASS_EXTERNAL_EMBEDDING_API_KEY", "status": "deprecated-ignored", "note": "The external HTTP embedding provider was removed by the local-only conversion; the credential is ignored and never sent anywhere."},
            {"name": "CASS_EXTERNAL_EMBEDDING_MAX_RETRIES", "status": "deprecated-ignored", "note": "The external HTTP embedding provider was removed by the local-only conversion; there are no embedding requests to retry."}
        ],
        "subcommands_added_by_world_class_doctor": [
            {"flag": "--ls", "added_in_pass": 2, "kind": "read-only"},
            {"flag": "--undo <RUN_ID>", "added_in_pass": 2, "kind": "mutating-via-chokepoint"},
            {"flag": "--robot-triage", "added_in_pass": 3, "kind": "read-only-mega-command"},
            {"flag": "--diff [<REF>]", "added_in_pass": 3, "kind": "read-only"},
            {"flag": "--gc-before <ISO> --yes", "added_in_pass": 3, "kind": "destructive-via-quarantine"},
            {"flag": "--watch", "added_in_pass": 6, "kind": "read-only-streaming"},
            {"flag": "--explain <RUN_ID>", "added_in_pass": 8, "kind": "read-only"},
            {"flag": "--emit-capabilities", "added_in_pass": 8, "kind": "read-only-self-describe"}
        ]
    });
    if structured_format.is_some() {
        println!(
            "{}",
            serde_json::to_string_pretty(&envelope).map_err(|e| CliError {
                code: 9,
                kind: "internal",
                message: format!("serialize emit-capabilities envelope: {e}"),
                hint: None,
                retryable: false,
            })?
        );
    } else {
        println!("cass doctor — extended capabilities surface");
        if let Some(detectors) = envelope.get("detectors").and_then(|v| v.as_array()) {
            println!("  {} detectors", detectors.len());
        }
        if let Some(fixers) = envelope.get("fixers").and_then(|v| v.as_array()) {
            println!("  {} fixers", fixers.len());
        }
        if let Some(codes) = envelope.get("exit_codes").and_then(|v| v.as_array()) {
            println!("  {} exit-code kinds", codes.len());
        }
        println!("  use --json for the full envelope");
    }
    Ok(())
}

/// Best-effort ISO8601 parser. Accepts `YYYY-MM-DDTHH:MM:SSZ` and
/// `YYYY-MM-DD` (midnight-Z assumed). Returns ms since epoch.
pub(crate) fn parse_iso8601_to_unix_ms(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp_millis())
        .or_else(|| {
            let nd = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()?;
            let ndt = nd.and_hms_opt(0, 0, 0)?;
            Some(ndt.and_utc().timestamp_millis())
        })
}

pub(crate) fn run_doctor_archive_export_impl(
    request: DoctorArchiveExportRequest,
    wrap: WrapConfig,
) -> CliResult<()> {
    let started = Instant::now();
    let data_dir = resolve_data_dir(&request.data_dir_override, request.db_override.as_ref());
    let db_path = request
        .db_override
        .unwrap_or_else(|| data_dir.join("agent_search.db"));
    let structured_format = request
        .output_format
        .or_else(robot_format_from_env)
        .map(|fmt| {
            if matches!(fmt, RobotFormat::Sessions) {
                RobotFormat::Compact
            } else {
                fmt
            }
        });
    if request.backup_id.is_some() || request.force_rebuild || request.allow_repeated_repair {
        return Err(CliError {
            code: 2,
            kind: "usage",
            message: "`cass doctor archive export|relocate` does not accept backup, rebuild, or repeated-repair controls".to_string(),
            hint: Some("Run archive preservation with only a target path, then apply with the exact plan fingerprint.".to_string()),
            retryable: false,
        });
    }
    if request.command_mode == DoctorArchiveExportCommandMode::Verify
        && (request.dry_run || request.yes || request.plan_fingerprint.is_some())
    {
        return Err(CliError {
            code: 2,
            kind: "usage",
            message: "`cass doctor archive export verify` is read-only and does not accept apply controls".to_string(),
            hint: Some("Run verify with only the exported target directory and --json.".to_string()),
            retryable: false,
        });
    }
    if request.command_mode == DoctorArchiveExportCommandMode::Apply
        && request
            .plan_fingerprint
            .as_deref()
            .is_none_or(str::is_empty)
    {
        return Err(CliError {
            code: 2,
            kind: "usage",
            message: "archive export apply requires --plan-fingerprint".to_string(),
            hint: Some(
                "Run the archive export dry-run first and apply the exact reported fingerprint."
                    .to_string(),
            ),
            retryable: false,
        });
    }
    if request.command_mode == DoctorArchiveExportCommandMode::Apply && request.dry_run {
        return Err(CliError {
            code: 2,
            kind: "usage",
            message: "archive export apply cannot combine --dry-run and --yes".to_string(),
            hint: Some(
                "Run the dry-run first, then apply with only --yes and the exact fingerprint."
                    .to_string(),
            ),
            retryable: false,
        });
    }
    if request.command_mode == DoctorArchiveExportCommandMode::Plan
        && request.plan_fingerprint.is_some()
    {
        return Err(CliError {
            code: 2,
            kind: "usage",
            message: "archive export planning does not accept --plan-fingerprint without --yes"
                .to_string(),
            hint: Some(
                "Run the dry-run without a fingerprint, then rerun with --yes --plan-fingerprint."
                    .to_string(),
            ),
            retryable: false,
        });
    }
    let Some(target_root) = request.target_root.as_ref() else {
        return Err(CliError {
            code: 2,
            kind: "usage",
            message: "`cass doctor archive export|relocate` requires a target directory"
                .to_string(),
            hint: Some(
                "Example: cass doctor archive export /mnt/bigdisk/cass-archive-export --json"
                    .to_string(),
            ),
            retryable: false,
        });
    };
    doctor_archive_export_validate_target(target_root, &data_dir).map_err(|message| CliError {
        code: 2,
        kind: "usage",
        message,
        hint: Some(
            "Choose a normal absolute target outside the current cass data directory.".to_string(),
        ),
        retryable: false,
    })?;

    if request.command_mode == DoctorArchiveExportCommandMode::Verify {
        let verify_status = doctor_archive_export_verify_target(target_root, &data_dir);
        let event_log = doctor_archive_export_event_log(
            "doctor_archive_export_verify",
            request.workflow,
            request.command_mode,
            verify_status
                .get("checked_asset_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0) as usize,
            started.elapsed().as_millis() as u64,
            verify_status
                .get("status")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown"),
        );
        let payload = serde_json::json!({
            "schema_version": 1,
            "surface": "archive-export",
            "mode": request.command_mode.stable_name(),
            "workflow": request.workflow.stable_name(),
            "status": verify_status["status"].clone(),
            "target_root": target_root.display().to_string(),
            "redacted_target_root": doctor_redacted_path(&target_root.display().to_string(), &data_dir),
            "verify_status": verify_status,
            "event_log": event_log,
            "event_log_path": serde_json::Value::Null,
            "old_archive_retained": true,
            "config_update_status": "not_requested",
            "recommended_action": "If verification is clean, run cass with the exported data path only after retaining the old archive as evidence.",
        });
        if let Some(fmt) = structured_format {
            output_structured_value(payload, fmt)?;
        } else {
            print_doctor_human_line(
                wrap,
                format!(
                    "Doctor archive export verify: {}",
                    payload["status"].as_str().unwrap_or("unknown")
                ),
            );
        }
        return Ok(());
    }

    let (assets, warnings) = doctor_archive_export_collect_assets(&data_dir, &db_path);
    let plan = doctor_archive_export_plan_value(
        request.workflow,
        &data_dir,
        &db_path,
        target_root,
        &assets,
        &warnings,
    );
    let plan_fingerprint = plan["plan_fingerprint"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let required_bytes = plan["required_bytes"].as_u64().unwrap_or(0);
    let target_available_bytes = doctor_archive_export_target_available_bytes(target_root).ok();
    let mut blocked_reasons = Vec::new();
    if let Some(available) = target_available_bytes
        && request.command_mode == DoctorArchiveExportCommandMode::Apply
        && available < required_bytes
    {
        blocked_reasons.push(format!(
            "target has {available} available bytes but archive export requires {required_bytes}"
        ));
    }
    if request.command_mode == DoctorArchiveExportCommandMode::Apply
        && request.plan_fingerprint.as_deref() != Some(plan_fingerprint.as_str())
    {
        blocked_reasons.push(
            "provided plan fingerprint does not match current archive export plan".to_string(),
        );
    }

    let mut copied_bytes = 0_u64;
    let mut receipts = Vec::new();
    let mut verify_status = serde_json::json!({
        "status": "not-run",
        "reason": "dry-run only",
    });
    let mut event_log_path = serde_json::Value::Null;
    let status = if request.command_mode == DoctorArchiveExportCommandMode::Plan {
        "planned"
    } else if !blocked_reasons.is_empty() {
        "blocked"
    } else {
        doctor_forensic_create_private_dir_all(target_root).map_err(|err| CliError {
            code: 14,
            kind: "io",
            message: format!("failed to create archive export target: {err}"),
            hint: Some("Choose a writable target on a filesystem with enough space.".to_string()),
            retryable: true,
        })?;
        let (copied, copy_receipts, copy_blockers) =
            doctor_archive_export_copy_assets(target_root, &assets);
        copied_bytes = copied;
        receipts = copy_receipts;
        blocked_reasons.extend(copy_blockers);
        let manifest_path = doctor_archive_export_manifest_path(target_root);
        let manifest = serde_json::json!({
            "schema_version": 1,
            "manifest_kind": "cass_doctor_archive_export_v1",
            "workflow": request.workflow.stable_name(),
            "relocation_plan_id": plan_fingerprint,
            "plan_fingerprint": plan_fingerprint,
            "created_at_ms": doctor_now_ms(),
            "source_roots": plan["source_roots"].clone(),
            "redacted_source_roots": plan["redacted_source_roots"].clone(),
            "target_root": target_root.display().to_string(),
            "required_bytes": required_bytes,
            "copied_bytes": copied_bytes,
            "privacy_mode": plan["privacy_mode"].clone(),
            "compression": plan["compression"].clone(),
            "encryption": plan["encryption"].clone(),
            "config_update_status": "not_requested",
            "old_archive_retained": true,
            "assets": assets,
            "raw_mirror_manifest_count": receipts.iter().filter(|receipt| {
                receipt["relative_path"].as_str().is_some_and(|path| path.contains("raw-mirror") && path.ends_with(".json"))
            }).count(),
            "backup_manifest_count": receipts.iter().filter(|receipt| {
                receipt["relative_path"].as_str().is_some_and(|path| path.contains("backups") && path.ends_with(".json"))
            }).count(),
            "receipts": receipts,
        });
        doctor_write_private_json_artifact(&manifest_path, &manifest, "archive export manifest")
            .map_err(|message| CliError {
                code: 14,
                kind: "io",
                message,
                hint: Some("Retry after checking target filesystem health.".to_string()),
                retryable: true,
            })?;
        verify_status = doctor_archive_export_verify_target(target_root, &data_dir);
        let event_log = doctor_archive_export_event_log(
            &plan_fingerprint,
            request.workflow,
            request.command_mode,
            receipts.len(),
            started.elapsed().as_millis() as u64,
            verify_status["status"].as_str().unwrap_or("unknown"),
        );
        let event_path = doctor_archive_export_event_log_path(target_root);
        doctor_write_private_json_artifact(&event_path, &event_log, "archive export event log")
            .map_err(|message| CliError {
                code: 14,
                kind: "io",
                message,
                hint: Some("Retry after checking target filesystem health.".to_string()),
                retryable: true,
            })?;
        let receipt_path = doctor_archive_export_receipt_path(target_root);
        doctor_write_private_json_artifact(
            &receipt_path,
            &serde_json::json!({
                "schema_version": 1,
                "receipt_kind": "cass_doctor_archive_export_receipt_v1",
                "workflow": request.workflow.stable_name(),
                "status": verify_status["status"].clone(),
                "plan_fingerprint": plan_fingerprint,
                "target_root": target_root.display().to_string(),
                "copied_bytes": copied_bytes,
                "old_archive_retained": true,
                "config_update_status": "not_requested",
                "asset_receipts": receipts,
                "verify_status": verify_status,
            }),
            "archive export receipt",
        )
        .map_err(|message| CliError {
            code: 14,
            kind: "io",
            message,
            hint: Some("Retry after checking target filesystem health.".to_string()),
            retryable: true,
        })?;
        sync_directory(target_root).map_err(|message| CliError {
            code: 14,
            kind: "io",
            message,
            hint: Some("Retry after checking target filesystem health.".to_string()),
            retryable: true,
        })?;
        event_log_path = serde_json::json!(event_path.display().to_string());
        if blocked_reasons.is_empty() && verify_status["status"].as_str() == Some("verified") {
            "applied"
        } else {
            "blocked"
        }
    };

    let event_log = doctor_archive_export_event_log(
        &plan_fingerprint,
        request.workflow,
        request.command_mode,
        assets.iter().filter(|asset| asset.included).count(),
        started.elapsed().as_millis() as u64,
        status,
    );
    let payload = serde_json::json!({
        "schema_version": 1,
        "surface": "archive-export",
        "mode": request.command_mode.stable_name(),
        "workflow": request.workflow.stable_name(),
        "status": status,
        "relocation_plan_id": plan_fingerprint,
        "plan_fingerprint": plan_fingerprint,
        "source_roots": plan["source_roots"].clone(),
        "redacted_source_roots": plan["redacted_source_roots"].clone(),
        "target_root": target_root.display().to_string(),
        "redacted_target_root": doctor_redacted_path(&target_root.display().to_string(), &data_dir),
        "required_bytes": required_bytes,
        "target_available_bytes": target_available_bytes,
        "copied_bytes": copied_bytes,
        "verified_asset_classes": plan["verified_asset_classes"].clone(),
        "skipped_asset_classes": plan["skipped_asset_classes"].clone(),
        "privacy_mode": plan["privacy_mode"].clone(),
        "compression": plan["compression"].clone(),
        "encryption": plan["encryption"].clone(),
        "config_update_status": "not_requested",
        "old_archive_retained": true,
        "will_delete_old_archive": false,
        "receipts": receipts,
        "event_log": event_log,
        "event_log_path": event_log_path,
        "verify_status": verify_status,
        "blocked_reasons": blocked_reasons,
        "archive_export_plan": plan,
        "recommended_action": if status == "planned" {
            "Review the plan, ensure the target is trusted, then rerun with --yes --plan-fingerprint. Do not delete the old archive."
        } else if status == "applied" {
            "Export verified. Keep the old archive retained as evidence; switch cass data paths only after an operator review."
        } else {
            "Archive export did not verify. Inspect blocked_reasons and retain the old archive."
        },
        "doctor_command": {
            "surface": "archive-export",
            "execution_mode": request.command_mode.stable_name(),
            "read_only": request.command_mode != DoctorArchiveExportCommandMode::Apply,
            "mutation_allowed": request.command_mode == DoctorArchiveExportCommandMode::Apply,
            "archive_mutation_allowed": false,
        },
        "_meta": {
            "elapsed_ms": started.elapsed().as_millis() as u64,
            "data_dir": data_dir.display().to_string(),
            "db_path": db_path.display().to_string(),
        },
    });
    if let Some(fmt) = structured_format {
        output_structured_value(payload, fmt)?;
    } else {
        print_doctor_human_line(
            wrap,
            format!(
                "Doctor archive {}: {status}; old archive retained, target {}",
                request.workflow.action_name(),
                target_root.display()
            ),
        );
        println!("This is preservation, not cleanup. Do not delete the old archive.");
    }
    Ok(())
}
