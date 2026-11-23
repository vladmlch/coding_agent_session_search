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

pub(crate) fn doctor_post_repair_probe_fault() -> Option<String> {
    dotenvy::var("CASS_TEST_DOCTOR_POST_REPAIR_PROBE_FAULT").ok()
}

#[derive(Clone, Copy)]
pub(crate) struct DoctorPostRepairProbeTarget<'a> {
    pub(crate) probe_id: &'a str,
    pub(crate) target_asset_class: DoctorAssetClass,
    pub(crate) target_path: &'a Path,
    pub(crate) data_dir: &'a Path,
}

pub(crate) struct DoctorPostRepairProbeOutcome {
    pub(crate) status: &'static str,
    pub(crate) duration_ms: u64,
    pub(crate) failure_reason: Option<String>,
    pub(crate) blocks_promotion: bool,
    pub(crate) blocks_cleanup: bool,
    pub(crate) rollback_confirmed: bool,
    pub(crate) steps: Vec<String>,
}

pub(crate) fn doctor_post_repair_probe_outcome(
    status: &'static str,
    duration_ms: u64,
    failure_reason: Option<String>,
    blocks_promotion: bool,
    blocks_cleanup: bool,
    rollback_confirmed: bool,
    steps: Vec<String>,
) -> DoctorPostRepairProbeOutcome {
    DoctorPostRepairProbeOutcome {
        status,
        duration_ms,
        failure_reason,
        blocks_promotion,
        blocks_cleanup,
        rollback_confirmed,
        steps,
    }
}

pub(crate) fn doctor_post_repair_probe_report(
    target: DoctorPostRepairProbeTarget<'_>,
    outcome: DoctorPostRepairProbeOutcome,
) -> DoctorPostRepairProbeReport {
    let target_path_text = target.target_path.display().to_string();
    DoctorPostRepairProbeReport {
        schema_version: 1,
        probe_id: target.probe_id.to_string(),
        target_asset_class: target.target_asset_class,
        target_path: target_path_text.clone(),
        redacted_target_path: doctor_redacted_path(&target_path_text, target.data_dir),
        generation_identity: None,
        manifest_identity: None,
        status: outcome.status.to_string(),
        duration_ms: outcome.duration_ms,
        failure_reason: outcome.failure_reason,
        blocks_promotion: outcome.blocks_promotion,
        blocks_cleanup: outcome.blocks_cleanup,
        receipt_correlation_id: "doctor_post_repair_probe_v1".to_string(),
        event_log_correlation_id: "doctor_post_repair_probe_v1".to_string(),
        failure_context_path: None,
        redacted_failure_context_path: None,
        rollback_confirmed: outcome.rollback_confirmed,
        steps: outcome.steps,
    }
}

pub(crate) fn doctor_post_repair_probe_duration_ms(start: Instant) -> u64 {
    start.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

pub(crate) fn doctor_probe_table_name(probe_id: &str, db_path: &Path) -> String {
    let hash = doctor_canonical_blake3(
        "doctor-post-repair-probe-table-v1",
        serde_json::json!({
            "probe_id": probe_id,
            "db_path": db_path.display().to_string(),
            "created_at_ms": doctor_now_ms(),
        }),
    );
    let suffix = hash
        .rsplit_once('-')
        .map(|(_, value)| value)
        .unwrap_or(&hash)
        .chars()
        .take(16)
        .collect::<String>();
    format!("cass_doctor_probe_{suffix}")
}

pub(crate) fn run_doctor_archive_db_post_repair_probe(
    data_dir: &Path,
    db_path: &Path,
) -> DoctorPostRepairProbeReport {
    use crate::franken_sync::compat::{ConnectionExt, RowExt};

    let probe_id = "archive-db-rollback-write-read";
    let target = DoctorPostRepairProbeTarget {
        probe_id,
        target_asset_class: DoctorAssetClass::CanonicalArchiveDb,
        target_path: db_path,
        data_dir,
    };
    let start = Instant::now();
    let mut steps = Vec::new();
    if !db_path.exists() {
        return doctor_post_repair_probe_report(
            target,
            doctor_post_repair_probe_outcome(
                "skipped",
                doctor_post_repair_probe_duration_ms(start),
                Some(
                    "archive DB does not exist, so rollback write/read probe was skipped"
                        .to_string(),
                ),
                false,
                false,
                false,
                vec!["db_missing_skip".to_string()],
            ),
        );
    }

    let fault = doctor_post_repair_probe_fault();
    let table_name = doctor_probe_table_name(probe_id, db_path);
    let sentinel_value = doctor_canonical_blake3(
        "doctor-post-repair-sentinel-v1",
        serde_json::json!({
            "probe_id": probe_id,
            "table_name": table_name,
            "db_path": db_path.display().to_string(),
        }),
    );

    let manager = match crate::storage::sqlite::FrankenConnectionManager::new(
        db_path,
        crate::storage::sqlite::ConnectionManagerConfig {
            reader_count: 1,
            max_writers: 1,
        },
    ) {
        Ok(manager) => {
            steps.push("open_production_connection_manager".to_string());
            manager
        }
        Err(err) => {
            return doctor_post_repair_probe_report(
                target,
                doctor_post_repair_probe_outcome(
                    "fail",
                    doctor_post_repair_probe_duration_ms(start),
                    Some(format!(
                        "failed to open archive DB through production frankensqlite manager: {err}"
                    )),
                    true,
                    true,
                    false,
                    steps,
                ),
            );
        }
    };

    let writer = match manager.writer() {
        Ok(writer) => {
            steps.push("acquire_production_writer".to_string());
            writer
        }
        Err(err) => {
            return doctor_post_repair_probe_report(
                target,
                doctor_post_repair_probe_outcome(
                    "fail",
                    doctor_post_repair_probe_duration_ms(start),
                    Some(format!("failed to acquire archive DB writer: {err}")),
                    true,
                    true,
                    false,
                    steps,
                ),
            );
        }
    };
    let conn = writer.storage().raw();
    if let Err(err) = conn.execute("BEGIN;") {
        return doctor_post_repair_probe_report(
            target,
            doctor_post_repair_probe_outcome(
                "fail",
                doctor_post_repair_probe_duration_ms(start),
                Some(format!("failed to start rollback-only transaction: {err}")),
                true,
                true,
                false,
                steps,
            ),
        );
    }
    steps.push("begin_rollback_only_transaction".to_string());

    let create_sql = format!(
        "CREATE TABLE {table_name} (
            probe_id TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );"
    );
    if let Err(err) = conn.execute(&create_sql) {
        let _ = conn.execute("ROLLBACK;");
        return doctor_post_repair_probe_report(
            target,
            doctor_post_repair_probe_outcome(
                "fail",
                doctor_post_repair_probe_duration_ms(start),
                Some(format!("failed to create rollback-only probe table: {err}")),
                true,
                true,
                false,
                steps,
            ),
        );
    }
    steps.push("create_doctor_owned_probe_table".to_string());

    let insert_sql = format!("INSERT INTO {table_name} (probe_id, value) VALUES (?1, ?2)");
    if let Err(err) = conn.execute_compat(
        &insert_sql,
        crate::franken_sync::params![probe_id, sentinel_value.as_str()],
    ) {
        let _ = conn.execute("ROLLBACK;");
        return doctor_post_repair_probe_report(
            target,
            doctor_post_repair_probe_outcome(
                "fail",
                doctor_post_repair_probe_duration_ms(start),
                Some(format!(
                    "failed to write rollback-only probe sentinel: {err}"
                )),
                true,
                true,
                false,
                steps,
            ),
        );
    }
    steps.push("write_probe_sentinel".to_string());

    let select_sql = format!("SELECT value FROM {table_name} WHERE probe_id = ?1");
    let mut read_back = match conn.query_row_map(
        &select_sql,
        crate::franken_sync::params![probe_id],
        |row: &crate::franken_sync::Row| row.get_typed::<String>(0),
    ) {
        Ok(value) => {
            steps.push("read_probe_sentinel".to_string());
            value
        }
        Err(err) => {
            let _ = conn.execute("ROLLBACK;");
            return doctor_post_repair_probe_report(
                target,
                doctor_post_repair_probe_outcome(
                    "fail",
                    doctor_post_repair_probe_duration_ms(start),
                    Some(format!(
                        "failed to read rollback-only probe sentinel: {err}"
                    )),
                    true,
                    true,
                    false,
                    steps,
                ),
            );
        }
    };
    if fault.as_deref() == Some("archive_db_read_mismatch") {
        read_back.push_str("-forced-mismatch");
    }
    if read_back != sentinel_value {
        let _ = conn.execute("ROLLBACK;");
        return doctor_post_repair_probe_report(
            target,
            doctor_post_repair_probe_outcome(
                "fail",
                doctor_post_repair_probe_duration_ms(start),
                Some(
                    "archive DB read-after-write probe returned a mismatched sentinel".to_string(),
                ),
                true,
                true,
                false,
                steps,
            ),
        );
    }

    if fault.as_deref() == Some("archive_db_rollback_failure") {
        let _ = conn.execute("ROLLBACK;");
        return doctor_post_repair_probe_report(
            target,
            doctor_post_repair_probe_outcome(
                "fail",
                doctor_post_repair_probe_duration_ms(start),
                Some("forced rollback failure for post-repair probe coverage".to_string()),
                true,
                true,
                false,
                steps,
            ),
        );
    }
    if let Err(err) = conn.execute("ROLLBACK;") {
        return doctor_post_repair_probe_report(
            target,
            doctor_post_repair_probe_outcome(
                "fail",
                doctor_post_repair_probe_duration_ms(start),
                Some(format!("failed to rollback probe transaction: {err}")),
                true,
                true,
                false,
                steps,
            ),
        );
    }
    let rollback_confirmed = true;
    steps.push("rollback_probe_transaction".to_string());
    drop(writer);
    drop(manager);

    let conn = match open_franken_cli_read_db(
        db_path.to_path_buf(),
        "doctor post-repair probe",
        Duration::from_secs(5),
    ) {
        Ok(conn) => conn,
        Err(err) => {
            return doctor_post_repair_probe_report(
                target,
                doctor_post_repair_probe_outcome(
                    "fail",
                    doctor_post_repair_probe_duration_ms(start),
                    Some(format!(
                        "failed to reopen archive DB after rollback probe: {}",
                        err.message
                    )),
                    true,
                    true,
                    rollback_confirmed,
                    steps,
                ),
            );
        }
    };
    let durable_table_count: i64 = conn
        .query_row_map(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
            crate::franken_sync::params![table_name.as_str()],
            |row: &crate::franken_sync::Row| row.get_typed(0),
        )
        .unwrap_or(1);
    let _ = close_franken_cli_read_db(conn, db_path, "doctor post-repair probe");
    steps.push("verify_probe_table_absent_after_rollback".to_string());
    if durable_table_count != 0 {
        return doctor_post_repair_probe_report(
            target,
            doctor_post_repair_probe_outcome(
                "fail",
                doctor_post_repair_probe_duration_ms(start),
                Some("rollback-only probe left a durable scratch table behind".to_string()),
                true,
                true,
                rollback_confirmed,
                steps,
            ),
        );
    }

    doctor_post_repair_probe_report(
        target,
        doctor_post_repair_probe_outcome(
            "pass",
            doctor_post_repair_probe_duration_ms(start),
            None,
            true,
            true,
            rollback_confirmed,
            steps,
        ),
    )
}

pub(crate) fn run_doctor_lexical_post_repair_probe(
    data_dir: &Path,
    index_path: &Path,
) -> DoctorPostRepairProbeReport {
    let probe_id = "derived-lexical-open-query";
    let target = DoctorPostRepairProbeTarget {
        probe_id,
        target_asset_class: DoctorAssetClass::DerivedLexicalIndex,
        target_path: index_path,
        data_dir,
    };
    let start = Instant::now();
    let mut steps = Vec::new();
    if !crate::search::tantivy::searchable_index_exists(index_path) {
        return doctor_post_repair_probe_report(
            target,
            doctor_post_repair_probe_outcome(
                "skipped",
                doctor_post_repair_probe_duration_ms(start),
                Some(
                    "lexical index does not exist, so derived lexical probe was skipped"
                        .to_string(),
                ),
                false,
                false,
                false,
                vec!["lexical_index_missing_skip".to_string()],
            ),
        );
    }
    if doctor_post_repair_probe_fault().as_deref() == Some("derived_index_open_failure") {
        return doctor_post_repair_probe_report(
            target,
            doctor_post_repair_probe_outcome(
                "fail",
                doctor_post_repair_probe_duration_ms(start),
                Some(
                    "forced derived lexical open failure for post-repair probe coverage"
                        .to_string(),
                ),
                true,
                true,
                false,
                steps,
            ),
        );
    }
    steps.push("detect_searchable_index".to_string());
    if let Err(err) = crate::search::tantivy::validate_searchable_index_contract(index_path) {
        return doctor_post_repair_probe_report(
            target,
            doctor_post_repair_probe_outcome(
                "fail",
                doctor_post_repair_probe_duration_ms(start),
                Some(format!(
                    "failed to open lexical index through search reader: {err}"
                )),
                true,
                true,
                false,
                steps,
            ),
        );
    }
    steps.push("open_search_reader".to_string());
    match crate::search::tantivy::searchable_index_summary(index_path) {
        Ok(summary) => {
            steps.push("read_generation_summary".to_string());
            let mut report = doctor_post_repair_probe_report(
                target,
                doctor_post_repair_probe_outcome(
                    "pass",
                    doctor_post_repair_probe_duration_ms(start),
                    None,
                    true,
                    true,
                    false,
                    steps,
                ),
            );
            report.generation_identity = summary
                .map(|summary| format!("docs={},segments={}", summary.docs, summary.segments));
            report
        }
        Err(err) => doctor_post_repair_probe_report(
            target,
            doctor_post_repair_probe_outcome(
                "fail",
                doctor_post_repair_probe_duration_ms(start),
                Some(format!(
                    "failed to summarize lexical index generation: {err}"
                )),
                true,
                true,
                false,
                steps,
            ),
        ),
    }
}

pub(crate) fn run_doctor_semantic_post_repair_probe(
    data_dir: &Path,
    db_path: &Path,
    readiness_snapshot: &serde_json::Value,
) -> DoctorPostRepairProbeReport {
    let probe_id = "derived-semantic-readiness";
    let start = Instant::now();
    let semantic = readiness_snapshot
        .get("semantic")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let can_search = semantic
        .get("can_search")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let vector_index_path = semantic
        .get("vector_index_path")
        .and_then(serde_json::Value::as_str)
        .map(PathBuf::from);
    let hnsw_path = semantic
        .get("hnsw_path")
        .and_then(serde_json::Value::as_str)
        .map(PathBuf::from);
    let target_path = vector_index_path.as_deref().unwrap_or(db_path);
    let target = DoctorPostRepairProbeTarget {
        probe_id,
        target_asset_class: DoctorAssetClass::DerivedSemanticIndex,
        target_path,
        data_dir,
    };
    if !can_search && vector_index_path.is_none() && hnsw_path.is_none() {
        return doctor_post_repair_probe_report(
            target,
            doctor_post_repair_probe_outcome(
                "skipped",
                doctor_post_repair_probe_duration_ms(start),
                Some("semantic assets are optional and not currently initialized".to_string()),
                false,
                false,
                false,
                vec!["semantic_not_initialized_skip".to_string()],
            ),
        );
    }
    let mut missing_paths = Vec::new();
    for path in [vector_index_path.as_deref(), hnsw_path.as_deref()]
        .into_iter()
        .flatten()
    {
        if !path.exists() {
            missing_paths.push(path.display().to_string());
        }
    }
    if can_search && !missing_paths.is_empty() {
        return doctor_post_repair_probe_report(
            target,
            doctor_post_repair_probe_outcome(
                "fail",
                doctor_post_repair_probe_duration_ms(start),
                Some(format!(
                    "semantic readiness claims searchable assets, but path(s) are missing: {}",
                    missing_paths.join(", ")
                )),
                true,
                false,
                false,
                vec!["inspect_semantic_readiness".to_string()],
            ),
        );
    }
    let mut report = doctor_post_repair_probe_report(
        target,
        doctor_post_repair_probe_outcome(
            "pass",
            doctor_post_repair_probe_duration_ms(start),
            None,
            true,
            false,
            false,
            vec!["inspect_semantic_readiness".to_string()],
        ),
    );
    report.generation_identity = semantic
        .get("status")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    report.manifest_identity = semantic
        .get("embedder_id")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    report
}

pub(crate) fn doctor_post_repair_probes_should_run(
    fix_can_mutate: bool,
    auto_fix_applied: bool,
    fs_mutation_receipts: &[DoctorFsMutationReceipt],
    cleanup_apply_result: Option<&DiagCleanupApplyResult>,
) -> bool {
    let fs_mutated = fs_mutation_receipts
        .iter()
        .any(|receipt| receipt.status == DoctorActionStatus::Applied);
    let cleanup_mutated = cleanup_apply_result.is_some_and(|result| {
        result.applied
            || result.pruned_asset_count > 0
            || result.actions.iter().any(|action| action.applied)
    });
    fix_can_mutate && (auto_fix_applied || fs_mutated || cleanup_mutated)
}

pub(crate) fn write_doctor_post_repair_probe_artifacts(
    data_dir: &Path,
    suite: &mut DoctorPostRepairProbeSuiteReport,
) {
    let probe_run_id = doctor_canonical_blake3(
        "doctor-post-repair-probe-run-v1",
        serde_json::json!({
            "created_at_ms": doctor_now_ms(),
            "probe_ids": suite.probes.iter().map(|probe| &probe.probe_id).collect::<Vec<_>>(),
            "status": suite.status,
        }),
    );
    let artifact_root = data_dir
        .join("doctor")
        .join("post-repair-probes")
        .join(&probe_run_id);
    let manifest_path = artifact_root.join("probes.json");
    if let Err(err) = std::fs::create_dir_all(&artifact_root) {
        suite.warnings.push(format!(
            "failed to create post-repair probe artifact directory: {err}"
        ));
        return;
    }

    for probe in &mut suite.probes {
        if probe.status == "fail" {
            let failure_path = artifact_root.join(format!("{}-failure.json", probe.probe_id));
            probe.failure_context_path = Some(failure_path.display().to_string());
            probe.redacted_failure_context_path = Some(doctor_redacted_path(
                &failure_path.display().to_string(),
                data_dir,
            ));
            let failure_value = doctor_post_repair_probe_failure_context(
                &probe_run_id,
                probe,
                data_dir,
                &manifest_path,
                &failure_path,
            );
            if let Err(err) = std::fs::write(
                &failure_path,
                serde_json::to_vec_pretty(&failure_value).unwrap_or_default(),
            ) {
                suite.warnings.push(format!(
                    "failed to write post-repair probe failure artifact for {}: {err}",
                    probe.probe_id
                ));
            }
        }
    }

    suite.probe_run_id = Some(probe_run_id);
    suite.artifact_root = Some(artifact_root.display().to_string());
    suite.redacted_artifact_root = Some(doctor_redacted_path(
        &artifact_root.display().to_string(),
        data_dir,
    ));
    suite.manifest_path = Some(manifest_path.display().to_string());
    suite.redacted_manifest_path = Some(doctor_redacted_path(
        &manifest_path.display().to_string(),
        data_dir,
    ));
    let manifest_value = serde_json::json!({
        "schema_version": 1,
        "manifest_kind": "cass_doctor_post_repair_probes_v1",
        "probe_run_id": suite.probe_run_id,
        "status": suite.status,
        "probe_count": suite.probe_count,
        "passed_count": suite.passed_count,
        "failed_count": suite.failed_count,
        "skipped_count": suite.skipped_count,
        "blocks_success": suite.blocks_success,
        "probes": suite.probes,
        "warnings": suite.warnings,
        "redaction_status": "local_probe_manifest_contains_exact_paths; robot metadata includes redacted path companions",
    });
    if let Err(err) = std::fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest_value).unwrap_or_default(),
    ) {
        suite
            .warnings
            .push(format!("failed to write post-repair probe manifest: {err}"));
        return;
    }
    if let Err(err) = sync_directory(&artifact_root) {
        suite.warnings.push(format!(
            "failed to sync post-repair probe artifact directory: {err}"
        ));
    }
}

pub(crate) fn doctor_post_repair_probe_failure_context(
    probe_run_id: &str,
    probe: &DoctorPostRepairProbeReport,
    data_dir: &Path,
    manifest_path: &Path,
    failure_path: &Path,
) -> serde_json::Value {
    let command_json = vec![
        "cass".to_string(),
        "doctor".to_string(),
        "check".to_string(),
        "--json".to_string(),
        "--data-dir".to_string(),
        "[cass-data]".to_string(),
    ];
    let shell_command = command_json
        .iter()
        .map(|arg| doctor_shell_quote_arg(arg))
        .collect::<Vec<_>>()
        .join(" ");
    let redacted_failure_reason = probe
        .failure_reason
        .as_ref()
        .map(|reason| doctor_redacted_text(reason, data_dir));
    let failure_reason_log_entries = redacted_failure_reason.iter().cloned().collect::<Vec<_>>();

    doctor_canonical_json_value(serde_json::json!({
        "schema_version": 1,
        "context_kind": "cass_doctor_post_repair_probe_failure_context",
        "probe_run_id": probe_run_id,
        "operation_id": probe_run_id,
        "failed_phase": "post_repair_probe",
        "failed_check": &probe.probe_id,
        "failure_reason": &redacted_failure_reason,
        "asset_class": doctor_asset_class_name(probe.target_asset_class),
        "authority_status": "post_repair_verification_failed",
        "blocked_reasons": [
            if probe.blocks_promotion { "blocks_promotion" } else { "promotion_not_blocked" },
            if probe.blocks_cleanup { "blocks_cleanup" } else { "cleanup_not_blocked" },
        ],
        "artifacts": {
            "artifact_manifest_path": doctor_redacted_path(&manifest_path.display().to_string(), data_dir),
            "failure_context_path": doctor_redacted_path(&failure_path.display().to_string(), data_dir),
        },
        "probe": {
            "probe_id": &probe.probe_id,
            "status": &probe.status,
            "duration_ms": probe.duration_ms,
            "failure_reason": &redacted_failure_reason,
            "blocks_promotion": probe.blocks_promotion,
            "blocks_cleanup": probe.blocks_cleanup,
            "rollback_confirmed": probe.rollback_confirmed,
            "redacted_target_path": &probe.redacted_target_path,
            "generation_identity": &probe.generation_identity,
            "manifest_identity": &probe.manifest_identity,
            "receipt_correlation_id": &probe.receipt_correlation_id,
            "event_log_correlation_id": &probe.event_log_correlation_id,
        },
        "captured_logs": [
            {
                "kind": "probe_steps",
                "entries": &probe.steps,
            },
            {
                "kind": "failure_reason",
                "entries": failure_reason_log_entries,
            },
        ],
        "repro": {
            "safety": "read-only-redacted-template",
            "mutates_live_archive": false,
            "requires_explicit_live_archive": true,
            "target": "[cass-data]",
            "command_json": command_json,
            "shell_command": shell_command,
            "notes": [
                "Default repro is read-only and targets the redacted data-dir placeholder.",
                "Copy the affected data directory to a scratch location before running any mutating doctor command.",
                "Do not replace [cass-data] with a live archive for --fix or cleanup --yes unless the operator explicitly accepts that risk."
            ],
        },
        "redaction_status": "redacted_by_default; exact local paths are omitted from the shareable failure context",
    }))
}

pub(crate) fn doctor_failure_context_report_failed(
    reason: impl Into<String>,
    data_dir: &Path,
) -> serde_json::Value {
    let reason = reason.into();
    serde_json::json!({
        "status": "failed",
        "context_kind": "cass_doctor_failure_context",
        "path": serde_json::Value::Null,
        "redacted_path": serde_json::Value::Null,
        "checksum_blake3": serde_json::Value::Null,
        "blocked_reason": doctor_redacted_text(&reason, data_dir),
        "redaction_status": "redacted",
    })
}

pub(crate) fn write_doctor_lock_contention_failure_context(
    data_dir: &Path,
    operation_id: &str,
    command_line_mode: &str,
    operation_state: &DoctorOperationStateReport,
    locks: &[DoctorLockDiagnosticReport],
    event_log: &DoctorEventLogMetadata,
) -> serde_json::Value {
    let root = data_dir.join("doctor").join("failures");
    if let Err(err) = doctor_forensic_create_private_dir_all(&root) {
        return doctor_failure_context_report_failed(
            format!("failed to create doctor failure context directory: {err}"),
            data_dir,
        );
    }
    if existing_path_has_symlink_below_root(&root, data_dir) {
        return doctor_failure_context_report_failed(
            format!(
                "refusing to write doctor failure context through symlinked directory {}",
                root.display()
            ),
            data_dir,
        );
    }

    let created_at_ms = doctor_now_ms();
    let base_context_id = doctor_forensic_bundle_id(operation_id, created_at_ms);
    let mut context_dir = root.join(&base_context_id);
    let mut context_id = base_context_id.clone();
    let mut allocated = false;
    for suffix in 0..100 {
        let candidate_id = if suffix == 0 {
            base_context_id.clone()
        } else {
            format!("{base_context_id}-{suffix}")
        };
        let candidate_dir = root.join(&candidate_id);
        match doctor_forensic_create_private_dir(&candidate_dir) {
            Ok(()) => {
                context_id = candidate_id;
                context_dir = candidate_dir;
                allocated = true;
                break;
            }
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(err) => {
                return doctor_failure_context_report_failed(
                    format!(
                        "failed to create doctor failure context directory {}: {err}",
                        candidate_dir.display()
                    ),
                    data_dir,
                );
            }
        }
    }
    if !allocated {
        return doctor_failure_context_report_failed(
            format!(
                "failed to allocate unique doctor failure context directory under {}",
                root.display()
            ),
            data_dir,
        );
    }

    let failure_path = context_dir.join("failure_context.json");
    let command_json = vec![
        "cass".to_string(),
        "doctor".to_string(),
        "check".to_string(),
        "--json".to_string(),
        "--data-dir".to_string(),
        "[cass-data]".to_string(),
    ];
    let shell_command = command_json
        .iter()
        .map(|arg| doctor_shell_quote_arg(arg))
        .collect::<Vec<_>>()
        .join(" ");
    let blocked_reason = operation_state
        .mutation_blocked_reason
        .as_deref()
        .unwrap_or("doctor mutation blocked by active repair lock");
    let redacted_reason = doctor_redacted_text(blocked_reason, data_dir);
    let redacted_locks = locks
        .iter()
        .map(|lock| {
            serde_json::json!({
                "schema_version": lock.schema_version,
                "lock_kind": doctor_serde_label(lock.lock_kind),
                "redacted_lock_path": &lock.redacted_lock_path,
                "active": lock.active,
                "owned_by_current_process": lock.owned_by_current_process,
                "owner_confidence": doctor_serde_label(lock.owner_confidence),
                "pid_metadata_status": &lock.pid_metadata_status,
                "pid_reuse_ambiguous": lock.pid_reuse_ambiguous,
                "age_ms": lock.age_ms,
                "last_heartbeat_age_ms": lock.last_heartbeat_age_ms,
                "stale_suspected": lock.stale_suspected,
                "wait_duration_ms": lock.wait_duration_ms,
                "retry_policy": &lock.retry_policy,
                "safe_to_wait": lock.safe_to_wait,
                "manual_delete_allowed": lock.manual_delete_allowed,
                "recommended_action": &lock.recommended_action,
                "evidence": &lock.evidence,
            })
        })
        .collect::<Vec<_>>();
    let recent_events = event_log
        .events
        .iter()
        .rev()
        .take(8)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .map(|event| serde_json::to_value(event).unwrap_or_else(|_| serde_json::json!({})))
        .collect::<Vec<_>>();
    let failure_value = doctor_canonical_json_value(serde_json::json!({
        "schema_version": 1,
        "context_kind": "cass_doctor_lock_contention_failure_context",
        "context_id": context_id,
        "operation_id": operation_id,
        "failed_phase": "mutation_lock_acquire",
        "failed_check": "operation_state",
        "reasons": [&redacted_reason],
        "failure_reason": redacted_reason,
        "asset_class": "operation_receipt",
        "authority_status": "mutation_lock_blocked",
        "blocked_reasons": ["active_doctor_repair", "lock_contention"],
        "command": {
            "command_line_mode": command_line_mode,
            "safe_followup": "cass doctor check --json --data-dir [cass-data]",
        },
        "artifacts": {
            "failure_context_path": doctor_redacted_path(&failure_path.display().to_string(), data_dir),
            "event_log_path": event_log
                .path
                .as_ref()
                .map(|path| doctor_redacted_path(path, data_dir))
                .unwrap_or_else(|| "embedded_operation_events".to_string()),
        },
        "active_locks": redacted_locks,
        "recent_events": recent_events,
        "captured_logs": [
            {
                "kind": "operation_state",
                "entries": [&redacted_reason],
            },
            {
                "kind": "operator_safety",
                "entries": [
                    "Wait and retry instead of deleting lock files manually.",
                    "The repro command is read-only and uses a redacted data-dir placeholder.",
                ],
            },
        ],
        "repro": {
            "safety": "read-only-redacted-template",
            "mutates_live_archive": false,
            "requires_explicit_live_archive": true,
            "target": "[cass-data]",
            "command_json": command_json,
            "shell_command": shell_command,
            "notes": [
                "Default repro is read-only and targets the redacted data-dir placeholder.",
                "Use a copied fixture or explicit test data directory for any follow-up mutation.",
                "Do not delete lock files manually; wait for the active doctor owner or inspect receipts.",
            ],
        },
        "redaction_status": "redacted_by_default; exact local paths are omitted from the shareable failure context",
    }));

    let encoded = match serde_json::to_vec_pretty(&failure_value) {
        Ok(encoded) => encoded,
        Err(err) => {
            return doctor_failure_context_report_failed(
                format!("failed to encode doctor failure context: {err}"),
                data_dir,
            );
        }
    };
    let write_result = (|| -> io::Result<()> {
        let mut file = doctor_forensic_create_private_file(&failure_path)?;
        file.write_all(&encoded)?;
        file.flush()?;
        file.sync_all()?;
        Ok(())
    })();
    if let Err(err) = write_result {
        return doctor_failure_context_report_failed(
            format!(
                "failed to write doctor failure context {}: {err}",
                failure_path.display()
            ),
            data_dir,
        );
    }
    if let Err(err) = sync_directory(&context_dir) {
        return doctor_failure_context_report_failed(
            format!(
                "failed to sync doctor failure context directory {}: {err}",
                context_dir.display()
            ),
            data_dir,
        );
    }
    if let Err(err) = sync_directory(&root) {
        return doctor_failure_context_report_failed(
            format!(
                "failed to sync doctor failure context root {}: {err}",
                root.display()
            ),
            data_dir,
        );
    }

    serde_json::json!({
        "status": "captured",
        "context_kind": "cass_doctor_lock_contention_failure_context",
        "path": failure_path.display().to_string(),
        "redacted_path": doctor_redacted_path(&failure_path.display().to_string(), data_dir),
        "checksum_blake3": file_blake3_hex(&failure_path).ok(),
        "blocked_reason": serde_json::Value::Null,
        "redaction_status": "redacted",
    })
}

pub(crate) struct DoctorPostRepairProbeRunContext<'a> {
    pub(crate) data_dir: &'a Path,
    pub(crate) db_path: &'a Path,
    pub(crate) index_path: &'a Path,
    pub(crate) readiness_snapshot: &'a serde_json::Value,
    pub(crate) fix_can_mutate: bool,
    pub(crate) auto_fix_applied: bool,
    pub(crate) fs_mutation_receipts: &'a [DoctorFsMutationReceipt],
    pub(crate) cleanup_apply_result: Option<&'a DiagCleanupApplyResult>,
}

pub(crate) fn collect_doctor_post_repair_probes(
    context: DoctorPostRepairProbeRunContext<'_>,
) -> DoctorPostRepairProbeSuiteReport {
    let requested = doctor_post_repair_probes_should_run(
        context.fix_can_mutate,
        context.auto_fix_applied,
        context.fs_mutation_receipts,
        context.cleanup_apply_result,
    );
    if !requested {
        let archive_target = DoctorPostRepairProbeTarget {
            probe_id: "archive-db-rollback-write-read",
            target_asset_class: DoctorAssetClass::CanonicalArchiveDb,
            target_path: context.db_path,
            data_dir: context.data_dir,
        };
        let lexical_target = DoctorPostRepairProbeTarget {
            probe_id: "derived-lexical-open-query",
            target_asset_class: DoctorAssetClass::DerivedLexicalIndex,
            target_path: context.index_path,
            data_dir: context.data_dir,
        };
        let semantic_target = DoctorPostRepairProbeTarget {
            probe_id: "derived-semantic-readiness",
            target_asset_class: DoctorAssetClass::DerivedSemanticIndex,
            target_path: context.db_path,
            data_dir: context.data_dir,
        };
        return DoctorPostRepairProbeSuiteReport {
            schema_version: 1,
            requested: false,
            status: "skipped".to_string(),
            skipped_count: 3,
            probes: vec![
                doctor_post_repair_probe_report(
                    archive_target,
                    doctor_post_repair_probe_outcome(
                        "skipped",
                        0,
                        Some("no mutating repair was applied".to_string()),
                        false,
                        false,
                        false,
                        vec!["not_requested".to_string()],
                    ),
                ),
                doctor_post_repair_probe_report(
                    lexical_target,
                    doctor_post_repair_probe_outcome(
                        "skipped",
                        0,
                        Some("no mutating repair was applied".to_string()),
                        false,
                        false,
                        false,
                        vec!["not_requested".to_string()],
                    ),
                ),
                doctor_post_repair_probe_report(
                    semantic_target,
                    doctor_post_repair_probe_outcome(
                        "skipped",
                        0,
                        Some("no mutating repair was applied".to_string()),
                        false,
                        false,
                        false,
                        vec!["not_requested".to_string()],
                    ),
                ),
            ],
            ..DoctorPostRepairProbeSuiteReport::default()
        };
    }

    let mut probes = vec![
        run_doctor_archive_db_post_repair_probe(context.data_dir, context.db_path),
        run_doctor_lexical_post_repair_probe(context.data_dir, context.index_path),
        run_doctor_semantic_post_repair_probe(
            context.data_dir,
            context.db_path,
            context.readiness_snapshot,
        ),
    ];
    let probe_count = probes.len();
    let passed_count = probes.iter().filter(|probe| probe.status == "pass").count();
    let failed_count = probes.iter().filter(|probe| probe.status == "fail").count();
    let skipped_count = probes
        .iter()
        .filter(|probe| probe.status == "skipped")
        .count();
    let blocks_success = probes
        .iter()
        .any(|probe| probe.status == "fail" && (probe.blocks_promotion || probe.blocks_cleanup));
    let status = if blocks_success {
        "fail"
    } else if failed_count > 0 {
        "warn"
    } else {
        "pass"
    };
    probes.sort_by(|left, right| left.probe_id.cmp(&right.probe_id));
    let mut suite = DoctorPostRepairProbeSuiteReport {
        schema_version: 1,
        requested: true,
        status: status.to_string(),
        probe_count,
        passed_count,
        failed_count,
        skipped_count,
        blocks_success,
        probes,
        ..DoctorPostRepairProbeSuiteReport::default()
    };
    write_doctor_post_repair_probe_artifacts(context.data_dir, &mut suite);
    suite
}

pub(crate) fn doctor_check_operation_id(
    checks: &[DoctorCheckReport],
    fix_requested: bool,
    operation_outcome: &DoctorOperationOutcomeReport,
    not_initialized: bool,
) -> String {
    doctor_canonical_blake3(
        "doctor-operation-v1",
        serde_json::json!({
            "schema_version": 1,
            "mode": if fix_requested {
                DoctorRepairMode::RepairApply
            } else {
                DoctorRepairMode::Check
            },
            "fix_requested": fix_requested,
            "not_initialized": not_initialized,
            "operation_outcome_kind": operation_outcome.kind,
            "checks": checks
                .iter()
                .map(|check| serde_json::json!({
                    "name": check.name,
                    "status": check.status,
                    "anomaly_class": check.anomaly_class,
                    "health_class": check.health_class,
                    "affected_asset_class": check.affected_asset_class,
                    "fix_available": check.fix_available,
                    "fix_applied": check.fix_applied,
                }))
                .collect::<Vec<_>>(),
        }),
    )
}

pub(crate) fn cleanup_apply_outcome_kind(
    result: &DiagCleanupApplyResult,
) -> DoctorRepairOutcomeKind {
    if !result.blocked_reasons.is_empty() && result.pruned_asset_count == 0 {
        DoctorRepairOutcomeKind::Blocked
    } else if result.pruned_asset_count > 0
        && result.skipped_asset_count == 0
        && result.blocked_reasons.is_empty()
    {
        DoctorRepairOutcomeKind::Applied
    } else if result.pruned_asset_count > 0
        || result.skipped_asset_count > 0
        || !result.blocked_reasons.is_empty()
    {
        DoctorRepairOutcomeKind::Partial
    } else {
        DoctorRepairOutcomeKind::NoOp
    }
}

pub(crate) fn cleanup_apply_retry_safety(
    result: &DiagCleanupApplyResult,
) -> DoctorRepairRetrySafety {
    match result.outcome_kind {
        DoctorRepairOutcomeKind::Applied | DoctorRepairOutcomeKind::NoOp => {
            DoctorRepairRetrySafety::SafeToRetry
        }
        DoctorRepairOutcomeKind::Partial => DoctorRepairRetrySafety::RetryAfterSameDryRun,
        DoctorRepairOutcomeKind::Blocked => DoctorRepairRetrySafety::RetryAfterInspection,
        DoctorRepairOutcomeKind::Failed | DoctorRepairOutcomeKind::Planned => {
            DoctorRepairRetrySafety::DoNotRetryWithoutReview
        }
    }
}

pub(crate) fn cleanup_apply_operation_outcome(
    result: &DiagCleanupApplyResult,
) -> DoctorOperationOutcomeReport {
    match result.outcome_kind {
        DoctorRepairOutcomeKind::Applied => doctor_operation_outcome_with_details(
            DoctorOperationOutcomeKind::Fixed,
            format!(
                "cleanup applied {} derived artifact action(s) and reclaimed {} byte(s)",
                result.pruned_asset_count, result.reclaimed_bytes
            ),
            "pruned only derived reclaimable cleanup targets".to_string(),
            "no planned safe cleanup action remained blocked".to_string(),
            DoctorDataLossRisk::None,
            Some("cass doctor --json".to_string()),
            Some("cleanup_apply.receipt.artifact_manifest".to_string()),
        ),
        DoctorRepairOutcomeKind::Partial => doctor_operation_outcome_with_details(
            DoctorOperationOutcomeKind::PartiallyFixed,
            format!(
                "cleanup applied {} action(s) but left {} skipped/blocking condition(s)",
                result.pruned_asset_count,
                result.skipped_asset_count + result.blocked_reasons.len()
            ),
            "applied the cleanup actions that passed safety gates".to_string(),
            "left skipped or blocked cleanup actions for review".to_string(),
            DoctorDataLossRisk::Medium,
            Some("cass diag --json --quarantine".to_string()),
            Some("cleanup_apply.receipt.artifact_manifest".to_string()),
        ),
        DoctorRepairOutcomeKind::Blocked => doctor_operation_outcome_with_details(
            DoctorOperationOutcomeKind::CleanupRefused,
            if result.blocked_reasons.is_empty() {
                "cleanup was refused by a safety gate".to_string()
            } else {
                format!("cleanup was refused: {}", result.blocked_reasons.join("; "))
            },
            "reported cleanup refusal without pruning".to_string(),
            "no cleanup target was pruned".to_string(),
            DoctorDataLossRisk::Medium,
            Some("cass diag --json --quarantine".to_string()),
            Some("cleanup_apply.receipt.artifact_manifest".to_string()),
        ),
        DoctorRepairOutcomeKind::Failed | DoctorRepairOutcomeKind::Planned => {
            doctor_operation_outcome_with_details(
                DoctorOperationOutcomeKind::RepairIncomplete,
                "cleanup did not produce a verified final outcome".to_string(),
                "preserved available cleanup plan and receipt evidence".to_string(),
                "doctor did not claim cleanup success".to_string(),
                DoctorDataLossRisk::Medium,
                Some("cass doctor --json".to_string()),
                Some("cleanup_apply.receipt.artifact_manifest".to_string()),
            )
        }
        DoctorRepairOutcomeKind::NoOp => doctor_operation_outcome_with_details(
            DoctorOperationOutcomeKind::OkNoActionNeeded,
            "cleanup found no reclaimable derived artifacts".to_string(),
            "completed cleanup eligibility check".to_string(),
            "nothing was pruned because no safe cleanup target was eligible".to_string(),
            DoctorDataLossRisk::None,
            Some("cass doctor --json".to_string()),
            Some("cleanup_apply.receipt.artifact_manifest".to_string()),
        ),
    }
}

pub(crate) fn finalize_cleanup_apply_contract(
    result: &mut DiagCleanupApplyResult,
    data_dir: &Path,
    db_path: &Path,
    index_path: &Path,
) {
    result.mode = DoctorRepairMode::CleanupApply;
    result.approval_requirement = DoctorApprovalRequirement::ApprovalFingerprint;
    result.outcome_kind = cleanup_apply_outcome_kind(result);
    result.retry_safety = cleanup_apply_retry_safety(result);
    result.operation_outcome = cleanup_apply_operation_outcome(result);
    if result.planned_actions.is_empty() {
        result.planned_actions = result.actions.clone();
    }
    if result.operation_finished_at_ms.is_none() {
        result.operation_finished_at_ms = Some(doctor_now_ms());
    }
    let plan = build_cleanup_doctor_plan(result, data_dir, db_path, index_path);
    let actual_actions = doctor_sorted_cleanup_actions(&result.actions)
        .iter()
        .map(|action| {
            doctor_action_from_cleanup_action(
                action,
                doctor_cleanup_action_status(action),
                data_dir,
                db_path,
                index_path,
                &result.approval_fingerprint,
            )
        })
        .collect::<Vec<_>>();
    let artifact_checksums = actual_actions
        .iter()
        .flat_map(|action| action.artifacts.clone())
        .collect::<Vec<_>>();
    let artifact_manifest = doctor_artifact_manifest(artifact_checksums.clone());
    let action_status_counts = doctor_action_status_counts(&actual_actions);
    let coverage_after = doctor_cleanup_coverage_snapshot(
        result.after_generation_count,
        result.after_reclaim_candidate_count,
        result.after_reclaimable_bytes,
        result.after_retained_bytes,
        &result.actions,
    );
    let duration_ms = match (
        result.operation_started_at_ms,
        result.operation_finished_at_ms,
    ) {
        (Some(started), Some(finished)) => Some(finished.saturating_sub(started)),
        _ => None,
    };
    let verification_outcomes = actual_actions
        .iter()
        .map(|action| action.verification_outcome.clone())
        .collect::<Vec<_>>();
    let event_log = doctor_cleanup_event_log_for_actions(
        &plan.plan_fingerprint,
        &actual_actions,
        result.operation_started_at_ms,
        result.operation_finished_at_ms,
        Some("doctor_cleanup_apply_v1"),
        "embedded_receipt_events",
    );
    let remaining_risk = actual_actions
        .iter()
        .flat_map(|action| action.remaining_risk.clone())
        .chain(result.blocked_reasons.clone())
        .collect::<Vec<_>>();
    result.plan = Some(plan.clone());
    result.receipt = DoctorRepairReceipt {
        receipt_kind: "doctor_cleanup_apply_v1",
        schema_version: 1,
        mode: result.mode,
        outcome_kind: result.outcome_kind,
        approval_fingerprint: result.approval_fingerprint.clone(),
        plan_fingerprint: plan.plan_fingerprint.clone(),
        started_at_ms: result.operation_started_at_ms,
        finished_at_ms: result.operation_finished_at_ms,
        duration_ms,
        planned_action_count: result.planned_actions.len(),
        applied_action_count: actual_actions
            .iter()
            .filter(|action| action.status == DoctorActionStatus::Applied)
            .count(),
        skipped_action_count: actual_actions
            .iter()
            .filter(|action| action.status == DoctorActionStatus::Skipped)
            .count(),
        failed_action_count: *action_status_counts.get("failed").unwrap_or(&0),
        bytes_planned: result
            .planned_actions
            .iter()
            .map(|action| action.planned_reclaimable_bytes)
            .sum(),
        bytes_pruned: result.reclaimed_bytes,
        reclaimed_bytes: result.reclaimed_bytes,
        selected_authorities: plan.selected_authorities.clone(),
        rejected_authorities: plan.rejected_authorities.clone(),
        verification_outcomes,
        remaining_risk,
        event_log,
        forensic_bundle: plan.forensic_bundle.clone(),
        artifact_manifest: artifact_manifest.clone(),
        artifact_checksums,
        drift_detection_status: artifact_manifest.drift_detection_status,
        coverage_before: plan.coverage_before.clone(),
        coverage_after,
        actions: actual_actions,
        action_status_counts,
        blocked_reasons: result.blocked_reasons.clone(),
        ..DoctorRepairReceipt::default()
    };
}
