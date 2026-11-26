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

/// One-shot first stop for agents: never starts repair/indexing, but returns
/// exact next commands and discovery pointers for the current dataset.
pub(crate) fn triage_budget_ms(timeout_ms: Option<u64>) -> u64 {
    timeout_ms.unwrap_or_else(|| {
        dotenvy::var("CASS_TRIAGE_BUDGET_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|&milliseconds| milliseconds > 0)
            .unwrap_or(8_000)
    })
}

/// Deterministic slowdown for the bounded triage E2E. This is inert unless the
/// explicit test-only environment variable is present.
pub(crate) fn maybe_test_triage_delay() {
    if let Ok(raw) = dotenvy::var("CASS_TEST_TRIAGE_SLOW_MS")
        && let Ok(milliseconds) = raw.parse::<u64>()
        && milliseconds > 0
    {
        std::thread::sleep(Duration::from_millis(milliseconds));
    }
}

pub(crate) const TRIAGE_RESPONSE_RESERVE_MS: u64 = 25;

pub(crate) fn triage_remaining_operation_timeout(
    budget: &crate::robot_budget_envelope::RobotBudget,
) -> Option<Duration> {
    let milliseconds = budget
        .remaining_ms()
        .saturating_sub(TRIAGE_RESPONSE_RESERVE_MS);
    (milliseconds > 0).then(|| Duration::from_millis(milliseconds))
}

#[derive(Clone, Copy)]
pub(crate) struct TriageCoreReadiness {
    pub(crate) db_exists: bool,
    pub(crate) lexical_exists: bool,
}

pub(crate) enum TriageReadinessWorkerMessage {
    Core(TriageCoreReadiness),
    Complete(serde_json::Value),
}

pub(crate) fn triage_budget_fallback_state(
    data_dir: &Path,
    db_path: &Path,
    core: Option<TriageCoreReadiness>,
) -> serde_json::Value {
    let db_exists = core
        .map(|facts| serde_json::Value::Bool(facts.db_exists))
        .unwrap_or(serde_json::Value::Null);
    let lexical_exists = core
        .map(|facts| serde_json::Value::Bool(facts.lexical_exists))
        .unwrap_or(serde_json::Value::Null);
    serde_json::json!({
        "index": {
            "exists": lexical_exists,
            "status": "not_inspected",
            "inspected": false,
            "reason": "Detailed index readiness was skipped because the triage execution budget was exhausted."
        },
        "database": {
            "exists": db_exists,
            "inspected": false,
            "open_error": "Database readiness probe was skipped because the triage execution budget was exhausted.",
            "counts_skipped": true,
            "open_skipped": true,
        },
        "pending": { "inspected": false },
        "rebuild": { "inspected": false },
        "semantic": {
            "status": "not_inspected",
            "inspected": false,
            "summary": "Semantic readiness was skipped because the triage execution budget was exhausted.",
            "hint": "Retry the recommended bounded triage probe to recover semantic readiness.",
        },
        "ingest_quarantine": {
            "status": "not_inspected",
            "inspected": false,
        },
        "_meta": {
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "data_dir": data_dir.display().to_string(),
            "db_path": db_path.display().to_string(),
        }
    })
}

pub(crate) fn triage_readiness_state_with_hard_timeout(
    data_dir: &Path,
    db_path: &Path,
    stale_threshold: u64,
    budget: &crate::robot_budget_envelope::RobotBudget,
) -> (serde_json::Value, bool) {
    let Some(timeout) = triage_remaining_operation_timeout(budget) else {
        return (triage_budget_fallback_state(data_dir, db_path, None), true);
    };

    let worker_data_dir = data_dir.to_path_buf();
    let worker_db_path = db_path.to_path_buf();
    let (tx, rx) = std::sync::mpsc::channel();
    let Ok(_state_worker) = std::thread::Builder::new()
        .name("cass-triage-readiness-probe".to_string())
        .spawn(move || {
            maybe_test_triage_worker_delay("CASS_TEST_TRIAGE_CORE_SLOW_MS");
            let core = TriageCoreReadiness {
                db_exists: worker_db_path.is_file(),
                lexical_exists: cass_lexical_index_initialized(&worker_data_dir),
            };
            if tx.send(TriageReadinessWorkerMessage::Core(core)).is_err() {
                return;
            }
            maybe_test_triage_delay();
            maybe_test_triage_worker_delay("CASS_TEST_TRIAGE_STATE_DB_SLOW_MS");
            let state =
                state_meta_json_for_status(&worker_data_dir, &worker_db_path, stale_threshold);
            let _ = tx.send(TriageReadinessWorkerMessage::Complete(state));
        })
    else {
        return (triage_budget_fallback_state(data_dir, db_path, None), true);
    };

    let wait_start = std::time::Instant::now();
    match rx.recv_timeout(timeout) {
        Ok(TriageReadinessWorkerMessage::Complete(state)) => (state, false),
        Ok(TriageReadinessWorkerMessage::Core(core)) => {
            let remaining = timeout.saturating_sub(wait_start.elapsed());
            if remaining.is_zero() {
                (
                    triage_budget_fallback_state(data_dir, db_path, Some(core)),
                    true,
                )
            } else {
                match rx.recv_timeout(remaining) {
                    Ok(TriageReadinessWorkerMessage::Complete(state)) => (state, false),
                    Ok(TriageReadinessWorkerMessage::Core(_))
                    | Err(
                        std::sync::mpsc::RecvTimeoutError::Timeout
                        | std::sync::mpsc::RecvTimeoutError::Disconnected,
                    ) => (
                        triage_budget_fallback_state(data_dir, db_path, Some(core)),
                        true,
                    ),
                }
            }
        }
        Err(
            std::sync::mpsc::RecvTimeoutError::Timeout
            | std::sync::mpsc::RecvTimeoutError::Disconnected,
        ) => (triage_budget_fallback_state(data_dir, db_path, None), true),
    }
}

pub(crate) fn triage_recovery_probe(
    data_dir: &Path,
    db_path: &Path,
    budget: &crate::robot_budget_envelope::RobotBudget,
    stale_threshold: u64,
) -> String {
    let retry_budget_ms = budget.total_ms().saturating_mul(4).max(60_000);
    cass_dataset_command(
        data_dir,
        db_path,
        &[
            "triage",
            "--json",
            "--stale-threshold",
            &stale_threshold.to_string(),
            "--timeout",
            &retry_budget_ms.to_string(),
        ],
    )
}

pub(crate) fn triage_uninspected_search_completeness(recovery_probe: &str) -> serde_json::Value {
    serde_json::json!({
        "inspected": false,
        "quarantine_status": "not_inspected",
        "quarantined_conversations": serde_json::Value::Null,
        "complete": serde_json::Value::Null,
        "can_search": serde_json::Value::Null,
        "coverage_suspect": serde_json::Value::Null,
        "impact": "Search completeness was not inspected before the triage budget expired.",
        "next_command": recovery_probe,
    })
}

pub(crate) fn triage_uninspected_root_cause(recovery_probe: &str) -> serde_json::Value {
    serde_json::json!({
        "schema_version": crate::root_cause_taxonomy::ROOT_CAUSE_TAXONOMY_VERSION,
        "inspected": false,
        "family": "unknown",
        "locus": "unknown",
        "confidence": "unknown",
        "evidence_refs": [],
        "summary": "No root cause was attributed because readiness probes did not complete.",
        "recommended_next_probe": recovery_probe,
    })
}

pub(crate) fn triage_recovery_recommended_commands(recovery_probe: &str) -> Vec<serde_json::Value> {
    vec![readiness_command_recommendation(
        ReadinessCommandRecommendation {
            id: "retry-bounded-triage",
            command: recovery_probe.to_string(),
            purpose: "Recover the readiness sections skipped by the bounded triage probe.",
            safety: "read-only",
            run_when: "Run when triage budget.skipped_sections is non-empty.",
            success_signal: "budget.skipped_sections=[]",
            parse_fields: &[
                "status",
                "readiness",
                "search_completeness",
                "root_cause",
                "budget",
            ],
            retry_after_ms: None,
        },
    )]
}

pub(crate) fn run_triage(
    data_dir_override: &Option<PathBuf>,
    db_override: Option<PathBuf>,
    output_format: Option<RobotFormat>,
    stale_threshold: u64,
    timeout_ms: Option<u64>,
) -> CliResult<()> {
    let data_dir = resolve_data_dir(data_dir_override, db_override.as_ref());
    let db_path = db_override.unwrap_or_else(|| data_dir.join("agent_search.db"));
    let triage_budget =
        crate::robot_budget_envelope::RobotBudget::new(triage_budget_ms(timeout_ms));
    let (mut state, readiness_state_skipped) = triage_readiness_state_with_hard_timeout(
        &data_dir,
        &db_path,
        stale_threshold,
        &triage_budget,
    );
    let mut hard_probe_timed_out = readiness_state_skipped;
    let mut skipped_sections = Vec::<String>::new();
    if readiness_state_skipped {
        skipped_sections.extend(
            [
                "readiness_state",
                "index_readiness",
                "database_readiness",
                "pending_readiness",
                "rebuild_readiness",
                "semantic_readiness",
                "ingest_quarantine_readiness",
            ]
            .into_iter()
            .map(str::to_string),
        );
    }
    if !readiness_state_skipped && state_database_counts_need_refresh(&state, &db_path) {
        match triage_remaining_operation_timeout(&triage_budget) {
            Some(timeout) => {
                if refresh_state_database_counts_with_hard_timeout(
                    &mut state, &db_path, "triage", timeout,
                ) == BoundedCountRefresh::TimedOut
                {
                    hard_probe_timed_out = true;
                    skipped_sections.push("database_counts".to_string());
                }
            }
            None => skipped_sections.push("database_counts".to_string()),
        }
    } else if readiness_state_skipped {
        skipped_sections.push("database_counts".to_string());
    }

    let index_exists = state
        .get("index")
        .and_then(|index| index.get("exists"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    // b4uax: legacy Tantivy generation — exists, but unreadable by the
    // current engine; the only fix is a full rebuild, not a refresh.
    let index_engine_incompatible = state
        .get("index")
        .and_then(|index| index.get("engine_incompatible"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let index_fresh = state
        .get("index")
        .and_then(|index| index.get("fresh"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let is_stale = state
        .get("index")
        .and_then(|index| index.get("stale"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(true);
    let rebuild_active = state
        .get("rebuild")
        .and_then(|rebuild| rebuild.get("active"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let rebuild_stalled = state
        .get("rebuild")
        .and_then(|rebuild| rebuild.get("stalled"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let db_exists = state
        .get("database")
        .and_then(|database| database.get("exists"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let db_opened = state
        .get("database")
        .and_then(|database| database.get("opened"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let db_open_retryable = state
        .get("database")
        .and_then(|database| database.get("open_retryable"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let pending_sessions = state
        .get("pending")
        .and_then(|pending| pending.get("sessions"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let index_empty_with_messages = state
        .get("index")
        .and_then(|index| index.get("empty_with_messages"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    // uojcg.3.3: triage previously omitted quarantine entirely; surface the same
    // cheap counts status/health use so its readiness block and search-completeness
    // verdict agree with the other surfaces.
    let quarantined_conversations = state
        .get("ingest_quarantine")
        .and_then(|q| q.get("quarantined_conversations"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let ingest_quarantine_critical = state
        .get("ingest_quarantine")
        .and_then(|q| q.get("circuit_breaker_active"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let recovery_probe =
        triage_recovery_probe(&data_dir, &db_path, &triage_budget, stale_threshold);
    let search_completeness = if readiness_state_skipped {
        triage_uninspected_search_completeness(&recovery_probe)
    } else {
        let mut completeness = serde_json::to_value(
            crate::search::quarantine_status::project_search_completeness(
                quarantined_conversations,
                ingest_quarantine_critical,
            ),
        )
        .unwrap_or(serde_json::Value::Null);
        if let Some(object) = completeness.as_object_mut() {
            object.insert("inspected".to_string(), serde_json::Value::Bool(true));
        }
        completeness
    };
    let db_available = db_opened || (db_exists && db_open_retryable);
    // Reuse the bounded readiness snapshot. A fresh filesystem probe here
    // would escape triage's hard deadline on a degraded or remote mount.
    let lexical_index_initialized = index_exists;
    let not_initialized =
        cass_not_initialized(db_exists, lexical_index_initialized, rebuild_active);
    let observed_healthy = db_exists
        && db_available
        && index_exists
        && index_fresh
        && !rebuild_active
        && !index_empty_with_messages;
    let observed_status = if rebuild_stalled {
        "stalled"
    } else if rebuild_active {
        "rebuilding"
    } else if observed_healthy {
        "healthy"
    } else if not_initialized {
        "not_initialized"
    } else if db_exists && !db_available {
        "degraded"
    } else {
        "unhealthy"
    };
    let observed_explanation = if not_initialized {
        Some(cass_not_initialized_explanation(&data_dir))
    } else {
        None
    };
    let observed_recommended_action = if rebuild_stalled {
        Some(
            "Index rebuild is wedged; capture diagnostics for a bug report and restart the watcher"
                .to_string(),
        )
    } else if rebuild_active {
        Some("Index rebuild is already in progress".to_string())
    } else if not_initialized {
        Some(cass_not_initialized_recommended_action())
    } else if !db_exists {
        Some("Run 'cass index --full' to create the database".to_string())
    } else if !db_available {
        Some("Run 'cass doctor check --json' before any repair; indexing will not replace an unreadable canonical database".to_string())
    } else if !index_exists {
        Some("Run 'cass index --full' to rebuild the search index".to_string())
    } else if index_engine_incompatible {
        Some(
            "Run 'cass index --full' to rebuild the search index for the current engine (a legacy Tantivy generation was found; one-time full lexical rebuild, cost scales with message count — `--force-rebuild` skips the filesystem rescan)"
                .to_string(),
        )
    } else if index_empty_with_messages {
        Some("Run 'cass index --full' to populate the empty search index".to_string())
    } else if is_stale || pending_sessions > 0 {
        let pending_msg = if pending_sessions > 0 {
            format!(" ({pending_sessions} sessions pending)")
        } else {
            String::new()
        };
        Some(format!(
            "Run 'cass index' to refresh the index{pending_msg}"
        ))
    } else {
        semantic_recommended_action(&state, not_initialized)
    };
    let observed_recommended_commands = readiness_recommended_commands(
        &data_dir,
        &db_path,
        &state,
        observed_status,
        observed_healthy,
        not_initialized,
        observed_recommended_action.as_deref(),
    );
    let observed_next_command = observed_recommended_commands
        .first()
        .and_then(|command| command.get("command"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    let mut observed_root_cause =
        serde_json::to_value(crate::root_cause_projection::project_root_cause(
            &gather_projection_signals_from_state(&state, None, None),
        ))
        .unwrap_or(serde_json::Value::Null);
    if let Some(object) = observed_root_cause.as_object_mut() {
        object.insert("inspected".to_string(), serde_json::Value::Bool(true));
    }
    let (
        status,
        healthy,
        initialized,
        explanation,
        recommended_action,
        recommended_commands,
        next_command,
        root_cause,
        rebuild_progress,
    ) = if readiness_state_skipped {
        (
            "partial",
            false,
            db_exists || lexical_index_initialized,
            Some(
                "Readiness probes did not complete within the triage execution budget.".to_string(),
            ),
            Some(recovery_probe.clone()),
            triage_recovery_recommended_commands(&recovery_probe),
            Some(recovery_probe.clone()),
            triage_uninspected_root_cause(&recovery_probe),
            serde_json::json!({ "inspected": false }),
        )
    } else {
        (
            observed_status,
            observed_healthy,
            !not_initialized,
            observed_explanation,
            observed_recommended_action,
            observed_recommended_commands,
            observed_next_command,
            observed_root_cause,
            rebuild_progress_summary_json(&state),
        )
    };

    let starter_workflows = if triage_budget.is_healthy() {
        build_workflow_capabilities()
    } else {
        skipped_sections.push("starter_workflows".to_string());
        Vec::new()
    };
    let mistake_recoveries = if triage_budget.is_healthy() {
        build_mistake_recovery_capabilities()
    } else {
        skipped_sections.push("mistake_recoveries".to_string());
        Vec::new()
    };
    let recommended_next_probe = if skipped_sections.is_empty() {
        None
    } else {
        Some(recovery_probe)
    };
    let mut budget = crate::robot_budget_envelope::BudgetBlock::from_budget(
        &triage_budget,
        skipped_sections,
        recommended_next_probe,
    );
    if hard_probe_timed_out {
        // The readiness/count worker reached its hard sub-deadline. Reserve
        // time is intentionally kept for emitting valid JSON, so wall-clock
        // sampling may be just below the outer cap; make the timeout explicit
        // and keep the budget snapshot internally consistent.
        budget.elapsed_ms = budget.elapsed_ms.max(budget.budget_ms);
        budget.timed_out = true;
    }

    let payload = serde_json::json!({
        "surface": "triage",
        "schema_version": 1,
        "status": status,
        "healthy": healthy,
        "initialized": initialized,
        "explanation": explanation,
        "recommended_action": recommended_action,
        "recommended_commands": recommended_commands,
        "next_command": next_command,
        "search_completeness": search_completeness,
        // uojcg.9.2/9.5: same conservative root-cause attribution as status
        // when inspected; an exhausted readiness probe is explicitly unknown.
        "root_cause": root_cause,
        "readiness": {
            "index": state.get("index").cloned().unwrap_or(serde_json::Value::Null),
            "database": state.get("database").cloned().unwrap_or(serde_json::Value::Null),
            "pending": state.get("pending").cloned().unwrap_or(serde_json::Value::Null),
            "rebuild": state.get("rebuild").cloned().unwrap_or(serde_json::Value::Null),
            "rebuild_progress": rebuild_progress,
            "semantic": state.get("semantic").cloned().unwrap_or(serde_json::Value::Null),
            "ingest_quarantine": state.get("ingest_quarantine").cloned().unwrap_or(serde_json::Value::Null),
        },
        "discovery": {
            "capabilities_command": "cass capabilities --json",
            "schemas_command": "cass introspect --json",
            "docs_command": "cass robot-docs guide",
            "api_version_command": "cass api-version --json",
        },
        "starter_workflows": starter_workflows,
        "mistake_recoveries": mistake_recoveries,
        "budget": budget,
        "_meta": state.get("_meta").cloned().unwrap_or(serde_json::Value::Null),
    });

    let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
        if matches!(fmt, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            fmt
        }
    });

    if let Some(fmt) = structured_format {
        return output_structured_value(payload, fmt);
    }

    println!("CASS agent triage: {status}");
    if let Some(action) = payload
        .get("recommended_action")
        .and_then(serde_json::Value::as_str)
    {
        println!("Recommended: {action}");
    }
    if let Some(command) = payload
        .get("next_command")
        .and_then(serde_json::Value::as_str)
    {
        println!("Next command: {command}");
    }
    if payload
        .pointer("/budget/skipped_sections")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|sections| !sections.is_empty())
    {
        println!("Budget-limited result: optional triage sections were skipped");
        if let Some(probe) = payload
            .pointer("/budget/recommended_next_probe")
            .and_then(serde_json::Value::as_str)
        {
            println!("Recovery probe: {probe}");
        }
    }
    println!("Machine output: cass triage --json");

    // Bead v6vuz: same bounded readiness summary as `status`, in triage's
    // agent-oriented vocabulary, so the human and `--json` triage agree.
    if !readiness_state_skipped {
        print_human_readiness_section(
            &state,
            crate::search::readiness_projection::SurfaceKind::Triage,
        );
    }

    Ok(())
}

/// Assemble and emit the redacted recovery/support evidence bundle (bead
/// `coding_agent_session_search-6f1lm`; the `.13.3` contract). Composes the
/// canonical readiness, next-command, source-doctor, quarantine, and
/// root-cause contracts — verbatim — into one share-safe
/// bundle via [`crate::recovery_support_bundle::build_support_bundle`], so the
/// bundle can never contradict the matching robot JSON.
///
/// Redaction is share-safe by default (raw payloads suppressed, content
/// fingerprints hashed, filesystem paths truncated to basenames). `--include-
/// full-paths` and `--include-raw-evidence` flip the [`RedactionPolicy`] and are
/// recorded in the bundle's `opt_in_flags`. `ExecutionMode::Live` marks the
/// provenance as a real on-host run (vs. the deterministic fixture mode the
/// goldens use).
pub(crate) fn run_support_bundle(
    data_dir_override: &Option<PathBuf>,
    db_override: Option<PathBuf>,
    output_format: Option<RobotFormat>,
    stale_threshold: u64,
    include_full_paths: bool,
    include_raw_evidence: bool,
) -> CliResult<()> {
    use crate::recovery_support_bundle as rsb;

    let data_dir = resolve_data_dir(data_dir_override, db_override.as_ref());
    let db_path = db_override.unwrap_or_else(|| data_dir.join("agent_search.db"));
    let data_dir_str = data_dir.display().to_string();

    // One live state snapshot (status-like: derive from the real DB-open
    // signal), reused for both the truth table and the root-cause signals so the
    // bundle is internally consistent and probes the data dir only once.
    let state = state_meta_json(&data_dir, &db_path, stale_threshold, true);
    let table = build_truth_table_from_state(&state);

    // Composed canonical contracts (verbatim from the same projections the robot
    // surfaces serialize).
    let readiness = crate::search::readiness_projection::project(
        &table,
        crate::search::readiness_projection::SurfaceKind::Triage,
    );
    let command_envelope = table.next_command_envelope(Some(&data_dir_str));
    let quarantine = table.quarantine.clone();
    let root_cause = crate::root_cause_projection::project_root_cause(
        &gather_projection_signals_from_state(&state, None, None),
    );

    // Local source provenance: a single read-only "local" observation derived
    // from the live truth table — this surface performs no remote probe, so it
    // only records local facts the bundle gathered.
    let source_provenance = rsb::SourceProvenance {
        source_id: "local".to_string(),
        cass_present: true,
        cass_current: true,
        archive_db_present: db_path.exists(),
        lexical_metadata_present: table.lexical_metadata.present,
    };

    let generated_at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| (d.as_millis().min(i64::MAX as u128)) as i64)
        .unwrap_or(0);

    // Share-safe default; the opt-in flags relax it and are recorded in the
    // bundle's redaction report.
    let mut policy = rsb::default_redaction_policy();
    if include_full_paths {
        policy.allow_full_paths = true;
    }
    if include_raw_evidence {
        policy.private_text = crate::search::incident_redaction::PrivateTextPolicy::RawOptIn;
    }

    let inputs = rsb::SupportBundleInputs {
        bundle_id: format!("support-bundle-{generated_at_ms}"),
        cass_version: env!("CARGO_PKG_VERSION").to_string(),
        // Single local host/source: identity fields are multi-source
        // concepts and are omitted (not guessed) for the local surface.
        host_id: None,
        source_id: None,
        data_dir: Some(data_dir_str),
        config_dir: None,
        model_dir: None,
        command_provenance: "cass support-bundle".to_string(),
        generated_at_ms,
        fixture_id: None,
        readiness,
        command_envelope,
        source_provenance,
        quarantine,
        root_cause,
        proof_logs: Vec::new(),
    };

    let bundle = rsb::build_support_bundle(inputs, policy, rsb::ExecutionMode::Live);

    let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
        if matches!(fmt, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            fmt
        }
    });

    if let Some(fmt) = structured_format {
        let payload = serde_json::to_value(&bundle).unwrap_or(serde_json::Value::Null);
        return output_structured_value(payload, fmt);
    }

    // Bounded, share-safety-first human summary. Reuses the v6vuz human
    // readiness projection for the readiness line so it matches the other
    // surfaces verbatim.
    let human = crate::search::human_readiness_summary::project_human_summary(
        &table,
        crate::search::readiness_projection::SurfaceKind::Status,
    );
    println!("CASS support bundle");
    println!();
    println!("  Bundle ID: {}", bundle.manifest.bundle_id);
    println!("  cass version: {}", bundle.manifest.cass_version);
    println!("  Mode: live");
    println!(
        "  Share-safe: {}",
        if bundle.is_share_safe() {
            "yes"
        } else {
            "no (opt-in evidence included)"
        }
    );
    println!(
        "  Manifest complete: {}",
        if bundle.manifest.is_complete() {
            "yes"
        } else {
            "no"
        }
    );
    println!();
    println!("{}", human.headline);
    println!("  Safest next step: {}", human.safest_next_action);
    println!();
    println!("Redaction posture:");
    if !bundle.redaction.fields_suppressed.is_empty() {
        println!(
            "  Suppressed: {}",
            bundle.redaction.fields_suppressed.join(", ")
        );
    }
    if !bundle.redaction.fields_truncated.is_empty() {
        println!(
            "  Truncated to basename: {}",
            bundle.redaction.fields_truncated.join(", ")
        );
    }
    if !bundle.redaction.opt_in_flags.is_empty() {
        println!(
            "  Opt-in available: {}",
            bundle.redaction.opt_in_flags.join(", ")
        );
    }
    println!();
    println!("Machine output: cass support-bundle --json");

    Ok(())
}

/// Archive-independent executable probe for package and binary-promotion gates.
///
/// This deliberately does not resolve a data directory or database override.
/// The probe exercises the linked SQLite engine against an in-memory
/// database, including a write and typed read, so a stale, corrupt, locked, or
/// multi-gigabyte archive cannot affect its cost or exit status (#398).
pub(crate) fn run_selftest(output_format: Option<RobotFormat>) -> CliResult<()> {
    let start = Instant::now();
    let database_probe = (|| -> std::result::Result<i64, crate::franken_sync::FrankenError> {
        let conn = crate::franken_sync::Connection::open(":memory:")?;
        conn.execute_batch(
            "CREATE TABLE cass_selftest_probe (value INTEGER NOT NULL); \
             INSERT INTO cass_selftest_probe (value) VALUES (42);",
        )?;
        conn.query_row("SELECT value FROM cass_selftest_probe LIMIT 1;")?
            .get_typed::<i64>(0)
    })();

    let (functional, check_status, detail, probe_error) = match database_probe {
        Ok(42) => (
            true,
            "pass",
            "opened an in-memory SQLite database and round-tripped a typed value".to_string(),
            None,
        ),
        Ok(value) => {
            let message = format!("in-memory database returned {value}, expected 42");
            (false, "fail", message.clone(), Some(message))
        }
        Err(error) => {
            let message = format!("in-memory SQLite probe failed: {error}");
            (false, "fail", message.clone(), Some(message))
        }
    };
    let latency_ms = start.elapsed().as_millis() as u64;

    let structured_format = output_format.or_else(robot_format_from_env).map(|format| {
        if matches!(format, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            format
        }
    });
    let structured_output = structured_format.is_some();
    if let Some(format) = structured_format {
        output_structured_value(
            serde_json::json!({
                "schema_version": 1,
                "status": if functional { "ok" } else { "error" },
                "functional": functional,
                "archive_accessed": false,
                "version": env!("CARGO_PKG_VERSION"),
                "contract_version": CONTRACT_VERSION,
                "latency_ms": latency_ms,
                "checks": [{
                    "name": "frankensqlite-in-memory-roundtrip",
                    "status": check_status,
                    "detail": detail,
                }],
            }),
            format,
        )?;
    } else if functional {
        println!("✓ cass executable self-test passed ({latency_ms}ms)");
        println!("  SQLite in-memory round-trip: pass");
        println!("  configured archive accessed: no");
    } else {
        println!("✗ cass executable self-test failed ({latency_ms}ms)");
        println!("  {detail}");
        println!("  configured archive accessed: no");
    }

    if functional {
        Ok(())
    } else {
        let error = CliError {
            code: 1,
            kind: CliErrorKind::Selftest.kind_str(),
            message: probe_error.unwrap_or_else(|| "executable self-test failed".to_string()),
            hint: Some(
                "Reinstall the cass binary from a checksum-verified release artifact.".to_string(),
            ),
            retryable: false,
        };
        if structured_output {
            Err(CliError::already_reported_from(&error))
        } else {
            Err(error)
        }
    }
}
