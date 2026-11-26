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

pub(crate) fn diagnostics_connector_paths(
    _home: &std::path::Path,
    _config_dir: &std::path::Path,
) -> Vec<(String, PathBuf)> {
    let opts = franken_agent_detection::AgentDetectOptions {
        include_undetected: true,
        ..Default::default()
    };
    let mut roots: Vec<(String, PathBuf)> =
        match franken_agent_detection::detect_installed_agents(&opts) {
            Ok(report) => report
                .installed_agents
                .into_iter()
                .flat_map(|entry| {
                    let slug = entry.slug;
                    entry
                        .root_paths
                        .into_iter()
                        .map(move |path| (slug.clone(), PathBuf::from(path)))
                })
                .collect(),
            Err(_) => Vec::new(),
        };
    // GH #448: the pinned registry probe ignores CLAUDE_CONFIG_DIR /
    // XDG_CONFIG_HOME, so doctor reported zero Claude Code roots for exactly
    // the redirected layouts the connector scans. Use the same env-aware
    // detection the indexer's connector factory uses.
    if !roots.iter().any(|(slug, _)| slug == "claude") {
        let detection =
            connectors::Connector::detect(&connectors::claude_code::ClaudeCodeConnector::new());
        if detection.detected {
            roots.extend(
                detection
                    .root_paths
                    .into_iter()
                    .map(|path| ("claude".to_string(), path)),
            );
        }
    }
    roots
}

pub(crate) fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} bytes")
    }
}

/// Truncate a string from the start, keeping the last `max_chars` characters.
/// UTF-8 safe. Adds "..." prefix if truncated.
pub(crate) fn truncate_start(s: &str, max_chars: usize) -> String {
    let char_count = s.chars().count();
    if char_count <= max_chars {
        s.to_string()
    } else if max_chars <= 3 {
        // Not enough room for any content plus "..."
        "...".to_string()
    } else {
        let skip = char_count.saturating_sub(max_chars.saturating_sub(3));
        format!("...{}", s.chars().skip(skip).collect::<String>())
    }
}

/// Truncate a string from the end, keeping the first `max_chars` characters.
/// UTF-8 safe. Adds "..." suffix if truncated.
pub(crate) fn truncate_end(s: &str, max_chars: usize) -> String {
    let char_count = s.chars().count();
    if char_count <= max_chars {
        s.to_string()
    } else if max_chars <= 3 {
        // Not enough room for any content plus "..."
        "...".to_string()
    } else {
        let take = max_chars.saturating_sub(3);
        format!("{}...", s.chars().take(take).collect::<String>())
    }
}

/// Quick health check for agents: index freshness, db stats, recommended action.
/// Designed to be fast (<100ms) for pre-search checks.
/// Bounded budget (ms) for `cass status` before it sheds optional/expensive
/// sections. Overridable via `CASS_STATUS_BUDGET_MS`; defaults to 8s (the report
/// observed status timing out under an 8s cap). Bead uojcg.2.2.
pub(crate) fn status_budget_ms() -> u64 {
    dotenvy::var("CASS_STATUS_BUDGET_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&ms| ms > 0)
        .unwrap_or(8000)
}

/// Test-only deterministic slowdown so E2E can trip the status budget. Honored
/// only when `CASS_TEST_STATUS_SLOW_MS` is set (operators never set it). Bead
/// uojcg.2.2.
pub(crate) fn maybe_test_status_delay() {
    if let Ok(raw) = dotenvy::var("CASS_TEST_STATUS_SLOW_MS")
        && let Ok(ms) = raw.parse::<u64>()
        && ms > 0
    {
        std::thread::sleep(Duration::from_millis(ms));
    }
}

/// Build a read-only [`ProjectionSignals`](crate::root_cause_projection::ProjectionSignals)
/// snapshot from the shared status/health `state` JSON so status/triage/doctor
/// can emit a [`RootCauseAttribution`](crate::root_cause_taxonomy::RootCauseAttribution)
/// in `--json` (bead cass-fleet-resilience-20260608-uojcg.9.2 / 9.5).
///
/// Conservative by construction: a signal is set only when the observed fact
/// clearly implicates that family, so the projection's false-positive
/// guardrails keep ambiguous or merely-degraded states `Unknown`. A *retryable*
/// open error, a permission/not-found open error, or semantic assets that are
/// simply absent-by-policy never drive an attribution. `host_disk_free_pct` is
/// supplied by callers that cheaply measure it (doctor); status/triage pass
/// `None` to keep the fast readiness surfaces probe-free.
pub(crate) fn gather_projection_signals_from_state(
    state: &serde_json::Value,
    host_disk_free_pct: Option<f64>,
    storage_integrity: Option<&serde_json::Value>,
) -> crate::root_cause_projection::ProjectionSignals {
    let mut signals = crate::root_cause_projection::ProjectionSignals {
        host_disk_free_pct,
        ..Default::default()
    };

    // SQLite storage: a non-retryable open failure whose error text names
    // a storage-engine fault (corruption / OpenRead / bad format) — not a
    // permission/not-found/busy condition, which are host/transient, not storage
    // corruption.
    let open_retryable = state
        .pointer("/database/open_retryable")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    if !open_retryable
        && let Some(err) = state
            .pointer("/database/open_error")
            .and_then(serde_json::Value::as_str)
    {
        let lower = err.to_ascii_lowercase();
        let storage_fault = lower.contains("openread")
            || lower.contains("malformed")
            || lower.contains("corrupt")
            || lower.contains("not a database")
            || lower.contains("disk image")
            || lower.contains("sqlite_");
        if storage_fault {
            signals.open_read_failure = true;
        }
    }

    // #374 Issue 3: the storage-integrity verdict is a first-class storage-fault
    // signal even when the failure is NOT surfaced as a `/database/open_error`
    // string — e.g. an archive that opened for the snapshot but fails the
    // dedicated OpenRead/integrity probe reports `storage_state: openread_failed`
    // / `archive_readability: unreadable` with an empty open_error, which
    // previously left `project_root_cause` unattributed. Consult the canonical
    // StorageState vocabulary so `archive-db-unreadable` attributes to the
    // frankensqlite-storage family. Busy/locked/derived-only/unchecked states are
    // deliberately NOT faults here (they are host/transient or non-canonical).
    if !signals.open_read_failure
        && let Some(integrity) = storage_integrity
    {
        let storage_state = integrity
            .get("storage_state")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let archive_readability = integrity
            .get("archive_readability")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let storage_fault = matches!(
            storage_state,
            "openread_failed"
                | "integrity_failed"
                | "legacy_interop_failed"
                | "fts_metadata_failed"
        ) || archive_readability == "unreadable";
        if storage_fault {
            signals.open_read_failure = true;
        }
    }

    // cass derived state: the lexical index is empty while the canonical DB has
    // messages — two cass-owned facts disagree. A cold/empty dir is not this.
    if state
        .pointer("/index/empty_with_messages")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        signals.derived_truth_table_mismatch = true;
    }

    // semantic assets: the model was present but failed to load. Absent /
    // not-initialized is opt-in policy, not a fault.
    if state
        .pointer("/semantic/availability")
        .and_then(serde_json::Value::as_str)
        == Some("load_failed")
    {
        signals.semantic_requested_but_missing = true;
    }

    signals
}

pub(crate) fn probe_daemon_runtime_for_diagnostics(
    data_dir: &std::path::Path,
) -> crate::daemon_runtime_state::DaemonRuntimeDiagnostic {
    #[cfg(unix)]
    {
        crate::daemon::client::probe_daemon_runtime(
            data_dir,
            crate::daemon::client::DEFAULT_RUNTIME_PROBE_TIMEOUT,
        )
    }

    #[cfg(not(unix))]
    {
        crate::daemon_runtime_state::DaemonRuntimeDiagnostic::from_observation(
            crate::daemon_runtime_state::DaemonRuntimeObservation {
                data_dir: Some(data_dir.display().to_string()),
                probe_incomplete: true,
                connect_error: Some(
                    "daemon runtime probing is unavailable on this platform".to_string(),
                ),
                ..Default::default()
            },
        )
    }
}

/// The status/health warning for conversations the watcher keeps deferring
/// after a bounded-guard NoMem (GH#510). They are not quarantined, but they are
/// missing from search until one ingests.
pub(crate) fn watch_nomem_deferral_warning(deferred_conversations: u64) -> String {
    format!(
        "{deferred_conversations} conversation(s) are deferred by the watcher after repeated bounded-allocation NoMem and are missing from search until they ingest; `cass quarantine list --json` lists them"
    )
}

pub(crate) fn run_status(
    data_dir_override: &Option<PathBuf>,
    db_override: Option<PathBuf>,
    output_format: Option<RobotFormat>,
    stale_threshold: u64,
    _robot_meta: bool,
) -> CliResult<()> {
    // k2k20 ask #1: status opens the canonical DB and was observed silent
    // for 13+ minutes on a 9.3GB corpus; share the doctor liveness heartbeat
    // (stderr only, first tick after one full interval).
    let _heartbeat = CliHeartbeat::start("status");
    let data_dir = resolve_data_dir(data_dir_override, db_override.as_ref());
    let db_path = db_override.unwrap_or_else(|| data_dir.join("agent_search.db"));
    // Bounded execution budget for the robot surface (uojcg.2.2): when the
    // optional/expensive sections would exceed it, status sheds them and returns
    // a parseable partial result (with timed_out + skipped_sections) instead of
    // blocking on a slow filesystem/coverage scan.
    let status_budget = crate::robot_budget_envelope::RobotBudget::new(status_budget_ms());
    let mut state = state_meta_json_for_status(&data_dir, &db_path, stale_threshold);
    refresh_state_database_counts_if_needed(&mut state, &db_path, "status");
    maybe_test_status_delay();

    let index_exists = state
        .get("index")
        .and_then(|i| i.get("exists"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    // b4uax: legacy Tantivy generation — exists, but unreadable by the
    // current engine; the only fix is a full rebuild, not a refresh.
    let index_engine_incompatible = state
        .get("index")
        .and_then(|i| i.get("engine_incompatible"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let index_fresh = state
        .get("index")
        .and_then(|i| i.get("fresh"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    // GH #457: served count contradicts the completed checkpoint.
    let index_hollow = state
        .get("index")
        .and_then(|i| i.get("hollow"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let index_age_secs = state
        .get("index")
        .and_then(|i| i.get("age_seconds"))
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let is_stale = state
        .get("index")
        .and_then(|i| i.get("stale"))
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let rebuild_active = state
        .get("rebuild")
        .and_then(|r| r.get("active"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let rebuild_stalled = state
        .get("rebuild")
        .and_then(|r| r.get("stalled"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let rebuild_processed = state
        .get("rebuild")
        .and_then(|r| r.get("processed_conversations"))
        .and_then(|v| v.as_u64());
    let rebuild_total = state
        .get("rebuild")
        .and_then(|r| r.get("total_conversations"))
        .and_then(|v| v.as_u64());
    let db_exists = state
        .get("database")
        .and_then(|d| d.get("exists"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let db_opened = state
        .get("database")
        .and_then(|d| d.get("opened"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let db_open_error = state
        .get("database")
        .and_then(|d| d.get("open_error"))
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let db_open_retryable = state
        .get("database")
        .and_then(|d| d.get("open_retryable"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let counts_skipped = state
        .get("database")
        .and_then(|d| d.get("counts_skipped"))
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let open_skipped = state
        .get("database")
        .and_then(|d| d.get("open_skipped"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let pending_sessions = state
        .get("pending")
        .and_then(|p| p.get("sessions"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let semantic_summary = state
        .get("semantic")
        .and_then(|s| s.get("summary"))
        .and_then(|v| v.as_str())
        .unwrap_or("Semantic asset status unknown");
    let semantic_status = state
        .get("semantic")
        .and_then(|s| s.get("status"))
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let semantic_hint = state
        .get("semantic")
        .and_then(|s| s.get("hint"))
        .and_then(|v| v.as_str());
    let index_empty_with_messages = state
        .get("index")
        .and_then(|i| i.get("empty_with_messages"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let quarantined_conversations = state
        .get("ingest_quarantine")
        .and_then(|q| q.get("quarantined_conversations"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let recent_quarantined_conversations = state
        .get("ingest_quarantine")
        .and_then(|q| q.get("recent_quarantined_conversations"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let ingest_quarantine_critical = state
        .get("ingest_quarantine")
        .and_then(|q| q.get("circuit_breaker_active"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let ingest_quarantine_recommended_action = state
        .get("ingest_quarantine")
        .and_then(|q| q.get("recommended_action"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    // i6upe: live bounded real-column probe verdict (milliseconds), the fast
    // complement to the persisted #331 deep-probe attestation below.
    let db_integrity = state
        .get("database")
        .and_then(|d| d.get("integrity"))
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    let db_integrity_hint = state
        .get("database")
        .and_then(|d| d.get("integrity_hint"))
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let live_integrity_corrupt = db_integrity == "corrupt";
    let mut warnings = Vec::<String>::new();
    if live_integrity_corrupt {
        warnings.push(db_integrity_hint.clone().unwrap_or_else(|| {
            "the canonical archive's stored pages are malformed (bounded real-column probe); do NOT run 'cass index' — run 'cass doctor --check' and recover first".to_string()
        }));
    }
    if ingest_quarantine_critical {
        warnings.push(format!(
            "{recent_quarantined_conversations} recent conversation quarantine(s) triggered the ingest quarantine circuit breaker; search coverage is suspect until the watcher/source path is inspected"
        ));
    } else if quarantined_conversations > 0 {
        warnings.push(format!(
            "{quarantined_conversations} conversation(s) are excluded from search after ingest quarantine; search remains usable for the rest of the archive, and `cass quarantine list --json` names them and says whether a retry can help"
        ));
    }
    let deferred_conversations = state
        .get("ingest_quarantine")
        .and_then(|q| q.get("deferred_conversations"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    if deferred_conversations > 0 {
        warnings.push(watch_nomem_deferral_warning(deferred_conversations));
    }

    let db_available = db_opened || (db_exists && db_open_retryable);
    let lexical_index_initialized = cass_lexical_index_initialized(&data_dir);
    let not_initialized =
        cass_not_initialized(db_exists, lexical_index_initialized, rebuild_active);
    // #331: a fingerprint-still-valid deep-probe attestation that proved
    // committed structural corruption must gate top-level `healthy` — a
    // known-bad canonical store cannot read as healthy just because it still
    // opens and the derived lexical index remains complete.
    let attested_integrity_failure = db_exists
        && crate::search::storage_integrity::load_matching_integrity_attestation(
            &data_dir, &db_path,
        )
        .is_some_and(|attestation| {
            attestation.verdict
                == crate::search::storage_integrity::IntegrityAttestationVerdict::Fail
        });
    if attested_integrity_failure {
        warnings.push(
            "a recent integrity probe proved committed structural corruption in the canonical archive (see storage_integrity); run 'cass doctor check --json' and plan a recovery before trusting new writes"
                .to_string(),
        );
    }
    let healthy = db_exists
        && db_available
        && index_exists
        && index_fresh
        && !rebuild_active
        && !index_empty_with_messages
        && !ingest_quarantine_critical
        && !attested_integrity_failure
        && !live_integrity_corrupt;
    // Stalled rebuilds are reported as a distinct status so operators
    // can tell a wedged indexer apart from a slow-but-progressing one
    // (issue #258). `stalled` implies `rebuild_active=true`, but it
    // *overrides* the "rebuilding" status because health is not
    // monotonically improving — the operator must intervene.
    let status = if rebuild_stalled {
        "stalled"
    } else if rebuild_active {
        "rebuilding"
    } else if ingest_quarantine_critical || live_integrity_corrupt {
        "unhealthy"
    } else if healthy {
        "healthy"
    } else if not_initialized {
        "not_initialized"
    } else if db_exists && !db_available {
        "degraded"
    } else {
        "unhealthy"
    };
    let explanation = if not_initialized {
        Some(cass_not_initialized_explanation(&data_dir))
    } else {
        None
    };

    let recommended_action = if rebuild_stalled {
        Some("Index rebuild is wedged; see `cass status --json | jq .rebuild` for the stall age and capture a stack trace with `sudo gdb -batch -ex 'thread apply all bt' -p $(pgrep -f 'cass index') 2>/dev/null | head -200` for a bug report".to_string())
    } else if rebuild_active {
        Some("Index rebuild is already in progress".to_string())
    } else if live_integrity_corrupt {
        // i6upe: staleness advice ('run cass index') is the WORST action on a
        // malformed archive — recovery must come first.
        Some("The canonical archive is malformed; do NOT run 'cass index'. Run 'cass doctor check --json', then recover the database (sqlite3 agent_search.db '.recover') before reindexing".to_string())
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
    } else if index_hollow {
        // GH #457: name the repair (the pre-scan sparse check rebuilds the
        // generation from the canonical DB) rather than the generic stale
        // advice, so the operator knows why the next run rebuilds.
        Some(HOLLOW_INDEX_RECOMMENDED_ACTION.to_string())
    } else if state_index_manifest_corrupt(&state) {
        Some(CORRUPT_MANIFEST_RECOMMENDED_ACTION.to_string())
    } else if index_empty_with_messages {
        Some("Run 'cass index --full' to populate the empty search index".to_string())
    } else if ingest_quarantine_critical {
        ingest_quarantine_recommended_action
    } else if is_stale || pending_sessions > 0 {
        let pending_msg = if pending_sessions > 0 {
            format!(" ({pending_sessions} sessions pending)")
        } else {
            String::new()
        };
        Some(format!(
            "Run 'cass index' to refresh the index{pending_msg}"
        ))
    } else if quarantined_conversations > 0 || deferred_conversations > 0 {
        ingest_quarantine_recommended_action
    } else {
        semantic_recommended_action(&state, not_initialized)
    };

    let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
        if matches!(fmt, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            fmt
        }
    });

    if let Some(fmt) = structured_format {
        let policy_registry = state
            .get("policy_registry")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let topology_budget =
            serde_json::to_value(crate::topology_budget::inspect_host_topology_budget())
                .unwrap_or(serde_json::Value::Null);
        let daemon_runtime = probe_daemon_runtime_for_diagnostics(&data_dir);

        // Status is a readiness surface, not a doctor run. Deep coverage checks
        // verify raw-mirror manifests and can hash very large archived blobs, and
        // the quarantine scan walks the filesystem; doing that here made
        // `cass status --json` CPU-bound on real archives. These optional sections
        // are now shed when the bounded budget is tripped (uojcg.2.2), so status
        // returns a parseable PARTIAL result with timed_out + skipped_sections
        // rather than blocking. The core readiness above is cheap and always
        // present; operators are routed to `cass doctor check` for the rest.
        let mut skipped_sections: Vec<String> = Vec::new();
        let (quarantine_value, coverage_value, doctor_summary_value) = if status_budget.is_healthy()
        {
            let quarantine_report = collect_diag_quarantine_report(
                &data_dir,
                &crate::search::tantivy::expected_index_dir(&data_dir),
            );
            let coverage_risk = doctor_fast_coverage_risk_unchecked(db_exists);
            let doctor_summary = build_doctor_runtime_summary(DoctorRuntimeSummaryInput {
                surface: "status-summary",
                state: &state,
                status,
                healthy,
                initialized: !not_initialized,
                db_exists,
                rebuild_active,
                coverage_risk: &coverage_risk,
                coverage_source: "status-fast-state",
                coverage_checked: false,
                quarantine_summary: Some(&quarantine_report.summary),
                recommended_action: recommended_action.as_ref(),
                data_dir: &data_dir,
            });
            (
                serde_json::to_value(&quarantine_report).unwrap_or(serde_json::Value::Null),
                serde_json::to_value(&coverage_risk).unwrap_or(serde_json::Value::Null),
                serde_json::to_value(&doctor_summary).unwrap_or(serde_json::Value::Null),
            )
        } else {
            for section in ["quarantine", "coverage_risk", "doctor_summary"] {
                skipped_sections.push(section.to_string());
            }
            (
                serde_json::Value::Null,
                serde_json::Value::Null,
                serde_json::Value::Null,
            )
        };
        let recommended_commands = readiness_recommended_commands(
            &data_dir,
            &db_path,
            &state,
            status,
            healthy,
            not_initialized,
            recommended_action.as_deref(),
        );
        let budget_timed_out = status_budget.is_exhausted();
        let recommended_next_probe = if skipped_sections.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::Value::String("cass doctor check --json".to_string())
        };
        let payload = serde_json::json!({
            "status": status,
            "healthy": healthy,
            "health_level": if ingest_quarantine_critical { "critical" } else if healthy && quarantined_conversations > 0 { "degraded" } else { status },
            "initialized": !not_initialized,
            "explanation": explanation,
            "warnings": warnings,
            "data_dir": data_dir.display().to_string(),
            "index": state.get("index").cloned().unwrap_or(serde_json::Value::Null),
            "database": serde_json::json!({
                "exists": db_exists,
                "opened": db_opened,
                "conversations": state.get("database").and_then(|d| d.get("conversations")).cloned().unwrap_or(serde_json::Value::Null),
                "messages": state.get("database").and_then(|d| d.get("messages")).cloned().unwrap_or(serde_json::Value::Null),
                "path": db_path.display().to_string(),
                // WS-B.4a: physical footprint from metadata (see state meta).
                "db_bytes": state.get("database").and_then(|d| d.get("db_bytes")).cloned().unwrap_or(serde_json::json!(0)),
                "wal_bytes": state.get("database").and_then(|d| d.get("wal_bytes")).cloned().unwrap_or(serde_json::json!(0)),
                "shm_present": state.get("database").and_then(|d| d.get("shm_present")).cloned().unwrap_or(serde_json::json!(false)),
                "open_error": db_open_error,
                "open_retryable": db_open_retryable,
                "counts_skipped": counts_skipped,
                "open_skipped": open_skipped,
                // i6upe: live bounded real-column probe verdict — the fast,
                // distinct corruption signal (never conflated with `stale`).
                "integrity": db_integrity,
                "integrity_detail": state.get("database").and_then(|d| d.get("integrity_detail")).cloned().unwrap_or(serde_json::Value::Null),
                "integrity_hint": db_integrity_hint,
            }),
            "pending": state.get("pending").cloned().unwrap_or(serde_json::Value::Null),
            "rebuild": state.get("rebuild").cloned().unwrap_or(serde_json::Value::Null),
            "rebuild_progress": rebuild_progress_summary_json(&state),
            "semantic": state.get("semantic").cloned().unwrap_or(serde_json::Value::Null),
            "ingest_quarantine": state.get("ingest_quarantine").cloned().unwrap_or(serde_json::Value::Null),
            "policy_registry": policy_registry,
            "topology_budget": topology_budget,
            "daemon_runtime": daemon_runtime,
            "doctor_summary": doctor_summary_value,
            "coverage_risk": coverage_value,
            "quarantine": quarantine_value,
            // uojcg.3.3: compact "is search missing known conversations?" verdict,
            // derived from the cheap ingest-quarantine counts above so health and
            // status agree. `quarantine` (above) is the deep, budget-shed scan.
            "search_completeness": serde_json::to_value(
                crate::search::quarantine_status::project_search_completeness(
                    quarantined_conversations,
                    ingest_quarantine_critical,
                ),
            )
            .unwrap_or(serde_json::Value::Null),
            // uojcg.9.2/9.5: likely root-cause family attributed from the live
            // state signals (storage open failure, derived truth-table mismatch,
            // semantic load failure). Conservative — Unknown when no signal
            // implicates a family. Status is probe-free, so no host-disk signal.
            "root_cause": serde_json::to_value(
                crate::root_cause_projection::project_root_cause(
                    &gather_projection_signals_from_state(&state, None, None),
                ),
            )
            .unwrap_or(serde_json::Value::Null),
            // qfswx: project the `.14.1` storage-integrity vocabulary doctor
            // emits onto status too, so every truth surface agrees. Probe-free:
            // derives ok / openread_failed / derived_only_drift /
            // unknown_deferred from the db-open + index-drift signals above.
            "storage_integrity": storage_integrity_value_from_state(&data_dir, &db_path, &state, not_initialized),
            "recommended_action": recommended_action,
            "recommended_commands": recommended_commands,
            "budget": serde_json::json!({
                "elapsed_ms": status_budget.elapsed_ms(),
                "budget_ms": status_budget.total_ms(),
                "timed_out": budget_timed_out,
                "skipped_sections": skipped_sections,
                "recommended_next_probe": recommended_next_probe,
            }),
            "_meta": state.get("_meta").cloned().unwrap_or(serde_json::Value::Null),
        });
        return output_structured_value(payload, fmt);
    }

    let status_icon = if healthy {
        "✓"
    } else if rebuild_active {
        "~"
    } else if not_initialized {
        "○"
    } else {
        "!"
    };
    let status_word = if rebuild_active {
        "Rebuilding"
    } else if healthy {
        "Healthy"
    } else if not_initialized {
        "Not initialized yet"
    } else {
        "Attention needed"
    };

    println!("{status_icon} CASS Status: {status_word}");
    println!();

    println!("Index:");
    if index_exists {
        if let Some(age) = index_age_secs.as_u64() {
            let age_str = if age < 60 {
                format!("{age} seconds ago")
            } else if age < 3600 {
                format!("{} minutes ago", age / 60)
            } else if age < 86400 {
                format!("{} hours ago", age / 3600)
            } else {
                format!("{} days ago", age / 86400)
            };
            let stale_indicator = if is_stale { " (stale)" } else { "" };
            println!("  Last indexed: {age_str}{stale_indicator}");
        } else {
            println!("  Last indexed: unknown");
        }
        if rebuild_active {
            match (rebuild_processed, rebuild_total) {
                (Some(processed), Some(total)) => {
                    println!("  Rebuild progress: {processed}/{total} conversations committed");
                }
                _ => println!("  Rebuild progress: in progress"),
            }
        }
        if let Some(scan) = state
            .get("rebuild")
            .and_then(|rebuild| rebuild.get("expected_docs_scan"))
            .filter(|scan| !scan.is_null())
        {
            println!(
                "  Coverage scan: counted through conversation id {} of {} ({} docs so far); the next index run resumes it if the archive only grew",
                scan["last_conversation_id"],
                scan["max_conversation_id"],
                scan["expected_docs_so_far"]
            );
        }
        if index_empty_with_messages {
            println!("  Warning: index has 0 documents but database has messages");
            println!("  Run 'cass index --full' to populate the search index");
        }
    } else if not_initialized {
        println!("  Not created yet - run 'cass index --full' once");
    } else {
        println!("  Not found - run 'cass index --full'");
    }

    println!();
    println!("Database:");
    if live_integrity_corrupt {
        println!("  Integrity: MALFORMED (bounded real-column probe)");
        if let Some(detail) = state
            .get("database")
            .and_then(|d| d.get("integrity_detail"))
            .and_then(|v| v.as_str())
        {
            println!("  Detail: {detail}");
        }
        println!(
            "  Do NOT run 'cass index' — run 'cass doctor check --json' and recover the database first"
        );
    }
    if db_exists {
        if db_opened {
            if open_skipped {
                println!("  Open skipped for fast status on large database");
            } else if counts_skipped {
                println!("  Counts skipped for fast status on large database");
            } else {
                if let Some(conversations) = state
                    .get("database")
                    .and_then(|d| d.get("conversations"))
                    .and_then(|v| v.as_i64())
                {
                    println!("  Conversations: {conversations}");
                }
                if let Some(messages) = state
                    .get("database")
                    .and_then(|d| d.get("messages"))
                    .and_then(|v| v.as_i64())
                {
                    println!("  Messages: {messages}");
                }
            }
        } else if db_open_retryable {
            println!("  Temporarily busy during probe");
            if let Some(err) = &db_open_error {
                println!("  Probe error: {err}");
            }
        } else {
            println!("  Exists, but could not be opened");
            if let Some(err) = &db_open_error {
                println!("  Error: {err}");
            }
        }
    } else if not_initialized {
        println!("  Not created yet");
    } else {
        println!("  Not found");
    }

    println!();
    println!("Semantic:");
    println!("  Status: {semantic_status}");
    println!("  Summary: {semantic_summary}");
    if let Some(hint) = semantic_hint {
        println!("  Hint: {hint}");
    }

    if pending_sessions > 0 {
        println!();
        println!("Pending: {pending_sessions} sessions awaiting indexing");
    }
    if quarantined_conversations > 0 {
        println!();
        println!(
            "Warning: {quarantined_conversations} conversation(s) quarantined after ingest OOM"
        );
    }
    if deferred_conversations > 0 {
        println!();
        println!(
            "Warning: {}",
            watch_nomem_deferral_warning(deferred_conversations)
        );
    }

    if let Some(explanation) = &explanation {
        println!();
        println!("Why: {explanation}");
    }

    if let Some(action) = &recommended_action {
        println!();
        println!("Recommended: {action}");
    }

    // Bead v6vuz: mirror the robot readiness vocabulary in the human surface so
    // the operator-facing summary never contradicts `cass status --json`.
    print_human_readiness_section(
        &state,
        crate::search::readiness_projection::SurfaceKind::Status,
    );

    Ok(())
}
