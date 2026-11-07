use crate::cli::args_sub::RobotFormat;
use crate::cli::commands::analytics::run_analytics::close_franken_cli_read_db_without_checkpoint;
use crate::cli::commands::models::run_models_command::resolve_semantic_index_embedder;
use crate::cli::commands::search::fieldbudgets::robot_format_from_env;
use crate::cli::commands::search::toon_encode_options_from_env::output_structured_value;
use crate::cli::commands::view::role_to_export_string::{
    index_result_counts_from_progress, index_worker_thread_builder,
    validate_successful_index_artifacts,
};
use crate::cli::contract_version::{
    index_entrypoint_diagnostics, resolve_watch_once_paths, with_frankensqlite_connection,
};
use crate::cli::error::ProgressResolved;
use crate::cli::error::{CliError, CliResult};
use crate::cli::output::cli_error_json_payload::cli_error_json_payload;
use crate::cli::paths::resolve_data_dir;
use crate::cli::state::probe_state_db_strict_worker::{
    ActiveIndexRunDetails, active_index_run_details, error_chain_indicates_active_cass_index,
    error_chain_indicates_archive_health_blocked,
    error_chain_indicates_index_disk_headroom_blocked,
    error_chain_indicates_lexical_rebuild_disk_stop,
    error_chain_indicates_orphan_fk_cleanup_blocked,
    error_chain_indicates_retryable_storage_contention,
    error_chain_indicates_unsupported_quill_writer_admission, index_archive_health_cli_error,
    index_disk_headroom_cli_error, index_orphan_fk_cleanup_cli_error,
    index_storage_contention_cli_error, index_unsupported_quill_writer_admission_cli_error,
    lexical_rebuild_disk_stop_cli_error,
};
use crate::franken_sync::compat::{ConnectionExt, RowExt};
use crate::index_result;
use crate::indexer;
use crate::indexer::IndexOptions;
use crate::indexer::stall_watchdog::{
    IndexShutdownSignals, IndexStallWatchdog, abort_after_index_stall_if_requested,
    index_stall_warning_lines,
};
use crate::model::cli_error_kind::ErrorKind as CliErrorKind;
use anyhow::Result;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tracing::info;

/// CLI wrapper for the qhiv2 / gh#382 targeted lexical reconcile: one
/// canonical conversation, upserted under stable identities, retry-safe.
pub(crate) fn run_lexical_reconcile_cli(
    db_override: Option<PathBuf>,
    data_dir_override: Option<PathBuf>,
    conversation_id: i64,
    output_format: Option<RobotFormat>,
) -> CliResult<()> {
    use colored::Colorize;

    let data_dir = resolve_data_dir(&data_dir_override, db_override.as_ref());
    let db_path = db_override.unwrap_or_else(|| data_dir.join("agent_search.db"));
    if !db_path.is_file() {
        return Err(CliError {
            code: 3,
            kind: CliErrorKind::MissingDb.kind_str(),
            message: format!("Database not found at {}.", db_path.display()),
            hint: Some("Run 'cass index --full' first.".into()),
            retryable: true,
        });
    }
    let report = crate::indexer::lexical_reconcile::run_lexical_conversation_reconcile(
        &data_dir,
        &db_path,
        conversation_id,
    )
    .map_err(|err| CliError {
        code: 5,
        kind: CliErrorKind::Storage.kind_str(),
        message: format!(
            "targeted lexical reconcile of conversation {conversation_id} failed: {err:#}"
        ),
        hint: Some(
            "The durable checkpoint (when written) is retained, so rerunning the same \
             command converges; if the canonical source changed, run 'cass index' first."
                .into(),
        ),
        retryable: true,
    })?;

    let structured_format = output_format.or_else(robot_format_from_env);
    if structured_format.is_some() {
        let mut payload = serde_json::to_value(&report).unwrap_or_else(|_| serde_json::json!({}));
        if let serde_json::Value::Object(ref mut map) = payload {
            map.insert("success".into(), serde_json::json!(true));
        }
        println!(
            "{}",
            serde_json::to_string_pretty(&payload).unwrap_or_default()
        );
        return Ok(());
    }

    println!(
        "{} conversation {} reconciled ({} docs upserted, attempt {})",
        "✓".green(),
        report.conversation_id,
        report.upserted_docs,
        report.attempt
    );
    println!(
        "  Live docs: {} -> {} (converged: {})",
        report.doc_count_before, report.doc_count_after, report.converged
    );
    println!(
        "  Canaries: early {:?}, late {:?}; checkpoint cleared: {}",
        report.early_canary_ok, report.late_canary_ok, report.checkpoint_cleared
    );
    Ok(())
}

/// `cass index --gc` (gh#453): run the lexical engine's garbage sweep on its
/// own and report what it reclaimed.
pub(crate) fn run_lexical_gc_cli(
    db_override: Option<PathBuf>,
    data_dir_override: Option<PathBuf>,
    output_format: Option<RobotFormat>,
) -> CliResult<()> {
    use colored::Colorize;

    let data_dir = resolve_data_dir(&data_dir_override, db_override.as_ref());
    let report = crate::indexer::run_lexical_segment_gc(&data_dir).map_err(|err| {
        let message = format!("lexical garbage sweep failed: {err:#}");
        let (kind, hint) = if message.contains("no published lexical index") {
            (
                CliErrorKind::MissingIndex,
                "Run 'cass index' to build the lexical index first.",
            )
        } else {
            (
                CliErrorKind::Index,
                "If another `cass index` is running, let it finish and retry; it performs the same sweep at open.",
            )
        };
        CliError {
            code: 5,
            kind: kind.kind_str(),
            message,
            hint: Some(hint.into()),
            retryable: true,
        }
    })?;

    let structured_format = output_format.or_else(robot_format_from_env);
    if structured_format.is_some() {
        let mut payload = serde_json::to_value(&report).unwrap_or_else(|_| serde_json::json!({}));
        if let serde_json::Value::Object(ref mut map) = payload {
            map.insert("success".into(), serde_json::json!(true));
        }
        println!(
            "{}",
            serde_json::to_string_pretty(&payload).unwrap_or_default()
        );
        return Ok(());
    }

    println!(
        "{} lexical garbage sweep at {}",
        "✓".green(),
        report.index_path.display()
    );
    println!(
        "  Reclaimed {} file(s), {} bytes; segment files {} -> {} ({} live)",
        report.reclaimed_files,
        report.reclaimed_bytes,
        report.segment_files_before,
        report.segment_files_after,
        report.live_segments
    );
    if report.retired_bytes_after > 0 {
        println!(
            "  {} bytes of retired segment files remain inside the engine's {}-second grace \
             period; rerun once that long has passed since the last `cass index` publish.",
            report.retired_bytes_after, report.grace_secs
        );
    }
    Ok(())
}

pub(crate) fn cached_index_payload(result_json: &str, key: &str) -> Option<serde_json::Value> {
    let mut payload =
        serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(result_json).ok()?;
    // Older versions could cache success despite nested connector failures.
    // Do not replay those receipts as a successful invocation after upgrading.
    if index_result::cached_scan_is_incomplete(&payload) {
        return None;
    }
    payload.insert("cached".to_string(), serde_json::Value::Bool(true));
    payload.insert(
        "idempotency_key".to_string(),
        serde_json::Value::String(key.to_string()),
    );
    Some(serde_json::Value::Object(payload))
}

pub(crate) fn read_index_idempotency_cache(
    db_path: &Path,
    key: &str,
    now_ms: i64,
) -> Option<(String, String)> {
    // Lookup precedes index admission. A missing or unavailable cache is a
    // miss: never create its table, expire rows, repair storage, or wait for
    // another archive owner before the indexer can report typed contention.
    let storage = crate::storage::sqlite::FrankenStorage::open_strict_readonly_with_timeout(
        db_path,
        Duration::ZERO,
    )
    .ok()?;
    let conn = storage.into_raw();
    let cached = conn
        .execute("PRAGMA busy_timeout = 0;")
        .and_then(|_| conn.query_row_map(
            "SELECT params_hash, result_json FROM idempotency_keys WHERE key = ?1 AND expires_at > ?2",
            crate::franken_sync::params![key, now_ms],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
        ))
        .ok();
    let _ = close_franken_cli_read_db_without_checkpoint(
        conn,
        db_path,
        "checking index idempotency cache",
    );
    cached
}

pub(crate) fn store_index_idempotency_result(
    data_dir: &Path,
    db_path: &Path,
    key: &str,
    params_hash: u64,
    payload: &serde_json::Value,
    now_ms: i64,
) -> Result<()> {
    use crate::franken_sync::compat::TransactionExt;

    let result_json = serde_json::to_string(payload)?;
    let hash_str = params_hash.to_string();
    let expires_ms = now_ms + 24 * 60 * 60 * 1000;
    // The indexing worker has already joined and released its ownership.
    // Reacquire both asset and archive admission for this short cache write;
    // a newly admitted daemon/indexer must keep its ownership undisturbed.
    let _mutation_guard = crate::indexer::acquire_search_maintenance_mutation_lock(
        data_dir,
        db_path,
        crate::search::asset_state::SearchMaintenanceJobKind::LexicalRefresh,
    )?;
    with_frankensqlite_connection(db_path, "storing index idempotency result", |conn| {
        let mut transaction = conn.transaction()?;
        transaction.execute(
            "CREATE TABLE IF NOT EXISTS idempotency_keys (
                key TEXT PRIMARY KEY,
                params_hash TEXT NOT NULL,
                result_json TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL
            )",
        )?;
        transaction.execute_compat(
            "DELETE FROM idempotency_keys WHERE expires_at <= ?1",
            crate::franken_sync::params![now_ms],
        )?;
        transaction.execute_compat(
            "INSERT OR REPLACE INTO idempotency_keys (key, params_hash, result_json, created_at, expires_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            crate::franken_sync::params![key, hash_str.as_str(), result_json.as_str(), now_ms, expires_ms],
        )?;
        transaction.commit()
    })?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_index_with_data(
    db_override: Option<PathBuf>,
    full: bool,
    force_rebuild: bool,
    watch: bool,
    watch_once: Option<Vec<PathBuf>>,
    watch_interval: u64,
    data_dir_override: Option<PathBuf>,
    semantic: bool,
    build_hnsw: bool,
    embedder: Option<String>,
    progress: ProgressResolved,
    output_format: Option<RobotFormat>,
    idempotency_key: Option<String>,
    progress_interval_ms: u64,
    no_progress_events: bool,
    robot_trace_ingest: bool,
    background: bool,
    mut captured_result: Option<&mut Option<serde_json::Value>>,
) -> CliResult<()> {
    use std::time::Instant;

    if background {
        let applied = crate::indexer::background_refresh::apply_background_priority();
        info!(
            nice = applied.nice,
            nice_applied = applied.nice_applied,
            ionice_class = applied.ionice_class,
            ionice_applied = applied.ionice_applied,
            "cass index --background: lowered process priority"
        );
    }

    let data_dir = resolve_data_dir(&data_dir_override, db_override.as_ref());
    let db_path = db_override.unwrap_or_else(|| data_dir.join("agent_search.db"));
    let embedder = resolve_semantic_index_embedder(embedder.as_deref());

    // GH #450: a background run never starts the engine's one-time migration
    // repair on a large archive; a foreground `cass index` performs it once.
    if background
        && let Some(bundle_bytes) =
            crate::indexer::background_migration_repair_pending_bytes(&db_path)
    {
        return Err(CliError {
            code: 7,
            kind: CliErrorKind::MigrationRepairPending.kind_str(),
            message: format!(
                "background index deferred: the {:.1} GiB archive at {} still needs its one-time storage migration repair, which rewrites the whole archive; background runs leave it to a foreground run",
                bundle_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
                db_path.display()
            ),
            hint: Some(
                "Run `cass index --full` in the foreground at a quiet time; it performs the repair once, preserves the original as a .pre-migration-bak copy (plan for that much free space), and later runs skip it"
                    .to_string(),
            ),
            retryable: false,
        });
    }

    let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
        if matches!(fmt, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            fmt
        }
    });
    let structured_output = structured_format.is_some();
    let capture_output = captured_result.is_some();
    let mut emit_result = |payload: serde_json::Value, format: RobotFormat| {
        if let Some(result) = captured_result.as_deref_mut() {
            *result = Some(payload);
            Ok(())
        } else {
            output_structured_value(payload, format)
        }
    };
    let indexing_exclusion_notice = active_indexing_exclusion_notice();
    let legacy_remote_notice = legacy_remote_source_notice();

    // Generate params hash for idempotency validation
    let params_hash = {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        full.hash(&mut hasher);
        force_rebuild.hash(&mut hasher);
        watch.hash(&mut hasher);
        semantic.hash(&mut hasher);
        build_hnsw.hash(&mut hasher);
        embedder.hash(&mut hasher);
        robot_trace_ingest.hash(&mut hasher);
        format!("{}", data_dir.display()).hash(&mut hasher);
        // Only a non-default profile enters the hash, so keys cached before
        // --rebuild-profile existed keep matching their default-profile runs.
        let rebuild_profile = indexer::lexical_rebuild_profile();
        if rebuild_profile != indexer::LexicalRebuildProfile::Default {
            rebuild_profile.as_str().hash(&mut hasher);
        }
        hasher.finish()
    };

    // Check for cached idempotency result
    if let Some(key) = &idempotency_key {
        let cached =
            read_index_idempotency_cache(&db_path, key, chrono::Utc::now().timestamp_millis());

        if let Some((stored_hash, result_json)) = cached {
            if stored_hash == params_hash.to_string() {
                // A syntactically valid scalar or array is not an index result.
                // Treat damaged cache entries as misses in every output mode.
                if let Some(val) = cached_index_payload(&result_json, key) {
                    if let Some(fmt) = structured_format {
                        emit_result(val, fmt)?;
                    } else {
                        eprintln!(
                            "Using cached result for idempotency key '{}' (use different key to force re-index)",
                            key
                        );
                    }
                    return Ok(());
                }
            } else {
                return Err(CliError {
                    code: 5,
                    kind: CliErrorKind::IdempotencyMismatch.kind_str(),
                    message: format!(
                        "Idempotency key '{}' was used with different parameters",
                        key
                    ),
                    hint: Some(
                        "Use a different idempotency key or wait for the existing one to expire (24h)"
                            .to_string(),
                    ),
                    retryable: false,
                });
            }
        }
    }

    let watch_once_paths = resolve_watch_once_paths(watch, watch_once);
    let entrypoint = index_entrypoint_diagnostics(
        full,
        force_rebuild,
        watch,
        watch_once_paths
            .as_ref()
            .map(std::vec::Vec::len)
            .unwrap_or_default(),
        semantic,
        build_hnsw,
    );

    // Decide whether to emit NDJSON progress events on stderr (structured output only).
    // Respect both the CLI flag and the CASS_INDEX_NO_PROGRESS_EVENTS env var.
    // Computed up here so the active-index pre-flight error path can also
    // participate in the event stream — callers polling stderr see a coherent
    // `started → error` sequence even for fail-fast outcomes.
    let env_disabled = dotenvy::var("CASS_INDEX_NO_PROGRESS_EVENTS")
        .map(|v| {
            let v = v.trim().to_ascii_lowercase();
            matches!(v.as_str(), "1" | "true" | "yes" | "on")
        })
        .unwrap_or(false);
    let emit_progress_events = structured_output && !no_progress_events && !env_disabled;
    // Clamp the user-supplied interval so callers can't DoS stderr or wait forever.
    let progress_interval = Duration::from_millis(progress_interval_ms.clamp(250, 60_000));

    // NDJSON event emitter. The event name is always embedded in payload["event"];
    // stdout is left untouched so pretty `--json` consumers keep working.
    let emit_event = |payload: serde_json::Value| {
        if let Ok(s) = serde_json::to_string(&payload) {
            eprintln!("{}", s);
        }
    };

    // #287: machine-readable liveness for robot callers. Healthy structured
    // runs still end with exactly one JSON document on stdout; but when the
    // stall watchdog fires, a robot caller that only reads stdout used to see
    // zero bytes until its external timeout killed the process. Emit the
    // stall/abort payload as an NDJSON event line on stdout too (flushed, in
    // case the caller kills us next) so the agent can distinguish "wedged
    // indexer" from "slow command" before its deadline.
    let emit_robot_stall_event_on_stdout = |payload: &serde_json::Value| {
        if !capture_output
            && structured_format.is_some()
            && let Ok(line) = serde_json::to_string(payload)
        {
            use std::io::Write as _;
            println!("{line}");
            let _ = std::io::stdout().flush();
        }
    };

    if let Some(notice) = &indexing_exclusion_notice {
        emit_indexing_exclusion_notice(notice, structured_output);
    }
    if let Some(names) = &legacy_remote_notice {
        emit_legacy_remote_source_notice(names);
    }

    if emit_progress_events {
        let pid = std::process::id();
        let started = serde_json::json!({
            "event": "started",
            "ts_ms": chrono::Utc::now().timestamp_millis(),
            "pid": pid,
            "mode": if full { "full" } else { "incremental" },
            "full": full,
            "force_rebuild": force_rebuild,
            "watch": watch,
            "entrypoint": entrypoint.kind,
            "semantic": semantic,
            "build_hnsw": build_hnsw,
            "embedder": embedder,
            "data_dir": data_dir.display().to_string(),
            "db_path": db_path.display().to_string(),
            "progress_interval_ms": progress_interval.as_millis() as u64,
            "hint": "Run `cass status --json` in another shell to inspect progress",
        });
        emit_event(started);
    }

    if let Some(active_index) = active_index_run_details(&data_dir, &db_path) {
        let err = active_index.to_cli_error();
        if let Some(fmt) = structured_format {
            let mut payload = cli_error_json_payload(&err, 0);
            payload["active_index"] = active_index.to_json();
            if emit_progress_events {
                let mut event = payload.clone();
                if let serde_json::Value::Object(ref mut m) = event {
                    m.insert("event".into(), serde_json::json!("error"));
                    m.insert(
                        "ts_ms".into(),
                        serde_json::json!(chrono::Utc::now().timestamp_millis()),
                    );
                }
                emit_event(event);
            }
            emit_result(payload, fmt)?;
            return Err(CliError::already_reported_from(&err));
        }
        return Err(err);
    }

    // Create progress tracker for real-time feedback
    let index_progress = std::sync::Arc::new(indexer::IndexingProgress::default());

    let opts = IndexOptions {
        full,
        force_rebuild,
        watch,
        watch_once_paths: watch_once_paths.clone(),
        db_path: db_path.clone(),
        data_dir: data_dir.clone(),
        semantic,
        build_hnsw,
        embedder: embedder.clone(),
        progress: Some(index_progress.clone()),
        watch_interval_secs: watch_interval,
    };

    // Set up progress display
    let show_progress = !structured_output && matches!(progress, ProgressResolved::Bars);
    let show_plain = !structured_output && matches!(progress, ProgressResolved::Plain);

    if show_plain {
        eprintln!(
            "index starting (full={}, watch={}, watch_once={})",
            full,
            watch,
            watch_once_paths
                .as_ref()
                .map(std::vec::Vec::len)
                .unwrap_or_default()
        );
    }

    let start = Instant::now();
    let mut progress_completion: Option<(indicatif::ProgressBar, usize, usize)> = None;

    // Run indexer in background thread so we can poll progress
    let mut shutdown_signals = IndexShutdownSignals::new(!watch).map_err(|error| CliError {
        code: 9,
        kind: CliErrorKind::Index.kind_str(),
        message: format!("failed to register indexing shutdown signals: {error}"),
        hint: None,
        retryable: true,
    })?;
    let opts_clone = opts.clone();
    let previous_robot_trace_ingest =
        robot_trace_ingest.then(|| indexer::set_robot_trace_ingest_enabled(true));
    let index_handle = index_worker_thread_builder()
        .spawn(move || indexer::run_cli_index(opts_clone))
        .map_err(|error| CliError {
            code: 9,
            kind: CliErrorKind::Index.kind_str(),
            message: format!("failed to start the index worker: {error}"),
            hint: Some("Retry after reducing process or memory pressure.".to_string()),
            retryable: true,
        })?;

    // Poll and display progress while indexer runs
    if show_progress {
        use indicatif::{ProgressBar, ProgressStyle};
        use std::sync::atomic::Ordering;

        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} {msg}")
                .unwrap_or_else(|_| ProgressStyle::default_spinner()),
        );
        // Set initial message BEFORE starting the tick
        pb.set_message(if full {
            "Starting full index...".to_string()
        } else {
            "Starting incremental index...".to_string()
        });
        pb.enable_steady_tick(Duration::from_millis(80));

        // Track last values to detect changes; use sentinel values to force first update
        let mut last_phase = usize::MAX;
        let mut last_current = usize::MAX;
        let mut last_agents = usize::MAX;
        let mut last_update = std::time::Instant::now();
        let mut stall_watchdog =
            IndexStallWatchdog::aborting_lexical_phases(data_dir.clone(), progress_interval)
                .watch_aware(watch)
                .semantic_aware(semantic);

        loop {
            // Check if indexer finished
            shutdown_signals.poll(&index_progress);
            if index_handle.is_finished() {
                break;
            }
            shutdown_signals.abort_if_needed(start.elapsed().as_millis());

            let phase = index_progress.phase.load(Ordering::Relaxed);
            let total = index_progress.total.load(Ordering::Relaxed);
            let current = index_progress.current.load(Ordering::Relaxed);
            let agents = index_progress.discovered_agents.load(Ordering::Relaxed);
            let is_rebuilding = index_progress.is_rebuilding.load(Ordering::Relaxed);

            // Get agent names for display
            let agent_names: Vec<String> = index_progress
                .discovered_agent_names
                .lock()
                .map(|names| names.clone())
                .unwrap_or_default();

            let phase_str = match phase {
                indexer::INDEX_PHASE_SCANNING => "Scanning",
                indexer::INDEX_PHASE_LEXICAL_INDEXING => "Indexing",
                indexer::INDEX_PHASE_SEMANTIC_INITIALIZE => "Semantic initialize",
                indexer::INDEX_PHASE_SEMANTIC_REPLAY => "Semantic replay",
                indexer::INDEX_PHASE_SEMANTIC_EMBEDDING => "Semantic embedding",
                indexer::INDEX_PHASE_SEMANTIC_VECTOR_PUBLISH => "Semantic vector publish",
                indexer::INDEX_PHASE_SEMANTIC_HNSW => "Semantic HNSW",
                indexer::INDEX_PHASE_SEMANTIC_MANIFEST => "Semantic manifest",
                indexer::INDEX_PHASE_SEMANTIC_FINALIZE => "Semantic finalize",
                indexer::INDEX_PHASE_ANALYTICS_REBUILD => "Rebuilding analytics",
                _ => "Preparing",
            };

            let rebuild_indicator = if is_rebuilding { " (rebuilding)" } else { "" };

            let msg = if phase == indexer::INDEX_PHASE_SCANNING {
                let scan_progress = if total > 0 {
                    format!("{current}/{total} connectors")
                } else {
                    "scanning connectors".to_string()
                };
                if agents > 0 {
                    let names_preview = if agent_names.len() <= 3 {
                        agent_names.join(", ")
                    } else {
                        format!(
                            "{}, ... +{} more",
                            agent_names[..3].join(", "),
                            agent_names.len() - 3
                        )
                    };
                    format!(
                        "{}{}: {} · {} agent(s): {}",
                        phase_str, rebuild_indicator, scan_progress, agents, names_preview
                    )
                } else {
                    format!(
                        "{}{}: {} · detecting agents...",
                        phase_str, rebuild_indicator, scan_progress
                    )
                }
            } else if phase == indexer::INDEX_PHASE_LEXICAL_INDEXING {
                // Indexing phase - show progress
                if total > 0 {
                    let pct = (current as f64 / total as f64 * 100.0).min(100.0);
                    format!(
                        "{}{}: {}/{} conversations ({:.0}%)",
                        phase_str, rebuild_indicator, current, total, pct
                    )
                } else {
                    format!("{}{}: Processing...", phase_str, rebuild_indicator)
                }
            } else if phase >= indexer::INDEX_PHASE_SEMANTIC_INITIALIZE && total > 0 {
                let pct = (current as f64 / total as f64 * 100.0).min(100.0);
                format!(
                    "{}: {}/{} {} ({:.0}%)",
                    phase_str,
                    current,
                    total,
                    indexer::IndexingProgress::phase_unit_for(phase),
                    pct
                )
            } else {
                format!("{}{}...", phase_str, rebuild_indicator)
            };

            // Update when values change OR every 500ms to show activity
            let now = std::time::Instant::now();
            let should_update = phase != last_phase
                || current != last_current
                || agents != last_agents
                || now.duration_since(last_update).as_millis() > 500;

            if should_update {
                pb.set_message(msg);
                last_phase = phase;
                last_current = current;
                last_agents = agents;
                last_update = now;
            }

            if let Some(payload) =
                stall_watchdog.observe(&index_progress, start.elapsed().as_millis())
            {
                let (summary, diagnostics) = index_stall_warning_lines(&payload);
                pb.println(summary);
                pb.println(diagnostics);
                abort_after_index_stall_if_requested(&payload, stall_watchdog.data_dir());
            }

            std::thread::sleep(Duration::from_millis(50));
        }

        // Final update
        let total = index_progress.total.load(Ordering::Relaxed);
        let current = index_progress.current.load(Ordering::Relaxed);
        let agents = index_progress.discovered_agents.load(Ordering::Relaxed);
        progress_completion = Some((pb, current.max(total), agents));
    } else if show_plain {
        // Plain mode: print periodic status updates
        use std::sync::atomic::Ordering;

        eprintln!("Starting index...");
        let mut last_phase = usize::MAX;
        let mut last_agents = 0;
        let mut last_current = 0;
        let mut last_scan_current = 0;
        let mut last_counter_emit = std::time::Instant::now()
            .checked_sub(Duration::from_secs(1))
            .unwrap_or_else(std::time::Instant::now);
        let mut stall_watchdog =
            IndexStallWatchdog::aborting_lexical_phases(data_dir.clone(), progress_interval)
                .watch_aware(watch)
                .semantic_aware(semantic);

        loop {
            shutdown_signals.poll(&index_progress);
            if index_handle.is_finished() {
                break;
            }
            shutdown_signals.abort_if_needed(start.elapsed().as_millis());

            let phase = index_progress.phase.load(Ordering::Relaxed);
            let total = index_progress.total.load(Ordering::Relaxed);
            let current = index_progress.current.load(Ordering::Relaxed);
            let agents = index_progress.discovered_agents.load(Ordering::Relaxed);

            // Print status on phase change
            if phase != last_phase {
                match phase {
                    indexer::INDEX_PHASE_SCANNING => eprintln!("Scanning for agents..."),
                    indexer::INDEX_PHASE_LEXICAL_INDEXING => {
                        eprintln!("Indexing conversations...")
                    }
                    phase if phase >= indexer::INDEX_PHASE_SEMANTIC_INITIALIZE => {
                        eprintln!("{}...", indexer::IndexingProgress::phase_label_for(phase));
                    }
                    _ => {}
                }
                last_phase = phase;
                last_current = current;
                last_counter_emit = std::time::Instant::now();
            }

            // Print scan progress during discovery
            if phase == indexer::INDEX_PHASE_SCANNING && current != last_scan_current {
                if total > 0 {
                    eprintln!("  Scanned {}/{} connectors", current, total);
                } else {
                    eprintln!("  Scanned {} connectors", current);
                }
                last_scan_current = current;
            }

            // Print agent discovery updates
            if agents > last_agents {
                eprintln!("  Found {} agent(s)", agents);
                last_agents = agents;
            }

            // Print indexing progress every 100 conversations
            if phase == indexer::INDEX_PHASE_LEXICAL_INDEXING
                && current > last_current
                && current % 100 == 0
            {
                if total > 0 {
                    eprintln!("  Indexed {}/{} conversations", current, total);
                } else {
                    eprintln!("  Indexed {} conversations", current);
                }
                last_current = current;
            }

            // Semantic counters can advance rapidly during replay/vector
            // publish. Emit at most once per second, plus the exact terminal
            // value, so plain mode is informative without becoming a log
            // flood on large corpora.
            if phase >= indexer::INDEX_PHASE_SEMANTIC_INITIALIZE
                && current != last_current
                && (last_counter_emit.elapsed() >= Duration::from_secs(1)
                    || (total > 0 && current >= total))
            {
                if total > 0 {
                    eprintln!(
                        "  {}/{} {}",
                        current,
                        total,
                        indexer::IndexingProgress::phase_unit_for(phase)
                    );
                } else {
                    eprintln!(
                        "  {} {}",
                        current,
                        indexer::IndexingProgress::phase_unit_for(phase)
                    );
                }
                last_current = current;
                last_counter_emit = std::time::Instant::now();
            }

            if let Some(payload) =
                stall_watchdog.observe(&index_progress, start.elapsed().as_millis())
            {
                let (summary, diagnostics) = index_stall_warning_lines(&payload);
                eprintln!("{summary}");
                eprintln!("{diagnostics}");
                abort_after_index_stall_if_requested(&payload, stall_watchdog.data_dir());
            }

            std::thread::sleep(Duration::from_millis(200));
        }
    } else if emit_progress_events {
        // Structured (--json / --robot / --robot-format ...) mode: emit NDJSON
        // progress events on stderr so callers see forward progress without
        // disturbing the single-payload JSON response on stdout.
        use std::sync::atomic::Ordering;

        let mut last_phase = usize::MAX;
        let mut last_emit = std::time::Instant::now()
            .checked_sub(progress_interval)
            .unwrap_or_else(std::time::Instant::now);
        let mut stall_watchdog =
            IndexStallWatchdog::aborting_lexical_phases(data_dir.clone(), progress_interval)
                .watch_aware(watch)
                .semantic_aware(semantic);

        // Finish-aware poll cadence: 100ms for snappy shutdown, but only emit a
        // `progress` event at `progress_interval`.
        loop {
            shutdown_signals.poll(&index_progress);
            if index_handle.is_finished() {
                break;
            }
            shutdown_signals.abort_if_needed(start.elapsed().as_millis());

            let phase_code = index_progress.phase.load(Ordering::Relaxed);
            let elapsed_ms = start.elapsed().as_millis();

            // Always emit on phase transitions, independent of the interval, so
            // callers see "scanning -> indexing" immediately.
            if phase_code != last_phase {
                let mut payload = index_progress.snapshot_json(elapsed_ms);
                if let serde_json::Value::Object(ref mut m) = payload {
                    m.insert("event".into(), serde_json::json!("phase"));
                    m.insert(
                        "ts_ms".into(),
                        serde_json::json!(chrono::Utc::now().timestamp_millis()),
                    );
                }
                emit_event(payload);
                last_phase = phase_code;
                last_emit = std::time::Instant::now();
            } else if last_emit.elapsed() >= progress_interval {
                let mut payload = index_progress.snapshot_json(elapsed_ms);
                if let serde_json::Value::Object(ref mut m) = payload {
                    m.insert("event".into(), serde_json::json!("progress"));
                    m.insert(
                        "ts_ms".into(),
                        serde_json::json!(chrono::Utc::now().timestamp_millis()),
                    );
                }
                emit_event(payload);
                last_emit = std::time::Instant::now();
            }

            if let Some(payload) = stall_watchdog.observe(&index_progress, elapsed_ms) {
                emit_event(payload.clone());
                emit_robot_stall_event_on_stdout(&payload);
                abort_after_index_stall_if_requested(&payload, stall_watchdog.data_dir());
            }

            std::thread::sleep(Duration::from_millis(100));
        }
    } else {
        // No progress display (json mode with events disabled, or plain=none):
        // just wait for completion.
        let mut stall_watchdog =
            IndexStallWatchdog::aborting_lexical_phases(data_dir.clone(), progress_interval)
                .watch_aware(watch)
                .semantic_aware(semantic);
        while !index_handle.is_finished() {
            shutdown_signals.poll(&index_progress);
            if index_handle.is_finished() {
                break;
            }
            shutdown_signals.abort_if_needed(start.elapsed().as_millis());
            if let Some(payload) =
                stall_watchdog.observe(&index_progress, start.elapsed().as_millis())
            {
                let (summary, diagnostics) = index_stall_warning_lines(&payload);
                eprintln!("{summary}");
                eprintln!("{diagnostics}");
                // #287: `--json --no-progress-events` callers still need a
                // machine-readable stall signal on stdout before any external
                // timeout kills the run.
                emit_robot_stall_event_on_stdout(&payload);
                abort_after_index_stall_if_requested(&payload, stall_watchdog.data_dir());
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    // Get the result from the indexer thread
    let mut active_index_error = None::<ActiveIndexRunDetails>;
    let mut res = match index_handle.join() {
        Ok(result) => result.map_err(|e| {
            if e.downcast_ref::<indexer::IndexInterrupted>().is_some() {
                // An interruption with no registered signal is the plain
                // SIGINT path: keep the literal 130 so the emitted-code scan
                // sees the documented exit it produces.
                if shutdown_signals.received.is_none() {
                    return CliError {
                        code: 130,
                        kind: CliErrorKind::Index.kind_str(),
                        message: "index interrupted after finishing the in-flight batch"
                            .into(),
                        hint: Some("Rerun the same index command to resume from committed source observations.".into()),
                        retryable: true,
                    };
                }
                // The signal streams register 130 (SIGINT) and 143 (SIGTERM);
                // the received code is a runtime value, so each arm spells the
                // `code:` field as a literal. The literal fields are
                // load-bearing: the emitted-exit-code scanners
                // (spec_exit_codes_documented, golden_error_envelope) only see
                // `code: <digits>` directly above `kind:` in a CliError
                // literal, not `received`/`Option` variables.
                return match shutdown_signals.received {
                    Some(143) => CliError {
                        code: 143,
                        kind: CliErrorKind::Index.kind_str(),
                        message: "index interrupted after finishing the in-flight batch".into(),
                        hint: Some("Rerun the same index command to resume from committed source observations.".into()),
                        retryable: true,
                    },
                    _ => CliError {
                        code: 130,
                        kind: CliErrorKind::Index.kind_str(),
                        message: "index interrupted after finishing the in-flight batch".into(),
                        hint: Some("Rerun the same index command to resume from committed source observations.".into()),
                        retryable: true,
                    },
                };
            }
            if e.downcast_ref::<indexer::ConnectorScanFailure>().is_some()
                && let Err(error) = index_result::require_complete_scan(true, &[])
            {
                // The source error and its path remain in connector stats.
                // A project named "busy" is not canonical-storage contention.
                return error;
            }
            let chain = e
                .chain()
                .map(std::string::ToString::to_string)
                .collect::<Vec<_>>()
                .join(" | ");
            if error_chain_indicates_active_cass_index(&chain) {
                let details = active_index_run_details(&data_dir, &db_path)
                    .unwrap_or_else(|| ActiveIndexRunDetails::without_owner(&data_dir, &db_path));
                let err = details.to_cli_error();
                active_index_error = Some(details);
                return err;
            }
            if error_chain_indicates_index_disk_headroom_blocked(&chain) {
                return index_disk_headroom_cli_error(&chain);
            }
            if error_chain_indicates_lexical_rebuild_disk_stop(&chain) {
                return lexical_rebuild_disk_stop_cli_error(&chain);
            }
            if error_chain_indicates_orphan_fk_cleanup_blocked(&chain) {
                return index_orphan_fk_cleanup_cli_error(&chain);
            }
            if error_chain_indicates_archive_health_blocked(&chain) {
                return index_archive_health_cli_error(&chain);
            }
            if error_chain_indicates_retryable_storage_contention(&chain) {
                return index_storage_contention_cli_error(&chain);
            }
            if error_chain_indicates_unsupported_quill_writer_admission(&chain) {
                return index_unsupported_quill_writer_admission_cli_error(&chain);
            }
            CliError {
                code: 9,
                kind: CliErrorKind::Index.kind_str(),
                message: format!("index failed: {chain}"),
                hint: None,
                retryable: true,
            }
        }),
        Err(_) => Err(CliError {
            code: 9,
            kind: CliErrorKind::Index.kind_str(),
            message: "index thread panicked".to_string(),
            hint: None,
            retryable: true,
        }),
    };
    if res.is_ok()
        && let Some(code) = shutdown_signals.received
    {
        // The final batch can complete just as the CLI observes a signal.
        // Honor the request without certifying an interrupted invocation as
        // successful merely because the worker won that race. Literal per-arm
        // `code:` fields: the emitted-exit-code scanners only see
        // `code: <digits>` directly above `kind:`.
        res = Err(match code {
            143 => CliError {
                code: 143,
                kind: CliErrorKind::Index.kind_str(),
                message: "index completed its current work after a stop was requested".into(),
                hint: Some("Rerun the same index command to confirm completion.".into()),
                retryable: true,
            },
            _ => CliError {
                code: 130,
                kind: CliErrorKind::Index.kind_str(),
                message: "index completed its current work after a stop was requested".into(),
                hint: Some("Rerun the same index command to confirm completion.".into()),
                retryable: true,
            },
        });
    }
    if let Some(previous) = previous_robot_trace_ingest {
        let _ = indexer::set_robot_trace_ingest_enabled(previous);
    }
    // Persisted neighbors are useful, but a failed source scan is not a
    // complete index invocation. Decide before success progress, completion
    // events and idempotency writes; do not reopen the archive to find out.
    if res.is_ok()
        && let Ok(stats) = index_progress.stats.lock()
        && let Err(error) =
            index_result::require_complete_scan(stats.scan_had_errors, &stats.connectors)
    {
        res = Err(error);
    }
    if res.is_ok()
        && let Err(err) =
            validate_successful_index_artifacts(&data_dir, &db_path, full, force_rebuild)
    {
        res = Err(err);
    }

    if let Some((pb, conversations, agents)) = progress_completion {
        match &res {
            Ok(_) => pb.finish_with_message(format!(
                "Done: {} conversations from {} agent(s)",
                conversations, agents
            )),
            Err(err) => pb.abandon_with_message(format!("Failed: {}", err)),
        }
    }

    let elapsed_ms = start.elapsed().as_millis();

    if let Err(err) = &res {
        // Surface every index failure at ERROR level so `cass index --watch`
        // (which runs without --json) can no longer exit with code 9 silently,
        // leaving the operator with no explanation in the watch log (issue #250,
        // coding_agent_session_search-u7r1z). stdout stays data-only; this
        // diagnostic line goes to stderr via tracing.
        tracing::error!(
            code = err.code,
            kind = %err.kind,
            retryable = err.retryable,
            "cass index failed (exit {}): {}",
            err.code,
            err.message
        );
        if let Some(fmt) = structured_format {
            let mut payload = cli_error_json_payload(err, elapsed_ms);
            if let Ok(stats) = index_progress.stats.lock()
                && index_result::annotate_partial_scan(
                    &mut payload,
                    stats.scan_had_errors,
                    &stats.connectors,
                )
            {
                // Include the original bounded connector diagnostics, not
                // just a generic command error. Counts remain run observations.
                payload["indexing_stats"] = serde_json::to_value(&*stats).unwrap_or_default();
            }
            if let Some(active_index) = &active_index_error {
                payload["active_index"] = active_index.to_json();
            }
            // A stalled or failed bounded run should still say what it used.
            payload["rebuild_profile"] = indexer::lexical_rebuild_profile_report();
            if emit_progress_events {
                let mut event = payload.clone();
                if let serde_json::Value::Object(ref mut m) = event {
                    m.insert("event".into(), serde_json::json!("error"));
                    m.insert(
                        "ts_ms".into(),
                        serde_json::json!(chrono::Utc::now().timestamp_millis()),
                    );
                }
                emit_event(event);
            }
            emit_result(payload, fmt)?;
        }
    } else if structured_format.is_some() || idempotency_key.is_some() {
        // Derive result counts from the indexer's own progress tracking rather
        // than reopening the live DB after the lexical checkpoint has already
        // been written (CASS #192). A post-checkpoint reopen through
        // with_frankensqlite_connection triggers Connection::open →
        // reload_memdb_from_pager_with_mode → rebuild_materialized_live_vtab,
        // which advances the DB fingerprint and leaves the just-written
        // lexical checkpoint stale, so the very next `cass health --json`
        // reports `unhealthy: database fingerprint changed since the last
        // lexical checkpoint`. Progress stats are captured before checkpoint,
        // so they are both safe and sufficient — if total_counts_exact is
        // false we still report whatever the indexer observed on this run
        // (falling back to 0 only when the indexer did not run a counting
        // pass at all).
        let (conversations, messages) = index_result_counts_from_progress(&index_progress)
            .or_else(|| {
                let stats = index_progress.stats.lock().ok()?;
                Some((
                    i64::try_from(stats.total_conversations).unwrap_or(i64::MAX),
                    i64::try_from(stats.total_messages).unwrap_or(i64::MAX),
                ))
            })
            .unwrap_or((0, 0));
        let (quarantined_conversations, lexical_update_deferred) = index_progress
            .stats
            .lock()
            .map(|stats| {
                (
                    stats.quarantined_conversations,
                    stats.lexical_update_deferred,
                )
            })
            .unwrap_or_default();
        let mut payload = serde_json::json!({
            "success": true,
            "elapsed_ms": elapsed_ms,
            "full": full,
            "force_rebuild": force_rebuild,
            "entrypoint": entrypoint,
            "data_dir": data_dir.display().to_string(),
            "db_path": db_path.display().to_string(),
            "conversations": conversations,
            "messages": messages,
            "quarantined_conversations": quarantined_conversations,
            "lexical_update_deferred": lexical_update_deferred,
            "rebuild_profile": indexer::lexical_rebuild_profile_report(),
        });

        // Add structured indexing stats if available (T7.4)
        if let Ok(stats) = index_progress.stats.lock()
            && let serde_json::Value::Object(ref mut map) = payload
        {
            map.insert(
                "indexing_stats".to_string(),
                serde_json::to_value(&*stats).unwrap_or_default(),
            );
        }

        // Store idempotency key if provided
        if let Some(key) = &idempotency_key {
            payload["idempotency_key"] = serde_json::json!(key);
            payload["cached"] = serde_json::json!(false);

            if let Err(e) = store_index_idempotency_result(
                &data_dir,
                &db_path,
                key,
                params_hash,
                &payload,
                chrono::Utc::now().timestamp_millis(),
            ) {
                // Persistence is best-effort, including typed index-busy when
                // a new writer starts after the completed indexing worker.
                tracing::warn!("Failed to store idempotency key: {e:#}");
            }
        }

        if emit_progress_events {
            let mut event = payload.clone();
            if let serde_json::Value::Object(ref mut m) = event {
                m.insert("event".into(), serde_json::json!("completed"));
                m.insert(
                    "ts_ms".into(),
                    serde_json::json!(chrono::Utc::now().timestamp_millis()),
                );
            }
            emit_event(event);
        }

        if let Some(fmt) = structured_format {
            emit_result(payload, fmt)?;
        }
    }

    // gh359: `res.is_ok()` — the plain completion line used to print even
    // after the error arm above had already reported a failed run, so a
    // failing `cass index` ended its stderr with "index completed".
    if show_plain && res.is_ok() {
        // gh359: "index completed" after a run that deferred the
        // authoritative lexical repair (and therefore made nothing new
        // searchable) was the worst possible signal for a large archive.
        // Say what actually happened and name the command that completes it.
        // Keyed on the specific size-threshold deferral record — the broad
        // `lexical_update_deferred` flag is also set by OOM-deferred inline
        // updates whose run then completes a full in-run rebuild, where this
        // footer would be wrong on both outcome and cause (adversarial-review
        // F3).
        let lexical_deferred = index_progress
            .stats
            .lock()
            .map(|stats| {
                stats.lexical_repair.as_ref().is_some_and(|repair| {
                    repair.kind == "deferred_authoritative_canonical_db_rebuild"
                })
            })
            .unwrap_or(false);
        if lexical_deferred {
            eprintln!(
                "index completed WITHOUT refreshing lexical search: this archive exceeds the automatic-repair size threshold, so the authoritative lexical rebuild was deferred. Run `cass index --full` to make new sessions searchable."
            );
        } else {
            eprintln!("index completed");
        }
    }

    match res {
        Err(err) if structured_format.is_some() => Err(CliError::already_reported_from(&err)),
        other => other,
    }
}

#[derive(Debug, Clone)]
pub(crate) struct IndexingExclusionNotice {
    pub(crate) config_path: String,
    pub(crate) disabled_agents: Vec<String>,
    pub(crate) disabled_connectors: Vec<String>,
    pub(crate) disabled_index_agents: Vec<String>,
    pub(crate) total_connectors: usize,
    pub(crate) enabled_connectors: usize,
    pub(crate) reenable_commands: Vec<String>,
}

impl IndexingExclusionNotice {
    pub(crate) fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "config_path": self.config_path,
            "disabled_agents": self.disabled_agents,
            "disabled_connectors": self.disabled_connectors,
            "disabled_index_agents": self.disabled_index_agents,
            "total_connectors": self.total_connectors,
            "enabled_connectors": self.enabled_connectors,
            "reenable_commands": self.reenable_commands,
        })
    }
}

pub(crate) fn active_indexing_exclusion_notice() -> Option<IndexingExclusionNotice> {
    use crate::sources::config::SourcesConfig;

    if dotenvy::var("CASS_IGNORE_SOURCES_CONFIG").is_ok() {
        return None;
    }

    let config = SourcesConfig::load().ok()?;
    let disabled_agents = config.configured_disabled_agents();
    if disabled_agents.is_empty() {
        return None;
    }

    let factories = crate::indexer::get_connector_factories();
    let total_connectors = factories.len();
    let disabled_connectors = factories
        .iter()
        .filter_map(|(name, _)| {
            config
                .is_agent_disabled(name)
                .then_some((*name).to_string())
        })
        .collect::<Vec<_>>();
    let mut disabled_index_agents = disabled_connectors
        .iter()
        .map(|name| index_agent_slug_for_connector_notice(name).to_string())
        .collect::<Vec<_>>();
    disabled_index_agents.sort();
    disabled_index_agents.dedup();
    let enabled_connectors = total_connectors.saturating_sub(disabled_connectors.len());
    let config_path = SourcesConfig::config_path()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    let reenable_commands = disabled_agents
        .iter()
        .map(|agent| format!("cass sources agents include {agent}"))
        .collect::<Vec<_>>();

    Some(IndexingExclusionNotice {
        config_path,
        disabled_agents,
        disabled_connectors,
        disabled_index_agents,
        total_connectors,
        enabled_connectors,
        reenable_commands,
    })
}

pub(crate) fn index_agent_slug_for_connector_notice(connector_name: &str) -> &str {
    match connector_name {
        "claude" => "claude_code",
        other => other,
    }
}

/// Names of legacy `type = "ssh"` source entries the index run will treat as
/// read-only archives. `None` when the config is ignored, unreadable, or has
/// no legacy entries. Emitted once per index run on stderr so robot stdout
/// stays clean.
pub(crate) fn legacy_remote_source_notice() -> Option<Vec<String>> {
    use crate::sources::config::SourcesConfig;

    if dotenvy::var("CASS_IGNORE_SOURCES_CONFIG").is_ok() {
        return None;
    }

    let config = SourcesConfig::load().ok()?;
    let names: Vec<String> = config
        .sources
        .iter()
        .filter(|source| source.is_remote())
        .map(|source| source.name.clone())
        .collect();
    (!names.is_empty()).then_some(names)
}

pub(crate) fn emit_legacy_remote_source_notice(names: &[String]) {
    eprintln!(
        "warning: sources.toml lists {} legacy remote entr{} ({}); remote synchronization was removed, so {} treated as read-only archive record(s) — mirrored data is indexed if present",
        names.len(),
        if names.len() == 1 { "y" } else { "ies" },
        names.join(", "),
        if names.len() == 1 {
            "it is"
        } else {
            "they are"
        },
    );
}

pub(crate) fn emit_indexing_exclusion_notice(
    notice: &IndexingExclusionNotice,
    structured_output: bool,
) {
    if structured_output {
        let mut payload = notice.to_json();
        if let serde_json::Value::Object(ref mut map) = payload {
            map.insert(
                "event".to_string(),
                serde_json::json!("indexing_exclusions"),
            );
            map.insert(
                "ts_ms".to_string(),
                serde_json::json!(chrono::Utc::now().timestamp_millis()),
            );
            map.insert(
                "hint".to_string(),
                serde_json::json!(
                    "These connectors are disabled by sources.toml and will not be scanned."
                ),
            );
        }
        if let Ok(line) = serde_json::to_string(&payload) {
            eprintln!("{line}");
        }
        return;
    }

    eprintln!(
        "Indexing exclusions active from {}: disabled agents [{}]; disabled connectors [{}]; indexed agent slugs [{}].",
        notice.config_path,
        notice.disabled_agents.join(", "),
        if notice.disabled_connectors.is_empty() {
            "none".to_string()
        } else {
            notice.disabled_connectors.join(", ")
        },
        if notice.disabled_index_agents.is_empty() {
            "none".to_string()
        } else {
            notice.disabled_index_agents.join(", ")
        }
    );
    if !notice.reenable_commands.is_empty() {
        eprintln!("Re-enable with:");
        for command in &notice.reenable_commands {
            eprintln!("  {command}");
        }
    }
}
