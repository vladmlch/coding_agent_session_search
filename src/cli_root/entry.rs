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

pub(crate) const CONTRACT_VERSION: &str = "2";
pub(crate) const DEFAULT_STALE_THRESHOLD_SECS: u64 = 1800;
pub(crate) const DEFAULT_PACK_FRESHNESS_WINDOW_SECONDS: i64 = 30 * 24 * 60 * 60;

#[cfg(test)]
pub(crate) fn read_watch_once_paths_env() -> Option<Vec<std::path::PathBuf>> {
    dotenvy::var("CASS_TEST_WATCH_PATHS")
        .ok()
        .map(|list| {
            list.split(',')
                .filter(|s| !s.trim().is_empty())
                .map(std::path::PathBuf::from)
                .collect::<Vec<_>>()
        })
        .filter(|v| !v.is_empty())
}

#[cfg(not(test))]
pub(crate) fn read_watch_once_paths_env() -> Option<Vec<std::path::PathBuf>> {
    None
}

/// Absolutize explicit paths without erasing provider hints in symlink names.
/// Classification resolves symlinks after retaining the original hint (#478),
/// then uses canonical paths for scan-root matching and I/O (#377).
pub(crate) fn absolutize_watch_once_paths(paths: Vec<PathBuf>) -> Vec<PathBuf> {
    paths
        .into_iter()
        .map(|path| {
            if path.is_absolute() {
                path
            } else {
                match std::env::current_dir() {
                    Ok(cwd) => cwd.join(&path),
                    Err(_) => path,
                }
            }
        })
        .collect()
}

pub(crate) fn resolve_watch_once_paths_from_sources(
    watch: bool,
    watch_once: Option<Vec<PathBuf>>,
    env_watch_once_paths: Option<Vec<PathBuf>>,
) -> Option<Vec<PathBuf>> {
    let explicit = watch_once.filter(|paths| !paths.is_empty());
    if let Some(paths) = explicit {
        return Some(absolutize_watch_once_paths(paths));
    }

    if watch {
        return env_watch_once_paths
            .filter(|paths| !paths.is_empty())
            .map(absolutize_watch_once_paths);
    }

    None
}

pub(crate) fn resolve_watch_once_paths(
    watch: bool,
    watch_once: Option<Vec<PathBuf>>,
) -> Option<Vec<PathBuf>> {
    resolve_watch_once_paths_from_sources(watch, watch_once, read_watch_once_paths_env())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct IndexEntrypointDiagnostics {
    pub(crate) kind: &'static str,
    pub(crate) full: bool,
    pub(crate) force_rebuild: bool,
    pub(crate) watch: bool,
    pub(crate) watch_once_path_count: usize,
    pub(crate) semantic: bool,
    pub(crate) build_hnsw: bool,
    pub(crate) migration_state: &'static str,
}

pub(crate) fn index_entrypoint_diagnostics(
    full: bool,
    force_rebuild: bool,
    watch: bool,
    watch_once_path_count: usize,
    semantic: bool,
    build_hnsw: bool,
) -> IndexEntrypointDiagnostics {
    let kind = if watch_once_path_count > 0 {
        if watch { "watch_cycle" } else { "watch_once" }
    } else if watch {
        "watch"
    } else if full {
        "full_rebuild"
    } else if force_rebuild {
        "force_rebuild"
    } else if semantic && build_hnsw {
        "semantic_hnsw"
    } else if semantic {
        "semantic_backfill"
    } else {
        "incremental"
    };

    IndexEntrypointDiagnostics {
        kind,
        full,
        force_rebuild,
        watch,
        watch_once_path_count,
        semantic,
        build_hnsw,
        migration_state: "tin8o_entrypoint_observed",
    }
}

pub(crate) fn with_frankensqlite_connection<T, F>(
    db_path: &Path,
    context: &str,
    op: F,
) -> std::result::Result<T, crate::franken_sync::FrankenError>
where
    F: FnOnce(
        &crate::franken_sync::Connection,
    ) -> std::result::Result<T, crate::franken_sync::FrankenError>,
{
    let mut conn = crate::franken_sync::Connection::open(db_path.to_string_lossy().as_ref())?;
    let result = op(&conn);
    let close_result = conn.close_in_place();
    match (result, close_result) {
        (Ok(value), Ok(())) => Ok(value),
        (Ok(value), Err(close_err)) => {
            warn!(
                error = %close_err,
                db_path = %db_path.display(),
                "{context}: close_in_place failed; falling back to best-effort close"
            );
            conn.close_best_effort_in_place();
            Ok(value)
        }
        (Err(err), Ok(())) => Err(err),
        (Err(err), Err(close_err)) => {
            warn!(
                error = %close_err,
                db_path = %db_path.display(),
                "{context}: close_in_place failed after error; falling back to best-effort close"
            );
            conn.close_best_effort_in_place();
            Err(err)
        }
    }
}

pub struct ParsedCli {
    pub cli: Cli,
    pub(crate) raw_args: Vec<String>,
    pub(crate) parse_note: Option<String>,
    pub(crate) heuristic_note: Option<String>,
}

pub fn parse_cli(raw_args: Vec<String>) -> CliResult<ParsedCli> {
    // A removed command or flag errors here, before normalization, alias
    // expansion, implicit-search folding or typo recovery can rewrite the
    // invocation into a different live command (cass-local plan, task 6).
    if let Some(error) = removed_invocation_error(&raw_args, REMOVED_COMMANDS, REMOVED_FLAGS) {
        return Err(error);
    }

    // First normalization pass (global flags lift)
    let (normalized_args, parse_note) = normalize_args(raw_args.clone());

    let (mut cli, heuristic_note, parsed_args) = match Cli::try_parse_from(&normalized_args) {
        Ok(cli) => (cli, None, normalized_args),
        Err(err) => {
            // Let clap handle help/version natively (exit 0, print to stdout)
            use clap::error::ErrorKind;
            let is_display = |kind: ErrorKind| {
                matches!(kind, ErrorKind::DisplayHelp | ErrorKind::DisplayVersion)
            };
            // Help renders from the whole tree: the root help then lists the
            // binary-dispatched subcommands (serve, archive), and `cass help
            // serve`, an unknown subcommand to this CLI alone, shows its help.
            if let Err(full) = cli_command().try_get_matches_from(&normalized_args)
                && is_display(full.kind())
            {
                full.exit();
            }
            if is_display(err.kind()) {
                err.exit();
            }

            // Handle bare subcommand invocations (e.g. `cass analytics` without a
            // sub-subcommand).  Clap reports MissingSubcommand for these; we turn
            // that into a targeted hint listing the available sub-subcommands.
            if err.kind() == ErrorKind::MissingSubcommand {
                let msg = format_missing_subcommand_error(&normalized_args);
                return Err(CliError::usage(msg, None));
            }

            // Attempt heuristic recovery
            if let Some((recovered_args, note)) = heuristic_parse_recovery(&err, &normalized_args) {
                // Try parsing again with recovered args
                match Cli::try_parse_from(&recovered_args) {
                    Ok(cli) => (cli, Some(note), recovered_args),
                    Err(retry_err) => {
                        // Check again for help/version in case recovered args triggered it
                        if matches!(
                            retry_err.kind(),
                            ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
                        ) {
                            retry_err.exit();
                        }
                        // Recovery failed to produce valid args, fail with original error + friendly help
                        let friendly =
                            format_friendly_parse_error(err, &raw_args, &normalized_args);
                        if friendly.trim().starts_with('{') {
                            return Err(CliError {
                                code: 2,
                                kind: CliErrorKind::Usage.kind_str(),
                                message: friendly,
                                hint: None,
                                retryable: false,
                            });
                        }
                        return Err(CliError::usage("Could not parse arguments", Some(friendly)));
                    }
                }
            } else {
                // No recovery possible
                let friendly = format_friendly_parse_error(err, &raw_args, &normalized_args);
                if friendly.trim().starts_with('{') {
                    return Err(CliError {
                        code: 2,
                        kind: CliErrorKind::Usage.kind_str(),
                        message: friendly,
                        hint: None,
                        retryable: false,
                    });
                }
                return Err(CliError::usage("Could not parse arguments", Some(friendly)));
            }
        }
    };

    // `--db` and `CASS_DB_PATH` fill the same field and only the matcher
    // knows which one supplied it.
    if cli.db.is_some() {
        cli.interpretation.db_from_env = Cli::command()
            .try_get_matches_from(&parsed_args)
            .ok()
            .and_then(|matches| matches.value_source("db"))
            == Some(clap::parser::ValueSource::EnvVariable);
    }
    cli.interpretation.corrections = [parse_note.as_deref(), heuristic_note.as_deref()]
        .into_iter()
        .flatten()
        .map(str::to_string)
        .collect();

    Ok(ParsedCli {
        cli,
        raw_args,
        parse_note,
        heuristic_note,
    })
}

pub async fn run() -> CliResult<()> {
    let parsed = parse_cli(std::env::args().collect())?;
    run_with_parsed(parsed).await
}

pub async fn run_with_parsed(parsed: ParsedCli) -> CliResult<()> {
    let ParsedCli {
        cli,
        raw_args,
        parse_note,
        heuristic_note,
    } = parsed;

    let stdout_is_tty = io::stdout().is_terminal();
    let stderr_is_tty = io::stderr().is_terminal();
    configure_color(cli.color, stdout_is_tty, stderr_is_tty);

    let wrap_cfg = WrapConfig::new(cli.wrap, cli.nowrap);
    let progress_resolved = resolve_progress(cli.progress, stdout_is_tty);

    let start_ts = Utc::now();
    let start_instant = Instant::now();
    let command_label = describe_command(&cli);

    // Output correction notices for AI agents
    // These teach the agent proper syntax while still honoring their intent
    // Detect robot mode from raw args (more reliable than pattern matching complex enums)
    let is_robot_mode = raw_args
        .iter()
        .any(|s| s == "--json" || s == "--robot" || s == "-json" || s == "-robot")
        || cli.robot_format.is_some()
        || robot_format_from_env().is_some()
        || matches!(&cli.command, Some(Commands::Capabilities { .. }))
        || matches!(&cli.command, Some(Commands::Introspect { .. }));
    let is_doc_mode = cli.robot_help || matches!(&cli.command, Some(Commands::RobotDocs { .. }));

    // Combine all correction notes
    let all_notes: Vec<&str> = [parse_note.as_deref(), heuristic_note.as_deref()]
        .into_iter()
        .flatten()
        .collect();

    // Human teaching notes can precede execution. For robots, defer notes
    // until success: on failure stderr must remain one structured error
    // envelope, not plaintext followed by JSON. Successful recovery retains
    // its documented teaching note without contaminating stdout.
    if !all_notes.is_empty() && !is_doc_mode && !is_robot_mode {
        emit_correction_notes(&all_notes, false);
    }

    let result = execute_cli(&cli, wrap_cfg, progress_resolved, stderr_is_tty).await;

    if result.is_ok() && !all_notes.is_empty() && !is_doc_mode && is_robot_mode {
        emit_correction_notes(&all_notes, true);
    }

    if let Some(path) = &cli.trace_file {
        let duration_ms = start_instant.elapsed().as_millis();
        let exit_code = result.as_ref().map_or_else(|e| e.code, |()| 0);
        if let Err(trace_err) = write_trace_line(
            path,
            &command_label,
            &cli,
            &start_ts,
            duration_ms,
            exit_code,
            result.as_ref().err(),
        ) {
            eprintln!("trace-write error: {trace_err}");
        }
    }

    result
}

/// Run startup-sensitive commands before constructing the async runtime.
///
/// `cass health --json` is documented as a <50ms fast-readiness surface and
/// `cass selftest --json` is a binary-promotion probe that must not pay archive
/// or runtime startup costs. Even current-thread runtime setup is visible in
/// subprocess latency tests, so keep both commands on this synchronous path
/// and leave the general async runner for commands that actually need it.
pub fn try_run_with_parsed_fast(parsed: ParsedCli) -> Result<CliResult<()>, Box<ParsedCli>> {
    let ParsedCli {
        cli,
        raw_args,
        parse_note,
        heuristic_note,
    } = parsed;

    let command = cli.command.clone();
    if !matches!(
        command.as_ref(),
        Some(Commands::Health { .. } | Commands::Selftest { .. })
    ) {
        return Err(Box::new(ParsedCli {
            cli,
            raw_args,
            parse_note,
            heuristic_note,
        }));
    }

    let stdout_is_tty = io::stdout().is_terminal();
    let stderr_is_tty = io::stderr().is_terminal();
    configure_color(cli.color, stdout_is_tty, stderr_is_tty);

    let start_ts = Utc::now();
    let start_instant = Instant::now();
    let command_label = describe_command(&cli);

    let is_robot_mode = raw_args
        .iter()
        .any(|s| matches!(s.as_str(), "--json" | "--robot" | "-json" | "-robot"))
        || cli.robot_format.is_some()
        || robot_format_from_env().is_some();
    let all_notes: Vec<&str> = [parse_note.as_deref(), heuristic_note.as_deref()]
        .into_iter()
        .flatten()
        .collect();

    if !all_notes.is_empty() && !is_robot_mode {
        emit_correction_notes(&all_notes, false);
    }

    let result = match command.expect("fast command was matched above") {
        Commands::Health {
            data_dir,
            robot_meta,
            stale_threshold,
            binary_only,
            json,
        } => {
            let structured_format = resolve_subcommand_structured_format(&cli, json);
            run_health(
                &data_dir,
                cli.db.clone(),
                structured_format,
                stale_threshold,
                binary_only,
                robot_meta,
            )
        }
        Commands::Selftest { json } => {
            let structured_format = resolve_subcommand_structured_format(&cli, json);
            run_selftest(structured_format)
        }
        _ => unreachable!("non-fast command passed the fast-command guard"),
    };

    if result.is_ok() && !all_notes.is_empty() && is_robot_mode {
        emit_correction_notes(&all_notes, true);
    }

    if let Some(path) = &cli.trace_file {
        let duration_ms = start_instant.elapsed().as_millis();
        let exit_code = result.as_ref().map_or_else(|e| e.code, |()| 0);
        if let Err(trace_err) = write_trace_line(
            path,
            &command_label,
            &cli,
            &start_ts,
            duration_ms,
            exit_code,
            result.as_ref().err(),
        ) {
            eprintln!("trace-write error: {trace_err}");
        }
    }

    Ok(result)
}
