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

pub(crate) async fn execute_cli(
    cli: &Cli,
    wrap: WrapConfig,
    progress: ProgressResolved,
    stderr_is_tty: bool,
) -> CliResult<()> {
    if cli.robot_help {
        print_robot_help(wrap)?;
        return Ok(());
    }

    let command = match cli.command.clone() {
        Some(command) => command,
        None => {
            if cli.robot_format.is_some() || robot_format_from_env().is_some() {
                Commands::Triage {
                    data_dir: None,
                    json: true,
                    stale_threshold: 1800,
                    timeout: None,
                }
            } else {
                Cli::command().print_help().map_err(|e| CliError {
                    code: 9,
                    kind: CliErrorKind::Io.kind_str(),
                    message: format!("Failed to print CLI help: {e}"),
                    hint: None,
                    retryable: false,
                })?;
                println!();
                return Ok(());
            }
        }
    };

    if let Commands::RobotDocs { topic } = command.clone() {
        print_robot_docs(topic, wrap)?;
        return Ok(());
    }

    // Auto-quiet in robot mode: suppress INFO/WARN logs for clean JSON output.
    // WARN was previously allowed through, but even on stderr it breaks JSON
    // consumers that merge stderr into stdout (e.g., `cass index --json 2>&1`).
    // Only errors are important enough to surface in machine-readable mode.
    let robot_mode = is_robot_mode(&command, cli);
    // Single enforcement chokepoint for stdout/stderr hygiene: robot/quiet
    // output is machine-consumed, so the tracing filter is pinned to `error`
    // (overriding RUST_LOG) to guarantee no dependency INFO/WARN logging can
    // interleave with JSON. See `robot_aware_log_directive` and the regression
    // suite in tests/cli_robot_log_hygiene.rs
    // (coding_agent_session_search-cass-fleet-resilience-20260608-uojcg.2.1).
    let filter = build_robot_aware_log_filter(robot_mode, cli.verbose, cli.quiet);

    match &command {
        Commands::Index { .. }
        | Commands::Search { .. }
        | Commands::Pack { .. }
        | Commands::Stats { .. }
        | Commands::Diag { .. }
        | Commands::Storage { .. }
        | Commands::Dedup { .. }
        | Commands::Status { .. }
        | Commands::Triage { .. }
        | Commands::SupportBundle { .. }
        | Commands::View { .. }
        | Commands::Pages { .. } => {
            // Robot-safe stderr hygiene + optional explicit --trace-file sink
            // (uojcg.2.1 / uojcg.2.5). Keep the guard alive for the command run.
            let _trace_guard = init_cli_tracing(
                filter,
                cli.trace_file.as_deref(),
                matches!(cli.color, ColorPref::Always)
                    || (matches!(cli.color, ColorPref::Auto) && stderr_is_tty),
            );

            match command {
                Commands::Index {
                    full,
                    force_rebuild,
                    reconcile_conversation,
                    gc,
                    watch,
                    watch_once,
                    watch_interval,
                    data_dir,
                    semantic,
                    build_hnsw,
                    embedder,
                    idempotency_key,
                    json,
                    progress_interval_ms,
                    no_progress_events,
                    robot_trace_ingest,
                    background,
                    rebuild_profile,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    // Before any rebuild setting is read in this process.
                    indexer::set_lexical_rebuild_profile(rebuild_profile.into());
                    if let Some(conversation_id) = reconcile_conversation {
                        return run_lexical_reconcile_cli(
                            cli.db.clone(),
                            data_dir,
                            conversation_id,
                            structured_format,
                        );
                    }
                    if gc {
                        return run_lexical_gc_cli(cli.db.clone(), data_dir, structured_format);
                    }
                    run_index_with_data(
                        cli.db.clone(),
                        full,
                        force_rebuild,
                        watch,
                        watch_once,
                        watch_interval,
                        data_dir,
                        semantic,
                        build_hnsw,
                        embedder,
                        progress,
                        structured_format,
                        idempotency_key,
                        progress_interval_ms,
                        no_progress_events,
                        robot_trace_ingest,
                        background,
                        None,
                    )?;
                }
                Commands::Search {
                    query,
                    agent,
                    workspace,
                    limit,
                    offset,
                    json,
                    robot_meta,
                    fields,
                    max_content_length,
                    max_tokens,
                    request_id,
                    cursor,
                    display,
                    data_dir,
                    days,
                    today,
                    yesterday,
                    week,
                    since,
                    until,
                    aggregate,
                    explain,
                    dry_run,
                    timeout,
                    highlight,
                    source,
                    sessions_from,
                    mode,
                    approximate,
                    model,
                    rerank,
                    reranker,
                    daemon,
                    no_daemon,
                    two_tier,
                    fast_only,
                    quality_only,
                    refresh,
                    no_maintenance,
                } => {
                    // Validate mutually exclusive two-tier flags
                    let tier_count = [two_tier, fast_only, quality_only]
                        .iter()
                        .filter(|&&b| b)
                        .count();
                    if tier_count > 1 {
                        return Err(CliError::usage(
                            "Cannot specify multiple tier flags",
                            Some(
                                "Use only one of --two-tier, --fast-only, or --quality-only"
                                    .to_string(),
                            ),
                        ));
                    }

                    // Validate mutually exclusive flags
                    if daemon && no_daemon {
                        return Err(CliError::usage(
                            "Cannot specify both --daemon and --no-daemon",
                            Some(
                                "Use --daemon to enable daemon or --no-daemon to disable it"
                                    .to_string(),
                            ),
                        ));
                    }

                    // Warn about reranker without rerank flag
                    if reranker.is_some() && !rerank {
                        eprintln!(
                            "Warning: --reranker specified but --rerank not enabled; reranker will be ignored"
                        );
                    }

                    // Build semantic options from new flags
                    let tier_mode = if two_tier {
                        crate::search::query::SemanticTierMode::Progressive
                    } else if fast_only {
                        crate::search::query::SemanticTierMode::FastOnly
                    } else if quality_only {
                        crate::search::query::SemanticTierMode::QualityOnly
                    } else {
                        crate::search::query::SemanticTierMode::Single
                    };

                    let daemon_policy = semantic_daemon_policy(daemon, no_daemon);
                    let semantic_opts = SemanticSearchOptions {
                        model: model.clone(),
                        rerank,
                        reranker: reranker.clone(),
                        // #347: an already-running daemon is the inexpensive
                        // default. `--daemon` additionally permits auto-spawn;
                        // `--no-daemon` is the explicit direct-inference escape.
                        use_daemon: daemon_policy.use_existing,
                        auto_spawn_daemon: daemon_policy.auto_spawn,
                        approximate,
                        tier_mode,
                    };

                    // Only pass through robot_format when it was explicitly
                    // requested (CLI flag or env var). Returning `None` here
                    // lets `run_cli_search` apply its full precedence chain
                    // (robot_format > --json > env > robot_auto > display),
                    // so `--display table|lines|markdown` actually wins when
                    // no structured format was asked for.
                    let effective_format = cli.robot_format.or_else(robot_format_from_env);

                    // Resolve configurable search defaults (#303): the explicit
                    // CLI flag wins, then the env var, then `~/.config/cass/cass.toml`
                    // `[search]`, then the built-in default. This lets agents set a
                    // default timeout/limit/mode once instead of appending flags to
                    // every invocation. A broken config file or a malformed env value
                    // is a clear usage error rather than a silent fall-through.
                    let (eff_timeout, eff_limit, eff_mode) =
                        resolve_search_defaults(timeout, limit, mode)?;

                    run_cli_search(
                        &query,
                        &agent,
                        &workspace,
                        &eff_limit,
                        &offset,
                        &json,
                        effective_format,
                        robot_meta,
                        fields,
                        max_content_length,
                        max_tokens,
                        request_id.clone(),
                        cursor.clone(),
                        display,
                        &data_dir,
                        cli.db.clone(),
                        wrap,
                        progress,
                        robot_mode,
                        TimeFilter::new(
                            days,
                            today,
                            yesterday,
                            week,
                            since.as_deref(),
                            until.as_deref(),
                        )?,
                        aggregate,
                        explain,
                        dry_run,
                        no_maintenance,
                        eff_timeout,
                        highlight,
                        source,
                        sessions_from,
                        eff_mode,
                        semantic_opts,
                        refresh,
                        &cli.interpretation,
                    )?;
                }
                Commands::Pack {
                    query,
                    agent,
                    workspace,
                    limit,
                    json,
                    fields,
                    max_tokens,
                    max_sessions,
                    max_evidence,
                    context_lines,
                    max_excerpt_chars,
                    request_id,
                    display,
                    data_dir,
                    days,
                    today,
                    yesterday,
                    week,
                    since,
                    until,
                    source,
                    sessions_from,
                    mode,
                    freshness_policy,
                    freshness_window_seconds,
                    include_skill_content,
                    require_evidence,
                    explain_selection,
                    refresh,
                    timeout,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    // Apply the configurable search defaults (#303) to pack too,
                    // so a configured default timeout/limit/mode covers the
                    // pack path (which runs the same search underneath).
                    let (eff_timeout, eff_limit, eff_mode) =
                        resolve_search_defaults(timeout, limit, mode)?;
                    run_cli_pack(
                        &query,
                        &agent,
                        &workspace,
                        eff_limit,
                        structured_format,
                        fields,
                        max_tokens,
                        max_sessions,
                        max_evidence,
                        context_lines,
                        max_excerpt_chars,
                        request_id.clone(),
                        display,
                        &data_dir,
                        cli.db.clone(),
                        TimeFilter::new(
                            days,
                            today,
                            yesterday,
                            week,
                            since.as_deref(),
                            until.as_deref(),
                        )?,
                        source,
                        sessions_from,
                        eff_mode,
                        &freshness_policy,
                        freshness_window_seconds,
                        include_skill_content,
                        require_evidence,
                        explain_selection,
                        refresh,
                        eff_timeout,
                        wrap,
                        &cli.interpretation,
                    )?;
                }
                Commands::Stats {
                    data_dir,
                    json,
                    source,
                    by_source,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_stats(
                        &data_dir,
                        cli.db.clone(),
                        structured_format,
                        source.as_deref(),
                        by_source,
                    )?;
                }
                Commands::Diag {
                    data_dir,
                    json,
                    quarantine,
                    verbose,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_diag(
                        &data_dir,
                        cli.db.clone(),
                        structured_format,
                        quarantine,
                        verbose,
                    )?;
                }
                Commands::Storage { data_dir, json } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_storage(&data_dir, cli.db.clone(), structured_format)?;
                }
                Commands::Dedup {
                    data_dir,
                    json,
                    apply,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_dedup(&data_dir, cli.db.clone(), structured_format, apply)?;
                }
                Commands::Status {
                    data_dir,
                    json,
                    robot_meta,
                    stale_threshold,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_status(
                        &data_dir,
                        cli.db.clone(),
                        structured_format,
                        stale_threshold,
                        robot_meta,
                    )?;
                }
                Commands::Triage {
                    data_dir,
                    json,
                    stale_threshold,
                    timeout,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_triage(
                        &data_dir,
                        cli.db.clone(),
                        structured_format,
                        stale_threshold,
                        timeout,
                    )?;
                }
                Commands::SupportBundle {
                    data_dir,
                    json,
                    stale_threshold,
                    include_full_paths,
                    include_raw_evidence,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_support_bundle(
                        &data_dir,
                        cli.db.clone(),
                        structured_format,
                        stale_threshold,
                        include_full_paths,
                        include_raw_evidence,
                    )?;
                }
                Commands::View {
                    path,
                    source,
                    conversation_id,
                    message_index,
                    line,
                    context,
                    json,
                    timeout,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    followup_coordinates::run_view(
                        &path,
                        cli.db.clone(),
                        source.as_deref(),
                        conversation_id,
                        ViewWindow { line, context },
                        structured_format,
                        timeout,
                        message_index,
                    )?;
                }
                cmd @ Commands::Pages { .. } => {
                    execute_pages_command(cmd, cli, robot_mode).await?;
                }
                #[cfg(unix)]
                Commands::Daemon {
                    socket,
                    idle_timeout,
                    max_connections,
                    data_dir,
                } => {
                    run_daemon(socket, idle_timeout, max_connections, data_dir)?;
                }
                _ => {}
            }
        }
        _ => {
            let _trace_guard = init_cli_tracing(
                filter,
                cli.trace_file.as_deref(),
                matches!(cli.color, ColorPref::Always)
                    || (matches!(cli.color, ColorPref::Auto) && stderr_is_tty),
            );

            match command {
                Commands::Completions { shell } => {
                    let mut cmd = cli_command();
                    clap_complete::generate(shell, &mut cmd, "cass", &mut std::io::stdout());
                }
                Commands::Man => {
                    let cmd = cli_command();
                    let man = clap_mangen::Man::new(cmd);
                    man.render(&mut std::io::stdout())
                        .map_err(|e| CliError::unknown(format!("failed to render man: {e}")))?;
                }
                Commands::Bookmarks(args) => {
                    // The bookmarks module owns its output contract: exactly
                    // one JSON document on stdout under --json, and on failure
                    // one error envelope on stderr. It hands back the exit
                    // code instead of a CliError so the top-level handler does
                    // not print a second envelope.
                    let code = bookmarks::run_bookmarks_command(args)
                        .map_err(|e| CliError::unknown(format!("bookmarks command failed: {e}")))?;
                    if code != 0 {
                        use std::io::Write as _;
                        let _ = std::io::stdout().flush();
                        let _ = std::io::stderr().flush();
                        std::process::exit(code);
                    }
                }
                Commands::Capabilities { json } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_capabilities(structured_format)?;
                }
                Commands::ApiVersion { json } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_api_version(structured_format)?;
                }
                Commands::State {
                    data_dir,
                    json,
                    robot_meta,
                    stale_threshold,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_status(
                        &data_dir,
                        None,
                        structured_format,
                        stale_threshold,
                        robot_meta,
                    )?;
                }
                Commands::Introspect { json } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_introspect(structured_format)?;
                }
                Commands::Health {
                    data_dir,
                    robot_meta,
                    stale_threshold,
                    binary_only,
                    json,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_health(
                        &data_dir,
                        cli.db.clone(),
                        structured_format,
                        stale_threshold,
                        binary_only,
                        robot_meta,
                    )?;
                }
                Commands::Selftest { json } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_selftest(structured_format)?;
                }
                Commands::Onboarding { data_dir, json } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_onboarding(&data_dir, cli.db.clone(), structured_format)?;
                }
                cmd @ Commands::Doctor { .. } => {
                    execute_doctor_command(cmd, cli, wrap)?;
                }
                Commands::Context {
                    path,
                    source,
                    data_dir,
                    limit,
                    json,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_context(
                        &path,
                        source.as_deref(),
                        &data_dir,
                        cli.db.clone(),
                        structured_format,
                        limit,
                    )?;
                }
                Commands::Sessions {
                    workspace,
                    current,
                    limit,
                    since,
                    json,
                    data_dir,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_sessions(
                        workspace.as_ref(),
                        current,
                        limit,
                        since.as_deref(),
                        &data_dir,
                        cli.db.clone(),
                        structured_format,
                    )?;
                }
                Commands::Resume {
                    path,
                    agent,
                    exec,
                    shell,
                    json,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_resume(&path, agent.as_deref(), exec, shell, structured_format)?;
                }
                Commands::Export {
                    path,
                    source,
                    conversation_id,
                    format,
                    output,
                    clipboard,
                    include_tools,
                    include_skills,
                    include_secrets,
                } => {
                    run_export(
                        &path,
                        cli.db.clone(),
                        source.as_deref(),
                        conversation_id,
                        format,
                        output.as_deref(),
                        clipboard,
                        include_tools,
                        include_skills,
                        include_secrets,
                    )?;
                }
                Commands::ExportHtml {
                    session,
                    source,
                    conversation_id,
                    output_dir,
                    filename,
                    include_tools,
                    show_timestamps,
                    include_skills,
                    include_secrets,
                    theme,
                    dry_run,
                    explain,
                    open,
                    json,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_export_html(
                        &session,
                        cli.db.clone(),
                        source.as_deref(),
                        conversation_id,
                        output_dir.as_deref(),
                        filename.as_deref(),
                        include_tools,
                        show_timestamps,
                        include_skills,
                        include_secrets,
                        &theme,
                        dry_run,
                        explain,
                        open,
                        structured_format,
                    )?;
                }
                Commands::Expand {
                    path,
                    source,
                    message_index,
                    conversation_id,
                    line,
                    context,
                    json,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    followup_coordinates::run_expand(
                        &path,
                        cli.db.clone(),
                        source.as_deref(),
                        line,
                        context,
                        structured_format,
                        message_index,
                        conversation_id,
                    )?;
                }
                Commands::Timeline {
                    since,
                    until,
                    today,
                    agent,
                    data_dir,
                    json,
                    group_by,
                    source,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_timeline(
                        since.as_deref(),
                        until.as_deref(),
                        today,
                        &agent,
                        &data_dir,
                        cli.db.clone(),
                        structured_format,
                        group_by,
                        source,
                    )?;
                }
                Commands::Quarantine(subcmd) => {
                    run_quarantine_command(subcmd, cli)?;
                }
                Commands::Forget {
                    source_glob,
                    workspace_glob,
                    older_than,
                    list,
                    reinclude,
                    source_path,
                    source_id,
                    agent,
                    external_id,
                    db,
                    apply,
                    json,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_forget_command(
                        ForgetCommandOptions {
                            source_glob,
                            workspace_glob,
                            older_than,
                            list,
                            reinclude,
                            source_path,
                            source_id,
                            agent,
                            external_id,
                            db,
                            apply,
                        },
                        cli,
                        structured_format,
                    )?;
                }
                Commands::Mirror(subcmd) => {
                    run_mirror_command(subcmd, cli)?;
                }
                Commands::Sources(subcmd) => {
                    run_sources_command(subcmd, cli)?;
                }
                Commands::Models(subcmd) => {
                    let subcmd = subcmd.clone();
                    let cli_clone = cli.clone();
                    let result = asupersync::runtime::spawn_blocking(move || {
                        run_models_command(subcmd, &cli_clone)
                    })
                    .await;
                    result?;
                }
                Commands::Import(subcmd) => {
                    handle_import(subcmd, cli).await?;
                }
                #[cfg(unix)]
                Commands::Daemon {
                    socket,
                    idle_timeout,
                    max_connections,
                    data_dir,
                } => {
                    run_daemon(socket, idle_timeout, max_connections, data_dir)?;
                }
                _ => {}
            }
        }
    }

    Ok(())
}
