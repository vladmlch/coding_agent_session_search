use crate::cli::args::{Cli, Commands};
use crate::cli::args_sub::ColorPref;
use crate::cli::args_sub::PagesSubcommand;
use crate::cli::commands::analytics::run_analytics::run_analytics;
use crate::cli::commands::capabilities_introspect::capabilitiesresponse::{
    ConfigExportSecretInputs, run_capabilities, run_config_based_export, run_introspect,
    staged_secret_scan_json,
};
use crate::cli::commands::capabilities_introspect::run_api_version::{
    cli_command, run_api_version,
};
use crate::cli::commands::daemon::run_daemon;
use crate::cli::commands::export::indexed_export_cli_error::run_export;
use crate::cli::commands::export_html::run_export_html;
use crate::cli::commands::forget::command::{ForgetCommandOptions, run_forget_command};
use crate::cli::commands::index::run_lexical_reconcile_cli::{
    run_index_with_data, run_lexical_gc_cli, run_lexical_reconcile_cli,
};
use crate::cli::commands::models::run_models_command::run_models_command;
use crate::cli::commands::pack::pack_budget_ms::run_cli_pack;
use crate::cli::commands::quarantine_mirror_import::handle_import;
use crate::cli::commands::quarantine_mirror_import::{run_mirror_command, run_quarantine_command};
use crate::cli::commands::resume::run_resume;
use crate::cli::commands::run_onboarding_interactive_wizard::{run_guide, run_onboarding};
use crate::cli::commands::schedule::run_schedule_command;
use crate::cli::commands::search::fieldbudgets::robot_format_from_env;
use crate::cli::commands::search::resolve_search_defaults::resolve_search_defaults;
use crate::cli::commands::search::run_cli_search::run_cli_search;
use crate::cli::commands::search::timefilter::semantic_daemon_policy;
use crate::cli::commands::search::timefilter::{SemanticSearchOptions, TimeFilter};
use crate::cli::commands::search::toon_encode_options_from_env::output_structured_value;
use crate::cli::commands::sessions_context::{run_context, run_sessions};
use crate::cli::commands::sources::run_sources_command::run_sources_command;
use crate::cli::commands::stats_diag_storage::run_diag::run_diag;
use crate::cli::commands::stats_diag_storage::run_stats::{run_dedup, run_stats, run_storage};
use crate::cli::commands::status_health::probe_daemon_runtime_for_diagnostics::{
    run_selftest, run_status, run_support_bundle, run_triage,
};
use crate::cli::commands::status_health::run_health::run_health;
use crate::cli::commands::timeline::run_timeline;
use crate::cli::commands::view::role_to_export_string::ViewWindow;
use crate::cli::error::{CliError, CliResult, ProgressResolved, WrapConfig};
use crate::cli::output::default_dep_log_suppression::build_robot_aware_log_filter;
use crate::cli::output::default_dep_log_suppression::init_cli_tracing;
use crate::cli::output::describe_command::resolve_subcommand_structured_format;
use crate::cli::output::emit_correction_notes::is_robot_mode;
use crate::cli::output::extract_search_terms::{print_robot_docs, print_robot_help};
use crate::cli::output::warn_tui_terminal_profile::prepare_headless_once_tui_artifacts;
use crate::cli::output::warn_tui_terminal_profile::warn_tui_terminal_profile;
use crate::cli::paths::default_db_path;
use crate::cli::paths::resolve_data_dir;
use crate::cli::state::state_meta_json::maybe_auto_refresh_index_after_read;
use crate::doctor::archive::export::{
    DoctorArchiveExportCommandMode, DoctorArchiveExportRequest, DoctorArchiveExportWorkflow,
};
use crate::doctor::backup_restore::validate_doctor_diagnostic_read_only_controls;
use crate::doctor::baseline::DoctorBaselineCommandMode;
use crate::doctor::baseline::run_doctor_baseline_impl;
use crate::doctor::run_doctor_ls::{
    run_doctor_archive_export_impl, run_doctor_diff, run_doctor_emit_capabilities,
    run_doctor_explain, run_doctor_gc, run_doctor_ls, run_doctor_robot_triage, run_doctor_undo,
    run_doctor_watch,
};
use crate::doctor::support_bundle::capture::run_doctor_support_bundle_impl;
use crate::doctor::support_bundle::{DoctorSupportBundleCommandMode, DoctorSupportBundleRequest};
use crate::indexer::stall_watchdog::refresh_index_inline;
use crate::model::cli_error_kind::ErrorKind as CliErrorKind;
use crate::{bookmarks, doctor, doctor_recover, followup_coordinates, indexer, tui_asciicast, ui};
use std::path::PathBuf;
use tracing::info;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub(crate) async fn execute_cli(
    cli: &Cli,
    wrap: WrapConfig,
    progress: ProgressResolved,
    stdout_is_tty: bool,
    stderr_is_tty: bool,
) -> CliResult<()> {
    let command = cli.command.clone().unwrap_or_else(|| {
        if cli.robot_format.is_some() || robot_format_from_env().is_some() {
            Commands::Triage {
                data_dir: None,
                json: true,
                stale_threshold: 1800,
                timeout: None,
            }
        } else {
            Commands::Tui {
                once: false,
                reset_state: false,
                asciicast: None,
                data_dir: None,
                inline: false,
                ui_height: 12,
                anchor: "bottom".to_string(),
                record_macro: None,
                play_macro: None,
                refresh: false,
            }
        }
    });

    if cli.robot_help {
        print_robot_help(wrap)?;
        return Ok(());
    }

    if let Commands::RobotDocs { topic } = command.clone() {
        print_robot_docs(topic, wrap)?;
        return Ok(());
    }

    // TUI preflight: call out env profiles that commonly make the UI look
    // degraded (global no-color, conservative TERM profiles).
    if matches!(command, Commands::Tui { .. }) {
        warn_tui_terminal_profile(stderr_is_tty);
    }

    // Block TUI in non-TTY contexts unless TUI_HEADLESS is set (for testing)
    if matches!(command, Commands::Tui { .. })
        && !stdout_is_tty
        && dotenvy::var("TUI_HEADLESS").is_err()
    {
        return Err(CliError::usage(
            "No subcommand provided; in non-TTY contexts TUI is disabled.",
            Some("Use an explicit subcommand, e.g., `cass search --json ...` or `cass --robot-help`.".to_string()),
        ));
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
        Commands::Tui { data_dir, .. } => {
            let log_dir = resolve_data_dir(data_dir, cli.db.as_ref());
            std::fs::create_dir_all(&log_dir).ok();

            let file_appender = tracing_appender::rolling::daily(&log_dir, "cass.log");
            let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

            tracing_subscriber::registry()
                .with(filter)
                .with(
                    tracing_subscriber::fmt::layer()
                        .with_writer(non_blocking)
                        .compact()
                        .with_target(false)
                        .with_ansi(false),
                )
                .init();

            // No update check anywhere: the checker and its TUI banner were
            // removed by the local-only conversion. The historical pre-TUI
            // check awaited the network and then blocked on a stdin prompt
            // before the first frame (2l1b0.56).
            if let Commands::Tui {
                once,
                reset_state,
                asciicast,
                data_dir,
                inline,
                ui_height,
                anchor,
                record_macro,
                play_macro,
                refresh,
            } = command.clone()
            {
                let tui_data_dir = resolve_data_dir(&data_dir, cli.db.as_ref());
                let tui_db_path = cli
                    .db
                    .clone()
                    .unwrap_or_else(|| tui_data_dir.join("agent_search.db"));
                if refresh {
                    refresh_index_inline(cli.db.clone(), Some(tui_data_dir.clone()));
                } else if let Some(outcome) =
                    maybe_auto_refresh_index_after_read(&tui_data_dir, &tui_db_path, None)
                {
                    // Stale-on-launch catch-up runs detached; the TUI opens
                    // immediately on the existing index and can reload
                    // (Ctrl+Shift+R / command palette) once it lands.
                    info!(?outcome, "tui: auto-refresh evaluated");
                }
                info!(once, inline, ui_height, %anchor, record_macro = ?record_macro, play_macro = ?play_macro, "launching ftui runtime");

                let inline_config = if inline {
                    let ui_anchor = if anchor == "top" {
                        ui::ftui_adapter::UiAnchor::Top
                    } else {
                        ui::ftui_adapter::UiAnchor::Bottom
                    };
                    Some(ui::app::InlineTuiConfig {
                        ui_height,
                        anchor: ui_anchor,
                    })
                } else {
                    None
                };

                if reset_state {
                    let state_path = tui_data_dir.join("tui_state.json");
                    match std::fs::remove_file(&state_path) {
                        Ok(()) => {}
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                        Err(e) => {
                            return Err(CliError {
                                code: 9,
                                kind: CliErrorKind::TuiResetState.kind_str(),
                                message: format!(
                                    "failed to remove persisted state {}: {e}",
                                    state_path.display()
                                ),
                                hint: Some(
                                    "Check file permissions or rerun without --reset-state."
                                        .to_string(),
                                ),
                                retryable: false,
                            });
                        }
                    }
                }

                let non_tty_headless_once =
                    once && !inline && !stdout_is_tty && dotenvy::var("TUI_HEADLESS").is_ok();

                if non_tty_headless_once {
                    prepare_headless_once_tui_artifacts(
                        &tui_data_dir,
                        &tui_db_path,
                        asciicast.as_deref(),
                    )
                    .map_err(|e| CliError {
                        code: 9,
                        kind: CliErrorKind::TuiHeadlessOnce.kind_str(),
                        message: format!(
                            "headless --once TUI bootstrap failed for {}: {e}",
                            tui_data_dir.display()
                        ),
                        hint: Some(
                            "Ensure the data directory is writable and retry the command."
                                .to_string(),
                        ),
                        retryable: false,
                    })?;
                    info!(
                        data_dir = %tui_data_dir.display(),
                        asciicast = ?asciicast,
                        "completed non-interactive headless --once TUI bootstrap"
                    );
                } else {
                    let macro_config = ui::app::MacroConfig {
                        record_path: record_macro,
                        play_path: play_macro,
                    };
                    let run_result = if let Some(path) = asciicast {
                        tui_asciicast::run_tui_with_asciicast(&path, !once)
                    } else {
                        ui::app::run_tui_ftui(
                            inline_config,
                            macro_config,
                            Some(tui_data_dir),
                            Some(tui_db_path),
                        )
                    };

                    if let Err(e) = run_result {
                        return Err(CliError {
                            code: 9,
                            kind: CliErrorKind::Tui.kind_str(),
                            message: format!("tui failed: {e}"),
                            hint: None,
                            retryable: false,
                        });
                    }
                }
            }
        }
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
        | Commands::Pages { .. }
        | Commands::Analytics(..) => {
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
                Commands::Pages {
                    subcommand,
                    export_only,
                    verify,
                    agents,
                    workspaces,
                    since,
                    until,
                    path_mode,
                    share_profile,
                    target,
                    dry_run,
                    scan_secrets,
                    fail_on_secrets,
                    secrets_allow,
                    secrets_deny,
                    verbose,
                    no_encryption,
                    i_understand_unencrypted_risks,
                    preview,
                    port,
                    no_open,
                    config,
                    validate_config,
                    example_config,
                    json,
                } => {
                    // `cass pages key …` operates on an exported bundle and
                    // never touches the export/wizard flags (WS-G.4).
                    if let Some(PagesSubcommand::Key(args)) = subcommand {
                        return crate::pages::key_cli::run_pages_key_command(&args);
                    }
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    let robot_mode_here = structured_format.is_some() || robot_mode;
                    // Handle --example-config (show example config and exit)
                    if example_config {
                        println!("{}", crate::pages::config_input::example_config());
                        return Ok(());
                    }

                    // Handle --config based export
                    if let Some(ref config_path) = config {
                        let mut pages_config = crate::pages::config_input::PagesConfig::load(
                            config_path,
                        )
                        .map_err(|e| CliError {
                            code: 2,
                            kind: CliErrorKind::Pages.kind_str(),
                            message: format!("Failed to load config: {e}"),
                            hint: Some(
                                "Check config file syntax with --example-config".to_string(),
                            ),
                            retryable: false,
                        })?;

                        // Only `local` reaches this point: the removed-flag
                        // table rejects `--target github|cloudflare` before
                        // clap parses, and the hosted flags are gone entirely.
                        if let Some(target) = target {
                            pages_config.deployment.target = target.as_config_value().to_string();
                        }
                        if let Some(profile) = share_profile {
                            pages_config.bundle.share_profile = Some(profile);
                        }

                        // Resolve environment variables
                        pages_config.resolve_env_vars().map_err(|e| CliError {
                            code: 2,
                            kind: CliErrorKind::Pages.kind_str(),
                            message: format!("Failed to resolve env vars: {e}"),
                            hint: Some(
                                "Ensure referenced environment variables are set".to_string(),
                            ),
                            retryable: false,
                        })?;

                        // Validate configuration
                        let validation = pages_config.validate();

                        if validate_config {
                            // Just validate and output result
                            println!(
                                "{}",
                                serde_json::to_string_pretty(&validation).unwrap_or_default()
                            );
                            if !validation.valid {
                                return Err(CliError {
                                    code: 2,
                                    kind: CliErrorKind::Pages.kind_str(),
                                    message: "Configuration validation failed".to_string(),
                                    hint: Some("Review errors in JSON output".to_string()),
                                    retryable: false,
                                });
                            }
                            return Ok(());
                        }

                        if !validation.valid {
                            if let Some(fmt) = structured_format {
                                output_structured_value(
                                    serde_json::to_value(&validation).unwrap_or_default(),
                                    fmt,
                                )?;
                            } else if robot_mode_here {
                                eprintln!("Configuration errors:");
                                for err in &validation.errors {
                                    eprintln!("  - {}", err);
                                }
                            }
                            return Err(CliError {
                                code: 2,
                                kind: CliErrorKind::Pages.kind_str(),
                                message: "Configuration validation failed".to_string(),
                                hint: Some("Fix errors listed above".to_string()),
                                retryable: false,
                            });
                        }

                        // Print warnings
                        if !validation.warnings.is_empty()
                            && structured_format.is_none()
                            && !robot_mode_here
                        {
                            eprintln!("Warnings:");
                            for warn in &validation.warnings {
                                eprintln!("  - {}", warn);
                            }
                            eprintln!();
                        }

                        // Get database path
                        let db_path = cli.db.clone().unwrap_or_else(default_db_path);

                        // Convert config to WizardState and run export
                        let wizard_state =
                            pages_config.to_wizard_state(db_path.clone()).map_err(|e| {
                                CliError {
                                    code: 9,
                                    kind: CliErrorKind::Pages.kind_str(),
                                    message: format!("Failed to create wizard state: {e}"),
                                    hint: None,
                                    retryable: false,
                                }
                            })?;

                        // Run the export using the config
                        run_config_based_export(
                            &pages_config,
                            &wizard_state,
                            &db_path,
                            dry_run,
                            structured_format,
                            verbose,
                            ConfigExportSecretInputs {
                                allow: &secrets_allow,
                                deny: &secrets_deny,
                            },
                        )
                        .map_err(|e| CliError {
                            code: 9,
                            kind: CliErrorKind::Pages.kind_str(),
                            message: format!("Export failed: {e:#}"),
                            hint: None,
                            retryable: false,
                        })?;

                        return Ok(());
                    }

                    // Handle --validate-config without --config
                    if validate_config {
                        return Err(CliError {
                            code: 2,
                            kind: CliErrorKind::Pages.kind_str(),
                            message: "--validate-config requires --config".to_string(),
                            hint: Some("Use --config <path> --validate-config".to_string()),
                            retryable: false,
                        });
                    }

                    // Handle --preview first (starts local preview server)
                    if let Some(preview_dir) = preview {
                        let config = crate::pages::preview::PreviewConfig {
                            site_dir: preview_dir,
                            port,
                            open_browser: !no_open,
                        };
                        crate::pages::preview::start_preview_server(config)
                            .await
                            .map_err(|e| CliError {
                                code: 9,
                                kind: CliErrorKind::Pages.kind_str(),
                                message: format!("Preview server failed: {e}"),
                                hint: Some(
                                    "Check that the directory exists and port is available"
                                        .to_string(),
                                ),
                                retryable: false,
                            })?;
                        return Ok(());
                    }

                    // Check for unencrypted export in robot mode
                    if no_encryption && robot_mode_here && !i_understand_unencrypted_risks {
                        let error = crate::pages::confirmation::robot_mode_blocked_error();
                        eprintln!(
                            "{}",
                            serde_json::to_string_pretty(&error).unwrap_or_default()
                        );
                        return Err(CliError {
                            code: crate::pages::confirmation::EXIT_CODE_UNENCRYPTED_NOT_CONFIRMED,
                            kind: CliErrorKind::Pages.kind_str(),
                            message: "Unencrypted exports are not allowed in robot mode"
                                .to_string(),
                            hint: Some(
                                "Use --i-understand-unencrypted-risks flag if you really need this"
                                    .to_string(),
                            ),
                            retryable: false,
                        });
                    }
                    // Handle --verify first
                    if let Some(verify_path) = verify {
                        let result = crate::pages::verify::verify_bundle(&verify_path, verbose)
                            .map_err(|e| CliError {
                                code: 9,
                                kind: CliErrorKind::Pages.kind_str(),
                                message: format!("Verification failed: {e}"),
                                hint: None,
                                retryable: false,
                            })?;

                        if let Some(fmt) = structured_format {
                            output_structured_value(
                                serde_json::to_value(&result).unwrap_or_default(),
                                fmt,
                            )?;
                        } else {
                            crate::pages::verify::print_result(&result, verbose);
                        }

                        // Exit with non-zero if invalid
                        if result.status != "valid" {
                            if structured_format.is_some() {
                                return Err(CliError::already_reported(
                                    1,
                                    CliErrorKind::Pages.kind_str(),
                                    false,
                                ));
                            }
                            return Err(CliError {
                                code: 1,
                                kind: CliErrorKind::Pages.kind_str(),
                                message: "Bundle verification failed".to_string(),
                                hint: Some(
                                    "Run with --verbose for detailed error information".to_string(),
                                ),
                                retryable: false,
                            });
                        }
                    } else if scan_secrets {
                        let db_path = cli.db.clone().unwrap_or_else(default_db_path);

                        let workspaces_path = workspaces
                            .clone()
                            .map(|ws| ws.into_iter().map(PathBuf::from).collect());

                        let filters = crate::pages::secret_scan::SecretScanFilters {
                            agents: agents.clone(),
                            workspaces: workspaces_path,
                            since_ts: since
                                .as_deref()
                                .and_then(crate::ui::time_parser::parse_time_input),
                            until_ts: until
                                .as_deref()
                                .and_then(crate::ui::time_parser::parse_time_input),
                        };

                        let config = crate::pages::secret_scan::SecretScanConfig::from_inputs(
                            &secrets_allow,
                            &secrets_deny,
                        )
                        .map_err(|e| CliError {
                            code: 9,
                            kind: CliErrorKind::Pages.kind_str(),
                            message: format!("Secret scan config error: {e}"),
                            hint: None,
                            retryable: false,
                        })?;

                        crate::pages::secret_scan::run_secret_scan_cli(
                            &db_path,
                            &filters,
                            &config,
                            structured_format.is_some(),
                            fail_on_secrets,
                        )
                        .map_err(|e| CliError {
                            code: 9,
                            kind: CliErrorKind::Pages.kind_str(),
                            message: format!("Secret scan failed: {e}"),
                            hint: None,
                            retryable: false,
                        })?;
                    } else if let Some(output_path) = export_only {
                        if dry_run {
                            if let Some(fmt) = structured_format {
                                output_structured_value(
                                    serde_json::json!({
                                        "status": "dry_run",
                                        "output_path": output_path,
                                        "secret_scan": {
                                            "performed": false,
                                            "complete": false,
                                            "approval_state": "not_performed_dry_run",
                                            "artifact_sha256": null,
                                        },
                                    }),
                                    fmt,
                                )?;
                            } else {
                                println!("Dry run: would export to {:?}", output_path);
                                println!("Secret scan: not performed during dry run");
                            }
                            return Ok(());
                        }

                        // Interactive unencrypted export confirmation (non-robot mode)
                        if no_encryption && structured_format.is_none() && !robot_mode_here {
                            use console::style;
                            use std::io::Write;

                            eprintln!("{}", style("⚠️  SECURITY WARNING").red().bold());
                            eprintln!();
                            for line in crate::pages::confirmation::unencrypted_warning_lines() {
                                eprintln!("{}", line);
                            }
                            eprintln!();
                            eprintln!("To proceed, type exactly:");
                            eprintln!();
                            eprintln!(
                                "  {}",
                                style(crate::pages::confirmation::UNENCRYPTED_ACK_PHRASE).cyan()
                            );
                            eprintln!();
                            eprint!("Your input: ");
                            std::io::stderr().flush().ok();

                            let mut input = String::new();
                            std::io::stdin().read_line(&mut input).map_err(|e| CliError {
                                code: crate::pages::confirmation::EXIT_CODE_UNENCRYPTED_NOT_CONFIRMED,
                                kind: CliErrorKind::Pages.kind_str(),
                                message: format!("Failed to read input: {e}"),
                                hint: None,
                                retryable: false,
                            })?;

                            match crate::pages::confirmation::validate_unencrypted_ack(&input) {
                                crate::pages::confirmation::StepValidation::Passed => {
                                    // Additional y/N confirmation
                                    eprintln!();
                                    eprint!("Are you ABSOLUTELY SURE? [y/N]: ");
                                    std::io::stderr().flush().ok();

                                    let mut confirm = String::new();
                                    std::io::stdin().read_line(&mut confirm).map_err(|e| CliError {
                                        code: crate::pages::confirmation::EXIT_CODE_UNENCRYPTED_NOT_CONFIRMED,
                                        kind: CliErrorKind::Pages.kind_str(),
                                        message: format!("Failed to read input: {e}"),
                                        hint: None,
                                        retryable: false,
                                    })?;

                                    if confirm.trim().to_lowercase() != "y" {
                                        eprintln!();
                                        eprintln!("{}", style("Export cancelled.").green());
                                        eprintln!(
                                            "To export with encryption (recommended), remove --no-encryption"
                                        );
                                        return Err(CliError {
                                            code: crate::pages::confirmation::EXIT_CODE_UNENCRYPTED_NOT_CONFIRMED,
                                            kind: CliErrorKind::Pages.kind_str(),
                                            message: "Unencrypted export not confirmed".to_string(),
                                            hint: Some("Remove --no-encryption to export with encryption (recommended)".to_string()),
                                            retryable: false,
                                        });
                                    }
                                }
                                crate::pages::confirmation::StepValidation::Failed(msg) => {
                                    eprintln!();
                                    eprintln!("{}", style("Export cancelled.").green());
                                    eprintln!("{}", msg);
                                    return Err(CliError {
                                        code: crate::pages::confirmation::EXIT_CODE_UNENCRYPTED_NOT_CONFIRMED,
                                        kind: CliErrorKind::Pages.kind_str(),
                                        message: "Unencrypted export not confirmed".to_string(),
                                        hint: Some("Remove --no-encryption to export with encryption (recommended)".to_string()),
                                        retryable: false,
                                    });
                                }
                            }
                        }

                        let scan_config = crate::pages::secret_scan::SecretScanConfig::from_inputs(
                            &secrets_allow,
                            &secrets_deny,
                        )
                        .map_err(|e| CliError {
                            code: 9,
                            kind: CliErrorKind::Pages.kind_str(),
                            message: format!("Secret scan config error: {e}"),
                            hint: None,
                            retryable: false,
                        })?;
                        let structured_export = structured_format.is_some() || robot_mode_here;
                        let (stats, (staged_secret_scan, approval_state)) =
                            crate::pages::export::export_pages_database_verified(
                                cli.db.clone(),
                                output_path.clone(),
                                agents.clone(),
                                workspaces.clone(),
                                since.clone(),
                                until.clone(),
                                path_mode,
                                // --export-only writes a plaintext database.
                                share_profile.unwrap_or(
                                    crate::pages::profiles::ShareProfile::default_for(false),
                                ),
                                |_current, _total| {},
                                |staged_db_path| {
                                    let scan = crate::pages::secret_scan::scan_staged_export_database(
                                        staged_db_path,
                                        &scan_config,
                                    )?;
                                    if scan.report.summary.total == 0 {
                                        return Ok((scan, "clean"));
                                    }
                                    if structured_export || fail_on_secrets {
                                        anyhow::bail!(
                                            "Staged export contains {} potential secret(s); robot/CI export-only runs fail closed",
                                            scan.report.summary.total
                                        );
                                    }

                                    let mut term = console::Term::stderr();
                                    crate::pages::secret_scan::print_human_report(
                                        &mut term,
                                        &scan.report,
                                        3,
                                    )?;
                                    eprintln!(
                                        "The report above describes the exact staged export database."
                                    );
                                    eprint!(
                                        "Type exactly \"I understand the risks\" to publish it: "
                                    );
                                    use std::io::Write;
                                    std::io::stderr().flush()?;
                                    let mut acknowledgment = String::new();
                                    std::io::stdin().read_line(&mut acknowledgment)?;
                                    if acknowledgment.trim() != "I understand the risks" {
                                        anyhow::bail!(
                                            "Staged export secret findings were not acknowledged"
                                        );
                                    }
                                    Ok((scan, "explicit_interactive_acknowledgment"))
                                },
                            )
                            .map_err(|e| CliError {
                                code: 9,
                                kind: CliErrorKind::Pages.kind_str(),
                                message: format!("Export failed: {e:#}"),
                                hint: Some(
                                    "Review findings with `cass pages --scan-secrets`; use --secrets-allow only for confirmed false positives."
                                        .to_string(),
                                ),
                                retryable: false,
                            })?;

                        if let Some(fmt) = structured_format {
                            output_structured_value(
                                serde_json::json!({
                                    "status": "success",
                                    "output_path": output_path,
                                    "stats": {
                                        "conversations": stats.conversations_processed,
                                        "messages": stats.messages_processed,
                                    },
                                    "secret_scan": staged_secret_scan_json(
                                        &staged_secret_scan,
                                        approval_state,
                                    ),
                                }),
                                fmt,
                            )?;
                        } else {
                            println!(
                                "Export complete! Processed {} conversations, {} messages.",
                                stats.conversations_processed, stats.messages_processed
                            );
                            println!(
                                "Secret scan: complete ({approval_state}); artifact SHA-256 {}",
                                staged_secret_scan.artifact_sha256
                            );
                        }
                    } else {
                        // `--target` may only be `local` here (hosted values are
                        // rejected by the removed-flag table); it needs no wiring.
                        let _ = target;

                        // Wizard mode: pass flags
                        let mut wizard = crate::pages::wizard::PagesWizard::new();
                        if let Some(db_path) = cli.db.clone() {
                            wizard.set_db_path(db_path);
                        }
                        if no_encryption {
                            wizard.set_no_encryption(true);
                        }
                        if let Some(profile) = share_profile {
                            wizard.set_share_profile(profile);
                        }
                        wizard.run().map_err(|e| CliError {
                            code: 9,
                            kind: CliErrorKind::Pages.kind_str(),
                            message: format!("Wizard failed: {e:#}"),
                            hint: None,
                            retryable: false,
                        })?;
                    }
                }
                Commands::Analytics(subcmd) => {
                    run_analytics(subcmd, cli.db.clone(), cli)?;
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
                Commands::Guide {
                    intent,
                    fixture,
                    data_dir,
                    apply,
                    confirm_step,
                    confirm_fact,
                    accept_privacy_tier,
                    accept_cost_risk,
                    confirm_stop_conditions_clear,
                    json,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_guide(
                        &intent,
                        fixture.as_deref(),
                        &data_dir,
                        cli.db.clone(),
                        structured_format,
                        apply,
                        &confirm_step,
                        &confirm_fact,
                        accept_privacy_tier.as_deref(),
                        accept_cost_risk.as_deref(),
                        confirm_stop_conditions_clear,
                    )?;
                }
                Commands::Doctor {
                    data_dir,
                    json,
                    check,
                    fix,
                    repair,
                    cleanup,
                    archive_scan,
                    archive_normalize,
                    backups_list,
                    backups_verify,
                    backups_restore,
                    baseline_save,
                    baseline_diff,
                    baseline_update,
                    support_bundle,
                    support_bundle_verify,
                    archive_export,
                    archive_relocate,
                    archive_export_verify,
                    archive_target,
                    backup_id,
                    baseline_id,
                    baseline_path,
                    support_bundle_path,
                    include_sensitive_attachments,
                    sensitive_attachment,
                    sensitive_attachment_max_bytes,
                    dry_run,
                    yes,
                    plan_fingerprint,
                    verbose,
                    force_rebuild,
                    allow_repeated_repair,
                    ls,
                    undo,
                    robot_triage,
                    diff,
                    gc_before,
                    watch,
                    watch_interval_ms,
                    watch_iterations,
                    explain,
                    emit_capabilities,
                    recover_from_archive,
                    rebuild_canonical_fts,
                    repair_leaked_pages,
                    cleanup_interrupted_artifacts,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    // Pass-12 fix (P2): the new pass-1+ flags have no clap-level
                    // `conflicts_with` wiring, so passing multiple at once
                    // would silently dispatch only the first match. That made
                    // `cass doctor --fix --ls` silently drop `--fix`. Detect
                    // and refuse multiple new flags at the dispatch boundary
                    // so agents get a clean usage error rather than silent
                    // precedence behavior.
                    let pass1plus_flag_count = [
                        explain.is_some(),
                        emit_capabilities,
                        ls,
                        undo.is_some(),
                        robot_triage,
                        diff.is_some(),
                        gc_before.is_some(),
                        watch,
                    ]
                    .iter()
                    .filter(|x| **x)
                    .count();
                    if pass1plus_flag_count > 1 {
                        return Err(CliError {
                            code: 2,
                            kind: "usage",
                            message:
                                "multiple world-class-doctor flags specified; only one allowed per invocation"
                                    .to_string(),
                            hint: Some(
                                "Pass exactly one of --ls, --undo, --robot-triage, --diff, --gc-before, --watch, --explain, --emit-capabilities at a time."
                                    .to_string(),
                            ),
                            retryable: false,
                        });
                    }
                    // World-class-doctor pass-8: `cass doctor --explain <run-id>`.
                    if let Some(run_id_arg) = explain {
                        run_doctor_explain(data_dir, &run_id_arg, structured_format)?;
                        return Ok(());
                    }
                    // World-class-doctor pass-8: `cass doctor --emit-capabilities`.
                    if emit_capabilities {
                        run_doctor_emit_capabilities(structured_format)?;
                        return Ok(());
                    }
                    // #285: `cass doctor --recover-from-archive <DIR>` — rebuild
                    // the source JSONL tree from the canonical archive's
                    // preserved events so a corrupt archive can be re-ingested.
                    if let Some(target_dir) = recover_from_archive {
                        doctor_recover::run_doctor_recover_from_archive(
                            data_dir,
                            cli.db.clone(),
                            target_dir,
                            structured_format,
                        )?;
                        return Ok(());
                    }
                    // #285: `cass doctor --rebuild-canonical-fts --yes` — drop
                    // and rebuild the canonical FTS5 shadow tables in place.
                    if rebuild_canonical_fts {
                        doctor_recover::run_doctor_rebuild_canonical_fts(
                            data_dir,
                            cli.db.clone(),
                            dry_run,
                            yes,
                            structured_format,
                        )?;
                        return Ok(());
                    }
                    // 2l1b0.73: `cass doctor --repair-leaked-pages --yes` —
                    // free pages no tree or freelist owns, in place.
                    if repair_leaked_pages {
                        doctor_recover::run_doctor_repair_leaked_pages(
                            data_dir,
                            cli.db.clone(),
                            dry_run,
                            yes,
                            structured_format,
                        )?;
                        return Ok(());
                    }
                    // #285: `cass doctor --cleanup-interrupted-artifacts --yes`
                    // — quarantine interrupted raw_mirror_capture artifacts.
                    if cleanup_interrupted_artifacts {
                        doctor_recover::run_doctor_cleanup_interrupted_artifacts(
                            data_dir,
                            yes,
                            structured_format,
                        )?;
                        return Ok(());
                    }
                    // World-class-doctor pass-2: read-only `cass doctor --ls`.
                    if ls {
                        run_doctor_ls(data_dir, structured_format)?;
                        return Ok(());
                    }
                    // World-class-doctor pass-2: `cass doctor --undo <run-id>`.
                    if let Some(run_id_arg) = undo {
                        run_doctor_undo(data_dir, &run_id_arg, structured_format)?;
                        return Ok(());
                    }
                    // World-class-doctor pass-3: `cass doctor --robot-triage`.
                    if robot_triage {
                        run_doctor_robot_triage(data_dir, structured_format)?;
                        return Ok(());
                    }
                    // World-class-doctor pass-3: `cass doctor --diff [<REF>]`.
                    if let Some(diff_ref) = diff {
                        run_doctor_diff(data_dir, &diff_ref, structured_format)?;
                        return Ok(());
                    }
                    // World-class-doctor pass-3: `cass doctor --gc-before <ISO> --yes`.
                    if let Some(before_iso) = gc_before {
                        run_doctor_gc(data_dir, &before_iso, yes, structured_format)?;
                        return Ok(());
                    }
                    // World-class-doctor pass-6: `cass doctor --watch`.
                    if watch {
                        run_doctor_watch(
                            data_dir,
                            watch_interval_ms,
                            watch_iterations,
                            structured_format,
                        )?;
                        return Ok(());
                    }
                    if archive_export || archive_relocate || archive_export_verify {
                        run_doctor_archive_export_impl(
                            DoctorArchiveExportRequest {
                                data_dir_override: data_dir,
                                db_override: cli.db.clone(),
                                output_format: structured_format,
                                workflow: if archive_relocate {
                                    DoctorArchiveExportWorkflow::Relocate
                                } else {
                                    DoctorArchiveExportWorkflow::Export
                                },
                                command_mode: if archive_export_verify {
                                    DoctorArchiveExportCommandMode::Verify
                                } else if yes {
                                    DoctorArchiveExportCommandMode::Apply
                                } else {
                                    DoctorArchiveExportCommandMode::Plan
                                },
                                target_root: archive_target,
                                dry_run,
                                yes,
                                plan_fingerprint,
                                backup_id,
                                force_rebuild,
                                allow_repeated_repair,
                            },
                            wrap,
                        )?;
                    } else if support_bundle || support_bundle_verify {
                        validate_doctor_diagnostic_read_only_controls(
                            "support-bundle",
                            &backup_id,
                            dry_run,
                            yes,
                            plan_fingerprint.as_deref(),
                            force_rebuild,
                            allow_repeated_repair,
                        )?;
                        let mode = if support_bundle_verify {
                            DoctorSupportBundleCommandMode::Verify
                        } else {
                            DoctorSupportBundleCommandMode::Create
                        };
                        run_doctor_support_bundle_impl(
                            DoctorSupportBundleRequest {
                                data_dir_override: data_dir,
                                db_override: cli.db.clone(),
                                output_format: structured_format,
                                mode,
                                baseline_id,
                                baseline_path,
                                support_bundle_path,
                                include_sensitive_attachments,
                                sensitive_attachments: sensitive_attachment,
                                sensitive_attachment_max_bytes,
                            },
                            wrap,
                        )?;
                    } else if baseline_save || baseline_diff || baseline_update {
                        validate_doctor_diagnostic_read_only_controls(
                            "baseline",
                            &backup_id,
                            dry_run,
                            yes,
                            plan_fingerprint.as_deref(),
                            force_rebuild,
                            allow_repeated_repair,
                        )?;
                        let mode = if baseline_diff {
                            DoctorBaselineCommandMode::Diff
                        } else if baseline_update {
                            DoctorBaselineCommandMode::Update
                        } else {
                            DoctorBaselineCommandMode::Save
                        };
                        run_doctor_baseline_impl(
                            data_dir,
                            cli.db.clone(),
                            structured_format,
                            mode,
                            baseline_id,
                            baseline_path,
                            wrap,
                        )?;
                    } else {
                        let request = doctor::DoctorCommandRequest::from_cli_flags_with_backups(
                            data_dir,
                            cli.db.clone(),
                            structured_format,
                            check,
                            fix,
                            repair,
                            cleanup,
                            archive_scan,
                            archive_normalize,
                            backups_list,
                            backups_verify,
                            backups_restore,
                            backup_id,
                            dry_run,
                            yes,
                            plan_fingerprint,
                            verbose,
                            force_rebuild,
                            allow_repeated_repair,
                        )?;
                        doctor::execute_doctor_command_with_wrap(request, wrap)?;
                    }
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
                    encrypt,
                    password_stdin,
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
                        encrypt,
                        password_stdin,
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
                Commands::Schedule(subcmd) => {
                    run_schedule_command(subcmd, cli)?;
                }
                _ => {}
            }
        }
    }

    Ok(())
}
