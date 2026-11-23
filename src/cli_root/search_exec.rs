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

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_cli_search(
    query: &str,
    agents: &[String],
    workspaces: &[String],
    limit: &usize,
    offset: &usize,
    json: &bool,
    robot_format: Option<RobotFormat>,
    robot_meta: bool,
    fields: Option<Vec<String>>,
    max_content_length: Option<usize>,
    max_tokens: Option<usize>,
    request_id: Option<String>,
    cursor: Option<String>,
    display_format: Option<DisplayFormat>,
    data_dir_override: &Option<PathBuf>,
    db_override: Option<PathBuf>,
    wrap: WrapConfig,
    _progress: ProgressResolved,
    robot_auto: bool,
    time_filter: TimeFilter,
    aggregate: Option<Vec<String>>,
    explain: bool,
    dry_run: bool,
    no_maintenance: bool,
    timeout_ms: Option<u64>,
    highlight: bool,
    source: Option<String>,
    sessions_from: Option<String>,
    mode: Option<crate::search::query::SearchMode>,
    semantic_opts: SemanticSearchOptions,
    refresh: bool,
    interpretation: &InvocationInterpretation,
) -> CliResult<()> {
    use crate::search::model_manager::{
        load_hash_semantic_context, load_hash_semantic_context_strict, load_semantic_context,
        load_semantic_context_deferred, load_semantic_context_deferred_strict,
        load_semantic_context_for_embedder, load_semantic_context_for_embedder_deferred,
        load_semantic_context_for_embedder_deferred_strict,
        load_semantic_context_for_embedder_strict, load_semantic_context_strict,
    };
    use crate::search::query::{QueryExplanation, SearchFilters, SearchMode};

    use crate::sources::provenance::SourceFilter;
    use std::collections::HashSet;
    use std::sync::Arc;

    // The depth refusal is a property of the query text, so it fires
    // before the effective-interpretation reading and every later lane —
    // none of which may answer a different query or touch an archive for
    // input the grammar's recursive stages must never see.
    ensure_cli_query_nesting_depth(query)?;

    // Start timing for robot_meta elapsed_ms
    let start_time = Instant::now();

    let data_dir = resolve_data_dir(data_dir_override, db_override.as_ref());
    let index_path = crate::search::tantivy::expected_index_dir(&data_dir);
    let refresh_db_override = db_override.clone();
    let db_path_source = effective_db_path_source(
        db_override.is_some(),
        data_dir_override.is_some(),
        interpretation,
    );
    let db_path = db_override.unwrap_or_else(|| data_dir.join("agent_search.db"));

    // Resolve robot mode before reading any user-supplied session scope so a
    // FIFO/stdin/file reader shares the same hard deadline as the search.
    let effective_robot = robot_format
        .or(if *json { Some(RobotFormat::Json) } else { None })
        .or_else(robot_format_from_env)
        .or(if robot_auto {
            Some(RobotFormat::Json)
        } else {
            None
        });
    let search_budget = effective_robot.map(|_| {
        crate::robot_budget_envelope::RobotBudget::with_start(
            search_budget_ms(timeout_ms),
            start_time,
        )
    });
    let mut skipped_sections = Vec::<String>::new();
    let mut session_paths_timed_out = false;

    let mut filters = SearchFilters {
        agents: normalize_agent_filters(agents)?.into_iter().collect(),
        created_from: time_filter.since,
        created_to: time_filter.until,
        ..Default::default()
    };
    if !workspaces.is_empty() {
        filters.workspaces = HashSet::from_iter(workspaces.iter().cloned());
    }

    // Apply source filter (P3.1)
    if let Some(ref source_str) = source {
        filters.source_filter = SourceFilter::parse(source_str);
    }

    // Apply session paths filter (for chained searches)
    if let Some(ref sessions_from_arg) = sessions_from {
        match read_session_paths_bounded(
            sessions_from_arg,
            search_budget
                .as_ref()
                .map(crate::robot_budget_envelope::RobotBudget::remaining_ms),
        )? {
            BoundedSessionPaths::Complete(session_paths) => {
                filters.session_paths = session_paths;
            }
            BoundedSessionPaths::TimedOut => {
                session_paths_timed_out = true;
                skipped_sections.push("sessions_from".to_string());
            }
        }
    }
    // GH#414: resolve how many requested session paths the index has seen, so
    // "your filter selected zero sessions" is distinguishable from "your
    // query found zero hits". `matched: None` renders as unknown.
    let matched_session_paths = if sessions_from.is_none() || filters.session_paths.is_empty() {
        Some(0)
    } else if let Some(budget) = search_budget.as_ref() {
        let worker_db_path = db_path.clone();
        let requested = filters.session_paths.clone();
        run_read_only_search_worker(budget.remaining_ms().min(500), move || {
            Ok(count_indexed_session_paths(&worker_db_path, &requested))
        })?
        .flatten()
    } else {
        count_indexed_session_paths(&db_path, &filters.session_paths)
    };
    let sessions_filter_stats: Option<SessionsFilterStats> =
        sessions_from.as_ref().map(|_| SessionsFilterStats {
            requested: filters.session_paths.len(),
            matched: matched_session_paths,
        });
    let effective = search_effective_interpretation(
        "search",
        query,
        &db_path,
        db_path_source,
        &time_filter,
        &filters,
        sessions_from.as_ref().map(|_| filters.session_paths.len()),
        interpretation,
        Some(&semantic_opts),
    );

    // Apply cursor overrides (base64-encoded JSON { "offset": usize, "limit": usize })
    let mut limit_val = *limit;
    let mut offset_val = *offset;
    if let Some(ref cursor_str) = cursor {
        let decoded = BASE64_STANDARD.decode(cursor_str).map_err(|e| CliError {
            code: 2,
            kind: CliErrorKind::CursorDecode.kind_str(),
            message: format!("invalid cursor: {e}"),
            hint: Some("Pass cursor returned in previous _meta.next_cursor".to_string()),
            retryable: false,
        })?;
        let cursor_json: serde_json::Value =
            serde_json::from_slice(&decoded).map_err(|e| CliError {
                code: 2,
                kind: CliErrorKind::CursorParse.kind_str(),
                message: format!("invalid cursor payload: {e}"),
                hint: Some("Cursor should be base64 of {\"offset\":N,\"limit\":M}".to_string()),
                retryable: false,
            })?;
        if let Some(o) = cursor_json
            .get("offset")
            .and_then(serde_json::Value::as_u64)
        {
            offset_val = o as usize;
        }
        if let Some(l) = cursor_json.get("limit").and_then(serde_json::Value::as_u64) {
            limit_val = l as usize;
        }
    }

    let mut daemon_embedder_id_for_rerank = None::<String>;
    let field_mask_visible_limit = token_budget_field_mask_visible_limit(max_tokens, limit_val);
    let field_mask = resolve_field_mask(
        &fields,
        max_content_length,
        max_tokens,
        field_mask_visible_limit,
        effective_robot,
        display_format,
    );

    // Parse aggregate fields if provided
    let agg_fields = aggregate
        .as_ref()
        .map(|f| parse_aggregate_fields(f))
        .transpose()?
        .unwrap_or_default();
    let has_aggregation = !agg_fields.is_empty();

    // A timeout retry is the same parsed request with a larger budget, not a
    // fresh unfiltered query. Freeze relative time bounds and cursor pagination
    // so following the advice cannot silently broaden the original scope.
    let search_retry = effective_robot.and_then(|format| {
        let mut args = vec![
            format!("--limit={limit_val}"),
            format!("--offset={offset_val}"),
        ];
        for (flag, values) in [("agent", agents), ("workspace", workspaces)] {
            args.extend(values.iter().map(|value| format!("--{flag}={value}")));
        }
        for (flag, bound) in [("since", time_filter.since), ("until", time_filter.until)] {
            if let Some(bound) = bound {
                let date = chrono::DateTime::from_timestamp_millis(bound)?;
                args.push(format!("--{flag}={}", date.to_rfc3339()));
            }
        }
        for (flag, value) in [
            ("source", source.as_deref()),
            ("request-id", request_id.as_deref()),
            ("model", semantic_opts.model.as_deref()),
            ("reranker", semantic_opts.reranker.as_deref()),
        ] {
            if let Some(value) = value {
                args.push(format!("--{flag}={value}"));
            }
        }
        for (flag, values) in [
            ("fields", fields.as_ref()),
            ("aggregate", aggregate.as_ref()),
        ] {
            if let Some(values) = values {
                args.push(format!("--{flag}={}", values.join(",")));
            }
        }
        for (flag, value) in [
            ("max-content-length", max_content_length),
            ("max-tokens", max_tokens),
        ] {
            if let Some(value) = value {
                args.push(format!("--{flag}={value}"));
            }
        }
        for (flag, enabled) in [
            ("no-maintenance", no_maintenance),
            ("robot-meta", robot_meta),
            ("explain", explain),
            ("dry-run", dry_run),
            ("highlight", highlight),
            ("refresh", refresh),
            ("approximate", semantic_opts.approximate),
            ("rerank", semantic_opts.rerank),
            ("daemon", semantic_opts.auto_spawn_daemon),
            ("no-daemon", !semantic_opts.use_daemon),
        ] {
            if enabled {
                args.push(format!("--{flag}"));
            }
        }
        use crate::search::query::SemanticTierMode;
        match semantic_opts.tier_mode {
            SemanticTierMode::Single => {}
            SemanticTierMode::Progressive => args.push("--two-tier".to_string()),
            SemanticTierMode::FastOnly => args.push("--fast-only".to_string()),
            SemanticTierMode::QualityOnly => args.push("--quality-only".to_string()),
        }
        search_budget_retry_command(
            query,
            format,
            search_budget.as_ref()?.total_ms(),
            (&data_dir, &db_path),
            sessions_from.as_deref(),
            mode,
            &args,
        )
    });

    let output_early_timeout = |mut skipped_sections: Vec<String>| {
        for (requested, section) in [
            (semantic_opts.rerank, "reranking"),
            (explain, "explanation"),
            (has_aggregation, "aggregations"),
            (robot_meta, "state_meta"),
        ] {
            if requested {
                skipped_sections.push(section.to_string());
            }
        }
        output_search_budget_partial(
            query,
            effective_robot.expect("bounded search is robot-only"),
            search_budget.as_ref().expect("robot search has a budget"),
            skipped_sections,
            search_retry.clone(),
            (
                SearchModeMeta::new(mode.unwrap_or_default(), mode.is_none()),
                robot_meta,
            ),
        )
    };

    // All fallible request parsing is complete before the opt-in human refresh
    // can mutate derived assets. Robot refresh remains a deferred, explicit
    // dataset-scoped recommendation.
    if refresh && effective_robot.is_none() {
        refresh_index_inline(refresh_db_override, data_dir_override.clone());
    } else if refresh {
        skipped_sections.push("refresh".to_string());
    }

    if session_paths_timed_out {
        skipped_sections.push("search".to_string());
        return output_early_timeout(skipped_sections);
    }

    // Handle dry-run mode before touching derived search assets. A dry run is
    // query analysis only, so it must work on a fresh data dir with no DB/index.
    if dry_run {
        let explanation = QueryExplanation::analyze(query, &filters);
        let elapsed_ms = start_time.elapsed().as_millis();
        let meta = search_dry_run_meta(robot_meta, elapsed_ms);

        let mut output = serde_json::json!({
            "dry_run": true,
            "valid": explanation.warnings.iter().all(|w| !w.contains("error") && !w.contains("invalid")),
            "query": query,
            "explanation": explanation,
            "estimated_cost": format!("{:?}", explanation.estimated_cost),
            "warnings": explanation.warnings,
            "request_id": request_id,
            "_meta": meta,
        });
        if let Some(budget) = search_budget.as_ref()
            && let serde_json::Value::Object(map) = &mut output
        {
            map.insert(
                "budget".to_string(),
                serde_json::to_value(crate::robot_budget_envelope::BudgetBlock::from_budget(
                    budget,
                    Vec::new(),
                    None,
                ))
                .unwrap_or(serde_json::Value::Null),
            );
        }

        println!(
            "{}",
            serde_json::to_string_pretty(&output).unwrap_or_else(|_| output.to_string())
        );
        return Ok(());
    }

    let setup = if let Some(budget) = search_budget.as_ref() {
        let worker_data_dir = data_dir.clone();
        let worker_db_path = db_path.clone();
        let worker_index_path = index_path.clone();
        run_read_only_search_worker(budget.remaining_ms(), move || {
            open_cli_search_setup(
                &worker_data_dir,
                &worker_db_path,
                &worker_index_path,
                mode,
                timeout_ms,
                start_time,
                no_maintenance,
                true,
            )
        })?
    } else {
        Some(open_cli_search_setup(
            &data_dir,
            &db_path,
            &index_path,
            mode,
            timeout_ms,
            start_time,
            no_maintenance,
            false,
        )?)
    };
    let Some(CliSearchSetup {
        self_heal: search_self_heal,
        client,
        ..
    }) = setup
    else {
        skipped_sections.extend(["search_setup".to_string(), "search".to_string()]);
        return output_early_timeout(skipped_sections);
    };
    if search_self_heal.action != "skipped" {
        tracing::info!(
            action = search_self_heal.action,
            reason = search_self_heal.reason.as_deref(),
            indexed_docs = search_self_heal.indexed_docs,
            "search lexical self-heal completed"
        );
    }
    if !client.has_tantivy() && !search_request_skips_lexical_self_heal(mode) {
        eprintln!(
            "Warning: Tantivy search index not found at {}. \
             Results will be severely limited. \
             Run `cass index --full` to rebuild the index.",
            index_path.display()
        );
    }

    // Hybrid is a preference for semantic refinement, not a strict dependency.
    // If semantic assets are unavailable, hybrid searches fail open to lexical
    // while robot metadata reports the realized mode and fallback reason.
    let mut mode_meta = SearchModeMeta::new(mode.unwrap_or_default(), mode.is_none());
    mode_meta.quality_tier_refined =
        semantic_opts.tier_mode != crate::search::query::SemanticTierMode::FastOnly;
    let hybrid_fail_open = mode_meta.fail_open_on_semantic_unavailable();
    let semantic_requested = matches!(
        mode_meta.requested,
        SearchMode::Semantic | SearchMode::Hybrid
    );

    if semantic_opts.tier_mode != crate::search::query::SemanticTierMode::Single
        && !matches!(mode_meta.requested, SearchMode::Semantic)
    {
        eprintln!("Warning: tier flags currently only affect --mode semantic.");
    }

    let mut semantic_setup_timed_out = false;
    if semantic_requested && let Some(budget) = search_budget.as_ref() {
        let worker_client = Arc::clone(&client);
        let worker_data_dir = data_dir.clone();
        let worker_db_path = db_path.clone();
        let worker_mode_meta = mode_meta.clone();
        let mut worker_options = semantic_opts.clone();
        worker_options.auto_spawn_daemon = false;
        let configured = run_read_only_search_worker(budget.remaining_ms(), move || {
            configure_bounded_search_semantics(
                worker_client,
                worker_data_dir,
                worker_db_path,
                worker_mode_meta,
                worker_options,
                no_maintenance,
            )
        })?;
        if let Some((configured_mode, daemon_embedder_id)) = configured {
            mode_meta = configured_mode;
            daemon_embedder_id_for_rerank = daemon_embedder_id;
        } else {
            semantic_setup_timed_out = true;
            mode_meta.semantic_work_completed = false;
            skipped_sections.push("semantic_setup".to_string());
            if mode_meta.fail_open_on_semantic_unavailable() {
                mode_meta.fall_back_to_lexical(
                    "semantic setup did not complete within the robot search budget",
                );
            }
        }
    }

    let semantic_budget_available = search_budget
        .as_ref()
        .is_none_or(|budget| budget.is_healthy());
    if semantic_requested && search_budget.is_some() {
        // Structured searches configure semantic assets on the bounded worker
        // above. An explicit semantic timeout remains semantic and later emits
        // an empty partial result; it must never substitute lexical evidence.
    } else if matches!(
        mode_meta.requested,
        SearchMode::Semantic | SearchMode::Hybrid
    ) && !semantic_budget_available
    {
        skipped_sections.push("semantic_refinement".to_string());
        mode_meta.fall_back_to_lexical(
            "semantic refinement skipped because the robot search budget is nearly exhausted",
        );
    }

    use crate::search::embedder_registry::{EmbedderRegistry, HASH_EMBEDDER};

    // Validate an explicitly named producer before budget shedding. This is a
    // cheap configuration check: an invalid or unavailable embedder must not
    // be masked by a retryable budget outcome. Model/index opening and
    // inference remain deferred until semantic execution is admitted below.
    let registry = EmbedderRegistry::new(&data_dir);
    let env_model = if semantic_requested {
        dotenvy::var("CASS_SEMANTIC_EMBEDDER")
            .ok()
            .and_then(|value| {
                if value.trim().eq_ignore_ascii_case("hash") {
                    Some("hash")
                } else {
                    // Legacy/unrecognized values (e.g. external) are ignored.
                    crate::search::fastembed_embedder::FastEmbedder::canonical_name(&value)
                }
            })
    } else {
        None
    };
    let requested_model = semantic_opts.model.as_deref().or(env_model);
    if semantic_requested
        && let Some(model_name) = semantic_opts.model.as_deref()
        && let Err(error) =
            crate::search::embedder_registry::validate_selection(&data_dir, model_name)
    {
        return Err(CliError {
            code: 15,
            kind: CliErrorKind::EmbedderUnavailable.kind_str(),
            message: format!("Embedder validation failed: {error}"),
            hint: Some("Run 'cass models status' to see available embedders".to_string()),
            retryable: false,
        });
    }

    let setup_budget_snapshot = search_budget.as_ref().map(BudgetSnapshot::capture);
    let budget_action = semantic_budget_action(&mode_meta, setup_budget_snapshot);
    let execute_semantic = match budget_action {
        // A budgeted (structured) search already configured semantics on the
        // bounded worker above, which never spawns a daemon. Repeating the
        // setup here paid its cost a second time outside the budget, retried
        // it unbounded after the worker timed out or fell back, and could
        // spawn the daemon the worker declined to spawn (2l1b0.68).
        SemanticBudgetAction::Execute => semantic_requested && search_budget.is_none(),
        SemanticBudgetAction::StrictSemanticTimeout => {
            // NearLimit is deliberately a typed timeout here: semantic setup
            // is an indivisible expensive stage, so insufficient remaining
            // headroom is a deadline-admission failure even before Exhausted.
            return Err(strict_semantic_budget_error(
                &mode_meta,
                setup_budget_snapshot,
                SemanticBudgetCheckpoint::BeforeSetup,
            ));
        }
        SemanticBudgetAction::HybridLexicalFallback => {
            skipped_sections.push("semantic_refinement".to_string());
            mode_meta.fall_back_to_lexical(semantic_budget_fallback_reason(
                SemanticBudgetCheckpoint::BeforeSetup,
            ));
            false
        }
    };

    if execute_semantic {
        maybe_test_search_worker_delay("CASS_TEST_SEARCH_DIRECT_SEMANTIC_SETUP_SLOW_MS");
        // Use embedder registry for model selection (bd-2mbe).

        // Determine which embedder to use
        let embedder_info = match requested_model {
            Some(name) => registry.get(name),
            None => Some(registry.best_available()),
        };
        daemon_embedder_id_for_rerank = embedder_info
            .filter(|entry| entry.is_semantic)
            .map(|entry| entry.id.to_string());
        // `--fast-only` is a request for the hash-vector space, not merely a
        // scoring toggle over a quality embedding. Feeding a MiniLM daemon
        // vector to the same-dimensional FNV index is silently wrong (#347).
        let prefer_hash = semantic_opts.tier_mode
            == crate::search::query::SemanticTierMode::FastOnly
            || embedder_info.is_some_and(|e| e.name == HASH_EMBEDDER);

        let setup = if prefer_hash {
            if no_maintenance {
                load_hash_semantic_context_strict(&data_dir, &db_path)
            } else {
                load_hash_semantic_context(&data_dir, &db_path)
            }
        } else if let Some(model_name) = requested_model {
            if semantic_opts.use_daemon && no_maintenance {
                load_semantic_context_for_embedder_deferred_strict(&data_dir, &db_path, model_name)
            } else if semantic_opts.use_daemon {
                load_semantic_context_for_embedder_deferred(&data_dir, &db_path, model_name)
            } else if no_maintenance {
                load_semantic_context_for_embedder_strict(&data_dir, &db_path, model_name)
            } else {
                load_semantic_context_for_embedder(&data_dir, &db_path, model_name)
            }
        } else if semantic_opts.use_daemon && no_maintenance {
            load_semantic_context_deferred_strict(&data_dir, &db_path)
        } else if semantic_opts.use_daemon {
            load_semantic_context_deferred(&data_dir, &db_path)
        } else if no_maintenance {
            load_semantic_context_strict(&data_dir, &db_path)
        } else {
            load_semantic_context(&data_dir, &db_path)
        };

        if let Some(context) = setup.context {
            let embedder = context.embedder;
            let artifacts = context.artifacts;
            let quality_artifact = context.quality_artifact;
            let filter_maps = context.filter_maps;
            let roles = context.roles;
            let daemon_embedder_compatible =
                crate::search::fastembed_embedder::FastEmbedder::canonical_name(embedder.id())
                    .is_some();

            let embedder: Arc<dyn crate::search::embedder::Embedder> = if semantic_opts.use_daemon
                && !prefer_hash
                && daemon_embedder_compatible
            {
                #[cfg(unix)]
                {
                    let daemon = if semantic_opts.auto_spawn_daemon {
                        crate::daemon::client::connect_or_spawn_for_embedder(
                            embedder.id(),
                            &data_dir,
                        )
                        .ok()
                    } else {
                        crate::daemon::client::try_connect_for_embedder(embedder.id(), &data_dir)
                    };
                    let config = crate::search::daemon_client::DaemonRetryConfig::from_env();
                    match daemon {
                        Some(daemon) => match daemon.attestation_channel(&data_dir) {
                            Ok((connection, verifier)) => {
                                let daemon: Arc<dyn crate::search::daemon_client::DaemonClient> =
                                    daemon;
                                compose_verified_daemon_embedder_or_local(
                                    daemon, embedder, config, connection, verifier,
                                )
                            }
                            Err(error) => {
                                tracing::warn!(
                                    error = %error,
                                    embedder_id = %embedder.id(),
                                    "Daemon attestation channel could not be established"
                                );
                                eprintln!(
                                    "Warning: a local embedding daemon is running but its \
                                         producer attestation could not be established ({error}); \
                                         falling back to in-process {} inference.",
                                    embedder.id()
                                );
                                embedder
                            }
                        },
                        None => embedder,
                    }
                }
                #[cfg(not(unix))]
                {
                    embedder
                }
            } else {
                embedder
            };

            if let Err(err) = client.set_semantic_artifacts_context(
                embedder,
                artifacts,
                quality_artifact,
                filter_maps,
                roles,
            ) {
                let hint = if prefer_hash {
                    "Run 'cass index --semantic --embedder hash' to rebuild the hash vector index, or omit --mode semantic when lexical evidence is acceptable"
                        .to_string()
                } else {
                    "Run 'cass models install --from-file <DIR>' and then 'cass index --semantic', or omit --mode semantic when lexical evidence is acceptable"
                        .to_string()
                };
                if hybrid_fail_open {
                    mode_meta.fall_back_to_lexical(format!("semantic context rejected: {err}"));
                    let _ = client.clear_semantic_context();
                } else {
                    return Err(CliError {
                        code: 15,
                        kind: CliErrorKind::SemanticUnavailable.kind_str(),
                        message: format!("Semantic search not available: {err}"),
                        hint: Some(hint),
                        retryable: false,
                    });
                }
            }
        } else {
            let _ = client.clear_semantic_context();
            let summary = setup.availability.summary();
            let hint = if prefer_hash {
                "Run 'cass index --semantic --embedder hash' to build the hash vector index, or omit --mode semantic when lexical evidence is acceptable"
                    .to_string()
            } else {
                "Run 'cass models install --from-file <DIR>' and then 'cass index --semantic', or omit --mode semantic when lexical evidence is acceptable"
                    .to_string()
            };
            if hybrid_fail_open {
                mode_meta.fall_back_to_lexical(format!("semantic context unavailable: {summary}"));
            } else {
                return Err(CliError {
                    code: 15,
                    kind: CliErrorKind::SemanticUnavailable.kind_str(),
                    message: format!("Semantic search not available: {summary}"),
                    hint: Some(hint),
                    retryable: false,
                });
            }
        }
    }

    // The one-shot CLI emits one final result set. Its legacy synchronous
    // two-tier helper accepts only one query vector, which would incorrectly
    // reuse a MiniLM vector against the FNV fast index. The interactive TUI has
    // the true two-embedder progressive pipeline; here progressive/quality-only
    // correctly return the final quality result, while fast-only uses the hash
    // context selected above (#347).
    let semantic_execution_tier = match semantic_opts.tier_mode {
        crate::search::query::SemanticTierMode::FastOnly => {
            crate::search::query::SemanticTierMode::FastOnly
        }
        crate::search::query::SemanticTierMode::Single
        | crate::search::query::SemanticTierMode::Progressive
        | crate::search::query::SemanticTierMode::QualityOnly => {
            crate::search::query::SemanticTierMode::Single
        }
    };

    // Use search_with_fallback to get full metadata (wildcard_fallback, cache_stats)
    let sparse_threshold = 3; // Threshold for triggering wildcard fallback

    // When aggregating, we need more results for accurate counts.
    // For non-aggregation mode, overfetch by one so cursor pagination can reliably
    // signal whether additional pages exist without a second query.
    let token_budget_page_limit = token_budget_search_limit(max_tokens);
    let cursor_page_limit = if has_aggregation {
        limit_val
    } else if limit_val == 0 {
        token_budget_page_limit.unwrap_or(0)
    } else {
        limit_val
    };
    let (search_limit, search_offset) = if has_aggregation {
        (1000.max(limit_val + offset_val), 0)
    } else if limit_val == 0 {
        match token_budget_page_limit {
            Some(page_limit) => (page_limit.saturating_add(1), offset_val),
            None => (0, offset_val),
        }
    } else {
        (limit_val.saturating_add(1), offset_val)
    };
    let sparse_visible_limit = if limit_val == 0 && cursor_page_limit > 0 {
        cursor_page_limit
    } else {
        limit_val
    };
    let search_sparse_threshold =
        sparse_threshold_for_visible_limit(sparse_threshold, sparse_visible_limit, has_aggregation);

    // Check if we're already past timeout before starting search
    let timeout_duration = timeout_ms.map(Duration::from_millis);
    if search_budget.is_none()
        && let Some(timeout) = timeout_duration
        && start_time.elapsed() >= timeout
    {
        return Err(CliError {
            code: 10,
            kind: CliErrorKind::Timeout.kind_str(),
            message: format!(
                "Operation timed out after {}ms (before search started)",
                timeout.as_millis()
            ),
            hint: Some("Increase --timeout value or simplify query".to_string()),
            retryable: true,
        });
    }

    // Log semantic options if any are set (bd-3bbv: flags are wired, infra pending)
    if semantic_opts.model.is_some()
        || semantic_opts.rerank
        || semantic_opts.reranker.is_some()
        || semantic_opts.use_daemon
        || semantic_opts.approximate
        || semantic_opts.tier_mode != crate::search::query::SemanticTierMode::Single
    {
        tracing::debug!(
            model = ?semantic_opts.model,
            rerank = semantic_opts.rerank,
            reranker = ?semantic_opts.reranker,
            use_daemon = semantic_opts.use_daemon,
            approximate = semantic_opts.approximate,
            tier_mode = ?semantic_opts.tier_mode,
            "Semantic search options configured"
        );
    }

    if execute_semantic && mode_meta.semantic_refinement() {
        maybe_test_force_semantic_dispatch_budget_phase(search_budget.as_ref());
    }
    // This is the one authoritative dispatch-boundary sample. Both the
    // mode-aware semantic policy and the generic exhausted-budget branch use
    // it, so a scheduler preemption cannot produce an empty successful result
    // that still claims semantic/hybrid refinement.
    let dispatch_budget_snapshot = search_budget.as_ref().map(BudgetSnapshot::capture);
    match semantic_budget_action(&mode_meta, dispatch_budget_snapshot) {
        SemanticBudgetAction::Execute => {}
        SemanticBudgetAction::StrictSemanticTimeout => {
            return Err(strict_semantic_budget_error(
                &mode_meta,
                dispatch_budget_snapshot,
                SemanticBudgetCheckpoint::BeforeSearch,
            ));
        }
        SemanticBudgetAction::HybridLexicalFallback => {
            skipped_sections.push("semantic_refinement".to_string());
            mode_meta.fall_back_to_lexical(semantic_budget_fallback_reason(
                SemanticBudgetCheckpoint::BeforeSearch,
            ));
            let _ = client.clear_semantic_context();
        }
    }
    let approximate =
        if semantic_opts.approximate && matches!(mode_meta.realized, SearchMode::Lexical) {
            eprintln!("Warning: --approximate has no effect in lexical mode.");
            false
        } else {
            semantic_opts.approximate
        };
    // Track search timing breakdown (T7.4) after admission and test-only
    // dispatch controls so it measures only the search operation.
    let search_start = Instant::now();
    let bounded_result = if sessions_from.is_some() && filters.session_paths.is_empty() {
        // An explicitly empty candidate stream selects no sessions. The
        // backend uses an empty set to mean no restriction, so never send
        // this scope into lexical, semantic, or wildcard-fallback search.
        mode_meta.semantic_work_completed = false;
        Some((
            crate::search::query::SearchResult {
                total_count: Some(0),
                ..empty_search_result()
            },
            mode_meta.clone(),
        ))
    } else if semantic_setup_timed_out && matches!(mode_meta.requested, SearchMode::Semantic) {
        None
    } else if let Some(budget) = search_budget.as_ref() {
        let worker_client = Arc::clone(&client);
        let worker_query = query.to_string();
        let worker_filters = filters.clone();
        let worker_mode_meta = mode_meta.clone();
        run_read_only_search_worker(budget.remaining_ms(), move || {
            execute_search_operation(
                worker_client,
                worker_query,
                worker_filters,
                search_limit,
                search_offset,
                search_sparse_threshold,
                field_mask,
                approximate,
                semantic_execution_tier,
                worker_mode_meta,
            )
        })?
    } else {
        None
    };
    let result = if let Some((result, realized_mode_meta)) = bounded_result {
        mode_meta = realized_mode_meta;
        result
    } else if search_budget.is_some() {
        if semantic_requested {
            mode_meta.semantic_work_completed = false;
        }
        skipped_sections.push("search".to_string());
        empty_search_result()
    } else {
        match mode_meta.realized {
            SearchMode::Lexical => client
                .search_with_fallback(
                    query,
                    filters.clone(),
                    search_limit,
                    search_offset,
                    search_sparse_threshold,
                    field_mask,
                )
                .map_err(|e| {
                    let invariant_hint = lexical_engine_invariant_hint(&e);
                    CliError {
                        code: 9,
                        kind: CliErrorKind::Search.kind_str(),
                        message: format!("search failed: {e}"),
                        retryable: invariant_hint.is_none(),
                        hint: invariant_hint,
                    }
                })?,
            SearchMode::Semantic => {
                // The former `semantic` Cargo feature (and its `-baseline`
                // refusal arm for the ONNX-era AVX2 hazard, cass#256) was retired
                // with bead tg5o9: every build carries the pure-Rust
                // frankensearch/native backend, so semantic availability is a
                // runtime question (model/vector assets), not a compile-time one.
                {
                    let (hits, ann_stats) = client
                    .search_semantic_with_tier(
                        query,
                        filters.clone(),
                        search_limit,
                        search_offset,
                        field_mask,
                        approximate,
                        semantic_execution_tier,
                    )
                    .map_err(|e| {
                        let err_str = e.to_string();
                        if err_str.contains("HNSW index") {
                            CliError {
                                code: 15,
                                kind: CliErrorKind::SemanticUnavailable.kind_str(),
                                message: "Approximate search unavailable (HNSW index missing)"
                                    .to_string(),
                                hint: Some(
                                    "Run 'cass index --semantic --build-hnsw' to build the ANN index, or omit --approximate"
                                        .to_string(),
                                ),
                                retryable: false,
                            }
                        } else if err_str.contains("unavailable")
                            || err_str.contains("no embedder")
                        {
                            CliError {
                                code: 15,
                                kind: CliErrorKind::SemanticUnavailable.kind_str(),
                                message: "Semantic search not available".to_string(),
                                hint: Some(
                                    "Run 'cass tui' and press Alt+S to set up semantic search, or omit --mode semantic when lexical evidence is acceptable"
                                        .to_string(),
                                ),
                                retryable: false,
                            }
                        } else {
                            CliError {
                                code: 9,
                                kind: CliErrorKind::Search.kind_str(),
                                message: format!("semantic search failed: {e}"),
                                hint: Some(
                                    "Retry with the default hybrid-preferred mode when lexical evidence is acceptable"
                                        .to_string(),
                                ),
                                retryable: true,
                            }
                        }
                    })?;
                    let ann_unavailable_reason = if approximate {
                        client.ann_unavailability_reason().map_err(|e| CliError {
                            code: 9,
                            kind: CliErrorKind::Search.kind_str(),
                            message: format!("failed to inspect ANN fallback state: {e}"),
                            hint: Some(
                                "Retry the search; CASS could not read the active semantic context"
                                    .to_string(),
                            ),
                            retryable: true,
                        })?
                    } else {
                        None
                    };
                    crate::search::query::SearchResult {
                        hits,
                        wildcard_fallback: false,
                        cache_stats: crate::search::query::CacheStats::default(),
                        suggestions: Vec::new(),
                        ann_stats,
                        ann_unavailable_reason,
                        total_count: None,
                    }
                }
            }
            SearchMode::Hybrid => match client.search_hybrid(
                query,
                query,
                filters.clone(),
                search_limit,
                search_offset,
                search_sparse_threshold,
                field_mask,
                approximate,
            ) {
                Ok(result) => result,
                Err(e) => {
                    let err_str = e.to_string();
                    if hybrid_fail_open
                        && (err_str.contains("unavailable") || err_str.contains("no embedder"))
                    {
                        mode_meta
                            .fall_back_to_lexical(format!("hybrid execution unavailable: {e}"));
                        client
                        .search_with_fallback(
                            query,
                            filters.clone(),
                            search_limit,
                            search_offset,
                            search_sparse_threshold,
                            field_mask,
                        )
                        .map_err(|fallback_err| {
                            let invariant_hint = lexical_engine_invariant_hint(&fallback_err);
                            CliError {
                                code: 9,
                                kind: CliErrorKind::Search.kind_str(),
                                message: format!(
                                    "hybrid search failed ({e}); lexical fallback failed: {fallback_err}"
                                ),
                                retryable: invariant_hint.is_none(),
                                hint: invariant_hint,
                            }
                        })?
                    } else if err_str.contains("unavailable") || err_str.contains("no embedder") {
                        return Err(CliError {
                        code: 15,
                        kind: CliErrorKind::SemanticUnavailable.kind_str(),
                        message: "Hybrid search not available (requires semantic search)".to_string(),
                        hint: Some(
                            "Run 'cass tui' and press Alt+S to set up semantic search, or omit --mode hybrid when lexical evidence is acceptable"
                                .to_string(),
                        ),
                        retryable: false,
                    });
                    } else {
                        let invariant_hint = lexical_engine_invariant_hint(&e);
                        return Err(CliError {
                        code: 9,
                        kind: CliErrorKind::Search.kind_str(),
                        message: format!("hybrid search failed: {e}"),
                        retryable: invariant_hint.is_none(),
                        hint: invariant_hint.or_else(|| {
                            Some(
                                "Retry with the default hybrid-preferred mode when lexical evidence is acceptable"
                                    .to_string(),
                            )
                        }),
                    });
                    }
                }
            },
        }
    };
    let search_ms = search_start.elapsed().as_millis() as u64;
    if search_budget.is_none() {
        maybe_test_search_delay();
    }

    // Apply reranking if enabled (bd-2t2d)
    let rerank_start = Instant::now();
    let rerank_budget_available = search_budget
        .as_ref()
        .is_none_or(|budget| budget.is_healthy());
    let result = if semantic_opts.rerank && !rerank_budget_available {
        skipped_sections.push("reranking".to_string());
        result
    } else if semantic_opts.rerank && !result.hits.is_empty() {
        use crate::search::fastembed_reranker::{FastEmbedReranker, LazyFastEmbedReranker};
        use crate::search::reranker::{Reranker, rerank_texts};

        let local_reranker: Option<Arc<dyn Reranker>> = if semantic_opts.use_daemon {
            Some(Arc::new(LazyFastEmbedReranker::new(&data_dir)))
        } else {
            let model_dir = FastEmbedReranker::default_model_dir(&data_dir);
            match FastEmbedReranker::load_from_dir(&model_dir) {
                Ok(reranker) => Some(Arc::new(reranker)),
                Err(e) => {
                    tracing::debug!(error = %e, "Reranker not available, skipping rerank");
                    None
                }
            }
        };

        let reranker: Option<Arc<dyn Reranker>> = if semantic_opts.use_daemon {
            #[cfg(unix)]
            {
                let config = crate::search::daemon_client::DaemonRetryConfig::from_env();
                // A budgeted (structured) search never spawns a persistent
                // daemon, for reranking any more than for embedding; it uses
                // one that is already running (2l1b0.68).
                let daemon = if semantic_opts.auto_spawn_daemon && search_budget.is_none() {
                    crate::daemon::client::connect_or_spawn_for_embedder(
                        daemon_embedder_id_for_rerank.as_deref().unwrap_or(
                            crate::search::fastembed_embedder::FastEmbedder::embedder_id_static(),
                        ),
                        &data_dir,
                    )
                    .ok()
                } else {
                    crate::daemon::client::try_connect_for_data_dir(&data_dir)
                };
                match daemon {
                    Some(daemon) => match daemon.attestation_channel(&data_dir) {
                        Ok((connection, verifier)) => {
                            let daemon: Arc<dyn crate::search::daemon_client::DaemonClient> =
                                daemon;
                            match crate::search::daemon_client::DaemonFallbackReranker::new_verified(
                                daemon,
                                local_reranker.clone(),
                                config,
                                connection,
                                verifier,
                            ) {
                                Ok(reranker) => Some(Arc::new(reranker) as Arc<dyn Reranker>),
                                Err(error) => {
                                    tracing::warn!(
                                        error = %error,
                                        "Daemon reranker attestation was rejected"
                                    );
                                    local_reranker
                                }
                            }
                        }
                        Err(error) => {
                            tracing::warn!(
                                error = %error,
                                "Daemon reranker attestation channel could not be established"
                            );
                            local_reranker
                        }
                    },
                    None => local_reranker,
                }
            }
            #[cfg(not(unix))]
            {
                // Only the Unix daemon reranks with the search's embedder.
                let _ = daemon_embedder_id_for_rerank;
                local_reranker
            }
        } else {
            local_reranker
        };

        if let Some(reranker) = reranker {
            // Extract content from hits for reranking (use snippet if content is empty)
            let docs: Vec<String> = result
                .hits
                .iter()
                .map(|hit| {
                    if hit.content.is_empty() {
                        hit.snippet.clone()
                    } else {
                        hit.content.clone()
                    }
                })
                .collect();

            // Skip reranking if any document is empty (reranker rejects empty docs)
            let has_empty_doc = docs.iter().any(|d| d.is_empty());
            if has_empty_doc {
                tracing::debug!("Skipping rerank: one or more hits have empty content and snippet");
                result
            } else {
                let doc_refs: Vec<&str> = docs.iter().map(|s| s.as_str()).collect();

                match rerank_texts(&*reranker, query, &doc_refs) {
                    Ok(scores) => {
                        // Update scores and re-sort hits
                        let mut scored_hits: Vec<_> = result
                            .hits
                            .into_iter()
                            .zip(scores)
                            .map(|(mut hit, score)| {
                                hit.score = score;
                                hit
                            })
                            .collect();
                        scored_hits.sort_by(|a, b| {
                            b.score
                                .partial_cmp(&a.score)
                                .unwrap_or(std::cmp::Ordering::Equal)
                        });

                        tracing::debug!(
                            reranker_id = reranker.id(),
                            hits_reranked = scored_hits.len(),
                            "Reranking complete"
                        );

                        crate::search::query::SearchResult {
                            hits: scored_hits,
                            wildcard_fallback: result.wildcard_fallback,
                            cache_stats: result.cache_stats,
                            suggestions: result.suggestions,
                            ann_stats: result.ann_stats,
                            ann_unavailable_reason: result.ann_unavailable_reason,
                            total_count: result.total_count,
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "Reranking failed, returning original results"
                        );
                        result
                    }
                }
            }
        } else {
            result
        }
    } else {
        result
    };
    // Track reranking time (0 if not applied) (T7.4)
    let rerank_ms = if semantic_opts.rerank {
        rerank_start.elapsed().as_millis() as u64
    } else {
        0
    };

    present_cli_search_results(SearchPresentationInput {
        query,
        filters,
        result,
        start_time,
        search_budget,
        skipped_sections,
        explain,
        has_aggregation,
        agg_fields,
        limit_val,
        offset_val,
        cursor_page_limit,
        max_content_length,
        max_tokens,
        robot_meta,
        data_dir,
        db_path,
        no_maintenance,
        effective_robot,
        client,
        field_mask,
        sessions_filter_stats,
        fields,
        mode_meta,
        search_retry,
        highlight,
        request_id,
        cursor,
        search_ms,
        rerank_ms,
        effective,
        display_format,
        wrap,
    })
}
