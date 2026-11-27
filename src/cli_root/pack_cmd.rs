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
pub(crate) fn pack_budget_ms(timeout_ms: Option<u64>) -> u64 {
    timeout_ms.unwrap_or_else(|| {
        dotenvy::var("CASS_PACK_BUDGET_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|&milliseconds| milliseconds > 0)
            .unwrap_or(10_000)
    })
}

/// Deterministic slowdowns for bounded-pack E2E coverage. Production callers
/// never set these test-only environment variables.
pub(crate) fn maybe_test_pack_worker_delay(variable: &str) {
    if let Ok(raw) = dotenvy::var(variable)
        && let Ok(milliseconds) = raw.parse::<u64>()
        && milliseconds > 0
    {
        std::thread::sleep(Duration::from_millis(milliseconds));
    }
}

pub(crate) fn maybe_test_pack_delay() {
    maybe_test_pack_worker_delay("CASS_TEST_PACK_SLOW_MS");
}

pub(crate) fn maybe_test_pack_plan_delay() {
    maybe_test_pack_worker_delay("CASS_TEST_PACK_PLAN_SLOW_MS");
}

pub(crate) fn maybe_test_pack_render_delay() {
    maybe_test_pack_worker_delay("CASS_TEST_PACK_RENDER_SLOW_MS");
}

pub(crate) fn pack_budget_block(
    budget: &crate::robot_budget_envelope::RobotBudget,
    skipped_sections: Vec<String>,
    recommended_next_probe: Option<String>,
    hard_deadline_reached: bool,
) -> crate::robot_budget_envelope::BudgetBlock {
    let mut block = crate::robot_budget_envelope::BudgetBlock::from_budget(
        budget,
        skipped_sections,
        recommended_next_probe,
    );
    if hard_deadline_reached {
        block.elapsed_ms = block.elapsed_ms.max(block.budget_ms);
        block.timed_out = true;
    }
    block
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn pack_retry_command(
    data_dir: &Path,
    db_path: &Path,
    query: &str,
    agents: &[String],
    workspaces: &[String],
    limit: usize,
    fields: Option<&Vec<String>>,
    max_tokens: usize,
    max_sessions: usize,
    max_evidence: usize,
    context_lines: usize,
    max_excerpt_chars: usize,
    request_id: Option<&str>,
    time_filter: &TimeFilter,
    source: Option<&str>,
    sessions_from: Option<&str>,
    mode: Option<crate::search::query::SearchMode>,
    freshness_policy: crate::search::pack_planner::PackFreshnessPolicy,
    freshness_window_seconds: i64,
    include_skill_content: bool,
    require_evidence: bool,
    explain_selection: bool,
    render_format: crate::search::pack_planner::PackRenderFormat,
    retry_budget_ms: u64,
) -> String {
    use crate::search::pack_planner::{PackFreshnessPolicy, PackRenderFormat};

    use std::borrow::Cow;

    let mut args: Vec<Cow<'static, str>> =
        vec![Cow::Borrowed("pack"), Cow::Owned(shell_quote_arg(query))];
    for agent in agents {
        args.push(Cow::Borrowed("--agent"));
        args.push(Cow::Owned(shell_quote_arg(agent)));
    }
    for workspace in workspaces {
        args.push(Cow::Borrowed("--workspace"));
        args.push(Cow::Owned(shell_quote_arg(workspace)));
    }
    args.extend([
        Cow::Borrowed("--limit"),
        Cow::Owned(limit.to_string()),
        Cow::Borrowed("--max-tokens"),
        Cow::Owned(max_tokens.to_string()),
        Cow::Borrowed("--max-sessions"),
        Cow::Owned(max_sessions.to_string()),
        Cow::Borrowed("--max-evidence"),
        Cow::Owned(max_evidence.to_string()),
        Cow::Borrowed("--context-lines"),
        Cow::Owned(context_lines.to_string()),
        Cow::Borrowed("--max-excerpt-chars"),
        Cow::Owned(max_excerpt_chars.to_string()),
    ]);
    if let Some(fields) = fields {
        args.push(Cow::Borrowed("--fields"));
        args.push(Cow::Owned(shell_quote_arg(&fields.join(","))));
    }
    if let Some(request_id) = request_id {
        args.push(Cow::Borrowed("--request-id"));
        args.push(Cow::Owned(shell_quote_arg(request_id)));
    }
    if let Some(since) = time_filter.since {
        args.push(Cow::Borrowed("--since"));
        args.push(Cow::Owned(since.to_string()));
    }
    if let Some(until) = time_filter.until {
        args.push(Cow::Borrowed("--until"));
        args.push(Cow::Owned(until.to_string()));
    }
    if let Some(source) = source {
        args.push(Cow::Borrowed("--source"));
        args.push(Cow::Owned(shell_quote_arg(source)));
    }
    if let Some(sessions_from) = sessions_from {
        args.push(Cow::Borrowed("--sessions-from"));
        args.push(Cow::Owned(shell_quote_arg(sessions_from)));
    }
    if let Some(mode) = mode {
        args.push(Cow::Borrowed("--mode"));
        args.push(Cow::Borrowed(search_mode_label(mode)));
    }
    args.push(Cow::Borrowed("--freshness-policy"));
    args.push(Cow::Borrowed(match freshness_policy {
        PackFreshnessPolicy::PreferRecent => "prefer-recent",
        PackFreshnessPolicy::Strict => "strict",
        PackFreshnessPolicy::AllowStale => "allow-stale",
    }));
    args.push(Cow::Borrowed("--freshness-window-seconds"));
    args.push(Cow::Owned(freshness_window_seconds.to_string()));
    if include_skill_content {
        args.push(Cow::Borrowed("--include-skill-content"));
    }
    if require_evidence {
        args.push(Cow::Borrowed("--require-evidence"));
    }
    if explain_selection {
        args.push(Cow::Borrowed("--explain-selection"));
    }
    match render_format {
        PackRenderFormat::Json => args.push(Cow::Borrowed("--json")),
        PackRenderFormat::CompactJson => {
            args.extend([Cow::Borrowed("--robot-format"), Cow::Borrowed("compact")]);
        }
        PackRenderFormat::Jsonl => {
            args.extend([Cow::Borrowed("--robot-format"), Cow::Borrowed("jsonl")]);
        }
        PackRenderFormat::Toon => {
            args.extend([Cow::Borrowed("--robot-format"), Cow::Borrowed("toon")]);
        }
        PackRenderFormat::Markdown => {}
    }
    args.extend([
        Cow::Borrowed("--timeout"),
        Cow::Owned(retry_budget_ms.to_string()),
    ]);
    let arg_refs = args.iter().map(Cow::as_ref).collect::<Vec<_>>();
    cass_dataset_command(data_dir, db_path, &arg_refs)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_cli_pack(
    query: &str,
    agents: &[String],
    workspaces: &[String],
    limit: usize,
    robot_format: Option<RobotFormat>,
    fields: Option<Vec<String>>,
    max_tokens: usize,
    max_sessions: usize,
    max_evidence: usize,
    context_lines: usize,
    max_excerpt_chars: usize,
    request_id: Option<String>,
    display_format: Option<DisplayFormat>,
    data_dir_override: &Option<PathBuf>,
    db_override: Option<PathBuf>,
    time_filter: TimeFilter,
    source: Option<String>,
    sessions_from: Option<String>,
    mode: Option<crate::search::query::SearchMode>,
    freshness_policy: &str,
    freshness_window_seconds: i64,
    include_skill_content: bool,
    require_evidence: bool,
    explain_selection: bool,
    refresh: bool,
    timeout_ms: Option<u64>,
    _wrap: WrapConfig,
    interpretation: &InvocationInterpretation,
) -> CliResult<()> {
    use crate::search::pack_planner::{
        PackLexicalReadiness, PackPlanRequest, PackPlannerLimits, PackReadinessSnapshot,
        PackRenderFormat, PackRenderRequest, PackSemanticReadiness, budget_fallback_answer_pack,
        pack_candidate_fetch_limit, plan_answer_pack,
    };
    use crate::search::query::{FieldMask, SearchFilters};
    use crate::sources::provenance::SourceFilter;

    let start_time = Instant::now();
    let normalized_agents = normalize_agent_filters(agents)?;
    let query = query.trim();
    if query.is_empty() {
        return Err(CliError {
            code: 2,
            kind: CliErrorKind::PackEmptyQuery.kind_str(),
            message: "pack query is empty after trimming whitespace".to_string(),
            hint: Some(
                "Provide a non-empty query, for example `cass pack \"checkout failure\" --robot`."
                    .to_string(),
            ),
            retryable: false,
        });
    }
    // Same typed refusal as `cass search`, before any pack planning or
    // archive access runs.
    ensure_cli_query_nesting_depth(query)?;
    let render_format = resolve_pack_render_format(robot_format, display_format)?;
    let structured_pack = !matches!(render_format, PackRenderFormat::Markdown);
    let pack_budget = crate::robot_budget_envelope::RobotBudget::with_start(
        if structured_pack || timeout_ms.is_some() {
            pack_budget_ms(timeout_ms)
        } else {
            u64::MAX
        },
        start_time,
    );
    let mut skipped_sections = Vec::<String>::new();
    let mut hard_deadline_reached = false;
    let mut session_scope_empty = false;
    let freshness_policy = parse_pack_freshness_policy(freshness_policy)?;
    if freshness_window_seconds <= 0 {
        return Err(CliError {
            code: 2,
            kind: CliErrorKind::PackInvalidLimit.kind_str(),
            message: format!(
                "invalid pack limit freshness_window_seconds={freshness_window_seconds} (must be positive)"
            ),
            hint: Some(
                "Set --freshness-window-seconds to a positive number of seconds.".to_string(),
            ),
            retryable: false,
        });
    }
    let limits = PackPlannerLimits {
        max_tokens,
        max_sessions,
        max_evidence,
        context_lines,
        max_excerpt_chars,
    };
    limits.validate().map_err(pack_invalid_limit_error)?;

    let data_dir = resolve_data_dir(data_dir_override, db_override.as_ref());
    let index_path = crate::search::tantivy::expected_index_dir(&data_dir);
    let db_path = db_override
        .clone()
        .unwrap_or_else(|| data_dir.join("agent_search.db"));
    // Human pack retains stale-on-read catch-up. Structured pack is a bounded,
    // mutation-free probe and routes refresh to an explicit index command.
    if !structured_pack
        && let Some(outcome) = maybe_auto_refresh_index_after_read(&data_dir, &db_path, None)
    {
        tracing::debug!(?outcome, "pack: auto-refresh evaluated");
    }

    let mut filters = SearchFilters {
        agents: normalized_agents.into_iter().collect(),
        created_from: time_filter.since,
        created_to: time_filter.until,
        ..Default::default()
    };
    if !workspaces.is_empty() {
        filters.workspaces = HashSet::from_iter(workspaces.iter().cloned());
    }
    if let Some(ref source_str) = source {
        filters.source_filter = SourceFilter::parse(source_str);
    }
    validate_pack_fields(fields.as_ref())?;

    if refresh && !structured_pack && pack_budget.is_healthy() {
        refresh_index_inline(db_override.clone(), data_dir_override.clone());
    } else if refresh {
        skipped_sections.push("refresh".to_string());
    }

    let requested_mode = mode.unwrap_or_default();
    let mut mode_meta = SearchModeMeta::new(requested_mode, mode.is_none());
    if matches!(requested_mode, crate::search::query::SearchMode::Semantic) {
        return Err(CliError {
            code: 15,
            kind: CliErrorKind::SemanticUnavailable.kind_str(),
            message: "cass pack cannot currently run semantic-only evidence selection".to_string(),
            hint: Some("Use --mode lexical or the default hybrid-preferred mode, which fails open to lexical evidence.".to_string()),
            retryable: true,
        });
    }
    if matches!(requested_mode, crate::search::query::SearchMode::Hybrid) {
        mode_meta
            .fall_back_to_lexical("pack semantic enrichment unavailable; using lexical evidence");
    }

    if cass_not_initialized(
        db_path.exists(),
        crate::search::tantivy::searchable_index_exists(&index_path),
        probe_index_run_lock(&data_dir, &db_path, !structured_pack).active,
    ) {
        return Err(CliError {
            code: 3,
            kind: CliErrorKind::MissingIndex.kind_str(),
            message: format!(
                "cass has not been initialized in {} yet, so pack generation cannot run until the first index completes.",
                data_dir.display()
            ),
            hint: Some(cass_not_initialized_recommended_action()),
            retryable: true,
        });
    }

    if let Some(ref sessions_from_arg) = sessions_from {
        match read_session_paths_bounded(
            sessions_from_arg,
            structured_pack.then(|| pack_budget.remaining_ms()),
        )? {
            BoundedSessionPaths::Complete(session_paths) => {
                session_scope_empty = session_paths.is_empty();
                filters.session_paths = session_paths;
            }
            BoundedSessionPaths::TimedOut => {
                hard_deadline_reached = true;
                skipped_sections.push("sessions_from".to_string());
            }
        }
    }

    // 2l1b0.68: pack runs a search and echoes what it ran in `_meta.effective`
    // like `search --robot-meta`, without the search-only daemon policy.
    let effective = search_effective_interpretation(
        "pack",
        query,
        &db_path,
        effective_db_path_source(
            db_override.is_some(),
            data_dir_override.is_some(),
            interpretation,
        ),
        &time_filter,
        &filters,
        sessions_from.as_ref().map(|_| filters.session_paths.len()),
        interpretation,
        None,
    );

    let setup = if structured_pack && !hard_deadline_reached {
        let worker_data_dir = data_dir.clone();
        let worker_db_path = db_path.clone();
        let worker_index_path = index_path.clone();
        run_read_only_search_worker(pack_budget.remaining_ms(), move || {
            open_cli_search_setup(
                &worker_data_dir,
                &worker_db_path,
                &worker_index_path,
                Some(crate::search::query::SearchMode::Lexical),
                timeout_ms,
                start_time,
                true,
                true,
            )
            .map_err(|mut error| {
                if error.kind == "maintenance-required" {
                    error.hint = Some(format!(
                        "Run `cass --db {} index --full --json --data-dir {}` in a mutating workflow, then retry this pack. Structured pack output does not repair archive assets.",
                        shell_quote_arg(&worker_db_path.display().to_string()),
                        shell_quote_arg(&worker_data_dir.display().to_string()),
                    ));
                }
                error
            })
        })?
    } else if structured_pack {
        None
    } else {
        Some(open_cli_search_setup(
            &data_dir,
            &db_path,
            &index_path,
            Some(crate::search::query::SearchMode::Lexical),
            timeout_ms,
            start_time,
            true,
            false,
        )?)
    };
    let (search_self_heal, client, rebuild_active) = if let Some(CliSearchSetup {
        self_heal,
        client,
        rebuild_active,
    }) = setup
    {
        (self_heal, Some(client), rebuild_active)
    } else {
        hard_deadline_reached = true;
        if !skipped_sections
            .iter()
            .any(|section| section == "search_setup")
        {
            skipped_sections.push("search_setup".to_string());
        }
        (SearchLexicalSelfHeal::skipped(), None, false)
    };
    if search_self_heal.action != "skipped" {
        tracing::info!(
            action = search_self_heal.action,
            reason = search_self_heal.reason.as_deref(),
            indexed_docs = search_self_heal.indexed_docs,
            "pack lexical self-heal completed"
        );
    }

    let timeout_duration = timeout_ms.map(Duration::from_millis);
    if !structured_pack
        && let Some(timeout) = timeout_duration
        && start_time.elapsed() >= timeout
    {
        return Err(CliError {
            code: 10,
            kind: CliErrorKind::Timeout.kind_str(),
            message: format!(
                "Operation timed out after {}ms (before pack search started)",
                timeout.as_millis()
            ),
            hint: Some("Increase --timeout value or simplify query".to_string()),
            retryable: true,
        });
    }

    let candidate_limit = if limit == 0 {
        pack_candidate_fetch_limit(&limits).map_err(pack_invalid_limit_error)?
    } else {
        limit
    };
    let search_sparse_threshold = sparse_threshold_for_visible_limit(3, candidate_limit, false);
    let result = if session_scope_empty {
        empty_search_result()
    } else if structured_pack && (hard_deadline_reached || pack_budget.is_exhausted()) {
        hard_deadline_reached = true;
        if !skipped_sections.iter().any(|section| section == "search") {
            skipped_sections.push("search".to_string());
        }
        empty_search_result()
    } else {
        let client = client.expect("completed pack setup provides a search client");
        let query = query.to_string();
        let search_task = move || {
            maybe_test_pack_delay();
            client
                .search_with_fallback(
                    &query,
                    filters,
                    candidate_limit,
                    0,
                    search_sparse_threshold,
                    FieldMask::FULL,
                )
                .map_err(|error| {
                    let invariant_hint = lexical_engine_invariant_hint(&error);
                    CliError {
                        code: 9,
                        kind: CliErrorKind::Search.kind_str(),
                        message: format!("pack search failed: {error}"),
                        retryable: invariant_hint.is_none(),
                        hint: invariant_hint.or_else(|| {
                            Some(
                                "Try `cass search <query> --robot --robot-meta` to inspect the search path."
                                    .to_string(),
                            )
                        }),
                    }
                })
        };
        if structured_pack {
            match run_read_only_search_worker(pack_budget.remaining_ms(), search_task)? {
                Some(result) => result,
                None => {
                    hard_deadline_reached = true;
                    skipped_sections.push("search".to_string());
                    empty_search_result()
                }
            }
        } else {
            search_task()?
        }
    };

    let query_term_count = pack_query_term_count(query);
    let query_phrase_count = pack_query_phrase_count(query);
    let source_explicitly_requested = source.is_some() || sessions_from.is_some();
    let candidates = result
        .hits
        .iter()
        .enumerate()
        .map(|(index, hit)| {
            let mut candidate = crate::search::pack_planner::PackCandidate::from_search_hit(
                hit,
                query_term_count,
                query_phrase_count,
            );
            candidate.hybrid_rank = Some(index + 1);
            candidate.source_explicitly_requested = source_explicitly_requested;
            candidate
        })
        .collect::<Vec<_>>();

    let generated_at_ms = Utc::now().timestamp_millis();
    let realized_explain_selection = if explain_selection && !pack_budget.is_healthy() {
        skipped_sections.push("selection_explanations".to_string());
        false
    } else {
        explain_selection
    };
    let plan_request = PackPlanRequest {
        now_ms: generated_at_ms,
        limits: limits.clone(),
        freshness_policy,
        freshness_window_seconds,
        candidates,
        explain_selection: realized_explain_selection,
        include_skill_content,
    };
    let candidate_count = plan_request.candidates.len();
    let mut plan = if structured_pack {
        if pack_budget.is_exhausted() {
            hard_deadline_reached = true;
            skipped_sections.push("pack_planning".to_string());
            budget_fallback_answer_pack(&limits, candidate_count)
                .map_err(pack_invalid_limit_error)?
        } else {
            let (sender, receiver) = std::sync::mpsc::sync_channel(1);
            std::thread::Builder::new()
                .name("cass-pack-budgeted-planner".to_string())
                .spawn(move || {
                    maybe_test_pack_plan_delay();
                    let result = plan_answer_pack(plan_request).map(|mut plan| {
                        // Planning must not consume the source phase allowance.
                        // The remaining request budget still bounds this phase.
                        crate::search::pack_planner::verify_pack_source_citations(
                            &mut plan,
                            Instant::now()
                                + Duration::from_millis(pack_budget.remaining_ms().min(1_000)),
                        );
                        plan
                    });
                    let _ = sender.send(result);
                })
                .map_err(|error| {
                    CliError::unknown(format!(
                        "failed to start bounded pack planner worker: {error}"
                    ))
                })?;
            match receiver.recv_timeout(Duration::from_millis(pack_budget.remaining_ms().max(1))) {
                Ok(result) => result.map_err(pack_invalid_limit_error)?,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    hard_deadline_reached = true;
                    skipped_sections.push("pack_planning".to_string());
                    if realized_explain_selection {
                        skipped_sections.push("selection_explanations".to_string());
                    }
                    budget_fallback_answer_pack(&limits, candidate_count)
                        .map_err(pack_invalid_limit_error)?
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(CliError::unknown(
                        "bounded pack planner worker disconnected before reporting a result",
                    ));
                }
            }
        }
    } else {
        let mut plan = plan_answer_pack(plan_request).map_err(pack_invalid_limit_error)?;
        crate::search::pack_planner::verify_pack_source_citations(
            &mut plan,
            Instant::now() + Duration::from_millis(pack_budget.remaining_ms().min(1_000)),
        );
        plan
    };
    if skipped_sections
        .iter()
        .any(|section| section == "pack_planning")
    {
        plan.candidate_count = candidate_count;
    }

    if require_evidence && plan.evidence.is_empty() && !hard_deadline_reached {
        return Err(CliError {
            code: 13,
            kind: CliErrorKind::NotFound.kind_str(),
            message: "no evidence matched the pack query".to_string(),
            hint: Some("Broaden the query or remove restrictive filters.".to_string()),
            retryable: false,
        });
    }

    let recommended_probe_for = |sections: &[String]| {
        let refresh_skipped = sections.iter().any(|section| section == "refresh");
        if sections.is_empty() {
            None
        } else if refresh_skipped {
            Some(cass_dataset_command(
                &data_dir,
                &db_path,
                &["index", "--json"],
            ))
        } else if sessions_from.as_deref() == Some("-") {
            None
        } else {
            let retry_budget_ms = pack_budget.total_ms().saturating_mul(2).max(1_000);
            Some(pack_retry_command(
                &data_dir,
                &db_path,
                query,
                agents,
                workspaces,
                limit,
                fields.as_ref(),
                max_tokens,
                max_sessions,
                max_evidence,
                context_lines,
                max_excerpt_chars,
                request_id.as_deref(),
                &time_filter,
                source.as_deref(),
                sessions_from.as_deref(),
                mode,
                freshness_policy,
                freshness_window_seconds,
                include_skill_content,
                require_evidence,
                explain_selection,
                render_format,
                retry_budget_ms,
            ))
        }
    };
    let budget = pack_budget_block(
        &pack_budget,
        skipped_sections.clone(),
        recommended_probe_for(&skipped_sections),
        hard_deadline_reached,
    );
    let mut render_request = PackRenderRequest {
        effective: Some(effective),
        query_text: query.to_string(),
        normalized_query: query.trim().to_string(),
        generated_at_ms,
        elapsed_ms: start_time.elapsed().as_millis() as u64,
        budget,
        request_id: request_id.clone(),
        format: render_format,
        limits: limits.clone(),
        search_mode: search_mode_label(mode_meta.realized).to_string(),
        fallback_mode: mode_meta.fallback_tier.map(str::to_string),
        fallback_reason: mode_meta
            .semantic_fallback_reason()
            .map(|reason| reason.code().to_string()),
        semantic_joined: mode_meta.semantic_refinement(),
        freshness_policy,
        freshness_window_seconds,
        redaction_policy: "strict".to_string(),
        sensitive_output: false,
        explain_selection: realized_explain_selection,
        readiness: PackReadinessSnapshot {
            index_generation: search_self_heal
                .indexed_docs
                .map(|count| format!("indexed_docs:{count}")),
            lexical_readiness: if rebuild_active {
                PackLexicalReadiness::Rebuilding
            } else {
                PackLexicalReadiness::Ready
            },
            semantic_readiness: if mode_meta.fell_back_to_lexical() {
                PackSemanticReadiness::FallbackLexical
            } else {
                PackSemanticReadiness::Disabled
            },
            active_rebuild: rebuild_active,
            lock_state: rebuild_active.then(|| "index-run-lock-active".to_string()),
            missing_database: !db_path.exists(),
            source_sync_gaps: Vec::new(),
            recommended_action: None,
        },
    };

    if !structured_pack {
        let correlation = crate::search::trust_correlation::build_for_cwd();
        let rendered = render_cli_pack_with_output_budget(
            plan,
            render_request,
            fields.as_ref(),
            require_evidence,
            start_time,
            &correlation,
        )?;
        return write_cli_pack_output(&rendered);
    }

    let render_with_correlation = {
        let plan = plan.clone();
        let request = render_request.clone();
        let fields = fields.clone();
        move || {
            maybe_test_pack_render_delay();
            let correlation = crate::search::trust_correlation::build_for_cwd();
            render_cli_pack_with_output_budget(
                plan,
                request,
                fields.as_ref(),
                require_evidence,
                start_time,
                &correlation,
            )
        }
    };
    let render_without_correlation =
        |plan: &crate::search::pack_planner::PlannedAnswerPack, request: &PackRenderRequest| {
            let correlation = crate::search::trust_correlation::CorrelationIndex::default();
            render_cli_pack_with_output_budget(
                plan.clone(),
                request.clone(),
                fields.as_ref(),
                // A timed-out fallback never fabricates required evidence.
                false,
                start_time,
                &correlation,
            )
        };
    let render_budget_fallback =
        budget_fallback_answer_pack(&limits, candidate_count).map_err(pack_invalid_limit_error)?;

    let render_result = if pack_budget.is_exhausted() {
        hard_deadline_reached = true;
        if !skipped_sections
            .iter()
            .any(|section| section == "answer_pack_render")
        {
            skipped_sections.push("answer_pack_render".to_string());
        }
        if !skipped_sections
            .iter()
            .any(|section| section == "trust_correlation")
        {
            skipped_sections.push("trust_correlation".to_string());
        }
        render_request.budget = pack_budget_block(
            &pack_budget,
            skipped_sections.clone(),
            recommended_probe_for(&skipped_sections),
            hard_deadline_reached,
        );
        render_request.elapsed_ms = render_request.budget.elapsed_ms;
        render_without_correlation(&render_budget_fallback, &render_request)
    } else {
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("cass-pack-budgeted-renderer".to_string())
            .spawn(move || {
                let _ = sender.send(render_with_correlation());
            })
            .map_err(|error| {
                CliError::unknown(format!(
                    "failed to start bounded pack renderer worker: {error}"
                ))
            })?;
        match receiver.recv_timeout(Duration::from_millis(pack_budget.remaining_ms().max(1))) {
            Ok(result) => result,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                hard_deadline_reached = true;
                if !skipped_sections
                    .iter()
                    .any(|section| section == "answer_pack_render")
                {
                    skipped_sections.push("answer_pack_render".to_string());
                }
                if !skipped_sections
                    .iter()
                    .any(|section| section == "trust_correlation")
                {
                    skipped_sections.push("trust_correlation".to_string());
                }
                render_request.budget = pack_budget_block(
                    &pack_budget,
                    skipped_sections.clone(),
                    recommended_probe_for(&skipped_sections),
                    hard_deadline_reached,
                );
                render_request.elapsed_ms = render_request.budget.elapsed_ms;
                render_without_correlation(&render_budget_fallback, &render_request)
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                return Err(CliError::unknown(
                    "bounded pack renderer worker disconnected before reporting a result",
                ));
            }
        }
    };
    let rendered = render_result?;
    if fields.is_some() && matches!(render_format, PackRenderFormat::Jsonl) {
        eprintln!("Warning: --fields is ignored for pack JSONL output.");
    }
    write_cli_pack_output(&rendered)
}

pub(crate) fn render_cli_pack_with_output_budget(
    mut plan: crate::search::pack_planner::PlannedAnswerPack,
    mut request: crate::search::pack_planner::PackRenderRequest,
    fields: Option<&Vec<String>>,
    require_evidence: bool,
    start_time: Instant,
    correlation: &crate::search::trust_correlation::CorrelationIndex,
) -> CliResult<String> {
    use crate::search::pack_planner::{
        PackRenderFormat, render_answer_pack_value_with_correlation,
        render_answer_pack_with_correlation,
    };
    let token_limit = plan.diagnostics.budget.max_output_tokens_with_overflow;
    let render_error = |err: crate::search::pack_planner::PackRenderError| CliError {
        code: 9,
        kind: CliErrorKind::EncodeJson.kind_str(),
        message: format!(
            "failed to render answer pack as {}: {}",
            err.format, err.message
        ),
        hint: None,
        retryable: false,
    };
    loop {
        // Freeze runtime metadata before encoding and measuring. No bytes may
        // be appended or metadata changed after final output admission.
        if !request.budget.timed_out {
            request.elapsed_ms = start_time.elapsed().as_millis() as u64;
            request.budget.elapsed_ms = request.elapsed_ms;
        }
        let mut output = match request.format {
            PackRenderFormat::Json | PackRenderFormat::CompactJson | PackRenderFormat::Toon => {
                let value = render_answer_pack_value_with_correlation(&plan, &request, correlation)
                    .map_err(render_error)?;
                let value = filter_pack_fields(value, fields)?;
                let format = match request.format {
                    PackRenderFormat::Json => RobotFormat::Json,
                    PackRenderFormat::CompactJson => RobotFormat::Compact,
                    _ => RobotFormat::Toon,
                };
                encode_structured_value(value, format)?
            }
            PackRenderFormat::Jsonl | PackRenderFormat::Markdown => {
                render_answer_pack_with_correlation(&plan, &request, correlation)
                    .map_err(render_error)?
            }
        };
        if !matches!(request.format, PackRenderFormat::Toon) {
            output.push('\n');
        }
        let output_tokens = output.chars().count().div_ceil(4);
        if output_tokens <= token_limit {
            return Ok(output);
        }
        if !plan.reduce_for_output_budget(output_tokens - token_limit, require_evidence) {
            return Err(CliError {
                code: 2,
                kind: CliErrorKind::PackBudgetTooSmall.kind_str(),
                message: format!(
                    "pack output needs {output_tokens} estimated tokens, exceeding the {token_limit}-token tolerance for --max-tokens {}",
                    request.limits.max_tokens
                ),
                hint: Some(
                    if matches!(
                        request.format,
                        PackRenderFormat::Jsonl | PackRenderFormat::Markdown
                    ) {
                        "Increase --max-tokens or shorten query metadata; required citation fields are preserved."
                    } else {
                        "Increase --max-tokens or select fewer output fields; required citation fields are preserved."
                    }
                    .to_string(),
                ),
                retryable: false,
            });
        }
    }
}

pub(crate) fn write_cli_pack_output(output: &str) -> CliResult<()> {
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    out.write_all(output.as_bytes())
        .and_then(|()| out.flush())
        .map_err(|error| CliError {
            code: 9,
            kind: CliErrorKind::Io.kind_str(),
            message: format!("failed to write answer pack: {error}"),
            hint: None,
            retryable: false,
        })
}

pub(crate) fn resolve_pack_render_format(
    robot_format: Option<RobotFormat>,
    display_format: Option<DisplayFormat>,
) -> CliResult<crate::search::pack_planner::PackRenderFormat> {
    use crate::search::pack_planner::PackRenderFormat;

    if matches!(
        display_format,
        Some(DisplayFormat::Table | DisplayFormat::Lines)
    ) {
        return Err(CliError {
            code: 2,
            kind: CliErrorKind::PackUnsupportedFormat.kind_str(),
            message: "cass pack only supports markdown for human display".to_string(),
            hint: Some(
                "Use --display markdown, --json/--robot, or --robot-format json|compact|jsonl|toon."
                    .to_string(),
            ),
            retryable: false,
        });
    }

    match robot_format {
        Some(RobotFormat::Json) => Ok(PackRenderFormat::Json),
        Some(RobotFormat::Compact) => Ok(PackRenderFormat::CompactJson),
        Some(RobotFormat::Jsonl) => Ok(PackRenderFormat::Jsonl),
        Some(RobotFormat::Toon) => Ok(PackRenderFormat::Toon),
        Some(RobotFormat::Sessions) => Err(CliError {
            code: 2,
            kind: CliErrorKind::PackUnsupportedFormat.kind_str(),
            message: "cass pack does not support --robot-format sessions".to_string(),
            hint: Some(
                "Use `cass search --robot-format sessions | cass pack <query> --robot --sessions-from -`."
                    .to_string(),
            ),
            retryable: false,
        }),
        None => Ok(PackRenderFormat::Markdown),
    }
}

pub(crate) fn pack_invalid_limit_error(
    err: crate::search::pack_planner::PackPlannerLimitError,
) -> CliError {
    CliError {
        code: 2,
        kind: CliErrorKind::PackInvalidLimit.kind_str(),
        message: format!(
            "invalid pack limit {}={} (allowed {}..={})",
            err.field, err.value, err.min, err.max
        ),
        hint: Some(
            "Adjust --max-tokens, --max-sessions, --max-evidence, --context-lines, or --max-excerpt-chars."
                .to_string(),
        ),
        retryable: false,
    }
}

pub(crate) fn parse_pack_freshness_policy(
    value: &str,
) -> CliResult<crate::search::pack_planner::PackFreshnessPolicy> {
    match value {
        "prefer-recent" => Ok(crate::search::pack_planner::PackFreshnessPolicy::PreferRecent),
        "strict" => Ok(crate::search::pack_planner::PackFreshnessPolicy::Strict),
        "allow-stale" => Ok(crate::search::pack_planner::PackFreshnessPolicy::AllowStale),
        other => Err(CliError::usage(
            format!("invalid freshness policy: {other}"),
            Some("Use prefer-recent, strict, or allow-stale.".to_string()),
        )),
    }
}

pub(crate) fn pack_query_term_count(query: &str) -> usize {
    query
        .split_whitespace()
        .filter(|term| !term.trim_matches('"').is_empty())
        .count()
}

pub(crate) fn pack_query_phrase_count(query: &str) -> usize {
    query.matches('"').count() / 2
}
