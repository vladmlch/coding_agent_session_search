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

pub(crate) struct SearchPresentationInput<'a> {
    pub(crate) query: &'a str,
    pub(crate) filters: crate::search::query::SearchFilters,
    pub(crate) result: crate::search::query::SearchResult,
    pub(crate) start_time: Instant,
    pub(crate) search_budget: Option<crate::robot_budget_envelope::RobotBudget>,
    pub(crate) skipped_sections: Vec<String>,
    pub(crate) explain: bool,
    pub(crate) has_aggregation: bool,
    pub(crate) agg_fields: Vec<AggregateField>,
    pub(crate) limit_val: usize,
    pub(crate) offset_val: usize,
    pub(crate) cursor_page_limit: usize,
    pub(crate) max_content_length: Option<usize>,
    pub(crate) max_tokens: Option<usize>,
    pub(crate) robot_meta: bool,
    pub(crate) data_dir: PathBuf,
    pub(crate) db_path: PathBuf,
    pub(crate) no_maintenance: bool,
    pub(crate) effective_robot: Option<RobotFormat>,
    pub(crate) client: Arc<crate::search::query::SearchClient>,
    pub(crate) field_mask: crate::search::query::FieldMask,
    pub(crate) sessions_filter_stats: Option<SessionsFilterStats>,
    pub(crate) fields: Option<Vec<String>>,
    pub(crate) mode_meta: SearchModeMeta,
    pub(crate) search_retry: Option<String>,
    pub(crate) highlight: bool,
    pub(crate) request_id: Option<String>,
    pub(crate) cursor: Option<String>,
    pub(crate) search_ms: u64,
    pub(crate) rerank_ms: u64,
    pub(crate) effective: serde_json::Value,
    pub(crate) display_format: Option<DisplayFormat>,
    pub(crate) wrap: WrapConfig,
}

pub(crate) fn present_cli_search_results(input: SearchPresentationInput<'_>) -> CliResult<()> {
    use crate::search::query::QueryExplanation;

    let SearchPresentationInput {
        query,
        filters,
        result,
        start_time,
        search_budget,
        mut skipped_sections,
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
    } = input;

    // Build query explanation if requested
    let explanation_budget_available = search_budget
        .as_ref()
        .is_none_or(|budget| budget.is_healthy());
    let explanation = if explain && !explanation_budget_available {
        skipped_sections.push("explanation".to_string());
        None
    } else if explain {
        Some(
            QueryExplanation::analyze(query, &filters)
                .with_wildcard_fallback(result.wildcard_fallback),
        )
    } else {
        None
    };

    // Compute aggregations and create display result based on mode
    let (aggregations, mut display_result, total_matches, has_more_results, total_matches_exact) =
        if has_aggregation
            && search_budget
                .as_ref()
                .is_none_or(|budget| budget.is_healthy())
        {
            // Compute aggregations from all fetched results
            let aggs = compute_aggregations(&result.hits, &agg_fields);
            let total = result.hits.len();

            // Apply offset and limit to get display hits.
            // When limit_val == 0 (meaning "no limit"), take all results.
            let agg_effective_limit = if limit_val == 0 {
                usize::MAX
            } else {
                limit_val
            };
            let display_hits: Vec<_> = result
                .hits
                .iter()
                .skip(offset_val)
                .take(agg_effective_limit)
                .cloned()
                .collect();

            let display = crate::search::query::SearchResult {
                hits: display_hits,
                wildcard_fallback: result.wildcard_fallback,
                cache_stats: result.cache_stats,
                suggestions: result.suggestions.clone(),
                ann_stats: result.ann_stats.clone(),
                ann_unavailable_reason: result.ann_unavailable_reason,
                total_count: result.total_count,
            };
            let has_more = total > offset_val + display.hits.len();
            (aggs, display, total, has_more, false)
        } else if has_aggregation {
            skipped_sections.push("aggregations".to_string());
            let total = result.hits.len();
            let effective_limit = if limit_val == 0 {
                usize::MAX
            } else {
                limit_val
            };
            let display_hits: Vec<_> = result
                .hits
                .iter()
                .skip(offset_val)
                .take(effective_limit)
                .cloned()
                .collect();
            let has_more = total > offset_val.saturating_add(display_hits.len());
            let display = crate::search::query::SearchResult {
                hits: display_hits,
                wildcard_fallback: result.wildcard_fallback,
                cache_stats: result.cache_stats,
                suggestions: result.suggestions.clone(),
                ann_stats: result.ann_stats.clone(),
                ann_unavailable_reason: result.ann_unavailable_reason,
                total_count: result.total_count,
            };
            (Aggregations::default(), display, total, has_more, false)
        } else {
            // No aggregation - result was over-fetched by one to derive pagination state.
            // When limit_val == 0 (meaning "no limit"), take all results.
            let total_matches_exact = result.total_count.is_some();
            let has_more = cursor_page_limit > 0 && result.hits.len() > cursor_page_limit;
            let effective_limit = if cursor_page_limit == 0 {
                usize::MAX
            } else {
                cursor_page_limit
            };
            let display_hits: Vec<_> = result.hits.into_iter().take(effective_limit).collect();
            // Use the true total from Tantivy's Count collector when available;
            // fall back to the page-window lower bound for semantic/hybrid/cached paths.
            let known_total = result.total_count.unwrap_or_else(|| {
                offset_val
                    .saturating_add(display_hits.len())
                    .saturating_add(usize::from(has_more))
            });
            let display = crate::search::query::SearchResult {
                hits: display_hits,
                wildcard_fallback: result.wildcard_fallback,
                cache_stats: result.cache_stats,
                suggestions: result.suggestions,
                ann_stats: result.ann_stats,
                ann_unavailable_reason: result.ann_unavailable_reason,
                total_count: result.total_count,
            };
            (
                Aggregations::default(),
                display,
                known_total,
                has_more,
                total_matches_exact,
            )
        };

    let elapsed_ms = start_time.elapsed().as_millis() as u64;

    // Derive per-field budgets, preferring snippet > content > title
    let (snippet_budget, content_budget, title_budget, fallback_budget) = {
        let base = max_content_length;
        if let Some(tokens) = max_tokens {
            let char_budget = tokens.saturating_mul(4);
            let per_hit = char_budget / std::cmp::max(1, display_result.hits.len());
            let snippet = std::cmp::max(16, (per_hit as f64 * 0.5) as usize);
            let content = std::cmp::max(12, (per_hit as f64 * 0.35) as usize);
            let title = std::cmp::max(8, (per_hit as f64 * 0.15) as usize);
            (
                Some(snippet),
                Some(content),
                Some(title),
                base.map(|b| std::cmp::min(b, per_hit)),
            )
        } else {
            (base, base, base, base)
        }
    };

    let truncation_budgets = FieldBudgets {
        snippet: snippet_budget,
        content: content_budget,
        title: title_budget,
        fallback: fallback_budget,
    };

    // Gather state meta for robot output (index/db freshness)
    let mut state_meta_budget_available = search_budget
        .as_ref()
        .is_none_or(|budget| budget.is_healthy());
    let state_meta = if robot_meta && !state_meta_budget_available {
        skipped_sections.push("state_meta".to_string());
        None
    } else if robot_meta && search_budget.is_some() {
        let worker_data_dir = data_dir.clone();
        let worker_db_path = db_path.clone();
        let state = run_read_only_search_worker(
            search_budget
                .as_ref()
                .expect("robot metadata has a search budget")
                .remaining_ms(),
            move || {
                maybe_test_search_worker_delay("CASS_TEST_SEARCH_META_SLOW_MS");
                Ok(if no_maintenance {
                    state_meta_json_for_strict_read(
                        &worker_data_dir,
                        &worker_db_path,
                        DEFAULT_STALE_THRESHOLD_SECS,
                    )
                } else {
                    state_meta_json(
                        &worker_data_dir,
                        &worker_db_path,
                        DEFAULT_STALE_THRESHOLD_SECS,
                        true,
                    )
                })
            },
        )?;
        if state.is_none() {
            state_meta_budget_available = false;
            skipped_sections.push("state_meta".to_string());
            skipped_sections.push("search_completeness".to_string());
        }
        state
    } else if robot_meta {
        Some(if no_maintenance {
            state_meta_json_for_strict_read(&data_dir, &db_path, DEFAULT_STALE_THRESHOLD_SECS)
        } else {
            state_meta_json(&data_dir, &db_path, DEFAULT_STALE_THRESHOLD_SECS, true)
        })
    } else {
        None
    };
    let robot_freshness = state_meta.as_ref().and_then(state_index_freshness);
    // Stale-on-read catch-up: never blocks this search; a detached
    // low-priority `cass index --background` makes the *next* search fresh.
    let auto_refresh = if no_maintenance
        || search_budget
            .as_ref()
            .is_some_and(crate::robot_budget_envelope::RobotBudget::is_exhausted)
    {
        None
    } else {
        maybe_auto_refresh_index_after_read(&data_dir, &db_path, robot_freshness.as_ref())
    };
    let robot_freshness = robot_freshness.map(|mut freshness| {
        if let (Some(outcome), serde_json::Value::Object(map)) = (&auto_refresh, &mut freshness) {
            map.insert("auto_refresh".to_string(), outcome.clone());
        }
        freshness
    });
    // i6tyy (GH #375 / #353): human search previously never learned the
    // index was stale — `state_meta` is a `--robot-meta`-only probe, so the
    // v6vuz stderr readiness banner below could never fire and a stale
    // index returning zero hits read as "that history does not exist".
    // Derive the same freshness verdict for human output from the
    // filesystem-only strict-read state (no DB open, no checkpoint or lock
    // mutation, the `--no-maintenance` surface) so the common healthy
    // search stays cheap and mutation-free. Deliberately NOT fed into the
    // stale-on-read auto-refresh above: human search must not start
    // spawning background index runs as a side effect of this warning.
    let human_freshness =
        if effective_robot.is_none() && state_meta.is_none() && state_meta_budget_available {
            state_index_freshness(&state_meta_json_for_strict_read(
                &data_dir,
                &db_path,
                DEFAULT_STALE_THRESHOLD_SECS,
            ))
        } else {
            None
        };
    let index_freshness = robot_freshness.or(human_freshness);
    // qfswx: project the `.14.1` storage-integrity verdict for --robot-meta
    // (Some only when state_meta is, i.e. with --robot-meta) so search agrees
    // with doctor/status on the canonical StorageState vocabulary. Computed
    // before state_meta is consumed into state_meta_with_warning below.
    let storage_integrity_meta = state_meta.as_ref().map(|state| {
        let not_initialized = cass_not_initialized(
            db_path.exists(),
            cass_lexical_index_initialized(&data_dir),
            false,
        );
        storage_integrity_value_from_state(&data_dir, &db_path, state, not_initialized)
    });
    // [coding_agent_session_search #301] A PARTIAL (aborted/interrupted)
    // index is a stronger, distinct signal than age-staleness: search
    // results silently omit conversations indexed after the abort. Prefer
    // the partial warning and surface its dedicated reason so agents stop
    // trusting the result set as complete.
    let partial_warning = index_freshness
        .as_ref()
        .and_then(|f: &serde_json::Value| f.get("partial"))
        .and_then(|v: &serde_json::Value| v.as_bool())
        .filter(|partial| *partial)
        .map(|_| {
            index_freshness
                .as_ref()
                .and_then(|f: &serde_json::Value| f.get("partial_reason"))
                .and_then(|v: &serde_json::Value| v.as_str())
                .map(str::to_string)
                .unwrap_or_else(|| {
                    "Lexical index is PARTIAL (a prior `cass index --full` was aborted); results may omit conversations. Re-run `cass index --full`.".to_string()
                })
        });
    let warning = partial_warning.or_else(|| {
        index_freshness
            .as_ref()
            .and_then(|f: &serde_json::Value| f.get("stale"))
            .and_then(|v: &serde_json::Value| v.as_bool())
            .filter(|stale| *stale)
            .map(|_| {
                let age = index_freshness
                    .as_ref()
                    .and_then(|f: &serde_json::Value| f.get("age_seconds"))
                    .and_then(|v: &serde_json::Value| v.as_u64()).map_or_else(|| "an unknown age".to_string(), |s| format!("{s} seconds"));
                let pending = index_freshness
                    .as_ref()
                    .and_then(|f: &serde_json::Value| f.get("pending_sessions"))
                    .and_then(|v: &serde_json::Value| v.as_u64())
                    .unwrap_or(0);
                format!(
                    "Index may be stale (age: {age}; pending sessions: {pending}). Run `cass index --full` or enable watch mode for fresh results."
                )
            })
    });

    let index_freshness_for_closure = index_freshness.clone();
    let state_meta_with_warning = state_meta.map(|mut meta| {
        if let Some(fresh) = index_freshness_for_closure
            && let serde_json::Value::Object(ref mut m) = meta
        {
            m.insert("index_freshness".to_string(), fresh);
        }
        if let Some(warn) = &warning
            && let serde_json::Value::Object(ref mut m) = meta
        {
            m.insert(
                "_warning".to_string(),
                serde_json::Value::String(warn.clone()),
            );
        }
        meta
    });

    // uojcg.3.3: with --robot-meta, attach a compact search-completeness verdict
    // to _meta when conversations are quarantined, so agents know results exclude
    // known content (distinct from mere staleness). Skipped entirely otherwise to
    // keep the common complete-coverage payload unchanged.
    let search_completeness = if robot_meta && state_meta_budget_available {
        let quarantine = crate::indexer::conversation_ingest_quarantine_summary(&data_dir);
        if quarantine.quarantined_conversations > 0 || quarantine.circuit_breaker_active {
            serde_json::to_value(
                crate::search::quarantine_status::project_search_completeness(
                    quarantine.quarantined_conversations as u64,
                    quarantine.circuit_breaker_active,
                ),
            )
            .ok()
        } else {
            None
        }
    } else {
        None
    };

    // uojcg.7.1: explain an empty search filtered to one workspace. The probes
    // run only on that empty path, inside the remaining robot budget. An empty
    // page past the last match (`--offset` beyond the total) is not an empty
    // search, so the total must be zero too.
    let zero_result_diagnosis = if display_result.hits.is_empty()
        && total_matches == 0
        && filters.workspaces.len() == 1
        && !skipped_sections.iter().any(|section| section == "search")
    {
        let probe_client = Arc::clone(&client);
        let probe_query = query.to_string();
        let probe_filters = filters.clone();
        let probe_db_path = db_path.clone();
        let probe =
            move || -> CliResult<Option<crate::search::zero_result_diagnosis::ZeroResultReport>> {
                Ok(diagnose_empty_workspace_search(
                    &probe_client,
                    &probe_query,
                    &probe_filters,
                    field_mask,
                    &probe_db_path,
                ))
            };
        match search_budget.as_ref() {
            Some(budget) if budget.is_healthy() => {
                run_read_only_search_worker(budget.remaining_ms(), probe)
                    .ok()
                    .flatten()
                    .flatten()
            }
            Some(_) => None,
            None => probe().ok().flatten(),
        }
    } else {
        None
    };

    // Bead v6vuz: captured before the output chain because `warning` and
    // `effective_robot` are conditionally moved into the robot branch below.
    let is_human_search = effective_robot.is_none();
    let has_readiness_warning = warning.is_some();
    // i6tyy: keep the human copy before `warning` is moved into the robot
    // envelope below.
    let human_warning = if is_human_search {
        warning.clone()
    } else {
        None
    };

    if let Some(format) = effective_robot {
        let expanded_trust_fields = expand_field_presets(&fields);
        let minimal_trust_projection = expanded_trust_fields.as_ref().is_some_and(|fields| {
            fields
                .iter()
                .map(String::as_str)
                .eq(SEARCH_MINIMAL_FIELDS.iter().copied())
        });
        let trust_projection_requested =
            robot_meta && !minimal_trust_projection && !display_result.hits.is_empty();
        let trust_values = if trust_projection_requested {
            let budget = search_budget
                .as_ref()
                .expect("robot output always establishes a search budget");
            if budget.is_healthy() {
                let trust_hits = display_result.hits.clone();
                let realized = trust_realized_mode(mode_meta.realized);
                run_read_only_search_worker(budget.remaining_ms(), move || {
                    maybe_test_search_worker_delay("CASS_TEST_SEARCH_TRUST_SLOW_MS");
                    let correlation = crate::search::trust_correlation::build_for_cwd();
                    let now_ms = crate::storage::sqlite::FrankenStorage::now_millis();
                    let query_workspace = correlation.project_workspace();
                    Ok(trust_hits
                        .iter()
                        .map(|hit| {
                            trust_value_for_hit(
                                hit,
                                now_ms,
                                realized,
                                &correlation,
                                query_workspace.as_deref(),
                            )
                        })
                        .collect::<Vec<_>>())
                })?
            } else {
                None
            }
        } else {
            None
        };
        if trust_projection_requested && trust_values.is_none() {
            skipped_sections.push("trust_correlation".to_string());
        }
        let refresh_skipped = skipped_sections.iter().any(|section| section == "refresh");
        let recommended_next_probe = if skipped_sections.is_empty() {
            None
        } else if refresh_skipped {
            Some(cass_dataset_command(
                &data_dir,
                &db_path,
                &["index", "--json"],
            ))
        } else {
            search_retry.clone()
        };
        let budget = crate::robot_budget_envelope::BudgetBlock::from_budget(
            search_budget
                .as_ref()
                .expect("robot output always establishes a search budget"),
            skipped_sections,
            recommended_next_probe,
        );
        let timed_out = budget.timed_out;
        if matches!(format, RobotFormat::Sessions)
            && (budget.timed_out || !budget.skipped_sections.is_empty())
        {
            let retry_hint = budget.recommended_next_probe.clone();
            let error_payload = serde_json::json!({
                "error": {
                    "code": 10,
                    "kind": CliErrorKind::Timeout.kind_str(),
                    "message": "sessions-format search did not complete within its robot budget",
                    "hint": retry_hint.clone(),
                    "retryable": true,
                },
                "budget": budget,
            });
            return Err(CliError {
                code: 10,
                kind: CliErrorKind::Timeout.kind_str(),
                message: error_payload.to_string(),
                hint: retry_hint,
                retryable: true,
            });
        }
        // 2l1b0.68: --highlight marks robot snippets exactly as it marks the
        // human output; robot mode used to accept the flag and ignore it.
        if highlight {
            for hit in &mut display_result.hits {
                hit.snippet = highlight_matches(&hit.snippet, query, "**", "**");
            }
        }
        // Robot output mode (JSON)
        output_robot_results(
            query,
            limit_val,
            cursor_page_limit,
            offset_val,
            &display_result,
            format,
            robot_meta,
            elapsed_ms,
            &fields,
            truncation_budgets,
            max_tokens,
            request_id.clone(),
            cursor.clone(),
            has_more_results,
            total_matches_exact,
            state_meta_with_warning,
            search_completeness,
            index_freshness,
            storage_integrity_meta,
            warning,
            &aggregations,
            total_matches,
            explanation.as_ref(),
            timed_out,
            Some(budget.budget_ms),
            &budget,
            trust_values.as_deref(),
            mode_meta,
            search_ms,
            rerank_ms,
            sessions_filter_stats,
            effective,
            zero_result_diagnosis.as_ref(),
        )?;
        if timed_out {
            emit_search_timeout_stderr_note(
                budget.budget_ms,
                budget.recommended_next_probe.as_deref(),
                display_result.hits.is_empty(),
            );
        }
    } else if display_result.hits.is_empty() {
        // GH#414: when the --sessions-from filter provably selected zero
        // indexed sessions, say so — the empty result reflects the filter,
        // and the natural (wrong) recovery is to broaden the query, doubt
        // the index, and rebuild it before ever suspecting the filter.
        if let Some(stats) = sessions_filter_stats
            && stats.provably_unmatched()
        {
            eprintln!(
                "No results: none of the {} path(s) supplied via --sessions-from match an \
                 indexed session. The empty result reflects the filter, not the query — index \
                 the sessions first (cass index) or check the paths.",
                stats.requested
            );
        } else {
            eprintln!("No results found.");
        }
        if let Some(report) = &zero_result_diagnosis {
            use crate::search::zero_result_diagnosis::ZeroResultDiagnosis;
            let note = match report.diagnosis {
                ZeroResultDiagnosis::WorkspaceFilterLikelyWrong => {
                    "The --workspace filter matches no indexed workspace exactly, but a close one exists."
                }
                ZeroResultDiagnosis::WorkspaceNotIndexed => {
                    "The --workspace filter matches no indexed workspace, and the query has hits elsewhere."
                }
                ZeroResultDiagnosis::WorkspaceHasNoMatch => {
                    "The workspace is indexed; the query has hits in other workspaces but not in this one."
                }
                ZeroResultDiagnosis::SourceIdFilter | ZeroResultDiagnosis::GenuineNoMatch => "",
            };
            if !note.is_empty() {
                eprintln!("{note}");
            }
            if let Some(rerun) = &report.suggested_rerun {
                eprintln!("Hint: {rerun}");
            }
        }
    } else if let Some(display) = display_format {
        // Human-readable display formats
        output_display_results(&display_result.hits, display, wrap, query, highlight)?;
    } else {
        // Default plain text output
        for hit in &display_result.hits {
            println!("----------------------------------------------------------------");
            println!(
                "Score: {:.2} | Agent: {} | WS: {}",
                hit.score, hit.agent, hit.workspace
            );
            println!("Path: {}", hit.source_path);
            let snippet = hit.snippet.replace('\n', " ");
            let snippet = if highlight {
                highlight_matches(&snippet, query, "**", "**")
            } else {
                snippet
            };
            println!("Snippet: {}", apply_wrap(&snippet, wrap));
        }
        println!("----------------------------------------------------------------");
    }

    // Bead v6vuz: in human (non-robot) search mode, surface the same bounded
    // readiness banner the robot `_meta` carries when the lexical index is
    // stale or partial — so a human is told results may be incomplete instead
    // of silently trusting them, in the same vocabulary as `--robot-meta`. The
    // banner goes to stderr (stdout stays results-only) and the truth-table
    // probe runs only on the already-degraded path (a staleness/partial
    // `warning` exists), so the common healthy search stays probe-free.
    if is_human_search && has_readiness_warning {
        // i6tyy: the one-line note itself — byte-identical to the robot
        // `_meta._warning` string so human and agent readers are told the
        // same thing (zero hits on a stale index is coverage, not absence).
        if let Some(warn) = &human_warning {
            eprintln!("Warning: {warn}");
        }
        let table =
            build_truth_table_for_data_dir(&data_dir, &db_path, DEFAULT_STALE_THRESHOLD_SECS);
        let summary = crate::search::human_readiness_summary::project_human_summary(
            &table,
            crate::search::readiness_projection::SurfaceKind::SearchMeta,
        );
        if summary.is_safe(table.archive_risk) {
            eprintln!("{}", summary.render_compact_line());
        }
    }
    if is_human_search && let Some(reason) = display_result.ann_unavailable_reason {
        eprintln!("{}", ann_fallback_human_warning(reason));
    }

    Ok(())
}
