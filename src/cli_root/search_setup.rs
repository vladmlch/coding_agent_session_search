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

/// Resolve the effective `(timeout_ms, limit, mode)` for a `cass search` /
/// `cass pack` invocation by layering the configurable defaults (#303) under
/// the explicit CLI flags.
///
/// Precedence (highest wins): CLI flag > env var > `~/.config/cass/cass.toml`
/// `[search]` > built-in default. The config file is loaded once per
/// invocation; a present-but-broken file or a malformed env value is surfaced
/// as a clear usage error rather than silently ignored.
///
/// `cli_limit == 0` is treated as "not explicitly set" so a configured default
/// limit engages for the common `cass search "<q>"` (no `--limit`) case; an
/// explicit `--limit 0` is semantically identical to the unset default ("no
/// limit", still RAM-capped downstream), so nothing surprising happens there.
pub(crate) fn resolve_search_defaults(
    cli_timeout: Option<u64>,
    cli_limit: usize,
    cli_mode: Option<crate::search::query::SearchMode>,
) -> CliResult<(Option<u64>, usize, Option<crate::search::query::SearchMode>)> {
    use crate::search_defaults as sd;

    let defaults = sd::load_search_defaults().map_err(|e| {
        CliError::usage(
            e.to_string(),
            Some("Fix or remove ~/.config/cass/cass.toml; expected a [search] table".to_string()),
        )
    })?;

    let (timeout, _t_src) = sd::resolve_timeout_ms(
        cli_timeout,
        sd::timeout_env().as_deref(),
        defaults.timeout_ms,
    )
    .map_err(|e| {
        CliError::usage(
            e,
            Some("Set a non-negative integer of milliseconds".to_string()),
        )
    })?;

    let (limit, _l_src) = sd::resolve_limit(
        cli_limit,
        cli_limit != 0,
        sd::limit_env().as_deref(),
        defaults.limit,
    )
    .map_err(|e| CliError::usage(e, Some("Set a non-negative integer".to_string())))?;

    let cli_mode_str = cli_mode.map(search_mode_canonical_str);
    let (mode_str, _m_src) = sd::resolve_mode(
        cli_mode_str,
        sd::mode_env().as_deref(),
        defaults.mode.as_deref(),
    )
    .map_err(|e| CliError::usage(e, Some("Use one of: lexical, semantic, hybrid".to_string())))?;
    let mode = mode_str.and_then(|m| search_mode_from_canonical_str(&m));

    Ok((timeout, limit, mode))
}

/// Canonical lowercase name for a [`crate::search::query::SearchMode`].
pub(crate) fn search_mode_canonical_str(mode: crate::search::query::SearchMode) -> &'static str {
    use crate::search::query::SearchMode;
    match mode {
        SearchMode::Lexical => "lexical",
        SearchMode::Semantic => "semantic",
        SearchMode::Hybrid => "hybrid",
    }
}

/// Inverse of [`search_mode_canonical_str`]; input is already validated by
/// `search_defaults::resolve_mode`, so an unknown value is a no-op `None`.
pub(crate) fn search_mode_from_canonical_str(s: &str) -> Option<crate::search::query::SearchMode> {
    use crate::search::query::SearchMode;
    match s {
        "lexical" => Some(SearchMode::Lexical),
        "semantic" => Some(SearchMode::Semantic),
        "hybrid" => Some(SearchMode::Hybrid),
        _ => None,
    }
}

pub(crate) fn search_budget_ms(timeout_ms: Option<u64>) -> u64 {
    timeout_ms.unwrap_or_else(|| {
        dotenvy::var("CASS_SEARCH_BUDGET_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|&milliseconds| milliseconds > 0)
            // Multi-shard federated Tantivy open on multi-GB archives routinely
            // exceeds the historical 8s default (cold open alone can be 20–30s+).
            // Prefer completing a real search over returning 0 hits on budget
            // exhaustion. Override down via CASS_SEARCH_BUDGET_MS or --timeout.
            .unwrap_or(120_000)
    })
}

/// Deterministic slowdown injected inside a bounded read-only worker for E2E
/// coverage. Production callers never set these test-only environment variables.
pub(crate) fn maybe_test_search_worker_delay(variable: &str) {
    if let Ok(raw) = dotenvy::var(variable)
        && let Ok(milliseconds) = raw.parse::<u64>()
        && milliseconds > 0
    {
        std::thread::sleep(Duration::from_millis(milliseconds));
    }
}

/// Execute read-only work on a detached worker and return at the hard deadline.
/// Callers must not use this for repairs, writes, downloads, or persistent
/// process spawning.
pub(crate) fn run_read_only_search_worker<T, F>(remaining_ms: u64, work: F) -> CliResult<Option<T>>
where
    T: Send + 'static,
    F: FnOnce() -> CliResult<T> + Send + 'static,
{
    use std::sync::mpsc::{RecvTimeoutError, sync_channel};

    if remaining_ms == 0 {
        return Ok(None);
    }
    let (sender, receiver) = sync_channel(1);
    std::thread::Builder::new()
        .name("cass-bounded-read-only".to_string())
        .spawn(move || {
            let _ = sender.send(work());
        })
        .map_err(|error| CliError {
            code: 9,
            kind: CliErrorKind::Search.kind_str(),
            message: format!("failed to start bounded read-only worker: {error}"),
            hint: Some("Retry the bounded read-only operation.".to_string()),
            retryable: true,
        })?;

    match receiver.recv_timeout(Duration::from_millis(remaining_ms)) {
        Ok(result) => result.map(Some),
        Err(RecvTimeoutError::Timeout) => Ok(None),
        Err(RecvTimeoutError::Disconnected) => Err(CliError {
            code: 9,
            kind: CliErrorKind::Search.kind_str(),
            message: "bounded read-only worker stopped before returning a result".to_string(),
            hint: Some("Retry the bounded read-only operation.".to_string()),
            retryable: true,
        }),
    }
}

/// Deterministic slowdown for bounded-search E2E coverage. Production callers
/// never set this test-only environment variable.
pub(crate) fn maybe_test_search_delay() {
    if let Ok(raw) = dotenvy::var("CASS_TEST_SEARCH_SLOW_MS")
        && let Ok(milliseconds) = raw.parse::<u64>()
        && milliseconds > 0
    {
        std::thread::sleep(Duration::from_millis(milliseconds));
    }
}

pub(crate) fn search_budget_retry_command(
    query: &str,
    format: RobotFormat,
    budget_ms: u64,
    dataset: (&Path, &Path),
    sessions_from: Option<&str>,
    mode: Option<crate::search::query::SearchMode>,
    request_args: &[String],
) -> Option<String> {
    if sessions_from == Some("-") {
        return None;
    }
    let (data_dir, db_path) = dataset;
    let mut command = vec![
        "cass".to_string(),
        "--db".to_string(),
        shell_quote_arg(db_path.to_str()?),
        "search".to_string(),
    ];
    match format {
        RobotFormat::Json => command.push("--robot".to_string()),
        RobotFormat::Jsonl => {
            command.extend(["--robot-format".to_string(), "jsonl".to_string()]);
        }
        RobotFormat::Compact => {
            command.extend(["--robot-format".to_string(), "compact".to_string()]);
        }
        RobotFormat::Sessions => {
            command.extend(["--robot-format".to_string(), "sessions".to_string()]);
        }
        RobotFormat::Toon => {
            command.extend(["--robot-format".to_string(), "toon".to_string()]);
        }
    }
    command.extend([
        "--timeout".to_string(),
        budget_ms
            .saturating_mul(2)
            .clamp(1_000, 300_000)
            .to_string(),
    ]);
    if let Some(sessions_from) = sessions_from {
        command.push(shell_quote_arg(&format!("--sessions-from={sessions_from}")));
    }
    if let Some(mode) = mode {
        let mode = match mode {
            crate::search::query::SearchMode::Lexical => "lexical",
            crate::search::query::SearchMode::Semantic => "semantic",
            crate::search::query::SearchMode::Hybrid => "hybrid",
        };
        command.extend(["--mode".to_string(), mode.to_string()]);
    }
    command.push("--data-dir".to_string());
    command.push(shell_quote_arg(data_dir.to_str()?));
    command.extend(request_args.iter().map(|arg| shell_quote_arg(arg)));
    command.extend(["--".to_string(), shell_quote_arg(query)]);
    Some(command.join(" "))
}

pub(crate) fn output_search_budget_partial(
    query: &str,
    format: RobotFormat,
    budget: &crate::robot_budget_envelope::RobotBudget,
    skipped_sections: Vec<String>,
    retry: Option<String>,
    mode_meta: (SearchModeMeta, bool),
) -> CliResult<()> {
    let (mode_meta, include_meta) = mode_meta;
    let mut budget_block = crate::robot_budget_envelope::BudgetBlock::from_budget(
        budget,
        skipped_sections,
        retry.clone(),
    );
    budget_block.elapsed_ms = budget_block.elapsed_ms.max(budget_block.budget_ms);
    budget_block.timed_out = true;

    if matches!(format, RobotFormat::Sessions) {
        let error_payload = serde_json::json!({
            "error": {
                "code": 10,
                "kind": CliErrorKind::Timeout.kind_str(),
                "message": "sessions-format search did not complete within its robot budget",
                "hint": retry.clone(),
                "retryable": true,
            },
            "budget": budget_block,
        });
        return Err(CliError {
            code: 10,
            kind: CliErrorKind::Timeout.kind_str(),
            message: error_payload.to_string(),
            hint: retry,
            retryable: true,
        });
    }

    let mut payload = serde_json::json!({
        "query": query,
        "hits": [],
        "total_matches": 0,
        "has_more": false,
        "budget": budget_block,
    });
    if include_meta {
        payload["_meta"] = serde_json::json!({
            "requested_search_mode": mode_meta.requested,
            "search_mode": mode_meta.realized,
            "mode_defaulted": mode_meta.defaulted,
            "fallback_tier": null,
            "fallback_reason": null,
            "semantic_refinement": false,
        });
    }
    // GH #422 follow-on: exit 0 with `hits: []` is the documented timeout
    // contract, but with a quiet stderr a caller that checks only the exit
    // status and the hit list reads it as "no history matches". Name the
    // partial result on the diagnostics stream; stdout stays data-only.
    let budget_ms = payload["budget"]["budget_ms"].as_u64().unwrap_or_default();
    output_structured_value(payload, format)?;
    eprintln!(
        "note: search did not finish within its {budget_ms} ms budget; the empty hit list is \
         incomplete, not a no-match result (budget.timed_out=true){}",
        retry
            .as_deref()
            .map(|retry| format!("; retry: {retry}"))
            .unwrap_or_default()
    );
    Ok(())
}

pub(crate) struct CliSearchSetup {
    pub(crate) self_heal: SearchLexicalSelfHeal,
    pub(crate) client: std::sync::Arc<crate::search::query::SearchClient>,
    pub(crate) rebuild_active: bool,
}

pub(crate) fn empty_search_result() -> crate::search::query::SearchResult {
    crate::search::query::SearchResult {
        hits: Vec::new(),
        wildcard_fallback: false,
        cache_stats: crate::search::query::CacheStats::default(),
        suggestions: Vec::new(),
        ann_stats: None,
        ann_unavailable_reason: None,
        total_count: None,
    }
}

/// Remedy for a lexical query the engine refused with a posting-cursor
/// invariant failure, or `None` for every other failure. That refusal belongs
/// to the published generation, not the moment: retrying the same query
/// against the same segments fails the same way, so an envelope carrying this
/// hint must also say `retryable: false` (GH #499, where agents following the
/// contract retried a date-filtered search forever).
pub(crate) fn lexical_engine_invariant_hint(error: &anyhow::Error) -> Option<String> {
    crate::search::quill_bridge::is_engine_invariant_failure(error).then(|| {
        "the lexical engine rejected this query on the current index generation, and \
         retrying fails the same way. Run 'cass index --full --force-rebuild' to publish a \
         clean generation, and report the query with `cass --version` if it recurs (GH #499 \
         was one such engine defect, fixed in frankensearch-quill 0.3.2)"
            .to_string()
    })
}

/// Remedy for a lexical query the engine refused because a prefix or
/// wildcard literal expanded past its per-leaf dictionary cap, or `None`
/// for every other failure. Like the invariant remedy, this refusal is
/// deterministic — the same query fails the same way — so an envelope
/// carrying this hint must also say `retryable: false`, and the engine's
/// own message names the pattern that exploded.
pub(crate) fn lexical_glob_expansion_hint(error: &anyhow::Error) -> Option<String> {
    crate::search::quill_bridge::is_glob_expansion_limited(error).then(|| {
        "a prefix or wildcard term expanded past the lexical engine's per-leaf \
         dictionary cap; lengthen the prefix before the wildcard (or use a rarer \
         term) and run the search again"
            .to_string()
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_search_operation(
    client: std::sync::Arc<crate::search::query::SearchClient>,
    query: String,
    filters: crate::search::query::SearchFilters,
    search_limit: usize,
    search_offset: usize,
    search_sparse_threshold: usize,
    field_mask: crate::search::query::FieldMask,
    approximate: bool,
    semantic_execution_tier: crate::search::query::SemanticTierMode,
    mut mode_meta: SearchModeMeta,
) -> CliResult<(crate::search::query::SearchResult, SearchModeMeta)> {
    use crate::search::query::SearchMode;

    maybe_test_search_worker_delay("CASS_TEST_SEARCH_SLOW_MS");
    let hybrid_fail_open = mode_meta.fail_open_on_semantic_unavailable();
    let result = match mode_meta.realized {
        SearchMode::Lexical => client
            .search_with_fallback(
                &query,
                filters.clone(),
                search_limit,
                search_offset,
                search_sparse_threshold,
                field_mask,
            )
            .map_err(|error| {
                // GH #441: a lexical-only search has no semantic leg to
                // degrade to, so name the real remedies instead of a bare
                // engine error.
                let fuel_exhausted = crate::search::quill_bridge::is_query_fuel_exhausted(&error);
                let invariant_hint = lexical_engine_invariant_hint(&error);
                let glob_hint = lexical_glob_expansion_hint(&error);
                let retryable = invariant_hint.is_none() && glob_hint.is_none();
                let hint = if fuel_exhausted {
                    Some(format!(
                        "the lexical engine hit its per-query work ceiling (usually a long, \
                         stopword-heavy query on an archive with many index segments); retry \
                         with fewer or rarer terms, raise {} above {}, or run 'cass index' \
                         (its maintenance pass consolidates segments in place; 'cass index \
                         --full' rebuilds from scratch and needs the headroom doctor's \
                         full_rebuild_readiness reports)",
                        crate::search::quill_bridge::CASS_QUILL_QUERY_FUEL_BUDGET_ENV,
                        crate::search::quill_bridge::cass_quill_config().query_fuel_budget
                    ))
                } else {
                    glob_hint.or(invariant_hint)
                };
                CliError {
                    code: 9,
                    kind: CliErrorKind::Search.kind_str(),
                    message: format!("search failed: {error}"),
                    hint,
                    retryable,
                }
            })?,
        SearchMode::Semantic => {
            let (hits, ann_stats) = client
                .search_semantic_with_tier(
                    &query,
                    filters.clone(),
                    search_limit,
                    search_offset,
                    field_mask,
                    approximate,
                    semantic_execution_tier,
                )
                .map_err(|error| {
                    let message = error.to_string();
                    if message.contains("HNSW index") {
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
                    } else if message.contains("unavailable") || message.contains("no embedder") {
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
                            message: format!("semantic search failed: {error}"),
                            hint: Some(
                                "Retry with the default hybrid-preferred mode when lexical evidence is acceptable"
                                    .to_string(),
                            ),
                            retryable: true,
                        }
                    }
                })?;
            let ann_unavailable_reason = if approximate {
                client
                    .ann_unavailability_reason()
                    .map_err(|error| CliError {
                        code: 9,
                        kind: CliErrorKind::Search.kind_str(),
                        message: format!("failed to inspect ANN fallback state: {error}"),
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
        SearchMode::Hybrid => match client.search_hybrid(
            &query,
            &query,
            filters.clone(),
            search_limit,
            search_offset,
            search_sparse_threshold,
            field_mask,
            approximate,
        ) {
            Ok(result) => result,
            Err(error) => {
                let message = error.to_string();
                if hybrid_fail_open
                    && (message.contains("unavailable") || message.contains("no embedder"))
                {
                    mode_meta
                        .fall_back_to_lexical(format!("hybrid execution unavailable: {error}"));
                    client
                        .search_with_fallback(
                            &query,
                            filters,
                            search_limit,
                            search_offset,
                            search_sparse_threshold,
                            field_mask,
                        )
                        .map_err(|fallback_error| {
                            let invariant_hint =
                                lexical_engine_invariant_hint(&fallback_error);
                            let glob_hint =
                                lexical_glob_expansion_hint(&fallback_error);
                            CliError {
                                code: 9,
                                kind: CliErrorKind::Search.kind_str(),
                                message: format!(
                                    "hybrid search failed ({error}); lexical fallback failed: {fallback_error}"
                                ),
                                retryable: invariant_hint.is_none() && glob_hint.is_none(),
                                hint: glob_hint.or(invariant_hint),
                            }
                        })?
                } else if message.contains("unavailable") || message.contains("no embedder") {
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
                    let invariant_hint = lexical_engine_invariant_hint(&error);
                    let glob_hint = lexical_glob_expansion_hint(&error);
                    return Err(CliError {
                        code: 9,
                        kind: CliErrorKind::Search.kind_str(),
                        message: format!("hybrid search failed: {error}"),
                        retryable: invariant_hint.is_none() && glob_hint.is_none(),
                        hint: glob_hint.or(invariant_hint).or_else(|| {
                            Some(
                                "Retry with the default hybrid-preferred mode when lexical evidence is acceptable"
                                    .to_string(),
                            )
                        }),
                    });
                }
            }
        },
    };
    mode_meta.lexical_degrade_reason = client.lexical_degrade_reason();
    mode_meta.wildcard_fallback_skipped = client.wildcard_fallback_skipped_reason();
    Ok((result, mode_meta))
}

pub(crate) fn configure_bounded_search_semantics(
    client: std::sync::Arc<crate::search::query::SearchClient>,
    data_dir: PathBuf,
    db_path: PathBuf,
    mut mode_meta: SearchModeMeta,
    semantic_opts: SemanticSearchOptions,
    no_maintenance: bool,
) -> CliResult<(SearchModeMeta, Option<String>)> {
    use crate::search::embedder_registry::{EmbedderRegistry, HASH_EMBEDDER};
    use crate::search::model_manager::{
        load_hash_semantic_context, load_hash_semantic_context_strict, load_semantic_context,
        load_semantic_context_deferred, load_semantic_context_deferred_strict,
        load_semantic_context_for_embedder, load_semantic_context_for_embedder_deferred,
        load_semantic_context_for_embedder_deferred_strict,
        load_semantic_context_for_embedder_strict, load_semantic_context_strict,
    };
    use std::sync::Arc;

    maybe_test_search_worker_delay("CASS_TEST_SEARCH_SEMANTIC_SETUP_SLOW_MS");

    let registry = EmbedderRegistry::new(&data_dir);
    let env_model = dotenvy::var("CASS_SEMANTIC_EMBEDDER")
        .ok()
        .and_then(|value| {
            if value.trim().eq_ignore_ascii_case("hash") {
                Some("hash")
            } else {
                // A leftover `external` (or any other unrecognized) value is
                // ignored: canonical_name returns None and auto selection
                // applies. Legacy settings must never reach a provider.
                crate::search::fastembed_embedder::FastEmbedder::canonical_name(&value)
            }
        });
    let requested_model = semantic_opts.model.as_deref().or(env_model);
    if let Some(model_name) = semantic_opts.model.as_deref()
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

    let embedder_info = match requested_model {
        Some(name) => registry.get(name),
        None => Some(registry.best_available()),
    };
    let daemon_embedder_id = embedder_info
        .filter(|entry| entry.is_semantic)
        .map(|entry| entry.id.to_string());
    let prefer_hash = semantic_opts.tier_mode == crate::search::query::SemanticTierMode::FastOnly
        || embedder_info.is_some_and(|entry| entry.name == HASH_EMBEDDER);
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
        let embedder: Arc<dyn crate::search::embedder::Embedder> =
            if semantic_opts.use_daemon && !prefer_hash && daemon_embedder_compatible {
                #[cfg(unix)]
                {
                    // A bounded robot worker never spawns a persistent daemon. It
                    // may use an already-running, attested daemon or fall back to
                    // the in-process embedder.
                    let daemon =
                        crate::daemon::client::try_connect_for_embedder(embedder.id(), &data_dir);
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

        if let Err(error) = client.set_semantic_artifacts_context(
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
            if mode_meta.fail_open_on_semantic_unavailable() {
                mode_meta.fall_back_to_lexical(format!("semantic context rejected: {error}"));
                let _ = client.clear_semantic_context();
            } else {
                return Err(CliError {
                    code: 15,
                    kind: CliErrorKind::SemanticUnavailable.kind_str(),
                    message: format!("Semantic search not available: {error}"),
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
        if mode_meta.fail_open_on_semantic_unavailable() {
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

    Ok((mode_meta, daemon_embedder_id))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn open_cli_search_setup(
    data_dir: &Path,
    db_path: &Path,
    index_path: &Path,
    mode: Option<crate::search::query::SearchMode>,
    timeout_ms: Option<u64>,
    started_at: Instant,
    no_maintenance: bool,
    robot_bounded_degraded: bool,
) -> CliResult<CliSearchSetup> {
    use crate::search::query::{SearchClient, SearchClientOptions};

    maybe_test_search_worker_delay("CASS_TEST_SEARCH_SETUP_SLOW_MS");
    let (self_heal, opened_index) = if search_request_skips_lexical_self_heal(mode) {
        (
            SearchLexicalSelfHeal::skipped(),
            Some(crate::search::tantivy::OpenedLexicalIndex {
                path: index_path.to_path_buf(),
                reader: None,
                federated_readers: None,
            }),
        )
    } else if no_maintenance {
        inspect_lexical_assets_for_search_read_only(data_dir, db_path, index_path)?
    } else {
        ensure_lexical_assets_for_search_admitting(
            data_dir,
            db_path,
            index_path,
            timeout_ms,
            started_at,
            false,
            robot_bounded_degraded,
        )?
    };
    let lexical_initialized = crate::search::tantivy::searchable_index_exists(index_path);
    let rebuild_active = probe_index_run_lock(
        data_dir,
        db_path,
        !no_maintenance && !search_request_skips_lexical_self_heal(mode),
    )
    .active;
    let db_exists = db_path.exists();
    let options = SearchClientOptions {
        enable_reload: false,
        enable_warm: false,
        strict_read_only: no_maintenance,
    };
    let client = match opened_index {
        Some(index) => SearchClient::from_opened_lexical_index(index, Some(db_path), options),
        None => SearchClient::open_with_options(index_path, Some(db_path), options),
    };
    let client = std::sync::Arc::new(
        client.map_err(|error| CliError {
            code: 9,
            kind: CliErrorKind::OpenIndex.kind_str(),
            message: format!("failed to open index: {error}"),
            hint: Some("try cass index --full".to_string()),
            retryable: true,
        })?
        .ok_or_else(|| {
            let (message, hint) = if rebuild_active && !lexical_initialized {
                (
                    format!(
                        "cass is already building the initial search index in {}. Search will become available when that index run finishes.",
                        data_dir.display()
                    ),
                    Some("Wait for the active 'cass index' run to finish, or inspect progress with 'cass status --json'.".to_string()),
                )
            } else if cass_not_initialized(db_exists, lexical_initialized, rebuild_active) {
                (
                    format!(
                        "cass has not been initialized in {} yet, so search cannot run until the first index completes.",
                        data_dir.display()
                    ),
                    Some(cass_not_initialized_recommended_action()),
                )
            } else if db_exists && !lexical_initialized {
                (
                    format!(
                        "Search index not found at {}. The archive database exists, but the lexical index has not been built yet.",
                        index_path.display()
                    ),
                    Some("Run 'cass index --full' to build the search index for this archive.".to_string()),
                )
            } else if !db_exists && lexical_initialized {
                (
                    format!(
                        "Search index exists at {}, but the archive database {} is missing.",
                        index_path.display(),
                        db_path.display()
                    ),
                    Some(
                        "Restore the canonical archive from backup if historical coverage matters; otherwise run 'cass index --full' to create a new archive from currently available source sessions."
                            .to_string(),
                    ),
                )
            } else {
                (
                    format!(
                        "Index not found at {}. Run 'cass index --full' first.",
                        index_path.display()
                    ),
                    Some(
                        "Run 'cass index --full' to create the local archive and search index."
                            .to_string(),
                    ),
                )
            };
            CliError {
                code: 3,
                kind: CliErrorKind::MissingIndex.kind_str(),
                message,
                hint,
                retryable: true,
            }
        })?,
    );

    Ok(CliSearchSetup {
        self_heal,
        client,
        rebuild_active,
    })
}

/// Force a dispatch-boundary budget transition for fresh-process integration tests.
/// Production callers never set this test-only environment variable.
pub(crate) fn maybe_test_force_semantic_dispatch_budget_phase(
    budget: Option<&crate::robot_budget_envelope::RobotBudget>,
) {
    use crate::robot_budget_envelope::{BudgetPhase, NEAR_LIMIT_FRACTION};

    let Ok(raw_phase) = dotenvy::var("CASS_TEST_SEMANTIC_DISPATCH_BUDGET_PHASE") else {
        return;
    };
    let target_phase = match raw_phase.trim() {
        "near-limit" => BudgetPhase::NearLimit,
        "exhausted" => BudgetPhase::Exhausted,
        _ => return,
    };
    let Some(budget) = budget else {
        return;
    };

    let budget_ms = budget.total_ms();
    let target_elapsed_ms = match target_phase {
        BudgetPhase::NearLimit => {
            let percent = (NEAR_LIMIT_FRACTION * 100.0) as u128;
            u64::try_from((u128::from(budget_ms) * percent) / 100).unwrap_or(u64::MAX)
        }
        BudgetPhase::Exhausted => budget_ms,
        BudgetPhase::Healthy => return,
    };
    let elapsed_ms = budget.elapsed_ms();
    if elapsed_ms < target_elapsed_ms {
        let sleep_ms = target_elapsed_ms
            .saturating_sub(elapsed_ms)
            .saturating_add(1);
        tracing::warn!(
            target_phase = ?target_phase,
            elapsed_ms,
            budget_ms,
            sleep_ms,
            test_hook = true,
            "forcing semantic dispatch budget transition"
        );
        std::thread::sleep(Duration::from_millis(sleep_ms));
    }
}

/// Refuse a query nested deeper than the grammar's stack bound
/// ([`crate::search::query::MAX_QUERY_NESTING_DEPTH`]) before any search
/// or pack lane runs: the recursive boolean stages behind every lane
/// (parser, renderers, source-scan evaluation, AST teardown) must never
/// see such a query, and the caller gets the limit in the typed
/// `query-depth-exceeded` envelope — the same fix-the-input family as
/// `pack-empty-query`, never a retry.
pub(crate) fn ensure_cli_query_nesting_depth(query: &str) -> CliResult<()> {
    if let Err(exceeded) = crate::search::query::ensure_query_nesting_depth(query) {
        return Err(CliError {
            code: 2,
            kind: CliErrorKind::QueryDepthExceeded.kind_str(),
            message: format!(
                "query nests {} groups deep; the maximum is {}",
                exceeded.depth, exceeded.limit
            ),
            hint: Some(format!(
                "Keep the query at or under {} nested groups of parentheses — \
                 split a deeply nested query into several searches.",
                exceeded.limit
            )),
            retryable: false,
        });
    }
    Ok(())
}

#[cfg(test)]
mod lexical_engine_invariant_hint_tests {
    use super::*;

    /// GH #499: the reporter's exact envelope message gets the remedy (and
    /// therefore `retryable: false` at every call site); fuel exhaustion and
    /// reader faults keep their existing, retryable handling.
    #[test]
    fn only_a_cursor_invariant_failure_gets_the_non_retryable_remedy() {
        let invariant = anyhow::anyhow!(
            "executing a Quill lexical query: posting cursor invariant failed: Boolean \
             children belong to different segment domains"
        );
        let hint = lexical_engine_invariant_hint(&invariant)
            .expect("the #499 engine failure must carry a remedy");
        assert!(hint.contains("cass index --full --force-rebuild"), "{hint}");
        assert!(hint.contains("retrying fails the same way"), "{hint}");

        for other in [
            anyhow::anyhow!(
                "executing a Quill lexical query: query fuel exhausted after 10/10 units"
            ),
            anyhow::anyhow!("opening the Quill CASS reader: manifest missing"),
            anyhow::anyhow!("database is locked"),
        ] {
            assert_eq!(lexical_engine_invariant_hint(&other), None, "{other}");
        }
    }
}
