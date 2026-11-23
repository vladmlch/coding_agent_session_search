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

/// Output search results in robot-friendly format
#[allow(clippy::too_many_arguments, unused_variables)]
pub(crate) fn output_robot_results(
    query: &str,
    limit: usize,
    cursor_page_limit: usize,
    offset: usize,
    result: &crate::search::query::SearchResult,
    format: RobotFormat,
    include_meta: bool,
    elapsed_ms: u64,
    fields: &Option<Vec<String>>,
    truncation_budgets: FieldBudgets,
    max_tokens: Option<usize>,
    request_id: Option<String>,
    input_cursor: Option<String>,
    has_more_results: bool,
    total_matches_exact: bool,
    state_meta: Option<serde_json::Value>,
    // uojcg.3.3: compact search-completeness verdict, present only with
    // --robot-meta and only when conversations are quarantined (so the common
    // complete-coverage case stays byte-for-byte unchanged).
    search_completeness: Option<serde_json::Value>,
    index_freshness: Option<serde_json::Value>,
    // qfswx: `.14.1` storage-integrity block, present only with --robot-meta
    // (Some mirrors state_meta), so search agrees with doctor/status on the
    // canonical StorageState vocabulary.
    storage_integrity: Option<serde_json::Value>,
    warning: Option<String>,
    aggregations: &Aggregations,
    total_matches: usize,
    explanation: Option<&crate::search::query::QueryExplanation>,
    timed_out: bool,
    timeout_ms: Option<u64>,
    budget: &crate::robot_budget_envelope::BudgetBlock,
    trust_values: Option<&[serde_json::Value]>,
    search_mode_meta: SearchModeMeta,
    search_ms: u64,
    rerank_ms: u64,
    // GH#414: present iff --sessions-from was supplied.
    sessions_filter: Option<SessionsFilterStats>,
    // 2l1b0.68: `_meta.effective`, from `search_effective_interpretation`.
    effective: serde_json::Value,
    // uojcg.7.1: present iff an empty search was filtered to one workspace.
    zero_result_diagnosis: Option<&crate::search::zero_result_diagnosis::ZeroResultReport>,
) -> CliResult<()> {
    use std::io::{BufWriter, Write};

    if matches!(format, RobotFormat::Sessions) {
        // Output unique session paths only, one per line.
        // This format is designed for chained searches via --sessions-from.
        use std::collections::BTreeSet;
        let paths: BTreeSet<&str> = result
            .hits
            .iter()
            .map(|hit| hit.source_path.as_str())
            .collect();
        for path in paths {
            println!("{path}");
        }
        return Ok(());
    }

    // Expand presets (minimal, summary, provenance, all, *)
    let resolved_fields = expand_field_presets(fields);

    // Filter hits to requested fields, and only apply truncation when limits are configured.
    let all_fields_requested = resolved_fields
        .as_ref()
        .is_none_or(|fields| fields.is_empty());
    let minimal_projection = resolved_fields.as_ref().is_some_and(|fields| {
        fields
            .iter()
            .map(String::as_str)
            .eq(SEARCH_MINIMAL_FIELDS.iter().copied())
    });
    let summary_projection = resolved_fields.as_ref().is_some_and(|fields| {
        fields
            .iter()
            .map(String::as_str)
            .eq(SEARCH_SUMMARY_FIELDS.iter().copied())
    });
    let needs_truncation = truncation_budgets.has_any_limit();
    let passthrough_all_fields = all_fields_requested;
    let output_projection = if minimal_projection {
        "minimal"
    } else if summary_projection {
        "summary"
    } else if passthrough_all_fields {
        "all"
    } else {
        "custom"
    };

    // Fast path: summary-field JSON output without optional metadata/features.
    // Avoid intermediary serde_json::Value maps and string clones per hit.
    if matches!(format, RobotFormat::Json)
        && summary_projection
        && !needs_truncation
        && max_tokens.is_none()
        && !include_meta
        && warning.is_none()
        && aggregations.is_empty()
        && result.suggestions.is_empty()
        && explanation.is_none()
        && !timed_out
        // GH#414: --sessions-from responses carry the sessions_filter block,
        // which only the general payload builder emits.
        && sessions_filter.is_none()
    {
        use serde::ser::{SerializeMap, SerializeSeq};
        use serde::{Serialize, Serializer};

        struct SummaryHitCompat<'a>(&'a crate::search::query::SearchHit);

        impl Serialize for SummaryHitCompat<'_> {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                let hit = self.0;
                let mut map = serializer.serialize_map(Some(SEARCH_SUMMARY_FIELDS.len()))?;
                map.serialize_entry("source_path", &hit.source_path)?;
                map.serialize_entry("line_number", &hit.line_number)?;
                map.serialize_entry("conversation_id", &hit.conversation_id)?;
                map.serialize_entry("agent", &hit.agent)?;
                map.serialize_entry("title", &hit.title)?;
                map.serialize_entry("source_id", &normalized_robot_hit_source_id(hit))?;
                let safe_score = safe_robot_score_value(hit.score);
                map.serialize_entry("score", &safe_score)?;
                map.end()
            }
        }

        fn serialize_summary_hits<S>(
            hits: &[crate::search::query::SearchHit],
            serializer: S,
        ) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut seq = serializer.serialize_seq(Some(hits.len()))?;
            for hit in hits {
                seq.serialize_element(&SummaryHitCompat(hit))?;
            }
            seq.end()
        }

        #[derive(serde::Serialize)]
        struct FastSummaryJsonPayload<'a> {
            query: &'a str,
            limit: usize,
            offset: usize,
            count: usize,
            total_matches: usize,
            #[serde(serialize_with = "serialize_summary_hits")]
            hits: &'a [crate::search::query::SearchHit],
            max_tokens: Option<usize>,
            request_id: Option<String>,
            cursor: Option<String>,
            hits_clamped: bool,
            budget: &'a crate::robot_budget_envelope::BudgetBlock,
        }

        let payload = FastSummaryJsonPayload {
            query,
            limit,
            offset,
            count: result.hits.len(),
            total_matches,
            hits: &result.hits,
            max_tokens,
            request_id,
            cursor: input_cursor,
            hits_clamped: false,
            budget,
        };
        let stdout = std::io::stdout();
        let mut out = BufWriter::new(stdout.lock());
        serde_json::to_writer_pretty(&mut out, &payload).map_err(|e| CliError {
            code: 9,
            kind: CliErrorKind::EncodeJson.kind_str(),
            message: format!("failed to encode json: {e}"),
            hint: None,
            retryable: false,
        })?;
        writeln!(&mut out).map_err(|e| CliError {
            code: 9,
            kind: CliErrorKind::EncodeJson.kind_str(),
            message: format!("failed to write trailing newline: {e}"),
            hint: None,
            retryable: false,
        })?;
        out.flush().map_err(|e| CliError {
            code: 9,
            kind: CliErrorKind::EncodeJson.kind_str(),
            message: format!("failed to flush buffered json output: {e}"),
            hint: None,
            retryable: false,
        })?;
        return Ok(());
    }

    // Fast path: full-field JSON output without optional metadata/features.
    // Serialize hits directly with compatibility formatting to avoid
    // materializing intermediary serde_json::Value hits.
    if matches!(format, RobotFormat::Json)
        && all_fields_requested
        && !needs_truncation
        && max_tokens.is_none()
        && !include_meta
        && warning.is_none()
        && aggregations.is_empty()
        && result.suggestions.is_empty()
        && explanation.is_none()
        && !timed_out
        // GH#414: --sessions-from responses carry the sessions_filter block,
        // which only the general payload builder emits.
        && sessions_filter.is_none()
    {
        use serde::ser::{SerializeMap, SerializeSeq};
        use serde::{Serialize, Serializer};

        struct FullHitCompat<'a>(&'a crate::search::query::SearchHit);

        impl Serialize for FullHitCompat<'_> {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                let hit = self.0;
                let normalized_source_id = normalized_robot_hit_source_id(hit);
                let normalized_origin_host = normalized_robot_hit_origin_host(hit);
                let mut fields = 13usize;
                if hit.workspace_original.is_some() {
                    fields += 1;
                }
                if normalized_origin_host.is_some() {
                    fields += 1;
                }
                let mut map = serializer.serialize_map(Some(fields))?;
                map.serialize_entry("title", &hit.title)?;
                map.serialize_entry("snippet", &hit.snippet)?;
                map.serialize_entry("content", &hit.content)?;
                let safe_score = safe_robot_score_value(hit.score);
                map.serialize_entry("score", &safe_score)?;
                map.serialize_entry("source_path", &hit.source_path)?;
                map.serialize_entry("agent", &hit.agent)?;
                map.serialize_entry("workspace", &hit.workspace)?;
                if let Some(ref workspace_original) = hit.workspace_original {
                    map.serialize_entry("workspace_original", workspace_original)?;
                }
                map.serialize_entry("created_at", &hit.created_at)?;
                map.serialize_entry("conversation_id", &hit.conversation_id)?;
                map.serialize_entry("line_number", &hit.line_number)?;
                map.serialize_entry("match_type", &hit.match_type)?;
                let normalized_origin_kind = normalized_robot_hit_origin_kind(hit);
                map.serialize_entry("source_id", &normalized_source_id)?;
                map.serialize_entry("origin_kind", &normalized_origin_kind)?;
                if let Some(ref origin_host) = normalized_origin_host {
                    map.serialize_entry("origin_host", origin_host)?;
                }
                map.end()
            }
        }

        fn serialize_full_hits<S>(
            hits: &[crate::search::query::SearchHit],
            serializer: S,
        ) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut seq = serializer.serialize_seq(Some(hits.len()))?;
            for hit in hits {
                seq.serialize_element(&FullHitCompat(hit))?;
            }
            seq.end()
        }

        #[derive(serde::Serialize)]
        struct FastJsonPayload<'a> {
            query: &'a str,
            limit: usize,
            offset: usize,
            count: usize,
            total_matches: usize,
            #[serde(serialize_with = "serialize_full_hits")]
            hits: &'a [crate::search::query::SearchHit],
            max_tokens: Option<usize>,
            request_id: Option<String>,
            cursor: Option<String>,
            hits_clamped: bool,
            budget: &'a crate::robot_budget_envelope::BudgetBlock,
        }

        let payload = FastJsonPayload {
            query,
            limit,
            offset,
            count: result.hits.len(),
            total_matches,
            hits: &result.hits,
            max_tokens,
            request_id,
            cursor: input_cursor,
            hits_clamped: false,
            budget,
        };
        let stdout = std::io::stdout();
        let mut out = BufWriter::new(stdout.lock());
        serde_json::to_writer_pretty(&mut out, &payload).map_err(|e| CliError {
            code: 9,
            kind: CliErrorKind::EncodeJson.kind_str(),
            message: format!("failed to encode json: {e}"),
            hint: None,
            retryable: false,
        })?;
        writeln!(&mut out).map_err(|e| CliError {
            code: 9,
            kind: CliErrorKind::EncodeJson.kind_str(),
            message: format!("failed to write trailing newline: {e}"),
            hint: None,
            retryable: false,
        })?;
        out.flush().map_err(|e| CliError {
            code: 9,
            kind: CliErrorKind::EncodeJson.kind_str(),
            message: format!("failed to flush buffered json output: {e}"),
            hint: None,
            retryable: false,
        })?;
        return Ok(());
    }

    let filtered_hits: Vec<serde_json::Value> = if minimal_projection {
        result
            .hits
            .iter()
            .map(|hit| {
                serde_json::json!({
                    "source_path": hit.source_path.as_str(),
                    "line_number": hit.line_number,
                    "agent": hit.agent.as_str(),
                    "source_id": normalized_robot_hit_source_id(hit),
                    "conversation_id": hit.conversation_id,
                })
            })
            .collect()
    } else if summary_projection && !needs_truncation {
        result
            .hits
            .iter()
            .map(|hit| {
                let mut map = serde_json::Map::with_capacity(SEARCH_SUMMARY_FIELDS.len());
                map.insert(
                    "source_path".to_string(),
                    serde_json::Value::String(hit.source_path.clone()),
                );
                map.insert(
                    "line_number".to_string(),
                    serde_json::to_value(hit.line_number).unwrap_or_default(),
                );
                map.insert(
                    "agent".to_string(),
                    serde_json::Value::String(hit.agent.clone()),
                );
                map.insert(
                    "title".to_string(),
                    serde_json::Value::String(hit.title.clone()),
                );
                map.insert("score".to_string(), safe_robot_score_value(hit.score));
                map.insert(
                    "source_id".to_string(),
                    serde_json::Value::String(normalized_robot_hit_source_id(hit)),
                );
                map.insert(
                    "conversation_id".to_string(),
                    serde_json::to_value(hit.conversation_id).unwrap_or_default(),
                );
                serde_json::Value::Object(map)
            })
            .collect()
    } else if passthrough_all_fields && !needs_truncation {
        // Sanitize NaN/Infinity scores before bulk serialization — serde_json
        // cannot represent non-finite floats, which would silently drop all hits.
        let sanitized: Vec<_> = result
            .hits
            .iter()
            .map(normalized_robot_hit_for_output)
            .collect();
        match serde_json::to_value(&sanitized).unwrap_or_default() {
            serde_json::Value::Array(values) => values,
            _ => Vec::new(),
        }
    } else if needs_truncation {
        result
            .hits
            .iter()
            .map(|hit| filter_hit_fields(hit, &resolved_fields))
            .map(|hit| apply_content_truncation(hit, truncation_budgets))
            .collect()
    } else {
        result
            .hits
            .iter()
            .map(|hit| filter_hit_fields(hit, &resolved_fields))
            .collect()
    };

    // Clamp hits to token budget if provided (approx 4 chars per token)
    // Must match the JSONL header condition in the output match below.
    let jsonl_meta_emitted = matches!(format, RobotFormat::Jsonl)
        && (include_meta
            || !aggregations.is_empty()
            || !result.suggestions.is_empty()
            || explanation.is_some()
            || budget.timed_out);
    let estimate_tokens = max_tokens.is_some() || include_meta || jsonl_meta_emitted;
    let (mut filtered_hits, tokens_estimated, hits_clamped) =
        clamp_hits_to_budget(filtered_hits, max_tokens, estimate_tokens);

    // 5u82n.3: attach a metadata-only trust/provenance verdict per hit. Gated on
    // --robot-meta so the fast paths above stay byte-identical, and skipped for
    // the minimal projection (intentionally source_path/line_number/agent only).
    // Advisory metadata — result ordering is untouched. `filtered_hits` is a
    // prefix projection of `result.hits` (same order; clamp only drops the
    // tail), so zip aligns each verdict with its hit.
    if include_meta
        && !minimal_projection
        && let Some(trust_values) = trust_values
    {
        for (trust, value) in trust_values.iter().zip(filtered_hits.iter_mut()) {
            if let serde_json::Value::Object(map) = value {
                map.insert("trust".to_string(), trust.clone());
            }
        }
    }

    let search_page_count = result.hits.len();
    let returned_count = filtered_hits.len();
    let clamped_unemitted_hits = returned_count < search_page_count;
    let cursor_has_more = has_more_results || clamped_unemitted_hits;
    let realized_cursor_limit = if cursor_page_limit == 0 {
        limit
    } else {
        cursor_page_limit
    };
    let next_cursor = if cursor_has_more && cursor_page_limit > 0 && returned_count > 0 {
        encode_search_cursor(offset.saturating_add(returned_count), cursor_page_limit)
    } else {
        None
    };
    let query_plan = crate::query_cost_planner::build_query_cost_plan(
        crate::query_cost_planner::QueryCostPlanInput {
            query_chars: query.chars().count(),
            requested_mode: search_mode_label(search_mode_meta.requested).to_string(),
            realized_mode: search_mode_label(search_mode_meta.realized).to_string(),
            fallback_tier: search_mode_meta.fallback_tier.map(str::to_string),
            fallback_reason: search_mode_meta.fallback_reason.clone(),
            semantic_refinement: search_mode_meta.semantic_refinement(),
            wildcard_fallback: result.wildcard_fallback,
            limit: realized_cursor_limit,
            offset,
            returned_count,
            total_matches,
            max_tokens,
            tokens_estimated,
            hits_clamped,
            timeout_ms,
            timed_out,
            input_cursor_present: input_cursor.is_some(),
            next_cursor_present: next_cursor.is_some(),
            output_projection: output_projection.to_string(),
            cache_hits: result.cache_stats.cache_hits,
            cache_misses: result.cache_stats.cache_miss,
            cache_shortfall: result.cache_stats.cache_shortfall,
            aggregation_count: usize::from(aggregations.agent.is_some())
                + usize::from(aggregations.workspace.is_some())
                + usize::from(aggregations.date.is_some())
                + usize::from(aggregations.match_type.is_some()),
        },
    );
    let query_plan_json = serde_json::to_value(&query_plan).unwrap_or(serde_json::Value::Null);
    let cursor_manifest_json = search_cursor_manifest_json(SearchCursorManifestInput {
        requested_limit: limit,
        realized_limit: realized_cursor_limit,
        offset,
        returned_count,
        search_page_count,
        total_matches,
        total_matches_exact,
        has_more: cursor_has_more,
        input_cursor_present: input_cursor.is_some(),
        next_cursor_present: next_cursor.is_some(),
        hits_clamped,
        tokens_estimated,
        max_tokens,
        timed_out,
        output_projection,
        requested_fields: fields,
        resolved_fields: &resolved_fields,
        search_mode_meta: &search_mode_meta,
        cache_stats: &result.cache_stats,
        index_freshness: index_freshness.as_ref(),
    });
    let index_rebuilding = index_freshness
        .as_ref()
        .and_then(|freshness| freshness.get("rebuilding"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let pending_sessions = index_freshness
        .as_ref()
        .and_then(|freshness| freshness.get("pending_sessions"))
        .and_then(serde_json::Value::as_u64);
    let explanation_cards = crate::explainability::search_robot_explanation_cards(
        crate::explainability::SearchRobotExplanationInput {
            requested_mode: search_mode_label(search_mode_meta.requested).to_string(),
            realized_mode: search_mode_label(search_mode_meta.realized).to_string(),
            fallback_tier: search_mode_meta.fallback_tier.map(str::to_string),
            fallback_reason: search_mode_meta.fallback_reason.clone(),
            semantic_refinement: search_mode_meta.semantic_refinement(),
            wildcard_fallback: result.wildcard_fallback,
            cache_policy: result.cache_stats.eviction_policy.to_string(),
            cache_hits: result.cache_stats.cache_hits,
            cache_misses: result.cache_stats.cache_miss,
            cache_shortfall: result.cache_stats.cache_shortfall,
            cache_evictions: result.cache_stats.eviction_count,
            cache_admission_rejects: result.cache_stats.admission_rejects,
            cache_ghost_entries: result.cache_stats.ghost_entries,
            index_rebuilding,
            pending_sessions,
        },
    );
    let explanation_cards_json =
        serde_json::to_value(&explanation_cards).unwrap_or_else(|_| serde_json::json!([]));

    // Serialize aggregations if present
    let agg_json = if aggregations.is_empty() {
        None
    } else {
        Some(serde_json::to_value(aggregations).unwrap_or_default())
    };

    match format {
        RobotFormat::Json => {
            let mut payload = serde_json::json!({
                "query": query,
                "limit": limit,
                "offset": offset,
                "count": filtered_hits.len(),
                "total_matches": total_matches,
                "hits": filtered_hits,
                "max_tokens": max_tokens,
                "request_id": request_id,
                "cursor": input_cursor,
                "hits_clamped": hits_clamped,
                "budget": budget,
            });

            // GH#414: report how the --sessions-from filter resolved, so a
            // filter selecting zero indexed sessions is distinguishable from
            // a genuine miss. `matched: null` means undetermined, not zero.
            if let (Some(stats), serde_json::Value::Object(map)) = (sessions_filter, &mut payload) {
                map.insert(
                    "sessions_filter".to_string(),
                    serde_json::json!({
                        "requested": stats.requested,
                        "matched": stats.matched,
                    }),
                );
                if stats.provably_unmatched() {
                    map.insert(
                        "sessions_filter_warning".to_string(),
                        serde_json::Value::String(
                            "no indexed session matches the paths supplied via --sessions-from; \
                             the empty result reflects the filter, not the query. Index the \
                             sessions first (cass index) or check the paths."
                                .to_string(),
                        ),
                    );
                }
            }

            // uojcg.7.1: why an empty, workspace-filtered search is empty.
            if let (Some(report), serde_json::Value::Object(map)) =
                (zero_result_diagnosis, &mut payload)
            {
                map.insert(
                    "zero_result_diagnosis".to_string(),
                    serde_json::to_value(report).unwrap_or_default(),
                );
            }

            // Add suggestions if present. When the --sessions-from filter
            // provably matched zero indexed sessions, the query is
            // demonstrably not the cause of an empty result, so the
            // broaden-the-query suggestion would mislead (GH#414).
            if !result.suggestions.is_empty()
                && !sessions_filter.is_some_and(SessionsFilterStats::provably_unmatched)
                && let serde_json::Value::Object(ref mut map) = payload
            {
                map.insert(
                    "suggestions".to_string(),
                    serde_json::to_value(&result.suggestions).unwrap_or_default(),
                );
            }

            // Add aggregations if present
            if let (Some(agg), serde_json::Value::Object(map)) = (&agg_json, &mut payload) {
                map.insert("aggregations".to_string(), agg.clone());
            }

            // Add query explanation if requested
            if let (Some(exp), serde_json::Value::Object(map)) = (explanation, &mut payload) {
                map.insert(
                    "explanation".to_string(),
                    serde_json::to_value(exp).unwrap_or_default(),
                );
            }

            // Add extended metadata if requested
            if include_meta && let serde_json::Value::Object(ref mut map) = payload {
                let mut meta = serde_json::json!({
                    "elapsed_ms": elapsed_ms,
                    "search_mode": search_mode_meta.realized,
                    "requested_search_mode": search_mode_meta.requested,
                    "mode_defaulted": search_mode_meta.defaulted,
                    "fallback_tier": search_mode_meta.fallback_tier,
                    "fallback_reason": search_mode_meta.fallback_reason.clone(),
                    "semantic_refinement": search_mode_meta.semantic_refinement(),
                    "refinement_level": search_mode_meta.realized_refinement(),
                    "semantic_fallback_reason": search_mode_meta.semantic_fallback_reason(),
                    "lexical_degrade_reason": search_mode_meta.lexical_degrade_reason,
                    "wildcard_fallback": result.wildcard_fallback,
                    "wildcard_fallback_skipped": search_mode_meta.wildcard_fallback_skipped,
                    "cache_stats": {
                        "hits": result.cache_stats.cache_hits,
                        "misses": result.cache_stats.cache_miss,
                        "shortfall": result.cache_stats.cache_shortfall,
                        "outcomes": result.cache_stats.searcher_cache,
                        "prewarm_scheduled": result.cache_stats.prewarm_scheduled,
                        "prewarm_skipped_pressure": result.cache_stats.prewarm_skipped_pressure,
                    },
                    // Search pipeline timing breakdown (T7.4)
                    "timing": {
                        "search_ms": search_ms,
                        "rerank_ms": rerank_ms,
                        "other_ms": elapsed_ms.saturating_sub(search_ms).saturating_sub(rerank_ms),
                    },
                    "tokens_estimated": tokens_estimated,
                    "max_tokens": max_tokens,
                    "request_id": request_id,
                    "next_cursor": next_cursor,
                    "hits_clamped": hits_clamped,
                    "query_plan": query_plan_json.clone(),
                    "cursor_manifest": cursor_manifest_json.clone(),
                    "explanation_cards": explanation_cards_json.clone(),
                    "effective": effective.clone(),
                });
                if let Some(state) = state_meta
                    && let serde_json::Value::Object(ref mut m) = meta
                {
                    m.insert("state".to_string(), state);
                }
                if let Some(freshness) = index_freshness
                    && let serde_json::Value::Object(ref mut m) = meta
                {
                    m.insert("index_freshness".to_string(), freshness);
                }
                // uojcg.3.3: surface the quarantine search-completeness verdict in
                // --robot-meta; only present when conversations are excluded.
                if let Some(sc) = search_completeness.as_ref()
                    && let serde_json::Value::Object(ref mut m) = meta
                {
                    m.insert("search_completeness".to_string(), sc.clone());
                }
                // qfswx: surface the storage-integrity verdict in --robot-meta so
                // search agrees with doctor/status on the canonical StorageState.
                if let Some(si) = storage_integrity.as_ref()
                    && let serde_json::Value::Object(ref mut m) = meta
                {
                    m.insert("storage_integrity".to_string(), si.clone());
                }
                // Add timeout info to _meta if timeout was configured
                if let Some(timeout) = timeout_ms
                    && let serde_json::Value::Object(ref mut m) = meta
                {
                    m.insert("timeout_ms".to_string(), serde_json::json!(timeout));
                    m.insert("timed_out".to_string(), serde_json::json!(timed_out));
                    if timed_out {
                        m.insert("partial_results".to_string(), serde_json::json!(true));
                    }
                }
                // Add ANN stats to _meta if approximate search was used
                if let Some(ref ann_stats) = result.ann_stats
                    && let serde_json::Value::Object(ref mut m) = meta
                {
                    m.insert(
                        "ann_stats".to_string(),
                        serde_json::to_value(ann_stats).unwrap_or_default(),
                    );
                }
                if let Some(reason) = result.ann_unavailable_reason
                    && let serde_json::Value::Object(ref mut m) = meta
                {
                    m.insert(
                        "ann_unavailable_reason".to_string(),
                        serde_json::json!(reason.code()),
                    );
                }
                map.insert("_meta".to_string(), meta);

                if let Some(warn) = &warning {
                    map.insert(
                        "_warning".to_string(),
                        serde_json::Value::String(warn.clone()),
                    );
                }
                // Add top-level timeout indicator if timed out
                if timed_out {
                    map.insert(
                        "_timeout".to_string(),
                        serde_json::json!({
                            "code": 10,
                            "kind": "timeout",
                            "message": format!("Operation exceeded timeout of {}ms", timeout_ms.unwrap_or(0)),
                            "retryable": true,
                            "partial_results": true
                        }),
                    );
                }
            }

            let stdout = std::io::stdout();
            let mut out = BufWriter::new(stdout.lock());
            serde_json::to_writer_pretty(&mut out, &payload).map_err(|e| CliError {
                code: 9,
                kind: CliErrorKind::EncodeJson.kind_str(),
                message: format!("failed to encode json: {e}"),
                hint: None,
                retryable: false,
            })?;
            writeln!(&mut out).map_err(|e| CliError {
                code: 9,
                kind: CliErrorKind::EncodeJson.kind_str(),
                message: format!("failed to write trailing newline: {e}"),
                hint: None,
                retryable: false,
            })?;
            out.flush().map_err(|e| CliError {
                code: 9,
                kind: CliErrorKind::EncodeJson.kind_str(),
                message: format!("failed to flush buffered json output: {e}"),
                hint: None,
                retryable: false,
            })?;
        }
        RobotFormat::Jsonl => {
            let stdout = std::io::stdout();
            let mut out = BufWriter::new(stdout.lock());

            // JSONL: one hit per line, preceded by a {budget, _meta} header
            // only when something asked for it or the budget ran out. Every
            // search has a budget, so gating on `budget_ms > 0` printed the
            // header always, and a line-per-hit consumer read it as a hit
            // (README: "jsonl # hits only"; 2l1b0.58).
            if include_meta
                || agg_json.is_some()
                || !result.suggestions.is_empty()
                || explanation.is_some()
                || budget.timed_out
            {
                let mut meta = serde_json::json!({
                    "budget": budget,
                    "_meta": {
                        "query": query,
                        "limit": limit,
                        "offset": offset,
                        "count": filtered_hits.len(),
                        "total_matches": total_matches,
                        "elapsed_ms": elapsed_ms,
                        "search_mode": search_mode_meta.realized,
                        "requested_search_mode": search_mode_meta.requested,
                        "mode_defaulted": search_mode_meta.defaulted,
                        "fallback_tier": search_mode_meta.fallback_tier,
                        "fallback_reason": search_mode_meta.fallback_reason.clone(),
                        "semantic_refinement": search_mode_meta.semantic_refinement(),
                        "refinement_level": search_mode_meta.realized_refinement(),
                        "semantic_fallback_reason": search_mode_meta.semantic_fallback_reason(),
                    "lexical_degrade_reason": search_mode_meta.lexical_degrade_reason,
                        "wildcard_fallback": result.wildcard_fallback,
                        "wildcard_fallback_skipped": search_mode_meta.wildcard_fallback_skipped,
                        "cache_stats": {
                            "hits": result.cache_stats.cache_hits,
                            "misses": result.cache_stats.cache_miss,
                            "shortfall": result.cache_stats.cache_shortfall,
                            "outcomes": result.cache_stats.searcher_cache,
                            "prewarm_scheduled": result.cache_stats.prewarm_scheduled,
                            "prewarm_skipped_pressure": result.cache_stats.prewarm_skipped_pressure,
                        },
                        "tokens_estimated": tokens_estimated,
                        "max_tokens": max_tokens,
                        "request_id": request_id,
                        "next_cursor": next_cursor,
                        "hits_clamped": hits_clamped,
                        "query_plan": query_plan_json.clone(),
                        "cursor_manifest": cursor_manifest_json.clone(),
                        "explanation_cards": explanation_cards_json.clone(),
                        "effective": effective.clone(),
                    }
                });
                if let Some(state) = state_meta
                    && let serde_json::Value::Object(ref mut outer) = meta
                    && let Some(serde_json::Value::Object(m)) = outer.get_mut("_meta")
                {
                    m.insert("state".to_string(), state);
                }
                if let Some(freshness) = index_freshness
                    && let serde_json::Value::Object(ref mut outer) = meta
                    && let Some(serde_json::Value::Object(m)) = outer.get_mut("_meta")
                {
                    m.insert("index_freshness".to_string(), freshness);
                }
                // uojcg.3.3: quarantine search-completeness verdict in the jsonl
                // _meta header; only present when conversations are excluded.
                if let Some(sc) = search_completeness.as_ref()
                    && let serde_json::Value::Object(ref mut outer) = meta
                    && let Some(serde_json::Value::Object(m)) = outer.get_mut("_meta")
                {
                    m.insert("search_completeness".to_string(), sc.clone());
                }
                // qfswx: surface the storage-integrity verdict in --robot-meta so
                // search agrees with doctor/status on the canonical StorageState.
                if let Some(si) = storage_integrity.as_ref()
                    && let serde_json::Value::Object(ref mut m) = meta
                {
                    m.insert("storage_integrity".to_string(), si.clone());
                }
                // Add suggestions to meta line
                if !result.suggestions.is_empty()
                    && let serde_json::Value::Object(ref mut map) = meta
                {
                    map.insert(
                        "suggestions".to_string(),
                        serde_json::to_value(&result.suggestions).unwrap_or_default(),
                    );
                }
                // Add aggregations to meta line
                if let (Some(agg), serde_json::Value::Object(map)) = (&agg_json, &mut meta) {
                    map.insert("aggregations".to_string(), agg.clone());
                }
                // Add explanation to meta line
                if let (Some(exp), serde_json::Value::Object(map)) = (explanation, &mut meta) {
                    map.insert(
                        "explanation".to_string(),
                        serde_json::to_value(exp).unwrap_or_default(),
                    );
                }
                if let Some(warn) = &warning
                    && let Some(m) = meta.get_mut("_meta").and_then(|v| v.as_object_mut())
                {
                    m.insert(
                        "_warning".to_string(),
                        serde_json::Value::String(warn.clone()),
                    );
                }
                // Add timeout info to JSONL _meta
                if let Some(m) = meta.get_mut("_meta").and_then(|v| v.as_object_mut())
                    && let Some(timeout) = timeout_ms
                {
                    m.insert("timeout_ms".to_string(), serde_json::json!(timeout));
                    m.insert("timed_out".to_string(), serde_json::json!(timed_out));
                    if timed_out {
                        m.insert("partial_results".to_string(), serde_json::json!(true));
                    }
                }
                // Add top-level timeout indicator if timed out
                if timed_out && let serde_json::Value::Object(ref mut map) = meta {
                    map.insert(
                        "_timeout".to_string(),
                        serde_json::json!({
                            "code": 10,
                            "kind": "timeout",
                            "message": format!("Operation exceeded timeout of {}ms", timeout_ms.unwrap_or(0)),
                            "retryable": true,
                            "partial_results": true
                        }),
                    );
                }
                serde_json::to_writer(&mut out, &meta).map_err(|e| CliError {
                    code: 9,
                    kind: CliErrorKind::EncodeJson.kind_str(),
                    message: format!("failed to encode jsonl metadata: {e}"),
                    hint: None,
                    retryable: false,
                })?;
                writeln!(&mut out).map_err(|e| CliError {
                    code: 9,
                    kind: CliErrorKind::EncodeJson.kind_str(),
                    message: format!("failed to write jsonl metadata newline: {e}"),
                    hint: None,
                    retryable: false,
                })?;
            }
            // One hit per line (with field filtering applied)
            for hit in &filtered_hits {
                serde_json::to_writer(&mut out, hit).map_err(|e| CliError {
                    code: 9,
                    kind: CliErrorKind::EncodeJson.kind_str(),
                    message: format!("failed to encode jsonl hit: {e}"),
                    hint: None,
                    retryable: false,
                })?;
                writeln!(&mut out).map_err(|e| CliError {
                    code: 9,
                    kind: CliErrorKind::EncodeJson.kind_str(),
                    message: format!("failed to write jsonl newline: {e}"),
                    hint: None,
                    retryable: false,
                })?;
            }
            out.flush().map_err(|e| CliError {
                code: 9,
                kind: CliErrorKind::EncodeJson.kind_str(),
                message: format!("failed to flush buffered jsonl output: {e}"),
                hint: None,
                retryable: false,
            })?;
        }
        RobotFormat::Compact => {
            // Single-line compact JSON
            let mut payload = serde_json::json!({
                "query": query,
                "limit": limit,
                "offset": offset,
                "count": filtered_hits.len(),
                "total_matches": total_matches,
                "hits": filtered_hits,
                "max_tokens": max_tokens,
                "request_id": request_id,
                "cursor": input_cursor,
                "hits_clamped": hits_clamped,
                "budget": budget,
            });

            // GH#414: report how the --sessions-from filter resolved, so a
            // filter selecting zero indexed sessions is distinguishable from
            // a genuine miss. `matched: null` means undetermined, not zero.
            if let (Some(stats), serde_json::Value::Object(map)) = (sessions_filter, &mut payload) {
                map.insert(
                    "sessions_filter".to_string(),
                    serde_json::json!({
                        "requested": stats.requested,
                        "matched": stats.matched,
                    }),
                );
                if stats.provably_unmatched() {
                    map.insert(
                        "sessions_filter_warning".to_string(),
                        serde_json::Value::String(
                            "no indexed session matches the paths supplied via --sessions-from; \
                             the empty result reflects the filter, not the query. Index the \
                             sessions first (cass index) or check the paths."
                                .to_string(),
                        ),
                    );
                }
            }

            // uojcg.7.1: why an empty, workspace-filtered search is empty.
            if let (Some(report), serde_json::Value::Object(map)) =
                (zero_result_diagnosis, &mut payload)
            {
                map.insert(
                    "zero_result_diagnosis".to_string(),
                    serde_json::to_value(report).unwrap_or_default(),
                );
            }

            // Add suggestions if present. When the --sessions-from filter
            // provably matched zero indexed sessions, the query is
            // demonstrably not the cause of an empty result, so the
            // broaden-the-query suggestion would mislead (GH#414).
            if !result.suggestions.is_empty()
                && !sessions_filter.is_some_and(SessionsFilterStats::provably_unmatched)
                && let serde_json::Value::Object(ref mut map) = payload
            {
                map.insert(
                    "suggestions".to_string(),
                    serde_json::to_value(&result.suggestions).unwrap_or_default(),
                );
            }

            // Add aggregations if present
            if let (Some(agg), serde_json::Value::Object(map)) = (&agg_json, &mut payload) {
                map.insert("aggregations".to_string(), agg.clone());
            }

            // Add query explanation if requested
            if let (Some(exp), serde_json::Value::Object(map)) = (explanation, &mut payload) {
                map.insert(
                    "explanation".to_string(),
                    serde_json::to_value(exp).unwrap_or_default(),
                );
            }

            if include_meta && let serde_json::Value::Object(ref mut map) = payload {
                let mut meta = serde_json::json!({
                    "elapsed_ms": elapsed_ms,
                    "search_mode": search_mode_meta.realized,
                    "requested_search_mode": search_mode_meta.requested,
                    "mode_defaulted": search_mode_meta.defaulted,
                    "fallback_tier": search_mode_meta.fallback_tier,
                    "fallback_reason": search_mode_meta.fallback_reason.clone(),
                    "semantic_refinement": search_mode_meta.semantic_refinement(),
                    "refinement_level": search_mode_meta.realized_refinement(),
                    "semantic_fallback_reason": search_mode_meta.semantic_fallback_reason(),
                    "lexical_degrade_reason": search_mode_meta.lexical_degrade_reason,
                    "wildcard_fallback": result.wildcard_fallback,
                    "wildcard_fallback_skipped": search_mode_meta.wildcard_fallback_skipped,
                    "tokens_estimated": tokens_estimated,
                    "max_tokens": max_tokens,
                    "request_id": request_id,
                    "next_cursor": next_cursor,
                    "hits_clamped": hits_clamped,
                    "query_plan": query_plan_json.clone(),
                    "cursor_manifest": cursor_manifest_json.clone(),
                    "explanation_cards": explanation_cards_json.clone(),
                    "effective": effective.clone(),
                });
                if let Some(state) = state_meta
                    && let serde_json::Value::Object(ref mut m) = meta
                {
                    m.insert("state".to_string(), state);
                }
                if let Some(freshness) = index_freshness
                    && let serde_json::Value::Object(ref mut m) = meta
                {
                    m.insert("index_freshness".to_string(), freshness);
                }
                // uojcg.3.3: surface the quarantine search-completeness verdict in
                // --robot-meta; only present when conversations are excluded.
                if let Some(sc) = search_completeness.as_ref()
                    && let serde_json::Value::Object(ref mut m) = meta
                {
                    m.insert("search_completeness".to_string(), sc.clone());
                }
                // qfswx: surface the storage-integrity verdict in --robot-meta so
                // search agrees with doctor/status on the canonical StorageState.
                if let Some(si) = storage_integrity.as_ref()
                    && let serde_json::Value::Object(ref mut m) = meta
                {
                    m.insert("storage_integrity".to_string(), si.clone());
                }
                // Add timeout info to _meta if timeout was configured
                if let Some(timeout) = timeout_ms
                    && let serde_json::Value::Object(ref mut m) = meta
                {
                    m.insert("timeout_ms".to_string(), serde_json::json!(timeout));
                    m.insert("timed_out".to_string(), serde_json::json!(timed_out));
                    if timed_out {
                        m.insert("partial_results".to_string(), serde_json::json!(true));
                    }
                }
                // Add ANN stats to _meta if approximate search was used
                if let Some(ref ann_stats) = result.ann_stats
                    && let serde_json::Value::Object(ref mut m) = meta
                {
                    m.insert(
                        "ann_stats".to_string(),
                        serde_json::to_value(ann_stats).unwrap_or_default(),
                    );
                }
                if let Some(reason) = result.ann_unavailable_reason
                    && let serde_json::Value::Object(ref mut m) = meta
                {
                    m.insert(
                        "ann_unavailable_reason".to_string(),
                        serde_json::json!(reason.code()),
                    );
                }
                map.insert("_meta".to_string(), meta);
                if let Some(warn) = &warning {
                    map.insert(
                        "_warning".to_string(),
                        serde_json::Value::String(warn.clone()),
                    );
                }
                // Add top-level timeout indicator if timed out
                if timed_out {
                    map.insert(
                        "_timeout".to_string(),
                        serde_json::json!({
                            "code": 10,
                            "kind": "timeout",
                            "message": format!("Operation exceeded timeout of {}ms", timeout_ms.unwrap_or(0)),
                            "retryable": true,
                            "partial_results": true
                        }),
                    );
                }
            }

            let out = serde_json::to_string(&payload).map_err(|e| CliError {
                code: 9,
                kind: CliErrorKind::EncodeJson.kind_str(),
                message: format!("failed to encode json: {e}"),
                hint: None,
                retryable: false,
            })?;
            println!("{out}");
        }
        RobotFormat::Toon => {
            // TOON: Token-Optimized Object Notation
            // Encodes via toon crate for token-efficient output
            let mut payload = serde_json::json!({
                "query": query,
                "limit": limit,
                "offset": offset,
                "count": filtered_hits.len(),
                "total_matches": total_matches,
                "hits": filtered_hits,
                "max_tokens": max_tokens,
                "request_id": request_id,
                "cursor": input_cursor,
                "hits_clamped": hits_clamped,
                "budget": budget,
            });

            // GH#414: report how the --sessions-from filter resolved, so a
            // filter selecting zero indexed sessions is distinguishable from
            // a genuine miss. `matched: null` means undetermined, not zero.
            if let (Some(stats), serde_json::Value::Object(map)) = (sessions_filter, &mut payload) {
                map.insert(
                    "sessions_filter".to_string(),
                    serde_json::json!({
                        "requested": stats.requested,
                        "matched": stats.matched,
                    }),
                );
                if stats.provably_unmatched() {
                    map.insert(
                        "sessions_filter_warning".to_string(),
                        serde_json::Value::String(
                            "no indexed session matches the paths supplied via --sessions-from; \
                             the empty result reflects the filter, not the query. Index the \
                             sessions first (cass index) or check the paths."
                                .to_string(),
                        ),
                    );
                }
            }

            // uojcg.7.1: why an empty, workspace-filtered search is empty.
            if let (Some(report), serde_json::Value::Object(map)) =
                (zero_result_diagnosis, &mut payload)
            {
                map.insert(
                    "zero_result_diagnosis".to_string(),
                    serde_json::to_value(report).unwrap_or_default(),
                );
            }

            // Add suggestions if present. When the --sessions-from filter
            // provably matched zero indexed sessions, the query is
            // demonstrably not the cause of an empty result, so the
            // broaden-the-query suggestion would mislead (GH#414).
            if !result.suggestions.is_empty()
                && !sessions_filter.is_some_and(SessionsFilterStats::provably_unmatched)
                && let serde_json::Value::Object(ref mut map) = payload
            {
                map.insert(
                    "suggestions".to_string(),
                    serde_json::to_value(&result.suggestions).unwrap_or_default(),
                );
            }

            // Add aggregations if present
            if let (Some(agg), serde_json::Value::Object(map)) = (&agg_json, &mut payload) {
                map.insert("aggregations".to_string(), agg.clone());
            }

            // Add query explanation if requested
            if let (Some(exp), serde_json::Value::Object(map)) = (explanation, &mut payload) {
                map.insert(
                    "explanation".to_string(),
                    serde_json::to_value(exp).unwrap_or_default(),
                );
            }

            if include_meta && let serde_json::Value::Object(ref mut map) = payload {
                let mut meta = serde_json::json!({
                    "elapsed_ms": elapsed_ms,
                    "search_mode": search_mode_meta.realized,
                    "requested_search_mode": search_mode_meta.requested,
                    "mode_defaulted": search_mode_meta.defaulted,
                    "fallback_tier": search_mode_meta.fallback_tier,
                    "fallback_reason": search_mode_meta.fallback_reason.clone(),
                    "semantic_refinement": search_mode_meta.semantic_refinement(),
                    "refinement_level": search_mode_meta.realized_refinement(),
                    "semantic_fallback_reason": search_mode_meta.semantic_fallback_reason(),
                    "lexical_degrade_reason": search_mode_meta.lexical_degrade_reason,
                    "wildcard_fallback": result.wildcard_fallback,
                    "wildcard_fallback_skipped": search_mode_meta.wildcard_fallback_skipped,
                    "tokens_estimated": tokens_estimated,
                    "max_tokens": max_tokens,
                    "request_id": request_id,
                    "next_cursor": next_cursor,
                    "hits_clamped": hits_clamped,
                    "query_plan": query_plan_json.clone(),
                    "cursor_manifest": cursor_manifest_json.clone(),
                    "explanation_cards": explanation_cards_json.clone(),
                    "effective": effective.clone(),
                });
                if let Some(state) = state_meta
                    && let serde_json::Value::Object(ref mut m) = meta
                {
                    m.insert("state".to_string(), state);
                }
                if let Some(freshness) = index_freshness
                    && let serde_json::Value::Object(ref mut m) = meta
                {
                    m.insert("index_freshness".to_string(), freshness);
                }
                // uojcg.3.3: surface the quarantine search-completeness verdict in
                // --robot-meta; only present when conversations are excluded.
                if let Some(sc) = search_completeness.as_ref()
                    && let serde_json::Value::Object(ref mut m) = meta
                {
                    m.insert("search_completeness".to_string(), sc.clone());
                }
                // qfswx: surface the storage-integrity verdict in --robot-meta so
                // search agrees with doctor/status on the canonical StorageState.
                if let Some(si) = storage_integrity.as_ref()
                    && let serde_json::Value::Object(ref mut m) = meta
                {
                    m.insert("storage_integrity".to_string(), si.clone());
                }
                if let Some(timeout) = timeout_ms
                    && let serde_json::Value::Object(ref mut m) = meta
                {
                    m.insert("timeout_ms".to_string(), serde_json::json!(timeout));
                    m.insert("timed_out".to_string(), serde_json::json!(timed_out));
                    if timed_out {
                        m.insert("partial_results".to_string(), serde_json::json!(true));
                    }
                }
                // Add ANN stats to _meta if approximate search was used
                if let Some(ref ann_stats) = result.ann_stats
                    && let serde_json::Value::Object(ref mut m) = meta
                {
                    m.insert(
                        "ann_stats".to_string(),
                        serde_json::to_value(ann_stats).unwrap_or_default(),
                    );
                }
                if let Some(reason) = result.ann_unavailable_reason
                    && let serde_json::Value::Object(ref mut m) = meta
                {
                    m.insert(
                        "ann_unavailable_reason".to_string(),
                        serde_json::json!(reason.code()),
                    );
                }
                map.insert("_meta".to_string(), meta);
                if let Some(warn) = &warning {
                    map.insert(
                        "_warning".to_string(),
                        serde_json::Value::String(warn.clone()),
                    );
                }
                if timed_out {
                    map.insert(
                        "_timeout".to_string(),
                        serde_json::json!({
                            "code": 10,
                            "kind": "timeout",
                            "message": format!("Operation exceeded timeout of {}ms", timeout_ms.unwrap_or(0)),
                            "retryable": true,
                            "partial_results": true
                        }),
                    );
                }
            }

            let json_str = serde_json::to_string(&payload).map_err(|e| CliError {
                code: 9,
                kind: CliErrorKind::EncodeJson.kind_str(),
                message: format!("failed to encode json: {e}"),
                hint: None,
                retryable: false,
            })?;

            let toon_str = toon::encode(payload, Some(toon_encode_options_from_env()));

            // Preserve the existing "compact JSON" behavior by first ensuring the payload is
            // valid JSON (serde_json::to_string above). We don't need the string itself here.
            drop(json_str);
            print!("{toon_str}");
        }
        RobotFormat::Sessions => {
            unreachable!("RobotFormat::Sessions is handled above to avoid building hit payloads");
        }
    }

    Ok(())
}
