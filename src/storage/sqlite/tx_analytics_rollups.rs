use super::aggregators_and_pricing::{
    AnalyticsRollupAggregator, MessageMetricsEntry, StatsDelta, TokenStatsDelta, TokenUsageEntry,
    UsageRollupDelta,
};
use super::orphan_fk_cleanup::sql_placeholders;
use super::reconcile_and_codebuff::{
    provider_workspace_is_authoritative, shelley_metadata_is_authoritative,
};
use super::storage_core::{EnsuredDailyStatsKey, FrankenStorage};
use crate::franken_sync::{
    Connection as FrankenConnection,
    compat::{
        ConnectionExt as FrankenConnectionExt, OptionalExtension as FrankenOptionalExtension,
        ParamValue, RowExt as FrankenRowExt, Transaction as FrankenTransaction,
        param_slice_to_values,
    },
};
use crate::model::types::Conversation;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Update daily stats within a SQLite transaction.
pub(super) fn franken_update_daily_stats_in_tx(
    storage: &FrankenStorage,
    tx: &FrankenTransaction<'_>,
    agent_slug: &str,
    source_id: &str,
    started_at: Option<i64>,
    delta: StatsDelta,
) -> Result<()> {
    let day_id = started_at
        .map(FrankenStorage::day_id_from_millis)
        .unwrap_or(0);
    let now = FrankenStorage::now_millis();

    let targets = [
        DailyStatsTarget {
            day_id,
            agent_slug,
            source_id,
        },
        DailyStatsTarget {
            day_id,
            agent_slug: "all",
            source_id,
        },
        DailyStatsTarget {
            day_id,
            agent_slug,
            source_id: "all",
        },
        DailyStatsTarget {
            day_id,
            agent_slug: "all",
            source_id: "all",
        },
    ];

    if agent_slug != "all"
        && source_id != "all"
        && franken_update_ensured_daily_stats_targets_in_tx(storage, tx, &targets, now, delta)?
    {
        return Ok(());
    }

    for target in targets {
        franken_apply_daily_stats_delta_in_tx(storage, tx, target, now, delta)?;
    }

    Ok(())
}

#[derive(Clone, Copy)]
struct DailyStatsTarget<'a> {
    day_id: i64,
    agent_slug: &'a str,
    source_id: &'a str,
}

fn franken_update_ensured_daily_stats_targets_in_tx(
    storage: &FrankenStorage,
    tx: &FrankenTransaction<'_>,
    targets: &[DailyStatsTarget<'_>; 4],
    now: i64,
    delta: StatsDelta,
) -> Result<bool> {
    let cache_keys = targets.map(|target| {
        EnsuredDailyStatsKey::new(target.day_id, target.agent_slug, target.source_id)
    });
    if !storage.daily_stats_keys_already_ensured(&cache_keys) {
        return Ok(false);
    }

    let primary = targets[0];
    let rows_changed = tx.execute_compat(
        "UPDATE daily_stats
         SET session_count = session_count + ?4,
             message_count = message_count + ?5,
             total_chars = total_chars + ?6,
             last_updated = ?7
         WHERE day_id = ?1
           AND ((agent_slug = ?2 AND source_id = ?3)
                OR (agent_slug = 'all' AND source_id = ?3)
                OR (agent_slug = ?2 AND source_id = 'all')
                OR (agent_slug = 'all' AND source_id = 'all'))",
        fparams![
            primary.day_id,
            primary.agent_slug,
            primary.source_id,
            delta.session_count_delta,
            delta.message_count_delta,
            delta.total_chars_delta,
            now
        ],
    )?;
    if rows_changed == targets.len() {
        return Ok(true);
    }

    for (target, cache_key) in targets.iter().copied().zip(cache_keys) {
        let exists = tx
            .query_row_map(
                "SELECT 1 FROM daily_stats
                 WHERE day_id = ?1 AND agent_slug = ?2 AND source_id = ?3
                 LIMIT 1",
                fparams![target.day_id, target.agent_slug, target.source_id],
                |row| row.get_typed::<i64>(0),
            )
            .optional()?
            .is_some();
        if exists {
            continue;
        }

        tx.execute_compat(
            "INSERT INTO daily_stats(day_id, agent_slug, source_id, session_count, message_count, total_chars, last_updated)
             VALUES(?1,?2,?3,?4,?5,?6,?7)",
            fparams![
                target.day_id,
                target.agent_slug,
                target.source_id,
                delta.session_count_delta,
                delta.message_count_delta,
                delta.total_chars_delta,
                now
            ],
        )?;
        storage.mark_daily_stats_key_ensured(cache_key);
    }

    Ok(true)
}

fn franken_apply_daily_stats_delta_in_tx(
    storage: &FrankenStorage,
    tx: &FrankenTransaction<'_>,
    target: DailyStatsTarget<'_>,
    now: i64,
    delta: StatsDelta,
) -> Result<()> {
    let cache_key = EnsuredDailyStatsKey::new(target.day_id, target.agent_slug, target.source_id);
    if storage.daily_stats_key_already_ensured(&cache_key) {
        let rows_changed = tx.execute_compat(
            "UPDATE daily_stats
             SET session_count = session_count + ?4,
                 message_count = message_count + ?5,
                 total_chars = total_chars + ?6,
                 last_updated = ?7
             WHERE day_id = ?1 AND agent_slug = ?2 AND source_id = ?3",
            fparams![
                target.day_id,
                target.agent_slug,
                target.source_id,
                delta.session_count_delta,
                delta.message_count_delta,
                delta.total_chars_delta,
                now
            ],
        )?;
        if rows_changed > 0 {
            return Ok(());
        }
    }

    tx.execute_compat(
        "INSERT INTO daily_stats(day_id, agent_slug, source_id, session_count, message_count, total_chars, last_updated)
         VALUES(?1,?2,?3,?4,?5,?6,?7)
         ON CONFLICT(day_id, agent_slug, source_id) DO UPDATE SET
            session_count = session_count + excluded.session_count,
            message_count = message_count + excluded.message_count,
            total_chars = total_chars + excluded.total_chars,
            last_updated = excluded.last_updated",
        fparams![
            target.day_id,
            target.agent_slug,
            target.source_id,
            delta.session_count_delta,
            delta.message_count_delta,
            delta.total_chars_delta,
            now
        ],
    )?;
    storage.mark_daily_stats_key_ensured(cache_key);
    Ok(())
}

// -------------------------------------------------------------------------
// SQLite batch helpers
// -------------------------------------------------------------------------

/// Batch upsert daily_stats within a SQLite transaction.
pub(super) fn franken_update_daily_stats_batched_in_tx(
    tx: &FrankenTransaction<'_>,
    entries: &[(i64, String, String, StatsDelta)],
) -> Result<usize> {
    franken_update_daily_stats_batched_in_tx_for_target(
        tx,
        entries,
        DailyStatsBatchTarget::Canonical,
    )
}

#[derive(Debug, Clone, Copy)]
pub(super) enum DailyStatsBatchTarget {
    Canonical,
    RebuildStage,
}

impl DailyStatsBatchTarget {
    fn upsert_sql(self) -> &'static str {
        match self {
            Self::Canonical => {
                "INSERT INTO daily_stats (day_id, agent_slug, source_id, session_count, message_count, total_chars, last_updated)
                 VALUES(?1,?2,?3,?4,?5,?6,?7)
                 ON CONFLICT(day_id, agent_slug, source_id) DO UPDATE SET
                     session_count = session_count + excluded.session_count,
                     message_count = message_count + excluded.message_count,
                     total_chars = total_chars + excluded.total_chars,
                     last_updated = excluded.last_updated"
            }
            Self::RebuildStage => {
                "INSERT INTO daily_stats_rebuild_stage (day_id, agent_slug, source_id, session_count, message_count, total_chars, last_updated)
                 VALUES(?1,?2,?3,?4,?5,?6,?7)
                 ON CONFLICT(day_id, agent_slug, source_id) DO UPDATE SET
                     session_count = session_count + excluded.session_count,
                     message_count = message_count + excluded.message_count,
                     total_chars = total_chars + excluded.total_chars,
                     last_updated = excluded.last_updated"
            }
        }
    }
}

pub(super) fn franken_update_daily_stats_batched_in_tx_for_target(
    tx: &FrankenTransaction<'_>,
    entries: &[(i64, String, String, StatsDelta)],
    target: DailyStatsBatchTarget,
) -> Result<usize> {
    if entries.is_empty() {
        return Ok(0);
    }

    let now = FrankenStorage::now_millis();
    let mut total_affected = 0;
    let upsert_sql = target.upsert_sql();

    for (day_id, agent, source, delta) in entries {
        total_affected += tx.execute_compat(
            upsert_sql,
            fparams![
                *day_id,
                agent.as_str(),
                source.as_str(),
                delta.session_count_delta,
                delta.message_count_delta,
                delta.total_chars_delta,
                now
            ],
        )?;
    }

    Ok(total_affected)
}

/// Batch insert token_usage rows within a SQLite transaction.
pub(super) fn franken_insert_token_usage_batched_in_tx(
    tx: &FrankenTransaction<'_>,
    entries: &[TokenUsageEntry],
) -> Result<usize> {
    if entries.is_empty() {
        return Ok(0);
    }

    let mut total_inserted = 0;

    for e in entries {
        let params_vec: Vec<ParamValue> = vec![
            ParamValue::from(e.message_id),
            ParamValue::from(e.conversation_id),
            ParamValue::from(e.agent_id),
            ParamValue::from(e.workspace_id),
            ParamValue::from(e.source_id.clone()),
            ParamValue::from(e.timestamp_ms),
            ParamValue::from(e.day_id),
            ParamValue::from(e.model_name.clone()),
            ParamValue::from(e.model_family.clone()),
            ParamValue::from(e.model_tier.clone()),
            ParamValue::from(e.service_tier.clone()),
            ParamValue::from(e.provider.clone()),
            ParamValue::from(e.input_tokens),
            ParamValue::from(e.output_tokens),
            ParamValue::from(e.cache_read_tokens),
            ParamValue::from(e.cache_creation_tokens),
            ParamValue::from(e.thinking_tokens),
            ParamValue::from(e.total_tokens),
            ParamValue::from(e.estimated_cost_usd),
            ParamValue::from(e.role.clone()),
            ParamValue::from(e.content_chars),
            ParamValue::from(e.has_tool_calls as i64),
            ParamValue::from(e.tool_call_count as i64),
            ParamValue::from(e.data_source.clone()),
        ];

        let values = param_slice_to_values(&params_vec);
        total_inserted += tx.execute_with_params(
            "INSERT OR IGNORE INTO token_usage (
                message_id, conversation_id, agent_id, workspace_id, source_id,
                timestamp_ms, day_id,
                model_name, model_family, model_tier, service_tier, provider,
                input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens,
                thinking_tokens, total_tokens, estimated_cost_usd,
                role, content_chars, has_tool_calls, tool_call_count, data_source
            )
            VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24)",
            &values,
        )?;
    }

    Ok(total_inserted)
}

/// Batch upsert token_daily_stats within a SQLite transaction.
pub(super) fn franken_update_token_daily_stats_batched_in_tx(
    tx: &FrankenTransaction<'_>,
    entries: &[(i64, String, String, String, TokenStatsDelta)],
) -> Result<usize> {
    franken_update_token_daily_stats_batched_in_tx_for_table(tx, TOKEN_DAILY_STATS_TABLE, entries)
}

/// Live Track B rollup table.
pub(super) const TOKEN_DAILY_STATS_TABLE: &str = "token_daily_stats";
/// Persistent staging table for a resumable Track B rebuild (GH #386). Same
/// shape as [`TOKEN_DAILY_STATS_TABLE`]; the live table is only touched by the
/// final atomic swap.
pub(super) const TOKEN_DAILY_STATS_REBUILD_STAGE_TABLE: &str = "token_daily_stats_rebuild_stage";
/// `meta` key holding the persisted rebuild cursor (GH #386).
pub(super) const TOKEN_DAILY_STATS_REBUILD_CURSOR_KEY: &str = "token_daily_stats_rebuild_cursor";
/// Same-row ledger revisions must invalidate both resumed and active rebuilds.
pub(super) const TOKEN_USAGE_REVISION_GENERATION_META_KEY: &str = "token_usage_revision_generation";

/// Same upsert as the live-table writer, addressed to `table` — which must be
/// one of the two compile-time table-name constants above (never caller
/// input; it is spliced into SQL text).
pub(super) fn franken_update_token_daily_stats_batched_in_tx_for_table(
    tx: &FrankenTransaction<'_>,
    table: &'static str,
    entries: &[(i64, String, String, String, TokenStatsDelta)],
) -> Result<usize> {
    if entries.is_empty() {
        return Ok(0);
    }
    debug_assert!(
        table == TOKEN_DAILY_STATS_TABLE || table == TOKEN_DAILY_STATS_REBUILD_STAGE_TABLE,
        "token daily stats upsert target must be a known table constant"
    );

    let now = FrankenStorage::now_millis();
    let mut total_affected = 0;
    let sql = format!(
        "INSERT INTO {table} (
                day_id, agent_slug, source_id, model_family,
                api_call_count, user_message_count, assistant_message_count, tool_message_count,
                total_input_tokens, total_output_tokens, total_cache_read_tokens,
                total_cache_creation_tokens, total_thinking_tokens, grand_total_tokens,
                total_content_chars, total_tool_calls, estimated_cost_usd, session_count,
                last_updated
            )
            VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19)
            ON CONFLICT(day_id, agent_slug, source_id, model_family) DO UPDATE SET
                api_call_count = api_call_count + excluded.api_call_count,
                user_message_count = user_message_count + excluded.user_message_count,
                assistant_message_count = assistant_message_count + excluded.assistant_message_count,
                tool_message_count = tool_message_count + excluded.tool_message_count,
                total_input_tokens = total_input_tokens + excluded.total_input_tokens,
                total_output_tokens = total_output_tokens + excluded.total_output_tokens,
                total_cache_read_tokens = total_cache_read_tokens + excluded.total_cache_read_tokens,
                total_cache_creation_tokens = total_cache_creation_tokens + excluded.total_cache_creation_tokens,
                total_thinking_tokens = total_thinking_tokens + excluded.total_thinking_tokens,
                grand_total_tokens = grand_total_tokens + excluded.grand_total_tokens,
                total_content_chars = total_content_chars + excluded.total_content_chars,
                total_tool_calls = total_tool_calls + excluded.total_tool_calls,
                estimated_cost_usd = estimated_cost_usd + excluded.estimated_cost_usd,
                session_count = session_count + excluded.session_count,
                last_updated = excluded.last_updated"
    );

    for (day_id, agent, source, model, delta) in entries {
        total_affected += tx.execute_compat(
            &sql,
            fparams![
                *day_id,
                agent.as_str(),
                source.as_str(),
                model.as_str(),
                delta.api_call_count,
                delta.user_message_count,
                delta.assistant_message_count,
                delta.tool_message_count,
                delta.total_input_tokens,
                delta.total_output_tokens,
                delta.total_cache_read_tokens,
                delta.total_cache_creation_tokens,
                delta.total_thinking_tokens,
                delta.grand_total_tokens,
                delta.total_content_chars,
                delta.total_tool_calls,
                delta.estimated_cost_usd,
                delta.session_count,
                now
            ],
        )?;
    }

    Ok(total_affected)
}

/// Batch insert message_metrics rows within a SQLite transaction.
pub(super) fn franken_insert_message_metrics_batched_in_tx(
    tx: &FrankenTransaction<'_>,
    entries: &[MessageMetricsEntry],
) -> Result<usize> {
    if entries.is_empty() {
        return Ok(0);
    }

    let mut total_inserted = 0;

    for e in entries {
        let params_vec: Vec<ParamValue> = vec![
            ParamValue::from(e.message_id),
            ParamValue::from(e.created_at_ms),
            ParamValue::from(e.hour_id),
            ParamValue::from(e.day_id),
            ParamValue::from(e.agent_slug.clone()),
            ParamValue::from(e.workspace_id),
            ParamValue::from(e.source_id.clone()),
            ParamValue::from(e.role.clone()),
            ParamValue::from(e.content_chars),
            ParamValue::from(e.content_tokens_est),
            ParamValue::from(e.model_name.clone()),
            ParamValue::from(e.model_family.clone()),
            ParamValue::from(e.model_tier.clone()),
            ParamValue::from(e.provider.clone()),
            ParamValue::from(e.api_input_tokens),
            ParamValue::from(e.api_output_tokens),
            ParamValue::from(e.api_cache_read_tokens),
            ParamValue::from(e.api_cache_creation_tokens),
            ParamValue::from(e.api_thinking_tokens),
            ParamValue::from(e.api_service_tier.clone()),
            ParamValue::from(e.api_data_source.clone()),
            ParamValue::from(e.tool_call_count),
            ParamValue::from(e.has_tool_calls as i64),
            ParamValue::from(e.has_plan as i64),
        ];

        let values = param_slice_to_values(&params_vec);
        total_inserted += tx.execute_with_params(
            "INSERT OR IGNORE INTO message_metrics (
                message_id, created_at_ms, hour_id, day_id,
                agent_slug, workspace_id, source_id, role,
                content_chars, content_tokens_est,
                model_name, model_family, model_tier, provider,
                api_input_tokens, api_output_tokens, api_cache_read_tokens,
                api_cache_creation_tokens, api_thinking_tokens,
                api_service_tier, api_data_source,
                tool_call_count, has_tool_calls, has_plan
            )
            VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24)",
            &values,
        )?;
    }

    Ok(total_inserted)
}

/// Flush one rollup table (shared logic for hourly + daily) within a SQLite transaction.
fn franken_flush_rollup_table(
    tx: &FrankenTransaction<'_>,
    table: &str,
    bucket_col: &str,
    deltas: &HashMap<(i64, String, i64, String), UsageRollupDelta>,
    now: i64,
) -> Result<usize> {
    if deltas.is_empty() {
        return Ok(0);
    }

    let mut total_affected = 0;

    for ((bucket_id, agent, workspace_id, source), d) in deltas {
        let sql = format!(
            "INSERT INTO {table} (
                {bucket_col}, agent_slug, workspace_id, source_id,
                message_count, user_message_count, assistant_message_count,
                tool_call_count, plan_message_count, plan_content_tokens_est_total,
                plan_api_tokens_total, api_coverage_message_count,
                content_tokens_est_total, content_tokens_est_user, content_tokens_est_assistant,
                api_tokens_total, api_input_tokens_total, api_output_tokens_total,
                api_cache_read_tokens_total, api_cache_creation_tokens_total,
                api_thinking_tokens_total, last_updated
            )
            VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22)
            ON CONFLICT({bucket_col}, agent_slug, workspace_id, source_id) DO UPDATE SET
                message_count = message_count + excluded.message_count,
                user_message_count = user_message_count + excluded.user_message_count,
                assistant_message_count = assistant_message_count + excluded.assistant_message_count,
                tool_call_count = tool_call_count + excluded.tool_call_count,
                plan_message_count = plan_message_count + excluded.plan_message_count,
                plan_content_tokens_est_total = plan_content_tokens_est_total + excluded.plan_content_tokens_est_total,
                plan_api_tokens_total = plan_api_tokens_total + excluded.plan_api_tokens_total,
                api_coverage_message_count = api_coverage_message_count + excluded.api_coverage_message_count,
                content_tokens_est_total = content_tokens_est_total + excluded.content_tokens_est_total,
                content_tokens_est_user = content_tokens_est_user + excluded.content_tokens_est_user,
                content_tokens_est_assistant = content_tokens_est_assistant + excluded.content_tokens_est_assistant,
                api_tokens_total = api_tokens_total + excluded.api_tokens_total,
                api_input_tokens_total = api_input_tokens_total + excluded.api_input_tokens_total,
                api_output_tokens_total = api_output_tokens_total + excluded.api_output_tokens_total,
                api_cache_read_tokens_total = api_cache_read_tokens_total + excluded.api_cache_read_tokens_total,
                api_cache_creation_tokens_total = api_cache_creation_tokens_total + excluded.api_cache_creation_tokens_total,
                api_thinking_tokens_total = api_thinking_tokens_total + excluded.api_thinking_tokens_total,
                last_updated = excluded.last_updated"
        );

        total_affected += tx.execute_compat(
            &sql,
            fparams![
                *bucket_id,
                agent.as_str(),
                *workspace_id,
                source.as_str(),
                d.message_count,
                d.user_message_count,
                d.assistant_message_count,
                d.tool_call_count,
                d.plan_message_count,
                d.plan_content_tokens_est_total,
                d.plan_api_tokens_total,
                d.api_coverage_message_count,
                d.content_tokens_est_total,
                d.content_tokens_est_user,
                d.content_tokens_est_assistant,
                d.api_tokens_total,
                d.api_input_tokens_total,
                d.api_output_tokens_total,
                d.api_cache_read_tokens_total,
                d.api_cache_creation_tokens_total,
                d.api_thinking_tokens_total,
                now
            ],
        )?;
    }

    Ok(total_affected)
}

/// Flush usage_models_daily rollup within a SQLite transaction.
fn franken_flush_model_daily_rollup_table(
    tx: &FrankenTransaction<'_>,
    deltas: &HashMap<(i64, String, i64, String, String, String), UsageRollupDelta>,
    now: i64,
) -> Result<usize> {
    if deltas.is_empty() {
        return Ok(0);
    }

    let mut total_affected = 0;

    for ((day_id, agent, workspace_id, source, model_family, model_tier), d) in deltas {
        total_affected += tx.execute_compat(
            "INSERT INTO usage_models_daily (
                day_id, agent_slug, workspace_id, source_id, model_family, model_tier,
                message_count, user_message_count, assistant_message_count,
                tool_call_count, plan_message_count, api_coverage_message_count,
                content_tokens_est_total, content_tokens_est_user, content_tokens_est_assistant,
                api_tokens_total, api_input_tokens_total, api_output_tokens_total,
                api_cache_read_tokens_total, api_cache_creation_tokens_total,
                api_thinking_tokens_total, last_updated
            )
            VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22)
            ON CONFLICT(day_id, agent_slug, workspace_id, source_id, model_family, model_tier) DO UPDATE SET
                message_count = message_count + excluded.message_count,
                user_message_count = user_message_count + excluded.user_message_count,
                assistant_message_count = assistant_message_count + excluded.assistant_message_count,
                tool_call_count = tool_call_count + excluded.tool_call_count,
                plan_message_count = plan_message_count + excluded.plan_message_count,
                api_coverage_message_count = api_coverage_message_count + excluded.api_coverage_message_count,
                content_tokens_est_total = content_tokens_est_total + excluded.content_tokens_est_total,
                content_tokens_est_user = content_tokens_est_user + excluded.content_tokens_est_user,
                content_tokens_est_assistant = content_tokens_est_assistant + excluded.content_tokens_est_assistant,
                api_tokens_total = api_tokens_total + excluded.api_tokens_total,
                api_input_tokens_total = api_input_tokens_total + excluded.api_input_tokens_total,
                api_output_tokens_total = api_output_tokens_total + excluded.api_output_tokens_total,
                api_cache_read_tokens_total = api_cache_read_tokens_total + excluded.api_cache_read_tokens_total,
                api_cache_creation_tokens_total = api_cache_creation_tokens_total + excluded.api_cache_creation_tokens_total,
                api_thinking_tokens_total = api_thinking_tokens_total + excluded.api_thinking_tokens_total,
                last_updated = excluded.last_updated",
            fparams![
                *day_id,
                agent.as_str(),
                *workspace_id,
                source.as_str(),
                model_family.as_str(),
                model_tier.as_str(),
                d.message_count,
                d.user_message_count,
                d.assistant_message_count,
                d.tool_call_count,
                d.plan_message_count,
                d.api_coverage_message_count,
                d.content_tokens_est_total,
                d.content_tokens_est_user,
                d.content_tokens_est_assistant,
                d.api_tokens_total,
                d.api_input_tokens_total,
                d.api_output_tokens_total,
                d.api_cache_read_tokens_total,
                d.api_cache_creation_tokens_total,
                d.api_thinking_tokens_total,
                now
            ],
        )?;
    }

    Ok(total_affected)
}

/// Flush AnalyticsRollupAggregator deltas within a SQLite transaction.
pub(super) fn franken_flush_analytics_rollups_in_tx(
    tx: &FrankenTransaction<'_>,
    agg: &AnalyticsRollupAggregator,
) -> Result<(usize, usize, usize)> {
    let now = FrankenStorage::now_millis();

    let hourly_affected =
        franken_flush_rollup_table(tx, "usage_hourly", "hour_id", &agg.hourly, now)?;
    let daily_affected = franken_flush_rollup_table(tx, "usage_daily", "day_id", &agg.daily, now)?;
    let models_daily_affected = franken_flush_model_daily_rollup_table(tx, &agg.models_daily, now)?;

    Ok((hourly_affected, daily_affected, models_daily_affected))
}

/// Relocate existing analytics without re-estimating tokens or creating missing
/// metrics. This also repairs authoritative replays whose canonical workspace
/// was corrected earlier, while their analytics still use the former workspace.
pub(super) fn franken_reassociate_provider_analytics_workspace(
    tx: &FrankenTransaction<'_>,
    conversation_id: i64,
    conv: &Conversation,
) -> Result<()> {
    if conv.external_id.is_none()
        || !(shelley_metadata_is_authoritative(conv)
            || provider_workspace_is_authoritative(
                &conv.agent_slug,
                conv.workspace.as_deref(),
                &conv.metadata_json,
            ))
    {
        return Ok(());
    }
    let workspace_id: Option<i64> = tx.query_row_map(
        "SELECT workspace_id FROM conversations WHERE id = ?1",
        fparams![conversation_id],
        |row| row.get_typed(0),
    )?;
    let metrics_workspace = workspace_id.unwrap_or(0);
    let message_ids: Vec<i64> = tx.query_map_collect(
        "SELECT id FROM messages WHERE conversation_id = ?1",
        fparams![conversation_id],
        |row| row.get_typed(0),
    )?;
    let mut old = AnalyticsRollupAggregator::new();
    // Primary-key IN lists keep the read scoped to this conversation. Never
    // hydrate message text or walk the archive to change one dimension.
    for ids in message_ids.chunks(128) {
        let mut params: Vec<ParamValue> = ids.iter().copied().map(ParamValue::from).collect();
        params.push(ParamValue::from(metrics_workspace));
        let entries = tx.query_map_collect(
            &format!(
                "SELECT message_id, created_at_ms, hour_id, day_id,
                agent_slug, workspace_id, source_id, role, content_chars, content_tokens_est,
                model_name, model_family, model_tier, provider,
                api_input_tokens, api_output_tokens, api_cache_read_tokens,
                api_cache_creation_tokens, api_thinking_tokens, api_service_tier,
                api_data_source, tool_call_count, has_tool_calls, has_plan
                FROM message_metrics WHERE message_id IN ({}) AND workspace_id != ?{}",
                sql_placeholders(ids.len()),
                params.len()
            ),
            &params,
            |row| {
                Ok(MessageMetricsEntry {
                    message_id: row.get_typed(0)?,
                    created_at_ms: row.get_typed(1)?,
                    hour_id: row.get_typed(2)?,
                    day_id: row.get_typed(3)?,
                    agent_slug: row.get_typed(4)?,
                    workspace_id: row.get_typed(5)?,
                    source_id: row.get_typed(6)?,
                    role: row.get_typed(7)?,
                    content_chars: row.get_typed(8)?,
                    content_tokens_est: row.get_typed(9)?,
                    model_name: row.get_typed(10)?,
                    model_family: row.get_typed(11)?,
                    model_tier: row.get_typed(12)?,
                    provider: row.get_typed(13)?,
                    api_input_tokens: row.get_typed(14)?,
                    api_output_tokens: row.get_typed(15)?,
                    api_cache_read_tokens: row.get_typed(16)?,
                    api_cache_creation_tokens: row.get_typed(17)?,
                    api_thinking_tokens: row.get_typed(18)?,
                    api_service_tier: row.get_typed(19)?,
                    api_data_source: row.get_typed(20)?,
                    tool_call_count: row.get_typed(21)?,
                    has_tool_calls: row.get_typed::<i64>(22)? != 0,
                    has_plan: row.get_typed::<i64>(23)? != 0,
                })
            },
        )?;
        for entry in entries {
            old.record(&entry);
        }
    }

    for (table, bucket_col, deltas) in [
        ("usage_hourly", "hour_id", &old.hourly),
        ("usage_daily", "day_id", &old.daily),
    ] {
        for ((bucket, agent, previous_workspace, source), delta) in deltas {
            let key = vec![
                (bucket_col, ParamValue::from(*bucket)),
                ("agent_slug", ParamValue::from(agent.as_str())),
                ("workspace_id", ParamValue::from(*previous_workspace)),
                ("source_id", ParamValue::from(source.as_str())),
            ];
            let destination_updated =
                franken_relocate_rollup_subtract(tx, table, key, metrics_workspace, delta)?;
            franken_flush_rollup_table(
                tx,
                table,
                bucket_col,
                &HashMap::from([(
                    (*bucket, agent.clone(), metrics_workspace, source.clone()),
                    delta.clone(),
                )]),
                destination_updated,
            )?;
        }
    }
    for ((day, agent, previous_workspace, source, family, tier), delta) in &old.models_daily {
        let key = vec![
            ("day_id", ParamValue::from(*day)),
            ("agent_slug", ParamValue::from(agent.as_str())),
            ("workspace_id", ParamValue::from(*previous_workspace)),
            ("source_id", ParamValue::from(source.as_str())),
            ("model_family", ParamValue::from(family.as_str())),
            ("model_tier", ParamValue::from(tier.as_str())),
        ];
        let destination_updated = franken_relocate_rollup_subtract(
            tx,
            "usage_models_daily",
            key,
            metrics_workspace,
            delta,
        )?;
        franken_flush_model_daily_rollup_table(
            tx,
            &HashMap::from([(
                (
                    *day,
                    agent.clone(),
                    metrics_workspace,
                    source.clone(),
                    family.clone(),
                    tier.clone(),
                ),
                delta.clone(),
            )]),
            destination_updated,
        )?;
    }
    for ids in message_ids.chunks(128) {
        let mut params: Vec<ParamValue> = ids.iter().copied().map(ParamValue::from).collect();
        params.push(ParamValue::from(metrics_workspace));
        let id_slots = (1..=ids.len())
            .map(|idx| format!("?{idx}"))
            .collect::<Vec<_>>()
            .join(", ");
        tx.execute_compat(
            &format!("UPDATE message_metrics SET workspace_id = ?{} WHERE message_id IN ({}) AND workspace_id != ?{}",
                params.len(), id_slots, params.len()),
            &params,
        )?;
    }
    tx.execute_compat(
        "UPDATE token_usage SET workspace_id = ?2 WHERE conversation_id = ?1 AND workspace_id IS NOT ?2",
        fparams![conversation_id, workspace_id],
    )?;
    Ok(())
}

/// Subtract only a contribution actually present in the old rollup. Ordinary
/// deferred analytics has neither metrics nor rollups; a missing/underfilled
/// bucket for existing metrics is drift and must roll back, never go negative.
/// Existing destination timestamps are retained; new buckets inherit the old
/// bucket's timestamp rather than rewriting stored measurement provenance.
pub(super) fn franken_relocate_rollup_subtract(
    tx: &FrankenTransaction<'_>,
    table: &str,
    key: Vec<(&str, ParamValue)>,
    workspace_id: i64,
    delta: &UsageRollupDelta,
) -> Result<i64> {
    let predicate = key
        .iter()
        .enumerate()
        .map(|(idx, (name, _))| format!("{name} = ?{}", idx + 1))
        .collect::<Vec<_>>()
        .join(" AND ");
    let mut params: Vec<ParamValue> = key.iter().map(|(_, value)| value.clone()).collect();
    let old_updated: i64 = tx
        .query_row_map(
            &format!("SELECT last_updated FROM {table} WHERE {predicate}"),
            &params,
            |row| row.get_typed(0),
        )
        .with_context(|| {
            format!("Cursor workspace analytics drift: missing {table} source bucket")
        })?;
    let mut destination = params.clone();
    destination[2] = ParamValue::from(workspace_id);
    let destination_updated: Option<i64> = tx
        .query_row_map(
            &format!("SELECT last_updated FROM {table} WHERE {predicate}"),
            &destination,
            |row| row.get_typed(0),
        )
        .optional()?;
    let mut counters = vec![
        ("message_count", delta.message_count),
        ("user_message_count", delta.user_message_count),
        ("assistant_message_count", delta.assistant_message_count),
        ("tool_call_count", delta.tool_call_count),
        ("plan_message_count", delta.plan_message_count),
        (
            "api_coverage_message_count",
            delta.api_coverage_message_count,
        ),
        ("content_tokens_est_total", delta.content_tokens_est_total),
        ("content_tokens_est_user", delta.content_tokens_est_user),
        (
            "content_tokens_est_assistant",
            delta.content_tokens_est_assistant,
        ),
        ("api_tokens_total", delta.api_tokens_total),
        ("api_input_tokens_total", delta.api_input_tokens_total),
        ("api_output_tokens_total", delta.api_output_tokens_total),
        (
            "api_cache_read_tokens_total",
            delta.api_cache_read_tokens_total,
        ),
        (
            "api_cache_creation_tokens_total",
            delta.api_cache_creation_tokens_total,
        ),
        ("api_thinking_tokens_total", delta.api_thinking_tokens_total),
    ];
    if table != "usage_models_daily" {
        counters.extend([
            (
                "plan_content_tokens_est_total",
                delta.plan_content_tokens_est_total,
            ),
            ("plan_api_tokens_total", delta.plan_api_tokens_total),
        ]);
    }
    let mut assignments = Vec::with_capacity(counters.len());
    let mut guards = Vec::with_capacity(counters.len());
    for (column, amount) in counters {
        anyhow::ensure!(
            amount >= 0,
            "Cursor workspace analytics drift: negative {column} contribution"
        );
        params.push(ParamValue::from(amount));
        assignments.push(format!("{column} = {column} - ?{}", params.len()));
        guards.push(format!("{column} >= ?{}", params.len()));
    }
    let changed = tx.execute_compat(
        &format!(
            "UPDATE {table} SET {} WHERE {predicate} AND {}",
            assignments.join(", "),
            guards.join(" AND ")
        ),
        &params,
    )?;
    anyhow::ensure!(
        changed == 1,
        "Cursor workspace analytics drift: underfilled {table} source bucket"
    );
    Ok(destination_updated.unwrap_or(old_updated))
}

/// Update conversation-level token summary columns within a SQLite transaction.
pub(super) fn franken_update_conversation_token_summaries_in_tx(
    tx: &FrankenTransaction<'_>,
    conversation_id: i64,
) -> Result<()> {
    tx.execute_compat(
        "UPDATE conversations SET
            total_input_tokens = (SELECT SUM(input_tokens) FROM token_usage WHERE conversation_id = ?1),
            total_output_tokens = (SELECT SUM(output_tokens) FROM token_usage WHERE conversation_id = ?1),
            total_cache_read_tokens = (SELECT SUM(cache_read_tokens) FROM token_usage WHERE conversation_id = ?1),
            total_cache_creation_tokens = (SELECT SUM(cache_creation_tokens) FROM token_usage WHERE conversation_id = ?1),
            grand_total_tokens = (SELECT SUM(total_tokens) FROM token_usage WHERE conversation_id = ?1),
            estimated_cost_usd = (SELECT SUM(estimated_cost_usd) FROM token_usage WHERE conversation_id = ?1),
            primary_model = (SELECT model_name FROM token_usage WHERE conversation_id = ?1
                             AND model_name IS NOT NULL
                             GROUP BY model_name ORDER BY COUNT(*) DESC LIMIT 1),
            api_call_count = (SELECT COUNT(*) FROM token_usage WHERE conversation_id = ?1
                              AND data_source = 'api'),
            tool_call_count = (SELECT SUM(tool_call_count) FROM token_usage WHERE conversation_id = ?1),
            user_message_count = (SELECT COUNT(*) FROM token_usage WHERE conversation_id = ?1
                                  AND role = 'user'),
            assistant_message_count = (SELECT COUNT(*) FROM token_usage WHERE conversation_id = ?1
                                       AND role IN ('assistant', 'agent'))
         WHERE id = ?1",
        fparams![conversation_id],
    )?;
    Ok(())
}

/// Identity of the `token_usage` ledger a Track B rebuild checkpoint was taken
/// against (GH #386). Any ledger change invalidates the staged aggregate.
pub(super) struct TokenDailyStatsLedgerFingerprint {
    pub(super) row_count: i64,
    pub(super) max_id: i64,
    pub(super) revision_generation: Option<String>,
}

impl TokenDailyStatsLedgerFingerprint {
    pub(super) fn fingerprint(&self) -> String {
        match &self.revision_generation {
            Some(generation) => format!(
                "token_usage-v2:{}:{}:{generation}",
                self.row_count, self.max_id
            ),
            None => format!("token_usage-v1:{}:{}", self.row_count, self.max_id),
        }
    }
}

pub(super) fn token_daily_stats_ledger_fingerprint(
    conn: &FrankenConnection,
) -> Result<TokenDailyStatsLedgerFingerprint> {
    let (row_count, max_id): (i64, i64) = conn.query_row_map(
        "SELECT COUNT(*), COALESCE(MAX(id), 0) FROM token_usage",
        fparams![],
        |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
    )?;
    let revision_generation = conn
        .query_row_map(
            "SELECT value FROM meta WHERE key = ?1",
            fparams![TOKEN_USAGE_REVISION_GENERATION_META_KEY],
            |row| row.get_typed(0),
        )
        .optional()?;
    Ok(TokenDailyStatsLedgerFingerprint {
        row_count,
        max_id,
        revision_generation,
    })
}

/// Persisted Track B rebuild cursor (GH #386), stored as JSON under
/// [`TOKEN_DAILY_STATS_REBUILD_CURSOR_KEY`] in `meta`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct TokenDailyStatsRebuildCursor {
    pub(super) ledger_fingerprint: String,
    pub(super) last_conversation_id: i64,
    pub(super) rows_created: usize,
}

pub(super) fn token_daily_stats_rebuild_stage_ddl() -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {TOKEN_DAILY_STATS_REBUILD_STAGE_TABLE} (
            day_id INTEGER NOT NULL,
            agent_slug TEXT NOT NULL,
            source_id TEXT NOT NULL DEFAULT 'all',
            model_family TEXT NOT NULL DEFAULT 'all',
            api_call_count INTEGER NOT NULL DEFAULT 0,
            user_message_count INTEGER NOT NULL DEFAULT 0,
            assistant_message_count INTEGER NOT NULL DEFAULT 0,
            tool_message_count INTEGER NOT NULL DEFAULT 0,
            total_input_tokens INTEGER NOT NULL DEFAULT 0,
            total_output_tokens INTEGER NOT NULL DEFAULT 0,
            total_cache_read_tokens INTEGER NOT NULL DEFAULT 0,
            total_cache_creation_tokens INTEGER NOT NULL DEFAULT 0,
            total_thinking_tokens INTEGER NOT NULL DEFAULT 0,
            grand_total_tokens INTEGER NOT NULL DEFAULT 0,
            total_content_chars INTEGER NOT NULL DEFAULT 0,
            total_tool_calls INTEGER NOT NULL DEFAULT 0,
            estimated_cost_usd REAL NOT NULL DEFAULT 0.0,
            session_count INTEGER NOT NULL DEFAULT 0,
            last_updated INTEGER NOT NULL,
            PRIMARY KEY (day_id, agent_slug, source_id, model_family)
        )"
    )
}

pub(super) fn read_token_daily_stats_rebuild_cursor(
    conn: &FrankenConnection,
) -> Result<Option<TokenDailyStatsRebuildCursor>> {
    let raw: Option<String> = conn
        .query_row_map(
            "SELECT value FROM meta WHERE key = ?1",
            fparams![TOKEN_DAILY_STATS_REBUILD_CURSOR_KEY],
            |row| row.get_typed::<String>(0),
        )
        .optional()
        .context("reading token_daily_stats rebuild cursor")?;
    let Some(raw) = raw else {
        return Ok(None);
    };
    match serde_json::from_str::<TokenDailyStatsRebuildCursor>(&raw) {
        Ok(cursor) => Ok(Some(cursor)),
        Err(err) => {
            // A malformed cursor is treated as "no checkpoint": the rebuild
            // resets the stage rather than trusting an unreadable position.
            tracing::warn!(
                target: "cass::analytics",
                error = %err,
                "token_daily_stats rebuild cursor is unreadable; restarting from zero"
            );
            Ok(None)
        }
    }
}

pub(super) fn write_token_daily_stats_rebuild_cursor(
    tx: &FrankenTransaction<'_>,
    cursor: &TokenDailyStatsRebuildCursor,
) -> Result<()> {
    let value = serde_json::to_string(cursor).context("serializing token_daily_stats cursor")?;
    tx.execute_compat(
        "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
        fparams![TOKEN_DAILY_STATS_REBUILD_CURSOR_KEY, value.as_str()],
    )?;
    Ok(())
}
