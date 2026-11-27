use super::aggregators_and_pricing::{
    AnalyticsRollupAggregator, MessageMetricsEntry, TokenStatsAggregator, has_plan_for_role,
};
use super::claude_usage_dedup;
use super::omp_and_semantic_meta::{
    LEGACY_OMP_ANALYTICS_CURSOR_CONTEXT_META_KEY, LEGACY_OMP_ANALYTICS_CURSOR_LAST_ID_META_KEY,
    LEGACY_OMP_ANALYTICS_CURSOR_PROCESSED_META_KEY, LEGACY_OMP_ANALYTICS_REBUILT_META_KEY,
};
use super::orphan_fk_cleanup::delete_rows_by_i64_chunks;
use super::reconcile_and_codebuff::codebuff_tool_count;
use super::source_and_historical_import::historical_table_exists;
use super::storage_core::FrankenStorage;
use super::tx_analytics_rollups::{
    TOKEN_DAILY_STATS_REBUILD_CURSOR_KEY, TOKEN_DAILY_STATS_REBUILD_STAGE_TABLE,
    TOKEN_DAILY_STATS_TABLE, TOKEN_USAGE_REVISION_GENERATION_META_KEY,
    TokenDailyStatsLedgerFingerprint, TokenDailyStatsRebuildCursor,
    franken_flush_analytics_rollups_in_tx, franken_insert_message_metrics_batched_in_tx,
    franken_update_token_daily_stats_batched_in_tx_for_table,
    read_token_daily_stats_rebuild_cursor, token_daily_stats_ledger_fingerprint,
    token_daily_stats_rebuild_stage_ddl, write_token_daily_stats_rebuild_cursor,
};
use crate::franken_sync::{
    Row as FrankenRow, TransactionBehavior,
    compat::{
        ConnectionExt as FrankenConnectionExt, OptionalExtension as FrankenOptionalExtension,
        RowExt as FrankenRowExt,
    },
};
use anyhow::{Context, Result, bail};
use std::collections::HashMap;
use std::time::Instant;

impl FrankenStorage {
    /// Rebuild token_daily_stats from the token_usage ledger.
    ///
    /// Resumable (GH #386): every conversation batch is aggregated into a
    /// persistent staging table and committed together with a `meta` cursor
    /// (`last_conversation_id` + a ledger fingerprint). An interrupted run —
    /// watchdog exit, OOM kill, Ctrl-C — therefore resumes from the last
    /// committed batch instead of restarting the message scan from zero, as
    /// long as the `token_usage` ledger has not changed underneath it. The
    /// live `token_daily_stats` table is replaced only by the final atomic
    /// swap, so readers never observe a half-rebuilt rollup.
    pub fn rebuild_token_daily_stats(&self) -> Result<usize> {
        self.rebuild_token_daily_stats_with_progress(None, None)
    }

    pub(super) fn rebuild_token_daily_stats_with_progress(
        &self,
        heartbeat: Option<&dyn Fn()>,
        control: Option<&dyn Fn() -> Result<()>>,
    ) -> Result<usize> {
        const CONVERSATION_BATCH_SIZE: usize = 1_000;
        const TOKEN_USAGE_BATCH_SIZE: usize = 10_000;

        if let Some(control) = control {
            control()?;
        }

        if let Some(heartbeat) = heartbeat {
            heartbeat();
        }
        let ledger = token_daily_stats_ledger_fingerprint(&self.conn)?;
        tracing::info!(
            target: "cass::analytics",
            total_usage_rows = ledger.row_count,
            "token_daily_stats_rebuild_start"
        );
        if let Some(heartbeat) = heartbeat {
            heartbeat();
        }

        // A cursor is only meaningful together with the stage table it
        // describes; a cursor whose stage is gone (manual cleanup, partial
        // restore) must reset rather than resume onto an empty stage.
        let stage_table_present =
            historical_table_exists(&self.conn, TOKEN_DAILY_STATS_REBUILD_STAGE_TABLE)?;
        self.conn
            .execute(&token_daily_stats_rebuild_stage_ddl())
            .context("creating token_daily_stats rebuild stage table")?;

        let resume = read_token_daily_stats_rebuild_cursor(&self.conn)?.filter(|cursor| {
            stage_table_present && cursor.ledger_fingerprint == ledger.fingerprint()
        });
        let (mut last_conversation_id, mut rows_created) = match resume {
            Some(cursor) => {
                tracing::info!(
                    target: "cass::analytics",
                    last_conversation_id = cursor.last_conversation_id,
                    rows_created = cursor.rows_created,
                    "token_daily_stats_rebuild_resume"
                );
                (cursor.last_conversation_id, cursor.rows_created)
            }
            None => {
                if let Some(control) = control {
                    control()?;
                }
                // No checkpoint, or the ledger changed since it was taken:
                // the staged partial aggregate is unusable. Reset it in one
                // transaction so a crash here leaves either the old
                // checkpoint (still consistent) or a clean slate.
                let mut tx = self
                    .conn
                    .transaction_with_behavior(TransactionBehavior::Immediate)?;
                tx.execute(&format!(
                    "DELETE FROM {TOKEN_DAILY_STATS_REBUILD_STAGE_TABLE}"
                ))?;
                tx.execute_compat(
                    "DELETE FROM meta WHERE key = ?1",
                    fparams![TOKEN_DAILY_STATS_REBUILD_CURSOR_KEY],
                )?;
                tx.commit()?;
                (0_i64, 0_usize)
            }
        };

        loop {
            if let Some(control) = control {
                control()?;
            }
            let mut tx = self
                .conn
                .transaction_with_behavior(TransactionBehavior::Immediate)?;
            let conversation_rows = tx.query_map_collect(
                "SELECT c.id, c.started_at, c.source_id,
                        COALESCE((SELECT a.slug FROM agents a WHERE a.id = c.agent_id), 'unknown')
                 FROM conversations c
                 WHERE c.id > ?1
                 ORDER BY c.id
                 LIMIT ?2",
                fparams![last_conversation_id, CONVERSATION_BATCH_SIZE as i64],
                |row| {
                    Ok((
                        row.get_typed::<i64>(0)?,
                        row.get_typed::<Option<i64>>(1)?,
                        row.get_typed::<String>(2)?,
                        row.get_typed::<String>(3)?,
                    ))
                },
            )?;
            if conversation_rows.is_empty() {
                break;
            }

            let mut aggregate = TokenStatsAggregator::new();

            for (conversation_id, started_at, source_id, agent_slug) in conversation_rows {
                if let Some(control) = control {
                    control()?;
                }
                last_conversation_id = conversation_id;
                let conversation_day_id = started_at.map(Self::day_id_from_millis).unwrap_or(0);
                let mut last_token_usage_id = 0_i64;
                let mut session_model_family = String::from("unknown");

                loop {
                    if let Some(control) = control {
                        control()?;
                    }
                    let usage_rows = tx.query_map_collect(
                        "SELECT id, day_id, role,
                                COALESCE(model_family, 'unknown'),
                                input_tokens, output_tokens, cache_read_tokens,
                                cache_creation_tokens, thinking_tokens,
                                has_tool_calls, tool_call_count,
                                content_chars, estimated_cost_usd
                         FROM token_usage
                         WHERE conversation_id = ?1
                           AND id > ?2
                         ORDER BY id
                         LIMIT ?3",
                        fparams![
                            conversation_id,
                            last_token_usage_id,
                            TOKEN_USAGE_BATCH_SIZE as i64
                        ],
                        |row| {
                            Ok((
                                row.get_typed::<i64>(0)?,
                                row.get_typed::<i64>(1)?,
                                row.get_typed::<String>(2)?,
                                row.get_typed::<String>(3)?,
                                row.get_typed::<Option<i64>>(4)?,
                                row.get_typed::<Option<i64>>(5)?,
                                row.get_typed::<Option<i64>>(6)?,
                                row.get_typed::<Option<i64>>(7)?,
                                row.get_typed::<Option<i64>>(8)?,
                                row.get_typed::<i64>(9)?,
                                row.get_typed::<i64>(10)?,
                                row.get_typed::<i64>(11)?,
                                row.get_typed::<Option<f64>>(12)?,
                            ))
                        },
                    )?;
                    if usage_rows.is_empty() {
                        break;
                    }

                    for (
                        token_usage_id,
                        day_id,
                        role,
                        model_family,
                        input_tokens,
                        output_tokens,
                        cache_read_tokens,
                        cache_creation_tokens,
                        thinking_tokens,
                        has_tool_calls,
                        tool_call_count,
                        content_chars,
                        estimated_cost_usd,
                    ) in usage_rows
                    {
                        last_token_usage_id = token_usage_id;
                        if model_family != "unknown" {
                            session_model_family = model_family.clone();
                        }
                        let usage = crate::connectors::ExtractedTokenUsage {
                            model_name: None,
                            provider: None,
                            input_tokens,
                            output_tokens,
                            cache_read_tokens,
                            cache_creation_tokens,
                            thinking_tokens,
                            service_tier: None,
                            has_tool_calls: has_tool_calls != 0,
                            tool_call_count: u32::try_from(tool_call_count.max(0)).unwrap_or(0),
                            data_source: franken_agent_detection::TokenDataSource::Api,
                        };
                        aggregate.record(
                            &agent_slug,
                            &source_id,
                            day_id,
                            &model_family,
                            &role,
                            &usage,
                            content_chars,
                            estimated_cost_usd.unwrap_or(0.0),
                        );
                    }
                    if let Some(heartbeat) = heartbeat {
                        heartbeat();
                    }
                }

                aggregate.record_session(
                    &agent_slug,
                    &source_id,
                    conversation_day_id,
                    &session_model_family,
                );
            }

            let entries = aggregate.expand();
            if let Some(control) = control {
                control()?;
            }
            rows_created = rows_created.saturating_add(entries.len());
            franken_update_token_daily_stats_batched_in_tx_for_table(
                &tx,
                TOKEN_DAILY_STATS_REBUILD_STAGE_TABLE,
                &entries,
            )?;
            write_token_daily_stats_rebuild_cursor(
                &tx,
                &TokenDailyStatsRebuildCursor {
                    ledger_fingerprint: ledger.fingerprint(),
                    last_conversation_id,
                    rows_created,
                },
            )?;
            if let Some(control) = control {
                control()?;
            }
            tx.commit()?;
            tracing::debug!(
                target: "cass::analytics",
                last_conversation_id,
                rows_created,
                "token_daily_stats_rebuild_checkpoint"
            );
            if let Some(heartbeat) = heartbeat {
                heartbeat();
            }
        }

        // Per-batch commits mean the scan is not one snapshot. Publishing when
        // the ledger changed would knowingly replace the last-good live rollup
        // with a potentially incomplete mixture of snapshots. Fail closed and
        // leave both the live table and resumable stage/cursor untouched. The
        // next run observes the new fingerprint and safely restarts the stage.
        // Check the exact ledger generation inside the publication transaction;
        // otherwise a revision between admission and BEGIN could publish a
        // stale stage despite passing the earlier fingerprint check.
        if let Some(control) = control {
            control()?;
        }
        let mut tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (row_count, max_id) = tx.query_row_map(
            "SELECT COUNT(*), COALESCE(MAX(id), 0) FROM token_usage",
            fparams![],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
        )?;
        let revision_generation = tx
            .query_row_map(
                "SELECT value FROM meta WHERE key = ?1",
                fparams![TOKEN_USAGE_REVISION_GENERATION_META_KEY],
                |row| row.get_typed(0),
            )
            .optional()?;
        let ledger_after = TokenDailyStatsLedgerFingerprint {
            row_count,
            max_id,
            revision_generation,
        };
        if ledger_after.fingerprint() != ledger.fingerprint() {
            bail!(
                "token_daily_stats phase=pre_publish_consistency: token_usage ledger changed during rebuild (before={}, after={}); the last-good live rollup was preserved — rerun `cass analytics rebuild --track b` once ingest is idle",
                ledger.fingerprint(),
                ledger_after.fingerprint()
            );
        }

        // Final atomic swap: the staged aggregate becomes the live rollup in
        // one transaction, the checkpoint is retired with it, and the scratch
        // table is dropped so a finished rebuild leaves the archive schema
        // exactly as it found it.
        tx.execute(&format!("DELETE FROM {TOKEN_DAILY_STATS_TABLE}"))?;
        tx.execute(&format!(
            "INSERT INTO {TOKEN_DAILY_STATS_TABLE} (
                day_id, agent_slug, source_id, model_family,
                api_call_count, user_message_count, assistant_message_count, tool_message_count,
                total_input_tokens, total_output_tokens, total_cache_read_tokens,
                total_cache_creation_tokens, total_thinking_tokens, grand_total_tokens,
                total_content_chars, total_tool_calls, estimated_cost_usd, session_count,
                last_updated
            )
            SELECT
                day_id, agent_slug, source_id, model_family,
                api_call_count, user_message_count, assistant_message_count, tool_message_count,
                total_input_tokens, total_output_tokens, total_cache_read_tokens,
                total_cache_creation_tokens, total_thinking_tokens, grand_total_tokens,
                total_content_chars, total_tool_calls, estimated_cost_usd, session_count,
                last_updated
            FROM {TOKEN_DAILY_STATS_REBUILD_STAGE_TABLE}"
        ))?;
        tx.execute(&format!(
            "DROP TABLE IF EXISTS {TOKEN_DAILY_STATS_REBUILD_STAGE_TABLE}"
        ))?;
        tx.execute_compat(
            "DELETE FROM meta WHERE key = ?1",
            fparams![TOKEN_DAILY_STATS_REBUILD_CURSOR_KEY],
        )?;
        if let Some(control) = control {
            control()?;
        }
        tx.commit()?;
        if let Some(heartbeat) = heartbeat {
            heartbeat();
        }

        tracing::info!(
            target: "cass::analytics",
            rows_created,
            "token_daily_stats_rebuild_complete"
        );

        Ok(rows_created)
    }

    /// Rebuild analytics tables (message_metrics + rollups) from existing
    /// messages in the database. Does NOT re-parse raw agent session files.
    ///
    /// Full rebuild: equivalent to [`Self::rebuild_analytics_since`] with
    /// `None`.
    pub fn rebuild_analytics(&self) -> Result<AnalyticsRebuildResult> {
        self.rebuild_analytics_since(None)
    }

    /// Rebuild analytics tables (message_metrics + rollups) from existing
    /// messages, optionally restricted to messages on or after `since_ms`.
    ///
    /// With `since_ms = None` every rollup row is dropped and re-derived from
    /// the whole `messages` table. With a cutoff, the window is widened to the
    /// start of the UTC day containing `since_ms` (rollups are bucketed by
    /// day/hour, so a partial day cannot be rebuilt independently): rollup
    /// rows for `day_id >= cutoff_day` (and `hour_id` within those days) are
    /// dropped and only messages whose effective timestamp
    /// (`COALESCE(created_at, conversation.started_at)`) falls on or after
    /// that day start are rescanned. Older rollup rows are left untouched
    /// (GH #412). Limitation: if a message's timestamp moved from a day
    /// outside the window to one inside it, its stale `message_metrics` row
    /// is kept (`INSERT OR IGNORE`) and the old day's rollup still counts
    /// it; a full rebuild reconciles that.
    ///
    /// Pagination is keyset (`WHERE m.id > last_id`), so the cost is linear in
    /// the number of rescanned rows rather than quadratic as the previous
    /// `LIMIT/OFFSET` form was. A progress event is logged per chunk.
    ///
    /// GH #424: the keyset runs over `messages` alone and resolves the
    /// `conversations` / `agents` dimensions from in-memory maps; every chunk
    /// commits on its own. See `Self::rebuild_analytics_since_with_chunk_size`.
    pub fn rebuild_analytics_since(&self, since_ms: Option<i64>) -> Result<AnalyticsRebuildResult> {
        self.rebuild_analytics_since_with_chunk_size(since_ms, ANALYTICS_REBUILD_CHUNK_SIZE)
    }

    /// [`Self::rebuild_analytics_since`] with an explicit keyset chunk size
    /// (rows fetched per `SELECT`, and the unit of commit).
    pub(crate) fn rebuild_analytics_since_with_chunk_size(
        &self,
        since_ms: Option<i64>,
        chunk_size: usize,
    ) -> Result<AnalyticsRebuildResult> {
        self.rebuild_analytics_since_with_chunk_size_and_progress(
            since_ms, chunk_size, None, None, None, None,
        )
    }

    pub(super) fn reset_legacy_omp_analytics(
        &self,
        context: &str,
        progress: Option<&dyn Fn(i64, i64)>,
        heartbeat: Option<&dyn Fn()>,
        control: Option<&dyn Fn() -> Result<()>>,
    ) -> Result<()> {
        const TABLES: [(&str, &str, &str); 4] = [
            (
                "message_metrics",
                "SELECT rowid FROM message_metrics ORDER BY rowid LIMIT 128",
                "DELETE FROM message_metrics WHERE rowid IN",
            ),
            (
                "usage_hourly",
                "SELECT rowid FROM usage_hourly ORDER BY rowid LIMIT 128",
                "DELETE FROM usage_hourly WHERE rowid IN",
            ),
            (
                "usage_daily",
                "SELECT rowid FROM usage_daily ORDER BY rowid LIMIT 128",
                "DELETE FROM usage_daily WHERE rowid IN",
            ),
            (
                "usage_models_daily",
                "SELECT rowid FROM usage_models_daily ORDER BY rowid LIMIT 128",
                "DELETE FROM usage_models_daily WHERE rowid IN",
            ),
        ];
        let checkpoint = || -> Result<()> {
            if let Some(progress) = progress {
                progress(0, 0);
            }
            if let Some(heartbeat) = heartbeat {
                heartbeat();
            }
            if let Some(control) = control {
                control()?;
            }
            Ok(())
        };
        checkpoint()?;
        // Inspect the actual archive before invalidating authority. The current
        // schema uses rowid tables; a historical alternate layout must not be
        // mistaken for the bounded rowid-seek delete path.
        let layouts: Vec<(String, String, String, i64)> =
            self.conn
                .query_map_collect("PRAGMA table_list", fparams![], |row| {
                    Ok((
                        row.get_typed(0)?,
                        row.get_typed(1)?,
                        row.get_typed(2)?,
                        row.get_typed(4)?,
                    ))
                })?;
        let mut bounded = true;
        for (table, _, _) in TABLES {
            let layout = layouts.iter().find(|(schema, name, kind, _)| {
                schema == "main" && name == table && kind == "table"
            });
            let Some((_, _, _, without_rowid)) = layout else {
                bail!("cannot reset legacy OMP analytics: missing ordinary table {table}");
            };
            if *without_rowid != 0 {
                bounded = false;
                tracing::warn!(
                    target: "cass::analytics",
                    table,
                    "legacy OMP analytics table is WITHOUT ROWID; retaining the unbounded transactional reset"
                );
            }
        }
        checkpoint()?;
        let mut tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute_compat(
            "DELETE FROM meta WHERE key IN (?1, ?2, ?3, ?4)",
            fparams![
                LEGACY_OMP_ANALYTICS_CURSOR_CONTEXT_META_KEY,
                LEGACY_OMP_ANALYTICS_CURSOR_LAST_ID_META_KEY,
                LEGACY_OMP_ANALYTICS_CURSOR_PROCESSED_META_KEY,
                LEGACY_OMP_ANALYTICS_REBUILT_META_KEY
            ],
        )?;
        if let Some(control) = control {
            control()?;
        }
        tx.commit()?;
        checkpoint()?;

        if bounded {
            // Always take the first remaining keys, including negative IDs.
            // Missing cursor authority makes interruption during clearing
            // idempotent: reopen clears the remainder before rebuilding.
            for (_, select_sql, delete_prefix) in TABLES {
                loop {
                    checkpoint()?;
                    let mut tx = self
                        .conn
                        .transaction_with_behavior(TransactionBehavior::Immediate)?;
                    let ids: Vec<i64> =
                        tx.query_map_collect(select_sql, fparams![], |row| row.get_typed(0))?;
                    if ids.is_empty() {
                        tx.commit()?;
                        break;
                    }
                    let deleted = delete_rows_by_i64_chunks(&tx, delete_prefix, &ids)?;
                    anyhow::ensure!(
                        deleted == ids.len(),
                        "legacy OMP analytics reset lost selected rows"
                    );
                    if let Some(control) = control {
                        control()?;
                    }
                    tx.commit()?;
                    checkpoint()?;
                }
            }
        }

        checkpoint()?;
        let mut tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if !bounded {
            // Preserve the previous all-table reset for unexpected layouts.
            // This fallback deliberately carries no row-count/RSS bound.
            for (table, _, _) in TABLES {
                tx.execute(&format!("DELETE FROM {table}"))?;
                if let Some(control) = control {
                    control()?;
                }
            }
        }
        for (table, _, _) in TABLES {
            let remaining: Vec<i64> = tx.query_map_collect(
                &format!("SELECT 1 FROM {table} LIMIT 1"),
                fparams![],
                |row| row.get_typed(0),
            )?;
            anyhow::ensure!(
                remaining.is_empty(),
                "legacy OMP analytics reset left rows in {table}"
            );
        }
        for (key, value) in [
            (LEGACY_OMP_ANALYTICS_CURSOR_CONTEXT_META_KEY, context),
            (LEGACY_OMP_ANALYTICS_CURSOR_LAST_ID_META_KEY, "start"),
            (LEGACY_OMP_ANALYTICS_CURSOR_PROCESSED_META_KEY, "0"),
        ] {
            tx.execute_compat(
                "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
                fparams![key, value],
            )?;
        }
        if let Some(control) = control {
            control()?;
        }
        tx.commit()?;
        checkpoint()?;
        Ok(())
    }

    pub(super) fn rebuild_analytics_since_with_chunk_size_and_progress(
        &self,
        since_ms: Option<i64>,
        chunk_size: usize,
        progress: Option<&dyn Fn(i64, i64)>,
        heartbeat: Option<&dyn Fn()>,
        resume_context: Option<&str>,
        control: Option<&dyn Fn() -> Result<()>>,
    ) -> Result<AnalyticsRebuildResult> {
        if let Some(control) = control {
            control()?;
        }
        let start = Instant::now();
        let chunk_size = i64::try_from(chunk_size.max(1)).unwrap_or(i64::MAX);

        // Day-aligned scope. `None` => full rebuild.
        let scope = since_ms.map(|ms| {
            let cutoff_day = Self::day_id_from_millis(ms);
            let cutoff_ms = Self::millis_from_day_id(cutoff_day);
            let cutoff_hour = Self::hour_id_from_millis(cutoff_ms);
            (cutoff_day, cutoff_hour, cutoff_ms)
        });
        if resume_context.is_some() && scope.is_some() {
            bail!("the legacy OMP resume cursor is valid only for a full analytics rebuild");
        }

        // The keyset walk below visits every `messages` row in both modes
        // (the day cutoff is applied in Rust), so the progress denominator
        // is the whole table.
        if let Some(heartbeat) = heartbeat {
            heartbeat();
        }
        if let Some(control) = control {
            control()?;
        }
        let total_messages: i64 =
            self.conn
                .query_row_map("SELECT COUNT(*) FROM messages", fparams![], |row| {
                    row.get_typed(0)
                })?;
        tracing::info!(
            target: "cass::analytics",
            total_messages,
            since_ms = scope.map(|(_, _, ms)| ms),
            "analytics_rebuild_start"
        );
        if let Some(progress) = progress {
            progress(0, total_messages);
        }
        if let Some(heartbeat) = heartbeat {
            heartbeat();
        }

        // GH #424: resolve the dimension tables once, in memory, instead of
        // JOINing `conversations` (plus the agents subquery) into every
        // chunk. Avoiding the repeated multi-table JOIN keeps every 10k-row
        // keyset chunk fast. Both maps are tiny next to `messages`: one entry
        // per conversation and one per agent.
        if let Some(control) = control {
            control()?;
        }
        let agent_slugs: HashMap<i64, String> = self
            .conn
            .query_map_collect("SELECT id, slug FROM agents", fparams![], |row| {
                Ok((row.get_typed(0)?, row.get_typed(1)?))
            })?
            .into_iter()
            .collect();
        if let Some(heartbeat) = heartbeat {
            heartbeat();
        }
        if let Some(control) = control {
            control()?;
        }
        let conversation_dims: HashMap<i64, AnalyticsConversationDim> = self
            .conn
            .query_map_collect(
                "SELECT id, started_at, source_id, workspace_id, agent_id FROM conversations",
                fparams![],
                |row| {
                    let id: i64 = row.get_typed(0)?;
                    let agent_id: Option<i64> = row.get_typed(4)?;
                    Ok((
                        id,
                        AnalyticsConversationDim {
                            started_at: row.get_typed(1)?,
                            source_id: row.get_typed(2)?,
                            workspace_id: row.get_typed(3)?,
                            // Same degradation as the former correlated
                            // subquery: NULL or dangling agent_id -> 'unknown',
                            // matching the FTS / lexical rebuild paths.
                            agent_slug: agent_id
                                .and_then(|agent_id| agent_slugs.get(&agent_id).cloned())
                                .unwrap_or_else(|| "unknown".to_string()),
                        },
                    ))
                },
            )?
            .into_iter()
            .collect();
        if let Some(heartbeat) = heartbeat {
            heartbeat();
        }

        // Outer `Option`: whether a complete cursor belongs to this migration
        // context. The inner optional id uses `None` as the explicit
        // pre-first-row sentinel, distinct from a legitimate `i64::MIN` id.
        // Persisting `processed` makes a bounded prefix-consistency check
        // possible on resume without reconstructing progress from rollups.
        let resume_cursor: Option<(Option<i64>, i64)> = if let Some(context) = resume_context {
            let recorded_context: Option<String> = self
                .conn
                .query_row_map(
                    "SELECT value FROM meta WHERE key = ?1",
                    fparams![LEGACY_OMP_ANALYTICS_CURSOR_CONTEXT_META_KEY],
                    |row| row.get_typed(0),
                )
                .optional()?;
            let recorded_last_id: Option<String> = self
                .conn
                .query_row_map(
                    "SELECT value FROM meta WHERE key = ?1",
                    fparams![LEGACY_OMP_ANALYTICS_CURSOR_LAST_ID_META_KEY],
                    |row| row.get_typed(0),
                )
                .optional()?;
            let recorded_processed: Option<String> = self
                .conn
                .query_row_map(
                    "SELECT value FROM meta WHERE key = ?1",
                    fparams![LEGACY_OMP_ANALYTICS_CURSOR_PROCESSED_META_KEY],
                    |row| row.get_typed(0),
                )
                .optional()?;
            if recorded_context.as_deref() == Some(context) {
                let candidate = match (
                    recorded_last_id,
                    recorded_processed.and_then(|value| value.parse::<i64>().ok()),
                ) {
                    (Some(last_id), Some(0)) if last_id == "start" => Some((None, 0)),
                    (Some(last_id), Some(processed))
                        if processed > 0 && processed <= total_messages =>
                    {
                        last_id
                            .parse::<i64>()
                            .ok()
                            .map(|last_id| (Some(last_id), processed))
                    }
                    _ => None,
                };
                match candidate {
                    Some((None, 0)) => {
                        // The initial cursor certifies an empty derivative set.
                        // A stale cursor over existing rows would replay additive
                        // rollups even when metric INSERT OR IGNORE skips them.
                        let mut empty = true;
                        for table in [
                            "message_metrics",
                            "usage_hourly",
                            "usage_daily",
                            "usage_models_daily",
                        ] {
                            if let Some(control) = control {
                                control()?;
                            }
                            let row: Option<i64> = self
                                .conn
                                .query_row_map(
                                    &format!("SELECT 1 FROM {table} LIMIT 1"),
                                    fparams![],
                                    |row| row.get_typed(0),
                                )
                                .optional()
                                .with_context(|| {
                                    format!("validating initial analytics cursor against {table}")
                                })?;
                            if row.is_some() {
                                empty = false;
                                break;
                            }
                        }
                        if empty {
                            Some((None, 0))
                        } else {
                            tracing::warn!(
                                target: "cass::analytics",
                                "legacy OMP initial analytics cursor has nonempty derivatives; restarting from zero"
                            );
                            None
                        }
                    }
                    Some((Some(last_id), processed)) => {
                        // The cursor counts every fetched message row, including
                        // orphaned and out-of-window rows. Verify that its exact
                        // committed prefix still exists before adding any more
                        // rollup deltas. A repair or prune between index runs can
                        // otherwise delete an already-counted row while leaving
                        // `processed <= total_messages`; blindly resuming would
                        // preserve the deleted row in additive usage rollups.
                        let (prefix_count, prefix_last_id): (i64, Option<i64>) =
                            self.conn.query_row_map(
                                "SELECT COUNT(*), MAX(id) FROM messages WHERE id <= ?1",
                                fparams![last_id],
                                |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
                            )?;
                        if prefix_count == processed && prefix_last_id == Some(last_id) {
                            Some((Some(last_id), processed))
                        } else {
                            tracing::warn!(
                                target: "cass::analytics",
                                cursor_last_id = last_id,
                                cursor_processed = processed,
                                observed_prefix_count = prefix_count,
                                observed_prefix_last_id = ?prefix_last_id,
                                "legacy OMP analytics cursor prefix changed; restarting from zero"
                            );
                            None
                        }
                    }
                    other => other,
                }
            } else {
                None
            }
        } else {
            None
        };

        // Scope reset, committed on its own. GH #424 (second finding): one
        // write transaction spanning the whole rebuild made every successive
        // chunk slower on the engine (+~8 s per 10k-row chunk, extrapolating
        // to ~4.5 h for 652k rows); committing per chunk holds the cost flat
        // (~30 min on that archive). The trade-off is that a reader during
        // the rebuild sees a partially rebuilt rollup instead of the old one;
        // an interrupted run leaves a prefix that the next full rebuild
        // simply re-derives.
        if resume_cursor.is_none()
            && let Some(context) = resume_context
        {
            self.reset_legacy_omp_analytics(context, progress, heartbeat, control)?;
        }
        if resume_cursor.is_none() && resume_context.is_none() {
            if let Some(control) = control {
                control()?;
            }
            let mut tx = self
                .conn
                .transaction_with_behavior(TransactionBehavior::Immediate)?;
            match scope {
                None => {
                    tx.execute("DELETE FROM message_metrics")?;
                    tx.execute("DELETE FROM usage_hourly")?;
                    tx.execute("DELETE FROM usage_daily")?;
                    tx.execute("DELETE FROM usage_models_daily")?;
                }
                Some((cutoff_day, cutoff_hour, _)) => {
                    tx.execute_compat(
                        "DELETE FROM message_metrics WHERE day_id >= ?1",
                        fparams![cutoff_day],
                    )?;
                    tx.execute_compat(
                        "DELETE FROM usage_hourly WHERE hour_id >= ?1",
                        fparams![cutoff_hour],
                    )?;
                    tx.execute_compat(
                        "DELETE FROM usage_daily WHERE day_id >= ?1",
                        fparams![cutoff_day],
                    )?;
                    tx.execute_compat(
                        "DELETE FROM usage_models_daily WHERE day_id >= ?1",
                        fparams![cutoff_day],
                    )?;
                }
            }
            // Any ordinary full or scoped rebuild changes the live
            // message-metrics/usage surfaces outside the OMP cursor's
            // transaction chain. Retaining that cursor would make a later
            // migration resume add already-rebuilt rollup deltas again.
            // Invalidate it atomically with this reset; the migration then
            // restarts from a known-empty scope instead of double-counting.
            tx.execute_compat(
                "DELETE FROM meta WHERE key IN (?1, ?2, ?3)",
                fparams![
                    LEGACY_OMP_ANALYTICS_CURSOR_CONTEXT_META_KEY,
                    LEGACY_OMP_ANALYTICS_CURSOR_LAST_ID_META_KEY,
                    LEGACY_OMP_ANALYTICS_CURSOR_PROCESSED_META_KEY
                ],
            )?;
            tx.commit()?;
        }

        // Keyset cursor: the largest message id fetched so far. The first
        // page omits the lower bound so the full signed SQLite key domain,
        // including `i64::MIN`, is reachable; later pages use `id > last_id`.
        let (resumed_last_id, mut processed) = resume_cursor.unwrap_or((None, 0));
        let starting_processed = processed;
        if (starting_processed > 0 || resume_context.is_some())
            && let Some(progress) = progress
        {
            progress(starting_processed, total_messages);
        }
        let mut last_id = resumed_last_id.unwrap_or(i64::MIN);
        let mut first_page = resumed_last_id.is_none();
        // Lower bound on the effective timestamp. A full rebuild must admit
        // every row (including any pre-1970 negative timestamp), so it uses
        // `i64::MIN` rather than 0.
        let cutoff_ms: i64 = scope.map_or(i64::MIN, |(_, _, ms)| ms);
        // Rows fetched (progress against `total_messages`) vs rows inside
        // the rebuild window.
        let mut kept: i64 = 0;
        let mut total_inserted: usize = 0;
        let mut usage_hourly_rows: usize = 0;
        let mut usage_daily_rows: usize = 0;
        let mut usage_models_daily_rows: usize = 0;
        // Local fix (review 2026-11-02, U1): same reply-once rule as ingest.
        // One state for the whole walk: the keyset scan interleaves
        // conversations, so it must not be reset per chunk.
        let mut claude_usage = claude_usage_dedup::ClaudeReplyUsageDedup::default();

        loop {
            if let Some(control) = control {
                control()?;
            }
            // Plan with byte lengths before hydrating raw payloads. A single
            // oversized native message is processed alone without truncation.
            let fetch_limit = if resume_context.is_some() {
                let lower = if first_page {
                    i64::MIN
                } else {
                    let Some(next) = last_id.checked_add(1) else {
                        break;
                    };
                    next
                };
                let lengths: Vec<i64> = self.conn.query_map_collect(
                    "SELECT COALESCE(LENGTH(CAST(content AS BLOB)), 0)
                            + COALESCE(LENGTH(CAST(extra_json AS BLOB)), 0)
                            + COALESCE(LENGTH(extra_bin), 0)
                     FROM messages WHERE id >= ?1 ORDER BY id LIMIT ?2",
                    fparams![
                        lower,
                        chunk_size.min(LEGACY_OMP_ANALYTICS_CHUNK_SIZE as i64)
                    ],
                    |row| row.get_typed(0),
                )?;
                let mut bytes = 0_i64;
                let mut count = 0_i64;
                for length in lengths {
                    let next = bytes.saturating_add(length.max(0));
                    if count > 0 && next > LEGACY_OMP_ANALYTICS_PAGE_BYTES {
                        break;
                    }
                    bytes = next;
                    count += 1;
                }
                if count == 0 {
                    break;
                }
                count
            } else {
                chunk_size
            };
            if let Some(control) = control {
                control()?;
            }
            let decode_row = |row: &FrankenRow| {
                let extra_json = row
                    .get_typed::<Option<String>>(3)?
                    .and_then(|s| serde_json::from_str(&s).ok())
                    .or_else(|| {
                        row.get_typed::<Option<Vec<u8>>>(4)
                            .ok()
                            .flatten()
                            .and_then(|b| rmp_serde::from_slice(&b).ok())
                    });
                Ok(AnalyticsMessageRow {
                    id: row.get_typed(0)?,
                    role: row.get_typed(1)?,
                    content: row.get_typed(2)?,
                    extra_json,
                    created_at: row.get_typed(5)?,
                    conversation_id: row.get_typed(6)?,
                })
            };
            let rows: Vec<AnalyticsMessageRow> = if first_page {
                self.conn.query_map_collect(
                    "SELECT m.id, m.role, m.content, m.extra_json, m.extra_bin,
                            m.created_at, m.conversation_id
                     FROM messages m
                     ORDER BY m.id
                     LIMIT ?1",
                    fparams![fetch_limit],
                    decode_row,
                )?
            } else {
                self.conn.query_map_collect(
                    "SELECT m.id, m.role, m.content, m.extra_json, m.extra_bin,
                            m.created_at, m.conversation_id
                     FROM messages m
                     WHERE m.id > ?1
                     ORDER BY m.id
                     LIMIT ?2",
                    fparams![last_id, fetch_limit],
                    decode_row,
                )?
            };

            let Some(last_row) = rows.last() else {
                break;
            };
            let fetched = rows.len();
            // Advance by the last *fetched* id, not the last kept one, so a
            // chunk consisting entirely of out-of-window rows can never
            // stall the cursor.
            last_id = last_row.id;
            first_page = false;

            let mut entries = Vec::with_capacity(fetched);
            let mut rollup_agg = AnalyticsRollupAggregator::new();

            for row in &rows {
                if let Some(control) = control {
                    control()?;
                }
                let Some(dim) = conversation_dims.get(&row.conversation_id) else {
                    // The former inner JOIN dropped messages whose
                    // conversation row is gone; keep that behaviour.
                    continue;
                };
                let ts = row.created_at.or(dim.started_at).unwrap_or(0);
                if ts < cutoff_ms {
                    continue;
                }
                let msg_id = &row.id;
                let role = &row.role;
                let content = &row.content;
                let extra_json = &row.extra_json;
                let workspace_id = &dim.workspace_id;
                let source_id = &dim.source_id;
                let agent_slug = &dim.agent_slug;
                let day_id = Self::day_id_from_millis(ts);
                let hour_id = Self::hour_id_from_millis(ts);
                let content_chars = content.len() as i64;
                let content_tokens_est = content_chars / 4;
                let extra = extra_json
                    .as_ref()
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                let mut usage =
                    crate::connectors::extract_tokens_for_agent(agent_slug, &extra, content, role);
                if agent_slug == "codebuff" {
                    usage.tool_call_count = codebuff_tool_count(&extra);
                    usage.has_tool_calls = usage.tool_call_count > 0;
                }
                if agent_slug == "claude_code" {
                    claude_usage.apply(row.conversation_id, &extra, &mut usage);
                }
                let model_info = usage
                    .model_name
                    .as_deref()
                    .map(crate::connectors::normalize_model);
                let model_family = model_info
                    .as_ref()
                    .map(|i| i.family.clone())
                    .unwrap_or_else(|| "unknown".into());
                let model_tier = model_info
                    .as_ref()
                    .map(|i| i.tier.clone())
                    .unwrap_or_else(|| "unknown".into());
                let provider = usage
                    .provider
                    .clone()
                    .or_else(|| model_info.as_ref().map(|i| i.provider.clone()))
                    .unwrap_or_else(|| "unknown".into());

                let entry = MessageMetricsEntry {
                    message_id: *msg_id,
                    created_at_ms: ts,
                    hour_id,
                    day_id,
                    agent_slug: agent_slug.clone(),
                    workspace_id: workspace_id.unwrap_or(0),
                    source_id: source_id.clone(),
                    role: role.clone(),
                    content_chars,
                    content_tokens_est,
                    model_name: usage.model_name.clone(),
                    model_family,
                    model_tier,
                    provider,
                    api_input_tokens: usage.input_tokens,
                    api_output_tokens: usage.output_tokens,
                    api_cache_read_tokens: usage.cache_read_tokens,
                    api_cache_creation_tokens: usage.cache_creation_tokens,
                    api_thinking_tokens: usage.thinking_tokens,
                    api_service_tier: usage.service_tier,
                    api_data_source: usage.data_source.as_str().to_string(),
                    tool_call_count: usage.tool_call_count as i64,
                    has_tool_calls: usage.has_tool_calls,
                    has_plan: has_plan_for_role(role, content),
                };
                rollup_agg.record(&entry);
                entries.push(entry);
            }
            // Extraction has copied only compact metrics into `entries`.
            // Do not retain raw content/JSON beside the writer's dirty pages.
            drop(rows);

            kept += entries.len() as i64;
            let next_processed = processed.saturating_add(fetched as i64);
            if !entries.is_empty() || resume_context.is_some() {
                if let Some(control) = control {
                    control()?;
                }
                // GH #424: one short write transaction per chunk.
                let mut tx = self
                    .conn
                    .transaction_with_behavior(TransactionBehavior::Immediate)?;
                if !entries.is_empty() {
                    for entry in &entries {
                        if let Some(control) = control {
                            control()?;
                        }
                        total_inserted += franken_insert_message_metrics_batched_in_tx(
                            &tx,
                            std::slice::from_ref(entry),
                        )?;
                    }
                    if let Some(control) = control {
                        control()?;
                    }
                    let (hourly, daily, models_daily) =
                        franken_flush_analytics_rollups_in_tx(&tx, &rollup_agg)?;
                    usage_hourly_rows += hourly;
                    usage_daily_rows += daily;
                    usage_models_daily_rows += models_daily;
                }
                if resume_context.is_some() {
                    // Commit the cursor in the same transaction as the rows
                    // and rollup deltas. A crash can therefore replay neither
                    // too little nor a double-counted chunk.
                    tx.execute_compat(
                        "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
                        fparams![
                            LEGACY_OMP_ANALYTICS_CURSOR_LAST_ID_META_KEY,
                            last_id.to_string()
                        ],
                    )?;
                    tx.execute_compat(
                        "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
                        fparams![
                            LEGACY_OMP_ANALYTICS_CURSOR_PROCESSED_META_KEY,
                            next_processed.to_string()
                        ],
                    )?;
                }
                if let Some(control) = control {
                    control()?;
                }
                tx.commit()?;
            }
            processed = next_processed;
            if let Some(control) = control {
                control()?;
            }

            // Per-chunk progress at INFO so a multi-hour rebuild is
            // distinguishable from a hang (GH #412).
            let elapsed_secs = start.elapsed().as_secs_f64();
            let rate = if elapsed_secs > 0.0 {
                processed.saturating_sub(starting_processed) as f64 / elapsed_secs
            } else {
                0.0
            };
            tracing::info!(
                target: "cass::analytics",
                processed,
                kept,
                total = total_messages,
                last_id,
                chunk = fetched,
                inserted = total_inserted,
                elapsed_secs = format!("{elapsed_secs:.1}"),
                msgs_per_sec = format!("{rate:.0}"),
                "analytics_rebuild_progress"
            );
            if let Some(progress) = progress {
                progress(processed, total_messages);
            }
            if let Some(heartbeat) = heartbeat {
                heartbeat();
            }

            if (fetched as i64) < fetch_limit {
                break;
            }
        }

        let elapsed = start.elapsed();
        let elapsed_ms = elapsed.as_millis() as u64;
        let msgs_per_sec = if elapsed_ms > 0 {
            (total_inserted as f64) / (elapsed_ms as f64 / 1000.0)
        } else {
            0.0
        };

        tracing::info!(
            target: "cass::analytics",
            message_metrics_rows = total_inserted,
            usage_hourly_rows,
            usage_daily_rows,
            usage_models_daily_rows,
            elapsed_ms,
            messages_per_sec = format!("{:.0}", msgs_per_sec),
            "analytics_rebuild_complete"
        );

        Ok(AnalyticsRebuildResult {
            message_metrics_rows: total_inserted,
            usage_hourly_rows,
            usage_daily_rows,
            usage_models_daily_rows,
            elapsed_ms,
            messages_per_sec: msgs_per_sec,
        })
    }
}

/// Default keyset chunk for `rebuild_analytics_since`: rows fetched per
/// `SELECT` and committed per transaction (GH #424).
const ANALYTICS_REBUILD_CHUNK_SIZE: usize = 10_000;
/// Bound legacy repair's transaction and simultaneously materialized payloads.
pub(super) const LEGACY_OMP_ANALYTICS_CHUNK_SIZE: usize = 128;
const LEGACY_OMP_ANALYTICS_PAGE_BYTES: i64 = 8 * 1024 * 1024;

/// Per-conversation dimensions resolved once per analytics rebuild instead
/// of being JOINed into every keyset chunk (GH #424).
struct AnalyticsConversationDim {
    started_at: Option<i64>,
    source_id: String,
    workspace_id: Option<i64>,
    /// Resolved agent slug, `'unknown'` when the conversation has no agent
    /// or its agent row is missing.
    agent_slug: String,
}

/// One `messages` row of an analytics-rebuild keyset chunk (GH #424).
struct AnalyticsMessageRow {
    id: i64,
    role: String,
    content: String,
    extra_json: Option<serde_json::Value>,
    created_at: Option<i64>,
    conversation_id: i64,
}

pub(super) fn rebuild_batch_size_env(var: &str, default: usize) -> usize {
    dotenvy::var(var)
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

/// Returns true when the error chain represents a real `FrankenError::OutOfMemory`
/// (typed variant) or a bare "out of memory" / "not enough memory" message.
///
/// We *deliberately* do not do substring matching on the rendered chain: SQLite's
/// `FrankenError::OutOfMemory` renders as the literal "out of memory" and is also emitted
/// for non-process-OOM internal conditions.
/// Contextual messages like "connector parse failed: not enough memory in record" must not
/// be promoted into the OOM-bisect/quarantine path. See `retryable_franken_anyhow` above
/// for the same downcast idiom.
pub(super) fn is_out_of_memory_error<E: OutOfMemoryProbe + ?Sized>(err: &E) -> bool {
    err.is_out_of_memory()
}

pub(super) trait OutOfMemoryProbe {
    fn is_out_of_memory(&self) -> bool;
}

impl OutOfMemoryProbe for anyhow::Error {
    fn is_out_of_memory(&self) -> bool {
        self.chain().any(|cause| {
            if cause
                .downcast_ref::<crate::franken_sync::FrankenError>()
                .is_some_and(|err| matches!(err, crate::franken_sync::FrankenError::OutOfMemory))
            {
                return true;
            }
            is_exact_out_of_memory_message(&cause.to_string())
        })
    }
}

impl OutOfMemoryProbe for crate::franken_sync::FrankenError {
    fn is_out_of_memory(&self) -> bool {
        matches!(self, crate::franken_sync::FrankenError::OutOfMemory)
    }
}

fn is_exact_out_of_memory_message(message: &str) -> bool {
    matches!(
        message.trim().to_ascii_lowercase().as_str(),
        "out of memory" | "not enough memory"
    )
}

// Second SqliteStorage impl block removed: SqliteStorage is now a type alias for FrankenStorage.
// All methods (insert_conversation_tree, list_agents, list_conversations, etc.) are
// available through FrankenStorage.

/// Daily count data for histogram display.
#[derive(Debug, Clone)]
pub struct DailyCount {
    pub day_id: i64,
    pub sessions: i64,
    pub messages: i64,
    pub chars: i64,
}

/// Result of an analytics rebuild operation.
#[derive(Debug, Clone)]
pub struct AnalyticsRebuildResult {
    pub message_metrics_rows: usize,
    pub usage_hourly_rows: usize,
    pub usage_daily_rows: usize,
    pub usage_models_daily_rows: usize,
    pub elapsed_ms: u64,
    pub messages_per_sec: f64,
}

/// Result of rebuilding daily stats.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DailyStatsRebuildResult {
    pub rows_created: i64,
    pub total_sessions: i64,
}
