use super::aggregators_and_pricing::StatsAggregator;
use super::analytics_rebuild::{
    DailyCount, DailyStatsRebuildResult, is_out_of_memory_error, rebuild_batch_size_env,
};
use super::backup_and_integrity::{
    DAILY_STATS_HEALTH_GENERATION, DAILY_STATS_HEALTH_GENERATION_META_KEY,
};
use super::conversation_tail_state::MessageForEmbedding;
use super::lexical_rebuild::EmbeddingJobRow;
use super::omp_and_semantic_meta::SemanticIdentityTier;
use super::storage_core::FrankenStorage;
use super::tx_analytics_rollups::{
    DailyStatsBatchTarget, franken_update_daily_stats_batched_in_tx_for_target,
};
use crate::franken_sync::{
    Row as FrankenRow, SqliteValue, TransactionBehavior,
    compat::{
        ConnectionExt as FrankenConnectionExt, OptionalExtension as FrankenOptionalExtension,
        ParamValue, RowExt as FrankenRowExt, TransactionExt as FrankenTransactionExt,
        params_from_iter,
    },
};
use anyhow::{Context, Result};

impl FrankenStorage {
    /// Fetch all messages for embedding generation.
    pub fn fetch_messages_for_embedding(&self) -> Result<Vec<MessageForEmbedding>> {
        // COALESCE(c.agent_id, 0) so legacy V1 conversations with NULL
        // agent_id don't cause a runtime row-decode failure (agent_id in
        // MessageForEmbedding is i64).  saturating_u32_from_i64 downstream
        // turns 0 into the "unknown agent" sentinel for doc-id hashing.
        self.conn
            .query_map_collect(
                "SELECT m.id, m.created_at, COALESCE(c.agent_id, 0), c.workspace_id, c.source_id, m.role, m.content
                 FROM messages m
                 JOIN conversations c ON m.conversation_id = c.id
                 ORDER BY m.id",
                fparams![],
                |row| {
                    let source_id: String = row.get_typed::<Option<String>>(4)?
                        .unwrap_or_else(|| "local".to_string());
                    Ok(MessageForEmbedding {
                        message_id: row.get_typed(0)?,
                        created_at: row.get_typed(1)?,
                        agent_id: row.get_typed(2)?,
                        workspace_id: row.get_typed(3)?,
                        source_id_hash: crc32fast::hash(source_id.as_bytes()),
                        role: row.get_typed(5)?,
                        content: row.get_typed(6)?,
                    })
                },
            )
            .with_context(|| "fetching messages for embedding")
    }

    /// Get the watermark for incremental semantic embedding.
    pub fn get_last_embedded_message_id(&self) -> Result<Option<i64>> {
        let result: Result<String, _> = self.conn.query_row_map(
            "SELECT value FROM meta WHERE key = 'last_embedded_message_id'",
            fparams![],
            |row| row.get_typed(0),
        );
        match result.optional() {
            Ok(Some(s)) => Ok(s.parse().ok()),
            Ok(None) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Highest message rowid currently in the canonical archive, or `None`
    /// when the messages table is empty. Compared against the
    /// `last_embedded_message_id` watermark to decide whether a one-shot
    /// `index --semantic` run has anything left to embed (issue #394).
    pub fn max_message_id(&self) -> Result<Option<i64>> {
        let max: i64 = self
            .conn
            .query_row_map(
                "SELECT COALESCE(MAX(id), 0) FROM messages",
                fparams![],
                |row| row.get_typed(0),
            )
            .with_context(|| "reading max message id for semantic watermark comparison")?;
        Ok((max > 0).then_some(max))
    }

    /// Set the watermark for incremental semantic embedding.
    pub fn set_last_embedded_message_id(&self, id: i64) -> Result<()> {
        self.conn.execute_compat(
            "INSERT OR REPLACE INTO meta(key, value) VALUES('last_embedded_message_id', ?1)",
            fparams![id.to_string()],
        )?;
        Ok(())
    }

    /// Whether semantic artifacts must be rebuilt because canonical filter
    /// identity changed without advancing message rowids.
    pub(crate) fn semantic_identity_rebuild_required(
        &self,
        tier: SemanticIdentityTier,
    ) -> Result<bool> {
        Ok(self.semantic_identity_rebuild_generation(tier)?.is_some())
    }

    /// Durable generation that namespaces resumable semantic checkpoints.
    /// `None` means the selected tier already describes current canonical
    /// filter identity.
    pub(crate) fn semantic_identity_rebuild_generation(
        &self,
        tier: SemanticIdentityTier,
    ) -> Result<Option<String>> {
        let state: Option<String> = self
            .conn
            .query_row_map(
                "SELECT value FROM meta WHERE key = ?1",
                fparams![tier.meta_key()],
                |row| row.get_typed(0),
            )
            .optional()?;
        Ok(state.filter(|value| value != "complete"))
    }

    /// Mark semantic filter identity stale until a full artifact publish has
    /// durably completed.
    #[cfg(test)]
    pub(crate) fn mark_semantic_identity_rebuild_required(
        &self,
        tier: SemanticIdentityTier,
    ) -> Result<()> {
        self.conn.execute_compat(
            "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, 'required:test')",
            fparams![tier.meta_key()],
        )?;
        Ok(())
    }

    /// Acknowledge that the live semantic artifact was republished from the
    /// current canonical agent/source identities.
    pub(crate) fn complete_semantic_identity_rebuild(
        &self,
        tier: SemanticIdentityTier,
    ) -> Result<()> {
        self.conn.execute_compat(
            "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, 'complete')",
            fparams![tier.meta_key()],
        )?;
        Ok(())
    }

    /// Get embedding jobs for a database path.
    pub fn get_embedding_jobs(&self, db_path: &str) -> Result<Vec<EmbeddingJobRow>> {
        self.conn
            .query_map_collect(
                "SELECT id, db_path, model_id, status, total_docs, completed_docs, error_message, created_at, started_at, completed_at
                 FROM embedding_jobs WHERE db_path = ?1 ORDER BY id DESC",
                fparams![db_path],
                |row| {
                    Ok(EmbeddingJobRow {
                        id: row.get_typed(0)?,
                        db_path: row.get_typed(1)?,
                        model_id: row.get_typed(2)?,
                        status: row.get_typed(3)?,
                        total_docs: row.get_typed(4)?,
                        completed_docs: row.get_typed(5)?,
                        error_message: row.get_typed(6)?,
                        created_at: row.get_typed(7)?,
                        started_at: row.get_typed(8)?,
                        completed_at: row.get_typed(9)?,
                    })
                },
            )
            .with_context(|| format!("fetching embedding jobs for {db_path}"))
    }

    /// Create or update an embedding job.
    pub fn upsert_embedding_job(
        &self,
        db_path: &str,
        model_id: &str,
        total_docs: i64,
    ) -> Result<i64> {
        let updated = self.conn.execute_compat(
            "UPDATE embedding_jobs
             SET total_docs = ?3
             WHERE db_path = ?1 AND model_id = ?2 AND status IN ('pending', 'running')",
            fparams![db_path, model_id, total_docs],
        )?;
        if updated == 0 {
            let insert_result = self.conn.execute_compat(
                "INSERT INTO embedding_jobs(db_path, model_id, total_docs) VALUES(?1,?2,?3)",
                fparams![db_path, model_id, total_docs],
            );
            if let Err(err) = insert_result {
                if !matches!(
                    err,
                    crate::franken_sync::FrankenError::UniqueViolation { .. }
                ) {
                    return Err(err.into());
                }
                self.conn.execute_compat(
                    "UPDATE embedding_jobs
                     SET total_docs = ?3
                     WHERE db_path = ?1 AND model_id = ?2 AND status IN ('pending', 'running')",
                    fparams![db_path, model_id, total_docs],
                )?;
            }
        }
        self.conn
            .query_row_map(
                "SELECT id FROM embedding_jobs
                 WHERE db_path = ?1 AND model_id = ?2 AND status IN ('pending', 'running')
                 ORDER BY id DESC
                 LIMIT 1",
                fparams![db_path, model_id],
                |row| row.get_typed(0),
            )
            .with_context(|| "resolving embedding job id after upsert")
    }

    /// Mark an embedding job as started.
    pub fn start_embedding_job(&self, job_id: i64) -> Result<()> {
        self.conn.execute_compat(
            "UPDATE embedding_jobs SET status = 'running', started_at = datetime('now') WHERE id = ?1",
            fparams![job_id],
        )?;
        Ok(())
    }

    /// Mark an embedding job as completed.
    pub fn complete_embedding_job(&self, job_id: i64) -> Result<()> {
        self.conn.execute_compat(
            "UPDATE embedding_jobs SET status = 'completed', completed_at = datetime('now') WHERE id = ?1",
            fparams![job_id],
        )?;
        Ok(())
    }

    /// Mark an embedding job as failed.
    pub fn fail_embedding_job(&self, job_id: i64, error: &str) -> Result<()> {
        self.conn.execute_compat(
            "UPDATE embedding_jobs SET status = 'failed', error_message = ?2, completed_at = datetime('now') WHERE id = ?1",
            fparams![job_id, error],
        )?;
        Ok(())
    }

    /// Cancel embedding jobs for a database path.
    pub fn cancel_embedding_jobs(&self, db_path: &str, model_id: Option<&str>) -> Result<usize> {
        if let Some(mid) = model_id {
            Ok(self.conn.execute_compat(
                "UPDATE embedding_jobs SET status = 'cancelled' WHERE db_path = ?1 AND model_id = ?2 AND status IN ('pending', 'running')",
                fparams![db_path, mid],
            )?)
        } else {
            Ok(self.conn.execute_compat(
                "UPDATE embedding_jobs SET status = 'cancelled' WHERE db_path = ?1 AND status IN ('pending', 'running')",
                fparams![db_path],
            )?)
        }
    }

    /// Update embedding job progress.
    pub fn update_job_progress(&self, job_id: i64, completed_docs: i64) -> Result<()> {
        self.conn.execute_compat(
            "UPDATE embedding_jobs SET completed_docs = ?2 WHERE id = ?1",
            fparams![job_id, completed_docs],
        )?;
        Ok(())
    }

    // =====================================================================
    // Analytics query methods
    // =====================================================================

    /// Get session count for a date range using materialized stats.
    /// Returns (count, is_from_cache) where is_from_cache is true if from daily_stats.
    ///
    /// Falls back to COUNT(*) query when daily_stats table is empty or stale.
    pub fn count_sessions_in_range(
        &self,
        start_ts_ms: Option<i64>,
        end_ts_ms: Option<i64>,
        agent_slug: Option<&str>,
        source_id: Option<&str>,
    ) -> Result<(i64, bool)> {
        let agent = agent_slug.unwrap_or("all");
        let source = source_id.unwrap_or("all");

        // Check if we have materialized stats
        let stats_count: i64 = self
            .conn
            .query_row_map("SELECT COUNT(*) FROM daily_stats", fparams![], |row| {
                row.get_typed(0)
            })
            .unwrap_or(0);

        if stats_count == 0 {
            return self.count_sessions_direct(start_ts_ms, end_ts_ms, agent_slug, source_id);
        }

        // Use materialized stats
        let start_day = start_ts_ms.map(Self::day_id_from_millis);
        let end_day = end_ts_ms.map(Self::day_id_from_millis);

        let count: i64 = match (start_day, end_day) {
            (Some(start), Some(end)) => self.conn.query_row_map(
                "SELECT COALESCE(SUM(session_count), 0) FROM daily_stats
                 WHERE day_id BETWEEN ?1 AND ?2 AND agent_slug = ?3 AND source_id = ?4",
                fparams![start, end, agent, source],
                |row| row.get_typed(0),
            )?,
            (Some(start), None) => self.conn.query_row_map(
                "SELECT COALESCE(SUM(session_count), 0) FROM daily_stats
                 WHERE day_id >= ?1 AND agent_slug = ?2 AND source_id = ?3",
                fparams![start, agent, source],
                |row| row.get_typed(0),
            )?,
            (None, Some(end)) => self.conn.query_row_map(
                "SELECT COALESCE(SUM(session_count), 0) FROM daily_stats
                 WHERE day_id <= ?1 AND agent_slug = ?2 AND source_id = ?3",
                fparams![end, agent, source],
                |row| row.get_typed(0),
            )?,
            (None, None) => self.conn.query_row_map(
                "SELECT COALESCE(SUM(session_count), 0) FROM daily_stats
                 WHERE agent_slug = ?1 AND source_id = ?2",
                fparams![agent, source],
                |row| row.get_typed(0),
            )?,
        };

        Ok((count, true))
    }

    /// Direct COUNT(*) query as fallback when daily_stats is empty.
    fn count_sessions_direct(
        &self,
        start_ts_ms: Option<i64>,
        end_ts_ms: Option<i64>,
        agent_slug: Option<&str>,
        source_id: Option<&str>,
    ) -> Result<(i64, bool)> {
        // Build dynamic SQL with positional params.  Single-table scan of
        // conversations; filter on agent slug via an EXISTS subquery only
        // when that filter is actually requested.  This avoids the unneeded
        // 2-table JOIN (which also silently dropped legacy conversations
        // with NULL agent_id) and keeps the query to a single-table scan.
        let mut sql = "SELECT COUNT(*) FROM conversations c WHERE 1=1".to_string();
        let mut param_values: Vec<ParamValue> = Vec::new();
        let mut idx = 1;

        if let Some(start) = start_ts_ms {
            sql.push_str(&format!(" AND c.started_at >= ?{idx}"));
            param_values.push(ParamValue::from(start));
            idx += 1;
        }
        if let Some(end) = end_ts_ms {
            sql.push_str(&format!(" AND c.started_at <= ?{idx}"));
            param_values.push(ParamValue::from(end));
            idx += 1;
        }
        if let Some(agent) = agent_slug
            && agent != "all"
        {
            sql.push_str(&format!(
                " AND EXISTS (SELECT 1 FROM agents a WHERE a.id = c.agent_id AND a.slug = ?{idx})"
            ));
            param_values.push(ParamValue::from(agent));
            idx += 1;
        }
        if let Some(source) = source_id
            && source != "all"
        {
            sql.push_str(&format!(" AND c.source_id = ?{idx}"));
            param_values.push(ParamValue::from(source));
            let _ = idx; // suppress unused warning
        }

        let count: i64 = self
            .conn
            .query_row_map(&sql, &param_values, |row| row.get_typed(0))?;
        Ok((count, false))
    }

    /// Get daily histogram data for a date range.
    pub fn get_daily_histogram(
        &self,
        start_ts_ms: i64,
        end_ts_ms: i64,
        agent_slug: Option<&str>,
        source_id: Option<&str>,
    ) -> Result<Vec<DailyCount>> {
        let start_day = Self::day_id_from_millis(start_ts_ms);
        let end_day = Self::day_id_from_millis(end_ts_ms);
        let agent = agent_slug.unwrap_or("all");
        let source = source_id.unwrap_or("all");

        let rows = self.conn.query_map_collect(
            "SELECT day_id, session_count, message_count, total_chars
             FROM daily_stats
             WHERE day_id BETWEEN ?1 AND ?2 AND agent_slug = ?3 AND source_id = ?4
             ORDER BY day_id",
            fparams![start_day, end_day, agent, source],
            |row| {
                Ok(DailyCount {
                    day_id: row.get_typed(0)?,
                    sessions: row.get_typed(1)?,
                    messages: row.get_typed(2)?,
                    chars: row.get_typed(3)?,
                })
            },
        )?;

        Ok(rows)
    }

    /// Check health of daily stats table.
    pub fn daily_stats_health(&self) -> Result<DailyStatsHealth> {
        let row_count: i64 =
            self.conn
                .query_row_map("SELECT COUNT(*) FROM daily_stats", fparams![], |row| {
                    row.get_typed(0)
                })?;

        let oldest_update: Option<i64> = self.conn.query_row_map(
            "SELECT MIN(last_updated) FROM daily_stats",
            fparams![],
            |row| row.get_typed(0),
        )?;

        let conversation_count: i64 =
            self.conn
                .query_row_map("SELECT COUNT(*) FROM conversations", fparams![], |row| {
                    row.get_typed(0)
                })?;

        let materialized_total: i64 = self.conn.query_row_map(
            "SELECT COALESCE(SUM(session_count), 0) FROM daily_stats
                 WHERE agent_slug = 'all' AND source_id = 'all'",
            fparams![],
            |row| row.get_typed(0),
        )?;

        Ok(DailyStatsHealth {
            populated: row_count > 0,
            row_count,
            oldest_update_ms: oldest_update,
            conversation_count,
            materialized_total,
            drift: (conversation_count - materialized_total).abs(),
        })
    }
}

impl FrankenStorage {
    /// Rebuild all daily stats from scratch.
    pub fn rebuild_daily_stats(&self) -> Result<DailyStatsRebuildResult> {
        self.rebuild_daily_stats_with_progress(None, None)
    }

    pub(super) fn rebuild_daily_stats_with_progress(
        &self,
        heartbeat: Option<&dyn Fn()>,
        control: Option<&dyn Fn() -> Result<()>>,
    ) -> Result<DailyStatsRebuildResult> {
        if let Some(control) = control {
            control()?;
        }
        const DAILY_STATS_REBUILD_CONVERSATION_BATCH_SIZE: usize = 1_000;
        const DAILY_STATS_REBUILD_MESSAGE_BATCH_SIZE: usize = 10_000;

        let mut conversation_batch_size = rebuild_batch_size_env(
            "CASS_DAILY_STATS_REBUILD_CONVERSATION_BATCH_SIZE",
            DAILY_STATS_REBUILD_CONVERSATION_BATCH_SIZE,
        );
        let mut message_batch_size = rebuild_batch_size_env(
            "CASS_DAILY_STATS_REBUILD_MESSAGE_BATCH_SIZE",
            DAILY_STATS_REBUILD_MESSAGE_BATCH_SIZE,
        );

        if let Some(heartbeat) = heartbeat {
            heartbeat();
        }

        // Build into a connection-local staging table and commit every bounded
        // aggregate batch. Holding one write transaction for the entire archive
        // makes the engine retain transaction state proportional to the full
        // message corpus, defeating the bounded query batches above. The live
        // materialization remains untouched until the final atomic publish, so
        // an interrupted rebuild leaves the last known-good stats available.
        if let Some(control) = control {
            control()?;
        }
        self.conn
            .execute(
                "CREATE TEMP TABLE IF NOT EXISTS daily_stats_rebuild_stage (
                day_id INTEGER NOT NULL,
                agent_slug TEXT NOT NULL,
                source_id TEXT NOT NULL,
                session_count INTEGER NOT NULL DEFAULT 0,
                message_count INTEGER NOT NULL DEFAULT 0,
                total_chars INTEGER NOT NULL DEFAULT 0,
                last_updated INTEGER NOT NULL,
                PRIMARY KEY (day_id, agent_slug, source_id)
            )",
            )
            .with_context(|| "daily_stats phase=create_stage")?;
        self.conn
            .execute("DELETE FROM daily_stats_rebuild_stage")
            .with_context(|| "daily_stats phase=clear_stage")?;

        let mut last_conversation_id = 0_i64;
        let mut conversation_batch_count = 0_usize;
        let mut conversations_processed = 0_usize;
        let mut messages_processed = 0_usize;
        let mut message_batch_count = 0_usize;
        let mut raw_entries_flushed = 0_usize;
        let mut expanded_entries_flushed = 0_usize;
        // GH #424 (third finding): the former `JOIN message_metrics` page scan
        // was slow regardless of conversation size, and replacing the JOIN with
        // one point lookup per message still executed hundreds of thousands of
        // avoidable statements. Canonical
        // message content is the source of truth. Rust's `String::len()` and
        // SQLite's BLOB length both count UTF-8 bytes, so the bounded single-
        // table keyset preserves the existing `total_chars` contract without
        // trusting a potentially stale derived `message_metrics` row.
        let has_conv_idx = self
            .conn
            .query_map_collect("PRAGMA index_list(messages)", fparams![], |row| {
                row.get_typed::<String>(1)
            })
            .unwrap_or_default()
            .iter()
            .any(|name| name == "idx_messages_conv_idx");
        let index_hint = if has_conv_idx {
            " INDEXED BY idx_messages_conv_idx"
        } else {
            ""
        };
        let first_message_scan_sql = format!(
            "SELECT m.idx, COALESCE(LENGTH(CAST(m.content AS BLOB)), 0)
             FROM messages m{index_hint}
             WHERE m.conversation_id = ?1
             ORDER BY m.idx
             LIMIT ?2"
        );
        let message_scan_sql = format!(
            "SELECT m.idx, COALESCE(LENGTH(CAST(m.content AS BLOB)), 0)
             FROM messages m{index_hint}
             WHERE m.conversation_id = ?1
               AND m.idx > ?2
             ORDER BY m.idx
             LIMIT ?3"
        );
        // #329: prepare this once and stream each bounded page through the row
        // handler. Re-preparing and materializing one Vec per conversation/page
        // retained engine execution state until the 967k-message field corpus
        // reached ~10.4 GiB RSS and failed with an internal OOM. The explicit
        // uniqueness-index hint pins the `(conversation_id, idx)` keyset walk;
        // neither memory nor planner work scales with unrelated messages.
        let first_message_scan_statement = self
            .conn
            .prepare(&first_message_scan_sql)
            .with_context(|| "preparing daily_stats initial bounded message scan")?;
        let message_scan_statement = self
            .conn
            .prepare(&message_scan_sql)
            .with_context(|| "preparing daily_stats bounded message scan")?;

        loop {
            // Avoid a 2-table JOIN with LIMIT that triggers full-table
            // materialization (which the OOM retry below defends against).
            // Inline agent slug via correlated subquery and degrade NULL
            // agent_id to 'unknown' for consistency with the lexical/FTS
            // rebuild paths.
            if let Some(control) = control {
                control()?;
            }
            let conversation_rows = match self.conn.query_with_params(
                "SELECT c.id, c.started_at,
                        COALESCE((SELECT a.slug FROM agents a WHERE a.id = c.agent_id), 'unknown'),
                        c.source_id
                 FROM conversations c
                 WHERE c.id > ?1
                 ORDER BY c.id
                 LIMIT ?2",
                &params_from_iter([
                    ParamValue::from(last_conversation_id),
                    ParamValue::from(conversation_batch_size as i64),
                ]),
            ) {
                Ok(rows) => rows,
                Err(err) if is_out_of_memory_error(&err) && conversation_batch_size > 1 => {
                    let previous_batch_size = conversation_batch_size;
                    conversation_batch_size = (conversation_batch_size / 2).max(1);
                    tracing::warn!(
                        previous_batch_size,
                        conversation_batch_size,
                        last_conversation_id,
                        "daily_stats conversation scan ran out of memory; retrying with smaller batch"
                    );
                    continue;
                }
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!(
                            "daily_stats phase=conversation_scan last_conversation_id={last_conversation_id} batch_size={conversation_batch_size}"
                        )
                    });
                }
            };
            if conversation_rows.is_empty() {
                break;
            }

            let mut aggregate = StatsAggregator::new();
            let mut conversation_batch_meta: Vec<(i64, i64, String, String)> =
                Vec::with_capacity(conversation_rows.len());
            for row in &conversation_rows {
                let conversation_id: i64 = row.get_typed(0)?;
                let started_at: Option<i64> = row.get_typed(1)?;
                let agent_slug: String = row.get_typed(2)?;
                let source_id: String = row.get_typed(3)?;
                last_conversation_id = conversation_id;
                let day_id = started_at.map(Self::day_id_from_millis).unwrap_or(0);
                aggregate.record_delta(&agent_slug, &source_id, day_id, 1, 0, 0);
                conversation_batch_meta.push((conversation_id, day_id, agent_slug, source_id));
                conversations_processed += 1;
            }

            conversation_batch_count += 1;
            raw_entries_flushed += aggregate.raw_entry_count();
            let entries = aggregate.expand();
            expanded_entries_flushed += entries.len();
            if !entries.is_empty() {
                if let Some(control) = control {
                    control()?;
                }
                let mut batch_tx = self
                    .conn
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .with_context(|| {
                        format!(
                            "daily_stats phase=conversation_stage_begin last_conversation_id={last_conversation_id}"
                        )
                    })?;
                franken_update_daily_stats_batched_in_tx_for_target(
                    &batch_tx,
                    &entries,
                    DailyStatsBatchTarget::RebuildStage,
                )
                .with_context(|| {
                    format!(
                        "daily_stats phase=conversation_stage_upsert last_conversation_id={last_conversation_id}"
                    )
                })?;
                if let Some(control) = control {
                    control()?;
                }
                batch_tx.commit().with_context(|| {
                    format!(
                        "daily_stats phase=conversation_stage_commit last_conversation_id={last_conversation_id}"
                    )
                })?;
            }
            if let Some(heartbeat) = heartbeat {
                heartbeat();
            }
            if conversation_batch_count.is_multiple_of(25) {
                tracing::info!(
                    target: "cass::perf::daily_stats",
                    conversations_processed,
                    batches = conversation_batch_count,
                    batch_size = conversation_batch_size,
                    last_conversation_id,
                    "daily_stats rebuild conversation scan progress"
                );
            }
            if conversation_batch_meta.is_empty() {
                continue;
            }

            for (conversation_id, day_id, agent_slug, source_id) in conversation_batch_meta {
                // `messages.idx` is any signed INTEGER; it has no nonnegative
                // schema constraint. The first page therefore omits a lower
                // bound instead of using a numeric sentinel that could exclude
                // a legitimate row (including i64::MIN).
                let mut cursor_message_idx = i64::MIN;
                let mut first_message_page = true;
                loop {
                    let page_start_message_idx = cursor_message_idx;
                    let mut next_message_idx = cursor_message_idx;
                    let mut page_rows = 0_usize;
                    let mut aggregate = StatsAggregator::new();
                    let mut accumulate_row = |row: &FrankenRow| {
                        let message_idx: i64 = row.get_typed(0)?;
                        let content_len: i64 = row.get_typed(1)?;
                        next_message_idx = message_idx;
                        page_rows = page_rows.saturating_add(1);
                        aggregate.record_delta(&agent_slug, &source_id, day_id, 0, 1, content_len);
                        Ok(())
                    };
                    if let Some(control) = control {
                        control()?;
                    }
                    let scan_result = if first_message_page {
                        let scan_params = [
                            SqliteValue::from(conversation_id),
                            SqliteValue::from(message_batch_size as i64),
                        ];
                        first_message_scan_statement
                            .query_with_params_for_each(&scan_params, &mut accumulate_row)
                    } else {
                        let scan_params = [
                            SqliteValue::from(conversation_id),
                            SqliteValue::from(page_start_message_idx),
                            SqliteValue::from(message_batch_size as i64),
                        ];
                        message_scan_statement
                            .query_with_params_for_each(&scan_params, &mut accumulate_row)
                    };
                    match scan_result {
                        Ok(()) => {
                            cursor_message_idx = next_message_idx;
                            first_message_page = false;
                        }
                        Err(err) if is_out_of_memory_error(&err) && message_batch_size > 1 => {
                            let previous_batch_size = message_batch_size;
                            message_batch_size = (message_batch_size / 2).max(1);
                            tracing::warn!(
                                previous_batch_size,
                                message_batch_size,
                                conversation_id,
                                cursor_message_idx = page_start_message_idx,
                                phase = "message_scan",
                                "daily_stats message scan ran out of memory; retrying with smaller batch"
                            );
                            continue;
                        }
                        Err(err) => {
                            return Err(err).with_context(|| {
                                format!(
                                    "daily_stats phase=message_scan conversation_id={conversation_id} cursor_message_idx={page_start_message_idx} batch_size={message_batch_size}"
                                )
                            });
                        }
                    }
                    if page_rows == 0 {
                        break;
                    }

                    messages_processed = messages_processed.saturating_add(page_rows);

                    message_batch_count += 1;
                    raw_entries_flushed += aggregate.raw_entry_count();
                    let entries = aggregate.expand();
                    expanded_entries_flushed += entries.len();
                    if !entries.is_empty() {
                        if let Some(control) = control {
                            control()?;
                        }
                        let mut batch_tx = self
                            .conn
                            .transaction_with_behavior(TransactionBehavior::Immediate)
                            .with_context(|| {
                                format!(
                                    "daily_stats phase=message_stage_begin conversation_id={conversation_id} cursor_message_idx={cursor_message_idx}"
                                )
                            })?;
                        franken_update_daily_stats_batched_in_tx_for_target(
                            &batch_tx,
                            &entries,
                            DailyStatsBatchTarget::RebuildStage,
                        )
                        .with_context(|| {
                            format!(
                                "daily_stats phase=message_stage_upsert conversation_id={conversation_id} cursor_message_idx={cursor_message_idx}"
                            )
                        })?;
                        if let Some(control) = control {
                            control()?;
                        }
                        batch_tx.commit().with_context(|| {
                            format!(
                                "daily_stats phase=message_stage_commit conversation_id={conversation_id} cursor_message_idx={cursor_message_idx}"
                            )
                        })?;
                    }
                    if let Some(heartbeat) = heartbeat {
                        heartbeat();
                    }
                    if message_batch_count.is_multiple_of(50) {
                        tracing::info!(
                            target: "cass::perf::daily_stats",
                            messages_processed,
                            batches = message_batch_count,
                            batch_size = message_batch_size,
                            source = "messages",
                            conversation_id,
                            cursor_message_idx,
                            "daily_stats rebuild message scan progress"
                        );
                    }
                    if page_rows < message_batch_size {
                        break;
                    }
                }
            }
        }

        let rows_created: i64 = self
            .conn
            .query_row_map(
                "SELECT COUNT(*) FROM daily_stats_rebuild_stage",
                fparams![],
                |row| row.get_typed(0),
            )
            .with_context(|| "daily_stats phase=stage_count metric=rows_created")?;
        let total_sessions: i64 = self
            .conn
            .query_row_map(
                "SELECT COALESCE(SUM(session_count), 0) FROM daily_stats_rebuild_stage WHERE agent_slug = 'all' AND source_id = 'all'",
                fparams![],
                |row| row.get_typed(0),
            )
            .with_context(|| "daily_stats phase=stage_count metric=total_sessions")?;

        if let Some(control) = control {
            control()?;
        }
        let mut publish_tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .with_context(|| "daily_stats phase=publish_begin")?;
        publish_tx
            .execute("DELETE FROM daily_stats")
            .with_context(|| "daily_stats phase=publish_delete")?;
        publish_tx
            .execute(
                "INSERT INTO daily_stats (
                day_id, agent_slug, source_id, session_count, message_count, total_chars, last_updated
             )
             SELECT day_id, agent_slug, source_id, session_count, message_count, total_chars, last_updated
             FROM daily_stats_rebuild_stage",
            )
            .with_context(|| "daily_stats phase=publish_insert")?;
        publish_tx.execute_compat(
            "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
            fparams![
                DAILY_STATS_HEALTH_GENERATION_META_KEY,
                DAILY_STATS_HEALTH_GENERATION.to_string()
            ],
        )?;
        if let Some(control) = control {
            control()?;
        }
        publish_tx
            .commit()
            .with_context(|| "daily_stats phase=publish_commit")?;
        if let Some(heartbeat) = heartbeat {
            heartbeat();
        }
        self.conn
            .execute("DELETE FROM daily_stats_rebuild_stage")
            .with_context(|| "daily_stats phase=clear_stage_after_publish")?;

        tracing::info!(
            target: "cass::perf::daily_stats",
            rows_created,
            total_sessions,
            conversations_processed,
            conversation_batches = conversation_batch_count,
            conversation_batch_size,
            message_batches = message_batch_count,
            message_batch_size,
            messages_processed,
            raw_entries_flushed,
            expanded_entries_flushed,
            "Daily stats rebuilt from conversations"
        );

        Ok(DailyStatsRebuildResult {
            rows_created,
            total_sessions,
        })
    }
}

/// Health status of daily stats table.
#[derive(Debug, Clone)]
pub struct DailyStatsHealth {
    pub populated: bool,
    pub row_count: i64,
    pub oldest_update_ms: Option<i64>,
    pub conversation_count: i64,
    pub materialized_total: i64,
    pub drift: i64,
}
