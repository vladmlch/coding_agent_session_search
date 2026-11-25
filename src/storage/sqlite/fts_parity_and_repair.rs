use super::backup_and_integrity::{
    DAILY_STATS_CONTENT_REVISION_PENDING, DAILY_STATS_HEALTH_GENERATION,
    DAILY_STATS_HEALTH_GENERATION_META_KEY, DAILY_STATS_HEALTH_META_KEY,
    FTS_FALLBACK_REPAIR_PENDING_DETAIL_MAX_BYTES, FTS_FALLBACK_REPAIR_PENDING_META_KEY,
    FTS_FRANKEN_CONTENT_REVISION_PENDING, FTS_FRANKEN_REBUILD_FINGERPRINT_META_KEY,
    FTS_FRANKEN_REBUILD_GENERATION, FTS_FRANKEN_REBUILD_META_KEY, FTS_INDEXABLE_MESSAGE_COUNT_SQL,
    FTS_INDEXED_CANONICAL_INTERSECTION_SQL, FTS_MESSAGES_REQUIRED_SHADOW_TABLES,
    FTS_SHADOW_NOT_VIABLE_META_KEY, FtsConsistencyRepair, FtsDryRunParity, FtsShadowParity,
    FtsShadowParityStatus, LEXICAL_REPAIR_DEFERRED_COUNT_META_KEY,
    LEXICAL_REPAIR_DEFERRED_REASON_META_KEY, classify_fts_shadow_parity,
    validate_fts_messages_integrity_for_connection,
};
use super::storage_core::FrankenStorage;
use super::tx_conversation_insert::{
    FtsShadowViability, bounded_fts_marker_detail, fts_inline_flush_budget,
    fts_shadow_max_messages, fts_shadow_not_viable_detail, incompatible_legacy_fts_shadow_ddl,
};
use crate::franken_sync::compat::{
    ConnectionExt as FrankenConnectionExt, OptionalExtension as FrankenOptionalExtension,
    RowExt as FrankenRowExt,
};
use anyhow::{Context, Result};
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::time::Duration;

impl FrankenStorage {
    /// Rebuild the FTS5 index from scratch in one failure-atomic transaction.
    ///
    /// Row materialization remains chunked to bound memory, but the old
    /// queryable shadow is not published away until the replacement has exact
    /// canonical parity and the transaction commits. A process interruption
    /// rolls the create-or-clear operation and every insert back together.
    pub fn rebuild_fts(&self) -> Result<()> {
        self.rebuild_fts_via_frankensqlite().map(|_| ())
    }

    /// Best-effort repair for the derived SQLite FTS fallback index.
    ///
    /// The canonical archive and Tantivy index remain authoritative, so callers
    /// should invoke this from maintenance paths rather than ordinary opens.
    pub(crate) fn ensure_search_fallback_fts_consistency(&self) -> Result<FtsConsistencyRepair> {
        self.ensure_fts_consistency_via_frankensqlite()
    }

    /// GH #413 follow-up: charge one inline shadow flush against this run's
    /// budget (`fts_inline_flush_budget`); past it the rest of this process
    /// skips the shadow and remembers why.
    pub(super) fn charge_fts_inline_flush(&self, elapsed: Duration, docs: usize) {
        let Some(budget) = fts_inline_flush_budget() else {
            return;
        };
        let elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
        let spent_ms = self
            .fts_shadow_run
            .inline_spent_ms
            .fetch_add(elapsed_ms, Ordering::SeqCst)
            .saturating_add(elapsed_ms);
        if spent_ms <= u64::try_from(budget.as_millis()).unwrap_or(u64::MAX) {
            return;
        }
        let budget_secs = budget.as_secs();
        let detail = format!(
            "inline fts_messages shadow writes suspended for this run: {spent_ms} ms spent \
             (last flush: {docs} docs in {elapsed_ms} ms) exceeded the {budget_secs} s budget \
             (CASS_FTS_INLINE_BUDGET_SECS); fsqlite's FTS5 does O(table) work per statement \
             on a shadow this size (GH #413, frankensqlite#405/#406). The shadow is behind \
             until a repair; Quill lexical search is unaffected"
        );
        if self.suspend_fts_inline_writes(detail) {
            tracing::warn!(
                target: "cass::fts_inline",
                spent_ms,
                budget_secs,
                last_flush_docs = docs,
                last_flush_ms = elapsed_ms,
                "inline fallback-FTS shadow writes suspended: budget exceeded"
            );
        }
    }

    /// Suspend inline shadow writes for the rest of this process with the
    /// given reason. Returns `false` when they were already suspended (the
    /// first reason stands).
    pub(super) fn suspend_fts_inline_writes(&self, detail: String) -> bool {
        let mut suspended = self.fts_shadow_run.inline_suspended.lock();
        if suspended.is_some() {
            return false;
        }
        *suspended = Some(detail);
        true
    }

    pub(super) fn fts_inline_writes_suspended(&self) -> bool {
        self.fts_shadow_run.inline_suspended.lock().is_some()
    }

    /// Why inline `fts_messages` writes were suspended in this process, if
    /// they were (GH #413 follow-up). The index run persists it for `doctor`.
    pub(crate) fn fts_inline_suspension(&self) -> Option<String> {
        self.fts_shadow_run.inline_suspended.lock().clone()
    }

    /// Indexable messages the canonical archive holds: what a complete shadow
    /// would have to materialize. The exact index-driven count the parity
    /// checks already use, so it costs no scan of `messages`.
    pub(crate) fn fts_shadow_corpus_messages(&self) -> Result<u64> {
        let total = self
            .count_fts_indexable_messages()
            .with_context(|| "counting the canonical corpus for the FTS shadow bound")?;
        Ok(u64::try_from(total).unwrap_or(0))
    }

    /// Is the shadow in the catalog? Virtual tables carry `rootpage = 0`, so
    /// this must not filter on rootpage (the unit test caught a probe that
    /// did and reported every shadow as absent).
    fn fts_shadow_registered(&self) -> Result<bool> {
        let rows: i64 = self
            .conn
            .query_row_map(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'fts_messages'",
                fparams![],
                |row| row.get_typed::<i64>(0),
            )
            .with_context(|| "probing the catalog for the fts_messages shadow")?;
        Ok(rows > 0)
    }

    /// GH #413 follow-up (iify0): may the derived shadow exist for this
    /// archive's corpus? Reads only the catalog and `conversations`, so it is
    /// safe before the first write on a fresh connection (the point at which
    /// fsqlite would materialize a populated shadow in memory).
    pub(crate) fn fts_shadow_viability(&self) -> Result<FtsShadowViability> {
        self.fts_shadow_viability_with_bound(fts_shadow_max_messages())
    }

    pub(crate) fn fts_shadow_viability_with_bound(
        &self,
        bound_messages: Option<u64>,
    ) -> Result<FtsShadowViability> {
        let registered = self.fts_shadow_registered()?;
        let corpus_messages = self.fts_shadow_corpus_messages()?;
        Ok(match bound_messages {
            Some(bound_messages) if corpus_messages > bound_messages => {
                FtsShadowViability::NotViable {
                    corpus_messages,
                    bound_messages,
                }
            }
            _ if !registered => FtsShadowViability::Absent,
            _ => FtsShadowViability::Viable { corpus_messages },
        })
    }

    /// The oversized shadow is already fully retired: no registration, no
    /// leftover `fts_messages_*` table, and both durable markers recorded.
    /// A startup preflight then has nothing to drop or record. Re-retiring
    /// would rewrite two meta rows on every index run, moving the WAL's
    /// physical identity and invalidating the one-shot fingerprint cache
    /// before the next search, for no change in state.
    pub(crate) fn fts_shadow_retirement_is_recorded(&self) -> Result<bool> {
        Ok(!self.fts_shadow_registered()?
            && self.fts_shadow_residue_detail(0)?.is_none()
            && self.fts_shadow_not_viable_marker()?.is_some()
            && self.read_fallback_fts_repair_pending()?.is_some())
    }

    /// GH #497 follow-up: the shadow is retired and both markers already hold
    /// exactly `detail` as [`Self::drop_fts_shadow_as_not_viable`] would
    /// write it, so retiring again would change nothing.
    fn fts_shadow_retirement_matches(&self, detail: &str) -> Result<bool> {
        let expected = bounded_fts_marker_detail(detail);
        Ok(self.fts_shadow_retirement_is_recorded()?
            && self.fts_shadow_not_viable_marker()?.as_deref() == Some(expected.as_str())
            && self.read_fallback_fts_repair_pending()?.as_deref() == Some(expected.as_str()))
    }

    /// GH #497 follow-up: the corpus is over the current bound and the
    /// recorded retirement already describes it exactly, so a repair has
    /// nothing to do (read-only; safe on a read-only connection).
    pub(crate) fn fts_shadow_retirement_is_current(&self) -> Result<bool> {
        let Some(bound_messages) = fts_shadow_max_messages() else {
            return Ok(false);
        };
        let corpus_messages = self.fts_shadow_corpus_messages()?;
        if corpus_messages <= bound_messages {
            return Ok(false);
        }
        self.fts_shadow_retirement_matches(&fts_shadow_not_viable_detail(
            corpus_messages,
            bound_messages,
        ))
    }

    /// Remember how much corpus the shadow already covers, so inline flushes
    /// can tell when this run crosses the bound.
    pub(crate) fn note_fts_shadow_corpus_messages(&self, corpus_messages: u64) {
        self.fts_shadow_run
            .messages_seen
            .store(corpus_messages, Ordering::SeqCst);
    }

    /// Drop the derived shadow because the engine cannot materialize it, and
    /// record why: the not-viable marker gates recreation, the repair-pending
    /// marker is what `status`/`doctor` already surface. Safe on a deferred-
    /// FTS5 connection (nothing is hydrated) and on an ordinary one (the
    /// vtab's destructor never reads the shadow).
    pub(crate) fn drop_fts_shadow_as_not_viable(&self, detail: &str) -> Result<()> {
        self.invalidate_fts_messages_present_cache();
        self.conn
            .execute("DROP TABLE IF EXISTS fts_messages")
            .with_context(|| "dropping the not-viable FTS5 shadow")?;
        for table in FTS_MESSAGES_REQUIRED_SHADOW_TABLES {
            self.conn
                .execute(&format!("DROP TABLE IF EXISTS {table}"))
                .with_context(|| format!("dropping the not-viable FTS5 shadow table {table}"))?;
        }
        self.set_fts_messages_present_cache(false);
        self.fts_shadow_run
            .drop_pending
            .store(false, Ordering::SeqCst);
        let bounded = bounded_fts_marker_detail(detail);
        self.conn
            .execute_compat(
                "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
                fparams![FTS_SHADOW_NOT_VIABLE_META_KEY, bounded],
            )
            .with_context(|| "recording the not-viable FTS shadow marker")?;
        self.record_fallback_fts_repair_pending(Some(detail))?;
        Ok(())
    }

    /// The persisted not-viable marker, if the shadow was dropped for size.
    pub(crate) fn fts_shadow_not_viable_marker(&self) -> Result<Option<String>> {
        Ok(self
            .conn
            .query_row_map(
                "SELECT value FROM meta WHERE key = ?1",
                fparams![FTS_SHADOW_NOT_VIABLE_META_KEY],
                |row| row.get_typed(0),
            )
            .optional()?
            .filter(|detail: &String| !detail.is_empty()))
    }

    pub(super) fn clear_fts_shadow_not_viable_marker(&self) -> Result<()> {
        self.conn
            .execute_compat(
                "DELETE FROM meta WHERE key = ?1",
                fparams![FTS_SHADOW_NOT_VIABLE_META_KEY],
            )
            .with_context(|| "clearing the not-viable FTS shadow marker")?;
        Ok(())
    }

    /// Whether an inline flush crossed the bound during this run, leaving the
    /// drop for the run's finalize step (the shadow is oversized now).
    pub(crate) fn fts_shadow_drop_pending(&self) -> bool {
        self.fts_shadow_run.drop_pending.load(Ordering::SeqCst)
    }

    /// Refuse shadow recreation whenever the corpus exceeds the current bound
    /// (`Some(detail)`), regardless of any historical retirement marker. Clear
    /// an obsolete marker and allow the rebuild once the corpus fits again.
    fn fts_shadow_recreate_refused(&self) -> Result<Option<String>> {
        self.fts_shadow_recreate_refused_with_bound(fts_shadow_max_messages())
    }

    pub(super) fn fts_shadow_recreate_refused_with_bound(
        &self,
        bound_messages: Option<u64>,
    ) -> Result<Option<String>> {
        let marker_present = self.fts_shadow_not_viable_marker()?.is_some();
        let corpus_messages = self.fts_shadow_corpus_messages()?;
        if let Some(bound_messages) = bound_messages
            && corpus_messages > bound_messages
        {
            return Ok(Some(fts_shadow_not_viable_detail(
                corpus_messages,
                bound_messages,
            )));
        }
        if marker_present {
            self.clear_fts_shadow_not_viable_marker()?;
        }
        Ok(None)
    }

    /// #439: install (or, with `None`, clear) the liveness callback that
    /// fallback-FTS shadow maintenance invokes once per streamed page. See
    /// the `fts_maintenance_heartbeat` field.
    pub(crate) fn set_fts_maintenance_heartbeat(
        &self,
        heartbeat: Option<Box<dyn Fn() + Send + Sync>>,
    ) {
        *self.fts_maintenance_heartbeat.lock() = heartbeat;
    }

    pub(super) fn tick_fts_maintenance_heartbeat(&self) {
        if let Some(heartbeat) = self.fts_maintenance_heartbeat.lock().as_ref() {
            heartbeat();
        }
    }

    pub(crate) fn validate_fts_messages_integrity(&self) -> Result<()> {
        validate_fts_messages_integrity_for_connection(&self.conn)
    }

    /// GH #438: rewrite the `fts_messages` segments with the current writer.
    /// fsqlite's FTS5 'optimize' merges every segment into one freshly written
    /// segment, which is the in-place migration for segments written before
    /// frankensqlite#404 (the format stock SQLite's validators reject even
    /// though reads work). On an already optimized modern index it is a no-op
    /// decided before any hydration. Parity cannot change: only derived
    /// segment storage is rewritten.
    pub(crate) fn optimize_fts_messages_segments(&self) -> Result<()> {
        self.conn
            .execute("INSERT INTO fts_messages(fts_messages) VALUES('optimize')")
            .with_context(|| "rewriting fts_messages segments with FTS5 optimize")?;
        Ok(())
    }

    pub(crate) fn fallback_fts_is_known_healthy_for_archive_fingerprint(
        &self,
        archive_fingerprint: &str,
    ) -> Result<bool> {
        Ok(
            self.read_fts_franken_rebuild_generation()? == Some(FTS_FRANKEN_REBUILD_GENERATION)
                && self
                    .read_fts_franken_rebuild_archive_fingerprint()?
                    .as_deref()
                    == Some(archive_fingerprint),
        )
    }

    pub(crate) fn record_search_fallback_fts_archive_fingerprint(
        &self,
        archive_fingerprint: &str,
    ) -> Result<()> {
        self.conn
            .execute_compat(
                "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
                fparams![
                    FTS_FRANKEN_REBUILD_FINGERPRINT_META_KEY,
                    archive_fingerprint.to_string()
                ],
            )
            .with_context(|| "recording frankensqlite FTS archive fingerprint")?;
        Ok(())
    }

    /// Record (or, with `None`, clear) the "the canonical FTS shadow may be
    /// half-rebuilt" marker after a full index run (zn1xn). Best-effort:
    /// callers ignore errors because this is observability, not correctness —
    /// a failed marker write must never fail a run whose canonical + Tantivy
    /// work already succeeded.
    pub(crate) fn record_fallback_fts_repair_pending(&self, detail: Option<&str>) -> Result<()> {
        match detail {
            Some(detail) => {
                let bounded = bounded_fts_marker_detail(detail);
                self.conn
                    .execute_compat(
                        "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
                        fparams![FTS_FALLBACK_REPAIR_PENDING_META_KEY, bounded],
                    )
                    .with_context(|| "recording fallback FTS repair-pending marker")?;
            }
            None => {
                self.conn
                    .execute_compat(
                        "DELETE FROM meta WHERE key = ?1",
                        fparams![FTS_FALLBACK_REPAIR_PENDING_META_KEY],
                    )
                    .with_context(|| "clearing fallback FTS repair-pending marker")?;
            }
        }
        Ok(())
    }

    /// The persisted fallback-FTS repair-pending detail, if the last full index
    /// run left the canonical `fts_messages` shadow unrepaired (zn1xn).
    pub(crate) fn read_fallback_fts_repair_pending(&self) -> Result<Option<String>> {
        Ok(self
            .conn
            .query_row_map(
                "SELECT value FROM meta WHERE key = ?1",
                fparams![FTS_FALLBACK_REPAIR_PENDING_META_KEY],
                |row| row.get_typed(0),
            )
            .optional()?
            .filter(|detail: &String| !detail.is_empty()))
    }

    fn read_lexical_repair_deferred_count(&self) -> Result<i64> {
        Ok(self
            .conn
            .query_row_map(
                "SELECT value FROM meta WHERE key = ?1",
                fparams![LEXICAL_REPAIR_DEFERRED_COUNT_META_KEY],
                |row| row.get_typed::<String>(0),
            )
            .optional()?
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0))
    }

    /// Increment the consecutive-deferral streak and record the reason after a
    /// non-watch run deferred inline lexical updates (zn1xn F4). Returns the new
    /// streak length. Best-effort observability — callers ignore errors.
    pub(crate) fn record_lexical_repair_deferred(&self, reason: &str) -> Result<i64> {
        let next = self.read_lexical_repair_deferred_count()?.saturating_add(1);
        self.conn
            .execute_compat(
                "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
                fparams![LEXICAL_REPAIR_DEFERRED_COUNT_META_KEY, next.to_string()],
            )
            .with_context(|| "recording lexical-repair deferral streak")?;
        let bounded: String = reason
            .chars()
            .take(FTS_FALLBACK_REPAIR_PENDING_DETAIL_MAX_BYTES)
            .collect();
        self.conn
            .execute_compat(
                "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
                fparams![LEXICAL_REPAIR_DEFERRED_REASON_META_KEY, bounded],
            )
            .with_context(|| "recording lexical-repair deferral reason")?;
        Ok(next)
    }

    /// Clear the consecutive-deferral streak after a non-watch run completed
    /// without deferring inline lexical updates (zn1xn F4).
    pub(crate) fn clear_lexical_repair_deferred(&self) -> Result<()> {
        self.conn
            .execute_compat(
                "DELETE FROM meta WHERE key = ?1",
                fparams![LEXICAL_REPAIR_DEFERRED_COUNT_META_KEY],
            )
            .with_context(|| "clearing lexical-repair deferral streak")?;
        self.conn
            .execute_compat(
                "DELETE FROM meta WHERE key = ?1",
                fparams![LEXICAL_REPAIR_DEFERRED_REASON_META_KEY],
            )
            .with_context(|| "clearing lexical-repair deferral reason")?;
        Ok(())
    }

    /// The consecutive-deferral streak length and last reason, if a non-watch
    /// run has persistently deferred inline lexical updates (zn1xn F4).
    #[cfg(test)]
    pub(crate) fn read_lexical_repair_deferred(&self) -> Result<Option<(i64, String)>> {
        let count = self.read_lexical_repair_deferred_count()?;
        if count <= 0 {
            return Ok(None);
        }
        let reason = self
            .conn
            .query_row_map(
                "SELECT value FROM meta WHERE key = ?1",
                fparams![LEXICAL_REPAIR_DEFERRED_REASON_META_KEY],
                |row| row.get_typed(0),
            )
            .optional()?
            .unwrap_or_default();
        Ok(Some((count, reason)))
    }

    pub(crate) fn daily_stats_is_known_healthy_for_archive_fingerprint(
        &self,
        archive_fingerprint: &str,
    ) -> Result<bool> {
        Ok(
            self.read_daily_stats_health_generation()? == Some(DAILY_STATS_HEALTH_GENERATION)
                && self.read_daily_stats_archive_fingerprint()?.as_deref()
                    == Some(archive_fingerprint),
        )
    }

    pub(crate) fn daily_stats_content_repair_required(&self) -> Result<bool> {
        Ok(
            self.read_daily_stats_health_generation()?
                == Some(DAILY_STATS_CONTENT_REVISION_PENDING),
        )
    }

    pub(crate) fn record_daily_stats_archive_fingerprint(
        &self,
        archive_fingerprint: &str,
    ) -> Result<()> {
        self.conn
            .execute_compat(
                "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
                fparams![
                    DAILY_STATS_HEALTH_GENERATION_META_KEY,
                    DAILY_STATS_HEALTH_GENERATION.to_string()
                ],
            )
            .with_context(|| "recording daily_stats health generation")?;
        self.conn
            .execute_compat(
                "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
                fparams![DAILY_STATS_HEALTH_META_KEY, archive_fingerprint.to_string()],
            )
            .with_context(|| "recording daily_stats archive fingerprint")?;
        Ok(())
    }

    pub(super) fn read_fts_franken_rebuild_generation(&self) -> Result<Option<i64>> {
        let value: Option<String> = self
            .conn
            .query_row_map(
                "SELECT value FROM meta WHERE key = ?1",
                fparams![FTS_FRANKEN_REBUILD_META_KEY],
                |row| row.get_typed(0),
            )
            .optional()?;
        Ok(value.and_then(|v| v.parse::<i64>().ok()))
    }

    fn read_fts_franken_rebuild_archive_fingerprint(&self) -> Result<Option<String>> {
        Ok(self
            .conn
            .query_row_map(
                "SELECT value FROM meta WHERE key = ?1",
                fparams![FTS_FRANKEN_REBUILD_FINGERPRINT_META_KEY],
                |row| row.get_typed(0),
            )
            .optional()?)
    }

    fn read_daily_stats_health_generation(&self) -> Result<Option<i64>> {
        let value: Option<String> = self
            .conn
            .query_row_map(
                "SELECT value FROM meta WHERE key = ?1",
                fparams![DAILY_STATS_HEALTH_GENERATION_META_KEY],
                |row| row.get_typed(0),
            )
            .optional()?;
        Ok(value.and_then(|value| value.parse::<i64>().ok()))
    }

    fn read_daily_stats_archive_fingerprint(&self) -> Result<Option<String>> {
        Ok(self
            .conn
            .query_row_map(
                "SELECT value FROM meta WHERE key = ?1",
                fparams![DAILY_STATS_HEALTH_META_KEY],
                |row| row.get_typed(0),
            )
            .optional()?)
    }

    pub(super) fn record_fts_franken_rebuild_generation(&self) -> Result<()> {
        self.conn
            .execute_compat(
                "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
                fparams![
                    FTS_FRANKEN_REBUILD_META_KEY,
                    FTS_FRANKEN_REBUILD_GENERATION.to_string()
                ],
            )
            .with_context(|| "recording frankensqlite FTS rebuild generation")?;
        Ok(())
    }

    fn fetch_indexable_message_id_parity_page(
        &self,
        after: Option<i64>,
        page_rows: i64,
    ) -> Result<Vec<i64>> {
        match after {
            Some(after) => self.conn.query_map_collect(
                "SELECT m.id
                 FROM messages m
                 INNER JOIN conversations c ON c.id = m.conversation_id
                 WHERE m.id > ?1
                 ORDER BY m.id
                 LIMIT ?2",
                fparams![after, page_rows],
                |row| row.get_typed(0),
            ),
            None => self.conn.query_map_collect(
                "SELECT m.id
                 FROM messages m
                 INNER JOIN conversations c ON c.id = m.conversation_id
                 ORDER BY m.id
                 LIMIT ?1",
                fparams![page_rows],
                |row| row.get_typed(0),
            ),
        }
        .with_context(|| "streaming bounded canonical message IDs for exact FTS parity")
    }

    fn fetch_fts_docsize_id_parity_page(
        &self,
        after: Option<i64>,
        page_rows: i64,
    ) -> Result<Vec<i64>> {
        match after {
            Some(after) => self.conn.query_map_collect(
                "SELECT id
                 FROM fts_messages_docsize
                 WHERE id > ?1
                 ORDER BY id
                 LIMIT ?2",
                fparams![after, page_rows],
                |row| row.get_typed(0),
            ),
            None => self.conn.query_map_collect(
                "SELECT id
                 FROM fts_messages_docsize
                 ORDER BY id
                 LIMIT ?1",
                fparams![page_rows],
                |row| row.get_typed(0),
            ),
        }
        .with_context(|| "streaming bounded FTS docsize IDs for exact parity")
    }

    /// Test oracle for the aggregate parity classifier. The production path
    /// deliberately avoids this exact-but-expensive repeated keyset traversal;
    /// keeping it available in tests lets small fixtures compare both methods.
    #[cfg(test)]
    pub(super) fn inspect_fts_rowid_set_difference_bounded(&self) -> Result<(bool, bool)> {
        self.inspect_fts_rowid_set_difference_with_page_rows(4_096)
    }

    #[cfg(test)]
    pub(super) fn inspect_fts_rowid_set_difference_with_page_rows(
        &self,
        page_rows: usize,
    ) -> Result<(bool, bool)> {
        anyhow::ensure!(page_rows > 0, "FTS parity page size must be positive");
        let page_limit = i64::try_from(page_rows).unwrap_or(i64::MAX);
        let mut canonical_page = self.fetch_indexable_message_id_parity_page(None, page_limit)?;
        let mut indexed_page = self.fetch_fts_docsize_id_parity_page(None, page_limit)?;
        let mut canonical_index = 0usize;
        let mut indexed_index = 0usize;
        let mut canonical_done = false;
        let mut indexed_done = false;
        let mut has_missing = false;
        let mut has_excess = false;

        loop {
            if canonical_index == canonical_page.len() && !canonical_done {
                if canonical_page.len() < page_rows {
                    canonical_done = true;
                } else {
                    let after = canonical_page.last().copied().ok_or_else(|| {
                        anyhow::anyhow!("non-empty canonical parity page lost its cursor")
                    })?;
                    canonical_page =
                        self.fetch_indexable_message_id_parity_page(Some(after), page_limit)?;
                    canonical_index = 0;
                    if canonical_page.is_empty() {
                        canonical_done = true;
                    }
                }
            }
            if indexed_index == indexed_page.len() && !indexed_done {
                if indexed_page.len() < page_rows {
                    indexed_done = true;
                } else {
                    let after = indexed_page.last().copied().ok_or_else(|| {
                        anyhow::anyhow!("non-empty FTS parity page lost its cursor")
                    })?;
                    indexed_page =
                        self.fetch_fts_docsize_id_parity_page(Some(after), page_limit)?;
                    indexed_index = 0;
                    if indexed_page.is_empty() {
                        indexed_done = true;
                    }
                }
            }

            let canonical_id = if canonical_done {
                None
            } else {
                canonical_page.get(canonical_index).copied()
            };
            let indexed_id = if indexed_done {
                None
            } else {
                indexed_page.get(indexed_index).copied()
            };
            match (canonical_id, indexed_id) {
                (Some(canonical_id), Some(indexed_id)) => {
                    match canonical_id.cmp(&indexed_id) {
                        std::cmp::Ordering::Less => {
                            has_missing = true;
                            canonical_index = canonical_index.saturating_add(1);
                        }
                        std::cmp::Ordering::Equal => {
                            canonical_index = canonical_index.saturating_add(1);
                            indexed_index = indexed_index.saturating_add(1);
                        }
                        std::cmp::Ordering::Greater => {
                            has_excess = true;
                            indexed_index = indexed_index.saturating_add(1);
                        }
                    }
                    if has_missing && has_excess {
                        return Ok((true, true));
                    }
                }
                (Some(_), None) => return Ok((true, has_excess)),
                (None, Some(_)) => return Ok((has_missing, true)),
                (None, None) => return Ok((has_missing, has_excess)),
            }
        }
    }

    fn load_fts_conversation_id_set(&self) -> Result<HashSet<i64>> {
        self.conn
            .query_map_collect(
                "SELECT id FROM conversations NOT INDEXED",
                fparams![],
                |row| row.get_typed(0),
            )
            .with_context(|| "loading bounded canonical conversation IDs for FTS parity")
            .map(|ids| ids.into_iter().collect())
    }

    /// Compute exact canonical/FTS rowid intersection in one bounded-memory,
    /// shadow-driven pass. A partial 39k-row shadow performs 39k primary-key
    /// probes rather than streaming the multi-million-row canonical message
    /// table; a complete shadow remains exact and read-only. Memory scales
    /// with parent conversations, not messages or the FTS rowid domain.
    pub(super) fn count_fts_indexed_canonical_intersection(&self) -> Result<i64> {
        let conversation_ids = self.load_fts_conversation_id_set()?;
        let mut intersection_messages = 0_i64;
        let mut count_overflowed = false;
        self.conn
            .query_with_params_for_each(FTS_INDEXED_CANONICAL_INTERSECTION_SQL, &[], |row| {
                let conversation_id = row.get_typed::<i64>(0)?;
                if conversation_ids.contains(&conversation_id) {
                    match intersection_messages.checked_add(1) {
                        Some(next) => intersection_messages = next,
                        None => count_overflowed = true,
                    }
                }
                Ok(())
            })
            .with_context(|| "streaming exact shadow-driven canonical/FTS intersection")?;
        anyhow::ensure!(!count_overflowed, "FTS parity row count exceeds i64::MAX");
        Ok(intersection_messages)
    }

    pub(super) fn count_fts_indexable_messages(&self) -> Result<i64> {
        self.conn
            .query_row_map(FTS_INDEXABLE_MESSAGE_COUNT_SQL, fparams![], |row| {
                row.get_typed::<i64>(0)
            })
            .with_context(|| "counting exact canonical FTS messages by parent index runs")
    }

    /// Inspect enough row IDs to make small shadows exact while bounding work
    /// on multi-million-row archives. Counts remain exact and cheap; row-ID
    /// comparison stops after `comparison_cap` IDs from each domain.
    pub(crate) fn inspect_search_fallback_fts_parity_dry_run(
        &self,
        comparison_cap: usize,
    ) -> Result<FtsDryRunParity> {
        anyhow::ensure!(
            comparison_cap > 0,
            "FTS dry-run comparison cap must be positive"
        );
        let canonical_messages =
            self.conn
                .query_row_map("SELECT COUNT(*) FROM messages", fparams![], |row| {
                    row.get_typed::<i64>(0)
                })?;
        let indexable_messages = self.count_fts_indexable_messages()?;
        let fts_schema_rows = self.conn.query_row_map(
            "SELECT COUNT(*) FROM sqlite_master WHERE name = 'fts_messages'",
            fparams![],
            |row| row.get_typed::<i64>(0),
        )?;
        let shadow_residue = self.fts_shadow_residue_detail(fts_schema_rows)?;

        let terminal = |status, indexed_messages, detail| FtsDryRunParity {
            exact_status: Some(status),
            canonical_messages,
            indexable_messages,
            indexed_messages,
            inspection_complete: true,
            comparison_cap,
            canonical_ids_examined: 0,
            indexed_ids_examined: 0,
            observed_missing_canonical_rowids_at_least: 0,
            observed_excess_fts_rowids_at_least: 0,
            detail,
        };
        if fts_schema_rows == 0 {
            if let Some(detail) = shadow_residue {
                return Ok(terminal(FtsShadowParityStatus::Residue, None, Some(detail)));
            }
            return Ok(terminal(FtsShadowParityStatus::Absent, None, None));
        }
        if fts_schema_rows != 1 {
            return Ok(terminal(
                FtsShadowParityStatus::Unqueryable,
                None,
                Some(format!(
                    "sqlite_master contains {fts_schema_rows} fts_messages rows; expected exactly one"
                )),
            ));
        }
        if let Some(detail) = shadow_residue {
            return Ok(terminal(FtsShadowParityStatus::Residue, None, Some(detail)));
        }

        let indexed_messages = match self.conn.query_row_map(
            "SELECT COUNT(*) FROM fts_messages_docsize",
            fparams![],
            |row| row.get_typed::<i64>(0),
        ) {
            Ok(count) => count,
            Err(err) => {
                return Ok(terminal(
                    FtsShadowParityStatus::Unqueryable,
                    None,
                    Some(format!("counting fts_messages_docsize failed: {err}")),
                ));
            }
        };
        anyhow::ensure!(
            indexed_messages >= 0,
            "invalid FTS docsize count: {indexed_messages}"
        );

        let page_limit = i64::try_from(comparison_cap).unwrap_or(i64::MAX);
        let canonical_ids = self.fetch_indexable_message_id_parity_page(None, page_limit)?;
        let indexed_ids = self.fetch_fts_docsize_id_parity_page(None, page_limit)?;
        let canonical_complete = indexable_messages <= page_limit;
        let indexed_complete = indexed_messages <= page_limit;
        if canonical_complete {
            anyhow::ensure!(
                i64::try_from(canonical_ids.len()).ok() == Some(indexable_messages),
                "bounded canonical FTS dry-run returned {} IDs for an exact {indexable_messages}-row domain",
                canonical_ids.len()
            );
        }
        if indexed_complete {
            anyhow::ensure!(
                i64::try_from(indexed_ids.len()).ok() == Some(indexed_messages),
                "bounded FTS dry-run returned {} IDs for an exact {indexed_messages}-row shadow",
                indexed_ids.len()
            );
        }

        let mut canonical_index = 0usize;
        let mut indexed_index = 0usize;
        let mut observed_missing = 0usize;
        let mut observed_excess = 0usize;
        while let (Some(canonical_id), Some(indexed_id)) = (
            canonical_ids.get(canonical_index),
            indexed_ids.get(indexed_index),
        ) {
            match canonical_id.cmp(indexed_id) {
                std::cmp::Ordering::Less => {
                    observed_missing = observed_missing.saturating_add(1);
                    canonical_index = canonical_index.saturating_add(1);
                }
                std::cmp::Ordering::Equal => {
                    canonical_index = canonical_index.saturating_add(1);
                    indexed_index = indexed_index.saturating_add(1);
                }
                std::cmp::Ordering::Greater => {
                    observed_excess = observed_excess.saturating_add(1);
                    indexed_index = indexed_index.saturating_add(1);
                }
            }
        }
        if indexed_complete {
            observed_missing = observed_missing
                .saturating_add(canonical_ids.len().saturating_sub(canonical_index));
        }
        if canonical_complete {
            observed_excess =
                observed_excess.saturating_add(indexed_ids.len().saturating_sub(indexed_index));
        }

        let inspection_complete = canonical_complete && indexed_complete;
        let (exact_status, detail) = if inspection_complete {
            let missing_messages = i64::try_from(observed_missing).unwrap_or(i64::MAX);
            let excess_messages = i64::try_from(observed_excess).unwrap_or(i64::MAX);
            let intersection_messages = indexable_messages.saturating_sub(missing_messages);
            let (status, detail) = classify_fts_shadow_parity(
                indexable_messages,
                indexed_messages,
                intersection_messages,
                missing_messages,
                excess_messages,
            );
            (Some(status), detail)
        } else {
            // #345: a capped inspection reports a divergence FLOOR (">= N
            // divergent"), never an exact count — the exact intersection is
            // deferred to the --yes apply path so a read-only dry-run on a
            // multi-million-row archive stays bounded.
            let observed_divergent = observed_missing.saturating_add(observed_excess);
            (
                None,
                Some(format!(
                    "bounded dry-run stopped after at most {comparison_cap} row IDs per domain \
                     (>= {observed_divergent} divergent row ID(s) observed within the cap: \
                     missing >= {observed_missing}, excess >= {observed_excess}); exact parity \
                     is deferred to --yes before any mutation"
                )),
            )
        };

        Ok(FtsDryRunParity {
            exact_status,
            canonical_messages,
            indexable_messages,
            indexed_messages: Some(indexed_messages),
            inspection_complete,
            comparison_cap,
            canonical_ids_examined: canonical_ids.len(),
            indexed_ids_examined: indexed_ids.len(),
            observed_missing_canonical_rowids_at_least: observed_missing,
            observed_excess_fts_rowids_at_least: observed_excess,
            detail,
        })
    }

    pub(crate) fn inspect_search_fallback_fts_parity(&self) -> Result<FtsShadowParity> {
        let canonical_messages =
            self.conn
                .query_row_map("SELECT COUNT(*) FROM messages", fparams![], |row| {
                    row.get_typed::<i64>(0)
                })?;
        let fts_schema_rows = self.conn.query_row_map(
            "SELECT COUNT(*) FROM sqlite_master WHERE name = 'fts_messages'",
            fparams![],
            |row| row.get_typed::<i64>(0),
        )?;
        let shadow_residue = self.fts_shadow_residue_detail(fts_schema_rows)?;

        if fts_schema_rows == 0 {
            if let Some(detail) = shadow_residue {
                return Ok(FtsShadowParity {
                    status: FtsShadowParityStatus::Residue,
                    canonical_messages,
                    indexable_messages: self.count_fts_indexable_messages()?,
                    indexed_messages: None,
                    detail: Some(detail),
                });
            }
            return Ok(FtsShadowParity {
                status: FtsShadowParityStatus::Absent,
                canonical_messages,
                indexable_messages: self.count_fts_indexable_messages()?,
                indexed_messages: None,
                detail: None,
            });
        }
        if fts_schema_rows != 1 {
            return Ok(FtsShadowParity {
                status: FtsShadowParityStatus::Unqueryable,
                canonical_messages,
                indexable_messages: self.count_fts_indexable_messages()?,
                indexed_messages: None,
                detail: Some(format!(
                    "sqlite_master contains {fts_schema_rows} fts_messages rows; expected exactly one"
                )),
            });
        }
        if let Some(detail) = shadow_residue {
            return Ok(FtsShadowParity {
                status: FtsShadowParityStatus::Residue,
                canonical_messages,
                indexable_messages: self.count_fts_indexable_messages()?,
                indexed_messages: None,
                detail: Some(detail),
            });
        }

        // The docsize shadow has one row per contentless FTS document and is
        // much cheaper to count than asking the virtual table to materialize a
        // multi-million-row scan. Failure is itself a corruption signal.
        let indexed_messages = match self.conn.query_row_map(
            "SELECT COUNT(*) FROM fts_messages_docsize",
            fparams![],
            |row| row.get_typed::<i64>(0),
        ) {
            Ok(count) => count,
            Err(err) => {
                return Ok(FtsShadowParity {
                    status: FtsShadowParityStatus::Unqueryable,
                    canonical_messages,
                    indexable_messages: self.count_fts_indexable_messages()?,
                    indexed_messages: None,
                    detail: Some(format!("counting fts_messages_docsize failed: {err}")),
                });
            }
        };
        anyhow::ensure!(
            indexed_messages >= 0,
            "invalid FTS docsize count: {indexed_messages}"
        );
        let indexable_messages = self.count_fts_indexable_messages()?;
        let intersection_messages = self.count_fts_indexed_canonical_intersection()?;
        anyhow::ensure!(
            intersection_messages >= 0
                && intersection_messages <= indexable_messages
                && intersection_messages <= indexed_messages,
            "invalid canonical/FTS intersection cardinality: intersection={intersection_messages}, indexable={indexable_messages}, indexed={indexed_messages}"
        );
        let missing_messages = indexable_messages.saturating_sub(intersection_messages);
        let excess_messages = indexed_messages.saturating_sub(intersection_messages);
        let (status, detail) = classify_fts_shadow_parity(
            indexable_messages,
            indexed_messages,
            intersection_messages,
            missing_messages,
            excess_messages,
        );
        Ok(FtsShadowParity {
            status,
            canonical_messages,
            indexable_messages,
            indexed_messages: Some(indexed_messages),
            detail,
        })
    }

    fn rebuild_unusable_fts_shadow(&self, before: &FtsShadowParity) -> Result<usize> {
        if before.status != FtsShadowParityStatus::Absent {
            anyhow::bail!(
                "refusing destructive FTS recreation for a {} shadow; preserve the complete database bundle",
                before.status.as_str()
            );
        }
        self.rebuild_fts_via_frankensqlite()
    }

    /// Identify catalog shapes for which `_docsize` is not the complete
    /// engine rowid domain (GH #495).
    fn fts_shadow_residue_detail(&self, fts_schema_rows: i64) -> Result<Option<String>> {
        if fts_schema_rows == 0 {
            let names = self.conn.query_map_collect(
                "SELECT name FROM sqlite_master
                 WHERE type = 'table' AND name IN (
                   'fts_messages_config','fts_messages_content','fts_messages_data',
                   'fts_messages_docsize','fts_messages_idx'
                 ) ORDER BY name",
                fparams![],
                |row| row.get_typed::<String>(0),
            )?;
            if !names.is_empty() {
                return Ok(Some(format!(
                    "fts_messages registration is absent but derived shadow residue remains: {}",
                    names.join(", ")
                )));
            }
            return Ok(None);
        }
        if fts_schema_rows == 1 && !self.fts_messages_schema_is_canonical_contentless()? {
            return Ok(Some(
                "fts_messages uses legacy/noncanonical DDL; _docsize parity cannot prove the engine rowid domain and the derived shadow must be recreated"
                    .to_string(),
            ));
        }
        if fts_schema_rows == 1 {
            // GH #374: a canonical registration over legacy rowid shadow
            // tables. Row counts can match exactly, so without this the full
            // index refused the archive and pointed at a repair that found
            // nothing to do.
            let shadow_ddl = self.conn.query_map_collect(
                "SELECT name, sql FROM sqlite_master
                 WHERE type = 'table' AND name IN ('fts_messages_config', 'fts_messages_idx')
                 ORDER BY name",
                fparams![],
                |row| Ok((row.get_typed::<String>(0)?, row.get_typed::<String>(1)?)),
            )?;
            if let Some(problem) = shadow_ddl
                .iter()
                .find_map(|(table, ddl)| incompatible_legacy_fts_shadow_ddl(table, ddl))
            {
                return Ok(Some(problem));
            }
        }
        Ok(None)
    }

    pub(super) fn drop_fts_shadow_residue(&self) -> Result<()> {
        self.invalidate_fts_messages_present_cache();
        self.conn
            .execute("DROP TABLE IF EXISTS fts_messages")
            .with_context(|| "dropping the residue-bearing FTS5 registration")?;
        for table in FTS_MESSAGES_REQUIRED_SHADOW_TABLES
            .iter()
            .copied()
            .chain(std::iter::once("fts_messages_content"))
        {
            self.conn
                .execute(&format!("DROP TABLE IF EXISTS {table}"))
                .with_context(|| format!("dropping FTS5 residue table {table}"))?;
        }
        self.set_fts_messages_present_cache(false);
        Ok(())
    }

    pub(super) fn require_healthy_fts_parity(&self, context: &str) -> Result<FtsShadowParity> {
        let parity = self.inspect_search_fallback_fts_parity()?;
        if parity.status != FtsShadowParityStatus::Healthy {
            anyhow::bail!(
                "{context} did not reach canonical/FTS parity: status={}, canonical_messages={}, indexable_messages={}, indexed_messages={:?}, detail={:?}",
                parity.status.as_str(),
                parity.canonical_messages,
                parity.indexable_messages,
                parity.indexed_messages,
                parity.detail
            );
        }
        Ok(parity)
    }

    fn ensure_fts_consistency_via_frankensqlite(&self) -> Result<FtsConsistencyRepair> {
        if self.read_fts_franken_rebuild_generation()? == Some(FTS_FRANKEN_CONTENT_REVISION_PENDING)
        {
            if let Some(detail) = self.fts_shadow_recreate_refused()? {
                anyhow::bail!("{detail}");
            }
            let inserted_rows = self.rebuild_fts_via_frankensqlite()?;
            return Ok(FtsConsistencyRepair::Rebuilt { inserted_rows });
        }
        let before = self.inspect_search_fallback_fts_parity()?;
        match before.status {
            FtsShadowParityStatus::Healthy => {
                self.record_fts_franken_rebuild_generation()?;
                self.set_fts_messages_present_cache(true);
                Ok(FtsConsistencyRepair::AlreadyHealthy {
                    rows: usize::try_from(before.indexable_messages.max(0)).unwrap_or(usize::MAX),
                })
            }
            FtsShadowParityStatus::Partial => {
                let inserted_rows = self
                    .stream_fts_rows_via_frankensqlite(true)
                    .with_context(|| "resuming missing FTS rows via frankensqlite")?;
                let after = self.require_healthy_fts_parity("resumable FTS catch-up")?;
                self.record_fts_franken_rebuild_generation()?;
                self.set_fts_messages_present_cache(true);
                Ok(FtsConsistencyRepair::IncrementalCatchUp {
                    inserted_rows,
                    total_rows: usize::try_from(after.indexable_messages.max(0))
                        .unwrap_or(usize::MAX),
                })
            }
            FtsShadowParityStatus::Absent => {
                // GH #476: enforce the current corpus bound even when this
                // archive has no historical size-retirement marker. The index
                // run maps this error to a nonfatal outcome.
                if let Some(detail) = self.fts_shadow_recreate_refused()? {
                    // GH #497 follow-up: when the recorded retirement already
                    // says exactly this, it is the settled state; rewriting
                    // identical markers on every repair is a mutation with no
                    // change in state. A stale, foreign, or missing marker is
                    // still rewritten with the current counts.
                    if !self.fts_shadow_retirement_matches(&detail)? {
                        self.drop_fts_shadow_as_not_viable(&detail)?;
                    }
                    anyhow::bail!("{detail}");
                }
                let inserted_rows = self.rebuild_unusable_fts_shadow(&before)?;
                self.require_healthy_fts_parity("FTS recreation")?;
                self.record_fts_franken_rebuild_generation()?;
                self.set_fts_messages_present_cache(true);
                Ok(FtsConsistencyRepair::Rebuilt { inserted_rows })
            }
            FtsShadowParityStatus::Residue => {
                if let Some(detail) = self.fts_shadow_recreate_refused()? {
                    self.drop_fts_shadow_as_not_viable(&detail)?;
                    anyhow::bail!("{detail}");
                }
                self.drop_fts_shadow_residue()?;
                let inserted_rows = self.rebuild_fts_via_frankensqlite()?;
                self.require_healthy_fts_parity("residue-aware FTS recreation")?;
                self.record_fts_franken_rebuild_generation()?;
                self.set_fts_messages_present_cache(true);
                Ok(FtsConsistencyRepair::Rebuilt { inserted_rows })
            }
            FtsShadowParityStatus::Unqueryable => anyhow::bail!(
                "refusing to replace an unqueryable FTS shadow in place because its prior artifact cannot be proven restorable; preserve the complete database bundle for staged recovery"
            ),
            FtsShadowParityStatus::Excess | FtsShadowParityStatus::Divergent => anyhow::bail!(
                "refusing to drop a queryable {} FTS shadow (indexed={}, indexable={}); preserve the current artifact and use a failure-atomic staged rebuild",
                before.status.as_str(),
                before.indexed_messages.unwrap_or_default(),
                before.indexable_messages
            ),
        }
    }
}
