use super::backup_and_integrity::{FTS5_DELETE_ALL_SQL, FTS5_REGISTER_SQL, FtsShadowParityStatus};
use super::storage_core::FrankenStorage;
use super::tx_conversation_insert::{
    fts_repair_page_budget, fts_shadow_max_messages, fts_shadow_not_viable_detail,
};
use super::tx_message_insert::{franken_batch_insert_fts, franken_batch_insert_fts_on_connection};
use crate::franken_sync::{
    SqliteValue,
    compat::{
        ConnectionExt as FrankenConnectionExt, RowExt as FrankenRowExt,
        Transaction as FrankenTransaction,
    },
};
use crate::model::types::{AgentKind, Conversation, Message, MessageRole};
use anyhow::{Context, Result, anyhow, bail};
use std::collections::{HashMap, HashSet};
use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

impl FrankenStorage {
    /// #368 defect 3: rebuild the canonical FTS5 shadow by DROPPING the
    /// (possibly structurally corrupt) shadow and recreating it fresh from
    /// canonical rows, rather than the DELETE-based clear in
    /// [`Self::rebuild_fts_via_frankensqlite`], which routes through the FTS5
    /// vtab update path and hydrates the corrupt `%_data`.
    ///
    /// For the corrupt-`%_data` case this must be called on a connection
    /// opened via [`Self::open_deferred_fts5_for_repair`], so the corrupt
    /// shadow was never read at open. [`Self::rebuild_fts_via_frankensqlite`]
    /// also routes here from ordinarily opened connections when the surviving
    /// `fts_messages` CREATE is not cass's canonical contentless registration
    /// (legacy or stale-duplicate catalog rows): the open already succeeded
    /// there, and DROP TABLE removes every same-named catalog row, which the
    /// in-place DELETE path cannot. The FTS5 vtab's destructor is a no-op that never reads
    /// `%_data`, and its backing shadow tables are plain, so `DROP TABLE`
    /// removes the corrupt structure without decoding it. Canonical rows are
    /// never modified. NOT fully failure-atomic: the drop+recreate runs in
    /// autocommit (frankensqlite cannot recreate a virtual table in the same
    /// transaction as its DROP), so only the repopulate is transactional — a
    /// failed repopulate leaves an EMPTY (not corrupt) shadow, and re-running
    /// the repair is safe because the shadow is fully derived from canonical.
    pub(crate) fn rebuild_fts_shadow_via_drop_recreate(&self) -> Result<usize> {
        self.invalidate_fts_messages_present_cache();

        // Drop + recreate the shadow in autocommit: frankensqlite does not
        // support recreating a virtual table in the SAME transaction as its
        // DROP. The corrupt shadow is replaced by a fresh empty one here; the
        // repopulate below is transactional. Full failure-atomicity (preserving
        // the ORIGINAL) has no value in this path because the original shadow is
        // corrupt — if the repopulate fails, the shadow is empty (not corrupt)
        // and re-running the repair is safe, since it is fully derived from
        // canonical rows.
        self.conn
            .execute("DROP TABLE IF EXISTS fts_messages")
            .with_context(|| "dropping the corrupt FTS5 shadow before recreate")?;
        self.conn
            .execute_compat(FTS5_REGISTER_SQL, fparams![])
            .with_context(|| "recreating a fresh FTS5 shadow after dropping the corrupt one")?;
        self.set_fts_messages_present_cache(true);

        self.conn
            .execute_batch("BEGIN IMMEDIATE TRANSACTION;")
            .with_context(|| "starting the corrupt-FTS shadow repopulate transaction")?;
        let repopulate = (|| -> Result<usize> {
            let inserted_rows = self.stream_fts_rows_via_frankensqlite(false)?;
            self.require_healthy_fts_parity("corrupt-FTS drop+recreate rebuild")?;
            self.record_fts_franken_rebuild_generation()?;
            self.conn
                .execute_batch("COMMIT;")
                .with_context(|| "publishing the corrupt-FTS shadow repopulate")?;
            Ok(inserted_rows)
        })();

        match repopulate {
            Ok(inserted_rows) => {
                self.set_fts_messages_present_cache(true);
                Ok(inserted_rows)
            }
            Err(err) => {
                self.invalidate_fts_messages_present_cache();
                let _ = self.conn.execute_batch("ROLLBACK;");
                Err(err.context(
                    "corrupt-FTS shadow repopulate rolled back after the shadow was dropped and \
                     recreated empty; re-run 'cass doctor --rebuild-canonical-fts --yes' to retry",
                ))
            }
        }
    }

    /// True when every `sqlite_master` row named `fts_messages` declares the
    /// canonical cass registration's options, `content=''` AND
    /// `contentless_delete=1` (see [`FTS5_REGISTER_SQL`]).
    ///
    /// A `false` here means the catalog carries a CREATE cass no longer
    /// writes: a pre-contentless legacy schema (internal or external content),
    /// the `content=''`-only family cass created until 2026-08-04 (GH #497:
    /// a 2-column `_docsize` into which older engines wrote 3-column rows that
    /// the current reader rejects, wedging every ingest that touched it; it
    /// also cannot take the transactional DELETE rebuild),
    /// or a stale duplicate row left behind by an interrupted legacy
    /// migration. FrankenSQLite 0.1.19 keeps such a duplicate visible when
    /// the canonical `fts_messages_content` shadow table exists on disk, and
    /// its schema reload resolves the surviving DDL last-row-wins — so a
    /// non-contentless row can shadow canonically persisted contentless
    /// state, and rows written through one connection's view are invisible
    /// to the next open (content lands in `%_data`/`%_docsize` while the
    /// reader hydrates from the empty `%_content`).
    pub(super) fn fts_messages_schema_is_canonical_contentless(&self) -> Result<bool> {
        let mut saw_row = false;
        let mut all_contentless = true;
        self.conn
            .query_with_params_for_each(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'fts_messages'",
                &[] as &[SqliteValue],
                |row| {
                    saw_row = true;
                    let sql: Option<String> = row.get_typed(0)?;
                    let normalized: String = sql
                        .unwrap_or_default()
                        .to_ascii_lowercase()
                        .split_whitespace()
                        .collect();
                    let has_empty_content =
                        normalized.contains("content=''") || normalized.contains("content=\"\"");
                    let has_contentless_delete = normalized.contains("contentless_delete=1");
                    if !(has_empty_content && has_contentless_delete) {
                        all_contentless = false;
                    }
                    Ok(())
                },
            )
            .with_context(|| "reading the fts_messages CREATE sql for rebuild routing")?;
        Ok(saw_row && all_contentless)
    }

    pub(crate) fn rebuild_fts_via_frankensqlite(&self) -> Result<usize> {
        self.invalidate_fts_messages_present_cache();
        let mut before = self
            .inspect_search_fallback_fts_parity()
            .with_context(|| "inspecting the published FTS shadow before atomic rebuild")?;
        // GH #495: residue (shadow tables surviving without a registration, or
        // a single registration cass never writes) cannot enter the atomic
        // rebuild: a leftover `fts_messages_*` table makes the CREATE fail, and
        // frankensqlite cannot recreate a virtual table in the transaction that
        // drops it. Every direct caller (`rebuild_fts`, dedup, forget, the
        // agent-exclusion purge, reset) previously reached a drop+recreate for
        // the legacy-DDL shape, so remove the derived residue in autocommit and
        // rebuild from an absent shadow. Only derived tables are dropped;
        // canonical rows are never touched.
        if before.status == FtsShadowParityStatus::Residue {
            self.drop_fts_shadow_residue()
                .with_context(|| "removing derived FTS residue before the rebuild")?;
            before = self
                .inspect_search_fallback_fts_parity()
                .with_context(|| "inspecting the FTS shadow after residue removal")?;
            anyhow::ensure!(
                before.status == FtsShadowParityStatus::Absent,
                "FTS residue removal left a {} shadow instead of an absent one",
                before.status.as_str()
            );
        }
        // Route queryable shadows whose surviving CREATE is NOT cass's
        // canonical contentless registration through DROP+recreate instead of
        // DELETE_ALL. cass only ever creates `content='', contentless_delete=1`
        // shadows, so a non-contentless CREATE is a legacy or stale-duplicate
        // catalog row (see `fts_messages_schema_is_canonical_contentless`);
        // rebuilding "in place" through that row splits writes and reads
        // across different shadow tables and silently loses the rebuilt
        // content on the next open. DROP TABLE removes every catalog row of
        // that name plus the module shadow-table cascade, leaving one clean
        // canonical schema, and the repopulate is fully derived from
        // canonical messages so nothing user-authored is at stake. The
        // `content=''`-without-`contentless_delete` legacy family is
        // non-canonical too (GH #497); with a single catalog row it is
        // classified as residue above. Unqueryable still bails in the match
        // below.
        if matches!(
            before.status,
            FtsShadowParityStatus::Healthy
                | FtsShadowParityStatus::Partial
                | FtsShadowParityStatus::Excess
                | FtsShadowParityStatus::Divergent
        ) && !self.fts_messages_schema_is_canonical_contentless()?
        {
            return self.rebuild_fts_shadow_via_drop_recreate();
        }
        self.conn
            .execute_batch("BEGIN IMMEDIATE TRANSACTION;")
            .with_context(|| "starting failure-atomic FTS rebuild transaction")?;

        let rebuild_result = (|| -> Result<usize> {
            match before.status {
                FtsShadowParityStatus::Absent => {
                    self.conn
                        .execute_compat(FTS5_REGISTER_SQL, fparams![])
                        .with_context(|| "creating fts_messages inside atomic rebuild")?;
                }
                FtsShadowParityStatus::Residue => {
                    anyhow::bail!("FTS residue must be removed before entering the atomic rebuild")
                }
                FtsShadowParityStatus::Healthy
                | FtsShadowParityStatus::Partial
                | FtsShadowParityStatus::Excess
                | FtsShadowParityStatus::Divergent => {
                    self.conn.execute(FTS5_DELETE_ALL_SQL).with_context(|| {
                        "clearing the queryable FTS shadow transactionally; a legacy shadow \
                         without contentless_delete=1 is preserved and requires resumable \
                         repair instead"
                    })?;
                }
                FtsShadowParityStatus::Unqueryable => anyhow::bail!(
                    "refusing to replace an unqueryable FTS shadow in place because its \
                     prior artifact cannot be proven restorable; preserve the complete \
                     database bundle for staged recovery"
                ),
            }
            self.set_fts_messages_present_cache(true);
            #[cfg(test)]
            injected_fts_rebuild_interruption(1)?;

            let inserted_rows = self.stream_fts_rows_via_frankensqlite(false)?;
            self.require_healthy_fts_parity("transactional FTS rebuild")?;
            self.record_fts_franken_rebuild_generation()?;
            self.conn
                .execute_batch("COMMIT;")
                .with_context(|| "publishing failure-atomic FTS rebuild transaction")?;
            Ok(inserted_rows)
        })();

        match rebuild_result {
            Ok(inserted_rows) => {
                self.set_fts_messages_present_cache(true);
                Ok(inserted_rows)
            }
            Err(err) => {
                self.invalidate_fts_messages_present_cache();
                match self.conn.execute_batch("ROLLBACK;") {
                    Ok(()) => Err(err.context(
                        "failure-atomic FTS rebuild rolled back without publishing a partial shadow",
                    )),
                    Err(rollback_err) => {
                        tracing::error!(
                            error = %err,
                            rollback_error = %rollback_err,
                            "FTS rebuild failed and explicit rollback could not be confirmed"
                        );
                        Err(err.context(format!(
                            "FTS rebuild failed and rollback could not be confirmed: \
                             {rollback_err}. Preserve the complete database bundle and inspect \
                             exact FTS parity before any retry"
                        )))
                    }
                }
            }
        }
    }

    /// `coding_agent_session_search-uhhxy`: rebuild the in-DB FTS5 shadow
    /// (`fts_messages`) after `cass dedup --apply` drops duplicate
    /// conversation rows, so the canonical-DB-backed full-text shadow no
    /// longer references messages that were deleted. The separate on-disk
    /// lexical search index (frankensearch) is rebuilt by the blessed
    /// `cass index --full` path; this wrapper only reconciles the in-DB
    /// shadow that the dedup deletion left stale.
    pub fn rebuild_lexical_index_after_dedup(&self) -> Result<()> {
        self.rebuild_fts_via_frankensqlite().map(|_| ())
    }

    pub(super) fn stream_fts_rows_via_frankensqlite(&self, missing_only: bool) -> Result<usize> {
        let batch_size = fts_rebuild_batch_size().max(1);
        let batch_limit = i64::try_from(batch_size).unwrap_or(i64::MAX);
        let mut total_inserted: usize = 0;
        let mut total_skipped_orphans: usize = 0;
        let mut total_skipped_existing: usize = 0;
        let mut last_rowid: i64 = 0;
        let conversation_by_id = self.load_fts_conversation_projection_map()?;
        let agent_slug_by_id = self.load_fts_agent_slug_map()?;
        let workspace_path_by_id = self.load_fts_workspace_path_map()?;
        let mut entries = Vec::new();
        let mut pending_chars = 0usize;

        // Test hooks for the #439 liveness contract (tests/cli_index.rs). A
        // PARK sleeps once before the first page WITHOUT a heartbeat — the
        // shape of a genuinely wedged repair, which the watchdog must still
        // abort. A PAGE_SLEEP slows every page while the per-page heartbeat
        // keeps ticking — the shape of a slow-but-alive repair on a large
        // archive, which must never be aborted. Unset in production.
        let test_page_sleep_ms = dotenvy::var("CASS_TEST_FTS_REPAIR_PAGE_SLEEP_MS")
            .ok()
            .and_then(|raw| raw.trim().parse::<u64>().ok())
            .filter(|ms| *ms > 0);
        if let Some(park_ms) = dotenvy::var("CASS_TEST_FTS_REPAIR_PARK_MS")
            .ok()
            .and_then(|raw| raw.trim().parse::<u64>().ok())
            .filter(|ms| *ms > 0)
        {
            std::thread::sleep(Duration::from_millis(park_ms));
        }

        loop {
            let page = self.fetch_fts_rebuild_message_page(last_rowid, batch_limit)?;
            // GH #413 follow-up: the per-page budget clock starts here, so the
            // #439 PAGE_SLEEP hook below also stands in for a slow engine page.
            let page_started = Instant::now();
            // #439: every fetched page is forward progress the stall watchdog
            // cannot otherwise see.
            self.tick_fts_maintenance_heartbeat();
            if let Some(sleep_ms) = test_page_sleep_ms {
                std::thread::sleep(Duration::from_millis(sleep_ms));
            }
            let fetched_count = page.rows.len();
            if fetched_count == 0 && page.exhausted {
                break;
            }
            // Keep resumable repair bounded by the canonical page size. The
            // previous implementation loaded every existing FTS rowid into a
            // process-wide HashSet, which scaled with a multi-million-message
            // archive before the first repair batch could make progress.
            let existing_fts_rowids = if missing_only {
                Some(self.load_fts_message_rowid_set_for_batch(&page.rows)?)
            } else {
                None
            };

            let inserted_before_batch = total_inserted;
            let skipped_before_batch = total_skipped_orphans;
            let existing_before_batch = total_skipped_existing;

            for row in page.rows {
                if existing_fts_rowids
                    .as_ref()
                    .is_some_and(|rowids| rowids.contains(&row.message_id))
                {
                    total_skipped_existing = total_skipped_existing.saturating_add(1);
                    continue;
                }
                let Some(conversation) = conversation_by_id.get(&row.conversation_id) else {
                    total_skipped_orphans = total_skipped_orphans.saturating_add(1);
                    continue;
                };
                let agent = conversation
                    .agent_id
                    .and_then(|agent_id| agent_slug_by_id.get(&agent_id))
                    .filter(|slug| !slug.is_empty())
                    .cloned()
                    .unwrap_or_else(|| "unknown".to_string());
                let workspace = conversation
                    .workspace_id
                    .and_then(|workspace_id| workspace_path_by_id.get(&workspace_id))
                    .cloned()
                    .unwrap_or_default();
                pending_chars = pending_chars.saturating_add(row.content.len());
                entries.push(FtsEntry {
                    content: row.content,
                    title: conversation.title.clone(),
                    agent,
                    workspace,
                    source_path: conversation.source_path.clone(),
                    created_at: row.created_at,
                    message_id: row.message_id,
                });
                if entries.len() >= FTS_ENTRY_BATCH_MAX_DOCS
                    || pending_chars >= FTS_ENTRY_BATCH_MAX_CHARS
                {
                    total_inserted = total_inserted.saturating_add(
                        franken_batch_insert_fts_on_connection(&self.conn, &entries)?,
                    );
                    entries.clear();
                    pending_chars = 0;
                }
            }

            if !entries.is_empty() {
                total_inserted = total_inserted.saturating_add(
                    franken_batch_insert_fts_on_connection(&self.conn, &entries)?,
                );
                entries.clear();
                pending_chars = 0;
            }

            // GH #413 follow-up: a page that takes longer than the budget is
            // the engine's O(table)-per-statement wall; stop truthfully here
            // instead of wedging the run for hours. The caller's parity check
            // reports the shadow as Partial and `doctor` carries this reason.
            if let Some(budget) = fts_repair_page_budget() {
                let page_elapsed = page_started.elapsed();
                if page_elapsed > budget {
                    anyhow::bail!(
                        "fallback FTS shadow maintenance stopped: a page of {fetched_count} rows \
                         took {:.1} s, over the {} s per-page budget \
                         (CASS_FTS_REPAIR_PAGE_BUDGET_SECS); fsqlite's FTS5 does O(table) work \
                         per statement on a shadow this size (GH #413, frankensqlite#405/#406). \
                         {total_inserted} rows were streamed before stopping; the shadow stays \
                         Partial and Quill lexical search is unaffected",
                        page_elapsed.as_secs_f64(),
                        budget.as_secs()
                    );
                }
            }

            tracing::debug!(
                target: "cass::fts_rebuild",
                batch_rows = fetched_count,
                batch_inserted = total_inserted.saturating_sub(inserted_before_batch),
                batch_skipped_orphans = total_skipped_orphans.saturating_sub(skipped_before_batch),
                batch_skipped_existing = total_skipped_existing.saturating_sub(existing_before_batch),
                total_inserted,
                total_skipped_orphans,
                total_skipped_existing,
                last_rowid = page.last_rowid,
                planned_body_bytes = page.planned_body_bytes,
                oversized_single_body = page.oversized_single_body,
                missing_only,
                "FTS streaming maintenance batch complete"
            );

            // Planning and body retrieval share one savepoint snapshot, so
            // this is the exact prefix boundary whose row/byte invariants were
            // checked before any FTS insert.
            last_rowid = page.last_rowid;

            #[cfg(test)]
            emit_fts_page_diagnostics(
                self,
                &format!(
                    "stream_batch_complete last_rowid={last_rowid} total_inserted={total_inserted}"
                ),
            );

            #[cfg(test)]
            if total_inserted > 0 {
                injected_fts_rebuild_interruption(2)?;
            }

            if page.exhausted {
                break;
            }
        }

        Ok(total_inserted)
    }

    fn fetch_fts_rebuild_message_page(
        &self,
        last_rowid: i64,
        batch_limit: i64,
    ) -> Result<FtsRebuildMessagePage> {
        self.fetch_fts_rebuild_message_page_with_cap(
            last_rowid,
            batch_limit,
            FTS_ENTRY_BATCH_MAX_CHARS,
        )
    }

    pub(super) fn fetch_fts_rebuild_message_page_with_cap(
        &self,
        last_rowid: i64,
        batch_limit: i64,
        max_body_bytes: usize,
    ) -> Result<FtsRebuildMessagePage> {
        const PAGE_SAVEPOINT: &str = "cass_fts_rebuild_page";
        self.conn
            .execute("SAVEPOINT cass_fts_rebuild_page")
            .with_context(|| "starting coherent FTS page read snapshot")?;
        let page = self.fetch_fts_rebuild_message_page_in_snapshot(
            last_rowid,
            batch_limit,
            max_body_bytes,
        );
        match page {
            Ok(page) => {
                if let Err(release_err) = self.conn.execute("RELEASE cass_fts_rebuild_page") {
                    let rollback = self.conn.execute_batch(
                        "ROLLBACK TO SAVEPOINT cass_fts_rebuild_page; \
                         RELEASE SAVEPOINT cass_fts_rebuild_page;",
                    );
                    anyhow::bail!(
                        "releasing coherent FTS page read snapshot {PAGE_SAVEPOINT} failed: \
                         {release_err}; rollback={rollback:?}"
                    );
                }
                Ok(page)
            }
            Err(err) => {
                let rollback = self.conn.execute_batch(
                    "ROLLBACK TO SAVEPOINT cass_fts_rebuild_page; \
                     RELEASE SAVEPOINT cass_fts_rebuild_page;",
                );
                if let Err(rollback_err) = rollback {
                    return Err(err.context(format!(
                        "rolling back coherent FTS page read snapshot {PAGE_SAVEPOINT} also failed: {rollback_err}"
                    )));
                }
                Err(err)
            }
        }
    }

    fn fetch_fts_rebuild_message_page_in_snapshot(
        &self,
        last_rowid: i64,
        batch_limit: i64,
        max_body_bytes: usize,
    ) -> Result<FtsRebuildMessagePage> {
        // Plan the page without materializing message bodies. The previous
        // LIMIT-only query could load 5,000 arbitrarily large strings before
        // the downstream 32 MiB entry-flush cap had a chance to run.
        let headers: Vec<(i64, usize)> = self
            .conn
            .query_map_collect(
                "SELECT m.rowid, COALESCE(LENGTH(CAST(m.content AS BLOB)), 0)
                 FROM messages m
                 WHERE m.rowid > ?1
                 ORDER BY m.rowid
                 LIMIT ?2",
                fparams![last_rowid, batch_limit],
                |row| {
                    let body_bytes = row.get_typed::<i64>(1)?.max(0);
                    Ok((
                        row.get_typed(0)?,
                        usize::try_from(body_bytes).unwrap_or(usize::MAX),
                    ))
                },
            )
            .with_context(|| {
                format!("planning bounded FTS maintenance page after rowid {last_rowid}")
            })?;
        let Some(plan) = plan_fts_rebuild_message_page(&headers, max_body_bytes) else {
            return Ok(FtsRebuildMessagePage {
                rows: Vec::new(),
                last_rowid,
                exhausted: true,
                planned_body_bytes: 0,
                oversized_single_body: false,
            });
        };
        #[cfg(test)]
        run_fts_page_after_plan_hook();
        let batch_limit = usize::try_from(batch_limit).unwrap_or(usize::MAX);
        let exhausted = plan.selected_rows == headers.len() && headers.len() < batch_limit;
        let rows = self
            .conn
            .query_map_collect(
                "SELECT m.id, m.conversation_id, m.content, m.created_at
                 FROM messages m
                 WHERE m.rowid > ?1 AND m.rowid <= ?2
                 ORDER BY m.rowid
                ",
                fparams![last_rowid, plan.last_rowid],
                |row| {
                    Ok(FtsRebuildMessageRow {
                        message_id: row.get_typed(0)?,
                        conversation_id: row.get_typed(1)?,
                        content: row.get_typed::<Option<String>>(2)?.unwrap_or_default(),
                        created_at: row.get_typed(3)?,
                    })
                },
            )
            .with_context(|| {
                format!(
                    "fetching byte-bounded FTS maintenance messages after rowid {last_rowid} through {}",
                    plan.last_rowid
                )
            })?;
        let materialized_body_bytes = rows
            .iter()
            .fold(0usize, |total, row| total.saturating_add(row.content.len()));
        if rows.len() != plan.selected_rows || materialized_body_bytes != plan.body_bytes {
            anyhow::bail!(
                "coherent FTS page violated its metadata/body snapshot invariant: \
                 planned_rows={}, materialized_rows={}, planned_body_bytes={}, \
                 materialized_body_bytes={materialized_body_bytes}",
                plan.selected_rows,
                rows.len(),
                plan.body_bytes
            );
        }
        Ok(FtsRebuildMessagePage {
            rows,
            last_rowid: plan.last_rowid,
            exhausted,
            planned_body_bytes: plan.body_bytes,
            oversized_single_body: plan.body_bytes > max_body_bytes,
        })
    }

    pub(super) fn load_fts_message_rowid_set_for_batch(
        &self,
        rows: &[FtsRebuildMessageRow],
    ) -> Result<HashSet<i64>> {
        if rows.is_empty() {
            return Ok(HashSet::new());
        }

        // A rowid range is not bounded by `rows.len()`: imported archives can
        // have sparse message IDs, so MIN..MAX could materialize millions of
        // unrelated docsize rows for a 5,000-row repair page. On fsqlite
        // 0.2.1 the correct bounded shape is a chunked *bound IN-list*: the
        // planner compiles one `SeekRowid` into the shadow per element with
        // no rewind/residual scan (the plan tests pin this). The pre-0.2
        // TEMP-driver join is no longer usable: at 0.2.1, join point-seeks
        // into fts5 shadow tables silently drop sparse high rowids
        // (probe-verified against a standalone fsqlite 0.2.1 binary), while
        // IN-list point-seeks return them correctly. Work and memory stay
        // O(page size) at one dispatch per `FTS_EXACT_ROWID_PROBE_CHUNK_SIZE`
        // IDs; the ignored two-million-row regression below measures the
        // complete page.
        let mut existing = HashSet::with_capacity(rows.len());
        for chunk in rows.chunks(FTS_EXACT_ROWID_PROBE_CHUNK_SIZE) {
            let ids: Vec<i64> = chunk.iter().map(|row| row.message_id).collect();
            let sql = fts_exact_rowid_probe_select_sql(&ids);
            let matched = self
                .conn
                .query(&sql)
                .with_context(|| "probing bounded exact FTS docsize rowids")?;
            for matched_row in matched {
                existing.insert(
                    matched_row
                        .get_typed::<i64>(0)
                        .with_context(|| "decoding exact FTS docsize rowid probe result")?,
                );
            }
        }
        Ok(existing)
    }

    fn load_fts_conversation_projection_map(
        &self,
    ) -> Result<HashMap<i64, FtsConversationProjection>> {
        let rows: Vec<(i64, FtsConversationProjection)> = self
            .conn
            .query_map_collect(
                "SELECT id, title, agent_id, workspace_id, source_path
                 FROM conversations",
                fparams![],
                |row| {
                    Ok((
                        row.get_typed(0)?,
                        FtsConversationProjection {
                            title: row.get_typed::<Option<String>>(1)?.unwrap_or_default(),
                            agent_id: row.get_typed(2)?,
                            workspace_id: row.get_typed(3)?,
                            source_path: row.get_typed::<Option<String>>(4)?.unwrap_or_default(),
                        },
                    ))
                },
            )
            .with_context(|| "loading FTS conversation projection map")?;
        Ok(rows.into_iter().collect())
    }

    fn load_fts_agent_slug_map(&self) -> Result<HashMap<i64, String>> {
        let rows: Vec<(i64, String)> = self
            .conn
            .query_map_collect("SELECT id, slug FROM agents", fparams![], |row| {
                Ok((
                    row.get_typed(0)?,
                    row.get_typed::<Option<String>>(1)?
                        .unwrap_or_else(|| "unknown".to_string()),
                ))
            })
            .with_context(|| "loading FTS agent slug map")?;
        Ok(rows.into_iter().collect())
    }

    fn load_fts_workspace_path_map(&self) -> Result<HashMap<i64, String>> {
        let rows: Vec<(i64, String)> = self
            .conn
            .query_map_collect("SELECT id, path FROM workspaces", fparams![], |row| {
                Ok((
                    row.get_typed(0)?,
                    row.get_typed::<Option<String>>(1)?.unwrap_or_default(),
                ))
            })
            .with_context(|| "loading FTS workspace path map")?;
        Ok(rows.into_iter().collect())
    }
}

// -------------------------------------------------------------------------
// FTS5 Batch Insert (P2 Opt 2.1)
// -------------------------------------------------------------------------

/// Rows per FTS5 INSERT statement during db-resident `fts_messages`
/// maintenance/rebuild. Each row binds 7 columns (rowid + 6 cols), and
/// frankensqlite's `MAX_VARIABLE_NUMBER` is 32766, so the hard ceiling is
/// 32766 / 7 = 4680 rows per statement; 4096 leaves margin (28672 params).
///
/// This value is performance-critical, NOT just a memory knob
/// (`coding_agent_session_search-nhqw0` / gh #301): `fts_messages` is a
/// contentless FTS5 table (`content=''`), which routes every INSERT through
/// frankensqlite's `persist_rootpage_zero_fts5_shadow_rows` full-table
/// re-encode (`build_pending_hash` re-tokenizes *all* rows). The cost of one
/// INSERT is therefore O(rows-so-far) regardless of the statement's row count,
/// so the total rebuild cost is (number of INSERT statements) × O(table). The
/// old value of 100 fragmented a single ~512-row flush into ~6 statements and
/// turned a multi-MB rebuild into ~120 whole-table re-encodes (O(N²)), which
/// wedged `cass index --full` above ~15-30 MB of content. Issuing one large
/// param-safe statement per flush collapses that to a handful of re-encodes.
/// The asymptotic fix (incremental contentless persistence) is tracked in
/// frankensqlite bd-sf8dx.
pub(super) const FTS5_BATCH_SIZE: usize = 4096;

#[derive(Debug, Clone)]
pub(super) struct FtsRebuildMessageRow {
    pub(super) message_id: i64,
    pub(super) conversation_id: i64,
    pub(super) content: String,
    pub(super) created_at: Option<i64>,
}

#[derive(Debug)]
pub(super) struct FtsRebuildMessagePage {
    pub(super) rows: Vec<FtsRebuildMessageRow>,
    pub(super) last_rowid: i64,
    pub(super) exhausted: bool,
    pub(super) planned_body_bytes: usize,
    pub(super) oversized_single_body: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct FtsRebuildPagePlan {
    pub(super) last_rowid: i64,
    pub(super) selected_rows: usize,
    pub(super) body_bytes: usize,
}

#[derive(Debug, Clone)]
pub(super) struct FtsConversationProjection {
    pub(super) title: String,
    pub(super) agent_id: Option<i64>,
    pub(super) workspace_id: Option<i64>,
    pub(super) source_path: String,
}

/// Entry for pending FTS5 insert.
#[derive(Debug, Clone)]
pub struct FtsEntry {
    pub content: String,
    pub title: String,
    pub agent: String,
    pub workspace: String,
    pub source_path: String,
    pub created_at: Option<i64>,
    pub message_id: i64,
}

impl FtsEntry {
    /// Create an FTS entry from a message and conversation.
    pub fn from_message(message_id: i64, msg: &Message, conv: &Conversation) -> Self {
        FtsEntry {
            content: msg.content.clone(),
            title: conv.title.clone().unwrap_or_default(),
            agent: conv.agent_slug.clone(),
            workspace: conv
                .workspace
                .as_ref()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_default(),
            source_path: path_to_string(&conv.source_path),
            created_at: msg.created_at.or(conv.started_at),
            message_id,
        }
    }
}

// Per-flush accumulation caps for the streaming FTS rebuild/maintenance paths.
// A flush is emitted once either cap is hit, then handed to
// `franken_batch_insert_fts_on_connection`, which splits it into
// `FTS5_BATCH_SIZE`-row INSERT statements. Because each contentless-FTS INSERT
// re-encodes the whole table (see `FTS5_BATCH_SIZE` above /
// `coding_agent_session_search-nhqw0`), these caps must be large enough that a
// flush is a single param-safe statement — keeping the doc cap at/under
// `FTS5_BATCH_SIZE` means one flush == one INSERT == one re-encode. The
// historically named `MAX_CHARS` cap is intentionally measured with
// `String::len()` / `LENGTH(CAST(... AS BLOB))`: it is a UTF-8 byte cap, not a
// Unicode scalar count, and bounds message-body buffer memory.
pub(super) const FTS_ENTRY_BATCH_MAX_DOCS: usize = 4000;
pub(super) const FTS_ENTRY_BATCH_MAX_CHARS: usize = 32 * 1024 * 1024;

/// Default batch size for the FTS rebuild INSERT (Bug #168).  When
/// `fts_messages` is empty but `messages` has 100K+ rows, a single unbounded
/// INSERT-SELECT OOMs.  This constant caps each batch so peak memory stays
/// bounded.  Override via `CASS_FTS_REBUILD_BATCH_SIZE` for tuning.
const FTS_REBUILD_BATCH_SIZE_DEFAULT: usize = 5_000;

/// Bound parameter count and SQL text while loading the TEMP driver used for
/// exact docsize membership probes. The page itself remains capped at 5,000.
pub(super) const FTS_EXACT_ROWID_PROBE_CHUNK_SIZE: usize = 512;

/// Choose the largest ordered prefix whose message bodies fit `max_body_bytes`.
///
/// At least one row is selected whenever input is non-empty. That single-row
/// escape hatch is necessary for a canonical message that is itself larger
/// than the cap: correctness forbids truncation, while selecting it alone
/// ensures the cursor still progresses and bounds all other pages.
pub(super) fn plan_fts_rebuild_message_page(
    headers: &[(i64, usize)],
    max_body_bytes: usize,
) -> Option<FtsRebuildPagePlan> {
    let mut selected_rows = 0usize;
    let mut body_bytes = 0usize;
    let mut last_rowid = 0i64;
    for &(rowid, row_body_bytes) in headers {
        if selected_rows > 0 && body_bytes.saturating_add(row_body_bytes) > max_body_bytes {
            break;
        }
        selected_rows = selected_rows.saturating_add(1);
        body_bytes = body_bytes.saturating_add(row_body_bytes);
        last_rowid = rowid;
    }
    (selected_rows > 0).then_some(FtsRebuildPagePlan {
        last_rowid,
        selected_rows,
        body_bytes,
    })
}

/// Literal (not bound) rowid IN-list: on fsqlite 0.2.1 a literal IN-list
/// compiles to one `SeekRowid` per element, while a bound-parameter IN-list
/// still compiles to a full rewind + residual-filter scan of the shadow.
/// Message rowids are `i64`, so inlining them is injection-safe.
pub(super) fn fts_exact_rowid_probe_select_sql(ids: &[i64]) -> String {
    debug_assert!(!ids.is_empty() && ids.len() <= FTS_EXACT_ROWID_PROBE_CHUNK_SIZE);
    let mut sql = String::from("SELECT id FROM fts_messages_docsize WHERE id IN (");
    for (index, id) in ids.iter().enumerate() {
        if index > 0 {
            sql.push(',');
        }
        sql.push_str(&id.to_string());
    }
    sql.push(')');
    sql
}

#[cfg(test)]
std::thread_local! {
    static TEST_FTS_REBUILD_INTERRUPTION_PHASE: std::cell::Cell<u8> = const { std::cell::Cell::new(0) };
    static TEST_FTS_PAGE_AFTER_PLAN_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        std::cell::RefCell::new(None);
    static TEST_FTS_PAGE_DIAGNOSTIC_PATH: std::cell::RefCell<Option<PathBuf>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(super) fn arm_fts_page_diagnostics(db_path: &Path) {
    TEST_FTS_PAGE_DIAGNOSTIC_PATH.with(|path| {
        *path.borrow_mut() = Some(db_path.to_path_buf());
    });
}

#[cfg(test)]
pub(super) fn emit_fts_page_diagnostics(storage: &FrankenStorage, stage: &str) {
    let enabled = TEST_FTS_PAGE_DIAGNOSTIC_PATH.with(|path| {
        path.borrow()
            .as_deref()
            .is_some_and(|path| path == storage.db_path)
    });
    if !enabled {
        return;
    }

    let query_i64 = |sql: &str| {
        storage
            .raw()
            .query_row_map(sql, fparams![], |row| row.get_typed::<i64>(0))
            .map_err(|error| error.to_string())
    };
    let page_count = query_i64("PRAGMA page_count");
    let page_size = query_i64("PRAGMA page_size");
    let temp_probe_root = query_i64(
        "SELECT rootpage FROM temp.sqlite_master WHERE name = 'cass_fts_repair_probe_ids'",
    );
    let docsize_root =
        query_i64("SELECT rootpage FROM main.sqlite_master WHERE name = 'fts_messages_docsize'");
    let file_len = std::fs::metadata(&storage.db_path)
        .map(|metadata| metadata.len())
        .map_err(|error| error.to_string());
    let header_page_count = (|| -> std::io::Result<u32> {
        use std::io::Read as _;

        let mut file = std::fs::File::open(&storage.db_path)?;
        let mut header = [0_u8; 32];
        file.read_exact(&mut header)?;
        Ok(u32::from_be_bytes([
            header[28], header[29], header[30], header[31],
        ]))
    })()
    .map_err(|error| error.to_string());

    eprintln!(
        "CASS_FTS_PAGE_DIAG stage={stage:?} pragma_page_count={page_count:?} \
         pragma_page_size={page_size:?} header_page_count={header_page_count:?} \
         file_len={file_len:?} temp_probe_root={temp_probe_root:?} \
         docsize_root={docsize_root:?}"
    );
}

#[cfg(test)]
pub(super) fn arm_fts_rebuild_interruption(phase: u8) {
    TEST_FTS_REBUILD_INTERRUPTION_PHASE.with(|armed| armed.set(phase));
}

#[cfg(test)]
fn injected_fts_rebuild_interruption(phase: u8) -> Result<()> {
    let should_interrupt = TEST_FTS_REBUILD_INTERRUPTION_PHASE.with(|armed| {
        if armed.get() == phase {
            armed.set(0);
            true
        } else {
            false
        }
    });
    if should_interrupt {
        anyhow::bail!("injected FTS rebuild interruption at test phase {phase}");
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn arm_fts_page_after_plan_hook(hook: impl FnOnce() + 'static) {
    TEST_FTS_PAGE_AFTER_PLAN_HOOK.with(|slot| {
        let previous = slot.replace(Some(Box::new(hook)));
        assert!(previous.is_none(), "FTS page test hook already armed");
    });
}

#[cfg(test)]
fn run_fts_page_after_plan_hook() {
    TEST_FTS_PAGE_AFTER_PLAN_HOOK.with(|slot| {
        if let Some(hook) = slot.borrow_mut().take() {
            hook();
        }
    });
}

/// Read the FTS rebuild batch size from the environment, falling back to the
/// compiled-in default.
fn fts_rebuild_batch_size() -> usize {
    dotenvy::var("CASS_FTS_REBUILD_BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(FTS_REBUILD_BATCH_SIZE_DEFAULT)
}

pub(super) fn flush_pending_fts_entries(
    storage: &FrankenStorage,
    tx: &FrankenTransaction<'_>,
    entries: &mut Vec<FtsEntry>,
    pending_chars: &mut usize,
    inserted_total: &mut usize,
) -> Result<()> {
    if entries.is_empty() {
        return Ok(());
    }

    // GH #413 follow-up: once this run's shadow writes blew their budget the
    // canonical rows still land and the shadow is left Partial on purpose.
    if storage.fts_inline_writes_suspended() {
        entries.clear();
        *pending_chars = 0;
        return Ok(());
    }

    if storage.fts_messages_present_cached(tx) {
        // GH #413 follow-up (iify0): a run that grows the corpus past the
        // shadow bound stops feeding the shadow here and leaves the drop to
        // the run's finalize step (see `fts_shadow_max_messages`).
        if let Some(bound_messages) = fts_shadow_max_messages() {
            let batch_messages = u64::try_from(entries.len()).unwrap_or(u64::MAX);
            let seen = storage
                .fts_shadow_run
                .messages_seen
                .fetch_add(batch_messages, Ordering::SeqCst)
                .saturating_add(batch_messages);
            if seen > bound_messages {
                let detail = fts_shadow_not_viable_detail(seen, bound_messages);
                storage
                    .fts_shadow_run
                    .drop_pending
                    .store(true, Ordering::SeqCst);
                if storage.suspend_fts_inline_writes(detail) {
                    tracing::warn!(
                        target: "cass::fts_inline",
                        corpus_messages_seen = seen,
                        bound_messages,
                        "inline fallback-FTS shadow writes suspended: the corpus crossed the shadow bound; the shadow is dropped at finalize"
                    );
                }
                entries.clear();
                *pending_chars = 0;
                return Ok(());
            }
        }
        let started = Instant::now();
        *inserted_total += franken_batch_insert_fts(storage, tx, entries)?;
        // Test hook (tests/cli_index.rs): make a flush look as slow as the
        // engine does on a huge shadow. Unset in production.
        if let Some(park_ms) = dotenvy::var("CASS_TEST_FTS_INLINE_FLUSH_PARK_MS")
            .ok()
            .and_then(|raw| raw.trim().parse::<u64>().ok())
            .filter(|ms| *ms > 0)
        {
            std::thread::sleep(Duration::from_millis(park_ms));
        }
        storage.charge_fts_inline_flush(started.elapsed(), entries.len());
    }
    entries.clear();
    *pending_chars = 0;
    Ok(())
}

pub(super) fn path_to_string<P: AsRef<Path>>(p: P) -> String {
    p.as_ref().to_string_lossy().into_owned()
}

pub(super) fn role_str(role: &MessageRole) -> String {
    role_as_str(role).to_owned()
}

pub(super) fn role_as_str(role: &MessageRole) -> &str {
    match role {
        MessageRole::User => "user",
        MessageRole::Agent => "agent",
        MessageRole::Tool => "tool",
        MessageRole::System => "system",
        MessageRole::Other(v) => v.as_str(),
    }
}

pub(super) fn agent_kind_str(kind: AgentKind) -> String {
    match kind {
        AgentKind::Cli => "cli".into(),
        AgentKind::VsCode => "vscode".into(),
        AgentKind::Hybrid => "hybrid".into(),
    }
}
