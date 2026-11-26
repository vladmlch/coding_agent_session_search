use anyhow::{Result, anyhow};
use std::cmp::Ordering as CmpOrdering;
use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::franken_sync::compat::{ParamValue, RowExt};
use crate::sources::provenance::SourceFilter;

use super::fusion::{
    normalize_search_source_filter_value, normalized_search_hit_origin_kind,
    normalized_search_hit_source_id_parts, normalized_search_origin_kind_sql_expr,
    normalized_search_source_id_sql_expr, snippet_from_content, snippet_from_content_for_query,
    stable_hit_hash,
};
use super::parsing::{dominant_match_type, transpile_to_fts5};
use super::sqlite_hydration::{
    SQLITE_FTS5_HYDRATE_PARAM_CHUNK, SQLITE_FTS5_POST_FILTER_SCAN_CHUNK,
    SQLITE_FTS5_POST_FILTER_SCAN_LIMIT, SQLITE_MAX_VARIABLE_NUMBER,
    SQLITE_MESSAGE_SCAN_FALLBACK_PAGE_ROWS, SearchSqliteConnectionExt, SqliteFtsHydratedRow,
    SqliteFtsMessageRow, franken_query_map_collect_retry, sql_placeholders, sql_rowid_literal_list,
};
use super::sqlite_scan::SqliteMessageScanRequest;
use super::types::{FieldMask, MatchType, SearchFilters, SearchHit, default_source_id};
use super::{SearchClient, SearchSqliteConnection};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::search::query) enum SqliteFtsMatchMode {
    Table,
    IndexedColumns,
}

impl SearchClient {
    pub(in crate::search::query) fn sqlite_fts_uses_message_id_column(
        conn: &SearchSqliteConnection,
    ) -> Result<bool> {
        let params: [ParamValue; 0] = [];
        let ddl_rows: Vec<String> = franken_query_map_collect_retry(
            conn,
            "SELECT COALESCE(sql, '')
             FROM sqlite_master
             WHERE name = 'fts_messages'
             ORDER BY rowid DESC
             LIMIT 1",
            &params,
            |row: &crate::franken_sync::Row| row.get_typed::<String>(0),
        )?;
        Ok(ddl_rows
            .first()
            .map(|sql| sql.to_ascii_lowercase().contains("message_id"))
            .unwrap_or(false))
    }

    pub(in crate::search::query) fn sqlite_fts_has_metadata_columns(
        conn: &SearchSqliteConnection,
    ) -> Result<bool> {
        let params: [ParamValue; 0] = [];
        let ddl_rows: Vec<String> = franken_query_map_collect_retry(
            conn,
            "SELECT COALESCE(sql, '')
             FROM sqlite_master
             WHERE name = 'fts_messages'
             ORDER BY rowid DESC
             LIMIT 1",
            &params,
            |row: &crate::franken_sync::Row| row.get_typed::<String>(0),
        )?;
        Ok(ddl_rows
            .first()
            .map(|sql| {
                let lower = sql.to_ascii_lowercase();
                lower.contains("agent")
                    && lower.contains("workspace")
                    && lower.contains("source_path")
                    && lower.contains("created_at")
            })
            .unwrap_or(false))
    }

    pub(in crate::search::query) fn sqlite_fts_match_mode(
        conn: &SearchSqliteConnection,
    ) -> Result<SqliteFtsMatchMode> {
        let params = [ParamValue::from("__cass_fts_probe_no_match__")];
        match franken_query_map_collect_retry(
            conn,
            "SELECT COUNT(*) FROM fts_messages WHERE fts_messages MATCH ?",
            &params,
            |row: &crate::franken_sync::Row| row.get_typed::<i64>(0),
        ) {
            Ok(_) => Ok(SqliteFtsMatchMode::Table),
            Err(err)
                if err
                    .to_string()
                    .contains("no such column: fts_messages in table fts_messages") =>
            {
                Ok(SqliteFtsMatchMode::IndexedColumns)
            }
            Err(err) => Err(anyhow!(err)),
        }
    }

    pub(in crate::search::query) fn sqlite_fts5_rowid_projection_available(
        conn: &SearchSqliteConnection,
    ) -> bool {
        let params: [ParamValue; 0] = [];
        franken_query_map_collect_retry(
            conn,
            "SELECT rowid FROM fts_messages LIMIT 1",
            &params,
            |row: &crate::franken_sync::Row| row.get_typed::<i64>(0),
        )
        .is_ok()
    }

    /// k0lo1: the SQLite FTS lane must honour the same field contract as the
    /// primary lexical index (title + content only). Table-wide MATCH used to
    /// search agent/workspace/source_path too, so a query term that exists
    /// only in metadata became a false-positive hit whenever the primary
    /// index was unavailable. Those fields remain filterable through the
    /// ordinary structured filters; they are simply not query text.
    pub(in crate::search::query) fn sqlite_fts5_match_clause(
        match_mode: SqliteFtsMatchMode,
    ) -> &'static str {
        match match_mode {
            SqliteFtsMatchMode::Table => "fts_messages MATCH ?",
            SqliteFtsMatchMode::IndexedColumns => "(content MATCH ? OR title MATCH ?)",
        }
    }

    /// FTS5 column-filter prefix restricting a table-wide MATCH to the
    /// primary lexical fields (`{content title} : (<query>)`).
    pub(in crate::search::query) fn sqlite_fts5_scoped_table_query(fts_query: &str) -> String {
        if fts_query.trim().is_empty() {
            return fts_query.to_string();
        }
        format!("{{content title}} : ({fts_query})")
    }

    pub(in crate::search::query) fn push_sqlite_fts5_match_params(
        params: &mut Vec<ParamValue>,
        fts_query: &str,
        match_mode: SqliteFtsMatchMode,
    ) {
        match match_mode {
            SqliteFtsMatchMode::Table => {
                params.push(ParamValue::from(Self::sqlite_fts5_scoped_table_query(
                    fts_query,
                )));
            }
            SqliteFtsMatchMode::IndexedColumns => {
                for _ in 0..2 {
                    params.push(ParamValue::from(fts_query));
                }
            }
        }
    }

    pub(in crate::search::query) fn sqlite_fts5_rank_query(
        fts_query: &str,
        _filters: &SearchFilters,
        limit: usize,
        offset: usize,
        _uses_message_id: bool,
        match_mode: SqliteFtsMatchMode,
    ) -> (String, Vec<ParamValue>) {
        let match_clause = Self::sqlite_fts5_match_clause(match_mode);
        let mut sql = format!(
            "SELECT rowid,
                    bm25(fts_messages)
             FROM fts_messages
             WHERE {match_clause}"
        );
        let mut params = Vec::with_capacity(9);
        Self::push_sqlite_fts5_match_params(&mut params, fts_query, match_mode);

        sql.push_str(" ORDER BY bm25(fts_messages), rowid LIMIT ? OFFSET ?");
        params.push(ParamValue::from(limit as i64));
        params.push(ParamValue::from(offset as i64));

        (sql, params)
    }

    pub(in crate::search::query) fn sqlite_fts5_hydrate_query(
        row_count: usize,
        field_mask: FieldMask,
        uses_message_id: bool,
    ) -> String {
        let title_expr = if field_mask.wants_title() {
            "fts_messages.title"
        } else {
            "NULL"
        };
        let content_expr = if field_mask.needs_content() || field_mask.wants_snippet() {
            "fts_messages.content"
        } else {
            "NULL"
        };
        let message_key_expr = if uses_message_id {
            "CAST(fts_messages.message_id AS INTEGER)"
        } else {
            "rowid"
        };
        let placeholders = sql_placeholders(row_count);

        format!(
            "SELECT rowid,
                    {message_key_expr},
                    {title_expr},
                    {content_expr},
                    fts_messages.agent,
                    fts_messages.workspace,
                    fts_messages.source_path,
                    CAST(fts_messages.created_at AS INTEGER)
             FROM fts_messages
             WHERE rowid IN ({placeholders})"
        )
    }

    pub(in crate::search::query) fn sqlite_fts5_message_hydrate_query(
        message_ids: &[i64],
        field_mask: FieldMask,
    ) -> String {
        let title_expr = if field_mask.wants_title() {
            "COALESCE(c.title, '')"
        } else {
            "''"
        };
        let content_expr = if field_mask.needs_content() || field_mask.wants_snippet() {
            "COALESCE(m.content, '')"
        } else {
            "''"
        };
        let normalized_source_sql =
            normalized_search_source_id_sql_expr("c.source_id", "s.kind", "c.origin_host");
        // Literal rowid list (see `sql_rowid_literal_list`).
        let rowids = sql_rowid_literal_list(message_ids);

        format!(
            "SELECT m.id,
                    {title_expr},
                    {content_expr},
                    COALESCE(a.slug, ''),
                    COALESCE(w.path, ''),
                    COALESCE(c.source_path, ''),
                    CAST(m.created_at AS INTEGER),
                    m.idx,
                    c.id,
                    {normalized_source_sql},
                    c.origin_host,
                    s.kind
             FROM messages m
             LEFT JOIN conversations c ON m.conversation_id = c.id
             LEFT JOIN sources s ON c.source_id = s.id
             LEFT JOIN agents a ON c.agent_id = a.id
             LEFT JOIN workspaces w ON c.workspace_id = w.id
             WHERE m.id IN ({rowids})"
        )
    }

    pub(in crate::search::query) fn sqlite_fts5_hydrate_row_chunks(
        ranked_rows: &[(i64, f64)],
    ) -> impl Iterator<Item = &[(i64, f64)]> {
        const _: () = assert!(SQLITE_FTS5_HYDRATE_PARAM_CHUNK <= SQLITE_MAX_VARIABLE_NUMBER);
        ranked_rows.chunks(SQLITE_FTS5_HYDRATE_PARAM_CHUNK)
    }

    pub(in crate::search::query) fn sqlite_fts5_filters_need_post_hydration(
        filters: &SearchFilters,
    ) -> bool {
        !filters.agents.is_empty()
            || !filters.workspaces.is_empty()
            || filters.created_from.is_some()
            || filters.created_to.is_some()
            || !filters.source_filter.is_all()
            || !filters.session_paths.is_empty()
    }

    pub(in crate::search::query) fn sqlite_fts5_hit_matches_filters(
        hit: &SearchHit,
        filters: &SearchFilters,
    ) -> bool {
        if !filters.agents.is_empty() && !filters.agents.contains(&hit.agent) {
            return false;
        }
        if !filters.workspaces.is_empty() && !filters.workspaces.contains(&hit.workspace) {
            return false;
        }
        if filters.created_from.is_some() || filters.created_to.is_some() {
            let Some(created_at) = hit.created_at else {
                return false;
            };
            if let Some(created_from) = filters.created_from
                && created_at < created_from
            {
                return false;
            }
            if let Some(created_to) = filters.created_to
                && created_at > created_to
            {
                return false;
            }
        }
        if !filters.session_paths.is_empty() && !filters.session_paths.contains(&hit.source_path) {
            return false;
        }

        match &filters.source_filter {
            SourceFilter::All => true,
            // Kind, not id: a named local-kind source (backup root,
            // chatgpt-import) is local too (bead 5bf29).
            SourceFilter::Local => hit.origin_kind == crate::sources::provenance::LOCAL_SOURCE_ID,
            SourceFilter::Remote => hit.origin_kind != crate::sources::provenance::LOCAL_SOURCE_ID,
            SourceFilter::SourceId(id) => {
                let normalized = normalize_search_source_filter_value(id);
                matches!(
                    hit.source_id.as_str().cmp(normalized.as_str()),
                    CmpOrdering::Equal
                )
            }
        }
    }

    pub(in crate::search::query) fn search_sqlite_fts5(
        &self,
        _db_path: &Path,
        raw_query: &str,
        filters: SearchFilters,
        limit: usize,
        offset: usize,
        field_mask: FieldMask,
    ) -> Result<Vec<SearchHit>> {
        if limit < 1 {
            return Ok(Vec::new());
        }

        let sqlite_guard = self.sqlite_guard()?;
        let Some(conn) = sqlite_guard.as_ref() else {
            return Ok(Vec::new());
        };

        let query_match_type = dominant_match_type(raw_query);
        let scan_request = SqliteMessageScanRequest {
            raw_query,
            filters: &filters,
            limit,
            offset,
            scan_page_rows: SQLITE_MESSAGE_SCAN_FALLBACK_PAGE_ROWS,
            field_mask,
            query_match_type,
        };
        let fts_query = match transpile_to_fts5(raw_query) {
            Some(q) if !q.trim().is_empty() => q,
            _ => {
                tracing::debug!(
                    query = raw_query,
                    "query is not faithfully representable in FTS5; using source-table scan fallback"
                );
                return self.search_sqlite_message_scan(conn, scan_request);
            }
        };

        let empty_params: [ParamValue; 0] = [];
        let has_fts = franken_query_map_collect_retry(
            conn,
            "SELECT 1 FROM sqlite_master WHERE name = 'fts_messages'",
            &empty_params,
            |row| row.get_typed::<i64>(0),
        )
        .map(|rows| !rows.is_empty())
        .unwrap_or(false);
        if !has_fts {
            return self.search_sqlite_message_scan(conn, scan_request);
        }
        if let Err(err) =
            crate::storage::sqlite::validate_fts_messages_integrity_for_async_connection(conn)
        {
            tracing::warn!(
                error = %err,
                "sqlite FTS fallback integrity check failed; using source-table scan fallback"
            );
            return self.search_sqlite_message_scan(conn, scan_request);
        }
        let uses_message_id =
            if let Ok(uses_message_id) = Self::sqlite_fts_uses_message_id_column(conn) {
                uses_message_id
            } else {
                tracing::warn!(
                    "sqlite FTS fallback is present but not queryable; skipping fallback search"
                );
                return self.search_sqlite_message_scan(conn, scan_request);
            };
        let has_metadata_columns = if let Ok(has_metadata) =
            Self::sqlite_fts_has_metadata_columns(conn)
        {
            has_metadata
        } else {
            tracing::warn!(
                "sqlite FTS fallback metadata columns could not be inspected; skipping fallback search"
            );
            return self.search_sqlite_message_scan(conn, scan_request);
        };
        let match_mode = match Self::sqlite_fts_match_mode(conn) {
            Ok(match_mode) => match_mode,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "sqlite FTS fallback is present but not queryable; skipping fallback search"
                );
                return self.search_sqlite_message_scan(conn, scan_request);
            }
        };
        if !Self::sqlite_fts5_rowid_projection_available(conn) {
            tracing::warn!(
                "sqlite FTS fallback cannot project rowid; using source-table scan fallback"
            );
            return self.search_sqlite_message_scan(conn, scan_request);
        }

        let post_filter = Self::sqlite_fts5_filters_need_post_hydration(&filters);
        let target_hits = if post_filter {
            offset.saturating_add(limit)
        } else {
            limit
        };
        let rank_batch_limit = if post_filter {
            target_hits.clamp(1, SQLITE_FTS5_POST_FILTER_SCAN_CHUNK)
        } else {
            limit
        };
        let mut rank_offset = if post_filter { 0 } else { offset };
        let mut scanned_rows = 0usize;
        let mut hits = Vec::with_capacity(target_hits.min(rank_batch_limit));

        loop {
            let (rank_sql, rank_params) = Self::sqlite_fts5_rank_query(
                fts_query.as_str(),
                &filters,
                rank_batch_limit,
                rank_offset,
                uses_message_id,
                match_mode,
            );
            let ranked_rows: Vec<(i64, f64)> =
                match franken_query_map_collect_retry(conn, &rank_sql, &rank_params, |row| {
                    Ok((row.get_typed(0)?, row.get_typed(1)?))
                }) {
                    Ok(rows) => rows,
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            "sqlite FTS fallback rank query failed; returning no fallback hits"
                        );
                        return self.search_sqlite_message_scan(conn, scan_request);
                    }
                };
            if ranked_rows.is_empty() {
                break;
            }

            scanned_rows = scanned_rows.saturating_add(ranked_rows.len());
            let bm25_by_rowid: HashMap<i64, f64> = ranked_rows.iter().copied().collect();
            let mut fts_rows_by_rowid = HashMap::with_capacity(ranked_rows.len());
            let mut message_ids = Vec::with_capacity(ranked_rows.len());
            let mut seen_message_ids = HashSet::with_capacity(ranked_rows.len());

            if has_metadata_columns {
                for rank_chunk in Self::sqlite_fts5_hydrate_row_chunks(&ranked_rows) {
                    let hydrate_sql = Self::sqlite_fts5_hydrate_query(
                        rank_chunk.len(),
                        field_mask,
                        uses_message_id,
                    );
                    let hydrate_params = rank_chunk
                        .iter()
                        .map(|(fts_rowid, _)| ParamValue::from(*fts_rowid))
                        .collect::<Vec<_>>();
                    let rows: Vec<SqliteFtsHydratedRow> = match franken_query_map_collect_retry(
                        conn,
                        &hydrate_sql,
                        &hydrate_params,
                        |row| {
                            Ok((
                                row.get_typed(0)?,
                                row.get_typed(1)?,
                                row.get_typed(2)?,
                                row.get_typed(3)?,
                                row.get_typed(4)?,
                                row.get_typed(5)?,
                                row.get_typed(6)?,
                                row.get_typed(7)?,
                            ))
                        },
                    ) {
                        Ok(rows) => rows,
                        Err(err) => {
                            tracing::warn!(
                                error = %err,
                                "sqlite FTS fallback rowid hydration query failed; returning no fallback hits"
                            );
                            return self.search_sqlite_message_scan(conn, scan_request);
                        }
                    };

                    for row in rows {
                        let fts_rowid = row.0;
                        let message_id = row.1.unwrap_or(fts_rowid);
                        if seen_message_ids.insert(message_id) {
                            message_ids.push(message_id);
                        }
                        fts_rows_by_rowid.insert(fts_rowid, row);
                    }
                }
            } else {
                for &(fts_rowid, _) in &ranked_rows {
                    if seen_message_ids.insert(fts_rowid) {
                        message_ids.push(fts_rowid);
                    }
                    fts_rows_by_rowid.insert(
                        fts_rowid,
                        (fts_rowid, None, None, None, None, None, None, None),
                    );
                }
            }

            let mut metadata_by_message_id = HashMap::with_capacity(message_ids.len());
            for message_chunk in message_ids.chunks(SQLITE_FTS5_HYDRATE_PARAM_CHUNK) {
                let metadata_sql =
                    Self::sqlite_fts5_message_hydrate_query(message_chunk, field_mask);
                let metadata_rows: Vec<SqliteFtsMessageRow> = match franken_query_map_collect_retry(
                    conn,
                    &metadata_sql,
                    &[],
                    |row| {
                        Ok((
                            row.get_typed(0)?,
                            row.get_typed(1)?,
                            row.get_typed(2)?,
                            row.get_typed(3)?,
                            row.get_typed(4)?,
                            row.get_typed(5)?,
                            row.get_typed(6)?,
                            row.get_typed(7)?,
                            row.get_typed(8)?,
                            row.get_typed::<Option<String>>(9)?,
                            row.get_typed(10)?,
                            row.get_typed(11)?,
                        ))
                    },
                ) {
                    Ok(rows) => rows,
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            "sqlite FTS fallback message hydration query failed; returning no fallback hits"
                        );
                        return self.search_sqlite_message_scan(conn, scan_request);
                    }
                };
                metadata_by_message_id.extend(metadata_rows.into_iter().map(|row| (row.0, row)));
            }

            let mut hits_by_rowid = HashMap::with_capacity(ranked_rows.len());
            for (
                fts_rowid,
                fts_message_id,
                fts_title,
                fts_content,
                fts_agent,
                fts_workspace,
                fts_source_path,
                fts_created_at,
            ) in fts_rows_by_rowid.into_values()
            {
                let Some(&bm25_score) = bm25_by_rowid.get(&fts_rowid) else {
                    continue;
                };
                let message_id = fts_message_id.unwrap_or(fts_rowid);
                let (
                    title,
                    raw_content,
                    agent,
                    workspace,
                    source_path,
                    created_at,
                    idx,
                    conversation_id,
                    raw_source_id,
                    origin_host,
                    raw_origin_kind,
                ) = match metadata_by_message_id.remove(&message_id) {
                    Some((
                        _,
                        metadata_title,
                        metadata_content,
                        metadata_agent,
                        metadata_workspace,
                        metadata_source_path,
                        metadata_created_at,
                        metadata_idx,
                        metadata_conversation_id,
                        metadata_raw_source_id,
                        metadata_origin_host,
                        metadata_raw_origin_kind,
                    )) => (
                        if metadata_title.is_empty() {
                            fts_title.unwrap_or_default()
                        } else {
                            metadata_title
                        },
                        if metadata_content.is_empty() {
                            fts_content.unwrap_or_default()
                        } else {
                            metadata_content
                        },
                        if metadata_agent.is_empty() {
                            fts_agent.unwrap_or_default()
                        } else {
                            metadata_agent
                        },
                        // Canonical NULL is authoritative too: a legacy
                        // content-bearing FTS row can retain an old attribution.
                        metadata_workspace,
                        if metadata_source_path.is_empty() {
                            fts_source_path.unwrap_or_default()
                        } else {
                            metadata_source_path
                        },
                        metadata_created_at.or(fts_created_at),
                        metadata_idx,
                        metadata_conversation_id,
                        metadata_raw_source_id.unwrap_or_else(default_source_id),
                        metadata_origin_host,
                        metadata_raw_origin_kind,
                    ),
                    None if !has_metadata_columns => continue,
                    None => (
                        fts_title.unwrap_or_default(),
                        fts_content.unwrap_or_default(),
                        fts_agent.unwrap_or_default(),
                        fts_workspace.unwrap_or_default(),
                        fts_source_path.unwrap_or_default(),
                        fts_created_at,
                        None,
                        None,
                        default_source_id(),
                        None,
                        None,
                    ),
                };

                let source_id = normalized_search_hit_source_id_parts(
                    raw_source_id.as_str(),
                    raw_origin_kind.as_deref().unwrap_or_default(),
                    origin_host.as_deref(),
                );
                let origin_kind = normalized_search_hit_origin_kind(
                    source_id.as_str(),
                    raw_origin_kind.as_deref(),
                );
                let line_number = idx
                    .and_then(|i| usize::try_from(i).ok())
                    .map(|i| i.saturating_add(1));
                let snippet = if field_mask.wants_snippet() {
                    snippet_from_content_for_query(&raw_content, raw_query)
                } else {
                    String::new()
                };
                let content = if field_mask.needs_content() {
                    raw_content
                } else {
                    String::new()
                };
                let content_hash = if content.is_empty() {
                    stable_hit_hash(&snippet, &source_path, line_number, created_at)
                } else {
                    stable_hit_hash(&content, &source_path, line_number, created_at)
                };

                let hit = SearchHit {
                    title,
                    snippet,
                    content,
                    content_hash,
                    conversation_id,
                    score: (-bm25_score) as f32,
                    source_path,
                    agent,
                    workspace,
                    workspace_original: None,
                    created_at,
                    line_number,
                    match_type: query_match_type,
                    source_id,
                    origin_kind,
                    origin_host,
                };
                hits_by_rowid.insert(fts_rowid, hit);
            }

            for (fts_rowid, _) in &ranked_rows {
                if let Some(hit) = hits_by_rowid.remove(fts_rowid)
                    && Self::sqlite_fts5_hit_matches_filters(&hit, &filters)
                {
                    hits.push(hit);
                    if hits.len() >= target_hits {
                        break;
                    }
                }
            }

            if hits.len() >= target_hits
                || !post_filter
                || ranked_rows.len() < rank_batch_limit
                || scanned_rows >= SQLITE_FTS5_POST_FILTER_SCAN_LIMIT
            {
                break;
            }
            rank_offset = rank_offset.saturating_add(ranked_rows.len());
        }

        if post_filter {
            let hits = hits
                .into_iter()
                .skip(offset)
                .take(limit)
                .collect::<Vec<_>>();
            if hits.is_empty() {
                self.search_sqlite_message_scan(conn, scan_request)
            } else {
                Ok(hits)
            }
        } else if hits.is_empty() {
            self.search_sqlite_message_scan(conn, scan_request)
        } else {
            Ok(hits)
        }
    }

    /// Browse messages ordered by date, without any text query.
    ///
    /// Used when the TUI query is empty and the user wants to see recent (or
    /// oldest) sessions. Bypasses BM25 scoring entirely and returns results
    /// ordered by `created_at`. Applies agent, workspace, time-range, and
    /// source filters identically to the normal search path.
    pub fn browse_by_date(
        &self,
        filters: SearchFilters,
        limit: usize,
        offset: usize,
        newest_first: bool,
        field_mask: FieldMask,
    ) -> Result<Vec<SearchHit>> {
        let sqlite_guard = self.sqlite_guard()?;
        if let Some(conn) = sqlite_guard.as_ref() {
            self.browse_by_date_sqlite(conn, filters, limit, offset, newest_first, field_mask)
        } else {
            Ok(Vec::new())
        }
    }

    pub(in crate::search::query) fn browse_by_date_sqlite(
        &self,
        conn: &SearchSqliteConnection,
        filters: SearchFilters,
        limit: usize,
        offset: usize,
        newest_first: bool,
        field_mask: FieldMask,
    ) -> Result<Vec<SearchHit>> {
        #[derive(Debug)]
        struct ConversationTailCandidate {
            conversation_id: i64,
            title: String,
            agent: String,
            workspace: String,
            source_path: String,
            browse_created_at: Option<i64>,
            last_message_idx: Option<i64>,
            source_id: String,
            origin_kind: String,
            origin_host: Option<String>,
        }

        let order = if newest_first { "DESC" } else { "ASC" };
        let title_expr = if field_mask.wants_title() {
            "c.title"
        } else {
            "''"
        };
        // Empty-query browsing is conversation-oriented. Do not join and sort
        // the full messages table: V17 deliberately removed the global
        // messages(created_at) index from the ingest hot path, so that query
        // materialized millions of message rows before applying a tiny TUI
        // LIMIT (GH#395). First choose a bounded page of conversations from
        // the compact tail-state cache, then hydrate exactly one indexed tail
        // message per selected conversation below.
        let browse_created_at_sql = "COALESCE(ts.last_message_created_at, c.last_message_created_at, ts.ended_at, c.ended_at, c.started_at)";
        let last_message_idx_sql = "COALESCE(ts.last_message_idx, c.last_message_idx)";
        let normalized_source_sql =
            normalized_search_source_id_sql_expr("c.source_id", "s.kind", "c.origin_host");
        let mut sql = format!(
            "SELECT c.id, {title_expr}, \
                 COALESCE((SELECT a.slug FROM agents a WHERE a.id = c.agent_id), 'unknown'), \
                 w.path, c.source_path, {browse_created_at_sql}, {last_message_idx_sql}, \
                 {normalized_source_sql}, c.origin_host, s.kind
             FROM conversations c
             LEFT JOIN conversation_tail_state ts ON ts.conversation_id = c.id
             LEFT JOIN workspaces w ON c.workspace_id = w.id
             LEFT JOIN sources s ON c.source_id = s.id
             WHERE EXISTS (
                 SELECT 1 FROM messages candidate_message
                 WHERE candidate_message.conversation_id = c.id
             )"
        );
        let mut params: Vec<ParamValue> = Vec::new();

        if !filters.agents.is_empty() {
            let placeholders = sql_placeholders(filters.agents.len());
            sql.push_str(&format!(
                " AND EXISTS (SELECT 1 FROM agents a WHERE a.id = c.agent_id AND a.slug IN ({placeholders}))"
            ));
            for a in &filters.agents {
                params.push(ParamValue::from(a.as_str()));
            }
        }

        if !filters.workspaces.is_empty() {
            let placeholders = sql_placeholders(filters.workspaces.len());
            sql.push_str(&format!(" AND COALESCE(w.path, '') IN ({placeholders})"));
            for w in &filters.workspaces {
                params.push(ParamValue::from(w.as_str()));
            }
        }

        if let Some(created_from) = filters.created_from {
            sql.push_str(&format!(" AND {browse_created_at_sql} >= ?"));
            params.push(ParamValue::from(created_from));
        }
        if let Some(created_to) = filters.created_to {
            sql.push_str(&format!(" AND {browse_created_at_sql} <= ?"));
            params.push(ParamValue::from(created_to));
        }

        // Apply source filter. `local`/`remote` select by origin *kind*
        // (bead 5bf29); only `SourceId` matches the normalized id.
        let origin_kind_sql =
            normalized_search_origin_kind_sql_expr("c.source_id", "s.kind", "c.origin_host");
        match &filters.source_filter {
            SourceFilter::All => {}
            SourceFilter::Local => sql.push_str(&format!(
                " AND {origin_kind_sql} = '{local}'",
                local = crate::sources::provenance::LOCAL_SOURCE_ID,
            )),
            SourceFilter::Remote => sql.push_str(&format!(
                " AND {origin_kind_sql} != '{local}'",
                local = crate::sources::provenance::LOCAL_SOURCE_ID,
            )),
            SourceFilter::SourceId(id) => {
                sql.push_str(&format!(" AND {normalized_source_sql} = ?"));
                params.push(ParamValue::from(normalize_search_source_filter_value(id)));
            }
        }

        sql.push_str(&format!(
            " ORDER BY CASE WHEN {browse_created_at_sql} IS NULL THEN 1 ELSE 0 END, {browse_created_at_sql} {order}, c.id {order} LIMIT ? OFFSET ?"
        ));
        params.push(ParamValue::from(limit as i64));
        params.push(ParamValue::from(offset as i64));

        let candidates: Vec<ConversationTailCandidate> =
            conn.query_map_collect(&sql, &params, |row: &crate::franken_sync::Row| {
                let conversation_id: i64 = row.get_typed(0)?;
                let title: String = if field_mask.wants_title() {
                    row.get_typed::<Option<String>>(1)?.unwrap_or_default()
                } else {
                    String::new()
                };
                let agent: String = row.get_typed(2)?;
                let workspace: Option<String> = row.get_typed(3)?;
                let source_path: String = row.get_typed(4)?;
                let browse_created_at: Option<i64> = row.get_typed(5)?;
                let last_message_idx: Option<i64> = row.get_typed(6)?;
                let raw_source_id: String = row
                    .get_typed::<Option<String>>(7)?
                    .unwrap_or_else(default_source_id);
                let origin_host: Option<String> = row.get_typed(8)?;
                let raw_origin_kind: Option<String> = row.get_typed(9)?;
                let source_id = normalized_search_hit_source_id_parts(
                    raw_source_id.as_str(),
                    raw_origin_kind.as_deref().unwrap_or_default(),
                    origin_host.as_deref(),
                );
                let origin_kind = normalized_search_hit_origin_kind(
                    source_id.as_str(),
                    raw_origin_kind.as_deref(),
                );

                Ok(ConversationTailCandidate {
                    conversation_id,
                    title,
                    agent,
                    workspace: workspace.unwrap_or_default(),
                    source_path,
                    browse_created_at,
                    last_message_idx,
                    source_id,
                    origin_kind,
                    origin_host,
                })
            })?;

        let mut hits = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let exact_tail_sql = candidate.last_message_idx.map(|_| {
                "SELECT content, created_at, idx
                 FROM messages
                 WHERE conversation_id = ?1 AND idx = ?2
                 LIMIT 1"
            });
            let tail_params = if let Some(last_message_idx) = candidate.last_message_idx {
                vec![
                    ParamValue::from(candidate.conversation_id),
                    ParamValue::from(last_message_idx),
                ]
            } else {
                vec![ParamValue::from(candidate.conversation_id)]
            };
            let fallback_tail_sql = "SELECT content, created_at, idx
                 FROM messages
                 WHERE conversation_id = ?1
                 ORDER BY idx DESC
                 LIMIT 1";
            let mut tail_rows: Vec<(String, Option<i64>, Option<i64>)> = conn.query_map_collect(
                exact_tail_sql.unwrap_or(fallback_tail_sql),
                &tail_params,
                |row: &crate::franken_sync::Row| {
                    Ok((row.get_typed(0)?, row.get_typed(1)?, row.get_typed(2)?))
                },
            )?;
            if tail_rows.is_empty() && exact_tail_sql.is_some() {
                tail_rows = conn.query_map_collect(
                    fallback_tail_sql,
                    &[ParamValue::from(candidate.conversation_id)],
                    |row: &crate::franken_sync::Row| {
                        Ok((row.get_typed(0)?, row.get_typed(1)?, row.get_typed(2)?))
                    },
                )?;
            }
            let Some((raw_content, message_created_at, idx)) = tail_rows.into_iter().next() else {
                continue;
            };

            let created_at = message_created_at.or(candidate.browse_created_at);
            let line_number = idx
                .and_then(|i| usize::try_from(i).ok())
                .map(|i| i.saturating_add(1));
            let snippet = if field_mask.wants_snippet() {
                snippet_from_content(&raw_content)
            } else {
                String::new()
            };
            let content = if field_mask.needs_content() {
                raw_content.clone()
            } else {
                String::new()
            };
            let content_hash = stable_hit_hash(
                &raw_content,
                &candidate.source_path,
                line_number,
                created_at,
            );
            hits.push(SearchHit {
                title: candidate.title,
                snippet,
                content,
                content_hash,
                conversation_id: Some(candidate.conversation_id),
                score: 0.0,
                source_path: candidate.source_path,
                agent: candidate.agent,
                workspace: candidate.workspace,
                workspace_original: None,
                created_at,
                line_number,
                match_type: MatchType::Exact,
                source_id: candidate.source_id,
                origin_kind: candidate.origin_kind,
                origin_host: candidate.origin_host,
            });
        }
        Ok(hits)
    }
}
