use anyhow::{Context, Result, bail};
use frankensearch::lexical_tantivy::{
    CassQueryToken as FsCassQueryToken, CassWildcardPattern as FsCassWildcardPattern,
};
use std::cmp::Ordering as CmpOrdering;

use super::SearchSqliteConnection;

use crate::franken_sync::compat::{ParamValue, RowExt};
use crate::sources::provenance::SourceFilter;

use super::SearchClient;
use super::fusion::{
    normalize_search_source_filter_value, normalized_search_hit_origin_kind,
    normalized_search_hit_source_id_parts, normalized_search_origin_kind_sql_expr,
    normalized_search_source_id_sql_expr, snippet_from_content, stable_hit_hash,
};
use super::parsing::{CassBoolExpr, CassBoolParser, cass_bool_tokens, normalize_phrase_terms};
use super::sqlite_hydration::{
    SQLITE_MAX_VARIABLE_NUMBER, SearchReadTransaction, SqliteFtsMessageRow,
    franken_query_map_collect_retry, nfc_sanitize_query, sql_placeholders,
};
use super::types::{FieldMask, MatchType, SearchFilters, SearchHit, default_source_id};

/// 1t79z: how one scan term part matches a normalized haystack token. The
/// primary lexical index supports suffix/substring/complex wildcards through
/// regex expansion, so the SQLite-only scan lane must honour the same
/// patterns on token boundaries instead of returning a false-empty result.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::search::query) enum SqliteMessageScanTermMatch {
    Exact,
    Prefix,
    Suffix,
    Substring,
    /// Interior wildcards (`f*o*bar`): the lowercased pattern with stars kept.
    Complex(String),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::search::query) struct SqliteMessageScanTermPart {
    pub(in crate::search::query) text: String,
    pub(in crate::search::query) matcher: SqliteMessageScanTermMatch,
}

impl SqliteMessageScanTermPart {
    /// Classify one wildcard-preserving term part (see
    /// `normalize_wildcard_term_parts`). Returns `None` for a bare `*`.
    pub(in crate::search::query) fn parse(part: &str) -> Option<Self> {
        let (text, matcher) = match FsCassWildcardPattern::parse(part) {
            FsCassWildcardPattern::Exact(core) => (core, SqliteMessageScanTermMatch::Exact),
            FsCassWildcardPattern::Prefix(core) => (core, SqliteMessageScanTermMatch::Prefix),
            FsCassWildcardPattern::Suffix(core) => (core, SqliteMessageScanTermMatch::Suffix),
            FsCassWildcardPattern::Substring(core) => (core, SqliteMessageScanTermMatch::Substring),
            FsCassWildcardPattern::Complex(full) => (
                full.trim_matches('*').to_string(),
                SqliteMessageScanTermMatch::Complex(full),
            ),
        };
        (!text.is_empty()).then_some(Self { text, matcher })
    }

    pub(in crate::search::query) fn matches_token(&self, token: &str) -> bool {
        match &self.matcher {
            SqliteMessageScanTermMatch::Exact => token.cmp(self.text.as_str()).is_eq(),
            SqliteMessageScanTermMatch::Prefix => token.starts_with(self.text.as_str()),
            SqliteMessageScanTermMatch::Suffix => token.ends_with(self.text.as_str()),
            SqliteMessageScanTermMatch::Substring => token.contains(self.text.as_str()),
            SqliteMessageScanTermMatch::Complex(pattern) => glob_token_matches(pattern, token),
        }
    }
}

/// Match a lowercased glob (`*` = any run of characters, including empty)
/// against one whole token. Anchored at both ends unless the pattern itself
/// starts/ends with `*`.
pub(in crate::search::query) fn glob_token_matches(pattern: &str, token: &str) -> bool {
    let anchored_start = !pattern.starts_with('*');
    let anchored_end = !pattern.ends_with('*');
    let pieces: Vec<&str> = pattern
        .split('*')
        .filter(|piece| !piece.is_empty())
        .collect();
    if pieces.is_empty() {
        return true;
    }
    let mut rest = token;
    let last = pieces.len() - 1;
    for (idx, piece) in pieces.iter().enumerate() {
        if matches!(idx, 0) && anchored_start {
            if !rest.starts_with(piece) {
                return false;
            }
            rest = &rest[piece.len()..];
            if idx.cmp(&last).is_eq() && anchored_end {
                return rest.is_empty();
            }
            continue;
        }
        if idx.cmp(&last).is_eq() && anchored_end {
            return rest.ends_with(piece);
        }
        let Some(at) = rest.find(piece) else {
            return false;
        };
        rest = &rest[at + piece.len()..];
    }
    true
}

/// Like `normalize_term_parts`, but keeps every `*` inside a token so leading
/// and interior wildcards survive for the scan lane (the FTS5 transpiler only
/// tolerates a trailing star).
pub(in crate::search::query) fn normalize_wildcard_term_parts(raw: &str) -> Vec<String> {
    let mut parts = Vec::new();
    for token in nfc_sanitize_query(raw).split_whitespace() {
        let mut current = String::new();
        for ch in token.chars() {
            if ch.is_alphanumeric() || matches!(ch, '_' | '*') {
                current.push(ch);
                continue;
            }
            if !current.is_empty() {
                parts.push(std::mem::take(&mut current));
            }
        }
        if !current.is_empty() {
            parts.push(current);
        }
    }
    parts
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::search::query) enum SqliteMessageScanOperand {
    Terms(Vec<SqliteMessageScanTermPart>),
    Phrase(Vec<String>),
}

pub(in crate::search::query) struct SqliteMessageScanQuery {
    pub(in crate::search::query) expr: CassBoolExpr<SqliteMessageScanOperand>,
}

#[derive(Clone, Copy)]
pub(in crate::search::query) struct SqliteMessageScanRequest<'a> {
    pub(in crate::search::query) raw_query: &'a str,
    pub(in crate::search::query) filters: &'a SearchFilters,
    pub(in crate::search::query) limit: usize,
    pub(in crate::search::query) offset: usize,
    pub(in crate::search::query) scan_page_rows: usize,
    pub(in crate::search::query) field_mask: FieldMask,
    pub(in crate::search::query) query_match_type: MatchType,
}

impl SearchClient {
    pub(in crate::search::query) fn sqlite_message_scan_query(
        raw_query: &str,
    ) -> Option<SqliteMessageScanQuery> {
        fn scan_parts(parts: Vec<String>) -> Vec<SqliteMessageScanTermPart> {
            parts
                .into_iter()
                .filter_map(|part| SqliteMessageScanTermPart::parse(&part))
                .collect()
        }

        let tokens = cass_bool_tokens(raw_query);
        let expr = CassBoolParser::parse(&tokens, |token: &FsCassQueryToken| {
            Ok(match token {
                FsCassQueryToken::Term(term) => {
                    let mut parts = scan_parts(normalize_wildcard_term_parts(term));
                    parts.sort();
                    parts.dedup();
                    (!parts.is_empty()).then_some(SqliteMessageScanOperand::Terms(parts))
                }
                FsCassQueryToken::Phrase(phrase) => {
                    let parts = normalize_phrase_terms(phrase);
                    (!parts.is_empty()).then_some(SqliteMessageScanOperand::Phrase(parts))
                }
                FsCassQueryToken::And
                | FsCassQueryToken::Or
                | FsCassQueryToken::Not
                | FsCassQueryToken::LParen
                | FsCassQueryToken::RParen => None,
            })
        })
        .ok()??;
        Some(SqliteMessageScanQuery { expr })
    }

    pub(in crate::search::query) fn sqlite_message_scan_score(
        haystacks: &[String],
        scan_query: &SqliteMessageScanQuery,
    ) -> f32 {
        let tokenized_haystacks = haystacks
            .iter()
            .map(|haystack| normalize_phrase_terms(haystack))
            .collect::<Vec<_>>();

        let operand_score = |operand: &SqliteMessageScanOperand| -> f32 {
            match operand {
                SqliteMessageScanOperand::Terms(terms) => {
                    let mut score = 0.0;
                    for term in terms {
                        let matches = tokenized_haystacks
                            .iter()
                            .flatten()
                            .filter(|token| term.matches_token(token.as_str()))
                            .count();
                        if matches < 1 {
                            return 0.0;
                        }
                        score += matches as f32;
                    }
                    score
                }
                SqliteMessageScanOperand::Phrase(phrase) => tokenized_haystacks
                    .iter()
                    .map(|tokens| {
                        tokens
                            .windows(phrase.len())
                            .filter(|window| *window == phrase.as_slice())
                            .count()
                    })
                    .sum::<usize>()
                    as f32,
            }
        };

        /// `None` when the row does not match; otherwise the positive score
        /// it earned (an excluded operand matches with zero).
        fn evaluate(
            expr: &CassBoolExpr<SqliteMessageScanOperand>,
            operand_score: &dyn Fn(&SqliteMessageScanOperand) -> f32,
        ) -> Option<f32> {
            match expr {
                CassBoolExpr::Operand(operand) => {
                    let score = operand_score(operand);
                    (score > 0.0).then_some(score)
                }
                CassBoolExpr::Not(inner) => evaluate(inner, operand_score).is_none().then_some(0.0),
                CassBoolExpr::And(operands) => operands
                    .iter()
                    .map(|operand| evaluate(operand, operand_score))
                    .sum(),
                CassBoolExpr::Or(operands) => operands
                    .iter()
                    .filter_map(|operand| evaluate(operand, operand_score))
                    .reduce(f32::max),
            }
        }
        let Some(score) = evaluate(&scan_query.expr, &operand_score) else {
            return 0.0;
        };

        // A negative-only query has no positive relevance contribution, but
        // its complement matches still need a non-zero sentinel so the caller
        // does not discard them. Mixed-query negative clauses stay score-neutral.
        if score > 0.0 { score } else { 1.0 }
    }

    #[cfg(test)]
    pub(in crate::search::query) fn sqlite_message_scan_query_sql(
        field_mask: FieldMask,
        filters: &SearchFilters,
        page_last_message_id: i64,
    ) -> (String, Vec<ParamValue>) {
        let mut session_paths = filters
            .session_paths
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        session_paths.sort_unstable();
        Self::sqlite_message_scan_query_sql_for_session_paths(
            field_mask,
            filters,
            session_paths.as_slice(),
            None,
            page_last_message_id,
        )
    }

    /// The message ids of one source-scan page: the next `scan_page_rows`
    /// ids after the cursor, in id order. A bare rowid range, so
    /// FrankenSQLite seeks it.
    pub(in crate::search::query) fn sqlite_message_scan_page_ids(
        conn: &SearchSqliteConnection,
        after_message_id: Option<i64>,
        scan_page_rows: usize,
    ) -> Result<Vec<i64>> {
        let limit = ParamValue::from(i64::try_from(scan_page_rows).unwrap_or(i64::MAX));
        let (sql, params) = match after_message_id {
            Some(after) => (
                "SELECT id FROM messages WHERE id > ? ORDER BY id LIMIT ?",
                vec![ParamValue::from(after), limit],
            ),
            None => ("SELECT id FROM messages ORDER BY id LIMIT ?", vec![limit]),
        };
        franken_query_map_collect_retry(conn, sql, &params, |row| row.get_typed(0))
            .context("listing the message ids of a SQLite source-scan page")
    }

    /// One source-scan page: the messages with ids in
    /// `(after_message_id, page_last_message_id]`, joined to their
    /// conversation metadata and narrowed by every filter.
    ///
    /// The page is a bare rowid range in a subquery, and the joins and
    /// filters apply outside it. FrankenSQLite seeks a rowid range only when
    /// nothing else is in its WHERE; `messages m LEFT JOIN … WHERE 1=1 AND
    /// <filters> AND m.id > ? ORDER BY m.id LIMIT ?` read the whole table for
    /// every page (5-15 s per 1,024-row page on a 496,736-message archive,
    /// so one fallback search took most of an hour; this shape takes
    /// 0.06-0.27 s per page).
    pub(in crate::search::query) fn sqlite_message_scan_query_sql_for_session_paths(
        field_mask: FieldMask,
        filters: &SearchFilters,
        session_paths: &[&str],
        after_message_id: Option<i64>,
        page_last_message_id: i64,
    ) -> (String, Vec<ParamValue>) {
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

        // Placeholders bind in text order: the page bounds come first.
        let mut params = Vec::new();
        let page_lower_sql = match after_message_id {
            Some(after_message_id) => {
                params.push(ParamValue::from(after_message_id));
                "id > ? AND "
            }
            None => "",
        };
        params.push(ParamValue::from(page_last_message_id));

        let mut sql = format!(
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
                    s.kind,
                    COALESCE(m.content, ''),
                    COALESCE(c.title, '')
             FROM (SELECT id, conversation_id, idx, content, created_at
                   FROM messages
                   WHERE {page_lower_sql}id <= ?) m
             LEFT JOIN conversations c ON m.conversation_id = c.id
             LEFT JOIN sources s ON c.source_id = s.id
             LEFT JOIN agents a ON c.agent_id = a.id
             LEFT JOIN workspaces w ON c.workspace_id = w.id
             WHERE 1=1"
        );

        // Each page bounds materialized rows, not corpus completeness. Every
        // metadata filter applies inside the page query, so only rows whose
        // title/content must actually be evaluated leave SQLite.
        if !filters.agents.is_empty() {
            let placeholders = sql_placeholders(filters.agents.len());
            sql.push_str(&format!(" AND COALESCE(a.slug, '') IN ({placeholders})"));
            for agent in &filters.agents {
                params.push(ParamValue::from(agent.as_str()));
            }
        }
        if !filters.workspaces.is_empty() {
            let placeholders = sql_placeholders(filters.workspaces.len());
            sql.push_str(&format!(" AND COALESCE(w.path, '') IN ({placeholders})"));
            for workspace in &filters.workspaces {
                params.push(ParamValue::from(workspace.as_str()));
            }
        }
        if let Some(created_from) = filters.created_from {
            sql.push_str(" AND CAST(m.created_at AS INTEGER) >= ?");
            params.push(ParamValue::from(created_from));
        }
        if let Some(created_to) = filters.created_to {
            sql.push_str(" AND CAST(m.created_at AS INTEGER) <= ?");
            params.push(ParamValue::from(created_to));
        }

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

        if !session_paths.is_empty() {
            let placeholders = sql_placeholders(session_paths.len());
            sql.push_str(&format!(
                " AND COALESCE(c.source_path, '') IN ({placeholders})"
            ));
            for source_path in session_paths {
                params.push(ParamValue::from(*source_path));
            }
        }

        sql.push_str(" ORDER BY m.id");
        (sql, params)
    }

    pub(in crate::search::query) fn sqlite_message_scan_row_to_hit(
        row: (SqliteFtsMessageRow, String, String),
        scan_query: &SqliteMessageScanQuery,
        request: SqliteMessageScanRequest<'_>,
    ) -> Option<(i64, SearchHit)> {
        let (
            (
                message_id,
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
            ),
            scan_content,
            scan_title,
        ) = row;

        // The primary CASS lexical query targets title/content; agent,
        // workspace, and source are filters rather than searchable text.
        // Keep the two text fields separate so a quoted phrase cannot
        // accidentally bridge the content/title boundary.
        let scan_haystacks = [scan_content.to_lowercase(), scan_title.to_lowercase()];
        let score = Self::sqlite_message_scan_score(&scan_haystacks, scan_query);
        if score <= 0.0 {
            return None;
        }

        let raw_source_id = raw_source_id.unwrap_or_else(default_source_id);
        let source_id = normalized_search_hit_source_id_parts(
            raw_source_id.as_str(),
            raw_origin_kind.as_deref().unwrap_or_default(),
            origin_host.as_deref(),
        );
        let origin_kind =
            normalized_search_hit_origin_kind(source_id.as_str(), raw_origin_kind.as_deref());
        let line_number = idx
            .and_then(|i| usize::try_from(i).ok())
            .map(|i| i.saturating_add(1));
        let snippet = if request.field_mask.wants_snippet() {
            snippet_from_content(&scan_content)
        } else {
            String::new()
        };
        let content = if request.field_mask.needs_content() {
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
            score,
            source_path,
            agent,
            workspace,
            workspace_original: None,
            created_at,
            line_number,
            match_type: request.query_match_type,
            source_id,
            origin_kind,
            origin_host,
        };

        Self::sqlite_fts5_hit_matches_filters(&hit, request.filters).then_some((message_id, hit))
    }

    pub(in crate::search::query) fn trim_sqlite_message_scan_hits(
        scored_hits: &mut Vec<(i64, SearchHit)>,
        retained_hit_count: usize,
    ) {
        scored_hits.sort_by(|(left_id, left), (right_id, right)| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(CmpOrdering::Equal)
                .then_with(|| left_id.cmp(right_id))
        });
        scored_hits.truncate(retained_hit_count);
    }

    pub(in crate::search::query) fn search_sqlite_message_scan(
        &self,
        conn: &SearchSqliteConnection,
        request: SqliteMessageScanRequest<'_>,
    ) -> Result<Vec<SearchHit>> {
        if request.limit == 0 || request.scan_page_rows == 0 {
            return Ok(Vec::new());
        }
        let Some(scan_query) = Self::sqlite_message_scan_query(request.raw_query) else {
            return Ok(Vec::new());
        };
        let retained_hit_count = request.offset.saturating_add(request.limit);
        // Keyset pagination must observe one archive generation. Without an
        // explicit read transaction, concurrent indexing can append rows
        // between pages, changing ranking inputs or preventing termination.
        let mut read_transaction = SearchReadTransaction::begin(conn)
            .context("starting SQLite source-scan read snapshot")?;

        let fixed_param_count = request
            .filters
            .agents
            .len()
            .saturating_add(request.filters.workspaces.len())
            .saturating_add(if request.filters.created_from.is_some() {
                1
            } else {
                0
            })
            .saturating_add(if request.filters.created_to.is_some() {
                1
            } else {
                0
            })
            .saturating_add(
                if matches!(&request.filters.source_filter, SourceFilter::SourceId(_)) {
                    1
                } else {
                    0
                },
            )
            .saturating_add(2); // page bounds: the cursor and the page's last message id
        if fixed_param_count > SQLITE_MAX_VARIABLE_NUMBER {
            bail!(
                "SQLite source-scan filters require {fixed_param_count} fixed parameters; maximum is {SQLITE_MAX_VARIABLE_NUMBER}"
            );
        }

        let mut session_paths = request
            .filters
            .session_paths
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        session_paths.sort_unstable();
        session_paths.dedup();
        let session_path_chunk_size = SQLITE_MAX_VARIABLE_NUMBER - fixed_param_count;
        if !session_paths.is_empty() && session_path_chunk_size == 0 {
            bail!("SQLite source-scan filters leave no bind-variable capacity for session paths");
        }

        let mut scored_hits = Vec::with_capacity(retained_hit_count.min(request.scan_page_rows));
        let session_path_chunks = if session_paths.is_empty() {
            vec![session_paths.as_slice()]
        } else {
            session_paths.chunks(session_path_chunk_size).collect()
        };
        for session_path_chunk in session_path_chunks {
            let mut after_message_id = None;
            loop {
                // The page is the next `scan_page_rows` message ids; the
                // filtered rows within it come from a second, bounded query.
                // Termination and the cursor follow the ids, never the
                // filtered rows: a page whose rows all fail the filters is
                // not the end of the archive.
                let page_ids = Self::sqlite_message_scan_page_ids(
                    conn,
                    after_message_id,
                    request.scan_page_rows,
                )?;
                let page_len = page_ids.len();
                let Some(&next_after_message_id) = page_ids.last() else {
                    break;
                };
                drop(page_ids);
                let (sql, params) = Self::sqlite_message_scan_query_sql_for_session_paths(
                    request.field_mask,
                    request.filters,
                    session_path_chunk,
                    after_message_id,
                    next_after_message_id,
                );
                let page_rows = franken_query_map_collect_retry(conn, &sql, &params, |row| {
                    Ok((
                        (
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
                        ),
                        row.get_typed(12)?,
                        row.get_typed(13)?,
                    ))
                })?;

                for row in page_rows {
                    if let Some(hit) =
                        Self::sqlite_message_scan_row_to_hit(row, &scan_query, request)
                    {
                        scored_hits.push(hit);
                    }
                }
                if scored_hits.len() > retained_hit_count {
                    Self::trim_sqlite_message_scan_hits(&mut scored_hits, retained_hit_count);
                }

                if page_len < request.scan_page_rows {
                    break;
                }
                if after_message_id.is_some_and(|previous| next_after_message_id <= previous) {
                    bail!(
                        "SQLite source-scan pagination did not advance beyond message id {next_after_message_id}"
                    );
                }
                after_message_id = Some(next_after_message_id);
            }
        }
        Self::trim_sqlite_message_scan_hits(&mut scored_hits, retained_hit_count);

        let hits = scored_hits
            .into_iter()
            .skip(request.offset)
            .take(request.limit)
            .map(|(_, hit)| hit)
            .collect();
        read_transaction
            .rollback()
            .context("closing SQLite source-scan read snapshot")?;
        Ok(hits)
    }
}
