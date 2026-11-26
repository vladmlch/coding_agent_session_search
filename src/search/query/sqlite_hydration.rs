use anyhow::{Context, Result};
use frankensearch::lexical_tantivy::cass_sanitize_query as fs_cass_sanitize_query;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::{Duration, Instant};

use super::SearchSqliteConnection;
use crate::franken_sync::compat::{ParamValue, RowExt, param_slice_to_values};

/// Row-mapping conveniences for the dedicated-owner search connection.
///
/// `AsyncConnection` intentionally exposes owned rows across its worker-thread
/// boundary. Mapping them on the caller keeps application-specific conversion
/// errors out of the worker protocol while preserving the former fallible row
/// mapper semantics.
pub(in crate::search::query) trait SearchSqliteConnectionExt {
    #[cfg(test)]
    fn query_row_map<T, F>(
        &self,
        sql: &str,
        params: &[ParamValue],
        map: F,
    ) -> Result<T, crate::franken_sync::FrankenError>
    where
        F: FnOnce(&crate::franken_sync::Row) -> Result<T, crate::franken_sync::FrankenError>;

    fn query_map_collect<T, F>(
        &self,
        sql: &str,
        params: &[ParamValue],
        map: F,
    ) -> Result<Vec<T>, crate::franken_sync::FrankenError>
    where
        F: FnMut(&crate::franken_sync::Row) -> Result<T, crate::franken_sync::FrankenError>;
}

impl SearchSqliteConnectionExt for SearchSqliteConnection {
    #[cfg(test)]
    fn query_row_map<T, F>(
        &self,
        sql: &str,
        params: &[ParamValue],
        map: F,
    ) -> Result<T, crate::franken_sync::FrankenError>
    where
        F: FnOnce(&crate::franken_sync::Row) -> Result<T, crate::franken_sync::FrankenError>,
    {
        let values = param_slice_to_values(params);
        let row = self.query_row_with_params_sync(sql, &values)?;
        map(&row)
    }

    fn query_map_collect<T, F>(
        &self,
        sql: &str,
        params: &[ParamValue],
        mut map: F,
    ) -> Result<Vec<T>, crate::franken_sync::FrankenError>
    where
        F: FnMut(&crate::franken_sync::Row) -> Result<T, crate::franken_sync::FrankenError>,
    {
        let values = param_slice_to_values(params);
        let rows = self.query_with_params_sync(sql, &values)?;
        rows.iter().map(&mut map).collect()
    }
}

#[cfg(test)]
pub(in crate::search::query) struct SearchSqliteFixture(Option<SearchSqliteConnection>);

#[cfg(test)]
impl SearchSqliteFixture {
    pub(in crate::search::query) fn in_memory() -> Result<Self, crate::franken_sync::FrankenError> {
        SearchSqliteConnection::open_sync(":memory:").map(|conn| Self(Some(conn)))
    }

    pub(in crate::search::query) fn into_connection(mut self) -> SearchSqliteConnection {
        self.0
            .take()
            .expect("search sqlite fixture connection must be present")
    }

    pub(in crate::search::query) fn connection(&self) -> &SearchSqliteConnection {
        self.0
            .as_ref()
            .expect("search sqlite fixture connection must be present")
    }

    pub(in crate::search::query) fn execute(
        &self,
        sql: &str,
    ) -> Result<usize, crate::franken_sync::FrankenError> {
        self.connection().execute_sync(sql)
    }

    pub(in crate::search::query) fn execute_with_params(
        &self,
        sql: &str,
        params: &[crate::franken_sync::SqliteValue],
    ) -> Result<usize, crate::franken_sync::FrankenError> {
        self.connection().execute_with_params_sync(sql, params)
    }

    pub(in crate::search::query) fn execute_batch(
        &self,
        sql: &str,
    ) -> Result<(), crate::franken_sync::FrankenError> {
        self.connection().execute_batch_sync(sql)
    }

    pub(in crate::search::query) fn execute_compat(
        &self,
        sql: &str,
        params: &[ParamValue],
    ) -> Result<usize, crate::franken_sync::FrankenError> {
        let values = param_slice_to_values(params);
        self.connection().execute_with_params_sync(sql, &values)
    }
}

#[cfg(test)]
impl std::ops::Deref for SearchSqliteFixture {
    type Target = SearchSqliteConnection;

    fn deref(&self) -> &Self::Target {
        self.connection()
    }
}

#[cfg(test)]
impl Drop for SearchSqliteFixture {
    fn drop(&mut self) {
        if let Some(mut conn) = self.0.take()
            && let Err(error) = conn.close_without_checkpoint_sync()
        {
            tracing::warn!(%error, "failed to close search sqlite fixture owner");
        }
    }
}

/// Read snapshot whose rollback obligation remains on the dedicated owner.
///
/// Search hydration deliberately takes an explicit transaction so related
/// message and conversation queries observe one archive generation. The
/// guard makes early-return/error paths rollback on the same worker that owns
/// the connection.
pub(in crate::search::query) struct SearchReadTransaction<'conn> {
    pub(in crate::search::query) conn: &'conn SearchSqliteConnection,
    pub(in crate::search::query) active: bool,
}

impl<'conn> SearchReadTransaction<'conn> {
    pub(in crate::search::query) fn begin(
        conn: &'conn SearchSqliteConnection,
    ) -> Result<Self, crate::franken_sync::FrankenError> {
        conn.begin_transaction_sync()?;
        Ok(Self { conn, active: true })
    }

    pub(in crate::search::query) fn query_map_collect<T, F>(
        &self,
        sql: &str,
        params: &[ParamValue],
        map: F,
    ) -> Result<Vec<T>, crate::franken_sync::FrankenError>
    where
        F: FnMut(&crate::franken_sync::Row) -> Result<T, crate::franken_sync::FrankenError>,
    {
        self.conn.query_map_collect(sql, params, map)
    }

    pub(in crate::search::query) fn rollback(
        &mut self,
    ) -> Result<(), crate::franken_sync::FrankenError> {
        self.conn.rollback_transaction_sync()?;
        self.active = false;
        Ok(())
    }
}

impl Drop for SearchReadTransaction<'_> {
    fn drop(&mut self) {
        if self.active
            && let Err(error) = self.conn.rollback_transaction_sync()
        {
            tracing::warn!(
                %error,
                "failed to roll back search hydration read transaction"
            );
        }
    }
}

pub(in crate::search::query) type TantivyContentExactKey = (i64, i64);
pub(in crate::search::query) type TantivyContentFallbackKey = (String, String, i64);
pub(in crate::search::query) type TantivyHydratedContentMaps = (
    HashMap<TantivyContentExactKey, String>,
    HashMap<TantivyContentFallbackKey, String>,
);
pub(in crate::search::query) type SqliteFtsHydratedRow = (
    i64,
    Option<i64>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
);
pub(in crate::search::query) type SqliteFtsMessageRow = (
    i64,
    String,
    String,
    String,
    String,
    String,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<String>,
    Option<String>,
    Option<String>,
);

// Frankensqlite follows SQLite's bind-variable ceiling. Keep fallback
// hydration IN-lists below that ceiling so large pages do not turn into
// empty fallback result sets.
pub(in crate::search::query) const SQLITE_FTS5_HYDRATE_PARAM_CHUNK: usize = 30_000;
pub(in crate::search::query) const SQLITE_MAX_VARIABLE_NUMBER: usize = 32_766;
pub(in crate::search::query) const SQLITE_FTS5_POST_FILTER_SCAN_CHUNK: usize = 1_024;
pub(in crate::search::query) const SQLITE_FTS5_POST_FILTER_SCAN_LIMIT: usize = 30_000;
// Source-table fallback scans the complete read snapshot while materializing
// at most this many raw message rows at once.
pub(in crate::search::query) const SQLITE_MESSAGE_SCAN_FALLBACK_PAGE_ROWS: usize = 1_024;
pub(in crate::search::query) const SEARCH_SQLITE_HYDRATION_CACHE_KIB: i64 = 4_096;
pub(in crate::search::query) const SEMANTIC_EXACT_CHUNK_OVERFETCH_MULTIPLIER: usize = 4;

pub(in crate::search::query) fn open_search_hydration_sqlite(
    path: &Path,
    timeout: Duration,
    strict_read_only: bool,
) -> Result<SearchSqliteConnection> {
    let conn = if strict_read_only {
        crate::storage::sqlite::open_franken_async_strict_readonly_connection_with_timeout(
            path, timeout,
        )?
    } else {
        crate::storage::sqlite::open_franken_async_readonly_connection_with_timeout(path, timeout)?
    };
    conn.execute_sync("PRAGMA query_only = 1;")
        .with_context(|| "setting search hydration query_only")?;
    conn.execute_sync("PRAGMA busy_timeout = 5000;")
        .with_context(|| "setting search hydration busy_timeout")?;
    conn.execute_sync(&format!(
        "PRAGMA cache_size = -{SEARCH_SQLITE_HYDRATION_CACHE_KIB};"
    ))
    .with_context(|| "setting search hydration cache_size")?;
    Ok(conn)
}

/// NFC-normalize a query string before sanitization so that decomposed
/// Unicode (NFD — common on macOS keyboard input) matches NFC-indexed content
/// produced by `DefaultCanonicalizer`.
pub(in crate::search::query) fn nfc_sanitize_query(raw: &str) -> String {
    use unicode_normalization::UnicodeNormalization;
    let nfc: String = raw.nfc().collect();
    fs_cass_sanitize_query(&nfc)
}

pub(in crate::search::query) fn franken_query_map_collect_retry<T, F>(
    conn: &SearchSqliteConnection,
    sql: &str,
    params: &[ParamValue],
    map: F,
) -> Result<Vec<T>, crate::franken_sync::FrankenError>
where
    F: Copy + Fn(&crate::franken_sync::Row) -> Result<T, crate::franken_sync::FrankenError>,
{
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut backoff = Duration::from_millis(4);
    loop {
        match conn.query_map_collect(sql, params, |row| map(row)) {
            Ok(values) => return Ok(values),
            Err(err) if crate::storage::sqlite::retryable_franken_error(&err) => {
                let now = Instant::now();
                if now >= deadline {
                    return Err(err);
                }
                let remaining = deadline.saturating_duration_since(now);
                crate::storage::sqlite::sleep_with_franken_retry_backoff(
                    &mut backoff,
                    remaining,
                    Duration::from_millis(64),
                );
            }
            Err(err) => return Err(err),
        }
    }
}

pub(in crate::search::query) fn hydrate_message_content_by_conversation(
    conn: &SearchSqliteConnection,
    requests: &[TantivyContentExactKey],
) -> Result<HashMap<TantivyContentExactKey, String>> {
    if requests.is_empty() {
        return Ok(HashMap::new());
    }

    let mut wanted_by_conversation: HashMap<i64, HashSet<i64>> = HashMap::new();
    for &(conversation_id, line_idx) in requests {
        wanted_by_conversation
            .entry(conversation_id)
            .or_default()
            .insert(line_idx);
    }

    let has_conv_idx = franken_query_map_collect_retry(
        conn,
        "PRAGMA index_list(messages)",
        &[],
        |row| row.get_typed::<String>(1),
    )
    .map(|names| names.iter().any(|name| name == "idx_messages_conv_idx"))
    .unwrap_or(false);
    let index_hint = if has_conv_idx {
        " INDEXED BY idx_messages_conv_idx"
    } else {
        ""
    };

    let mut conversation_ids = wanted_by_conversation.keys().copied().collect::<Vec<_>>();
    conversation_ids.sort_unstable();
    let mut hydrated = HashMap::with_capacity(requests.len());

    for conversation_id in conversation_ids {
        let Some(wanted_indices) = wanted_by_conversation.get(&conversation_id) else {
            continue;
        };
        let mut wanted_indices = wanted_indices.iter().copied().collect::<Vec<_>>();
        wanted_indices.sort_unstable();
        let placeholders = sql_placeholders(wanted_indices.len());
        let sql = format!(
            "SELECT m.conversation_id, m.idx, m.content
             FROM messages m{index_hint}
             WHERE m.conversation_id = ? AND m.idx IN ({placeholders})
             ORDER BY m.idx"
        );
        let mut params = Vec::with_capacity(wanted_indices.len() + 1);
        params.push(ParamValue::from(conversation_id));
        params.extend(wanted_indices.iter().copied().map(ParamValue::from));
        let rows: Vec<(i64, i64, String)> =
            franken_query_map_collect_retry(conn, &sql, &params, |row| {
                Ok((row.get_typed(0)?, row.get_typed(1)?, row.get_typed(2)?))
            })?;
        for (conversation_id, line_idx, content) in rows {
            hydrated.insert((conversation_id, line_idx), content);
        }
    }

    Ok(hydrated)
}

pub(in crate::search::query) fn semantic_message_id_from_db(
    message_id: i64,
) -> std::io::Result<u64> {
    u64::try_from(message_id).map_err(|_| std::io::Error::other("negative message_id"))
}

#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::search::query) fn semantic_doc_component_id_from_db(raw: Option<i64>) -> u32 {
    raw.map(|value| u32::try_from(value.max(0)).unwrap_or(u32::MAX))
        .unwrap_or(0)
}

// ============================================================================
// SQL Placeholder Builder (Opt 4.5: Pre-sized String Buffers)
// ============================================================================

/// Render rowids as a literal SQL list (`1,2,3`) for an `IN (...)` over an
/// `INTEGER PRIMARY KEY`.
///
/// FrankenSQLite through 0.3.17 (verified on 0.3.16, cass GH #382) planned
/// parameterized `WHERE id IN (?,?,...)` as `SCAN messages`, a full table
/// walk, while integer literals used primary-key seeks. Version 0.3.18
/// adds parameterized seeks; this established literal path remains covered
/// at the full hydration chunk size. The ids come from
/// cass's own index (never from user text) and `i64`'s `Display` emits only
/// an optional `-` and ASCII digits, so embedding them cannot form SQL.
pub fn sql_rowid_literal_list(ids: &[i64]) -> String {
    let mut out = String::with_capacity(ids.len().saturating_mul(8));
    for (idx, id) in ids.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        // `write!` into a String cannot fail.
        use std::fmt::Write as _;
        let _ = write!(out, "{id}");
    }
    out
}

/// Build a comma-separated list of SQL placeholders with pre-allocated capacity.
///
/// For `n` items, produces "?,?,?..." (n "?" with n-1 ",").
/// Uses pre-sized String to avoid reallocations.
///
/// # Examples
/// ```ignore
/// assert_eq!(sql_placeholders(0), "");
/// assert_eq!(sql_placeholders(1), "?");
/// assert_eq!(sql_placeholders(3), "?,?,?");
/// ```
#[inline]
pub fn sql_placeholders(count: usize) -> String {
    if count == 0 {
        return String::new();
    }
    // Capacity: n "?" + (n-1) "," = 2n - 1
    let capacity = count.saturating_mul(2).saturating_sub(1);
    let mut result = String::with_capacity(capacity);
    for i in 0..count {
        if i > 0 {
            result.push(',');
        }
        result.push('?');
    }
    result
}
