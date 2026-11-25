use super::analytics_rebuild::is_out_of_memory_error;
use crate::franken_sync::{
    Connection as FrankenConnection, Row as FrankenRow, SqliteValue,
    compat::{
        ConnectionExt as FrankenConnectionExt, RowExt as FrankenRowExt,
        Transaction as FrankenTransaction, TransactionExt as FrankenTransactionExt,
    },
};
use anyhow::{Context, Result, anyhow};
use std::collections::HashSet;

pub(super) fn franken_table_column_names(
    conn: &FrankenConnection,
    table_name: &str,
) -> Result<HashSet<String>> {
    if !table_name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(anyhow!(
            "unsafe table name for PRAGMA table_info: {table_name}"
        ));
    }

    conn.query_map_collect(
        &format!("PRAGMA table_info({table_name})"),
        fparams![],
        |row: &FrankenRow| row.get_typed::<String>(1),
    )
    .with_context(|| format!("reading PRAGMA table_info({table_name})"))
    .map(|columns| columns.into_iter().collect())
}

pub(super) fn error_indicates_missing_table(err: &impl std::fmt::Display) -> bool {
    err.to_string()
        .to_ascii_lowercase()
        .contains("no such table")
}

pub(super) fn error_indicates_missing_column(err: &impl std::fmt::Display) -> bool {
    err.to_string()
        .to_ascii_lowercase()
        .contains("no such column")
}

pub(super) const ORPHAN_FK_ID_CHUNK_SIZE: usize = 256;

pub(super) fn collect_orphan_message_ids(conn: &FrankenConnection) -> Result<Vec<i64>> {
    conn.query_map_collect(
        "SELECT m.id
         FROM messages AS m
         WHERE NOT EXISTS (
             SELECT 1
             FROM conversations AS c
             WHERE c.id = m.conversation_id
         )",
        fparams![],
        |row| row.get_typed(0),
    )
    .context("listing orphan message ids for orphan FK cleanup")
}

pub(super) fn delete_rows_by_i64_chunks(
    tx: &FrankenTransaction<'_>,
    delete_many_sql_prefix: &'static str,
    ids: &[i64],
) -> Result<usize> {
    if ids.is_empty() {
        return Ok(0);
    }

    let full_chunk_sql = delete_rows_by_i64_sql(delete_many_sql_prefix, ORPHAN_FK_ID_CHUNK_SIZE);
    let tail_len = ids.len() % ORPHAN_FK_ID_CHUNK_SIZE;
    let tail_sql =
        (tail_len != 0).then(|| delete_rows_by_i64_sql(delete_many_sql_prefix, tail_len));

    let mut deleted = 0;
    for chunk in ids.chunks(ORPHAN_FK_ID_CHUNK_SIZE) {
        let sql = if chunk.len() == ORPHAN_FK_ID_CHUNK_SIZE {
            &full_chunk_sql
        } else {
            tail_sql.as_ref().unwrap_or(&full_chunk_sql)
        };
        let params = chunk
            .iter()
            .map(|id| SqliteValue::from(*id))
            .collect::<Vec<_>>();
        deleted += tx.execute_with_params(sql, &params)?;
    }
    Ok(deleted)
}

fn delete_rows_by_i64_sql(delete_many_sql_prefix: &'static str, count: usize) -> String {
    let placeholders = sql_placeholders(count);
    format!("{delete_many_sql_prefix} ({placeholders})")
}

pub(super) fn sql_placeholders(count: usize) -> String {
    vec!["?"; count].join(", ")
}

pub(super) fn delete_orphan_message_ids_bisecting_oom(
    conn: &FrankenConnection,
    ids: &[i64],
) -> Result<usize> {
    let mut deleted = 0usize;
    for chunk in ids.chunks(ORPHAN_FK_ID_CHUNK_SIZE) {
        deleted = deleted.saturating_add(delete_orphan_message_id_chunk(conn, chunk)?);
    }
    Ok(deleted)
}

fn delete_orphan_message_id_chunk(conn: &FrankenConnection, ids: &[i64]) -> Result<usize> {
    if ids.is_empty() {
        return Ok(0);
    }

    match delete_orphan_message_id_chunk_once(conn, ids) {
        Ok(deleted) => Ok(deleted),
        Err(err) if is_out_of_memory_error(&err) && ids.len() > 1 => {
            let split_at = ids.len() / 2;
            tracing::warn!(
                target: "cass::fk_repair",
                rows = ids.len(),
                left = split_at,
                right = ids.len().saturating_sub(split_at),
                error = %err,
                "orphan-message cleanup ran out of memory; retrying as smaller batches"
            );
            let left = delete_orphan_message_id_chunk(conn, &ids[..split_at])?;
            let right = delete_orphan_message_id_chunk(conn, &ids[split_at..])?;
            Ok(left.saturating_add(right))
        }
        Err(err) => Err(err),
    }
}

fn delete_orphan_message_id_chunk_once(conn: &FrankenConnection, ids: &[i64]) -> Result<usize> {
    let mut tx = conn.transaction()?;
    let mut deleted = 0usize;
    for entry in ORPHAN_MESSAGE_DEPENDENT_TABLES {
        match delete_rows_by_i64_chunks(&tx, entry.delete_many_sql_prefix, ids) {
            Ok(count) => {
                deleted = deleted.saturating_add(count);
            }
            Err(err) if error_indicates_missing_table(&err) => {
                tracing::debug!(
                    target: "cass::fk_repair",
                    child_table = entry.child_table,
                    error = %err,
                    "skipping orphan-message dependent cleanup (table unavailable)"
                );
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!(
                        "deleting rows from {} that depend on orphan messages",
                        entry.child_table
                    )
                });
            }
        }
    }
    deleted = deleted.saturating_add(
        delete_rows_by_i64_chunks(&tx, "DELETE FROM messages WHERE id IN", ids)
            .context("deleting orphan rows from messages")?,
    );
    tx.commit()?;
    Ok(deleted)
}

pub(super) fn collect_direct_orphan_id_page(
    conn: &FrankenConnection,
    entry: &'static OrphanFkTable,
) -> Result<Vec<i64>> {
    Ok(conn.query_map_collect(
        entry.orphan_id_page_sql,
        fparams![i64::try_from(ORPHAN_FK_ID_CHUNK_SIZE).unwrap_or(i64::MAX)],
        |row| row.get_typed(0),
    )?)
}

pub(super) fn delete_direct_orphan_ids_bisecting_oom(
    conn: &FrankenConnection,
    entry: &'static OrphanFkTable,
    ids: &[i64],
) -> Result<usize> {
    let mut deleted = 0usize;
    for chunk in ids.chunks(ORPHAN_FK_ID_CHUNK_SIZE) {
        deleted = deleted.saturating_add(delete_direct_orphan_id_chunk(conn, entry, chunk)?);
    }
    Ok(deleted)
}

fn delete_direct_orphan_id_chunk(
    conn: &FrankenConnection,
    entry: &'static OrphanFkTable,
    ids: &[i64],
) -> Result<usize> {
    if ids.is_empty() {
        return Ok(0);
    }

    match delete_direct_orphan_id_chunk_once(conn, entry, ids) {
        Ok(deleted) => Ok(deleted),
        Err(err) if is_out_of_memory_error(&err) && ids.len() > 1 => {
            let split_at = ids.len() / 2;
            tracing::warn!(
                target: "cass::fk_repair",
                child_table = entry.child_table,
                rows = ids.len(),
                left = split_at,
                right = ids.len().saturating_sub(split_at),
                error = %err,
                "direct orphan cleanup ran out of memory; retrying as smaller batches"
            );
            let left = delete_direct_orphan_id_chunk(conn, entry, &ids[..split_at])?;
            let right = delete_direct_orphan_id_chunk(conn, entry, &ids[split_at..])?;
            Ok(left.saturating_add(right))
        }
        Err(err) => Err(err),
    }
}

fn delete_direct_orphan_id_chunk_once(
    conn: &FrankenConnection,
    entry: &'static OrphanFkTable,
    ids: &[i64],
) -> Result<usize> {
    let mut tx = conn.transaction()?;
    let deleted = delete_rows_by_i64_chunks(&tx, entry.delete_many_sql_prefix, ids)?;
    tx.commit()?;
    Ok(deleted)
}

/// Tables whose FK parent rows can go missing when an index transaction is
/// dropped mid-flight. The select and delete SQL strings are intentionally
/// static (no dynamic table names) so they can be audited at a glance and so
/// they cannot be subverted by injected identifiers. The select statement
/// yields the integer FK key used by the matching chunked delete.
pub(super) struct OrphanFkTable {
    pub(super) child_table: &'static str,
    pub(super) orphan_id_page_sql: &'static str,
    pub(super) delete_many_sql_prefix: &'static str,
}

pub(super) const ORPHAN_DIRECT_CHILD_TABLES: &[OrphanFkTable] = &[
    OrphanFkTable {
        child_table: "message_metrics",
        orphan_id_page_sql: "SELECT message_id FROM message_metrics \
                             WHERE NOT EXISTS (\
                                 SELECT 1 FROM messages \
                                 WHERE messages.id = message_metrics.message_id\
                             ) \
                             ORDER BY message_id \
                             LIMIT ?1",
        delete_many_sql_prefix: "DELETE FROM message_metrics WHERE message_id IN",
    },
    OrphanFkTable {
        child_table: "token_usage",
        orphan_id_page_sql: "SELECT message_id FROM token_usage \
                             WHERE NOT EXISTS (\
                                 SELECT 1 FROM messages \
                                 WHERE messages.id = token_usage.message_id\
                             ) \
                             ORDER BY message_id \
                             LIMIT ?1",
        delete_many_sql_prefix: "DELETE FROM token_usage WHERE message_id IN",
    },
    OrphanFkTable {
        child_table: "snippets",
        orphan_id_page_sql: "SELECT message_id FROM snippets \
                             WHERE NOT EXISTS (\
                                 SELECT 1 FROM messages \
                                 WHERE messages.id = snippets.message_id\
                             ) \
                             ORDER BY message_id \
                             LIMIT ?1",
        delete_many_sql_prefix: "DELETE FROM snippets WHERE message_id IN",
    },
    OrphanFkTable {
        child_table: "conversation_tags",
        orphan_id_page_sql: "SELECT conversation_id FROM conversation_tags \
                             WHERE NOT EXISTS (\
                                 SELECT 1 FROM conversations \
                                 WHERE conversations.id = conversation_tags.conversation_id\
                             ) \
                             ORDER BY conversation_id \
                             LIMIT ?1",
        delete_many_sql_prefix: "DELETE FROM conversation_tags WHERE conversation_id IN",
    },
];

pub(super) struct OrphanMessageDependentTable {
    pub(super) child_table: &'static str,
    pub(super) delete_many_sql_prefix: &'static str,
}

const ORPHAN_MESSAGE_DEPENDENT_TABLES: &[OrphanMessageDependentTable] = &[
    OrphanMessageDependentTable {
        child_table: "message_metrics",
        delete_many_sql_prefix: "DELETE FROM message_metrics WHERE message_id IN",
    },
    OrphanMessageDependentTable {
        child_table: "token_usage",
        delete_many_sql_prefix: "DELETE FROM token_usage WHERE message_id IN",
    },
    OrphanMessageDependentTable {
        child_table: "snippets",
        delete_many_sql_prefix: "DELETE FROM snippets WHERE message_id IN",
    },
];

/// Summary of orphan rows detected and removed by `cleanup_orphan_fk_rows`.
///
/// Message-root counts come from the probe phase, while direct child counts
/// come from bounded page deletes. Under the function's intended use — a single
/// indexer-startup pass holding the index run lock — no concurrent writers
/// exist, so these counts match the primary orphan roots identified and
/// removed during cleanup. Dependent rows below an orphan message
/// (`message_metrics` / `token_usage` / `snippets`) are an expected consequence
/// of removing that root orphan and are *not* separately counted in `total` or
/// `per_table`.
#[derive(Debug, Default, Clone)]
pub(crate) struct OrphanFkCleanupReport {
    pub total: i64,
    pub per_table: Vec<(&'static str, i64)>,
}

impl OrphanFkCleanupReport {
    pub(super) fn record(&mut self, child_table: &'static str, count: i64) {
        if let Some((_, existing)) = self
            .per_table
            .iter_mut()
            .find(|(table, _)| *table == child_table)
        {
            *existing = existing.saturating_add(count);
        } else {
            self.per_table.push((child_table, count));
        }
        self.total = self.total.saturating_add(count);
    }
}
