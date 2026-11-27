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

#[cfg(test)]
pub(crate) fn source_filter_where_clause(
    source_filter: Option<&crate::sources::provenance::SourceFilter>,
) -> (String, Option<String>) {
    source_filter_where_clause_with_columns(
        source_filter,
        "c.source_id",
        "(SELECT s.kind FROM sources s WHERE s.id = c.source_id LIMIT 1)",
        "c.origin_host",
    )
}

pub(crate) fn source_filter_where_clause_with_columns(
    source_filter: Option<&crate::sources::provenance::SourceFilter>,
    source_id_sql: &str,
    origin_kind_sql: &str,
    origin_host_sql: &str,
) -> (String, Option<String>) {
    let normalized_source_sql = normalized_source_identity_sql_expr(source_id_sql, origin_host_sql);
    let normalized_origin_kind_sql =
        normalized_source_origin_kind_sql_expr(source_id_sql, origin_kind_sql, origin_host_sql);
    match source_filter {
        None | Some(crate::sources::provenance::SourceFilter::All) => (String::new(), None),
        Some(crate::sources::provenance::SourceFilter::Local) => (
            format!(
                " WHERE {normalized_origin_kind_sql} = '{local}'",
                local = crate::sources::provenance::LOCAL_SOURCE_ID,
            ),
            None,
        ),
        Some(crate::sources::provenance::SourceFilter::Remote) => (
            format!(
                " WHERE {normalized_origin_kind_sql} != '{local}'",
                local = crate::sources::provenance::LOCAL_SOURCE_ID,
            ),
            None,
        ),
        Some(crate::sources::provenance::SourceFilter::SourceId(id)) => (
            format!(" WHERE {normalized_source_sql} = ?"),
            Some(
                canonical_followup_source_id(Some(id.as_str()))
                    .unwrap_or_else(|| id.trim().to_string()),
            ),
        ),
    }
}

/// `stats --by-source` rows (source, conversations, messages), most
/// conversations first, from each selected conversation's normalized source.
/// Message counts come from one streaming pass over the messages index:
/// joining messages into a GROUP BY by source ran for more than 15 minutes
/// on a 9.7M-message archive.
pub(crate) fn stats_source_rows(
    conn: &crate::franken_sync::Connection,
    conversation_sources: Vec<(i64, String)>,
) -> Result<Vec<(String, i64, i64)>, crate::franken_sync::FrankenError> {
    let mut rows: Vec<(String, i64, i64)> = Vec::new();
    let mut row_of_source: HashMap<String, usize> = HashMap::new();
    let mut row_of_conversation: HashMap<i64, usize> =
        HashMap::with_capacity(conversation_sources.len());
    for (conversation_id, source_id) in conversation_sources {
        let row = *row_of_source.entry(source_id.clone()).or_insert_with(|| {
            rows.push((source_id, 0, 0));
            rows.len() - 1
        });
        rows[row].1 += 1;
        row_of_conversation.insert(conversation_id, row);
    }
    if !rows.is_empty() {
        for_each_message_conversation_id(conn, |conversation_id| {
            if let Some(row) = row_of_conversation.get(&conversation_id) {
                rows[*row].2 += 1;
            }
        })?;
    }
    rows.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    Ok(rows)
}

pub(crate) fn stats_message_count_sql(source_join: &str, source_where: &str) -> String {
    if source_join.is_empty() && source_where.is_empty() {
        "SELECT COUNT(*) FROM messages".to_string()
    } else {
        format!(
            "SELECT COUNT(*) FROM messages m JOIN conversations c ON m.conversation_id = c.id{source_join}{source_where}"
        )
    }
}

pub(crate) fn stats_workspace_count_sql(source_join: &str, source_where: &str) -> String {
    if source_where.is_empty() {
        format!(
            "SELECT c.workspace_id, COUNT(*) FROM conversations c{source_join} WHERE c.workspace_id IS NOT NULL GROUP BY c.workspace_id"
        )
    } else {
        format!(
            "SELECT c.workspace_id, COUNT(*) FROM conversations c{source_join}{source_where} AND c.workspace_id IS NOT NULL GROUP BY c.workspace_id"
        )
    }
}

pub(crate) fn stats_agent_count_sql(source_join: &str, source_where: &str) -> String {
    format!(
        "SELECT c.agent_id, COUNT(*) FROM conversations c{source_join}{source_where} GROUP BY c.agent_id"
    )
}

pub(crate) fn sort_stats_counts<K: Ord>(rows: &mut [(K, i64)]) {
    // Keep aggregate ordering out of SQL: sorting only the grouped counts
    // in Rust keeps the GROUP BY query simple and avoids materializing rows.
    // Sort only the groups, with ascending IDs (NULL first) breaking ties.
    rows.sort_by(|(left_id, left_count), (right_id, right_count)| {
        right_count
            .cmp(left_count)
            .then_with(|| left_id.cmp(right_id))
    });
}

pub(crate) fn stats_query_error(
    operation: &str,
    err: crate::franken_sync::FrankenError,
) -> CliError {
    CliError {
        code: 5,
        kind: CliErrorKind::Storage.kind_str(),
        message: format!("stats could not {operation}: {err}"),
        hint: Some("Run 'cass doctor --json' to inspect the archive; unavailable statistics are not zero counts.".into()),
        retryable: crate::storage::sqlite::retryable_franken_error(&err),
    }
}

#[derive(Default)]
pub(crate) struct UnfilteredConversationStats {
    pub(crate) count: i64,
    pub(crate) agents: HashMap<Option<i64>, i64>,
    pub(crate) workspaces: HashMap<i64, i64>,
    pub(crate) oldest: Option<i64>,
    pub(crate) newest: Option<i64>,
}

pub(crate) fn unfiltered_conversation_stats(
    conn: &crate::franken_sync::Connection,
) -> Result<UnfilteredConversationStats, crate::franken_sync::FrankenError> {
    use crate::franken_sync::compat::RowExt as _;

    let deadline = std::time::Instant::now() + CLI_DB_QUERY_RETRY_TIMEOUT;
    let mut backoff = Duration::from_millis(4);
    loop {
        // A failed streaming attempt may have delivered a prefix. Discard that
        // attempt's entire fold before retrying, so rows cannot be counted twice.
        let mut stats = UnfilteredConversationStats::default();
        let mut callback_failed = false;
        let result = conn.query_with_params_for_each(
            "SELECT agent_id, workspace_id, started_at FROM conversations",
            &[],
            |row| {
                let values = (|| {
                    Ok::<_, crate::franken_sync::FrankenError>((
                        row.get_typed::<Option<i64>>(0)?,
                        row.get_typed::<Option<i64>>(1)?,
                        row.get_typed::<Option<i64>>(2)?,
                    ))
                })()
                .inspect_err(|_| callback_failed = true)?;
                let (agent, workspace, started_at) = values;
                stats.count += 1;
                *stats.agents.entry(agent).or_default() += 1;
                if let Some(workspace) = workspace {
                    *stats.workspaces.entry(workspace).or_default() += 1;
                }
                if let Some(started_at) = started_at {
                    stats.oldest = Some(stats.oldest.map_or(started_at, |old| old.min(started_at)));
                    stats.newest = Some(stats.newest.map_or(started_at, |old| old.max(started_at)));
                }
                Ok(())
            },
        );
        match result {
            Ok(()) => return Ok(stats),
            Err(err) if callback_failed => return Err(err),
            Err(err) if crate::storage::sqlite::retryable_franken_error(&err) => {
                let now = std::time::Instant::now();
                if now >= deadline {
                    return Err(err);
                }
                crate::storage::sqlite::sleep_with_franken_retry_backoff(
                    &mut backoff,
                    deadline.saturating_duration_since(now),
                    Duration::from_millis(64),
                );
            }
            Err(err) => return Err(err),
        }
    }
}

pub(crate) fn append_source_filter_condition(
    sql: &mut String,
    params: &mut Vec<crate::franken_sync::compat::ParamValue>,
    source_filter: &crate::sources::provenance::SourceFilter,
) {
    append_source_filter_condition_with_columns(
        sql,
        params,
        source_filter,
        "c.source_id",
        "s.kind",
        "c.origin_host",
    );
}

pub(crate) fn append_source_filter_condition_with_columns(
    sql: &mut String,
    params: &mut Vec<crate::franken_sync::compat::ParamValue>,
    source_filter: &crate::sources::provenance::SourceFilter,
    source_id_sql: &str,
    origin_kind_sql: &str,
    origin_host_sql: &str,
) {
    let normalized_source_sql = normalized_source_identity_sql_expr(source_id_sql, origin_host_sql);
    let normalized_origin_kind_sql =
        normalized_source_origin_kind_sql_expr(source_id_sql, origin_kind_sql, origin_host_sql);
    match source_filter {
        crate::sources::provenance::SourceFilter::All => {}
        crate::sources::provenance::SourceFilter::Local => {
            sql.push_str(&format!(
                " AND {normalized_origin_kind_sql} = '{local}'",
                local = crate::sources::provenance::LOCAL_SOURCE_ID,
            ));
        }
        crate::sources::provenance::SourceFilter::Remote => {
            sql.push_str(&format!(
                " AND {normalized_origin_kind_sql} != '{local}'",
                local = crate::sources::provenance::LOCAL_SOURCE_ID,
            ));
        }
        crate::sources::provenance::SourceFilter::SourceId(id) => {
            sql.push_str(&format!(
                " AND {normalized_source_sql} = ?{}",
                params.len() + 1
            ));
            params.push(
                canonical_followup_source_id(Some(id.as_str()))
                    .unwrap_or_else(|| id.trim().to_string())
                    .into(),
            );
        }
    }
}

pub(crate) fn run_stats(
    data_dir_override: &Option<PathBuf>,
    db_override: Option<PathBuf>,
    output_format: Option<RobotFormat>,
    source: Option<&str>,
    by_source: bool,
) -> CliResult<()> {
    use crate::sources::provenance::SourceFilter;

    use crate::franken_sync::compat::{ParamValue, RowExt};

    let data_dir = resolve_data_dir(data_dir_override, db_override.as_ref());
    let db_path = db_override.unwrap_or_else(|| data_dir.join("agent_search.db"));
    let conn = open_franken_cli_read_db(db_path.clone(), "stats", Duration::from_secs(30))?;
    validate_fts_messages_integrity_for_cli(&conn, "stats")?;
    let conversation_columns = doctor_table_columns(&conn, "conversations");
    let source_id_sql = if conversation_columns.contains("source_id") {
        "c.source_id"
    } else {
        "'local'"
    };
    let origin_host_sql = if conversation_columns.contains("origin_host") {
        "c.origin_host"
    } else {
        "NULL"
    };

    // Parse source filter (P3.7)
    let source_filter = source.map(SourceFilter::parse);
    let source_kind_filter_active = matches!(
        source_filter.as_ref(),
        Some(SourceFilter::Local | SourceFilter::Remote)
    );
    let source_columns = if source_kind_filter_active {
        doctor_table_columns(&conn, "sources")
    } else {
        HashSet::new()
    };
    let can_join_source_kind = conversation_columns.contains("source_id")
        && source_columns.contains("id")
        && source_columns.contains("kind");
    let source_join = if source_kind_filter_active && can_join_source_kind {
        " LEFT JOIN sources s ON s.id = c.source_id"
    } else {
        ""
    };
    let origin_kind_sql = if source_join.is_empty() {
        "NULL"
    } else {
        "s.kind"
    };

    // Build WHERE clause for source filtering
    let (source_where, source_param): (String, Option<String>) =
        source_filter_where_clause_with_columns(
            source_filter.as_ref(),
            source_id_sql,
            origin_kind_sql,
            origin_host_sql,
        );

    // Helper: build params slice from optional source param
    let make_params = |param: &Option<String>| -> Vec<ParamValue> {
        match param {
            Some(p) => vec![ParamValue::from(p.as_str())],
            None => vec![],
        }
    };

    // Get counts and statistics with source filter
    let params = make_params(&source_param);
    let conversation_sql =
        format!("SELECT COUNT(*) FROM conversations c{source_join}{source_where}");
    let message_sql = stats_message_count_sql(source_join, &source_where);
    let mut unfiltered = if source_join.is_empty() && source_where.is_empty() {
        Some(unfiltered_conversation_stats(&conn).map_err(|err| {
            stats_query_error(
                "count conversations, group agents/workspaces, and read conversation date range",
                err,
            )
        })?)
    } else {
        None
    };

    let conversation_count: i64 = if let Some(stats) = &unfiltered {
        stats.count
    } else {
        franken_query_row_map_retry(&conn, &conversation_sql, &params, |r| r.get_typed(0))
            .map_err(|err| stats_query_error("count conversations", err))?
    };

    let message_count: i64 =
        franken_query_row_map_retry(&conn, &message_sql, &params, |r| r.get_typed(0))
            .map_err(|err| stats_query_error("count messages", err))?;

    let agent_lookup: HashMap<i64, String> =
        franken_query_map_collect_retry(&conn, "SELECT id, slug FROM agents", &[], |r| {
            Ok((r.get_typed::<i64>(0)?, r.get_typed::<String>(1)?))
        })
        .map_err(|e| CliError::unknown(format!("query: {e}")))?
        .into_iter()
        .collect();

    let agent_sql = stats_agent_count_sql(source_join, &source_where);
    let mut agent_count_rows: Vec<(Option<i64>, i64)> = if let Some(stats) = &mut unfiltered {
        std::mem::take(&mut stats.agents).into_iter().collect()
    } else {
        franken_query_map_collect_retry(&conn, &agent_sql, &params, |r| {
            Ok((r.get_typed::<Option<i64>>(0)?, r.get_typed::<i64>(1)?))
        })
        .map_err(|e| CliError::unknown(format!("query: {e}")))?
    };
    sort_stats_counts(&mut agent_count_rows);
    let agent_rows: Vec<(String, i64)> = agent_count_rows
        .into_iter()
        .map(|(agent_id, count)| {
            let agent = agent_id
                .and_then(|id| agent_lookup.get(&id).cloned())
                .unwrap_or_else(|| "unknown".to_string());
            (agent, count)
        })
        .collect();

    let workspace_lookup: HashMap<i64, String> =
        franken_query_map_collect_retry(&conn, "SELECT id, path FROM workspaces", &[], |r| {
            Ok((r.get_typed::<i64>(0)?, r.get_typed::<String>(1)?))
        })
        .map_err(|e| CliError::unknown(format!("query: {e}")))?
        .into_iter()
        .collect();

    let ws_sql = stats_workspace_count_sql(source_join, &source_where);
    let mut workspace_count_rows: Vec<(i64, i64)> = if let Some(stats) = &mut unfiltered {
        std::mem::take(&mut stats.workspaces).into_iter().collect()
    } else {
        franken_query_map_collect_retry(&conn, &ws_sql, &params, |r| {
            Ok((r.get_typed::<i64>(0)?, r.get_typed::<i64>(1)?))
        })
        .map_err(|e| CliError::unknown(format!("query: {e}")))?
    };
    sort_stats_counts(&mut workspace_count_rows);
    let ws_rows: Vec<(String, i64)> = workspace_count_rows
        .into_iter()
        .filter_map(|(workspace_id, count)| {
            workspace_lookup
                .get(&workspace_id)
                .cloned()
                .map(|path| (path, count))
        })
        .take(10)
        .collect();

    // Get date range with source filter.
    // Note: source_where already includes a leading " WHERE ...", so when it is present we must
    // append additional conditions with " AND ..." (not another WHERE).
    let date_sql = if source_where.is_empty() {
        "SELECT MIN(started_at), MAX(started_at) FROM conversations c WHERE started_at IS NOT NULL"
            .to_string()
    } else {
        format!(
            "SELECT MIN(started_at), MAX(started_at) FROM conversations c{source_join}{source_where} AND started_at IS NOT NULL"
        )
    };
    let (oldest, newest): (Option<i64>, Option<i64>) = if let Some(stats) = &unfiltered {
        (stats.oldest, stats.newest)
    } else {
        franken_query_row_map_retry(&conn, &date_sql, &params, |r| {
            Ok((r.get_typed(0)?, r.get_typed(1)?))
        })
        .map_err(|err| stats_query_error("read conversation date range", err))?
    };
    let raw_mirror_summary = crate::raw_mirror::storage_summary(&data_dir);

    // Get per-source breakdown if requested (P3.7)
    let source_rows: Vec<(String, i64, i64)> = if by_source {
        let normalized_source_sql =
            normalized_source_identity_sql_expr(source_id_sql, origin_host_sql);
        let conversation_sources: Vec<(i64, String)> = franken_query_map_collect_retry(
            &conn,
            &format!(
                "SELECT c.id, {normalized_source_sql} FROM conversations c{source_join}{source_where}"
            ),
            &params,
            |r| Ok((r.get_typed::<i64>(0)?, r.get_typed::<String>(1)?)),
        )
        .map_err(|e| CliError::unknown(format!("query: {e}")))?;
        stats_source_rows(&conn, conversation_sources)
            .map_err(|e| CliError::unknown(format!("query: {e}")))?
    } else {
        Vec::new()
    };

    let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
        if matches!(fmt, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            fmt
        }
    });

    if let Some(fmt) = structured_format {
        let mut payload = serde_json::json!({
            "conversations": conversation_count,
            "messages": message_count,
            "by_agent": agent_rows.iter().map(|(a, c)| serde_json::json!({"agent": a, "count": c})).collect::<Vec<_>>(),
            "top_workspaces": ws_rows.iter().map(|(w, c)| serde_json::json!({"workspace": w, "count": c})).collect::<Vec<_>>(),
            "date_range": {
                "oldest": oldest.and_then(|ts| chrono::DateTime::from_timestamp_millis(ts).map(|d| d.to_rfc3339())),
                "newest": newest.and_then(|ts| chrono::DateTime::from_timestamp_millis(ts).map(|d| d.to_rfc3339())),
            },
            "raw_mirror": &raw_mirror_summary,
            "db_path": db_path.display().to_string(),
        });

        // Add source filter info if specified (P3.7)
        if let Some(ref filter) = source_filter {
            payload["source_filter"] = serde_json::json!(filter.to_string());
        }

        // Add by_source breakdown if requested (P3.7)
        if by_source && !source_rows.is_empty() {
            payload["by_source"] = serde_json::json!(
                source_rows
                    .iter()
                    .map(|(s, convs, msgs)| {
                        serde_json::json!({
                            "source_id": s,
                            "conversations": convs,
                            "messages": msgs
                        })
                    })
                    .collect::<Vec<_>>()
            );
        }

        return output_structured_value(payload, fmt);
    }

    // Header with source filter indicator
    let title = if let Some(ref filter) = source_filter {
        format!("CASS Index Statistics (source: {})", filter)
    } else {
        "CASS Index Statistics".to_string()
    };
    println!("{title}");
    println!("{}", "=".repeat(title.len()));
    println!("Database: {}", db_path.display());
    println!();

    // Show by_source breakdown if requested (P3.7)
    if by_source && !source_rows.is_empty() {
        println!("By Source:");
        println!("  {:20} {:>10} {:>12}", "Source", "Convs", "Messages");
        println!("  {}", "-".repeat(44));
        for (src, convs, msgs) in &source_rows {
            println!("  {:20} {:>10} {:>12}", src, convs, msgs);
        }
        println!();
    }

    println!("Totals:");
    println!("  Conversations: {conversation_count}");
    println!("  Messages: {message_count}");
    println!();
    println!("Raw Mirror:");
    if raw_mirror_summary.initialized {
        println!(
            "  Storage bytes: {}",
            raw_mirror_summary.total_storage_bytes
        );
        println!("  Manifests: {}", raw_mirror_summary.manifest_count);
        println!("  Unique blobs: {}", raw_mirror_summary.unique_blob_count);
        println!("  Blob bytes: {}", raw_mirror_summary.total_blob_bytes);
        println!("  Orphan blobs: {}", raw_mirror_summary.orphan_blob_count);
        println!(
            "  Orphan blob bytes: {}",
            raw_mirror_summary.orphan_blob_bytes
        );
        println!(
            "  Largest blob bytes: {}",
            raw_mirror_summary.largest_blob_bytes
        );
        if raw_mirror_summary.missing_blob_count > 0
            || raw_mirror_summary.invalid_manifest_count > 0
        {
            println!(
                "  Warnings: {} missing blob(s), {} invalid manifest(s)",
                raw_mirror_summary.missing_blob_count, raw_mirror_summary.invalid_manifest_count
            );
        }
    } else {
        println!("  Status: not initialized");
    }
    println!();
    println!("By Agent:");
    for (agent, count) in &agent_rows {
        println!("  {agent}: {count}");
    }
    println!();
    if !ws_rows.is_empty() {
        println!("Top Workspaces:");
        for (ws, count) in &ws_rows {
            println!("  {ws}: {count}");
        }
        println!();
    }
    if let (Some(old), Some(new)) = (oldest, newest)
        && let (Some(old_dt), Some(new_dt)) = (
            chrono::DateTime::from_timestamp_millis(old),
            chrono::DateTime::from_timestamp_millis(new),
        )
    {
        println!(
            "Date Range: {} to {}",
            old_dt.format("%Y-%m-%d"),
            new_dt.format("%Y-%m-%d")
        );
    }

    Ok(())
}

/// Stable component identifiers for the `cass storage` breakdown. The
/// order here is the order they appear in the JSON `components` array and
/// the human table (golden-frozen). Each variant maps a user-meaningful
/// on-disk component to the data-dir entries that compose it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StorageComponent {
    CanonicalDb,
    DbWal,
    DbShm,
    LexicalIndex,
    RawMirror,
    SemanticIndex,
    Quarantine,
    Backups,
    Other,
}

impl StorageComponent {
    /// Stable robot key (kebab-case-free snake to match other cass JSON).
    pub(crate) fn key(self) -> &'static str {
        match self {
            Self::CanonicalDb => "canonical_db",
            Self::DbWal => "db_wal",
            Self::DbShm => "db_shm",
            Self::LexicalIndex => "lexical_index",
            Self::RawMirror => "raw_mirror",
            Self::SemanticIndex => "semantic_index",
            Self::Quarantine => "quarantine",
            Self::Backups => "backups",
            Self::Other => "other",
        }
    }

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::CanonicalDb => "Canonical DB (agent_search.db)",
            Self::DbWal => "DB WAL (agent_search.db-wal)",
            Self::DbShm => "DB shared-memory (agent_search.db-shm)",
            Self::LexicalIndex => "Lexical index (index/)",
            Self::RawMirror => "Raw mirror (raw-mirror/)",
            Self::SemanticIndex => "Semantic index (vector_index/, semantic/)",
            Self::Quarantine => "Quarantine (quarantine/, doctor-quarantine/)",
            Self::Backups => "Backups (backups/)",
            Self::Other => "Other (remotes, caches, locks, misc)",
        }
    }

    /// Whether this component is a derived asset that can be safely rebuilt
    /// from the canonical DB (advisory, mirrors the search-asset contract).
    pub(crate) fn derived(self) -> bool {
        matches!(
            self,
            Self::DbWal
                | Self::DbShm
                | Self::LexicalIndex
                | Self::SemanticIndex
                | Self::Quarantine
                | Self::Backups
        )
    }

    /// All components in stable display order.
    pub(crate) fn ordered() -> [Self; 9] {
        [
            Self::CanonicalDb,
            Self::DbWal,
            Self::DbShm,
            Self::LexicalIndex,
            Self::RawMirror,
            Self::SemanticIndex,
            Self::Quarantine,
            Self::Backups,
            Self::Other,
        ]
    }
}

/// Classify a top-level data-dir entry name into a storage component.
/// Anything unrecognized rolls into `Other` so the per-component bytes
/// always sum to the total accounted footprint.
pub(crate) fn classify_storage_entry(name: &str) -> StorageComponent {
    match name {
        "agent_search.db" => StorageComponent::CanonicalDb,
        "agent_search.db-wal" => StorageComponent::DbWal,
        "agent_search.db-shm" => StorageComponent::DbShm,
        "index" => StorageComponent::LexicalIndex,
        "raw-mirror" | "raw_mirror" => StorageComponent::RawMirror,
        "vector_index" | "semantic" | "semantic-cache" => StorageComponent::SemanticIndex,
        "quarantine" | "doctor-quarantine" => StorageComponent::Quarantine,
        "backups" | "backup" => StorageComponent::Backups,
        _ => StorageComponent::Other,
    }
}

/// `cass storage` (#292.4): read-only on-disk footprint breakdown by
/// component. Sums per-component bytes from the data dir so operators can
/// answer "what is my N GB and what is safe to reclaim" without manual
/// `du` + `sqlite3`. Mutates nothing; pairs with `mirror prune` /
/// `cass forget` for actual reclamation.
pub(crate) fn run_storage(
    data_dir_override: &Option<PathBuf>,
    db_override: Option<PathBuf>,
    output_format: Option<RobotFormat>,
) -> CliResult<()> {
    use std::collections::BTreeMap;

    let data_dir = resolve_data_dir(data_dir_override, db_override.as_ref());
    let db_path = db_override
        .clone()
        .unwrap_or_else(|| data_dir.join("agent_search.db"));
    let data_dir_exists = data_dir.exists();

    let mut bytes_by_component: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut files_by_component: BTreeMap<&'static str, u64> = BTreeMap::new();
    for component in StorageComponent::ordered() {
        bytes_by_component.insert(component.key(), 0);
        files_by_component.insert(component.key(), 0);
    }

    // Walk the top-level data-dir entries once. Each entry (file or dir)
    // is attributed wholesale to a single component; nested files are
    // counted via fs_dir_size. The canonical DB may live OUTSIDE the data
    // dir when --db is overridden, so account it explicitly below.
    if data_dir_exists && let Ok(entries) = std::fs::read_dir(&data_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            let path = entry.path();
            // When --db points back into the data dir, the canonical-DB
            // entry is handled by the explicit accounting below to keep a
            // single source of truth; skip it here to avoid double count.
            if path == db_path && classify_storage_entry(&name) == StorageComponent::CanonicalDb {
                continue;
            }
            let component = classify_storage_entry(&name);
            let size = fs_dir_size(&path);
            let file_count = if path.is_dir() {
                storage_file_count(&path)
            } else {
                1
            };
            *bytes_by_component.entry(component.key()).or_insert(0) += size;
            *files_by_component.entry(component.key()).or_insert(0) += file_count;
        }
    }

    // Explicit canonical-DB accounting (honors --db override that may sit
    // outside the data dir). The WAL/SHM sidecars are derived from the
    // resolved DB path so an overridden DB still reports its sidecars.
    let db_size = std::fs::metadata(&db_path).map(|m| m.len()).unwrap_or(0);
    if db_size > 0 {
        *bytes_by_component
            .entry(StorageComponent::CanonicalDb.key())
            .or_insert(0) += db_size;
        *files_by_component
            .entry(StorageComponent::CanonicalDb.key())
            .or_insert(0) += 1;
    }
    let db_exists = db_path.exists();
    for (suffix, component) in [
        ("-wal", StorageComponent::DbWal),
        ("-shm", StorageComponent::DbShm),
    ] {
        let mut sidecar = db_path.clone();
        let file_name = format!(
            "{}{suffix}",
            db_path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| "agent_search.db".to_string())
        );
        sidecar.set_file_name(&file_name);
        // Only account an overridden-DB sidecar; the in-data-dir sidecar
        // was already counted by the directory walk above.
        if sidecar.parent() != Some(data_dir.as_path()) {
            let size = std::fs::metadata(&sidecar).map(|m| m.len()).unwrap_or(0);
            if size > 0 {
                *bytes_by_component.entry(component.key()).or_insert(0) += size;
                *files_by_component.entry(component.key()).or_insert(0) += 1;
            }
        }
    }

    let total_bytes: u64 = bytes_by_component.values().sum();
    let total_files: u64 = files_by_component.values().sum();

    let components_json: Vec<serde_json::Value> = StorageComponent::ordered()
        .iter()
        .map(|component| {
            let key = component.key();
            let bytes = *bytes_by_component.get(key).unwrap_or(&0);
            let files = *files_by_component.get(key).unwrap_or(&0);
            let percent = if total_bytes > 0 {
                ((bytes as f64 / total_bytes as f64) * 10000.0).round() / 100.0
            } else {
                0.0
            };
            serde_json::json!({
                "component": key,
                "label": component.label(),
                "bytes": bytes,
                "human": format_bytes(bytes),
                "file_count": files,
                "percent_of_total": percent,
                "derived_reclaimable": component.derived(),
            })
        })
        .collect();

    let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
        if matches!(fmt, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            fmt
        }
    });

    if let Some(fmt) = structured_format {
        let payload = serde_json::json!({
            "schema_version": 1,
            "data_dir": data_dir.display().to_string(),
            "data_dir_exists": data_dir_exists,
            "db_path": db_path.display().to_string(),
            "db_exists": db_exists,
            "total_bytes": total_bytes,
            "total_human": format_bytes(total_bytes),
            "total_file_count": total_files,
            "components": components_json,
        });
        return output_structured_value(payload, fmt);
    }

    println!("CASS Storage Breakdown");
    println!("======================");
    println!();
    println!("Data directory: {}", data_dir.display());
    if !data_dir_exists {
        println!("  (data directory does not exist yet — run 'cass index --full')");
    }
    println!("Database: {}", db_path.display());
    println!();
    println!("Total on-disk footprint: {}", format_bytes(total_bytes));
    println!();
    println!("{:<42} {:>12} {:>7}", "Component", "Size", "Share");
    println!("{}", "-".repeat(63));
    for component in StorageComponent::ordered() {
        let key = component.key();
        let bytes = *bytes_by_component.get(key).unwrap_or(&0);
        let percent = if total_bytes > 0 {
            (bytes as f64 / total_bytes as f64) * 100.0
        } else {
            0.0
        };
        let derived = if component.derived() {
            " (reclaimable)"
        } else {
            ""
        };
        println!(
            "{:<42} {:>12} {:>6.1}%{}",
            component.label(),
            format_bytes(bytes),
            percent,
            derived
        );
    }
    println!();
    println!("Components marked (reclaimable) are derived assets that can be rebuilt");
    println!("from the canonical DB. Use 'cass mirror prune' or 'cass forget' to reclaim.");

    Ok(())
}

/// `cass dedup` (#302 ask #2 / bead uhhxy): collapse PRE-EXISTING duplicate
/// conversation rows where the watcher and full-rebuild ingest paths created
/// both a `projects/<rel>` and a bare `<rel>` external_id for the same
/// `source_path`. Dry-run by default (inspect only); `--apply` performs the
/// deletion and rebuilds the lexical FTS index so search stops returning the
/// dropped twins.
pub(crate) fn run_dedup(
    data_dir_override: &Option<PathBuf>,
    db_override: Option<PathBuf>,
    output_format: Option<RobotFormat>,
    apply: bool,
) -> CliResult<()> {
    let data_dir = resolve_data_dir(data_dir_override, db_override.as_ref());
    let db_path = db_override.unwrap_or_else(|| data_dir.join("agent_search.db"));

    let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
        if matches!(fmt, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            fmt
        }
    });

    if !db_path.exists() {
        // No DB: nothing to dedup. Report an empty, successful result so the
        // robot contract stays uniform (matches stats/diag missing-DB shape).
        if let Some(fmt) = structured_format {
            let payload = serde_json::json!({
                "schema_version": 1,
                "applied": false,
                "dry_run": !apply,
                "db_path": db_path.display().to_string(),
                "db_exists": false,
                "conversations_collapsed": 0,
                "messages_removed": 0,
                "lexical_rebuilt": false,
                "pairs": [],
                "misattributed_mirror": {
                    "conversations_removed": 0,
                    "messages_removed": 0,
                    "unmatched": 0,
                    "rows": [],
                },
                "recommended_action": "No database found. Run 'cass index --full' first.",
            });
            return output_structured_value(payload, fmt);
        }
        println!("No database found at {}.", db_path.display());
        println!("Run 'cass index --full' to create it.");
        return Ok(());
    }

    let storage =
        crate::storage::sqlite::FrankenStorage::open(&db_path).map_err(|err| CliError {
            code: 3,
            kind: CliErrorKind::DbOpen.kind_str(),
            message: format!("failed to open canonical DB for dedup: {err}"),
            hint: Some("Run 'cass doctor check --json' for a read-only diagnosis.".to_string()),
            retryable: true,
        })?;

    let result = storage
        .collapse_external_id_prefix_duplicates(!apply)
        .map_err(|err| CliError {
            code: 5,
            kind: CliErrorKind::Storage.kind_str(),
            message: format!("duplicate-conversation cleanup failed: {err}"),
            hint: Some(
                "Re-run without --apply to inspect, or 'cass doctor check --json' for diagnosis."
                    .to_string(),
            ),
            retryable: false,
        })?;

    let mirror_result = storage
        .collapse_misattributed_mirror_rows(
            &misattributed_mirror_containers(&storage, &data_dir),
            !apply,
        )
        .map_err(|err| CliError {
            code: 5,
            kind: CliErrorKind::Storage.kind_str(),
            message: format!("misattributed mirror cleanup failed: {err}"),
            hint: Some(
                "Re-run without --apply to inspect, or 'cass doctor check --json' for diagnosis."
                    .to_string(),
            ),
            retryable: false,
        })?;
    let conversations_removed =
        result.conversations_affected + mirror_result.conversations_affected;

    // On apply, rebuild the lexical FTS index so the dropped twins stop
    // surfacing in search. Only worth doing when something was removed.
    let mut lexical_rebuilt = false;
    if apply && conversations_removed > 0 {
        match storage.rebuild_lexical_index_after_dedup() {
            Ok(()) => lexical_rebuilt = true,
            Err(err) => {
                tracing::warn!(
                    "duplicate cleanup removed rows but lexical rebuild failed: {err}; run 'cass index --full' to refresh search"
                );
            }
        }
    }

    // WS-B.5 (z2uon): an applied dedup deletes rows and rewrites the FTS
    // shadow; close through the checkpointing path so the next opener does
    // not replay that work from the WAL. Dry runs and no-op applies wrote
    // nothing and must not checkpoint anything.
    if apply
        && conversations_removed > 0
        && let Err(err) =
            crate::indexer::close_storage_with_wal_checkpoint(storage, &db_path, "dedup")
    {
        tracing::warn!(
            error = %format!("{err:#}"),
            db_path = %db_path.display(),
            "dedup: final WAL checkpoint did not complete"
        );
        eprintln!("Warning: final WAL checkpoint after dedup did not complete: {err:#}");
    }

    let messages_removed = result.messages_affected + mirror_result.messages_affected;
    let mut recommended_action = if conversations_removed == 0 {
        "No pre-existing duplicate conversations found. Nothing to do.".to_string()
    } else if apply {
        if lexical_rebuilt {
            format!(
                "Collapsed {conversations_removed} duplicate conversation(s) and rebuilt the in-DB FTS shadow. Run 'cass index --full' to also refresh the on-disk lexical search index."
            )
        } else {
            format!(
                "Collapsed {conversations_removed} duplicate conversation(s). Run 'cass index --full' to refresh the lexical index."
            )
        }
    } else {
        format!(
            "Found {conversations_removed} duplicate conversation(s) ({messages_removed} message rows). Re-run with --apply to collapse them."
        )
    };
    if mirror_result.unmatched > 0 {
        recommended_action.push_str(&format!(
            " {} conversation(s) from a remote mirror are archived under an agent that does not own that store and have no copy under the right agent, so they were kept; run 'cass index' to index them under the right agent, then dedup again.",
            mirror_result.unmatched
        ));
    }
    let misattributed_mirror = serde_json::json!({
        "conversations_removed": mirror_result.conversations_affected,
        "messages_removed": mirror_result.messages_affected,
        "unmatched": mirror_result.unmatched,
        "rows": mirror_result.rows,
    });

    if let Some(fmt) = structured_format {
        let pairs: Vec<serde_json::Value> = result
            .pairs
            .iter()
            .map(|pair| {
                serde_json::json!({
                    "keep_conversation_id": pair.keep_conversation_id,
                    "drop_conversation_id": pair.drop_conversation_id,
                    "source_path": pair.source_path,
                    "keep_external_id": pair.keep_external_id,
                    "drop_external_id": pair.drop_external_id,
                })
            })
            .collect();
        let payload = serde_json::json!({
            "schema_version": 1,
            "applied": apply,
            "dry_run": !apply,
            "db_path": db_path.display().to_string(),
            "db_exists": true,
            "conversations_collapsed": result.conversations_affected,
            "messages_removed": result.messages_affected,
            "lexical_rebuilt": lexical_rebuilt,
            "pairs": pairs,
            "misattributed_mirror": misattributed_mirror,
            "recommended_action": recommended_action,
        });
        return output_structured_value(payload, fmt);
    }

    println!("CASS Duplicate Conversation Cleanup");
    println!("===================================");
    println!();
    println!("Database: {}", db_path.display());
    println!(
        "Mode: {}",
        if apply {
            "APPLY (mutating)"
        } else {
            "dry-run (inspect only)"
        }
    );
    println!();
    if conversations_removed == 0 && mirror_result.unmatched == 0 {
        println!("No pre-existing duplicate conversations found.");
        return Ok(());
    }
    if result.conversations_affected > 0 {
        println!(
            "Duplicate conversation pairs (projects/<rel> twin vs bare canonical): {}",
            result.conversations_affected
        );
        println!(
            "Message rows in the dropped twins: {}",
            result.messages_affected
        );
        println!();
        for pair in result.pairs.iter().take(20) {
            println!(
                "  drop id={:<8} keep id={:<8} {}",
                pair.drop_conversation_id, pair.keep_conversation_id, pair.source_path
            );
        }
        if result.pairs.len() > 20 {
            println!("  ... and {} more", result.pairs.len() - 20);
        }
        println!();
    }
    if !mirror_result.rows.is_empty() {
        println!(
            "Remote mirror rows under an agent that does not own the store: {} duplicate(s) ({} message rows), {} without a copy under the right agent (kept)",
            mirror_result.conversations_affected,
            mirror_result.messages_affected,
            mirror_result.unmatched
        );
        println!();
        for row in mirror_result.rows.iter().take(20) {
            let action = match row.owner_conversation_id {
                Some(owner) => std::borrow::Cow::Owned(format!("drop (copy id={owner})")),
                None => std::borrow::Cow::Borrowed("keep (no copy)"),
            };
            println!(
                "  id={:<8} {:<12} {:<22} {}",
                row.conversation_id, row.agent, action, row.source_path
            );
        }
        if mirror_result.rows.len() > 20 {
            println!("  ... and {} more", mirror_result.rows.len() - 20);
        }
        println!();
    }
    println!("{recommended_action}");

    Ok(())
}

/// Remote mirror containers whose configured path names a known provider
/// store, with the archive agent slugs that own it: the ownership rule the
/// indexer applies to remote roots (mx520).
pub(crate) fn misattributed_mirror_containers(
    storage: &crate::storage::sqlite::FrankenStorage,
    data_dir: &Path,
) -> Vec<crate::storage::sqlite::MirrorContainerOwners> {
    crate::indexer::build_scan_roots(Some(storage), data_dir)
        .into_iter()
        .filter(|root| root.origin.is_remote())
        .filter_map(|root| {
            let owners = crate::indexer::mirrored_root_owners(&root)?;
            Some(crate::storage::sqlite::MirrorContainerOwners {
                source_id: root.origin.source_id.clone(),
                owner_agents: owners
                    .iter()
                    .map(|owner| public_connector_slug(owner).to_string())
                    .collect(),
                container: root.path,
            })
        })
        .collect()
}

/// Count files (not bytes) recursively under a directory for the storage
/// breakdown. Mirrors `fs_dir_size`'s traversal but counts leaf files.
pub(crate) fn storage_file_count(path: &std::path::Path) -> u64 {
    if !path.is_dir() {
        return 1;
    }
    std::fs::read_dir(path)
        .map(|entries| {
            entries
                .filter_map(std::result::Result::ok)
                .map(|e| {
                    let p = e.path();
                    if p.is_dir() {
                        storage_file_count(&p)
                    } else {
                        1
                    }
                })
                .sum()
        })
        .unwrap_or(0)
}

pub(crate) fn run_diag(
    data_dir_override: &Option<PathBuf>,
    db_override: Option<PathBuf>,
    output_format: Option<RobotFormat>,
    quarantine: bool,
    verbose: bool,
) -> CliResult<()> {
    use crate::franken_sync::compat::RowExt;
    use crate::franken_sync::params;
    use std::fs;

    let version = env!("CARGO_PKG_VERSION");
    let data_dir = resolve_data_dir(data_dir_override, db_override.as_ref());
    let db_path = db_override.unwrap_or_else(|| data_dir.join("agent_search.db"));
    // Use the actual versioned lexical index path without creating it during diagnostics.
    let index_path = crate::search::tantivy::expected_index_dir(&data_dir);

    // Check database existence and get stats
    let (db_exists, db_size, conversation_count, message_count) = if db_path.exists() {
        let size = fs::metadata(&db_path).map(|m| m.len()).unwrap_or(0);
        let (convs, msgs) = match open_franken_cli_read_db(
            db_path.clone(),
            "diagnostics",
            CLI_DIAG_DB_OPEN_TIMEOUT,
        ) {
            Ok(conn) => {
                let mut convs: i64 = franken_query_row_map_retry(
                    &conn,
                    "SELECT COUNT(*) FROM conversations",
                    params![],
                    |r| r.get_typed(0),
                )
                .unwrap_or(0);
                let mut msgs: i64 = franken_query_row_map_retry(
                    &conn,
                    "SELECT COUNT(*) FROM messages",
                    params![],
                    |r| r.get_typed(0),
                )
                .unwrap_or(0);
                if convs == 0 {
                    convs = fresh_franken_count_retry(
                        &db_path,
                        "diagnostics",
                        CLI_DIAG_DB_OPEN_TIMEOUT,
                        "SELECT COUNT(*) FROM conversations",
                        params![],
                    )
                    .unwrap_or(0);
                }
                if msgs == 0 {
                    msgs = fresh_franken_count_retry(
                        &db_path,
                        "diagnostics",
                        CLI_DIAG_DB_OPEN_TIMEOUT,
                        "SELECT COUNT(*) FROM messages",
                        params![],
                    )
                    .unwrap_or(0);
                }
                (convs, msgs)
            }
            Err(e) => {
                tracing::warn!("failed to open database for diagnostics: {e}");
                (-1, -1)
            }
        };
        (true, size, convs, msgs)
    } else {
        (false, 0, 0, 0)
    };

    // Check index existence
    let (index_exists, index_size) = if crate::search::tantivy::searchable_index_exists(&index_path)
    {
        let size = fs_dir_size(&index_path);
        (true, size)
    } else {
        (false, 0)
    };

    // Agent search paths - compute path once, then check existence
    let home = dirs::home_dir().unwrap_or_default();
    let config_dir = dirs::config_dir().unwrap_or_default();

    let agent_paths: Vec<(String, PathBuf, bool)> = diagnostics_connector_paths(&home, &config_dir)
        .into_iter()
        .map(|(name, path)| {
            let exists = path.exists();
            (name, path, exists)
        })
        .collect();
    let mut connector_paths_by_name: HashMap<String, Vec<PathBuf>> = HashMap::new();
    for (name, path, exists) in &agent_paths {
        if *exists {
            connector_paths_by_name
                .entry(public_connector_slug(name).to_string())
                .or_default()
                .push(path.clone());
        }
    }
    let connector_rows: Vec<(String, String, bool)> = capabilities_connector_names()
        .into_iter()
        .map(|name| {
            let path = connector_paths_by_name
                .get(&name)
                .and_then(|paths| paths.first())
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "(not detected)".to_string());
            let found = path != "(not detected)";
            (name, path, found)
        })
        .collect();

    let platform = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    let quarantine_report = quarantine.then(|| {
        let mut report = collect_diag_quarantine_report(&data_dir, &index_path);
        report.lexical_rebuild_quarantine = Some(
            crate::search::lexical_quarantine_diagnostics::collect(&data_dir),
        );
        report
    });

    let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
        if matches!(fmt, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            fmt
        }
    });

    if let Some(fmt) = structured_format {
        let mut payload = serde_json::json!({
            "version": version,
            "platform": { "os": platform, "arch": arch },
            "paths": {
                "data_dir": data_dir.display().to_string(),
                "db_path": db_path.display().to_string(),
                "index_path": index_path.display().to_string(),
            },
            "database": {
                "exists": db_exists,
                "size_bytes": db_size,
                "conversations": conversation_count,
                "messages": message_count,
            },
            "index": {
                "exists": index_exists,
                "size_bytes": index_size,
            },
            "connectors": connector_rows.iter().map(|(name, path, found)| {
                serde_json::json!({
                    "name": name,
                    "path": path,
                    "found": found,
                })
            }).collect::<Vec<_>>(),
        });
        if let Some(report) = quarantine_report.as_ref() {
            payload["quarantine"] = serde_json::json!(report);
        }
        return output_structured_value(payload, fmt);
    }

    println!("CASS Diagnostic Report");
    println!("======================");
    println!();
    println!("Version: {version}");
    println!("Platform: {platform} ({arch})");
    println!();
    println!("Paths:");
    println!("  Data directory: {}", data_dir.display());
    println!("  Database: {}", db_path.display());
    println!("  Lexical index: {}", index_path.display());
    println!();
    println!("Database Status:");
    if db_exists {
        println!("  Status: OK");
        if verbose {
            println!("  Size: {}", format_bytes(db_size));
        }
        println!("  Conversations: {conversation_count}");
        println!("  Messages: {message_count}");
    } else {
        println!("  Status: NOT FOUND");
        println!("  Hint: Run 'cass index --full' to create the database");
    }
    println!();
    println!("Index Status:");
    if index_exists {
        println!("  Status: OK");
        if verbose {
            println!("  Size: {}", format_bytes(index_size));
        }
    } else {
        println!("  Status: NOT FOUND");
        println!("  Hint: Run 'cass index --full' to create the index");
    }
    println!();
    println!("Connector Search Paths:");
    for (name, path, found) in &connector_rows {
        let status = if *found { "✓" } else { "✗" };
        println!("  {} {}: {}", status, name, path);
    }
    if let Some(report) = quarantine_report.as_ref() {
        println!();
        println!("Quarantine:");
        if let Some(rebuilds) = &report.lexical_rebuild_quarantine {
            for line in rebuilds.human_lines() {
                println!("  {line}");
            }
        }
        println!(
            "  Failed seed bundles: {}",
            report.summary.failed_seed_bundle_count
        );
        println!(
            "  Retained publish backups: {}",
            report.summary.retained_publish_backup_count
        );
        println!(
            "  Quarantined lexical generations: {}",
            report.summary.lexical_quarantined_generation_count
        );
        println!(
            "  Quarantined lexical shards: {}",
            report.summary.lexical_quarantined_shard_count
        );
        println!(
            "  Total retained bytes: {}",
            report.summary.total_retained_bytes
        );
        println!(
            "  Retained publish backup cap: {}",
            report.summary.retained_publish_backup_retention_limit
        );
        println!(
            "  GC-eligible assets: {}",
            report.summary.gc_eligible_asset_count
        );
        println!("  GC-eligible bytes: {}", report.summary.gc_eligible_bytes);
        println!(
            "  Inspection-required assets: {}",
            report.summary.inspection_required_asset_count
        );
        println!(
            "  Inspection-required bytes: {}",
            report.summary.inspection_required_bytes
        );
        if !report.failed_seed_bundle_files.is_empty() {
            println!("  Failed seed bundle files:");
            for artifact in &report.failed_seed_bundle_files {
                println!(
                    "    - {} ({} bytes, age_s={}, last_read_ms={}, gc={}, reason={})",
                    artifact.path,
                    artifact.size_bytes,
                    format_optional_u64(artifact.age_seconds),
                    format_optional_i64(artifact.last_read_at_ms),
                    artifact.safe_to_gc,
                    artifact.gc_reason
                );
            }
        }
        if !report.retained_publish_backups.is_empty() {
            println!("  Retained publish backups:");
            for artifact in &report.retained_publish_backups {
                println!(
                    "    - {} ({} bytes, age_s={}, last_read_ms={}, gc={}, reason={})",
                    artifact.path,
                    artifact.size_bytes,
                    format_optional_u64(artifact.age_seconds),
                    format_optional_i64(artifact.last_read_at_ms),
                    artifact.safe_to_gc,
                    artifact.gc_reason
                );
            }
        }
        if !report.lexical_generations.is_empty() {
            println!("  Quarantined lexical generations:");
            for generation in &report.lexical_generations {
                println!(
                    "    - {} [{}] shards={}/{} bytes={} age_s={} last_read_ms={} reclaimable_bytes={} inspection_required={} gc={} reason={}",
                    generation.path,
                    generation.publish_state,
                    generation.quarantined_shards,
                    generation.total_shards,
                    generation.artifact_bytes,
                    format_optional_u64(generation.age_seconds),
                    format_optional_i64(generation.last_read_at_ms),
                    generation.reclaimable_bytes,
                    generation.inspection_required,
                    generation.safe_to_gc,
                    generation.gc_reason
                );
            }
        }
        if verbose && !report.warnings.is_empty() {
            println!("  Scan warnings:");
            for warning in &report.warnings {
                println!("    - {warning}");
            }
        }
    }

    Ok(())
}
