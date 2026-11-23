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

/// Open a read-only frankensqlite connection for analytics queries.
pub(crate) fn open_franken_analytics_db(
    data_dir: &Option<PathBuf>,
    db_path_override: Option<&PathBuf>,
) -> CliResult<crate::franken_sync::Connection> {
    open_franken_cli_read_db(
        analytics_db_path(data_dir, db_path_override),
        "analytics",
        Duration::from_secs(1),
    )
}

pub(crate) fn analytics_db_path(
    data_dir: &Option<PathBuf>,
    db_path_override: Option<&PathBuf>,
) -> PathBuf {
    let data_dir = data_dir.clone().unwrap_or_else(default_data_dir);
    db_path_override
        .cloned()
        .unwrap_or_else(|| data_dir.join("agent_search.db"))
}

pub(crate) fn open_franken_cli_read_db(
    path: PathBuf,
    reason: &str,
    busy_timeout: Duration,
) -> CliResult<crate::franken_sync::Connection> {
    if !path.exists() {
        return Err(CliError {
            code: 3,
            kind: CliErrorKind::MissingDb.kind_str(),
            message: format!(
                "Database not found at {}. Run 'cass index --full' first.",
                path.display()
            ),
            hint: Some("Run 'cass index --full' to create the database.".into()),
            retryable: true,
        });
    }

    let conn = match crate::storage::sqlite::open_franken_readonly_storage_with_timeout(
        &path,
        busy_timeout,
    ) {
        Ok(storage) => storage.into_raw(),
        Err(err) => {
            let readonly_retryable = crate::storage::sqlite::retryable_franken_anyhow(&err);
            match crate::storage::sqlite::open_franken_raw_readonly_connection_with_timeout(
                &path,
                busy_timeout,
            ) {
                Ok(conn) => conn,
                Err(raw_readonly_err) => {
                    let raw_readonly_retryable =
                        crate::storage::sqlite::retryable_franken_anyhow(&raw_readonly_err);
                    let message = format!(
                        "Failed to open {reason} database at {}: readonly storage open failed ({err}); raw readonly open failed ({raw_readonly_err})",
                        path.display()
                    );
                    if let Some(fts_err) =
                        crate::storage::sqlite::fts_messages_integrity_error_from_message(&message)
                    {
                        return Err(fts_messages_integrity_cli_error(reason, fts_err.into()));
                    }
                    return Err(CliError {
                        code: 9,
                        kind: CliErrorKind::DbOpen.kind_str(),
                        message,
                        hint: None,
                        retryable: readonly_retryable || raw_readonly_retryable,
                    });
                }
            }
        }
    };

    let timeout_ms = busy_timeout.as_millis().clamp(1, u128::from(u32::MAX));
    let _ = conn.execute(&format!("PRAGMA busy_timeout = {timeout_ms};"));
    let _ = conn.execute("PRAGMA query_only = 1;");

    Ok(conn)
}

pub(crate) fn open_franken_cli_read_db_with_hard_timeout(
    path: PathBuf,
    reason: &str,
    timeout: Duration,
) -> CliResult<crate::storage::sqlite::FrankenOwnerConnection> {
    let display_path = path.display().to_string();
    let reason = reason.to_string();
    if let Some(err) = sqlite_header_preflight_error(&path, &display_path, &reason) {
        return Err(err);
    }
    let (tx, rx) = std::sync::mpsc::channel();
    let _open_worker = std::thread::spawn({
        let reason = reason.clone();
        move || {
            let result = open_franken_cli_owner_read_db(path, &reason, timeout);
            if let Err(std::sync::mpsc::SendError(Ok(mut conn))) = tx.send(result)
                && let Err(err) = conn.close_without_checkpoint_sync()
            {
                warn!(
                    error = %err,
                    reason = %reason,
                    "hard-timeout CLI read open completed after its receiver exited; owner-thread cleanup failed"
                );
            }
        }
    });

    receive_franken_cli_read_db_open_result_with_hard_timeout(rx, display_path, reason, timeout)
}

/// Open a read-only handle whose raw engine state never leaves its dedicated
/// owner thread. This is the only handle the CLI hard-timeout lane may send
/// between the open worker, probe worker, and caller.
pub(crate) fn open_franken_cli_owner_read_db(
    path: PathBuf,
    reason: &str,
    busy_timeout: Duration,
) -> CliResult<crate::storage::sqlite::FrankenOwnerConnection> {
    if !path.exists() {
        return Err(CliError {
            code: 3,
            kind: CliErrorKind::MissingDb.kind_str(),
            message: format!(
                "Database not found at {}. Run 'cass index --full' first.",
                path.display()
            ),
            hint: Some("Run 'cass index --full' to create the database.".into()),
            retryable: true,
        });
    }

    let conn = crate::storage::sqlite::open_franken_owner_readonly_connection_with_timeout(
        &path,
        busy_timeout,
    )
    .map_err(|err| {
        let retryable = crate::storage::sqlite::retryable_franken_anyhow(&err);
        let message = format!(
            "Failed to open {reason} database at {}: dedicated-owner readonly open failed ({err})",
            path.display()
        );
        if let Some(fts_err) =
            crate::storage::sqlite::fts_messages_integrity_error_from_message(&message)
        {
            return fts_messages_integrity_cli_error(reason, fts_err.into());
        }
        CliError {
            code: 9,
            kind: CliErrorKind::DbOpen.kind_str(),
            message,
            hint: None,
            retryable,
        }
    })?;

    let timeout_ms = busy_timeout.as_millis().clamp(1, u128::from(u32::MAX));
    let _ = conn.execute(&format!("PRAGMA busy_timeout = {timeout_ms};"));
    let _ = conn.execute("PRAGMA query_only = 1;");
    let _ = conn.execute("PRAGMA cache_size = -65536;");
    let _ = conn.execute("PRAGMA foreign_keys = ON;");
    Ok(conn)
}

pub(crate) fn sqlite_header_preflight_error(
    path: &Path,
    display_path: &str,
    reason: &str,
) -> Option<CliError> {
    const SQLITE_HEADER: &[u8; 16] = b"SQLite format 3\0";

    let metadata = path.metadata().ok()?;
    if !metadata.is_file() {
        return None;
    }
    if metadata.len() < SQLITE_HEADER.len() as u64 {
        return Some(CliError {
            code: 9,
            kind: CliErrorKind::DbOpen.kind_str(),
            message: format!(
                "Failed to open {reason} database at {display_path}: file is too small to contain a SQLite header (possible corruption)"
            ),
            hint: None,
            retryable: true,
        });
    }

    let mut file = std::fs::File::open(path).ok()?;
    let mut header = [0_u8; SQLITE_HEADER.len()];
    if file.read_exact(&mut header).is_err() {
        return None;
    }
    if &header == SQLITE_HEADER {
        return None;
    }

    Some(CliError {
        code: 9,
        kind: CliErrorKind::DbOpen.kind_str(),
        message: format!(
            "Failed to open {reason} database at {display_path}: file header is not SQLite format 3 (possible corruption)"
        ),
        hint: None,
        retryable: true,
    })
}

pub(crate) fn receive_franken_cli_read_db_open_result_with_hard_timeout(
    rx: std::sync::mpsc::Receiver<CliResult<crate::storage::sqlite::FrankenOwnerConnection>>,
    display_path: String,
    reason: String,
    timeout: Duration,
) -> CliResult<crate::storage::sqlite::FrankenOwnerConnection> {
    match rx.recv_timeout(timeout) {
        Ok(Ok(conn)) => Ok(conn),
        Ok(Err(err)) => Err(err),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => Err(CliError {
            code: 9,
            kind: CliErrorKind::DbOpen.kind_str(),
            message: format!(
                "Failed to open {reason} database at {display_path}: open timed out after {}s (possible corruption or lock contention)",
                timeout.as_secs()
            ),
            hint: None,
            retryable: true,
        }),
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Err(CliError {
            code: 9,
            kind: CliErrorKind::DbOpen.kind_str(),
            message: format!(
                "Failed to open {reason} database at {display_path}: open worker disconnected"
            ),
            hint: None,
            retryable: true,
        }),
    }
}

pub(crate) fn close_franken_cli_owner_read_db(
    mut conn: crate::storage::sqlite::FrankenOwnerConnection,
    path: &Path,
    reason: &str,
) -> CliResult<()> {
    if let Err(err) = conn.close_without_checkpoint_sync() {
        warn!(
            error = %err,
            db_path = %path.display(),
            reason,
            "dedicated-owner CLI read probe close failed; dropping the handle for owner-thread cleanup"
        );
    }
    Ok(())
}

pub(crate) fn close_franken_cli_read_db(
    mut conn: crate::franken_sync::Connection,
    path: &Path,
    reason: &str,
) -> CliResult<()> {
    if let Err(err) = conn.close_in_place() {
        warn!(
            error = %err,
            db_path = %path.display(),
            reason,
            "close_in_place failed for CLI read probe; falling back to best-effort close"
        );
        conn.close_best_effort_in_place();
    }
    Ok(())
}

pub(crate) fn close_franken_cli_read_db_without_checkpoint(
    mut conn: crate::franken_sync::Connection,
    path: &Path,
    reason: &str,
) -> CliResult<()> {
    if let Err(err) = conn.close_without_checkpoint_in_place() {
        warn!(
            error = %err,
            db_path = %path.display(),
            reason,
            "checkpoint-free close failed for CLI read probe; falling back to best-effort owner-thread cleanup"
        );
        conn.close_best_effort_in_place();
    }
    Ok(())
}

pub(crate) fn fts_messages_integrity_cli_error(surface: &str, err: anyhow::Error) -> CliError {
    CliError {
        code: 5,
        kind: CliErrorKind::Storage.kind_str(),
        message: format!("{surface} cannot continue: {err:#}"),
        hint: Some(crate::storage::sqlite::FTS_MESSAGES_CORRUPTION_RECOVERY_HINT.to_string()),
        retryable: false,
    }
}

pub(crate) fn validate_fts_messages_integrity_for_cli(
    conn: &crate::franken_sync::Connection,
    surface: &str,
) -> CliResult<()> {
    crate::storage::sqlite::validate_fts_messages_integrity_for_connection(conn)
        .map_err(|err| fts_messages_integrity_cli_error(surface, err))
}

pub(crate) fn franken_query_row_map_retry<T, F>(
    conn: &crate::franken_sync::Connection,
    sql: &str,
    params: &[crate::franken_sync::compat::ParamValue],
    map: F,
) -> Result<T, crate::franken_sync::FrankenError>
where
    F: Copy + Fn(&crate::franken_sync::Row) -> Result<T, crate::franken_sync::FrankenError>,
{
    let deadline = std::time::Instant::now() + CLI_DB_QUERY_RETRY_TIMEOUT;
    let mut backoff = Duration::from_millis(4);
    // negs9: a failure raised by the row-mapping callback is NOT engine
    // contention, even when its text resembles one ("busy"/"locked"), and
    // must never cause the query — and the callback — to be replayed.
    let callback_failed = std::cell::Cell::new(false);
    loop {
        callback_failed.set(false);
        let mapped = conn.query_row_map(sql, params, |row| {
            map(row).inspect_err(|_| callback_failed.set(true))
        });
        match mapped {
            Ok(value) => return Ok(value),
            Err(err) if callback_failed.get() => return Err(err),
            Err(err) if crate::storage::sqlite::retryable_franken_error(&err) => {
                let now = std::time::Instant::now();
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

pub(crate) fn franken_query_map_collect_retry<T, F>(
    conn: &crate::franken_sync::Connection,
    sql: &str,
    params: &[crate::franken_sync::compat::ParamValue],
    map: F,
) -> Result<Vec<T>, crate::franken_sync::FrankenError>
where
    F: Copy + Fn(&crate::franken_sync::Row) -> Result<T, crate::franken_sync::FrankenError>,
{
    let deadline = std::time::Instant::now() + CLI_DB_QUERY_RETRY_TIMEOUT;
    let mut backoff = Duration::from_millis(4);
    // negs9: callback failures are propagated exactly once, never replayed.
    let callback_failed = std::cell::Cell::new(false);
    loop {
        callback_failed.set(false);
        let mapped = conn.query_map_collect(sql, params, |row| {
            map(row).inspect_err(|_| callback_failed.set(true))
        });
        match mapped {
            Ok(values) => return Ok(values),
            Err(err) if callback_failed.get() => return Err(err),
            Err(err) if crate::storage::sqlite::retryable_franken_error(&err) => {
                let now = std::time::Instant::now();
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

pub(crate) fn fresh_franken_count_retry(
    path: &Path,
    reason: &str,
    busy_timeout: Duration,
    sql: &str,
    params: &[crate::franken_sync::compat::ParamValue],
) -> Option<i64> {
    let deadline = std::time::Instant::now() + CLI_DB_QUERY_RETRY_TIMEOUT;
    let mut backoff = Duration::from_millis(4);
    loop {
        let conn = match open_franken_cli_read_db(path.to_path_buf(), reason, busy_timeout) {
            Ok(conn) => conn,
            Err(err) => {
                if !err.retryable {
                    return None;
                }
                let now = std::time::Instant::now();
                if now >= deadline {
                    return None;
                }
                let remaining = deadline.saturating_duration_since(now);
                crate::storage::sqlite::sleep_with_franken_retry_backoff(
                    &mut backoff,
                    remaining,
                    Duration::from_millis(64),
                );
                continue;
            }
        };

        let result = conn.query_row_map(sql, params, |row| row.get_typed::<i64>(0));
        let _ = close_franken_cli_read_db(conn, path, reason);
        match result {
            Ok(value) => return Some(value),
            Err(err) if crate::storage::sqlite::retryable_franken_error(&err) => {
                let now = std::time::Instant::now();
                if now >= deadline {
                    return None;
                }
                let remaining = deadline.saturating_duration_since(now);
                crate::storage::sqlite::sleep_with_franken_retry_backoff(
                    &mut backoff,
                    remaining,
                    Duration::from_millis(64),
                );
            }
            Err(_) => return None,
        }
    }
}

#[cfg(test)]
mod franken_query_retry_callback_tests {
    use super::{franken_query_map_collect_retry, franken_query_row_map_retry};
    use crate::franken_sync::{Connection, FrankenError};
    use std::cell::Cell;

    fn seeded_connection(dir: &std::path::Path) -> Connection {
        let path = dir.join("callback-once.db");
        let conn = Connection::open(path.to_string_lossy().into_owned()).expect("open");
        conn.execute_batch("CREATE TABLE t (x INTEGER); INSERT INTO t (x) VALUES (1);")
            .expect("seed");
        conn
    }

    /// negs9: a callback failure whose class resembles engine contention
    /// (`Busy`) must be propagated exactly once, never replayed until the
    /// 10s CLI retry deadline.
    #[test]
    fn row_map_callback_failure_is_not_replayed() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let conn = seeded_connection(tmp.path());
        let calls = Cell::new(0u32);
        let started = std::time::Instant::now();
        let result: Result<i64, FrankenError> =
            franken_query_row_map_retry(&conn, "SELECT x FROM t", &[], |_row| {
                calls.set(calls.get() + 1);
                Err(FrankenError::Busy)
            });
        assert!(result.is_err());
        assert!(
            calls.get().cmp(&1).is_eq(),
            "callback invoked {} times",
            calls.get()
        );
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
    }

    #[test]
    fn map_collect_callback_failure_is_not_replayed() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let conn = seeded_connection(tmp.path());
        let calls = Cell::new(0u32);
        let result: Result<Vec<i64>, FrankenError> =
            franken_query_map_collect_retry(&conn, "SELECT x FROM t", &[], |_row| {
                calls.set(calls.get() + 1);
                Err(FrankenError::Busy)
            });
        assert!(result.is_err());
        assert!(
            calls.get().cmp(&1).is_eq(),
            "callback invoked {} times",
            calls.get()
        );
    }
}
