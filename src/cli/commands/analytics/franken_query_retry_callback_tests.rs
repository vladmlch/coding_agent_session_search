use crate::cli::commands::analytics::run_analytics::{
    franken_query_map_collect_retry, franken_query_row_map_retry,
};
use crate::franken_sync::{Connection, FrankenError};
use anyhow::Result;
use std::cell::Cell;

pub(crate) fn seeded_connection(dir: &std::path::Path) -> Connection {
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
pub(crate) fn row_map_callback_failure_is_not_replayed() {
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
pub(crate) fn map_collect_callback_failure_is_not_replayed() {
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
