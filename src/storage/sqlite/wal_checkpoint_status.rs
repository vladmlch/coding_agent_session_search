//! Local fix (review 2026-11-02, S1).
//!
//! `PRAGMA wal_checkpoint(TRUNCATE)` reports a blocked checkpoint as a
//! successful query whose status row carries `busy = 1`, or fewer
//! backfilled frames than logged ones. Recovery code must read that row
//! instead of trusting `Ok(_)`. Same rule as the indexer's
//! `classify_final_wal_checkpoint`.

use crate::franken_sync::{Row, compat::RowExt};

/// True only when nothing was busy and every logged frame was backfilled.
pub(super) fn truncate_status_is_complete(
    busy: i64,
    log_frames: i64,
    checkpointed_frames: i64,
) -> bool {
    busy == 0 && !(log_frames > 0 && checkpointed_frames < log_frames)
}

/// Reads the `(busy, log, checkpointed)` status row. A missing or
/// unreadable row is not proof of completion.
pub(super) fn truncate_rows_report_complete(rows: &[Row]) -> bool {
    let Some(row) = rows.first() else {
        return false;
    };
    let (Ok(busy), Ok(log_frames), Ok(checkpointed_frames)) = (
        row.get_typed::<i64>(0),
        row.get_typed::<i64>(1),
        row.get_typed::<i64>(2),
    ) else {
        return false;
    };
    truncate_status_is_complete(busy, log_frames, checkpointed_frames)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn status_tuple_classification() {
        assert!(truncate_status_is_complete(0, 0, 0));
        assert!(truncate_status_is_complete(0, 120, 120));
        assert!(!truncate_status_is_complete(1, 120, 80));
        assert!(!truncate_status_is_complete(1, 120, 120));
        assert!(!truncate_status_is_complete(0, 120, 80));
    }

    fn rows_for(sql: &str) -> Vec<Row> {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("rows.db");
        let conn =
            crate::franken_sync::Connection::open(path.to_string_lossy().into_owned()).unwrap();
        let rows = conn.query(sql).unwrap();
        conn.close().unwrap();
        rows
    }

    #[test]
    fn rows_reader_rejects_busy_missing_and_malformed_rows() {
        assert!(truncate_rows_report_complete(&rows_for("SELECT 0, 0, 0;")));
        assert!(!truncate_rows_report_complete(&rows_for(
            "SELECT 1, 120, 80;"
        )));
        assert!(!truncate_rows_report_complete(&rows_for(
            "SELECT 1, 120, 120;"
        )));
        assert!(!truncate_rows_report_complete(&rows_for(
            "SELECT 'x', 0, 0;"
        )));
        assert!(!truncate_rows_report_complete(&[]));
    }
}
