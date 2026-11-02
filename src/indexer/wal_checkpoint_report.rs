//! Local fix (review 2026-11-02, I2).
//!
//! `close_storage_after_index` deliberately returns `Ok(())` for a blocked or
//! never-started final checkpoint (lenient index-close contract). The
//! analytics rebuild reports checkpoint state to the user, so it needs the
//! outcome itself. This is a sibling with the same close + bounded-checkpoint
//! sequence that returns the outcome; existing callers are untouched.

use std::path::Path;

use anyhow::{Context, Result};
use serde_json::json;

use super::{
    AbortWalCheckpointAttempt, FinalWalCheckpointOutcome, final_wal_checkpoint_timeout,
    prepare_storage_for_final_checkpoint, run_bounded_abort_wal_checkpoint,
    run_final_wal_checkpoint,
};
use crate::storage::sqlite::FrankenStorage;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WalCheckpointReport {
    Completed,
    Blocked {
        busy: i64,
        log_frames: i64,
        checkpointed_frames: i64,
    },
    NotAttempted {
        reason: String,
    },
}

pub(crate) fn close_storage_with_wal_checkpoint_report(
    storage: FrankenStorage,
    db_path: &Path,
    context: &str,
) -> Result<WalCheckpointReport> {
    prepare_storage_for_final_checkpoint(&storage, db_path, context);
    storage.close().with_context(|| {
        format!(
            "closing canonical db before final WAL checkpoint after {context}: {}",
            db_path.display()
        )
    })?;
    let timeout = final_wal_checkpoint_timeout();
    match run_bounded_abort_wal_checkpoint(db_path.to_path_buf(), timeout, context, |path| {
        run_final_wal_checkpoint(path, context)
    }) {
        AbortWalCheckpointAttempt::Finished(Ok(FinalWalCheckpointOutcome::Completed)) => {
            Ok(WalCheckpointReport::Completed)
        }
        AbortWalCheckpointAttempt::Finished(Ok(FinalWalCheckpointOutcome::Blocked {
            busy,
            log_frames,
            checkpointed_frames,
        })) => Ok(WalCheckpointReport::Blocked {
            busy,
            log_frames,
            checkpointed_frames,
        }),
        AbortWalCheckpointAttempt::Finished(Err(error)) => Err(anyhow::anyhow!(
            "final WAL checkpoint after {context} failed: {error}"
        )),
        AbortWalCheckpointAttempt::DeadlineUnavailable(error) => {
            tracing::warn!(
                db_path = %db_path.display(),
                context,
                %error,
                "final WAL checkpoint deadline timer was unavailable; the WAL is left for the next opener"
            );
            Ok(WalCheckpointReport::NotAttempted { reason: error })
        }
    }
}

/// `(wal_checkpoint, wal_checkpoint_detail)` for the analytics payload. The
/// first value keeps the documented `completed | failed` enum.
pub(crate) fn analytics_wal_checkpoint_fields(
    report: &WalCheckpointReport,
) -> (&'static str, serde_json::Value) {
    match report {
        WalCheckpointReport::Completed => ("completed", json!({"status": "completed"})),
        WalCheckpointReport::Blocked {
            busy,
            log_frames,
            checkpointed_frames,
        } => (
            "failed",
            json!({
                "status": "blocked",
                "busy": busy,
                "log_frames": log_frames,
                "checkpointed_frames": checkpointed_frames,
            }),
        ),
        WalCheckpointReport::NotAttempted { reason } => (
            "failed",
            json!({"status": "not_attempted", "reason": reason}),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_completed_checkpoint_reports_completed() {
        assert_eq!(
            analytics_wal_checkpoint_fields(&WalCheckpointReport::Completed),
            ("completed", json!({"status": "completed"}))
        );
        let (state, detail) = analytics_wal_checkpoint_fields(&WalCheckpointReport::Blocked {
            busy: 1,
            log_frames: 120,
            checkpointed_frames: 80,
        });
        assert_eq!(state, "failed");
        assert_eq!(detail["status"], "blocked");
        assert_eq!(detail["checkpointed_frames"], 80);
        let (state, detail) = analytics_wal_checkpoint_fields(&WalCheckpointReport::NotAttempted {
            reason: "timer unavailable".into(),
        });
        assert_eq!(state, "failed");
        assert_eq!(detail["status"], "not_attempted");
    }
}
