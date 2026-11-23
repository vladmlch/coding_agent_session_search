//! Local fix (review 2026-11-02, I2).
//!
//! `close_storage_after_index` deliberately returns `Ok(())` for a blocked or
//! never-started final checkpoint (lenient index-close contract). The
//! analytics rebuild reports checkpoint state to the user, so it needs the
//! outcome itself. This is a sibling with the same close + bounded-checkpoint
//! sequence that returns the outcome; existing callers are untouched.

use serde_json::json;

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
