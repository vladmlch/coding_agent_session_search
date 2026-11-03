//! Bridge-facing surface for the Task-11 change journal.
//!
//! The journal implementation lives in
//! [`coding_agent_search::viewer_extensions::changes`] because the stock
//! indexer (library code) must reach the same `notify_committed_change` /
//! `append_change_event` helpers — a separate indexer process notifies a
//! bridge through the SQLite journal, never through shared memory (local
//! IPC — the Task-12 event stream — is wakeup-only). This module is the
//! stable name the RPC surface binds: `mod changes;` is registered in
//! `viewer_bridge/mod.rs` and `changes_since` serves the `changes_since`
//! RPC op; the push channel lives in `event_stream.rs`.
#![allow(dead_code, unused_imports)]

use anyhow::Result;
use coding_agent_search::franken_sync::Connection;
use coding_agent_search::viewer_extensions::changes as journal;

pub use coding_agent_search::viewer_extensions::changes::{
    CHANGES_PAGE_LIMIT, ChangeEvent, ChangePage, JOURNAL_RETENTION, JournalRevisions,
    append_change_event, kind, notify_committed_change, notify_committed_change_best_effort,
    notify_committed_change_with_retention,
};

/// `changes_since(after_seq, limit)` — bounded replay of the committed
/// journal: `{events, next_seq, truncated, current_revisions}`.
///
/// * `after_seq` is the caller's last consumed durable seq (`0` replays
///   the retained journal head); `next_seq` is the resume token.
/// * `truncated` + a `reset_required` event answers an expired cursor or a
///   seq regression (archive replacement); consumers refetch coarsely and
///   resume at `next_seq`.
/// * `current_revisions` binds the committed logical generations; a
///   canonical revision the journal cannot explain yields a trailing
///   coarse `session_updated` (`conversation_id = NULL`).
///
/// Read-only: safe on the `StrictArchive` connection under snapshot
/// discipline. RPC wiring lands with Task 12.
pub fn changes_since(connection: &Connection, after_seq: i64, limit: usize) -> Result<ChangePage> {
    journal::changes_since(connection, after_seq, limit)
}
