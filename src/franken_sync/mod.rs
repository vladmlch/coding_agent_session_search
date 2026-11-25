//! CASS — synchronous SQLite facade backed by `rusqlite` (bundled C SQLite).
//!
//! The public surface mirrors the historical FrankenSQLite facade:
//! `Connection`, `PreparedStatement`, `compat::{ConnectionExt, Transaction,
//! TransactionExt}`, `Row`, `SqliteValue`, `FrankenError`, `FileIdentity`,
//! `params!`, cancellation scopes, `shutdown_driver`, and `BackupReport`.
//!
//! Underneath, `rusqlite::Connection` (bundled SQLite C engine) executes
//! every operation synchronously and directly on the calling thread — no
//! async runtime, no engine thread. Cancellation is therefore implemented
//! with real SQLite primitives: `sqlite3_interrupt` via `InterruptHandle`,
//! slice-bounded `busy_timeout` retry loops, and a shared deadline timer
//! thread that interrupts running statements when their deadline expires.
//!
//! Threading contract: `Connection` is `Send` but not `Sync`; keep each
//! `Connection` on one thread. `CancellationHandle` and `DeadlineGuard` are
//! `Send + Sync` and interrupt work from any thread. rusqlite's statement
//! cache makes `Connection` `!Sync`; cloning the handle keeps a cheap `Arc`.

mod connection;
#[path = "compat.rs"]
mod compat_surface;
mod deadline_and_cancel;
mod sidecar_probes;
mod statement;
#[cfg(test)]
mod tests;
mod value_and_error;

/// Re-exported interrupt handle so hot paths can cancel work on other threads.
pub use rusqlite::InterruptHandle;
/// Typed transaction begin behavior (`Deferred`/`Immediate`/`Exclusive`).
pub use rusqlite::TransactionBehavior;

pub use crate::__cass_franken_params as params;
#[cfg(not(target_arch = "wasm32"))]
pub use compat_surface::BackupReport;
pub use compat_surface::{compat, migrate};
pub(crate) use compat_surface::shutdown_driver;
pub use connection::Connection;
#[allow(unused_imports)]
pub(crate) use deadline_and_cancel::check_connection_admission;
pub use deadline_and_cancel::{CancellationHandle, CancellationScope, DeadlineGuard};
pub use sidecar_probes::AsyncView;
pub(crate) use sidecar_probes::{
    SqliteHeaderStamp, probe_sqlite_header_stamp_safely, record_live_archive_stamp,
};
pub use statement::PreparedStatement;
pub use value_and_error::{ErrorCode, FileIdentity, FrankenError, Row, SqliteValue};
