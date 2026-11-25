use std::cell::RefCell;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::{Duration, Instant};

use rusqlite::config::DbConfig;
use rusqlite::types::ToSql;

#[cfg(not(target_arch = "wasm32"))]
use super::BackupReport;
#[cfg(not(target_arch = "wasm32"))]
use super::compat_surface::read_backup_report;
use super::deadline_and_cancel::{current_cancellation, current_is_cancelled};
use super::sidecar_probes::{
    BusyTimeoutRestore, register_live_connection, row_i64, unregister_live_connection,
    update_live_wal_state,
};
use super::value_and_error::materialize_row;
use super::{
    AsyncView, DeadlineGuard, FileIdentity, FrankenError, PreparedStatement, Row, SqliteValue,
    check_connection_admission, compat,
};

// ---------------------------------------------------------------------------
// Operation helpers
// ---------------------------------------------------------------------------

/// `&[&dyn ToSql]` view over a `SqliteValue` slice for `stmt.query`/`execute`.
pub(super) fn dynamic_params<'a>(values: &'a [SqliteValue]) -> Vec<&'a (dyn ToSql + 'a)> {
    values
        .iter()
        .map(|value| value as &(dyn ToSql + 'a))
        .collect()
}

/// Detect a lock-taking `BEGIN IMMEDIATE` / `BEGIN EXCLUSIVE`
/// (`TRANSACTION`/`TRAN` suffix optional, case-insensitive, whitespace
/// tolerant, one trailing `;` allowed) so the execute entry points can
/// route them through `begin_with_retry` — the slice-bounded busy-retry
/// path — instead of firing a single non-retried attempt. Plain `BEGIN`
/// (deferred) takes no lock and keeps the direct path; a `;` still
/// present after stripping the terminator means a multi-statement batch
/// and must stay on `execute_batch`.
pub(super) fn lock_taking_begin_verb(sql: &str) -> Option<&'static str> {
    let trimmed = sql.trim();
    let body = trimmed.strip_suffix(';').unwrap_or(trimmed);
    if body.contains(';') {
        return None;
    }
    let mut words = body.split_whitespace();
    if !words
        .next()
        .is_some_and(|w| w.eq_ignore_ascii_case("begin"))
    {
        return None;
    }
    let mode = words.next()?;
    let verb = if mode.eq_ignore_ascii_case("immediate") {
        "BEGIN IMMEDIATE"
    } else if mode.eq_ignore_ascii_case("exclusive") {
        "BEGIN EXCLUSIVE"
    } else {
        return None;
    };
    match words.next() {
        None => Some(verb),
        // Tolerate "BEGIN IMMEDIATE TRANSACTION" (SQLite grammar).
        Some(w)
            if w.eq_ignore_ascii_case("transaction") && words.next().is_none()
                || w.eq_ignore_ascii_case("tran") && words.next().is_none() =>
        {
            Some(verb)
        }
        Some(_) => None,
    }
}

/// Execute a statement to completion, discarding any rows it returns.
///
/// `PRAGMA journal_mode = WAL`, `BEGIN`, etc. all return rows under rusqlite's
/// strict `execute`, so the facade steps the statement manually instead.
/// Multi-statement SQL falls back to `execute_batch` (bound parameters apply
/// to the first statement only, which matches sqlite semantics anyway).
fn run_execute(
    conn: &rusqlite::Connection,
    sql: &str,
    params: &[SqliteValue],
) -> Result<usize, rusqlite::Error> {
    match conn.prepare(sql) {
        Ok(mut stmt) => {
            // Always step through `query` — even with zero params — because
            // `raw_execute` still fails with `ExecuteReturnedResults` on
            // row-returning statements (`PRAGMA journal_mode = WAL`,
            // `BEGIN`, `... RETURNING`). `dynamic_params(&[])` allocates
            // nothing and `query` drains `SQLITE_ROW` results cleanly.
            let refs = dynamic_params(params);
            let mut rows = stmt.query(&refs[..])?;
            while let Some(_row) = rows.next()? {}
            Ok(conn.changes() as usize)
        }
        Err(rusqlite::Error::MultipleStatement) => {
            conn.execute_batch(sql)?;
            Ok(conn.changes() as usize)
        }
        Err(error) => Err(error),
    }
}

/// Small deterministic jitter for busy-retry pacing: exponential base
/// (4 ms → 128 ms cap) plus a lightweight counter-derived jitter term so
/// contending waiters do not retry in lock-step. No RNG dependency.
fn retry_sleep(attempt: u32) -> Duration {
    static JITTER_SEED: AtomicU64 = AtomicU64::new(0x9E37_79B9_7F4A_7C15);
    let base_ms: u64 = (4u64 << attempt.min(5)).min(128);
    let jitter_ms = JITTER_SEED.fetch_add(0x9E37_79B9, AtomicOrdering::Relaxed) % 13;
    Duration::from_millis(base_ms + jitter_ms)
}

/// Read `PRAGMA busy_timeout` straight off the raw connection: an
/// uncancellable in-memory pragma read so the slice-bounded retry loops
/// capture `Some(original_ms)` for `BusyTimeoutRestore` even inside a
/// cancelled `CancellationScope` (`guarded` would return `Abort`).
fn read_busy_timeout_ms(conn: &rusqlite::Connection) -> Option<i64> {
    conn.query_row("PRAGMA busy_timeout", [], |r| r.get(0)).ok()
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

/// Synchronous database connection backed directly by `rusqlite::Connection`.
///
/// `Send` but not `Sync` (rusqlite caches prepared statements in a
/// `RefCell`). All blocking work runs on the calling thread; cancellation is
/// delivered by `sqlite3_interrupt` through `CancellationHandle` /
/// `DeadlineGuard`.
pub struct Connection {
    pub(super) inner: Option<rusqlite::Connection>,
    /// Filesystem path the connection was opened with (`:memory:` kept as-is
    /// for diagnostics).
    pub(super) path: Option<PathBuf>,
    pub(super) savepoint_seq: AtomicU64,
    /// Filesystem identity captured once at open so `file_identity()` never
    /// opens a temporary descriptor (preserving POSIX `fcntl` locks) and
    /// still detects `rename()` replacements.
    open_identity: Option<FileIdentity>,
    /// Key currently registered in the in-process live-connection table.
    live_registration: Option<FileIdentity>,
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FrankenSyncConnection")
            .field("path", &self.path)
            .field("closed", &self.inner.is_none())
            .finish()
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.close_best_effort_in_place();
    }
}

impl Connection {
    pub(super) fn from_parts(
        inner: rusqlite::Connection,
        path: Option<PathBuf>,
        open_identity: Option<FileIdentity>,
        known_wal: Option<bool>,
    ) -> Self {
        let live_registration = open_identity.map(|id| {
            let detected_wal = known_wal.or_else(|| {
                inner
                    .query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))
                    .ok()
                    .map(|mode| mode.eq_ignore_ascii_case("wal"))
            });
            register_live_connection(id, detected_wal);
            id
        });
        Self {
            inner: Some(inner),
            path,
            savepoint_seq: AtomicU64::new(0),
            open_identity,
            live_registration,
        }
    }

    /// Open a database at `path` (`:memory:` supported).
    pub fn open(path: impl AsRef<str>) -> Result<Self, FrankenError> {
        check_connection_admission()?;
        let path = path.as_ref().to_owned();
        let inner = rusqlite::Connection::open(&path)
            .map_err(|error| FrankenError::from_rusqlite(error, Some(Path::new(&path))))?;
        let open_identity = compute_open_identity(&path, None, None)?;
        Ok(Self::from_parts(
            inner,
            Some(PathBuf::from(path)),
            open_identity,
            None,
        ))
    }

    /// Open an existing database read/write without creating it.
    pub fn open_existing_schema_only(path: impl AsRef<str>) -> Result<Self, FrankenError> {
        Self::open_with_flags_inner(path.as_ref(), compat::OpenFlags::SQLITE_OPEN_READ_WRITE)
    }

    /// Alias kept for historical naming; identical to
    /// `open_existing_schema_only`.
    pub fn open_existing_schema_only_deferred_fts5(
        path: impl AsRef<str>,
    ) -> Result<Self, FrankenError> {
        Self::open_existing_schema_only(path)
    }

    /// Open an existing database read-only, permitting SQLite's built-in
    /// WAL-index recovery (heap walindex / shm rebuild as needed).
    pub fn open_schema_only_with_wal_index_recovery(
        path: impl AsRef<str>,
    ) -> Result<Self, FrankenError> {
        Self::open_with_flags_inner(path.as_ref(), compat::OpenFlags::SQLITE_OPEN_READ_ONLY)
    }

    /// Historical alias preserved for callers referencing the
    /// `open_existing_schema_only...` variants.
    pub fn open_existing(path: impl AsRef<str>) -> Result<Self, FrankenError> {
        Self::open_existing_schema_only(path)
    }

    /// Compatibility shim for the old `connection.as_async()` surface: the
    /// engine is synchronous now, so the "async view" only reports
    /// transaction state and exposes the raw connection.
    pub fn as_async(&self) -> AsyncView<'_> {
        AsyncView { inner: self.raw() }
    }

    /// Direct access to the underlying rusqlite connection (hot paths).
    ///
    /// # Panics
    /// Panics if the connection was already closed; use `try_raw` when the
    /// closed state is possible.
    pub fn raw(&self) -> &rusqlite::Connection {
        self.inner
            .as_ref()
            .expect("franken_sync::Connection used after close")
    }

    /// Mutable access to the underlying rusqlite connection.
    pub fn raw_mut(&mut self) -> &mut rusqlite::Connection {
        self.inner
            .as_mut()
            .expect("franken_sync::Connection used after close")
    }

    /// Fallible variant of `raw()` for close-sensitive paths.
    pub fn try_raw(&self) -> Result<&rusqlite::Connection, FrankenError> {
        self.inner
            .as_ref()
            .ok_or_else(|| FrankenError::Internal("connection closed".to_string()))
    }

    /// Zero-copy access to the underlying rusqlite connection for streaming
    /// call sites — no facade wrapping, no materialization.
    pub fn with_raw_rusqlite<R>(&self, f: impl FnOnce(&rusqlite::Connection) -> R) -> R {
        f(self.raw())
    }

    /// Mutable variant of `with_raw_rusqlite` (e.g. `conn.transaction()`).
    pub fn with_raw_rusqlite_mut<R>(
        &mut self,
        f: impl FnOnce(&mut rusqlite::Connection) -> R,
    ) -> R {
        f(self.raw_mut())
    }

    /// `true` while SQLite reports a live (non-autocommit) transaction.
    /// Returns `false` once the connection is closed.
    pub fn in_transaction(&self) -> bool {
        self.inner
            .as_ref()
            .is_some_and(|inner| !inner.is_autocommit())
    }

    /// `true` while the connection is in autocommit mode.
    pub fn is_autocommit(&self) -> bool {
        !self.in_transaction()
    }

    /// Interrupt handle for `sqlite3_interrupt` from other threads.
    pub fn interrupt_handle(&self) -> rusqlite::InterruptHandle {
        self.raw().get_interrupt_handle()
    }

    /// Arm a deadline on the shared timer; expiry interrupts this connection.
    pub fn deadline_guard(&self, deadline: Instant) -> DeadlineGuard {
        DeadlineGuard::arm(self.raw().get_interrupt_handle(), deadline)
    }

    /// Arm a deadline `within` from now.
    pub fn deadline_guard_after(&self, within: Duration) -> DeadlineGuard {
        self.deadline_guard(Instant::now() + within)
    }

    /// Filesystem identity of the opened database file (`Ok(None)` for
    /// `:memory:` or platforms without inode identity). Returns the cached
    /// open-time identity without opening a descriptor.
    pub fn file_identity(&self) -> Result<Option<FileIdentity>, FrankenError> {
        Ok(self.open_identity)
    }

    /// Path the connection was opened with.
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    // -- statement execution -------------------------------------------------

    /// Execute a statement to completion; rows it returns are discarded.
    /// Multi-statement SQL runs via `sqlite3_exec` semantics. Direct
    /// `BEGIN IMMEDIATE` / `BEGIN EXCLUSIVE` calls are routed through
    /// `begin_with_retry` so they inherit slice-bounded busy retry and
    /// cancellation checks exactly like `transaction_with_behavior`.
    pub fn execute(&self, sql: &str) -> Result<usize, FrankenError> {
        if let Some(verb) = lock_taking_begin_verb(sql) {
            self.begin_with_retry(verb, None)?;
            return Ok(0);
        }
        let changed = self.guarded(|conn| run_execute(conn, sql, &[]))?;
        self.maybe_refresh_wal_state(sql);
        Ok(changed)
    }

    /// Execute with bound parameters. A parameterless lock-taking
    /// `BEGIN IMMEDIATE` / `BEGIN EXCLUSIVE` routes through
    /// `begin_with_retry` exactly like [`Self::execute`].
    pub fn execute_with_params(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<usize, FrankenError> {
        let begin_verb = if params.is_empty() {
            lock_taking_begin_verb(sql)
        } else {
            None
        };
        if let Some(verb) = begin_verb {
            self.begin_with_retry(verb, None)?;
            return Ok(0);
        }
        let changed = self.guarded(|conn| run_execute(conn, sql, params))?;
        self.maybe_refresh_wal_state(sql);
        Ok(changed)
    }

    /// Execute a semicolon-separated script. A lone lock-taking
    /// `BEGIN IMMEDIATE` / `BEGIN EXCLUSIVE` (one optional trailing `;`)
    /// routes through `begin_with_retry`; multi-statement scripts keep
    /// `sqlite3_exec` semantics.
    pub fn execute_batch(&self, sql: &str) -> Result<(), FrankenError> {
        if let Some(verb) = lock_taking_begin_verb(sql) {
            return self.begin_with_retry(verb, None);
        }
        self.guarded(|conn| conn.execute_batch(sql))?;
        self.maybe_refresh_wal_state(sql);
        Ok(())
    }

    fn maybe_refresh_wal_state(&self, sql: &str) {
        let Some(id) = self.live_registration else {
            return;
        };
        if sql.len() >= 12
            && sql
                .as_bytes()
                .windows(12)
                .any(|w| w.eq_ignore_ascii_case(b"journal_mode"))
        {
            if let Some(raw) = self.inner.as_ref() {
                if let Ok(mode) =
                    raw.query_row("PRAGMA journal_mode", [], |r| r.get::<_, String>(0))
                {
                    update_live_wal_state(id, mode.eq_ignore_ascii_case("wal"));
                }
            }
        }
    }

    /// Materialize every row of `sql`.
    pub fn query(&self, sql: &str) -> Result<Vec<Row>, FrankenError> {
        self.query_with_params(sql, &[])
    }

    /// Materialize every row of `sql` with bound parameters.
    pub fn query_with_params(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<Vec<Row>, FrankenError> {
        let mut out = Vec::new();
        self.query_with_params_for_each(sql, params, |row| {
            out.push(row.clone());
            Ok(())
        })?;
        Ok(out)
    }

    /// Streaming per-row callback (no params): each materialized `Row` is
    /// handed to `f` as it is stepped; rows are not collected.
    pub fn query_for_each<F>(&self, sql: &str, f: F) -> Result<(), FrankenError>
    where
        F: FnMut(&Row) -> Result<(), FrankenError>,
    {
        self.query_with_params_for_each(sql, &[], f)
    }

    /// Streaming per-row callback with bound parameters.
    ///
    /// Unlike `query_with_params` this does not collect the result set; each
    /// row is materialized and passed to `f` one at a time.
    pub fn query_with_params_for_each<F>(
        &self,
        sql: &str,
        params: &[SqliteValue],
        mut f: F,
    ) -> Result<(), FrankenError>
    where
        F: FnMut(&Row) -> Result<(), FrankenError>,
    {
        self.guarded_result(|conn| {
            let refs = dynamic_params(params);
            let mut stmt = conn
                .prepare(sql)
                .map_err(|error| self.map_err_cancellable(error))?;
            let column_count = stmt.column_count();
            let mut rows = stmt
                .query(&refs[..])
                .map_err(|error| self.map_err_cancellable(error))?;
            // Post-prepare/pre-step re-check: an interrupt landing during
            // prepare (nVdbeActive == 0) is a C SQLite no-op.
            if current_is_cancelled() {
                return Err(FrankenError::Abort);
            }
            loop {
                match rows.next() {
                    Err(error) => return Err(self.map_err_cancellable(error)),
                    Ok(None) => return Ok(()),
                    Ok(Some(row)) => {
                        let materialized = materialize_row(row, column_count)?;
                        f(&materialized)?;
                    }
                }
            }
        })
    }

    /// Zero-copy streaming map: `f` borrows each `rusqlite::Row<'_>` directly
    /// (no `SqliteValue` materialization) and its results are collected.
    /// This is the hot-path API for callers that only need a few columns.
    pub fn query_map<T, F>(
        &self,
        sql: &str,
        params: &[SqliteValue],
        mut f: F,
    ) -> Result<Vec<T>, FrankenError>
    where
        F: FnMut(&rusqlite::Row<'_>) -> Result<T, FrankenError>,
    {
        self.guarded_result(|conn| {
            let refs = dynamic_params(params);
            let mut stmt = conn
                .prepare(sql)
                .map_err(|error| self.map_err_cancellable(error))?;
            let mut rows = stmt
                .query(&refs[..])
                .map_err(|error| self.map_err_cancellable(error))?;
            if current_is_cancelled() {
                return Err(FrankenError::Abort);
            }
            let mut out = Vec::new();
            loop {
                match rows.next() {
                    Err(error) => return Err(self.map_err_cancellable(error)),
                    Ok(None) => break,
                    Ok(Some(row)) => out.push(f(row)?),
                }
            }
            Ok(out)
        })
    }

    /// Execute `sql` expecting exactly one row.
    /// `QueryReturnedNoRows` on empty, `QueryReturnedMultipleRows` on >1.
    pub fn query_row(&self, sql: &str) -> Result<Row, FrankenError> {
        self.query_row_with_params(sql, &[])
    }

    /// `query_row` with bound parameters.
    pub fn query_row_with_params(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<Row, FrankenError> {
        self.guarded_result(|conn| {
            let refs = dynamic_params(params);
            let mut stmt = conn
                .prepare(sql)
                .map_err(|error| self.map_err_cancellable(error))?;
            let column_count = stmt.column_count();
            let mut rows = stmt
                .query(&refs[..])
                .map_err(|error| self.map_err_cancellable(error))?;
            let first = match rows.next() {
                Err(error) => return Err(self.map_err_cancellable(error)),
                Ok(None) => return Err(FrankenError::QueryReturnedNoRows),
                Ok(Some(row)) => materialize_row(row, column_count)?,
            };
            match rows.next() {
                Err(error) => Err(self.map_err_cancellable(error)),
                Ok(None) => Ok(first),
                Ok(Some(_)) => Err(FrankenError::QueryReturnedMultipleRows),
            }
        })
    }

    /// Prepare a statement for repeated use.
    pub fn prepare(&self, sql: &str) -> Result<PreparedStatement<'_>, FrankenError> {
        let stmt = self.guarded(|conn| conn.prepare(sql))?;
        Ok(PreparedStatement {
            stmt: RefCell::new(stmt),
            connection: self,
        })
    }

    /// Rowid of the most recent insert on this connection.
    /// Mirrors `sqlite3_last_insert_rowid`: infallible, `0` on a closed
    /// connection.
    pub fn last_insert_rowid(&self) -> i64 {
        if check_connection_admission().is_err() {
            return 0;
        }
        self.try_raw().map_or(0, |conn| conn.last_insert_rowid())
    }

    // -- engine-maintenance shims --------------------------------------------

    /// The rusqlite engine manages page freelists itself; orphaned-page
    /// repair is a no-op that reports zero pages repaired.
    pub fn repair_orphaned_pages(&self) -> Result<usize, FrankenError> {
        check_connection_admission()?;
        Ok(0)
    }

    /// Exact-content backup via `VACUUM INTO`, refused while a transaction is
    /// open (SQLite rejects `VACUUM` inside one; the old facade reported that
    /// as `Busy`).
    #[cfg(not(target_arch = "wasm32"))]
    pub fn backup_exact_to(&self, target: impl AsRef<Path>) -> Result<BackupReport, FrankenError> {
        check_connection_admission()?;
        let target = target.as_ref();
        if self.in_transaction() {
            return Err(FrankenError::Busy);
        }
        match std::fs::symlink_metadata(target) {
            Ok(_) => {
                return Err(FrankenError::CannotOpen {
                    path: target.to_path_buf(),
                });
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(FrankenError::Io(error)),
        }
        let target_str = target.to_string_lossy().into_owned();
        self.execute_with_params("VACUUM INTO ?1", &[SqliteValue::Text(target_str)])?;
        read_backup_report(target)
    }

    /// Cancellable, slice-bounded `PRAGMA wal_checkpoint(TRUNCATE)` loop.
    ///
    /// Returns `(busy, log_frames, checkpointed_frames)` once `busy == 0`, or
    /// the last busy-class error once `deadline` is exhausted. The configured
    /// `PRAGMA busy_timeout` is sliced per attempt (≤250 ms) so `cancel()`
    /// lands promptly; when `deadline` is `None` the busy timeout itself is
    /// the retry budget (`busy_timeout=0` → a single attempt).
    pub fn wal_checkpoint_truncate(
        &self,
        deadline: Option<Instant>,
    ) -> Result<(i64, i64, i64), FrankenError> {
        const SLICE: Duration = Duration::from_millis(250);
        // Admission/open checks precede any busy_timeout rewrite: a
        // cancelled scope fails fast and a closed connection returns
        // Internal — never a raw() panic — so the sliced timeout cannot
        // leak past BusyTimeoutRestore.
        check_connection_admission()?;
        let raw = self.try_raw()?;
        let configured_ms = read_busy_timeout_ms(raw);
        let budget_deadline = deadline.or_else(|| {
            configured_ms
                .filter(|ms| *ms > 0)
                .map(|ms| Instant::now() + Duration::from_millis(ms.max(0) as u64))
        });
        let restore = BusyTimeoutRestore::arm(raw, configured_ms);
        let configured_ms = configured_ms.unwrap_or(0);
        let mut attempt = 0u32;
        let mut last_busy_error: Option<FrankenError> = None;
        let result = loop {
            if let Some(limit) = budget_deadline {
                let remaining = limit.saturating_duration_since(Instant::now());
                if remaining.is_zero() && attempt > 0 {
                    break Err(last_busy_error.take().unwrap_or(FrankenError::Busy));
                }
                let _ = raw.busy_timeout(remaining.min(SLICE).max(Duration::from_millis(1)));
            }
            match self.query_row("PRAGMA wal_checkpoint(TRUNCATE)") {
                Ok(row) => {
                    let busy = row_i64(&row, 0);
                    let log = row_i64(&row, 1);
                    let checkpointed = row_i64(&row, 2);
                    if busy == 0 {
                        break Ok((busy, log, checkpointed));
                    }
                    // Checkpoint returned busy: retry within the budget.
                    if let Some(limit) = budget_deadline {
                        if Instant::now() >= limit {
                            break Err(FrankenError::Busy);
                        }
                    } else if attempt > 0 || configured_ms <= 0 {
                        break Err(FrankenError::Busy);
                    }
                }
                // Busy-class *errors* from query_row retry exactly like a
                // busy=1 row does.
                Err(error) if error.is_busy_class() => {
                    if let Some(limit) = budget_deadline {
                        if Instant::now() >= limit {
                            break Err(error);
                        }
                    } else if attempt > 0 || configured_ms <= 0 {
                        break Err(error);
                    }
                    // Keep the specific busy-class error so an exhausted
                    // deadline reports it instead of plain Busy.
                    last_busy_error = Some(error);
                }
                Err(error) => break Err(error),
            }
            if let Err(error) = check_connection_admission() {
                break Err(error);
            }
            let sleep = budget_deadline.map_or_else(
                || retry_sleep(attempt),
                |limit| retry_sleep(attempt).min(limit.saturating_duration_since(Instant::now())),
            );
            std::thread::sleep(sleep);
            if let Err(error) = check_connection_admission() {
                break Err(error);
            }
            attempt = attempt.saturating_add(1);
        };
        drop(restore);
        result
    }

    // -- transactions ---------------------------------------------------------

    /// `BEGIN` with cancellable, slice-bounded busy retry. The configured
    /// `PRAGMA busy_timeout` is the overall budget; each attempt bounds the
    /// engine-internal wait to a ≤250 ms slice so `cancel()` lands promptly.
    pub fn transaction_with_behavior(
        &self,
        behavior: compat::TransactionBehavior,
    ) -> Result<compat::Transaction<'_>, FrankenError> {
        self.transaction_with_deadline(behavior, None)
    }

    /// `transaction_with_behavior` with an explicit deadline: retries stop at
    /// `deadline` and surface the last busy-class error.
    pub fn transaction_with_deadline(
        &self,
        behavior: compat::TransactionBehavior,
        deadline: Option<Instant>,
    ) -> Result<compat::Transaction<'_>, FrankenError> {
        let verb = match behavior {
            compat::TransactionBehavior::Immediate => "BEGIN IMMEDIATE",
            compat::TransactionBehavior::Exclusive => "BEGIN EXCLUSIVE",
            // Deferred plus any future non-exhaustive variants fall back to
            // plain BEGIN.
            _ => "BEGIN",
        };
        self.begin_with_retry(verb, deadline)?;
        Ok(compat::Transaction::active(self))
    }

    fn begin_with_retry(
        &self,
        verb: &'static str,
        deadline: Option<Instant>,
    ) -> Result<(), FrankenError> {
        const SLICE: Duration = Duration::from_millis(250);
        // Same ordering as wal_checkpoint_truncate: admission/open checks
        // before any busy_timeout rewrite so a cancelled scope aborts
        // fast and a closed connection returns Internal, keeping
        // BusyTimeoutRestore able to restore the real configured value.
        check_connection_admission()?;
        let raw = self.try_raw()?;
        let configured_ms = read_busy_timeout_ms(raw);
        let budget_deadline = deadline.or_else(|| {
            configured_ms
                .filter(|ms| *ms > 0)
                .map(|ms| Instant::now() + Duration::from_millis(ms.max(0) as u64))
        });
        let restore = BusyTimeoutRestore::arm(raw, configured_ms);
        let configured_ms = configured_ms.unwrap_or(0);
        let mut attempt = 0u32;
        let mut last_busy_error: Option<FrankenError> = None;
        let result = loop {
            if let Some(limit) = budget_deadline {
                let remaining = limit.saturating_duration_since(Instant::now());
                if remaining.is_zero() && attempt > 0 {
                    break Err(last_busy_error.take().unwrap_or(FrankenError::Busy));
                }
                let _ = raw.busy_timeout(remaining.min(SLICE).max(Duration::from_millis(1)));
            }
            match self.guarded(|conn| conn.execute(verb, [])) {
                Ok(_) => break Ok(()),
                Err(error) if error.is_busy_class() => {
                    if let Some(limit) = budget_deadline {
                        if Instant::now() >= limit {
                            break Err(error);
                        }
                    } else {
                        // No configured budget: a single attempt is sqlite's
                        // busy_timeout=0 contract.
                        if attempt > 0 || configured_ms <= 0 {
                            break Err(error);
                        }
                    }
                    // Keep the specific busy-class error so an exhausted
                    // deadline reports it instead of plain Busy.
                    last_busy_error = Some(error);
                    check_connection_admission()?;
                    // Clamp the inter-attempt sleep to the remaining budget
                    // so the loop cannot oversleep past `deadline`.
                    let sleep = budget_deadline.map_or_else(
                        || retry_sleep(attempt),
                        |limit| {
                            retry_sleep(attempt)
                                .min(limit.saturating_duration_since(Instant::now()))
                        },
                    );
                    std::thread::sleep(sleep);
                    // A cancel may have landed while we slept.
                    if let Err(error) = check_connection_admission() {
                        break Err(error);
                    }
                    attempt = attempt.saturating_add(1);
                }
                Err(error) => break Err(error),
            }
        };
        drop(restore);
        result
    }

    /// Open a savepoint (`SAVEPOINT cass_sp_N`) usable inside or outside an
    /// outer transaction.
    pub fn savepoint(&self) -> Result<compat::Savepoint<'_>, FrankenError> {
        compat::Savepoint::open(self)
    }

    // -- close ----------------------------------------------------------------

    /// Close, rolling back any open transaction. `Ok(())` when already closed.
    pub fn close(mut self) -> Result<(), FrankenError> {
        self.close_in_place()
    }

    /// Close in place; `self` remains usable-for-drop but closed. Resets
    /// `SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE` first so a prior failed
    /// `close_without_checkpoint_in_place` cannot leak the flag into this
    /// checkpointing close.
    pub fn close_in_place(&mut self) -> Result<(), FrankenError> {
        let Some(conn) = self.inner.take() else {
            return Ok(());
        };
        let _ = conn.set_db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, false);
        match conn.close() {
            Ok(()) => {
                if let Some(id) = self.live_registration.take() {
                    unregister_live_connection(id);
                }
                Ok(())
            }
            Err((conn, error)) => {
                self.inner = Some(conn);
                Err(self.map_err_cancellable(error))
            }
        }
    }

    /// Close without checkpointing the WAL on close
    /// (`SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE`). Preserves the `-wal` payload for
    /// later read-only recovery.
    pub fn close_without_checkpoint(mut self) -> Result<(), FrankenError> {
        self.close_without_checkpoint_in_place()
    }

    /// In-place variant of `close_without_checkpoint`.
    pub fn close_without_checkpoint_in_place(&mut self) -> Result<(), FrankenError> {
        let Some(conn) = self.inner.take() else {
            return Ok(());
        };
        // rusqlite 0.40.2 uses the two-argument setter; the one-argument
        // `db_config()` is the getter only.
        let _ = conn.set_db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, true);
        match conn.close() {
            Ok(()) => {
                if let Some(id) = self.live_registration.take() {
                    unregister_live_connection(id);
                }
                Ok(())
            }
            Err((conn, error)) => {
                self.inner = Some(conn);
                Err(self.map_err_cancellable(error))
            }
        }
    }

    /// Close best-effort, never erroring: skip the close-time checkpoint and
    /// discard the handle.
    pub fn close_best_effort_in_place(&mut self) {
        if let Some(conn) = self.inner.take() {
            let _ = conn.set_db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, true);
            match conn.close() {
                Ok(()) => {}
                Err((conn, _)) => drop(conn),
            }
        }
        if let Some(id) = self.live_registration.take() {
            unregister_live_connection(id);
        }
    }

    // -- internal -------------------------------------------------------------

    pub(super) fn open_with_flags_inner(
        path: &str,
        flags: compat::OpenFlags,
    ) -> Result<Self, FrankenError> {
        check_connection_admission()?;
        if flags.contains(compat::OpenFlags::SQLITE_OPEN_STRICT_READ_ONLY) {
            return Self::open_strict_readonly(path, flags);
        }
        let rusqlite_flags = flags.to_rusqlite();
        let inner = rusqlite::Connection::open_with_flags(path, rusqlite_flags)
            .map_err(|error| FrankenError::from_rusqlite(error, Some(Path::new(path))))?;
        let open_identity = compute_open_identity(path, None, Some(flags))?;
        Ok(Self::from_parts(
            inner,
            Some(PathBuf::from(path)),
            open_identity,
            None,
        ))
    }

    /// Cancellation-checked operation runner: pre-checks the TLS scope,
    /// registers this connection's interrupt handle so `cancel()` lands in
    /// `sqlite3_interrupt`, then maps errors (Interrupted → Abort when the
    /// scope latched).
    fn guarded<'s, T>(
        &'s self,
        op: impl FnOnce(&'s rusqlite::Connection) -> Result<T, rusqlite::Error>,
    ) -> Result<T, FrankenError> {
        let cancellation = current_cancellation();
        if cancellation.as_ref().is_some_and(|c| c.is_cancelled()) {
            return Err(FrankenError::Abort);
        }
        let conn = self.try_raw()?;
        let _registration = cancellation
            .as_ref()
            .map(|c| c.register(conn.get_interrupt_handle()))
            .transpose()?;
        // Re-check AFTER registering: a cancel() landing between the
        // pre-check and the push would interrupt() while
        // nVdbeActive == 0 (a no-op) — bail before op() starts so the
        // pre-step interrupt is never silently lost.
        if cancellation.as_ref().is_some_and(|c| c.is_cancelled()) {
            return Err(FrankenError::Abort);
        }
        match op(conn) {
            Ok(value) => Ok(value),
            Err(error) => Err(self.map_err_cancellable(error)),
        }
    }

    /// Same wrapper for ops whose inner error channel is already
    /// `FrankenError` (e.g. per-row callbacks); rusqlite errors inside must
    /// still flow through `map_err_cancellable`.
    fn guarded_result<'s, T>(
        &'s self,
        op: impl FnOnce(&'s rusqlite::Connection) -> Result<T, FrankenError>,
    ) -> Result<T, FrankenError> {
        let cancellation = current_cancellation();
        if cancellation.as_ref().is_some_and(|c| c.is_cancelled()) {
            return Err(FrankenError::Abort);
        }
        let conn = self.try_raw()?;
        let _registration = cancellation
            .as_ref()
            .map(|c| c.register(conn.get_interrupt_handle()))
            .transpose()?;
        if cancellation.as_ref().is_some_and(|c| c.is_cancelled()) {
            return Err(FrankenError::Abort);
        }
        op(conn)
    }

    /// Map a rusqlite error; `Interrupted`/`Abort` become `Abort` when the
    /// TLS cancellation scope is latched.
    pub(super) fn map_err_cancellable(&self, error: rusqlite::Error) -> FrankenError {
        let mapped = FrankenError::from_rusqlite(error, self.path.as_deref());
        if mapped.is_interrupt_class() && current_is_cancelled() {
            return FrankenError::Abort;
        }
        mapped
    }

    /// Direct (uncancellable) batch execution — used for mandatory rollback
    /// paths that must run even inside a cancelled scope, and safe on a
    /// closed connection (returns `Internal`, never panics).
    pub(super) fn execute_direct(&self, sql: &str) -> Result<(), FrankenError> {
        let conn = self.try_raw()?;
        conn.execute_batch(sql)
            .map_err(|error| FrankenError::from_rusqlite(error, self.path.as_deref()))
    }
}

fn compute_open_identity(
    path_str: &str,
    fs_path: Option<&Path>,
    flags: Option<compat::OpenFlags>,
) -> Result<Option<FileIdentity>, FrankenError> {
    if flags.is_some_and(|f| f.contains(compat::OpenFlags::SQLITE_OPEN_MEMORY)) {
        return Ok(None);
    }
    if let Some(path) = fs_path {
        return FileIdentity::from_path(path).map_err(FrankenError::Io);
    }
    if path_str.is_empty() || path_str == ":memory:" {
        return Ok(None);
    }
    if path_str.starts_with("file:")
        && (path_str.starts_with("file::memory:") || path_str.contains("mode=memory"))
    {
        return Ok(None);
    }
    match FileIdentity::from_path(Path::new(path_str)) {
        Ok(identity) => Ok(identity),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(FrankenError::Io(error)),
    }
}

/// Narrow compatibility view returned by `Connection::as_async()`.
///
/// The engine is synchronous now; this only preserves the
/// `.as_async().in_transaction()` shape used by semantic replay and archive
/// code.
