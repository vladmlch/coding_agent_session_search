use super::open_and_locks::{
    DOCTOR_MUTATION_DB_OPEN_LOCK_TIMEOUT, acquire_doctor_mutation_db_open_guard,
    open_archive_writer_connection,
};
use super::storage_core::FrankenStorage;
use crate::franken_sync::{
    Row as FrankenRow, SqliteValue,
    compat::{ParamValue, param_slice_to_values},
};
use anyhow::{Result, anyhow};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tracing::info;

pub(super) type FrankenAsyncConnection = FrankenOwnerConnection;

// -------------------------------------------------------------------------
// Lazy FrankenSQLite Connection (bd-1ueu)
// -------------------------------------------------------------------------
// Defers opening the database until first use, cutting startup cost for
// commands that may not need the DB at all.  Thread-safe via parking_lot
// Mutex; logs the reason and duration of the open on first access.

/// Error from lazy database initialization.
#[derive(Debug, Error)]
pub enum LazyDbError {
    #[error("Database not found at {0}")]
    NotFound(PathBuf),
    #[error("Failed to open FrankenSQLite database at {path}: {source}")]
    FrankenOpenFailed {
        path: PathBuf,
        source: crate::franken_sync::FrankenError,
    },
}

// -------------------------------------------------------------------------
// LazyFrankenDb — lazy wrapper around a dedicated-owner connection
// -------------------------------------------------------------------------

/// Synchronous `Send + Sync` facade over [`crate::franken_sync::Connection`].
///
/// The underlying synchronous `rusqlite`-backed connection is `Send` but `!Sync`;
/// serializing access with `parking_lot::Mutex` preserves `Send + Sync` safely
/// without manual `unsafe` auto-trait overrides while keeping CASS's existing
/// synchronous and `*_sync` method surface.
#[derive(Debug)]
pub struct FrankenOwnerConnection(pub(super) parking_lot::Mutex<crate::franken_sync::Connection>);

impl FrankenOwnerConnection {
    pub(super) fn open(
        path: impl Into<String>,
    ) -> std::result::Result<Self, crate::franken_sync::FrankenError> {
        crate::franken_sync::Connection::open(path.into())
            .map(parking_lot::Mutex::new)
            .map(Self)
    }

    pub(crate) fn open_sync(
        path: impl Into<String>,
    ) -> std::result::Result<Self, crate::franken_sync::FrankenError> {
        Self::open(path)
    }

    pub(crate) fn open_schema_only_with_wal_index_recovery_sync(
        path: impl Into<String>,
    ) -> std::result::Result<Self, crate::franken_sync::FrankenError> {
        crate::franken_sync::Connection::open_schema_only_with_wal_index_recovery(path.into())
            .map(parking_lot::Mutex::new)
            .map(Self)
    }

    pub(crate) fn open_with_flags_sync(
        path: impl Into<String>,
        flags: crate::franken_sync::compat::OpenFlags,
    ) -> std::result::Result<Self, crate::franken_sync::FrankenError> {
        crate::franken_sync::compat::open_with_flags(path.into(), flags)
            .map(parking_lot::Mutex::new)
            .map(Self)
    }

    pub fn execute(
        &self,
        sql: &str,
    ) -> std::result::Result<usize, crate::franken_sync::FrankenError> {
        self.0.lock().execute(sql)
    }

    pub(crate) fn execute_sync(
        &self,
        sql: &str,
    ) -> std::result::Result<usize, crate::franken_sync::FrankenError> {
        self.execute(sql)
    }

    pub fn execute_with_params(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> std::result::Result<usize, crate::franken_sync::FrankenError> {
        self.0.lock().execute_with_params(sql, params)
    }

    pub(crate) fn execute_with_params_sync(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> std::result::Result<usize, crate::franken_sync::FrankenError> {
        self.execute_with_params(sql, params)
    }

    pub fn execute_batch(
        &self,
        sql: &str,
    ) -> std::result::Result<(), crate::franken_sync::FrankenError> {
        self.0.lock().execute_batch(sql)
    }

    pub(crate) fn execute_batch_sync(
        &self,
        sql: &str,
    ) -> std::result::Result<(), crate::franken_sync::FrankenError> {
        self.execute_batch(sql)
    }

    pub fn query(
        &self,
        sql: &str,
    ) -> std::result::Result<Vec<FrankenRow>, crate::franken_sync::FrankenError> {
        self.0.lock().query(sql)
    }

    pub(crate) fn query_sync(
        &self,
        sql: &str,
    ) -> std::result::Result<Vec<FrankenRow>, crate::franken_sync::FrankenError> {
        self.query(sql)
    }

    pub fn query_with_params(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> std::result::Result<Vec<FrankenRow>, crate::franken_sync::FrankenError> {
        self.0.lock().query_with_params(sql, params)
    }

    pub(crate) fn query_with_params_sync(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> std::result::Result<Vec<FrankenRow>, crate::franken_sync::FrankenError> {
        self.query_with_params(sql, params)
    }

    pub fn query_row_with_params(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> std::result::Result<FrankenRow, crate::franken_sync::FrankenError> {
        self.0.lock().query_row_with_params(sql, params)
    }

    pub(crate) fn query_row_with_params_sync(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> std::result::Result<FrankenRow, crate::franken_sync::FrankenError> {
        self.query_row_with_params(sql, params)
    }

    pub fn query_row_map<T, F>(
        &self,
        sql: &str,
        params: &[ParamValue],
        map: F,
    ) -> std::result::Result<T, crate::franken_sync::FrankenError>
    where
        F: FnOnce(&FrankenRow) -> std::result::Result<T, crate::franken_sync::FrankenError>,
    {
        let values = param_slice_to_values(params);
        let row = self.0.lock().query_row_with_params(sql, &values)?;
        map(&row)
    }

    pub fn query_map_collect<T, F>(
        &self,
        sql: &str,
        params: &[ParamValue],
        mut map: F,
    ) -> std::result::Result<Vec<T>, crate::franken_sync::FrankenError>
    where
        F: FnMut(&FrankenRow) -> std::result::Result<T, crate::franken_sync::FrankenError>,
    {
        let values = param_slice_to_values(params);
        let rows = self.0.lock().query_with_params(sql, &values)?;
        rows.iter().map(&mut map).collect()
    }

    pub fn execute_compat(
        &self,
        sql: &str,
        params: &[ParamValue],
    ) -> std::result::Result<usize, crate::franken_sync::FrankenError> {
        let values = param_slice_to_values(params);
        self.0.lock().execute_with_params(sql, &values)
    }

    pub(crate) fn begin_transaction_sync(
        &self,
    ) -> std::result::Result<(), crate::franken_sync::FrankenError> {
        self.execute("BEGIN;").map(|_| ())
    }

    pub(crate) fn rollback_transaction_sync(
        &self,
    ) -> std::result::Result<(), crate::franken_sync::FrankenError> {
        self.execute("ROLLBACK;").map(|_| ())
    }

    pub(crate) fn close_without_checkpoint_sync(
        &mut self,
    ) -> std::result::Result<(), crate::franken_sync::FrankenError> {
        self.0.get_mut().close_without_checkpoint_in_place()
    }

    pub(super) fn close_best_effort_in_place(&mut self) {
        if let Err(err) = self.close_without_checkpoint_sync() {
            tracing::debug!(
                error = %err,
                "failed to close dedicated-owner connection without checkpoint"
            );
        }
    }
}

/// Lazy-opening wrapper for a dedicated-owner FrankenSQLite connection.
///
/// Constructing a `LazyFrankenDb` is cheap (no I/O).  The underlying
/// raw connection is opened on its owner worker on the first call to [`get`].
/// Subsequent calls return the cached connection.
pub struct LazyFrankenDb {
    path: PathBuf,
    conn: parking_lot::Mutex<Option<FrankenOwnerConnection>>,
}

/// RAII guard that dereferences to the dedicated-owner dispatch facade.
pub struct LazyFrankenDbGuard<'a>(parking_lot::MutexGuard<'a, Option<FrankenOwnerConnection>>);

impl std::fmt::Debug for LazyFrankenDbGuard<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("LazyFrankenDbGuard")
            .field(&self.0.is_some())
            .finish()
    }
}

impl std::ops::Deref for LazyFrankenDbGuard<'_> {
    type Target = FrankenOwnerConnection;
    fn deref(&self) -> &FrankenOwnerConnection {
        self.0
            .as_ref()
            .expect("LazyFrankenDb connection must be initialized before access")
    }
}

impl LazyFrankenDb {
    /// Create a lazy handle pointing at `path`.  No I/O is performed.
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            conn: parking_lot::Mutex::new(None),
        }
    }

    /// Resolve path from optional CLI overrides.
    ///
    /// Uses `data_dir / agent_search.db` as fallback.
    pub fn from_overrides(data_dir: &Option<PathBuf>, db_override: Option<PathBuf>) -> Self {
        let data_dir = data_dir.clone().unwrap_or_else(crate::default_data_dir);
        let path = db_override.unwrap_or_else(|| data_dir.join("agent_search.db"));
        Self::new(path)
    }

    /// Get the connection, opening the database on first access.
    ///
    /// `reason` is logged alongside the open duration so callers can
    /// identify which command triggered the open.
    pub fn get(&self, reason: &str) -> std::result::Result<LazyFrankenDbGuard<'_>, LazyDbError> {
        let mut guard = self.conn.lock();
        if guard.is_none() {
            if !self.path.exists() {
                return Err(LazyDbError::NotFound(self.path.clone()));
            }
            let start = Instant::now();
            let _doctor_guard = acquire_doctor_mutation_db_open_guard(
                &self.path,
                DOCTOR_MUTATION_DB_OPEN_LOCK_TIMEOUT,
            )
            .map_err(|err| LazyDbError::FrankenOpenFailed {
                path: self.path.clone(),
                source: crate::franken_sync::FrankenError::Internal(err.to_string()),
            })?;
            let conn = FrankenOwnerConnection::open(self.path.to_string_lossy().into_owned())
                .map_err(|e| LazyDbError::FrankenOpenFailed {
                    path: self.path.clone(),
                    source: e,
                })?;
            let elapsed_ms = start.elapsed().as_millis();
            info!(
                path = %self.path.display(),
                elapsed_ms = elapsed_ms,
                reason = reason,
                "lazily opened FrankenSQLite database"
            );
            *guard = Some(conn);
        }
        Ok(LazyFrankenDbGuard(guard))
    }

    /// Get the connection with a timeout, opening the database on first access.
    ///
    /// Like [`get`] but spawns the open in a background thread and waits up to
    /// `timeout` for it to complete. Returns `LazyDbError::FrankenOpenFailed`
    /// with a descriptive message if the timeout elapses. Fix for #128.
    pub fn get_with_timeout(
        &self,
        reason: &str,
        timeout: Duration,
    ) -> std::result::Result<LazyFrankenDbGuard<'_>, LazyDbError> {
        let mut guard = self.conn.lock();
        if guard.is_none() {
            if !self.path.exists() {
                return Err(LazyDbError::NotFound(self.path.clone()));
            }
            let start = Instant::now();
            let path_owned = self.path.to_string_lossy().into_owned();
            let path_for_guard = self.path.clone();
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let _doctor_guard =
                    match acquire_doctor_mutation_db_open_guard(&path_for_guard, timeout) {
                        Ok(guard) => guard,
                        Err(err) => {
                            let _ = tx.send(Err(crate::franken_sync::FrankenError::Internal(
                                err.to_string(),
                            )));
                            return;
                        }
                    };
                if let Err(std::sync::mpsc::SendError(Ok(mut conn))) =
                    tx.send(FrankenOwnerConnection::open(path_owned))
                {
                    conn.close_best_effort_in_place();
                }
            });
            let conn = rx
                .recv_timeout(timeout)
                .map_err(|_| LazyDbError::FrankenOpenFailed {
                    path: self.path.clone(),
                    source: crate::franken_sync::FrankenError::Internal(format!(
                        "database open timed out after {}s (possible corruption or lock contention)",
                        timeout.as_secs()
                    )),
                })?
                .map_err(|e| LazyDbError::FrankenOpenFailed {
                    path: self.path.clone(),
                    source: e,
                })?;
            let elapsed_ms = start.elapsed().as_millis();
            info!(
                path = %self.path.display(),
                elapsed_ms = elapsed_ms,
                reason = reason,
                "lazily opened FrankenSQLite database (with timeout)"
            );
            *guard = Some(conn);
        }
        Ok(LazyFrankenDbGuard(guard))
    }

    /// Path to the database file (even if not yet opened).
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Whether the connection has been opened.
    pub fn is_open(&self) -> bool {
        self.conn.lock().is_some()
    }
}

impl Drop for LazyFrankenDb {
    fn drop(&mut self) {
        let Some(mut conn) = self.conn.get_mut().take() else {
            return;
        };
        conn.close_best_effort_in_place();
    }
}

// -------------------------------------------------------------------------
// FrankenSQLite Connection Manager (bead 3rlf8)
// -------------------------------------------------------------------------
// Multi-connection management: reader pool + concurrent writer connections.
// Replaces the LazyFrankenDb single-connection bottleneck for high-throughput
// scenarios (indexer parallel writes, concurrent TUI reads + indexer writes).

/// Configuration for the [`FrankenConnectionManager`].
#[derive(Debug, Clone)]
pub struct ConnectionManagerConfig {
    /// Number of pre-opened reader connections (default: 4).
    pub reader_count: usize,
    /// Maximum concurrent writer connections (default: available parallelism).
    pub max_writers: usize,
}

impl Default for ConnectionManagerConfig {
    fn default() -> Self {
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Self {
            reader_count: 4,
            max_writers: cpus,
        }
    }
}

/// Multi-connection manager for frankensqlite.
///
/// Provides:
/// - A pool of pre-opened reader connections (round-robin, Mutex-protected)
/// - Controlled creation of writer connections with token-based limits
/// - RAII guards that auto-rollback uncommitted transactions on drop
///
/// Thread-safe: reader handles dispatch to dedicated owner threads. Writer
/// connections remain thread-affine and are created and consumed per request.
pub struct FrankenConnectionManager {
    db_path: PathBuf,
    readers: Vec<parking_lot::Mutex<FrankenOwnerConnection>>,
    pub(super) reader_idx: std::sync::atomic::AtomicUsize,
    /// Token-based writer limit: channel pre-filled with `max_writers` tokens.
    /// `recv()` = acquire slot, `send()` = release slot.
    writer_tokens: (
        crossbeam_channel::Sender<()>,
        crossbeam_channel::Receiver<()>,
    ),
    config: ConnectionManagerConfig,
}

impl FrankenConnectionManager {
    /// Create a new connection manager.
    ///
    /// Opens `config.reader_count` reader connections immediately.
    /// Writer connections are created on demand (up to `config.max_writers`).
    pub fn new(db_path: impl Into<PathBuf>, config: ConnectionManagerConfig) -> Result<Self> {
        let db_path = db_path.into();
        let path_str = db_path.to_string_lossy().to_string();

        let reader_count = config.reader_count.max(1);
        let mut readers: Vec<parking_lot::Mutex<FrankenOwnerConnection>> =
            Vec::with_capacity(reader_count);
        for _ in 0..reader_count {
            let conn = match FrankenOwnerConnection::open(path_str.clone()) {
                Ok(conn) => conn,
                Err(source) => {
                    for reader in &mut readers {
                        reader.get_mut().close_best_effort_in_place();
                    }
                    return Err(anyhow::Error::from(source).context(format!(
                        "opening reader connection at {}",
                        db_path.display()
                    )));
                }
            };
            // Apply read-tuned config (no migration, no write PRAGMAs)
            let _ = conn.execute("PRAGMA busy_timeout = 5000;"); // match writer config
            let _ = conn.execute("PRAGMA cache_size = -16384;"); // 16MB reader cache
            readers.push(parking_lot::Mutex::new(conn));
        }

        let max_writers = config.max_writers.max(1);

        // Pre-fill bounded channel with tokens (acts as counting semaphore).
        // A zero-capacity channel with no initial tokens would make the first
        // writer acquisition block forever.
        let (tx, rx) = crossbeam_channel::bounded(max_writers);
        for _ in 0..max_writers {
            tx.send(())
                .map_err(|_| anyhow!("writer token channel closed during initialization"))?;
        }

        Ok(Self {
            db_path,
            readers,
            reader_idx: std::sync::atomic::AtomicUsize::new(0),
            writer_tokens: (tx, rx),
            config: ConnectionManagerConfig {
                reader_count,
                max_writers,
            },
        })
    }

    /// Get a reader connection (round-robin from the pool).
    ///
    /// Returns a mutex guard wrapping the connection. The guard prevents
    /// concurrent command streams through the same owner handle.
    pub fn reader(&self) -> parking_lot::MutexGuard<'_, FrankenOwnerConnection> {
        let idx = self
            .reader_idx
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.readers[idx % self.readers.len()].lock()
    }

    /// Acquire a writer connection.
    ///
    /// Opens a new frankensqlite connection with full config (no migration).
    /// Blocks if `max_writers` connections are already in use.
    /// The returned [`WriterGuard`] auto-rolls back on drop.
    pub fn writer(&self) -> Result<WriterGuard<'_>> {
        self.writer_tokens
            .1
            .recv()
            .map_err(|_| anyhow!("writer token channel closed"))?;
        let conn = match open_archive_writer_connection(&self.db_path) {
            Ok(c) => c,
            Err(e) => {
                let _ = self.writer_tokens.0.send(());
                return Err(e.context(format!(
                    "opening writer connection at {}",
                    self.db_path.display()
                )));
            }
        };
        let storage = FrankenStorage::new(conn, self.db_path.clone());
        if let Err(e) = storage.apply_config() {
            let _ = self.writer_tokens.0.send(());
            return Err(e);
        }
        Ok(WriterGuard {
            storage,
            mgr: self,
            committed: false,
        })
    }

    /// Acquire a concurrent writer connection (BEGIN CONCURRENT via MVCC).
    ///
    /// Similar to [`writer`] but tuned for the parallel indexer write pool.
    /// Uses reduced cache size and is designed for short-lived batch inserts.
    pub fn concurrent_writer(&self) -> Result<WriterGuard<'_>> {
        self.writer_tokens
            .1
            .recv()
            .map_err(|_| anyhow!("writer token channel closed"))?;
        let conn = match open_archive_writer_connection(&self.db_path) {
            Ok(c) => c,
            Err(e) => {
                let _ = self.writer_tokens.0.send(());
                return Err(e.context(format!(
                    "opening concurrent writer at {}",
                    self.db_path.display()
                )));
            }
        };
        let storage = FrankenStorage::new(conn, self.db_path.clone());
        if let Err(e) = storage.apply_config() {
            let _ = self.writer_tokens.0.send(());
            return Err(e);
        }
        // Reduced cache for concurrent writers (they're short-lived)
        let _ = storage.raw().execute("PRAGMA cache_size = -4096;");
        Ok(WriterGuard {
            storage,
            mgr: self,
            committed: false,
        })
    }

    /// Database path managed by this pool.
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    /// Number of reader connections in the pool.
    pub fn reader_count(&self) -> usize {
        self.readers.len()
    }

    /// Maximum concurrent writers allowed.
    pub fn max_writers(&self) -> usize {
        self.config.max_writers
    }
}

impl Drop for FrankenConnectionManager {
    fn drop(&mut self) {
        for reader in &mut self.readers {
            reader.get_mut().close_best_effort_in_place();
        }
    }
}

/// RAII guard for a writer connection.
///
/// Provides access to a [`FrankenStorage`] for write operations.
/// Releases the writer semaphore slot when dropped.
pub struct WriterGuard<'a> {
    storage: FrankenStorage,
    mgr: &'a FrankenConnectionManager,
    committed: bool,
}

impl<'a> WriterGuard<'a> {
    /// Access the underlying storage for read/write operations.
    pub fn storage(&self) -> &FrankenStorage {
        &self.storage
    }

    /// Mark this writer as successfully committed.
    ///
    /// Call after your transaction's `commit()` succeeds. Prevents the drop
    /// guard from attempting a rollback.
    pub fn mark_committed(&mut self) {
        self.committed = true;
    }
}

impl Drop for WriterGuard<'_> {
    fn drop(&mut self) {
        if !self.committed {
            // Best-effort rollback — connection may already be in autocommit
            let _ = self.storage.raw().execute("ROLLBACK;");
        }
        self.storage.close_best_effort_in_place();
        // Release writer token
        let _ = self.mgr.writer_tokens.0.send(());
    }
}
