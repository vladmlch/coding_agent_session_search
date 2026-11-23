use super::backup_and_integrity::{
    MAX_BACKUPS, MigrationError, cleanup_old_backups, create_backup,
    dedupe_conflicting_fts_schema_rows_via_sqlite3, remove_database_files,
};
use super::fts_rebuild::agent_kind_str;
use super::local_lookup_indexes;
use super::open_and_locks::{
    DOCTOR_MUTATION_DB_OPEN_LOCK_TIMEOUT, acquire_doctor_mutation_db_open_guard,
    acquire_existing_doctor_mutation_db_open_guard, attempt_dirty_wal_recovery_checkpoint,
    open_archive_writer_connection, retry_transient_storage_op, retryable_franken_anyhow,
    sleep_with_franken_retry_backoff,
};
use super::schema_migrations::{
    SchemaCheck, check_schema_compatibility, schema_check_error_requires_rebuild,
};
use super::schema_repair::{
    BASE_MIGRATION_STEPS, LEGACY_FTS_TEARDOWN_BUDGET_ENV, LegacyFtsTeardownRefusal,
    ORPHAN_DIRECT_CHILD_TABLES, OrphanFkCleanupReport, POST_TAIL_CACHE_MIGRATION_STEPS,
    REQUIRED_CONVERSATION_TOKEN_COLUMNS, REQUIRED_CURRENT_SCHEMA_TABLE_PROBES,
    apply_conversation_tail_state_cache_migration, archive_family_bytes,
    collect_direct_orphan_id_page, collect_orphan_message_ids,
    current_schema_repair_batches_for_missing_tables, delete_direct_orphan_ids_bisecting_oom,
    delete_orphan_message_ids_bisecting_oom, error_indicates_missing_column,
    error_indicates_missing_table, franken_table_column_names, legacy_fts_teardown_budget_bytes,
    run_attributed_migration_steps, schema_migration_is_applied, transition_from_meta_version,
};
use crate::franken_sync::{
    Connection as FrankenConnection, FrankenError,
    compat::{
        ConnectionExt as FrankenConnectionExt, OpenFlags as FrankenOpenFlags, ParamValue,
        RowExt as FrankenRowExt, Transaction as FrankenTransaction,
        open_with_flags as open_franken_with_flags,
    },
};
use crate::model::types::Agent;
use crate::sources::provenance::{Source, SourceKind};
use anyhow::{Context, Result, anyhow, bail};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicI8, AtomicI64, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};
use tracing::info;

/// Compatibility alias retained while call sites finish converging on `FrankenStorage`.
pub type SqliteStorage = FrankenStorage;

/// Primary frankensqlite-backed storage backend.
/// Busy-class engine errors that a connection close can hit transiently while
/// a sibling connection briefly holds the WAL write or checkpoint lock.
fn franken_close_error_is_transiently_busy(err: &crate::franken_sync::FrankenError) -> bool {
    use crate::franken_sync::FrankenError;
    matches!(
        err,
        FrankenError::Busy
            | FrankenError::BusyRecovery
            | FrankenError::BusySnapshot { .. }
            | FrankenError::WriteConflict { .. }
            | FrankenError::SerializationFailure { .. }
    )
}

/// Close a frankensqlite connection with a bounded retry on Busy-class errors.
///
/// The engine's close path commits any cached/retained write transaction and
/// (for `checkpoint_on_close`) runs a passive WAL checkpoint. Neither step
/// waits out the connection's busy timeout, so a sibling writer or reader that
/// momentarily holds the WAL write or checkpoint lock surfaces as a hard
/// `FrankenError::Busy` from an otherwise healthy close. That window is
/// timing-sensitive and much wider on macOS, where commits pay F_FULLFSYNC
/// (observed as darwin-only "closing frankensqlite connection: database is
/// busy" failures in the begin-concurrent persist paths; bead
/// coding_agent_session_search-t8gor, darwin section).
///
/// `Connection::close_in_place` explicitly retains the handle on error so
/// callers can retry; every attempt after a failed one re-runs the same
/// commit/rollback/teardown steps, and a successful close is terminal, so a
/// bounded backoff loop is safe. Non-busy errors are returned immediately.
pub(crate) fn close_franken_in_place_with_busy_retry(
    conn: &mut FrankenConnection,
    checkpoint_on_close: bool,
) -> std::result::Result<(), crate::franken_sync::FrankenError> {
    const MAX_ATTEMPTS: usize = 12;
    const BACKOFF_START: Duration = Duration::from_millis(5);
    const BACKOFF_CAP: Duration = Duration::from_millis(250);

    let mut backoff = BACKOFF_START;
    for attempt in 1..=MAX_ATTEMPTS {
        let result = if checkpoint_on_close {
            conn.close_in_place()
        } else {
            conn.close_without_checkpoint_in_place()
        };
        match result {
            Ok(()) => return Ok(()),
            Err(err) if attempt < MAX_ATTEMPTS && franken_close_error_is_transiently_busy(&err) => {
                tracing::debug!(
                    error = %err,
                    attempt,
                    "frankensqlite close hit a transient busy state; retrying"
                );
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(BACKOFF_CAP);
            }
            Err(err) => return Err(err),
        }
    }
    unreachable!("close retry loop returns on the final attempt")
}

/// Thread-affine storage owning the primary raw connection and any cached raw
/// ephemeral writer. Moving it to another OS thread must remain a compile-time
/// error; use [`FrankenOwnerConnection`] for cross-thread reads instead.
///
/// ```compile_fail
/// use coding_agent_search::storage::sqlite::FrankenStorage;
/// fn requires_send<T: Send>(_: T) {}
/// fn raw_storage_must_not_cross_threads(storage: FrankenStorage) {
///     requires_send(storage);
/// }
/// ```
pub struct FrankenStorage {
    pub(super) conn: FrankenConnection,
    pub(super) db_path: PathBuf,
    ephemeral_writer_preflight_verified: AtomicBool,
    index_writer_checkpoint_pages: AtomicI64,
    index_writer_busy_timeout_ms: AtomicU64,
    cached_ephemeral_writer: parking_lot::Mutex<CachedEphemeralWriter>,
    /// #425: once an in-run WAL reset has closed the cached writer, every
    /// later batch must stay on the primary connection. Reopening a second
    /// writer both disables fsqlite's single-connection TRUNCATE fast path and
    /// can derive a stale MVCC clock immediately after the reset.
    bulk_single_connection: AtomicBool,
    /// WAL size at which a failed/non-shrinking in-run checkpoint may be
    /// retried. This prevents an externally pinned WAL from being re-read and
    /// backfilled at every batch boundary.
    bulk_checkpoint_retry_at_bytes: AtomicU64,
    ensured_agents: Arc<parking_lot::Mutex<HashMap<EnsuredAgentKey, i64>>>,
    ensured_workspaces: Arc<parking_lot::Mutex<HashMap<EnsuredWorkspaceKey, i64>>>,
    ensured_conversation_sources: Arc<parking_lot::Mutex<HashSet<EnsuredConversationSourceKey>>>,
    ensured_daily_stats_keys: Arc<parking_lot::Mutex<HashSet<EnsuredDailyStatsKey>>>,
    fts_messages_present_cache: AtomicI8,
    /// #439: liveness callback invoked once per streamed page during the
    /// fallback-FTS shadow maintenance (`stream_fts_rows_via_frankensqlite`).
    /// The post-publish `index --full` tail runs that maintenance with every
    /// progress counter parked at `current == total`, so without a tick the
    /// stall watchdog reads a healthy multi-minute shadow rebuild as a wedge
    /// and exits 70 after the base abort threshold.
    pub(super) fts_maintenance_heartbeat: parking_lot::Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
    /// GH #413 follow-up (iify0): the run's view of the derived fts5 shadow,
    /// shared with every pool writer this connection spawns (see
    /// [`FtsShadowRunState`]).
    pub(super) fts_shadow_run: Arc<FtsShadowRunState>,
}

/// GH #413 follow-up (iify0): per-process state of the derived `fts_messages`
/// shadow. The index run inserts through pool writers built by
/// `new_with_shared_caches`, so a suspension decided inside a writer's flush
/// must be visible to the primary connection's finalize step — this lives in
/// an `Arc` next to the ensured-row caches for the same reason they do.
#[derive(Default)]
pub(super) struct FtsShadowRunState {
    /// Wall-clock spent inside inline `fts_messages` flushes, against
    /// `fts_inline_flush_budget`.
    pub(super) inline_spent_ms: AtomicU64,
    /// The reason inline shadow writes were suspended, once they were.
    pub(super) inline_suspended: parking_lot::Mutex<Option<String>>,
    /// Messages the shadow already covers (noted at preflight) plus the
    /// messages flushed into it since, against `fts_shadow_max_messages`.
    pub(super) messages_seen: AtomicU64,
    /// Whether crossing the bound mid-run left a drop for finalize.
    pub(super) drop_pending: AtomicBool,
}

/// Keep ordinary storage commits from tripping over frequent auto-checkpoints
/// while still bounding WAL growth. Bulk index paths may override this through
/// their explicit checkpoint policy.
const DEFAULT_WAL_AUTOCHECKPOINT_PAGES: i64 = 4096;
const UNSET_INDEX_WRITER_CHECKPOINT_PAGES: i64 = i64::MIN;
const UNSET_INDEX_WRITER_BUSY_TIMEOUT_MS: u64 = 0;
const FTS_MESSAGES_PRESENT_UNKNOWN: i8 = 0;
const FTS_MESSAGES_PRESENT_ABSENT: i8 = 1;
const FTS_MESSAGES_PRESENT_PRESENT: i8 = 2;

enum CachedEphemeralWriter {
    Uninitialized,
    Cached(Box<CachedRawFrankenConnection>),
    InUse,
}

/// Thread-affine writer state cached only inside the owning `FrankenStorage`.
///
/// Unlike the public reader facade, this intentionally contains the raw
/// `!Send` connection and has no manual auto-trait implementation. That makes
/// moving a storage (and therefore a cached writer) across OS threads a
/// compile-time error while preserving the inexpensive same-thread reuse path.
struct CachedRawFrankenConnection {
    conn: FrankenConnection,
    index_writer_checkpoint_pages: i64,
    index_writer_busy_timeout_ms: u64,
}

impl CachedRawFrankenConnection {
    fn new(
        conn: FrankenConnection,
        index_writer_checkpoint_pages: i64,
        index_writer_busy_timeout_ms: u64,
    ) -> Self {
        Self {
            conn,
            index_writer_checkpoint_pages,
            index_writer_busy_timeout_ms,
        }
    }

    fn into_parts(self) -> (FrankenConnection, i64, u64) {
        (
            self.conn,
            self.index_writer_checkpoint_pages,
            self.index_writer_busy_timeout_ms,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct EnsuredAgentKey {
    slug: String,
    name: String,
    version: Option<String>,
    kind: String,
}

impl EnsuredAgentKey {
    pub(super) fn from_agent(agent: &Agent) -> Self {
        Self {
            slug: agent.slug.clone(),
            name: agent.name.clone(),
            version: agent.version.clone(),
            kind: agent_kind_str(agent.kind.clone()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct EnsuredWorkspaceKey {
    path: String,
    display_name: Option<String>,
}

impl EnsuredWorkspaceKey {
    pub(super) fn new(path: String, display_name: Option<&str>) -> Self {
        Self {
            path,
            display_name: display_name.map(str::to_owned),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct EnsuredConversationSourceKey {
    id: String,
    kind: SourceKind,
    host_label: Option<String>,
}

impl EnsuredConversationSourceKey {
    pub(super) fn from_source(source: &Source) -> Self {
        Self {
            id: source.id.clone(),
            kind: source.kind,
            host_label: source.host_label.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct EnsuredDailyStatsKey {
    day_id: i64,
    agent_slug: String,
    source_id: String,
}

impl EnsuredDailyStatsKey {
    pub(super) fn new(day_id: i64, agent_slug: &str, source_id: &str) -> Self {
        Self {
            day_id,
            agent_slug: agent_slug.to_owned(),
            source_id: source_id.to_owned(),
        }
    }
}

pub(super) const AUTOCOMMIT_RETAIN_OFF_PRAGMAS: [&str; 2] = [
    "PRAGMA fsqlite.autocommit_retain = OFF;",
    "PRAGMA autocommit_retain = OFF;",
];

pub(super) fn disable_autocommit_retain<E>(
    mut execute: impl FnMut(&'static str) -> std::result::Result<(), E>,
) -> Result<&'static str>
where
    E: std::fmt::Display,
{
    let mut failures = Vec::new();
    for pragma in AUTOCOMMIT_RETAIN_OFF_PRAGMAS {
        match execute(pragma) {
            Ok(()) => return Ok(pragma),
            Err(err) => {
                let error = err.to_string();
                tracing::debug!(
                    %pragma,
                    error = %error,
                    "autocommit_retain PRAGMA variant not supported"
                );
                failures.push(format!("{pragma}: {error}"));
            }
        }
    }

    Err(anyhow!(
        "failed to disable autocommit_retain on frankensqlite connection; \
         refusing to keep a long-lived MVCC connection that may accumulate \
         unbounded write snapshots. Upgrade frankensqlite to a version that \
         supports one of these PRAGMAs or use a short-lived connection path. \
         attempts: {}",
        failures.join("; ")
    ))
}

pub(crate) fn error_message_indicates_populated_fts_shadow_without_rowid_reload(
    message: &str,
) -> bool {
    let lower = message.to_ascii_lowercase();
    let mentions_populated_without_rowid_shadow = (lower
        .contains("loading populated without rowid table")
        || lower.contains("reloading populated without rowid table"))
        && (lower.contains("table `fts_messages_") || lower.contains("table fts_messages_"));

    mentions_populated_without_rowid_shadow && lower.contains("not yet supported")
}

/// Test-only rendezvous separating uncancellable engine bootstrap from SQL
/// issued on an admitted connection. Baseline and candidate use the same hook.
#[cfg(debug_assertions)]
fn index_storage_open_rendezvous_for_test(path: &Path) -> Result<()> {
    let (Some(ready), Some(release)) = (
        dotenvy::var("CASS_TEST_INDEX_STORAGE_OPEN_READY").ok(),
        dotenvy::var("CASS_TEST_INDEX_STORAGE_OPEN_RELEASE").ok(),
    ) else {
        return Ok(());
    };
    let ready = PathBuf::from(ready);
    let release = PathBuf::from(release);
    let receipt = serde_json::to_vec(&serde_json::json!({
        "pid": std::process::id(),
        "db_path": path,
    }))?;
    let temporary = ready.with_extension(format!("{}.tmp", std::process::id()));
    fs::write(&temporary, receipt).context("writing storage-open test rendezvous")?;
    fs::rename(&temporary, &ready).context("publishing storage-open test rendezvous")?;
    let deadline = Instant::now() + Duration::from_secs(30);
    while !release.exists() {
        if Instant::now() >= deadline {
            bail!("storage-open test rendezvous was not released within 30 seconds");
        }
        std::thread::park_timeout(Duration::from_millis(10));
    }
    Ok(())
}

impl FrankenStorage {
    pub(super) fn new(conn: FrankenConnection, db_path: PathBuf) -> Self {
        Self::new_with_shared_caches(
            conn,
            db_path,
            Arc::new(parking_lot::Mutex::new(HashMap::new())),
            Arc::new(parking_lot::Mutex::new(HashMap::new())),
            Arc::new(parking_lot::Mutex::new(HashSet::new())),
            Arc::new(parking_lot::Mutex::new(HashSet::new())),
            Arc::new(FtsShadowRunState::default()),
        )
    }

    pub(super) fn new_with_shared_caches(
        conn: FrankenConnection,
        db_path: PathBuf,
        ensured_agents: Arc<parking_lot::Mutex<HashMap<EnsuredAgentKey, i64>>>,
        ensured_workspaces: Arc<parking_lot::Mutex<HashMap<EnsuredWorkspaceKey, i64>>>,
        ensured_conversation_sources: Arc<
            parking_lot::Mutex<HashSet<EnsuredConversationSourceKey>>,
        >,
        ensured_daily_stats_keys: Arc<parking_lot::Mutex<HashSet<EnsuredDailyStatsKey>>>,
        fts_shadow_run: Arc<FtsShadowRunState>,
    ) -> Self {
        Self {
            conn,
            db_path,
            ephemeral_writer_preflight_verified: AtomicBool::new(false),
            index_writer_checkpoint_pages: AtomicI64::new(UNSET_INDEX_WRITER_CHECKPOINT_PAGES),
            index_writer_busy_timeout_ms: AtomicU64::new(UNSET_INDEX_WRITER_BUSY_TIMEOUT_MS),
            cached_ephemeral_writer: parking_lot::Mutex::new(CachedEphemeralWriter::Uninitialized),
            bulk_single_connection: AtomicBool::new(false),
            bulk_checkpoint_retry_at_bytes: AtomicU64::new(0),
            ensured_agents,
            ensured_workspaces,
            ensured_conversation_sources,
            ensured_daily_stats_keys,
            fts_messages_present_cache: AtomicI8::new(FTS_MESSAGES_PRESENT_UNKNOWN),
            fts_maintenance_heartbeat: parking_lot::Mutex::new(None),
            fts_shadow_run,
        }
    }

    pub(super) fn apply_open_stage_busy_timeout(&self) {
        if let Err(err) = self.conn.execute("PRAGMA busy_timeout = 5000;") {
            tracing::debug!(
                error = %err,
                "failed to apply open-stage busy_timeout before migrations"
            );
        }
    }

    /// Open a frankensqlite connection, run migrations, and apply config.
    ///
    /// This initializes canonical schema state only. Derived fallback search
    /// structures like the in-database `fts_messages` table are repaired
    /// separately so ordinary opens never block on heavyweight maintenance.
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("creating db directory {}", parent.display()))?;
        }

        let path_str = path.to_string_lossy().to_string();
        let _doctor_guard =
            acquire_doctor_mutation_db_open_guard(path, DOCTOR_MUTATION_DB_OPEN_LOCK_TIMEOUT)?;
        let existing_archive = match fs::metadata(path) {
            Ok(metadata) => metadata.len() > 0,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => false,
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("inspecting frankensqlite db path at {}", path.display())
                });
            }
        };
        let conn = if existing_archive {
            // `open_existing_schema_only` never creates or zero-initializes
            // the archive path. FrankenSQLite 0.1.19 loads only the schema
            // image for this writable existing-only lane (rows stay
            // pager-backed; the compatibility `MemDatabase` is not hydrated
            // eagerly), so ordinary opens stay bounded on multi-gigabyte
            // archives.
            match FrankenConnection::open_existing_schema_only(&path_str) {
                Ok(conn) => conn,
                // fsqlite 0.3.x validates sqlite_master at schema load and
                // refuses the historical duplicate `fts_messages` row debris
                // that 0.2.x tolerated (and that cass used to dedupe lazily in
                // `ensure_search_fallback_fts_consistency`). Repair the
                // duplicate rows through the rusqlite salvage bridge, then
                // retry the open once.
                Err(err)
                    if err
                        .to_string()
                        .contains("conflicting virtual-table entries") =>
                {
                    tracing::warn!(
                        db = %path.display(),
                        error = %err,
                        "frankensqlite refused open due to duplicate fts_messages \
                         schema rows; deduplicating via sqlite3 bridge and retrying"
                    );
                    dedupe_conflicting_fts_schema_rows_via_sqlite3(path)?;
                    FrankenConnection::open_existing_schema_only(&path_str).with_context(|| {
                        format!(
                            "opening existing frankensqlite db at {} after \
                                 duplicate fts schema-row repair",
                            path.display()
                        )
                    })?
                }
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!("opening existing frankensqlite db at {}", path.display())
                    });
                }
            }
        } else {
            FrankenConnection::open(&path_str)
                .with_context(|| format!("creating frankensqlite db at {}", path.display()))?
        };
        #[cfg(debug_assertions)]
        index_storage_open_rendezvous_for_test(path)?;
        let storage = Self::new(conn, path.to_path_buf());
        storage.apply_open_stage_busy_timeout();
        retry_transient_storage_op("run_migrations", || storage.run_migrations())?;
        retry_transient_storage_op("repair_missing_current_schema_objects", || {
            storage.repair_missing_current_schema_objects()
        })?;
        storage.apply_config()?;
        // Migrations intentionally leave the derived FTS shadow absent.
        // Keep UNKNOWN until a write transaction observes the real catalog.
        Ok(storage)
    }

    /// Open an existing archive for a narrowly scoped derived-FTS repair.
    ///
    /// Unlike [`Self::open`], this uses FrankenSQLite's existing-only
    /// read-write lane: the path must already exist (never created or
    /// zero-initialized), and 0.1.19 keeps canonical rows pager-backed
    /// instead of hydrating them into the compatibility `MemDatabase`. It
    /// intentionally does not run migrations or repair canonical schema
    /// objects. The caller may inspect ordinary B-trees and mutate only the
    /// rebuildable FTS shadow under an explicit doctor repair contract.
    pub(crate) fn open_existing_schema_only_for_fts_repair(path: &Path) -> Result<Self> {
        let path_str = path.to_string_lossy().to_string();
        let _doctor_guard =
            acquire_doctor_mutation_db_open_guard(path, DOCTOR_MUTATION_DB_OPEN_LOCK_TIMEOUT)?;
        let conn = FrankenConnection::open_existing_schema_only(&path_str).with_context(|| {
            format!(
                "opening existing frankensqlite db for FTS repair at {}",
                path.display()
            )
        })?;
        let storage = Self::new(conn, path.to_path_buf());
        storage.apply_open_stage_busy_timeout();
        storage.apply_config()?;
        Ok(storage)
    }

    /// #368 defect 3: open an archive for FTS repair with FTS5 shadow
    /// validation/hydration DEFERRED, so a database whose FTS5 `%_data` is
    /// structurally corrupt (which fails an ordinary open during the schema
    /// reload) can still be opened to drop+recreate the corrupt shadow. FTS5
    /// queries on this connection see an empty index until the shadow is
    /// rebuilt — it is for repair only (see `rebuild_fts_shadow_via_drop_recreate`).
    pub(crate) fn open_deferred_fts5_for_repair(path: &Path) -> Result<Self> {
        let path_str = path.to_string_lossy().to_string();
        let _doctor_guard =
            acquire_doctor_mutation_db_open_guard(path, DOCTOR_MUTATION_DB_OPEN_LOCK_TIMEOUT)?;
        let conn =
            FrankenConnection::open_existing_schema_only_deferred_fts5(&path_str).with_context(
                || {
                    format!(
                        "opening frankensqlite db with deferred FTS5 validation for corrupt-shadow repair at {}",
                        path.display()
                    )
                },
            )?;
        let storage = Self::new(conn, path.to_path_buf());
        storage.apply_open_stage_busy_timeout();
        storage.apply_config()?;
        Ok(storage)
    }

    /// Open a writer connection that skips migration (assumes DB already migrated).
    ///
    /// Used by the BEGIN CONCURRENT parallel writer pool: each writer needs its
    /// own connection with config applied, but migrations have already been run
    /// by the primary connection.
    pub fn open_writer(path: &Path) -> Result<Self> {
        Self::open_writer_with_shared_caches(
            path,
            Arc::new(parking_lot::Mutex::new(HashMap::new())),
            Arc::new(parking_lot::Mutex::new(HashMap::new())),
            Arc::new(parking_lot::Mutex::new(HashSet::new())),
            Arc::new(parking_lot::Mutex::new(HashSet::new())),
            Arc::new(FtsShadowRunState::default()),
        )
    }

    pub(super) fn open_writer_with_shared_caches(
        path: &Path,
        ensured_agents: Arc<parking_lot::Mutex<HashMap<EnsuredAgentKey, i64>>>,
        ensured_workspaces: Arc<parking_lot::Mutex<HashMap<EnsuredWorkspaceKey, i64>>>,
        ensured_conversation_sources: Arc<
            parking_lot::Mutex<HashSet<EnsuredConversationSourceKey>>,
        >,
        ensured_daily_stats_keys: Arc<parking_lot::Mutex<HashSet<EnsuredDailyStatsKey>>>,
        fts_shadow_run: Arc<FtsShadowRunState>,
    ) -> Result<Self> {
        let _doctor_guard =
            acquire_doctor_mutation_db_open_guard(path, DOCTOR_MUTATION_DB_OPEN_LOCK_TIMEOUT)?;
        let conn = open_archive_writer_connection(path)
            .with_context(|| format!("opening frankensqlite writer at {}", path.display()))?;
        let storage = Self::new_with_shared_caches(
            conn,
            path.to_path_buf(),
            ensured_agents,
            ensured_workspaces,
            ensured_conversation_sources,
            ensured_daily_stats_keys,
            fts_shadow_run,
        );
        storage.apply_config()?;
        // Canonical schema readiness does not imply an FTS shadow exists.
        Ok(storage)
    }

    pub(crate) fn acquire_cached_ephemeral_writer(&self) -> Result<(Self, bool)> {
        let mut cached = self.cached_ephemeral_writer.lock();
        match std::mem::replace(&mut *cached, CachedEphemeralWriter::InUse) {
            CachedEphemeralWriter::Cached(conn) => {
                let (conn, checkpoint_pages, busy_timeout_ms) = (*conn).into_parts();
                let writer = Self::new_with_shared_caches(
                    conn,
                    self.db_path.clone(),
                    Arc::clone(&self.ensured_agents),
                    Arc::clone(&self.ensured_workspaces),
                    Arc::clone(&self.ensured_conversation_sources),
                    Arc::clone(&self.ensured_daily_stats_keys),
                    Arc::clone(&self.fts_shadow_run),
                );
                writer
                    .index_writer_checkpoint_pages
                    .store(checkpoint_pages, Ordering::Relaxed);
                writer
                    .index_writer_busy_timeout_ms
                    .store(busy_timeout_ms, Ordering::Relaxed);
                // Reacquisition constructs an UNKNOWN cache: neither a prior
                // missing shadow nor a newly materialized one may be guessed.
                Ok((writer, true))
            }
            CachedEphemeralWriter::Uninitialized => {
                drop(cached);
                match Self::open_writer_with_shared_caches(
                    &self.db_path,
                    Arc::clone(&self.ensured_agents),
                    Arc::clone(&self.ensured_workspaces),
                    Arc::clone(&self.ensured_conversation_sources),
                    Arc::clone(&self.ensured_daily_stats_keys),
                    Arc::clone(&self.fts_shadow_run),
                ) {
                    Ok(writer) => Ok((writer, true)),
                    Err(err) => {
                        let mut cached = self.cached_ephemeral_writer.lock();
                        if matches!(&*cached, CachedEphemeralWriter::InUse) {
                            *cached = CachedEphemeralWriter::Uninitialized;
                        }
                        Err(err)
                    }
                }
            }
            CachedEphemeralWriter::InUse => {
                *cached = CachedEphemeralWriter::InUse;
                drop(cached);
                Ok((
                    Self::open_writer_with_shared_caches(
                        &self.db_path,
                        Arc::clone(&self.ensured_agents),
                        Arc::clone(&self.ensured_workspaces),
                        Arc::clone(&self.ensured_conversation_sources),
                        Arc::clone(&self.ensured_daily_stats_keys),
                        Arc::clone(&self.fts_shadow_run),
                    )?,
                    false,
                ))
            }
        }
    }

    pub(crate) fn release_cached_ephemeral_writer(&self, writer: Self) {
        let checkpoint_pages = writer.index_writer_checkpoint_pages.load(Ordering::Relaxed);
        let busy_timeout_ms = writer.index_writer_busy_timeout_ms.load(Ordering::Relaxed);
        let conn = writer.into_raw();
        let mut cached = self.cached_ephemeral_writer.lock();
        debug_assert!(
            matches!(&*cached, CachedEphemeralWriter::InUse),
            "cached ephemeral writer state should be in-use when releasing"
        );
        *cached = CachedEphemeralWriter::Cached(Box::new(CachedRawFrankenConnection::new(
            conn,
            checkpoint_pages,
            busy_timeout_ms,
        )));
    }

    pub(crate) fn discard_cached_ephemeral_writer(&self, mut writer: Self) {
        writer.close_best_effort_in_place();
        let mut cached = self.cached_ephemeral_writer.lock();
        if matches!(&*cached, CachedEphemeralWriter::InUse) {
            *cached = CachedEphemeralWriter::Uninitialized;
        }
    }

    /// Close the idle cached writer without checkpointing and leave this
    /// storage with only its primary connection. Returns `true` when a cached
    /// writer was actually closed. An in-use writer is never stolen; callers
    /// invoke this only at a between-batches boundary where `InUse` would be a
    /// programming error rather than a state to race.
    ///
    /// A close error restores the still-live connection to the cache. Callers
    /// must not enter a single-connection-only checkpoint path unless this
    /// method confirms that the close completed.
    pub(crate) fn close_idle_cached_ephemeral_writer(&self) -> Result<bool> {
        let cached_writer = {
            let mut cached = self.cached_ephemeral_writer.lock();
            match std::mem::replace(&mut *cached, CachedEphemeralWriter::Uninitialized) {
                CachedEphemeralWriter::Cached(conn) => Some(conn),
                state => {
                    *cached = state;
                    None
                }
            }
        };
        let Some(mut cached_writer) = cached_writer else {
            return Ok(false);
        };
        if let Err(error) = close_franken_in_place_with_busy_retry(&mut cached_writer.conn, false) {
            let mut cached = self.cached_ephemeral_writer.lock();
            debug_assert!(
                matches!(&*cached, CachedEphemeralWriter::Uninitialized),
                "cached writer state changed while an idle writer was closing"
            );
            if matches!(&*cached, CachedEphemeralWriter::Uninitialized) {
                *cached = CachedEphemeralWriter::Cached(cached_writer);
            }
            return Err(error).with_context(
                || "closing idle cached frankensqlite writer before bulk WAL checkpoint",
            );
        }
        Ok(true)
    }

    pub(crate) fn cached_ephemeral_writer_in_use(&self) -> bool {
        matches!(
            &*self.cached_ephemeral_writer.lock(),
            CachedEphemeralWriter::InUse
        )
    }

    pub(super) fn cached_agent_id(&self, key: &EnsuredAgentKey) -> Option<i64> {
        self.ensured_agents.lock().get(key).copied()
    }

    pub(super) fn mark_agent_ensured(&self, key: EnsuredAgentKey, id: i64) {
        self.ensured_agents.lock().insert(key, id);
    }

    pub(super) fn cached_workspace_id(&self, key: &EnsuredWorkspaceKey) -> Option<i64> {
        self.ensured_workspaces.lock().get(key).copied()
    }

    pub(super) fn mark_workspace_ensured(&self, key: EnsuredWorkspaceKey, id: i64) {
        self.ensured_workspaces.lock().insert(key, id);
    }

    pub(super) fn conversation_source_already_ensured(
        &self,
        key: &EnsuredConversationSourceKey,
    ) -> bool {
        self.ensured_conversation_sources.lock().contains(key)
    }

    pub(super) fn mark_conversation_source_ensured(&self, key: EnsuredConversationSourceKey) {
        self.ensured_conversation_sources.lock().insert(key);
    }

    pub(super) fn daily_stats_key_already_ensured(&self, key: &EnsuredDailyStatsKey) -> bool {
        self.ensured_daily_stats_keys.lock().contains(key)
    }

    pub(super) fn daily_stats_keys_already_ensured(
        &self,
        keys: &[EnsuredDailyStatsKey; 4],
    ) -> bool {
        let ensured = self.ensured_daily_stats_keys.lock();
        keys.iter().all(|key| ensured.contains(key))
    }

    pub(super) fn mark_daily_stats_key_ensured(&self, key: EnsuredDailyStatsKey) {
        self.ensured_daily_stats_keys.lock().insert(key);
    }

    pub(super) fn fts_messages_present_cached(&self, tx: &FrankenTransaction<'_>) -> bool {
        match self.fts_messages_present_cache.load(Ordering::Acquire) {
            FTS_MESSAGES_PRESENT_PRESENT => return true,
            FTS_MESSAGES_PRESENT_ABSENT => return false,
            _ => {}
        }

        let present = tx
            .query_row_map(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE name = 'fts_messages'
                   AND type = 'table'",
                fparams![],
                |row| row.get_typed::<i64>(0),
            )
            .map(|count| count > 0)
            .unwrap_or_else(|err| {
                tracing::debug!(
                    error = %err,
                    "failed to probe fts_messages presence; skipping db-resident FTS maintenance"
                );
                false
            });
        self.set_fts_messages_present_cache(present);
        present
    }

    pub(super) fn set_fts_messages_present_cache(&self, present: bool) {
        self.fts_messages_present_cache.store(
            if present {
                FTS_MESSAGES_PRESENT_PRESENT
            } else {
                FTS_MESSAGES_PRESENT_ABSENT
            },
            Ordering::Release,
        );
    }

    pub(super) fn invalidate_fts_messages_present_cache(&self) {
        self.fts_messages_present_cache
            .store(FTS_MESSAGES_PRESENT_UNKNOWN, Ordering::Release);
    }

    pub(super) fn invalidate_conversation_source_cache(&self, source_id: &str) {
        self.ensured_conversation_sources
            .lock()
            .retain(|key| key.id != source_id);
    }

    fn close_cached_ephemeral_writer_best_effort_in_place(&mut self) {
        let cached = self.cached_ephemeral_writer.get_mut();
        if let CachedEphemeralWriter::Cached(conn) =
            std::mem::replace(cached, CachedEphemeralWriter::Uninitialized)
        {
            let mut conn = conn;
            conn.conn.close_best_effort_in_place();
        }
    }

    fn close_cached_ephemeral_writer_without_checkpoint_in_place(&mut self) -> Result<()> {
        let cached = self.cached_ephemeral_writer.get_mut();
        match std::mem::replace(cached, CachedEphemeralWriter::Uninitialized) {
            CachedEphemeralWriter::Cached(mut conn) => {
                close_franken_in_place_with_busy_retry(&mut conn.conn, false)
                    .with_context(|| "closing cached frankensqlite writer without final checkpoint")
            }
            CachedEphemeralWriter::Uninitialized | CachedEphemeralWriter::InUse => Ok(()),
        }
    }

    /// Open read-only, allowing locked recovery of the derived WAL index.
    pub fn open_readonly(path: &Path) -> Result<Self> {
        Self::open_readonly_with_doctor_lock_timeout(path, DOCTOR_MUTATION_DB_OPEN_LOCK_TIMEOUT)
    }

    /// Open in read-only mode with an explicit doctor mutation-lock timeout.
    ///
    /// This is primarily useful for probes that need to prove a reader would
    /// not enter the archive while `cass doctor --fix` owns the repair lock.
    pub fn open_readonly_with_doctor_lock_timeout(path: &Path, timeout: Duration) -> Result<Self> {
        let path_str = path.to_string_lossy().to_string();
        let _doctor_guard = acquire_doctor_mutation_db_open_guard(path, timeout)?;
        // fsqlite 0.3.x surfaces transient `BusyRecovery` from a readonly open
        // while a peer connection's WAL-index recovery is in flight (the same
        // class upstream retries in prepare's prologue since 55d7f2c5). The
        // lexical-rebuild page-prep workers open readonly right next to the
        // active writer, so a bounded retry here is load-bearing.
        let conn = match retry_transient_storage_op("open_readonly", || {
            FrankenConnection::open_schema_only_with_wal_index_recovery(&path_str)
                .map_err(anyhow::Error::new)
        }) {
            Ok(conn) => conn,
            // Same duplicate-fts_messages debris handling as the canonical
            // open: fsqlite 0.3.x refuses the historical duplicate schema row
            // at load. The dedupe is a sanctioned mutation from an ordinary
            // read lane — it repairs sqlite_master metadata only (the lexical
            // self-healing contract), then the readonly open is retried.
            Err(err) if format!("{err:#}").contains("conflicting virtual-table entries") => {
                tracing::warn!(
                    db = %path.display(),
                    error = %err,
                    "frankensqlite refused readonly open due to duplicate \
                     fts_messages schema rows; deduplicating via sqlite3 bridge"
                );
                dedupe_conflicting_fts_schema_rows_via_sqlite3(path)?;
                FrankenConnection::open_schema_only_with_wal_index_recovery(&path_str)
                    .map_err(anyhow::Error::new)?
            }
            // gh #389: a hard (non-busy) readonly failure with a non-empty
            // `-wal` present is the unclean-shutdown dirty-WAL shape — the
            // readonly connection cannot write the wal-index it needs to
            // recover. Run the same RW-open + TRUNCATE checkpoint the clean
            // shutdown path uses, then retry the readonly open once. The
            // doctor mutation guard acquired above is still held here.
            Err(err) if attempt_dirty_wal_recovery_checkpoint(path, &err) => {
                FrankenConnection::open_schema_only_with_wal_index_recovery(&path_str)
                    .map_err(anyhow::Error::new)
                    .with_context(|| {
                        format!(
                            "reopening frankensqlite db readonly after dirty-WAL checkpoint recovery at {}",
                            path.display()
                        )
                    })?
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("opening frankensqlite db readonly at {}", path.display())
                });
            }
        };
        let storage = Self::new(conn, path.to_path_buf());
        storage.apply_readonly_config()?;
        Ok(storage)
    }

    /// Open an archive for a strict non-mutating read.
    ///
    /// This deliberately omits the ordinary read opener's sanctioned recovery
    /// writes (WAL-index repair, duplicate-FTS repair and dirty-WAL checkpoint)
    /// and observes only an already-existing doctor lock. Callers must surface
    /// any recovery-required error and leave repair to an explicitly mutating
    /// command.
    pub(crate) fn open_strict_readonly(path: &Path) -> Result<Self> {
        Self::open_strict_readonly_with_timeout(path, DOCTOR_MUTATION_DB_OPEN_LOCK_TIMEOUT)
    }

    pub(crate) fn open_strict_readonly_with_timeout(
        path: &Path,
        timeout: Duration,
    ) -> Result<Self> {
        let path_str = path.to_string_lossy().to_string();
        // The caller supplies one end-to-end admission/open budget. Starting
        // the deadline after the doctor guard was acquired allowed a busy
        // repair lock and a busy database open to each consume `timeout`, so
        // `search --no-maintenance` could block for nearly twice its declared
        // strict-open bound.
        let deadline = Instant::now() + timeout;
        let _doctor_guard = acquire_existing_doctor_mutation_db_open_guard(path, timeout)?;
        let mut backoff = Duration::from_millis(4);
        let conn = loop {
            match open_franken_with_flags(&path_str, FrankenOpenFlags::SQLITE_OPEN_READ_ONLY)
                .map_err(anyhow::Error::new)
            {
                Ok(conn) => break conn,
                Err(err) if retryable_franken_anyhow(&err) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return Err(err).with_context(|| {
                            format!(
                                "strictly opening frankensqlite db readonly at {}",
                                path.display()
                            )
                        });
                    }
                    let remaining = deadline.saturating_duration_since(now);
                    sleep_with_franken_retry_backoff(
                        &mut backoff,
                        remaining,
                        Duration::from_millis(128),
                    );
                }
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!(
                            "strictly opening frankensqlite db readonly at {}",
                            path.display()
                        )
                    });
                }
            }
        };
        let storage = Self::new(conn, path.to_path_buf());
        storage.apply_readonly_config()?;
        Ok(storage)
    }

    pub fn close(self) -> Result<()> {
        let mut this = self;
        this.close_cached_ephemeral_writer_best_effort_in_place();
        close_franken_in_place_with_busy_retry(&mut this.conn, true)
            .with_context(|| "closing frankensqlite connection")
    }

    pub fn close_without_checkpoint(self) -> Result<()> {
        let mut this = self;
        this.close_cached_ephemeral_writer_without_checkpoint_in_place()?;
        close_franken_in_place_with_busy_retry(&mut this.conn, false)
            .with_context(|| "closing frankensqlite connection without final checkpoint")
    }

    pub fn close_best_effort_in_place(&mut self) {
        self.close_cached_ephemeral_writer_best_effort_in_place();
        self.conn.close_best_effort_in_place();
    }

    pub fn close_without_checkpoint_in_place(&mut self) -> Result<()> {
        self.close_cached_ephemeral_writer_without_checkpoint_in_place()?;
        close_franken_in_place_with_busy_retry(&mut self.conn, false)
            .with_context(|| "closing frankensqlite connection without final checkpoint")
    }

    /// Access the raw frankensqlite connection.
    pub fn raw(&self) -> &FrankenConnection {
        &self.conn
    }

    /// Consume the storage wrapper and return the underlying frankensqlite
    /// connection after migrations/repair have already been applied.
    pub fn into_raw(self) -> FrankenConnection {
        let mut this = self;
        this.close_cached_ephemeral_writer_best_effort_in_place();
        this.conn
    }

    /// Apply connection PRAGMAs for parity with SqliteStorage's `apply_pragmas()`.
    ///
    /// Frankensqlite supports all PRAGMAs cass uses (journal_mode, synchronous,
    /// cache_size, foreign_keys, busy_timeout). Its default journal_mode is already
    /// WAL and default synchronous is NORMAL, matching cass's requirements.
    ///
    pub fn apply_config(&self) -> Result<()> {
        // journal_mode: frankensqlite defaults to WAL, same as cass.
        // synchronous: frankensqlite defaults to NORMAL, same as cass.
        // Both are set explicitly for clarity.
        self.conn
            .execute("PRAGMA journal_mode = WAL;")
            .with_context(|| "setting journal_mode")?;
        self.conn
            .execute("PRAGMA synchronous = NORMAL;")
            .with_context(|| "setting synchronous")?;

        // cache_size: 64MB (negative value = KiB).
        self.conn
            .execute("PRAGMA cache_size = -65536;")
            .with_context(|| "setting cache_size")?;

        // foreign_keys: enable constraint enforcement.
        self.conn
            .execute("PRAGMA foreign_keys = ON;")
            .with_context(|| "setting foreign_keys")?;

        // busy_timeout: 5 seconds (in milliseconds).
        self.conn
            .execute("PRAGMA busy_timeout = 5000;")
            .with_context(|| "setting busy_timeout")?;

        // temp_store = MEMORY and mmap_size are C SQLite performance knobs.
        // In frankensqlite's architecture (in-memory MVCC engine with pager
        // backend), temp_store is always memory-resident and mmap_size does not
        // apply. Skipped intentionally — these are no-ops or errors.

        // wal_autocheckpoint: use a bounded cadence that avoids checkpointing
        // inside common append batches without deferring checkpoints forever.
        let checkpoint_pragma =
            format!("PRAGMA wal_autocheckpoint = {DEFAULT_WAL_AUTOCHECKPOINT_PAGES};");
        let _ = self.conn.execute(&checkpoint_pragma);
        self.index_writer_checkpoint_pages
            .store(DEFAULT_WAL_AUTOCHECKPOINT_PAGES, Ordering::Relaxed);
        // Explicitly enable concurrent writer mode for BEGIN/transaction paths.
        // Try both namespace variants for compatibility across fsqlite builds.
        let _ = self.conn.execute("PRAGMA fsqlite.concurrent_mode = ON;");
        let _ = self.conn.execute("PRAGMA concurrent_mode = ON;");
        // Frankensqlite retained autocommit currently mis-serves same-connection
        // read-after-write queries on cass's storage paths; keep it off here
        // until the upstream visibility bug is fixed.
        //
        // CASS #163 item 3: If neither PRAGMA variant succeeds, the MVCC engine
        // will accumulate write snapshots for the lifetime of the connection,
        // causing unbounded memory growth on long-lived watch-mode handles.
        // Log at warn level so the failure is visible instead of silently
        // swallowed, and set a flag for callers that need to periodically
        // recycle the connection.
        match disable_autocommit_retain(|pragma| self.conn.execute(pragma).map(|_| ())) {
            Ok(autocommit_pragma) => {
                tracing::debug!(
                    pragma = autocommit_pragma,
                    "disabled frankensqlite autocommit_retain for storage connection"
                );
            }
            Err(err) => {
                let detail = format!("{err:#}");
                if error_message_indicates_populated_fts_shadow_without_rowid_reload(&detail) {
                    tracing::warn!(
                        error = %detail,
                        "frankensqlite could not disable autocommit_retain because a populated derived FTS shadow table cannot yet be reloaded; continuing so canonical indexing can proceed"
                    );
                } else {
                    return Err(err);
                }
            }
        }

        Ok(())
    }

    fn apply_readonly_config(&self) -> Result<()> {
        self.conn
            .execute("PRAGMA query_only = 1;")
            .with_context(|| "setting query_only")?;
        self.conn
            .execute("PRAGMA busy_timeout = 5000;")
            .with_context(|| "setting busy_timeout")?;
        self.conn
            .execute("PRAGMA cache_size = -65536;")
            .with_context(|| "setting cache_size")?;
        self.conn
            .execute("PRAGMA foreign_keys = ON;")
            .with_context(|| "setting foreign_keys")?;
        Ok(())
    }

    /// Run all schema migrations, handling transition from meta table versioning.
    ///
    /// The existing `SqliteStorage` tracks schema version in a `meta` table entry.
    /// The new `MigrationRunner` uses a `_schema_migrations` table. This method:
    /// 1. Transitions existing databases from meta table → `_schema_migrations`
    /// 2. Runs pending migrations via `MigrationRunner`
    /// 3. Syncs `meta.schema_version` for backward compatibility
    ///
    /// # Fresh vs existing databases
    ///
    /// Fresh databases use a single combined migration (`MIGRATION_FRESH_SCHEMA`)
    /// that creates the complete V13 schema directly. This avoids the incremental
    /// V5 migration which uses `DROP TABLE` — an operation that triggers a known
    /// frankensqlite autoindex limitation.
    ///
    /// Existing databases (transitioned from SqliteStorage) are typically at
    /// V13 or newer already; additive post-V13 migrations are applied normally.
    pub fn run_migrations(&self) -> Result<()> {
        transition_from_meta_version(&self.conn)?;
        self.refuse_unbounded_legacy_fts_teardown()?;

        let (mut applied, was_fresh) =
            run_attributed_migration_steps(&self.conn, BASE_MIGRATION_STEPS)?;
        if apply_conversation_tail_state_cache_migration(&self.conn)
            .with_context(|| "running conversation tail-state cache migration")?
        {
            applied.push(15);
        }

        let (post_applied, _) =
            run_attributed_migration_steps(&self.conn, POST_TAIL_CACHE_MIGRATION_STEPS)?;
        applied.extend(post_applied);

        let current = self.schema_version()?;
        if !applied.is_empty() {
            info!(
                applied = ?applied,
                current,
                was_fresh,
                "frankensqlite schema migrations applied"
            );
        }

        // Keep meta.schema_version in sync for backward compatibility.
        self.sync_meta_schema_version(current)?;

        Ok(())
    }

    /// GH #349: bounded pre-flight for the V14 `fts_contentless` migration.
    ///
    /// When the archive still carries a legacy `fts_messages` FTS5 shadow,
    /// the migration must DROP it, and that teardown is unbounded inside
    /// frankensqlite — on multi-GB archives it aborts with `out of memory`
    /// after minutes at multi-GiB RSS. Above the configured byte budget this
    /// refuses up front with a typed, actionable error instead of attempting
    /// the drop, leaving the archive untouched. Small archives migrate
    /// exactly as before, and `CASS_MIGRATION_LEGACY_FTS_DROP_BUDGET_BYTES`
    /// (see [`LEGACY_FTS_TEARDOWN_BUDGET_ENV`]) tunes or disables the gate.
    ///
    /// Every probe here is bounded: one indexed sqlite_master row read, one
    /// `_schema_migrations` point query, and file metadata for the archive
    /// family. Probes that themselves fail fail open — the migration then
    /// runs and any real failure surfaces attributed per step.
    fn refuse_unbounded_legacy_fts_teardown(&self) -> Result<()> {
        let carries_fts_messages = self
            .conn
            .query(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' \
                 AND name = 'fts_messages' LIMIT 1;",
            )
            .map(|rows| !rows.is_empty())
            .unwrap_or(false);
        if !carries_fts_messages {
            return Ok(());
        }
        let Some(budget_bytes) = legacy_fts_teardown_budget_bytes() else {
            return Ok(());
        };
        let v14_pending = !schema_migration_is_applied(&self.conn, 14).unwrap_or(true);
        if !v14_pending {
            return Ok(());
        }
        let archive_bytes = archive_family_bytes(&self.db_path);
        if archive_bytes <= budget_bytes {
            return Ok(());
        }
        tracing::warn!(
            archive_bytes,
            budget_bytes,
            env = LEGACY_FTS_TEARDOWN_BUDGET_ENV,
            "refusing unbounded legacy fts_messages teardown during schema migration"
        );
        Err(anyhow::Error::new(LegacyFtsTeardownRefusal {
            archive_bytes,
            budget_bytes,
        }))
    }

    /// Some historical canonical rebuild paths produced databases whose
    /// version markers claim the current schema while post-V10 analytics
    /// tables were never materialized. Detect that drift and backfill the
    /// idempotent table/index set from the combined schema migration.
    pub(super) fn repair_missing_current_schema_objects(&self) -> Result<()> {
        let mut missing_tables = Vec::new();
        for &(table_name, probe_sql) in REQUIRED_CURRENT_SCHEMA_TABLE_PROBES {
            if let Err(err) = self.conn.query(probe_sql) {
                if error_indicates_missing_table(&err) {
                    missing_tables.push(table_name);
                    continue;
                }
                return Err(err).with_context(|| {
                    format!("probing required schema table {table_name} for completeness")
                });
            }
        }

        if !missing_tables.is_empty() {
            info!(
                missing_tables = ?missing_tables,
                "repairing missing current-schema tables on an already-versioned cass database"
            );

            for batch in current_schema_repair_batches_for_missing_tables(&missing_tables)? {
                self.conn
                    .execute_batch(batch.sql)
                    .with_context(|| format!("repairing current-schema batch {}", batch.name))?;
            }

            for &(table_name, probe_sql) in REQUIRED_CURRENT_SCHEMA_TABLE_PROBES {
                if !missing_tables.contains(&table_name) {
                    continue;
                }
                self.conn
                    .query(probe_sql)
                    .with_context(|| format!("verifying repaired schema table {table_name}"))?;
            }
        }
        self.repair_missing_conversation_token_columns()?;
        // Local fix (review 2026-11-02, S2).
        local_lookup_indexes::ensure_snippets_message_id_index(&self.conn)?;
        Ok(())
    }

    pub(super) fn repair_missing_conversation_token_columns(&self) -> Result<()> {
        let columns = franken_table_column_names(&self.conn, "conversations")
            .with_context(|| "inspecting conversations columns for token-summary repair")?;
        let mut missing_columns = Vec::new();
        for &(column_name, column_type) in REQUIRED_CONVERSATION_TOKEN_COLUMNS {
            if columns.contains(column_name) {
                continue;
            }
            let sql = format!("ALTER TABLE conversations ADD COLUMN {column_name} {column_type};");
            self.conn.execute(&sql).with_context(|| {
                format!("adding missing conversations.{column_name} token-summary column")
            })?;
            missing_columns.push(column_name);
        }
        if !missing_columns.is_empty() {
            tracing::warn!(
                target: "cass::schema_repair",
                db_path = %self.db_path.display(),
                missing_columns = ?missing_columns,
                "cass#222: repaired missing conversations token-summary columns"
            );
        }
        Ok(())
    }

    /// Detect and remove orphan rows whose FK parent has gone missing.
    ///
    /// A `Connection` dropped mid-transaction (the `drop_close` warning emitted
    /// by frankensqlite's `Drop` impl) can leave child rows persisted without a
    /// matching parent — `messages` referencing a `conversation_id` that does
    /// not exist, `message_metrics`/`token_usage`/`snippets` referencing a
    /// `message_id` that does not exist, etc. With `PRAGMA foreign_keys = ON`,
    /// every subsequent indexer pass then trips `FOREIGN KEY constraint failed`
    /// on the next write, the session never gets marked indexed, and the
    /// pending backlog grows without bound (issue #202).
    ///
    /// This pass runs at indexer startup as defense in depth: it scans each
    /// child table for rows whose parent row has gone missing and removes them
    /// in bounded committed chunks, breaking the failure cycle even when the
    /// underlying transaction-discipline bug has not been fully root-caused.
    /// The pass is idempotent (a clean database is a no-op), and emits a
    /// `WARN` after successful cleanup so the upstream `drop_close` condition
    /// stays visible.
    pub(crate) fn cleanup_orphan_fk_rows(&self) -> Result<OrphanFkCleanupReport> {
        let mut report = OrphanFkCleanupReport::default();
        let orphan_message_ids = match collect_orphan_message_ids(&self.conn) {
            Ok(ids) => ids,
            Err(err) if error_indicates_missing_table(&err) => {
                tracing::debug!(
                    target: "cass::fk_repair",
                    child_table = "messages",
                    error = %err,
                    "skipping orphan-message probe (table or column unavailable)"
                );
                Vec::new()
            }
            Err(err) => return Err(err),
        };
        if !orphan_message_ids.is_empty() {
            report.record("messages", orphan_message_ids.len() as i64);
        }

        if !orphan_message_ids.is_empty() {
            delete_orphan_message_ids_bisecting_oom(&self.conn, &orphan_message_ids)
                .context("deleting orphan message rows and dependent children")?;
        }

        for entry in ORPHAN_DIRECT_CHILD_TABLES {
            loop {
                let ids = match collect_direct_orphan_id_page(&self.conn, entry) {
                    Ok(ids) => ids,
                    Err(err)
                        if error_indicates_missing_table(&err)
                            || error_indicates_missing_column(&err) =>
                    {
                        // Tolerant probe: a missing child/parent table or FK
                        // column on older schemas means there is nothing to
                        // clean up for this table.
                        tracing::debug!(
                            target: "cass::fk_repair",
                            child_table = entry.child_table,
                            error = %err,
                            "skipping orphan probe (table or column unavailable)"
                        );
                        break;
                    }
                    Err(err) => {
                        return Err(err).with_context(|| {
                            format!("probing orphan rows in {}", entry.child_table)
                        });
                    }
                };
                if ids.is_empty() {
                    break;
                }

                let deleted = delete_direct_orphan_ids_bisecting_oom(&self.conn, entry, &ids)
                    .with_context(|| format!("deleting orphan rows from {}", entry.child_table))?;
                if deleted == 0 {
                    break;
                }
                report.record(
                    entry.child_table,
                    i64::try_from(deleted).unwrap_or(i64::MAX),
                );
            }
        }

        if report.total == 0 {
            return Ok(report);
        }

        // WARN only fires after a successful commit so the message accurately
        // reflects what actually happened on disk. db_path is included so logs
        // from concurrent indexers against different databases stay
        // disambiguated.
        tracing::warn!(
            target: "cass::fk_repair",
            db_path = %self.db_path.display(),
            total_orphans = report.total,
            per_table = ?report.per_table,
            "cass#202: removed orphan rows left behind by interrupted index transactions"
        );

        Ok(report)
    }

    /// Return the current schema version from `_schema_migrations`.
    pub fn schema_version(&self) -> Result<i64> {
        let rows = self
            .conn
            .query("SELECT MAX(version) FROM _schema_migrations;")
            .with_context(|| "reading schema version from _schema_migrations")?;

        if let Some(row) = rows.first()
            && let Ok(v) = row.get_typed::<Option<i64>>(0)
        {
            return Ok(v.unwrap_or(0));
        }
        Ok(0)
    }

    /// Keep `meta.schema_version` in sync for backward compatibility with `SqliteStorage`.
    pub(super) fn sync_meta_schema_version(&self, version: i64) -> Result<()> {
        // The meta table is created by V1 migration. If it doesn't exist yet,
        // there's nothing to sync.
        if self.conn.query("SELECT key FROM meta LIMIT 1;").is_err() {
            return Ok(());
        }

        // Only write if the version needs updating to avoid write lock contention
        if let Ok(rows) = self
            .conn
            .query("SELECT value FROM meta WHERE key = 'schema_version';")
            && let Some(row) = rows.first()
            && let Ok(val) = row.get_typed::<String>(0)
            && val == version.to_string()
        {
            return Ok(()); // Already up to date
        }

        self.conn
            .execute_compat(
                "INSERT OR REPLACE INTO meta(key, value) VALUES('schema_version', ?1);",
                &[ParamValue::from(version.to_string())],
            )
            .with_context(|| "syncing meta schema_version")?;

        Ok(())
    }

    /// Resolve the database file path for this connection.
    pub fn database_path(&self) -> Result<PathBuf> {
        Ok(self.db_path.clone())
    }

    pub(crate) fn ephemeral_writer_preflight_verified(&self) -> bool {
        self.ephemeral_writer_preflight_verified
            .load(Ordering::Relaxed)
    }

    pub(crate) fn mark_ephemeral_writer_preflight_verified(&self) {
        self.ephemeral_writer_preflight_verified
            .store(true, Ordering::Relaxed);
    }

    pub(crate) fn index_writer_checkpoint_pages(&self) -> Option<i64> {
        let pages = self.index_writer_checkpoint_pages.load(Ordering::Relaxed);
        (pages != UNSET_INDEX_WRITER_CHECKPOINT_PAGES).then_some(pages)
    }

    pub(crate) fn mark_index_writer_checkpoint_pages(&self, pages: i64) {
        self.index_writer_checkpoint_pages
            .store(pages, Ordering::Relaxed);
    }

    pub(crate) fn index_writer_busy_timeout_ms(&self) -> Option<u64> {
        let timeout_ms = self.index_writer_busy_timeout_ms.load(Ordering::Relaxed);
        (timeout_ms != UNSET_INDEX_WRITER_BUSY_TIMEOUT_MS).then_some(timeout_ms)
    }

    pub(crate) fn mark_index_writer_busy_timeout_ms(&self, timeout_ms: u64) {
        self.index_writer_busy_timeout_ms
            .store(timeout_ms, Ordering::Relaxed);
    }

    pub(crate) fn bulk_single_connection_enabled(&self) -> bool {
        self.bulk_single_connection.load(Ordering::Relaxed)
    }

    pub(crate) fn enable_bulk_single_connection(&self) {
        // The existing preflight result belongs to the cached ephemeral
        // connection that was just closed. The primary must perform its own
        // write preflight after the WAL reset; otherwise a stale primary MVCC
        // clock could remain hidden until the first real persistence write.
        self.ephemeral_writer_preflight_verified
            .store(false, Ordering::Relaxed);
        self.bulk_single_connection.store(true, Ordering::Relaxed);
    }

    pub(crate) fn bulk_checkpoint_retry_at_bytes(&self) -> u64 {
        self.bulk_checkpoint_retry_at_bytes.load(Ordering::Relaxed)
    }

    pub(crate) fn set_bulk_checkpoint_retry_at_bytes(&self, retry_at_bytes: u64) {
        self.bulk_checkpoint_retry_at_bytes
            .store(retry_at_bytes, Ordering::Relaxed);
    }

    /// Open database with migration, backing up if schema is incompatible.
    pub fn open_or_rebuild(path: &Path) -> std::result::Result<Self, MigrationError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        if path.exists() {
            let check_result = check_schema_compatibility(path);
            match check_result {
                Ok(SchemaCheck::Compatible) | Ok(SchemaCheck::NeedsMigration) => {
                    // Continue with normal open
                }
                Ok(SchemaCheck::NeedsRebuild(reason)) => {
                    let backup_path = create_backup(path)?;
                    cleanup_old_backups(path, MAX_BACKUPS)?;
                    remove_database_files(path)?;
                    return Err(MigrationError::RebuildRequired {
                        reason,
                        backup_path,
                    });
                }
                Err(err) if schema_check_error_requires_rebuild(&err) => {
                    let backup_path = create_backup(path)?;
                    cleanup_old_backups(path, MAX_BACKUPS)?;
                    remove_database_files(path)?;
                    return Err(MigrationError::RebuildRequired {
                        reason: format!("Database appears corrupted: {err}"),
                        backup_path,
                    });
                }
                Err(err) => return Err(MigrationError::Database(err)),
            }
        }

        let storage = Self::open(path).map_err(|e| MigrationError::Other(e.to_string()))?;
        Ok(storage)
    }
}
