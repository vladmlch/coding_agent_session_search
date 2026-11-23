use super::backup_and_integrity::{
    database_sidecar_path, dedupe_conflicting_fts_schema_rows_via_sqlite3,
};
use super::connection_pool::FrankenOwnerConnection;
use super::schema_migrations::CURRENT_SCHEMA_VERSION;
use super::schema_repair::transition_from_meta_version;
use super::storage_core::FrankenStorage;
use super::wal_checkpoint_status;
use crate::franken_sync::{
    Connection as FrankenConnection,
    compat::{OpenFlags as FrankenOpenFlags, RowExt as FrankenRowExt},
};
use anyhow::{Context, Result, anyhow};
use frankensqlite::AsyncConnection as FrankenAsyncConnection;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

pub(super) const DOCTOR_MUTATION_DB_OPEN_LOCK_TIMEOUT: Duration = Duration::from_secs(30);
const DOCTOR_MUTATION_LOCK_MAX_METADATA_READ: u64 = 64 * 1024;

static FRANKEN_RETRY_JITTER_STATE: AtomicU64 = AtomicU64::new(0x9e37_79b9_7f4a_7c15);
static DOCTOR_MUTATION_DB_OPEN_BYPASS_DEPTH: AtomicUsize = AtomicUsize::new(0);
pub(super) static MESSAGE_LOOKUP_TRACE_ENABLED: AtomicBool = AtomicBool::new(false);
static MESSAGE_LOOKUP_EXACT_IDX_PROBES: AtomicU64 = AtomicU64::new(0);
static MESSAGE_LOOKUP_BOUNDED_QUERIES: AtomicU64 = AtomicU64::new(0);
static MESSAGE_LOOKUP_FULL_SCAN_QUERIES: AtomicU64 = AtomicU64::new(0);
static MESSAGE_LOOKUP_ROWS_MATERIALIZED: AtomicU64 = AtomicU64::new(0);
pub(super) static DEFER_ANALYTICS_UPDATES_GUARD_DEPTH: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub(crate) struct MessageLookupTraceCounters {
    pub exact_idx_probes: u64,
    pub bounded_lookup_queries: u64,
    pub full_scan_queries: u64,
    pub rows_materialized: u64,
}

impl MessageLookupTraceCounters {
    pub(crate) fn saturating_sub(self, before: Self) -> Self {
        Self {
            exact_idx_probes: self
                .exact_idx_probes
                .saturating_sub(before.exact_idx_probes),
            bounded_lookup_queries: self
                .bounded_lookup_queries
                .saturating_sub(before.bounded_lookup_queries),
            full_scan_queries: self
                .full_scan_queries
                .saturating_sub(before.full_scan_queries),
            rows_materialized: self
                .rows_materialized
                .saturating_sub(before.rows_materialized),
        }
    }

    pub(crate) fn lookups_against_global(self) -> u64 {
        self.exact_idx_probes.saturating_add(self.rows_materialized)
    }
}

pub(crate) fn set_message_lookup_trace_enabled(enabled: bool) -> bool {
    MESSAGE_LOOKUP_TRACE_ENABLED.swap(enabled, Ordering::Relaxed)
}

pub(crate) fn message_lookup_trace_snapshot() -> MessageLookupTraceCounters {
    MessageLookupTraceCounters {
        exact_idx_probes: MESSAGE_LOOKUP_EXACT_IDX_PROBES.load(Ordering::Relaxed),
        bounded_lookup_queries: MESSAGE_LOOKUP_BOUNDED_QUERIES.load(Ordering::Relaxed),
        full_scan_queries: MESSAGE_LOOKUP_FULL_SCAN_QUERIES.load(Ordering::Relaxed),
        rows_materialized: MESSAGE_LOOKUP_ROWS_MATERIALIZED.load(Ordering::Relaxed),
    }
}

pub(crate) struct DeferAnalyticsUpdatesGuard {
    depth: &'static AtomicUsize,
}

impl Drop for DeferAnalyticsUpdatesGuard {
    fn drop(&mut self) {
        self.depth.fetch_sub(1, Ordering::SeqCst);
    }
}

pub(super) fn defer_analytics_updates_guard_for(
    depth: &'static AtomicUsize,
) -> DeferAnalyticsUpdatesGuard {
    depth.fetch_add(1, Ordering::SeqCst);
    DeferAnalyticsUpdatesGuard { depth }
}

pub(crate) fn defer_analytics_updates_guard() -> DeferAnalyticsUpdatesGuard {
    defer_analytics_updates_guard_for(&DEFER_ANALYTICS_UPDATES_GUARD_DEPTH)
}

pub(super) fn record_message_lookup_bounded_queries(query_count: u64, rows: usize) {
    if MESSAGE_LOOKUP_TRACE_ENABLED.load(Ordering::Relaxed) {
        MESSAGE_LOOKUP_BOUNDED_QUERIES.fetch_add(query_count, Ordering::Relaxed);
        MESSAGE_LOOKUP_ROWS_MATERIALIZED.fetch_add(rows as u64, Ordering::Relaxed);
    }
}

pub(super) fn record_message_lookup_full_scan_query(rows: usize) {
    if MESSAGE_LOOKUP_TRACE_ENABLED.load(Ordering::Relaxed) {
        MESSAGE_LOOKUP_FULL_SCAN_QUERIES.fetch_add(1, Ordering::Relaxed);
        MESSAGE_LOOKUP_ROWS_MATERIALIZED.fetch_add(rows as u64, Ordering::Relaxed);
    }
}

pub(crate) struct DoctorMutationDbOpenBypassGuard;

impl Drop for DoctorMutationDbOpenBypassGuard {
    fn drop(&mut self) {
        DOCTOR_MUTATION_DB_OPEN_BYPASS_DEPTH.fetch_sub(1, Ordering::SeqCst);
    }
}

pub(crate) fn enter_doctor_mutation_db_open_bypass() -> DoctorMutationDbOpenBypassGuard {
    DOCTOR_MUTATION_DB_OPEN_BYPASS_DEPTH.fetch_add(1, Ordering::SeqCst);
    DoctorMutationDbOpenBypassGuard
}

fn doctor_mutation_db_open_bypass_active() -> bool {
    DOCTOR_MUTATION_DB_OPEN_BYPASS_DEPTH.load(Ordering::SeqCst) > 0
}

fn next_franken_retry_jitter_ms(max_inclusive: u64) -> u64 {
    let mut value = FRANKEN_RETRY_JITTER_STATE.fetch_add(0x9e37_79b9_7f4a_7c15, Ordering::Relaxed);
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^= value >> 31;
    value % max_inclusive.saturating_add(1)
}

/// Sleep with jittered exponential backoff to avoid lock-step retry storms
/// when many threads hit the same transient SQLite/frankensqlite contention.
pub(crate) fn sleep_with_franken_retry_backoff(
    backoff: &mut Duration,
    remaining: Duration,
    max_backoff: Duration,
) {
    let capped = (*backoff).min(remaining);
    let extra_budget = remaining.saturating_sub(capped).min(capped);
    let extra_ms = extra_budget.as_millis().min(u128::from(u64::MAX)) as u64;
    let sleep_for = if extra_ms == 0 {
        capped
    } else {
        capped
            .saturating_add(Duration::from_millis(next_franken_retry_jitter_ms(
                extra_ms,
            )))
            .min(remaining)
    };
    std::thread::sleep(sleep_for);
    *backoff = backoff.saturating_mul(2).min(max_backoff);
}

pub(super) struct DoctorMutationDbOpenGuard(Option<fs::File>);

impl Drop for DoctorMutationDbOpenGuard {
    fn drop(&mut self) {
        if let Some(file) = self.0.as_ref() {
            let _ = fs2::FileExt::unlock(file);
        }
    }
}

pub(super) fn doctor_mutation_lock_path_for_db_open(db_path: &Path) -> Option<PathBuf> {
    if db_path.file_name().and_then(|name| name.to_str()) != Some("agent_search.db") {
        return None;
    }

    Some(
        db_path
            .parent()?
            .join("doctor")
            .join("locks")
            .join("doctor-repair.lock"),
    )
}

pub(super) fn doctor_lock_metadata_pid_is_current_process(raw: &str) -> bool {
    raw.lines().any(|line| {
        let Some((key, value)) = line.split_once('=') else {
            return false;
        };
        key.trim() == "pid"
            && value
                .trim()
                .parse::<u32>()
                .is_ok_and(|pid| pid == std::process::id())
    })
}

fn doctor_lock_file_pid_is_current_process(file: &fs::File) -> bool {
    use std::io::Read as _;

    let Ok(mut file) = file.try_clone() else {
        return false;
    };
    let mut raw = String::new();
    let _ = std::io::Read::take(&mut file, DOCTOR_MUTATION_LOCK_MAX_METADATA_READ)
        .read_to_string(&mut raw);
    doctor_lock_metadata_pid_is_current_process(&raw)
}

/// fs2 reports a contended try-lock as its `lock_contended_error()`:
/// EWOULDBLOCK on unix, but raw ERROR_LOCK_VIOLATION on Windows, which no
/// `ErrorKind` matches, so a held doctor lock read as not held (2l1b0.74).
fn doctor_mutation_lock_error_is_active(err: &std::io::Error) -> bool {
    err.kind() == std::io::ErrorKind::WouldBlock
        || (err.raw_os_error().is_some()
            && err.raw_os_error() == fs2::lock_contended_error().raw_os_error())
}

pub(super) fn acquire_doctor_mutation_db_open_guard(
    db_path: &Path,
    timeout: Duration,
) -> Result<DoctorMutationDbOpenGuard> {
    let Some(lock_path) = doctor_mutation_lock_path_for_db_open(db_path) else {
        return Ok(DoctorMutationDbOpenGuard(None));
    };
    if doctor_mutation_db_open_bypass_active() {
        return Ok(DoctorMutationDbOpenGuard(None));
    }

    if let Some(parent) = lock_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "creating doctor mutation lock directory {} before opening {}",
                parent.display(),
                db_path.display()
            )
        })?;
    }

    let deadline = Instant::now() + timeout;
    let mut backoff = Duration::from_millis(4);
    loop {
        let file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
            .with_context(|| {
                format!(
                    "opening doctor mutation lock {} before opening {}",
                    lock_path.display(),
                    db_path.display()
                )
            })?;

        if doctor_lock_file_pid_is_current_process(&file) {
            return Ok(DoctorMutationDbOpenGuard(None));
        }

        match fs2::FileExt::try_lock_shared(&file) {
            Ok(()) => return Ok(DoctorMutationDbOpenGuard(Some(file))),
            Err(err) if doctor_mutation_lock_error_is_active(&err) => {
                let now = Instant::now();
                if now >= deadline {
                    return Err(anyhow!(
                        "doctor mutation lock {} is active while opening {}; refusing to open during repair after waiting {}ms",
                        lock_path.display(),
                        db_path.display(),
                        timeout.as_millis()
                    ));
                }
                let remaining = deadline.saturating_duration_since(now);
                sleep_with_franken_retry_backoff(
                    &mut backoff,
                    remaining,
                    Duration::from_millis(128),
                );
            }
            Err(err) => {
                return Err(anyhow!(
                    "failed to acquire shared doctor mutation lock {} before opening {}: {}",
                    lock_path.display(),
                    db_path.display(),
                    err
                ));
            }
        }
    }
}

/// Acquire the doctor-repair admission lock without creating any filesystem
/// object. Strict read-only callers use this variant so a search against an
/// otherwise untouched data directory cannot create `doctor/locks/` merely by
/// observing the archive. If the lock file does not already exist, there is no
/// doctor repair to coordinate with and the caller proceeds without a guard.
pub(super) fn acquire_existing_doctor_mutation_db_open_guard(
    db_path: &Path,
    timeout: Duration,
) -> Result<DoctorMutationDbOpenGuard> {
    let Some(lock_path) = doctor_mutation_lock_path_for_db_open(db_path) else {
        return Ok(DoctorMutationDbOpenGuard(None));
    };
    if doctor_mutation_db_open_bypass_active() {
        return Ok(DoctorMutationDbOpenGuard(None));
    }

    let deadline = Instant::now() + timeout;
    let mut backoff = Duration::from_millis(4);
    loop {
        let file = match fs::OpenOptions::new().read(true).open(&lock_path) {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Ok(DoctorMutationDbOpenGuard(None));
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!(
                        "opening existing doctor mutation lock {} before strict read-only open of {}",
                        lock_path.display(),
                        db_path.display()
                    )
                });
            }
        };

        match fs2::FileExt::try_lock_shared(&file) {
            Ok(()) => return Ok(DoctorMutationDbOpenGuard(Some(file))),
            Err(err) if doctor_mutation_lock_error_is_active(&err) => {
                let now = Instant::now();
                if now >= deadline {
                    return Err(anyhow!(
                        "doctor mutation lock {} is active while strictly opening {} read-only; refusing to race repair after waiting {}ms",
                        lock_path.display(),
                        db_path.display(),
                        timeout.as_millis()
                    ));
                }
                let remaining = deadline.saturating_duration_since(now);
                sleep_with_franken_retry_backoff(
                    &mut backoff,
                    remaining,
                    Duration::from_millis(128),
                );
            }
            Err(err) => {
                return Err(anyhow!(
                    "failed to acquire shared existing doctor mutation lock {} before strict read-only open of {}: {}",
                    lock_path.display(),
                    db_path.display(),
                    err
                ));
            }
        }
    }
}

pub(crate) fn open_franken_storage_with_timeout(
    path: &Path,
    timeout: Duration,
) -> Result<FrankenStorage> {
    if !path.exists() {
        return Err(anyhow!("Database not found at {}", path.display()));
    }

    let deadline = Instant::now() + timeout;
    let mut backoff = Duration::from_millis(4);
    loop {
        match FrankenStorage::open(path) {
            Ok(storage) => return Ok(storage),
            Err(err) if retryable_franken_anyhow(&err) => {
                let now = Instant::now();
                if now >= deadline {
                    return Err(err);
                }
                let remaining = deadline.saturating_duration_since(now);
                sleep_with_franken_retry_backoff(
                    &mut backoff,
                    remaining,
                    Duration::from_millis(128),
                );
            }
            Err(err) => return Err(err),
        }
    }
}

pub(crate) fn open_current_schema_storage_with_timeout(
    path: &Path,
    timeout: Duration,
) -> Result<Option<FrankenStorage>> {
    if !path.exists() {
        return Ok(None);
    }

    let mut storage = FrankenStorage::new(
        open_index_schema_connection_with_timeout(path, timeout)?,
        path.to_path_buf(),
    );
    storage.apply_open_stage_busy_timeout();

    let version = storage
        .raw()
        .query("SELECT value FROM meta WHERE key = 'schema_version';")
        .ok()
        .and_then(|rows| rows.first().cloned())
        .and_then(|row| row.get_typed::<String>(0).ok())
        .and_then(|raw| raw.parse::<i64>().ok());

    if version != Some(CURRENT_SCHEMA_VERSION) {
        if let Err(close_err) = storage.close_without_checkpoint_in_place() {
            tracing::debug!(
                error = %close_err,
                db_path = %path.display(),
                "open_current_schema_storage_with_timeout: close_without_checkpoint_in_place failed; falling back to best-effort close"
            );
            storage.close_best_effort_in_place();
        }
        return Ok(None);
    }

    transition_from_meta_version(&storage.conn)?;
    storage.repair_missing_current_schema_objects()?;
    storage.apply_config()?;
    Ok(Some(storage))
}

/// Ordinary engine handles permit prepared-query optimization to hydrate the
/// whole compatibility database. Schema-only handles prohibit that promotion
/// (fsqlite #402). Use one only when the pinned engine would skip its first-open
/// repair anyway (GH #443/#450); FTS validation and CASS repair remain enabled.
/// An archive whose repair is pending gets an ordinary open to run it, then
/// continues in the schema-only lane once the engine has recorded it.
fn open_index_schema_connection_with_timeout(
    path: &Path,
    timeout: Duration,
) -> Result<FrankenConnection> {
    if !index_engine_migration_is_complete(path) {
        let repairing = open_franken_raw_connection_with_timeout(path, timeout)?;
        if !index_engine_migration_is_complete(path) {
            return Ok(repairing);
        }
        close_first_open_repair_handle(repairing, path);
    }
    let deadline = Instant::now() + timeout;
    let mut backoff = Duration::from_millis(4);
    loop {
        let _doctor_guard = acquire_doctor_mutation_db_open_guard(path, timeout)?;
        match FrankenConnection::open_existing_schema_only(path.to_string_lossy().to_string()) {
            Ok(conn) => return Ok(conn),
            Err(err) if retryable_franken_error(&err) && Instant::now() < deadline => {
                sleep_with_franken_retry_backoff(
                    &mut backoff,
                    deadline.saturating_duration_since(Instant::now()),
                    Duration::from_millis(128),
                );
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!(
                        "opening migrated index archive schema at {}",
                        path.display()
                    )
                });
            }
        }
    }
}

/// Writer handles on an existing archive take the same bounded lane as the
/// index open. An ordinary handle's first autocommit point lookup (for example
/// `SELECT value FROM meta WHERE key = ?1`) hydrates every row of every table
/// into the compatibility MemDatabase: on a 16 GB archive the legacy OMP
/// analytics writer passed a 24 GB memory limit that way before its first
/// chunk (xcqqa). An archive whose engine migration is still pending gets the
/// ordinary constructor so the engine's first-open repair runs, then the
/// bounded lane once the repair is recorded.
///
/// The bounded lane can fail to open while peers commit: fsqlite's
/// `open_existing_schema_only` returns `BusyRecovery` ("recovery in progress")
/// or `Busy` when it meets a peer's WAL-index recovery, where the ordinary
/// constructor waits it out. With 4 threads each opening a writer per commit,
/// 2-9 of 400 schema-only opens failed per round on fsqlite 0.4.4 and 0 of
/// 2,000 ordinary opens did (same worker); cass's
/// frankensqlite_concurrent_stress hit it in `concurrent_writer()`. Opening is
/// idempotent, so both opens retry busy-class errors, with a budget long
/// enough to outlast a loaded host (a 5 s budget was exhausted there).
pub(super) fn open_archive_writer_connection(path: &Path) -> Result<FrankenConnection> {
    const OPEN_RETRY_BUDGET: Duration = Duration::from_secs(30);
    let path_str = path.to_string_lossy().to_string();
    if !index_engine_migration_is_complete(path) {
        let repairing =
            retry_transient_storage_op_within("open_archive_writer", OPEN_RETRY_BUDGET, || {
                FrankenConnection::open(path_str.clone()).map_err(anyhow::Error::new)
            })?;
        if !index_engine_migration_is_complete(path) {
            return Ok(repairing);
        }
        close_first_open_repair_handle(repairing, path);
    }
    retry_transient_storage_op_within("open_archive_writer_schema_only", OPEN_RETRY_BUDGET, || {
        FrankenConnection::open_existing_schema_only(path_str.clone()).map_err(anyhow::Error::new)
    })
}

/// Close the ordinary handle whose open ran the engine's first-open repair.
/// Keeping it for the rest of a run would let its first autocommit point lookup
/// hydrate the whole archive (xcqqa); callers reopen in the schema-only lane.
fn close_first_open_repair_handle(mut conn: FrankenConnection, path: &Path) {
    if let Err(err) = conn.close_without_checkpoint_in_place() {
        tracing::debug!(
            error = %err,
            db_path = %path.display(),
            "closing the first-open repair handle failed; falling back to best-effort close"
        );
        conn.close_best_effort_in_place();
    }
}

pub(crate) fn index_engine_migration_is_complete(path: &Path) -> bool {
    // fsqlite-core 0.3.18 migration::MigrationMarker and
    // CURRENT_MIGRATION_VERSION. build.rs enforces this exact engine family;
    // review this admission when changing the pin. The engine's marker is
    // pathname-scoped, not an integrity attestation or content fingerprint.
    // Missing/old/malformed markers must still reach its repair constructor.
    #[derive(Deserialize)]
    struct EngineMigrationMarker {
        last_upgrade_version: u32,
        last_run_at: u64,
        repairs_applied: Vec<String>,
    }
    let mut marker_path = path.as_os_str().to_os_string();
    marker_path.push(".fsqlite-migration-state");
    let Ok(file) = fs::File::open(Path::new(&marker_path)) else {
        return false;
    };
    let Ok(marker) =
        serde_json::from_reader::<_, EngineMigrationMarker>(std::io::Read::take(file, 64 * 1024))
    else {
        return false;
    };
    // Deserialize the entire engine contract, including fields not used by
    // the decision, rather than accepting a truncated version-only object.
    let _ = (marker.last_run_at, marker.repairs_applied);
    marker.last_upgrade_version >= 1
}

pub(crate) fn open_franken_readonly_storage_with_timeout(
    path: &Path,
    timeout: Duration,
) -> Result<FrankenStorage> {
    if !path.exists() {
        return Err(anyhow!("Database not found at {}", path.display()));
    }

    let deadline = Instant::now() + timeout;
    let mut backoff = Duration::from_millis(4);
    loop {
        match FrankenStorage::open_readonly(path) {
            Ok(storage) => return Ok(storage),
            Err(err) if retryable_franken_anyhow(&err) => {
                let now = Instant::now();
                if now >= deadline {
                    return Err(err);
                }
                let remaining = deadline.saturating_duration_since(now);
                sleep_with_franken_retry_backoff(
                    &mut backoff,
                    remaining,
                    Duration::from_millis(128),
                );
            }
            Err(err) => return Err(err),
        }
    }
}

pub(crate) fn open_franken_raw_connection_with_timeout(
    path: &Path,
    timeout: Duration,
) -> Result<FrankenConnection> {
    if !path.exists() {
        return Err(anyhow!("Database not found at {}", path.display()));
    }

    let path_str = path.to_string_lossy().to_string();
    let deadline = Instant::now() + timeout;
    let mut backoff = Duration::from_millis(4);
    loop {
        let _doctor_guard = acquire_doctor_mutation_db_open_guard(path, timeout)?;
        match FrankenConnection::open(&path_str)
            .with_context(|| format!("opening raw frankensqlite db at {}", path.display()))
        {
            Ok(conn) => return Ok(conn),
            Err(err) if retryable_franken_anyhow(&err) => {
                let now = Instant::now();
                if now >= deadline {
                    return Err(err);
                }
                let remaining = deadline.saturating_duration_since(now);
                sleep_with_franken_retry_backoff(
                    &mut backoff,
                    remaining,
                    Duration::from_millis(128),
                );
            }
            Err(err) => return Err(err),
        }
    }
}

pub(crate) fn open_franken_raw_readonly_connection_with_timeout(
    path: &Path,
    timeout: Duration,
) -> Result<FrankenConnection> {
    if !path.exists() {
        return Err(anyhow!("Database not found at {}", path.display()));
    }

    let path_str = path.to_string_lossy().to_string();
    let deadline = Instant::now() + timeout;
    let mut backoff = Duration::from_millis(4);
    let mut wal_recovery_attempted = false;
    loop {
        let _doctor_guard = acquire_doctor_mutation_db_open_guard(path, timeout)?;
        match FrankenConnection::open_schema_only_with_wal_index_recovery(&path_str).with_context(
            || {
                format!(
                    "opening raw frankensqlite db readonly at {}",
                    path.display()
                )
            },
        ) {
            Ok(conn) => return Ok(conn),
            Err(err) if retryable_franken_anyhow(&err) => {
                let now = Instant::now();
                if now >= deadline {
                    return Err(err);
                }
                let remaining = deadline.saturating_duration_since(now);
                sleep_with_franken_retry_backoff(
                    &mut backoff,
                    remaining,
                    Duration::from_millis(128),
                );
            }
            // gh #389: hard readonly failure + non-empty `-wal` sidecar is
            // the unclean-shutdown dirty-WAL shape; checkpoint it through the
            // write path (under the doctor guard held above) and retry the
            // readonly open exactly once.
            Err(err)
                if !wal_recovery_attempted && attempt_dirty_wal_recovery_checkpoint(path, &err) =>
            {
                wal_recovery_attempted = true;
            }
            Err(err) => return Err(err),
        }
    }
}

/// Open a read-only FrankenSQLite connection whose raw engine remains on one
/// dedicated owner thread for its entire lifetime.
///
/// This mirrors [`open_franken_raw_readonly_connection_with_timeout`]'s doctor
/// lock, explicit WAL-index repair, bounded contention retry, and one-shot
/// dirty-WAL recovery contract,
/// and retains the canonical storage opener's duplicate-FTS-schema repair. It
/// returns the thread-safe dispatch handle required by shared search clients.
/// The worker creates, uses, closes, and drops the `!Send` raw connection on
/// that same worker thread.
pub(crate) fn open_franken_async_readonly_connection_with_timeout(
    path: &Path,
    timeout: Duration,
) -> Result<FrankenAsyncConnection> {
    if !path.exists() {
        return Err(anyhow!("Database not found at {}", path.display()));
    }

    let path_str = path.to_string_lossy().to_string();
    let deadline = Instant::now() + timeout;
    let mut backoff = Duration::from_millis(4);
    let mut wal_recovery_attempted = false;
    let mut duplicate_fts_repair_attempted = false;
    loop {
        let _doctor_guard = acquire_doctor_mutation_db_open_guard(path, timeout)?;
        match FrankenAsyncConnection::open_schema_only_with_wal_index_recovery_sync(&path_str)
            .with_context(|| {
                format!(
                    "opening dedicated-owner frankensqlite db readonly at {}",
                    path.display()
                )
            }) {
            Ok(conn) => return Ok(conn),
            Err(err) if retryable_franken_anyhow(&err) => {
                let now = Instant::now();
                if now >= deadline {
                    return Err(err);
                }
                let remaining = deadline.saturating_duration_since(now);
                sleep_with_franken_retry_backoff(
                    &mut backoff,
                    remaining,
                    Duration::from_millis(128),
                );
            }
            Err(err)
                if !duplicate_fts_repair_attempted
                    && format!("{err:#}").contains("conflicting virtual-table entries") =>
            {
                tracing::warn!(
                    db = %path.display(),
                    error = %err,
                    "dedicated-owner readonly open found duplicate fts_messages schema rows; deduplicating through the sanctioned sqlite3 bridge"
                );
                dedupe_conflicting_fts_schema_rows_via_sqlite3(path)?;
                duplicate_fts_repair_attempted = true;
            }
            Err(err)
                if !wal_recovery_attempted && attempt_dirty_wal_recovery_checkpoint(path, &err) =>
            {
                wal_recovery_attempted = true;
            }
            Err(err) => return Err(err),
        }
    }
}

/// Open a dedicated-owner read connection without performing or preparing any
/// repair. Unlike the ordinary read opener, this path never creates the doctor
/// lock hierarchy, repairs the WAL index, deduplicates schema rows, or
/// checkpoints a dirty WAL. It is
/// the storage seam for `search --no-maintenance`: an archive that requires
/// recovery is reported as unavailable instead of being changed by a read.
pub(crate) fn open_franken_async_strict_readonly_connection_with_timeout(
    path: &Path,
    timeout: Duration,
) -> Result<FrankenAsyncConnection> {
    if !path.exists() {
        return Err(anyhow!("Database not found at {}", path.display()));
    }

    let path_str = path.to_string_lossy().to_string();
    let deadline = Instant::now() + timeout;
    let mut backoff = Duration::from_millis(4);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let _doctor_guard = acquire_existing_doctor_mutation_db_open_guard(path, remaining)?;
        match FrankenAsyncConnection::open_with_flags_sync(
            &path_str,
            FrankenOpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .with_context(|| {
            format!(
                "strictly opening dedicated-owner frankensqlite db readonly at {}",
                path.display()
            )
        }) {
            Ok(conn) => return Ok(conn),
            Err(err) if retryable_franken_anyhow(&err) => {
                let now = Instant::now();
                if now >= deadline {
                    return Err(err);
                }
                let remaining = deadline.saturating_duration_since(now);
                sleep_with_franken_retry_backoff(
                    &mut backoff,
                    remaining,
                    Duration::from_millis(128),
                );
            }
            Err(err) => return Err(err),
        }
    }
}

/// Open the synchronous dedicated-owner facade used by non-async callers
/// that must safely transfer a read handle between worker threads.
pub(crate) fn open_franken_owner_readonly_connection_with_timeout(
    path: &Path,
    timeout: Duration,
) -> Result<FrankenOwnerConnection> {
    open_franken_async_readonly_connection_with_timeout(path, timeout).map(FrankenOwnerConnection)
}

/// Dedicated-owner facade over the strict (mutation-free) readonly opener:
/// no doctor-lock creation, no schema-row dedupe, no dirty-WAL checkpoint.
/// Readiness reads (`cass status`, k2k20) use this so a truth surface never
/// changes the archive it is describing.
pub(crate) fn open_franken_owner_strict_readonly_connection_with_timeout(
    path: &Path,
    timeout: Duration,
) -> Result<FrankenOwnerConnection> {
    open_franken_async_strict_readonly_connection_with_timeout(path, timeout)
        .map(FrankenOwnerConnection)
}

pub(crate) fn retryable_franken_error(err: &crate::franken_sync::FrankenError) -> bool {
    matches!(
        err,
        crate::franken_sync::FrankenError::Busy
            | crate::franken_sync::FrankenError::BusyRecovery
            | crate::franken_sync::FrankenError::BusySnapshot { .. }
            | crate::franken_sync::FrankenError::DatabaseLocked { .. }
            | crate::franken_sync::FrankenError::LockFailed { .. }
            | crate::franken_sync::FrankenError::WriteConflict { .. }
            | crate::franken_sync::FrankenError::SerializationFailure { .. }
    ) || retryable_storage_error_message(&err.to_string())
}

pub(crate) fn retryable_storage_error_message(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("busy")
        || lower.contains("locked")
        || lower.contains("locking")
        || lower.contains("contention")
        || lower.contains("temporarily unavailable")
        || lower.contains("would block")
}

pub(crate) fn retryable_franken_anyhow(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<crate::franken_sync::FrankenError>()
            .is_some_and(retryable_franken_error)
            || retryable_storage_error_message(&cause.to_string())
    })
}

/// fsqlite 0.3.x defers dropped-session reclamation, so the first write
/// stages of a fresh open (migrations / schema repair) can transiently hit
/// `BusySnapshot` while a just-closed peer connection's epoch unwinds
/// (observed in the v0.6.25 release gate on legacy-fixture reopen tests).
/// Both stages are idempotent — the migration ledger skips already-applied
/// steps and repair re-checks object presence — so a bounded, backoff-paced
/// retry is safe and mirrors upstream's own prepare-prologue retry posture.
pub(crate) fn retry_transient_storage_op<T>(
    stage: &'static str,
    attempt: impl FnMut() -> Result<T>,
) -> Result<T> {
    retry_transient_storage_op_within(stage, Duration::from_secs(5), attempt)
}

/// [`retry_transient_storage_op`] with an explicit retry budget.
pub(crate) fn retry_transient_storage_op_within<T>(
    stage: &'static str,
    budget: Duration,
    mut attempt: impl FnMut() -> Result<T>,
) -> Result<T> {
    let deadline = Instant::now() + budget;
    let mut delay = Duration::from_millis(25);
    loop {
        match attempt() {
            Ok(value) => return Ok(value),
            Err(err) if Instant::now() < deadline && retryable_franken_anyhow(&err) => {
                tracing::debug!(
                    stage,
                    error = %err,
                    "transient contention during open stage; retrying"
                );
                std::thread::sleep(delay);
                delay = (delay * 2).min(Duration::from_millis(400));
            }
            Err(err) => return Err(err),
        }
    }
}

/// gh #389: recover a dirty WAL before surfacing a hard readonly-open failure.
///
/// A readonly-first open of a WAL-mode archive whose WAL still holds
/// unreplayed frames (the normal artifact of an unclean host shutdown) fails
/// with a CANTOPEN-class error, because a readonly connection may not write
/// the wal-index it needs to recover the WAL. Every cass surface probes
/// readonly-first, so one unclean shutdown used to wedge the whole tool and
/// route doctor toward a multi-hour archive reconstruction — while the actual
/// repair is the same read-write open + TRUNCATE checkpoint that
/// `run_final_wal_checkpoint` performs on every clean indexer shutdown.
///
/// This helper attempts that checkpoint exactly once, and only when it is the
/// safe interpretation of the failure:
/// - never on a retryable busy/lock-class error — that is a live writer doing
///   real work, not a dirty WAL (the caller's bounded retry loop owns that
///   case);
/// - only when a non-empty regular-file `-wal` sidecar is actually present.
///
/// Returns `true` when a checkpoint ran to completion, in which case the
/// caller should retry its readonly open once. A failed recovery attempt is
/// logged and returns `false`; the caller then surfaces the ORIGINAL open
/// error, so recovery can never mask the underlying failure.
pub(crate) fn attempt_dirty_wal_recovery_checkpoint(
    db_path: &Path,
    original_err: &anyhow::Error,
) -> bool {
    if retryable_franken_anyhow(original_err) {
        return false;
    }
    // Never let the recovery write-open CREATE a database: a stray `-wal`
    // next to a missing main file must surface the original open failure.
    if !db_path.is_file() {
        return false;
    }
    let wal_path = database_sidecar_path(db_path, "-wal");
    let Ok(meta) = std::fs::symlink_metadata(&wal_path) else {
        return false;
    };
    // A WAL of 32 bytes or fewer is header-only (or empty): no frames to
    // recover, so a checkpoint cannot explain or fix the open failure.
    if !meta.file_type().is_file() || meta.len() <= 32 {
        return false;
    }
    let conn = match crate::franken_sync::Connection::open(db_path.to_string_lossy().into_owned()) {
        Ok(conn) => conn,
        Err(err) => {
            tracing::warn!(
                db = %db_path.display(),
                wal_bytes = meta.len(),
                error = %err,
                "dirty-WAL recovery: read-write open failed; surfacing the original readonly failure"
            );
            return false;
        }
    };
    let checkpoint = conn.query("PRAGMA wal_checkpoint(TRUNCATE);");
    let close_result = conn.close();
    match (&checkpoint, &close_result) {
        // Local fix (review 2026-11-02, S1): a blocked checkpoint returns
        // Ok with busy=1 or unbackfilled frames; only a complete status row
        // counts as recovery.
        (Ok(rows), Ok(())) if wal_checkpoint_status::truncate_rows_report_complete(rows) => {
            tracing::info!(
                db = %db_path.display(),
                wal_bytes = meta.len(),
                "dirty-WAL recovery: checkpoint(TRUNCATE) completed after failed readonly open; retrying readonly open"
            );
            true
        }
        (Ok(_), Ok(())) => {
            tracing::warn!(
                db = %db_path.display(),
                wal_bytes = meta.len(),
                "dirty-WAL recovery: checkpoint(TRUNCATE) was blocked or left frames; surfacing the original readonly failure"
            );
            false
        }
        _ => {
            if let Err(err) = &checkpoint {
                tracing::warn!(
                    db = %db_path.display(),
                    error = %err,
                    "dirty-WAL recovery: checkpoint failed; surfacing the original readonly failure"
                );
            }
            if let Err(err) = &close_result {
                tracing::warn!(
                    db = %db_path.display(),
                    error = %err,
                    "dirty-WAL recovery: close failed after checkpoint attempt"
                );
            }
            false
        }
    }
}

#[cfg(test)]
mod doctor_mutation_lock_error_tests {
    use super::*;

    /// fs2's own contended error must read as a held doctor lock on every
    /// platform (on Windows it is raw ERROR_LOCK_VIOLATION, not WouldBlock),
    /// and unrelated failures must not (2l1b0.74).
    #[test]
    fn contended_lock_error_is_active_and_other_errors_are_not() {
        assert!(doctor_mutation_lock_error_is_active(
            &fs2::lock_contended_error()
        ));
        assert!(!doctor_mutation_lock_error_is_active(
            &std::io::Error::from(std::io::ErrorKind::NotFound)
        ));
        assert!(!doctor_mutation_lock_error_is_active(
            &std::io::Error::other("permission denied")
        ));
    }
}
