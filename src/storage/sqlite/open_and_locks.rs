use super::connection_pool::FrankenOwnerConnection;
use super::omp_and_semantic_meta::{CURRENT_SCHEMA_VERSION, MIN_IN_PLACE_MIGRATION_SCHEMA_VERSION};
use super::open::{ArchiveDisposition, archive_disposition, archive_disposition_with_doctor_guard};
use super::storage_core::FrankenStorage;
use crate::franken_sync::{
    Connection as FrankenConnection, FrankenError,
    compat::{ConnectionExt as FrankenConnectionExt, OpenFlags as FrankenOpenFlags, RowExt},
};
use anyhow::{Context, Result, anyhow, bail};
use serde::Serialize;
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
/// when many threads hit the same transient SQLite contention.
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

fn apply_raw_writer_pragmas(conn: &FrankenConnection) -> Result<()> {
    conn.execute("PRAGMA busy_timeout = 5000;")
        .with_context(|| "setting busy_timeout")?;
    conn.execute("PRAGMA foreign_keys = ON;")
        .with_context(|| "setting foreign_keys")?;
    let _ = conn.execute("PRAGMA temp_store = MEMORY;");
    Ok(())
}

fn apply_raw_readonly_pragmas(conn: &FrankenConnection, cache_kib: i64) -> Result<()> {
    conn.execute("PRAGMA query_only = 1;")
        .with_context(|| "setting query_only")?;
    conn.execute("PRAGMA busy_timeout = 5000;")
        .with_context(|| "setting busy_timeout")?;
    conn.execute("PRAGMA foreign_keys = ON;")
        .with_context(|| "setting foreign_keys")?;
    let _ = conn.execute("PRAGMA temp_store = MEMORY;");
    conn.execute(&format!("PRAGMA cache_size = {cache_kib};"))
        .with_context(|| "setting cache_size")?;
    Ok(())
}

fn apply_async_readonly_pragmas(conn: &FrankenOwnerConnection, cache_kib: i64) -> Result<()> {
    conn.execute_sync("PRAGMA query_only = 1;")
        .with_context(|| "setting query_only")?;
    conn.execute_sync("PRAGMA busy_timeout = 5000;")
        .with_context(|| "setting busy_timeout")?;
    conn.execute_sync("PRAGMA foreign_keys = ON;")
        .with_context(|| "setting foreign_keys")?;
    let _ = conn.execute_sync("PRAGMA temp_store = MEMORY;");
    conn.execute_sync(&format!("PRAGMA cache_size = {cache_kib};"))
        .with_context(|| "setting cache_size")?;
    Ok(())
}

pub(crate) fn open_franken_storage_with_timeout(
    path: &Path,
    timeout: Duration,
) -> Result<FrankenStorage> {
    {
        let _doctor_guard = acquire_doctor_mutation_db_open_guard(path, timeout)?;
        if matches!(
            archive_disposition_with_doctor_guard(path, &_doctor_guard)?,
            ArchiveDisposition::Uninitialized
        ) {
            return Err(anyhow!("Database not found at {}", path.display()));
        }
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
    let _doctor_guard = acquire_doctor_mutation_db_open_guard(path, timeout)?;
    // Legacy/foreign/uninitialized archives are not "current schema" — report
    // them as absent rather than opening or mutating anything.
    if !matches!(
        archive_disposition_with_doctor_guard(path, &_doctor_guard),
        Ok(ArchiveDisposition::Current)
    ) {
        return Ok(None);
    }

    let storage = FrankenStorage::new(
        open_index_schema_connection_with_timeout_and_guard(path, timeout, &_doctor_guard)?,
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
        let mut storage = storage;
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

    storage.apply_config()?;
    Ok(Some(storage))
}

/// Bounded retry lane for opening the canonical archive's schema on the
/// bundled-rusqlite engine. The archive stamp is verified first under the
/// doctor guard: a legacy or foreign archive refuses before the engine ever
/// touches the file.
fn open_index_schema_connection_with_timeout_and_guard(
    path: &Path,
    timeout: Duration,
    doctor_guard: &DoctorMutationDbOpenGuard,
) -> Result<FrankenConnection> {
    match archive_disposition_with_doctor_guard(path, doctor_guard)? {
        ArchiveDisposition::Current => {}
        ArchiveDisposition::Uninitialized => {
            bail!(
                "cass index archive is not initialized at {}",
                path.display()
            );
        }
    }
    let deadline = Instant::now() + timeout;
    let mut backoff = Duration::from_millis(4);
    loop {
        match FrankenConnection::open_existing_schema_only(path.to_string_lossy()) {
            Ok(conn) => {
                apply_raw_writer_pragmas(&conn)?;
                return Ok(conn);
            }
            Err(err) if retryable_franken_error(&err) && Instant::now() < deadline => {
                sleep_with_franken_retry_backoff(
                    &mut backoff,
                    deadline.saturating_duration_since(Instant::now()),
                    Duration::from_millis(128),
                );
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("opening index archive schema at {}", path.display())
                });
            }
        }
    }
}

/// Writer handles on an existing archive take the same bounded lane as the
/// index open. rusqlite `open_existing_schema_only` never creates or
/// zero-initializes the path; the archive stamp is verified first under the
/// held doctor guard so a legacy archive refuses instead of being opened for
/// writes.
pub(super) fn open_archive_writer_connection_with_guard(
    path: &Path,
    doctor_guard: &DoctorMutationDbOpenGuard,
) -> Result<FrankenConnection> {
    const OPEN_RETRY_BUDGET: Duration = Duration::from_secs(30);
    let path_str = path.to_string_lossy().to_string();
    match archive_disposition_with_doctor_guard(path, doctor_guard)? {
        ArchiveDisposition::Current => {}
        ArchiveDisposition::Uninitialized => {
            bail!(
                "cass index archive is not initialized at {}",
                path.display()
            );
        }
    }
    let conn = retry_transient_storage_op_within(
        "open_archive_writer_schema_only",
        OPEN_RETRY_BUDGET,
        || {
            FrankenConnection::open_existing_schema_only(path_str.clone())
                .map_err(anyhow::Error::new)
        },
    )?;
    apply_raw_writer_pragmas(&conn)?;
    Ok(conn)
}

/// Whether the archive already carries the engine's completed schema
/// identity. Under bundled rusqlite there is no engine-migration marker file;
/// "migration complete" means the archive is stamped `(0x43415353, 1)`.
pub(crate) fn index_engine_migration_is_complete(path: &Path) -> bool {
    matches!(archive_disposition(path), Ok(ArchiveDisposition::Current))
}

pub(crate) fn open_franken_readonly_storage_with_timeout(
    path: &Path,
    timeout: Duration,
) -> Result<FrankenStorage> {
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
    let path_str = path.to_string_lossy().to_string();
    let deadline = Instant::now() + timeout;
    let mut backoff = Duration::from_millis(4);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let _doctor_guard = acquire_doctor_mutation_db_open_guard(path, remaining)?;
        // A read/write open must never touch a legacy or foreign archive:
        // `SchemaRebuildRequired` refuses those; `Uninitialized` fails here.
        match archive_disposition_with_doctor_guard(path, &_doctor_guard)? {
            ArchiveDisposition::Current => {}
            ArchiveDisposition::Uninitialized => {
                return Err(anyhow!("Database not found at {}", path.display()));
            }
        }
        match FrankenConnection::open(&path_str)
            .map_err(anyhow::Error::new)
            .and_then(|conn| {
                apply_raw_writer_pragmas(&conn)?;
                Ok(conn)
            })
            .with_context(|| format!("opening raw sqlite db at {}", path.display()))
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
    let path_str = path.to_string_lossy().to_string();
    let deadline = Instant::now() + timeout;
    let mut backoff = Duration::from_millis(4);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let _doctor_guard = acquire_doctor_mutation_db_open_guard(path, remaining)?;
        match archive_disposition_with_doctor_guard(path, &_doctor_guard)? {
            ArchiveDisposition::Current => {}
            ArchiveDisposition::Uninitialized => {
                return Err(anyhow!("Database not found at {}", path.display()));
            }
        }
        match FrankenConnection::open_schema_only_with_wal_index_recovery(&path_str)
            .map_err(anyhow::Error::new)
            .and_then(|conn| {
                apply_raw_readonly_pragmas(&conn, -65536)?;
                Ok(conn)
            })
            .with_context(|| format!("opening raw sqlite db readonly at {}", path.display()))
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

/// Open a read-only connection on the canonical archive for shared search
/// clients.
///
/// Mirrors [`open_franken_raw_readonly_connection_with_timeout`]'s doctor
/// lock, bounded contention retry, and archive-stamp refusal, returning the
/// `Send + Sync` dispatch facade expected by shared search callers.
pub(crate) fn open_franken_async_readonly_connection_with_timeout(
    path: &Path,
    timeout: Duration,
) -> Result<FrankenOwnerConnection> {
    let path_str = path.to_string_lossy().to_string();
    let deadline = Instant::now() + timeout;
    let mut backoff = Duration::from_millis(4);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let _doctor_guard = acquire_doctor_mutation_db_open_guard(path, remaining)?;
        match archive_disposition_with_doctor_guard(path, &_doctor_guard)? {
            ArchiveDisposition::Current => {}
            ArchiveDisposition::Uninitialized => {
                return Err(anyhow!("Database not found at {}", path.display()));
            }
        }
        match FrankenOwnerConnection::open_schema_only_with_wal_index_recovery_sync(&path_str)
            .map_err(anyhow::Error::new)
            .and_then(|conn| {
                apply_async_readonly_pragmas(&conn, -65536)?;
                Ok(conn)
            })
            .with_context(|| format!("opening shared sqlite db readonly at {}", path.display()))
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

/// Open a strict read-only connection without performing or preparing any
/// mutation. This path never creates the doctor lock hierarchy, repairs the
/// WAL index, or checkpoints a dirty WAL. It is the storage seam for
/// `search --no-maintenance`: an archive that requires recovery is reported
/// as unavailable instead of being changed by a read.
pub(crate) fn open_franken_async_strict_readonly_connection_with_timeout(
    path: &Path,
    timeout: Duration,
) -> Result<FrankenOwnerConnection> {
    let path_str = path.to_string_lossy().to_string();
    let deadline = Instant::now() + timeout;
    let mut backoff = Duration::from_millis(4);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let _doctor_guard = acquire_existing_doctor_mutation_db_open_guard(path, remaining)?;
        match archive_disposition_with_doctor_guard(path, &_doctor_guard)? {
            ArchiveDisposition::Current => {}
            ArchiveDisposition::Uninitialized => {
                return Err(anyhow!("Database not found at {}", path.display()));
            }
        }
        match FrankenOwnerConnection::open_with_flags_sync(
            &path_str,
            FrankenOpenFlags::SQLITE_OPEN_STRICT_READ_ONLY,
        ) {
            Ok(conn) => {
                apply_async_readonly_pragmas(&conn, -65536)?;
                return Ok(conn);
            }
            Err(err @ FrankenError::BusyRecovery) => {
                // BusyRecovery on strict read-only open is deterministic; do
                // not spin for the full 30 s timeout.
                return Err(anyhow::Error::new(err).context(format!(
                    "strictly opening sqlite db readonly at {}",
                    path.display()
                )));
            }
            Err(err) if retryable_franken_error(&err) => {
                let now = Instant::now();
                if now >= deadline {
                    return Err(anyhow::Error::new(err).context(format!(
                        "strictly opening sqlite db readonly at {}",
                        path.display()
                    )));
                }
                let remaining = deadline.saturating_duration_since(now);
                sleep_with_franken_retry_backoff(
                    &mut backoff,
                    remaining,
                    Duration::from_millis(128),
                );
            }
            Err(err) => {
                return Err(anyhow::Error::new(err).context(format!(
                    "strictly opening sqlite db readonly at {}",
                    path.display()
                )));
            }
        }
    }
}

/// Open the synchronous dedicated-owner facade used by non-async callers
/// that must safely transfer a read handle between worker threads.
pub(crate) fn open_franken_owner_readonly_connection_with_timeout(
    path: &Path,
    timeout: Duration,
) -> Result<FrankenOwnerConnection> {
    open_franken_async_readonly_connection_with_timeout(path, timeout)
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
}

pub(crate) fn retryable_franken_error(err: &crate::franken_sync::FrankenError) -> bool {
    matches!(
        err,
        crate::franken_sync::FrankenError::Busy
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

/// Transient `Busy`/`BusySnapshot` contention can occur right after a peer
/// connection closes on a freshly opened archive; bounded backoff retry keeps
/// idempotent open-stage operations resilient.
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

/// Result of checking schema compatibility.
#[derive(Debug, Clone)]
pub enum SchemaCheck {
    /// Schema is up to date, no migration needed.
    Compatible,
    /// Schema needs migration but can be done incrementally.
    NeedsMigration,
    /// Schema is incompatible and needs a full rebuild (with reason).
    NeedsRebuild(String),
}

pub(super) fn schema_check_error_requires_rebuild(err: &crate::franken_sync::FrankenError) -> bool {
    // Only on-disk corruption classes justify destructive rebuild.
    // Locking, open, and generic I/O failures are often transient and must
    // surface as errors rather than deleting the database under the caller.
    matches!(
        err,
        crate::franken_sync::FrankenError::DatabaseCorrupt { .. }
            | crate::franken_sync::FrankenError::WalCorrupt { .. }
            | crate::franken_sync::FrankenError::NotADatabase { .. }
            | crate::franken_sync::FrankenError::ShortRead { .. }
    )
}

/// Check schema compatibility without modifying the database.
///
/// Opens the database read-only and checks the schema version.
pub(super) fn check_schema_compatibility(
    path: &Path,
) -> std::result::Result<SchemaCheck, crate::franken_sync::FrankenError> {
    let mut conn =
        FrankenConnection::open_schema_only_with_wal_index_recovery(path.to_string_lossy())?;

    let result = (|| {
        // Check if meta table exists
        let meta_exists: i32 = conn.query_row_map(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='meta'",
            fparams![],
            |row| row.get_typed(0),
        )?;

        if meta_exists == 0 {
            // No meta table - could be empty or very old schema, needs rebuild
            // But first check if there are any tables at all
            let table_count: i32 = conn.query_row_map(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'",
                fparams![],
                |row| row.get_typed(0),
            )?;

            if table_count == 0 {
                // Empty database, will be initialized fresh
                return Ok(SchemaCheck::NeedsMigration);
            }

            // Has tables but no meta - very old or corrupted
            return Ok(SchemaCheck::NeedsRebuild(
                "Database missing schema version metadata".to_string(),
            ));
        }

        // Get the schema version
        let version: Option<i64> = conn
            .query_row_map(
                "SELECT value FROM meta WHERE key = 'schema_version'",
                fparams![],
                |row| Ok(row.get_typed::<String>(0)?.parse().ok()),
            )
            .ok()
            .flatten();

        match version {
            Some(v) if v == CURRENT_SCHEMA_VERSION => Ok(SchemaCheck::Compatible),
            Some(v)
                if (MIN_IN_PLACE_MIGRATION_SCHEMA_VERSION..CURRENT_SCHEMA_VERSION).contains(&v) =>
            {
                Ok(SchemaCheck::NeedsMigration)
            }
            Some(v) if v > 0 && v < MIN_IN_PLACE_MIGRATION_SCHEMA_VERSION => {
                Ok(SchemaCheck::NeedsRebuild(format!(
                    "Schema version {} is too old for in-place migration; supported upgrade path starts at version {}",
                    v, MIN_IN_PLACE_MIGRATION_SCHEMA_VERSION
                )))
            }
            Some(v) => {
                // v > CURRENT_SCHEMA_VERSION - database is from a newer version
                Ok(SchemaCheck::NeedsRebuild(format!(
                    "Schema version {} is newer than supported version {}",
                    v, CURRENT_SCHEMA_VERSION
                )))
            }
            None => Ok(SchemaCheck::NeedsRebuild(
                "Schema version not found or invalid".to_string(),
            )),
        }
    })();

    if let Err(close_err) = conn.close_without_checkpoint_in_place() {
        tracing::warn!(
            error = %close_err,
            db_path = %path.display(),
            "check_schema_compatibility: close_without_checkpoint_in_place failed; falling back to best-effort close"
        );
        conn.close_best_effort_in_place();
    }

    result
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

const CANONICAL_DB_CORRUPTION_PROBE_MAX_BYTES: u64 = 256 * 1024 * 1024;
const CANONICAL_DB_CORRUPTION_PROBE_OPEN_TIMEOUT: Duration = Duration::from_secs(5);
const CANONICAL_DB_CORRUPTION_PROBE_QUERY_RETRY_TIMEOUT: Duration = Duration::from_secs(10);

fn sqlite_sidecar_path(db_path: &Path, suffix: &str) -> Option<PathBuf> {
    let mut file_name = db_path.file_name()?.to_os_string();
    file_name.push(suffix);
    Some(db_path.with_file_name(file_name))
}

fn corruption_probe_query_row_map_retry<T, F>(
    conn: &FrankenConnection,
    sql: &str,
    params: &[crate::franken_sync::compat::ParamValue],
    map: F,
) -> std::result::Result<T, FrankenError>
where
    F: Copy + Fn(&crate::franken_sync::Row) -> std::result::Result<T, FrankenError>,
{
    let deadline = Instant::now() + CANONICAL_DB_CORRUPTION_PROBE_QUERY_RETRY_TIMEOUT;
    let mut backoff = Duration::from_millis(4);
    // negs9: a failure raised by the row-mapping callback is NOT engine
    // contention, even when its text resembles one ("busy"/"locked"), and
    // must never cause the query — and the callback — to be replayed.
    let callback_failed = std::cell::Cell::new(false);
    loop {
        callback_failed.set(false);
        let mapped = conn.query_row_map(sql, params, |row| {
            map(row).inspect_err(|_| callback_failed.set(true))
        });
        match mapped {
            Ok(value) => return Ok(value),
            Err(err) if callback_failed.get() => return Err(err),
            Err(err) if retryable_franken_error(&err) => {
                let now = Instant::now();
                if now >= deadline {
                    return Err(err);
                }
                let remaining = deadline.saturating_duration_since(now);
                sleep_with_franken_retry_backoff(
                    &mut backoff,
                    remaining,
                    Duration::from_millis(64),
                );
            }
            Err(err) => return Err(err),
        }
    }
}

/// i6upe: verdict of the bounded real-column integrity probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StateDbIntegrity {
    Ok,
    Corrupt,
}

/// True for FrankenError classes that mean the archive's stored pages are
/// malformed — never for busy/locked/transient classes.
pub(crate) fn corruption_class_franken_error(err: &FrankenError) -> bool {
    if matches!(
        err,
        FrankenError::DatabaseCorrupt { .. }
            | FrankenError::NotADatabase { .. }
            | FrankenError::WalCorrupt { .. }
    ) {
        return true;
    }
    let lower = err.to_string().to_ascii_lowercase();
    lower.contains("malformed") || lower.contains("disk image")
}

/// i6upe: real-column corruption probe used only below a physical archive
/// bundle-size ceiling.
///
/// `SELECT COUNT(*)` is answered from index B-trees without touching table
/// pages, so a DB whose table pages are malformed counts perfectly and only
/// fails when real columns are read (proven on ts2/csd 2026-07-26, where
/// counts matched while every projection failed). Each probe here projects
/// real columns from a single row at both ends of the two hot tables. The SQL
/// shape is logically bounded, but GH #413 proved that a query may still read
/// or hydrate a large fraction of a huge archive while executing it. Callers
/// must therefore enforce a physical main+sidecar byte ceiling before using
/// this probe.
///
/// Returns `(Some(Ok), None)` when every probe reads (or the table is
/// empty), `(Some(Corrupt), Some(detail))` on a corruption-class error, and
/// `(None, None)` when a transient error prevents classification — never a
/// false-positive `Corrupt` from a busy archive.
///
/// `run_probe` executes one probe statement and reads its first column, so
/// the classification is shared by the ordinary-connection lane and the
/// dedicated-owner strict lane (`probe_state_db_strict_bounded`).
pub(crate) fn probe_state_db_integrity(
    run_probe: impl Fn(&str) -> std::result::Result<(), FrankenError>,
) -> (Option<StateDbIntegrity>, Option<String>) {
    const PROBES: [&str; 4] = [
        "SELECT id, title, source_path FROM conversations ORDER BY id LIMIT 1",
        "SELECT id, title, source_path FROM conversations ORDER BY id DESC LIMIT 1",
        "SELECT id, conversation_id, role FROM messages ORDER BY id LIMIT 1",
        "SELECT id, conversation_id, role FROM messages ORDER BY id DESC LIMIT 1",
    ];
    for sql in PROBES {
        match run_probe(sql) {
            Ok(()) => {}
            Err(FrankenError::QueryReturnedNoRows) => {}
            Err(err) if corruption_class_franken_error(&err) => {
                return (Some(StateDbIntegrity::Corrupt), Some(err.to_string()));
            }
            Err(_) => return (None, None),
        }
    }
    (Some(StateDbIntegrity::Ok), None)
}

/// Read the canonical database's fixed-size SQLite header, then run the
/// stronger real-column probe only when the complete main+sidecar bundle is
/// below 256 MiB. `Some(detail)` means corruption was classified; `None`
/// means the bounded checks passed or could not safely classify the archive.
///
/// This is deliberately not a complete page-integrity check. Large archives
/// receive only the constant-size header check; deeper corruption still needs
/// a current doctor integrity attestation or an explicit full integrity scan.
pub(crate) fn bounded_canonical_db_corruption_probe(
    db_path: &Path,
    reason: &str,
) -> Option<String> {
    if !db_path.is_file() {
        return None;
    }
    // GH #413: this preflight must have a physical-I/O bound, not merely a
    // SQL row-count bound. A query can hydrate a large part of the
    // B-tree while executing an ORDER BY ... LIMIT 1 edge lookup; the old
    // four-query "bounded" probe read 8.4 GiB from a 35.7 GB archive before
    // indexing had even entered its preparing phase. A query timeout would
    // not repair that contract because the synchronous query cannot be
    // cancelled safely from this call site.
    //
    // Reading the fixed 100-byte SQLite database header is genuinely bounded
    // regardless of archive size. The stronger real-column probe below is
    // retained for small bundles, where its worst-case physical work is also
    // bounded, so the earlier i6upe smashed-page guard does not regress.
    let mut header = [0_u8; 100];
    let file_len = fs::metadata(db_path).ok()?.len();
    if file_len == 0 {
        // SQLite permits a zero-byte file as a new, empty database.
        return None;
    }
    if file_len < header.len() as u64 {
        return Some(format!(
            "{reason}: truncated SQLite database header ({} bytes; expected at least {})",
            file_len,
            header.len()
        ));
    }
    let mut file = fs::File::open(db_path).ok()?;
    std::io::Read::read_exact(&mut file, &mut header).ok()?;

    const SQLITE_MAGIC: &[u8; 16] = b"SQLite format 3\0";
    if &header[..SQLITE_MAGIC.len()] != SQLITE_MAGIC {
        return Some(format!("{reason}: invalid SQLite database header magic"));
    }

    let encoded_page_size = u16::from_be_bytes([header[16], header[17]]);
    let page_size = if encoded_page_size == 1 {
        65_536_u32
    } else {
        u32::from(encoded_page_size)
    };
    if !(512..=65_536).contains(&page_size) || !page_size.is_power_of_two() {
        return Some(format!(
            "{reason}: invalid SQLite database page size {page_size}"
        ));
    }
    if file_len % u64::from(page_size) != 0 {
        return Some(format!(
            "{reason}: SQLite database length {file_len} is not a multiple of page size {page_size}"
        ));
    }
    let reserved_bytes = u32::from(header[20]);
    if page_size.saturating_sub(reserved_bytes) < 480 {
        return Some(format!(
            "{reason}: invalid SQLite reserved-byte count {reserved_bytes} for page size {page_size}"
        ));
    }
    if !matches!(header[18], 1 | 2) || !matches!(header[19], 1 | 2) {
        return Some(format!(
            "{reason}: invalid SQLite file format versions write={} read={}",
            header[18], header[19]
        ));
    }
    if header[21..24] != [64, 32, 32] {
        return Some(format!(
            "{reason}: invalid SQLite payload fractions {},{},{}",
            header[21], header[22], header[23]
        ));
    }

    let mut bundle_bytes = file_len;
    for suffix in ["-wal", "-shm"] {
        let sidecar_path = sqlite_sidecar_path(db_path, suffix)?;
        match fs::symlink_metadata(&sidecar_path) {
            Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                bundle_bytes = bundle_bytes.saturating_add(metadata.len());
            }
            Ok(_) => return None,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return None,
        }
    }
    if bundle_bytes > CANONICAL_DB_CORRUPTION_PROBE_MAX_BYTES {
        return None;
    }

    // This observation also runs before a read-only force rebuild has archive
    // writer admission. It must never repair WAL or schema state on open.
    match FrankenStorage::open_strict_readonly_with_timeout(
        db_path,
        CANONICAL_DB_CORRUPTION_PROBE_OPEN_TIMEOUT,
    ) {
        Ok(storage) => {
            let mut conn = storage.into_raw();
            let (integrity, detail) = probe_state_db_integrity(|sql| {
                corruption_probe_query_row_map_retry(&conn, sql, &[], |row| {
                    row.get_typed::<i64>(0).map(|_| ())
                })
            });
            if let Err(err) = conn.close_without_checkpoint_in_place() {
                tracing::warn!(
                    error = %err,
                    db_path = %db_path.display(),
                    reason,
                    "checkpoint-free close failed for canonical DB corruption probe; falling back to best-effort owner-thread cleanup"
                );
                conn.close_best_effort_in_place();
            }
            match integrity {
                Some(StateDbIntegrity::Corrupt) => {
                    Some(detail.unwrap_or_else(|| "corruption-class read failure".to_owned()))
                }
                _ => None,
            }
        }
        Err(err) if !retryable_franken_anyhow(&err) => {
            let message = format!("{err:#}");
            let lower = message.to_ascii_lowercase();
            // Duplicate `fts_messages` rows in sqlite_master make the strict
            // open report a malformed image, but they describe the derived
            // FTS catalog, not the archive's data. The writable open that
            // follows deduplicates them (dedupe_conflicting_fts_schema_rows_
            // via_sqlite3), so refusing here would block the repair.
            if lower.contains("conflicting virtual-table entries") && lower.contains("fts_messages")
            {
                return None;
            }
            (lower.contains("malformed")
                || lower.contains("disk image")
                || lower.contains("not a database"))
            .then_some(message)
        }
        Err(_) => None,
    }
}
