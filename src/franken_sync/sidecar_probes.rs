use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use super::{Connection, FileIdentity, FrankenError, Row, SqliteValue, compat};

#[derive(Clone, Copy, Default)]
struct LiveDbState {
    count: usize,
    rw_count: usize,
    strict_wal_ro_count: usize,
    known_wal: Option<bool>,
    known_stamp: Option<(i64, i64)>,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct LiveRegHandle {
    pub(super) id: FileIdentity,
    pub(super) is_rw: bool,
    pub(super) is_strict_wal_ro: bool,
}

/// Result of safely probing the 100-byte SQLite header stamp without dropping
/// any in-process POSIX `fcntl` locks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SqliteHeaderStamp {
    /// File is shorter than 100 bytes or lacks the `SQLite format 3\0` header.
    NotSqlite,
    /// `(application_id, user_version)` read from the 100-byte header (when no
    /// connection on this inode is open in-process) or via a live SQLite
    /// connection (`PRAGMA application_id` / `PRAGMA user_version`).
    Stamp {
        application_id: i64,
        user_version: i64,
    },
}

static LIVE_DB_CONNECTIONS: OnceLock<Mutex<HashMap<FileIdentity, LiveDbState>>> = OnceLock::new();

fn live_db_map() -> &'static Mutex<HashMap<FileIdentity, LiveDbState>> {
    LIVE_DB_CONNECTIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(super) fn canonical_db_path(db_path: &Path) -> PathBuf {
    std::fs::canonicalize(db_path).unwrap_or_else(|_| db_path.to_path_buf())
}

pub(super) fn uri_has_readonly_mode(path_str: &str) -> bool {
    if !path_str.starts_with("file:") {
        return false;
    }
    let Some((_, query)) = path_str.split_once('?') else {
        return false;
    };
    let query = query.split('#').next().unwrap_or(query);
    query.split('&').any(|pair| pair == "mode=ro")
}

fn read_header_stamp_from_file(path: &Path) -> Option<(i64, i64)> {
    let mut file = std::fs::File::open(path).ok()?;
    let mut header = [0_u8; SQLITE_HEADER_PROBE_SIZE];
    file.read_exact(&mut header).ok()?;
    match parse_sqlite_header_stamp(&header) {
        SqliteHeaderStamp::Stamp {
            application_id,
            user_version,
        } => Some((application_id, user_version)),
        SqliteHeaderStamp::NotSqlite => None,
    }
}

pub(super) fn register_live_connection(
    id: FileIdentity,
    known_wal: Option<bool>,
    is_rw: bool,
    is_strict_wal_ro: bool,
    known_stamp: Option<(i64, i64)>,
) -> LiveRegHandle {
    let mut map = match live_db_map().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    let entry = map.entry(id).or_default();
    entry.count = entry.count.saturating_add(1);
    if is_rw {
        entry.rw_count = entry.rw_count.saturating_add(1);
    }
    if is_strict_wal_ro {
        entry.strict_wal_ro_count = entry.strict_wal_ro_count.saturating_add(1);
    }
    if known_wal.is_some() {
        entry.known_wal = known_wal;
    }
    if entry.known_stamp.is_none() && known_stamp.is_some() {
        entry.known_stamp = known_stamp;
    }
    LiveRegHandle {
        id,
        is_rw,
        is_strict_wal_ro,
    }
}

pub(super) fn update_live_wal_state(id: FileIdentity, is_wal: bool) {
    let mut map = match live_db_map().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    if let Some(entry) = map.get_mut(&id) {
        entry.known_wal = Some(is_wal);
    }
}

pub(crate) fn record_live_archive_stamp(
    id: FileIdentity,
    application_id: i64,
    user_version: i64,
) {
    let mut map = match live_db_map().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    if let Some(entry) = map.get_mut(&id)
        && entry.count > 0
    {
        entry.known_stamp = Some((application_id, user_version));
    }
}

pub(super) fn unregister_live_connection(reg: LiveRegHandle) {
    let mut map = match live_db_map().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    if let Some(entry) = map.get_mut(&reg.id) {
        entry.count = entry.count.saturating_sub(1);
        if reg.is_rw {
            entry.rw_count = entry.rw_count.saturating_sub(1);
        }
        if reg.is_strict_wal_ro {
            entry.strict_wal_ro_count = entry.strict_wal_ro_count.saturating_sub(1);
        }
        if entry.count == 0 {
            map.remove(&reg.id);
        }
    }
}

pub(super) fn live_connection_state(id: FileIdentity) -> Option<Option<bool>> {
    let map = match live_db_map().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    map.get(&id)
        .filter(|entry| entry.count > 0)
        .map(|entry| entry.known_wal)
}

fn live_archive_stamp(id: FileIdentity) -> Option<(i64, i64)> {
    let map = match live_db_map().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    map.get(&id)
        .filter(|entry| entry.count > 0)
        .and_then(|entry| entry.known_stamp)
}

fn acquire_pre_reg_in_map(
    map: &mut HashMap<FileIdentity, LiveDbState>,
    id: Option<FileIdentity>,
    is_rw: bool,
    is_strict_wal_ro: bool,
    known_stamp: Option<(i64, i64)>,
) -> LivePreRegistration {
    if let Some(id) = id {
        let entry = map.entry(id).or_default();
        entry.count = entry.count.saturating_add(1);
        if is_rw {
            entry.rw_count = entry.rw_count.saturating_add(1);
        }
        if is_strict_wal_ro {
            entry.strict_wal_ro_count = entry.strict_wal_ro_count.saturating_add(1);
        }
        if entry.known_stamp.is_none() && known_stamp.is_some() {
            entry.known_stamp = known_stamp;
        }
    }
    LivePreRegistration {
        id,
        is_rw,
        is_strict_wal_ro,
    }
}

fn parse_sqlite_header_stamp(header: &[u8; SQLITE_HEADER_PROBE_SIZE]) -> SqliteHeaderStamp {
    if header[..16] != SQLITE_HEADER_MAGIC {
        return SqliteHeaderStamp::NotSqlite;
    }
    let user_version = i64::from(i32::from_be_bytes([
        header[60], header[61], header[62], header[63],
    ]));
    let application_id = i64::from(i32::from_be_bytes([
        header[68], header[69], header[70], header[71],
    ]));
    SqliteHeaderStamp::Stamp {
        application_id,
        user_version,
    }
}

/// Read `(application_id, user_version)` for `path` without dropping any
/// in-process POSIX `fcntl` locks.
///
/// * If no `franken_sync::Connection` on `path`'s inode is open in this
///   process, the 100-byte SQLite header is read and closed while holding
///   `LIVE_DB_CONNECTIONS`, so no concurrent `Connection::open` can race with
///   the raw `File::open` + `close()`.
/// * If a connection on this inode IS open in this process, no raw `File` is
///   opened on `path` at all; instead `PRAGMA application_id` and
///   `PRAGMA user_version` are queried through a strict read-only SQLite
///   connection (C SQLite's `unixClose` retains descriptors while another
///   in-process connection holds locks), falling back to the registered live
///   stamp only if the live WAL store has no sidecars yet or is exclusively
///   locked.
fn read_stamp_via_connection(
    conn: &Connection,
    db_id: Option<FileIdentity>,
) -> std::io::Result<SqliteHeaderStamp> {
    let _ = conn.raw().busy_timeout(Duration::from_millis(250));
    let app_res = conn.query_row("PRAGMA application_id;");
    let ver_res = conn.query_row("PRAGMA user_version;");
    match (app_res, ver_res) {
        (Ok(app_row), Ok(ver_row)) => {
            let application_id = row_i64(&app_row, 0);
            let user_version = row_i64(&ver_row, 0);
            if let Some(id) = db_id {
                record_live_archive_stamp(id, application_id, user_version);
            }
            Ok(SqliteHeaderStamp::Stamp {
                application_id,
                user_version,
            })
        }
        (Err(err), _) | (_, Err(err)) if err.is_busy_class() => {
            if let Some((application_id, user_version)) = db_id.and_then(live_archive_stamp) {
                return Ok(SqliteHeaderStamp::Stamp {
                    application_id,
                    user_version,
                });
            }
            Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                err.to_string(),
            ))
        }
        (Err(FrankenError::NotADatabase { .. }), _)
        | (_, Err(FrankenError::NotADatabase { .. })) => Ok(SqliteHeaderStamp::NotSqlite),
        (Err(err), _) | (_, Err(err)) => Err(std::io::Error::other(err.to_string())),
    }
}

pub(crate) fn probe_sqlite_header_stamp_safely(
    path: &Path,
) -> std::io::Result<SqliteHeaderStamp> {
    let db_id = FileIdentity::from_path(path)?;
    let (has_rw, has_strict_wal_ro, known_wal) = {
        let map = match live_db_map().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(entry) = db_id.and_then(|id| map.get(&id).filter(|e| e.count > 0)) {
            if let Some((application_id, user_version)) = entry.known_stamp {
                return Ok(SqliteHeaderStamp::Stamp {
                    application_id,
                    user_version,
                });
            }
            (
                entry.rw_count > 0,
                entry.strict_wal_ro_count > 0,
                entry.known_wal,
            )
        } else {
            let mut file = std::fs::File::open(path)?;
            let mut header = [0_u8; SQLITE_HEADER_PROBE_SIZE];
            let stamp = match file.read_exact(&mut header) {
                Ok(()) => parse_sqlite_header_stamp(&header),
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                    SqliteHeaderStamp::NotSqlite
                }
                Err(error) => return Err(error),
            };
            drop(file);
            return Ok(stamp);
        }
    };

    let canonical_db = canonical_db_path(path);
    let path_str = path.to_string_lossy();
    let wal_exists = sidecar_exists(&sidecar_path(&canonical_db, "-wal"));
    let shm_exists = sidecar_exists(&sidecar_path(&canonical_db, "-shm"));
    if !has_strict_wal_ro
        && ((wal_exists && shm_exists)
            || (!wal_exists && !shm_exists && known_wal == Some(false)))
    {
        match Connection::open_in_place_readonly_for_pool(path_str.as_ref()) {
            Ok(conn) => return read_stamp_via_connection(&conn, db_id),
            Err(FrankenError::NotADatabase { .. }) => return Ok(SqliteHeaderStamp::NotSqlite),
            Err(err) if !err.is_busy_class() && !matches!(err, FrankenError::BusyRecovery) => {
                return Err(std::io::Error::other(err.to_string()));
            }
            Err(_) => {}
        }
    }

    let strict_flags =
        compat::OpenFlags::SQLITE_OPEN_READ_ONLY | compat::OpenFlags::SQLITE_OPEN_STRICT_READ_ONLY;
    let mut attempts = 0u32;
    loop {
        match Connection::open_strict_readonly(&path_str, strict_flags) {
            Ok(conn) => return read_stamp_via_connection(&conn, db_id),
            Err(FrankenError::NotADatabase { .. }) => return Ok(SqliteHeaderStamp::NotSqlite),
            Err(FrankenError::BusyRecovery) => {
                if db_id.and_then(live_connection_state).flatten().is_none() && attempts < 20 {
                    attempts += 1;
                    std::thread::sleep(Duration::from_millis(5));
                    continue;
                }
                if let Some((application_id, user_version)) = db_id.and_then(live_archive_stamp) {
                    return Ok(SqliteHeaderStamp::Stamp {
                        application_id,
                        user_version,
                    });
                }
                let _ = has_rw;
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "archive is live in-process without readable WAL sidecars",
                ));
            }
            Err(err) => return Err(std::io::Error::other(err.to_string())),
        }
    }
}

/// RAII pre-registration guard: increments `LIVE_DB_CONNECTIONS` before
/// `rusqlite::Connection::open*` so concurrent `open_strict_readonly` calls
/// never observe `live_connection_state == None` after the fd is opened.
/// Rolls back the registration on error and hands ownership to `Connection` on
/// success.
pub(super) struct LivePreRegistration {
    id: Option<FileIdentity>,
    is_rw: bool,
    is_strict_wal_ro: bool,
}

impl LivePreRegistration {
    pub(super) fn acquire_for_path(
        path_str: &str,
        fs_path: Option<&Path>,
        flags: Option<compat::OpenFlags>,
    ) -> Result<Self, FrankenError> {
        let id = compute_open_identity(path_str, fs_path, flags)?;
        let is_readonly = match flags {
            Some(f) => {
                f.contains(compat::OpenFlags::SQLITE_OPEN_READ_ONLY)
                    || f.contains(compat::OpenFlags::SQLITE_OPEN_STRICT_READ_ONLY)
            }
            None => uri_has_readonly_mode(path_str),
        };
        let is_rw = !is_readonly;
        let Some(db_id) = id else {
            return Ok(Self {
                id: None,
                is_rw,
                is_strict_wal_ro: false,
            });
        };
        let header_path = fs_path.or_else(|| {
            (!path_str.starts_with("file:")).then(|| Path::new(path_str))
        });
        let mut map = match live_db_map().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        let known_stamp = if map.get(&db_id).is_none_or(|e| e.count == 0) {
            header_path.and_then(read_header_stamp_from_file)
        } else {
            None
        };
        Ok(acquire_pre_reg_in_map(
            &mut map,
            Some(db_id),
            is_rw,
            false,
            known_stamp,
        ))
    }

    pub(super) fn acquire_id_with_mode(
        id: Option<FileIdentity>,
        is_rw: bool,
        is_strict_wal_ro: bool,
    ) -> Self {
        if let Some(id) = id {
            let mut map = match live_db_map().lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            return acquire_pre_reg_in_map(&mut map, Some(id), is_rw, is_strict_wal_ro, None);
        }
        Self {
            id: None,
            is_rw,
            is_strict_wal_ro,
        }
    }

    pub(super) fn id(&self) -> Option<FileIdentity> {
        self.id
    }

    pub(super) fn commit(&mut self, known_wal: Option<bool>) -> Option<LiveRegHandle> {
        let id = self.id.take()?;
        if let Some(is_wal) = known_wal {
            update_live_wal_state(id, is_wal);
        }
        Some(LiveRegHandle {
            id,
            is_rw: self.is_rw,
            is_strict_wal_ro: self.is_strict_wal_ro,
        })
    }
}

impl Drop for LivePreRegistration {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            unregister_live_connection(LiveRegHandle {
                id,
                is_rw: self.is_rw,
                is_strict_wal_ro: self.is_strict_wal_ro,
            });
        }
    }
}

pub(super) fn compute_open_identity(
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

impl Connection {
    /// Strict read-only open *in place*: not one byte, mtime, or sidecar of
    /// the db, `-wal`, `-shm`, or `-journal` may be created or modified.
    /// Mirrors `franken_agent_detection`'s proven
    /// `open_external_strict_in_place`:
    ///
    /// * **Mode 1** — non-WAL header (`header[18..20] != (2,_)`) and no
    ///   `-wal`/`-shm` sidecars: plain `READ_ONLY | NO_MUTEX` open; a
    ///   rollback-mode store creates no sidecar on a read-only open.
    /// * **Mode 2** (`#[cfg(unix)]`, `-wal` + `-shm` present): both are
    ///   validated byte-for-byte, then the store opens through a
    ///   `file:...?mode=ro&readonly_shm=1` URI so C SQLite mmaps the
    ///   wal-index read-only instead of `O_RDWR`/`PROT_WRITE` mutating it.
    /// * **Every other WAL/sidecar shape** — WAL header with missing or
    ///   invalid `-shm`, `-wal` without `-shm`, `-shm` without `-wal`, or
    ///   a live `-journal` — is refused with `BusyRecovery` *without* an
    ///   open on the original path.
    ///
    /// When this process already holds an open `Connection` on the same
    /// database inode, `.db`, `-journal`, and `-shm` are never opened via
    /// `std::fs::File::open` (closing a temporary descriptor would drop
    /// in-process POSIX `fcntl` locks).
    pub(super) fn open_strict_readonly(
        path: &str,
        flags: compat::OpenFlags,
    ) -> Result<Self, FrankenError> {
        super::check_connection_admission()?;
        let db_path = Path::new(path);
        let meta = std::fs::metadata(db_path).map_err(|error| match error.kind() {
            std::io::ErrorKind::NotFound => FrankenError::CannotOpen {
                path: db_path.to_path_buf(),
            },
            _ => FrankenError::Io(error),
        })?;
        if !meta.is_file() {
            return Err(FrankenError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("SQLite path is not a regular file: {}", db_path.display()),
            )));
        }
        if meta.len() < SQLITE_HEADER_PROBE_SIZE as u64 {
            return Err(FrankenError::NotADatabase {
                path: db_path.to_path_buf(),
            });
        }
        let canonical_db = canonical_db_path(db_path);
        let db_id = FileIdentity::from_path(&canonical_db).map_err(FrankenError::Io)?;
        enum StrictPlan {
            Live(Option<bool>),
            Mode1Rollback(LivePreRegistration),
            CleanWalSnapshot,
            #[cfg(unix)]
            Mode2Wal(LivePreRegistration),
        }

        let journal_path = sidecar_path(&canonical_db, "-journal");
        let wal_path = sidecar_path(&canonical_db, "-wal");
        let shm_path = sidecar_path(&canonical_db, "-shm");

        // Synchronize offline `File::open` + `close()` checks against
        // `LIVE_DB_CONNECTIONS` so no concurrent `Connection::open` can acquire
        // POSIX `fcntl` locks on the same inode while our temporary fd closes.
        let plan = {
            let mut map = match live_db_map().lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            if let Some(known_wal) = db_id.and_then(|id| {
                map.get(&id)
                    .filter(|entry| entry.count > 0)
                    .map(|entry| entry.known_wal)
            }) {
                StrictPlan::Live(known_wal)
            } else {
                let mut header = [0_u8; SQLITE_HEADER_PROBE_SIZE];
                std::fs::File::open(&canonical_db)
                    .and_then(|mut file| file.read_exact(&mut header))
                    .map_err(|error| {
                        if error.kind() == std::io::ErrorKind::UnexpectedEof {
                            FrankenError::NotADatabase {
                                path: db_path.to_path_buf(),
                            }
                        } else {
                            FrankenError::Io(error)
                        }
                    })?;
                if header[..16] != SQLITE_HEADER_MAGIC {
                    return Err(FrankenError::NotADatabase {
                        path: db_path.to_path_buf(),
                    });
                }
                let parsed_stamp = match parse_sqlite_header_stamp(&header) {
                    SqliteHeaderStamp::Stamp {
                        application_id,
                        user_version,
                    } => Some((application_id, user_version)),
                    SqliteHeaderStamp::NotSqlite => None,
                };

                // A live rollback journal means the store needs recovery a
                // read-only open must not perform.
                if std::fs::metadata(&journal_path)
                    .is_ok_and(|meta| meta.is_file() && meta.len() > 0)
                {
                    let mut first = [0_u8; 1];
                    match std::fs::File::open(&journal_path)
                        .and_then(|mut f| f.read_exact(&mut first))
                    {
                        Ok(()) if first[0] != 0 => return Err(FrankenError::BusyRecovery),
                        Ok(()) => {}
                        Err(error) => return Err(FrankenError::Io(error)),
                    }
                }

                let is_wal_header = header[18] == 2 || header[19] == 2;
                let wal_exists = sidecar_exists(&wal_path);
                let shm_exists = sidecar_exists(&shm_path);

                if !is_wal_header && !wal_exists && !shm_exists {
                    StrictPlan::Mode1Rollback(acquire_pre_reg_in_map(
                        &mut map,
                        db_id,
                        false,
                        false,
                        parsed_stamp,
                    ))
                } else if is_wal_header && !wal_exists && !shm_exists {
                    // Cleanly closed 1-file WAL database (no sidecars, no live
                    // journal, not open in-process): read through a private
                    // `TempDir` snapshot so C SQLite never creates `-wal`/`-shm`
                    // beside the original archive (P0-2).
                    StrictPlan::CleanWalSnapshot
                } else {
                    #[cfg(unix)]
                    if wal_exists && shm_exists {
                        if !wal_and_shm_are_coherent(&wal_path, &shm_path)? {
                            return Err(FrankenError::BusyRecovery);
                        }
                        StrictPlan::Mode2Wal(acquire_pre_reg_in_map(
                            &mut map,
                            db_id,
                            false,
                            true,
                            parsed_stamp,
                        ))
                    } else {
                        return Err(FrankenError::BusyRecovery);
                    }
                    #[cfg(not(unix))]
                    {
                        let _ = (wal_exists, shm_exists);
                        return Err(FrankenError::BusyRecovery);
                    }
                }
            }
        };

        let open_flags = strict_readonly_rusqlite_flags(flags);
        match plan {
            StrictPlan::Live(known_wal) => Self::open_strict_readonly_live_process(
                &canonical_db,
                db_path,
                path,
                db_id,
                known_wal,
                flags,
            ),
            StrictPlan::Mode1Rollback(pre_reg) => {
                let inner = rusqlite::Connection::open_with_flags(path, open_flags)
                    .map_err(|error| FrankenError::from_rusqlite(error, Some(db_path)))?;
                Ok(Self::from_parts(
                    inner,
                    Some(PathBuf::from(path)),
                    pre_reg,
                    Some(false),
                    true,
                ))
            }
            StrictPlan::CleanWalSnapshot => Self::open_snapshot_copy(db_path),
            #[cfg(unix)]
            StrictPlan::Mode2Wal(pre_reg) => {
                Self::open_wal_strict_in_place(&canonical_db, db_path, path, pre_reg)
            }
        }
    }

    /// Read-only open with safe WAL recovery:
    ///
    /// * When no connection on the archive is live in-process: rollback-only
    ///   archives open in place (`READ_ONLY | NO_MUTEX`), while WAL / sidecar /
    ///   journal archives read through a bounded private snapshot copy (never
    ///   `readonly_shm=1`, so subsequent writers in this process never hit
    ///   `unixShmNode.isReadonly = 1`).
    /// * When a live connection already holds descriptors on the inode: NEVER
    ///   snapshots (copying `.db` closes an fd and drops POSIX `fcntl` locks);
    ///   opens in place without `readonly_shm=1` unless an explicit strict-WAL
    ///   connection is the only live handle.
    pub(super) fn open_recovery_readonly(path: &str) -> Result<Self, FrankenError> {
        let db_path = Path::new(path);
        let meta = std::fs::metadata(db_path).map_err(|error| match error.kind() {
            std::io::ErrorKind::NotFound => FrankenError::CannotOpen {
                path: db_path.to_path_buf(),
            },
            _ => FrankenError::Io(error),
        })?;
        if !meta.is_file() {
            return Err(FrankenError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("SQLite path is not a regular file: {}", db_path.display()),
            )));
        }
        if meta.len() < SQLITE_HEADER_PROBE_SIZE as u64 {
            return Err(FrankenError::NotADatabase {
                path: db_path.to_path_buf(),
            });
        }
        let canonical_db = canonical_db_path(db_path);
        let db_id = FileIdentity::from_path(&canonical_db).map_err(FrankenError::Io)?;
        let journal_path = sidecar_path(&canonical_db, "-journal");
        let wal_path = sidecar_path(&canonical_db, "-wal");
        let shm_path = sidecar_path(&canonical_db, "-shm");

        enum RecoveryPlan {
            Live {
                known_wal: Option<bool>,
                has_rw: bool,
                has_strict_wal_ro: bool,
            },
            Mode1Rollback(LivePreRegistration),
            Snapshot {
                allow_mode2_fallback: bool,
            },
        }

        let plan = {
            let mut map = match live_db_map().lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            if let Some(entry) = db_id.and_then(|id| map.get(&id).filter(|e| e.count > 0)).copied()
            {
                RecoveryPlan::Live {
                    known_wal: entry.known_wal,
                    has_rw: entry.rw_count > 0,
                    has_strict_wal_ro: entry.strict_wal_ro_count > 0,
                }
            } else {
                let mut header = [0_u8; SQLITE_HEADER_PROBE_SIZE];
                std::fs::File::open(&canonical_db)
                    .and_then(|mut file| file.read_exact(&mut header))
                    .map_err(|error| {
                        if error.kind() == std::io::ErrorKind::UnexpectedEof {
                            FrankenError::NotADatabase {
                                path: db_path.to_path_buf(),
                            }
                        } else {
                            FrankenError::Io(error)
                        }
                    })?;
                if header[..16] != SQLITE_HEADER_MAGIC {
                    return Err(FrankenError::NotADatabase {
                        path: db_path.to_path_buf(),
                    });
                }
                let parsed_stamp = match parse_sqlite_header_stamp(&header) {
                    SqliteHeaderStamp::Stamp {
                        application_id,
                        user_version,
                    } => Some((application_id, user_version)),
                    SqliteHeaderStamp::NotSqlite => None,
                };
                let journal_active = if std::fs::metadata(&journal_path)
                    .is_ok_and(|meta| meta.is_file() && meta.len() > 0)
                {
                    let mut first = [0_u8; 1];
                    match std::fs::File::open(&journal_path)
                        .and_then(|mut f| f.read_exact(&mut first))
                    {
                        Ok(()) => first[0] != 0,
                        Err(error) => return Err(FrankenError::Io(error)),
                    }
                } else {
                    false
                };
                let is_wal_header = header[18] == 2 || header[19] == 2;
                let wal_exists = sidecar_exists(&wal_path);
                let shm_exists = sidecar_exists(&shm_path);

                if !journal_active && !is_wal_header && !wal_exists && !shm_exists {
                    RecoveryPlan::Mode1Rollback(acquire_pre_reg_in_map(
                        &mut map,
                        db_id,
                        false,
                        false,
                        parsed_stamp,
                    ))
                } else {
                    RecoveryPlan::Snapshot {
                        allow_mode2_fallback: !journal_active && wal_exists && shm_exists,
                    }
                }
            }
        };

        let plain_ro_flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
            | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
        match plan {
            RecoveryPlan::Mode1Rollback(pre_reg) => {
                let inner = rusqlite::Connection::open_with_flags(path, plain_ro_flags)
                    .map_err(|error| FrankenError::from_rusqlite(error, Some(db_path)))?;
                Ok(Self::from_parts(
                    inner,
                    Some(PathBuf::from(path)),
                    pre_reg,
                    Some(false),
                    true,
                ))
            }
            RecoveryPlan::Snapshot {
                allow_mode2_fallback,
            } => match Self::open_snapshot_copy(db_path) {
                Ok(conn) => Ok(conn),
                Err(FrankenError::Internal(msg))
                    if allow_mode2_fallback && msg.contains("snapshot limit") =>
                {
                    let strict_flags = compat::OpenFlags::SQLITE_OPEN_READ_ONLY
                        | compat::OpenFlags::SQLITE_OPEN_STRICT_READ_ONLY;
                    Self::open_strict_readonly(path, strict_flags)
                }
                Err(error) => Err(error),
            },
            RecoveryPlan::Live {
                known_wal,
                has_rw,
                has_strict_wal_ro,
            } => {
                if std::fs::metadata(&journal_path)
                    .is_ok_and(|meta| meta.is_file() && meta.len() > 0)
                {
                    return Err(FrankenError::BusyRecovery);
                }
                let wal_exists = sidecar_exists(&wal_path);
                let shm_exists = sidecar_exists(&shm_path);
                if !wal_exists && !shm_exists {
                    return match known_wal {
                        Some(false) => {
                            let pre_reg =
                                LivePreRegistration::acquire_id_with_mode(db_id, false, false);
                            let inner = rusqlite::Connection::open_with_flags(path, plain_ro_flags)
                                .map_err(|error| {
                                    FrankenError::from_rusqlite(error, Some(db_path))
                                })?;
                            Ok(Self::from_parts(
                                inner,
                                Some(PathBuf::from(path)),
                                pre_reg,
                                Some(false),
                                true,
                            ))
                        }
                        Some(true) | None => Err(FrankenError::BusyRecovery),
                    };
                }
                #[cfg(unix)]
                if wal_exists && shm_exists {
                    if !wal_file_is_valid(&wal_path)? {
                        return Err(FrankenError::BusyRecovery);
                    }
                    let shm_meta = match std::fs::metadata(&shm_path) {
                        Ok(meta) => meta,
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                            return Err(FrankenError::BusyRecovery);
                        }
                        Err(error) => return Err(FrankenError::Io(error)),
                    };
                    if !shm_meta.is_file() || shm_meta.len() < SQLITE_SHM_MIN_SIZE {
                        return Err(FrankenError::BusyRecovery);
                    }
                    if has_strict_wal_ro && !has_rw {
                        return Self::open_wal_strict_in_place(
                            &canonical_db,
                            db_path,
                            path,
                            LivePreRegistration::acquire_id_with_mode(db_id, false, true),
                        );
                    }
                    let pre_reg = LivePreRegistration::acquire_id_with_mode(db_id, false, false);
                    let inner = rusqlite::Connection::open_with_flags(path, plain_ro_flags)
                        .map_err(|error| {
                            if wal_index_open_failure(&error) {
                                FrankenError::BusyRecovery
                            } else {
                                FrankenError::from_rusqlite(error, Some(db_path))
                            }
                        })?;
                    match inner.query_row("PRAGMA schema_version;", [], |row| {
                        row.get::<usize, i64>(0)
                    }) {
                        Ok(_) => {
                            return Ok(Self::from_parts(
                                inner,
                                Some(PathBuf::from(path)),
                                pre_reg,
                                Some(true),
                                true,
                            ));
                        }
                        Err(error) if wal_index_open_failure(&error) => {
                            drop(inner);
                            return Err(FrankenError::BusyRecovery);
                        }
                        Err(error) => {
                            return Err(FrankenError::from_rusqlite(error, Some(db_path)));
                        }
                    }
                }
                Err(FrankenError::BusyRecovery)
            }
        }
    }

    /// Strict read-only open when this process already has `db_id` open:
    /// avoids `File::open` on `.db`, `-journal`, and `-shm` so existing
    /// in-process POSIX `fcntl` locks stay intact.
    fn open_strict_readonly_live_process(
        canonical_db: &Path,
        db_path: &Path,
        path: &str,
        db_id: Option<FileIdentity>,
        known_wal: Option<bool>,
        flags: compat::OpenFlags,
    ) -> Result<Self, FrankenError> {
        let journal_path = sidecar_path(canonical_db, "-journal");
        if std::fs::metadata(&journal_path).is_ok_and(|meta| meta.is_file() && meta.len() > 0) {
            return Err(FrankenError::BusyRecovery);
        }

        let wal_path = sidecar_path(canonical_db, "-wal");
        let shm_path = sidecar_path(canonical_db, "-shm");
        let wal_exists = sidecar_exists(&wal_path);
        let shm_exists = sidecar_exists(&shm_path);
        let open_flags = strict_readonly_rusqlite_flags(flags);

        if !wal_exists && !shm_exists {
            return match known_wal {
                Some(false) => {
                    let pre_reg =
                        LivePreRegistration::acquire_id_with_mode(db_id, false, false);
                    let inner = rusqlite::Connection::open_with_flags(path, open_flags)
                        .map_err(|error| FrankenError::from_rusqlite(error, Some(db_path)))?;
                    Ok(Self::from_parts(
                        inner,
                        Some(PathBuf::from(path)),
                        pre_reg,
                        Some(false),
                        true,
                    ))
                }
                // `Some(true)` means WAL header with missing sidecars; `None`
                // means WAL vs rollback is unknown while another connection is
                // live — refuse without letting C SQLite create `<db>-wal`.
                Some(true) | None => Err(FrankenError::BusyRecovery),
            };
        }

        #[cfg(unix)]
        if wal_exists && shm_exists {
            // SQLite holds no POSIX fcntl locks on `-wal`, only on `.db` and `-shm`.
            if !wal_file_is_valid(&wal_path)? {
                return Err(FrankenError::BusyRecovery);
            }
            let shm_meta = match std::fs::metadata(&shm_path) {
                Ok(meta) => meta,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Err(FrankenError::BusyRecovery);
                }
                Err(error) => return Err(FrankenError::Io(error)),
            };
            if !shm_meta.is_file() || shm_meta.len() < SQLITE_SHM_MIN_SIZE {
                return Err(FrankenError::BusyRecovery);
            }
            return Self::open_wal_strict_in_place(
                canonical_db,
                db_path,
                path,
                LivePreRegistration::acquire_id_with_mode(db_id, false, true),
            );
        }
        #[cfg(not(unix))]
        let _ = open_flags;

        Err(FrankenError::BusyRecovery)
    }

    /// Mode 2 of `open_strict_readonly`: a WAL store whose `-wal` and
    /// `-shm` both exist and validate is opened in place through a
    /// `mode=ro&readonly_shm=1` URI — C SQLite mmaps the wal-index
    /// read-only and takes read marks through locks instead of writing
    /// them: zero bytes and zero mtime changes on all three files.
    #[cfg(unix)]
    fn open_wal_strict_in_place(
        canonical_db: &Path,
        db_path: &Path,
        path: &str,
        pre_reg: LivePreRegistration,
    ) -> Result<Self, FrankenError> {
        let uri = read_only_uri(canonical_db);
        let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
            | rusqlite::OpenFlags::SQLITE_OPEN_URI
            | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let inner = match rusqlite::Connection::open_with_flags(uri.as_str(), flags) {
            Ok(inner) => inner,
            Err(error) if wal_index_open_failure(&error) => {
                return Err(FrankenError::BusyRecovery);
            }
            Err(error) => {
                return Err(FrankenError::from_rusqlite(error, Some(db_path)));
            }
        };
        // The schema read forces C SQLite to attach the wal-index now, so
        // a store that only fails on first access reports `BusyRecovery`
        // here instead of mid-scan.
        match inner.query_row("PRAGMA schema_version;", [], |row| row.get::<usize, i64>(0)) {
            Ok(_) => Ok(Self::from_parts(
                inner,
                Some(PathBuf::from(path)),
                pre_reg,
                Some(true),
                true,
            )),
            Err(error) if wal_index_open_failure(&error) => {
                drop(inner);
                Err(FrankenError::BusyRecovery)
            }
            Err(error) => Err(FrankenError::from_rusqlite(error, Some(db_path))),
        }
    }
}

fn strict_readonly_rusqlite_flags(flags: compat::OpenFlags) -> rusqlite::OpenFlags {
    let mut open_flags = flags.to_rusqlite();
    // A strict read-only open strips any write/create bits the caller
    // may have combined — the engine must never create or mutate.
    open_flags.remove(
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
    );
    open_flags.insert(
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    );
    open_flags
}

/// Narrow compatibility view returned by `Connection::as_async()`.
///
/// The engine is synchronous now; this only preserves the
/// `.as_async().in_transaction()` shape used by semantic replay and archive
/// code.
pub struct AsyncView<'conn> {
    pub(super) inner: &'conn rusqlite::Connection,
}

impl AsyncView<'_> {
    /// `true` while SQLite reports a live (non-autocommit) transaction.
    pub fn in_transaction(&self) -> bool {
        !self.inner.is_autocommit()
    }

    /// `true` while the connection is in autocommit mode.
    pub fn is_autocommit(&self) -> bool {
        self.inner.is_autocommit()
    }

    /// Historical in-memory-hydration counter. Under rusqlite every query
    /// materializes through real SQLite rows, so the count is reported as
    /// saturated (`usize::MAX` = "always hydrated").
    pub fn memdb_row_hydration_count(&self) -> usize {
        usize::MAX
    }

    /// Direct access to the underlying rusqlite connection.
    pub fn raw(&self) -> &rusqlite::Connection {
        self.inner
    }
}

impl Clone for AsyncView<'_> {
    fn clone(&self) -> Self {
        *self
    }
}
impl Copy for AsyncView<'_> {}

pub(super) fn row_i64(row: &Row, index: usize) -> i64 {
    match row.get(index) {
        Some(SqliteValue::Integer(value)) => *value,
        Some(SqliteValue::Float(value)) => *value as i64,
        _ => 0,
    }
}

/// RAII helper restoring the original `PRAGMA busy_timeout` after a
/// slice-bounded retry loop rewrote it (`None` = pragma unreadable → do not
/// restore).
pub(super) struct BusyTimeoutRestore<'c> {
    conn: &'c rusqlite::Connection,
    original_ms: Option<i64>,
}

impl<'c> BusyTimeoutRestore<'c> {
    pub(super) fn arm(conn: &'c rusqlite::Connection, original_ms: Option<i64>) -> Self {
        Self { conn, original_ms }
    }
}

impl Drop for BusyTimeoutRestore<'_> {
    fn drop(&mut self) {
        if let Some(original_ms) = self.original_ms {
            let _ = self
                .conn
                .busy_timeout(Duration::from_millis(original_ms.max(0) as u64));
        }
    }
}

// ---------------------------------------------------------------------------
// Strict read-only sidecar validation (ported verbatim from
// franken_agent_detection::connectors::sqlite_sync — proven in production)
// ---------------------------------------------------------------------------

/// The SQLite database file magic: bytes 0..16 of a database header
/// (`SQLite format 3\0`). A file without it is not a database, however an
/// engine build might otherwise treat it.
const SQLITE_HEADER_MAGIC: [u8; 16] = *b"SQLite format 3\0";

/// Length of the standard SQLite database file header in bytes.
const SQLITE_HEADER_PROBE_SIZE: usize = 100;

/// Length of the standard SQLite WAL header in bytes.
///
/// A `-wal` file at or below this length holds no transaction frames
/// (read-only clients commonly create a 0-byte or 32-byte frameless `-wal`).
const SQLITE_WAL_HEADER_SIZE: usize = 32;

/// Documented SQLite WAL header magic values (little-endian / big-endian
/// checksum byte order) and format version `3007000`.
const SQLITE_WAL_MAGIC_LE: u32 = 0x377f_0682;
const SQLITE_WAL_MAGIC_BE: u32 = 0x377f_0683;
const SQLITE_WAL_FORMAT_VERSION: u32 = 3_007_000;

/// Length of the SQLite wal-index header in bytes.
///
/// The `-shm` file starts with two 48-byte copies of the header, so the
/// file must hold at least those 96 bytes; real wal-indexes additionally
/// carry at least one checkpoint-block region, and a file under the
/// documented minimum of 136 bytes can never be a usable wal-index.
const SQLITE_SHM_MIN_SIZE: u64 = 136;
const SQLITE_SHM_HEADER_PAIR: usize = 96;

pub(super) fn sidecar_path(db_path: &Path, suffix: &str) -> PathBuf {
    let mut sidecar = db_path.as_os_str().to_os_string();
    sidecar.push(suffix);
    PathBuf::from(sidecar)
}

/// Whether the sidecar exists at all (any filesystem object).
fn sidecar_exists(path: &Path) -> bool {
    path.symlink_metadata().is_ok()
}

#[cfg(unix)]
const fn read_be_u32(buf: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
    ])
}

#[cfg(unix)]
enum WalHeaderInfo {
    Empty,
    Present {
        len: u64,
        page_size: u32,
        salts: [u8; 8],
    },
}

#[cfg(unix)]
struct ShmHeaderInfo {
    page_size: u32,
    mx_frame: u32,
    salts: [u8; 8],
}

/// Read and validate the `-wal` file's header. A 0-byte `-wal` returns
/// `Ok(Some(WalHeaderInfo::Empty))` (a cleanly checkpointed store); absent,
/// non-regular, sub-header, or header-invalid `-wal` files return `Ok(None)`.
#[cfg(unix)]
fn read_valid_wal_header(wal_path: &Path) -> Result<Option<WalHeaderInfo>, FrankenError> {
    let meta = match std::fs::metadata(wal_path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(FrankenError::Io(error)),
    };
    if !meta.is_file() {
        return Ok(None);
    }
    let mut file = match std::fs::File::open(wal_path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(FrankenError::Io(error)),
    };
    let opened_meta = file.metadata().map_err(FrankenError::Io)?;
    if !opened_meta.is_file() {
        return Ok(None);
    }
    let len = opened_meta.len();
    if len == 0 {
        return Ok(Some(WalHeaderInfo::Empty));
    }
    if len < SQLITE_WAL_HEADER_SIZE as u64 {
        return Ok(None);
    }
    let mut header = [0_u8; SQLITE_WAL_HEADER_SIZE];
    match file.read_exact(&mut header) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(FrankenError::Io(error)),
    }
    let magic = read_be_u32(&header, 0);
    if magic != SQLITE_WAL_MAGIC_LE && magic != SQLITE_WAL_MAGIC_BE {
        return Ok(None);
    }
    if read_be_u32(&header, 4) != SQLITE_WAL_FORMAT_VERSION {
        return Ok(None);
    }
    let page_size = read_be_u32(&header, 8);
    let valid_page_size =
        page_size == 65_536 || (512..65_536).contains(&page_size) && page_size.is_power_of_two();
    if !valid_page_size {
        return Ok(None);
    }
    let salts = [
        header[16], header[17], header[18], header[19], header[20], header[21], header[22],
        header[23],
    ];
    Ok(Some(WalHeaderInfo::Present {
        len,
        page_size,
        salts,
    }))
}

#[cfg(unix)]
fn wal_file_is_valid(wal_path: &Path) -> Result<bool, FrankenError> {
    Ok(read_valid_wal_header(wal_path)?.is_some())
}

/// The SQLite wal-index header checksum: five pairs of native-endian
/// `u32`s over the header's first 40 bytes.
#[cfg(unix)]
fn wal_index_checksum(header_40: &[u8]) -> (u32, u32) {
    let mut s0 = 0_u32;
    let mut s1 = 0_u32;
    for pair in header_40.chunks_exact(8) {
        let x0 = u32::from_ne_bytes([pair[0], pair[1], pair[2], pair[3]]);
        let x1 = u32::from_ne_bytes([pair[4], pair[5], pair[6], pair[7]]);
        s0 = s0.wrapping_add(x0).wrapping_add(s1);
        s1 = s1.wrapping_add(x1).wrapping_add(s0);
    }
    (s0, s1)
}

/// Read and validate the `-shm` wal-index header: a regular file at least
/// [`SQLITE_SHM_MIN_SIZE`] bytes, whose two 48-byte header copies agree and
/// whose first header carries format version `3007000`, the `isInit` marker, a
/// plausible `bigEndCksum` flag, a valid `szPage`, and a matching header
/// checksum.
#[cfg(unix)]
fn read_valid_shm_header(shm_path: &Path) -> Result<Option<ShmHeaderInfo>, FrankenError> {
    let meta = match std::fs::metadata(shm_path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(FrankenError::Io(error)),
    };
    if !meta.is_file() || meta.len() < SQLITE_SHM_MIN_SIZE {
        return Ok(None);
    }
    let mut file = match std::fs::File::open(shm_path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(FrankenError::Io(error)),
    };
    let opened_meta = file.metadata().map_err(FrankenError::Io)?;
    if !opened_meta.is_file() || opened_meta.len() < SQLITE_SHM_MIN_SIZE {
        return Ok(None);
    }
    let mut buf = [0_u8; SQLITE_SHM_HEADER_PAIR];
    match file.read_exact(&mut buf) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(FrankenError::Io(error)),
    }
    drop(file);
    let (h0, h1) = buf.split_at(48);
    if h0 != h1 {
        return Ok(None);
    }
    if u32::from_ne_bytes([h0[0], h0[1], h0[2], h0[3]]) != SQLITE_WAL_FORMAT_VERSION {
        return Ok(None);
    }
    if h0[12] != 1 || h0[13] > 1 {
        return Ok(None);
    }
    let sz_page = u16::from_ne_bytes([h0[14], h0[15]]);
    if !(sz_page == 1 || (sz_page >= 512 && sz_page.is_power_of_two())) {
        return Ok(None);
    }
    let (s0, s1) = wal_index_checksum(&h0[..40]);
    if s0 != u32::from_ne_bytes([h0[40], h0[41], h0[42], h0[43]])
        || s1 != u32::from_ne_bytes([h0[44], h0[45], h0[46], h0[47]])
    {
        return Ok(None);
    }
    let page_size = if sz_page == 1 {
        65_536
    } else {
        u32::from(sz_page)
    };
    let mx_frame = u32::from_ne_bytes([h0[16], h0[17], h0[18], h0[19]]);
    // `WalIndexHdr.aSalt[2]` (bytes 32..40) is `memcpy`'d verbatim from the
    // WAL header's `salt[0..2]` (bytes 16..24).
    let salts = [
        h0[32], h0[33], h0[34], h0[35], h0[36], h0[37], h0[38], h0[39],
    ];
    Ok(Some(ShmHeaderInfo {
        page_size,
        mx_frame,
        salts,
    }))
}

/// Verify that `-wal` and `-shm` are both individually valid and mutually
/// coherent for a `readonly_shm=1` open:
///
/// - A 0-byte `-wal` is only coherent when `shm.mxFrame == 0`.
/// - Otherwise `wal.aSalt == shm.aSalt`, `wal.page_size == shm.page_size`, and
///   `-wal` has at least `32 + mxFrame * (24 + page_size)` bytes (checked `u64`
///   arithmetic so `mxFrame` cannot overflow).
#[cfg(unix)]
fn wal_and_shm_are_coherent(wal_path: &Path, shm_path: &Path) -> Result<bool, FrankenError> {
    let Some(wal) = read_valid_wal_header(wal_path)? else {
        return Ok(false);
    };
    let Some(shm) = read_valid_shm_header(shm_path)? else {
        return Ok(false);
    };
    match wal {
        WalHeaderInfo::Empty => Ok(shm.mx_frame == 0),
        WalHeaderInfo::Present {
            len,
            page_size,
            salts,
        } => {
            if page_size != shm.page_size || salts != shm.salts {
                return Ok(false);
            }
            let Some(frame_bytes) = 24_u64.checked_add(u64::from(page_size)) else {
                return Ok(false);
            };
            let Some(frames_total) = u64::from(shm.mx_frame).checked_mul(frame_bytes) else {
                return Ok(false);
            };
            let Some(min_wal_len) = (SQLITE_WAL_HEADER_SIZE as u64).checked_add(frames_total)
            else {
                return Ok(false);
            };
            Ok(len >= min_wal_len)
        }
    }
}

/// Build a `file:` URI for `db_path` in pure `std` (no `urlencoding`
/// crate): iterate the raw path bytes so non-UTF-8 names survive,
/// collapse consecutive `/` into one, keep `/` itself and every other
/// printable ASCII byte unescaped, and percent-encode `%`, `?`, `#`,
/// every byte `<= 0x20`, and every non-ASCII byte `>= 0x80` —
/// `sqlite3ParseUri` decodes each `%XX` back into the exact raw byte.
#[cfg(unix)]
fn read_only_uri(db_path: &Path) -> String {
    use std::os::unix::ffi::OsStrExt as _;

    const HEX: &[u8; 16] = b"0123456789abcdef";
    let absolute = std::fs::canonicalize(db_path)
        .unwrap_or_else(|_| std::path::absolute(db_path).unwrap_or_else(|_| db_path.to_path_buf()));
    let mut encoded = String::with_capacity(absolute.as_os_str().len() + 16);
    let mut prev_was_slash = false;
    for &byte in absolute.as_os_str().as_bytes() {
        if byte == b'/' {
            if !prev_was_slash {
                encoded.push('/');
            }
            prev_was_slash = true;
            continue;
        }
        prev_was_slash = false;
        if byte <= b' ' || matches!(byte, b'%' | b'?' | b'#') || byte >= 0x80 {
            encoded.push('%');
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
        } else {
            encoded.push(char::from(byte));
        }
    }
    format!("file:{encoded}?mode=ro&readonly_shm=1")
}

/// Whether an open or first-read failure is the WAL-index class a snapshot
/// copy resolves (busy, cannot-open, read-only, or `SQLITE_PROTOCOL` /
/// `FileLockingProtocolFailed`), as opposed to the caller's own problem
/// (SQL errors, missing file, ...).
#[cfg(unix)]
const fn wal_index_open_failure(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code: rusqlite::ErrorCode::DatabaseBusy
                    | rusqlite::ErrorCode::CannotOpen
                    | rusqlite::ErrorCode::ReadOnly
                    | rusqlite::ErrorCode::FileLockingProtocolFailed,
                ..
            },
            ..
        )
    )
}

#[cfg(all(test, unix))]
mod stamp_probe_tests {
    use super::*;

    #[test]
    fn header_stamp_probe_preserves_writer_posix_lock() {
        const CHILD_ENV: &str = "CASS_TEST_HEADER_STAMP_LOCK_PROBE_DB";
        const TEST_NAME: &str =
            "franken_sync::sidecar_probes::stamp_probe_tests::header_stamp_probe_preserves_writer_posix_lock";

        if let Some(probe_db) = std::env::var_os(CHILD_ENV) {
            let probe_str = probe_db.to_string_lossy().into_owned();
            let probe = Connection::open(&probe_str).expect("child open");
            probe
                .execute("PRAGMA busy_timeout = 0")
                .expect("child timeout");
            match probe.execute("BEGIN IMMEDIATE") {
                Ok(_) => {
                    let _ = probe.execute_batch("ROLLBACK");
                    println!("\nLOCK_PROBE=ACQUIRED");
                }
                Err(error) if error.is_busy_class() => {
                    println!("\nLOCK_PROBE=HELD_BY_PARENT");
                }
                Err(error) => panic!("unexpected child lock probe error: {error:?}"),
            }
            return;
        }

        let probe_child = |db_path: &Path| -> String {
            let output = std::process::Command::new(std::env::current_exe().expect("current_exe"))
                .args(["--exact", TEST_NAME, "--test-threads=1", "--nocapture"])
                .env(CHILD_ENV, db_path)
                .output()
                .expect("spawn lock probe child");
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(
                output.status.success(),
                "child lock probe failed ({}):\nstdout:\n{stdout}\nstderr:\n{stderr}",
                output.status
            );
            stdout
                .lines()
                .find_map(|line| {
                    line.split_once("LOCK_PROBE=")
                        .and_then(|(_, rest)| rest.split_whitespace().next())
                })
                .map(str::to_owned)
                .unwrap_or_else(|| panic!("missing LOCK_PROBE line in stdout:\n{stdout}"))
        };

        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("stamp_probe_wal.db");
        let db_str = db_path.to_string_lossy().into_owned();
        let writer = Connection::open(&db_str).expect("open writer");
        writer.execute("PRAGMA journal_mode = WAL;").expect("wal");
        writer
            .execute_batch(
                "PRAGMA application_id = 1128354643;
                 PRAGMA user_version = 1;
                 CREATE TABLE t(x);
                 INSERT INTO t VALUES (1);",
            )
            .expect("seed");
        writer.execute("BEGIN IMMEDIATE;").expect("begin");
        writer.execute("INSERT INTO t VALUES (2);").expect("insert");
        assert_eq!(probe_child(&db_path), "HELD_BY_PARENT");

        assert_eq!(
            probe_sqlite_header_stamp_safely(&db_path).expect("safe stamp probe"),
            SqliteHeaderStamp::Stamp {
                application_id: 0x4341_5353,
                user_version: 1,
            }
        );
        assert_eq!(
            probe_child(&db_path),
            "HELD_BY_PARENT",
            "header stamp probe must not drop writer POSIX lock"
        );

        writer.execute_batch("ROLLBACK;").expect("rollback");
        assert_eq!(probe_child(&db_path), "ACQUIRED");
        writer.close().expect("close");
    }
}

