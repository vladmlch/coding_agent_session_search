use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use super::{Connection, FileIdentity, FrankenError, Row, SqliteValue, compat};

#[derive(Clone, Copy, Default)]
struct LiveDbState {
    count: usize,
    known_wal: Option<bool>,
}

static LIVE_DB_CONNECTIONS: OnceLock<Mutex<HashMap<FileIdentity, LiveDbState>>> = OnceLock::new();

fn live_db_map() -> &'static Mutex<HashMap<FileIdentity, LiveDbState>> {
    LIVE_DB_CONNECTIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(super) fn register_live_connection(id: FileIdentity, known_wal: Option<bool>) {
    let mut map = match live_db_map().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    let entry = map.entry(id).or_default();
    entry.count = entry.count.saturating_add(1);
    if known_wal.is_some() {
        entry.known_wal = known_wal;
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

pub(super) fn unregister_live_connection(id: FileIdentity) {
    let mut map = match live_db_map().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    if let Some(entry) = map.get_mut(&id) {
        entry.count = entry.count.saturating_sub(1);
        if entry.count == 0 {
            map.remove(&id);
        }
    }
}

fn live_connection_state(id: FileIdentity) -> Option<Option<bool>> {
    let map = match live_db_map().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    map.get(&id)
        .filter(|entry| entry.count > 0)
        .map(|entry| entry.known_wal)
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
        let db_id = FileIdentity::from_path(db_path).map_err(FrankenError::Io)?;
        if let Some(known_wal) = db_id.and_then(live_connection_state) {
            return Self::open_strict_readonly_live_process(
                db_path, path, db_id, known_wal, flags,
            );
        }
        let mut header = [0_u8; SQLITE_HEADER_PROBE_SIZE];
        std::fs::File::open(db_path)
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

        // A live rollback journal means the store needs recovery a
        // read-only open must not perform.
        let journal_path = sidecar_path(db_path, "-journal");
        if std::fs::metadata(&journal_path).is_ok_and(|meta| meta.is_file() && meta.len() > 0) {
            let mut first = [0_u8; 1];
            match std::fs::File::open(&journal_path).and_then(|mut f| f.read_exact(&mut first)) {
                Ok(()) if first[0] != 0 => return Err(FrankenError::BusyRecovery),
                Ok(()) => {}
                Err(error) => return Err(FrankenError::Io(error)),
            }
        }

        let is_wal_header = header[18] == 2 || header[19] == 2;
        let wal_path = sidecar_path(db_path, "-wal");
        let shm_path = sidecar_path(db_path, "-shm");
        let wal_exists = sidecar_exists(&wal_path);
        let shm_exists = sidecar_exists(&shm_path);

        let open_flags = strict_readonly_rusqlite_flags(flags);

        // Mode 1 — pure rollback-mode store: no WAL marker anywhere, so a
        // read-only open cannot create `-wal` / `-shm`.
        if !is_wal_header && !wal_exists && !shm_exists {
            let inner = rusqlite::Connection::open_with_flags(path, open_flags)
                .map_err(|error| FrankenError::from_rusqlite(error, Some(db_path)))?;
            return Ok(Self::from_parts(
                inner,
                Some(PathBuf::from(path)),
                db_id,
                Some(false),
            ));
        }

        // Mode 2 (Unix only) — WAL store with both sidecars: validate each
        // byte-for-byte, then open through the `readonly_shm=1` URI.
        #[cfg(unix)]
        if wal_exists && shm_exists {
            if !wal_file_is_valid(&wal_path)? || !shm_file_is_valid(&shm_path)? {
                return Err(FrankenError::BusyRecovery);
            }
            return Self::open_wal_strict_in_place(db_path, path, db_id);
        }
        #[cfg(not(unix))]
        let _ = (wal_exists, shm_exists, open_flags);

        // Trap 1 (and every ambiguous sidecar shape): never let C SQLite
        // near the original path; callers snapshot-copy on BusyRecovery.
        Err(FrankenError::BusyRecovery)
    }

    /// Strict read-only open when this process already has `db_id` open:
    /// avoids `File::open` on `.db`, `-journal`, and `-shm` so existing
    /// in-process POSIX `fcntl` locks stay intact.
    fn open_strict_readonly_live_process(
        db_path: &Path,
        path: &str,
        db_id: Option<FileIdentity>,
        known_wal: Option<bool>,
        flags: compat::OpenFlags,
    ) -> Result<Self, FrankenError> {
        let journal_path = sidecar_path(db_path, "-journal");
        if std::fs::metadata(&journal_path).is_ok_and(|meta| meta.is_file() && meta.len() > 0) {
            return Err(FrankenError::BusyRecovery);
        }

        let wal_path = sidecar_path(db_path, "-wal");
        let shm_path = sidecar_path(db_path, "-shm");
        let wal_exists = sidecar_exists(&wal_path);
        let shm_exists = sidecar_exists(&shm_path);
        let open_flags = strict_readonly_rusqlite_flags(flags);

        if !wal_exists && !shm_exists {
            return match known_wal {
                Some(true) => Err(FrankenError::BusyRecovery),
                Some(false) => {
                    let inner = rusqlite::Connection::open_with_flags(path, open_flags)
                        .map_err(|error| FrankenError::from_rusqlite(error, Some(db_path)))?;
                    Ok(Self::from_parts(
                        inner,
                        Some(PathBuf::from(path)),
                        db_id,
                        Some(false),
                    ))
                }
                None => {
                    #[cfg(unix)]
                    {
                        let uri = read_only_uri(db_path);
                        let uri_flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                            | rusqlite::OpenFlags::SQLITE_OPEN_URI
                            | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
                        let inner = rusqlite::Connection::open_with_flags(uri.as_str(), uri_flags)
                            .map_err(|error| FrankenError::from_rusqlite(error, Some(db_path)))?;
                        match inner
                            .query_row("PRAGMA schema_version;", [], |row| row.get::<usize, i64>(0))
                        {
                            Ok(_) => Ok(Self::from_parts(
                                inner,
                                Some(PathBuf::from(path)),
                                db_id,
                                None,
                            )),
                            Err(rusqlite::Error::SqliteFailure(
                                rusqlite::ffi::Error {
                                    code:
                                        rusqlite::ErrorCode::CannotOpen
                                        | rusqlite::ErrorCode::ReadOnly,
                                    ..
                                },
                                ..,
                            )) => {
                                drop(inner);
                                Err(FrankenError::BusyRecovery)
                            }
                            Err(error) => Err(FrankenError::from_rusqlite(error, Some(db_path))),
                        }
                    }
                    #[cfg(not(unix))]
                    {
                        let _ = open_flags;
                        Err(FrankenError::BusyRecovery)
                    }
                }
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
            return Self::open_wal_strict_in_place(db_path, path, db_id);
        }

        Err(FrankenError::BusyRecovery)
    }

    /// Mode 2 of `open_strict_readonly`: a WAL store whose `-wal` and
    /// `-shm` both exist and validate is opened in place through a
    /// `mode=ro&readonly_shm=1` URI — C SQLite mmaps the wal-index
    /// read-only and takes read marks through locks instead of writing
    /// them: zero bytes and zero mtime changes on all three files.
    #[cfg(unix)]
    fn open_wal_strict_in_place(
        db_path: &Path,
        path: &str,
        db_id: Option<FileIdentity>,
    ) -> Result<Self, FrankenError> {
        let uri = read_only_uri(db_path);
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
                db_id,
                Some(true),
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

/// Whether the `-wal` file's header is internally sane. A 0-byte `-wal` is
/// valid (a cleanly checkpointed store); anything shorter than the 32-byte
/// header, or a header whose magic, format version, or page size is wrong,
/// means the wal-index cannot be trusted and the store must not be opened
/// in place.
#[cfg(unix)]
fn wal_file_is_valid(wal_path: &Path) -> Result<bool, FrankenError> {
    let meta = match std::fs::metadata(wal_path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(FrankenError::Io(error)),
    };
    if !meta.is_file() {
        return Ok(false);
    }
    let len = meta.len();
    if len == 0 {
        return Ok(true);
    }
    if len < SQLITE_WAL_HEADER_SIZE as u64 {
        return Ok(false);
    }
    let mut header = [0_u8; SQLITE_WAL_HEADER_SIZE];
    let mut file = match std::fs::File::open(wal_path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(FrankenError::Io(error)),
    };
    file.read_exact(&mut header).map_err(FrankenError::Io)?;
    let magic = read_be_u32(&header, 0);
    if magic != SQLITE_WAL_MAGIC_LE && magic != SQLITE_WAL_MAGIC_BE {
        return Ok(false);
    }
    if read_be_u32(&header, 4) != SQLITE_WAL_FORMAT_VERSION {
        return Ok(false);
    }
    let page_size = read_be_u32(&header, 8);
    Ok(page_size == 65_536 || (512..65_536).contains(&page_size) && page_size.is_power_of_two())
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

/// Whether the `-shm` file is a coherent wal-index: a regular file at
/// least [`SQLITE_SHM_MIN_SIZE`] bytes, whose two 48-byte header copies
/// agree and whose first header carries format version `3007000`, the
/// `isInit` marker, a plausible `bigEndCksum` flag, a valid `szPage`, and a
/// matching header checksum.
#[cfg(unix)]
fn shm_file_is_valid(shm_path: &Path) -> Result<bool, FrankenError> {
    let meta = match std::fs::metadata(shm_path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(FrankenError::Io(error)),
    };
    if !meta.is_file() || meta.len() < SQLITE_SHM_MIN_SIZE {
        return Ok(false);
    }
    let mut buf = [0_u8; SQLITE_SHM_HEADER_PAIR];
    let mut file = std::fs::File::open(shm_path).map_err(FrankenError::Io)?;
    file.read_exact(&mut buf).map_err(FrankenError::Io)?;
    let (h0, h1) = buf.split_at(48);
    if h0 != h1 {
        return Ok(false);
    }
    if u32::from_ne_bytes([h0[0], h0[1], h0[2], h0[3]]) != SQLITE_WAL_FORMAT_VERSION {
        return Ok(false);
    }
    if h0[12] != 1 || h0[13] > 1 {
        return Ok(false);
    }
    let sz_page = u16::from_ne_bytes([h0[14], h0[15]]);
    if !(sz_page == 1 || (sz_page >= 512 && sz_page.is_power_of_two())) {
        return Ok(false);
    }
    let (s0, s1) = wal_index_checksum(&h0[..40]);
    Ok(s0 == u32::from_ne_bytes([h0[40], h0[41], h0[42], h0[43]])
        && s1 == u32::from_ne_bytes([h0[44], h0[45], h0[46], h0[47]]))
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
/// copy resolves (busy, cannot-open, or read-only), as opposed to the
/// caller's own problem (SQL errors, missing file, ...).
#[cfg(unix)]
const fn wal_index_open_failure(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code: rusqlite::ErrorCode::DatabaseBusy
                    | rusqlite::ErrorCode::CannotOpen
                    | rusqlite::ErrorCode::ReadOnly,
                ..
            },
            ..
        )
    )
}

