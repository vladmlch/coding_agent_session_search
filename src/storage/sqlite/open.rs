//! Safe-open archive stamping for the bundled-rusqlite `agent_search.db`
//! (Task C3).
//!
//! Every storage opener in `src/storage/sqlite.rs` first calls
//! [`archive_disposition`]: an existing non-empty file that does not carry
//! the exact `(application_id = 0x43415353, user_version = 1)` stamp is a
//! legacy or foreign archive and is *refused* with [`SchemaRebuildRequired`].
//! Refusal never modifies, deletes, quarantines, or replaces the archive —
//! the only remedy is an explicit rebuild via `cass index --full`.
//!
//! The probe reads the two identity fields directly out of the 100-byte
//! SQLite database header so it never has to open — and therefore never has
//! to recover, checkpoint, or create `-wal`/`-shm` sidecars on — a refused
//! archive. `meta.schema_version` is probed only as best-effort diagnostic
//! evidence: first through the facade's strict in-place read-only open
//! (which cannot create or mutate anything), then through a same-directory
//! hardlink whose own throwaway sidecars absorb WAL recovery. No code path
//! here uses `?immutable=1`.

use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};

use crate::franken_sync::{
    Connection as FrankenConnection,
    compat::{self, RowExt, open_with_flags as open_franken_with_flags},
};

use super::schema_v1::{BASELINE_SCHEMA_V1_SQL, CASS_APPLICATION_ID, SCHEMA_V1_USER_VERSION};

/// A non-empty on-disk archive that is not stamped as schema v1.
///
/// Raised by every open lane (writable, read-only, strict read-only) before
/// the file is touched by the engine: the archive was left exactly as found —
/// no bytes modified, no file deleted or quarantined, no replacement created.
/// The operator remedy is `cass index --full`, which rebuilds the index
/// archive through the explicitly destructive full-index path.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "agent_search.db rebuild required at {}: {reason} \
     (found application_id={found_application_id:#x}, user_version={found_user_version}, \
     meta schema_version={found_meta_version:?}). \
     Run 'cass index --full' to rebuild the index archive. \
     agent_search.db was NOT modified or deleted.",
    .path.display()
)]
pub struct SchemaRebuildRequired {
    /// Path of the refused archive.
    pub path: PathBuf,
    /// `PRAGMA application_id` found in the 100-byte SQLite header (0 when the
    /// header is unreadable or not a SQLite database).
    pub found_application_id: i64,
    /// `PRAGMA user_version` found in the header.
    pub found_user_version: i64,
    /// `meta.schema_version` when it could be read without mutating the
    /// archive; `None` when the probe could not open it in place or through a
    /// disposable hardlink (e.g. a clean-closed WAL archive on an unwritable
    /// directory).
    pub found_meta_version: Option<i64>,
    /// Human-readable classification of why the stamp is unacceptable.
    pub reason: String,
}

/// What a path holds, as far as the schema-v1 open contract cares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArchiveDisposition {
    /// Missing path, zero-byte file, or `:memory:` — the caller may initialize
    /// a fresh schema-v1 archive (writable lanes) or must fail
    /// non-destructively (read-only lanes).
    Uninitialized,
    /// Existing file carrying the exact `(0x43415353, 1)` stamp.
    Current,
}

/// 100-byte SQLite database header offsets for the two identity pragmas.
const SQLITE_HEADER_LEN: usize = 100;
const SQLITE_HEADER_MAGIC: &[u8; 16] = b"SQLite format 3\0";
const USER_VERSION_OFFSET: usize = 60;
const APPLICATION_ID_OFFSET: usize = 68;

/// Read `(application_id, user_version)` straight from the database header.
///
/// `Ok(None)` covers a file too small to hold a header or whose magic does
/// not match `SQLite format 3` — both are foreign files, never archives the
/// caller may silently reuse.
fn read_archive_stamp(path: &Path) -> io::Result<Option<(i64, i64)>> {
    let mut file = fs::File::open(path)?;
    let mut header = [0_u8; SQLITE_HEADER_LEN];
    match file.read_exact(&mut header) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    if header[..16] != SQLITE_HEADER_MAGIC[..] {
        return Ok(None);
    }
    let user_version = i64::from(i32::from_be_bytes(
        header[USER_VERSION_OFFSET..USER_VERSION_OFFSET + 4]
            .try_into()
            .expect("4-byte field"),
    ));
    let application_id = i64::from(i32::from_be_bytes(
        header[APPLICATION_ID_OFFSET..APPLICATION_ID_OFFSET + 4]
            .try_into()
            .expect("4-byte field"),
    ));
    Ok(Some((application_id, user_version)))
}

/// Classify an existing path for every open lane.
///
/// * `:memory:`, a missing path, or a zero-byte file →
///   [`ArchiveDisposition::Uninitialized`].
/// * Existing non-empty file stamped `(0x43415353, 1)` →
///   [`ArchiveDisposition::Current`].
/// * Anything else → `Err(SchemaRebuildRequired)`: legacy v1-v23
///   FrankenSQLite/SQLite archives (`user_version = 0` with
///   `meta.schema_version` set), archives stamped by a newer schema, foreign
///   SQLite files, and non-SQLite content all refuse identically. Nothing is
///   created or modified either way.
pub(crate) fn archive_disposition(
    path: &Path,
) -> std::result::Result<ArchiveDisposition, SchemaRebuildRequired> {
    if path == Path::new(":memory:") {
        return Ok(ArchiveDisposition::Uninitialized);
    }
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(ArchiveDisposition::Uninitialized);
        }
        Err(error) => {
            return Err(SchemaRebuildRequired {
                path: path.to_path_buf(),
                found_application_id: 0,
                found_user_version: 0,
                found_meta_version: None,
                reason: format!("could not stat archive file ({error})"),
            });
        }
    };
    if !metadata.is_file() || metadata.len() == 0 {
        return Ok(ArchiveDisposition::Uninitialized);
    }

    let stamp = match read_archive_stamp(path) {
        Ok(stamp) => stamp,
        Err(error) => {
            return Err(SchemaRebuildRequired {
                path: path.to_path_buf(),
                found_application_id: 0,
                found_user_version: 0,
                found_meta_version: None,
                reason: format!("could not read the SQLite header ({error})"),
            });
        }
    };
    let (application_id, user_version) = stamp.unwrap_or((0, 0));
    if application_id == CASS_APPLICATION_ID && user_version == SCHEMA_V1_USER_VERSION {
        return Ok(ArchiveDisposition::Current);
    }

    let found_meta_version = probe_meta_schema_version(path);
    Err(SchemaRebuildRequired {
        path: path.to_path_buf(),
        found_application_id: application_id,
        found_user_version: user_version,
        found_meta_version,
        reason: rebuild_reason(stamp, found_meta_version),
    })
}

fn rebuild_reason(stamp: Option<(i64, i64)>, meta_version: Option<i64>) -> String {
    if let Some(version) = meta_version {
        return format!(
            "archive carries legacy cass meta.schema_version {version} and lacks the \
             schema-v1 identity stamp"
        );
    }
    match stamp {
        None => "file is not a SQLite database (short or bad 100-byte header)".to_string(),
        Some((application_id, user_version)) if application_id == CASS_APPLICATION_ID => {
            format!(
                "archive carries the cass application id but user_version {user_version} \
                 != {SCHEMA_V1_USER_VERSION}"
            )
        }
        Some((0, 0)) => {
            "unstamped SQLite archive (application_id=0, user_version=0): a pre-migration \
             cass database that must be rebuilt"
                .to_string()
        }
        Some((_, user_version)) if user_version > SCHEMA_V1_USER_VERSION => {
            format!(
                "archive reports user_version {user_version}, newer than schema \
                 v{SCHEMA_V1_USER_VERSION}"
            )
        }
        Some((application_id, _)) => {
            format!(
                "SQLite archive stamped with foreign application_id {application_id:#x} \
                 is not a cass index database"
            )
        }
    }
}

/// Best-effort `meta.schema_version` evidence for the refusal diagnostic.
///
/// Stage 1 opens the file in place through the facade's strict read-only
/// mode, which by contract cannot create or mutate the db, `-wal`, `-shm`, or
/// `-journal` — covering rollback-journal archives and WAL archives that
/// still carry both sidecars. Stage 2 covers clean-closed WAL archives
/// (WAL header bytes but no sidecars): opening that name in place would
/// fabricate `-shm`/`-wal`, so the probe opens a same-directory hardlink
/// instead — WAL recovery then writes only the link's own throwaway
/// sidecars, and the original inode, mtime, and sidecars stay byte-identical.
fn probe_meta_schema_version(path: &Path) -> Option<i64> {
    probe_meta_schema_version_in_place(path)
        .or_else(|| probe_meta_schema_version_via_hardlink(path))
}

fn read_meta_schema_version(conn: &FrankenConnection) -> Option<i64> {
    let rows = conn
        .query("SELECT value FROM meta WHERE key = 'schema_version';")
        .ok()?;
    let value: String = rows.first()?.get_typed(0).ok()?;
    value.trim().parse().ok()
}

fn probe_meta_schema_version_in_place(path: &Path) -> Option<i64> {
    let conn = open_franken_with_flags(
        &path.to_string_lossy(),
        compat::OpenFlags::SQLITE_OPEN_READ_ONLY | compat::OpenFlags::SQLITE_OPEN_STRICT_READ_ONLY,
    )
    .ok()?;
    read_meta_schema_version(&conn)
}

static PROBE_LINK_NONCE: AtomicU64 = AtomicU64::new(0);

fn probe_meta_schema_version_via_hardlink(path: &Path) -> Option<i64> {
    let parent = path.parent()?;
    let nonce = PROBE_LINK_NONCE.fetch_add(1, Ordering::Relaxed);
    let probe_path = parent.join(format!(
        ".cass_schema_probe.{}.{}.tmp",
        std::process::id(),
        nonce
    ));
    fs::hard_link(path, &probe_path).ok()?;
    let probe_path_str = probe_path.to_str();
    let result = probe_path_str.and_then(|probe_str| {
        open_franken_with_flags(probe_str, compat::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .ok()
            .and_then(|conn| read_meta_schema_version(&conn))
    });
    // Remove the throwaway link and any sidecars WAL recovery created beside
    // it. Best-effort: a leftover probe file is hidden cruft, not damage.
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let _ = fs::remove_file(PathBuf::from(format!("{}{}", probe_path.display(), suffix)));
    }
    result
}

/// Bootstrap a fresh archive to schema v1.
///
/// `PRAGMA application_id`, `PRAGMA user_version`, and the complete baseline
/// schema commit atomically inside one `BEGIN IMMEDIATE` transaction. The
/// caller enables `PRAGMA journal_mode = WAL` only *after* this returns — a
/// crashed bootstrap can therefore never leave a half-stamped WAL archive
/// looking like a valid schema-v1 one. Idempotent by construction
/// (`IF NOT EXISTS` / `INSERT OR REPLACE`), so re-running on a stamped
/// archive is a no-op.
pub(crate) fn initialize_schema_v1(conn: &FrankenConnection) -> Result<()> {
    conn.execute("BEGIN IMMEDIATE;")
        .with_context(|| "beginning schema-v1 bootstrap transaction")?;
    let result = (|| -> Result<()> {
        conn.execute(&format!("PRAGMA application_id = {CASS_APPLICATION_ID};"))
            .with_context(|| "stamping application_id for schema v1")?;
        conn.execute(&format!("PRAGMA user_version = {SCHEMA_V1_USER_VERSION};"))
            .with_context(|| "stamping user_version for schema v1")?;
        conn.execute(BASELINE_SCHEMA_V1_SQL)
            .with_context(|| "creating baseline schema v1")?;
        Ok(())
    })();
    match result {
        Ok(()) => conn
            .execute("COMMIT;")
            .with_context(|| "committing schema-v1 bootstrap transaction")
            .map(|_| ()),
        Err(error) => {
            let _ = conn.execute("ROLLBACK;");
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::sqlite::FrankenStorage;

    /// Byte content, modification time, and sidecar presence of the archive
    /// bundle — the complete mutation surface a refused probe must leave
    /// untouched.
    #[derive(Debug, PartialEq)]
    struct BundleSnapshot {
        bytes: Option<Vec<u8>>,
        mtime: Option<std::time::SystemTime>,
        wal: bool,
        shm: bool,
        journal: bool,
    }

    fn snapshot_bundle(path: &Path) -> BundleSnapshot {
        BundleSnapshot {
            bytes: fs::read(path).ok(),
            mtime: fs::metadata(path).ok().and_then(|m| m.modified().ok()),
            wal: PathBuf::from(format!("{}-wal", path.display())).exists(),
            shm: PathBuf::from(format!("{}-shm", path.display())).exists(),
            journal: PathBuf::from(format!("{}-journal", path.display())).exists(),
        }
    }

    fn pragma_i64(storage: &FrankenStorage, pragma: &str) -> i64 {
        storage
            .raw()
            .query(&format!("PRAGMA {pragma};"))
            .unwrap()
            .first()
            .unwrap()
            .get_typed(0)
            .unwrap()
    }

    /// (a) A zero-byte `agent_search.db` bootstraps to schema v1 with the
    /// application id, user version, meta marker, and WAL all in place.
    #[test]
    fn zero_byte_archive_bootstraps_schema_v1() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agent_search.db");
        fs::File::create(&path).unwrap();

        let storage = FrankenStorage::open(&path).unwrap();

        assert_eq!(pragma_i64(&storage, "application_id"), CASS_APPLICATION_ID);
        assert_eq!(pragma_i64(&storage, "user_version"), SCHEMA_V1_USER_VERSION);
        let journal_mode: String = storage
            .raw()
            .query("PRAGMA journal_mode;")
            .unwrap()
            .first()
            .unwrap()
            .get_typed(0)
            .unwrap();
        assert_eq!(journal_mode.to_ascii_lowercase(), "wal");
        let meta_version: String = storage
            .raw()
            .query("SELECT value FROM meta WHERE key = 'schema_version';")
            .unwrap()
            .first()
            .unwrap()
            .get_typed(0)
            .unwrap();
        assert_eq!(meta_version, "1");
        // Canonical objects exist.
        storage
            .raw()
            .query("SELECT id FROM conversations LIMIT 1;")
            .unwrap();
        storage
            .raw()
            .query("SELECT rowid, content, title FROM fts_messages LIMIT 1;")
            .unwrap();
    }

    /// (b) `:memory:` always bootstraps schema v1 (journal stays `memory`).
    #[test]
    fn memory_archive_bootstraps_schema_v1() {
        let storage = FrankenStorage::open(Path::new(":memory:")).unwrap();
        assert_eq!(pragma_i64(&storage, "application_id"), CASS_APPLICATION_ID);
        assert_eq!(pragma_i64(&storage, "user_version"), SCHEMA_V1_USER_VERSION);
        let meta_version: String = storage
            .raw()
            .query("SELECT value FROM meta WHERE key = 'schema_version';")
            .unwrap()
            .first()
            .unwrap()
            .get_typed(0)
            .unwrap();
        assert_eq!(meta_version, "1");
    }

    /// (c) A legacy v23 archive (`user_version = 0`,
    /// `meta.schema_version = '23'`) is refused on every open lane while its
    /// bytes, mtime, and sidecars stay untouched.
    #[test]
    fn legacy_v23_archive_is_refused_without_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agent_search.db");
        {
            // Build a rollback-journal legacy archive via rusqlite itself.
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "PRAGMA journal_mode = DELETE;
                 CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                 INSERT INTO meta (key, value) VALUES ('schema_version', '23');
                 CREATE TABLE conversations (id INTEGER PRIMARY KEY);
                 INSERT INTO conversations (id) VALUES (1);",
            )
            .unwrap();
        }
        let before = snapshot_bundle(&path);

        let err = FrankenStorage::open(&path).unwrap_err();
        let refusal = err
            .chain()
            .find_map(|cause| cause.downcast_ref::<SchemaRebuildRequired>())
            .expect("open must surface a typed SchemaRebuildRequired");
        assert_eq!(refusal.found_application_id, 0);
        assert_eq!(refusal.found_user_version, 0);
        assert_eq!(refusal.found_meta_version, Some(23));
        let detail = format!("{err:#}");
        assert!(
            detail.contains("cass index --full"),
            "refusal must instruct the operator to run 'cass index --full': {detail}"
        );
        assert!(
            detail.contains("was NOT modified or deleted"),
            "refusal must state the archive was not modified: {detail}"
        );

        // Read-only lanes refuse identically.
        assert!(FrankenStorage::open_readonly(&path).is_err());
        assert!(FrankenStorage::open_strict_readonly(&path).is_err());

        let after = snapshot_bundle(&path);
        assert_eq!(
            before, after,
            "refused legacy archive must keep identical bytes, mtime, and sidecars"
        );
    }

    /// A stamped archive is accepted by `archive_disposition` and re-opening
    /// it does not re-run the bootstrap.
    #[test]
    fn stamped_archive_opens_normally() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agent_search.db");
        fs::File::create(&path).unwrap();
        let storage = FrankenStorage::open(&path).unwrap();
        storage.close().unwrap();
        drop(storage);

        assert!(matches!(
            archive_disposition(&path),
            Ok(ArchiveDisposition::Current)
        ));
        let reopened = FrankenStorage::open(&path).unwrap();
        assert_eq!(
            pragma_i64(&reopened, "user_version"),
            SCHEMA_V1_USER_VERSION
        );
    }

    /// Read-only opening of a missing or zero-byte file fails
    /// non-destructively (no file is created).
    #[test]
    fn readonly_open_of_uninitialized_archive_fails_without_creating() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("agent_search.db");
        assert!(FrankenStorage::open_readonly(&missing).is_err());
        assert!(FrankenStorage::open_strict_readonly(&missing).is_err());
        assert!(!missing.exists(), "readonly open must not create a file");

        let zero = dir.path().join("agent_search.db");
        fs::File::create(&zero).unwrap();
        let before = snapshot_bundle(&zero);
        assert!(FrankenStorage::open_readonly(&zero).is_err());
        assert!(FrankenStorage::open_strict_readonly(&zero).is_err());
        assert_eq!(
            before,
            snapshot_bundle(&zero),
            "zero-byte archive must stay byte-identical after a refused readonly open"
        );
    }
}
