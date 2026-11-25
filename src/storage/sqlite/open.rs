//! Safe-open archive stamping for the bundled-rusqlite `agent_search.db`
//! (Task C3).
//!
//! Every storage opener in `src/storage/sqlite/` first calls
//! [`archive_disposition`] (or [`archive_disposition_with_doctor_guard`] under
//! a held `DoctorMutationDbOpenGuard`): an existing non-empty file that does
//! not carry the exact `(application_id = 0x43415353, user_version = 1)` stamp
//! is a legacy or foreign archive and is *refused* with
//! [`SchemaRebuildRequired`]. Refusal never modifies, deletes, quarantines, or
//! replaces the archive — the only remedy is an explicit rebuild via
//! `cass index --full`.
//!
//! When no `franken_sync::Connection` is open on the archive in this process,
//! the probe reads the two identity fields directly out of the 100-byte SQLite
//! database header (synchronized against `LIVE_DB_CONNECTIONS`) so it never
//! has to open — and therefore never has to recover, checkpoint, or create
//! `-wal`/`-shm` sidecars on — a refused archive. When a connection to that
//! inode IS already live in-process, the probe never calls `fs::File::open`
//! + `close()` on the live database file (which would drop POSIX `fcntl`
//! locks); instead it reads `PRAGMA application_id` / `PRAGMA user_version`
//! through a strict read-only SQLite connection. `meta.schema_version` is
//! probed only as best-effort diagnostic evidence through the facade's strict
//! in-place read-only open (no hardlinks, no `?immutable=1`).

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::franken_sync::{
    Connection as FrankenConnection, SqliteHeaderStamp,
    compat::{self, RowExt, open_with_flags as open_franken_with_flags},
    probe_sqlite_header_stamp_safely, record_live_archive_stamp,
};

use super::open_and_locks::DoctorMutationDbOpenGuard;
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
    /// `meta.schema_version` when it could be read strictly in place without
    /// mutating the archive; `None` when strict read-only open refused (e.g.
    /// a clean-closed WAL archive without `-wal`/`-shm` sidecars).
    pub found_meta_version: Option<i64>,
    /// Human-readable classification of why the stamp is unacceptable.
    pub reason: String,
}

/// What a path holds, as far as the schema-v1 open contract cares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArchiveDisposition {
    /// Missing path (with no legacy sidecars), zero-byte regular file (with no
    /// non-empty legacy `-wal`/`-journal`), or `:memory:` — the caller may
    /// initialize a fresh schema-v1 archive (writable lanes) or must fail
    /// non-destructively (read-only lanes).
    Uninitialized,
    /// Existing file carrying the exact `(0x43415353, 1)` stamp.
    Current,
}

fn sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let mut sidecar = path.as_os_str().to_os_string();
    sidecar.push(suffix);
    PathBuf::from(sidecar)
}

fn rebuild_error(path: &Path, reason: String) -> SchemaRebuildRequired {
    SchemaRebuildRequired {
        path: path.to_path_buf(),
        found_application_id: 0,
        found_user_version: 0,
        found_meta_version: None,
        reason,
    }
}

/// Refuse `Uninitialized` when a missing or zero-byte `.db` is paired with a
/// non-empty (or symlinked/non-regular) `-wal` or `-journal` sidecar.
fn check_uninitialized_sidecars(path: &Path) -> std::result::Result<(), SchemaRebuildRequired> {
    for suffix in ["-wal", "-journal"] {
        let sidecar = sidecar_path(path, suffix);
        let lmeta = match fs::symlink_metadata(&sidecar) {
            Ok(meta) => meta,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(rebuild_error(
                    path,
                    format!(
                        "could not inspect sidecar {} beside uninitialized archive ({error})",
                        sidecar.display()
                    ),
                ));
            }
        };
        if lmeta.file_type().is_symlink() {
            return Err(rebuild_error(
                path,
                format!(
                    "uninitialized archive has symlinked sidecar {} ({})",
                    suffix,
                    sidecar.display()
                ),
            ));
        }
        if !lmeta.is_file() || lmeta.len() > 0 {
            return Err(rebuild_error(
                path,
                format!(
                    "uninitialized or zero-byte archive has non-empty legacy {} sidecar ({}, {} bytes)",
                    suffix,
                    sidecar.display(),
                    lmeta.len()
                ),
            ));
        }
    }
    Ok(())
}

/// Read `(application_id, user_version)` without dropping in-process POSIX
/// locks on a live database file.
fn read_archive_stamp(path: &Path) -> io::Result<Option<(i64, i64)>> {
    match probe_sqlite_header_stamp_safely(path)? {
        SqliteHeaderStamp::NotSqlite => Ok(None),
        SqliteHeaderStamp::Stamp {
            application_id,
            user_version,
        } => Ok(Some((application_id, user_version))),
    }
}

/// Classify an existing path under a held [`DoctorMutationDbOpenGuard`] so
/// doctor repair cannot race between the disposition check and opening the
/// SQLite handle.
pub(crate) fn archive_disposition_with_doctor_guard(
    path: &Path,
    _guard: &DoctorMutationDbOpenGuard,
) -> std::result::Result<ArchiveDisposition, SchemaRebuildRequired> {
    archive_disposition(path)
}

/// Classify an existing path for every open lane.
///
/// * `:memory:`, a missing path without legacy sidecars, or a zero-byte regular
///   file without non-empty `-wal`/`-journal` →
///   [`ArchiveDisposition::Uninitialized`].
/// * Existing non-empty file stamped `(0x43415353, 1)` →
///   [`ArchiveDisposition::Current`].
/// * Anything else — dangling symlinks, non-regular files, zero-byte `.db`
///   paired with non-empty `-wal`/`-journal`, legacy v1-v23 archives
///   (`user_version = 0`), archives stamped by a newer schema, foreign SQLite
///   files, and non-SQLite content — refuses with `Err(SchemaRebuildRequired)`.
///   Nothing is created or modified either way.
pub(crate) fn archive_disposition(
    path: &Path,
) -> std::result::Result<ArchiveDisposition, SchemaRebuildRequired> {
    if path == Path::new(":memory:") {
        return Ok(ArchiveDisposition::Uninitialized);
    }
    let lmeta = match fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            check_uninitialized_sidecars(path)?;
            return Ok(ArchiveDisposition::Uninitialized);
        }
        Err(error) => {
            return Err(rebuild_error(
                path,
                format!("could not stat archive file ({error})"),
            ));
        }
    };
    let metadata = if lmeta.file_type().is_symlink() {
        match fs::metadata(path) {
            Ok(target_meta) => target_meta,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(rebuild_error(
                    path,
                    "archive path is a dangling symlink; refusing to create target file"
                        .to_string(),
                ));
            }
            Err(error) => {
                return Err(rebuild_error(
                    path,
                    format!("could not stat symlinked archive target ({error})"),
                ));
            }
        }
    } else {
        lmeta
    };
    if !metadata.is_file() {
        return Err(rebuild_error(
            path,
            "archive path is not a regular file".to_string(),
        ));
    }
    if metadata.len() == 0 {
        check_uninitialized_sidecars(path)?;
        return Ok(ArchiveDisposition::Uninitialized);
    }

    let stamp = match read_archive_stamp(path) {
        Ok(stamp) => stamp,
        Err(error) => {
            return Err(rebuild_error(
                path,
                format!("could not read the SQLite header ({error})"),
            ));
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
/// Opens the file in place strictly through the facade's strict read-only
/// mode, which by contract cannot create or mutate the db, `-wal`, `-shm`, or
/// `-journal`. If strict read-only refuses (for example a WAL archive missing
/// `-shm`), returns `None` without creating hardlinks or throwaway sidecars.
fn probe_meta_schema_version(path: &Path) -> Option<i64> {
    probe_meta_schema_version_in_place(path)
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
        Ok(()) => {
            conn.execute("COMMIT;")
                .with_context(|| "committing schema-v1 bootstrap transaction")?;
            if let Ok(Some(id)) = conn.file_identity() {
                record_live_archive_stamp(id, CASS_APPLICATION_ID, SCHEMA_V1_USER_VERSION);
            }
            Ok(())
        }
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
            wal: sidecar_path(path, "-wal").exists(),
            shm: sidecar_path(path, "-shm").exists(),
            journal: sidecar_path(path, "-journal").exists(),
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

        let err = FrankenStorage::open(&path)
            .err()
            .expect("open must refuse legacy archive");
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

    /// P0-7: A zero-byte `.db` paired with a non-empty legacy `-wal` or
    /// `-journal` must refuse with `SchemaRebuildRequired` (never
    /// `Uninitialized`) without modifying the files.
    #[test]
    fn zero_byte_db_with_nonempty_legacy_sidecars_refuses_schema_rebuild_required() {
        for suffix in ["-wal", "-journal"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("agent_search.db");
            fs::File::create(&path).unwrap();
            let sidecar = sidecar_path(&path, suffix);
            fs::write(&sidecar, b"non-empty-legacy-sidecar-bytes").unwrap();
            let before = snapshot_bundle(&path);

            let err = archive_disposition(&path).expect_err("must refuse");
            assert!(
                err.reason.contains(suffix),
                "refusal should name {suffix}: {}",
                err.reason
            );
            assert!(FrankenStorage::open(&path).is_err());
            assert_eq!(
                before,
                snapshot_bundle(&path),
                "zero-byte db and {suffix} must remain untouched"
            );
        }
    }

    /// P0-7: A dangling symlink must refuse with `SchemaRebuildRequired` and
    /// never cause `open()` to create the symlink's target file.
    #[cfg(unix)]
    #[test]
    fn dangling_symlink_refuses_without_creating_target() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("missing_target.db");
        let link = dir.path().join("agent_search.db");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let err = archive_disposition(&link).expect_err("dangling symlink must refuse");
        assert!(
            err.reason.contains("dangling symlink"),
            "unexpected refusal reason: {}",
            err.reason
        );
        assert!(FrankenStorage::open(&link).is_err());
        assert!(
            !target.exists(),
            "dangling symlink target must never be created"
        );
        assert!(fs::symlink_metadata(&link).is_ok());
    }

    /// P0-8: Probing `archive_disposition` while an in-process writer holds an
    /// open transaction lock must NOT drop the writer's POSIX `fcntl` lock.
    #[cfg(unix)]
    #[test]
    fn archive_disposition_preserves_writer_posix_lock() {
        const CHILD_ENV: &str = "CASS_TEST_OPEN_PROBE_LOCK_DB";
        const TEST_NAME: &str =
            "storage::sqlite::open::tests::archive_disposition_preserves_writer_posix_lock";

        if let Some(probe_db) = std::env::var_os(CHILD_ENV) {
            let probe_str = probe_db.to_string_lossy().into_owned();
            let probe = FrankenConnection::open(&probe_str).expect("child open");
            probe
                .execute("PRAGMA busy_timeout = 0")
                .expect("child timeout");
            match probe.execute("BEGIN IMMEDIATE") {
                Ok(_) => {
                    let _ = probe.execute_batch("ROLLBACK");
                    println!("LOCK_PROBE=ACQUIRED");
                }
                Err(error)
                    if matches!(
                        error,
                        crate::franken_sync::FrankenError::Busy
                            | crate::franken_sync::FrankenError::BusyRecovery
                            | crate::franken_sync::FrankenError::BusySnapshot { .. }
                            | crate::franken_sync::FrankenError::DatabaseLocked { .. }
                            | crate::franken_sync::FrankenError::LockFailed { .. }
                    ) =>
                {
                    println!("LOCK_PROBE=HELD_BY_PARENT");
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
                .find_map(|line| line.strip_prefix("LOCK_PROBE="))
                .map(str::to_owned)
                .unwrap_or_else(|| panic!("missing LOCK_PROBE line in stdout:\n{stdout}"))
        };

        let dir = tempfile::tempdir().unwrap();

        // 1) Rollback (DELETE) mode writer holding BEGIN IMMEDIATE.
        let rollback_db = dir.path().join("rollback_probe_lock.db");
        let rollback_str = rollback_db.to_string_lossy().into_owned();
        let rb_writer = FrankenConnection::open(&rollback_str).expect("open rollback writer");
        rb_writer
            .execute("PRAGMA journal_mode = DELETE;")
            .expect("delete mode");
        initialize_schema_v1(&rb_writer).expect("init v1");
        rb_writer
            .execute("BEGIN IMMEDIATE;")
            .expect("begin immediate rollback");
        assert_eq!(probe_child(&rollback_db), "HELD_BY_PARENT");
        assert!(matches!(
            archive_disposition(&rollback_db),
            Ok(ArchiveDisposition::Current)
        ));
        assert_eq!(
            probe_child(&rollback_db),
            "HELD_BY_PARENT",
            "archive_disposition must preserve rollback writer POSIX lock"
        );
        rb_writer.execute_batch("ROLLBACK;").expect("rollback");
        assert_eq!(probe_child(&rollback_db), "ACQUIRED");
        rb_writer.close().expect("close rollback writer");

        // 2) WAL mode writer holding BEGIN IMMEDIATE with uncommitted insert.
        let wal_db = dir.path().join("wal_probe_lock.db");
        let wal_storage = FrankenStorage::open(&wal_db).expect("open wal storage");
        wal_storage
            .raw()
            .execute("BEGIN IMMEDIATE;")
            .expect("begin immediate wal");
        wal_storage
            .raw()
            .execute(
                "INSERT INTO agents (id, slug, name, kind, created_at, updated_at)
                 VALUES (1, 'codex', 'Codex', 'local', 1, 1);",
            )
            .expect("insert under wal lock");
        assert_eq!(probe_child(&wal_db), "HELD_BY_PARENT");
        assert!(matches!(
            archive_disposition(&wal_db),
            Ok(ArchiveDisposition::Current)
        ));
        assert_eq!(
            probe_child(&wal_db),
            "HELD_BY_PARENT",
            "archive_disposition must preserve WAL writer POSIX lock"
        );
        wal_storage
            .raw()
            .execute_batch("ROLLBACK;")
            .expect("rollback wal");
        assert_eq!(probe_child(&wal_db), "ACQUIRED");
        wal_storage.close().expect("close wal storage");
    }
}
