use super::*;
use crate::franken_sync::compat::{
    ConnectionExt as FrankenConnectionExt, ParamValue, RowExt as FrankenRowExt,
};
#[cfg(unix)]
use anyhow::Result;
use anyhow::anyhow;
#[cfg(unix)]
use std::fs;
#[cfg(unix)]
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::process::{Command, Stdio};
use std::time::Duration;
#[cfg(unix)]
use std::time::Instant;
use tempfile::TempDir;

// =========================================================================
// User data file protection tests (bead yln.4)
// =========================================================================

#[test]
fn is_user_data_file_detects_bookmarks() {
    assert!(is_user_data_file(Path::new("/data/bookmarks.db")));
    assert!(is_user_data_file(Path::new("bookmarks.db")));
}

#[test]
fn is_user_data_file_detects_tui_state() {
    assert!(is_user_data_file(Path::new("/data/tui_state.json")));
}

#[test]
fn is_user_data_file_detects_sources_toml() {
    assert!(is_user_data_file(Path::new("/config/sources.toml")));
}

#[test]
fn is_user_data_file_detects_env() {
    assert!(is_user_data_file(Path::new(".env")));
}

#[test]
fn is_user_data_file_rejects_other_files() {
    assert!(!is_user_data_file(Path::new("index.db")));
    assert!(!is_user_data_file(Path::new("conversations.db")));
    assert!(!is_user_data_file(Path::new("random.txt")));
}

// =========================================================================
// Backup creation tests (bead yln.4)
// =========================================================================

#[test]
fn create_backup_returns_none_for_nonexistent() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("nonexistent.db");
    let result = create_backup(&db_path).unwrap();
    assert!(result.is_none());
}

#[test]
fn create_backup_creates_named_file() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    std::fs::write(&db_path, b"test data").unwrap();

    let backup_path = create_backup(&db_path).unwrap();
    assert!(backup_path.is_some());
    let backup = backup_path.unwrap();
    assert!(backup.exists());
    assert!(
        backup
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .contains("backup")
    );
}

#[test]
fn create_backup_paths_are_unique() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    std::fs::write(&db_path, b"test data").unwrap();

    let first = create_backup(&db_path).unwrap().unwrap();
    let second = create_backup(&db_path).unwrap().unwrap();

    assert_ne!(first, second);
    assert!(first.exists());
    assert!(second.exists());
}

#[test]
fn schema_check_rebuild_classification_ignores_transient_errors() {
    assert!(!schema_check_error_requires_rebuild(
        &crate::franken_sync::FrankenError::Busy
    ));
    assert!(!schema_check_error_requires_rebuild(
        &crate::franken_sync::FrankenError::DatabaseLocked {
            path: PathBuf::from("/tmp/test.db"),
        }
    ));
    assert!(!schema_check_error_requires_rebuild(
        &crate::franken_sync::FrankenError::CannotOpen {
            path: PathBuf::from("/tmp/test.db"),
        }
    ));
    assert!(!schema_check_error_requires_rebuild(
        &crate::franken_sync::FrankenError::Io(std::io::Error::other("disk hiccup"))
    ));
}

#[test]
fn schema_check_rebuild_classification_keeps_corruption_errors() {
    assert!(schema_check_error_requires_rebuild(
        &crate::franken_sync::FrankenError::DatabaseCorrupt {
            detail: "bad header".to_string(),
        }
    ));
    assert!(schema_check_error_requires_rebuild(
        &crate::franken_sync::FrankenError::WalCorrupt {
            detail: "bad wal".to_string(),
        }
    ));
    assert!(schema_check_error_requires_rebuild(
        &crate::franken_sync::FrankenError::NotADatabase {
            path: PathBuf::from("/tmp/test.db"),
        }
    ));
    assert!(schema_check_error_requires_rebuild(
        &crate::franken_sync::FrankenError::ShortRead {
            expected: 4096,
            actual: 64,
        }
    ));
}

#[test]
fn create_backup_refuses_raw_copy_after_retryable_vacuum_errors() {
    let retryable_errors = [
        crate::franken_sync::FrankenError::Busy,
        crate::franken_sync::FrankenError::BusyRecovery,
        crate::franken_sync::FrankenError::BusySnapshot {
            conflicting_pages: "1,2".to_string(),
        },
        crate::franken_sync::FrankenError::DatabaseLocked {
            path: PathBuf::from("/tmp/test.db"),
        },
        crate::franken_sync::FrankenError::LockFailed {
            detail: "fcntl lock still held".to_string(),
        },
        crate::franken_sync::FrankenError::WriteConflict { page: 7, holder: 9 },
        crate::franken_sync::FrankenError::SerializationFailure { page: 11 },
        crate::franken_sync::FrankenError::Internal("database is locked".to_string()),
    ];

    for err in retryable_errors {
        assert!(
            backup_vacuum_error_requires_consistent_retry(&err),
            "retryable VACUUM failure must not fall back to raw bundle copy: {err}"
        );
    }

    assert!(!backup_vacuum_error_requires_consistent_retry(
        &crate::franken_sync::FrankenError::NotADatabase {
            path: PathBuf::from("/tmp/test.db")
        }
    ));
    assert!(!backup_vacuum_error_requires_consistent_retry(
        &crate::franken_sync::FrankenError::DatabaseCorrupt {
            detail: "bad header".to_string()
        }
    ));
}

#[test]
fn create_backup_uses_hidden_vacuum_stage_path() {
    let backup_path = PathBuf::from("/tmp/test.db.backup.123.456.0");
    let stage_path = vacuum_stage_backup_path(&backup_path);
    let stage_name = stage_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();

    assert!(stage_name.starts_with('.'));
    assert!(stage_name.ends_with(".vacuum-in-progress"));
    assert!(
        !is_backup_root_name(stage_name, "test.db.backup."),
        "incomplete VACUUM output must not be discoverable as a backup root"
    );
}

#[test]
fn create_backup_preserves_content() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let original_content = b"test database content 12345";
    std::fs::write(&db_path, original_content).unwrap();

    let backup_path = create_backup(&db_path).unwrap().unwrap();
    let backup_content = std::fs::read(&backup_path).unwrap();
    assert_eq!(backup_content, original_content);
}

#[test]
fn create_backup_copies_sidecars_when_present() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    std::fs::write(&db_path, b"db").unwrap();
    std::fs::write(database_sidecar_path(&db_path, "-wal"), b"wal").unwrap();
    std::fs::write(database_sidecar_path(&db_path, "-shm"), b"shm").unwrap();

    let backup_path = create_backup(&db_path).unwrap().unwrap();

    assert_eq!(
        std::fs::read(database_sidecar_path(&backup_path, "-wal")).unwrap(),
        b"wal"
    );
    assert_eq!(
        std::fs::read(database_sidecar_path(&backup_path, "-shm")).unwrap(),
        b"shm"
    );
}

#[test]
#[cfg(unix)]
fn create_backup_rejects_symlink_root_during_raw_fallback() {
    use std::os::unix::fs::symlink;

    let dir = TempDir::new().unwrap();
    let outside_db = dir.path().join("outside.db");
    let db_path = dir.path().join("test.db");
    std::fs::write(&outside_db, b"not sqlite").unwrap();
    symlink(&outside_db, &db_path).unwrap();

    let err = create_backup(&db_path).unwrap_err();

    assert!(
        err.to_string().contains("bundle symlink"),
        "unexpected error: {err:#}"
    );
    assert_eq!(std::fs::read(&outside_db).unwrap(), b"not sqlite");
    let backup_roots: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with("test.db.backup."))
        .collect();
    assert!(
        backup_roots.is_empty(),
        "symlinked backup source must not publish backup roots: {backup_roots:?}"
    );
}

#[test]
#[cfg(unix)]
fn create_backup_rejects_symlink_sidecar_without_partial_backup() {
    use std::os::unix::fs::symlink;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let outside_wal = dir.path().join("outside.wal");
    let wal_path = database_sidecar_path(&db_path, "-wal");
    std::fs::write(&db_path, b"not sqlite").unwrap();
    std::fs::write(&outside_wal, b"outside wal").unwrap();
    symlink(&outside_wal, &wal_path).unwrap();

    let err = create_backup(&db_path).unwrap_err();

    assert!(
        err.to_string().contains("bundle symlink"),
        "unexpected error: {err:#}"
    );
    assert_eq!(std::fs::read(&outside_wal).unwrap(), b"outside wal");
    let backup_roots: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with("test.db.backup."))
        .collect();
    assert!(
        backup_roots.is_empty(),
        "sidecar preflight failure must not leave a partial backup root: {backup_roots:?}"
    );
}

// =========================================================================
// Backup cleanup tests (bead yln.4)
// =========================================================================

#[test]
fn cleanup_old_backups_keeps_recent() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    // Create 5 backup files with different timestamps
    for i in 0..5 {
        let backup_name = format!("test.db.backup.{}", 1000 + i);
        std::fs::write(dir.path().join(&backup_name), format!("backup {i}")).unwrap();
    }

    cleanup_old_backups(&db_path, 3).unwrap();

    // Count remaining backup files
    let backups: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_str().unwrap_or("").contains("backup"))
        .collect();

    assert_eq!(backups.len(), 3);
}

#[test]
fn cleanup_old_backups_ignores_wal_and_shm_sidecars() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    for i in 0..3 {
        let backup_name = format!("test.db.backup.{}", 1000 + i);
        let backup_path = dir.path().join(&backup_name);
        std::fs::write(&backup_path, format!("backup {i}")).unwrap();
        std::fs::write(format!("{}-wal", backup_path.display()), b"wal").unwrap();
        std::fs::write(format!("{}-shm", backup_path.display()), b"shm").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
    }

    cleanup_old_backups(&db_path, 2).unwrap();

    let mut roots = Vec::new();
    let mut wals = Vec::new();
    let mut shms = Vec::new();
    for entry in std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
    {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.ends_with("-wal") {
            wals.push(name);
        } else if name.ends_with("-shm") {
            shms.push(name);
        } else if name.contains("backup") {
            roots.push(name);
        }
    }

    assert_eq!(roots.len(), 2, "should keep two backup roots");
    assert_eq!(
        wals.len(),
        2,
        "should keep WAL sidecars only for retained backups"
    );
    assert_eq!(
        shms.len(),
        2,
        "should keep SHM sidecars only for retained backups"
    );
}

#[test]
fn move_database_bundle_moves_database_and_sidecars() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let backup_path = dir.path().join("test.db.corrupt");

    std::fs::write(&db_path, b"db").unwrap();
    std::fs::write(database_sidecar_path(&db_path, "-wal"), b"wal").unwrap();
    std::fs::write(database_sidecar_path(&db_path, "-shm"), b"shm").unwrap();

    let moved = move_database_bundle(&db_path, &backup_path).unwrap();
    assert_eq!(
        moved,
        DatabaseBundleMoveResult {
            database: true,
            wal: true,
            shm: true
        }
    );
    assert!(moved.moved_any());

    assert!(!db_path.exists());
    assert!(!database_sidecar_path(&db_path, "-wal").exists());
    assert!(!database_sidecar_path(&db_path, "-shm").exists());

    assert_eq!(std::fs::read(&backup_path).unwrap(), b"db");
    assert_eq!(
        std::fs::read(database_sidecar_path(&backup_path, "-wal")).unwrap(),
        b"wal"
    );
    assert_eq!(
        std::fs::read(database_sidecar_path(&backup_path, "-shm")).unwrap(),
        b"shm"
    );
}

#[test]
fn move_database_bundle_preserves_orphan_sidecars_without_main_db() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let backup_path = dir.path().join("test.db.corrupt");

    std::fs::write(database_sidecar_path(&db_path, "-wal"), b"wal").unwrap();
    std::fs::write(database_sidecar_path(&db_path, "-shm"), b"shm").unwrap();

    let moved = move_database_bundle(&db_path, &backup_path).unwrap();
    assert_eq!(
        moved,
        DatabaseBundleMoveResult {
            database: false,
            wal: true,
            shm: true
        }
    );
    assert!(moved.moved_any());
    assert!(!db_path.exists());
    assert!(!database_sidecar_path(&db_path, "-wal").exists());
    assert!(!database_sidecar_path(&db_path, "-shm").exists());
    assert_eq!(
        std::fs::read(database_sidecar_path(&backup_path, "-wal")).unwrap(),
        b"wal"
    );
    assert_eq!(
        std::fs::read(database_sidecar_path(&backup_path, "-shm")).unwrap(),
        b"shm"
    );
}

#[test]
#[cfg(unix)]
fn move_database_bundle_moves_dangling_symlink_database_root() {
    use std::os::unix::fs::symlink;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let backup_path = dir.path().join("test.db.corrupt");
    let missing_target = dir.path().join("missing-target.db");

    symlink(&missing_target, &db_path).unwrap();

    let moved = move_database_bundle(&db_path, &backup_path).unwrap();

    assert_eq!(
        moved,
        DatabaseBundleMoveResult {
            database: true,
            wal: false,
            shm: false
        }
    );
    assert!(std::fs::symlink_metadata(&db_path).is_err());
    assert!(
        std::fs::symlink_metadata(&backup_path)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert!(!missing_target.exists());
}

#[test]
#[cfg(unix)]
fn move_database_bundle_moves_dangling_symlink_sidecars_without_main_db() {
    use std::os::unix::fs::symlink;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let backup_path = dir.path().join("test.db.corrupt");
    let missing_wal_target = dir.path().join("missing-wal");
    let missing_shm_target = dir.path().join("missing-shm");
    let wal_path = database_sidecar_path(&db_path, "-wal");
    let shm_path = database_sidecar_path(&db_path, "-shm");

    symlink(&missing_wal_target, &wal_path).unwrap();
    symlink(&missing_shm_target, &shm_path).unwrap();

    let moved = move_database_bundle(&db_path, &backup_path).unwrap();

    assert_eq!(
        moved,
        DatabaseBundleMoveResult {
            database: false,
            wal: true,
            shm: true
        }
    );
    assert!(std::fs::symlink_metadata(&wal_path).is_err());
    assert!(std::fs::symlink_metadata(&shm_path).is_err());
    assert!(
        std::fs::symlink_metadata(database_sidecar_path(&backup_path, "-wal"))
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert!(
        std::fs::symlink_metadata(database_sidecar_path(&backup_path, "-shm"))
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert!(!missing_wal_target.exists());
    assert!(!missing_shm_target.exists());
}

#[test]
fn copy_database_bundle_copies_database_and_sidecars() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let copied_path = dir.path().join("copy.db");

    std::fs::write(&db_path, b"db").unwrap();
    std::fs::write(database_sidecar_path(&db_path, "-wal"), b"wal").unwrap();
    std::fs::write(database_sidecar_path(&db_path, "-shm"), b"shm").unwrap();

    copy_database_bundle(&db_path, &copied_path).unwrap();

    assert_eq!(std::fs::read(&copied_path).unwrap(), b"db");
    assert_eq!(
        std::fs::read(database_sidecar_path(&copied_path, "-wal")).unwrap(),
        b"wal"
    );
    assert_eq!(
        std::fs::read(database_sidecar_path(&copied_path, "-shm")).unwrap(),
        b"shm"
    );
    assert_eq!(std::fs::read(&db_path).unwrap(), b"db");
}

#[test]
fn copy_database_bundle_creates_destination_parent() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let copied_path = dir.path().join("nested/copies/copy.db");

    std::fs::write(&db_path, b"db").unwrap();
    std::fs::write(database_sidecar_path(&db_path, "-wal"), b"wal").unwrap();

    copy_database_bundle(&db_path, &copied_path).unwrap();

    assert!(copied_path.parent().unwrap().is_dir());
    assert_eq!(std::fs::read(&copied_path).unwrap(), b"db");
    assert_eq!(
        std::fs::read(database_sidecar_path(&copied_path, "-wal")).unwrap(),
        b"wal"
    );
}

#[test]
#[cfg(unix)]
fn copy_database_bundle_rejects_symlink_source_root() {
    use std::os::unix::fs::symlink;

    let dir = TempDir::new().unwrap();
    let outside_db = dir.path().join("outside.db");
    let db_path = dir.path().join("test.db");
    let copied_path = dir.path().join("copy.db");

    std::fs::write(&outside_db, b"outside").unwrap();
    symlink(&outside_db, &db_path).unwrap();

    let err = copy_database_bundle(&db_path, &copied_path).unwrap_err();

    assert!(
        err.to_string().contains("bundle symlink"),
        "unexpected error: {err:#}"
    );
    assert!(!copied_path.exists());
    assert_eq!(std::fs::read(&outside_db).unwrap(), b"outside");
}

#[test]
#[cfg(unix)]
fn copy_database_bundle_rejects_symlink_sidecar() {
    use std::os::unix::fs::symlink;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let copied_path = dir.path().join("copy.db");
    let outside_wal = dir.path().join("outside.wal");
    let wal_path = database_sidecar_path(&db_path, "-wal");

    std::fs::write(&db_path, b"db").unwrap();
    std::fs::write(&outside_wal, b"outside wal").unwrap();
    symlink(&outside_wal, &wal_path).unwrap();

    let err = copy_database_bundle(&db_path, &copied_path).unwrap_err();

    assert!(
        err.to_string().contains("bundle symlink"),
        "unexpected error: {err:#}"
    );
    assert_eq!(std::fs::read(&outside_wal).unwrap(), b"outside wal");
    assert!(!copied_path.exists());
    assert!(!database_sidecar_path(&copied_path, "-wal").exists());
}

#[test]
fn move_database_bundle_creates_destination_parent_and_moves_sidecars() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let backup_path = dir.path().join("nested/backups/test.db.corrupt");

    std::fs::write(&db_path, b"db").unwrap();
    std::fs::write(database_sidecar_path(&db_path, "-wal"), b"wal").unwrap();
    std::fs::write(database_sidecar_path(&db_path, "-shm"), b"shm").unwrap();

    let moved = move_database_bundle(&db_path, &backup_path).unwrap();
    assert_eq!(
        moved,
        DatabaseBundleMoveResult {
            database: true,
            wal: true,
            shm: true
        }
    );
    assert!(backup_path.parent().unwrap().is_dir());
    assert_eq!(std::fs::read(&backup_path).unwrap(), b"db");
    assert_eq!(
        std::fs::read(database_sidecar_path(&backup_path, "-wal")).unwrap(),
        b"wal"
    );
    assert_eq!(
        std::fs::read(database_sidecar_path(&backup_path, "-shm")).unwrap(),
        b"shm"
    );
}

#[test]
fn remove_database_files_removes_orphan_sidecars_without_main_db() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    std::fs::write(database_sidecar_path(&db_path, "-wal"), b"wal").unwrap();
    std::fs::write(database_sidecar_path(&db_path, "-shm"), b"shm").unwrap();

    remove_database_files(&db_path).unwrap();

    assert!(!db_path.exists());
    assert!(!database_sidecar_path(&db_path, "-wal").exists());
    assert!(!database_sidecar_path(&db_path, "-shm").exists());
}

#[test]
fn cleanup_old_backups_ignores_backup_named_directories() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    for i in 0..3 {
        let backup_name = format!("test.db.backup.{}", 1000 + i);
        std::fs::write(dir.path().join(&backup_name), format!("backup {i}")).unwrap();
    }
    std::fs::create_dir(dir.path().join("test.db.backup.directory")).unwrap();

    cleanup_old_backups(&db_path, 2).unwrap();

    let mut backup_files = Vec::new();
    let mut backup_dirs = Vec::new();
    for entry in std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
    {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.starts_with("test.db.backup.") {
            continue;
        }
        if entry.path().is_dir() {
            backup_dirs.push(name);
        } else {
            backup_files.push(name);
        }
    }

    assert_eq!(
        backup_files.len(),
        2,
        "only real backup files count toward retention"
    );
    assert_eq!(
        backup_dirs.len(),
        1,
        "backup-named directories should be ignored"
    );
}

// =========================================================================
// Storage open/create tests (bead yln.4)
// =========================================================================

#[test]
fn open_creates_new_database() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("new.db");
    assert!(!db_path.exists());

    let storage = SqliteStorage::open(&db_path).unwrap();
    assert!(db_path.exists());
    storage.close().unwrap();
}

#[test]
fn open_readonly_fails_for_nonexistent() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("nonexistent.db");
    let result = SqliteStorage::open_readonly(&db_path);
    assert!(result.is_err());
}

#[test]
fn open_readonly_succeeds_for_existing() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("existing.db");

    // Create first
    let _storage = SqliteStorage::open(&db_path).unwrap();
    drop(_storage);

    // Now open readonly
    let storage = SqliteStorage::open_readonly(&db_path).unwrap();
    assert!(storage.schema_version().is_ok());
}

// gh #389: dirty-WAL recovery must never fire without a non-empty WAL
// sidecar, and must never fire on retryable (live-writer) failures.
#[test]
fn dirty_wal_recovery_declines_without_wal_sidecar() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("no-wal.db");
    drop(SqliteStorage::open(&db_path).unwrap());
    // A clean close leaves no (or an empty) WAL: recovery must decline.
    let wal_path = database_sidecar_path(&db_path, "-wal");
    if wal_path.exists() {
        std::fs::write(&wal_path, b"").unwrap();
    }
    let hard_err = anyhow!("unable to open database file");
    assert!(!attempt_dirty_wal_recovery_checkpoint(&db_path, &hard_err));
}

#[test]
fn dirty_wal_recovery_declines_on_retryable_error() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("busy.db");
    drop(SqliteStorage::open(&db_path).unwrap());
    let wal_path = database_sidecar_path(&db_path, "-wal");
    std::fs::write(&wal_path, vec![0_u8; 4096]).unwrap();
    // A busy/lock-class failure is a live writer, not a dirty WAL: the
    // recovery write-open must not be attempted at all.
    let busy_err = anyhow!("database is busy (snapshot conflict on pages: 12)");
    assert!(!attempt_dirty_wal_recovery_checkpoint(&db_path, &busy_err));
    // The fabricated WAL is untouched (recovery never opened the DB).
    assert_eq!(
        std::fs::symlink_metadata(&wal_path).unwrap().len(),
        4096,
        "recovery must not touch the WAL on a retryable failure"
    );
}

#[test]
fn dirty_wal_recovery_checkpoints_nonempty_wal() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("dirty.db");
    // Write real rows, then close WITHOUT the final checkpoint so the WAL
    // keeps its unreplayed frames — the unclean-shutdown artifact from
    // gh #389.
    {
        let mut conn =
            crate::franken_sync::Connection::open(db_path.to_string_lossy().into_owned()).unwrap();
        conn.execute("CREATE TABLE t (x INTEGER);").unwrap();
        conn.execute("INSERT INTO t (x) VALUES (1), (2), (3);")
            .unwrap();
        close_franken_in_place_with_busy_retry(&mut conn, false).unwrap();
    }
    let wal_path = database_sidecar_path(&db_path, "-wal");
    let dirty_len = std::fs::symlink_metadata(&wal_path)
        .map(|meta| meta.len())
        .unwrap_or(0);
    assert!(
        dirty_len > 32,
        "precondition: close-without-checkpoint must leave a WAL with frames \
         (len={dirty_len}; engine behavior changed; rework this test)"
    );
    let hard_err = anyhow!("unable to open database file");
    assert!(
        attempt_dirty_wal_recovery_checkpoint(&db_path, &hard_err),
        "recovery must checkpoint a present non-empty WAL"
    );
    // TRUNCATE checkpoint drains the WAL frames; fsqlite retains the
    // 32-byte header, so "no frames left" is `len <= 32`. The rows
    // survive in the main DB.
    let truncated_len = std::fs::symlink_metadata(&wal_path)
        .map(|meta| meta.len())
        .unwrap_or(0);
    assert!(
        truncated_len <= 32,
        "checkpoint(TRUNCATE) must leave a frame-free WAL (len={truncated_len}, was {dirty_len})"
    );
    let storage = SqliteStorage::open_readonly(&db_path);
    // The DB has no cass schema, so open_readonly may object to schema
    // shape — but the raw rows must be readable through a raw connection.
    drop(storage);
    let conn =
        crate::franken_sync::Connection::open(db_path.to_string_lossy().into_owned()).unwrap();
    let rows = conn.query("SELECT COUNT(*) FROM t;").unwrap();
    let count: i64 = rows.first().unwrap().get_typed(0).unwrap();
    assert_eq!(count, 3, "checkpointed rows must survive recovery");
    conn.close().unwrap();
}

// review 2026-11-02 S1: a checkpoint pinned by an open reader reports
// busy instead of failing; recovery must not call that "completed".
#[test]
fn dirty_wal_recovery_declines_when_checkpoint_reports_busy() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("pinned.db");
    {
        let mut conn =
            crate::franken_sync::Connection::open(db_path.to_string_lossy().into_owned()).unwrap();
        conn.execute("CREATE TABLE t (x INTEGER);").unwrap();
        conn.execute("INSERT INTO t (x) VALUES (1), (2), (3);")
            .unwrap();
        close_franken_in_place_with_busy_retry(&mut conn, false).unwrap();
    }
    // Pin the WAL with an open read transaction from a second connection.
    let reader =
        crate::franken_sync::Connection::open(db_path.to_string_lossy().into_owned()).unwrap();
    reader.execute("BEGIN;").unwrap();
    reader.query("SELECT COUNT(*) FROM t;").unwrap();
    let probe =
        crate::franken_sync::Connection::open(db_path.to_string_lossy().into_owned()).unwrap();
    let status = probe.query("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    probe.close().unwrap();
    if wal_checkpoint_status::truncate_rows_report_complete(&status) {
        // The engine let TRUNCATE finish despite the reader; there is
        // no busy state to reproduce here. The pure tests in
        // wal_checkpoint_status cover the classification.
        reader.execute("COMMIT;").unwrap();
        reader.close().unwrap();
        return;
    }
    // Re-dirty the WAL so recovery has frames to attempt.
    {
        let writer =
            crate::franken_sync::Connection::open(db_path.to_string_lossy().into_owned()).unwrap();
        writer.execute("INSERT INTO t (x) VALUES (4);").unwrap();
        writer.close().unwrap();
    }
    let hard_err = anyhow!("unable to open database file");
    assert!(
        !attempt_dirty_wal_recovery_checkpoint(&db_path, &hard_err),
        "a busy TRUNCATE checkpoint must not be reported as completed recovery"
    );
    reader.execute("COMMIT;").unwrap();
    reader.close().unwrap();
}

fn snippets_index_names(storage: &SqliteStorage) -> Vec<String> {
    storage
        .conn
        .query("SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'snippets' ORDER BY name;")
        .unwrap()
        .iter()
        .map(|row| row.get_typed::<String>(0).unwrap())
        .collect()
}

// review 2026-11-02 S2
#[test]
fn normal_open_creates_snippets_message_id_index() {
    let dir = TempDir::new().unwrap();
    let storage = SqliteStorage::open(&dir.path().join("a.db")).unwrap();
    assert_eq!(
        snippets_index_names(&storage),
        vec![local_lookup_indexes::SNIPPETS_MESSAGE_ID_INDEX.to_string()]
    );
}

// review 2026-11-02 S2: the fast current-schema open skips run_migrations,
// so it must get the index through repair_missing_current_schema_objects.
#[test]
fn fast_current_schema_open_creates_snippets_message_id_index() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("fast.db");
    {
        let storage = SqliteStorage::open(&db_path).unwrap();
        storage
            .conn
            .execute(&format!(
                "DROP INDEX {};",
                local_lookup_indexes::SNIPPETS_MESSAGE_ID_INDEX
            ))
            .unwrap();
        storage.close().unwrap();
    }
    let storage =
        open_current_schema_storage_with_timeout(&db_path, std::time::Duration::from_secs(30))
            .unwrap()
            .expect("current schema must take the fast path");
    assert_eq!(
        snippets_index_names(&storage),
        vec![local_lookup_indexes::SNIPPETS_MESSAGE_ID_INDEX.to_string()]
    );
}

// review 2026-11-02 S2: an equivalent remote index under another name is
// accepted and no duplicate is created.
#[test]
fn open_accepts_equivalent_snippets_index_without_duplicate() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("remote.db");
    {
        let storage = SqliteStorage::open(&db_path).unwrap();
        storage
            .conn
            .execute(&format!(
                "DROP INDEX {};",
                local_lookup_indexes::SNIPPETS_MESSAGE_ID_INDEX
            ))
            .unwrap();
        storage
            .conn
            .execute("CREATE INDEX idx_snippets_msg ON snippets(message_id);")
            .unwrap();
        storage.close().unwrap();
    }
    let storage = SqliteStorage::open(&db_path).unwrap();
    assert_eq!(
        snippets_index_names(&storage),
        vec!["idx_snippets_msg".to_string()]
    );
}

// review 2026-11-02 S3: tail readers must not depend on the index V16 dropped.
#[test]
fn lexical_rebuild_tail_sql_uses_live_message_index() {
    for sql in [
        footprint_tail_sql::TAIL_MAX_IDX_SQL,
        footprint_tail_sql::MISSING_TAIL_SQL,
    ] {
        assert!(!sql.contains("idx_messages_conv_idx"), "{sql}");
        assert!(
            sql.contains("INDEXED BY sqlite_autoindex_messages_1"),
            "{sql}"
        );
    }
}

#[cfg(unix)]
#[test]
fn gh477_readonly_openers_recover_only_derived_wal_index() {
    // Stock SQLite supplies two real WAL generations and a foreign lock
    // owner; all application opens and reads below use FrankenSQLite.
    let script = r#"
import os, pathlib, shutil, sqlite3, sys
p = pathlib.Path(sys.argv[1])
seed = str(p) + '.seed'
c = sqlite3.connect(seed)
c.executescript('PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE t(n INTEGER); INSERT INTO t VALUES(10);')
old = pathlib.Path(seed + '-shm').read_bytes()
c.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchall()
c.execute('INSERT INTO t VALUES(20)')
c.commit()
current = pathlib.Path(seed + '-shm').read_bytes()
assert old[32:40] != current[32:40]
if sys.argv[3] == 'header':
    checkpoint = c.execute('PRAGMA wal_checkpoint(FULL)').fetchone()
    assert checkpoint[0] == 0 and checkpoint[1] == checkpoint[2]
for suffix in ['', '-wal']:
    shutil.copyfile(seed + suffix, str(p) + suffix)
if sys.argv[3] == 'header':
    header = pathlib.Path(seed + '-wal').read_bytes()[:32]
    assert len(header) == 32
    pathlib.Path(str(p) + '-wal').write_bytes(header)
pathlib.Path(str(p) + '-shm').write_bytes(old)
c.close()
if sys.argv[2] == 'writer':
    c = sqlite3.connect(str(p), timeout=0)
    # Closing any SHM descriptor releases this process's POSIX locks.
    # Keep the fixture descriptor open for the complete writer lifetime.
    shm_fd = os.open(str(p) + '-shm', os.O_RDWR)
    c.execute('BEGIN IMMEDIATE')
    assert os.pwrite(shm_fd, old, 0) == len(old)
print('ready', flush=True)
sys.stdin.readline()
if sys.argv[2] == 'writer':
    c.rollback()
    print('released', flush=True)
    sys.stdin.readline()
    c.close()
    os.close(shm_fd)
"#;
    struct Fixture(std::process::Child);
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    // How long to wait for the stock-SQLite fixture process to report
    // `ready` / `released`: interpreter start plus fsync'd WAL setup in
    // another process, not a cass latency bound (those are the strict
    // refusal and open bounds below). Its setup takes ~50 ms on an idle
    // host but has exceeded 10 s on a loaded build worker (bead gtafk).
    const FIXTURE_WAIT: Duration = Duration::from_secs(120);

    let dir = TempDir::new().unwrap();
    for opener in ["raw", "storage", "owner"] {
        for header_only in [false, true] {
            for writer in [false, true] {
                let case_dir = dir.path().join(format!("{opener}-{header_only}-{writer}"));
                fs::create_dir(&case_dir).unwrap();
                let path = case_dir.join("archive.db");
                let mut fixture = Fixture(
                    Command::new("python3")
                        .args(["-c", script])
                        .arg(&path)
                        .arg(if writer { "writer" } else { "idle" })
                        .arg(if header_only { "header" } else { "frames" })
                        .stdin(Stdio::piped())
                        .stdout(Stdio::piped())
                        .spawn()
                        .expect("Python sqlite3 is required for the GH477 foreign-writer fixture"),
                );
                let stdout = fixture.0.stdout.take().unwrap();
                let (sender, output) = std::sync::mpsc::channel();
                let reader = std::thread::spawn(move || {
                    for line in BufReader::new(stdout).lines() {
                        if sender.send(line.unwrap()).is_err() {
                            break;
                        }
                    }
                });
                assert_eq!(output.recv_timeout(FIXTURE_WAIT).unwrap(), "ready");
                if writer {
                    // Witness the stock writer's real WAL_WRITE_LOCK from
                    // another process before trusting the negative control.
                    let probe = r#"
import fcntl, os, sys
fd = os.open(sys.argv[1], os.O_RDWR)
try:
    fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB, 1, 120, os.SEEK_SET)
except BlockingIOError:
    sys.exit(0)
sys.exit('stock writer does not own WAL_WRITE_LOCK')
"#;
                    let probe_output = Command::new("python3")
                        .args(["-c", probe])
                        .arg(database_sidecar_path(&path, "-shm"))
                        .output()
                        .unwrap();
                    assert!(
                        probe_output.status.success(),
                        "{}",
                        String::from_utf8_lossy(&probe_output.stderr)
                    );
                }
                let fingerprint = |suffix: &str| {
                    let file = database_sidecar_path(&path, suffix);
                    (
                        fs::read(&file).unwrap(),
                        fs::metadata(&file).unwrap().modified().unwrap(),
                    )
                };
                let main_before = fingerprint("");
                let wal_before = fingerprint("-wal");
                let shm_before = fingerprint("-shm");
                if header_only {
                    assert_eq!(wal_before.0.len(), 32);
                } else {
                    assert!(wal_before.0.len() > 32);
                }
                let timeout = Duration::from_millis(50);
                let refusals_started = Instant::now();
                let strict = FrankenStorage::open_strict_readonly_with_timeout(&path, timeout);
                let err = strict.err().expect("strict storage must refuse stale SHM");
                assert!(matches!(
                    err.downcast_ref::<crate::franken_sync::FrankenError>(),
                    Some(crate::franken_sync::FrankenError::BusyRecovery)
                ));
                let strict =
                    open_franken_async_strict_readonly_connection_with_timeout(&path, timeout);
                let err = strict.expect_err("strict owner must refuse stale SHM");
                assert!(matches!(
                    err.downcast_ref::<crate::franken_sync::FrankenError>(),
                    Some(crate::franken_sync::FrankenError::BusyRecovery)
                ));
                // e9f0v: fsqlite 0.4.7 retried a strict read-only bootstrap
                // on BusyRecovery for a 30 s floor. A stale WAL index must be
                // refused at once.
                let refusals = refusals_started.elapsed();
                assert!(
                    refusals < Duration::from_secs(5),
                    "strict refusals took {refusals:?} ({opener}, header_only={header_only}, writer={writer})"
                );
                assert_eq!(fingerprint("-shm"), shm_before);
                assert_eq!(fingerprint(""), main_before);
                assert_eq!(fingerprint("-wal"), wal_before);
                assert!(!case_dir.join("doctor").exists());

                let open_and_read = || -> Result<()> {
                    if opener == "owner" {
                        let mut conn =
                            open_franken_async_readonly_connection_with_timeout(&path, timeout)?;
                        let rows = conn.query_sync("SELECT sum(n) FROM t")?;
                        assert_eq!(rows[0].get_typed::<i64>(0)?, 30);
                        assert!(conn.execute_sync("INSERT INTO t VALUES(99)").is_err());
                        conn.close_without_checkpoint_sync()?;
                    } else if opener == "storage" {
                        let storage =
                            FrankenStorage::open_readonly_with_doctor_lock_timeout(&path, timeout)?;
                        let rows = storage.raw().query("SELECT sum(n) FROM t")?;
                        assert_eq!(rows[0].get_typed::<i64>(0)?, 30);
                        assert!(storage.raw().execute("INSERT INTO t VALUES(99)").is_err());
                        storage.close_without_checkpoint()?;
                    } else {
                        let mut conn =
                            open_franken_raw_readonly_connection_with_timeout(&path, timeout)?;
                        let rows = conn.query("SELECT sum(n) FROM t")?;
                        assert_eq!(rows[0].get_typed::<i64>(0)?, 30);
                        assert!(conn.execute("INSERT INTO t VALUES(99)").is_err());
                        close_franken_in_place_with_busy_retry(&mut conn, false)?;
                    }
                    Ok(())
                };
                let started = Instant::now();
                let result = open_and_read();
                assert!(started.elapsed() < Duration::from_secs(10));
                if writer {
                    let err = result.expect_err("live writer must prevent SHM repair");
                    assert!(retryable_franken_anyhow(&err), "{err:#}");
                    assert_eq!(fingerprint("-shm"), shm_before);
                    assert_eq!(fingerprint(""), main_before);
                    assert_eq!(fingerprint("-wal"), wal_before);
                    writeln!(fixture.0.stdin.as_mut().unwrap(), "release").unwrap();
                    assert_eq!(output.recv_timeout(FIXTURE_WAIT).unwrap(), "released");
                    open_and_read().expect("same stale fixture recovers after writer release");
                } else {
                    result.expect("ordinary read opener repairs stale SHM");
                }
                assert_ne!(fingerprint("-shm").0, shm_before.0);
                let strict = FrankenStorage::open_strict_readonly_with_timeout(&path, timeout)
                    .expect("strict reads succeed once the WAL index is repaired");
                let rows = strict.raw().query("SELECT sum(n) FROM t").unwrap();
                assert_eq!(rows[0].get_typed::<i64>(0).unwrap(), 30);
                strict.close_without_checkpoint().unwrap();
                assert_eq!(fingerprint(""), main_before);
                assert_eq!(fingerprint("-wal"), wal_before);
                writeln!(fixture.0.stdin.as_mut().unwrap(), "exit").unwrap();
                assert!(fixture.0.wait().unwrap().success());
                reader.join().unwrap();
            }
        }
    }
}

#[test]
fn dedicated_owner_readonly_open_reads_nonempty_wal_without_mutating_it() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("dedicated-owner-dirty.db");
    {
        let mut conn =
            crate::franken_sync::Connection::open(db_path.to_string_lossy().into_owned()).unwrap();
        conn.execute("CREATE TABLE t (x INTEGER);").unwrap();
        conn.execute("INSERT INTO t (x) VALUES (1), (2), (3);")
            .unwrap();
        close_franken_in_place_with_busy_retry(&mut conn, false).unwrap();
    }

    let wal_path = database_sidecar_path(&db_path, "-wal");
    let dirty_len = std::fs::symlink_metadata(&wal_path)
        .map(|meta| meta.len())
        .unwrap_or(0);
    assert!(
        dirty_len > 32,
        "precondition: dedicated-owner test requires unreplayed WAL frames (len={dirty_len})"
    );

    let mut conn =
        open_franken_async_readonly_connection_with_timeout(&db_path, Duration::from_secs(2))
            .expect("dedicated-owner readonly open should read through the dirty WAL");
    let rows = conn
        .query_sync("SELECT COUNT(*) FROM t;")
        .expect("dedicated owner should read committed rows through the WAL");
    let count: i64 = rows
        .first()
        .expect("count query should return one row")
        .get_typed(0)
        .expect("count should be an integer");
    assert_eq!(count, 3, "dirty-WAL reads must preserve committed rows");
    // fsqlite 0.3.8 admits write STATEMENTS on a readonly connection as
    // long as they touch no pages (`WHERE 1 = 0` plans to zero row
    // writes and returns Ok(0)); SQLite proper refuses at prepare. Pin
    // the enforcement that matters — a write that would actually mutate
    // pages must refuse — and track the no-op prepare-time gap upstream.
    conn.execute_sync("DELETE FROM t")
        .expect_err("dedicated-owner readonly connection must refuse writes");
    conn.close_without_checkpoint_sync()
        .expect("dedicated-owner readonly close should join its worker");

    let reopened_len = std::fs::symlink_metadata(&wal_path)
        .map(|meta| meta.len())
        .unwrap_or(0);
    assert_eq!(
        reopened_len, dirty_len,
        "successful readonly dispatch must not rewrite or checkpoint the dirty WAL"
    );
}

#[test]
fn gh422_strict_read_openers_do_not_create_lock_state_or_change_database_bundle() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("agent_search.db");
    {
        let conn =
            crate::franken_sync::Connection::open(db_path.to_string_lossy().into_owned()).unwrap();
        conn.execute("CREATE TABLE t (x INTEGER);").unwrap();
        conn.execute("INSERT INTO t (x) VALUES (1), (2), (3);")
            .unwrap();
        conn.close().unwrap();
    }
    let bundle_snapshot = || {
        [
            db_path.clone(),
            database_sidecar_path(&db_path, "-wal"),
            database_sidecar_path(&db_path, "-shm"),
        ]
        .into_iter()
        .map(|path| {
            let bytes = std::fs::read(&path).ok();
            (path, bytes)
        })
        .collect::<std::collections::BTreeMap<_, _>>()
    };
    let before = bundle_snapshot();
    assert!(!dir.path().join("doctor").exists());

    let mut owner = open_franken_async_strict_readonly_connection_with_timeout(
        &db_path,
        Duration::from_secs(2),
    )
    .expect("strict dedicated-owner read open");
    let rows = owner.query_sync("SELECT COUNT(*) FROM t;").unwrap();
    assert_eq!(rows[0].get_typed::<i64>(0).unwrap(), 3);
    owner.close_without_checkpoint_sync().unwrap();

    let strict_storage =
        FrankenStorage::open_strict_readonly(&db_path).expect("strict storage read open");
    let count: i64 = strict_storage
        .raw()
        .query_row_map("SELECT COUNT(*) FROM t;", &[] as &[ParamValue], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(count, 3);
    strict_storage.close_without_checkpoint().unwrap();

    assert_eq!(before, bundle_snapshot());
    assert!(
        !dir.path().join("doctor").exists(),
        "a strict read must not create doctor/locks admission state"
    );
}

#[test]
fn reopen_existing_current_schema_is_idempotent() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("existing.db");

    // First open creates and migrates to current schema.
    {
        let storage = SqliteStorage::open(&db_path).unwrap();
        assert_eq!(storage.schema_version().unwrap(), CURRENT_SCHEMA_VERSION);
    }

    // Re-open should not fail on current schema.
    let reopened = SqliteStorage::open(&db_path).unwrap();
    assert_eq!(
        reopened.schema_version().unwrap(),
        CURRENT_SCHEMA_VERSION,
        "reopening current schema DB should be idempotent"
    );
}

#[test]
fn open_or_rebuild_current_schema_does_not_trigger_rebuild() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("existing.db");

    // Create DB at current schema.
    {
        let storage = SqliteStorage::open(&db_path).unwrap();
        assert_eq!(storage.schema_version().unwrap(), CURRENT_SCHEMA_VERSION);
    }

    // Should open normally, not require rebuild.
    let reopened = SqliteStorage::open_or_rebuild(&db_path)
        .expect("current schema DB should open without rebuild");
    assert_eq!(reopened.schema_version().unwrap(), CURRENT_SCHEMA_VERSION);
}

#[test]
fn open_or_rebuild_does_not_treat_non_database_paths_as_corruption() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("db_dir");
    std::fs::create_dir(&db_path).unwrap();

    let result = SqliteStorage::open_or_rebuild(&db_path);

    assert!(
        matches!(
            result,
            Err(MigrationError::Database(_)) | Err(MigrationError::Io(_))
        ),
        "non-database path should report the underlying open error without rebuild"
    );

    assert!(
        db_path.is_dir(),
        "non-database directory must be left in place"
    );
}

// =========================================================================
// Schema version tests (bead yln.4)
// =========================================================================

#[test]
fn schema_version_returns_current() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();
    let version = storage.schema_version().unwrap();
    assert!(version >= 5, "Schema version should be at least 5");
}
