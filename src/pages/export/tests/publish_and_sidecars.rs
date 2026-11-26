use super::super::engine::resolve_export_output_path;
#[cfg(not(windows))]
use super::super::publish_guard::first_unmarked_export_publish_backup;
#[cfg(unix)]
use super::super::publish_guard::require_export_publish_guard;
use super::super::publish_guard::{
    acquire_export_publish_guard, export_publish_lock_path, export_publish_recovery_journal_path,
    inspect_export_regular_file, legacy_export_publish_recovery_backup_path,
    unpredictable_atomic_sidecar_path, unpredictable_export_publish_backup_path,
    write_export_publish_journal,
};
#[cfg(windows)]
use super::super::publish_guard::{journal_backup_path, read_export_publish_journal};
use super::super::publish_replace::{
    cleanup_sqlite_temp_artifacts, create_staged_export_file,
    enforce_private_candidate_permissions, finalize_staged_sqlite_sidecars,
    recover_or_refuse_interrupted_export_publish, reject_existing_sqlite_sidecars,
    remove_prior_export_backup_after_publish, replace_file_from_temp,
    replace_file_from_temp_via_backup,
};
#[cfg(unix)]
use super::super::publish_replace::{
    reject_non_regular_existing_publish_destination, replacement_path_entry_exists,
};
use super::write_test_publish_journal;
use crate::franken_sync::compat::{ConnectionExt, RowExt};
use crate::franken_sync::{Connection, params};
use crate::pages::{
    sqlite_content_artifact_paths, sqlite_fixed_artifact_paths, sqlite_runtime_artifact_paths,
};
use anyhow::Result;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use tempfile::TempDir;

#[test]
fn output_resolution_rejects_alias_created_by_missing_parent() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let source_path = temp_dir.path().join("source.db");
    let missing_parent = temp_dir.path().join("created-during-export");
    let output_path = missing_parent.join("..").join("source.db");
    std::fs::write(&source_path, b"source generation")?;

    assert!(
        std::fs::canonicalize(&output_path).is_err(),
        "the regression requires the raw output alias to be unresolved before its parent exists"
    );
    let error = resolve_export_output_path(&source_path, &output_path)
        .expect_err("creating the parent must expose and reject the source alias");

    assert!(
        format!("{error:#}").contains("output path must be different"),
        "unexpected alias rejection: {error:#}"
    );
    assert!(
        missing_parent.is_dir(),
        "the test must cross the state transition that used to make the alias dangerous"
    );
    assert_eq!(
        std::fs::read(&source_path)?,
        b"source generation",
        "alias rejection must preserve the source database"
    );
    Ok(())
}

#[test]
fn output_resolution_returns_entry_under_resolved_created_parent() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let source_path = temp_dir.path().join("source.db");
    let output_path = temp_dir.path().join("new-parent").join("export.db");
    std::fs::write(&source_path, b"source generation")?;

    let resolved = resolve_export_output_path(&source_path, &output_path)?;

    assert_eq!(
        resolved,
        std::fs::canonicalize(temp_dir.path().join("new-parent"))?.join("export.db")
    );
    assert_ne!(resolved, std::fs::canonicalize(source_path)?);
    Ok(())
}

#[cfg(unix)]
#[test]
fn output_resolution_rejects_existing_hard_link_to_source() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let source_path = temp_dir.path().join("source.db");
    let output_path = temp_dir.path().join("export.db");
    std::fs::write(&source_path, b"source generation")?;
    std::fs::hard_link(&source_path, &output_path)?;

    let error = resolve_export_output_path(&source_path, &output_path)
        .expect_err("an existing output with the source identity must be rejected");

    assert!(
        format!("{error:#}").contains("same filesystem object"),
        "unexpected filesystem-identity rejection: {error:#}"
    );
    assert_eq!(std::fs::read(source_path)?, b"source generation");
    assert_eq!(std::fs::read(output_path)?, b"source generation");
    Ok(())
}

#[cfg(unix)]
#[test]
fn replacement_path_entry_exists_detects_dangling_symlink() -> Result<()> {
    use std::os::unix::fs::symlink;

    let temp_dir = TempDir::new()?;
    let link_path = temp_dir.path().join("export.db");
    let missing_target = temp_dir.path().join("missing-export.db");

    symlink(&missing_target, &link_path)?;

    if link_path.exists() {
        return Err(anyhow::anyhow!(
            "Path::exists stopped following the missing target"
        ));
    }
    if !replacement_path_entry_exists(&link_path)? {
        return Err(anyhow::anyhow!(
            "replacement path helper missed a dangling symlink entry"
        ));
    }

    Ok(())
}

#[cfg(unix)]
#[test]
fn backup_replacement_rejects_existing_destination_symlink() -> Result<()> {
    use std::os::unix::fs::symlink;

    let temp_dir = TempDir::new()?;
    let final_path = temp_dir.path().join("export.db");
    let target_path = temp_dir.path().join("target.db");
    std::fs::write(&target_path, b"target generation")?;
    symlink(&target_path, &final_path)?;

    let error = reject_non_regular_existing_publish_destination(&final_path)
        .expect_err("backup replacement must not move an untrusted link");

    assert!(format!("{error:#}").contains("not a regular file"));
    assert!(
        std::fs::symlink_metadata(&final_path)?
            .file_type()
            .is_symlink()
    );
    assert_eq!(std::fs::read(&target_path)?, b"target generation");

    Ok(())
}

#[test]
fn publish_recovery_uses_stable_journal_and_unpredictable_backups() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let final_path = temp_dir.path().join("export.db");
    let journal_path = export_publish_recovery_journal_path(&final_path);
    let first = unpredictable_export_publish_backup_path(&final_path)?;
    let second = unpredictable_export_publish_backup_path(&final_path)?;

    assert_eq!(
        journal_path.file_name().and_then(|name| name.to_str()),
        Some(".export.db.pages-export-publish-in-progress.json")
    );
    assert_ne!(first, second, "backup nonce must not be reused");
    for backup_path in [first, second] {
        let name = backup_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| anyhow::anyhow!("backup name is not UTF-8"))?;
        assert!(name.starts_with(".export.db.publish-backup."));
        assert_eq!(
            name.rsplit_once('.').map(|(_, nonce)| nonce.len()),
            Some(32)
        );
    }
    Ok(())
}

#[test]
fn interrupted_publish_refuses_unmarked_deterministic_backup() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let final_path = temp_dir.path().join("export.db");
    let backup_path = legacy_export_publish_recovery_backup_path(&final_path);
    std::fs::write(&backup_path, b"unowned bytes")?;
    let guard = acquire_export_publish_guard(&final_path)?;

    let error = recover_or_refuse_interrupted_export_publish(&final_path, &guard)
        .expect_err("an unmarked deterministic backup must never be adopted");

    assert!(format!("{error:#}").contains("unmarked"));
    assert_eq!(std::fs::read(&backup_path)?, b"unowned bytes");
    assert!(std::fs::symlink_metadata(&final_path).is_err());
    Ok(())
}

#[test]
fn interrupted_publish_refuses_unmarked_random_backup() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let final_path = temp_dir.path().join("export.db");
    let backup_path = unpredictable_export_publish_backup_path(&final_path)?;
    std::fs::write(&backup_path, b"unowned random backup")?;
    let guard = acquire_export_publish_guard(&final_path)?;

    let error = recover_or_refuse_interrupted_export_publish(&final_path, &guard)
        .expect_err("an unmarked random backup must never be adopted");

    assert!(format!("{error:#}").contains("unmarked"));
    assert_eq!(std::fs::read(&backup_path)?, b"unowned random backup");
    assert!(std::fs::symlink_metadata(&final_path).is_err());
    Ok(())
}

#[test]
fn interrupted_publish_restores_only_journaled_prior_generation() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let final_path = temp_dir.path().join("export.db");
    let backup_path = unpredictable_export_publish_backup_path(&final_path)?;
    let candidate_path = temp_dir.path().join("candidate-evidence.db");
    std::fs::write(&backup_path, b"prior generation")?;
    std::fs::write(&candidate_path, b"candidate generation")?;
    let guard = acquire_export_publish_guard(&final_path)?;
    write_test_publish_journal(&final_path, &backup_path, &candidate_path, &guard)?;

    let result = recover_or_refuse_interrupted_export_publish(&final_path, &guard);
    #[cfg(not(windows))]
    result?;
    #[cfg(windows)]
    {
        let error = result.expect_err("Windows recovery must retain its journal");
        assert!(format!("{error:#}").contains("journal retained"));
    }

    assert_eq!(std::fs::read(&final_path)?, b"prior generation");
    assert!(std::fs::symlink_metadata(&backup_path).is_err());
    #[cfg(not(windows))]
    assert!(std::fs::symlink_metadata(export_publish_recovery_journal_path(&final_path)).is_err());
    #[cfg(windows)]
    assert!(export_publish_recovery_journal_path(&final_path).is_file());
    Ok(())
}

#[test]
fn interrupted_publish_finalizes_journal_written_before_backup_move() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let final_path = temp_dir.path().join("export.db");
    let backup_path = unpredictable_export_publish_backup_path(&final_path)?;
    let candidate_path = temp_dir.path().join("candidate-evidence.db");
    std::fs::write(&backup_path, b"prior generation")?;
    std::fs::write(&candidate_path, b"candidate generation")?;
    let guard = acquire_export_publish_guard(&final_path)?;
    write_test_publish_journal(&final_path, &backup_path, &candidate_path, &guard)?;
    std::fs::rename(&backup_path, &final_path)?;

    let result = recover_or_refuse_interrupted_export_publish(&final_path, &guard);
    #[cfg(not(windows))]
    result?;
    #[cfg(windows)]
    {
        let error = result.expect_err("Windows must retain the unresolved journal");
        assert!(format!("{error:#}").contains("journal retained"));
    }

    assert_eq!(std::fs::read(&final_path)?, b"prior generation");
    assert!(std::fs::symlink_metadata(&backup_path).is_err());
    #[cfg(not(windows))]
    assert!(std::fs::symlink_metadata(export_publish_recovery_journal_path(&final_path)).is_err());
    #[cfg(windows)]
    assert!(export_publish_recovery_journal_path(&final_path).is_file());
    Ok(())
}

#[test]
fn interrupted_publish_finalizes_journaled_completed_generation() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let final_path = temp_dir.path().join("export.db");
    let backup_path = unpredictable_export_publish_backup_path(&final_path)?;
    std::fs::write(&final_path, b"candidate generation")?;
    std::fs::write(&backup_path, b"prior generation")?;
    let guard = acquire_export_publish_guard(&final_path)?;
    write_test_publish_journal(&final_path, &backup_path, &final_path, &guard)?;

    let result = recover_or_refuse_interrupted_export_publish(&final_path, &guard);
    #[cfg(not(windows))]
    result?;
    #[cfg(windows)]
    {
        let error = result.expect_err("Windows finalization must preserve backup and journal");
        assert!(format!("{error:#}").contains("backup and journal were preserved"));
    }

    assert_eq!(std::fs::read(&final_path)?, b"candidate generation");
    #[cfg(not(windows))]
    {
        assert!(std::fs::symlink_metadata(&backup_path).is_err());
        assert!(
            std::fs::symlink_metadata(export_publish_recovery_journal_path(&final_path)).is_err()
        );
    }
    #[cfg(windows)]
    {
        assert_eq!(std::fs::read(&backup_path)?, b"prior generation");
        assert!(export_publish_recovery_journal_path(&final_path).is_file());
    }
    Ok(())
}

#[test]
fn interrupted_publish_preserves_journaled_backup_after_content_drift() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let final_path = temp_dir.path().join("export.db");
    let backup_path = unpredictable_export_publish_backup_path(&final_path)?;
    let candidate_path = temp_dir.path().join("candidate-evidence.db");
    std::fs::write(&backup_path, b"prior generation")?;
    std::fs::write(&candidate_path, b"candidate generation")?;
    let guard = acquire_export_publish_guard(&final_path)?;
    write_test_publish_journal(&final_path, &backup_path, &candidate_path, &guard)?;
    std::fs::write(&backup_path, b"changed generation")?;

    let error = recover_or_refuse_interrupted_export_publish(&final_path, &guard)
        .expect_err("drifted recovery bytes must never be restored or removed");

    assert!(format!("{error:#}").contains("does not match"));
    assert_eq!(std::fs::read(&backup_path)?, b"changed generation");
    assert!(std::fs::symlink_metadata(&final_path).is_err());
    assert!(export_publish_recovery_journal_path(&final_path).is_file());
    Ok(())
}

#[test]
fn interrupted_publish_preserves_backup_when_destination_companion_survives() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let final_path = temp_dir.path().join("export.db");
    let backup_path = unpredictable_export_publish_backup_path(&final_path)?;
    let candidate_path = temp_dir.path().join("candidate-evidence.db");
    let destination_sidecar = sqlite_content_artifact_paths(&final_path)
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("SQLite artifact family unexpectedly empty"))?;
    std::fs::write(&backup_path, b"prior main")?;
    std::fs::write(&candidate_path, b"candidate main")?;
    std::fs::write(&destination_sidecar, b"unbound destination companion")?;
    let guard = acquire_export_publish_guard(&final_path)?;
    write_test_publish_journal(&final_path, &backup_path, &candidate_path, &guard)?;

    let error = recover_or_refuse_interrupted_export_publish(&final_path, &guard)
        .expect_err("recovery must not mix a parked main with destination companions");

    assert!(format!("{error:#}").contains(&destination_sidecar.display().to_string()));
    assert_eq!(std::fs::read(&backup_path)?, b"prior main");
    assert_eq!(
        std::fs::read(&destination_sidecar)?,
        b"unbound destination companion"
    );
    assert!(std::fs::symlink_metadata(&final_path).is_err());
    Ok(())
}

#[cfg(unix)]
#[test]
fn publish_lock_rejects_symlink_before_opening_target() -> Result<()> {
    use std::os::unix::fs::symlink;

    let temp_dir = TempDir::new()?;
    let final_path = temp_dir.path().join("export.db");
    let lock_path = export_publish_lock_path(&final_path);
    let target_path = temp_dir.path().join("unowned-lock-target");
    std::fs::write(&target_path, b"unowned lock bytes")?;
    symlink(&target_path, &lock_path)?;

    let error = acquire_export_publish_guard(&final_path)
        .expect_err("publish lock acquisition must reject a pre-existing symlink");

    assert!(format!("{error:#}").contains("refused to open"));
    assert!(
        std::fs::symlink_metadata(&lock_path)?
            .file_type()
            .is_symlink()
    );
    assert_eq!(std::fs::read(&target_path)?, b"unowned lock bytes");
    Ok(())
}

#[test]
fn publish_lock_rejects_special_entry_before_opening_it() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let final_path = temp_dir.path().join("export.db");
    let lock_path = export_publish_lock_path(&final_path);
    std::fs::create_dir(&lock_path)?;

    let error = acquire_export_publish_guard(&final_path)
        .expect_err("publish lock acquisition must reject a non-file entry");

    assert!(format!("{error:#}").contains("refused to open"));
    assert!(lock_path.is_dir(), "unowned lock entry must be preserved");
    Ok(())
}

#[test]
fn publish_lock_serializes_same_destination() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let final_path = temp_dir.path().join("export.db");
    let first_guard = acquire_export_publish_guard(&final_path)?;

    let error = acquire_export_publish_guard(&final_path)
        .expect_err("a second publisher must not acquire the same destination lock");

    assert!(format!("{error:#}").contains("already publishing"));
    assert_eq!(first_guard.final_path, final_path);
    Ok(())
}

#[cfg(unix)]
#[test]
fn replaced_publish_lock_invalidates_existing_guard() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let final_path = temp_dir.path().join("export.db");
    let lock_path = export_publish_lock_path(&final_path);
    let parked_lock_path = temp_dir.path().join("parked-publish.lock");
    let first_guard = acquire_export_publish_guard(&final_path)?;
    std::fs::rename(&lock_path, &parked_lock_path)?;
    std::fs::write(&lock_path, b"replacement lock")?;
    let second_guard = acquire_export_publish_guard(&final_path)?;

    let error = require_export_publish_guard(&final_path, &first_guard)
        .expect_err("a guard for a replaced lock pathname must not authorize mutation");

    assert!(format!("{error:#}").contains("was replaced"));
    require_export_publish_guard(&final_path, &second_guard)?;
    assert!(
        parked_lock_path.is_file(),
        "original lock must be preserved"
    );
    Ok(())
}

#[test]
fn vacuum_candidate_path_uses_fresh_unpredictable_names() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let final_path = temp_dir.path().join("export.db");
    let first = unpredictable_atomic_sidecar_path(&final_path, "tmp", "pages_export.db")?;
    let second = unpredictable_atomic_sidecar_path(&final_path, "tmp", "pages_export.db")?;

    assert_ne!(first, second, "candidate nonce was reused");
    for path in [first, second] {
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| anyhow::anyhow!("candidate name is not UTF-8"))?;
        assert!(
            name.starts_with(".export.db.tmp."),
            "unexpected name: {name}"
        );
        assert_eq!(
            name.rsplit_once('.').map(|(_, nonce)| nonce.len()),
            Some(32),
            "candidate nonce must carry 128 bits"
        );
    }
    Ok(())
}

#[test]
#[cfg(unix)]
fn staged_export_file_is_exclusive_and_owner_only() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let temp_dir = TempDir::new()?;
    let staged_path = temp_dir.path().join("export.tmp.db");
    create_staged_export_file(&staged_path)?;

    let mode = std::fs::metadata(&staged_path)?.permissions().mode();
    if mode & 0o077 != 0 {
        return Err(anyhow::anyhow!("staged export mode was {mode:o}"));
    }
    if create_staged_export_file(&staged_path).is_ok() {
        return Err(anyhow::anyhow!(
            "exclusive staging unexpectedly reused an existing path"
        ));
    }
    Ok(())
}

#[test]
fn rejected_export_cleanup_removes_every_sqlite_artifact() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let staged_path = temp_dir.path().join("export.tmp.db");
    let wal_segment = temp_dir.path().join("export.tmp.db-wal-seg-not-an-epoch");
    let artifacts = std::iter::once(staged_path.clone())
        .chain(sqlite_fixed_artifact_paths(&staged_path))
        .chain(std::iter::once(wal_segment))
        .collect::<Vec<_>>();
    for artifact in &artifacts {
        std::fs::write(artifact, b"staged bytes")?;
    }

    cleanup_sqlite_temp_artifacts(&staged_path)?;

    for artifact in artifacts {
        if std::fs::symlink_metadata(&artifact).is_ok() {
            return Err(anyhow::anyhow!(
                "rejected staged SQLite artifact survived cleanup: {}",
                artifact.display()
            ));
        }
    }
    Ok(())
}

#[test]
fn rejected_export_cleanup_preserves_sidecars_when_main_removal_fails() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let staged_path = temp_dir.path().join("export.tmp.db");
    let sidecar_path = sqlite_content_artifact_paths(&staged_path)
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("SQLite artifact family unexpectedly empty"))?;
    std::fs::create_dir(&staged_path)?;
    std::fs::write(&sidecar_path, b"recovery bytes")?;

    let error = cleanup_sqlite_temp_artifacts(&staged_path)
        .expect_err("a non-file main path must make cleanup fail closed");

    assert!(
        format!("{error:#}").contains("preserved every companion"),
        "unexpected main-removal error: {error:#}"
    );
    assert_eq!(
        std::fs::read(&sidecar_path)?,
        b"recovery bytes",
        "failed main removal must not destroy a recoverable companion"
    );
    Ok(())
}

#[test]
fn rejected_export_cleanup_preserves_sidecars_after_main_identity_loss() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let staged_path = temp_dir.path().join("export.tmp.db");
    let sidecar_path = sqlite_content_artifact_paths(&staged_path)
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("SQLite artifact family unexpectedly empty"))?;
    std::fs::write(&sidecar_path, b"unbound recovery bytes")?;

    let error = cleanup_sqlite_temp_artifacts(&staged_path)
        .expect_err("a surviving companion without its main anchor must be preserved");

    assert!(
        format!("{error:#}").contains("pathname ownership is no longer provable"),
        "unexpected missing-main cleanup error: {error:#}"
    );
    assert_eq!(std::fs::read(&sidecar_path)?, b"unbound recovery bytes");
    Ok(())
}

#[cfg(unix)]
#[test]
fn rejected_export_cleanup_preserves_replacement_symlink_and_sidecars() -> Result<()> {
    use std::os::unix::fs::symlink;

    let temp_dir = TempDir::new()?;
    let staged_path = temp_dir.path().join("export.tmp.db");
    let replacement_target = temp_dir.path().join("replacement-target.db");
    let sidecar_path = sqlite_content_artifact_paths(&staged_path)
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("SQLite artifact family unexpectedly empty"))?;
    std::fs::write(&replacement_target, b"unowned replacement")?;
    symlink(&replacement_target, &staged_path)?;
    std::fs::write(&sidecar_path, b"unowned sidecar")?;

    let error = cleanup_sqlite_temp_artifacts(&staged_path)
        .expect_err("cleanup must not unlink a replacement symlink");

    assert!(
        format!("{error:#}").contains("no longer a regular file"),
        "unexpected replacement-entry error: {error:#}"
    );
    assert_eq!(std::fs::read(&replacement_target)?, b"unowned replacement");
    assert!(
        std::fs::symlink_metadata(&staged_path)?
            .file_type()
            .is_symlink(),
        "replacement symlink must be preserved"
    );
    assert_eq!(std::fs::read(&sidecar_path)?, b"unowned sidecar");
    Ok(())
}

#[test]
fn staged_finalization_rejects_content_sidecars_without_mutating_them() -> Result<()> {
    let content_paths = sqlite_content_artifact_paths(Path::new("export.tmp.db"));
    for relative_path in content_paths {
        let temp_dir = TempDir::new()?;
        let staged_path = temp_dir.path().join("export.tmp.db");
        let sentinel_path = temp_dir.path().join(relative_path);
        std::fs::write(&staged_path, b"staged main")?;
        std::fs::write(&sentinel_path, b"content-bearing sentinel")?;

        let error = finalize_staged_sqlite_sidecars(&staged_path)
            .expect_err("a content-bearing staged sidecar must block publication");
        let message = format!("{error:#}");
        if !message.contains(&sentinel_path.display().to_string()) {
            return Err(anyhow::anyhow!(
                "staged sidecar rejection omitted {}: {message}",
                sentinel_path.display()
            ));
        }
        if std::fs::read(&staged_path)? != b"staged main" {
            return Err(anyhow::anyhow!(
                "staged sidecar rejection mutated the main file for {}",
                sentinel_path.display()
            ));
        }
        if std::fs::read(&sentinel_path)? != b"content-bearing sentinel" {
            return Err(anyhow::anyhow!(
                "staged sidecar rejection mutated the sidecar {}",
                sentinel_path.display()
            ));
        }
    }
    Ok(())
}

#[test]
fn staged_finalization_rejects_parallel_wal_segments_without_mutation() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let staged_path = temp_dir.path().join("export.tmp.db");
    let segment_path = temp_dir.path().join("export.tmp.db-wal-seg-not-an-epoch");
    std::fs::write(&staged_path, b"staged main")?;
    std::fs::write(&segment_path, b"parallel WAL segment")?;

    let error = finalize_staged_sqlite_sidecars(&staged_path)
        .expect_err("a parallel WAL segment must block publication");
    assert!(
        format!("{error:#}").contains(&segment_path.display().to_string()),
        "WAL-segment rejection omitted exact artifact path"
    );
    assert_eq!(std::fs::read(&staged_path)?, b"staged main");
    assert_eq!(std::fs::read(&segment_path)?, b"parallel WAL segment");
    Ok(())
}

#[test]
fn staged_vacuum_candidate_rejects_even_a_valid_marker_at_birth() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let staged_path = temp_dir.path().join("export.tmp.db");
    let marker_path = crate::pages::sqlite_migration_marker_path(&staged_path);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    std::fs::write(&staged_path, b"staged main")?;
    let marker_bytes =
        format!(r#"{{"last_upgrade_version":1,"last_run_at":{now},"repairs_applied":[]}}"#);
    std::fs::write(&marker_path, marker_bytes.as_bytes())?;

    let error = finalize_staged_sqlite_sidecars(&staged_path)
        .expect_err("a VACUUM candidate must never carry a migration marker");
    assert!(
        format!("{error:#}").contains(&marker_path.display().to_string()),
        "candidate-marker rejection omitted exact marker path"
    );
    assert_eq!(std::fs::read(&staged_path)?, b"staged main");
    assert_eq!(std::fs::read(&marker_path)?, marker_bytes.as_bytes());
    Ok(())
}

#[test]
fn staged_finalization_rejects_runtime_sidecars_without_mutation() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let staged_path = temp_dir.path().join("export.tmp.db");
    std::fs::write(&staged_path, b"staged main")?;
    let runtime_sidecars = sqlite_runtime_artifact_paths(&staged_path);
    for sidecar in &runtime_sidecars {
        std::fs::write(sidecar, b"unowned runtime sentinel")?;
    }

    let error = finalize_staged_sqlite_sidecars(&staged_path)
        .expect_err("a VACUUM candidate must not consume runtime sidecars");
    assert!(
        runtime_sidecars
            .iter()
            .any(|sidecar| format!("{error:#}").contains(&sidecar.display().to_string())),
        "runtime-sidecar rejection omitted the exact conflicting path"
    );

    if std::fs::read(&staged_path)? != b"staged main" {
        return Err(anyhow::anyhow!(
            "staged finalization mutated the SQLite main file"
        ));
    }
    for sidecar in runtime_sidecars {
        if std::fs::read(&sidecar)? != b"unowned runtime sentinel" {
            return Err(anyhow::anyhow!(
                "staged finalization mutated runtime sidecar {}",
                sidecar.display()
            ));
        }
    }
    Ok(())
}

#[test]
fn vacuum_into_detaches_candidate_from_expected_builder_wal() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let builder_path = temp_dir.path().join("builder.db");
    let candidate_path = temp_dir.path().join("candidate.db");
    create_staged_export_file(&builder_path)?;

    let builder = Connection::open(builder_path.to_string_lossy().as_ref())?;
    builder.execute_batch(
        "PRAGMA page_size = 4096;
         PRAGMA journal_mode = 'delete';
         CREATE TABLE exported (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
         INSERT INTO exported VALUES (1, 'candidate row');",
    )?;
    let candidate_path_text = candidate_path.to_string_lossy();
    builder.execute_compat("VACUUM INTO ?1;", params![candidate_path_text.as_ref()])?;
    builder.close()?;
    enforce_private_candidate_permissions(&candidate_path)?;

    #[cfg(unix)]
    assert_eq!(
        std::fs::metadata(&candidate_path)?.permissions().mode() & 0o077,
        0,
        "VACUUM candidate must be private before verification"
    );

    let builder_wal = sqlite_content_artifact_paths(&builder_path)
        .into_iter()
        .find(|path| path.as_os_str().to_string_lossy().ends_with("-wal"))
        .ok_or_else(|| anyhow::anyhow!("shared artifact family omitted builder WAL"))?;
    reject_existing_sqlite_sidecars(&candidate_path, "VACUUM INTO candidate")?;
    assert!(
        std::fs::metadata(&candidate_path)?.len() > 0,
        "VACUUM INTO candidate must contain a database image"
    );

    cleanup_sqlite_temp_artifacts(&builder_path)?;
    assert!(
        !builder_path.exists(),
        "closed private builder survived exact-family cleanup"
    );
    assert!(
        !builder_wal.exists(),
        "closed private builder WAL survived exact-family cleanup"
    );

    let candidate = crate::pages::open_existing_sqlite_db(&candidate_path)?;
    let row = candidate.query_row("SELECT COUNT(*) FROM exported")?;
    assert_eq!(row.get_typed::<i64>(0)?, 1);
    let page_size = candidate
        .query_row("PRAGMA page_size;")?
        .get_typed::<i64>(0)?;
    assert_eq!(page_size, 4096);
    let journal_mode = candidate
        .query_row("PRAGMA journal_mode;")?
        .get_typed::<String>(0)?;
    assert_eq!(journal_mode, "delete");
    candidate.close()?;
    Ok(())
}

#[test]
fn replace_file_from_temp_via_backup_overwrites_existing_file() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let final_path = temp_dir.path().join("export.db");
    let temp_path = temp_dir.path().join("export.tmp");

    std::fs::write(&final_path, b"old export")?;
    std::fs::write(&temp_path, b"new export")?;

    let guard = acquire_export_publish_guard(&final_path)?;
    let mut retain_temp_on_error = false;
    let result = replace_file_from_temp_via_backup(
        &temp_path,
        &final_path,
        &mut retain_temp_on_error,
        &guard,
    );
    #[cfg(not(windows))]
    result?;
    #[cfg(windows)]
    {
        let error = result.expect_err(
            "Windows must preserve the last recoverable generation when namespace durability is unprovable",
        );
        assert!(format!("{error:#}").contains("cannot prove durable namespace cleanup"));
    }

    if !matches!(
        std::fs::read(&final_path)?.as_slice().cmp(b"new export"),
        std::cmp::Ordering::Equal
    ) {
        return Err(anyhow::anyhow!(
            "backup replacement did not publish temp bytes"
        ));
    }
    if temp_path.exists() {
        return Err(anyhow::anyhow!("export temp path was not consumed"));
    }
    if retain_temp_on_error {
        return Err(anyhow::anyhow!(
            "successful replacement incorrectly requested temp retention"
        ));
    }
    let journal_path = export_publish_recovery_journal_path(&final_path);
    #[cfg(not(windows))]
    assert!(
        std::fs::symlink_metadata(&journal_path).is_err()
            && first_unmarked_export_publish_backup(&final_path, None)?.is_none(),
        "successful replacement left a recovery artifact behind"
    );
    #[cfg(windows)]
    {
        let journal = read_export_publish_journal(&final_path)?
            .ok_or_else(|| anyhow::anyhow!("Windows publish did not retain its journal"))?;
        let backup_path = journal_backup_path(&final_path, &journal)?;
        assert!(
            backup_path.is_file(),
            "Windows publish lost its prior generation"
        );
        assert!(journal_path.is_file(), "Windows publish lost its journal");
    }

    Ok(())
}

#[test]
fn completed_backup_publish_reports_retained_sensitive_generation() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let final_path = temp_dir.path().join("export.db");
    let backup_path = unpredictable_export_publish_backup_path(&final_path)?;
    std::fs::write(&final_path, b"new live generation")?;
    std::fs::write(&backup_path, b"prior generation")?;
    let prior = inspect_export_regular_file(&backup_path, "test prior generation")?;
    let candidate = inspect_export_regular_file(&final_path, "test live generation")?;
    let guard = acquire_export_publish_guard(&final_path)?;
    let journal =
        write_export_publish_journal(&final_path, &backup_path, &prior, &candidate, &guard)?;
    std::fs::write(&backup_path, b"changed prior generation")?;

    let error = remove_prior_export_backup_after_publish(
        &backup_path,
        &final_path,
        &journal,
        Some(prior.identity),
        &guard,
    )
    .expect_err("a changed prior generation must not be silently removed");

    let message = format!("{error:#}");
    assert!(
        message.contains("new Pages export is live"),
        "partial-success state was not reported: {message}"
    );
    assert!(
        message.contains(&backup_path.display().to_string()),
        "retained backup path was not reported: {message}"
    );
    assert_eq!(std::fs::read(&final_path)?, b"new live generation");
    assert_eq!(std::fs::read(&backup_path)?, b"changed prior generation");
    assert!(
        export_publish_recovery_journal_path(&final_path).is_file(),
        "journal for retained prior generation must be preserved"
    );
    Ok(())
}

#[cfg(not(windows))]
#[test]
fn completed_publish_preserves_backup_when_journal_changes_before_cleanup() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let final_path = temp_dir.path().join("export.db");
    let backup_path = unpredictable_export_publish_backup_path(&final_path)?;
    std::fs::write(&final_path, b"new live generation")?;
    std::fs::write(&backup_path, b"prior generation")?;
    let prior = inspect_export_regular_file(&backup_path, "test prior generation")?;
    let candidate = inspect_export_regular_file(&final_path, "test live generation")?;
    let guard = acquire_export_publish_guard(&final_path)?;
    let journal =
        write_export_publish_journal(&final_path, &backup_path, &prior, &candidate, &guard)?;
    let mut changed_journal = serde_json::to_value(&journal)?;
    changed_journal["candidate_sha256"] = serde_json::Value::String("0".repeat(64));
    std::fs::write(
        export_publish_recovery_journal_path(&final_path),
        serde_json::to_vec(&changed_journal)?,
    )?;
    let error = remove_prior_export_backup_after_publish(
        &backup_path,
        &final_path,
        &journal,
        Some(prior.identity),
        &guard,
    )
    .expect_err("changed journal must block prior-generation cleanup");

    assert!(format!("{error:#}").contains("journal changed"));
    assert_eq!(std::fs::read(&backup_path)?, b"prior generation");
    assert!(export_publish_recovery_journal_path(&final_path).is_file());
    Ok(())
}

#[test]
fn test_replace_file_from_temp_overwrites_existing_file() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let final_path = temp_dir.path().join("export.db");
    let first_tmp = temp_dir.path().join("first.tmp");
    let second_tmp = temp_dir.path().join("second.tmp");
    let mut retain_temp_on_error = false;
    let guard = acquire_export_publish_guard(&final_path).expect("acquire publish guard");

    std::fs::write(&first_tmp, b"first").expect("write first temp");
    let first_result =
        replace_file_from_temp(&first_tmp, &final_path, &mut retain_temp_on_error, &guard);
    #[cfg(not(windows))]
    first_result.expect("initial replace");
    #[cfg(windows)]
    {
        let error = first_result
            .expect_err("Windows first publication must report namespace durability limits");
        assert!(format!("{error:#}").contains("cannot prove durable directory-entry"));
    }
    assert_eq!(
        std::fs::read(&final_path).expect("read first final"),
        b"first"
    );

    std::fs::write(&second_tmp, b"second").expect("write second temp");
    let second_result =
        replace_file_from_temp(&second_tmp, &final_path, &mut retain_temp_on_error, &guard);
    #[cfg(not(windows))]
    second_result.expect("overwrite replace");
    #[cfg(windows)]
    {
        let error = second_result.expect_err("Windows replacement must preserve recovery state");
        assert!(format!("{error:#}").contains("cannot prove durable namespace cleanup"));
    }
    assert_eq!(
        std::fs::read(&final_path).expect("read second final"),
        b"second"
    );
    assert!(!retain_temp_on_error);
}

#[test]
fn replacement_rejects_existing_sqlite_sidecars_without_mutating_artifacts() -> Result<()> {
    // Namespace identity records are exempt from the rejection policy;
    // see `sqlite_content_bearing_artifact_paths`.
    let artifact_paths = sqlite_fixed_artifact_paths(Path::new("export.db"))
        .into_iter()
        .filter(|path| !crate::pages::is_legacy_namespace_identity_record(path));
    for relative_path in artifact_paths {
        let temp_dir = TempDir::new()?;
        let final_path = temp_dir.path().join("export.db");
        let staged_path = temp_dir.path().join("export.tmp.db");
        let sentinel_path = temp_dir.path().join(relative_path);
        let artifact_label = sentinel_path.display().to_string();
        let old_generation = format!("old main for {artifact_label}");
        let new_generation = format!("new main for {artifact_label}");
        let sentinel = format!("sentinel sidecar for {artifact_label}");

        std::fs::write(&final_path, old_generation.as_bytes())?;
        std::fs::write(&staged_path, new_generation.as_bytes())?;
        std::fs::write(&sentinel_path, sentinel.as_bytes())?;

        let guard = acquire_export_publish_guard(&final_path)?;
        let mut retain_temp_on_error = false;
        let error =
            replace_file_from_temp(&staged_path, &final_path, &mut retain_temp_on_error, &guard)
                .expect_err("an existing SQLite sidecar must block main-file replacement");

        let message = format!("{error:#}");
        if !message.contains(&sentinel_path.display().to_string()) {
            return Err(anyhow::anyhow!(
                "sidecar rejection did not identify {}: {message}",
                sentinel_path.display()
            ));
        }
        if std::fs::read(&final_path)? != old_generation.as_bytes() {
            return Err(anyhow::anyhow!(
                "sidecar rejection mutated the prior main database for {artifact_label}"
            ));
        }
        if std::fs::read(&staged_path)? != new_generation.as_bytes() {
            return Err(anyhow::anyhow!(
                "sidecar rejection consumed the staged database for {artifact_label}"
            ));
        }
        if std::fs::read(&sentinel_path)? != sentinel.as_bytes() {
            return Err(anyhow::anyhow!(
                "sidecar rejection mutated the sentinel artifact for {artifact_label}"
            ));
        }
        if retain_temp_on_error {
            return Err(anyhow::anyhow!(
                "preflight sidecar rejection incorrectly marked a catastrophic replacement failure"
            ));
        }
    }
    Ok(())
}

#[test]
fn replacement_rejects_existing_parallel_wal_segment_without_mutation() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let final_path = temp_dir.path().join("export.db");
    let staged_path = temp_dir.path().join("export.tmp.db");
    let segment_path = temp_dir.path().join("export.db-wal-seg-42");
    std::fs::write(&final_path, b"old main")?;
    std::fs::write(&staged_path, b"new main")?;
    std::fs::write(&segment_path, b"old WAL segment")?;

    let guard = acquire_export_publish_guard(&final_path)?;
    let mut retain_temp_on_error = false;
    let error =
        replace_file_from_temp(&staged_path, &final_path, &mut retain_temp_on_error, &guard)
            .expect_err("an existing WAL segment must block main-file replacement");

    assert!(
        format!("{error:#}").contains(&segment_path.display().to_string()),
        "replacement refusal omitted exact WAL segment path"
    );
    assert_eq!(std::fs::read(&final_path)?, b"old main");
    assert_eq!(std::fs::read(&staged_path)?, b"new main");
    assert_eq!(std::fs::read(&segment_path)?, b"old WAL segment");
    assert!(!retain_temp_on_error);
    Ok(())
}
