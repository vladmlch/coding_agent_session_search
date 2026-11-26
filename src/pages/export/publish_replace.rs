use super::engine::{ExportEngine, ExportFilter, ExportStats, PathMode};
#[cfg(all(not(windows), test))]
use super::publish_guard::remove_export_publish_journal;
use super::publish_guard::{
    ExportPublishGuard, evidence_matches, inspect_export_regular_file,
    require_export_publish_guard, sync_export_regular_file,
};
#[cfg(any(windows, test))]
use super::publish_guard::{
    ExportPublishJournal, export_publish_recovery_journal_path,
    first_unmarked_export_publish_backup, read_export_publish_journal,
    unpredictable_export_publish_backup_path, validate_export_publish_journal,
    write_export_publish_journal,
};
use crate::pages::profiles::ShareProfile;
use crate::pages::{
    sqlite_artifact_paths, sqlite_content_bearing_artifact_paths, sqlite_sidecar_path,
};
use crate::time_parser::parse_time_input;
use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

fn cleanup_sqlite_sidecars(artifacts: Vec<PathBuf>) -> Result<()> {
    let mut first_error = None;
    for artifact in artifacts {
        match std::fs::remove_file(&artifact) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                if first_error.is_none() {
                    first_error = Some(anyhow::Error::new(err).context(format!(
                        "failed removing staged SQLite artifact {}",
                        artifact.display()
                    )));
                }
            }
        }
    }
    first_error.map_or(Ok(()), Err)
}

/// Legacy VFS layers stamped two zero-content namespace identity records
/// (`-fsqlite-ns-gate` / `-fsqlite-ns-use`) beside every `VACUUM INTO`
/// target. Once the staged candidate has been renamed onto the destination
/// its main file is gone, so those two records would otherwise remain as
/// unpredictable-named litter in the output directory. Remove exactly that
/// pair, tolerating absence.
///
/// Runs only after the export is live and durably synced, so a failure here
/// is logged, never surfaced: cosmetic litter must not turn a successful
/// publish into an error.
fn cleanup_candidate_namespace_identity_records(temp_path: &Path) {
    if let Err(err) = cleanup_sqlite_sidecars(vec![
        sqlite_sidecar_path(temp_path, "-fsqlite-ns-gate"),
        sqlite_sidecar_path(temp_path, "-fsqlite-ns-use"),
    ]) {
        tracing::warn!(
            error = %err,
            candidate = %temp_path.display(),
            "published Pages export is live, but its staged candidate's legacy namespace identity records could not be removed"
        );
    }
}

pub(super) fn finalize_staged_sqlite_sidecars(path: &Path) -> Result<()> {
    // The publishable path is a VACUUM INTO image and is never opened through
    // a read-write SQLite connection. It therefore owns no companion
    // artifacts at all. Even namespace or lock files at this random pathname
    // are unexpected entries, not builder residue, so preserve and reject
    // them. Only the separately owned, explicitly closed builder is cleaned.
    reject_existing_sqlite_sidecars(path, "staged VACUUM candidate")
}

pub(super) fn cleanup_sqlite_temp_artifacts(path: &Path) -> Result<()> {
    // Resolve the complete exact family before removing the main path. If the
    // bounded directory scan cannot prove the dynamic WAL-segment set, fail
    // without mutation rather than losing the namespace anchor first.
    let sidecars = sqlite_artifact_paths(path)?;
    let main_metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return ensure_no_unbound_sqlite_sidecars(path, sidecars);
        }
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "failed inspecting staged SQLite artifact {} before cleanup",
                    path.display()
                )
            });
        }
    };
    if !main_metadata.file_type().is_file() {
        bail!(
            "staged SQLite artifact {} is no longer a regular file; preserved every companion",
            path.display()
        );
    }
    #[cfg(unix)]
    if main_metadata.nlink() != 1 {
        bail!(
            "staged SQLite artifact {} has {} hard links; preserved every companion because exclusive pathname ownership is no longer provable",
            path.display(),
            main_metadata.nlink()
        );
    }

    match std::fs::remove_file(path) {
        Ok(()) => cleanup_sqlite_sidecars(sidecars),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            ensure_no_unbound_sqlite_sidecars(path, sidecars)
        }
        Err(error) => Err(anyhow::Error::new(error).context(format!(
            "failed removing staged SQLite artifact {}; preserved every companion",
            path.display()
        ))),
    }
}

fn ensure_no_unbound_sqlite_sidecars(path: &Path, sidecars: Vec<PathBuf>) -> Result<()> {
    for sidecar in sidecars {
        match std::fs::symlink_metadata(&sidecar) {
            Ok(_) => {
                bail!(
                    "staged SQLite main file {} disappeared before cleanup while companion {} still exists; preserved the companion because pathname ownership is no longer provable",
                    path.display(),
                    sidecar.display()
                );
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "failed inspecting staged SQLite companion {} after main-file disappearance",
                        sidecar.display()
                    )
                });
            }
        }
    }
    Ok(())
}

pub(super) fn create_staged_export_file(path: &Path) -> Result<()> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    options
        .open(path)
        .with_context(|| format!("failed securely creating staged export {}", path.display()))?;
    Ok(())
}

#[cfg(unix)]
pub(super) fn enforce_private_candidate_permissions(path: &Path) -> Result<()> {
    let path_metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("failed inspecting staged export {}", path.display()))?;
    if !path_metadata.file_type().is_file() {
        bail!(
            "staged Pages export is not a regular file: {}",
            path.display()
        );
    }
    let file = std::fs::File::open(path)
        .with_context(|| format!("failed opening staged export {} for chmod", path.display()))?;
    let metadata = file
        .metadata()
        .with_context(|| format!("failed inspecting staged export {}", path.display()))?;
    if !metadata.file_type().is_file() {
        bail!(
            "staged Pages export is not a regular file: {}",
            path.display()
        );
    }
    if (path_metadata.dev(), path_metadata.ino()) != (metadata.dev(), metadata.ino()) {
        bail!(
            "staged Pages export {} changed identity before permission enforcement",
            path.display()
        );
    }
    file.set_permissions(std::fs::Permissions::from_mode(0o600))
        .with_context(|| {
            format!(
                "failed setting staged export {} to mode 0600",
                path.display()
            )
        })?;
    file.sync_all().with_context(|| {
        format!(
            "failed syncing staged export {} after setting mode 0600",
            path.display()
        )
    })?;
    let mode = file
        .metadata()
        .with_context(|| format!("failed verifying staged export mode for {}", path.display()))?
        .permissions()
        .mode();
    if mode & 0o077 != 0 {
        bail!(
            "staged Pages export {} retained non-owner permission bits after chmod: {mode:o}",
            path.display()
        );
    }
    let final_path_metadata = std::fs::symlink_metadata(path).with_context(|| {
        format!(
            "failed re-inspecting staged export {} after setting mode 0600",
            path.display()
        )
    })?;
    if !final_path_metadata.file_type().is_file()
        || (final_path_metadata.dev(), final_path_metadata.ino())
            != (metadata.dev(), metadata.ino())
    {
        bail!(
            "staged Pages export {} changed identity during permission enforcement",
            path.display()
        );
    }
    Ok(())
}

#[cfg(not(unix))]
pub(super) fn enforce_private_candidate_permissions(path: &Path) -> Result<()> {
    let evidence = inspect_export_regular_file(path, "staged Pages export")?;
    sync_export_regular_file(path, &evidence, "staged Pages export")
}

/// Refuse to publish one SQLite main file over an existing artifact family.
///
/// A WAL, shared-memory file, or rollback journal beside `final_path` may
/// contain state belonging to the prior main database generation. Replacing
/// only the main file while retaining any of those sidecars can therefore make
/// readers observe a mixed or corrupt generation. The exporter cannot safely
/// decide that an existing sidecar is stale, so preserve it and fail closed.
pub(super) fn reject_existing_sqlite_sidecars(path: &Path, artifact_label: &str) -> Result<()> {
    // Namespace identity records are exempt: legacy VFS layers stamped them next
    // to every database they touched (VACUUM INTO targets included) and they
    // carry no database content, so they cannot make a main-file-only
    // artifact incomplete.
    for sidecar in sqlite_content_bearing_artifact_paths(path)? {
        match std::fs::symlink_metadata(&sidecar) {
            Ok(_) => {
                bail!(
                    "refusing main-file-only Pages export publication because {artifact_label} {} has SQLite sidecar {}; close every process using that artifact and preserve or move the complete SQLite family before retrying",
                    path.display(),
                    sidecar.display()
                );
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(err).with_context(|| {
                    format!(
                        "failed inspecting SQLite sidecar {} for {artifact_label} {}",
                        sidecar.display(),
                        path.display()
                    )
                });
            }
        }
    }
    Ok(())
}

#[cfg(any(windows, test))]
pub(super) fn replacement_path_entry_exists(path: &Path) -> Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(err) if matches!(err.kind(), std::io::ErrorKind::NotFound) => Ok(false),
        Err(err) => {
            Err(err).with_context(|| format!("failed inspecting export path {}", path.display()))
        }
    }
}

/// The backup fallback can recover only an existing regular database file.
/// Reject links and special filesystem entries before moving anything so a
/// crash can never strand an entry that the recovery path must not trust.
#[cfg(any(windows, test))]
pub(super) fn reject_non_regular_existing_publish_destination(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => {
            #[cfg(unix)]
            if metadata.nlink() != 1 {
                bail!(
                    "existing Pages export destination {} has {} hard links; refused recoverable replacement because exclusive pathname ownership is not provable",
                    path.display(),
                    metadata.nlink()
                );
            }
            Ok(())
        }
        Ok(_) => {
            bail!(
                "existing Pages export destination {} is not a regular file; refused backup replacement without mutation",
                path.display()
            )
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| {
            format!(
                "failed inspecting existing Pages export destination {}",
                path.display()
            )
        }),
    }
}

/// Recover only states authenticated by a file-synced journal. Its namespace
/// is also directory-synced where the platform exposes that operation. An
/// unmarked file at any reserved backup spelling is never treated as
/// cass-owned: it is preserved and reported for explicit operator resolution.
#[cfg(any(windows, test))]
pub(super) fn recover_or_refuse_interrupted_export_publish(
    final_path: &Path,
    publish_guard: &ExportPublishGuard,
) -> Result<()> {
    require_export_publish_guard(final_path, publish_guard)?;
    let Some(journal) = read_export_publish_journal(final_path)? else {
        if let Some(unmarked_backup) = first_unmarked_export_publish_backup(final_path, None)? {
            bail!(
                "unmarked Pages export recovery artifact {} was preserved; refusing to infer ownership or modify live path {}",
                unmarked_backup.display(),
                final_path.display()
            );
        }
        return Ok(());
    };
    let backup_path = validate_export_publish_journal(final_path, &journal)?;
    if let Some(unmarked_backup) =
        first_unmarked_export_publish_backup(final_path, Some(&backup_path))?
    {
        bail!(
            "Pages export publish journal identifies {}, but additional unmarked recovery artifact {} also exists; preserved every generation",
            backup_path.display(),
            unmarked_backup.display()
        );
    }

    let backup_evidence = if replacement_path_entry_exists(&backup_path)? {
        reject_existing_sqlite_sidecars(&backup_path, "publish-recovery backup")?;
        let evidence = inspect_export_regular_file(&backup_path, "publish-recovery backup")?;
        if !evidence_matches(&evidence, journal.prior_size_bytes, &journal.prior_sha256) {
            bail!(
                "Pages export publish-recovery backup {} does not match the journaled prior generation; preserved it without mutation",
                backup_path.display()
            );
        }
        Some(evidence)
    } else {
        None
    };
    reject_existing_sqlite_sidecars(final_path, "interrupted publish destination")?;
    let final_evidence = if replacement_path_entry_exists(final_path)? {
        reject_non_regular_existing_publish_destination(final_path)?;
        Some(inspect_export_regular_file(
            final_path,
            "interrupted publish destination",
        )?)
    } else {
        None
    };

    match (final_evidence, backup_evidence) {
        (None, Some(prior)) => {
            if replacement_path_entry_exists(final_path)? {
                bail!(
                    "journaled Pages export backup {} was ready to restore, but live path {} was concurrently repopulated; preserved every entry and the journal",
                    backup_path.display(),
                    final_path.display()
                );
            }
            require_export_publish_guard(final_path, publish_guard)?;
            std::fs::rename(&backup_path, final_path).with_context(|| {
                format!(
                    "failed restoring journaled Pages export backup {} to missing live path {}; preserved the backup and journal",
                    backup_path.display(),
                    final_path.display()
                )
            })?;
            let restored = inspect_export_regular_file(final_path, "restored Pages export")?;
            if restored.identity != prior.identity
                || !evidence_matches(&restored, journal.prior_size_bytes, &journal.prior_sha256)
            {
                bail!(
                    "restored Pages export {} does not retain the journaled prior-generation identity; preserved the journal",
                    final_path.display()
                );
            }
            sync_export_regular_file(final_path, &restored, "restored Pages export")?;
            sync_parent_directory(final_path).with_context(|| {
                format!(
                    "the prior Pages export was restored at {}, but the recovery rename could not be durably synced; journal retained at {}",
                    final_path.display(),
                    export_publish_recovery_journal_path(final_path).display()
                )
            })?;
            #[cfg(windows)]
            bail!(
                "restored the prior Pages export at {}, but Windows std cannot prove durable deletion of the recovery namespace; journal retained at {} for explicit resolution",
                final_path.display(),
                export_publish_recovery_journal_path(final_path).display()
            );
            #[cfg(not(windows))]
            remove_export_publish_journal(final_path, &journal, publish_guard)
        }
        (Some(live), Some(_prior)) => {
            if !evidence_matches(
                &live,
                journal.candidate_size_bytes,
                &journal.candidate_sha256,
            ) {
                bail!(
                    "journaled Pages export recovery found live {} and prior {}; live bytes do not match the journaled candidate, so both were preserved",
                    final_path.display(),
                    backup_path.display()
                );
            }
            #[cfg(windows)]
            bail!(
                "new Pages export is live at {} and journaled prior generation remains at {}; Windows std cannot prove durable namespace cleanup, so backup and journal were preserved",
                final_path.display(),
                backup_path.display()
            );
            #[cfg(not(windows))]
            remove_prior_export_backup_after_publish(
                &backup_path,
                final_path,
                &journal,
                Some(_prior.identity),
                publish_guard,
            )
        }
        (Some(live), None) => {
            let live_is_prior =
                evidence_matches(&live, journal.prior_size_bytes, &journal.prior_sha256);
            let live_is_candidate = evidence_matches(
                &live,
                journal.candidate_size_bytes,
                &journal.candidate_sha256,
            );
            if !live_is_prior && !live_is_candidate {
                bail!(
                    "Pages export publish journal remains at {}, but live file {} matches neither journaled generation; preserved both",
                    export_publish_recovery_journal_path(final_path).display(),
                    final_path.display()
                );
            }
            #[cfg(windows)]
            bail!(
                "Pages export {} matches a journaled generation, but its backup namespace is absent and Windows std cannot prove the completed transition; journal retained at {}",
                final_path.display(),
                export_publish_recovery_journal_path(final_path).display()
            );
            #[cfg(not(windows))]
            remove_export_publish_journal(final_path, &journal, publish_guard)
        }
        (None, None) => bail!(
            "Pages export publish journal remains at {}, but both live {} and journaled backup {} are missing; preserved the journal",
            export_publish_recovery_journal_path(final_path).display(),
            final_path.display(),
            backup_path.display()
        ),
    }
}

#[cfg(any(windows, test))]
pub(super) fn replace_file_from_temp_via_backup(
    temp_path: &Path,
    final_path: &Path,
    retain_temp_on_error: &mut bool,
    publish_guard: &ExportPublishGuard,
) -> Result<()> {
    *retain_temp_on_error = false;
    require_export_publish_guard(final_path, publish_guard)?;
    recover_or_refuse_interrupted_export_publish(final_path, publish_guard)?;
    reject_non_regular_existing_publish_destination(final_path)?;
    reject_existing_sqlite_sidecars(final_path, "destination before backup replacement")?;

    let prior = inspect_export_regular_file(final_path, "prior Pages export")?;
    let candidate = inspect_export_regular_file(temp_path, "staged Pages export")?;
    sync_export_regular_file(temp_path, &candidate, "staged Pages export")?;
    let backup_path = unpredictable_export_publish_backup_path(final_path)?;
    if replacement_path_entry_exists(&backup_path)? {
        bail!(
            "unpredictable Pages export backup path unexpectedly exists; preserved every generation: {}",
            backup_path.display()
        );
    }
    let journal =
        write_export_publish_journal(final_path, &backup_path, &prior, &candidate, publish_guard)?;
    if replacement_path_entry_exists(&backup_path)? {
        bail!(
            "journaled Pages export backup path {} appeared before the prior generation could be parked; preserved it, the live generation, staged candidate, and journal without mutation",
            backup_path.display()
        );
    }
    let live_probe = inspect_export_regular_file(final_path, "prior Pages export")?;
    if live_probe.identity != prior.identity
        || !evidence_matches(&live_probe, journal.prior_size_bytes, &journal.prior_sha256)
    {
        bail!(
            "prior Pages export {} changed identity or content after its publish journal was created; preserved the live and staged generations and journal without mutation",
            final_path.display()
        );
    }

    require_export_publish_guard(final_path, publish_guard)?;
    std::fs::rename(final_path, &backup_path).with_context(|| {
        format!(
            "failed parking prior Pages export {} at journaled backup {}; staged candidate and journal were preserved",
            final_path.display(),
            backup_path.display()
        )
    })?;
    let parked = inspect_export_regular_file(&backup_path, "parked prior Pages export")?;
    if parked.identity != prior.identity
        || !evidence_matches(&parked, journal.prior_size_bytes, &journal.prior_sha256)
    {
        *retain_temp_on_error = true;
        bail!(
            "parked Pages export {} changed identity or content; preserved it, the staged candidate {}, and the publish journal",
            backup_path.display(),
            temp_path.display()
        );
    }
    sync_export_regular_file(&backup_path, &parked, "parked prior Pages export")?;
    sync_parent_directory(&backup_path).with_context(|| {
        format!(
            "prior Pages export was parked at {} and staged candidate remains at {}, but the backup rename could not be durably synced; journal preserved",
            backup_path.display(),
            temp_path.display()
        )
    })?;

    require_export_publish_guard(final_path, publish_guard)?;
    match std::fs::rename(temp_path, final_path) {
        Ok(()) => {
            let published =
                inspect_export_regular_file(final_path, "published Pages export candidate")?;
            if published.identity != candidate.identity
                || !evidence_matches(
                    &published,
                    journal.candidate_size_bytes,
                    &journal.candidate_sha256,
                )
            {
                *retain_temp_on_error = true;
                bail!(
                    "published Pages export {} changed identity or content; prior generation and journal retained at {}",
                    final_path.display(),
                    backup_path.display()
                );
            }
            sync_export_regular_file(final_path, &published, "published Pages export candidate")?;
            sync_parent_directory(final_path).with_context(|| {
                format!(
                    "new Pages export is live at {} and prior generation remains at {}, but publication could not be durably synced; journal preserved",
                    final_path.display(),
                    backup_path.display()
                )
            })?;
            cleanup_candidate_namespace_identity_records(temp_path);
            remove_prior_export_backup_after_publish(
                &backup_path,
                final_path,
                &journal,
                Some(prior.identity),
                publish_guard,
            )
        }
        Err(publish_error) => {
            let restore_probe =
                inspect_export_regular_file(&backup_path, "parked prior Pages export")?;
            if restore_probe.identity != prior.identity
                || !evidence_matches(
                    &restore_probe,
                    journal.prior_size_bytes,
                    &journal.prior_sha256,
                )
            {
                *retain_temp_on_error = true;
                bail!(
                    "failed publishing staged Pages export {} at {}: {}; parked prior generation {} no longer matches its journal, so every artifact was preserved",
                    temp_path.display(),
                    final_path.display(),
                    publish_error,
                    backup_path.display()
                );
            }
            if replacement_path_entry_exists(final_path)? {
                *retain_temp_on_error = true;
                bail!(
                    "failed publishing staged Pages export {} at {}: {}; the live path was concurrently repopulated, so staged candidate {}, journaled prior generation {}, and current live entry were preserved",
                    temp_path.display(),
                    final_path.display(),
                    publish_error,
                    temp_path.display(),
                    backup_path.display()
                );
            }
            require_export_publish_guard(final_path, publish_guard)?;
            match std::fs::rename(&backup_path, final_path) {
                Ok(()) => {
                    let restored =
                        inspect_export_regular_file(final_path, "restored prior Pages export")?;
                    if restored.identity != prior.identity
                        || !evidence_matches(
                            &restored,
                            journal.prior_size_bytes,
                            &journal.prior_sha256,
                        )
                    {
                        *retain_temp_on_error = true;
                        bail!(
                            "failed publishing staged Pages export and restored path {} changed identity or content; staged candidate and journal preserved",
                            final_path.display()
                        );
                    }
                    sync_export_regular_file(final_path, &restored, "restored prior Pages export")?;
                    sync_parent_directory(final_path).with_context(|| {
                        format!(
                            "restored prior Pages export at {}, but could not durably sync the rollback; journal preserved",
                            final_path.display()
                        )
                    })?;
                    #[cfg(windows)]
                    return Err(publish_error).context(format!(
                        "restored prior Pages export at {}, but Windows std cannot prove durable journal cleanup; journal retained at {}",
                        final_path.display(),
                        export_publish_recovery_journal_path(final_path).display()
                    ));
                    #[cfg(not(windows))]
                    {
                        remove_export_publish_journal(final_path, &journal, publish_guard)?;
                        Err(publish_error).with_context(|| {
                            format!(
                                "failed publishing staged Pages export {} at {}; restored the prior generation",
                                temp_path.display(),
                                final_path.display()
                            )
                        })
                    }
                }
                Err(restore_error) => {
                    *retain_temp_on_error = true;
                    bail!(
                        "failed publishing staged Pages export {} at {}: {}; restore error: {}; staged candidate retained at {}; prior generation retained at {}; journal retained at {}",
                        temp_path.display(),
                        final_path.display(),
                        publish_error,
                        restore_error,
                        temp_path.display(),
                        backup_path.display(),
                        export_publish_recovery_journal_path(final_path).display()
                    );
                }
            }
        }
    }
}

#[cfg(any(windows, test))]
pub(super) fn remove_prior_export_backup_after_publish(
    backup_path: &Path,
    final_path: &Path,
    journal: &ExportPublishJournal,
    expected_identity: Option<crate::franken_sync::FileIdentity>,
    publish_guard: &ExportPublishGuard,
) -> Result<()> {
    require_export_publish_guard(final_path, publish_guard)?;
    #[cfg(windows)]
    {
        let _ = (journal, expected_identity);
        bail!(
            "new Pages export is live at {}, but Windows std cannot prove durable namespace cleanup; prior generation {} and journal {} were preserved",
            final_path.display(),
            backup_path.display(),
            export_publish_recovery_journal_path(final_path).display()
        );
    }

    #[cfg(not(windows))]
    {
        let journaled_backup_path = validate_export_publish_journal(final_path, journal)?;
        if journaled_backup_path != backup_path {
            bail!(
                "new Pages export is live at {}, but backup {} is not the path owned by its publish journal {}; preserved it without mutation",
                final_path.display(),
                backup_path.display(),
                export_publish_recovery_journal_path(final_path).display()
            );
        }
        let current_journal = read_export_publish_journal(final_path)?.ok_or_else(|| {
            anyhow::anyhow!(
                "new Pages export is live at {}, but its publish journal disappeared before backup cleanup; prior generation {} was preserved",
                final_path.display(),
                backup_path.display()
            )
        })?;
        if current_journal != *journal {
            bail!(
                "new Pages export is live at {}, but its publish journal changed before backup cleanup; prior generation {} and the current journal were preserved",
                final_path.display(),
                backup_path.display()
            );
        }
        let cleanup_probe =
            inspect_export_regular_file(backup_path, "prior Pages export cleanup backup")?;
        #[cfg(unix)]
        {
            let cleanup_metadata = std::fs::symlink_metadata(backup_path).with_context(|| {
                format!(
                    "failed re-inspecting prior Pages export cleanup backup {}",
                    backup_path.display()
                )
            })?;
            if cleanup_metadata.nlink() != 1 {
                bail!(
                    "new Pages export is live at {}, but prior-generation backup {} has {} hard links; backup and journal were preserved",
                    final_path.display(),
                    backup_path.display(),
                    cleanup_metadata.nlink()
                );
            }
        }
        if expected_identity
            .map(|identity| identity != cleanup_probe.identity)
            .unwrap_or(false)
            || !evidence_matches(
                &cleanup_probe,
                journal.prior_size_bytes,
                &journal.prior_sha256,
            )
        {
            bail!(
                "new Pages export is live at {}, but prior-generation backup {} changed identity or content; backup and journal were preserved",
                final_path.display(),
                backup_path.display()
            );
        }
        require_export_publish_guard(final_path, publish_guard)?;
        std::fs::remove_file(backup_path).with_context(|| {
            format!(
                "new Pages export is live at {}, but the validated prior sensitive generation remains at {}",
                final_path.display(),
                backup_path.display()
            )
        })?;
        sync_parent_directory(backup_path).with_context(|| {
            format!(
                "new Pages export is live at {} and prior backup {} was removed, but backup deletion could not be durably synced; journal preserved",
                final_path.display(),
                backup_path.display()
            )
        })?;
        remove_export_publish_journal(final_path, journal, publish_guard)
    }
}

pub(super) fn replace_file_from_temp(
    temp_path: &Path,
    final_path: &Path,
    retain_temp_on_error: &mut bool,
    publish_guard: &ExportPublishGuard,
) -> Result<()> {
    *retain_temp_on_error = false;
    require_export_publish_guard(final_path, publish_guard)?;
    let candidate = inspect_export_regular_file(temp_path, "staged Pages export")?;
    sync_export_regular_file(temp_path, &candidate, "staged Pages export")?;
    #[cfg(windows)]
    recover_or_refuse_interrupted_export_publish(final_path, publish_guard)?;
    #[cfg(windows)]
    reject_non_regular_existing_publish_destination(final_path)?;
    reject_existing_sqlite_sidecars(final_path, "destination")?;
    #[cfg(windows)]
    {
        if replacement_path_entry_exists(final_path)? {
            return replace_file_from_temp_via_backup(
                temp_path,
                final_path,
                retain_temp_on_error,
                publish_guard,
            );
        }
        require_export_publish_guard(final_path, publish_guard)?;
        std::fs::rename(temp_path, final_path).with_context(|| {
            format!(
                "failed renaming completed export {} into place at {}",
                temp_path.display(),
                final_path.display()
            )
        })?;
        let published = inspect_export_regular_file(final_path, "published Pages export")?;
        if published.identity != candidate.identity
            || !evidence_matches(&published, candidate.size_bytes, &candidate.sha256)
        {
            *retain_temp_on_error = true;
            bail!(
                "published Pages export {} changed identity or content after its staged rename",
                final_path.display()
            );
        }
        sync_export_regular_file(final_path, &published, "published Pages export")?;
        sync_parent_directory(final_path).with_context(|| {
            format!(
                "new Pages export is live at {}, but its first publication could not be durably synced",
                final_path.display()
            )
        })?;
        cleanup_candidate_namespace_identity_records(temp_path);
        bail!(
            "new Pages export is live at {}, but Windows std cannot prove durable directory-entry publication; no prior generation existed to retain",
            final_path.display()
        )
    }

    #[cfg(not(windows))]
    {
        require_export_publish_guard(final_path, publish_guard)?;
        std::fs::rename(temp_path, final_path).with_context(|| {
            format!(
                "failed renaming completed export {} into place at {}",
                temp_path.display(),
                final_path.display()
            )
        })?;
        let published = inspect_export_regular_file(final_path, "published Pages export")?;
        if published.identity != candidate.identity
            || !evidence_matches(&published, candidate.size_bytes, &candidate.sha256)
        {
            *retain_temp_on_error = true;
            bail!(
                "published Pages export {} changed identity or content after its staged rename",
                final_path.display()
            );
        }
        sync_export_regular_file(final_path, &published, "published Pages export")?;
        sync_parent_directory(final_path).with_context(|| {
            format!(
                "new Pages export is live at {}, but its publication could not be durably synced",
                final_path.display()
            )
        })?;
        cleanup_candidate_namespace_identity_records(temp_path);
        Ok(())
    }
}

#[cfg(not(windows))]
pub(super) fn sync_parent_directory(path: &Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    std::fs::File::open(parent)
        .with_context(|| format!("failed opening parent directory {}", parent.display()))?
        .sync_all()
        .with_context(|| format!("failed syncing parent directory {}", parent.display()))
}

#[cfg(windows)]
// `std` exposes no safe directory-handle sync on Windows. Callers use this as
// a sequencing point, then return a partial-success error while preserving the
// journal/recoverable generation instead of claiming namespace durability.
pub(super) fn sync_parent_directory(_path: &Path) -> Result<()> {
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn run_pages_export(
    db_path: Option<PathBuf>,
    output_path: PathBuf,
    agents: Option<Vec<String>>,
    workspaces: Option<Vec<String>>,
    since: Option<String>,
    until: Option<String>,
    path_mode: PathMode,
    share_profile: ShareProfile,
    dry_run: bool,
) -> Result<()> {
    if dry_run {
        println!("Dry run: would export to {:?}", output_path);
        return Ok(());
    }

    println!("Exporting to {:?}...", output_path);
    let (stats, ()) = export_pages_database_verified(
        db_path,
        output_path,
        agents,
        workspaces,
        since,
        until,
        path_mode,
        share_profile,
        |current, total| {
            if total > 0 && current % 100 == 0 {
                use std::io::Write;
                print!("\rProcessed {}/{} conversations...", current, total);
                std::io::stdout().flush().ok();
            }
        },
        |_| Ok(()),
    )?;
    println!(
        "\rExport complete! Processed {} conversations, {} messages.",
        stats.conversations_processed, stats.messages_processed
    );

    Ok(())
}

/// Export a filtered Pages database and verify its private staged generation
/// before the final output path is replaced.
#[allow(clippy::too_many_arguments)]
pub fn export_pages_database_verified<F, V, T>(
    db_path: Option<PathBuf>,
    output_path: PathBuf,
    agents: Option<Vec<String>>,
    workspaces: Option<Vec<String>>,
    since: Option<String>,
    until: Option<String>,
    path_mode: PathMode,
    share_profile: ShareProfile,
    progress: F,
    verifier: V,
) -> Result<(ExportStats, T)>
where
    F: Fn(usize, usize),
    V: FnOnce(&Path) -> Result<T>,
{
    let db_path = db_path.unwrap_or_else(crate::default_db_path);

    let since_dt = parse_export_time_arg("--since", since.as_deref())?;
    let until_dt = parse_export_time_arg("--until", until.as_deref())?;

    if let (Some(since_dt), Some(until_dt)) = (since_dt, until_dt)
        && since_dt > until_dt
    {
        bail!(
            "Invalid time range: --since ({}) is after --until ({})",
            since_dt.to_rfc3339(),
            until_dt.to_rfc3339()
        );
    }

    let workspaces_path = workspaces.map(|ws| ws.into_iter().map(PathBuf::from).collect());

    let filter = ExportFilter {
        agents,
        workspaces: workspaces_path,
        since: since_dt,
        until: until_dt,
        path_mode,
    };

    let engine =
        ExportEngine::new(&db_path, &output_path, filter).with_share_profile(share_profile);
    engine.execute_verified(progress, None, verifier)
}

fn parse_export_time_arg(
    flag_name: &str,
    raw_value: Option<&str>,
) -> Result<Option<DateTime<Utc>>> {
    let Some(raw_value) = raw_value else {
        return Ok(None);
    };

    let timestamp = parse_time_input(raw_value)
        .ok_or_else(|| anyhow::anyhow!("Invalid {flag_name} value: {raw_value}"))?;
    let parsed = DateTime::from_timestamp_millis(timestamp)
        .ok_or_else(|| anyhow::anyhow!("{flag_name} value is out of range: {raw_value}"))?;
    Ok(Some(parsed))
}
