use anyhow::{Context, Result, bail};
#[cfg(windows)]
use std::fs;
#[cfg(not(windows))]
use std::fs::{self, File};
use std::path::Path;

use super::publish_journal::{
    BundlePublishJournal, BundleTreeEvidence, acquire_bundle_publish_guard,
    bundle_path_entry_exists, bundle_publish_recovery_journal_path, ensure_bundle_directory_entry,
    ensure_bundle_tree_matches, ensure_replaceable_bundle_output_dir,
    first_unowned_bundle_publish_backup, inspect_bundle_tree, journal_candidate_evidence,
    journal_prior_evidence, read_bundle_publish_journal, remove_bundle_publish_journal,
    require_bundle_publish_guard, unpredictable_bundle_publish_backup_path,
    validate_bundle_publish_journal, write_bundle_publish_journal,
};

pub(super) fn replace_dir_from_temp(
    temp_dir: &Path,
    final_dir: &Path,
    retain_temp_on_error: &mut bool,
) -> Result<()> {
    *retain_temp_on_error = false;
    if !ensure_bundle_directory_entry(temp_dir, "staged bundle path")? {
        bail!("staged bundle path does not exist: {}", temp_dir.display());
    }

    let publish_guard = acquire_bundle_publish_guard(final_dir)?;
    require_bundle_publish_guard(final_dir, &publish_guard)?;
    recover_interrupted_bundle_publish(final_dir)?;
    require_bundle_publish_guard(final_dir, &publish_guard)?;
    let candidate = inspect_bundle_tree(temp_dir, "staged bundle path")?;
    if !ensure_replaceable_bundle_output_dir(final_dir)? {
        require_bundle_publish_guard(final_dir, &publish_guard)?;
        fs::rename(temp_dir, final_dir).with_context(|| {
            format!(
                "failed renaming completed bundle {} into place at {}",
                temp_dir.display(),
                final_dir.display()
            )
        })?;
        ensure_bundle_tree_matches(final_dir, "first published bundle", &candidate)?;
        sync_parent_directory(final_dir).with_context(|| {
            format!(
                "completed bundle is live at {}, but its first publication could not be durably synced",
                final_dir.display()
            )
        })?;
        return Ok(());
    }

    let prior = inspect_bundle_tree(final_dir, "prior live bundle")?;
    let backup_dir = unpredictable_bundle_publish_backup_path(final_dir)?;
    if bundle_path_entry_exists(&backup_dir)? {
        bail!(
            "unpredictable Pages bundle backup path unexpectedly exists; preserved every generation: {}",
            backup_dir.display()
        );
    }
    require_bundle_publish_guard(final_dir, &publish_guard)?;
    let journal = write_bundle_publish_journal(final_dir, &backup_dir, &prior, &candidate)?;
    if bundle_path_entry_exists(&backup_dir)? {
        bail!(
            "journaled Pages bundle backup path {} appeared before publication; preserved every generation and the journal",
            backup_dir.display()
        );
    }
    ensure_bundle_tree_matches(final_dir, "prior live bundle", &prior)?;
    ensure_bundle_tree_matches(temp_dir, "staged bundle path", &candidate)?;
    require_bundle_publish_guard(final_dir, &publish_guard)?;

    #[cfg(target_os = "linux")]
    {
        if try_publish_linux_bundle_via_atomic_exchange(
            temp_dir,
            final_dir,
            &backup_dir,
            &journal,
            &prior,
            &candidate,
            retain_temp_on_error,
        )? {
            return Ok(());
        }
    }

    replace_dir_from_temp_via_recoverable_rename_pair(
        temp_dir,
        final_dir,
        &backup_dir,
        &journal,
        &prior,
        &candidate,
        retain_temp_on_error,
    )
}

pub(super) fn recover_interrupted_bundle_publish(final_dir: &Path) -> Result<()> {
    let Some(journal) = read_bundle_publish_journal(final_dir)? else {
        if let Some(unowned_backup) = first_unowned_bundle_publish_backup(final_dir, None)? {
            bail!(
                "unowned Pages bundle recovery artifact {} was preserved; refusing to infer ownership or modify live path {}",
                unowned_backup.display(),
                final_dir.display()
            );
        }
        return Ok(());
    };
    let backup_dir = validate_bundle_publish_journal(final_dir, &journal)?;
    if let Some(unowned_backup) = first_unowned_bundle_publish_backup(final_dir, Some(&backup_dir))?
    {
        bail!(
            "Pages bundle publish journal identifies {}, but additional unowned recovery artifact {} also exists; preserved every generation",
            backup_dir.display(),
            unowned_backup.display()
        );
    }
    let prior = journal_prior_evidence(&journal);
    let candidate = journal_candidate_evidence(&journal);
    let final_evidence = if ensure_replaceable_bundle_output_dir(final_dir)? {
        Some(inspect_bundle_tree(
            final_dir,
            "interrupted bundle publish destination",
        )?)
    } else {
        None
    };
    let backup_evidence = if bundle_path_entry_exists(&backup_dir)? {
        Some(inspect_bundle_tree(
            &backup_dir,
            "journaled bundle publish backup",
        )?)
    } else {
        None
    };

    match (final_evidence, backup_evidence) {
        (None, Some(backup)) if backup == prior => {
            if bundle_path_entry_exists(final_dir)? {
                bail!(
                    "journaled prior Pages bundle {} was ready to restore, but live path {} was concurrently repopulated; preserved every entry and the journal",
                    backup_dir.display(),
                    final_dir.display()
                );
            }
            ensure_bundle_tree_matches(&backup_dir, "journaled prior Pages bundle", &prior)?;
            fs::rename(&backup_dir, final_dir).with_context(|| {
                format!(
                    "failed restoring journaled prior Pages bundle {} to missing live path {}; preserved the backup and journal",
                    backup_dir.display(),
                    final_dir.display()
                )
            })?;
            ensure_bundle_tree_matches(final_dir, "restored prior Pages bundle", &prior)?;
            sync_parent_directory(final_dir).with_context(|| {
                format!(
                    "restored the prior Pages bundle at {}, but could not durably sync the recovery; journal retained at {}",
                    final_dir.display(),
                    bundle_publish_recovery_journal_path(final_dir).display()
                )
            })?;
            remove_bundle_publish_journal(final_dir, &journal)
        }
        (None, Some(_)) => bail!(
            "journaled Pages bundle backup {} does not match the prior generation while live path {} is missing; preserved the backup and journal",
            backup_dir.display(),
            final_dir.display()
        ),
        (Some(live), Some(backup)) if live == candidate && backup == prior => {
            ensure_bundle_tree_matches(
                final_dir,
                "journaled live candidate Pages bundle",
                &candidate,
            )?;
            cleanup_journaled_bundle_tree_after_publish(
                &backup_dir,
                final_dir,
                "journaled prior Pages bundle",
                &prior,
            )?;
            remove_bundle_publish_journal(final_dir, &journal)
        }
        (Some(live), Some(backup)) if live == prior && backup == candidate => {
            ensure_bundle_tree_matches(final_dir, "journaled prior live Pages bundle", &prior)?;
            cleanup_journaled_bundle_tree_after_publish(
                &backup_dir,
                final_dir,
                "journaled rejected candidate Pages bundle",
                &candidate,
            )?;
            remove_bundle_publish_journal(final_dir, &journal)
        }
        (Some(_), Some(_)) => bail!(
            "journaled Pages bundle recovery found live {} and backup {}, but their exact tree evidence does not match a valid publish state; preserved both and the journal",
            final_dir.display(),
            backup_dir.display()
        ),
        (Some(live), None) if live == prior || live == candidate => {
            ensure_bundle_tree_matches(final_dir, "journaled surviving Pages bundle", &live)?;
            remove_bundle_publish_journal(final_dir, &journal)
        }
        (Some(_), None) => bail!(
            "Pages bundle publish journal remains at {}, but live tree {} matches neither journaled generation; preserved both",
            bundle_publish_recovery_journal_path(final_dir).display(),
            final_dir.display()
        ),
        (None, None) => bail!(
            "Pages bundle publish journal remains at {}, but both live {} and journaled backup {} are missing; preserved the journal",
            bundle_publish_recovery_journal_path(final_dir).display(),
            final_dir.display(),
            backup_dir.display()
        ),
    }
}

fn cleanup_journaled_bundle_tree_after_publish(
    tree_dir: &Path,
    final_dir: &Path,
    label: &str,
    expected: &BundleTreeEvidence,
) -> Result<()> {
    cleanup_journaled_bundle_tree_after_publish_with(tree_dir, final_dir, label, expected, |path| {
        fs::remove_dir_all(path)
    })
}

pub(super) fn cleanup_journaled_bundle_tree_after_publish_with<F>(
    tree_dir: &Path,
    final_dir: &Path,
    label: &str,
    expected: &BundleTreeEvidence,
    remove_dir: F,
) -> Result<()>
where
    F: FnOnce(&Path) -> std::io::Result<()>,
{
    ensure_bundle_tree_matches(tree_dir, label, expected)?;
    remove_dir(tree_dir).with_context(|| {
        format!(
            "new bundle is live at {}, but failed to remove {label} containing private artifacts retained at {}",
            final_dir.display(),
            tree_dir.display()
        )
    })?;
    sync_parent_directory(final_dir).with_context(|| {
        format!(
            "removed {label} after publishing {}, but could not durably sync its parent directory",
            final_dir.display()
        )
    })
}

#[cfg(target_os = "linux")]
fn try_publish_linux_bundle_via_atomic_exchange(
    temp_dir: &Path,
    final_dir: &Path,
    backup_dir: &Path,
    journal: &BundlePublishJournal,
    prior: &BundleTreeEvidence,
    candidate: &BundleTreeEvidence,
    retain_temp_on_error: &mut bool,
) -> Result<bool> {
    fs::rename(temp_dir, backup_dir).with_context(|| {
        format!(
            "failed moving staged bundle {} to journaled atomic-exchange path {}",
            temp_dir.display(),
            backup_dir.display()
        )
    })?;
    if let Err(error) =
        ensure_bundle_tree_matches(backup_dir, "journaled atomic-exchange candidate", candidate)
    {
        *retain_temp_on_error = true;
        return Err(error);
    }
    if let Err(sync_error) = sync_parent_directory(backup_dir) {
        return match restore_linux_atomic_staged_candidate(
            backup_dir,
            temp_dir,
            candidate,
            retain_temp_on_error,
        ) {
            Ok(()) => Err(sync_error).with_context(|| {
                format!(
                    "failed durably staging bundle at journaled atomic-exchange path {}; restored candidate at {}",
                    backup_dir.display(),
                    temp_dir.display()
                )
            }),
            Err(restore_error) => Err(anyhow::anyhow!(
                "failed durably staging bundle at journaled atomic-exchange path {}: {sync_error:#}; failed restoring staged candidate to {}: {restore_error:#}",
                backup_dir.display(),
                temp_dir.display()
            )),
        };
    }

    match crate::resource_plan::fs_atomic::atomic_exchange_paths(final_dir, backup_dir) {
        Ok(()) => {
            if let Err(sync_error) = sync_parent_directory(final_dir) {
                *retain_temp_on_error = true;
                bail!(
                    "new bundle is live at {} after atomic exchange, but the parent directory sync failed: {}; prior bundle containing private artifacts retained at {}",
                    final_dir.display(),
                    sync_error,
                    backup_dir.display()
                );
            }
            if let Err(error) =
                ensure_bundle_tree_matches(final_dir, "published Pages bundle candidate", candidate)
            {
                *retain_temp_on_error = true;
                return Err(error);
            }
            if let Err(cleanup_error) = cleanup_journaled_bundle_tree_after_publish(
                backup_dir,
                final_dir,
                "journaled prior Pages bundle",
                prior,
            ) {
                *retain_temp_on_error = true;
                return Err(cleanup_error);
            }
            remove_bundle_publish_journal(final_dir, journal)?;
            Ok(true)
        }
        Err(exchange_error)
            if crate::resource_plan::fs_atomic::linux_atomic_exchange_is_unsupported(&exchange_error) =>
        {
            restore_linux_atomic_staged_candidate(
                backup_dir,
                temp_dir,
                candidate,
                retain_temp_on_error,
            )
            .context(
                "atomic bundle exchange is unsupported and the staged candidate could not be restored for rename-pair publication",
            )?;
            tracing::info!(
                live_bundle_path = %final_dir.display(),
                staged_bundle_path = %temp_dir.display(),
                "renameat2(RENAME_EXCHANGE) is unsupported for the Pages bundle output; using recoverable rename-pair publication"
            );
            Ok(false)
        }
        Err(exchange_error) => {
            match restore_linux_atomic_staged_candidate(
                backup_dir,
                temp_dir,
                candidate,
                retain_temp_on_error,
            ) {
                Ok(()) => Err(exchange_error).with_context(|| {
                    format!(
                        "failed atomically exchanging staged bundle {} with live bundle {}",
                        temp_dir.display(),
                        final_dir.display()
                    )
                }),
                Err(restore_error) => Err(anyhow::anyhow!(
                    "failed atomically exchanging staged bundle {} with live bundle {}: {exchange_error:#}; failed restoring staged candidate from {}: {restore_error:#}",
                    temp_dir.display(),
                    final_dir.display(),
                    backup_dir.display()
                )),
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn restore_linux_atomic_staged_candidate(
    backup_dir: &Path,
    temp_dir: &Path,
    candidate: &BundleTreeEvidence,
    retain_temp_on_error: &mut bool,
) -> Result<()> {
    if let Err(restore_error) = fs::rename(backup_dir, temp_dir) {
        *retain_temp_on_error = true;
        bail!(
            "failed restoring staged bundle from journaled atomic-exchange path {} to {}; staged bundle containing private artifacts retained at {}: {}",
            backup_dir.display(),
            temp_dir.display(),
            backup_dir.display(),
            restore_error
        );
    }
    if let Err(error) = ensure_bundle_tree_matches(
        temp_dir,
        "restored staged Pages bundle candidate",
        candidate,
    ) {
        *retain_temp_on_error = true;
        return Err(error);
    }
    sync_parent_directory(temp_dir).with_context(|| {
        format!(
            "restored staged bundle to {}, but could not durably sync the rename from {}",
            temp_dir.display(),
            backup_dir.display()
        )
    })
}

pub(super) fn replace_dir_from_temp_via_recoverable_rename_pair(
    temp_dir: &Path,
    final_dir: &Path,
    backup_dir: &Path,
    journal: &BundlePublishJournal,
    prior: &BundleTreeEvidence,
    candidate: &BundleTreeEvidence,
    retain_temp_on_error: &mut bool,
) -> Result<()> {
    ensure_bundle_tree_matches(final_dir, "prior live Pages bundle", prior)?;
    ensure_bundle_tree_matches(temp_dir, "staged Pages bundle candidate", candidate)?;
    if bundle_path_entry_exists(backup_dir)? {
        bail!(
            "journaled Pages bundle backup path appeared before rename-pair publication; preserved every generation: {}",
            backup_dir.display()
        );
    }
    fs::rename(final_dir, backup_dir).with_context(|| {
        format!(
            "failed parking live bundle {} at journaled recovery path {}",
            final_dir.display(),
            backup_dir.display()
        )
    })?;
    if let Err(error) =
        ensure_bundle_tree_matches(backup_dir, "journaled prior Pages bundle", prior)
    {
        *retain_temp_on_error = true;
        return Err(error);
    }

    if let Err(park_sync_error) = sync_parent_directory(final_dir) {
        return match fs::rename(backup_dir, final_dir) {
            Ok(()) => {
                ensure_bundle_tree_matches(final_dir, "restored prior Pages bundle", prior)?;
                sync_parent_directory(final_dir).with_context(|| {
                    format!(
                        "failed syncing recovery after restoring prior live bundle at {}",
                        final_dir.display()
                    )
                })?;
                Err(park_sync_error).with_context(|| {
                    format!(
                        "failed durably parking the prior live bundle at {}; restored it at {}",
                        backup_dir.display(),
                        final_dir.display()
                    )
                })
            }
            Err(restore_error) => {
                *retain_temp_on_error = true;
                bail!(
                    "failed durably parking the prior live bundle at {}: {}; restore to {} also failed: {}; prior bundle retained at {} and staged bundle retained at {}",
                    backup_dir.display(),
                    park_sync_error,
                    final_dir.display(),
                    restore_error,
                    backup_dir.display(),
                    temp_dir.display()
                )
            }
        };
    }

    match fs::rename(temp_dir, final_dir) {
        Ok(()) => {
            if let Err(error) =
                ensure_bundle_tree_matches(final_dir, "published Pages bundle candidate", candidate)
            {
                *retain_temp_on_error = true;
                return Err(error);
            }
            if let Err(sync_error) = sync_parent_directory(final_dir) {
                *retain_temp_on_error = true;
                bail!(
                    "new bundle was moved into place at {}, but the publish could not be durably synced: {}; prior bundle containing private artifacts retained at {}",
                    final_dir.display(),
                    sync_error,
                    backup_dir.display()
                );
            }
            if let Err(cleanup_error) = cleanup_journaled_bundle_tree_after_publish(
                backup_dir,
                final_dir,
                "journaled prior Pages bundle",
                prior,
            ) {
                *retain_temp_on_error = true;
                return Err(cleanup_error);
            }
            remove_bundle_publish_journal(final_dir, journal)
        }
        Err(publish_error) => match fs::rename(backup_dir, final_dir) {
            Ok(()) => {
                ensure_bundle_tree_matches(final_dir, "restored prior Pages bundle", prior)?;
                sync_parent_directory(final_dir)?;
                Err(publish_error).with_context(|| {
                    format!(
                        "failed publishing staged bundle {} at {}; restored the prior live bundle",
                        temp_dir.display(),
                        final_dir.display()
                    )
                })
            }
            Err(restore_error) => {
                *retain_temp_on_error = true;
                bail!(
                    "failed publishing staged bundle {} at {}: {}; restore also failed: {}; prior bundle retained at {} and staged bundle retained at {}",
                    temp_dir.display(),
                    final_dir.display(),
                    publish_error,
                    restore_error,
                    backup_dir.display(),
                    temp_dir.display()
                );
            }
        },
    }
}

#[cfg(not(windows))]
pub(super) fn sync_tree(path: &Path) -> Result<()> {
    sync_tree_inner(path)?;
    sync_parent_directory(path)
}

#[cfg(windows)]
pub(super) fn sync_tree(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(not(windows))]
fn sync_tree_inner(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("failed reading metadata for {}", path.display()))?;
    let file_type = metadata.file_type();
    if file_type.is_symlink() {
        return Ok(());
    }
    if file_type.is_file() {
        File::open(path)
            .with_context(|| format!("failed opening {} for sync", path.display()))?
            .sync_all()
            .with_context(|| format!("failed syncing {}", path.display()))?;
        return Ok(());
    }
    if file_type.is_dir() {
        for entry in
            fs::read_dir(path).with_context(|| format!("failed reading {}", path.display()))?
        {
            let entry = entry.with_context(|| format!("failed walking {}", path.display()))?;
            sync_tree_inner(&entry.path())?;
        }
        File::open(path)
            .with_context(|| format!("failed opening directory {} for sync", path.display()))?
            .sync_all()
            .with_context(|| format!("failed syncing directory {}", path.display()))?;
    }
    Ok(())
}

#[cfg(not(windows))]
pub(super) fn sync_parent_directory(path: &Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    File::open(parent)
        .with_context(|| format!("failed opening parent directory {}", parent.display()))?
        .sync_all()
        .with_context(|| format!("failed syncing parent directory {}", parent.display()))
}

/// 70o8f durability contract, Windows: NTFS exposes no supported way to
/// fsync a DIRECTORY handle's entry table from stable Rust (directory
/// handles need FILE_FLAG_BACKUP_SEMANTICS and FlushFileBuffers on them is
/// not documented to durably order sibling renames), so this is an explicit
/// no-op. The crash-recovery model therefore does NOT rely on rename
/// ordering being durable on Windows: every rename-pair phase is journaled
/// first (BundlePublishJournal, written and fsynced as a FILE), and
/// `recover_interrupted_bundle_publish` re-derives the phase from the
/// journal plus on-disk evidence after restart — the strongest contract
/// available without platform-specific unsafe. On Unix the parent directory
/// is fsynced for real (above).
#[cfg(windows)]
pub(super) fn sync_parent_directory(_path: &Path) -> Result<()> {
    Ok(())
}
