use anyhow::{Context, Result, bail};
use chrono::Utc;
use std::fs::{self, OpenOptions};
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::Path;

use super::publish::sync_parent_directory;
use super::publish_journal::{
    ensure_existing_parent_ancestors_are_real_dirs, unique_bundle_sidecar_path,
};

/// Write private artifacts that should never be deployed
pub fn write_private_fingerprint(private_dir: &Path, fingerprint: &str) -> Result<()> {
    let fingerprint_content = format!(
        "Integrity Fingerprint: {}\n\n\
        Generated: {}\n\n\
        Verify this fingerprint matches the one displayed in the web viewer\n\
        before proceeding. If it doesn't match, the archive may have been\n\
        tampered with.\n",
        fingerprint,
        Utc::now().to_rfc3339()
    );
    write_private_artifact_file(
        private_dir,
        "integrity-fingerprint.txt",
        fingerprint_content.as_bytes(),
    )?;
    Ok(())
}

pub(super) fn ensure_private_artifact_dir(private_dir: &Path) -> Result<()> {
    ensure_existing_parent_ancestors_are_real_dirs(private_dir, "private artifact directory")?;

    match fs::symlink_metadata(private_dir) {
        Ok(metadata) => {
            let file_type = metadata.file_type();
            if file_type.is_symlink() {
                bail!(
                    "private artifact directory must not be a symlink: {}",
                    private_dir.display()
                );
            }
            if !file_type.is_dir() {
                bail!(
                    "private artifact path must be a directory: {}",
                    private_dir.display()
                );
            }
            secure_private_artifact_dir(private_dir)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            create_private_artifact_dir(private_dir)?;
            ensure_private_artifact_dir(private_dir)
        }
        Err(err) => Err(err).with_context(|| {
            format!(
                "Failed to inspect private artifact directory {}",
                private_dir.display()
            )
        }),
    }
}

fn create_private_artifact_dir(private_dir: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let mut builder = fs::DirBuilder::new();
        builder.recursive(true).mode(0o700);
        builder.create(private_dir).with_context(|| {
            format!(
                "Failed to create owner-only private artifact directory {}",
                private_dir.display()
            )
        })?;
    }
    #[cfg(not(unix))]
    fs::create_dir_all(private_dir).with_context(|| {
        format!(
            "Failed to create private artifact directory {}",
            private_dir.display()
        )
    })?;
    Ok(())
}

fn secure_private_artifact_dir(private_dir: &Path) -> Result<()> {
    #[cfg(unix)]
    fs::set_permissions(private_dir, fs::Permissions::from_mode(0o700)).with_context(|| {
        format!(
            "Failed to set owner-only permissions on private artifact directory {}",
            private_dir.display()
        )
    })?;
    #[cfg(not(unix))]
    let _ = private_dir;
    Ok(())
}

fn reject_symlinked_private_artifact(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            let file_type = metadata.file_type();
            if file_type.is_symlink() {
                bail!(
                    "private artifact file must not be a symlink: {}",
                    path.display()
                );
            }
            if file_type.is_dir() {
                bail!(
                    "private artifact path must be a regular file, not a directory: {}",
                    path.display()
                );
            }
            Ok(())
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err)
            .with_context(|| format!("Failed to inspect private artifact {}", path.display())),
    }
}

pub(super) fn write_private_artifact_file(
    private_dir: &Path,
    filename: &str,
    contents: &[u8],
) -> Result<()> {
    if filename.contains(['/', '\\']) {
        bail!("private artifact filename must not contain path separators: {filename}");
    }
    let mut components = Path::new(filename).components();
    let Some(std::path::Component::Normal(validated_name)) = components.next() else {
        bail!("private artifact filename must be a plain relative file name: {filename}");
    };
    if components.next().is_some() {
        bail!("private artifact filename must be a single path segment: {filename}");
    }

    ensure_private_artifact_dir(private_dir)?;
    let final_path = private_dir.join(validated_name); // ubs:ignore[rust.security.path-traversal] — single Component::Normal filename validated above
    reject_symlinked_private_artifact(&final_path)?;
    let temp_path = unique_bundle_sidecar_path(&final_path, "tmp", "private_artifact")?;

    let write_result = (|| -> Result<()> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options.open(&temp_path).with_context(|| {
            format!(
                "Failed to create temporary private artifact {}",
                temp_path.display()
            )
        })?;
        file.write_all(contents).with_context(|| {
            format!(
                "Failed to write temporary private artifact {}",
                temp_path.display()
            )
        })?;
        file.sync_all().with_context(|| {
            format!(
                "Failed to sync temporary private artifact {}",
                temp_path.display()
            )
        })?;
        Ok(())
    })();

    if let Err(err) = write_result {
        let _ = fs::remove_file(&temp_path);
        return Err(err);
    }

    let mut retain_temp_on_replace_error = false;
    if let Err(err) = replace_private_artifact_from_temp(
        &temp_path,
        &final_path,
        &mut retain_temp_on_replace_error,
    ) {
        if !retain_temp_on_replace_error {
            let _ = fs::remove_file(&temp_path);
        }
        return Err(err);
    }
    Ok(())
}

#[cfg(windows)]
fn private_artifact_path_entry_exists(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err)
            .with_context(|| format!("Failed to inspect private artifact {}", path.display())),
    }
}

#[cfg(any(windows, test))]
pub(super) fn replace_private_artifact_from_temp_via_backup(
    temp_path: &Path,
    final_path: &Path,
    first_error: &std::io::Error,
    retain_temp_on_error: &mut bool,
) -> Result<()> {
    *retain_temp_on_error = false;
    reject_symlinked_private_artifact(final_path)?;
    let backup_path = unique_bundle_sidecar_path(final_path, "bak", "private_artifact")?;
    fs::rename(final_path, &backup_path).with_context(|| {
        format!(
            "Failed to preserve existing private artifact {} at {} after initial replacement error: {}",
            final_path.display(),
            backup_path.display(),
            first_error
        )
    })?;

    match fs::rename(temp_path, final_path) {
        Ok(()) => {
            fs::remove_file(&backup_path).with_context(|| {
                format!(
                    "Installed private artifact {} but failed to remove prior generation {}",
                    final_path.display(),
                    backup_path.display()
                )
            })?;
            sync_parent_directory(final_path)
        }
        Err(replace_error) => match fs::rename(&backup_path, final_path) {
            Ok(()) => {
                sync_parent_directory(final_path)?;
                Err(replace_error).with_context(|| {
                    format!(
                        "Failed to replace private artifact {} after initial error {}; restored its prior generation",
                        final_path.display(),
                        first_error
                    )
                })
            }
            Err(restore_error) => {
                *retain_temp_on_error = true;
                bail!(
                    "Failed to replace private artifact {} after initial error {}; replacement error: {}; restore error: {}; prior generation retained at {} and new generation retained at {}",
                    final_path.display(),
                    first_error,
                    replace_error,
                    restore_error,
                    backup_path.display(),
                    temp_path.display()
                )
            }
        },
    }
}

fn replace_private_artifact_from_temp(
    temp_path: &Path,
    final_path: &Path,
    retain_temp_on_error: &mut bool,
) -> Result<()> {
    *retain_temp_on_error = false;
    #[cfg(windows)]
    {
        match fs::rename(temp_path, final_path) {
            Ok(()) => sync_parent_directory(final_path),
            Err(first_error)
                if matches!(
                    first_error.kind(),
                    std::io::ErrorKind::AlreadyExists | std::io::ErrorKind::PermissionDenied
                ) && private_artifact_path_entry_exists(final_path)? =>
            {
                replace_private_artifact_from_temp_via_backup(
                    temp_path,
                    final_path,
                    &first_error,
                    retain_temp_on_error,
                )
            }
            Err(error) => Err(error).with_context(|| {
                format!(
                    "Failed to install private artifact {} from {}",
                    final_path.display(),
                    temp_path.display()
                )
            }),
        }
    }

    #[cfg(not(windows))]
    {
        fs::rename(temp_path, final_path).with_context(|| {
            format!(
                "Failed to install private artifact {} from {}",
                final_path.display(),
                temp_path.display()
            )
        })?;
        sync_parent_directory(final_path)
    }
}

pub(super) fn write_private_unencrypted_notice(private_dir: &Path) -> Result<()> {
    let content = format!(
        "UNENCRYPTED ARCHIVE WARNING\n\
        ============================\n\n\
        This bundle was generated WITHOUT encryption.\n\
        Anyone with access to the site can read its contents.\n\n\
        Generated: {}\n",
        Utc::now().to_rfc3339()
    );
    write_private_artifact_file(private_dir, "unencrypted-warning.txt", content.as_bytes())?;
    Ok(())
}
