#[cfg(any(windows, test))]
use super::publish_replace::sync_parent_directory;
use anyhow::{Context, Result, bail};
use ring::rand::{SecureRandom, SystemRandom};
#[cfg(any(windows, test))]
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::Read;
#[cfg(any(windows, test))]
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

#[cfg(any(windows, test))]
const EXPORT_PUBLISH_JOURNAL_FORMAT: &str = "cass-pages-export-publish-v1";
#[cfg(any(windows, test))]
const EXPORT_PUBLISH_JOURNAL_MAX_BYTES: u64 = 16 * 1024;
#[cfg(any(windows, test))]
const EXPORT_PUBLISH_BACKUP_SCAN_LIMIT: usize = 65_536;

pub(super) struct ExportPublishGuard {
    pub(super) final_path: PathBuf,
    lock_path: PathBuf,
    lock_identity: crate::franken_sync::FileIdentity,
    _lock_file: std::fs::File,
}

impl std::fmt::Debug for ExportPublishGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExportPublishGuard")
            .field("final_path", &self.final_path)
            .field("lock_path", &self.lock_path)
            .finish_non_exhaustive()
    }
}

pub(super) struct ExportFileEvidence {
    pub(super) identity: crate::franken_sync::FileIdentity,
    pub(super) size_bytes: u64,
    pub(super) sha256: String,
}

#[cfg(any(windows, test))]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ExportPublishJournal {
    format: String,
    backup_file_name: String,
    pub(super) prior_size_bytes: u64,
    pub(super) prior_sha256: String,
    pub(super) candidate_size_bytes: u64,
    pub(super) candidate_sha256: String,
}

pub(super) fn export_publish_lock_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .unwrap_or_else(|| std::ffi::OsStr::new("pages_export.db"));
    let mut lock_name = std::ffi::OsString::from(".");
    lock_name.push(file_name);
    lock_name.push(".pages-export-publish.lock");
    path.with_file_name(lock_name)
}

pub(super) fn acquire_export_publish_guard(final_path: &Path) -> Result<ExportPublishGuard> {
    let lock_path = export_publish_lock_path(final_path);
    let lock_exists = match std::fs::symlink_metadata(&lock_path) {
        Ok(metadata) if metadata.file_type().is_file() => {
            #[cfg(unix)]
            if metadata.nlink() != 1 {
                bail!(
                    "Pages export publish lock {} has {} hard links; refused to open it because exclusive pathname ownership is not provable",
                    lock_path.display(),
                    metadata.nlink()
                );
            }
            true
        }
        Ok(_) => {
            bail!(
                "Pages export publish lock is not a regular file; refused to open it: {}",
                lock_path.display()
            )
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "failed inspecting Pages export publish lock {} before open",
                    lock_path.display()
                )
            });
        }
    };
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true);
    if lock_exists {
        options.create(false);
    } else {
        options.create_new(true);
    }
    #[cfg(unix)]
    options.mode(0o600);
    let lock_file = options.open(&lock_path).with_context(|| {
        format!(
            "failed opening Pages export publish lock {}",
            lock_path.display()
        )
    })?;

    let lock_identity = crate::franken_sync::FileIdentity::from_file(&lock_file)
        .with_context(|| {
            format!(
                "failed identifying opened Pages export publish lock {}",
                lock_path.display()
            )
        })?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "filesystem does not expose a stable identity for Pages export publish lock {}",
                lock_path.display()
            )
        })?;
    // std's try_lock reports contention as WouldBlock on every platform; fs2
    // surfaced Windows contention as raw ERROR_LOCK_VIOLATION (2l1b0.74).
    match lock_file.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => {
            bail!(
                "another Pages export is already publishing to {}; lock contention at {}",
                final_path.display(),
                lock_path.display()
            );
        }
        Err(std::fs::TryLockError::Error(error)) => {
            return Err(error).with_context(|| {
                format!(
                    "failed acquiring Pages export publish lock {} for {}",
                    lock_path.display(),
                    final_path.display()
                )
            });
        }
    }

    let path_metadata = std::fs::symlink_metadata(&lock_path).with_context(|| {
        format!(
            "failed re-inspecting Pages export publish lock {} after acquisition",
            lock_path.display()
        )
    })?;
    if !path_metadata.file_type().is_file() {
        bail!(
            "Pages export publish lock {} changed to a non-regular entry during acquisition",
            lock_path.display()
        );
    }
    #[cfg(unix)]
    if path_metadata.nlink() != 1 {
        bail!(
            "Pages export publish lock {} has {} hard links after acquisition; exclusive pathname ownership is not provable",
            lock_path.display(),
            path_metadata.nlink()
        );
    }
    let path_probe = std::fs::File::open(&lock_path).with_context(|| {
        format!(
            "failed re-opening Pages export publish lock {} after acquisition",
            lock_path.display()
        )
    })?;
    let path_identity = crate::franken_sync::FileIdentity::from_file(&path_probe)
        .with_context(|| {
            format!(
                "failed re-identifying Pages export publish lock {} after acquisition",
                lock_path.display()
            )
        })?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "filesystem does not expose a stable pathname identity for Pages export publish lock {}",
                lock_path.display()
            )
        })?;
    if path_identity != lock_identity {
        bail!(
            "Pages export publish lock {} changed identity during acquisition",
            lock_path.display()
        );
    }

    Ok(ExportPublishGuard {
        final_path: final_path.to_path_buf(),
        lock_path,
        lock_identity,
        _lock_file: lock_file,
    })
}

pub(super) fn require_export_publish_guard(
    final_path: &Path,
    publish_guard: &ExportPublishGuard,
) -> Result<()> {
    if publish_guard.final_path != final_path {
        bail!(
            "Pages export publish guard for {} cannot authorize publication to {}",
            publish_guard.final_path.display(),
            final_path.display()
        );
    }
    let held_identity = crate::franken_sync::FileIdentity::from_file(&publish_guard._lock_file)
        .with_context(|| {
            format!(
                "failed re-identifying held Pages export publish lock {}",
                publish_guard.lock_path.display()
            )
        })?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "filesystem stopped exposing a stable identity for held Pages export publish lock {}",
                publish_guard.lock_path.display()
            )
        })?;
    if held_identity != publish_guard.lock_identity {
        bail!(
            "held Pages export publish lock {} changed identity",
            publish_guard.lock_path.display()
        );
    }
    let path_metadata = std::fs::symlink_metadata(&publish_guard.lock_path).with_context(|| {
        format!(
            "failed re-inspecting Pages export publish lock {} before authorization",
            publish_guard.lock_path.display()
        )
    })?;
    if !path_metadata.file_type().is_file() {
        bail!(
            "Pages export publish lock {} is no longer a regular file; refused namespace mutation",
            publish_guard.lock_path.display()
        );
    }
    #[cfg(unix)]
    if path_metadata.nlink() != 1 {
        bail!(
            "Pages export publish lock {} has {} hard links before authorization; refused namespace mutation",
            publish_guard.lock_path.display(),
            path_metadata.nlink()
        );
    }
    let path_probe = std::fs::File::open(&publish_guard.lock_path).with_context(|| {
        format!(
            "failed re-opening Pages export publish lock {} before authorization",
            publish_guard.lock_path.display()
        )
    })?;
    let path_identity = crate::franken_sync::FileIdentity::from_file(&path_probe)
        .with_context(|| {
            format!(
                "failed re-identifying Pages export publish lock {} before authorization",
                publish_guard.lock_path.display()
            )
        })?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "filesystem stopped exposing a stable pathname identity for Pages export publish lock {}",
                publish_guard.lock_path.display()
            )
        })?;
    if path_identity != publish_guard.lock_identity {
        bail!(
            "Pages export publish lock {} was replaced after acquisition; refused namespace mutation",
            publish_guard.lock_path.display()
        );
    }
    Ok(())
}

pub(super) fn inspect_export_regular_file(path: &Path, label: &str) -> Result<ExportFileEvidence> {
    let path_metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("failed inspecting {label} {}", path.display()))?;
    if !path_metadata.file_type().is_file() {
        bail!("{label} is not a regular file: {}", path.display());
    }

    let mut file = std::fs::File::open(path)
        .with_context(|| format!("failed opening {label} {}", path.display()))?;
    let identity = crate::franken_sync::FileIdentity::from_file(&file)
        .with_context(|| format!("failed identifying {label} {}", path.display()))?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "filesystem does not expose a stable identity for {label} {}",
                path.display()
            )
        })?;
    let metadata = file
        .metadata()
        .with_context(|| format!("failed inspecting opened {label} {}", path.display()))?;
    if !metadata.file_type().is_file() {
        bail!("opened {label} is not a regular file: {}", path.display());
    }

    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("failed hashing {label} {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }

    let hashed_metadata = file
        .metadata()
        .with_context(|| format!("failed re-inspecting opened {label} {}", path.display()))?;
    if hashed_metadata.len() != metadata.len() {
        bail!("{label} changed size while inspected: {}", path.display());
    }

    let final_path_metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("failed re-inspecting {label} {}", path.display()))?;
    if !final_path_metadata.file_type().is_file() {
        bail!(
            "{label} changed file type while inspected: {}",
            path.display()
        );
    }
    let path_probe = std::fs::File::open(path)
        .with_context(|| format!("failed re-opening {label} {}", path.display()))?;
    let path_identity = crate::franken_sync::FileIdentity::from_file(&path_probe)
        .with_context(|| format!("failed re-identifying {label} {}", path.display()))?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "filesystem stopped exposing a stable identity for {label} {}",
                path.display()
            )
        })?;
    if path_identity != identity {
        bail!(
            "{label} changed identity while inspected: {}",
            path.display()
        );
    }

    Ok(ExportFileEvidence {
        identity,
        size_bytes: metadata.len(),
        sha256: hex::encode(hasher.finalize()),
    })
}

pub(super) fn evidence_matches(
    actual: &ExportFileEvidence,
    expected_size_bytes: u64,
    expected_sha256: &str,
) -> bool {
    actual.size_bytes == expected_size_bytes && actual.sha256 == expected_sha256
}

pub(super) fn sync_export_regular_file(
    path: &Path,
    expected: &ExportFileEvidence,
    label: &str,
) -> Result<()> {
    let before_sync = inspect_export_regular_file(path, label)?;
    if before_sync.identity != expected.identity
        || !evidence_matches(&before_sync, expected.size_bytes, &expected.sha256)
    {
        bail!(
            "{label} changed identity or content before sync: {}",
            path.display()
        );
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .with_context(|| format!("failed opening {label} {} for sync", path.display()))?;
    let identity = crate::franken_sync::FileIdentity::from_file(&file)
        .with_context(|| format!("failed identifying {label} {} before sync", path.display()))?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "filesystem does not expose a stable identity for {label} {} before sync",
                path.display()
            )
        })?;
    if identity != expected.identity {
        bail!("{label} changed identity before sync: {}", path.display());
    }
    file.sync_all()
        .with_context(|| format!("failed syncing {label} {}", path.display()))?;
    let path_probe = std::fs::File::open(path)
        .with_context(|| format!("failed re-opening {label} {} after sync", path.display()))?;
    let path_identity = crate::franken_sync::FileIdentity::from_file(&path_probe)
        .with_context(|| {
            format!(
                "failed re-identifying {label} {} after sync",
                path.display()
            )
        })?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "filesystem stopped exposing a stable identity for {label} {} after sync",
                path.display()
            )
        })?;
    if path_identity != expected.identity {
        bail!("{label} changed identity during sync: {}", path.display());
    }
    let after_sync = inspect_export_regular_file(path, label)?;
    if after_sync.identity != expected.identity
        || !evidence_matches(&after_sync, expected.size_bytes, &expected.sha256)
    {
        bail!(
            "{label} changed identity or content during sync: {}",
            path.display()
        );
    }
    Ok(())
}

#[cfg(any(windows, test))]
pub(super) fn export_publish_recovery_journal_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .unwrap_or_else(|| std::ffi::OsStr::new("pages_export.db"));
    let mut journal_name = std::ffi::OsString::from(".");
    journal_name.push(file_name);
    journal_name.push(".pages-export-publish-in-progress.json");
    path.with_file_name(journal_name)
}

#[cfg(any(windows, test))]
pub(super) fn legacy_export_publish_recovery_backup_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .unwrap_or_else(|| std::ffi::OsStr::new("pages_export.db"));
    let mut backup_name = std::ffi::OsString::from(".");
    backup_name.push(file_name);
    backup_name.push(".pages-export-publish-in-progress.bak");
    path.with_file_name(backup_name)
}

pub(super) fn unpredictable_atomic_sidecar_path(
    path: &Path,
    suffix: &str,
    fallback_name: &str,
) -> Result<PathBuf> {
    let mut nonce = [0_u8; 16];
    SystemRandom::new()
        .fill(&mut nonce)
        .map_err(|_| anyhow::anyhow!("failed to obtain randomness for Pages export staging"))?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(fallback_name);
    Ok(path.with_file_name(format!(".{file_name}.{suffix}.{}", hex::encode(nonce))))
}

#[cfg(any(windows, test))]
pub(super) fn unpredictable_export_publish_backup_path(path: &Path) -> Result<PathBuf> {
    unpredictable_atomic_sidecar_path(path, "publish-backup", "pages_export.db")
}

#[cfg(any(windows, test))]
fn export_publish_backup_prefix(path: &Path) -> String {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("pages_export.db");
    format!(".{file_name}.publish-backup.")
}

#[cfg(any(windows, test))]
pub(super) fn journal_backup_path(
    final_path: &Path,
    journal: &ExportPublishJournal,
) -> Result<PathBuf> {
    let backup_name_path = Path::new(&journal.backup_file_name);
    if backup_name_path.file_name() != Some(backup_name_path.as_os_str())
        || backup_name_path.components().count() != 1
    {
        bail!(
            "Pages export publish journal for {} contains a non-local backup name",
            final_path.display()
        );
    }
    let prefix = export_publish_backup_prefix(final_path);
    let Some(nonce) = journal.backup_file_name.strip_prefix(&prefix) else {
        bail!(
            "Pages export publish journal for {} contains an unexpected backup name {}",
            final_path.display(),
            journal.backup_file_name
        );
    };
    if nonce.len() != 32 || !nonce.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!(
            "Pages export publish journal for {} contains an invalid backup nonce",
            final_path.display()
        );
    }
    Ok(final_path.with_file_name(&journal.backup_file_name))
}

#[cfg(any(windows, test))]
pub(super) fn validate_export_publish_journal(
    final_path: &Path,
    journal: &ExportPublishJournal,
) -> Result<PathBuf> {
    if journal.format != EXPORT_PUBLISH_JOURNAL_FORMAT {
        bail!(
            "Pages export publish journal for {} has unsupported format {}",
            final_path.display(),
            journal.format
        );
    }
    for (label, digest) in [
        ("prior", journal.prior_sha256.as_str()),
        ("candidate", journal.candidate_sha256.as_str()),
    ] {
        if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            bail!(
                "Pages export publish journal for {} has an invalid {label} SHA-256 digest",
                final_path.display()
            );
        }
    }
    journal_backup_path(final_path, journal)
}

#[cfg(any(windows, test))]
pub(super) fn read_export_publish_journal(
    final_path: &Path,
) -> Result<Option<ExportPublishJournal>> {
    let journal_path = export_publish_recovery_journal_path(final_path);
    let metadata = match std::fs::symlink_metadata(&journal_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "failed inspecting Pages export publish journal {}",
                    journal_path.display()
                )
            });
        }
    };
    if !metadata.file_type().is_file() {
        bail!(
            "Pages export publish journal is not a regular file; preserved it without mutation: {}",
            journal_path.display()
        );
    }
    #[cfg(unix)]
    if metadata.nlink() != 1 {
        bail!(
            "Pages export publish journal {} has {} hard links; preserved it because exclusive pathname ownership is not provable",
            journal_path.display(),
            metadata.nlink()
        );
    }

    let file = std::fs::File::open(&journal_path).with_context(|| {
        format!(
            "failed opening Pages export publish journal {}",
            journal_path.display()
        )
    })?;
    let opened_identity = crate::franken_sync::FileIdentity::from_file(&file)
        .with_context(|| {
            format!(
                "failed identifying opened Pages export publish journal {}",
                journal_path.display()
            )
        })?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "filesystem does not expose a stable identity for Pages export publish journal {}",
                journal_path.display()
            )
        })?;
    let opened_metadata = file.metadata().with_context(|| {
        format!(
            "failed inspecting opened Pages export publish journal {}",
            journal_path.display()
        )
    })?;
    if !opened_metadata.file_type().is_file() {
        bail!(
            "opened Pages export publish journal is not a regular file; preserved it without mutation: {}",
            journal_path.display()
        );
    }
    if opened_metadata.len() > EXPORT_PUBLISH_JOURNAL_MAX_BYTES {
        bail!(
            "Pages export publish journal exceeds the {}-byte bound; preserved it without mutation: {}",
            EXPORT_PUBLISH_JOURNAL_MAX_BYTES,
            journal_path.display()
        );
    }

    let mut bytes = Vec::new();
    file.take(EXPORT_PUBLISH_JOURNAL_MAX_BYTES + 1)
        .read_to_end(&mut bytes)
        .with_context(|| {
            format!(
                "failed reading Pages export publish journal {}",
                journal_path.display()
            )
        })?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > EXPORT_PUBLISH_JOURNAL_MAX_BYTES {
        bail!(
            "Pages export publish journal exceeds the {}-byte bound; preserved it without mutation: {}",
            EXPORT_PUBLISH_JOURNAL_MAX_BYTES,
            journal_path.display()
        );
    }
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != opened_metadata.len() {
        bail!(
            "Pages export publish journal changed size while read; preserved it without mutation: {}",
            journal_path.display()
        );
    }
    let final_metadata = std::fs::symlink_metadata(&journal_path).with_context(|| {
        format!(
            "failed re-inspecting Pages export publish journal {}",
            journal_path.display()
        )
    })?;
    if !final_metadata.file_type().is_file() {
        bail!(
            "Pages export publish journal changed file type while read; preserved the current entry without mutation: {}",
            journal_path.display()
        );
    }
    #[cfg(unix)]
    if final_metadata.nlink() != 1 {
        bail!(
            "Pages export publish journal {} gained additional hard links while read; preserved it without mutation",
            journal_path.display()
        );
    }
    let path_probe = std::fs::File::open(&journal_path).with_context(|| {
        format!(
            "failed re-opening Pages export publish journal {}",
            journal_path.display()
        )
    })?;
    let path_identity = crate::franken_sync::FileIdentity::from_file(&path_probe)
        .with_context(|| {
            format!(
                "failed re-identifying Pages export publish journal {}",
                journal_path.display()
            )
        })?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "filesystem stopped exposing a stable identity for Pages export publish journal {}",
                journal_path.display()
            )
        })?;
    if path_identity != opened_identity {
        bail!(
            "Pages export publish journal changed identity while read; preserved the current entry without mutation: {}",
            journal_path.display()
        );
    }
    let journal: ExportPublishJournal = serde_json::from_slice(&bytes).with_context(|| {
        format!(
            "Pages export publish journal is invalid and was preserved without mutation: {}",
            journal_path.display()
        )
    })?;
    validate_export_publish_journal(final_path, &journal)?;
    Ok(Some(journal))
}

#[cfg(any(windows, test))]
pub(super) fn write_export_publish_journal(
    final_path: &Path,
    backup_path: &Path,
    prior: &ExportFileEvidence,
    candidate: &ExportFileEvidence,
    publish_guard: &ExportPublishGuard,
) -> Result<ExportPublishJournal> {
    require_export_publish_guard(final_path, publish_guard)?;
    let backup_file_name = backup_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Pages export publish backup has no UTF-8 file name: {}",
                backup_path.display()
            )
        })?
        .to_string();
    let journal = ExportPublishJournal {
        format: EXPORT_PUBLISH_JOURNAL_FORMAT.to_string(),
        backup_file_name,
        prior_size_bytes: prior.size_bytes,
        prior_sha256: prior.sha256.clone(),
        candidate_size_bytes: candidate.size_bytes,
        candidate_sha256: candidate.sha256.clone(),
    };
    validate_export_publish_journal(final_path, &journal)?;
    let bytes =
        serde_json::to_vec(&journal).context("failed serializing Pages export publish journal")?;
    let journal_path = export_publish_recovery_journal_path(final_path);
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(&journal_path).with_context(|| {
        format!(
            "failed exclusively creating Pages export publish journal {}; preserved the live and staged generations",
            journal_path.display()
        )
    })?;
    let created_identity = crate::franken_sync::FileIdentity::from_file(&file)
        .with_context(|| {
            format!(
                "failed identifying newly created Pages export publish journal {}",
                journal_path.display()
            )
        })?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "filesystem does not expose a stable identity for newly created Pages export publish journal {}",
                journal_path.display()
            )
        })?;
    file.write_all(&bytes).with_context(|| {
        format!(
            "failed writing Pages export publish journal {}; preserved it for inspection",
            journal_path.display()
        )
    })?;
    file.sync_all().with_context(|| {
        format!(
            "failed syncing Pages export publish journal {}; preserved it for inspection",
            journal_path.display()
        )
    })?;
    sync_parent_directory(&journal_path).with_context(|| {
        format!(
            "failed durably publishing Pages export journal {}; preserved it for recovery",
            journal_path.display()
        )
    })?;
    let path_probe = std::fs::File::open(&journal_path).with_context(|| {
        format!(
            "failed re-opening newly created Pages export publish journal {}",
            journal_path.display()
        )
    })?;
    let path_identity = crate::franken_sync::FileIdentity::from_file(&path_probe)
        .with_context(|| {
            format!(
                "failed re-identifying newly created Pages export publish journal {}",
                journal_path.display()
            )
        })?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "filesystem stopped exposing a stable identity for newly created Pages export publish journal {}",
                journal_path.display()
            )
        })?;
    if path_identity != created_identity {
        bail!(
            "Pages export publish journal {} changed identity while it was created; live and staged generations were preserved",
            journal_path.display()
        );
    }
    let observed_journal = read_export_publish_journal(final_path)?.ok_or_else(|| {
        anyhow::anyhow!(
            "newly created Pages export publish journal disappeared before publication: {}",
            journal_path.display()
        )
    })?;
    if observed_journal != journal {
        bail!(
            "Pages export publish journal {} changed content before publication; live and staged generations were preserved",
            journal_path.display()
        );
    }
    require_export_publish_guard(final_path, publish_guard)?;
    Ok(journal)
}

#[cfg(all(not(windows), test))]
pub(super) fn remove_export_publish_journal(
    final_path: &Path,
    expected_journal: &ExportPublishJournal,
    publish_guard: &ExportPublishGuard,
) -> Result<()> {
    require_export_publish_guard(final_path, publish_guard)?;
    let journal_path = export_publish_recovery_journal_path(final_path);
    let current_journal = read_export_publish_journal(final_path)?.ok_or_else(|| {
        anyhow::anyhow!(
            "completed Pages export publish journal disappeared before validated cleanup: {}",
            journal_path.display()
        )
    })?;
    if current_journal != *expected_journal {
        bail!(
            "Pages export publish journal changed before cleanup; preserved the current entry without mutation: {}",
            journal_path.display()
        );
    }
    require_export_publish_guard(final_path, publish_guard)?;
    match std::fs::remove_file(&journal_path) {
        Ok(()) => sync_parent_directory(&journal_path).with_context(|| {
            format!(
                "removed completed Pages export publish journal {}, but could not durably sync its parent directory",
                journal_path.display()
            )
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| {
            format!(
                "failed removing completed Pages export publish journal {}",
                journal_path.display()
            )
        }),
    }
}

#[cfg(any(windows, test))]
pub(super) fn first_unmarked_export_publish_backup(
    final_path: &Path,
    owned_backup_path: Option<&Path>,
) -> Result<Option<PathBuf>> {
    let legacy_path = legacy_export_publish_recovery_backup_path(final_path);
    match std::fs::symlink_metadata(&legacy_path) {
        Ok(_) => return Ok(Some(legacy_path)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "failed inspecting legacy Pages export recovery path {}",
                    legacy_path.display()
                )
            });
        }
    }

    let parent = final_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let prefix = export_publish_backup_prefix(final_path);
    let entries = std::fs::read_dir(parent).with_context(|| {
        format!(
            "failed scanning Pages export directory {} for unmarked recovery backups",
            parent.display()
        )
    })?;
    for (index, entry) in entries.enumerate() {
        if index >= EXPORT_PUBLISH_BACKUP_SCAN_LIMIT {
            bail!(
                "Pages export directory {} exceeds the {}-entry recovery scan bound",
                parent.display(),
                EXPORT_PUBLISH_BACKUP_SCAN_LIMIT
            );
        }
        let entry = entry.with_context(|| {
            format!(
                "failed reading Pages export directory entry in {}",
                parent.display()
            )
        })?;
        let entry_path = entry.path();
        if entry.file_name().to_string_lossy().starts_with(&prefix)
            && owned_backup_path != Some(entry_path.as_path())
        {
            return Ok(Some(entry_path));
        }
    }
    Ok(None)
}
