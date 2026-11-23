use anyhow::{Context, Result, anyhow, bail};
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

use super::publish::sync_parent_directory;

const BUNDLE_PUBLISH_JOURNAL_FORMAT: &str = "cass-pages-bundle-publish-v1";
const BUNDLE_PUBLISH_JOURNAL_MAX_BYTES: u64 = 16 * 1024;
const BUNDLE_PUBLISH_BACKUP_SCAN_LIMIT: usize = 65_536;
const BUNDLE_TREE_EVIDENCE_ENTRY_LIMIT: usize = 1_000_000;
const BUNDLE_TREE_EVIDENCE_DOMAIN: &[u8] = b"cass-pages-bundle-tree-v1\0";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct BundleTreeEvidence {
    file_count: u64,
    total_size_bytes: u64,
    sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct BundlePublishJournal {
    format: String,
    backup_file_name: String,
    prior_file_count: u64,
    prior_size_bytes: u64,
    prior_sha256: String,
    candidate_file_count: u64,
    candidate_size_bytes: u64,
    candidate_sha256: String,
}

#[derive(Debug)]
pub(super) struct BundlePublishGuard {
    final_dir: PathBuf,
    lock_path: PathBuf,
    lock_identity: crate::franken_sync::FileIdentity,
    lock_file: File,
}

pub(super) fn legacy_bundle_publish_in_progress_backup_path(path: &Path) -> PathBuf {
    let mut backup_name = std::ffi::OsString::from(".");
    backup_name.push(
        path.file_name()
            .unwrap_or_else(|| std::ffi::OsStr::new("pages_bundle")),
    );
    backup_name.push(".publish-in-progress.bak");
    path.with_file_name(backup_name)
}

pub(super) fn bundle_publish_recovery_journal_path(path: &Path) -> PathBuf {
    let mut journal_name = std::ffi::OsString::from(".");
    journal_name.push(
        path.file_name()
            .unwrap_or_else(|| std::ffi::OsStr::new("pages_bundle")),
    );
    journal_name.push(".pages-bundle-publish-in-progress.json");
    path.with_file_name(journal_name)
}

pub(super) fn bundle_publish_lock_path(path: &Path) -> PathBuf {
    let mut lock_name = std::ffi::OsString::from(".");
    lock_name.push(
        path.file_name()
            .unwrap_or_else(|| std::ffi::OsStr::new("pages_bundle")),
    );
    lock_name.push(".pages-bundle-publish.lock");
    path.with_file_name(lock_name)
}

pub(super) fn acquire_bundle_publish_guard(final_dir: &Path) -> Result<BundlePublishGuard> {
    ensure_existing_parent_ancestors_are_real_dirs(final_dir, "bundle publish lock")?;
    if let Some(parent) = final_dir
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed creating Pages bundle publish-lock parent {}",
                parent.display()
            )
        })?;
    }
    let lock_path = bundle_publish_lock_path(final_dir);
    let lock_exists = match fs::symlink_metadata(&lock_path) {
        Ok(metadata) if metadata.file_type().is_file() => {
            #[cfg(unix)]
            if metadata.nlink() != 1 {
                bail!(
                    "Pages bundle publish lock {} has {} hard links; exclusive pathname ownership is not provable",
                    lock_path.display(),
                    metadata.nlink()
                );
            }
            true
        }
        Ok(_) => bail!(
            "Pages bundle publish lock is not a regular file: {}",
            lock_path.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "failed inspecting Pages bundle publish lock {}",
                    lock_path.display()
                )
            });
        }
    };
    let mut options = OpenOptions::new();
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
            "failed opening Pages bundle publish lock {}",
            lock_path.display()
        )
    })?;
    let lock_identity = crate::franken_sync::FileIdentity::from_file(&lock_file)
        .with_context(|| {
            format!(
                "failed identifying Pages bundle publish lock {}",
                lock_path.display()
            )
        })?
        .ok_or_else(|| {
            anyhow!(
                "filesystem does not expose a stable identity for Pages bundle publish lock {}",
                lock_path.display()
            )
        })?;
    // std's try_lock reports contention as WouldBlock on every platform; fs2
    // surfaced Windows contention as raw ERROR_LOCK_VIOLATION (2l1b0.74).
    match lock_file.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => bail!(
            "another Pages bundle publication is already active for {}; lock contention at {}",
            final_dir.display(),
            lock_path.display()
        ),
        Err(std::fs::TryLockError::Error(error)) => {
            return Err(error).with_context(|| {
                format!(
                    "failed acquiring Pages bundle publish lock {}",
                    lock_path.display()
                )
            });
        }
    }
    let guard = BundlePublishGuard {
        final_dir: final_dir.to_path_buf(),
        lock_path,
        lock_identity,
        lock_file,
    };
    require_bundle_publish_guard(final_dir, &guard)?;
    Ok(guard)
}

pub(super) fn require_bundle_publish_guard(
    final_dir: &Path,
    guard: &BundlePublishGuard,
) -> Result<()> {
    if guard.final_dir != final_dir {
        bail!(
            "Pages bundle publish guard for {} cannot authorize mutation of {}",
            guard.final_dir.display(),
            final_dir.display()
        );
    }
    let held_identity = crate::franken_sync::FileIdentity::from_file(&guard.lock_file)
        .context("failed re-identifying held Pages bundle publish lock")?
        .ok_or_else(|| {
            anyhow!(
                "filesystem stopped exposing a stable identity for held Pages bundle publish lock {}",
                guard.lock_path.display()
            )
        })?;
    if held_identity != guard.lock_identity {
        bail!(
            "held Pages bundle publish lock {} changed identity",
            guard.lock_path.display()
        );
    }
    let metadata = fs::symlink_metadata(&guard.lock_path).with_context(|| {
        format!(
            "failed re-inspecting Pages bundle publish lock {}",
            guard.lock_path.display()
        )
    })?;
    if !metadata.file_type().is_file() {
        bail!(
            "Pages bundle publish lock {} is no longer a regular file",
            guard.lock_path.display()
        );
    }
    #[cfg(unix)]
    if metadata.nlink() != 1 {
        bail!(
            "Pages bundle publish lock {} has {} hard links; refused publication",
            guard.lock_path.display(),
            metadata.nlink()
        );
    }
    let probe = File::open(&guard.lock_path).with_context(|| {
        format!(
            "failed re-opening Pages bundle publish lock {}",
            guard.lock_path.display()
        )
    })?;
    let path_identity = crate::franken_sync::FileIdentity::from_file(&probe)
        .context("failed re-identifying Pages bundle publish lock pathname")?
        .ok_or_else(|| {
            anyhow!(
                "filesystem stopped exposing a stable pathname identity for Pages bundle publish lock {}",
                guard.lock_path.display()
            )
        })?;
    if path_identity != guard.lock_identity {
        bail!(
            "Pages bundle publish lock pathname changed identity: {}",
            guard.lock_path.display()
        );
    }
    Ok(())
}

fn bundle_publish_backup_prefix(path: &Path) -> Result<String> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            anyhow!(
                "Pages bundle output has no UTF-8 file name: {}",
                path.display()
            )
        })?;
    Ok(format!(".{file_name}.publish-backup."))
}

pub(super) fn unpredictable_bundle_publish_backup_path(path: &Path) -> Result<PathBuf> {
    let prefix = bundle_publish_backup_prefix(path)?;
    let nonce = bundle_sidecar_random_nonce()?;
    Ok(path.with_file_name(format!("{prefix}{nonce:032x}")))
}

pub(super) fn bundle_path_entry_exists(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error)
            .with_context(|| format!("failed inspecting bundle path entry {}", path.display())),
    }
}

fn journal_bundle_backup_path(final_dir: &Path, journal: &BundlePublishJournal) -> Result<PathBuf> {
    let backup_name_path = Path::new(&journal.backup_file_name);
    if backup_name_path.file_name() != Some(backup_name_path.as_os_str())
        || backup_name_path.components().count() != 1
    {
        bail!(
            "Pages bundle publish journal for {} contains a non-local backup name",
            final_dir.display()
        );
    }
    let prefix = bundle_publish_backup_prefix(final_dir)?;
    let Some(nonce) = journal.backup_file_name.strip_prefix(&prefix) else {
        bail!(
            "Pages bundle publish journal for {} contains an unexpected backup name {}",
            final_dir.display(),
            journal.backup_file_name
        );
    };
    if nonce.len() != 32
        || !nonce
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!(
            "Pages bundle publish journal for {} contains an invalid backup nonce",
            final_dir.display()
        );
    }
    Ok(final_dir.with_file_name(&journal.backup_file_name))
}

pub(super) fn validate_bundle_publish_journal(
    final_dir: &Path,
    journal: &BundlePublishJournal,
) -> Result<PathBuf> {
    if journal.format != BUNDLE_PUBLISH_JOURNAL_FORMAT {
        bail!(
            "Pages bundle publish journal for {} has unsupported format {}",
            final_dir.display(),
            journal.format
        );
    }
    for (label, digest) in [
        ("prior", journal.prior_sha256.as_str()),
        ("candidate", journal.candidate_sha256.as_str()),
    ] {
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            bail!(
                "Pages bundle publish journal for {} has an invalid {label} SHA-256 digest",
                final_dir.display()
            );
        }
    }
    journal_bundle_backup_path(final_dir, journal)
}

pub(super) fn read_bundle_publish_journal(
    final_dir: &Path,
) -> Result<Option<BundlePublishJournal>> {
    let journal_path = bundle_publish_recovery_journal_path(final_dir);
    let metadata = match fs::symlink_metadata(&journal_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "failed inspecting Pages bundle publish journal {}",
                    journal_path.display()
                )
            });
        }
    };
    if !metadata.file_type().is_file() {
        bail!(
            "Pages bundle publish journal is not a regular file; preserved it without mutation: {}",
            journal_path.display()
        );
    }
    #[cfg(unix)]
    if metadata.nlink() != 1 {
        bail!(
            "Pages bundle publish journal {} has {} hard links; preserved it because exclusive pathname ownership is not provable",
            journal_path.display(),
            metadata.nlink()
        );
    }
    if metadata.len() > BUNDLE_PUBLISH_JOURNAL_MAX_BYTES {
        bail!(
            "Pages bundle publish journal exceeds the {}-byte bound; preserved it without mutation: {}",
            BUNDLE_PUBLISH_JOURNAL_MAX_BYTES,
            journal_path.display()
        );
    }

    let mut file = File::open(&journal_path).with_context(|| {
        format!(
            "failed opening Pages bundle publish journal {}",
            journal_path.display()
        )
    })?;
    let opened_identity = crate::franken_sync::FileIdentity::from_file(&file)
        .with_context(|| {
            format!(
                "failed identifying Pages bundle publish journal {}",
                journal_path.display()
            )
        })?
        .ok_or_else(|| {
            anyhow!(
                "filesystem does not expose a stable identity for Pages bundle publish journal {}",
                journal_path.display()
            )
        })?;
    let mut bytes = Vec::new();
    (&mut file)
        .take(BUNDLE_PUBLISH_JOURNAL_MAX_BYTES + 1)
        .read_to_end(&mut bytes)
        .with_context(|| {
            format!(
                "failed reading Pages bundle publish journal {}",
                journal_path.display()
            )
        })?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > BUNDLE_PUBLISH_JOURNAL_MAX_BYTES
        || u64::try_from(bytes.len()).unwrap_or(u64::MAX) != metadata.len()
    {
        bail!(
            "Pages bundle publish journal changed size while read; preserved it without mutation: {}",
            journal_path.display()
        );
    }
    let path_probe = File::open(&journal_path).with_context(|| {
        format!(
            "failed re-opening Pages bundle publish journal {}",
            journal_path.display()
        )
    })?;
    let path_identity = crate::franken_sync::FileIdentity::from_file(&path_probe)
        .with_context(|| {
            format!(
                "failed re-identifying Pages bundle publish journal {}",
                journal_path.display()
            )
        })?
        .ok_or_else(|| {
            anyhow!(
                "filesystem stopped exposing a stable identity for Pages bundle publish journal {}",
                journal_path.display()
            )
        })?;
    if path_identity != opened_identity {
        bail!(
            "Pages bundle publish journal changed identity while read; preserved it without mutation: {}",
            journal_path.display()
        );
    }
    let observed_metadata = fs::symlink_metadata(&journal_path).with_context(|| {
        format!(
            "failed re-inspecting Pages bundle publish journal {}",
            journal_path.display()
        )
    })?;
    if !observed_metadata.file_type().is_file() {
        bail!(
            "Pages bundle publish journal changed file type while read; preserved it without mutation: {}",
            journal_path.display()
        );
    }
    #[cfg(unix)]
    if observed_metadata.nlink() != 1 {
        bail!(
            "Pages bundle publish journal {} gained additional hard links while read; preserved it without mutation",
            journal_path.display()
        );
    }

    let journal: BundlePublishJournal = serde_json::from_slice(&bytes).with_context(|| {
        format!(
            "Pages bundle publish journal is invalid and was preserved without mutation: {}",
            journal_path.display()
        )
    })?;
    validate_bundle_publish_journal(final_dir, &journal)?;
    Ok(Some(journal))
}

pub(super) fn write_bundle_publish_journal(
    final_dir: &Path,
    backup_dir: &Path,
    prior: &BundleTreeEvidence,
    candidate: &BundleTreeEvidence,
) -> Result<BundlePublishJournal> {
    let backup_file_name = backup_dir
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            anyhow!(
                "Pages bundle publish backup has no UTF-8 file name: {}",
                backup_dir.display()
            )
        })?
        .to_string();
    let journal = BundlePublishJournal {
        format: BUNDLE_PUBLISH_JOURNAL_FORMAT.to_string(),
        backup_file_name,
        prior_file_count: prior.file_count,
        prior_size_bytes: prior.total_size_bytes,
        prior_sha256: prior.sha256.clone(),
        candidate_file_count: candidate.file_count,
        candidate_size_bytes: candidate.total_size_bytes,
        candidate_sha256: candidate.sha256.clone(),
    };
    validate_bundle_publish_journal(final_dir, &journal)?;
    let bytes =
        serde_json::to_vec(&journal).context("failed serializing Pages bundle publish journal")?;
    let journal_path = bundle_publish_recovery_journal_path(final_dir);
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(&journal_path).with_context(|| {
        format!(
            "failed exclusively creating Pages bundle publish journal {}; preserved the live and staged generations",
            journal_path.display()
        )
    })?;
    let created_identity = crate::franken_sync::FileIdentity::from_file(&file)
        .with_context(|| {
            format!(
                "failed identifying newly created Pages bundle publish journal {}",
                journal_path.display()
            )
        })?
        .ok_or_else(|| {
            anyhow!(
                "filesystem does not expose a stable identity for newly created Pages bundle publish journal {}",
                journal_path.display()
            )
        })?;
    file.write_all(&bytes).with_context(|| {
        format!(
            "failed writing Pages bundle publish journal {}; preserved it for inspection",
            journal_path.display()
        )
    })?;
    file.sync_all().with_context(|| {
        format!(
            "failed syncing Pages bundle publish journal {}; preserved it for inspection",
            journal_path.display()
        )
    })?;
    sync_parent_directory(&journal_path).with_context(|| {
        format!(
            "failed durably publishing Pages bundle journal {}; preserved it for recovery",
            journal_path.display()
        )
    })?;
    let path_probe = File::open(&journal_path).with_context(|| {
        format!(
            "failed re-opening newly created Pages bundle publish journal {}",
            journal_path.display()
        )
    })?;
    let path_identity = crate::franken_sync::FileIdentity::from_file(&path_probe)
        .context("failed re-identifying newly created Pages bundle publish journal")?
        .ok_or_else(|| {
            anyhow!(
                "filesystem stopped exposing a stable identity for newly created Pages bundle publish journal {}",
                journal_path.display()
            )
        })?;
    if path_identity != created_identity {
        bail!(
            "Pages bundle publish journal {} changed identity while it was created; live and staged generations were preserved",
            journal_path.display()
        );
    }
    let observed = read_bundle_publish_journal(final_dir)?.ok_or_else(|| {
        anyhow!(
            "newly created Pages bundle publish journal disappeared before publication: {}",
            journal_path.display()
        )
    })?;
    if observed != journal {
        bail!(
            "Pages bundle publish journal {} changed content before publication; live and staged generations were preserved",
            journal_path.display()
        );
    }
    Ok(journal)
}

pub(super) fn remove_bundle_publish_journal(
    final_dir: &Path,
    expected: &BundlePublishJournal,
) -> Result<()> {
    let journal_path = bundle_publish_recovery_journal_path(final_dir);
    let observed = read_bundle_publish_journal(final_dir)?.ok_or_else(|| {
        anyhow!(
            "completed Pages bundle publish journal disappeared before validated cleanup: {}",
            journal_path.display()
        )
    })?;
    if observed != *expected {
        bail!(
            "Pages bundle publish journal changed before cleanup; preserved the current entry without mutation: {}",
            journal_path.display()
        );
    }
    fs::remove_file(&journal_path).with_context(|| {
        format!(
            "failed removing completed Pages bundle publish journal {}",
            journal_path.display()
        )
    })?;
    sync_parent_directory(&journal_path)
}

pub(super) fn first_unowned_bundle_publish_backup(
    final_dir: &Path,
    owned_backup: Option<&Path>,
) -> Result<Option<PathBuf>> {
    let legacy_path = legacy_bundle_publish_in_progress_backup_path(final_dir);
    if owned_backup != Some(legacy_path.as_path()) && bundle_path_entry_exists(&legacy_path)? {
        return Ok(Some(legacy_path));
    }

    let Some(parent) = final_dir.parent() else {
        return Ok(None);
    };
    let entries = match fs::read_dir(parent) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "failed scanning Pages bundle publish namespace {}",
                    parent.display()
                )
            });
        }
    };
    let prefix = bundle_publish_backup_prefix(final_dir)?;
    for (index, entry) in entries.enumerate() {
        if index >= BUNDLE_PUBLISH_BACKUP_SCAN_LIMIT {
            bail!(
                "Pages bundle publish namespace {} exceeds the {}-entry recovery scan bound; preserved every entry",
                parent.display(),
                BUNDLE_PUBLISH_BACKUP_SCAN_LIMIT
            );
        }
        let entry = entry.with_context(|| {
            format!(
                "failed reading Pages bundle publish namespace {}",
                parent.display()
            )
        })?;
        let path = entry.path();
        if owned_backup == Some(path.as_path()) {
            continue;
        }
        if entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.starts_with(&prefix))
        {
            return Ok(Some(path));
        }
    }
    Ok(None)
}

fn update_bundle_evidence_path(hasher: &mut Sha256, kind: u8, path: &str) -> Result<()> {
    let path_len = u64::try_from(path.len())
        .map_err(|_| anyhow!("Pages bundle relative path length exceeds u64"))?;
    hasher.update([kind]);
    hasher.update(path_len.to_le_bytes());
    hasher.update(path.as_bytes());
    Ok(())
}

pub(super) fn inspect_bundle_tree(path: &Path, label: &str) -> Result<BundleTreeEvidence> {
    inspect_bundle_tree_with_entry_limit(path, label, BUNDLE_TREE_EVIDENCE_ENTRY_LIMIT)
}

pub(super) fn inspect_bundle_tree_with_entry_limit(
    path: &Path,
    label: &str,
    entry_limit: usize,
) -> Result<BundleTreeEvidence> {
    if !ensure_bundle_directory_entry(path, label)? {
        bail!("{label} does not exist: {}", path.display());
    }

    let mut hasher = Sha256::new();
    hasher.update(BUNDLE_TREE_EVIDENCE_DOMAIN);
    let mut file_count = 0_u64;
    let mut total_size_bytes = 0_u64;
    let mut entry_count = 0_usize;
    let mut pending_directories = vec![path.to_path_buf()];
    let mut buffer = [0_u8; 64 * 1024];

    while let Some(directory) = pending_directories.pop() {
        let read_dir = fs::read_dir(&directory)
            .with_context(|| format!("failed reading {label} directory {}", directory.display()))?;
        let mut entries = Vec::new();
        for entry in read_dir {
            if entry_count >= entry_limit {
                bail!(
                    "{label} exceeds the {entry_limit}-entry recovery-evidence bound; preserved the tree without mutation: {}",
                    path.display()
                );
            }
            entries.push(entry.with_context(|| {
                format!("failed walking {label} directory {}", directory.display())
            })?);
            entry_count += 1;
        }
        entries.sort_by_key(|entry| entry.file_name());
        let mut child_directories = Vec::new();

        for entry in entries {
            let entry_path = entry.path();
            let relative = entry_path.strip_prefix(path).with_context(|| {
                format!(
                    "{label} entry escaped its root {}: {}",
                    path.display(),
                    entry_path.display()
                )
            })?;
            let relative = relative.to_str().ok_or_else(|| {
                anyhow!(
                    "{label} contains a non-UTF-8 relative path, which cannot be recovery-bound: {}",
                    entry_path.display()
                )
            })?;
            let metadata = fs::symlink_metadata(&entry_path).with_context(|| {
                format!("failed inspecting {label} entry {}", entry_path.display())
            })?;
            let file_type = metadata.file_type();
            if file_type.is_symlink() {
                bail!(
                    "{label} contains a symlink, which cannot be safely published or recursively cleaned: {}",
                    entry_path.display()
                );
            }
            if file_type.is_dir() {
                update_bundle_evidence_path(&mut hasher, b'D', relative)?;
                child_directories.push(entry_path);
                continue;
            }
            if !file_type.is_file() {
                bail!(
                    "{label} contains an unsupported filesystem entry: {}",
                    entry_path.display()
                );
            }

            let mut file = File::open(&entry_path)
                .with_context(|| format!("failed opening {label} file {}", entry_path.display()))?;
            let opened_metadata = file.metadata().with_context(|| {
                format!(
                    "failed inspecting opened {label} file {}",
                    entry_path.display()
                )
            })?;
            if !opened_metadata.file_type().is_file() {
                bail!(
                    "{label} entry changed file type while opening: {}",
                    entry_path.display()
                );
            }
            #[cfg(unix)]
            if opened_metadata.nlink() != 1 {
                bail!(
                    "{label} file {} has {} hard links; exclusive tree ownership is not provable",
                    entry_path.display(),
                    opened_metadata.nlink()
                );
            }
            let opened_identity = crate::franken_sync::FileIdentity::from_file(&file)
                .with_context(|| {
                    format!(
                        "failed identifying opened {label} file {}",
                        entry_path.display()
                    )
                })?
                .ok_or_else(|| {
                    anyhow!(
                        "filesystem does not expose a stable identity for {label} file {}",
                        entry_path.display()
                    )
                })?;
            update_bundle_evidence_path(&mut hasher, b'F', relative)?;
            hasher.update(opened_metadata.len().to_le_bytes());
            let mut observed_size = 0_u64;
            loop {
                let read = file.read(&mut buffer).with_context(|| {
                    format!("failed hashing {label} file {}", entry_path.display())
                })?;
                if read == 0 {
                    break;
                }
                observed_size = observed_size
                    .checked_add(u64::try_from(read).unwrap_or(u64::MAX))
                    .ok_or_else(|| anyhow!("{label} size overflow while hashing"))?;
                hasher.update(&buffer[..read]);
            }
            if observed_size != opened_metadata.len() {
                bail!(
                    "{label} file changed size while hashing: {}",
                    entry_path.display()
                );
            }
            let path_probe = File::open(&entry_path).with_context(|| {
                format!("failed re-opening {label} file {}", entry_path.display())
            })?;
            let path_identity = crate::franken_sync::FileIdentity::from_file(&path_probe)
                .with_context(|| {
                    format!(
                        "failed re-identifying {label} file {}",
                        entry_path.display()
                    )
                })?
                .ok_or_else(|| {
                    anyhow!(
                        "filesystem stopped exposing a stable identity for {label} file {}",
                        entry_path.display()
                    )
                })?;
            if path_identity != opened_identity {
                bail!(
                    "{label} file changed identity while hashing: {}",
                    entry_path.display()
                );
            }
            let observed_metadata = fs::symlink_metadata(&entry_path).with_context(|| {
                format!("failed re-inspecting {label} file {}", entry_path.display())
            })?;
            if !observed_metadata.file_type().is_file()
                || observed_metadata.len() != opened_metadata.len()
            {
                bail!(
                    "{label} file changed metadata while hashing: {}",
                    entry_path.display()
                );
            }
            #[cfg(unix)]
            if observed_metadata.nlink() != 1 {
                bail!(
                    "{label} file {} gained additional hard links while hashing",
                    entry_path.display()
                );
            }
            file_count = file_count
                .checked_add(1)
                .ok_or_else(|| anyhow!("{label} file count overflow"))?;
            total_size_bytes = total_size_bytes
                .checked_add(opened_metadata.len())
                .ok_or_else(|| anyhow!("{label} total size overflow"))?;
        }

        for child in child_directories.into_iter().rev() {
            pending_directories.push(child);
        }
    }

    Ok(BundleTreeEvidence {
        file_count,
        total_size_bytes,
        sha256: hex::encode(hasher.finalize()),
    })
}

pub(super) fn journal_prior_evidence(journal: &BundlePublishJournal) -> BundleTreeEvidence {
    BundleTreeEvidence {
        file_count: journal.prior_file_count,
        total_size_bytes: journal.prior_size_bytes,
        sha256: journal.prior_sha256.clone(),
    }
}

pub(super) fn journal_candidate_evidence(journal: &BundlePublishJournal) -> BundleTreeEvidence {
    BundleTreeEvidence {
        file_count: journal.candidate_file_count,
        total_size_bytes: journal.candidate_size_bytes,
        sha256: journal.candidate_sha256.clone(),
    }
}

pub(super) fn ensure_bundle_tree_matches(
    path: &Path,
    label: &str,
    expected: &BundleTreeEvidence,
) -> Result<BundleTreeEvidence> {
    let observed = inspect_bundle_tree(path, label)?;
    if observed != *expected {
        bail!(
            "{label} changed identity or content; preserved it without mutation: {}",
            path.display()
        );
    }
    Ok(observed)
}

pub(super) fn unique_bundle_sidecar_path(
    path: &Path,
    suffix: &str,
    fallback_name: &str,
) -> Result<PathBuf> {
    static NEXT_NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    let random_nonce = bundle_sidecar_random_nonce()?;
    let nonce = NEXT_NONCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(fallback_name);

    Ok(path.with_file_name(format!(".{file_name}.{suffix}.{random_nonce:032x}.{nonce}")))
}

fn bundle_sidecar_random_nonce() -> Result<u128> {
    let mut bytes = [0u8; 16];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| anyhow!("failed to generate random bundle sidecar nonce"))?;
    Ok(u128::from_le_bytes(bytes))
}

pub(super) fn ensure_replaceable_bundle_output_dir(path: &Path) -> Result<bool> {
    ensure_bundle_directory_entry(path, "bundle output path")
}

pub(super) fn ensure_bundle_directory_entry(path: &Path, label: &str) -> Result<bool> {
    ensure_existing_parent_ancestors_are_real_dirs(path, label)?;

    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            let file_type = metadata.file_type();
            if file_type.is_symlink() {
                bail!("{label} must not be a symlink: {}", path.display());
            }
            if !file_type.is_dir() {
                bail!(
                    "{label} points to a file, expected a directory: {}",
                    path.display()
                );
            }
            Ok(true)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => {
            Err(err).with_context(|| format!("failed inspecting {label} {}", path.display()))
        }
    }
}

pub(super) fn ensure_existing_parent_ancestors_are_real_dirs(
    path: &Path,
    label: &str,
) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };

    let mut ancestors = Vec::new();
    let mut current = Some(parent);
    while let Some(ancestor) = current {
        if ancestor.as_os_str().is_empty() {
            break;
        }
        ancestors.push(ancestor.to_path_buf());
        current = ancestor.parent();
    }
    ancestors.reverse();

    for ancestor in ancestors {
        match fs::symlink_metadata(&ancestor) {
            Ok(metadata) => {
                let file_type = metadata.file_type();
                if file_type.is_symlink() {
                    if is_allowed_system_symlink_ancestor(&ancestor) {
                        continue;
                    }
                    bail!(
                        "{label} parent must not contain symlinks: {}",
                        ancestor.display()
                    );
                }
                if !file_type.is_dir() {
                    bail!("{label} parent must be a directory: {}", ancestor.display());
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed inspecting {label} parent {}", ancestor.display())
                });
            }
        }
    }

    Ok(())
}

#[cfg(target_os = "macos")]
fn is_allowed_system_symlink_ancestor(path: &Path) -> bool {
    path == Path::new("/var") || path == Path::new("/tmp")
}

#[cfg(not(target_os = "macos"))]
fn is_allowed_system_symlink_ancestor(_path: &Path) -> bool {
    false
}
