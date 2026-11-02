//! Retained strict engine connection. No initializer, recovery, or checkpoint lane.
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use anyhow::{Context, Result};
use coding_agent_search::franken_sync::compat::{OpenFlags, RowExt, open_with_flags};
use coding_agent_search::franken_sync::Connection;
use serde::Serialize;

use super::protocol::refuse;

const FULL_HASH_LIMIT: u64 = 16 * 1024 * 1024;
const EDGE_HASH_BYTES: usize = 65_536;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArchiveStamp {
    pub canonical_path: String,
    pub device: Option<u64>,
    pub inode: Option<u64>,
    pub schema_version: i64,
}

fn hash_file_state(hasher: &mut blake3::Hasher, path: &Path) -> Result<()> {
    let meta = std::fs::symlink_metadata(path)
        .map_err(|e| refuse("canonical_db_missing", format!("Cannot inspect canonical archive: {e}")))?;
    if !meta.file_type().is_file() {
        return Err(refuse("canonical_read_failed", "Archive must be a regular file, not a symlink"));
    }
    let len = meta.len();
    hasher.update(&len.to_le_bytes());
    if let Ok(modified) = meta.modified()
        && let Ok(duration) = modified.duration_since(UNIX_EPOCH)
    {
        hasher.update(&duration.as_secs().to_le_bytes());
        hasher.update(&duration.subsec_nanos().to_le_bytes());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        hasher.update(&meta.dev().to_le_bytes());
        hasher.update(&meta.ino().to_le_bytes());
        hasher.update(&meta.mtime().to_le_bytes());
        hasher.update(&meta.mtime_nsec().to_le_bytes());
        hasher.update(&meta.ctime().to_le_bytes());
        hasher.update(&meta.ctime_nsec().to_le_bytes());
    }
    let mut file = File::open(path)
        .map_err(|e| refuse("canonical_read_failed", format!("Cannot read canonical archive bytes: {e}")))?;
    if len <= FULL_HASH_LIMIT {
        let mut buf = Vec::with_capacity(len as usize);
        file.read_to_end(&mut buf)
            .map_err(|e| refuse("canonical_read_failed", format!("Cannot read canonical archive: {e}")))?;
        hasher.update(&buf);
    } else {
        let mut head = vec![0_u8; EDGE_HASH_BYTES];
        let head_read = file.read(&mut head)
            .map_err(|e| refuse("canonical_read_failed", format!("Cannot read canonical archive header: {e}")))?;
        hasher.update(&head[..head_read]);
        file.seek(SeekFrom::End(-(EDGE_HASH_BYTES as i64)))
            .map_err(|e| refuse("canonical_read_failed", format!("Cannot seek canonical archive tail: {e}")))?;
        let mut tail = vec![0_u8; EDGE_HASH_BYTES];
        let tail_read = file.read(&mut tail)
            .map_err(|e| refuse("canonical_read_failed", format!("Cannot read canonical archive tail: {e}")))?;
        hasher.update(&tail[..tail_read]);
    }
    Ok(())
}

fn disk_revision(path: &Path) -> Result<String> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"db:");
    hash_file_state(&mut hasher, path)?;
    for suffix in ["-wal", "-journal"] {
        let sidecar = PathBuf::from(format!("{}{suffix}", path.display()));
        match std::fs::symlink_metadata(&sidecar) {
            Ok(meta) if meta.file_type().is_file() => {
                hasher.update(suffix.as_bytes());
                let _ = hash_file_state(&mut hasher, &sidecar);
            }
            _ => {
                hasher.update(b"none:");
                hasher.update(suffix.as_bytes());
            }
        }
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn stamp(path: &Path, schema_version: i64) -> Result<(ArchiveStamp, u64)> {
    let meta = std::fs::symlink_metadata(path)
        .map_err(|e| refuse("canonical_db_missing", format!("Cannot inspect canonical archive: {e}")))?;
    if !meta.file_type().is_file() {
        return Err(refuse("canonical_read_failed", "Archive must be a regular file, not a symlink"));
    }
    let canonical = std::fs::canonicalize(path)?;
    let canonical_path = canonical.to_str().context("Archive path is not UTF-8")?.to_owned();
    #[cfg(unix)]
    let (device, inode) = {
        use std::os::unix::fs::MetadataExt;
        (Some(meta.dev()), Some(meta.ino()))
    };
    #[cfg(not(unix))]
    let (device, inode) = (None, None);
    Ok((ArchiveStamp { canonical_path, device, inode, schema_version }, meta.len()))
}

pub fn strict_open(path: &Path) -> Result<Connection> {
    stamp(path, 22)?;
    let text = path.to_str().context("Archive path is not UTF-8")?;
    let connection = open_with_flags(text, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .context("Open canonical archive strictly read-only")?;
    connection.execute("PRAGMA query_only = ON")?;
    connection.execute("PRAGMA busy_timeout = 250")?;
    Ok(connection)
}

fn schema_version(connection: &Connection) -> Result<i64> {
    let rows = connection.query("SELECT value FROM meta WHERE key = 'schema_version' LIMIT 2")?;
    if rows.len() != 1 {
        return Err(refuse("incompatible_schema", "Missing or ambiguous canonical schema version"));
    }
    let text: String = rows[0].get_typed(0)?;
    if text != "22" {
        return Err(refuse("incompatible_schema", "Bridge requires the pinned schema-22 profile"));
    }
    Ok(22)
}

pub struct StrictArchive {
    path: PathBuf,
    connection: Connection,
    pub stamp: ArchiveStamp,
    size: u64,
    nonce: String,
    data_version: i64,
    disk_revision: String,
}

impl StrictArchive {
    pub fn open(path: &Path) -> Result<Self> {
        for attempt in 0..4 {
            let before = stamp(path, 22)?;
            let before_disk = disk_revision(path)?;
            let connection = strict_open(path)?;
            schema_version(&connection)?;
            super::catalog::check_schema(&connection)
                .map_err(|e| refuse("incompatible_schema", format!("Canonical reader profile mismatch: {e}")))?;
            let after = stamp(path, 22)?;
            let after_disk = disk_revision(path)?;
            if before.0 != after.0 || after.1 < before.1 || before_disk != after_disk {
                if attempt < 3 {
                    continue;
                }
                return Err(refuse("stale_snapshot", "Archive replaced during strict open"));
            }
            let data_version = connection.query_row("PRAGMA data_version")?.get_typed(0)?;
            return Ok(Self {
                path: path.to_owned(), connection, stamp: after.0, size: after.1,
                nonce: format!("{:032x}", rand::random::<u128>()), data_version,
                disk_revision: after_disk,
            });
        }
        unreachable!()
    }

    pub fn observe(&mut self) -> Result<String> {
        let (observed, size) = stamp(&self.path, 22)?;
        if observed != self.stamp || size < self.size {
            return Err(refuse("stale_snapshot", "Canonical archive identity changed"));
        }
        let current_disk = disk_revision(&self.path)?;
        if current_disk != self.disk_revision {
            for attempt in 0..4 {
                let target_disk = if attempt == 0 {
                    current_disk.clone()
                } else {
                    disk_revision(&self.path)?
                };
                let connection = strict_open(&self.path)?;
                schema_version(&connection)?;
                super::catalog::check_schema(&connection)
                    .map_err(|e| refuse("incompatible_schema", format!("Canonical reader profile mismatch: {e}")))?;
                let (rechecked, rechecked_size) = stamp(&self.path, 22)?;
                let rechecked_disk = disk_revision(&self.path)?;
                if rechecked != self.stamp || rechecked_size < size {
                    return Err(refuse("stale_snapshot", "Canonical archive changed during refresh"));
                }
                if rechecked_disk != target_disk {
                    if attempt < 3 {
                        continue;
                    }
                    return Err(refuse("stale_snapshot", "Canonical archive changed during refresh"));
                }
                self.connection = connection;
                self.disk_revision = rechecked_disk;
                self.size = rechecked_size;
                break;
            }
        } else {
            schema_version(&self.connection)?;
            self.size = size;
        }
        self.data_version = self.connection.query_row("PRAGMA data_version")?.get_typed(0)?;
        Ok(format!("{}:{}:{}", self.nonce, self.data_version, self.disk_revision))
    }

    pub fn snapshot<T>(&mut self, read: impl FnOnce(&Connection) -> Result<T>) -> Result<(T, String)> {
        let before = self.observe()?;
        self.connection.execute("BEGIN DEFERRED")?;
        let result = read(&self.connection);
        let release = self.connection.execute("ROLLBACK");
        if let Err(error) = release {
            return Err(refuse("canonical_read_failed", format!("Strict snapshot cleanup failed: {error}")));
        }
        let value = result?;
        let after = self.observe()?;
        if before != after {
            return Err(refuse("stale_snapshot", "Canonical archive changed while reading a page"));
        }
        Ok((value, before))
    }
}
