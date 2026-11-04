//! Retained strict engine connection. No initializer, recovery, or checkpoint lane.
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use anyhow::{Context, Result};
use coding_agent_search::franken_sync::Connection;
use coding_agent_search::franken_sync::compat::{OpenFlags, RowExt, open_with_flags};
use serde::Serialize;

use super::protocol::refuse;

const FULL_HASH_LIMIT: u64 = 16 * 1024 * 1024;
const EDGE_HASH_BYTES: usize = 65_536;

/// Canonical schema profile this strict reader is qualified against. Kept
/// pinned on purpose: a newer canonical schema must fail qualification
/// instead of being read under stale assumptions.
pub(crate) const STRICT_SCHEMA_VERSION: i64 = 23;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArchiveStamp {
    pub canonical_path: String,
    pub device: Option<u64>,
    pub inode: Option<u64>,
    pub schema_version: i64,
}

fn hash_file_state(hasher: &mut blake3::Hasher, path: &Path) -> Result<()> {
    let meta = std::fs::symlink_metadata(path).map_err(|e| {
        refuse(
            "canonical_db_missing",
            format!("Cannot inspect canonical archive: {e}"),
        )
    })?;
    if !meta.file_type().is_file() {
        return Err(refuse(
            "canonical_read_failed",
            "Archive must be a regular file, not a symlink",
        ));
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
    let mut file = File::open(path).map_err(|e| {
        refuse(
            "canonical_read_failed",
            format!("Cannot read canonical archive bytes: {e}"),
        )
    })?;
    if len <= FULL_HASH_LIMIT {
        let mut buf = Vec::with_capacity(len as usize);
        file.read_to_end(&mut buf).map_err(|e| {
            refuse(
                "canonical_read_failed",
                format!("Cannot read canonical archive: {e}"),
            )
        })?;
        hasher.update(&buf);
    } else {
        let mut head = vec![0_u8; EDGE_HASH_BYTES];
        let head_read = file.read(&mut head).map_err(|e| {
            refuse(
                "canonical_read_failed",
                format!("Cannot read canonical archive header: {e}"),
            )
        })?;
        hasher.update(&head[..head_read]);
        file.seek(SeekFrom::End(-(EDGE_HASH_BYTES as i64)))
            .map_err(|e| {
                refuse(
                    "canonical_read_failed",
                    format!("Cannot seek canonical archive tail: {e}"),
                )
            })?;
        let mut tail = vec![0_u8; EDGE_HASH_BYTES];
        let tail_read = file.read(&mut tail).map_err(|e| {
            refuse(
                "canonical_read_failed",
                format!("Cannot read canonical archive tail: {e}"),
            )
        })?;
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

/// Metadata-only revision token covering the same file set as
/// [`disk_revision`] — the canonical db plus its `-wal`/`-journal`
/// sidecars — mixed from `(dev, ino, len, mtime_ns, ctime_ns)` per present
/// file, read by `lstat` alone (no opens, no byte reads).
///
/// Soundness of the fast path: every successful content write to a regular
/// file must move `mtime`; restoring `mtime` via `utimens(2)` stamps a
/// FRESH `ctime` — inode change-time is never writable through a syscall,
/// so an unchanged `(ctime, mtime, len, ino, dev)` tuple proves no write
/// touched the file since the token was captured. On non-Unix targets the
/// token deliberately falls back to a per-call unique marker so
/// [`StrictArchive::observe`] always takes the full content-hash path
/// (same behavior as before this token existed).
#[cfg(unix)]
fn meta_revision(path: &Path) -> Result<String> {
    fn feed(hasher: &mut blake3::Hasher, meta: &std::fs::Metadata, tag: &[u8]) {
        use std::os::unix::fs::MetadataExt;
        hasher.update(tag);
        hasher.update(&meta.len().to_le_bytes());
        hasher.update(&meta.dev().to_le_bytes());
        hasher.update(&meta.ino().to_le_bytes());
        hasher.update(&meta.mtime().to_le_bytes());
        hasher.update(&meta.mtime_nsec().to_le_bytes());
        hasher.update(&meta.ctime().to_le_bytes());
        hasher.update(&meta.ctime_nsec().to_le_bytes());
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"meta:");
    let db_meta = std::fs::symlink_metadata(path).map_err(|e| {
        refuse(
            "canonical_db_missing",
            format!("Cannot inspect canonical archive: {e}"),
        )
    })?;
    if !db_meta.file_type().is_file() {
        return Err(refuse(
            "canonical_read_failed",
            "Archive must be a regular file, not a symlink",
        ));
    }
    feed(&mut hasher, &db_meta, b"db");
    for suffix in ["-wal", "-journal"] {
        let sidecar = PathBuf::from(format!("{}{suffix}", path.display()));
        match std::fs::symlink_metadata(&sidecar) {
            Ok(meta) if meta.file_type().is_file() => feed(&mut hasher, &meta, suffix.as_bytes()),
            _ => {
                hasher.update(b"none:");
                hasher.update(suffix.as_bytes());
            }
        }
    }
    Ok(hasher.finalize().to_hex().to_string())
}

/// Non-Unix fallback: without an unforgeable inode change-time the metadata
/// token cannot prove "no write happened", so every call yields a distinct
/// marker and [`StrictArchive::observe`] always verifies content the slow
/// way — identical to the pre-token behavior.
#[cfg(not(unix))]
fn meta_revision(path: &Path) -> Result<String> {
    let _ = path;
    Ok(format!("uncached:{}", rand::random::<u128>()))
}

fn stamp(path: &Path, schema_version: i64) -> Result<(ArchiveStamp, u64)> {
    let meta = std::fs::symlink_metadata(path).map_err(|e| {
        refuse(
            "canonical_db_missing",
            format!("Cannot inspect canonical archive: {e}"),
        )
    })?;
    if !meta.file_type().is_file() {
        return Err(refuse(
            "canonical_read_failed",
            "Archive must be a regular file, not a symlink",
        ));
    }
    let canonical = std::fs::canonicalize(path)?;
    let canonical_path = canonical
        .to_str()
        .context("Archive path is not UTF-8")?
        .to_owned();
    #[cfg(unix)]
    let (device, inode) = {
        use std::os::unix::fs::MetadataExt;
        (Some(meta.dev()), Some(meta.ino()))
    };
    #[cfg(not(unix))]
    let (device, inode) = (None, None);
    Ok((
        ArchiveStamp {
            canonical_path,
            device,
            inode,
            schema_version,
        },
        meta.len(),
    ))
}

pub fn strict_open(path: &Path) -> Result<Connection> {
    stamp(path, STRICT_SCHEMA_VERSION)?;
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
        return Err(refuse(
            "incompatible_schema",
            "Missing or ambiguous canonical schema version",
        ));
    }
    let text: String = rows[0].get_typed(0)?;
    if text != "23" {
        return Err(refuse(
            "incompatible_schema",
            "Bridge requires the pinned schema-23 profile",
        ));
    }
    Ok(STRICT_SCHEMA_VERSION)
}

pub struct StrictArchive {
    path: PathBuf,
    connection: Connection,
    pub stamp: ArchiveStamp,
    size: u64,
    nonce: String,
    data_version: i64,
    disk_revision: String,
    /// Last observed [`meta_revision`] token — the cheap staleness proof
    /// that lets [`StrictArchive::observe`] skip the full content hash
    /// when nothing on disk could have changed.
    meta_revision: String,
}

/// Extension-lane visibility observed through the strict read connection.
/// `generation` is `None` on unextended archives; `canonical_observers`
/// reports that EVERY canonical-observer trigger is installed, which is the
/// precondition for trusting `canonical_revision` as content-complete.
pub struct ExtensionState {
    pub installed: bool,
    pub generation: Option<super::protocol::Generation>,
    pub canonical_observers: bool,
}

impl StrictArchive {
    pub fn open(path: &Path) -> Result<Self> {
        for attempt in 0..4 {
            let before = stamp(path, STRICT_SCHEMA_VERSION)?;
            let before_disk = disk_revision(path)?;
            let connection = strict_open(path)?;
            schema_version(&connection)?;
            super::catalog::check_schema(&connection).map_err(|e| {
                refuse(
                    "incompatible_schema",
                    format!("Canonical reader profile mismatch: {e}"),
                )
            })?;
            let after = stamp(path, STRICT_SCHEMA_VERSION)?;
            let after_disk = disk_revision(path)?;
            if before.0 != after.0 || after.1 < before.1 || before_disk != after_disk {
                if attempt < 3 {
                    continue;
                }
                return Err(refuse(
                    "stale_snapshot",
                    "Archive replaced during strict open",
                ));
            }
            let data_version = connection.query_row("PRAGMA data_version")?.get_typed(0)?;
            return Ok(Self {
                path: path.to_owned(),
                connection,
                stamp: after.0,
                size: after.1,
                nonce: format!("{:032x}", rand::random::<u128>()),
                data_version,
                disk_revision: after_disk,
                // If the file moved between the stability check and this
                // probe, the next `observe` sees a meta mismatch and takes
                // the full hash path — self-healing, never a false "fresh".
                meta_revision: meta_revision(path)?,
            });
        }
        unreachable!()
    }

    pub fn observe(&mut self) -> Result<String> {
        let (observed, size) = stamp(&self.path, STRICT_SCHEMA_VERSION)?;
        if observed != self.stamp || size < self.size {
            return Err(refuse(
                "stale_snapshot",
                "Canonical archive identity changed",
            ));
        }
        // Cheap staleness gate: an unchanged metadata token proves no write
        // touched the archive family (see `meta_revision`), so the full
        // content hash — the per-RPC cost this replaces — is skipped only
        // when it is provably redundant. Metadata churn takes the same
        // refresh path the hash did.
        let current_meta = meta_revision(&self.path)?;
        if current_meta != self.meta_revision {
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
                    super::catalog::check_schema(&connection).map_err(|e| {
                        refuse(
                            "incompatible_schema",
                            format!("Canonical reader profile mismatch: {e}"),
                        )
                    })?;
                    let (rechecked, rechecked_size) = stamp(&self.path, STRICT_SCHEMA_VERSION)?;
                    let rechecked_meta = meta_revision(&self.path)?;
                    // The stability proof the hash supplied is redundant
                    // when the metadata token still matches `current_meta`
                    // (no write could have landed); only fresh metadata
                    // churn pays for another content read.
                    let rechecked_disk = if rechecked_meta == current_meta {
                        target_disk.clone()
                    } else {
                        disk_revision(&self.path)?
                    };
                    if rechecked != self.stamp || rechecked_size < size {
                        return Err(refuse(
                            "stale_snapshot",
                            "Canonical archive changed during refresh",
                        ));
                    }
                    if rechecked_disk != target_disk {
                        if attempt < 3 {
                            continue;
                        }
                        return Err(refuse(
                            "stale_snapshot",
                            "Canonical archive changed during refresh",
                        ));
                    }
                    self.connection = connection;
                    self.disk_revision = rechecked_disk;
                    self.meta_revision = rechecked_meta;
                    self.size = rechecked_size;
                    break;
                }
            } else {
                // Metadata churn without a content move (e.g. a WAL-index
                // touch): the retained connection stays qualified — the
                // schema probe it re-ran here before was redundant then and
                // stays redundant now — and the token is refreshed so the
                // next observe re-enters the fast path.
                self.meta_revision = current_meta;
                self.size = size;
            }
        } else {
            self.size = size;
        }
        self.data_version = self
            .connection
            .query_row("PRAGMA data_version")?
            .get_typed(0)?;
        Ok(format!(
            "{}:{}:{}",
            self.nonce, self.data_version, self.disk_revision
        ))
    }

    /// Committed logical generation read through the strict connection, or
    /// `None` on unextended archives. Pure read: it never refreshes the
    /// connection, so callers decide where in the snapshot discipline it
    /// belongs (`observe` refreshes first when the physical token moved).
    pub fn read_generation(&self) -> Result<Option<super::protocol::Generation>> {
        super::revisions::generation(&self.connection)
    }

    /// `true` only when every canonical-observer trigger is installed;
    /// `canonical_revision` is content-complete only under this guard.
    pub fn canonical_observers(&self) -> Result<bool> {
        Ok(super::revisions::extensions_installed(&self.connection)?
            && super::revisions::observers_installed(&self.connection)?)
    }

    /// Extension-lane state on the strict connection. Callers run
    /// `observe()` first so a just-committed extension write is visible on
    /// the refreshed connection before this read.
    pub fn extension_state(&mut self) -> Result<ExtensionState> {
        let installed = super::revisions::extensions_installed(&self.connection)?;
        let generation = if installed {
            self.read_generation()?
        } else {
            None
        };
        let canonical_observers = installed && self.canonical_observers()?;
        Ok(ExtensionState {
            installed,
            generation,
            canonical_observers,
        })
    }

    pub fn snapshot<T>(
        &mut self,
        read: impl FnOnce(&Connection) -> Result<T>,
    ) -> Result<(T, String)> {
        self.snapshot_stamped(|connection, _revision| read(connection))
    }

    /// Same strict read discipline as [`snapshot`], additionally handing
    /// the read closure the pre-read `observe` token so callers can key
    /// revision-scoped memos to exactly the snapshot state the closure
    /// sees. The token is this archive's `nonce:data_version:disk_revision`
    /// — it changes exactly when the observable archive does, so a memo
    /// keyed to it can never bind stale content (the `before == after`
    /// check below still rejects a mid-read change).
    pub fn snapshot_stamped<T>(
        &mut self,
        read: impl FnOnce(&Connection, &str) -> Result<T>,
    ) -> Result<(T, String)> {
        let before = self.observe()?;
        self.connection.execute("BEGIN DEFERRED")?;
        let result = read(&self.connection, &before);
        let release = self.connection.execute("ROLLBACK");
        if let Err(error) = release {
            return Err(refuse(
                "canonical_read_failed",
                format!("Strict snapshot cleanup failed: {error}"),
            ));
        }
        let value = result?;
        let after = self.observe()?;
        if before != after {
            return Err(refuse(
                "stale_snapshot",
                "Canonical archive changed while reading a page",
            ));
        }
        Ok((value, before))
    }
}
