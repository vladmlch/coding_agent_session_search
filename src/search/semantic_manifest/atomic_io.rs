use super::legacy::MANIFEST_FILENAME;
use super::{OpenOptions, Path, PathBuf, SecureRandom, SystemRandom, SystemTime, UNIX_EPOCH, fs};

// ─── Errors ────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum ManifestError {
    Io { path: PathBuf, source: String },
    Parse { path: PathBuf, source: String },
    Serialize { source: String },
    UnsupportedVersion { found: u32, max_supported: u32 },
}

impl std::fmt::Display for ManifestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io { path, source } => {
                write!(f, "manifest I/O error at {}: {source}", path.display())
            }
            Self::Parse { path, source } => {
                write!(f, "manifest parse error at {}: {source}", path.display())
            }
            Self::Serialize { source } => write!(f, "manifest serialization error: {source}"),
            Self::UnsupportedVersion {
                found,
                max_supported,
            } => write!(
                f,
                "manifest version {found} is newer than supported version {max_supported}"
            ),
        }
    }
}

impl std::error::Error for ManifestError {}

// ─── Helpers ───────────────────────────────────────────────────────────────

pub(in crate::search::semantic_manifest) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn unique_manifest_temp_path(path: &Path, attempt: u32, random: u64) -> PathBuf {
    static NEXT_NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(MANIFEST_FILENAME);
    let nonce = NEXT_NONCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    path.with_file_name(format!(
        ".{file_name}.tmp.{attempt}.{}.{}.{random:016x}",
        now_ms(),
        nonce
    ))
}

pub(in crate::search::semantic_manifest) fn create_unique_manifest_temp_file(
    path: &Path,
) -> std::io::Result<(PathBuf, fs::File)> {
    for attempt in 0..100 {
        let random = random_manifest_path_nonce()?;
        let tmp_path = unique_manifest_temp_path(path, attempt, random);
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp_path)
        {
            Ok(file) => return Ok((tmp_path, file)),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => return Err(err),
        }
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        format!(
            "could not create a unique temporary manifest file for {} after 100 attempts",
            path.display()
        ),
    ))
}

fn random_manifest_path_nonce() -> std::io::Result<u64> {
    let mut random_bytes = [0u8; 8];
    SystemRandom::new()
        .fill(&mut random_bytes)
        .map_err(|_| std::io::Error::other("failed to generate manifest temp path nonce"))?;
    Ok(u64::from_le_bytes(random_bytes))
}

pub(in crate::search::semantic_manifest) fn replace_file_from_temp(
    temp_path: &Path,
    final_path: &Path,
) -> std::io::Result<()> {
    #[cfg(windows)]
    {
        windows_move_file_write_through(temp_path, final_path, true)?;
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(final_path)?
            .sync_all()
    }
    #[cfg(not(windows))]
    {
        let temp = tempfile::TempPath::try_from_path(temp_path.to_path_buf())?;
        temp.persist(final_path).map_err(|error| error.error)?;
        sync_parent_directory(final_path)
    }
}

#[cfg(not(windows))]
pub(in crate::search::semantic_manifest) fn persist_named_temp(
    temp: tempfile::NamedTempFile,
    final_path: &Path,
    replace_existing: bool,
) -> std::io::Result<fs::File> {
    if replace_existing {
        temp.persist(final_path).map_err(|error| error.error)
    } else {
        temp.persist_noclobber(final_path)
            .map_err(|error| error.error)
    }
}

#[cfg(windows)]
pub(in crate::search::semantic_manifest) fn persist_named_temp(
    temp: tempfile::NamedTempFile,
    final_path: &Path,
    replace_existing: bool,
) -> std::io::Result<fs::File> {
    windows_move_file_write_through(temp.path(), final_path, replace_existing)?;
    Ok(temp.into_file())
}

#[cfg(windows)]
#[allow(unsafe_code)]
fn windows_move_file_write_through(
    source: &Path,
    destination: &Path,
    replace_existing: bool,
) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt;

    const FILE_ATTRIBUTE_NORMAL: u32 = 0x0000_0080;
    const MOVEFILE_REPLACE_EXISTING: u32 = 0x0000_0001;
    const MOVEFILE_WRITE_THROUGH: u32 = 0x0000_0008;

    // SAFETY: declarations match the documented kernel32 prototypes.
    #[allow(unsafe_code)]
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn MoveFileExW(
            existing_file_name: *const u16,
            new_file_name: *const u16,
            flags: u32,
        ) -> i32;
        fn SetFileAttributesW(file_name: *const u16, file_attributes: u32) -> i32;
    }

    fn wide_path(path: &Path) -> std::io::Result<Vec<u16>> {
        let mut wide = path.as_os_str().encode_wide().collect::<Vec<_>>();
        if wide.contains(&0) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Windows path contains an interior NUL",
            ));
        }
        wide.push(0);
        Ok(wide)
    }

    let source = wide_path(source)?;
    let destination = wide_path(destination)?;
    let flags = MOVEFILE_WRITE_THROUGH
        | if replace_existing {
            MOVEFILE_REPLACE_EXISTING
        } else {
            0
        };
    // SAFETY: both pointers reference NUL-terminated UTF-16 buffers that live
    // through each call. The flags are documented MoveFileExW values. The
    // source is a private same-directory temporary file and the destination
    // was validated before this helper is invoked.
    unsafe {
        if SetFileAttributesW(source.as_ptr(), FILE_ATTRIBUTE_NORMAL) == 0 {
            return Err(std::io::Error::last_os_error());
        }
        if MoveFileExW(source.as_ptr(), destination.as_ptr(), flags) == 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(all(unix, test))]
pub(in crate::search::semantic_manifest) fn replacement_path_entry_exists(
    path: &Path,
) -> std::io::Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(err) if matches!(err.kind(), std::io::ErrorKind::NotFound) => Ok(false),
        Err(err) => Err(std::io::Error::new(
            err.kind(),
            format!(
                "failed inspecting semantic manifest replacement target {}: {err}",
                path.display()
            ),
        )),
    }
}

#[cfg(not(windows))]
pub(in crate::search::semantic_manifest) fn sync_directory(path: &Path) -> std::io::Result<()> {
    let directory = fs::File::open(path)?;
    directory.sync_all()
}

#[cfg(windows)]
pub(in crate::search::semantic_manifest) fn sync_directory(_path: &Path) -> std::io::Result<()> {
    // Windows does not document FlushFileBuffers for directory handles.
    // Artifact files themselves are opened writable and flushed; every
    // manifest/current namespace transition goes through MoveFileExW with
    // MOVEFILE_WRITE_THROUGH in `persist_named_temp`.
    Ok(())
}

pub(in crate::search::semantic_manifest) fn sync_parent_directory(
    path: &Path,
) -> std::io::Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    sync_directory(parent)
}

// ─── Tests ─────────────────────────────────────────────────────────────────
