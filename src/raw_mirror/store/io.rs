use super::manifest::same_source_identity;
use super::types::{CachedRawMirrorBlobRecord, RawMirrorBlobSourceKey};
use anyhow::{Context, Result, anyhow};
use serde_json::{Value, json};
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub(in crate::raw_mirror) const RAW_MIRROR_SCHEMA_VERSION: u32 = 1;
pub(in crate::raw_mirror) const RAW_MIRROR_ROOT_DIR: &str = "raw-mirror";
pub(in crate::raw_mirror) const RAW_MIRROR_VERSION_DIR: &str = "v1";
pub(in crate::raw_mirror) const RAW_MIRROR_MANIFEST_KIND: &str = "cass_raw_session_mirror_v1";
pub(in crate::raw_mirror) const RAW_MIRROR_HASH_ALGORITHM: &str = "blake3";
pub(in crate::raw_mirror) const RAW_MIRROR_BLOB_EXTENSION: &str = "raw";
pub(in crate::raw_mirror) const RAW_MIRROR_MANIFEST_MAX_BYTES: u64 = 16 * 1024 * 1024;
pub(in crate::raw_mirror) const RAW_MIRROR_CHUNK_SIZE_BYTES: usize = 4 * 1024 * 1024;
pub(in crate::raw_mirror) const RAW_MIRROR_CHUNK_THRESHOLD_BYTES: u64 = 8 * 1024 * 1024;
pub(in crate::raw_mirror) const RAW_MIRROR_FIXED_CHUNKS_KIND: &str = "fixed_chunks_v1";
pub(in crate::raw_mirror) const RAW_MIRROR_BLOB_CACHE_MAX_ENTRIES: usize = 16_384;
pub(in crate::raw_mirror) const RAW_MIRROR_MUTATION_LOCK_FILE: &str = ".mutation.lock";

static TEMP_NONCE: AtomicU64 = AtomicU64::new(0);
pub(in crate::raw_mirror) static BLOB_CAPTURE_CACHE: OnceLock<
    Mutex<HashMap<RawMirrorBlobSourceKey, CachedRawMirrorBlobRecord>>,
> = OnceLock::new();

#[derive(Clone)]
struct RawMirrorIoObserver {
    on_progress: Arc<dyn Fn() + Send + Sync>,
    is_cancelled: Arc<dyn Fn() -> bool + Send + Sync>,
}

thread_local! {
    static RAW_MIRROR_IO_OBSERVER: RefCell<Option<RawMirrorIoObserver>> = const { RefCell::new(None) };
}

/// The observer belongs to the thread doing the I/O, including connector
/// producer threads. Dropping a nested scope restores the previous observer.
pub struct RawMirrorIoScope {
    previous: Option<RawMirrorIoObserver>,
    _thread_bound: std::marker::PhantomData<std::rc::Rc<()>>,
}

impl Drop for RawMirrorIoScope {
    fn drop(&mut self) {
        RAW_MIRROR_IO_OBSERVER.with(|observer| {
            *observer.borrow_mut() = self.previous.take();
        });
    }
}

pub fn observe_raw_mirror_io(
    on_progress: Arc<dyn Fn() + Send + Sync>,
    is_cancelled: Arc<dyn Fn() -> bool + Send + Sync>,
) -> RawMirrorIoScope {
    let previous = RAW_MIRROR_IO_OBSERVER.with(|observer| {
        observer.replace(Some(RawMirrorIoObserver {
            on_progress,
            is_cancelled,
        }))
    });
    RawMirrorIoScope {
        previous,
        _thread_bound: std::marker::PhantomData,
    }
}

struct RawMirrorActiveIo {
    operation: &'static str,
    path: PathBuf,
    started: Instant,
}

static RAW_MIRROR_IO_NONCE: AtomicU64 = AtomicU64::new(0);
static RAW_MIRROR_ACTIVE_IO: OnceLock<Mutex<BTreeMap<u64, RawMirrorActiveIo>>> = OnceLock::new();

struct RawMirrorActiveIoGuard(u64);

impl Drop for RawMirrorActiveIoGuard {
    fn drop(&mut self) {
        if let Some(active) = RAW_MIRROR_ACTIVE_IO.get() {
            active
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&self.0);
        }
    }
}

/// This snapshot performs no filesystem I/O and never waits for a filesystem
/// operation or its lock. In particular it can name an lstat that has not
/// returned on macOS, even in a stripped release binary (GH #538).
pub fn raw_mirror_io_snapshot() -> Value {
    let Some(active) = RAW_MIRROR_ACTIVE_IO.get() else {
        return json!([]);
    };
    let active = active
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    Value::Array(
        active
            .values()
            .map(|entry| {
                json!({
                    "operation": entry.operation,
                    "path": entry.path.to_string_lossy(),
                    "elapsed_ms": entry.started.elapsed().as_millis() as u64,
                })
            })
            .collect(),
    )
}

pub(in crate::raw_mirror) fn raw_mirror_checkpoint() -> std::io::Result<()> {
    let cancelled = RAW_MIRROR_IO_OBSERVER.with(|observer| {
        observer
            .borrow()
            .as_ref()
            .is_some_and(|observer| (observer.is_cancelled)())
    });
    if cancelled {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            "raw-mirror operation cancelled",
        ));
    }
    Ok(())
}

pub(in crate::raw_mirror) fn raw_mirror_record_progress() {
    let observer = RAW_MIRROR_IO_OBSERVER.with(|observer| observer.borrow().clone());
    if let Some(observer) = observer {
        (observer.on_progress)();
    }
}

pub(in crate::raw_mirror) fn raw_mirror_io<T>(
    operation: &'static str,
    path: &Path,
    perform: impl FnOnce() -> std::io::Result<T>,
) -> std::io::Result<T> {
    raw_mirror_checkpoint()?;
    raw_mirror_tracked_io(operation, path, perform)
}

/// Attribute an operation without introducing a cancellation boundary. In
/// particular, publication's durability steps and staging cleanup must still
/// finish when cancellation arrives after a link/rename. Cancellable work uses
/// `raw_mirror_io`; both paths report progress only after the operation returns.
pub(in crate::raw_mirror) fn raw_mirror_tracked_io<T>(
    operation: &'static str,
    path: &Path,
    perform: impl FnOnce() -> std::io::Result<T>,
) -> std::io::Result<T> {
    let observed = RAW_MIRROR_IO_OBSERVER.with(|observer| observer.borrow().is_some());
    // The before-operation trace is useful even outside an index run. No
    // timer manufactures liveness while a syscall or lock is still blocked.
    tracing::trace!(operation, path = %path.display(), "raw-mirror filesystem operation");
    let guard = observed.then(|| {
        let id = RAW_MIRROR_IO_NONCE.fetch_add(1, Ordering::Relaxed);
        RAW_MIRROR_ACTIVE_IO
            .get_or_init(|| Mutex::new(BTreeMap::new()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                id,
                RawMirrorActiveIo {
                    operation,
                    path: path.to_path_buf(),
                    started: Instant::now(),
                },
            );
        RawMirrorActiveIoGuard(id)
    });
    let result = perform();
    drop(guard);
    raw_mirror_record_progress();
    result
}

pub(in crate::raw_mirror) fn raw_mirror_symlink_metadata(
    path: &Path,
) -> std::io::Result<fs::Metadata> {
    raw_mirror_io("symlink_metadata", path, || fs::symlink_metadata(path))
}

#[derive(Debug)]
pub(in crate::raw_mirror) struct RawMirrorFileLockGuard {
    file: File,
}

// Locks here use std's `File` locking: it reports contention as
// `TryLockError::WouldBlock` on every platform, whereas fs2 surfaces Windows
// contention as a raw ERROR_LOCK_VIOLATION that no `ErrorKind` check matches
// (2l1b0.62). Both take the same flock/LockFileEx lock, so a lock held through
// fs2 by the indexer still excludes these.
impl Drop for RawMirrorFileLockGuard {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

fn raw_mirror_fsync_enabled() -> bool {
    dotenvy::var("CASS_RAW_MIRROR_FSYNC")
        .ok()
        .is_some_and(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
}

pub(in crate::raw_mirror) fn acquire_raw_mirror_mutation_lock(
    root: &Path,
) -> Result<RawMirrorFileLockGuard> {
    let lock_path = root.join(RAW_MIRROR_MUTATION_LOCK_FILE);
    if raw_mirror_path_has_symlink_below_root(root, &lock_path) {
        return Err(anyhow!(
            "raw mirror mutation lock path {} contains a symlink",
            lock_path.display()
        ));
    }
    match raw_mirror_symlink_metadata(&lock_path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err(anyhow!(
                "raw mirror mutation lock path {} is not a regular file",
                lock_path.display()
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error)
                .with_context(|| format!("stat raw mirror mutation lock {}", lock_path.display()));
        }
    }

    // bet45: first use of a mirror may acquire the mutation lock before the
    // root has ever been created; the root was just verified symlink-free
    // (or nonexistent) above, so creating it here is safe.
    raw_mirror_tracked_io("create_mutation_lock_parent", root, || {
        fs::create_dir_all(root)
    })
    .with_context(|| {
        format!(
            "create raw mirror root for mutation lock {}",
            root.display()
        )
    })?;

    let mut options = OpenOptions::new();
    options.create(true).truncate(false).read(true).write(true);
    configure_lock_open_options(&mut options);
    let file = raw_mirror_io("open_mutation_lock", &lock_path, || {
        options.open(&lock_path)
    })
    .with_context(|| format!("open raw mirror mutation lock {}", lock_path.display()))?;
    raw_mirror_io("acquire_mutation_lock", &lock_path, || {
        loop {
            raw_mirror_checkpoint()?;
            match file.try_lock() {
                Ok(()) => break Ok(()),
                Err(std::fs::TryLockError::WouldBlock) => {
                    // Waiting is not forward progress. Keep the operation visible
                    // to diagnostics and observe cancellation between attempts.
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(std::fs::TryLockError::Error(error)) => break Err(error),
            }
        }
    })
    .with_context(|| format!("acquire raw mirror mutation lock {}", lock_path.display()))?;

    let opened_metadata =
        raw_mirror_tracked_io("fstat_mutation_lock", &lock_path, || file.metadata()).with_context(
            || {
                format!(
                    "stat opened raw mirror mutation lock {}",
                    lock_path.display()
                )
            },
        )?;
    let path_metadata = raw_mirror_symlink_metadata(&lock_path)
        .with_context(|| format!("restat raw mirror mutation lock {}", lock_path.display()))?;
    if path_metadata.file_type().is_symlink()
        || !path_metadata.is_file()
        || !same_source_identity(&opened_metadata, &path_metadata)
    {
        let _ = file.unlock();
        return Err(anyhow!(
            "raw mirror mutation lock {} changed identity while being acquired",
            lock_path.display()
        ));
    }
    set_private_open_file_permissions(&file, &lock_path)?;

    Ok(RawMirrorFileLockGuard { file })
}

pub(in crate::raw_mirror) fn try_acquire_index_run_lock_for_prune(
    data_dir: &Path,
) -> Result<RawMirrorFileLockGuard> {
    let lock_path = data_dir.join("index-run.lock");
    match raw_mirror_symlink_metadata(&lock_path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err(anyhow!(
                "refusing raw mirror prune because index-run lock {} is not a regular file",
                lock_path.display()
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error)
                .with_context(|| format!("stat index-run lock {}", lock_path.display()));
        }
    }
    let mut options = OpenOptions::new();
    options.create(true).truncate(false).read(true).write(true);
    configure_lock_open_options(&mut options);
    let file = raw_mirror_io("open_index_run_lock_for_prune", &lock_path, || {
        options.open(&lock_path)
    })
    .with_context(|| format!("open index-run lock {}", lock_path.display()))?;
    match file.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => {
            return Err(anyhow!(
                "refusing raw mirror prune because an index run acquired {}",
                lock_path.display()
            ));
        }
        Err(std::fs::TryLockError::Error(error)) => {
            return Err(error)
                .with_context(|| format!("acquire index-run lock {}", lock_path.display()));
        }
    }
    let opened_metadata =
        raw_mirror_tracked_io("fstat_index_run_lock_for_prune", &lock_path, || {
            file.metadata()
        })
        .with_context(|| format!("stat opened index-run lock {}", lock_path.display()))?;
    let path_metadata = raw_mirror_symlink_metadata(&lock_path)
        .with_context(|| format!("restat index-run lock {}", lock_path.display()))?;
    if path_metadata.file_type().is_symlink()
        || !path_metadata.is_file()
        || !same_source_identity(&opened_metadata, &path_metadata)
    {
        let _ = file.unlock();
        return Err(anyhow!(
            "refusing raw mirror prune because index-run lock {} changed identity while being acquired",
            lock_path.display()
        ));
    }
    Ok(RawMirrorFileLockGuard { file })
}

pub(in crate::raw_mirror) fn configure_lock_open_options(options: &mut OpenOptions) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    #[cfg(not(unix))]
    let _ = options;
}

#[cfg(unix)]
pub(in crate::raw_mirror) fn set_private_open_file_permissions(
    file: &File,
    path: &Path,
) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    if raw_mirror_tracked_io("fstat_permissions", path, || file.metadata())?
        .permissions()
        .mode()
        & 0o7777
        == 0o600
    {
        return Ok(());
    }
    raw_mirror_tracked_io("set_file_permissions", path, || {
        file.set_permissions(fs::Permissions::from_mode(0o600))
    })
    .with_context(|| format!("set raw mirror lock permissions {}", path.display()))
}

#[cfg(not(unix))]
pub(in crate::raw_mirror) fn set_private_open_file_permissions(
    _file: &File,
    _path: &Path,
) -> Result<()> {
    Ok(())
}

pub(in crate::raw_mirror) fn raw_mirror_root(data_dir: &Path) -> PathBuf {
    data_dir
        .join(RAW_MIRROR_ROOT_DIR)
        .join(RAW_MIRROR_VERSION_DIR)
}

pub(in crate::raw_mirror) fn ensure_raw_mirror_root(data_dir: &Path) -> Result<PathBuf> {
    let root_parent = data_dir.join(RAW_MIRROR_ROOT_DIR);
    ensure_private_dir(&root_parent)?;
    let root = root_parent.join(RAW_MIRROR_VERSION_DIR);
    ensure_private_dir(&root)?;
    Ok(root)
}

pub(in crate::raw_mirror) fn raw_mirror_path_has_symlink_below_root(
    root: &Path,
    path: &Path,
) -> bool {
    let Ok(relative) = path.strip_prefix(root) else {
        return true;
    };
    // bet45: a NONEXISTENT root is not a symlink — first-use callers (e.g.
    // acquiring the mutation lock before the mirror root has ever been
    // created) must not be refused with a misleading symlink error. This
    // mirrors the component walk below, where NotFound => false. Any other
    // metadata error stays fail-closed.
    match raw_mirror_symlink_metadata(root) {
        Ok(metadata) if metadata.file_type().is_symlink() => return true,
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return false,
        Err(_) => return true,
    }
    let mut current = root.to_path_buf();
    for component in relative.components() {
        let Component::Normal(part) = component else {
            return true;
        };
        current.push(part);
        match raw_mirror_symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => return true,
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return false,
            Err(_) => return true,
        }
    }
    false
}

pub(in crate::raw_mirror) fn file_blake3(path: &Path) -> Result<String> {
    let expected_metadata = raw_mirror_symlink_metadata(path)
        .with_context(|| format!("stat {} before hashing", path.display()))?;
    if expected_metadata.file_type().is_symlink() || !expected_metadata.is_file() {
        return Err(anyhow!(
            "refusing to hash non-regular raw mirror file {}",
            path.display()
        ));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    configure_lock_open_options(&mut options);
    let mut file = raw_mirror_io("open_blob", path, || options.open(path))
        .with_context(|| format!("open {}", path.display()))?;
    let opened_metadata = raw_mirror_tracked_io("fstat_blob", path, || file.metadata())
        .with_context(|| format!("stat opened raw mirror file {}", path.display()))?;
    let current_path_metadata = raw_mirror_symlink_metadata(path)
        .with_context(|| format!("restat raw mirror file {}", path.display()))?;
    if current_path_metadata.file_type().is_symlink()
        || !same_source_identity(&expected_metadata, &opened_metadata)
        || !same_source_identity(&opened_metadata, &current_path_metadata)
    {
        return Err(anyhow!(
            "raw mirror file {} changed identity while being opened for hashing",
            path.display()
        ));
    }
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = raw_mirror_io("hash_blob_read", path, || file.read(&mut buffer))
            .with_context(|| format!("read {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

pub(in crate::raw_mirror) fn ensure_private_dir(path: &Path) -> Result<()> {
    raw_mirror_io("create_dir", path, || create_private_dir_all(path))
        .with_context(|| format!("create raw mirror dir {}", path.display()))?;
    let metadata = raw_mirror_symlink_metadata(path)
        .with_context(|| format!("stat raw mirror dir {}", path.display()))?;
    let file_type = metadata.file_type();
    if file_type.is_symlink() {
        return Err(anyhow!(
            "refusing to use symlink raw mirror dir {}",
            path.display()
        ));
    }
    if !file_type.is_dir() {
        return Err(anyhow!(
            "refusing to use non-directory raw mirror path {}",
            path.display()
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o777 != 0o700 {
            set_private_dir_permissions(path)?;
        }
    }
    #[cfg(not(unix))]
    {
        set_private_dir_permissions(path)?;
    }
    Ok(())
}

pub(in crate::raw_mirror) fn ensure_private_dir_descendant(root: &Path, path: &Path) -> Result<()> {
    let relative = path.strip_prefix(root).with_context(|| {
        format!(
            "raw mirror private dir {} is not under root {}",
            path.display(),
            root.display()
        )
    })?;

    if let Some(root_parent) = root.parent() {
        ensure_private_dir(root_parent)?;
    }
    ensure_private_dir(root)?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        match component {
            Component::Normal(part) => {
                current.push(part);
                ensure_private_dir(&current)?;
            }
            Component::CurDir => {}
            _ => {
                return Err(anyhow!(
                    "raw mirror private dir contains non-normal component: {}",
                    path.display()
                ));
            }
        }
    }

    Ok(())
}

pub(in crate::raw_mirror) fn private_create_new_file(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    set_private_create_file_mode(&mut options);
    let file = raw_mirror_tracked_io("create_file", path, || options.open(path))
        .with_context(|| format!("create raw mirror file {}", path.display()))?;
    Ok(file)
}

#[cfg(unix)]
fn create_private_dir_all(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;

    let mut builder = fs::DirBuilder::new();
    builder.recursive(true).mode(0o700).create(path)
}

#[cfg(not(unix))]
fn create_private_dir_all(path: &Path) -> std::io::Result<()> {
    fs::create_dir_all(path)
}

#[cfg(unix)]
fn set_private_create_file_mode(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;

    options.mode(0o600);
}

#[cfg(not(unix))]
fn set_private_create_file_mode(_options: &mut OpenOptions) {}

pub(in crate::raw_mirror) fn sync_open_file_if_required(
    message_file: &File,
    path: &Path,
    context: impl FnOnce() -> String,
) -> Result<()> {
    if !raw_mirror_fsync_enabled() {
        return Ok(());
    }
    raw_mirror_tracked_io("sync_file", path, || message_file.sync_all()).with_context(context)
}

pub(in crate::raw_mirror) fn sync_file(path: &Path) -> Result<()> {
    if !raw_mirror_fsync_enabled() {
        return Ok(());
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(windows)]
    options.write(true);
    let file = raw_mirror_tracked_io("open_file_for_sync", path, || options.open(path))
        .with_context(|| format!("open raw mirror file {} for sync", path.display()))?;
    raw_mirror_tracked_io("sync_file", path, || file.sync_all())
        .with_context(|| format!("sync raw mirror file {}", path.display()))
}

pub(in crate::raw_mirror) fn sync_parent(path: &Path) -> Result<()> {
    if !raw_mirror_fsync_enabled() {
        return Ok(());
    }
    sync_parent_unconditionally(path)
}

#[cfg(not(windows))]
pub(in crate::raw_mirror) fn sync_parent_unconditionally(path: &Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    let directory = raw_mirror_tracked_io("open_parent_for_sync", parent, || File::open(parent))
        .with_context(|| format!("open raw mirror parent {} for sync", parent.display()))?;
    raw_mirror_tracked_io("sync_parent", parent, || directory.sync_all())
        .with_context(|| format!("sync raw mirror parent {}", parent.display()))
}

#[cfg(windows)]
pub(in crate::raw_mirror) fn sync_parent_unconditionally(_path: &Path) -> Result<()> {
    Ok(())
}

pub(in crate::raw_mirror) fn unique_temp_path(dir: &Path, label: &str) -> PathBuf {
    let nonce = TEMP_NONCE.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    dir.join(format!(
        ".{label}.{}.{}.{}.tmp",
        std::process::id(),
        nanos,
        nonce
    ))
}

pub(in crate::raw_mirror) fn unique_capture_temp_dir(root: &Path) -> PathBuf {
    let nonce = TEMP_NONCE.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    root.join("tmp").join(format!(
        "capture.{}.{}.{}",
        std::process::id(),
        nanos,
        nonce
    ))
}

pub(in crate::raw_mirror) fn remove_temp_best_effort(path: &Path) {
    if let Err(err) = raw_mirror_tracked_io("remove_temp_file", path, || fs::remove_file(path)) {
        tracing::debug!(
            path = %path.display(),
            error = %err,
            "failed to remove raw mirror temp file"
        );
    }
}

pub(in crate::raw_mirror) fn remove_empty_temp_dir_best_effort(path: &Path) {
    if let Err(err) = raw_mirror_tracked_io("remove_temp_dir", path, || fs::remove_dir(path)) {
        tracing::debug!(
            path = %path.display(),
            error = %err,
            "failed to remove raw mirror temp directory"
        );
    }
}

pub(in crate::raw_mirror) fn redacted_original_path(provider: &str, source_path: &Path) -> String {
    let file_name = source_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("session");
    format!("[{provider}]/{file_name}")
}

pub(in crate::raw_mirror) fn now_ms() -> i64 {
    system_time_to_ms(SystemTime::now()).unwrap_or(0)
}

pub(in crate::raw_mirror) fn system_time_to_ms(time: SystemTime) -> Option<i64> {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
}

pub(in crate::raw_mirror) fn system_time_to_ns(time: SystemTime) -> Option<u128> {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_nanos())
}

#[cfg(unix)]
fn set_private_dir_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    raw_mirror_tracked_io("set_dir_permissions", path, || {
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
    })
    .with_context(|| format!("set raw mirror dir permissions {}", path.display()))
}

#[cfg(not(unix))]
fn set_private_dir_permissions(_path: &Path) -> Result<()> {
    Ok(())
}
