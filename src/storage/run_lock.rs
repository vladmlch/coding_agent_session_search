use std::fs::{self, File, OpenOptions};
use std::io::{Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use anyhow::{Context, Result};

use super::archive_lease;
use super::lexical_checkpoint_io::LexicalRebuildDbState;
use crate::search_defaults::{SearchMaintenanceJobKind, SearchMaintenanceMode};
use crate::storage::sqlite::FrankenStorage;

pub fn index_run_lock_path(data_dir: &Path) -> PathBuf {
    data_dir.join("index-run.lock")
}

pub fn index_run_lock_metadata_sidecar_path(lock_path: &Path) -> PathBuf {
    lock_path.with_file_name("index-run.lock.meta")
}

pub fn write_index_run_lock_metadata_sidecar(lock_path: &Path, contents: &str) -> Result<()> {
    let sidecar_path = index_run_lock_metadata_sidecar_path(lock_path);
    let mut sidecar = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&sidecar_path)
        .with_context(|| {
            format!(
                "opening index-run lock metadata sidecar {}",
                sidecar_path.display()
            )
        })?;
    sidecar.write_all(contents.as_bytes()).with_context(|| {
        format!(
            "writing index-run lock metadata sidecar {}",
            sidecar_path.display()
        )
    })?;
    sidecar.flush().with_context(|| {
        format!(
            "flushing index-run lock metadata sidecar {}",
            sidecar_path.display()
        )
    })?;
    sidecar.sync_all().with_context(|| {
        format!(
            "syncing index-run lock metadata sidecar {}",
            sidecar_path.display()
        )
    })
}

pub fn clear_index_run_lock_metadata_sidecar(lock_path: &Path) -> Result<()> {
    let sidecar_path = index_run_lock_metadata_sidecar_path(lock_path);
    let sidecar = match OpenOptions::new()
        .truncate(true)
        .write(true)
        .open(&sidecar_path)
    {
        Ok(sidecar) => sidecar,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => {
            return Err(err).with_context(|| {
                format!(
                    "opening index-run lock metadata sidecar {}",
                    sidecar_path.display()
                )
            });
        }
    };
    sidecar.sync_all().with_context(|| {
        format!(
            "syncing cleared index-run lock metadata sidecar {}",
            sidecar_path.display()
        )
    })
}

#[cfg(windows)]
pub fn windows_lock_conflict(err: &std::io::Error) -> bool {
    matches!(err.raw_os_error(), Some(32 | 33))
}

#[cfg(not(windows))]
pub fn windows_lock_conflict(_err: &std::io::Error) -> bool {
    false
}

/// 3: generations built under the per-message content cap (bgn6s). A v2
/// generation left every message past a long conversation's first 8 MiB
/// unindexed; the version change stops it certifying, so it is rebuilt once.
pub(crate) const LEXICAL_REBUILD_STATE_VERSION: u8 = 3;
// Size of each SQL page fetched from the conversations table during a lexical
// rebuild. The page is a primary-key-ordered LIMIT window materialized as
// lightweight `LexicalRebuildConversationRow` records (~1 KB each — no message
// bodies at this stage). Inside the page we process sub-chunks of
// `batch_fetch_conversation_limit` conversations at a time, each of which
// triggers one batched message fetch (capped by
// `CASS_TANTIVY_REBUILD_BATCH_FETCH_MESSAGE_BYTES`) and feeds the Tantivy
// writer. Larger page sizes mean:
//   - Fewer SQL round-trips without reintroducing eager full-table
//     materialization.
//   - Peak memory for the page itself is modest: 1024 × ~1 KB ≈ 1 MB. The
//     real memory ceiling is the per-chunk batched-message-fetch budget
//     (`BATCH_FETCH_MESSAGE_BYTES`, default 128 MB), not the page window.
//   - SQL parameter limit: well below SQLite's 32k-param IN clause cap.
// Changing this constant invalidates existing `.lexical-rebuild-state.json`
// checkpoints (via `LexicalRebuildState::matches_run`) and restarts any
// in-flight rebuild from offset 0. That's acceptable given the 5–10×
// throughput gain this batch-size change unlocks.
pub(crate) const LEXICAL_REBUILD_PAGE_SIZE: i64 = 1024;
pub(crate) const LEXICAL_REBUILD_LEGACY_COMPAT_PAGE_SIZE: i64 = 200;
pub(crate) const LEXICAL_REBUILD_PAGE_SIZE_PUBLIC: i64 = LEXICAL_REBUILD_PAGE_SIZE;

pub(crate) fn lexical_rebuild_page_size_is_compatible(page_size: i64) -> bool {
    page_size == LEXICAL_REBUILD_PAGE_SIZE || page_size == LEXICAL_REBUILD_LEGACY_COMPAT_PAGE_SIZE
}

pub(crate) fn lexical_rebuild_db_paths_match(saved: &str, current: &str) -> bool {
    crate::model::paths::path_identities_match(Path::new(saved), Path::new(current))
}

pub(crate) fn lexical_rebuild_db_state_matches(
    saved: &LexicalRebuildDbState,
    current: &LexicalRebuildDbState,
) -> bool {
    saved.total_conversations == current.total_conversations
        && saved.storage_fingerprint == current.storage_fingerprint
        && lexical_rebuild_db_paths_match(&saved.db_path, &current.db_path)
}

pub(crate) fn lexical_rebuild_db_state_matches_legacy(
    saved: &LexicalRebuildDbState,
    current: &LexicalRebuildDbState,
) -> bool {
    let messages_match = saved.total_messages == 0
        || current.total_messages == 0
        || saved.total_messages == current.total_messages;
    saved.total_conversations == current.total_conversations
        && messages_match
        && lexical_rebuild_db_paths_match(&saved.db_path, &current.db_path)
}

/// Contention on the canonical maintenance lock is an admission outcome, not
/// an embedding or storage failure. Keep it typed so the daemon can retain an
/// accepted job for retry without interpreting an arbitrary error message.
#[derive(Debug, thiserror::Error)]
#[error("another cass index process already holds {}", lock_path.display())]
pub struct IndexRunBusy {
    pub(crate) lock_path: PathBuf,
}

pub(crate) fn acquire_archive_write_lease(
    db_path: &Path,
) -> Result<archive_lease::ArchiveWriteLease> {
    match archive_lease::ArchiveWriteLease::try_acquire(db_path).with_context(|| {
        format!(
            "acquiring canonical archive writer lease for {}",
            db_path.display()
        )
    })? {
        archive_lease::ArchiveLeaseAttempt::Acquired(lease) => Ok(lease),
        archive_lease::ArchiveLeaseAttempt::Busy(lock_path) => {
            Err(IndexRunBusy { lock_path }.into())
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum ArchiveAdmission {
    Writable,
    DataDirOnly,
}

pub(crate) fn acquire_index_run_lock_with_job_kind(
    data_dir: &Path,
    db_path: &Path,
    mode: SearchMaintenanceMode,
    job_kind: SearchMaintenanceJobKind,
) -> Result<IndexRunLockGuard> {
    acquire_index_run_lock_with_archive_admission(
        data_dir,
        db_path,
        mode,
        job_kind,
        ArchiveAdmission::Writable,
    )
}

pub(crate) fn acquire_index_run_lock_with_archive_admission(
    data_dir: &Path,
    db_path: &Path,
    mode: SearchMaintenanceMode,
    job_kind: SearchMaintenanceJobKind,
    archive_admission: ArchiveAdmission,
) -> Result<IndexRunLockGuard> {
    fs::create_dir_all(data_dir)
        .with_context(|| format!("creating cass data directory {}", data_dir.display()))?;
    let lock_path = data_dir.join("index-run.lock");
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("opening index-run lock file {}", lock_path.display()))?;

    // std's File::try_lock reports contention as WouldBlock on every platform;
    // fs2 surfaced Windows contention as raw ERROR_LOCK_VIOLATION, so a second
    // `cass index` there got a generic error instead of the index-busy (exit 7)
    // contract that error_chain_indicates_active_cass_index keys on
    // (2l1b0.74). Same flock/LockFileEx lock, so fs2 holders still exclude it.
    match file.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => {
            return Err(IndexRunBusy { lock_path }.into());
        }
        Err(std::fs::TryLockError::Error(err)) => {
            return Err(err)
                .with_context(|| format!("acquiring index-run lock {}", lock_path.display()));
        }
    }

    let archive_path = if db_path.is_absolute() {
        db_path.to_path_buf()
    } else {
        std::env::current_dir()
            .context("resolving the archive writer path")?
            .join(db_path)
    };
    // Do not publish this attempt as an active owner until both leases are
    // held. If archive admission fails, dropping `file` releases the data-dir
    // lock without changing either owner's metadata or opening the database.
    let archive_write_lease = match archive_admission {
        ArchiveAdmission::Writable => Some(acquire_archive_write_lease(&archive_path)?),
        ArchiveAdmission::DataDirOnly => None,
    };
    let db_path = archive_write_lease.as_ref().map_or_else(
        || {
            archive_lease::canonical_database_path(&archive_path)
                .unwrap_or_else(|_| archive_path.clone())
        },
        |lease| lease.database_path().to_path_buf(),
    );
    let now_ms = FrankenStorage::now_millis();
    let mut guard = IndexRunLockGuard {
        file,
        archive_write_lease,
        _path: lock_path,
        started_at_ms: now_ms,
        updated_at_ms: now_ms,
        // cass#258: seed with `now` so the very first `read_search_maintenance_snapshot`
        // call after `acquire_index_run_lock` sees a populated cursor.
        // Forward progress will refresh this on every `write_metadata`
        // / `set_mode` call and every per-batch atomic progress bump (F4).
        last_progress_at_ms: now_ms,
        // F4: indexer-thread mailbox the heartbeat folds into the on-disk
        // payload. Seed with `now_ms` so a stall-detector that fires
        // before the first bump cannot false-positive on `0`.
        last_progress_at_ms_atomic: Arc::new(AtomicI64::new(now_ms)),
        db_path,
        archive_path,
        job_id: String::new(),
        job_kind,
        metadata_write_lock: Arc::new(Mutex::new(())),
    };
    guard.job_id = format!(
        "{}-{}-{}",
        guard.job_kind.as_lock_value(),
        guard.started_at_ms,
        std::process::id()
    );
    guard.write_metadata(mode)?;
    Ok(guard)
}

#[derive(Debug)]
pub(crate) struct IndexRunLockGuard {
    // Keep the file handle alive for the lifetime of the lock.
    pub(crate) file: File,
    // The data-dir lock protects derived assets; this separate pathname lease
    // excludes writers using the same archive with another --data-dir. It is
    // declared last among lock handles so data-dir metadata is cleared first.
    pub(crate) archive_write_lease: Option<archive_lease::ArchiveWriteLease>,
    pub(crate) _path: PathBuf,
    pub(crate) started_at_ms: i64,
    pub(crate) updated_at_ms: i64,
    /// Forward-progress timestamp owned exclusively by the indexing
    /// thread (cass#258). The background `IndexRunLockHeartbeat` thread
    /// refreshes `updated_at_ms` to prove the process is alive, but it
    /// MUST NOT touch `last_progress_at_ms` — only real mode/phase
    /// transitions (i.e. `write_metadata` / `set_mode`) and explicit
    /// per-batch atomic progress bumps advance it. Readers
    /// (`asset_state::maintenance_stall_age_ms`) compare this to
    /// wall-clock to flip `status: "stalled"` when the indexer wedges
    /// while the heartbeat keeps refreshing.
    pub(crate) last_progress_at_ms: i64,
    /// F4 (cass tech debt): atomic mirror of `last_progress_at_ms` that
    /// the indexer's per-batch / per-conversation / per-shard-flush hot
    /// loops can update with a single `Relaxed` store — no I/O, no
    /// POSIX-advisory-lock contention with the heartbeat thread. The
    /// heartbeat thread (which is already paying the I/O cost every ~1 s
    /// to refresh `updated_at_ms`) consults this atomic and folds the
    /// most-recent bump into the same on-disk rewrite. This preserves
    /// the cass#258 ownership invariant — the indexer thread is the
    /// sole writer of forward progress — while making the indexer
    /// itself trivially cheap to instrument. A `0` value means
    /// "indexer has not yet posted a per-batch progress bump"; the
    /// heartbeat falls back to leaving `last_progress_at_ms` unchanged
    /// in that case.
    pub(crate) last_progress_at_ms_atomic: Arc<AtomicI64>,
    pub(crate) db_path: PathBuf,
    // Keep the original filesystem spelling for delayed writer admission.
    // Lexically normalizing `..` before resolving symlinks changes identity.
    pub(crate) archive_path: PathBuf,
    pub(crate) job_id: String,
    pub(crate) job_kind: SearchMaintenanceJobKind,
    pub(crate) metadata_write_lock: Arc<Mutex<()>>,
}

impl Drop for IndexRunLockGuard {
    fn drop(&mut self) {
        let _ = self.file.set_len(0);
        let _ = self.file.rewind();
        let _ = self.file.flush();
        let _ = clear_index_run_lock_metadata_sidecar(&self._path);
        let _ = self.file.unlock();
    }
}

impl IndexRunLockGuard {
    pub(crate) fn protects_data_dir(&self, data_dir: &Path) -> bool {
        self._path == data_dir.join("index-run.lock")
    }

    pub(crate) fn acquire_archive_write_lease(&mut self) -> Result<()> {
        if self.archive_write_lease.is_none() {
            self.archive_write_lease = Some(acquire_archive_write_lease(&self.archive_path)?);
        }
        Ok(())
    }

    pub(crate) fn write_metadata(&mut self, mode: SearchMaintenanceMode) -> Result<()> {
        self.write_metadata_with_phase(mode, mode.as_lock_value())
    }

    /// Write the full lock metadata with a caller-supplied sub-phase
    /// string. The on-disk `mode=` field tracks the user-facing
    /// `SearchMaintenanceMode` (`watch_startup`, `watch`, `index`,
    /// `watch_once`, …), while the `phase=` field carries a finer-
    /// grained breadcrumb (e.g. `watch_startup:fingerprint`,
    /// `watch_startup:fts_validate`) so cass#265 operators can see
    /// which preflight step wedged on a multi-GB DB.
    ///
    /// Phase breadcrumbs MUST satisfy two invariants so the existing
    /// `IndexStallWatchdog` keeps resetting its `last_progress_advance`
    /// timer at each sub-phase transition:
    ///   1. Each call produces a strictly different `phase=` string
    ///      than the previous call (the watchdog gates on
    ///      `phase_code != self.last_phase` to reset).
    ///   2. The free function `bump_index_run_lock_progress_atomic`
    ///      is also called at each transition so the heartbeat
    ///      thread folds a fresh `last_progress_at_ms=` into the
    ///      on-disk payload even if the watchdog tick is rare.
    pub(crate) fn write_metadata_with_phase(
        &mut self,
        mode: SearchMaintenanceMode,
        phase: &str,
    ) -> Result<()> {
        let _write_guard = self
            .metadata_write_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("index-run metadata write lock poisoned"))?;
        let now_ms = FrankenStorage::now_millis();
        self.updated_at_ms = now_ms;
        // cass#258: every `write_metadata` / `set_mode` / `set_phase`
        // call is, by definition, the indexing thread reporting a real
        // mode/phase transition — i.e. forward progress. Bump
        // `last_progress_at_ms` here, while the background heartbeat
        // thread refreshes `updated_at_ms` independently but preserves
        // this field verbatim so a wedged indexer flips
        // `status: "stalled"` instead of silently staying "building".
        self.last_progress_at_ms = now_ms;
        self.last_progress_at_ms_atomic
            .store(now_ms, Ordering::Relaxed);
        self.file.set_len(0).with_context(|| {
            format!(
                "truncating index-run lock file before metadata update: {}",
                self._path.display()
            )
        })?;
        self.file.rewind().with_context(|| {
            format!(
                "rewinding index-run lock file after truncation: {}",
                self._path.display()
            )
        })?;
        let metadata = format!(
            "pid={}\nstarted_at_ms={}\nupdated_at_ms={}\nlast_progress_at_ms={}\ndb_path={}\nmode={}\njob_id={}\njob_kind={}\nphase={}",
            std::process::id(),
            self.started_at_ms,
            self.updated_at_ms,
            self.last_progress_at_ms,
            self.db_path.display(),
            mode.as_lock_value(),
            self.job_id,
            self.job_kind.as_lock_value(),
            phase
        );
        writeln!(self.file, "{metadata}")
            .with_context(|| format!("writing index-run metadata to {}", self._path.display()))?;
        self.file
            .flush()
            .with_context(|| format!("flushing index-run lock file {}", self._path.display()))?;
        self.file
            .sync_all()
            .with_context(|| format!("syncing index-run lock file {}", self._path.display()))?;
        write_index_run_lock_metadata_sidecar(&self._path, &format!("{metadata}\n"))?;
        Ok(())
    }

    pub(crate) fn set_mode(&mut self, mode: SearchMaintenanceMode) -> Result<()> {
        self.write_metadata(mode)
    }

    /// Update only the `phase=` sub-phase breadcrumb without changing
    /// the `mode=`. Use this at the entry of each index preflight step so
    /// cass#265/GH#413 operators can pinpoint which step wedged — the
    /// previous v0.6.5 lock-file payload only reported `phase=watch_startup`,
    /// which hid the wedge inside a multi-second-to-multi-hour preflight
    /// block.
    pub(crate) fn set_phase(&mut self, mode: SearchMaintenanceMode, phase: &str) -> Result<()> {
        self.write_metadata_with_phase(mode, phase)
    }
}

/// cass#265/GH#413: index preflight per-op skip + timeout configuration.
///
/// The v0.6.6 fix `fad3f03d` shipped sub-phase breadcrumbs so operators
/// could pinpoint which preflight step wedged, but it did NOT add a
/// failure mode: a wedged step still hangs forever. The reporter on
/// cass#265 confirmed `phase=watch_startup:cleanup_orphan_fk_rows` on
/// v0.6.6 against a 3.25 GB / 7,566-conversation / 417,234-message DB
/// — i.e. the `ORDER BY conversation_id ASC LIMIT 1` query in
/// `collect_orphan_message_ids` descends the `messages` B-tree without
/// a usable index and wedges in SQLite.
///
/// This module turns the diagnostic infrastructure into a *self-
/// debugging surface*. The hard watchdog applies to watch startup. Plain
/// index can emit the cheap checkpoint-classification breadcrumb, but its
/// count-free restart does not inherit a preflight process-abort policy:
/// 1. Per-op atomic watchdog: every timeout-enforced `preflight_phase!(...)` enter
///    records `step_started_at_ms`. A dedicated watchdog thread
///    polls it every 5 s; if a step exceeds `CASS_PREFLIGHT_OP_TIMEOUT_SECS`
///    (default 180 s), the watchdog rewrites the lock file's
///    `phase=` to `<step>_TIMEOUT`, logs a structured diagnostic, and
///    aborts the process with `EX_SOFTWARE (70)` so operators get an
///    immediate, named failure instead of staring at a stalled health
///    JSON for hours.
/// 2. Per-op skip env vars: `CASS_SKIP_PREFLIGHT_<NAME>=1` lets an
///    operator bypass a single wedged step (e.g.
///    `CASS_SKIP_PREFLIGHT_CLEANUP_ORPHAN_FK_ROWS=1`) so they can
///    actually use cass while the underlying storage/cass bug is
///    being root-caused.
/// 3. Per-op completion bump: every bounded preflight step calls the
///    completion macro after it returns, disarming the per-op watchdog
///    and bumping the shared progress timestamp so the heartbeat can
///    fold completion progress into the lock metadata.
pub(crate) mod watch_startup_preflight {
    use anyhow::Context;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, Ordering};
    use std::thread::{self, JoinHandle};
    use std::time::Duration;

    /// Default per-op timeout (seconds). Chosen so a single preflight
    /// step has 60 s of headroom over the F4 `CASS_INDEX_STALL_DETECT_SECS`
    /// default (120 s). Operators investigating a wedge see the F4
    /// `stall_detected` event first (so logs still flag the wedge at
    /// 120 s), and the harder per-op kill at 180 s prevents the
    /// indefinite hang the reporter on cass#265 observed.
    pub(crate) const DEFAULT_TIMEOUT_SECS: u64 = 180;

    /// Sentinel meaning "no preflight step is currently active".
    /// Used so the watchdog can skip its check between steps (e.g.
    /// while a `preflight_phase!` block is between active step
    /// boundaries).
    pub(crate) const NO_STEP: u8 = u8::MAX;

    /// Read the per-op timeout once. We resolve the env var lazily on
    /// `WatchStartupPreflightWatchdog::start` so test harnesses that
    /// mutate the env (`set_var`) see the latest value, while
    /// production code pays one `dotenvy::var` lookup per `run_index`
    /// invocation.
    pub(crate) fn op_timeout() -> Duration {
        let secs = dotenvy::var("CASS_PREFLIGHT_OP_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_TIMEOUT_SECS);
        // Hard floor at 1 s so test harnesses can deliberately trip
        // the watchdog inside one second. Hard ceiling at 1 hour so
        // a wedged production process can't outlive any reasonable
        // operator-attention window.
        Duration::from_secs(secs.clamp(1, 3600))
    }

    /// Convert a sub-phase string (e.g. `"watch_startup:cleanup_orphan_fk_rows"`)
    /// to the env-var name fragment (`CLEANUP_ORPHAN_FK_ROWS`).
    ///
    /// The mapping is intentionally lossless: an operator who sees
    /// `phase=watch_startup:foo_bar` in the lock file knows exactly
    /// which env var to set (`CASS_SKIP_PREFLIGHT_FOO_BAR=1`).
    pub(crate) fn skip_env_name(sub_phase: &str) -> String {
        let bare = sub_phase
            .strip_prefix("watch_startup:")
            .unwrap_or(sub_phase);
        format!("CASS_SKIP_PREFLIGHT_{}", bare.to_uppercase())
    }

    /// Returns true if the operator has opted to skip this preflight
    /// step. Logs at WARN so the bypass is auditable.
    pub(crate) fn should_skip(sub_phase: &str) -> bool {
        let var = skip_env_name(sub_phase);
        let raw = dotenvy::var(&var).ok();
        let skip = raw
            .as_deref()
            .is_some_and(|v| matches!(v.trim(), "1" | "true" | "TRUE" | "yes" | "YES"));
        if skip {
            tracing::warn!(
                target: "cass::preflight_skip",
                sub_phase,
                env_var = %var,
                "cass#265: operator-requested preflight skip — bypassing this watch_startup step"
            );
        }
        skip
    }

    /// Shared state the watchdog thread reads on every tick. Held by
    /// `Arc` so the macro can clone references into nested scopes
    /// without threading the `&mut IndexRunLockGuard` through every
    /// preflight call site.
    pub(crate) struct WatchStartupPreflightState {
        /// Index into the documented sub-phase taxonomy, or `NO_STEP`
        /// when no step is currently active. `AtomicU8` because the
        /// taxonomy is < 256 entries.
        pub(crate) current_step_idx: AtomicU8,
        /// `FrankenStorage::now_millis()` of the most recent
        /// `preflight_phase!` enter for `current_step_idx`. The
        /// watchdog computes `now - step_started_at_ms` and fires if
        /// it exceeds `op_timeout`.
        pub(crate) step_started_at_ms: AtomicI64,
        /// Set to true by the watchdog when it fires, so the main
        /// thread (if it ever unwedges) can't race a successful
        /// completion ahead of the abort, and so the regression test
        /// can observe the trip without exiting the process.
        pub(crate) tripped: AtomicBool,
    }

    impl WatchStartupPreflightState {
        pub(crate) fn new() -> Arc<Self> {
            Arc::new(Self {
                current_step_idx: AtomicU8::new(NO_STEP),
                step_started_at_ms: AtomicI64::new(0),
                tripped: AtomicBool::new(false),
            })
        }

        /// Called by `preflight_phase!` at the START of each step.
        /// Records the active step so the watchdog can compute
        /// `now - step_started_at_ms`. Must NOT be called from within
        /// the watchdog thread.
        ///
        /// Memory ordering: `step_started_at_ms` is written first with
        /// `Relaxed`, then `current_step_idx` with `Release`. The
        /// watchdog reads `current_step_idx` with `Acquire` and
        /// `step_started_at_ms` with `Relaxed`. The Release–Acquire pair
        /// guarantees that once the watchdog observes the new
        /// `current_step_idx` it also observes the `step_started_at_ms`
        /// written before it. Without this pairing, a weakly-ordered
        /// CPU (AArch64) could let the watchdog see the new step index
        /// but the old (or zero) timestamp, computing a falsely enormous
        /// `elapsed_ms` and firing a spurious timeout.
        pub(crate) fn enter(&self, step_idx: u8, started_at_ms: i64) {
            // Write timestamp BEFORE step index so the Release on
            // `current_step_idx` acts as the synchronisation point.
            self.step_started_at_ms
                .store(started_at_ms, Ordering::Relaxed);
            self.current_step_idx.store(step_idx, Ordering::Release);
        }

        /// Documented API for explicitly quiescing the watchdog after a
        /// bounded preflight step completes and before long-running scan
        /// or rebuild work begins.
        pub(crate) fn exit(&self) {
            self.current_step_idx.store(NO_STEP, Ordering::Relaxed);
        }
    }

    /// Policy for what the watchdog does on a timeout. Production
    /// uses `Abort`; the regression test injects `RecordOnly` so it
    /// can observe the trip without exiting the test process.
    #[derive(Clone, Copy)]
    pub(crate) enum TimeoutPolicy {
        /// Rewrite lock-file phase, emit structured event, then exit
        /// the process with status 70.
        Abort,
        /// Rewrite lock-file phase, emit structured event, but
        /// DON'T exit. The watchdog still stops itself after one
        /// trip so the test doesn't loop. Used only by
        /// `tests::watch_startup_preflight_watchdog_fires_on_wedged_step`.
        #[cfg_attr(not(test), allow(dead_code))]
        RecordOnly,
    }

    /// The watchdog thread itself. Polls `WatchStartupPreflightState`
    /// every 5 s in production (50 ms in tests, gated by the
    /// `poll_interval` parameter). On timeout: rewrites the lock-file
    /// `phase=` to `<step>_TIMEOUT`, logs structured diagnostics, and
    /// applies the configured `TimeoutPolicy`.
    pub(crate) struct WatchStartupPreflightWatchdog {
        stop: Arc<AtomicBool>,
        join: Option<JoinHandle<()>>,
    }

    impl WatchStartupPreflightWatchdog {
        /// `metadata_write_lock` is the same `Arc<Mutex<()>>` that the
        /// heartbeat thread and `IndexRunLockGuard::write_metadata` take
        /// before every lock-file write. Passing it here ensures the
        /// watchdog's `_TIMEOUT` rewrite cannot be overwritten by a
        /// concurrent heartbeat tick: without this lock the heartbeat
        /// could clobber the `_TIMEOUT` breadcrumb between the watchdog's
        /// `set_len(0)` and `write_all` (or just after), leaving
        /// operators with an unmodified `phase=` in the lock file even
        /// though `process::exit(70)` was called.
        #[allow(clippy::too_many_arguments)]
        pub(crate) fn start(
            state: Arc<WatchStartupPreflightState>,
            taxonomy: &'static [&'static str],
            lock_path: std::path::PathBuf,
            data_dir: std::path::PathBuf,
            timeout: Duration,
            poll_interval: Duration,
            policy: TimeoutPolicy,
            metadata_write_lock: std::sync::Arc<std::sync::Mutex<()>>,
        ) -> Self {
            let stop = Arc::new(AtomicBool::new(false));
            let stop_flag = Arc::clone(&stop);
            let join = thread::spawn(move || {
                while !stop_flag.load(Ordering::Relaxed) {
                    thread::park_timeout(poll_interval);
                    if stop_flag.load(Ordering::Relaxed) {
                        break;
                    }
                    // Acquire pairs with the Release in `enter()` so
                    // that once we see the new step index we are also
                    // guaranteed to see the `step_started_at_ms` that
                    // was stored (Relaxed) before the Release store.
                    let step_idx = state.current_step_idx.load(Ordering::Acquire);
                    if step_idx == NO_STEP {
                        continue;
                    }
                    let started_at_ms = state.step_started_at_ms.load(Ordering::Relaxed);
                    let now_ms = crate::storage::sqlite::FrankenStorage::now_millis();
                    let elapsed_ms = now_ms.saturating_sub(started_at_ms).max(0);
                    if (elapsed_ms as u128) < timeout.as_millis() {
                        continue;
                    }
                    // Race guard: if the main thread has already
                    // advanced past this step (exit -> NO_STEP -> next
                    // enter sets a different idx) then `step_idx` here
                    // may be stale. Re-read with Acquire so we also
                    // observe any `step_started_at_ms` update the main
                    // thread wrote before advancing past this step.
                    let current_again = state.current_step_idx.load(Ordering::Acquire);
                    if current_again != step_idx {
                        continue;
                    }
                    if state.tripped.load(Ordering::SeqCst) {
                        // Another tick already tripped; nothing to do.
                        break;
                    }
                    let sub_phase = taxonomy
                        .get(step_idx as usize)
                        .copied()
                        .unwrap_or("watch_startup:unknown");
                    let timeout_phase = format!("{sub_phase}_TIMEOUT");
                    let timeout_secs = timeout.as_secs();
                    tracing::error!(
                        target: "cass::preflight_timeout",
                        sub_phase,
                        elapsed_ms,
                        timeout_secs,
                        skip_env = %skip_env_name(sub_phase),
                        "cass#265/GH#413: index preflight step exceeded timeout; \
                         the indexing process is being aborted to surface the wedge. \
                         Re-run with the documented skip env var set to bypass this step."
                    );
                    // Rewrite the lock file's `phase=` to the
                    // `_TIMEOUT` variant so any concurrent reader
                    // (cass health --json, the operator's terminal)
                    // sees exactly which step hung. ORDER MATTERS:
                    // the lock-file and sidecar rewrite happen before
                    // `tripped=true` so the test and any same-process
                    // observer can treat the flag as "timeout metadata
                    // is already durable".
                    //
                    // We hold `metadata_write_lock` across the entire
                    // rewrite. The heartbeat thread and
                    // `IndexRunLockGuard::write_metadata` both take the
                    // same lock before every lock-file write, so this
                    // prevents a concurrent heartbeat tick from
                    // clobbering the `_TIMEOUT` breadcrumb after we
                    // truncate but before we finish writing.
                    let _metadata_guard = metadata_write_lock.lock().ok();
                    if let Err(err) =
                        rewrite_lock_phase_for_timeout(&lock_path, &timeout_phase, now_ms)
                    {
                        tracing::error!(
                            target: "cass::preflight_timeout",
                            error = %err,
                            "failed to rewrite lock-file phase to TIMEOUT variant; aborting anyway"
                        );
                    }
                    state.tripped.store(true, Ordering::SeqCst);
                    // Also emit a JSON line on stderr so structured-
                    // log consumers get a self-contained event.
                    let event = serde_json::json!({
                        "event": "watch_startup_preflight_timeout",
                        "sub_phase": sub_phase,
                        "elapsed_ms": elapsed_ms,
                        "timeout_secs": timeout_secs,
                        "skip_env": skip_env_name(sub_phase),
                        "data_dir": data_dir.display().to_string(),
                        "lock_path": lock_path.display().to_string(),
                        "hint": format!(
                            "Set {} to bypass this preflight step. The wedge \
                             needs a root-cause fix; the skip is a temporary \
                             workaround. See cass#265 and cass#413 for the running investigation.",
                            skip_env_name(sub_phase)
                        ),
                    });
                    eprintln!("{event}");
                    match policy {
                        TimeoutPolicy::Abort => {
                            // EX_SOFTWARE (70): internal software
                            // error. `std::process::exit` skips stack
                            // destructors, so the
                            // `IndexRunLockGuard::Drop` impl doesn't
                            // run — it would truncate the file and
                            // erase the `_TIMEOUT` breadcrumb we
                            // just wrote.
                            std::process::exit(70);
                        }
                        TimeoutPolicy::RecordOnly => {
                            // Test path: stop polling and let the
                            // main thread observe `state.tripped`
                            // and the rewritten lock file.
                            break;
                        }
                    }
                }
            });
            Self {
                stop,
                join: Some(join),
            }
        }

        /// Stop the watchdog cleanly. Called by `Drop` on the watchdog
        /// itself so a normal `run_index` exit doesn't leak the
        /// watchdog thread.
        pub(crate) fn shutdown(&mut self) {
            self.stop.store(true, Ordering::Relaxed);
            if let Some(join) = self.join.take() {
                join.thread().unpark();
                let _ = join.join();
            }
        }
    }

    impl Drop for WatchStartupPreflightWatchdog {
        fn drop(&mut self) {
            self.shutdown();
        }
    }

    /// Direct lock-file rewrite for the TIMEOUT path. We can't go
    /// through `IndexRunLockGuard::set_phase` from the watchdog
    /// thread because the guard isn't `Send`. Re-open, re-write,
    /// fsync.
    pub(crate) fn rewrite_lock_phase_for_timeout(
        lock_path: &std::path::Path,
        timeout_phase: &str,
        now_ms: i64,
    ) -> anyhow::Result<()> {
        match rewrite_lock_phase_file_for_timeout(lock_path, timeout_phase, now_ms) {
            Ok(payload) => super::write_index_run_lock_metadata_sidecar(lock_path, &payload),
            Err(err) if timeout_rewrite_hit_windows_lock_conflict(&err) => {
                let sidecar_path = super::index_run_lock_metadata_sidecar_path(lock_path);
                let raw = std::fs::read_to_string(&sidecar_path).with_context(|| {
                    format!(
                        "reading index-run lock metadata sidecar for timeout rewrite {}",
                        sidecar_path.display()
                    )
                })?;
                let payload = timeout_lock_metadata_payload(&raw, timeout_phase, now_ms);
                super::write_index_run_lock_metadata_sidecar(lock_path, &payload)
            }
            Err(err) => Err(err),
        }
    }

    fn rewrite_lock_phase_file_for_timeout(
        lock_path: &std::path::Path,
        timeout_phase: &str,
        now_ms: i64,
    ) -> anyhow::Result<String> {
        use std::io::{Read, Seek, SeekFrom, Write};
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(lock_path)?;
        let mut buf = String::new();
        file.read_to_string(&mut buf)?;
        let payload = timeout_lock_metadata_payload(&buf, timeout_phase, now_ms);
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(payload.as_bytes())?;
        file.flush()?;
        file.sync_all()?;
        Ok(payload)
    }

    fn timeout_lock_metadata_payload(existing: &str, timeout_phase: &str, now_ms: i64) -> String {
        // Rewrite each line in place: `phase=...` -> `phase=<timeout_phase>`,
        // `updated_at_ms=...` -> `updated_at_ms=<now_ms>`. Preserve
        // every other line verbatim so the field order remains stable
        // for parsers.
        let mut new_lines = Vec::with_capacity(16);
        let mut wrote_phase = false;
        let mut wrote_updated_at = false;
        for line in existing.lines() {
            if line.strip_prefix("phase=").is_some() {
                new_lines.push(format!("phase={timeout_phase}"));
                wrote_phase = true;
            } else if line.starts_with("updated_at_ms=") {
                new_lines.push(format!("updated_at_ms={now_ms}"));
                wrote_updated_at = true;
            } else {
                new_lines.push(line.to_string());
            }
        }
        if !wrote_updated_at {
            new_lines.push(format!("updated_at_ms={now_ms}"));
        }
        if !wrote_phase {
            new_lines.push(format!("phase={timeout_phase}"));
        }
        let joined = new_lines.join("\n");
        if joined.ends_with('\n') {
            joined
        } else {
            format!("{joined}\n")
        }
    }

    fn timeout_rewrite_hit_windows_lock_conflict(err: &anyhow::Error) -> bool {
        err.chain().any(|cause| {
            cause
                .downcast_ref::<std::io::Error>()
                .is_some_and(super::windows_lock_conflict)
        })
    }
}

pub(crate) use watch_startup_preflight::{
    TimeoutPolicy as PreflightTimeoutPolicy, WatchStartupPreflightState,
    WatchStartupPreflightWatchdog,
};

/// Canonical stable-ID taxonomy of index preflight sub-phase breadcrumbs.
/// The historical names retain their `watch_startup:` prefix for contract
/// compatibility. Watch startup emits the active taxonomy; plain incremental
/// indexing also emits the nonresumable-checkpoint classification phase that it
/// can run. Retired entries remain reserved so existing numeric step IDs never
/// move. Newly appended entries need not match execution order. The watchdog
/// uses the array index as a `u8` step id; the
/// `watch_startup_sub_phase_taxonomy_is_documented_and_stable`
/// regression test pins this list against the call sites; and the
/// public docs / release notes / cass#265 triage runbook reference
/// these strings verbatim.
///
/// Two invariants every entry must satisfy:
/// 1. Starts with `watch_startup:`.
/// 2. No whitespace, no second colon — the lock file parser splits on
///    `=` and the sub-phase becomes a single-token value.
pub(crate) const WATCH_STARTUP_SUB_PHASE_TAXONOMY: &[&str] = &[
    "watch_startup:ensure_index_dir",
    "watch_startup:ensure_storage_headroom",
    "watch_startup:open_storage",
    "watch_startup:writable_preflight",
    "watch_startup:validate_fts_messages",
    "watch_startup:cleanup_orphan_fk_rows",
    "watch_startup:count_total_conversations",
    "watch_startup:probe_lexical_checkpoint",
    "watch_startup:repair_daily_stats",
    "watch_startup:tantivy_reader_preflight",
    "watch_startup:historical_salvage",
    "watch_startup:count_total_messages",
    "watch_startup:published_index_validate",
    "watch_startup:scan_entry",
    "watch_startup:reclassify_legacy_omp",
    "watch_startup:classify_nonresumable_checkpoint",
    // Retired by GH #413's count-free authoritative restart. Keep the slot so
    // the stable IDs of any future appended phases cannot reuse it.
    "watch_startup:count_nonresumable_checkpoint_conversations",
    "watch_startup:fts_shadow_viability",
    "watch_startup:raw_mirror_maintenance",
];

/// Lookup the taxonomy step index for a sub-phase string. Returns
/// `None` if the string isn't documented. Stable order — adding a new
/// preflight must append to the array (not insert) so existing call
/// sites keep the same id.
pub(crate) fn watch_startup_step_idx(sub_phase: &str) -> Option<u8> {
    WATCH_STARTUP_SUB_PHASE_TAXONOMY
        .iter()
        .position(|s| *s == sub_phase)
        .and_then(|i| u8::try_from(i).ok())
}

/// Should `run_index` write `sub_phase` to the lock file's `phase=` line
/// under `mode`?
///
/// Always. The preflight steps run in the same order in every index mode,
/// and a breadcrumb that is written for one step and then never advanced is
/// a lie: GH #443 reported a plain incremental run "stalled in
/// `classify_nonresumable_checkpoint`" (a sidecar-only read that returns in
/// milliseconds) when the process was actually wedged in the writable
/// SQLite open that follows it. `cass status`, the `stall_detected`
/// diagnostics and the operator all key on this string, so it must name the
/// step that is running now. The per-step abort watchdog is a separate
/// decision (`timeout_enforced` in the macro) and stays watch-startup-only.
pub(crate) const fn preflight_breadcrumb_visible(
    _mode: SearchMaintenanceMode,
    _sub_phase: &str,
) -> bool {
    true
}

/// True if the operator has set `CASS_SKIP_PREFLIGHT_<NAME>=1` for the
/// given sub-phase. Logs at WARN when it returns true so the bypass
/// is auditable in production. Thin wrapper over
/// `watch_startup_preflight::should_skip` so call sites in `run_index`
/// can read as `if preflight_skip("watch_startup:cleanup_orphan_fk_rows") { ... }`.
pub(crate) fn preflight_skip(sub_phase: &str) -> bool {
    watch_startup_preflight::should_skip(sub_phase)
}

pub(crate) fn dotenvy_truthy(var: &str) -> bool {
    dotenvy::var(var)
        .ok()
        .is_some_and(|v| matches!(v.trim(), "1" | "true" | "TRUE" | "yes" | "YES"))
}

pub(crate) fn preflight_validate_fts_messages_enabled() -> bool {
    dotenvy_truthy("CASS_PREFLIGHT_VALIDATE_FTS_MESSAGES")
}

pub(crate) fn preflight_cleanup_orphan_fk_rows_enabled() -> bool {
    dotenvy_truthy("CASS_PREFLIGHT_CLEANUP_ORPHAN_FK_ROWS")
}

pub(crate) fn preflight_historical_salvage_discovery_enabled() -> bool {
    dotenvy_truthy("CASS_PREFLIGHT_HISTORICAL_SALVAGE_DISCOVERY")
}

/// Per-batch / per-conversation / per-shard-flush atomic bump that the
/// indexer hot loops call. Single `Relaxed` store; the heartbeat folds
/// the value into the on-disk payload on its next tick. Returns the
/// timestamp that was stored (useful for tracing).
///
/// Threading the full `IndexRunLockGuard` reference down through every
/// layer of the staged lexical rebuild pipeline is a layering
/// nightmare; the only state the bump actually needs is the shared
/// atomic, which is `Arc::clone`-able and trivially passable.
pub(crate) fn bump_index_run_lock_progress_atomic(atomic: &Arc<AtomicI64>) -> i64 {
    let now_ms = FrankenStorage::now_millis();
    atomic.store(now_ms, Ordering::Relaxed);
    now_ms
}

pub(crate) fn bump_index_run_lock_progress_if_present(progress_bump: Option<&Arc<AtomicI64>>) {
    if let Some(atomic) = progress_bump {
        bump_index_run_lock_progress_atomic(atomic);
    }
}

pub(crate) struct IndexRunLockHeartbeat {
    pub(crate) stop: Arc<AtomicBool>,
    pub(crate) join: Option<JoinHandle<()>>,
}

impl IndexRunLockHeartbeat {
    pub(crate) fn start(
        data_dir: PathBuf,
        interval: Duration,
        metadata_write_lock: Arc<Mutex<()>>,
        // F4: indexer hot loops bump this atomic on each batch
        // commit / shard flush / conversation completion. The heartbeat
        // thread reads it on every tick and folds the latest value into
        // the on-disk `last_progress_at_ms=` line in the same rewrite
        // that already refreshes `updated_at_ms=`, so cass#258's
        // stall detector flips correctly on a wedged indexer without
        // making each batch pay the I/O cost.
        last_progress_at_ms_atomic: Arc<AtomicI64>,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        // Use `park_timeout` instead of `thread::sleep` so `Drop` below can
        // wake the worker immediately via `unpark`. With `sleep`, every
        // `run_index` call paid up to `interval` (default 1 s) at shutdown
        // while the main thread blocked on `join()` — which quantised
        // bench wall-clock to 1 s bins regardless of the actual indexing
        // cost. `park_timeout` preserves the periodic-heartbeat semantics
        // (it returns on token, on deadline, or spuriously; the outer
        // `while` loop re-checks `stop` each iteration) but makes the
        // shutdown path O(drain), not O(interval).
        let join = thread::spawn(move || {
            while !stop_flag.load(Ordering::Relaxed) {
                thread::park_timeout(interval);
                if stop_flag.load(Ordering::Relaxed) {
                    break;
                }
                let last_progress_at_ms = last_progress_at_ms_atomic.load(Ordering::Relaxed);
                if let Err(err) = heartbeat_index_run_lock_with_lock_and_progress(
                    &data_dir,
                    Some(&metadata_write_lock),
                    last_progress_at_ms,
                ) {
                    tracing::debug!(
                        error = %err,
                        path = %data_dir.display(),
                        "failed to refresh index-run heartbeat from background worker"
                    );
                }
            }
        });

        Self {
            stop,
            join: Some(join),
        }
    }
}

impl Drop for IndexRunLockHeartbeat {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(join) = self.join.take() {
            // Wake the worker if it's currently parked on `park_timeout`.
            // Ordered after the `stop.store` above so the worker always
            // sees `stop = true` on its next stop-flag read.
            join.thread().unpark();
            let _ = join.join();
        }
    }
}

pub(crate) fn heartbeat_index_run_lock_with_lock(
    data_dir: &Path,
    metadata_write_lock: Option<&Arc<Mutex<()>>>,
) -> Result<()> {
    // Legacy call site (test-only): when no fresh forward-progress
    // value is supplied, do NOT touch the `last_progress_at_ms=` line.
    // cass#258 invariant — heartbeat refreshing the progress field by
    // itself would silently disable the stall detector.
    heartbeat_index_run_lock_with_lock_and_progress(data_dir, metadata_write_lock, 0)
}

/// Production heartbeat that ALSO folds the indexer's most-recent
/// `last_progress_at_ms` atomic bump into the on-disk rewrite. The
/// indexer owns the value (it writes the atomic from its hot loops);
/// the heartbeat thread is purely the I/O layer — it never invents
/// progress on its own. Pass `0` (or any value ≤ the existing on-disk
/// value) to leave the field untouched, preserving the legacy heartbeat
/// behaviour for tests that exercise just the `updated_at_ms` path.
pub(crate) fn heartbeat_index_run_lock_with_lock_and_progress(
    data_dir: &Path,
    metadata_write_lock: Option<&Arc<Mutex<()>>>,
    last_progress_at_ms: i64,
) -> Result<()> {
    let _write_guard = metadata_write_lock
        .map(|lock| {
            lock.lock()
                .map_err(|_| anyhow::anyhow!("index-run metadata write lock poisoned"))
        })
        .transpose()?;
    let lock_path = index_run_lock_path(data_dir);
    let existing = match fs::read_to_string(&lock_path) {
        Ok(contents) if !contents.is_empty() => contents,
        Ok(_) => return Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) if windows_lock_conflict(&err) => {
            let sidecar_path = index_run_lock_metadata_sidecar_path(&lock_path);
            match fs::read_to_string(&sidecar_path) {
                Ok(contents) if !contents.is_empty() => contents,
                Ok(_) => return Ok(()),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!(
                            "reading index-run lock heartbeat sidecar {}",
                            sidecar_path.display()
                        )
                    });
                }
            }
        }
        Err(err) => {
            return Err(err).with_context(|| {
                format!("reading index-run lock heartbeat {}", lock_path.display())
            });
        }
    };

    let now_ms = FrankenStorage::now_millis();
    let mut updated_at_buf = itoa::Buffer::new();
    let now_ms_str = updated_at_buf.format(now_ms);

    // Read the existing on-disk progress so we only rewrite the
    // `last_progress_at_ms=` line if the indexer-owned atomic carries a
    // strictly larger value. This preserves the cass#258 invariant
    // (heartbeat never invents progress on its own) and avoids racing
    // with a `write_metadata` call that just wrote a fresh phase
    // transition.
    let existing_progress = existing
        .lines()
        .find_map(|line| line.strip_prefix("last_progress_at_ms="))
        .and_then(|raw| raw.trim().parse::<i64>().ok())
        .unwrap_or(0);
    let should_advance_progress = last_progress_at_ms > existing_progress;
    let mut progress_buf = itoa::Buffer::new();
    let progress_str = if should_advance_progress {
        progress_buf.format(last_progress_at_ms)
    } else {
        ""
    };

    let mut wrote_updated_at = false;
    let mut wrote_progress = !should_advance_progress;
    let mut refreshed = String::with_capacity(existing.len() + 64);
    for line in existing.lines() {
        if line.strip_prefix("updated_at_ms=").is_some() {
            refreshed.push_str("updated_at_ms=");
            refreshed.push_str(now_ms_str);
            wrote_updated_at = true;
        } else if line.strip_prefix("last_progress_at_ms=").is_some() {
            if should_advance_progress {
                refreshed.push_str("last_progress_at_ms=");
                refreshed.push_str(progress_str);
            } else {
                refreshed.push_str(line);
            }
            wrote_progress = true;
        } else {
            refreshed.push_str(line);
        }
        refreshed.push('\n');
    }
    if !wrote_updated_at {
        refreshed.push_str("updated_at_ms=");
        refreshed.push_str(now_ms_str);
        refreshed.push('\n');
    }
    if !wrote_progress {
        refreshed.push_str("last_progress_at_ms=");
        refreshed.push_str(progress_str);
        refreshed.push('\n');
    }

    write_index_run_lock_heartbeat_in_place(&lock_path, &refreshed)
}

pub(crate) fn write_index_run_lock_heartbeat_in_place(
    lock_path: &Path,
    refreshed: &str,
) -> Result<()> {
    match write_index_run_lock_file_in_place(lock_path, refreshed) {
        Ok(()) => write_index_run_lock_metadata_sidecar(lock_path, refreshed),
        Err(err) => {
            let lock_conflict = err.chain().any(|cause| {
                cause
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(windows_lock_conflict)
            });
            if lock_conflict {
                write_index_run_lock_metadata_sidecar(lock_path, refreshed)
            } else {
                Err(err)
            }
        }
    }
}

pub(crate) fn write_index_run_lock_file_in_place(lock_path: &Path, refreshed: &str) -> Result<()> {
    // Do not temp-file+rename `index-run.lock`: POSIX advisory locks attach
    // to the existing file inode/open handle, and replacing the path would
    // let another process lock a fresh inode while this process still holds
    // the old one.
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(lock_path)
        .with_context(|| format!("opening index-run lock heartbeat {}", lock_path.display()))?;
    file.set_len(0).with_context(|| {
        format!(
            "truncating index-run lock heartbeat {}",
            lock_path.display()
        )
    })?;
    file.rewind()
        .with_context(|| format!("rewinding index-run lock heartbeat {}", lock_path.display()))?;
    file.write_all(refreshed.as_bytes())
        .with_context(|| format!("writing index-run lock heartbeat {}", lock_path.display()))?;
    file.flush()
        .with_context(|| format!("flushing index-run lock heartbeat {}", lock_path.display()))?;
    file.sync_all()
        .with_context(|| format!("syncing index-run lock heartbeat {}", lock_path.display()))
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn heartbeat_index_run_lock(data_dir: &Path) -> Result<()> {
    heartbeat_index_run_lock_with_lock(data_dir, None)
}

pub(crate) fn index_run_lock_heartbeat_interval() -> Duration {
    Duration::from_millis(
        dotenvy::var("CASS_INDEX_RUN_LOCK_HEARTBEAT_EVERY_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(1_000),
    )
}
