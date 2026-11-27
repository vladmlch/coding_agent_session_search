use std::fs::OpenOptions;
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::Result;

pub use crate::search_defaults::{SearchMaintenanceJobKind, SearchMaintenanceMode};
pub use crate::storage::run_lock::{
    clear_index_run_lock_metadata_sidecar, index_run_lock_metadata_sidecar_path,
    index_run_lock_path, windows_lock_conflict, write_index_run_lock_metadata_sidecar,
};

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct SearchMaintenanceSnapshot {
    pub active: bool,
    pub pid: Option<u32>,
    pub started_at_ms: Option<i64>,
    pub db_path: Option<PathBuf>,
    pub mode: Option<SearchMaintenanceMode>,
    pub job_id: Option<String>,
    pub job_kind: Option<SearchMaintenanceJobKind>,
    pub phase: Option<String>,
    pub updated_at_ms: Option<i64>,
    pub last_progress_at_ms: Option<i64>,
    pub orphaned: bool,
}

fn read_capped_metadata_from_path(path: &Path, max_len: u64) -> std::io::Result<String> {
    let file = OpenOptions::new().read(true).open(path)?;
    let mut raw = String::new();
    (&file).take(max_len).read_to_string(&mut raw)?;
    Ok(raw)
}

/// Pure-read maintenance probe (gh#422): search-triggered refresh decisions
/// must never write, so this variant is byte-for-byte read-only even for
/// stale metadata (which it still hides from the caller).
pub fn read_search_maintenance_snapshot(data_dir: &Path) -> SearchMaintenanceSnapshot {
    read_search_maintenance_snapshot_inner(data_dir, false)
}

/// Observation-surface maintenance probe (#176 / bd-k9jb9): status, health,
/// and TUI polls additionally REAP stale metadata in place (flock-guarded,
/// best-effort) so subsequent readers observe a clean lock file without
/// re-doing the staleness dance. Search paths must use the pure variant.
pub fn read_search_maintenance_snapshot_reaping(data_dir: &Path) -> SearchMaintenanceSnapshot {
    read_search_maintenance_snapshot_inner(data_dir, true)
}

fn read_search_maintenance_snapshot_inner(
    data_dir: &Path,
    reap_stale: bool,
) -> SearchMaintenanceSnapshot {
    // Real index-run.lock files written by `acquire_index_run_lock`
    // have a fixed key=value shape under ~1 KiB. Cap the read at 64 KiB
    // so a corrupted or maliciously-large lock file cannot force us to
    // allocate arbitrary memory just to inspect its metadata.
    const MAX_LOCK_FILE_READ: u64 = 64 * 1024;

    let lock_path = index_run_lock_path(data_dir);
    let sidecar_path = index_run_lock_metadata_sidecar_path(&lock_path);
    let file = match OpenOptions::new().read(true).open(&lock_path) {
        Ok(file) => file,
        Err(err) if windows_lock_conflict(&err) => {
            let raw = read_capped_metadata_from_path(&sidecar_path, MAX_LOCK_FILE_READ)
                .unwrap_or_default();
            return parse_search_maintenance_snapshot(raw, true);
        }
        Err(_) => return SearchMaintenanceSnapshot::default(),
    };

    let mut raw = String::new();
    let mut read_blocked_by_lock = false;
    if let Err(err) = (&file).take(MAX_LOCK_FILE_READ).read_to_string(&mut raw)
        && windows_lock_conflict(&err)
    {
        read_blocked_by_lock = true;
    }
    if raw.trim().is_empty() {
        raw = read_capped_metadata_from_path(&sidecar_path, MAX_LOCK_FILE_READ).unwrap_or_default();
    }

    let mut snapshot = parse_search_maintenance_snapshot(raw, read_blocked_by_lock);
    if read_blocked_by_lock {
        return snapshot;
    }

    let metadata_present = snapshot.pid.is_some()
        || snapshot.started_at_ms.is_some()
        || snapshot.db_path.is_some()
        || snapshot.mode.is_some()
        || snapshot.job_id.is_some()
        || snapshot.job_kind.is_some()
        || snapshot.phase.is_some()
        || snapshot.updated_at_ms.is_some()
        || snapshot.last_progress_at_ms.is_some();

    #[cfg(windows)]
    let current_process_owns_recorded_lock =
        metadata_present && snapshot.pid == Some(std::process::id());
    #[cfg(not(windows))]
    let current_process_owns_recorded_lock = false;

    snapshot.active = if current_process_owns_recorded_lock {
        true
    } else {
        match file.try_lock_shared() {
            Ok(()) => {
                // A shared lock succeeds only when no writer holds the
                // exclusive index-run lock: the metadata is stale.
                //
                // Historically stale metadata produced a permanent
                // `orphaned: true` state that callers (notably the TUI)
                // interpreted as "rebuild in progress, keep polling" —
                // yielding a tight CPU-bound loop that only cleared when the
                // user manually deleted the lock file (see issue #176).
                //
                // On observation surfaces (#176, pinned by bd-k9jb9's e2e
                // contract) the stale metadata is also reaped IN PLACE —
                // guarded by the exclusive flock, best-effort — so every
                // subsequent reader observes a clean file without re-doing
                // this dance. Pure-read callers (gh#422 search paths) skip
                // the reap and stay byte-for-byte read-only. On a read-only
                // filesystem (or a lost lock race) the truncate is skipped;
                // either way the caller gets the clean default snapshot.
                let _ = file.unlock();
                if metadata_present {
                    if reap_stale {
                        reap_stale_index_run_lock_metadata(&lock_path, &sidecar_path);
                    }
                    return SearchMaintenanceSnapshot::default();
                }
                false
            }
            Err(std::fs::TryLockError::WouldBlock) => true,
            Err(std::fs::TryLockError::Error(err)) if windows_lock_conflict(&err) => true,
            Err(_) => false,
        }
    };
    snapshot.orphaned = metadata_present && !snapshot.active;
    snapshot
}

/// #176 / bd-k9jb9: truncate stale `index-run.lock` metadata in place so a
/// subsequent reader (next `cass status`, next TUI poll) observes a clean
/// state without re-doing the reaping dance. The truncate happens only while
/// holding the exclusive flock (concurrent readers/writers cannot race the
/// reap) and is best-effort: a read-only filesystem or a lost lock race
/// leaves the bytes untouched — the caller already reports the clean default
/// snapshot either way. The file itself is truncated, never deleted, to
/// preserve permissions and avoid create/recreate races with writers. The
/// metadata sidecar is cleared alongside it, since an empty lock file falls
/// back to the sidecar on the next read.
fn reap_stale_index_run_lock_metadata(lock_path: &Path, sidecar_path: &Path) {
    let Ok(file) = OpenOptions::new().read(true).write(true).open(lock_path) else {
        return;
    };
    if file.try_lock().is_err() {
        // A writer appeared between our shared probe and now; its metadata
        // is live again — do not touch it.
        return;
    }
    match file.set_len(0) {
        Ok(()) => {
            let _ = file.sync_all();
            tracing::info!(
                path = %lock_path.display(),
                "reaped stale index-run.lock metadata left by a dead owner (#176)"
            );
            if sidecar_path.exists() {
                let _ = std::fs::write(sidecar_path, b"");
            }
        }
        Err(err) => {
            tracing::warn!(
                path = %lock_path.display(),
                error = %err,
                "failed to reap stale index-run.lock metadata; leaving bytes in place"
            );
        }
    }
    let _ = file.unlock();
}

fn parse_search_maintenance_snapshot(
    raw: String,
    active_when_metadata_unreadable: bool,
) -> SearchMaintenanceSnapshot {
    let mut pid = None;
    let mut started_at_ms = None;
    let mut lock_db_path = None::<PathBuf>;
    let mut mode = None;
    let mut job_id = None;
    let mut job_kind = None;
    let mut phase = None;
    let mut updated_at_ms = None;
    let mut last_progress_at_ms = None;
    for line in raw.lines() {
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        match key.trim() {
            "pid" => pid = value.trim().parse::<u32>().ok(),
            "started_at_ms" => started_at_ms = value.trim().parse::<i64>().ok(),
            "db_path" => lock_db_path = Some(PathBuf::from(value.trim())),
            "mode" => mode = SearchMaintenanceMode::parse_lock_value(value),
            "job_id" => job_id = Some(value.trim().to_string()).filter(|value| !value.is_empty()),
            "job_kind" => job_kind = SearchMaintenanceJobKind::parse_lock_value(value),
            "phase" => phase = Some(value.trim().to_string()).filter(|value| !value.is_empty()),
            "updated_at_ms" => updated_at_ms = value.trim().parse::<i64>().ok(),
            "last_progress_at_ms" => last_progress_at_ms = value.trim().parse::<i64>().ok(),
            _ => {}
        }
    }

    let metadata_present = pid.is_some()
        || started_at_ms.is_some()
        || lock_db_path.is_some()
        || mode.is_some()
        || job_id.is_some()
        || job_kind.is_some()
        || phase.is_some()
        || updated_at_ms.is_some()
        || last_progress_at_ms.is_some();

    SearchMaintenanceSnapshot {
        active: active_when_metadata_unreadable,
        pid,
        started_at_ms,
        db_path: lock_db_path,
        mode,
        job_id,
        job_kind,
        phase,
        updated_at_ms,
        last_progress_at_ms,
        orphaned: metadata_present && !active_when_metadata_unreadable,
    }
}

pub const REBUILD_STALL_DETECT_SECS_DEFAULT: u64 = 120;

pub fn rebuild_stall_detect_threshold_ms() -> Option<i64> {
    let threshold_secs = dotenvy::var("CASS_REBUILD_STALL_DETECT_SECS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(REBUILD_STALL_DETECT_SECS_DEFAULT);
    if threshold_secs == 0 {
        return None;
    }
    let threshold_ms = threshold_secs.saturating_mul(1000);
    Some(i64::try_from(threshold_ms).unwrap_or(i64::MAX))
}

pub fn maintenance_stall_age_ms(snapshot: &SearchMaintenanceSnapshot, now_ms: i64) -> Option<i64> {
    if !snapshot.active
        || !snapshot
            .mode
            .is_some_and(SearchMaintenanceMode::rebuild_active)
    {
        return None;
    }
    // The native HNSW builder is one opaque, CPU-bound call without a
    // progress callback. Its owner heartbeat still proves the process is
    // alive, but treating an unchanged forward-progress cursor as a stalled
    // *lexical* rebuild during this explicitly labelled phase is misleading.
    // All other semantic phases expose real counters and remain eligible for
    // stale-progress reporting.
    if snapshot.job_kind == Some(SearchMaintenanceJobKind::SemanticRebuild)
        && snapshot.phase.as_deref() == Some("semantic:hnsw")
    {
        return None;
    }
    let last_progress_at_ms = snapshot.last_progress_at_ms?;
    let age_ms = now_ms.saturating_sub(last_progress_at_ms);
    let threshold_ms = rebuild_stall_detect_threshold_ms()?;
    (age_ms >= threshold_ms).then_some(age_ms)
}
