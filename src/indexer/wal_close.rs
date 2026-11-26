use super::*;

/// Close a write handle the way `cass index` closes its own: restore the
/// checkpoint policy, close, then `wal_checkpoint(TRUNCATE)` on a fresh
/// handle so the sidecar does not outlive the command that filled it.
///
/// WS-B.5 (z2uon): every mutating command that is not an index run —
/// analytics rebuilds, doctor repairs, quarantine retries — used to drop its
/// handle and leave the WAL for the next opener to replay. The owner's archive
/// carried a 200 MB WAL for 18 days that way, and every default search paid
/// for it. A blocked checkpoint (a concurrent reader pinning the WAL) is not
/// an error here; it is logged by `query_final_wal_checkpoint` and the caller
/// reports it truthfully.
pub(crate) fn close_storage_with_wal_checkpoint(
    storage: FrankenStorage,
    db_path: &Path,
    context: &str,
) -> Result<()> {
    close_storage_after_index(storage, db_path, context)
}

pub(in crate::indexer) fn close_storage_after_index(
    storage: FrankenStorage,
    db_path: &Path,
    context: &str,
) -> Result<()> {
    prepare_storage_for_final_checkpoint(&storage, db_path, context);
    storage.close_without_checkpoint().with_context(|| {
        format!(
            "closing canonical db before final WAL checkpoint after {context}: {}",
            db_path.display()
        )
    })?;
    // The storage handle is already closed here, so the checkpoint should not be
    // blocked; a `Blocked` outcome (e.g. another cass process holds a reader) is
    // already surfaced via the WARN inside `query_final_wal_checkpoint`. Preserve
    // the historically-lenient contract of this normal-close path (Ok even if the
    // checkpoint could not fully truncate) — the abort path (#321) is the one that
    // must branch on the outcome.
    //
    // GH #382 / g3zyo: bounded. If a writable checkpoint stalls or loops on a
    // large WAL, this checkpoint must not hang *after* a successful publish.
    // The publish is durable before this point, but a
    // still-running checkpoint cannot be left behind when writer ownership
    // is released. The existing deadline now ends the process with exit 70;
    // the next owner recovers the committed WAL normally.
    let timeout = final_wal_checkpoint_timeout();
    match run_bounded_abort_wal_checkpoint(db_path.to_path_buf(), timeout, context, |path| {
        run_final_wal_checkpoint(path, context)
    }) {
        AbortWalCheckpointAttempt::Finished(Ok(_outcome)) => Ok(()),
        AbortWalCheckpointAttempt::Finished(Err(error)) => Err(anyhow::anyhow!(
            "final WAL checkpoint after {context} failed: {error}"
        )),
        AbortWalCheckpointAttempt::DeadlineUnavailable(error) => {
            tracing::warn!(
                db_path = %db_path.display(),
                context,
                %error,
                "final WAL checkpoint deadline timer was unavailable; the WAL is left for the next opener"
            );
            Ok(())
        }
    }
}

/// Wall-clock budget for the index run's final `wal_checkpoint(TRUNCATE)`
/// (GH #382 / g3zyo). `CASS_INDEX_FINAL_WAL_CHECKPOINT_TIMEOUT_SECS` overrides
/// the 900 s default, which stays below the finalize stall abort (1800 s) so
/// the checkpoint's own exit-70 deadline fires before the broader finalize
/// watchdog; `0` falls back to the default rather than disabling the bound.
pub(in crate::indexer) fn final_wal_checkpoint_timeout() -> Duration {
    const DEFAULT_SECS: u64 = 900;
    let secs = dotenvy::var("CASS_INDEX_FINAL_WAL_CHECKPOINT_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .unwrap_or(DEFAULT_SECS);
    Duration::from_secs(secs)
}

pub(in crate::indexer) fn prepare_storage_for_final_checkpoint(
    storage: &FrankenStorage,
    db_path: &Path,
    context: &str,
) {
    let previous_pages = storage.index_writer_checkpoint_pages();
    persist::apply_index_writer_checkpoint_policy(storage, false);
    let restored_pages = storage.index_writer_checkpoint_pages();
    if previous_pages == Some(0) {
        tracing::info!(
            db_path = %db_path.display(),
            context,
            restored_wal_autocheckpoint_pages = ?restored_pages,
            "restored checkpoint policy before final index close"
        );
    }
}

/// Result of a `PRAGMA wal_checkpoint(TRUNCATE)` issued during index finalize
/// or the bounded stall-abort path.
///
/// The distinction is load-bearing for #321: a checkpoint that SQLite reports
/// as `busy=1` (or that backfilled fewer frames than the WAL held) did **not**
/// truncate the WAL, so the canonical DB is still a replay dependency on a
/// large `*.db-wal` and stock `PRAGMA integrity_check` will fail. Callers must
/// not report such a checkpoint as success.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::indexer) enum FinalWalCheckpointOutcome {
    /// The WAL was fully checkpointed and truncated (`busy == 0` and every
    /// logged frame was backfilled).
    Completed,
    /// The checkpoint was blocked by active readers/writers (`busy != 0`) or
    /// left un-backfilled frames. The canonical WAL was NOT truncated.
    Blocked {
        busy: i64,
        log_frames: i64,
        checkpointed_frames: i64,
    },
}

/// Outcome of a checkpoint with an owned terminal deadline. A timeout cannot
/// return: doing so would release ownership with a database operation alive.
#[derive(Debug)]
pub(in crate::indexer) enum AbortWalCheckpointAttempt {
    Finished(Result<FinalWalCheckpointOutcome, String>),
    DeadlineUnavailable(String),
}

pub(in crate::indexer) const ABORT_WAL_CHECKPOINT_TIMEOUT: Duration = Duration::from_secs(5);

/// Keep checkpoint open, SQL and close on the caller thread. An owned timer
/// preserves the existing deadline, including for a constructor that does not
/// expose operation cancellation. Timeout is terminal for this process; the
/// helper never returns while a detached writer could outlive its caller's
/// leases. This also protects nonterminal streaming-cancellation callers.
pub(in crate::indexer) fn run_bounded_abort_wal_checkpoint<F>(
    db_path: PathBuf,
    timeout: Duration,
    context: &str,
    checkpoint: F,
) -> AbortWalCheckpointAttempt
where
    F: FnOnce(&Path) -> Result<FinalWalCheckpointOutcome>,
{
    let diagnostic = serde_json::json!({
        "success": false,
        "code": 70,
        "kind": "wal-checkpoint-timeout",
        "retryable": true,
        "db_path": db_path,
        "context": context,
        "deadline_ms": timeout.as_millis().min(u128::from(u64::MAX)) as u64,
        "message": "WAL checkpoint exceeded its deadline; terminating the process before releasing archive ownership. Committed rows remain in the database/WAL for the next run to recover.",
    })
    .to_string();
    match checkpoint_deadline::run(timeout, diagnostic, || {
        checkpoint(&db_path).map_err(|error| format!("{error:#}"))
    }) {
        Ok(result) => AbortWalCheckpointAttempt::Finished(result),
        Err(error) => AbortWalCheckpointAttempt::DeadlineUnavailable(error.to_string()),
    }
}

/// Classify a `wal_checkpoint` status row `(busy, log_frames, checkpointed)`.
///
/// A checkpoint only counts as [`FinalWalCheckpointOutcome::Completed`] when
/// SQLite reports `busy == 0` AND every frame in the WAL log was backfilled
/// (`checkpointed_frames >= log_frames`). Anything else — the #321 case of
/// `busy=1, log_frames=289842, checkpointed_frames=0` being the canonical
/// example — is [`FinalWalCheckpointOutcome::Blocked`] and must never be logged
/// or reported as a successful checkpoint.
pub(in crate::indexer) fn classify_final_wal_checkpoint(
    busy: i64,
    log_frames: i64,
    checkpointed_frames: i64,
) -> FinalWalCheckpointOutcome {
    let left_frames_uncheckpointed = log_frames > 0 && checkpointed_frames < log_frames;
    if busy > 0 || left_frames_uncheckpointed {
        FinalWalCheckpointOutcome::Blocked {
            busy,
            log_frames,
            checkpointed_frames,
        }
    } else {
        FinalWalCheckpointOutcome::Completed
    }
}

/// Best-effort canonical-WAL checkpoint before a bounded abort/exit (#296).
///
/// The `cass index` stall-abort path exits via `std::process::exit(70)`, which
/// skips destructors — so the canonical WAL is never checkpointed and a killed
/// run leaves a multi-GB orphaned `*.db-wal` behind a tiny `*.db` (the #296
/// symptom). This opens a *fresh, independent* SQLite connection to the
/// canonical DB file and runs
/// `wal_checkpoint(TRUNCATE)` so the post-abort DB is recoverable by stock
/// SQLite. The fresh checkpoint stays on this calling thread and gets only
/// [`ABORT_WAL_CHECKPOINT_TIMEOUT`]: the still-live wedged writer
/// can otherwise block the fresh open/checkpoint indefinitely and turn the
/// watchdog's promised exit into another hang. Ordinary errors remain
/// best-effort; timeout terminates the whole process rather than returning
/// with an independent archive writer still alive.
/// Stale index-run locks are already reaped on the next startup by
/// `read_search_maintenance_snapshot` (flock-based), so this focuses on the
/// WAL.
///
/// #321: the abort races the still-running `run_index` thread, which holds the
/// canonical storage handle open while it performs its own slow finalize
/// checkpoint of the (deferred, multi-GB) WAL. That open handle makes this
/// fresh-connection checkpoint report `busy=1 / checkpointed_frames=0` — a
/// no-op. We must NOT log success in that case: the WAL is left un-truncated
/// and stock SQLite integrity checks fail. Report the truth instead so the
/// operator (and the next startup's recovery) know the WAL is still stranded.
pub fn best_effort_abort_wal_checkpoint(data_dir: &Path) {
    best_effort_abort_wal_checkpoint_for_db(&data_dir.join("agent_search.db"));
}

pub(in crate::indexer) fn best_effort_abort_wal_checkpoint_for_db(db_path: &Path) {
    let mut database_absent = false;
    match run_bounded_abort_wal_checkpoint(
        db_path.to_path_buf(),
        ABORT_WAL_CHECKPOINT_TIMEOUT,
        "stall abort",
        |path| {
            // Metadata can block on the same filesystem as the stalled
            // indexer. Arm the terminal deadline before even this existence
            // probe; keep absent archives as a no-op without creating one.
            if !path.try_exists()? {
                database_absent = true;
                return Ok(FinalWalCheckpointOutcome::Completed);
            }
            run_final_wal_checkpoint(path, "stall abort")
        },
    ) {
        AbortWalCheckpointAttempt::Finished(Ok(_)) if database_absent => {}
        AbortWalCheckpointAttempt::Finished(Ok(FinalWalCheckpointOutcome::Completed)) => {
            tracing::info!(
                db_path = %db_path.display(),
                "checkpointed canonical WAL before stall abort (#296)"
            );
        }
        AbortWalCheckpointAttempt::Finished(Ok(FinalWalCheckpointOutcome::Blocked {
            busy,
            log_frames,
            checkpointed_frames,
        })) => {
            tracing::warn!(
                db_path = %db_path.display(),
                busy,
                log_frames,
                checkpointed_frames,
                "WAL checkpoint before stall abort was blocked (busy or frames left \
                 un-truncated); the canonical DB is left depending on an un-checkpointed \
                 WAL and stock SQLite integrity checks will fail until the next clean run \
                 checkpoints it (#296/#321)"
            );
        }
        AbortWalCheckpointAttempt::Finished(Err(error)) => {
            tracing::warn!(
                db_path = %db_path.display(),
                %error,
                "best-effort WAL checkpoint before stall abort failed; the WAL may remain uncheckpointed until the next clean run"
            );
        }
        AbortWalCheckpointAttempt::DeadlineUnavailable(error) => {
            tracing::warn!(
                db_path = %db_path.display(),
                %error,
                "best-effort WAL checkpoint deadline timer was unavailable; leaving the WAL for the next opener"
            );
        }
    }
}

/// Run one `wal_checkpoint(TRUNCATE)` on a fresh handle and report whether
/// the WAL was actually truncated (`Ok(true)`) or left in place because a
/// concurrent reader/writer pinned it (`Ok(false)`). For repair surfaces
/// (`doctor --fix`, WS-B.5) that must tell the operator the truth about an
/// oversized sidecar rather than claim a checkpoint that did not happen.
/// Test hook: park every `wal_checkpoint(TRUNCATE)` issued through
/// [`run_final_wal_checkpoint`] for this many milliseconds before it opens the
/// archive, so the deadlines around it (doctor `--fix`, the index run's final
/// close) can be exercised without an archive whose writable open really
/// loops (GH #382 needs a multi-GB archive for that).
pub(crate) const CASS_TEST_WAL_CHECKPOINT_PARK_MS_ENV: &str = "CASS_TEST_WAL_CHECKPOINT_PARK_MS";

/// [`checkpoint_wal_truncate`] with a wall-clock deadline (GH #382, bead
/// g3zyo). On the owner-scale archive with a 200 MB WAL, a slow or stalled
/// writable open/checkpoint turned `cass doctor --fix` into an
/// infinite hang. The checkpoint stays on the caller thread, under its own
/// canonical archive lease. If it exceeds `deadline`, exit 70 preserves the
/// ownership boundary; returning a report while a detached writer remained
/// alive would let another maintenance command enter the same archive.
pub(crate) fn checkpoint_wal_truncate_with_deadline(
    db_path: &Path,
    context: &str,
    deadline: Duration,
) -> Result<bool> {
    // The doctor caller owns its separate repair lock, but that does not
    // serialize against a writer using another data directory. Admission must
    // be nonblocking here: an index owner may itself be awaiting that doctor
    // lock, so waiting for its archive lease would invert the lock order.
    let _archive_write_lease = acquire_archive_write_lease(db_path)?;
    match run_bounded_abort_wal_checkpoint(db_path.to_path_buf(), deadline, context, |path| {
        run_final_wal_checkpoint(path, context)
    }) {
        AbortWalCheckpointAttempt::Finished(Ok(outcome)) => {
            Ok(matches!(outcome, FinalWalCheckpointOutcome::Completed))
        }
        AbortWalCheckpointAttempt::Finished(Err(error)) => Err(anyhow::anyhow!("{error}")),
        AbortWalCheckpointAttempt::DeadlineUnavailable(error) => Err(anyhow::anyhow!(
            "WAL checkpoint deadline timer was unavailable after {context}: {error}"
        )),
    }
}

pub(in crate::indexer) fn run_final_wal_checkpoint(
    db_path: &Path,
    context: &str,
) -> Result<FinalWalCheckpointOutcome> {
    if let Some(park_ms) = dotenvy::var(CASS_TEST_WAL_CHECKPOINT_PARK_MS_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|ms| *ms > 0)
    {
        std::thread::sleep(Duration::from_millis(park_ms));
    }
    // Run this after closing the indexing storage handle so pending writes are
    // flushed, and TRUNCATE avoids leaving the completed bulk-ingest WAL for
    // the next opener to replay.
    let checkpoint_db_path = db_path.to_string_lossy().into_owned();
    let conn = crate::franken_sync::Connection::open(checkpoint_db_path).with_context(|| {
        format!(
            "opening canonical db for final WAL checkpoint after {context}: {}",
            db_path.display()
        )
    })?;

    let checkpoint_result = query_final_wal_checkpoint(&conn, db_path, context);
    let close_result = conn.close().with_context(|| {
        format!(
            "closing final WAL checkpoint handle after {context}: {}",
            db_path.display()
        )
    });
    let outcome = checkpoint_result?;
    close_result?;
    Ok(outcome)
}

pub(in crate::indexer) fn query_final_wal_checkpoint(
    conn: &crate::franken_sync::Connection,
    db_path: &Path,
    context: &str,
) -> Result<FinalWalCheckpointOutcome> {
    let rows = conn
        .query("PRAGMA wal_checkpoint(TRUNCATE);")
        .with_context(|| {
            format!(
                "running final WAL checkpoint after {context}: {}",
                db_path.display()
            )
        })?;
    let row = rows.first().ok_or_else(|| {
        anyhow::anyhow!(
            "final WAL checkpoint returned no status row after {context}: {}",
            db_path.display()
        )
    })?;
    let busy: i64 = row
        .get_typed(0)
        .with_context(|| "reading final WAL checkpoint busy flag")?;
    let log_frames: i64 = row
        .get_typed(1)
        .with_context(|| "reading final WAL checkpoint log frame count")?;
    let checkpointed_frames: i64 = row
        .get_typed(2)
        .with_context(|| "reading final WAL checkpoint backfilled frame count")?;

    let outcome = classify_final_wal_checkpoint(busy, log_frames, checkpointed_frames);
    if log_frames >= 0 {
        match outcome {
            FinalWalCheckpointOutcome::Blocked { .. } => {
                tracing::warn!(
                    db_path = %db_path.display(),
                    context,
                    busy,
                    log_frames,
                    checkpointed_frames,
                    "final WAL checkpoint was blocked by active readers or left frames un-truncated"
                );
            }
            FinalWalCheckpointOutcome::Completed => {
                tracing::info!(
                    db_path = %db_path.display(),
                    context,
                    log_frames,
                    checkpointed_frames,
                    "final WAL checkpoint completed after index run"
                );
            }
        }
    }

    Ok(outcome)
}

pub(in crate::indexer) fn restore_watch_steady_state_checkpoint_policy(
    storage: &FrankenStorage,
    watch_enabled: bool,
) {
    if watch_enabled {
        persist::apply_index_writer_checkpoint_policy(storage, false);
    }
}

pub(in crate::indexer) fn close_watch_storage_for_recycle(
    storage: &Mutex<FrankenStorage>,
    reopen_pending: &std::cell::Cell<bool>,
) -> Result<()> {
    if reopen_pending.get() {
        return Ok(());
    }
    let mut storage = storage
        .lock()
        .map_err(|_| anyhow::anyhow!("storage lock poisoned before watch recycle"))?;
    // Record the obligation before closing. A failed open must never turn
    // the next callback into another attempt to use this closed connection.
    reopen_pending.set(true);
    storage.close_best_effort_in_place();
    Ok(())
}

pub(in crate::indexer) fn reopen_watch_storage_if_pending(
    storage: &Mutex<FrankenStorage>,
    db_path: &Path,
    reopen_pending: &std::cell::Cell<bool>,
    watch_enabled: bool,
    progress: Option<&Arc<IndexingProgress>>,
) -> Result<()> {
    if !reopen_pending.get() {
        return Ok(());
    }
    if let Some(progress) = progress {
        progress.check_stop()?;
    }
    let mut storage = storage
        .lock()
        .map_err(|_| anyhow::anyhow!("storage lock poisoned while reopening watch storage"))?;
    let replacement = FrankenStorage::open(db_path)
        .with_context(|| {
            format!(
                "reopening canonical db after watch recycle: {}",
                db_path.display()
            )
        })
        .inspect_err(|error| {
            set_progress_last_error(progress, Some(format!("{error:#}")));
        })?;
    restore_watch_steady_state_checkpoint_policy(&replacement, watch_enabled);
    *storage = replacement;
    reopen_pending.set(false);
    set_progress_last_error(progress, None);
    if let Some(progress) = progress {
        progress.tick_activity();
    }
    Ok(())
}

pub(in crate::indexer) fn release_watch_storage_after_index(
    storage: Rc<Mutex<FrankenStorage>>,
    db_path: &Path,
    context: &str,
) -> Result<()> {
    let storage = Rc::try_unwrap(storage).map_err(|_| {
        anyhow::anyhow!(
            "watch indexing retained extra canonical db handles while closing {}",
            db_path.display()
        )
    })?;
    match storage.into_inner() {
        Ok(storage) => close_storage_after_index(storage, db_path, context),
        Err(poisoned) => {
            let mut storage = poisoned.into_inner();
            storage.close_best_effort_in_place();
            Err(anyhow::anyhow!(
                "storage mutex poisoned while closing canonical db after {context}: {}",
                db_path.display()
            ))
        }
    }
}
