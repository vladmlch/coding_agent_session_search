use super::*;
use rayon::prelude::*;

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::indexer) enum LexicalPublishRenameSite {
    FirstPublish,
    LinuxParkPriorLiveToCanonicalSidecar,
    LinuxRetainCanonicalSidecar,
    NonLinuxParkPriorLive,
    NonLinuxPublishStagedLive,
    NonLinuxRetainPriorLive,
    RecoverRetainStaleInProgress,
    RecoverRestorePriorLive,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::indexer) struct LexicalPublishInjectedRenameFailure {
    pub(in crate::indexer) site: LexicalPublishRenameSite,
    pub(in crate::indexer) remaining_hits: usize,
    pub(in crate::indexer) raw_os_error: i32,
}

#[cfg(test)]
pub(in crate::indexer) static LEXICAL_PUBLISH_INJECTED_RENAME_FAILURE: std::sync::Mutex<
    Option<LexicalPublishInjectedRenameFailure>,
> = std::sync::Mutex::new(None);

#[cfg(test)]
pub(in crate::indexer) struct LexicalPublishInjectedRenameFailureGuard {
    pub(in crate::indexer) previous: Option<LexicalPublishInjectedRenameFailure>,
}

#[cfg(test)]
impl Drop for LexicalPublishInjectedRenameFailureGuard {
    fn drop(&mut self) {
        let mut guard = match LEXICAL_PUBLISH_INJECTED_RENAME_FAILURE.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        *guard = self.previous;
    }
}

#[cfg(test)]
pub(in crate::indexer) fn inject_lexical_publish_rename_failure_once(
    site: LexicalPublishRenameSite,
    raw_os_error: i32,
) -> LexicalPublishInjectedRenameFailureGuard {
    let mut guard = LEXICAL_PUBLISH_INJECTED_RENAME_FAILURE
        .lock()
        .expect("lexical publish rename fault injection lock");
    let previous = *guard;
    *guard = Some(LexicalPublishInjectedRenameFailure {
        site,
        remaining_hits: 1,
        raw_os_error,
    });
    LexicalPublishInjectedRenameFailureGuard { previous }
}

#[cfg(test)]
pub(in crate::indexer) fn maybe_inject_lexical_publish_rename_failure(
    site: LexicalPublishRenameSite,
) -> std::io::Result<()> {
    let mut guard = LEXICAL_PUBLISH_INJECTED_RENAME_FAILURE
        .lock()
        .expect("lexical publish rename fault injection lock");
    let Some(state) = *guard else {
        return Ok(());
    };
    if state.site != site || state.remaining_hits == 0 {
        return Ok(());
    }

    if state.remaining_hits == 1 {
        *guard = None;
    } else {
        *guard = Some(LexicalPublishInjectedRenameFailure {
            remaining_hits: state.remaining_hits - 1,
            ..state
        });
    }
    Err(std::io::Error::from_raw_os_error(state.raw_os_error))
}

pub(in crate::indexer) fn rename_lexical_publish_path(
    src: &Path,
    dst: &Path,
    site: LexicalPublishRenameSite,
) -> std::io::Result<()> {
    #[cfg(test)]
    maybe_inject_lexical_publish_rename_failure(site)?;
    #[cfg(not(test))]
    let _ = site;
    fs::rename(src, dst)
}

#[cfg(test)]
thread_local! {
    pub(in crate::indexer) static LEXICAL_TERMINAL_FAILPOINT: std::cell::Cell<Option<&'static str>> =
        const { std::cell::Cell::new(None) };
}

/// Test-only interruption probe for the terminal finalize. An armed stage makes
/// the next terminal step on this thread fail at that point; the rebuild then
/// returns without any cleanup on the terminal path, so the files stay as they
/// were at that point. Each probe fires once. A thread-local keeps parallel
/// tests from arming each other's rebuilds.
#[cfg(test)]
pub(in crate::indexer) fn lexical_terminal_failpoint(stage: &'static str) -> Result<()> {
    if LEXICAL_TERMINAL_FAILPOINT.with(std::cell::Cell::get) == Some(stage) {
        LEXICAL_TERMINAL_FAILPOINT.with(|armed| armed.set(None));
        anyhow::bail!("injected interruption at lexical terminal stage `{stage}`");
    }
    Ok(())
}

/// GH #441 test hook (tests/cli_index.rs): skip the post-run segment
/// maintenance (`optimize_if_idle` after an incremental run, `force_merge`
/// after a full rebuild) so a test can build a deliberately fragmented
/// generation — the shape of a v0.7.1 archive — and then prove that an
/// ordinary `cass index` consolidates it. Unset in production.
pub(in crate::indexer) fn lexical_post_run_maintenance_skipped_for_test() -> bool {
    dotenvy::var("CASS_TEST_SKIP_POST_RUN_LEXICAL_MAINTENANCE")
        .ok()
        .is_some_and(|raw| matches!(raw.trim(), "1" | "true" | "yes"))
}

/// GH #440 test hook (tests/cli_index.rs): after the Nth staged engine commit
/// (`CASS_TEST_LEXICAL_REBUILD_KILL_AFTER_COMMITS`, default 1), write a
/// sentinel describing the authority/checkpoint gap the commit just opened
/// and park so the test can SIGKILL the run inside that window. Unset in
/// production: an absent sentinel path returns immediately. A release path
/// lets process tests resume the real commit without a timing-dependent sleep.
pub(in crate::indexer) fn maybe_pause_lexical_rebuild_after_commit_for_kill(
    state_path: &Path,
    checkpoint_indexed_docs: usize,
    committed_indexed_docs: usize,
) -> Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    static COMMITS_SEEN: AtomicUsize = AtomicUsize::new(0);

    let sentinel_path = match dotenvy::var("CASS_TEST_LEXICAL_REBUILD_KILL_AFTER_COMMIT_SENTINEL") {
        Ok(raw) if !raw.trim().is_empty() => PathBuf::from(raw),
        _ => return Ok(()),
    };
    let fire_on = dotenvy::var("CASS_TEST_LEXICAL_REBUILD_KILL_AFTER_COMMITS")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(1);
    let seen = COMMITS_SEEN.fetch_add(1, AtomicOrdering::SeqCst) + 1;
    if seen != fire_on {
        return Ok(());
    }
    let sleep_ms = dotenvy::var("CASS_TEST_LEXICAL_REBUILD_KILL_AFTER_COMMIT_SLEEP_MS")
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(30_000);
    let payload = serde_json::json!({
        "stage": "staged_commit_published_checkpoint_not_yet_written",
        "pid": std::process::id(),
        "commit_ordinal": seen,
        "state_path": state_path.display().to_string(),
        "checkpoint_indexed_docs": checkpoint_indexed_docs,
        "committed_indexed_docs": committed_indexed_docs,
    });
    write_json_pretty_atomically(&sentinel_path, &payload).with_context(|| {
        format!(
            "writing lexical rebuild kill-after-commit sentinel {}",
            sentinel_path.display()
        )
    })?;
    if let Some(release_path) = dotenvy::var("CASS_TEST_LEXICAL_REBUILD_KILL_AFTER_COMMIT_RELEASE")
        .ok()
        .filter(|path| !path.trim().is_empty())
        .map(PathBuf::from)
    {
        let started = std::time::Instant::now();
        let timeout = Duration::from_millis(sleep_ms);
        while !release_path.exists() {
            let Some(remaining) = timeout.checked_sub(started.elapsed()) else {
                anyhow::bail!(
                    "lexical rebuild commit rendezvous was not released within {sleep_ms} ms"
                );
            };
            thread::park_timeout(remaining.min(Duration::from_millis(10)));
        }
    } else {
        thread::sleep(Duration::from_millis(sleep_ms));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
pub(in crate::indexer) fn maybe_pause_lexical_publish_for_kill_relaunch(
    index_path: &Path,
    canonical_sidecar: &Path,
) -> Result<()> {
    let sentinel_path = match dotenvy::var("CASS_TEST_LEXICAL_PUBLISH_KILL_RELAUNCH_SENTINEL") {
        Ok(raw) if !raw.trim().is_empty() => PathBuf::from(raw),
        _ => return Ok(()),
    };
    let sleep_ms = dotenvy::var("CASS_TEST_LEXICAL_PUBLISH_KILL_RELAUNCH_SLEEP_MS")
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(30_000);
    let payload = serde_json::json!({
        "stage": "linux_swap_committed_prior_live_parked",
        "pid": std::process::id(),
        "live_index_path": index_path.display().to_string(),
        "canonical_sidecar_path": canonical_sidecar.display().to_string(),
    });
    write_json_pretty_atomically(&sentinel_path, &payload).with_context(|| {
        format!(
            "writing lexical publish kill-relaunch sentinel {}",
            sentinel_path.display()
        )
    })?;
    thread::sleep(Duration::from_millis(sleep_ms));
    Ok(())
}

/// Scratch directory a from-zero lexical rebuild builds into before publishing.
///
/// A sibling of the live index rather than a temp dir elsewhere, so the final
/// publish is a same-filesystem rename and cannot fail partway across a device
/// boundary.
///
/// # Why this name is deliberately NOT a `staging_reclaim` staging prefix
///
/// `staging_reclaim` deletes `LockSerialized` staging directories once they are
/// 60 seconds old. A from-zero rebuild of a large archive runs for minutes to
/// hours, and a directory's mtime does not necessarily advance while content is
/// written into its subdirectories — so adopting a reclaimable prefix would let
/// a concurrent sweep delete a live rebuild out from under itself. Trading a
/// bounded disk residue for that is a bad trade.
///
/// The residue is bounded without reclaim: on the success path the publish
/// `RENAME_EXCHANGE` consumes this directory (it ends up holding the prior-live
/// index, which the retained-backup contract then owns), and on the crash path
/// the next from-zero rebuild clears it before reuse while an interrupted run
/// resumes into it. Worst case is one stale scratch directory, never a growing
/// set.
/// Extra documents routed through the upsert path beyond the measured
/// authority/checkpoint gap (#440). Upserting a document whose identity is
/// absent is an ordinary insert, so the margin only costs an identity probe
/// per document and buys tolerance for any off-by-a-batch drift between the
/// checkpoint's document accounting and the engine's live count.
pub(in crate::indexer) const RESUME_RECONCILE_SLACK_DOCS: usize = 4_096;

/// How many leading replayed documents a resumed rebuild must upsert (#440).
///
/// * A from-zero build (`resuming == false`) owes nothing: its target is empty.
/// * A resume into the LIVE index (`builds_in_live_index`) may be replaying
///   over a complete prior generation, so every document is upserted.
/// * A resume into a staged scratch owes the documents the authority holds
///   beyond the checkpoint, plus [`RESUME_RECONCILE_SLACK_DOCS`]; nothing when
///   the authority is at or behind the checkpoint.
pub(in crate::indexer) fn resume_reconcile_upsert_budget(
    builds_in_live_index: bool,
    resuming: bool,
    checkpoint_indexed_docs: usize,
    published_docs: usize,
) -> usize {
    if !resuming {
        return 0;
    }
    if builds_in_live_index {
        return usize::MAX;
    }
    match published_docs.checked_sub(checkpoint_indexed_docs) {
        Some(gap) if gap > 0 => gap.saturating_add(RESUME_RECONCILE_SLACK_DOCS),
        _ => 0,
    }
}

pub(in crate::indexer) fn staged_lexical_rebuild_scratch_path(index_path: &Path) -> PathBuf {
    let name = index_path
        .file_name()
        .map_or_else(|| "index".to_string(), |n| n.to_string_lossy().into_owned());
    index_path.with_file_name(format!(".{name}{LEXICAL_REBUILD_STAGING_SUFFIX}"))
}

pub(in crate::indexer) fn publish_staged_lexical_index(
    staged_index_path: &Path,
    index_path: &Path,
) -> Result<()> {
    if let Some(parent) = index_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "creating parent directory for staged lexical publish {}",
                parent.display()
            )
        })?;
    }
    recover_or_finalize_interrupted_lexical_publish_backup(index_path)?;
    remove_lexical_index_dir_placeholder(index_path);

    if !index_path.exists() {
        rename_lexical_publish_path(
            staged_index_path,
            index_path,
            LexicalPublishRenameSite::FirstPublish,
        )
        .with_context(|| {
            format!(
                "publishing first staged lexical index {} -> {}",
                staged_index_path.display(),
                index_path.display()
            )
        })?;
        sync_parent_directory(index_path)?;
        return Ok(());
    }

    ensure_lexical_publish_backups_dir(index_path)?;

    let retained_backup_path = unique_lexical_publish_backup_path(index_path);

    #[cfg(target_os = "linux")]
    {
        // A: atomic renameat2(RENAME_EXCHANGE). After the syscall,
        // `index_path` holds NEW content (live readers see it atomically)
        // and `staged_index_path` holds the OLD prior-live content.
        //
        // Filesystems that don't implement RENAME_EXCHANGE (NFSv3, some
        // older FUSE mounts, certain network exports) reject the syscall
        // with EINVAL. In that case we fall through to the rename-pair
        // strategy that the non-Linux branch already uses — same atomicity
        // guarantees on the live `index_path` handle, slightly wider crash
        // window because the swap is a sequence of two renames instead of
        // one syscall. See coding_agent_session_search#225.
        if let Err(err) = atomic_exchange_paths(index_path, staged_index_path) {
            // `atomic_exchange_paths` returns the io::Error wrapped via
            // `.with_context(...)`, so the io::Error sits in the source
            // chain rather than at the top level. Walk the chain
            // explicitly via `err.chain()` and `Error::downcast_ref` on
            // each link — robust regardless of how anyhow's top-level
            // `downcast_ref` resolves chained types across versions.
            let einval = linux_atomic_exchange_is_unsupported(&err);
            if einval {
                tracing::info!(
                    index_path = %index_path.display(),
                    staged_index_path = %staged_index_path.display(),
                    "renameat2(RENAME_EXCHANGE) returned EINVAL; falling through to rename-pair publish (likely NFSv3 or other filesystem without RENAME_EXCHANGE support)"
                );
                publish_via_rename_pair(staged_index_path, index_path, &retained_backup_path)?;
                sync_parent_directory(index_path)?;
                sync_parent_directory(&retained_backup_path)?;
                tracing::info!(
                    retained_backup_path = %retained_backup_path.display(),
                    "staged lexical index published via rename-pair fallback"
                );
                return Ok(());
            }
            return Err(err);
        }

        // A.5 (bead 9wkx5): park OLD at the canonical
        // `.<name>.publish-in-progress.bak` sidecar — the SAME handle the
        // non-Linux path and recovery already understand. Without this
        // step, a crash between A and B would strand OLD inside the
        // `cass-lexical-merge.<RANDOM>` TempDir (which gets reclaimed on
        // normal Drop but persists on hard crash), silently breaking the
        // retained-backup contract pinned by
        // publish_staged_lexical_index_retains_every_backup_unboundedly_today_pending_retention_cap.
        //
        // Same-filesystem rename within `index_path`'s parent is
        // essentially atomic, so this narrows the uncovered crash window
        // from "two rename operations across potentially different FS
        // mount points" to "one same-dir rename". Recovery on next
        // startup (`recover_or_finalize_interrupted_lexical_publish_backup`)
        // finds the canonical sidecar and moves it into
        // `.lexical-publish-backups/` automatically.
        let canonical_sidecar = lexical_publish_in_progress_backup_path(index_path);
        if let Err(park_err) = rename_lexical_publish_path(
            staged_index_path,
            &canonical_sidecar,
            LexicalPublishRenameSite::LinuxParkPriorLiveToCanonicalSidecar,
        ) {
            // A.5 failed: staged_index_path still holds OLD (inside the
            // merge tempdir). Roll the atomic swap back so live returns
            // to OLD and the NEW content goes back into the tempdir
            // (where normal Drop discards it).
            match atomic_exchange_paths(index_path, staged_index_path) {
                Ok(()) => {
                    return Err(park_err).with_context(|| {
                        format!(
                            "parking prior lexical index at canonical sidecar {} after atomic swap publish failed; rolled back to keep previous live index at {}",
                            canonical_sidecar.display(),
                            index_path.display()
                        )
                    });
                }
                Err(rollback_err) => {
                    return Err(anyhow::anyhow!(
                        "parking prior lexical index at canonical sidecar {} after atomic swap publish failed: {park_err:#}; rollback also failed: {rollback_err:#}",
                        canonical_sidecar.display()
                    ));
                }
            }
        }

        maybe_pause_lexical_publish_for_kill_relaunch(index_path, &canonical_sidecar)
            .with_context(|| {
                format!(
                    "pausing lexical publish after parking prior live generation at {}",
                    canonical_sidecar.display()
                )
            })?;

        // B: move canonical sidecar into `.lexical-publish-backups/` under
        // a unique dated name. Failure here is recoverable without rollback:
        // the live index is NEW (step A already committed), the canonical
        // sidecar holds OLD at a fixed-name handle, and startup recovery
        // will complete the retain step on the next cass invocation. We
        // intentionally do NOT roll the swap back here because (a) rolling
        // back would discard the validated NEW publish, and (b) recovery
        // is idempotent.
        if let Err(retain_err) = rename_lexical_publish_path(
            &canonical_sidecar,
            &retained_backup_path,
            LexicalPublishRenameSite::LinuxRetainCanonicalSidecar,
        ) {
            tracing::warn!(
                error = %retain_err,
                canonical_sidecar = %canonical_sidecar.display(),
                retained_backup_path = %retained_backup_path.display(),
                "published staged lexical index but could not move the prior live artifact into retained-backup storage; canonical sidecar preserved — startup recovery will finish the retain step"
            );
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        publish_via_rename_pair(staged_index_path, index_path, &retained_backup_path)?;
    }

    sync_parent_directory(index_path)?;
    sync_parent_directory(&retained_backup_path)?;
    tracing::info!(
        retained_backup_path = %retained_backup_path.display(),
        live_index_path = %index_path.display(),
        "published staged lexical index and retained the prior live artifact as a backup"
    );

    // Bead 0k0sk: enforce a bounded retention cap on the backups dir so
    // `.lexical-publish-backups/` does not grow unboundedly across many
    // successive publishes. Default: keep only the last K backups
    // (K=1 by default), configurable via CASS_LEXICAL_PUBLISH_BACKUP_RETENTION.
    // A best-effort failure here does NOT fail the publish — the live
    // index is already committed and retained; pruning is cleanup hygiene.
    if let Err(prune_err) = prune_lexical_publish_backups(index_path) {
        tracing::warn!(
            error = %prune_err,
            live_index_path = %index_path.display(),
            "failed to prune old retained lexical-publish backups; disk may not be reclaimed until the next successful prune attempt"
        );
    }
    Ok(())
}

/// Rename-pair publish strategy: park the live index at the canonical
/// sidecar, move the staged index into place, then retain the parked
/// generation under a unique dated name. Used directly on non-Linux
/// platforms and as the EINVAL fallback when Linux's atomic
/// `renameat2(RENAME_EXCHANGE)` is rejected (NFSv3, some older FUSE
/// mounts, certain network exports — see coding_agent_session_search#225).
///
/// Crash window: a single same-filesystem rename is essentially atomic
/// on every modern filesystem we care about, so the window is bounded
/// by the time between the publish rename and the retain rename. A
/// crash in that window leaves the prior-live tree at the canonical
/// sidecar; `recover_or_finalize_interrupted_lexical_publish_backup`
/// finishes the retain step on next startup.
pub(in crate::indexer) fn publish_via_rename_pair(
    staged_index_path: &Path,
    index_path: &Path,
    retained_backup_path: &Path,
) -> Result<()> {
    let in_progress_backup_path = lexical_publish_in_progress_backup_path(index_path);
    rename_lexical_publish_path(
        index_path,
        &in_progress_backup_path,
        LexicalPublishRenameSite::NonLinuxParkPriorLive,
    )
    .with_context(|| {
        format!(
            "parking prior lexical index at {} before staged publish",
            in_progress_backup_path.display()
        )
    })?;
    if let Err(publish_err) = rename_lexical_publish_path(
        staged_index_path,
        index_path,
        LexicalPublishRenameSite::NonLinuxPublishStagedLive,
    ) {
        match rename_lexical_publish_path(
            &in_progress_backup_path,
            index_path,
            LexicalPublishRenameSite::RecoverRestorePriorLive,
        ) {
            Ok(()) => {
                return Err(publish_err).with_context(|| {
                    format!(
                        "publishing staged lexical index {} -> {} failed after parking the prior live index; restored the previous live index",
                        staged_index_path.display(),
                        index_path.display()
                    )
                });
            }
            Err(rollback_err) => {
                return Err(anyhow::anyhow!(
                    "publishing staged lexical index {} -> {} failed after parking the prior live index at {}: {publish_err:#}; rollback also failed: {rollback_err:#}",
                    staged_index_path.display(),
                    index_path.display(),
                    in_progress_backup_path.display()
                ));
            }
        }
    }
    if let Err(retain_err) = rename_lexical_publish_path(
        &in_progress_backup_path,
        retained_backup_path,
        LexicalPublishRenameSite::NonLinuxRetainPriorLive,
    ) {
        tracing::warn!(
            error = %retain_err,
            backup_path = %in_progress_backup_path.display(),
            retained_backup_path = %retained_backup_path.display(),
            "published staged lexical index but could not move the prior live artifact into retained backup storage"
        );
    }
    Ok(())
}

/// How many retained lexical-publish backups to keep on disk. Defaults to
/// 1 (just the most-recent prior-live tree, sufficient for a one-step
/// manual rollback). Operators who want to disable retention entirely can
/// set the env var to 0; operators who want a deeper rollback history can
/// set it to N. Bead 0k0sk.
pub(in crate::indexer) fn lexical_publish_backup_retention_limit() -> usize {
    const DEFAULT_RETENTION: usize = 1;
    dotenvy::var("CASS_LEXICAL_PUBLISH_BACKUP_RETENTION")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_RETENTION)
}

/// Remove retained backups beyond the configured retention cap, oldest
/// first. Safe to call when the backups dir is missing or empty; emits
/// `tracing::info!` per pruned backup with freed-byte accounting so
/// operators can correlate disk recovery. Bead 0k0sk.
pub(in crate::indexer) fn prune_lexical_publish_backups(index_path: &Path) -> Result<()> {
    let retention_limit = lexical_publish_backup_retention_limit();
    let backups_dir = lexical_publish_backups_dir(index_path);
    if !backups_dir.exists() {
        return Ok(());
    }

    // Collect (path, modified_ts, size_bytes). Unreadable entries are
    // skipped with a debug log — we do not let one bad entry block
    // pruning of the rest.
    let mut entries: Vec<(PathBuf, SystemTime, u64)> = Vec::new();
    for entry in fs::read_dir(&backups_dir).with_context(|| {
        format!(
            "reading retained lexical-publish backups dir for pruning {}",
            backups_dir.display()
        )
    })? {
        let entry = match entry {
            Ok(e) => e,
            Err(err) => {
                tracing::debug!(
                    error = %err,
                    backups_dir = %backups_dir.display(),
                    "skipping unreadable entry while enumerating retained lexical-publish backups"
                );
                continue;
            }
        };
        let path = entry.path();
        let metadata = match entry.metadata() {
            Ok(m) => m,
            Err(err) => {
                tracing::debug!(
                    error = %err,
                    path = %path.display(),
                    "skipping retained lexical-publish backup without readable metadata"
                );
                continue;
            }
        };
        let modified = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        let size = directory_size_bytes_best_effort(&path);
        entries.push((path, modified, size));
    }

    if entries.len() <= retention_limit {
        return Ok(());
    }

    // Sort newest-first by modified time (falls back to path ordering
    // on ties, which stabilizes against filesystems without
    // nanosecond resolution).
    entries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

    // Keep the first `retention_limit`, prune the rest.
    let mut pruned_count = 0usize;
    let mut freed_bytes: u64 = 0;
    for (path, _modified, size) in entries.into_iter().skip(retention_limit) {
        match fs::remove_dir_all(&path) {
            Ok(()) => {
                pruned_count = pruned_count.saturating_add(1);
                freed_bytes = freed_bytes.saturating_add(size);
                tracing::info!(
                    pruned_backup_path = %path.display(),
                    freed_bytes = size,
                    retention_limit,
                    "pruned a retained lexical-publish backup beyond retention cap"
                );
            }
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    pruned_backup_path = %path.display(),
                    "failed to prune retained lexical-publish backup; leaving on disk"
                );
            }
        }
    }

    if pruned_count > 0 {
        tracing::info!(
            pruned_count,
            freed_bytes,
            retention_limit,
            backups_dir = %backups_dir.display(),
            "completed lexical-publish retention prune"
        );
    }
    Ok(())
}

/// Best-effort recursive byte count for a directory. Returns 0 on any
/// I/O error — this is purely for tracing, never fails the caller.
pub(in crate::indexer) fn directory_size_bytes_best_effort(path: &Path) -> u64 {
    let mut total: u64 = 0;
    let mut stack: Vec<PathBuf> = vec![path.to_path_buf()];
    while let Some(current) = stack.pop() {
        let Ok(iter) = fs::read_dir(&current) else {
            continue;
        };
        for entry in iter.flatten() {
            let entry_path = entry.path();
            match entry.metadata() {
                Ok(m) if m.is_dir() => stack.push(entry_path),
                Ok(m) => total = total.saturating_add(m.len()),
                Err(_) => continue,
            }
        }
    }
    total
}

pub(in crate::indexer) fn lexical_publish_backups_dir(index_path: &Path) -> PathBuf {
    index_path
        .parent()
        .unwrap_or(index_path)
        .join(".lexical-publish-backups")
}

pub(in crate::indexer) fn lexical_publish_in_progress_backup_path(index_path: &Path) -> PathBuf {
    let file_name = index_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("index");
    index_path.with_file_name(format!(".{file_name}.publish-in-progress.bak"))
}

pub(in crate::indexer) fn unique_lexical_publish_backup_path(index_path: &Path) -> PathBuf {
    let file_name = index_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("index");
    let backup_seed = lexical_publish_backups_dir(index_path).join(file_name);
    unique_atomic_sidecar_path(&backup_seed, "published-bak", "index")
}

pub(in crate::indexer) fn ensure_lexical_publish_backups_dir(index_path: &Path) -> Result<()> {
    let backups_dir = lexical_publish_backups_dir(index_path);
    fs::create_dir_all(&backups_dir).with_context(|| {
        format!(
            "creating retained lexical publish backup directory {}",
            backups_dir.display()
        )
    })?;
    sync_parent_directory(&backups_dir)?;
    Ok(())
}

pub(in crate::indexer) fn remove_lexical_index_dir_placeholder(index_path: &Path) {
    // `index_dir(&data_dir)` calls `fs::create_dir_all` at startup, creating an
    // empty directory before any lexical generation has been published. A first
    // full rebuild then writes its in-progress checkpoint into it, and status may
    // cache the archive fingerprint there. Both are rewritten with the published
    // generation, so a directory holding only them is still a placeholder;
    // retaining it gave a fresh install a "prior-live" publish backup with no
    // index in it. Only those two sidecars are removed. `fs::remove_dir` only
    // succeeds when the directory has zero entries (failing with ENOTEMPTY if
    // any other file or subdirectory exists), so calling it here turns the
    // placeholder back into `!index_path.exists()` without touching a populated
    // live index (dcp01).
    if !fs::symlink_metadata(index_path).is_ok_and(|meta| meta.is_dir()) {
        return;
    }
    let Ok(entries) = fs::read_dir(index_path) else {
        return;
    };
    let placeholder_sidecars = [
        lexical_rebuild_state_path(index_path),
        // ubs:ignore[rust.security.path-traversal] — joins compile-time ARCHIVE_FINGERPRINT_CACHE_FILE constant
        index_path.join(ARCHIVE_FINGERPRINT_CACHE_FILE),
    ];
    let mut sidecars = Vec::new();
    for entry in entries {
        let Ok(entry) = entry else {
            return;
        };
        let path = entry.path();
        if !placeholder_sidecars.contains(&path)
            || !entry.file_type().is_ok_and(|kind| kind.is_file())
        {
            return;
        }
        sidecars.push(path);
    }
    for sidecar in &sidecars {
        if fs::remove_file(sidecar).is_err() {
            return;
        }
    }
    let _ = fs::remove_dir(index_path);
}

pub(in crate::indexer) fn recover_or_finalize_interrupted_lexical_publish_backup(
    index_path: &Path,
) -> Result<()> {
    let in_progress_backup_path = lexical_publish_in_progress_backup_path(index_path);
    if !in_progress_backup_path.exists() {
        return Ok(());
    }

    remove_lexical_index_dir_placeholder(index_path);

    if index_path.exists() {
        ensure_lexical_publish_backups_dir(index_path)?;
        let retained_backup_path = unique_lexical_publish_backup_path(index_path);
        rename_lexical_publish_path(
            &in_progress_backup_path,
            &retained_backup_path,
            LexicalPublishRenameSite::RecoverRetainStaleInProgress,
        )
        .with_context(|| {
            format!(
                "moving stale in-progress lexical publish backup {} into retained backup storage {}",
                in_progress_backup_path.display(),
                retained_backup_path.display()
            )
        })?;
        sync_parent_directory(&retained_backup_path)?;
        tracing::info!(
            backup_path = %retained_backup_path.display(),
            live_index_path = %index_path.display(),
            "retained a stale in-progress lexical publish backup because the live index was already present"
        );
        return Ok(());
    }

    if let Some(parent) = index_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "creating parent directory for interrupted lexical publish recovery {}",
                parent.display()
            )
        })?;
    }
    rename_lexical_publish_path(
        &in_progress_backup_path,
        index_path,
        LexicalPublishRenameSite::RecoverRestorePriorLive,
    )
    .with_context(|| {
        format!(
            "restoring prior published lexical index from interrupted publish backup {} -> {}",
            in_progress_backup_path.display(),
            index_path.display()
        )
    })?;
    sync_parent_directory(index_path)?;
    tracing::warn!(
        recovered_backup_path = %index_path.display(),
        "restored the prior published lexical index from an interrupted staged publish backup"
    );
    Ok(())
}

#[cfg(target_os = "linux")]
pub(crate) use crate::resource_plan::fs_atomic::{
    atomic_exchange_paths, linux_atomic_exchange_is_unsupported,
};

pub(in crate::indexer) struct LexicalRebuildFinalMergeArtifact {
    pub(in crate::indexer) publish_path: PathBuf,
    pub(in crate::indexer) docs: usize,
    // Kept for test assertions + future diagnostics; production code only
    // consumes the path + doc count, but regression tests still read this to
    // prove the segment frontier is not silently remerged.
    #[allow(dead_code)]
    pub(in crate::indexer) segments: usize,
}

#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::indexer) fn finalize_staged_lexical_rebuild_publish_artifact(
    output_path: &Path,
    input_paths: &[PathBuf],
    stage_root: &Path,
    max_parallel_jobs: usize,
) -> Result<LexicalRebuildFinalMergeArtifact> {
    let mut input_artifacts = Vec::with_capacity(input_paths.len());
    for (idx, input_path) in input_paths.iter().enumerate() {
        let summary =
            crate::search::tantivy::searchable_index_summary(input_path)?.ok_or_else(|| {
                anyhow::anyhow!(
                    "staged lexical rebuild artifact is not searchable: {}",
                    input_path.display()
                )
            })?;
        input_artifacts.push(LexicalRebuildShardMergeArtifact {
            first_shard_index: idx,
            last_shard_index: idx,
            index_path: input_path.clone(),
            docs: summary.docs,
            segments: summary.segments,
        });
    }
    finalize_staged_lexical_rebuild_publish_artifact_from_artifacts(
        output_path,
        &input_artifacts,
        stage_root,
        max_parallel_jobs,
    )
}

pub(in crate::indexer) fn finalize_staged_lexical_rebuild_publish_artifact_from_artifacts(
    output_path: &Path,
    input_artifacts: &[LexicalRebuildShardMergeArtifact],
    stage_root: &Path,
    max_parallel_jobs: usize,
) -> Result<LexicalRebuildFinalMergeArtifact> {
    if input_artifacts.is_empty() {
        return Err(anyhow::anyhow!(
            "cannot finalize staged lexical rebuild without at least one merged artifact"
        ));
    }

    if input_artifacts.len() == 1 {
        let artifact = &input_artifacts[0];
        let publish_path = artifact.index_path.clone();
        tracing::info!(
            publish_path = %publish_path.display(),
            "reusing already-final staged lexical rebuild artifact without redundant final merge"
        );
        crate::search::tantivy::validate_searchable_index_contract(&publish_path).with_context(
            || {
                format!(
                    "single-input staged lexical rebuild artifact is not searchable: {}",
                    publish_path.display()
                )
            },
        )?;
        return Ok(LexicalRebuildFinalMergeArtifact {
            publish_path,
            docs: artifact.docs,
            segments: artifact.segments,
        });
    }

    tracing::info!(
        publish_path = %output_path.display(),
        staged_artifacts = input_artifacts.len(),
        "publishing staged lexical rebuild as federated lexical shard set without final assembly collapse"
    );
    let _ = stage_root;
    let _ = max_parallel_jobs;
    let publish_inputs = input_artifacts
        .iter()
        .map(|artifact| {
            (
                artifact.index_path.clone(),
                SearchableIndexSummary {
                    docs: artifact.docs,
                    segments: artifact.segments,
                },
            )
        })
        .collect::<Vec<_>>();
    let summary =
        crate::search::tantivy::publish_federated_searchable_index_directories_with_summaries(
            output_path,
            &publish_inputs,
        )?;
    Ok(LexicalRebuildFinalMergeArtifact {
        publish_path: output_path.to_path_buf(),
        docs: summary.docs,
        segments: summary.segments,
    })
}

#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::indexer) fn merge_lexical_rebuild_shard_index_tree(
    output_path: &Path,
    input_paths: &[PathBuf],
    stage_root: &Path,
    max_parallel_jobs: usize,
) -> Result<crate::search::tantivy::TantivyIndex> {
    const MERGE_FAN_IN: usize = 4;

    if input_paths.is_empty() {
        return Err(anyhow::anyhow!(
            "cannot merge staged lexical rebuild shards without at least one shard index"
        ));
    }

    if input_paths.len() <= MERGE_FAN_IN {
        return crate::search::tantivy::TantivyIndex::merge_compatible_index_directories(
            output_path,
            input_paths,
        );
    }

    let mut current_round_paths = input_paths.to_vec();
    let mut round = 0usize;
    let merge_pool = if max_parallel_jobs > 1 {
        Some(
            ThreadPoolBuilder::new()
                .num_threads(max_parallel_jobs)
                .thread_name(|idx| format!("cass-lexical-merge-{idx}"))
                .build()
                .context("building staged lexical merge thread pool")?,
        )
    } else {
        None
    };
    while current_round_paths.len() > 1 {
        let round_dir = stage_root.join(format!("round-{round:05}"));
        fs::create_dir_all(&round_dir).with_context(|| {
            format!(
                "creating staged lexical merge round directory {}",
                round_dir.display()
            )
        })?;
        let final_round = current_round_paths.len() <= MERGE_FAN_IN;
        let merge_jobs = current_round_paths
            .chunks(MERGE_FAN_IN)
            .enumerate()
            .map(|(group_idx, group_inputs)| LexicalRebuildShardMergeWork {
                output_path: if final_round {
                    output_path.to_path_buf()
                } else {
                    round_dir.join(format!("merge-{group_idx:05}"))
                },
                input_paths: group_inputs.to_vec(),
            })
            .collect::<Vec<_>>();

        tracing::info!(
            merge_round = round,
            merge_jobs = merge_jobs.len(),
            merge_inputs = current_round_paths.len(),
            merge_fan_in = MERGE_FAN_IN,
            merge_workers = max_parallel_jobs.max(1),
            "running staged lexical rebuild merge round"
        );

        let merge_job = |job: LexicalRebuildShardMergeWork| {
            crate::search::tantivy::TantivyIndex::merge_compatible_index_directories(
                &job.output_path,
                &job.input_paths,
            )
            .with_context(|| {
                format!(
                    "merging {} staged lexical shard indices into {}",
                    job.input_paths.len(),
                    job.output_path.display()
                )
            })?;
            Ok::<PathBuf, anyhow::Error>(job.output_path)
        };
        let merge_jobs_parallel = merge_jobs.clone();
        let run_merge_jobs_parallel = move || {
            merge_jobs_parallel
                .into_par_iter()
                .map(merge_job)
                .collect::<Result<Vec<_>>>()
        };
        let run_merge_jobs_sequential = move || {
            merge_jobs
                .into_iter()
                .map(merge_job)
                .collect::<Result<Vec<_>>>()
        };

        current_round_paths = match &merge_pool {
            Some(pool) => pool.install(run_merge_jobs_parallel)?,
            None => run_merge_jobs_sequential()?,
        };
        round = round.saturating_add(1);
    }

    crate::search::tantivy::TantivyIndex::open_or_create(output_path)
}
