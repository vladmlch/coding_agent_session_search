#[allow(unused_imports)]
use crate::franken_sync::compat::{
    ConnectionExt, OpenFlags as FrankenOpenFlags, RowExt,
    open_with_flags as open_franken_with_flags,
};
#[allow(unused_imports)]
use crate::indexer::IndexOptions;
#[allow(unused_imports)]
use crate::model::cli_error_kind::ErrorKind as CliErrorKind;
#[allow(unused_imports)]
use crate::robot_budget_envelope::BudgetSnapshot;
#[allow(unused_imports)]
use crate::{cli_app::*, cli_root::*, lib_doctor::*, *};
#[allow(unused_imports)]
use anyhow::Result;
#[allow(unused_imports)]
use base64::prelude::*;
#[allow(unused_imports)]
use chrono::Utc;
#[allow(unused_imports)]
use clap::{Arg, ArgAction, Command, CommandFactory, Parser, Subcommand, ValueEnum, ValueHint};
#[allow(unused_imports)]
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
#[allow(unused_imports)]
use std::fs::{File, OpenOptions};
#[allow(unused_imports)]
use std::io::{self, BufRead, BufReader, IsTerminal, Read, Seek, SeekFrom, Write};
#[allow(unused_imports)]
use std::path::{Path, PathBuf};
#[allow(unused_imports)]
use std::sync::{Arc, Mutex};
#[allow(unused_imports)]
use std::time::{Duration, Instant};
#[allow(unused_imports)]
use tracing::{info, warn};
#[allow(unused_imports)]
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

pub(crate) fn index_result_counts_from_progress(
    progress: &indexer::IndexingProgress,
) -> Option<(i64, i64)> {
    let stats = progress.stats.lock().ok()?;
    if !stats.total_counts_exact {
        return None;
    }
    Some((
        i64::try_from(stats.total_conversations).unwrap_or(i64::MAX),
        i64::try_from(stats.total_messages).unwrap_or(i64::MAX),
    ))
}

/// True only when it is PROVEN that the canonical archive holds no messages:
/// the DB file is absent, or it opens read-only and reports no message id.
/// Any open error keeps the strict postcondition (an unreadable archive is
/// not evidence of an empty one).
pub(crate) fn canonical_archive_has_no_messages(db_path: &Path) -> bool {
    if !db_path.exists() {
        return true;
    }
    crate::storage::sqlite::FrankenStorage::open_readonly(db_path)
        .and_then(|storage| storage.max_message_id())
        .is_ok_and(|max_id| max_id.is_none())
}

/// Re-prove a completed full index against the final, closed DB/WAL bytes and
/// cache that exact fingerprint for lightweight status/search projections.
/// Large archives retain the existing bounded-doctor behavior: an intentionally
/// deferred deep probe leaves storage state `unchecked` instead of inventing a
/// pass. An affirmative failure is a failed index postcondition.
pub(crate) fn persist_post_index_integrity_attestation(
    data_dir: &Path,
    db_path: &Path,
) -> CliResult<()> {
    let bundle_bytes = doctor_archive_bundle_bytes(db_path);
    let timeout = doctor_archive_db_probe_hard_timeout(bundle_bytes);
    let conn = open_franken_cli_read_db_with_hard_timeout(
        db_path.to_path_buf(),
        "post-index integrity attestation",
        Duration::from_secs(30),
    )
    .map_err(|error| CliError {
        code: 9,
        kind: CliErrorKind::Index.kind_str(),
        message: format!(
            "indexing returned success but the final archive could not be opened for post-close integrity attestation: {}",
            error.message
        ),
        hint: Some(
            "The canonical database was preserved. Inspect `cass doctor check --json` before retrying the full index."
                .to_string(),
        ),
        retryable: error.retryable,
    })?;

    match run_bounded_doctor_archive_db_probe(conn, db_path, timeout) {
        DoctorBoundedArchiveDbProbeOutcome::TimedOut { phase } => {
            tracing::warn!(
                phase,
                timeout_secs = timeout.as_secs(),
                "post-index integrity attestation timed out; storage readiness remains unchecked"
            );
            Ok(())
        }
        DoctorBoundedArchiveDbProbeOutcome::Completed(probe) => {
            if let Some(reason) = probe.integrity_skipped_reason {
                tracing::info!(
                    reason = %reason,
                    "post-index integrity attestation was deliberately deferred"
                );
                return Ok(());
            }
            match probe.integrity {
                Some(Ok(integrity)) if integrity.is_ok() => {
                    crate::search::storage_integrity::store_integrity_attestation(
                        data_dir,
                        db_path,
                        crate::search::storage_integrity::IntegrityAttestationVerdict::Pass,
                        "integrity_check",
                        None,
                    )
                    .ok_or_else(|| CliError {
                        code: 9,
                        kind: CliErrorKind::Index.kind_str(),
                        message: "indexing returned success but the final archive fingerprint could not be captured for its completed integrity attestation".to_string(),
                        hint: Some(
                            "The canonical database was preserved. Inspect `cass status --json`; storage readiness will remain unchecked until a matching attestation can be captured."
                                .to_string(),
                        ),
                        retryable: true,
                    })?;
                    Ok(())
                }
                Some(Ok(integrity)) => Err(CliError {
                    code: 9,
                    kind: CliErrorKind::Index.kind_str(),
                    message: format!(
                        "indexing returned success but the final archive failed frankensqlite {}: {}",
                        integrity.failed_pragma_name(),
                        integrity.diagnostic_summary()
                    ),
                    hint: Some(
                        "The canonical database was preserved. Run `cass doctor check --json` before any further indexing or repair."
                            .to_string(),
                    ),
                    retryable: false,
                }),
                Some(Err(error)) => Err(CliError {
                    code: 9,
                    kind: CliErrorKind::Index.kind_str(),
                    message: format!(
                        "indexing returned success but the final archive integrity probe could not establish a verdict: {error}"
                    ),
                    hint: Some(
                        "The canonical database was preserved. Run `cass doctor check --json` to inspect the archive."
                            .to_string(),
                    ),
                    retryable: true,
                }),
                None => Err(CliError {
                    code: 9,
                    kind: CliErrorKind::Index.kind_str(),
                    message: "indexing returned success but the final archive integrity probe returned no verdict".to_string(),
                    hint: Some(
                        "The canonical database was preserved. Run `cass doctor check --json` to inspect the archive."
                            .to_string(),
                    ),
                    retryable: true,
                }),
            }
        }
    }
}

pub(crate) fn validate_successful_index_artifacts(
    data_dir: &Path,
    db_path: &Path,
    full: bool,
    force_rebuild: bool,
) -> CliResult<()> {
    let index_path = crate::search::tantivy::expected_index_dir(data_dir);
    if !crate::search::tantivy::searchable_index_exists(&index_path) {
        // An incremental / watch-once run that found nothing to publish (no
        // canonical messages at all — e.g. a targeted path that was not a
        // session, or a skipped active source into a fresh data dir) has no
        // lexical asset to validate; only a full/forced rebuild, or a run
        // over an archive that does hold messages, is a genuine
        // "success with missing assets" (gh#422 postcondition).
        if !full && !force_rebuild && canonical_archive_has_no_messages(db_path) {
            return Ok(());
        }
        return Err(CliError {
            code: 9,
            kind: CliErrorKind::Index.kind_str(),
            message: format!(
                "indexing returned success but no readable lexical index exists at {}",
                index_path.display()
            ),
            hint: Some(
                "The canonical database was preserved. Inspect `cass status --json`; retry `cass index --full --force-rebuild --json` after addressing the reported lexical publish failure."
                    .to_string(),
            ),
            retryable: true,
        });
    }
    crate::search::tantivy::validate_searchable_index_contract(&index_path).map_err(|err| {
        CliError {
            code: 9,
            kind: CliErrorKind::Index.kind_str(),
            message: format!(
                "indexing returned success but the published lexical index at {} is unusable: {err:#}",
                index_path.display()
            ),
            hint: Some(
                "The canonical database was preserved. Inspect `cass status --json` and retry a full forced rebuild after resolving the publish error."
                    .to_string(),
            ),
            retryable: true,
        }
    })?;

    if full || force_rebuild {
        let checkpoint = crate::indexer::load_lexical_rebuild_checkpoint(&index_path).map_err(
            |err| CliError {
                code: 9,
                kind: CliErrorKind::Index.kind_str(),
                message: format!(
                    "indexing returned success but its lexical checkpoint cannot be read: {err:#}"
                ),
                hint: Some(
                    "The canonical database was preserved. Retry the full rebuild; a successful run must publish a readable completed checkpoint."
                        .to_string(),
                ),
                retryable: true,
            },
        )?;
        let checkpoint = checkpoint.ok_or_else(|| CliError {
            code: 9,
            kind: CliErrorKind::Index.kind_str(),
            message: "indexing returned success but the full rebuild published no lexical checkpoint"
                .to_string(),
            hint: Some(
                "The canonical database was preserved. Retry the full rebuild; success requires a completed checkpoint."
                    .to_string(),
            ),
            retryable: true,
        })?;
        let checkpoint_valid = checkpoint.completed
            && stored_path_identity_matches(&checkpoint.db_path, db_path)
            && checkpoint.schema_hash == crate::search::tantivy::SCHEMA_HASH
            && crate::indexer::lexical_rebuild_page_size_is_compatible(checkpoint.page_size)
            && checkpoint.processed_conversations == checkpoint.total_conversations
            && usize::try_from(checkpoint.committed_offset).ok()
                == Some(checkpoint.total_conversations)
            && crate::indexer::completed_lexical_storage_fingerprint_matches_total(
                &checkpoint.storage_fingerprint,
                checkpoint.total_conversations,
            );
        if !checkpoint_valid {
            return Err(CliError {
                code: 9,
                kind: CliErrorKind::Index.kind_str(),
                message: format!(
                    "indexing returned success but the full rebuild checkpoint is incomplete or incompatible (completed={}, db_path={}, schema_hash={}, page_size={}, processed_conversations={}, total_conversations={}, committed_offset={}, storage_fingerprint={})",
                    checkpoint.completed,
                    checkpoint.db_path,
                    checkpoint.schema_hash,
                    checkpoint.page_size,
                    checkpoint.processed_conversations,
                    checkpoint.total_conversations,
                    checkpoint.committed_offset,
                    checkpoint.storage_fingerprint,
                ),
                hint: Some(
                    "The canonical database was preserved. Inspect `cass status --json`; the rebuild must publish a completed checkpoint for this database before success can be reported."
                        .to_string(),
                ),
                retryable: true,
            });
        }
        persist_post_index_integrity_attestation(data_dir, db_path)?;
    }

    Ok(())
}

// FrankenSQLite's debug-mode async open futures can exceed the 2 MiB default
// worker stack on Linux as well as the 1 MiB MSVC default. Keep the explicit
// bound on every platform so `cass index` does not depend on RUST_MIN_STACK or
// release-mode frame shrinking just to open the canonical archive.
pub(crate) const INDEX_WORKER_STACK_SIZE_BYTES: usize = 16 * 1024 * 1024;

pub(crate) fn index_worker_thread_builder() -> std::thread::Builder {
    std::thread::Builder::new()
        .name("cass-index-worker".to_string())
        .stack_size(INDEX_WORKER_STACK_SIZE_BYTES)
}

/// Maximum time for an indexing worker to reach a safe boundary after a signal.
pub(crate) const INDEX_SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// Poll cancel-safe Asupersync signal streams from the existing CLI progress
/// loop. The first signal lets the index worker drain its current batch; a
/// repeated signal or an expired grace period ends a worker that cannot reach
/// a cancellation checkpoint (GH #515).
pub(crate) struct IndexShutdownSignals {
    pub(crate) streams: Vec<(asupersync::signal::Signal, i32)>,
    pub(crate) received: Option<i32>,
    pub(crate) received_at: Option<Instant>,
    pub(crate) repeated: bool,
}

impl IndexShutdownSignals {
    pub(crate) fn new(enabled: bool) -> std::io::Result<Self> {
        use asupersync::signal::{SignalKind, signal};
        let streams = if enabled && cfg!(any(unix, windows)) {
            vec![
                (signal(SignalKind::interrupt())?, 130),
                (signal(SignalKind::terminate())?, 143),
            ]
        } else {
            Vec::new()
        };
        Ok(Self {
            streams,
            received: None,
            received_at: None,
            repeated: false,
        })
    }

    pub(crate) fn poll_exit_code(&mut self) -> Option<i32> {
        use std::future::Future;
        use std::task::{Context, Poll, Waker};
        let mut context = Context::from_waker(Waker::noop());
        for (signal, code) in &mut self.streams {
            let mut receive = std::pin::pin!(signal.recv());
            if matches!(receive.as_mut().poll(&mut context), Poll::Ready(Some(()))) {
                if self.received.is_some() {
                    self.repeated = true;
                } else {
                    self.received = Some(*code);
                    self.received_at = Some(Instant::now());
                }
            }
        }
        self.received
    }

    pub(crate) fn poll(&mut self, progress: &indexer::IndexingProgress) {
        let already_received = self.received.is_some();
        if let Some(code) = self.poll_exit_code() {
            progress.request_stop();
            if !already_received {
                tracing::info!(
                    exit_code = code,
                    grace_secs = INDEX_SHUTDOWN_GRACE.as_secs(),
                    "index shutdown requested; draining current work before exit"
                );
            }
        }
    }

    /// Only call while the worker is still running. A worker that drained in
    /// time follows the ordinary joined-result path, which also preserves the
    /// signal's exit code when its last batch happened to finish successfully.
    pub(crate) fn abort_if_needed(&self, elapsed_ms: u128) {
        let (Some(code), Some(received_at)) = (self.received, self.received_at) else {
            return;
        };
        let reason = if self.repeated {
            "a second shutdown signal was received".to_string()
        } else if received_at.elapsed() >= INDEX_SHUTDOWN_GRACE {
            format!(
                "the index worker did not stop within {} seconds",
                INDEX_SHUTDOWN_GRACE.as_secs()
            )
        } else {
            return;
        };
        // Literal `code:` fields and literal `process::exit` arguments are
        // load-bearing: the emitted-exit-code scanners
        // (spec_exit_codes_documented, golden_error_envelope) only see
        // `code: <digits>` directly above `kind:` and `process::exit(<digits>)`
        // calls — not signal-carried `received` variables.
        let error = match code {
            143 => CliError {
                code: 143,
                kind: CliErrorKind::Index.kind_str(),
                message: format!(
                    "index interrupted: {reason}; exiting without waiting for the worker"
                ),
                hint: Some(
                    "Committed work and the database WAL are retained. Rerun the same index command to resume from committed source observations."
                        .to_string(),
                ),
                retryable: true,
            },
            _ => CliError {
                code: 130,
                kind: CliErrorKind::Index.kind_str(),
                message: format!(
                    "index interrupted: {reason}; exiting without waiting for the worker"
                ),
                hint: Some(
                    "Committed work and the database WAL are retained. Rerun the same index command to resume from committed source observations."
                        .to_string(),
                ),
                retryable: true,
            },
        };
        // A closed stderr must not panic and divert forced cancellation into
        // unwinding before the process exit releases the worker's locks.
        {
            use std::io::Write as _;
            let mut stderr = std::io::stderr().lock();
            let _ = writeln!(stderr, "{}", cli_error_json_payload(&error, elapsed_ms));
            let _ = stderr.flush();
        }
        // The worker may be blocked inside a storage operation. Opening a new
        // connection or waiting for destructors here can block on that same
        // operation and swallow SIGTERM again. Exit releases its OS locks;
        // committed WAL frames remain available to the next indexing run.
        match code {
            143 => std::process::exit(143),
            _ => std::process::exit(130),
        }
    }
}

/// Run an incremental index pass before a TUI or Search invocation when the
/// user passes `--refresh` (alias `--catch-up`). Mirrors `cass index` with
/// `full=false`, `force_rebuild=false`, `watch=false`, `semantic=false` so it
/// does the minimum work needed to catch up.
///
/// Progress is emitted as one-line updates on stderr whenever the indexer's
/// phase or batch counter changes (matching the "plain" progress style the
/// issue asked for). This avoids a silent multi-second wait when launching
/// the TUI or running a search against a slightly stale index.
///
/// Ordinary returned errors are non-fatal: emit a warning and continue against
/// the existing index. A watchdog-confirmed no-progress stall is different:
/// the worker cannot be detached without retaining `index-run.lock`, so the
/// process exits 70 after the configured bound and releases every live lock.
pub(crate) fn refresh_index_inline(
    db_override: Option<PathBuf>,
    data_dir_override: Option<PathBuf>,
) {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    let data_dir = resolve_data_dir(&data_dir_override, db_override.as_ref());
    let db_path = db_override.unwrap_or_else(|| data_dir.join("agent_search.db"));
    let progress = Arc::new(indexer::IndexingProgress::default());
    let opts = indexer::IndexOptions {
        full: false,
        force_rebuild: false,
        watch: false,
        watch_once_paths: None,
        db_path,
        data_dir,
        semantic: false,
        build_hnsw: false,
        embedder: "fastembed".to_string(),
        progress: Some(Arc::clone(&progress)),
        watch_interval_secs: 30,
    };
    eprintln!("Refreshing index...");

    // Run the indexer on a dedicated thread so the main thread can poll the
    // progress counters and emit plain-text status lines while it runs. We
    // move the whole opts struct (it contains the shared progress handle).
    let watchdog_data_dir = opts.data_dir.clone();
    let index_handle =
        match index_worker_thread_builder().spawn(move || indexer::run_index(opts, None)) {
            Ok(handle) => handle,
            Err(error) => {
                eprintln!("Warning: failed to start the refresh index worker: {error}");
                return;
            }
        };

    // Sentinels are `usize::MAX` so the first observed atomic values always
    // differ and trigger the first print. Using `0` for `last_total` would
    // collide with the legitimate "haven't set total yet" state.
    let mut last_phase = usize::MAX;
    let mut last_current = usize::MAX;
    let mut last_total = usize::MAX;
    let started = Instant::now();
    let mut stall_watchdog =
        IndexStallWatchdog::aborting_lexical_phases(watchdog_data_dir, Duration::from_secs(1));
    while !index_handle.is_finished() {
        let phase = progress.phase.load(Ordering::Relaxed);
        let current = progress.current.load(Ordering::Relaxed);
        let total = progress.total.load(Ordering::Relaxed);
        if phase != last_phase || current != last_current || total != last_total {
            let phase_str = indexer::IndexingProgress::phase_label_for(phase);
            if total > 0 {
                eprintln!("  {phase_str}: {current}/{total}");
            } else {
                eprintln!("  {phase_str}...");
            }
            last_phase = phase;
            last_current = current;
            last_total = total;
        }
        if let Some(payload) = stall_watchdog.observe(&progress, started.elapsed().as_millis()) {
            let (summary, diagnostics) = index_stall_warning_lines(&payload);
            eprintln!("{summary}");
            eprintln!("{diagnostics}");
            abort_after_index_stall_if_requested(&payload, stall_watchdog.data_dir());
        }
        std::thread::sleep(Duration::from_millis(500));
    }

    // One final poll so the terminal reflects the final counters instead of
    // whatever the last mid-run sample happened to catch (e.g. 140/145 right
    // before completion would otherwise jump straight to `done.`).
    let final_phase = progress.phase.load(Ordering::Relaxed);
    let final_current = progress.current.load(Ordering::Relaxed);
    let final_total = progress.total.load(Ordering::Relaxed);
    if (final_phase != last_phase || final_current != last_current || final_total != last_total)
        && final_total > 0
    {
        let phase_str = indexer::IndexingProgress::phase_label_for(final_phase);
        eprintln!("  {phase_str}: {final_current}/{final_total}");
    }

    match index_handle.join() {
        Ok(Ok(())) => {
            eprintln!("  done.");
        }
        Ok(Err(e)) => {
            eprintln!("Warning: --refresh indexing failed: {e}. Continuing with existing index.");
            tracing::warn!(error = %e, "refresh_index_inline failed; proceeding with stale index");
        }
        Err(panic_payload) => {
            // Try to recover a useful panic message so the user (and logs)
            // actually see what broke. `std::panic::catch_unwind`-style
            // payloads are most commonly `String` or `&'static str`; anything
            // else falls back to a placeholder.
            let msg = panic_payload
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic_payload.downcast_ref::<&'static str>().copied())
                .unwrap_or("<non-string panic payload>");
            eprintln!(
                "Warning: --refresh indexing panicked: {msg}. Continuing with existing index."
            );
            tracing::warn!(
                panic = msg,
                "refresh_index_inline indexer thread panicked; proceeding with stale index"
            );
        }
    }
}

/// Collect optional on-disk forensic context on a separate thread. Even a
/// metadata lookup can block in the kernel on the filesystem being diagnosed
/// (GH #538), so the watchdog must never call this function synchronously.
/// Missing or unreadable paths remain best-effort diagnostic failures.
pub(crate) fn collect_stall_diagnostics(data_dir: &Path) -> serde_json::Value {
    // Keep the snapshot compact — this is an observability event on stderr,
    // not a full forensic bundle. If the reporter needs more, the event hint
    // already tells them how to grab a stack trace.
    const MAX_CHECKPOINT_BYTES: u64 = 64 * 1024;

    let mut out = serde_json::Map::new();
    let index_dir = data_dir.join("index");
    out.insert(
        "index_dir".into(),
        serde_json::json!(index_dir.display().to_string()),
    );
    out.insert(
        "index_dir_exists".into(),
        serde_json::json!(index_dir.is_dir()),
    );

    // Candidate checkpoint filenames observed across cass versions. Recording
    // any we find means the hang reporter doesn't need to hunt for the right
    // name on their host.
    let checkpoint_candidates = [
        "lexical_rebuild_state.json",
        "lexical_rebuild_state.v2.json",
    ];
    let mut checkpoints = serde_json::Map::new();
    for name in checkpoint_candidates {
        let path = index_dir.join(name);
        if !path.exists() {
            continue;
        }
        let meta = std::fs::metadata(&path).ok();
        let size = meta.as_ref().map(std::fs::Metadata::len).unwrap_or(0);
        let modified_ms = meta
            .as_ref()
            .and_then(|m| m.modified().ok())
            .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as u64);
        let mut entry = serde_json::Map::new();
        entry.insert("path".into(), serde_json::json!(path.display().to_string()));
        entry.insert("size_bytes".into(), serde_json::json!(size));
        if let Some(ms) = modified_ms {
            entry.insert("modified_ms".into(), serde_json::json!(ms));
        }
        if size <= MAX_CHECKPOINT_BYTES {
            match read_bounded_stall_diagnostic(&path, MAX_CHECKPOINT_BYTES) {
                Ok(contents) => {
                    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&contents) {
                        entry.insert("content".into(), parsed);
                    } else {
                        entry.insert("content_raw".into(), serde_json::json!(contents));
                    }
                }
                Err(err) => {
                    entry.insert("read_error".into(), serde_json::json!(err.to_string()));
                }
            }
        } else {
            entry.insert(
                "content_omitted".into(),
                serde_json::json!("file larger than 64 KiB; dump manually with `cat`"),
            );
        }
        checkpoints.insert(name.to_string(), serde_json::Value::Object(entry));
    }
    out.insert(
        "lexical_rebuild_checkpoint".into(),
        serde_json::Value::Object(checkpoints),
    );

    // Tantivy segment count at stall time — catches the producer/consumer
    // mismatch where segments are being written but the checkpoint still
    // reports zero processed conversations.
    if let Ok(entries) = std::fs::read_dir(&index_dir) {
        let mut segment_count: u64 = 0;
        let mut segment_bytes: u64 = 0;
        for entry in entries.flatten() {
            if let Some(ext) = entry.path().extension().and_then(|e| e.to_str())
                && matches!(ext, "idx" | "pos" | "fast" | "term" | "store" | "fieldnorm")
            {
                segment_count = segment_count.saturating_add(1);
                if let Ok(meta) = entry.metadata() {
                    segment_bytes = segment_bytes.saturating_add(meta.len());
                }
            }
        }
        out.insert(
            "tantivy_segment_files".into(),
            serde_json::json!(segment_count),
        );
        out.insert(
            "tantivy_segment_bytes".into(),
            serde_json::json!(segment_bytes),
        );
    }

    // Index run lock file — if heartbeating, tells the reporter another
    // process may actually be holding the writer.
    let lock_path = data_dir.join("index-run.lock");
    if lock_path.exists() {
        let mut lock = serde_json::Map::new();
        lock.insert(
            "path".into(),
            serde_json::json!(lock_path.display().to_string()),
        );
        if let Ok(meta) = std::fs::metadata(&lock_path) {
            lock.insert("size_bytes".into(), serde_json::json!(meta.len()));
            if let Ok(modified) = meta.modified()
                && let Ok(duration) = modified.duration_since(std::time::UNIX_EPOCH)
            {
                lock.insert(
                    "modified_ms".into(),
                    serde_json::json!(duration.as_millis() as u64),
                );
            }
        }
        if let Ok(contents) = read_bounded_stall_diagnostic(&lock_path, MAX_CHECKPOINT_BYTES) {
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&contents) {
                lock.insert("content".into(), parsed);
            } else {
                lock.insert("content_raw".into(), serde_json::json!(contents));
            }
        }
        out.insert("index_run_lock".into(), serde_json::Value::Object(lock));
    }

    serde_json::Value::Object(out)
}

pub(crate) fn read_bounded_stall_diagnostic(
    path: &Path,
    max_bytes: u64,
) -> std::io::Result<String> {
    use std::io::Read;

    // A pre-read metadata length is not a bound: the file can grow or be
    // replaced between stat and open. Keep the diagnostic worker's allocation
    // bounded as well as keeping its I/O off the watchdog thread.
    let mut contents = String::new();
    std::fs::File::open(path)?
        .take(max_bytes.saturating_add(1))
        .read_to_string(&mut contents)?;
    if contents.len() as u64 > max_bytes {
        return Err(std::io::Error::other("diagnostic file exceeds byte limit"));
    }
    Ok(contents)
}
