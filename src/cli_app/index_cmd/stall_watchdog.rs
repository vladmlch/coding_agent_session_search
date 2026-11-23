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

/// At most one filesystem probe belongs to a watchdog, even if an earlier
/// probe is kernel-blocked and indexing later resumes. Its receiver is only
/// polled: diagnostic collection never delays a warning or the abort deadline.
#[derive(Default)]
pub(crate) enum IndexStallFilesystemProbe {
    #[default]
    NotStarted,
    Pending {
        receiver: std::sync::mpsc::Receiver<(serde_json::Value, i64)>,
        started: std::time::Instant,
        started_at_ms: i64,
    },
    Complete {
        diagnostics: serde_json::Value,
        started_at_ms: i64,
        completed_at_ms: i64,
    },
    Failed(String),
}

impl IndexStallFilesystemProbe {
    pub(crate) fn reset_after_progress(&mut self) {
        // Never abandon an in-flight worker and spawn another: a stuck volume
        // must not accumulate diagnostic threads over repeated watch cycles.
        // A result already buffered in the channel belongs to the old stall;
        // discard it now so a later stall starts a fresh filesystem snapshot.
        let still_running = matches!(
            self,
            Self::Pending { receiver, .. }
                if matches!(receiver.try_recv(), Err(std::sync::mpsc::TryRecvError::Empty))
        );
        if !still_running {
            *self = Self::NotStarted;
        }
    }

    pub(crate) fn snapshot(&mut self, data_dir: &Path) -> serde_json::Value {
        if matches!(self, Self::NotStarted) {
            let (sender, receiver) = std::sync::mpsc::sync_channel(1);
            let probe_dir = data_dir.to_path_buf();
            let started = std::time::Instant::now();
            let started_at_ms = chrono::Utc::now().timestamp_millis();
            *self = match std::thread::Builder::new()
                .name("cass-stall-diagnostics".to_string())
                .spawn(move || {
                    let diagnostics = collect_stall_diagnostics(&probe_dir);
                    let _ = sender.send((diagnostics, chrono::Utc::now().timestamp_millis()));
                }) {
                Ok(_worker) => Self::Pending {
                    receiver,
                    started,
                    started_at_ms,
                },
                Err(error) => Self::Failed(error.to_string()),
            };
        }

        if let Self::Pending {
            receiver,
            started_at_ms,
            ..
        } = self
        {
            match receiver.try_recv() {
                Ok((diagnostics, completed_at_ms)) => {
                    *self = Self::Complete {
                        diagnostics,
                        started_at_ms: *started_at_ms,
                        completed_at_ms,
                    };
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => {}
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    *self = Self::Failed("diagnostic worker exited without a result".to_string());
                }
            }
        }

        let mut diagnostics = match self {
            Self::Complete { diagnostics, .. } => diagnostics.clone(),
            _ => serde_json::json!({"index_dir": data_dir.join("index").display().to_string()}),
        };
        diagnostics["filesystem_probe"] = match self {
            Self::Pending {
                started,
                started_at_ms,
                ..
            } => serde_json::json!({
                "status": "pending",
                "started_at_ms": *started_at_ms,
                "elapsed_ms": started.elapsed().as_millis() as u64,
            }),
            Self::Complete {
                started_at_ms,
                completed_at_ms,
                ..
            } => serde_json::json!({
                "status": "complete",
                "started_at_ms": *started_at_ms,
                "completed_at_ms": *completed_at_ms,
            }),
            Self::Failed(error) => serde_json::json!({"status": "failed", "error": error}),
            Self::NotStarted => serde_json::json!({"status": "not_started"}),
        };
        // Always take this in-memory snapshot now, independently of the
        // optional filesystem result. No stat or debug symbols are needed to
        // identify the operation that is still blocking the indexing thread.
        diagnostics["raw_mirror_io"] = crate::raw_mirror::raw_mirror_io_snapshot();
        diagnostics
    }
}

pub(crate) const INDEX_STALL_HINT: &str = concat!(
    "Indexer made no forward progress for the configured stall window. ",
    "For raw-mirror stalls, diagnostics.raw_mirror_io names the in-flight filesystem operation, path and elapsed_ms. ",
    "Filesystem diagnostics run separately: filesystem_probe.status=pending means the probe has not completed, not that assets are absent. ",
    "Capture a stack trace with `sudo cat /proc/$(pgrep -f 'cass index')/stack` ",
    "and/or `sudo gdb -batch -ex 'thread apply all bt' -p $(pgrep -f 'cass index') 2>/dev/null | head -200` ",
    "and attach it to a new GitHub issue (say whether it is an indexing-phase wedge or a watch_startup wedge ",
    "where the lock-file heartbeat keeps refreshing while one thread spins). Semantic replay, embedding, vector publish, manifest, ",
    "and finalize phases expose their own counters and are report-only; native HNSW construction is explicitly ",
    "labelled but opaque. Set CASS_INDEX_STALL_DETECT_SECS=0 to disable detection; set ",
    "CASS_INDEX_STALL_ABORT_SECS=0 to keep abort-eligible lexical stalls report-only; set ",
    "CASS_INDEX_STALL_ABORT_ALL_PHASES=1 to promote these report-only warnings to a hard abort ",
    "in every phase, report-only lanes included (#437). ",
    "If `finalizing` is true in the diagnostics the indexer is inside post-publish finalize work that cannot ",
    "report per-row progress: the final WAL checkpoint of a large deferred bulk-ingest WAL (slow on macOS), ",
    "the fallback-FTS shadow parity probe/rebuild that follows a `--full` publish (#439), or a post-run ",
    "segment fold; CASS_INDEX_FINALIZE_ABORT_SECS (default 1800) bounds that window, ",
    "and CASS_INDEX_WRITER_WAL_AUTOCHECKPOINT_PAGES=1000 caps WAL growth so the final checkpoint stays small. ",
    "A `preparing` stall with `pre_index_io_active` true is a slow canonical open/scan doing real disk IO ",
    "(for example replaying a multi-GiB WAL, #382); it gets the same bounded grace."
);

/// gh373/oeu5a: phase-2 hint for a stall observed while `persist_in_progress`
/// is set — the giant-conversation persist window, NOT a finalize-WAL
/// checkpoint. The generic hint's finalize-WAL guidance sent operators to
/// `CASS_INDEX_WRITER_WAL_AUTOCHECKPOINT_PAGES`, which is irrelevant here and
/// cost a real investigation a decoy environment variable.
pub(crate) const INDEX_STALL_HINT_PERSIST: &str = concat!(
    "Indexer counters are frozen but a batched persist is ACTIVE ",
    "(`persist_in_progress: true`): one very large conversation (multi-MB ",
    "single-session transcript, tens of thousands of messages) is inside ",
    "redaction/map or its writer transaction. This is live work, not a wedge ",
    "— the watchdog grants it the CASS_INDEX_FINALIZE_ABORT_SECS-class grace ",
    "(default 1800s) and per-message heartbeats normally keep it from even ",
    "reporting. If this stall repeats with no `activity` movement at all, ",
    "capture a stack trace (`sudo gdb -batch -ex 'thread apply all bt' -p ",
    "$(pgrep -f 'cass index')`) and attach it to issue #373. Memory pressure ",
    "multiplies redaction time on giant conversations; re-running on a ",
    "quieter machine is the fastest operational fix. ",
    "CASS_INDEX_STALL_DETECT_SECS / CASS_INDEX_STALL_ABORT_SECS tune or ",
    "disable detection/abort."
);

pub(crate) fn index_stall_threshold(progress_interval: Duration) -> Option<Duration> {
    let stall_threshold_secs = dotenvy::var("CASS_INDEX_STALL_DETECT_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(120);
    if stall_threshold_secs == 0 {
        None
    } else {
        let min = progress_interval.saturating_add(Duration::from_secs(1));
        Some(Duration::from_secs(stall_threshold_secs).max(min))
    }
}

/// #437: does `CASS_INDEX_STALL_ABORT_ALL_PHASES` request promoting the stall
/// watchdog's report-only warnings to hard aborts in every phase? Pure parse
/// half so the truthiness contract is unit-testable without touching the
/// process environment.
pub(crate) fn stall_abort_all_phases_flag(raw: Option<&str>) -> bool {
    raw.map(str::trim).is_some_and(|v| {
        v == "1"
            || v.eq_ignore_ascii_case("true")
            || v.eq_ignore_ascii_case("yes")
            || v.eq_ignore_ascii_case("on")
    })
}

/// #437: opt-in env that extends the existing stall-abort containment
/// (`CASS_INDEX_STALL_ABORT_SECS`) to EVERY phase the stall detector reports
/// on — including the scribe/accumulate and semantic lanes that are
/// report-only by default. Default off preserves current behavior exactly.
pub(crate) fn index_stall_abort_all_phases_enabled() -> bool {
    stall_abort_all_phases_flag(
        dotenvy::var("CASS_INDEX_STALL_ABORT_ALL_PHASES")
            .ok()
            .as_deref(),
    )
}

pub(crate) fn index_stall_abort_threshold(
    progress_interval: Duration,
    report_threshold: Option<Duration>,
) -> Option<Duration> {
    let report_threshold = report_threshold?;
    let abort_threshold_secs = dotenvy::var("CASS_INDEX_STALL_ABORT_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(300);
    if abort_threshold_secs == 0 {
        return None;
    }
    let min_after_report = report_threshold
        .checked_add(progress_interval.max(Duration::from_secs(1)))
        .unwrap_or(report_threshold);
    Some(Duration::from_secs(abort_threshold_secs).max(min_after_report))
}

/// Bounded-but-generous abort grace for the post-publish *finalize* window
/// (#319/#321).
///
/// A non-watch, non-semantic `cass index --full` ends by checkpointing the
/// deferred bulk-ingest WAL inside `close_storage_after_index` — a synchronous,
/// `!Send` frankensqlite `conn.close()` + `wal_checkpoint(TRUNCATE)` that runs
/// on the indexer thread and cannot advance the progress atomics. On a large
/// corpus (the #319 report: a ~1.1 GB / ~290k-frame WAL) that checkpoint
/// legitimately takes minutes, especially on macOS where Darwin fsync/flock is
/// slow. The generic finalize-wedge abort (`CASS_INDEX_STALL_ABORT_SECS`,
/// default 300 s) misreads that active checkpoint as a #297 wedge and kills the
/// process with `exit(70)` mid-checkpoint — stranding the un-truncated WAL and
/// leaving the DB malformed to stock SQLite (#296/#321).
///
/// While the indexer signals `IndexingProgress::finalizing`, the watchdog uses
/// this larger threshold instead, so a slow-but-progressing final checkpoint
/// completes. Because the checkpoint is a single blocking `!Send` call on the
/// indexer thread, it cannot be heartbeated — so a bound is the only liveness
/// guard against a genuinely stuck finalize: once it elapses the abort still
/// fires. `CASS_INDEX_FINALIZE_ABORT_SECS=0` makes the finalize window
/// report-only (never abort). The floor is the ordinary abort threshold, so
/// this can only ever *lengthen* the finalize grace, never shorten it.
pub(crate) fn index_finalize_abort_threshold(
    abort_threshold: Option<Duration>,
) -> Option<Duration> {
    let abort_threshold = abort_threshold?;
    let finalize_secs = dotenvy::var("CASS_INDEX_FINALIZE_ABORT_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(1800);
    if finalize_secs == 0 {
        return None;
    }
    Some(Duration::from_secs(finalize_secs).max(abort_threshold))
}

/// fr7wp / gh#413: cumulative block-IO bytes for this process (Linux
/// `/proc/self/io` `read_bytes + write_bytes`; `None` elsewhere or when
/// unreadable). Page-cache-hot userspace spins — the #301 wedge class that
/// disqualified a CPU-liveness signal — do not advance these counters, so
/// block-IO growth is a safe liveness hint for the pre-index window.
#[cfg_attr(test, allow(dead_code))]
pub(crate) fn process_block_io_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let text = std::fs::read_to_string("/proc/self/io").ok()?;
        let mut total: u64 = 0;
        let mut seen = false;
        for line in text.lines() {
            if let Some(rest) = line
                .strip_prefix("read_bytes:")
                .or_else(|| line.strip_prefix("write_bytes:"))
            {
                total = total.saturating_add(rest.trim().parse::<u64>().ok()?);
                seen = true;
            }
        }
        seen.then_some(total)
    }
    #[cfg(target_os = "macos")]
    {
        macos_process_disk_io_bytes()
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

/// #382: the macOS counterpart of `/proc/self/io` — `proc_pid_rusage`'s
/// `ri_diskio_bytesread + ri_diskio_byteswritten`. The 2026-08-31 retest of
/// the large-archive `preparing` wedge was on Apple Silicon: the canonical
/// open sat in `pread` over a ~37 GiB WAL for the whole 300 s abort window,
/// and because this probe was Linux-only the watchdog had no liveness signal
/// and killed six healthy-but-slow opens in a row. This is the one FFI call
/// with no safe wrapper in `std`; the buffer is the exact `rusage_info_v2`
/// layout the `RUSAGE_INFO_V2` flavour fills, and a nonzero return leaves it
/// unread.
#[cfg(target_os = "macos")]
#[allow(unsafe_code)]
pub(crate) fn macos_process_disk_io_bytes() -> Option<u64> {
    let mut info = std::mem::MaybeUninit::<libc::rusage_info_v2>::zeroed();
    // SAFETY: `proc_pid_rusage` writes at most `size_of::<rusage_info_v2>()`
    // bytes for the `RUSAGE_INFO_V2` flavour into a caller-provided buffer of
    // exactly that type; the buffer outlives the call and is only read after
    // the kernel reports success (return value 0). The libc binding spells
    // the C `rusage_info_t buffer` (a `void *`) as `*mut rusage_info_t`, so
    // the pointer to our struct is cast to that (ABI-identical) pointer type.
    let rc = unsafe {
        libc::proc_pid_rusage(
            libc::getpid(),
            libc::RUSAGE_INFO_V2,
            info.as_mut_ptr().cast::<libc::rusage_info_t>(),
        )
    };
    if rc != 0 {
        return None;
    }
    // SAFETY: the kernel filled the v2 layout on success (checked above).
    let info = unsafe { info.assume_init() }; // ubs:ignore[rust.unsafe-memory.assume-init] — Zeroed integer-only rusage_info_v2 is read only after proc_pid_rusage reports success.
    Some(
        info.ri_diskio_bytesread
            .saturating_add(info.ri_diskio_byteswritten),
    )
}

/// Default watchdog block-IO sampler: the real `/proc` probe in production,
/// pinned to `None` under `cfg(test)` so the timing-sensitive watchdog unit
/// tests stay deterministic (the host process's own block IO must not grant
/// the pre-index grace mid-test). Tests that exercise the grace inject their
/// own sampler; integration/E2E suites run the real binary and keep the live
/// probe.
pub(crate) fn default_watchdog_io_probe() -> fn() -> Option<u64> {
    #[cfg(test)]
    {
        || None
    }
    #[cfg(not(test))]
    {
        process_block_io_bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IndexStallAbortPolicy {
    #[cfg(test)]
    ReportOnly,
    AbortLexicalPhases,
}

pub(crate) struct IndexStallWatchdog {
    pub(crate) data_dir: PathBuf,
    pub(crate) filesystem_probe: IndexStallFilesystemProbe,
    pub(crate) threshold: Option<Duration>,
    pub(crate) abort_threshold: Option<Duration>,
    /// Larger abort grace used only while `IndexingProgress::finalizing` is set
    /// (the post-publish WAL-checkpoint window). `None` = never abort during
    /// finalize (report-only). See `index_finalize_abort_threshold` (#319/#321).
    pub(crate) finalize_abort_threshold: Option<Duration>,
    pub(crate) abort_policy: IndexStallAbortPolicy,
    /// True when supervising a long-lived `cass index --watch` daemon. Only
    /// its explicitly marked event wait is healthy idle. Cumulative counters
    /// cannot distinguish recovered work from an active blocked callback.
    pub(crate) is_watch: bool,
    /// True when this run is building semantic (vector) assets. Semantic work
    /// now publishes honest per-phase counters; this flag keeps those phases
    /// report-only and suppresses false stall reports during the one opaque
    /// native HNSW call. A preceding phase-2 lexical wedge remains abortable.
    pub(crate) semantic_build: bool,
    /// #437: opt-in (`CASS_INDEX_STALL_ABORT_ALL_PHASES=1`) promotion of the
    /// existing stall detection to a hard abort in ANY phase that reaches the
    /// abort gate — including the scribe/accumulate and semantic report-only
    /// lanes whose #258-signature wedges otherwise hold the index lock
    /// unbounded. States whose detection is suppressed entirely (opaque HNSW,
    /// quiescent watch idle, quiescent semantic finalize) never warn and are
    /// therefore never promoted. Default off.
    pub(crate) abort_all_phases: bool,
    pub(crate) last_phase: usize,
    pub(crate) last_current: usize,
    /// #332: last observed `IndexingProgress::activity` tick. Producers bump
    /// activity once per parsed conversation and the consumer once per batch,
    /// so an advancing tick proves live work between coarse `current`
    /// publications (a long parse of one large source artifact must not read
    /// as a stall).
    pub(crate) last_activity: u64,
    pub(crate) last_progress_advance: std::time::Instant,
    pub(crate) stall_reported_for_phase: Option<usize>,
    pub(crate) stall_abort_reported_for_phase: Option<usize>,
    /// fr7wp / gh#413: injectable process block-IO sampler (see
    /// [`process_block_io_bytes`]); a fn pointer so tests can pin the signal.
    pub(crate) io_probe: fn() -> Option<u64>,
    /// Last sampled cumulative block-IO byte count (pre-index phases only).
    pub(crate) last_io_bytes: Option<u64>,
    /// When block IO last grew. Recency within the detect threshold marks the
    /// slow canonical open/scan as live and grants the bounded grace.
    pub(crate) last_io_advance: std::time::Instant,
}

impl IndexStallWatchdog {
    #[cfg(test)]
    pub(crate) fn new(data_dir: PathBuf, progress_interval: Duration) -> Self {
        Self::with_abort_policy(
            data_dir,
            progress_interval,
            IndexStallAbortPolicy::ReportOnly,
        )
    }

    pub(crate) fn aborting_lexical_phases(data_dir: PathBuf, progress_interval: Duration) -> Self {
        Self::with_abort_policy(
            data_dir,
            progress_interval,
            IndexStallAbortPolicy::AbortLexicalPhases,
        )
    }

    /// Mark this watchdog as supervising a long-lived `cass index --watch`
    /// daemon. Its explicit `watch_idle` scope distinguishes normal event
    /// waits from active callbacks, including work that blocks before it can
    /// update phase or counters. Set on the watch path only.
    pub(crate) fn watch_aware(mut self, is_watch: bool) -> Self {
        self.is_watch = is_watch;
        self
    }

    /// Mark this watchdog as supervising a one-shot semantic (vector) build.
    /// Measured semantic phases can emit diagnostics but never exit the
    /// process; native HNSW is opaque and therefore excluded from no-progress
    /// diagnostics. A genuine preparing/scanning/indexing wedge is still
    /// detected and bounded under the lexical policy.
    pub(crate) fn semantic_aware(mut self, semantic_build: bool) -> Self {
        self.semantic_build = semantic_build;
        self
    }

    pub(crate) fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub(crate) fn with_abort_policy(
        data_dir: PathBuf,
        progress_interval: Duration,
        abort_policy: IndexStallAbortPolicy,
    ) -> Self {
        let threshold = index_stall_threshold(progress_interval);
        let abort_threshold = index_stall_abort_threshold(progress_interval, threshold);
        let finalize_abort_threshold = index_finalize_abort_threshold(abort_threshold);
        Self {
            data_dir,
            filesystem_probe: IndexStallFilesystemProbe::default(),
            threshold,
            abort_threshold,
            finalize_abort_threshold,
            abort_policy,
            is_watch: false,
            semantic_build: false,
            abort_all_phases: index_stall_abort_all_phases_enabled(),
            last_phase: usize::MAX,
            last_current: 0,
            last_activity: 0,
            last_progress_advance: std::time::Instant::now(),
            stall_reported_for_phase: None,
            stall_abort_reported_for_phase: None,
            io_probe: default_watchdog_io_probe(),
            last_io_bytes: None,
            last_io_advance: std::time::Instant::now(),
        }
    }

    pub(crate) fn observe(
        &mut self,
        index_progress: &indexer::IndexingProgress,
        elapsed_ms: u128,
    ) -> Option<serde_json::Value> {
        let phase_code = index_progress
            .phase
            .load(std::sync::atomic::Ordering::Relaxed);
        let current = index_progress
            .current
            .load(std::sync::atomic::Ordering::Relaxed);
        let activity = index_progress
            .activity
            .load(std::sync::atomic::Ordering::Relaxed);

        if phase_code != self.last_phase {
            self.filesystem_probe.reset_after_progress();
            self.last_phase = phase_code;
            self.last_current = current;
            self.last_activity = activity;
            self.last_progress_advance = std::time::Instant::now();
            self.stall_reported_for_phase = None;
            self.stall_abort_reported_for_phase = None;
            return None;
        }

        if current != self.last_current {
            self.filesystem_probe.reset_after_progress();
            self.last_progress_advance = std::time::Instant::now();
            self.last_current = current;
            self.last_activity = activity;
            self.stall_reported_for_phase = None;
            self.stall_abort_reported_for_phase = None;
            return None;
        }

        // #332: `current` measures batch publication, not worker activity. A
        // producer mid-parse of one large source artifact publishes nothing
        // for minutes while ticking the activity counter per parsed
        // conversation — that is forward progress, not a stall.
        if activity != self.last_activity {
            self.filesystem_probe.reset_after_progress();
            self.last_progress_advance = std::time::Instant::now();
            self.last_activity = activity;
            self.stall_reported_for_phase = None;
            self.stall_abort_reported_for_phase = None;
            return None;
        }

        if self.semantic_build && phase_code == indexer::INDEX_PHASE_SEMANTIC_HNSW {
            return None;
        }

        // fr7wp / gh#413: the slow canonical-DB open (and fsqlite's WAL-replay
        // work inside it) runs under "preparing" before any progress atomic
        // can tick — the 2026-08-23 report needed CASS_INDEX_STALL_ABORT_SECS=0
        // just to get past a healthy slow open on a 2.1GB archive. Sample
        // cumulative block IO while the run is still pre-index; recency feeds
        // a BOUNDED grace below (finalize-class threshold), never an
        // unbounded deferral, so #422's pre-index-wedges-are-abortable
        // guarantee holds and a #301-class page-cache-hot spin (no block IO)
        // gets no grace at all.
        if phase_code < indexer::INDEX_PHASE_LEXICAL_INDEXING
            && let Some(io_bytes) = (self.io_probe)()
        {
            if self
                .last_io_bytes
                .is_some_and(|previous| io_bytes > previous)
            {
                self.last_io_advance = std::time::Instant::now();
            }
            self.last_io_bytes = Some(io_bytes);
        }

        let threshold = self.threshold?;
        let stall_elapsed = self.last_progress_advance.elapsed();
        // GH #538 / #311: only the owner's actual event/cooldown wait is idle.
        // A failed batch followed by a successful retry leaves cumulative
        // current < total; equal counters can also precede a blocked classify
        // or optimize call. Neither count shape proves whether work is active.
        if self.is_watch
            && phase_code == 0
            && index_progress
                .watch_idle
                .load(std::sync::atomic::Ordering::Acquire)
            && index_progress.rebuild_pipeline_is_quiescent()
        {
            // Pause the deadline during a genuine wait so a long quiet night
            // is not charged against the first syscall of the next callback.
            // This does not tick activity or advance any completion counter.
            // A callback can also return to idle without changing counters.
            // Retire its completed probe, keeping any blocked worker owned.
            self.filesystem_probe.reset_after_progress();
            self.last_progress_advance = std::time::Instant::now();
            self.stall_reported_for_phase = None;
            self.stall_abort_reported_for_phase = None;
            return None;
        }

        // cass #315: a one-shot `cass index --semantic` build has the same
        // healthy-idle shape here for the entire semantic pass. The lexical
        // rebuild finishes and parks progress at phase 0 / `current == total` /
        // quiescent, then the native MiniLM embed + HNSW build + vector publish
        // run for minutes WITHOUT ever advancing those atomics. That legitimate
        // work is indistinguishable from a finalize wedge, so the abort used to
        // kill the process (exit 70) before publishing any vector index. Treat
        // the quiescent finalize state as healthy active work for semantic
        // builds too, so the embed/publish runs to completion. A genuine phase-2
        // lexical wedge does not match (phase_code != 0) and is still caught.
        if self.semantic_build && phase_code == 0 {
            let total = index_progress
                .total
                .load(std::sync::atomic::Ordering::Relaxed);
            if total > 0 && current >= total && index_progress.rebuild_pipeline_is_quiescent() {
                return None;
            }
        }
        // Historically this gated on `phase_code != 0` so the watchdog
        // never fired during the "preparing" phase (phase=0). Issue #258
        // is exactly that: the v0.6.2 watcher wedged at startup before
        // ever advancing to Scanning/Indexing, the watchdog stayed
        // silent for 4+ hours, and operators had no signal that the
        // indexer was stuck. The fix is to also fire on phase=0 wedges
        // — a startup that takes >120 s with no progress is, by any
        // sensible definition, a stall worth reporting. The repeat
        // guard `stall_reported_for_phase == Some(phase_code)` still
        // prevents log spam from a single stalled phase.
        if self.stall_reported_for_phase != Some(phase_code) {
            if stall_elapsed < threshold {
                return None;
            }

            self.stall_reported_for_phase = Some(phase_code);
            return Some(self.stall_payload(
                index_progress,
                elapsed_ms,
                stall_elapsed,
                threshold,
                "stall_detected",
                None,
            ));
        }

        let abort_threshold = self.abort_threshold?;
        // The post-publish / post-rebuild finalize runs under the
        // "preparing" phase (phase 0) with `current == total`. Issue #297:
        // a wedge there (workers parked after the lexical publish, never
        // woken to checkpoint/clean up) used to keep the process alive
        // indefinitely, because the abort was historically gated on
        // `phase == 2` only. Extend the abort to fire in any phase once the
        // work is nominally complete (`current >= total > 0`) AND the
        // rebuild pipeline is fully quiescent — so a legitimately
        // slow-but-active sort/rebuild (#294) is never killed. The
        // `abort_threshold` (default 300 s of zero forward progress) remains
        // the outer safety margin on top of that.
        let total = index_progress
            .total
            .load(std::sync::atomic::Ordering::Relaxed);
        // `!self.is_watch`: in watch mode the quiescent `current >= total`
        // resting state is normal idle (handled by the early return above), so
        // it must never be treated as a finalize wedge (cass #311).
        // `!self.semantic_build`: for a one-shot semantic build the same
        // quiescent state is the active embed/publish window (also handled by
        // the early return above); belt-and-suspenders so an off-by-one phase
        // never resurrects the exit(70) that killed the build pre-publish (cass
        // #315).
        let finalize_wedge = !self.is_watch
            && !self.semantic_build
            && total > 0
            && current >= total
            && index_progress.rebuild_pipeline_is_quiescent();
        // #422: a search-triggered lexical refresh can wedge before phase 2,
        // notably in preparing while opening/counting the canonical DB. A
        // phase-2-only policy merely warned and then let the live process hold
        // index-run.lock forever. Every lexical phase is bounded; semantic
        // phases retain their measured/report-only treatment below.
        let pre_index_lexical_wedge = phase_code < indexer::INDEX_PHASE_LEXICAL_INDEXING
            && index_progress.rebuild_pipeline_is_quiescent();
        // #437: CASS_INDEX_STALL_ABORT_ALL_PHASES=1 promotes the stall
        // detection above to a hard abort in EVERY phase that reaches this
        // gate — including the scribe/accumulate lane (phase 0/2 with a
        // non-quiescent-but-frozen rebuild pipeline) and the semantic
        // report-only phases. The abort path is identical to the lexical
        // aborts (`abort_after_index_stall_if_requested`: best-effort WAL
        // checkpoint, then exit 70), so locks are released and checkpoint
        // state stays consistent exactly as before. The finalize/persist/
        // pre-index-IO graces below still lengthen the bound where they apply.
        let abort_eligible = self.abort_all_phases
            || phase_code == indexer::INDEX_PHASE_LEXICAL_INDEXING
            || phase_code == indexer::INDEX_PHASE_ANALYTICS_REBUILD
            || pre_index_lexical_wedge
            || finalize_wedge;
        // #319/#321: while the indexer signals `finalizing`, the phase-0 /
        // current==total / quiescent shape is the post-publish WAL-checkpoint
        // window, not a #297 wedge. The checkpoint is a synchronous, `!Send`
        // frankensqlite call on the indexer thread that cannot advance the
        // progress atomics, so it can only be recognised via this flag. Give it
        // the larger `finalize_abort_threshold` so a slow-but-active checkpoint
        // of a large deferred WAL completes instead of being killed mid-write
        // (which strands the WAL and leaves the DB malformed). A genuine phase-2
        // wedge (phase_code == 2) is unaffected and still aborts at the normal
        // threshold. Because the checkpoint blocks a single thread and cannot be
        // heartbeated, the bound is the only liveness guard: once it elapses the
        // finalize is aborted too.
        // #366: the finalize grace also applies while a staged lexical
        // rebuild is active but its pipeline has not produced the first page
        // yet (`is_rebuilding` set, quiescent, current >= total). That
        // ingest→staged-shard-build hand-off legitimately blocks in
        // synchronous frankensqlite work — e.g. a multi-GB WAL replay inside
        // the producer's read-only open — that cannot be heartbeated, exactly
        // like the finalize checkpoint. Once the pipeline starts moving, the
        // runtime refresh ticks `activity` on every stage-progress change and
        // quiescence ends. Note the breadth honestly: `is_rebuilding` is set
        // at scan start and cleared only on rebuild completion, so ANY
        // quiescent wedge between ingest completion and rebuild
        // completion in a one-shot run gets the finalize grace (a bounded
        // delay by default; never-abort only if the operator sets
        // CASS_INDEX_FINALIZE_ABORT_SECS=0). A genuinely wedged rebuild
        // still aborts at the (larger) finalize threshold.
        // #373: this grace now also covers the phase-2 rebuild *tail* — the
        // terminal `t_index.commit()` / federated-publish window that runs with
        // `phase == 2`, `current == total`, and a quiescent pipeline while
        // `is_rebuilding` is still set. That single blocking Tantivy commit
        // (bulk-load merges collapse into it) can run for many minutes on a
        // large index without advancing any progress atomic, and the previous
        // `phase_code != 2` guard denied it the grace, so a healthy multi-hour
        // rebuild tail was killed with exit(70) at the 300s abort. `finalize_wedge`
        // already requires `current >= total` + quiescence, so a mid-rebuild
        // phase-2 wedge (current < total, or a still-producing pipeline) is NOT
        // a finalize_wedge and continues to abort at the normal threshold.
        // gh373 third variant (bead oeu5a): an active batched persist of one
        // giant conversation (observed: 60-90MB codex rollouts, ~40k
        // messages) legitimately runs for minutes mid-phase-2 with
        // `current < total` and a non-quiescent pipeline — a shape neither
        // the finalize grace nor the #311/#315 early-returns cover. The
        // per-message persist heartbeat is the primary liveness signal for
        // that window; `persist_in_progress` grants the finalize-class grace
        // as defense in depth so any future untick'd blocking stretch inside
        // persist degrades to the bounded finalize threshold instead of a
        // 300s exit(70). Liveness stays bounded: a genuinely wedged persist
        // still aborts once the finalize grace elapses.
        let persist_grace = phase_code == 2
            && index_progress
                .persist_in_progress
                .load(std::sync::atomic::Ordering::Relaxed);
        // fr7wp / gh#413: a pre-index stall whose process block IO advanced
        // within the current detect window is a slow open/scan doing real
        // work, not a wedge. It gets the finalize-class abort bound instead of
        // the base one — a bounded delay, exactly like the finalize/persist
        // graces above it.
        let preparing_io_grace = pre_index_lexical_wedge
            && self.last_io_bytes.is_some()
            && self.last_io_advance.elapsed() < threshold;
        let effective_abort_threshold = if (finalize_wedge
            && (index_progress
                .finalizing
                .load(std::sync::atomic::Ordering::Relaxed)
                || index_progress
                    .is_rebuilding
                    .load(std::sync::atomic::Ordering::Relaxed)))
            || persist_grace
            || preparing_io_grace
        {
            // Finalize aborts disabled (CASS_INDEX_FINALIZE_ABORT_SECS=0):
            // treat the finalize window as report-only.
            self.finalize_abort_threshold?
        } else {
            abort_threshold
        };
        if self.abort_policy != IndexStallAbortPolicy::AbortLexicalPhases
            || !abort_eligible
            || self.stall_abort_reported_for_phase == Some(phase_code)
            || stall_elapsed < effective_abort_threshold
        {
            return None;
        }

        self.stall_abort_reported_for_phase = Some(phase_code);
        // gh #400: report the threshold that actually gated this abort. When
        // the finalize/persist grace is active the process was allowed
        // `effective_abort_threshold` (e.g. 1800s) of quiescence, so emitting
        // the base `abort_threshold` (300s) here made the payload claim an
        // abort budget the watchdog never applied.
        Some(self.stall_payload(
            index_progress,
            elapsed_ms,
            stall_elapsed,
            threshold,
            "stall_aborting",
            Some(effective_abort_threshold),
        ))
    }

    pub(crate) fn stall_payload(
        &mut self,
        index_progress: &indexer::IndexingProgress,
        elapsed_ms: u128,
        stall_elapsed: Duration,
        threshold: Duration,
        event: &'static str,
        abort_threshold: Option<Duration>,
    ) -> serde_json::Value {
        let stall_elapsed_ms = stall_elapsed.as_millis();
        let mut payload = index_progress.snapshot_json(elapsed_ms);
        if let serde_json::Value::Object(ref mut m) = payload {
            m.insert("event".into(), serde_json::json!(event));
            m.insert(
                "ts_ms".into(),
                serde_json::json!(chrono::Utc::now().timestamp_millis()),
            );
            m.insert(
                "stall_elapsed_ms".into(),
                serde_json::json!(stall_elapsed_ms as u64),
            );
            m.insert(
                "stall_threshold_secs".into(),
                serde_json::json!(threshold.as_secs()),
            );
            if let Some(abort_threshold) = abort_threshold {
                m.insert(
                    "abort_threshold_secs".into(),
                    serde_json::json!(abort_threshold.as_secs()),
                );
                m.insert("abort_process".into(), serde_json::json!(true));
                m.insert("exit_code".into(), serde_json::json!(70));
            }
            m.insert(
                "diagnostics".into(),
                self.filesystem_probe.snapshot(&self.data_dir),
            );
            // gh373/oeu5a: phase-aware hint. A phase-2 stall with an active
            // batched persist is the giant-conversation window; the generic
            // finalize-WAL guidance is a decoy there.
            let phase_code = index_progress
                .phase
                .load(std::sync::atomic::Ordering::Relaxed);
            let persist_active = index_progress
                .persist_in_progress
                .load(std::sync::atomic::Ordering::Relaxed);
            let hint = if phase_code == 2 && persist_active {
                INDEX_STALL_HINT_PERSIST
            } else {
                INDEX_STALL_HINT
            };
            m.insert("hint".into(), serde_json::json!(hint));
            // fr7wp / gh#413: pre-index stall with recently-advancing block
            // IO — the canonical open/scan is doing real work and the bounded
            // grace applies; operators reading the event should wait, not
            // kill the run.
            if phase_code < indexer::INDEX_PHASE_LEXICAL_INDEXING
                && self.last_io_bytes.is_some()
                && self.last_io_advance.elapsed() < threshold
            {
                m.insert("pre_index_io_active".into(), serde_json::json!(true));
            }
        }
        payload
    }
}

pub(crate) fn index_stall_warning_lines(payload: &serde_json::Value) -> (String, String) {
    let phase = payload
        .get("phase")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    let elapsed_secs = payload
        .get("stall_elapsed_ms")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default()
        / 1000;
    let summary = format!(
        "Warning: cass index made no {phase} progress for {elapsed_secs}s; stall diagnostics follow."
    );
    let diagnostics = serde_json::to_string(payload)
        .unwrap_or_else(|err| format!(r#"{{"event":"stall_detected","serialize_error":"{err}"}}"#));
    (summary, diagnostics)
}

/// The robot error envelope for a stall abort (WS-F.2, #439): the same shape
/// `cli_error_json_payload` gives every other failure (`success`, `error`,
/// `code`, `kind`, `retryable`, `hint`), plus the stall fields agents branch
/// on. Returns `None` for a payload that does not request an abort, so the
/// warn-only stall path never emits an error envelope.
pub(crate) fn index_stall_abort_envelope(payload: &serde_json::Value) -> Option<serde_json::Value> {
    if !payload
        .get("abort_process")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        return None;
    }
    let phase = payload
        .get("phase")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    let stall_elapsed_ms = payload
        .get("stall_elapsed_ms")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default();
    let abort_threshold_secs = payload
        .get("abort_threshold_secs")
        .and_then(serde_json::Value::as_u64);
    let hint = payload
        .get("hint")
        .and_then(serde_json::Value::as_str)
        .map_or_else(|| INDEX_STALL_HINT.to_string(), |hint| hint.to_string());
    Some(serde_json::json!({
        "success": false,
        "error": format!(
            "cass index made no {phase} progress for {}s past the abort threshold",
            stall_elapsed_ms / 1000
        ),
        "code": 70,
        "kind": "index-stalled",
        "retryable": true,
        "phase": phase,
        "stall_elapsed_ms": stall_elapsed_ms,
        "abort_threshold_secs": abort_threshold_secs,
        "hint": hint,
        "recommended_action": "Inspect `cass status --json` (rebuild.phase, rebuild.last_progress_age_ms) and the stall diagnostics above; re-run `cass index` — the lock is reaped on the next start. Raise CASS_INDEX_STALL_ABORT_SECS only if the diagnostics show real forward progress.",
    }))
}

pub(crate) fn abort_after_index_stall_if_requested(payload: &serde_json::Value, data_dir: &Path) {
    if let Some(envelope) = index_stall_abort_envelope(payload) {
        eprintln!(
            "cass index made no indexing progress past the abort threshold; exiting with code 70."
        );
        // WS-F.2: one machine-readable line on stderr (stdout stays data-only)
        // so agents get the documented error envelope for exit 70 instead of
        // only a prose warning.
        eprintln!("{envelope}");
        // #296: before the bounded exit (which skips destructors), best-effort
        // checkpoint the canonical WAL so the killed run leaves a recoverable
        // DB instead of a multi-GB orphaned WAL. Stale locks are reaped by the
        // next startup's flock-based recovery.
        crate::indexer::best_effort_abort_wal_checkpoint(data_dir);
        exit_without_this_threads_tls_destructors(70);
    }
}

/// Exit with `code` without running the calling thread's thread-local
/// destructors. On Windows, `std::process::exit` terminates every other thread
/// and then runs the calling thread's TLS destructors; one that joins a worker
/// thread the exit already killed panics ("threads should not terminate
/// unexpectedly"), and a panic in a TLS destructor aborts the process with
/// 0xC0000409 instead of `code`. A fresh thread owns no such state, so exiting
/// from it keeps the documented code. Elsewhere this is `std::process::exit`.
pub(crate) fn exit_without_this_threads_tls_destructors(code: i32) -> ! {
    #[cfg(windows)]
    {
        // The spawned thread ends the process; join only returns if the spawn
        // itself failed, and then the direct exit below still runs.
        let _ = std::thread::Builder::new()
            .name("cass-exit".to_string())
            .spawn(move || std::process::exit(code))
            .map(std::thread::JoinHandle::join);
    }
    std::process::exit(code)
}
