use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// Shared shape of every liveness callback (the indexer passes
/// `IndexingProgress::tick_activity`).
pub type EngineHeartbeat = Arc<dyn Fn() + Send + Sync>;

/// How often the sampler looks for engine progress while an opaque engine call
/// is running. Well inside the watchdog's 120 s detect window.
pub(super) const ENGINE_LIVENESS_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);
/// Minimum process CPU time consumed within one sample interval for the
/// interval to count as work. A wedged process (workers parked on futexes,
/// #413) burns microseconds per second; a sealing/merging engine burns most
/// of a core.
const ENGINE_LIVENESS_MIN_CPU_ADVANCE: Duration = Duration::from_millis(100);

/// GH #446: liveness ticks from inside the Quill sink.
///
/// The stall watchdog's only signals are `phase`/`current`/`activity`, and
/// nothing on the sink side of the phase-2 rebuild pipeline used to move any
/// of them: a Quill commit on a multi-million-message archive is minutes of
/// scribe seal + segment build + merge + durable publish, the producer is
/// parked on pipeline budget the whole time, and the pipeline counters stay
/// byte-identical — a healthy 66-minute rebuild logged 12 abort-eligible
/// `stall_detected` windows and, with the default 300 s abort, exit 70.
///
/// The engine exposes no intra-commit callback, so the probe measures the
/// commit's *observable* work instead of granting a phase-shaped grace:
///
/// 1. every accumulate call (a batch of documents admitted into the scribe)
///    ticks synchronously — that is real, completed work;
/// 2. while an opaque engine call (commit / merge / compact) runs, a sampler
///    thread ticks once per interval in which EITHER the index directory's
///    byte footprint grew (segment files being written) OR the process
///    consumed at least [`ENGINE_LIVENESS_MIN_CPU_ADVANCE`] of CPU (a
///    CPU-bound seal or merge stretch between writes).
///
/// Liveness stays honest: a wedged engine call — parked threads, no writes,
/// no CPU — advances neither signal, the counters freeze exactly as before,
/// and the watchdog still aborts (#413 detection preserved). The sampler
/// sleeps while no engine call is in flight and exits when the index is
/// dropped.
pub(super) struct EngineLivenessProbe {
    armed: Arc<AtomicBool>,
    /// Dropping this sender disconnects the sampler's wait, so the probe's
    /// drop returns at once instead of after the rest of a sample interval.
    stop: Option<std::sync::mpsc::Sender<()>>,
    pub(super) heartbeat: EngineHeartbeat,
    sampler: Option<std::thread::JoinHandle<()>>,
}

impl EngineLivenessProbe {
    pub(super) fn spawn(targets: Vec<PathBuf>, heartbeat: EngineHeartbeat) -> Self {
        let armed = Arc::new(AtomicBool::new(false));
        let (stop, stopped) = std::sync::mpsc::channel::<()>();
        let sampler = {
            let armed = Arc::clone(&armed);
            let heartbeat = Arc::clone(&heartbeat);
            std::thread::Builder::new()
                .name("cass-quill-liveness".to_string())
                .spawn(move || {
                    let mut tracker = EngineLivenessTracker::default();
                    // A plain sleep here made every probe drop wait out the
                    // rest of a whole interval: one second per storage open
                    // and Quill index drop, two in every `cass index`. The
                    // probe's drop disconnects the channel, ending the wait.
                    while let Err(std::sync::mpsc::RecvTimeoutError::Timeout) =
                        stopped.recv_timeout(ENGINE_LIVENESS_SAMPLE_INTERVAL)
                    {
                        if !armed.load(Ordering::Relaxed) {
                            // Forget the previous op's baseline so the first
                            // sample of the next op is a fresh comparison.
                            tracker = EngineLivenessTracker::default();
                            continue;
                        }
                        let sample = EngineLivenessSample::capture(&targets);
                        if tracker.observe(sample) {
                            heartbeat();
                        }
                    }
                })
                .ok()
        };
        Self {
            armed,
            stop: Some(stop),
            heartbeat,
            sampler,
        }
    }

    /// Run one opaque engine call with the sampler armed.
    pub(super) fn during<T>(&self, op: impl FnOnce() -> T) -> T {
        self.armed.store(true, Ordering::Relaxed);
        let result = op();
        self.armed.store(false, Ordering::Relaxed);
        result
    }
}

impl Drop for EngineLivenessProbe {
    fn drop(&mut self) {
        drop(self.stop.take());
        if let Some(sampler) = self.sampler.take() {
            // The disconnected channel wakes the sampler at once; joining
            // bounds the thread's lifetime to the index's without blocking a
            // drop on a stuck heartbeat callback.
            if std::thread::current().id() != sampler.thread().id() {
                drop(sampler.join());
            }
        }
    }
}

/// One observation of the two engine-progress signals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct EngineLivenessSample {
    /// Total bytes of regular files under the index directory (plus file
    /// count, so a size-neutral rename/replace still registers).
    pub directory_footprint: (u64, u64),
    /// Process user+system CPU time; `None` where unavailable.
    pub process_cpu_time: Option<Duration>,
}

impl EngineLivenessSample {
    pub(super) fn capture(targets: &[PathBuf]) -> Self {
        Self {
            directory_footprint: watch_target_footprint(targets),
            process_cpu_time: process_cpu_time(),
        }
    }
}

/// Summed `(bytes, files)` over every watch target: a directory contributes
/// its whole recursive footprint, a plain file contributes itself, and a
/// missing path contributes nothing.
pub(super) fn watch_target_footprint(targets: &[PathBuf]) -> (u64, u64) {
    let mut bytes = 0u64;
    let mut files = 0u64;
    for target in targets {
        let Ok(metadata) = std::fs::metadata(target) else {
            continue;
        };
        if metadata.is_dir() {
            let (dir_bytes, dir_files) = directory_footprint(target);
            bytes = bytes.saturating_add(dir_bytes);
            files = files.saturating_add(dir_files);
        } else if metadata.is_file() {
            bytes = bytes.saturating_add(metadata.len());
            files = files.saturating_add(1);
        }
    }
    (bytes, files)
}

/// GH #450: run one opaque, potentially very long blocking call — a
/// frankensqlite open that may perform a multi-gigabyte migration repair — with
/// the same honest liveness sampler the Quill sink uses (see
/// [`EngineLivenessProbe`]).
///
/// `targets` are the files/directories whose byte footprint counts as evidence
/// of work (for a storage open: the database and its sidecars). `heartbeat` is
/// called at most once per sample interval, and only in an interval where the
/// footprint grew or the process burned CPU — so a genuinely wedged call still
/// posts no progress and the stall watchdog still fires.
pub fn run_with_liveness_ticks<T>(
    targets: Vec<PathBuf>,
    heartbeat: EngineHeartbeat,
    op: impl FnOnce() -> T,
) -> T {
    let probe = EngineLivenessProbe::spawn(targets, heartbeat);
    probe.during(op)
}

/// Pure decision core of the sampler: compares consecutive samples and says
/// whether the interval showed engine work. Unit-tested without threads.
#[derive(Debug, Default)]
pub(super) struct EngineLivenessTracker {
    previous: Option<EngineLivenessSample>,
}

impl EngineLivenessTracker {
    /// Returns `true` when `sample` shows progress relative to the previous
    /// one: the directory footprint changed, or CPU advanced by at least
    /// [`ENGINE_LIVENESS_MIN_CPU_ADVANCE`]. The first sample of an op only
    /// establishes the baseline.
    pub(super) fn observe(&mut self, sample: EngineLivenessSample) -> bool {
        let Some(previous) = self.previous.replace(sample) else {
            return false;
        };
        if sample.directory_footprint != previous.directory_footprint {
            return true;
        }
        match (previous.process_cpu_time, sample.process_cpu_time) {
            (Some(before), Some(now)) => {
                now.saturating_sub(before) >= ENGINE_LIVENESS_MIN_CPU_ADVANCE
            }
            _ => false,
        }
    }
}

/// `(bytes, files)` for every regular file under `directory`, recursively.
/// Best-effort: unreadable entries are skipped, never fatal.
pub(super) fn directory_footprint(directory: &Path) -> (u64, u64) {
    let mut bytes = 0u64;
    let mut files = 0u64;
    let mut pending = vec![directory.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir() {
                pending.push(entry.path());
            } else if file_type.is_file()
                && let Ok(metadata) = entry.metadata()
            {
                bytes = bytes.saturating_add(metadata.len());
                files = files.saturating_add(1);
            }
        }
    }
    (bytes, files)
}

/// Process user+system CPU time via `getrusage(RUSAGE_SELF)`.
///
/// Unavoidable FFI (AGENTS.md): there is no safe std API for process CPU
/// time, and the daemon's `resource.rs` takes the same allow for its POSIX
/// calls.
#[cfg(unix)]
#[allow(unsafe_code)]
fn process_cpu_time() -> Option<Duration> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: `getrusage(RUSAGE_SELF, ptr)` writes a fully initialized
    // `rusage` into the caller-owned buffer on success (return 0), retains
    // no pointer, and touches nothing else.
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    // SAFETY: rc == 0 guarantees the kernel initialized the whole struct.
    let usage = unsafe { usage.assume_init() };
    let timeval_to_duration = |tv: libc::timeval| {
        Duration::from_secs(u64::try_from(tv.tv_sec).unwrap_or(0))
            + Duration::from_micros(u64::try_from(tv.tv_usec).unwrap_or(0))
    };
    Some(timeval_to_duration(usage.ru_utime) + timeval_to_duration(usage.ru_stime))
}

#[cfg(not(unix))]
fn process_cpu_time() -> Option<Duration> {
    None
}
