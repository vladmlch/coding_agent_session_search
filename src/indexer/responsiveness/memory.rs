#[cfg(target_os = "macos")]
use std::sync::{LazyLock, Mutex};
#[cfg(any(test, target_os = "macos"))]
use std::time::Duration;
#[cfg(target_os = "macos")]
use std::time::Instant;

#[cfg(target_os = "macos")]
use super::DEFAULT_TICK_SECS;
use super::{
    GovernorConfig, HealthReader, HealthSnapshot, ProcHealthReader, disabled_via_env, env_u64,
};

/// `footprint(1)` is materially heavier than reading `/proc` or invoking
/// `ps`, and lexical-pipeline snapshots can be captured many times per second.
/// Cache the Darwin sample at the same cadence as the responsiveness governor
/// so accurate physical-footprint telemetry does not become rebuild work.
#[cfg(target_os = "macos")]
const MACOS_PROCESS_MEMORY_SAMPLE_INTERVAL: Duration = Duration::from_secs(DEFAULT_TICK_SECS);

#[cfg(target_os = "macos")]
#[derive(Debug)]
struct MacosProcessMemorySample {
    sampled_at: Option<Instant>,
    bytes: Option<u64>,
}

#[cfg(target_os = "macos")]
static MACOS_PROCESS_MEMORY_SAMPLE: LazyLock<Mutex<MacosProcessMemorySample>> =
    LazyLock::new(|| {
        Mutex::new(MacosProcessMemorySample {
            sampled_at: None,
            bytes: None,
        })
    });

/// Wall-clock bound for one OS telemetry probe (`footprint`, `ps`, `sysctl`,
/// `vm_stat`, `ioreg`). Each normally answers in milliseconds. A probe that
/// has not answered by then is killed with its process group and treated as
/// "telemetry unavailable", which every caller already handles.
///
/// Observed on a Mac whose `footprint`/`sample` service had wedged: every
/// `footprint --pid` call hung forever, and `cass index` sat at 0% CPU in
/// `Command::output()` until the stall watchdog aborted it (exit 70).
#[cfg(any(test, target_os = "macos"))]
const OS_TELEMETRY_PROBE_TIMEOUT: Duration = Duration::from_secs(3);

/// Largest stdout/stderr accepted from one telemetry probe. `ioreg -c
/// IOHIDSystem` is the largest at a few hundred KiB.
#[cfg(any(all(test, unix), target_os = "macos"))]
const OS_TELEMETRY_PROBE_MAX_OUTPUT_BYTES: usize = 4 * 1024 * 1024;

/// Outcome of one bounded telemetry probe.
#[cfg(any(test, target_os = "macos"))]
#[derive(Debug)]
pub(super) enum TelemetryProbe {
    /// The command exited within the deadline (any status).
    // Only the Unix probe runner constructs it; Windows test builds lack it.
    #[cfg_attr(not(unix), allow(dead_code))]
    Completed(std::process::Output),
    /// The deadline passed; the command and its process group were killed.
    TimedOut,
    /// The command could not be spawned or its output could not be read.
    Failed,
}

/// Run a telemetry command with no stdin, captured output, its own process
/// group, and a hard wall-clock deadline. Never blocks past `timeout` on the
/// child itself: a hung command is killed (SIGKILL to its group) and reaped.
#[cfg(any(all(test, unix), target_os = "macos"))]
pub(super) fn run_bounded_telemetry_probe(
    mut command: std::process::Command,
    timeout: Duration,
) -> TelemetryProbe {
    use std::process::Stdio;

    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    crate::sources::configure_child_process_group(&mut command);
    let Ok(child) = command.spawn() else {
        return TelemetryProbe::Failed;
    };
    match crate::sources::wait_for_child_output_with_limit(
        child,
        timeout,
        Some(OS_TELEMETRY_PROBE_MAX_OUTPUT_BYTES),
    ) {
        Ok(Some(output)) => TelemetryProbe::Completed(output),
        Ok(None) => TelemetryProbe::TimedOut,
        Err(_) => TelemetryProbe::Failed,
    }
}

/// Successful stdout of a bounded telemetry probe, or `None` when the probe
/// failed, exited nonzero, or timed out.
#[cfg(target_os = "macos")]
fn bounded_telemetry_stdout(command: std::process::Command) -> Option<String> {
    match run_bounded_telemetry_probe(command, OS_TELEMETRY_PROBE_TIMEOUT) {
        TelemetryProbe::Completed(output) if output.status.success() => {
            Some(String::from_utf8_lossy(&output.stdout).into_owned())
        }
        _ => None,
    }
}

/// Set once `footprint(1)` has timed out in this process. A wedged
/// `footprint` service stays wedged, so later samples go straight to the
/// `ps` RSS fallback instead of paying the probe deadline on every tick.
#[cfg(target_os = "macos")]
static FOOTPRINT_PROBE_DISABLED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Physical footprint through `probe`, honouring and maintaining the
/// `disabled` circuit breaker. Separated from the `footprint` command so the
/// timeout policy is testable on every platform.
#[cfg(any(test, target_os = "macos"))]
pub(super) fn phys_footprint_bytes_via_probe(
    disabled: &std::sync::atomic::AtomicBool,
    probe: impl FnOnce() -> TelemetryProbe,
) -> Option<u64> {
    use std::sync::atomic::Ordering;

    if disabled.load(Ordering::Relaxed) {
        return None;
    }
    match probe() {
        TelemetryProbe::Completed(output) if output.status.success() => {
            macos_phys_footprint_bytes_from_output(&String::from_utf8_lossy(&output.stdout))
        }
        TelemetryProbe::Completed(_) | TelemetryProbe::Failed => None,
        TelemetryProbe::TimedOut => {
            if !disabled.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    timeout_ms = OS_TELEMETRY_PROBE_TIMEOUT.as_millis() as u64,
                    "footprint(1) did not answer within its deadline; using ps RSS for process memory from now on"
                );
            }
            None
        }
    }
}

#[cfg(target_os = "linux")]
pub(super) fn read_loadavg() -> Option<f32> {
    let raw = std::fs::read_to_string("/proc/loadavg").ok()?;
    let first = raw.split_whitespace().next()?;
    first.parse::<f32>().ok()
}

/// macOS: the 1-minute load average, read in-process.
///
/// `getloadavg(3)` reads the same `vm.loadavg` sysctl the CLI does, so this is
/// one syscall instead of a `posix_spawn` + `poll` + reap. The governor samples
/// this on every tick, and the spawned `sysctl` was visible as real work in
/// stack samples of long test runs, so the subprocess is now only a fallback
/// for the (practically unreachable) case where `getloadavg` reports nothing.
#[cfg(target_os = "macos")]
#[allow(unsafe_code)]
pub(super) fn read_loadavg() -> Option<f32> {
    let mut samples = [0f64; 3];
    // SAFETY: `getloadavg` writes at most `nelem` doubles into the buffer, and
    // we pass the true length of a stack array we exclusively own.
    let filled = unsafe { libc::getloadavg(samples.as_mut_ptr(), samples.len() as libc::c_int) };
    if filled >= 1 {
        let one_minute = samples[0] as f32;
        if one_minute.is_finite() && one_minute >= 0.0 {
            return Some(one_minute);
        }
    }
    read_loadavg_via_sysctl()
}

/// Fallback for [`read_loadavg`]: `sysctl -n vm.loadavg` prints `{ 1.23 1.45 1.50 }`.
#[cfg(target_os = "macos")]
fn read_loadavg_via_sysctl() -> Option<f32> {
    let mut command = std::process::Command::new("sysctl");
    command.args(["-n", "vm.loadavg"]);
    macos_loadavg_1m_from_sysctl(&bounded_telemetry_stdout(command)?)
}

/// Parse the 1-minute field out of `sysctl -n vm.loadavg` output
/// (`{ 1.23 1.45 1.50 }`). Tolerates missing braces.
#[cfg(any(test, target_os = "macos"))]
pub(super) fn macos_loadavg_1m_from_sysctl(raw: &str) -> Option<f32> {
    raw.split(|c: char| c.is_whitespace() || c == '{' || c == '}')
        .find(|token| !token.is_empty())
        .and_then(|token| token.parse::<f32>().ok())
        .filter(|value| value.is_finite() && *value >= 0.0)
}

/// Seconds since the last keyboard/mouse input from the console user, when
/// the platform exposes it. Used as an *optional* gate for scheduled
/// background maintenance ("only when the human has stepped away"), never for
/// foreground throttling. `None` means "unknown", which callers must treat as
/// "do not gate".
///
/// * macOS: `ioreg -c IOHIDSystem` reports `HIDIdleTime` in nanoseconds.
/// * Linux / others: no portable console-idle signal without a display server
///   dependency; returns `None`. (Load/PSI already cover "machine busy".)
pub(super) fn user_idle_seconds() -> Option<u64> {
    #[cfg(target_os = "macos")]
    {
        let mut command = std::process::Command::new("ioreg");
        command.args(["-c", "IOHIDSystem", "-d", "4", "-r"]);
        macos_hid_idle_seconds_from_ioreg(&bounded_telemetry_stdout(command)?)
    }
    #[cfg(not(target_os = "macos"))]
    {
        None
    }
}

/// Parse `"HIDIdleTime" = <nanoseconds>` out of `ioreg` output.
#[cfg(any(test, target_os = "macos"))]
pub(super) fn macos_hid_idle_seconds_from_ioreg(raw: &str) -> Option<u64> {
    raw.lines()
        .filter_map(|line| {
            let (key, value) = line.split_once('=')?;
            if !key.contains("HIDIdleTime") {
                return None;
            }
            value.trim().parse::<u64>().ok()
        })
        .min()
        .map(|nanos| nanos / 1_000_000_000)
}

/// Immediate (no sampler warm-up) machine-pressure verdict for one-shot
/// scheduled jobs. The live governor needs several ticks before its capacity
/// figure means anything; a `cass schedule run` process lives for one job and
/// wants an answer now.
#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize)]
pub struct MachinePressure {
    pub load_per_core: Option<f32>,
    pub psi_cpu_some_avg10: Option<f32>,
    /// Above the step-down threshold (`CASS_RESPONSIVENESS_MAX_LOAD_PER_CORE`
    /// / `..._MAX_PSI_AVG10`).
    pub pressured: bool,
    /// Above the severe threshold.
    pub severe: bool,
}

pub fn machine_pressure_now() -> MachinePressure {
    // `CASS_RESPONSIVENESS_DISABLE=1` pins the governor at 100%; the
    // scheduled-work gates built on this probe must honor the same kill
    // switch, or "skip governor" would still load-gate cron/daemon jobs.
    if disabled_via_env() {
        return MachinePressure {
            load_per_core: None,
            psi_cpu_some_avg10: None,
            pressured: false,
            severe: false,
        };
    }
    let cfg = GovernorConfig::from_env();
    let snapshot = ProcHealthReader::new().snapshot();
    machine_pressure_for(&snapshot, &cfg)
}

pub(super) fn machine_pressure_for(
    snapshot: &HealthSnapshot,
    cfg: &GovernorConfig,
) -> MachinePressure {
    MachinePressure {
        load_per_core: snapshot.load_per_core,
        psi_cpu_some_avg10: snapshot.psi_cpu_some_avg10,
        pressured: snapshot.is_pressured(cfg),
        severe: snapshot.is_severe(cfg),
    }
}

/// Optional "human has stepped away" gate for scheduled maintenance.
/// `CASS_RESPONSIVENESS_MIN_USER_IDLE_SECS=<N>` (default `0` = disabled)
/// requires at least `N` seconds of console idle time before scheduled
/// semantic backfill / scheduled index jobs run. When the platform cannot
/// report idle time the gate passes (fail open) so a Linux headless box is
/// never wedged by a signal it does not have.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct UserIdleGate {
    pub required_secs: u64,
    pub observed_secs: Option<u64>,
    pub satisfied: bool,
}

pub fn user_idle_gate() -> UserIdleGate {
    let required_secs = env_u64("CASS_RESPONSIVENESS_MIN_USER_IDLE_SECS").unwrap_or(0);
    let observed_secs = if required_secs == 0 {
        None
    } else {
        user_idle_seconds()
    };
    user_idle_gate_for(required_secs, observed_secs)
}

pub(super) fn user_idle_gate_for(required_secs: u64, observed_secs: Option<u64>) -> UserIdleGate {
    let satisfied = required_secs == 0 || observed_secs.is_none_or(|idle| idle >= required_secs);
    UserIdleGate {
        required_secs,
        observed_secs,
        satisfied,
    }
}

#[cfg(target_os = "linux")]
pub(super) fn read_psi_cpu_some_avg10() -> Option<f32> {
    let raw = std::fs::read_to_string("/proc/pressure/cpu").ok()?;
    // Expected format (first line):
    //   some avg10=0.00 avg60=0.00 avg300=0.00 total=0
    for line in raw.lines() {
        if let Some(rest) = line.strip_prefix("some ") {
            for token in rest.split_whitespace() {
                if let Some(v) = token.strip_prefix("avg10=") {
                    return v.parse::<f32>().ok();
                }
            }
        }
    }
    None
}

/// GH #496: the memory this process may still use under its cgroup limits.
///
/// Host `MemAvailable` ignores systemd `MemoryMax`, container limits and
/// memory-capped slices, so budgets sized from it (in-flight bytes, writer
/// heaps, caches) could exceed the real ceiling and swap-thrash or OOM inside
/// a 10 GB unit on a larger host. Walks from this process's cgroup to the
/// root and returns the tightest level's headroom: `memory.max - usage`,
/// crediting that level's reclaimable `inactive_file` page cache the way
/// `MemAvailable` does. `None` when no level sets a limit.
#[cfg(any(target_os = "linux", test))]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(super) fn cgroup_memory_budget(
    proc_self_cgroup: &str,
    cgroup_root: &std::path::Path,
) -> Option<CgroupMemoryBudget> {
    let read = |path: std::path::PathBuf| std::fs::read_to_string(path).ok();
    let number = |text: Option<String>| text?.trim().parse::<u64>().ok();
    let stat_field = |text: Option<String>, field: &str| {
        text?.lines().find_map(|line| {
            let (key, value) = line.split_once(' ')?;
            (key == field).then(|| value.trim().parse::<u64>().ok())?
        })
    };
    // cgroup v1 reports "no limit" as a page-rounded i64::MAX.
    const V1_UNLIMITED_FLOOR: u64 = 1 << 62;
    let mut tightest: Option<CgroupMemoryBudget> = None;
    let mut consider = |limit: u64, usage: u64, inactive_file: u64, limit_level: String| {
        let headroom = limit
            .saturating_sub(usage.saturating_sub(inactive_file))
            .min(limit);
        tightest = Some(match tightest.take() {
            // The walk runs leaf to root, so on a tie the deeper level keeps
            // the attribution.
            Some(current) if current.limit <= limit => CgroupMemoryBudget {
                headroom: current.headroom.min(headroom),
                ..current
            },
            Some(current) => CgroupMemoryBudget {
                limit,
                headroom: current.headroom.min(headroom),
                limit_level,
            },
            None => CgroupMemoryBudget {
                limit,
                headroom,
                limit_level,
            },
        });
    };
    let level_name = |version: &str, base: &std::path::Path, level: &std::path::Path| {
        let relative = level.strip_prefix(base).unwrap_or(level);
        format!("{version}:/{}", relative.display())
    };
    for line in proc_self_cgroup.lines() {
        let mut fields = line.splitn(3, ':');
        let (Some(_), Some(controllers), Some(relative)) =
            (fields.next(), fields.next(), fields.next())
        else {
            continue;
        };
        let relative = relative.trim_start_matches('/');
        let (base, v2) = if controllers.is_empty() {
            (cgroup_root.to_path_buf(), true)
        } else if controllers
            .split(',')
            .any(|controller| controller == "memory")
        {
            (cgroup_root.join("memory"), false)
        } else {
            continue;
        };
        let mut level = base.join(relative);
        loop {
            if v2 {
                if let Some(limit) = number(read(level.join("memory.max"))) {
                    let usage = number(read(level.join("memory.current"))).unwrap_or(0);
                    let inactive = stat_field(read(level.join("memory.stat")), "inactive_file");
                    consider(
                        limit,
                        usage,
                        inactive.unwrap_or(0),
                        level_name("cgroup_v2", &base, &level),
                    );
                }
            } else if let Some(limit) = number(read(level.join("memory.limit_in_bytes")))
                && limit < V1_UNLIMITED_FLOOR
            {
                let usage = number(read(level.join("memory.usage_in_bytes"))).unwrap_or(0);
                let stat = read(level.join("memory.stat"));
                let inactive = stat_field(stat.clone(), "total_inactive_file")
                    .or_else(|| stat_field(stat, "inactive_file"));
                consider(
                    limit,
                    usage,
                    inactive.unwrap_or(0),
                    level_name("cgroup_v1", &base, &level),
                );
            }
            if level == base || !level.pop() || !level.starts_with(&base) {
                break;
            }
        }
    }
    tightest
}

#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CgroupMemoryBudget {
    pub(super) limit: u64,
    pub(super) headroom: u64,
    /// The level whose `memory.max` / `memory.limit_in_bytes` is `limit`,
    /// e.g. `cgroup_v2:/user.slice/app.scope`.
    pub(super) limit_level: String,
}

#[cfg(target_os = "linux")]
fn current_cgroup_memory_budget() -> Option<CgroupMemoryBudget> {
    let proc_self_cgroup = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    cgroup_memory_budget(&proc_self_cgroup, std::path::Path::new("/sys/fs/cgroup"))
}

pub fn available_memory_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
        let host = proc_kib_field_bytes(&meminfo, "MemAvailable:")?;
        Some(current_cgroup_memory_budget().map_or(host, |budget| host.min(budget.headroom)))
    }
    // #294: on macOS there is no `/proc/meminfo`, so the Linux probe returned
    // `None` and the entire host-memory-reserve throttle in the lexical
    // rebuild pipeline was silently disabled — letting the post-ingest
    // sort/rebuild peak push a 16 GB Mac into red memory pressure. Parse
    // `vm_stat` (pages free + inactive + speculative + purgeable ≈ reclaimable
    // memory) so the throttle actually engages.
    #[cfg(target_os = "macos")]
    {
        let text = bounded_telemetry_stdout(std::process::Command::new("vm_stat"))?;
        macos_available_memory_bytes_from_vm_stat(&text)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

pub fn total_memory_bytes() -> Option<u64> {
    memory_total().map(|total| total.effective_bytes)
}

/// GH #496: the memory total budgets are sized from, the host total it was
/// clamped from, and what set it. `status` reports all three, so an operator
/// can see whether a `MemoryMax` or container limit actually reached this
/// process instead of inferring it from derived budgets.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
    not(any(target_os = "linux", target_os = "macos", test)),
    allow(dead_code)
)]
pub struct MemoryTotal {
    pub effective_bytes: u64,
    pub host_bytes: u64,
    /// `host`, or the cgroup level whose limit is below the host total
    /// (`cgroup_v2:/<path>` or `cgroup_v1:/<path>`).
    pub limit_source: String,
}

pub fn memory_total() -> Option<MemoryTotal> {
    #[cfg(target_os = "linux")]
    {
        let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
        let host = proc_kib_field_bytes(&meminfo, "MemTotal:")?;
        Some(clamp_memory_total(host, current_cgroup_memory_budget()))
    }
    #[cfg(target_os = "macos")]
    {
        let mut command = std::process::Command::new("sysctl");
        command.args(["-n", "hw.memsize"]);
        let host = bounded_telemetry_stdout(command)?
            .trim()
            .parse::<u64>()
            .ok()?;
        Some(MemoryTotal {
            effective_bytes: host,
            host_bytes: host,
            limit_source: "host".to_owned(),
        })
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

#[cfg(any(target_os = "linux", test))]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(super) fn clamp_memory_total(host: u64, budget: Option<CgroupMemoryBudget>) -> MemoryTotal {
    match budget {
        Some(budget) if budget.limit < host => MemoryTotal {
            effective_bytes: budget.limit,
            host_bytes: host,
            limit_source: budget.limit_level,
        },
        _ => MemoryTotal {
            effective_bytes: host,
            host_bytes: host,
            limit_source: "host".to_owned(),
        },
    }
}

/// Process memory used by the indexing responsiveness controller.
///
/// Linux exposes RSS cheaply through `/proc`. On macOS RSS is not the memory
/// pressure number operators see: compressed/private VM ownership is charged
/// to `phys_footprint`, which can be many times larger. Use the OS `footprint`
/// utility there and cache the result so a hot progress loop cannot spawn it
/// repeatedly. `ps` RSS remains a compatibility fallback when `footprint` is
/// unavailable.
pub fn process_resident_memory_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        proc_kib_field_bytes(&status, "VmRSS:")
    }
    #[cfg(target_os = "macos")]
    {
        let now = Instant::now();
        {
            let sample = MACOS_PROCESS_MEMORY_SAMPLE
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if sample
                .sampled_at
                .and_then(|sampled_at| now.checked_duration_since(sampled_at))
                .is_some_and(|age| age < MACOS_PROCESS_MEMORY_SAMPLE_INTERVAL)
            {
                return sample.bytes;
            }
        }

        let bytes = read_macos_process_phys_footprint_bytes().or_else(read_macos_process_rss_bytes);
        let mut sample = MACOS_PROCESS_MEMORY_SAMPLE
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        sample.sampled_at = Some(now);
        sample.bytes = bytes;
        bytes
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

#[cfg(target_os = "macos")]
fn read_macos_process_phys_footprint_bytes() -> Option<u64> {
    phys_footprint_bytes_via_probe(&FOOTPRINT_PROBE_DISABLED, || {
        let pid = std::process::id().to_string();
        let mut command = std::process::Command::new("/usr/bin/footprint");
        command.args(["--pid", &pid, "--noCategories", "--format", "bytes"]);
        run_bounded_telemetry_probe(command, OS_TELEMETRY_PROBE_TIMEOUT)
    })
}

#[cfg(target_os = "macos")]
fn read_macos_process_rss_bytes() -> Option<u64> {
    let pid = std::process::id().to_string();
    let mut command = std::process::Command::new("ps");
    command.args(["-o", "rss=", "-p", &pid]);
    macos_rss_kib_to_bytes(&bounded_telemetry_stdout(command)?)
}

#[cfg(any(target_os = "linux", test))]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn proc_kib_field_bytes(contents: &str, prefix: &str) -> Option<u64> {
    for line in contents.lines() {
        if let Some(rest) = line.strip_prefix(prefix) {
            let kb = rest.split_whitespace().next()?.parse::<u64>().ok()?;
            return kb.checked_mul(1024);
        }
    }
    None
}

/// Parse `vm_stat` output into reclaimable bytes. `vm_stat` reports a page size
/// header (`page size of N bytes`) and per-state page counts. We sum the page
/// classes that are genuinely reclaimable under pressure — free, inactive,
/// speculative, and purgeable — which approximates Linux `MemAvailable`. Kept
/// pure (no command execution) so it is testable on any platform.
#[cfg(any(target_os = "macos", test))]
pub(super) fn macos_available_memory_bytes_from_vm_stat(text: &str) -> Option<u64> {
    let mut page_size: u64 = 4096;
    let mut pages_free: u64 = 0;
    let mut pages_inactive: u64 = 0;
    let mut pages_speculative: u64 = 0;
    let mut pages_purgeable: u64 = 0;
    let mut saw_any = false;

    for line in text.lines() {
        let line = line.trim();
        if let Some(idx) = line.find("page size of") {
            // "Mach Virtual Memory Statistics: (page size of 16384 bytes)"
            let rest = &line[idx + "page size of".len()..];
            if let Some(n) = rest
                .split_whitespace()
                .next()
                .and_then(|t| t.parse::<u64>().ok())
            {
                page_size = n;
            }
            continue;
        }
        let Some((label, value)) = line.split_once(':') else {
            continue;
        };
        let pages = value
            .trim()
            .trim_end_matches('.')
            .split_whitespace()
            .next()
            .and_then(|t| t.parse::<u64>().ok());
        let Some(pages) = pages else { continue };
        match label.trim() {
            "Pages free" => {
                pages_free = pages;
                saw_any = true;
            }
            "Pages inactive" => {
                pages_inactive = pages;
                saw_any = true;
            }
            "Pages speculative" => {
                pages_speculative = pages;
                saw_any = true;
            }
            "Pages purgeable" => {
                pages_purgeable = pages;
                saw_any = true;
            }
            _ => {}
        }
    }

    if !saw_any {
        return None;
    }
    let reclaimable_pages = pages_free
        .saturating_add(pages_inactive)
        .saturating_add(pages_speculative)
        .saturating_add(pages_purgeable);
    reclaimable_pages.checked_mul(page_size)
}

/// Parse `ps -o rss=` output (resident set size in KiB) into bytes. Pure +
/// testable.
#[cfg(any(target_os = "macos", test))]
pub(super) fn macos_rss_kib_to_bytes(text: &str) -> Option<u64> {
    text.trim()
        .lines()
        .next()?
        .trim()
        .parse::<u64>()
        .ok()?
        .checked_mul(1024)
}

/// Parse the byte-formatted `footprint --noCategories` auxiliary field. Match
/// the exact key so `phys_footprint_peak` cannot silently replace the current
/// sample if Apple reorders the output.
#[cfg(any(target_os = "macos", test))]
pub(super) fn macos_phys_footprint_bytes_from_output(text: &str) -> Option<u64> {
    text.lines().find_map(|line| {
        let (key, value) = line.trim().split_once(':')?;
        if key.trim() != "phys_footprint" {
            return None;
        }
        value.split_whitespace().next()?.parse::<u64>().ok()
    })
}
