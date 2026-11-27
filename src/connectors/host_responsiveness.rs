//! Machine-responsiveness governor for the indexing pipeline.
//!
//! The indexer can easily saturate every core of a big host. That is normally
//! fine on a dedicated build box, but on a shared workstation it makes
//! interactive shells, editors, and a foreground `cass search` feel dead
//! while a rebuild runs. This module provides a lightweight, hysteresis-aware
//! governor that publishes a *capacity factor* (an integer percentage in
//! `[min_capacity_pct, 100]`). Callers consult `effective_worker_count`
//! before committing to a worker fan-out, and get back a bounded count that
//! respects the current system load.
//!
//! Design goals (aligned with bead `coding_agent_session_search-d2qix`):
//!
//! * **Conservative by default.** Defaults never grow past caller-requested
//!   fan-out; they only shrink when the box is already under pressure.
//! * **Explainable.** Thresholds live in named env vars and the full decision
//!   history is queryable via `telemetry_snapshot`.
//! * **Non-oscillating.** Shrink is immediate so responsiveness recovers
//!   fast, but growth back to full capacity requires multiple consecutive
//!   healthy ticks (hysteresis) so flapping loads do not flap the worker count.
//! * **Opt-out.** `CASS_RESPONSIVENESS_DISABLE=1` pins capacity to 100% for
//!   before/after comparison runs and sandboxed environments.
//!
//! Signals read on Linux:
//!
//! * `/proc/loadavg` first field (1-minute load average), compared against
//!   the number of logical CPUs.
//! * `/proc/pressure/cpu` `some avg10` (percent of wall-time some task was
//!   delayed on CPU in the last 10s). This is the best single "how
//!   unresponsive does the machine feel" signal available from the kernel.
//!
//! On macOS only the load-average signal is available (`sysctl -n vm.loadavg`);
//! there is no PSI equivalent. On other platforms the reader always reports
//! healthy, so the governor reduces to a no-op.
//!
//! Scheduled maintenance (`cass schedule run`, `cass models backfill
//! --scheduled`, the daemon's periodic index spawn) can additionally require
//! console idle time via `CASS_RESPONSIVENESS_MIN_USER_IDLE_SECS` — see
//! `user_idle_gate`. That gate is deliberately *not* consulted by foreground
//! indexing: a human who typed `cass index` wants it to run now.

mod resident;

use std::collections::VecDeque;
use std::sync::{
    Arc, LazyLock, Mutex,
    atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

mod calibration;
mod memory;
#[cfg(test)]
mod tests;

use calibration::{CalibrationMode, CalibrationTelemetry, GovernorCalibration};
pub(crate) use memory::{
    available_memory_bytes, machine_pressure_now, memory_total, process_resident_memory_bytes,
    total_memory_bytes, user_idle_gate,
};

/// Lower bound for the published capacity, as a percentage of the caller's
/// desired fan-out. Never shrinks below this regardless of signals.
const DEFAULT_MIN_CAPACITY_PCT: u32 = 25;

/// Loadavg / ncpu threshold above which the governor shrinks by one step.
const DEFAULT_MAX_LOAD_PER_CORE: f32 = 1.25;

/// Loadavg / ncpu threshold above which the governor shrinks *hard* to the
/// floor.
const DEFAULT_SEVERE_LOAD_PER_CORE: f32 = 1.75;

/// PSI cpu `some avg10` threshold above which the governor shrinks by one step.
const DEFAULT_MAX_PSI_AVG10: f32 = 20.0;

/// PSI cpu `some avg10` threshold above which the governor shrinks to the
/// floor.
const DEFAULT_SEVERE_PSI_AVG10: f32 = 40.0;

/// Number of consecutive healthy ticks required before capacity is grown
/// back toward 100%. Prevents flapping under bursty load.
const DEFAULT_GROWTH_CONSECUTIVE_HEALTHY_TICKS: u32 = 3;

/// Background sampling interval. Shorter = more responsive throttling, but
/// more wasted wakeups on an idle box.
pub(super) const DEFAULT_TICK_SECS: u64 = 2;

/// Fallback process-wide in-flight byte ceiling for responsiveness-governed
/// maintenance work when memory telemetry is unavailable.
const DEFAULT_MAX_INFLIGHT_BYTES: usize = 512 * 1024 * 1024;

/// High-memory hosts should be able to keep more batches in flight without
/// needing manual env tuning. Use a small fraction of currently available RAM,
/// bounded so laptops keep the old conservative behavior and large servers do
/// not accidentally reserve unbounded heap.
const DEFAULT_MAX_INFLIGHT_MEMORY_FRACTION_DENOMINATOR: u64 = 32;
const DEFAULT_MAX_INFLIGHT_BYTES_CEILING: u64 = 16 * 1024 * 1024 * 1024;

/// Lower bound applied only after scaling a non-zero in-flight byte budget.
/// This prevents pressure shrinkage from producing tiny queue budgets that
/// thrash, while still never increasing a caller-requested smaller budget.
const DEFAULT_MIN_INFLIGHT_BYTES: usize = 1024 * 1024;

/// Maximum number of decisions retained in the telemetry ring buffer.
/// Sized so the structure stays under 16 KB and covers ~4 minutes of
/// history at the default 2-second tick.
const TELEMETRY_DECISION_HISTORY: usize = 128;

/// Default calibration window size for split-conformal pressure thresholds.
/// 4 096 samples at the default 2 s tick = ~2.3 hours of history, which
/// comfortably spans a typical workday's duty cycle without inflating
/// memory beyond a few KiB per signal.
const DEFAULT_CONFORMAL_K: usize = 256;

/// Minimum number of calibration samples required before the conformal
/// quantile is emitted. Below this, the governor falls back to static
/// thresholds for the current tick (deterministic conservative fallback).
const DEFAULT_CONFORMAL_K_MIN: usize = 32;

/// Default coverage level for the "pressured" quantile. Higher α = more
/// false-positive shrinks, but faster response to real pressure. 0.05
/// means we expect ≈ 1 false-positive step-down per 20 healthy ticks.
const DEFAULT_CONFORMAL_ALPHA_PRESSURED: f32 = 0.05;

/// Default coverage level for the "severe" quantile. Tighter than
/// pressured because drop-to-floor is disruptive. 0.01 means ≈ 1
/// false-positive floor-drop per 100 healthy ticks.
const DEFAULT_CONFORMAL_ALPHA_SEVERE: f32 = 0.01;

/// Page-Hinkley drift-detection parameter δ (allowed mean-drift
/// tolerance). A larger δ tolerates more drift before triggering a
/// calibration reset; smaller δ is stricter but flaps more often.
const DEFAULT_DRIFT_DELTA: f32 = 0.01;

/// Page-Hinkley trigger threshold λ. Once the cumulative drift signal
/// exceeds λ, declare change-point and reset the calibration window.
/// Conservative default — tuned so stationary streams trip < 1 time
/// per 10k samples on average.
const DEFAULT_DRIFT_LAMBDA: f32 = 0.5;

#[derive(Clone, Copy, Debug)]
pub(crate) struct GovernorConfig {
    pub available_parallelism: usize,
    pub reserved_cores: usize,
    pub max_workers: usize,
    pub max_inflight_bytes: usize,
    pub min_inflight_bytes: usize,
    pub min_capacity_pct: u32,
    pub max_load_per_core: f32,
    pub severe_load_per_core: f32,
    pub max_psi_avg10: f32,
    pub severe_psi_avg10: f32,
    pub growth_consecutive_healthy_ticks: u32,
    pub tick: Duration,
    pub disabled: bool,
    /// Threshold policy for pressured/severe signal classification.
    /// `Static` preserves the pre-conformal behaviour bit-for-bit.
    pub calibration_mode: CalibrationMode,
    /// Calibration window size for split-conformal quantile. Only used
    /// when `calibration_mode == CalibrationMode::Conformal`.
    pub conformal_k: usize,
    /// Minimum samples before the conformal path emits a threshold.
    pub conformal_k_min: usize,
    /// α for the pressured quantile (expected FP rate per tick).
    pub conformal_alpha_pressured: f32,
    /// α for the severe quantile (always ≤ alpha_pressured so
    /// `q̂_severe` ≥ `q̂_pressured`).
    pub conformal_alpha_severe: f32,
    /// Page-Hinkley δ parameter (mean-drift tolerance).
    pub drift_delta: f32,
    /// Page-Hinkley λ threshold (triggers calibration reset).
    pub drift_lambda: f32,
}

impl GovernorConfig {
    pub fn from_env() -> Self {
        let available_parallelism = available_parallelism();
        let reserved_cores = env_usize("CASS_RESPONSIVENESS_RESERVED_CORES")
            .unwrap_or_else(|| default_reserved_cores_for_available(available_parallelism))
            .min(available_parallelism.saturating_sub(1));
        let worker_ceiling = worker_ceiling_for(available_parallelism, reserved_cores);
        let max_workers = env_usize("CASS_RESPONSIVENESS_MAX_WORKERS")
            .filter(|v| *v > 0)
            .map(|v| v.min(worker_ceiling))
            .unwrap_or(worker_ceiling)
            .max(1);
        let max_inflight_bytes = env_usize("CASS_RESPONSIVENESS_MAX_INFLIGHT_BYTES")
            .filter(|v| *v > 0)
            .unwrap_or_else(default_max_inflight_bytes);
        let min_inflight_bytes = env_usize("CASS_RESPONSIVENESS_MIN_INFLIGHT_BYTES")
            .filter(|v| *v > 0)
            .map(|v| v.min(max_inflight_bytes))
            .unwrap_or(DEFAULT_MIN_INFLIGHT_BYTES.min(max_inflight_bytes))
            .max(1);
        let min_capacity_pct = env_u32("CASS_RESPONSIVENESS_MIN_CAPACITY_PCT")
            .map(|v| v.clamp(10, 100))
            .unwrap_or(DEFAULT_MIN_CAPACITY_PCT);
        let max_load_per_core =
            env_f32("CASS_RESPONSIVENESS_MAX_LOAD_PER_CORE").unwrap_or(DEFAULT_MAX_LOAD_PER_CORE);
        let severe_load_per_core = env_f32("CASS_RESPONSIVENESS_SEVERE_LOAD_PER_CORE")
            .unwrap_or(DEFAULT_SEVERE_LOAD_PER_CORE);
        let max_psi_avg10 =
            env_f32("CASS_RESPONSIVENESS_MAX_PSI_AVG10").unwrap_or(DEFAULT_MAX_PSI_AVG10);
        let severe_psi_avg10 =
            env_f32("CASS_RESPONSIVENESS_SEVERE_PSI_AVG10").unwrap_or(DEFAULT_SEVERE_PSI_AVG10);
        let growth_consecutive_healthy_ticks = env_u32("CASS_RESPONSIVENESS_GROWTH_TICKS")
            .unwrap_or(DEFAULT_GROWTH_CONSECUTIVE_HEALTHY_TICKS);
        let tick_secs = env_u32("CASS_RESPONSIVENESS_TICK_SECS")
            .map(|v| v as u64)
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_TICK_SECS);
        let disabled = env_bool_truthy("CASS_RESPONSIVENESS_DISABLE");

        // Conformal knobs. Never clamped to zero — a zero `K` or `K_min`
        // would make the calibration path operate on an empty window,
        // which we handle by refusing to emit a threshold. But values
        // outside sane ranges are clamped so env misconfig can't make
        // the governor do something absurd.
        // Default is `Conformal` (post-flip): every host gets an
        // adaptive per-host threshold with a finite-sample coverage
        // guarantee. Users can pin the legacy static thresholds back on
        // with `CASS_RESPONSIVENESS_CALIBRATION=static`.
        let calibration_mode = match dotenvy::var("CASS_RESPONSIVENESS_CALIBRATION")
            .ok()
            .as_deref()
            .map(str::trim)
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("static") => CalibrationMode::Static,
            _ => CalibrationMode::Conformal,
        };
        let conformal_k = env_u32("CASS_RESPONSIVENESS_CONFORMAL_K")
            .map(|v| (v as usize).clamp(16, 4096))
            .unwrap_or(DEFAULT_CONFORMAL_K);
        let conformal_k_min = env_u32("CASS_RESPONSIVENESS_CONFORMAL_K_MIN")
            .map(|v| (v as usize).clamp(4, conformal_k))
            .unwrap_or(DEFAULT_CONFORMAL_K_MIN.min(conformal_k));
        let conformal_alpha_pressured = env_f32("CASS_RESPONSIVENESS_CONFORMAL_ALPHA_PRESSURED")
            .filter(|v| v.is_finite() && *v > 0.0 && *v < 0.5)
            .unwrap_or(DEFAULT_CONFORMAL_ALPHA_PRESSURED);
        let conformal_alpha_severe = env_f32("CASS_RESPONSIVENESS_CONFORMAL_ALPHA_SEVERE")
            .filter(|v| v.is_finite() && *v > 0.0 && *v < conformal_alpha_pressured)
            .unwrap_or_else(|| DEFAULT_CONFORMAL_ALPHA_SEVERE.min(conformal_alpha_pressured * 0.5));
        let drift_delta = env_f32("CASS_RESPONSIVENESS_DRIFT_DELTA")
            .filter(|v| v.is_finite() && *v > 0.0 && *v < 10.0)
            .unwrap_or(DEFAULT_DRIFT_DELTA);
        let drift_lambda = env_f32("CASS_RESPONSIVENESS_DRIFT_LAMBDA")
            .filter(|v| v.is_finite() && *v > 0.0 && *v < 100.0)
            .unwrap_or(DEFAULT_DRIFT_LAMBDA);

        Self {
            available_parallelism,
            reserved_cores,
            max_workers,
            max_inflight_bytes,
            min_inflight_bytes,
            min_capacity_pct,
            max_load_per_core,
            severe_load_per_core,
            max_psi_avg10,
            severe_psi_avg10,
            growth_consecutive_healthy_ticks,
            tick: Duration::from_secs(tick_secs),
            disabled,
            calibration_mode,
            conformal_k,
            conformal_k_min,
            conformal_alpha_pressured,
            conformal_alpha_severe,
            drift_delta,
            drift_lambda,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize)]
pub(crate) struct HealthSnapshot {
    /// Load average (1-minute) divided by the number of CPUs. `None` on
    /// platforms where the signal is unavailable.
    pub load_per_core: Option<f32>,
    /// PSI `some avg10` for cpu. `None` when `/proc/pressure/cpu` isn't
    /// readable (older kernels, non-Linux).
    pub psi_cpu_some_avg10: Option<f32>,
}

impl HealthSnapshot {
    /// Returns true when either signal is above the severe threshold.
    pub fn is_severe(&self, cfg: &GovernorConfig) -> bool {
        self.load_per_core
            .is_some_and(|v| v > cfg.severe_load_per_core)
            || self
                .psi_cpu_some_avg10
                .is_some_and(|v| v > cfg.severe_psi_avg10)
    }

    /// Returns true when either signal is above the "step down" threshold.
    pub fn is_pressured(&self, cfg: &GovernorConfig) -> bool {
        self.load_per_core
            .is_some_and(|v| v > cfg.max_load_per_core)
            || self
                .psi_cpu_some_avg10
                .is_some_and(|v| v > cfg.max_psi_avg10)
    }
}

/// Classification of why the governor chose a given next-capacity value.
/// Serialized with snake_case tags so robot-mode consumers can switch on a
/// stable string vocabulary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum GovernorDecisionReason {
    /// Governor was disabled via config; capacity is pinned at 100%.
    Disabled,
    /// Severe pressure observed; capacity dropped straight to the floor.
    Severe,
    /// Moderate pressure observed; capacity stepped down by 25pp.
    Pressured,
    /// Pressure present but capacity already at the floor; held.
    PressuredFloorHold,
    /// Sample healthy, healthy-streak not yet long enough to grow; held.
    HealthyHold,
    /// Sample healthy, streak threshold reached, capacity grew by 25pp.
    HealthyGrow,
    /// Sample healthy, streak threshold reached but already at 100%; held.
    HealthyCeilingHold,
}

/// Decide what the new published capacity should be given the latest signal
/// snapshot, the previous capacity, and an internal "healthy streak" counter.
///
/// Returns `(next_capacity_pct, next_healthy_streak, reason)`. The `reason`
/// lets callers record *why* a decision was made, not just what the decision
/// was. This is the core input to the telemetry surface that bead
/// `coding_agent_session_search-d2qix` asks for.
pub(crate) fn next_capacity(
    prev_capacity_pct: u32,
    healthy_streak: u32,
    snapshot: &HealthSnapshot,
    cfg: &GovernorConfig,
) -> (u32, u32, GovernorDecisionReason) {
    if cfg.disabled {
        return (100, 0, GovernorDecisionReason::Disabled);
    }

    if snapshot.is_severe(cfg) {
        // Severe pressure: drop straight to the floor, reset healthy streak.
        return (cfg.min_capacity_pct, 0, GovernorDecisionReason::Severe);
    }

    if snapshot.is_pressured(cfg) {
        // Moderate pressure: take a 25pp step down, but never below floor.
        let step_down = prev_capacity_pct
            .saturating_sub(25)
            .max(cfg.min_capacity_pct);
        let reason = if step_down == prev_capacity_pct {
            GovernorDecisionReason::PressuredFloorHold
        } else {
            GovernorDecisionReason::Pressured
        };
        return (step_down, 0, reason);
    }

    // Healthy sample. Require N consecutive healthy ticks before growing back.
    let new_streak = healthy_streak.saturating_add(1);
    if new_streak >= cfg.growth_consecutive_healthy_ticks {
        let grown = prev_capacity_pct.saturating_add(25).min(100);
        if grown > prev_capacity_pct {
            // Reset streak after a successful growth step so each step
            // requires a fresh N-tick run of healthy samples.
            return (grown, 0, GovernorDecisionReason::HealthyGrow);
        }
        // Already at the ceiling; hold capacity and keep the streak at its
        // current value so we don't keep incrementing an unbounded counter.
        (
            grown,
            new_streak,
            GovernorDecisionReason::HealthyCeilingHold,
        )
    } else {
        (
            prev_capacity_pct,
            new_streak,
            GovernorDecisionReason::HealthyHold,
        )
    }
}

/// Scale a caller-requested worker count by the current capacity. Always
/// returns at least 1 to keep the pipeline moving.
pub(crate) fn scale_worker_count(desired: usize, capacity_pct: u32) -> usize {
    if desired == 0 {
        return 0;
    }
    let capacity = capacity_pct.clamp(1, 100) as usize;
    let scaled = desired.saturating_mul(capacity) / 100;
    scaled.max(1)
}

fn available_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

pub(crate) fn default_reserved_cores_for_available(available_parallelism: usize) -> usize {
    match available_parallelism {
        0 | 1 => 0,
        2..=4 => 1,
        5..=16 => 2,
        n => (n / 8).clamp(2, 8),
    }
}

fn worker_ceiling_for(available_parallelism: usize, reserved_cores: usize) -> usize {
    available_parallelism
        .max(1)
        .saturating_sub(reserved_cores)
        .max(1)
}

pub(crate) fn scale_worker_count_with_policy(
    desired: usize,
    capacity_pct: u32,
    cfg: &GovernorConfig,
) -> usize {
    if desired == 0 {
        return 0;
    }
    let ceiling = worker_ceiling_for(cfg.available_parallelism, cfg.reserved_cores)
        .min(cfg.max_workers.max(1));
    scale_worker_count(desired.min(ceiling), capacity_pct)
}

pub(crate) fn scale_inflight_byte_limit(
    desired_bytes: usize,
    capacity_pct: u32,
    cfg: &GovernorConfig,
) -> usize {
    if desired_bytes == 0 {
        return 0;
    }
    let capped = desired_bytes.min(cfg.max_inflight_bytes.max(1));
    let capacity = capacity_pct.clamp(1, 100) as usize;
    let scaled = capped.saturating_mul(capacity) / 100;
    scaled.max(cfg.min_inflight_bytes.min(capped)).max(1)
}

/// Reader abstraction for the health signals. Stubbed in tests so the
/// hysteresis policy can be exercised without touching /proc.
pub(crate) trait HealthReader: Send + Sync {
    fn snapshot(&self) -> HealthSnapshot;
}

pub(crate) struct ProcHealthReader {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    ncpu: usize,
}

impl ProcHealthReader {
    pub fn new() -> Self {
        Self {
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            ncpu: available_parallelism(),
        }
    }
}

impl HealthReader for ProcHealthReader {
    #[cfg(target_os = "linux")]
    fn snapshot(&self) -> HealthSnapshot {
        let load_per_core = memory::read_loadavg().map(|l1| {
            if self.ncpu == 0 {
                l1
            } else {
                l1 / self.ncpu as f32
            }
        });
        let psi_cpu_some_avg10 = memory::read_psi_cpu_some_avg10();
        HealthSnapshot {
            load_per_core,
            psi_cpu_some_avg10,
        }
    }

    // macOS has no `/proc`, but the 1-minute load average is one `sysctl`
    // away. Without this the governor reported "healthy" unconditionally on
    // Apple hardware, so background/scheduled indexing had no idle signal at
    // all there (the whole point of running it "when the machine is quiet").
    #[cfg(target_os = "macos")]
    fn snapshot(&self) -> HealthSnapshot {
        let load_per_core = memory::read_loadavg().map(|l1| {
            if self.ncpu == 0 {
                l1
            } else {
                l1 / self.ncpu as f32
            }
        });
        HealthSnapshot {
            load_per_core,
            psi_cpu_some_avg10: None,
        }
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    fn snapshot(&self) -> HealthSnapshot {
        HealthSnapshot {
            load_per_core: None,
            psi_cpu_some_avg10: None,
        }
    }
}

/// One recorded decision, suitable for inclusion in the robot telemetry
/// surface. Kept deliberately small (a few tens of bytes) so the ring
/// buffer's memory footprint stays bounded.
#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize)]
pub(crate) struct GovernorDecision {
    /// Time since governor startup, in milliseconds.
    pub at_elapsed_ms: u64,
    pub prev_capacity_pct: u32,
    pub next_capacity_pct: u32,
    pub reason: GovernorDecisionReason,
    pub snapshot: HealthSnapshot,
}

/// Telemetry snapshot returned by [`telemetry_snapshot`]. Operators and
/// automated callers can render this as JSON to understand why the governor
/// chose the currently-published capacity.
#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct GovernorTelemetry {
    pub current_capacity_pct: u32,
    pub resource_policy: ResourcePolicyTelemetry,
    pub healthy_streak: u32,
    pub shrink_count: u64,
    pub grow_count: u64,
    pub ticks_total: u64,
    pub disabled_via_env: bool,
    pub last_snapshot: Option<HealthSnapshot>,
    pub last_reason: Option<GovernorDecisionReason>,
    /// Oldest → newest. Bounded at [`TELEMETRY_DECISION_HISTORY`].
    pub recent_decisions: Vec<GovernorDecision>,
    /// Present only when `CASS_RESPONSIVENESS_CALIBRATION=conformal`.
    /// Carries the current calibration-window fill, recent drift trips,
    /// and the quantiles the conformal policy is emitting for each
    /// signal × severity pairing.
    pub calibration: Option<CalibrationTelemetry>,
    /// Live resident-memory observation, absent before the sampler has run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory: Option<resident::MemoryTelemetry>,
}

#[derive(Clone, Copy, Debug, serde::Serialize)]
pub(crate) struct ResourcePolicyTelemetry {
    pub available_parallelism: usize,
    pub reserved_cores: usize,
    pub max_workers: usize,
    pub effective_worker_ceiling: usize,
    pub max_inflight_bytes: usize,
    pub min_inflight_bytes: usize,
}

impl ResourcePolicyTelemetry {
    fn from_config(cfg: &GovernorConfig) -> Self {
        let effective_worker_ceiling =
            worker_ceiling_for(cfg.available_parallelism, cfg.reserved_cores)
                .min(cfg.max_workers.max(1));
        Self {
            available_parallelism: cfg.available_parallelism,
            reserved_cores: cfg.reserved_cores,
            max_workers: cfg.max_workers,
            effective_worker_ceiling,
            max_inflight_bytes: cfg.max_inflight_bytes,
            min_inflight_bytes: cfg.min_inflight_bytes,
        }
    }
}

struct GovernorRuntimeState {
    recent_decisions: VecDeque<GovernorDecision>,
    last_snapshot: Option<HealthSnapshot>,
    last_reason: Option<GovernorDecisionReason>,
    calibration: Option<GovernorCalibration>,
}

impl GovernorRuntimeState {
    fn new(cfg: &GovernorConfig) -> Self {
        let calibration = match cfg.calibration_mode {
            CalibrationMode::Conformal => Some(GovernorCalibration::new(cfg)),
            CalibrationMode::Static => None,
        };
        Self {
            recent_decisions: VecDeque::with_capacity(TELEMETRY_DECISION_HISTORY),
            last_snapshot: None,
            last_reason: None,
            calibration,
        }
    }
}

struct Governor {
    cfg: GovernorConfig,
    current_capacity: AtomicU32,
    healthy_streak: AtomicU32,
    shrink_count: AtomicU64,
    grow_count: AtomicU64,
    ticks_total: AtomicU64,
    started: AtomicBool,
    reader: Arc<dyn HealthReader>,
    runtime: Mutex<GovernorRuntimeState>,
    started_at: Instant,
}

impl Governor {
    fn new(cfg: GovernorConfig, reader: Arc<dyn HealthReader>) -> Self {
        Self {
            cfg,
            current_capacity: AtomicU32::new(100),
            healthy_streak: AtomicU32::new(0),
            shrink_count: AtomicU64::new(0),
            grow_count: AtomicU64::new(0),
            ticks_total: AtomicU64::new(0),
            started: AtomicBool::new(false),
            reader,
            runtime: Mutex::new(GovernorRuntimeState::new(&cfg)),
            started_at: Instant::now(),
        }
    }

    fn ensure_started(self: &Arc<Self>) {
        if self.cfg.disabled {
            return;
        }
        if self
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            // Another thread already claimed the spawn slot.
            return;
        }

        let me = Arc::clone(self);
        // Background sampler. One long-lived daemon thread per process.
        let spawn_result = thread::Builder::new()
            .name("cass-responsiveness-governor".into())
            .spawn(move || me.run());

        if let Err(err) = spawn_result {
            // Spawn failed (usually RLIMIT_NPROC). Roll back the started flag
            // so a later caller can retry when resource pressure eases, and
            // leave `current_capacity` pinned at its initial 100. We
            // deliberately do not panic: the indexer must keep making progress
            // even when the governor can't.
            self.started.store(false, Ordering::Release);
            tracing::warn!(
                error = %err,
                "failed to spawn cass responsiveness governor thread; capacity pinned at 100% until a later start succeeds"
            );
        }
    }

    fn run(&self) {
        loop {
            resident::sample(&self.cfg);
            self.step_once();
            thread::sleep(self.cfg.tick);
        }
    }

    /// Fold the current tick's snapshot into the calibration window (if
    /// conformal mode is active) and return the `GovernorConfig` to use
    /// for the decision. In static mode this is identical to `self.cfg`;
    /// in conformal mode the four threshold fields are overridden by the
    /// current quantile estimates (or left at static values if the
    /// calibration isn't ready yet).
    fn apply_calibration(
        &self,
        snapshot: &HealthSnapshot,
        is_healthy_tick: bool,
    ) -> GovernorConfig {
        if self.cfg.calibration_mode == CalibrationMode::Static {
            return self.cfg;
        }
        let mut runtime = self
            .runtime
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(cal) = runtime.calibration.as_mut() else {
            // This can happen if someone constructs a Governor with
            // CalibrationMode::Conformal but forgets to populate the
            // runtime calibration. Static fallback is always safe.
            return self.cfg;
        };
        match cal.observe_and_compute_thresholds(snapshot, is_healthy_tick, &self.cfg) {
            Some(effective) => effective.apply_to(&self.cfg),
            None => self.cfg,
        }
    }

    /// Apply one sampling tick. Split out from `run()` so unit tests can
    /// drive deterministic sequences through the decision machinery without
    /// spawning a background thread or sleeping.
    fn step_once(&self) {
        let snapshot = self.reader.snapshot();
        let prev = self.current_capacity.load(Ordering::Relaxed);
        let streak = self.healthy_streak.load(Ordering::Relaxed);

        // In conformal mode, first classify the tick under the STATIC
        // thresholds: this tells the calibration window whether the
        // sample came from a healthy or pressured regime. Feeding
        // pressured-regime samples into the calibration window would
        // teach the quantile "what pressure looks like" — exactly the
        // opposite of what we want. We never touch static-mode behaviour
        // here: if calibration is disabled, `effective_cfg` stays as
        // `self.cfg` and the decision path below is bit-identical to
        // pre-conformal builds.
        let static_is_pressured = snapshot.is_pressured(&self.cfg);
        let is_healthy_tick = !static_is_pressured;
        let effective_cfg = self.apply_calibration(&snapshot, is_healthy_tick);

        let (next, next_streak, reason) = next_capacity(prev, streak, &snapshot, &effective_cfg);

        if next < prev {
            self.shrink_count.fetch_add(1, Ordering::Relaxed);
        } else if next > prev {
            self.grow_count.fetch_add(1, Ordering::Relaxed);
        }
        self.ticks_total.fetch_add(1, Ordering::Relaxed);
        self.current_capacity.store(next, Ordering::Relaxed);
        self.healthy_streak.store(next_streak, Ordering::Relaxed);

        // Only retain decisions that describe meaningful events: a capacity
        // change, or a pressure signal (even while already pinned at the
        // floor). "Healthy hold" and "healthy ceiling hold" ticks are the
        // vast majority on an idle box and would otherwise flood the ring
        // buffer with useless rows.
        let record_this_tick = next != prev
            || matches!(
                reason,
                GovernorDecisionReason::Severe
                    | GovernorDecisionReason::Pressured
                    | GovernorDecisionReason::PressuredFloorHold
            );

        // `unwrap_or_else(PoisonError::into_inner)` unconditionally yields the
        // guard: a poisoned `Mutex` still holds a valid `GovernorRuntimeState`
        // (its invariants are tick-local), so silently dropping telemetry
        // forever after a single panic-while-locked is strictly worse than
        // keeping it flowing. The `if let Ok` pattern we used before had
        // exactly that failure mode.
        let mut runtime = self
            .runtime
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        runtime.last_snapshot = Some(snapshot);
        runtime.last_reason = Some(reason);
        if record_this_tick {
            if runtime.recent_decisions.len() >= TELEMETRY_DECISION_HISTORY {
                runtime.recent_decisions.pop_front();
            }
            runtime.recent_decisions.push_back(GovernorDecision {
                at_elapsed_ms: self.started_at.elapsed().as_millis() as u64,
                prev_capacity_pct: prev,
                next_capacity_pct: next,
                reason,
                snapshot,
            });
        }
        drop(runtime);

        if next != prev {
            tracing::info!(
                prev_capacity_pct = prev,
                next_capacity_pct = next,
                reason = ?reason,
                load_per_core = ?snapshot.load_per_core,
                psi_cpu_some_avg10 = ?snapshot.psi_cpu_some_avg10,
                "cass responsiveness governor updated capacity"
            );
        }
    }

    #[cfg(test)]
    fn telemetry(&self) -> GovernorTelemetry {
        // Same poison-safe acquisition pattern as `step_once`: the runtime
        // state is tick-local, so reading the most-recent-committed history
        // after a panic-while-locked is strictly more useful than returning
        // an empty slice for the rest of the process lifetime.
        let runtime = self
            .runtime
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let recent: Vec<_> = runtime.recent_decisions.iter().copied().collect();
        let last_snapshot = runtime.last_snapshot;
        let last_reason = runtime.last_reason;
        let calibration = runtime
            .calibration
            .as_ref()
            .map(|cal| cal.telemetry(&self.cfg));
        drop(runtime);
        let disabled = env_bool_truthy("CASS_RESPONSIVENESS_DISABLE") || self.cfg.disabled;
        // When the governor is disabled via env or config, the effective
        // capacity that every caller of `effective_worker_count` /
        // `current_capacity_pct` observes is pinned at 100. Reporting the
        // raw atomic (which may still hold a stale shrunken value from a
        // pre-disable sampler tick) would leave robot consumers with two
        // different "current" values for the same process. Align the
        // telemetry with what the rest of the module reports.
        let current = if disabled {
            100
        } else {
            self.current_capacity.load(Ordering::Relaxed)
        };
        GovernorTelemetry {
            current_capacity_pct: current,
            resource_policy: ResourcePolicyTelemetry::from_config(&self.cfg),
            healthy_streak: self.healthy_streak.load(Ordering::Relaxed),
            shrink_count: self.shrink_count.load(Ordering::Relaxed),
            grow_count: self.grow_count.load(Ordering::Relaxed),
            ticks_total: self.ticks_total.load(Ordering::Relaxed),
            disabled_via_env: disabled,
            last_snapshot,
            last_reason,
            recent_decisions: recent,
            calibration,
            memory: resident::telemetry(),
        }
    }
}

static GOVERNOR: LazyLock<Arc<Governor>> = LazyLock::new(|| {
    Arc::new(Governor::new(
        GovernorConfig::from_env(),
        Arc::new(ProcHealthReader::new()),
    ))
});

/// Read the currently published capacity percentage. Starts the background
/// sampler on first call. Safe to call from any thread.
///
/// When `CASS_RESPONSIVENESS_DISABLE` is truthy, returns 100 unconditionally
/// and skips starting the sampler thread. This check happens on every read
/// (not just at init) so tests, benchmarks, and long-running daemons can
/// toggle the governor live without fighting `LazyLock` init order — the
/// static `GOVERNOR` is constructed at most once per process, but the
/// disable signal is honored at every read site.
pub(crate) fn current_capacity_pct() -> u32 {
    if disabled_via_env() {
        return 100;
    }
    let g = GOVERNOR.clone();
    g.ensure_started();
    g.current_capacity
        .load(Ordering::Relaxed)
        .min(resident::capacity_pct())
}

pub(crate) fn disabled_via_env() -> bool {
    env_bool_truthy("CASS_RESPONSIVENESS_DISABLE")
}

/// Scale a caller-requested worker count by the current governor capacity.
/// Callers pass the *maximum* fan-out they would like (e.g. CPU count minus
/// reserved cores); the governor returns a bounded count that respects the
/// current machine responsiveness policy. Always returns at least 1.
pub(crate) fn effective_worker_count(desired: usize) -> usize {
    let g = GOVERNOR.clone();
    scale_worker_count_with_policy(desired, current_capacity_pct(), &g.cfg)
}

/// Apply the same live capacity factor and explicit byte ceilings to
/// producer-side in-flight byte budgets.
pub(crate) fn effective_inflight_byte_limit(desired_bytes: usize) -> usize {
    let g = GOVERNOR.clone();
    resident::limit_inflight_bytes(scale_inflight_byte_limit(
        desired_bytes,
        current_capacity_pct(),
        &g.cfg,
    ))
}

/// Return the configured governor policy without starting the background
/// sampler. This is for startup-sensitive read-only surfaces such as
/// `cass health --json`, where merely observing telemetry must not spawn a
/// thread or pay the live sampler's first-tick cost.
pub(crate) fn telemetry_snapshot_passive() -> GovernorTelemetry {
    let cfg = GovernorConfig::from_env();
    let calibration = match cfg.calibration_mode {
        CalibrationMode::Conformal => Some(CalibrationTelemetry {
            mode: cfg.calibration_mode,
            load_window_len: 0,
            psi_window_len: 0,
            conformal_k: cfg.conformal_k,
            conformal_k_min: cfg.conformal_k_min,
            conformal_alpha_pressured: cfg.conformal_alpha_pressured,
            conformal_alpha_severe: cfg.conformal_alpha_severe,
            drift_reset_count: 0,
            outliers_rejected: 0,
            observations_total: 0,
            load_pressured_q: None,
            load_severe_q: None,
            psi_pressured_q: None,
            psi_severe_q: None,
        }),
        CalibrationMode::Static => None,
    };
    GovernorTelemetry {
        current_capacity_pct: 100,
        resource_policy: ResourcePolicyTelemetry::from_config(&cfg),
        healthy_streak: 0,
        shrink_count: 0,
        grow_count: 0,
        ticks_total: 0,
        disabled_via_env: cfg.disabled,
        last_snapshot: None,
        last_reason: None,
        recent_decisions: Vec::new(),
        calibration,
        memory: resident::telemetry(),
    }
}

fn env_u32(key: &str) -> Option<u32> {
    dotenvy::var(key).ok().and_then(|v| v.trim().parse().ok())
}

pub(super) fn env_u64(key: &str) -> Option<u64> {
    dotenvy::var(key).ok().and_then(|v| v.trim().parse().ok())
}

fn env_f32(key: &str) -> Option<f32> {
    dotenvy::var(key).ok().and_then(|v| v.trim().parse().ok())
}

fn env_usize(key: &str) -> Option<usize> {
    dotenvy::var(key).ok().and_then(|v| v.trim().parse().ok())
}

fn env_bool_truthy(key: &str) -> bool {
    match dotenvy::var(key) {
        Ok(v) => matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => false,
    }
}

fn default_max_inflight_bytes() -> usize {
    default_max_inflight_bytes_for_available(available_memory_bytes())
}

fn default_max_inflight_bytes_for_available(available_bytes: Option<u64>) -> usize {
    let Some(available_bytes) = available_bytes else {
        return DEFAULT_MAX_INFLIGHT_BYTES;
    };
    let ceiling = usize::try_from(DEFAULT_MAX_INFLIGHT_BYTES_CEILING).unwrap_or(usize::MAX);
    let budget = available_bytes / DEFAULT_MAX_INFLIGHT_MEMORY_FRACTION_DENOMINATOR;
    let budget = budget.min(DEFAULT_MAX_INFLIGHT_BYTES_CEILING);
    let budget = usize::try_from(budget).unwrap_or(ceiling);
    budget.clamp(DEFAULT_MAX_INFLIGHT_BYTES, ceiling)
}
