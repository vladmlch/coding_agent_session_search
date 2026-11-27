use std::collections::VecDeque;

use super::{GovernorConfig, HealthSnapshot};

/// Huber / MAD outlier rejection multiplier. Samples with
/// |v − median| > HUBER_K × MAD are dropped from the calibration
/// window (still used for the current-tick decision). 3.5 is the
/// published Huber constant; equivalent to ≈ 3σ under Normal tails.
const MAD_OUTLIER_K: f32 = 3.5;

/// Which calibration policy the governor uses to compute the next
/// `pressured` / `severe` thresholds for each health signal.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CalibrationMode {
    /// Static, hand-tuned thresholds from `DEFAULT_*` constants plus env
    /// overrides. Default for backwards compatibility — all 17 existing
    /// governor unit tests exercise this path.
    Static,
    /// Split-conformal quantile over a rolling window of healthy-period
    /// samples. Provides a finite-sample coverage guarantee per-host.
    /// See `ALIEN-ARTIFACT-CARD2-SPEC.md` under
    /// `tests/artifacts/perf/2026-04-21-profile-run/`.
    Conformal,
}

// ---------------------------------------------------------------------------
// Conformal calibration machinery (bead `d2qix` Card 2). See
// `tests/artifacts/perf/2026-04-21-profile-run/ALIEN-ARTIFACT-CARD2-SPEC.md`
// for the decision-theoretic spec + proof obligations this code discharges.
// ---------------------------------------------------------------------------

/// Split-conformal quantile per Vovk/Gammerman/Shafer 2005 Theorem 1.
/// Returns `scores[ ⌈(K + 1)(1 − α)⌉ − 1 ]` from a sorted-ascending slice,
/// clamped to the last element when the target index falls outside the
/// available range (which is exactly the convention in the Vovk text:
/// when (K+1)(1-α) > K we fall back to the empirical max).
///
/// Preconditions: `scores.len() ≥ 1`, `0 < alpha < 1`. Violating either
/// returns `None`.
pub(super) fn conformal_quantile_sorted(scores: &[f32], alpha: f32) -> Option<f32> {
    if scores.is_empty() || !alpha.is_finite() || alpha <= 0.0 || alpha >= 1.0 {
        return None;
    }
    // ⌈(K + 1)(1 − α)⌉ − 1, clamped to [0, K−1].
    let k = scores.len();
    let target = ((k as f32 + 1.0) * (1.0 - alpha)).ceil() as isize - 1;
    let idx = target.clamp(0, (k as isize) - 1) as usize;
    Some(scores[idx])
}

/// Rolling ring buffer of healthy-period samples for one signal. Keeps the
/// most recent `K` entries so we can recompute a split-conformal quantile
/// on demand. Writes are cheap (push_back + pop_front when full); reads
/// allocate a sorted copy once per observation request.
#[derive(Debug, Clone)]
pub(super) struct CalibrationStream {
    samples: VecDeque<f32>,
    k: usize,
}

impl CalibrationStream {
    pub(super) fn new(k: usize) -> Self {
        Self {
            samples: VecDeque::with_capacity(k.max(1)),
            k: k.max(1),
        }
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.samples.len()
    }

    fn clear(&mut self) {
        self.samples.clear();
    }

    pub(super) fn push(&mut self, value: f32) {
        if !value.is_finite() {
            return;
        }
        if self.samples.len() >= self.k {
            self.samples.pop_front();
        }
        self.samples.push_back(value);
    }

    /// Median of the current window. Linear-time via sort into a temporary.
    /// We intentionally re-sort per call instead of maintaining a sorted
    /// structure — the window is small (≤ 4 096) and called at most once
    /// per governor tick.
    fn median(&self) -> Option<f32> {
        if self.samples.is_empty() {
            return None;
        }
        let mut buf: Vec<f32> = self.samples.iter().copied().collect();
        buf.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let n = buf.len();
        Some(if n.is_multiple_of(2) {
            (buf[n / 2 - 1] + buf[n / 2]) / 2.0
        } else {
            buf[n / 2]
        })
    }

    /// Median absolute deviation (MAD). Returns None on empty window.
    fn mad(&self) -> Option<f32> {
        let med = self.median()?;
        let mut deviations: Vec<f32> = self.samples.iter().map(|v| (v - med).abs()).collect();
        deviations.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let n = deviations.len();
        if n == 0 {
            return None;
        }
        Some(if n.is_multiple_of(2) {
            (deviations[n / 2 - 1] + deviations[n / 2]) / 2.0
        } else {
            deviations[n / 2]
        })
    }

    /// True iff `v` is a Huber-outlier given the window's current median
    /// and MAD. The gate only engages once the window has enough data
    /// to estimate spread; small windows or degenerate-all-identical
    /// windows (MAD = 0) admit every new sample so we don't get stuck
    /// with a 1-sample window forever.
    pub(super) fn is_outlier(&self, v: f32) -> bool {
        const MIN_SAMPLES_FOR_GATE: usize = 8;
        if self.samples.len() < MIN_SAMPLES_FOR_GATE {
            return false;
        }
        let (Some(med), Some(mad)) = (self.median(), self.mad()) else {
            return false;
        };
        if mad == 0.0 {
            // Everything currently in the window is identical. A new
            // varying value is fine to admit — letting it in is how
            // MAD eventually becomes non-zero. Rejecting it would lock
            // the window at its initial value permanently.
            return false;
        }
        (v - med).abs() > MAD_OUTLIER_K * mad
    }

    /// Compute the split-conformal quantile at the given α. Returns None
    /// when the window has fewer than `k_min` samples (caller falls back
    /// to the static threshold).
    pub(super) fn quantile(&self, alpha: f32, k_min: usize) -> Option<f32> {
        if self.samples.len() < k_min {
            return None;
        }
        let mut sorted: Vec<f32> = self.samples.iter().copied().collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        conformal_quantile_sorted(&sorted, alpha)
    }
}

/// Page-Hinkley drift detector — a one-sided sequential change-point test
/// covering the same ground as §12.13 ADWIN for our purposes but in under
/// 30 lines. Tracks a cumulative deviation statistic `g_t`; when it
/// exceeds `λ` we declare a regime shift and reset.
///
///    µ_t = running mean of observed values
///    m_t = (x_t - µ_t) - δ
///    g_t = max(0, g_{t-1} + m_t)      ← one-sided increase detector
///    g_t = min(g_t', g_{t-1} + m_t)   ← track running minimum
///    declare change iff (g_t - min_g) > λ
#[derive(Debug, Clone)]
pub(super) struct PageHinkley {
    delta: f32,
    lambda: f32,
    n: u64,
    running_mean: f32,
    cumulative: f32,
    min_cumulative: f32,
}

impl PageHinkley {
    pub(super) fn new(delta: f32, lambda: f32) -> Self {
        Self {
            delta,
            lambda,
            n: 0,
            running_mean: 0.0,
            cumulative: 0.0,
            min_cumulative: 0.0,
        }
    }

    pub(super) fn reset(&mut self) {
        self.n = 0;
        self.running_mean = 0.0;
        self.cumulative = 0.0;
        self.min_cumulative = 0.0;
    }

    /// Update with a new observation. Returns `true` iff the cumulative
    /// drift exceeds the configured λ — at that point the caller should
    /// clear its calibration window and reset this detector.
    pub(super) fn observe(&mut self, v: f32) -> bool {
        if !v.is_finite() {
            return false;
        }
        self.n = self.n.saturating_add(1);
        // Welford-style running mean update for numerical stability.
        self.running_mean += (v - self.running_mean) / (self.n as f32);
        self.cumulative += v - self.running_mean - self.delta;
        if self.cumulative < self.min_cumulative {
            self.min_cumulative = self.cumulative;
        }
        (self.cumulative - self.min_cumulative) > self.lambda
    }
}

/// Per-signal calibration state (one load stream, one PSI stream). Each
/// stream holds its own rolling window and its own Page-Hinkley detector
/// so drift on one signal does not invalidate the other.
#[derive(Debug, Clone)]
struct SignalCalibration {
    window: CalibrationStream,
    drift: PageHinkley,
}

impl SignalCalibration {
    fn new(cfg: &GovernorConfig) -> Self {
        Self {
            window: CalibrationStream::new(cfg.conformal_k),
            drift: PageHinkley::new(cfg.drift_delta, cfg.drift_lambda),
        }
    }

    /// Observe a sample taken during a healthy tick. Returns a status the
    /// caller can record into telemetry. `is_healthy_tick` must be true
    /// for the sample to enter the calibration window — pressured-tick
    /// samples would contaminate the quantile we want to learn.
    fn observe(&mut self, v: f32, is_healthy_tick: bool) -> SignalObserveOutcome {
        if !v.is_finite() {
            return SignalObserveOutcome::NotFinite;
        }
        let drift_detected = self.drift.observe(v);
        if drift_detected {
            self.window.clear();
            self.drift.reset();
            return SignalObserveOutcome::DriftResetTriggered;
        }
        if !is_healthy_tick {
            return SignalObserveOutcome::SkippedPressuredTick;
        }
        if self.window.is_outlier(v) {
            return SignalObserveOutcome::RejectedOutlier;
        }
        self.window.push(v);
        SignalObserveOutcome::Accepted
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum SignalObserveOutcome {
    Accepted,
    SkippedPressuredTick,
    RejectedOutlier,
    DriftResetTriggered,
    NotFinite,
}

/// Aggregate per-host calibration across both signals (load, PSI).
#[derive(Debug, Clone)]
pub(super) struct GovernorCalibration {
    load: SignalCalibration,
    psi: SignalCalibration,
    /// Monotone counter: total drift resets triggered (both signals).
    drift_reset_count: u64,
    /// Monotone counter: samples dropped by the MAD outlier gate.
    outliers_rejected: u64,
    /// Total observation calls, for telemetry denominators.
    observations_total: u64,
}

impl GovernorCalibration {
    pub(super) fn new(cfg: &GovernorConfig) -> Self {
        Self {
            load: SignalCalibration::new(cfg),
            psi: SignalCalibration::new(cfg),
            drift_reset_count: 0,
            outliers_rejected: 0,
            observations_total: 0,
        }
    }

    /// Observe one snapshot. Returns the effective thresholds the caller
    /// should use for the current tick (None → caller falls back to the
    /// static thresholds from `GovernorConfig`).
    ///
    /// `is_healthy_tick` signals whether the CURRENT tick's reading is
    /// considered healthy by the static classifier — this prevents the
    /// calibration window from learning from pressured samples.
    pub(super) fn observe_and_compute_thresholds(
        &mut self,
        snapshot: &HealthSnapshot,
        is_healthy_tick: bool,
        cfg: &GovernorConfig,
    ) -> Option<EffectiveThresholds> {
        self.observations_total = self.observations_total.saturating_add(1);
        // Observe each signal independently so drift on one doesn't
        // destroy the other's calibration.
        if let Some(v) = snapshot.load_per_core {
            let outcome = self.load.observe(v, is_healthy_tick);
            match outcome {
                SignalObserveOutcome::RejectedOutlier => {
                    self.outliers_rejected = self.outliers_rejected.saturating_add(1);
                }
                SignalObserveOutcome::DriftResetTriggered => {
                    self.drift_reset_count = self.drift_reset_count.saturating_add(1);
                }
                _ => {}
            }
        }
        if let Some(v) = snapshot.psi_cpu_some_avg10 {
            let outcome = self.psi.observe(v, is_healthy_tick);
            match outcome {
                SignalObserveOutcome::RejectedOutlier => {
                    self.outliers_rejected = self.outliers_rejected.saturating_add(1);
                }
                SignalObserveOutcome::DriftResetTriggered => {
                    self.drift_reset_count = self.drift_reset_count.saturating_add(1);
                }
                _ => {}
            }
        }

        // Require BOTH streams to have accumulated K_min healthy samples
        // before we start emitting dynamic thresholds — otherwise we'd
        // mix a dynamic bound on one signal with a static bound on the
        // other, which makes the composite behaviour hard to reason
        // about.
        let load_pressured = self
            .load
            .window
            .quantile(cfg.conformal_alpha_pressured, cfg.conformal_k_min);
        let load_severe = self
            .load
            .window
            .quantile(cfg.conformal_alpha_severe, cfg.conformal_k_min);
        let psi_pressured = self
            .psi
            .window
            .quantile(cfg.conformal_alpha_pressured, cfg.conformal_k_min);
        let psi_severe = self
            .psi
            .window
            .quantile(cfg.conformal_alpha_severe, cfg.conformal_k_min);

        let (lp, ls, pp, ps) = (load_pressured?, load_severe?, psi_pressured?, psi_severe?);

        // Invariant PO-C2-3: severe must strictly exceed pressured so the
        // classifier has a non-empty "only pressured" band. When
        // distribution happens to be so narrow that both quantiles equal,
        // fall back to static — preserves the ordering contract.
        if ls <= lp || ps <= pp {
            return None;
        }

        Some(EffectiveThresholds {
            pressured_load: lp,
            severe_load: ls,
            pressured_psi: pp,
            severe_psi: ps,
        })
    }

    #[cfg(test)]
    pub(super) fn telemetry(&self, cfg: &GovernorConfig) -> CalibrationTelemetry {
        CalibrationTelemetry {
            mode: cfg.calibration_mode,
            load_window_len: self.load.window.len(),
            psi_window_len: self.psi.window.len(),
            conformal_k: cfg.conformal_k,
            conformal_k_min: cfg.conformal_k_min,
            conformal_alpha_pressured: cfg.conformal_alpha_pressured,
            conformal_alpha_severe: cfg.conformal_alpha_severe,
            drift_reset_count: self.drift_reset_count,
            outliers_rejected: self.outliers_rejected,
            observations_total: self.observations_total,
            load_pressured_q: self
                .load
                .window
                .quantile(cfg.conformal_alpha_pressured, cfg.conformal_k_min),
            load_severe_q: self
                .load
                .window
                .quantile(cfg.conformal_alpha_severe, cfg.conformal_k_min),
            psi_pressured_q: self
                .psi
                .window
                .quantile(cfg.conformal_alpha_pressured, cfg.conformal_k_min),
            psi_severe_q: self
                .psi
                .window
                .quantile(cfg.conformal_alpha_severe, cfg.conformal_k_min),
        }
    }
}

/// Effective classifier thresholds for the current tick. In static mode
/// these are just copies of `GovernorConfig`'s static fields; in conformal
/// mode they are the dynamic quantiles from the calibration window.
#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize)]
pub(super) struct EffectiveThresholds {
    pub pressured_load: f32,
    pub severe_load: f32,
    pub pressured_psi: f32,
    pub severe_psi: f32,
}

impl EffectiveThresholds {
    /// Rebuild a config with these thresholds substituted, preserving
    /// every other field (min_capacity_pct, growth_ticks, tick, disabled,
    /// calibration_mode, drift_*). Used to feed `next_capacity` via the
    /// existing `cfg.is_severe/is_pressured` methods unchanged.
    pub(super) fn apply_to(self, cfg: &GovernorConfig) -> GovernorConfig {
        GovernorConfig {
            max_load_per_core: self.pressured_load,
            severe_load_per_core: self.severe_load,
            max_psi_avg10: self.pressured_psi,
            severe_psi_avg10: self.severe_psi,
            ..*cfg
        }
    }
}

/// Calibration telemetry embedded in `GovernorTelemetry.calibration`.
/// Includes enough information for an operator to tell:
///  - whether conformal mode is active
///  - how full the calibration windows are
///  - what quantiles are currently being emitted
///  - whether drift has been detected recently
#[derive(Clone, Debug, serde::Serialize)]
pub struct CalibrationTelemetry {
    pub mode: CalibrationMode,
    pub load_window_len: usize,
    pub psi_window_len: usize,
    pub conformal_k: usize,
    pub conformal_k_min: usize,
    pub conformal_alpha_pressured: f32,
    pub conformal_alpha_severe: f32,
    pub drift_reset_count: u64,
    pub outliers_rejected: u64,
    pub observations_total: u64,
    pub load_pressured_q: Option<f32>,
    pub load_severe_q: Option<f32>,
    pub psi_pressured_q: Option<f32>,
    pub psi_severe_q: Option<f32>,
}
