use serde::{Deserialize, Serialize};

use super::{RefreshLedger, RefreshPhase};

// ─── Evidence-grade derived metrics (ibuuh.24) ─────────────────────────────
//
// `coding_agent_session_search-ibuuh.24` SCOPE bullet 1 calls for "a hard
// evidence ledger for the stale-refresh path so future tuning is grounded
// in measured truth." The raw `RefreshLedger` captures phase counters and
// timings; benchmark agents and operator dashboards still need *derived*
// summaries (throughput, phase-share, hot-phase identification) that are
// stable across runs and trivially comparable. This section adds those
// pure-data summaries so consumers can read one struct instead of
// re-deriving the math at every call site.

/// Per-phase throughput summary derived from a `PhaseRecord`.
///
/// `items_per_second` is the headline tuning metric. `seconds` is
/// captured separately (rather than as a division by zero) so callers
/// can render either form without re-doing the math, and so a phase
/// that processed items but completed in <1ms still surfaces a usable
/// throughput rather than reporting `NaN`. When `duration_ms == 0` the
/// throughput is reported as `None` (you cannot extrapolate from a
/// zero-duration measurement).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RefreshThroughputProfile {
    pub phase: RefreshPhase,
    pub duration_ms: u64,
    pub items_processed: u64,
    /// `items_processed / (duration_ms / 1000)`, rounded to 3 decimal
    /// places via the f64 path. `None` when `duration_ms == 0` or the
    /// phase did not run.
    pub items_per_second: Option<f64>,
}

/// Share of total wall-clock time spent in a single phase.
///
/// `share_pct` sums to ~100.0 across all phases that ran (sub-millisecond
/// rounding can cause ±0.01 drift). The zero-duration case is handled
/// explicitly: phases that contributed 0ms get share_pct=0.0 instead of
/// NaN.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RefreshPhaseShare {
    pub phase: RefreshPhase,
    pub duration_ms: u64,
    /// Percentage of total `RefreshLedger.total_duration_ms` (0.0–100.0).
    pub share_pct: f64,
}

/// Single-shot derived evidence summary suitable for benchmark
/// comparison and operator dashboards. Computed from a `RefreshLedger`
/// in O(phases) time with zero allocations beyond the output structs.
///
/// Comparing two `RefreshLedgerEvidence` values across runs is the
/// intended consumer pattern: regression gates assert that
/// `aggregate_items_per_second` did not drop more than X%, that
/// `dominant_phase` did not migrate, etc.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RefreshLedgerEvidence {
    /// Per-phase throughput. Excludes phases with `items_processed == 0`
    /// to keep the output focused on phases that actually moved data.
    pub throughput: Vec<RefreshThroughputProfile>,
    /// Per-phase wall-clock share. Includes ALL phases that ran (even
    /// zero-item phases like a brief Recovery) so the shares sum
    /// transparently.
    pub phase_share: Vec<RefreshPhaseShare>,
    /// Phase consuming the largest share of wall time, or `None` when
    /// no phases ran. The "where to optimize next" pointer.
    pub dominant_phase: Option<RefreshPhase>,
    /// Total items processed across every phase.
    pub aggregate_items_processed: u64,
    /// Total wall-clock duration in milliseconds (mirrors
    /// `RefreshLedger.total_duration_ms` for ergonomic single-struct
    /// access).
    pub aggregate_duration_ms: u64,
    /// Aggregate items/second across the whole refresh; `None` when
    /// `aggregate_duration_ms == 0`.
    pub aggregate_items_per_second: Option<f64>,
}

impl RefreshLedger {
    /// Compute the derived evidence summary for benchmark comparison and
    /// operator dashboards. See [`RefreshLedgerEvidence`] for shape +
    /// invariants. This is pure (no I/O) and runs in O(phases).
    pub fn evidence_summary(&self) -> RefreshLedgerEvidence {
        let total_ms = self.total_duration_ms;
        let throughput: Vec<RefreshThroughputProfile> = self
            .phases
            .iter()
            .filter(|phase| phase.items_processed > 0)
            .map(|phase| {
                let items_per_second =
                    items_per_second_for(phase.duration_ms, phase.items_processed);
                RefreshThroughputProfile {
                    phase: phase.phase,
                    duration_ms: phase.duration_ms,
                    items_processed: phase.items_processed,
                    items_per_second,
                }
            })
            .collect();
        let phase_share: Vec<RefreshPhaseShare> = self
            .phases
            .iter()
            .map(|phase| RefreshPhaseShare {
                phase: phase.phase,
                duration_ms: phase.duration_ms,
                share_pct: share_pct_for(phase.duration_ms, total_ms),
            })
            .collect();
        let dominant_phase = self
            .phases
            .iter()
            .max_by_key(|phase| phase.duration_ms)
            .filter(|phase| phase.duration_ms > 0)
            .map(|phase| phase.phase);
        let aggregate_items_processed = self.total_items_processed();
        let aggregate_items_per_second = items_per_second_for(total_ms, aggregate_items_processed);
        RefreshLedgerEvidence {
            throughput,
            phase_share,
            dominant_phase,
            aggregate_items_processed,
            aggregate_duration_ms: total_ms,
            aggregate_items_per_second,
        }
    }
}

/// Compute items/second to 3-decimal precision; returns `None` when
/// `duration_ms == 0` (cannot extrapolate from a zero-duration
/// measurement) or `items == 0` (no work to extrapolate).
fn items_per_second_for(duration_ms: u64, items: u64) -> Option<f64> {
    if duration_ms == 0 || items == 0 {
        return None;
    }
    let seconds = duration_ms as f64 / 1000.0;
    if seconds <= 0.0 {
        return None;
    }
    let raw = items as f64 / seconds;
    Some((raw * 1000.0).round() / 1000.0)
}

/// Compute the wall-clock share of one phase relative to the total
/// duration. Returns 0.0 when `total_ms == 0` (avoids NaN; an empty
/// ledger has no phase shares to compute) or when `phase_ms == 0`.
fn share_pct_for(phase_ms: u64, total_ms: u64) -> f64 {
    if total_ms == 0 || phase_ms == 0 {
        return 0.0;
    }
    let raw = (phase_ms as f64 / total_ms as f64) * 100.0;
    (raw * 100.0).round() / 100.0
}

// ─── Cross-run comparison (ibuuh.24) ───────────────────────────────────────
//
// `coding_agent_session_search-ibuuh.24` benchmark/regression slice:
// the evidence summary lets a single run be inspected; cross-run
// comparison is what benchmark CI gates ACTUALLY need ("did this
// build regress vs the baseline?"). Adding a structured one-call
// comparator means CI / dashboards stop hand-rolling delta math —
// every consumer reads the same `RefreshLedgerEvidenceComparison`
// shape and branches on the same regression-class signals.

/// One phase's regression signal between baseline and current.
///
/// `duration_delta_pct` is positive when the phase got SLOWER
/// (current > baseline) — the conventional regression sign that
/// matches operator expectations ("this PR added 12% to publish").
/// `throughput_delta_pct` is positive when the phase got FASTER
/// (current items/sec > baseline items/sec). Both are `None` when
/// the corresponding base measurement is zero/missing — the
/// comparator refuses to invent an extrapolation from no data.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RefreshPhaseDelta {
    pub phase: RefreshPhase,
    pub baseline_duration_ms: u64,
    pub current_duration_ms: u64,
    /// `(current - baseline) / baseline * 100`, rounded to 2 decimals.
    /// Positive ⇒ slower in `current`. `None` when baseline is 0ms
    /// (no rate of change defined) or when the phase didn't run in
    /// either side (cannot compare).
    pub duration_delta_pct: Option<f64>,
    pub baseline_items_processed: u64,
    pub current_items_processed: u64,
    pub baseline_items_per_second: Option<f64>,
    pub current_items_per_second: Option<f64>,
    /// `(current - baseline) / baseline * 100`, rounded to 2 decimals.
    /// Positive ⇒ faster in `current`. `None` when either side has
    /// no items/sec measurement (cannot compute a meaningful delta).
    pub throughput_delta_pct: Option<f64>,
}

/// Cross-run comparison summary suitable for benchmark CI gates and
/// regression dashboards. Computed by
/// [`RefreshLedgerEvidence::compare_to`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RefreshLedgerEvidenceComparison {
    /// Per-phase delta for every phase that ran in EITHER side.
    /// Phases unique to one side surface with a zero on the missing
    /// side — operators can grep for the missing phase and decide.
    pub phase_deltas: Vec<RefreshPhaseDelta>,
    /// Aggregate wall-clock delta. Positive ⇒ slower in `current`.
    pub aggregate_duration_delta_pct: Option<f64>,
    /// Aggregate items/sec delta. Positive ⇒ faster in `current`.
    pub aggregate_throughput_delta_pct: Option<f64>,
    /// `Some((from, to))` when the dominant phase shifted between
    /// baseline and current. A dominant-phase shift is itself a
    /// regression signal — the operator should look at why the
    /// hot phase changed even if absolute totals are similar.
    pub dominant_phase_shift: Option<(RefreshPhase, RefreshPhase)>,
}

impl RefreshLedgerEvidence {
    /// Compare this evidence summary against a `baseline` and return
    /// a structured regression report. Pure (no I/O); runs in
    /// O(phases_baseline + phases_current).
    ///
    /// Direction convention: positive `duration_delta_pct` ⇒ slower
    /// in `self`; positive `throughput_delta_pct` ⇒ faster in `self`.
    /// Picking these signs (not the opposite) makes the JSON read
    /// naturally for benchmark CI ("PR #123 added +12.5% to publish
    /// duration").
    pub fn compare_to(&self, baseline: &Self) -> RefreshLedgerEvidenceComparison {
        // Index baseline + current phase-share entries by phase so
        // zero-item phases still participate in the comparison. The
        // throughput vectors intentionally skip zero-item phases, so
        // using them as the "phase ran" source would hide publish or
        // recovery work that consumed wall-clock time.
        //
        // (RefreshPhase derives Hash but not Ord, so HashMap/HashSet —
        // we re-sort by RefreshPhase::ALL declaration order at the
        // end so the output is deterministic across runs regardless
        // of HashMap iteration order.)
        use std::collections::{HashMap, HashSet};
        let mut baseline_share_by_phase: HashMap<RefreshPhase, &RefreshPhaseShare> = HashMap::new();
        for entry in &baseline.phase_share {
            baseline_share_by_phase.insert(entry.phase, entry);
        }
        let mut current_share_by_phase: HashMap<RefreshPhase, &RefreshPhaseShare> = HashMap::new();
        for entry in &self.phase_share {
            current_share_by_phase.insert(entry.phase, entry);
        }
        let mut baseline_by_phase: HashMap<RefreshPhase, &RefreshThroughputProfile> =
            HashMap::new();
        for entry in &baseline.throughput {
            baseline_by_phase.insert(entry.phase, entry);
        }
        let mut current_by_phase: HashMap<RefreshPhase, &RefreshThroughputProfile> = HashMap::new();
        for entry in &self.throughput {
            current_by_phase.insert(entry.phase, entry);
        }
        // Union the two key sets so a phase unique to one side still
        // surfaces in the comparison. Iterate RefreshPhase::ALL to
        // preserve canonical pipeline order in the output.
        let mut all_phases: HashSet<RefreshPhase> = HashSet::new();
        all_phases.extend(baseline_share_by_phase.keys().copied());
        all_phases.extend(current_share_by_phase.keys().copied());
        all_phases.extend(baseline_by_phase.keys().copied());
        all_phases.extend(current_by_phase.keys().copied());

        let phase_deltas: Vec<RefreshPhaseDelta> = RefreshPhase::ALL
            .iter()
            .copied()
            .filter(|phase| all_phases.contains(phase))
            .map(|phase| {
                let baseline_entry = baseline_by_phase.get(&phase).copied();
                let current_entry = current_by_phase.get(&phase).copied();
                let baseline_duration_ms = baseline_share_by_phase
                    .get(&phase)
                    .map(|e| e.duration_ms)
                    .or_else(|| baseline_entry.map(|e| e.duration_ms))
                    .unwrap_or(0);
                let current_duration_ms = current_share_by_phase
                    .get(&phase)
                    .map(|e| e.duration_ms)
                    .or_else(|| current_entry.map(|e| e.duration_ms))
                    .unwrap_or(0);
                let baseline_items_processed =
                    baseline_entry.map(|e| e.items_processed).unwrap_or(0);
                let current_items_processed = current_entry.map(|e| e.items_processed).unwrap_or(0);
                let baseline_items_per_second = baseline_entry.and_then(|e| e.items_per_second);
                let current_items_per_second = current_entry.and_then(|e| e.items_per_second);

                RefreshPhaseDelta {
                    phase,
                    baseline_duration_ms,
                    current_duration_ms,
                    duration_delta_pct: pct_delta(
                        baseline_duration_ms as f64,
                        current_duration_ms as f64,
                    ),
                    baseline_items_processed,
                    current_items_processed,
                    baseline_items_per_second,
                    current_items_per_second,
                    throughput_delta_pct: match (
                        baseline_items_per_second,
                        current_items_per_second,
                    ) {
                        (Some(b), Some(c)) => pct_delta(b, c),
                        _ => None,
                    },
                }
            })
            .collect();

        let aggregate_duration_delta_pct = pct_delta(
            baseline.aggregate_duration_ms as f64,
            self.aggregate_duration_ms as f64,
        );
        let aggregate_throughput_delta_pct = match (
            baseline.aggregate_items_per_second,
            self.aggregate_items_per_second,
        ) {
            (Some(b), Some(c)) => pct_delta(b, c),
            _ => None,
        };

        let dominant_phase_shift = match (baseline.dominant_phase, self.dominant_phase) {
            (Some(from), Some(to)) if from != to => Some((from, to)),
            _ => None,
        };

        RefreshLedgerEvidenceComparison {
            phase_deltas,
            aggregate_duration_delta_pct,
            aggregate_throughput_delta_pct,
            dominant_phase_shift,
        }
    }
}

/// Compute `(current - baseline) / baseline * 100` rounded to 2
/// decimals, with safe handling of the degenerate cases:
/// - baseline == 0.0 ⇒ `None` (no rate of change defined; an empty
///   baseline means the phase didn't run, so a delta is meaningless)
/// - current == baseline ⇒ `Some(0.0)` (no change is a real signal)
/// - NaN/Infinity ⇒ `None` (defensive — should never happen given
///   inputs are non-negative finite f64s, but pin the contract)
fn pct_delta(baseline: f64, current: f64) -> Option<f64> {
    if !baseline.is_finite() || !current.is_finite() {
        return None;
    }
    if baseline == 0.0 {
        return None;
    }
    let raw = ((current - baseline) / baseline) * 100.0;
    if !raw.is_finite() {
        return None;
    }
    Some((raw * 100.0).round() / 100.0)
}

/// CI-bench-gate threshold configuration. Project-specific values
/// let bench harnesses tune their tolerance: a noisy benchmark
/// runner picks looser thresholds than a deterministic CI worker.
///
/// `coding_agent_session_search-ibuuh.24`: complementary surface to
/// `emit_tracing_summary` (operator-visibility soft signal) — the
/// hard-gate consumer uses `regression_verdict` to decide whether
/// to exit non-zero in CI.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RegressionVerdictThresholds {
    /// Aggregate duration delta percent at which the verdict
    /// becomes `Warning`. Inclusive (`>=` triggers).
    /// Reasonable default: `+15.0`.
    pub warning_duration_pct: f64,
    /// Aggregate duration delta percent at which the verdict
    /// becomes `Failure`. Inclusive. MUST be `>= warning_duration_pct`
    /// or the constructor returns Err.
    /// Reasonable default: `+30.0`.
    pub failure_duration_pct: f64,
}

impl RegressionVerdictThresholds {
    /// Default threshold pair calibrated for typical bench-CI
    /// workloads on cass: 15% warning, 30% failure.
    pub fn defaults() -> Self {
        Self {
            warning_duration_pct: 15.0,
            failure_duration_pct: 30.0,
        }
    }

    /// Custom threshold pair. Returns `Err(&'static str)` when the
    /// configuration is internally inconsistent (warning >= failure
    /// would never raise a warning before the failure trips).
    pub fn try_new(
        warning_duration_pct: f64,
        failure_duration_pct: f64,
    ) -> Result<Self, &'static str> {
        if !warning_duration_pct.is_finite() || !failure_duration_pct.is_finite() {
            return Err("regression thresholds must be finite f64s");
        }
        if warning_duration_pct < 0.0 || failure_duration_pct < 0.0 {
            return Err("regression thresholds must be non-negative percentages");
        }
        if warning_duration_pct >= failure_duration_pct {
            return Err(
                "warning_duration_pct must be strictly less than failure_duration_pct, \
                 otherwise the warning level is unreachable",
            );
        }
        Ok(Self {
            warning_duration_pct,
            failure_duration_pct,
        })
    }

    fn is_valid(&self) -> bool {
        self.warning_duration_pct.is_finite()
            && self.failure_duration_pct.is_finite()
            && self.warning_duration_pct >= 0.0
            && self.failure_duration_pct >= 0.0
            && self.warning_duration_pct < self.failure_duration_pct
    }
}

impl<'de> Deserialize<'de> for RegressionVerdictThresholds {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawThresholds {
            warning_duration_pct: f64,
            failure_duration_pct: f64,
        }

        let raw = RawThresholds::deserialize(deserializer)?;
        Self::try_new(raw.warning_duration_pct, raw.failure_duration_pct)
            .map_err(serde::de::Error::custom)
    }
}

/// Hard-gate verdict for CI bench runners. `Failure` is the only
/// signal that should cause a non-zero exit; `Warning` is for
/// PR-comment / dashboard surfaces; `Clean` is the steady-state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "verdict")]
pub enum RegressionVerdict {
    /// Either no comparison data was available (e.g. baseline
    /// missing) or the duration delta is below the warning
    /// threshold. CI MUST treat this as pass.
    Clean,
    /// Warning band reached: duration delta `>= warning_duration_pct`
    /// but `< failure_duration_pct`. CI should surface this in PR
    /// comments / dashboards but NOT fail the build.
    Warning {
        duration_delta_pct: f64,
        threshold_pct: f64,
    },
    /// Failure band reached: duration delta `>= failure_duration_pct`.
    /// CI MUST exit non-zero on this verdict.
    Failure {
        duration_delta_pct: f64,
        threshold_pct: f64,
    },
}

impl RegressionVerdict {
    /// Convenience: is this a CI-fail verdict? Lets bench-CI
    /// harnesses write `if verdict.should_fail_build() { exit(1); }`
    /// without matching every variant.
    pub fn should_fail_build(&self) -> bool {
        matches!(self, Self::Failure { .. })
    }
}

impl RefreshLedgerEvidenceComparison {
    /// Compute the CI hard-gate verdict for this comparison against
    /// caller-supplied thresholds. Pure function; no I/O. Use
    /// `emit_tracing_summary` for operator-visibility soft signaling
    /// instead of CI gating.
    ///
    /// `coding_agent_session_search-ibuuh.24`: this is the
    /// bench-CI consumer of `compare_to`. A regression test asserts
    /// the verdict tiering matches the threshold contract exactly,
    /// so a project tuning thresholds for its own bench harness
    /// gets predictable behavior at the boundary cases.
    ///
    /// Degenerate cases:
    /// - `aggregate_duration_delta_pct == None` (baseline missing
    ///   or empty) ⇒ `Clean` — no measurement to gate on.
    /// - Negative duration delta (improvement) ⇒ always `Clean`,
    ///   regardless of threshold polarity (an improvement cannot
    ///   trigger a regression failure).
    pub fn regression_verdict(
        &self,
        thresholds: &RegressionVerdictThresholds,
    ) -> RegressionVerdict {
        if !thresholds.is_valid() {
            return RegressionVerdict::Clean;
        }
        let Some(duration_pct) = self.aggregate_duration_delta_pct else {
            return RegressionVerdict::Clean;
        };
        // Improvements never trigger regression verdicts. Pin the
        // sign explicitly rather than relying on threshold values
        // staying positive — a future maintainer who passes a
        // negative warning_duration_pct (e.g. to gate on
        // improvements as a positive signal) would otherwise see
        // every steady-state run trip.
        if duration_pct < 0.0 {
            return RegressionVerdict::Clean;
        }
        if duration_pct >= thresholds.failure_duration_pct {
            return RegressionVerdict::Failure {
                duration_delta_pct: duration_pct,
                threshold_pct: thresholds.failure_duration_pct,
            };
        }
        if duration_pct >= thresholds.warning_duration_pct {
            return RegressionVerdict::Warning {
                duration_delta_pct: duration_pct,
                threshold_pct: thresholds.warning_duration_pct,
            };
        }
        RegressionVerdict::Clean
    }
}

impl RefreshLedgerEvidenceComparison {
    /// Emit a single structured tracing event summarizing the
    /// cross-run comparison. Operators see "this rebuild was N%
    /// slower than the previous publish" in default-level logs
    /// without running a benchmark harness.
    ///
    /// `coding_agent_session_search-ibuuh.24`: pure helper that any
    /// caller (the publish path, a `cass status` summary surface,
    /// CI bench gates) can invoke after `compare_to`. Severity is
    /// chosen by the regression magnitude:
    ///
    /// - `aggregate_duration_delta_pct >= +25.0` ⇒ `warn`
    ///   (significant slowdown — surface in default logs so the
    ///   operator sees it without dredging)
    /// - `aggregate_duration_delta_pct <= -10.0` ⇒ `info`
    ///   (notable improvement — worth surfacing as a positive
    ///   signal)
    /// - otherwise ⇒ `debug` (steady state — high-volume noise on
    ///   every publish; only visible at debug level)
    ///
    /// The thresholds (+25% slowdown / -10% improvement) are the
    /// "operator should look" signal levels, NOT a hard regression
    /// gate. CI hard gates compare against benchmark baselines with
    /// project-specific thresholds; this helper is for ambient
    /// operator visibility.
    ///
    /// `dominant_phase_shift` is reported on every emission
    /// regardless of severity tier — a hot-phase change is itself
    /// a regression signal worth surfacing even when the absolute
    /// totals look similar.
    pub fn emit_tracing_summary(&self) {
        let dominant_shift_str = self
            .dominant_phase_shift
            .map(|(from, to)| format!("{}->{}", from.as_str(), to.as_str()))
            .unwrap_or_else(|| "none".to_string());
        let aggregate_duration_str = self
            .aggregate_duration_delta_pct
            .map(|pct| format!("{pct:+.2}%"))
            .unwrap_or_else(|| "n/a".to_string());
        let aggregate_throughput_str = self
            .aggregate_throughput_delta_pct
            .map(|pct| format!("{pct:+.2}%"))
            .unwrap_or_else(|| "n/a".to_string());

        // Severity tier from the duration delta. Throughput delta
        // alone doesn't drive severity because items_per_second
        // is None on zero-item phases; duration is the always-
        // present signal.
        const SLOWDOWN_WARN_THRESHOLD_PCT: f64 = 25.0;
        const IMPROVEMENT_INFO_THRESHOLD_PCT: f64 = -10.0;
        let duration_pct = self.aggregate_duration_delta_pct.unwrap_or(0.0);
        let phase_count = self.phase_deltas.len();

        // [coding_agent_session_search-urscl] Pre-fix this branch
        // repeated the same 6-field tracing payload across three
        // tracing::{warn,info,debug}! call sites. A field added in
        // one branch but forgotten in another would silently ship.
        // The local `emit_tier!` macro inlines the shared payload at
        // each call site (no runtime cost — same code generation as
        // before), so adding a field once propagates to all three
        // tiers and the per-tier difference is reduced to (macro
        // ident, message literal). Tests continue to observe the
        // per-tier level + message exactly as before.
        let aggregate_throughput_pct = self.aggregate_throughput_delta_pct.unwrap_or(0.0);
        macro_rules! emit_tier {
            ($macro:ident, $msg:literal) => {
                tracing::$macro!(
                    target: "cass::indexer::lexical_refresh",
                    aggregate_duration_delta_pct = duration_pct,
                    aggregate_throughput_delta_pct = aggregate_throughput_pct,
                    aggregate_duration = %aggregate_duration_str,
                    aggregate_throughput = %aggregate_throughput_str,
                    dominant_phase_shift = %dominant_shift_str,
                    phase_count,
                    $msg
                )
            };
        }
        if duration_pct >= SLOWDOWN_WARN_THRESHOLD_PCT {
            emit_tier!(
                warn,
                "lexical refresh evidence: significant slowdown vs previous publish"
            );
        } else if duration_pct <= IMPROVEMENT_INFO_THRESHOLD_PCT {
            emit_tier!(
                info,
                "lexical refresh evidence: notable improvement vs previous publish"
            );
        } else {
            emit_tier!(debug, "lexical refresh evidence: cross-run comparison");
        }
    }
}
