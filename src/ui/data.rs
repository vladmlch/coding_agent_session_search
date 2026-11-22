use crate::model::types::MessageRole;
use crate::ui::components::theme::ThemePalette;

pub use crate::conversation_view::{
    CONVERSATION_CACHE, CacheStats, ConversationCache, ConversationView, load_conversation,
    load_conversation_arc, load_conversation_for_hit, load_conversation_for_source,
    log_conversation_cache_stats,
};
pub(crate) use crate::conversation_view::{
    canonical_message_number, conversation_view_matches_hit, load_conversation_by_id_uncached,
    normalize_ui_hit_source_id, search_hit_has_identity_hint,
};

// -------------------------------------------------------------------------
// Input Mode
// -------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InputMode {
    Query,
    Agent,
    Workspace,
    CreatedFrom,
    CreatedTo,
    PaneFilter,
    /// Inline find within the detail pane (local, non-indexed)
    DetailFind,
}

pub(crate) fn search_hit_has_secondary_identity_hint(hit: &SearchHit) -> bool {
    let snippet = hit.snippet.trim();
    let snippet_prefix = snippet.strip_suffix("...").unwrap_or(snippet).trim();
    let title = hit.title.trim();
    hit.line_number.is_some_and(|line| line > 0)
        || hit.created_at.is_some()
        || !hit.content.is_empty()
        || !snippet_prefix.is_empty()
        || !title.is_empty()
}

pub fn role_style(role: &MessageRole, palette: ThemePalette) -> ftui::Style {
    match role {
        MessageRole::User => ftui::Style::new().fg(palette.user),
        MessageRole::Agent => ftui::Style::new().fg(palette.agent),
        MessageRole::Tool => ftui::Style::new().fg(palette.tool),
        MessageRole::System => ftui::Style::new().fg(palette.system),
        MessageRole::Other(_) => ftui::Style::new().fg(palette.hint),
    }
}

// -------------------------------------------------------------------------
// Shared TUI types (moved from tui.rs to remove ratatui dependency)
// -------------------------------------------------------------------------

/// How search results are ranked and ordered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RankingMode {
    RecentHeavy,
    Balanced,
    RelevanceHeavy,
    MatchQualityHeavy,
    DateNewest,
    DateOldest,
}

/// Format a timestamp as a short human-readable date for filter chips.
/// Shows "Nov 25" for same year, "Nov 25, 2023" for other years.
pub fn format_time_short(ms: i64) -> String {
    use chrono::{DateTime, Datelike, Utc};
    let now = Utc::now();
    DateTime::<Utc>::from_timestamp_millis(ms)
        .map(|dt| {
            if dt.year() == now.year() {
                dt.format("%b %d").to_string()
            } else {
                dt.format("%b %d, %Y").to_string()
            }
        })
        .unwrap_or_else(|| "?".to_string())
}

// =========================================================================
// Explainability Cockpit — Information Architecture (1mfw3.3.1)
// =========================================================================
//
// The cockpit is an inspector-mode overlay that surfaces causal explanations
// for adaptive runtime decisions: diff strategy, resize coalescing, frame
// budget/degradation, and a correlating timeline of decision events.
//
// Panel taxonomy:
//   1. DiffStrategy   — Why the last frame used full vs partial redraw.
//   2. ResizeRegime   — BOCPD regime classification and coalescer decisions.
//   3. BudgetHealth   — Frame budget vs actual, degradation level, PID state.
//   4. Timeline       — Chronological feed of major decision events.
//
// Each panel has a data contract struct defining required fields, source of
// truth, and empty/error-state policies.

/// Panel taxonomy for the explainability cockpit.
///
/// Each variant represents one cockpit surface. Panels are rendered as
/// stacked sections inside the inspector overlay when the cockpit mode is
/// active. The inspector can be in either classic (Timing/Layout/HitRegions)
/// or cockpit mode, toggled independently.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CockpitPanel {
    #[default]
    /// Frame diff strategy decisions: full vs partial redraw, dirty-row counts.
    DiffStrategy,
    /// Resize coalescer regime: Steady vs Burst, BOCPD probability, recent history.
    ResizeRegime,
    /// Frame budget health: target vs actual, degradation level, PID controller state.
    BudgetHealth,
    /// Chronological timeline of major decision events across all subsystems.
    Timeline,
}

impl CockpitPanel {
    pub fn label(self) -> &'static str {
        match self {
            Self::DiffStrategy => "Diff",
            Self::ResizeRegime => "Resize",
            Self::BudgetHealth => "Budget",
            Self::Timeline => "Timeline",
        }
    }

    pub fn next(self) -> Self {
        match self {
            Self::DiffStrategy => Self::ResizeRegime,
            Self::ResizeRegime => Self::BudgetHealth,
            Self::BudgetHealth => Self::Timeline,
            Self::Timeline => Self::DiffStrategy,
        }
    }

    pub fn prev(self) -> Self {
        match self {
            Self::DiffStrategy => Self::Timeline,
            Self::ResizeRegime => Self::DiffStrategy,
            Self::BudgetHealth => Self::ResizeRegime,
            Self::Timeline => Self::BudgetHealth,
        }
    }

    /// All panels in display order.
    pub const ALL: [CockpitPanel; 4] = [
        Self::DiffStrategy,
        Self::ResizeRegime,
        Self::BudgetHealth,
        Self::Timeline,
    ];
}

/// Empty/error-state display policy for cockpit panels.
///
/// When telemetry data is missing (cold start, no resize events, etc.),
/// panels should never crash or show garbled output. Each field specifies
/// the placeholder text to render when the corresponding data is absent.
#[derive(Clone, Debug)]
pub struct CockpitEmptyPolicy {
    /// Placeholder when no evidence is available at all.
    pub no_data: &'static str,
    /// Placeholder when the subsystem hasn't fired yet (e.g., no resize events).
    pub awaiting: &'static str,
    /// Placeholder when the feature is disabled in config.
    pub disabled: &'static str,
}

impl Default for CockpitEmptyPolicy {
    fn default() -> Self {
        Self {
            no_data: "\u{2014}", // em dash
            awaiting: "awaiting first event\u{2026}",
            disabled: "(disabled)",
        }
    }
}

/// Data contract for the Diff Strategy cockpit panel.
///
/// Source of truth: `ftui::runtime::evidence_telemetry::diff_snapshot()`
///
/// Answers: "Why did the last frame use full vs partial redraw?"
#[derive(Clone, Debug, Default)]
pub struct DiffStrategyContract {
    /// Whether the last frame was a full redraw.
    pub last_was_full_redraw: bool,
    /// Number of dirty rows detected in the last partial redraw.
    pub dirty_row_count: u32,
    /// Total row count for the frame (dirty_row_count / total = dirty ratio).
    pub total_row_count: u32,
    /// Reason for the diff decision (human-readable).
    pub reason: &'static str,
    /// Number of consecutive full redraws.
    pub consecutive_full_redraws: u32,
    /// Cumulative full-redraw ratio (full / total frames observed).
    pub full_redraw_ratio: f64,
}

impl DiffStrategyContract {
    /// Dirty row ratio (0.0..1.0). Returns 0.0 if total_row_count is zero.
    pub fn dirty_ratio(&self) -> f64 {
        if self.total_row_count == 0 {
            0.0
        } else {
            self.dirty_row_count as f64 / self.total_row_count as f64
        }
    }

    /// Whether meaningful data has been captured.
    pub fn has_data(&self) -> bool {
        self.total_row_count > 0
    }
}

/// Data contract for the Resize Regime cockpit panel.
///
/// Source of truth: `ftui::runtime::evidence_telemetry::resize_snapshot()`
/// and `ResizeEvidenceSummary::recent_resizes` ring buffer.
///
/// Answers: "What resize regime are we in and why?"
#[derive(Clone, Debug)]
pub struct ResizeRegimeContract {
    /// Current regime label ("Steady", "Burst", or em-dash).
    pub regime: &'static str,
    /// Current terminal size (cols, rows).
    pub terminal_size: Option<(u16, u16)>,
    /// BOCPD burst probability (0.0..1.0), None if BOCPD disabled.
    pub bocpd_p_burst: Option<f64>,
    /// BOCPD recommended coalescer delay (ms), None if not applicable.
    pub bocpd_delay_ms: Option<u32>,
    /// Number of resize events in history buffer.
    pub history_len: usize,
    /// Most recent resize action ("apply", "defer", "coalesce").
    pub last_action: &'static str,
    /// Inter-arrival time of the most recent resize event (ms).
    pub last_dt_ms: f64,
    /// Events per second at the last decision.
    pub last_event_rate: f64,
}

impl Default for ResizeRegimeContract {
    fn default() -> Self {
        Self {
            regime: "\u{2014}",
            terminal_size: None,
            bocpd_p_burst: None,
            bocpd_delay_ms: None,
            history_len: 0,
            last_action: "\u{2014}",
            last_dt_ms: 0.0,
            last_event_rate: 0.0,
        }
    }
}

impl ResizeRegimeContract {
    /// Whether meaningful data has been captured.
    pub fn has_data(&self) -> bool {
        self.regime != "\u{2014}"
    }
}

/// Data contract for the Budget Health cockpit panel.
///
/// Source of truth: `ftui::runtime::evidence_telemetry::budget_snapshot()`
/// and `ResizeEvidenceSummary` budget-related fields.
///
/// Answers: "Is the frame budget healthy? What degradation is active?"
#[derive(Clone, Debug)]
pub struct BudgetHealthContract {
    /// Current degradation level label.
    pub degradation: &'static str,
    /// Target frame budget (microseconds).
    pub budget_us: f64,
    /// Actual frame time (microseconds).
    pub frame_time_us: f64,
    /// PID controller output (positive = headroom, negative = over-budget).
    pub pid_output: f64,
    /// Whether the budget controller is still in warmup.
    pub in_warmup: bool,
    /// Total frames observed by the budget controller.
    pub frames_observed: u32,
    /// Budget pressure ratio: frame_time / budget (>1.0 means over-budget).
    pub pressure: f64,
}

impl Default for BudgetHealthContract {
    fn default() -> Self {
        Self {
            degradation: "\u{2014}",
            budget_us: 0.0,
            frame_time_us: 0.0,
            pid_output: 0.0,
            in_warmup: true,
            frames_observed: 0,
            pressure: 0.0,
        }
    }
}

impl BudgetHealthContract {
    /// Whether meaningful data has been captured.
    pub fn has_data(&self) -> bool {
        self.frames_observed > 0
    }

    /// Whether the frame budget is currently exceeded.
    pub fn is_over_budget(&self) -> bool {
        self.pressure > 1.0
    }
}

/// A single event in the cockpit timeline feed.
///
/// Timeline events correlate decision points across subsystems,
/// enabling causal diagnosis ("the resize burst caused degradation
/// to drop to SimpleBorders").
#[derive(Clone, Debug)]
pub struct CockpitTimelineEvent {
    /// Subsystem that generated the event.
    pub source: CockpitPanel,
    /// Human-readable one-line summary of what happened.
    pub summary: String,
    /// Monotonic event index for ordering.
    pub event_idx: u64,
    /// Elapsed time since app start (seconds).
    pub elapsed_secs: f64,
    /// Severity/importance of the event.
    pub severity: TimelineEventSeverity,
}

/// Severity levels for cockpit timeline events.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TimelineEventSeverity {
    /// Routine decision (e.g., normal resize apply).
    #[default]
    Info,
    /// Notable state change (e.g., regime transition Steady -> Burst).
    StateChange,
    /// Degradation or pressure event (e.g., over-budget, degradation level change).
    Warning,
}

/// Data contract for the Timeline cockpit panel.
///
/// Source of truth: aggregated from all other cockpit contracts.
/// Events are pushed by `EvidenceSnapshots::refresh()` when it detects
/// state transitions.
///
/// Answers: "What changed, when, and across which subsystem?"
#[derive(Clone, Debug)]
pub struct TimelineContract {
    /// Ring buffer of recent timeline events (newest last).
    pub events: std::collections::VecDeque<CockpitTimelineEvent>,
    /// Maximum events to retain.
    pub capacity: usize,
}

/// Default timeline capacity.
const TIMELINE_CAPACITY: usize = 64;

impl Default for TimelineContract {
    fn default() -> Self {
        Self::new()
    }
}

impl TimelineContract {
    pub fn new() -> Self {
        Self {
            events: std::collections::VecDeque::with_capacity(TIMELINE_CAPACITY),
            capacity: TIMELINE_CAPACITY,
        }
    }

    /// Push a new event, evicting the oldest if at capacity.
    pub fn push(&mut self, event: CockpitTimelineEvent) {
        if self.events.len() >= self.capacity {
            self.events.pop_front();
        }
        self.events.push_back(event);
    }

    /// Whether any events have been recorded.
    pub fn has_data(&self) -> bool {
        !self.events.is_empty()
    }

    /// Number of events in the buffer.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

/// Cockpit display mode controlling overlay sizing behaviour.
///
/// **Overlay** is a compact bottom-right panel (default).
/// **Expanded** is a taller panel that occupies more vertical space,
/// allowing multi-panel stacking and more timeline events.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CockpitMode {
    /// Compact overlay anchored to bottom-right corner.
    #[default]
    Overlay,
    /// Expanded surface that takes more vertical space for deeper inspection.
    Expanded,
}

impl CockpitMode {
    /// Cycle to the next mode.
    pub fn cycle(self) -> Self {
        match self {
            Self::Overlay => Self::Expanded,
            Self::Expanded => Self::Overlay,
        }
    }

    /// Short label for status display.
    pub fn label(self) -> &'static str {
        match self {
            Self::Overlay => "overlay",
            Self::Expanded => "expanded",
        }
    }
}

/// Aggregated cockpit state holding all panel contracts.
///
/// This struct is the single rendering-ready data source for the
/// cockpit overlay. It is updated each tick by polling evidence
/// telemetry and detecting state transitions for timeline events.
#[derive(Clone, Debug, Default)]
pub struct CockpitState {
    /// Active cockpit panel (for single-panel focus mode).
    pub active_panel: CockpitPanel,
    /// Whether cockpit mode is active (vs classic inspector tabs).
    pub enabled: bool,
    /// Display mode (overlay vs expanded).
    pub mode: CockpitMode,
    /// Diff strategy contract.
    pub diff: DiffStrategyContract,
    /// Resize regime contract.
    pub resize: ResizeRegimeContract,
    /// Budget health contract.
    pub budget: BudgetHealthContract,
    /// Timeline event feed.
    pub timeline: TimelineContract,
    /// Empty-state display policy.
    pub empty_policy: CockpitEmptyPolicy,
}

impl CockpitState {
    pub fn new() -> Self {
        Self {
            timeline: TimelineContract::new(),
            ..Default::default()
        }
    }

    /// Whether any panel has meaningful data to display.
    pub fn has_any_data(&self) -> bool {
        self.diff.has_data()
            || self.resize.has_data()
            || self.budget.has_data()
            || self.timeline.has_data()
    }

    /// Get the empty-state message for a panel.
    pub fn empty_message(&self, panel: CockpitPanel) -> &'static str {
        match panel {
            CockpitPanel::DiffStrategy => {
                if self.diff.has_data() {
                    ""
                } else {
                    self.empty_policy.awaiting
                }
            }
            CockpitPanel::ResizeRegime => {
                if self.resize.has_data() {
                    ""
                } else {
                    self.empty_policy.awaiting
                }
            }
            CockpitPanel::BudgetHealth => {
                if self.budget.has_data() {
                    ""
                } else {
                    self.empty_policy.awaiting
                }
            }
            CockpitPanel::Timeline => {
                if self.timeline.has_data() {
                    ""
                } else {
                    self.empty_policy.no_data
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // =====================================================================
    // Cockpit IA contract tests (1mfw3.3.1)
    // =====================================================================

    #[test]
    fn cockpit_panel_label_and_navigation() {
        assert_eq!(CockpitPanel::DiffStrategy.label(), "Diff");
        assert_eq!(CockpitPanel::ResizeRegime.label(), "Resize");
        assert_eq!(CockpitPanel::BudgetHealth.label(), "Budget");
        assert_eq!(CockpitPanel::Timeline.label(), "Timeline");

        // Full forward cycle
        let mut p = CockpitPanel::DiffStrategy;
        p = p.next();
        assert_eq!(p, CockpitPanel::ResizeRegime);
        p = p.next();
        assert_eq!(p, CockpitPanel::BudgetHealth);
        p = p.next();
        assert_eq!(p, CockpitPanel::Timeline);
        p = p.next();
        assert_eq!(p, CockpitPanel::DiffStrategy);

        // Full backward cycle
        p = CockpitPanel::DiffStrategy;
        p = p.prev();
        assert_eq!(p, CockpitPanel::Timeline);
        p = p.prev();
        assert_eq!(p, CockpitPanel::BudgetHealth);
        p = p.prev();
        assert_eq!(p, CockpitPanel::ResizeRegime);
        p = p.prev();
        assert_eq!(p, CockpitPanel::DiffStrategy);
    }

    #[test]
    fn cockpit_panel_all_constant() {
        assert_eq!(CockpitPanel::ALL.len(), 4);
        assert_eq!(CockpitPanel::ALL[0], CockpitPanel::DiffStrategy);
        assert_eq!(CockpitPanel::ALL[3], CockpitPanel::Timeline);
    }

    #[test]
    fn diff_strategy_contract_defaults_no_data() {
        let diff = DiffStrategyContract::default();
        assert!(!diff.has_data());
        assert_eq!(diff.dirty_ratio(), 0.0);
        assert!(!diff.last_was_full_redraw);
    }

    #[test]
    fn diff_strategy_contract_dirty_ratio() {
        let diff = DiffStrategyContract {
            dirty_row_count: 10,
            total_row_count: 40,
            ..Default::default()
        };
        assert!(diff.has_data());
        assert!((diff.dirty_ratio() - 0.25).abs() < f64::EPSILON);
    }

    #[test]
    fn resize_regime_contract_defaults_no_data() {
        let resize = ResizeRegimeContract::default();
        assert!(!resize.has_data());
        assert_eq!(resize.regime, "\u{2014}");
    }

    #[test]
    fn resize_regime_contract_with_data() {
        let resize = ResizeRegimeContract {
            regime: "Burst",
            terminal_size: Some((120, 40)),
            bocpd_p_burst: Some(0.87),
            history_len: 5,
            last_action: "defer",
            ..Default::default()
        };
        assert!(resize.has_data());
        assert_eq!(resize.terminal_size, Some((120, 40)));
    }

    #[test]
    fn budget_health_contract_defaults_no_data() {
        let budget = BudgetHealthContract::default();
        assert!(!budget.has_data());
        assert!(!budget.is_over_budget());
    }

    #[test]
    fn budget_health_contract_over_budget() {
        let budget = BudgetHealthContract {
            budget_us: 16_666.0,
            frame_time_us: 25_000.0,
            pressure: 1.5,
            frames_observed: 100,
            ..Default::default()
        };
        assert!(budget.has_data());
        assert!(budget.is_over_budget());
    }

    #[test]
    fn timeline_contract_push_and_eviction() {
        let mut timeline = TimelineContract {
            events: std::collections::VecDeque::new(),
            capacity: 3,
        };
        assert!(timeline.is_empty());
        assert!(!timeline.has_data());

        for i in 0..5 {
            timeline.push(CockpitTimelineEvent {
                source: CockpitPanel::BudgetHealth,
                summary: format!("event {i}"),
                event_idx: i,
                elapsed_secs: i as f64,
                severity: TimelineEventSeverity::Info,
            });
        }

        assert_eq!(timeline.len(), 3);
        assert!(timeline.has_data());
        // Oldest events should be evicted
        assert_eq!(timeline.events[0].event_idx, 2);
        assert_eq!(timeline.events[2].event_idx, 4);
    }

    #[test]
    fn cockpit_state_empty_messages() {
        let state = CockpitState::new();
        assert!(!state.has_any_data());

        // All panels should return awaiting/no_data messages
        assert!(!state.empty_message(CockpitPanel::DiffStrategy).is_empty());
        assert!(!state.empty_message(CockpitPanel::ResizeRegime).is_empty());
        assert!(!state.empty_message(CockpitPanel::BudgetHealth).is_empty());
        assert!(!state.empty_message(CockpitPanel::Timeline).is_empty());
    }

    #[test]
    fn cockpit_state_partial_data() {
        let mut state = CockpitState::new();
        state.resize = ResizeRegimeContract {
            regime: "Steady",
            ..Default::default()
        };
        assert!(state.has_any_data());
        // Resize has data, so empty_message returns ""
        assert_eq!(state.empty_message(CockpitPanel::ResizeRegime), "");
        // Others still show placeholder
        assert!(!state.empty_message(CockpitPanel::DiffStrategy).is_empty());
    }

    #[test]
    fn timeline_event_severity_default_is_info() {
        assert_eq!(
            TimelineEventSeverity::default(),
            TimelineEventSeverity::Info
        );
    }

    #[test]
    fn cockpit_empty_policy_defaults() {
        let policy = CockpitEmptyPolicy::default();
        assert_eq!(policy.no_data, "\u{2014}");
        assert!(policy.awaiting.contains("awaiting"));
        assert!(policy.disabled.contains("disabled"));
    }

    // -- CockpitMode tests (1mfw3.3.3) ------------------------------------

    #[test]
    fn cockpit_mode_default_is_overlay() {
        assert_eq!(CockpitMode::default(), CockpitMode::Overlay);
    }

    #[test]
    fn cockpit_mode_cycle() {
        assert_eq!(CockpitMode::Overlay.cycle(), CockpitMode::Expanded);
        assert_eq!(CockpitMode::Expanded.cycle(), CockpitMode::Overlay);
    }

    #[test]
    fn cockpit_mode_labels() {
        assert_eq!(CockpitMode::Overlay.label(), "overlay");
        assert_eq!(CockpitMode::Expanded.label(), "expanded");
    }

    #[test]
    fn cockpit_state_includes_mode() {
        let state = CockpitState::new();
        assert_eq!(state.mode, CockpitMode::Overlay);
        assert!(!state.enabled);
    }
}
