use crate::indexer::responsiveness;
use crate::search::policy::SemanticPolicy;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticBackfillSchedulerState {
    Running,
    Paused,
    Disabled,
}

impl SemanticBackfillSchedulerState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Disabled => "disabled",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticBackfillSchedulerReason {
    IdleBudgetAvailable,
    OperatorDisabled,
    PolicyDisabled,
    ForegroundPressure,
    LexicalRepairActive,
    CapacityBelowFloor,
    ThreadBudgetZero,
    BatchBudgetZero,
    /// The console user is active and the operator asked scheduled work to
    /// wait for `CASS_RESPONSIVENESS_MIN_USER_IDLE_SECS` of idle time.
    UserActive,
}

impl SemanticBackfillSchedulerReason {
    pub fn next_step(self) -> &'static str {
        match self {
            Self::IdleBudgetAvailable => "background semantic backfill is within idle budgets",
            Self::OperatorDisabled => {
                "background semantic backfill is disabled by CASS_SEMANTIC_BACKFILL_DISABLE"
            }
            Self::PolicyDisabled => "semantic policy disables background semantic backfill",
            Self::ForegroundPressure => {
                "foreground pressure is present; retry after the idle delay"
            }
            Self::LexicalRepairActive => "lexical repair is active; semantic backfill is yielding",
            Self::CapacityBelowFloor => {
                "machine responsiveness capacity is below the semantic backfill floor"
            }
            Self::ThreadBudgetZero => "semantic backfill thread budget is zero",
            Self::BatchBudgetZero => "semantic backfill batch budget is zero",
            Self::UserActive => {
                "console user is active (CASS_RESPONSIVENESS_MIN_USER_IDLE_SECS); retry after the idle delay"
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SemanticBackfillSchedulerSignals {
    pub foreground_pressure: bool,
    pub lexical_repair_active: bool,
    pub force: bool,
    pub operator_disabled: bool,
    /// Console user is active and `CASS_RESPONSIVENESS_MIN_USER_IDLE_SECS`
    /// asks scheduled work to wait. Always `false` when the gate is disabled
    /// or the platform cannot report idle time (fail open).
    pub user_active: bool,
}

impl SemanticBackfillSchedulerSignals {
    pub fn from_env() -> Self {
        Self {
            foreground_pressure: env_truthy("CASS_SEMANTIC_BACKFILL_FOREGROUND_ACTIVE"),
            lexical_repair_active: env_truthy("CASS_SEMANTIC_BACKFILL_LEXICAL_REPAIR_ACTIVE"),
            force: env_truthy("CASS_SEMANTIC_BACKFILL_FORCE"),
            operator_disabled: env_truthy("CASS_SEMANTIC_BACKFILL_DISABLE"),
            user_active: !responsiveness::user_idle_gate().satisfied,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SemanticBackfillSchedulerDecision {
    pub state: SemanticBackfillSchedulerState,
    pub reason: SemanticBackfillSchedulerReason,
    pub requested_batch_conversations: usize,
    pub scheduled_batch_conversations: usize,
    pub current_capacity_pct: u32,
    pub min_capacity_pct: u32,
    pub max_backfill_threads: usize,
    pub idle_delay_seconds: u64,
    pub chunk_timeout_seconds: u64,
    pub foreground_pressure: bool,
    pub lexical_repair_active: bool,
    pub forced: bool,
    pub next_eligible_after_ms: u64,
}

impl SemanticBackfillSchedulerDecision {
    pub fn should_run(&self) -> bool {
        matches!(self.state, SemanticBackfillSchedulerState::Running)
    }
}

pub(in crate::indexer::semantic::engine) fn env_truthy(key: &str) -> bool {
    dotenvy::var(key)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn env_backfill_min_capacity_pct() -> u32 {
    dotenvy::var("CASS_SEMANTIC_BACKFILL_MIN_CAPACITY_PCT")
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
        .map(|value| value.clamp(1, 100))
        .unwrap_or(75)
}

pub fn semantic_backfill_scheduler_decision(
    policy: &SemanticPolicy,
    requested_batch_conversations: usize,
    signals: &SemanticBackfillSchedulerSignals,
) -> SemanticBackfillSchedulerDecision {
    semantic_backfill_scheduler_decision_for_capacity(
        policy,
        requested_batch_conversations,
        signals,
        responsiveness::current_capacity_pct(),
    )
}

pub fn semantic_backfill_scheduler_decision_for_capacity(
    policy: &SemanticPolicy,
    requested_batch_conversations: usize,
    signals: &SemanticBackfillSchedulerSignals,
    current_capacity_pct: u32,
) -> SemanticBackfillSchedulerDecision {
    let min_capacity_pct = env_backfill_min_capacity_pct();
    let paused_delay_ms = policy.idle_delay_seconds.saturating_mul(1000);
    let mut decision = SemanticBackfillSchedulerDecision {
        state: SemanticBackfillSchedulerState::Running,
        reason: SemanticBackfillSchedulerReason::IdleBudgetAvailable,
        requested_batch_conversations,
        scheduled_batch_conversations: requested_batch_conversations,
        current_capacity_pct: current_capacity_pct.clamp(0, 100),
        min_capacity_pct,
        max_backfill_threads: policy.max_backfill_threads,
        idle_delay_seconds: policy.idle_delay_seconds,
        chunk_timeout_seconds: policy.chunk_timeout_seconds,
        foreground_pressure: signals.foreground_pressure,
        lexical_repair_active: signals.lexical_repair_active,
        forced: signals.force,
        next_eligible_after_ms: 0,
    };

    if requested_batch_conversations == 0 {
        return stopped_scheduler_decision(
            decision,
            SemanticBackfillSchedulerState::Disabled,
            SemanticBackfillSchedulerReason::BatchBudgetZero,
            paused_delay_ms,
        );
    }
    if policy.max_backfill_threads == 0 && !signals.force {
        return stopped_scheduler_decision(
            decision,
            SemanticBackfillSchedulerState::Disabled,
            SemanticBackfillSchedulerReason::ThreadBudgetZero,
            paused_delay_ms,
        );
    }
    if signals.operator_disabled && !signals.force {
        return stopped_scheduler_decision(
            decision,
            SemanticBackfillSchedulerState::Disabled,
            SemanticBackfillSchedulerReason::OperatorDisabled,
            paused_delay_ms,
        );
    }
    if !policy.mode.should_build_semantic() && !signals.force {
        return stopped_scheduler_decision(
            decision,
            SemanticBackfillSchedulerState::Disabled,
            SemanticBackfillSchedulerReason::PolicyDisabled,
            paused_delay_ms,
        );
    }
    if signals.lexical_repair_active && !signals.force {
        return stopped_scheduler_decision(
            decision,
            SemanticBackfillSchedulerState::Paused,
            SemanticBackfillSchedulerReason::LexicalRepairActive,
            paused_delay_ms,
        );
    }
    if signals.foreground_pressure && !signals.force {
        return stopped_scheduler_decision(
            decision,
            SemanticBackfillSchedulerState::Paused,
            SemanticBackfillSchedulerReason::ForegroundPressure,
            paused_delay_ms,
        );
    }
    if current_capacity_pct < min_capacity_pct && !signals.force {
        return stopped_scheduler_decision(
            decision,
            SemanticBackfillSchedulerState::Paused,
            SemanticBackfillSchedulerReason::CapacityBelowFloor,
            paused_delay_ms,
        );
    }
    if signals.user_active && !signals.force {
        return stopped_scheduler_decision(
            decision,
            SemanticBackfillSchedulerState::Paused,
            SemanticBackfillSchedulerReason::UserActive,
            paused_delay_ms,
        );
    }

    let capacity = current_capacity_pct.clamp(1, 100) as usize;
    let scaled = requested_batch_conversations.saturating_mul(capacity) / 100;
    decision.scheduled_batch_conversations = scaled.max(1).min(requested_batch_conversations);
    decision
}

fn stopped_scheduler_decision(
    mut decision: SemanticBackfillSchedulerDecision,
    state: SemanticBackfillSchedulerState,
    reason: SemanticBackfillSchedulerReason,
    next_eligible_after_ms: u64,
) -> SemanticBackfillSchedulerDecision {
    decision.state = state;
    decision.reason = reason;
    decision.scheduled_batch_conversations = 0;
    decision.next_eligible_after_ms = next_eligible_after_ms;
    decision
}
