use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::indexer) enum LexicalRebuildResponsivenessState {
    Startup,
    Steady,
    PressureLimited,
    PinnedSteady,
    PinnedConservative,
}

impl LexicalRebuildResponsivenessState {
    pub(in crate::indexer) fn as_str(self) -> &'static str {
        match self {
            Self::Startup => "startup",
            Self::Steady => "steady",
            Self::PressureLimited => "pressure_limited",
            Self::PinnedSteady => "pinned_steady",
            Self::PinnedConservative => "pinned_conservative",
        }
    }
}

#[derive(Debug)]
pub(in crate::indexer) struct LexicalRebuildBudgetTransition {
    pub(in crate::indexer) old_budget: LexicalRebuildPipelineBudgetSnapshot,
    pub(in crate::indexer) new_budget: LexicalRebuildPipelineBudgetSnapshot,
    pub(in crate::indexer) mode: &'static str,
    pub(in crate::indexer) reason: String,
}

#[derive(Debug)]
pub(in crate::indexer) struct LexicalRebuildResponsivenessController {
    pub(in crate::indexer) policy: LexicalRebuildResponsivenessPolicy,
    pub(in crate::indexer) startup_budget: LexicalRebuildPipelineBudgetSnapshot,
    pub(in crate::indexer) steady_budget: LexicalRebuildPipelineBudgetSnapshot,
    pub(in crate::indexer) pipeline_channel_size: usize,
    pub(in crate::indexer) loadavg_high_watermark_1m_milli: Option<u32>,
    pub(in crate::indexer) loadavg_low_watermark_1m_milli: Option<u32>,
    pub(in crate::indexer) restore_clear_samples: usize,
    pub(in crate::indexer) restore_hold: Duration,
    pub(in crate::indexer) memory_reserve_bytes: usize,
    pub(in crate::indexer) emergency_memory_reserve_bytes: usize,
    pub(in crate::indexer) state: LexicalRebuildResponsivenessState,
    pub(in crate::indexer) reason: String,
    pub(in crate::indexer) clear_samples: usize,
    pub(in crate::indexer) last_transition_at: Instant,
    /// GH #445: pressure demotions in this run. Each one doubles the restore
    /// hold (capped) so a host with no load telemetry, where saturation alone
    /// can demote, cannot flap between budgets every few seconds.
    pub(in crate::indexer) pressure_demotions: u32,
    pub(in crate::indexer) last_observed_producer_budget_wait_count: usize,
    pub(in crate::indexer) last_observed_producer_handoff_wait_count: usize,
}

impl LexicalRebuildResponsivenessController {
    pub(in crate::indexer) const INFLIGHT_HIGH_WATERMARK_PERCENT: usize = 90;
    /// Upper bound on the restore-hold doubling (2^3 = 8x the base hold).
    pub(in crate::indexer) const MAX_RESTORE_HOLD_DOUBLINGS: u32 = 3;

    pub(in crate::indexer) fn new(
        policy: LexicalRebuildResponsivenessPolicy,
        startup_budget: LexicalRebuildPipelineBudgetSnapshot,
        steady_budget: LexicalRebuildPipelineBudgetSnapshot,
        pipeline_channel_size: usize,
        start_conservative: bool,
        loadavg_high_watermark_1m_milli: Option<u32>,
        loadavg_low_watermark_1m_milli: Option<u32>,
    ) -> Self {
        let (state, reason) = match policy {
            LexicalRebuildResponsivenessPolicy::Steady => (
                LexicalRebuildResponsivenessState::PinnedSteady,
                "pinned_steady_by_controller_mode".to_string(),
            ),
            LexicalRebuildResponsivenessPolicy::Conservative => (
                LexicalRebuildResponsivenessState::PinnedConservative,
                "pinned_conservative_by_controller_mode".to_string(),
            ),
            LexicalRebuildResponsivenessPolicy::Auto if start_conservative => (
                LexicalRebuildResponsivenessState::Startup,
                "startup_safe_budget_before_first_durable_commit".to_string(),
            ),
            LexicalRebuildResponsivenessPolicy::Auto => (
                LexicalRebuildResponsivenessState::Steady,
                "steady_budget_selected_for_resumed_or_pinned_run".to_string(),
            ),
        };

        Self {
            policy,
            startup_budget,
            steady_budget,
            pipeline_channel_size: pipeline_channel_size.max(1),
            loadavg_high_watermark_1m_milli,
            loadavg_low_watermark_1m_milli,
            restore_clear_samples: lexical_rebuild_controller_restore_clear_samples(),
            restore_hold: lexical_rebuild_controller_restore_hold(),
            memory_reserve_bytes: lexical_rebuild_staged_shard_build_memory_reserve_bytes(),
            emergency_memory_reserve_bytes:
                lexical_rebuild_staged_shard_build_emergency_memory_reserve_bytes(),
            state,
            reason,
            clear_samples: 0,
            last_transition_at: Instant::now(),
            pressure_demotions: 0,
            last_observed_producer_budget_wait_count: 0,
            last_observed_producer_handoff_wait_count: 0,
        }
    }

    pub(in crate::indexer) fn current_budget(&self) -> LexicalRebuildPipelineBudgetSnapshot {
        match self.state {
            LexicalRebuildResponsivenessState::Startup
            | LexicalRebuildResponsivenessState::PressureLimited
            | LexicalRebuildResponsivenessState::PinnedConservative => self.startup_budget,
            LexicalRebuildResponsivenessState::Steady
            | LexicalRebuildResponsivenessState::PinnedSteady => self.steady_budget,
        }
    }

    pub(in crate::indexer) fn mode(&self) -> &'static str {
        self.state.as_str()
    }

    pub(in crate::indexer) fn reason(&self) -> &str {
        &self.reason
    }

    pub(in crate::indexer) fn waits_for_first_durable_commit(&self) -> bool {
        self.policy == LexicalRebuildResponsivenessPolicy::Auto
            && self.state == LexicalRebuildResponsivenessState::Startup
            && self.startup_budget != self.steady_budget
    }

    pub(in crate::indexer) fn record_first_durable_commit(
        &mut self,
    ) -> Option<LexicalRebuildBudgetTransition> {
        self.promote_startup_to_steady("first_durable_commit_promoted_steady_budget")
    }

    pub(in crate::indexer) fn promote_startup_to_steady(
        &mut self,
        reason: impl Into<String>,
    ) -> Option<LexicalRebuildBudgetTransition> {
        if self.state != LexicalRebuildResponsivenessState::Startup {
            return None;
        }

        let old_budget = self.current_budget();
        self.state = LexicalRebuildResponsivenessState::Steady;
        self.reason = reason.into();
        self.clear_samples = 0;
        self.last_transition_at = Instant::now();
        let new_budget = self.current_budget();
        if old_budget == new_budget {
            return None;
        }

        Some(LexicalRebuildBudgetTransition {
            old_budget,
            new_budget,
            mode: self.mode(),
            reason: self.reason.clone(),
        })
    }

    pub(in crate::indexer) fn observe_runtime(
        &mut self,
        runtime: &LexicalRebuildPipelineRuntimeSnapshot,
    ) -> Option<LexicalRebuildBudgetTransition> {
        let new_producer_budget_wait =
            self.observe_new_producer_budget_wait(runtime.producer_budget_wait_count);
        let new_producer_handoff_wait =
            self.observe_new_producer_handoff_wait(runtime.producer_handoff_wait_count);
        if self.policy != LexicalRebuildResponsivenessPolicy::Auto {
            return None;
        }
        if self.state == LexicalRebuildResponsivenessState::Startup {
            return self
                .detect_startup_budget_starvation(runtime, new_producer_budget_wait)
                .and_then(|reason| self.promote_startup_to_steady(reason));
        }

        if let Some(reason) = self.detect_pressure(runtime, new_producer_handoff_wait) {
            self.clear_samples = 0;
            self.reason = reason.clone();
            if self.state != LexicalRebuildResponsivenessState::PressureLimited {
                let old_budget = self.current_budget();
                self.state = LexicalRebuildResponsivenessState::PressureLimited;
                self.last_transition_at = Instant::now();
                self.pressure_demotions = self.pressure_demotions.saturating_add(1);
                let new_budget = self.current_budget();
                if old_budget != new_budget {
                    return Some(LexicalRebuildBudgetTransition {
                        old_budget,
                        new_budget,
                        mode: self.mode(),
                        reason,
                    });
                }
            }
            return None;
        }

        if self.state == LexicalRebuildResponsivenessState::PressureLimited {
            let held_for = self.last_transition_at.elapsed();
            let restore_hold = self.restore_hold_after_demotions();
            if held_for < restore_hold {
                self.reason = format!(
                    "holding_conservative_budget_after_pressure_demote_for_{}ms",
                    restore_hold.as_millis()
                );
                self.clear_samples = 0;
                return None;
            }

            // GH #445: restore judges host pressure, not pipeline emptiness.
            // The previous gate demanded a fully drained pipeline (no queued
            // pages, no pending batch, inflight <= 50% of cap) for three
            // consecutive samples — unreachable while a producer exists to
            // keep the pipeline full — so one transient demotion pinned the
            // whole rebuild to the startup budget (66 min in steady mode vs.
            // crawl/wedge under auto on a 2.4M-doc archive).
            if let Some(reason) = self.host_pressure_not_clear_reason(runtime) {
                self.clear_samples = 0;
                self.reason = reason;
            } else {
                self.clear_samples = self.clear_samples.saturating_add(1);
                if self.clear_samples >= self.restore_clear_samples {
                    let old_budget = self.current_budget();
                    self.state = LexicalRebuildResponsivenessState::Steady;
                    self.reason = format!(
                        "restored_steady_budget_after_{}_clear_samples",
                        self.restore_clear_samples
                    );
                    self.clear_samples = 0;
                    self.last_transition_at = Instant::now();
                    let new_budget = self.current_budget();
                    if old_budget != new_budget {
                        return Some(LexicalRebuildBudgetTransition {
                            old_budget,
                            new_budget,
                            mode: self.mode(),
                            reason: self.reason.clone(),
                        });
                    }
                } else {
                    self.reason = format!(
                        "awaiting_clear_pressure_window_{}/{}",
                        self.clear_samples, self.restore_clear_samples
                    );
                }
            }
        } else {
            self.reason = "steady_budget_with_headroom".to_string();
        }

        None
    }

    /// GH #445: the restore hold doubles with every pressure demotion in this
    /// run (capped at 2^[`Self::MAX_RESTORE_HOLD_DOUBLINGS`]). On hosts without
    /// load telemetry a saturated pipeline alone can demote, and it saturates
    /// again as soon as the steady budget returns; the growing hold bounds
    /// that demote/restore churn without ever pinning the demoted budget.
    pub(in crate::indexer) fn restore_hold_after_demotions(&self) -> Duration {
        let doublings = self
            .pressure_demotions
            .saturating_sub(1)
            .min(Self::MAX_RESTORE_HOLD_DOUBLINGS);
        self.restore_hold
            .checked_mul(1u32 << doublings)
            .unwrap_or(Duration::MAX)
    }

    pub(in crate::indexer) fn observe_new_producer_budget_wait(
        &mut self,
        observed_count: usize,
    ) -> bool {
        let new_wait_observed = observed_count > self.last_observed_producer_budget_wait_count;
        self.last_observed_producer_budget_wait_count = observed_count;
        new_wait_observed
    }

    pub(in crate::indexer) fn observe_new_producer_handoff_wait(
        &mut self,
        observed_count: usize,
    ) -> bool {
        let new_wait_observed = observed_count > self.last_observed_producer_handoff_wait_count;
        self.last_observed_producer_handoff_wait_count = observed_count;
        new_wait_observed
    }

    pub(in crate::indexer) fn detect_startup_budget_starvation(
        &self,
        runtime: &LexicalRebuildPipelineRuntimeSnapshot,
        new_producer_budget_wait: bool,
    ) -> Option<String> {
        if self.startup_budget == self.steady_budget {
            return None;
        }
        if new_producer_budget_wait {
            return Some(format!(
                "startup_producer_budget_wait_count_{}_promoted_steady_budget_before_first_durable_commit",
                runtime.producer_budget_wait_count
            ));
        }
        if runtime.producer_budget_waiter_count > 0 {
            return Some(format!(
                "startup_active_producer_budget_waiters_{}_promoted_steady_budget_before_first_durable_commit",
                runtime.producer_budget_waiter_count
            ));
        }

        None
    }

    pub(in crate::indexer) fn detect_pressure(
        &self,
        runtime: &LexicalRebuildPipelineRuntimeSnapshot,
        new_producer_handoff_wait: bool,
    ) -> Option<String> {
        if let Some(reason) = self.detect_host_pressure(runtime) {
            return Some(reason);
        }
        // GH #445: queue depth at capacity, inflight bytes near the cap, a
        // full pending batch, an ordered-barrier backlog, or a producer
        // handoff wait are the pipeline's bounded backpressure doing its job
        // — the healthy steady run on the reporting archive sat at 93% of
        // the inflight cap for an hour. They only mean the HOST is in trouble
        // when host telemetry says so (or is unavailable, in which case the
        // pipeline shape is the only signal we have and the old behaviour
        // stands). Demoting on saturation alone starved the producer while
        // Quill's accumulation kept growing RSS regardless.
        if self.host_is_demonstrably_calm(runtime) {
            return None;
        }
        self.detect_pipeline_saturation(runtime, new_producer_handoff_wait)
    }

    pub(in crate::indexer) fn detect_host_pressure(
        &self,
        runtime: &LexicalRebuildPipelineRuntimeSnapshot,
    ) -> Option<String> {
        if let (Some(loadavg_1m_milli), Some(high_watermark_1m_milli)) = (
            runtime.host_loadavg_1m_milli,
            self.loadavg_high_watermark_1m_milli,
        ) && loadavg_1m_milli >= high_watermark_1m_milli
        {
            return Some(format!(
                "host_loadavg_1m_{}_reached_high_watermark_{}",
                format_lexical_rebuild_loadavg_1m_milli(loadavg_1m_milli),
                format_lexical_rebuild_loadavg_1m_milli(high_watermark_1m_milli)
            ));
        }
        if let Some(available_memory_bytes) = runtime.host_available_memory_bytes {
            let available_memory_bytes = usize_from_u64_saturating(available_memory_bytes);
            if available_memory_bytes <= self.emergency_memory_reserve_bytes {
                return Some(format!(
                    "host_available_memory_bytes_{}_below_emergency_reserve_{}",
                    available_memory_bytes, self.emergency_memory_reserve_bytes
                ));
            }
            if available_memory_bytes <= self.memory_reserve_bytes {
                return Some(format!(
                    "host_available_memory_bytes_{}_below_reserve_{}",
                    available_memory_bytes, self.memory_reserve_bytes
                ));
            }
        }
        None
    }

    /// True only when host telemetry positively shows a calm machine: a
    /// loadavg reading at or below the low watermark AND (when known)
    /// available memory above the reserve. Missing loadavg telemetry (no
    /// reading, or no watermark on this platform) is NOT calm — it is
    /// unknown, and pipeline saturation keeps its say.
    pub(in crate::indexer) fn host_is_demonstrably_calm(
        &self,
        runtime: &LexicalRebuildPipelineRuntimeSnapshot,
    ) -> bool {
        let loadavg_calm = match (
            runtime.host_loadavg_1m_milli,
            self.loadavg_low_watermark_1m_milli,
        ) {
            (Some(loadavg_1m_milli), Some(low_watermark_1m_milli)) => {
                loadavg_1m_milli <= low_watermark_1m_milli
            }
            _ => false,
        };
        loadavg_calm && self.memory_is_clear(runtime)
    }

    pub(in crate::indexer) fn memory_is_clear(
        &self,
        runtime: &LexicalRebuildPipelineRuntimeSnapshot,
    ) -> bool {
        runtime
            .host_available_memory_bytes
            .is_none_or(|available_memory_bytes| {
                usize_from_u64_saturating(available_memory_bytes) > self.memory_reserve_bytes
            })
    }

    /// Why a demoted budget cannot be restored yet, or `None` when the host
    /// signals that can demote have cleared (loadavg at or below the low
    /// watermark — hysteresis against the high-watermark demote — and memory
    /// above the reserve). Pipeline occupancy is deliberately not consulted:
    /// a rebuild with a live producer never drains mid-run.
    pub(in crate::indexer) fn host_pressure_not_clear_reason(
        &self,
        runtime: &LexicalRebuildPipelineRuntimeSnapshot,
    ) -> Option<String> {
        if let (Some(loadavg_1m_milli), Some(low_watermark_1m_milli)) = (
            runtime.host_loadavg_1m_milli,
            self.loadavg_low_watermark_1m_milli,
        ) && loadavg_1m_milli > low_watermark_1m_milli
        {
            return Some(format!(
                "host_loadavg_1m_{}_above_low_watermark_{}",
                format_lexical_rebuild_loadavg_1m_milli(loadavg_1m_milli),
                format_lexical_rebuild_loadavg_1m_milli(low_watermark_1m_milli)
            ));
        }
        if !self.memory_is_clear(runtime) {
            return Some(format!(
                "host_available_memory_bytes_{}_at_or_below_reserve_{}",
                runtime.host_available_memory_bytes.unwrap_or_default(),
                self.memory_reserve_bytes
            ));
        }
        None
    }

    pub(in crate::indexer) fn detect_pipeline_saturation(
        &self,
        runtime: &LexicalRebuildPipelineRuntimeSnapshot,
        new_producer_handoff_wait: bool,
    ) -> Option<String> {
        let current_budget = self.current_budget();
        if new_producer_handoff_wait {
            return Some(format!(
                "producer_handoff_wait_count_{}_observed_consumer_backpressure",
                runtime.producer_handoff_wait_count
            ));
        }
        if runtime.queue_depth >= self.pipeline_channel_size {
            return Some(format!(
                "queue_depth_{}_reached_pipeline_capacity_{}",
                runtime.queue_depth, self.pipeline_channel_size
            ));
        }
        if runtime.ordered_buffered_pages > 0 && runtime.queue_depth > 0 {
            return Some(format!(
                "ordered_barrier_buffered_{}_pages_while_queue_depth_is_{}",
                runtime.ordered_buffered_pages, runtime.queue_depth
            ));
        }
        if current_budget.max_message_bytes_in_flight > 0
            && runtime.inflight_message_bytes.saturating_mul(100)
                >= current_budget
                    .max_message_bytes_in_flight
                    .saturating_mul(Self::INFLIGHT_HIGH_WATERMARK_PERCENT)
        {
            return Some(format!(
                "inflight_message_bytes_{}_near_limit_{}",
                runtime.inflight_message_bytes, current_budget.max_message_bytes_in_flight
            ));
        }
        if runtime.pending_batch_conversations >= current_budget.page_conversation_limit {
            return Some(format!(
                "pending_batch_conversations_{}_reached_limit_{}",
                runtime.pending_batch_conversations, current_budget.page_conversation_limit
            ));
        }
        if runtime.pending_batch_message_bytes >= current_budget.batch_fetch_message_bytes_limit {
            return Some(format!(
                "pending_batch_message_bytes_{}_reached_limit_{}",
                runtime.pending_batch_message_bytes, current_budget.batch_fetch_message_bytes_limit
            ));
        }
        None
    }
}

pub(in crate::indexer) fn format_lexical_rebuild_loadavg_1m_milli(loadavg_1m_milli: u32) -> String {
    format!("{:.3}", f64::from(loadavg_1m_milli) / 1000.0)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::indexer) struct LexicalRebuildPipelineBudgetSnapshot {
    pub(in crate::indexer) page_conversation_limit: usize,
    pub(in crate::indexer) batch_fetch_message_limit: usize,
    pub(in crate::indexer) batch_fetch_message_bytes_limit: usize,
    pub(in crate::indexer) max_message_bytes_in_flight: usize,
    pub(in crate::indexer) commit_interval_conversations: usize,
    pub(in crate::indexer) commit_interval_messages: usize,
    pub(in crate::indexer) commit_interval_message_bytes: usize,
}

impl LexicalRebuildPipelineBudgetSnapshot {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::indexer) fn new(
        page_conversation_limit: usize,
        batch_fetch_message_limit: usize,
        batch_fetch_message_bytes_limit: usize,
        max_message_bytes_in_flight: usize,
        commit_interval_conversations: usize,
        commit_interval_messages: usize,
        commit_interval_message_bytes: usize,
    ) -> Self {
        let max_message_bytes_in_flight = max_message_bytes_in_flight.max(2);
        // A reservation equal to the entire cap can be admitted only when no
        // other prepared page or sink packet retains any bytes. Ordered page
        // preparation does not guarantee that zero-inflight moment, so
        // cap-equality is a deadlocking configuration (GH#413). This generic
        // constructor lacks the channel geometry needed for a tighter split;
        // reserve half the cap so one equal-size predecessor and its successor
        // can coexist. The runtime constructor below divides by every bounded
        // channel slot and therefore remains more conservative.
        let batch_fetch_message_bytes_limit = batch_fetch_message_bytes_limit
            .max(1)
            .min(max_message_bytes_in_flight / 2)
            .max(1);
        Self {
            page_conversation_limit: page_conversation_limit.max(1),
            batch_fetch_message_limit: batch_fetch_message_limit.max(1),
            batch_fetch_message_bytes_limit,
            max_message_bytes_in_flight,
            commit_interval_conversations: commit_interval_conversations.max(1),
            commit_interval_messages: commit_interval_messages.max(1),
            commit_interval_message_bytes: commit_interval_message_bytes.max(1),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::indexer) struct LexicalRebuildFirstBudgetPromotionWait {
    pub(in crate::indexer) generation: usize,
    pub(in crate::indexer) commit_interval_conversations: usize,
    pub(in crate::indexer) commit_interval_messages: usize,
    pub(in crate::indexer) commit_interval_message_bytes: usize,
}

#[derive(Debug)]
pub(in crate::indexer) struct LexicalRebuildPipelineBudgetController {
    pub(in crate::indexer) page_conversation_limit: AtomicUsize,
    pub(in crate::indexer) batch_fetch_message_limit: AtomicUsize,
    pub(in crate::indexer) batch_fetch_message_bytes_limit: AtomicUsize,
    pub(in crate::indexer) max_message_bytes_in_flight: AtomicUsize,
    pub(in crate::indexer) commit_interval_conversations: AtomicUsize,
    pub(in crate::indexer) commit_interval_messages: AtomicUsize,
    pub(in crate::indexer) commit_interval_message_bytes: AtomicUsize,
    pub(in crate::indexer) generation: AtomicUsize,
    pub(in crate::indexer) generation_lock: Mutex<usize>,
    pub(in crate::indexer) generation_cv: Condvar,
}

impl LexicalRebuildPipelineBudgetController {
    pub(in crate::indexer) fn new(snapshot: LexicalRebuildPipelineBudgetSnapshot) -> Self {
        Self {
            page_conversation_limit: AtomicUsize::new(snapshot.page_conversation_limit),
            batch_fetch_message_limit: AtomicUsize::new(snapshot.batch_fetch_message_limit),
            batch_fetch_message_bytes_limit: AtomicUsize::new(
                snapshot.batch_fetch_message_bytes_limit,
            ),
            max_message_bytes_in_flight: AtomicUsize::new(snapshot.max_message_bytes_in_flight),
            commit_interval_conversations: AtomicUsize::new(snapshot.commit_interval_conversations),
            commit_interval_messages: AtomicUsize::new(snapshot.commit_interval_messages),
            commit_interval_message_bytes: AtomicUsize::new(snapshot.commit_interval_message_bytes),
            generation: AtomicUsize::new(0),
            generation_lock: Mutex::new(0),
            generation_cv: Condvar::new(),
        }
    }

    pub(in crate::indexer) fn snapshot(&self) -> LexicalRebuildPipelineBudgetSnapshot {
        LexicalRebuildPipelineBudgetSnapshot {
            page_conversation_limit: self.page_conversation_limit.load(Ordering::Relaxed),
            batch_fetch_message_limit: self.batch_fetch_message_limit.load(Ordering::Relaxed),
            batch_fetch_message_bytes_limit: self
                .batch_fetch_message_bytes_limit
                .load(Ordering::Relaxed),
            max_message_bytes_in_flight: self.max_message_bytes_in_flight.load(Ordering::Relaxed),
            commit_interval_conversations: self
                .commit_interval_conversations
                .load(Ordering::Relaxed),
            commit_interval_messages: self.commit_interval_messages.load(Ordering::Relaxed),
            commit_interval_message_bytes: self
                .commit_interval_message_bytes
                .load(Ordering::Relaxed),
        }
    }

    pub(in crate::indexer) fn generation(&self) -> usize {
        self.generation.load(Ordering::Acquire)
    }

    pub(in crate::indexer) fn update(&self, snapshot: LexicalRebuildPipelineBudgetSnapshot) {
        self.page_conversation_limit
            .store(snapshot.page_conversation_limit, Ordering::Relaxed);
        self.batch_fetch_message_limit
            .store(snapshot.batch_fetch_message_limit, Ordering::Relaxed);
        self.batch_fetch_message_bytes_limit
            .store(snapshot.batch_fetch_message_bytes_limit, Ordering::Relaxed);
        self.max_message_bytes_in_flight
            .store(snapshot.max_message_bytes_in_flight, Ordering::Relaxed);
        self.commit_interval_conversations
            .store(snapshot.commit_interval_conversations, Ordering::Relaxed);
        self.commit_interval_messages
            .store(snapshot.commit_interval_messages, Ordering::Relaxed);
        self.commit_interval_message_bytes
            .store(snapshot.commit_interval_message_bytes, Ordering::Relaxed);
        let new_generation = self.generation.fetch_add(1, Ordering::AcqRel) + 1;
        if lexical_pipeline_trace_enabled() {
            tracing::info!(
                new_generation,
                page_conversation_limit = snapshot.page_conversation_limit,
                batch_fetch_message_limit = snapshot.batch_fetch_message_limit,
                batch_fetch_message_bytes_limit = snapshot.batch_fetch_message_bytes_limit,
                max_message_bytes_in_flight = snapshot.max_message_bytes_in_flight,
                "CASS_PIPELINE_TRACE lexical rebuild pipeline budget generation bump"
            );
        }
        let mut guard = self
            .generation_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *guard = new_generation;
        self.generation_cv.notify_all();
    }

    pub(in crate::indexer) fn wait_for_update_after(
        &self,
        observed_generation: usize,
        timeout: Duration,
    ) -> bool {
        if self.generation() > observed_generation {
            return true;
        }

        let deadline = Instant::now() + timeout;
        let mut guard = self
            .generation_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while *guard <= observed_generation {
            let now = Instant::now();
            if now >= deadline {
                return self.generation() > observed_generation;
            }
            let remaining = deadline.saturating_duration_since(now);
            let (next_guard, timeout_result) = self
                .generation_cv
                .wait_timeout(guard, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard = next_guard;
            if timeout_result.timed_out() {
                return self.generation() > observed_generation;
            }
        }
        true
    }
}

/// Opt-in high-detail tracing for the lexical-rebuild flow-control path
/// (#288). When truthy, the producer/worker park-site transitions, every
/// budget-generation bump, and the periodic bounded-condvar-wait probes are
/// emitted at INFO so a parked pipeline can be diagnosed from logs without a
/// debugger; when unset the same probes stay at DEBUG/TRACE.
pub(in crate::indexer) fn lexical_pipeline_trace_enabled() -> bool {
    dotenvy_truthy("CASS_PIPELINE_TRACE")
}

/// How long a lexical-rebuild flow-control condvar wait may park before the
/// blocked predicate is logged and the wait re-arms. Pure observability: the
/// waiting thread always continues waiting after logging (#288).
pub(in crate::indexer) const LEXICAL_PIPELINE_BLOCKED_WAIT_LOG_INTERVAL: Duration =
    Duration::from_secs(5);

/// Current blocking site of the lexical-rebuild page-production stage
/// (producer thread + ordered page-prep workers), published through an
/// `AtomicU8` so the stall watchdog can report *where* the pipeline is
/// parked instead of only the cumulative after-success wait counters
/// (#288: `producer_budget_wait_count` kept growing while the controller
/// reported `steady_budget_with_headroom` and every thread was parked —
/// the payload never said which condvar held the pipeline).
///
/// Updated on entry to each blocking section and reset to `Idle` when the
/// section completes. Multiple threads share the cell (last writer wins);
/// in the all-parked starvation case there are no further writers, so the
/// final value identifies a genuine park site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(in crate::indexer) enum LexicalRebuildProducerParkSite {
    Idle = 0,
    ListingDb = 1,
    WaitingResult = 2,
    WaitingTurn = 3,
    WaitingBudget = 4,
    HandoffSend = 5,
}

impl LexicalRebuildProducerParkSite {
    pub(in crate::indexer) fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::ListingDb => "listing_db",
            Self::WaitingResult => "waiting_result",
            Self::WaitingTurn => "waiting_turn",
            Self::WaitingBudget => "waiting_budget",
            Self::HandoffSend => "handoff_send",
        }
    }

    pub(in crate::indexer) fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::ListingDb,
            2 => Self::WaitingResult,
            3 => Self::WaitingTurn,
            4 => Self::WaitingBudget,
            5 => Self::HandoffSend,
            _ => Self::Idle,
        }
    }

    pub(in crate::indexer) fn label_for(value: u8) -> &'static str {
        Self::from_u8(value).as_str()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::indexer) struct LexicalRebuildProducerTelemetrySnapshot {
    pub(in crate::indexer) page_prep_workers: usize,
    pub(in crate::indexer) active_page_prep_jobs: usize,
    pub(in crate::indexer) ordered_buffered_pages: usize,
    pub(in crate::indexer) budget_wait_count: usize,
    pub(in crate::indexer) budget_wait_ms: usize,
    pub(in crate::indexer) handoff_wait_count: usize,
    pub(in crate::indexer) handoff_wait_ms: usize,
    pub(in crate::indexer) producer_state: u8,
    pub(in crate::indexer) reservation_next_sequence: u64,
}

#[derive(Debug, Default)]
pub(in crate::indexer) struct LexicalRebuildProducerTelemetry {
    pub(in crate::indexer) page_prep_workers: AtomicUsize,
    pub(in crate::indexer) active_page_prep_jobs: AtomicUsize,
    pub(in crate::indexer) ordered_buffered_pages: AtomicUsize,
    pub(in crate::indexer) budget_wait_count: AtomicUsize,
    pub(in crate::indexer) budget_wait_ms: AtomicUsize,
    pub(in crate::indexer) handoff_wait_count: AtomicUsize,
    pub(in crate::indexer) handoff_wait_ms: AtomicUsize,
    /// See [`LexicalRebuildProducerParkSite`].
    pub(in crate::indexer) producer_state: AtomicU8,
    /// Mirror of `LexicalRebuildReservationOrder::next_sequence`, refreshed
    /// around each ordered budget acquisition so the stall payload can show
    /// which page sequence the reservation barrier is waiting to admit.
    pub(in crate::indexer) reservation_next_sequence: AtomicU64,
}

impl LexicalRebuildProducerTelemetry {
    pub(in crate::indexer) fn saturating_add(counter: &AtomicUsize, value: usize) {
        let _ = counter.try_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.saturating_add(value))
        });
    }

    pub(in crate::indexer) fn duration_millis(duration: Duration) -> usize {
        if duration.is_zero() {
            return 0;
        }
        usize::try_from(duration.as_millis())
            .unwrap_or(usize::MAX)
            .max(1)
    }

    pub(in crate::indexer) fn snapshot(&self) -> LexicalRebuildProducerTelemetrySnapshot {
        LexicalRebuildProducerTelemetrySnapshot {
            page_prep_workers: self.page_prep_workers.load(Ordering::Relaxed),
            active_page_prep_jobs: self.active_page_prep_jobs.load(Ordering::Relaxed),
            ordered_buffered_pages: self.ordered_buffered_pages.load(Ordering::Relaxed),
            budget_wait_count: self.budget_wait_count.load(Ordering::Relaxed),
            budget_wait_ms: self.budget_wait_ms.load(Ordering::Relaxed),
            handoff_wait_count: self.handoff_wait_count.load(Ordering::Relaxed),
            handoff_wait_ms: self.handoff_wait_ms.load(Ordering::Relaxed),
            producer_state: self.producer_state.load(Ordering::Relaxed),
            reservation_next_sequence: self.reservation_next_sequence.load(Ordering::Relaxed),
        }
    }

    pub(in crate::indexer) fn set_producer_state(&self, state: LexicalRebuildProducerParkSite) {
        let previous = self.producer_state.swap(state as u8, Ordering::Relaxed);
        if previous != state as u8 && lexical_pipeline_trace_enabled() {
            tracing::info!(
                from = LexicalRebuildProducerParkSite::label_for(previous),
                to = state.as_str(),
                "CASS_PIPELINE_TRACE lexical rebuild page-production park-site transition"
            );
        }
    }

    pub(in crate::indexer) fn record_reservation_next_sequence(&self, next_sequence: u64) {
        self.reservation_next_sequence
            .store(next_sequence, Ordering::Relaxed);
    }

    pub(in crate::indexer) fn record(
        &self,
        page_prep_workers: usize,
        active_page_prep_jobs: usize,
        ordered_buffered_pages: usize,
    ) {
        self.page_prep_workers
            .store(page_prep_workers, Ordering::Relaxed);
        self.active_page_prep_jobs
            .store(active_page_prep_jobs, Ordering::Relaxed);
        self.ordered_buffered_pages
            .store(ordered_buffered_pages, Ordering::Relaxed);
    }

    pub(in crate::indexer) fn record_budget_wait(&self, duration: Duration) {
        Self::saturating_add(&self.budget_wait_count, 1);
        Self::saturating_add(&self.budget_wait_ms, Self::duration_millis(duration));
    }

    pub(in crate::indexer) fn record_handoff_wait(&self, duration: Duration) {
        Self::saturating_add(&self.handoff_wait_count, 1);
        Self::saturating_add(&self.handoff_wait_ms, Self::duration_millis(duration));
    }
}

pub(in crate::indexer) fn lexical_rebuild_runtime_pipeline_budget_snapshot(
    page_conversation_limit: usize,
    batch_fetch_message_limit: usize,
    batch_fetch_message_bytes_limit: usize,
    pipeline_channel_size: usize,
    commit_interval_conversations: usize,
    commit_interval_messages: usize,
    commit_interval_message_bytes: usize,
) -> LexicalRebuildPipelineBudgetSnapshot {
    let configured_batch_fetch_message_bytes_limit = batch_fetch_message_bytes_limit.max(1);
    let max_message_bytes_in_flight = lexical_rebuild_pipeline_max_message_bytes_in_flight(
        configured_batch_fetch_message_bytes_limit,
        pipeline_channel_size,
    );
    let (batch_fetch_message_bytes_limit, max_message_bytes_in_flight) =
        bounded_lexical_rebuild_page_reservation_limits(
            configured_batch_fetch_message_bytes_limit,
            max_message_bytes_in_flight,
            pipeline_channel_size,
        );
    LexicalRebuildPipelineBudgetSnapshot::new(
        page_conversation_limit,
        batch_fetch_message_limit,
        batch_fetch_message_bytes_limit,
        max_message_bytes_in_flight,
        commit_interval_conversations,
        commit_interval_messages,
        commit_interval_message_bytes,
    )
}

/// GH #496: the message bytes one prepared page may hold, for the first
/// durable slice and for steady state, derived by the same budget path the
/// live rebuild uses (the commit-bytes interval, clamped to an equal share
/// of the in-flight budget). `status` reports both so a memory cap's effect
/// on page prep is visible, not only on the in-flight total.
pub(crate) fn lexical_rebuild_page_fetch_message_bytes(
    settings: &LexicalRebuildPipelineSettingsSnapshot,
) -> (usize, usize) {
    let page_bytes = |conversations, messages, message_bytes, commit_conversations| {
        lexical_rebuild_runtime_pipeline_budget_snapshot(
            conversations,
            messages,
            message_bytes,
            settings.pipeline_channel_size,
            commit_conversations,
            messages,
            message_bytes,
        )
        .batch_fetch_message_bytes_limit
    };
    (
        page_bytes(
            settings.startup_batch_fetch_conversations,
            settings.startup_commit_every_messages,
            settings.startup_commit_every_message_bytes,
            settings.startup_commit_every_conversations,
        ),
        page_bytes(
            settings.steady_batch_fetch_conversations,
            settings.steady_commit_every_messages,
            settings.steady_commit_every_message_bytes,
            settings.steady_commit_every_conversations,
        ),
    )
}

pub(in crate::indexer) fn bounded_lexical_rebuild_page_reservation_limits(
    configured_batch_fetch_message_bytes_limit: usize,
    max_message_bytes_in_flight: usize,
    pipeline_channel_size: usize,
) -> (usize, usize) {
    let reservation_slots = pipeline_channel_size.saturating_add(1).max(2);
    let max_message_bytes_in_flight = max_message_bytes_in_flight.max(reservation_slots);
    // The responsiveness governor may clamp the derived in-flight cap below
    // the configured per-page fetch ceiling. Re-establish the derivation
    // invariant after that clamp: one page may consume at most its equal share
    // of the bounded channel plus the active sink slot. This prevents
    // `request == cap`, which otherwise waits forever whenever any earlier
    // page retains bytes (GH#413), while preserving explicit smaller limits.
    let batch_fetch_message_bytes_limit = configured_batch_fetch_message_bytes_limit
        .max(1)
        .min(max_message_bytes_in_flight / reservation_slots)
        .max(1);
    (batch_fetch_message_bytes_limit, max_message_bytes_in_flight)
}

/// Size a page so a typical page of conversations fits its content
/// reservation: at most one per-message cap's worth of text per conversation.
/// This is a heuristic; the byte-budgeted batch fetch enforces the real bound
/// (it stops reading a conversation that cannot fit), and a long conversation
/// falls back to the prep worker's chunked path (bgn6s).
pub(in crate::indexer) fn lexical_rebuild_content_bounded_page_conversation_limit(
    configured_page_conversation_limit: usize,
    batch_fetch_message_bytes_limit: usize,
) -> usize {
    let batch_fetch_message_bytes_limit = batch_fetch_message_bytes_limit.max(1);
    let effective_conversation_content_cap =
        crate::storage::sqlite::lexical_max_message_content_bytes()
            .min(i32::MAX as usize)
            .min(batch_fetch_message_bytes_limit)
            .max(1);
    configured_page_conversation_limit.max(1).min(
        batch_fetch_message_bytes_limit
            .saturating_div(effective_conversation_content_cap)
            .max(1),
    )
}

pub(in crate::indexer) fn lexical_rebuild_pipeline_channel_size() -> usize {
    lexical_rebuild_setting("CASS_TANTIVY_REBUILD_PIPELINE_CHANNEL_SIZE")
        // Two slots left the ordered page producer spending ~5s stalled on
        // sink handoff after the final-frontier publish work moved out of the
        // critical path. Four keeps the queue explicitly bounded while giving
        // shard builders enough slack to overlap with page prep.
        .unwrap_or(4)
}

pub(in crate::indexer) fn lexical_rebuild_default_page_prep_worker_parallelism_for_workers(
    worker_parallelism: usize,
) -> usize {
    // Keep the producer decoupled from the tiny ordered sink channel, but cap
    // eager page prep at the measured point where extra DB overlap still buys
    // wall time. Eight workers was the fastest measured ceiling after the
    // missing-tail-state query stopped dominating shard planning; operators can
    // still override higher explicitly.
    if worker_parallelism <= 1 {
        1
    } else {
        worker_parallelism.div_ceil(2).clamp(2, 8)
    }
}

pub(in crate::indexer) fn lexical_rebuild_page_prep_worker_parallelism() -> usize {
    let desired = lexical_rebuild_page_prep_worker_parallelism_configured();
    // Let the machine-responsiveness governor scale this down when the host
    // is under pressure. `effective_worker_count` returns the caller-requested
    // value when the governor is idle or disabled, so behaviour on an idle
    // box is unchanged.
    responsiveness::effective_worker_count(desired).max(1)
}

pub(in crate::indexer) fn lexical_rebuild_page_prep_worker_parallelism_configured() -> usize {
    lexical_rebuild_setting("CASS_TANTIVY_REBUILD_PAGE_PREP_WORKERS")
        .unwrap_or_else(|| {
            // Keep page-prep fanout bounded and conservative, but decouple it
            // from the tiny ordered pipeline channel. The channel size limits
            // how many prepared pages can queue at the sink; it should not
            // silently cap producer-side fetch/prep overlap to ~3 workers on
            // large hosts.
            lexical_rebuild_default_page_prep_worker_parallelism_for_workers(
                lexical_rebuild_worker_parallelism(),
            )
        })
        .max(1)
}

pub(in crate::indexer) fn lexical_rebuild_first_budget_promotion_wait() -> Duration {
    Duration::from_millis(
        dotenvy::var("CASS_TANTIVY_REBUILD_FIRST_BUDGET_PROMOTION_WAIT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(5_000),
    )
}

pub(in crate::indexer) fn lexical_rebuild_pipeline_max_message_bytes_in_flight(
    batch_fetch_message_bytes_limit: usize,
    pipeline_channel_size: usize,
) -> usize {
    let desired = dotenvy::var("CASS_TANTIVY_REBUILD_PIPELINE_MAX_MESSAGE_BYTES_IN_FLIGHT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or_else(|| {
            batch_fetch_message_bytes_limit
                .max(1)
                .saturating_mul(pipeline_channel_size.saturating_add(1).max(1))
        });
    responsiveness::effective_inflight_byte_limit(desired).max(1)
}

pub(in crate::indexer) fn should_commit_lexical_rebuild(
    conversations_since_commit: usize,
    messages_since_commit: usize,
    message_bytes_since_commit: usize,
    commit_interval_conversations: usize,
    commit_interval_messages: usize,
    commit_interval_message_bytes: usize,
) -> bool {
    conversations_since_commit >= commit_interval_conversations
        || messages_since_commit >= commit_interval_messages
        || message_bytes_since_commit >= commit_interval_message_bytes
}

pub(in crate::indexer) fn pending_lexical_rebuild_batch_reached_runtime_limit(
    pending_conversations: usize,
    current_batch_conversation_limit: usize,
) -> bool {
    pending_conversations >= current_batch_conversation_limit.max(1)
}

pub(in crate::indexer) fn apply_lexical_rebuild_budget_transition(
    transition: LexicalRebuildBudgetTransition,
    flow_limiter: &StreamingByteLimiter,
    pipeline_budget_controller: &LexicalRebuildPipelineBudgetController,
    current_batch_conversation_limit: &mut usize,
    active_commit_intervals: Option<(&mut usize, &mut usize, &mut usize)>,
) {
    flow_limiter.update_max_bytes_in_flight(transition.new_budget.max_message_bytes_in_flight);
    pipeline_budget_controller.update(transition.new_budget);
    *current_batch_conversation_limit = transition.new_budget.page_conversation_limit;
    if let Some((
        commit_interval_conversations,
        commit_interval_messages,
        commit_interval_message_bytes,
    )) = active_commit_intervals
    {
        *commit_interval_conversations = transition.new_budget.commit_interval_conversations;
        *commit_interval_messages = transition.new_budget.commit_interval_messages;
        *commit_interval_message_bytes = transition.new_budget.commit_interval_message_bytes;
    }
    tracing::info!(
        controller_mode = transition.mode,
        controller_reason = %transition.reason,
        old_page_conversation_limit = transition.old_budget.page_conversation_limit,
        new_page_conversation_limit = transition.new_budget.page_conversation_limit,
        old_batch_fetch_message_limit = transition.old_budget.batch_fetch_message_limit,
        new_batch_fetch_message_limit = transition.new_budget.batch_fetch_message_limit,
        old_batch_fetch_message_bytes_limit = transition.old_budget.batch_fetch_message_bytes_limit,
        new_batch_fetch_message_bytes_limit = transition.new_budget.batch_fetch_message_bytes_limit,
        old_max_message_bytes_in_flight = transition.old_budget.max_message_bytes_in_flight,
        new_max_message_bytes_in_flight = transition.new_budget.max_message_bytes_in_flight,
        old_commit_interval_conversations =
            transition.old_budget.commit_interval_conversations,
        new_commit_interval_conversations =
            transition.new_budget.commit_interval_conversations,
        old_commit_interval_messages = transition.old_budget.commit_interval_messages,
        new_commit_interval_messages = transition.new_budget.commit_interval_messages,
        old_commit_interval_message_bytes =
            transition.old_budget.commit_interval_message_bytes,
        new_commit_interval_message_bytes =
            transition.new_budget.commit_interval_message_bytes,
        "updated lexical rebuild pipeline budgets"
    );
}

pub(in crate::indexer) fn should_persist_lexical_rebuild_progress(
    conversations_since_progress_persist: usize,
    progress_heartbeat_interval_conversations: usize,
    time_since_last_progress_persist: Duration,
    progress_heartbeat_interval: Duration,
) -> bool {
    conversations_since_progress_persist >= progress_heartbeat_interval_conversations
        || time_since_last_progress_persist >= progress_heartbeat_interval
}
