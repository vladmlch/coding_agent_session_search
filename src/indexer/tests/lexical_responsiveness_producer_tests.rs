use super::super::*;
use super::helpers::*;
use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use serial_test::serial;
use tempfile::TempDir;

#[test]
fn lexical_rebuild_default_page_prep_worker_parallelism_stays_bounded_without_channel_cap() {
    assert_eq!(
        lexical_rebuild_default_page_prep_worker_parallelism_for_workers(1),
        1
    );
    assert_eq!(
        lexical_rebuild_default_page_prep_worker_parallelism_for_workers(2),
        2
    );
    assert_eq!(
        lexical_rebuild_default_page_prep_worker_parallelism_for_workers(4),
        2
    );
    assert_eq!(
        lexical_rebuild_default_page_prep_worker_parallelism_for_workers(6),
        3
    );
    assert_eq!(
        lexical_rebuild_default_page_prep_worker_parallelism_for_workers(8),
        4
    );
    assert_eq!(
        lexical_rebuild_default_page_prep_worker_parallelism_for_workers(16),
        8
    );
    assert_eq!(
        lexical_rebuild_default_page_prep_worker_parallelism_for_workers(32),
        8,
        "measured ceiling moves to 8 after the shard-planning tail-state fix exposed page-prep overlap"
    );
    assert_eq!(
        lexical_rebuild_default_page_prep_worker_parallelism_for_workers(128),
        8,
        "128-core budget is still clamped at the measured 8-worker ceiling"
    );
}

#[test]
#[serial]
fn lexical_rebuild_pipeline_settings_snapshot_defaults_page_prep_workers_from_worker_budget() {
    let _responsiveness = set_env("CASS_RESPONSIVENESS_DISABLE", "1");
    let _workers = set_env("CASS_TANTIVY_REBUILD_WORKERS", "12");
    let _pipeline_channel = set_env("CASS_TANTIVY_REBUILD_PIPELINE_CHANNEL_SIZE", "2");
    let _writer_threads = set_env("CASS_TANTIVY_MAX_WRITER_THREADS", "2");

    let snapshot = lexical_rebuild_pipeline_settings_snapshot();

    assert_eq!(snapshot.workers, 12);
    assert_eq!(snapshot.pipeline_channel_size, 2);
    assert_eq!(
        snapshot.tantivy_writer_threads,
        snapshot.available_parallelism.min(2)
    );
    assert_eq!(
        snapshot.steady_batch_fetch_conversations,
        lexical_rebuild_default_batch_fetch_conversation_limit(
            LEXICAL_REBUILD_PAGE_SIZE,
            snapshot.tantivy_writer_threads
        )
    );
    assert_eq!(
        snapshot.page_prep_workers,
        responsiveness::effective_worker_count(6).max(1)
    );
    assert_eq!(
        snapshot.staged_merge_workers,
        responsiveness::effective_worker_count(3).max(1)
    );
}

#[test]
#[serial]
fn lexical_rebuild_pipeline_settings_snapshot_defaults_channel_to_measured_handoff_depth() {
    let _responsiveness = set_env("CASS_RESPONSIVENESS_DISABLE", "1");
    let _pipeline_channel = unset_env_var("CASS_TANTIVY_REBUILD_PIPELINE_CHANNEL_SIZE");
    let _pipeline_bytes =
        unset_env_var("CASS_TANTIVY_REBUILD_PIPELINE_MAX_MESSAGE_BYTES_IN_FLIGHT");

    let snapshot = lexical_rebuild_pipeline_settings_snapshot();
    let uncapped_message_bytes_in_flight = snapshot
        .startup_commit_every_message_bytes
        .max(1)
        .saturating_mul(snapshot.pipeline_channel_size.saturating_add(1).max(1));

    assert_eq!(snapshot.pipeline_channel_size, 4);
    assert_eq!(
        snapshot.pipeline_max_message_bytes_in_flight,
        responsiveness::effective_inflight_byte_limit(uncapped_message_bytes_in_flight).max(1)
    );
    assert!(snapshot.pipeline_max_message_bytes_in_flight <= uncapped_message_bytes_in_flight);
}

#[test]
fn lexical_rebuild_staged_shard_merge_settings_scales_with_eager_group_count() {
    let settings = LexicalRebuildPipelineSettingsSnapshot {
        workers: 12,
        available_parallelism: 12,
        reserved_cores: 2,
        tantivy_writer_threads: 8,
        staged_shard_builders: 8,
        staged_merge_workers: 3,
        controller_mode: "steady".into(),
        controller_restore_clear_samples: 3,
        controller_restore_hold_ms: 0,
        controller_loadavg_high_watermark_1m_milli: None,
        controller_loadavg_low_watermark_1m_milli: None,
        page_size: LEXICAL_REBUILD_PAGE_SIZE,
        steady_batch_fetch_conversations: 1024,
        startup_batch_fetch_conversations: 256,
        steady_commit_every_conversations: 1024,
        startup_commit_every_conversations: 256,
        steady_commit_every_messages: 2048,
        startup_commit_every_messages: 512,
        steady_commit_every_message_bytes: 1_000_000,
        startup_commit_every_message_bytes: 250_000,
        pipeline_channel_size: 2,
        page_prep_workers: 6,
        pipeline_max_message_bytes_in_flight: 4 * 1024 * 1024,
    };

    assert_eq!(
        lexical_rebuild_staged_shard_merge_settings(&settings, 3),
        LexicalRebuildShardMergeSettings { workers: 1 }
    );
    assert_eq!(
        lexical_rebuild_staged_shard_merge_settings(&settings, 5),
        LexicalRebuildShardMergeSettings { workers: 1 }
    );
    assert_eq!(
        lexical_rebuild_staged_shard_merge_settings(
            &settings,
            LexicalRebuildShardMergeCoordinator::EAGER_MERGE_FAN_IN * 3,
        ),
        LexicalRebuildShardMergeSettings { workers: 3 }
    );
    assert_eq!(
        lexical_rebuild_staged_shard_merge_settings(
            &settings,
            LexicalRebuildShardMergeCoordinator::EAGER_MERGE_FAN_IN * 8,
        ),
        LexicalRebuildShardMergeSettings { workers: 3 }
    );
}

#[test]
#[serial]
fn lexical_rebuild_first_budget_promotion_wait_defaults_to_short_bounded_wait() {
    let _guard = unset_env_var("CASS_TANTIVY_REBUILD_FIRST_BUDGET_PROMOTION_WAIT_MS");

    assert_eq!(
        lexical_rebuild_first_budget_promotion_wait(),
        Duration::from_millis(5_000)
    );
}

#[test]
fn gh413_lexical_rebuild_page_reservation_keeps_headroom_after_cap_clamp() {
    let (page_reservation, inflight_cap) =
        bounded_lexical_rebuild_page_reservation_limits(512, 512, 4);

    assert_eq!(inflight_cap, 512);
    assert_eq!(page_reservation, 102);
    assert!(
        page_reservation < inflight_cap,
        "a page reservation equal to the cap can only run at zero inflight bytes"
    );

    let limiter = StreamingByteLimiter::new(inflight_cap);
    let retained = limiter.acquire(38).unwrap();
    let (reserved, _wait_duration, waited) = limiter.acquire_with_wait(page_reservation).unwrap();
    assert_eq!(reserved, page_reservation);
    assert!(
        !waited,
        "the next page must remain admissible while an ordered predecessor retains bytes"
    );
    limiter.release(reserved);
    limiter.release(retained);
}

#[test]
#[serial]
fn lexical_rebuild_page_conversation_limit_respects_message_content_cap() {
    let _cap = set_env("CASS_LEXICAL_MAX_MESSAGE_CONTENT_BYTES", "8");

    assert_eq!(
        lexical_rebuild_content_bounded_page_conversation_limit(32, 20),
        2,
        "two eight-byte message caps fit inside a twenty-byte page budget"
    );
    assert_eq!(
        lexical_rebuild_content_bounded_page_conversation_limit(1, 20),
        1,
        "the content bound must not enlarge an explicitly smaller page"
    );
    assert_eq!(
        lexical_rebuild_content_bounded_page_conversation_limit(32, 4),
        1,
        "a page smaller than one message cap must still admit one outlier"
    );
}

#[test]
fn lexical_rebuild_conversation_chunks_cover_every_message_within_budget() {
    assert!(lexical_rebuild_conversation_chunks(&[], 10).is_empty());
    assert_eq!(
        lexical_rebuild_conversation_chunks(&[(0, 4), (1, 4), (2, 2)], 10),
        vec![(0, 2)],
        "a conversation within budget is one chunk"
    );
    assert_eq!(
        lexical_rebuild_conversation_chunks(&[(0, 4), (1, 4), (2, 4), (5, 1), (6, 9)], 10),
        vec![(0, 1), (2, 5), (6, 6)],
        "chunks close before exceeding the budget and span idx gaps"
    );
    assert_eq!(
        lexical_rebuild_conversation_chunks(&[(0, 3), (1, 25), (2, 3)], 10),
        vec![(0, 0), (1, 1), (2, 2)],
        "a message larger than the budget is a chunk of its own"
    );
    assert_eq!(
        lexical_rebuild_conversation_chunks(&[(0, 0), (1, 0), (2, 0)], 0),
        vec![(0, 2)],
        "empty messages never split a conversation"
    );
}

#[test]
fn gh413_lexical_rebuild_budget_snapshot_normalizes_cap_equal_reservations() {
    let budget = LexicalRebuildPipelineBudgetSnapshot::new(32, 64, 512, 512, 16, 128, 4_096);

    assert_eq!(budget.max_message_bytes_in_flight, 512);
    assert_eq!(budget.batch_fetch_message_bytes_limit, 256);
    assert!(
        budget.batch_fetch_message_bytes_limit < budget.max_message_bytes_in_flight,
        "directly constructed budgets must not reintroduce the cap-equality deadlock"
    );

    let limiter = StreamingByteLimiter::new(budget.max_message_bytes_in_flight);
    let predecessor = limiter
        .acquire(budget.batch_fetch_message_bytes_limit)
        .unwrap();
    let (successor, _wait_duration, waited) = limiter
        .acquire_with_wait(budget.batch_fetch_message_bytes_limit)
        .unwrap();
    assert!(!waited, "one predecessor must not strand its successor");
    limiter.release(successor);
    limiter.release(predecessor);
}

#[test]
fn gh413_lexical_rebuild_pending_batch_is_rechecked_after_runtime_limit_shrinks() {
    let retained_conversations = 128;

    assert!(!pending_lexical_rebuild_batch_reached_runtime_limit(
        retained_conversations,
        256,
    ));
    assert!(pending_lexical_rebuild_batch_reached_runtime_limit(
        retained_conversations,
        32,
    ));
}

#[test]
#[serial]
fn lexical_rebuild_responsiveness_controller_demotes_and_restores_with_hysteresis() {
    let _clear_samples = set_env("CASS_TANTIVY_REBUILD_CONTROLLER_RESTORE_CLEAR_SAMPLES", "2");
    let _hold_ms = set_env("CASS_TANTIVY_REBUILD_CONTROLLER_RESTORE_HOLD_MS", "1");
    let startup_budget =
        LexicalRebuildPipelineBudgetSnapshot::new(32, 64, 1024, 2_048, 16, 128, 4_096);
    let steady_budget =
        LexicalRebuildPipelineBudgetSnapshot::new(256, 512, 4096, 8_192, 1_024, 8_192, 65_536);
    let mut controller = LexicalRebuildResponsivenessController::new(
        LexicalRebuildResponsivenessPolicy::Auto,
        startup_budget,
        steady_budget,
        2,
        false,
        None,
        None,
    );

    assert_eq!(controller.mode(), "steady");
    assert_eq!(controller.current_budget(), steady_budget);

    let pressured_runtime = LexicalRebuildPipelineRuntimeSnapshot {
        queue_depth: 2,
        ..LexicalRebuildPipelineRuntimeSnapshot::default()
    };
    let transition = controller
        .observe_runtime(&pressured_runtime)
        .expect("pressure should demote");
    assert_eq!(transition.old_budget, steady_budget);
    assert_eq!(transition.new_budget, startup_budget);
    assert_eq!(transition.mode, "pressure_limited");
    assert_eq!(transition.new_budget.commit_interval_conversations, 16);
    assert_eq!(transition.new_budget.commit_interval_messages, 128);
    assert_eq!(transition.new_budget.commit_interval_message_bytes, 4_096);
    assert_eq!(controller.current_budget(), startup_budget);

    controller.last_transition_at = Instant::now() - controller.restore_hold;
    let clear_runtime = LexicalRebuildPipelineRuntimeSnapshot::default();
    assert!(controller.observe_runtime(&clear_runtime).is_none());
    assert_eq!(controller.mode(), "pressure_limited");
    let restore = controller
        .observe_runtime(&clear_runtime)
        .expect("clear streak should restore steady budget");
    assert_eq!(restore.old_budget, startup_budget);
    assert_eq!(restore.new_budget, steady_budget);
    assert_eq!(restore.mode, "steady");
    assert_eq!(restore.new_budget.commit_interval_conversations, 1_024);
    assert_eq!(restore.new_budget.commit_interval_messages, 8_192);
    assert_eq!(restore.new_budget.commit_interval_message_bytes, 65_536);
    assert_eq!(controller.current_budget(), steady_budget);
}

/// GH #445 fixtures: a 24-core Linux host (high watermark 22.0, low 21.0)
/// with plenty of memory, in steady mode, hold/samples pinned directly so
/// no env var is touched.
fn gh445_controller() -> (
    LexicalRebuildResponsivenessController,
    LexicalRebuildPipelineBudgetSnapshot,
    LexicalRebuildPipelineBudgetSnapshot,
) {
    // Byte limits mirror production shape (startup: 640 MiB inflight,
    // 128 MiB page fetch; steady: ~2.2 GiB inflight, 512 MiB page fetch)
    // so a 100 MB pending batch is routine, not a saturation trigger.
    let startup_budget =
        LexicalRebuildPipelineBudgetSnapshot::new(32, 64, 128 << 20, 640 << 20, 2_048, 128, 4_096);
    let steady_budget = LexicalRebuildPipelineBudgetSnapshot::new(
        256,
        512,
        512 << 20,
        2_357_809_664,
        10_000,
        8_192,
        65_536,
    );
    let mut controller = LexicalRebuildResponsivenessController::new(
        LexicalRebuildResponsivenessPolicy::Auto,
        startup_budget,
        steady_budget,
        8,
        false,
        Some(22_000),
        Some(21_000),
    );
    controller.restore_clear_samples = 3;
    controller.restore_hold = Duration::from_millis(1);
    controller.memory_reserve_bytes = 11 << 30;
    controller.emergency_memory_reserve_bytes = 4 << 30;
    (controller, startup_budget, steady_budget)
}

/// The reporter's mid-rebuild shape: pending batch, bytes in flight at
/// ~82% of the cap, a routine loadavg of 12.72 on 24 cores, 51 GB free.
fn gh445_busy_runtime(loadavg_1m_milli: u32) -> LexicalRebuildPipelineRuntimeSnapshot {
    LexicalRebuildPipelineRuntimeSnapshot {
        queue_depth: 0,
        inflight_message_bytes: 524_666_240,
        max_message_bytes_in_flight: 640 << 20,
        pending_batch_conversations: 15,
        pending_batch_message_bytes: 104_771_393,
        producer_budget_wait_count: 355,
        producer_budget_waiter_count: 1,
        host_loadavg_1m_milli: Some(loadavg_1m_milli),
        host_available_memory_bytes: Some(51_625_844_736),
        ..LexicalRebuildPipelineRuntimeSnapshot::default()
    }
}

#[test]
fn gh445_controller_restores_mid_rebuild_with_in_flight_work_once_host_is_calm() {
    let (mut controller, startup_budget, steady_budget) = gh445_controller();
    assert_eq!(controller.mode(), "steady");

    // A genuine host trigger demotes: loadavg over the high watermark.
    let overloaded = LexicalRebuildPipelineRuntimeSnapshot {
        host_loadavg_1m_milli: Some(25_000),
        ..gh445_busy_runtime(25_000)
    };
    let transition = controller
        .observe_runtime(&overloaded)
        .expect("host loadavg over the high watermark must demote");
    assert_eq!(transition.mode, "pressure_limited");
    assert!(
        transition
            .reason
            .starts_with("host_loadavg_1m_25.000_reached_high_watermark")
    );
    assert_eq!(controller.current_budget(), startup_budget);

    // Load drops to routine levels while the pipeline is still full of
    // work. The old gate needed an EMPTY pipeline (queue, pending batch,
    // inflight <= 50%) for three samples — never true mid-rebuild — and
    // pinned the run to the startup budget for good.
    controller.last_transition_at = Instant::now() - Duration::from_secs(1);
    let busy = gh445_busy_runtime(12_720);
    assert!(controller.observe_runtime(&busy).is_none());
    assert_eq!(controller.reason(), "awaiting_clear_pressure_window_1/3");
    assert!(controller.observe_runtime(&busy).is_none());
    assert_eq!(controller.reason(), "awaiting_clear_pressure_window_2/3");
    let restore = controller
        .observe_runtime(&busy)
        .expect("three calm host samples must restore the steady budget mid-rebuild");
    assert_eq!(restore.old_budget, startup_budget);
    assert_eq!(restore.new_budget, steady_budget);
    assert_eq!(restore.mode, "steady");
    assert_eq!(
        restore.reason,
        "restored_steady_budget_after_3_clear_samples"
    );
    assert_eq!(controller.current_budget(), steady_budget);
}

#[test]
fn gh445_inflight_near_cap_is_backpressure_not_pressure_when_host_is_calm() {
    let (mut controller, _, steady_budget) = gh445_controller();
    // The healthy steady run on the reporting archive sat at 93% of the
    // inflight cap with loadavg ~12 on 24 cores. That must not demote.
    let saturated = LexicalRebuildPipelineRuntimeSnapshot {
        inflight_message_bytes: 2_196_486_820,
        max_message_bytes_in_flight: steady_budget.max_message_bytes_in_flight,
        pending_batch_conversations: 63,
        queue_depth: 8,
        ordered_buffered_pages: 2,
        producer_handoff_wait_count: 1,
        ..gh445_busy_runtime(12_720)
    };
    assert!(controller.observe_runtime(&saturated).is_none());
    assert_eq!(controller.mode(), "steady");
    assert_eq!(controller.reason(), "steady_budget_with_headroom");
    assert_eq!(controller.current_budget(), steady_budget);
}

#[test]
fn gh445_saturation_still_demotes_when_loadavg_is_above_the_low_watermark() {
    let (mut controller, startup_budget, steady_budget) = gh445_controller();
    // Between the low (21.0) and high (22.0) watermarks the host is not
    // demonstrably calm, so a saturated pipeline keeps its say.
    let saturated_under_load = LexicalRebuildPipelineRuntimeSnapshot {
        inflight_message_bytes: 2_196_486_820,
        max_message_bytes_in_flight: steady_budget.max_message_bytes_in_flight,
        ..gh445_busy_runtime(21_500)
    };
    let transition = controller
        .observe_runtime(&saturated_under_load)
        .expect("inflight near the cap under load must demote");
    assert!(
        transition
            .reason
            .starts_with("inflight_message_bytes_2196486820_near_limit_")
    );
    assert_eq!(controller.current_budget(), startup_budget);

    // Restore is blocked while loadavg stays above the low watermark —
    // and the reason names the actual blocker, not a generic
    // "pressure_signals_not_yet_clear".
    controller.last_transition_at = Instant::now() - Duration::from_secs(1);
    let busy_under_load = gh445_busy_runtime(21_500);
    assert!(controller.observe_runtime(&busy_under_load).is_none());
    assert_eq!(
        controller.reason(),
        "host_loadavg_1m_21.500_above_low_watermark_21.000"
    );
    assert_eq!(controller.mode(), "pressure_limited");

    // Memory below the reserve blocks restore too (hysteresis on the
    // memory demote), then a calm streak restores.
    let low_memory = LexicalRebuildPipelineRuntimeSnapshot {
        host_available_memory_bytes: Some(10 << 30),
        ..gh445_busy_runtime(12_000)
    };
    assert!(controller.observe_runtime(&low_memory).is_none());
    assert!(
        controller
            .reason()
            .starts_with("host_available_memory_bytes_"),
        "{}",
        controller.reason()
    );
    let calm = gh445_busy_runtime(12_000);
    assert!(controller.observe_runtime(&calm).is_none());
    assert!(controller.observe_runtime(&calm).is_none());
    let restore = controller
        .observe_runtime(&calm)
        .expect("calm host restores");
    assert_eq!(restore.new_budget, steady_budget);
}

#[test]
fn gh445_saturation_demotes_without_host_telemetry_and_restore_hold_backs_off() {
    // No loadavg reading (non-Linux, or no watermark): the pipeline shape
    // is the only signal, so saturation demotes as before — and because
    // the steady budget saturates again as soon as it returns, the hold
    // doubles per demotion (capped at 8x) so the budgets cannot flap
    // every few seconds.
    let startup_budget =
        LexicalRebuildPipelineBudgetSnapshot::new(32, 64, 1024, 2_048, 16, 128, 4_096);
    let steady_budget =
        LexicalRebuildPipelineBudgetSnapshot::new(256, 512, 4096, 8_192, 1_024, 8_192, 65_536);
    let mut controller = LexicalRebuildResponsivenessController::new(
        LexicalRebuildResponsivenessPolicy::Auto,
        startup_budget,
        steady_budget,
        2,
        false,
        None,
        None,
    );
    controller.restore_clear_samples = 1;
    controller.restore_hold = Duration::from_millis(10);
    assert_eq!(
        controller.restore_hold_after_demotions(),
        Duration::from_millis(10)
    );

    let saturated = LexicalRebuildPipelineRuntimeSnapshot {
        queue_depth: 2,
        ..LexicalRebuildPipelineRuntimeSnapshot::default()
    };
    let calm = LexicalRebuildPipelineRuntimeSnapshot::default();
    let expected_holds_ms = [10u64, 20, 40, 80, 80, 80];
    for (demotion, expected_hold_ms) in expected_holds_ms.iter().enumerate() {
        let transition = controller
            .observe_runtime(&saturated)
            .unwrap_or_else(|| panic!("saturation demotes (demotion #{})", demotion + 1));
        assert_eq!(transition.mode, "pressure_limited");
        assert_eq!(
            controller.restore_hold_after_demotions(),
            Duration::from_millis(*expected_hold_ms),
            "hold after demotion #{}",
            demotion + 1
        );
        // Inside the hold: no restore, even though nothing is pressuring.
        assert!(controller.observe_runtime(&calm).is_none());
        assert!(
            controller
                .reason()
                .starts_with("holding_conservative_budget_after_pressure_demote_for_")
        );
        controller.last_transition_at = Instant::now() - Duration::from_millis(*expected_hold_ms);
        let restore = controller
            .observe_runtime(&calm)
            .expect("one calm sample after the hold restores");
        assert_eq!(restore.new_budget, steady_budget);
    }
}

#[test]
#[serial]
fn lexical_rebuild_responsiveness_controller_demotes_on_new_handoff_wait_delta() {
    let _clear_samples = set_env("CASS_TANTIVY_REBUILD_CONTROLLER_RESTORE_CLEAR_SAMPLES", "2");
    let _hold_ms = set_env("CASS_TANTIVY_REBUILD_CONTROLLER_RESTORE_HOLD_MS", "1");
    let startup_budget =
        LexicalRebuildPipelineBudgetSnapshot::new(32, 64, 1024, 2_048, 16, 128, 4_096);
    let steady_budget =
        LexicalRebuildPipelineBudgetSnapshot::new(256, 512, 4096, 8_192, 1_024, 8_192, 65_536);
    let mut controller = LexicalRebuildResponsivenessController::new(
        LexicalRebuildResponsivenessPolicy::Auto,
        startup_budget,
        steady_budget,
        2,
        false,
        None,
        None,
    );

    let baseline_runtime = LexicalRebuildPipelineRuntimeSnapshot::default();
    assert!(controller.observe_runtime(&baseline_runtime).is_none());
    assert_eq!(controller.mode(), "steady");

    let pressured_runtime = LexicalRebuildPipelineRuntimeSnapshot {
        producer_handoff_wait_count: 1,
        producer_handoff_wait_ms: 9,
        ..LexicalRebuildPipelineRuntimeSnapshot::default()
    };
    let transition = controller
        .observe_runtime(&pressured_runtime)
        .expect("a new producer handoff stall should demote the runtime budget");
    assert_eq!(transition.old_budget, steady_budget);
    assert_eq!(transition.new_budget, startup_budget);
    assert_eq!(transition.mode, "pressure_limited");
    assert!(
        transition
            .reason
            .contains("producer_handoff_wait_count_1_observed_consumer_backpressure"),
        "unexpected transition reason: {}",
        transition.reason
    );

    controller.last_transition_at = Instant::now() - controller.restore_hold;
    assert!(
        controller.observe_runtime(&pressured_runtime).is_none(),
        "cumulative handoff counters alone must not keep the controller permanently pressured"
    );
    let restore = controller
        .observe_runtime(&pressured_runtime)
        .expect("steady state should restore after the clear-window hold expires");
    assert_eq!(restore.old_budget, startup_budget);
    assert_eq!(restore.new_budget, steady_budget);
    assert_eq!(restore.mode, "steady");
}

#[test]
fn lexical_rebuild_responsiveness_controller_promotes_startup_on_budget_wait() {
    let startup_budget =
        LexicalRebuildPipelineBudgetSnapshot::new(32, 64, 1024, 2_048, 16, 128, 4_096);
    let steady_budget =
        LexicalRebuildPipelineBudgetSnapshot::new(256, 512, 4096, 8_192, 1_024, 8_192, 65_536);
    let mut controller = LexicalRebuildResponsivenessController::new(
        LexicalRebuildResponsivenessPolicy::Auto,
        startup_budget,
        steady_budget,
        2,
        true,
        None,
        None,
    );

    assert_eq!(controller.mode(), "startup");
    assert_eq!(controller.current_budget(), startup_budget);
    assert!(controller.waits_for_first_durable_commit());

    let stalled_runtime = LexicalRebuildPipelineRuntimeSnapshot {
        producer_budget_wait_count: 1,
        producer_budget_wait_ms: 7,
        ..LexicalRebuildPipelineRuntimeSnapshot::default()
    };
    let transition = controller
        .observe_runtime(&stalled_runtime)
        .expect("startup producer budget wait should promote before durable commit");
    assert_eq!(transition.old_budget, startup_budget);
    assert_eq!(transition.new_budget, steady_budget);
    assert_eq!(transition.mode, "steady");
    assert!(
        transition.reason.contains(
            "startup_producer_budget_wait_count_1_promoted_steady_budget_before_first_durable_commit"
        ),
        "unexpected transition reason: {}",
        transition.reason
    );
    assert_eq!(controller.current_budget(), steady_budget);
    assert!(!controller.waits_for_first_durable_commit());
    assert!(
        controller.record_first_durable_commit().is_none(),
        "the later durable checkpoint should not double-promote"
    );
}

#[test]
fn lexical_rebuild_responsiveness_controller_promotes_startup_on_active_budget_waiter() {
    let startup_budget =
        LexicalRebuildPipelineBudgetSnapshot::new(32, 64, 1024, 2_048, 16, 128, 4_096);
    let steady_budget =
        LexicalRebuildPipelineBudgetSnapshot::new(256, 512, 4096, 8_192, 1_024, 8_192, 65_536);
    let mut controller = LexicalRebuildResponsivenessController::new(
        LexicalRebuildResponsivenessPolicy::Auto,
        startup_budget,
        steady_budget,
        2,
        true,
        None,
        None,
    );

    let stalled_runtime = LexicalRebuildPipelineRuntimeSnapshot {
        producer_budget_waiter_count: 1,
        ..LexicalRebuildPipelineRuntimeSnapshot::default()
    };
    let transition = controller
        .observe_runtime(&stalled_runtime)
        .expect("active startup budget waiter should promote before the wait completes");
    assert_eq!(transition.old_budget, startup_budget);
    assert_eq!(transition.new_budget, steady_budget);
    assert_eq!(transition.mode, "steady");
    assert!(
        transition.reason.contains(
            "startup_active_producer_budget_waiters_1_promoted_steady_budget_before_first_durable_commit"
        ),
        "unexpected transition reason: {}",
        transition.reason
    );
    assert_eq!(controller.current_budget(), steady_budget);
}

#[test]
fn lexical_rebuild_idle_refresh_promotes_startup_waiter_budget() {
    let startup_budget =
        LexicalRebuildPipelineBudgetSnapshot::new(32, 64, 1024, 2_048, 16, 128, 64);
    let steady_budget =
        LexicalRebuildPipelineBudgetSnapshot::new(256, 512, 4096, 8_192, 1_024, 8_192, 65_536);
    let mut controller = LexicalRebuildResponsivenessController::new(
        LexicalRebuildResponsivenessPolicy::Auto,
        startup_budget,
        steady_budget,
        2,
        true,
        None,
        None,
    );
    let flow_limiter = Arc::new(StreamingByteLimiter::new(
        startup_budget.max_message_bytes_in_flight,
    ));
    let first_reservation = flow_limiter
        .acquire(startup_budget.max_message_bytes_in_flight)
        .unwrap();
    let (ready_tx, ready_rx) = bounded(1);
    let (result_tx, result_rx) = bounded(1);
    let waiter = {
        let flow_limiter = Arc::clone(&flow_limiter);
        thread::spawn(move || {
            ready_tx.send(()).unwrap();
            let reservation = flow_limiter.acquire(1).map_err(|err| err.to_string());
            if let Ok(reserved_bytes) = reservation {
                flow_limiter.release(reserved_bytes);
                result_tx.send(Ok(reserved_bytes)).unwrap();
            } else {
                result_tx.send(reservation).unwrap();
            }
        })
    };
    ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    for _ in 0..100 {
        if flow_limiter.active_waiter_count() > 0 {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    if flow_limiter.active_waiter_count() != 1 {
        flow_limiter.close();
        flow_limiter.release(first_reservation);
        let _ = result_rx.recv_timeout(Duration::from_secs(1));
        waiter.join().unwrap();
        panic!("expected one producer to be parked on startup byte budget");
    }

    let producer_telemetry = LexicalRebuildProducerTelemetry::default();
    let pipeline_budget_controller = LexicalRebuildPipelineBudgetController::new(startup_budget);
    let mut latest_runtime = LexicalRebuildPipelineRuntimeSnapshot::default();
    let mut current_batch_conversation_limit = startup_budget.page_conversation_limit;
    let mut commit_interval_conversations = startup_budget.commit_interval_conversations;
    let mut commit_interval_messages = startup_budget.commit_interval_messages;
    let mut commit_interval_message_bytes = startup_budget.commit_interval_message_bytes;
    refresh_and_maybe_apply_lexical_rebuild_pipeline_runtime(
        &mut latest_runtime,
        None,
        flow_limiter.as_ref(),
        Some(&producer_telemetry),
        &mut controller,
        &pipeline_budget_controller,
        &mut current_batch_conversation_limit,
        Some((
            &mut commit_interval_conversations,
            &mut commit_interval_messages,
            &mut commit_interval_message_bytes,
        )),
        LexicalRebuildPipelineSinkRuntimeSnapshot::new(0, 0, 0),
    );

    assert_eq!(controller.mode(), "steady");
    assert_eq!(
        flow_limiter.max_bytes_in_flight(),
        steady_budget.max_message_bytes_in_flight
    );
    match result_rx.recv_timeout(Duration::from_secs(1)) {
        Ok(Ok(reservation)) => assert_eq!(reservation, 1),
        Ok(Err(err)) => {
            flow_limiter.release(first_reservation);
            waiter.join().unwrap();
            panic!("startup budget waiter failed before idle refresh woke it: {err}");
        }
        Err(err) => {
            flow_limiter.close();
            flow_limiter.release(first_reservation);
            waiter.join().unwrap();
            panic!("startup budget waiter was not woken by idle refresh: {err}");
        }
    }
    flow_limiter.release(first_reservation);
    waiter.join().unwrap();
    assert_eq!(flow_limiter.active_waiter_count(), 0);
    assert_eq!(
        current_batch_conversation_limit,
        steady_budget.page_conversation_limit
    );
    assert_eq!(
        (
            commit_interval_conversations,
            commit_interval_messages,
            commit_interval_message_bytes,
        ),
        (
            steady_budget.commit_interval_conversations,
            steady_budget.commit_interval_messages,
            steady_budget.commit_interval_message_bytes,
        )
    );
}

#[test]
fn lexical_rebuild_responsiveness_controller_keeps_startup_on_live_inflight_cap_without_wait() {
    let startup_budget =
        LexicalRebuildPipelineBudgetSnapshot::new(32, 64, 1024, 2_048, 16, 128, 4_096);
    let steady_budget =
        LexicalRebuildPipelineBudgetSnapshot::new(256, 512, 4096, 8_192, 1_024, 8_192, 65_536);
    let mut controller = LexicalRebuildResponsivenessController::new(
        LexicalRebuildResponsivenessPolicy::Auto,
        startup_budget,
        steady_budget,
        2,
        true,
        None,
        None,
    );

    let stalled_runtime = LexicalRebuildPipelineRuntimeSnapshot {
        inflight_message_bytes: startup_budget.max_message_bytes_in_flight,
        max_message_bytes_in_flight: startup_budget.max_message_bytes_in_flight,
        active_page_prep_jobs: 1,
        ..LexicalRebuildPipelineRuntimeSnapshot::default()
    };
    assert!(
        controller.observe_runtime(&stalled_runtime).is_none(),
        "startup cap fullness alone is normal bounded backpressure, not starvation"
    );
    assert_eq!(controller.mode(), "startup");
    assert_eq!(controller.current_budget(), startup_budget);
}

#[test]
fn lexical_rebuild_responsiveness_controller_honors_pinned_conservative_mode() {
    let startup_budget =
        LexicalRebuildPipelineBudgetSnapshot::new(32, 64, 1024, 2_048, 16, 128, 4_096);
    let steady_budget =
        LexicalRebuildPipelineBudgetSnapshot::new(256, 512, 4096, 8_192, 1_024, 8_192, 65_536);
    let mut controller = LexicalRebuildResponsivenessController::new(
        LexicalRebuildResponsivenessPolicy::Conservative,
        startup_budget,
        steady_budget,
        2,
        true,
        None,
        None,
    );

    assert_eq!(controller.mode(), "pinned_conservative");
    assert_eq!(controller.current_budget(), startup_budget);
    assert!(!controller.waits_for_first_durable_commit());
    assert!(controller.record_first_durable_commit().is_none());
    assert!(
        controller
            .observe_runtime(&LexicalRebuildPipelineRuntimeSnapshot::default())
            .is_none()
    );
    assert!(
        controller
            .observe_runtime(&LexicalRebuildPipelineRuntimeSnapshot {
                inflight_message_bytes: startup_budget.max_message_bytes_in_flight,
                max_message_bytes_in_flight: startup_budget.max_message_bytes_in_flight,
                active_page_prep_jobs: 1,
                producer_budget_wait_count: 1,
                producer_budget_wait_ms: 7,
                producer_budget_waiter_count: 1,
                ..LexicalRebuildPipelineRuntimeSnapshot::default()
            })
            .is_none(),
        "pinned conservative mode must ignore startup escape telemetry"
    );
    assert_eq!(controller.current_budget(), startup_budget);
}

#[test]
fn lexical_rebuild_budget_transition_updates_active_commit_cadence() {
    let startup_budget =
        LexicalRebuildPipelineBudgetSnapshot::new(32, 64, 1024, 2_048, 16, 128, 4_096);
    let steady_budget =
        LexicalRebuildPipelineBudgetSnapshot::new(256, 512, 4096, 8_192, 1_024, 8_192, 65_536);
    let flow_limiter = StreamingByteLimiter::new(steady_budget.max_message_bytes_in_flight);
    let pipeline_budget_controller = LexicalRebuildPipelineBudgetController::new(steady_budget);
    let mut current_batch_conversation_limit = steady_budget.page_conversation_limit;
    let mut commit_interval_conversations = steady_budget.commit_interval_conversations;
    let mut commit_interval_messages = steady_budget.commit_interval_messages;
    let mut commit_interval_message_bytes = steady_budget.commit_interval_message_bytes;

    apply_lexical_rebuild_budget_transition(
        LexicalRebuildBudgetTransition {
            old_budget: steady_budget,
            new_budget: startup_budget,
            mode: "pressure_limited",
            reason: "test_pressure".into(),
        },
        &flow_limiter,
        &pipeline_budget_controller,
        &mut current_batch_conversation_limit,
        Some((
            &mut commit_interval_conversations,
            &mut commit_interval_messages,
            &mut commit_interval_message_bytes,
        )),
    );

    assert_eq!(
        current_batch_conversation_limit,
        startup_budget.page_conversation_limit
    );
    assert_eq!(
        flow_limiter.max_bytes_in_flight(),
        startup_budget.max_message_bytes_in_flight
    );
    assert_eq!(
        pipeline_budget_controller.snapshot(),
        startup_budget,
        "producer-side budget snapshots should include the demoted commit cadence"
    );
    assert_eq!(
        (
            commit_interval_conversations,
            commit_interval_messages,
            commit_interval_message_bytes,
        ),
        (
            startup_budget.commit_interval_conversations,
            startup_budget.commit_interval_messages,
            startup_budget.commit_interval_message_bytes,
        )
    );
}

#[test]
fn parse_lexical_rebuild_loadavg_1m_milli_reads_first_field() {
    assert_eq!(
        parse_lexical_rebuild_loadavg_1m_milli("1.50 1.21 0.80 2/199 1234\n"),
        Some(1_500)
    );
}

#[test]
#[serial]
fn lexical_rebuild_responsiveness_controller_demotes_and_restores_on_host_loadavg_pressure() {
    let _clear_samples = set_env("CASS_TANTIVY_REBUILD_CONTROLLER_RESTORE_CLEAR_SAMPLES", "1");
    let _hold_ms = set_env("CASS_TANTIVY_REBUILD_CONTROLLER_RESTORE_HOLD_MS", "1");
    let startup_budget =
        LexicalRebuildPipelineBudgetSnapshot::new(32, 64, 1024, 2_048, 16, 128, 4_096);
    let steady_budget =
        LexicalRebuildPipelineBudgetSnapshot::new(256, 512, 4096, 8_192, 1_024, 8_192, 65_536);
    let mut controller = LexicalRebuildResponsivenessController::new(
        LexicalRebuildResponsivenessPolicy::Auto,
        startup_budget,
        steady_budget,
        2,
        false,
        Some(7_000),
        Some(6_000),
    );

    let pressured_runtime = LexicalRebuildPipelineRuntimeSnapshot {
        host_loadavg_1m_milli: Some(7_250),
        ..LexicalRebuildPipelineRuntimeSnapshot::default()
    };
    let transition = controller
        .observe_runtime(&pressured_runtime)
        .expect("host loadavg should demote");
    assert_eq!(transition.old_budget, steady_budget);
    assert_eq!(transition.new_budget, startup_budget);
    assert_eq!(transition.mode, "pressure_limited");
    assert!(
        transition
            .reason
            .contains("host_loadavg_1m_7.250_reached_high_watermark_7.000"),
        "unexpected reason: {}",
        transition.reason
    );

    controller.last_transition_at = Instant::now() - controller.restore_hold;
    let clear_runtime = LexicalRebuildPipelineRuntimeSnapshot {
        host_loadavg_1m_milli: Some(5_750),
        ..LexicalRebuildPipelineRuntimeSnapshot::default()
    };
    let restore = controller
        .observe_runtime(&clear_runtime)
        .expect("sub-threshold loadavg should restore steady budget");
    assert_eq!(restore.old_budget, startup_budget);
    assert_eq!(restore.new_budget, steady_budget);
    assert_eq!(restore.mode, "steady");
}

#[test]
fn lexical_rebuild_packet_producer_builds_lookup_and_source_context_internally() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("producer-lookups.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    seed_lexical_rebuild_fixture(&storage);
    let agent_id = storage
        .ensure_agent(&Agent {
            id: None,
            slug: "codex".into(),
            name: "Codex".into(),
            version: Some("0.2.3".into()),
            kind: AgentKind::Cli,
        })
        .unwrap();
    storage
        .insert_conversation_tree(
            agent_id,
            None,
            &Conversation {
                id: None,
                agent_slug: "codex".into(),
                workspace: Some(PathBuf::from("/workspace/alpha")),
                external_id: Some("remote-lexical-fixture".into()),
                title: Some("Remote lexical rebuild fixture".into()),
                source_path: PathBuf::from("/tmp/remote-lexical-fixture.jsonl"),
                started_at: Some(1_700_000_010_000),
                ended_at: Some(1_700_000_010_100),
                approx_tokens: Some(64),
                metadata_json: serde_json::Value::Null,
                messages: vec![
                    Message {
                        id: None,
                        idx: 0,
                        role: MessageRole::User,
                        author: Some("user".into()),
                        created_at: Some(1_700_000_010_010),
                        content: "remote-first".into(),
                        extra_json: serde_json::json!({"opaque": true}),
                        snippets: Vec::new(),
                    },
                    Message {
                        id: None,
                        idx: 1,
                        role: MessageRole::Agent,
                        author: Some("assistant".into()),
                        created_at: Some(1_700_000_010_020),
                        content: "remote-second".into(),
                        extra_json: serde_json::json!({"opaque": true}),
                        snippets: Vec::new(),
                    },
                ],
                source_id: "remote-builder".into(),
                origin_host: Some("builder-host".into()),
            },
        )
        .unwrap();
    drop(storage);

    let (tx, rx) = bounded::<LexicalRebuildPipelineMessage>(4);
    // Keep the lookup fixture at one three-conversation page. The live
    // governor may shrink a runtime request under load, so inject the
    // fixed page budget and use its matching in-flight limit.
    let pipeline_budget = LexicalRebuildPipelineBudgetSnapshot::new(
        3,
        32,
        3 * 8 * 1024 * 1024,
        15 * 8 * 1024 * 1024,
        3,
        32,
        3 * 8 * 1024 * 1024,
    );
    let flow_limiter = Arc::new(StreamingByteLimiter::new(
        pipeline_budget.max_message_bytes_in_flight,
    ));
    let handle = spawn_lexical_rebuild_packet_producer(
        db_path,
        None,
        None,
        LEXICAL_REBUILD_PAGE_SIZE,
        4,
        None,
        Arc::new(LexicalRebuildPipelineBudgetController::new(pipeline_budget)),
        tx,
        flow_limiter.clone(),
        None,
        Arc::new(LexicalRebuildProducerTelemetry::default()),
    );

    let batch = match rx.recv_timeout(Duration::from_secs(10)).unwrap() {
        LexicalRebuildPipelineMessage::Batch(batch) => batch,
        other => panic!("expected prepared batch, got {other:?}"),
    };
    match rx.recv_timeout(Duration::from_secs(10)).unwrap() {
        LexicalRebuildPipelineMessage::Done => {}
        other => panic!("expected pipeline completion, got {other:?}"),
    }
    handle.join().unwrap();

    assert_eq!(batch.packets.len(), 3);
    let remote_packet = batch
        .packets
        .iter()
        .find(|packet| packet.identity.external_id.as_deref() == Some("remote-lexical-fixture"))
        .expect("remote fixture packet should be present");
    assert_eq!(remote_packet.identity.agent, "codex");
    assert_eq!(
        remote_packet.identity.external_id.as_deref(),
        Some("remote-lexical-fixture")
    );
    assert_eq!(remote_packet.provenance.source_id, "remote-builder");
    assert_eq!(remote_packet.provenance.origin_kind, "remote");
    assert_eq!(
        remote_packet.provenance.origin_host.as_deref(),
        Some("builder-host")
    );
    assert!(
        batch.packets.iter().all(|packet| packet.message_count > 0),
        "fixture pages should still carry grouped messages after producer-owned lookup warmup"
    );
    assert!(
        flow_limiter.bytes_in_flight() > 0,
        "consumer-owned release should keep byte reservations visible until the sink drains them"
    );
    flow_limiter.release(flow_limiter.bytes_in_flight());
    assert_eq!(flow_limiter.bytes_in_flight(), 0);
}

#[test]
fn lexical_rebuild_producer_startup_delivers_first_batch_quickly() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("startup-timing.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    seed_lexical_rebuild_fixture(&storage);
    let agent_id = storage
        .ensure_agent(&Agent {
            id: None,
            slug: "codex".into(),
            name: "Codex".into(),
            version: Some("0.2.3".into()),
            kind: AgentKind::Cli,
        })
        .unwrap();
    for i in 0..50 {
        storage
            .insert_conversation_tree(
                agent_id,
                None,
                &Conversation {
                    id: None,
                    agent_slug: "codex".into(),
                    workspace: Some(PathBuf::from("/workspace/bulk")),
                    external_id: Some(format!("bulk-conv-{i}")),
                    title: Some(format!("Bulk conversation {i}")),
                    source_path: PathBuf::from(format!("/tmp/bulk-{i}.jsonl")),
                    started_at: Some(1_700_000_100_000 + i * 1000),
                    ended_at: Some(1_700_000_100_100 + i * 1000),
                    approx_tokens: Some(32),
                    metadata_json: serde_json::Value::Null,
                    messages: vec![Message {
                        id: None,
                        idx: 0,
                        role: MessageRole::User,
                        author: None,
                        created_at: Some(1_700_000_100_010 + i * 1000),
                        content: format!("bulk message {i}"),
                        extra_json: serde_json::Value::Null,
                        snippets: Vec::new(),
                    }],
                    source_id: LOCAL_SOURCE_ID.into(),
                    origin_host: None,
                },
            )
            .unwrap();
    }
    drop(storage);

    let started = Instant::now();
    let (tx, rx) = bounded::<LexicalRebuildPipelineMessage>(4);
    let flow_limiter = Arc::new(StreamingByteLimiter::new(64 * 1024));
    let handle = spawn_lexical_rebuild_packet_producer(
        db_path,
        None,
        None,
        LEXICAL_REBUILD_PAGE_SIZE,
        4,
        None,
        Arc::new(LexicalRebuildPipelineBudgetController::new(
            lexical_rebuild_runtime_pipeline_budget_snapshot(16, 128, 8192, 4, 16, 128, 8192),
        )),
        tx,
        flow_limiter.clone(),
        None,
        Arc::new(LexicalRebuildProducerTelemetry::default()),
    );

    let first_batch = match rx.recv_timeout(Duration::from_secs(10)) {
        Ok(LexicalRebuildPipelineMessage::Batch(batch)) => batch,
        Ok(other) => panic!("expected batch as first message, got {other:?}"),
        Err(_) => panic!("timed out waiting for first batch from producer"),
    };
    let first_batch_ms = started.elapsed().as_millis();

    assert!(
        !first_batch.packets.is_empty(),
        "first batch should contain at least one conversation packet"
    );
    assert!(
        first_batch_ms < 5_000,
        "producer should deliver first batch within 5s, took {first_batch_ms}ms"
    );
    // The first batch carries a flow-limiter reservation exactly like the
    // ones the drain loop below releases. Holding on to it starves the
    // page-prep workers of budget they can never get back, which parks the
    // producer in `acquire_with_wait` forever and turns the `join()` at the
    // end of this test into an unkillable hang.
    release_lexical_rebuild_prepared_page_reservation(&first_batch, flow_limiter.as_ref());

    let mut saw_done = false;
    while let Ok(msg) = rx.recv_timeout(Duration::from_secs(5)) {
        match msg {
            LexicalRebuildPipelineMessage::Done => {
                saw_done = true;
                break;
            }
            LexicalRebuildPipelineMessage::Batch(batch) => {
                release_lexical_rebuild_prepared_page_reservation(&batch, flow_limiter.as_ref());
            }
            LexicalRebuildPipelineMessage::Error(err) => panic!("producer error: {err}"),
        }
    }
    // Without this the test would fall straight into `join()` and block
    // forever whenever the producer goes quiet, hiding the real failure.
    assert!(
        saw_done,
        "producer stopped sending without a Done message; joining it would hang"
    );
    handle.join().unwrap();
}

#[test]
#[serial]
fn lexical_rebuild_packet_producer_skips_first_promotion_wait_after_early_generation_update() {
    let _wait = set_env("CASS_TANTIVY_REBUILD_FIRST_BUDGET_PROMOTION_WAIT_MS", "25");
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("producer-early-budget-promotion.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    seed_lexical_rebuild_fixture(&storage);
    drop(storage);

    let startup_budget = lexical_rebuild_runtime_pipeline_budget_snapshot(1, 2, 1024, 2, 1, 1, 1);
    let steady_budget =
        lexical_rebuild_runtime_pipeline_budget_snapshot(2, 32, 64 * 1024, 2, 16, 32, 64);
    let pipeline_budget_controller =
        Arc::new(LexicalRebuildPipelineBudgetController::new(startup_budget));
    let first_budget_promotion_wait = Some(LexicalRebuildFirstBudgetPromotionWait {
        generation: pipeline_budget_controller.generation(),
        commit_interval_conversations: startup_budget.commit_interval_conversations,
        commit_interval_messages: startup_budget.commit_interval_messages,
        commit_interval_message_bytes: startup_budget.commit_interval_message_bytes,
    });
    pipeline_budget_controller.update(steady_budget);

    let logs = capture_logs(|| {
        let (tx, rx) = bounded::<LexicalRebuildPipelineMessage>(2);
        let flow_limiter = Arc::new(StreamingByteLimiter::new(
            steady_budget.max_message_bytes_in_flight,
        ));
        let handle = spawn_lexical_rebuild_packet_producer(
            db_path,
            None,
            None,
            LEXICAL_REBUILD_PAGE_SIZE,
            2,
            first_budget_promotion_wait,
            pipeline_budget_controller,
            tx,
            flow_limiter.clone(),
            None,
            Arc::new(LexicalRebuildProducerTelemetry::default()),
        );

        let mut batches = 0usize;
        loop {
            match rx.recv_timeout(Duration::from_secs(10)).unwrap() {
                LexicalRebuildPipelineMessage::Batch(batch) => {
                    batches = batches.saturating_add(1);
                    release_lexical_rebuild_prepared_page_reservation(
                        &batch,
                        flow_limiter.as_ref(),
                    );
                }
                LexicalRebuildPipelineMessage::Done => break,
                LexicalRebuildPipelineMessage::Error(error) => {
                    panic!("producer returned error: {error}")
                }
            }
        }
        handle.join().unwrap();
        assert!(batches > 0);
        assert_eq!(flow_limiter.bytes_in_flight(), 0);
    });

    assert!(
        logs.contains("lexical rebuild producer observed first budget promotion before waiting"),
        "producer should recognize an already-advanced budget generation without parking: {logs}"
    );
    assert!(
        !logs.contains("lexical rebuild producer waiting for first durable budget promotion"),
        "producer must not enter the old timeout path after an early generation update: {logs}"
    );
    assert!(
        !logs.contains("timed out waiting for first durable budget promotion"),
        "producer must not pay even the bounded first-promotion timeout: {logs}"
    );
}

#[test]
fn lexical_rebuild_packet_producer_releases_budget_when_consumer_disconnects() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("producer-disconnect.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    seed_lexical_rebuild_fixture(&storage);
    drop(storage);

    let (tx, rx) = bounded::<LexicalRebuildPipelineMessage>(1);
    drop(rx);
    let flow_limiter = Arc::new(StreamingByteLimiter::new(1));
    let handle = spawn_lexical_rebuild_packet_producer(
        db_path,
        None,
        None,
        LEXICAL_REBUILD_PAGE_SIZE,
        1,
        None,
        Arc::new(LexicalRebuildPipelineBudgetController::new(
            lexical_rebuild_runtime_pipeline_budget_snapshot(2, 32, 1024, 4, 2, 32, 1024),
        )),
        tx,
        flow_limiter.clone(),
        None,
        Arc::new(LexicalRebuildProducerTelemetry::default()),
    );

    handle
        .join()
        .expect("lexical rebuild producer should stop cleanly after consumer disconnect");
    assert_eq!(
        flow_limiter.bytes_in_flight(),
        0,
        "disconnect path must release any reserved pipeline byte budget"
    );
}

#[test]
#[serial]
fn lexical_rebuild_packet_producer_preserves_flow_budget_under_sustained_burst() {
    let _page_prep_workers = set_env("CASS_TANTIVY_REBUILD_PAGE_PREP_WORKERS", "2");
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("producer-handoff-backpressure.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    seed_lexical_rebuild_fixture(&storage);
    let agent_id = storage
        .ensure_agent(&Agent {
            id: None,
            slug: "codex".into(),
            name: "Codex".into(),
            version: Some("0.2.3".into()),
            kind: AgentKind::Cli,
        })
        .unwrap();
    for suffix in 3..=8 {
        let external_id = format!("handoff-burst-{suffix}");
        let base_ts = 1_700_000_300_000_i64 + i64::from(suffix) * 1_000;
        storage
            .insert_conversation_tree(
                agent_id,
                None,
                &Conversation {
                    id: None,
                    agent_slug: "codex".into(),
                    workspace: Some(PathBuf::from("/tmp/workspace")),
                    external_id: Some(external_id.clone()),
                    title: Some("Handoff burst fixture".into()),
                    source_path: PathBuf::from(format!("/tmp/{external_id}.jsonl")),
                    started_at: Some(base_ts),
                    ended_at: Some(base_ts + 100),
                    approx_tokens: Some(64),
                    metadata_json: serde_json::Value::Null,
                    messages: vec![
                        Message {
                            id: None,
                            idx: 0,
                            role: MessageRole::User,
                            author: Some("user".into()),
                            created_at: Some(base_ts + 10),
                            content: format!("{external_id}-first"),
                            extra_json: serde_json::json!({"opaque": true}),
                            snippets: Vec::new(),
                        },
                        Message {
                            id: None,
                            idx: 1,
                            role: MessageRole::Agent,
                            author: Some("assistant".into()),
                            created_at: Some(base_ts + 20),
                            content: format!("{external_id}-second"),
                            extra_json: serde_json::json!({"opaque": true}),
                            snippets: Vec::new(),
                        },
                    ],
                    source_id: LOCAL_SOURCE_ID.into(),
                    origin_host: None,
                },
            )
            .unwrap();
    }
    drop(storage);

    let (tx, rx) = bounded::<LexicalRebuildPipelineMessage>(1);
    let flow_limiter = Arc::new(StreamingByteLimiter::new(256 * 1024));
    let producer_telemetry = Arc::new(LexicalRebuildProducerTelemetry::default());
    let handle = spawn_lexical_rebuild_packet_producer(
        db_path,
        None,
        None,
        1,
        1,
        None,
        Arc::new(LexicalRebuildPipelineBudgetController::new(
            lexical_rebuild_runtime_pipeline_budget_snapshot(1, 32, 64 * 1024, 1, 1, 32, 64 * 1024),
        )),
        tx,
        flow_limiter.clone(),
        None,
        producer_telemetry.clone(),
    );

    let saturation_deadline = Instant::now() + Duration::from_secs(5);
    while rx.is_empty() && Instant::now() < saturation_deadline {
        assert!(
            !handle.is_finished(),
            "producer finished before bounded handoff queue filled"
        );
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        rx.len(),
        1,
        "slow consumer should leave the bounded handoff queue saturated"
    );
    assert!(
        !handle.is_finished(),
        "producer should still be running while the handoff queue is saturated"
    );

    thread::sleep(Duration::from_millis(50));
    let first_batch = match rx.recv_timeout(Duration::from_secs(10)).unwrap() {
        LexicalRebuildPipelineMessage::Batch(batch) => batch,
        other => panic!("expected first burst batch, got {other:?}"),
    };
    assert_eq!(first_batch.packets.len(), 1);

    let mut held_batches = vec![first_batch];
    assert!(
        flow_limiter.bytes_in_flight() > 0,
        "holding a prepared page should keep its byte reservation visible until the consumer releases it"
    );

    while let Ok(LexicalRebuildPipelineMessage::Batch(batch)) =
        rx.recv_timeout(Duration::from_millis(10))
    {
        held_batches.push(batch);
        assert!(
            flow_limiter.bytes_in_flight() <= flow_limiter.max_bytes_in_flight(),
            "producer must keep prepared-page reservations inside the configured byte budget"
        );
    }

    for batch in &held_batches {
        release_lexical_rebuild_prepared_page_reservation(batch, flow_limiter.as_ref());
    }

    loop {
        match rx.recv_timeout(Duration::from_secs(10)).unwrap() {
            LexicalRebuildPipelineMessage::Batch(batch) => {
                release_lexical_rebuild_prepared_page_reservation(&batch, flow_limiter.as_ref());
            }
            LexicalRebuildPipelineMessage::Done => break,
            LexicalRebuildPipelineMessage::Error(error) => {
                panic!("producer returned error: {error}")
            }
        }
    }
    handle.join().unwrap();

    assert_eq!(
        flow_limiter.bytes_in_flight(),
        0,
        "consumer-owned releases should drain all reserved bytes after the burst completes"
    );
}
