use super::super::*;
use super::helpers::*;
use crate::franken_sync::compat::{ConnectionExt, ParamValue};
use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use serial_test::serial;
use tempfile::TempDir;

#[test]
fn lexical_rebuild_staged_merge_controller_restores_parallelism_after_producer_finishes() {
    let controller = LexicalRebuildStagedMergeController::new(3, Some(7_000));
    let fan_in = LexicalRebuildShardMergeCoordinator::EAGER_MERGE_FAN_IN;
    let ready_groups = (LEXICAL_REBUILD_FINAL_FRONTIER_FEDERATED_SHARD_LIMIT / fan_in) + 1;
    let merge_coordinator = LexicalRebuildShardMergeCoordinator {
        stage_root: PathBuf::from("/tmp/eager-merge"),
        ready_levels: vec![
            (0..fan_in * ready_groups)
                .map(|idx| LexicalRebuildShardMergeArtifact {
                    first_shard_index: idx,
                    last_shard_index: idx,
                    index_path: PathBuf::from(format!("/tmp/shard-{idx}")),
                    docs: 0,
                    segments: 0,
                })
                .collect(),
        ],
        next_output_seq_by_level: vec![0, 0],
        pending_merge_jobs: 1,
        allowed_pending_merge_jobs: 1,
    };
    let clear_runtime = LexicalRebuildPipelineRuntimeSnapshot {
        page_prep_workers: 6,
        host_loadavg_1m_milli: Some(4_500),
        ..LexicalRebuildPipelineRuntimeSnapshot::default()
    };

    let decision = controller.decide(true, &clear_runtime, &merge_coordinator);

    assert_eq!(decision.allowed_jobs, 3);
    assert_eq!(decision.active_jobs, 1);
    assert_eq!(decision.ready_artifacts, fan_in * ready_groups);
    assert_eq!(decision.ready_groups, ready_groups);
    assert_eq!(
        decision.controller_reason,
        "producer_finished_allowing_max_staged_merge_parallelism"
    );
}

#[test]
fn lexical_rebuild_staged_shard_build_controller_borrows_idle_merge_budget() {
    let controller = LexicalRebuildStagedShardBuildController::new(6, Some(7_000));
    let runtime = LexicalRebuildPipelineRuntimeSnapshot::default();
    let staged_merge_runtime = LexicalRebuildStagedMergeRuntimeSnapshot {
        workers_max: 3,
        allowed_jobs: 0,
        active_jobs: 0,
        ready_artifacts: 0,
        ready_groups: 0,
        controller_reason: "no_staged_merge_backlog".to_string(),
    };

    let decision = controller.decide(&runtime, &staged_merge_runtime, 2, 3, None);

    assert_eq!(decision.workers_max, 6);
    assert_eq!(decision.allowed_jobs, 5);
    assert_eq!(decision.active_jobs, 2);
    assert_eq!(decision.pending_jobs, 3);
    assert_eq!(
        decision.controller_reason,
        "staged_merge_idle_lending_full_budget_to_shard_builders"
    );
}

#[test]
fn lexical_rebuild_staged_shard_build_controller_reserves_slots_for_merge_backlog() {
    let controller = LexicalRebuildStagedShardBuildController::new(6, Some(7_000));
    let runtime = LexicalRebuildPipelineRuntimeSnapshot::default();
    let staged_merge_runtime = LexicalRebuildStagedMergeRuntimeSnapshot {
        workers_max: 3,
        allowed_jobs: 2,
        active_jobs: 1,
        ready_artifacts: 5,
        ready_groups: 1,
        controller_reason: "pipeline_active".to_string(),
    };

    let decision = controller.decide(&runtime, &staged_merge_runtime, 3, 3, None);

    assert_eq!(decision.workers_max, 6);
    assert_eq!(decision.allowed_jobs, 4);
    assert_eq!(decision.active_jobs, 3);
    assert_eq!(decision.pending_jobs, 3);
    assert_eq!(
        decision.controller_reason,
        "reserving_2_slots_for_staged_merge_active_jobs_1_ready_groups_1"
    );
}

#[test]
fn lexical_rebuild_staged_shard_build_controller_pauses_new_jobs_at_emergency_memory_reserve() {
    const GIB: usize = 1024 * 1024 * 1024;
    let controller = LexicalRebuildStagedShardBuildController::new_with_memory_reserves(
        6,
        None,
        16 * GIB,
        4 * GIB,
    );
    let runtime = LexicalRebuildPipelineRuntimeSnapshot {
        host_available_memory_bytes: Some(2 * GIB as u64),
        ..LexicalRebuildPipelineRuntimeSnapshot::default()
    };
    let staged_merge_runtime = LexicalRebuildStagedMergeRuntimeSnapshot::default();

    let decision = controller.decide(&runtime, &staged_merge_runtime, 2, 4, Some(64 << 20));

    assert_eq!(decision.allowed_jobs, 2);
    assert_eq!(decision.active_jobs, 2);
    assert_eq!(decision.pending_jobs, 4);
    assert_eq!(
        decision.controller_reason,
        format!(
            "host_available_memory_bytes_{}_below_emergency_reserve_{}_pausing_new_staged_shard_builds",
            2 * GIB,
            4 * GIB
        )
    );
}

#[test]
fn lexical_rebuild_staged_shard_build_controller_allows_one_idle_job_below_memory_reserve() {
    const GIB: usize = 1024 * 1024 * 1024;
    let controller = LexicalRebuildStagedShardBuildController::new_with_memory_reserves(
        6,
        None,
        16 * GIB,
        4 * GIB,
    );
    let runtime = LexicalRebuildPipelineRuntimeSnapshot {
        host_available_memory_bytes: Some(8 * GIB as u64),
        ..LexicalRebuildPipelineRuntimeSnapshot::default()
    };
    let staged_merge_runtime = LexicalRebuildStagedMergeRuntimeSnapshot::default();

    let decision = controller.decide(&runtime, &staged_merge_runtime, 0, 4, Some(64 << 20));

    assert_eq!(decision.allowed_jobs, 1);
    assert_eq!(
        decision.controller_reason,
        format!(
            "host_available_memory_bytes_{}_below_reserve_{}_limiting_staged_shard_builds_to_1",
            8 * GIB,
            16 * GIB
        )
    );
}

#[test]
fn lexical_rebuild_staged_shard_build_controller_allows_small_idle_probe_below_emergency() {
    const GIB: usize = 1024 * 1024 * 1024;
    const MIB: usize = 1024 * 1024;
    let controller =
        LexicalRebuildStagedShardBuildController::new_with_memory_reserves(6, None, 4 * GIB, GIB);
    let runtime = LexicalRebuildPipelineRuntimeSnapshot {
        host_available_memory_bytes: Some(768 * MIB as u64),
        ..LexicalRebuildPipelineRuntimeSnapshot::default()
    };
    let staged_merge_runtime = LexicalRebuildStagedMergeRuntimeSnapshot::default();

    let decision = controller.decide(&runtime, &staged_merge_runtime, 0, 4, Some(MIB));

    assert_eq!(decision.allowed_jobs, 1);
    assert_eq!(
        decision.controller_reason,
        format!(
            "host_available_memory_bytes_{}_below_emergency_reserve_{}_admitting_single_small_staged_shard_build_estimated_builder_bytes_{}",
            768 * MIB,
            GIB,
            512 * MIB
        )
    );
}

#[test]
fn staged_lexical_pipeline_handoff_pause_requires_async_wake_source() {
    assert!(
        !should_pause_staged_lexical_pipeline_handoff(false, 0, 0),
        "no backlog means the producer handoff should stay open"
    );
    assert!(
        !should_pause_staged_lexical_pipeline_handoff(true, 0, 0),
        "a saturated backlog with no active shard or merge job must keep draining pipeline_rx"
    );
    assert!(
        should_pause_staged_lexical_pipeline_handoff(true, 1, 0),
        "active shard builders can wake the sink, so backlog pause is safe"
    );
    assert!(
        should_pause_staged_lexical_pipeline_handoff(true, 0, 1),
        "active merge jobs can wake the sink, so backlog pause is safe"
    );
}

#[test]
fn lexical_rebuild_staged_shard_build_controller_caps_override_fanout_by_memory_headroom() {
    const GIB: usize = 1024 * 1024 * 1024;
    let controller =
        LexicalRebuildStagedShardBuildController::new_with_memory_reserves(8, None, 4 * GIB, GIB);
    let runtime = LexicalRebuildPipelineRuntimeSnapshot {
        host_available_memory_bytes: Some(8 * GIB as u64),
        staged_shard_build_observed_amplification_milli: Some(24_000),
        ..LexicalRebuildPipelineRuntimeSnapshot::default()
    };
    let staged_merge_runtime = LexicalRebuildStagedMergeRuntimeSnapshot::default();

    let decision = controller.decide(&runtime, &staged_merge_runtime, 0, 8, Some(64 << 20));

    assert_eq!(decision.allowed_jobs, 1);
    assert_eq!(
        decision.controller_reason,
        format!(
            "host_available_memory_bytes_{}_reserve_{}_estimated_builder_bytes_{}_limiting_staged_shard_builds_to_1",
            8 * GIB,
            4 * GIB,
            36usize * (64usize << 20)
        )
    );
}

#[test]
fn lexical_rebuild_shard_build_telemetry_records_conservative_amplification() {
    let telemetry = LexicalRebuildShardBuildTelemetry::default();
    let result = LexicalRebuildShardBuildResult {
        shard: LexicalShardPlanShard {
            shard_index: 7,
            first_conversation_id: 10,
            last_conversation_id: 11,
            conversation_count: 2,
            message_count: 4,
            message_bytes: 1_000,
            conversation_id_fingerprint: "fingerprint".to_string(),
            oversized_single_conversation: false,
        },
        indexed_docs: 4,
        segments: 1,
        shard_index_path: PathBuf::from("/tmp/shard-7"),
        message_bytes: 1_000,
        index_size_bytes: 2_500,
        build_duration_ms: 123,
        amplification_milli: lexical_rebuild_amplification_milli(2_500, 1_000),
    };

    telemetry.record(&result);
    let snapshot = telemetry.snapshot();

    assert_eq!(snapshot.completed_jobs, 1);
    assert_eq!(snapshot.last_shard_index, Some(7));
    assert_eq!(snapshot.last_message_bytes, 1_000);
    assert_eq!(snapshot.last_index_size_bytes, 2_500);
    assert_eq!(snapshot.last_duration_ms, 123);
    assert_eq!(snapshot.last_amplification_milli, Some(2_500));
    assert_eq!(
        snapshot.observed_amplification_milli,
        Some(LEXICAL_REBUILD_STAGED_SHARD_BUILD_AMPLIFICATION_FLOOR_MILLI),
        "admission should use a conservative floor until observed amplification exceeds it"
    );
}

#[test]
#[serial]
fn lexical_rebuild_pipeline_settings_snapshot_honors_env_overrides() {
    let _responsiveness = set_env("CASS_RESPONSIVENESS_DISABLE", "1");
    let _workers = set_env("CASS_TANTIVY_REBUILD_WORKERS", "7");
    let _reserved_cores = set_env("CASS_TANTIVY_REBUILD_RESERVED_CORES", "4");
    let _controller_mode = set_env("CASS_TANTIVY_REBUILD_CONTROLLER_MODE", "steady");
    let _controller_clear_samples =
        set_env("CASS_TANTIVY_REBUILD_CONTROLLER_RESTORE_CLEAR_SAMPLES", "5");
    let _controller_hold_ms = set_env("CASS_TANTIVY_REBUILD_CONTROLLER_RESTORE_HOLD_MS", "2345");
    let _controller_loadavg_high = set_env(
        "CASS_TANTIVY_REBUILD_CONTROLLER_LOADAVG_HIGH_WATERMARK_1M",
        "7.5",
    );
    let _controller_loadavg_low = set_env(
        "CASS_TANTIVY_REBUILD_CONTROLLER_LOADAVG_LOW_WATERMARK_1M",
        "6.25",
    );
    let _steady_fetch = set_env("CASS_TANTIVY_REBUILD_BATCH_FETCH_CONVERSATIONS", "321");
    let _startup_fetch = set_env(
        "CASS_TANTIVY_REBUILD_INITIAL_BATCH_FETCH_CONVERSATIONS",
        "33",
    );
    let _steady_conversations = set_env("CASS_TANTIVY_REBUILD_COMMIT_EVERY_CONVERSATIONS", "654");
    let _startup_conversations = set_env(
        "CASS_TANTIVY_REBUILD_INITIAL_COMMIT_EVERY_CONVERSATIONS",
        "65",
    );
    let _steady_messages = set_env("CASS_TANTIVY_REBUILD_COMMIT_EVERY_MESSAGES", "987");
    let _startup_messages = set_env("CASS_TANTIVY_REBUILD_INITIAL_COMMIT_EVERY_MESSAGES", "98");
    let _steady_message_bytes =
        set_env("CASS_TANTIVY_REBUILD_COMMIT_EVERY_MESSAGE_BYTES", "123456");
    let _startup_message_bytes = set_env(
        "CASS_TANTIVY_REBUILD_INITIAL_COMMIT_EVERY_MESSAGE_BYTES",
        "12345",
    );
    let _pipeline_channel = set_env("CASS_TANTIVY_REBUILD_PIPELINE_CHANNEL_SIZE", "5");
    let _page_prep_workers = set_env("CASS_TANTIVY_REBUILD_PAGE_PREP_WORKERS", "3");
    let _pipeline_bytes = set_env(
        "CASS_TANTIVY_REBUILD_PIPELINE_MAX_MESSAGE_BYTES_IN_FLIGHT",
        "777777",
    );
    let _writer_threads = set_env("CASS_TANTIVY_MAX_WRITER_THREADS", "2");
    let _shard_builders = set_env("CASS_TANTIVY_REBUILD_STAGED_SHARD_BUILDERS", "4");
    let _merge_workers = set_env("CASS_TANTIVY_REBUILD_STAGED_MERGE_WORKERS", "2");

    let snapshot = lexical_rebuild_pipeline_settings_snapshot();

    assert_eq!(snapshot.workers, 7);
    assert!(snapshot.available_parallelism >= 1);
    assert_eq!(
        snapshot.reserved_cores,
        4.min(snapshot.available_parallelism.saturating_sub(1))
    );
    assert_eq!(
        snapshot.tantivy_writer_threads,
        snapshot.available_parallelism.min(2)
    );
    assert_eq!(
        snapshot.staged_shard_builders,
        responsiveness::effective_worker_count(4).max(1)
    );
    assert_eq!(
        snapshot.staged_merge_workers,
        responsiveness::effective_worker_count(2).max(1)
    );
    assert_eq!(snapshot.controller_mode, "steady");
    assert_eq!(snapshot.controller_restore_clear_samples, 5);
    assert_eq!(snapshot.controller_restore_hold_ms, 2345);
    assert_eq!(
        snapshot.controller_loadavg_high_watermark_1m_milli,
        Some(7_500)
    );
    assert_eq!(
        snapshot.controller_loadavg_low_watermark_1m_milli,
        Some(6_250)
    );
    assert_eq!(snapshot.page_size, LEXICAL_REBUILD_PAGE_SIZE);
    assert_eq!(snapshot.steady_batch_fetch_conversations, 321);
    assert_eq!(snapshot.startup_batch_fetch_conversations, 33);
    assert_eq!(snapshot.steady_commit_every_conversations, 654);
    assert_eq!(snapshot.startup_commit_every_conversations, 65);
    assert_eq!(snapshot.steady_commit_every_messages, 987);
    assert_eq!(snapshot.startup_commit_every_messages, 98);
    assert_eq!(snapshot.steady_commit_every_message_bytes, 123456);
    assert_eq!(snapshot.startup_commit_every_message_bytes, 12345);
    assert_eq!(snapshot.pipeline_channel_size, 5);
    assert_eq!(
        snapshot.page_prep_workers,
        responsiveness::effective_worker_count(3).max(1)
    );
    assert_eq!(snapshot.pipeline_max_message_bytes_in_flight, 777777);
}

/// GH #483: an explicit positive environment value beats the profile, the
/// profile beats the computed default, and a knob the profile does not
/// name keeps its default.
#[test]
fn rebuild_profile_settings_resolve_env_over_profile_over_default() {
    use LexicalRebuildProfile::{Bounded, Default as HostSized};
    use LexicalRebuildSettingSource::{Env, Profile};
    const WORKERS: &str = "CASS_TANTIVY_REBUILD_WORKERS";
    assert_eq!(
        resolve_lexical_rebuild_setting(None, Bounded, WORKERS),
        Some((1, Profile))
    );
    assert_eq!(
        resolve_lexical_rebuild_setting(None, HostSized, WORKERS),
        None
    );
    for profile in [Bounded, HostSized] {
        assert_eq!(
            resolve_lexical_rebuild_setting(Some("3"), profile, WORKERS),
            Some((3, Env))
        );
    }
    // Values the readers always ignored stay ignored; the profile fills in.
    for ignored in ["0", "abc", "-2", ""] {
        assert_eq!(
            resolve_lexical_rebuild_setting(Some(ignored), Bounded, WORKERS),
            Some((1, Profile)),
            "{ignored:?}"
        );
        assert_eq!(
            resolve_lexical_rebuild_setting(Some(ignored), HostSized, WORKERS),
            None
        );
    }
    assert_eq!(
        resolve_lexical_rebuild_setting(
            None,
            Bounded,
            "CASS_TANTIVY_REBUILD_COMMIT_EVERY_MESSAGES"
        ),
        None
    );
    assert!(HostSized.settings().is_empty());
    assert_eq!(
        Bounded.settings(),
        &[
            ("CASS_TANTIVY_REBUILD_WORKERS", 1),
            ("CASS_TANTIVY_REBUILD_PAGE_PREP_WORKERS", 1),
            ("CASS_TANTIVY_REBUILD_PIPELINE_CHANNEL_SIZE", 1),
            ("CASS_TANTIVY_REBUILD_BATCH_FETCH_CONVERSATIONS", 64),
            ("CASS_TANTIVY_REBUILD_COMMIT_EVERY_MESSAGE_BYTES", 64 << 20),
        ]
    );
}

struct RebuildProfileGuard;

impl Drop for RebuildProfileGuard {
    fn drop(&mut self) {
        set_lexical_rebuild_profile(LexicalRebuildProfile::Default);
    }
}

/// GH #483: the bounded profile reaches every reader the live rebuild
/// uses, including the settings derived from the worker count, and the
/// `cass index --json` report names where each value came from.
#[test]
#[serial]
fn bounded_rebuild_profile_drives_the_live_settings_and_its_report() {
    let _responsiveness = set_env("CASS_RESPONSIVENESS_DISABLE", "1");
    let _cleared: Vec<EnvGuard> = [
        "CASS_TANTIVY_REBUILD_WORKERS",
        "CASS_TANTIVY_REBUILD_PAGE_PREP_WORKERS",
        "CASS_TANTIVY_REBUILD_PIPELINE_CHANNEL_SIZE",
        "CASS_TANTIVY_REBUILD_BATCH_FETCH_CONVERSATIONS",
        "CASS_TANTIVY_REBUILD_INITIAL_BATCH_FETCH_CONVERSATIONS",
        "CASS_TANTIVY_REBUILD_COMMIT_EVERY_MESSAGE_BYTES",
        "CASS_TANTIVY_REBUILD_INITIAL_COMMIT_EVERY_MESSAGE_BYTES",
        "CASS_TANTIVY_REBUILD_PIPELINE_MAX_MESSAGE_BYTES_IN_FLIGHT",
        "CASS_TANTIVY_REBUILD_STAGED_SHARD_BUILDERS",
        "CASS_TANTIVY_REBUILD_STAGED_MERGE_WORKERS",
    ]
    .into_iter()
    .map(unset_env_var)
    .collect();
    let _profile = RebuildProfileGuard;

    let default = lexical_rebuild_pipeline_settings_snapshot();
    assert_eq!(
        default.workers,
        lexical_rebuild_configured_worker_parallelism_for_available(
            lexical_rebuild_available_parallelism()
        ),
        "the default profile keeps host-sized workers"
    );
    assert_eq!(default.pipeline_channel_size, 4);

    set_lexical_rebuild_profile(LexicalRebuildProfile::Bounded);
    let bounded = lexical_rebuild_pipeline_settings_snapshot();
    assert_eq!(bounded.workers, 1);
    assert_eq!(
        bounded.page_prep_workers,
        responsiveness::effective_worker_count(1).max(1)
    );
    assert_eq!(bounded.pipeline_channel_size, 1);
    assert_eq!(bounded.steady_batch_fetch_conversations, 64);
    assert_eq!(bounded.startup_batch_fetch_conversations, 32);
    assert_eq!(bounded.steady_commit_every_message_bytes, 64 << 20);
    assert_eq!(bounded.startup_commit_every_message_bytes, 64 << 20);
    assert_eq!(
        bounded.pipeline_max_message_bytes_in_flight,
        responsiveness::effective_inflight_byte_limit(2 * (64 << 20)).max(1),
        "one page in the channel plus one at the sink"
    );
    assert_eq!(
        lexical_rebuild_staged_shard_builder_parallelism_configured(),
        lexical_rebuild_default_staged_shard_builder_parallelism_for_workers(1)
    );
    assert_eq!(
        lexical_rebuild_staged_merge_worker_parallelism_configured(),
        1
    );

    let report = lexical_rebuild_profile_report();
    assert_eq!(report["name"], "bounded");
    assert_eq!(report["settings"].as_array().map(Vec::len), Some(5));
    assert!(
        report["settings"]
            .as_array()
            .unwrap()
            .iter()
            .all(|setting| setting["source"] == "profile"),
        "{report}"
    );
    assert_eq!(report["effective"]["workers"], 1);
    assert_eq!(report["effective"]["pipeline_channel_size"], 1);

    // An explicit operator value still wins under the profile.
    let _workers = set_env("CASS_TANTIVY_REBUILD_WORKERS", "2");
    assert_eq!(lexical_rebuild_pipeline_settings_snapshot().workers, 2);
    let report = lexical_rebuild_profile_report();
    let workers = report["settings"]
        .as_array()
        .unwrap()
        .iter()
        .find(|setting| setting["env"] == "CASS_TANTIVY_REBUILD_WORKERS")
        .unwrap();
    assert_eq!(workers["value"], 2);
    assert_eq!(workers["source"], "env");
    assert_eq!(report["effective"]["workers"], 2);
    assert_eq!(report["effective"]["pipeline_channel_size"], 1);

    set_lexical_rebuild_profile(LexicalRebuildProfile::Default);
    let report = lexical_rebuild_profile_report();
    assert_eq!(report["name"], "default");
    assert_eq!(report["settings"], serde_json::json!([]));
}

#[test]
#[serial]
fn lexical_rebuild_pipeline_settings_snapshot_disables_global_controller() {
    let _responsiveness = set_env("CASS_RESPONSIVENESS_DISABLE", "1");
    let _controller_mode = unset_env_var("CASS_TANTIVY_REBUILD_CONTROLLER_MODE");
    let _controller_loadavg_high =
        unset_env_var("CASS_TANTIVY_REBUILD_CONTROLLER_LOADAVG_HIGH_WATERMARK_1M");
    let _controller_loadavg_low =
        unset_env_var("CASS_TANTIVY_REBUILD_CONTROLLER_LOADAVG_LOW_WATERMARK_1M");

    let snapshot = lexical_rebuild_pipeline_settings_snapshot();

    assert_eq!(snapshot.controller_mode, "steady");
    assert_eq!(snapshot.controller_loadavg_high_watermark_1m_milli, None);
    assert_eq!(snapshot.controller_loadavg_low_watermark_1m_milli, None);
}

#[test]
fn lexical_rebuild_default_worker_parallelism_reserves_machine_headroom() {
    assert_eq!(lexical_rebuild_default_reserved_cores_for_available(1), 0);
    assert_eq!(
        lexical_rebuild_default_worker_parallelism_for_available(1),
        1
    );
    assert_eq!(lexical_rebuild_default_reserved_cores_for_available(4), 1);
    assert_eq!(
        lexical_rebuild_default_worker_parallelism_for_available(4),
        3
    );
    assert_eq!(lexical_rebuild_default_reserved_cores_for_available(8), 2);
    assert_eq!(
        lexical_rebuild_default_worker_parallelism_for_available(8),
        6
    );
    assert_eq!(lexical_rebuild_default_reserved_cores_for_available(32), 4);
    assert_eq!(
        lexical_rebuild_default_worker_parallelism_for_available(32),
        28
    );
    assert_eq!(lexical_rebuild_default_reserved_cores_for_available(128), 8);
    assert_eq!(
        lexical_rebuild_default_worker_parallelism_for_available(128),
        64
    );
}

#[test]
fn lexical_rebuild_default_batch_fetch_conversation_limit_scales_with_writer_parallelism() {
    assert_eq!(
        lexical_rebuild_default_batch_fetch_conversation_limit(1024, 1),
        512
    );
    assert_eq!(
        lexical_rebuild_default_batch_fetch_conversation_limit(1024, 4),
        512
    );
    assert_eq!(
        lexical_rebuild_default_batch_fetch_conversation_limit(1024, 8),
        1024
    );
    assert_eq!(
        lexical_rebuild_default_batch_fetch_conversation_limit(1024, 16),
        1024
    );
    assert_eq!(
        lexical_rebuild_default_batch_fetch_conversation_limit(256, 16),
        256
    );
}

#[test]
fn lexical_rebuild_default_staged_merge_worker_parallelism_tracks_quarter_budget() {
    assert_eq!(
        lexical_rebuild_default_staged_merge_worker_parallelism_for_workers(1),
        1
    );
    assert_eq!(
        lexical_rebuild_default_staged_merge_worker_parallelism_for_workers(4),
        1
    );
    assert_eq!(
        lexical_rebuild_default_staged_merge_worker_parallelism_for_workers(8),
        2
    );
    assert_eq!(
        lexical_rebuild_default_staged_merge_worker_parallelism_for_workers(12),
        3
    );
    assert_eq!(
        lexical_rebuild_default_staged_merge_worker_parallelism_for_workers(32),
        8
    );
    assert_eq!(
        lexical_rebuild_default_staged_merge_worker_parallelism_for_workers(64),
        8
    );
}

#[test]
fn lexical_rebuild_final_frontier_reduction_only_runs_above_federated_publish_cap() {
    assert!(!should_reduce_staged_lexical_final_frontier(0));
    assert!(!should_reduce_staged_lexical_final_frontier(
        LexicalRebuildShardMergeCoordinator::EAGER_MERGE_FAN_IN + 1
    ));
    assert!(!should_reduce_staged_lexical_final_frontier(
        LEXICAL_REBUILD_FINAL_FRONTIER_FEDERATED_SHARD_LIMIT
    ));
    assert!(should_reduce_staged_lexical_final_frontier(
        LEXICAL_REBUILD_FINAL_FRONTIER_FEDERATED_SHARD_LIMIT + 1
    ));
}

#[test]
fn staged_merge_controller_skips_finished_tail_merges_within_federated_publish_cap() {
    let tmp = TempDir::new().unwrap();
    let (merge_work_tx, _merge_work_rx) = bounded::<LexicalRebuildShardMergeJob>(1);
    let mut merge_coordinator =
        LexicalRebuildShardMergeCoordinator::new(tmp.path().join("eager-merge-stage"));
    for shard_index in 0..LexicalRebuildShardMergeCoordinator::EAGER_MERGE_FAN_IN {
        merge_coordinator
            .queue_base_artifact(
                LexicalRebuildShardMergeArtifact {
                    first_shard_index: shard_index,
                    last_shard_index: shard_index,
                    index_path: tmp.path().join(format!("shard-{shard_index:05}")),
                    docs: 1,
                    segments: 1,
                },
                &merge_work_tx,
            )
            .unwrap();
    }

    let controller = LexicalRebuildStagedMergeController::new(8, None);
    let decision = controller.decide(
        true,
        &LexicalRebuildPipelineRuntimeSnapshot::default(),
        &merge_coordinator,
    );

    assert_eq!(decision.ready_artifacts, 8);
    assert_eq!(decision.ready_groups, 1);
    assert_eq!(
        decision.allowed_jobs, 0,
        "a bounded final frontier should publish federated instead of paying another eager merge"
    );
    assert!(
        decision
            .controller_reason
            .contains("final_frontier_within_federated_cap"),
        "unexpected controller reason: {}",
        decision.controller_reason
    );
}

#[test]
fn lexical_rebuild_default_staged_shard_builder_parallelism_uses_bounded_builder_farm() {
    const GIB: u64 = 1024 * 1024 * 1024;

    assert_eq!(
        lexical_rebuild_default_staged_shard_builder_parallelism_for_workers_and_memory(1, None,),
        1
    );
    assert_eq!(
        lexical_rebuild_default_staged_shard_builder_parallelism_for_workers_and_memory(4, None,),
        4
    );
    assert_eq!(
        lexical_rebuild_default_staged_shard_builder_parallelism_for_workers_and_memory(
            8,
            Some(128 * GIB),
        ),
        2,
        "128 GiB hosts should not default to an 8-shard Tantivy build storm"
    );
    assert_eq!(
        lexical_rebuild_default_staged_shard_builder_parallelism_for_workers_and_memory(
            32,
            Some(512 * GIB),
        ),
        8
    );
}

#[test]
fn lexical_rebuild_default_staged_shard_max_message_bytes_scales_with_available_memory() {
    const GIB: u64 = 1024 * 1024 * 1024;

    assert_eq!(
        lexical_rebuild_default_staged_shard_max_message_bytes_for_available_memory(None),
        64 * 1024 * 1024
    );
    assert_eq!(
        lexical_rebuild_default_staged_shard_max_message_bytes_for_available_memory(Some(32 * GIB)),
        16 * 1024 * 1024
    );
    assert_eq!(
        lexical_rebuild_default_staged_shard_max_message_bytes_for_available_memory(Some(
            128 * GIB
        )),
        64 * 1024 * 1024
    );
    assert_eq!(
        lexical_rebuild_default_staged_shard_max_message_bytes_for_available_memory(Some(
            512 * GIB
        )),
        128 * 1024 * 1024
    );
}

#[test]
#[serial]
fn lexical_rebuild_shard_planner_respects_staged_shard_byte_cap() {
    let _cap = set_env(
        "CASS_TANTIVY_REBUILD_STAGED_SHARD_MAX_MESSAGE_BYTES",
        "65536",
    );
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
        steady_commit_every_message_bytes: 512 * 1024 * 1024,
        startup_commit_every_message_bytes: 128 * 1024 * 1024,
        pipeline_channel_size: 2,
        page_prep_workers: 6,
        pipeline_max_message_bytes_in_flight: 4 * 1024 * 1024,
    };

    let budgets = lexical_rebuild_default_shard_planner_budgets_for_totals(
        &settings,
        10_000,
        1_000_000,
        4usize * 1024 * 1024 * 1024,
    );

    assert_eq!(budgets.max_message_bytes_per_shard, 65_536);
}

#[test]
#[serial]
fn lexical_rebuild_pending_shard_build_job_cap_scales_with_builders_and_env() {
    let _invalid_override = set_env("CASS_TANTIVY_REBUILD_PENDING_SHARD_BUILD_MAX_JOBS", "0");
    let mut settings = LexicalRebuildPipelineSettingsSnapshot {
        workers: 8,
        available_parallelism: 8,
        reserved_cores: 2,
        tantivy_writer_threads: 8,
        staged_shard_builders: 2,
        staged_merge_workers: 1,
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
        steady_commit_every_message_bytes: 512 * 1024 * 1024,
        startup_commit_every_message_bytes: 128 * 1024 * 1024,
        pipeline_channel_size: 2,
        page_prep_workers: 6,
        pipeline_max_message_bytes_in_flight: 4 * 1024 * 1024,
    };

    assert_eq!(lexical_rebuild_pending_shard_build_max_jobs(&settings), 64);
    settings.staged_shard_builders = 16;
    assert_eq!(lexical_rebuild_pending_shard_build_max_jobs(&settings), 256);
    drop(_invalid_override);

    let _explicit_override = set_env("CASS_TANTIVY_REBUILD_PENDING_SHARD_BUILD_MAX_JOBS", "17");
    assert_eq!(lexical_rebuild_pending_shard_build_max_jobs(&settings), 17);
}

#[test]
#[serial]
fn lexical_rebuild_large_corpus_planner_keeps_5m_message_shards_byte_bounded() {
    const CONVERSATIONS: usize = 56_512;
    const MESSAGES: usize = 5_347_311;
    const MESSAGE_BYTES: usize = 8 * 1024 * 1024 * 1024;
    const SHARD_CAP: usize = 64 * 1024 * 1024;
    let _cap = set_env(
        "CASS_TANTIVY_REBUILD_STAGED_SHARD_MAX_MESSAGE_BYTES",
        &SHARD_CAP.to_string(),
    );
    let settings = LexicalRebuildPipelineSettingsSnapshot {
        workers: 16,
        available_parallelism: 16,
        reserved_cores: 2,
        tantivy_writer_threads: 8,
        staged_shard_builders: 2,
        staged_merge_workers: 2,
        controller_mode: "steady".into(),
        controller_restore_clear_samples: 3,
        controller_restore_hold_ms: 0,
        controller_loadavg_high_watermark_1m_milli: None,
        controller_loadavg_low_watermark_1m_milli: None,
        page_size: LEXICAL_REBUILD_PAGE_SIZE,
        steady_batch_fetch_conversations: 1024,
        startup_batch_fetch_conversations: 256,
        steady_commit_every_conversations: 10_000,
        startup_commit_every_conversations: 2_048,
        steady_commit_every_messages: 800_000,
        startup_commit_every_messages: 800_000,
        steady_commit_every_message_bytes: 512 * 1024 * 1024,
        startup_commit_every_message_bytes: 128 * 1024 * 1024,
        pipeline_channel_size: 4,
        page_prep_workers: 8,
        pipeline_max_message_bytes_in_flight: 512 * 1024 * 1024,
    };
    let budgets = lexical_rebuild_default_shard_planner_budgets_for_totals(
        &settings,
        CONVERSATIONS,
        MESSAGES,
        MESSAGE_BYTES,
    );
    let mut conversations = Vec::with_capacity(CONVERSATIONS);
    for idx in 0..CONVERSATIONS {
        let message_count = MESSAGES / CONVERSATIONS + usize::from(idx < MESSAGES % CONVERSATIONS);
        let message_bytes =
            MESSAGE_BYTES / CONVERSATIONS + usize::from(idx < MESSAGE_BYTES % CONVERSATIONS);
        conversations.push(LexicalShardPlannerConversation {
            conversation_id: i64::try_from(idx + 1).unwrap(),
            message_count,
            message_bytes,
        });
    }

    let plan = plan_lexical_rebuild_shards(&conversations, budgets);

    assert_eq!(budgets.max_message_bytes_per_shard, SHARD_CAP);
    assert!(
        plan.shards.len() >= MESSAGE_BYTES.div_ceil(SHARD_CAP),
        "large corpus should be split into enough shards to keep Tantivy builder heaps bounded"
    );
    assert!(
        plan.shards
            .iter()
            .all(|shard| shard.message_bytes <= SHARD_CAP),
        "no synthetic 5M-message shard should exceed the staged shard byte cap"
    );
}

#[test]
fn lexical_rebuild_staged_shard_builder_settings_preserve_total_writer_budget() {
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
        lexical_rebuild_staged_shard_builder_settings(&settings, 3),
        LexicalRebuildShardBuilderSettings {
            max_builders: 3,
            writer_parallelism_budget: 8,
        }
    );
    assert_eq!(
        lexical_rebuild_staged_shard_builder_settings(&settings, 32),
        LexicalRebuildShardBuilderSettings {
            max_builders: 8,
            writer_parallelism_budget: 8,
        }
    );

    let constrained_writer_budget = LexicalRebuildPipelineSettingsSnapshot {
        tantivy_writer_threads: 4,
        ..settings
    };
    assert_eq!(
        lexical_rebuild_staged_shard_builder_settings(&constrained_writer_budget, 32),
        LexicalRebuildShardBuilderSettings {
            max_builders: 4,
            writer_parallelism_budget: 4,
        }
    );
}

#[test]
fn lexical_rebuild_staged_shard_builder_dispatch_writer_parallelism_rebalances_budget() {
    let balanced = (0..6)
        .map(|slot| {
            lexical_rebuild_staged_shard_builder_writer_parallelism_for_dispatch(8, 6, slot)
        })
        .collect::<Vec<_>>();
    assert_eq!(balanced, vec![2, 2, 1, 1, 1, 1]);
    assert_eq!(balanced.iter().sum::<usize>(), 8);

    let widened = (0..2)
        .map(|slot| {
            lexical_rebuild_staged_shard_builder_writer_parallelism_for_dispatch(8, 2, slot)
        })
        .collect::<Vec<_>>();
    assert_eq!(widened, vec![4, 4]);
}

#[test]
fn lexical_shard_plan_is_deterministic_across_input_order() {
    let budgets = LexicalShardPlannerBudgets {
        max_conversations_per_shard: 2,
        max_messages_per_shard: 20,
        max_message_bytes_per_shard: 2_000,
    };
    let ordered = vec![
        LexicalShardPlannerConversation {
            conversation_id: 10,
            message_count: 4,
            message_bytes: 400,
        },
        LexicalShardPlannerConversation {
            conversation_id: 20,
            message_count: 5,
            message_bytes: 500,
        },
        LexicalShardPlannerConversation {
            conversation_id: 30,
            message_count: 6,
            message_bytes: 600,
        },
    ];
    let permuted = vec![ordered[2], ordered[0], ordered[1]];

    let ordered_plan = plan_lexical_rebuild_shards(&ordered, budgets);
    let permuted_plan = plan_lexical_rebuild_shards(&permuted, budgets);

    assert_eq!(ordered_plan, permuted_plan);
    assert_eq!(ordered_plan.shards.len(), 2);
    assert_eq!(ordered_plan.shards[0].first_conversation_id, 10);
    assert_eq!(ordered_plan.shards[0].last_conversation_id, 20);
    assert_eq!(ordered_plan.shards[1].first_conversation_id, 30);
    assert_eq!(ordered_plan.shards[1].last_conversation_id, 30);
}

#[test]
fn lexical_shard_plan_id_changes_when_sparse_assignments_change() {
    let budgets = LexicalShardPlannerBudgets {
        max_conversations_per_shard: 3,
        max_messages_per_shard: 20,
        max_message_bytes_per_shard: 2_000,
    };
    let plan_a = plan_lexical_rebuild_shards(
        &[
            LexicalShardPlannerConversation {
                conversation_id: 1,
                message_count: 2,
                message_bytes: 200,
            },
            LexicalShardPlannerConversation {
                conversation_id: 2,
                message_count: 2,
                message_bytes: 200,
            },
            LexicalShardPlannerConversation {
                conversation_id: 4,
                message_count: 2,
                message_bytes: 200,
            },
        ],
        budgets,
    );
    let plan_b = plan_lexical_rebuild_shards(
        &[
            LexicalShardPlannerConversation {
                conversation_id: 1,
                message_count: 2,
                message_bytes: 200,
            },
            LexicalShardPlannerConversation {
                conversation_id: 3,
                message_count: 2,
                message_bytes: 200,
            },
            LexicalShardPlannerConversation {
                conversation_id: 4,
                message_count: 2,
                message_bytes: 200,
            },
        ],
        budgets,
    );

    assert_eq!(plan_a.shards.len(), 1);
    assert_eq!(plan_b.shards.len(), 1);
    assert_eq!(plan_a.shards[0].first_conversation_id, 1);
    assert_eq!(plan_b.shards[0].first_conversation_id, 1);
    assert_eq!(plan_a.shards[0].last_conversation_id, 4);
    assert_eq!(plan_b.shards[0].last_conversation_id, 4);
    assert_eq!(plan_a.shards[0].conversation_count, 3);
    assert_eq!(plan_b.shards[0].conversation_count, 3);
    assert_ne!(
        plan_a.shards[0].conversation_id_fingerprint, plan_b.shards[0].conversation_id_fingerprint,
        "sparse shard assignments with the same range and totals need distinct shard evidence"
    );
    assert_ne!(
        plan_a.plan_id, plan_b.plan_id,
        "plan identity must include interior conversation IDs, not only shard ranges"
    );
}

#[test]
fn lexical_shard_plan_isolates_oversized_single_conversation() {
    let budgets = LexicalShardPlannerBudgets {
        max_conversations_per_shard: 3,
        max_messages_per_shard: 10,
        max_message_bytes_per_shard: 1_000,
    };
    let plan = plan_lexical_rebuild_shards(
        &[
            LexicalShardPlannerConversation {
                conversation_id: 1,
                message_count: 3,
                message_bytes: 300,
            },
            LexicalShardPlannerConversation {
                conversation_id: 2,
                message_count: 50,
                message_bytes: 5_000,
            },
            LexicalShardPlannerConversation {
                conversation_id: 3,
                message_count: 2,
                message_bytes: 200,
            },
        ],
        budgets,
    );

    assert_eq!(plan.oversized_conversation_ids, vec![2]);
    assert_eq!(plan.shards.len(), 3);
    assert!(!plan.shards[0].oversized_single_conversation);
    assert!(plan.shards[1].oversized_single_conversation);
    assert_eq!(plan.shards[1].first_conversation_id, 2);
    assert_eq!(plan.shards[1].last_conversation_id, 2);
    assert_eq!(plan.shards[2].first_conversation_id, 3);
    assert_eq!(plan.shards[2].last_conversation_id, 3);
}

#[test]
fn lexical_rebuild_target_shard_count_scales_with_parallelism() {
    assert_eq!(lexical_rebuild_target_shard_count(1, 1), 4);
    assert_eq!(lexical_rebuild_target_shard_count(8, 4), 32);
    assert_eq!(lexical_rebuild_target_shard_count(4, 12), 48);
    assert_eq!(lexical_rebuild_target_shard_count(128, 64), 256);
}

#[test]
fn lexical_rebuild_default_shard_budget_respects_parallel_targets_and_caps() {
    assert_eq!(
        lexical_rebuild_default_shard_budget(48_000, 48, 512, 10_000),
        1_000
    );
    assert_eq!(
        lexical_rebuild_default_shard_budget(4_000, 48, 512, 10_000),
        512
    );
    assert_eq!(
        lexical_rebuild_default_shard_budget(5_000_000, 16, 32_000, 250_000),
        250_000
    );
}

#[test]
fn lexical_rebuild_shard_planner_conversations_from_storage_uses_estimated_byte_footprints() {
    let temp = TempDir::new().unwrap();
    let db_path = temp.path().join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);

    let agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: Some("0.2.3".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();

    let insert = |external_id: &str, base_ts: i64, messages: Vec<Message>| {
        storage
            .insert_conversation_tree(
                agent_id,
                None,
                &Conversation {
                    id: None,
                    agent_slug: "codex".into(),
                    workspace: Some(PathBuf::from("/tmp/workspace")),
                    external_id: Some(external_id.to_string()),
                    title: Some(external_id.to_string()),
                    source_path: PathBuf::from(format!("/tmp/{external_id}.jsonl")),
                    started_at: Some(base_ts),
                    ended_at: Some(base_ts + 100),
                    approx_tokens: Some(64),
                    metadata_json: serde_json::Value::Null,
                    messages,
                    source_id: LOCAL_SOURCE_ID.into(),
                    origin_host: None,
                },
            )
            .unwrap()
            .conversation_id
    };

    let ascii_id = insert(
        "planner-ascii",
        1_700_000_000_000,
        vec![
            Message {
                id: None,
                idx: 0,
                role: MessageRole::User,
                author: None,
                created_at: Some(1_700_000_000_010),
                content: "abc".into(),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            },
            Message {
                id: None,
                idx: 1,
                role: MessageRole::Agent,
                author: None,
                created_at: Some(1_700_000_000_020),
                content: "defg".into(),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            },
        ],
    );
    let empty_id = insert("planner-empty", 1_700_000_001_000, Vec::new());
    let utf8_id = insert(
        "planner-utf8",
        1_700_000_002_000,
        vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::Tool,
            author: None,
            created_at: Some(1_700_000_002_010),
            content: "hé🙂".into(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
    );

    let conversations = lexical_rebuild_shard_planner_conversations_from_storage(&storage).unwrap();
    assert_eq!(
        conversations,
        vec![
            LexicalShardPlannerConversation {
                conversation_id: ascii_id,
                message_count: 2,
                message_bytes: 2
                    * crate::storage::sqlite::LEXICAL_REBUILD_PLANNER_ESTIMATED_BYTES_PER_MESSAGE,
            },
            LexicalShardPlannerConversation {
                conversation_id: empty_id,
                message_count: 0,
                message_bytes: 0,
            },
            LexicalShardPlannerConversation {
                conversation_id: utf8_id,
                message_count: 1,
                message_bytes:
                    crate::storage::sqlite::LEXICAL_REBUILD_PLANNER_ESTIMATED_BYTES_PER_MESSAGE,
            },
        ]
    );
}

#[test]
fn lexical_rebuild_shard_plan_from_storage_uses_message_footprints() {
    let temp = TempDir::new().unwrap();
    let db_path = temp.path().join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);

    let agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: Some("0.2.3".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();

    for conversation_idx in 0..3 {
        let external_id = format!("footprint-plan-{conversation_idx}");
        let messages = (0..4)
            .map(|message_idx| Message {
                id: None,
                idx: message_idx,
                role: MessageRole::User,
                author: None,
                created_at: Some(1_700_000_000_000 + message_idx),
                content: format!("{external_id}-{message_idx}"),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            })
            .collect::<Vec<_>>();
        storage
            .insert_conversation_tree(
                agent_id,
                None,
                &Conversation {
                    id: None,
                    agent_slug: "codex".into(),
                    workspace: Some(PathBuf::from("/tmp/workspace")),
                    external_id: Some(external_id.clone()),
                    title: Some(external_id.clone()),
                    source_path: PathBuf::from(format!("/tmp/{external_id}.jsonl")),
                    started_at: Some(1_700_000_000_000),
                    ended_at: Some(1_700_000_000_100),
                    approx_tokens: Some(64),
                    metadata_json: serde_json::Value::Null,
                    messages,
                    source_id: LOCAL_SOURCE_ID.into(),
                    origin_host: None,
                },
            )
            .unwrap();
    }

    let settings = LexicalRebuildPipelineSettingsSnapshot {
        workers: 1,
        available_parallelism: 1,
        reserved_cores: 0,
        tantivy_writer_threads: 1,
        staged_shard_builders: 1,
        staged_merge_workers: 1,
        controller_mode: "steady".into(),
        controller_restore_clear_samples: 1,
        controller_restore_hold_ms: 0,
        controller_loadavg_high_watermark_1m_milli: None,
        controller_loadavg_low_watermark_1m_milli: None,
        page_size: LEXICAL_REBUILD_PAGE_SIZE,
        steady_batch_fetch_conversations: 10,
        startup_batch_fetch_conversations: 10,
        steady_commit_every_conversations: 10,
        startup_commit_every_conversations: 10,
        steady_commit_every_messages: 5,
        startup_commit_every_messages: 5,
        steady_commit_every_message_bytes: 1024 * 1024,
        startup_commit_every_message_bytes: 1024 * 1024,
        pipeline_channel_size: 2,
        page_prep_workers: 1,
        pipeline_max_message_bytes_in_flight: 2 * 1024 * 1024,
    };

    let plan =
        plan_lexical_rebuild_shards_from_storage_with_settings(&storage, &settings, 3).unwrap();

    assert_eq!(plan.total_conversations, 3);
    assert_eq!(plan.total_messages, 12);
    assert_eq!(plan.shards.len(), 3);
    assert_eq!(
        plan.shards
            .iter()
            .map(|shard| shard.message_count)
            .collect::<Vec<_>>(),
        vec![
            LEXICAL_SHARD_UNKNOWN_MESSAGE_COUNT,
            LEXICAL_SHARD_UNKNOWN_MESSAGE_COUNT,
            LEXICAL_SHARD_UNKNOWN_MESSAGE_COUNT
        ]
    );
    assert_eq!(
        plan.shards
            .iter()
            .map(|shard| shard.conversation_count)
            .collect::<Vec<_>>(),
        vec![1, 1, 1]
    );
}

#[test]
fn lexical_rebuild_shard_plan_without_tail_metadata_uses_conservative_id_only_plan() {
    let temp = TempDir::new().unwrap();
    let db_path = temp.path().join("legacy-canonical.db");
    let db_path_str = db_path.to_string_lossy().into_owned();
    let conn = crate::franken_sync::Connection::open(db_path_str).unwrap();
    conn.execute_compat(
        "CREATE TABLE conversations (id INTEGER PRIMARY KEY, source_path TEXT NOT NULL)",
        &[] as &[ParamValue],
    )
    .unwrap();
    conn.execute_compat(
        "CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            conversation_id INTEGER NOT NULL,
            idx INTEGER NOT NULL,
            role TEXT NOT NULL,
            author TEXT,
            created_at INTEGER,
            content TEXT NOT NULL,
            UNIQUE(conversation_id, idx)
        )",
        &[] as &[ParamValue],
    )
    .unwrap();
    conn.execute_compat(
        "CREATE INDEX idx_messages_conv_idx ON messages(conversation_id, idx)",
        &[] as &[ParamValue],
    )
    .unwrap();
    for conversation_id in 1..=130 {
        conn.execute_compat(
            "INSERT INTO conversations(id, source_path) VALUES (?1, ?2)",
            &[
                ParamValue::from(i64::from(conversation_id)),
                ParamValue::from(format!("/tmp/legacy-{conversation_id}.jsonl")),
            ],
        )
        .unwrap();
        conn.execute_compat(
            "INSERT INTO messages(conversation_id, idx, role, content)
             VALUES (?1, 0, 'user', ?2)",
            &[
                ParamValue::from(i64::from(conversation_id)),
                ParamValue::from(format!("message {conversation_id}")),
            ],
        )
        .unwrap();
    }
    drop(conn);

    let storage = FrankenStorage::open_readonly(&db_path).unwrap();
    assert!(
        !storage
            .lexical_rebuild_has_tail_footprint_metadata()
            .unwrap(),
        "legacy canonical DB fixture intentionally has no tail metadata"
    );
    let settings = LexicalRebuildPipelineSettingsSnapshot {
        workers: 8,
        available_parallelism: 8,
        reserved_cores: 0,
        tantivy_writer_threads: 8,
        staged_shard_builders: 2,
        staged_merge_workers: 1,
        controller_mode: "steady".into(),
        controller_restore_clear_samples: 1,
        controller_restore_hold_ms: 0,
        controller_loadavg_high_watermark_1m_milli: None,
        controller_loadavg_low_watermark_1m_milli: None,
        page_size: LEXICAL_REBUILD_PAGE_SIZE,
        steady_batch_fetch_conversations: 512,
        startup_batch_fetch_conversations: 512,
        steady_commit_every_conversations: 512,
        startup_commit_every_conversations: 512,
        steady_commit_every_messages: 10_000,
        startup_commit_every_messages: 10_000,
        steady_commit_every_message_bytes: 64 * 1024 * 1024,
        startup_commit_every_message_bytes: 64 * 1024 * 1024,
        pipeline_channel_size: 2,
        page_prep_workers: 1,
        pipeline_max_message_bytes_in_flight: 256 * 1024,
    };

    let plan =
        plan_lexical_rebuild_shards_from_storage_with_settings(&storage, &settings, 130).unwrap();

    assert_eq!(plan.total_conversations, 130);
    assert_eq!(plan.total_messages, 130);
    assert_eq!(
        plan.shards
            .iter()
            .map(|shard| shard.conversation_count)
            .collect::<Vec<_>>(),
        vec![64, 64, 2]
    );
    assert!(
        plan.shards
            .iter()
            .all(|shard| shard.message_count == LEXICAL_SHARD_UNKNOWN_MESSAGE_COUNT),
        "id-only plans must leave doc-count validation to observed rebuild accounting"
    );
}

#[test]
fn lexical_rebuild_shard_plan_with_sparse_tail_metadata_uses_conservative_id_only_plan() {
    let temp = TempDir::new().unwrap();
    let db_path = temp.path().join("sparse-tail-canonical.db");
    let db_path_str = db_path.to_string_lossy().into_owned();
    let conn = crate::franken_sync::Connection::open(db_path_str).unwrap();
    conn.execute_compat(
        "CREATE TABLE conversations (
            id INTEGER PRIMARY KEY,
            source_path TEXT NOT NULL,
            last_message_idx INTEGER
        )",
        &[] as &[ParamValue],
    )
    .unwrap();
    conn.execute_compat(
        "CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            conversation_id INTEGER NOT NULL,
            idx INTEGER NOT NULL,
            role TEXT NOT NULL,
            author TEXT,
            created_at INTEGER,
            content TEXT NOT NULL,
            UNIQUE(conversation_id, idx)
        )",
        &[] as &[ParamValue],
    )
    .unwrap();
    conn.execute_compat(
        "CREATE INDEX idx_messages_conv_idx ON messages(conversation_id, idx)",
        &[] as &[ParamValue],
    )
    .unwrap();
    conn.execute_compat(
        "CREATE TABLE conversation_tail_state (
            conversation_id INTEGER PRIMARY KEY,
            ended_at INTEGER,
            last_message_idx INTEGER,
            last_message_created_at INTEGER
        )",
        &[] as &[ParamValue],
    )
    .unwrap();
    for conversation_id in 1..=130 {
        if conversation_id == 1 {
            conn.execute_compat(
                "INSERT INTO conversations(id, source_path, last_message_idx)
                 VALUES (?1, ?2, 0)",
                &[
                    ParamValue::from(i64::from(conversation_id)),
                    ParamValue::from(format!("/tmp/sparse-tail-{conversation_id}.jsonl")),
                ],
            )
            .unwrap();
        } else {
            conn.execute_compat(
                "INSERT INTO conversations(id, source_path, last_message_idx)
                 VALUES (?1, ?2, NULL)",
                &[
                    ParamValue::from(i64::from(conversation_id)),
                    ParamValue::from(format!("/tmp/sparse-tail-{conversation_id}.jsonl")),
                ],
            )
            .unwrap();
        }
        conn.execute_compat(
            "INSERT INTO messages(conversation_id, idx, role, content)
             VALUES (?1, 0, 'user', ?2)",
            &[
                ParamValue::from(i64::from(conversation_id)),
                ParamValue::from(format!("message {conversation_id}")),
            ],
        )
        .unwrap();
        conn.execute_compat(
            "INSERT INTO conversation_tail_state(conversation_id, last_message_idx)
             VALUES (?1, 0)",
            &[ParamValue::from(i64::from(conversation_id + 1_000))],
        )
        .unwrap();
    }
    drop(conn);

    let storage = FrankenStorage::open_readonly(&db_path).unwrap();
    assert!(
        !storage
            .lexical_rebuild_has_tail_footprint_metadata()
            .unwrap(),
        "one populated conversation tail plus stale tail-state rows must not send a large legacy DB through full footprint aggregation"
    );
    let settings = LexicalRebuildPipelineSettingsSnapshot {
        workers: 8,
        available_parallelism: 8,
        reserved_cores: 0,
        tantivy_writer_threads: 8,
        staged_shard_builders: 2,
        staged_merge_workers: 1,
        controller_mode: "steady".into(),
        controller_restore_clear_samples: 1,
        controller_restore_hold_ms: 0,
        controller_loadavg_high_watermark_1m_milli: None,
        controller_loadavg_low_watermark_1m_milli: None,
        page_size: LEXICAL_REBUILD_PAGE_SIZE,
        steady_batch_fetch_conversations: 512,
        startup_batch_fetch_conversations: 512,
        steady_commit_every_conversations: 512,
        startup_commit_every_conversations: 512,
        steady_commit_every_messages: 10_000,
        startup_commit_every_messages: 10_000,
        steady_commit_every_message_bytes: 64 * 1024 * 1024,
        startup_commit_every_message_bytes: 64 * 1024 * 1024,
        pipeline_channel_size: 2,
        page_prep_workers: 1,
        pipeline_max_message_bytes_in_flight: 256 * 1024,
    };

    let plan =
        plan_lexical_rebuild_shards_from_storage_with_settings(&storage, &settings, 130).unwrap();

    assert_eq!(
        plan.shards
            .iter()
            .map(|shard| shard.conversation_count)
            .collect::<Vec<_>>(),
        vec![64, 64, 2]
    );
    assert!(
        plan.shards
            .iter()
            .all(|shard| shard.message_count == LEXICAL_SHARD_UNKNOWN_MESSAGE_COUNT),
        "sparse tail metadata should use the same validation-safe id-only plan as absent metadata"
    );
}
