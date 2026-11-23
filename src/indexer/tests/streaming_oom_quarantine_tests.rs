use super::super::*;
use super::helpers::*;
use crate::connectors::{NormalizedConversation, NormalizedMessage};
use crate::franken_sync::compat::{ConnectionExt, ParamValue, RowExt};
use serial_test::serial;
use tempfile::TempDir;

#[test]
#[serial]
fn page_prep_worker_panic_surfaces_as_error_result_instead_of_parking_producer() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("agent_search.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    storage.close().unwrap();

    let worker_count = 2usize;
    let (work_tx, work_rx) = bounded::<LexicalRebuildPagePrepWork>(worker_count);
    let (result_tx, result_rx) = bounded::<LexicalRebuildPagePrepResult>(worker_count);
    let flow_limiter = Arc::new(StreamingByteLimiter::new(1024));
    let reservation_order = Arc::new(LexicalRebuildReservationOrder::new());
    let telemetry = Arc::new(LexicalRebuildProducerTelemetry::default());
    let worker_handles = spawn_lexical_rebuild_page_prep_workers(
        worker_count,
        db_path,
        Arc::new(HashMap::new()),
        work_rx,
        result_tx,
        Arc::clone(&flow_limiter),
        Arc::clone(&reservation_order),
        Arc::clone(&telemetry),
        None,
    )
    .unwrap();

    LEXICAL_REBUILD_PAGE_PREP_INJECTED_PANIC.arm(tmp.path());
    work_tx
        .send(LexicalRebuildPagePrepWork {
            sequence: 0,
            conversation_page: Vec::new(),
            page_last_conversation_id: 0,
            configured_page_size: 16,
            planned_shard_index: None,
            finishes_planned_shard: false,
            conversation_list_duration: Duration::ZERO,
            pipeline_budget: LexicalRebuildPipelineBudgetSnapshot::new(
                16, 64, 1024, 1024, 16, 64, 1024,
            ),
            budget_generation: 0,
        })
        .unwrap();

    let result = result_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("a panicking page-prep worker must still emit an Error result (#288)");
    match result {
        LexicalRebuildPagePrepResult::Error { sequence, error } => {
            assert_eq!(sequence, 0);
            assert!(
                error.to_string().contains("panicked"),
                "error must identify the panic: {error}"
            );
            assert!(
                error
                    .root_cause()
                    .to_string()
                    .contains("injected lexical rebuild page-prep panic"),
                "error must preserve the panic payload text: {error}"
            );
            assert!(
                !error
                    .to_string()
                    .contains("injected lexical rebuild page-prep panic"),
                "the public worker context must omit private panic payloads"
            );
        }
        other => panic!("expected a page-prep Error result, got {other:?}"),
    }

    drop(work_tx);
    for handle in worker_handles {
        let _ = handle.join();
    }
}

/// #282 regression: a panic inside a shard-build worker must surface as a
/// `LexicalRebuildShardBuildMessage::Error`. Before the catch_unwind
/// containment, the panicking worker unwound silently while sibling
/// workers kept the result channel open, so the staging→publish consumer
/// parked forever at the handoff (the terminal-wedge signature: all
/// threads on a condvar, governor spinning one core).
#[test]
#[serial]
fn shard_build_worker_panic_surfaces_as_error_result_instead_of_parking_consumer() {
    let worker_count = 1usize;
    let (work_tx, work_rx) = bounded::<LexicalRebuildShardBuildWork>(worker_count);
    let (msg_tx, msg_rx) = bounded::<LexicalRebuildShardBuildMessage>(worker_count);
    let flow_limiter = Arc::new(StreamingByteLimiter::new(1024));
    let handles = spawn_lexical_rebuild_shard_builder_workers(
        worker_count,
        work_rx,
        msg_tx,
        Arc::clone(&flow_limiter),
        None,
        None,
    );

    let tmp = TempDir::new().unwrap();
    LEXICAL_REBUILD_SHARD_BUILD_INJECTED_PANIC.arm(tmp.path());
    work_tx
        .send(LexicalRebuildShardBuildWork {
            shard: LexicalShardPlanShard {
                shard_index: 7,
                first_conversation_id: 0,
                last_conversation_id: 0,
                conversation_count: 0,
                message_count: 0,
                message_bytes: 0,
                conversation_id_fingerprint: String::new(),
                oversized_single_conversation: false,
            },
            packets: Vec::new(),
            message_bytes: 0,
            shard_index_path: tmp.path().join("shard-7"),
            writer_parallelism: 1,
        })
        .unwrap();

    let message = msg_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("a panicking shard-build worker must still emit an Error result (#282)");
    match message {
        LexicalRebuildShardBuildMessage::Error { shard_index, error } => {
            assert_eq!(shard_index, 7);
            assert!(
                error.contains("panicked"),
                "error must identify the panic: {error}"
            );
            assert!(
                error.contains("injected lexical rebuild shard-build panic"),
                "error must preserve the panic payload text: {error}"
            );
        }
        other => panic!("expected a shard-build Error result, got {other:?}"),
    }

    drop(work_tx);
    for handle in handles {
        let _ = handle.join();
    }
}

/// #282 regression: a panic inside a shard-merge worker must surface as a
/// `LexicalRebuildShardMergeMessage::Error`. Before the catch_unwind
/// containment, the panicking worker left the result channel open and the
/// final-frontier reducer parked forever on `merge_result_rx.recv()`.
#[test]
#[serial]
fn shard_merge_worker_panic_surfaces_as_error_result_instead_of_parking_reducer() {
    let worker_count = 1usize;
    let (job_tx, job_rx) = bounded::<LexicalRebuildShardMergeJob>(worker_count);
    let (msg_tx, msg_rx) = bounded::<LexicalRebuildShardMergeMessage>(worker_count);
    let handles = spawn_lexical_rebuild_shard_merge_workers(worker_count, job_rx, msg_tx);

    let tmp = TempDir::new().unwrap();
    LEXICAL_REBUILD_SHARD_MERGE_INJECTED_PANIC.arm(tmp.path());
    job_tx
        .send(LexicalRebuildShardMergeJob {
            output_level: 3,
            output_path: tmp.path().join("merge-out"),
            input_artifacts: Vec::new(),
        })
        .unwrap();

    let message = msg_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("a panicking shard-merge worker must still emit an Error result (#282)");
    match message {
        LexicalRebuildShardMergeMessage::Error {
            output_level,
            error,
            ..
        } => {
            assert_eq!(output_level, 3);
            assert!(
                error.contains("panicked"),
                "error must identify the panic: {error}"
            );
            assert!(
                error.contains("injected lexical rebuild shard-merge panic"),
                "error must preserve the panic payload text: {error}"
            );
        }
        other => panic!("expected a shard-merge Error result, got {other:?}"),
    }

    drop(job_tx);
    for handle in handles {
        let _ = handle.join();
    }
}

/// `coding_agent_session_search-wxsy8`: stress-pin the
/// capacity-shrink-then-grow lost-wakeup race that the fix
/// closed. Pre-fix, `update_max_bytes_in_flight` stored the new
/// max via `AtomicUsize::store` + `cv.notify_all` WITHOUT
/// acquiring the state lock the waiter held during predicate
/// evaluation. If the update fired in the gap between the
/// waiter's predicate check (line ~7513) and its `cv.wait` call
/// (line ~7524), the notification landed on zero subscribers
/// and the waiter parked indefinitely. Post-fix, the updater
/// MUST acquire `self.state` first — serializing with the
/// waiter's predicate check — so either (a) the update lands
/// before the waiter takes the lock and the waiter sees the new
/// max on its first predicate check, OR (b) the waiter is
/// already parked when the update fires and the notification
/// reaches it.
///
/// The race is tight, so a single iteration can pass even with
/// the bug. This test runs 50 iterations of the same shrink-grow
/// pattern to maximize the chance of hitting the race window.
/// With the fix it always completes within a generous timeout;
/// without the fix at least one waiter would stall past the
/// per-iteration deadline under enough scheduling pressure.
#[test]
fn streaming_byte_limiter_update_does_not_lose_wakeup_under_repeated_shrink_grow() {
    const ITERATIONS: usize = 50;
    for iteration in 0..ITERATIONS {
        let limiter = Arc::new(StreamingByteLimiter::new(16));
        let first = limiter.acquire(16).unwrap();
        let (ready_tx, ready_rx) = bounded(1);
        let (result_tx, result_rx) = bounded(1);
        let waiter = {
            let limiter = limiter.clone();
            thread::spawn(move || {
                ready_tx.send(()).unwrap();
                let second = limiter.acquire(16).unwrap();
                result_tx.send(second).unwrap();
                limiter.release(second);
            })
        };
        ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        // Hand off the scheduler immediately rather than sleeping —
        // we WANT the update to race against the waiter's
        // predicate-check window, not after it has comfortably
        // parked.
        thread::yield_now();
        limiter.update_max_bytes_in_flight(64);
        let woken = result_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap_or_else(|err| {
                panic!(
                    "iteration {iteration}: update_max_bytes_in_flight failed to wake parked \
                 waiter within 2s — lost-wakeup race regressed (wxsy8): {err}"
                )
            });
        assert_eq!(woken, 16);
        limiter.release(first);
        waiter.join().unwrap();
    }
}

#[test]
fn streaming_byte_limiter_acquire_with_wait_reports_capacity_stall() {
    let limiter = Arc::new(StreamingByteLimiter::new(64));
    let first = limiter.acquire(64).unwrap();
    let releaser = {
        let limiter = limiter.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(25));
            limiter.release(first);
        })
    };

    let (reserved, _wait_duration, waited) = limiter.acquire_with_wait(32).unwrap();
    assert_eq!(reserved, 32);
    assert!(waited, "capacity stall should be reported to telemetry");
    limiter.release(reserved);
    releaser.join().unwrap();
}

#[test]
fn lexical_rebuild_pipeline_runtime_snapshot_observes_stall_only_telemetry() {
    let runtime = LexicalRebuildPipelineRuntimeSnapshot {
        producer_budget_wait_count: 1,
        producer_budget_wait_ms: 7,
        producer_handoff_wait_count: 1,
        producer_handoff_wait_ms: 3,
        ..LexicalRebuildPipelineRuntimeSnapshot::default()
    };

    assert!(
        runtime.is_observed(),
        "stall counters alone should keep attach/status runtime visible"
    );
}

#[test]
fn send_conversation_batches_marks_only_first_batch_as_discovered() {
    let (tx, rx) = bounded(4);
    let convs = vec![
        norm_conv(
            Some("a"),
            (0..1_200).map(|i| norm_msg(i, 1_000 + i)).collect(),
        ),
        norm_conv(
            Some("b"),
            (0..1_200).map(|i| norm_msg(i, 2_000 + i)).collect(),
        ),
    ];

    send_conversation_batches(&tx, "claude", convs, true);
    drop(tx);

    let batches = rx.try_iter().collect::<Vec<_>>();
    assert_eq!(batches.len(), 2);

    match &batches[0] {
        IndexMessage::Batch {
            connector_name,
            is_discovered,
            message_count,
            conversations,
            ..
        } => {
            assert_eq!(*connector_name, "claude");
            assert!(*is_discovered);
            assert_eq!(*message_count, 1_200);
            assert_eq!(conversations.len(), 1);
        }
        _ => panic!("expected first message to be a batch"),
    }

    match &batches[1] {
        IndexMessage::Batch {
            connector_name,
            is_discovered,
            message_count,
            conversations,
            ..
        } => {
            assert_eq!(*connector_name, "claude");
            assert!(!*is_discovered);
            assert_eq!(*message_count, 1_200);
            assert_eq!(conversations.len(), 1);
        }
        _ => panic!("expected second message to be a batch"),
    }
}

#[test]
fn snapshot_json_phase_label_matches_phase_code_sample() {
    let progress = IndexingProgress::default();
    progress.phase.store(2, Ordering::Relaxed);
    let snapshot = progress.snapshot_json(2500);

    assert_eq!(snapshot["phase_code"], serde_json::json!(2));
    assert_eq!(snapshot["phase"], serde_json::json!("indexing"));
}

#[test]
fn legacy_omp_analytics_progress_reports_messages_and_typed_stop() {
    let progress = Arc::new(IndexingProgress::default());
    check_legacy_omp_analytics_stop(Some(&progress)).expect("repair initially permitted");
    report_legacy_omp_analytics_progress(Some(&progress), 128, 513);
    let snapshot = progress.snapshot_json(1000);
    assert_eq!(snapshot["phase"], serde_json::json!("analytics_rebuild"));
    assert_eq!(
        snapshot["phase_code"],
        serde_json::json!(INDEX_PHASE_ANALYTICS_REBUILD)
    );
    assert_eq!(snapshot["unit"], serde_json::json!("messages"));
    assert_eq!(snapshot["current"], serde_json::json!(128));
    assert_eq!(snapshot["total"], serde_json::json!(513));
    assert!(progress.total_is_final.load(Ordering::Relaxed));

    progress.request_stop();
    let error = check_legacy_omp_analytics_stop(Some(&progress))
        .context("legacy OMP analytics checkpoint")
        .expect_err("stop request must escape the repair callback");
    assert!(error.downcast_ref::<IndexInterrupted>().is_some());
    assert_eq!(progress.current.load(Ordering::Relaxed), 128);
    assert_eq!(progress.phase_label(), "analytics_rebuild");
    check_legacy_omp_analytics_stop(None).expect("headless callers without a stop source");

    let aggregate_progress = Arc::new(IndexingProgress::default());
    report_legacy_omp_analytics_progress(Some(&aggregate_progress), 513, 513);
    report_legacy_omp_analytics_progress(Some(&aggregate_progress), 0, 0);
    assert_eq!(aggregate_progress.phase_label(), "analytics_rebuild");
    assert_eq!(aggregate_progress.current.load(Ordering::Relaxed), 0);
    assert!(!aggregate_progress.total_is_final.load(Ordering::Relaxed));
}

#[test]
fn gh515_stop_before_preflight_never_opens_archive() -> Result<()> {
    let tmp = TempDir::new()?;
    let data_dir = tmp.path().join("not-started");
    let progress = Arc::new(IndexingProgress::default());
    progress.request_stop();
    let error = run_index_with_local_connector_roots(
        IndexOptions {
            full: false,
            force_rebuild: false,
            watch: false,
            watch_once_paths: None,
            db_path: data_dir.join("agent_search.db"),
            data_dir: data_dir.clone(),
            semantic: false,
            build_hnsw: false,
            embedder: "hash".to_string(),
            progress: Some(progress),
            watch_interval_secs: 30,
        },
        HashMap::new(),
        None,
    )
    .expect_err("an already-cancelled run must stop before preflight");
    assert!(error.downcast_ref::<IndexInterrupted>().is_some());
    assert!(
        !data_dir.exists(),
        "cancellation must not create an archive or acquire the indexing lock"
    );
    Ok(())
}

#[test]
fn snapshot_json_includes_rebuild_pipeline_runtime_metrics() {
    let progress = IndexingProgress::default();
    progress.phase.store(2, Ordering::Relaxed);
    progress.is_rebuilding.store(true, Ordering::Relaxed);
    progress
        .rebuild_pipeline_queue_depth
        .store(3, Ordering::Relaxed);
    progress
        .rebuild_pipeline_inflight_message_bytes
        .store(65_536, Ordering::Relaxed);
    progress
        .rebuild_pipeline_pending_batch_conversations
        .store(9, Ordering::Relaxed);
    progress
        .rebuild_pipeline_pending_batch_message_bytes
        .store(131_072, Ordering::Relaxed);
    progress
        .rebuild_pipeline_page_prep_workers
        .store(6, Ordering::Relaxed);
    progress
        .rebuild_pipeline_active_page_prep_jobs
        .store(2, Ordering::Relaxed);
    progress
        .rebuild_pipeline_ordered_buffered_pages
        .store(4, Ordering::Relaxed);
    progress
        .rebuild_pipeline_budget_generation
        .store(1, Ordering::Relaxed);
    progress
        .rebuild_pipeline_producer_budget_wait_count
        .store(2, Ordering::Relaxed);
    progress
        .rebuild_pipeline_producer_budget_wait_ms
        .store(17, Ordering::Relaxed);
    progress
        .rebuild_pipeline_producer_handoff_wait_count
        .store(1, Ordering::Relaxed);
    progress
        .rebuild_pipeline_producer_handoff_wait_ms
        .store(9, Ordering::Relaxed);
    *progress
        .rebuild_pipeline_host_loadavg_1m_milli
        .lock()
        .expect("lock host loadavg") = Some(7_250);
    *progress
        .rebuild_pipeline_host_available_memory_bytes
        .lock()
        .expect("lock host available memory") = Some(123_456_789);
    *progress
        .rebuild_pipeline_process_rss_bytes
        .lock()
        .expect("lock process rss") = Some(98_765_432);
    *progress
        .rebuild_pipeline_controller_mode
        .lock()
        .expect("lock controller mode") = "pressure_limited".to_string();
    *progress
        .rebuild_pipeline_controller_reason
        .lock()
        .expect("lock controller reason") = "queue_depth_2_reached_pipeline_capacity_2".to_string();
    progress
        .rebuild_pipeline_staged_merge_workers_max
        .store(3, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_merge_allowed_jobs
        .store(1, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_merge_active_jobs
        .store(1, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_merge_ready_artifacts
        .store(5, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_merge_ready_groups
        .store(1, Ordering::Relaxed);
    *progress
        .rebuild_pipeline_staged_merge_controller_reason
        .lock()
        .expect("lock staged merge reason") = "page_prep_workers_saturated_6_of_6".to_string();
    progress
        .rebuild_pipeline_staged_shard_build_workers_max
        .store(6, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_shard_build_allowed_jobs
        .store(5, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_shard_build_active_jobs
        .store(4, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_shard_build_pending_jobs
        .store(2, Ordering::Relaxed);
    *progress
        .rebuild_pipeline_staged_shard_build_controller_reason
        .lock()
        .expect("lock staged shard-build reason") =
        "reserving_1_slots_for_staged_merge_active_jobs_1_ready_groups_1".to_string();
    progress
        .rebuild_pipeline_staged_shard_build_memory_reserve_bytes
        .store(16_000, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_shard_build_emergency_memory_reserve_bytes
        .store(4_000, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_shard_build_completed_jobs
        .store(7, Ordering::Relaxed);
    *progress
        .rebuild_pipeline_staged_shard_build_last_shard_index
        .lock()
        .expect("lock last shard index") = Some(6);
    progress
        .rebuild_pipeline_staged_shard_build_last_message_bytes
        .store(65_000_000, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_shard_build_last_index_size_bytes
        .store(130_000_000, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_shard_build_last_duration_ms
        .store(2_500, Ordering::Relaxed);
    *progress
        .rebuild_pipeline_staged_shard_build_last_amplification_milli
        .lock()
        .expect("lock last amplification") = Some(2_000);
    *progress
        .rebuild_pipeline_staged_shard_build_observed_amplification_milli
        .lock()
        .expect("lock observed amplification") = Some(8_000);

    let snapshot = progress.snapshot_json(2500);

    assert_eq!(
        snapshot["rebuild_pipeline"]["queue_depth"],
        serde_json::json!(3)
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["inflight_message_bytes"],
        serde_json::json!(65_536)
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["pending_batch_conversations"],
        serde_json::json!(9)
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["pending_batch_message_bytes"],
        serde_json::json!(131_072)
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["page_prep_workers"],
        serde_json::json!(6)
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["active_page_prep_jobs"],
        serde_json::json!(2)
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["ordered_buffered_pages"],
        serde_json::json!(4)
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["budget_generation"],
        serde_json::json!(1)
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["producer_budget_wait_count"],
        serde_json::json!(2)
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["producer_budget_wait_ms"],
        serde_json::json!(17)
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["producer_handoff_wait_count"],
        serde_json::json!(1)
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["producer_handoff_wait_ms"],
        serde_json::json!(9)
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["host_loadavg_1m"],
        serde_json::json!(7.25)
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["host_available_memory_bytes"],
        serde_json::json!(123_456_789)
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["process_rss_bytes"],
        serde_json::json!(98_765_432)
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["controller_mode"],
        serde_json::json!("pressure_limited")
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["controller_reason"],
        serde_json::json!("queue_depth_2_reached_pipeline_capacity_2")
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["staged_merge_workers_max"],
        serde_json::json!(3)
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["staged_merge_allowed_jobs"],
        serde_json::json!(1)
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["staged_merge_active_jobs"],
        serde_json::json!(1)
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["staged_merge_ready_artifacts"],
        serde_json::json!(5)
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["staged_merge_ready_groups"],
        serde_json::json!(1)
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["staged_merge_controller_reason"],
        serde_json::json!("page_prep_workers_saturated_6_of_6")
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["staged_shard_build_workers_max"],
        serde_json::json!(6)
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["staged_shard_build_allowed_jobs"],
        serde_json::json!(5)
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["staged_shard_build_active_jobs"],
        serde_json::json!(4)
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["staged_shard_build_pending_jobs"],
        serde_json::json!(2)
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["staged_shard_build_controller_reason"],
        serde_json::json!("reserving_1_slots_for_staged_merge_active_jobs_1_ready_groups_1")
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["staged_shard_build_memory_reserve_bytes"],
        serde_json::json!(16_000)
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["staged_shard_build_emergency_memory_reserve_bytes"],
        serde_json::json!(4_000)
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["staged_shard_build_completed_jobs"],
        serde_json::json!(7)
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["staged_shard_build_last_shard_index"],
        serde_json::json!(6)
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["staged_shard_build_last_message_bytes"],
        serde_json::json!(65_000_000)
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["staged_shard_build_last_index_size_bytes"],
        serde_json::json!(130_000_000u64)
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["staged_shard_build_last_duration_ms"],
        serde_json::json!(2_500u64)
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["staged_shard_build_last_amplification_milli"],
        serde_json::json!(2_000u64)
    );
    assert_eq!(
        snapshot["rebuild_pipeline"]["staged_shard_build_observed_amplification_milli"],
        serde_json::json!(8_000u64)
    );
}

/// A tiny two-message claude_code session unique per `ordinal` — the
/// shape the region marks machinery provably projects and publishes.
fn grind_stream_session(ordinal: i64) -> NormalizedConversation {
    NormalizedConversation {
        agent_slug: "claude_code".into(),
        external_id: Some(format!("region-stream-grind-{ordinal}")),
        title: Some(format!("Region stream grind {ordinal}")),
        workspace: Some(PathBuf::from("/workspace/grind")),
        source_path: PathBuf::from(format!("/logs/region-stream-grind-{ordinal}.jsonl")),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_050),
        metadata: serde_json::json!({"model": "fixture-model"}),
        messages: vec![
            NormalizedMessage {
                idx: 0,
                role: "user".into(),
                author: None,
                created_at: Some(1_700_000_000_000),
                content: format!("question {ordinal}"),
                extra: serde_json::json!({"uuid": format!("u-{ordinal}-0")}),
                snippets: Vec::new(),
                invocations: Vec::new(),
            },
            NormalizedMessage {
                idx: 1,
                role: "assistant".into(),
                author: None,
                created_at: Some(1_700_000_000_050),
                content: format!("answer {ordinal}"),
                extra: serde_json::json!({"uuid": format!("u-{ordinal}-1")}),
                snippets: Vec::new(),
                invocations: Vec::new(),
            },
        ],
    }
}

/// `(region_version, docs_state, count)` rows of the region marks table.
fn stream_region_mark_histogram(storage: &FrankenStorage) -> Vec<(i64, String, i64)> {
    storage
        .raw()
        .query_map_collect(
            "SELECT region_version, docs_state, COUNT(*) \
             FROM viewer_search_region_marks \
             GROUP BY region_version, docs_state \
             ORDER BY region_version, docs_state",
            &[] as &[ParamValue],
            |row| {
                Ok((
                    row.get_typed::<i64>(0)?,
                    row.get_typed::<String>(1)?,
                    row.get_typed::<i64>(2)?,
                ))
            },
        )
        .unwrap()
}

/// Drive the real streaming consumer over one Batch message per
/// conversation — the batch-of-one shape every oversized agent session
/// produces on the lane — plus the producer's Done. The final flag is
/// the consumer's republish-deferral lane gate: `true` models an
/// ordinary non-watch run, `false` the watch lane.
fn run_streaming_consumer_over_sessions(
    storage: &FrankenStorage,
    data_dir: &Path,
    index: &mut TantivyIndex,
    convs: Vec<NormalizedConversation>,
    allow_region_doc_republish_deferral: bool,
) -> Result<()> {
    let capacity = convs.len() + 1;
    let (tx, rx) = bounded(capacity);
    for conv in convs {
        tx.send(IndexMessage::Batch {
            connector_name: "claude",
            conversations: vec![conv],
            is_discovered: false,
            message_count: 2,
            byte_reservation: 0,
        })?;
    }
    tx.send(IndexMessage::Done {
        connector_name: "claude",
        scan_ms: 1,
        is_discovered: false,
        scan_succeeded: true,
        active_source_skipped: false,
    })?;
    drop(tx);
    run_streaming_consumer(
        rx,
        1,
        storage,
        data_dir,
        Some(index),
        Arc::new(StreamingByteLimiter::new(STREAMING_MAX_BYTES_IN_FLIGHT)),
        &None,
        LexicalPopulationStrategy::IncrementalInline,
        allow_region_doc_republish_deferral,
        None,
        None,
    )?;
    Ok(())
}

/// The whole-archive republish through the REAL streaming consumer: a
/// REGION_VERSION bump leaves every marks row stale (still reading
/// `published` for its old version), so every session's receipt demands
/// a re-projection and the lane re-persists one session per batch. The
/// consumer must latch the deferral at its prologue, stage no region
/// documents per batch, and converge the accumulated pending set ONCE
/// at its final commit — not publish a successor segment per session.
#[test]
fn streaming_consumer_republish_grind_converges_once_not_per_batch() -> Result<()> {
    let tmp = TempDir::new()?;
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir)?;
    let db_path = data_dir.join("agent_search.db");
    let storage = FrankenStorage::open(&db_path)?;
    ensure_fts_schema(&storage);
    let index_path = index_dir(&data_dir)?;
    let mut index = TantivyIndex::open_or_create(&index_path)?;
    let session_count = persist::REGION_DOC_REPUBLISH_DEFER_MIN_STALE_SESSIONS + 1;
    let convs: Vec<_> = (0..session_count).map(grind_stream_session).collect();

    // Run 1 seeds the archive at the current REGION_VERSION: the
    // per-batch prelude converges each session with its batch and the
    // consumer's final commit stamps every mark `published`.
    run_streaming_consumer_over_sessions(&storage, &data_dir, &mut index, convs.clone(), true)?;
    assert_eq!(
        stream_region_mark_histogram(&storage),
        vec![(
            crate::viewer_extensions::search_regions::REGION_VERSION,
            crate::viewer_extensions::search_regions::DOCS_PUBLISHED.to_string(),
            session_count
        )],
        "the seeded archive must hold every mark published at the current version"
    );
    let seeded_segments = index.segment_count();

    // The durable state a REGION_VERSION bump creates.
    storage
        .raw()
        .execute("UPDATE viewer_search_region_marks SET region_version = 5")?;

    // Run 2 — the republish grind: every receipt demands a re-projection,
    // one session per persist batch. The consumer's final commit must
    // leave every mark published again, and the whole grind must cost a
    // bounded handful of segment publishes — not one per session.
    run_streaming_consumer_over_sessions(&storage, &data_dir, &mut index, convs, true)?;

    assert_eq!(
        stream_region_mark_histogram(&storage),
        vec![(
            crate::viewer_extensions::search_regions::REGION_VERSION,
            crate::viewer_extensions::search_regions::DOCS_PUBLISHED.to_string(),
            session_count
        )],
        "the final drain plus the consumer's final commit must republish every mark"
    );
    assert!(
        crate::viewer_extensions::search_regions::pending_region_doc_sessions(
            storage.raw(),
            i64::MAX
        )?
        .is_empty(),
        "no session may remain pending after the run's final commit"
    );
    // The bound's provenance (do not reflexively tighten or loosen):
    // the one final drain publishes successor segments for the live
    // segments it replaces — measured +6 for this fixture's 513
    // sessions — so +32 is generous headroom for commit-pacer
    // batching, while staying 16x below the +513 that the
    // one-publish-per-session failure mode would cost. A drain that
    // publishes near the session count is the regression this bound
    // exists to catch; one that lands near +6 is the expected shape.
    assert!(
        index.segment_count() <= seeded_segments + 32,
        "the republish grind must not publish one segment per session: seeded={seeded_segments} after={}",
        index.segment_count()
    );
    Ok(())
}

/// Regression (review Important 1): the watch lane's startup broad scan
/// drives this same consumer before the watch loop inherits the writer,
/// and a watch run has NO final drain — if the prologue latched the
/// whole-archive republish deferral on that lane, every persist the
/// watch loop later committed would skip region-doc convergence and its
/// marks would sit `pending` for the life of the process. The consumer
/// must never latch when the caller passes the watch lane, mirroring
/// the scan entry's `!opts.watch && opts.watch_once_paths.is_none()`
/// gate (tantivy.rs: the latch is "never latched by watch").
#[test]
fn streaming_consumer_watch_lane_never_latches_republish_deferral() -> Result<()> {
    let tmp = TempDir::new()?;
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir)?;
    let db_path = data_dir.join("agent_search.db");
    let storage = FrankenStorage::open(&db_path)?;
    ensure_fts_schema(&storage);
    let index_path = index_dir(&data_dir)?;
    let mut index = TantivyIndex::open_or_create(&index_path)?;
    let session_count = persist::REGION_DOC_REPUBLISH_DEFER_MIN_STALE_SESSIONS + 1;
    let convs: Vec<_> = (0..session_count).map(grind_stream_session).collect();

    // Run 1 seeds the archive at the current REGION_VERSION through the
    // ordinary lane: zero stale marks, so the allowed deferral never
    // engages and per-batch convergence publishes every mark.
    run_streaming_consumer_over_sessions(&storage, &data_dir, &mut index, convs.clone(), true)?;
    assert_eq!(
        stream_region_mark_histogram(&storage),
        vec![(
            crate::viewer_extensions::search_regions::REGION_VERSION,
            crate::viewer_extensions::search_regions::DOCS_PUBLISHED.to_string(),
            session_count
        )],
        "the seeded archive must hold every mark published at the current version"
    );

    // The durable state a REGION_VERSION bump creates: every mark now
    // reads stale — more than the deferral threshold — so an ordinary
    // non-watch consumer run WOULD latch here (the grind test above
    // pins that lane).
    storage
        .raw()
        .execute("UPDATE viewer_search_region_marks SET region_version = 5")?;

    // Run 2 — the same above-threshold stale archive through the WATCH
    // lane. The writer the watch loop would inherit must stay
    // unlatched, and per-event convergence must still republish every
    // mark: the consumer's final drain is itself gated on the latch
    // flag, so a false flag plus zero pending marks proves convergence
    // happened per batch, not in one deferred drain.
    run_streaming_consumer_over_sessions(&storage, &data_dir, &mut index, convs, false)?;

    assert!(
        !index.region_doc_convergence_deferred_to_final_drain(),
        "the watch lane must never latch the whole-archive republish deferral: \
         a watch run has no final drain, so latched marks would never converge"
    );
    assert_eq!(
        stream_region_mark_histogram(&storage),
        vec![(
            crate::viewer_extensions::search_regions::REGION_VERSION,
            crate::viewer_extensions::search_regions::DOCS_PUBLISHED.to_string(),
            session_count
        )],
        "the watch lane's per-event convergence must republish every mark"
    );
    assert!(
        crate::viewer_extensions::search_regions::pending_region_doc_sessions(
            storage.raw(),
            i64::MAX
        )?
        .is_empty(),
        "no session may remain pending on the watch lane"
    );
    Ok(())
}

#[test]
fn streaming_consumer_preserves_discovered_connector_with_no_batches() {
    for scan_start_ts in [None, Some(1_700_000_000_000)] {
        let tmp = TempDir::new().unwrap();
        let data_dir = tmp.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();

        let db_path = data_dir.join("db.sqlite");
        let storage = FrankenStorage::open(&db_path).unwrap();
        ensure_fts_schema(&storage);
        storage.set_connector_last_scan_ts("claude", 7).unwrap();
        let mut index = TantivyIndex::open_or_create(&index_dir(&data_dir).unwrap()).unwrap();
        let progress = Arc::new(IndexingProgress::default());
        let (tx, rx) = bounded(2);

        tx.send(IndexMessage::Done {
            connector_name: "claude",
            scan_ms: 42,
            is_discovered: true,
            scan_succeeded: true,
            active_source_skipped: false,
        })
        .unwrap();
        drop(tx);

        let (discovered, mutations) = run_streaming_consumer(
            rx,
            1,
            &storage,
            &data_dir,
            Some(&mut index),
            Arc::new(StreamingByteLimiter::new(STREAMING_MAX_BYTES_IN_FLIGHT)),
            &Some(progress.clone()),
            LexicalPopulationStrategy::IncrementalInline,
            true,
            scan_start_ts,
            None,
        )
        .unwrap();

        assert_eq!(discovered, vec!["claude".to_string()]);
        assert_eq!(mutations, CanonicalMutationCounts::default());
        let stats = progress.stats.lock().unwrap_or_else(|e| e.into_inner());
        assert_eq!(stats.agents_discovered, vec!["claude".to_string()]);
        assert_eq!(stats.total_conversations, 0);
        assert_eq!(stats.total_messages, 0);
        assert_eq!(
            mutations.scanned_connectors,
            if scan_start_ts.is_some() {
                BTreeSet::from(["claude".to_string()])
            } else {
                BTreeSet::new()
            }
        );
        assert_eq!(
            storage.get_connector_last_scan_ts("claude").unwrap(),
            Some(scan_start_ts.unwrap_or(7))
        );
        assert!(!mutations.scan_had_errors);
    }
}

#[test]
fn gh426_active_source_skip_only_withholds_its_own_connector_watermark() -> Result<()> {
    let tmp = TempDir::new()?;
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir)?;

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path)?;
    ensure_fts_schema(&storage);
    let mut index = TantivyIndex::open_or_create(&index_dir(&data_dir)?)?;
    let (tx, rx) = bounded(4);
    let scan_start_ts = 1_700_000_123_456_i64;

    tx.send(IndexMessage::Done {
        connector_name: "claude",
        scan_ms: 1,
        is_discovered: true,
        scan_succeeded: true,
        active_source_skipped: true,
    })
    .map_err(|_| anyhow::anyhow!("active-source Done message should send"))?;
    tx.send(IndexMessage::Done {
        connector_name: "codex",
        scan_ms: 1,
        is_discovered: true,
        scan_succeeded: true,
        active_source_skipped: false,
    })
    .map_err(|_| anyhow::anyhow!("safe Done message should send"))?;
    drop(tx);

    let (_discovered, outcome) = run_streaming_consumer(
        rx,
        2,
        &storage,
        &data_dir,
        Some(&mut index),
        Arc::new(StreamingByteLimiter::new(STREAMING_MAX_BYTES_IN_FLIGHT)),
        &None,
        LexicalPopulationStrategy::IncrementalInline,
        true,
        Some(scan_start_ts),
        None,
    )?;

    assert_eq!(
        outcome.scanned_connectors,
        BTreeSet::from(["codex".to_string()]),
        "one active source must not freeze unrelated connector watermarks"
    );
    assert_eq!(storage.get_connector_last_scan_ts("claude")?, None);
    assert_eq!(
        storage.get_connector_last_scan_ts("codex")?,
        Some(scan_start_ts)
    );
    Ok(())
}

#[test]
fn streaming_consumer_does_not_watermark_failed_discovered_connector() -> Result<()> {
    let tmp = TempDir::new()?;
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir)?;

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path)?;
    ensure_fts_schema(&storage);
    let index_path = index_dir(&data_dir)?;
    let mut index = TantivyIndex::open_or_create(&index_path)?;
    let progress = Arc::new(IndexingProgress::default());
    let (tx, rx) = bounded(2);

    tx.send(IndexMessage::ScanError {
        connector_name: "claude",
        error: "local scan failed after detection".to_string(),
    })
    .map_err(|_| anyhow::anyhow!("scan error message should send"))?;
    tx.send(IndexMessage::Done {
        connector_name: "claude",
        scan_ms: 42,
        is_discovered: true,
        scan_succeeded: false,
        active_source_skipped: false,
    })
    .map_err(|_| anyhow::anyhow!("done message should send"))?;
    drop(tx);

    let (discovered, mutations) = run_streaming_consumer(
        rx,
        1,
        &storage,
        &data_dir,
        Some(&mut index),
        Arc::new(StreamingByteLimiter::new(STREAMING_MAX_BYTES_IN_FLIGHT)),
        &Some(progress.clone()),
        LexicalPopulationStrategy::IncrementalInline,
        true,
        None,
        None,
    )?;

    assert_eq!(discovered, vec!["claude".to_string()]);
    assert!(
        mutations.scanned_connectors.is_empty(),
        "failed scans must not advance per-connector watermarks"
    );
    assert!(mutations.scan_had_errors);
    let stats = progress.stats.lock().unwrap_or_else(|e| e.into_inner());
    assert!(stats.scan_had_errors, "CLI must observe the failed scan");
    assert_eq!(stats.agents_discovered, vec!["claude".to_string()]);
    assert_eq!(
        stats
            .connectors
            .iter()
            .find(|connector| connector.name == "claude")
            .and_then(|connector| connector.error.as_deref()),
        Some("local scan failed after detection")
    );
    Ok(())
}

#[test]
fn streaming_consumer_does_not_watermark_connector_after_prior_scan_error() -> Result<()> {
    let tmp = TempDir::new()?;
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir)?;

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path)?;
    ensure_fts_schema(&storage);
    let index_path = index_dir(&data_dir)?;
    let mut index = TantivyIndex::open_or_create(&index_path)?;
    let progress = Arc::new(IndexingProgress::default());
    let (tx, rx) = bounded(2);

    tx.send(IndexMessage::ScanError {
        connector_name: "claude",
        error: "scan error must dominate done success".to_string(),
    })
    .map_err(|_| anyhow::anyhow!("scan error message should send"))?;
    tx.send(IndexMessage::Done {
        connector_name: "claude",
        scan_ms: 42,
        is_discovered: true,
        scan_succeeded: true,
        active_source_skipped: false,
    })
    .map_err(|_| anyhow::anyhow!("done message should send"))?;
    drop(tx);

    let (discovered, mutations) = run_streaming_consumer(
        rx,
        1,
        &storage,
        &data_dir,
        Some(&mut index),
        Arc::new(StreamingByteLimiter::new(STREAMING_MAX_BYTES_IN_FLIGHT)),
        &Some(progress),
        LexicalPopulationStrategy::IncrementalInline,
        true,
        None,
        None,
    )?;

    assert_eq!(discovered, vec!["claude".to_string()]);
    assert!(
        mutations.scanned_connectors.is_empty(),
        "a prior scan error must prevent per-connector watermark advancement"
    );
    assert!(mutations.scan_had_errors);
    Ok(())
}

#[test]
fn streaming_consumer_can_defer_authoritative_lexical_updates_without_tantivy_writer() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    let progress = Arc::new(IndexingProgress::default());
    let (tx, rx) = bounded(STREAMING_CHANNEL_SIZE);

    send_conversation_batches(
        &tx,
        "codex",
        vec![norm_conv(
            Some("stream-deferred"),
            vec![
                norm_msg(0, 1_700_000_000_000),
                norm_msg(1, 1_700_000_000_100),
            ],
        )],
        true,
    );
    send_done(&tx, "codex", true);
    drop(tx);

    let (discovered, mutations) = run_streaming_consumer(
        rx,
        1,
        &storage,
        &data_dir,
        None,
        Arc::new(StreamingByteLimiter::new(STREAMING_MAX_BYTES_IN_FLIGHT)),
        &Some(progress.clone()),
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
        true,
        Some(FrankenStorage::now_millis()),
        None,
    )
    .expect("deferred streaming ingest should not require a Tantivy writer");

    let conversation_count: i64 = storage
        .raw()
        .query_row_map("SELECT COUNT(*) FROM conversations", &[], |row| {
            row.get_typed(0)
        })
        .unwrap();
    let message_count: i64 = storage
        .raw()
        .query_row_map("SELECT COUNT(*) FROM messages", &[], |row| row.get_typed(0))
        .unwrap();

    assert_eq!(discovered, vec!["codex".to_string()]);
    assert_eq!(
        mutations,
        CanonicalMutationCounts {
            inserted_conversations: 1,
            inserted_messages: 2,
            updated_messages: 0,
        }
    );
    assert_eq!(conversation_count, 1);
    assert_eq!(message_count, 2);
    assert!(
        !index_dir(&data_dir).unwrap().join("meta.json").exists(),
        "deferred streaming ingest should not materialize a live Tantivy index before the authoritative rebuild"
    );
}

#[test]
#[serial]
fn streaming_consumer_defers_lexical_oom_without_quarantining_persisted_conversation() {
    let _oom_guard = inject_incremental_lexical_update_test_oom();
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    let mut index = TantivyIndex::open_or_create(&index_dir(&data_dir).unwrap()).unwrap();
    let progress = Arc::new(IndexingProgress::default());
    let (tx, rx) = bounded(STREAMING_CHANNEL_SIZE);

    send_conversation_batches(
        &tx,
        "codex",
        vec![norm_conv(
            Some("lexical-oom-deferred"),
            vec![norm_msg(0, 1_700_000_000_000)],
        )],
        true,
    );
    send_done(&tx, "codex", true);
    drop(tx);

    let (_discovered, outcome) = run_streaming_consumer(
        rx,
        1,
        &storage,
        &data_dir,
        Some(&mut index),
        Arc::new(StreamingByteLimiter::new(STREAMING_MAX_BYTES_IN_FLIGHT)),
        &Some(progress.clone()),
        LexicalPopulationStrategy::IncrementalInline,
        true,
        Some(FrankenStorage::now_millis()),
        None,
    )
    .expect("lexical OOM after SQLite ingest should defer repair, not fail the scan");

    assert_eq!(outcome.inserted_conversations, 1);
    assert_eq!(outcome.inserted_messages, 1);
    assert!(outcome.lexical_update_deferred);
    assert_eq!(outcome.quarantined_conversations, 0);
    let conversation_rows: i64 = storage
        .raw()
        .query_row_map("SELECT COUNT(*) FROM conversations", &[], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(conversation_rows, 1);
    assert!(
        !data_dir
            .join("quarantine/index_ingest_poison.jsonl")
            .exists(),
        "lexical-only OOM must not quarantine a conversation already preserved in SQLite"
    );
    let stats = progress.stats.lock().unwrap_or_else(|e| e.into_inner());
    assert!(stats.lexical_update_deferred);
    assert_eq!(stats.quarantined_conversations, 0);
}

#[test]
#[serial]
fn streaming_consumer_quarantines_single_non_watch_oom_and_dedupes_record() {
    let _oom_guard = inject_non_watch_ingest_test_oom(1);
    // #298: this test models a *genuinely* poisoned conversation, so the
    // small-conversation plausibility gate must be disabled — otherwise a
    // tiny fixture conversation is deferred instead of quarantined.
    let _small_gate_guard = set_env("CASS_WATCH_OOM_SMALL_CONVERSATION_BYTES", "0");
    // #364: quarantine now additionally requires *real* host memory
    // pressure; pin the probe to "always real pressure" so the poison
    // path stays reachable deterministically on any host.
    let _pressure_guard = set_env(
        "CASS_WATCH_OOM_REAL_PRESSURE_RESERVE_BYTES",
        "18446744073709551615",
    );
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    let progress = Arc::new(IndexingProgress::default());
    let conv = norm_conv(Some("poison-single"), vec![norm_msg(0, 1_700_000_000_000)]);

    for expected_attempts in [1_u64, 2] {
        let mut index = TantivyIndex::open_or_create(&index_dir(&data_dir).unwrap()).unwrap();
        let (tx, rx) = bounded(STREAMING_CHANNEL_SIZE);
        send_conversation_batches(&tx, "codex", vec![conv.clone()], true);
        send_done(&tx, "codex", true);
        drop(tx);

        let (_discovered, outcome) = run_streaming_consumer(
            rx,
            1,
            &storage,
            &data_dir,
            Some(&mut index),
            Arc::new(StreamingByteLimiter::new(STREAMING_MAX_BYTES_IN_FLIGHT)),
            &Some(progress.clone()),
            LexicalPopulationStrategy::IncrementalInline,
            true,
            Some(FrankenStorage::now_millis()),
            None,
        )
        .expect("single deferred ingest OOM should quarantine and continue");

        assert_eq!(outcome.inserted_conversations, 0);
        assert_eq!(outcome.inserted_messages, 0);
        assert!(outcome.lexical_update_deferred);
        assert_eq!(outcome.quarantined_conversations, 1);
        assert!(outcome.scan_had_errors);
        assert!(outcome.scanned_connectors.is_empty());
        assert_eq!(storage.get_connector_last_scan_ts("codex").unwrap(), None);

        let quarantine_path = data_dir.join("quarantine/index_ingest_poison.jsonl");
        let contents = std::fs::read_to_string(&quarantine_path).unwrap();
        let lines: Vec<_> = contents
            .lines()
            .filter(|line| !line.trim().is_empty())
            .collect();
        assert_eq!(
            lines.len(),
            1,
            "poison record must be upserted, not appended"
        );
        let record: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(record["reason"], "index-ingest-out-of-memory");
        assert_eq!(
            record["attempt_count"],
            serde_json::json!(expected_attempts)
        );
        assert_eq!(record["external_id"], serde_json::json!("poison-single"));

        let structured_state = QuarantineState::load(&data_dir);
        assert_eq!(structured_state.len(), 1);
        let structured_attempt_count = structured_state
            .iter()
            .next()
            .map(|(_, record)| record.attempt_count);
        assert_eq!(structured_attempt_count, Some(expected_attempts));

        let summary = conversation_ingest_quarantine_summary(&data_dir);
        assert_eq!(summary.quarantined_conversations, 1);
        assert_eq!(summary.status, "degraded");
    }
}
