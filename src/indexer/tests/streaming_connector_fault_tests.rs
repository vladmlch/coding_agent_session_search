use super::super::*;
use super::helpers::*;
use crate::connectors::NormalizedMessage;
use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use serial_test::serial;
use tempfile::TempDir;

#[test]
fn lexical_rebuild_packet_producer_respects_planned_shard_boundaries() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("producer-planned-shards.db");
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
    for suffix in 3..=6 {
        let external_id = format!("lexical-fixture-{suffix}");
        let base_ts = 1_700_000_000_000_i64 + i64::from(suffix) * 1_000;
        storage
            .insert_conversation_tree(
                agent_id,
                None,
                &Conversation {
                    id: None,
                    agent_slug: "codex".into(),
                    workspace: Some(PathBuf::from("/tmp/workspace")),
                    external_id: Some(external_id.clone()),
                    title: Some("Lexical rebuild fixture".into()),
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

    let planned_settings = LexicalRebuildPipelineSettingsSnapshot {
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
        steady_batch_fetch_conversations: 3,
        startup_batch_fetch_conversations: 3,
        steady_commit_every_conversations: 3,
        startup_commit_every_conversations: 3,
        steady_commit_every_messages: 6,
        startup_commit_every_messages: 6,
        steady_commit_every_message_bytes: 65_536,
        startup_commit_every_message_bytes: 65_536,
        pipeline_channel_size: 2,
        page_prep_workers: 1,
        pipeline_max_message_bytes_in_flight: 256 * 1024,
    };
    let storage = FrankenStorage::open_readonly(&db_path).unwrap();
    let planned_shard_plan =
        plan_lexical_rebuild_shards_from_storage_with_settings(&storage, &planned_settings, 6)
            .unwrap();
    assert_eq!(
        planned_shard_plan
            .shards
            .iter()
            .map(|shard| shard.conversation_count)
            .collect::<Vec<_>>(),
        vec![3, 3]
    );
    storage.close_without_checkpoint().unwrap();

    let (tx, rx) = bounded::<LexicalRebuildPipelineMessage>(2);
    // This test exercises exact shard boundaries, so inject a fixed
    // budget. The runtime helper applies live machine pressure and can
    // reduce the requested three-conversation page to a smaller page.
    let pipeline_budget = LexicalRebuildPipelineBudgetSnapshot::new(
        64,
        256,
        3 * 8 * 1024 * 1024,
        9 * 8 * 1024 * 1024,
        64,
        256,
        3 * 8 * 1024 * 1024,
    );
    let flow_limiter = Arc::new(StreamingByteLimiter::new(
        pipeline_budget.max_message_bytes_in_flight,
    ));
    let handle = spawn_lexical_rebuild_packet_producer(
        db_path,
        None,
        Some(planned_shard_plan),
        LEXICAL_REBUILD_PAGE_SIZE,
        2,
        None,
        Arc::new(LexicalRebuildPipelineBudgetController::new(pipeline_budget)),
        tx,
        flow_limiter.clone(),
        None,
        Arc::new(LexicalRebuildProducerTelemetry::default()),
    );

    let mut observed_pages = Vec::new();
    loop {
        match rx.recv_timeout(Duration::from_secs(10)).unwrap() {
            LexicalRebuildPipelineMessage::Batch(batch) => {
                assert_eq!(
                    batch.planned_shard_index,
                    Some(observed_pages.len()),
                    "each emitted page should stay tagged with its deterministic shard index"
                );
                assert!(
                    batch.finishes_planned_shard,
                    "each page in this fixture should close exactly one planned shard"
                );
                observed_pages.push(
                    batch
                        .packets
                        .iter()
                        .map(|packet| {
                            packet
                                .identity
                                .external_id
                                .clone()
                                .expect("fixture conversation has external id")
                        })
                        .collect::<Vec<_>>(),
                );
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
        observed_pages,
        vec![
            vec![
                "lexical-fixture-1".to_string(),
                "lexical-fixture-2".to_string(),
                "lexical-fixture-3".to_string(),
            ],
            vec![
                "lexical-fixture-4".to_string(),
                "lexical-fixture-5".to_string(),
                "lexical-fixture-6".to_string(),
            ],
        ]
    );
    assert_eq!(flow_limiter.bytes_in_flight(), 0);
}

#[test]
#[serial]
fn lexical_rebuild_packet_producer_preserves_order_across_parallel_page_prep_workers() {
    let _responsiveness = set_env("CASS_RESPONSIVENESS_DISABLE", "1");
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("producer-ordered-workers.db");
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
    for suffix in 3..=6 {
        let external_id = format!("lexical-fixture-{suffix}");
        let base_ts = 1_700_000_000_000_i64 + i64::from(suffix) * 1_000;
        storage
            .insert_conversation_tree(
                agent_id,
                None,
                &Conversation {
                    id: None,
                    agent_slug: "codex".into(),
                    workspace: Some(PathBuf::from("/tmp/workspace")),
                    external_id: Some(external_id.clone()),
                    title: Some("Lexical rebuild fixture".into()),
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

    let _page_prep_workers = set_env("CASS_TANTIVY_REBUILD_PAGE_PREP_WORKERS", "2");
    let (tx, rx) = bounded::<LexicalRebuildPipelineMessage>(2);
    let flow_limiter = Arc::new(StreamingByteLimiter::new(256 * 1024));
    let producer_telemetry = Arc::new(LexicalRebuildProducerTelemetry::default());
    let handle = spawn_lexical_rebuild_packet_producer(
        db_path,
        None,
        None,
        LEXICAL_REBUILD_PAGE_SIZE,
        2,
        None,
        Arc::new(LexicalRebuildPipelineBudgetController::new(
            lexical_rebuild_runtime_pipeline_budget_snapshot(1, 32, 64 * 1024, 2, 1, 32, 64 * 1024),
        )),
        tx,
        flow_limiter.clone(),
        None,
        producer_telemetry.clone(),
    );

    let mut observed_external_ids = Vec::new();
    loop {
        match rx.recv_timeout(Duration::from_secs(10)).unwrap() {
            LexicalRebuildPipelineMessage::Batch(batch) => {
                assert_eq!(batch.packets.len(), 1);
                observed_external_ids.push(
                    batch.packets[0]
                        .identity
                        .external_id
                        .clone()
                        .expect("fixture conversation has external id"),
                );
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
        observed_external_ids,
        vec![
            "lexical-fixture-1",
            "lexical-fixture-2",
            "lexical-fixture-3",
            "lexical-fixture-4",
            "lexical-fixture-5",
            "lexical-fixture-6",
        ]
    );
    assert_eq!(
        flow_limiter.bytes_in_flight(),
        0,
        "ordered worker handoff must leave byte reservations owned and released by the sink"
    );
    let telemetry = producer_telemetry.snapshot();
    assert_eq!(telemetry.page_prep_workers, 2);
    assert_eq!(telemetry.active_page_prep_jobs, 0);
    assert_eq!(telemetry.ordered_buffered_pages, 0);
}

#[test]
fn next_streaming_batch_splits_large_message_batches() {
    let limits = StreamingBatchLimits {
        max_conversations: 8,
        max_messages: 1_000,
        max_chars: usize::MAX,
    };
    let convs = vec![
        norm_conv(
            Some("a"),
            (0..700).map(|i| norm_msg(i, 1_000 + i)).collect(),
        ),
        norm_conv(
            Some("b"),
            (0..400).map(|i| norm_msg(i, 2_000 + i)).collect(),
        ),
        norm_conv(
            Some("c"),
            (0..300).map(|i| norm_msg(i, 3_000 + i)).collect(),
        ),
    ];

    let mut iter = convs.into_iter().peekable();
    let (batch1, batch1_messages) = next_streaming_batch(&mut iter, limits).unwrap();
    let (batch2, batch2_messages) = next_streaming_batch(&mut iter, limits).unwrap();

    assert_eq!(
        batch1
            .iter()
            .map(|conv| conv.external_id.as_deref().unwrap())
            .collect::<Vec<_>>(),
        vec!["a"]
    );
    assert_eq!(batch1_messages, 700);

    assert_eq!(
        batch2
            .iter()
            .map(|conv| conv.external_id.as_deref().unwrap())
            .collect::<Vec<_>>(),
        vec!["b", "c"]
    );
    assert_eq!(batch2_messages, 700);
    assert!(next_streaming_batch(&mut iter, limits).is_none());
}

#[test]
fn next_streaming_batch_keeps_single_oversized_conversation_isolated() {
    let limits = StreamingBatchLimits {
        max_conversations: 8,
        max_messages: 8,
        max_chars: 64,
    };
    let oversized = NormalizedMessage {
        content: "x".repeat(256),
        ..norm_msg(0, 1_000)
    };
    let convs = vec![
        norm_conv(Some("huge"), vec![oversized]),
        norm_conv(Some("small"), vec![norm_msg(0, 2_000)]),
    ];

    let mut iter = convs.into_iter().peekable();
    let (batch1, batch1_messages) = next_streaming_batch(&mut iter, limits).unwrap();
    let (batch2, batch2_messages) = next_streaming_batch(&mut iter, limits).unwrap();

    assert_eq!(
        batch1[0].external_id.as_deref(),
        Some("huge"),
        "oversized conversations should still index, but alone"
    );
    assert_eq!(batch1_messages, 1);
    assert_eq!(batch2[0].external_id.as_deref(), Some("small"));
    assert_eq!(batch2_messages, 1);
    assert!(next_streaming_batch(&mut iter, limits).is_none());
}

#[test]
fn streaming_batch_sender_flushes_single_oversized_conversation_immediately() {
    let (tx, rx) = bounded(2);
    let mut sender = StreamingBatchSender::new(
        &tx,
        Arc::new(StreamingByteLimiter::new(STREAMING_MAX_BYTES_IN_FLIGHT)),
        "gemini",
        false,
    );
    let oversized = NormalizedMessage {
        content: "x".repeat(DEFAULT_STREAMING_BATCH_LIMITS.max_chars + 1),
        ..norm_msg(0, 1_000)
    };
    let conversation = norm_conv(Some("huge"), vec![oversized]);
    let expected_reservation = conversation_batch_footprint(&conversation).retained_bytes;

    sender
        .push(conversation)
        .expect("oversized conversation should still flush even in tests");

    match rx
        .try_recv()
        .expect("oversized conversation should flush immediately")
    {
        IndexMessage::Batch {
            connector_name,
            conversations,
            message_count,
            byte_reservation,
            ..
        } => {
            assert_eq!(connector_name, "gemini");
            assert_eq!(conversations.len(), 1);
            assert_eq!(conversations[0].external_id.as_deref(), Some("huge"));
            assert_eq!(message_count, 1);
            assert_eq!(
                byte_reservation, expected_reservation,
                "the limiter must charge the full retained object graph, not content alone"
            );
        }
        other => panic!(
            "expected batch for oversized conversation flush, got {:?}",
            std::mem::discriminant(&other)
        ),
    }

    assert!(
        rx.try_recv().is_err(),
        "sender buffer should be empty after auto-flush"
    );
    sender.flush().unwrap();
    assert!(rx.try_recv().is_err(), "explicit flush should be a no-op");
}

#[test]
fn streaming_batch_sender_accounts_pending_conversation_bytes_before_flush() {
    let (tx, rx) = bounded(2);
    let limiter = Arc::new(StreamingByteLimiter::new(STREAMING_MAX_BYTES_IN_FLIGHT));
    let mut sender = StreamingBatchSender::new(&tx, limiter.clone(), "codex", true);
    let content = "x".repeat(512);
    let conversation = norm_conv(
        Some("pending"),
        vec![NormalizedMessage {
            content,
            ..norm_msg(0, 1_000)
        }],
    );
    let expected_bytes = conversation_batch_footprint(&conversation).retained_bytes;

    sender
        .push(conversation)
        .expect("pending conversation should fit in the streaming budget");

    assert_eq!(
        limiter.bytes_in_flight(),
        expected_bytes,
        "producer-side pending batches must be counted before flush"
    );
    assert!(
        rx.try_recv().is_err(),
        "non-oversized conversation should remain pending until flush"
    );

    sender.flush().unwrap();
    match rx.try_recv().expect("flush should publish pending batch") {
        IndexMessage::Batch {
            connector_name,
            conversations,
            message_count,
            byte_reservation,
            ..
        } => {
            assert_eq!(connector_name, "codex");
            assert_eq!(conversations.len(), 1);
            assert_eq!(conversations[0].external_id.as_deref(), Some("pending"));
            assert_eq!(message_count, 1);
            assert_eq!(byte_reservation, expected_bytes);
            assert_eq!(limiter.bytes_in_flight(), expected_bytes);
            limiter.release(byte_reservation);
        }
        other => panic!(
            "expected batch for pending conversation flush, got {:?}",
            std::mem::discriminant(&other)
        ),
    }
    assert_eq!(limiter.bytes_in_flight(), 0);
}

#[test]
fn gh320_streaming_batch_limiter_charges_metadata_and_structural_overhead() {
    let (tx, rx) = bounded(2);
    let limiter = Arc::new(StreamingByteLimiter::new(STREAMING_MAX_BYTES_IN_FLIGHT));
    let mut sender = StreamingBatchSender::new(&tx, limiter.clone(), "codex", false);
    let mut conversation = norm_conv(Some("metadata-heavy"), vec![norm_msg(0, 1_000)]);
    conversation.metadata = serde_json::json!({
        "nested": [{"blob": "m".repeat(4096)}],
    });
    conversation.messages[0].content.clear();
    conversation.messages[0].extra = serde_json::json!({
        "tool_result": "e".repeat(8192),
    });
    conversation.messages[0].snippets.push(NormalizedSnippet {
        file_path: Some(PathBuf::from("/tmp/large-snippet.rs")),
        start_line: Some(1),
        end_line: Some(2),
        language: Some("rust".into()),
        snippet_text: Some("s".repeat(2048)),
    });

    let footprint = conversation_batch_footprint(&conversation);
    assert_eq!(footprint.content_bytes, 0);
    assert!(
        footprint.retained_bytes >= 14 * 1024,
        "metadata/extra/snippet allocations must be visible even with empty message content: {footprint:?}"
    );

    sender.push(conversation).unwrap();
    assert_eq!(limiter.bytes_in_flight(), footprint.retained_bytes);
    sender.flush().unwrap();
    let message = rx.try_recv().expect("flushed metadata-heavy batch");
    let IndexMessage::Batch {
        byte_reservation, ..
    } = message
    else {
        panic!("expected a streamed batch")
    };
    assert_eq!(byte_reservation, footprint.retained_bytes);
    limiter.release(byte_reservation);
    assert_eq!(limiter.bytes_in_flight(), 0);
}

#[test]
fn gh320_lexical_page_reservation_charges_preparation_working_set() {
    let content_bytes = 1_024;
    let messages = 10;
    let conversations = 2;
    let reserved =
        lexical_rebuild_page_working_set_reservation_bytes(content_bytes, messages, conversations);
    assert!(
        reserved > content_bytes,
        "reservation must include owned content plus overlapping message/conversation structures"
    );

    let empty_content_reservation = lexical_rebuild_page_working_set_reservation_bytes(0, 1_000, 1);
    assert!(
        empty_content_reservation > 1_000,
        "one thousand empty messages still retain a material object graph"
    );
    assert_eq!(
        lexical_rebuild_page_working_set_reservation_bytes(usize::MAX, usize::MAX, usize::MAX),
        usize::MAX,
        "overflow must saturate rather than wrapping below the configured cap"
    );
}

#[test]
fn streaming_batch_sender_drop_releases_unflushed_reservation() {
    let (tx, rx) = bounded(2);
    let limiter = Arc::new(StreamingByteLimiter::new(STREAMING_MAX_BYTES_IN_FLIGHT));
    let content = "x".repeat(384);
    let conversation = norm_conv(
        Some("unflushed"),
        vec![NormalizedMessage {
            content,
            ..norm_msg(0, 1_000)
        }],
    );
    let expected_bytes = conversation_batch_footprint(&conversation).retained_bytes;

    {
        let mut sender = StreamingBatchSender::new(&tx, limiter.clone(), "cursor", false);
        sender
            .push(conversation)
            .expect("pending conversation should reserve bytes");

        assert_eq!(limiter.bytes_in_flight(), expected_bytes);
    }

    assert_eq!(
        limiter.bytes_in_flight(),
        0,
        "dropping an unflushed producer must not leak byte budget"
    );
    assert!(
        rx.try_recv().is_err(),
        "drop must not publish a partial batch"
    );
}

#[test]
fn streaming_batch_sender_send_failure_releases_preacquired_reservation() {
    let (tx, rx) = bounded(1);
    drop(rx);
    let limiter = Arc::new(StreamingByteLimiter::new(STREAMING_MAX_BYTES_IN_FLIGHT));
    let mut sender = StreamingBatchSender::new(&tx, limiter.clone(), "amp", false);
    let content = "x".repeat(256);
    let conversation = norm_conv(
        Some("disconnected"),
        vec![NormalizedMessage {
            content,
            ..norm_msg(0, 1_000)
        }],
    );
    let expected_bytes = conversation_batch_footprint(&conversation).retained_bytes;

    sender
        .push(conversation)
        .expect("push should only reserve bytes before flush");
    assert_eq!(limiter.bytes_in_flight(), expected_bytes);

    let error = sender
        .flush()
        .expect_err("closed receiver should fail flush");
    assert!(is_streaming_consumer_disconnected(&error));
    assert_eq!(
        limiter.bytes_in_flight(),
        0,
        "flush failure must release the reservation exactly once"
    );
}

#[test]
fn streaming_byte_limiter_blocks_until_capacity_is_released() {
    let limiter = Arc::new(StreamingByteLimiter::new(64));
    let first = limiter.acquire(128).unwrap();
    let (ready_tx, ready_rx) = bounded(1);
    let (result_tx, result_rx) = bounded(1);
    let waiter = {
        let limiter = limiter.clone();
        thread::spawn(move || {
            ready_tx.send(()).unwrap();
            let second = limiter.acquire(32).unwrap();
            result_tx.send(second).unwrap();
            limiter.release(second);
        })
    };

    ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(
        result_rx.try_recv().is_err(),
        "waiter should remain blocked while the limiter is full"
    );

    limiter.release(first);
    assert_eq!(result_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 32);
    waiter.join().unwrap();
}

#[test]
fn streaming_byte_limiter_reports_active_capacity_waiter() {
    let limiter = Arc::new(StreamingByteLimiter::new(64));
    let first = limiter.acquire(64).unwrap();
    let (ready_tx, ready_rx) = bounded(1);
    let (result_tx, result_rx) = bounded(1);
    let waiter = {
        let limiter = limiter.clone();
        thread::spawn(move || {
            ready_tx.send(()).unwrap();
            let second = limiter.acquire(1).unwrap();
            result_tx.send(second).unwrap();
            limiter.release(second);
        })
    };

    ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    for _ in 0..100 {
        if limiter.active_waiter_count() > 0 {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        limiter.active_waiter_count(),
        1,
        "a producer parked on byte capacity must be visible before the wait completes"
    );

    limiter.release(first);
    assert_eq!(result_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 1);
    waiter.join().unwrap();
    assert_eq!(
        limiter.active_waiter_count(),
        0,
        "active waiter count must clear after the producer wakes"
    );
}

#[test]
fn streaming_byte_limiter_close_wakes_waiters() {
    let limiter = Arc::new(StreamingByteLimiter::new(64));
    let first = limiter.acquire(64).unwrap();
    let (ready_tx, ready_rx) = bounded(1);
    let (result_tx, result_rx) = bounded(1);
    let waiter = {
        let limiter = limiter.clone();
        thread::spawn(move || {
            ready_tx.send(()).unwrap();
            let result = limiter.acquire(1).map_err(|error| error.to_string());
            result_tx.send(result).unwrap();
        })
    };

    ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(
        result_rx.try_recv().is_err(),
        "waiter should remain blocked until the limiter is closed"
    );
    for _ in 0..100 {
        if limiter.active_waiter_count() > 0 {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        limiter.active_waiter_count(),
        1,
        "waiter should be counted while blocked before close"
    );

    limiter.close();
    let error = result_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .expect_err("closing the limiter should wake blocked waiters with an error");
    assert!(error.contains("closed"));
    limiter.release(first);
    waiter.join().unwrap();
    assert_eq!(
        limiter.active_waiter_count(),
        0,
        "active waiter count must clear when close wakes the blocked producer"
    );
}

#[test]
fn streaming_byte_limiter_update_max_bytes_in_flight_wakes_waiters() {
    let limiter = Arc::new(StreamingByteLimiter::new(64));
    let first = limiter.acquire(64).unwrap();
    let (ready_tx, ready_rx) = bounded(1);
    let (result_tx, result_rx) = bounded(1);
    let waiter = {
        let limiter = limiter.clone();
        thread::spawn(move || {
            ready_tx.send(()).unwrap();
            let second = limiter.acquire(64).unwrap();
            result_tx.send(second).unwrap();
            limiter.release(second);
        })
    };

    ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(
        result_rx.try_recv().is_err(),
        "waiter should remain blocked while the limiter is full at the startup cap"
    );

    limiter.update_max_bytes_in_flight(128);
    assert_eq!(limiter.max_bytes_in_flight(), 128);
    assert_eq!(result_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 64);
    limiter.release(first);
    waiter.join().unwrap();
}

#[test]
fn lexical_rebuild_reservation_order_keeps_later_pages_from_spending_budget_first() {
    let order = Arc::new(LexicalRebuildReservationOrder::new());
    let limiter = Arc::new(StreamingByteLimiter::new(64));
    let telemetry = Arc::new(LexicalRebuildProducerTelemetry::default());
    let (ready_tx, ready_rx) = bounded(1);
    let (result_tx, result_rx) = bounded(1);
    let waiter = {
        let order = order.clone();
        let limiter = limiter.clone();
        let telemetry = telemetry.clone();
        thread::spawn(move || {
            ready_tx.send(()).unwrap();
            let (reserved, _, _) =
                acquire_ordered_lexical_rebuild_page_budget(&order, &limiter, &telemetry, 1, 64)
                    .unwrap();
            result_tx.send(reserved).unwrap();
        })
    };

    ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(
        result_rx.recv_timeout(Duration::from_millis(50)).is_err(),
        "sequence 1 must wait even though the byte limiter is empty"
    );
    assert_eq!(
        limiter.bytes_in_flight(),
        0,
        "later pages must not reserve bytes before earlier sequences"
    );
    let waiting_turn_deadline = Instant::now() + Duration::from_secs(2);
    while telemetry.snapshot().producer_state != LexicalRebuildProducerParkSite::WaitingTurn as u8
        && Instant::now() < waiting_turn_deadline
    {
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(
        telemetry.snapshot().producer_state,
        LexicalRebuildProducerParkSite::WaitingTurn as u8,
        "a parked ordered reservation must publish the waiting_turn park site (#288)"
    );

    let (first, _, _) =
        acquire_ordered_lexical_rebuild_page_budget(&order, &limiter, &telemetry, 0, 1).unwrap();
    assert_eq!(first, 1);
    assert_eq!(limiter.bytes_in_flight(), 1);
    assert!(
        result_rx.recv_timeout(Duration::from_millis(50)).is_err(),
        "sequence 1 should still respect normal byte capacity after order opens"
    );

    limiter.release(first);
    assert_eq!(result_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 64);
    limiter.release(64);
    waiter.join().unwrap();
    assert_eq!(
        telemetry.snapshot().producer_state,
        LexicalRebuildProducerParkSite::Idle as u8,
        "a completed ordered acquisition must settle the park site back to idle"
    );
    assert_eq!(
        telemetry.snapshot().reservation_next_sequence,
        2,
        "the reservation barrier mirror must track the next admissible sequence"
    );
}

/// #288 regression: a panic inside a page-prep worker must surface as a
/// `LexicalRebuildPagePrepResult::Error` on the result channel. Before
/// the catch_unwind containment, the panicking thread unwound silently,
/// the surviving workers kept the result channel open, and the producer
/// parked forever at `result_rx.recv()` — exactly the reporter's
/// all-parked thread dump (queue_depth=0, active_page_prep_jobs=0,
/// inflight=0).
/// 2l1b0.70: an armed injection fires only for work inside its scope and
/// only once. With the former process-global flag, a rebuild in another
/// test (a different temp directory) consumed it.
/// bgn6s: a page-prep worker streaming a long conversation waits for the
/// consumer to flush its previous chunk. A flush releases it; a torn-down
/// pipeline (consumer failure closes the flow limiter, producer failure
/// closes the reservation order) releases it with an error instead of
/// parking it forever.
#[test]
fn chunk_gate_waits_for_the_flush_and_fails_once_the_pipeline_closes() {
    let flow_limiter = Arc::new(StreamingByteLimiter::new(1024));
    let reservation_order = Arc::new(LexicalRebuildReservationOrder::new());

    let gate = Arc::new(LexicalRebuildChunkGate::default());
    gate.wait_until_flushed(&reservation_order, &flow_limiter)
        .expect("nothing outstanding");
    gate.sent();
    let flusher = {
        let gate = Arc::clone(&gate);
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            gate.flushed();
        })
    };
    gate.wait_until_flushed(&reservation_order, &flow_limiter)
        .expect("the consumer flushed the outstanding chunk");
    flusher.join().unwrap();

    for close_limiter in [true, false] {
        let flow_limiter = Arc::new(StreamingByteLimiter::new(1024));
        let reservation_order = Arc::new(LexicalRebuildReservationOrder::new());
        let gate = Arc::new(LexicalRebuildChunkGate::default());
        gate.sent();
        let waiter = {
            let gate = Arc::clone(&gate);
            let flow_limiter = Arc::clone(&flow_limiter);
            let reservation_order = Arc::clone(&reservation_order);
            std::thread::spawn(move || gate.wait_until_flushed(&reservation_order, &flow_limiter))
        };
        std::thread::sleep(Duration::from_millis(50));
        assert!(
            !waiter.is_finished(),
            "an unflushed chunk must hold the worker"
        );
        if close_limiter {
            flow_limiter.close();
        } else {
            reservation_order.close();
        }
        let started = Instant::now();
        while !waiter.is_finished() {
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "a closed pipeline must release the waiting worker"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let error = waiter.join().unwrap().expect_err("closed pipeline");
        assert!(
            error.to_string().contains("pipeline closed"),
            "unexpected error: {error}"
        );
    }
}

#[test]
fn scoped_panic_injection_fires_only_inside_its_scope() {
    let injection = ScopedPanicInjection::new();
    let armed = TempDir::new().unwrap();
    let other = TempDir::new().unwrap();
    injection.arm(armed.path());

    injection.fire_if_armed_for(&other.path().join("agent_search.db"), "must not fire");

    let fired = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        injection.fire_if_armed_for(&armed.path().join("agent_search.db"), "fired in scope");
    }));
    assert!(
        fired.is_err(),
        "an injection armed for this scope must fire"
    );

    // Consumed: a second worker in the same scope proceeds normally.
    injection.fire_if_armed_for(&armed.path().join("agent_search.db"), "must not fire twice");
}

#[test]
fn lexical_rebuild_failure_channels_preserve_database_and_io_causes() -> Result<()> {
    use crate::franken_sync::FrankenError;

    let causes = [
        anyhow::Error::new(FrankenError::IoRead { page: 42 }),
        anyhow::Error::new(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "PRIVATE-LEXICAL-FAILURE-PATH",
        )),
        anyhow::Error::new(FrankenError::CheckpointFailed {
            detail: "PRIVATE-LEXICAL-FAILURE-CHECKPOINT".to_owned(),
        }),
    ];
    for (case, cause) in causes.into_iter().enumerate() {
        let (worker_tx, worker_rx) = bounded(1);
        let worker = thread::spawn(move || {
            worker_tx.send(LexicalRebuildPagePrepResult::Error {
                sequence: 37,
                error: cause.context("PRIVATE-LEXICAL-FAILURE-WORKER-CONTEXT"),
            })
        });
        let response = worker_rx.recv_timeout(Duration::from_secs(10))?;
        worker
            .join()
            .map_err(|_| anyhow::anyhow!("failure fixture worker panicked"))?
            .map_err(|_| anyhow::anyhow!("failure fixture channel disconnected"))?;
        let LexicalRebuildPagePrepResult::Error { sequence, error } = response else {
            anyhow::bail!("failure fixture did not reach the page-prep dispatcher");
        };
        assert_eq!(sequence, 37);
        let error = lexical_rebuild_page_prep_error(sequence, error);
        assert!(!error.to_string().contains("PRIVATE-LEXICAL-FAILURE"));

        let (producer_tx, producer_rx) = bounded(1);
        send_lexical_rebuild_pipeline_error(&producer_tx, error);
        let LexicalRebuildPipelineMessage::Error(error) =
            producer_rx.recv_timeout(Duration::from_secs(10))?
        else {
            anyhow::bail!("failure fixture did not reach the rebuild sink");
        };
        assert_eq!(error.to_string(), "lexical rebuild packet producer failed");
        assert!(error.chain().any(|cause| {
            cause.to_string() == "lexical rebuild page-prep worker failed at sequence 37"
        }));
        match case {
            0 => assert!(matches!(
                error.downcast_ref::<FrankenError>(),
                Some(FrankenError::IoRead { page: 42 })
            )),
            1 => assert_eq!(
                error
                    .downcast_ref::<std::io::Error>()
                    .context("filesystem failure lost its typed cause")?
                    .kind(),
                std::io::ErrorKind::PermissionDenied
            ),
            2 => assert!(matches!(
                error.downcast_ref::<FrankenError>(),
                Some(FrankenError::CheckpointFailed { .. })
            )),
            _ => anyhow::bail!("unexpected failure fixture case"),
        }
    }
    Ok(())
}

#[test]
fn lexical_rebuild_page_prep_open_failure_retains_its_engine_cause() -> Result<()> {
    let root = TempDir::new()?;

    // 1. Missing path: strict read-only admission refuses without creating the archive
    // or leaking the private path into the outer error message.
    let missing_path = root.path().join("PRIVATE-MISSING-WORKER-ARCHIVE-PATH.db");
    let (work_tx, work_rx) = bounded(1);
    let (result_tx, result_rx) = bounded(1);
    let workers = spawn_lexical_rebuild_page_prep_workers(
        1,
        missing_path.clone(),
        Arc::new(HashMap::new()),
        work_rx,
        result_tx,
        Arc::new(StreamingByteLimiter::new(1024)),
        Arc::new(LexicalRebuildReservationOrder::new()),
        Arc::new(LexicalRebuildProducerTelemetry::default()),
        None,
    )?;
    let response = result_rx.recv_timeout(Duration::from_secs(30))?;
    drop(work_tx);
    for worker in workers {
        worker
            .join()
            .map_err(|_| anyhow::anyhow!("archive admission worker panicked"))?;
    }
    let LexicalRebuildPagePrepResult::Error { sequence, error } = response else {
        anyhow::bail!("missing archive did not fail page-prep admission");
    };
    assert_eq!(sequence, 0);
    assert!(error.to_string().contains("page-prep worker 0"));
    assert!(
        !error
            .to_string()
            .contains("PRIVATE-MISSING-WORKER-ARCHIVE-PATH")
    );
    assert!(
        !missing_path.exists(),
        "read-only admission cannot create an archive"
    );

    // 2. Stamped schema-v1 archive with an orphaned `-wal` (no `-shm`):
    // strict read-only admission surfaces `FrankenError::BusyRecovery` intact.
    let db_path = root.path().join("PRIVATE-WORKER-ARCHIVE-PATH.db");
    {
        let storage = FrankenStorage::open(&db_path)?;
        storage.close()?;
    }
    let wal_path = database_path_with_suffix(&db_path, "-wal");
    let shm_path = database_path_with_suffix(&db_path, "-shm");
    std::fs::write(&wal_path, vec![0_u8; 64])?;
    let _ = std::fs::remove_file(&shm_path);

    let (work_tx, work_rx) = bounded(1);
    let (result_tx, result_rx) = bounded(1);
    let workers = spawn_lexical_rebuild_page_prep_workers(
        1,
        db_path,
        Arc::new(HashMap::new()),
        work_rx,
        result_tx,
        Arc::new(StreamingByteLimiter::new(1024)),
        Arc::new(LexicalRebuildReservationOrder::new()),
        Arc::new(LexicalRebuildProducerTelemetry::default()),
        None,
    )?;
    let response = result_rx.recv_timeout(Duration::from_secs(30))?;
    drop(work_tx);
    for worker in workers {
        worker
            .join()
            .map_err(|_| anyhow::anyhow!("archive admission worker panicked"))?;
    }
    let LexicalRebuildPagePrepResult::Error { sequence, error } = response else {
        anyhow::bail!("unrecovered WAL archive did not fail strict page-prep admission");
    };
    assert_eq!(sequence, 0);
    assert!(
        matches!(
            error.downcast_ref::<crate::franken_sync::FrankenError>(),
            Some(crate::franken_sync::FrankenError::BusyRecovery)
        ),
        "page-prep admission must retain the engine BusyRecovery error type, got: {error:#}"
    );
    assert!(error.to_string().contains("page-prep worker 0"));
    assert!(!error.to_string().contains("PRIVATE-WORKER-ARCHIVE-PATH"));
    Ok(())
}
