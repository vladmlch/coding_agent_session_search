use super::super::*;
use tempfile::TempDir;

#[test]
fn assign_lexical_rebuild_flow_reservation_bytes_preserves_exact_total() {
    let make_packet = |conversation_id: i64, content: &str| {
        let messages = if content.is_empty() {
            crate::storage::sqlite::LexicalRebuildGroupedMessageRows::new()
        } else {
            vec![crate::storage::sqlite::LexicalRebuildGroupedMessageRow {
                idx: 0,
                is_tool_role: false,
                created_at: Some(1_700_000_600_000 + conversation_id),
                content: content.to_string(),
            }]
            .into_iter()
            .collect::<crate::storage::sqlite::LexicalRebuildGroupedMessageRows>()
        };
        LexicalRebuildConversationPacket::from_canonical_replay(
            crate::storage::sqlite::LexicalRebuildConversationRow {
                id: Some(conversation_id),
                agent_slug: "codex".into(),
                workspace: Some(PathBuf::from("/tmp/workspace")),
                external_id: Some(format!("reservation-{conversation_id}")),
                title: Some("Reservation packet".into()),
                source_path: PathBuf::from(format!("/tmp/reservation-{conversation_id}.jsonl")),
                started_at: Some(1_700_000_600_000),
                ended_at: Some(1_700_000_600_100),
                source_id: LOCAL_SOURCE_ID.into(),
                origin_host: None,
            },
            messages,
            Some(conversation_id),
            &HashMap::new(),
        )
    };

    let mut packets = vec![
        make_packet(1, "abcdefghij"),
        make_packet(2, ""),
        make_packet(3, "0123456789abcdefghij0123456789"),
    ];

    assign_lexical_rebuild_flow_reservation_bytes(&mut packets, 17);

    assert_eq!(
        packets
            .iter()
            .map(|packet| packet.flow_reservation_bytes)
            .sum::<usize>(),
        17
    );
    assert_eq!(packets[1].flow_reservation_bytes, 0);
    assert!(packets[2].flow_reservation_bytes >= packets[0].flow_reservation_bytes);
}

#[test]
fn streaming_batch_flow_reservation_releases_on_drop_and_is_idempotent() {
    let limiter = StreamingByteLimiter::new(16);
    let reserved = limiter.acquire(7).unwrap();
    {
        let _guard = StreamingBatchFlowReservation::new(Some(&limiter), reserved);
        assert_eq!(limiter.bytes_in_flight(), reserved);
    }
    assert_eq!(limiter.bytes_in_flight(), 0);

    let reserved = limiter.acquire(5).unwrap();
    {
        let mut guard = StreamingBatchFlowReservation::new(Some(&limiter), reserved);
        guard.release_now();
        guard.release_now();
    }
    assert_eq!(limiter.bytes_in_flight(), 0);
}

/// GH#413 deadlock geometry: the sink retains a pending batch's byte
/// reservations while idle, a page-prep worker holding the ordered
/// reservation turn parks in `acquire_with_wait`, and — because the turn
/// blocks every later page — nothing can ever refill the sink's channel.
/// The starvation flush breaks the cycle by releasing the sink's retained
/// bytes. This pins the two properties that fix depends on:
/// `has_active_waiters` observes the parked acquirer from another thread,
/// and `release` wakes it so the (budget-capped) acquisition completes.
#[test]
fn gh413_sink_release_unparks_budget_waiter_holding_the_reservation_turn() {
    use std::sync::atomic::AtomicBool;

    let limiter = Arc::new(StreamingByteLimiter::new(100));
    assert!(!limiter.has_active_waiters());
    // The idle sink retains 80 of the 100-byte budget in its pending batch.
    let sink_retained = limiter.acquire(80).unwrap();
    assert_eq!(sink_retained, 80);

    // A page-prep worker needs 50: the capped request fits the budget but
    // not the free space, so it parks.
    let acquired = Arc::new(AtomicBool::new(false));
    let worker = {
        let limiter = Arc::clone(&limiter);
        let acquired = Arc::clone(&acquired);
        thread::spawn(move || {
            let (reservation, _wait, waited) = limiter
                .acquire_with_wait(50)
                .expect("limiter stays open for the whole test");
            acquired.store(true, Ordering::SeqCst);
            assert!(waited, "the worker must have parked before acquiring");
            assert_eq!(reservation, 50);
            limiter.release(reservation);
        })
    };

    // The sink's starvation poll must be able to observe the parked
    // worker from this thread.
    let observed = (0..500).any(|_| {
        limiter.has_active_waiters() || {
            thread::sleep(Duration::from_millis(10));
            false
        }
    });
    assert!(
        observed,
        "has_active_waiters must observe the parked worker"
    );
    assert!(
        !acquired.load(Ordering::SeqCst),
        "the worker cannot acquire while the sink retains the budget"
    );

    // The starvation flush releases the retained bytes; the parked worker
    // wakes and completes.
    limiter.release(sink_retained);
    worker.join().expect("worker thread");
    assert!(acquired.load(Ordering::SeqCst));
    assert_eq!(limiter.bytes_in_flight(), 0);
    assert!(!limiter.has_active_waiters());
}

#[test]
fn gh382_flush_streamed_lexical_rebuild_batch_preserves_controller_limit_and_releases_flow_reservation_bytes()
 {
    let tmp = TempDir::new().unwrap();
    let index_path = tmp.path().join("tantivy");
    let mut t_index = TantivyIndex::open_or_create(&index_path).unwrap();
    let mut pending_batch = vec![
        LexicalRebuildConversationPacket::from_canonical_replay(
            crate::storage::sqlite::LexicalRebuildConversationRow {
                id: Some(101),
                agent_slug: "codex".into(),
                workspace: Some(PathBuf::from("/tmp/workspace")),
                external_id: Some("flush-a".into()),
                title: Some("Flush A".into()),
                source_path: PathBuf::from("/tmp/flush-a.jsonl"),
                started_at: Some(1_700_000_700_000),
                ended_at: Some(1_700_000_700_100),
                source_id: LOCAL_SOURCE_ID.into(),
                origin_host: None,
            },
            vec![crate::storage::sqlite::LexicalRebuildGroupedMessageRow {
                idx: 0,
                is_tool_role: false,
                created_at: Some(1_700_000_700_010),
                content: "alpha".into(),
            }]
            .into_iter()
            .collect::<crate::storage::sqlite::LexicalRebuildGroupedMessageRows>(),
            Some(501),
            &HashMap::new(),
        ),
        LexicalRebuildConversationPacket::from_canonical_replay(
            crate::storage::sqlite::LexicalRebuildConversationRow {
                id: Some(102),
                agent_slug: "codex".into(),
                workspace: Some(PathBuf::from("/tmp/workspace")),
                external_id: Some("flush-b".into()),
                title: Some("Flush B".into()),
                source_path: PathBuf::from("/tmp/flush-b.jsonl"),
                started_at: Some(1_700_000_700_200),
                ended_at: Some(1_700_000_700_300),
                source_id: LOCAL_SOURCE_ID.into(),
                origin_host: None,
            },
            vec![crate::storage::sqlite::LexicalRebuildGroupedMessageRow {
                idx: 0,
                is_tool_role: false,
                created_at: Some(1_700_000_700_210),
                content: "beta".into(),
            }]
            .into_iter()
            .collect::<crate::storage::sqlite::LexicalRebuildGroupedMessageRows>(),
            Some(502),
            &HashMap::new(),
        ),
    ];
    let total_messages = pending_batch
        .iter()
        .map(|packet| packet.message_count)
        .sum::<usize>();
    let total_message_bytes = pending_batch
        .iter()
        .map(|packet| packet.message_bytes)
        .sum::<usize>();
    let limiter = StreamingByteLimiter::new(total_message_bytes.max(1));
    let reserved = limiter.acquire(total_message_bytes).unwrap();
    assign_lexical_rebuild_flow_reservation_bytes(&mut pending_batch, reserved);
    assert_eq!(limiter.bytes_in_flight(), reserved);

    let mut pending_batch_message_count = total_messages;
    let mut pending_batch_message_bytes = total_message_bytes;
    let mut indexed_docs = 0usize;
    let mut messages_since_commit = 0usize;
    let mut message_bytes_since_commit = 0usize;
    let current_batch_conversation_limit = 3usize;

    flush_streamed_lexical_rebuild_batch(
        &mut pending_batch,
        &mut pending_batch_message_count,
        &mut pending_batch_message_bytes,
        Some(&limiter),
        None,
        &mut t_index,
        &mut indexed_docs,
        &mut messages_since_commit,
        &mut message_bytes_since_commit,
        current_batch_conversation_limit,
        8,
        None,
    )
    .unwrap();

    assert_eq!(limiter.bytes_in_flight(), 0);
    assert!(pending_batch.is_empty());
    assert_eq!(indexed_docs, 2);
    assert_eq!(messages_since_commit, total_messages);
    assert_eq!(message_bytes_since_commit, total_message_bytes);
    assert_eq!(
        current_batch_conversation_limit, 3,
        "a batch flush must preserve the responsiveness controller's live limit"
    );
}

#[test]
fn flush_streamed_lexical_rebuild_batch_for_planned_shard_boundary_flushes_even_below_batch_limit()
{
    let tmp = TempDir::new().unwrap();
    let index_path = tmp.path().join("tantivy");
    let mut t_index = TantivyIndex::open_or_create(&index_path).unwrap();
    let mut pending_batch = vec![
        LexicalRebuildConversationPacket::from_canonical_replay(
            crate::storage::sqlite::LexicalRebuildConversationRow {
                id: Some(201),
                agent_slug: "codex".into(),
                workspace: Some(PathBuf::from("/tmp/workspace")),
                external_id: Some("boundary-a".into()),
                title: Some("Boundary A".into()),
                source_path: PathBuf::from("/tmp/boundary-a.jsonl"),
                started_at: Some(1_700_000_800_000),
                ended_at: Some(1_700_000_800_100),
                source_id: LOCAL_SOURCE_ID.into(),
                origin_host: None,
            },
            vec![crate::storage::sqlite::LexicalRebuildGroupedMessageRow {
                idx: 0,
                is_tool_role: false,
                created_at: Some(1_700_000_800_010),
                content: "alpha".into(),
            }]
            .into_iter()
            .collect::<crate::storage::sqlite::LexicalRebuildGroupedMessageRows>(),
            Some(601),
            &HashMap::new(),
        ),
        LexicalRebuildConversationPacket::from_canonical_replay(
            crate::storage::sqlite::LexicalRebuildConversationRow {
                id: Some(202),
                agent_slug: "codex".into(),
                workspace: Some(PathBuf::from("/tmp/workspace")),
                external_id: Some("boundary-b".into()),
                title: Some("Boundary B".into()),
                source_path: PathBuf::from("/tmp/boundary-b.jsonl"),
                started_at: Some(1_700_000_800_200),
                ended_at: Some(1_700_000_800_300),
                source_id: LOCAL_SOURCE_ID.into(),
                origin_host: None,
            },
            vec![crate::storage::sqlite::LexicalRebuildGroupedMessageRow {
                idx: 0,
                is_tool_role: false,
                created_at: Some(1_700_000_800_210),
                content: "beta".into(),
            }]
            .into_iter()
            .collect::<crate::storage::sqlite::LexicalRebuildGroupedMessageRows>(),
            Some(602),
            &HashMap::new(),
        ),
    ];
    let total_messages = pending_batch
        .iter()
        .map(|packet| packet.message_count)
        .sum::<usize>();
    let total_message_bytes = pending_batch
        .iter()
        .map(|packet| packet.message_bytes)
        .sum::<usize>();
    let limiter = StreamingByteLimiter::new(total_message_bytes.max(1));
    let reserved = limiter.acquire(total_message_bytes).unwrap();
    assign_lexical_rebuild_flow_reservation_bytes(&mut pending_batch, reserved);

    let mut pending_batch_message_count = total_messages;
    let mut pending_batch_message_bytes = total_message_bytes;
    let mut indexed_docs = 0usize;
    let mut messages_since_commit = 0usize;
    let mut message_bytes_since_commit = 0usize;
    let current_batch_conversation_limit = 3usize;

    let flushed = flush_streamed_lexical_rebuild_batch_for_planned_shard_boundary(
        Some(4),
        true,
        &mut pending_batch,
        &mut pending_batch_message_count,
        &mut pending_batch_message_bytes,
        Some(&limiter),
        None,
        &mut t_index,
        &mut indexed_docs,
        &mut messages_since_commit,
        &mut message_bytes_since_commit,
        current_batch_conversation_limit,
        8,
        None,
    )
    .unwrap();

    assert!(
        flushed,
        "planned shard boundary should flush below the batch limit"
    );
    assert_eq!(limiter.bytes_in_flight(), 0);
    assert!(pending_batch.is_empty());
    assert_eq!(indexed_docs, 2);
    assert_eq!(messages_since_commit, total_messages);
    assert_eq!(message_bytes_since_commit, total_message_bytes);
    assert_eq!(
        current_batch_conversation_limit, 3,
        "shard-boundary flush should not silently promote the active batch limit"
    );
}

#[test]
// Asserts the staged-shard rebuild strategy, which builds N independent
// index directories and merges them. That merge is Tantivy-only (Quill's
// concat_merge joins segments WITHIN one manifest), so the strategy is
// disabled for the Quill backend — see the `staged_shard_plan` binding.
// Ignored rather than deleted: these are exactly the tests that must pass
// if an N-directory merge is ever built, and weakening them would hide
// that the strategy no longer runs.
#[ignore = "staged-shard strategy is Tantivy-only; disabled on the Quill backend"]
fn lexical_rebuild_shard_indices_can_be_built_and_merged_from_packet_batches() {
    let tmp = TempDir::new().unwrap();
    let shard_a_path = tmp.path().join("shard-a");
    let shard_b_path = tmp.path().join("shard-b");
    let merged_path = tmp.path().join("merged");
    let make_packet =
        |conversation_id: i64, external_id: &str, message_a: &str, message_b: &str| {
            LexicalRebuildConversationPacket::from_canonical_replay(
                crate::storage::sqlite::LexicalRebuildConversationRow {
                    id: Some(conversation_id),
                    agent_slug: "codex".into(),
                    workspace: Some(PathBuf::from("/tmp/workspace")),
                    external_id: Some(external_id.into()),
                    title: Some(format!("Shard {external_id}")),
                    source_path: PathBuf::from(format!("/tmp/{external_id}.jsonl")),
                    started_at: Some(1_700_000_900_000 + conversation_id),
                    ended_at: Some(1_700_000_900_100 + conversation_id),
                    source_id: LOCAL_SOURCE_ID.into(),
                    origin_host: None,
                },
                vec![
                    crate::storage::sqlite::LexicalRebuildGroupedMessageRow {
                        idx: 0,
                        is_tool_role: false,
                        created_at: Some(1_700_000_900_010 + conversation_id),
                        content: message_a.into(),
                    },
                    crate::storage::sqlite::LexicalRebuildGroupedMessageRow {
                        idx: 1,
                        is_tool_role: false,
                        created_at: Some(1_700_000_900_020 + conversation_id),
                        content: message_b.into(),
                    },
                ]
                .into_iter()
                .collect::<crate::storage::sqlite::LexicalRebuildGroupedMessageRows>(),
                Some(conversation_id * 10),
                &HashMap::new(),
            )
        };

    let shard_a = vec![
        make_packet(1, "segment-a1", "alpha-1", "alpha-2"),
        make_packet(2, "segment-a2", "alpha-3", "alpha-4"),
    ];
    let shard_b = vec![
        make_packet(3, "segment-b1", "beta-1", "beta-2"),
        make_packet(4, "segment-b2", "beta-3", "beta-4"),
    ];

    assert_eq!(
        build_lexical_rebuild_shard_index(&shard_a_path, &shard_a, None).unwrap(),
        4
    );
    assert_eq!(
        build_lexical_rebuild_shard_index(&shard_b_path, &shard_b, None).unwrap(),
        4
    );

    let merged_index = crate::search::tantivy::TantivyIndex::merge_compatible_index_directories(
        &merged_path,
        &[&shard_a_path, &shard_b_path],
    )
    .unwrap();
    let reader = merged_index.reader().unwrap();
    crate::search::quill_bridge::refresh_reader(&reader).unwrap();
    assert_eq!(reader.doc_count().expect("doc count"), 8);
    assert_eq!(
        merged_index.segment_count(),
        1,
        "merged shard indices should collapse into a single final segment"
    );
}

#[test]
fn lexical_rebuild_validates_built_shard_before_merge_frontier() {
    let tmp = TempDir::new().unwrap();
    let shard_path = tmp.path().join("validated-shard");
    let packet = LexicalRebuildConversationPacket::from_canonical_replay(
        crate::storage::sqlite::LexicalRebuildConversationRow {
            id: Some(7),
            agent_slug: "codex".into(),
            workspace: Some(PathBuf::from("/tmp/workspace")),
            external_id: Some("validated-shard".into()),
            title: Some("Validated shard".into()),
            source_path: PathBuf::from("/tmp/validated-shard.jsonl"),
            started_at: Some(1_700_000_935_000),
            ended_at: Some(1_700_000_935_100),
            source_id: LOCAL_SOURCE_ID.into(),
            origin_host: None,
        },
        vec![
            crate::storage::sqlite::LexicalRebuildGroupedMessageRow {
                idx: 0,
                is_tool_role: false,
                created_at: Some(1_700_000_935_010),
                content: "validated alpha".into(),
            },
            crate::storage::sqlite::LexicalRebuildGroupedMessageRow {
                idx: 1,
                is_tool_role: false,
                created_at: Some(1_700_000_935_020),
                content: "validated beta".into(),
            },
        ]
        .into_iter()
        .collect::<crate::storage::sqlite::LexicalRebuildGroupedMessageRows>(),
        Some(70),
        &HashMap::new(),
    );
    let indexed_docs = build_lexical_rebuild_shard_index(&shard_path, &[packet], None).unwrap();
    let shard = LexicalShardPlanShard {
        shard_index: 0,
        first_conversation_id: 7,
        last_conversation_id: 7,
        conversation_count: 1,
        message_count: 2,
        message_bytes: "validated alpha".len() + "validated beta".len(),
        conversation_id_fingerprint: lexical_shard_conversation_ids_fingerprint(&[7]),
        oversized_single_conversation: false,
    };
    let result = LexicalRebuildShardBuildResult {
        shard: shard.clone(),
        indexed_docs,
        segments: 1,
        shard_index_path: shard_path.clone(),
        message_bytes: shard.message_bytes,
        index_size_bytes: 0,
        build_duration_ms: 0,
        amplification_milli: None,
    };

    let artifact = validate_lexical_rebuild_shard_build_result(&result).unwrap();

    assert_eq!(artifact.first_shard_index, 0);
    assert_eq!(artifact.last_shard_index, 0);
    assert_eq!(artifact.index_path, shard_path);
    validate_complete_lexical_rebuild_shard_artifacts(
        &LexicalShardPlan {
            planner_version: LEXICAL_SHARD_PLAN_VERSION,
            plan_id: "validated-plan".into(),
            budgets: LexicalShardPlannerBudgets {
                max_conversations_per_shard: 1,
                max_messages_per_shard: 2,
                max_message_bytes_per_shard: 10_000,
            },
            total_conversations: 1,
            total_messages: 2,
            total_message_bytes: "validated alpha".len() + "validated beta".len(),
            oversized_conversation_ids: Vec::new(),
            shards: vec![shard],
        },
        &[artifact],
    )
    .unwrap();
}

#[test]
fn lexical_rebuild_rejects_shard_doc_count_mismatch_before_merge_frontier() {
    let tmp = TempDir::new().unwrap();
    let shard_path = tmp.path().join("mismatched-shard");
    let packet = LexicalRebuildConversationPacket::from_canonical_replay(
        crate::storage::sqlite::LexicalRebuildConversationRow {
            id: Some(8),
            agent_slug: "codex".into(),
            workspace: Some(PathBuf::from("/tmp/workspace")),
            external_id: Some("mismatched-shard".into()),
            title: Some("Mismatched shard".into()),
            source_path: PathBuf::from("/tmp/mismatched-shard.jsonl"),
            started_at: Some(1_700_000_936_000),
            ended_at: Some(1_700_000_936_100),
            source_id: LOCAL_SOURCE_ID.into(),
            origin_host: None,
        },
        vec![
            crate::storage::sqlite::LexicalRebuildGroupedMessageRow {
                idx: 0,
                is_tool_role: false,
                created_at: Some(1_700_000_936_010),
                content: "mismatch alpha".into(),
            },
            crate::storage::sqlite::LexicalRebuildGroupedMessageRow {
                idx: 1,
                is_tool_role: false,
                created_at: Some(1_700_000_936_020),
                content: "mismatch beta".into(),
            },
        ]
        .into_iter()
        .collect::<crate::storage::sqlite::LexicalRebuildGroupedMessageRows>(),
        Some(80),
        &HashMap::new(),
    );
    assert_eq!(
        build_lexical_rebuild_shard_index(&shard_path, &[packet], None).unwrap(),
        2
    );
    let result = LexicalRebuildShardBuildResult {
        shard: LexicalShardPlanShard {
            shard_index: 0,
            first_conversation_id: 8,
            last_conversation_id: 8,
            conversation_count: 1,
            message_count: 2,
            message_bytes: "mismatch alpha".len() + "mismatch beta".len(),
            conversation_id_fingerprint: lexical_shard_conversation_ids_fingerprint(&[8]),
            oversized_single_conversation: false,
        },
        indexed_docs: 1,
        segments: 1,
        shard_index_path: shard_path,
        message_bytes: "mismatch alpha".len() + "mismatch beta".len(),
        index_size_bytes: 0,
        build_duration_ms: 0,
        amplification_milli: None,
    };

    let err = validate_lexical_rebuild_shard_build_result(&result)
        .unwrap_err()
        .to_string();

    assert!(
        err.contains("reported 1 docs but a fresh Tantivy reader sees 2"),
        "expected doc-count mismatch error, got {err}"
    );
}

/// `coding_agent_session_search-rx1ex`: pre-fix,
/// `validate_lexical_rebuild_shard_build_result` failed any
/// rebuild where the post-filter Tantivy doc count was less
/// than the shard plan's raw `message_count`. The reproducer
/// in the bead (1 conversation × 2 messages) reliably tripped
/// this even though the lexical sink legitimately drops messages
/// via `is_hard_message_noise`. Post-fix, the validator tolerates
/// the filter-induced gap (only fails on the impossible
/// `observed_docs > message_count` "fan-out" case) and emits a
/// structured debug log naming the filtered count.
#[test]
fn shard_validate_tolerates_filter_induced_doc_lt_message_count_gap() {
    let tmp = TempDir::new().unwrap();
    let shard_path = tmp.path().join("filter-tolerance-shard");
    // Build a single packet with 2 messages, both with content
    // so the lexical sink emits 2 docs. We then construct a
    // shard plan whose message_count claims 3 (simulating a
    // filter that dropped 1 message after the planner was sized).
    let packet = LexicalRebuildConversationPacket::from_canonical_replay(
        crate::storage::sqlite::LexicalRebuildConversationRow {
            id: Some(80),
            agent_slug: "codex".into(),
            workspace: None,
            external_id: Some("filter-gap".into()),
            title: Some("filter gap".into()),
            source_path: PathBuf::from("/tmp/filter-gap.jsonl"),
            started_at: Some(1_700_000_940_000),
            ended_at: Some(1_700_000_940_100),
            source_id: LOCAL_SOURCE_ID.into(),
            origin_host: None,
        },
        vec![
            crate::storage::sqlite::LexicalRebuildGroupedMessageRow {
                idx: 0,
                is_tool_role: false,
                created_at: Some(1_700_000_940_010),
                content: "filter alpha".into(),
            },
            crate::storage::sqlite::LexicalRebuildGroupedMessageRow {
                idx: 1,
                is_tool_role: false,
                created_at: Some(1_700_000_940_020),
                content: "filter beta".into(),
            },
        ]
        .into_iter()
        .collect::<crate::storage::sqlite::LexicalRebuildGroupedMessageRows>(),
        Some(80),
        &HashMap::new(),
    );
    let indexed_docs = build_lexical_rebuild_shard_index(&shard_path, &[packet], None).unwrap();
    assert_eq!(
        indexed_docs, 2,
        "fixture must build exactly 2 docs (precondition)"
    );

    let shard = LexicalShardPlanShard {
        shard_index: 0,
        first_conversation_id: 80,
        last_conversation_id: 80,
        conversation_count: 1,
        // Plan claims 3 messages — simulates the filter case
        // where one source message was dropped via
        // is_hard_message_noise but the planner sized for the
        // raw row count from list_conversation_footprints_*.
        message_count: 3,
        message_bytes: "filter alpha".len() + "filter beta".len() + "dropped".len(),
        conversation_id_fingerprint: lexical_shard_conversation_ids_fingerprint(&[80]),
        oversized_single_conversation: false,
    };
    let result = LexicalRebuildShardBuildResult {
        shard: shard.clone(),
        indexed_docs,
        segments: 1,
        shard_index_path: shard_path.clone(),
        message_bytes: shard.message_bytes,
        index_size_bytes: 0,
        build_duration_ms: 0,
        amplification_milli: None,
    };

    // Pre-fix this would have errored with "indexed 2 docs but
    // its shard plan expected 3 messages"; post-fix it
    // succeeds because the gap is filter-attributable.
    let artifact = validate_lexical_rebuild_shard_build_result(&result)
        .expect("filter-induced doc<message gap must NOT fail validation");
    assert_eq!(artifact.first_shard_index, 0);
    assert_eq!(artifact.last_shard_index, 0);
    assert_eq!(artifact.index_path, shard_path);

    // Same tolerance must also apply to
    // validate_complete_lexical_rebuild_shard_artifacts so the
    // plan-level validation matches the per-shard-build path.
    validate_complete_lexical_rebuild_shard_artifacts(
        &LexicalShardPlan {
            planner_version: LEXICAL_SHARD_PLAN_VERSION,
            plan_id: "filter-tolerance".into(),
            budgets: LexicalShardPlannerBudgets {
                max_conversations_per_shard: 1,
                max_messages_per_shard: 3,
                max_message_bytes_per_shard: 10_000,
            },
            total_conversations: 1,
            total_messages: 3,
            total_message_bytes: shard.message_bytes,
            oversized_conversation_ids: Vec::new(),
            shards: vec![shard],
        },
        &[artifact],
    )
    .expect("plan-level validator must tolerate filter-induced gaps too");
}

/// Companion test for the inflation case: if the lexical sink
/// somehow emits MORE docs than the shard plan's raw message
/// count, that's the "fan-out / duplicate document" bug class
/// and validation MUST hard-error. Post-fix, the second check
/// continues to catch this.
#[test]
fn shard_validate_rejects_doc_count_exceeding_shard_plan_message_count() {
    let tmp = TempDir::new().unwrap();
    let shard_path = tmp.path().join("inflation-shard");
    let packet = LexicalRebuildConversationPacket::from_canonical_replay(
        crate::storage::sqlite::LexicalRebuildConversationRow {
            id: Some(81),
            agent_slug: "codex".into(),
            workspace: None,
            external_id: Some("inflation".into()),
            title: Some("inflation".into()),
            source_path: PathBuf::from("/tmp/inflation.jsonl"),
            started_at: Some(1_700_000_950_000),
            ended_at: Some(1_700_000_950_100),
            source_id: LOCAL_SOURCE_ID.into(),
            origin_host: None,
        },
        vec![
            crate::storage::sqlite::LexicalRebuildGroupedMessageRow {
                idx: 0,
                is_tool_role: false,
                created_at: Some(1_700_000_950_010),
                content: "inflation alpha".into(),
            },
            crate::storage::sqlite::LexicalRebuildGroupedMessageRow {
                idx: 1,
                is_tool_role: false,
                created_at: Some(1_700_000_950_020),
                content: "inflation beta".into(),
            },
        ]
        .into_iter()
        .collect::<crate::storage::sqlite::LexicalRebuildGroupedMessageRows>(),
        Some(81),
        &HashMap::new(),
    );
    let indexed_docs = build_lexical_rebuild_shard_index(&shard_path, &[packet], None).unwrap();
    assert_eq!(indexed_docs, 2);

    // Construct a deliberately-undersized shard plan: claims
    // only 1 message but actually 2 docs were emitted.
    let shard = LexicalShardPlanShard {
        shard_index: 0,
        first_conversation_id: 81,
        last_conversation_id: 81,
        conversation_count: 1,
        message_count: 1,
        message_bytes: "inflation alpha".len(),
        conversation_id_fingerprint: lexical_shard_conversation_ids_fingerprint(&[81]),
        oversized_single_conversation: false,
    };
    let result = LexicalRebuildShardBuildResult {
        shard: shard.clone(),
        indexed_docs,
        segments: 1,
        shard_index_path: shard_path.clone(),
        message_bytes: shard.message_bytes,
        index_size_bytes: 0,
        build_duration_ms: 0,
        amplification_milli: None,
    };

    let err = validate_lexical_rebuild_shard_build_result(&result)
        .unwrap_err()
        .to_string();
    // Pin the exact "EXCEEDS" phrasing so a regression that
    // softens this hard-error trips the test.
    assert!(
        err.contains("EXCEEDS"),
        "expected inflation hard-error to use the explicit EXCEEDS phrasing; got: {err}"
    );
    assert!(
        err.contains("2 docs") && err.contains("1 source messages"),
        "error must name observed (2) and planned (1) counts; got: {err}"
    );
}

#[test]
// Asserts the staged-shard rebuild strategy, which builds N independent
// index directories and merges them. That merge is Tantivy-only (Quill's
// concat_merge joins segments WITHIN one manifest), so the strategy is
// disabled for the Quill backend — see the `staged_shard_plan` binding.
// Ignored rather than deleted: these are exactly the tests that must pass
// if an N-directory merge is ever built, and weakening them would hide
// that the strategy no longer runs.
#[ignore = "staged-shard strategy is Tantivy-only; disabled on the Quill backend"]
fn lexical_rebuild_shard_index_tree_merges_multiple_rounds() {
    let tmp = TempDir::new().unwrap();
    let merge_stage_root = tmp.path().join("merge-stage");
    let merged_path = tmp.path().join("merged");
    let make_packet =
        |conversation_id: i64, external_id: &str, message_a: &str, message_b: &str| {
            LexicalRebuildConversationPacket::from_canonical_replay(
                crate::storage::sqlite::LexicalRebuildConversationRow {
                    id: Some(conversation_id),
                    agent_slug: "codex".into(),
                    workspace: Some(PathBuf::from("/tmp/workspace")),
                    external_id: Some(external_id.into()),
                    title: Some(format!("Shard {external_id}")),
                    source_path: PathBuf::from(format!("/tmp/{external_id}.jsonl")),
                    started_at: Some(1_700_000_910_000 + conversation_id),
                    ended_at: Some(1_700_000_910_100 + conversation_id),
                    source_id: LOCAL_SOURCE_ID.into(),
                    origin_host: None,
                },
                vec![
                    crate::storage::sqlite::LexicalRebuildGroupedMessageRow {
                        idx: 0,
                        is_tool_role: false,
                        created_at: Some(1_700_000_910_010 + conversation_id),
                        content: message_a.into(),
                    },
                    crate::storage::sqlite::LexicalRebuildGroupedMessageRow {
                        idx: 1,
                        is_tool_role: false,
                        created_at: Some(1_700_000_910_020 + conversation_id),
                        content: message_b.into(),
                    },
                ]
                .into_iter()
                .collect::<crate::storage::sqlite::LexicalRebuildGroupedMessageRows>(),
                Some(conversation_id * 10),
                &HashMap::new(),
            )
        };

    let shard_paths = (0..5)
        .map(|idx| {
            let shard_path = tmp.path().join(format!("tree-shard-{idx}"));
            let packet = make_packet(
                i64::from(idx + 1),
                &format!("tree-segment-{idx}"),
                &format!("alpha-{idx}"),
                &format!("beta-{idx}"),
            );
            assert_eq!(
                build_lexical_rebuild_shard_index(&shard_path, &[packet], None).unwrap(),
                2
            );
            shard_path
        })
        .collect::<Vec<_>>();

    let merged_index =
        merge_lexical_rebuild_shard_index_tree(&merged_path, &shard_paths, &merge_stage_root, 2)
            .unwrap();
    let reader = merged_index.reader().unwrap();
    crate::search::quill_bridge::refresh_reader(&reader).unwrap();
    assert_eq!(reader.doc_count().expect("doc count"), 10);
    assert_eq!(
        merged_index.segment_count(),
        1,
        "tree-merged shard indices should collapse into a single final segment"
    );
}

#[test]
fn finalize_staged_lexical_rebuild_publish_artifact_reuses_single_input_without_remerge() {
    let tmp = TempDir::new().unwrap();
    let merge_stage_root = tmp.path().join("merge-stage");
    let merged_path = tmp.path().join("merged");
    let shard_path = tmp.path().join("single-shard");
    let packet = LexicalRebuildConversationPacket::from_canonical_replay(
        crate::storage::sqlite::LexicalRebuildConversationRow {
            id: Some(1),
            agent_slug: "codex".into(),
            workspace: Some(PathBuf::from("/tmp/workspace")),
            external_id: Some("single-segment".into()),
            title: Some("Single shard".into()),
            source_path: PathBuf::from("/tmp/single-segment.jsonl"),
            started_at: Some(1_700_000_915_000),
            ended_at: Some(1_700_000_915_100),
            source_id: LOCAL_SOURCE_ID.into(),
            origin_host: None,
        },
        vec![
            crate::storage::sqlite::LexicalRebuildGroupedMessageRow {
                idx: 0,
                is_tool_role: false,
                created_at: Some(1_700_000_915_010),
                content: "alpha".into(),
            },
            crate::storage::sqlite::LexicalRebuildGroupedMessageRow {
                idx: 1,
                is_tool_role: false,
                created_at: Some(1_700_000_915_020),
                content: "beta".into(),
            },
        ]
        .into_iter()
        .collect::<crate::storage::sqlite::LexicalRebuildGroupedMessageRows>(),
        Some(10),
        &HashMap::new(),
    );

    assert_eq!(
        build_lexical_rebuild_shard_index(&shard_path, &[packet], None).unwrap(),
        2
    );

    let final_artifact = finalize_staged_lexical_rebuild_publish_artifact(
        &merged_path,
        std::slice::from_ref(&shard_path),
        &merge_stage_root,
        2,
    )
    .unwrap();
    assert_eq!(final_artifact.docs, 2);
    assert_eq!(
        final_artifact.publish_path, shard_path,
        "single-input finalization should reuse the existing shard artifact directly"
    );
    assert!(
        !merged_path.exists(),
        "single-input finalization should skip materializing a redundant merged directory"
    );
}

#[test]
// Asserts the staged-shard rebuild strategy, which builds N independent
// index directories and merges them. That merge is Tantivy-only (Quill's
// concat_merge joins segments WITHIN one manifest), so the strategy is
// disabled for the Quill backend — see the `staged_shard_plan` binding.
// Ignored rather than deleted: these are exactly the tests that must pass
// if an N-directory merge is ever built, and weakening them would hide
// that the strategy no longer runs.
#[ignore = "staged-shard strategy is Tantivy-only; disabled on the Quill backend"]
fn finalize_staged_lexical_rebuild_publish_artifact_publishes_federated_multi_input_frontier_without_doc_remerge()
 {
    let tmp = TempDir::new().unwrap();
    let merge_stage_root = tmp.path().join("merge-stage");
    let merged_path = tmp.path().join("merged");
    let make_packet =
        |conversation_id: i64, external_id: &str, message_a: &str, message_b: &str| {
            LexicalRebuildConversationPacket::from_canonical_replay(
                crate::storage::sqlite::LexicalRebuildConversationRow {
                    id: Some(conversation_id),
                    agent_slug: "codex".into(),
                    workspace: Some(PathBuf::from("/tmp/workspace")),
                    external_id: Some(external_id.into()),
                    title: Some(format!("Shard {external_id}")),
                    source_path: PathBuf::from(format!("/tmp/{external_id}.jsonl")),
                    started_at: Some(1_700_000_916_000 + conversation_id),
                    ended_at: Some(1_700_000_916_100 + conversation_id),
                    source_id: LOCAL_SOURCE_ID.into(),
                    origin_host: None,
                },
                vec![
                    crate::storage::sqlite::LexicalRebuildGroupedMessageRow {
                        idx: 0,
                        is_tool_role: false,
                        created_at: Some(1_700_000_916_010 + conversation_id),
                        content: message_a.into(),
                    },
                    crate::storage::sqlite::LexicalRebuildGroupedMessageRow {
                        idx: 1,
                        is_tool_role: false,
                        created_at: Some(1_700_000_916_020 + conversation_id),
                        content: message_b.into(),
                    },
                ]
                .into_iter()
                .collect::<crate::storage::sqlite::LexicalRebuildGroupedMessageRows>(),
                Some(conversation_id * 10),
                &HashMap::new(),
            )
        };

    let shard_paths = (0..3)
        .map(|idx| {
            let shard_path = tmp.path().join(format!("finalize-shard-{idx}"));
            let packet = make_packet(
                i64::from(idx + 1),
                &format!("finalize-{idx}"),
                &format!("alpha-{idx}"),
                &format!("beta-{idx}"),
            );
            assert_eq!(
                build_lexical_rebuild_shard_index(&shard_path, &[packet], None).unwrap(),
                2
            );
            shard_path
        })
        .collect::<Vec<_>>();

    let final_artifact = finalize_staged_lexical_rebuild_publish_artifact(
        &merged_path,
        &shard_paths,
        &merge_stage_root,
        3,
    )
    .unwrap();
    assert_eq!(final_artifact.docs, 6);
    assert_eq!(
        final_artifact.segments, 3,
        "multi-input finalization should preserve the final shard frontier without remerging docs"
    );
    assert_eq!(
        final_artifact.publish_path, merged_path,
        "multi-input finalization should materialize the federated publish bundle at the requested output path"
    );
    assert_eq!(
        crate::search::tantivy::open_federated_search_readers(&merged_path)
            .unwrap()
            .expect("federated readers")
            .len(),
        3,
        "multi-input finalization should publish the three final shard artifacts as a federated lexical bundle"
    );
    assert!(
        !merge_stage_root.join("round-00000").exists(),
        "multi-input finalization should not materialize a fallback merge-tree round"
    );
}

#[test]
// Asserts the staged-shard rebuild strategy, which builds N independent
// index directories and merges them. That merge is Tantivy-only (Quill's
// concat_merge joins segments WITHIN one manifest), so the strategy is
// disabled for the Quill backend — see the `staged_shard_plan` binding.
// Ignored rather than deleted: these are exactly the tests that must pass
// if an N-directory merge is ever built, and weakening them would hide
// that the strategy no longer runs.
#[ignore = "staged-shard strategy is Tantivy-only; disabled on the Quill backend"]
fn merge_lexical_rebuild_shard_index_tree_merges_small_frontier_without_round_directory() {
    let tmp = TempDir::new().unwrap();
    let merge_stage_root = tmp.path().join("merge-stage");
    let merged_path = tmp.path().join("merged");
    let make_packet =
        |conversation_id: i64, external_id: &str, message_a: &str, message_b: &str| {
            LexicalRebuildConversationPacket::from_canonical_replay(
                crate::storage::sqlite::LexicalRebuildConversationRow {
                    id: Some(conversation_id),
                    agent_slug: "codex".into(),
                    workspace: Some(PathBuf::from("/tmp/workspace")),
                    external_id: Some(external_id.into()),
                    title: Some(format!("Shard {external_id}")),
                    source_path: PathBuf::from(format!("/tmp/{external_id}.jsonl")),
                    started_at: Some(1_700_000_918_000 + conversation_id),
                    ended_at: Some(1_700_000_918_100 + conversation_id),
                    source_id: LOCAL_SOURCE_ID.into(),
                    origin_host: None,
                },
                vec![
                    crate::storage::sqlite::LexicalRebuildGroupedMessageRow {
                        idx: 0,
                        is_tool_role: false,
                        created_at: Some(1_700_000_918_010 + conversation_id),
                        content: message_a.into(),
                    },
                    crate::storage::sqlite::LexicalRebuildGroupedMessageRow {
                        idx: 1,
                        is_tool_role: false,
                        created_at: Some(1_700_000_918_020 + conversation_id),
                        content: message_b.into(),
                    },
                ]
                .into_iter()
                .collect::<crate::storage::sqlite::LexicalRebuildGroupedMessageRows>(),
                Some(conversation_id * 10),
                &HashMap::new(),
            )
        };

    let shard_paths = (0..3)
        .map(|idx| {
            let shard_path = tmp.path().join(format!("small-frontier-shard-{idx}"));
            let packet = make_packet(
                i64::from(idx + 1),
                &format!("small-frontier-{idx}"),
                &format!("alpha-{idx}"),
                &format!("beta-{idx}"),
            );
            assert_eq!(
                build_lexical_rebuild_shard_index(&shard_path, &[packet], None).unwrap(),
                2
            );
            shard_path
        })
        .collect::<Vec<_>>();

    let merged_index =
        merge_lexical_rebuild_shard_index_tree(&merged_path, &shard_paths, &merge_stage_root, 3)
            .unwrap();
    let reader = merged_index.reader().unwrap();
    crate::search::quill_bridge::refresh_reader(&reader).unwrap();
    assert_eq!(reader.doc_count().expect("doc count"), 6);
    assert!(
        !merge_stage_root.join("round-00000").exists(),
        "small final frontiers should merge directly without materializing a merge-tree round directory"
    );
}

#[test]
// Asserts the staged-shard rebuild strategy, which builds N independent
// index directories and merges them. That merge is Tantivy-only (Quill's
// concat_merge joins segments WITHIN one manifest), so the strategy is
// disabled for the Quill backend — see the `staged_shard_plan` binding.
// Ignored rather than deleted: these are exactly the tests that must pass
// if an N-directory merge is ever built, and weakening them would hide
// that the strategy no longer runs.
#[ignore = "staged-shard strategy is Tantivy-only; disabled on the Quill backend"]
fn reduce_staged_lexical_final_merge_frontier_via_workers_reduces_large_frontier_to_single_artifact()
 {
    let tmp = TempDir::new().unwrap();
    let stage_root = tmp.path().join("final-frontier-stage");
    let (merge_work_tx, merge_work_rx) = bounded::<LexicalRebuildShardMergeJob>(2);
    let (merge_result_tx, merge_result_rx) = bounded::<LexicalRebuildShardMergeMessage>(8);
    let merge_worker_handles =
        spawn_lexical_rebuild_shard_merge_workers(2, merge_work_rx, merge_result_tx);
    let make_packet =
        |conversation_id: i64, external_id: &str, message_a: &str, message_b: &str| {
            LexicalRebuildConversationPacket::from_canonical_replay(
                crate::storage::sqlite::LexicalRebuildConversationRow {
                    id: Some(conversation_id),
                    agent_slug: "codex".into(),
                    workspace: Some(PathBuf::from("/tmp/workspace")),
                    external_id: Some(external_id.into()),
                    title: Some(format!("Shard {external_id}")),
                    source_path: PathBuf::from(format!("/tmp/{external_id}.jsonl")),
                    started_at: Some(1_700_000_930_000 + conversation_id),
                    ended_at: Some(1_700_000_930_100 + conversation_id),
                    source_id: LOCAL_SOURCE_ID.into(),
                    origin_host: None,
                },
                vec![
                    crate::storage::sqlite::LexicalRebuildGroupedMessageRow {
                        idx: 0,
                        is_tool_role: false,
                        created_at: Some(1_700_000_930_010 + conversation_id),
                        content: message_a.into(),
                    },
                    crate::storage::sqlite::LexicalRebuildGroupedMessageRow {
                        idx: 1,
                        is_tool_role: false,
                        created_at: Some(1_700_000_930_020 + conversation_id),
                        content: message_b.into(),
                    },
                ]
                .into_iter()
                .collect::<crate::storage::sqlite::LexicalRebuildGroupedMessageRows>(),
                Some(conversation_id * 10),
                &HashMap::new(),
            )
        };

    let frontier_ranges = [
        (0usize, 3usize),
        (4, 7),
        (8, 11),
        (12, 12),
        (13, 13),
        (14, 14),
    ];
    let frontier = frontier_ranges
        .iter()
        .enumerate()
        .map(|(idx, (first_shard_index, last_shard_index))| {
            let shard_path = tmp.path().join(format!("frontier-artifact-{idx}"));
            let packet = make_packet(
                i64::try_from(idx + 1).unwrap(),
                &format!("frontier-{idx}"),
                &format!("alpha-{idx}"),
                &format!("beta-{idx}"),
            );
            assert_eq!(
                build_lexical_rebuild_shard_index(&shard_path, &[packet], None).unwrap(),
                2
            );
            LexicalRebuildShardMergeArtifact {
                first_shard_index: *first_shard_index,
                last_shard_index: *last_shard_index,
                index_path: shard_path,
                docs: 2,
                segments: 1,
            }
        })
        .collect::<Vec<_>>();

    let reduced = reduce_staged_lexical_final_merge_frontier_via_workers(
        frontier,
        &stage_root,
        2,
        &merge_work_tx,
        &merge_result_rx,
        None,
    )
    .unwrap();
    assert_eq!(reduced.len(), 1);
    assert_eq!(reduced[0].first_shard_index, 0);
    assert_eq!(reduced[0].last_shard_index, 14);

    let merged_index =
        crate::search::tantivy::TantivyIndex::open_or_create(&reduced[0].index_path).unwrap();
    let reader = merged_index.reader().unwrap();
    crate::search::quill_bridge::refresh_reader(&reader).unwrap();
    assert_eq!(reader.doc_count().expect("doc count"), 12);

    drop(merge_work_tx);
    for handle in merge_worker_handles {
        handle.join().unwrap();
    }
}

#[test]
// Asserts the staged-shard rebuild strategy, which builds N independent
// index directories and merges them. That merge is Tantivy-only (Quill's
// concat_merge joins segments WITHIN one manifest), so the strategy is
// disabled for the Quill backend — see the `staged_shard_plan` binding.
// Ignored rather than deleted: these are exactly the tests that must pass
// if an N-directory merge is ever built, and weakening them would hide
// that the strategy no longer runs.
#[ignore = "staged-shard strategy is Tantivy-only; disabled on the Quill backend"]
fn lexical_rebuild_eager_merge_coordinator_reduces_full_groups_before_final_merge() {
    let tmp = TempDir::new().unwrap();
    let eager_merge_stage_root = tmp.path().join("eager-merge-stage");
    let final_merge_stage_root = tmp.path().join("final-merge-stage");
    let merged_path = tmp.path().join("merged");
    let (merge_work_tx, merge_work_rx) = bounded::<LexicalRebuildShardMergeJob>(1);
    let (merge_result_tx, merge_result_rx) = bounded::<LexicalRebuildShardMergeMessage>(8);
    let merge_worker_handles =
        spawn_lexical_rebuild_shard_merge_workers(1, merge_work_rx, merge_result_tx);
    let make_packet =
        |conversation_id: i64, external_id: &str, message_a: &str, message_b: &str| {
            LexicalRebuildConversationPacket::from_canonical_replay(
                crate::storage::sqlite::LexicalRebuildConversationRow {
                    id: Some(conversation_id),
                    agent_slug: "codex".into(),
                    workspace: Some(PathBuf::from("/tmp/workspace")),
                    external_id: Some(external_id.into()),
                    title: Some(format!("Shard {external_id}")),
                    source_path: PathBuf::from(format!("/tmp/{external_id}.jsonl")),
                    started_at: Some(1_700_000_920_000 + conversation_id),
                    ended_at: Some(1_700_000_920_100 + conversation_id),
                    source_id: LOCAL_SOURCE_ID.into(),
                    origin_host: None,
                },
                vec![
                    crate::storage::sqlite::LexicalRebuildGroupedMessageRow {
                        idx: 0,
                        is_tool_role: false,
                        created_at: Some(1_700_000_920_010 + conversation_id),
                        content: message_a.into(),
                    },
                    crate::storage::sqlite::LexicalRebuildGroupedMessageRow {
                        idx: 1,
                        is_tool_role: false,
                        created_at: Some(1_700_000_920_020 + conversation_id),
                        content: message_b.into(),
                    },
                ]
                .into_iter()
                .collect::<crate::storage::sqlite::LexicalRebuildGroupedMessageRows>(),
                Some(conversation_id * 10),
                &HashMap::new(),
            )
        };

    let shard_count = LexicalRebuildShardMergeCoordinator::EAGER_MERGE_FAN_IN + 1;
    let shard_paths = (0..shard_count)
        .map(|idx| {
            let shard_path = tmp.path().join(format!("eager-shard-{idx}"));
            let packet = make_packet(
                i64::try_from(idx + 1).unwrap(),
                &format!("eager-segment-{idx}"),
                &format!("alpha-{idx}"),
                &format!("beta-{idx}"),
            );
            assert_eq!(
                build_lexical_rebuild_shard_index(&shard_path, &[packet], None).unwrap(),
                2
            );
            shard_path
        })
        .collect::<Vec<_>>();

    let mut merge_coordinator = LexicalRebuildShardMergeCoordinator::new(eager_merge_stage_root);
    // The coordinator's eager-merge gate defaults to
    // allowed_pending_merge_jobs=0 so that production callers always
    // opt in to the concurrency they want. Grant the test enough budget
    // for at least one in-flight eager merge; without this, the gate
    // at schedule_ready_merges swallows every ready group and the
    // 4-way merge never fires (previously this was silently elided,
    // which is the regression that made this test red).
    merge_coordinator
        .set_allowed_pending_merge_jobs(8, &merge_work_tx)
        .unwrap();
    for (idx, shard_path) in shard_paths.iter().enumerate() {
        merge_coordinator
            .queue_base_artifact(
                LexicalRebuildShardMergeArtifact {
                    first_shard_index: idx,
                    last_shard_index: idx,
                    index_path: shard_path.clone(),
                    docs: 2,
                    segments: 1,
                },
                &merge_work_tx,
            )
            .unwrap();
    }

    assert_eq!(
        merge_coordinator.pending_merge_jobs(),
        1,
        "one more than the eager fan-in should trigger one eager merge with one shard left unmerged"
    );
    let eager_merge_result = match merge_result_rx
        .recv_timeout(Duration::from_secs(30))
        .unwrap()
    {
        LexicalRebuildShardMergeMessage::Built(result) => result,
        LexicalRebuildShardMergeMessage::Error {
            output_level,
            first_shard_index,
            last_shard_index,
            error,
        } => panic!(
            "unexpected eager merge failure at level {output_level} for {first_shard_index}..={last_shard_index}: {error}"
        ),
    };
    merge_coordinator
        .complete_merge(eager_merge_result, &merge_work_tx)
        .unwrap();
    assert_eq!(merge_coordinator.pending_merge_jobs(), 0);

    let final_merge_inputs = merge_coordinator
        .final_merge_input_artifacts()
        .into_iter()
        .map(|artifact| artifact.index_path)
        .collect::<Vec<_>>();
    assert_eq!(
        final_merge_inputs.len(),
        2,
        "eager reduction should shrink one fan-in group down to one merged artifact plus one tail shard"
    );

    drop(merge_work_tx);
    for handle in merge_worker_handles {
        handle.join().unwrap();
    }

    let merged_index = merge_lexical_rebuild_shard_index_tree(
        &merged_path,
        &final_merge_inputs,
        &final_merge_stage_root,
        1,
    )
    .unwrap();
    let reader = merged_index.reader().unwrap();
    crate::search::quill_bridge::refresh_reader(&reader).unwrap();
    assert_eq!(
        reader.doc_count().expect("doc count"),
        u64::try_from(shard_count * 2).unwrap()
    );
}

#[test]
fn lexical_rebuild_staged_merge_controller_trickles_under_builder_pressure() {
    let controller = LexicalRebuildStagedMergeController::new(3, Some(7_000));
    let fan_in = LexicalRebuildShardMergeCoordinator::EAGER_MERGE_FAN_IN;
    let merge_coordinator = LexicalRebuildShardMergeCoordinator {
        stage_root: PathBuf::from("/tmp/eager-merge"),
        ready_levels: vec![
            (0..fan_in)
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
        pending_merge_jobs: 0,
        allowed_pending_merge_jobs: 0,
    };
    let pressured_runtime = LexicalRebuildPipelineRuntimeSnapshot {
        queue_depth: 2,
        pending_batch_conversations: 4,
        pending_batch_message_bytes: 32_768,
        page_prep_workers: 6,
        active_page_prep_jobs: 6,
        ordered_buffered_pages: 1,
        producer_handoff_wait_count: 1,
        ..LexicalRebuildPipelineRuntimeSnapshot::default()
    };

    let decision = controller.decide(false, &pressured_runtime, &merge_coordinator);

    assert_eq!(decision.workers_max, 3);
    assert_eq!(decision.allowed_jobs, 1);
    assert_eq!(decision.ready_artifacts, fan_in);
    assert_eq!(decision.ready_groups, 1);
    assert_eq!(
        decision.controller_reason,
        "builder_handoff_pressure_scaling_staged_merge_budget_1_active_jobs_0_ready_groups_1_debt_budget_1_buffered_pages_1_queue_depth_2"
    );
}

#[test]
fn lexical_rebuild_staged_merge_controller_spends_debt_budget_under_page_prep_saturation() {
    let controller = LexicalRebuildStagedMergeController::new(8, Some(7_000));
    let merge_coordinator = LexicalRebuildShardMergeCoordinator {
        stage_root: PathBuf::from("/tmp/eager-merge"),
        ready_levels: vec![
            (0..LexicalRebuildShardMergeCoordinator::EAGER_MERGE_FAN_IN * 3)
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
        pending_merge_jobs: 0,
        allowed_pending_merge_jobs: 0,
    };
    let saturated_runtime = LexicalRebuildPipelineRuntimeSnapshot {
        page_prep_workers: 6,
        active_page_prep_jobs: 6,
        ..LexicalRebuildPipelineRuntimeSnapshot::default()
    };

    let decision = controller.decide(false, &saturated_runtime, &merge_coordinator);

    assert_eq!(decision.workers_max, 8);
    assert_eq!(decision.allowed_jobs, 3);
    assert_eq!(
        decision.ready_artifacts,
        LexicalRebuildShardMergeCoordinator::EAGER_MERGE_FAN_IN * 3
    );
    assert_eq!(decision.ready_groups, 3);
    assert_eq!(
        decision.controller_reason,
        "page_prep_workers_saturated_6_of_6_merge_budget_3_active_jobs_0_ready_groups_3"
    );
}

#[test]
fn lexical_rebuild_staged_merge_controller_scales_under_large_merge_debt() {
    let controller = LexicalRebuildStagedMergeController::new(4, Some(7_000));
    let fan_in = LexicalRebuildShardMergeCoordinator::EAGER_MERGE_FAN_IN;
    let ready_groups = 16;
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
    let pressured_runtime = LexicalRebuildPipelineRuntimeSnapshot {
        ordered_buffered_pages: 150,
        ..LexicalRebuildPipelineRuntimeSnapshot::default()
    };

    let decision = controller.decide(false, &pressured_runtime, &merge_coordinator);

    assert_eq!(decision.workers_max, 4);
    assert_eq!(decision.allowed_jobs, 4);
    assert_eq!(decision.active_jobs, 1);
    assert_eq!(decision.ready_artifacts, fan_in * ready_groups);
    assert_eq!(decision.ready_groups, ready_groups);
    assert_eq!(
        decision.controller_reason,
        format!(
            "builder_handoff_pressure_scaling_staged_merge_budget_4_active_jobs_1_ready_groups_{ready_groups}_debt_budget_4_buffered_pages_150_queue_depth_0"
        )
    );
}

#[test]
fn lexical_rebuild_staged_merge_controller_keeps_debt_budget_monotonic_with_more_workers() {
    let controller = LexicalRebuildStagedMergeController::new(8, Some(7_000));
    let fan_in = LexicalRebuildShardMergeCoordinator::EAGER_MERGE_FAN_IN;
    let ready_groups = 12;
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
    let pressured_runtime = LexicalRebuildPipelineRuntimeSnapshot {
        ordered_buffered_pages: 120,
        ..LexicalRebuildPipelineRuntimeSnapshot::default()
    };

    let decision = controller.decide(false, &pressured_runtime, &merge_coordinator);

    assert_eq!(decision.workers_max, 8);
    assert_eq!(decision.allowed_jobs, 8);
    assert_eq!(decision.active_jobs, 1);
    assert_eq!(decision.ready_artifacts, fan_in * ready_groups);
    assert_eq!(decision.ready_groups, ready_groups);
    assert_eq!(
        decision.controller_reason,
        format!(
            "builder_handoff_pressure_scaling_staged_merge_budget_8_active_jobs_1_ready_groups_{ready_groups}_debt_budget_8_buffered_pages_120_queue_depth_0"
        )
    );
}
