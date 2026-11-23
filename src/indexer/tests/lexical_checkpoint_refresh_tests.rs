use super::super::*;
use super::helpers::*;
use serial_test::serial;
use tempfile::TempDir;

#[test]
fn reconcile_pending_lexical_commit_rolls_back_uncommitted_batch_when_meta_unchanged() {
    let tmp = TempDir::new().unwrap();
    let index_path = tmp.path().join("index");
    fs::create_dir_all(&index_path).unwrap();
    fs::write(index_path.join("meta.json"), b"stable").unwrap();

    let db_state = LexicalRebuildDbState {
        db_path: "/tmp/agent_search.db".to_string(),
        total_conversations: 400,
        total_messages: 800,
        storage_fingerprint: "seed:400".to_string(),
    };
    let mut state = LexicalRebuildState::new(db_state.clone(), LEXICAL_REBUILD_PAGE_SIZE);
    state.committed_offset = 100;
    state.committed_conversation_id = Some(100);
    state.processed_conversations = 100;
    state.indexed_docs = 250;
    state.set_runtime(&LexicalRebuildPipelineRuntimeSnapshot {
        queue_depth: 2,
        inflight_message_bytes: 32_768,
        max_message_bytes_in_flight: 65_536,
        pending_batch_conversations: 4,
        pending_batch_message_bytes: 65_536,
        page_prep_workers: 4,
        active_page_prep_jobs: 1,
        ordered_buffered_pages: 3,
        budget_generation: 2,
        producer_budget_wait_count: 1,
        producer_budget_wait_ms: 11,
        producer_handoff_wait_count: 2,
        producer_handoff_wait_ms: 22,
        host_loadavg_1m_milli: None,
        controller_mode: "steady".to_string(),
        controller_reason: "first_durable_commit_promoted_steady_budget".to_string(),
        staged_merge_workers_max: 0,
        staged_merge_allowed_jobs: 0,
        staged_merge_active_jobs: 0,
        staged_merge_ready_artifacts: 0,
        staged_merge_ready_groups: 0,
        staged_merge_controller_reason: String::new(),
        staged_shard_build_workers_max: 0,
        staged_shard_build_allowed_jobs: 0,
        staged_shard_build_active_jobs: 0,
        staged_shard_build_pending_jobs: 0,
        staged_shard_build_controller_reason: String::new(),
        updated_at_ms: 1_733_000_224_000_i64,
        ..LexicalRebuildPipelineRuntimeSnapshot::default()
    });
    state.record_pending_commit(
        Some(200),
        200,
        600,
        index_meta_fingerprint(&index_path).unwrap(),
    );
    persist_lexical_rebuild_state(&index_path, &state).unwrap();

    let reconciled = reconcile_pending_lexical_commit(&index_path, state).unwrap();
    assert_eq!(reconciled.committed_offset, 100);
    assert_eq!(reconciled.committed_conversation_id, Some(100));
    assert_eq!(reconciled.processed_conversations, 100);
    assert_eq!(reconciled.indexed_docs, 250);
    assert!(reconciled.pending.is_none());
    assert_eq!(
        reconciled.runtime,
        LexicalRebuildPipelineRuntimeSnapshot::default()
    );
    let status = matching_lexical_rebuild_state_status(&index_path, &db_state).unwrap();
    assert!(status.has_pending_resume);
    let persisted = load_lexical_rebuild_state(&index_path)
        .unwrap()
        .expect("rolled-back checkpoint should persist");
    assert_eq!(
        persisted.runtime,
        LexicalRebuildPipelineRuntimeSnapshot::default()
    );
}

#[test]
fn legacy_lexical_rebuild_page_size_still_counts_as_pending_rebuild() {
    let tmp = TempDir::new().unwrap();
    let index_path = tmp.path().join("index");
    fs::create_dir_all(&index_path).unwrap();

    let db_state = LexicalRebuildDbState {
        db_path: "/tmp/agent_search.db".to_string(),
        total_conversations: 400,
        total_messages: 800,
        storage_fingerprint: "seed:400".to_string(),
    };
    let state = LexicalRebuildState::new(db_state.clone(), 200);
    persist_lexical_rebuild_state(&index_path, &state).unwrap();

    let status = matching_lexical_rebuild_state_status(&index_path, &db_state).unwrap();
    assert!(status.has_pending_resume);
}

#[test]
fn pending_lexical_rebuild_matches_equivalent_db_path_spellings() {
    let tmp = TempDir::new().unwrap();
    let index_path = tmp.path().join("index");
    fs::create_dir_all(&index_path).unwrap();

    let db_path = tmp.path().join("agent_search.db");
    fs::write(&db_path, b"db").unwrap();
    let db_path_variant = tmp.path().join(".").join("agent_search.db");

    let checkpoint_db_state = LexicalRebuildDbState {
        db_path: db_path_variant.to_string_lossy().into_owned(),
        total_conversations: 400,
        total_messages: 800,
        storage_fingerprint: "seed:400".to_string(),
    };
    let current_db_state = LexicalRebuildDbState {
        db_path: crate::normalize_path_identity(&db_path)
            .to_string_lossy()
            .into_owned(),
        total_conversations: 400,
        total_messages: 800,
        storage_fingerprint: "seed:400".to_string(),
    };
    let state = LexicalRebuildState::new(checkpoint_db_state, 200);
    persist_lexical_rebuild_state(&index_path, &state).unwrap();

    let status = matching_lexical_rebuild_state_status(&index_path, &current_db_state).unwrap();
    assert!(status.has_pending_resume);
}

#[test]
fn legacy_lexical_rebuild_matches_despite_storage_fingerprint_drift_when_counts_match() {
    let checkpoint_db_state = LexicalRebuildDbState {
        db_path: "/tmp/agent_search.db".to_string(),
        total_conversations: 400,
        total_messages: 0,
        storage_fingerprint: "22396870656:1776366130822:8536672:1776366130775".to_string(),
    };
    let current_db_state = LexicalRebuildDbState {
        db_path: "/tmp/agent_search.db".to_string(),
        total_conversations: 400,
        total_messages: 800,
        storage_fingerprint: "22396870656:1776384918595:8577872:1776384918548".to_string(),
    };
    let state = LexicalRebuildState::new(checkpoint_db_state, 200);

    assert!(state.matches_run(&current_db_state, LEXICAL_REBUILD_PAGE_SIZE));
}

#[test]
fn lexical_rebuild_rejects_resume_when_content_fingerprint_changes() {
    let checkpoint_db_state = LexicalRebuildDbState {
        db_path: "/tmp/agent_search.db".to_string(),
        total_conversations: 400,
        total_messages: 0,
        storage_fingerprint: "content-v1:400:1200:4800".to_string(),
    };
    let current_db_state = LexicalRebuildDbState {
        db_path: "/tmp/agent_search.db".to_string(),
        total_conversations: 400,
        total_messages: 0,
        storage_fingerprint: "content-v1:400:1200:4801".to_string(),
    };
    let state = LexicalRebuildState::new(checkpoint_db_state, LEXICAL_REBUILD_PAGE_SIZE);

    assert!(!state.matches_run(&current_db_state, LEXICAL_REBUILD_PAGE_SIZE));
}

#[test]
fn normalize_legacy_lexical_rebuild_page_size_adopts_current_contract() {
    let tmp = TempDir::new().unwrap();
    let index_path = tmp.path().join("index");
    fs::create_dir_all(&index_path).unwrap();

    let db_state = LexicalRebuildDbState {
        db_path: "/tmp/agent_search.db".to_string(),
        total_conversations: 400,
        total_messages: 800,
        storage_fingerprint: "seed:400".to_string(),
    };
    let mut state = LexicalRebuildState::new(db_state, 200);
    persist_lexical_rebuild_state(&index_path, &state).unwrap();

    normalize_lexical_rebuild_state_for_current_run(&index_path, &mut state).unwrap();
    assert_eq!(state.page_size, LEXICAL_REBUILD_PAGE_SIZE);
    let persisted = load_lexical_rebuild_state(&index_path)
        .unwrap()
        .expect("normalized checkpoint");
    assert_eq!(persisted.page_size, LEXICAL_REBUILD_PAGE_SIZE);
}

#[test]
fn upgrade_legacy_lexical_rebuild_resume_cursor_records_stable_conversation_ids() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    seed_lexical_rebuild_fixture(&storage);

    let rows = storage
        .list_conversations_for_lexical_rebuild_after_id(2, 0, &HashMap::new(), &HashMap::new())
        .unwrap();
    let first_conversation_id = rows[0].id.expect("first conversation id");
    let second_conversation_id = rows[1].id.expect("second conversation id");
    let index_path = index_dir(&data_dir).unwrap();
    std::fs::create_dir_all(&index_path).unwrap();

    let mut state = LexicalRebuildState::new(
        lexical_rebuild_db_state(&storage, &db_path).unwrap(),
        LEXICAL_REBUILD_PAGE_SIZE,
    );
    state.committed_offset = 1;
    state.processed_conversations = 1;
    state.indexed_docs = 2;
    state.pending = Some(PendingLexicalCommit {
        next_offset: 2,
        next_conversation_id: None,
        processed_conversations: 2,
        indexed_docs: 4,
        base_meta_fingerprint: None,
        region_docs: 0,
    });

    upgrade_lexical_rebuild_state_resume_cursor_if_needed(&storage, &index_path, &mut state)
        .unwrap();

    assert_eq!(state.committed_conversation_id, Some(first_conversation_id));
    assert_eq!(
        state
            .pending
            .as_ref()
            .and_then(|pending| pending.next_conversation_id),
        Some(second_conversation_id)
    );
}

#[test]
fn incompatible_lexical_rebuild_page_size_does_not_count_as_pending_rebuild() {
    let tmp = TempDir::new().unwrap();
    let index_path = tmp.path().join("index");
    fs::create_dir_all(&index_path).unwrap();

    let db_state = LexicalRebuildDbState {
        db_path: "/tmp/agent_search.db".to_string(),
        total_conversations: 400,
        total_messages: 800,
        storage_fingerprint: "seed:400".to_string(),
    };
    let state = LexicalRebuildState::new(db_state.clone(), 13);
    persist_lexical_rebuild_state(&index_path, &state).unwrap();

    let status = matching_lexical_rebuild_state_status(&index_path, &db_state).unwrap();
    assert!(!status.has_pending_resume);
}

#[test]
fn clear_lexical_rebuild_state_removes_stale_snapshot() {
    let tmp = TempDir::new().unwrap();
    let index_path = tmp.path().join("index");
    fs::create_dir_all(&index_path).unwrap();

    let state = LexicalRebuildState::new(
        LexicalRebuildDbState {
            db_path: "/tmp/agent_search.db".to_string(),
            total_conversations: 12,
            total_messages: 24,
            storage_fingerprint: "seed:12".to_string(),
        },
        LEXICAL_REBUILD_PAGE_SIZE,
    );
    persist_lexical_rebuild_state(&index_path, &state).unwrap();
    assert!(
        load_lexical_rebuild_snapshot(&index_path, Path::new("/tmp/agent_search.db"))
            .unwrap()
            .is_some()
    );

    clear_lexical_rebuild_state(&index_path).unwrap();
    assert!(
        load_lexical_rebuild_snapshot(&index_path, Path::new("/tmp/agent_search.db"))
            .unwrap()
            .is_none()
    );
}

#[test]
fn load_active_lexical_rebuild_pipeline_runtime_ignores_empty_snapshot() {
    let tmp = TempDir::new().unwrap();
    let index_path = tmp.path().join("index");
    fs::create_dir_all(&index_path).unwrap();

    let state = LexicalRebuildState::new(
        LexicalRebuildDbState {
            db_path: "/tmp/agent_search.db".to_string(),
            total_conversations: 12,
            total_messages: 24,
            storage_fingerprint: "seed:12".to_string(),
        },
        LEXICAL_REBUILD_PAGE_SIZE,
    );
    persist_lexical_rebuild_state(&index_path, &state).unwrap();

    assert!(
        load_active_lexical_rebuild_pipeline_runtime(
            &index_path,
            Path::new("/tmp/agent_search.db")
        )
        .unwrap()
        .is_none()
    );
}

#[test]
fn prepare_lexical_rebuild_state_for_active_run_clears_stale_runtime_and_persists() {
    let tmp = TempDir::new().unwrap();
    let index_path = tmp.path().join("index");
    fs::create_dir_all(&index_path).unwrap();

    let mut state = LexicalRebuildState::new(
        LexicalRebuildDbState {
            db_path: "/tmp/agent_search.db".to_string(),
            total_conversations: 12,
            total_messages: 24,
            storage_fingerprint: "seed:12".to_string(),
        },
        LEXICAL_REBUILD_PAGE_SIZE,
    );
    state.committed_offset = 4;
    state.committed_conversation_id = Some(4);
    state.processed_conversations = 4;
    state.indexed_docs = 8;
    state.set_runtime(&LexicalRebuildPipelineRuntimeSnapshot {
        queue_depth: 2,
        inflight_message_bytes: 32_768,
        max_message_bytes_in_flight: 65_536,
        pending_batch_conversations: 4,
        pending_batch_message_bytes: 65_536,
        page_prep_workers: 4,
        active_page_prep_jobs: 1,
        ordered_buffered_pages: 3,
        budget_generation: 2,
        producer_budget_wait_count: 1,
        producer_budget_wait_ms: 11,
        producer_handoff_wait_count: 2,
        producer_handoff_wait_ms: 22,
        host_loadavg_1m_milli: None,
        controller_mode: "steady".to_string(),
        controller_reason: "first_durable_commit_promoted_steady_budget".to_string(),
        staged_merge_workers_max: 0,
        staged_merge_allowed_jobs: 0,
        staged_merge_active_jobs: 0,
        staged_merge_ready_artifacts: 0,
        staged_merge_ready_groups: 0,
        staged_merge_controller_reason: String::new(),
        staged_shard_build_workers_max: 0,
        staged_shard_build_allowed_jobs: 0,
        staged_shard_build_active_jobs: 0,
        staged_shard_build_pending_jobs: 0,
        staged_shard_build_controller_reason: String::new(),
        updated_at_ms: 1_733_000_224_000_i64,
        ..LexicalRebuildPipelineRuntimeSnapshot::default()
    });
    persist_lexical_rebuild_state(&index_path, &state).unwrap();

    prepare_lexical_rebuild_state_for_active_run(&index_path, &mut state).unwrap();

    assert_eq!(
        state.runtime,
        LexicalRebuildPipelineRuntimeSnapshot::default()
    );
    let persisted = load_lexical_rebuild_state(&index_path)
        .unwrap()
        .expect("prepared checkpoint should persist");
    assert_eq!(
        persisted.runtime,
        LexicalRebuildPipelineRuntimeSnapshot::default()
    );
}

#[test]
fn prepare_lexical_rebuild_state_for_active_run_preserves_completed_runtime_snapshot() {
    let tmp = TempDir::new().unwrap();
    let index_path = tmp.path().join("index");
    fs::create_dir_all(&index_path).unwrap();

    let mut state = LexicalRebuildState::new(
        LexicalRebuildDbState {
            db_path: "/tmp/agent_search.db".to_string(),
            total_conversations: 12,
            total_messages: 24,
            storage_fingerprint: "seed:12".to_string(),
        },
        LEXICAL_REBUILD_PAGE_SIZE,
    );
    state.mark_completed(Some("stable-meta".to_string()));
    persist_lexical_rebuild_state(&index_path, &state).unwrap();

    prepare_lexical_rebuild_state_for_active_run(&index_path, &mut state).unwrap();

    assert!(state.completed);
    let persisted = load_lexical_rebuild_state(&index_path)
        .unwrap()
        .expect("completed checkpoint should persist");
    assert!(persisted.completed);
}

#[test]
fn persist_lexical_rebuild_state_for_active_run_start_overwrites_stale_checkpoint() {
    let tmp = TempDir::new().unwrap();
    let index_path = tmp.path().join("index");
    fs::create_dir_all(&index_path).unwrap();

    let mut stale_state = LexicalRebuildState::new(
        LexicalRebuildDbState {
            db_path: "/tmp/agent_search.db".to_string(),
            total_conversations: 999,
            total_messages: 1998,
            storage_fingerprint: "stale-seed".to_string(),
        },
        LEXICAL_REBUILD_PAGE_SIZE,
    );
    stale_state.committed_offset = 500;
    stale_state.committed_conversation_id = Some(500);
    stale_state.processed_conversations = 500;
    stale_state.indexed_docs = 1000;
    stale_state.set_runtime(&LexicalRebuildPipelineRuntimeSnapshot {
        queue_depth: 2,
        inflight_message_bytes: 32_768,
        max_message_bytes_in_flight: 65_536,
        pending_batch_conversations: 4,
        pending_batch_message_bytes: 65_536,
        page_prep_workers: 4,
        active_page_prep_jobs: 1,
        ordered_buffered_pages: 3,
        budget_generation: 2,
        producer_budget_wait_count: 1,
        producer_budget_wait_ms: 11,
        producer_handoff_wait_count: 2,
        producer_handoff_wait_ms: 22,
        host_loadavg_1m_milli: None,
        controller_mode: "steady".to_string(),
        controller_reason: "first_durable_commit_promoted_steady_budget".to_string(),
        staged_merge_workers_max: 0,
        staged_merge_allowed_jobs: 0,
        staged_merge_active_jobs: 0,
        staged_merge_ready_artifacts: 0,
        staged_merge_ready_groups: 0,
        staged_merge_controller_reason: String::new(),
        staged_shard_build_workers_max: 0,
        staged_shard_build_allowed_jobs: 0,
        staged_shard_build_active_jobs: 0,
        staged_shard_build_pending_jobs: 0,
        staged_shard_build_controller_reason: String::new(),
        updated_at_ms: 1_733_000_224_000_i64,
        ..LexicalRebuildPipelineRuntimeSnapshot::default()
    });
    persist_lexical_rebuild_state(&index_path, &stale_state).unwrap();

    let fresh_state = LexicalRebuildState::new(
        LexicalRebuildDbState {
            db_path: "/tmp/agent_search.db".to_string(),
            total_conversations: 12,
            total_messages: 24,
            storage_fingerprint: "fresh-seed".to_string(),
        },
        LEXICAL_REBUILD_PAGE_SIZE,
    );
    persist_lexical_rebuild_state_for_active_run_start(&index_path, &fresh_state).unwrap();

    let persisted = load_lexical_rebuild_state(&index_path)
        .unwrap()
        .expect("fresh startup checkpoint should persist");
    assert_eq!(persisted.db.total_conversations, 12);
    assert_eq!(persisted.db.total_messages, 24);
    assert_eq!(persisted.db.storage_fingerprint, "fresh-seed");
    assert_eq!(persisted.committed_offset, 0);
    assert_eq!(persisted.committed_conversation_id, None);
    assert_eq!(persisted.processed_conversations, 0);
    assert_eq!(persisted.indexed_docs, 0);
    assert_eq!(
        persisted.runtime,
        LexicalRebuildPipelineRuntimeSnapshot::default()
    );
}

#[test]
fn persist_pending_lexical_rebuild_progress_refreshes_runtime_when_progress_is_unchanged() {
    let tmp = TempDir::new().unwrap();
    let index_path = tmp.path().join("index");
    fs::create_dir_all(&index_path).unwrap();
    fs::write(index_path.join("meta.json"), b"stable-meta").unwrap();

    let mut state = LexicalRebuildState::new(
        LexicalRebuildDbState {
            db_path: "/tmp/agent_search.db".to_string(),
            total_conversations: 12,
            total_messages: 24,
            storage_fingerprint: "seed:12".to_string(),
        },
        LEXICAL_REBUILD_PAGE_SIZE,
    );
    state.committed_offset = 4;
    state.committed_conversation_id = Some(4);
    state.processed_conversations = 4;
    state.indexed_docs = 8;
    state.record_pending_commit(Some(6), 6, 12, index_meta_fingerprint(&index_path).unwrap());
    state.set_runtime(&LexicalRebuildPipelineRuntimeSnapshot {
        queue_depth: 1,
        inflight_message_bytes: 1_024,
        max_message_bytes_in_flight: 4_096,
        pending_batch_conversations: 1,
        pending_batch_message_bytes: 2_048,
        page_prep_workers: 2,
        active_page_prep_jobs: 1,
        ordered_buffered_pages: 0,
        budget_generation: 1,
        producer_budget_wait_count: 1,
        producer_budget_wait_ms: 3,
        producer_handoff_wait_count: 0,
        producer_handoff_wait_ms: 0,
        host_loadavg_1m_milli: None,
        controller_mode: "startup".to_string(),
        controller_reason: "seeded-runtime".to_string(),
        staged_merge_workers_max: 0,
        staged_merge_allowed_jobs: 0,
        staged_merge_active_jobs: 0,
        staged_merge_ready_artifacts: 0,
        staged_merge_ready_groups: 0,
        staged_merge_controller_reason: String::new(),
        staged_shard_build_workers_max: 0,
        staged_shard_build_allowed_jobs: 0,
        staged_shard_build_active_jobs: 0,
        staged_shard_build_pending_jobs: 0,
        staged_shard_build_controller_reason: String::new(),
        updated_at_ms: 1_733_000_111_000_i64,
        ..LexicalRebuildPipelineRuntimeSnapshot::default()
    });
    persist_lexical_rebuild_state(&index_path, &state).unwrap();

    let refreshed_runtime = LexicalRebuildPipelineRuntimeSnapshot {
        queue_depth: 3,
        inflight_message_bytes: 4_096,
        max_message_bytes_in_flight: 16_384,
        pending_batch_conversations: 2,
        pending_batch_message_bytes: 8_192,
        page_prep_workers: 4,
        active_page_prep_jobs: 2,
        ordered_buffered_pages: 1,
        budget_generation: 2,
        producer_budget_wait_count: 2,
        producer_budget_wait_ms: 9,
        producer_handoff_wait_count: 1,
        producer_handoff_wait_ms: 5,
        host_loadavg_1m_milli: Some(7_250),
        controller_mode: "pressure_limited".to_string(),
        controller_reason: "queue_depth_3_reached_pipeline_capacity_3".to_string(),
        staged_merge_workers_max: 0,
        staged_merge_allowed_jobs: 0,
        staged_merge_active_jobs: 0,
        staged_merge_ready_artifacts: 0,
        staged_merge_ready_groups: 0,
        staged_merge_controller_reason: String::new(),
        staged_shard_build_workers_max: 0,
        staged_shard_build_allowed_jobs: 0,
        staged_shard_build_active_jobs: 0,
        staged_shard_build_pending_jobs: 0,
        staged_shard_build_controller_reason: String::new(),
        updated_at_ms: 1_733_000_222_000_i64,
        ..LexicalRebuildPipelineRuntimeSnapshot::default()
    };

    persist_pending_lexical_rebuild_progress(
        &index_path,
        &index_path,
        &mut state,
        Some(6),
        6,
        12,
        &refreshed_runtime,
        0,
    )
    .unwrap();

    assert_eq!(state.runtime, refreshed_runtime);
    let persisted = load_lexical_rebuild_state(&index_path)
        .unwrap()
        .expect("heartbeat checkpoint should persist");
    assert_eq!(persisted.runtime, refreshed_runtime);
    assert_eq!(
        persisted
            .pending
            .as_ref()
            .map(|pending| pending.next_conversation_id),
        Some(Some(6))
    );
}

#[test]
fn persist_pending_lexical_rebuild_progress_treats_active_waiters_as_transient() {
    let tmp = TempDir::new().unwrap();
    let index_path = tmp.path().join("index");
    fs::create_dir_all(&index_path).unwrap();
    fs::write(index_path.join("meta.json"), b"stable-meta").unwrap();

    let base_runtime = LexicalRebuildPipelineRuntimeSnapshot {
        queue_depth: 1,
        inflight_message_bytes: 1_024,
        max_message_bytes_in_flight: 4_096,
        pending_batch_conversations: 1,
        pending_batch_message_bytes: 2_048,
        page_prep_workers: 2,
        active_page_prep_jobs: 1,
        ordered_buffered_pages: 0,
        budget_generation: 1,
        producer_budget_wait_count: 1,
        producer_budget_wait_ms: 3,
        producer_handoff_wait_count: 0,
        producer_handoff_wait_ms: 0,
        host_loadavg_1m_milli: None,
        controller_mode: "startup".to_string(),
        controller_reason: "seeded-runtime".to_string(),
        staged_merge_workers_max: 0,
        staged_merge_allowed_jobs: 0,
        staged_merge_active_jobs: 0,
        staged_merge_ready_artifacts: 0,
        staged_merge_ready_groups: 0,
        staged_merge_controller_reason: String::new(),
        staged_shard_build_workers_max: 0,
        staged_shard_build_allowed_jobs: 0,
        staged_shard_build_active_jobs: 0,
        staged_shard_build_pending_jobs: 0,
        staged_shard_build_controller_reason: String::new(),
        updated_at_ms: 1_733_000_111_000_i64,
        ..LexicalRebuildPipelineRuntimeSnapshot::default()
    };
    let mut state = LexicalRebuildState::new(
        LexicalRebuildDbState {
            db_path: "/tmp/agent_search.db".to_string(),
            total_conversations: 12,
            total_messages: 24,
            storage_fingerprint: "seed:12".to_string(),
        },
        LEXICAL_REBUILD_PAGE_SIZE,
    );
    state.record_pending_commit(Some(6), 6, 12, index_meta_fingerprint(&index_path).unwrap());
    state.set_runtime(&base_runtime);
    state.updated_at_ms = 1_234;
    persist_lexical_rebuild_state(&index_path, &state).unwrap();
    let before = fs::read(lexical_rebuild_state_path(&index_path)).unwrap();

    let live_runtime = LexicalRebuildPipelineRuntimeSnapshot {
        producer_budget_waiter_count: 2,
        ..base_runtime.clone()
    };
    assert!(
        live_runtime.is_observed(),
        "the live active-waiter signal should still count for the controller"
    );

    persist_pending_lexical_rebuild_progress(
        &index_path,
        &index_path,
        &mut state,
        Some(6),
        6,
        12,
        &live_runtime,
        0,
    )
    .unwrap();

    assert_eq!(
        state.updated_at_ms, 1_234,
        "active waiter count alone should not rewrite the durable checkpoint"
    );
    assert_eq!(state.runtime, base_runtime);
    assert_eq!(
        fs::read(lexical_rebuild_state_path(&index_path)).unwrap(),
        before,
        "serde-skipped active waiter telemetry should not churn checkpoint bytes"
    );
    let persisted = load_lexical_rebuild_state(&index_path)
        .unwrap()
        .expect("checkpoint should remain readable");
    assert_eq!(persisted.runtime, base_runtime);
    assert_eq!(persisted.runtime.producer_budget_waiter_count, 0);
}

#[test]
fn stale_stage_heartbeat_does_not_rewind_rebuild_checkpoint() {
    let tmp = TempDir::new().unwrap();
    let index_path = tmp.path().join("index");
    fs::create_dir_all(&index_path).unwrap();
    fs::write(index_path.join("meta.json"), b"stable-meta").unwrap();

    let mut state = LexicalRebuildState::new(
        LexicalRebuildDbState {
            db_path: "/tmp/agent_search.db".to_string(),
            total_conversations: 12,
            total_messages: 24,
            storage_fingerprint: "seed:12".to_string(),
        },
        LEXICAL_REBUILD_PAGE_SIZE,
    );
    state.committed_offset = 4;
    state.committed_conversation_id = Some(4);
    state.processed_conversations = 4;
    state.indexed_docs = 8;
    let stable_meta = index_meta_fingerprint(&index_path).unwrap();
    state.record_pending_commit(Some(6), 6, 12, stable_meta);
    persist_lexical_rebuild_state(&index_path, &state).unwrap();

    let stale_stage_runtime = LexicalRebuildPipelineRuntimeSnapshot {
        queue_depth: 2,
        inflight_message_bytes: 2_048,
        max_message_bytes_in_flight: 8_192,
        pending_batch_conversations: 1,
        pending_batch_message_bytes: 4_096,
        page_prep_workers: 2,
        active_page_prep_jobs: 1,
        producer_handoff_wait_count: 1,
        producer_handoff_wait_ms: 5,
        controller_mode: "startup".to_string(),
        controller_reason: "stale-stage-heartbeat".to_string(),
        updated_at_ms: 1_733_000_777_000_i64,
        ..Default::default()
    };

    persist_pending_lexical_rebuild_progress(
        &index_path,
        &index_path,
        &mut state,
        Some(5),
        5,
        10,
        &stale_stage_runtime,
        0,
    )
    .unwrap();

    assert_eq!(
        state.reported_processed_conversations(),
        6,
        "stale stage heartbeat must not move the restart checkpoint backwards"
    );
    assert_eq!(state.reported_indexed_docs(), 12);
    assert_eq!(
        state.reported_committed_conversation_id(),
        Some(6),
        "stale next_conversation_id must not replace the monotone pending cursor"
    );
    assert_eq!(
        state.runtime, stale_stage_runtime,
        "runtime telemetry should still refresh for attach/status visibility"
    );

    let persisted = load_lexical_rebuild_state(&index_path)
        .unwrap()
        .expect("monotone checkpoint should remain persisted");
    assert_eq!(persisted.reported_processed_conversations(), 6);
    assert_eq!(persisted.reported_indexed_docs(), 12);
    assert_eq!(persisted.reported_committed_conversation_id(), Some(6));
    assert_eq!(persisted.runtime, stale_stage_runtime);
}

#[test]
fn persist_pending_lexical_rebuild_progress_can_reuse_cached_base_meta_fingerprint() {
    let tmp = TempDir::new().unwrap();
    let index_path = tmp.path().join("index");
    fs::create_dir_all(&index_path).unwrap();
    fs::write(index_path.join("meta.json"), b"old-live-meta").unwrap();
    let cached_base_meta_fingerprint = index_meta_fingerprint(&index_path).unwrap();
    fs::write(index_path.join("meta.json"), b"new-live-meta").unwrap();

    let runtime = LexicalRebuildPipelineRuntimeSnapshot {
        queue_depth: 2,
        inflight_message_bytes: 2_048,
        max_message_bytes_in_flight: 8_192,
        pending_batch_conversations: 1,
        pending_batch_message_bytes: 4_096,
        page_prep_workers: 2,
        active_page_prep_jobs: 1,
        ordered_buffered_pages: 0,
        budget_generation: 1,
        producer_budget_wait_count: 1,
        producer_budget_wait_ms: 2,
        producer_handoff_wait_count: 0,
        producer_handoff_wait_ms: 0,
        host_loadavg_1m_milli: None,
        controller_mode: "startup".to_string(),
        controller_reason: "cached-live-meta".to_string(),
        staged_merge_workers_max: 0,
        staged_merge_allowed_jobs: 0,
        staged_merge_active_jobs: 0,
        staged_merge_ready_artifacts: 0,
        staged_merge_ready_groups: 0,
        staged_merge_controller_reason: String::new(),
        staged_shard_build_workers_max: 0,
        staged_shard_build_allowed_jobs: 0,
        staged_shard_build_active_jobs: 0,
        staged_shard_build_pending_jobs: 0,
        staged_shard_build_controller_reason: String::new(),
        updated_at_ms: 1_733_000_333_000_i64,
        ..LexicalRebuildPipelineRuntimeSnapshot::default()
    };
    let mut state = LexicalRebuildState::new(
        LexicalRebuildDbState {
            db_path: "/tmp/agent_search.db".to_string(),
            total_conversations: 12,
            total_messages: 24,
            storage_fingerprint: "seed:12".to_string(),
        },
        LEXICAL_REBUILD_PAGE_SIZE,
    );

    persist_pending_lexical_rebuild_progress_with_base_meta_fingerprint(
        &index_path,
        &mut state,
        Some(5),
        5,
        10,
        &runtime,
        cached_base_meta_fingerprint.clone(),
        0,
    )
    .unwrap();

    let persisted = load_lexical_rebuild_state(&index_path)
        .unwrap()
        .expect("pending checkpoint should persist");
    assert_eq!(
        persisted
            .pending
            .as_ref()
            .and_then(|pending| pending.base_meta_fingerprint.as_deref()),
        cached_base_meta_fingerprint.as_deref()
    );
    assert_ne!(
        persisted
            .pending
            .as_ref()
            .and_then(|pending| pending.base_meta_fingerprint.as_deref()),
        index_meta_fingerprint(&index_path).unwrap().as_deref()
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
fn commit_lexical_rebuild_progress_without_finalized_checkpoint_persist_leaves_recoverable_pending_state()
 {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let index_path = index_dir(&data_dir).unwrap();
    let mut index = TantivyIndex::open_or_create(&index_path).unwrap();
    let conversation = norm_conv(Some("final-commit"), vec![norm_msg(0, 1_700_000_000_000)]);
    index
        .add_messages_with_conversation_id(&conversation, &conversation.messages, Some(1))
        .unwrap();

    let mut state = LexicalRebuildState::new(
        LexicalRebuildDbState {
            db_path: "/tmp/agent_search.db".to_string(),
            total_conversations: 1,
            total_messages: 1,
            storage_fingerprint: "seed:1".to_string(),
        },
        LEXICAL_REBUILD_PAGE_SIZE,
    );
    let runtime = LexicalRebuildPipelineRuntimeSnapshot {
        queue_depth: 0,
        inflight_message_bytes: 0,
        max_message_bytes_in_flight: 4_096,
        pending_batch_conversations: 1,
        pending_batch_message_bytes: 128,
        page_prep_workers: 1,
        active_page_prep_jobs: 0,
        ordered_buffered_pages: 0,
        budget_generation: 1,
        producer_budget_wait_count: 0,
        producer_budget_wait_ms: 0,
        producer_handoff_wait_count: 1,
        producer_handoff_wait_ms: 4,
        host_loadavg_1m_milli: None,
        controller_mode: "steady".to_string(),
        controller_reason: "final-commit".to_string(),
        staged_merge_workers_max: 0,
        staged_merge_allowed_jobs: 0,
        staged_merge_active_jobs: 0,
        staged_merge_ready_artifacts: 0,
        staged_merge_ready_groups: 0,
        staged_merge_controller_reason: String::new(),
        staged_shard_build_workers_max: 0,
        staged_shard_build_allowed_jobs: 0,
        staged_shard_build_active_jobs: 0,
        staged_shard_build_pending_jobs: 0,
        staged_shard_build_controller_reason: String::new(),
        updated_at_ms: 1_733_000_555_000_i64,
        ..LexicalRebuildPipelineRuntimeSnapshot::default()
    };

    commit_lexical_rebuild_progress(
        &index_path,
        &index_path,
        &mut state,
        Some(1),
        1,
        1,
        &runtime,
        &mut index,
        false,
        None,
        0,
    )
    .unwrap();

    assert!(state.pending.is_none());
    assert_eq!(state.committed_offset, 1);
    assert_eq!(state.committed_conversation_id, Some(1));
    assert_eq!(state.processed_conversations, 1);
    assert_eq!(state.indexed_docs, 1);
    assert_eq!(
        state.committed_meta_fingerprint.as_deref(),
        index_meta_fingerprint(&index_path).unwrap().as_deref()
    );

    let persisted = load_lexical_rebuild_state(&index_path)
        .unwrap()
        .expect("pending checkpoint should remain on disk until completion");
    assert!(persisted.pending.is_some());
    assert_eq!(persisted.committed_meta_fingerprint, None);
    assert_eq!(persisted.runtime, runtime);

    let reconciled = reconcile_pending_lexical_commit(&index_path, persisted).unwrap();
    assert!(reconciled.pending.is_none());
    assert_eq!(reconciled.committed_offset, 1);
    assert_eq!(reconciled.committed_conversation_id, Some(1));
    assert_eq!(reconciled.processed_conversations, 1);
    assert_eq!(reconciled.indexed_docs, 1);
    assert_eq!(
        reconciled.committed_meta_fingerprint.as_deref(),
        index_meta_fingerprint(&index_path).unwrap().as_deref()
    );
}

#[test]
fn maybe_persist_staged_lexical_rebuild_progress_skips_write_until_heartbeat_is_due() {
    let tmp = TempDir::new().unwrap();
    let index_path = tmp.path().join("index");
    fs::create_dir_all(&index_path).unwrap();
    fs::write(index_path.join("meta.json"), b"stable-meta").unwrap();

    let mut state = LexicalRebuildState::new(
        LexicalRebuildDbState {
            db_path: "/tmp/agent_search.db".to_string(),
            total_conversations: 12,
            total_messages: 24,
            storage_fingerprint: "seed:12".to_string(),
        },
        LEXICAL_REBUILD_PAGE_SIZE,
    );
    let runtime = LexicalRebuildPipelineRuntimeSnapshot {
        queue_depth: 1,
        inflight_message_bytes: 1_024,
        max_message_bytes_in_flight: 4_096,
        pending_batch_conversations: 1,
        pending_batch_message_bytes: 2_048,
        page_prep_workers: 2,
        active_page_prep_jobs: 1,
        ordered_buffered_pages: 0,
        budget_generation: 1,
        producer_budget_wait_count: 1,
        producer_budget_wait_ms: 3,
        producer_handoff_wait_count: 0,
        producer_handoff_wait_ms: 0,
        host_loadavg_1m_milli: None,
        controller_mode: "startup".to_string(),
        controller_reason: "staged-heartbeat".to_string(),
        staged_merge_workers_max: 1,
        staged_merge_allowed_jobs: 0,
        staged_merge_active_jobs: 0,
        staged_merge_ready_artifacts: 0,
        staged_merge_ready_groups: 0,
        staged_merge_controller_reason: "warming".to_string(),
        staged_shard_build_workers_max: 4,
        staged_shard_build_allowed_jobs: 2,
        staged_shard_build_active_jobs: 2,
        staged_shard_build_pending_jobs: 3,
        staged_shard_build_controller_reason: "backlog".to_string(),
        updated_at_ms: 1_733_000_444_000_i64,
        ..LexicalRebuildPipelineRuntimeSnapshot::default()
    };
    let base_meta_fingerprint = index_meta_fingerprint(&index_path).unwrap();
    let mut conversations_since_progress_persist = 1usize;
    let mut last_progress_persist = Instant::now();

    let persisted = maybe_persist_staged_lexical_rebuild_progress(
        &index_path,
        &mut state,
        Some(1),
        1,
        2,
        &runtime,
        base_meta_fingerprint.as_deref(),
        false,
        &mut conversations_since_progress_persist,
        8,
        &mut last_progress_persist,
        Duration::from_secs(60),
        None,
        None,
    )
    .unwrap();

    assert!(!persisted);
    assert_eq!(conversations_since_progress_persist, 1);
    assert!(
        load_lexical_rebuild_state(&index_path).unwrap().is_none(),
        "staged progress should not rewrite the state file before the heartbeat is due"
    );
}

#[test]
fn maybe_persist_staged_lexical_rebuild_progress_force_refreshes_runtime_without_new_progress() {
    let tmp = TempDir::new().unwrap();
    let index_path = tmp.path().join("index");
    fs::create_dir_all(&index_path).unwrap();
    fs::write(index_path.join("meta.json"), b"stable-meta").unwrap();

    let mut state = LexicalRebuildState::new(
        LexicalRebuildDbState {
            db_path: "/tmp/agent_search.db".to_string(),
            total_conversations: 12,
            total_messages: 24,
            storage_fingerprint: "seed:12".to_string(),
        },
        LEXICAL_REBUILD_PAGE_SIZE,
    );
    let base_meta_fingerprint = index_meta_fingerprint(&index_path).unwrap();
    state.record_pending_commit(Some(6), 6, 12, base_meta_fingerprint.clone());
    state.set_runtime(&LexicalRebuildPipelineRuntimeSnapshot {
        queue_depth: 1,
        inflight_message_bytes: 1_024,
        max_message_bytes_in_flight: 4_096,
        pending_batch_conversations: 1,
        pending_batch_message_bytes: 2_048,
        page_prep_workers: 2,
        active_page_prep_jobs: 1,
        ordered_buffered_pages: 0,
        budget_generation: 1,
        producer_budget_wait_count: 1,
        producer_budget_wait_ms: 3,
        producer_handoff_wait_count: 0,
        producer_handoff_wait_ms: 0,
        host_loadavg_1m_milli: None,
        controller_mode: "startup".to_string(),
        controller_reason: "seeded-runtime".to_string(),
        staged_merge_workers_max: 1,
        staged_merge_allowed_jobs: 0,
        staged_merge_active_jobs: 0,
        staged_merge_ready_artifacts: 0,
        staged_merge_ready_groups: 0,
        staged_merge_controller_reason: "warming".to_string(),
        staged_shard_build_workers_max: 4,
        staged_shard_build_allowed_jobs: 2,
        staged_shard_build_active_jobs: 2,
        staged_shard_build_pending_jobs: 3,
        staged_shard_build_controller_reason: "backlog".to_string(),
        updated_at_ms: 1_733_000_555_000_i64,
        ..LexicalRebuildPipelineRuntimeSnapshot::default()
    });
    persist_lexical_rebuild_state(&index_path, &state).unwrap();

    let refreshed_runtime = LexicalRebuildPipelineRuntimeSnapshot {
        queue_depth: 0,
        inflight_message_bytes: 0,
        max_message_bytes_in_flight: 4_096,
        pending_batch_conversations: 0,
        pending_batch_message_bytes: 0,
        page_prep_workers: 2,
        active_page_prep_jobs: 0,
        ordered_buffered_pages: 0,
        budget_generation: 2,
        producer_budget_wait_count: 1,
        producer_budget_wait_ms: 3,
        producer_handoff_wait_count: 1,
        producer_handoff_wait_ms: 5,
        host_loadavg_1m_milli: Some(4_250),
        controller_mode: "merge_tail".to_string(),
        controller_reason: "draining_eager_merges".to_string(),
        staged_merge_workers_max: 2,
        staged_merge_allowed_jobs: 2,
        staged_merge_active_jobs: 1,
        staged_merge_ready_artifacts: 3,
        staged_merge_ready_groups: 1,
        staged_merge_controller_reason: "merge_backlog".to_string(),
        staged_shard_build_workers_max: 4,
        staged_shard_build_allowed_jobs: 0,
        staged_shard_build_active_jobs: 0,
        staged_shard_build_pending_jobs: 0,
        staged_shard_build_controller_reason: "builders_idle".to_string(),
        updated_at_ms: 1_733_000_666_000_i64,
        ..LexicalRebuildPipelineRuntimeSnapshot::default()
    };
    let mut conversations_since_progress_persist = 0usize;
    let mut last_progress_persist = Instant::now();

    let persisted = maybe_persist_staged_lexical_rebuild_progress(
        &index_path,
        &mut state,
        Some(6),
        6,
        12,
        &refreshed_runtime,
        base_meta_fingerprint.as_deref(),
        true,
        &mut conversations_since_progress_persist,
        64,
        &mut last_progress_persist,
        Duration::from_secs(60),
        None,
        None,
    )
    .unwrap();

    assert!(persisted);
    assert_eq!(state.runtime, refreshed_runtime);
    let persisted_state = load_lexical_rebuild_state(&index_path)
        .unwrap()
        .expect("forced staged heartbeat should persist");
    assert_eq!(persisted_state.runtime, refreshed_runtime);
    assert_eq!(
        persisted_state
            .pending
            .as_ref()
            .map(|pending| pending.next_conversation_id),
        Some(Some(6)),
        "forced staged heartbeat should keep the existing pending progress visible"
    );
}

#[test]
fn load_lexical_rebuild_checkpoint_reports_pending_progress() {
    let tmp = TempDir::new().unwrap();
    let index_path = tmp.path().join("index");
    fs::create_dir_all(&index_path).unwrap();
    fs::write(index_path.join("meta.json"), b"stable-meta").unwrap();

    let mut state = LexicalRebuildState::new(
        LexicalRebuildDbState {
            db_path: "/tmp/agent_search.db".to_string(),
            total_conversations: 12,
            total_messages: 24,
            storage_fingerprint: "seed:12".to_string(),
        },
        LEXICAL_REBUILD_PAGE_SIZE,
    );
    state.committed_offset = 4;
    state.committed_conversation_id = Some(4);
    state.processed_conversations = 4;
    state.indexed_docs = 20;
    state.record_pending_commit(Some(7), 7, 35, index_meta_fingerprint(&index_path).unwrap());
    persist_lexical_rebuild_state(&index_path, &state).unwrap();

    let checkpoint = load_lexical_rebuild_checkpoint(&index_path)
        .unwrap()
        .expect("checkpoint should load");
    assert_eq!(checkpoint.committed_offset, 4);
    assert_eq!(checkpoint.committed_conversation_id, Some(7));
    assert_eq!(
        checkpoint.processed_conversations, 7,
        "pending rebuild progress should stay visible to status/health readers"
    );
    assert_eq!(
        checkpoint.indexed_docs, 35,
        "pending indexed doc counts should stay visible to status/health readers"
    );
}

#[test]
fn status_skip_open_lane_sees_checkpoint_fingerprint_mismatch_from_sidecar() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("agent_search.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    let agent = crate::model::types::Agent {
        id: None,
        slug: "tester".into(),
        name: "Tester".into(),
        version: None,
        kind: crate::model::types::AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();
    let insert_one = |external: &str, idx: i64| {
        let conv = norm_conv(Some(external), vec![norm_msg(idx, 1_700_000_000_000 + idx)]);
        storage
            .insert_conversation_tree(
                agent_id,
                None,
                &crate::model::types::Conversation {
                    id: None,
                    agent_slug: conv.agent_slug.clone(),
                    workspace: conv.workspace.clone(),
                    external_id: conv.external_id.clone(),
                    title: conv.title.clone(),
                    source_path: conv.source_path.clone(),
                    started_at: conv.started_at,
                    ended_at: conv.ended_at,
                    approx_tokens: None,
                    metadata_json: conv.metadata.clone(),
                    messages: conv
                        .messages
                        .iter()
                        .map(|m| crate::model::types::Message {
                            id: None,
                            idx: m.idx,
                            role: crate::model::types::MessageRole::User,
                            author: m.author.clone(),
                            created_at: m.created_at,
                            content: m.content.clone(),
                            extra_json: m.extra.clone(),
                            snippets: Vec::new(),
                        })
                        .collect(),
                    source_id: "local".to_string(),
                    origin_host: None,
                },
            )
            .unwrap();
    };
    insert_one("gh353-a", 0);

    let index_path = crate::search::tantivy::expected_index_dir(&data_dir);
    fs::create_dir_all(&index_path).unwrap();
    fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .unwrap();
    fs::write(index_path.join("meta.json"), b"stable-meta").unwrap();

    // A completed checkpoint frozen at the one-conversation fingerprint.
    let mut state = LexicalRebuildState::new(
        lexical_rebuild_db_state(&storage, &db_path).unwrap(),
        LEXICAL_REBUILD_PAGE_SIZE,
    );
    state.mark_completed(index_meta_fingerprint(&index_path).unwrap());
    persist_lexical_rebuild_state(&index_path, &state).unwrap();
    let checkpoint_fingerprint = load_lexical_rebuild_checkpoint(&index_path)
        .unwrap()
        .expect("checkpoint")
        .storage_fingerprint;

    // New canonical rows land afterwards: the checkpoint fingerprint is
    // now stale, which is exactly the GH #353 drift.
    insert_one("gh353-b", 1);
    // Search runs, computes the current fingerprint through the cached
    // path, and primes the identity-keyed sidecar.
    let current = lexical_storage_fingerprint_for_db_cached(&db_path, &index_path).unwrap();
    assert_ne!(current, checkpoint_fingerprint);

    // The status skip-open lane (db_available, no fingerprint compute)
    // must see the same mismatch without opening the archive.
    let readonly = lexical_storage_fingerprint_for_db_cached_readonly(&db_path, &index_path);
    assert_eq!(readonly.as_deref(), Some(current.as_str()));
    let snapshot = crate::search::asset_state::inspect_search_assets(
        crate::search::asset_state::InspectSearchAssetsInput {
            data_dir: &data_dir,
            db_path: &db_path,
            stale_threshold: 3600,
            last_indexed_at_ms: Some(FrankenStorage::now_millis()),
            now_ms: FrankenStorage::now_millis(),
            maintenance: crate::search::asset_state::SearchMaintenanceSnapshot::default(),
            semantic_preference: crate::search::asset_state::SemanticPreference::HashFallback,
            db_available: true,
            compute_lexical_fingerprint: false,
            inspect_semantic: false,
        },
    )
    .unwrap();
    let lexical = &snapshot.lexical;
    assert_eq!(
        lexical.fingerprint.current_db_fingerprint.as_deref(),
        Some(current.as_str()),
        "skip-open status must surface the sidecar fingerprint search uses"
    );
    assert_eq!(
        lexical.fingerprint.matches_current_db_fingerprint,
        Some(false),
        "GH #353: the fingerprint comparison must be truthful, not null"
    );
    assert_eq!(lexical.status, "stale");
    assert_eq!(
        lexical.checkpoint.db_matches,
        Some(true),
        "checkpoint.db_matches stays path-scoped; the fingerprint truth lives in the fingerprint block"
    );
    let reason = lexical.status_reason.clone().unwrap_or_default();
    assert!(
        reason.contains("fingerprint"),
        "stale reason should name the fingerprint mismatch, got: {reason}"
    );
}

#[test]
fn status_skip_open_lane_stays_null_when_sidecar_does_not_describe_archive() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("agent_search.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    let agent = crate::model::types::Agent {
        id: None,
        slug: "tester".into(),
        name: "Tester".into(),
        version: None,
        kind: crate::model::types::AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();
    let conv = norm_conv(Some("gh353-c"), vec![norm_msg(0, 1_700_000_000_000)]);
    storage
        .insert_conversation_tree(
            agent_id,
            None,
            &crate::model::types::Conversation {
                id: None,
                agent_slug: conv.agent_slug.clone(),
                workspace: conv.workspace.clone(),
                external_id: conv.external_id.clone(),
                title: conv.title.clone(),
                source_path: conv.source_path.clone(),
                started_at: conv.started_at,
                ended_at: conv.ended_at,
                approx_tokens: None,
                metadata_json: conv.metadata.clone(),
                messages: conv
                    .messages
                    .iter()
                    .map(|m| crate::model::types::Message {
                        id: None,
                        idx: m.idx,
                        role: crate::model::types::MessageRole::User,
                        author: m.author.clone(),
                        created_at: m.created_at,
                        content: m.content.clone(),
                        extra_json: m.extra.clone(),
                        snippets: Vec::new(),
                    })
                    .collect(),
                source_id: "local".to_string(),
                origin_host: None,
            },
        )
        .unwrap();
    drop(storage);

    // No sidecar was ever primed for this archive: the skip-open lane
    // must report an honestly null comparison, never an assumed-good one.
    assert_eq!(
        lexical_storage_fingerprint_for_db_cached_readonly(
            &db_path,
            &crate::search::tantivy::expected_index_dir(&data_dir),
        ),
        None,
    );
    let index_path = crate::search::tantivy::expected_index_dir(&data_dir);
    fs::create_dir_all(&index_path).unwrap();
    fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .unwrap();
    let snapshot = crate::search::asset_state::inspect_search_assets(
        crate::search::asset_state::InspectSearchAssetsInput {
            data_dir: &data_dir,
            db_path: &db_path,
            stale_threshold: 3600,
            last_indexed_at_ms: Some(FrankenStorage::now_millis()),
            now_ms: FrankenStorage::now_millis(),
            maintenance: crate::search::asset_state::SearchMaintenanceSnapshot::default(),
            semantic_preference: crate::search::asset_state::SemanticPreference::HashFallback,
            db_available: true,
            compute_lexical_fingerprint: false,
            inspect_semantic: false,
        },
    )
    .unwrap();
    let lexical = &snapshot.lexical;
    assert_eq!(lexical.fingerprint.current_db_fingerprint, None);
    assert_eq!(lexical.fingerprint.matches_current_db_fingerprint, None);
    assert_ne!(lexical.status, "error");
}

#[test]
fn refresh_completed_lexical_rebuild_checkpoint_preserves_content_fingerprint_across_meta_only_writes()
 {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("agent_search.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);

    let agent = crate::model::types::Agent {
        id: None,
        slug: "tester".into(),
        name: "Tester".into(),
        version: None,
        kind: crate::model::types::AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();
    let conv = norm_conv(
        Some("checkpoint-refresh"),
        vec![norm_msg(0, 1_700_000_000_000)],
    );
    storage
        .insert_conversation_tree(
            agent_id,
            None,
            &crate::model::types::Conversation {
                id: None,
                agent_slug: conv.agent_slug.clone(),
                workspace: conv.workspace.clone(),
                external_id: conv.external_id.clone(),
                title: conv.title.clone(),
                source_path: conv.source_path.clone(),
                started_at: conv.started_at,
                ended_at: conv.ended_at,
                approx_tokens: None,
                metadata_json: conv.metadata.clone(),
                messages: conv
                    .messages
                    .iter()
                    .map(|m| crate::model::types::Message {
                        id: None,
                        idx: m.idx,
                        role: crate::model::types::MessageRole::User,
                        author: m.author.clone(),
                        created_at: m.created_at,
                        content: m.content.clone(),
                        extra_json: m.extra.clone(),
                        snippets: Vec::new(),
                    })
                    .collect(),
                source_id: "local".to_string(),
                origin_host: None,
            },
        )
        .unwrap();

    let index_path = index_dir(&data_dir).unwrap();
    let mut index = TantivyIndex::open_or_create(&index_path).unwrap();
    index
        .add_messages_with_conversation_id(&conv, &conv.messages, Some(1))
        .unwrap();
    index.commit().unwrap();
    drop(index);

    let total_conversations = count_total_conversations_exact(&storage).unwrap();
    let total_messages = count_total_messages_exact(&storage).unwrap();
    let original_fingerprint = lexical_rebuild_storage_fingerprint(&db_path).unwrap();

    let mut state = LexicalRebuildState::new(
        lexical_rebuild_db_state(&storage, &db_path).unwrap(),
        LEXICAL_REBUILD_PAGE_SIZE,
    );
    state.mark_completed(index_meta_fingerprint(&index_path).unwrap());
    persist_lexical_rebuild_state(&index_path, &state).unwrap();

    std::thread::sleep(Duration::from_millis(5));
    storage
        .set_last_indexed_at(FrankenStorage::now_millis())
        .unwrap();
    let changed_fingerprint = lexical_rebuild_storage_fingerprint(&db_path).unwrap();
    assert_eq!(
        original_fingerprint, changed_fingerprint,
        "meta-only writes should not churn the lexical content fingerprint"
    );

    refresh_completed_lexical_rebuild_checkpoint(&storage, &db_path, &data_dir).unwrap();

    let checkpoint = load_lexical_rebuild_checkpoint(&index_path)
        .unwrap()
        .expect("refreshed checkpoint");
    assert!(checkpoint.completed);
    assert_eq!(checkpoint.storage_fingerprint, original_fingerprint);
    assert_eq!(checkpoint.total_conversations, total_conversations);
    assert_eq!(checkpoint.processed_conversations, total_conversations);
    assert_eq!(
        checkpoint.committed_offset,
        i64::try_from(total_conversations).unwrap_or(i64::MAX)
    );
    assert_eq!(checkpoint.committed_conversation_id, Some(1));
    assert_eq!(checkpoint.indexed_docs, total_messages);
}

#[test]
#[serial]
fn refresh_completed_lexical_rebuild_checkpoint_bootstraps_missing_state_from_live_tantivy() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("agent_search.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    let mut index = TantivyIndex::open_or_create(&index_dir(&data_dir).unwrap()).unwrap();
    let convs = vec![norm_conv(
        Some("bootstrap"),
        vec![
            norm_msg(0, 1_700_000_000_000),
            norm_msg(1, 1_700_000_000_100),
        ],
    )];
    ingest_batch(
        &storage,
        Some(&mut index),
        &data_dir,
        &convs,
        &None,
        LexicalPopulationStrategy::IncrementalInline,
        false,
    )
    .unwrap();
    index.commit().unwrap();
    drop(index);

    let index_path = index_dir(&data_dir).unwrap();
    assert!(
        load_lexical_rebuild_checkpoint(&index_path)
            .unwrap()
            .is_none(),
        "plain incremental ingest should start with no lexical checkpoint"
    );

    refresh_completed_lexical_rebuild_checkpoint(&storage, &db_path, &data_dir).unwrap();

    let checkpoint = load_lexical_rebuild_checkpoint(&index_path)
        .unwrap()
        .expect("bootstrapped checkpoint");
    assert!(checkpoint.completed);
    assert_eq!(checkpoint.total_conversations, 1);
    assert_eq!(checkpoint.processed_conversations, 1);
    assert_eq!(checkpoint.indexed_docs - checkpoint.indexed_region_docs, 2);
    assert_eq!(
        checkpoint.storage_fingerprint,
        lexical_rebuild_storage_fingerprint(&db_path).unwrap()
    );
}

/// GH #353: a deferred large-incremental run must break the forever-defer
/// loop by refreshing a STALE lexical checkpoint fingerprint to the live DB
/// fingerprint WHEN the live Tantivy index already matches the canonical
/// message count (the drift was absorbed inline). This is the exact call the
/// deferred branch now makes; the doc-count gate keeps it safe when Tantivy
/// is genuinely behind (verified separately by the gate's own tests).
#[test]
#[serial]
fn gh353_refresh_advances_stale_fingerprint_when_tantivy_is_equivalent() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("agent_search.db");
    let index_path = index_dir(&data_dir).unwrap();
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);

    let mut index = TantivyIndex::open_or_create(&index_path).unwrap();
    let convs = vec![norm_conv(
        Some("c1"),
        vec![
            norm_msg(0, 1_700_000_000_000),
            norm_msg(1, 1_700_000_000_100),
        ],
    )];
    ingest_batch(
        &storage,
        Some(&mut index),
        &data_dir,
        &convs,
        &None,
        LexicalPopulationStrategy::IncrementalInline,
        false,
    )
    .unwrap();
    index.commit().unwrap();
    drop(index);

    let live_fingerprint = lexical_rebuild_storage_fingerprint(&db_path).unwrap();

    // Persist a completed checkpoint carrying a STALE fingerprint — the #353
    // loop state where search keeps deferring on the mismatch and a large
    // incremental run keeps re-preserving it.
    let mut state = LexicalRebuildState::new(
        lexical_rebuild_db_state(&storage, &db_path).unwrap(),
        LEXICAL_REBUILD_PAGE_SIZE,
    );
    state.db.storage_fingerprint = "content-v1:0:0:0".to_string();
    state.mark_completed(index_meta_fingerprint(&index_path).unwrap());
    persist_lexical_rebuild_state(&index_path, &state).unwrap();
    assert_ne!(
        load_lexical_rebuild_checkpoint(&index_path)
            .unwrap()
            .unwrap()
            .storage_fingerprint,
        live_fingerprint,
        "precondition: the checkpoint starts stale"
    );

    refresh_completed_lexical_rebuild_checkpoint(&storage, &db_path, &data_dir).unwrap();

    assert_eq!(
        load_lexical_rebuild_checkpoint(&index_path)
            .unwrap()
            .unwrap()
            .storage_fingerprint,
        live_fingerprint,
        "GH #353: a stale checkpoint must be refreshed to the live fingerprint when Tantivy is equivalent, breaking the forever-defer loop"
    );
}
