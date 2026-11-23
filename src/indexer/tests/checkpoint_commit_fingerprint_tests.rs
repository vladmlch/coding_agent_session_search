use super::super::*;
use super::helpers::*;
use serial_test::serial;
use tempfile::TempDir;

#[test]
fn skip_post_full_scan_authoritative_rebuild_requires_matching_completed_checkpoint_doc_match_and_no_mutations()
 {
    let checkpoint = MatchingLexicalRebuildStateStatus {
        has_pending_resume: false,
        has_completed_checkpoint: true,
        completed_indexed_docs: Some(42),
        completed_exact_totals: Some((7, 42)),
        completed_storage_fingerprint: Some("content-v1:7:42:42".to_string()),
    };

    assert!(should_skip_post_full_scan_authoritative_rebuild(
        true,
        false,
        0,
        &checkpoint,
        CanonicalMutationCounts::default(),
        Some(42),
    ));
    assert!(!should_skip_post_full_scan_authoritative_rebuild(
        false,
        false,
        0,
        &checkpoint,
        CanonicalMutationCounts::default(),
        Some(42),
    ));
    assert!(!should_skip_post_full_scan_authoritative_rebuild(
        true,
        true,
        0,
        &checkpoint,
        CanonicalMutationCounts::default(),
        Some(42),
    ));
    assert!(!should_skip_post_full_scan_authoritative_rebuild(
        true,
        false,
        1,
        &checkpoint,
        CanonicalMutationCounts::default(),
        Some(42),
    ));
    assert!(!should_skip_post_full_scan_authoritative_rebuild(
        true,
        false,
        0,
        &checkpoint,
        CanonicalMutationCounts {
            inserted_conversations: 1,
            inserted_messages: 0,
            updated_messages: 0,
        },
        Some(42),
    ));
    assert!(!should_skip_post_full_scan_authoritative_rebuild(
        true,
        false,
        0,
        &MatchingLexicalRebuildStateStatus::default(),
        CanonicalMutationCounts::default(),
        Some(42),
    ));
    assert!(!should_skip_post_full_scan_authoritative_rebuild(
        true,
        false,
        0,
        &checkpoint,
        CanonicalMutationCounts::default(),
        Some(41),
    ));
    assert!(!should_skip_post_full_scan_authoritative_rebuild(
        true,
        false,
        0,
        &checkpoint,
        CanonicalMutationCounts::default(),
        None,
    ));
}

#[test]
fn lexical_rebuild_db_state_with_exact_totals_matches_settled_storage() {
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
        Some("exact-totals-helper"),
        vec![
            norm_msg(0, 1_700_000_000_000),
            norm_msg(1, 1_700_000_000_001),
        ],
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

    let total_conversations = count_total_conversations_exact(&storage).unwrap();
    let total_messages = count_total_messages_exact(&storage).unwrap();
    let db_state = lexical_rebuild_db_state_with_exact_totals(
        &storage,
        &db_path,
        total_conversations,
        total_messages,
    )
    .unwrap();

    assert_eq!(db_state.total_conversations, total_conversations);
    assert_eq!(db_state.total_messages, total_messages);
    assert_eq!(
        db_state.storage_fingerprint,
        lexical_rebuild_storage_fingerprint(&db_path).unwrap()
    );
}

#[test]
fn persist_final_index_run_metadata_updates_last_scan_ts_and_last_indexed_at_together() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("agent_search.db");
    let storage = FrankenStorage::open(&db_path).unwrap();

    persist_final_index_run_metadata(&storage, &db_path, true, 123, 456).unwrap();

    assert_eq!(storage.get_last_scan_ts().unwrap(), Some(123));
    assert_eq!(storage.get_last_indexed_at().unwrap(), Some(456));
}

#[test]
fn persist_final_index_run_metadata_preserves_last_scan_ts_for_lexical_resume() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("agent_search.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    storage.set_last_scan_ts(123).unwrap();

    persist_final_index_run_metadata(&storage, &db_path, false, 999, 456).unwrap();

    assert_eq!(storage.get_last_scan_ts().unwrap(), Some(123));
    assert_eq!(storage.get_last_indexed_at().unwrap(), Some(456));
}

#[test]
fn persist_final_index_run_metadata_logs_warning_and_returns_ok_when_retries_exhausted() {
    // Bead zz8ni regression: a successful multi-minute rebuild must not
    // be reported as failed just because the tail-end status-marker
    // write lost the writer race to a peer cass process. The function
    // log-swallows a retry-exhausted Err so the outer run reports
    // success; a later incremental cass index rewrites the markers
    // once contention clears.
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("agent_search.db");

    let logs = capture_logs(|| {
        let result = persist_final_index_run_metadata_with_writer(&db_path, true, 999, 456, || {
            Err(anyhow::anyhow!(
                "ephemeral writer preflight write failed for updating final \
                     last_indexed_at after index run at {}: database is busy",
                db_path.display()
            ))
        });
        assert!(
            result.is_ok(),
            "metadata-only contention must not fail a fully-committed rebuild; \
             got {result:?}"
        );
    });

    assert!(
        logs.contains("deferred final index-run metadata update after retries exhausted"),
        "expected deferred-update warning in logs; got:\n{logs}"
    );
    assert!(
        logs.contains("database is busy"),
        "expected underlying writer error surfaced in warn log; got:\n{logs}"
    );
    assert!(
        !logs.contains("updated last_indexed_at for status display"),
        "must not emit the success INFO log when the writer retry was swallowed; got:\n{logs}"
    );
}

#[test]
fn persist_final_index_run_metadata_with_writer_logs_success_info_when_writer_succeeds() {
    // Companion probe: on the happy path both INFO markers fire and
    // the deferred-update warning does NOT.
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("agent_search.db");
    let logs = capture_logs(|| {
        let result =
            persist_final_index_run_metadata_with_writer(&db_path, true, 111, 222, || Ok(()));
        assert!(result.is_ok());
    });
    assert!(
        logs.contains("updated last_scan_ts for incremental indexing"),
        "expected scan_ts INFO log; got:\n{logs}"
    );
    assert!(
        logs.contains("updated last_indexed_at for status display"),
        "expected indexed_at INFO log; got:\n{logs}"
    );
    assert!(
        !logs.contains("deferred final index-run metadata update"),
        "must not emit deferred-update warn on happy path; got:\n{logs}"
    );
}

#[test]
fn final_checkpoint_refresh_normalizes_db_path_identity() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("agent_search.db");
    let db_path_variant = data_dir.join(".").join("agent_search.db");
    let mut storage = FrankenStorage::open(&db_path).unwrap();
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
        Some("checkpoint-path-normalize"),
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

    storage
        .set_last_indexed_at(FrankenStorage::now_millis())
        .unwrap();
    let mut state = LexicalRebuildState::new(
        lexical_rebuild_db_state(&storage, &db_path).unwrap(),
        LEXICAL_REBUILD_PAGE_SIZE,
    );
    state.mark_completed(index_meta_fingerprint(&index_path).unwrap());
    persist_lexical_rebuild_state(&index_path, &state).unwrap();

    refresh_completed_lexical_rebuild_checkpoint_for_final_state(
        &mut storage,
        &db_path_variant,
        &data_dir,
        false,
        None,
    )
    .unwrap();

    let checkpoint = load_lexical_rebuild_checkpoint(&index_path)
        .unwrap()
        .expect("refreshed checkpoint");
    assert_eq!(
        checkpoint.db_path,
        crate::normalize_path_identity(&db_path)
            .to_string_lossy()
            .into_owned()
    );
}

#[test]
fn should_commit_lexical_rebuild_when_message_count_threshold_is_hit() {
    assert!(should_commit_lexical_rebuild(
        10,
        5_000,
        1_024,
        1_000,
        5_000,
        16 * 1024 * 1024
    ));
    assert!(!should_commit_lexical_rebuild(
        10,
        4_999,
        1_024,
        1_000,
        5_000,
        16 * 1024 * 1024
    ));
}

#[test]
fn should_commit_lexical_rebuild_when_message_byte_threshold_is_hit() {
    assert!(should_commit_lexical_rebuild(
        10,
        100,
        16 * 1024 * 1024,
        1_000,
        5_000,
        16 * 1024 * 1024
    ));
    assert!(!should_commit_lexical_rebuild(
        10,
        100,
        (16 * 1024 * 1024) - 1,
        1_000,
        5_000,
        16 * 1024 * 1024
    ));
}

#[test]
fn lexical_rebuild_commit_intervals_keep_initial_slice_bounded_before_first_commit() {
    let state = LexicalRebuildState::new(
        LexicalRebuildDbState {
            db_path: "/tmp/agent_search.db".to_string(),
            total_conversations: 400,
            total_messages: 800,
            storage_fingerprint: "seed:400".to_string(),
        },
        LEXICAL_REBUILD_PAGE_SIZE,
    );

    let (conversations, messages, message_bytes) =
        lexical_rebuild_commit_intervals_for_state(&state);
    assert_eq!(
        conversations,
        lexical_rebuild_initial_commit_interval_conversations()
            .min(lexical_rebuild_commit_interval_conversations())
    );
    assert_eq!(
        messages,
        lexical_rebuild_initial_commit_interval_messages()
            .min(lexical_rebuild_commit_interval_messages())
    );
    assert_eq!(
        message_bytes,
        lexical_rebuild_initial_commit_interval_message_bytes()
            .min(lexical_rebuild_commit_interval_message_bytes())
    );
}

#[test]
fn lexical_rebuild_commit_intervals_return_to_steady_state_after_first_commit() {
    let mut state = LexicalRebuildState::new(
        LexicalRebuildDbState {
            db_path: "/tmp/agent_search.db".to_string(),
            total_conversations: 400,
            total_messages: 800,
            storage_fingerprint: "seed:400".to_string(),
        },
        LEXICAL_REBUILD_PAGE_SIZE,
    );
    state.committed_offset = 2_048;
    state.processed_conversations = 2_048;

    let (conversations, messages, message_bytes) =
        lexical_rebuild_commit_intervals_for_state(&state);
    assert_eq!(
        conversations,
        lexical_rebuild_commit_interval_conversations()
    );
    assert_eq!(messages, lexical_rebuild_commit_interval_messages());
    assert_eq!(
        message_bytes,
        lexical_rebuild_commit_interval_message_bytes()
    );
}

#[test]
fn should_persist_lexical_rebuild_progress_when_conversation_threshold_is_hit() {
    assert!(should_persist_lexical_rebuild_progress(
        250,
        250,
        Duration::from_millis(10),
        Duration::from_secs(2)
    ));
    assert!(!should_persist_lexical_rebuild_progress(
        249,
        250,
        Duration::from_millis(10),
        Duration::from_secs(2)
    ));
}

#[test]
fn should_persist_lexical_rebuild_progress_when_time_threshold_is_hit() {
    assert!(should_persist_lexical_rebuild_progress(
        1,
        250,
        Duration::from_secs(2),
        Duration::from_secs(2)
    ));
    assert!(!should_persist_lexical_rebuild_progress(
        1,
        250,
        Duration::from_millis(1999),
        Duration::from_secs(2)
    ));
}

#[test]
#[serial]
fn initial_batch_fetch_limit_defaults_to_bounded_warmup_chunk() {
    let _initial_conversation_limit = set_env(
        "CASS_TANTIVY_REBUILD_INITIAL_BATCH_FETCH_CONVERSATIONS",
        "0",
    );
    // The first durable rebuild chunk should stay small enough to land an
    // early restartable checkpoint without exploding RSS. Once that commit
    // lands, the rebuild immediately ramps back to the steady-state chunk
    // size.
    assert_eq!(
        lexical_rebuild_initial_batch_fetch_conversation_limit(16),
        16,
        "initial chunk should respect smaller steady-state limits"
    );
    assert_eq!(
        lexical_rebuild_initial_batch_fetch_conversation_limit(128),
        32,
        "initial chunk should default to one SQL batch when steady-state is larger"
    );
    assert_eq!(
        lexical_rebuild_initial_batch_fetch_conversation_limit(1),
        1,
        "clamp to default_limit when it is the smaller value"
    );
    assert_eq!(
        lexical_rebuild_initial_batch_fetch_conversation_limit(0),
        1,
        "never return 0 even if default_limit degenerates to 0"
    );
}

#[test]
fn finalize_watch_reindex_result_clears_stale_error_on_success() {
    let detector = StaleDetector::new(StaleConfig::default());
    let progress = Arc::new(IndexingProgress::default());
    *progress
        .last_error
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = Some("old".to_string());

    let indexed = finalize_watch_reindex_result(
        Ok(3),
        &detector,
        Some(&progress),
        "watch incremental reindex",
    );

    assert_eq!(indexed.expect("successful watch reindex"), 3);
    assert_eq!(detector.stats().total_ingests, 1);
    assert_eq!(
        progress
            .last_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_deref(),
        None,
        "successful watch reindex should clear stale error diagnostics"
    );
}

#[test]
fn finalize_watch_once_reindex_result_propagates_error_and_resets_phase() {
    let detector = StaleDetector::new(StaleConfig::default());
    let progress = Arc::new(IndexingProgress::default());
    progress.phase.store(2, Ordering::Relaxed);

    let error = finalize_watch_once_reindex_result(
        Err(anyhow::anyhow!("boom")),
        &detector,
        Some(&progress),
        "watch incremental reindex",
    )
    .expect_err("watch-once failures must propagate to the CLI");

    assert_eq!(error.to_string(), "boom");
    assert_eq!(
        detector.stats().consecutive_zero_scans,
        1,
        "failed watch-once reindex should still count as a zero-result scan for stale detection"
    );
    assert_eq!(
        progress.phase.load(Ordering::Relaxed),
        0,
        "failed watch-once reindex should reset progress phase back to idle"
    );
    assert_eq!(
        progress
            .last_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_deref(),
        Some("watch incremental reindex: boom"),
        "failed watch-once reindex should surface the real error"
    );
}

#[test]
fn finalize_watch_once_reindex_result_clears_stale_error_on_success() {
    let detector = StaleDetector::new(StaleConfig::default());
    let progress = Arc::new(IndexingProgress::default());
    *progress
        .last_error
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = Some("old".to_string());

    let indexed = finalize_watch_once_reindex_result(
        Ok(5),
        &detector,
        Some(&progress),
        "watch incremental reindex",
    )
    .expect("watch-once success should be preserved");

    assert_eq!(indexed, 5);
    assert_eq!(detector.stats().total_ingests, 1);
    assert_eq!(
        progress
            .last_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_deref(),
        None,
        "successful watch-once reindex should clear stale error diagnostics"
    );
}

#[test]
fn stale_detector_check_respects_disabled() {
    let detector = StaleDetector::new(StaleConfig {
        action: StaleAction::None,
        ..Default::default()
    });

    // Even with many zero scans, should return None when disabled
    for _ in 0..20 {
        detector.record_scan(0);
    }
    assert!(detector.check_stale().is_none());
}

#[test]
fn stale_detector_requires_min_zero_scans() {
    let detector = StaleDetector::new(StaleConfig {
        min_zero_scans: 5,
        check_interval_mins: 0, // Disable interval check for test
        threshold_hours: 0,     // Immediate threshold for test
        ..Default::default()
    });

    // Not enough zero scans yet
    for _ in 0..4 {
        detector.record_scan(0);
    }
    assert!(detector.check_stale().is_none());

    // One more to meet threshold
    detector.record_scan(0);
    // Now should trigger (if interval allows)
    // Note: check_stale has its own interval check, so this test is limited
}

#[test]
fn stale_stats_serializes_correctly() {
    let stats = StaleStats {
        consecutive_zero_scans: 5,
        total_ingests: 10,
        seconds_since_last_ingest: Some(3600),
        warning_emitted: true,
        config_action: "Warn".to_string(),
        config_threshold_hours: 24,
    };

    let json = serde_json::to_string(&stats).unwrap();
    assert!(json.contains("consecutive_zero_scans"));
    assert!(json.contains("total_ingests"));
}

#[test]
fn quarantine_failed_seed_bundle_moves_sidecars_and_uses_unique_paths() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("agent_search.db");

    std::fs::write(&db_path, b"db-one").unwrap();
    std::fs::write(tmp.path().join("agent_search.db-wal"), b"wal-one").unwrap();
    std::fs::write(tmp.path().join("agent_search.db-shm"), b"shm-one").unwrap();

    let first_backup = quarantine_failed_seed_bundle(&db_path)
        .unwrap()
        .expect("first quarantine path");
    let first_name = first_backup
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    assert_eq!(std::fs::read(&first_backup).unwrap(), b"db-one");
    assert_eq!(
        std::fs::read(first_backup.with_file_name(format!("{first_name}-wal"))).unwrap(),
        b"wal-one"
    );
    assert_eq!(
        std::fs::read(first_backup.with_file_name(format!("{first_name}-shm"))).unwrap(),
        b"shm-one"
    );
    assert!(!db_path.exists());
    assert!(!tmp.path().join("agent_search.db-wal").exists());
    assert!(!tmp.path().join("agent_search.db-shm").exists());

    std::fs::write(&db_path, b"db-two").unwrap();
    std::fs::write(tmp.path().join("agent_search.db-wal"), b"wal-two").unwrap();
    std::fs::write(tmp.path().join("agent_search.db-shm"), b"shm-two").unwrap();

    let second_backup = quarantine_failed_seed_bundle(&db_path)
        .unwrap()
        .expect("second quarantine path");
    assert_ne!(
        first_backup, second_backup,
        "repeated quarantines should not collide on backup path"
    );
    assert_eq!(std::fs::read(&second_backup).unwrap(), b"db-two");
}

#[test]
fn record_pending_commit_derives_next_offset_from_processed_conversations() {
    let db_state = LexicalRebuildDbState {
        db_path: "/tmp/agent_search.db".to_string(),
        total_conversations: 100,
        total_messages: 200,
        storage_fingerprint: "seed:100".to_string(),
    };
    let mut state = LexicalRebuildState::new(db_state, LEXICAL_REBUILD_PAGE_SIZE);
    state.record_pending_commit(Some(42), 17, 34, None);

    let pending = state.pending.as_ref().unwrap();
    assert_eq!(pending.next_offset, 17);
    assert_eq!(pending.next_conversation_id, Some(42));
    assert_eq!(pending.processed_conversations, 17);
}

#[test]
fn finalize_commit_derives_committed_offset_from_processed_conversations() {
    let db_state = LexicalRebuildDbState {
        db_path: "/tmp/agent_search.db".to_string(),
        total_conversations: 100,
        total_messages: 200,
        storage_fingerprint: "seed:100".to_string(),
    };
    let mut state = LexicalRebuildState::new(db_state, LEXICAL_REBUILD_PAGE_SIZE);
    state.record_pending_commit(Some(42), 17, 34, None);
    state.finalize_commit(Some("fp-abc".to_string()));

    assert_eq!(state.committed_offset, 17);
    assert_eq!(state.committed_conversation_id, Some(42));
    assert_eq!(state.processed_conversations, 17);
    assert_eq!(state.indexed_docs, 34);
}

#[test]
fn keyset_checkpoint_round_trips_through_serialization() {
    let db_state = LexicalRebuildDbState {
        db_path: "/tmp/agent_search.db".to_string(),
        total_conversations: 500,
        total_messages: 1000,
        storage_fingerprint: "content-v1:500:999:5000".to_string(),
    };
    let mut state = LexicalRebuildState::new(db_state, LEXICAL_REBUILD_PAGE_SIZE);
    state.record_pending_commit(Some(999), 250, 500, None);
    state.finalize_commit(None);
    state.record_pending_commit(Some(1500), 300, 600, None);

    let json = serde_json::to_string(&state).unwrap();
    assert!(
        json.contains(r#""execution_mode":"shared_writer""#),
        "new checkpoints must record shared_writer explicitly so absence remains a legacy provenance signal"
    );
    let restored: LexicalRebuildState = serde_json::from_str(&json).unwrap();

    assert_eq!(restored.committed_offset, 250);
    assert_eq!(restored.committed_conversation_id, Some(999));
    assert_eq!(restored.processed_conversations, 250);

    let pending = restored.pending.as_ref().unwrap();
    assert_eq!(pending.next_offset, 300);
    assert_eq!(pending.next_conversation_id, Some(1500));
    assert_eq!(pending.processed_conversations, 300);
}

#[test]
fn legacy_checkpoint_without_conversation_id_deserializes() {
    let legacy_json = r#"{
        "version": 3,
        "schema_hash": "test-hash",
        "db": {
            "db_path": "/tmp/agent_search.db",
            "total_conversations": 100,
            "total_messages": 200,
            "storage_fingerprint": "seed:100"
        },
        "page_size": 64,
        "committed_offset": 50,
        "processed_conversations": 50,
        "indexed_docs": 100,
        "committed_meta_fingerprint": null,
        "pending": {
            "next_offset": 60,
            "processed_conversations": 60,
            "indexed_docs": 120,
            "base_meta_fingerprint": null
        },
        "completed": false,
        "updated_at_ms": 1700000000000
    }"#;

    let state: LexicalRebuildState = serde_json::from_str(legacy_json).unwrap();
    assert_eq!(state.committed_offset, 50);
    assert_eq!(state.committed_conversation_id, None);
    assert_eq!(state.processed_conversations, 50);
    assert_eq!(state.execution_mode, None);
    assert!(state.requires_restart_from_zero_on_resume());

    let pending = state.pending.as_ref().unwrap();
    assert_eq!(pending.next_offset, 60);
    assert_eq!(pending.next_conversation_id, None);
    assert_eq!(pending.processed_conversations, 60);
}

#[test]
fn checkpoint_progress_is_monotone_across_commits() {
    let db_state = LexicalRebuildDbState {
        db_path: "/tmp/agent_search.db".to_string(),
        total_conversations: 1000,
        total_messages: 5000,
        storage_fingerprint: "seed:1000".to_string(),
    };
    let mut state = LexicalRebuildState::new(db_state, LEXICAL_REBUILD_PAGE_SIZE);

    let conversation_ids = [10, 25, 100, 500, 999];
    for (i, &cid) in conversation_ids.iter().enumerate() {
        let processed = i + 1;
        let docs = processed * 5;
        state.record_pending_commit(Some(cid), processed, docs, None);
        state.finalize_commit(None);
        assert_eq!(state.committed_conversation_id, Some(cid));
        assert_eq!(state.processed_conversations, processed);
        assert_eq!(state.indexed_docs, docs);
        assert_eq!(state.committed_offset, processed as i64);
    }

    assert_eq!(state.committed_conversation_id, Some(999));
    assert_eq!(state.processed_conversations, 5);
    assert_eq!(state.committed_offset, 5);
}

/// The on-disk archive fingerprint sidecar is served while the archive's
/// physical identity is unchanged, and recomputed once the archive
/// changes. Positive observable: a planted sentinel fingerprint in the
/// sidecar is returned verbatim (proving the DB was not consulted).
/// Planted negative: after a write, the sentinel is discarded and the
/// real fingerprint comes back — through the disk layer and through the
/// in-process memo wrapper alike. No-claim: this does not measure the
/// WAL-replay cost the sidecar exists to avoid.
#[test]
fn cached_archive_fingerprint_hits_on_unchanged_identity_and_misses_after_writes() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("db.sqlite");
    let cache_dir = tmp.path().join("index");
    std::fs::create_dir_all(&cache_dir).unwrap();
    let storage = FrankenStorage::open(&db_path).unwrap();
    storage.run_migrations().unwrap();
    seed_archive_conversation(&storage, "first");
    storage.close_without_checkpoint().unwrap();

    let authoritative = lexical_storage_fingerprint_for_db_strict(&db_path).unwrap();
    assert!(authoritative.starts_with("content-v1:"));
    let identity = lexical_storage_physical_identity(&db_path).expect("archive identity");
    let first =
        lexical_storage_fingerprint_for_db_disk_cached(&db_path, &identity, &cache_dir).unwrap();
    assert_eq!(first, authoritative);
    let cache_path = cache_dir.join(ARCHIVE_FINGERPRINT_CACHE_FILE);
    assert!(cache_path.is_file(), "miss must populate the sidecar");
    assert_eq!(
        lexical_storage_fingerprint_for_db_cached(&db_path, &cache_dir).unwrap(),
        authoritative,
        "the memo wrapper must agree with the disk layer"
    );

    // Plant a sentinel under the SAME identity: the disk layer must
    // return it without touching the database.
    let mut cached: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&cache_path).unwrap()).unwrap();
    cached["fingerprint"] = serde_json::Value::String("content-v1:sentinel".into());
    std::fs::write(&cache_path, cached.to_string()).unwrap();
    let hit =
        lexical_storage_fingerprint_for_db_disk_cached(&db_path, &identity, &cache_dir).unwrap();
    assert_eq!(
        hit, "content-v1:sentinel",
        "unchanged identity must be served from the sidecar"
    );

    // A sidecar from a different schema version is ignored, not trusted.
    cached["schema_version"] = serde_json::Value::from(999);
    std::fs::write(&cache_path, cached.to_string()).unwrap();
    assert_eq!(
        lexical_storage_fingerprint_for_db_disk_cached(&db_path, &identity, &cache_dir).unwrap(),
        authoritative,
        "an unknown sidecar schema must fall back to the strict computation"
    );

    // Change the archive: the identity moves and the sentinel is discarded
    // by both layers.
    let storage = FrankenStorage::open(&db_path).unwrap();
    seed_archive_conversation(&storage, "second");
    storage.close_without_checkpoint().unwrap();
    let changed = lexical_storage_physical_identity(&db_path).expect("archive identity");
    assert_ne!(
        changed, identity,
        "a committed write must move the identity"
    );
    let recomputed =
        lexical_storage_fingerprint_for_db_disk_cached(&db_path, &changed, &cache_dir).unwrap();
    assert_ne!(recomputed, "content-v1:sentinel");
    assert_eq!(
        recomputed,
        lexical_storage_fingerprint_for_db_strict(&db_path).unwrap()
    );
    assert_ne!(
        recomputed, authoritative,
        "a second conversation changes the fingerprint"
    );
    assert_eq!(
        lexical_storage_fingerprint_for_db_cached(&db_path, &cache_dir).unwrap(),
        recomputed,
        "the memo wrapper must not serve the pre-write fingerprint"
    );
}
