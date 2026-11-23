use super::super::*;
use super::helpers::*;
use crate::connectors::{NormalizedConversation, NormalizedMessage};
use crate::franken_sync::compat::{ConnectionExt, ParamValue, RowExt};
use crate::model::types::{Agent, AgentKind};
use serial_test::serial;
use tempfile::TempDir;

#[test]
#[serial]
fn watch_lexical_oom_retry_preserves_child_scope_and_unrelated_documents() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("cass");
    fs::create_dir_all(&data_dir).unwrap();
    let selected = tmp.path().join("selected/codex");
    let first = selected.join("rollout-first.jsonl");
    let second = selected.join("rollout-second.jsonl");
    let unrelated = tmp.path().join("unrelated/codex/rollout-other.jsonl");
    write_watch_lexical_source(&first, "thread-first", "amberneedle");
    write_watch_lexical_source(&second, "thread-second", "bronzeneedle");
    write_watch_lexical_source(&unrelated, "thread-other", "copperneedle");
    let opts = watch_lexical_options(&data_dir);
    let storage = Mutex::new(FrankenStorage::open(&opts.db_path).unwrap());
    let index_path = index_dir(&data_dir).unwrap();
    let index = Mutex::new(Some(TantivyIndex::open_or_create(&index_path).unwrap()));
    assert_eq!(
        run_watch_lexical_selection(&opts, &unrelated, &storage, &index).unwrap(),
        1
    );
    let unrelated_ids = watch_lexical_search_ids(&index_path, "copperneedle");
    assert_eq!(unrelated_ids.len(), 1);
    refresh_completed_lexical_rebuild_checkpoint(
        &storage.lock().unwrap(),
        &opts.db_path,
        &data_dir,
    )
    .unwrap();
    let checkpoint = lexical_rebuild_state_path(&index_path);
    let checkpoint_before = fs::read(&checkpoint).unwrap();

    // The first conversation reaches the real sink; a later OOM drops
    // its uncommitted work too. Both canonical conversations need replay.
    {
        let _fault = WatchLexicalFaultGuard::set(WatchLexicalTestFault::PublicationAfter(1));
        assert_eq!(
            run_watch_lexical_selection(&opts, &selected, &storage, &index).unwrap(),
            2
        );
    }
    assert!(index.lock().unwrap().is_none());
    assert!(watch_lexical_search_ids(&index_path, "amberneedle").is_empty());
    assert!(watch_lexical_search_ids(&index_path, "bronzeneedle").is_empty());
    assert_eq!(
        watch_lexical_search_ids(&index_path, "copperneedle"),
        unrelated_ids
    );
    let canonical_before = watch_lexical_canonical_rows(&storage.lock().unwrap());
    assert_eq!(canonical_before.len(), 3);
    assert_eq!(fs::read(&checkpoint).unwrap(), checkpoint_before);

    storage.into_inner().unwrap().close().unwrap();
    let storage = Mutex::new(FrankenStorage::open(&opts.db_path).unwrap());
    let index = Mutex::new(None);
    assert_eq!(
        run_watch_lexical_selection(&opts, &first, &storage, &index).unwrap(),
        1
    );
    let first_ids = watch_lexical_search_ids(&index_path, "amberneedle");
    assert_eq!(first_ids.len(), 1);
    assert!(watch_lexical_search_ids(&index_path, "bronzeneedle").is_empty());
    assert_eq!(
        watch_lexical_search_ids(&index_path, "copperneedle"),
        unrelated_ids
    );
    let parent_root = ScanRoot::local(selected.clone());
    let parent_debt = watch_lexical_replay_debt_for_root(
        &storage.lock().unwrap(),
        ConnectorKind::Codex,
        &parent_root,
    )
    .unwrap();
    assert_eq!(parent_debt.len(), 1, "child retry must retain sibling debt");
    assert_eq!(
        parent_debt[0].path,
        crate::normalize_path_identity(&selected)
    );

    for fault in [
        WatchLexicalTestFault::PublicationAfter(1),
        WatchLexicalTestFault::Commit,
    ] {
        let _fault = WatchLexicalFaultGuard::set(fault);
        let result = run_watch_lexical_selection(&opts, &selected, &storage, &index);
        if fault == WatchLexicalTestFault::Commit {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("injected watch lexical")
            );
        } else {
            assert_eq!(result.unwrap(), 2);
        }
        // A failed commit may leave staged work in the handle. Reopening
        // must still recover from durable debt and published documents.
        *index.lock().unwrap() = None;
        assert_eq!(
            watch_lexical_search_ids(&index_path, "amberneedle"),
            first_ids
        );
        assert!(watch_lexical_search_ids(&index_path, "bronzeneedle").is_empty());
        assert_eq!(
            watch_lexical_search_ids(&index_path, "copperneedle"),
            unrelated_ids
        );
        assert_eq!(
            watch_lexical_canonical_rows(&storage.lock().unwrap()),
            canonical_before
        );
        assert_eq!(
            watch_lexical_replay_debt_for_root(
                &storage.lock().unwrap(),
                ConnectorKind::Codex,
                &parent_root,
            )
            .unwrap()
            .len(),
            1
        );
    }

    {
        let _fault = WatchLexicalFaultGuard::set(WatchLexicalTestFault::DebtClear);
        assert!(run_watch_lexical_selection(&opts, &selected, &storage, &index).is_err());
    }
    let second_ids = watch_lexical_search_ids(&index_path, "bronzeneedle");
    assert_eq!(second_ids.len(), 1, "commit precedes debt clearing");
    assert_eq!(
        watch_lexical_replay_debt_for_root(
            &storage.lock().unwrap(),
            ConnectorKind::Codex,
            &parent_root,
        )
        .unwrap()
        .len(),
        1
    );
    {
        // Forced typed OOM splits the real persistence batch. Every leaf
        // has unchanged canonical rows, so recovery needs their merged IDs.
        let _fault = WatchLexicalFaultGuard::set(WatchLexicalTestFault::SplitAt(2));
        assert_eq!(
            run_watch_lexical_selection(&opts, &selected, &storage, &index).unwrap(),
            2
        );
    }
    assert!(
        watch_lexical_replay_debt_for_root(
            &storage.lock().unwrap(),
            ConnectorKind::Codex,
            &parent_root,
        )
        .unwrap()
        .is_empty()
    );
    assert_eq!(
        watch_lexical_search_ids(&index_path, "amberneedle"),
        first_ids
    );
    assert_eq!(
        watch_lexical_search_ids(&index_path, "bronzeneedle"),
        second_ids
    );
    assert_eq!(
        watch_lexical_search_ids(&index_path, "copperneedle"),
        unrelated_ids
    );
    assert_eq!(
        watch_lexical_canonical_rows(&storage.lock().unwrap()),
        canonical_before
    );
    assert_eq!(fs::read(&checkpoint).unwrap(), checkpoint_before);
    assert!(
        !data_dir
            .join("quarantine/watch_ingest_poison.jsonl")
            .exists()
    );
}

#[test]
#[serial]
fn watch_lexical_oom_retry_preserves_other_connector_debt_on_shared_root() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("cass");
    let selected = tmp.path().join("selected");
    let gemini_file = selected.join("chats/session-shared.json");
    let codex_file = selected.join("sessions/rollout-shared.jsonl");
    fs::create_dir_all(&data_dir).unwrap();
    fs::create_dir_all(gemini_file.parent().unwrap()).unwrap();
    fs::create_dir_all(codex_file.parent().unwrap()).unwrap();
    // The nested Gemini shape is a real provider format; Codex only scans
    // `rollout-*.{jsonl,json}` files and ignores `chats/session-*.json`.
    fs::write(
        &gemini_file,
        serde_json::to_vec(&serde_json::json!({
            "sessionId": "session-shared",
            "messages": [{
                "type": "user",
                "content": "geminiconnectorneedle",
                "timestamp": "2025-11-12T18:31:18.000Z"
            }]
        }))
        .unwrap(),
    )
    .unwrap();
    write_watch_lexical_source(&codex_file, "shared-codex", "codexconnectorneedle");
    let mut opts = watch_lexical_options(&data_dir);
    opts.watch_once_paths = Some(vec![selected.clone()]);
    let root = ScanRoot::local(selected.clone());
    let roots = [
        (ConnectorKind::Gemini, root.clone()),
        (ConnectorKind::Codex, root.clone()),
    ];
    let kinds: HashSet<_> = classify_paths(vec![selected.clone()], &roots, true)
        .into_iter()
        .map(|(kind, _, _, _)| kind)
        .collect();
    assert_eq!(
        kinds,
        HashSet::from([ConnectorKind::Gemini, ConnectorKind::Codex])
    );
    let storage = Mutex::new(FrankenStorage::open(&opts.db_path).unwrap());
    let index_path = index_dir(&data_dir).unwrap();
    let index = Mutex::new(Some(TantivyIndex::open_or_create(&index_path).unwrap()));
    {
        let _fault = WatchLexicalFaultGuard::set(WatchLexicalTestFault::PublicationAfter(0));
        assert_eq!(
            reindex_paths(
                &opts,
                vec![selected.clone()],
                &[(ConnectorKind::Gemini, root.clone())],
                &Mutex::new(HashMap::new()),
                &storage,
                &index,
                &index_path,
                false,
            )
            .unwrap(),
            1
        );
    }
    assert!(index.lock().unwrap().is_none());
    let gemini_rows = watch_lexical_canonical_rows(&storage.lock().unwrap());
    assert_eq!(gemini_rows.len(), 1);
    assert_eq!(gemini_rows[0].3, "geminiconnectorneedle");
    assert!(watch_lexical_search_ids(&index_path, "geminiconnectorneedle").is_empty());
    // These scans create the final v1 schema, including connector identity;
    // this test does not fabricate or claim migration of older debt rows.
    let gemini_debt =
        watch_lexical_replay_debt_for_root(&storage.lock().unwrap(), ConnectorKind::Gemini, &root)
            .unwrap();
    assert_eq!(gemini_debt.len(), 1);
    assert_eq!(gemini_debt[0].connector, ConnectorKind::Gemini);
    let gemini_metadata = gemini_debt[0].metadata().unwrap();
    assert!(
        watch_lexical_replay_debt_for_root(&storage.lock().unwrap(), ConnectorKind::Codex, &root,)
            .unwrap()
            .is_empty()
    );

    // Configured roots trigger every connector. Fix the order here so a
    // healthy Codex publication follows Gemini's failed publication exactly.
    assert_eq!(
        reindex_paths(
            &opts,
            vec![selected.clone()],
            &[(ConnectorKind::Codex, root.clone())],
            &Mutex::new(HashMap::new()),
            &storage,
            &index,
            &index_path,
            false,
        )
        .unwrap(),
        1
    );
    let canonical_before = watch_lexical_canonical_rows(&storage.lock().unwrap());
    assert_eq!(canonical_before.len(), 2);
    assert_eq!(canonical_before[0], gemini_rows[0]);
    assert_eq!(canonical_before[1].3, "codexconnectorneedle");
    let expected_id = |source: &Path, row: &(i64, i64, i64, String)| {
        frankensearch::quill::cass::cass_document_identity(
            "local",
            frankensearch::quill::cass::CassConversationKey {
                source_path: &source.to_string_lossy(),
                id: Some(row.0),
            },
            row.2 as u64,
        )
    };
    let gemini_ids = BTreeSet::from([expected_id(&gemini_file, &canonical_before[0])]);
    let codex_ids = BTreeSet::from([expected_id(&codex_file, &canonical_before[1])]);
    assert!(watch_lexical_search_ids(&index_path, "geminiconnectorneedle").is_empty());
    assert_eq!(
        watch_lexical_search_ids(&index_path, "codexconnectorneedle"),
        codex_ids
    );
    let remaining =
        watch_lexical_replay_debt_for_root(&storage.lock().unwrap(), ConnectorKind::Gemini, &root)
            .unwrap();
    assert_eq!(remaining.len(), 1, "Codex must not discharge Gemini's debt");
    assert_eq!(remaining[0].metadata().unwrap(), gemini_metadata);
    assert!(
        watch_lexical_replay_debt_for_root(&storage.lock().unwrap(), ConnectorKind::Codex, &root,)
            .unwrap()
            .is_empty()
    );

    drop(index);
    storage.into_inner().unwrap().close().unwrap();
    let storage = Mutex::new(FrankenStorage::open(&opts.db_path).unwrap());
    storage
        .lock()
        .unwrap()
        .set_last_indexed_at(FrankenStorage::now_millis().saturating_add(10_000))
        .unwrap();
    let index = Mutex::new(None);
    let mut file_selection = opts.clone();
    file_selection.watch_once_paths = Some(vec![gemini_file.clone()]);
    assert!(
        !should_skip_unchanged_explicit_watch_once_paths(
            &file_selection,
            &storage.lock().unwrap(),
            &roots,
        )
        .unwrap(),
        "the outer unchanged-file fast path must preserve Gemini's debt"
    );
    for _ in 0..2 {
        // Both packets are unchanged. Only Gemini's durable debt permits
        // recovery on the first retry; the second must remain idempotent.
        assert_eq!(
            reindex_paths(
                &opts,
                vec![selected.clone()],
                &[(ConnectorKind::Gemini, root.clone())],
                &Mutex::new(HashMap::new()),
                &storage,
                &index,
                &index_path,
                false,
            )
            .unwrap(),
            1
        );
        assert_eq!(
            watch_lexical_canonical_rows(&storage.lock().unwrap()),
            canonical_before
        );
        assert_eq!(
            watch_lexical_search_ids(&index_path, "geminiconnectorneedle"),
            gemini_ids
        );
        assert_eq!(
            watch_lexical_search_ids(&index_path, "codexconnectorneedle"),
            codex_ids
        );
        for kind in [ConnectorKind::Gemini, ConnectorKind::Codex] {
            assert!(
                watch_lexical_replay_debt_for_root(&storage.lock().unwrap(), kind, &root)
                    .unwrap()
                    .is_empty()
            );
        }
    }
    assert!(
        !data_dir
            .join("quarantine/watch_ingest_poison.jsonl")
            .exists()
    );
}

#[test]
#[serial]
fn watch_lexical_oom_debt_write_failure_precedes_canonical_mutation() {
    let tmp = TempDir::new().unwrap();
    let opts = watch_lexical_options(tmp.path());
    let source = tmp.path().join("codex/rollout-debt-write.jsonl");
    write_watch_lexical_source(&source, "thread-debt-write", "debtwriteneedle");
    let storage = Mutex::new(FrankenStorage::open(&opts.db_path).unwrap());
    let index_path = index_dir(tmp.path()).unwrap();
    let index = Mutex::new(Some(TantivyIndex::open_or_create(&index_path).unwrap()));
    let prior_root = ScanRoot::local(tmp.path().join("prior/codex"));
    let prior_debt = WatchLexicalReplayDebt::for_root(ConnectorKind::Codex, &prior_root);
    persist_watch_lexical_replay_debt(&storage.lock().unwrap(), &prior_debt).unwrap();
    let mut broad_pending = LexicalRebuildState::new(
        deferred_lexical_rebuild_db_state(&opts.db_path, 0),
        LEXICAL_REBUILD_PAGE_SIZE,
    );
    broad_pending.set_execution_mode(LexicalRebuildExecutionMode::CanonicalMetadataRepair);
    persist_lexical_rebuild_state(&index_path, &broad_pending).unwrap();
    let checkpoint_before = fs::read(lexical_rebuild_state_path(&index_path)).unwrap();
    {
        let _fault = WatchLexicalFaultGuard::set(WatchLexicalTestFault::DebtWrite);
        let error = run_watch_lexical_selection(&opts, &source, &storage, &index).unwrap_err();
        assert!(error.to_string().contains("injected watch lexical"));
    }
    assert!(watch_lexical_canonical_rows(&storage.lock().unwrap()).is_empty());
    let conversations: i64 = storage
        .lock()
        .unwrap()
        .raw()
        .query_row_map("SELECT COUNT(*) FROM conversations", &[], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(conversations, 0);
    assert!(watch_lexical_search_ids(&index_path, "debtwriteneedle").is_empty());
    let prior_after = watch_lexical_replay_debt_for_root(
        &storage.lock().unwrap(),
        ConnectorKind::Codex,
        &prior_root,
    )
    .unwrap();
    assert_eq!(prior_after.len(), 1);
    assert_eq!(
        prior_after[0].metadata().unwrap(),
        prior_debt.metadata().unwrap()
    );
    assert_eq!(
        fs::read(lexical_rebuild_state_path(&index_path)).unwrap(),
        checkpoint_before
    );
}

#[test]
#[serial]
fn explicit_watch_once_skips_active_session_source_without_watermarking() {
    let _active_skip_reset = ActiveSessionSkipReset;
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("active-source-skip");
    let codex_dir = data_dir.join("codex/sessions");
    std::fs::create_dir_all(&codex_dir).unwrap();
    let codex_file = codex_dir.join("rollout-active-source.jsonl");
    write_watch_codex_messages(
        &codex_file,
        "thread-active-source",
        &[("user", "not ready yet", 1_700_000_000_100)],
    );

    let active_paths = std::env::join_paths([codex_file.as_os_str()]).unwrap();
    let active_paths = active_paths.to_string_lossy().to_string();
    let _active_guard = set_env("CASS_TEST_ACTIVE_SESSION_SOURCE_PATHS", &active_paths);

    let opts = crate::indexer::IndexOptions {
        full: false,
        watch: false,
        force_rebuild: false,
        db_path: data_dir.join("agent_search.db"),
        data_dir: data_dir.clone(),
        semantic: false,
        build_hnsw: false,
        embedder: "fastembed".to_string(),
        progress: None,
        watch_once_paths: Some(vec![codex_file.clone()]),
        watch_interval_secs: 30,
    };

    let storage = FrankenStorage::open(&opts.db_path).unwrap();
    storage.run_migrations().unwrap();
    let index_path = index_dir(&opts.data_dir).unwrap();
    let t_index = TantivyIndex::open_or_create(&index_path).unwrap();
    let state = std::sync::Mutex::new(std::collections::HashMap::new());
    let storage = std::sync::Mutex::new(storage);
    let t_index = std::sync::Mutex::new(Some(t_index));
    let roots = vec![(ConnectorKind::Codex, ScanRoot::local(codex_file.clone()))];

    let indexed = reindex_paths(
        &opts,
        vec![codex_file.clone()],
        &roots,
        &state,
        &storage,
        &t_index,
        &index_path,
        false,
    )
    .unwrap();

    assert_eq!(indexed, 0);
    let (conversation_rows, message_rows, last_indexed_at): (i64, i64, Option<i64>) = {
        let storage = storage.lock().unwrap();
        let conversations = storage
            .raw()
            .query_row_map(
                "SELECT COUNT(*) FROM conversations",
                &[] as &[ParamValue],
                |row| row.get_typed(0),
            )
            .unwrap();
        let messages = storage
            .raw()
            .query_row_map(
                "SELECT COUNT(*) FROM messages",
                &[] as &[ParamValue],
                |row| row.get_typed(0),
            )
            .unwrap();
        (
            conversations,
            messages,
            storage.get_last_indexed_at().unwrap(),
        )
    };
    assert_eq!(conversation_rows, 0);
    assert_eq!(message_rows, 0);
    assert_eq!(
        last_indexed_at, None,
        "active source skips must not advance last_indexed_at"
    );
    assert!(
        !data_dir
            .join("quarantine/watch_ingest_poison.jsonl")
            .exists(),
        "active source skips are retryable, not poison quarantine"
    );
}

#[test]
#[serial]
fn watch_mode_recent_session_source_skips_without_quarantine_or_watermarking() {
    let _active_skip_reset = ActiveSessionSkipReset;
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("recent-active-source-skip");
    let codex_dir = data_dir.join("codex/sessions");
    std::fs::create_dir_all(&codex_dir).unwrap();
    let codex_file = codex_dir.join("rollout-recent-active-source.jsonl");
    write_watch_codex_messages(
        &codex_file,
        "thread-recent-active-source",
        &[("user", "still streaming", 1_700_000_000_100)],
    );

    let _window_guard = set_env("CASS_ACTIVE_SESSION_RECENT_WRITE_WINDOW_SECS", "3600");
    let opts = crate::indexer::IndexOptions {
        full: false,
        watch: true,
        force_rebuild: false,
        db_path: data_dir.join("agent_search.db"),
        data_dir: data_dir.clone(),
        semantic: false,
        build_hnsw: false,
        embedder: "fastembed".to_string(),
        progress: None,
        watch_once_paths: None,
        watch_interval_secs: 30,
    };

    let storage = FrankenStorage::open(&opts.db_path).unwrap();
    storage.run_migrations().unwrap();
    let index_path = index_dir(&opts.data_dir).unwrap();
    let t_index = TantivyIndex::open_or_create(&index_path).unwrap();
    let state = std::sync::Mutex::new(std::collections::HashMap::new());
    let storage = std::sync::Mutex::new(storage);
    let t_index = std::sync::Mutex::new(Some(t_index));
    let roots = vec![(ConnectorKind::Codex, ScanRoot::local(codex_file.clone()))];

    let indexed = reindex_paths(
        &opts,
        vec![codex_file.clone()],
        &roots,
        &state,
        &storage,
        &t_index,
        &index_path,
        false,
    )
    .unwrap();

    assert_eq!(indexed, 0);
    let last_indexed_at = storage.lock().unwrap().get_last_indexed_at().unwrap();
    assert_eq!(
        last_indexed_at, None,
        "recent active source skips must not advance last_indexed_at"
    );
    assert!(
        !data_dir
            .join("quarantine/watch_ingest_poison.jsonl")
            .exists(),
        "recent active source skips are retryable, not poison quarantine"
    );
}

#[test]
fn watch_conversation_sort_orders_missing_then_ascending_watermarks() {
    fn conv(source: &str, external_id: &str, started_at: Option<i64>) -> NormalizedConversation {
        NormalizedConversation {
            agent_slug: "amp".to_string(),
            source_path: PathBuf::from(source),
            external_id: Some(external_id.to_string()),
            title: None,
            workspace: None,
            started_at,
            ended_at: None,
            messages: Vec::new(),
            metadata: serde_json::Value::Null,
        }
    }

    let mut convs = vec![
        conv("/tmp/c", "c", Some(30)),
        conv("/tmp/a", "a", None),
        conv("/tmp/b", "b", Some(10)),
    ];

    sort_watch_conversations_for_watermark(&mut convs);

    let ordered: Vec<_> = convs
        .iter()
        .map(|conv| conv.external_id.as_deref().unwrap())
        .collect();
    assert_eq!(ordered, vec!["a", "b", "c"]);
}

fn watch_spool_fixture(
    session: usize,
    message_count: usize,
    content_bytes: usize,
) -> NormalizedConversation {
    let timestamp = 1_700_000_000_000_i64 + session as i64 * 1_000;
    NormalizedConversation {
        agent_slug: "amp".to_string(),
        external_id: Some(format!("watch-spool-{session:05}")),
        title: Some(format!("Conversation {session}")),
        workspace: Some(PathBuf::from("/workspaces/watch-spool")),
        source_path: PathBuf::from(format!("/sessions/thread-{session:05}.json")),
        started_at: Some(timestamp),
        ended_at: None,
        metadata: serde_json::json!({"session": session, "preserve": [true, null, "é"]}),
        messages: (0..message_count)
            .map(|message| NormalizedMessage {
                idx: message as i64,
                role: if message.is_multiple_of(2) {
                    "user"
                } else {
                    "assistant"
                }
                .to_string(),
                author: None,
                created_at: Some(timestamp + message as i64),
                content: format!(
                    "session {session} message {message} {}",
                    "x".repeat(content_bytes)
                ),
                extra: serde_json::json!({"message": message}),
                snippets: Vec::new(),
                invocations: Vec::new(),
            })
            .collect(),
    }
}

#[test]
fn watch_spool_matches_in_memory_global_order_and_payloads() -> Result<()> {
    let temp = TempDir::new()?;
    let progress = IndexingProgress::default();
    for count_limit in [1, 8, 512] {
        let mut original = Vec::new();
        let mut spool = WatchConversationSpool::new(temp.path(), count_limit);
        for session in (0..97).rev() {
            let mut conversation = watch_spool_fixture(session, session % 5, 100);
            // Include absent and tied timestamps as well as path/id ties.
            if session % 7 == 0 {
                conversation.started_at = None;
                conversation.messages.clear();
            } else if session % 3 == 0 {
                conversation.started_at = Some(100);
                for message in &mut conversation.messages {
                    message.created_at = Some(100);
                }
            }
            original.push(conversation.clone());
            spool.push(conversation, Some(&progress))?;
        }
        sort_watch_conversations_for_watermark(&mut original);
        let mut batches = spool.into_sorted_batches()?;
        let mut actual = Vec::new();
        loop {
            let batch = batches.next_batch(Some(&progress))?;
            if batch.is_empty() {
                break;
            }
            assert!(batch.len() <= count_limit);
            actual.extend(batch);
        }
        assert_eq!(
            serde_json::to_value(actual)?,
            serde_json::to_value(original)?
        );
    }
    assert!(progress.activity.load(Ordering::Relaxed) >= 97 * 3);
    assert_eq!(
        fs::read_dir(temp.path())?.count(),
        0,
        "anonymous spools leave no files"
    );
    Ok(())
}

#[test]
fn watch_spool_bounds_payload_memory_at_reported_archive_scale() -> Result<()> {
    // GH #538: 8,560 conversations / 683,558 messages. Generate and consume
    // one conversation at a time so the fixture does not hide a second
    // full-corpus allocation beside the code under test.
    const SESSIONS: usize = 8_560;
    const MESSAGES: usize = 683_558;
    let temp = TempDir::new()?;
    let progress = IndexingProgress::default();
    let mut spool = WatchConversationSpool::new(temp.path(), 32);
    let limit = spool.memory_limit;
    let mut peak_buffered_bytes = 0usize;
    let mut full_corpus_bytes = 0usize;
    for session in (0..SESSIONS).rev() {
        let messages = MESSAGES / SESSIONS + usize::from(session < MESSAGES % SESSIONS);
        let conversation = watch_spool_fixture(session, messages, 64);
        full_corpus_bytes = full_corpus_bytes
            .saturating_add(conversation_batch_footprint(&conversation).retained_bytes);
        spool.push(conversation, Some(&progress))?;
        peak_buffered_bytes = peak_buffered_bytes.max(spool.memory_bytes);
        assert!(spool.memory_bytes <= limit);
    }
    assert_eq!(spool.len(), SESSIONS);
    assert!(
        spool.file.is_some(),
        "large watch scans must spill payloads"
    );
    assert!(full_corpus_bytes > limit * 20);
    let spooled_bytes = spool.written_bytes;
    let mut batches = spool.into_sorted_batches()?;
    let mut sessions = 0usize;
    let mut messages = 0usize;
    let mut batch_count = 0usize;
    loop {
        let batch = batches.next_batch(Some(&progress))?;
        if batch.is_empty() {
            break;
        }
        batch_count += 1;
        let buffered_bytes = batch.iter().fold(0usize, |bytes, conversation| {
            bytes.saturating_add(conversation_batch_footprint(conversation).retained_bytes)
        });
        peak_buffered_bytes = peak_buffered_bytes.max(buffered_bytes);
        assert!(buffered_bytes <= limit);
        assert!(batch.len() <= 32);
        for conversation in batch {
            assert_eq!(
                conversation.external_id,
                Some(format!("watch-spool-{sessions:05}"))
            );
            assert_eq!(conversation.metadata["session"], sessions);
            messages += conversation.messages.len();
            sessions += 1;
        }
    }
    assert_eq!((sessions, messages), (SESSIONS, MESSAGES));
    assert!(progress.activity.load(Ordering::Relaxed) >= 2 * SESSIONS as u64);
    eprintln!(
        "GH538 watch spool: conversations={sessions} messages={messages} batches={batch_count} \
         peak_payload_bytes={peak_buffered_bytes} full_corpus_payload_bytes={full_corpus_bytes} \
         temporary_bytes={spooled_bytes}"
    );
    Ok(())
}

#[test]
fn watch_spool_keeps_one_oversized_conversation_isolated() -> Result<()> {
    let temp = TempDir::new()?;
    let mut spool = WatchConversationSpool::new(temp.path(), 32);
    spool.push(watch_spool_fixture(2, 1, 16), None)?;
    spool.push(watch_spool_fixture(1, 1, spool.memory_limit + 1), None)?;
    spool.push(watch_spool_fixture(0, 1, 16), None)?;
    let mut batches = spool.into_sorted_batches()?;
    for session in 0..3 {
        let batch = batches.next_batch(None)?;
        assert_eq!(batch.len(), 1);
        assert_eq!(
            batch[0].external_id,
            Some(format!("watch-spool-{session:05}"))
        );
    }
    assert!(batches.next_batch(None)?.is_empty());
    Ok(())
}

#[test]
fn watch_spool_cancellation_leaves_sources_replayable() -> Result<()> {
    let temp = TempDir::new()?;
    let progress = IndexingProgress::default();
    let mut spool = WatchConversationSpool::new(temp.path(), 1);
    spool.push(watch_spool_fixture(0, 2, 100), Some(&progress))?;
    progress.request_stop();
    let error = spool
        .push(watch_spool_fixture(1, 2, 100), Some(&progress))
        .expect_err("cancelled spill must stop before adding another source");
    assert!(error.downcast_ref::<IndexInterrupted>().is_some());
    drop(spool);
    assert_eq!(fs::read_dir(temp.path())?.count(), 0);

    let progress = IndexingProgress::default();
    let mut retry = WatchConversationSpool::new(temp.path(), 1);
    for session in (0..2).rev() {
        retry.push(watch_spool_fixture(session, 2, 100), Some(&progress))?;
    }
    let mut retry = retry.into_sorted_batches()?;
    assert_eq!(
        retry.next_batch(Some(&progress))?[0].external_id.as_deref(),
        Some("watch-spool-00000")
    );
    progress.request_stop();
    assert!(
        retry
            .next_batch(Some(&progress))
            .unwrap_err()
            .downcast_ref::<IndexInterrupted>()
            .is_some()
    );
    Ok(())
}

#[test]
fn watch_spool_reader_checks_stop_between_bounded_reads() -> Result<()> {
    let progress = IndexingProgress::default();
    let data = vec![b'x'; 256 * 1024];
    let mut reader = WatchSpoolReader {
        reader: std::io::Cursor::new(data),
        progress: Some(&progress),
    };
    let mut buffer = vec![0; 128 * 1024];
    assert_eq!(reader.read(&mut buffer)?, 64 * 1024);
    assert_eq!(progress.activity.load(Ordering::Relaxed), 1);
    progress.request_stop();
    assert!(reader.read(&mut buffer).is_err());
    assert_eq!(progress.activity.load(Ordering::Relaxed), 1);
    Ok(())
}

#[test]
fn watch_idle_wait_honors_cancellation_without_activity() {
    let progress = Arc::new(IndexingProgress::default());
    let stopping = Arc::clone(&progress);
    let stop = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !stopping.watch_idle.load(Ordering::Acquire) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        let observed_idle = stopping.watch_idle.load(Ordering::Acquire);
        stopping.request_stop();
        observed_idle
    });
    let started = Instant::now();
    let result = wait_watch_loop_event(
        &never(),
        &never(),
        Duration::from_secs(300),
        Some(&progress),
    );
    assert!(stop.join().expect("stop request thread"));
    let Err(error) = result else {
        panic!("idle watch wait must return the stop request");
    };
    assert!(error.downcast_ref::<IndexInterrupted>().is_some());
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(progress.activity.load(Ordering::Relaxed), 0);
    assert!(!progress.watch_idle.load(Ordering::Acquire));
}

#[test]
fn watch_idle_wait_preserves_deadline_without_activity() -> Result<()> {
    let progress = Arc::new(IndexingProgress::default());
    let timeout = Duration::from_millis(80);
    let started = Instant::now();
    assert!(matches!(
        wait_watch_loop_event(&never(), &never(), timeout, Some(&progress))?,
        WatchLoopEvent::Timeout
    ));
    assert!(started.elapsed() >= timeout);
    assert_eq!(progress.activity.load(Ordering::Relaxed), 0);
    assert!(!progress.watch_idle.load(Ordering::Acquire));
    Ok(())
}

#[test]
fn gh538_watch_idle_marker_clears_before_event_or_disconnect() -> Result<()> {
    let progress = Arc::new(IndexingProgress::default());
    let (wake, notifications) = bounded(1);
    wake.send(())?;
    assert!(matches!(
        wait_watch_loop_event(
            &never(),
            &notifications,
            Duration::from_secs(300),
            Some(&progress),
        )?,
        WatchLoopEvent::Filesystem
    ));
    assert!(!progress.watch_idle.load(Ordering::Acquire));
    drop(wake);
    assert!(matches!(
        wait_watch_loop_event(
            &never(),
            &notifications,
            Duration::from_secs(300),
            Some(&progress),
        )?,
        WatchLoopEvent::Disconnected
    ));
    assert!(!progress.watch_idle.load(Ordering::Acquire));
    assert_eq!(progress.activity.load(Ordering::Relaxed), 0);
    Ok(())
}

#[cfg(unix)]
#[test]
fn watch_spool_preserves_non_utf8_source_workspace_and_snippet_paths() -> Result<()> {
    use std::os::unix::ffi::OsStringExt;

    let temp = TempDir::new()?;
    let path = PathBuf::from(std::ffi::OsString::from_vec(
        b"/sessions/non-utf8-\xff".to_vec(),
    ));
    let mut conversation = watch_spool_fixture(0, 1, 10);
    conversation.source_path = path.clone();
    conversation.workspace = Some(path.clone());
    conversation.messages[0].snippets.push(NormalizedSnippet {
        file_path: Some(path.clone()),
        start_line: Some(1),
        end_line: Some(2),
        language: Some("rust".to_string()),
        snippet_text: Some("let preserved = true;".to_string()),
    });
    let mut spool = WatchConversationSpool::new(temp.path(), 1);
    spool.push(conversation, None)?;
    spool.push(watch_spool_fixture(1, 1, 10), None)?;
    let mut batches = spool.into_sorted_batches()?;
    let restored = batches.next_batch(None)?.remove(0);
    assert_eq!(restored.source_path, path);
    assert_eq!(restored.workspace.as_ref(), Some(&path));
    assert_eq!(
        restored.messages[0].snippets[0].file_path.as_ref(),
        Some(&path)
    );
    Ok(())
}

#[test]
#[serial]
fn reindex_paths_carries_only_new_packet_semantic_delta_messages() {
    let tmp = TempDir::new().unwrap();
    let xdg = tmp.path().join("xdg_watch_semantic_delta");
    std::fs::create_dir_all(&xdg).unwrap();
    let prev = dotenvy::var("XDG_DATA_HOME").ok();
    unsafe { std::env::set_var("XDG_DATA_HOME", &xdg) };
    // Disable the active-source mtime fallback: the test writes the source
    // file inline and immediately calls reindex_paths in watch mode, so
    // every fresh write would otherwise be skipped as "still being
    // written". The semantic-delta behavior under test is independent of
    // the active-source filter.
    let _window_guard = set_env("CASS_ACTIVE_SESSION_RECENT_WRITE_WINDOW_SECS", "0");

    let data_dir = xdg.join("codex");
    std::fs::create_dir_all(&data_dir).unwrap();
    let codex_dir = data_dir.join("codex/sessions");
    std::fs::create_dir_all(&codex_dir).unwrap();
    let codex_file = codex_dir.join("rollout-semantic.jsonl");
    let now_u128 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let base_ts = i64::try_from(now_u128)
        .unwrap_or(i64::MAX)
        .saturating_add(10_000);

    write_watch_codex_messages(
        &codex_file,
        "thread-semantic",
        &[
            ("user", "hi", base_ts),
            ("assistant", "hello", base_ts + 100),
        ],
    );

    let opts = crate::indexer::IndexOptions {
        full: false,
        watch: true,
        force_rebuild: false,
        db_path: data_dir.join("agent_search.db"),
        data_dir: data_dir.clone(),
        semantic: true,
        build_hnsw: false,
        embedder: "hash".to_string(),
        progress: None,
        watch_once_paths: None,
        watch_interval_secs: 30,
    };

    let storage = FrankenStorage::open(&opts.db_path).unwrap();
    let index_path = index_dir(&opts.data_dir).unwrap();
    let t_index = TantivyIndex::open_or_create(&index_path).unwrap();
    let state = std::sync::Mutex::new(std::collections::HashMap::new());
    let storage = std::sync::Mutex::new(storage);
    let t_index = std::sync::Mutex::new(Some(t_index));
    let roots = vec![(ConnectorKind::Codex, ScanRoot::local(codex_dir.clone()))];

    let mut first_delta = WatchSemanticDelta::default();
    let indexed = reindex_paths_with_semantic_delta(
        &opts,
        vec![codex_file.clone()],
        &roots,
        &state,
        &storage,
        &t_index,
        &index_path,
        false,
        Some(&mut first_delta),
        &ActiveSessionSourceFilter::new(false),
    )
    .unwrap();
    assert_eq!(indexed.indexed, 1);
    let first_contents: Vec<_> = first_delta
        .inputs
        .iter()
        .map(|input| input.content.as_str())
        .collect();
    assert_eq!(first_contents, vec!["hi", "hello"]);
    assert!(first_delta.max_message_id.is_some());

    std::thread::sleep(Duration::from_millis(25));
    write_watch_codex_messages(
        &codex_file,
        "thread-semantic",
        &[
            ("user", "hi", base_ts),
            ("assistant", "hello", base_ts + 100),
            ("assistant", "followup", base_ts + 200),
        ],
    );

    let mut second_delta = WatchSemanticDelta::default();
    let indexed = reindex_paths_with_semantic_delta(
        &opts,
        vec![codex_file.clone()],
        &roots,
        &state,
        &storage,
        &t_index,
        &index_path,
        false,
        Some(&mut second_delta),
        &ActiveSessionSourceFilter::new(false),
    )
    .unwrap();
    assert_eq!(indexed.indexed, 1);
    assert_eq!(second_delta.inputs.len(), 1);
    assert_eq!(second_delta.inputs[0].content, "followup");
    assert_eq!(
        semantic_role_name(second_delta.inputs[0].role),
        Some("assistant")
    );
    assert!(second_delta.max_message_id.is_some());

    if let Some(prev) = prev {
        unsafe { std::env::set_var("XDG_DATA_HOME", prev) };
    } else {
        unsafe { std::env::remove_var("XDG_DATA_HOME") };
    }
}

#[test]
#[serial]
fn reindex_paths_uses_oldest_trigger_window_when_state_is_newer() {
    let tmp = TempDir::new().unwrap();
    let xdg = tmp.path().join("xdg_oldest_window");
    std::fs::create_dir_all(&xdg).unwrap();
    let prev = dotenvy::var("XDG_DATA_HOME").ok();
    unsafe { std::env::set_var("XDG_DATA_HOME", &xdg) };

    // Use xdg directly (not dirs::data_dir() which doesn't respect XDG_DATA_HOME on macOS)
    let data_dir = xdg.join("codex");
    std::fs::create_dir_all(&data_dir).unwrap();
    let codex_dir = data_dir.join("codex/sessions");
    std::fs::create_dir_all(&codex_dir).unwrap();
    let codex_file = codex_dir.join("rollout-window.jsonl");
    let now_u128 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let now = i64::try_from(now_u128)
        .unwrap_or(i64::MAX)
        .saturating_add(10_000);
    write_watch_codex_messages(&codex_file, "tw", &[("user", "p", now)]);

    let opts = crate::indexer::IndexOptions {
        full: false,
        watch: false,
        force_rebuild: false,
        watch_once_paths: None,
        db_path: data_dir.join("db.sqlite"),
        data_dir: data_dir.clone(),
        semantic: false,
        build_hnsw: false,
        embedder: "fastembed".to_string(),
        progress: None,
        watch_interval_secs: 30,
    };

    let storage = FrankenStorage::open(&opts.db_path).unwrap();
    let index_path = index_dir(&opts.data_dir).unwrap();
    let t_index = TantivyIndex::open_or_create(&index_path).unwrap();
    let mut initial = HashMap::new();
    initial.insert(ConnectorKind::Codex, i64::MAX / 4);
    let state = Mutex::new(initial);
    let storage = Mutex::new(storage);
    let t_index = Mutex::new(Some(t_index));
    let roots = vec![(ConnectorKind::Codex, ScanRoot::local(codex_dir))];

    let indexed = reindex_paths(
        &opts,
        vec![codex_file],
        &roots,
        &state,
        &storage,
        &t_index,
        &index_path,
        false,
    )
    .unwrap();
    assert!(
        indexed > 0,
        "expected indexing to use trigger min_ts instead of stale future watch-state"
    );

    if let Some(prev) = prev {
        unsafe { std::env::set_var("XDG_DATA_HOME", prev) };
    } else {
        unsafe { std::env::remove_var("XDG_DATA_HOME") };
    }
}

#[test]
#[serial]
fn reindex_paths_does_not_advance_watch_state_when_scan_yields_no_conversations() {
    let tmp = TempDir::new().unwrap();
    let xdg = tmp.path().join("xdg_zero_scan");
    std::fs::create_dir_all(&xdg).unwrap();
    let prev = dotenvy::var("XDG_DATA_HOME").ok();
    unsafe { std::env::set_var("XDG_DATA_HOME", &xdg) };

    // Use xdg directly (not dirs::data_dir() which doesn't respect XDG_DATA_HOME on macOS)
    let data_dir = xdg.join("codex");
    std::fs::create_dir_all(&data_dir).unwrap();
    let codex_dir = data_dir.join("codex/sessions");
    std::fs::create_dir_all(&codex_dir).unwrap();
    let codex_file = codex_dir.join("rollout-zero.jsonl");
    // Intentionally malformed payload so scan yields zero conversations.
    std::fs::write(&codex_file, "not valid json").unwrap();

    let opts = crate::indexer::IndexOptions {
        full: false,
        watch: false,
        force_rebuild: false,
        watch_once_paths: None,
        db_path: data_dir.join("db.sqlite"),
        data_dir: data_dir.clone(),
        semantic: false,
        build_hnsw: false,
        embedder: "fastembed".to_string(),
        progress: None,
        watch_interval_secs: 30,
    };

    let storage = FrankenStorage::open(&opts.db_path).unwrap();
    let index_path = index_dir(&opts.data_dir).unwrap();
    let mut initial = HashMap::new();
    initial.insert(ConnectorKind::Codex, 10_000);
    let state = Mutex::new(initial);
    let storage = Mutex::new(storage);
    let t_index = Mutex::new(None);
    let roots = vec![(ConnectorKind::Codex, ScanRoot::local(codex_dir))];

    let indexed = reindex_paths(
        &opts,
        vec![codex_file],
        &roots,
        &state,
        &storage,
        &t_index,
        &index_path,
        false,
    )
    .unwrap();
    assert_eq!(
        indexed, 0,
        "fixture should produce no indexed conversations"
    );
    assert_eq!(
        state.lock().unwrap().get(&ConnectorKind::Codex),
        Some(&10_000)
    );
    // The storage-side last_indexed_at and Tantivy writer should remain
    // untouched: both live on the ingest path that conv_count == 0 now
    // short-circuits around. See issue #194.
    assert!(
        t_index.lock().unwrap().is_none(),
        "empty watch scan must not open Tantivy"
    );
    assert_eq!(
        storage.lock().unwrap().get_last_indexed_at().unwrap(),
        None,
        "empty watch scan must not bump last_indexed_at"
    );

    if let Some(prev) = prev {
        unsafe { std::env::set_var("XDG_DATA_HOME", prev) };
    } else {
        unsafe { std::env::remove_var("XDG_DATA_HOME") };
    }
}

#[test]
#[serial]
fn reindex_paths_updates_progress() {
    let tmp = TempDir::new().unwrap();
    // Use unique subdirectory to avoid conflicts with other tests
    let xdg = tmp.path().join("xdg_progress");
    std::fs::create_dir_all(&xdg).unwrap();
    let prev = dotenvy::var("XDG_DATA_HOME").ok();
    unsafe { std::env::set_var("XDG_DATA_HOME", &xdg) };

    // Prepare amp fixture using temp directory directly (not dirs::data_dir()
    // which doesn't respect XDG_DATA_HOME on macOS)
    let data_dir = xdg.join("codex");
    std::fs::create_dir_all(&data_dir).unwrap();
    let codex_dir = data_dir.join("codex/sessions");
    std::fs::create_dir_all(&codex_dir).unwrap();
    let codex_file = codex_dir.join("rollout-progress.jsonl");
    let now_u128 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let now = i64::try_from(now_u128)
        .unwrap_or(i64::MAX)
        .saturating_add(10_000);
    write_watch_codex_messages(&codex_file, "tp", &[("user", "p", now)]);

    let progress = Arc::new(crate::indexer::IndexingProgress::default());
    let opts = crate::indexer::IndexOptions {
        full: false,
        watch: false,
        force_rebuild: false,
        watch_once_paths: None,
        db_path: data_dir.join("db.sqlite"),
        data_dir: data_dir.clone(),
        semantic: false,
        build_hnsw: false,
        embedder: "fastembed".to_string(),
        progress: Some(progress.clone()),
        watch_interval_secs: 30,
    };

    let storage = FrankenStorage::open(&opts.db_path).unwrap();
    let index_path = index_dir(&opts.data_dir).unwrap();
    let t_index = TantivyIndex::open_or_create(&index_path).unwrap();
    let state = Mutex::new(HashMap::new());
    let storage = Mutex::new(storage);
    let t_index = Mutex::new(Some(t_index));

    reindex_paths(
        &opts,
        vec![codex_file],
        &[(ConnectorKind::Codex, ScanRoot::local(codex_dir))],
        &state,
        &storage,
        &t_index,
        &index_path,
        false,
    )
    .unwrap();

    // Progress should reflect the indexed conversation
    assert_eq!(progress.total.load(Ordering::Relaxed), 1);
    assert_eq!(progress.current.load(Ordering::Relaxed), 1);
    // Phase resets to 0 (idle) at the end
    assert_eq!(progress.phase.load(Ordering::Relaxed), 0);

    // Explicitly drop resources to release locks before cleanup
    drop(t_index);
    storage.into_inner().unwrap().close().unwrap();
    drop(state);

    if let Some(prev) = prev {
        unsafe { std::env::set_var("XDG_DATA_HOME", prev) };
    } else {
        unsafe { std::env::remove_var("XDG_DATA_HOME") };
    }
}

#[test]
#[serial]
fn reindex_paths_watch_once_ignores_file_mtime_since_ts() {
    let tmp = TempDir::new().unwrap();
    let xdg = tmp.path().join("xdg_watch_once_old_messages");
    std::fs::create_dir_all(&xdg).unwrap();
    let prev = dotenvy::var("XDG_DATA_HOME").ok();
    unsafe { std::env::set_var("XDG_DATA_HOME", &xdg) };

    let data_dir = xdg.join("codex");
    std::fs::create_dir_all(&data_dir).unwrap();
    let codex_dir = data_dir.join("codex/sessions");
    std::fs::create_dir_all(&codex_dir).unwrap();
    let codex_file = codex_dir.join("rollout-old.jsonl");

    // Intentionally ancient timestamp relative to the current file mtime.
    write_watch_codex_messages(&codex_file, "old", &[("user", "p", 1_000)]);

    let progress = Arc::new(IndexingProgress::default());
    let opts = crate::indexer::IndexOptions {
        full: false,
        watch: false,
        force_rebuild: false,
        watch_once_paths: Some(vec![codex_file.clone()]),
        db_path: data_dir.join("db.sqlite"),
        data_dir: data_dir.clone(),
        semantic: false,
        build_hnsw: false,
        embedder: "fastembed".to_string(),
        progress: Some(progress.clone()),
        watch_interval_secs: 30,
    };

    let storage = FrankenStorage::open(&opts.db_path).unwrap();
    let index_path = index_dir(&opts.data_dir).unwrap();
    let t_index = TantivyIndex::open_or_create(&index_path).unwrap();
    let mut initial = HashMap::new();
    initial.insert(ConnectorKind::Codex, i64::MAX / 4);
    let state = Mutex::new(initial);
    let storage = Mutex::new(storage);
    let t_index = Mutex::new(Some(t_index));

    let indexed = reindex_paths(
        &opts,
        vec![codex_file],
        &[(ConnectorKind::Codex, ScanRoot::local(codex_dir))],
        &state,
        &storage,
        &t_index,
        &index_path,
        false,
    )
    .unwrap();

    assert_eq!(
        indexed, 1,
        "explicit watch_once imports should ignore file mtime watermarks"
    );
    {
        let stats = progress.stats.lock().unwrap();
        assert_eq!(stats.total_conversations, 1);
        assert_eq!(stats.total_messages, 1);
        assert_eq!(stats.connectors.len(), 1);
        assert_eq!(stats.connectors[0].name, "codex");
        assert_eq!(stats.connectors[0].conversations, 1);
        assert_eq!(stats.connectors[0].messages, 1);
    }

    if let Some(prev) = prev {
        unsafe { std::env::set_var("XDG_DATA_HOME", prev) };
    } else {
        unsafe { std::env::remove_var("XDG_DATA_HOME") };
    }
}

/// GH #502: a SQLite source's committed WAL (or rollback journal) change is
/// a source change even when the main database file is untouched, while a
/// reader-rewritten -shm is not.
#[test]
fn explicit_watch_once_freshness_counts_committed_sqlite_sidecars() {
    let tmp = TempDir::new().unwrap();
    let storage = FrankenStorage::open(&tmp.path().join("cass.db")).unwrap();
    let agent_id = storage
        .ensure_agent(&Agent {
            id: None,
            slug: "devin".into(),
            name: "Devin".into(),
            version: None,
            kind: AgentKind::Cli,
        })
        .unwrap();
    let workspace_id = storage.ensure_workspace(tmp.path(), None).unwrap();
    let source = tmp.path().join("devin.db");
    std::fs::write(&source, b"main database").unwrap();
    storage
        .raw()
        .execute_compat(
            "INSERT INTO conversations(
                 id, agent_id, workspace_id, source_id, title, source_path, metadata_json
             ) VALUES (1, ?1, ?2, 'local', 'live', ?3, '{}')",
            &[
                ParamValue::from(agent_id),
                ParamValue::from(workspace_id),
                ParamValue::from(source.to_string_lossy().as_ref()),
            ],
        )
        .unwrap();
    let indexed_at = FrankenStorage::now_millis();
    storage.set_last_indexed_at(indexed_at).unwrap();
    let root = ScanRoot::local(source.clone());
    let set_mtime =
        |path: &Path, epoch_ms: i64| {
            std::fs::OpenOptions::new()
                .write(true)
                .open(path)
                .unwrap()
                .set_times(std::fs::FileTimes::new().set_modified(
                    UNIX_EPOCH + Duration::from_millis(u64::try_from(epoch_ms).unwrap()),
                ))
                .unwrap();
        };
    set_mtime(&source, indexed_at - 60_000);
    let unchanged = || {
        explicit_watch_once_root_unchanged_after_last_index(&storage, ConnectorKind::Devin, &root)
            .unwrap()
    };

    // Negative control: nothing newer than the last index keeps the fast path.
    assert!(unchanged(), "an unchanged database must skip");
    let shm = database_path_with_suffix(&source, "-shm");
    std::fs::write(&shm, b"reader index").unwrap();
    set_mtime(&shm, indexed_at + 60_000);
    assert!(
        unchanged(),
        "a reader-rewritten -shm is not a source change"
    );
    let wal = database_path_with_suffix(&source, "-wal");
    std::fs::write(&wal, b"old frames").unwrap();
    set_mtime(&wal, indexed_at - 30_000);
    assert!(unchanged(), "a WAL already covered by the last index skips");

    // The bug: a WAL-only commit after the last index, main file untouched.
    set_mtime(&wal, indexed_at + 1);
    assert!(!unchanged(), "a committed WAL append must be re-read");
    set_mtime(&wal, indexed_at - 30_000);
    let journal = database_path_with_suffix(&source, "-journal");
    std::fs::write(&journal, b"rollback journal").unwrap();
    set_mtime(&journal, indexed_at + 1);
    assert!(!unchanged(), "a newer rollback journal must be re-read");
}

#[test]
#[serial]
fn reindex_paths_watch_once_skips_unchanged_indexed_file() {
    let tmp = TempDir::new().unwrap();
    let xdg = tmp.path().join("xdg_watch_once_unchanged");
    std::fs::create_dir_all(&xdg).unwrap();
    let prev = dotenvy::var("XDG_DATA_HOME").ok();
    unsafe { std::env::set_var("XDG_DATA_HOME", &xdg) };

    let data_dir = xdg.join("codex");
    std::fs::create_dir_all(&data_dir).unwrap();
    let codex_dir = data_dir.join("codex/sessions");
    std::fs::create_dir_all(&codex_dir).unwrap();
    let codex_file = codex_dir.join("rollout-unchanged.jsonl");
    write_watch_codex_messages(
        &codex_file,
        "unchanged",
        &[("user", "p", 1_700_000_000_100)],
    );

    let opts = crate::indexer::IndexOptions {
        full: false,
        watch: false,
        force_rebuild: false,
        watch_once_paths: Some(vec![codex_file.clone()]),
        db_path: data_dir.join("db.sqlite"),
        data_dir: data_dir.clone(),
        semantic: false,
        build_hnsw: false,
        embedder: "fastembed".to_string(),
        progress: None,
        watch_interval_secs: 30,
    };

    let storage = FrankenStorage::open(&opts.db_path).unwrap();
    let index_path = index_dir(&opts.data_dir).unwrap();
    let t_index = TantivyIndex::open_or_create(&index_path).unwrap();
    let state = Mutex::new(HashMap::new());
    let storage = Mutex::new(storage);
    let t_index = Mutex::new(Some(t_index));

    let first = reindex_paths(
        &opts,
        vec![codex_file.clone()],
        &[(ConnectorKind::Codex, ScanRoot::local(codex_dir.clone()))],
        &state,
        &storage,
        &t_index,
        &index_path,
        false,
    )
    .unwrap();
    assert_eq!(first, 1);

    storage.lock().unwrap().set_last_indexed_at(0).unwrap();

    let before_last_indexed_marker = {
        let guard = storage.lock().unwrap();
        should_skip_unchanged_explicit_watch_once_paths(
            &opts,
            &guard,
            &[(ConnectorKind::Codex, ScanRoot::local(codex_dir.clone()))],
        )
        .unwrap()
    };
    assert!(
        !before_last_indexed_marker,
        "indexed files without a settled last_indexed_at marker must still flow through watch-once ingest"
    );

    storage
        .lock()
        .unwrap()
        .set_last_indexed_at(FrankenStorage::now_millis().saturating_add(10_000))
        .unwrap();

    let preopen_skip = {
        let guard = storage.lock().unwrap();
        should_skip_unchanged_explicit_watch_once_paths(
            &opts,
            &guard,
            &[(ConnectorKind::Codex, ScanRoot::local(codex_dir.clone()))],
        )
        .unwrap()
    };
    assert!(
        preopen_skip,
        "unchanged explicit watch-once files can skip before Tantivy is opened"
    );

    let startup_skip = {
        let guard = storage.lock().unwrap();
        can_skip_unchanged_explicit_watch_once_index_run(&opts, &guard, &index_path, false, false)
            .unwrap()
    };
    assert!(
        startup_skip,
        "unchanged explicit watch-once files with current lexical assets can skip startup maintenance"
    );
    let migration_skip = {
        let guard = storage.lock().unwrap();
        can_skip_unchanged_explicit_watch_once_index_run(&opts, &guard, &index_path, true, false)
            .unwrap()
    };
    assert!(
        !migration_skip,
        "a pending canonical identity migration must publish fresh lexical assets"
    );
    let analytics_migration_skip = {
        let guard = storage.lock().unwrap();
        can_skip_unchanged_explicit_watch_once_index_run(&opts, &guard, &index_path, false, true)
            .unwrap()
    };
    assert!(
        !analytics_migration_skip,
        "an analytics-only legacy OMP migration must resume after lexical publication"
    );

    let second = reindex_paths(
        &opts,
        vec![codex_file],
        &[(ConnectorKind::Codex, ScanRoot::local(codex_dir))],
        &state,
        &storage,
        &t_index,
        &index_path,
        false,
    )
    .unwrap();
    assert_eq!(
        second, 0,
        "unchanged explicit watch-once files already indexed before last_indexed_at should skip storage ingest"
    );

    if let Some(prev) = prev {
        unsafe { std::env::set_var("XDG_DATA_HOME", prev) };
    } else {
        unsafe { std::env::remove_var("XDG_DATA_HOME") };
    }
}
