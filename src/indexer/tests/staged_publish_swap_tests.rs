use super::super::*;
use super::helpers::*;
use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use serial_test::serial;
use tempfile::TempDir;

#[test]
#[serial]
fn rebuild_tantivy_from_db_promotes_pipeline_budgets_after_first_commit() {
    let _responsiveness = set_env("CASS_RESPONSIVENESS_DISABLE", "1");
    let _controller_mode = set_env("CASS_TANTIVY_REBUILD_CONTROLLER_MODE", "auto");
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    seed_lexical_rebuild_fixture(&storage);
    let agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: Some("0.2.3".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();
    for (suffix, base_ts) in [("3", 1_700_000_002_000_i64), ("4", 1_700_000_003_000_i64)] {
        let external_id = format!("lexical-fixture-{suffix}");
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

    let _conversation_limit = set_env("CASS_TANTIVY_REBUILD_BATCH_FETCH_CONVERSATIONS", "2");
    let _initial_conversation_limit = set_env(
        "CASS_TANTIVY_REBUILD_INITIAL_BATCH_FETCH_CONVERSATIONS",
        "1",
    );
    let _commit_messages = set_env("CASS_TANTIVY_REBUILD_COMMIT_EVERY_MESSAGES", "4");
    let _initial_commit_messages =
        set_env("CASS_TANTIVY_REBUILD_INITIAL_COMMIT_EVERY_MESSAGES", "2");
    let _commit_message_bytes = set_env("CASS_TANTIVY_REBUILD_COMMIT_EVERY_MESSAGE_BYTES", "4096");
    let _initial_commit_message_bytes = set_env(
        "CASS_TANTIVY_REBUILD_INITIAL_COMMIT_EVERY_MESSAGE_BYTES",
        "64",
    );
    let _channel_size = set_env("CASS_TANTIVY_REBUILD_PIPELINE_CHANNEL_SIZE", "1");
    let _controller_loadavg_high = set_env(
        "CASS_TANTIVY_REBUILD_CONTROLLER_LOADAVG_HIGH_WATERMARK_1M",
        "1000000",
    );
    let _controller_loadavg_low = set_env(
        "CASS_TANTIVY_REBUILD_CONTROLLER_LOADAVG_LOW_WATERMARK_1M",
        "999999",
    );

    let logs = capture_logs(|| {
        let rebuild = rebuild_tantivy_from_db(&db_path, &data_dir, 4, None).unwrap();
        assert_eq!(rebuild.indexed_docs - rebuild.indexed_region_docs, 8);
        assert_eq!(rebuild.observed_messages, Some(8));
        assert!(rebuild.exact_checkpoint_persisted);
    });

    // bet45: startup->steady promotion has two legitimate triggers that
    // race on tiny budgets. The startup flow budget IS the initial
    // commit-bytes interval (startup_commit_interval_message_bytes feeds
    // the startup pipeline budget's batch-fetch bytes and in-flight cap),
    // and the prep worker's working-set reservation is budget-limit
    // derived, so a page-prep budget wait before the first durable commit
    // is inherent here — when the controller samples that waiter first it
    // promotes with a `startup_*_promoted_steady_budget_before_first_
    // durable_commit` reason and the later commit-side
    // record_first_durable_commit correctly becomes a no-op. Both paths
    // prove the invariant under test: the startup budget was promoted to
    // the steady budget exactly once, with the budget-update log emitted.
    assert!(
        logs.contains("updated lexical rebuild pipeline budgets")
            && (logs.contains("controller_reason=first_durable_commit_promoted_steady_budget")
                || logs.contains("promoted_steady_budget_before_first_durable_commit")),
        "expected a startup->steady budget-promotion log, got:
{logs}"
    );
    assert!(
        logs.contains("old_page_conversation_limit=1"),
        "expected startup page budget in logs, got:
{logs}"
    );
    assert!(
        logs.contains("new_page_conversation_limit=2"),
        "expected steady page budget in logs, got:
{logs}"
    );
    assert!(
        logs.contains("old_batch_fetch_message_bytes_limit=64"),
        "expected startup byte budget in logs, got:
{logs}"
    );
    assert!(
        logs.contains("new_batch_fetch_message_bytes_limit=4096"),
        "expected steady byte budget in logs, got:
{logs}"
    );
    let producer_budget_lines = logs
        .lines()
        .filter(|line| line.contains("lexical rebuild producer adopted pipeline budgets"))
        .collect::<Vec<_>>();
    assert!(
        !producer_budget_lines.is_empty(),
        "expected producer budget adoption logs, got:
{logs}"
    );
    assert!(
        producer_budget_lines
            .iter()
            .any(|line| line.contains("page_conversation_limit=1")),
        "expected startup producer page budget adoption log, got:
{logs}"
    );
    assert!(
        producer_budget_lines.iter().any(|line| {
            line.contains("page_conversation_limit=2")
                && line.contains("batch_fetch_message_limit=4")
                && line.contains("batch_fetch_message_bytes_limit=4096")
        }),
        "expected steady-state producer budget adoption log, got:
{logs}"
    );
    assert_eq!(tantivy_doc_count_for_data_dir(&data_dir), 8);
}

/// GH #382's one-worker/one-channel reproduction: after the first
/// startup-sized flush, the old implementation silently replaced the
/// pinned conservative sink limit with the larger steady limit. Two page
/// reservations then filled the byte window before the sink could reach
/// that larger threshold, leaving producer and consumer parked forever.
#[test]
#[serial]
fn gh382_reduced_pipeline_completes_across_startup_sized_flushes() {
    let _controller = set_env("CASS_TANTIVY_REBUILD_CONTROLLER_MODE", "conservative");
    let _workers = set_env("CASS_TANTIVY_REBUILD_WORKERS", "1");
    let _page_workers = set_env("CASS_TANTIVY_REBUILD_PAGE_PREP_WORKERS", "1");
    let _channel = set_env("CASS_TANTIVY_REBUILD_PIPELINE_CHANNEL_SIZE", "1");
    let _steady_fetch = set_env("CASS_TANTIVY_REBUILD_BATCH_FETCH_CONVERSATIONS", "8");
    let _startup_fetch = set_env(
        "CASS_TANTIVY_REBUILD_INITIAL_BATCH_FETCH_CONVERSATIONS",
        "2",
    );
    let _steady_commit_conversations =
        set_env("CASS_TANTIVY_REBUILD_COMMIT_EVERY_CONVERSATIONS", "4096");
    let _startup_commit_conversations = set_env(
        "CASS_TANTIVY_REBUILD_INITIAL_COMMIT_EVERY_CONVERSATIONS",
        "4096",
    );
    let _steady_commit_messages = set_env("CASS_TANTIVY_REBUILD_COMMIT_EVERY_MESSAGES", "4096");
    let _startup_commit_messages =
        set_env("CASS_TANTIVY_REBUILD_INITIAL_COMMIT_EVERY_MESSAGES", "4096");
    let _steady_commit_bytes = set_env("CASS_TANTIVY_REBUILD_COMMIT_EVERY_MESSAGE_BYTES", "4096");
    let _startup_commit_bytes = set_env(
        "CASS_TANTIVY_REBUILD_INITIAL_COMMIT_EVERY_MESSAGE_BYTES",
        "4096",
    );

    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    let conversations = (0..12)
        .map(|idx| large_startup_conv("codex", "gh382-reduced", idx, 2, 128, 1_700_500_000_000))
        .collect::<Vec<_>>();
    ingest_batch(
        &storage,
        None,
        &data_dir,
        &conversations,
        &None,
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
        false,
    )
    .unwrap();
    drop(storage);

    let rebuild = rebuild_tantivy_from_db(&db_path, &data_dir, conversations.len(), None)
        .expect("reduced GH #382 geometry must make forward progress to completion");
    // Task 5: `indexed_docs` includes converged search-region
    // documents; the canonical share is what this test means.
    assert_eq!(
        rebuild.indexed_docs - rebuild.indexed_region_docs,
        conversations.len() * 2
    );
    assert_eq!(rebuild.observed_messages, Some(conversations.len() * 2));
    assert!(rebuild.exact_checkpoint_persisted);
    assert_eq!(
        tantivy_doc_count_for_data_dir(&data_dir),
        (conversations.len() * 2) as u64
    );
}

#[test]
#[serial]
fn rebuild_tantivy_from_db_resume_reports_total_observed_messages() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    let index_path = index_dir(&data_dir).unwrap();
    let mut index = TantivyIndex::open_or_create(&index_path).unwrap();

    let first = norm_conv(Some("resume-1"), vec![norm_msg(0, 100), norm_msg(1, 200)]);
    persist::persist_conversation(&storage, &mut index, &first).unwrap();
    index.commit().unwrap();

    let _defer_guard = set_env("CASS_DEFER_LEXICAL_UPDATES", "1");
    let second = norm_conv(Some("resume-2"), vec![norm_msg(0, 300), norm_msg(1, 400)]);
    persist::persist_conversation(&storage, &mut index, &second).unwrap();

    let db_state = lexical_rebuild_db_state(&storage, &db_path).unwrap();
    let committed_meta_fingerprint = index_meta_fingerprint(&index_path).unwrap();
    let mut state = LexicalRebuildState::new(db_state, LEXICAL_REBUILD_PAGE_SIZE);
    state.committed_offset = 1;
    state.processed_conversations = 1;
    state.indexed_docs = 2;
    state.committed_meta_fingerprint = committed_meta_fingerprint;
    persist_lexical_rebuild_state(&index_path, &state).unwrap();
    drop(index);

    let rebuild = rebuild_tantivy_from_db(&db_path, &data_dir, 2, None).unwrap();
    assert_eq!(rebuild.indexed_docs - rebuild.indexed_region_docs, 4);
    assert_eq!(rebuild.observed_messages, Some(4));
    assert!(rebuild.exact_checkpoint_persisted);
    let checkpoint = load_lexical_rebuild_checkpoint(&index_path)
        .unwrap()
        .expect("completed checkpoint after rebuild");
    assert!(checkpoint.completed);
    assert_eq!(checkpoint.processed_conversations, 2);
    assert_eq!(checkpoint.committed_offset, 2);
    assert_eq!(checkpoint.indexed_docs - checkpoint.indexed_region_docs, 4);
    let state = load_lexical_rebuild_state(&index_path)
        .unwrap()
        .expect("completed lexical rebuild state after rebuild");
    assert_eq!(state.db.total_messages, 4);
    assert_eq!(tantivy_doc_count_for_data_dir(&data_dir), 4);
}

#[test]
#[serial]
fn rebuild_tantivy_from_db_exposes_active_pipeline_runtime_to_attachers() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);

    let conversation_count = 24usize;
    let messages_per_conversation = 24usize;
    let expected_docs = conversation_count * messages_per_conversation;
    let convs = (0..conversation_count)
        .map(|idx| {
            large_startup_conv(
                "codex",
                "attach-active-runtime",
                idx,
                messages_per_conversation,
                8 * 1024,
                1_700_000_000_000,
            )
        })
        .collect::<Vec<_>>();
    ingest_batch(
        &storage,
        None,
        &data_dir,
        &convs,
        &None,
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
        false,
    )
    .unwrap();
    drop(storage);

    let _fetch_conversations = set_env("CASS_TANTIVY_REBUILD_BATCH_FETCH_CONVERSATIONS", "1");
    let _startup_fetch_conversations = set_env(
        "CASS_TANTIVY_REBUILD_INITIAL_BATCH_FETCH_CONVERSATIONS",
        "1",
    );
    let _progress_conversations = set_env(
        "CASS_TANTIVY_REBUILD_PROGRESS_HEARTBEAT_EVERY_CONVERSATIONS",
        "1",
    );
    let _progress_ms = set_env("CASS_TANTIVY_REBUILD_PROGRESS_HEARTBEAT_EVERY_MS", "1");
    let _commit_conversations = set_env("CASS_TANTIVY_REBUILD_COMMIT_EVERY_CONVERSATIONS", "4096");
    let _startup_commit_conversations = set_env(
        "CASS_TANTIVY_REBUILD_INITIAL_COMMIT_EVERY_CONVERSATIONS",
        "4096",
    );
    let _workers = set_env("CASS_TANTIVY_REBUILD_WORKERS", "6");
    let _writer_threads = set_env("CASS_TANTIVY_MAX_WRITER_THREADS", "2");
    let _pipeline_channel = set_env("CASS_TANTIVY_REBUILD_PIPELINE_CHANNEL_SIZE", "1");
    let _page_prep_workers = set_env("CASS_TANTIVY_REBUILD_PAGE_PREP_WORKERS", "1");

    let db_path_for_thread = db_path.clone();
    let data_dir_for_thread = data_dir.clone();
    let rebuild_handle = std::thread::spawn(move || {
        rebuild_tantivy_from_db_with_options(
            &db_path_for_thread,
            &data_dir_for_thread,
            2,
            None,
            LexicalRebuildStartupOptions {
                defer_initial_content_fingerprint: true,
            },
            None,
        )
    });

    let index_path = index_dir(&data_dir).unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    let active_runtime = loop {
        if let Some(runtime) =
            load_active_lexical_rebuild_pipeline_runtime(&index_path, &db_path).unwrap()
            && runtime.is_observed()
        {
            break runtime;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for active rebuild pipeline runtime to become attach-visible"
        );
        std::thread::sleep(Duration::from_millis(25));
    };

    assert!(
        active_runtime.updated_at_ms > 0,
        "attach-visible active runtime should include an update timestamp"
    );
    assert!(
        active_runtime.page_prep_workers > 0
            || active_runtime.pending_batch_conversations > 0
            || active_runtime.queue_depth > 0
            || active_runtime.staged_shard_build_workers_max > 0,
        "attach-visible runtime should expose concrete pipeline activity: {active_runtime:?}"
    );

    let rebuild = rebuild_handle.join().unwrap().unwrap();
    assert_eq!(
        rebuild.indexed_docs - rebuild.indexed_region_docs,
        expected_docs
    );
    assert_eq!(rebuild.observed_messages, Some(expected_docs));
    assert!(
        load_active_lexical_rebuild_pipeline_runtime(&index_path, &db_path)
            .unwrap()
            .is_none(),
        "completed rebuild should stop advertising active pipeline runtime"
    );
}

#[test]
#[serial]
fn rebuild_tantivy_from_db_deferred_startup_ignores_stale_zero_count_and_persists_exact_state() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    seed_lexical_rebuild_fixture(&storage);

    let rebuild = rebuild_tantivy_from_db_with_options(
        &db_path,
        &data_dir,
        // GH #413: an interrupted sidecar is a hint, not authority. A
        // stale zero must not short-circuit before useful Quill ingest.
        0,
        None,
        LexicalRebuildStartupOptions {
            defer_initial_content_fingerprint: true,
        },
        None,
    )
    .unwrap();
    assert_eq!(rebuild.observed_conversations, 2);
    assert_eq!(rebuild.indexed_docs - rebuild.indexed_region_docs, 4);
    assert_eq!(rebuild.observed_messages, Some(4));
    assert!(rebuild.exact_checkpoint_persisted);

    let index_path = index_dir(&data_dir).unwrap();
    let checkpoint = load_lexical_rebuild_checkpoint(&index_path)
        .unwrap()
        .expect("completed checkpoint after deferred-fingerprint rebuild");
    assert!(checkpoint.completed);
    assert_eq!(checkpoint.total_conversations, 2);
    assert_eq!(checkpoint.processed_conversations, 2);
    assert_eq!(checkpoint.committed_offset, 2);
    assert_eq!(
        checkpoint.storage_fingerprint,
        lexical_rebuild_storage_fingerprint(&db_path).unwrap()
    );
    let state = load_lexical_rebuild_state(&index_path)
        .unwrap()
        .expect("completed lexical rebuild state after deferred-fingerprint rebuild");
    assert_eq!(
        state.committed_meta_fingerprint.as_deref(),
        index_meta_fingerprint(&index_path).unwrap().as_deref(),
        "shared-writer completion should retain the final committed meta fingerprint"
    );
    assert_eq!(tantivy_doc_count_for_data_dir(&data_dir), 4);
}

/// cass#368: the storage-fingerprint failure chain must keep its root
/// cause. Every render site formats with `{err:#}`; if the chain itself
/// were collapsed (e.g. a `map_err` swapping in a fresh error), users
/// would again see only "opening readonly storage ..." while the real
/// frankensqlite failure (a corrupt FTS5 structure record, a bad header)
/// stays invisible.
#[test]
fn storage_fingerprint_error_keeps_root_cause_in_alternate_display() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("corrupt.db");
    std::fs::write(&db_path, b"definitely not a sqlite database").unwrap();

    let err = lexical_rebuild_storage_fingerprint(&db_path).unwrap_err();
    assert!(
        err.chain().count() > 1,
        "fingerprint failure must carry the inner storage-open cause, got: {err:#}"
    );
    let root_cause = err.chain().last().unwrap().to_string();
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("opening readonly storage to compute lexical fingerprint"),
        "outer context missing: {rendered}"
    );
    assert!(
        rendered.contains(root_cause.trim()),
        "root cause {root_cause:?} must survive alternate display: {rendered}"
    );
}

#[test]
#[serial]
fn rebuild_tantivy_from_db_deferred_startup_emits_deferred_prep_profile_logs() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    seed_lexical_rebuild_fixture(&storage);

    let _prep_profile = set_env("CASS_PREP_PROFILE", "1");
    let _conversation_limit = set_env("CASS_TANTIVY_REBUILD_BATCH_FETCH_CONVERSATIONS", "2");

    let logs = capture_logs(|| {
        let rebuild =
            rebuild_tantivy_from_db_deferred_startup(&db_path, &data_dir, 2, None).unwrap();
        assert_eq!(rebuild.indexed_docs - rebuild.indexed_region_docs, 4);
        assert_eq!(rebuild.observed_messages, Some(4));
    });

    assert!(
        logs.contains(r#"step="prepare_db_state_deferred_fingerprint""#),
        "expected deferred startup prep-profile log, got:\n{logs}"
    );
    assert!(
        !logs.contains(r#"step="compute_db_state_fingerprint""#),
        "deferred startup should skip exact startup fingerprinting: {logs}"
    );
    // Bead 9752r: after 9ct8r, restart_from_zero rebuilds go
    // through the staged-shards path which does not open a live
    // Tantivy writer (no `open_tantivy` step). The staged-path
    // analogue for producer-handoff ordering is
    // `persist_initial_checkpoint`: the producer must start up
    // before the initial checkpoint is written.
    let start_packet_producer = logs
        .find(r#"step="start_packet_producer""#)
        .expect("start_packet_producer log position");
    let persist_initial_checkpoint = logs
        .find(r#"step="persist_initial_checkpoint""#)
        .expect("persist_initial_checkpoint log position");
    assert!(
        start_packet_producer < persist_initial_checkpoint,
        "deferred fresh staged rebuild should start the packet producer before \
         persisting the initial checkpoint (producer-handoff overlap): {logs}"
    );
}

#[test]
#[serial]
// Asserts the staged-shard rebuild strategy, which builds N independent
// index directories and merges them. That merge is Tantivy-only (Quill's
// concat_merge joins segments WITHIN one manifest), so the strategy is
// disabled for the Quill backend — see the `staged_shard_plan` binding.
// Ignored rather than deleted: these are exactly the tests that must pass
// if an N-directory merge is ever built, and weakening them would hide
// that the strategy no longer runs.
#[ignore = "staged-shard strategy is Tantivy-only; disabled on the Quill backend"]
fn rebuild_tantivy_from_db_fresh_run_uses_staged_shard_build_path() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    seed_lexical_rebuild_fixture(&storage);

    let _workers = set_env("CASS_TANTIVY_REBUILD_WORKERS", "6");
    let _writer_threads = set_env("CASS_TANTIVY_MAX_WRITER_THREADS", "2");
    let _fetch_conversations = set_env("CASS_TANTIVY_REBUILD_BATCH_FETCH_CONVERSATIONS", "1");
    let _startup_fetch_conversations = set_env(
        "CASS_TANTIVY_REBUILD_INITIAL_BATCH_FETCH_CONVERSATIONS",
        "1",
    );
    let _first_budget_promotion_wait = set_env(
        "CASS_TANTIVY_REBUILD_FIRST_BUDGET_PROMOTION_WAIT_MS",
        "5000",
    );
    let _commit_conversations = set_env("CASS_TANTIVY_REBUILD_COMMIT_EVERY_CONVERSATIONS", "1");
    let _startup_commit_conversations = set_env(
        "CASS_TANTIVY_REBUILD_INITIAL_COMMIT_EVERY_CONVERSATIONS",
        "1",
    );

    let logs = capture_logs(|| {
        let rebuild = rebuild_tantivy_from_db_with_options(
            &db_path,
            &data_dir,
            2,
            None,
            LexicalRebuildStartupOptions {
                defer_initial_content_fingerprint: true,
            },
            None,
        )
        .unwrap();
        assert_eq!(rebuild.indexed_docs - rebuild.indexed_region_docs, 4);
        assert_eq!(rebuild.observed_messages, Some(4));
        assert!(rebuild.exact_checkpoint_persisted);
    });

    assert!(
        logs.contains("staged shard-build path"),
        "expected staged shard-build log, got:\n{logs}"
    );
    assert!(
        !logs.contains("timed out waiting for first durable budget promotion"),
        "staged shard-build path should advance the first durable shard without a producer timeout: {logs}"
    );
    let index_path = index_dir(&data_dir).unwrap();
    let state = load_lexical_rebuild_state(&index_path)
        .unwrap()
        .expect("completed lexical rebuild state after staged shard build");
    assert!(state.completed);
    assert_eq!(
        state.execution_mode,
        Some(LexicalRebuildExecutionMode::SharedWriter)
    );
    assert_eq!(tantivy_doc_count_for_data_dir(&data_dir), 4);
}

#[test]
#[serial]
// Asserts the staged-shard rebuild strategy, which builds N independent
// index directories and merges them. That merge is Tantivy-only (Quill's
// concat_merge joins segments WITHIN one manifest), so the strategy is
// disabled for the Quill backend — see the `staged_shard_plan` binding.
// Ignored rather than deleted: these are exactly the tests that must pass
// if an N-directory merge is ever built, and weakening them would hide
// that the strategy no longer runs.
#[ignore = "staged-shard strategy is Tantivy-only; disabled on the Quill backend"]
fn rebuild_tantivy_from_db_bounded_final_frontier_publishes_federated_without_remerge() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    seed_lexical_rebuild_fixture(&storage);
    let agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: Some("0.2.3".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();
    for (suffix, base_ts) in [("3", 1_700_000_002_000_i64), ("4", 1_700_000_003_000_i64)] {
        let external_id = format!("lexical-fixture-{suffix}");
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

    let _workers = set_env("CASS_TANTIVY_REBUILD_WORKERS", "6");
    let _writer_threads = set_env("CASS_TANTIVY_MAX_WRITER_THREADS", "2");
    let _fetch_conversations = set_env("CASS_TANTIVY_REBUILD_BATCH_FETCH_CONVERSATIONS", "1");
    let _startup_fetch_conversations = set_env(
        "CASS_TANTIVY_REBUILD_INITIAL_BATCH_FETCH_CONVERSATIONS",
        "1",
    );
    let _commit_conversations = set_env("CASS_TANTIVY_REBUILD_COMMIT_EVERY_CONVERSATIONS", "1");
    let _startup_commit_conversations = set_env(
        "CASS_TANTIVY_REBUILD_INITIAL_COMMIT_EVERY_CONVERSATIONS",
        "1",
    );

    let logs = capture_logs(|| {
        let rebuild = rebuild_tantivy_from_db_with_options(
            &db_path,
            &data_dir,
            2,
            None,
            LexicalRebuildStartupOptions {
                defer_initial_content_fingerprint: true,
            },
            None,
        )
        .unwrap();
        assert_eq!(rebuild.indexed_docs - rebuild.indexed_region_docs, 8);
        assert_eq!(rebuild.observed_messages, Some(8));
        assert!(rebuild.exact_checkpoint_persisted);
    });

    assert!(
        logs.contains("staged shard-build path"),
        "expected staged shard-build log, got:\n{logs}"
    );
    assert!(
        logs.contains(
            "publishing staged lexical rebuild as federated lexical shard set without final assembly collapse"
        ),
        "expected bounded final-frontier federated publish log, got:\n{logs}"
    );
    assert!(
        !logs.contains("running staged lexical rebuild merge round"),
        "a bounded final frontier should not trigger a redundant final merge round: {logs}"
    );
    assert_eq!(tantivy_doc_count_for_data_dir(&data_dir), 8);
}

#[test]
#[serial]
// Asserts the staged-shard rebuild strategy, which builds N independent
// index directories and merges them. That merge is Tantivy-only (Quill's
// concat_merge joins segments WITHIN one manifest), so the strategy is
// disabled for the Quill backend — see the `staged_shard_plan` binding.
// Ignored rather than deleted: these are exactly the tests that must pass
// if an N-directory merge is ever built, and weakening them would hide
// that the strategy no longer runs.
#[ignore = "staged-shard strategy is Tantivy-only; disabled on the Quill backend"]
fn rebuild_tantivy_from_db_multi_artifact_final_frontier_assembles_publish_generation_without_doc_remerge()
 {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    seed_lexical_rebuild_fixture(&storage);
    let agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: Some("0.2.3".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();
    let base_ts = 1_700_000_100_000_i64;
    let external_id = "lexical-fixture-3";
    storage
        .insert_conversation_tree(
            agent_id,
            None,
            &Conversation {
                id: None,
                agent_slug: "codex".into(),
                workspace: Some(PathBuf::from("/tmp/workspace")),
                external_id: Some(external_id.into()),
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

    let _workers = set_env("CASS_TANTIVY_REBUILD_WORKERS", "6");
    let _writer_threads = set_env("CASS_TANTIVY_MAX_WRITER_THREADS", "2");
    let _fetch_conversations = set_env("CASS_TANTIVY_REBUILD_BATCH_FETCH_CONVERSATIONS", "1");
    let _startup_fetch_conversations = set_env(
        "CASS_TANTIVY_REBUILD_INITIAL_BATCH_FETCH_CONVERSATIONS",
        "1",
    );
    let _commit_conversations = set_env("CASS_TANTIVY_REBUILD_COMMIT_EVERY_CONVERSATIONS", "1");
    let _startup_commit_conversations = set_env(
        "CASS_TANTIVY_REBUILD_INITIAL_COMMIT_EVERY_CONVERSATIONS",
        "1",
    );

    let logs = capture_logs(|| {
        let rebuild = rebuild_tantivy_from_db_with_options(
            &db_path,
            &data_dir,
            2,
            None,
            LexicalRebuildStartupOptions {
                defer_initial_content_fingerprint: true,
            },
            None,
        )
        .unwrap();
        assert_eq!(rebuild.indexed_docs - rebuild.indexed_region_docs, 6);
        assert_eq!(rebuild.observed_messages, Some(6));
        assert!(rebuild.exact_checkpoint_persisted);
    });

    assert!(
        logs.contains("staged shard-build path"),
        "expected staged shard-build log, got:\n{logs}"
    );
    assert!(
        logs.contains(
            "publishing staged lexical rebuild as federated lexical shard set without final assembly collapse"
        ),
        "expected federated final publish log, got:\n{logs}"
    );
    assert!(
        !logs.contains("running staged lexical rebuild merge round"),
        "federated multi-artifact publish should avoid the fallback merge-tree tail: {logs}"
    );
    let index_path = index_dir(&data_dir).unwrap();
    assert_eq!(
        crate::search::tantivy::open_federated_search_readers(&index_path)
            .unwrap()
            .expect("published federated readers")
            .len(),
        3,
        "published staged rebuild should preserve the three final shard artifacts as a federated lexical bundle"
    );
    assert_eq!(
        crate::search::tantivy::searchable_index_summary(&index_path)
            .unwrap()
            .expect("searchable summary")
            .segments,
        3,
        "published staged rebuild should report the three preserved final shard segments"
    );
    assert_eq!(tantivy_doc_count_for_data_dir(&data_dir), 6);
}

#[test]
#[serial]
fn publish_staged_lexical_index_replaces_live_index_and_retains_prior_backup() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let index_path = index_dir(&data_dir).unwrap();

    let old_conv = norm_conv(Some("publish-old"), vec![norm_msg(0, 1_700_000_000_000)]);
    let mut live_index = TantivyIndex::open_or_create(&index_path).unwrap();
    live_index
        .add_messages_with_conversation_id(&old_conv, &old_conv.messages, Some(1))
        .unwrap();
    live_index.commit().unwrap();
    drop(live_index);

    let stage_root = TempDirBuilder::new()
        .prefix("cass-test-publish-stage.")
        .tempdir_in(index_path.parent().unwrap())
        .unwrap();
    let staged_index_path = stage_root.path().join("staged");
    let new_conv = norm_conv(
        Some("publish-new"),
        vec![
            norm_msg(0, 1_700_000_001_000),
            norm_msg(1, 1_700_000_001_100),
        ],
    );
    let mut staged_index = TantivyIndex::open_or_create(&staged_index_path).unwrap();
    staged_index
        .add_messages_with_conversation_id(&new_conv, &new_conv.messages, Some(2))
        .unwrap();
    staged_index.commit().unwrap();
    drop(staged_index);

    assert_eq!(
        crate::search::tantivy::searchable_index_summary(&index_path)
            .unwrap()
            .unwrap()
            .docs,
        1
    );
    assert_eq!(
        crate::search::tantivy::searchable_index_summary(&staged_index_path)
            .unwrap()
            .unwrap()
            .docs,
        2
    );

    publish_staged_lexical_index(&staged_index_path, &index_path).unwrap();

    assert_eq!(
        crate::search::tantivy::searchable_index_summary(&index_path)
            .unwrap()
            .unwrap()
            .docs,
        2,
        "the live lexical index should expose the new staged publish"
    );
    assert!(
        !staged_index_path.exists(),
        "the staged publish path should be consumed into the retained backup flow"
    );

    let retained_backups = fs::read_dir(lexical_publish_backups_dir(&index_path))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    assert_eq!(retained_backups.len(), 1);
    assert_eq!(
        crate::search::tantivy::searchable_index_summary(&retained_backups[0])
            .unwrap()
            .unwrap()
            .docs,
        1,
        "the retained backup should preserve the previously published live index"
    );
}

/// Regression for GH #333: an authoritative rebuild must not move the
/// currently searchable generation out of the live path before its staged
/// replacement is ready. The injected publication failure models a killed
/// or failed post-lexical/semantic run: the command must fail while the
/// exact prior live document surface remains searchable.
#[test]
#[serial]
// Asserts the staged-shard rebuild strategy, which builds N independent
// index directories and merges them. That merge is Tantivy-only (Quill's
// concat_merge joins segments WITHIN one manifest), so the strategy is
// disabled for the Quill backend — see the `staged_shard_plan` binding.
// Ignored rather than deleted: these are exactly the tests that must pass
// if an N-directory merge is ever built, and weakening them would hide
// that the strategy no longer runs.
#[ignore = "staged-shard strategy is Tantivy-only; disabled on the Quill backend"]
fn run_index_authoritative_rebuild_failure_preserves_prior_live_generation() {
    const ENOSPC_RAW_OS_ERROR: i32 = 28;

    let tmp = TempDir::new().unwrap();
    let home = tmp.path().join("home");
    let data_dir = home.join("cass-data");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("db.sqlite");
    let mut storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    seed_lexical_rebuild_fixture(&storage);
    storage.close_best_effort_in_place();

    let index_path = index_dir(&data_dir).unwrap();
    let prior_conv = norm_conv(
        Some("gh333-prior-live"),
        vec![norm_msg(0, 1_700_013_000_000)],
    );
    let mut prior_live = TantivyIndex::open_or_create(&index_path).unwrap();
    prior_live
        .add_messages_with_conversation_id(&prior_conv, &prior_conv.messages, Some(1_300))
        .unwrap();
    prior_live.commit().unwrap();
    drop(prior_live);
    assert_eq!(
        crate::search::tantivy::searchable_index_summary(&index_path)
            .unwrap()
            .unwrap()
            .docs,
        1
    );

    // Force the normal run_index preflight down the authoritative rebuild
    // path without using --force-rebuild's readonly fast path.
    fs::write(
        index_path.join("schema_hash.json"),
        br#"{"schema_hash":"gh333-stale-schema"}"#,
    )
    .unwrap();

    let _home = set_env("HOME", &home.to_string_lossy());
    let _codex_home = set_env("CODEX_HOME", &home.join(".codex-empty").to_string_lossy());
    let _ignore_sources = ignore_sources_config();
    let progress = Arc::new(IndexingProgress::default());
    let opts = IndexOptions {
        full: false,
        force_rebuild: false,
        watch: false,
        watch_once_paths: None,
        db_path,
        data_dir: data_dir.clone(),
        semantic: false,
        build_hnsw: false,
        embedder: "hash".to_string(),
        progress: Some(progress),
        watch_interval_secs: 30,
    };

    #[cfg(target_os = "linux")]
    let _fault = inject_lexical_publish_rename_failure_once(
        LexicalPublishRenameSite::LinuxParkPriorLiveToCanonicalSidecar,
        ENOSPC_RAW_OS_ERROR,
    );
    #[cfg(not(target_os = "linux"))]
    let _fault = inject_lexical_publish_rename_failure_once(
        LexicalPublishRenameSite::NonLinuxPublishStagedLive,
        ENOSPC_RAW_OS_ERROR,
    );

    let error = run_index(opts, None)
        .expect_err("injected staged publication failure must fail the index command");
    assert!(
        format!("{error:#}").contains("rolled back")
            || format!("{error:#}").contains("publishing staged lexical index"),
        "failure should retain staged-publication context: {error:#}"
    );
    assert_eq!(
        crate::search::tantivy::searchable_index_summary(&index_path)
            .unwrap()
            .unwrap()
            .docs,
        1,
        "failed authoritative publication must preserve the prior searchable generation"
    );
    assert!(
        !index_path.with_extension("bak").exists(),
        "run_index must not strand the published generation in the obsolete eager .bak path"
    );
}

#[test]
#[serial]
fn publish_staged_lexical_index_moves_generation_audit_files_with_the_staged_directory() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let index_path = index_dir(&data_dir).unwrap();

    let old_conv = norm_conv(Some("audit-old"), vec![norm_msg(0, 1_700_000_030_000)]);
    let mut live_index = TantivyIndex::open_or_create(&index_path).unwrap();
    live_index
        .add_messages_with_conversation_id(&old_conv, &old_conv.messages, Some(30))
        .unwrap();
    live_index.commit().unwrap();
    drop(live_index);

    let old_evidence = crate::indexer::LexicalRebuildEquivalenceEvidence {
        document_count: 1,
        manifest_fingerprint: "old-manifest-fingerprint".to_string(),
        golden_query_digest: "old-golden-digest".to_string(),
        golden_query_hit_counts: vec![crate::indexer::LexicalRebuildEquivalenceGoldenHit {
            probe: "audit-old".to_string(),
            hit_count: 1,
        }],
    };
    let old_manifest = persist_lexical_rebuild_generation_artifacts(
        &index_path,
        "content-v1:old-fingerprint",
        1,
        1,
        1,
        1,
        &old_evidence,
    )
    .unwrap();

    let stage_root = TempDirBuilder::new()
        .prefix("cass-test-publish-audit-stage.")
        .tempdir_in(index_path.parent().unwrap())
        .unwrap();
    let staged_index_path = stage_root.path().join("staged");
    let new_conv = norm_conv(
        Some("audit-new"),
        vec![
            norm_msg(0, 1_700_000_040_000),
            norm_msg(1, 1_700_000_040_100),
        ],
    );
    let mut staged_index = TantivyIndex::open_or_create(&staged_index_path).unwrap();
    staged_index
        .add_messages_with_conversation_id(&new_conv, &new_conv.messages, Some(40))
        .unwrap();
    staged_index.commit().unwrap();
    drop(staged_index);

    let new_evidence = crate::indexer::LexicalRebuildEquivalenceEvidence {
        document_count: 2,
        manifest_fingerprint: "new-manifest-fingerprint".to_string(),
        golden_query_digest: "new-golden-digest".to_string(),
        golden_query_hit_counts: vec![crate::indexer::LexicalRebuildEquivalenceGoldenHit {
            probe: "audit-new".to_string(),
            hit_count: 2,
        }],
    };
    let new_manifest = persist_lexical_rebuild_generation_artifacts(
        &staged_index_path,
        "content-v1:new-fingerprint",
        2,
        1,
        2,
        2,
        &new_evidence,
    )
    .unwrap();

    publish_staged_lexical_index(&staged_index_path, &index_path).unwrap();

    let live_manifest = lexical_generation::load_manifest(&index_path)
        .unwrap()
        .expect("live manifest after staged publish");
    assert_eq!(live_manifest.generation_id, new_manifest.generation_id);
    assert_eq!(
        live_manifest.equivalence_manifest_fingerprint.as_deref(),
        Some(new_evidence.manifest_fingerprint.as_str())
    );
    let live_evidence: crate::indexer::LexicalRebuildEquivalenceEvidence = serde_json::from_slice(
        &fs::read(lexical_rebuild_equivalence_evidence_path(&index_path)).unwrap(),
    )
    .unwrap();
    assert_eq!(live_evidence, new_evidence);

    let retained_backups = fs::read_dir(lexical_publish_backups_dir(&index_path))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    assert_eq!(retained_backups.len(), 1);
    let backup_manifest = lexical_generation::load_manifest(&retained_backups[0])
        .unwrap()
        .expect("retained backup manifest");
    assert_eq!(backup_manifest.generation_id, old_manifest.generation_id);
    assert_eq!(
        backup_manifest.equivalence_manifest_fingerprint.as_deref(),
        Some(old_evidence.manifest_fingerprint.as_str())
    );
    let backup_evidence: crate::indexer::LexicalRebuildEquivalenceEvidence =
        serde_json::from_slice(
            &fs::read(lexical_rebuild_equivalence_evidence_path(
                &retained_backups[0],
            ))
            .unwrap(),
        )
        .unwrap();
    assert_eq!(backup_evidence, old_evidence);
}

#[test]
#[serial]
fn publish_staged_lexical_index_recovers_interrupted_backup_before_replacing_live_index() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let index_path = index_dir(&data_dir).unwrap();

    let old_conv = norm_conv(
        Some("interrupted-old"),
        vec![
            norm_msg(0, 1_700_000_010_000),
            norm_msg(1, 1_700_000_010_100),
        ],
    );
    let mut live_index = TantivyIndex::open_or_create(&index_path).unwrap();
    live_index
        .add_messages_with_conversation_id(&old_conv, &old_conv.messages, Some(10))
        .unwrap();
    live_index.commit().unwrap();
    drop(live_index);

    let in_progress_backup_path = lexical_publish_in_progress_backup_path(&index_path);
    fs::rename(&index_path, &in_progress_backup_path).unwrap();
    assert!(
        !index_path.exists(),
        "the live path should be missing to simulate an interrupted publish after parking the old index"
    );

    let stage_root = TempDirBuilder::new()
        .prefix("cass-test-publish-recovery.")
        .tempdir_in(in_progress_backup_path.parent().unwrap())
        .unwrap();
    let staged_index_path = stage_root.path().join("staged");
    let new_conv = norm_conv(
        Some("interrupted-new"),
        vec![norm_msg(0, 1_700_000_020_000)],
    );
    let mut staged_index = TantivyIndex::open_or_create(&staged_index_path).unwrap();
    staged_index
        .add_messages_with_conversation_id(&new_conv, &new_conv.messages, Some(11))
        .unwrap();
    staged_index.commit().unwrap();
    drop(staged_index);

    publish_staged_lexical_index(&staged_index_path, &index_path).unwrap();

    assert!(
        !in_progress_backup_path.exists(),
        "successful publish should consume the interrupted in-progress backup"
    );
    assert_eq!(
        crate::search::tantivy::searchable_index_summary(&index_path)
            .unwrap()
            .unwrap()
            .docs,
        1,
        "the new staged publish should become live after interrupted-backup recovery"
    );

    let retained_backups = fs::read_dir(lexical_publish_backups_dir(&index_path))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    assert_eq!(retained_backups.len(), 1);
    assert_eq!(
        crate::search::tantivy::searchable_index_summary(&retained_backups[0])
            .unwrap()
            .unwrap()
            .docs,
        2,
        "the retained backup should still preserve the old live publish recovered from the interrupted swap"
    );
}

/// First-ever publish when no live index exists yet. The atomic-swap
/// path at src/indexer/mod.rs:~10720 short-circuits to a plain rename
/// in that case; this test pins that contract so a future refactor
/// doesn't accidentally introduce a renameat2 call on a missing live
/// path (EEXIST / ENOENT depending on flags).
#[test]
fn publish_staged_lexical_index_first_publish_with_no_prior_live_index_uses_plain_rename() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let index_path = index_dir(&data_dir).unwrap();
    // `index_dir` calls std::fs::create_dir_all, so we have to remove
    // the freshly-created empty directory to simulate the truly
    // pristine "no prior live index" state that the fast-path branch
    // in publish_staged_lexical_index is written to handle.
    fs::remove_dir_all(&index_path).unwrap();
    assert!(!index_path.exists(), "precondition: no prior live index");

    let stage_root = TempDirBuilder::new()
        .prefix("cass-test-first-publish-stage.")
        .tempdir_in(data_dir.parent().unwrap())
        .unwrap();
    let staged_index_path = stage_root.path().join("staged");
    let conv = norm_conv(Some("first-publish"), vec![norm_msg(0, 1_700_000_100_000)]);
    let mut staged_index = TantivyIndex::open_or_create(&staged_index_path).unwrap();
    staged_index
        .add_messages_with_conversation_id(&conv, &conv.messages, Some(1))
        .unwrap();
    staged_index.commit().unwrap();
    drop(staged_index);

    publish_staged_lexical_index(&staged_index_path, &index_path).unwrap();

    assert_eq!(
        crate::search::tantivy::searchable_index_summary(&index_path)
            .unwrap()
            .unwrap()
            .docs,
        1,
        "first publish must land the staged index exactly"
    );
    assert!(
        !staged_index_path.exists(),
        "staged path must be consumed by the rename"
    );
    // No retained backup is legitimate: there was nothing to retain.
    // The backups dir is ensured only on the prior-live branch, so it
    // may or may not exist — just confirm that if it exists, it's empty.
    let backups_dir = lexical_publish_backups_dir(&index_path);
    if backups_dir.exists() {
        let entries: Vec<_> = fs::read_dir(&backups_dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        assert!(
            entries.is_empty(),
            "first publish must not create retained backups; got {entries:?}"
        );
    }
}

#[test]
fn publish_staged_lexical_index_first_publish_over_empty_index_dir_does_not_retain_empty_backup() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    // `index_dir(&data_dir)` creates the empty `index/v9-quill` directory at
    // startup before any lexical generation is published (dcp01).
    let index_path = index_dir(&data_dir).unwrap();
    assert!(
        index_path.is_dir(),
        "precondition: empty index_dir placeholder exists"
    );

    let stage_root = TempDirBuilder::new()
        .prefix("cass-test-first-publish-empty-dir-stage.")
        .tempdir_in(data_dir.parent().unwrap())
        .unwrap();
    let staged_index_path = stage_root.path().join("staged");
    let conv = norm_conv(
        Some("first-publish-empty-dir"),
        vec![norm_msg(0, 1_700_000_100_000)],
    );
    let mut staged_index = TantivyIndex::open_or_create(&staged_index_path).unwrap();
    staged_index
        .add_messages_with_conversation_id(&conv, &conv.messages, Some(1))
        .unwrap();
    staged_index.commit().unwrap();
    drop(staged_index);

    publish_staged_lexical_index(&staged_index_path, &index_path).unwrap();

    assert_eq!(
        crate::search::tantivy::searchable_index_summary(&index_path)
            .unwrap()
            .unwrap()
            .docs,
        1,
        "first publish over empty index_dir placeholder must land the staged index"
    );
    assert!(
        !staged_index_path.exists(),
        "staged path must be consumed by the rename"
    );
    let backups_dir = lexical_publish_backups_dir(&index_path);
    if backups_dir.exists() {
        let entries: Vec<_> = fs::read_dir(&backups_dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        assert!(
            entries.is_empty(),
            "first publish over empty index_dir placeholder must not retain an empty backup; got {entries:?}"
        );
    }
}

#[test]
fn publish_staged_lexical_index_first_publish_over_checkpoint_only_index_dir_does_not_retain_it() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    // A first full rebuild checkpoints into the startup placeholder, and
    // status may cache the archive fingerprint there, before any generation
    // exists. `cass index --force-rebuild` on a fresh data dir retained that
    // directory as a "prior-live" publish backup holding no index.
    let index_path = index_dir(&data_dir).unwrap();
    fs::write(
        lexical_rebuild_state_path(&index_path),
        b"{\"completed\":false}",
    )
    .unwrap();
    fs::write(index_path.join(ARCHIVE_FINGERPRINT_CACHE_FILE), b"{}").unwrap();

    let stage_root = TempDirBuilder::new()
        .prefix("cass-test-first-publish-checkpoint-dir-stage.")
        .tempdir_in(data_dir.parent().unwrap())
        .unwrap();
    let staged_index_path = stage_root.path().join("staged");
    let conv = norm_conv(
        Some("first-publish-checkpoint-dir"),
        vec![norm_msg(0, 1_700_000_100_000)],
    );
    let mut staged_index = TantivyIndex::open_or_create(&staged_index_path).unwrap();
    staged_index
        .add_messages_with_conversation_id(&conv, &conv.messages, Some(1))
        .unwrap();
    staged_index.commit().unwrap();
    drop(staged_index);

    publish_staged_lexical_index(&staged_index_path, &index_path).unwrap();

    assert_eq!(
        crate::search::tantivy::searchable_index_summary(&index_path)
            .unwrap()
            .unwrap()
            .docs,
        1,
        "first publish over a checkpoint-only placeholder must land the staged index"
    );
    assert!(
        !lexical_rebuild_state_path(&index_path).exists(),
        "the superseded in-progress checkpoint must not survive into the published generation"
    );
    assert!(
        !lexical_publish_in_progress_backup_path(&index_path).exists(),
        "first publish must not park the placeholder"
    );
    let backups_dir = lexical_publish_backups_dir(&index_path);
    if backups_dir.exists() {
        let entries: Vec<_> = fs::read_dir(&backups_dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        assert!(
            entries.is_empty(),
            "a checkpoint-only placeholder is not a prior-live generation; got {entries:?}"
        );
    }
}

#[test]
fn publish_staged_lexical_index_retains_a_checkpointed_index_dir_holding_anything_else() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let index_path = index_dir(&data_dir).unwrap();
    fs::write(
        lexical_rebuild_state_path(&index_path),
        b"{\"completed\":false}",
    )
    .unwrap();
    // Any entry beyond the two placeholder sidecars means the directory is
    // not a placeholder, so publish must retain it untouched.
    fs::write(
        index_path.join("segment-unknown"),
        b"not a placeholder sidecar",
    )
    .unwrap();

    let stage_root = TempDirBuilder::new()
        .prefix("cass-test-publish-checkpoint-plus-dir-stage.")
        .tempdir_in(data_dir.parent().unwrap())
        .unwrap();
    let staged_index_path = stage_root.path().join("staged");
    let conv = norm_conv(
        Some("publish-checkpoint-plus-dir"),
        vec![norm_msg(0, 1_700_000_100_000)],
    );
    let mut staged_index = TantivyIndex::open_or_create(&staged_index_path).unwrap();
    staged_index
        .add_messages_with_conversation_id(&conv, &conv.messages, Some(1))
        .unwrap();
    staged_index.commit().unwrap();
    drop(staged_index);

    publish_staged_lexical_index(&staged_index_path, &index_path).unwrap();

    assert_eq!(
        crate::search::tantivy::searchable_index_summary(&index_path)
            .unwrap()
            .unwrap()
            .docs,
        1,
        "publish must land the staged index"
    );
    let entries: Vec<_> = fs::read_dir(lexical_publish_backups_dir(&index_path))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(
        entries.len(),
        1,
        "the prior live directory must be retained; got {entries:?}"
    );
    assert_eq!(
        fs::read(entries[0].join("segment-unknown")).unwrap(),
        b"not a placeholder sidecar"
    );
    assert_eq!(
        fs::read(entries[0].join(".lexical-rebuild-state.json")).unwrap(),
        b"{\"completed\":false}",
        "a retained directory keeps its checkpoint"
    );
}
