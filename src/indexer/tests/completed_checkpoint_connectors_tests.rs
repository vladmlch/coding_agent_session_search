use super::super::*;
use super::helpers::*;
use crate::connectors::{NormalizedConversation, ScanContext};
use crate::franken_sync::compat::{ConnectionExt, ParamValue, RowExt};
use serial_test::serial;
use tempfile::TempDir;

/// GH #435: a corpus containing hard-noise messages (empty content /
/// tool-acks the lexical sink drops via `is_hard_message_noise`) has a
/// HEALTHY live index with fewer docs than `COUNT(*) FROM messages`.
/// The checkpoint refresh must gate on the noise-adjusted
/// `expected_live_lexical_doc_count` (#317) rather than the raw message
/// count — otherwise the #353 metadata-only refresh skips forever and the
/// stale fingerprint drives a full `--force-rebuild` on every cycle.
#[test]
#[serial]
fn gh435_refresh_advances_stale_fingerprint_despite_hard_noise_messages() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("agent_search.db");
    let index_path = index_dir(&data_dir).unwrap();
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);

    // Two searchable messages plus one hard-noise (empty-content) message
    // the lexical sink intentionally drops.
    let mut noise = norm_msg(2, 1_700_000_000_200);
    noise.content = String::new();
    let convs = vec![norm_conv(
        Some("c1"),
        vec![
            norm_msg(0, 1_700_000_000_000),
            norm_msg(1, 1_700_000_000_100),
            noise,
        ],
    )];
    let mut index = TantivyIndex::open_or_create(&index_path).unwrap();
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

    let total_messages = count_total_messages_exact(&storage).unwrap();
    assert_eq!(total_messages, 3, "canonical DB must hold all 3 messages");
    assert_eq!(
        live_tantivy_doc_count(&index_path)
            .unwrap()
            .map(|docs| docs.saturating_sub(
                crate::viewer_extensions::search_regions::live_region_doc_bound(storage.raw())
                    .unwrap_or(0) as usize,
            )),
        Some(2),
        "the lexical sink must have dropped the hard-noise message \
         (Task 5: live docs also include the session's region documents; \
         subtract their persisted bound to compare the canonical share)"
    );
    assert_eq!(
        expected_live_lexical_doc_count(&storage).unwrap(),
        2,
        "the noise-adjusted expectation must match the live index"
    );

    let live_fingerprint = lexical_rebuild_storage_fingerprint(&db_path).unwrap();

    // Persist a completed checkpoint carrying a STALE fingerprint — the
    // state the #435 reporter observed after every incremental_inline run.
    let mut state = LexicalRebuildState::new(
        lexical_rebuild_db_state(&storage, &db_path).unwrap(),
        LEXICAL_REBUILD_PAGE_SIZE,
    );
    state.db.storage_fingerprint = "content-v1:0:0:0".to_string();
    state.mark_completed(index_meta_fingerprint(&index_path).unwrap());
    persist_lexical_rebuild_state(&index_path, &state).unwrap();

    refresh_completed_lexical_rebuild_checkpoint(&storage, &db_path, &data_dir).unwrap();

    let checkpoint = load_lexical_rebuild_checkpoint(&index_path)
        .unwrap()
        .expect("refreshed checkpoint");
    assert_eq!(
        checkpoint.storage_fingerprint, live_fingerprint,
        "GH #435: the checkpoint must advance when the live index equals the noise-adjusted expected count"
    );
    assert_eq!(
        checkpoint.indexed_docs - checkpoint.indexed_region_docs,
        2,
        "GH #435: the checkpoint must record the OBSERVED live doc count, not the raw message count \
         (Task 5: subtract the region-document share for the canonical count)"
    );
}

/// GH #436: a read-triggered lexical repair whose canonical DB cannot be
/// opened must not leave a dead zero-byte `index-run.lock` (or `.meta`
/// sidecar) behind. The fix probes DB openability before acquiring the
/// lock, so on this failure arm the lock artifacts are never created.
#[test]
#[serial]
fn gh436_failed_search_repair_leaves_no_index_run_lock_artifacts() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("agent_search.db");
    fs::write(&db_path, b"definitely not a sqlite database image").unwrap();

    let err = repair_lexical_index_from_canonical_db_for_search(&db_path, &data_dir, None)
        .expect_err("repair must fail on a malformed canonical database");
    assert!(
        format!("{err:#}").contains("opening database to repair lexical index for search"),
        "failure must come from the canonical DB open probe, got: {err:#}"
    );

    let lock_path = crate::search::asset_state::index_run_lock_path(&data_dir);
    assert!(
        !lock_path.exists(),
        "GH #436: a failed read-path repair must not leave index-run.lock behind"
    );
    let sidecar_path = crate::search::asset_state::index_run_lock_metadata_sidecar_path(&lock_path);
    assert!(
        !sidecar_path.exists(),
        "GH #436: a failed read-path repair must not leave index-run.lock.meta behind"
    );
    assert!(
        !archive_lease::ArchiveWriteLease::path(&db_path)
            .unwrap()
            .exists(),
        "an unopenable archive must not create an archive writer lease"
    );
}

#[test]
#[serial]
fn refresh_completed_lexical_rebuild_checkpoint_skips_rewriting_exact_completed_state() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("agent_search.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);

    let convs = vec![norm_conv(
        Some("exact-refresh"),
        vec![
            norm_msg(0, 1_700_000_000_000),
            norm_msg(1, 1_700_000_000_100),
        ],
    )];
    let mut index = TantivyIndex::open_or_create(&index_dir(&data_dir).unwrap()).unwrap();
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
    refresh_completed_lexical_rebuild_checkpoint(&storage, &db_path, &data_dir).unwrap();

    let state_path = lexical_rebuild_state_path(&index_path);
    let before_state = load_lexical_rebuild_state(&index_path)
        .unwrap()
        .expect("exact completed checkpoint");
    let before_modified = fs::metadata(&state_path).unwrap().modified().unwrap();

    std::thread::sleep(Duration::from_millis(5));
    refresh_completed_lexical_rebuild_checkpoint(&storage, &db_path, &data_dir).unwrap();

    let after_state = load_lexical_rebuild_state(&index_path)
        .unwrap()
        .expect("exact completed checkpoint should remain present");
    let after_modified = fs::metadata(&state_path).unwrap().modified().unwrap();

    assert_eq!(
        after_state, before_state,
        "refresh should not rewrite an already exact completed checkpoint"
    );
    assert_eq!(
        after_modified, before_modified,
        "refresh should not touch the checkpoint file when no fields changed"
    );
}

#[test]
#[serial]
fn refresh_completed_lexical_rebuild_checkpoint_skips_sparse_live_tantivy() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("agent_search.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);

    let conv_a = vec![norm_conv(
        Some("sparse-a"),
        vec![norm_msg(0, 1_700_000_000_000)],
    )];
    let conv_b = vec![norm_conv(
        Some("sparse-b"),
        vec![norm_msg(0, 1_700_000_001_000)],
    )];

    let mut canonical_index = TantivyIndex::open_or_create(&index_dir(&data_dir).unwrap()).unwrap();
    ingest_batch(
        &storage,
        Some(&mut canonical_index),
        &data_dir,
        &conv_a,
        &None,
        LexicalPopulationStrategy::IncrementalInline,
        false,
    )
    .unwrap();
    ingest_batch(
        &storage,
        Some(&mut canonical_index),
        &data_dir,
        &conv_b,
        &None,
        LexicalPopulationStrategy::IncrementalInline,
        false,
    )
    .unwrap();
    drop(canonical_index);

    let index_path = index_dir(&data_dir).unwrap();
    let sparse_backup = data_dir.join("index-full-backup");
    fs::rename(&index_path, &sparse_backup).unwrap();
    fs::create_dir_all(&index_path).unwrap();

    let mut sparse_index = TantivyIndex::open_or_create(&index_path).unwrap();
    sparse_index
        .add_messages_with_conversation_id(&conv_a[0], &conv_a[0].messages, Some(1))
        .unwrap();
    sparse_index.commit().unwrap();
    drop(sparse_index);

    refresh_completed_lexical_rebuild_checkpoint(&storage, &db_path, &data_dir).unwrap();

    assert!(
        load_lexical_rebuild_checkpoint(&index_path)
            .unwrap()
            .is_none(),
        "sparse live Tantivy should not bootstrap a completed lexical checkpoint"
    );
}

#[test]
fn final_checkpoint_refresh_uses_settled_storage_fingerprint() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("agent_search.db");
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
        Some("checkpoint-settled"),
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
    let total_conversations = count_total_conversations_exact(&storage).unwrap();
    let total_messages = count_total_messages_exact(&storage).unwrap();
    let mut state = LexicalRebuildState::new(
        lexical_rebuild_db_state(&storage, &db_path).unwrap(),
        LEXICAL_REBUILD_PAGE_SIZE,
    );
    state.mark_completed(index_meta_fingerprint(&index_path).unwrap());
    persist_lexical_rebuild_state(&index_path, &state).unwrap();

    refresh_completed_lexical_rebuild_checkpoint_for_final_state(
        &mut storage,
        &db_path,
        &data_dir,
        false,
        Some((total_conversations, total_messages)),
    )
    .unwrap();

    let checkpoint = load_lexical_rebuild_checkpoint(&index_path)
        .unwrap()
        .expect("refreshed checkpoint");
    assert!(checkpoint.completed);
    assert_eq!(checkpoint.total_conversations, total_conversations);
    assert_eq!(checkpoint.processed_conversations, total_conversations);
    assert_eq!(checkpoint.indexed_docs, total_messages);
    assert_eq!(
        checkpoint.storage_fingerprint,
        lexical_rebuild_storage_fingerprint(&db_path).unwrap()
    );
}

#[test]
fn matching_lexical_rebuild_state_status_recognizes_completed_checkpoint() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("agent_search.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);

    let db_state = lexical_rebuild_db_state(&storage, &db_path).unwrap();
    let index_path = index_dir(&data_dir).unwrap();
    let mut state = LexicalRebuildState::new(db_state.clone(), LEXICAL_REBUILD_PAGE_SIZE);
    state.mark_completed(Some("stable-meta".to_string()));
    persist_lexical_rebuild_state(&index_path, &state).unwrap();

    let status = matching_lexical_rebuild_state_status(&index_path, &db_state).unwrap();
    assert!(!status.has_pending_resume);
    assert!(status.has_completed_checkpoint);
    assert_eq!(status.completed_indexed_docs, Some(0));
    assert_eq!(status.completed_exact_totals, Some((0, 0)));
}

#[test]
fn matching_lexical_rebuild_state_status_if_present_skips_db_state_without_checkpoint() {
    let tmp = TempDir::new().unwrap();
    let index_path = tmp.path().join("index");
    fs::create_dir_all(&index_path).unwrap();
    let load_current_db_state_called = std::cell::Cell::new(false);

    let status = matching_lexical_rebuild_state_status_if_present(&index_path, || {
        load_current_db_state_called.set(true);
        Ok(LexicalRebuildDbState {
            db_path: "unused.db".to_string(),
            total_conversations: 1,
            total_messages: 0,
            storage_fingerprint: "content-v1:1:1:1".to_string(),
        })
    })
    .unwrap();

    assert_eq!(status, MatchingLexicalRebuildStateStatus::default());
    assert!(!load_current_db_state_called.get());
}

#[test]
fn completed_checkpoint_can_match_without_content_fingerprint() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("agent_search.db");
    fs::write(&db_path, b"db").unwrap();
    let index_path = tmp.path().join("index");
    fs::create_dir_all(&index_path).unwrap();

    let checkpoint_db_state = LexicalRebuildDbState {
        db_path: db_path.to_string_lossy().into_owned(),
        total_conversations: 7,
        total_messages: 0,
        storage_fingerprint: "content-v1:7:42:42".to_string(),
    };
    let mut state = LexicalRebuildState::new(checkpoint_db_state, LEXICAL_REBUILD_PAGE_SIZE);
    state.indexed_docs = 42;
    state.mark_completed(Some("meta-fingerprint".to_string()));
    persist_lexical_rebuild_state(&index_path, &state).unwrap();

    let status = matching_completed_lexical_rebuild_state_status_without_fingerprint(
        &index_path,
        &db_path,
        7,
    )
    .unwrap()
    .expect("completed checkpoints do not need an exact storage fingerprint");

    assert!(status.has_completed_checkpoint);
    assert_eq!(status.completed_indexed_docs, Some(42));
    assert_eq!(status.completed_exact_totals, Some((7, 42)));
    assert_eq!(
        status.completed_storage_fingerprint.as_deref(),
        Some("content-v1:7:42:42")
    );

    // Task 5: persisted search-region documents share the completed
    // checkpoint's `indexed_docs`; the exact message total is the
    // canonical share only.
    state.indexed_region_docs = 9;
    persist_lexical_rebuild_state(&index_path, &state).unwrap();
    let region_status = matching_completed_lexical_rebuild_state_status_without_fingerprint(
        &index_path,
        &db_path,
        7,
    )
    .unwrap()
    .expect("completed checkpoint with region docs still matches");
    assert_eq!(region_status.completed_indexed_docs, Some(42));
    assert_eq!(region_status.completed_exact_totals, Some((7, 33)));
    state.indexed_region_docs = 0;

    state.execution_mode = None;
    persist_lexical_rebuild_state(&index_path, &state).unwrap();
    let legacy_status = matching_completed_lexical_rebuild_state_status_without_fingerprint(
        &index_path,
        &db_path,
        7,
    )
    .unwrap()
    .expect("completed legacy checkpoints retain shared-writer semantics");
    assert!(legacy_status.has_completed_checkpoint);
    assert_eq!(legacy_status.completed_indexed_docs, Some(42));
}

#[test]
fn incomplete_checkpoint_still_requires_content_fingerprint() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("agent_search.db");
    fs::write(&db_path, b"db").unwrap();
    let index_path = tmp.path().join("index");
    fs::create_dir_all(&index_path).unwrap();

    let checkpoint_db_state = LexicalRebuildDbState {
        db_path: db_path.to_string_lossy().into_owned(),
        total_conversations: 7,
        total_messages: 0,
        storage_fingerprint: "content-v1:7:42:42".to_string(),
    };
    let state = LexicalRebuildState::new(checkpoint_db_state, LEXICAL_REBUILD_PAGE_SIZE);
    persist_lexical_rebuild_state(&index_path, &state).unwrap();

    assert!(
        matching_completed_lexical_rebuild_state_status_without_fingerprint(
            &index_path,
            &db_path,
            7,
        )
        .unwrap()
        .is_none(),
        "incomplete shared-writer checkpoints must keep the exact-fingerprint resume path"
    );
}

#[test]
fn nonresumable_staged_checkpoint_can_restart_without_current_fingerprint() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("agent_search.db");
    fs::write(&db_path, b"db").unwrap();
    let index_path = tmp.path().join("index");
    fs::create_dir_all(&index_path).unwrap();

    let checkpoint_db_state = LexicalRebuildDbState {
        db_path: db_path.to_string_lossy().into_owned(),
        total_conversations: 400,
        total_messages: 0,
        storage_fingerprint: "content-v1:400:1200:4800".to_string(),
    };
    let mut state =
        LexicalRebuildState::new(checkpoint_db_state.clone(), LEXICAL_REBUILD_PAGE_SIZE);
    state.set_execution_mode(LexicalRebuildExecutionMode::StagedShardBuild);
    persist_lexical_rebuild_state(&index_path, &state).unwrap();

    let status =
        nonresumable_pending_lexical_rebuild_status_without_fingerprint(&index_path, &db_path, 400)
            .unwrap()
            .expect("staged checkpoint should route to restart without exact fingerprint");
    assert!(status.has_pending_resume);
    assert!(!status.has_completed_checkpoint);

    let shared_index_path = tmp.path().join("shared-index");
    fs::create_dir_all(&shared_index_path).unwrap();
    let shared_state = LexicalRebuildState::new(checkpoint_db_state, LEXICAL_REBUILD_PAGE_SIZE);
    persist_lexical_rebuild_state(&shared_index_path, &shared_state).unwrap();
    assert!(
        nonresumable_pending_lexical_rebuild_status_without_fingerprint(
            &shared_index_path,
            &db_path,
            400,
        )
        .unwrap()
        .is_none(),
        "shared-writer checkpoints still require the exact fingerprint path"
    );

    let legacy_index_path = tmp.path().join("legacy-index");
    fs::create_dir_all(&legacy_index_path).unwrap();
    let mut legacy_state = LexicalRebuildState::new(
        LexicalRebuildDbState {
            db_path: db_path.to_string_lossy().into_owned(),
            total_conversations: 400,
            total_messages: 0,
            storage_fingerprint: "content-v1:400:1200:4800".to_string(),
        },
        LEXICAL_REBUILD_PAGE_SIZE,
    );
    legacy_state.execution_mode = None;
    persist_lexical_rebuild_state(&legacy_index_path, &legacy_state).unwrap();
    assert!(
        nonresumable_pending_lexical_rebuild_status_without_fingerprint(
            &legacy_index_path,
            &db_path,
            400,
        )
        .unwrap()
        .is_some(),
        "an incomplete checkpoint that predates execution-mode recording must restart without an exact archive count"
    );
}

#[test]
fn readonly_db_probe_classifies_nonresumable_staged_checkpoint() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("agent_search.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    assert_eq!(count_total_conversations_exact(&storage).unwrap(), 0);
    storage.close().unwrap();

    let index_path = tmp.path().join("index");
    fs::create_dir_all(&index_path).unwrap();
    let checkpoint_db_state = LexicalRebuildDbState {
        db_path: db_path.to_string_lossy().into_owned(),
        total_conversations: 0,
        total_messages: 0,
        storage_fingerprint: "content-v1:0:0:0".to_string(),
    };
    let mut state = LexicalRebuildState::new(checkpoint_db_state, LEXICAL_REBUILD_PAGE_SIZE);
    state.set_execution_mode(LexicalRebuildExecutionMode::StagedShardBuild);
    persist_lexical_rebuild_state(&index_path, &state).unwrap();

    let (status, total_conversations) =
        nonresumable_pending_lexical_rebuild_status_from_readonly_db(&index_path, &db_path)
            .unwrap()
            .expect("readonly probe should classify matching staged checkpoint");
    assert!(status.has_pending_resume);
    assert_eq!(total_conversations, 0);
}

/// GH #413: checkpoint fields already prove that this staged rebuild
/// belongs to another canonical database. Reject it from the small JSON
/// sidecar before opening or exactly counting the current archive. The
/// current file is deliberately not SQLite: the assertion can return
/// `Ok(None)` only if no database read was attempted.
#[test]
fn readonly_nonresumable_probe_rejects_mismatched_path_before_db_open() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("agent_search.db");
    fs::write(&db_path, b"not a sqlite database").unwrap();

    let index_path = tmp.path().join("index");
    fs::create_dir_all(&index_path).unwrap();
    let checkpoint_db_state = LexicalRebuildDbState {
        db_path: tmp
            .path()
            .join("different-agent-search.db")
            .to_string_lossy()
            .into_owned(),
        total_conversations: 400,
        total_messages: 0,
        storage_fingerprint: "content-v1:400:1200:4800".to_string(),
    };
    let mut state = LexicalRebuildState::new(checkpoint_db_state, LEXICAL_REBUILD_PAGE_SIZE);
    state.set_execution_mode(LexicalRebuildExecutionMode::StagedShardBuild);
    persist_lexical_rebuild_state(&index_path, &state).unwrap();

    assert!(
        nonresumable_pending_lexical_rebuild_status_from_readonly_db(&index_path, &db_path)
            .unwrap()
            .is_none(),
        "a path-mismatched checkpoint must be rejected before opening the canonical DB"
    );
}

/// GH #413: plain `cass index` must turn an interrupted non-resumable
/// checkpoint into useful Quill work without first hydrating the archive
/// for COUNT(*). The deliberately stale zero count proves that the sidecar
/// is only a startup hint; successful completion must publish all rows and
/// replace every derived count/fingerprint with streamed observations.
#[test]
#[serial]
fn plain_index_restarts_legacy_checkpoint_with_stale_count_to_quill_completion() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("agent_search.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    seed_lexical_rebuild_fixture(&storage);
    drop(storage);

    let index_path = index_dir(&data_dir).unwrap();
    fs::create_dir_all(&index_path).unwrap();
    let checkpoint_db_state = LexicalRebuildDbState {
        db_path: db_path.to_string_lossy().into_owned(),
        total_conversations: 0,
        total_messages: 0,
        storage_fingerprint: lexical_rebuild_deferred_content_fingerprint(0),
    };
    let mut state = LexicalRebuildState::new(checkpoint_db_state, LEXICAL_REBUILD_PAGE_SIZE);
    state.execution_mode = None;
    persist_lexical_rebuild_state(&index_path, &state).unwrap();

    run_index(
        IndexOptions {
            full: false,
            force_rebuild: false,
            watch: false,
            watch_once_paths: None,
            db_path: db_path.clone(),
            data_dir: data_dir.clone(),
            semantic: false,
            build_hnsw: false,
            embedder: "hash".to_string(),
            progress: None,
            watch_interval_secs: 30,
        },
        None,
    )
    .unwrap();

    assert_eq!(tantivy_doc_count_for_data_dir(&data_dir), 4);
    let completed = load_lexical_rebuild_checkpoint(&index_path)
        .unwrap()
        .expect("completed checkpoint after count-free plain-index restart");
    assert!(completed.completed);
    assert_eq!(completed.total_conversations, 2);
    assert_eq!(completed.processed_conversations, 2);
    assert_eq!(completed.committed_offset, 2);
    assert_eq!(completed.indexed_docs, 4);
    assert_eq!(
        completed.storage_fingerprint,
        lexical_rebuild_storage_fingerprint(&db_path).unwrap()
    );
}

/// A source scan replaces progress totals even when it skips every source.
/// Resume-only and post-scan repairs must retain their exact-count shortcut.
#[test]
fn redrive_final_checkpoint_refresh_after_any_scan_following_a_pre_scan_repair() {
    let unchanged = CanonicalMutationCounts::default();
    let revised_messages = CanonicalMutationCounts {
        inserted_conversations: 0,
        inserted_messages: 0,
        updated_messages: 1,
    };

    assert!(revised_messages.changed());
    assert_eq!(unchanged.accumulate(revised_messages), revised_messages);
    assert!(should_redrive_final_lexical_checkpoint_refresh_after_pre_scan_repair(true, true));
    assert!(!should_redrive_final_lexical_checkpoint_refresh_after_pre_scan_repair(true, false));
    assert!(!should_redrive_final_lexical_checkpoint_refresh_after_pre_scan_repair(false, true));
    assert!(!should_redrive_final_lexical_checkpoint_refresh_after_pre_scan_repair(false, false));
}

/// GH #457 follow-on regression: the pre-scan authoritative repair rebuilds
/// the lexical index from SQLite and persists an EXACT completed
/// checkpoint, and then the same run continues into the incremental source
/// scan. The end-of-run refresh used to be skipped outright ("the
/// authoritative rebuild already persisted exact completed state"), so a
/// scan that ingested a new session left the checkpoint carrying the
/// PRE-scan `COUNT`/`MAX(id)` fingerprint — search's fingerprint check
/// ("storage fingerprint no longer matches") and `cass status` then
/// reported the lexical assets stale until some LATER run happened to
/// rewrite the checkpoint.
#[test]
#[serial]
fn pre_scan_sparse_repair_refreshes_the_checkpoint_fingerprint_within_the_same_run() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("agent_search.db");
    let codex_home = tmp.path().join(".codex");

    let write_session = |name: &str, text: &str| {
        let sessions = codex_home.join("sessions/2026/09/09");
        fs::create_dir_all(&sessions).unwrap();
        let now_ms = FrankenStorage::now_millis();
        let stamp = |offset: i64| {
            chrono::DateTime::from_timestamp_millis(now_ms + offset)
                .unwrap_or_else(chrono::Utc::now)
                .to_rfc3339()
        };
        let lines = [
            serde_json::json!({
                "timestamp": stamp(0),
                "type": "session_meta",
                "payload": { "id": name, "cwd": "/data/projects/cass", "cli_version": "0.42.0" }
            }),
            serde_json::json!({
                "timestamp": stamp(1_000),
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "role": "user",
                    "content": [{ "type": "input_text", "text": text }]
                }
            }),
            serde_json::json!({
                "timestamp": stamp(2_000),
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "role": "assistant",
                    "content": [{ "type": "text", "text": format!("{text} response") }]
                }
            }),
        ];
        let mut body = String::new();
        for line in lines {
            body.push_str(&serde_json::to_string(&line).unwrap());
            body.push('\n');
        }
        fs::write(sessions.join(format!("rollout-{name}.jsonl")), body).unwrap();
    };
    write_session("sparse-seed-one", "sparserepair seed one");
    write_session("sparse-seed-two", "sparserepair seed two");

    let opts = || IndexOptions {
        full: false,
        force_rebuild: false,
        watch: false,
        watch_once_paths: None,
        db_path: db_path.clone(),
        data_dir: data_dir.clone(),
        semantic: false,
        build_hnsw: false,
        embedder: "hash".to_string(),
        // A real progress handle is load-bearing here, not decoration: the
        // authoritative rebuild records its exact canonical row counts
        // THROUGH progress, and the end-of-run refresh decision reads them
        // back from it. With `progress: None` those counts are dropped and
        // the skip this test is about can never be reached, so the fixture
        // would pass whether or not the fix is present. Every `cass index`
        // invocation supplies one.
        progress: Some(Arc::new(IndexingProgress::default())),
        watch_interval_secs: 30,
    };
    let roots = || HashMap::from([("codex".to_string(), vec![codex_home.clone()])]);

    run_index_with_local_connector_roots(opts(), roots(), None).unwrap();

    let index_path = index_dir(&data_dir).unwrap();
    let baseline = load_lexical_rebuild_checkpoint(&index_path)
        .unwrap()
        .expect("baseline completed checkpoint");
    assert!(baseline.completed);
    assert_eq!(
        baseline.total_conversations, 2,
        "the baseline run must ingest both seed sessions: {baseline:?}"
    );
    assert_eq!(
        baseline.storage_fingerprint,
        lexical_rebuild_storage_fingerprint(&db_path).unwrap(),
        "the baseline run must leave a checkpoint matching its own database"
    );

    // Hollow the live generation out from under that still-completed
    // checkpoint. The replacement is a VALID but empty index, which is what
    // makes the planner take the sparse branch rather than the
    // missing/invalid one; the populated generation is renamed aside rather
    // than removed so a failure stays inspectable.
    let sparse_backup = data_dir.join("index-before-sparse-repair");
    fs::rename(&index_path, &sparse_backup).unwrap();
    fs::create_dir_all(&index_path).unwrap();
    let mut hollow = TantivyIndex::open_or_create(&index_path).unwrap();
    hollow.commit().unwrap();
    drop(hollow);
    fs::copy(
        lexical_rebuild_state_path(&sparse_backup),
        lexical_rebuild_state_path(&index_path),
    )
    .unwrap();

    // ... and give the repair's follow-up scan real canonical work to do.
    write_session("sparse-post-repair", "sparserepair follow up");

    run_index_with_local_connector_roots(opts(), roots(), None).unwrap();

    let after = load_lexical_rebuild_checkpoint(&index_path)
        .unwrap()
        .expect("completed checkpoint after the repair run");
    assert!(after.completed);
    assert_eq!(
        after.total_conversations, 3,
        "the follow-up scan must have ingested the new session: {after:?}"
    );
    assert_eq!(
        after.storage_fingerprint,
        lexical_rebuild_storage_fingerprint(&db_path).unwrap(),
        "the pre-scan repair's checkpoint predates its own follow-up scan, so the run must refresh it before exiting"
    );
}

#[test]
fn skip_noop_final_checkpoint_refresh_requires_matching_completed_checkpoint_and_no_mutations() {
    let exact_counts = None;
    let no_mutations = CanonicalMutationCounts::default();
    let changed = CanonicalMutationCounts {
        inserted_conversations: 0,
        inserted_messages: 1,
        updated_messages: 0,
    };

    assert!(should_skip_noop_final_lexical_checkpoint_refresh(
        false,
        false,
        &MatchingLexicalRebuildStateStatus {
            has_pending_resume: false,
            has_completed_checkpoint: true,
            completed_indexed_docs: Some(0),
            completed_exact_totals: Some((0, 0)),
            completed_storage_fingerprint: Some("content-v1:0:0:0".to_string()),
        },
        exact_counts,
        no_mutations,
        true,
    ));
    assert!(!should_skip_noop_final_lexical_checkpoint_refresh(
        true,
        false,
        &MatchingLexicalRebuildStateStatus {
            has_pending_resume: false,
            has_completed_checkpoint: true,
            completed_indexed_docs: Some(0),
            completed_exact_totals: Some((0, 0)),
            completed_storage_fingerprint: Some("content-v1:0:0:0".to_string()),
        },
        exact_counts,
        no_mutations,
        true,
    ));
    assert!(!should_skip_noop_final_lexical_checkpoint_refresh(
        false,
        false,
        &MatchingLexicalRebuildStateStatus {
            has_pending_resume: false,
            has_completed_checkpoint: false,
            completed_indexed_docs: None,
            completed_exact_totals: None,
            completed_storage_fingerprint: None,
        },
        exact_counts,
        no_mutations,
        true,
    ));
    assert!(!should_skip_noop_final_lexical_checkpoint_refresh(
        false,
        false,
        &MatchingLexicalRebuildStateStatus {
            has_pending_resume: false,
            has_completed_checkpoint: true,
            completed_indexed_docs: Some(0),
            completed_exact_totals: Some((0, 0)),
            completed_storage_fingerprint: Some("content-v1:0:0:0".to_string()),
        },
        Some((1, 2)),
        no_mutations,
        true,
    ));
    assert!(!should_skip_noop_final_lexical_checkpoint_refresh(
        false,
        false,
        &MatchingLexicalRebuildStateStatus {
            has_pending_resume: false,
            has_completed_checkpoint: true,
            completed_indexed_docs: Some(0),
            completed_exact_totals: Some((0, 0)),
            completed_storage_fingerprint: Some("content-v1:0:0:0".to_string()),
        },
        exact_counts,
        changed,
        true,
    ));
    assert!(!should_skip_noop_final_lexical_checkpoint_refresh(
        false,
        true,
        &MatchingLexicalRebuildStateStatus {
            has_pending_resume: false,
            has_completed_checkpoint: true,
            completed_indexed_docs: Some(0),
            completed_exact_totals: Some((0, 0)),
            completed_storage_fingerprint: Some("content-v1:0:0:0".to_string()),
        },
        exact_counts,
        no_mutations,
        true,
    ));
    // #289: a run-start completed checkpoint is not enough — if the
    // checkpoint file vanished mid-run (e.g. the federated-bundle
    // materialization replaced the live directory), the final refresh
    // must run so the checkpoint is re-persisted.
    assert!(!should_skip_noop_final_lexical_checkpoint_refresh(
        false,
        false,
        &MatchingLexicalRebuildStateStatus {
            has_pending_resume: false,
            has_completed_checkpoint: true,
            completed_indexed_docs: Some(0),
            completed_exact_totals: Some((0, 0)),
            completed_storage_fingerprint: Some("content-v1:0:0:0".to_string()),
        },
        exact_counts,
        no_mutations,
        false,
    ));
}

#[test]
fn preserve_matching_completed_lexical_checkpoint_during_full_scan_requires_plain_full_scan_with_checkpoint()
 {
    let completed = MatchingLexicalRebuildStateStatus {
        has_pending_resume: false,
        has_completed_checkpoint: true,
        completed_indexed_docs: Some(42),
        completed_exact_totals: Some((7, 42)),
        completed_storage_fingerprint: Some("content-v1:7:42:42".to_string()),
    };

    assert!(
        should_preserve_matching_completed_lexical_checkpoint_during_full_scan(
            true, false, false, &completed
        )
    );
    assert!(
        !should_preserve_matching_completed_lexical_checkpoint_during_full_scan(
            false, false, false, &completed
        )
    );
    assert!(
        !should_preserve_matching_completed_lexical_checkpoint_during_full_scan(
            true, true, false, &completed
        )
    );
    assert!(
        !should_preserve_matching_completed_lexical_checkpoint_during_full_scan(
            true, false, true, &completed
        )
    );
    assert!(
        !should_preserve_matching_completed_lexical_checkpoint_during_full_scan(
            true,
            false,
            false,
            &MatchingLexicalRebuildStateStatus::default(),
        )
    );
}

#[test]
fn preflight_existing_tantivy_reader_is_skipped_for_resume_or_full_rebuilds() {
    assert!(should_preflight_existing_tantivy_reader(false, false));
    assert!(!should_preflight_existing_tantivy_reader(true, false));
    assert!(!should_preflight_existing_tantivy_reader(false, true));
}

#[test]
fn live_tantivy_doc_probe_for_post_full_skip_only_runs_for_noop_full_candidates() {
    let checkpoint = MatchingLexicalRebuildStateStatus {
        has_pending_resume: false,
        has_completed_checkpoint: true,
        completed_indexed_docs: Some(42),
        completed_exact_totals: Some((7, 42)),
        completed_storage_fingerprint: Some("content-v1:7:42:42".to_string()),
    };

    assert!(should_probe_live_tantivy_docs_for_post_full_scan_skip(
        true,
        false,
        0,
        &checkpoint,
        CanonicalMutationCounts::default(),
        None,
    ));
    assert!(!should_probe_live_tantivy_docs_for_post_full_scan_skip(
        false,
        false,
        0,
        &checkpoint,
        CanonicalMutationCounts::default(),
        None,
    ));
    assert!(!should_probe_live_tantivy_docs_for_post_full_scan_skip(
        true,
        true,
        0,
        &checkpoint,
        CanonicalMutationCounts::default(),
        None,
    ));
    assert!(!should_probe_live_tantivy_docs_for_post_full_scan_skip(
        true,
        false,
        1,
        &checkpoint,
        CanonicalMutationCounts::default(),
        None,
    ));
    assert!(!should_probe_live_tantivy_docs_for_post_full_scan_skip(
        true,
        false,
        0,
        &checkpoint,
        CanonicalMutationCounts {
            inserted_conversations: 1,
            inserted_messages: 0,
            updated_messages: 0,
        },
        None,
    ));
    assert!(!should_probe_live_tantivy_docs_for_post_full_scan_skip(
        true,
        false,
        0,
        &MatchingLexicalRebuildStateStatus::default(),
        CanonicalMutationCounts::default(),
        None,
    ));
    assert!(!should_probe_live_tantivy_docs_for_post_full_scan_skip(
        true,
        false,
        0,
        &checkpoint,
        CanonicalMutationCounts::default(),
        Some(42),
    ));
}

#[test]
fn authoritative_rebuild_requirement_only_treats_canonical_storage_reset_as_rebuild() {
    assert!(!should_force_authoritative_rebuild(false, false));
    assert!(should_force_authoritative_rebuild(true, false));
    assert!(should_force_authoritative_rebuild(false, true));
}

#[test]
#[serial]
fn configured_connector_factories_skip_disabled_agents_from_sources_config() {
    let temp = TempDir::new().unwrap();
    let config_home = temp.path().join("xdg-config");
    fs::create_dir_all(config_home.join("cass")).unwrap();
    fs::write(
        config_home.join("cass").join("sources.toml"),
        "disabled_agents = [\"openclaw\"]\n",
    )
    .unwrap();

    let _config_home_guard = set_env_var("XDG_CONFIG_HOME", config_home.to_string_lossy());
    let _sources_guard = unset_env_var("CASS_IGNORE_SOURCES_CONFIG");

    let filtered = filter_disabled_connector_factories(vec![
        ("openclaw", never_constructed_connector_factory),
        ("codex", never_constructed_connector_factory),
    ]);
    let names = filtered
        .into_iter()
        .map(|(name, _)| name)
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["codex"]);
}

/// The cass registry mirrors the upstream one entry for entry: the three
/// cass adapters (codex, omp, pi_agent) replace the upstream factory, and
/// every other entry behaves like the upstream connector. The pass-through
/// half is asserted behaviorally — same detection result and streaming
/// capability from both factories — because function-pointer identity
/// (`fn_addr_eq`) is not guaranteed across codegen units and failed on two
/// otherwise-green fleet runs (bead zgzva).
#[test]
fn cass_connector_registry_installs_every_cass_adapter() {
    let upstream = franken_agent_detection::get_connector_factories();
    let configured = get_connector_factories();
    assert_eq!(configured.len(), upstream.len());

    for ((configured_name, configured_factory), (upstream_name, upstream_factory)) in
        configured.into_iter().zip(upstream)
    {
        assert_eq!(configured_name, upstream_name);
        let kind = ConnectorKind::from_slug(configured_name)
            .unwrap_or_else(|| panic!("{configured_name}: missing watch/quarantine route"));
        assert_eq!(kind.slug(), configured_name);
        let serialized = serde_json::to_string(&kind).expect("serialize connector kind");
        assert_eq!(
            serde_json::from_str::<ConnectorKind>(&serialized).expect("read connector kind"),
            kind
        );
        let routed = kind.create_connector(None);
        let expected = configured_factory();
        let routed_detection = routed.detect();
        let expected_detection = expected.detect();
        assert_eq!(
            (
                routed_detection.detected,
                routed_detection.root_paths,
                routed.supports_streaming_scan()
            ),
            (
                expected_detection.detected,
                expected_detection.root_paths,
                expected.supports_streaming_scan()
            ),
            "{configured_name}: watch/quarantine must use the configured connector"
        );
        // copilot: CASS widens detection to VS Code's native chat stores,
        // so its detect() legitimately differs where such stores exist.
        if matches!(configured_name, "codex" | "omp" | "pi_agent" | "copilot") {
            assert!(
                !std::ptr::fn_addr_eq(configured_factory, upstream_factory),
                "{configured_name}: cass must install its own adapter, not the upstream one"
            );
        } else {
            let configured_connector = configured_factory();
            let upstream_connector = upstream_factory();
            let configured_detection = configured_connector.detect();
            let upstream_detection = upstream_connector.detect();
            assert_eq!(
                (
                    configured_detection.detected,
                    &configured_detection.evidence,
                    &configured_detection.root_paths,
                    configured_connector.supports_streaming_scan(),
                ),
                (
                    upstream_detection.detected,
                    &upstream_detection.evidence,
                    &upstream_detection.root_paths,
                    upstream_connector.supports_streaming_scan(),
                ),
                "{configured_name}: the pass-through entry must behave like the upstream connector"
            );
        }
    }
}

#[test]
#[serial]
fn configured_codex_factory_indexes_searchable_modern_tool_calls_once() {
    let temp = TempDir::new().unwrap();
    let codex_home = temp.path().join(".codex");
    let sessions = codex_home.join("sessions/2026/05/08");
    fs::create_dir_all(&sessions).unwrap();
    fs::write(
        sessions.join("rollout-modern.jsonl"),
        r#"{"timestamp":"2026-05-08T23:09:00.000Z","type":"session_meta","payload":{"id":"modern-id","cwd":"/data/projects/ntm","cli_version":"0.49.0"}}
{"timestamp":"2026-05-08T23:09:01.000Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"investigate cass"}]}}
{"timestamp":"2026-05-08T23:09:02.000Z","type":"response_item","payload":{"type":"function_call","name":"exec_command","arguments":"{\"cmd\":\"git log --grep='cass339toolargsneedle'\"}","call_id":"call-modern-1"}}
"#,
    )
    .unwrap();

    let _codex_home_guard = set_env_var("CODEX_HOME", codex_home.to_string_lossy());
    let _sources_guard = set_env_var("CASS_IGNORE_SOURCES_CONFIG", "1");
    let factory = configured_connector_factories()
        .into_iter()
        .find_map(|(name, factory)| (name == "codex").then_some(factory))
        .expect("configured Codex factory");
    let data_dir = temp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let scan_context = ScanContext::local_default(data_dir.clone(), None);
    let upstream_conversations = franken_agent_detection::CodexConnector::new()
        .scan(&scan_context)
        .unwrap();
    let conversations = factory().scan(&scan_context).unwrap();
    let repeated_conversations = factory().scan(&scan_context).unwrap();

    assert_eq!(upstream_conversations.len(), 1);
    assert_eq!(conversations.len(), 1);
    assert_eq!(repeated_conversations.len(), 1);
    let upstream = &upstream_conversations[0];
    let conversation = &conversations[0];
    let repeated = &repeated_conversations[0];
    assert_eq!(conversation.agent_slug, upstream.agent_slug);
    assert_eq!(conversation.external_id, upstream.external_id);
    assert_eq!(conversation.title, upstream.title);
    assert_eq!(conversation.workspace, upstream.workspace);
    assert_eq!(conversation.source_path, upstream.source_path);
    assert_eq!(conversation.started_at, upstream.started_at);
    assert_eq!(conversation.ended_at, upstream.ended_at);
    assert_eq!(conversation.metadata, upstream.metadata);
    let stable_message_identity = |conversation: &NormalizedConversation| {
        conversation
            .messages
            .iter()
            .map(|message| {
                (
                    message.idx,
                    message.role.clone(),
                    message.author.clone(),
                    message.created_at,
                    message.extra.clone(),
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        stable_message_identity(conversation),
        stable_message_identity(upstream),
        "enrichment must preserve upstream message identity and order"
    );
    assert_eq!(
        stable_message_identity(repeated),
        stable_message_identity(conversation),
        "repeated enrichment scans must preserve message identity and order"
    );

    let tool_calls = conversations
        .iter()
        .flat_map(|conversation| &conversation.messages)
        .filter(|message| {
            message
                .invocations
                .iter()
                .any(|invocation| invocation.call_id.as_deref() == Some("call-modern-1"))
        })
        .collect::<Vec<_>>();
    assert_eq!(tool_calls.len(), 1, "tool call must remain deduplicated");
    let tool_call = tool_calls[0];
    assert_eq!(
        tool_call.invocations.len(),
        1,
        "the enriched tool-call message must contain exactly one invocation"
    );
    assert!(
        tool_call
            .content
            .contains("git log --grep='cass339toolargsneedle'")
    );
    let invocation = &tool_call.invocations[0];
    assert_eq!(invocation.name, "exec_command");
    assert_eq!(invocation.call_id.as_deref(), Some("call-modern-1"));
    assert_eq!(
        invocation
            .arguments
            .as_ref()
            .and_then(|arguments| arguments.get("cmd"))
            .and_then(serde_json::Value::as_str),
        Some("git log --grep='cass339toolargsneedle'")
    );
    let repeated_tool_calls = repeated
        .messages
        .iter()
        .filter(|message| {
            message
                .invocations
                .iter()
                .any(|invocation| invocation.call_id.as_deref() == Some("call-modern-1"))
        })
        .collect::<Vec<_>>();
    assert_eq!(repeated_tool_calls.len(), 1);
    assert_eq!(repeated_tool_calls[0].invocations.len(), 1);
    assert_eq!(repeated_tool_calls[0].content, tool_call.content);
    assert_eq!(repeated_tool_calls[0].invocations, tool_call.invocations);

    let db_path = data_dir.join("agent_search.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    let index_path = index_dir(&data_dir).unwrap();
    let mut lexical_index = TantivyIndex::open_or_create(&index_path).unwrap();
    let opts = IndexOptions {
        full: true,
        force_rebuild: false,
        watch: false,
        watch_once_paths: None,
        db_path,
        data_dir,
        semantic: false,
        build_hnsw: false,
        embedder: "fastembed".to_string(),
        progress: None,
        watch_interval_secs: 30,
    };
    let outcome = run_batch_index_with_connector_factories(
        &storage,
        Some(&mut lexical_index),
        &opts,
        None,
        LexicalPopulationStrategy::IncrementalInline,
        Vec::new(),
        None,
        vec![("codex", factory)],
        Some(FrankenStorage::now_millis()),
        None,
    )
    .unwrap();
    assert_eq!(outcome.canonical_mutations.inserted_conversations, 1);
    assert_eq!(
        outcome.canonical_mutations.inserted_messages,
        conversation.messages.len()
    );
    lexical_index.commit().unwrap();

    let persisted_messages = storage
        .raw()
        .query_map_collect(
            "SELECT idx, role, created_at, content FROM messages ORDER BY idx",
            &[] as &[ParamValue],
            |row| {
                Ok((
                    row.get_typed::<i64>(0)?,
                    row.get_typed::<String>(1)?,
                    row.get_typed::<Option<i64>>(2)?,
                    row.get_typed::<String>(3)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(persisted_messages.len(), conversation.messages.len());
    let persisted_tool_calls = persisted_messages
        .iter()
        .filter(|(_, _, _, content)| content.contains("git log --grep='cass339toolargsneedle'"))
        .collect::<Vec<_>>();
    assert_eq!(
        persisted_tool_calls.len(),
        1,
        "the production indexer must persist one enriched tool call"
    );
    assert_eq!(persisted_tool_calls[0].0, tool_call.idx);
    assert_eq!(
        persisted_tool_calls[0].1, "agent",
        "the indexer intentionally canonicalizes assistant roles to agent"
    );
    assert_eq!(persisted_tool_calls[0].2, tool_call.created_at);

    let reader = crate::search::quill_bridge::open_cass_reader(&index_path).unwrap();
    let parser = frankensearch::quill::query::CassQueryParser::new(
        frankensearch::quill::schema::CASS_SEMANTIC_SCHEMA,
    )
    .unwrap();
    let parsed = parser.parse(
        "cass339toolargsneedle",
        &frankensearch::quill::query::CassQueryFilters::default(),
    );
    // Task 5: the session's tool-region document also carries the
    // needle in its bounded `indexed_text`; count canonical messages.
    let canonical = crate::search::quill_bridge::canonical_message_query(parsed.query.clone());
    let lexical_matches =
        crate::search::quill_bridge::search_paginated(&reader, &canonical, 100, 0, true)
            .unwrap()
            .total_count
            .unwrap_or(0);
    assert_eq!(
        lexical_matches, 1,
        "tool-call arguments must reach the production lexical index"
    );
}
