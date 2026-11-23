use super::super::*;
use super::helpers::*;
use crate::connectors::{NormalizedConversation, NormalizedMessage};
use crate::franken_sync::compat::{ConnectionExt, ParamValue, RowExt};
use serial_test::serial;
use tempfile::TempDir;

/// GH #381 (ntxs1): an interrupted full expected-docs scan resumes from its
/// saved cursor instead of starting over, and still lands on exactly the
/// full count: unchanged, after pure appends (rows appended to an
/// already-counted conversation are added; conversations above the cursor
/// are read whole), and never after a deletion, which restarts the scan.
#[test]
fn gh381_interrupted_expected_docs_scan_resumes_exactly() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("gh381-resume.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    seed_lexical_rebuild_fixture(&storage);
    let db = db_path.to_string_lossy().into_owned();
    let partial_path = dir.path().join(EXPECTED_LEXICAL_DOCS_PARTIAL_FILE);
    // The generation directory whose parent holds the scan sidecars.
    let index_path = dir.path().join("index-generation");
    let ids: Vec<i64> = storage
        .raw()
        .query_map_collect(
            "SELECT id FROM conversations ORDER BY id",
            &[] as &[ParamValue],
            |row: &crate::franken_sync::Row| row.get_typed::<i64>(0),
        )
        .unwrap();
    assert_eq!(ids.len(), 2, "the fixture seeds two conversations");
    let (first, second) = (ids[0], ids[1]);
    let insert = |conversation_id: i64, idx: i64, role: &str, content: &str| {
        storage
            .raw()
            .execute_compat(
                "INSERT INTO messages(conversation_id, idx, role, content) VALUES(?1, ?2, ?3, ?4)",
                &[
                    ParamValue::from(conversation_id),
                    ParamValue::from(idx),
                    ParamValue::from(role.to_string()),
                    ParamValue::from(content.to_string()),
                ],
            )
            .unwrap();
    };
    let current = || {
        expected_lexical_docs_identity(&storage, count_total_messages_exact(&storage).unwrap())
            .unwrap()
    };
    let full = || expected_live_lexical_doc_count(&storage).unwrap();
    // Interrupt right after the first conversation, as a watchdog kill
    // would; the position was saved first (interval zero).
    let interrupted_scan = || {
        let result = expected_live_lexical_doc_count_resumable_with(
            &storage,
            Some(&partial_path),
            &db,
            &current(),
            None,
            Duration::ZERO,
            |_| Err(anyhow::anyhow!("interrupted")),
        );
        assert!(result.is_err());
        let partial: ExpectedLexicalDocsPartial =
            serde_json::from_slice(&fs::read(&partial_path).unwrap()).unwrap();
        assert_eq!(partial.last_conversation_id, first);
        // Status reads the same position, and only for this database.
        let position = expected_lexical_docs_scan_position(&index_path, &db_path)
            .expect("status sees the saved scan position");
        assert_eq!(
            (
                position.last_conversation_id,
                position.max_conversation_id,
                position.expected_docs_so_far
            ),
            (first, second, partial.expected_docs)
        );
        assert!(position.saved_at_ms.is_some());
        assert_eq!(
            expected_lexical_docs_scan_position(&index_path, &dir.path().join("other.db")),
            None,
            "another archive's position is not this one's"
        );
    };
    let finishing_scan = || {
        let mut visited = Vec::new();
        let count = expected_live_lexical_doc_count_resumable_with(
            &storage,
            Some(&partial_path),
            &db,
            &current(),
            None,
            Duration::ZERO,
            |conversation_id| {
                visited.push(conversation_id);
                Ok(())
            },
        )
        .unwrap();
        assert!(
            !partial_path.exists(),
            "a completed scan removes its partial"
        );
        assert_eq!(
            expected_lexical_docs_scan_position(&index_path, &db_path),
            None
        );
        (count, visited)
    };
    insert(first, 50, "user", "   ");

    // Unchanged archive: only the conversation after the cursor is read.
    interrupted_scan();
    assert_eq!(finishing_scan(), (full(), vec![second]));

    // Pure appends after the interruption: below the cursor (one indexed,
    // one noise) and above it. The resumed count still matches a full scan.
    interrupted_scan();
    insert(first, 60, "assistant", "appended below the cursor");
    insert(first, 61, "tool", "");
    insert(second, 60, "assistant", "appended above the cursor");
    assert_eq!(finishing_scan(), (full(), vec![second]));

    // A deletion below the cursor invalidates the partial: start over.
    interrupted_scan();
    storage
        .raw()
        .execute_compat(
            "DELETE FROM messages WHERE conversation_id = ?1 AND idx = 60",
            &[ParamValue::from(first)],
        )
        .unwrap();
    assert_eq!(finishing_scan(), (full(), vec![first, second]));
}

#[test]
fn gh515_stop_during_expected_docs_scan_saves_cursor_before_returning() -> Result<()> {
    let dir = TempDir::new()?;
    let db_path = dir.path().join("stop-count.db");
    let storage = FrankenStorage::open(&db_path)?;
    seed_lexical_rebuild_fixture(&storage);
    let db = db_path.to_string_lossy().into_owned();
    let partial_path = dir.path().join(EXPECTED_LEXICAL_DOCS_PARTIAL_FILE);
    let identity = expected_lexical_docs_identity(&storage, count_total_messages_exact(&storage)?)?;
    let full_count = expected_live_lexical_doc_count(&storage)?;
    let ids: Vec<i64> = storage.raw().query_map_collect(
        "SELECT id FROM conversations ORDER BY id",
        &[] as &[ParamValue],
        |row: &crate::franken_sync::Row| row.get_typed::<i64>(0),
    )?;
    assert_eq!(ids.len(), 2);
    let progress = IndexingProgress::default();
    let mut visited = Vec::new();
    let error = expected_live_lexical_doc_count_resumable_with(
        &storage,
        Some(&partial_path),
        &db,
        &identity,
        Some(&progress),
        Duration::MAX,
        |conversation_id| {
            visited.push(conversation_id);
            progress.request_stop();
            Ok(())
        },
    )
    .expect_err("stop must interrupt the preflight scan at its next conversation boundary");
    assert!(error.downcast_ref::<IndexInterrupted>().is_some());
    assert_eq!(visited, vec![ids[0]], "no next conversation may be read");
    let partial: ExpectedLexicalDocsPartial = serde_json::from_slice(&fs::read(&partial_path)?)?;
    assert_eq!(partial.last_conversation_id, ids[0]);
    assert!(partial.expected_docs > 0);

    let mut resumed = Vec::new();
    let count = expected_live_lexical_doc_count_resumable_with(
        &storage,
        Some(&partial_path),
        &db,
        &identity,
        None,
        Duration::MAX,
        |conversation_id| {
            resumed.push(conversation_id);
            Ok(())
        },
    )?;
    assert_eq!(resumed, vec![ids[1]]);
    assert_eq!(
        count, full_count,
        "resumption must agree with the full count"
    );
    assert!(!partial_path.exists());
    Ok(())
}

/// GH #381: the expected-docs memo describes the canonical database, so a
/// lexical publish (which exchanges the generation directory) must not
/// discard it. Otherwise the first run after every rebuild re-reads every
/// message in the archive. A planted sentinel that is still served after
/// a real from-zero rebuild and publish proves no full scan was owed.
#[test]
#[serial_test::serial]
fn gh381_expected_lexical_docs_memo_survives_lexical_publication() {
    let dir = TempDir::new().unwrap();
    let data_dir = dir.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("gh381.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    seed_lexical_rebuild_fixture(&storage);
    drop(storage);
    rebuild_tantivy_from_db(&db_path, &data_dir, 2, None).unwrap();
    let index_path = index_dir(&data_dir).unwrap();
    let db = db_path.to_string_lossy().into_owned();
    let cached = || {
        let storage = FrankenStorage::open_readonly(&db_path).unwrap();
        expected_live_lexical_doc_count_cached(
            &storage,
            &index_path,
            &db,
            count_total_messages_exact(&storage).unwrap(),
            None,
        )
        .unwrap()
    };
    let full =
        expected_live_lexical_doc_count(&FrankenStorage::open_readonly(&db_path).unwrap()).unwrap();
    assert_eq!(cached(), full);
    let sidecar_path = expected_lexical_docs_cache_path(&index_path).unwrap();
    assert!(
        !sidecar_path.starts_with(&index_path),
        "the memo must live outside the generation directory a publish exchanges"
    );
    let mut planted: ExpectedLexicalDocsCache =
        serde_json::from_slice(&fs::read(&sidecar_path).unwrap()).unwrap();
    planted.expected_docs = 999_999;
    fs::write(&sidecar_path, serde_json::to_vec(&planted).unwrap()).unwrap();

    // Drop the completed checkpoint so the next rebuild starts from zero,
    // builds off-live and publishes by exchanging the generation directory.
    clear_lexical_rebuild_state(&index_path).unwrap();
    let retained_backups = || -> std::collections::BTreeSet<std::ffi::OsString> {
        fs::read_dir(
            index_path
                .parent()
                .unwrap()
                .join(".lexical-publish-backups"),
        )
        .map(|entries| entries.map(|entry| entry.unwrap().file_name()).collect())
        .unwrap_or_default()
    };
    let backups_before = retained_backups();
    let rebuilt = rebuild_tantivy_from_db(&db_path, &data_dir, 2, None).unwrap();
    assert!(rebuilt.exact_checkpoint_persisted);
    assert_eq!(
        retained_backups().difference(&backups_before).count(),
        1,
        "the rebuild must have exchanged the generation directory and retained the prior one"
    );

    assert_eq!(
        cached(),
        999_999,
        "an unchanged canonical identity is served from the memo after publication"
    );
}

/// Task 8: the authoritative rebuild lane certifies ONLY after the
/// candidate validates and (for the staged lanes) swaps live — the
/// durable receipt, the `lexical_generation` bump and the
/// `index_ready` journal row land atomically at the end of the
/// publish path, bound to the coverage floor captured at build start.
#[test]
fn rebuild_tantivy_certifies_published_manifest_after_validation() {
    let dir = TempDir::new().unwrap();
    let data_dir = dir.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("rebuild-certify.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    crate::viewer_extensions::schema::ensure_schema(storage.raw()).unwrap();
    seed_lexical_rebuild_fixture(&storage);
    drop(storage);

    rebuild_tantivy_from_db(&db_path, &data_dir, 2, None).unwrap();

    let reader = FrankenStorage::open_readonly(&db_path).unwrap();
    let receipt = crate::viewer_extensions::changes::latest_lexical_publication(reader.raw())
        .unwrap()
        .expect("a published rebuild must leave a durable PublicationReceipt");
    assert_eq!(receipt.lexical_generation, 1);
    assert!(
        receipt.quill_generation >= 1,
        "the receipt must name the committed Quill generation: {receipt:?}"
    );
    let revisions = crate::viewer_extensions::changes::current_journal_revisions(reader.raw())
        .unwrap()
        .expect("revisions");
    assert_eq!(
        revisions.lexical_generation, 1,
        "viewer_revisions must carry the certified ordinal"
    );
    let page = crate::viewer_extensions::changes::changes_since(reader.raw(), 0, 64).unwrap();
    assert!(
        page.events
            .iter()
            .any(|e| e.kind == crate::viewer_extensions::changes::kind::INDEX_READY),
        "the certified publication must journal index_ready: {:?}",
        page.events
    );
}

/// #439: the fallback-FTS shadow maintenance must report liveness per
/// streamed page, otherwise the post-publish `--full` tail looks like a
/// finalize wedge to the stall watchdog.
#[test]
fn fallback_fts_repair_after_full_run_ticks_its_heartbeat_per_page() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("fts-heartbeat.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    // No shadow at all, so the repair has to stream every canonical row.
    seed_lexical_rebuild_fixture(&storage);

    let ticks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let heartbeat_ticks = Arc::clone(&ticks);
    let repair = repair_fallback_fts_after_full_index_run(
        &storage,
        &db_path,
        true,
        false,
        None,
        Some(Box::new(move || {
            heartbeat_ticks.fetch_add(1, Ordering::Relaxed);
        })),
    )
    .unwrap();
    assert_eq!(
        repair,
        Some(FallbackFtsRepairOutcome::Repaired(
            FtsConsistencyRepair::Rebuilt { inserted_rows: 4 }
        ))
    );
    assert!(
        ticks.load(Ordering::Relaxed) >= 1,
        "streaming the shadow rows must tick the liveness heartbeat"
    );
}

#[test]
fn full_rebuild_does_not_restart_based_on_historical_local_rowids() {
    fn insert_demo_conversation(db_path: &Path, external_id: &str, msg_idx: i64, ts: i64) {
        let storage = crate::storage::sqlite::SqliteStorage::open(db_path).unwrap();
        let agent = crate::model::types::Agent {
            id: None,
            slug: "tester".into(),
            name: "Tester".into(),
            version: None,
            kind: crate::model::types::AgentKind::Cli,
        };
        let agent_id = storage.ensure_agent(&agent).unwrap();
        let conv = norm_conv(Some(external_id), vec![norm_msg(msg_idx, ts)]);
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
        crate::storage::sqlite::rebuild_fts_via_rusqlite(db_path).unwrap();
    }

    let tmp = TempDir::new().unwrap();
    let canonical_db = tmp.path().join("agent_search.db");
    let backups_dir = tmp.path().join("backups");
    std::fs::create_dir_all(&backups_dir).unwrap();
    let healthy_backup = backups_dir.join("agent_search.db.20260322T020200.bak");

    insert_demo_conversation(&canonical_db, "canonical-only", 0, 1_700_000_000_000);
    insert_demo_conversation(&healthy_backup, "backup-1", 0, 1_700_000_000_100);
    insert_demo_conversation(&healthy_backup, "backup-2", 1, 1_700_000_000_200);

    // Migrated from rusqlite per AGENTS.md Rule 2 + bead uiojh: the
    // meta row seed is a plain INSERT into a user table (not a
    // writable_schema path), so fsqlite handles it with no upstream
    // feature dependency. Opens a FrankenConnection directly against
    // the file written by SqliteStorage above.
    let canonical_db_path = canonical_db.to_string_lossy().to_string();
    let conn = crate::franken_sync::Connection::open(canonical_db_path).unwrap();
    conn.execute_compat(
        "INSERT INTO meta(key, value) VALUES(?1, ?2)",
        &[
            ParamValue::from("historical_bundle_salvaged:test"),
            ParamValue::from("{\"salvage_version\":2,\"method\":\"baseline-bulk-sql-copy\"}"),
        ],
    )
    .unwrap();
    drop(conn);

    let storage = FrankenStorage::open(&canonical_db).unwrap();
    let canonical_sessions = count_total_conversations_exact(&storage).unwrap();
    assert_eq!(canonical_sessions, 1);

    assert!(
        !full_rebuild_requires_historical_restart(&storage, &canonical_db, canonical_sessions)
            .unwrap(),
        "full rebuild must not compare local message rowids across different sqlite files"
    );
}

#[test]
fn full_rebuild_restart_ignores_stale_progress_when_canonical_is_healthy() {
    fn insert_demo_conversation(db_path: &Path, external_id: &str, msg_idx: i64, ts: i64) {
        let storage = crate::storage::sqlite::SqliteStorage::open(db_path).unwrap();
        let agent = crate::model::types::Agent {
            id: None,
            slug: "tester".into(),
            name: "Tester".into(),
            version: None,
            kind: crate::model::types::AgentKind::Cli,
        };
        let agent_id = storage.ensure_agent(&agent).unwrap();
        let conv = norm_conv(Some(external_id), vec![norm_msg(msg_idx, ts)]);
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
        crate::storage::sqlite::rebuild_fts_via_rusqlite(db_path).unwrap();
    }

    let tmp = TempDir::new().unwrap();
    let canonical_db = tmp.path().join("agent_search.db");
    let backups_dir = tmp.path().join("backups");
    std::fs::create_dir_all(&backups_dir).unwrap();
    let healthy_backup = backups_dir.join("agent_search.db.20260322T020200.bak");

    insert_demo_conversation(&canonical_db, "canonical-only", 0, 1_700_000_000_000);
    insert_demo_conversation(&healthy_backup, "backup-only", 0, 1_700_000_000_100);

    let storage = FrankenStorage::open(&canonical_db).unwrap();
    storage
        .raw()
        .execute_compat(
            "INSERT INTO meta(key, value) VALUES(?1, ?2)",
            &[
                ParamValue::from("historical_bundle_progress:test"),
                ParamValue::from("{\"progress_version\":1,\"last_completed_source_row_id\":78}"),
            ],
        )
        .unwrap();

    let canonical_sessions = count_total_conversations_exact(&storage).unwrap();
    assert_eq!(canonical_sessions, 1);
    assert!(
        !full_rebuild_requires_historical_restart(&storage, &canonical_db, canonical_sessions)
            .unwrap(),
        "stale salvage progress alone must not force a fresh canonical restart when the canonical db is healthy"
    );
}

#[test]
fn open_storage_for_index_refuses_corrupt_archive_without_replacing_db() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db.sqlite");
    let original = b"not a sqlite database";
    std::fs::write(&db_path, original).unwrap();

    let err = match open_storage_for_index(&db_path, true) {
        Ok(_) => panic!("corrupt existing archive must fail closed"),
        Err(err) => err.to_string(),
    };
    assert!(
        err.contains("will not replace or truncate"),
        "diagnostic should explain archive preservation: {err}"
    );
    assert_eq!(
        std::fs::read(&db_path).unwrap(),
        original,
        "corrupt archive bytes must be preserved for recovery"
    );

    let backup_count = std::fs::read_dir(tmp.path())
        .unwrap()
        .flatten()
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .map(|name| name.starts_with("db.sqlite.backup."))
                .unwrap_or(false)
        })
        .count();
    assert!(
        backup_count == 0,
        "index open must not create backup artifacts for corrupt archive"
    );
}

#[test]
fn persist_append_only_adds_new_messages_to_index() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    let mut index = TantivyIndex::open_or_create(&index_dir(&data_dir).unwrap()).unwrap();

    // Task 5: persist also stages one `meta:title` search-region
    // document per conversation in the `msg_idx >= REGION_DOC_BASE`
    // space; this test counts canonical message documents only.
    let canonical_doc_count = |reader: &frankensearch::quill::QuillSearchIndex| {
        let query = crate::search::quill_bridge::canonical_message_query(
            frankensearch::quill::query::Query::all(),
        );
        crate::search::quill_bridge::search_paginated(reader, &query, 1, 0, true)
            .expect("canonical doc count")
            .total_count
            .unwrap_or(0)
    };

    let conv1 = norm_conv(Some("ext"), vec![norm_msg(0, 100), norm_msg(1, 200)]);
    persist::persist_conversation(&storage, &mut index, &conv1).unwrap();
    index.commit().unwrap();

    let reader = index.reader().unwrap();
    crate::search::quill_bridge::refresh_reader(&reader).unwrap();
    assert_eq!(canonical_doc_count(&reader), 2);

    let conv2 = norm_conv(
        Some("ext"),
        vec![norm_msg(0, 100), norm_msg(1, 200), norm_msg(2, 300)],
    );
    persist::persist_conversation(&storage, &mut index, &conv2).unwrap();
    index.commit().unwrap();

    let reader = index.reader().unwrap();
    crate::search::quill_bridge::refresh_reader(&reader).unwrap();
    assert_eq!(canonical_doc_count(&reader), 3);
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
fn rebuild_tantivy_from_db_emits_phase_exact_prep_profile_logs() {
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
        let rebuild = rebuild_tantivy_from_db(&db_path, &data_dir, 2, None).unwrap();
        assert_eq!(rebuild.indexed_docs - rebuild.indexed_region_docs, 4);
    });

    // Bead 9752r: commit 9e6c0ef7 (9ct8r) routes restart_from_zero
    // rebuilds with total_conversations > 0 through
    // `rebuild_tantivy_from_db_via_staged_shards`, which builds in
    // a scratch tmpdir and publishes atomically via
    // `publish_staged_lexical_index`. That path never opens a
    // Tantivy writer against the live index, so the in-place-only
    // steps `open_tantivy` and `ready_to_index` are no longer
    // emitted for this scenario. The staged path's analogue is
    // `persist_initial_checkpoint` (which already appears below).
    for needle in [
        "lexical rebuild prep profile",
        r#"component="main""#,
        r#"step="open_readonly""#,
        r#"step="compute_db_state_fingerprint""#,
        r#"step="load_checkpoint_state""#,
        r#"step="start_packet_producer""#,
        r#"step="plan_lexical_shards""#,
        r#"step="persist_initial_checkpoint""#,
        r#"component="producer""#,
        r#"step="load_sources""#,
        r#"step="build_lookups""#,
        r#"step="load_resume_cursor""#,
        r#"step="first_batch_handoff""#,
    ] {
        assert!(
            logs.contains(needle),
            "expected prep-profile log fragment `{needle}`, got:
{logs}"
        );
    }

    // Ordering invariant: deterministic shard planning must
    // complete before the producer starts up, and the producer
    // startup must complete before the initial checkpoint is
    // persisted (the staged-path analogue of the old
    // "producer-before-open_tantivy" overlap contract).
    let start_packet_producer = logs
        .find(r#"step="start_packet_producer""#)
        .expect("start_packet_producer log position");
    let plan_lexical_shards = logs
        .find(r#"step="plan_lexical_shards""#)
        .expect("plan_lexical_shards log position");
    let persist_initial_checkpoint = logs
        .find(r#"step="persist_initial_checkpoint""#)
        .expect("persist_initial_checkpoint log position");
    assert!(
        plan_lexical_shards < start_packet_producer,
        "fresh rebuild should finish deterministic shard planning before producer startup overlap: {logs}"
    );
    assert!(
        start_packet_producer < persist_initial_checkpoint,
        "fresh staged rebuild should start the packet producer before \
         persisting the initial checkpoint (producer-handoff overlap): {logs}"
    );
}

#[test]
#[serial]
fn rebuild_tantivy_from_db_skips_tantivy_open_when_checkpoint_is_complete() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    seed_lexical_rebuild_fixture(&storage);
    drop(storage);

    let index_path = index_dir(&data_dir).unwrap();
    // #494: a completed checkpoint is honoured only over a readable
    // generation whose live count matches it. Build that generation for
    // real rather than planting a checkpoint over an empty directory.
    let first = rebuild_tantivy_from_db(&db_path, &data_dir, 2, None).unwrap();
    assert_eq!(first.indexed_docs, 4);
    assert!(
        load_lexical_rebuild_state(&index_path)
            .unwrap()
            .is_some_and(|state| state.completed),
        "the first rebuild must leave a completed checkpoint"
    );

    let _prep_profile = set_env("CASS_PREP_PROFILE", "1");
    let logs = capture_logs(|| {
        let rebuild = rebuild_tantivy_from_db(&db_path, &data_dir, 2, None).unwrap();
        assert_eq!(rebuild.indexed_docs - rebuild.indexed_region_docs, 4);
        assert_eq!(rebuild.observed_messages, Some(4));
        assert!(!rebuild.exact_checkpoint_persisted);
    });

    assert!(
        !logs.contains(r#"step="open_tantivy""#),
        "completed checkpoint should not reopen Tantivy: {logs}"
    );
    assert!(
        !logs.contains(r#"step="start_packet_producer""#),
        "completed checkpoint should not start producer warmup: {logs}"
    );
    assert!(
        !logs.contains(r#"component="producer""#),
        "completed checkpoint should not emit producer prep logs: {logs}"
    );
    assert!(
        !index_path.join("meta.json").exists(),
        "completed checkpoint fast-path should not create Tantivy metadata"
    );
}

/// One conversation with two messages. Region documents exist for it once
/// it is persisted, so the terminal publication has a region share to
/// converge, record and resume.
fn terminal_resume_conversation(dir: &Path) -> NormalizedConversation {
    NormalizedConversation {
        agent_slug: "claude_code".into(),
        external_id: Some("terminal-resume".into()),
        title: Some("Terminal resume".into()),
        workspace: Some(dir.join("workspace")),
        source_path: dir.join("terminal-resume.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_050),
        metadata: serde_json::json!({"model": "fixture-model"}),
        messages: vec![
            NormalizedMessage {
                idx: 0,
                role: "user".into(),
                author: None,
                created_at: Some(1_700_000_000_000),
                content: "terminal question".into(),
                extra: serde_json::json!({"uuid": "u-0"}),
                snippets: Vec::new(),
                invocations: Vec::new(),
            },
            NormalizedMessage {
                idx: 1,
                role: "assistant".into(),
                author: None,
                created_at: Some(1_700_000_000_050),
                content: "terminal answer".into(),
                extra: serde_json::json!({"uuid": "a-1"}),
                snippets: Vec::new(),
                invocations: Vec::new(),
            },
        ],
    }
}

/// Migrated archive holding one persisted conversation. Commits are forced
/// after every conversation, so the last streaming checkpoint's cursor
/// already equals the total before the terminal finalize begins.
fn terminal_resume_archive(data_dir: &Path) -> PathBuf {
    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    storage.run_migrations().unwrap();
    let conversation = terminal_resume_conversation(data_dir);
    persist::persist_conversations_batched(
        &storage,
        None,
        std::slice::from_ref(&conversation),
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
        false,
    )
    .unwrap();
    drop(storage);
    db_path
}

/// Quarantine directories are reserved beside the versioned index
/// directory (`<data>/index/`), where the staged candidate also lives.
fn quarantine_entries(index_path: &Path) -> Vec<String> {
    std::fs::read_dir(index_path.parent().unwrap())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with(".lexical-rebuild-quarantine-"))
        .collect()
}

/// A kill anywhere after the last batch committed must leave a checkpoint
/// that the next run finalizes in place: the interrupted candidate is the
/// one published, nothing is quarantined, and no conversation is re-indexed.
#[test]
#[serial]
#[cfg(unix)]
fn rebuild_resumes_terminal_finalize_without_reindexing_after_interruption() {
    use std::os::unix::fs::MetadataExt;
    for stage in [
        "terminal_pending_recorded",
        "candidate_committed",
        "before_terminal_checkpoint",
    ] {
        let tmp = TempDir::new().unwrap();
        let data_dir = tmp.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();
        let db_path = terminal_resume_archive(&data_dir);
        let _commit_every = set_env("CASS_TANTIVY_REBUILD_COMMIT_EVERY_CONVERSATIONS", "1");
        let index_path = index_dir(&data_dir).unwrap();
        let scratch = staged_lexical_rebuild_scratch_path(&index_path);

        LEXICAL_TERMINAL_FAILPOINT.with(|armed| armed.set(Some(stage)));
        let interrupted = rebuild_tantivy_from_db(&db_path, &data_dir, 1, None);
        assert!(
            interrupted.is_err(),
            "{stage}: the probe must interrupt the terminal finalize"
        );
        let interrupted_state = load_lexical_rebuild_state(&index_path)
            .unwrap()
            .expect("interrupted run leaves a checkpoint");
        assert!(!interrupted_state.completed, "{stage}");
        assert_eq!(
            interrupted_state.processed_conversations, 1,
            "{stage}: the cursor already reached the end of the archive"
        );
        let candidate_ino = std::fs::metadata(&scratch).unwrap().ino();

        let resumed = rebuild_tantivy_from_db(&db_path, &data_dir, 1, None)
            .unwrap_or_else(|error| panic!("{stage}: resume failed: {error:#}"));

        assert_eq!(
            std::fs::metadata(&index_path).unwrap().ino(),
            candidate_ino,
            "{stage}: the interrupted candidate must be published, not rebuilt"
        );
        assert_eq!(
            quarantine_entries(&index_path),
            Vec::<String>::new(),
            "{stage}: a fully indexed candidate must not be quarantined"
        );
        let final_state = load_lexical_rebuild_state(&index_path).unwrap().unwrap();
        assert!(final_state.completed, "{stage}");
        assert!(
            resumed.indexed_region_docs > 0,
            "{stage}: the fixture must carry region documents"
        );
        assert_eq!(
            final_state.indexed_region_docs, resumed.indexed_region_docs,
            "{stage}: the resumed terminal keeps the region share it recorded"
        );
        assert_eq!(
            crate::search::tantivy::validated_searchable_index_summary(&index_path)
                .unwrap()
                .docs,
            resumed.indexed_docs,
            "{stage}: the published generation holds exactly the certified documents"
        );
    }
}

/// A checkpoint that claims fewer documents than the candidate it names is
/// not finalized. The resume quarantines the candidate and rebuilds it.
#[test]
#[serial]
#[cfg(unix)]
fn rebuild_restarts_when_terminal_checkpoint_disagrees_with_candidate() {
    use std::os::unix::fs::MetadataExt;
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let db_path = terminal_resume_archive(&data_dir);
    let _commit_every = set_env("CASS_TANTIVY_REBUILD_COMMIT_EVERY_CONVERSATIONS", "1");
    let index_path = index_dir(&data_dir).unwrap();
    let scratch = staged_lexical_rebuild_scratch_path(&index_path);

    LEXICAL_TERMINAL_FAILPOINT.with(|armed| armed.set(Some("candidate_committed")));
    assert!(rebuild_tantivy_from_db(&db_path, &data_dir, 1, None).is_err());
    let candidate_ino = std::fs::metadata(&scratch).unwrap().ino();

    // Forge the disagreement: no pending commit to reconcile, and a count
    // one document short of what the candidate holds.
    let mut forged = load_lexical_rebuild_state(&index_path).unwrap().unwrap();
    forged.pending = None;
    forged.indexed_docs -= 1;
    persist_lexical_rebuild_state(&index_path, &forged).unwrap();

    let resumed = rebuild_tantivy_from_db(&db_path, &data_dir, 1, None)
        .expect("a mismatched checkpoint restarts and completes");
    assert_eq!(
        quarantine_entries(&index_path).len(),
        1,
        "the mismatched candidate is retained as evidence"
    );
    assert_ne!(
        std::fs::metadata(&index_path).unwrap().ino(),
        candidate_ino,
        "the published generation is a fresh build, not the forged candidate"
    );
    assert!(
        load_lexical_rebuild_state(&index_path)
            .unwrap()
            .unwrap()
            .completed
    );
    assert_eq!(
        crate::search::tantivy::validated_searchable_index_summary(&index_path)
            .unwrap()
            .docs,
        resumed.indexed_docs
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
fn rebuild_tantivy_from_db_logs_streamed_batch_stats() {
    // Bead 9752r: after 9e6c0ef7 (9ct8r) routes restart_from_zero
    // rebuilds with total_conversations > 0 through the
    // staged-shards path, the in-place streaming `flushed a
    // streamed conversation batch` + `chunk_*` logs no longer
    // fire for this scenario. The staged path's equivalent
    // page-level diagnostic is `lexical rebuild prepared bounded
    // page` (already emitted) plus `built lexical rebuild shard
    // index` as the commit-phase analogue. The bounded-page
    // budget fields and the overall indexed_docs assertion below
    // are preserved because they're path-agnostic contracts.
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    seed_lexical_rebuild_fixture(&storage);

    let _conversation_limit = set_env("CASS_TANTIVY_REBUILD_BATCH_FETCH_CONVERSATIONS", "2");

    let logs = capture_logs(|| {
        let rebuild = rebuild_tantivy_from_db(&db_path, &data_dir, 2, None).unwrap();
        assert_eq!(rebuild.indexed_docs - rebuild.indexed_region_docs, 4);
        assert_eq!(rebuild.observed_messages, Some(4));
    });

    assert!(
        logs.contains("running fresh authoritative lexical rebuild via staged shard-build path"),
        "expected staged-shards rebuild banner after 9ct8r routing, got:
{logs}"
    );
    assert!(
        logs.contains("lexical rebuild prepared bounded page"),
        "expected page-level bounded-prep diagnostics, got:
{logs}"
    );
    assert!(
        logs.contains("budget_shrink_decision=")
            && logs.contains("batch_fetch_message_bytes_limit=")
            && logs.contains("max_message_bytes_in_flight="),
        "expected bounded page budget diagnostics, got:
{logs}"
    );
    assert!(
        logs.contains("page_messages=4") && logs.contains("page_conversations=2"),
        "expected per-page conversation + message counts matching the fixture (2 convs / 4 msgs), got:
{logs}"
    );
    assert!(
        logs.contains("built lexical rebuild shard index"),
        "expected staged-shards commit-phase diagnostic, got:
{logs}"
    );
    assert!(
        logs.contains("indexed_docs=4") && logs.contains("shard_conversations=2"),
        "expected shard-build completion counts (4 docs / 2 convs) in staged output, got:
{logs}"
    );
    assert_eq!(tantivy_doc_count_for_data_dir(&data_dir), 4);
}

#[test]
#[serial]
fn rebuild_tantivy_from_db_emits_equivalence_evidence() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    seed_lexical_rebuild_fixture(&storage);

    let logs = capture_logs(|| {
        let rebuild = rebuild_tantivy_from_db(&db_path, &data_dir, 2, None).unwrap();
        assert_eq!(rebuild.indexed_docs - rebuild.indexed_region_docs, 4);
        let evidence = rebuild
            .equivalence
            .as_ref()
            .expect("authoritative rebuild must emit equivalence evidence");
        assert_eq!(evidence.document_count, 4);
        assert_eq!(
            evidence.manifest_fingerprint.len(),
            64,
            "manifest fingerprint should be a 32-byte blake3 hex digest"
        );
        assert_eq!(evidence.golden_query_digest.len(), 64);
        let default_probes = LEXICAL_REBUILD_EQUIVALENCE_DEFAULT_PROBES.iter().copied();
        let recorded_probes = evidence
            .golden_query_hit_counts
            .iter()
            .map(|hit| hit.probe.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            recorded_probes,
            default_probes.collect::<Vec<_>>(),
            "evidence should record the default probe list verbatim"
        );

        let index_path = index_dir(&data_dir).unwrap();
        let artifact_path = lexical_rebuild_equivalence_evidence_path(&index_path);
        let persisted = std::fs::read_to_string(&artifact_path)
            .expect("equivalence evidence artifact should be persisted on disk");
        // `super::` skips the test-local shadowed struct so we can round-trip
        // into the production type emitted by the rebuild.
        let parsed: crate::indexer::LexicalRebuildEquivalenceEvidence =
            serde_json::from_str(&persisted).expect("persisted evidence is valid JSON");
        assert_eq!(&parsed, evidence);
    });

    assert!(
        logs.contains("lexical rebuild authoritative equivalence evidence"),
        "expected authoritative equivalence evidence log, got:
{logs}"
    );
    assert!(
        logs.contains("manifest_fingerprint="),
        "expected manifest_fingerprint field in evidence log, got:
{logs}"
    );
    assert!(
        logs.contains("golden_query_digest="),
        "expected golden_query_digest field in evidence log, got:
{logs}"
    );
}

#[test]
#[serial]
fn rebuild_tantivy_from_db_persists_serveable_generation_manifest() {
    // Bead ibuuh.30 slice: the authoritative rebuild must write a
    // lexical generation manifest alongside the equivalence ledger, and
    // that manifest must declare the generation serveable (Validated +
    // Published) with counts that line up with the rebuild outcome and
    // the equivalence fingerprint that ibuuh.29 computed.
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    seed_lexical_rebuild_fixture(&storage);

    let logs = capture_logs(|| {
        let rebuild = rebuild_tantivy_from_db(&db_path, &data_dir, 2, None).unwrap();
        let evidence = rebuild
            .equivalence
            .as_ref()
            .expect("authoritative rebuild must emit equivalence evidence");
        assert_eq!(rebuild.indexed_docs - rebuild.indexed_region_docs, 4);

        let index_path = index_dir(&data_dir).unwrap();
        let loaded = lexical_generation::load_manifest(&index_path)
            .expect("loading the generation manifest must not error")
            .expect(
                "authoritative rebuild must persist a generation manifest \
                 alongside the equivalence ledger",
            );

        // Publish-readiness contract: only a manifest in Validated build state
        // AND Published publish state may be served by ordinary search.
        assert!(
            loaded.is_serveable(),
            "freshly committed generation must be Validated+Published; got \
             build_state={:?} publish_state={:?}",
            loaded.build_state,
            loaded.publish_state
        );
        assert_eq!(
            loaded.build_state,
            lexical_generation::LexicalGenerationBuildState::Validated
        );
        assert_eq!(
            loaded.publish_state,
            lexical_generation::LexicalGenerationPublishState::Published
        );

        // Counts must line up with the rebuild outcome so operators can
        // sanity-check the manifest against progress logs without
        // replaying the rebuild.
        assert_eq!(
            loaded.indexed_doc_count, evidence.document_count,
            "manifest indexed_doc_count must match equivalence accumulator"
        );
        assert_eq!(
            loaded.indexed_doc_count, rebuild.indexed_docs as u64,
            "manifest indexed_doc_count must match outcome.indexed_docs"
        );

        // The equivalence fingerprint bridges ibuuh.29 and ibuuh.30:
        // the manifest must point at the streaming accumulator digest
        // the rebuild just emitted, so validation tooling can diff
        // them without re-reading the ledger artifact.
        assert_eq!(
            loaded.equivalence_manifest_fingerprint.as_deref(),
            Some(evidence.manifest_fingerprint.as_str()),
            "generation manifest must link to the equivalence accumulator fingerprint"
        );

        assert!(
            !loaded.generation_id.is_empty(),
            "generation_id must be non-empty"
        );
        assert!(
            !loaded.attempt_id.is_empty(),
            "attempt_id must be non-empty"
        );
        assert!(
            !loaded.source_db_fingerprint.is_empty(),
            "source_db_fingerprint must be propagated from rebuild_state"
        );
        assert!(
            loaded.failure_history.is_empty(),
            "successful rebuild should have an empty failure history"
        );
        assert!(
            loaded.updated_at_ms >= loaded.created_at_ms,
            "updated_at_ms must be non-decreasing relative to created_at_ms"
        );
    });

    assert!(
        logs.contains("lexical generation manifest published"),
        "expected generation manifest publish log, got:
{logs}"
    );
    assert!(
        logs.contains("generation_id=") && logs.contains("attempt_id="),
        "expected generation_id + attempt_id fields in publish log, got:
{logs}"
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
fn rebuild_tantivy_from_db_persists_packet_refresh_ledger() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    seed_lexical_rebuild_fixture(&storage);

    let logs = capture_logs(|| {
        let rebuild = rebuild_tantivy_from_db(&db_path, &data_dir, 2, None).unwrap();
        let evidence = rebuild
            .equivalence
            .as_ref()
            .expect("authoritative rebuild must emit equivalence evidence");

        let index_path = index_dir(&data_dir).unwrap();
        let raw = std::fs::read_to_string(lexical_refresh_ledger_path(&index_path))
            .expect("refresh ledger should be persisted for the packet-driven rebuild");
        let ledger: refresh_ledger::RefreshLedger =
            serde_json::from_str(&raw).expect("refresh ledger must be valid JSON");
        let milestones = ledger.readiness_milestones();

        assert_eq!(
            milestones.search_readiness_state,
            refresh_ledger::RefreshSearchReadinessState::Published
        );
        assert!(
            milestones.time_to_lexical_ready_ms.is_some(),
            "packet rebuild should record a lexical-ready milestone"
        );
        assert!(
            milestones.time_to_search_ready_ms.is_some(),
            "packet rebuild should record a search-ready milestone"
        );
        assert_eq!(
            ledger.tags.get("dataflow").map(String::as_str),
            Some("conversation_packet")
        );
        assert_eq!(
            ledger.tags.get("publish_mode").map(String::as_str),
            Some("atomic_staged_swap")
        );
        assert!(
            ledger.full_rebuild,
            "authoritative canonical packet replay is a full lexical rebuild"
        );
        assert_eq!(
            ledger
                .phase(refresh_ledger::RefreshPhase::LexicalRebuild)
                .and_then(|phase| phase.counters.get("indexed_docs"))
                .copied(),
            Some(u64::try_from(rebuild.indexed_docs).unwrap_or(u64::MAX))
        );
        assert_eq!(
            ledger.equivalence.search_hit_digest.as_deref(),
            Some(evidence.golden_query_digest.as_str())
        );
    });

    assert!(
        logs.contains("lexical refresh ledger published"),
        "expected refresh ledger publish log, got:\n{logs}"
    );
}

/// `coding_agent_session_search-ibuuh.24` evidence-sidecar wiring:
/// every authoritative lexical publish MUST also write
/// `.lexical-refresh-evidence.json` next to the raw ledger so
/// benchmark gates and operator dashboards can diff derived
/// metrics across runs without re-running the math. Pin both:
/// 1. The sidecar file exists at the expected path.
/// 2. Its JSON shape matches the RefreshLedgerEvidence contract
///    (presence of every documented field).
/// 3. The values are internally consistent with the raw ledger
///    (aggregate_duration_ms == ledger.total_duration_ms,
///    aggregate_items_processed == ledger.total_items_processed()).
///
/// A regression that drops the sidecar OR breaks the ledger ↔
/// evidence consistency invariant trips this test immediately.
#[test]
fn authoritative_publish_emits_lexical_refresh_evidence_sidecar() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("evidence-sidecar.db");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    seed_lexical_rebuild_fixture(&storage);

    rebuild_tantivy_from_db(&db_path, &data_dir, 2, None).unwrap();

    let index_path = index_dir(&data_dir).unwrap();
    let ledger_path = lexical_refresh_ledger_path(&index_path);
    let evidence_path = lexical_refresh_evidence_path(&index_path);

    // Invariant 1: the sidecar lands next to the ledger.
    assert!(
        evidence_path.exists(),
        "evidence sidecar must be written at {} after publish",
        evidence_path.display()
    );

    // Invariant 2: shape matches the documented contract.
    let evidence_raw = std::fs::read_to_string(&evidence_path)
        .expect("evidence sidecar must be readable after publish");
    let parsed: serde_json::Value =
        serde_json::from_str(&evidence_raw).expect("evidence sidecar must be valid JSON");
    for required in [
        "throughput",
        "phase_share",
        "dominant_phase",
        "aggregate_items_processed",
        "aggregate_duration_ms",
        "aggregate_items_per_second",
    ] {
        assert!(
            parsed.get(required).is_some(),
            "evidence sidecar missing required field {required}; got: {parsed}"
        );
    }

    // Invariant 3: ledger ↔ evidence are internally consistent.
    // Pre-fix, a regression that swapped the sidecar's source
    // (e.g. computed evidence from a different ledger snapshot)
    // would silently desync. Pinning the equality directly
    // surfaces that.
    let ledger_raw =
        std::fs::read_to_string(&ledger_path).expect("raw ledger must also be persisted");
    let ledger: refresh_ledger::RefreshLedger =
        serde_json::from_str(&ledger_raw).expect("raw ledger must be valid JSON");
    let computed_evidence = ledger.evidence_summary();
    let serialized_computed: serde_json::Value =
        serde_json::from_str(&serde_json::to_string(&computed_evidence).unwrap()).unwrap();
    assert_eq!(
        parsed, serialized_computed,
        "persisted evidence sidecar must equal evidence_summary() of the persisted ledger; \
         a divergence here means the sidecar was computed from a different ledger snapshot"
    );

    // Sanity: aggregate fields agree with the raw ledger.
    assert_eq!(
        parsed["aggregate_duration_ms"].as_u64(),
        Some(ledger.total_duration_ms),
        "aggregate_duration_ms must equal ledger.total_duration_ms"
    );
    assert_eq!(
        parsed["aggregate_items_processed"].as_u64(),
        Some(ledger.total_items_processed()),
        "aggregate_items_processed must equal ledger.total_items_processed()"
    );
}

/// `coding_agent_session_search-ibuuh.24`: pin the
/// publish-time cross-run tracing wiring. First publish has no
/// prior sidecar to compare against, so no comparison event
/// should fire. Second publish reads the persisted prior and
/// MUST emit a single tracing event under the
/// `cass::indexer::lexical_refresh` target carrying the
/// regression metrics. A regression that broke either branch
/// (always emits / never emits) trips here.
#[test]
fn persist_lexical_refresh_ledger_emits_cross_run_tracing_when_prior_sidecar_exists() {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};
    use tracing::field::{Field, Visit};
    use tracing::{Event, Subscriber};
    use tracing_subscriber::Registry;
    use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

    #[derive(Debug, Clone, Default)]
    struct CapturedEvent {
        level: String,
        message: String,
    }

    #[derive(Clone, Default)]
    struct ComparisonCollector {
        events: Arc<Mutex<Vec<CapturedEvent>>>,
    }

    impl<S: Subscriber> Layer<S> for ComparisonCollector {
        fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
            if event.metadata().target() != "cass::indexer::lexical_refresh" {
                return;
            }
            let mut visitor = MessageVisitor::default();
            event.record(&mut visitor);
            // Only capture cross-run-comparison events (the
            // tracing target is shared with sidecar-write-failure
            // warnings — filter by message prefix so an
            // unrelated emission doesn't pollute the count).
            if !visitor.message.contains("lexical refresh evidence") {
                return;
            }
            self.events
                .lock()
                .expect("collector lock")
                .push(CapturedEvent {
                    level: event.metadata().level().to_string(),
                    message: visitor.message,
                });
        }
    }

    #[derive(Default)]
    struct MessageVisitor {
        message: String,
    }
    impl Visit for MessageVisitor {
        fn record_str(&mut self, _field: &Field, _value: &str) {}
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                self.message = format!("{:?}", value).trim_matches('"').to_string();
            }
        }
    }

    let tmp = TempDir::new().expect("temp dir");
    let data_dir = tmp.path().join("data");
    let index_path = index_dir(&data_dir).unwrap();
    std::fs::create_dir_all(&index_path).expect("create index path");

    // Helper: construct a ledger whose evidence_summary will
    // produce a non-empty Scan throughput (so the comparison has
    // something to compare).
    fn make_ledger(scan_duration_ms: u64) -> RefreshLedger {
        RefreshLedger {
            version: 1,
            started_at_ms: 1_700_000_000_000,
            completed_at_ms: 1_700_000_000_000 + scan_duration_ms as i64,
            total_duration_ms: scan_duration_ms,
            full_rebuild: true,
            corpus_family: "ibuuh.24-wiring-test".to_string(),
            phases: vec![PhaseRecord {
                phase: RefreshPhase::Scan,
                duration_ms: scan_duration_ms,
                items_processed: 100,
                items_skipped: 0,
                errors: 0,
                counters: BTreeMap::new(),
                success: true,
                error_message: None,
            }],
            equivalence: refresh_ledger::EquivalenceArtifacts::default(),
            tags: BTreeMap::new(),
        }
    }

    // ─── First publish: no prior sidecar ─────────────────────
    let collector = ComparisonCollector::default();
    let subscriber = Registry::default().with(collector.clone());
    tracing::subscriber::with_default(subscriber, || {
        persist_lexical_refresh_ledger(&index_path, &make_ledger(100))
            .expect("first persist must succeed");
    });
    let first_events = collector.events.lock().expect("lock").clone();
    assert!(
        first_events.is_empty(),
        "first publish has no prior sidecar to compare against; no cross-run \
         tracing event should fire. Got: {first_events:?}"
    );

    // Sanity: the new sidecar landed for the second publish to read.
    assert!(
        lexical_refresh_evidence_path(&index_path).exists(),
        "first publish must persist a sidecar so the second publish can compare"
    );

    // ─── Second publish: prior sidecar present ───────────────
    // Make the second ledger 200ms (vs prior 100ms) so the
    // comparison reports a +100% slowdown — should trigger the
    // warn tier inside emit_tracing_summary.
    let collector = ComparisonCollector::default();
    let subscriber = Registry::default().with(collector.clone());
    tracing::subscriber::with_default(subscriber, || {
        persist_lexical_refresh_ledger(&index_path, &make_ledger(200))
            .expect("second persist must succeed");
    });
    let second_events = collector.events.lock().expect("lock").clone();
    assert_eq!(
        second_events.len(),
        1,
        "second publish (with prior sidecar) must emit EXACTLY one cross-run \
         comparison event. Got: {second_events:?}"
    );
    let event = &second_events[0];
    assert_eq!(
        event.level, "WARN",
        "+100% duration delta vs prior must route through the warn tier \
         (≥+25% slowdown threshold from emit_tracing_summary). Got: {event:?}"
    );
    assert!(
        event.message.contains("significant slowdown"),
        "warn-tier message must name the slowdown signal; got: {:?}",
        event.message
    );
}

#[test]
fn lexical_rebuild_equivalence_accumulator_matches_legacy_and_keyset_replays() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("lexical-equivalence-accumulator.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    seed_lexical_rebuild_fixture(&storage);

    let legacy_packets = legacy_offset_lexical_rebuild_packets(&storage, 1).unwrap();
    let keyset_packets = keyset_batched_lexical_rebuild_packets(&storage, 2, 2).unwrap();

    let accumulate = |packets: &[LexicalRebuildConversationPacket]| {
        let mut acc = LexicalRebuildEquivalenceAccumulator::new();
        for packet in packets {
            acc.absorb_packet(packet);
        }
        acc.finalize()
    };
    let legacy_evidence = accumulate(&legacy_packets);
    let keyset_evidence = accumulate(&keyset_packets);

    assert_eq!(
        legacy_evidence, keyset_evidence,
        "streaming accumulator must agree across legacy OFFSET and keyset replays"
    );
    assert_eq!(legacy_evidence.document_count, 4);
    assert_eq!(legacy_evidence.manifest_fingerprint.len(), 64);
    assert_eq!(legacy_evidence.golden_query_digest.len(), 64);
}

#[test]
fn lexical_rebuild_equivalence_accumulator_counts_probe_hits_and_hashes_are_stable() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("lexical-equivalence-probes.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    seed_lexical_rebuild_fixture(&storage);

    let packets = keyset_batched_lexical_rebuild_packets(&storage, 10, 10).unwrap();

    let probe_evidence = |probes: &[&str]| {
        let mut acc = LexicalRebuildEquivalenceAccumulator::with_probes(
            probes.iter().map(|probe| (*probe).to_string()),
        );
        for packet in &packets {
            acc.absorb_packet(packet);
        }
        acc.finalize()
    };

    // Fixture content contains two "lexical-fixture-*" strings so only two
    // probes should register hits; "missing-golden-query" should be zero.
    let targeted = probe_evidence(&[
        "lexical-fixture-1",
        "lexical-fixture-2-second",
        "missing-golden-query",
    ]);
    let hits: Vec<_> = targeted
        .golden_query_hit_counts
        .iter()
        .map(|hit| (hit.probe.as_str(), hit.hit_count))
        .collect();
    assert_eq!(
        hits,
        vec![
            ("lexical-fixture-1", 2),
            ("lexical-fixture-2-second", 1),
            ("missing-golden-query", 0),
        ],
        "fixture should produce deterministic per-probe hit counts"
    );

    // Running the accumulator a second time on the same inputs must produce
    // byte-identical evidence, otherwise the hash is not a legitimate proof.
    let repeat = probe_evidence(&[
        "lexical-fixture-1",
        "lexical-fixture-2-second",
        "missing-golden-query",
    ]);
    assert_eq!(
        targeted, repeat,
        "equivalence evidence must be deterministic across invocations"
    );

    // Any change to the probe list must change the digest.
    let shuffled = probe_evidence(&[
        "lexical-fixture-2-second",
        "lexical-fixture-1",
        "missing-golden-query",
    ]);
    assert_ne!(
        targeted.golden_query_digest, shuffled.golden_query_digest,
        "probe order must be part of the digest"
    );
}
