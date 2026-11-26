use super::super::*;
use super::helpers::*;
use crate::connectors::{NormalizedConversation, NormalizedMessage};
use crate::franken_sync::compat::{ConnectionExt, ParamValue, RowExt};
use crate::indexer::LexicalPopulationStrategy;
use crate::model::types::{Agent, AgentKind, Conversation, MessageRole};
use crate::search::tantivy::TantivyIndex;
use crate::sources::provenance::{Source, SourceKind};
use crate::storage::sqlite::FrankenStorage;
use crate::franken_sync::SqliteValue;
use serial_test::serial;
use std::collections::HashSet;

#[test]
#[serial]
fn gh423_codebuff_indexer_revisions_replace_search_preserving_canonical_identity() {
    let _defer = set_env("CASS_DEFER_LEXICAL_UPDATES", "0");
    for route in 0..2 {
        let dir = tempfile::TempDir::new().unwrap();
        let storage = create_franken_db(&dir.path().join("agent_search.db"));
        let index_path = crate::search::tantivy::index_dir(dir.path()).unwrap();
        let mut index = TantivyIndex::open_or_create(&index_path).unwrap();
        let mut conv = NormalizedConversation {
            agent_slug: "codebuff".into(),
            external_id: Some(r#"["/profile/.config/manicode","project","chat"]"#.into()),
            title: Some("Shared lineage".into()), workspace: None,
            source_path: "/profile/.config/manicode/projects/project/chats/chat/chat-messages.json".into(),
            started_at: Some(1_700_000_000_000), ended_at: Some(1_700_000_000_000),
            metadata: serde_json::json!({"shared_lineage":true}),
            messages: (0..4).map(|idx| NormalizedMessage {
                idx, role: "assistant".into(), author: None,
                created_at: Some(1_700_000_000_000),
                content: format!("oldneedle marker{idx}"),
                extra: serde_json::json!({"id":format!("native-{idx}"),"codebuff_message_id":format!("native-{idx}"),"isComplete":false}),
                snippets: Vec::new(), invocations: Vec::new(),
            }).collect(),
        };
        let persist = |index: &mut TantivyIndex, conv: &NormalizedConversation| {
            if route == 0 {
                persist_conversation(&storage, index, conv).unwrap();
                None
            } else {
                Some(
                    persist_conversations_batched_inner(
                        &storage,
                        Some(index),
                        std::slice::from_ref(conv),
                        LexicalPopulationStrategy::IncrementalInline,
                        false,
                        true,
                        Some(dir.path()),
                        PersistHeartbeat::NONE,
                        None,
                    )
                    .unwrap(),
                )
            }
        };
        let hits = |index: &TantivyIndex, term: &str| {
            let reader = index.reader().unwrap();
            let parser = frankensearch::quill::query::CassQueryParser::new(
                frankensearch::quill::schema::CASS_SEMANTIC_SCHEMA,
            )
            .unwrap();
            let query = parser.parse(
                term,
                &frankensearch::quill::query::CassQueryFilters::default(),
            );
            let page =
                crate::search::quill_bridge::search_paginated(&reader, &query.query, 32, 0, true)
                    .unwrap();
            let ids: HashSet<_> = page
                .hits
                .iter()
                .map(|hit| hit.document_id.clone())
                .collect();
            assert_eq!(
                ids.len(),
                page.hits.len(),
                "no duplicate document identities"
            );
            assert_eq!(page.total_count, Some(ids.len()));
            ids
        };
        persist(&mut index, &conv);
        assert_eq!(tantivy_doc_count(&mut index), 4);
        let original_hit = hits(&index, "marker0");
        assert_eq!(original_hit.len(), 1);
        let conversation_id: i64 = storage
            .raw()
            .query_row_map("SELECT id FROM conversations", &[], |row| row.get_typed(0))
            .unwrap();
        let original_ids: Vec<_> = storage
            .fetch_messages(conversation_id)
            .unwrap()
            .iter()
            .map(|message| (message.id, message.idx))
            .collect();

        // The source may be reached through another path, and a native
        // message can finish in place or become non-searchable noise.
        conv.source_path = "/profile/alias/manicode/chat-messages.json".into();
        conv.messages[0].content = "newneedle marker0".into();
        conv.messages[0].extra["isComplete"] = serde_json::json!(true);
        conv.messages[2].content.clear();
        conv.messages.push(NormalizedMessage {
            idx: 4,
            role: "assistant".into(),
            author: None,
            created_at: Some(1_700_000_000_001),
            content: "tailneedle marker4".into(),
            extra: serde_json::json!({"id":"native-4","codebuff_message_id":"native-4"}),
            snippets: Vec::new(),
            invocations: Vec::new(),
        });
        if let Some(outcome) = persist(&mut index, &conv) {
            assert_eq!(outcome.inserted_conversations, 0);
            assert_eq!(outcome.inserted_messages, 1);
            assert_eq!(outcome.updated_messages, 2);
        }
        assert_eq!(tantivy_doc_count(&mut index), 4);
        assert_eq!(
            hits(&index, "newneedle"),
            original_hit,
            "stable stored source path controls lexical identity"
        );
        assert_eq!(hits(&index, "oldneedle").len(), 2);
        assert!(hits(&index, "marker2").is_empty());
        assert_eq!(hits(&index, "tailneedle").len(), 1);
        let stored = storage.fetch_messages(conversation_id).unwrap();
        assert_eq!(stored.len(), 5);
        assert_eq!(
            stored[..4]
                .iter()
                .map(|message| (message.id, message.idx))
                .collect::<Vec<_>>(),
            original_ids
        );
        assert_eq!(stored[0].content, "newneedle marker0");
        assert!(stored[2].content.is_empty());
        let all_ids: Vec<_> = stored
            .iter()
            .map(|message| (message.id, message.idx))
            .collect();
        if let Some(replay) = persist(&mut index, &conv) {
            assert_eq!(replay.inserted_messages, 0);
            assert_eq!(replay.updated_messages, 0);
        }
        assert_eq!(tantivy_doc_count(&mut index), 4);
        assert_eq!(hits(&index, "newneedle"), original_hit);
        assert_eq!(
            storage
                .fetch_messages(conversation_id)
                .unwrap()
                .iter()
                .map(|message| (message.id, message.idx))
                .collect::<Vec<_>>(),
            all_ids
        );

        // Full scan packets can overlap within a batch and across
        // batches. Hydrating final canonical history must remain an
        // upsert even when neither packet inserts another message.
        let mut full_index = TantivyIndex::open_or_create(&dir.path().join("full-index")).unwrap();
        persist_conversations_batched_inner(
            &storage,
            Some(&mut full_index),
            &[conv.clone(), conv.clone()],
            LexicalPopulationStrategy::InlineRebuildFromScan,
            false,
            false,
            Some(dir.path()),
            PersistHeartbeat::NONE,
            None,
        )
        .unwrap();
        full_index.commit().unwrap();
        persist_conversations_batched_inner(
            &storage,
            Some(&mut full_index),
            std::slice::from_ref(&conv),
            LexicalPopulationStrategy::InlineRebuildFromScan,
            false,
            false,
            Some(dir.path()),
            PersistHeartbeat::NONE,
            None,
        )
        .unwrap();
        assert_eq!(tantivy_doc_count(&mut full_index), 4);
        assert_eq!(hits(&full_index, "newneedle"), original_hit);
        assert!(hits(&full_index, "marker2").is_empty());

        // Stop after the real canonical transaction, before any
        // lexical publication. The unchanged replay must retain the
        // durable repair obligation despite unchanged COUNT/MAX(id).
        conv.messages[0].content = "durableneedle marker0".into();
        let deferred = persist_conversations_batched_inner(
            &storage,
            None,
            std::slice::from_ref(&conv),
            LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
            false,
            false,
            Some(dir.path()),
            PersistHeartbeat::NONE,
            None,
        )
        .unwrap();
        assert_eq!(deferred.updated_messages, 1);
        let pending = crate::indexer::load_lexical_rebuild_state(&index_path)
            .unwrap()
            .unwrap();
        assert!(!pending.completed);
        assert!(pending.requires_restart_from_zero_on_resume());
        assert_eq!(hits(&index, "newneedle"), original_hit);
        assert!(hits(&index, "durableneedle").is_empty());
        let replay = persist_conversations_batched_inner(
            &storage,
            None,
            std::slice::from_ref(&conv),
            LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
            false,
            false,
            Some(dir.path()),
            PersistHeartbeat::NONE,
            None,
        )
        .unwrap();
        assert_eq!(replay.updated_messages, 0);
        assert!(
            !crate::indexer::load_lexical_rebuild_state(&index_path)
                .unwrap()
                .unwrap()
                .completed
        );
        drop(index);
        crate::indexer::rebuild_tantivy_from_db_deferred_startup(
            &storage.database_path().unwrap(),
            dir.path(),
            1,
            None,
        )
        .unwrap();
        let index = TantivyIndex::open_or_create(&index_path).unwrap();
        assert_eq!(hits(&index, "durableneedle"), original_hit);
        assert!(hits(&index, "newneedle").is_empty());
        // Task 5: one `meta:title` region doc per conversation also
        // lives in the index; count the canonical message share.
        assert_eq!(
            index.doc_count().unwrap()
                - crate::viewer_extensions::search_regions::live_region_doc_bound(storage.raw(),)
                    .unwrap_or(0) as u64,
            4
        );
        assert!(
            crate::indexer::load_lexical_rebuild_state(&index_path)
                .unwrap()
                .unwrap()
                .completed
        );

        // A filesystem failure to revoke authority must happen before
        // changing the canonical payload, and preserve the old state.
        let checkpoint = crate::indexer::lexical_rebuild_state_path(&index_path);
        std::fs::rename(&checkpoint, index_path.join("retained-checkpoint.json")).unwrap();
        std::fs::create_dir(&checkpoint).unwrap();
        conv.messages[0].content = "mustnotcommit marker0".into();
        assert!(
            persist_conversations_batched_inner(
                &storage,
                None,
                std::slice::from_ref(&conv),
                LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
                false,
                false,
                Some(dir.path()),
                PersistHeartbeat::NONE,
                None,
            )
            .is_err()
        );
        assert_eq!(
            storage.fetch_messages(conversation_id).unwrap()[0].content,
            "durableneedle marker0"
        );
    }
}

#[test]
fn packet_semantic_delta_for_outcome_replays_persisted_canonical_state() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("semantic-delta-replay.db");
    let storage = create_franken_db(&db_path);
    let agent_id = storage
        .ensure_agent(&Agent {
            id: None,
            slug: "codex".into(),
            name: "Codex".into(),
            version: None,
            kind: AgentKind::Cli,
        })
        .unwrap();
    let workspace_id = storage
        .ensure_workspace(std::path::Path::new("/tmp/persist-semantic"), None)
        .unwrap();

    let conversation = Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(std::path::PathBuf::from("/tmp/persist-semantic")),
        external_id: Some("semantic-delta-replay".into()),
        title: Some("Semantic delta replay".into()),
        source_path: std::path::PathBuf::from("/tmp/persist-semantic.jsonl"),
        started_at: Some(1_700_000_200_000),
        ended_at: Some(1_700_000_200_200),
        approx_tokens: Some(32),
        metadata_json: serde_json::json!({}),
        messages: vec![
            crate::model::types::Message {
                id: None,
                idx: 0,
                role: MessageRole::User,
                author: Some("user".into()),
                created_at: Some(1_700_000_200_010),
                content: "original user".into(),
                extra_json: serde_json::json!({}),
                snippets: Vec::new(),
            },
            crate::model::types::Message {
                id: None,
                idx: 1,
                role: MessageRole::Agent,
                author: Some("assistant".into()),
                created_at: Some(1_700_000_200_020),
                content: "original assistant".into(),
                extra_json: serde_json::json!({}),
                snippets: Vec::new(),
            },
        ],
        source_id: "initial-remote".into(),
        origin_host: Some("initial-host".into()),
    };

    let outcome = storage
        .insert_conversation_tree(agent_id, Some(workspace_id), &conversation)
        .unwrap();
    let inserted_ids = load_inserted_message_ids_by_idx(
        &storage,
        outcome.conversation_id,
        &outcome.inserted_indices,
    )
    .unwrap();
    let assistant_id = inserted_ids.get(&1).copied().expect("assistant message id");
    storage
        .upsert_source(&Source {
            id: "replayed-remote".into(),
            kind: SourceKind::Ssh,
            host_label: Some("replayed-host".into()),
            machine_id: None,
            platform: None,
            config_json: None,
            created_at: None,
            updated_at: None,
        })
        .unwrap();

    storage
        .raw()
        .execute_compat(
            "UPDATE conversations SET source_id = ?1, origin_host = ?2 WHERE id = ?3",
            &[
                ParamValue::from("replayed-remote"),
                ParamValue::from("replayed-host"),
                ParamValue::from(outcome.conversation_id),
            ],
        )
        .unwrap();
    storage
        .raw()
        .execute_compat(
            "UPDATE messages SET role = ?1, content = ?2 WHERE id = ?3",
            &[
                ParamValue::from("tool"),
                ParamValue::from("persisted tool reply"),
                ParamValue::from(assistant_id),
            ],
        )
        .unwrap();

    let (inputs, max_message_id) = packet_semantic_delta_for_outcome(&storage, &outcome).unwrap();
    assert_eq!(max_message_id, Some(assistant_id));
    assert_eq!(inputs.len(), 2);
    assert_eq!(inputs[0].content, "original user");
    assert_eq!(
        crate::indexer::semantic_role_name(inputs[0].role),
        Some("user")
    );
    assert_eq!(inputs[1].content, "persisted tool reply");
    assert_eq!(
        crate::indexer::semantic_role_name(inputs[1].role),
        Some("tool")
    );

    let expected_source_id = crate::search::tantivy::normalized_index_source_id(
        Some("replayed-remote"),
        None,
        Some("replayed-host"),
    );
    let expected_source_hash = crc32fast::hash(expected_source_id.as_bytes());
    assert!(
        inputs
            .iter()
            .all(|input| input.source_id == expected_source_hash),
        "semantic delta should use canonical replay provenance: {inputs:#?}"
    );
}

#[test]
fn apply_index_writer_checkpoint_policy_round_trips_pragma() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("checkpoint-policy.db");
    let storage = create_franken_db(&db_path);

    assert_eq!(storage.index_writer_checkpoint_pages(), Some(4096));

    apply_index_writer_checkpoint_policy(&storage, true);
    let rows = storage.raw().query("PRAGMA wal_autocheckpoint;").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get(0).unwrap(),
        &SqliteValue::Integer(BULK_IMPORT_WAL_AUTOCHECKPOINT_PAGES)
    );
    assert_eq!(
        storage.index_writer_checkpoint_pages(),
        Some(BULK_IMPORT_WAL_AUTOCHECKPOINT_PAGES)
    );

    apply_index_writer_checkpoint_policy(&storage, false);
    let rows = storage.raw().query("PRAGMA wal_autocheckpoint;").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Integer(1000));
    assert_eq!(storage.index_writer_checkpoint_pages(), Some(1000));
}

#[test]
#[serial]
fn gh425_bulk_checkpoint_mode_parses_case_insensitively_and_fails_safe() {
    let passive_guard = set_env("CASS_INDEX_INGEST_WAL_CHECKPOINT_MODE", "passive");
    assert_eq!(
        BulkIngestWalCheckpointMode::from_env(),
        BulkIngestWalCheckpointMode::Passive
    );
    drop(passive_guard);

    let invalid_guard = set_env("CASS_INDEX_INGEST_WAL_CHECKPOINT_MODE", "invalid");
    assert_eq!(
        BulkIngestWalCheckpointMode::from_env(),
        BulkIngestWalCheckpointMode::Truncate
    );
    drop(invalid_guard);
}

#[test]
fn with_ephemeral_writer_marks_preflight_verified_after_first_success() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("preflight-cache.db");
    let storage = create_franken_db(&db_path);

    assert!(!storage.ephemeral_writer_preflight_verified());

    with_ephemeral_writer(&storage, false, "preflight-cache-test", |_writer| Ok(())).unwrap();

    assert!(storage.ephemeral_writer_preflight_verified());

    with_ephemeral_writer(&storage, false, "preflight-cache-test", |_writer| Ok(())).unwrap();

    assert!(storage.ephemeral_writer_preflight_verified());
}

#[test]
fn with_ephemeral_writer_reuses_cached_connection_when_idle() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("ephemeral-writer-reuse.db");
    let storage = create_franken_db(&db_path);

    with_ephemeral_writer(&storage, false, "ephemeral-writer-reuse", |writer| {
        writer
            .raw()
            .execute("CREATE TEMP TABLE temp_writer_reuse(marker INTEGER NOT NULL);")?;
        writer
            .raw()
            .execute("INSERT INTO temp_writer_reuse(marker) VALUES (1);")?;
        Ok(())
    })
    .unwrap();

    let count: i64 = with_ephemeral_writer(&storage, false, "ephemeral-writer-reuse", |writer| {
        Ok(writer
            .raw()
            .query_row_map("SELECT COUNT(*) FROM temp_writer_reuse;", &[], |row| {
                row.get_typed(0)
            })?)
    })
    .unwrap();

    assert_eq!(count, 1, "temp table should persist on the reused writer");
}

#[test]
#[serial]
fn gh425_bulk_checkpoint_resets_wal_and_stays_on_the_primary_writer() {
    let _threshold = set_env("CASS_INDEX_INGEST_WAL_CHECKPOINT_BYTES", "1");
    let _mode = set_env("CASS_INDEX_INGEST_WAL_CHECKPOINT_MODE", "TRUNCATE");
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("gh425-single-writer.db");
    let storage = create_franken_db(&db_path);

    // A TEMP table is connection-local and therefore an exact witness
    // for whether post-reset writes use this primary connection.
    storage
        .raw()
        .execute("CREATE TEMP TABLE gh425_primary_only(value INTEGER NOT NULL);")
        .unwrap();
    with_ephemeral_writer(&storage, true, "gh425 seed cached writer", |writer| {
        writer
            .raw()
            .execute("CREATE TABLE gh425_payload(value INTEGER NOT NULL);")?;
        writer
            .raw()
            .execute("INSERT INTO gh425_payload(value) VALUES (1);")?;
        Ok(())
    })
    .unwrap();

    let mut wal_path = db_path.clone().into_os_string();
    wal_path.push("-wal");
    let wal_path = std::path::PathBuf::from(wal_path);
    let wal_bytes_before = std::fs::metadata(&wal_path)
        .expect("seed write must create a WAL")
        .len();
    assert!(wal_bytes_before > 1);
    assert!(!storage.bulk_single_connection_enabled());

    maybe_checkpoint_bulk_ingest_wal(&storage, true);

    let wal_bytes_after = std::fs::metadata(&wal_path)
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    assert!(
        wal_bytes_after < wal_bytes_before,
        "TRUNCATE must shrink the in-run WAL: before={wal_bytes_before}, after={wal_bytes_after}"
    );
    assert!(storage.bulk_single_connection_enabled());
    assert!(
        !storage.ephemeral_writer_preflight_verified(),
        "the old cached writer's preflight must not be reused by the primary"
    );

    with_ephemeral_writer(&storage, true, "gh425 post-reset primary write", |writer| {
        // This INSERT can succeed only on the original primary: a
        // newly opened or cached second connection cannot see its TEMP
        // table. It also proves the MVCC clock refresh prevented the
        // planted post-TRUNCATE BusySnapshot failure.
        writer
            .raw()
            .execute("INSERT INTO gh425_primary_only(value) VALUES (2);")?;
        Ok(())
    })
    .unwrap();
    assert!(storage.ephemeral_writer_preflight_verified());
    let primary_count: i64 = storage
        .raw()
        .query_row_map("SELECT COUNT(*) FROM gh425_primary_only;", &[], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(primary_count, 1);
}

#[test]
fn gh425_checkpoint_never_steals_an_in_use_ephemeral_writer() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("gh425-in-use-writer.db");
    let storage = create_franken_db(&db_path);

    with_ephemeral_writer(&storage, false, "gh425 in-use witness", |_writer| {
        assert!(storage.cached_ephemeral_writer_in_use());
        assert!(!storage.close_idle_cached_ephemeral_writer().unwrap());
        assert!(storage.cached_ephemeral_writer_in_use());
        Ok(())
    })
    .unwrap();
}

#[test]
#[serial]
fn persist_conversations_batched_can_defer_inline_lexical_updates() {
    use crate::connectors::NormalizedConversation;
    use crate::franken_sync::compat::{ConnectionExt, RowExt};
    use crate::search::tantivy::TantivyIndex;

    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("serial-deferred.db");
    let index_path = dir.path().join("tantivy");

    let storage = create_franken_db(&db_path);
    let mut t_index = TantivyIndex::open_or_create(&index_path).unwrap();
    let convs = vec![NormalizedConversation {
        agent_slug: "serial-agent".into(),
        external_id: Some("serial-1".into()),
        title: Some("Serial Deferred".into()),
        workspace: Some(std::path::PathBuf::from("/ws/serial")),
        source_path: std::path::PathBuf::from("/log/serial.jsonl"),
        started_at: Some(10),
        ended_at: Some(20),
        metadata: serde_json::json!({}),
        messages: vec![
            NormalizedMessage {
                idx: 0,
                role: "user".into(),
                author: Some("tester".into()),
                created_at: Some(10),
                content: "serial deferred first".into(),
                extra: serde_json::json!({}),
                snippets: vec![],
                invocations: Vec::new(),
            },
            NormalizedMessage {
                idx: 1,
                role: "assistant".into(),
                author: Some("tester".into()),
                created_at: Some(11),
                content: "serial deferred second".into(),
                extra: serde_json::json!({}),
                snippets: vec![],
                invocations: Vec::new(),
            },
        ],
    }];

    persist_conversations_batched(
        &storage,
        Some(&mut t_index),
        &convs,
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
        false,
    )
    .expect("serial batched persist should succeed");

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

    assert_eq!(conversation_count, 1);
    assert_eq!(message_count, 2);
    assert_eq!(tantivy_doc_count(&mut t_index), 0);
}

#[test]
fn lexical_population_strategy_prefers_single_authoritative_pass() {
    assert_eq!(
        crate::indexer::select_lexical_population_strategy(false, false),
        LexicalPopulationStrategy::IncrementalInline
    );
    assert_eq!(
        crate::indexer::select_lexical_population_strategy(true, false),
        LexicalPopulationStrategy::InlineRebuildFromScan
    );
    assert_eq!(
        crate::indexer::select_lexical_population_strategy(false, true),
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild
    );
    assert_eq!(
        crate::indexer::select_lexical_population_strategy(true, true),
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild
    );
}

#[test]
fn lexical_population_strategy_reason_covers_full_stale_salvage_and_incremental_modes() {
    assert_eq!(
        crate::indexer::resolve_lexical_population_strategy(false, false, 0),
        (
            LexicalPopulationStrategy::IncrementalInline,
            "incremental_scan_applies_inline_lexical_updates_only_for_new_messages",
        )
    );
    assert_eq!(
        crate::indexer::resolve_lexical_population_strategy(true, false, 0),
        (
            LexicalPopulationStrategy::InlineRebuildFromScan,
            "lexical_index_needs_rebuild_so_scan_results_repopulate_tantivy_directly",
        )
    );
    assert_eq!(
        crate::indexer::resolve_lexical_population_strategy(false, true, 0),
        (
            LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
            "full_refresh_defers_inline_lexical_writes_to_authoritative_db_rebuild",
        )
    );
    assert_eq!(
        crate::indexer::resolve_lexical_population_strategy(true, false, 7),
        (
            LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
            "historical_salvage_imported_messages_require_authoritative_db_rebuild",
        )
    );
}

fn incremental_repair_policy_test_options(
    data_dir: std::path::PathBuf,
    db_path: std::path::PathBuf,
) -> crate::indexer::IndexOptions {
    crate::indexer::IndexOptions {
        full: false,
        force_rebuild: false,
        watch: false,
        watch_once_paths: None,
        db_path,
        data_dir,
        semantic: false,
        build_hnsw: false,
        embedder: "fnv1a-384".to_string(),
        progress: None,
        watch_interval_secs: 30,
    }
}

#[test]
#[serial]
fn large_incremental_authoritative_lexical_repair_defers_by_default() {
    let _max_guard = set_env(
        "CASS_INCREMENTAL_AUTHORITATIVE_LEXICAL_REPAIR_MAX_DB_BYTES",
        "8",
    );
    let _force_guard = set_env("CASS_INCREMENTAL_AUTHORITATIVE_LEXICAL_REPAIR", "0");
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("agent_search.db");
    std::fs::write(&db_path, b"0123456789abcdef").unwrap();
    let opts = incremental_repair_policy_test_options(dir.path().to_path_buf(), db_path.clone());

    let deferred = crate::indexer::should_defer_incremental_authoritative_lexical_repair(
        &opts,
        false,
        42,
        true,
        Some(7),
    )
    .expect("large populated incremental repair should be deferred");

    assert_eq!(deferred.canonical_conversations, 42);
    assert_eq!(deferred.db_size_bytes, 16);
    assert_eq!(deferred.max_automatic_repair_db_size_bytes, 8);
    assert_eq!(deferred.observed_tantivy_docs, Some(7));
    assert_eq!(
        deferred.reason,
        crate::indexer::DEFERRED_LARGE_INCREMENTAL_LEXICAL_REPAIR_REASON
    );
}

/// #289: the run-level probe binding used to hardcode
/// `authoritative_rebuild_required: true`, so every plain populated
/// incremental over the DB-size cap reported a deferred repair probe
/// even when no rebuild was required — which suppressed the
/// end-of-run checkpoint refresh (including the cheap metadata-only
/// bootstrap of a missing checkpoint) forever on large archives.
#[test]
#[serial]
fn large_incremental_repair_probe_not_deferred_when_no_rebuild_required() {
    let _max_guard = set_env(
        "CASS_INCREMENTAL_AUTHORITATIVE_LEXICAL_REPAIR_MAX_DB_BYTES",
        "8",
    );
    let _force_guard = set_env("CASS_INCREMENTAL_AUTHORITATIVE_LEXICAL_REPAIR", "0");
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("agent_search.db");
    std::fs::write(&db_path, b"0123456789abcdef").unwrap();
    let opts = incremental_repair_policy_test_options(dir.path().to_path_buf(), db_path.clone());

    assert_eq!(
        crate::indexer::should_defer_incremental_authoritative_lexical_repair(
            &opts,
            false,
            42,
            false,
            Some(7),
        ),
        None,
        "a healthy lexical index must not report a deferred repair probe; \
         the final checkpoint refresh (and missing-checkpoint bootstrap) depends on it"
    );
}

#[test]
#[serial]
fn small_incremental_authoritative_lexical_repair_keeps_automatic_repair() {
    let _max_guard = set_env(
        "CASS_INCREMENTAL_AUTHORITATIVE_LEXICAL_REPAIR_MAX_DB_BYTES",
        "1024",
    );
    let _force_guard = set_env("CASS_INCREMENTAL_AUTHORITATIVE_LEXICAL_REPAIR", "0");
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("agent_search.db");
    std::fs::write(&db_path, b"tiny").unwrap();
    let opts = incremental_repair_policy_test_options(dir.path().to_path_buf(), db_path.clone());

    assert_eq!(
        crate::indexer::should_defer_incremental_authoritative_lexical_repair(
            &opts, false, 42, true, None,
        ),
        None
    );
}

#[test]
#[serial]
fn explicit_incremental_authoritative_lexical_repair_override_keeps_automatic_repair() {
    let _max_guard = set_env(
        "CASS_INCREMENTAL_AUTHORITATIVE_LEXICAL_REPAIR_MAX_DB_BYTES",
        "1",
    );
    let _force_guard = set_env("CASS_INCREMENTAL_AUTHORITATIVE_LEXICAL_REPAIR", "1");
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("agent_search.db");
    std::fs::write(&db_path, b"0123456789abcdef").unwrap();
    let opts = incremental_repair_policy_test_options(dir.path().to_path_buf(), db_path.clone());

    assert_eq!(
        crate::indexer::should_defer_incremental_authoritative_lexical_repair(
            &opts, false, 42, true, None,
        ),
        None
    );
}

#[test]
#[serial]
fn non_plain_incremental_modes_keep_authoritative_lexical_repair_behavior() {
    let _max_guard = set_env(
        "CASS_INCREMENTAL_AUTHORITATIVE_LEXICAL_REPAIR_MAX_DB_BYTES",
        "1",
    );
    let _force_guard = set_env("CASS_INCREMENTAL_AUTHORITATIVE_LEXICAL_REPAIR", "0");
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("agent_search.db");
    std::fs::write(&db_path, b"0123456789abcdef").unwrap();
    let base = incremental_repair_policy_test_options(dir.path().to_path_buf(), db_path.clone());

    let mut full = base.clone();
    full.full = true;
    assert_eq!(
        crate::indexer::should_defer_incremental_authoritative_lexical_repair(
            &full, false, 42, true, None,
        ),
        None
    );

    let mut force_rebuild = base.clone();
    force_rebuild.force_rebuild = true;
    assert_eq!(
        crate::indexer::should_defer_incremental_authoritative_lexical_repair(
            &force_rebuild,
            false,
            42,
            true,
            None,
        ),
        None
    );

    let mut watch_once = base;
    watch_once.watch_once_paths = Some(vec![dir.path().join("session.jsonl")]);
    assert_eq!(
        crate::indexer::should_defer_incremental_authoritative_lexical_repair(
            &watch_once,
            false,
            42,
            true,
            None,
        ),
        None
    );
}

#[test]
#[serial]
fn large_missing_incremental_scan_watermark_bootstraps_by_default() {
    let _max_guard = set_env(
        "CASS_INCREMENTAL_MISSING_WATERMARK_FULL_SCAN_MAX_DB_BYTES",
        "8",
    );
    let _force_guard = set_env("CASS_INCREMENTAL_MISSING_WATERMARK_FULL_SCAN", "0");
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("agent_search.db");
    std::fs::write(&db_path, b"0123456789abcdef").unwrap();
    let opts = incremental_repair_policy_test_options(dir.path().to_path_buf(), db_path.clone());

    let bootstrap = crate::indexer::should_bootstrap_missing_incremental_scan_watermark(
        &opts, false, 42, false, false, None,
    )
    .expect("large populated incremental run should bootstrap missing scan watermark");

    assert_eq!(bootstrap.canonical_conversations, 42);
    assert_eq!(bootstrap.db_size_bytes, 16);
    assert_eq!(bootstrap.max_automatic_full_scan_db_size_bytes, 8);
    assert_eq!(
        bootstrap.reason,
        crate::indexer::BOOTSTRAP_LARGE_INCREMENTAL_MISSING_WATERMARK_REASON
    );
}

#[test]
#[serial]
fn existing_or_small_missing_incremental_scan_watermark_keeps_full_scan_behavior() {
    let _max_guard = set_env(
        "CASS_INCREMENTAL_MISSING_WATERMARK_FULL_SCAN_MAX_DB_BYTES",
        "1024",
    );
    let _force_guard = set_env("CASS_INCREMENTAL_MISSING_WATERMARK_FULL_SCAN", "0");
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("agent_search.db");
    std::fs::write(&db_path, b"tiny").unwrap();
    let opts = incremental_repair_policy_test_options(dir.path().to_path_buf(), db_path.clone());

    assert_eq!(
        crate::indexer::should_bootstrap_missing_incremental_scan_watermark(
            &opts, false, 42, false, false, None,
        ),
        None
    );
    assert_eq!(
        crate::indexer::should_bootstrap_missing_incremental_scan_watermark(
            &opts,
            false,
            42,
            false,
            false,
            Some(1_700_000_000_000),
        ),
        None
    );
}

#[test]
#[serial]
fn explicit_missing_watermark_full_scan_override_keeps_historical_rescan() {
    let _max_guard = set_env(
        "CASS_INCREMENTAL_MISSING_WATERMARK_FULL_SCAN_MAX_DB_BYTES",
        "1",
    );
    let _force_guard = set_env("CASS_INCREMENTAL_MISSING_WATERMARK_FULL_SCAN", "1");
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("agent_search.db");
    std::fs::write(&db_path, b"0123456789abcdef").unwrap();
    let opts = incremental_repair_policy_test_options(dir.path().to_path_buf(), db_path.clone());

    assert_eq!(
        crate::indexer::should_bootstrap_missing_incremental_scan_watermark(
            &opts, false, 42, false, false, None,
        ),
        None
    );
}

#[test]
#[serial]
fn non_plain_missing_watermark_modes_keep_historical_scan_behavior() {
    let _max_guard = set_env(
        "CASS_INCREMENTAL_MISSING_WATERMARK_FULL_SCAN_MAX_DB_BYTES",
        "1",
    );
    let _force_guard = set_env("CASS_INCREMENTAL_MISSING_WATERMARK_FULL_SCAN", "0");
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("agent_search.db");
    std::fs::write(&db_path, b"0123456789abcdef").unwrap();
    let base = incremental_repair_policy_test_options(dir.path().to_path_buf(), db_path.clone());

    let mut full = base.clone();
    full.full = true;
    assert_eq!(
        crate::indexer::should_bootstrap_missing_incremental_scan_watermark(
            &full, false, 42, false, false, None,
        ),
        None
    );

    let mut force_rebuild = base.clone();
    force_rebuild.force_rebuild = true;
    assert_eq!(
        crate::indexer::should_bootstrap_missing_incremental_scan_watermark(
            &force_rebuild,
            false,
            42,
            false,
            false,
            None,
        ),
        None
    );

    let mut watch_once = base;
    watch_once.watch_once_paths = Some(vec![dir.path().join("session.jsonl")]);
    assert_eq!(
        crate::indexer::should_bootstrap_missing_incremental_scan_watermark(
            &watch_once,
            false,
            42,
            false,
            false,
            None,
        ),
        None
    );
}

#[test]
fn deferred_incremental_authoritative_lexical_repair_is_reported_in_progress_stats() {
    let progress = std::sync::Arc::new(crate::indexer::IndexingProgress::default());
    let deferred = crate::indexer::DeferredIncrementalCanonicalLexicalRepair {
        canonical_conversations: 42,
        db_size_bytes: 16,
        max_automatic_repair_db_size_bytes: 8,
        observed_tantivy_docs: Some(7),
        reason: crate::indexer::DEFERRED_LARGE_INCREMENTAL_LEXICAL_REPAIR_REASON,
    };

    crate::indexer::record_deferred_incremental_canonical_lexical_repair(
        Some(&progress),
        &deferred,
    );

    let stats = progress.stats.lock().unwrap();
    assert!(stats.lexical_update_deferred);
    assert_eq!(
        stats.lexical_repair,
        Some(crate::indexer::LexicalRepairStats {
            kind: "deferred_authoritative_canonical_db_rebuild".to_string(),
            reason: crate::indexer::DEFERRED_LARGE_INCREMENTAL_LEXICAL_REPAIR_REASON.to_string(),
            canonical_conversations: 42,
            canonical_messages: 0,
            observed_tantivy_docs: Some(7),
        })
    );
}

#[test]
fn incremental_canonical_lexical_repair_short_circuits_when_full_or_force_paths_apply() {
    let base = crate::indexer::IncrementalCanonicalLexicalRepairContext {
        full_refresh: false,
        force_rebuild: false,
        resume_lexical_rebuild: false,
        targeted_watch_once_only: false,
        salvage_messages_imported: 0,
        canonical_messages: 0,
        expected_lexical_docs: 0,
        tantivy_requires_rebuild: true,
        observed_tantivy_docs: None,
        region_doc_bound: 0,
        published_index_validated_for_current_data: false,
    };

    assert!(crate::indexer::should_evaluate_incremental_canonical_lexical_repair(&base));
    assert!(
        !crate::indexer::should_evaluate_incremental_canonical_lexical_repair(
            &crate::indexer::IncrementalCanonicalLexicalRepairContext {
                full_refresh: true,
                ..base
            }
        )
    );
    assert!(
        !crate::indexer::should_evaluate_incremental_canonical_lexical_repair(
            &crate::indexer::IncrementalCanonicalLexicalRepairContext {
                force_rebuild: true,
                ..base
            }
        )
    );
    assert!(
        !crate::indexer::should_evaluate_incremental_canonical_lexical_repair(
            &crate::indexer::IncrementalCanonicalLexicalRepairContext {
                resume_lexical_rebuild: true,
                ..base
            }
        )
    );
    assert!(
        !crate::indexer::should_evaluate_incremental_canonical_lexical_repair(
            &crate::indexer::IncrementalCanonicalLexicalRepairContext {
                targeted_watch_once_only: true,
                ..base
            }
        )
    );
    assert!(
        !crate::indexer::should_evaluate_incremental_canonical_lexical_repair(
            &crate::indexer::IncrementalCanonicalLexicalRepairContext {
                salvage_messages_imported: 1,
                ..base
            }
        )
    );
}

#[test]
fn incremental_canonical_lexical_repair_plan_prefers_authoritative_db_for_invalid_tantivy() {
    assert_eq!(
        crate::indexer::choose_incremental_canonical_lexical_repair_plan(
            crate::indexer::IncrementalCanonicalLexicalRepairContext {
                full_refresh: false,
                force_rebuild: false,
                resume_lexical_rebuild: false,
                targeted_watch_once_only: false,
                salvage_messages_imported: 0,
                canonical_messages: 42,
                expected_lexical_docs: 42,
                tantivy_requires_rebuild: true,
                observed_tantivy_docs: None,
                region_doc_bound: 0,
                published_index_validated_for_current_data: false,
            },
        ),
        Some(crate::indexer::IncrementalCanonicalLexicalRepairPlan {
            canonical_messages: 42,
            observed_tantivy_docs: None,
            reason: "incremental_index_repairs_missing_or_invalid_tantivy_from_authoritative_canonical_db_before_scan",
        })
    );
}

#[test]
fn incremental_canonical_lexical_repair_plan_prefers_authoritative_db_for_sparse_tantivy() {
    assert_eq!(
        crate::indexer::choose_incremental_canonical_lexical_repair_plan(
            crate::indexer::IncrementalCanonicalLexicalRepairContext {
                full_refresh: false,
                force_rebuild: false,
                resume_lexical_rebuild: false,
                targeted_watch_once_only: false,
                salvage_messages_imported: 0,
                canonical_messages: 42,
                expected_lexical_docs: 42,
                tantivy_requires_rebuild: false,
                observed_tantivy_docs: Some(3),
                region_doc_bound: 0,
                published_index_validated_for_current_data: false,
            },
        ),
        Some(crate::indexer::IncrementalCanonicalLexicalRepairPlan {
            canonical_messages: 42,
            observed_tantivy_docs: Some(3),
            reason: "incremental_index_repairs_sparse_tantivy_from_authoritative_canonical_db_before_scan",
        })
    );
}

#[test]
fn incremental_canonical_lexical_repair_progress_records_authoritative_repair_stats() {
    let progress = std::sync::Arc::new(crate::indexer::IndexingProgress::default());
    let plan = crate::indexer::IncrementalCanonicalLexicalRepairPlan {
        canonical_messages: 42,
        observed_tantivy_docs: Some(3),
        reason: "incremental_index_repairs_sparse_tantivy_from_authoritative_canonical_db_before_scan",
    };

    crate::indexer::record_incremental_canonical_lexical_repair(Some(&progress), &plan, 7);

    let stats = match progress.stats.lock() {
        Ok(stats) => stats,
        Err(poisoned) => poisoned.into_inner(),
    };
    assert_eq!(
        stats.lexical_repair,
        Some(crate::indexer::LexicalRepairStats {
            kind: "authoritative_canonical_db_rebuild".to_string(),
            reason:
                "incremental_index_repairs_sparse_tantivy_from_authoritative_canonical_db_before_scan"
                    .to_string(),
            canonical_conversations: 7,
            canonical_messages: 42,
            observed_tantivy_docs: Some(3),
        })
    );

    let json = match serde_json::to_value(&*stats) {
        Ok(json) => json,
        Err(err) => {
            panic!("indexing stats should serialize: {err}");
        }
    };
    assert_eq!(
        json["lexical_repair"]["kind"],
        "authoritative_canonical_db_rebuild"
    );
    assert_eq!(json["lexical_repair"]["canonical_conversations"], 7);
    assert_eq!(json["lexical_repair"]["canonical_messages"], 42);
    assert_eq!(json["lexical_repair"]["observed_tantivy_docs"], 3);
}

#[test]
fn incremental_canonical_lexical_repair_plan_stays_incremental_when_tantivy_covers_db() {
    assert_eq!(
        crate::indexer::choose_incremental_canonical_lexical_repair_plan(
            crate::indexer::IncrementalCanonicalLexicalRepairContext {
                full_refresh: false,
                force_rebuild: false,
                resume_lexical_rebuild: false,
                targeted_watch_once_only: false,
                salvage_messages_imported: 0,
                canonical_messages: 42,
                expected_lexical_docs: 42,
                tantivy_requires_rebuild: false,
                observed_tantivy_docs: Some(42),
                region_doc_bound: 0,
                published_index_validated_for_current_data: false,
            },
        ),
        None
    );
}

#[test]
fn incremental_canonical_lexical_repair_plan_region_docs_never_mask_sparse_index() {
    // Task 5: live docs now include converged search-region
    // documents — observed = canonical share + region share. An
    // index missing ONE canonical doc but carrying a region doc
    // reports the expected total: without the bound subtraction
    // the sparse gate would call that healthy and never repair.
    assert_eq!(
        crate::indexer::choose_incremental_canonical_lexical_repair_plan(
            crate::indexer::IncrementalCanonicalLexicalRepairContext {
                full_refresh: false,
                force_rebuild: false,
                resume_lexical_rebuild: false,
                targeted_watch_once_only: false,
                salvage_messages_imported: 0,
                canonical_messages: 42,
                expected_lexical_docs: 42,
                tantivy_requires_rebuild: false,
                // 41 live message docs + 1 region doc = 42 total.
                observed_tantivy_docs: Some(42),
                region_doc_bound: 1,
                published_index_validated_for_current_data: false,
            },
        ),
        Some(crate::indexer::IncrementalCanonicalLexicalRepairPlan {
            canonical_messages: 42,
            observed_tantivy_docs: Some(42),
            reason: "incremental_index_repairs_sparse_tantivy_from_authoritative_canonical_db_before_scan",
        }),
        "a region doc must never substitute for a missing canonical doc"
    );
    // ...and a fully covered index with its region docs stays
    // healthy — the bound splits them out of the comparison.
    assert_eq!(
        crate::indexer::choose_incremental_canonical_lexical_repair_plan(
            crate::indexer::IncrementalCanonicalLexicalRepairContext {
                full_refresh: false,
                force_rebuild: false,
                resume_lexical_rebuild: false,
                targeted_watch_once_only: false,
                salvage_messages_imported: 0,
                canonical_messages: 42,
                expected_lexical_docs: 42,
                tantivy_requires_rebuild: false,
                // 42 live message docs + 2 region docs = 44 total.
                observed_tantivy_docs: Some(44),
                region_doc_bound: 2,
                published_index_validated_for_current_data: false,
            },
        ),
        None,
        "a covered index with region docs is healthy, not sparse"
    );
}

#[test]
fn incremental_canonical_lexical_repair_plan_healthy_when_gap_is_only_hard_noise() {
    // cass#317: the lexical sink drops hard-noise messages (empty /
    // tool-acks), so a perfectly healthy live index has
    // `expected_lexical_docs` docs (< raw `canonical_messages`). The
    // observed doc count matching that noise-adjusted expectation must
    // NOT be treated as sparse — otherwise a corpus with any tool-ack
    // traffic rebuilds authoritatively on every index/watch run.
    assert_eq!(
        crate::indexer::choose_incremental_canonical_lexical_repair_plan(
            crate::indexer::IncrementalCanonicalLexicalRepairContext {
                full_refresh: false,
                force_rebuild: false,
                resume_lexical_rebuild: false,
                targeted_watch_once_only: false,
                salvage_messages_imported: 0,
                // 100 messages, 37 of them hard-noise → 63 expected docs.
                canonical_messages: 100,
                expected_lexical_docs: 63,
                tantivy_requires_rebuild: false,
                // The live index holds exactly the non-noise docs.
                observed_tantivy_docs: Some(63),
                region_doc_bound: 0,
                published_index_validated_for_current_data: false,
            },
        ),
        None,
        "a doc count equal to the noise-adjusted expectation is healthy, not sparse (#317)"
    );
}

#[test]
fn incremental_canonical_lexical_repair_plan_repairs_real_loss_below_noise_expectation() {
    // A genuine loss — observed BELOW the noise-adjusted expectation —
    // must still repair. Subtracting hard-noise must not mask real doc
    // loss: 63 expected, only 40 observed → repair.
    assert_eq!(
        crate::indexer::choose_incremental_canonical_lexical_repair_plan(
            crate::indexer::IncrementalCanonicalLexicalRepairContext {
                full_refresh: false,
                force_rebuild: false,
                resume_lexical_rebuild: false,
                targeted_watch_once_only: false,
                salvage_messages_imported: 0,
                canonical_messages: 100,
                expected_lexical_docs: 63,
                tantivy_requires_rebuild: false,
                observed_tantivy_docs: Some(40),
                region_doc_bound: 0,
                published_index_validated_for_current_data: false,
            },
        ),
        Some(crate::indexer::IncrementalCanonicalLexicalRepairPlan {
            canonical_messages: 100,
            observed_tantivy_docs: Some(40),
            reason: "incremental_index_repairs_sparse_tantivy_from_authoritative_canonical_db_before_scan",
        }),
        "loss below the noise-adjusted expectation must still repair (#317)"
    );
}

#[test]
fn incremental_canonical_lexical_repair_plan_repairs_sparse_live_index_despite_checkpoint() {
    assert_eq!(
        crate::indexer::choose_incremental_canonical_lexical_repair_plan(
            crate::indexer::IncrementalCanonicalLexicalRepairContext {
                full_refresh: false,
                force_rebuild: false,
                resume_lexical_rebuild: false,
                targeted_watch_once_only: false,
                salvage_messages_imported: 0,
                canonical_messages: 42,
                expected_lexical_docs: 42,
                tantivy_requires_rebuild: false,
                observed_tantivy_docs: Some(3),
                region_doc_bound: 0,
                published_index_validated_for_current_data: true,
            },
        ),
        Some(crate::indexer::IncrementalCanonicalLexicalRepairPlan {
            canonical_messages: 42,
            observed_tantivy_docs: Some(3),
            reason: "incremental_index_repairs_sparse_tantivy_from_authoritative_canonical_db_before_scan",
        }),
        "a matching checkpoint is not proof that the current live derived index covers SQLite"
    );

    // The validation flag must NOT mask a genuinely missing/unopenable
    // index: `tantivy_requires_rebuild` still forces a rebuild plan.
    assert!(
        crate::indexer::choose_incremental_canonical_lexical_repair_plan(
            crate::indexer::IncrementalCanonicalLexicalRepairContext {
                full_refresh: false,
                force_rebuild: false,
                resume_lexical_rebuild: false,
                targeted_watch_once_only: false,
                salvage_messages_imported: 0,
                canonical_messages: 42,
                expected_lexical_docs: 42,
                tantivy_requires_rebuild: true,
                observed_tantivy_docs: None,
                region_doc_bound: 0,
                published_index_validated_for_current_data: true,
            },
        )
        .is_some(),
        "a missing/invalid tantivy index must still rebuild even when the validation flag is set"
    );
}
