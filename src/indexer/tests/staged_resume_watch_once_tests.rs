use super::super::*;
use super::helpers::*;
use crate::franken_sync::compat::{ConnectionExt, RowExt};
use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use serial_test::serial;
use tempfile::TempDir;

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
fn rebuild_tantivy_from_db_discards_incomplete_staged_shard_checkpoint_and_restarts() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    seed_lexical_rebuild_fixture(&storage);

    let index_path = index_dir(&data_dir).unwrap();
    std::fs::create_dir_all(&index_path).unwrap();
    let fingerprint = lexical_storage_fingerprint_for_db(&db_path).unwrap();
    std::fs::write(
        lexical_rebuild_state_path(&index_path),
        serde_json::to_vec_pretty(&serde_json::json!({
            "version": LEXICAL_REBUILD_STATE_VERSION,
            "schema_hash": crate::search::tantivy::SCHEMA_HASH,
            "db": {
                "db_path": db_path.display().to_string(),
                "total_conversations": 2,
                "total_messages": 0,
                "storage_fingerprint": fingerprint,
            },
            "page_size": LEXICAL_REBUILD_PAGE_SIZE,
            "committed_offset": 0,
            "committed_conversation_id": null,
            "processed_conversations": 1,
            "indexed_docs": 2,
            "committed_meta_fingerprint": null,
            "pending": {
                "next_offset": 1,
                "next_conversation_id": 1,
                "processed_conversations": 1,
                "indexed_docs": 2,
                "base_meta_fingerprint": null,
            },
            "completed": false,
            "updated_at_ms": FrankenStorage::now_millis(),
            "execution_mode": "staged_shard_build",
            "runtime": {
                "queue_depth": 1,
                "inflight_message_bytes": 128,
                "pending_batch_conversations": 1,
                "pending_batch_message_bytes": 128,
                "page_prep_workers": 1,
                "active_page_prep_jobs": 1,
                "ordered_buffered_pages": 0,
                "budget_generation": 0,
                "host_loadavg_1m_milli": null,
                "controller_mode": "",
                "controller_reason": "",
                "updated_at_ms": FrankenStorage::now_millis(),
            }
        }))
        .unwrap(),
    )
    .unwrap();

    let _prep_profile = set_env("CASS_PREP_PROFILE", "1");
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
            LexicalRebuildStartupOptions::default(),
            None,
        )
        .unwrap();
        assert_eq!(rebuild.indexed_docs - rebuild.indexed_region_docs, 4);
        assert_eq!(rebuild.observed_messages, Some(4));
    });

    assert!(
        logs.contains("discarding non-resumable staged lexical rebuild checkpoint"),
        "expected staged checkpoint reset log, got:\n{logs}"
    );
    for needle in [
        r#"step="restart_from_zero_reset""#,
        r#"step="plan_lexical_shards""#,
        r#"step="start_packet_producer""#,
        r#"step="persist_initial_checkpoint""#,
        r#"step="first_batch_handoff""#,
    ] {
        assert!(
            logs.contains(needle),
            "expected staged restart prep-profile log fragment `{needle}`, got:\n{logs}"
        );
    }
    let restart_from_zero_reset = logs
        .find(r#"step="restart_from_zero_reset""#)
        .expect("restart_from_zero_reset log position");
    let plan_lexical_shards = logs
        .find(r#"step="plan_lexical_shards""#)
        .expect("plan_lexical_shards log position");
    let start_packet_producer = logs
        .find(r#"step="start_packet_producer""#)
        .expect("start_packet_producer log position");
    let persist_initial_checkpoint = logs
        .find(r#"step="persist_initial_checkpoint""#)
        .expect("persist_initial_checkpoint log position");
    let first_batch_handoff = logs
        .find(r#"step="first_batch_handoff""#)
        .expect("first_batch_handoff log position");
    // Bead 9752r: commit 9e6c0ef7 (9ct8r) moved shard planning
    // BEFORE the restart-from-zero reset — the reset now needs to
    // know whether a staged shard plan exists so it can skip the
    // live-index pre-wipe when the staged-shards path will
    // atomically publish instead. Planning still completes before
    // producer startup and initial-checkpoint persistence. The
    // producer can hand off its first page before the main thread
    // logs checkpoint persistence on a fast scheduler, so the
    // checkpoint's final durable contents below are the safety
    // proof rather than that intra-startup log order.
    assert!(
        plan_lexical_shards < restart_from_zero_reset,
        "9ct8r: staged shard planning must run BEFORE the restart-from-zero \
         reset so the reset can decide whether to skip the pre-wipe: {logs}"
    );
    assert!(
        plan_lexical_shards < start_packet_producer,
        "restart-from-zero rebuild should finish shard planning before producer startup overlap: {logs}"
    );
    assert!(
        start_packet_producer < persist_initial_checkpoint,
        "restart-from-zero staged rebuild should overlap producer startup before persisting the fresh startup checkpoint: {logs}"
    );
    assert!(
        start_packet_producer < first_batch_handoff,
        "restart-from-zero rebuild must start the producer before it hands off its first batch: {logs}"
    );
    let checkpoint = load_lexical_rebuild_checkpoint(&index_path)
        .unwrap()
        .expect("completed checkpoint after staged checkpoint reset");
    assert!(checkpoint.completed);
    assert_eq!(checkpoint.processed_conversations, 2);
    assert_eq!(checkpoint.indexed_docs - checkpoint.indexed_region_docs, 4);
    assert_eq!(tantivy_doc_count_for_data_dir(&data_dir), 4);
}

#[test]
#[serial]
fn rebuild_tantivy_from_db_falls_back_for_single_conversation_guardrail_outlier() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);

    let agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: Some("0.2.3".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();
    storage
        .insert_conversation_tree(
            agent_id,
            None,
            &Conversation {
                id: None,
                agent_slug: "codex".into(),
                workspace: Some(PathBuf::from("/tmp/workspace")),
                external_id: Some("oversized-single-conversation".into()),
                title: Some("Oversized single conversation".into()),
                source_path: PathBuf::from("/tmp/oversized-single-conversation.jsonl"),
                started_at: Some(1_700_000_000_000),
                ended_at: Some(1_700_000_000_100),
                approx_tokens: Some(64),
                metadata_json: serde_json::Value::Null,
                messages: vec![
                    Message {
                        id: None,
                        idx: 0,
                        role: MessageRole::User,
                        author: None,
                        created_at: Some(1_700_000_000_010),
                        content: "first".into(),
                        extra_json: serde_json::Value::Null,
                        snippets: Vec::new(),
                    },
                    Message {
                        id: None,
                        idx: 1,
                        role: MessageRole::Agent,
                        author: None,
                        created_at: Some(1_700_000_000_020),
                        content: "second".into(),
                        extra_json: serde_json::Value::Null,
                        snippets: Vec::new(),
                    },
                    Message {
                        id: None,
                        idx: 2,
                        role: MessageRole::Tool,
                        author: None,
                        created_at: Some(1_700_000_000_030),
                        content: "third".into(),
                        extra_json: serde_json::Value::Null,
                        snippets: Vec::new(),
                    },
                ],
                source_id: LOCAL_SOURCE_ID.into(),
                origin_host: None,
            },
        )
        .unwrap();
    drop(storage);

    let _conversation_limit = set_env("CASS_TANTIVY_REBUILD_BATCH_FETCH_CONVERSATIONS", "1");
    let _commit_messages = set_env("CASS_TANTIVY_REBUILD_COMMIT_EVERY_MESSAGES", "2");
    let _initial_commit_messages =
        set_env("CASS_TANTIVY_REBUILD_INITIAL_COMMIT_EVERY_MESSAGES", "2");

    let logs = capture_logs(|| {
        let rebuild = rebuild_tantivy_from_db(&db_path, &data_dir, 1, None).unwrap();
        assert_eq!(rebuild.indexed_docs - rebuild.indexed_region_docs, 3);
        assert_eq!(rebuild.observed_messages, Some(3));
        assert!(rebuild.exact_checkpoint_persisted);
    });

    assert!(
        logs.contains("preparing one conversation at a time"),
        "expected oversized single conversation fallback log, got:\n{logs}"
    );
    assert_eq!(tantivy_doc_count_for_data_dir(&data_dir), 3);
}

/// bgn6s: a conversation larger than one page's content budget is read
/// and indexed in message-range chunks. Every message is searchable,
/// including the last; the conversation after it is indexed too; and the
/// checkpoint certifies. A cumulative per-conversation cap at the
/// per-message cap would have left only the first message searchable.
#[test]
#[serial]
fn rebuild_tantivy_from_db_streams_a_long_conversation_in_chunks() {
    use crate::search::query::{FieldMask, SearchClient, SearchFilters};

    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    let agent_id = storage
        .ensure_agent(&Agent {
            id: None,
            slug: "codex".into(),
            name: "Codex".into(),
            version: Some("0.2.3".into()),
            kind: AgentKind::Cli,
        })
        .unwrap();
    let long_markers = [
        "alphamark",
        "bravomark",
        "charliemark",
        "deltamark",
        "echomark",
        "foxtrotmark",
    ];
    let conversation = |external_id: &str, markers: &[&str]| Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: Some(external_id.into()),
        title: Some(external_id.into()),
        source_path: PathBuf::from(format!("/tmp/{external_id}.jsonl")),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_100),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: markers
            .iter()
            .enumerate()
            .map(|(idx, marker)| Message {
                id: None,
                idx: idx as i64,
                role: MessageRole::User,
                author: None,
                created_at: Some(1_700_000_000_010 + idx as i64),
                content: format!("{marker} steady text"),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            })
            .collect(),
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };
    for (external_id, markers) in [
        ("long-conversation", &long_markers[..]),
        ("short-conversation", &["hotelmark"][..]),
    ] {
        storage
            .insert_conversation_tree(agent_id, None, &conversation(external_id, markers))
            .unwrap();
    }
    drop(storage);

    // A 64-byte page budget holds two 21-byte messages: the long
    // conversation (126 bytes) becomes three chunks.
    let _cap = set_env("CASS_LEXICAL_MAX_MESSAGE_CONTENT_BYTES", "40");
    let _commit_bytes = set_env("CASS_TANTIVY_REBUILD_COMMIT_EVERY_MESSAGE_BYTES", "64");
    let _initial_commit_bytes = set_env(
        "CASS_TANTIVY_REBUILD_INITIAL_COMMIT_EVERY_MESSAGE_BYTES",
        "64",
    );

    let logs = capture_logs(|| {
        let rebuild = rebuild_tantivy_from_db(&db_path, &data_dir, 2, None).unwrap();
        assert_eq!(rebuild.indexed_docs - rebuild.indexed_region_docs, 7);
        assert_eq!(rebuild.observed_messages, Some(7));
        assert!(rebuild.exact_checkpoint_persisted);
    });
    assert!(
        logs.contains("streaming a long conversation in message-range chunks")
            && logs.contains("chunks=3"),
        "expected the long conversation to stream in three chunks, got:\n{logs}"
    );
    assert_eq!(tantivy_doc_count_for_data_dir(&data_dir), 7);
    let checkpoint = load_lexical_rebuild_checkpoint(&index_dir(&data_dir).unwrap())
        .unwrap()
        .expect("completed checkpoint");
    assert!(checkpoint.completed);
    assert_eq!(checkpoint.processed_conversations, 2);
    assert_eq!(checkpoint.indexed_docs - checkpoint.indexed_region_docs, 7);

    let client = SearchClient::open(&index_dir(&data_dir).unwrap(), None)
        .unwrap()
        .expect("search client");
    for marker in long_markers.iter().chain(["hotelmark"].iter()) {
        let hits = client
            .search(marker, SearchFilters::default(), 10, 0, FieldMask::FULL)
            .unwrap();
        assert_eq!(hits.len(), 1, "{marker} must be searchable exactly once");
    }
}

#[test]
#[serial]
fn rebuild_tantivy_from_db_preserves_empty_conversation_gaps_in_stream() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);

    let agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: Some("0.2.3".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();

    let insert_fixture = |external_id: &str, base_ts: i64, messages: Vec<Message>| {
        let conversation = Conversation {
            id: None,
            agent_slug: "codex".into(),
            workspace: Some(PathBuf::from("/tmp/workspace")),
            external_id: Some(external_id.to_string()),
            title: Some("Lexical rebuild fixture".into()),
            source_path: PathBuf::from(format!("/tmp/{external_id}.jsonl")),
            started_at: Some(base_ts),
            ended_at: Some(base_ts + 100),
            approx_tokens: Some(64),
            metadata_json: serde_json::Value::Null,
            messages,
            source_id: LOCAL_SOURCE_ID.into(),
            origin_host: None,
        };
        storage
            .insert_conversation_tree(agent_id, None, &conversation)
            .unwrap();
    };

    insert_fixture(
        "lexical-fixture-1",
        1_700_000_000_000_i64,
        vec![
            Message {
                id: None,
                idx: 0,
                role: MessageRole::User,
                author: Some("user".into()),
                created_at: Some(1_700_000_000_010_i64),
                content: "lexical-fixture-1-first".into(),
                extra_json: serde_json::json!({"opaque": true}),
                snippets: Vec::new(),
            },
            Message {
                id: None,
                idx: 1,
                role: MessageRole::Agent,
                author: Some("assistant".into()),
                created_at: Some(1_700_000_000_020_i64),
                content: "lexical-fixture-1-second".into(),
                extra_json: serde_json::json!({"opaque": true}),
                snippets: Vec::new(),
            },
        ],
    );
    insert_fixture("lexical-fixture-empty", 1_700_000_000_500_i64, Vec::new());
    insert_fixture(
        "lexical-fixture-2",
        1_700_000_001_000_i64,
        vec![
            Message {
                id: None,
                idx: 0,
                role: MessageRole::User,
                author: Some("user".into()),
                created_at: Some(1_700_000_001_010_i64),
                content: "lexical-fixture-2-first".into(),
                extra_json: serde_json::json!({"opaque": true}),
                snippets: Vec::new(),
            },
            Message {
                id: None,
                idx: 1,
                role: MessageRole::Agent,
                author: Some("assistant".into()),
                created_at: Some(1_700_000_001_020_i64),
                content: "lexical-fixture-2-second".into(),
                extra_json: serde_json::json!({"opaque": true}),
                snippets: Vec::new(),
            },
        ],
    );

    let rebuild = rebuild_tantivy_from_db(&db_path, &data_dir, 3, None).unwrap();
    assert_eq!(rebuild.indexed_docs - rebuild.indexed_region_docs, 4);
    assert_eq!(rebuild.observed_messages, Some(4));
    assert_eq!(tantivy_doc_count_for_data_dir(&data_dir), 4);

    let state = load_lexical_rebuild_state(&index_dir(&data_dir).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(state.committed_offset, 3);
    assert_eq!(state.processed_conversations, 3);
    assert!(state.completed);
}

#[test]
fn devin_watch_scan_lower_bound_does_not_filter_delayed_provider_commits() {
    assert_eq!(watch_scan_lower_bound(ConnectorKind::Devin, None), None);
    for input in [0, 1, 999, 1000, 1001, 1_700_000_000_999, i64::MAX] {
        assert_eq!(
            watch_scan_lower_bound(ConnectorKind::Devin, Some(input)),
            None,
            "a filesystem watermark must not exclude an older provider activity timestamp"
        );
        assert_eq!(
            watch_scan_lower_bound(ConnectorKind::Codex, Some(input)),
            Some(input),
            "other connectors retain their existing millisecond scan bound"
        );
    }
}

#[test]
fn classify_devin_database_sidecars_preserves_root_and_rejects_neighbors() {
    let tmp = TempDir::new().unwrap();
    let db = tmp.path().join("sessions.db");
    fs::write(&db, b"source database").unwrap();
    let root = ScanRoot::remote(db.clone(), Origin::remote("devin-host"), None);
    let roots = vec![(ConnectorKind::Devin, root.clone())];
    for sidecar in database_sidecar_paths(&db) {
        fs::write(&sidecar, b"sidecar event").unwrap();
        for explicit in [false, true] {
            let classified = classify_paths(vec![sidecar.clone()], &roots, explicit);
            assert_eq!(classified.len(), 1);
            assert_eq!(classified[0].0, ConnectorKind::Devin);
            let expected = if explicit {
                fs::canonicalize(&db).unwrap()
            } else {
                db.clone()
            };
            assert_eq!(classified[0].1.path, expected);
            assert_eq!(classified[0].1.origin, root.origin);
            assert_eq!(classified[0].1.platform, root.platform);
        }
        assert!(
            classify_paths(
                vec![sidecar],
                &[(ConnectorKind::Codex, root.clone())],
                false,
            )
            .is_empty(),
            "SQLite sidecar routing must stay scoped to Devin"
        );
    }
    for name in ["other.db-wal", "sessions.db-wal-extra", "sessions.db2-wal"] {
        let neighbor = tmp.path().join(name);
        fs::write(&neighbor, b"unrelated event").unwrap();
        for explicit in [false, true] {
            assert!(
                classify_paths(vec![neighbor.clone()], &roots, explicit).is_empty(),
                "unrelated neighbor must not trigger Devin: {name}"
            );
        }
    }
}

#[test]
fn classify_paths_uses_latest_mtime_per_connector() {
    let tmp = TempDir::new().unwrap();
    let codex = tmp.path().join(".codex/sessions/rollout-1.jsonl");
    std::fs::create_dir_all(codex.parent().unwrap()).unwrap();
    std::fs::write(&codex, "{{}}\n{{}}").unwrap();

    let claude = tmp.path().join("project/.claude.json");
    std::fs::create_dir_all(claude.parent().unwrap()).unwrap();
    std::fs::write(&claude, "{{}}").unwrap();

    let cursor = tmp.path().join("Cursor/User/globalStorage/state.vscdb");
    std::fs::create_dir_all(cursor.parent().unwrap()).unwrap();
    std::fs::write(&cursor, b"").unwrap();

    let chatgpt = tmp
        .path()
        .join("Library/Application Support/com.openai.chat/conversations-abc/data.json");
    std::fs::create_dir_all(chatgpt.parent().unwrap()).unwrap();
    std::fs::write(&chatgpt, "{}").unwrap();

    // roots are needed for classify_paths now
    let roots = vec![
        (
            ConnectorKind::Codex,
            ScanRoot::local(tmp.path().join(".codex")),
        ),
        (
            ConnectorKind::Claude,
            ScanRoot::local(tmp.path().join("project")),
        ),
        (
            ConnectorKind::Cursor,
            ScanRoot::local(tmp.path().join("Cursor/User")),
        ),
        (
            ConnectorKind::ChatGpt,
            ScanRoot::local(
                tmp.path()
                    .join("Library/Application Support/com.openai.chat"),
            ),
        ),
    ];

    let paths = vec![codex.clone(), claude.clone(), cursor, chatgpt];
    let classified = classify_paths(paths, &roots, false);

    let kinds: std::collections::HashSet<_> = classified.iter().map(|(k, _, _, _)| *k).collect();
    assert!(kinds.contains(&ConnectorKind::Codex));
    assert!(kinds.contains(&ConnectorKind::Claude));
    assert!(kinds.contains(&ConnectorKind::Cursor));
    assert!(kinds.contains(&ConnectorKind::ChatGpt));

    for (_, _, mtime, _) in classified {
        assert!(mtime.is_some(), "mtime should be captured");
    }
}

#[test]
fn classify_paths_uses_deepest_root_per_connector_and_origin() {
    let tmp = TempDir::new().unwrap();
    let app_root = tmp.path().join("share/codex");
    let profile_root = app_root.join("profiles/work");
    let session = profile_root
        .join("sessions/-projects-cass")
        .join("rollout-2026-08-24T12-00-00.jsonl");
    std::fs::create_dir_all(session.parent().unwrap()).unwrap();
    std::fs::write(&session, b"{}\n").unwrap();
    let roots = vec![
        (ConnectorKind::Codex, ScanRoot::local(app_root)),
        (ConnectorKind::Codex, ScanRoot::local(profile_root.clone())),
    ];

    let classified = classify_paths(vec![session], &roots, false);

    assert_eq!(classified.len(), 1);
    assert_eq!(classified[0].0, ConnectorKind::Codex);
    assert_eq!(classified[0].1.path, profile_root);
}

#[cfg(unix)]
#[test]
fn gh538_classify_paths_prefers_a_physical_descendant_through_a_short_alias() {
    let tmp = TempDir::new().unwrap();
    let parent = tmp.path().join("storage/user/codex");
    let child = parent.join("profiles/work");
    let session = child.join("sessions/session.jsonl");
    std::fs::create_dir_all(session.parent().unwrap()).unwrap();
    std::fs::write(&session, b"{}\n").unwrap();
    let alias = tmp.path().join("work");
    std::os::unix::fs::symlink(&child, &alias).unwrap();
    let origin = Origin::remote_with_host("work-machine", "work-host");
    let nested = ScanRoot::remote(alias, origin.clone(), None);
    let ancestor = ScanRoot::remote(parent.clone(), origin.clone(), None);
    let other = ScanRoot::remote(parent, Origin::remote("other-machine"), None);
    let event = dunce::canonicalize(&session).unwrap();

    for ordered in [
        [ancestor.clone(), nested.clone()],
        [nested.clone(), ancestor.clone()],
    ] {
        let mut roots: Vec<_> = ordered
            .into_iter()
            .map(|root| (ConnectorKind::Codex, root))
            .collect();
        roots.push((ConnectorKind::Codex, other.clone()));
        let classified = classify_paths(vec![event.clone()], &roots, false);
        assert_eq!(classified.len(), 2, "distinct sources must remain distinct");
        for (_, selected, since, watermark) in &classified {
            let expected = if selected.origin == origin {
                &nested
            } else {
                &other
            };
            assert_eq!(selected.path, expected.path);
            assert_eq!(selected.origin, expected.origin);
            assert_eq!(selected.platform, expected.platform);
            assert!(since.is_some() && watermark.is_some());
        }
        let outside = tmp.path().join("outside.jsonl");
        std::fs::write(&outside, b"{}\n").unwrap();
        assert!(classify_paths(vec![outside], &roots, false).is_empty());
    }
}

#[cfg(unix)]
#[test]
fn gh538_classify_paths_does_not_prefer_a_long_alias_to_an_ancestor() {
    let tmp = TempDir::new().unwrap();
    let parent = tmp.path().join("omp");
    let child = parent.join("profiles/work");
    let session = child.join("sessions/session.jsonl");
    std::fs::create_dir_all(session.parent().unwrap()).unwrap();
    std::fs::write(&session, b"{}\n").unwrap();
    let alias = tmp.path().join("long/configured/path/to/shared/omp");
    std::fs::create_dir_all(alias.parent().unwrap()).unwrap();
    std::os::unix::fs::symlink(&parent, &alias).unwrap();
    let ancestor = ScanRoot::local(alias);
    let nested = ScanRoot::local(child);
    let event = dunce::canonicalize(&session).unwrap();

    for ordered in [
        [ancestor.clone(), nested.clone()],
        [nested.clone(), ancestor.clone()],
    ] {
        let roots: Vec<_> = ordered
            .into_iter()
            .map(|root| (ConnectorKind::Codex, root))
            .collect();
        let classified = classify_paths(vec![event.clone()], &roots, false);
        assert_eq!(classified.len(), 1, "one scan per connector and source");
        assert_eq!(classified[0].0, ConnectorKind::Codex);
        assert_eq!(classified[0].1.path, nested.path);
        assert_eq!(classified[0].1.origin, nested.origin);
    }
}

/// A live watch must match an event in the canonical spelling of its root:
/// macOS FSEvents reports /private/var/... for a root under /var, and the
/// target path for a symlinked root, so `cass index --watch` on macOS
/// dropped every such event. The scan still uses the configured root, so
/// source paths keep the spelling a full scan records.
#[cfg(unix)]
#[test]
fn classify_paths_live_watch_matches_a_symlinked_root_by_its_canonical_path() {
    let tmp = TempDir::new().unwrap();
    let real = tmp.path().join("real-sessions");
    let session = real.join("rollout-2026-10-06T12-00-00.jsonl");
    std::fs::create_dir_all(&real).unwrap();
    std::fs::write(&session, b"{}\n").unwrap();
    let linked = tmp.path().join("linked-sessions");
    std::os::unix::fs::symlink(&real, &linked).unwrap();
    let roots = vec![(ConnectorKind::Codex, ScanRoot::local(linked.clone()))];

    // The event arrives under the symlink's target, as FSEvents reports it.
    let event = std::fs::canonicalize(&session).unwrap();
    let classified = classify_paths(vec![event], &roots, false);
    assert_eq!(classified.len(), 1, "{classified:?}");
    assert_eq!(classified[0].1.path, linked, "scan the configured root");

    // A path under neither spelling is not claimed.
    let outside = tmp.path().join("elsewhere.jsonl");
    std::fs::write(&outside, b"{}\n").unwrap();
    assert!(classify_paths(vec![outside], &roots, false).is_empty());
}

/// s1nqh: FSEvents reports nothing for a provider's WAL commits while the
/// provider keeps the `-wal` open, so the database root is also polled.
#[test]
fn database_root_poller_queues_held_open_wal_writes_and_ignores_neighbours() {
    use std::io::Write;

    let tmp = TempDir::new().unwrap();
    let database = tmp.path().join("sessions.db");
    std::fs::write(&database, b"database").unwrap();
    let neighbour = tmp.path().join("notes.txt");
    std::fs::write(&neighbour, b"before").unwrap();
    let backlog = Arc::new(WatchNotificationMailbox::default());
    let rescan = Arc::new(AtomicBool::new(false));
    let (wake, woken) = bounded(1);

    let directory_root = vec![(
        ConnectorKind::Codex,
        ScanRoot::local(tmp.path().to_path_buf()),
    )];
    let mut native_watcher = recommended_watcher(|_| {}).unwrap();
    let mut registered = BTreeMap::new();
    let directory_selection = refresh_watch_root_registrations(
        &mut native_watcher,
        &directory_root,
        &mut registered,
        None,
    )
    .unwrap();
    assert!(directory_selection.database_roots.is_empty());
    let (poller, retry) = database_root_poller(
        &directory_selection.database_roots,
        Duration::from_millis(50),
        Arc::clone(&backlog),
        Arc::clone(&rescan),
        wake.clone(),
    );
    assert!(poller.is_none(), "only database roots are polled");
    assert!(
        !retry,
        "an empty database selection needs no registration retry"
    );

    let roots = vec![(ConnectorKind::Devin, ScanRoot::local(database.clone()))];
    let database_selection =
        refresh_watch_root_registrations(&mut native_watcher, &roots, &mut registered, None)
            .unwrap();
    assert_eq!(
        database_selection.database_roots,
        BTreeSet::from([database.clone()])
    );
    let (poller, retry) = database_root_poller(
        &database_selection.database_roots,
        Duration::from_millis(50),
        Arc::clone(&backlog),
        Arc::clone(&rescan),
        wake,
    );
    assert!(
        !retry,
        "an available database parent is registered immediately"
    );
    let _poller = poller.expect("a database root is polled");
    std::thread::sleep(Duration::from_millis(300));

    std::fs::write(&neighbour, b"after, and longer").unwrap();
    let [wal, _shm] = database_sidecar_paths(&database);
    let mut provider = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&wal)
        .unwrap();
    provider.write_all(b"commit frame").unwrap();
    provider.sync_all().unwrap();

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut queued = WatchEventBacklog::default();
    let mut pending_rescan = false;
    while !queued.paths.contains(&wal) {
        woken
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .expect("the WAL commit must wake the watch loop");
        collect_watch_notifications(&backlog, &rescan, &mut queued, &mut pending_rescan);
    }
    assert!(!pending_rescan, "a retained WAL event stays scoped");
    assert!(!queued.paths.contains(&neighbour), "{:?}", queued.paths);
    assert!(!rescan.load(Ordering::Acquire));
    drop(provider);
}

#[test]
fn classify_paths_keeps_distinct_source_provenance_for_one_mirror_path() {
    let tmp = TempDir::new().unwrap();
    let mirror_root = tmp.path().join("shared-mirror");
    let session = mirror_root.join("session.jsonl");
    std::fs::create_dir_all(&mirror_root).unwrap();
    std::fs::write(&session, b"{}\n").unwrap();
    let roots = vec![
        (
            ConnectorKind::Codex,
            ScanRoot::remote(mirror_root.clone(), Origin::remote("source-a"), None),
        ),
        (
            ConnectorKind::Codex,
            ScanRoot::remote(mirror_root, Origin::remote("source-b"), None),
        ),
    ];

    let classified = classify_paths(vec![session], &roots, false);
    let source_ids = classified
        .iter()
        .map(|(_, root, _, _)| root.origin.source_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let expected = std::collections::BTreeSet::from(["source-a", "source-b"]);

    assert_eq!(classified.len(), 2);
    assert_eq!(source_ids, expected);
}

#[test]
fn classify_paths_prefers_explicit_watch_once_paths() {
    let tmp = tempfile::tempdir().unwrap();
    let project_root = tmp.path().join("project");
    let session = project_root.join("subagents").join("session.jsonl");
    std::fs::create_dir_all(session.parent().unwrap()).unwrap();
    std::fs::write(&session, b"{}").unwrap();

    let roots = vec![(ConnectorKind::Claude, ScanRoot::local(project_root.clone()))];

    let classified = classify_paths(vec![session.clone()], &roots, true);

    assert_eq!(classified.len(), 1);
    assert_eq!(classified[0].0, ConnectorKind::Claude);
    // The plain spelling a scan records, not Windows' verbatim `\\?\`
    // (9v9xi); std::fs::canonicalize everywhere else.
    assert_eq!(classified[0].1.path, dunce::canonicalize(session).unwrap());
}

#[test]
fn classify_paths_hints_codex_connector_for_explicit_codex_paths() {
    let tmp = tempfile::tempdir().unwrap();
    let codex_root = tmp.path().join(".codex").join("sessions");
    let session = codex_root.join("2026").join("03").join("rollout-1.jsonl");
    std::fs::create_dir_all(session.parent().unwrap()).unwrap();
    std::fs::write(&session, b"{}").unwrap();

    let roots = vec![
        (ConnectorKind::Codex, ScanRoot::local(codex_root.clone())),
        (ConnectorKind::Claude, ScanRoot::local(codex_root.clone())),
        (ConnectorKind::Gemini, ScanRoot::local(codex_root)),
    ];

    let classified = classify_paths(vec![session.clone()], &roots, true);

    assert_eq!(classified.len(), 1);
    assert_eq!(classified[0].0, ConnectorKind::Codex);
    assert_eq!(classified[0].1.path, dunce::canonicalize(session).unwrap());
}

#[test]
fn classify_paths_gives_database_connectors_only_sqlite_watch_once_files() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("sessions");
    std::fs::create_dir_all(&root).unwrap();
    let rollout =
        root.join("rollout-2026-10-13T16-20-00-00000000-0000-4000-8000-000000000001.jsonl");
    std::fs::write(&rollout, b"{}\n").unwrap();
    let database = root.join("chats.db");
    std::fs::write(&database, b"SQLite format 3\0rest of the header").unwrap();
    let roots = vec![
        (ConnectorKind::Codex, ScanRoot::local(root.clone())),
        (ConnectorKind::Devin, ScanRoot::local(root.clone())),
    ];
    let kinds = |path: &PathBuf, explicit: bool| -> Vec<ConnectorKind> {
        classify_paths(vec![path.clone()], &roots, explicit)
            .into_iter()
            .map(|(kind, ..)| kind)
            .collect()
    };

    // An unhinted rollout under a shared root reaches Codex only; Devin
    // would open it as its database and fail the run (GH #524).
    assert_eq!(kinds(&rollout, true), vec![ConnectorKind::Codex]);
    // A SQLite file under the same root still reaches Devin.
    let database_kinds = kinds(&database, true);
    assert!(database_kinds.contains(&ConnectorKind::Devin));
    // A live watch event keeps scanning the configured root for every kind.
    assert_eq!(kinds(&rollout, false).len(), 2);
}

#[test]
fn classify_paths_keeps_explicit_codex_path_without_detected_root() {
    let tmp = tempfile::tempdir().unwrap();
    let session = tmp
        .path()
        .join(".codex")
        .join("sessions")
        .join("2026")
        .join("03")
        .join("rollout-1.jsonl");
    std::fs::create_dir_all(session.parent().unwrap()).unwrap();
    std::fs::write(&session, b"{}").unwrap();

    let classified = classify_paths(vec![session.clone()], &[], true);

    assert_eq!(classified.len(), 1);
    assert_eq!(classified[0].0, ConnectorKind::Codex);
    assert_eq!(classified[0].1.path, dunce::canonicalize(session).unwrap());
    assert!(classified[0].2.is_some());
    assert!(classified[0].3.is_some());
}

#[cfg(unix)]
#[test]
fn classify_paths_keeps_codex_hint_across_symlinks_and_deduplicates_aliases() {
    let tmp = tempfile::tempdir().unwrap();
    let real = tmp.path().join("archive/2026/01/rollout.jsonl");
    std::fs::create_dir_all(real.parent().unwrap()).unwrap();
    std::fs::write(&real, b"{}\n").unwrap();
    let linked_dir = tmp.path().join(".codex/sessions/2026/01");
    std::fs::create_dir_all(linked_dir.parent().unwrap()).unwrap();
    std::os::unix::fs::symlink(real.parent().unwrap(), &linked_dir).unwrap();
    let linked = linked_dir.join("rollout.jsonl");
    let canonical = std::fs::canonicalize(&real).unwrap();
    let classified = classify_paths(vec![linked.clone(), linked, canonical.clone()], &[], true);

    assert_eq!(classified.len(), 1);
    assert_eq!(classified[0].0, ConnectorKind::Codex);
    assert_eq!(classified[0].1.path, canonical);
    assert!(classified[0].2.is_some());
    assert!(classified[0].3.is_some());
}

/// GH #478 follow-up: only existing paths that no connector claims are
/// reported. Claimed paths (by root or by provider hint) and absent paths
/// (the deliberate hook fast path) are not.
#[test]
fn unclaimed_explicit_watch_once_paths_names_only_existing_unclaimed_files() {
    let tmp = tempfile::tempdir().unwrap();
    let rooted = tmp.path().join("sessions-root/session.jsonl");
    let hinted = tmp.path().join(".codex/sessions/2026/01/rollout-a.jsonl");
    let unclaimed = tmp.path().join("notes/plain.jsonl");
    for path in [&rooted, &hinted, &unclaimed] {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, b"{}\n").unwrap();
    }
    let absent = tmp.path().join("notes/not-written-yet.jsonl");
    let roots = vec![(
        ConnectorKind::Claude,
        ScanRoot::local(tmp.path().join("sessions-root")),
    )];

    assert_eq!(
        unclaimed_explicit_watch_once_paths(
            &[rooted.clone(), hinted.clone(), unclaimed.clone(), absent],
            &roots
        ),
        vec![unclaimed]
    );
    assert!(unclaimed_explicit_watch_once_paths(&[rooted, hinted], &roots).is_empty());
}

#[cfg(unix)]
#[test]
fn classify_paths_recovers_canonical_hint_and_matches_canonical_roots() {
    let tmp = tempfile::tempdir().unwrap();
    let real = tmp.path().join(".codex/sessions/rollout.jsonl");
    std::fs::create_dir_all(real.parent().unwrap()).unwrap();
    std::fs::write(&real, b"{}\n").unwrap();
    let link = tmp.path().join("unmarked-alias.jsonl");
    std::os::unix::fs::symlink(&real, &link).unwrap();
    let canonical = std::fs::canonicalize(&real).unwrap();
    let root = ScanRoot::local(canonical.parent().unwrap().to_path_buf());

    for roots in [Vec::new(), vec![(ConnectorKind::Codex, root)]] {
        let classified = classify_paths(vec![link.clone()], &roots, true);
        assert_eq!(classified.len(), 1);
        assert_eq!(classified[0].0, ConnectorKind::Codex);
        assert_eq!(classified[0].1.path, canonical);
    }
}

#[cfg(unix)]
#[test]
fn classify_paths_preserves_remote_provenance_through_a_symlinked_root() {
    let tmp = tempfile::tempdir().unwrap();
    let real_root = tmp.path().join("archive");
    std::fs::create_dir_all(&real_root).unwrap();
    let session = real_root.join("session.jsonl");
    std::fs::write(&session, b"{}\n").unwrap();
    let linked_root = tmp.path().join("mirror-alias");
    std::os::unix::fs::symlink(&real_root, &linked_root).unwrap();
    let roots = vec![
        (
            ConnectorKind::Claude,
            ScanRoot::remote(linked_root.clone(), Origin::remote("source-a"), None),
        ),
        (
            ConnectorKind::Claude,
            ScanRoot::remote(linked_root.clone(), Origin::remote("source-b"), None),
        ),
    ];
    let classified = classify_paths(vec![linked_root.join("session.jsonl")], &roots, true);
    let canonical = std::fs::canonicalize(&session).unwrap();
    assert_eq!(classified.len(), 2);
    for (kind, root, _, _) in &classified {
        assert_eq!(*kind, ConnectorKind::Claude);
        assert_eq!(root.path, canonical);
    }
    let source_ids = classified
        .iter()
        .map(|(_, root, _, _)| root.origin.source_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(source_ids, ["source-a", "source-b"].into_iter().collect());
}

#[test]
#[serial]
fn reindex_paths_watch_once_indexes_explicit_codex_path_without_detected_root() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let session = tmp
        .path()
        .join(".codex")
        .join("sessions")
        .join("2026")
        .join("05")
        .join("08")
        .join("rollout-explicit-watch-once.jsonl");
    std::fs::create_dir_all(session.parent().unwrap()).unwrap();
    std::fs::write(
        &session,
        r#"{"timestamp":"2026-05-08T23:09:00.000Z","type":"session_meta","payload":{"id":"explicit-watch-once","cwd":"/data/projects/ntm"}}
{"timestamp":"2026-05-08T23:09:01.000Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"bd-2mb03 explicit watch once"}]}}
{"timestamp":"2026-05-08T23:09:02.000Z","type":"response_item","payload":{"type":"function_call_output","call_id":"call-explicit","output":"bd-2mb03 explicit tool output\n"}}
"#,
    )
    .unwrap();

    let opts = crate::indexer::IndexOptions {
        full: false,
        watch: false,
        force_rebuild: false,
        watch_once_paths: Some(vec![session.clone()]),
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
    let state = Mutex::new(HashMap::new());
    let storage = Mutex::new(storage);
    let t_index = Mutex::new(None);

    let indexed = reindex_paths(
        &opts,
        vec![session],
        &[],
        &state,
        &storage,
        &t_index,
        &index_path,
        false,
    )
    .unwrap();

    assert_eq!(indexed, 1);
    let message_count: i64 = storage
        .lock()
        .unwrap()
        .raw()
        .query_row_map("SELECT COUNT(*) FROM messages", &[], |row| row.get_typed(0))
        .unwrap();
    assert_eq!(message_count, 2);
}

#[test]
#[serial]
fn connector_diagnostics_mixed_corpus_indexes_valid_records_and_quarantines_bad_line() {
    use crate::search::query::{FieldMask, SearchClient, SearchFilters};

    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let codex_session = tmp
        .path()
        .join(".codex/sessions/2026/07/22/rollout-mixed.jsonl");
    std::fs::create_dir_all(codex_session.parent().unwrap()).unwrap();
    std::fs::write(
        &codex_session,
        r#"{"timestamp":"2026-07-22T06:00:00.000Z","type":"session_meta","payload":{"id":"mixed-codex","cwd":"/workspace/mixed"}}
{"timestamp":"2026-07-22T06:00:01.000Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"stmh6-codex-searchable"}]}}
{malformed connector line}
{"timestamp":"2026-07-22T06:00:02.000Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"stmh6-codex-tail-searchable"}]}}
"#,
    )
    .unwrap();

    let claude_projects = tmp.path().join(".claude/projects/-workspace-mixed");
    std::fs::create_dir_all(&claude_projects).unwrap();
    let claude_session = claude_projects.join("未来-session-名前.jsonl");
    std::fs::write(
        &claude_session,
        r#"{"parentUuid":null,"cwd":"/workspace/mixed","sessionId":"mixed-claude","version":"2.0.37","type":"user","message":{"role":"user","content":"stmh6-claude-searchable"},"uuid":"msg-001","timestamp":"2026-07-22T06:00:00.000Z"}
"#,
    )
    .unwrap();

    let progress = Arc::new(IndexingProgress::default());
    let opts = crate::indexer::IndexOptions {
        full: false,
        watch: false,
        force_rebuild: false,
        watch_once_paths: Some(vec![codex_session.clone(), claude_session.clone()]),
        db_path: data_dir.join("db.sqlite"),
        data_dir: data_dir.clone(),
        semantic: false,
        build_hnsw: false,
        embedder: "fastembed".to_string(),
        progress: Some(Arc::clone(&progress)),
        watch_interval_secs: 30,
    };
    let storage = FrankenStorage::open(&opts.db_path).unwrap();
    let index_path = index_dir(&opts.data_dir).unwrap();
    let state = Mutex::new(HashMap::new());
    let storage = Mutex::new(storage);
    let t_index = Mutex::new(None);
    let roots = vec![
        (
            ConnectorKind::Codex,
            ScanRoot::local(codex_session.parent().unwrap().to_path_buf()),
        ),
        (ConnectorKind::Claude, ScanRoot::local(claude_projects)),
    ];

    let indexed = reindex_paths(
        &opts,
        vec![codex_session, claude_session],
        &roots,
        &state,
        &storage,
        &t_index,
        &index_path,
        false,
    )
    .unwrap();
    assert_eq!(indexed, 2);

    let message_count: i64 = storage
        .lock()
        .unwrap()
        .raw()
        .query_row_map("SELECT COUNT(*) FROM messages", &[], |row| row.get_typed(0))
        .unwrap();
    assert_eq!(message_count, 3);

    let stats = progress.stats.lock().unwrap();
    let codex = stats.connector_summary.get("codex").unwrap();
    assert_eq!(codex.partially_indexed, 1);
    let claude = stats.connector_summary.get("claude").unwrap();
    assert_eq!(claude.indexed, 1);
    assert_eq!(stats.connector_diagnostics.len(), 1);
    assert_eq!(
        stats.connector_diagnostics[0].failure_kind,
        crate::connector_ingest_diagnostics::IngestFailureKind::MalformedJsonLine
    );
    assert_eq!(stats.connector_diagnostics[0].line_or_row, Some(3));
    drop(stats);
    drop(t_index);

    let client = SearchClient::open(&index_path, None).unwrap().unwrap();
    for marker in ["stmh6-codex-tail-searchable", "stmh6-claude-searchable"] {
        let hits = client
            .search(marker, SearchFilters::default(), 10, 0, FieldMask::FULL)
            .unwrap();
        assert!(
            !hits.is_empty(),
            "missing searchable mixed-corpus marker: {marker}"
        );
    }

    let quarantine =
        std::fs::read_to_string(data_dir.join("quarantine/connector_ingest_lines.json")).unwrap();
    assert!(!quarantine.contains("{malformed connector line}"));
    assert!(quarantine.contains("payload_blake3"));
}

#[test]
#[serial]
fn run_index_watch_once_reindexes_changed_explicit_codex_path_already_in_db() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let session = tmp
        .path()
        .join(".codex")
        .join("sessions")
        .join("2026")
        .join("05")
        .join("08")
        .join("rollout-explicit-watch-once-repeat.jsonl");
    std::fs::create_dir_all(session.parent().unwrap()).unwrap();
    let initial = r#"{"timestamp":"2026-05-08T23:09:00.000Z","type":"session_meta","payload":{"id":"explicit-watch-once-repeat","cwd":"/data/projects/ntm"}}
{"timestamp":"2026-05-08T23:09:01.000Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"bd-2mb03 repeat first"}]}}
"#;
    std::fs::write(&session, initial).unwrap();

    let opts = |data_dir: &Path, session: &Path| crate::indexer::IndexOptions {
        full: false,
        watch: false,
        force_rebuild: false,
        watch_once_paths: Some(vec![session.to_path_buf()]),
        db_path: data_dir.join("db.sqlite"),
        data_dir: data_dir.to_path_buf(),
        semantic: false,
        build_hnsw: false,
        embedder: "fastembed".to_string(),
        progress: None,
        watch_interval_secs: 30,
    };

    run_index(opts(&data_dir, &session), None).unwrap();

    std::thread::sleep(std::time::Duration::from_millis(25));
    std::fs::write(
        &session,
        format!(
            "{initial}{}",
            r#"{"timestamp":"2026-05-08T23:09:02.000Z","type":"response_item","payload":{"type":"function_call_output","call_id":"call-repeat","output":"bd-2mb03 repeat second\n"}}
"#,
        ),
    )
    .unwrap();

    run_index(opts(&data_dir, &session), None).unwrap();

    let storage = FrankenStorage::open(&data_dir.join("db.sqlite")).unwrap();
    let message_count: i64 = storage
        .raw()
        .query_row_map("SELECT COUNT(*) FROM messages", &[], |row| row.get_typed(0))
        .unwrap();
    assert_eq!(message_count, 2);
}
