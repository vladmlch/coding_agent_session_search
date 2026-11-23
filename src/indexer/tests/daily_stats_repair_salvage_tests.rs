use super::super::*;
use super::helpers::*;
use crate::franken_sync::compat::{ConnectionExt, ParamValue, RowExt};
use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use serial_test::serial;
use tempfile::TempDir;

#[test]
fn repair_daily_stats_if_drifted_rebuilds_materialized_totals() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    storage.run_migrations().unwrap();

    let agent = crate::model::types::Agent {
        id: None,
        slug: "tester".into(),
        name: "Tester".into(),
        version: None,
        kind: crate::model::types::AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();
    let conversation = crate::model::types::Conversation {
        id: None,
        agent_slug: "tester".into(),
        workspace: Some(std::path::PathBuf::from("/tmp/workspace")),
        external_id: Some("daily-stats-repair".into()),
        title: Some("repair".into()),
        source_path: std::path::PathBuf::from("/tmp/repair.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_100),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: vec![crate::model::types::Message {
            id: None,
            idx: 0,
            role: crate::model::types::MessageRole::User,
            author: None,
            created_at: Some(1_700_000_000_000),
            content: "hello".into(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        source_id: "local".into(),
        origin_host: None,
    };
    storage
        .insert_conversations_batched(&[(agent_id, None, &conversation)])
        .unwrap();

    storage.raw().execute("DELETE FROM daily_stats").unwrap();
    storage
        .raw()
        .execute(
            "INSERT INTO daily_stats(day_id, agent_slug, source_id, session_count, message_count, total_chars, last_updated)
             VALUES(0, 'all', 'all', 99, 99, 99, 0)",
        )
        .unwrap();

    let before = storage.daily_stats_health().unwrap();
    assert_eq!(before.materialized_total, 99);
    assert!(before.drift > 0);

    assert_eq!(
        repair_daily_stats_if_drifted(&storage, &db_path, None).unwrap(),
        DailyStatsRepairOutcome::Rebuilt {
            rows_created: 4,
            total_sessions: 1,
        }
    );
    let after = storage.daily_stats_health().unwrap();
    assert_eq!(after.conversation_count, 1);
    assert_eq!(after.materialized_total, 1);
    assert_eq!(after.drift, 0);
}

#[test]
#[serial]
fn repair_daily_stats_if_drifted_bypasses_the_unbounded_packet_rebuild() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    storage.run_migrations().unwrap();

    let agent = crate::model::types::Agent {
        id: None,
        slug: "tester".into(),
        name: "Tester".into(),
        version: None,
        kind: crate::model::types::AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();
    let conversation = crate::model::types::Conversation {
        id: None,
        agent_slug: "tester".into(),
        workspace: Some(std::path::PathBuf::from("/tmp/workspace")),
        external_id: Some("daily-stats-fallback".into()),
        title: Some("fallback".into()),
        source_path: std::path::PathBuf::from("/tmp/fallback.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_100),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: vec![crate::model::types::Message {
            id: None,
            idx: 0,
            role: crate::model::types::MessageRole::User,
            author: None,
            created_at: Some(1_700_000_000_000),
            content: "hello".into(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        source_id: "local".into(),
        origin_host: None,
    };
    storage
        .insert_conversations_batched(&[(agent_id, None, &conversation)])
        .unwrap();
    storage.raw().execute("DELETE FROM daily_stats").unwrap();
    storage
        .raw()
        .execute(
            "INSERT INTO daily_stats(day_id, agent_slug, source_id, session_count, message_count, total_chars, last_updated)
             VALUES(0, 'all', 'all', 99, 99, 99, 0)",
        )
        .unwrap();

    let _oom_guard = set_env("CASS_TEST_PACKET_DAILY_STATS_REBUILD_OOM", "1");
    // #329: this fault injection would fail the legacy packet path. The
    // production repair must no longer enter it at all.
    assert_eq!(
        repair_daily_stats_if_drifted(&storage, &db_path, None).unwrap(),
        DailyStatsRepairOutcome::Rebuilt {
            rows_created: 4,
            total_sessions: 1,
        }
    );
    let after = storage.daily_stats_health().unwrap();
    assert_eq!(after.materialized_total, 1);
    assert_eq!(after.drift, 0);
}

#[test]
fn packet_daily_stats_projection_matches_streaming_storage_rebuild() {
    fn load_daily_stats_rows(
        storage: &FrankenStorage,
    ) -> Vec<(i64, String, String, i64, i64, i64)> {
        storage
            .raw()
            .query_map_collect(
                "SELECT day_id, agent_slug, source_id, session_count, message_count, total_chars
                 FROM daily_stats
                 ORDER BY day_id, agent_slug, source_id",
                &[] as &[ParamValue],
                |row| {
                    Ok((
                        row.get_typed(0)?,
                        row.get_typed(1)?,
                        row.get_typed(2)?,
                        row.get_typed(3)?,
                        row.get_typed(4)?,
                        row.get_typed(5)?,
                    ))
                },
            )
            .unwrap()
    }

    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    storage.run_migrations().unwrap();

    let tester = crate::model::types::Agent {
        id: None,
        slug: "tester".into(),
        name: "Tester".into(),
        version: None,
        kind: crate::model::types::AgentKind::Cli,
    };
    let reviewer = crate::model::types::Agent {
        id: None,
        slug: "reviewer".into(),
        name: "Reviewer".into(),
        version: None,
        kind: crate::model::types::AgentKind::Cli,
    };
    let tester_id = storage.ensure_agent(&tester).unwrap();
    let reviewer_id = storage.ensure_agent(&reviewer).unwrap();

    let conv_local = crate::model::types::Conversation {
        id: None,
        agent_slug: "tester".into(),
        workspace: Some(std::path::PathBuf::from("/tmp/workspace-local")),
        external_id: Some("daily-stats-local".into()),
        title: Some("local".into()),
        source_path: std::path::PathBuf::from("/tmp/local.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_500),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: vec![
            crate::model::types::Message {
                id: None,
                idx: 0,
                role: crate::model::types::MessageRole::User,
                author: None,
                created_at: Some(1_700_000_000_000),
                content: "hello".into(),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            },
            crate::model::types::Message {
                id: None,
                idx: 1,
                role: crate::model::types::MessageRole::Tool,
                author: None,
                created_at: Some(1_700_000_000_100),
                content: String::new(),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            },
            crate::model::types::Message {
                id: None,
                idx: 2,
                role: crate::model::types::MessageRole::Agent,
                author: None,
                created_at: Some(1_700_000_000_200),
                content: "done".into(),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            },
        ],
        source_id: "local".into(),
        origin_host: None,
    };
    let conv_remote = crate::model::types::Conversation {
        id: None,
        agent_slug: "reviewer".into(),
        workspace: Some(std::path::PathBuf::from("/tmp/workspace-remote")),
        external_id: Some("daily-stats-remote".into()),
        title: Some("remote".into()),
        source_path: std::path::PathBuf::from("/tmp/remote.jsonl"),
        started_at: Some(1_700_086_400_000),
        ended_at: Some(1_700_086_400_800),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: vec![
            crate::model::types::Message {
                id: None,
                idx: 0,
                role: crate::model::types::MessageRole::System,
                author: None,
                created_at: Some(1_700_086_400_000),
                content: "prep".into(),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            },
            crate::model::types::Message {
                id: None,
                idx: 1,
                role: crate::model::types::MessageRole::Other("narrator".into()),
                author: None,
                created_at: Some(1_700_086_400_050),
                content: "note".into(),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            },
            crate::model::types::Message {
                id: None,
                idx: 2,
                role: crate::model::types::MessageRole::User,
                author: None,
                created_at: Some(1_700_086_400_100),
                content: "go".into(),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            },
        ],
        source_id: "builder.example.com".into(),
        origin_host: Some("builder.example.com".into()),
    };

    storage
        .insert_conversations_batched(&[
            (tester_id, None, &conv_local),
            (reviewer_id, None, &conv_remote),
        ])
        .unwrap();

    storage.raw().execute("DELETE FROM daily_stats").unwrap();
    let expected_rebuild = storage.rebuild_daily_stats().unwrap();
    let expected_rows = load_daily_stats_rows(&storage);

    storage.raw().execute("DELETE FROM daily_stats").unwrap();
    assert_eq!(
        rebuild_daily_stats_from_conversation_packets(&storage, &db_path).unwrap(),
        expected_rebuild
    );
    assert_eq!(load_daily_stats_rows(&storage), expected_rows);
}

#[test]
fn repair_daily_stats_if_drifted_skips_known_healthy_archive_fingerprint() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    storage.run_migrations().unwrap();

    let agent = crate::model::types::Agent {
        id: None,
        slug: "tester".into(),
        name: "Tester".into(),
        version: None,
        kind: crate::model::types::AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();
    let conversation = crate::model::types::Conversation {
        id: None,
        agent_slug: "tester".into(),
        workspace: Some(std::path::PathBuf::from("/tmp/workspace")),
        external_id: Some("daily-stats-known-healthy".into()),
        title: Some("healthy".into()),
        source_path: std::path::PathBuf::from("/tmp/healthy.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_100),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: vec![crate::model::types::Message {
            id: None,
            idx: 0,
            role: crate::model::types::MessageRole::User,
            author: None,
            created_at: Some(1_700_000_000_000),
            content: "hello".into(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        source_id: "local".into(),
        origin_host: None,
    };
    storage
        .insert_conversations_batched(&[(agent_id, None, &conversation)])
        .unwrap();

    let archive_fingerprint = lexical_storage_fingerprint_for_db(&db_path).unwrap();
    storage
        .record_daily_stats_archive_fingerprint(&archive_fingerprint)
        .unwrap();

    assert_eq!(
        repair_daily_stats_if_drifted(&storage, &db_path, Some(&archive_fingerprint)).unwrap(),
        DailyStatsRepairOutcome::SkippedKnownHealthyForFingerprint {
            archive_fingerprint
        }
    );
}

#[test]
fn daily_stats_repair_after_historical_salvage_avoids_duplicate_plain_full_probe() {
    assert!(
        !should_repair_daily_stats_after_historical_salvage(true, true, false, 0),
        "plain full runs should not re-check daily_stats after a pre-scan probe when no salvage changed canonical data"
    );
    assert!(should_repair_daily_stats_after_historical_salvage(
        false, true, false, 0
    ));
    assert!(should_repair_daily_stats_after_historical_salvage(
        true, true, false, 7
    ));
    assert!(!should_repair_daily_stats_after_historical_salvage(
        false, true, true, 0
    ));
    assert!(!should_repair_daily_stats_after_historical_salvage(
        true, false, false, 0
    ));
}

#[test]
fn historical_salvage_decision_skips_populated_canonical_db() {
    assert!(!should_salvage_historical_databases(false, 1, false, false));
    assert!(!should_salvage_historical_databases(
        false, 43_678, false, false
    ));
}

#[test]
fn historical_salvage_decision_keeps_empty_or_rebuilt_storage() {
    assert!(should_salvage_historical_databases(false, 0, false, false));
    assert!(should_salvage_historical_databases(true, 0, false, false));
    assert!(should_salvage_historical_databases(
        true, 43_678, false, false
    ));
}

#[test]
fn historical_salvage_decision_keeps_populated_canonical_when_more_bundles_are_pending() {
    assert!(should_salvage_historical_databases(
        false, 43_678, true, false
    ));
}

#[test]
fn historical_salvage_decision_skips_pending_bundles_during_canonical_only_full_rebuild() {
    assert!(!should_salvage_historical_databases(
        false, 43_678, true, true
    ));
}

#[test]
fn pending_historical_bundle_probe_skips_populated_incremental_by_default() {
    assert!(
        !should_probe_pending_historical_bundles(false, false, 43_678, false, false),
        "routine populated incremental index must not open historical backup bundles before indexing"
    );
}

#[test]
fn pending_historical_bundle_probe_keeps_recovery_paths() {
    assert!(
        should_probe_pending_historical_bundles(true, false, 43_678, false, false),
        "--full is an explicit heavy recovery/rebuild path and keeps historical discovery"
    );
    assert!(
        should_probe_pending_historical_bundles(false, true, 43_678, false, false),
        "freshly rebuilt canonical storage keeps historical discovery"
    );
    assert!(
        should_probe_pending_historical_bundles(false, false, 0, false, false),
        "empty canonical archives keep historical discovery so seed recovery still works"
    );
    assert!(
        should_probe_pending_historical_bundles(false, false, 43_678, false, true),
        "operators can explicitly request populated-incremental historical discovery"
    );
    assert!(
        !should_probe_pending_historical_bundles(true, true, 0, true, true),
        "canonical-only full rebuilds must not broaden into historical salvage"
    );
}

#[test]
fn targeted_watch_once_only_allows_empty_or_populated_incremental_run() {
    assert!(should_run_targeted_watch_once_only(
        true, false, false, false, 43_678
    ));
    assert!(should_run_targeted_watch_once_only(
        true, false, false, false, 0
    ));
    assert!(
        should_run_targeted_watch_once_only(true, false, false, true, 0),
        "fresh explicit watch-once imports should not broaden into every detected connector"
    );
    assert!(
        should_run_targeted_watch_once_only(true, false, false, true, 43_678),
        "GH #350: a populated archive whose derived index looks stale must STILL bound a \
         targeted watch-once to the requested paths (ingesting the changed sessions inline \
         and deferring any broader rebuild), never silently broadened into a full-corpus scan"
    );
    // --watch-once bounds startup even when --watch is also supplied.
    assert!(should_run_targeted_watch_once_only(
        true, true, false, false, 43_678
    ));
    assert!(!should_run_targeted_watch_once_only(
        true, false, true, false, 43_678
    ));
    assert!(!should_run_targeted_watch_once_only(
        false, false, false, false, 43_678
    ));
}

fn watch_once_skip_test_options(
    data_dir: std::path::PathBuf,
    watch_once_paths: Option<Vec<std::path::PathBuf>>,
) -> IndexOptions {
    IndexOptions {
        full: false,
        force_rebuild: false,
        watch: false,
        watch_once_paths,
        db_path: data_dir.join("agent_search.db"),
        data_dir,
        semantic: false,
        build_hnsw: false,
        embedder: "fastembed".to_string(),
        progress: None,
        watch_interval_secs: 30,
    }
}

#[test]
fn targeted_watch_once_allows_absent_lazy_fts_shadow() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let opts = watch_once_skip_test_options(
        data_dir,
        Some(vec![tmp.path().join("not-opened-by-this-unit-test.jsonl")]),
    );
    let storage = FrankenStorage::open(&opts.db_path).unwrap();
    assert_eq!(
        storage
            .inspect_search_fallback_fts_parity()
            .expect("inspect the fresh lazy FTS schema")
            .status,
        crate::storage::sqlite::FtsShadowParityStatus::Absent,
        "a fresh archive must exercise the genuinely absent lazy-shadow path"
    );

    assert_eq!(
        targeted_watch_once_fts_parity_problem(&storage, &opts).expect("inspect absent FTS parity"),
        None,
        "an absent lazy shadow has no partial artifact to re-encode and must not block fresh targeted ingestion"
    );
}

#[test]
fn targeted_watch_once_refuses_partial_fts_before_canonical_mutation() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let session = tmp.path().join("rollout-partial-fts.jsonl");
    std::fs::write(
        &session,
        concat!(
            "{\"timestamp\":\"2026-07-17T12:00:00Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"watch-once-partial\",\"cwd\":\"/tmp\"}}\n",
            "{\"timestamp\":\"2026-07-17T12:00:01Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"must not be ingested\"}]}}\n"
        ),
    )
    .unwrap();
    let opts = watch_once_skip_test_options(data_dir, Some(vec![session]));
    let storage = FrankenStorage::open(&opts.db_path).unwrap();
    let agent_id = storage
        .ensure_agent(&Agent {
            id: None,
            slug: "codex".into(),
            name: "Codex".into(),
            version: None,
            kind: AgentKind::Cli,
        })
        .unwrap();
    storage
        .insert_conversation_tree(
            agent_id,
            None,
            &Conversation {
                id: None,
                agent_slug: "codex".into(),
                workspace: None,
                external_id: Some("existing-partial".into()),
                title: Some("Existing partial fixture".into()),
                source_path: tmp.path().join("existing.jsonl"),
                started_at: Some(1_700_000_000_000),
                ended_at: Some(1_700_000_000_001),
                approx_tokens: None,
                metadata_json: serde_json::Value::Null,
                messages: vec![Message {
                    id: None,
                    idx: 0,
                    role: MessageRole::User,
                    author: None,
                    created_at: Some(1_700_000_000_001),
                    content: "indexed before interruption".into(),
                    extra_json: serde_json::Value::Null,
                    snippets: Vec::new(),
                }],
                source_id: LOCAL_SOURCE_ID.into(),
                origin_host: None,
            },
        )
        .unwrap();
    storage.rebuild_fts().unwrap();
    let conversation_id: i64 = storage
        .raw()
        .query_row_map(
            "SELECT id FROM conversations LIMIT 1",
            &[] as &[ParamValue],
            |row| row.get_typed(0),
        )
        .unwrap();
    storage
        .raw()
        .execute_compat(
            "INSERT INTO messages(conversation_id, idx, role, content) VALUES(?1, 1, 'assistant', 'missing after interruption')",
            &[ParamValue::from(conversation_id)] as &[ParamValue],
        )
        .unwrap();
    assert!(
        targeted_watch_once_fts_parity_problem(&storage, &opts)
            .unwrap()
            .is_some()
    );
    drop(storage);

    let error = run_index(opts.clone(), None)
        .expect_err("partial FTS must stop targeted watch-once before ingest");
    let error_chain = format!("{error:#}");
    assert!(
        error_chain.contains("stopped before canonical mutation"),
        "actionable fail-fast diagnostic: {error:#}"
    );
    let storage = FrankenStorage::open_readonly(&opts.db_path).unwrap();
    assert_eq!(storage.total_conversation_count().unwrap(), 1);
    assert_eq!(storage.total_message_count().unwrap(), 2);
    let parity = storage.inspect_search_fallback_fts_parity().unwrap();
    assert_eq!(
        parity.status,
        crate::storage::sqlite::FtsShadowParityStatus::Partial
    );
    assert_eq!(parity.indexed_messages, Some(1));
}

#[test]
fn readonly_canonical_force_rebuild_fast_path_is_narrow() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let mut opts = watch_once_skip_test_options(data_dir.clone(), None);
    opts.force_rebuild = true;

    assert!(
        !should_try_readonly_canonical_force_rebuild(&opts),
        "missing databases should continue through normal index creation"
    );
    std::fs::write(&opts.db_path, b"placeholder").unwrap();
    assert!(
        should_try_readonly_canonical_force_rebuild(&opts),
        "plain force rebuild against an existing DB can use the read-only canonical path"
    );

    opts.full = true;
    assert!(
        !should_try_readonly_canonical_force_rebuild(&opts),
        "--full still owns source rescan and historical salvage behavior"
    );
    opts.full = false;
    opts.semantic = true;
    assert!(
        !should_try_readonly_canonical_force_rebuild(&opts),
        "semantic indexing still requires the normal post-lexical command flow"
    );
    opts.semantic = false;
    opts.watch_once_paths = Some(vec![tmp.path().join("session.jsonl")]);
    assert!(
        !should_try_readonly_canonical_force_rebuild(&opts),
        "targeted watch-once force rebuild should not ignore explicit paths"
    );
}

#[test]
fn gh515_readonly_recovery_classification_rejects_unrelated_open_failures() -> Result<()> {
    use crate::franken_sync::FrankenError;

    let tmp = TempDir::new()?;
    let db_path = tmp.path().join("archive.db");
    fs::write(&db_path, b"canonical bytes")?;
    let cannot_open: anyhow::Error = FrankenError::CannotOpen {
        path: db_path.clone(),
    }
    .into();
    assert!(!readonly_rebuild_needs_archive_recovery(
        &db_path,
        &cannot_open
    ));
    let wal_path = tmp.path().join("archive.db-wal");
    fs::write(&wal_path, [0; 32])?;
    assert!(!readonly_rebuild_needs_archive_recovery(
        &db_path,
        &cannot_open
    ));
    fs::write(&wal_path, [0; 33])?;
    assert!(readonly_rebuild_needs_archive_recovery(
        &db_path,
        &cannot_open
    ));
    for error in [
        FrankenError::Busy,
        FrankenError::DatabaseCorrupt {
            detail: "malformed canonical B-tree".into(),
        },
        FrankenError::DatabaseCorrupt {
            detail: "conflicting virtual-table entries for another_table".into(),
        },
        FrankenError::NotADatabase {
            path: db_path.clone(),
        },
    ] {
        assert!(!readonly_rebuild_needs_archive_recovery(
            &db_path,
            &error.into()
        ));
    }
    assert!(readonly_rebuild_needs_archive_recovery(
        &db_path,
        &FrankenError::DatabaseCorrupt {
            detail: "conflicting virtual-table entries for fts_messages".into(),
        }
        .into(),
    ));
    Ok(())
}

#[test]
#[serial]
fn gh515_readonly_rebuilds_survive_unavailable_archive_write_admission() -> Result<()> {
    let _ignore_sources = ignore_sources_config();
    for busy_owner in [false, true] {
        let tmp = TempDir::new()?;
        let data_dir = tmp.path().join("rebuilt");
        let db_path = tmp.path().join("archive.db");
        let storage = FrankenStorage::open(&db_path)?;
        seed_lexical_rebuild_fixture(&storage);
        storage.set_last_indexed_at(123)?;
        storage.close()?;
        let lease_path = archive_lease::ArchiveWriteLease::path(&db_path)?;
        let owner = if busy_owner {
            Some(acquire_semantic_backfill_lock(
                &tmp.path().join("writer"),
                &db_path,
            )?)
        } else {
            // A directory makes lease creation fail even when the test
            // runs as root. No archive write lease can be acquired here.
            fs::create_dir(&lease_path)?;
            None
        };
        let mut opts = watch_once_skip_test_options(data_dir.clone(), None);
        opts.force_rebuild = true;
        opts.db_path = db_path.clone();
        opts.progress = Some(Arc::new(IndexingProgress::default()));
        run_index_with_local_connector_roots(opts, HashMap::new(), None)?;
        assert_eq!(live_tantivy_doc_count(&index_dir(&data_dir)?)?, Some(4));
        let search_data_dir = tmp.path().join("search-repair");
        let repair =
            repair_lexical_index_from_canonical_db_for_search(&db_path, &search_data_dir, None)?;
        assert_eq!(repair.indexed_docs, 4);
        assert_eq!(
            live_tantivy_doc_count(&index_dir(&search_data_dir)?)?,
            Some(4)
        );
        let storage = FrankenStorage::open_strict_readonly(&db_path)?;
        assert_eq!(count_total_messages_exact(&storage)?, 4);
        assert_eq!(storage.get_last_indexed_at()?, Some(123));
        assert_eq!(
            storage
                .raw()
                .query_row_map("PRAGMA integrity_check", &[] as &[ParamValue], |row| row
                    .get_typed::<String>(
                    0
                ))?,
            "ok"
        );
        assert!(storage.raw().query("PRAGMA foreign_key_check")?.is_empty());
        storage.close_without_checkpoint()?;
        assert!(!crate::search::asset_state::read_search_maintenance_snapshot(&data_dir).active);
        assert_eq!(lease_path.is_file(), busy_owner);
        drop(owner);
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn gh515_readonly_recovery_requires_archive_admission_and_retries_after_release() -> Result<()> {
    let tmp = TempDir::new()?;
    let db_path = tmp.path().join("archive.db");
    // Generate two real SQLite WAL generations, then retain the old SHM
    // beside the new WAL. The fixture process exits; recovery ownership
    // below belongs entirely to the real CASS admission and storage code.
    let script = r#"
import pathlib, shutil, sqlite3, sys
p = pathlib.Path(sys.argv[1])
seed = str(p) + '.seed'
c = sqlite3.connect(seed)
c.executescript('PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE t(n INTEGER); INSERT INTO t VALUES(10);')
old = pathlib.Path(seed + '-shm').read_bytes()
c.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchall()
c.execute('INSERT INTO t VALUES(20)')
c.commit()
current = pathlib.Path(seed + '-shm').read_bytes()
assert old[32:40] != current[32:40]
for suffix in ['', '-wal']:
shutil.copyfile(seed + suffix, str(p) + suffix)
pathlib.Path(str(p) + '-shm').write_bytes(old)
c.close()
"#;
    let fixture = std::process::Command::new("python3")
        .args(["-c", script])
        .arg(&db_path)
        .output()
        .context("Python sqlite3 is required for the real stale-WAL-index fixture")?;
    assert!(
        fixture.status.success(),
        "{}",
        String::from_utf8_lossy(&fixture.stderr)
    );
    let fingerprint = |suffix: &str| -> Result<(Vec<u8>, SystemTime)> {
        let mut path = db_path.as_os_str().to_os_string();
        path.push(suffix);
        Ok((fs::read(&path)?, fs::metadata(&path)?.modified()?))
    };
    let main_before = fingerprint("")?;
    let wal_before = fingerprint("-wal")?;
    let shm_before = fingerprint("-shm")?;
    let first_data = tmp.path().join("first");
    let second_data = tmp.path().join("second");
    let owner = acquire_semantic_backfill_lock(&first_data, &db_path)?;
    let mut recovery_guard = None;
    let started = Instant::now();
    let error = open_readonly_rebuild_storage(&db_path, || {
        recovery_guard = Some(acquire_search_maintenance_mutation_lock(
            &second_data,
            &db_path,
            SearchMaintenanceJobKind::LexicalRefresh,
        )?);
        Ok(())
    })
    .err()
    .context("a stale WAL index must not recover outside archive ownership")?;
    assert!(error.is::<IndexRunBusy>(), "{error:#}");
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(recovery_guard.is_none());
    assert_eq!(fingerprint("")?, main_before);
    assert_eq!(fingerprint("-wal")?, wal_before);
    assert_eq!(fingerprint("-shm")?, shm_before);
    assert!(read_index_run_lock_metadata_for_test(&second_data.join("index-run.lock"))?.is_empty());
    drop(owner);

    let started = Instant::now();
    let storage = open_readonly_rebuild_storage(&db_path, || {
        recovery_guard = Some(acquire_search_maintenance_mutation_lock(
            &second_data,
            &db_path,
            SearchMaintenanceJobKind::LexicalRefresh,
        )?);
        Ok(())
    })?;
    assert!(started.elapsed() < Duration::from_secs(10));
    assert!(recovery_guard.is_some());
    assert!(try_acquire_semantic_backfill_lock(&first_data, &db_path)?.is_none());
    assert_eq!(
        storage
            .raw()
            .query_row_map("SELECT sum(n) FROM t", &[] as &[ParamValue], |row| {
                row.get_typed::<i64>(0)
            })?,
        30
    );
    storage.close_without_checkpoint()?;
    assert_eq!(fingerprint("")?, main_before);
    assert_eq!(fingerprint("-wal")?, wal_before);
    drop(recovery_guard);
    FrankenStorage::open_strict_readonly(&db_path)?.close_without_checkpoint()?;
    let _retry = acquire_semantic_backfill_lock(&first_data, &db_path)?;
    Ok(())
}

#[test]
#[serial]
fn gh515_empty_readonly_force_rebuild_acquires_archive_before_writable_fallback() -> Result<()> {
    let _ignore_sources = ignore_sources_config();
    let tmp = TempDir::new()?;
    let db_path = tmp.path().join("archive.db");
    let storage = FrankenStorage::open(&db_path)?;
    storage.set_last_indexed_at(123)?;
    storage.close()?;
    let owner = acquire_semantic_backfill_lock(&tmp.path().join("writer"), &db_path)?;
    let data_dir = tmp.path().join("contender");
    let mut opts = watch_once_skip_test_options(data_dir.clone(), None);
    opts.force_rebuild = true;
    opts.db_path = db_path.clone();
    opts.progress = Some(Arc::new(IndexingProgress::default()));
    let error = run_index_with_local_connector_roots(opts.clone(), HashMap::new(), None)
        .expect_err("empty readonly probe must not bypass the existing archive writer");
    assert!(error.is::<IndexRunBusy>());
    let storage = FrankenStorage::open_readonly(&db_path)?;
    assert_eq!(count_total_messages_exact(&storage)?, 0);
    assert_eq!(storage.get_last_indexed_at()?, Some(123));
    storage.close_without_checkpoint()?;
    assert!(!crate::search::asset_state::read_search_maintenance_snapshot(&data_dir).active);
    drop(owner);
    run_index_with_local_connector_roots(opts, HashMap::new(), None)?;
    let storage = FrankenStorage::open_readonly(&db_path)?;
    assert_eq!(count_total_messages_exact(&storage)?, 0);
    assert!(
        storage
            .get_last_indexed_at()?
            .is_some_and(|timestamp| timestamp > 123)
    );
    assert_eq!(
        storage
            .raw()
            .query_row_map("PRAGMA integrity_check", &[] as &[ParamValue], |row| row
                .get_typed::<String>(
                0
            ))?,
        "ok"
    );
    assert!(storage.raw().query("PRAGMA foreign_key_check")?.is_empty());
    storage.close_without_checkpoint()?;
    Ok(())
}

#[test]
fn absent_explicit_watch_once_paths_skip_heavy_index_setup() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    let opts = watch_once_skip_test_options(
        data_dir.clone(),
        Some(vec![
            tmp.path().join("missing-a.jsonl"),
            tmp.path().join("missing-b.jsonl"),
        ]),
    );

    assert!(should_skip_absent_explicit_watch_once_paths(&opts));
    assert!(
        !can_skip_absent_explicit_watch_once_index_run(&opts),
        "missing canonical/index assets must keep the normal repair/create path"
    );

    let storage = FrankenStorage::open(&opts.db_path).unwrap();
    ensure_fts_schema(&storage);
    let _index = TantivyIndex::open_or_create(&index_dir(&data_dir).unwrap()).unwrap();

    assert!(
        can_skip_absent_explicit_watch_once_index_run(&opts),
        "populated data dirs with a current lexical index can skip absent explicit paths"
    );
}

#[test]
fn absent_explicit_watch_once_paths_preserve_non_noop_modes() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let existing = data_dir.join("session.jsonl");
    std::fs::write(&existing, "{}\n").unwrap();
    let missing = data_dir.join("missing.jsonl");

    let existing_opts =
        watch_once_skip_test_options(data_dir.clone(), Some(vec![existing.clone()]));
    assert!(
        !should_skip_absent_explicit_watch_once_paths(&existing_opts),
        "existing explicit paths must flow through normal watch-once reindexing"
    );

    let mixed_opts = watch_once_skip_test_options(
        data_dir.clone(),
        Some(vec![missing.clone(), existing.clone()]),
    );
    assert!(
        !should_skip_absent_explicit_watch_once_paths(&mixed_opts),
        "mixed existing/missing batches must still index the existing paths"
    );

    let mut watch_opts =
        watch_once_skip_test_options(data_dir.clone(), Some(vec![missing.clone()]));
    watch_opts.watch = true;
    assert!(!should_skip_absent_explicit_watch_once_paths(&watch_opts));

    let mut full_opts = watch_once_skip_test_options(data_dir.clone(), Some(vec![missing.clone()]));
    full_opts.full = true;
    assert!(!should_skip_absent_explicit_watch_once_paths(&full_opts));

    let mut force_opts =
        watch_once_skip_test_options(data_dir.clone(), Some(vec![missing.clone()]));
    force_opts.force_rebuild = true;
    assert!(!should_skip_absent_explicit_watch_once_paths(&force_opts));

    let mut semantic_opts =
        watch_once_skip_test_options(data_dir.clone(), Some(vec![missing.clone()]));
    semantic_opts.semantic = true;
    assert!(!should_skip_absent_explicit_watch_once_paths(
        &semantic_opts
    ));

    let mut hnsw_opts = watch_once_skip_test_options(data_dir, Some(vec![missing]));
    hnsw_opts.build_hnsw = true;
    assert!(!should_skip_absent_explicit_watch_once_paths(&hnsw_opts));
}

#[test]
fn watch_once_authoritative_repair_skips_broad_followup_scan() {
    assert!(should_skip_broad_scan_after_watch_once_authoritative_repair(true, false, false, true));
    assert!(should_skip_broad_scan_after_watch_once_authoritative_repair(true, true, false, true));
    assert!(!should_skip_broad_scan_after_watch_once_authoritative_repair(true, false, true, true));
    assert!(
        !should_skip_broad_scan_after_watch_once_authoritative_repair(true, false, false, false)
    );
    assert!(
        !should_skip_broad_scan_after_watch_once_authoritative_repair(false, false, false, true)
    );
}

/// #440: the resume fence owes the upsert path exactly the documents the
/// published authority holds beyond the checkpoint (plus slack), nothing
/// for a from-zero build, and everything for a resume into the live index.
#[test]
fn resume_reconcile_upsert_budget_matches_the_authority_gap() {
    // From zero: never upsert, whatever the index holds.
    assert_eq!(resume_reconcile_upsert_budget(false, false, 0, 0), 0);
    assert_eq!(resume_reconcile_upsert_budget(false, false, 0, 500), 0);
    // Resume into staging: authority at or behind the checkpoint owes nothing.
    assert_eq!(resume_reconcile_upsert_budget(false, true, 300, 300), 0);
    assert_eq!(resume_reconcile_upsert_budget(false, true, 300, 120), 0);
    // Resume into staging with the authority ahead: gap plus slack.
    assert_eq!(
        resume_reconcile_upsert_budget(false, true, 316_934, 320_000),
        3_066 + RESUME_RECONCILE_SLACK_DOCS
    );
    // Resume into the live index: every replayed document is upserted.
    assert_eq!(
        resume_reconcile_upsert_budget(true, true, 300, 300),
        usize::MAX
    );
}

#[test]
fn fallback_fts_repair_is_skipped_for_canonical_only_full_rebuild() {
    assert!(!should_repair_fallback_fts_after_full_index_run(true, true));
    assert!(should_repair_fallback_fts_after_full_index_run(true, false));
    assert!(!should_repair_fallback_fts_after_full_index_run(
        false, false
    ));
    assert!(!should_repair_fallback_fts_after_full_index_run(
        false, true
    ));
}

#[test]
fn fallback_fts_repair_classifies_missing_shadow_as_nonfatal_derived_damage() {
    let outcome = nonfatal_fallback_fts_repair_outcome(
        "inserting 10000 fallback rows: table not found: fts_messages_data".to_string(),
    );
    assert!(matches!(
        outcome,
        Some(FallbackFtsRepairOutcome::SkippedCorruptDerivedIndex { detail })
            if detail.contains("fts_messages_data")
    ));

    assert_eq!(
        nonfatal_fallback_fts_repair_outcome(
            "disk I/O error while opening canonical messages".to_string()
        ),
        None,
        "canonical I/O failures must remain fatal"
    );
}

#[test]
fn full_run_fallback_fts_repair_skips_rebuild_when_fts_is_already_healthy() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("fts-healthy.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    seed_lexical_rebuild_fixture(&storage);
    // `ensure_fts_schema` creates the legacy internal-content shadow, which
    // is residue since GH #495, not a healthy shadow. Converge it through
    // the production repair so this test's precondition actually holds.
    storage.ensure_search_fallback_fts_consistency().unwrap();

    let repair =
        repair_fallback_fts_after_full_index_run(&storage, &db_path, true, false, None, None)
            .unwrap();
    assert_eq!(
        repair,
        Some(FallbackFtsRepairOutcome::Repaired(
            FtsConsistencyRepair::AlreadyHealthy { rows: 4 }
        ))
    );
}

#[test]
fn full_run_fallback_fts_repair_rebuilds_missing_schema_when_needed() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("fts-missing.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    seed_lexical_rebuild_fixture(&storage);

    let repair =
        repair_fallback_fts_after_full_index_run(&storage, &db_path, true, false, None, None)
            .unwrap();
    assert_eq!(
        repair,
        Some(FallbackFtsRepairOutcome::Repaired(
            FtsConsistencyRepair::Rebuilt { inserted_rows: 4 }
        ))
    );
}

#[test]
fn full_run_fallback_fts_repair_skips_known_healthy_archive_fingerprint() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("fts-known-healthy.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    seed_lexical_rebuild_fixture(&storage);

    let archive_fingerprint = lexical_storage_fingerprint_for_db(&db_path).unwrap();
    storage.ensure_search_fallback_fts_consistency().unwrap();
    storage
        .record_search_fallback_fts_archive_fingerprint(&archive_fingerprint)
        .unwrap();

    let repair = repair_fallback_fts_after_full_index_run(
        &storage,
        &db_path,
        true,
        false,
        Some(&archive_fingerprint),
        None,
    )
    .unwrap();
    assert_eq!(
        repair,
        Some(
            FallbackFtsRepairOutcome::SkippedKnownHealthyForFingerprint {
                archive_fingerprint
            }
        )
    );
}

/// GH #495 follow-up: since an absent over-bound shadow counts as
/// `NotViable`, the startup preflight retires it on the first run. A later
/// run on the same, already retired archive must not rewrite the markers
/// (per-run meta writes move the WAL's physical identity and invalidate
/// the one-shot fingerprint cache), while a half-recorded retirement must
/// still be recorded again.
#[test]
fn gh495_preflight_leaves_an_already_retired_shadow_untouched() {
    const CHILD: &str = "CASS_TEST_GH495_PREFLIGHT_CHILD";
    if dotenvy::var(CHILD).is_err() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                &format!(
                    "{}::gh495_preflight_leaves_an_already_retired_shadow_untouched",
                    module_path!().split_once("::").unwrap().1
                ),
                "--nocapture",
            ])
            .env(CHILD, "1")
            .env("CASS_FTS_SHADOW_MAX_MESSAGES", "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "child failed: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
        return;
    }
    use crate::storage::sqlite::error_message_indicates_fts_shadow_not_viable;
    const SENTINEL: &str = "sentinel retirement recorded by an earlier run";

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("gh495-preflight.db");
    {
        let storage = FrankenStorage::open(&db_path).unwrap();
        seed_lexical_rebuild_fixture(&storage);
        storage.close_without_checkpoint().unwrap();
    }
    let with_deferred = |f: &dyn Fn(&FrankenStorage)| {
        let storage = FrankenStorage::open_deferred_fts5_for_repair(&db_path).unwrap();
        f(&storage);
        storage.close_without_checkpoint().unwrap();
    };
    let markers = || {
        let storage = FrankenStorage::open_deferred_fts5_for_repair(&db_path).unwrap();
        let markers = (
            storage.fts_shadow_not_viable_marker().unwrap(),
            storage.read_fallback_fts_repair_pending().unwrap(),
        );
        storage.close_without_checkpoint().unwrap();
        markers
    };

    // First run retires the over-bound shadow and records both markers.
    preflight_fts_shadow_before_lexical_readers(&db_path).unwrap();
    let (marker, pending) = markers();
    assert!(
        marker
            .as_deref()
            .is_some_and(error_message_indicates_fts_shadow_not_viable),
        "first preflight must record the retirement: {marker:?}"
    );
    assert!(pending.is_some());

    // An already retired archive keeps its recorded state byte-for-byte.
    with_deferred(&|storage| storage.drop_fts_shadow_as_not_viable(SENTINEL).unwrap());
    preflight_fts_shadow_before_lexical_readers(&db_path).unwrap();
    assert_eq!(markers().0.as_deref(), Some(SENTINEL));

    // Negative: a half-recorded retirement is not current, so the next
    // preflight records it again (a bare "skip when unregistered" fails).
    with_deferred(&|storage| storage.record_fallback_fts_repair_pending(None).unwrap());
    preflight_fts_shadow_before_lexical_readers(&db_path).unwrap();
    let (marker, pending) = markers();
    assert!(
        marker
            .as_deref()
            .is_some_and(|marker| marker != SENTINEL
                && error_message_indicates_fts_shadow_not_viable(marker)),
        "a half-recorded retirement must be recorded again: {marker:?}"
    );
    assert!(pending.is_some());
}

/// GH #461: the noise-adjusted expected doc count must not re-read the
/// whole archive on every run, yet it must stay exactly what the full scan
/// computes. An unchanged identity is served from the sidecar (a planted
/// sentinel proves no scan ran); appends extend it by a delta equal to the
/// full count; a deletion or an insertion before retained messages falls
/// back to a full recount (generation resets to 0).
#[test]
fn gh461_expected_lexical_docs_memo_serves_unchanged_and_extends_appends_exactly() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("gh461.db");
    let index_path = dir.path().join("index");
    fs::create_dir_all(&index_path).unwrap();
    let storage = FrankenStorage::open(&db_path).unwrap();
    seed_lexical_rebuild_fixture(&storage);
    let db = db_path.to_string_lossy().into_owned();
    let conversation_id: i64 = storage
        .raw()
        .query_row_map(
            "SELECT MIN(id) FROM conversations",
            &[] as &[ParamValue],
            |row| row.get_typed(0),
        )
        .unwrap();
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
    let cached = || {
        expected_live_lexical_doc_count_cached(
            &storage,
            &index_path,
            &db,
            count_total_messages_exact(&storage).unwrap(),
            None,
        )
        .unwrap()
    };
    let sidecar_path = expected_lexical_docs_cache_path(&index_path).unwrap();
    let sidecar = || {
        serde_json::from_slice::<ExpectedLexicalDocsCache>(&fs::read(&sidecar_path).unwrap())
            .unwrap()
    };
    let full = || expected_live_lexical_doc_count(&storage).unwrap();

    // Empty content is hard noise, so the expectation is below the raw count.
    insert(conversation_id, 50, "user", "   ");
    assert_eq!(cached(), full());
    assert_eq!(sidecar().delta_generations, 0);
    assert!(full() < count_total_messages_exact(&storage).unwrap());

    // Unchanged identity: served from the sidecar without a scan.
    let mut planted = sidecar();
    planted.expected_docs = 999_999;
    fs::write(&sidecar_path, serde_json::to_vec(&planted).unwrap()).unwrap();
    assert_eq!(cached(), 999_999, "an unchanged identity must not rescan");
    planted.expected_docs = full();
    fs::write(&sidecar_path, serde_json::to_vec(&planted).unwrap()).unwrap();

    // Appends (after retained messages, plus a new noise row) extend exactly.
    insert(conversation_id, 60, "assistant", "appended evidence");
    insert(conversation_id, 61, "tool", "");
    assert_eq!(cached(), full());
    assert_eq!(
        sidecar().delta_generations,
        1,
        "appends take the delta path"
    );

    // Text is capped per message (bgn6s, 938bf986), so a row inserted
    // before retained messages changes no other message's truncation or
    // noise class and is still an exact delta. The memo's first design
    // capped text per conversation and had to recount here.
    insert(conversation_id, 55, "user", "inserted mid-conversation");
    assert_eq!(cached(), full());
    assert_eq!(
        sidecar().delta_generations,
        2,
        "a mid-conversation insertion is an exact delta under the per-message cap"
    );

    insert(conversation_id, 70, "user", "one more append");
    assert_eq!(cached(), full());
    assert_eq!(sidecar().delta_generations, 3);

    // Orphaned rows are never indexed: an appended message whose
    // conversation row does not exist must not count, and it is still an
    // append (5ajer: the delta filters it with a point lookup, not a JOIN).
    storage.raw().execute("PRAGMA foreign_keys = OFF").unwrap();
    insert(conversation_id + 1_000_000, 0, "user", "orphaned evidence");
    assert_eq!(cached(), full());
    assert_eq!(
        sidecar().delta_generations,
        4,
        "an orphan append still takes the delta path"
    );
    storage
        .raw()
        .execute_compat(
            "DELETE FROM messages WHERE conversation_id = ?1 AND idx = 60",
            &[ParamValue::from(conversation_id)],
        )
        .unwrap();
    assert_eq!(cached(), full());
    assert_eq!(sidecar().delta_generations, 0, "a deletion must recount");
}
