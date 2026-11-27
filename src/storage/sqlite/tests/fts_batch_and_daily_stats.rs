use super::*;
use crate::franken_sync::compat::{
    ConnectionExt as FrankenConnectionExt, RowExt as FrankenRowExt,
};
use crate::model::types::{Agent, AgentKind, Snippet};
use crate::sources::provenance::LOCAL_SOURCE_ID;
use serial_test::serial;
use std::path::PathBuf;
use tempfile::TempDir;

/// Multi-row INSERT programs with large register counts must preserve every
/// row. The FTS shadow is written in FTS5_BATCH_SIZE-row statements; every row
/// of a full batch, the last ones included, must round-trip.
#[test]
fn fts_batch_insert_round_trips_every_row_of_a_full_batch() {
    let dir = TempDir::new().unwrap();
    let storage = SqliteStorage::open(&dir.path().join("fts-batch.db")).unwrap();
    storage
        .ensure_search_fallback_fts_consistency()
        .expect("create the FTS shadow");
    let rows = FTS5_BATCH_SIZE as i64;
    let entries: Vec<FtsEntry> = (0..rows)
        .map(|i| FtsEntry {
            content: format!("batchprobe{i} filler words for the batch"),
            title: format!("title {i}"),
            agent: "codex".to_string(),
            workspace: "/ws/batch".to_string(),
            source_path: format!("/s/batch-{i}.jsonl"),
            created_at: Some(1_700_000_000_000 + i),
            message_id: i + 1,
        })
        .collect();

    let inserted = franken_batch_insert_fts_on_connection(&storage.conn, &entries).unwrap();
    assert_eq!(inserted, FTS5_BATCH_SIZE);
    let count: i64 = storage
        .conn
        .query_row_map("SELECT COUNT(*) FROM fts_messages", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(count, rows);
    for i in [0, rows / 2, rows - 2, rows - 1] {
        let hits = storage
            .raw()
            .query(&format!(
                "SELECT rowid FROM fts_messages WHERE fts_messages MATCH 'batchprobe{i}'"
            ))
            .unwrap();
        assert_eq!(hits.len(), 1, "row {i} of {rows} must be indexed once");
        assert_eq!(hits[0].get_typed::<i64>(0).unwrap(), i + 1, "row {i}");
    }
    storage
        .raw()
        .execute("INSERT INTO fts_messages(fts_messages) VALUES('integrity-check')")
        .expect("FTS5 integrity-check after a full batch");
}

#[test]
#[serial]
fn insert_conversations_batched_flushes_large_fts_batches() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    // The inline FTS writes this test counts are skipped while
    // CASS_DEFER_LEXICAL_UPDATES is truthy, and #[serial] tests set it to
    // "1" for their duration; pin it off and run serially with them.
    let _defer_guard = set_env_var("CASS_DEFER_LEXICAL_UPDATES", "0");
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();
    // V14 drops fts_messages during migration; cass normally recreates it
    // during startup via `ensure_search_fallback_fts_consistency`. Tests
    // that inspect fts_messages directly need to run the same repair pass
    // to exercise the "insert flushes FTS" contract.
    storage
        .ensure_search_fallback_fts_consistency()
        .expect("ensure FTS consistency before insert");

    let agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: Some("0.2.3".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();

    let content = "y".repeat((FTS_ENTRY_BATCH_MAX_CHARS / 2) + 1);
    let messages: Vec<_> = (0_i64..2)
        .map(|i| Message {
            id: None,
            idx: i,
            role: MessageRole::Agent,
            author: None,
            created_at: Some(1_700_000_000_000 + i),
            content: format!("{i}-{content}"),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        })
        .collect();
    let conv = Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: Some("fts-large-batch".into()),
        title: Some("FTS Large Batch".into()),
        source_path: PathBuf::from("/tmp/rollout.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_999),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages,
        source_id: "local".into(),
        origin_host: None,
    };

    let outcomes = storage
        .insert_conversations_batched(&[(agent_id, None, &conv)])
        .unwrap();
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].inserted_indices.len(), conv.messages.len());

    let message_count: i64 = storage
        .conn
        .query_row_map("SELECT COUNT(*) FROM messages", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    let fts_count: i64 = storage
        .conn
        .query_row_map("SELECT COUNT(*) FROM fts_messages", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();

    assert_eq!(message_count, conv.messages.len() as i64);
    assert_eq!(fts_count, conv.messages.len() as i64);
}

#[test]
fn insert_conversation_tree_batched_new_message_ids_match_snippet_rows() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("batched-message-ids.db");
    let storage = SqliteStorage::open(&db_path).unwrap();
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
        .ensure_workspace(&PathBuf::from("/ws/profiled-storage-remote"), None)
        .unwrap();
    let mut conv = make_profiled_storage_remote_conversation(42, 5);
    for (idx, msg) in conv.messages.iter_mut().enumerate() {
        msg.snippets.push(Snippet {
            id: None,
            file_path: Some(PathBuf::from(format!("src/file_{idx}.rs"))),
            start_line: Some((idx + 1) as i64),
            end_line: Some((idx + 2) as i64),
            language: Some("rust".into()),
            snippet_text: Some(format!("fn snippet_{idx}() {{}}")),
        });
    }
    let outcome = storage
        .insert_conversation_tree(agent_id, Some(workspace_id), &conv)
        .unwrap();

    let message_count: i64 = storage
        .conn
        .query_row_map(
            "SELECT COUNT(*) FROM messages WHERE conversation_id = ?1",
            fparams![outcome.conversation_id],
            |row| row.get_typed(0),
        )
        .unwrap();
    let joined_snippet_count: i64 = storage
        .conn
        .query_row_map(
            "SELECT COUNT(*)
             FROM snippets s
             JOIN messages m ON s.message_id = m.id
             WHERE m.conversation_id = ?1",
            fparams![outcome.conversation_id],
            |row| row.get_typed(0),
        )
        .unwrap();

    assert_eq!(message_count, conv.messages.len() as i64);
    assert_eq!(joined_snippet_count, conv.messages.len() as i64);
}

#[test]
fn insert_conversation_tree_batched_appended_message_ids_match_snippet_rows() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("batched-append-message-ids.db");
    let storage = SqliteStorage::open(&db_path).unwrap();
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
        .ensure_workspace(&PathBuf::from("/ws/profiled-storage-remote"), None)
        .unwrap();

    let mut initial = make_profiled_storage_remote_conversation(77, 2);
    for (idx, msg) in initial.messages.iter_mut().enumerate() {
        msg.snippets.push(Snippet {
            id: None,
            file_path: Some(PathBuf::from(format!("src/append_initial_{idx}.rs"))),
            start_line: Some((idx + 1) as i64),
            end_line: Some((idx + 2) as i64),
            language: Some("rust".into()),
            snippet_text: Some(format!("fn append_initial_{idx}() {{}}")),
        });
    }
    let first = storage
        .insert_conversation_tree(agent_id, Some(workspace_id), &initial)
        .unwrap();
    assert_eq!(first.inserted_indices, vec![0, 1]);

    let mut appended = make_profiled_storage_remote_conversation(77, 5);
    for (idx, msg) in appended.messages.iter_mut().enumerate() {
        msg.snippets.push(Snippet {
            id: None,
            file_path: Some(PathBuf::from(format!("src/append_full_{idx}.rs"))),
            start_line: Some((idx + 10) as i64),
            end_line: Some((idx + 11) as i64),
            language: Some("rust".into()),
            snippet_text: Some(format!("fn append_full_{idx}() {{}}")),
        });
    }
    let second = storage
        .insert_conversation_tree(agent_id, Some(workspace_id), &appended)
        .unwrap();
    assert_eq!(second.conversation_id, first.conversation_id);
    assert_eq!(second.inserted_indices, vec![2, 3, 4]);

    let message_count: i64 = storage
        .conn
        .query_row_map(
            "SELECT COUNT(*) FROM messages WHERE conversation_id = ?1",
            fparams![first.conversation_id],
            |row| row.get_typed(0),
        )
        .unwrap();
    let joined_snippets: Vec<(i64, String)> = storage
        .conn
        .query_map_collect(
            "SELECT m.idx, s.file_path
             FROM snippets s
             JOIN messages m ON s.message_id = m.id
             WHERE m.conversation_id = ?1
             ORDER BY m.idx, s.id",
            fparams![first.conversation_id],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
        )
        .unwrap();

    assert_eq!(message_count, 5);
    assert_eq!(
        joined_snippets,
        vec![
            (0, "src/append_initial_0.rs".to_string()),
            (1, "src/append_initial_1.rs".to_string()),
            (2, "src/append_full_2.rs".to_string()),
            (3, "src/append_full_3.rs".to_string()),
            (4, "src/append_full_4.rs".to_string()),
        ]
    );
}

#[test]
fn insert_conversation_tree_rehydrates_external_lookup_after_manual_clear() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("external-lookup-rehydrate.db");
    let storage = SqliteStorage::open(&db_path).unwrap();
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
        .ensure_workspace(&PathBuf::from("/ws/profiled-storage-remote"), None)
        .unwrap();

    let initial = make_profiled_storage_remote_conversation(88, 2);
    let first = storage
        .insert_conversation_tree(agent_id, Some(workspace_id), &initial)
        .unwrap();
    let external_id = initial.external_id.as_deref().unwrap();
    let lookup_key = conversation_external_lookup_key(&initial.source_id, agent_id, external_id);
    let lookup_id: i64 = storage
        .conn
        .query_row_map(
            "SELECT conversation_id
             FROM conversation_external_tail_lookup
             WHERE lookup_key = ?1",
            fparams![lookup_key.as_str()],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(lookup_id, first.conversation_id);

    storage
        .conn
        .execute_compat(
            "DELETE FROM conversation_external_tail_lookup WHERE lookup_key = ?1",
            fparams![lookup_key.as_str()],
        )
        .unwrap();

    let appended = make_profiled_storage_remote_conversation(88, 4);
    let second = storage
        .insert_conversation_tree(agent_id, Some(workspace_id), &appended)
        .unwrap();
    assert_eq!(second.conversation_id, first.conversation_id);
    assert_eq!(second.inserted_indices, vec![2, 3]);

    let conversation_count: i64 = storage
        .conn
        .query_row_map(
            "SELECT COUNT(*)
                 FROM conversations
             WHERE source_id = ?1 AND agent_id = ?2 AND external_id = ?3",
            fparams![initial.source_id.as_str(), agent_id, external_id],
            |row| row.get_typed(0),
        )
        .unwrap();
    let restored_lookup: (i64, Option<i64>, Option<i64>, Option<i64>) = storage
        .conn
        .query_row_map(
            "SELECT conversation_id, ended_at, last_message_idx, last_message_created_at
             FROM conversation_external_tail_lookup
             WHERE lookup_key = ?1",
            fparams![lookup_key.as_str()],
            |row| {
                Ok((
                    row.get_typed(0)?,
                    row.get_typed(1)?,
                    row.get_typed(2)?,
                    row.get_typed(3)?,
                ))
            },
        )
        .unwrap();
    let tail_state: (Option<i64>, Option<i64>, Option<i64>) = storage
        .conn
        .query_row_map(
            "SELECT ended_at, last_message_idx, last_message_created_at
             FROM conversation_tail_state
             WHERE conversation_id = ?1",
            fparams![first.conversation_id],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?, row.get_typed(2)?)),
        )
        .unwrap();
    assert_eq!(conversation_count, 1);
    assert_eq!(
        restored_lookup,
        (
            first.conversation_id,
            tail_state.0,
            tail_state.1,
            tail_state.2
        )
    );
    assert_eq!(
        tail_state,
        (
            appended.messages[3].created_at,
            Some(3),
            appended.messages[3].created_at
        )
    );
}

#[test]
fn insert_conversation_tree_recreates_daily_stats_after_manual_clear() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();
    let agent_id = storage
        .ensure_agent(&Agent {
            id: None,
            slug: "codex".into(),
            name: "Codex".into(),
            version: None,
            kind: AgentKind::Cli,
        })
        .unwrap();
    let workspace = PathBuf::from("/ws/profiled-storage-remote");
    let workspace_id = storage.ensure_workspace(&workspace, None).unwrap();

    // Pin inline analytics explicitly: the ambient default is process-global
    // and a parallel test driving an index run defers analytics for its
    // whole duration, which turned this assertion into a cross-test race.
    storage
        .insert_conversation_tree_with_analytics(
            agent_id,
            Some(workspace_id),
            &make_profiled_storage_remote_conversation(0, 3),
            false,
        )
        .unwrap();
    storage.conn.execute("DELETE FROM daily_stats").unwrap();

    storage
        .insert_conversation_tree_with_analytics(
            agent_id,
            Some(workspace_id),
            &make_profiled_storage_remote_conversation(1, 2),
            false,
        )
        .unwrap();

    let row_count: i64 = storage
        .conn
        .query_row_map("SELECT COUNT(*) FROM daily_stats", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    let (session_count, message_count): (i64, i64) = storage
        .conn
        .query_row_map(
            "SELECT session_count, message_count
             FROM daily_stats
             WHERE agent_slug = 'all' AND source_id = 'all'",
            fparams![],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
        )
        .unwrap();

    assert_eq!(row_count, 4);
    assert_eq!(session_count, 1);
    assert_eq!(message_count, 2);
}

#[test]
#[serial]
fn insert_conversation_tree_stage_profile_tracks_steady_state_remote_reuse() {
    let _defer_guard = set_env_var("CASS_DEFER_LEXICAL_UPDATES", "0");

    for &(msg_count, iterations) in &[(5usize, 80usize), (20, 50), (50, 24)] {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join(format!("profile-{msg_count}.db"));
        let storage = SqliteStorage::open(&db_path).unwrap();
        let agent_id = storage
            .ensure_agent(&Agent {
                id: None,
                slug: "codex".into(),
                name: "Codex".into(),
                version: None,
                kind: AgentKind::Cli,
            })
            .unwrap();
        let workspace = PathBuf::from("/ws/profiled-storage-remote");
        let workspace_id = storage.ensure_workspace(&workspace, None).unwrap();

        storage
            .insert_conversation_tree(
                agent_id,
                Some(workspace_id),
                &make_profiled_storage_remote_conversation(0, msg_count),
            )
            .unwrap();

        let mut profile = InsertConversationTreePerfProfile::default();
        for external_id in 1..=iterations {
            // Transient busy/snapshot contention can surface during
            // back-to-back writes; the production insert path retries these,
            // so the raw profiled call must too. Restore the profile snapshot
            // on retry so an aborted attempt's stage durations don't
            // double-count against the recorded total.
            let profile_snapshot = profile.clone();
            retry_transient_storage_op("stage_profile_insert", || {
                profile = profile_snapshot.clone();
                storage.insert_conversation_tree_with_profile(
                    agent_id,
                    Some(workspace_id),
                    &make_profiled_storage_remote_conversation(external_id as i64, msg_count),
                    &mut profile,
                )
            })
            .unwrap();
        }

        let accounted_duration = profile.source_duration
            + profile.tx_open_duration
            + profile.existing_lookup_duration
            + profile.conversation_row_duration
            + profile.message_insert_duration
            + profile.snippet_insert_duration
            + profile.fts_entry_duration
            + profile.fts_flush_duration
            + profile.analytics_duration
            + profile.commit_duration;
        assert_eq!(profile.invocations, iterations);
        assert_eq!(profile.messages, iterations * msg_count);
        assert_eq!(profile.inserted_messages, iterations * msg_count);
        assert!(
            profile.total_duration >= accounted_duration,
            "accounted stage durations cannot exceed total duration"
        );

        profile.log_summary(&format!("remote_reuse_{msg_count}_msgs"));
    }
}

#[test]
#[serial]
fn insert_conversation_tree_stage_profile_tracks_append_remote_source_merge() {
    let _defer_guard = set_env_var("CASS_DEFER_LEXICAL_UPDATES", "0");

    for &(msg_count, iterations) in &[(5usize, 80usize), (20, 50), (50, 24)] {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join(format!("append-profile-{msg_count}.db"));
        let storage = SqliteStorage::open(&db_path).unwrap();
        let agent_id = storage
            .ensure_agent(&Agent {
                id: None,
                slug: "codex".into(),
                name: "Codex".into(),
                version: None,
                kind: AgentKind::Cli,
            })
            .unwrap();
        let workspace = PathBuf::from("/ws/profiled-append-remote");
        let workspace_id = storage.ensure_workspace(&workspace, None).unwrap();

        for external_id in 0..iterations {
            storage
                .insert_conversation_tree(
                    agent_id,
                    Some(workspace_id),
                    &make_profiled_append_remote_merge_conversation(external_id as i64, msg_count),
                )
                .unwrap();
        }

        let mut profile = InsertConversationTreePerfProfile::default();
        for external_id in 0..iterations {
            // See stage_profile_insert note: transient contention must be
            // retried here just as the production append path does, with the
            // profile snapshot restored per attempt to keep stage sums <= total.
            let profile_snapshot = profile.clone();
            retry_transient_storage_op("stage_profile_append", || {
                profile = profile_snapshot.clone();
                storage.append_existing_conversation_with_profile(
                    agent_id,
                    Some(workspace_id),
                    &make_profiled_append_remote_merge_conversation(
                        external_id as i64,
                        msg_count * 2,
                    ),
                    &mut profile,
                )
            })
            .unwrap();
        }

        let accounted_duration = profile.source_duration
            + profile.tx_open_duration
            + profile.existing_lookup_duration
            + profile.existing_idx_lookup_duration
            + profile.existing_replay_lookup_duration
            + profile.dedupe_filter_duration
            + profile.conversation_row_duration
            + profile.message_insert_duration
            + profile.snippet_insert_duration
            + profile.fts_entry_duration
            + profile.fts_flush_duration
            + profile.analytics_duration
            + profile.commit_duration;
        assert_eq!(profile.invocations, iterations);
        assert_eq!(profile.messages, iterations * msg_count * 2);
        assert_eq!(profile.inserted_messages, iterations * msg_count);
        assert!(
            profile.total_duration >= accounted_duration,
            "accounted append stage durations cannot exceed total duration"
        );

        profile.log_summary(&format!("append_remote_merge_{msg_count}_msgs"));
    }
}

#[test]
#[serial]
fn rebuild_daily_stats_recomputes_materialized_totals_without_monolithic_group_by() {
    let _conversation_batch = set_env_var("CASS_DAILY_STATS_REBUILD_CONVERSATION_BATCH_SIZE", "1");
    let _message_batch = set_env_var("CASS_DAILY_STATS_REBUILD_MESSAGE_BATCH_SIZE", "1");
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();
    let started_at = 1_700_000_000_000_i64;
    let day_id = FrankenStorage::day_id_from_millis(started_at);
    let hour_id = FrankenStorage::hour_id_from_millis(started_at);

    storage
        .conn
        .execute_compat(
            "INSERT INTO agents (id, slug, name, version, kind, created_at, updated_at)
             VALUES (?1, ?2, ?3, NULL, ?4, 0, 0)",
            fparams![1_i64, "codex", "Codex", "cli"],
        )
        .unwrap();
    storage
        .conn
        .execute_compat(
            "INSERT INTO agents (id, slug, name, version, kind, created_at, updated_at)
                 VALUES (?1, ?2, ?3, NULL, ?4, 0, 0)",
            fparams![2_i64, "claude", "Claude", "cli"],
        )
        .unwrap();

    storage
        .conn
        .execute_compat(
            "INSERT INTO conversations (
                id, agent_id, workspace_id, source_id, external_id, title, source_path,
                started_at, ended_at, approx_tokens, metadata_json, origin_host, metadata_bin
             ) VALUES (?1, ?2, NULL, ?3, ?4, ?5, ?6, ?7, ?8, NULL, ?9, NULL, NULL)",
            fparams![
                1_i64,
                1_i64,
                LOCAL_SOURCE_ID,
                "daily-a",
                "Daily A",
                "/tmp/daily-a.jsonl",
                started_at,
                started_at + 200,
                "{}"
            ],
        )
        .unwrap();
    storage
        .conn
        .execute_compat(
            "INSERT INTO conversations (
                id, agent_id, workspace_id, source_id, external_id, title, source_path,
                started_at, ended_at, approx_tokens, metadata_json, origin_host, metadata_bin
             ) VALUES (?1, ?2, NULL, ?3, ?4, ?5, ?6, ?7, ?8, NULL, ?9, NULL, NULL)",
            fparams![
                2_i64,
                2_i64,
                LOCAL_SOURCE_ID,
                "daily-b",
                "Daily B",
                "/tmp/daily-b.jsonl",
                started_at,
                started_at + 300,
                "{}"
            ],
        )
        .unwrap();

    storage
        .conn
        .execute_compat(
            "INSERT INTO messages (
                    id, conversation_id, idx, role, author, created_at, content, extra_json, extra_bin
                 ) VALUES (?1, ?2, ?3, ?4, NULL, ?5, ?6, NULL, NULL)",
            // The schema permits the full signed `idx` domain. Pin the
            // first-page scan so it cannot silently skip negative indices.
            fparams![1_i64, 1_i64, i64::MIN, "user", started_at, "hello"],
        )
        .unwrap();
    storage
        .conn
        .execute_compat(
            "INSERT INTO messages (
                id, conversation_id, idx, role, author, created_at, content, extra_json, extra_bin
             ) VALUES (?1, ?2, ?3, ?4, NULL, ?5, ?6, NULL, NULL)",
            fparams![
                2_i64,
                1_i64,
                1_i64,
                "assistant",
                started_at + 100,
                "response"
            ],
        )
        .unwrap();
    storage
        .conn
        .execute_compat(
            "INSERT INTO messages (
                id, conversation_id, idx, role, author, created_at, content, extra_json, extra_bin
             ) VALUES (?1, ?2, ?3, ?4, NULL, ?5, ?6, NULL, NULL)",
            fparams![3_i64, 2_i64, 0_i64, "user", started_at + 50, "abc"],
        )
        .unwrap();

    for (message_id, agent_slug, role, content_len) in [
        (1_i64, "codex", "user", 5_i64),
        (2_i64, "codex", "assistant", 8_i64),
        (3_i64, "claude", "user", 3_i64),
    ] {
        storage
                .conn
                .execute_compat(
                    "INSERT INTO message_metrics (
                        message_id, created_at_ms, hour_id, day_id, agent_slug, workspace_id, source_id,
                    role, content_chars, content_tokens_est, api_input_tokens, api_output_tokens,
                    api_cache_read_tokens, api_cache_creation_tokens, api_thinking_tokens,
                    api_service_tier, api_data_source, tool_call_count, has_tool_calls, has_plan,
                    model_name, model_family, model_tier, provider
                 ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7,
                    ?8, ?9, ?10, ?11, ?12,
                    ?13, ?14, ?15,
                    ?16, ?17, ?18, ?19, ?20,
                    ?21, ?22, ?23, ?24
                 )",
                fparams![
                    message_id,
                    started_at,
                    hour_id,
                    day_id,
                    agent_slug,
                    0_i64,
                    LOCAL_SOURCE_ID,
                    role,
                    content_len,
                    content_len / 4,
                    0_i64,
                    0_i64,
                    0_i64,
                    0_i64,
                    0_i64,
                    "",
                    "estimated",
                    0_i64,
                    0_i64,
                    0_i64,
                    "",
                    "unknown",
                        "unknown",
                        "unknown"
                ],
            )
            .unwrap();
    }

    storage
        .conn
        .execute_compat(
            "INSERT INTO daily_stats (
                day_id, agent_slug, source_id, session_count, message_count, total_chars, last_updated
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            fparams![999_i64, "stale", "stale", 99_i64, 99_i64, 99_i64, 0_i64],
        )
        .unwrap();

    let rebuilt = storage.rebuild_daily_stats().unwrap();
    assert_eq!(rebuilt.total_sessions, 2);

    let health = storage.daily_stats_health().unwrap();
    assert_eq!(health.conversation_count, 2);
    assert_eq!(health.materialized_total, 2);
    assert_eq!(health.drift, 0);

    let total_messages: i64 = storage
        .conn
        .query_row_map(
            "SELECT message_count FROM daily_stats WHERE agent_slug = 'all' AND source_id = 'all'",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(total_messages, 3);

    let stale_rows: i64 = storage
        .conn
        .query_row_map(
            "SELECT COUNT(*) FROM daily_stats WHERE agent_slug = 'stale'",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(
        stale_rows, 0,
        "atomic publish must replace stale live stats"
    );

    let staged_rows: i64 = storage
        .conn
        .query_row_map(
            "SELECT COUNT(*) FROM daily_stats_rebuild_stage",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(staged_rows, 0, "successful publish must clear staging rows");
}

#[test]
fn rebuild_daily_stats_uses_canonical_message_bytes_when_metrics_are_stale() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let content = "ASCII🙂é漢字";
    let expected_bytes = content.len() as i64;
    let started_at = 1_704_067_200_000_i64;
    let day_id = FrankenStorage::day_id_from_millis(started_at);
    let hour_id = FrankenStorage::hour_id_from_millis(started_at);

    storage
        .conn
        .execute_compat(
            "INSERT INTO agents (id, slug, name, version, kind, created_at, updated_at)
             VALUES (?1, ?2, ?3, NULL, ?4, 0, 0)",
            fparams![1_i64, "tester", "Tester", "cli"],
        )
        .unwrap();
    storage
        .conn
        .execute_compat(
            "INSERT INTO conversations (
                id, agent_id, workspace_id, source_id, external_id, title, source_path,
                started_at, ended_at, approx_tokens, metadata_json, origin_host, metadata_bin
             ) VALUES (?1, ?2, NULL, ?3, ?4, ?5, ?6, ?7, NULL, NULL, ?8, NULL, NULL)",
            fparams![
                1_i64,
                1_i64,
                LOCAL_SOURCE_ID,
                "unicode-metrics",
                "Unicode Metrics",
                "/tmp/unicode-metrics.jsonl",
                started_at,
                "{}"
            ],
        )
        .unwrap();
    storage
        .conn
            .execute_compat(
                "INSERT INTO messages (
                    id, conversation_id, idx, role, author, created_at, content, extra_json, extra_bin
                 ) VALUES (?1, ?2, ?3, ?4, NULL, ?5, ?6, NULL, NULL)",
                fparams![1_i64, 1_i64, 0_i64, "user", started_at, content],
            )
            .unwrap();
    storage
        .conn
        .execute_compat(
            "INSERT INTO message_metrics (
                    message_id, created_at_ms, hour_id, day_id, agent_slug, workspace_id, source_id,
                role, content_chars, content_tokens_est, api_input_tokens, api_output_tokens,
                api_cache_read_tokens, api_cache_creation_tokens, api_thinking_tokens,
                api_service_tier, api_data_source, tool_call_count, has_tool_calls, has_plan,
                model_name, model_family, model_tier, provider
             ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7,
                ?8, ?9, ?10, ?11, ?12,
                ?13, ?14, ?15,
                ?16, ?17, ?18, ?19, ?20,
                ?21, ?22, ?23, ?24
             )",
            fparams![
                1_i64,
                started_at,
                hour_id,
                day_id,
                "tester",
                0_i64,
                LOCAL_SOURCE_ID,
                "user",
                // A complete-but-stale derived row must not override the
                // canonical message bytes during the rebuild.
                expected_bytes + 10_000,
                expected_bytes / 4,
                0_i64,
                0_i64,
                0_i64,
                0_i64,
                0_i64,
                "",
                "estimated",
                0_i64,
                0_i64,
                0_i64,
                "",
                "unknown",
                "unknown",
                "unknown"
            ],
        )
        .unwrap();

    let mut tx = storage
        .conn
        .transaction_with_behavior(crate::franken_sync::TransactionBehavior::Immediate)
        .unwrap();
    franken_update_daily_stats_in_tx(
        &storage,
        &tx,
        "tester",
        LOCAL_SOURCE_ID,
        Some(started_at),
        StatsDelta {
            session_count_delta: 1,
            message_count_delta: 1,
            total_chars_delta: expected_bytes,
        },
    )
    .unwrap();
    tx.commit().unwrap();

    let inline_total: i64 = storage
        .conn
        .query_row_map(
            "SELECT total_chars FROM daily_stats WHERE agent_slug = 'all' AND source_id = 'all'",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(inline_total, expected_bytes);

    storage.conn.execute("DELETE FROM daily_stats").unwrap();

    let rebuilt = storage.rebuild_daily_stats().unwrap();
    assert_eq!(rebuilt.total_sessions, 1);

    let rebuilt_total: i64 = storage
        .conn
        .query_row_map(
            "SELECT total_chars FROM daily_stats WHERE agent_slug = 'all' AND source_id = 'all'",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(rebuilt_total, expected_bytes);
}

#[test]
fn rebuild_daily_stats_raw_fallback_preserves_byte_counts() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let content = "fallback🙂é漢字";
    let expected_bytes = content.len() as i64;
    let started_at = 1_704_067_200_000_i64;
    storage
        .conn
        .execute_compat(
            "INSERT INTO agents (id, slug, name, version, kind, created_at, updated_at)
             VALUES (?1, ?2, ?3, NULL, ?4, 0, 0)",
            fparams![1_i64, "tester", "Tester", "cli"],
        )
        .unwrap();
    storage
        .conn
        .execute_compat(
            "INSERT INTO conversations (
                id, agent_id, workspace_id, source_id, external_id, title, source_path,
                started_at, ended_at, approx_tokens, metadata_json, origin_host, metadata_bin
             ) VALUES (?1, ?2, NULL, ?3, ?4, ?5, ?6, ?7, NULL, NULL, ?8, NULL, NULL)",
            fparams![
                1_i64,
                1_i64,
                LOCAL_SOURCE_ID,
                "unicode-fallback",
                "Unicode Fallback",
                "/tmp/unicode-fallback.jsonl",
                started_at,
                "{}"
            ],
        )
        .unwrap();
    storage
        .conn
        .execute_compat(
            "INSERT INTO messages (
                id, conversation_id, idx, role, author, created_at, content, extra_json, extra_bin
             ) VALUES (?1, ?2, ?3, ?4, NULL, ?5, ?6, NULL, NULL)",
            fparams![1_i64, 1_i64, 0_i64, "assistant", started_at, content],
        )
        .unwrap();

    let mut tx = storage
        .conn
        .transaction_with_behavior(crate::franken_sync::TransactionBehavior::Immediate)
        .unwrap();
    franken_update_daily_stats_in_tx(
        &storage,
        &tx,
        "tester",
        LOCAL_SOURCE_ID,
        Some(started_at),
        StatsDelta {
            session_count_delta: 1,
            message_count_delta: 1,
            total_chars_delta: expected_bytes,
        },
    )
    .unwrap();
    tx.commit().unwrap();

    storage.conn.execute("DELETE FROM daily_stats").unwrap();

    let rebuilt = storage.rebuild_daily_stats().unwrap();
    assert_eq!(rebuilt.total_sessions, 1);

    let rebuilt_total: i64 = storage
        .conn
        .query_row_map(
            "SELECT total_chars FROM daily_stats WHERE agent_slug = 'all' AND source_id = 'all'",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(rebuilt_total, expected_bytes);
}

#[test]
#[serial]
fn rebuild_daily_stats_reuses_streaming_statement_across_large_message_pages() {
    const MESSAGE_COUNT: i64 = 512;
    const CONTENT_BYTES: usize = 8 * 1024;

    fn linux_rss_bytes() -> Option<u64> {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        let rss_kib = status.lines().find_map(|line| {
            line.strip_prefix("VmRSS:")?
                .split_whitespace()
                .next()?
                .parse::<u64>()
                .ok()
        })?;
        rss_kib.checked_mul(1_024)
    }

    let _conversation_batch = set_env_var("CASS_DAILY_STATS_REBUILD_CONVERSATION_BATCH_SIZE", "1");
    let _message_batch = set_env_var("CASS_DAILY_STATS_REBUILD_MESSAGE_BATCH_SIZE", "1");
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("daily-stats-many-pages.db");
    let bootstrap = SqliteStorage::open(&db_path).unwrap();

    // Build the fixture outside the measured region through the same
    // SQLite compatibility transaction used by production storage.
    // Batch size one forces
    // 512 executions of the same prepared SQLite statement; the
    // pre-#329 ad-hoc materialization path retained enough per-execution
    // state to reach ~10.4 GiB on the 967k-message field corpus.
    let mut fixture_tx = bootstrap
        .conn
        .transaction_with_behavior(crate::franken_sync::TransactionBehavior::Immediate)
        .unwrap();
    fixture_tx
        .execute(
            "INSERT INTO agents(id, slug, name, kind, created_at, updated_at)
         VALUES(1, 'codex', 'Codex', 'cli', 0, 0)",
        )
        .unwrap();
    fixture_tx
        .execute_compat(
            "INSERT INTO conversations(
                 id, agent_id, source_id, external_id, source_path, started_at
             ) VALUES(1, 1, 'local', 'cass-329-many-pages', '/tmp/cass-329.jsonl', ?1)",
            fparams![1_700_000_000_000_i64],
        )
        .unwrap();
    let content = "x".repeat(CONTENT_BYTES);
    for message_id in 1..=MESSAGE_COUNT {
        fixture_tx
            .execute_compat(
                "INSERT INTO messages(id, conversation_id, idx, role, content)
                 VALUES(?1, 1, ?2, 'user', ?3)",
                fparams![message_id, message_id - 1, content.as_str()],
            )
            .unwrap();
    }
    fixture_tx.commit().unwrap();
    drop(fixture_tx);
    drop(bootstrap);

    let storage = SqliteStorage::open(&db_path).unwrap();
    let plan_details: Vec<String> = storage
        .conn
        .query_map_collect(
            "EXPLAIN QUERY PLAN \
             SELECT m.idx, COALESCE(LENGTH(CAST(m.content AS BLOB)), 0) \
             FROM messages m INDEXED BY idx_messages_conv_idx \
             WHERE m.conversation_id = ?1 AND m.idx > ?2 \
             ORDER BY m.idx \
             LIMIT ?3",
            fparams![1_i64, -1_i64, 1_i64],
            |row| row.get_typed(3),
        )
        .unwrap();
    assert!(
        plan_details
            .iter()
            .any(|detail| detail.contains("idx_messages_conv_idx")),
        "daily_stats message scan must use idx_messages_conv_idx: {plan_details:?}"
    );
    assert!(
        !plan_details
            .iter()
            .any(|detail| detail.contains("TEMP B-TREE")),
        "daily_stats message scan must not materialize a sorter: {plan_details:?}"
    );

    let rss_before = linux_rss_bytes();
    let started = std::time::Instant::now();
    let rebuilt = storage.rebuild_daily_stats().unwrap();
    let elapsed = started.elapsed();
    let rss_after = linux_rss_bytes();
    assert_eq!(rebuilt.total_sessions, 1);

    let totals: (i64, i64) = storage
        .conn
        .query_row_map(
            "SELECT message_count, total_chars
             FROM daily_stats
             WHERE agent_slug = 'all' AND source_id = 'all'",
            fparams![],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
        )
        .unwrap();
    assert_eq!(totals.0, MESSAGE_COUNT);
    assert_eq!(
        totals.1,
        MESSAGE_COUNT * i64::try_from(CONTENT_BYTES).unwrap()
    );
    assert!(
        elapsed < std::time::Duration::from_secs(60),
        "512 prepared streaming pages took {elapsed:?}"
    );
    // The staging/publish sequence must be idempotent, not additive.
    let repeated = storage.rebuild_daily_stats().unwrap();
    assert_eq!(repeated, rebuilt);

    // pd5nw: /proc RSS is process-wide, so inside the parallel lib-test
    // process one sample includes every neighbour's allocator churn
    // (observed 289 MB..1.38 GB while the same revision passes alone).
    // Sample twice — re-measure a settled second rebuild only when the
    // first sample exceeds the bound — and judge the smaller growth: a
    // genuine per-rebuild leak shows in both samples, transient
    // neighbour churn does not.
    const RSS_GROWTH_BOUND: u64 = 256 * 1024 * 1024;
    if let (Some(before), Some(after)) = (rss_before, rss_after) {
        let mut growth = after.saturating_sub(before);
        if growth >= RSS_GROWTH_BOUND {
            std::thread::sleep(std::time::Duration::from_millis(500));
            let settled_before = linux_rss_bytes();
            let _ = storage.rebuild_daily_stats().unwrap();
            let settled_after = linux_rss_bytes();
            if let (Some(b), Some(a)) = (settled_before, settled_after) {
                growth = growth.min(a.saturating_sub(b));
            }
        }
        assert!(
            growth < RSS_GROWTH_BOUND,
            "prepared streaming rebuild grew RSS by {growth} bytes in both samples"
        );
    }
}
