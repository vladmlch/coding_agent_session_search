use super::*;
use crate::franken_sync::SqliteValue;
use crate::franken_sync::compat::{
    ConnectionExt as FrankenConnectionExt, RowExt as FrankenRowExt,
};
use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use serial_test::serial;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

// =========================================================================
// Agent storage tests (bead yln.4)
// =========================================================================

#[test]
fn ensure_agent_creates_new() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let agent = Agent {
        id: None,
        slug: "test_agent".into(),
        name: "Test Agent".into(),
        version: Some("1.0".into()),
        kind: AgentKind::Cli,
    };

    let id = storage.ensure_agent(&agent).unwrap();
    assert!(id > 0);
}

fn gh459_seed_workspace_analytics(
    storage: &FrankenStorage,
    label: &str,
    workspace: &Path,
) -> (i64, i64, i64, Conversation) {
    let agent_id = storage
        .ensure_agent(&Agent {
            id: None,
            slug: "cursor".into(),
            name: "Cursor".into(),
            version: None,
            kind: AgentKind::Cli,
        })
        .unwrap();
    let workspace_id = storage.ensure_workspace(workspace, None).unwrap();
    let conv = Conversation {
        id: None,
        agent_slug: "cursor".into(),
        workspace: Some(workspace.to_path_buf()),
        external_id: Some(label.into()),
        title: Some(label.into()),
        source_path: PathBuf::from(format!("/cursor/{label}.jsonl")),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_090_000_000),
        approx_tokens: None,
        metadata_json: serde_json::json!({"cursor_format":"agent"}),
        messages: (0..2)
            .map(|idx| Message {
                id: None,
                idx,
                role: if idx == 0 {
                    MessageRole::User
                } else {
                    MessageRole::Agent
                },
                author: None,
                created_at: Some(1_700_000_000_000 + idx * 90_000_000),
                content: format!("{label} retained message {idx}"),
                extra_json: serde_json::json!({"keep":idx}),
                snippets: vec![],
            })
            .collect(),
        source_id: "local".into(),
        origin_host: None,
    };
    let id = storage
        .insert_conversations_batched_with_analytics(&[(agent_id, Some(workspace_id), &conv)], true)
        .unwrap()[0]
        .conversation_id;
    let messages = storage.fetch_messages(id).unwrap();
    let mut tx = storage
        .conn
        .transaction_with_behavior(crate::franken_sync::TransactionBehavior::Immediate)
        .unwrap();
    let mut metrics = Vec::new();
    let mut tokens = Vec::new();
    let mut rollups = AnalyticsRollupAggregator::new();
    for message in messages {
        let api = message.idx == 1;
        let timestamp = message.created_at.unwrap();
        let entry = MessageMetricsEntry {
            message_id: message.id.unwrap(),
            created_at_ms: timestamp,
            hour_id: FrankenStorage::hour_id_from_millis(timestamp),
            day_id: FrankenStorage::day_id_from_millis(timestamp),
            agent_slug: "cursor".into(),
            workspace_id,
            source_id: "local".into(),
            role: if api { "assistant" } else { "user" }.into(),
            content_chars: message.content.len() as i64,
            // Deliberately different from content length / 4. A dimension
            // move must retain stored measurements, not re-extract them.
            content_tokens_est: if api { 251 } else { 117 },
            model_name: api.then(|| "claude-opus-4".into()),
            model_family: if api { "claude" } else { "unknown" }.into(),
            model_tier: if api { "opus" } else { "unknown" }.into(),
            provider: if api { "anthropic" } else { "unknown" }.into(),
            api_input_tokens: api.then_some(131),
            api_output_tokens: api.then_some(59),
            api_cache_read_tokens: api.then_some(17),
            api_cache_creation_tokens: api.then_some(7),
            api_thinking_tokens: api.then_some(23),
            api_service_tier: api.then(|| "standard".into()),
            api_data_source: if api { "api" } else { "estimated" }.into(),
            tool_call_count: i64::from(api) * 2,
            has_tool_calls: api,
            has_plan: api,
        };
        tokens.push(TokenUsageEntry {
            message_id: entry.message_id,
            conversation_id: id,
            agent_id,
            workspace_id: Some(workspace_id),
            source_id: entry.source_id.clone(),
            timestamp_ms: timestamp,
            day_id: entry.day_id,
            model_name: entry.model_name.clone(),
            model_family: Some(entry.model_family.clone()),
            model_tier: Some(entry.model_tier.clone()),
            service_tier: entry.api_service_tier.clone(),
            provider: Some(entry.provider.clone()),
            input_tokens: entry.api_input_tokens,
            output_tokens: entry.api_output_tokens,
            cache_read_tokens: entry.api_cache_read_tokens,
            cache_creation_tokens: entry.api_cache_creation_tokens,
            thinking_tokens: entry.api_thinking_tokens,
            total_tokens: api.then_some(237),
            estimated_cost_usd: api.then_some(0.125),
            role: entry.role.clone(),
            content_chars: entry.content_chars,
            has_tool_calls: api,
            tool_call_count: u32::from(api) * 2,
            data_source: entry.api_data_source.clone(),
        });
        rollups.record(&entry);
        metrics.push(entry);
    }
    franken_insert_token_usage_batched_in_tx(&tx, &tokens).unwrap();
    franken_insert_message_metrics_batched_in_tx(&tx, &metrics).unwrap();
    franken_flush_analytics_rollups_in_tx(&tx, &rollups).unwrap();
    franken_update_conversation_token_summaries_in_tx(&tx, id).unwrap();
    tx.commit().unwrap();
    (agent_id, workspace_id, id, conv)
}

fn gh459_analytics_rows_without_workspace(
    storage: &FrankenStorage,
    table: &str,
) -> Vec<Vec<SqliteValue>> {
    let columns: Vec<String> = storage
        .raw()
        .query_map_collect(&format!("PRAGMA table_info({table})"), fparams![], |row| {
            row.get_typed(1)
        })
        .unwrap()
        .into_iter()
        .filter(|column: &String| column != "workspace_id")
        .collect();
    storage
        .raw()
        .query(&format!(
            "SELECT {} FROM {table} ORDER BY 1",
            columns.join(",")
        ))
        .unwrap()
        .into_iter()
        .map(|row| row.values().to_vec())
        .collect()
}

fn gh459_rollup_amounts(storage: &FrankenStorage) -> Vec<Vec<SqliteValue>> {
    ["usage_hourly", "usage_daily", "usage_models_daily"]
        .into_iter()
        .map(|table| {
            let columns: Vec<String> = storage
                .raw()
                .query_map_collect(&format!("PRAGMA table_info({table})"), fparams![], |row| {
                    Ok((row.get_typed::<String>(1)?, row.get_typed::<String>(2)?))
                })
                .unwrap()
                .into_iter()
                .filter(|(column, kind)| {
                    kind == "INTEGER"
                        && !matches!(
                            column.as_str(),
                            "hour_id" | "day_id" | "workspace_id" | "last_updated"
                        )
                })
                .map(|(column, _)| format!("SUM({column})"))
                .collect();
            storage
                .raw()
                .query(&format!("SELECT {} FROM {table}", columns.join(",")))
                .unwrap()[0]
                .values()
                .to_vec()
        })
        .collect()
}

#[test]
fn gh459_analytics_workspace_move_conserves_shared_buckets_and_stored_amounts() {
    let dir = TempDir::new().unwrap();
    let storage = FrankenStorage::open(&dir.path().join("analytics.db")).unwrap();
    let (agent, old, id, mut conv) =
        gh459_seed_workspace_analytics(&storage, "moving", Path::new("/old"));
    let (_, _, sibling, _) = gh459_seed_workspace_analytics(&storage, "staying", Path::new("/old"));
    let (_, new, _, _) = gh459_seed_workspace_analytics(&storage, "destination", Path::new("/new"));
    storage.rebuild_token_daily_stats().unwrap();
    let metrics = gh459_analytics_rows_without_workspace(&storage, "message_metrics");
    let tokens = gh459_analytics_rows_without_workspace(&storage, "token_usage");
    assert_eq!(metrics.len(), 6);
    assert_eq!(tokens.len(), 6);
    let (api_input, cost): (i64, f64) = storage
        .raw()
        .query_row_map(
            "SELECT SUM(input_tokens), SUM(estimated_cost_usd) FROM token_usage",
            fparams![],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
        )
        .unwrap();
    assert_eq!(api_input, 393);
    assert_eq!(cost, 0.375);
    let track_b = gh459_analytics_rows_without_workspace(&storage, "token_daily_stats");
    let amounts = gh459_rollup_amounts(&storage);
    let messages = serde_json::to_value(storage.fetch_messages(id).unwrap()).unwrap();
    let timestamps: Vec<Vec<Vec<SqliteValue>>> =
        ["usage_hourly", "usage_daily", "usage_models_daily"]
            .into_iter()
            .map(|table| {
                storage
                    .raw()
                    .query(&format!(
                        "SELECT workspace_id, last_updated FROM {table} ORDER BY workspace_id, 2"
                    ))
                    .unwrap()
                    .into_iter()
                    .map(|row| row.values().to_vec())
                    .collect()
            })
            .collect();
    conv.workspace = Some(PathBuf::from("/new"));
    conv.metadata_json["cursor_workspace_attribution"] = serde_json::json!("workspace_trusted");
    for replay in 0..2 {
        // Even explicit deferred creation must correct existing attribution.
        let outcome = storage
            .insert_conversations_batched_with_analytics(&[(agent, Some(new), &conv)], true)
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(outcome.workspace_changed, replay == 0);
        assert!(outcome.inserted_indices.is_empty());
        assert_eq!(outcome.conversation_id, id);
        assert_eq!(
            gh459_analytics_rows_without_workspace(&storage, "message_metrics"),
            metrics
        );
        assert_eq!(
            gh459_analytics_rows_without_workspace(&storage, "token_usage"),
            tokens
        );
        assert_eq!(
            gh459_analytics_rows_without_workspace(&storage, "token_daily_stats"),
            track_b
        );
        assert_eq!(gh459_rollup_amounts(&storage), amounts);
        for (index, table) in ["usage_hourly", "usage_daily", "usage_models_daily"]
            .into_iter()
            .enumerate()
        {
            let counts: Vec<(i64, i64)> = storage.raw().query_map_collect(
                &format!("SELECT workspace_id, SUM(message_count) FROM {table} GROUP BY workspace_id ORDER BY workspace_id"), fparams![],
                |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
            ).unwrap();
            assert_eq!(counts, vec![(old, 2), (new, 4)]);
            let after: Vec<Vec<SqliteValue>> = storage
                .raw()
                .query(&format!(
                    "SELECT workspace_id, last_updated FROM {table} ORDER BY workspace_id, 2"
                ))
                .unwrap()
                .into_iter()
                .map(|row| row.values().to_vec())
                .collect();
            assert_eq!(after, timestamps[index]);
        }
        let sibling_workspace: Option<i64> = storage
            .raw()
            .query_row_map(
                "SELECT workspace_id FROM token_usage WHERE conversation_id = ?1 LIMIT 1",
                fparams![sibling],
                |row| row.get_typed(0),
            )
            .unwrap();
        assert_eq!(sibling_workspace, Some(old));
        let moved_token_workspaces: Vec<i64> = storage
            .raw()
            .query_map_collect(
                "SELECT workspace_id FROM token_usage WHERE conversation_id = ?1",
                fparams![id],
                |row| row.get_typed(0),
            )
            .unwrap();
        assert_eq!(moved_token_workspaces, vec![new, new]);
        let moved_metrics: i64 = storage
            .raw()
            .query_row_map(
                "SELECT COUNT(*) FROM message_metrics WHERE workspace_id = ?1",
                fparams![new],
                |row| row.get_typed(0),
            )
            .unwrap();
        assert_eq!(moved_metrics, 4);
        assert_eq!(
            serde_json::to_value(storage.fetch_messages(id).unwrap()).unwrap(),
            messages
        );
    }
    let new_bucket_timestamps: Vec<Vec<i64>> =
        ["usage_hourly", "usage_daily", "usage_models_daily"]
            .into_iter()
            .map(|table| {
                storage
            .raw()
            .query_map_collect(
                &format!(
                    "SELECT last_updated FROM {table} WHERE workspace_id = ?1 ORDER BY last_updated"
                ),
                fparams![new],
                |row| row.get_typed(0),
            )
            .unwrap()
            })
            .collect();
    conv.workspace = None;
    conv.metadata_json["cursor_workspace_attribution"] = serde_json::json!("unresolved");
    for replay in 0..2 {
        let outcome = storage
            .insert_conversation_tree(agent, None, &conv)
            .unwrap();
        assert_eq!(outcome.workspace_changed, replay == 0);
        assert!(outcome.inserted_indices.is_empty());
        let workspaces: Vec<Option<i64>> = storage
            .raw()
            .query_map_collect(
                "SELECT workspace_id FROM token_usage WHERE conversation_id = ?1",
                fparams![id],
                |row| row.get_typed(0),
            )
            .unwrap();
        assert_eq!(workspaces, vec![None, None]);
        let metric_workspaces: Vec<i64> = storage.raw().query_map_collect("SELECT workspace_id FROM message_metrics WHERE message_id IN (SELECT id FROM messages WHERE conversation_id = ?1)", fparams![id], |row| row.get_typed(0)).unwrap();
        assert_eq!(metric_workspaces, vec![0, 0]);
        assert_eq!(gh459_rollup_amounts(&storage), amounts);
        assert_eq!(
            gh459_analytics_rows_without_workspace(&storage, "message_metrics"),
            metrics
        );
        assert_eq!(
            gh459_analytics_rows_without_workspace(&storage, "token_usage"),
            tokens
        );
        for (index, table) in ["usage_hourly", "usage_daily", "usage_models_daily"]
            .into_iter()
            .enumerate()
        {
            let timestamps: Vec<i64> = storage.raw().query_map_collect(&format!("SELECT last_updated FROM {table} WHERE workspace_id = 0 ORDER BY last_updated"), fparams![], |row| row.get_typed(0)).unwrap();
            assert_eq!(
                timestamps, new_bucket_timestamps[index],
                "new unknown-workspace buckets inherit the stored source timestamps"
            );
        }
    }
}

#[test]
fn gh459_analytics_replay_repairs_stale_rows_after_buffered_append_and_canonical_noop() {
    let dir = TempDir::new().unwrap();
    let storage = FrankenStorage::open(&dir.path().join("analytics.db")).unwrap();
    let (agent, old, id, mut conv) =
        gh459_seed_workspace_analytics(&storage, "moving", Path::new("/old"));
    let new = storage.ensure_workspace(Path::new("/new"), None).unwrap();
    // Simulate an archive whose canonical identity was already repaired by
    // an older build, while both existing analytics rows stayed behind.
    storage
        .raw()
        .execute_compat(
            "UPDATE conversations SET workspace_id = ?1 WHERE id = ?2",
            fparams![new, id],
        )
        .unwrap();
    conv.workspace = Some(PathBuf::from("/new"));
    conv.metadata_json["cursor_workspace_attribution"] = serde_json::json!("workspace_trusted");
    let no_change = storage
        .insert_conversations_batched_with_analytics(&[(agent, Some(new), &conv)], false)
        .unwrap()
        .pop()
        .unwrap();
    assert!(!no_change.workspace_changed);
    assert!(no_change.inserted_indices.is_empty());
    let old_count: i64 = storage
        .raw()
        .query_row_map(
            "SELECT COUNT(*) FROM message_metrics WHERE workspace_id = ?1",
            fparams![old],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(old_count, 0);
    let before_messages = storage.fetch_messages(id).unwrap();
    let before_amounts = gh459_rollup_amounts(&storage);
    let mut appended = conv.clone();
    appended.workspace = Some(PathBuf::from("/old"));
    appended.messages.push(Message {
        id: None,
        idx: 2,
        role: MessageRole::User,
        author: None,
        created_at: Some(1_700_090_001_000),
        content: "one truly new appended message".into(),
        extra_json: serde_json::json!({}),
        snippets: vec![],
    });
    // First packet buffers new metrics for /old; second packet returns
    // canonical identity to /new before that buffer has reached storage.
    let outcomes = storage
        .insert_conversations_batched_with_analytics(
            &[(agent, Some(old), &appended), (agent, Some(new), &conv)],
            false,
        )
        .unwrap();
    assert_eq!(outcomes[0].inserted_indices, vec![2]);
    assert!(outcomes[1].inserted_indices.is_empty());
    let rows = storage.fetch_messages(id).unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(
        serde_json::to_value(&rows[..2]).unwrap(),
        serde_json::to_value(before_messages).unwrap()
    );
    for table in ["message_metrics", "token_usage"] {
        let workspaces: Vec<i64> = storage
            .raw()
            .query_map_collect(
                &format!("SELECT workspace_id FROM {table}"),
                fparams![],
                |row| row.get_typed(0),
            )
            .unwrap();
        assert_eq!(workspaces, vec![new, new, new]);
    }
    let after_amounts = gh459_rollup_amounts(&storage);
    assert_ne!(after_amounts, before_amounts);
    for table in ["usage_hourly", "usage_daily", "usage_models_daily"] {
        let total: i64 = storage
            .raw()
            .query_row_map(
                &format!("SELECT SUM(message_count) FROM {table}"),
                fparams![],
                |row| row.get_typed(0),
            )
            .unwrap();
        assert_eq!(total, 3, "append must be counted once in {table}");
    }
    let replay = storage
        .insert_conversations_batched_with_analytics(&[(agent, Some(new), &conv)], false)
        .unwrap();
    assert!(replay[0].inserted_indices.is_empty());
    assert_eq!(gh459_rollup_amounts(&storage), after_amounts);
}

#[test]
fn gh459_analytics_relocation_rejects_drift_and_rolls_back_with_identity() {
    for drift in ["missing", "underfilled"] {
        let dir = TempDir::new().unwrap();
        let storage = FrankenStorage::open(&dir.path().join("analytics.db")).unwrap();
        let (agent, old, id, mut conv) =
            gh459_seed_workspace_analytics(&storage, "moving", Path::new("/old"));
        let new = storage.ensure_workspace(Path::new("/new"), None).unwrap();
        conv.workspace = Some(PathBuf::from("/new"));
        conv.metadata_json["cursor_workspace_attribution"] = serde_json::json!("workspace_trusted");
        let before = gh459_analytics_rows_without_workspace(&storage, "token_usage");
        if drift == "missing" {
            storage
                .raw()
                .execute("DELETE FROM usage_models_daily")
                .unwrap();
        } else {
            storage
                .raw()
                .execute("UPDATE usage_models_daily SET content_tokens_est_total = 0")
                .unwrap();
        }
        let tables = [
            "conversations",
            "messages",
            "message_metrics",
            "token_usage",
            "usage_hourly",
            "usage_daily",
            "usage_models_daily",
            "meta",
        ];
        let snapshot = || {
            tables
                .iter()
                .map(|table| {
                    storage
                        .raw()
                        .query(&format!("SELECT * FROM {table} ORDER BY 1"))
                        .unwrap()
                        .into_iter()
                        .map(|row| row.values().to_vec())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        };
        let original = snapshot();
        let error = storage
            .insert_conversations_batched_with_analytics(&[(agent, Some(new), &conv)], false)
            .err()
            .expect("damaged source rollup must reject relocation");
        assert!(format!("{error:#}").contains("Cursor workspace analytics drift"));
        assert_eq!(
            snapshot(),
            original,
            "late model-rollup failure must roll back earlier hourly/daily moves and canonical/semantic changes"
        );
        assert_eq!(
            gh459_analytics_rows_without_workspace(&storage, "token_usage"),
            before
        );
        assert_eq!(
            storage.list_conversations(10, 0).unwrap()[0].workspace,
            Some(PathBuf::from("/old"))
        );
        let metric_workspace: i64 = storage
            .raw()
            .query_row_map(
                "SELECT workspace_id FROM message_metrics LIMIT 1",
                fparams![],
                |row| row.get_typed(0),
            )
            .unwrap();
        assert_eq!(metric_workspace, old);
        // Neither a generic partial packet nor another provider may move
        // rows, even when the private helper is called directly.
        for metadata in [
            serde_json::json!({}),
            serde_json::json!({"cursor_format":"ide", "cursor_workspace_attribution":"workspace_trusted"}),
        ] {
            conv.metadata_json = metadata;
            let outcome = storage
                .insert_conversations_batched_with_analytics(&[(agent, Some(new), &conv)], false)
                .unwrap();
            assert!(!outcome[0].workspace_changed);
            assert_eq!(snapshot(), original);
        }
        conv.agent_slug = "codex".into();
        conv.metadata_json = serde_json::json!({"cursor_format":"agent", "cursor_workspace_attribution":"workspace_trusted"});
        let mut tx = storage
            .conn
            .transaction_with_behavior(crate::franken_sync::TransactionBehavior::Immediate)
            .unwrap();
        franken_reassociate_provider_analytics_workspace(&tx, id, &conv).unwrap();
        tx.commit().unwrap();
        assert_eq!(snapshot(), original);
    }
}

#[test]
fn gh447_native_fifo_retains_history_and_indices_across_restarts() {
    for batched in [false, true] {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("native.db");
        let storage = FrankenStorage::open(&path).unwrap();
        let agent = storage
            .ensure_agent(&Agent {
                id: None,
                slug: "grok_bot".into(),
                name: "Grok Bot".into(),
                version: None,
                kind: AgentKind::Cli,
            })
            .unwrap();
        let first = gh447_native_window(1, 200);
        let original = gh447_persist(&storage, agent, &first, batched).unwrap();
        assert_eq!(original.inserted_indices, (0..200).collect::<Vec<_>>());
        let before = storage.fetch_messages(original.conversation_id).unwrap();
        assert_eq!(
            before.len(),
            200,
            "distinct native IDs survive identical text/time"
        );
        let encoded: i64 = storage
            .raw()
            .query_row_map(
                "SELECT COUNT(*) FROM messages WHERE extra_bin IS NOT NULL AND extra_json IS NULL",
                fparams![],
                |row| row.get_typed(0),
            )
            .unwrap();
        assert_eq!(
            encoded, 200,
            "replay must actually read MessagePack identities"
        );
        let rolled = gh447_native_window(2, 200);
        let outcome = gh447_persist(&storage, agent, &rolled, batched).unwrap();
        assert_eq!(outcome.conversation_id, original.conversation_id);
        assert!(!outcome.conversation_inserted);
        assert_eq!(outcome.inserted_indices, vec![200]);
        let after = storage.fetch_messages(original.conversation_id).unwrap();
        assert_eq!(after.len(), 201);
        assert_eq!(
            serde_json::to_value(&after[..200]).unwrap(),
            serde_json::to_value(&before).unwrap()
        );
        assert_eq!(after[200].extra_json["grok_bot_entry_id"], "entry-201");
        assert_eq!(after[200].idx, 200);
        drop(storage);
        let storage = FrankenStorage::open(&path).unwrap();
        for replay in [&rolled, &first] {
            let outcome = gh447_persist(&storage, agent, replay, batched).unwrap();
            assert!(outcome.inserted_indices.is_empty());
            assert_eq!(outcome.conversation_id, original.conversation_id);
        }
        let mut nonoverlap = gh447_native_window(1000, 3);
        for message in &mut nonoverlap.messages {
            message.created_at = None;
        }
        nonoverlap.ended_at = None;
        let outcome = gh447_persist(&storage, agent, &nonoverlap, batched).unwrap();
        assert_eq!(outcome.inserted_indices, vec![201, 202, 203]);
        assert!(
            gh447_persist(&storage, agent, &nonoverlap, batched)
                .unwrap()
                .inserted_indices
                .is_empty()
        );
        let saved = storage.fetch_messages(original.conversation_id).unwrap();
        assert_eq!(saved.len(), 204);
        assert_eq!(
            serde_json::to_value(&saved[..201]).unwrap(),
            serde_json::to_value(&after).unwrap()
        );
    }
}

#[test]
fn gh426_source_completion_commits_with_canonical_rows_and_rolls_back_on_failure() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("ledger.db");
    let storage = FrankenStorage::open(&path).unwrap();
    let agent = storage
        .ensure_agent(&Agent {
            id: None,
            slug: "grok_bot".into(),
            name: "Grok Bot".into(),
            version: None,
            kind: AgentKind::Cli,
        })
        .unwrap();
    let completion = SourceIngestLedgerEntry {
        key: "source_ingest_v1:sample".into(),
        observation: "verified source observation".into(),
    };
    let first = gh447_native_window(1, 2);
    let mut invalid = gh447_native_window(3, 1);
    invalid.messages[0].extra_json = serde_json::Value::Null;
    assert!(
        storage
            .insert_conversations_batched_with_source_completion(
                &[(agent, None, &first), (agent, None, &invalid)],
                &completion,
            )
            .is_err()
    );
    assert!(storage.source_ingest_ledger_entries().unwrap().is_empty());
    let messages: i64 = storage
        .raw()
        .query_row_map("SELECT COUNT(*) FROM messages", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(
        messages, 0,
        "failed final batch must roll back earlier canonical writes"
    );
    let outcomes = storage
        .insert_conversations_batched_with_source_completion(&[(agent, None, &first)], &completion)
        .unwrap();
    assert_eq!(outcomes[0].inserted_indices, vec![0, 1]);
    drop(storage);
    let storage = FrankenStorage::open(&path).unwrap();
    assert_eq!(
        storage
            .source_ingest_ledger_entries()
            .unwrap()
            .get(&completion.key),
        Some(&completion.observation)
    );
    assert_eq!(
        storage
            .fetch_messages(outcomes[0].conversation_id)
            .unwrap()
            .len(),
        2
    );
    let changed = SourceIngestLedgerEntry {
        key: completion.key.clone(),
        observation: "changed observation".into(),
    };
    assert!(
        storage
            .insert_conversations_batched_with_source_completion(
                &[(agent, None, &invalid)],
                &changed
            )
            .is_err()
    );
    assert_eq!(
        storage
            .source_ingest_ledger_entries()
            .unwrap()
            .get(&completion.key),
        Some(&completion.observation)
    );
    let empty = SourceIngestLedgerEntry {
        key: "source_ingest_v1:empty".into(),
        observation: "verified empty source".into(),
    };
    assert!(
        storage
            .insert_conversations_batched_with_source_completion(&[], &empty)
            .is_err()
    );
    assert!(
        !storage
            .source_ingest_ledger_entries()
            .unwrap()
            .contains_key(&empty.key)
    );
    let foreign = SourceIngestLedgerEntry {
        key: "schema_version".into(),
        observation: "bad".into(),
    };
    assert!(
        storage
            .insert_conversations_batched_with_source_completion(&[], &foreign)
            .is_err()
    );
    assert_eq!(storage.schema_version().unwrap(), CURRENT_SCHEMA_VERSION);
}

#[test]
#[serial]
fn gh423_codebuff_revision_admission_compares_payload_without_mutation() {
    let _lexical = set_env_var("CASS_DEFER_LEXICAL_UPDATES", "0");
    let dir = TempDir::new().unwrap();
    let storage = FrankenStorage::open(&dir.path().join("revision-admission.db")).unwrap();
    let agent = gh423_agent(&storage);
    let original = gh423_storage_snapshot();
    assert!(
        !storage
            .codebuff_message_revisions_needed(&original)
            .unwrap()
    );
    let outcome = storage
        .insert_conversation_tree(agent, None, &original)
        .unwrap();
    let saved = storage.fetch_messages(outcome.conversation_id).unwrap();
    let analytics = gh423_analytics_snapshot(&storage);
    assert!(
        !storage
            .codebuff_message_revisions_needed(&original)
            .unwrap()
    );

    let mut tail = original.clone();
    let mut message = tail.messages[0].clone();
    message.extra_json["id"] = serde_json::json!("new-native");
    message.extra_json["codebuff_message_id"] = serde_json::json!("new-native");
    tail.messages.push(message);
    assert!(!storage.codebuff_message_revisions_needed(&tail).unwrap());
    for mutation in 0..3 {
        let mut revised = original.clone();
        match mutation {
            0 => revised.messages[1].content = "revised admission needle".into(),
            1 => revised.messages[1].extra_json["isComplete"] = serde_json::json!(true),
            _ => revised.messages[1].snippets[0].snippet_text = Some("revised snippet".into()),
        }
        assert!(storage.codebuff_message_revisions_needed(&revised).unwrap());
        let mut isolated = revised.clone();
        isolated.source_id = "other-source".into();
        assert!(
            !storage
                .codebuff_message_revisions_needed(&isolated)
                .unwrap()
        );
        isolated = revised.clone();
        isolated.external_id = Some("other-store".into());
        assert!(
            !storage
                .codebuff_message_revisions_needed(&isolated)
                .unwrap()
        );
        isolated = revised;
        isolated.agent_slug = "other-agent".into();
        assert!(
            !storage
                .codebuff_message_revisions_needed(&isolated)
                .unwrap()
        );
    }
    let mut invalid = original.clone();
    invalid.messages[1].role = MessageRole::User;
    assert!(storage.codebuff_message_revisions_needed(&invalid).is_err());
    invalid = original.clone();
    invalid.messages.push(invalid.messages[0].clone());
    assert!(storage.codebuff_message_revisions_needed(&invalid).is_err());
    invalid = original.clone();
    invalid.messages[1].extra_json["codebuff_message_id"] = serde_json::json!("conflicting-marker");
    assert!(storage.codebuff_message_revisions_needed(&invalid).is_err());
    assert_eq!(
        serde_json::to_value(storage.fetch_messages(outcome.conversation_id).unwrap()).unwrap(),
        serde_json::to_value(&saved).unwrap(),
    );
    assert_eq!(gh423_analytics_snapshot(&storage), analytics);
    assert!(
        !storage
            .semantic_identity_rebuild_required(SemanticIdentityTier::Fast)
            .unwrap()
    );

    // Legacy JSON and current MessagePack extras use the same decoder.
    storage
        .raw()
        .execute_compat(
            "UPDATE messages SET extra_json = ?2, extra_bin = NULL WHERE id = ?1",
            fparams![
                saved[1].id.unwrap(),
                serde_json::to_string(&original.messages[1].extra_json).unwrap()
            ],
        )
        .unwrap();
    assert!(
        !storage
            .codebuff_message_revisions_needed(&original)
            .unwrap()
    );
    let mut revised = original.clone();
    gh423_revise(&mut revised);
    assert!(storage.codebuff_message_revisions_needed(&revised).unwrap());
    storage
        .insert_conversation_tree(agent, None, &revised)
        .unwrap();
    assert!(!storage.codebuff_message_revisions_needed(&revised).unwrap());
}

#[test]
#[serial]
fn gh423_codebuff_revisions_preserve_ids_payload_fts_and_analytics_all_routes() {
    let _lexical = set_env_var("CASS_DEFER_LEXICAL_UPDATES", "0");
    for route in 0..3 {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("revisions.db");
        let storage = FrankenStorage::open(&path).unwrap();
        storage.ensure_search_fallback_fts_consistency().unwrap();
        let agent = gh423_agent(&storage);
        let mut conv = gh423_storage_snapshot();
        let first = gh423_persist(&storage, agent, &conv, route).unwrap();
        assert_eq!(
            first.inserted_indices,
            vec![0, 1, 2],
            "same text/time IDs remain distinct"
        );
        assert!(first.updated_indices.is_empty());
        let before = storage.fetch_messages(first.conversation_id).unwrap();
        if route == 0 {
            // Historical JSON and current MessagePack envelopes must use
            // the same native identity reconciliation.
            storage
                .raw()
                .execute_compat(
                    "UPDATE messages SET extra_json = ?2, extra_bin = NULL WHERE id = ?1",
                    fparams![
                        before[1].id.unwrap(),
                        serde_json::to_string(&before[1].extra_json).unwrap()
                    ],
                )
                .unwrap();
        }
        storage
            .set_last_embedded_message_id(before[2].id.unwrap())
            .unwrap();
        gh423_revise(&mut conv);
        let update = gh423_persist(&storage, agent, &conv, route).unwrap();
        assert_eq!(update.conversation_id, first.conversation_id);
        assert!(update.inserted_indices.is_empty());
        assert_eq!(update.updated_indices, vec![1]);
        assert!(!update.workspace_changed);
        let after = storage.fetch_messages(first.conversation_id).unwrap();
        assert_eq!(after.len(), 3);
        for idx in 0..3 {
            assert_eq!(
                (after[idx].id, after[idx].idx),
                (before[idx].id, before[idx].idx)
            );
        }
        assert_eq!(after[1].content, conv.messages[1].content);
        assert_eq!(after[1].extra_json, conv.messages[1].extra_json);
        for idx in [0, 2] {
            assert_eq!(
                serde_json::to_value(&after[idx]).unwrap(),
                serde_json::to_value(&before[idx]).unwrap()
            );
        }
        let snippets: Vec<String> = storage
            .raw()
            .query_map_collect(
                "SELECT snippet_text FROM snippets WHERE message_id = ?1 ORDER BY id",
                fparams![after[1].id.unwrap()],
                |row| row.get_typed(0),
            )
            .unwrap();
        assert_eq!(snippets, vec!["updated snippet", "second snippet"]);
        let matches: Vec<i64> = storage
            .raw()
            .query_map_collect(
                "SELECT rowid FROM fts_messages WHERE fts_messages MATCH 'newneedle'",
                fparams![],
                |row| row.get_typed(0),
            )
            .unwrap();
        assert_eq!(matches, vec![after[1].id.unwrap()]);
        let old_matches: Vec<i64> = storage.raw().query_map_collect(
            "SELECT rowid FROM fts_messages WHERE fts_messages MATCH 'oldneedle' ORDER BY rowid",
            fparams![], |row| row.get_typed(0),
        ).unwrap();
        assert_eq!(
            old_matches,
            vec![after[0].id.unwrap(), after[2].id.unwrap()]
        );
        gh423_assert_analytics(
            &storage,
            first.conversation_id,
            3,
            conv.messages.iter().map(|m| m.content.len() as i64).sum(),
            4,
        );
        for tier in [SemanticIdentityTier::Fast, SemanticIdentityTier::Quality] {
            assert!(storage.semantic_identity_rebuild_required(tier).unwrap());
            storage.complete_semantic_identity_rebuild(tier).unwrap();
        }
        assert_eq!(storage.get_last_embedded_message_id().unwrap(), None);
        let analytics = gh423_analytics_snapshot(&storage);
        let replay = gh423_persist(&storage, agent, &conv, route).unwrap();
        assert!(replay.inserted_indices.is_empty() && replay.updated_indices.is_empty());
        assert_eq!(gh423_analytics_snapshot(&storage), analytics);
        assert!(
            !storage
                .semantic_identity_rebuild_required(SemanticIdentityTier::Fast)
                .unwrap()
        );
        drop(storage);
        let storage = FrankenStorage::open(&path).unwrap();
        let replay = gh423_persist(&storage, agent, &conv, 0).unwrap();
        assert!(replay.updated_indices.is_empty() && replay.inserted_indices.is_empty());
        assert_eq!(
            serde_json::to_value(storage.fetch_messages(first.conversation_id).unwrap()).unwrap(),
            serde_json::to_value(after).unwrap()
        );
    }
}

#[test]
#[serial]
fn gh423_codebuff_multiple_batch_revisions_and_new_native_tail_conserve_counts() {
    let _lexical = set_env_var("CASS_DEFER_LEXICAL_UPDATES", "0");
    let dir = TempDir::new().unwrap();
    let storage = FrankenStorage::open(&dir.path().join("batch.db")).unwrap();
    storage.ensure_search_fallback_fts_consistency().unwrap();
    let agent = gh423_agent(&storage);
    let first = gh423_storage_snapshot();
    let mut second = first.clone();
    gh423_revise(&mut second);
    let mut third = second.clone();
    third.messages[1].content = "thirdneedle final revision".into();
    let mut new = first.messages[0].clone();
    new.idx = 0; // A packet position collision is not a native identity collision.
    new.extra_json["id"] = serde_json::json!("new-native");
    new.extra_json["codebuff_message_id"] = serde_json::json!("new-native");
    third.messages.push(new);
    let outcomes = storage
        .insert_conversations_batched_with_analytics(
            &[
                (agent, None, &first),
                (agent, None, &second),
                (agent, None, &third),
                (agent, None, &third),
            ],
            false,
        )
        .unwrap();
    assert_eq!(outcomes[0].inserted_indices, vec![0, 1, 2]);
    assert_eq!(outcomes[1].updated_indices, vec![1]);
    assert_eq!(outcomes[2].updated_indices, vec![1]);
    assert_eq!(outcomes[2].inserted_indices, vec![3]);
    assert!(outcomes[3].updated_indices.is_empty() && outcomes[3].inserted_indices.is_empty());
    let saved = storage.fetch_messages(outcomes[0].conversation_id).unwrap();
    assert_eq!(saved.len(), 4);
    assert_eq!(saved[1].content, "thirdneedle final revision");
    assert_eq!(saved[3].idx, 3);
    assert_eq!(saved[3].extra_json["codebuff_message_id"], "new-native");
    gh423_assert_analytics(
        &storage,
        outcomes[0].conversation_id,
        4,
        third.messages.iter().map(|m| m.content.len() as i64).sum(),
        5,
    );
    let matches: Vec<i64> = storage
        .raw()
        .query_map_collect(
            "SELECT rowid FROM fts_messages WHERE fts_messages MATCH 'newneedle'",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert!(
        matches.is_empty(),
        "buffered intermediate FTS must not survive"
    );
}
