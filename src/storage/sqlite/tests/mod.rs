pub(in crate::storage::sqlite::tests) use super::*;

pub(in crate::storage::sqlite::tests) use super::aggregators_and_pricing::{
    date_str_to_day_id, has_plan_for_role, has_plan_heuristic, sql_like_match,
};
pub(in crate::storage::sqlite::tests) use super::backup_and_integrity::{
    FTS_FRANKEN_CONTENT_REVISION_PENDING, FTS_FRANKEN_REBUILD_GENERATION,
    FTS_FRANKEN_REBUILD_META_KEY, FTS_INDEXED_CANONICAL_INTERSECTION_SQL,
    backup_vacuum_error_requires_consistent_retry, copy_database_bundle,
    deserialize_msgpack_to_json, franken_fts_schema_rows, franken_read_message_extra_compat,
    is_backup_root_name, serialize_json_to_msgpack, sqlite3_cli_launch_error,
    vacuum_stage_backup_path,
};
pub(in crate::storage::sqlite::tests) use super::batch_ingest::{
    defer_storage_lexical_updates_enabled, normalized_storage_source_parts,
};
pub(in crate::storage::sqlite::tests) use super::conversation_tail_state::{
    collect_existing_conversation_noop_from_conversation_ended_at,
    collect_existing_conversation_noop_from_idx_tail,
    collect_existing_conversation_tail_from_ended_at, conversation_merge_key,
};
pub(in crate::storage::sqlite::tests) use super::forget_and_dedup::source_path_is_shared_store;
pub(in crate::storage::sqlite::tests) use super::fts_rebuild::{
    FTS_ENTRY_BATCH_MAX_CHARS, FTS_EXACT_ROWID_PROBE_CHUNK_SIZE, FTS5_BATCH_SIZE,
    FtsRebuildMessageRow, FtsRebuildPagePlan, arm_fts_page_after_plan_hook,
    arm_fts_page_diagnostics, arm_fts_rebuild_interruption, emit_fts_page_diagnostics,
    fts_exact_rowid_probe_select_sql, path_to_string, plan_fts_rebuild_message_page, role_str,
};
pub(in crate::storage::sqlite::tests) use super::lexical_rebuild::truncate_lexical_rebuild_message_content;
pub(in crate::storage::sqlite::tests) use super::omp_and_semantic_meta::{
    LEGACY_OMP_ANALYTICS_CURSOR_CONTEXT_META_KEY, LEGACY_OMP_ANALYTICS_CURSOR_LAST_ID_META_KEY,
    LEGACY_OMP_ANALYTICS_CURSOR_PROCESSED_META_KEY, LEGACY_OMP_ANALYTICS_REBUILT_META_KEY,
    LEGACY_OMP_LEXICAL_PUBLISHED_META_KEY, LEGACY_OMP_RECLASSIFICATION_META_KEY,
    PREVIOUS_LEGACY_OMP_RECLASSIFICATION_META_KEY,
};
pub(in crate::storage::sqlite::tests) use super::open_and_locks::{
    defer_analytics_updates_guard_for, doctor_lock_metadata_pid_is_current_process,
    doctor_mutation_lock_path_for_db_open, schema_check_error_requires_rebuild,
};
pub(in crate::storage::sqlite::tests) use super::orphan_fk_cleanup::{
    ORPHAN_FK_ID_CHUNK_SIZE, franken_table_column_names,
};
pub(in crate::storage::sqlite::tests) use super::reconcile_and_codebuff::{
    InsertConversationTreePerfProfile, conversation_external_lookup_key,
    conversation_tail_ended_at_candidate, franken_reconcile_provider_workspace,
    provider_workspace_is_authoritative, replay_owns_provider_workspace,
};
pub(in crate::storage::sqlite::tests) use super::source_and_historical_import::{
    HISTORICAL_RECOVERY_CORE_SCHEMA, HistoricalBatchEntry, HistoricalBatchImportTotals,
    historical_raw_json, historical_table_exists, json_value_size_hint,
    parse_historical_json_column, wrap_historical_raw_json,
};
pub(in crate::storage::sqlite::tests) use super::storage_core::{
    AUTOCOMMIT_RETAIN_OFF_PRAGMAS, SNIPPETS_MESSAGE_ID_INDEX, disable_autocommit_retain,
};
pub(in crate::storage::sqlite::tests) use super::tx_analytics_rollups::{
    TOKEN_DAILY_STATS_REBUILD_STAGE_TABLE, TokenDailyStatsRebuildCursor,
    franken_flush_analytics_rollups_in_tx, franken_insert_message_metrics_batched_in_tx,
    franken_insert_token_usage_batched_in_tx, franken_reassociate_provider_analytics_workspace,
    franken_update_conversation_token_summaries_in_tx, franken_update_daily_stats_in_tx,
    read_token_daily_stats_rebuild_cursor, token_daily_stats_ledger_fingerprint,
    token_daily_stats_rebuild_stage_ddl, write_token_daily_stats_rebuild_cursor,
};
pub(in crate::storage::sqlite::tests) use super::tx_conversation_insert::{
    ConversationInsertStatus, bounded_fts_marker_detail, franken_insert_conversation,
    franken_insert_conversation_or_get_existing_after_miss, resolve_defer_analytics_updates,
};
pub(in crate::storage::sqlite::tests) use super::tx_message_insert::franken_batch_insert_fts_on_connection;

use crate::franken_sync::compat::{ConnectionExt as FrankenConnectionExt, RowExt as FrankenRowExt};
use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole, Snippet};
use crate::sources::provenance::LOCAL_SOURCE_ID;
use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::process::Command;

pub(super) fn exact_child_test_name(module_path_str: &str, fn_name: &str) -> String {
    let rel = module_path_str
        .strip_prefix("coding_agent_search::")
        .unwrap_or_else(|| {
            module_path_str
                .split_once("::")
                .map_or(module_path_str, |(_, rest)| rest)
        });
    format!("{rel}::{fn_name}")
}

/// The legacy duplicate-FTS-schema repair and historical-bundle seeding
/// shell out to the `sqlite3` CLI (`Command::new("sqlite3")`). A host
/// without it cannot exercise those paths; the affected tests return
/// early with a printed reason rather than failing on `ENOENT` — a loud
/// skip, not a green claim (fleet receipt 2026-09-02, bead hyqjz).
pub(super) fn sqlite3_cli_available_or_skip(test: &str) -> bool {
    let available = Command::new("sqlite3")
        .arg("-version")
        .output()
        .is_ok_and(|output| output.status.success());
    if !available {
        eprintln!("SKIPPED {test}: the sqlite3 CLI is not installed on this host");
    }
    available
}

pub(super) struct EnvGuard {
    key: &'static str,
    previous: Option<String>,
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        if let Some(value) = &self.previous {
            // SAFETY: test helper restores prior process env for isolation.
            unsafe {
                std::env::set_var(self.key, value);
            }
        } else {
            // SAFETY: test helper restores prior process env for isolation.
            unsafe {
                std::env::remove_var(self.key);
            }
        }
    }
}

pub(super) fn set_env_var(key: &'static str, value: impl AsRef<str>) -> EnvGuard {
    let previous = dotenvy::var(key).ok();
    // SAFETY: test helper toggles a process-local env var for isolation.
    unsafe {
        std::env::set_var(key, value.as_ref());
    }
    EnvGuard { key, previous }
}

pub(super) fn frontier_test_conversation(idx_created_at: &[(i64, Option<i64>)]) -> Conversation {
    Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: None,
        external_id: Some("frontier-test".into()),
        title: Some("Frontier test".into()),
        source_path: PathBuf::from("/tmp/frontier-test.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: None,
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: idx_created_at
            .iter()
            .map(|(idx, created_at)| Message {
                id: None,
                idx: *idx,
                role: MessageRole::User,
                author: None,
                created_at: *created_at,
                content: format!("message-{idx}"),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            })
            .collect(),
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    }
}

pub(super) fn seed_atomic_fts_rebuild_fixture(storage: &FrankenStorage) {
    let agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: None,
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).expect("seed FTS agent");
    let conversation = Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/tmp/fts-atomic")),
        external_id: Some("fts-atomic".into()),
        title: Some("FTS atomic fixture".into()),
        source_path: PathBuf::from("/tmp/fts-atomic.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_001),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: Some("user".into()),
            created_at: Some(1_700_000_000_001),
            content: "atomic shadow survives interruption".into(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };
    storage
        .insert_conversation_tree(agent_id, None, &conversation)
        .expect("seed FTS conversation");
    storage
        .rebuild_fts()
        .expect("materialize healthy FTS shadow");
    assert_eq!(
        storage
            .inspect_search_fallback_fts_parity()
            .expect("inspect seeded FTS")
            .status,
        FtsShadowParityStatus::Healthy
    );
}

pub(super) fn reference_indexable_message_count(storage: &FrankenStorage) -> i64 {
    storage
        .raw()
        .query_row_map(
            "SELECT COUNT(*)
                 FROM messages m
                 INNER JOIN conversations c ON c.id = m.conversation_id",
            fparams![],
            |row| row.get_typed(0),
        )
        .expect("count indexable messages with the unforced reference join")
}

/// Open a raw rusqlite connection on `db_path` for the narrow purpose of
/// injecting (or inspecting the raw projection of) sqlite_master
/// corruption patterns in test fixtures. All callers are in this test
/// module and run under #[cfg(test)]; no production code path touches
/// raw rusqlite directly through this helper.
pub(super) fn rusqlite_test_fixture_conn(db_path: &Path) -> rusqlite::Connection {
    rusqlite::Connection::open(db_path).expect("open rusqlite test fixture connection")
}

pub(super) fn make_profiled_storage_remote_conversation(
    external_id: i64,
    msg_count: usize,
) -> Conversation {
    Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/ws/profiled-storage-remote")),
        external_id: Some(format!("profiled-storage-remote-{external_id}")),
        title: Some(format!(
            "Profiled storage remote conversation {external_id}"
        )),
        source_path: PathBuf::from(format!("/log/profiled-storage-remote-{external_id}.jsonl")),
        started_at: Some(10_000 + external_id * 100),
        ended_at: Some(10_000 + external_id * 100 + msg_count as i64),
        approx_tokens: Some(msg_count as i64 * 32),
        metadata_json: serde_json::json!({ "bench": true }),
        messages: (0..msg_count)
            .map(|idx| Message {
                id: None,
                idx: idx as i64,
                role: if idx % 2 == 0 {
                    MessageRole::User
                } else {
                    MessageRole::Agent
                },
                author: Some("tester".into()),
                created_at: Some(20_000 + external_id * 100 + idx as i64),
                content: format!(
                    "profiled storage remote content ext={external_id} idx={idx} {}",
                    "x".repeat(64)
                ),
                extra_json: serde_json::json!({ "idx": idx }),
                snippets: Vec::new(),
            })
            .collect(),
        source_id: "profiled-storage-remote-source".into(),
        origin_host: Some("builder-profile".into()),
    }
}

pub(super) fn make_profiled_append_remote_merge_conversation(
    external_id: i64,
    msg_count: usize,
) -> Conversation {
    let base_ts = 100_000 + external_id * 1_000;
    Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/ws/profiled-append-remote")),
        external_id: Some(format!("profiled-append-remote-{external_id}")),
        title: Some(format!("Profiled append remote conversation {external_id}")),
        source_path: PathBuf::from(format!("/log/profiled-append-remote-{external_id}.jsonl")),
        started_at: Some(base_ts),
        ended_at: Some(base_ts + msg_count as i64),
        approx_tokens: Some(msg_count as i64 * 50),
        metadata_json: serde_json::json!({ "bench": true }),
        messages: (0..msg_count)
            .map(|idx| Message {
                id: None,
                idx: idx as i64,
                role: if idx % 2 == 0 {
                    MessageRole::User
                } else {
                    MessageRole::Agent
                },
                author: Some(format!("model-{}", external_id % 5)),
                created_at: Some(base_ts + idx as i64),
                content: format!(
                    "Profiled append remote conversation {} message {}: Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
                    external_id, idx
                ),
                extra_json: serde_json::json!({ "bench": true }),
                snippets: Vec::new(),
            })
            .collect(),
        source_id: "profiled-append-remote-source".into(),
        origin_host: Some("builder-profile".into()),
    }
}

pub(super) fn gh423_storage_snapshot() -> Conversation {
    Conversation {
        id: None, agent_slug: "codebuff".into(), workspace: None,
        external_id: Some(r#"["/profile/.config/manicode","project","native-chat"]"#.into()),
        title: Some("Codebuff / Freebuff".into()),
        source_path: PathBuf::from("/profile/.config/manicode/projects/project/chats/native-chat/chat-messages.json"),
        started_at: Some(1_767_225_622_759), ended_at: Some(1_767_225_622_759), approx_tokens: None,
        metadata_json: serde_json::json!({"shared_lineage":true,"storage_format":"manicode-chat-messages-array"}),
        messages: (0..3).map(|idx| Message {
            id: None, idx, role: MessageRole::Agent, author: None,
            created_at: Some(1_767_225_622_759), content: "oldneedle unchanged tool output".into(),
            extra_json: serde_json::json!({"id":format!("native-{idx}"),"codebuff_message_id":format!("native-{idx}"),
                "variant":"ai", "isComplete":false, "credits":1.25,
                "blocks":[{"type":"tool","toolCallId":format!("call-{idx}"),"toolName":"read_files",
                    "input":{"paths":["src/main.rs"]},"output":"oldneedle unchanged tool output"}]}),
            snippets: vec![Snippet { id:None, file_path:Some(PathBuf::from("src/main.rs")),
                start_line:Some(1), end_line:Some(1), language:Some("rust".into()), snippet_text:Some("old snippet".into()) }],
        }).collect(), source_id: "local".into(), origin_host: None,
    }
}

pub(super) fn gh423_revise(conv: &mut Conversation) {
    let message = &mut conv.messages[1];
    message.content = "newneedle finished tool output and final ordered response".into();
    message.extra_json["isComplete"] = serde_json::json!(true);
    message.extra_json["blocks"][0]["output"] = serde_json::json!("newneedle finished tool output");
    message.extra_json["blocks"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({
        "type":"text","content":"final ordered response"}));
    message.extra_json["blocks"].as_array_mut().unwrap().push(serde_json::json!({
        "type":"agent", "content":"Nested review", "blocks":[{"type":"tool",
            "toolCallId":"nested-call", "toolName":"read_files", "input":{}, "output":"reviewed"}]}));
    message.snippets[0].snippet_text = Some("updated snippet".into());
    message.snippets.push(Snippet {
        id: None,
        file_path: None,
        start_line: None,
        end_line: None,
        language: Some("text".into()),
        snippet_text: Some("second snippet".into()),
    });
}

pub(super) fn gh423_persist(
    storage: &FrankenStorage,
    agent: i64,
    conv: &Conversation,
    route: u8,
) -> Result<InsertOutcome> {
    match route {
        0 => storage.insert_conversation_tree_with_analytics(agent, None, conv, false),
        1 => {
            let (writer, _) = storage.acquire_cached_ephemeral_writer()?;
            let outcome = writer.insert_conversation_tree_with_analytics(agent, None, conv, false);
            storage.release_cached_ephemeral_writer(writer);
            outcome
        }
        _ => storage
            .insert_conversations_batched_with_analytics(&[(agent, None, conv)], false)?
            .pop()
            .context("expected Codebuff outcome"),
    }
}

pub(super) fn gh423_agent(storage: &FrankenStorage) -> i64 {
    storage
        .ensure_agent(&Agent {
            id: None,
            slug: "codebuff".into(),
            name: "Codebuff / Freebuff".into(),
            version: None,
            kind: AgentKind::Cli,
        })
        .unwrap()
}

pub(super) fn gh423_analytics_snapshot(storage: &FrankenStorage) -> Vec<String> {
    [
        "daily_stats",
        "token_usage",
        "token_daily_stats",
        "message_metrics",
        "usage_hourly",
        "usage_daily",
        "usage_models_daily",
    ]
    .iter()
    .map(|table| {
        format!(
            "{:?}",
            storage
                .raw()
                .query(&format!("SELECT * FROM {table} ORDER BY 1, 2, 3"))
                .unwrap()
        )
    })
    .collect()
}

pub(super) fn gh423_assert_analytics(
    storage: &FrankenStorage,
    conversation_id: i64,
    expected_messages: i64,
    expected_chars: i64,
    expected_tools: i64,
) {
    let counts: (i64, i64, i64) = storage.raw().query_row_map(
        "SELECT session_count, message_count, total_chars FROM daily_stats WHERE agent_slug = 'codebuff' AND source_id = 'local'",
        fparams![], |row| Ok((row.get_typed(0)?,row.get_typed(1)?,row.get_typed(2)?)),
    ).unwrap();
    assert_eq!(counts, (1, expected_messages, expected_chars));
    for table in ["token_usage", "message_metrics"] {
        let totals: (i64, i64) = storage
            .raw()
            .query_row_map(
                &format!("SELECT COUNT(*), SUM(content_chars) FROM {table}"),
                fparams![],
                |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
            )
            .unwrap();
        assert_eq!(totals, (expected_messages, expected_chars));
    }
    let token_daily: (i64, i64, i64, i64) = storage.raw().query_row_map(
        "SELECT api_call_count, session_count, total_content_chars, total_tool_calls FROM token_daily_stats
         WHERE agent_slug = 'codebuff' AND source_id = 'local' AND model_family = 'unknown'",
        fparams![], |row| Ok((row.get_typed(0)?,row.get_typed(1)?,row.get_typed(2)?,row.get_typed(3)?)),
    ).unwrap();
    assert_eq!(
        token_daily,
        (expected_messages, 1, expected_chars, expected_tools)
    );
    for table in ["usage_hourly", "usage_daily", "usage_models_daily"] {
        let totals: (i64, i64, i64) = storage.raw().query_row_map(
                &format!("SELECT SUM(message_count), SUM(tool_call_count), SUM(api_coverage_message_count) FROM {table}"),
                fparams![], |row| Ok((row.get_typed(0)?,row.get_typed(1)?,row.get_typed(2)?)),
        ).unwrap();
        assert_eq!(
            totals,
            (expected_messages, expected_tools, 0),
            "credits are not API tokens"
        );
    }
    let summary: (i64, i64) = storage
        .raw()
        .query_row_map(
            "SELECT assistant_message_count, tool_call_count FROM conversations WHERE id = ?1",
            fparams![conversation_id],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
        )
        .unwrap();
    assert_eq!(summary, (expected_messages, expected_tools));
    let token_totals: (i64,i64,i64) = storage.raw().query_row_map(
        "SELECT (SELECT SUM(total_tokens) FROM token_usage),
         (SELECT grand_total_tokens FROM token_daily_stats WHERE agent_slug = 'codebuff' AND source_id = 'local' AND model_family = 'unknown'),
         (SELECT grand_total_tokens FROM conversations WHERE id = ?1)",
        fparams![conversation_id], |row| Ok((row.get_typed(0)?,row.get_typed(1)?,row.get_typed(2)?)),
    ).unwrap();
    let canonical_estimate: i64 = storage.raw().query_row_map(
        "SELECT SUM(CAST(octet_length(content) / 4 AS INTEGER)) FROM messages WHERE conversation_id = ?1",
        fparams![conversation_id], |row|row.get_typed(0),
    ).unwrap();
    assert_eq!(
        token_totals,
        (canonical_estimate, canonical_estimate, canonical_estimate)
    );
}

pub(super) fn gh447_native_window(start: u32, count: u32) -> Conversation {
    Conversation {
        id: None, agent_slug: "grok_bot".into(), workspace: None,
        external_id: Some("sand.client.slice.account.auth0%7Cuser_a.transcript.replicas.agent-a".into()),
        title: Some("Grok Bot rolling transcript".into()),
        source_path: PathBuf::from("/Grok Bot 空間/sand-client-persistence/replica.blob"),
        started_at: Some(1_767_225_622_759), ended_at: Some(1_767_225_622_759), approx_tokens: None,
        metadata_json: serde_json::json!({"history_kind":"rolling_agent_transcript","history_complete":false}),
        messages: (start..start + count).enumerate().map(|(idx, native)| Message {
            id: None, idx: idx as i64, role: MessageRole::Agent, author: None,
            created_at: Some(1_767_225_622_759), content: "same-time identical chat".into(),
            extra_json: serde_json::json!({"grok_bot_entry_id":format!("entry-{native}"),"grok_bot_entry_kind":"send-message"}),
            snippets: vec![],
        }).collect(),
        source_id: "local".into(), origin_host: None,
    }
}

pub(super) fn gh447_persist(
    storage: &FrankenStorage,
    agent: i64,
    conv: &Conversation,
    batched: bool,
) -> Result<InsertOutcome> {
    if batched {
        Ok(storage
            .insert_conversations_batched_with_analytics(&[(agent, None, conv)], false)?
            .pop()
            .unwrap())
    } else {
        storage.insert_conversation_tree_with_analytics(agent, None, conv, false)
    }
}

pub(super) fn forget_fixture_conversation(
    source_path: &str,
    external_id: Option<&str>,
    workspace: Option<&str>,
    timestamps: &[Option<i64>],
) -> Conversation {
    Conversation {
        id: None,
        agent_slug: "opencode".into(),
        workspace: workspace.map(PathBuf::from),
        external_id: external_id.map(str::to_owned),
        title: external_id.map(str::to_owned),
        source_path: PathBuf::from(source_path),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_001),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: timestamps
            .iter()
            .enumerate()
            .map(|(idx, timestamp)| Message {
                id: None,
                idx: i64::try_from(idx).unwrap(),
                role: MessageRole::User,
                author: None,
                created_at: *timestamp,
                content: format!(
                    "retained {} message {idx}",
                    external_id.unwrap_or("unknown")
                ),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            })
            .collect(),
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    }
}

pub(super) fn seed_forget_fixture(
    storage: &FrankenStorage,
    conversation: &Conversation,
) -> Result<i64> {
    let agent_id = storage.ensure_agent(&Agent {
        id: None,
        slug: conversation.agent_slug.clone(),
        name: conversation.agent_slug.clone(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    let workspace_id = conversation
        .workspace
        .as_deref()
        .map(|path| storage.ensure_workspace(path, None))
        .transpose()?;
    Ok(storage.insert_conversations_batched_with_analytics(
        &[(agent_id, workspace_id, conversation)],
        true,
    )?[0]
        .conversation_id)
}

mod agents_workspaces_sources;
mod analytics_omp_cursor;
mod analytics_rollups_and_claude_dedup;
mod bundles_backups_and_open;
mod codebuff_and_native_fifo;
mod forget_and_purge;
mod fts_batch_and_daily_stats;
mod fts_parity_and_guards;
mod fts_shadow_lifecycle;
mod gh515_and_connection_manager;
mod historical_salvage;
mod ingest_external_and_source_merge;
mod ingest_replay_and_tool_outputs;
mod legacy_omp_reclassification_a;
mod legacy_omp_reclassification_b;
mod lexical_rebuild_and_dedup;
mod lexical_rebuild_stream;
mod migrations_and_schema_repair;
mod msgpack_lazy_and_pricing;
mod reinclude_shared_and_orphan_fk;
mod workspace_and_codebuff_part1;
