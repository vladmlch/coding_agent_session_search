use super::*;
use crate::franken_sync::{
    Connection as FrankenConnection,
    compat::{
        ConnectionExt as FrankenConnectionExt, OpenFlags as FrankenOpenFlags,
        OptionalExtension as FrankenOptionalExtension, RowExt as FrankenRowExt,
        open_with_flags as open_franken_with_flags,
    },
};
use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use crate::sources::provenance::LOCAL_SOURCE_ID;
use anyhow::anyhow;
use serial_test::serial;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

fn seed_historical_db_direct(db_path: &Path, conversations: &[crate::model::types::Conversation]) {
    if let Some(parent) = db_path.parent() {
        fs::create_dir_all(parent).unwrap();
    }

    let conn = FrankenConnection::open(db_path.to_string_lossy().into_owned()).unwrap();
    conn.execute_batch(HISTORICAL_RECOVERY_CORE_SCHEMA).unwrap();
    conn.execute_compat(
        "INSERT INTO agents(id, slug, name, version, kind, created_at, updated_at)
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        fparams![1_i64, "codex", "Codex", "0.2.3", "cli", 0_i64, 0_i64],
    )
    .unwrap();

    let mut next_message_id = 1_i64;
    for (conv_index, conv) in conversations.iter().enumerate() {
        let conversation_id = i64::try_from(conv_index + 1).unwrap();
        let workspace_id = conv.workspace.as_ref().map(|workspace| {
            let workspace_id = conversation_id;
            let workspace_path = workspace.to_string_lossy().into_owned();
            conn.execute_compat(
                "INSERT INTO workspaces(id, path, display_name) VALUES(?1, ?2, ?3)",
                fparams![
                    workspace_id,
                    workspace_path.as_str(),
                    workspace_path.as_str()
                ],
            )
            .unwrap();
            workspace_id
        });
        let source_path = conv.source_path.to_string_lossy().into_owned();
        let metadata_json = conv.metadata_json.to_string();
        conn.execute_compat(
            "INSERT INTO conversations (
                    id, agent_id, workspace_id, source_id, external_id, title, source_path,
                    started_at, ended_at, approx_tokens, metadata_json, origin_host
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            fparams![
                conversation_id,
                1_i64,
                workspace_id,
                conv.source_id.as_str(),
                conv.external_id.as_deref(),
                conv.title.as_deref(),
                source_path.as_str(),
                conv.started_at,
                conv.ended_at,
                conv.approx_tokens,
                metadata_json.as_str(),
                conv.origin_host.as_deref()
            ],
        )
        .unwrap();

        for msg in &conv.messages {
            let extra_json = msg.extra_json.to_string();
            let role = role_str(&msg.role);
            conn.execute_compat(
                "INSERT INTO messages(
                        id, conversation_id, idx, role, author, created_at, content, extra_json
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                fparams![
                    next_message_id,
                    conversation_id,
                    msg.idx,
                    role.as_str(),
                    msg.author.as_deref(),
                    msg.created_at,
                    msg.content.as_str(),
                    extra_json.as_str()
                ],
            )
            .unwrap();
            next_message_id += 1;
        }
    }
}

#[test]
fn salvage_historical_databases_imports_backups_once_and_merges_overlap() {
    use crate::model::types::{Conversation, Message, MessageRole};
    use std::path::PathBuf;

    fn base_conv(source_path: &str, messages: Vec<Message>) -> Conversation {
        Conversation {
            id: None,
            agent_slug: "codex".into(),
            workspace: Some(PathBuf::from("/tmp/workspace")),
            external_id: None,
            title: Some("Recovered".into()),
            source_path: PathBuf::from(source_path),
            started_at: Some(1_700_000_000_000),
            ended_at: Some(1_700_000_000_999),
            approx_tokens: None,
            metadata_json: serde_json::Value::Null,
            messages,
            source_id: "local".into(),
            origin_host: None,
        }
    }

    let dir = TempDir::new().unwrap();
    let canonical_db = dir.path().join("agent_search.db");
    let storage = SqliteStorage::open(&canonical_db).unwrap();

    let overlapping_a = base_conv(
        "/tmp/shared-history.jsonl",
        vec![
            Message {
                id: None,
                idx: 0,
                role: MessageRole::User,
                author: None,
                created_at: Some(1_700_000_000_000),
                content: "first".into(),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            },
            Message {
                id: None,
                idx: 1,
                role: MessageRole::Agent,
                author: None,
                created_at: Some(1_700_000_000_100),
                content: "second".into(),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            },
        ],
    );
    let overlapping_b = base_conv(
        "/tmp/shared-history.jsonl",
        vec![
            Message {
                id: None,
                idx: 1,
                role: MessageRole::Agent,
                author: None,
                created_at: Some(1_700_000_000_100),
                content: "second".into(),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            },
            Message {
                id: None,
                idx: 2,
                role: MessageRole::User,
                author: None,
                created_at: Some(1_700_000_000_200),
                content: "third".into(),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            },
        ],
    );
    let unique = Conversation {
        source_path: PathBuf::from("/tmp/unique-history.jsonl"),
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: None,
            created_at: Some(1_700_000_001_000),
            content: "unique".into(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        started_at: Some(1_700_000_001_000),
        ended_at: Some(1_700_000_001_100),
        ..base_conv("/tmp/unique-history.jsonl", Vec::new())
    };

    seed_historical_db_direct(
        &dir.path()
            .join("backups/agent_search.db.20260322T020200.bak"),
        std::slice::from_ref(&overlapping_a),
    );
    seed_historical_db_direct(
        &dir.path().join("agent_search.corrupt.20260324_212907"),
        &[overlapping_b, unique],
    );

    let first = storage.salvage_historical_databases(&canonical_db).unwrap();
    assert_eq!(first.bundles_considered, 2);
    assert_eq!(first.bundles_imported, 2);
    assert_eq!(first.messages_imported, 4);

    let conversations = storage.list_conversations(10, 0).unwrap();
    let row_debug: Vec<(i64, Option<String>, String, Option<i64>)> = storage
        .conn
        .query_map_collect(
            "SELECT id, external_id, source_path, started_at FROM conversations ORDER BY id",
            fparams![],
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
    assert_eq!(conversations.len(), 2, "conversation rows: {row_debug:?}");

    let shared_id = conversations
        .iter()
        .find(|conv| conv.source_path == std::path::Path::new("/tmp/shared-history.jsonl"))
        .and_then(|conv| conv.id)
        .unwrap();
    let shared_indices: Vec<i64> = storage
        .fetch_messages(shared_id)
        .unwrap()
        .into_iter()
        .map(|msg| msg.idx)
        .collect();
    assert_eq!(shared_indices, vec![0, 1, 2]);

    let second = storage.salvage_historical_databases(&canonical_db).unwrap();
    assert_eq!(second.bundles_imported, 0);
    assert_eq!(second.messages_imported, 0);
}

#[test]
fn salvage_historical_databases_normalizes_host_only_remote_provenance() {
    use crate::model::types::{Conversation, Message, MessageRole};
    use std::path::PathBuf;

    let dir = TempDir::new().unwrap();
    let canonical_db = dir.path().join("agent_search.db");
    let storage = SqliteStorage::open(&canonical_db).unwrap();

    let host_only_remote = Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: None,
        title: Some("Recovered Host Only Remote".into()),
        source_path: PathBuf::from("/tmp/host-only-history.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_999),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: None,
            created_at: Some(1_700_000_000_000),
            content: "host-only remote".into(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        source_id: "   ".into(),
        origin_host: Some("builder-5".into()),
    };

    let historical_db = dir
        .path()
        .join("backups/agent_search.db.20260322T020200.bak");
    seed_historical_db_direct(&historical_db, std::slice::from_ref(&host_only_remote));

    let historical_conn =
        FrankenConnection::open(historical_db.to_string_lossy().into_owned()).unwrap();
    historical_conn
        .execute_compat(
            "INSERT INTO sources(id, kind, host_label, created_at, updated_at) VALUES(?1, ?2, ?3, ?4, ?5)",
            fparams!["   ", "ssh", "builder-5", 0_i64, 0_i64],
        )
        .unwrap();
    historical_conn
        .execute_compat(
            "UPDATE conversations SET source_id = ?1, origin_host = ?2 WHERE source_path = ?3",
            fparams!["   ", "builder-5", "/tmp/host-only-history.jsonl"],
        )
        .unwrap();
    historical_conn
        .execute_compat("DELETE FROM sources WHERE id = ?1", fparams!["builder-5"])
        .unwrap();
    drop(historical_conn);

    let first = storage.salvage_historical_databases(&canonical_db).unwrap();
    assert_eq!(first.bundles_imported, 1);
    assert_eq!(first.messages_imported, 1);

    let source_ids = storage.get_source_ids().unwrap();
    assert_eq!(source_ids, vec!["builder-5".to_string()]);

    let conversations = storage.list_conversations(10, 0).unwrap();
    assert_eq!(conversations.len(), 1);
    assert_eq!(conversations[0].source_id, "builder-5");
    assert_eq!(conversations[0].origin_host.as_deref(), Some("builder-5"));
}

#[test]
fn historical_salvage_retry_splits_single_conversation_until_it_fits() {
    use crate::model::types::{Conversation, Message, MessageRole};
    use std::path::PathBuf;

    let mut attempts: Vec<Vec<usize>> = Vec::new();
    let entry = HistoricalBatchEntry {
        source_row_id: 77,
        agent_id: 1,
        workspace_id: None,
        conversation: Conversation {
            id: None,
            agent_slug: "gemini".into(),
            workspace: Some(PathBuf::from("/tmp/workspace")),
            external_id: Some("conv-77".into()),
            title: Some("Large recovered conversation".into()),
            source_path: PathBuf::from("/tmp/history.jsonl"),
            started_at: Some(1_700_000_000_000),
            ended_at: Some(1_700_000_000_999),
            approx_tokens: None,
            metadata_json: serde_json::Value::Null,
            messages: (0..4)
                .map(|idx| Message {
                    id: None,
                    idx,
                    role: MessageRole::User,
                    author: None,
                    created_at: Some(1_700_000_000_000 + idx),
                    content: format!("message-{idx}"),
                    extra_json: serde_json::Value::Null,
                    snippets: Vec::new(),
                })
                .collect(),
            source_id: LOCAL_SOURCE_ID.into(),
            origin_host: None,
        },
    };

    let totals = SqliteStorage::import_historical_batch_with_retry(
        std::slice::from_ref(&entry),
        &mut |batch| {
            attempts.push(
                batch
                    .iter()
                    .map(|entry| entry.conversation.messages.len())
                    .collect(),
            );
            let total_messages: usize = batch
                .iter()
                .map(|entry| entry.conversation.messages.len())
                .sum();
            if total_messages > 1 {
                Err(anyhow!("out of memory"))
            } else {
                Ok(HistoricalBatchImportTotals {
                    inserted_source_rows: batch.len(),
                    inserted_messages: total_messages,
                })
            }
        },
    )
    .unwrap();

    assert_eq!(
        totals,
        HistoricalBatchImportTotals {
            inserted_source_rows: 1,
            inserted_messages: 4,
        }
    );
    assert_eq!(attempts.first().cloned(), Some(vec![4]));
    assert!(
        attempts.iter().filter(|sizes| sizes == &&vec![1]).count() >= 4,
        "expected recursive fallback to reach one-message slices"
    );
}

#[test]
fn salvage_historical_databases_resumes_from_progress_checkpoint() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    fn make_conv(source_path: &str, idx_seed: i64) -> Conversation {
        Conversation {
            id: None,
            agent_slug: "codex".into(),
            workspace: Some(PathBuf::from("/tmp/workspace")),
            external_id: Some(format!("conv-{idx_seed}")),
            title: Some(format!("Recovered {idx_seed}")),
            source_path: PathBuf::from(source_path),
            started_at: Some(1_700_000_000_000 + idx_seed),
            ended_at: Some(1_700_000_000_100 + idx_seed),
            approx_tokens: None,
            metadata_json: serde_json::Value::Null,
            messages: vec![Message {
                id: None,
                idx: 0,
                role: MessageRole::User,
                author: None,
                created_at: Some(1_700_000_000_000 + idx_seed),
                content: format!("message-{idx_seed}"),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            }],
            source_id: LOCAL_SOURCE_ID.into(),
            origin_host: None,
        }
    }

    let dir = TempDir::new().unwrap();
    let canonical_db = dir.path().join("agent_search.db");
    let backup_db = dir
        .path()
        .join("backups/agent_search.db.20260322T020200.bak");
    let storage = SqliteStorage::open(&canonical_db).unwrap();
    let conv_a = make_conv("/tmp/one.jsonl", 1);
    let conv_b = make_conv("/tmp/two.jsonl", 2);
    let conv_c = make_conv("/tmp/three.jsonl", 3);
    seed_historical_db_direct(
        &backup_db,
        &[conv_a.clone(), conv_b.clone(), conv_c.clone()],
    );

    let agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: Some("0.2.3".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();
    storage
        .insert_conversation_tree(agent_id, None, &conv_a)
        .unwrap();

    let bundle = discover_historical_database_bundles(&canonical_db)
        .into_iter()
        .find(|bundle| bundle.root_path == backup_db)
        .unwrap();
    let first_row_id: i64 = FrankenConnection::open(backup_db.to_string_lossy().into_owned())
        .unwrap()
        .query_row_map(
            "SELECT id FROM conversations WHERE source_path = ?1",
            fparams!["/tmp/one.jsonl"],
            |row| row.get_typed(0),
        )
        .unwrap();
    storage
        .record_historical_bundle_progress(&bundle, "direct-readonly", first_row_id, 50, 99)
        .unwrap();

    let outcome = storage.salvage_historical_databases(&canonical_db).unwrap();
    assert_eq!(outcome.bundles_imported, 1);
    assert_eq!(outcome.conversations_imported, 52);
    assert_eq!(outcome.messages_imported, 101);
    assert_eq!(storage.list_conversations(10, 0).unwrap().len(), 3);

    let progress_key = SqliteStorage::historical_bundle_progress_key(&bundle);
    let progress_left: Option<String> = storage
        .conn
        .query_row_map(
            "SELECT value FROM meta WHERE key = ?1",
            fparams![progress_key.as_str()],
            |row| row.get_typed(0),
        )
        .optional()
        .unwrap();
    assert!(
        progress_left.is_none(),
        "completed salvage should clear bundle progress"
    );

    let second = storage.salvage_historical_databases(&canonical_db).unwrap();
    assert_eq!(second.bundles_imported, 0);
    assert_eq!(second.messages_imported, 0);
}

#[test]
fn salvage_historical_databases_skips_bundle_when_checkpoint_covers_backup() {
    // Regression for issue #247 (coding_agent_session_search-r8pcy): a bundle
    // whose progress checkpoint already covers the backup's entire conversation
    // row-id space (daemon OOM-killed after the last batch committed but before
    // the completion ledger marker landed) must be ledgered + skipped, not
    // re-scanned O(n) with imported=0 every batch.
    use crate::model::types::{Conversation, Message, MessageRole};
    use std::path::PathBuf;

    fn make_conv(source_path: &str, idx_seed: i64) -> Conversation {
        Conversation {
            id: None,
            agent_slug: "codex".into(),
            workspace: Some(PathBuf::from("/tmp/workspace")),
            external_id: Some(format!("conv-{idx_seed}")),
            title: Some(format!("Recovered {idx_seed}")),
            source_path: PathBuf::from(source_path),
            started_at: Some(1_700_000_000_000 + idx_seed),
            ended_at: Some(1_700_000_000_100 + idx_seed),
            approx_tokens: None,
            metadata_json: serde_json::Value::Null,
            messages: vec![Message {
                id: None,
                idx: 0,
                role: MessageRole::User,
                author: None,
                created_at: Some(1_700_000_000_000 + idx_seed),
                content: format!("message-{idx_seed}"),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            }],
            source_id: LOCAL_SOURCE_ID.into(),
            origin_host: None,
        }
    }

    let dir = TempDir::new().unwrap();
    let canonical_db = dir.path().join("agent_search.db");
    let backup_db = dir
        .path()
        .join("backups/agent_search.db.20260322T020200.bak");
    let storage = SqliteStorage::open(&canonical_db).unwrap();
    seed_historical_db_direct(
        &backup_db,
        &[
            make_conv("/tmp/one.jsonl", 1),
            make_conv("/tmp/two.jsonl", 2),
            make_conv("/tmp/three.jsonl", 3),
        ],
    );

    let bundle = discover_historical_database_bundles(&canonical_db)
        .into_iter()
        .find(|bundle| bundle.root_path == backup_db)
        .unwrap();

    // Checkpoint high-water mark == backup's max conversation id.
    let backup_max_id: i64 = FrankenConnection::open(backup_db.to_string_lossy().into_owned())
        .unwrap()
        .query_row_map(
            "SELECT COALESCE(MAX(id), 0) FROM conversations",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert!(backup_max_id > 0, "seeded backup should have conversations");
    storage
        .record_historical_bundle_progress(&bundle, "direct-readonly", backup_max_id, 3, 3)
        .unwrap();

    let outcome = storage.salvage_historical_databases(&canonical_db).unwrap();
    assert_eq!(
        outcome.bundles_imported, 0,
        "fully-checkpointed bundle must not be re-scanned"
    );
    assert_eq!(outcome.conversations_imported, 0);
    assert_eq!(outcome.messages_imported, 0);
    assert_eq!(
        storage.list_conversations(10, 0).unwrap().len(),
        0,
        "skip path must not import anything"
    );
    assert!(
        storage.historical_bundle_already_imported(&bundle).unwrap(),
        "skipped bundle must be ledgered as salvaged so future runs short-circuit"
    );

    let progress_key = SqliteStorage::historical_bundle_progress_key(&bundle);
    let progress_left: Option<String> = storage
        .conn
        .query_row_map(
            "SELECT value FROM meta WHERE key = ?1",
            fparams![progress_key.as_str()],
            |row| row.get_typed(0),
        )
        .optional()
        .unwrap();
    assert!(
        progress_left.is_none(),
        "skip path must clear the bundle progress checkpoint"
    );
}

#[test]
#[serial]
fn seed_canonical_from_best_historical_bundle_copies_data_and_resets_runtime_meta() {
    if !sqlite3_cli_available_or_skip(
        "seed_canonical_from_best_historical_bundle_copies_data_and_resets_runtime_meta",
    ) {
        return;
    }
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    let dir = TempDir::new().unwrap();
    let canonical_db = dir.path().join("agent_search.db");
    let source_db = dir
        .path()
        .join("backups/agent_search.db.20260322T020200.bak");

    fs::create_dir_all(source_db.parent().unwrap()).unwrap();

    let source = SqliteStorage::open(&source_db).unwrap();
    let agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: Some("0.2.3".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = source.ensure_agent(&agent).unwrap();
    let conversation = Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: Some("seed-conv".into()),
        title: Some("Historical seed".into()),
        source_path: PathBuf::from("/tmp/historical-seed.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_100),
        approx_tokens: Some(42),
        metadata_json: serde_json::json!({"seed": true}),
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::Agent,
            author: Some("assistant".into()),
            created_at: Some(1_700_000_000_050),
            content: "seeded message".into(),
            extra_json: serde_json::json!({"usage": {"total_tokens": 12}}),
            snippets: Vec::new(),
        }],
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };
    source
        .insert_conversation_tree(agent_id, None, &conversation)
        .unwrap();
    source.set_last_scan_ts(123).unwrap();
    source.set_last_indexed_at(456).unwrap();
    source.set_last_embedded_message_id(789).unwrap();
    source
        .conn
        .execute_compat(
            "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
            fparams!["historical_bundle_salvaged:stale", "{\"stale\":true}"],
        )
        .unwrap();
    drop(source);

    #[cfg(not(windows))]
    {
        // Legacy "duplicate FTS" fixture reconstruction.
        //
        // Post-V14 migration cass drops the V13-era fts_messages virtual table
        // and recreates it lazily, so a freshly-opened canonical DB has zero
        // fts_messages entries in sqlite_master. To reproduce the historical
        // failure mode this test exercises — a legacy v13 bundle with a
        // duplicated CREATE VIRTUAL TABLE row — we have to inject *both*
        // entries: the original V13-era contentless row and the buggy duplicate
        // row. Before V14 existed the original was already present after
        // migration and only the duplicate needed manual injection.
        let legacy_v13_fts_sql = "CREATE VIRTUAL TABLE fts_messages USING fts5(content, title, agent, workspace, source_path, created_at UNINDEXED, content='', tokenize='porter')";
        let duplicate_legacy_fts_sql = "CREATE VIRTUAL TABLE fts_messages USING fts5(content, title, agent, workspace, source_path, created_at UNINDEXED, message_id UNINDEXED, tokenize='porter')";
        let legacy = rusqlite_test_fixture_conn(&source_db);
        legacy
            .execute_batch(
                "UPDATE meta SET value = '13' WHERE key = 'schema_version';
                 DELETE FROM _schema_migrations WHERE version = 14;
                 PRAGMA writable_schema = ON;",
            )
            .unwrap();
        legacy
            .execute(
                "DELETE FROM meta WHERE key = ?1",
                [FTS_FRANKEN_REBUILD_META_KEY],
            )
            .unwrap();
        // Inject the V13 original first.
        legacy
            .execute(
                "INSERT INTO sqlite_master(type, name, tbl_name, rootpage, sql)
                 VALUES('table', 'fts_messages', 'fts_messages', 0, ?1)",
                [legacy_v13_fts_sql],
            )
            .unwrap();
        // Then the duplicate that's the real subject of the fixup logic.
        legacy
            .execute(
                "INSERT INTO sqlite_master(type, name, tbl_name, rootpage, sql)
                 VALUES('table', 'fts_messages', 'fts_messages', 0, ?1)",
                [duplicate_legacy_fts_sql],
            )
            .unwrap();
        legacy
            .execute_batch("PRAGMA writable_schema = OFF;")
            .unwrap();
        drop(legacy);

        // Verify fixture with rusqlite+writable_schema to see raw
        // sqlite_master rows (frankensqlite deduplicates schema entries).
        {
            let verify = rusqlite_test_fixture_conn(&source_db);
            verify
                .execute_batch("PRAGMA writable_schema = ON;")
                .unwrap();
            let fts_entries: i64 = verify
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE name = 'fts_messages'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(
                fts_entries, 2,
                "test fixture should reproduce the duplicate legacy fts_messages rows"
            );
            let msg_count: i64 = verify
                .query_row("SELECT COUNT(*) FROM messages", [], |row| row.get(0))
                .unwrap();
            assert_eq!(msg_count, 1);
        }
    }

    let fresh = SqliteStorage::open(&canonical_db).unwrap();
    drop(fresh);

    let outcome = seed_canonical_from_best_historical_bundle(&canonical_db)
        .unwrap()
        .unwrap();
    assert_eq!(outcome.bundles_imported, 1);
    assert_eq!(outcome.conversations_imported, 1);
    assert_eq!(outcome.messages_imported, 1);

    let readonly = open_franken_with_flags(
        &canonical_db.to_string_lossy(),
        FrankenOpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let readonly_message_count: i64 = readonly
        .query_row_map("SELECT COUNT(*) FROM messages", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(readonly_message_count, 1);

    let seeded = SqliteStorage::open(&canonical_db).unwrap();
    assert_eq!(
        seeded
            .count_sessions_in_range(None, None, None, None)
            .unwrap()
            .0,
        1
    );
    let message_count: i64 = seeded
        .conn
        .query_row_map("SELECT COUNT(*) FROM messages", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(message_count, 1);
    assert_eq!(seeded.get_last_scan_ts().unwrap(), None);
    assert_eq!(seeded.get_last_embedded_message_id().unwrap(), None);

    let last_indexed: Option<String> = seeded
        .conn
        .query_row_map(
            "SELECT value FROM meta WHERE key = 'last_indexed_at'",
            fparams![],
            |row| row.get_typed(0),
        )
        .optional()
        .unwrap();
    assert!(last_indexed.is_none());

    let salvage_keys: Vec<String> = seeded
        .conn
        .query_map_collect(
            "SELECT key FROM meta WHERE key LIKE 'historical_bundle_salvaged:%' ORDER BY key",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(salvage_keys.len(), 1);

    let reopened_readonly = open_franken_with_flags(
        &canonical_db.to_string_lossy(),
        FrankenOpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let reopened_fts_entries: i64 = reopened_readonly
        .query_row_map(
            "SELECT COUNT(*) FROM sqlite_master WHERE name = 'fts_messages'",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(
        reopened_fts_entries, 1,
        "seeded canonical db should keep a single stock-SQLite fts_messages schema row"
    );
    let reopened_message_count: i64 = reopened_readonly
        .query_row_map("SELECT COUNT(*) FROM messages", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(reopened_message_count, 1);

    let franken_seeded = FrankenStorage::open(&canonical_db).unwrap();
    assert_eq!(
        franken_seeded.schema_version().unwrap(),
        CURRENT_SCHEMA_VERSION
    );
    // Post-V14 fts_messages is recreated lazily. `FrankenStorage::open`
    // alone doesn't re-register the virtual table for the frankensqlite
    // query engine — the consistency pass does, and this is exactly what
    // normal cass startup runs before the first search. Invoke it
    // explicitly so the query below exercises the expected post-repair
    // state rather than the between-steps state.
    franken_seeded
        .ensure_search_fallback_fts_consistency()
        .expect("ensure FTS consistency after seed");
    let post_franken_schema_rows: i64 = franken_seeded
        .raw()
        .query_row_map(
            "SELECT COUNT(*) FROM sqlite_master WHERE name = 'fts_messages'",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(post_franken_schema_rows, 1);
    let fts_probe = franken_seeded
        .raw()
        .query("SELECT COUNT(*) FROM fts_messages");
    assert!(
        fts_probe.is_ok(),
        "expected post-seed FTS to be queryable, got {fts_probe:?}"
    );
}

#[test]
fn failed_baseline_seed_preserves_existing_canonical_bundle() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    let dir = TempDir::new().unwrap();
    let canonical_db = dir.path().join("agent_search.db");
    let source_db = dir
        .path()
        .join("backups/agent_search.db.20260325T120000Z.bad-seed.bak");

    fs::create_dir_all(source_db.parent().unwrap()).unwrap();

    let canonical = SqliteStorage::open(&canonical_db).unwrap();
    canonical
        .conn
        .execute_compat(
            "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
            fparams!["sentinel", "keep-me"],
        )
        .unwrap();
    drop(canonical);

    let source = SqliteStorage::open(&source_db).unwrap();
    let agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: Some("0.2.3".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = source.ensure_agent(&agent).unwrap();
    let conversation = Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: Some("bad-seed-conv".into()),
        title: Some("Bad seed".into()),
        source_path: PathBuf::from("/tmp/bad-seed.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_100),
        approx_tokens: Some(42),
        metadata_json: serde_json::json!({"seed": "bad"}),
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::Agent,
            author: Some("assistant".into()),
            created_at: Some(1_700_000_000_050),
            content: "this seed should fail".into(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };
    source
        .insert_conversation_tree(agent_id, None, &conversation)
        .unwrap();
    drop(source);

    let legacy = FrankenConnection::open(source_db.to_string_lossy().into_owned()).unwrap();
    legacy
        .execute("UPDATE meta SET value = '12' WHERE key = 'schema_version'")
        .unwrap();
    drop(legacy);

    let err = seed_canonical_from_best_historical_bundle(&canonical_db).unwrap_err();
    assert!(
        err.to_string()
            .contains("schema_version 12 is too old for baseline import"),
        "unexpected seed error: {err:#}"
    );

    let reopened = SqliteStorage::open(&canonical_db).unwrap();
    let sentinel: Option<String> = reopened
        .conn
        .query_row_map(
            "SELECT value FROM meta WHERE key = 'sentinel'",
            fparams![],
            |row| row.get_typed(0),
        )
        .optional()
        .unwrap();
    assert_eq!(sentinel.as_deref(), Some("keep-me"));

    let conversation_count: i64 = reopened
        .conn
        .query_row_map("SELECT COUNT(*) FROM conversations", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(conversation_count, 0);

    let readonly = open_franken_with_flags(
        &canonical_db.to_string_lossy(),
        FrankenOpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let readonly_conversation_count: i64 = readonly
        .query_row_map("SELECT COUNT(*) FROM conversations", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(readonly_conversation_count, 0);
}

#[test]
fn discover_historical_database_bundles_prefers_larger_archives_first() {
    let dir = TempDir::new().unwrap();
    let canonical_db = dir.path().join("agent_search.db");
    fs::write(&canonical_db, b"canonical").unwrap();

    let smaller = dir.path().join("agent_search.corrupt.small");
    fs::write(&smaller, vec![0_u8; 32]).unwrap();

    let backups_dir = dir.path().join("backups");
    fs::create_dir_all(&backups_dir).unwrap();
    let larger = backups_dir.join("agent_search.db.20260322T020200.bak");
    fs::write(&larger, vec![0_u8; 128]).unwrap();

    let bundles = discover_historical_database_bundles(&canonical_db);
    let ordered_paths: Vec<PathBuf> = bundles.into_iter().map(|bundle| bundle.root_path).collect();

    assert_eq!(ordered_paths, vec![larger, smaller]);
}

#[test]
fn discover_historical_database_bundles_prefers_queryable_direct_bundles_first() {
    let dir = TempDir::new().unwrap();
    let canonical_db = dir.path().join("agent_search.db");
    fs::write(&canonical_db, b"canonical").unwrap();

    let larger_corrupt = dir.path().join("agent_search.corrupt.20260324_212907");
    fs::write(&larger_corrupt, vec![0_u8; 4096]).unwrap();

    let backups_dir = dir.path().join("backups");
    fs::create_dir_all(&backups_dir).unwrap();
    let smaller_healthy = backups_dir.join("agent_search.db.20260322T020200.bak");
    let conn = FrankenConnection::open(smaller_healthy.to_string_lossy().into_owned()).unwrap();
    conn.execute_batch(
        "CREATE TABLE conversations (id INTEGER PRIMARY KEY, source_path TEXT);
         CREATE TABLE messages (
             id INTEGER PRIMARY KEY,
             conversation_id INTEGER NOT NULL,
             idx INTEGER NOT NULL,
             content TEXT
         );
         INSERT INTO conversations(id, source_path) VALUES (1, '/tmp/history.jsonl');
         INSERT INTO messages(id, conversation_id, idx, content)
         VALUES (1, 1, 0, 'seed');",
    )
    .unwrap();
    drop(conn);

    let bundles = discover_historical_database_bundles(&canonical_db);
    let ordered_paths: Vec<PathBuf> = bundles
        .iter()
        .map(|bundle| bundle.root_path.clone())
        .collect();

    assert_eq!(ordered_paths, vec![smaller_healthy, larger_corrupt]);
    assert!(bundles[0].supports_direct_readonly);
    assert!(!bundles[1].supports_direct_readonly);
}

#[test]
fn salvage_historical_databases_skips_unreadable_quarantined_bundles() {
    let dir = TempDir::new().unwrap();
    let canonical_db = dir.path().join("agent_search.db");
    let storage = SqliteStorage::open(&canonical_db).unwrap();

    let quarantined = dir.path().join("agent_search.corrupt.20260324_212907");
    fs::write(&quarantined, b"not a sqlite database").unwrap();

    let discovered: Vec<PathBuf> = discover_historical_database_bundles(&canonical_db)
        .into_iter()
        .map(|bundle| bundle.root_path)
        .collect();
    assert_eq!(discovered, vec![quarantined]);

    let outcome = storage.salvage_historical_databases(&canonical_db).unwrap();
    assert_eq!(outcome.bundles_considered, 1);
    assert_eq!(outcome.bundles_imported, 0);
    assert_eq!(outcome.conversations_imported, 0);
    assert_eq!(outcome.messages_imported, 0);
    assert!(storage.list_conversations(10, 0).unwrap().is_empty());
}

#[test]
fn discover_historical_database_bundles_includes_repair_lab_and_snapshots_named_roots() {
    let dir = TempDir::new().unwrap();
    let canonical_db = dir.path().join("agent_search.db");
    fs::write(&canonical_db, b"canonical").unwrap();

    let repair_lab_dir = dir.path().join("repair-lab").join("live-copy");
    fs::create_dir_all(&repair_lab_dir).unwrap();
    let repair_lab_db = repair_lab_dir.join("agent_search.db");
    fs::write(&repair_lab_db, vec![0_u8; 96]).unwrap();
    fs::write(
        repair_lab_dir.join("agent_search.rebuild-test.db"),
        vec![0_u8; 192],
    )
    .unwrap();

    let snapshots_dir = dir.path().join("snapshots").join("20260324T013201Z");
    fs::create_dir_all(&snapshots_dir).unwrap();
    let snapshot_db = snapshots_dir.join("agent_search.db");
    fs::write(&snapshot_db, vec![0_u8; 64]).unwrap();

    let bundles = discover_historical_database_bundles(&canonical_db);
    let ordered_paths: Vec<PathBuf> = bundles.into_iter().map(|bundle| bundle.root_path).collect();

    assert!(ordered_paths.contains(&repair_lab_db));
    assert!(ordered_paths.contains(&snapshot_db));
    assert!(
        !ordered_paths
            .iter()
            .any(|path| path.file_name().and_then(|name| name.to_str())
                == Some("agent_search.rebuild-test.db"))
    );
}

#[test]
#[serial]
fn discover_historical_database_bundles_prefers_healthy_backup_over_replay_priority() {
    if !sqlite3_cli_available_or_skip(
        "discover_historical_database_bundles_prefers_healthy_backup_over_replay_priority",
    ) {
        return;
    }
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};

    let dir = TempDir::new().unwrap();
    let canonical_db = dir.path().join("agent_search.db");
    fs::write(&canonical_db, b"canonical").unwrap();

    let replay_dir = dir
        .path()
        .join("repair-lab")
        .join("replay-20260324T070101Z");
    fs::create_dir_all(&replay_dir).unwrap();
    let replay_db = replay_dir.join("agent_search.db");
    let replay_storage = SqliteStorage::open(&replay_db).unwrap();
    let agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: Some("0.2.3".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = replay_storage.ensure_agent(&agent).unwrap();
    let conversation = Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: Some("replay-conv".into()),
        title: Some("Replay bundle".into()),
        source_path: PathBuf::from("/tmp/replay.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_100),
        approx_tokens: Some(42),
        metadata_json: serde_json::Value::Null,
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::Agent,
            author: Some("assistant".into()),
            created_at: Some(1_700_000_000_050),
            content: "replay message".into(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };
    replay_storage
        .insert_conversation_tree(agent_id, None, &conversation)
        .unwrap();
    drop(replay_storage);

    let replay_legacy = rusqlite_test_fixture_conn(&replay_db);
    replay_legacy
        .execute_batch(
            "UPDATE meta SET value = '13' WHERE key = 'schema_version';
             DELETE FROM _schema_migrations WHERE version = 14;
             PRAGMA writable_schema = ON;",
        )
        .unwrap();
    replay_legacy
        .execute(
            "DELETE FROM meta WHERE key = ?1",
            [FTS_FRANKEN_REBUILD_META_KEY],
        )
        .unwrap();
    #[cfg(not(windows))]
    {
        let duplicate_legacy_fts_sql = "CREATE VIRTUAL TABLE fts_messages USING fts5(content, title, agent, workspace, source_path, created_at UNINDEXED, message_id UNINDEXED, tokenize='porter')";
        replay_legacy
            .execute(
                "INSERT INTO sqlite_master(type, name, tbl_name, rootpage, sql)
                 VALUES('table', 'fts_messages', 'fts_messages', 0, ?1)",
                [duplicate_legacy_fts_sql],
            )
            .unwrap();
    }
    replay_legacy
        .execute_batch("PRAGMA writable_schema = OFF;")
        .unwrap();
    drop(replay_legacy);

    let backups_dir = dir.path().join("backups");
    fs::create_dir_all(&backups_dir).unwrap();
    let clean_backup = backups_dir.join("agent_search.db.20260322T020200.bak");
    let clean_storage = SqliteStorage::open(&clean_backup).unwrap();
    let clean_agent_id = clean_storage.ensure_agent(&agent).unwrap();
    clean_storage
        .insert_conversation_tree(clean_agent_id, None, &conversation)
        .unwrap();
    drop(clean_storage);

    let bundles = discover_historical_database_bundles(&canonical_db);
    let ordered_paths: Vec<PathBuf> = bundles
        .iter()
        .map(|bundle| bundle.root_path.clone())
        .collect();

    assert_eq!(ordered_paths[0], clean_backup);
    assert_eq!(ordered_paths[1], replay_db);
    assert_eq!(
        bundles[0].probe.schema_version,
        Some(CURRENT_SCHEMA_VERSION)
    );
    // Post-V14 cass drops the fts_messages virtual table during migration
    // and recreates it lazily on first open, so a freshly-migrated "clean"
    // backup has zero fts_messages rows in sqlite_master. The bundle is
    // still ranked as healthy by `bundle_health_rank` because 0 rows is a
    // legitimate lazy-FTS state (see comment there).
    assert_eq!(bundles[0].probe.fts_schema_rows, Some(0));
    // `fts_queryable` mirrors a direct rusqlite probe; with 0 sqlite_master
    // rows the table isn't queryable until lazy repair runs.
    assert!(!bundles[0].probe.fts_queryable);
    assert_eq!(bundles[1].probe.schema_version, Some(13));
    // The replay bundle had V14 run (dropping fts_messages → 0 rows), then
    // the test rolls meta.schema_version back to 13 and deletes the V14
    // marker. On Unix CI we also inject a duplicate sqlite_master row to
    // exercise the malformed-bundle probe path that depends on sqlite3.
    let expected_fts_schema_rows = if cfg!(windows) { Some(0) } else { Some(1) };
    assert_eq!(bundles[1].probe.fts_schema_rows, expected_fts_schema_rows);
}

#[test]
fn ensure_fts_consistency_via_rusqlite_catches_up_missing_rows() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("fts-catchup.db");
    let storage = SqliteStorage::open(&db_path).unwrap();
    let agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: Some("0.2.3".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();
    let conversation = Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: Some("fts-catchup".into()),
        title: Some("FTS catchup".into()),
        source_path: PathBuf::from("/tmp/fts-catchup.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_100),
        approx_tokens: Some(42),
        metadata_json: serde_json::Value::Null,
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: Some("user".into()),
            created_at: Some(1_700_000_000_050),
            content: "initial message".into(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };
    storage
        .insert_conversation_tree(agent_id, None, &conversation)
        .unwrap();
    drop(storage);

    rebuild_fts_via_rusqlite(&db_path).unwrap();

    let conn = FrankenConnection::open(db_path.to_string_lossy().into_owned()).unwrap();
    let conversation_id: i64 = conn
        .query_row_map("SELECT id FROM conversations LIMIT 1", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    conn.execute_compat(
        "INSERT INTO messages(id, conversation_id, idx, role, author, created_at, content, extra_json, extra_bin)
         VALUES(2, ?1, 1, 'assistant', 'assistant', 1700000000060, 'authentication catchup', NULL, NULL)",
        fparams![conversation_id],
    )
    .unwrap();
    conn.execute_compat(
        "DELETE FROM meta WHERE key = ?1",
        fparams![FTS_FRANKEN_REBUILD_META_KEY],
    )
    .unwrap();
    drop(conn);

    let repair = ensure_fts_consistency_via_rusqlite(&db_path).unwrap();
    assert_eq!(
        repair,
        FtsConsistencyRepair::IncrementalCatchUp {
            inserted_rows: 1,
            total_rows: 2
        }
    );

    let conn = FrankenConnection::open(db_path.to_string_lossy().into_owned()).unwrap();
    let auth_rows: i64 = conn
        .query_row_map(
            "SELECT COUNT(*) FROM fts_messages WHERE rowid = 2",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(auth_rows, 1);
}

#[test]
fn drop_recreate_repairs_corrupt_fts_shadow_structure() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("corrupt-fts.db");

    // Canonical data + a valid FTS shadow.
    {
        let storage = SqliteStorage::open(&db_path).unwrap();
        let agent = Agent {
            id: None,
            slug: "codex".into(),
            name: "Codex".into(),
            version: None,
            kind: AgentKind::Cli,
        };
        let agent_id = storage.ensure_agent(&agent).unwrap();
        let conversation = Conversation {
            id: None,
            agent_slug: "codex".into(),
            workspace: None,
            external_id: Some("corrupt-fts".into()),
            title: Some("corrupt fts".into()),
            source_path: PathBuf::from("/tmp/corrupt-fts.jsonl"),
            started_at: Some(1_700_000_000_000),
            ended_at: Some(1_700_000_000_100),
            approx_tokens: None,
            metadata_json: serde_json::Value::Null,
            messages: vec![Message {
                id: None,
                idx: 0,
                role: MessageRole::User,
                author: Some("user".into()),
                created_at: Some(1_700_000_000_050),
                content: "authentication needle".into(),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            }],
            source_id: LOCAL_SOURCE_ID.into(),
            origin_host: None,
        };
        storage
            .insert_conversation_tree(agent_id, None, &conversation)
            .unwrap();
        storage.rebuild_fts_via_frankensqlite().unwrap();
    }

    // Corrupt the %_data structure record (rowid 10) via a fresh connection
    // (the FTS is still valid here, so the open succeeds).
    {
        let conn = FrankenConnection::open(db_path.to_string_lossy().into_owned()).unwrap();
        let garbage = [0xFFu8; 12];
        conn.execute_compat(
            "UPDATE fts_messages_data SET block = ?1 WHERE id = ?2",
            fparams![garbage.as_slice(), 10_i64],
        )
        .unwrap();
    }

    // #368 defect 3: an ordinary open now fails on the corrupt structure.
    assert!(
        SqliteStorage::open(&db_path).is_err(),
        "ordinary open must fail on a corrupt FTS5 %_data structure record"
    );

    // The deferred-open repair drops+recreates the shadow and rebuilds it
    // from canonical.
    let inserted = {
        let storage = SqliteStorage::open_deferred_fts5_for_repair(&db_path).unwrap();
        storage
            .rebuild_fts_shadow_via_drop_recreate()
            .unwrap_or_else(|e| panic!("drop+recreate rebuild failed: {e:#}"))
    };
    assert!(
        inserted >= 1,
        "repair must rebuild the canonical message into the FTS shadow"
    );

    // The archive now opens normally and the rebuilt shadow is queryable.
    let conn = FrankenConnection::open(db_path.to_string_lossy().into_owned()).unwrap();
    let matches: i64 = conn
        .query_row_map(
            "SELECT COUNT(*) FROM fts_messages WHERE fts_messages MATCH 'needle'",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(
        matches, 1,
        "rebuilt FTS shadow must be queryable for 'needle'"
    );
}

#[test]
fn rebuild_fts_via_rusqlite_cleans_duplicate_legacy_schema_rows() {
    if !sqlite3_cli_available_or_skip(
        "rebuild_fts_via_rusqlite_cleans_duplicate_legacy_schema_rows",
    ) {
        return;
    }
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("fts-duplicate-rebuild.db");

    let storage = SqliteStorage::open(&db_path).unwrap();
    let agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: Some("0.2.3".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();
    let conversation = Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/ws")),
        external_id: Some("retro".into()),
        title: Some("retro".into()),
        source_path: PathBuf::from("/tmp/retro.jsonl"),
        started_at: Some(42),
        ended_at: Some(42),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: None,
            created_at: Some(42),
            content: "retro investigation".into(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };
    storage
        .insert_conversation_tree(agent_id, None, &conversation)
        .unwrap();
    drop(storage);
    materialize_fresh_fts_schema_via_rusqlite(&db_path).unwrap();

    let conn = rusqlite_test_fixture_conn(&db_path);
    conn.execute_batch("PRAGMA writable_schema = ON;").unwrap();
    conn.execute(
        "INSERT INTO sqlite_master(type, name, tbl_name, rootpage, sql)
         VALUES('table', 'fts_messages', 'fts_messages', 0, ?1)",
        ["CREATE VIRTUAL TABLE fts_messages USING fts5(content, title, agent, workspace, source_path, created_at UNINDEXED, message_id UNINDEXED, tokenize='porter')"],
    )
    .unwrap();
    conn.execute_batch("PRAGMA writable_schema = OFF;").unwrap();
    let duplicate_rows: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE name = 'fts_messages'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(duplicate_rows, 2);
    drop(conn);

    let inserted = rebuild_fts_via_rusqlite(&db_path).unwrap();
    // The frankensqlite loader discards the stale rootpage-zero duplicate
    // while retaining the independently reloadable canonical FTS5 table.
    // Depending on whether that retained table is already hydrated, the
    // explicit rebuild either observes exact parity (zero inserted) or
    // repopulates the sole canonical row. The exact postcondition below,
    // rather than the internal repair route, is the compatibility
    // contract this regression protects.
    assert!(inserted <= 1, "one canonical message bounds repair work");

    let conn = FrankenConnection::open(db_path.to_string_lossy().into_owned()).unwrap();
    let schema_rows = franken_fts_schema_rows(&conn).unwrap();
    assert_eq!(
        schema_rows, 1,
        "DROP TABLE should leave one clean FTS schema"
    );
    let match_count: i64 = conn
        .query_row_map("SELECT COUNT(*) FROM fts_messages", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(match_count, 1);
}
