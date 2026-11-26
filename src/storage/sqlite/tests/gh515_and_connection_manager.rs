use super::*;
use crate::franken_sync::compat::TransactionExt as FrankenTransactionExt;
use std::time::Duration;
use tempfile::TempDir;

#[test]
fn franken_insert_conversations_batched_populates_analytics_rollups() {
    use crate::franken_sync::compat::{ConnectionExt, RowExt};
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("franken-index.db");
    let storage = FrankenStorage::open(&db_path).unwrap();

    let agent = Agent {
        id: None,
        slug: "claude_code".into(),
        name: "Claude Code".into(),
        version: Some("1.0".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();

    let ts_ms = 1_770_551_400_000_i64;
    let usage_json = serde_json::json!({
        "message": {
            "model": "claude-opus-4-6",
            "usage": {
                "input_tokens": 100,
                "output_tokens": 50,
                "cache_read_input_tokens": 25,
                "cache_creation_input_tokens": 10,
                "service_tier": "standard"
            }
        }
    });

    let conv = Conversation {
        id: None,
        agent_slug: "claude_code".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: Some("franken-batch-upsert".into()),
        title: Some("Franken batch upsert".into()),
        source_path: PathBuf::from("/tmp/franken.jsonl"),
        started_at: Some(ts_ms),
        ended_at: Some(ts_ms + 60_000),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: vec![
            Message {
                id: None,
                idx: 0,
                role: MessageRole::User,
                author: None,
                created_at: Some(ts_ms),
                content: "Please make a plan.".into(),
                extra_json: serde_json::Value::Null,
                snippets: vec![],
            },
            Message {
                id: None,
                idx: 1,
                role: MessageRole::Agent,
                author: None,
                created_at: Some(ts_ms + 30_000),
                content: "## Plan\n\n1. Reproduce\n2. Patch\n3. Verify".into(),
                extra_json: usage_json,
                snippets: vec![],
            },
        ],
        source_id: "local".into(),
        origin_host: None,
    };

    let outcomes = storage
        .insert_conversations_batched_with_analytics(&[(agent_id, None, &conv)], false)
        .unwrap();
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].inserted_indices, vec![0, 1]);

    let conn = storage.raw();
    let daily_stats_rows: i64 = conn
        .query_row_map("SELECT COUNT(*) FROM daily_stats", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    let token_daily_rows: i64 = conn
        .query_row_map(
            "SELECT COUNT(*) FROM token_daily_stats",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    let usage_daily_rows: i64 = conn
        .query_row_map("SELECT COUNT(*) FROM usage_daily", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    let model_daily_rows: i64 = conn
        .query_row_map(
            "SELECT COUNT(*) FROM usage_models_daily",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();

    assert!(daily_stats_rows > 0, "daily_stats should be populated");
    assert!(
        token_daily_rows > 0,
        "token_daily_stats should be populated"
    );
    assert!(usage_daily_rows > 0, "usage_daily should be populated");
    assert!(
        model_daily_rows > 0,
        "usage_models_daily should be populated"
    );
}

// =========================================================================
// FrankenConnectionManager tests (bead 3rlf8)
// =========================================================================

#[test]
fn connection_manager_creates_readers() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("cm.db");

    // Create the DB first
    let fs = FrankenStorage::open(&db_path).unwrap();
    drop(fs);

    let config = ConnectionManagerConfig {
        reader_count: 3,
        max_writers: 2,
    };
    let mgr = FrankenConnectionManager::new(&db_path, config).unwrap();
    assert_eq!(mgr.reader_count(), 3);
    assert_eq!(mgr.max_writers(), 2);
}

#[test]
fn connection_manager_clamps_zero_writer_limit_to_prevent_deadlock() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("cm.db");

    let fs = FrankenStorage::open(&db_path).unwrap();
    drop(fs);

    let mgr = std::sync::Arc::new(
        FrankenConnectionManager::new(
            &db_path,
            ConnectionManagerConfig {
                reader_count: 0,
                max_writers: 0,
            },
        )
        .unwrap(),
    );
    assert_eq!(mgr.reader_count(), 1);
    assert_eq!(mgr.max_writers(), 1);

    let (tx, rx) = std::sync::mpsc::channel();
    let mgr_for_thread = std::sync::Arc::clone(&mgr);
    std::thread::spawn(move || {
        let result = mgr_for_thread.writer().map(|mut guard| {
            guard.mark_committed();
        });
        tx.send(result.is_ok()).expect("writer result send");
    });

    assert!(
        rx.recv_timeout(Duration::from_secs(10)).unwrap(),
        "writer acquisition should not block forever when configured with zero writer slots"
    );
}

#[test]
fn connection_manager_reader_round_robin() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("cm.db");

    let fs = FrankenStorage::open(&db_path).unwrap();
    drop(fs);

    let config = ConnectionManagerConfig {
        reader_count: 2,
        max_writers: 1,
    };
    let mgr = FrankenConnectionManager::new(&db_path, config).unwrap();

    // Reader index should advance (round-robin)
    let idx_before = mgr.reader_idx.load(std::sync::atomic::Ordering::Relaxed);
    let _r1 = mgr.reader();
    let idx_after = mgr.reader_idx.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(idx_after, idx_before + 1, "reader index should advance");
}

/// GH #386: a Track B rebuild must checkpoint per batch and resume from
/// the persisted cursor instead of rescanning from zero, and must discard
/// that checkpoint when the ledger underneath it has changed.
#[test]
fn token_daily_stats_rebuild_resumes_from_persisted_cursor() {
    use crate::franken_sync::compat::{ConnectionExt as _, RowExt as _};

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("track-b.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    let raw = storage.raw();
    raw.execute_compat(
        "INSERT INTO agents(id, slug, name, kind, created_at, updated_at) \
         VALUES(1, 'codex', 'codex', 'cli', 0, 0)",
        fparams![],
    )
    .unwrap();
    for conversation_id in 1_i64..=3 {
        raw.execute_compat(
            "INSERT INTO conversations(id, agent_id, source_id, source_path, started_at) \
             VALUES(?1, 1, 'local', ?2, 86400000)",
            fparams![
                conversation_id,
                format!("/tmp/conv-{conversation_id}.jsonl")
            ],
        )
        .unwrap();
        raw.execute_compat(
            "INSERT INTO messages(id, conversation_id, idx, role, content) \
             VALUES(?1, ?1, 0, 'assistant', 'answer')",
            fparams![conversation_id],
        )
        .unwrap();
        raw.execute_compat(
            "INSERT INTO token_usage(
                 message_id, conversation_id, agent_id, timestamp_ms, day_id,
                 role, content_chars, input_tokens, output_tokens, total_tokens
             ) VALUES(?1, ?1, 1, 86400000, 1, 'assistant', 6, 10, 5, 15)",
            fparams![conversation_id],
        )
        .unwrap();
    }

    let count = |sql: &str| -> i64 {
        raw.query_row_map(sql, fparams![], |row| row.get_typed(0))
            .unwrap()
    };

    // A complete run leaves no checkpoint and no scratch table behind.
    let full_rows = storage.rebuild_token_daily_stats().unwrap();
    assert!(full_rows > 0);
    let live_after_full = count("SELECT COUNT(*) FROM token_daily_stats");
    assert!(live_after_full > 0);
    assert!(
        !historical_table_exists(raw, TOKEN_DAILY_STATS_REBUILD_STAGE_TABLE).unwrap(),
        "a finished rebuild must drop its stage table"
    );
    assert!(
        read_token_daily_stats_rebuild_cursor(raw)
            .unwrap()
            .is_none()
    );
    let full_total_tokens = count(
        "SELECT COALESCE(SUM(grand_total_tokens), 0) FROM token_daily_stats \
         WHERE agent_slug = 'all' AND source_id = 'all' AND model_family = 'all'",
    );
    assert_eq!(full_total_tokens, 45, "three conversations x 15 tokens");

    // Simulate an interrupted run that had already processed every
    // conversation: the stage holds a sentinel aggregate and the cursor
    // points past the last conversation id. Resuming must NOT rescan —
    // the sentinel (not the true aggregate) becomes the live table.
    let ledger = token_daily_stats_ledger_fingerprint(raw).unwrap();
    raw.execute(&token_daily_stats_rebuild_stage_ddl()).unwrap();
    raw.execute_compat(
        "INSERT INTO token_daily_stats_rebuild_stage(
             day_id, agent_slug, source_id, model_family, grand_total_tokens, last_updated
         ) VALUES(1, 'all', 'all', 'all', 999, 0)",
        fparams![],
    )
    .unwrap();
    {
        let mut tx = raw
            .transaction_with_behavior(crate::franken_sync::TransactionBehavior::Immediate)
            .unwrap();
        write_token_daily_stats_rebuild_cursor(
            &tx,
            &TokenDailyStatsRebuildCursor {
                ledger_fingerprint: ledger.fingerprint(),
                last_conversation_id: 3,
                rows_created: 1,
            },
        )
        .unwrap();
        tx.commit().unwrap();
    }
    let resumed_rows = storage.rebuild_token_daily_stats().unwrap();
    assert_eq!(resumed_rows, 1, "resume reports the checkpointed count");
    assert_eq!(count("SELECT COUNT(*) FROM token_daily_stats"), 1);
    assert_eq!(
        count("SELECT grand_total_tokens FROM token_daily_stats"),
        999,
        "the staged aggregate was swapped in without rescanning"
    );
    assert!(
        read_token_daily_stats_rebuild_cursor(raw)
            .unwrap()
            .is_none()
    );

    // A checkpoint taken against a different ledger is discarded: the
    // rebuild starts from zero and recomputes the true aggregate.
    raw.execute(&token_daily_stats_rebuild_stage_ddl()).unwrap();
    raw.execute_compat(
        "INSERT INTO token_daily_stats_rebuild_stage(
             day_id, agent_slug, source_id, model_family, grand_total_tokens, last_updated
         ) VALUES(1, 'all', 'all', 'all', 999, 0)",
        fparams![],
    )
    .unwrap();
    {
        let mut tx = raw
            .transaction_with_behavior(crate::franken_sync::TransactionBehavior::Immediate)
            .unwrap();
        write_token_daily_stats_rebuild_cursor(
            &tx,
            &TokenDailyStatsRebuildCursor {
                ledger_fingerprint: "token_usage-v1:0:0".to_string(),
                last_conversation_id: 3,
                rows_created: 1,
            },
        )
        .unwrap();
        tx.commit().unwrap();
    }
    assert_eq!(storage.rebuild_token_daily_stats().unwrap(), full_rows);
    assert_eq!(
        count(
            "SELECT COALESCE(SUM(grand_total_tokens), 0) FROM token_daily_stats \
             WHERE agent_slug = 'all' AND source_id = 'all' AND model_family = 'all'"
        ),
        45,
        "a stale checkpoint must be thrown away, not swapped in"
    );
    assert_eq!(
        count("SELECT COUNT(*) FROM token_daily_stats"),
        live_after_full
    );
}

/// A rebuild scans through separately committed batches, so a concurrent
/// ledger writer can otherwise make the stage a mixture of snapshots. The
/// pre-publish fingerprint guard must preserve the live rollup rather than
/// knowingly replacing it with that inconsistent stage.
#[test]
fn token_daily_stats_rebuild_refuses_to_publish_when_ledger_changes_mid_run() {
    use crate::franken_sync::compat::{ConnectionExt as _, RowExt as _};
    use std::cell::Cell;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("track-b-ledger-change.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    let raw = storage.raw();
    raw.execute_compat(
        "INSERT INTO agents(id, slug, name, kind, created_at, updated_at) \
         VALUES(1, 'codex', 'codex', 'cli', 0, 0)",
        fparams![],
    )
    .unwrap();
    raw.execute_compat(
        "INSERT INTO conversations(id, agent_id, source_id, source_path, started_at) \
         VALUES(1, 1, 'local', '/tmp/ledger-change.jsonl', 86400000)",
        fparams![],
    )
    .unwrap();
    raw.execute_compat(
        "INSERT INTO messages(id, conversation_id, idx, role, content) VALUES \
         (1, 1, 0, 'assistant', 'first'), \
         (2, 1, 1, 'assistant', 'second')",
        fparams![],
    )
    .unwrap();
    raw.execute_compat(
        "INSERT INTO token_usage( \
             message_id, conversation_id, agent_id, timestamp_ms, day_id, \
             role, content_chars, input_tokens, output_tokens, total_tokens \
         ) VALUES(1, 1, 1, 86400000, 1, 'assistant', 5, 10, 5, 15)",
        fparams![],
    )
    .unwrap();
    raw.execute_compat(
        "INSERT INTO token_daily_stats( \
             day_id, agent_slug, source_id, model_family, grand_total_tokens, last_updated \
         ) VALUES(1, 'all', 'all', 'all', 777, 0)",
        fparams![],
    )
    .unwrap();

    let heartbeat_calls = Cell::new(0_usize);
    let heartbeat = || {
        let call = heartbeat_calls.get().saturating_add(1);
        heartbeat_calls.set(call);
        if call == 2 {
            raw.execute_compat(
                "INSERT INTO token_usage( \
                     message_id, conversation_id, agent_id, timestamp_ms, day_id, \
                     role, content_chars, input_tokens, output_tokens, total_tokens \
                 ) VALUES(2, 1, 1, 86400000, 1, 'assistant', 6, 20, 10, 30)",
                fparams![],
            )
            .unwrap();
        }
    };

    let error = storage
        .rebuild_token_daily_stats_with_progress(Some(&heartbeat), None)
        .expect_err("a changed ledger must reject publication");
    assert!(
        error.to_string().contains("phase=pre_publish_consistency"),
        "the error must identify the exact rejected publication phase: {error:#}"
    );
    let live_total: i64 = raw
        .query_row_map(
            "SELECT grand_total_tokens FROM token_daily_stats \
             WHERE day_id = 1 AND agent_slug = 'all' \
               AND source_id = 'all' AND model_family = 'all'",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(
        live_total, 777,
        "a rejected mixed-snapshot stage must not replace the last-good live rollup"
    );
    assert!(
        historical_table_exists(raw, TOKEN_DAILY_STATS_REBUILD_STAGE_TABLE).unwrap(),
        "the rejected stage remains available for deterministic reset on retry"
    );
    assert!(
        read_token_daily_stats_rebuild_cursor(raw)
            .unwrap()
            .is_some(),
        "the rejected stage must retain its matching checkpoint"
    );
}

#[test]
fn connection_manager_writer_reads_and_writes() {
    use crate::franken_sync::compat::RowExt;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("cm.db");

    let fs = FrankenStorage::open(&db_path).unwrap();
    drop(fs);

    let mgr = FrankenConnectionManager::new(&db_path, Default::default()).unwrap();

    // Acquire writer and insert data
    {
        let mut guard = mgr.writer().unwrap();
        guard
            .storage()
            .raw()
            .execute("CREATE TABLE IF NOT EXISTS cm_test (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        guard
            .storage()
            .raw()
            .execute("INSERT INTO cm_test (val) VALUES ('hello')")
            .unwrap();
        guard.mark_committed();
    }

    // Verify via a guard around the dedicated-owner reader handle.
    let reader_guard = mgr.reader();
    let rows = reader_guard.query("SELECT val FROM cm_test").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get_typed::<String>(0).unwrap(), "hello");
}

#[test]
fn connection_manager_writer_guard_drops_releases_slot() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("cm.db");

    let fs = FrankenStorage::open(&db_path).unwrap();
    drop(fs);

    let config = ConnectionManagerConfig {
        reader_count: 1,
        max_writers: 1,
    };
    let mgr = FrankenConnectionManager::new(&db_path, config).unwrap();

    // Acquire and release writer
    {
        let mut guard = mgr.writer().unwrap();
        guard.mark_committed();
    }

    // Should be able to acquire again (slot released)
    let mut guard2 = mgr.writer().unwrap();
    guard2.mark_committed();
}

#[test]
fn connection_manager_concurrent_writer_works() {
    use crate::franken_sync::compat::RowExt;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("cm.db");

    let fs = FrankenStorage::open(&db_path).unwrap();
    drop(fs);

    let config = ConnectionManagerConfig {
        reader_count: 1,
        max_writers: 2,
    };
    let mgr = FrankenConnectionManager::new(&db_path, config).unwrap();

    {
        let mut guard = mgr.concurrent_writer().unwrap();
        guard
            .storage()
            .raw()
            .execute("CREATE TABLE IF NOT EXISTS cm_conc (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        guard
            .storage()
            .raw()
            .execute("INSERT INTO cm_conc (val) VALUES ('concurrent')")
            .unwrap();
        guard.mark_committed();
    }

    let reader_guard = mgr.reader();
    let rows = reader_guard.query("SELECT val FROM cm_conc").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get_typed::<String>(0).unwrap(), "concurrent");
}

#[test]
fn connection_manager_default_config() {
    let config = ConnectionManagerConfig::default();
    assert_eq!(config.reader_count, 4);
    assert!(config.max_writers > 0);
}

#[test]
fn owner_open_in_memory_sync_and_open_sync_are_writable() {
    use crate::franken_sync::compat::RowExt;

    let mem1 = FrankenOwnerConnection::open_in_memory_sync().expect("open_in_memory_sync");
    mem1.execute_sync("CREATE TABLE m1(x INTEGER); INSERT INTO m1(x) VALUES (7);")
        .expect("write via open_in_memory_sync");
    let rows1 = mem1.query_sync("SELECT x FROM m1").expect("query m1");
    assert_eq!(rows1[0].get_typed::<i64>(0).unwrap(), 7);

    let mem2 = FrankenOwnerConnection::open_sync(":memory:").expect("open_sync(:memory:)");
    mem2.execute_sync("CREATE TABLE m2(x INTEGER); INSERT INTO m2(x) VALUES (11);")
        .expect("write via open_sync(:memory:)");
    let rows2 = mem2.query_sync("SELECT x FROM m2").expect("query m2");
    assert_eq!(rows2[0].get_typed::<i64>(0).unwrap(), 11);

    let mem3 = FrankenOwnerConnection::open_sync("file::memory:").expect("file::memory:");
    mem3.execute_sync("CREATE TABLE m3(x INTEGER); INSERT INTO m3(x) VALUES (13);")
        .expect("write via file::memory:");
    let rows3 = mem3.query_sync("SELECT x FROM m3").expect("query m3");
    assert_eq!(rows3[0].get_typed::<i64>(0).unwrap(), 13);
}

#[test]
fn connection_manager_readers_on_wal_do_not_poison_concurrent_writers() {
    use crate::franken_sync::compat::{ConnectionExt, RowExt};

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("cm_wal_no_poison.db");

    let fs = FrankenStorage::open(&db_path).unwrap();
    fs.raw().execute("PRAGMA journal_mode = WAL;").unwrap();
    fs.raw()
        .execute_batch("CREATE TABLE cm_t (id INTEGER PRIMARY KEY, val TEXT); INSERT INTO cm_t (val) VALUES ('init');")
        .unwrap();
    fs.close_without_checkpoint().unwrap();

    let config = ConnectionManagerConfig {
        reader_count: 2,
        max_writers: 2,
    };
    let mgr = FrankenConnectionManager::new(&db_path, config).unwrap();

    // Hold a reader open while acquiring writers so any shared shm node would expose isReadonly=1.
    let initial_reader = mgr.reader();
    let initial_rows = initial_reader.query("SELECT val FROM cm_t").unwrap();
    assert_eq!(initial_rows.len(), 1);
    drop(initial_reader);

    {
        let mut guard = mgr.concurrent_writer().unwrap();
        guard
            .storage()
            .raw()
            .execute("INSERT INTO cm_t (val) VALUES ('from_concurrent')")
            .unwrap();
        guard.mark_committed();
    }

    {
        let mut guard = mgr.writer().unwrap();
        guard
            .storage()
            .raw()
            .execute("INSERT INTO cm_t (val) VALUES ('from_writer')")
            .unwrap();
        guard.mark_committed();
    }

    // Read across all reader slots to trigger epoch reopen via open_pool_reader.
    let mut vals = Vec::new();
    for _ in 0..2 {
        let reader_guard = mgr.reader();
        let rows = reader_guard
            .query("SELECT val FROM cm_t ORDER BY id")
            .unwrap();
        vals = rows
            .iter()
            .map(|r| r.get_typed::<String>(0).unwrap())
            .collect();
    }
    assert_eq!(vals, vec!["init", "from_concurrent", "from_writer"]);
}
