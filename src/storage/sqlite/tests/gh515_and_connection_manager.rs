use super::*;
use crate::franken_sync::{
    Connection as FrankenConnection,
    compat::{
        ConnectionExt as FrankenConnectionExt, OpenFlags as FrankenOpenFlags,
        RowExt as FrankenRowExt, TransactionExt as FrankenTransactionExt,
        open_with_flags as open_franken_with_flags,
    },
};
use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use anyhow::{Context, Result, bail};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};
use tempfile::TempDir;

fn gh515_v21_preparation_fixture(path: &Path) -> Result<FrankenStorage> {
    let conn = FrankenConnection::open(path.to_string_lossy().into_owned())?;
    run_attributed_migration_steps(&conn, BASE_MIGRATION_STEPS)?;
    apply_conversation_tail_state_cache_migration(&conn)?;
    let historic_steps: Vec<_> = POST_TAIL_CACHE_MIGRATION_STEPS
        .iter()
        .copied()
        .filter(|(version, _, _)| *version <= 21)
        .collect();
    run_attributed_migration_steps(&conn, &historic_steps)?;
    let fixture = FrankenStorage::new(conn, path.to_path_buf());
    fixture.apply_config()?;
    fixture.sync_meta_schema_version(21)?;
    seed_atomic_fts_rebuild_fixture(&fixture);
    assert_eq!(fixture.schema_version()?, 21);
    assert!(
        fixture
            .raw()
            .query("SELECT name FROM sqlite_master WHERE name = 'forgotten_sources'")?
            .is_empty(),
        "the fixture must genuinely predate the v22 table"
    );
    Ok(fixture)
}

fn gh515_stock_sqlite_v21_preparation_fixture(path: &Path) -> Result<()> {
    assert!(
        !path.exists(),
        "the stock-SQLite fixture must use a fresh path"
    );
    // A FrankenSQLite-created fixture is stamped at birth and cannot prove
    // first-engine-open validation. Build this historical archive entirely
    // through the existing stock-SQLite interop layer, retaining its meta
    // version tracking for the normal CASS migration-ledger transition.
    let conn = rusqlite_test_fixture_conn(path);
    conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON;")?;
    for &(_, _, sql) in BASE_MIGRATION_STEPS {
        conn.execute_batch(sql)?;
    }
    // The combined V13 schema already includes the V15 tail-state columns.
    conn.execute_batch(MIGRATION_V15_TAIL_STATE_TABLE)?;
    for &(version, _, sql) in POST_TAIL_CACHE_MIGRATION_STEPS {
        if version <= 21 {
            conn.execute_batch(sql)?;
        }
    }
    conn.execute_batch(
        "INSERT INTO meta(key, value) VALUES('schema_version', '21');
         INSERT INTO agents(id, slug, name, kind, created_at, updated_at)
         VALUES(1, 'codex', 'Codex', 'cli', 1700000000000, 1700000000000);
         INSERT INTO workspaces(id, path) VALUES(1, '/tmp/gh515-stock-sqlite');
         INSERT INTO conversations(id, agent_id, workspace_id, source_path, started_at)
         VALUES(1, 1, 1, '/tmp/gh515-stock-sqlite/session.jsonl', 1700000000000);
         INSERT INTO messages(id, conversation_id, idx, role, content, created_at)
         VALUES(1, 1, 0, 'user', 'first engine migration retains canonical text', 1700000000001);",
    )?;
    conn.execute_batch(FTS5_REGISTER_SQL)?;
    conn.execute_batch(
        "INSERT INTO fts_messages(rowid, content, title, agent, workspace, source_path, created_at)
         VALUES(1, 'first engine migration retains canonical text', '', 'codex',
                '/tmp/gh515-stock-sqlite', '/tmp/gh515-stock-sqlite/session.jsonl', 1700000000001);",
    )?;
    assert_eq!(
        conn.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))?,
        "ok",
        "the first engine open must receive a valid stock-SQLite archive"
    );
    assert_eq!(
        conn.query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |row| row.get::<_, String>(0),
        )?,
        "21"
    );
    assert_eq!(
        conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE name = 'forgotten_sources'",
            [],
            |row| row.get::<_, i64>(0),
        )?,
        0,
        "the stock-SQLite fixture must genuinely predate the v22 table"
    );
    conn.close().map_err(|(_, error)| error)?;
    assert!(!database_sidecar_path(path, ".fsqlite-migration-state").exists());
    assert!(
        !database_sidecar_path(path, "-wal").exists(),
        "closing the sole stock-SQLite owner must leave WAL initialization pending"
    );
    Ok(())
}

#[test]
fn gh515_first_v22_migration_then_preparation_preserves_live_reader() -> Result<()> {
    use crate::franken_sync::FrankenError;

    let test_name = exact_child_test_name(
        module_path!(),
        "gh515_first_v22_migration_then_preparation_preserves_live_reader",
    );
    const DIRECTORY_ENV: &str = "CASS_TEST_GH515_MIGRATION_DIRECTORY";
    const CHILD_TIMEOUT: Duration = Duration::from_secs(60);

    let Some(test_root) = std::env::var_os(DIRECTORY_ENV) else {
        struct Child(std::process::Child);
        impl Drop for Child {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }

        // The parent owns all fixture files and reaps the SQL owner before
        // deleting them, including when a stuck migration exceeds its bound.
        let directory = TempDir::new()?;
        let log_path = directory.path().join("same-thread-migration.log");
        let log = fs::File::create(&log_path)?;
        let mut child = Child(
            Command::new(std::env::current_exe()?)
                .args(["--exact", &test_name, "--test-threads=1", "--nocapture"])
                .env(DIRECTORY_ENV, directory.path())
                .env("CASS_DEFER_LEXICAL_UPDATES", "0")
                .env("CASS_FTS_SHADOW_MAX_MESSAGES", "1000")
                .stdout(log.try_clone()?)
                .stderr(log)
                .spawn()?,
        );
        let deadline = Instant::now() + CHILD_TIMEOUT;
        loop {
            if let Some(status) = child.0.try_wait()? {
                let output = fs::read_to_string(&log_path)?;
                anyhow::ensure!(
                    status.success() && output.contains("1 passed"),
                    "GH515 same-thread migration child failed ({status}):\n{output}"
                );
                return Ok(());
            }
            if Instant::now() >= deadline {
                bail!(
                    "GH515 same-thread migration child exceeded {CHILD_TIMEOUT:?}:\n{}",
                    fs::read_to_string(&log_path)?
                );
            }
            std::thread::park_timeout(Duration::from_millis(10));
        }
    };
    let test_root = PathBuf::from(test_root);
    for (stock_sqlite_fixture, retain_reader) in
        [(false, false), (false, true), (true, false), (true, true)]
    {
        let directory = TempDir::new_in(&test_root)?;
        let path = directory.path().join("v21-preparation.db");
        eprintln!(
            "GH515 migration: stock_sqlite_fixture={stock_sqlite_fixture}, retain_reader={retain_reader}"
        );
        if stock_sqlite_fixture {
            gh515_stock_sqlite_v21_preparation_fixture(&path)?;
            assert!(
                !database_sidecar_path(&path, ".fsqlite-migration-state").exists(),
                "the stock-SQLite archive must have no marker before its first engine open"
            );
        } else {
            let fixture = gh515_v21_preparation_fixture(&path)?;
            fixture.close_without_checkpoint()?;
        }

        // Keep the same owner thread across open, migration and the two
        // reported preparation queries. A peer on an initialized WAL can
        // retain its snapshot through DDL and subsequent writer activity.
        // A stock archive's first reader instead pins the main image while
        // WAL initialization is still pending; that maintenance must wait
        // until the reader releases its snapshot.
        let mut reader = if retain_reader {
            let reader = FrankenConnection::open_schema_only_with_wal_index_recovery(
                path.to_string_lossy().into_owned(),
            )?;
            reader.execute("BEGIN DEFERRED")?;
            assert_eq!(
                reader
                    .query_row("SELECT COUNT(*) FROM messages")?
                    .get_typed::<i64>(0)?,
                1
            );
            Some(reader)
        } else {
            None
        };
        if stock_sqlite_fixture && retain_reader {
            // fsqlite 0.4.9 install_wal_backend_with_vfs routes a missing
            // WAL through with_wal_initialization. Its exclusive pager
            // maintenance upgrade refuses an active peer transaction with
            // BusyRecovery before initializing the WAL or running repair.
            let original = fs::read(&path)?;
            let started = Instant::now();
            let error =
                match open_current_schema_storage_with_timeout(&path, Duration::from_secs(10)) {
                    Err(error) => error,
                    Ok(_) => bail!("first WAL initialization must respect the live reader"),
                };
            assert!(
                matches!(
                    error.downcast_ref::<FrankenError>(),
                    Some(FrankenError::BusyRecovery)
                ),
                "first-open contention must retain its retryable type: {error:#}"
            );
            assert!(
                started.elapsed() < CHILD_TIMEOUT,
                "contended bootstrap must settle within the existing child bound"
            );
            assert!(
                !database_sidecar_path(&path, ".fsqlite-migration-state").exists(),
                "a refused bootstrap must not claim that engine migration completed"
            );
            assert_eq!(
                fs::read(&path)?,
                original,
                "a refused bootstrap must preserve the canonical database image"
            );
            let peer = reader.take().context("retained stock-archive reader")?;
            assert_eq!(
                peer.query_row("SELECT value FROM meta WHERE key = 'schema_version'")?
                    .get_typed::<String>(0)?,
                "21"
            );
            assert_eq!(
                peer.query_row("SELECT COUNT(*) FROM messages")?
                    .get_typed::<i64>(0)?,
                1
            );
            assert_eq!(
                peer.query_row("SELECT content FROM messages WHERE id = 1")?
                    .get_typed::<String>(0)?,
                "first engine migration retains canonical text"
            );
            peer.execute("ROLLBACK")?;
            peer.close_without_checkpoint()?;
        }
        let cancellation = crate::franken_sync::CancellationHandle::default();
        let migrated = {
            let _scope = cancellation.enter();
            if stock_sqlite_fixture {
                assert!(
                    !database_sidecar_path(&path, ".fsqlite-migration-state").exists(),
                    "the optional schema-only reader must leave engine migration pending"
                );
                // Follow index admission: the ordinary constructor must
                // run pending engine validation even with a schema-only
                // peer, then v21 falls back to normal CASS migration.
                assert!(
                    open_current_schema_storage_with_timeout(&path, Duration::from_secs(10))?
                        .is_none(),
                    "the historical v21 schema must still require CASS migration"
                );
                assert!(
                    index_engine_migration_is_complete(&path),
                    "first-engine-open validation must complete before v22 migration"
                );
            }
            let migrated = FrankenStorage::open(&path)?;
            assert_eq!(migrated.schema_version()?, CURRENT_SCHEMA_VERSION);
            assert_eq!(
                migrated
                    .raw()
                    .query_row("SELECT COUNT(*) FROM messages")?
                    .get_typed::<i64>(0)?,
                1,
                "the exact message count must finish on the migration handle"
            );
            assert_eq!(
                migrated.fts_shadow_viability_with_bound(None)?,
                FtsShadowViability::Viable { corpus_messages: 1 }
            );
            migrated.raw().execute(
                "INSERT INTO forgotten_sources(source_path, forgotten_at_ms) \
                 VALUES('/tmp/v22-recovery.jsonl', 1700000000000)",
            )?;
            migrated
        };
        if let Some(reader) = reader {
            assert_eq!(
                reader
                    .query_row("SELECT COUNT(*) FROM messages")?
                    .get_typed::<i64>(0)?,
                1,
                "migration must preserve the live reader's canonical snapshot"
            );
            reader.execute("ROLLBACK")?;
            reader.close_without_checkpoint()?;
        }
        let integrity = migrated.raw().query("PRAGMA integrity_check")?;
        assert!(
            !integrity.is_empty(),
            "integrity proof must return a result"
        );
        for row in integrity {
            assert_eq!(row.get_typed::<String>(0)?, "ok");
        }
        migrated.close()?;
        let reopened = FrankenStorage::open(&path)?;
        assert_eq!(reopened.schema_version()?, CURRENT_SCHEMA_VERSION);
        assert_eq!(
            reopened
                .raw()
                .query_row("SELECT COUNT(*) FROM messages")?
                .get_typed::<i64>(0)?,
            1
        );
        assert_eq!(
            reopened.fts_shadow_viability_with_bound(None)?,
            FtsShadowViability::Viable { corpus_messages: 1 }
        );
        assert_eq!(
            reopened
                .raw()
                .query_row("SELECT COUNT(*) FROM forgotten_sources")?
                .get_typed::<i64>(0)?,
            1,
            "the first post-migration write must survive reopening"
        );
        reopened.close()?;
    }
    Ok(())
}

/// Every potentially blocking engine operation runs in an owned child,
/// including fixture creation and recovery. The supervisor can therefore
/// fail a preparation stall and reap both peers without abandoning a
/// thread-affine connection or leaving an orphaned test process behind.
#[test]
fn gh515_cross_process_migration_and_preparation_are_bounded() -> Result<()> {
    const ROLE_ENV: &str = "CASS_TEST_GH515_STORAGE_ROLE";
    const DIRECTORY_ENV: &str = "CASS_TEST_GH515_STORAGE_DIRECTORY";
    const BASE_MESSAGES: i64 = 128;
    const INDEX_ATTEMPTS: i64 = 7;
    const CHILD_TIMEOUT: Duration = Duration::from_secs(60);

    if let Ok(role) = dotenvy::var(ROLE_ENV) {
        let directory = PathBuf::from(dotenvy::var(DIRECTORY_ENV)?);
        let path = directory.join("archive.db");
        let count = |conn: &FrankenConnection| -> Result<i64> {
            Ok(conn
                .query_row("SELECT COUNT(*) FROM messages")?
                .get_typed(0)?)
        };
        match role.as_str() {
            "fixture" => {
                eprintln!("GH515 fixture: create genuine v21 archive");
                let storage = gh515_v21_preparation_fixture(&path)?;
                let conversation_id = storage
                    .raw()
                    .query_row("SELECT id FROM conversations LIMIT 1")?
                    .get_typed::<i64>(0)?;
                let mut transaction = storage.raw().transaction()?;
                let body = "multipage canonical snapshot ".repeat(64);
                for idx in 1..BASE_MESSAGES {
                    transaction.execute_compat(
                        "INSERT INTO messages(conversation_id, idx, role, content) \
                         VALUES(?1, ?2, 'user', ?3)",
                        fparams![conversation_id, idx, body.as_str()],
                    )?;
                }
                transaction.commit()?;
                drop(transaction);
                storage.rebuild_fts()?;
                assert_eq!(count(storage.raw())?, BASE_MESSAGES);
                storage.close_without_checkpoint()?;
            }
            "reader" => {
                eprintln!("GH515 reader: establish v21 WAL snapshot");
                let reader = open_franken_with_flags(
                    path.to_string_lossy().as_ref(),
                    FrankenOpenFlags::SQLITE_OPEN_READ_ONLY,
                )?;
                reader.execute("BEGIN DEFERRED TRANSACTION")?;
                assert_eq!(count(&reader)?, BASE_MESSAGES);
                fs::write(directory.join("reader-ready"), b"pinned")?;
                let deadline = Instant::now() + CHILD_TIMEOUT + CHILD_TIMEOUT;
                while !directory.join("reader-release").exists() {
                    if Instant::now() >= deadline {
                        bail!("GH515 retained reader was not released");
                    }
                    std::thread::park_timeout(Duration::from_millis(10));
                }
                assert_eq!(
                    count(&reader)?,
                    BASE_MESSAGES,
                    "the peer must retain its original snapshot through migration and writes"
                );
                reader.execute("ROLLBACK")?;
                reader.close_without_checkpoint()?;
            }
            "writer" => {
                let cancellation = crate::franken_sync::CancellationHandle::default();
                let _scope = cancellation.enter();
                for attempt in 0..INDEX_ATTEMPTS {
                    eprintln!("GH515 writer {attempt}: open/migrate");
                    let storage = FrankenStorage::open(&path)?;
                    assert_eq!(storage.schema_version()?, CURRENT_SCHEMA_VERSION);
                    eprintln!("GH515 writer {attempt}: exact count");
                    assert_eq!(count(storage.raw())?, BASE_MESSAGES + attempt);
                    eprintln!("GH515 writer {attempt}: FTS viability");
                    assert_eq!(
                        storage.fts_shadow_viability_with_bound(None)?,
                        FtsShadowViability::Viable {
                            corpus_messages: u64::try_from(BASE_MESSAGES + attempt)?
                        }
                    );
                    eprintln!("GH515 writer {attempt}: append/close");
                    storage.raw().execute_compat(
                        "INSERT INTO messages(conversation_id, idx, role, content) \
                         SELECT id, ?1, 'user', 'post-migration message' \
                         FROM conversations LIMIT 1",
                        fparams![BASE_MESSAGES + attempt],
                    )?;
                    storage.raw().execute_compat(
                        "INSERT INTO forgotten_sources(source_path, forgotten_at_ms) \
                         VALUES(?1, 1700000000000)",
                        fparams![format!("/tmp/gh515-{attempt}.jsonl")],
                    )?;
                    storage.close_without_checkpoint()?;
                }
            }
            "recovery" => {
                eprintln!("GH515 recovery: reopen and verify committed archive");
                let storage = FrankenStorage::open(&path)?;
                assert_eq!(count(storage.raw())?, BASE_MESSAGES + INDEX_ATTEMPTS);
                assert_eq!(
                    storage
                        .raw()
                        .query_row("SELECT COUNT(*) FROM forgotten_sources")?
                        .get_typed::<i64>(0)?,
                    INDEX_ATTEMPTS
                );
                let integrity = storage.raw().query("PRAGMA quick_check")?;
                assert!(!integrity.is_empty());
                for row in integrity {
                    assert_eq!(row.get_typed::<String>(0)?, "ok");
                }
                storage.close()?;
            }
            _ => bail!("unknown GH515 storage child role: {role}"),
        }
        return Ok(());
    }

    struct Child {
        process: std::process::Child,
        log_path: PathBuf,
    }
    impl Child {
        fn spawn(directory: &Path, role: &str) -> Result<Self> {
            let log_path = directory.join(format!("{role}.log"));
            let log = fs::File::create(&log_path)?;
            let process = Command::new(std::env::current_exe()?)
                .args([
                    "--exact",
                    &exact_child_test_name(
                        module_path!(),
                        "gh515_cross_process_migration_and_preparation_are_bounded",
                    ),
                    "--nocapture",
                ])
                .env(ROLE_ENV, role)
                .env(DIRECTORY_ENV, directory)
                .env("CASS_DEFER_LEXICAL_UPDATES", "0")
                .env("CASS_FTS_SHADOW_MAX_MESSAGES", "1000")
                .stdout(log.try_clone()?)
                .stderr(log)
                .spawn()?;
            Ok(Self { process, log_path })
        }

        fn wait(&mut self, ready: Option<&Path>) -> Result<()> {
            let deadline = Instant::now() + CHILD_TIMEOUT;
            loop {
                if let Some(status) = self.process.try_wait()? {
                    let output = fs::read_to_string(&self.log_path)?;
                    if ready.is_some() || !status.success() || !output.contains("1 passed") {
                        bail!("GH515 storage child exited {status}:\n{output}");
                    }
                    return Ok(());
                }
                if ready.is_some_and(Path::exists) {
                    return Ok(());
                }
                if Instant::now() >= deadline {
                    let output = fs::read_to_string(&self.log_path)?;
                    bail!("GH515 storage child exceeded {CHILD_TIMEOUT:?}:\n{output}");
                }
                std::thread::park_timeout(Duration::from_millis(10));
            }
        }
    }
    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.process.kill();
            let _ = self.process.wait();
        }
    }

    for retain_reader in [false, true] {
        let directory = TempDir::new()?;
        Child::spawn(directory.path(), "fixture")?.wait(None)?;
        let mut reader = if retain_reader {
            let mut reader = Child::spawn(directory.path(), "reader")?;
            reader.wait(Some(&directory.path().join("reader-ready")))?;
            Some(reader)
        } else {
            None
        };
        Child::spawn(directory.path(), "writer")?.wait(None)?;
        if let Some(reader) = reader.as_mut() {
            assert!(reader.process.try_wait()?.is_none());
            fs::write(directory.path().join("reader-release"), b"release")?;
            reader.wait(None)?;
        }
        Child::spawn(directory.path(), "recovery")?.wait(None)?;
    }
    Ok(())
}

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
        let mut tx = raw.transaction().unwrap();
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
        let mut tx = raw.transaction().unwrap();
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
