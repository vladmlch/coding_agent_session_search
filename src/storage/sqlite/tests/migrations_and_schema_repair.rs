use super::*;
use crate::franken_sync::{
    Connection as FrankenConnection, SqliteValue,
    compat::{
        ConnectionExt as FrankenConnectionExt, ParamValue, RowExt as FrankenRowExt,
        TransactionExt as FrankenTransactionExt,
    },
};
use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use crate::sources::provenance::LOCAL_SOURCE_ID;
use serial_test::serial;
use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;

// =========================================================================
// FrankenStorage migration tests (bead 2j6p6)
// =========================================================================

/// Helper: create a FrankenStorage wrapping an in-memory connection and
/// run migrations. This exercises the same code path as `open()` but avoids
/// frankensqlite's file-based autoindex renaming limitation (V5 uses
/// ALTER TABLE RENAME which triggers sqlite_autoindex lookup issues on
/// file-based pagers).
fn franken_storage_in_memory() -> FrankenStorage {
    let conn = FrankenConnection::open(":memory:").unwrap();
    let storage = FrankenStorage::new(conn, PathBuf::from(":memory:"));
    storage.run_migrations().unwrap();
    storage.apply_config().unwrap();
    storage
}

#[test]
fn franken_migrations_create_all_tables() {
    let storage = franken_storage_in_memory();

    // Should be at CURRENT_SCHEMA_VERSION.
    let version = storage.schema_version().unwrap();
    assert_eq!(
        version, CURRENT_SCHEMA_VERSION,
        "fresh FrankenStorage should be at current schema version"
    );

    // Core tables from V1 should exist.
    let rows = storage
        .raw()
        .query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
        .unwrap();
    let table_names: Vec<String> = rows
        .iter()
        .filter_map(|r| r.get_typed::<String>(0).ok())
        .collect();

    for required in [
        "meta",
        "agents",
        "workspaces",
        "conversations",
        "messages",
        "snippets",
        "tags",
        "conversation_tags",
    ] {
        assert!(
            table_names.contains(&required.to_string()),
            "missing table: {required}"
        );
    }

    // V4 sources table.
    assert!(
        table_names.contains(&"sources".to_string()),
        "missing sources table"
    );

    // V8 daily_stats table.
    assert!(
        table_names.contains(&"daily_stats".to_string()),
        "missing daily_stats table"
    );

    // V9 embedding_jobs table.
    assert!(
        table_names.contains(&"embedding_jobs".to_string()),
        "missing embedding_jobs table"
    );

    // V11 message_metrics, usage_hourly, usage_daily tables.
    for analytics_table in ["message_metrics", "usage_hourly", "usage_daily"] {
        assert!(
            table_names.contains(&analytics_table.to_string()),
            "missing table: {analytics_table}"
        );
    }
    assert!(
        table_names.contains(&"conversation_tail_state".to_string()),
        "missing conversation_tail_state table"
    );
    assert!(
        table_names.contains(&"conversation_external_lookup".to_string()),
        "missing conversation_external_lookup table"
    );
    assert!(
        table_names.contains(&"conversation_external_tail_lookup".to_string()),
        "missing conversation_external_tail_lookup table"
    );

    // Fresh frankensqlite databases should record the combined V13 base
    // schema plus every additive post-V13 migration.
    let rows = storage
        .raw()
        .query("SELECT COUNT(*) FROM _schema_migrations;")
        .unwrap();
    let count: i64 = rows.first().unwrap().get_typed(0).unwrap();
    assert_eq!(
        count,
        (13..=CURRENT_SCHEMA_VERSION).count() as i64,
        "_schema_migrations should record the V13 base schema and post-V13 migrations"
    );

    // The latest applied migration should be the current schema version.
    let rows = storage
        .raw()
        .query("SELECT version FROM _schema_migrations ORDER BY version;")
        .unwrap();
    let versions: Vec<i64> = rows
        .iter()
        .map(|row| row.get_typed(0))
        .collect::<std::result::Result<_, _>>()
        .unwrap();
    assert_eq!(
        versions,
        (13..=CURRENT_SCHEMA_VERSION).collect::<Vec<i64>>(),
        "_schema_migrations should contain v13 through current"
    );
}

#[test]
fn franken_migrations_idempotent() {
    let storage = franken_storage_in_memory();
    assert_eq!(storage.schema_version().unwrap(), CURRENT_SCHEMA_VERSION);

    // Re-running migrations on the same connection is a no-op.
    storage.run_migrations().unwrap();
    assert_eq!(storage.schema_version().unwrap(), CURRENT_SCHEMA_VERSION);
}

/// The V13-era internal-content `fts_messages` CREATE injected by the
/// legacy-teardown fixtures (mirrors the catalog row MIGRATION_V14 drops).
const LEGACY_V13_FTS_MESSAGES_SQL: &str = "CREATE VIRTUAL TABLE fts_messages USING fts5(content, title, agent, workspace, source_path, created_at UNINDEXED, content='', tokenize='porter')";

#[test]
fn attributed_migration_steps_report_applied_versions_freshness_and_failing_step() {
    // Virgin connection: exercise the helper directly, without the
    // storage open path that runs migrations first.
    let conn = FrankenConnection::open(":memory:").unwrap();

    let (applied, was_fresh) = run_attributed_migration_steps(&conn, BASE_MIGRATION_STEPS).unwrap();
    assert_eq!(applied, vec![13, 14]);
    assert!(was_fresh, "first pass over an empty database is fresh");

    // Second pass: everything already applied, and the initial version is
    // no longer zero, so was_fresh mirrors the combined-runner semantics.
    let (applied, was_fresh) = run_attributed_migration_steps(&conn, BASE_MIGRATION_STEPS).unwrap();
    assert!(applied.is_empty());
    assert!(!was_fresh);

    // A failing step must be attributed to its exact version and name.
    let err = run_attributed_migration_steps(&conn, &[(99, "bogus_step", "THIS IS NOT VALID SQL")])
        .unwrap_err();
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("running schema migration v99 (bogus_step)"),
        "failure should name the failing step: {rendered}"
    );
}

#[test]
#[serial]
fn legacy_fts_teardown_refusal_names_migration_and_leaves_archive_untouched() {
    let _budget = set_env_var(LEGACY_FTS_TEARDOWN_BUDGET_ENV, "1");
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("canonical.db");

    // Seed a fully-migrated archive. No conversation is written, so the
    // V14 drop leaves no fts_messages catalog row behind.
    drop(FrankenStorage::open(&db_path).unwrap());

    // Downgrade to a V14-pending legacy state and inject the legacy
    // internal-content fts_messages catalog row (rootpage 0; the refusal
    // only reads the catalog, so absent shadow tables are fine here).
    {
        let fixture = rusqlite_test_fixture_conn(&db_path);
        fixture
            .execute_batch(
                "PRAGMA writable_schema = ON;
                 UPDATE meta SET value = '13' WHERE key = 'schema_version';
                 DELETE FROM _schema_migrations WHERE version = 14;
                 DELETE FROM sqlite_master WHERE name = 'fts_messages';",
            )
            .unwrap();
        fixture
            .execute(
                "INSERT INTO sqlite_master(type, name, tbl_name, rootpage, sql)
                 VALUES('table', 'fts_messages', 'fts_messages', 0, ?1)",
                [LEGACY_V13_FTS_MESSAGES_SQL],
            )
            .unwrap();
        fixture
            .execute_batch("PRAGMA writable_schema = OFF;")
            .unwrap();
    }

    let err = match FrankenStorage::open(&db_path) {
        Ok(_) => panic!("open must refuse the unbounded V14 teardown above the budget"),
        Err(err) => err,
    };
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("v14") && rendered.contains("fts_contentless"),
        "refusal should name the failing migration: {rendered}"
    );
    assert!(
        rendered.contains(LEGACY_FTS_TEARDOWN_BUDGET_ENV),
        "refusal should name the budget override: {rendered}"
    );
    assert!(
        rendered.contains("cass doctor check --json"),
        "refusal should carry the bounded-diagnosis next action: {rendered}"
    );

    // The archive is untouched: still V14-pending, still meta version 13.
    let check = rusqlite_test_fixture_conn(&db_path);
    let v14_rows: i64 = check
        .query_row(
            "SELECT COUNT(*) FROM _schema_migrations WHERE version = 14",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(v14_rows, 0, "refusal must not record the migration");
    let meta_version: String = check
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(meta_version, "13", "refusal must not modify the archive");
}

#[test]
#[serial]
fn legacy_fts_teardown_zero_budget_disables_the_refusal() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("canonical.db");
    drop(FrankenStorage::open(&db_path).unwrap());

    {
        let fixture = rusqlite_test_fixture_conn(&db_path);
        fixture
            .execute_batch(
                "PRAGMA writable_schema = ON;
                 UPDATE meta SET value = '13' WHERE key = 'schema_version';
                 DELETE FROM _schema_migrations WHERE version = 14;
                 DELETE FROM sqlite_master WHERE name = 'fts_messages';",
            )
            .unwrap();
        fixture
            .execute(
                "INSERT INTO sqlite_master(type, name, tbl_name, rootpage, sql)
                 VALUES('table', 'fts_messages', 'fts_messages', 0, ?1)",
                [LEGACY_V13_FTS_MESSAGES_SQL],
            )
            .unwrap();
        fixture
            .execute_batch("PRAGMA writable_schema = OFF;")
            .unwrap();
    }

    let _budget = set_env_var(LEGACY_FTS_TEARDOWN_BUDGET_ENV, "0");
    let storage = FrankenStorage::open(&db_path)
        .expect("zero budget must disable the refusal and allow the migration");
    assert_eq!(storage.schema_version().unwrap(), CURRENT_SCHEMA_VERSION);
}

#[test]
#[serial]
fn legacy_fts_teardown_refusal_skips_current_archives_entirely() {
    let _budget = set_env_var(LEGACY_FTS_TEARDOWN_BUDGET_ENV, "1");
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("canonical.db");

    drop(FrankenStorage::open(&db_path).unwrap());
    // Reopen: V14 is already applied, so even an absurdly small budget
    // must never fire the refusal on a current archive.
    let again = FrankenStorage::open(&db_path).unwrap();
    assert_eq!(again.schema_version().unwrap(), CURRENT_SCHEMA_VERSION);
}

#[test]
fn migration_v21_adds_covering_context_index_without_changing_archive_rows() {
    let storage = franken_storage_in_memory();
    let conn = storage.raw();
    // Reconstruct the preceding version, including a wide conversation.
    conn.execute_batch(
        "DROP INDEX idx_conversations_context;
         DELETE FROM _schema_migrations WHERE version >= 21;
         UPDATE meta SET value = '20' WHERE key = 'schema_version';
         INSERT INTO agents(id, slug, name, kind, created_at, updated_at)
             VALUES(1, 'codex', 'Codex', 'cli', 0, 0);
         INSERT INTO conversations(id, agent_id, source_path, started_at, metadata_bin)
             VALUES(1, 1, '/context.jsonl', 100, zeroblob(86016));",
    )
    .unwrap();
    let before = conn
        .query("SELECT * FROM conversations WHERE id = 1")
        .unwrap();
    storage.run_migrations().unwrap();
    storage.run_migrations().unwrap();
    assert_eq!(storage.schema_version().unwrap(), CURRENT_SCHEMA_VERSION);
    assert_eq!(
        conn.query("SELECT * FROM conversations WHERE id = 1")
            .unwrap(),
        before
    );
    let columns: Vec<String> = conn
        .query("PRAGMA index_info(idx_conversations_context)")
        .unwrap()
        .iter()
        .map(|row| row.get_typed(2).unwrap())
        .collect();
    assert_eq!(columns, ["started_at", "workspace_id", "agent_id"]);
    let recorded: i64 = conn
        .query_row_map(
            "SELECT COUNT(*) FROM _schema_migrations WHERE version = 21",
            &[],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(recorded, 1, "the additive migration must be idempotent");
}

#[test]
fn migration_v20_backfills_conversation_external_tail_lookup() {
    let storage = franken_storage_in_memory();
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
    let mut conv = make_profiled_storage_remote_conversation(1919, 2);
    conv.source_id = "profiled-storage-remote-source-東京".into();
    conv.external_id = Some("profiled-storage-remote-☃-1919".into());
    let outcome = storage
        .insert_conversation_tree(agent_id, Some(workspace_id), &conv)
        .unwrap();
    let external_id = conv.external_id.as_deref().unwrap();
    let lookup_key = conversation_external_lookup_key(&conv.source_id, agent_id, external_id);

    storage
        .raw()
        .execute("DELETE FROM conversation_external_tail_lookup")
        .unwrap();
    storage
        .raw()
        .execute("DELETE FROM _schema_migrations WHERE version >= 20")
        .unwrap();
    storage
        .raw()
        .execute_compat(
            "UPDATE meta SET value = ?1 WHERE key = 'schema_version'",
            fparams!["19"],
        )
        .unwrap();

    storage.run_migrations().unwrap();

    let backfilled: (i64, Option<i64>, Option<i64>, Option<i64>) = storage
        .raw()
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
    assert_eq!(
        backfilled,
        (
            outcome.conversation_id,
            conv.ended_at,
            Some(1),
            conv.messages[1].created_at
        )
    );
    assert_eq!(storage.schema_version().unwrap(), CURRENT_SCHEMA_VERSION);
}

#[test]
fn migration_v15_creates_lazy_tail_state_cache() {
    let conn = FrankenConnection::open(":memory:").unwrap();
    conn.execute_batch(
        "CREATE TABLE conversations (
                 id INTEGER PRIMARY KEY,
             ended_at INTEGER
         );
         CREATE TABLE messages (
             id INTEGER PRIMARY KEY,
             conversation_id INTEGER NOT NULL,
             idx INTEGER NOT NULL,
             created_at INTEGER
         );
         INSERT INTO conversations(id, ended_at) VALUES
             (1, 1710000000300),
             (2, NULL);
         INSERT INTO messages(id, conversation_id, idx, created_at) VALUES
             (10, 1, 0, 1710000000100),
             (11, 1, 1, 1710000000200),
             (12, 2, 0, 1710000000400);",
    )
    .unwrap();

    conn.execute(
        "CREATE TABLE _schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
         );",
    )
    .unwrap();

    assert!(
        apply_conversation_tail_state_cache_migration(&conn).unwrap(),
        "v15 migration should apply once"
    );
    assert!(
        !apply_conversation_tail_state_cache_migration(&conn).unwrap(),
        "v15 migration should be idempotent once recorded"
    );

    let columns = conn.query("PRAGMA table_info(conversations);").unwrap();
    let column_names: HashSet<String> = columns
        .iter()
        .map(|row| row.get_typed(1))
        .collect::<std::result::Result<_, crate::franken_sync::FrankenError>>()
        .unwrap();
    assert!(column_names.contains("last_message_idx"));
    assert!(column_names.contains("last_message_created_at"));

    let tail_rows: i64 = conn
        .query("SELECT COUNT(*) FROM conversation_tail_state;")
        .unwrap()
        .first()
        .unwrap()
        .get_typed(0)
        .unwrap();
    assert_eq!(
        tail_rows, 0,
        "v15 should create the cache without an open-time message scan"
    );

    let applied: i64 = conn
        .query("SELECT COUNT(*) FROM _schema_migrations WHERE version = 15;")
        .unwrap()
        .first()
        .unwrap()
        .get_typed(0)
        .unwrap();
    assert_eq!(applied, 1);
}

#[test]
fn schema_repair_adds_missing_conversations_token_columns() {
    let conn = FrankenConnection::open(":memory:").unwrap();
    conn.execute_batch(
        "CREATE TABLE conversations (
             id INTEGER PRIMARY KEY,
             agent_id INTEGER NOT NULL,
             source_path TEXT NOT NULL
         );",
    )
    .unwrap();
    let storage = FrankenStorage::new(conn, std::path::PathBuf::from(":memory:"));

    storage.repair_missing_conversation_token_columns().unwrap();
    storage.repair_missing_conversation_token_columns().unwrap();

    let columns = franken_table_column_names(&storage.conn, "conversations").unwrap();
    for &(column_name, _) in REQUIRED_CONVERSATION_TOKEN_COLUMNS {
        assert!(
            columns.contains(column_name),
            "schema repair should add conversations.{column_name}"
        );
    }
}

#[test]
fn franken_meta_schema_version_in_sync() {
    let storage = franken_storage_in_memory();

    // meta.schema_version should be kept in sync.
    let rows = storage
        .raw()
        .query("SELECT value FROM meta WHERE key = 'schema_version';")
        .unwrap();
    let meta_version: String = rows.first().unwrap().get_typed(0).unwrap();
    assert_eq!(
        meta_version,
        CURRENT_SCHEMA_VERSION.to_string(),
        "meta.schema_version should match CURRENT_SCHEMA_VERSION"
    );
}

#[test]
fn gh443_current_schema_open_preserves_rows_and_search_after_writes() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("migrated.db");
    let marker_path = path.with_file_name("migrated.db.fsqlite-migration-state");
    {
        let storage = FrankenStorage::open(&path).unwrap();
        storage
            .raw()
            .execute_batch(
                "CREATE TABLE startup_payload(id INTEGER PRIMARY KEY, content TEXT NOT NULL);\n\
             CREATE VIRTUAL TABLE startup_search USING fts5(content);\n\
             INSERT INTO startup_search(content) VALUES('prior searchable evidence');",
            )
            .unwrap();
        let payload = "prior archive payload ".repeat(16384);
        for id in 0..8_i64 {
            storage
                .raw()
                .execute_compat(
                    "INSERT INTO startup_payload(id,content) VALUES(?1,?2)",
                    fparams![id, payload.as_str()],
                )
                .unwrap();
        }
    }
    let marker = fs::read(&marker_path).unwrap();
    assert!(index_engine_migration_is_complete(&path));
    {
        let mut old =
            open_franken_raw_connection_with_timeout(&path, Duration::from_secs(10)).unwrap();
        {
            let metadata = old
                .prepare("SELECT value FROM meta WHERE key = ?1")
                .unwrap();
            let versions = metadata
                .query_with_params(&[SqliteValue::Text("schema_version".into())])
                .unwrap();
            assert_eq!(
                versions[0].get_typed::<String>(0).unwrap(),
                CURRENT_SCHEMA_VERSION.to_string()
            );
        }
        assert!(
            old.as_async().memdb_row_hydration_count() >= 8,
            "control must exercise the old constructor's unrelated row hydration"
        );
        old.close_without_checkpoint_in_place().unwrap();
    }
    let storage = open_current_schema_storage_with_timeout(&path, Duration::from_secs(10))
        .unwrap()
        .expect("current migrated archive must open");
    let metadata = storage
        .raw()
        .prepare("SELECT value FROM meta WHERE key = ?1")
        .unwrap();
    let versions = metadata
        .query_with_params(&[SqliteValue::Text("schema_version".into())])
        .unwrap();
    assert_eq!(
        versions[0].get_typed::<String>(0).unwrap(),
        CURRENT_SCHEMA_VERSION.to_string()
    );
    assert_eq!(
        storage.raw().as_async().memdb_row_hydration_count(),
        0,
        "metadata admission must not hydrate unrelated archive rows"
    );
    let rows = storage
        .raw()
        .query("SELECT id,content FROM startup_payload ORDER BY id")
        .unwrap();
    assert_eq!(rows.len(), 8);
    for (id, row) in rows.iter().enumerate() {
        assert_eq!(row.get_typed::<i64>(0).unwrap(), id as i64);
        assert_eq!(
            row.get_typed::<String>(1).unwrap(),
            "prior archive payload ".repeat(16384)
        );
    }
    storage
        .raw()
        .execute("INSERT INTO startup_search(content) VALUES('new searchable evidence')")
        .unwrap();
    let hits = storage.raw().query("SELECT content FROM startup_search WHERE startup_search MATCH 'searchable' ORDER BY rowid").unwrap();
    assert_eq!(
        hits.len(),
        2,
        "schema-only open must retain existing FTS and accept new writes"
    );
    assert_eq!(
        hits[0].get_typed::<String>(0).unwrap(),
        "prior searchable evidence"
    );
    assert_eq!(
        hits[1].get_typed::<String>(0).unwrap(),
        "new searchable evidence"
    );
    assert_eq!(fs::read(&marker_path).unwrap(), marker);
}

#[test]
fn xcqqa_archive_writers_do_not_hydrate_unrelated_rows_on_point_lookups() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("writers.db");
    let payload = "archived session payload ".repeat(16384);
    {
        let storage = FrankenStorage::open(&path).unwrap();
        storage
            .raw()
            .execute("CREATE TABLE bulk_payload(id INTEGER PRIMARY KEY, content TEXT NOT NULL)")
            .unwrap();
        for id in 0..8_i64 {
            storage
                .raw()
                .execute_compat(
                    "INSERT INTO bulk_payload(id,content) VALUES(?1,?2)",
                    fparams![id, payload.as_str()],
                )
                .unwrap();
        }
    }
    assert!(index_engine_migration_is_complete(&path));
    let schema_version_lookup = |conn: &FrankenConnection| -> String {
        conn.query_row_map(
            "SELECT value FROM meta WHERE key = ?1",
            fparams!["schema_version"],
            |row| row.get_typed(0),
        )
        .unwrap()
    };

    // Control: the constructor writers used before xcqqa hydrates every
    // row of the unrelated payload table for one metadata lookup.
    {
        let mut ordinary = FrankenConnection::open(path.to_string_lossy().to_string()).unwrap();
        assert_eq!(
            schema_version_lookup(&ordinary),
            CURRENT_SCHEMA_VERSION.to_string()
        );
        assert!(
            ordinary.as_async().memdb_row_hydration_count() >= 8,
            "control must exercise the ordinary constructor's whole-file hydration"
        );
        ordinary.close_without_checkpoint_in_place().unwrap();
    }

    // The legacy OMP analytics writer and the ingest writers.
    let writer = FrankenStorage::open_writer(&path).unwrap();
    assert_eq!(
        schema_version_lookup(writer.raw()),
        CURRENT_SCHEMA_VERSION.to_string()
    );
    assert_eq!(
        writer.raw().as_async().memdb_row_hydration_count(),
        0,
        "a writer's metadata lookup must not hydrate unrelated archive rows"
    );
    {
        let mut tx = writer.raw().transaction().unwrap();
        tx.execute_compat(
            "INSERT INTO bulk_payload(id,content) VALUES(?1,?2)",
            fparams![8_i64, "written through the bounded writer"],
        )
        .unwrap();
        tx.commit().unwrap();
    }
    drop(writer);

    let manager = FrankenConnectionManager::new(
        &path,
        ConnectionManagerConfig {
            reader_count: 1,
            max_writers: 2,
        },
    )
    .unwrap();
    for guard in [
        manager.writer().unwrap(),
        manager.concurrent_writer().unwrap(),
    ] {
        assert_eq!(
            schema_version_lookup(guard.storage().raw()),
            CURRENT_SCHEMA_VERSION.to_string()
        );
        assert_eq!(
            guard.storage().raw().as_async().memdb_row_hydration_count(),
            0,
            "a managed writer's metadata lookup must not hydrate unrelated archive rows"
        );
    }
    drop(manager);

    let reader = FrankenStorage::open(&path).unwrap();
    let rows = reader
        .raw()
        .query("SELECT id, length(content) FROM bulk_payload ORDER BY id")
        .unwrap();
    assert_eq!(rows.len(), 9);
    for row in &rows[..8] {
        assert_eq!(row.get_typed::<i64>(1).unwrap(), payload.len() as i64);
    }
    assert_eq!(rows[8].get_typed::<i64>(0).unwrap(), 8);
    drop(reader);

    // An archive whose engine migration is pending still gets the ordinary
    // constructor, which runs and records the engine's first-open repair;
    // the writer then continues in the bounded lane.
    let marker_path = path.with_file_name("writers.db.fsqlite-migration-state");
    fs::write(&marker_path, r#"{"last_upgrade_version":1}"#).unwrap();
    assert!(!index_engine_migration_is_complete(&path));
    let pending_writer = FrankenStorage::open_writer(&path).unwrap();
    assert!(
        index_engine_migration_is_complete(&path),
        "the ordinary constructor must finish and record the required repair"
    );
    assert_eq!(
        schema_version_lookup(pending_writer.raw()),
        CURRENT_SCHEMA_VERSION.to_string()
    );
    assert_eq!(
        pending_writer.raw().as_async().memdb_row_hydration_count(),
        0,
        "the writer that ran the repair must not keep the hydrating ordinary handle"
    );
    {
        let mut tx = pending_writer.raw().transaction().unwrap();
        tx.execute_compat(
            "INSERT INTO bulk_payload(id,content) VALUES(?1,?2)",
            fparams![9_i64, "written after the first-open repair"],
        )
        .unwrap();
        tx.commit().unwrap();
    }
    drop(pending_writer);
    let rows = FrankenStorage::open(&path)
        .unwrap()
        .raw()
        .query("SELECT id FROM bulk_payload ORDER BY id")
        .unwrap();
    assert_eq!(rows.len(), 10);
}

#[cfg(target_os = "linux")]
#[test]
fn xcqqa_token_rollup_stage_on_a_stale_writer_does_not_hydrate_archive_rows() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("stale-writer.db");
    let payload = "archived session payload ".repeat(16384);
    {
        let storage = FrankenStorage::open(&path).unwrap();
        storage
            .raw()
            .execute("CREATE TABLE bulk_payload(id INTEGER PRIMARY KEY, content TEXT NOT NULL)")
            .unwrap();
        // ~30 x 400 KB of overflow pages: a ~3,000-page file, so a probe
        // that walks the file is distinguishable from one that does not.
        for id in 0..30_i64 {
            storage
                .raw()
                .execute_compat(
                    "INSERT INTO bulk_payload(id,content) VALUES(?1,?2)",
                    fparams![id, payload.as_str()],
                )
                .unwrap();
        }
    }
    assert!(index_engine_migration_is_complete(&path));

    // The legacy OMP analytics writer commits thousands of Track A chunks
    // while the index run's own connection stays open, then runs the token
    // rollup stage (Track B) on the same writer.
    let peer = FrankenStorage::open(&path).unwrap();
    let writer = FrankenStorage::open_writer(&path).unwrap();
    for id in 30..34_i64 {
        let mut tx = writer.raw().transaction().unwrap();
        tx.execute_compat(
            "INSERT INTO bulk_payload(id,content) VALUES(?1,?2)",
            fparams![id, "writer chunk"],
        )
        .unwrap();
        tx.commit().unwrap();
    }
    peer.raw()
        .execute_compat(
            "INSERT INTO bulk_payload(id,content) VALUES(?1,?2)",
            fparams![34_i64, "peer commit"],
        )
        .unwrap();
    assert_eq!(writer.raw().as_async().memdb_row_hydration_count(), 0);
    // Bytes returned by read syscalls. fsqlite 0.4.4 reads every page of
    // the file (through pread, invisible to its page-cache counters) on a
    // connection's first sqlite_master query: 4,237,131 preads for the
    // 4,137,946-page owner archive. Counted per thread: the synchronous
    // wrapper drives the engine on this thread, and the process-wide
    // counter also collects every test running in parallel (this test
    // failed its bound in a 735-test storage run and passed alone).
    let read_bytes = || {
        fs::read_to_string("/proc/thread-self/io")
            .ok()
            .and_then(|io| {
                io.lines()
                    .find_map(|line| line.strip_prefix("rchar: "))
                    .and_then(|value| value.trim().parse::<u64>().ok())
            })
            .expect("/proc/thread-self/io rchar")
    };

    let before_probe = read_bytes();
    assert!(historical_table_exists(writer.raw(), "bulk_payload").unwrap());
    assert!(!historical_table_exists(writer.raw(), TOKEN_DAILY_STATS_REBUILD_STAGE_TABLE).unwrap());
    let probe_read = read_bytes() - before_probe;
    assert!(
        probe_read < 1024 * 1024,
        "two table-existence probes read {probe_read} bytes of a ~12 MB archive"
    );
    assert_eq!(
        writer.raw().as_async().memdb_row_hydration_count(),
        0,
        "a table-existence probe must not hydrate archive rows"
    );

    let before_stage = read_bytes();
    writer
        .rebuild_token_daily_stats_with_progress(None, None)
        .unwrap();
    let stage_read = read_bytes() - before_stage;
    assert!(
        stage_read < 4 * 1024 * 1024,
        "the token rollup stage read {stage_read} bytes of a ~12 MB archive"
    );
    assert_eq!(
        writer.raw().as_async().memdb_row_hydration_count(),
        0,
        "the token rollup stage must not hydrate archive rows"
    );
    drop(writer);
    drop(peer);
}

#[test]
fn gh443_current_schema_open_runs_required_engine_migration() {
    for marker in [
        None,
        Some("not json"),
        Some(r#"{"last_upgrade_version":1}"#),
        Some(r#"{"last_upgrade_version":0,"last_run_at":0,"repairs_applied":[]}"#),
    ] {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pending.db");
        let marker_path = path.with_file_name("pending.db.fsqlite-migration-state");
        drop(FrankenStorage::open(&path).unwrap());
        match marker {
            Some(bytes) => fs::write(&marker_path, bytes).unwrap(),
            None => {
                fs::rename(&marker_path, dir.path().join("retained-birth-marker.json")).unwrap()
            }
        }
        assert!(!index_engine_migration_is_complete(&path));
        let storage = open_current_schema_storage_with_timeout(&path, Duration::from_secs(10))
            .unwrap()
            .expect("required engine migration must still reach a usable archive");
        assert_eq!(storage.schema_version().unwrap(), CURRENT_SCHEMA_VERSION);
        assert!(
            index_engine_migration_is_complete(&path),
            "full constructor must finish and record required repair"
        );
        let version: String = storage
            .raw()
            .query_row_map(
                "SELECT value FROM meta WHERE key = ?1",
                fparams!["schema_version"],
                |row| row.get_typed(0),
            )
            .unwrap();
        assert_eq!(version, CURRENT_SCHEMA_VERSION.to_string());
        assert_eq!(
            storage.raw().as_async().memdb_row_hydration_count(),
            0,
            "after the repair the index handle must be schema-only (xcqqa)"
        );
    }
}

#[test]
fn gh443_current_schema_marker_cannot_admit_legacy_schema() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("legacy.db");
    {
        let storage = FrankenStorage::open(&path).unwrap();
        storage
            .raw()
            .execute("UPDATE meta SET value='13' WHERE key='schema_version'")
            .unwrap();
    }
    assert!(index_engine_migration_is_complete(&path));
    assert!(
        open_current_schema_storage_with_timeout(&path, Duration::from_secs(10))
            .unwrap()
            .is_none()
    );
}

#[test]
fn franken_transition_from_meta_version() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test_transition.db");

    // Simulate an existing database created by SqliteStorage at version 10.
    // We create just enough schema to test the transition.
    let conn = FrankenConnection::open(db_path.to_string_lossy().to_string()).unwrap();
    conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);")
        .unwrap();
    conn.execute("INSERT INTO meta(key, value) VALUES('schema_version', '10');")
        .unwrap();
    // Create a dummy conversations table so transition doesn't think it's corrupted.
    conn.execute("CREATE TABLE conversations (id INTEGER PRIMARY KEY);")
        .unwrap();
    drop(conn);

    // Now run the transition function.
    let conn = FrankenConnection::open(db_path.to_string_lossy().to_string()).unwrap();
    transition_from_meta_version(&conn).unwrap();

    // The frankensqlite path uses a combined V13 base migration, so a
    // legacy V10 marker is bridged to V13 and later idempotent repair fills
    // in any missing V11-V13 objects.
    let rows = conn
        .query("SELECT version FROM _schema_migrations ORDER BY version;")
        .unwrap();
    let versions: Vec<i64> = rows.iter().filter_map(|r| r.get_typed(0).ok()).collect();
    assert_eq!(
        versions,
        (1..=13).collect::<Vec<i64>>(),
        "transition should bridge legacy V10 databases through the combined V13 base marker"
    );
}

#[test]
fn franken_transition_from_current_meta_backfills_current_schema_marker() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test_current_transition.db");

    let conn = FrankenConnection::open(db_path.to_string_lossy().to_string()).unwrap();
    conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);")
        .unwrap();
    conn.execute_compat(
        "INSERT INTO meta(key, value) VALUES('schema_version', ?1);",
        &[ParamValue::from(CURRENT_SCHEMA_VERSION.to_string())],
    )
    .unwrap();
    conn.execute("CREATE TABLE conversations (id INTEGER PRIMARY KEY);")
        .unwrap();
    drop(conn);

    let conn = FrankenConnection::open(db_path.to_string_lossy().to_string()).unwrap();
    transition_from_meta_version(&conn).unwrap();

    let rows = conn
        .query("SELECT version FROM _schema_migrations ORDER BY version;")
        .unwrap();
    let versions: Vec<i64> = rows.iter().filter_map(|r| r.get_typed(0).ok()).collect();
    assert_eq!(
        versions,
        (1..=CURRENT_SCHEMA_VERSION).collect::<Vec<i64>>(),
        "current meta schema marker should backfill every known migration"
    );
}

#[test]
fn franken_transition_skips_when_already_done() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test_transition_skip.db");

    // Create a DB that already has _schema_migrations.
    let conn = FrankenConnection::open(db_path.to_string_lossy().to_string()).unwrap();
    conn.execute(
        "CREATE TABLE _schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TEXT NOT NULL DEFAULT 'now');",
    ).unwrap();
    conn.execute("INSERT INTO _schema_migrations (version, name) VALUES (1, 'test');")
        .unwrap();

    // Transition should be a no-op.
    transition_from_meta_version(&conn).unwrap();

    // Should still have exactly 1 entry.
    let rows = conn
        .query("SELECT COUNT(*) FROM _schema_migrations;")
        .unwrap();
    let count: i64 = rows.first().unwrap().get_typed(0).unwrap();
    assert_eq!(
        count, 1,
        "transition should not re-run on already-transitioned DB"
    );
}

#[test]
fn franken_transition_fresh_db_is_noop() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test_fresh_noop.db");

    // Empty database — no meta table, no tables at all.
    let conn = FrankenConnection::open(db_path.to_string_lossy().to_string()).unwrap();
    transition_from_meta_version(&conn).unwrap();

    // _schema_migrations should NOT have been created.
    let res = conn.query("SELECT * FROM \"_schema_migrations\";");
    assert!(
        res.is_err(),
        "transition should not create _schema_migrations on fresh DB"
    );
}

#[test]
fn franken_transition_with_fts_virtual_table_succeeds() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test_transition_with_fts.db");

    let conn = FrankenConnection::open(db_path.to_string_lossy().into_owned()).unwrap();
    // BEGIN/COMMIT makes the fixture batch atomic so a transient
    // fsqlite-0.3.x BusySnapshot abort (stale registry state after tempdir
    // inode reuse) can be retried wholesale without partial-apply debris.
    retry_transient_storage_op("legacy_v13_fixture_batch", || {
        conn.execute_batch(
            "BEGIN;
         CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
         INSERT INTO meta(key, value) VALUES('schema_version', '13');
         CREATE TABLE conversations (id INTEGER PRIMARY KEY);
         CREATE VIRTUAL TABLE fts_messages USING fts5(
             content,
             title,
             agent,
             workspace,
             source_path,
             created_at,
             content='',
             tokenize='porter unicode61'
         );
         COMMIT;",
        )
        .map_err(anyhow::Error::new)
    })
    .unwrap();
    drop(conn);

    let conn = FrankenConnection::open(db_path.to_string_lossy().to_string()).unwrap();
    transition_from_meta_version(&conn).unwrap();

    let rows = conn
        .query("SELECT version FROM _schema_migrations ORDER BY version;")
        .unwrap();
    let versions: Vec<i64> = rows.iter().filter_map(|r| r.get_typed(0).ok()).collect();
    assert_eq!(versions, (1..=13).collect::<Vec<i64>>());
}

#[test]
fn franken_storage_open_legacy_v13_with_fts_virtual_table_succeeds() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test_open_legacy_v13_with_fts.db");

    let conn = FrankenConnection::open(db_path.to_string_lossy().into_owned()).unwrap();
    // BEGIN/COMMIT makes the fixture batch atomic so a transient
    // fsqlite-0.3.x BusySnapshot abort (stale registry state after tempdir
    // inode reuse) can be retried wholesale without partial-apply debris.
    retry_transient_storage_op("legacy_v13_open_fixture_batch", || {
            conn.execute_batch(
                "BEGIN;
             CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
             INSERT INTO meta(key, value) VALUES('schema_version', '13');
             CREATE TABLE agents (
                 id INTEGER PRIMARY KEY,
                 slug TEXT NOT NULL
             );
             CREATE TABLE workspaces (
                 id INTEGER PRIMARY KEY,
                 path TEXT NOT NULL
             );
             CREATE TABLE sources (
                 id TEXT PRIMARY KEY,
                 kind TEXT NOT NULL,
                 host_label TEXT,
                 machine_id TEXT,
                 platform TEXT,
                 config_json TEXT,
                 created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL
             );
             CREATE TABLE conversations (
                 id INTEGER PRIMARY KEY,
                 agent_id INTEGER NOT NULL,
                 workspace_id INTEGER,
                 source_id TEXT NOT NULL DEFAULT 'local',
                 external_id TEXT,
                 title TEXT,
                 source_path TEXT NOT NULL,
                 started_at INTEGER,
                 ended_at INTEGER
             );
             CREATE TABLE messages (
                 id INTEGER PRIMARY KEY,
                 conversation_id INTEGER NOT NULL,
                 idx INTEGER NOT NULL,
                 role TEXT NOT NULL,
                 author TEXT,
                 created_at INTEGER,
                 content TEXT NOT NULL,
                 extra_json TEXT,
                 extra_bin BLOB
             );
             INSERT INTO agents(id, slug) VALUES (1, 'codex');
             INSERT INTO workspaces(id, path) VALUES (1, '/data/projects/coding_agent_session_search');
             INSERT INTO sources(id, kind, host_label, created_at, updated_at)
             VALUES ('local', 'local', NULL, 1710000000000, 1710000000000);
             INSERT INTO conversations(
                 id,
                 agent_id,
                 workspace_id,
                 source_id,
                 external_id,
                 title,
                 source_path,
             started_at
         )
         VALUES (
             1,
             1,
             1,
             'local',
             'legacy-session',
             'legacy session',
             '/tmp/legacy.jsonl',
             1710000000000
         );
         INSERT INTO messages(id, conversation_id, idx, role, author, created_at, content)
         VALUES (1, 1, 0, 'user', 'tester', 1710000000000, 'legacy content');
         CREATE VIRTUAL TABLE fts_messages USING fts5(
             content,
             title,
             agent,
             workspace,
             source_path,
             created_at,
             message_id,
             content='',
             tokenize='porter unicode61'
         );
         COMMIT;",
        )
        .map_err(anyhow::Error::new)
    })
    .unwrap();
    drop(conn);

    let storage = FrankenStorage::open(&db_path).unwrap();
    assert_eq!(storage.schema_version().unwrap(), CURRENT_SCHEMA_VERSION);

    let rows = storage
        .raw()
        .query("SELECT version FROM _schema_migrations ORDER BY version;")
        .unwrap();
    let versions: Vec<i64> = rows.iter().filter_map(|r| r.get_typed(0).ok()).collect();
    assert_eq!(versions, (1..=CURRENT_SCHEMA_VERSION).collect::<Vec<i64>>());
}

#[test]
fn franken_storage_open_repairs_duplicate_fts_messages_schema_rows() {
    if !sqlite3_cli_available_or_skip(
        "franken_storage_open_repairs_duplicate_fts_messages_schema_rows",
    ) {
        return;
    }
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test_open_repairs_duplicate_fts_schema.db");

    let storage = FrankenStorage::open(&db_path).unwrap();
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
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: Some("dup-fts-schema".into()),
        title: Some("Duplicate FTS schema".into()),
        source_path: PathBuf::from("/tmp/dup-fts-schema.jsonl"),
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
            content: "message that should remain queryable".into(),
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

    let duplicate_legacy_fts_sql = "CREATE VIRTUAL TABLE fts_messages USING fts5(content, title, agent, workspace, source_path, created_at UNINDEXED, message_id UNINDEXED, tokenize='porter')";
    let conn = rusqlite_test_fixture_conn(&db_path);
    conn.execute_batch("PRAGMA writable_schema = ON;").unwrap();
    conn.execute(
        "INSERT INTO sqlite_master(type, name, tbl_name, rootpage, sql)
         VALUES('table', 'fts_messages', 'fts_messages', 0, ?1)",
        [duplicate_legacy_fts_sql],
    )
    .unwrap();
    conn.execute(
        "DELETE FROM meta WHERE key = ?1",
        [FTS_FRANKEN_REBUILD_META_KEY],
    )
    .unwrap();
    // Simulate a pre-fix upgraded database that has never gone through the
    // authoritative frankensqlite FTS rebuild generation yet.
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

    let mut owner_reader =
        open_franken_async_readonly_connection_with_timeout(&db_path, Duration::from_secs(5))
            .expect("dedicated-owner readonly open should repair duplicate FTS schema rows");
    let message_rows = owner_reader
        .query_sync("SELECT COUNT(*) FROM messages")
        .expect("canonical messages must remain readable after schema-row dedupe");
    assert_eq!(
        message_rows[0].get_typed::<i64>(0).unwrap(),
        1,
        "dedupe must preserve canonical archive rows"
    );
    owner_reader
        .close_without_checkpoint_sync()
        .expect("dedicated-owner duplicate-schema probe should close cleanly");

    let reopened = FrankenStorage::open(&db_path).unwrap();
    assert_eq!(reopened.schema_version().unwrap(), CURRENT_SCHEMA_VERSION);
    let generation_rows: Vec<String> = reopened
        .raw()
        .query_map_collect(
            "SELECT value FROM meta WHERE key = ?1",
            fparams![FTS_FRANKEN_REBUILD_META_KEY],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(
        generation_rows.len(),
        0,
        "canonical open should not eagerly rewrite FTS repair metadata"
    );
    reopened.ensure_search_fallback_fts_consistency().unwrap();
    let repaired = FrankenConnection::open(db_path.to_string_lossy().into_owned()).unwrap();
    assert_eq!(franken_fts_schema_rows(&repaired).unwrap(), 1);

    let total_messages: i64 = reopened
        .raw()
        .query_row_map("SELECT COUNT(*) FROM messages", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    let total_fts_rows: i64 = reopened
        .raw()
        .query_row_map("SELECT COUNT(*) FROM fts_messages", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(total_fts_rows, total_messages);
}

#[test]
fn fts_messages_integrity_reports_missing_shadow_tables() {
    let direct_missing_shadow = fts_messages_integrity_error_from_message(
        "inserting 10000 rows into fallback FTS: table not found: fts_messages_data",
    )
    .expect("a missing required shadow table must be classified as derived FTS damage");
    assert_eq!(
        direct_missing_shadow.missing_shadow_tables(),
        &["fts_messages_data"]
    );

    let dir = TempDir::new().unwrap();
    let healthy_db_path = dir.path().join("healthy_fts.db");

    {
        let storage = FrankenStorage::open(&healthy_db_path).unwrap();
        storage.ensure_search_fallback_fts_consistency().unwrap();
        storage
            .validate_fts_messages_integrity()
            .expect("freshly materialized fts_messages should pass integrity validation");
    }

    let corrupt_db_path = dir.path().join("test_corrupt_fts_missing_shadows.db");
    {
        let conn = rusqlite_test_fixture_conn(&corrupt_db_path);
        conn.execute("CREATE TABLE schema_anchor(id INTEGER PRIMARY KEY)", [])
            .unwrap();
        let orphaned_fts_sql = "CREATE VIRTUAL TABLE fts_messages USING fts5(content, title, agent, workspace, source_path, created_at UNINDEXED, message_id UNINDEXED, tokenize='porter')";
        conn.execute_batch("PRAGMA writable_schema = ON;").unwrap();
        conn.execute(
            "INSERT INTO sqlite_master(type, name, tbl_name, rootpage, sql)
             VALUES('table', 'fts_messages', 'fts_messages', 0, ?1)",
            [orphaned_fts_sql],
        )
        .unwrap();
        conn.execute_batch("PRAGMA writable_schema = OFF;").unwrap();
    }

    let open_err = FrankenConnection::open(corrupt_db_path.to_string_lossy().to_string())
        .expect_err("orphaned fts_messages schema should fail during connection open");
    let integrity = fts_messages_integrity_error_from_message(open_err.to_string())
        .expect("open-time FTS corruption should map to the typed FTS integrity kind");
    assert_eq!(integrity.missing_shadow_tables(), &["fts_messages_content"]);
    let rendered = integrity.to_string();
    assert!(
        rendered.contains("fts_messages")
            && rendered.contains("required FTS5 shadow tables")
            && rendered.contains("fts_messages_content"),
        "error should be an operator-facing FTS corruption diagnosis: {rendered}"
    );
}

#[test]
fn franken_storage_open_fresh_db_keeps_single_franken_fts_schema_row() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("fresh-franken-storage-open.db");

    let storage = FrankenStorage::open(&db_path).unwrap();
    assert_eq!(storage.schema_version().unwrap(), CURRENT_SCHEMA_VERSION);

    // The FTS5 virtual table is no longer created eagerly by the
    // migration runner (V14 drops the old internal-content table and the
    // current contentless table is recreated lazily — see MIGRATION_V14).
    // Invoke the repair path to match normal cass startup, then assert
    // there is exactly one fts_messages entry in sqlite_schema (no
    // duplicates).
    storage
        .ensure_search_fallback_fts_consistency()
        .expect("ensure FTS consistency after fresh open");
    drop(storage);

    let c_reader = FrankenConnection::open(db_path.to_string_lossy().into_owned())
        .expect("open DB via frankensqlite for sqlite_master inspection");
    assert_eq!(
        franken_fts_schema_rows(&c_reader).unwrap(),
        1,
        "exactly one fts_messages schema row should exist after ensure_search_fallback_fts_consistency"
    );
    drop(c_reader);

    let storage = FrankenStorage::open(&db_path).unwrap();
    assert!(
        storage
            .raw()
            .query("SELECT COUNT(*) FROM fts_messages")
            .is_ok(),
        "fts_messages must be queryable through frankensqlite after open"
    );
}

#[test]
fn franken_storage_open_repairs_missing_analytics_tables_when_version_markers_lie() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test_repair_missing_analytics.db");

    {
        let storage = FrankenStorage::open(&db_path).unwrap();
        assert_eq!(storage.schema_version().unwrap(), CURRENT_SCHEMA_VERSION);
    }

    {
        let conn = FrankenConnection::open(db_path.to_string_lossy().into_owned()).unwrap();
        for table in &[
            "usage_models_daily",
            "usage_daily",
            "usage_hourly",
            "message_metrics",
            "token_daily_stats",
            "token_usage",
            "model_pricing",
            "embedding_jobs",
            "daily_stats",
        ] {
            conn.execute(&format!("DROP TABLE IF EXISTS {table}"))
                .unwrap();
        }
        conn.execute_compat(
            "UPDATE meta SET value = ?1 WHERE key = 'schema_version'",
            &[ParamValue::from(CURRENT_SCHEMA_VERSION.to_string())],
        )
        .unwrap();
    }

    let repaired = FrankenStorage::open(&db_path).unwrap();
    assert_eq!(repaired.schema_version().unwrap(), CURRENT_SCHEMA_VERSION);

    let analytics_count: i64 = repaired
        .raw()
        .query_row_map(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type='table'
               AND name IN (
                 'daily_stats',
                 'embedding_jobs',
                 'token_usage',
                 'token_daily_stats',
                 'model_pricing',
                 'message_metrics',
                 'usage_hourly',
                 'usage_daily',
                 'usage_models_daily'
               )",
            &[],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(
        analytics_count, 9,
        "open() should recreate the missing analytics tables even when schema_version already says current"
    );
}

#[test]
fn current_schema_repair_batches_cover_every_required_probe() {
    let missing_tables: Vec<&'static str> = REQUIRED_CURRENT_SCHEMA_TABLE_PROBES
        .iter()
        .map(|(table_name, _)| *table_name)
        .collect();

    let batches = current_schema_repair_batches_for_missing_tables(&missing_tables).unwrap();
    let covered_tables: HashSet<&'static str> = batches
        .iter()
        .flat_map(|batch| batch.tables.iter().copied())
        .collect();

    for table_name in missing_tables {
        assert!(
            covered_tables.contains(table_name),
            "missing repair coverage for {table_name}"
        );
    }
}

#[test]
fn current_schema_repair_batches_do_not_replay_core_schema_bootstrap() {
    for batch in CURRENT_SCHEMA_REPAIR_BATCHES {
        assert!(
            !batch.sql.contains("CREATE TABLE IF NOT EXISTS meta"),
            "repair batch {} should not recreate meta",
            batch.name
        );
        assert!(
            !batch.sql.contains("CREATE TABLE IF NOT EXISTS agents"),
            "repair batch {} should not recreate agents",
            batch.name
        );
        assert!(
            !batch.sql.contains("CREATE TABLE IF NOT EXISTS workspaces"),
            "repair batch {} should not recreate workspaces",
            batch.name
        );
        assert!(
            !batch
                .sql
                .contains("CREATE TABLE IF NOT EXISTS conversations"),
            "repair batch {} should not recreate conversations",
            batch.name
        );
        assert!(
            !batch.sql.contains("CREATE TABLE IF NOT EXISTS messages"),
            "repair batch {} should not recreate messages",
            batch.name
        );
        assert!(
            !batch.sql.contains("CREATE TABLE IF NOT EXISTS snippets"),
            "repair batch {} should not recreate snippets",
            batch.name
        );
        assert!(
            !batch.sql.contains("CREATE VIRTUAL TABLE fts_messages"),
            "repair batch {} should not recreate FTS tables",
            batch.name
        );
        assert!(
            !batch.sql.contains("DROP TABLE"),
            "repair batch {} should never drop tables",
            batch.name
        );
    }
}

#[test]
fn build_cass_migrations_applies_combined_v13() {
    let conn = FrankenConnection::open(":memory:").unwrap();
    let (mut applied, was_fresh) =
        run_attributed_migration_steps(&conn, BASE_MIGRATION_STEPS).unwrap();
    assert!(apply_conversation_tail_state_cache_migration(&conn).unwrap());
    let (post_applied, _) =
        run_attributed_migration_steps(&conn, POST_TAIL_CACHE_MIGRATION_STEPS).unwrap();

    assert!(was_fresh);
    applied.push(15);
    applied.extend(post_applied);
    assert_eq!(
        applied,
        (13..=CURRENT_SCHEMA_VERSION).collect::<Vec<i64>>(),
        "should apply combined V13 plus additive post-V13 migrations"
    );
    let current: i64 = conn
        .query("SELECT MAX(version) FROM _schema_migrations;")
        .unwrap()
        .first()
        .unwrap()
        .get_typed(0)
        .unwrap();
    assert_eq!(current, CURRENT_SCHEMA_VERSION);
}
