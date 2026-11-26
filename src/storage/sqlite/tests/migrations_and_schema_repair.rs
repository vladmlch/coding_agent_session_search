use super::*;
use crate::franken_sync::{
    Connection as FrankenConnection, SqliteValue,
    compat::RowExt as FrankenRowExt,
};
use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;

// =========================================================================
// FrankenStorage migration tests (bead 2j6p6)
// =========================================================================

/// Helper: create a FrankenStorage wrapping an in-memory connection and
/// run migrations. This exercises the same schema-v1 bootstrap path as
/// `open()` on an in-memory SQLite database.
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
    assert!(
        table_names.contains(&"forgotten_sources".to_string()),
        "missing forgotten_sources table"
    );
    assert!(
        table_names.contains(&"forgotten_conversations".to_string()),
        "missing forgotten_conversations table"
    );
    assert!(
        table_names.contains(&"fts_messages".to_string()),
        "missing fts_messages table"
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

#[test]
fn franken_table_column_names_reads_schema_v1_conversations_columns() {
    let storage = franken_storage_in_memory();
    let columns = franken_table_column_names(&storage.conn, "conversations").unwrap();
    for column_name in [
        "id",
        "agent_id",
        "source_path",
        "total_input_tokens",
        "total_output_tokens",
        "grand_total_tokens",
        "last_message_idx",
        "last_message_created_at",
    ] {
        assert!(
            columns.contains(column_name),
            "schema v1 should include conversations.{column_name}"
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
    assert!(index_engine_migration_is_complete(&path));
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
        let orphaned_fts_sql = "CREATE VIRTUAL TABLE fts_messages USING fts5(content, title, content='', contentless_delete=1, tokenize='porter unicode61')";
        conn.execute_batch("PRAGMA writable_schema = ON;").unwrap();
        conn.execute(
            "INSERT INTO sqlite_master(type, name, tbl_name, rootpage, sql)
             VALUES('table', 'fts_messages', 'fts_messages', 0, ?1)",
            [orphaned_fts_sql],
        )
        .unwrap();
        conn.execute_batch("PRAGMA writable_schema = OFF;").unwrap();
    }

    let conn = FrankenConnection::open(corrupt_db_path.to_string_lossy().to_string()).unwrap();
    let integrity = validate_fts_messages_integrity_for_connection(&conn)
        .expect_err("orphaned fts_messages schema should fail integrity validation")
        .downcast::<FtsMessagesIntegrityError>()
        .expect("missing shadow tables should map to FtsMessagesIntegrityError");
    assert_eq!(
        integrity.missing_shadow_tables(),
        &[
            "fts_messages_config",
            "fts_messages_data",
            "fts_messages_docsize",
            "fts_messages_idx",
        ]
    );
    let rendered = integrity.to_string();
    assert!(
        rendered.contains("fts_messages")
            && rendered.contains("required FTS5 shadow tables")
            && rendered.contains("fts_messages_data"),
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
        .expect("open DB via SQLite for sqlite_master inspection");
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
        "fts_messages must be queryable through SQLite after open"
    );
}
