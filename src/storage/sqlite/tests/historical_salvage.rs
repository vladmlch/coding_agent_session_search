use super::*;
use crate::franken_sync::{
    Connection as FrankenConnection,
    compat::{ConnectionExt as FrankenConnectionExt, RowExt as FrankenRowExt},
};
use crate::sources::provenance::LOCAL_SOURCE_ID;
use serial_test::serial;
use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;

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
    let conn = FrankenConnection::open(smaller_healthy.to_string_lossy()).unwrap();
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
    // Schema v1 creates the contentless fts_messages virtual table at birth,
    // so a clean backup has 1 fts_messages row and is immediately queryable.
    assert_eq!(bundles[0].probe.fts_schema_rows, Some(1));
    assert!(bundles[0].probe.fts_queryable);
    assert!(!bundles[1].probe.fts_queryable);
    if cfg!(windows) {
        assert_eq!(bundles[1].probe.schema_version, Some(13));
        assert_eq!(bundles[1].probe.fts_schema_rows, Some(1));
    } else {
        assert!(
            matches!(bundles[1].probe.schema_version, None | Some(13)),
            "unexpected replay schema_version: {:?}",
            bundles[1].probe.schema_version
        );
        assert!(
            matches!(bundles[1].probe.fts_schema_rows, None | Some(2)),
            "unexpected replay fts_schema_rows: {:?}",
            bundles[1].probe.fts_schema_rows
        );
    }
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

    let conn = FrankenConnection::open(db_path.to_string_lossy()).unwrap();
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

    let conn = FrankenConnection::open(db_path.to_string_lossy()).unwrap();
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
        storage.rebuild_fts().unwrap();
    }

    // Corrupt the %_data structure record (rowid 10) via a fresh connection
    // (the FTS is still valid here, so the open succeeds).
    {
        let conn = FrankenConnection::open(db_path.to_string_lossy()).unwrap();
        let garbage = [0xFFu8; 12];
        conn.execute_compat(
            "UPDATE fts_messages_data SET block = ?1 WHERE id = ?2",
            fparams![garbage.as_slice(), 10_i64],
        )
        .unwrap();
    }

    // Querying the corrupt %_data structure record fails under SQLite C.
    let corrupt_open = SqliteStorage::open(&db_path).unwrap();
    assert!(
        corrupt_open
            .raw()
            .query("SELECT COUNT(*) FROM fts_messages WHERE fts_messages MATCH 'needle'")
            .is_err(),
        "querying a corrupt FTS5 %_data structure record must fail"
    );
    drop(corrupt_open);

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
    let conn = FrankenConnection::open(db_path.to_string_lossy()).unwrap();
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
fn rebuild_fts_via_rusqlite_repopulates_empty_fts_shadow() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("fts-rebuild.db");

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

    let conn = FrankenConnection::open(db_path.to_string_lossy()).unwrap();
    conn.execute("DROP TABLE IF EXISTS fts_messages").unwrap();
    drop(conn);

    let inserted = rebuild_fts_via_rusqlite(&db_path).unwrap();
    assert_eq!(inserted, 1);

    let conn = FrankenConnection::open(db_path.to_string_lossy()).unwrap();
    let schema_rows = franken_fts_schema_rows(&conn).unwrap();
    assert_eq!(schema_rows, 1, "rebuild should leave one clean FTS schema");
    let match_count: i64 = conn
        .query_row_map(
            "SELECT COUNT(*) FROM fts_messages WHERE fts_messages MATCH 'retro'",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(match_count, 1);
}
