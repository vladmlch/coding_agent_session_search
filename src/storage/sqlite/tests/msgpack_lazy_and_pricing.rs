use super::*;
use crate::franken_sync::compat::{ConnectionExt as FrankenConnectionExt, RowExt as FrankenRowExt};
use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use crate::sources::provenance::LOCAL_SOURCE_ID;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

// =========================================================================
// now_millis utility test (bead yln.4)
// =========================================================================

#[test]
fn now_millis_returns_reasonable_value() {
    let ts = SqliteStorage::now_millis();
    // Should be after Jan 1, 2020 (approx 1577836800000)
    assert!(ts > 1577836800000);
    // Should be before Jan 1, 2100 (approx 4102444800000)
    assert!(ts < 4102444800000);
}

// =========================================================================
// Binary Metadata Serialization Tests (Opt 3.1)
// =========================================================================

#[test]
fn msgpack_roundtrip_basic_object() {
    let value = serde_json::json!({
        "key": "value",
        "number": 42,
        "nested": { "inner": true }
    });

    let bytes = serialize_json_to_msgpack(&value).expect("should serialize");
    let recovered = deserialize_msgpack_to_json(&bytes);

    assert_eq!(value, recovered);
}

#[test]
fn msgpack_returns_none_for_null() {
    let value = serde_json::Value::Null;
    assert!(serialize_json_to_msgpack(&value).is_none());
}

#[test]
fn message_insert_stores_null_extra_json_as_sql_null() {
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
    let conversation = Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: None,
        external_id: Some("null-extra-json".into()),
        title: Some("Null extra_json".into()),
        source_path: PathBuf::from("/tmp/null-extra-json.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_001),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: None,
            created_at: Some(1_700_000_000_000),
            content: "null metadata message".into(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };

    let conversation_id = storage
        .insert_conversation_tree(agent_id, None, &conversation)
        .unwrap()
        .conversation_id;

    let (extra_json, extra_bin): (Option<String>, Option<Vec<u8>>) = storage
        .conn
        .query_row_map(
            "SELECT extra_json, extra_bin FROM messages WHERE conversation_id = ?1",
            fparams![conversation_id],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
        )
        .unwrap();
    assert!(extra_json.is_none());
    assert!(extra_bin.is_none());

    let stored = storage.fetch_messages(conversation_id).unwrap();
    assert!(stored[0].extra_json.is_null());
}

#[test]
fn message_insert_stores_nonempty_extra_json_as_msgpack_only() {
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
    let extra_json = serde_json::json!({ "idx": 7, "kind": "profile" });
    let conversation = Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: None,
        external_id: Some("msgpack-extra-json".into()),
        title: Some("MessagePack extra_json".into()),
        source_path: PathBuf::from("/tmp/msgpack-extra-json.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_001),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: None,
            created_at: Some(1_700_000_000_000),
            content: "msgpack metadata message".into(),
            extra_json: extra_json.clone(),
            snippets: Vec::new(),
        }],
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };

    let conversation_id = storage
        .insert_conversation_tree(agent_id, None, &conversation)
        .unwrap()
        .conversation_id;

    let (extra_json_text, extra_bin): (Option<String>, Option<Vec<u8>>) = storage
        .conn
        .query_row_map(
            "SELECT extra_json, extra_bin FROM messages WHERE conversation_id = ?1",
            fparams![conversation_id],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
        )
        .unwrap();
    assert!(extra_json_text.is_none());
    assert!(extra_bin.is_some());

    let stored = storage.fetch_messages(conversation_id).unwrap();
    assert_eq!(stored[0].extra_json, extra_json);
}

#[test]
fn conversation_insert_preserves_null_metadata_json_as_json_null() {
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
    let conversation = Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: None,
        external_id: Some("null-conversation-metadata".into()),
        title: Some("Null conversation metadata".into()),
        source_path: PathBuf::from("/tmp/null-conversation-metadata.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_001),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: None,
            created_at: Some(1_700_000_000_000),
            content: "null conversation metadata message".into(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };

    storage
        .insert_conversation_tree(agent_id, None, &conversation)
        .unwrap();

    let (metadata_json, metadata_bin): (Option<String>, Option<Vec<u8>>) = storage
        .conn
        .query_row_map(
            "SELECT metadata_json, metadata_bin FROM conversations WHERE external_id = ?1",
            fparams!["null-conversation-metadata"],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
        )
        .unwrap();
    assert_eq!(metadata_json.as_deref(), Some("null"));
    assert!(metadata_bin.is_none());

    let listed = storage.list_conversations(10, 0).unwrap();
    assert!(listed[0].metadata_json.is_null());
}

#[test]
fn conversation_insert_stores_nonempty_metadata_as_msgpack_only() {
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
    let metadata_json = serde_json::json!({ "bench": true, "source": "profile" });
    let conversation = Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: None,
        external_id: Some("msgpack-conversation-metadata".into()),
        title: Some("MessagePack conversation metadata".into()),
        source_path: PathBuf::from("/tmp/msgpack-conversation-metadata.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_001),
        approx_tokens: None,
        metadata_json: metadata_json.clone(),
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: None,
            created_at: Some(1_700_000_000_000),
            content: "msgpack conversation metadata message".into(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };

    storage
        .insert_conversation_tree(agent_id, None, &conversation)
        .unwrap();

    let (metadata_text, metadata_bin): (Option<String>, Option<Vec<u8>>) = storage
        .conn
        .query_row_map(
            "SELECT metadata_json, metadata_bin FROM conversations WHERE external_id = ?1",
            fparams!["msgpack-conversation-metadata"],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
        )
        .unwrap();
    assert!(metadata_text.is_none());
    assert!(metadata_bin.is_some());

    let listed = storage.list_conversations(10, 0).unwrap();
    assert_eq!(listed[0].metadata_json, metadata_json);
}

#[test]
fn msgpack_returns_none_for_empty_object() {
    let value = serde_json::json!({});
    assert!(serialize_json_to_msgpack(&value).is_none());
}

#[test]
fn parse_historical_json_column_preserves_large_payloads_as_raw_json() {
    let raw = format!("{{\"blob\":\"{}\"}}", "x".repeat(1_000_000));

    let value = parse_historical_json_column(Some(raw.clone()));

    assert_eq!(historical_raw_json(&value), Some(raw.as_str()));
    assert_eq!(json_value_size_hint(&value), raw.len());
}

#[test]
fn parse_historical_json_column_preserves_small_payloads_as_raw_json() {
    let raw = String::from("{\"ok\":true,\"n\":1}");

    let value = parse_historical_json_column(Some(raw.clone()));

    assert_eq!(historical_raw_json(&value), Some(raw.as_str()));
}

#[test]
fn message_extra_to_source_jsonl_line_round_trips_all_envelope_shapes() {
    // #285: the salvage path that rebuilds source JSONL from the canonical
    // archive's preserved per-message envelopes.

    // 1. Historical raw-line wrapper → verbatim line, byte-for-byte.
    let verbatim = r#"{"type":"user","uuid":"u1","text":"hi","ws":  "kept"}"#;
    let wrapped = wrap_historical_raw_json(verbatim.to_string());
    assert_eq!(
        message_extra_to_source_jsonl_line(&wrapped).as_deref(),
        Some(verbatim)
    );

    // 2. A decoded event object (the shape after msgpack/JSON decode) →
    //    compact JSON re-serialization.
    let event = serde_json::json!({"type": "assistant", "uuid": "a1"});
    let line = message_extra_to_source_jsonl_line(&event).expect("line");
    let reparsed: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(reparsed, event);
    assert!(!line.contains('\n'));

    // 3. A null envelope (no preserved event) → skipped.
    assert_eq!(
        message_extra_to_source_jsonl_line(&serde_json::Value::Null),
        None
    );
}

#[test]
fn reconstruct_source_jsonl_for_conversation_orders_by_idx() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("agent_search.db");
    let storage = FrankenStorage::open(&db_path).unwrap();

    // Seed one agent + one conversation with two raw-line messages.
    storage
        .conn
        .execute_compat(
            "INSERT INTO agents(slug, name, version, kind, created_at, updated_at) \
             VALUES('claude', 'Claude Code', NULL, 'cli', 1000, 1000)",
            fparams![],
        )
        .unwrap();
    let agent_id: i64 = storage
        .conn
        .query_row_map("SELECT last_insert_rowid()", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    storage
        .conn
        .execute_compat(
            "INSERT INTO conversations(agent_id, external_id, title, source_path, started_at) \
             VALUES(?1, 'sess-1', 't', '/orig/a.jsonl', 1000)",
            fparams![agent_id],
        )
        .unwrap();
    let cid: i64 = storage
        .conn
        .query_row_map("SELECT last_insert_rowid()", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    // Insert out of idx order to prove ORDER BY idx in fetch_messages.
    for (idx, line) in [(1_i64, r#"{"u":"a1"}"#), (0_i64, r#"{"u":"u1"}"#)] {
        let wrapped = serde_json::to_string(&wrap_historical_raw_json(line.to_string())).unwrap();
        storage
            .conn
            .execute_compat(
                "INSERT INTO messages(conversation_id, idx, role, author, created_at, content, extra_json, extra_bin) \
                 VALUES(?1, ?2, 'user', NULL, ?3, 'c', ?4, NULL)",
                fparams![cid, idx, 1000_i64 + idx, wrapped],
            )
            .unwrap();
    }

    let lines = storage
        .reconstruct_source_jsonl_for_conversation(cid)
        .unwrap();
    assert_eq!(
        lines,
        vec![r#"{"u":"u1"}"#.to_string(), r#"{"u":"a1"}"#.to_string()]
    );
}

#[test]
fn msgpack_serializes_non_empty_array() {
    let value = serde_json::json!([1, 2, 3]);
    let bytes = serialize_json_to_msgpack(&value).expect("should serialize array");
    let recovered = deserialize_msgpack_to_json(&bytes);
    assert_eq!(value, recovered);
}

#[test]
fn msgpack_smaller_than_json() {
    let value = serde_json::json!({
        "field_name_one": "some_value",
        "field_name_two": 123456,
        "field_name_three": [1, 2, 3, 4, 5],
        "field_name_four": { "nested": true }
    });

    let json_bytes = serde_json::to_vec(&value).unwrap();
    let msgpack_bytes = serialize_json_to_msgpack(&value).unwrap();

    // MessagePack should be smaller due to more compact encoding
    assert!(
        msgpack_bytes.len() < json_bytes.len(),
        "MessagePack ({} bytes) should be smaller than JSON ({} bytes)",
        msgpack_bytes.len(),
        json_bytes.len()
    );
}

#[test]
fn migration_v7_adds_binary_columns() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    // Verify metadata_bin column exists
    let has_metadata_bin = storage
        .raw()
        .query("PRAGMA table_info(conversations)")
        .unwrap()
        .iter()
        .any(|row| row.get_typed::<String>(1).unwrap() == "metadata_bin");
    assert!(
        has_metadata_bin,
        "conversations should have metadata_bin column"
    );

    // Verify extra_bin column exists
    let has_extra_bin = storage
        .raw()
        .query("PRAGMA table_info(messages)")
        .unwrap()
        .iter()
        .any(|row| row.get_typed::<String>(1).unwrap() == "extra_bin");
    assert!(has_extra_bin, "messages should have extra_bin column");
}

#[test]
fn insert_conversation_tree_rehydrates_append_tail_state_cache_after_manual_clear() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("append-tail-state-cache.db");
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

    let initial = make_profiled_append_remote_merge_conversation(11, 5);
    let insert_outcome = storage
        .insert_conversation_tree(agent_id, Some(workspace_id), &initial)
        .unwrap();
    let conversation_id = insert_outcome.conversation_id;

    let initial_tail: (Option<i64>, Option<i64>, Option<i64>) = storage
        .raw()
        .query_row_map(
            "SELECT ended_at, last_message_idx, last_message_created_at
             FROM conversation_tail_state
             WHERE conversation_id = ?1",
            fparams![conversation_id],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?, row.get_typed(2)?)),
        )
        .unwrap();
    assert_eq!(initial_tail, (Some(111_005), Some(4), Some(111_004)));

    storage
        .raw()
        .execute_compat(
            "UPDATE conversations SET ended_at = ?1 WHERE id = ?2",
            fparams![111_999_i64, conversation_id],
        )
        .unwrap();
    storage
        .raw()
        .execute_compat(
            "DELETE FROM conversation_tail_state WHERE conversation_id = ?1",
            fparams![conversation_id],
        )
        .unwrap();

    let appended = make_profiled_append_remote_merge_conversation(11, 10);
    let append_outcome = storage
        .insert_conversation_tree(agent_id, Some(workspace_id), &appended)
        .unwrap();
    assert_eq!(append_outcome.inserted_indices, vec![5, 6, 7, 8, 9]);

    let final_tail: (Option<i64>, Option<i64>, Option<i64>) = storage
        .raw()
        .query_row_map(
            "SELECT ended_at, last_message_idx, last_message_created_at
             FROM conversation_tail_state
             WHERE conversation_id = ?1",
            fparams![conversation_id],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?, row.get_typed(2)?)),
        )
        .unwrap();
    assert_eq!(final_tail, (Some(111_999), Some(9), Some(111_009)));
}

#[test]
fn msgpack_deserialize_empty_returns_default() {
    let recovered = deserialize_msgpack_to_json(&[]);
    assert_eq!(recovered, serde_json::Value::Object(serde_json::Map::new()));
}

#[test]
fn msgpack_deserialize_garbage_returns_default() {
    // Use truncated msgpack data that will fail to parse
    // 0x85 indicates a fixmap with 5 elements, but we don't provide them
    let recovered = deserialize_msgpack_to_json(&[0x85]);
    assert_eq!(recovered, serde_json::Value::Object(serde_json::Map::new()));
}

#[test]
fn stats_aggregator_collects_and_expands() {
    let mut agg = StatsAggregator::new();
    assert!(agg.is_empty());

    // Record some stats
    // Day 100, agent "claude", source "local"
    agg.record("claude", "local", 100, 5, 500);
    // Day 100, agent "codex", source "local"
    agg.record("codex", "local", 100, 3, 300);
    // Day 101, agent "claude", source "local"
    agg.record("claude", "local", 101, 2, 200);

    assert!(!agg.is_empty());
    assert_eq!(agg.raw_entry_count(), 3);

    let entries = agg.expand();
    // Each raw entry expands to 4 permutations.
    // But (all, local) and (all, all) will aggregate.
    //
    // Raw:
    // 1. (100, claude, local) -> 1 sess, 5 msgs, 500 chars
    // 2. (100, codex, local)  -> 1 sess, 3 msgs, 300 chars
    // 3. (101, claude, local) -> 1 sess, 2 msgs, 200 chars
    //
    // Expanded 1 (day 100):
    // - (100, claude, local): 1 sess, 5 msgs, 500 chars
    // - (100, all, local):    1 (from claude) + 1 (from codex) = 2 sess, 8 msgs, 800 chars
    // - (100, claude, all):   1 sess, 5 msgs, 500 chars
    // - (100, codex, local):  1 sess, 3 msgs, 300 chars
    // - (100, codex, all):    1 sess, 3 msgs, 300 chars
    // - (100, all, all):      2 sess, 8 msgs, 800 chars
    //
    // Expanded 3 (day 101):
    // - (101, claude, local): 1 sess, 2 msgs, 200 chars
    // - (101, all, local):    1 sess, 2 msgs, 200 chars
    // - (101, claude, all):   1 sess, 2 msgs, 200 chars
    // - (101, all, all):      1 sess, 2 msgs, 200 chars
    //
    // Total unique keys in expanded map:
    // Day 100: (claude, local), (codex, local), (all, local), (claude, all), (codex, all), (all, all) = 6
    // Day 101: (claude, local), (all, local), (claude, all), (all, all) = 4
    // Total = 10 entries

    assert_eq!(entries.len(), 10);

    // Verify totals for day 100, all/all
    let day100_all = entries
        .iter()
        .find(|(d, a, s, _)| *d == 100 && a == "all" && s == "all")
        .unwrap();
    assert_eq!(day100_all.3.session_count_delta, 2);
    assert_eq!(day100_all.3.message_count_delta, 8);
    assert_eq!(day100_all.3.total_chars_delta, 800);
}

// =========================================================================
// LazyFrankenDb tests (bd-1ueu)
// =========================================================================

#[test]
fn lazy_franken_db_not_open_before_get() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("lazy_test.db");

    // Create a real DB so the path exists
    let _storage = SqliteStorage::open(&db_path).unwrap();

    let lazy = LazyFrankenDb::new(db_path);
    assert!(
        !lazy.is_open(),
        "LazyFrankenDb must not open on construction"
    );
}

#[test]
fn lazy_franken_db_opens_on_first_get() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("lazy_test.db");

    // Create a real DB so the path exists
    let _storage = SqliteStorage::open(&db_path).unwrap();
    drop(_storage);

    let lazy = LazyFrankenDb::new(db_path);
    assert!(!lazy.is_open());

    let conn = lazy.get("test").expect("should open successfully");
    let count: i64 = conn
        .query_row_map("SELECT COUNT(*) FROM conversations", fparams![], |r| {
            r.get_typed(0)
        })
        .unwrap();
    assert_eq!(count, 0);
    drop(conn);

    assert!(lazy.is_open(), "LazyFrankenDb must be open after get()");
}

#[test]
fn lazy_franken_db_reuses_connection() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("lazy_test.db");
    let _storage = SqliteStorage::open(&db_path).unwrap();
    drop(_storage);

    let lazy = LazyFrankenDb::new(db_path);

    // First access opens
    {
        let conn = lazy.get("first").unwrap();
        conn.execute_batch("CREATE TABLE IF NOT EXISTS test_tbl (id INTEGER)")
            .unwrap();
    }

    // Second access reuses (table still exists)
    {
        let conn = lazy.get("second").unwrap();
        let count: i64 = conn
            .query_row_map("SELECT COUNT(*) FROM test_tbl", fparams![], |r| {
                r.get_typed(0)
            })
            .unwrap();
        assert_eq!(count, 0);
    }
}

#[test]
fn lazy_franken_db_not_found_error() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("nonexistent.db");

    let lazy = LazyFrankenDb::new(db_path);
    let result = lazy.get("test");
    assert!(result.is_err());
    assert!(
        matches!(result.unwrap_err(), LazyDbError::NotFound(_)),
        "should return NotFound for missing DB"
    );
}

#[test]
fn lazy_franken_db_path_accessor() {
    let path = PathBuf::from("/tmp/test_lazy.db");
    let lazy = LazyFrankenDb::new(path.clone());
    assert_eq!(lazy.path(), path.as_path());
}

#[test]
fn dedicated_owner_safe_handles_are_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<FrankenOwnerConnection>();
    assert_send_sync::<LazyFrankenDb>();
    assert_send_sync::<FrankenConnectionManager>();
}

#[test]
fn storage_source_forbids_manual_raw_connection_auto_trait_overrides() {
    fn collect_rs_files(dir: &std::path::Path, out: &mut Vec<PathBuf>) {
        let mut entries: Vec<_> = std::fs::read_dir(dir)
            .unwrap_or_else(|err| panic!("read_dir {}: {err}", dir.display()))
            .map(|entry| entry.expect("dir entry").path())
            .collect();
        entries.sort();
        for path in entries {
            if path.is_dir() {
                collect_rs_files(&path, out);
            } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
                out.push(path);
            }
        }
    }

    let manifest_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let src_root = manifest_root.join("src");
    let mut rs_files = Vec::new();
    collect_rs_files(&src_root, &mut rs_files);

    let forbidden = [
        ["unsafe impl ", "Send for"].concat(),
        ["unsafe impl ", "Sync for"].concat(),
        ["SendFranken", "Connection"].concat(),
    ];

    let mut visited_files = std::collections::BTreeSet::new();
    let mut total_bytes_scanned = 0usize;
    for path in &rs_files {
        let rel_path = path
            .strip_prefix(manifest_root)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        let source =
            std::fs::read_to_string(path).unwrap_or_else(|err| panic!("read {rel_path}: {err}"));
        total_bytes_scanned += source.len();
        visited_files.insert(rel_path.clone());
        for fragment in &forbidden {
            assert!(
                !source.contains(fragment),
                "thread-affine FrankenSQLite safety override reintroduced in {rel_path}: {fragment}"
            );
        }
    }

    assert!(
        visited_files.contains("src/storage/sqlite.rs"),
        "source scan must include src/storage/sqlite.rs"
    );
    assert!(
        visited_files.contains("src/lib.rs"),
        "source scan must include src/lib.rs"
    );
    assert!(
        visited_files.len() >= 500,
        "expected at least 500 src/**/*.rs files scanned, got {}",
        visited_files.len()
    );
    assert!(
        total_bytes_scanned > 0,
        "source scan must read non-empty bytes"
    );
}

#[test]
fn dedicated_owner_lazy_and_manager_reads_cross_worker_boundaries() {
    use crate::franken_sync::compat::RowExt as _;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("owner-thread-dispatch.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    storage
        .raw()
        .execute_batch(
            "CREATE TABLE owner_thread_probe (value INTEGER NOT NULL);\
             INSERT INTO owner_thread_probe(value) VALUES (41), (1);",
        )
        .unwrap();
    storage.close().unwrap();

    let lazy = Arc::new(LazyFrankenDb::new(db_path.clone()));
    let lazy_workers = (0..4)
        .map(|_| {
            let lazy = Arc::clone(&lazy);
            std::thread::spawn(move || {
                let conn = lazy
                    .get_with_timeout("cross-worker lazy read", Duration::from_secs(5))
                    .unwrap();
                conn.query_row_map(
                    "SELECT SUM(value) FROM owner_thread_probe",
                    fparams![],
                    |row| row.get_typed::<i64>(0),
                )
                .unwrap()
            })
        })
        .collect::<Vec<_>>();
    for worker in lazy_workers {
        assert_eq!(worker.join().unwrap(), 42);
    }

    let manager = Arc::new(
        FrankenConnectionManager::new(
            &db_path,
            ConnectionManagerConfig {
                reader_count: 2,
                max_writers: 1,
            },
        )
        .unwrap(),
    );
    let manager_workers = (0..4)
        .map(|_| {
            let manager = Arc::clone(&manager);
            std::thread::spawn(move || {
                let conn = manager.reader();
                conn.query_row_map(
                    "SELECT SUM(value) FROM owner_thread_probe",
                    fparams![],
                    |row| row.get_typed::<i64>(0),
                )
                .unwrap()
            })
        })
        .collect::<Vec<_>>();
    for worker in manager_workers {
        assert_eq!(worker.join().unwrap(), 42);
    }
}

// =========================================================================
// Pricing / cost estimation tests (bead z9fse.10)
// =========================================================================

#[test]
fn sql_like_match_basic_patterns() {
    assert!(sql_like_match("claude-opus-4-20250101", "claude-opus-4%"));
    assert!(sql_like_match("claude-opus-4", "claude-opus-4%"));
    assert!(!sql_like_match("claude-sonnet-4", "claude-opus-4%"));

    // Middle wildcard (gemini pattern)
    assert!(sql_like_match("gemini-2.0-flash-001", "gemini-2%flash%"));
    assert!(sql_like_match("gemini-2-flash", "gemini-2%flash%"));
    assert!(!sql_like_match("gemini-2-pro", "gemini-2%flash%"));

    // Exact match
    assert!(sql_like_match("hello", "hello"));
    assert!(!sql_like_match("hello!", "hello"));

    // Underscore wildcard
    assert!(sql_like_match("gpt-4o", "gpt-4_"));
    assert!(!sql_like_match("gpt-4oo", "gpt-4_"));

    // Case insensitive
    assert!(sql_like_match("Claude-Opus-4", "claude-opus-4%"));
}

#[test]
fn date_str_to_day_id_converts_correctly() {
    // 2025-10-01 is 2100 days after 2020-01-01
    assert_eq!(date_str_to_day_id("2025-10-01").unwrap(), 2100);
    // 2024-04-01 is 1552 days after 2020-01-01
    assert_eq!(date_str_to_day_id("2024-04-01").unwrap(), 1552);
    assert!(date_str_to_day_id("invalid").is_err());
}

#[test]
fn pricing_table_lookup_selects_matching_entry() {
    let effective_day = date_str_to_day_id("2025-10-01").unwrap();
    let lookup_day = date_str_to_day_id("2026-02-06").unwrap();
    let table = PricingTable {
        entries: vec![
            PricingEntry {
                model_pattern: "claude-opus-4%".into(),
                provider: "anthropic".into(),
                input_cost_per_mtok: 15.0,
                output_cost_per_mtok: 75.0,
                cache_read_cost_per_mtok: Some(1.5),
                cache_creation_cost_per_mtok: Some(18.75),
                effective_day_id: effective_day,
            },
            PricingEntry {
                model_pattern: "claude-sonnet-4%".into(),
                provider: "anthropic".into(),
                input_cost_per_mtok: 3.0,
                output_cost_per_mtok: 15.0,
                cache_read_cost_per_mtok: Some(0.3),
                cache_creation_cost_per_mtok: Some(3.75),
                effective_day_id: effective_day,
            },
        ],
    };

    let result = table.lookup("claude-opus-4-20260101", lookup_day);
    assert!(result.is_some());
    assert_eq!(result.unwrap().input_cost_per_mtok, 15.0);

    let result = table.lookup("claude-sonnet-4-latest", lookup_day);
    assert!(result.is_some());
    assert_eq!(result.unwrap().input_cost_per_mtok, 3.0);

    assert!(table.lookup("unknown-model", lookup_day).is_none());
}

#[test]
fn pricing_table_lookup_respects_effective_date() {
    let effective_day_1 = date_str_to_day_id("2025-10-01").unwrap();
    let effective_day_2 = date_str_to_day_id("2026-01-01").unwrap();
    let table = PricingTable {
        entries: vec![
            PricingEntry {
                model_pattern: "claude-opus-4%".into(),
                provider: "anthropic".into(),
                input_cost_per_mtok: 15.0,
                output_cost_per_mtok: 75.0,
                cache_read_cost_per_mtok: None,
                cache_creation_cost_per_mtok: None,
                effective_day_id: effective_day_1,
            },
            PricingEntry {
                model_pattern: "claude-opus-4%".into(),
                provider: "anthropic".into(),
                input_cost_per_mtok: 12.0,
                output_cost_per_mtok: 60.0,
                cache_read_cost_per_mtok: None,
                cache_creation_cost_per_mtok: None,
                effective_day_id: effective_day_2,
            },
        ],
    };

    // Before price drop
    let result = table.lookup("claude-opus-4", date_str_to_day_id("2025-11-01").unwrap());
    assert!(result.is_some());
    assert_eq!(result.unwrap().input_cost_per_mtok, 15.0);

    // After price drop
    let result = table.lookup("claude-opus-4", date_str_to_day_id("2026-02-01").unwrap());
    assert!(result.is_some());
    assert_eq!(result.unwrap().input_cost_per_mtok, 12.0);

    // Before all pricing
    assert!(
        table
            .lookup("claude-opus-4", date_str_to_day_id("2024-01-01").unwrap())
            .is_none()
    );
}

#[test]
fn pricing_table_lookup_specificity_tiebreak() {
    let effective_day = date_str_to_day_id("2025-01-01").unwrap();
    let lookup_day = date_str_to_day_id("2026-01-01").unwrap();
    let table = PricingTable {
        entries: vec![
            PricingEntry {
                model_pattern: "gpt-4%".into(),
                provider: "openai".into(),
                input_cost_per_mtok: 10.0,
                output_cost_per_mtok: 30.0,
                cache_read_cost_per_mtok: None,
                cache_creation_cost_per_mtok: None,
                effective_day_id: effective_day,
            },
            PricingEntry {
                model_pattern: "gpt-4-turbo%".into(),
                provider: "openai".into(),
                input_cost_per_mtok: 5.0,
                output_cost_per_mtok: 15.0,
                cache_read_cost_per_mtok: None,
                cache_creation_cost_per_mtok: None,
                effective_day_id: effective_day,
            },
        ],
    };

    // Longer pattern wins for specific model
    let result = table.lookup("gpt-4-turbo-2025", lookup_day);
    assert!(result.is_some());
    assert_eq!(result.unwrap().input_cost_per_mtok, 5.0);

    // Shorter pattern matches broader model
    let result = table.lookup("gpt-4o", lookup_day);
    assert!(result.is_some());
    assert_eq!(result.unwrap().input_cost_per_mtok, 10.0);
}

#[test]
fn pricing_table_compute_cost_basic() {
    let effective_day = date_str_to_day_id("2025-10-01").unwrap();
    let table = PricingTable {
        entries: vec![PricingEntry {
            model_pattern: "claude-opus-4%".into(),
            provider: "anthropic".into(),
            input_cost_per_mtok: 15.0,
            output_cost_per_mtok: 75.0,
            cache_read_cost_per_mtok: Some(1.5),
            cache_creation_cost_per_mtok: Some(18.75),
            effective_day_id: effective_day,
        }],
    };

    let cost = table.compute_cost(
        Some("claude-opus-4-latest"),
        date_str_to_day_id("2026-02-06").unwrap(),
        Some(1000),
        Some(500),
        None,
        None,
    );
    assert!(cost.is_some());
    // 1000 * 15.0 / 1M + 500 * 75.0 / 1M = 0.015 + 0.0375 = 0.0525
    assert!((cost.unwrap() - 0.0525).abs() < 1e-10);
}

#[test]
fn pricing_table_compute_cost_with_cache() {
    let effective_day = date_str_to_day_id("2025-10-01").unwrap();
    let table = PricingTable {
        entries: vec![PricingEntry {
            model_pattern: "claude-opus-4%".into(),
            provider: "anthropic".into(),
            input_cost_per_mtok: 15.0,
            output_cost_per_mtok: 75.0,
            cache_read_cost_per_mtok: Some(1.5),
            cache_creation_cost_per_mtok: Some(18.75),
            effective_day_id: effective_day,
        }],
    };

    let cost = table.compute_cost(
        Some("claude-opus-4-latest"),
        date_str_to_day_id("2026-02-06").unwrap(),
        Some(1_000_000),
        Some(100_000),
        Some(500_000),
        Some(200_000),
    );
    assert!(cost.is_some());
    // input excludes cache tokens to avoid double-charging them at both the
    // full input rate and the cache-specific rates.
    // non-cache input: 300K * 15/1M = 4.5, output: 100K * 75/1M = 7.5
    // cache_read: 500K * 1.5/1M = 0.75, cache_creation: 200K * 18.75/1M = 3.75
    // total = 16.5
    assert!((cost.unwrap() - 16.5).abs() < 1e-10);
}

#[test]
fn pricing_table_compute_cost_returns_none_for_unknown_model() {
    let effective_day = date_str_to_day_id("2025-10-01").unwrap();
    let lookup_day = date_str_to_day_id("2026-02-06").unwrap();
    let table = PricingTable {
        entries: vec![PricingEntry {
            model_pattern: "claude-opus-4%".into(),
            provider: "anthropic".into(),
            input_cost_per_mtok: 15.0,
            output_cost_per_mtok: 75.0,
            cache_read_cost_per_mtok: None,
            cache_creation_cost_per_mtok: None,
            effective_day_id: effective_day,
        }],
    };

    assert!(
        table
            .compute_cost(
                Some("unknown-model"),
                lookup_day,
                Some(1000),
                Some(500),
                None,
                None
            )
            .is_none()
    );
    assert!(
        table
            .compute_cost(None, lookup_day, Some(1000), Some(500), None, None)
            .is_none()
    );
    assert!(
        table
            .compute_cost(Some("claude-opus-4"), lookup_day, None, None, None, None)
            .is_none()
    );
}

#[test]
fn pricing_table_load_from_db() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let table = PricingTable::load(&storage.conn).unwrap();
    assert!(!table.is_empty());

    let lookup_day = date_str_to_day_id("2026-02-06").unwrap();

    let opus = table.lookup("claude-opus-4-latest", lookup_day);
    assert!(opus.is_some());
    assert_eq!(opus.unwrap().input_cost_per_mtok, 15.0);

    let flash = table.lookup("gemini-2.0-flash-001", lookup_day);
    assert!(flash.is_some());
    assert_eq!(flash.unwrap().input_cost_per_mtok, 0.075);
}

#[test]
fn pricing_table_load_rejects_invalid_effective_date() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    storage
        .conn
        .execute_compat(
            "INSERT INTO model_pricing (
                model_pattern, provider, input_cost_per_mtok, output_cost_per_mtok,
                cache_read_cost_per_mtok, cache_creation_cost_per_mtok, effective_date
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            fparams![
                "broken-model%",
                "test",
                1.0_f64,
                2.0_f64,
                Option::<f64>::None,
                Option::<f64>::None,
                "not-a-date"
            ],
        )
        .unwrap();

    let err = PricingTable::load(&storage.conn).unwrap_err();
    assert!(err.to_string().contains("invalid effective_date"));
}

#[test]
fn pricing_diagnostics_tracks_coverage() {
    let mut diag = PricingDiagnostics::default();
    diag.record_priced();
    diag.record_priced();
    diag.record_unpriced(Some("custom-model-v1"));
    diag.record_unpriced(Some("custom-model-v1"));
    diag.record_unpriced(None);

    assert_eq!(diag.priced_count, 2);
    assert_eq!(diag.unpriced_count, 3);
    assert_eq!(diag.unknown_models.len(), 2);
    assert_eq!(diag.unknown_models["custom-model-v1"], 2);
    assert_eq!(diag.unknown_models["(none)"], 1);
}
