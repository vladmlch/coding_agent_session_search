use super::*;
use crate::franken_sync::compat::{ConnectionExt as FrankenConnectionExt, RowExt as FrankenRowExt};
use tempfile::TempDir;

#[test]
fn insert_conversation_tree_merges_replay_equivalent_messages_with_shifted_idx() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: Some("0.2.3".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();

    let make_conv = |started_at: i64, messages: Vec<Message>| Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: None,
        title: Some("Shifted replay".into()),
        source_path: PathBuf::from("/tmp/replay-session.jsonl"),
        started_at: Some(started_at),
        ended_at: Some(started_at + 500),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages,
        source_id: "local".into(),
        origin_host: None,
    };

    let first = storage
        .insert_conversation_tree(
            agent_id,
            None,
            &make_conv(
                1_700_000_000_000,
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
            ),
        )
        .unwrap();

    let second = storage
        .insert_conversation_tree(
            agent_id,
            None,
            &make_conv(
                1_700_000_900_000,
                vec![
                    Message {
                        id: None,
                        idx: 10,
                        role: MessageRole::User,
                        author: None,
                        created_at: Some(1_700_000_000_000),
                        content: "first".into(),
                        extra_json: serde_json::Value::Null,
                        snippets: Vec::new(),
                    },
                    Message {
                        id: None,
                        idx: 11,
                        role: MessageRole::Agent,
                        author: None,
                        created_at: Some(1_700_000_000_100),
                        content: "second".into(),
                        extra_json: serde_json::Value::Null,
                        snippets: Vec::new(),
                    },
                    Message {
                        id: None,
                        idx: 12,
                        role: MessageRole::User,
                        author: None,
                        created_at: Some(1_700_000_000_200),
                        content: "third".into(),
                        extra_json: serde_json::Value::Null,
                        snippets: Vec::new(),
                    },
                ],
            ),
        )
        .unwrap();

    assert_eq!(first.conversation_id, second.conversation_id);
    assert_eq!(second.inserted_indices, vec![12]);

    let stored_indices: Vec<i64> = storage
        .conn
        .query_map_collect(
            "SELECT idx FROM messages WHERE conversation_id = ?1 ORDER BY idx",
            fparams![first.conversation_id],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(stored_indices, vec![0, 1, 12]);
}

#[test]
fn insert_conversation_tree_keeps_both_tool_outputs_when_only_call_id_differs() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: Some("0.2.3".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();

    let make_conv = |messages: Vec<Message>| Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: None,
        title: Some("Codex twin tool outputs".into()),
        source_path: PathBuf::from("/tmp/codex-twin-tool-outputs.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_999),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages,
        source_id: "local".into(),
        origin_host: None,
    };

    // Two codex `function_call_output` records whose raw JSONL envelopes are
    // archived whole in `extra`. They collide on the replay fingerprint
    // (same millisecond created_at, role, author and content); only the
    // native `payload.call_id` distinguishes them.
    let tool_output = |idx: i64, call_id: &str| Message {
        id: None,
        idx,
        role: MessageRole::Tool,
        author: None,
        created_at: Some(1_700_000_000_000),
        content: "0 files changed\n".into(),
        extra_json: serde_json::json!({
            "type": "response_item",
            "payload": {
                "type": "function_call_output",
                "call_id": call_id,
                "output": "0 files changed\n"
            }
        }),
        snippets: Vec::new(),
    };

    let outcome = storage
        .insert_conversation_tree(
            agent_id,
            None,
            &make_conv(vec![
                tool_output(0, "call_apply_patch"),
                tool_output(1, "call_read_file"),
            ]),
        )
        .unwrap();

    assert_eq!(outcome.inserted_indices, vec![0, 1]);
    let stored_call_ids: Vec<String> = storage
        .conn
        .query_map_collect(
            "SELECT extra_json, extra_bin
             FROM messages
             WHERE conversation_id = ?1
             ORDER BY idx",
            fparams![outcome.conversation_id],
            |row| {
                let extra = franken_read_message_extra_compat(row, 0, 1);
                Ok(extra
                    .pointer("/payload/call_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_string())
            },
        )
        .unwrap();
    assert_eq!(
        stored_call_ids,
        vec!["call_apply_patch".to_string(), "call_read_file".to_string(),]
    );
}

#[test]
fn insert_conversation_tree_appends_tool_outputs_when_only_call_id_differs() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: Some("0.2.3".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();

    let make_conv = |messages: Vec<Message>| Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: None,
        title: Some("Codex twin tool outputs".into()),
        source_path: PathBuf::from("/tmp/codex-twin-tool-outputs.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_999),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages,
        source_id: "local".into(),
        origin_host: None,
    };

    let tool_output = |idx: i64, call_id: &str| Message {
        id: None,
        idx,
        role: MessageRole::Tool,
        author: None,
        created_at: Some(1_700_000_000_000),
        content: "0 files changed\n".into(),
        extra_json: serde_json::json!({
            "type": "response_item",
            "payload": {
                "type": "function_call_output",
                "call_id": call_id,
                "output": "0 files changed\n"
            }
        }),
        snippets: Vec::new(),
    };

    let first = storage
        .insert_conversation_tree(
            agent_id,
            None,
            &make_conv(vec![tool_output(0, "call_apply_patch")]),
        )
        .unwrap();

    // Re-scan of the same session file that now carries two further tool
    // outputs sharing the stored record's millisecond timestamp and
    // content; only their native call ids differ.
    let second = storage
        .insert_conversation_tree(
            agent_id,
            None,
            &make_conv(vec![
                tool_output(0, "call_apply_patch"),
                tool_output(1, "call_read_file"),
                tool_output(2, "call_shell"),
            ]),
        )
        .unwrap();

    assert_eq!(first.conversation_id, second.conversation_id);
    assert_eq!(second.inserted_indices, vec![1, 2]);
    let stored_call_ids: Vec<String> = storage
        .conn
        .query_map_collect(
            "SELECT extra_json, extra_bin
             FROM messages
             WHERE conversation_id = ?1
             ORDER BY idx",
            fparams![first.conversation_id],
            |row| {
                let extra = franken_read_message_extra_compat(row, 0, 1);
                Ok(extra
                    .pointer("/payload/call_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_string())
            },
        )
        .unwrap();
    assert_eq!(
        stored_call_ids,
        vec![
            "call_apply_patch".to_string(),
            "call_read_file".to_string(),
            "call_shell".to_string(),
        ]
    );
}

#[test]
fn insert_conversation_tree_reingests_tool_outputs_with_native_ids_once() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: Some("0.2.3".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();

    let make_conv = |messages: Vec<Message>| Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: None,
        title: Some("Codex twin tool outputs".into()),
        source_path: PathBuf::from("/tmp/codex-twin-tool-outputs.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_999),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages,
        source_id: "local".into(),
        origin_host: None,
    };

    let tool_output = |idx: i64, call_id: &str| Message {
        id: None,
        idx,
        role: MessageRole::Tool,
        author: None,
        created_at: Some(1_700_000_000_000),
        content: "0 files changed\n".into(),
        extra_json: serde_json::json!({
            "type": "response_item",
            "payload": {
                "type": "function_call_output",
                "call_id": call_id,
                "output": "0 files changed\n"
            }
        }),
        snippets: Vec::new(),
    };

    fn stored_message_count(storage: &SqliteStorage, conversation_id: i64) -> i64 {
        storage
            .conn
            .query_row_map(
                "SELECT COUNT(*) FROM messages WHERE conversation_id = ?1",
                fparams![conversation_id],
                |row| row.get_typed(0),
            )
            .unwrap()
    }

    // Two native-identified tool outputs sharing a replay core.
    let first = storage
        .insert_conversation_tree(
            agent_id,
            None,
            &make_conv(vec![
                tool_output(0, "call_apply_patch"),
                tool_output(1, "call_read_file"),
            ]),
        )
        .unwrap();
    assert_eq!(first.inserted_indices, vec![0, 1]);

    // Identical re-ingest: every message must be stored exactly once.
    let second = storage
        .insert_conversation_tree(
            agent_id,
            None,
            &make_conv(vec![
                tool_output(0, "call_apply_patch"),
                tool_output(1, "call_read_file"),
            ]),
        )
        .unwrap();
    assert!(second.inserted_indices.is_empty());
    assert_eq!(stored_message_count(&storage, first.conversation_id), 2);

    // Re-ingest of renumbered source records: the replay guard must match
    // through the native call ids rather than re-insert the messages.
    let third = storage
        .insert_conversation_tree(
            agent_id,
            None,
            &make_conv(vec![
                tool_output(10, "call_apply_patch"),
                tool_output(11, "call_read_file"),
            ]),
        )
        .unwrap();
    assert!(third.inserted_indices.is_empty());
    assert_eq!(stored_message_count(&storage, first.conversation_id), 2);

    // Re-ingest whose extras lost the native id (franken-agent-detection
    // compacts codex records for >=32 MiB sessions): the unknown id must
    // still replay the stored copies instead of duplicating them.
    let compacted_tool_output = |idx: i64| Message {
        id: None,
        idx,
        role: MessageRole::Tool,
        author: None,
        created_at: Some(1_700_000_000_000),
        content: "0 files changed\n".into(),
        extra_json: serde_json::json!({"cass": {}}),
        snippets: Vec::new(),
    };
    let fourth = storage
        .insert_conversation_tree(
            agent_id,
            None,
            &make_conv(vec![compacted_tool_output(20), compacted_tool_output(21)]),
        )
        .unwrap();
    assert!(fourth.inserted_indices.is_empty());
    assert_eq!(stored_message_count(&storage, first.conversation_id), 2);
}

#[test]
fn insert_conversation_tree_keeps_distinct_untimestamped_messages_sharing_text() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let agent = Agent {
        id: None,
        slug: "cursor".into(),
        name: "Cursor".into(),
        version: None,
        kind: AgentKind::VsCode,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();

    // Cursor transcript rows carry no per-message timestamp (created_at
    // NULL), and distinct tool calls flatten to the same placeholder text:
    // only the archived `cass.invocations` arguments distinguish them.
    // Plain assistant texts repeat as well. Every row below is a distinct
    // record of one merged Cursor conversation (dense idx).
    let cursor_row = |idx: i64, content: &str, path: Option<&str>| Message {
        id: None,
        idx,
        role: MessageRole::Agent,
        author: None,
        created_at: None,
        content: content.into(),
        extra_json: match path {
            Some(path) => serde_json::json!({
                "cass": {"invocations": [{
                    "kind": "tool", "name": "Read",
                    "arguments": {"path": path}
                }]}
            }),
            None => serde_json::json!({}),
        },
        snippets: Vec::new(),
    };

    let conv = Conversation {
        id: None,
        agent_slug: "cursor".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: Some("a8831300-8e86-46af-851a-b6b2080466f1".into()),
        title: Some("Cursor twin tool calls".into()),
        source_path: PathBuf::from("/tmp/cursor-state.vscdb"),
        started_at: None,
        ended_at: None,
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: vec![
            cursor_row(0, "[Tool: Read]", Some("/repo/src/lib.rs")),
            cursor_row(1, "[Tool: Read]", Some("/repo/src/main.rs")),
            cursor_row(2, "Working on it", None),
            cursor_row(3, "Working on it", None),
        ],
        source_id: "local".into(),
        origin_host: None,
    };

    let outcome = storage
        .insert_conversation_tree(agent_id, None, &conv)
        .unwrap();
    assert_eq!(outcome.inserted_indices, vec![0, 1, 2, 3]);

    let rows: Vec<(i64, String)> = storage
        .conn
        .query_map_collect(
            "SELECT idx, content FROM messages WHERE conversation_id = ?1 ORDER BY idx",
            fparams![outcome.conversation_id],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
        )
        .unwrap();
    assert_eq!(
        rows,
        vec![
            (0, "[Tool: Read]".to_string()),
            (1, "[Tool: Read]".to_string()),
            (2, "Working on it".to_string()),
            (3, "Working on it".to_string()),
        ]
    );
}

#[test]
fn insert_conversation_tree_appends_distinct_untimestamped_messages_matching_stored_text() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let agent = Agent {
        id: None,
        slug: "cursor".into(),
        name: "Cursor".into(),
        version: None,
        kind: AgentKind::VsCode,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();

    let make_conv = |messages: Vec<Message>| Conversation {
        id: None,
        agent_slug: "cursor".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: Some("a8831300-8e86-46af-851a-b6b2080466f1".into()),
        title: Some("Cursor twin tool calls".into()),
        source_path: PathBuf::from("/tmp/cursor-state.vscdb"),
        started_at: None,
        ended_at: None,
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages,
        source_id: "local".into(),
        origin_host: None,
    };

    let cursor_row = |idx: i64, content: &str, path: &str| Message {
        id: None,
        idx,
        role: MessageRole::Agent,
        author: None,
        created_at: None,
        content: content.into(),
        extra_json: serde_json::json!({
            "cass": {"invocations": [{
                "kind": "tool", "name": "Read",
                "arguments": {"path": path}
            }]}
        }),
        snippets: Vec::new(),
    };

    let first = storage
        .insert_conversation_tree(
            agent_id,
            None,
            &make_conv(vec![
                cursor_row(0, "[Tool: Read]", "/repo/src/lib.rs"),
                cursor_row(1, "Reviewing the diff", "/repo/src/main.rs"),
            ]),
        )
        .unwrap();
    assert_eq!(first.inserted_indices, vec![0, 1]);

    // A later scan of the same conversation appends a new tool call that
    // flattens to the same "[Tool: Read]" placeholder text as the stored
    // row; its arguments are distinct, so it is a new record.
    let second = storage
        .insert_conversation_tree(
            agent_id,
            None,
            &make_conv(vec![
                cursor_row(0, "[Tool: Read]", "/repo/src/lib.rs"),
                cursor_row(1, "Reviewing the diff", "/repo/src/main.rs"),
                cursor_row(2, "[Tool: Read]", "/repo/tests/api.rs"),
            ]),
        )
        .unwrap();

    assert_eq!(first.conversation_id, second.conversation_id);
    assert_eq!(second.inserted_indices, vec![2]);
    let count: i64 = storage
        .conn
        .query_row_map(
            "SELECT COUNT(*) FROM messages WHERE conversation_id = ?1",
            fparams![first.conversation_id],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(count, 3);
}

#[test]
fn insert_conversation_tree_replays_untimestamped_messages_only_through_shared_native_ids() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let agent = Agent {
        id: None,
        slug: "cursor".into(),
        name: "Cursor".into(),
        version: None,
        kind: AgentKind::VsCode,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();

    let make_conv = |messages: Vec<Message>| Conversation {
        id: None,
        agent_slug: "cursor".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: Some("a8831300-8e86-46af-851a-b6b2080466f1".into()),
        title: Some("Cursor twin tool calls".into()),
        source_path: PathBuf::from("/tmp/cursor-state.vscdb"),
        started_at: None,
        ended_at: None,
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages,
        source_id: "local".into(),
        origin_host: None,
    };

    // Untimestamped rows that do carry a native record id keep native-id
    // replay semantics: the same id re-emitted at a shifted idx is a
    // replay, a different id is a distinct record.
    let cursor_row = |idx: i64, call_id: &str| Message {
        id: None,
        idx,
        role: MessageRole::Agent,
        author: None,
        created_at: None,
        content: "[Tool: Read]".into(),
        extra_json: serde_json::json!({
            "cass": {"invocations": [{
                "kind": "tool", "name": "Read", "call_id": call_id,
                "arguments": {"path": "/repo/src/lib.rs"}
            }]}
        }),
        snippets: Vec::new(),
    };

    fn stored_message_count(storage: &SqliteStorage, conversation_id: i64) -> i64 {
        storage
            .conn
            .query_row_map(
                "SELECT COUNT(*) FROM messages WHERE conversation_id = ?1",
                fparams![conversation_id],
                |row| row.get_typed(0),
            )
            .unwrap()
    }

    let first = storage
        .insert_conversation_tree(
            agent_id,
            None,
            &make_conv(vec![
                cursor_row(0, "call_read_lib"),
                cursor_row(1, "call_read_main"),
            ]),
        )
        .unwrap();
    assert_eq!(first.inserted_indices, vec![0, 1]);

    // Re-emission of both records at shifted idx: same native ids, so both
    // are replays and nothing is re-inserted.
    let second = storage
        .insert_conversation_tree(
            agent_id,
            None,
            &make_conv(vec![
                cursor_row(10, "call_read_lib"),
                cursor_row(11, "call_read_main"),
            ]),
        )
        .unwrap();
    assert!(second.inserted_indices.is_empty());
    assert_eq!(stored_message_count(&storage, first.conversation_id), 2);

    // A distinct native id is a distinct record even at shifted idx.
    let third = storage
        .insert_conversation_tree(
            agent_id,
            None,
            &make_conv(vec![cursor_row(12, "call_read_tests")]),
        )
        .unwrap();
    assert_eq!(third.inserted_indices, vec![12]);
    assert_eq!(stored_message_count(&storage, first.conversation_id), 3);
}

#[test]
fn insert_conversation_tree_keeps_distinct_timestamped_tool_results_with_distinct_tool_call_ids() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let agent = Agent {
        id: None,
        slug: "devin".into(),
        name: "Devin".into(),
        version: None,
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();

    // Devin's sparse tool rows carry their call identity ONLY as a
    // top-level `tool_call_id` in extras, and every row of one turn
    // shares the turn's `created_at` (second-granularity node
    // timestamps). Two identical reads in one turn therefore share the
    // whole replay core; their distinct ids are the only native
    // identity the records carry, so the native-id resolution must
    // read the top-level key — or every duplicate-text result after
    // the first is silently dropped at ingest (62 devin sessions, 507
    // unpaired calls on the owner archive).
    let make_conv = |messages: Vec<Message>| Conversation {
        id: None,
        agent_slug: "devin".into(),
        workspace: None,
        external_id: Some("devin-session-285".into()),
        title: Some("Devin duplicate reads".into()),
        source_path: PathBuf::from("/tmp/devin-sessions.db"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_000),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages,
        source_id: "local".into(),
        origin_host: None,
    };
    let turn_ts = 1_700_000_000_000i64;
    let user_prompt = Message {
        id: None,
        idx: 0,
        role: MessageRole::User,
        author: None,
        created_at: Some(turn_ts),
        content: "check both configs".into(),
        extra_json: serde_json::json!({"message_id": "m0", "is_user_input": true}),
        snippets: Vec::new(),
    };
    let assistant_call = Message {
        id: None,
        idx: 1,
        role: MessageRole::Agent,
        author: None,
        created_at: Some(turn_ts),
        content: "[tool_call: read_file]\n[tool_call: read_file]".into(),
        extra_json: serde_json::json!({
            "message_id": "m1",
            "cass": {"invocations": [
                {"kind": "tool", "name": "read_file", "call_id": "tc_a",
                 "arguments": {"path": "/repo/a.toml"}},
                {"kind": "tool", "name": "read_file", "call_id": "tc_b",
                 "arguments": {"path": "/repo/b.toml"}}]}
        }),
        snippets: Vec::new(),
    };
    let tool_result = |idx: i64, call_id: &str| Message {
        id: None,
        idx,
        role: MessageRole::Tool,
        author: None,
        created_at: Some(turn_ts),
        content: "0 matches\n".into(),
        extra_json: serde_json::json!({
            "message_id": format!("m{idx}"),
            "tool_call_id": call_id
        }),
        snippets: Vec::new(),
    };

    fn stored_message_count(storage: &SqliteStorage, conversation_id: i64) -> i64 {
        storage
            .conn
            .query_row_map(
                "SELECT COUNT(*) FROM messages WHERE conversation_id = ?1",
                fparams![conversation_id],
                |row| row.get_typed(0),
            )
            .unwrap()
    }

    // First ingest: both duplicate-text results persist as distinct
    // records because their top-level ids are distinct identities.
    let first = storage
        .insert_conversation_tree(
            agent_id,
            None,
            &make_conv(vec![
                user_prompt.clone(),
                assistant_call.clone(),
                tool_result(2, "tc_a"),
                tool_result(3, "tc_b"),
            ]),
        )
        .unwrap();
    assert_eq!(first.inserted_indices, vec![0, 1, 2, 3]);
    assert_eq!(stored_message_count(&storage, first.conversation_id), 4);

    // Identical re-ingest: every record still stored exactly once.
    let second = storage
        .insert_conversation_tree(
            agent_id,
            None,
            &make_conv(vec![
                user_prompt,
                assistant_call,
                tool_result(2, "tc_a"),
                tool_result(3, "tc_b"),
            ]),
        )
        .unwrap();
    assert!(second.inserted_indices.is_empty());
    assert_eq!(stored_message_count(&storage, first.conversation_id), 4);
}

#[test]
fn insert_conversation_tree_replays_timestamped_tool_results_through_shared_tool_call_ids() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let agent = Agent {
        id: None,
        slug: "devin".into(),
        name: "Devin".into(),
        version: None,
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();

    let make_conv = |messages: Vec<Message>| Conversation {
        id: None,
        agent_slug: "devin".into(),
        workspace: None,
        external_id: Some("devin-session-286".into()),
        title: Some("Devin same-id replay".into()),
        source_path: PathBuf::from("/tmp/devin-sessions.db"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_000),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages,
        source_id: "local".into(),
        origin_host: None,
    };
    let turn_ts = 1_700_000_000_000i64;
    let tool_result = |idx: i64, call_id: &str| Message {
        id: None,
        idx,
        role: MessageRole::Tool,
        author: None,
        created_at: Some(turn_ts),
        content: "0 matches\n".into(),
        extra_json: serde_json::json!({
            "message_id": format!("m{idx}"),
            "tool_call_id": call_id
        }),
        snippets: Vec::new(),
    };

    fn stored_message_count(storage: &SqliteStorage, conversation_id: i64) -> i64 {
        storage
            .conn
            .query_row_map(
                "SELECT COUNT(*) FROM messages WHERE conversation_id = ?1",
                fparams![conversation_id],
                |row| row.get_typed(0),
            )
            .unwrap()
    }

    // Two timestamped rows sharing the core AND the top-level id: the
    // second is a genuine replay of the first, never a second record.
    let first = storage
        .insert_conversation_tree(
            agent_id,
            None,
            &make_conv(vec![tool_result(0, "tc_same"), tool_result(1, "tc_same")]),
        )
        .unwrap();
    assert_eq!(first.inserted_indices, vec![0]);
    assert_eq!(stored_message_count(&storage, first.conversation_id), 1);

    // Re-ingest at a shifted idx with the same id: still one row.
    let second = storage
        .insert_conversation_tree(agent_id, None, &make_conv(vec![tool_result(10, "tc_same")]))
        .unwrap();
    assert!(second.inserted_indices.is_empty());
    assert_eq!(stored_message_count(&storage, first.conversation_id), 1);

    // A distinct top-level id is a distinct record, even at a shifted
    // idx with an identical (created_at, role, content) core.
    let third = storage
        .insert_conversation_tree(
            agent_id,
            None,
            &make_conv(vec![tool_result(11, "tc_other")]),
        )
        .unwrap();
    assert_eq!(third.inserted_indices, vec![11]);
    assert_eq!(stored_message_count(&storage, first.conversation_id), 2);
}

#[test]
fn insert_conversation_tree_refills_holes_left_by_the_legacy_timestamped_replay_skip() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let agent = Agent {
        id: None,
        slug: "devin".into(),
        name: "Devin".into(),
        version: None,
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();

    // An archive written before the top-level `tool_call_id` was read
    // carries only the first row of a duplicate-text burst (the legacy
    // wildcard replay skip collapsed the rest). A re-parse from source
    // re-offers the full tree; the holes must refill because the stored
    // sibling's id no longer anchors a record with a distinct id.
    let make_conv = |messages: Vec<Message>| Conversation {
        id: None,
        agent_slug: "devin".into(),
        workspace: None,
        external_id: Some("devin-session-278".into()),
        title: Some("Devin burst heal".into()),
        source_path: PathBuf::from("/tmp/devin-sessions.db"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_000),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages,
        source_id: "local".into(),
        origin_host: None,
    };
    let turn_ts = 1_700_000_000_000i64;
    let tool_result = |idx: i64, call_id: &str| Message {
        id: None,
        idx,
        role: MessageRole::Tool,
        author: None,
        created_at: Some(turn_ts),
        content: "0 matches\n".into(),
        extra_json: serde_json::json!({
            "message_id": format!("m{idx}"),
            "tool_call_id": call_id
        }),
        snippets: Vec::new(),
    };

    // The legacy-archive shape: the burst collapsed to its first row.
    let first = storage
        .insert_conversation_tree(agent_id, None, &make_conv(vec![tool_result(2, "tc_a")]))
        .unwrap();
    assert_eq!(first.inserted_indices, vec![2]);

    // The re-parse offers the full burst at the source's idx values.
    let second = storage
        .insert_conversation_tree(
            agent_id,
            None,
            &make_conv(vec![
                tool_result(2, "tc_a"),
                tool_result(3, "tc_b"),
                tool_result(4, "tc_c"),
            ]),
        )
        .unwrap();
    assert_eq!(second.inserted_indices, vec![3, 4]);
    let stored: Vec<i64> = storage
        .conn
        .query_map_collect(
            "SELECT idx FROM messages WHERE conversation_id = ?1 ORDER BY idx",
            fparams![first.conversation_id],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(stored, vec![2, 3, 4]);
}
