use super::*;
use crate::franken_sync::compat::{ConnectionExt as FrankenConnectionExt, RowExt as FrankenRowExt};
use crate::sources::provenance::LOCAL_SOURCE_ID;
use tempfile::TempDir;

#[test]
fn fetch_messages_for_lexical_rebuild_skips_extra_json() {
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

    let conversation = Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: Some("lexical-rebuild-test".into()),
        title: Some("Lexical rebuild".into()),
        source_path: PathBuf::from("/tmp/lexical-rebuild.jsonl"),
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
            content: "indexed text".into(),
            extra_json: serde_json::json!({
                "usage": { "total_tokens": 1234 },
                "irrelevant_blob": "still preserved in canonical storage"
            }),
            snippets: Vec::new(),
        }],
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };

    let inserted = storage
        .insert_conversation_tree(agent_id, None, &conversation)
        .unwrap();
    let conversation_id = inserted.conversation_id;

    let stored = storage.fetch_messages(conversation_id).unwrap();
    assert_eq!(stored.len(), 1);
    assert!(!stored[0].extra_json.is_null());

    let lexical = storage
        .fetch_messages_for_lexical_rebuild(conversation_id)
        .unwrap();
    assert_eq!(lexical.len(), 1);
    assert_eq!(lexical[0].content, "indexed text");
    assert_eq!(lexical[0].author.as_deref(), Some("assistant"));
    assert!(lexical[0].extra_json.is_null());
}

#[test]
fn fetch_messages_for_lexical_rebuild_batch_groups_and_orders_messages() {
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

    let first = Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: Some("lexical-batch-1".into()),
        title: Some("Lexical batch 1".into()),
        source_path: PathBuf::from("/tmp/lexical-batch-1.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_100),
        approx_tokens: Some(42),
        metadata_json: serde_json::Value::Null,
        messages: vec![
            Message {
                id: None,
                idx: 0,
                role: MessageRole::User,
                author: Some("user".into()),
                created_at: Some(1_700_000_000_010),
                content: "first-a".into(),
                extra_json: serde_json::json!({"opaque": true}),
                snippets: Vec::new(),
            },
            Message {
                id: None,
                idx: 1,
                role: MessageRole::Agent,
                author: Some("assistant".into()),
                created_at: Some(1_700_000_000_020),
                content: "first-b".into(),
                extra_json: serde_json::json!({"opaque": true}),
                snippets: Vec::new(),
            },
        ],
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };

    let second = Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: Some("lexical-batch-2".into()),
        title: Some("Lexical batch 2".into()),
        source_path: PathBuf::from("/tmp/lexical-batch-2.jsonl"),
        started_at: Some(1_700_000_000_200),
        ended_at: Some(1_700_000_000_300),
        approx_tokens: Some(84),
        metadata_json: serde_json::Value::Null,
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::Tool,
            author: Some("tool".into()),
            created_at: Some(1_700_000_000_210),
            content: "second-a".into(),
            extra_json: serde_json::json!({"opaque": true}),
            snippets: Vec::new(),
        }],
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };
    let third = Conversation {
        external_id: Some("lexical-batch-3".into()),
        title: Some("Lexical batch 3".into()),
        source_path: PathBuf::from("/tmp/lexical-batch-3.jsonl"),
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::System,
            author: Some("system".into()),
            created_at: Some(1_700_000_000_410),
            content: "third-a".into(),
            extra_json: serde_json::json!({"opaque": true}),
            snippets: Vec::new(),
        }],
        ..second.clone()
    };

    let first_id = storage
        .insert_conversation_tree(agent_id, None, &first)
        .unwrap()
        .conversation_id;
    let second_id = storage
        .insert_conversation_tree(agent_id, None, &second)
        .unwrap()
        .conversation_id;
    let third_id = storage
        .insert_conversation_tree(agent_id, None, &third)
        .unwrap()
        .conversation_id;

    let lexical = storage
        .fetch_messages_for_lexical_rebuild_batch(&[third_id, first_id], None, None)
        .unwrap();

    let first_messages = lexical.get(&first_id).expect("first conversation");
    assert_eq!(first_messages.len(), 2);
    assert_eq!(first_messages[0].content, "first-a");
    assert_eq!(first_messages[1].content, "first-b");
    assert!(
        first_messages
            .iter()
            .all(|message| message.extra_json.is_null())
    );

    assert!(
        !lexical.contains_key(&second_id),
        "batch fetch must exclude conversations not requested by the caller"
    );

    let third_messages = lexical.get(&third_id).expect("third conversation");
    assert_eq!(third_messages.len(), 1);
    assert_eq!(third_messages[0].content, "third-a");
    assert!(third_messages[0].extra_json.is_null());
}

#[test]
fn fetch_messages_for_lexical_rebuild_batch_enforces_content_byte_guardrail() {
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

    let conversation = Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: Some("lexical-batch-guard".into()),
        title: Some("Lexical batch guard".into()),
        source_path: PathBuf::from("/tmp/lexical-batch-guard.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_100),
        approx_tokens: Some(42),
        metadata_json: serde_json::Value::Null,
        messages: vec![
            Message {
                id: None,
                idx: 0,
                role: MessageRole::User,
                author: Some("user".into()),
                created_at: Some(1_700_000_000_010),
                content: "123456".into(),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            },
            Message {
                id: None,
                idx: 1,
                role: MessageRole::Agent,
                author: Some("assistant".into()),
                created_at: Some(1_700_000_000_020),
                content: "abcdef".into(),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            },
        ],
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };

    let conversation_id = storage
        .insert_conversation_tree(agent_id, None, &conversation)
        .unwrap()
        .conversation_id;

    let error = storage
        .fetch_messages_for_lexical_rebuild_batch(&[conversation_id], Some(10), Some(8))
        .expect_err("guardrail should reject oversized batch content");

    let message = format!("{error:#}");
    assert!(
        message.contains("content-byte guardrail"),
        "expected guardrail reason in error, got {message}"
    );
}

#[test]
fn fetch_messages_handles_manual_rows_inserted_via_raw_connection() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("manual-rows.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    let conn = storage.raw();

    conn.execute(
        "INSERT INTO agents (id, slug, name, kind, created_at, updated_at)
         VALUES (1, 'claude_code', 'Claude Code', 'local', 0, 0)",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO conversations
         (id, agent_id, external_id, title, source_path, source_id, started_at)
         VALUES (1, 1, 'manual-ext', 'Manual Session', '/tmp/manual.jsonl', 'local', 200)",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO messages
         (id, conversation_id, idx, role, author, created_at, content, extra_json, extra_bin)
         VALUES (1, 1, 0, 'user', 'tester', 1700000000000, 'manual body', '{\"k\":1}', NULL)",
    )
    .unwrap();

    let lexical = storage.fetch_messages_for_lexical_rebuild(1).unwrap();
    assert_eq!(lexical.len(), 1);
    assert_eq!(lexical[0].content, "manual body");

    let full = storage.fetch_messages(1).unwrap();
    assert_eq!(full.len(), 1);
    assert_eq!(full[0].content, "manual body");
    assert_eq!(full[0].author.as_deref(), Some("tester"));
    assert_eq!(full[0].extra_json, serde_json::json!({ "k": 1 }));
}

#[test]
fn lexical_rebuild_batch_messages_query_avoids_sorter_temp_btrees() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("agent_search.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let agent = Agent {
        id: None,
        slug: "claude_code".into(),
        name: "Claude Code".into(),
        version: None,
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();

    for (external_id, base_ts) in [
        ("conv-1", 1_700_000_000_000_i64),
        ("conv-2", 1_700_000_001_000_i64),
    ] {
        let conversation = Conversation {
            id: None,
            agent_slug: "claude_code".into(),
            workspace: Some(PathBuf::from("/tmp/workspace")),
            external_id: Some(external_id.to_string()),
            title: Some("Lexical rebuild".into()),
            source_path: PathBuf::from(format!("/tmp/{external_id}.jsonl")),
            started_at: Some(base_ts),
            ended_at: Some(base_ts + 100),
            approx_tokens: None,
            metadata_json: serde_json::Value::Null,
            messages: vec![
                Message {
                    id: None,
                    idx: 0,
                    role: MessageRole::User,
                    author: Some("user".into()),
                    created_at: Some(base_ts + 10),
                    content: format!("{external_id}-first"),
                    extra_json: serde_json::Value::Null,
                    snippets: Vec::new(),
                },
                Message {
                    id: None,
                    idx: 1,
                    role: MessageRole::Agent,
                    author: Some("assistant".into()),
                    created_at: Some(base_ts + 20),
                    content: format!("{external_id}-second"),
                    extra_json: serde_json::Value::Null,
                    snippets: Vec::new(),
                },
            ],
            source_id: LOCAL_SOURCE_ID.into(),
            origin_host: None,
        };
        storage
            .insert_conversation_tree(agent_id, None, &conversation)
            .unwrap();
    }

    let conversation_ids: Vec<i64> = storage
        .conn
        .query_map_collect(
            "SELECT id FROM conversations ORDER BY id",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(conversation_ids.len(), 2);

    let plan_details: Vec<String> = storage
        .conn
        .query_map_collect(
            "EXPLAIN QUERY PLAN \
             SELECT conversation_id, id, idx, role, author, created_at, content \
             FROM messages \
             WHERE conversation_id IN (?1, ?2) \
             ORDER BY conversation_id ASC, idx ASC",
            fparams![conversation_ids[0], conversation_ids[1]],
            |row| row.get_typed(3),
        )
        .unwrap();

    assert!(
        plan_details
            .iter()
            .any(|detail| detail.contains("sqlite_autoindex_messages_1")),
        "expected batched lexical rebuild fetch to use the conversation_id/idx composite index, got {plan_details:?}"
    );
    assert!(
        !plan_details
            .iter()
            .any(|detail| detail.contains("TEMP B-TREE")),
        "expected batched lexical rebuild fetch to avoid sorter temp b-trees, got {plan_details:?}"
    );
}

#[test]
fn stream_messages_for_lexical_rebuild_groups_and_orders_messages() {
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

    let first = Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: Some("lexical-stream-1".into()),
        title: Some("Lexical stream 1".into()),
        source_path: PathBuf::from("/tmp/lexical-stream-1.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_100),
        approx_tokens: Some(42),
        metadata_json: serde_json::Value::Null,
        messages: vec![
            Message {
                id: None,
                idx: 0,
                role: MessageRole::User,
                author: Some("user".into()),
                created_at: Some(1_700_000_000_010),
                content: "first-a".into(),
                extra_json: serde_json::json!({"opaque": true}),
                snippets: Vec::new(),
            },
            Message {
                id: None,
                idx: 1,
                role: MessageRole::Agent,
                author: Some("assistant".into()),
                created_at: Some(1_700_000_000_020),
                content: "first-b".into(),
                extra_json: serde_json::json!({"opaque": true}),
                snippets: Vec::new(),
            },
        ],
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };

    let second = Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: Some("lexical-stream-2".into()),
        title: Some("Lexical stream 2".into()),
        source_path: PathBuf::from("/tmp/lexical-stream-2.jsonl"),
        started_at: Some(1_700_000_000_200),
        ended_at: Some(1_700_000_000_300),
        approx_tokens: Some(84),
        metadata_json: serde_json::Value::Null,
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::Tool,
            author: Some("tool".into()),
            created_at: Some(1_700_000_000_210),
            content: "second-a".into(),
            extra_json: serde_json::json!({"opaque": true}),
            snippets: Vec::new(),
        }],
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };

    let first_id = storage
        .insert_conversation_tree(agent_id, None, &first)
        .unwrap()
        .conversation_id;
    let second_id = storage
        .insert_conversation_tree(agent_id, None, &second)
        .unwrap()
        .conversation_id;

    let mut streamed = Vec::new();
    storage
        .stream_messages_for_lexical_rebuild_from_conversation_id(first_id, |row| {
            streamed.push((
                row.conversation_id,
                row.idx,
                row.role,
                row.author,
                row.content,
            ));
            Ok(())
        })
        .unwrap();

    assert_eq!(
        streamed,
        vec![
            (
                first_id,
                0,
                "user".to_string(),
                Some("user".to_string()),
                "first-a".to_string(),
            ),
            (
                first_id,
                1,
                "agent".to_string(),
                Some("assistant".to_string()),
                "first-b".to_string(),
            ),
            (
                second_id,
                0,
                "tool".to_string(),
                Some("tool".to_string()),
                "second-a".to_string(),
            ),
        ]
    );
}

#[test]
fn stream_messages_for_lexical_rebuild_between_conversation_ids_respects_upper_bound() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("agent_search.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let agent = Agent {
        id: None,
        slug: "claude_code".into(),
        name: "Claude Code".into(),
        version: Some("1.2.3".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();

    let first = Conversation {
        id: None,
        agent_slug: "claude_code".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: Some("lexical-range-1".into()),
        title: Some("Lexical range 1".into()),
        source_path: PathBuf::from("/tmp/lexical-range-1.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_100),
        approx_tokens: Some(42),
        metadata_json: serde_json::Value::Null,
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: Some("user".into()),
            created_at: Some(1_700_000_000_010),
            content: "first-only".into(),
            extra_json: serde_json::json!({"opaque": true}),
            snippets: Vec::new(),
        }],
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };

    let second = Conversation {
        id: None,
        agent_slug: "claude_code".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: Some("lexical-range-2".into()),
        title: Some("Lexical range 2".into()),
        source_path: PathBuf::from("/tmp/lexical-range-2.jsonl"),
        started_at: Some(1_700_000_000_200),
        ended_at: Some(1_700_000_000_300),
        approx_tokens: Some(84),
        metadata_json: serde_json::Value::Null,
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::Tool,
            author: Some("tool".into()),
            created_at: Some(1_700_000_000_210),
            content: "second-should-not-appear".into(),
            extra_json: serde_json::json!({"opaque": true}),
            snippets: Vec::new(),
        }],
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };

    let first_id = storage
        .insert_conversation_tree(agent_id, None, &first)
        .unwrap()
        .conversation_id;
    let second_id = storage
        .insert_conversation_tree(agent_id, None, &second)
        .unwrap()
        .conversation_id;

    let mut streamed = Vec::new();
    storage
        .stream_messages_for_lexical_rebuild_between_conversation_ids(first_id, first_id, |row| {
            streamed.push((row.conversation_id, row.idx, row.content));
            Ok(())
        })
        .unwrap();

    assert_eq!(streamed, vec![(first_id, 0, "first-only".to_string())]);
    assert!(
        streamed
            .iter()
            .all(|(conversation_id, _, _)| *conversation_id != second_id),
        "upper bound should exclude later conversation ids"
    );
}

#[test]
fn stream_messages_for_lexical_rebuild_between_conversation_ids_handles_mixed_ranges() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("agent_search.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let claude_agent_id = storage
        .ensure_agent(&Agent {
            id: None,
            slug: "claude_code".into(),
            name: "Claude Code".into(),
            version: None,
            kind: AgentKind::Cli,
        })
        .unwrap();
    let aider_agent_id = storage
        .ensure_agent(&Agent {
            id: None,
            slug: "aider".into(),
            name: "Aider".into(),
            version: None,
            kind: AgentKind::Cli,
        })
        .unwrap();

    type MessageSpec = (i64, MessageRole, Option<String>, Option<i64>, String);

    let mut expected = Vec::new();
    let mut first_conversation_id = None;
    let mut last_conversation_id = None;
    let mut insert_conversation =
        |agent_id: i64,
         external_id: &str,
         title: &str,
         source_path: &str,
         started_at: i64,
         message_specs: Vec<MessageSpec>| {
            let conversation = Conversation {
                id: None,
                agent_slug: if agent_id == aider_agent_id {
                    "aider".into()
                } else {
                    "claude_code".into()
                },
                workspace: Some(PathBuf::from("/tmp/workspace")),
                external_id: Some(external_id.to_string()),
                title: Some(title.to_string()),
                source_path: PathBuf::from(source_path),
                started_at: Some(started_at),
                ended_at: Some(started_at + 100),
                approx_tokens: None,
                metadata_json: serde_json::Value::Null,
                messages: message_specs
                    .iter()
                    .map(|(idx, role, author, created_at, content)| Message {
                        id: None,
                        idx: *idx,
                        role: role.clone(),
                        author: author.clone(),
                        created_at: *created_at,
                        content: content.clone(),
                        extra_json: serde_json::Value::Null,
                        snippets: Vec::new(),
                    })
                    .collect(),
                source_id: LOCAL_SOURCE_ID.into(),
                origin_host: None,
            };
            let conversation_id = storage
                .insert_conversation_tree(agent_id, None, &conversation)
                .unwrap()
                .conversation_id;
            if first_conversation_id.is_none() {
                first_conversation_id = Some(conversation_id);
            }
            last_conversation_id = Some(conversation_id);
            expected.extend(message_specs.into_iter().map(
                |(idx, role, author, created_at, content)| {
                    (
                        conversation_id,
                        idx,
                        match role {
                            MessageRole::User => "user".to_string(),
                            MessageRole::Agent => "agent".to_string(),
                            MessageRole::Tool => "tool".to_string(),
                            MessageRole::System => "system".to_string(),
                            MessageRole::Other(other) => other,
                        },
                        author,
                        created_at,
                        content,
                    )
                },
            ));
        };

    for (label, base_ts) in [
        ("alpha", 1_700_000_000_000_i64),
        ("beta", 1_700_000_001_000_i64),
        ("gamma", 1_700_000_002_000_i64),
        ("delta", 1_700_000_003_000_i64),
        ("epsilon", 1_700_000_004_000_i64),
    ] {
        insert_conversation(
            claude_agent_id,
            &format!("lexical-{label}"),
            &format!("Lexical {label}"),
            &format!("/tmp/{label}.jsonl"),
            base_ts,
            vec![
                (
                    0,
                    MessageRole::User,
                    None,
                    Some(base_ts + 10),
                    format!("{label}_content"),
                ),
                (
                    1,
                    MessageRole::Agent,
                    None,
                    Some(base_ts + 20),
                    format!("{label}_content_response"),
                ),
            ],
        );
    }

    insert_conversation(
        aider_agent_id,
        "lexical-aider-history",
        "Aider Chat: coding_agent_session_search",
        "/tmp/.aider.chat.history.md",
        1_764_619_673_394,
        vec![
            (
                0,
                MessageRole::System,
                Some("system".to_string()),
                None,
                "# aider chat started at 2025-12-01 20:07:47".to_string(),
            ),
            (
                1,
                MessageRole::User,
                Some("user".to_string()),
                None,
                "/tmp/workspace/.venv/bin/aider --no-git --message hello world".to_string(),
            ),
        ],
    );
    insert_conversation(
        aider_agent_id,
        "lexical-aider-fixture",
        "Aider Chat: aider",
        "/tmp/tests/fixtures/aider/.aider.chat.history.md",
        1_764_621_401_399,
        vec![
            (
                0,
                MessageRole::User,
                Some("user".to_string()),
                None,
                "/add src/main.rs".to_string(),
            ),
            (
                1,
                MessageRole::Agent,
                Some("assistant".to_string()),
                None,
                "Added src/main.rs to the chat.

#### /add src/main.rs"
                    .to_string(),
            ),
            (
                2,
                MessageRole::User,
                Some("user".to_string()),
                None,
                "Please refactor.".to_string(),
            ),
            (
                3,
                MessageRole::Agent,
                Some("assistant".to_string()),
                None,
                "Sure, here is the code.".to_string(),
            ),
        ],
    );

    let mut streamed = Vec::new();
    storage
        .stream_messages_for_lexical_rebuild_between_conversation_ids(
            first_conversation_id.unwrap(),
            last_conversation_id.unwrap(),
            |row| {
                streamed.push((
                    row.conversation_id,
                    row.idx,
                    row.role,
                    row.author,
                    row.created_at,
                    row.content,
                ));
                Ok(())
            },
        )
        .unwrap();

    assert_eq!(streamed, expected);
}

#[test]
fn lexical_rebuild_stream_queries_use_rowid_and_per_conversation_probes() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("agent_search.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let agent = Agent {
        id: None,
        slug: "claude_code".into(),
        name: "Claude Code".into(),
        version: None,
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();

    for (external_id, base_ts) in [
        ("conv-1", 1_700_000_000_000_i64),
        ("conv-2", 1_700_000_001_000_i64),
    ] {
        let conversation = Conversation {
            id: None,
            agent_slug: "claude_code".into(),
            workspace: Some(PathBuf::from("/tmp/workspace")),
            external_id: Some(external_id.to_string()),
            title: Some("Lexical rebuild".into()),
            source_path: PathBuf::from(format!("/tmp/{external_id}.jsonl")),
            started_at: Some(base_ts),
            ended_at: Some(base_ts + 100),
            approx_tokens: None,
            metadata_json: serde_json::Value::Null,
            messages: vec![
                Message {
                    id: None,
                    idx: 0,
                    role: MessageRole::User,
                    author: Some("user".into()),
                    created_at: Some(base_ts + 10),
                    content: format!("{external_id}-first"),
                    extra_json: serde_json::Value::Null,
                    snippets: Vec::new(),
                },
                Message {
                    id: None,
                    idx: 1,
                    role: MessageRole::Agent,
                    author: Some("assistant".into()),
                    created_at: Some(base_ts + 20),
                    content: format!("{external_id}-second"),
                    extra_json: serde_json::Value::Null,
                    snippets: Vec::new(),
                },
            ],
            source_id: LOCAL_SOURCE_ID.into(),
            origin_host: None,
        };
        storage
            .insert_conversation_tree(agent_id, None, &conversation)
            .unwrap();
    }

    let first_id: i64 = storage
        .conn
        .query_row_map(
            "SELECT id FROM conversations ORDER BY id LIMIT 1",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    let last_id: i64 = storage
        .conn
        .query_row_map(
            "SELECT id FROM conversations ORDER BY id DESC LIMIT 1",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();

    let conversation_plan_details: Vec<String> = storage
        .conn
        .query_map_collect(
            "EXPLAIN QUERY PLAN                  SELECT id FROM conversations                  WHERE id >= ?1 AND id <= ?2                  ORDER BY id ASC",
            fparams![first_id, last_id],
            |row| row.get_typed(3),
        )
        .unwrap();
    assert!(
        !conversation_plan_details
            .iter()
            .any(|detail| detail.contains("TEMP B-TREE")),
        "expected streamed lexical rebuild conversation listing to avoid sorter temp b-trees, got {conversation_plan_details:?}"
    );

    let message_plan_details: Vec<String> = storage
        .conn
        .query_map_collect(
            "EXPLAIN QUERY PLAN                  SELECT id, idx, role, author, created_at, content                  FROM messages INDEXED BY sqlite_autoindex_messages_1                  WHERE conversation_id = ?1                  ORDER BY idx",
            fparams![first_id],
            |row| row.get_typed(3),
        )
        .unwrap();
    assert!(
        message_plan_details
            .iter()
            .any(|detail| detail.contains("sqlite_autoindex_messages_1")
                || detail.contains("idx_messages_conv_idx")),
        "expected per-conversation lexical rebuild fetch to use the conversation_id/idx index, got {message_plan_details:?}"
    );
    assert!(
        !message_plan_details
            .iter()
            .any(|detail| detail.contains("TEMP B-TREE")),
        "expected per-conversation lexical rebuild fetch to avoid sorter temp b-trees, got {message_plan_details:?}"
    );
}
