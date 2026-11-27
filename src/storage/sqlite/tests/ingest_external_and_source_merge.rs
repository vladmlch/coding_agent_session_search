use super::*;
use crate::franken_sync::compat::{ConnectionExt as FrankenConnectionExt, RowExt as FrankenRowExt};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;

#[test]
fn insert_conversations_batched_appends_duplicate_external_id() {
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

    let base_conv = |messages: Vec<Message>| Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: Some("shared-session".into()),
        title: Some("Shared Session".into()),
        source_path: PathBuf::from("/tmp/rollout.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_999),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages,
        source_id: "local".into(),
        origin_host: None,
    };

    let conv_a = base_conv(vec![
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
    ]);
    let conv_b = base_conv(vec![
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
        Message {
            id: None,
            idx: 3,
            role: MessageRole::Agent,
            author: None,
            created_at: Some(1_700_000_000_300),
            content: "fourth".into(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        },
    ]);

    let outcomes = storage
        .insert_conversations_batched(&[(agent_id, None, &conv_a), (agent_id, None, &conv_b)])
        .unwrap();
    assert_eq!(outcomes.len(), 2);
    assert_eq!(outcomes[0].inserted_indices, vec![0, 1]);
    assert_eq!(outcomes[1].inserted_indices, vec![2, 3]);
    assert_eq!(outcomes[0].conversation_id, outcomes[1].conversation_id);

    let conversation_count: i64 = storage
        .conn
        .query_row_map("SELECT COUNT(*) FROM conversations", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    let conversation_count_not_indexed: i64 = storage
        .conn
        .query_row_map(
            "SELECT COUNT(*) FROM conversations NOT INDEXED",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    let conversation_count_source_index: i64 = storage
        .conn
        .query_row_map(
            "SELECT COUNT(*) FROM conversations INDEXED BY idx_conversations_source_id",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    let message_count: i64 = storage
        .conn
        .query_row_map("SELECT COUNT(*) FROM messages", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    let reopened_storage = SqliteStorage::open(&db_path).unwrap();
    let reopened_conversation_count: i64 = reopened_storage
        .conn
        .query_row_map("SELECT COUNT(*) FROM conversations", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    let reopened_conversation_count_not_indexed: i64 = reopened_storage
        .conn
        .query_row_map(
            "SELECT COUNT(*) FROM conversations NOT INDEXED",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    let reopened_conversation_ids: Vec<i64> = reopened_storage
        .conn
        .query_map_collect(
            "SELECT id FROM conversations ORDER BY id",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    let reopened_conversation_ids_not_indexed: Vec<i64> = reopened_storage
        .conn
        .query_map_collect(
            "SELECT id FROM conversations NOT INDEXED ORDER BY id",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    let reopened_conversation_ids_source_index: Vec<i64> = reopened_storage
        .conn
        .query_map_collect(
            "SELECT id FROM conversations INDEXED BY idx_conversations_source_id ORDER BY id",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();

    assert_eq!(reopened_conversation_ids, vec![outcomes[0].conversation_id]);
    assert_eq!(
        reopened_conversation_ids_not_indexed,
        vec![outcomes[0].conversation_id]
    );
    assert_eq!(
        reopened_conversation_ids_source_index,
        vec![outcomes[0].conversation_id]
    );
    assert_eq!(reopened_conversation_count, 1);
    assert_eq!(reopened_conversation_count_not_indexed, 1);
    assert_eq!(conversation_count_not_indexed, 1);
    assert_eq!(conversation_count_source_index, 1);
    assert_eq!(conversation_count, 1);
    assert_eq!(message_count, 4);
}

#[test]
fn franken_insert_conversation_or_get_existing_recovers_unique_conflict() {
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

    let conv = Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: Some("recover-duplicate".into()),
        title: Some("Recover Duplicate".into()),
        source_path: PathBuf::from("/tmp/rollout.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_100),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: None,
            created_at: Some(1_700_000_000_000),
            content: "hello".into(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        source_id: "local".into(),
        origin_host: None,
    };

    let tx = storage
        .conn
        .transaction_with_behavior(crate::franken_sync::TransactionBehavior::Immediate)
        .unwrap();
    let inserted_id = franken_insert_conversation(&tx, agent_id, None, &conv)
        .unwrap()
        .expect("first insert should succeed");

    let conversation_key = conversation_merge_key(agent_id, &conv);
    let resolved = franken_insert_conversation_or_get_existing_after_miss(
        &tx,
        agent_id,
        None,
        &conv,
        &conversation_key,
    )
    .unwrap();

    assert!(
        matches!(
            resolved,
            ConversationInsertStatus::Existing(existing_id)
                if existing_id.cmp(&inserted_id).is_eq()
        ),
        "expected existing conversation id {inserted_id}"
    );

    let conversation_count: i64 = tx
        .query_row_map("SELECT COUNT(*) FROM conversations", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(conversation_count, 1);
}

#[test]
fn insert_conversations_batched_merges_duplicate_external_id_with_gaps() {
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

    let base_conv = |messages: Vec<Message>| Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: Some("shared-session-gap".into()),
        title: Some("Shared Session Gap".into()),
        source_path: PathBuf::from("/tmp/rollout.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_999),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages,
        source_id: "local".into(),
        origin_host: None,
    };

    let conv_a = base_conv(vec![
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
        Message {
            id: None,
            idx: 3,
            role: MessageRole::Agent,
            author: None,
            created_at: Some(1_700_000_000_300),
            content: "fourth".into(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        },
    ]);
    let conv_b = base_conv(vec![
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
        Message {
            id: None,
            idx: 3,
            role: MessageRole::Agent,
            author: None,
            created_at: Some(1_700_000_000_300),
            content: "fourth".into(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        },
    ]);

    let outcomes = storage
        .insert_conversations_batched(&[(agent_id, None, &conv_a), (agent_id, None, &conv_b)])
        .unwrap();
    assert_eq!(outcomes.len(), 2);
    assert_eq!(outcomes[0].inserted_indices, vec![2, 3]);
    assert_eq!(outcomes[1].inserted_indices, vec![0, 1]);
    assert_eq!(outcomes[0].conversation_id, outcomes[1].conversation_id);

    let stored_indices: Vec<i64> = storage
        .conn
        .query_map_collect("SELECT idx FROM messages ORDER BY idx", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(stored_indices, vec![0, 1, 2, 3]);
}

#[test]
fn insert_conversations_batched_refreshes_partial_pending_message_lookup() {
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

    let make_message = |idx: i64, content: &str| Message {
        id: None,
        idx,
        role: if idx == 0 {
            MessageRole::User
        } else {
            MessageRole::Agent
        },
        author: None,
        created_at: Some(1_700_000_000_000 + idx),
        content: content.into(),
        extra_json: serde_json::Value::Null,
        snippets: Vec::new(),
    };

    let base_conv = |messages: Vec<Message>| Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: Some("partial-cache-session".into()),
        title: Some("Partial cache session".into()),
        source_path: PathBuf::from("/tmp/partial-cache.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_100),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages,
        source_id: "local".into(),
        origin_host: None,
    };

    let canonical = base_conv(vec![
        make_message(0, "canonical zero"),
        make_message(20, "canonical twenty"),
    ]);
    storage
        .insert_conversation_tree(agent_id, None, &canonical)
        .unwrap();

    let exact_prefix = base_conv(vec![make_message(0, "canonical zero")]);
    let conflicting_tail = base_conv(vec![make_message(20, "conflicting twenty")]);

    let outcomes = storage
        .insert_conversations_batched(&[
            (agent_id, None, &exact_prefix),
            (agent_id, None, &conflicting_tail),
        ])
        .unwrap();

    assert_eq!(outcomes.len(), 2);
    assert!(outcomes[0].inserted_indices.is_empty());
    assert!(
        outcomes[1].inserted_indices.is_empty(),
        "the second batch item must refresh the partial pending lookup and retain the canonical idx=20 row"
    );

    let stored_messages: Vec<(i64, String)> = storage
        .conn
        .query_map_collect(
            "SELECT idx, content FROM messages ORDER BY idx",
            fparams![],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
        )
        .unwrap();
    assert_eq!(
        stored_messages,
        vec![
            (0, "canonical zero".to_string()),
            (20, "canonical twenty".to_string()),
        ]
    );
}

#[test]
fn insert_conversations_batched_reprocessing_conversation_is_idempotent() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    const MESSAGE_COUNT: i64 = 64;

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

    let messages: Vec<Message> = (0..MESSAGE_COUNT)
        .map(|idx| Message {
            id: None,
            idx,
            role: if idx % 2 == 0 {
                MessageRole::User
            } else {
                MessageRole::Agent
            },
            author: None,
            created_at: Some(1_700_000_000_000 + idx),
            content: format!("message {idx}"),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        })
        .collect();

    let conversation = Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: Some("large-reprocess-session".into()),
        title: Some("Large Reprocess Session".into()),
        source_path: PathBuf::from("/tmp/large-reprocess-session.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_000 + MESSAGE_COUNT - 1),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages,
        source_id: "local".into(),
        origin_host: None,
    };

    let first = storage
        .insert_conversations_batched(&[(agent_id, None, &conversation)])
        .unwrap();
    let second = storage
        .insert_conversations_batched(&[(agent_id, None, &conversation)])
        .unwrap();

    assert_eq!(first.len(), 1);
    assert_eq!(second.len(), 1);
    assert_eq!(first[0].inserted_indices.len(), MESSAGE_COUNT as usize);
    assert!(
        second[0].inserted_indices.is_empty(),
        "full reprocessing of a large conversation must not attempt duplicate idx inserts"
    );
    assert_eq!(first[0].conversation_id, second[0].conversation_id);

    let conversation_count: i64 = storage
        .conn
        .query_row_map("SELECT COUNT(*) FROM conversations", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    let message_count: i64 = storage
        .conn
        .query_row_map("SELECT COUNT(*) FROM messages", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();

    assert_eq!(conversation_count, 1);
    assert_eq!(message_count, MESSAGE_COUNT);
}

#[test]
fn parallel_insert_conversation_tree_keeps_unique_external_ids_distinct() {
    use crate::connectors::{NormalizedConversation, NormalizedMessage};
    use crate::franken_sync::compat::{ConnectionExt, RowExt};
    use crate::source_onboarding::conv_map::map_to_internal;
    use crate::model::types::{Agent, AgentKind};
    use rand::RngExt;
    use rayon::prelude::*;

    fn with_retry<F, T>(mut f: F) -> anyhow::Result<T>
    where
        F: FnMut() -> anyhow::Result<T>,
    {
        let mut rng = rand::rng();
        let mut backoff_ms = 4_u64;
        for attempt in 0..=24 {
            match f() {
                Ok(value) => return Ok(value),
                Err(err)
                    if attempt < 24 && crate::storage::sqlite::retryable_franken_anyhow(&err) =>
                {
                    let sleep_ms = backoff_ms + rng.random_range(0..=backoff_ms);
                    std::thread::sleep(Duration::from_millis(sleep_ms));
                    backoff_ms = (backoff_ms * 2).min(512);
                }
                Err(err) => return Err(err),
            }
        }
        unreachable!("retry loop must return on success or final failure")
    }

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("parallel_insert_conversation_tree.db");
    let seed = FrankenStorage::open(&db_path).unwrap();
    drop(seed);

    let conversations: Vec<NormalizedConversation> = (0..10)
        .map(|i| NormalizedConversation {
            agent_slug: format!("agent-{}", i % 3),
            external_id: Some(format!("conv-{i}")),
            title: Some(format!("Conversation {i}")),
            workspace: Some(PathBuf::from(format!("/ws/{i}"))),
            source_path: PathBuf::from(format!("/log/{i}.jsonl")),
            started_at: Some(1_000 + i * 100),
            ended_at: Some(1_000 + i * 100 + 50),
            metadata: serde_json::json!({}),
            messages: (0..3)
                .map(|j| NormalizedMessage {
                    idx: j,
                    role: if j % 2 == 0 { "user" } else { "assistant" }.to_string(),
                    author: Some("tester".into()),
                    created_at: Some(1_000 + i * 100 + j * 10),
                    content: format!("parallel-distinct-test conv={i} msg={j}"),
                    extra: serde_json::json!({}),
                    snippets: vec![],
                    invocations: Vec::new(),
                })
                .collect(),
        })
        .collect();

    let mut outcomes: Vec<(String, i64, Vec<i64>)> = conversations
        .par_chunks(3)
        .map(|chunk| {
            let mut storage = with_retry(|| FrankenStorage::open_writer(&db_path)).unwrap();
            let mut agent_cache: HashMap<String, i64> = HashMap::new();
            let mut workspace_cache: HashMap<PathBuf, i64> = HashMap::new();
            let mut chunk_outcomes = Vec::with_capacity(chunk.len());

            for conv in chunk {
                let agent_slug = conv.agent_slug.clone();
                let workspace = conv.workspace.clone();
                let external_id = conv.external_id.clone().expect("external id");
                let internal = map_to_internal(conv);
                let outcome = with_retry(|| {
                    let agent_id = if let Some(id) = agent_cache.get(&agent_slug) {
                        *id
                    } else {
                        let agent = Agent {
                            id: None,
                            slug: agent_slug.clone(),
                            name: agent_slug.clone(),
                            version: None,
                            kind: AgentKind::Cli,
                        };
                        let id = storage.ensure_agent(&agent)?;
                        agent_cache.insert(agent_slug.clone(), id);
                        id
                    };
                    let workspace_id = if let Some(path) = &workspace {
                        if let Some(id) = workspace_cache.get(path) {
                            Some(*id)
                        } else {
                            let id = storage.ensure_workspace(path, None)?;
                            workspace_cache.insert(path.clone(), id);
                            Some(id)
                        }
                    } else {
                        None
                    };
                    storage.insert_conversation_tree(agent_id, workspace_id, &internal)
                })
                .unwrap();
                chunk_outcomes.push((
                    external_id,
                    outcome.conversation_id,
                    outcome.inserted_indices,
                ));
            }

            with_retry(|| storage.close_without_checkpoint_in_place()).unwrap();
            chunk_outcomes
        })
        .flatten()
        .collect();
    outcomes.sort_by(|left, right| left.0.cmp(&right.0));

    assert!(
        outcomes
            .iter()
            .all(|(_, _, inserted_indices)| inserted_indices == &vec![0, 1, 2]),
        "unique external ids must not be routed through the existing-conversation merge path: {outcomes:?}"
    );

    let distinct_ids: HashSet<i64> = outcomes
        .iter()
        .map(|(_, conversation_id, _)| *conversation_id)
        .collect();
    assert_eq!(
        distinct_ids.len(),
        conversations.len(),
        "unique external ids must produce distinct conversation ids: {outcomes:?}"
    );

    let reader = FrankenStorage::open(&db_path).unwrap();
    let stored_rows: Vec<(i64, String)> = reader
        .raw()
        .query_map_collect(
            "SELECT id, external_id FROM conversations ORDER BY id",
            &[],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
        )
        .unwrap();
    let stored_count: i64 = reader
        .raw()
        .query_row_map("SELECT COUNT(*) FROM conversations", &[], |row| {
            row.get_typed(0)
        })
        .unwrap();

    assert_eq!(
        stored_count as usize,
        conversations.len(),
        "parallel distinct inserts must persist one row per external id; rows={stored_rows:?}; outcomes={outcomes:?}"
    );
    assert_eq!(
        stored_rows.len(),
        conversations.len(),
        "parallel distinct inserts must remain visible after reopening; rows={stored_rows:?}; outcomes={outcomes:?}"
    );
}

#[test]
fn insert_conversation_tree_merges_duplicate_external_id_with_gaps() {
    use crate::connectors::{NormalizedConversation, NormalizedMessage};
    use crate::source_onboarding::conv_map::map_to_internal;
    use crate::model::types::{Agent, AgentKind};
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

    let base_conv = |messages: Vec<NormalizedMessage>| NormalizedConversation {
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: Some("tree-gap-session".into()),
        title: Some("Tree Gap Session".into()),
        source_path: PathBuf::from("/tmp/tree.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_999),
        metadata: serde_json::Value::Null,
        messages,
    };

    let conv_a = map_to_internal(&base_conv(vec![
        NormalizedMessage {
            idx: 2,
            role: "user".into(),
            author: None,
            created_at: Some(1_700_000_000_200),
            content: "third".into(),
            extra: serde_json::Value::Null,
            snippets: Vec::new(),
            invocations: Vec::new(),
        },
        NormalizedMessage {
            idx: 3,
            role: "assistant".into(),
            author: None,
            created_at: Some(1_700_000_000_300),
            content: "fourth".into(),
            extra: serde_json::Value::Null,
            snippets: Vec::new(),
            invocations: Vec::new(),
        },
    ]));
    let conv_b = map_to_internal(&base_conv(vec![
        NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(1_700_000_000_000),
            content: "first".into(),
            extra: serde_json::Value::Null,
            snippets: Vec::new(),
            invocations: Vec::new(),
        },
        NormalizedMessage {
            idx: 1,
            role: "assistant".into(),
            author: None,
            created_at: Some(1_700_000_000_100),
            content: "second".into(),
            extra: serde_json::Value::Null,
            snippets: Vec::new(),
            invocations: Vec::new(),
        },
        NormalizedMessage {
            idx: 3,
            role: "assistant".into(),
            author: None,
            created_at: Some(1_700_000_000_300),
            content: "fourth".into(),
            extra: serde_json::Value::Null,
            snippets: Vec::new(),
            invocations: Vec::new(),
        },
    ]));

    let first = storage
        .insert_conversation_tree(agent_id, None, &conv_a)
        .unwrap();
    let second = storage
        .insert_conversation_tree(agent_id, None, &conv_b)
        .unwrap();

    assert_eq!(first.inserted_indices, vec![2, 3]);
    assert_eq!(second.inserted_indices, vec![0, 1]);
    assert_eq!(first.conversation_id, second.conversation_id);

    let stored_indices: Vec<i64> = storage
        .conn
        .query_map_collect("SELECT idx FROM messages ORDER BY idx", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(stored_indices, vec![0, 1, 2, 3]);
}

#[test]
fn insert_conversation_tree_skips_duplicate_message_indices_for_new_conversation() {
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

    let conversation = Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: Some("duplicate-new-session".into()),
        title: Some("Duplicate New Session".into()),
        source_path: PathBuf::from("/tmp/duplicate-new-session.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_999),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: vec![
            Message {
                id: None,
                idx: 0,
                role: MessageRole::User,
                author: None,
                created_at: Some(1_700_000_000_000),
                content: "first canonical".into(),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            },
            Message {
                id: None,
                idx: 0,
                role: MessageRole::User,
                author: None,
                created_at: Some(1_700_000_000_001),
                content: "duplicate idx should be skipped".into(),
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
        source_id: "local".into(),
        origin_host: None,
    };

    let outcome = storage
        .insert_conversation_tree(agent_id, None, &conversation)
        .unwrap();

    assert_eq!(outcome.inserted_indices, vec![0, 1]);

    let stored_messages: Vec<(i64, String)> = storage
        .conn
        .query_map_collect(
            "SELECT idx, content FROM messages ORDER BY idx",
            fparams![],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
        )
        .unwrap();
    assert_eq!(
        stored_messages,
        vec![
            (0, "first canonical".to_string()),
            (1, "second".to_string())
        ]
    );
}

#[test]
fn insert_conversation_tree_merges_duplicate_source_path_without_external_id() {
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

    let base_conv = |messages: Vec<Message>| Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: None,
        title: Some("Source Path Merge".into()),
        source_path: PathBuf::from("/tmp/shared-session.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_999),
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
            &base_conv(vec![
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
            ]),
        )
        .unwrap();

    let second = storage
        .insert_conversation_tree(
            agent_id,
            None,
            &base_conv(vec![
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
            ]),
        )
        .unwrap();

    assert_eq!(first.conversation_id, second.conversation_id);
    assert_eq!(first.inserted_indices, vec![0, 1]);
    assert_eq!(second.inserted_indices, vec![2]);

    let stored_indices: Vec<i64> = storage
        .conn
        .query_map_collect("SELECT idx FROM messages ORDER BY idx", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(stored_indices, vec![0, 1, 2]);
}

#[test]
fn insert_conversation_tree_merges_source_path_duplicates_with_start_drift() {
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

    let base_conv = |started_at: Option<i64>, messages: Vec<Message>| Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: None,
        title: Some("Drift Merge".into()),
        source_path: PathBuf::from("/tmp/drift-session.jsonl"),
        started_at,
        ended_at: Some(1_700_000_000_999),
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
            &base_conv(
                Some(1_700_000_000_000),
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
            &base_conv(
                Some(1_700_000_004_000),
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
                        created_at: Some(1_700_000_004_200),
                        content: "third".into(),
                        extra_json: serde_json::Value::Null,
                        snippets: Vec::new(),
                    },
                ],
            ),
        )
        .unwrap();

    assert_eq!(first.conversation_id, second.conversation_id);
    assert_eq!(second.inserted_indices, vec![2]);
}

#[test]
fn insert_conversation_tree_keeps_single_message_overlap_sessions_separate() {
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

    let make_conv = |started_at: i64, idx: i64, content: &str| Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: None,
        title: Some("Partial overlap".into()),
        source_path: PathBuf::from("/tmp/reused-session.jsonl"),
        started_at: Some(started_at),
        ended_at: Some(started_at + 500),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: vec![Message {
            id: None,
            idx,
            role: MessageRole::User,
            author: None,
            created_at: Some(started_at),
            content: content.into(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        source_id: "local".into(),
        origin_host: None,
    };

    storage
        .insert_conversation_tree(
            agent_id,
            None,
            &Conversation {
                messages: vec![
                    Message {
                        id: None,
                        idx: 0,
                        role: MessageRole::User,
                        author: None,
                        created_at: Some(1_700_000_000_000),
                        content: "shared opener".into(),
                        extra_json: serde_json::Value::Null,
                        snippets: Vec::new(),
                    },
                    Message {
                        id: None,
                        idx: 1,
                        role: MessageRole::Agent,
                        author: None,
                        created_at: Some(1_700_000_000_100),
                        content: "first session unique".into(),
                        extra_json: serde_json::Value::Null,
                        snippets: Vec::new(),
                    },
                ],
                ..make_conv(1_700_000_000_000, 0, "unused")
            },
        )
        .unwrap();
    storage
        .insert_conversation_tree(
            agent_id,
            None,
            &make_conv(1_700_000_900_000, 0, "shared opener"),
        )
        .unwrap();

    let conversation_count: i64 = storage
        .conn
        .query_row_map("SELECT COUNT(*) FROM conversations", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(conversation_count, 2);
}

#[test]
fn insert_conversation_tree_keeps_distinct_source_path_sessions_separate() {
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

    let make_conv = |started_at: i64, created_at: i64, content: &str| Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: None,
        title: Some("Same Path Different Session".into()),
        source_path: PathBuf::from("/tmp/reused-session.jsonl"),
        started_at: Some(started_at),
        ended_at: Some(started_at + 500),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: None,
            created_at: Some(created_at),
            content: content.into(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        source_id: "local".into(),
        origin_host: None,
    };

    storage
        .insert_conversation_tree(
            agent_id,
            None,
            &make_conv(1_700_000_000_000, 1_700_000_000_000, "first session"),
        )
        .unwrap();
    storage
        .insert_conversation_tree(
            agent_id,
            None,
            &make_conv(1_700_000_900_000, 1_700_000_900_000, "second session"),
        )
        .unwrap();

    let conversation_count: i64 = storage
        .conn
        .query_row_map("SELECT COUNT(*) FROM conversations", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(conversation_count, 2);
}
