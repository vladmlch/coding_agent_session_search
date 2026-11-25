use super::*;
use crate::franken_sync::compat::{ConnectionExt as FrankenConnectionExt, RowExt as FrankenRowExt};
use crate::sources::provenance::LOCAL_SOURCE_ID;
use serial_test::serial;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

#[test]
fn lexical_rebuild_messages_query_uses_conversation_idx_access_path() {
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
    let conversation = Conversation {
        id: None,
        agent_slug: "claude_code".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: Some("conv-1".into()),
        title: Some("Lexical rebuild".into()),
        source_path: PathBuf::from("/tmp/conv-1.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_100),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: vec![
            Message {
                id: None,
                idx: 0,
                role: MessageRole::User,
                author: Some("user".into()),
                created_at: Some(1_700_000_000_010),
                content: "first".into(),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            },
            Message {
                id: None,
                idx: 1,
                role: MessageRole::Agent,
                author: Some("assistant".into()),
                created_at: Some(1_700_000_000_020),
                content: "second".into(),
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
    let conversation_id = storage
        .conn
        .query_row_map(
            "SELECT id FROM conversations WHERE external_id = ?1",
            fparams!["conv-1"],
            |row| row.get_typed::<i64>(0),
        )
        .unwrap();

    let opcodes: Vec<String> = storage
        .conn
        .query_map_collect(
            "EXPLAIN \
             SELECT id, idx, role, author, created_at, content \
             FROM messages \
             WHERE conversation_id = ?1 ORDER BY idx",
            fparams![conversation_id],
            |row| row.get_typed(1),
        )
        .unwrap();

    assert!(
        opcodes.iter().any(|opcode| opcode == "SeekGE"),
        "expected lexical rebuild message fetch to seek into the conversation_id/idx access path, got {opcodes:?}"
    );
    assert!(
        !opcodes.iter().any(|opcode| opcode == "SorterOpen"),
        "expected lexical rebuild message fetch to avoid sorter temp b-trees, got {opcodes:?}"
    );
}

/// `coding_agent_session_search-uhhxy` (gh #302 ask #2): the dedup
/// cleanup must detect a `projects/<rel>` twin of a bare `<rel>`
/// conversation sharing one source_path, report it on dry-run without
/// mutating, and on apply drop ONLY the prefixed twin + its message rows
/// while leaving the canonical row and its messages intact.
#[test]
fn collapse_external_id_prefix_duplicates_drops_prefixed_twin_only() {
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

    let make_conv = |external_id: &str| Conversation {
        id: None,
        agent_slug: "claude_code".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: Some(external_id.to_string()),
        title: Some("Dedup".into()),
        source_path: PathBuf::from("/home/u/.claude/projects/-proj/abc.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_100),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: vec![
            Message {
                id: None,
                idx: 0,
                role: MessageRole::User,
                author: Some("user".into()),
                created_at: Some(1_700_000_000_010),
                content: format!("first {external_id}"),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            },
            Message {
                id: None,
                idx: 1,
                role: MessageRole::Agent,
                author: Some("assistant".into()),
                created_at: Some(1_700_000_000_020),
                content: format!("second {external_id}"),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            },
        ],
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };

    // Bare canonical row (kept) + projects/-prefixed twin (dropped).
    storage
        .insert_conversation_tree(agent_id, None, &make_conv("-proj/abc.jsonl"))
        .unwrap();
    storage
        .insert_conversation_tree(agent_id, None, &make_conv("projects/-proj/abc.jsonl"))
        .unwrap();

    let conv_count = |storage: &FrankenStorage| -> i64 {
        storage
            .conn
            .query_row_map("SELECT COUNT(*) FROM conversations", fparams![], |row| {
                row.get_typed(0)
            })
            .unwrap()
    };
    let msg_count = |storage: &FrankenStorage| -> i64 {
        storage
            .conn
            .query_row_map("SELECT COUNT(*) FROM messages", fparams![], |row| {
                row.get_typed(0)
            })
            .unwrap()
    };
    assert_eq!(conv_count(&storage), 2, "two rows seeded");
    assert_eq!(msg_count(&storage), 4, "two messages per row");

    let max_message_id = storage.max_message_id().unwrap().unwrap();
    storage
        .set_last_embedded_message_id(max_message_id)
        .unwrap();

    // Dry-run: detects the pair, mutates nothing.
    let dry = storage
        .collapse_external_id_prefix_duplicates(true)
        .unwrap();
    assert!(dry.dry_run);
    assert_eq!(dry.conversations_affected, 1);
    assert_eq!(dry.messages_affected, 2);
    assert_eq!(dry.pairs.len(), 1);
    assert_eq!(dry.pairs[0].drop_external_id, "projects/-proj/abc.jsonl");
    assert_eq!(dry.pairs[0].keep_external_id, "-proj/abc.jsonl");
    assert_eq!(conv_count(&storage), 2, "dry-run must not delete rows");
    assert_eq!(msg_count(&storage), 4, "dry-run must not delete messages");
    assert_eq!(
        storage.get_last_embedded_message_id().unwrap(),
        Some(max_message_id),
        "dry-run must not invalidate semantic assets"
    );

    // Apply: drops the prefixed twin + its 2 messages; keeps canonical.
    let applied = storage
        .collapse_external_id_prefix_duplicates(false)
        .unwrap();
    assert!(!applied.dry_run);
    assert_eq!(applied.conversations_affected, 1);
    assert_eq!(applied.messages_affected, 2);
    assert_eq!(conv_count(&storage), 1, "twin row dropped");
    assert_eq!(msg_count(&storage), 2, "twin's messages dropped");
    // 2l1b0.78: the twin's vectors remain in the semantic artifact.
    assert_eq!(storage.get_last_embedded_message_id().unwrap(), None);

    let surviving: String = storage
        .conn
        .query_row_map("SELECT external_id FROM conversations", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(
        surviving, "-proj/abc.jsonl",
        "the bare canonical row must survive, not the projects/ twin"
    );

    // Idempotent: a second apply finds nothing.
    let again = storage
        .collapse_external_id_prefix_duplicates(false)
        .unwrap();
    assert_eq!(again.conversations_affected, 0);
    assert!(again.pairs.is_empty());
}

/// mxr52: rows a remote mirror attributed to an agent that does not own
/// the mirrored store are removed only when the same file is also archived
/// under an owning agent; a misattributed only copy is reported and kept,
/// and rows of other sources, other containers and local rows are untouched.
#[test]
fn collapse_misattributed_mirror_rows_drops_only_duplicates_of_owned_files() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};

    let dir = TempDir::new().unwrap();
    let storage = SqliteStorage::open(&dir.path().join("agent_search.db")).unwrap();
    let mirror = PathBuf::from("/cass/remotes/laptop/mirror");
    let factory_container = mirror.join(".factory_sessions_0123456789abcdef");
    let pi_container = mirror.join(".pi_agent_sessions_0123456789abcdef");
    let claude_container = mirror.join(".claude_projects_0123456789abcdef");
    let seed = |slug: &str, source_id: &str, path: &Path, external_id: &str| -> i64 {
        let agent_id = storage
            .ensure_agent(&Agent {
                id: None,
                slug: slug.into(),
                name: slug.into(),
                version: None,
                kind: AgentKind::Cli,
            })
            .unwrap();
        let conversation = Conversation {
            id: None,
            agent_slug: slug.into(),
            workspace: None,
            external_id: Some(external_id.into()),
            title: Some(external_id.into()),
            source_path: path.to_path_buf(),
            started_at: Some(1_700_000_000_000),
            ended_at: Some(1_700_000_000_100),
            approx_tokens: None,
            metadata_json: serde_json::Value::Null,
            messages: (0..2)
                .map(|idx| Message {
                    id: None,
                    idx,
                    role: MessageRole::User,
                    author: None,
                    created_at: Some(1_700_000_000_010 + idx),
                    content: format!("{external_id} {idx}"),
                    extra_json: serde_json::Value::Null,
                    snippets: Vec::new(),
                })
                .collect(),
            source_id: source_id.into(),
            origin_host: None,
        };
        storage
            .insert_conversation_tree(agent_id, None, &conversation)
            .unwrap()
            .conversation_id
    };
    let factory_file = factory_container.join("sessions/-proj/fac.jsonl");
    let factory_row = seed("factory", "laptop", &factory_file, "fac");
    let claude_dup = seed(
        "claude_code",
        "laptop",
        &factory_file,
        "sessions/-proj/fac.jsonl",
    );
    let pi_file = pi_container.join("sessions/--proj--/pi.jsonl");
    let claude_only = seed(
        "claude_code",
        "laptop",
        &pi_file,
        "sessions/--proj--/pi.jsonl",
    );
    let claude_ok = seed(
        "claude_code",
        "laptop",
        &claude_container.join("projects/-proj/c.jsonl"),
        "projects/-proj/c.jsonl",
    );
    let local = seed(
        "claude_code",
        LOCAL_SOURCE_ID,
        Path::new("/home/u/.claude/projects/-proj/l.jsonl"),
        "-proj/l.jsonl",
    );
    let other_source = seed("claude_code", "server", &factory_file, "server-copy");

    let containers = [
        MirrorContainerOwners {
            source_id: "laptop".into(),
            container: factory_container.clone(),
            owner_agents: vec!["factory".into()],
        },
        MirrorContainerOwners {
            source_id: "laptop".into(),
            container: pi_container.clone(),
            owner_agents: vec!["pi_agent".into(), "pi_durable".into()],
        },
        MirrorContainerOwners {
            source_id: "laptop".into(),
            container: claude_container.clone(),
            owner_agents: vec!["claude_code".into()],
        },
    ];
    let ids = |storage: &FrankenStorage| -> Vec<i64> {
        storage
            .conn
            .query_map_collect(
                "SELECT id FROM conversations ORDER BY id",
                fparams![],
                |row| row.get_typed(0),
            )
            .unwrap()
    };
    let all = ids(&storage);

    let dry = storage
        .collapse_misattributed_mirror_rows(&containers, true)
        .unwrap();
    assert!(dry.dry_run);
    assert_eq!(dry.conversations_affected, 1);
    assert_eq!(dry.messages_affected, 2);
    assert_eq!(dry.unmatched, 1);
    let reported: Vec<(i64, Option<i64>)> = dry
        .rows
        .iter()
        .map(|row| (row.conversation_id, row.owner_conversation_id))
        .collect();
    assert_eq!(
        reported,
        [(claude_dup, Some(factory_row)), (claude_only, None)]
    );
    assert_eq!(ids(&storage), all, "a dry run deletes nothing");

    let applied = storage
        .collapse_misattributed_mirror_rows(&containers, false)
        .unwrap();
    assert_eq!(applied.conversations_affected, 1);
    let mut expected = vec![factory_row, claude_only, claude_ok, local, other_source];
    expected.sort_unstable();
    assert_eq!(ids(&storage), expected, "only the duplicate is removed");
    let orphan_messages: i64 = storage
        .conn
        .query_row_map(
            "SELECT COUNT(*) FROM messages WHERE conversation_id = ?1",
            fparams![claude_dup],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(orphan_messages, 0);

    let again = storage
        .collapse_misattributed_mirror_rows(&containers, false)
        .unwrap();
    assert_eq!(again.conversations_affected, 0);
    assert_eq!(
        again.unmatched, 1,
        "the only copy is still reported, never removed"
    );
}

/// bgn6s: the cap bounds each message, never the conversation. A cumulative
/// cap (the old behavior) cleared every message after the first 100 bytes.
#[test]
fn lexical_content_truncation_caps_each_message_and_keeps_later_text() {
    use crate::model::types::{Message, MessageRole};

    let cap = 50usize;
    let mut messages = vec![
        Message {
            id: Some(1),
            idx: 0,
            role: MessageRole::User,
            author: None,
            created_at: Some(1),
            content: "a".repeat(60),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        },
        Message {
            id: Some(2),
            idx: 1,
            role: MessageRole::Agent,
            author: None,
            created_at: Some(2),
            content: "b".repeat(60),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        },
        Message {
            id: Some(3),
            idx: 2,
            role: MessageRole::Agent,
            author: None,
            created_at: Some(3),
            content: "c".repeat(60),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        },
    ];

    truncate_lexical_rebuild_message_content(&mut messages, cap);

    // Structure (message count + ids) preserved; only indexed text trimmed.
    assert_eq!(messages.len(), 3, "message rows must be preserved");
    assert_eq!(
        messages.iter().map(|m| m.id).collect::<Vec<_>>(),
        vec![Some(1), Some(2), Some(3)]
    );
    // Every message keeps its own capped prefix; the last one included.
    for (message, letter) in messages.iter().zip(["a", "b", "c"]) {
        assert_eq!(message.content, letter.repeat(cap));
    }
}

#[test]
fn lexical_content_truncation_is_noop_within_cap() {
    use crate::model::types::{Message, MessageRole};

    let mut messages = vec![Message {
        id: Some(1),
        idx: 0,
        role: MessageRole::User,
        author: None,
        created_at: Some(1),
        content: "short".to_string(),
        extra_json: serde_json::Value::Null,
        snippets: Vec::new(),
    }];
    truncate_lexical_rebuild_message_content(&mut messages, 1024);
    assert_eq!(messages[0].content, "short", "within-cap content untouched");
}

#[test]
fn lexical_content_truncation_respects_utf8_char_boundaries() {
    use crate::model::types::{Message, MessageRole};

    // Each "é" is 2 bytes; a cap of 5 must not split a code point.
    let mut messages = vec![Message {
        id: Some(1),
        idx: 0,
        role: MessageRole::User,
        author: None,
        created_at: Some(1),
        content: "ééé".to_string(), // 6 bytes
        extra_json: serde_json::Value::Null,
        snippets: Vec::new(),
    }];
    truncate_lexical_rebuild_message_content(&mut messages, 5);
    // Largest char boundary <= 5 is 4 bytes ("éé").
    assert_eq!(messages[0].content, "éé");
    assert!(
        messages[0]
            .content
            .is_char_boundary(messages[0].content.len())
    );
}

/// #290 + bgn6s: oversized messages are capped one by one; no message of a
/// long conversation loses its text to a conversation-wide budget, and the
/// footprint / range / budgeted reads let callers bound memory instead.
#[test]
#[serial]
fn fetch_messages_for_lexical_rebuild_caps_each_message_of_a_long_conversation() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    // Force a small, deterministic per-message cap independent of host memory.
    let _cap = set_env_var("CASS_LEXICAL_MAX_MESSAGE_CONTENT_BYTES", "1025");

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

    // Many messages of 2 KiB each model an image/base64-heavy conversation.
    // Trailing empty projections must not retain one large buffer per row.
    const MESSAGE_COUNT: i64 = 64;
    let big = "é".repeat(1024);
    let conversation = Conversation {
        id: None,
        agent_slug: "claude_code".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: Some("conv-big".into()),
        title: Some("Oversized".into()),
        source_path: PathBuf::from("/tmp/conv-big.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_100),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: (0..MESSAGE_COUNT)
            .map(|idx| Message {
                id: None,
                idx,
                role: MessageRole::Agent,
                author: Some("assistant".into()),
                created_at: Some(1_700_000_000_010 + idx),
                content: big.clone(),
                extra_json: serde_json::json!({"canonical_metadata": "retained"}),
                snippets: Vec::new(),
            })
            .collect(),
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };
    storage
        .insert_conversation_tree(agent_id, None, &conversation)
        .unwrap();
    let conversation_id = storage
        .conn
        .query_row_map(
            "SELECT id FROM conversations WHERE external_id = ?1",
            fparams!["conv-big"],
            |row| row.get_typed::<i64>(0),
        )
        .unwrap();

    let projection_opcodes: Vec<(String, Option<String>)> = storage
        .conn
        .query_map_collect(
            "EXPLAIN SELECT id, idx, role, author, created_at, \
                 substr(content, 1, 1025), COALESCE(octet_length(content), 0) \
                 FROM messages INDEXED BY sqlite_autoindex_messages_1 \
                 WHERE conversation_id = ?1 ORDER BY idx",
            fparams![conversation_id],
            |row| Ok((row.get_typed(1)?, row.get_typed::<String>(5).ok())),
        )
        .expect("explain bounded lexical projection");
    assert!(
        projection_opcodes
            .iter()
            .any(|(opcode, _)| opcode == "SeekGE")
            && projection_opcodes
                .iter()
                .any(|(opcode, _)| opcode == "IdxGT"),
        "bounded lexical projection must range-scan the composite index: {projection_opcodes:?}"
    );
    assert!(
        projection_opcodes.iter().any(|(opcode, p4)| {
            opcode == "Function" && p4.as_deref().is_some_and(|s| s.starts_with("substr"))
        }) && projection_opcodes.iter().any(|(opcode, p4)| {
            opcode == "Function" && p4.as_deref().is_some_and(|s| s.starts_with("octet_length"))
        }),
        "bounded lexical projection must evaluate substr and octet_length in SQLite: {projection_opcodes:?}"
    );
    assert!(
        !projection_opcodes
            .iter()
            .any(|(opcode, _)| opcode == "SorterOpen"),
        "bounded lexical projection must avoid sorter temp tables: {projection_opcodes:?}"
    );
    let projection_eqp: Vec<String> = storage
        .conn
        .query_map_collect(
            "EXPLAIN QUERY PLAN SELECT id, idx, role, author, created_at, \
                 substr(content, 1, 1025), COALESCE(octet_length(content), 0) \
                 FROM messages INDEXED BY sqlite_autoindex_messages_1 \
                 WHERE conversation_id = ?1 ORDER BY idx",
            fparams![conversation_id],
            |row| row.get_typed(3),
        )
        .expect("explain query plan bounded lexical projection");
    assert!(
        !projection_eqp
            .iter()
            .any(|detail| detail.contains("TEMP B-TREE")),
        "bounded lexical projection must avoid TEMP B-TREE sorts: {projection_eqp:?}"
    );

    let messages = storage
        .fetch_messages_for_lexical_rebuild(conversation_id)
        .unwrap();

    // All message rows survive (count/structure intact) ...
    assert_eq!(
        messages.len(),
        MESSAGE_COUNT as usize,
        "message rows preserved"
    );
    // ... and EVERY message keeps its own capped prefix (bgn6s): a 1,025-byte
    // cap cannot split a two-byte `é`, so each keeps 1,024 bytes. The old
    // cumulative cap kept only the first message and blanked the other 63.
    assert!(
        messages.iter().all(|message| message.content.len() == 1024),
        "each message is capped on its own UTF-8 boundary"
    );
    assert!(
        messages
            .iter()
            .all(|message| message.content.capacity() <= 1025),
        "a retained prefix never keeps the full projected cell allocation"
    );
    assert!(messages.iter().all(|message| message.extra_json.is_null()));

    // Footprints report each message's capped size without reading text.
    let footprints = storage
        .lexical_rebuild_message_footprints(conversation_id)
        .unwrap();
    assert_eq!(footprints.len(), MESSAGE_COUNT as usize);
    assert!(footprints.iter().all(|(_, bytes)| *bytes == 1025));
    assert_eq!(
        footprints.iter().map(|(idx, _)| *idx).collect::<Vec<_>>(),
        (0..MESSAGE_COUNT).collect::<Vec<_>>()
    );
    // An inclusive idx range returns exactly those messages, in order.
    let range = storage
        .fetch_messages_for_lexical_rebuild_idx_range(conversation_id, 10, 12)
        .unwrap();
    assert_eq!(
        range.iter().map(|message| message.idx).collect::<Vec<_>>(),
        vec![10, 11, 12]
    );
    assert!(range.iter().all(|message| message.content.len() == 1024));
    // A byte budget stops the read before the whole conversation is held.
    assert!(
        storage
            .fetch_messages_for_lexical_rebuild_within(conversation_id, 5 * 1024)
            .unwrap()
            .is_none(),
        "64 KiB of text does not fit a 5 KiB budget"
    );
    let within = storage
        .fetch_messages_for_lexical_rebuild_within(conversation_id, 64 * 1024)
        .unwrap()
        .expect("the whole conversation fits a 64 KiB budget");
    assert_eq!(within.len(), MESSAGE_COUNT as usize);
    let stored = storage.fetch_messages(conversation_id).unwrap();
    assert_eq!(stored.len(), MESSAGE_COUNT as usize);
    assert_eq!(
        messages
            .iter()
            .map(|message| (message.id, message.idx))
            .collect::<Vec<_>>(),
        stored
            .iter()
            .map(|message| (message.id, message.idx))
            .collect::<Vec<_>>(),
        "bounded lexical hydration preserves canonical message identities"
    );
    assert!(stored.iter().all(|message| message.content == big));
    assert!(stored.iter().all(|message| !message.extra_json.is_null()));
}

#[test]
fn list_conversations_for_lexical_rebuild_uses_stable_id_order() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("agent_search.db");
    let storage = SqliteStorage::open(&db_path).unwrap();
    let agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: Some("0.2.3".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();

    let make_conv = |source_path: &str, started_at: i64| Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: Some(source_path.to_string()),
        title: Some(source_path.to_string()),
        source_path: PathBuf::from(source_path),
        started_at: Some(started_at),
        ended_at: Some(started_at + 1),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: None,
            created_at: Some(started_at),
            content: format!("message for {source_path}"),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };

    let conv_a = make_conv("/tmp/a.jsonl", 3_000);
    let conv_b = make_conv("/tmp/b.jsonl", 1_000);
    let conv_c = make_conv("/tmp/c.jsonl", 2_000);

    storage
        .insert_conversation_tree(agent_id, None, &conv_a)
        .unwrap();
    storage
        .insert_conversation_tree(agent_id, None, &conv_b)
        .unwrap();
    storage
        .insert_conversation_tree(agent_id, None, &conv_c)
        .unwrap();

    let user_order: Vec<PathBuf> = storage
        .list_conversations(10, 0)
        .unwrap()
        .into_iter()
        .map(|conv| conv.source_path)
        .collect();
    assert_eq!(
        user_order,
        vec![
            PathBuf::from("/tmp/a.jsonl"),
            PathBuf::from("/tmp/c.jsonl"),
            PathBuf::from("/tmp/b.jsonl"),
        ]
    );

    let (agent_slugs, workspace_paths) = storage.build_lexical_rebuild_lookups().unwrap();
    let rebuild_order: Vec<PathBuf> = storage
        .list_conversations_for_lexical_rebuild_after_id(10, 0, &agent_slugs, &workspace_paths)
        .unwrap()
        .into_iter()
        .map(|conv| conv.source_path)
        .collect();
    assert_eq!(
        rebuild_order,
        vec![
            PathBuf::from("/tmp/a.jsonl"),
            PathBuf::from("/tmp/b.jsonl"),
            PathBuf::from("/tmp/c.jsonl"),
        ]
    );

    let first_page = storage
        .list_conversations_for_lexical_rebuild_after_id(2, 0, &agent_slugs, &workspace_paths)
        .unwrap();
    let first_page_paths: Vec<PathBuf> = first_page
        .iter()
        .map(|conv| conv.source_path.clone())
        .collect();
    assert_eq!(
        first_page_paths,
        vec![PathBuf::from("/tmp/a.jsonl"), PathBuf::from("/tmp/b.jsonl")]
    );

    let second_page = storage
        .list_conversations_for_lexical_rebuild_after_id(
            2,
            first_page
                .last()
                .and_then(|conv| conv.id)
                .expect("first page should include an id"),
            &agent_slugs,
            &workspace_paths,
        )
        .unwrap();
    let second_page_paths: Vec<PathBuf> = second_page
        .iter()
        .map(|conv| conv.source_path.clone())
        .collect();
    assert_eq!(second_page_paths, vec![PathBuf::from("/tmp/c.jsonl")]);

    let bounded_page = storage
        .list_conversations_for_lexical_rebuild_after_id_through_id(
            10,
            0,
            first_page
                .last()
                .and_then(|conv| conv.id)
                .expect("first page should include an id"),
            &agent_slugs,
            &workspace_paths,
        )
        .unwrap();
    let bounded_paths: Vec<PathBuf> = bounded_page
        .iter()
        .map(|conv| conv.source_path.clone())
        .collect();
    assert_eq!(
        bounded_paths,
        vec![PathBuf::from("/tmp/a.jsonl"), PathBuf::from("/tmp/b.jsonl")]
    );
}

#[test]
fn keyset_traversal_handles_sparse_holey_conversation_ids() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("agent_search.db");
    let storage = SqliteStorage::open(&db_path).unwrap();
    let agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: Some("0.2.3".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();

    let make_conv = |label: &str, ts: i64| Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: Some(label.to_string()),
        title: Some(label.to_string()),
        source_path: PathBuf::from(format!("/tmp/{label}.jsonl")),
        started_at: Some(ts),
        ended_at: Some(ts + 1),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: None,
            created_at: Some(ts),
            content: format!("msg for {label}"),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };

    for i in 0..6 {
        storage
            .insert_conversation_tree(agent_id, None, &make_conv(&format!("conv-{i}"), 1000 + i))
            .unwrap();
    }

    storage.conn.execute("PRAGMA foreign_keys = OFF").unwrap();
    storage
        .conn
        .execute_compat("DELETE FROM conversations WHERE id IN (2, 4)", fparams![])
        .unwrap();
    storage
        .conn
        .execute_compat(
            "DELETE FROM messages WHERE conversation_id IN (2, 4)",
            fparams![],
        )
        .unwrap();
    storage.conn.execute("PRAGMA foreign_keys = ON").unwrap();

    let (agent_slugs, workspace_paths) = storage.build_lexical_rebuild_lookups().unwrap();

    let page1 = storage
        .list_conversations_for_lexical_rebuild_after_id(2, 0, &agent_slugs, &workspace_paths)
        .unwrap();
    assert_eq!(page1.len(), 2);
    let page1_ids: Vec<i64> = page1.iter().map(|c| c.id.unwrap()).collect();
    assert_eq!(page1_ids, vec![1, 3]);

    let page2 = storage
        .list_conversations_for_lexical_rebuild_after_id(
            2,
            *page1_ids.last().unwrap(),
            &agent_slugs,
            &workspace_paths,
        )
        .unwrap();
    assert_eq!(page2.len(), 2);
    let page2_ids: Vec<i64> = page2.iter().map(|c| c.id.unwrap()).collect();
    assert_eq!(page2_ids, vec![5, 6]);

    let page3 = storage
        .list_conversations_for_lexical_rebuild_after_id(
            2,
            *page2_ids.last().unwrap(),
            &agent_slugs,
            &workspace_paths,
        )
        .unwrap();
    assert!(page3.is_empty());

    let all_ids: Vec<i64> = page1_ids.iter().chain(page2_ids.iter()).copied().collect();
    assert_eq!(all_ids, vec![1, 3, 5, 6]);
}

#[test]
fn keyset_traversal_through_id_with_sparse_ranges() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("agent_search.db");
    let storage = SqliteStorage::open(&db_path).unwrap();
    let agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: Some("0.2.3".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();

    let make_conv = |label: &str, ts: i64| Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        external_id: Some(label.to_string()),
        title: Some(label.to_string()),
        source_path: PathBuf::from(format!("/tmp/{label}.jsonl")),
        started_at: Some(ts),
        ended_at: Some(ts + 1),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: None,
            created_at: Some(ts),
            content: format!("msg for {label}"),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };

    for i in 0..10 {
        storage
            .insert_conversation_tree(agent_id, None, &make_conv(&format!("conv-{i}"), 1000 + i))
            .unwrap();
    }

    storage.conn.execute("PRAGMA foreign_keys = OFF").unwrap();
    storage
        .conn
        .execute_compat(
            "DELETE FROM conversations WHERE id IN (3, 5, 7, 8)",
            fparams![],
        )
        .unwrap();
    storage
        .conn
        .execute_compat(
            "DELETE FROM messages WHERE conversation_id IN (3, 5, 7, 8)",
            fparams![],
        )
        .unwrap();
    storage.conn.execute("PRAGMA foreign_keys = ON").unwrap();

    let (agent_slugs, workspace_paths) = storage.build_lexical_rebuild_lookups().unwrap();

    let through_5 = storage
        .list_conversations_for_lexical_rebuild_after_id_through_id(
            100,
            0,
            5,
            &agent_slugs,
            &workspace_paths,
        )
        .unwrap();
    let through_5_ids: Vec<i64> = through_5.iter().map(|c| c.id.unwrap()).collect();
    assert_eq!(through_5_ids, vec![1, 2, 4]);

    let after_4_through_10 = storage
        .list_conversations_for_lexical_rebuild_after_id_through_id(
            100,
            4,
            10,
            &agent_slugs,
            &workspace_paths,
        )
        .unwrap();
    let ids: Vec<i64> = after_4_through_10.iter().map(|c| c.id.unwrap()).collect();
    assert_eq!(ids, vec![6, 9, 10]);

    let after_10 = storage
        .list_conversations_for_lexical_rebuild_after_id_through_id(
            100,
            10,
            20,
            &agent_slugs,
            &workspace_paths,
        )
        .unwrap();
    assert!(after_10.is_empty());
}

#[test]
fn list_conversation_footprints_for_lexical_rebuild_estimates_bytes_and_keeps_empty_conversations()
{
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("agent_search.db");
    let storage = SqliteStorage::open(&db_path).unwrap();
    let agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: Some("0.2.3".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();

    let insert = |external_id: &str, base_ts: i64, messages: Vec<Message>| {
        storage
            .insert_conversation_tree(
                agent_id,
                None,
                &Conversation {
                    id: None,
                    agent_slug: "codex".into(),
                    workspace: Some(PathBuf::from("/tmp/workspace")),
                    external_id: Some(external_id.to_string()),
                    title: Some(external_id.to_string()),
                    source_path: PathBuf::from(format!("/tmp/{external_id}.jsonl")),
                    started_at: Some(base_ts),
                    ended_at: Some(base_ts + 100),
                    approx_tokens: None,
                    metadata_json: serde_json::Value::Null,
                    messages,
                    source_id: LOCAL_SOURCE_ID.into(),
                    origin_host: None,
                },
            )
            .unwrap()
            .conversation_id
    };

    let ascii_id = insert(
        "footprint-ascii",
        1_700_000_000_000,
        vec![
            Message {
                id: None,
                idx: 0,
                role: MessageRole::User,
                author: None,
                created_at: Some(1_700_000_000_001),
                content: "abc".into(),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            },
            Message {
                id: None,
                idx: 1,
                role: MessageRole::Agent,
                author: None,
                created_at: Some(1_700_000_000_002),
                content: "defg".into(),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            },
        ],
    );
    let empty_id = insert("footprint-empty", 1_700_000_001_000, Vec::new());
    let utf8_id = insert(
        "footprint-utf8",
        1_700_000_002_000,
        vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::Tool,
            author: None,
            created_at: Some(1_700_000_002_001),
            content: "hé🙂".into(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
    );
    let sparse_id = insert(
        "footprint-sparse",
        1_700_000_003_000,
        vec![Message {
            id: None,
            idx: 10,
            role: MessageRole::User,
            author: None,
            created_at: Some(1_700_000_003_010),
            content: "sparse".into(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
    );
    storage
        .conn
        .execute_compat(
            "DELETE FROM conversation_tail_state WHERE conversation_id = ?1",
            fparams![utf8_id],
        )
        .unwrap();

    let footprints = storage
        .list_conversation_footprints_for_lexical_rebuild()
        .unwrap();
    assert_eq!(
        footprints,
        vec![
            LexicalRebuildConversationFootprintRow {
                conversation_id: ascii_id,
                message_count: 2,
                message_bytes: 2 * LEXICAL_REBUILD_PLANNER_ESTIMATED_BYTES_PER_MESSAGE,
            },
            LexicalRebuildConversationFootprintRow {
                conversation_id: empty_id,
                message_count: 0,
                message_bytes: 0,
            },
            LexicalRebuildConversationFootprintRow {
                conversation_id: utf8_id,
                message_count: 1,
                message_bytes: LEXICAL_REBUILD_PLANNER_ESTIMATED_BYTES_PER_MESSAGE,
            },
            LexicalRebuildConversationFootprintRow {
                conversation_id: sparse_id,
                message_count: 11,
                message_bytes: 11 * LEXICAL_REBUILD_PLANNER_ESTIMATED_BYTES_PER_MESSAGE,
            },
        ]
    );
}

#[test]
fn list_conversation_footprints_for_lexical_rebuild_falls_back_for_missing_tail_cache() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("agent_search.db");
    let storage = SqliteStorage::open(&db_path).unwrap();
    let agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: Some("0.2.3".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();
    let conversation_id = storage
        .insert_conversation_tree(
            agent_id,
            None,
            &Conversation {
                id: None,
                agent_slug: "codex".into(),
                workspace: Some(PathBuf::from("/tmp/workspace")),
                external_id: Some("footprint-missing-tail".to_string()),
                title: Some("footprint-missing-tail".to_string()),
                source_path: PathBuf::from("/tmp/footprint-missing-tail.jsonl"),
                started_at: Some(1_700_000_000_000),
                ended_at: Some(1_700_000_000_100),
                approx_tokens: None,
                metadata_json: serde_json::Value::Null,
                messages: vec![Message {
                    id: None,
                    idx: 10,
                    role: MessageRole::User,
                    author: None,
                    created_at: Some(1_700_000_000_010),
                    content: "legacy sparse tail".into(),
                    extra_json: serde_json::Value::Null,
                    snippets: Vec::new(),
                }],
                source_id: LOCAL_SOURCE_ID.into(),
                origin_host: None,
            },
        )
        .unwrap()
        .conversation_id;

    storage
        .conn
        .execute_compat(
            "UPDATE conversations
             SET last_message_idx = NULL, last_message_created_at = NULL
             WHERE id = ?1",
            fparams![conversation_id],
        )
        .unwrap();
    storage
        .conn
        .execute_compat(
            "DELETE FROM conversation_tail_state WHERE conversation_id = ?1",
            fparams![conversation_id],
        )
        .unwrap();

    let footprints = storage
        .list_conversation_footprints_for_lexical_rebuild()
        .unwrap();

    assert_eq!(
        footprints,
        vec![LexicalRebuildConversationFootprintRow {
            conversation_id,
            message_count: 11,
            message_bytes: 11 * LEXICAL_REBUILD_PLANNER_ESTIMATED_BYTES_PER_MESSAGE,
        }],
        "missing tail-cache metadata should fall back to messages MAX(idx) instead of treating legacy conversations as empty"
    );
}

#[test]
fn list_conversation_footprints_for_lexical_rebuild_raises_stale_low_tail_cache() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("agent_search.db");
    let storage = SqliteStorage::open(&db_path).unwrap();
    let agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: Some("0.2.3".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();
    let conversation_id = storage
        .insert_conversation_tree(
            agent_id,
            None,
            &Conversation {
                id: None,
                agent_slug: "codex".into(),
                workspace: Some(PathBuf::from("/tmp/workspace")),
                external_id: Some("footprint-stale-tail".to_string()),
                title: Some("footprint-stale-tail".to_string()),
                source_path: PathBuf::from("/tmp/footprint-stale-tail.jsonl"),
                started_at: Some(1_700_000_000_000),
                ended_at: Some(1_700_000_000_100),
                approx_tokens: None,
                metadata_json: serde_json::Value::Null,
                messages: (0..3)
                    .map(|idx| Message {
                        id: None,
                        idx,
                        role: MessageRole::User,
                        author: None,
                        created_at: Some(1_700_000_000_010 + idx),
                        content: format!("message {idx}"),
                        extra_json: serde_json::Value::Null,
                        snippets: Vec::new(),
                    })
                    .collect(),
                source_id: LOCAL_SOURCE_ID.into(),
                origin_host: None,
            },
        )
        .unwrap()
        .conversation_id;

    storage
        .conn
        .execute_compat(
            "UPDATE conversations
             SET last_message_idx = 0, last_message_created_at = 1700000000010
             WHERE id = ?1",
            fparams![conversation_id],
        )
        .unwrap();
    storage
        .conn
        .execute_compat(
            "UPDATE conversation_tail_state
             SET last_message_idx = 0, last_message_created_at = 1700000000010
             WHERE conversation_id = ?1",
            fparams![conversation_id],
        )
        .unwrap();

    let footprints = storage
        .list_conversation_footprints_for_lexical_rebuild()
        .unwrap();

    assert_eq!(
        footprints,
        vec![LexicalRebuildConversationFootprintRow {
            conversation_id,
            message_count: 3,
            message_bytes: 3 * LEXICAL_REBUILD_PLANNER_ESTIMATED_BYTES_PER_MESSAGE,
        }],
        "stale-low tail caches must not under-plan lexical shards and trip doc>plan invariants"
    );
}

#[test]
fn list_conversation_footprints_for_lexical_rebuild_tolerates_missing_tail_state_table() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("agent_search.db");
    let storage = SqliteStorage::open(&db_path).unwrap();
    let agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: Some("0.2.3".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();
    let conversation_id = storage
        .insert_conversation_tree(
            agent_id,
            None,
            &Conversation {
                id: None,
                agent_slug: "codex".into(),
                workspace: Some(PathBuf::from("/tmp/workspace")),
                external_id: Some("footprint-missing-tail-table".to_string()),
                title: Some("footprint-missing-tail-table".to_string()),
                source_path: PathBuf::from("/tmp/footprint-missing-tail-table.jsonl"),
                started_at: Some(1_700_000_000_000),
                ended_at: Some(1_700_000_000_100),
                approx_tokens: None,
                metadata_json: serde_json::Value::Null,
                messages: vec![Message {
                    id: None,
                    idx: 10,
                    role: MessageRole::User,
                    author: None,
                    created_at: Some(1_700_000_000_010),
                    content: "legacy sparse tail without hot table".into(),
                    extra_json: serde_json::Value::Null,
                    snippets: Vec::new(),
                }],
                source_id: LOCAL_SOURCE_ID.into(),
                origin_host: None,
            },
        )
        .unwrap()
        .conversation_id;

    storage
        .conn
        .execute_compat(
            "UPDATE conversations
             SET last_message_idx = NULL, last_message_created_at = NULL
             WHERE id = ?1",
            fparams![conversation_id],
        )
        .unwrap();
    storage
        .conn
        .execute_compat("DROP TABLE conversation_tail_state", fparams![])
        .unwrap();

    let footprints = storage
        .list_conversation_footprints_for_lexical_rebuild()
        .unwrap();

    assert_eq!(
        footprints,
        vec![LexicalRebuildConversationFootprintRow {
            conversation_id,
            message_count: 11,
            message_bytes: 11 * LEXICAL_REBUILD_PLANNER_ESTIMATED_BYTES_PER_MESSAGE,
        }],
        "read-only lexical self-heal must tolerate pre-tail-cache databases and use messages MAX(idx)"
    );
}

#[test]
fn list_conversation_footprints_for_lexical_rebuild_tolerates_legacy_search_demo_fixture() {
    let fixture_db = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("search_demo_data")
        .join("agent_search.db");
    // Never open the checked-in fixture in place: FrankenSQLite maintains
    // machine-local namespace identity records beside every opened DB.
    // A dirty local tree can already contain those ignored sidecars while
    // a clean remote checkout cannot, masking first-contact failures and
    // making the test depend on source-tree writability.
    let fixture_dir = TempDir::new().unwrap();
    let staged_db = fixture_dir.path().join("agent_search.db");
    std::fs::copy(&fixture_db, &staged_db).unwrap();
    for suffix in ["-fsqlite-ns-gate", "-fsqlite-ns-use"] {
        let mut sidecar = staged_db.as_os_str().to_os_string();
        sidecar.push(suffix);
        assert!(
            !PathBuf::from(sidecar).exists(),
            "freshly staged fixture must begin without machine-local namespace sidecars"
        );
    }

    let rebuild_err = FrankenStorage::open_readonly(&staged_db)
        .err()
        .expect("unstamped legacy fixture must be refused by schema-v1 open_readonly");
    assert!(
        rebuild_err.downcast_ref::<SchemaRebuildRequired>().is_some(),
        "expected SchemaRebuildRequired, got {rebuild_err:#}"
    );

    let stamp_conn =
        crate::franken_sync::Connection::open(staged_db.to_string_lossy().into_owned()).unwrap();
    stamp_conn
        .execute_batch(
            "PRAGMA application_id = 1128354643;
             PRAGMA user_version = 1;",
        )
        .unwrap();
    stamp_conn.close().unwrap();

    let storage = FrankenStorage::open_readonly(&staged_db).unwrap();

    let footprints = storage
        .list_conversation_footprints_for_lexical_rebuild()
        .unwrap();

    assert!(
        !footprints.is_empty(),
        "search self-heal should be able to plan a lexical rebuild from the legacy search demo fixture"
    );
    assert!(
        footprints
            .iter()
            .all(|footprint| footprint.message_count > 0),
        "legacy fixture conversations should derive message counts from messages when tail caches are absent"
    );
}

#[test]
fn lexical_rebuild_listing_normalizes_host_only_remote_source_from_blank_source_id() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("agent_search.db");
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
        external_id: Some("legacy-blank-source".into()),
        title: Some("Legacy blank source".into()),
        source_path: PathBuf::from("/tmp/legacy-blank-source.jsonl"),
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
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };

    let conversation_id = storage
        .insert_conversation_tree(agent_id, None, &conversation)
        .unwrap()
        .conversation_id;
    storage.conn.execute("PRAGMA foreign_keys = OFF").unwrap();
    storage
        .conn
        .execute_compat(
            "UPDATE conversations SET source_id = ?1, origin_host = ?2 WHERE id = ?3",
            fparams!["   ", "dev@laptop", conversation_id],
        )
        .unwrap();
    storage.conn.execute("PRAGMA foreign_keys = ON").unwrap();

    let listed = storage.list_conversations(10, 0).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].source_id, "dev@laptop");
    assert_eq!(listed[0].origin_host.as_deref(), Some("dev@laptop"));

    let (agent_slugs, workspace_paths) = storage.build_lexical_rebuild_lookups().unwrap();
    let rebuild_listed = storage
        .list_conversations_for_lexical_rebuild_after_id(10, 0, &agent_slugs, &workspace_paths)
        .unwrap();
    assert_eq!(rebuild_listed.len(), 1);
    assert_eq!(rebuild_listed[0].source_id, "dev@laptop");
    assert_eq!(rebuild_listed[0].origin_host.as_deref(), Some("dev@laptop"));
}
