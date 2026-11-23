use super::super::*;
use super::helpers::*;
use crate::model::types::{Agent, AgentKind, Message, MessageRole};
use crate::storage::sqlite::FrankenStorage;
use serde_json::json;
use std::path::Path;
use tempfile::tempdir;

#[test]
fn checkpoint_caps_stop_at_whole_conversation_boundary() -> Result<()> {
    let temp = tempdir().unwrap();
    let db_path = temp.path().join("agent_search.db");
    let storage = FrankenStorage::open(&db_path)?;
    let agent_id = storage.ensure_agent(&Agent {
        id: None,
        slug: "codex".to_string(),
        name: "Codex".to_string(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    for external_id in ["cap-first", "cap-second", "cap-third"] {
        storage.insert_conversation_tree(
            agent_id,
            None,
            &test_conversation_with_messages(
                external_id,
                vec![
                    Message {
                        id: None,
                        idx: 0,
                        role: MessageRole::User,
                        author: None,
                        created_at: Some(1_700_000_000_500),
                        content: format!("{external_id} user semantic text"),
                        extra_json: json!({}),
                        snippets: Vec::new(),
                    },
                    Message {
                        id: None,
                        idx: 1,
                        role: MessageRole::Agent,
                        author: None,
                        created_at: Some(1_700_000_000_600),
                        content: format!("{external_id} assistant semantic text"),
                        extra_json: json!({}),
                        snippets: Vec::new(),
                    },
                ],
            ),
        )?;
    }

    let first_conversation_id: i64 = storage.raw().query_row_map(
        "SELECT MIN(id) FROM conversations",
        &[] as &[ParamValue],
        |row| row.get_typed(0),
    )?;
    let batch = fetch_canonical_embedding_batch_inner_with_caps(
        &storage,
        0,
        8,
        None,
        SemanticCheckpointCaps {
            max_messages: 3,
            max_bytes: 0,
        },
    )?;

    assert_eq!(batch.conversations_in_batch, 1);
    anyhow::ensure!(
        !batch.cursor_exhausted,
        "checkpoint caps that stop before the next whole conversation must not publish"
    );
    assert_eq!(batch.last_conversation_id, first_conversation_id);
    assert_eq!(batch.inputs.len(), 2);
    assert!(
        batch
            .inputs
            .iter()
            .all(|input| input.content.contains("cap-first"))
    );
    assert_eq!(batch.total_conversations, 3);
    Ok(())
}

#[test]
fn packet_embedding_inputs_from_storage_since_only_emits_new_canonical_messages() -> Result<()> {
    let temp = tempdir().unwrap();
    let db_path = temp.path().join("agent_search.db");
    let storage = FrankenStorage::open(&db_path)?;
    let agent_id = storage.ensure_agent(&Agent {
        id: None,
        slug: "codex".to_string(),
        name: "Codex".to_string(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    storage.insert_conversation_tree(
        agent_id,
        None,
        &test_conversation_with_messages(
            "packet-delta",
            vec![
                Message {
                    id: None,
                    idx: 0,
                    role: MessageRole::User,
                    author: None,
                    created_at: Some(1_700_000_000_500),
                    content: "existing semantic text".to_string(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
                Message {
                    id: None,
                    idx: 1,
                    role: MessageRole::Agent,
                    author: None,
                    created_at: Some(1_700_000_000_600),
                    content: "existing assistant text".to_string(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
            ],
        ),
    )?;
    let watermark: i64 = storage.raw().query_row_map(
        "SELECT MAX(id) FROM messages",
        &[] as &[ParamValue],
        |row| row.get_typed(0),
    )?;

    storage.insert_conversation_tree(
        agent_id,
        None,
        &test_conversation_with_messages(
            "packet-delta",
            vec![
                Message {
                    id: None,
                    idx: 0,
                    role: MessageRole::User,
                    author: None,
                    created_at: Some(1_700_000_000_500),
                    content: "existing semantic text".to_string(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
                Message {
                    id: None,
                    idx: 1,
                    role: MessageRole::Agent,
                    author: None,
                    created_at: Some(1_700_000_000_600),
                    content: "existing assistant text".to_string(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
                Message {
                    id: None,
                    idx: 2,
                    role: MessageRole::Agent,
                    author: None,
                    created_at: Some(1_700_000_000_700),
                    content: "new packet semantic text".to_string(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
                Message {
                    id: None,
                    idx: 3,
                    role: MessageRole::System,
                    author: None,
                    created_at: Some(1_700_000_000_800),
                    content: String::new(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
            ],
        ),
    )?;

    let batch = packet_embedding_inputs_from_storage_since(&storage, watermark)?;

    assert_eq!(batch.conversations_in_batch, 1);
    assert_eq!(batch.inputs.len(), 1);
    assert_eq!(batch.inputs[0].content, "new packet semantic text");
    assert_eq!(
        batch.inputs[0].role,
        role_code_from_str("assistant").unwrap()
    );
    let normalized_source_id =
        normalized_index_source_id(Some("remote-laptop"), None, Some("builder-host"));
    assert_eq!(
        batch.inputs[0].source_id,
        crc32fast::hash(normalized_source_id.as_bytes())
    );
    let expected_raw_max_id: i64 = storage.raw().query_row_map(
        "SELECT MAX(id) FROM messages",
        &[] as &[ParamValue],
        |row| row.get_typed(0),
    )?;
    assert_eq!(batch.raw_max_message_id, Some(expected_raw_max_id));
    Ok(())
}

#[test]
fn packet_catch_up_emits_expected_semantic_docs_after_watermark() -> Result<()> {
    let temp = tempdir().unwrap();
    let db_path = temp.path().join("agent_search.db");
    let storage = FrankenStorage::open(&db_path)?;
    let agent_id = storage.ensure_agent(&Agent {
        id: None,
        slug: "codex".to_string(),
        name: "Codex".to_string(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    let workspace_id = storage.ensure_workspace(Path::new("/tmp/workspace"), None)?;

    storage.insert_conversation_tree(
        agent_id,
        Some(workspace_id),
        &test_conversation_with_messages(
            "legacy-packet-semantics",
            vec![
                Message {
                    id: None,
                    idx: 0,
                    role: MessageRole::User,
                    author: None,
                    created_at: Some(1_700_000_000_500),
                    content: "before watermark".to_string(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
                Message {
                    id: None,
                    idx: 1,
                    role: MessageRole::Agent,
                    author: None,
                    created_at: Some(1_700_000_000_600),
                    content: "before watermark assistant".to_string(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
            ],
        ),
    )?;

    let watermark: i64 = storage.raw().query_row_map(
        "SELECT MAX(id) FROM messages",
        &[] as &[ParamValue],
        |row| row.get_typed(0),
    )?;

    storage.insert_conversation_tree(
        agent_id,
        Some(workspace_id),
        &test_conversation_with_messages(
            "legacy-packet-semantics",
            vec![
                Message {
                    id: None,
                    idx: 0,
                    role: MessageRole::User,
                    author: None,
                    created_at: Some(1_700_000_000_500),
                    content: "before watermark".to_string(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
                Message {
                    id: None,
                    idx: 1,
                    role: MessageRole::Agent,
                    author: None,
                    created_at: Some(1_700_000_000_600),
                    content: "before watermark assistant".to_string(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
                Message {
                    id: None,
                    idx: 2,
                    role: MessageRole::Agent,
                    author: None,
                    created_at: Some(1_700_000_000_700),
                    content: "after watermark assistant".to_string(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
                Message {
                    id: None,
                    idx: 3,
                    role: MessageRole::System,
                    author: None,
                    created_at: Some(1_700_000_000_800),
                    content: String::new(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
            ],
        ),
    )?;
    storage.insert_conversation_tree(
        agent_id,
        Some(workspace_id),
        &test_conversation_with_messages(
            "legacy-packet-semantics-second-conv",
            vec![
                Message {
                    id: None,
                    idx: 0,
                    role: MessageRole::Tool,
                    author: None,
                    created_at: Some(1_700_000_000_900),
                    content: "after watermark tool".to_string(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
                Message {
                    id: None,
                    idx: 1,
                    role: MessageRole::System,
                    author: None,
                    created_at: Some(1_700_000_001_000),
                    content: String::new(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
            ],
        ),
    )?;

    let packet_batch = packet_embedding_inputs_from_storage_since(&storage, watermark)?;
    let normalized_source_id =
        normalized_index_source_id(Some("remote-laptop"), None, Some("builder-host"));
    let source_id_hash = crc32fast::hash(normalized_source_id.as_bytes());
    let expected = vec![
        ComparableSemanticInput {
            message_id: u64::try_from(watermark + 1).unwrap(),
            created_at_ms: 1_700_000_000_700,
            agent_id: u32::try_from(agent_id).unwrap(),
            workspace_id: u32::try_from(workspace_id).unwrap(),
            source_id: source_id_hash,
            role: role_code_from_str("assistant").unwrap(),
            content: "after watermark assistant".to_string(),
        },
        ComparableSemanticInput {
            message_id: u64::try_from(watermark + 3).unwrap(),
            created_at_ms: 1_700_000_000_900,
            agent_id: u32::try_from(agent_id).unwrap(),
            workspace_id: u32::try_from(workspace_id).unwrap(),
            source_id: source_id_hash,
            role: role_code_from_str("tool").unwrap(),
            content: "after watermark tool".to_string(),
        },
    ];

    assert_eq!(comparable_semantic_inputs(packet_batch.inputs), expected);
    assert_eq!(packet_batch.conversations_in_batch, 2);
    assert_eq!(packet_batch.raw_max_message_id, Some(watermark + 4));
    Ok(())
}

#[test]
fn packet_embedding_inputs_for_message_ids_matches_since_selection() -> Result<()> {
    let temp = tempdir().unwrap();
    let db_path = temp.path().join("agent_search.db");
    let storage = FrankenStorage::open(&db_path)?;
    let agent_id = storage.ensure_agent(&Agent {
        id: None,
        slug: "codex".to_string(),
        name: "Codex".to_string(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    let workspace_id = storage.ensure_workspace(Path::new("/tmp/workspace"), None)?;

    storage.insert_conversation_tree(
        agent_id,
        Some(workspace_id),
        &test_conversation_with_messages(
            "selected-vs-since",
            vec![
                Message {
                    id: None,
                    idx: 0,
                    role: MessageRole::User,
                    author: None,
                    created_at: Some(1_700_000_100_100),
                    content: "before watermark".to_string(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
                Message {
                    id: None,
                    idx: 1,
                    role: MessageRole::Agent,
                    author: None,
                    created_at: Some(1_700_000_100_200),
                    content: "before watermark assistant".to_string(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
            ],
        ),
    )?;

    let watermark: i64 = storage.raw().query_row_map(
        "SELECT MAX(id) FROM messages",
        &[] as &[ParamValue],
        |row| row.get_typed(0),
    )?;

    storage.insert_conversation_tree(
        agent_id,
        Some(workspace_id),
        &test_conversation_with_messages(
            "selected-vs-since",
            vec![
                Message {
                    id: None,
                    idx: 0,
                    role: MessageRole::User,
                    author: None,
                    created_at: Some(1_700_000_100_100),
                    content: "before watermark".to_string(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
                Message {
                    id: None,
                    idx: 1,
                    role: MessageRole::Agent,
                    author: None,
                    created_at: Some(1_700_000_100_200),
                    content: "before watermark assistant".to_string(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
                Message {
                    id: None,
                    idx: 2,
                    role: MessageRole::Tool,
                    author: None,
                    created_at: Some(1_700_000_100_300),
                    content: "after watermark tool".to_string(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
                Message {
                    id: None,
                    idx: 3,
                    role: MessageRole::System,
                    author: None,
                    created_at: Some(1_700_000_100_400),
                    content: String::new(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
            ],
        ),
    )?;
    storage.insert_conversation_tree(
        agent_id,
        Some(workspace_id),
        &test_conversation_with_messages(
            "selected-vs-since-second",
            vec![Message {
                id: None,
                idx: 0,
                role: MessageRole::Agent,
                author: None,
                created_at: Some(1_700_000_100_500),
                content: "after watermark assistant".to_string(),
                extra_json: json!({}),
                snippets: Vec::new(),
            }],
        ),
    )?;

    let since_batch = packet_embedding_inputs_from_storage_since(&storage, watermark)?;
    let conversation_ids: Vec<i64> = storage.raw().query_map_collect(
        "SELECT DISTINCT conversation_id
         FROM messages
         WHERE id > ?1
         ORDER BY conversation_id ASC",
        &[ParamValue::from(watermark)],
        |row| row.get_typed(0),
    )?;
    let selected_message_ids: HashSet<i64> = storage
        .raw()
        .query_map_collect(
            "SELECT id
             FROM messages
             WHERE id > ?1
             ORDER BY id ASC",
            &[ParamValue::from(watermark)],
            |row| row.get_typed(0),
        )?
        .into_iter()
        .collect();
    let selected_inputs = packet_embedding_inputs_from_storage_for_message_ids(
        &storage,
        &conversation_ids,
        &selected_message_ids,
    )?;

    assert_eq!(
        comparable_semantic_inputs(selected_inputs),
        comparable_semantic_inputs(since_batch.inputs)
    );
    Ok(())
}

#[test]
fn default_batch_size_uses_new_value() {
    // The test setup must not leak a caller-provided CASS_SEMANTIC_BATCH_SIZE
    // override, which would mask the constant bump we're asserting on.
    let _guard = EnvVarGuard::remove("CASS_SEMANTIC_BATCH_SIZE");
    let indexer = SemanticIndexer::new("hash", None).unwrap();
    assert_eq!(indexer.batch_size(), DEFAULT_SEMANTIC_BATCH_SIZE);
}

#[test]
fn semantic_watchdog_and_checkpoint_caps_have_derived_defaults() {
    let _warn = EnvVarGuard::remove("CASS_SEMANTIC_EMBED_BATCH_WARN_AFTER_MS");
    let _fail = EnvVarGuard::remove("CASS_SEMANTIC_EMBED_BATCH_FAIL_AFTER_MS");
    let _messages = EnvVarGuard::remove("CASS_SEMANTIC_MAX_MESSAGES_PER_CHECKPOINT");
    let _bytes = EnvVarGuard::remove("CASS_SEMANTIC_MAX_BYTES_PER_CHECKPOINT");

    assert_eq!(
        resolved_semantic_embed_batch_warn_after_ms(),
        DEFAULT_SEMANTIC_EMBED_BATCH_WARN_AFTER_MS
    );
    assert_eq!(
        resolved_semantic_embed_batch_fail_after_ms(),
        DEFAULT_SEMANTIC_EMBED_BATCH_FAIL_AFTER_MS
    );
    assert_eq!(
        SemanticCheckpointCaps::from_env(),
        SemanticCheckpointCaps {
            max_messages: DEFAULT_SEMANTIC_MAX_MESSAGES_PER_CHECKPOINT,
            max_bytes: DEFAULT_SEMANTIC_MAX_BYTES_PER_CHECKPOINT,
        }
    );
}

#[test]
fn parallel_prep_enabled_reuses_truthy_env_parser() {
    for (value, expected) in [
        ("1", true),
        ("true", true),
        (" YeS ", true),
        ("on", true),
        ("0", false),
        ("false", false),
        ("off", false),
    ] {
        let _guard = EnvVarGuard::set("CASS_SEMANTIC_PREP_PARALLEL", value);
        assert_eq!(parallel_prep_enabled(), expected, "env value {value:?}");
    }

    let _guard = EnvVarGuard::remove("CASS_SEMANTIC_PREP_PARALLEL");
    assert!(!parallel_prep_enabled());
}

#[test]
fn saturating_u64_from_usize_covers_bounds() {
    assert_eq!(saturating_u64_from_usize(0), 0);
    assert_eq!(saturating_u64_from_usize(42), 42);
    assert_eq!(
        saturating_u64_from_usize(usize::MAX),
        u64::try_from(usize::MAX).unwrap_or(u64::MAX)
    );
}

/// `coding_agent_session_search-ibuuh.32` (sink #3 equivalence gate):
/// the packet-driven `semantic_inputs_from_packets` helper must
/// produce the same `EmbeddingInput` list a fresh storage replay
/// returns for the same canonical corpus. Once this passes, callers
/// that already hold packets (rebuild pipeline, salvage replay,
/// repair flows) can drive the semantic preparation consumer
/// without a second canonical-row round-trip.
#[test]
fn issue_342_semantic_replay_reports_progress_and_matches_packets() -> Result<()> {
    let temp = tempdir().unwrap();
    let db_path = temp.path().join("agent_search.db");
    let storage = FrankenStorage::open(&db_path)?;

    let agent_id_codex = storage.ensure_agent(&Agent {
        id: None,
        slug: "codex".to_string(),
        name: "Codex".to_string(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    let agent_id_claude = storage.ensure_agent(&Agent {
        id: None,
        slug: "claude_code".to_string(),
        name: "Claude Code".to_string(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    let workspace_id = storage.ensure_workspace(Path::new("/tmp/semantic-equivalence-ws"), None)?;

    // Two conversations on different agents, mixed roles, including
    // an empty-content system message that the semantic projection
    // must filter (matches the legacy storage replay).
    storage.insert_conversation_tree(
        agent_id_codex,
        Some(workspace_id),
        &test_conversation_with_messages(
            "packet-equiv-1",
            vec![
                Message {
                    id: None,
                    idx: 0,
                    role: MessageRole::User,
                    author: None,
                    created_at: Some(1_700_000_000_500),
                    content: "first user prompt".to_string(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
                Message {
                    id: None,
                    idx: 1,
                    role: MessageRole::Agent,
                    author: None,
                    created_at: Some(1_700_000_000_600),
                    content: "first assistant reply".to_string(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
                Message {
                    id: None,
                    idx: 2,
                    role: MessageRole::System,
                    author: None,
                    created_at: Some(1_700_000_000_700),
                    // Empty content is filtered by both paths.
                    content: String::new(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
            ],
        ),
    )?;
    storage.insert_conversation_tree(
        agent_id_claude,
        Some(workspace_id),
        &test_conversation_with_messages(
            "packet-equiv-2",
            vec![
                Message {
                    id: None,
                    idx: 0,
                    role: MessageRole::Tool,
                    author: Some("ripgrep".to_string()),
                    created_at: Some(1_700_000_001_500),
                    content: "tool output line".to_string(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
                Message {
                    id: None,
                    idx: 1,
                    role: MessageRole::Agent,
                    author: None,
                    created_at: Some(1_700_000_001_600),
                    content: "second assistant reply".to_string(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
            ],
        ),
    )?;

    // Legacy path: the storage-driven replay that the rebuild
    // pipeline currently uses.
    let mut replay_progress = Vec::new();
    let storage_inputs =
        packet_embedding_inputs_from_storage_with_progress(&storage, |current, total| {
            replay_progress.push((current, total))
        })?;
    assert_eq!(replay_progress.last(), Some(&(2, 2)));

    // Packet-driven path: re-fetch the canonical envelopes (so we
    // get the storage-internal agent/workspace ids the rebuild path
    // would normally pair with packets), then convert those rows
    // into ConversationPackets via canonical replay and feed them
    // through `semantic_inputs_from_packets`.
    let conversation_ids: Vec<i64> = storage.raw().query_map_collect(
        "SELECT DISTINCT m.conversation_id
         FROM messages m
         JOIN conversations c ON c.id = m.conversation_id
         ORDER BY m.conversation_id ASC",
        &[] as &[ParamValue],
        |row| row.get_typed(0),
    )?;
    let envelopes = fetch_canonical_embedding_conversations(&storage, &conversation_ids)?;
    let mut grouped_messages =
        storage.fetch_messages_for_lexical_rebuild_batch(&conversation_ids, None, None)?;
    let mut packets: Vec<ConversationPacket> = Vec::with_capacity(envelopes.len());
    let mut contexts: Vec<SemanticPacketContext> = Vec::with_capacity(envelopes.len());
    for envelope in &envelopes {
        let messages = grouped_messages
            .remove(&envelope.conversation_id)
            .unwrap_or_default();
        let provenance = canonical_embedding_packet_provenance(envelope);
        let canonical = canonical_embedding_conversation(envelope, &provenance, messages);
        packets.push(ConversationPacket::from_canonical_replay(
            &canonical, provenance,
        ));
        contexts.push(SemanticPacketContext {
            conversation_id: envelope.conversation_id,
            agent_id: saturating_u32_from_i64(envelope.agent_id),
            workspace_id: saturating_u32_from_i64(envelope.workspace_id.unwrap_or(0)),
        });
    }
    let packet_inputs = semantic_inputs_from_packets(&packets, &contexts)?;

    // The two paths must produce the same EmbeddingInput list
    // (sortable comparison normalizes ordering across the two
    // helpers' iteration orders).
    assert!(
        !storage_inputs.is_empty(),
        "fixture should produce non-empty semantic inputs (sanity)"
    );
    assert_eq!(
        comparable_semantic_inputs(storage_inputs.clone()),
        comparable_semantic_inputs(packet_inputs.clone()),
        "packet-driven semantic preparation must match storage replay byte-for-byte"
    );

    // Sanity-pin a couple of contract details so a regression in
    // either path (e.g. role normalization or empty-content
    // filtering) trips a clear assertion rather than a generic
    // length mismatch.
    let storage_count = storage_inputs.len();
    let packet_count = packet_inputs.len();
    assert_eq!(
        storage_count, packet_count,
        "storage and packet semantic input counts must agree exactly"
    );
    // Empty-content system message must NOT appear in the output.
    assert!(
        packet_inputs.iter().all(|input| !input.content.is_empty()),
        "empty content must be filtered by the packet semantic projection"
    );
    // The remote-host source_id pins the cross-path provenance hash.
    let normalized_source_id =
        normalized_index_source_id(Some("remote-laptop"), None, Some("builder-host"));
    let expected_hash = crc32fast::hash(normalized_source_id.as_bytes());
    assert!(
        packet_inputs
            .iter()
            .all(|input| input.source_id == expected_hash),
        "every emitted EmbeddingInput must hash provenance via the packet's normalized source_id"
    );

    Ok(())
}

/// Length-mismatch defense: if a caller hands `semantic_inputs_from_packets`
/// a packet/context slice pair of different lengths, the helper must
/// return an error rather than silently mis-correlating ids. Pinning
/// this is part of the bead's "shadow / compare mode plus explicit
/// kill-switch" acceptance language.
#[test]
fn semantic_inputs_from_packets_rejects_length_mismatch() {
    let provenance = ConversationPacketProvenance::local();
    let canonical = test_conversation("packet-mismatch", "hello");
    let packet = ConversationPacket::from_canonical_replay(&canonical, provenance);
    let result = semantic_inputs_from_packets(&[packet], &[]);
    assert!(
        result.is_err(),
        "expected error on packet/context length mismatch"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("length mismatch"),
        "error should mention length mismatch, got: {err}"
    );
}
