use super::super::*;
use super::helpers::*;
use crate::model::types::{Agent, AgentKind, Message, MessageRole};
use crate::storage::sqlite::FrankenStorage;
use serde_json::json;
use tempfile::tempdir;

#[test]
fn canonical_embedding_batch_uses_conversation_packet_semantic_projection() -> Result<()> {
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
            "packet-projection",
            vec![
                Message {
                    id: None,
                    idx: 0,
                    role: MessageRole::User,
                    author: None,
                    created_at: Some(1_700_000_000_500),
                    content: "user semantic text".to_string(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
                Message {
                    id: None,
                    idx: 1,
                    role: MessageRole::Tool,
                    author: None,
                    created_at: Some(1_700_000_000_600),
                    content: "tool semantic text".to_string(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
                Message {
                    id: None,
                    idx: 2,
                    role: MessageRole::System,
                    author: None,
                    created_at: Some(1_700_000_000_700),
                    content: String::new(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
            ],
        ),
    )?;

    let batch = fetch_canonical_embedding_batch(&storage, 0, 1)?;

    assert_eq!(batch.conversations_in_batch, 1);
    assert_eq!(batch.inputs.len(), 2);
    assert_eq!(batch.inputs[0].content, "user semantic text");
    assert_eq!(batch.inputs[1].content, "tool semantic text");
    assert_eq!(batch.inputs[0].role, role_code_from_str("user").unwrap());
    assert_eq!(batch.inputs[1].role, role_code_from_str("tool").unwrap());
    let normalized_source_id =
        normalized_index_source_id(Some("remote-laptop"), None, Some("builder-host"));
    let expected_hash = crc32fast::hash(normalized_source_id.as_bytes());
    assert_eq!(batch.inputs[0].source_id, expected_hash);
    assert_eq!(batch.inputs[1].source_id, expected_hash);
    Ok(())
}

#[test]
fn canonical_embedding_batch_pushes_last_message_id_filter_into_selection() -> Result<()> {
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
        &test_conversation("before-watermark", "old semantic message"),
    )?;
    let watermark: i64 = storage.raw().query_row_map(
        "SELECT MAX(id) FROM messages",
        &[] as &[ParamValue],
        |row| row.get_typed(0),
    )?;
    storage.insert_conversation_tree(
        agent_id,
        None,
        &test_conversation("after-watermark", "new semantic message"),
    )?;

    let batch = fetch_canonical_embedding_batch_inner(&storage, 0, 8, Some(watermark))?;

    assert_eq!(
        batch.conversations_in_batch, 1,
        "last_message_id must be pushed into candidate selection so old conversations are not counted in the batch"
    );
    assert_eq!(batch.total_conversations, 2);
    anyhow::ensure!(
        batch.cursor_exhausted,
        "message-cursor selection should exhaust the remaining cursor"
    );
    assert_eq!(batch.inputs.len(), 1);
    assert_eq!(batch.inputs[0].content, "new semantic message");
    assert!(
        i64::try_from(batch.inputs[0].message_id).unwrap_or(i64::MAX) > watermark,
        "selected semantic input must be strictly newer than the checkpoint message id"
    );
    Ok(())
}

#[test]
fn semantic_conversation_total_uses_the_production_schema_aggregate() -> Result<()> {
    let temp = tempdir()?;
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
        &test_conversation("counted-first", "first counted semantic message"),
    )?;
    storage.insert_conversation_tree(
        agent_id,
        None,
        &test_conversation("counted-second", "second counted semantic message"),
    )?;
    storage.insert_conversation_tree(
        agent_id,
        None,
        &test_conversation_with_messages("empty-not-counted", Vec::new()),
    )?;

    // Legacy imports may have messages without either tail cache. Preserve
    // exactness by forcing one populated conversation through the residual
    // indexed point-probe path.
    let first_conversation_id: i64 = storage.raw().query_row_map(
        "SELECT MIN(id) FROM conversations",
        &[] as &[ParamValue],
        |row| row.get_typed(0),
    )?;
    let params = [ParamValue::from(first_conversation_id)];
    storage.raw().execute_compat(
        "UPDATE conversations SET last_message_idx = NULL WHERE id = ?1",
        &params,
    )?;
    storage.raw().execute_compat(
        "UPDATE conversation_tail_state
         SET last_message_idx = NULL
         WHERE conversation_id = ?1",
        &params,
    )?;

    let production_columns: Vec<String> = storage.raw().query_map_collect(
        "PRAGMA table_info(conversations)",
        &[] as &[ParamValue],
        |row| row.get_typed(1),
    )?;
    assert!(
        !production_columns
            .iter()
            .any(|name| name == "message_count"),
        "the regression fixture must use the production schema without conversations.message_count"
    );
    assert_eq!(total_semantic_conversations(&storage)?, 2);
    Ok(())
}

#[test]
fn semantic_total_cache_is_scoped_to_the_active_build_identity() {
    let mut manifest = SemanticManifest {
        checkpoint: Some(BuildCheckpoint {
            tier: TierKind::Fast,
            embedder_id: "fnv1a-384".to_string(),
            last_offset: 91,
            docs_embedded: 1_247,
            conversations_processed: 91,
            total_conversations: 15_246,
            db_fingerprint: "content-v1:432504:1:432504".to_string(),
            schema_version: SEMANTIC_SCHEMA_VERSION,
            chunking_version: CHUNKING_STRATEGY_VERSION,
            saved_at_ms: 1,
            last_message_id: Some(1_250),
            cursor_exhausted: false,
        }),
        ..SemanticManifest::default()
    };

    assert_eq!(
        cached_semantic_total_conversations(
            &manifest,
            TierKind::Fast,
            "fnv1a-384",
            "content-v1:432504:1:432504"
        ),
        Some((15_246, "checkpoint"))
    );
    assert_eq!(
        cached_semantic_total_conversations(
            &manifest,
            TierKind::Quality,
            "minilm-384",
            "content-v1:432504:1:432504"
        ),
        None,
        "a different tier/embedder must not inherit an unrelated checkpoint total"
    );
    assert_eq!(
        cached_semantic_total_conversations(
            &manifest,
            TierKind::Fast,
            "fnv1a-384",
            "content-v1:432505:1:432505"
        ),
        None,
        "a changed DB fingerprint must force a fresh aggregate"
    );
    manifest
        .checkpoint
        .as_mut()
        .expect("checkpoint")
        .schema_version = SEMANTIC_SCHEMA_VERSION.saturating_add(1);
    assert_eq!(
        cached_semantic_total_conversations(
            &manifest,
            TierKind::Fast,
            "fnv1a-384",
            "content-v1:432504:1:432504"
        ),
        None,
        "an incompatible semantic schema must invalidate the cached total"
    );
}

#[test]
fn resumed_semantic_candidate_selection_is_cursor_bounded() -> Result<()> {
    let temp = tempdir()?;
    let db_path = temp.path().join("agent_search.db");
    let storage = FrankenStorage::open(&db_path)?;
    let agent_id = storage.ensure_agent(&Agent {
        id: None,
        slug: "codex".to_string(),
        name: "Codex".to_string(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    for index in 0..192 {
        storage.insert_conversation_tree(
            agent_id,
            None,
            &test_conversation(
                &format!("resume-before-{index}"),
                &format!("old semantic message {index}"),
            ),
        )?;
    }
    let checkpoint_conversation_id: i64 = storage.raw().query_row_map(
        "SELECT MAX(id) FROM conversations",
        &[] as &[ParamValue],
        |row| row.get_typed(0),
    )?;
    let checkpoint_message_id: i64 = storage.raw().query_row_map(
        "SELECT MAX(id) FROM messages",
        &[] as &[ParamValue],
        |row| row.get_typed(0),
    )?;
    for index in 0..128 {
        storage.insert_conversation_tree(
            agent_id,
            None,
            &test_conversation(
                &format!("resume-after-{index}"),
                &format!("new semantic message {index}"),
            ),
        )?;
    }

    let total = total_semantic_conversations(&storage)?;
    assert_eq!(total, 320);
    let started = Instant::now();
    let batch = fetch_canonical_embedding_batch_inner_with_caps_and_total(
        &storage,
        checkpoint_conversation_id,
        64,
        Some(checkpoint_message_id),
        SemanticCheckpointCaps::unlimited(),
        total,
        None,
    )?;

    assert_eq!(batch.conversations_in_batch, 64);
    assert_eq!(batch.inputs.len(), 64);
    assert_eq!(batch.total_conversations, 320);
    assert!(!batch.cursor_exhausted);
    assert!(
        batch.inputs.iter().all(|input| {
            i64::try_from(input.message_id).unwrap_or(i64::MAX) > checkpoint_message_id
        }),
        "every selected message must advance past the durable message cursor"
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(10),
        "a 64-conversation resume must not replay the whole canonical corpus"
    );
    Ok(())
}

#[test]
fn resumed_semantic_candidate_selection_streams_across_sparse_parent_gap() -> Result<()> {
    const CHECKPOINT_CONVERSATION_ID: i64 = 1;
    const CHECKPOINT_MESSAGE_ID: i64 = 6_669;
    const FIRST_ELIGIBLE_CONVERSATION_ID: i64 = 6_670;
    const SECOND_ELIGIBLE_CONVERSATION_ID: i64 = 6_671;
    const EARLY_MESSAGE_LATE_CONVERSATION_ID: i64 = 6_672;

    fn linux_rss_bytes() -> Option<u64> {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        let rss_kib = status.lines().find_map(|line| {
            line.strip_prefix("VmRSS:")?
                .split_whitespace()
                .next()?
                .parse::<u64>()
                .ok()
        })?;
        rss_kib.checked_mul(1_024)
    }

    let temp = tempdir()?;
    let db_path = temp.path().join("agent_search.db");
    let bootstrap_storage = FrankenStorage::open(&db_path)?;
    let agent_id = bootstrap_storage.ensure_agent(&Agent {
        id: None,
        slug: "codex".to_string(),
        name: "Codex".to_string(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    // Reproduce #348's production shape without spending the test budget
    // on fixture construction through thousands of individual commits.
    // Conversations 2..=6,669 are newer parents whose messages are all at
    // or below the durable message cursor: 6,668 consecutive misses before
    // the first eligible conversation in conversation-id order.
    // Seed through FrankenSQLite itself (the project's sole SQLite engine).
    // Multi-row statements are chunked so fixture construction stays
    // parser-bounded while one outer transaction avoids thousands of
    // fsyncs. Every interpolated value below is an integer controlled by
    // this test; no external string enters the SQL.
    use std::fmt::Write as _;
    const SEED_ROWS_PER_STATEMENT: i64 = 256;
    let mut seed_sql = String::from("BEGIN IMMEDIATE;");
    let mut chunk_start = CHECKPOINT_CONVERSATION_ID;
    while chunk_start <= EARLY_MESSAGE_LATE_CONVERSATION_ID {
        let chunk_end =
            (chunk_start + SEED_ROWS_PER_STATEMENT - 1).min(EARLY_MESSAGE_LATE_CONVERSATION_ID);
        seed_sql.push_str(
            "INSERT INTO conversations(
                 id, agent_id, source_id, external_id, source_path
             ) VALUES",
        );
        for conversation_id in chunk_start..=chunk_end {
            if conversation_id > chunk_start {
                seed_sql.push(',');
            }
            write!(
                seed_sql,
                "({conversation_id},{agent_id},'local','cass-348-sparse-{conversation_id}',\
                 '/tmp/cass-348/{conversation_id}.jsonl')"
            )
            .expect("writing fixture SQL to String cannot fail");
        }
        seed_sql.push(';');

        seed_sql.push_str(
            "INSERT INTO conversation_tail_state(conversation_id, last_message_idx) VALUES",
        );
        for conversation_id in chunk_start..=chunk_end {
            if conversation_id > chunk_start {
                seed_sql.push(',');
            }
            write!(seed_sql, "({conversation_id},0)")
                .expect("writing fixture SQL to String cannot fail");
        }
        seed_sql.push(';');
        chunk_start = chunk_end + 1;
    }

    let mut message_chunk_start = 2_i64;
    while message_chunk_start < FIRST_ELIGIBLE_CONVERSATION_ID {
        let message_chunk_end = (message_chunk_start + SEED_ROWS_PER_STATEMENT - 1)
            .min(FIRST_ELIGIBLE_CONVERSATION_ID - 1);
        seed_sql.push_str(
            "INSERT INTO messages(
                 id, conversation_id, idx, role, content
             ) VALUES",
        );
        for conversation_id in message_chunk_start..=message_chunk_end {
            if conversation_id > message_chunk_start {
                seed_sql.push(',');
            }
            let message_id = conversation_id - 1;
            write!(
                seed_sql,
                "({message_id},{conversation_id},0,'user','sparse semantic resume row')"
            )
            .expect("writing fixture SQL to String cannot fail");
        }
        seed_sql.push(';');
        message_chunk_start = message_chunk_end + 1;
    }

    // Global message order intentionally disagrees with conversation
    // order. A correct bounded selector must evict 6,672 and return
    // 6,670/6,671, otherwise advancing the conversation checkpoint would
    // permanently skip an older eligible conversation.
    write!(
        seed_sql,
        "INSERT INTO messages(id, conversation_id, idx, role, content) VALUES
         ({CHECKPOINT_MESSAGE_ID},{CHECKPOINT_CONVERSATION_ID},0,'user','checkpoint row'),
         ({},{EARLY_MESSAGE_LATE_CONVERSATION_ID},0,'user','eligible row'),
         ({},{FIRST_ELIGIBLE_CONVERSATION_ID},0,'user','eligible row'),
         ({},{SECOND_ELIGIBLE_CONVERSATION_ID},0,'user','eligible row');COMMIT;",
        CHECKPOINT_MESSAGE_ID + 1,
        CHECKPOINT_MESSAGE_ID + 2,
        CHECKPOINT_MESSAGE_ID + 3,
    )
    .expect("writing fixture SQL to String cannot fail");
    bootstrap_storage.raw().execute_batch(&seed_sql)?;
    drop(bootstrap_storage);

    let storage = FrankenStorage::open(&db_path)?;
    let rss_before = linux_rss_bytes();
    let started = Instant::now();
    let (selected, eligible_message_rows_scanned) =
        fetch_bounded_semantic_candidate_conversation_ids(
            &storage,
            CHECKPOINT_CONVERSATION_ID,
            Some(CHECKPOINT_MESSAGE_ID),
            2,
        )?;
    let elapsed = started.elapsed();
    let rss_after = linux_rss_bytes();

    assert_eq!(
        selected,
        [
            FIRST_ELIGIBLE_CONVERSATION_ID,
            SECOND_ELIGIBLE_CONVERSATION_ID
        ]
    );
    assert_eq!(
        eligible_message_rows_scanned, 3,
        "the selector should stream only eligible post-cursor messages, not execute 6,668 parent probes"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(10),
        "sparse-gap selection took {elapsed:?}"
    );
    if let (Some(before), Some(after)) = (rss_before, rss_after) {
        assert!(
            after.saturating_sub(before) < 256 * 1024 * 1024,
            "sparse-gap selection grew RSS by {} bytes",
            after.saturating_sub(before)
        );
    }
    Ok(())
}

/// #383: the tail-state fast path must answer the has-message-after-cursor
/// question from `conversation_tail_state` + one exact `(conversation_id,
/// idx)` point lookup, and must decline (None) when the tail row or its
/// message is absent so the caller can fall back to the exhaustive probe.
#[test]
fn tail_state_fast_path_answers_message_after_cursor() -> Result<()> {
    let temp = tempdir()?;
    let db_path = temp.path().join("agent_search.db");
    let storage = FrankenStorage::open(&db_path)?;
    let agent_id = storage.ensure_agent(&Agent {
        id: None,
        slug: "codex".to_string(),
        name: "Codex".to_string(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    storage.raw().execute_batch(&format!(
        "BEGIN;
         INSERT INTO conversations(id, agent_id, source_id, external_id, source_path)
         VALUES (1,{agent_id},'local','cass-383-a','/tmp/cass-383/a.jsonl'),
                (2,{agent_id},'local','cass-383-b','/tmp/cass-383/b.jsonl'),
                (3,{agent_id},'local','cass-383-c','/tmp/cass-383/c.jsonl');
         INSERT INTO messages(id, conversation_id, idx, role, content)
         VALUES (10,1,0,'user','a0'),(11,1,1,'assistant','a1'),
                (20,2,0,'user','b0'),(21,2,1,'assistant','b1');
         INSERT INTO conversation_tail_state(conversation_id, last_message_idx)
         VALUES (1,1),(2,1),(3,7);
         COMMIT;"
    ))?;

    // Tail message id 11 vs cursors on both sides.
    assert_eq!(
        semantic_tail_state_has_message_after(&storage, 1, 10)?,
        Some(true),
        "tail id 11 > cursor 10 must report a pending message"
    );
    assert_eq!(
        semantic_tail_state_has_message_after(&storage, 1, 11)?,
        Some(false),
        "tail id 11 <= cursor 11 must report the conversation as covered"
    );
    // Conversation 3 has a tail row pointing at a missing message: the
    // fast path must decline rather than guess.
    assert_eq!(
        semantic_tail_state_has_message_after(&storage, 3, 0)?,
        None,
        "missing tail message must fall back to the exhaustive probe"
    );
    // No tail row at all (id 99): decline as well.
    assert_eq!(
        semantic_tail_state_has_message_after(&storage, 99, 0)?,
        None
    );

    // The public probe agrees with the fast path and with the fallback.
    assert!(semantic_conversation_has_message_after(
        &storage,
        2,
        Some(20)
    )?);
    assert!(!semantic_conversation_has_message_after(
        &storage,
        2,
        Some(21)
    )?);
    assert!(!semantic_conversation_has_message_after(
        &storage,
        3,
        Some(0)
    )?);
    Ok(())
}

#[test]
#[ignore = "large 15,246-conversation / 432,504-message acceptance proof for cass#343"]
fn resumed_semantic_selection_matches_large_production_shape() -> Result<()> {
    const CONVERSATION_COUNT: i64 = 15_246;
    const MESSAGE_COUNT: i64 = 432_504;
    const CONVERSATIONS_WITH_EXTRA_MESSAGE: i64 = MESSAGE_COUNT - (CONVERSATION_COUNT * 28);

    fn linux_rss_bytes() -> Option<u64> {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        let rss_kib = status.lines().find_map(|line| {
            line.strip_prefix("VmRSS:")?
                .split_whitespace()
                .next()?
                .parse::<u64>()
                .ok()
        })?;
        rss_kib.checked_mul(1_024)
    }

    let temp = tempdir()?;
    let db_path = temp.path().join("agent_search.db");
    let bootstrap_storage = FrankenStorage::open(&db_path)?;
    let agent_id = bootstrap_storage.ensure_agent(&Agent {
        id: None,
        slug: "codex".to_string(),
        name: "Codex".to_string(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    drop(bootstrap_storage);

    // Fixture construction is deliberately outside the measured region.
    // Use the repo's existing C-SQLite interop dependency and prepared
    // statements so creating 432k rows does not dominate this ignored
    // acceptance test. The actual count and selection proof below reopens
    // the database through FrankenStorage/FrankenSQLite.
    let mut sqlite = rusqlite::Connection::open(&db_path)?;
    let tx = sqlite.transaction()?;
    let mut message_id = 1_i64;
    {
        let mut insert_conversation = tx.prepare(
            "INSERT INTO conversations(
                 id, agent_id, source_id, external_id, source_path
             ) VALUES(?1, ?2, 'local', ?3, ?4)",
        )?;
        let mut insert_message = tx.prepare(
            "INSERT INTO messages(
                 id, conversation_id, idx, role, content
             ) VALUES(?1, ?2, ?3, 'user', 'semantic acceptance row')",
        )?;
        let mut insert_tail = tx.prepare(
            "INSERT INTO conversation_tail_state(conversation_id, last_message_idx)
             VALUES(?1, ?2)",
        )?;
        for conversation_id in 1..=CONVERSATION_COUNT {
            let external_id = format!("cass-343-large-{conversation_id}");
            let source_path = format!("/tmp/cass-343/{conversation_id}.jsonl");
            insert_conversation.execute(rusqlite::params![
                conversation_id,
                agent_id,
                external_id,
                source_path
            ])?;
            let messages_in_conversation =
                28 + i64::from(conversation_id <= CONVERSATIONS_WITH_EXTRA_MESSAGE);
            for message_index in 0..messages_in_conversation {
                insert_message.execute(rusqlite::params![
                    message_id,
                    conversation_id,
                    message_index
                ])?;
                message_id = message_id.saturating_add(1);
            }
            insert_tail.execute(rusqlite::params![
                conversation_id,
                messages_in_conversation - 1
            ])?;
        }
    }
    tx.commit()?;
    drop(sqlite);
    assert_eq!(message_id - 1, MESSAGE_COUNT);

    let storage = FrankenStorage::open(&db_path)?;
    let count_started = Instant::now();
    let total = total_semantic_conversations(&storage)?;
    let count_elapsed = count_started.elapsed();
    assert_eq!(total, u64::try_from(CONVERSATION_COUNT)?);

    let rss_before = linux_rss_bytes();
    let selection_started = Instant::now();
    let batch = fetch_canonical_embedding_batch_inner_with_caps_and_total(
        &storage,
        91,
        64,
        Some(1_250),
        SemanticCheckpointCaps::unlimited(),
        total,
        None,
    )?;
    let selection_elapsed = selection_started.elapsed();
    let rss_after = linux_rss_bytes();

    eprintln!(
        "CASS_343_LARGE_SELECTION conversations={CONVERSATION_COUNT} messages={MESSAGE_COUNT} count_ms={} selection_ms={} rss_before={rss_before:?} rss_after={rss_after:?}",
        count_elapsed.as_millis(),
        selection_elapsed.as_millis()
    );
    assert_eq!(batch.conversations_in_batch, 64);
    assert_eq!(batch.inputs.len(), 64 * 29);
    assert_eq!(batch.total_conversations, 15_246);
    assert!(!batch.cursor_exhausted);
    assert!(
        batch.inputs.iter().all(|input| input.message_id > 1_250),
        "the durable nonzero message cursor must be applied before materialization"
    );
    assert!(
        count_elapsed < std::time::Duration::from_secs(30),
        "production-schema aggregate must not clone/execute and materialize per outer row"
    );
    assert!(
        selection_elapsed < std::time::Duration::from_secs(30),
        "64-conversation candidate selection must stay bounded on the 432k-message shape"
    );
    if let (Some(before), Some(after)) = (rss_before, rss_after) {
        assert!(
            after.saturating_sub(before) < 256 * 1024 * 1024,
            "bounded resume selection grew RSS by {} bytes",
            after.saturating_sub(before)
        );
    }
    Ok(())
}

#[test]
fn canonical_embedding_batch_reports_unexhausted_cursor_at_sql_limit() -> Result<()> {
    let temp = tempdir()?;
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
        &test_conversation("limit-first", "first limit semantic message"),
    )?;
    storage.insert_conversation_tree(
        agent_id,
        None,
        &test_conversation("limit-second", "second limit semantic message"),
    )?;

    let first = fetch_canonical_embedding_batch_inner(&storage, 0, 1, None)?;
    anyhow::ensure!(
        first.conversations_in_batch == 1,
        "wanted first batch to select 1 conversation, got {}",
        first.conversations_in_batch
    );
    anyhow::ensure!(
        !first.cursor_exhausted,
        "over-fetching one candidate should prove another conversation remains"
    );

    let second =
        fetch_canonical_embedding_batch_inner(&storage, first.last_conversation_id, 1, None)?;
    anyhow::ensure!(
        second.conversations_in_batch == 1,
        "wanted second batch to select 1 conversation, got {}",
        second.conversations_in_batch
    );
    anyhow::ensure!(
        second.cursor_exhausted,
        "the final page should report cursor exhaustion even when it exactly fills the requested limit"
    );
    Ok(())
}

#[test]
fn canonical_embedding_visitor_replays_every_bounded_page() -> Result<()> {
    let temp = tempdir()?;
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
        &test_conversation("visitor-first", "first visitor semantic message"),
    )?;
    storage.insert_conversation_tree(
        agent_id,
        None,
        &test_conversation("visitor-second", "second visitor semantic message"),
    )?;

    let mut contents = Vec::new();
    visit_packet_embedding_inputs_from_storage_with_limits(
        &storage,
        1,
        SemanticCheckpointCaps {
            max_messages: 1,
            max_bytes: 1,
        },
        |input| {
            contents.push(input.content);
            Ok(())
        },
    )?;

    assert_eq!(
        contents,
        [
            "first visitor semantic message",
            "second visitor semantic message"
        ]
    );
    Ok(())
}
