use super::super::*;
use super::helpers::*;
use crate::model::types::{Agent, AgentKind, Message, MessageRole};
use crate::storage::sqlite::FrankenStorage;
use serde_json::json;
use tempfile::tempdir;

#[test]
fn parallel_prep_matches_serial_prep_bitwise() {
    // Mix of short, long, empty, markdown, code-block, and unicode inputs
    // to make sure the canonicalizer is exercised across all of its paths.
    let inputs: Vec<EmbeddingInput> = (0..500)
        .map(|i| {
            let text = match i % 7 {
                0 => format!("Plain message number {i} with some ordinary words."),
                1 => format!("**Bold** and _italic_ markdown line {i}"),
                2 => format!(
                    "```rust\nfn example_{i}() {{\n    println!(\"code block {i}\");\n}}\n```\nfollow-up text"
                ),
                3 => String::new(), // empty — should be filtered
                4 => format!("   whitespace   galore   {i}   "),
                5 => format!("Unicode \u{00E9}\u{0301} (combining accent) and emoji \u{1F600} line {i}"),
                _ => format!(
                    "Mixed line {i}: `inline_code`, [link](http://x), {{braces}}, and \u{201C}curly quotes\u{201D}."
                ),
            };
            EmbeddingInput::new(i as u64, text)
        })
        .collect();

    let serial = prepare_window(&inputs, true);
    let parallel = prepare_window(&inputs, false);

    assert_eq!(
        serial.len(),
        parallel.len(),
        "serial and parallel prep should skip the same number of empty canonicals"
    );

    for (s, p) in serial.iter().zip(parallel.iter()) {
        assert_eq!(
            s.msg.message_id, p.msg.message_id,
            "ordering must be preserved between serial and parallel prep"
        );
        assert_eq!(
            s.canonical, p.canonical,
            "canonical form diverged between serial and parallel prep"
        );
        assert_eq!(
            s.hash, p.hash,
            "content hash diverged between serial and parallel prep"
        );
    }
}

#[test]
fn parallel_prep_filters_empty_canonicals() {
    let inputs = vec![
        EmbeddingInput::new(1, "valid content"),
        EmbeddingInput::new(2, ""),
        EmbeddingInput::new(3, "   \n\n   \t  "),
        EmbeddingInput::new(4, "more valid content"),
    ];

    let prepared = prepare_window(&inputs, false);
    let ids: Vec<u64> = prepared.iter().map(|p| p.msg.message_id).collect();

    assert!(ids.contains(&1));
    assert!(ids.contains(&4));
    // ids 2 and 3 should be dropped because their canonicals are empty.
    assert!(!ids.contains(&2));
    assert!(!ids.contains(&3));
}

#[test]
fn memoized_serial_prep_matches_stateless_prepare_window() {
    let inputs = vec![
        EmbeddingInput::new(1, "repeat me exactly"),
        EmbeddingInput::new(2, "repeat me exactly"),
        EmbeddingInput::new(3, "unique payload"),
        EmbeddingInput::new(4, ""),
        EmbeddingInput::new(5, "repeat me exactly"),
    ];

    let baseline = prepare_window(&inputs, true);
    let mut cache = ContentAddressedMemoCache::with_capacity(16);
    let memoized = prepare_window_with_memo(&inputs, &mut cache);

    assert_eq!(baseline.len(), memoized.len());
    for (plain, cached) in baseline.iter().zip(memoized.iter()) {
        assert_eq!(plain.msg.message_id, cached.msg.message_id);
        assert_eq!(plain.canonical, cached.canonical);
        assert_eq!(plain.hash, cached.hash);
    }
}

#[test]
fn semantic_prep_memo_key_uses_stable_content_hash_bytes() {
    let key = semantic_prep_memo_key("repeat me exactly");
    let expected = content_hash("repeat me exactly");

    assert_eq!(key.content_hash.as_bytes(), expected.as_slice());
    assert_eq!(key.content_hash.as_bytes().len(), expected.len());
    assert_eq!(key.algorithm, SEMANTIC_PREP_MEMO_ALGORITHM);
    assert_eq!(key.algorithm_version, SEMANTIC_PREP_MEMO_VERSION);
}

#[test]
fn memoized_serial_prep_reuses_duplicate_content_across_windows() {
    let inputs = vec![
        EmbeddingInput::new(1, "repeat me exactly"),
        EmbeddingInput::new(2, "repeat me exactly"),
        EmbeddingInput::new(3, "unique payload"),
        EmbeddingInput::new(4, ""),
        EmbeddingInput::new(5, "repeat me exactly"),
    ];

    let mut cache = ContentAddressedMemoCache::with_capacity(16);
    let prepared = prepare_window_with_memo(&inputs, &mut cache);
    let stats = cache.stats().clone();

    assert_eq!(prepared.len(), 4);
    assert_eq!(stats.hits, 2);
    assert_eq!(stats.misses, 3);
    assert_eq!(stats.inserts, 2);
    assert_eq!(stats.live_entries, 2);
}

#[test]
fn packet_embedding_inputs_reuse_memoized_prep_for_duplicate_content() -> Result<()> {
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
            "packet-memo-conv-one",
            vec![
                Message {
                    id: None,
                    idx: 0,
                    role: MessageRole::User,
                    author: None,
                    created_at: Some(1_700_000_010_100),
                    content: "shared semantic payload".to_string(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
                Message {
                    id: None,
                    idx: 1,
                    role: MessageRole::Agent,
                    author: None,
                    created_at: Some(1_700_000_010_200),
                    content: "unique semantic payload one".to_string(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
            ],
        ),
    )?;
    storage.insert_conversation_tree(
        agent_id,
        None,
        &test_conversation_with_messages(
            "packet-memo-conv-two",
            vec![
                Message {
                    id: None,
                    idx: 0,
                    role: MessageRole::Tool,
                    author: None,
                    created_at: Some(1_700_000_010_300),
                    content: "shared semantic payload".to_string(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
                Message {
                    id: None,
                    idx: 1,
                    role: MessageRole::Agent,
                    author: None,
                    created_at: Some(1_700_000_010_400),
                    content: "unique semantic payload two".to_string(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
            ],
        ),
    )?;

    let packet_inputs = packet_embedding_inputs_from_storage(&storage)?;
    let mut cache = ContentAddressedMemoCache::with_capacity(16);
    let prepared = prepare_window_with_memo(&packet_inputs, &mut cache);
    let stats = cache.stats().clone();

    assert_eq!(packet_inputs.len(), 4);
    assert_eq!(prepared.len(), 4);
    assert_eq!(
        semantic_prep_memo_key("shared semantic payload")
            .content_hash
            .as_bytes()
            .len(),
        32
    );
    assert_eq!(stats.hits, 1);
    assert_eq!(stats.misses, 3);
    assert_eq!(stats.inserts, 3);
    assert_eq!(stats.live_entries, 3);
    Ok(())
}

#[test]
fn backfill_batch_saves_checkpoint_and_staged_index_until_complete() {
    let temp = tempdir().unwrap();
    let mut manifest = SemanticManifest::default();
    let indexer = SemanticIndexer::new("hash", None).unwrap();
    let messages = vec![
        EmbeddingInput::new(10, "first staged semantic message"),
        EmbeddingInput::new(11, "second staged semantic message"),
    ];

    let outcome = indexer
        .run_backfill_batch(
            &messages,
            temp.path(),
            &mut manifest,
            SemanticBackfillBatchPlan {
                tier: TierKind::Fast,
                db_fingerprint: "db-fp-backfill-partial".to_string(),
                model_revision: "hash".to_string(),
                total_conversations: 2,
                conversations_in_batch: 1,
                last_offset: 1,
                cursor_exhausted: false,
            },
        )
        .unwrap();

    assert!(!outcome.published);
    assert!(outcome.checkpoint_saved);
    assert!(outcome.index_path.exists());
    assert!(!vector_index_path(temp.path(), indexer.embedder_id()).exists());
    let checkpoint = manifest.checkpoint.as_ref().expect("checkpoint");
    assert_eq!(checkpoint.tier, TierKind::Fast);
    assert_eq!(checkpoint.conversations_processed, 1);
    assert_eq!(checkpoint.docs_embedded, 2);
    assert_eq!(manifest.backlog.total_conversations, 2);
    assert!(SemanticManifest::path(temp.path()).exists());
}

#[test]
fn backfill_batch_does_not_publish_until_cursor_exhausted() -> Result<()> {
    let temp = tempdir()?;
    let mut manifest = SemanticManifest::default();
    let indexer = SemanticIndexer::new("hash", None)?;
    let db_fingerprint = "db-fp-backfill-cursor-not-exhausted";

    let first = vec![EmbeddingInput::new(30, "first cursor batch")];
    let first_outcome = indexer.run_backfill_batch(
        &first,
        temp.path(),
        &mut manifest,
        SemanticBackfillBatchPlan {
            tier: TierKind::Fast,
            db_fingerprint: db_fingerprint.to_string(),
            model_revision: "hash".to_string(),
            total_conversations: 2,
            conversations_in_batch: 1,
            last_offset: 1,
            cursor_exhausted: false,
        },
    )?;
    anyhow::ensure!(!first_outcome.published, "first batch must not publish");
    anyhow::ensure!(
        first_outcome.checkpoint_saved,
        "first batch should save a checkpoint"
    );

    let second = vec![EmbeddingInput::new(31, "second cursor batch")];
    let second_outcome = indexer.run_backfill_batch(
        &second,
        temp.path(),
        &mut manifest,
        SemanticBackfillBatchPlan {
            tier: TierKind::Fast,
            db_fingerprint: db_fingerprint.to_string(),
            model_revision: "hash".to_string(),
            total_conversations: 2,
            conversations_in_batch: 1,
            last_offset: 2,
            cursor_exhausted: false,
        },
    )?;

    anyhow::ensure!(
        !second_outcome.published,
        "count-based completion would publish here; cursor state must win"
    );
    anyhow::ensure!(
        second_outcome.checkpoint_saved,
        "second batch should save a checkpoint"
    );
    anyhow::ensure!(
        !vector_index_path(temp.path(), indexer.embedder_id()).exists(),
        "non-exhausted cursor must not publish the final vector index"
    );
    let Some(checkpoint) = manifest.checkpoint.as_ref() else {
        bail!("checkpoint should remain after a non-exhausted cursor");
    };
    anyhow::ensure!(
        checkpoint.conversations_processed == 2,
        "wanted 2 processed conversations, got {}",
        checkpoint.conversations_processed
    );
    anyhow::ensure!(
        checkpoint.total_conversations == 2,
        "checkpoint should preserve the real DB total, got {}",
        checkpoint.total_conversations
    );
    anyhow::ensure!(
        !checkpoint.cursor_exhausted,
        "saved checkpoint should preserve the non-exhausted cursor state"
    );
    anyhow::ensure!(
        !checkpoint.is_complete(),
        "non-exhausted cursor checkpoint must not be complete"
    );
    anyhow::ensure!(
        checkpoint.progress_pct() == 99,
        "non-exhausted cursor checkpoint should report 99% progress, got {}",
        checkpoint.progress_pct()
    );
    anyhow::ensure!(
        second_outcome.conversations_processed == 2,
        "wanted outcome to report 2 processed conversations, got {}",
        second_outcome.conversations_processed
    );
    anyhow::ensure!(
        second_outcome.total_conversations == 2,
        "wanted outcome to preserve total 2, got {}",
        second_outcome.total_conversations
    );
    let progress_pct = second_outcome.progress_pct();
    anyhow::ensure!(
        (progress_pct - 99.0).abs() < f64::EPSILON,
        "non-published outcome should cap progress below 100%, got {}",
        progress_pct
    );
    Ok(())
}

#[test]
fn backfill_batch_resumes_staged_index_and_publishes_manifest_atomically() {
    let temp = tempdir().unwrap();
    let mut manifest = SemanticManifest::default();
    let indexer = SemanticIndexer::new("hash", None).unwrap();
    let db_fingerprint = "db-fp-backfill-complete";
    let staging_path = semantic_staging_index_path(
        temp.path(),
        TierKind::Fast,
        indexer.embedder_id(),
        db_fingerprint,
    );

    let first = vec![EmbeddingInput::new(20, "first resume batch")];
    let first_outcome = indexer
        .run_backfill_batch(
            &first,
            temp.path(),
            &mut manifest,
            SemanticBackfillBatchPlan {
                tier: TierKind::Fast,
                db_fingerprint: db_fingerprint.to_string(),
                model_revision: "hash".to_string(),
                total_conversations: 2,
                conversations_in_batch: 1,
                last_offset: 1,
                cursor_exhausted: false,
            },
        )
        .unwrap();
    assert_eq!(first_outcome.index_path, staging_path);
    assert!(staging_path.exists());

    let second = vec![EmbeddingInput::new(21, "second resume batch")];
    let second_outcome = indexer
        .run_backfill_batch(
            &second,
            temp.path(),
            &mut manifest,
            SemanticBackfillBatchPlan {
                tier: TierKind::Fast,
                db_fingerprint: db_fingerprint.to_string(),
                model_revision: "hash".to_string(),
                total_conversations: 2,
                conversations_in_batch: 1,
                last_offset: 2,
                cursor_exhausted: true,
            },
        )
        .unwrap();

    assert!(second_outcome.published);
    assert!(!second_outcome.checkpoint_saved);
    assert!((second_outcome.progress_pct() - 100.0).abs() < f64::EPSILON);
    assert!(!staging_path.exists());
    let final_path = vector_index_path(temp.path(), indexer.embedder_id());
    assert_eq!(second_outcome.index_path, final_path);
    assert!(final_path.exists());
    assert!(manifest.checkpoint.is_none());
    let artifact = manifest.fast_tier.as_ref().expect("published fast tier");
    assert!(artifact.ready);
    assert_eq!(artifact.conversation_count, 2);
    assert_eq!(artifact.doc_count, 2);
    assert_eq!(manifest.backlog.fast_tier_processed, 2);

    let loaded = SemanticManifest::load(temp.path()).unwrap().unwrap();
    assert!(loaded.checkpoint.is_none());
    assert!(loaded.fast_tier.as_ref().is_some_and(|record| record.ready));
}

#[test]
fn backfill_publish_compacts_resumed_wal_before_rename() {
    let temp = tempdir().unwrap();
    let mut manifest = SemanticManifest::default();
    let indexer = SemanticIndexer::new("hash", None).unwrap();
    let db_fingerprint = "db-fp-backfill-small-resume";
    let first: Vec<EmbeddingInput> = (0..20)
        .map(|idx| EmbeddingInput::new(100 + idx, format!("first batch message {idx}")))
        .collect();

    let first_outcome = indexer
        .run_backfill_batch(
            &first,
            temp.path(),
            &mut manifest,
            SemanticBackfillBatchPlan {
                tier: TierKind::Fast,
                db_fingerprint: db_fingerprint.to_string(),
                model_revision: "hash".to_string(),
                total_conversations: 2,
                conversations_in_batch: 1,
                last_offset: 1,
                cursor_exhausted: false,
            },
        )
        .unwrap();
    assert!(first_outcome.checkpoint_saved);

    let second = vec![EmbeddingInput::new(200, "small final resume batch")];
    let second_outcome = indexer
        .run_backfill_batch(
            &second,
            temp.path(),
            &mut manifest,
            SemanticBackfillBatchPlan {
                tier: TierKind::Fast,
                db_fingerprint: db_fingerprint.to_string(),
                model_revision: "hash".to_string(),
                total_conversations: 2,
                conversations_in_batch: 1,
                last_offset: 2,
                cursor_exhausted: true,
            },
        )
        .unwrap();

    assert!(second_outcome.published);
    let final_path = vector_index_path(temp.path(), indexer.embedder_id());
    let mut final_wal_path = final_path.as_os_str().to_os_string();
    final_wal_path.push(".wal");
    assert!(!PathBuf::from(final_wal_path).exists());

    let published_index = FsVectorIndex::open(&final_path).unwrap();
    assert_eq!(published_index.record_count(), 21);
    let artifact = manifest.fast_tier.as_ref().expect("published fast tier");
    assert_eq!(artifact.doc_count, 21);
    assert_eq!(artifact.conversation_count, 2);
}

#[test]
fn backfill_from_storage_fetches_canonical_batches_and_resumes() -> Result<()> {
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
        &test_conversation("first", "first canonical semantic message"),
    )?;
    storage.insert_conversation_tree(
        agent_id,
        None,
        &test_conversation("second", "second canonical semantic message"),
    )?;

    let mut manifest = SemanticManifest::default();
    let indexer = SemanticIndexer::new("hash", None)?;

    let first = indexer.run_backfill_from_storage(
        &storage,
        temp.path(),
        &mut manifest,
        SemanticBackfillStoragePlan {
            tier: TierKind::Fast,
            db_fingerprint: "canonical-db-fp".to_string(),
            model_revision: "hash".to_string(),
            max_conversations: 1,
        },
    )?;
    assert!(!first.published);
    assert!(first.checkpoint_saved);
    assert_eq!(first.conversations_processed, 1);
    assert_eq!(first.total_conversations, 2);
    assert_eq!(first.embedded_docs, 1);
    assert!(first.last_offset > 0);

    let second = indexer.run_backfill_from_storage(
        &storage,
        temp.path(),
        &mut manifest,
        SemanticBackfillStoragePlan {
            tier: TierKind::Fast,
            db_fingerprint: "canonical-db-fp".to_string(),
            model_revision: "hash".to_string(),
            max_conversations: 1,
        },
    )?;
    assert!(second.published);
    assert!(!second.checkpoint_saved);
    assert_eq!(second.conversations_processed, 2);
    assert_eq!(second.embedded_docs, 1);
    assert!(manifest.checkpoint.is_none());
    assert_eq!(
        manifest.fast_tier.as_ref().map(|record| record.doc_count),
        Some(2)
    );
    Ok(())
}
