//! Cancellation must not turn a read-only proof into new serving authority.
//!
//! The provider is the local deterministic hash embedder: cancellation is a
//! general backfill concern, never an HTTP boundary.
use super::*;
use crate::indexer::semantic::engine::CancelCheck;
use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn cancelled_read_only_proof_preserves_publication_and_cannot_refresh_receipt() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let data = temp.path();
    let storage = FrankenStorage::open(&data.join("agent_search.db"))?;
    let agent = storage.ensure_agent(&Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    for ordinal in 0..4 {
        storage.insert_conversation_tree(
            agent,
            None,
            &Conversation {
                id: None,
                agent_slug: "codex".into(),
                workspace: None,
                external_id: Some(format!("proof-{ordinal}")),
                title: None,
                source_path: format!("/fixture/proof-{ordinal}.jsonl").into(),
                started_at: Some(1),
                ended_at: Some(2),
                approx_tokens: None,
                metadata_json: json!({}),
                source_id: "local".into(),
                origin_host: None,
                messages: vec![Message {
                    id: None,
                    idx: 0,
                    role: MessageRole::User,
                    author: None,
                    created_at: Some(1),
                    content: format!("private proof corpus {ordinal}"),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                }],
            },
        )?;
    }
    let polls = Arc::new(AtomicUsize::new(0));
    let cancel_at = Arc::new(AtomicUsize::new(usize::MAX));
    let check: CancelCheck = {
        let polls = Arc::clone(&polls);
        let cancel_at = Arc::clone(&cancel_at);
        Arc::new(move || {
            polls.fetch_add(1, Ordering::SeqCst) + 1 >= cancel_at.load(Ordering::SeqCst)
        })
    };
    let engine = engine::SemanticIndexer::new_with_cancel("hash", None, check)?;
    let plan = SemanticBackfillStoragePlan {
        tier: TierKind::Quality,
        db_fingerprint: crate::indexer::lexical_storage_fingerprint_for_storage(&storage)?,
        model_revision: engine.embedder_id().into(),
        max_conversations: 16,
    };
    let mut manifest = SemanticManifest::default();
    let built = engine.run_backfill_from_storage(&storage, data, &mut manifest, plan.clone())?;
    assert!(built.published);
    let receipt = data
        .join(crate::search::vector_index::VECTOR_INDEX_DIR)
        .join(format!(
            ".completed-backfill-quality-{}.json",
            engine.embedder_id(),
        ));
    // Corrupt only the disposable skip hint: the complete generation stays valid.
    fs::write(&receipt, b"incomplete skip receipt")?;
    let vectors_before = fs::read(&built.index_path)?;
    let manifest_before = fs::read(SemanticManifest::path(data))?;
    let sink = SemanticProgressSink::disabled();
    // Cancel in the vector loop, in canonical replay, and at the final proof
    // checkpoint. A no-op must not hide cancellation or refresh its skip hint.
    for (threshold, final_checkpoint) in [(3, false), (7, false), (usize::MAX, true)] {
        polls.store(0, Ordering::SeqCst);
        cancel_at.store(threshold, Ordering::SeqCst);
        let error = prove_unchanged(&engine, &storage, data, &manifest, &plan, &sink, || {
            if final_checkpoint {
                cancel_at.store(0, Ordering::SeqCst);
            }
            Ok(())
        })
        .expect_err("a cancelled proof must not announce completion");
        assert!(
            format!("{error:#}").contains("semantic_backfill_cancelled"),
            "{error:#}"
        );
        assert_eq!(fs::read(&built.index_path)?, vectors_before);
        assert_eq!(fs::read(SemanticManifest::path(data))?, manifest_before);
        assert_eq!(fs::read(&receipt)?, b"incomplete skip receipt");
    }
    cancel_at.store(usize::MAX, Ordering::SeqCst);
    let outcome = prove_unchanged(&engine, &storage, data, &manifest, &plan, &sink, || Ok(()))?
        .context("uncancelled proof must retain the completed generation")?;
    assert!(outcome.unchanged && outcome.published);
    assert_eq!(outcome.embedded_docs, 0);
    assert_eq!(fs::read(&built.index_path)?, vectors_before);
    assert_eq!(fs::read(SemanticManifest::path(data))?, manifest_before);
    assert!(
        engine
            .completed_backfill_fingerprint(
                &storage,
                data,
                &manifest,
                plan.tier,
                &plan.model_revision,
            )?
            .is_some()
    );
    Ok(())
}
