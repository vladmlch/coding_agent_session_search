use std::collections::HashSet;
use std::path::Path;

use anyhow::{Context, Result};

use crate::connectors::NormalizedConversation;
use crate::indexer::{
    LexicalPopulationStrategy, lexical_population_strategy_requires_inline_tantivy,
};
use crate::model::types::{Agent, AgentKind, Conversation, MessageRole};
use crate::search::tantivy::TantivyIndex;
use crate::storage::sqlite::{FrankenStorage, IndexingCache};

use super::batch_map::{
    PersistHeartbeat, map_batch_to_internal, prepare_codebuff_canonical_repair,
    prepare_cursor_workspace_repair,
};
use super::canonical_replay::{
    PersistBatchOutcome, changed_message_indices, lexical_packet_for_changed_messages,
    packet_semantic_delta_for_outcome, positional_indices_for_inserted,
    publish_changed_packet_messages, record_persisted_raw_mirror_db_links,
    replay_canonical_conversation_for_rebuild, should_defer_incremental_lexical_update_after_error,
    should_inject_incremental_lexical_update_oom, stage_persisted_region_docs,
};
use super::concurrent::{
    SERIAL_CHUNK_CONTENTION_RETRIES, begin_concurrent_writes_enabled,
    defer_lexical_updates_enabled, duplicate_conversation_keys_present,
    persist_conversations_batched_begin_concurrent, serial_batch_chunk_size, with_concurrent_retry,
    with_ephemeral_writer,
};

#[allow(clippy::too_many_arguments)]
pub(super) fn persist_conversations_batched_whole(
    storage: &FrankenStorage,
    mut t_index: Option<&mut TantivyIndex>,
    convs: &[NormalizedConversation],
    lexical_strategy: LexicalPopulationStrategy,
    defer_checkpoints: bool,
    capture_semantic_delta: bool,
    raw_mirror_data_dir: Option<&Path>,
    record_raw_mirror_links: bool,
    heartbeat: PersistHeartbeat<'_>,
    source_completion: Option<&crate::storage::sqlite::SourceIngestLedgerEntry>,
) -> Result<PersistBatchOutcome> {
    // Link recording re-verifies the mirror blob; a sliced conversation
    // records one link, with its full message count, after its last slice.
    let link_data_dir = raw_mirror_data_dir.filter(|_| record_raw_mirror_links);
    // GH459: revoke completed lexical authority durably BEFORE any writer
    // transaction can change workspace associations. A crash or failed
    // publication is then recovered by the existing full canonical rebuild.
    prepare_cursor_workspace_repair(storage, raw_mirror_data_dir, convs)?;
    if convs.iter().any(|conv| conv.agent_slug == "codebuff") {
        let canonical_index_path = raw_mirror_data_dir
            .map(crate::search::tantivy::index_dir)
            .transpose()?;
        prepare_codebuff_canonical_repair(
            storage,
            canonical_index_path
                .as_deref()
                .or_else(|| t_index.as_deref().map(TantivyIndex::path)),
            convs,
        )?;
    }
    if lexical_population_strategy_requires_inline_tantivy(lexical_strategy) && t_index.is_none() {
        anyhow::bail!(
            "batched persistence requires a Tantivy writer for {}",
            lexical_strategy.as_str()
        );
    }

    let begin_concurrent_enabled = source_completion.is_none() && begin_concurrent_writes_enabled();
    let duplicate_keys_present =
        begin_concurrent_enabled && duplicate_conversation_keys_present(convs);

    if begin_concurrent_enabled && !duplicate_keys_present {
        let db_path = storage
            .database_path()
            .with_context(|| "resolving database path for begin-concurrent write mode")?;
        tracing::info!(
            conversations = convs.len(),
            "using begin-concurrent write path for indexing"
        );
        return persist_conversations_batched_begin_concurrent(
            storage,
            &db_path,
            t_index,
            convs,
            lexical_strategy,
            defer_checkpoints,
            capture_semantic_delta,
            link_data_dir,
            heartbeat,
        );
    }

    if duplicate_keys_present {
        tracing::info!(
            conversations = convs.len(),
            "duplicate conversation keys detected; falling back to serial batched indexing path"
        );
    }

    // Hoist map_to_internal out of the writer transaction. It is a pure,
    // allocator-heavy transformation (string clones, optional secret
    // redaction, message + snippet remap) that does not depend on any
    // writer-assigned id, so there is no reason to hold the SQLite writer
    // lock while we burn CPU on it. Running it in parallel via rayon
    // shortens the serial writer-hold window and exploits headroom on
    // many-core hosts. This is the hot path for ingest batches.
    let internal_convs: Vec<Conversation> = map_batch_to_internal(convs, heartbeat);

    let outcomes = with_ephemeral_writer(
        storage,
        defer_checkpoints,
        "serial batched indexing",
        |writer| {
            let cache_enabled = IndexingCache::is_enabled();
            let mut cache = IndexingCache::new();

            // Prepare data for batched insert without consuming the canonical
            // conversations: the same redacted payload must feed both SQLite
            // and the derived lexical sink after the transaction commits.
            let mut prepared: Vec<(i64, Option<i64>, &Conversation)> =
                Vec::with_capacity(convs.len());

            for (conv, internal_conv) in convs.iter().zip(internal_convs.iter()) {
                let agent = Agent {
                    id: None,
                    slug: conv.agent_slug.clone(),
                    name: conv.agent_slug.clone(),
                    version: None,
                    kind: AgentKind::Cli,
                };

                // These idempotent parent upserts precede the canonical
                // chunk transaction. They need their own bounded contention
                // retry when a warm writer encounters a new identity (GH473).
                let agent_id = with_concurrent_retry(SERIAL_CHUNK_CONTENTION_RETRIES, || {
                    if cache_enabled {
                        cache.get_or_insert_agent(writer, &agent)
                    } else {
                        writer.ensure_agent(&agent)
                    }
                })?;

                let workspace_id = if let Some(ws) = &conv.workspace {
                    Some(with_concurrent_retry(
                        SERIAL_CHUNK_CONTENTION_RETRIES,
                        || {
                            if cache_enabled {
                                cache.get_or_insert_workspace(writer, ws, None)
                            } else {
                                writer.ensure_workspace(ws, None)
                            }
                        },
                    )?)
                } else {
                    None
                };

                prepared.push((agent_id, workspace_id, internal_conv));
            }

            if cache_enabled {
                let (hits, misses, hit_rate) = cache.stats();
                tracing::debug!(
                    hits,
                    misses,
                    hit_rate = format!("{:.1}%", hit_rate * 100.0),
                    agents = cache.agent_count(),
                    workspaces = cache.workspace_count(),
                    "IndexingCache stats"
                );
            }

            let chunk_size = serial_batch_chunk_size().min(prepared.len().max(1));
            let mut outcomes = Vec::with_capacity(prepared.len());

            for start in (0..prepared.len()).step_by(chunk_size) {
                let end = (start + chunk_size).min(prepared.len());
                let chunk_refs = &prepared[start..end];
                // GH #401/#406: a single writer can still see a transient
                // `BusySnapshot`/`WriteConflict` from the engine (the
                // fsqlite 0.3.1 freelist append-gate false positive, or a
                // peer reader's epoch unwinding). The batch transaction is
                // the rollback boundary and the writer merges by
                // conversation key, so re-running the whole chunk is
                // idempotent; bound it instead of surfacing one transient
                // conflict as a fatal exit 7 that discards the run.
                outcomes.extend(with_concurrent_retry(
                    SERIAL_CHUNK_CONTENTION_RETRIES,
                    || match source_completion.filter(|_| end == prepared.len()) {
                        Some(completion) => writer
                            .insert_conversations_batched_with_source_completion(
                                chunk_refs, completion,
                            ),
                        None => writer.insert_conversations_batched(chunk_refs),
                    },
                )?);
                // gh373/oeu5a: each written chunk is live work even while
                // the batch-level `current` bump waits for the whole
                // persist call to return.
                heartbeat.tick();
            }

            // One coherent extension generation per conversation rides
            // the same ingest pass — one transaction whose commit
            // makes rows and receipt durable together, so a failure
            // names the reason instead of silently skipping the lane.
            // Idempotent on the receipt's
            // (source_revision, extractor_version, projection_version).
            crate::viewer_extensions::projection::publish_ingest_generations_batch_best_effort(
                writer.raw(),
                prepared
                    .iter()
                    .zip(&outcomes)
                    .map(|((_, _, conv), outcome)| (*conv, outcome.conversation_id)),
            );
            // Task 11: post-commit change-journal notify — canonical
            // events were already journaled by the observer triggers
            // inside the ingest commit; this coalesces them and stamps
            // canonical_ready (SQLite journal is truth, IPC is wakeup-only).
            crate::viewer_extensions::changes::notify_committed_change_best_effort(writer.raw());
            // Task 12: wake any bridge event listener (wakeup-only).
            if let Ok(db_path) = writer.database_path() {
                crate::viewer_extensions::changes::send_bridge_wakeup_best_effort(&db_path);
            }

            Ok(outcomes)
        },
    )?;
    let defer_lexical_updates = defer_lexical_updates_enabled();
    let mut batch_outcome = PersistBatchOutcome::default();
    record_persisted_raw_mirror_db_links(link_data_dir, convs, &outcomes);
    if !defer_lexical_updates {
        let rebuild_inline = !outcomes.is_empty()
            && lexical_strategy == LexicalPopulationStrategy::InlineRebuildFromScan;
        if rebuild_inline {
            // A prior direct or batched incremental call may still have
            // staged identities. Establish the published batch boundary
            // before Quill resolves canonical replacements. The same
            // commit seals any region documents that call staged —
            // flush its queued marks stamps `published` with it.
            t_index
                .as_deref_mut()
                .expect("inline rebuild requires Tantivy writer")
                .commit_with_region_doc_stamps(storage.raw())?;
        }
        // Task 5 region-doc prelude: stage every pending region
        // document — the convergence sweep plus this batch's sessions —
        // BEFORE any canonical staging. Quill's `upsert` publishes
        // pending work when a re-staged identity already lives in the
        // committed snapshot (a republish), so a region upsert issued
        // after canonical staging would flush the batch
        // mid-generation: a failed outer commit would leave canonical
        // docs searchable without their publication receipt.
        // Region-only early commits stay honest — marks remain
        // `pending` and session reads gate on them until the
        // post-commit stamp.
        //
        // Whole-archive republish deferral (see the begin-concurrent
        // twin above): a latched writer skips the sweep AND the hooks —
        // the lane drains the accumulated pending set once, bounded,
        // right before its final commit, instead of publishing one
        // successor segment per persist batch.
        if let Some(index) = t_index.as_deref_mut() {
            let defer_region_docs = lexical_strategy
                == LexicalPopulationStrategy::IncrementalInline
                && index.region_doc_convergence_deferred_to_final_drain();
            if !defer_region_docs {
                index.converge_pending_search_region_docs(storage.raw(), 4096)?;
                for outcome in &outcomes {
                    stage_persisted_region_docs(
                        index,
                        storage.raw(),
                        outcome.conversation_id,
                        lexical_strategy,
                    )?;
                }
            }
        }
        // ibuuh.32 / 5b9p0: route the serial-batched lexical sink
        // through the packet pipeline. Build each packet ONCE and
        // reuse it for both InlineRebuildFromScan (full message
        // set) and IncrementalInline (positional subset derived
        // from outcome.inserted_indices).
        let mut rebuilt_conversation_ids = HashSet::new();
        let mut skip_inline_lexical_updates = false;
        for outcome in &outcomes {
            batch_outcome.record_insert_outcome(outcome);
            // gh373/oeu5a: per-conversation liveness through the inline
            // lexical sink (packet build + Tantivy add can be slow for a
            // giant conversation).
            heartbeat.tick();
            if skip_inline_lexical_updates {
                // The prelude may already have staged this session's
                // region docs; rescind the queued stamp so the
                // persisted mark stays `pending` with the canonical
                // debt instead of publishing region docs without
                // their messages.
                if let Some(index) = t_index.as_deref_mut() {
                    index.unstage_search_region_docs(outcome.conversation_id);
                }
                continue;
            }
            match lexical_strategy {
                LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild => continue,
                LexicalPopulationStrategy::InlineRebuildFromScan => {
                    // Coalesce only lexical replay; every persistence
                    // outcome still contributes accounting and semantics.
                    if rebuilt_conversation_ids.insert(outcome.conversation_id) {
                        replay_canonical_conversation_for_rebuild(
                            storage,
                            t_index
                                .as_deref_mut()
                                .expect("inline rebuild requires Tantivy writer"),
                            outcome.conversation_id,
                        )?;
                    }
                }
                LexicalPopulationStrategy::IncrementalInline => {
                    let changed_indices = changed_message_indices(outcome);
                    if !changed_indices.is_empty() {
                        let packet = lexical_packet_for_changed_messages(
                            storage,
                            outcome.conversation_id,
                            &changed_indices,
                        )?;
                        let positional = positional_indices_for_inserted(&packet, &changed_indices);
                        if !positional.is_empty() {
                            let add_result = if should_inject_incremental_lexical_update_oom() {
                                Err(anyhow::anyhow!("out of memory"))
                            } else {
                                publish_changed_packet_messages(
                                    t_index.as_deref_mut().expect(
                                        "incremental inline updates require Tantivy writer",
                                    ),
                                    &packet,
                                    &positional,
                                    outcome.conversation_id,
                                    !outcome.updated_indices.is_empty(),
                                )
                            };
                            if let Err(error) = add_result {
                                if should_defer_incremental_lexical_update_after_error(&error) {
                                    batch_outcome.record_deferred_lexical_update(&error);
                                    skip_inline_lexical_updates = true;
                                    // Rescind this session's queued
                                    // region-doc stamp — its mark stays
                                    // `pending` with the canonical debt.
                                    if let Some(index) = t_index.as_deref_mut() {
                                        index.unstage_search_region_docs(outcome.conversation_id);
                                    }
                                    tracing::warn!(
                                        error = %error,
                                        "incremental lexical update ran out of memory; preserving SQLite ingest and deferring lexical repair"
                                    );
                                } else {
                                    return Err(error);
                                }
                            }
                        }
                    }
                }
            }
        }
        // The pending-mark convergence sweep runs in the
        // pre-canonical prelude above — same contract as the other
        // batch lane. Sessions skipped after the prelude (mid-batch
        // OOM) had their stamps rescinded in the loop and keep
        // `pending` marks with the canonical debt.
        if rebuild_inline {
            // Same two-phase rule: the commit seals the staged
            // documents, then the queued marks stamps land
            // `published` — never before.
            t_index
                .expect("inline rebuild requires Tantivy writer")
                .commit_with_region_doc_stamps(storage.raw())?;
        }
    } else {
        for outcome in &outcomes {
            batch_outcome.record_insert_outcome(outcome);
        }
        // Deferred-lexical lane: region docs wait for the authoritative
        // rebuild, which reconciles every pending marks row.
    }

    if capture_semantic_delta {
        for outcome in outcomes.iter() {
            let (inputs, max_message_id) = packet_semantic_delta_for_outcome(storage, outcome)?;
            batch_outcome.extend_semantic_delta(inputs, max_message_id);
        }
    }

    Ok(batch_outcome)
}

pub(super) fn map_role(role: &str) -> MessageRole {
    match role {
        "user" => MessageRole::User,
        "assistant" | "agent" => MessageRole::Agent,
        "tool" => MessageRole::Tool,
        "system" => MessageRole::System,
        other => MessageRole::Other(other.to_string()),
    }
}
