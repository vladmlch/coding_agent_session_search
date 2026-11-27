use std::collections::HashSet;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use rand::RngExt;

use crate::connectors::NormalizedConversation;
use crate::franken_sync::FrankenError;
use crate::franken_sync::compat::RowExt;
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
                let agent_id = with_writer_retry(SERIAL_CHUNK_CONTENTION_RETRIES, || {
                    if cache_enabled {
                        cache.get_or_insert_agent(writer, &agent)
                    } else {
                        writer.ensure_agent(&agent)
                    }
                })?;

                let workspace_id = if let Some(ws) = &conv.workspace {
                    Some(with_writer_retry(SERIAL_CHUNK_CONTENTION_RETRIES, || {
                        if cache_enabled {
                            cache.get_or_insert_workspace(writer, ws, None)
                        } else {
                            writer.ensure_workspace(ws, None)
                        }
                    })?)
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

            for (chunk_idx, start) in (0..prepared.len()).step_by(chunk_size).enumerate() {
                let end = (start + chunk_size).min(prepared.len());
                let chunk_refs = &prepared[start..end];
                let shadow_guard = crate::indexer::parallel_wal_shadow::start_chunk(
                    chunk_idx,
                    start,
                    chunk_refs.len(),
                );
                // GH #401/#406: a writer can still see transient Busy/BusySnapshot
                // contention with a concurrent reader or external process. The
                // batch transaction is the rollback boundary and the writer merges
                // by conversation key, so re-running the whole chunk is
                // idempotent; bound it instead of surfacing one transient
                // conflict as a fatal exit 7 that discards the run.
                let chunk_result =
                    with_writer_retry(SERIAL_CHUNK_CONTENTION_RETRIES, || match source_completion
                        .filter(|_| end == prepared.len())
                    {
                        Some(completion) => writer
                            .insert_conversations_batched_with_source_completion(
                                chunk_refs, completion,
                            ),
                        None => writer.insert_conversations_batched(chunk_refs),
                    });
                match chunk_result {
                    Ok(chunk_outcomes) => {
                        outcomes.extend(chunk_outcomes);
                        if let Some(guard) = shadow_guard {
                            guard.finish_ok();
                        }
                    }
                    Err(err) => {
                        if let Some(guard) = shadow_guard {
                            guard.finish_err();
                        }
                        return Err(err);
                    }
                }
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
        // Whole-archive republish deferral: a latched writer skips the
        // sweep AND the hooks — the lane drains the accumulated pending
        // set once, bounded, right before its final commit, instead of
        // publishing one successor segment per persist batch.
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

#[allow(unused_imports)]
pub(super) use crate::source_onboarding::conv_map::map_role;

/// Cap on writer contention retries (`writer_retry_limit`).
/// Worst-case wall time = max_retries * (256 ms backoff + 256 ms jitter) ≈ ~16 s at 32 retries.
pub(super) const WRITER_RETRY_MAX: usize = 32;

fn env_usize_bounded_opt(var: &str, max: usize) -> Option<usize> {
    match dotenvy::var(var).ok().and_then(|v| v.parse::<usize>().ok()) {
        Some(0) | None => None,
        Some(v) if v > max => {
            tracing::warn!(
                env_var = var,
                requested = v,
                cap = max,
                "env var exceeds safe cap; clamping"
            );
            Some(max)
        }
        Some(v) => Some(v),
    }
}

pub(in crate::indexer) fn writer_retry_limit() -> usize {
    env_usize_bounded_opt("CASS_INDEXER_WRITER_RETRIES", WRITER_RETRY_MAX)
        .or_else(|| {
            env_usize_bounded_opt("CASS_INDEXER_BEGIN_CONCURRENT_RETRIES", WRITER_RETRY_MAX)
        })
        .unwrap_or(6)
}

pub(super) fn serial_batch_chunk_size() -> usize {
    dotenvy::var("CASS_INDEXER_SERIAL_CHUNK_SIZE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(128)
}

fn index_writer_busy_timeout_ms() -> u64 {
    // GH473: keep engine-local waits short; complete idempotent write
    // transactions already have bounded retries at the CASS boundary.
    dotenvy::var("CASS_INDEX_WRITER_BUSY_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(10)
}

/// WAL autocheckpoint threshold (pages) for the index writer.
///
/// GH#320: bulk-import mode used to disable autocheckpoints entirely
/// (`0`), deferring the whole checkpoint to post-publish. That let the
/// WAL grow with the corpus (1 GB+ on large fleets) and drove high
/// ingest-phase pager memory footprints. A bounded-but-large threshold keeps
/// bulk import's amortized-checkpoint intent (one checkpoint per
/// ~256 MB of WAL at the default 4 KiB page size) while capping the
/// WAL-proportional memory. Operators can restore the old unbounded
/// behavior with `CASS_INDEX_WRITER_WAL_AUTOCHECKPOINT_PAGES=0`.
pub(in crate::indexer) const BULK_IMPORT_WAL_AUTOCHECKPOINT_PAGES: i64 = 65_536;

pub(super) fn index_writer_wal_autocheckpoint_pages(defer_checkpoints: bool) -> i64 {
    dotenvy::var("CASS_INDEX_WRITER_WAL_AUTOCHECKPOINT_PAGES")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .filter(|v| *v >= 0)
        .unwrap_or(if defer_checkpoints {
            BULK_IMPORT_WAL_AUTOCHECKPOINT_PAGES
        } else {
            1000
        })
}

pub(in crate::indexer) fn defer_lexical_updates_enabled() -> bool {
    dotenvy::var("CASS_DEFER_LEXICAL_UPDATES")
        .map(|v| !(v == "0" || v.eq_ignore_ascii_case("false")))
        .unwrap_or(false)
}

pub(in crate::indexer) fn apply_index_writer_busy_timeout(storage: &FrankenStorage) {
    let busy_timeout_ms = index_writer_busy_timeout_ms();
    if storage.index_writer_busy_timeout_ms() == Some(busy_timeout_ms) {
        return;
    }
    let pragma = format!("PRAGMA busy_timeout = {busy_timeout_ms};");
    if let Err(err) = storage.raw().execute(&pragma) {
        tracing::debug!(
            busy_timeout_ms,
            error = %err,
            "failed_to_apply_index_writer_busy_timeout"
        );
    } else {
        storage.mark_index_writer_busy_timeout_ms(busy_timeout_ms);
    }
}

/// GH#320/#425: WAL-size threshold (bytes) above which bulk ingest resets
/// the WAL between batches. `0` disables the bound and restores the
/// fully-deferred pre-#320 behavior.
///
/// Rationale: bulk-import mode defers checkpoints so the hot insert loop
/// never stalls on backfill I/O, but a corpus-sized WAL (1-2 GB+ on large
/// fleets) drags a proportional slice of pager state into memory and makes
/// the single post-publish TRUNCATE checkpoint — the #319/#321 wedge
/// window — enormous. A between-batches single-connection TRUNCATE now
/// resets the WAL instead of repeatedly backfilling it without shrinking.
const BULK_INGEST_WAL_CHECKPOINT_DEFAULT_BYTES: u64 = 512 * 1024 * 1024;

fn bulk_ingest_wal_checkpoint_threshold_bytes() -> u64 {
    dotenvy::var("CASS_INDEX_INGEST_WAL_CHECKPOINT_BYTES")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(BULK_INGEST_WAL_CHECKPOINT_DEFAULT_BYTES)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BulkIngestWalCheckpointMode {
    Passive,
    Full,
    Restart,
    Truncate,
}

impl BulkIngestWalCheckpointMode {
    pub(super) fn from_env() -> Self {
        let Ok(raw) = dotenvy::var("CASS_INDEX_INGEST_WAL_CHECKPOINT_MODE") else {
            return Self::Truncate;
        };
        match raw.trim().to_ascii_uppercase().as_str() {
            "PASSIVE" => Self::Passive,
            "FULL" => Self::Full,
            "RESTART" => Self::Restart,
            "TRUNCATE" => Self::Truncate,
            invalid => {
                tracing::warn!(
                    value = invalid,
                    "invalid CASS_INDEX_INGEST_WAL_CHECKPOINT_MODE; using TRUNCATE"
                );
                Self::Truncate
            }
        }
    }

    const fn sql(self) -> &'static str {
        match self {
            Self::Passive => "PRAGMA wal_checkpoint(PASSIVE);",
            Self::Full => "PRAGMA wal_checkpoint(FULL);",
            Self::Restart => "PRAGMA wal_checkpoint(RESTART);",
            Self::Truncate => "PRAGMA wal_checkpoint(TRUNCATE);",
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Passive => "PASSIVE",
            Self::Full => "FULL",
            Self::Restart => "RESTART",
            Self::Truncate => "TRUNCATE",
        }
    }
}

/// Reset an oversized bulk-ingest WAL from a true single-connection state.
///
/// #425 showed that PASSIVE backfilled the complete WAL at every batch but
/// never reset its write position while the cached ephemeral writer remained
/// open alongside the primary connection. Before checkpointing, close that
/// idle writer and execute one primary-connection read to sync its view past
/// the writer's last commit, then switch the storage permanently to the
/// primary writer and issue TRUNCATE (default).
///
/// Only active in deferred-checkpoint (non-watch) mode. Failures remain
/// nonfatal, but a non-shrinking externally pinned WAL is not retried until
/// it grows by another threshold, avoiding an O(WAL) backfill every batch.
pub(in crate::indexer) fn maybe_checkpoint_bulk_ingest_wal(
    storage: &FrankenStorage,
    defer_checkpoints: bool,
) {
    if !defer_checkpoints {
        return;
    }
    let threshold = bulk_ingest_wal_checkpoint_threshold_bytes();
    if threshold == 0 {
        return;
    }
    let Ok(db_path) = storage.database_path() else {
        return;
    };
    let mut wal_path = db_path.clone().into_os_string();
    wal_path.push("-wal");
    let wal_bytes = match std::fs::metadata(std::path::Path::new(&wal_path)) {
        Ok(metadata) => metadata.len(),
        Err(_) => return,
    };
    let retry_at_bytes = storage.bulk_checkpoint_retry_at_bytes();
    if wal_bytes < threshold || (retry_at_bytes > 0 && wal_bytes < retry_at_bytes) {
        return;
    }

    let cached_writer_closed = match storage.close_idle_cached_ephemeral_writer() {
        Ok(closed) => closed,
        Err(err) => {
            tracing::warn!(
                db_path = %db_path.display(),
                wal_bytes,
                error = %err,
                "bulk ingest WAL reset skipped because the cached writer could not be closed"
            );
            storage.set_bulk_checkpoint_retry_at_bytes(wal_bytes.saturating_add(threshold));
            return;
        }
    };
    if storage.cached_ephemeral_writer_in_use() {
        tracing::warn!(
            db_path = %db_path.display(),
            wal_bytes,
            "bulk ingest WAL reset reached a non-quiescent writer boundary; deferring instead of checkpointing with two live connections"
        );
        storage.set_bulk_checkpoint_retry_at_bytes(wal_bytes.saturating_add(threshold));
        return;
    }
    // #384 / #425: after the cached writer committed, execute a primary read
    // so the primary handle refreshes its WAL view before TRUNCATE.
    if let Err(err) = storage
        .raw()
        .query("SELECT value FROM meta WHERE key = 'schema_version';")
    {
        tracing::warn!(
            db_path = %db_path.display(),
            wal_bytes,
            cached_writer_closed,
            error = %err,
            "bulk ingest WAL reset skipped because the primary WAL snapshot could not be refreshed"
        );
        storage.set_bulk_checkpoint_retry_at_bytes(wal_bytes.saturating_add(threshold));
        return;
    }

    // This transition is intentionally one-way for the lifetime of the
    // storage handle.
    storage.enable_bulk_single_connection();
    let mode = BulkIngestWalCheckpointMode::from_env();
    let started = std::time::Instant::now();
    match storage.raw().query(mode.sql()) {
        Ok(rows) => {
            let status = rows.first().map(|row| {
                (
                    row.get_typed::<i64>(0).unwrap_or(-1),
                    row.get_typed::<i64>(1).unwrap_or(-1),
                    row.get_typed::<i64>(2).unwrap_or(-1),
                )
            });
            let (busy, log_frames, checkpointed_frames) = status.unwrap_or((-1, -1, -1));
            let wal_bytes_after = match std::fs::metadata(std::path::Path::new(&wal_path)) {
                Ok(metadata) => metadata.len(),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => 0,
                Err(err) => {
                    tracing::warn!(
                        db_path = %db_path.display(),
                        error = %err,
                        "could not measure the bulk-ingest WAL after checkpoint; conservatively treating it as unshrunk"
                    );
                    wal_bytes
                }
            };
            let shrank = wal_bytes_after < wal_bytes;
            storage.set_bulk_checkpoint_retry_at_bytes(if shrank {
                0
            } else {
                wal_bytes.saturating_add(threshold)
            });
            if shrank {
                tracing::info!(
                    db_path = %db_path.display(),
                    wal_bytes_before = wal_bytes,
                    wal_bytes_after,
                    threshold_bytes = threshold,
                    checkpoint_mode = mode.label(),
                    cached_writer_closed,
                    busy,
                    log_frames,
                    checkpointed_frames,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "bulk ingest WAL checkpoint reset the WAL (#320/#425 memory bound)"
                );
            } else {
                tracing::warn!(
                    db_path = %db_path.display(),
                    wal_bytes_before = wal_bytes,
                    wal_bytes_after,
                    threshold_bytes = threshold,
                    checkpoint_mode = mode.label(),
                    cached_writer_closed,
                    busy,
                    log_frames,
                    checkpointed_frames,
                    retry_at_bytes = storage.bulk_checkpoint_retry_at_bytes(),
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "bulk ingest WAL checkpoint did not shrink the WAL; an external reader may be pinning it"
                );
            }
        }
        Err(err) => {
            storage.set_bulk_checkpoint_retry_at_bytes(wal_bytes.saturating_add(threshold));
            tracing::warn!(
                db_path = %db_path.display(),
                wal_bytes,
                checkpoint_mode = mode.label(),
                cached_writer_closed,
                retry_at_bytes = storage.bulk_checkpoint_retry_at_bytes(),
                error = %err,
                "bulk ingest WAL checkpoint failed; retry deferred until the WAL grows by another threshold"
            );
        }
    }
}

pub(in crate::indexer) fn apply_index_writer_checkpoint_policy(
    storage: &FrankenStorage,
    defer_checkpoints: bool,
) {
    let wal_autocheckpoint_pages = index_writer_wal_autocheckpoint_pages(defer_checkpoints);
    if storage.index_writer_checkpoint_pages() == Some(wal_autocheckpoint_pages) {
        return;
    }
    let pragma = format!("PRAGMA wal_autocheckpoint = {wal_autocheckpoint_pages};");
    if let Err(err) = storage.raw().execute(&pragma) {
        tracing::debug!(
            wal_autocheckpoint_pages,
            error = %err,
            "failed_to_apply_index_writer_checkpoint_policy"
        );
    } else {
        storage.mark_index_writer_checkpoint_pages(wal_autocheckpoint_pages);
    }
}

pub(in crate::indexer) fn with_ephemeral_writer<T, F>(
    storage: &FrankenStorage,
    defer_checkpoints: bool,
    context: &str,
    f: F,
) -> Result<T>
where
    F: FnOnce(&FrankenStorage) -> Result<T>,
{
    let db_path = storage
        .database_path()
        .with_context(|| format!("resolving database path for {context}"))?;
    // Keep the long-lived handle's connection-local checkpoint state aligned
    // with the short-lived writer so watch-mode observability and follow-up
    // policy transitions still reflect the active ingestion mode.
    apply_index_writer_checkpoint_policy(storage, defer_checkpoints);

    if storage.bulk_single_connection_enabled() {
        // Preflight is a write too; tune it before entering the retryable operation.
        apply_index_writer_busy_timeout(storage);
        if !storage.ephemeral_writer_preflight_verified() {
            with_writer_retry(SERIAL_CHUNK_CONTENTION_RETRIES, || {
                storage
                    .raw()
                    .execute("UPDATE meta SET value = value WHERE key = 'schema_version'")
                    .with_context(|| {
                        format!(
                            "primary writer preflight failed for {context} after the bulk WAL reset at {}",
                            db_path.display()
                        )
                    })
            })?;
            storage.mark_ephemeral_writer_preflight_verified();
        }
        apply_index_writer_checkpoint_policy(storage, defer_checkpoints);
        return f(storage);
    }

    // Writer acquisition and the idempotent preflight happen before any
    // canonical transaction. Short engine waits must not turn a transient
    // cold-open/preflight conflict into a fatal ingest failure (GH473).
    // Retry only setup: replaying f or close here could lose the outcomes
    // of already-committed chunks and therefore skip lexical publication.
    let (writer, reusable) = with_writer_retry(SERIAL_CHUNK_CONTENTION_RETRIES, || {
        let (writer, reusable) = storage.acquire_cached_ephemeral_writer().with_context(|| {
            format!(
                "opening short-lived SQLite writer for {context}: {}",
                db_path.display()
            )
        })?;

        let discard_writer = |mut writer: FrankenStorage| {
            if reusable {
                storage.discard_cached_ephemeral_writer(writer);
            } else {
                writer.close_best_effort_in_place();
            }
        };

        // A cold/reused writer must not inherit a long timeout for its first write.
        apply_index_writer_busy_timeout(&writer);
        if !storage.ephemeral_writer_preflight_verified() {
            // CASS #162 item 2: Preflight write check to catch "attempt to write
            // a readonly database" early with a clear diagnostic instead of letting
            // it surface deep inside a batched insert. Once one writer open has
            // succeeded on this storage handle, repeating the same idempotent
            // meta-table write on every steady-state persist just adds fixed
            // overhead without improving correctness.
            if let Err(err) = writer
                .raw()
                .execute("UPDATE meta SET value = value WHERE key = 'schema_version'")
            {
                discard_writer(writer);
                // Preserve the typed engine error for outer retry/fallback classification.
                return Err(anyhow::Error::new(err).context(format!(
                    "ephemeral writer preflight write failed for {context} at {}. \
                     The database may be locked by another process or opened in \
                     readonly mode. Try closing other cass instances and retrying.",
                    db_path.display()
                )));
            }
            storage.mark_ephemeral_writer_preflight_verified();
        }

        Ok((writer, reusable))
    })?;

    let release_writer = |writer: FrankenStorage| -> Result<()> {
        if reusable {
            storage.release_cached_ephemeral_writer(writer);
            Ok(())
        } else {
            writer.close().with_context(|| {
                format!(
                    "closing short-lived SQLite writer for {context}: {}",
                    db_path.display()
                )
            })
        }
    };

    apply_index_writer_checkpoint_policy(&writer, defer_checkpoints);

    let result = f(&writer);

    match result {
        Ok(value) => {
            release_writer(writer)?;
            Ok(value)
        }
        Err(err) => {
            if let Err(close_err) = release_writer(writer) {
                tracing::warn!(
                    error = %close_err,
                    db_path = %db_path.display(),
                    context,
                    "failed to close short-lived writer cleanly after write error"
                );
            }
            Err(err)
        }
    }
}

fn transient_franken_error(err: &anyhow::Error) -> Option<&FrankenError> {
    err.downcast_ref::<FrankenError>()
        .or_else(|| err.root_cause().downcast_ref::<FrankenError>())
}

pub(super) fn is_retryable_franken_error(err: &anyhow::Error) -> bool {
    transient_franken_error(err).is_some_and(|inner| {
        // DatabaseCorrupt is intentionally NOT retried here — retrying on
        // corruption can amplify damage by hammering corrupt pages and
        // spreading partial writes through the WAL.  Let it fail fast so
        // the caller can trigger the backup/quarantine path instead.
        matches!(
            inner,
            FrankenError::Busy
                | FrankenError::BusyRecovery
                | FrankenError::BusySnapshot { .. }
                | FrankenError::WriteConflict { .. }
                | FrankenError::SerializationFailure { .. }
        )
    })
}

/// Bounded retries for one serial persist chunk on retryable engine
/// contention (GH #401/#406). With the 4ms→256ms doubling jittered
/// backoff in [`with_writer_retry`] this is roughly 1.5s of total
/// waiting before the conflict is surfaced as exit 7 — enough to ride
/// out a busy writer window, short enough that a real lock holder still
/// fails fast.
pub(in crate::indexer) const SERIAL_CHUNK_CONTENTION_RETRIES: usize = 8;

/// Retry wrapper for any retryable FrankenError (Busy, BusySnapshot, etc.)
pub(in crate::indexer) fn with_writer_retry<F, T>(max_retries: usize, mut f: F) -> Result<T>
where
    F: FnMut() -> Result<T>,
{
    let mut rng = rand::rng();
    let mut backoff_ms = 4_u64;
    for attempt in 0..=max_retries {
        match f() {
            Ok(val) => return Ok(val),
            Err(err) if attempt < max_retries && is_retryable_franken_error(&err) => {
                let sleep_ms = backoff_ms + rng.random_range(0..=backoff_ms);
                tracing::debug!(
                    target: "cass::indexer::persist",
                    attempt = attempt + 1,
                    max_retries,
                    backoff_ms = sleep_ms,
                    error = %err,
                    "writer_retry"
                );
                std::thread::sleep(Duration::from_millis(sleep_ms));
                backoff_ms = (backoff_ms * 2).min(256);
            }
            Err(err) => return Err(err),
        }
    }
    Err(anyhow!("exhausted writer retries"))
}
