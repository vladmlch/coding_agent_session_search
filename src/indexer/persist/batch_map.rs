use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
#[cfg(test)]
use std::time::Instant;

use anyhow::{Context, Result};

use crate::connectors::NormalizedConversation;
use crate::indexer::{LexicalPopulationStrategy, WorkspaceExclusions};
use crate::model::types::{Agent, AgentKind, Conversation};
use crate::resource_plan::redact_secrets::MemoizingRedactor;
use crate::search::tantivy::TantivyIndex;
use crate::storage::sqlite::{FrankenStorage, InsertOutcome};

#[cfg(test)]
use super::canonical_replay::PersistConversationPerfProfile;
use super::canonical_replay::{
    PersistBatchOutcome, lexical_packet_for_changed_messages, positional_indices_for_inserted,
    publish_changed_packet_messages, record_sliced_raw_mirror_db_link,
};
use super::serial_batch::{
    defer_lexical_updates_enabled, persist_conversations_batched_whole, with_ephemeral_writer,
};

#[cfg(test)]
pub(super) use crate::source_onboarding::conv_map::{
    INNER_REDACTION_MIN_MESSAGES, INNER_REDACTION_MIN_TEXT_BYTES, MAP_HEARTBEAT_MESSAGE_STRIDE,
    use_inner_message_redaction,
};
pub use crate::source_onboarding::conv_map::map_to_internal;
pub(super) use crate::source_onboarding::conv_map::extract_provenance;
#[allow(unused_imports)]
pub(crate) use crate::source_onboarding::conv_map::map_to_internal_with_redactor;

/// Work-liveness heartbeat threaded through batched persistence
/// (gh373/oeu5a). Mirrors the #282/#332/#366 pattern: real work ticks
/// `IndexingProgress::activity` for the run-side stall watchdog and bumps
/// the index-run lock's `last_progress_at_ms` for the status-side stall
/// detector. `NONE` keeps legacy call sites (tests, paths without a
/// progress handle) unchanged.
#[derive(Clone, Copy)]
pub(crate) struct PersistHeartbeat<'a> {
    progress: Option<&'a crate::indexer::IndexingProgress>,
    progress_bump: Option<&'a Arc<AtomicI64>>,
}

impl<'a> PersistHeartbeat<'a> {
    pub(crate) const NONE: PersistHeartbeat<'static> = PersistHeartbeat {
        progress: None,
        progress_bump: None,
    };

    pub(crate) fn new(
        progress: Option<&'a crate::indexer::IndexingProgress>,
        progress_bump: Option<&'a Arc<AtomicI64>>,
    ) -> Self {
        Self {
            progress,
            progress_bump,
        }
    }

    pub(crate) fn tick(&self) {
        if let Some(progress) = self.progress {
            progress.tick_activity();
        }
        crate::indexer::bump_index_run_lock_progress_if_present(self.progress_bump);
    }
}

/// Map a batch of normalized conversations to internal (redacted)
/// form on the rayon pool with one `MemoizingRedactor` per worker.
///
/// This is the single shared implementation for BOTH batched persist
/// paths (batched and single-conversation) — and for the
/// `redaction_perf` bench harness via
/// [`bench_map_batch_to_internal`] — so benchmark numbers measure
/// the exact production transform by construction.
pub(crate) fn map_batch_to_internal(
    convs: &[NormalizedConversation],
    heartbeat: PersistHeartbeat<'_>,
) -> Vec<Conversation> {
    crate::source_onboarding::conv_map::map_batch_to_internal_with_callback(convs, || {
        heartbeat.tick();
    })
}

/// Bench-only public entry point for the production batch mapping
/// path (memoized redaction on the rayon pool). Hidden from docs;
/// see `benches/redaction_perf.rs`.
#[doc(hidden)]
pub fn bench_map_batch_to_internal(convs: &[NormalizedConversation]) -> Vec<Conversation> {
    map_batch_to_internal(convs, PersistHeartbeat::NONE)
}

#[allow(dead_code)]
pub(crate) fn map_to_internal_with_redactor_and_heartbeat(
    conv: &NormalizedConversation,
    redactor: Option<&mut MemoizingRedactor>,
    heartbeat: PersistHeartbeat<'_>,
) -> Conversation {
    crate::source_onboarding::conv_map::map_to_internal_with_redactor_and_callback(
        conv,
        redactor,
        || {
            heartbeat.tick();
        },
    )
}

#[cfg(test)]
pub(super) fn map_message_to_internal(
    m: &crate::connectors::NormalizedMessage,
    mapped_message_index: usize,
    should_redact: bool,
    redactor: Option<&mut MemoizingRedactor>,
    heartbeat: PersistHeartbeat<'_>,
) -> crate::model::types::Message {
    crate::source_onboarding::conv_map::map_message_to_internal_with_callback(
        m,
        mapped_message_index,
        should_redact,
        redactor,
        || {
            heartbeat.tick();
        },
    )
}

/// Retire the stale per-segment Codex rows a merged thread supersedes,
/// then drop their derived canonical + region lexical documents. Called
/// after a persist batch (or a single watch persist) commits, so the
/// merged identity row already exists. A no-op for single-file threads
/// (no metadata.segments) and idempotent once the stale rows are gone.
/// The cleanup is keyed on metadata.segments[].external_id, never on
/// filename patterns. Without a lexical writer (deferred-rebuild lanes)
/// the authoritative rebuild from canonical rows drops the documents.
fn retire_superseded_codex_segments(
    storage: &FrankenStorage,
    mut t_index: Option<&mut TantivyIndex>,
    convs: &[NormalizedConversation],
) -> Result<usize> {
    let mut retired_total = 0usize;
    for conv in convs {
        if conv.agent_slug != "codex" {
            continue;
        }
        let Some(segments) = conv.metadata.get("segments").and_then(|v| v.as_array()) else {
            continue;
        };
        if segments.len() < 2 {
            continue;
        }
        let Some(identity_external_id) = conv.external_id.as_deref() else {
            continue;
        };
        let segment_external_ids: Vec<String> = segments
            .iter()
            .filter_map(|s| s.get("external_id").and_then(|v| v.as_str()))
            .map(str::to_owned)
            .collect();
        if segment_external_ids.is_empty() {
            continue;
        }
        let retired = storage.retire_superseded_codex_segment_rows(
            &conv.source_path.to_string_lossy(),
            identity_external_id,
            &segment_external_ids,
        )?;
        if retired.is_empty() {
            continue;
        }
        retired_total += retired.len();
        if let Some(index) = t_index.as_deref_mut() {
            let mut doc_ids: Vec<String> = Vec::new();
            for row in &retired {
                let identity = |slot: u64| {
                    frankensearch::quill::cass::cass_document_identity(
                        &row.source_id,
                        frankensearch::quill::cass::CassConversationKey {
                            source_path: &row.source_path,
                            id: Some(row.conversation_id),
                        },
                        slot,
                    )
                };
                for &idx in &row.message_indices {
                    doc_ids.push(identity(idx.max(0) as u64));
                }
                for &slot in &row.region_slots {
                    doc_ids.push(identity(slot as u64));
                }
            }
            index.delete_cass_document_ids(&doc_ids)?;
        }
        tracing::info!(
            retired = retired.len(),
            identity = %conv.source_path.display(),
            "retired stale per-segment Codex rows superseded by a merged thread"
        );
    }
    Ok(retired_total)
}

pub fn persist_conversation(
    storage: &FrankenStorage,
    t_index: &mut TantivyIndex,
    conv: &NormalizedConversation,
) -> Result<()> {
    if retain_admitted_conversations(storage, std::slice::from_ref(conv))?
        .as_ref()
        .is_some_and(Vec::is_empty)
    {
        return Ok(());
    }
    tracing::info!(agent = %conv.agent_slug, messages = conv.messages.len(), "persist_conversation");
    prepare_cursor_workspace_repair(storage, None, std::slice::from_ref(conv))?;
    prepare_codebuff_canonical_repair(storage, Some(t_index.path()), std::slice::from_ref(conv))?;
    let internal_conv = map_to_internal(conv);
    let InsertOutcome {
        conversation_id,
        conversation_inserted: _conversation_inserted,
        inserted_indices,
        updated_indices,
        workspace_changed: _,
    } = with_ephemeral_writer(storage, false, "persist_conversation", |writer| {
        let agent = Agent {
            id: None,
            slug: conv.agent_slug.clone(),
            name: conv.agent_slug.clone(),
            version: None,
            kind: AgentKind::Cli,
        };
        let agent_id = writer.ensure_agent(&agent)?;

        let workspace_id = if let Some(ws) = &conv.workspace {
            Some(writer.ensure_workspace(ws, None)?)
        } else {
            None
        };

        let outcome = writer.insert_conversation_tree(agent_id, workspace_id, &internal_conv)?;
        // One coherent extension generation (facts + structural
        // projection + durable viewer_meta receipt) in the same ingest
        // pass — one transaction, receipt and rows land together, or
        // a durable failed mark the backfill lane reconciles.
        crate::viewer_extensions::projection::publish_ingest_generation_best_effort(
            writer.raw(),
            &internal_conv,
            outcome.conversation_id,
        );
        // Task 11: post-commit change-journal notify (see the batch
        // site below for the trigger-vs-notify split).
        crate::viewer_extensions::changes::notify_committed_change_best_effort(writer.raw());
        // Task 12: wake any bridge event listener (wakeup-only).
        if let Ok(db_path) = writer.database_path() {
            crate::viewer_extensions::changes::send_bridge_wakeup_best_effort(&db_path);
        }
        Ok(outcome)
    })?;

    // Task 5: the generation publication above already committed this
    // session's region inventory; its region documents stage on the
    // same writer and stamp `published` after this writer's commit
    // (a no-op marks row — e.g. a lane without the extension profile —
    // leaves docs_state absent and is skipped honestly by the
    // persisted-mark lookup). Staged BEFORE the canonical documents:
    // Quill's `upsert` publishes pending work when a re-staged
    // identity already lives in the committed snapshot, so staging
    // regions after canonical docs would flush this generation's
    // messages ahead of the writer's real commit boundary.
    t_index.stage_search_region_docs(storage.raw(), conversation_id)?;

    // Publish inserted messages and replace revised native messages.
    // Routed through the packet pipeline per
    // ibuuh.32 sink migration; equivalence guaranteed by
    // tests::persist_packet_pipeline_matches_legacy_for_incremental_inline.
    let changed_indices: Vec<_> = inserted_indices
        .iter()
        .chain(&updated_indices)
        .copied()
        .collect();
    if !defer_lexical_updates_enabled() && !changed_indices.is_empty() {
        let packet =
            lexical_packet_for_changed_messages(storage, conversation_id, &changed_indices)?;
        let positional = positional_indices_for_inserted(&packet, &changed_indices);
        if !positional.is_empty() {
            publish_changed_packet_messages(
                t_index,
                &packet,
                &positional,
                conversation_id,
                !updated_indices.is_empty(),
            )?;
        }
    }
    retire_superseded_codex_segments(storage, Some(t_index), std::slice::from_ref(conv))?;
    Ok(())
}

#[cfg(test)]
pub(super) fn persist_conversation_with_profile(
    storage: &FrankenStorage,
    t_index: &mut TantivyIndex,
    conv: &NormalizedConversation,
    profile: &mut PersistConversationPerfProfile,
) -> Result<()> {
    let total_started = Instant::now();
    let db_started = Instant::now();
    prepare_cursor_workspace_repair(storage, None, std::slice::from_ref(conv))?;
    prepare_codebuff_canonical_repair(storage, Some(t_index.path()), std::slice::from_ref(conv))?;
    let internal_conv = map_to_internal(conv);
    let InsertOutcome {
        conversation_id,
        conversation_inserted: _conversation_inserted,
        inserted_indices,
        updated_indices,
        workspace_changed: _,
    } = with_ephemeral_writer(storage, false, "persist_conversation", |writer| {
        let agent = Agent {
            id: None,
            slug: conv.agent_slug.clone(),
            name: conv.agent_slug.clone(),
            version: None,
            kind: AgentKind::Cli,
        };
        let agent_id = writer.ensure_agent(&agent)?;

        let workspace_id = if let Some(ws) = &conv.workspace {
            Some(writer.ensure_workspace(ws, None)?)
        } else {
            None
        };

        writer.insert_conversation_tree(agent_id, workspace_id, &internal_conv)
    })?;
    profile.db_duration += db_started.elapsed();

    let changed_indices: Vec<_> = inserted_indices
        .iter()
        .chain(&updated_indices)
        .copied()
        .collect();
    if !defer_lexical_updates_enabled() && !changed_indices.is_empty() {
        let packet_started = Instant::now();
        let packet =
            lexical_packet_for_changed_messages(storage, conversation_id, &changed_indices)?;
        profile.packet_duration += packet_started.elapsed();

        let positional_started = Instant::now();
        let positional = positional_indices_for_inserted(&packet, &changed_indices);
        profile.positional_duration += positional_started.elapsed();

        if !positional.is_empty() {
            let tantivy_add_started = Instant::now();
            publish_changed_packet_messages(
                t_index,
                &packet,
                &positional,
                conversation_id,
                !updated_indices.is_empty(),
            )?;
            profile.tantivy_add_duration += tantivy_add_started.elapsed();
        }
    }

    profile.invocations = profile.invocations.saturating_add(1);
    profile.messages = profile.messages.saturating_add(conv.messages.len());
    profile.inserted_messages = profile
        .inserted_messages
        .saturating_add(inserted_indices.len());
    profile.total_duration += total_started.elapsed();
    Ok(())
}

/// Persist multiple conversations in a single database transaction for better performance.
/// This reduces SQLite transaction overhead when indexing many conversations at once.
///
/// Uses `IndexingCache` (Opt 7.2) to prevent N+1 queries for agent/workspace IDs.
/// Set `CASS_SQLITE_CACHE=0` to disable caching for debugging.
#[cfg(test)]
pub(in crate::indexer) fn persist_conversations_batched(
    storage: &FrankenStorage,
    t_index: Option<&mut TantivyIndex>,
    convs: &[NormalizedConversation],
    lexical_strategy: LexicalPopulationStrategy,
    defer_checkpoints: bool,
) -> Result<PersistBatchOutcome> {
    persist_conversations_batched_inner(
        storage,
        t_index,
        convs,
        lexical_strategy,
        defer_checkpoints,
        false,
        None,
        PersistHeartbeat::NONE,
        None,
    )
}

/// gh373/oeu5a: carries the work-liveness heartbeat for the streaming
/// ingest chokepoints, so a minutes-long giant-batch persist keeps
/// ticking the stall watchdog and the index-run lock instead of reading
/// as a wedge. Callers without a progress handle pass
/// `PersistHeartbeat::NONE`.
#[allow(clippy::too_many_arguments)]
pub(in crate::indexer) fn persist_conversations_batched_with_raw_mirror_links(
    storage: &FrankenStorage,
    t_index: Option<&mut TantivyIndex>,
    data_dir: &Path,
    convs: &[NormalizedConversation],
    lexical_strategy: LexicalPopulationStrategy,
    defer_checkpoints: bool,
    heartbeat: PersistHeartbeat<'_>,
) -> Result<PersistBatchOutcome> {
    persist_conversations_batched_inner(
        storage,
        t_index,
        convs,
        lexical_strategy,
        defer_checkpoints,
        false,
        Some(data_dir),
        heartbeat,
        None,
    )
}

/// gh373/oeu5a: heartbeat-carrying (see
/// `persist_conversations_batched_with_raw_mirror_links`).
#[allow(clippy::too_many_arguments)]
pub(in crate::indexer) fn persist_conversations_batched_with_semantic_delta_and_raw_mirror_links(
    storage: &FrankenStorage,
    t_index: Option<&mut TantivyIndex>,
    data_dir: &Path,
    convs: &[NormalizedConversation],
    lexical_strategy: LexicalPopulationStrategy,
    defer_checkpoints: bool,
    heartbeat: PersistHeartbeat<'_>,
) -> Result<PersistBatchOutcome> {
    persist_conversations_batched_inner(
        storage,
        t_index,
        convs,
        lexical_strategy,
        defer_checkpoints,
        true,
        Some(data_dir),
        heartbeat,
        None,
    )
}

pub(in crate::indexer) fn prepare_cursor_workspace_repair(
    storage: &FrankenStorage,
    data_dir: Option<&Path>,
    convs: &[NormalizedConversation],
) -> Result<()> {
    for conv in convs {
        if conv.agent_slug != "cursor" {
            continue;
        }
        let (source_id, _) = extract_provenance(&conv.metadata);
        if !storage.provider_workspace_repair_needed(
            &conv.agent_slug,
            &source_id,
            conv.external_id.as_deref(),
            &conv.source_path,
            conv.workspace.as_deref(),
            &conv.metadata,
        )? {
            continue;
        }
        let data_dir = data_dir
            .context("Cursor workspace repair requires the canonical index data directory")?;
        let index_path = crate::search::tantivy::index_dir(data_dir)?;
        let db_path = storage.database_path()?;
        let mut state = crate::indexer::LexicalRebuildState::new(
            crate::indexer::deferred_lexical_rebuild_db_state(
                &db_path,
                crate::indexer::count_total_conversations_exact(storage)?,
            ),
            crate::indexer::LEXICAL_REBUILD_PAGE_SIZE,
        );
        state.set_execution_mode(
            crate::indexer::LexicalRebuildExecutionMode::CanonicalMetadataRepair,
        );
        crate::indexer::persist_lexical_rebuild_state(&index_path, &state)?;
        crate::indexer::sync_parent_directory(&crate::indexer::lexical_rebuild_state_path(
            &index_path,
        ))?;
        return Ok(());
    }
    Ok(())
}

pub(super) fn prepare_codebuff_canonical_repair(
    storage: &FrankenStorage,
    index_path: Option<&Path>,
    convs: &[NormalizedConversation],
) -> Result<()> {
    for conv in convs.iter().filter(|conv| conv.agent_slug == "codebuff") {
        let (source_id, _) = extract_provenance(&conv.metadata);
        if !storage.codebuff_message_revisions_needed(&map_to_internal(conv))?
            && !storage.provider_workspace_repair_needed(
                &conv.agent_slug,
                &source_id,
                conv.external_id.as_deref(),
                &conv.source_path,
                conv.workspace.as_deref(),
                &conv.metadata,
            )?
        {
            continue;
        }
        let index_path = index_path.context(
            "Codebuff message revision or workspace change requires the canonical index directory",
        )?;
        let db_path = storage.database_path()?;
        // An in-place revision or a run-state workspace change leaves
        // COUNT/MAX unchanged. Revoke lexical authority before committing
        // canonical rows, so a failed publish followed by an unchanged
        // source replay still rebuilds from SQLite. This repair mode needs
        // no archive-wide count for admission.
        let mut state = crate::indexer::LexicalRebuildState::new(
            crate::indexer::deferred_lexical_rebuild_db_state(&db_path, 0),
            crate::indexer::LEXICAL_REBUILD_PAGE_SIZE,
        );
        state.set_execution_mode(
            crate::indexer::LexicalRebuildExecutionMode::CanonicalMetadataRepair,
        );
        crate::indexer::persist_lexical_rebuild_state(index_path, &state)?;
        crate::indexer::sync_parent_directory(&crate::indexer::lexical_rebuild_state_path(
            index_path,
        ))?;
        return Ok(());
    }
    Ok(())
}

/// Apply explicit archive exclusions before any canonical/derived writes.
/// Shared-store session tombstones survive a sibling's source-file change;
/// single-session source stamps retain their existing change/reingest rule.
/// Workspace exclusions are prospective and never remove archived rows.
fn retain_admitted_conversations(
    storage: &FrankenStorage,
    convs: &[NormalizedConversation],
) -> Result<Option<Vec<NormalizedConversation>>> {
    // Bound retention checks to this batch. Loading every historical
    // exclusion for every source completion multiplies work on full scans.
    let tombstones = {
        let mut paths: Vec<_> = convs
            .iter()
            .map(|conv| conv.source_path.to_string_lossy().into_owned())
            .collect();
        paths.sort_unstable();
        paths.dedup();
        storage.forgotten_source_stamps_for(&paths)?
    };
    let forgotten_sessions = {
        let candidate_keys: Vec<_> = convs
            .iter()
            .filter_map(|conv| {
                let external_id = conv.external_id.as_ref()?;
                let (source_id, _) = extract_provenance(&conv.metadata);
                Some(crate::storage::sqlite::ForgottenConversationKey {
                    source_id,
                    agent_slug: conv.agent_slug.clone(),
                    source_path: conv.source_path.to_string_lossy().into_owned(),
                    external_id: external_id.clone(),
                })
            })
            .collect();
        storage.forgotten_conversation_keys_for(&candidate_keys)?
    };
    let workspace_policy = WorkspaceExclusions::from_env()?;
    if tombstones.is_empty()
        && forgotten_sessions.is_empty()
        && workspace_policy.patterns.is_empty()
    {
        return Ok(None);
    }
    let mut keep = Vec::with_capacity(convs.len());
    let mut changed = Vec::new();
    for conv in convs {
        let path = conv.source_path.to_string_lossy();
        let forgotten_session = !forgotten_sessions.is_empty()
            && conv.external_id.as_ref().is_some_and(|external_id| {
                let (source_id, _) = extract_provenance(&conv.metadata);
                forgotten_sessions.contains(&crate::storage::sqlite::ForgottenConversationKey {
                    source_id,
                    agent_slug: conv.agent_slug.clone(),
                    source_path: path.to_string(),
                    external_id: external_id.clone(),
                })
            });
        if forgotten_session || workspace_policy.excludes(conv.workspace.as_deref()) {
            tracing::debug!(
                source_path = %conv.source_path.display(),
                forgotten_session,
                "skipping a conversation excluded by archive policy"
            );
            keep.push(false);
            continue;
        }
        // Local fix (review 2026-11-02, I1): an unreadable file is not "changed".
        let forgotten_and_unchanged = match tombstones.get(path.as_ref()) {
            None => false,
            Some(stamp) => {
                use crate::indexer::forgotten_source_admission::{
                    ForgottenSourceObservation, observe_forgotten_source,
                };
                match observe_forgotten_source(stamp, &conv.source_path) {
                    ForgottenSourceObservation::Unchanged
                    | ForgottenSourceObservation::Unavailable => true,
                    ForgottenSourceObservation::Changed => {
                        changed.push(path.into_owned());
                        false
                    }
                }
            }
        };
        if forgotten_and_unchanged {
            tracing::debug!(
                source_path = %conv.source_path.display(),
                "skipping a conversation from a source forgotten by `cass forget`"
            );
        }
        keep.push(!forgotten_and_unchanged);
    }
    if !changed.is_empty() {
        changed.sort();
        changed.dedup();
        storage.clear_forgotten_sources(&changed)?;
    }
    if keep.iter().all(|kept| *kept) {
        return Ok(None);
    }
    Ok(Some(
        convs
            .iter()
            .zip(keep)
            .filter(|(_, kept)| *kept)
            .map(|(conv, _)| conv.clone())
            .collect(),
    ))
}

/// m91r6: the most one persist call may carry for a single conversation.
/// A giant conversation (Codex rollouts reach 1 GiB) persisted as ONE
/// storage transaction plus ONE lexical batch peaked at ~22x its source
/// size in RSS; on a machine already in swap that thrashed for longer than
/// the watchdog's 1800 s persist grace.
#[derive(Debug, Clone, Copy)]
pub(in crate::indexer) struct PersistSliceLimits {
    pub(in crate::indexer) messages: usize,
    pub(in crate::indexer) content_bytes: usize,
}

impl PersistSliceLimits {
    pub(in crate::indexer) const DEFAULT: Self = Self {
        messages: 16_384,
        content_bytes: 32 * 1024 * 1024,
    };
}

/// Changed slices an incremental run buffers in the lexical writer before
/// publishing (at most ~128 MiB of text with the default slice size).
const PERSIST_SLICES_PER_LEXICAL_COMMIT: usize = 4;

/// Consecutive message ranges of `conv`, each within `limits` (a single
/// oversized message still forms its own range).
pub(super) fn persist_slice_ranges(
    conv: &NormalizedConversation,
    limits: PersistSliceLimits,
) -> Vec<std::ops::Range<usize>> {
    let mut ranges = Vec::new();
    let mut start = 0;
    let mut bytes = 0usize;
    for (pos, message) in conv.messages.iter().enumerate() {
        if pos > start
            && (pos - start >= limits.messages
                || bytes.saturating_add(message.content.len()) > limits.content_bytes)
        {
            ranges.push(start..pos);
            start = pos;
            bytes = 0;
        }
        bytes = bytes.saturating_add(message.content.len());
    }
    ranges.push(start..conv.messages.len());
    ranges
}

/// Slice only conversations whose later slices take the append path's
/// bounded lookups. That needs a timestamp on every message: an untimed
/// message makes that path read every stored row of the conversation,
/// once per slice (quadratic). Native-ID agents (grok_bot, codebuff)
/// reconcile a whole conversation at once and are never sliced.
pub(super) fn should_slice_conversation(
    conv: &NormalizedConversation,
    limits: PersistSliceLimits,
) -> bool {
    !matches!(conv.agent_slug.as_str(), "grok_bot" | "codebuff")
        && conv
            .messages
            .iter()
            .all(|message| message.created_at.is_some())
        && persist_slice_ranges(conv, limits).len() > 1
}

/// One slice of a giant conversation. A non-final slice ends at its own
/// newest message, so the conversation's recorded end never runs ahead of
/// the rows actually stored; the final slice carries the real `ended_at`.
pub(super) fn persist_conversation_slice(
    conv: &NormalizedConversation,
    range: std::ops::Range<usize>,
    is_final: bool,
) -> NormalizedConversation {
    let messages = conv.messages[range].to_vec();
    let ended_at = if is_final {
        conv.ended_at
    } else {
        messages
            .iter()
            .filter_map(|message| message.created_at)
            .max()
    };
    NormalizedConversation {
        agent_slug: conv.agent_slug.clone(),
        external_id: conv.external_id.clone(),
        title: conv.title.clone(),
        workspace: conv.workspace.clone(),
        source_path: conv.source_path.clone(),
        started_at: conv.started_at,
        ended_at,
        metadata: conv.metadata.clone(),
        messages,
    }
}

#[allow(clippy::too_many_arguments)]
pub(in crate::indexer) fn persist_conversations_batched_inner(
    storage: &FrankenStorage,
    t_index: Option<&mut TantivyIndex>,
    convs: &[NormalizedConversation],
    lexical_strategy: LexicalPopulationStrategy,
    defer_checkpoints: bool,
    capture_semantic_delta: bool,
    raw_mirror_data_dir: Option<&Path>,
    heartbeat: PersistHeartbeat<'_>,
    source_completion: Option<&crate::storage::sqlite::SourceIngestLedgerEntry>,
) -> Result<PersistBatchOutcome> {
    persist_conversations_batched_sliced(
        storage,
        t_index,
        convs,
        lexical_strategy,
        defer_checkpoints,
        capture_semantic_delta,
        raw_mirror_data_dir,
        heartbeat,
        source_completion,
        PersistSliceLimits::DEFAULT,
    )
}

/// Persist `convs`, splitting any conversation larger than `limits` into
/// consecutive slices, each its own storage transaction and lexical
/// update. The first slice creates the conversation; later slices take
/// the ordinary append path (the same one a growing session takes every
/// run), so a crash between slices leaves a prefix that the next run
/// completes. The source-ledger completion rides only on the final call.
/// Native-ID agents (grok_bot, codebuff) reconcile a whole conversation at
/// once and are never sliced.
#[allow(clippy::too_many_arguments)]
pub(in crate::indexer) fn persist_conversations_batched_sliced(
    storage: &FrankenStorage,
    mut t_index: Option<&mut TantivyIndex>,
    convs: &[NormalizedConversation],
    lexical_strategy: LexicalPopulationStrategy,
    defer_checkpoints: bool,
    capture_semantic_delta: bool,
    raw_mirror_data_dir: Option<&Path>,
    heartbeat: PersistHeartbeat<'_>,
    source_completion: Option<&crate::storage::sqlite::SourceIngestLedgerEntry>,
    limits: PersistSliceLimits,
) -> Result<PersistBatchOutcome> {
    let retained;
    let admitted = match retain_admitted_conversations(storage, convs)? {
        Some(kept) => {
            retained = kept;
            retained.as_slice()
        }
        None => convs,
    };
    if admitted.is_empty() {
        if !convs.is_empty()
            && let Some(completion) = source_completion
        {
            // Intentional exclusion is a completed policy decision. Keep
            // its source evidence so an unchanged excluded store is not
            // reparsed on every pass; the policy binds the observation.
            with_ephemeral_writer(
                storage,
                defer_checkpoints,
                "recording deliberately filtered source completion",
                |writer| writer.record_filtered_source_completion(completion),
            )?;
        }
        return Ok(PersistBatchOutcome::default());
    }
    let convs = admitted;
    let oversized = |conv: &NormalizedConversation| should_slice_conversation(conv, limits);
    if !convs.iter().any(oversized) {
        let outcome = persist_conversations_batched_whole(
            storage,
            t_index.as_deref_mut(),
            convs,
            lexical_strategy,
            defer_checkpoints,
            capture_semantic_delta,
            raw_mirror_data_dir,
            true,
            heartbeat,
            source_completion,
        )?;
        retire_superseded_codex_segments(storage, t_index, convs)?;
        return Ok(outcome);
    }

    // Runs of ordinary conversations still persist together; each giant
    // conversation becomes its own sequence of slice calls. The final
    // slice carries the whole conversation for its raw-mirror link.
    enum Part<'a> {
        Whole(&'a [NormalizedConversation]),
        Slice {
            slice: Box<NormalizedConversation>,
            finishes: Option<&'a NormalizedConversation>,
        },
    }
    let mut parts = Vec::new();
    let mut run_start = 0;
    for (pos, conv) in convs.iter().enumerate() {
        if !oversized(conv) {
            continue;
        }
        if run_start < pos {
            parts.push(Part::Whole(&convs[run_start..pos]));
        }
        let ranges = persist_slice_ranges(conv, limits);
        let last = ranges.len() - 1;
        for (slice, range) in ranges.into_iter().enumerate() {
            let is_final = slice == last;
            parts.push(Part::Slice {
                slice: Box::new(persist_conversation_slice(conv, range, is_final)),
                finishes: is_final.then_some(conv),
            });
        }
        run_start = pos + 1;
    }
    if run_start < convs.len() {
        parts.push(Part::Whole(&convs[run_start..]));
    }

    let mut outcome = PersistBatchOutcome::default();
    let last_part = parts.len() - 1;
    let mut changed_parts_since_commit = 0usize;
    for (index, part) in parts.into_iter().enumerate() {
        let is_last = index == last_part;
        // An inline rebuild replays each touched conversation in full from
        // the canonical rows (streamed in bounded chunks); replaying it
        // after every slice would be quadratic. A giant conversation's
        // earlier slices only store rows, and its final slice replays the
        // whole conversation. A part replays only its own conversations,
        // so runs of ordinary conversations, and a final slice that is not
        // the batch's last part, keep the inline rebuild.
        let part_strategy = if matches!(part, Part::Slice { finishes: None, .. })
            && lexical_strategy == LexicalPopulationStrategy::InlineRebuildFromScan
        {
            LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild
        } else {
            lexical_strategy
        };
        let completion = source_completion.filter(|_| is_last);
        let part_outcome = match &part {
            Part::Whole(batch) => persist_conversations_batched_whole(
                storage,
                t_index.as_deref_mut(),
                batch,
                part_strategy,
                defer_checkpoints,
                capture_semantic_delta,
                raw_mirror_data_dir,
                true,
                heartbeat,
                completion,
            ),
            Part::Slice { slice, .. } => persist_conversations_batched_whole(
                storage,
                t_index.as_deref_mut(),
                std::slice::from_ref(slice.as_ref()),
                part_strategy,
                defer_checkpoints,
                capture_semantic_delta,
                raw_mirror_data_dir,
                false,
                heartbeat,
                completion,
            ),
        }?;
        if let Part::Slice {
            finishes: Some(conv),
            ..
        } = &part
            && let Some(&conversation_id) = part_outcome.canonical_conversation_ids.first()
        {
            record_sliced_raw_mirror_db_link(raw_mirror_data_dir, conv, conversation_id);
        }
        let changed = part_outcome.inserted_messages + part_outcome.updated_messages > 0;
        outcome.merge(part_outcome);
        // Bound the lexical writer's buffered documents to a few slices.
        // Every publish re-verifies the live segments (seconds on a large
        // index), so slices that changed nothing (a growing session's
        // already-stored history) never force one.
        changed_parts_since_commit += usize::from(changed);
        if !is_last
            && changed_parts_since_commit >= PERSIST_SLICES_PER_LEXICAL_COMMIT
            && lexical_strategy == LexicalPopulationStrategy::IncrementalInline
            && let Some(index) = t_index.as_deref_mut()
        {
            // Slice-bound commit seals staged region documents too —
            // flush their queued marks stamps `published` with it.
            index.commit_with_region_doc_stamps(storage.raw())?;
            changed_parts_since_commit = 0;
        }
        heartbeat.tick();
    }
    retire_superseded_codex_segments(storage, t_index, convs)?;
    Ok(outcome)
}
