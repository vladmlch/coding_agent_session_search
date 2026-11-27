use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Write as _;
use std::path::Path;
#[cfg(test)]
use std::time::Duration;

use anyhow::Result;

use crate::connectors::NormalizedConversation;
use crate::franken_sync::compat::{ConnectionExt, ParamValue, RowExt};
use crate::indexer::LexicalPopulationStrategy;
#[cfg(test)]
use crate::indexer::WatchLexicalTestFault;
use crate::indexer::semantic::{
    EmbeddingInput, packet_embedding_inputs_from_storage_for_message_ids,
};
use crate::model::conversation_packet::{ConversationPacket, ConversationPacketProvenance};
use crate::model::types::{Conversation, Message};
use crate::search::tantivy::TantivyIndex;
use crate::storage::sqlite::{FrankenStorage, InsertOutcome};

/// Indexed-text bytes read per step when a whole canonical conversation is
/// replayed inline, so a conversation with gigabytes of history never has
/// more than one chunk in memory (bgn6s). Each message is still capped by
/// `lexical_max_message_content_bytes`, so a chunk exceeds this only when
/// it holds a single message.
const CANONICAL_LEXICAL_REPLAY_CHUNK_BYTES: usize = 64 * 1024 * 1024;

/// The canonical conversation a lexical packet is built on, without its
/// messages. As in a full rebuild, stored metadata and snippets are
/// omitted: only the origin envelope the lexical sink reads is synthesized
/// from canonical columns, and the incoming transcript is never cloned.
pub(super) fn canonical_lexical_conversation(
    storage: &FrankenStorage,
    conversation_id: i64,
) -> Result<Conversation> {
    let mut canonical = storage.raw().query_row_map(
        "SELECT COALESCE(a.slug, 'unknown'), c.external_id, c.title, c.source_path,
                c.started_at, c.ended_at, c.source_id, c.origin_host, w.path
         FROM conversations c
         LEFT JOIN agents a ON a.id = c.agent_id
         LEFT JOIN workspaces w ON w.id = c.workspace_id
         WHERE c.id = ?1",
        &[ParamValue::from(conversation_id)],
        |row| {
            let raw_source_id: Option<String> = row.get_typed(6)?;
            let raw_origin_host: Option<String> = row.get_typed(7)?;
            let source_id = crate::search::tantivy::normalized_index_source_id(
                raw_source_id.as_deref(),
                None,
                raw_origin_host.as_deref(),
            );
            let origin_host = if source_id == "local" {
                None
            } else {
                crate::search::tantivy::normalized_index_origin_host(raw_origin_host.as_deref())
            };
            Ok(Conversation {
                id: Some(conversation_id),
                agent_slug: row.get_typed(0)?,
                external_id: row.get_typed(1)?,
                title: row.get_typed(2)?,
                source_path: row.get_typed::<String>(3)?.into(),
                started_at: row.get_typed(4)?,
                ended_at: row.get_typed(5)?,
                workspace: row.get_typed::<Option<String>>(8)?.map(Into::into),
                approx_tokens: None,
                metadata_json: serde_json::Value::Null,
                messages: Vec::new(),
                source_id,
                origin_host,
            })
        },
    )?;
    // Use the registered kind, as a full rebuild's source map does. Without
    // it, a local source named anything but `local` (GH #524's
    // `full-history-plan`) was replayed as remote and dropped out of
    // local-only keyword searches.
    let registered_kind = storage
        .get_source(&canonical.source_id)?
        .map(|source| source.kind);
    let origin_kind = crate::search::tantivy::normalized_index_origin_kind(
        &canonical.source_id,
        registered_kind
            .as_ref()
            .map(crate::sources::provenance::SourceKind::as_str),
    );
    canonical.metadata_json = serde_json::json!({
        "cass": {
            "origin": {
                "source_id": &canonical.source_id,
                "kind": origin_kind,
                "host": &canonical.origin_host,
            }
        }
    });
    Ok(canonical)
}

pub(super) fn canonical_lexical_packet(
    conversation: &Conversation,
    messages: Vec<Message>,
) -> ConversationPacket {
    // The envelope canonical_lexical_conversation synthesized already
    // carries the registered kind.
    let provenance = ConversationPacketProvenance {
        source_id: conversation.source_id.clone(),
        origin_kind: crate::search::tantivy::normalized_index_origin_kind(
            &conversation.source_id,
            conversation
                .metadata_json
                .pointer("/cass/origin/kind")
                .and_then(serde_json::Value::as_str),
        ),
        origin_host: conversation.origin_host.clone(),
    };
    let mut canonical = conversation.clone();
    canonical.messages = messages;
    ConversationPacket::from_canonical_replay_owned(canonical, provenance)
}

/// The canonical rows of the message indices a persistence outcome
/// changed. GH #466: persistence can retain an earlier message variant, so
/// the packet carries what was stored, never the freshly parsed source.
/// Only the changed rows are held, so an append to a long conversation
/// never reads its older text into memory (bgn6s).
pub(super) fn lexical_packet_for_changed_messages(
    storage: &FrankenStorage,
    conversation_id: i64,
    changed_indices: &[i64],
) -> Result<ConversationPacket> {
    let conversation = canonical_lexical_conversation(storage, conversation_id)?;
    let wanted: HashSet<i64> = changed_indices.iter().copied().collect();
    let mut messages = Vec::with_capacity(wanted.len());
    if let (Some(&first), Some(&last)) = (wanted.iter().min(), wanted.iter().max()) {
        storage.for_each_lexical_rebuild_message(
            conversation_id,
            Some((first, last)),
            |message| {
                if wanted.contains(&message.idx) {
                    messages.push(message);
                }
                Ok(true)
            },
        )?;
    }
    Ok(canonical_lexical_packet(&conversation, messages))
}

/// Reconcile every canonical message of a conversation, one bounded
/// chunk at a time. Reconciliation publishes under stable per-message
/// identities in independent engine batches and never promised
/// whole-conversation atomicity, so chunking bounds memory without
/// changing the published documents.
pub(super) fn replay_canonical_conversation_for_rebuild(
    storage: &FrankenStorage,
    index: &mut TantivyIndex,
    conversation_id: i64,
) -> Result<()> {
    replay_canonical_conversation_in_chunks(
        storage,
        index,
        conversation_id,
        CANONICAL_LEXICAL_REPLAY_CHUNK_BYTES,
    )
}

pub(super) fn replay_canonical_conversation_in_chunks(
    storage: &FrankenStorage,
    index: &mut TantivyIndex,
    conversation_id: i64,
    chunk_bytes: usize,
) -> Result<()> {
    let conversation = canonical_lexical_conversation(storage, conversation_id)?;
    let footprints = storage.lexical_rebuild_message_footprints(conversation_id)?;
    for (first_idx, last_idx) in
        crate::indexer::lexical_rebuild_conversation_chunks(&footprints, chunk_bytes)
    {
        let messages = storage.fetch_messages_for_lexical_rebuild_idx_range(
            conversation_id,
            first_idx,
            last_idx,
        )?;
        let packet = canonical_lexical_packet(&conversation, messages);
        publish_canonical_packet_for_rebuild(index, &packet, conversation_id)?;
    }
    Ok(())
}

/// Map `outcome.inserted_indices` (message idx values from
/// `InsertOutcome`) onto positional indices into the packet's
/// messages Vec. Preserves source ordering so the lexical sink
/// emits docs in the same order the legacy filter produced.
pub(super) fn positional_indices_for_inserted(
    packet: &ConversationPacket,
    inserted_indices: &[i64],
) -> Vec<usize> {
    if inserted_indices.is_empty() {
        return Vec::new();
    }
    // Small linear-search lookup is fine here: a single conversation's
    // inserted_indices is bounded by message count (typically <100,
    // rarely >>10k) and we only run this once per persist outcome.
    let inserted: HashSet<i64> = inserted_indices.iter().copied().collect();
    packet
        .payload
        .messages
        .iter()
        .enumerate()
        .filter(|(_, message)| inserted.contains(&message.idx))
        .map(|(position, _)| position)
        .collect()
}

pub(super) fn changed_message_indices(outcome: &InsertOutcome) -> Vec<i64> {
    outcome
        .inserted_indices
        .iter()
        .chain(&outcome.updated_indices)
        .copied()
        .collect()
}

/// Stage one persisted session's region documents on the lexical lane's
/// writer. Only `IncrementalInline` writes into the live snapshot, so only
/// it may skip a session whose region documents are already published.
/// The rebuild lanes (`InlineRebuildFromScan`, `DeferredAuthoritativeDbRebuild`)
/// populate a fresh snapshot that holds no region documents, so every
/// persisted session must re-materialize there.
pub(super) fn stage_persisted_region_docs(
    index: &mut TantivyIndex,
    db: &impl crate::viewer_extensions::facts::FactsDb,
    conversation_id: i64,
    lexical_strategy: LexicalPopulationStrategy,
) -> Result<usize> {
    if lexical_strategy == LexicalPopulationStrategy::IncrementalInline {
        index.stage_search_region_docs_unless_published(db, conversation_id)
    } else {
        index.stage_search_region_docs(db, conversation_id)
    }
}

/// Whole-archive republish deferral threshold: once MORE sessions than
/// this carry stale-version region marks, per-batch region-doc
/// convergence costs one Quill segment publish per persist batch — the
/// streaming batch limits make every oversized agent session its own
/// batch, so a REGION_VERSION republish of thousands of sessions
/// publishes thousands of successor segments and re-validates the whole
/// archive that many times (measured 2026-11-10 on the owner's
/// 6,107-session archive: the incumbent lane spent ~3 s per session and
/// was heading past six hours). Above the threshold the lane defers
/// convergence to its final commit. Below it, convergence keeps riding
/// each batch's prelude so ordinary incremental runs, watch events and
/// crash leftovers converge with their batch exactly as before. Eight
/// streaming batches (64 conversations each) is far past any ordinary
/// incremental run's dirty set and small enough that a deferral never
/// delays a small run's first region search.
pub(in crate::indexer) const REGION_DOC_REPUBLISH_DEFER_MIN_STALE_SESSIONS: i64 = 512;

/// Latch whole-archive republish deferral onto the run's lexical writer
/// when the stale-version marks a REGION_VERSION bump leaves exceed
/// [`REGION_DOC_REPUBLISH_DEFER_MIN_STALE_SESSIONS`]. The non-watch
/// incremental lane calls this before its first persist batch (the scan
/// entry and the streaming consumer's prologue, each behind the scan
/// entry's lane gate `!opts.watch && opts.watch_once_paths.is_none()` —
/// the watch contract lives at the call sites, not in this helper);
/// the per-batch
/// region-doc prelude then stages nothing and the lane drains the
/// accumulated pending set with one bounded
/// [`TantivyIndex::converge_pending_search_region_docs`] pass right
/// before its final commit. Marks stay honestly `pending` in between —
/// a crash mid-run leaves exactly the state the next run's sweep
/// re-converges. Watch never latches (its chunks converge per event,
/// and a watch run has no final drain), and neither do the rebuild
/// lanes (a fresh snapshot re-materializes every session; additive
/// upserts do not republish). Idempotent: a second call on an already
/// latched writer is a no-op.
pub(in crate::indexer) fn maybe_defer_region_doc_convergence_for_republish(
    db: &impl crate::viewer_extensions::facts::FactsDb,
    index: &mut TantivyIndex,
) -> Result<()> {
    if index.region_doc_convergence_deferred_to_final_drain() {
        return Ok(());
    }
    let stale_sessions =
        crate::viewer_extensions::search_regions::stale_region_version_session_count(db)?;
    if stale_sessions > REGION_DOC_REPUBLISH_DEFER_MIN_STALE_SESSIONS {
        index.defer_region_doc_convergence_to_final_drain();
        tracing::info!(
            stale_sessions,
            threshold = REGION_DOC_REPUBLISH_DEFER_MIN_STALE_SESSIONS,
            "region-doc republish set exceeds the deferral threshold; converging region documents once at this run's final commit instead of per persist batch"
        );
    }
    Ok(())
}

pub(super) fn publish_changed_packet_messages(
    index: &mut TantivyIndex,
    packet: &ConversationPacket,
    positional: &[usize],
    conversation_id: i64,
    has_revisions: bool,
) -> Result<()> {
    if has_revisions {
        index.reconcile_messages_from_packet(packet, positional, Some(conversation_id))
    } else {
        index.add_messages_from_packet(packet, Some(positional), Some(conversation_id), |_| Ok(()))
    }
}

fn publish_canonical_packet_for_rebuild(
    index: &mut TantivyIndex,
    packet: &ConversationPacket,
    conversation_id: i64,
) -> Result<()> {
    // Every provider now replays canonical history. Overlapping source
    // packets can therefore cover the same rows within or across batches.
    let positions: Vec<_> = (0..packet.payload.messages.len()).collect();
    index.reconcile_messages_from_packet(packet, &positions, Some(conversation_id))
}

pub(in crate::indexer) fn replay_watch_canonical_conversation(
    storage: &FrankenStorage,
    index: &mut TantivyIndex,
    conversation_id: i64,
) -> Result<()> {
    if should_inject_incremental_lexical_update_oom() {
        anyhow::bail!("out of memory");
    }
    replay_canonical_conversation_for_rebuild(storage, index, conversation_id)
}

#[cfg(test)]
#[derive(Debug, Clone, Default)]
pub(super) struct PersistConversationPerfProfile {
    pub(super) invocations: usize,
    pub(super) messages: usize,
    pub(super) inserted_messages: usize,
    pub(super) total_duration: Duration,
    pub(super) db_duration: Duration,
    pub(super) packet_duration: Duration,
    pub(super) positional_duration: Duration,
    pub(super) tantivy_add_duration: Duration,
}

#[cfg(test)]
impl PersistConversationPerfProfile {
    fn millis(duration: Duration) -> f64 {
        duration.as_secs_f64() * 1000.0
    }

    pub(super) fn log_summary(&self, label: &str) {
        let calls = self.invocations.max(1) as f64;
        let accounted_duration = self.db_duration
            + self.packet_duration
            + self.positional_duration
            + self.tantivy_add_duration;
        let residual_duration = self.total_duration.saturating_sub(accounted_duration);
        eprintln!(
            concat!(
                "CASS_PERSIST_STAGE_PROFILE ",
                "label={} calls={} messages={} inserted_messages={} ",
                "total_ms={:.3} db_ms={:.3} packet_ms={:.3} positional_ms={:.3} ",
                "tantivy_add_ms={:.3} residual_ms={:.3} ",
                "avg_total_ms={:.3} avg_db_ms={:.3} avg_packet_ms={:.3} ",
                "avg_positional_ms={:.3} avg_tantivy_add_ms={:.3}"
            ),
            label,
            self.invocations,
            self.messages,
            self.inserted_messages,
            Self::millis(self.total_duration),
            Self::millis(self.db_duration),
            Self::millis(self.packet_duration),
            Self::millis(self.positional_duration),
            Self::millis(self.tantivy_add_duration),
            Self::millis(residual_duration),
            Self::millis(self.total_duration) / calls,
            Self::millis(self.db_duration) / calls,
            Self::millis(self.packet_duration) / calls,
            Self::millis(self.positional_duration) / calls,
            Self::millis(self.tantivy_add_duration) / calls,
        );
    }
}

#[derive(Debug, Clone, Default)]
pub(in crate::indexer) struct PersistBatchOutcome {
    /// All successfully persisted identities, including unchanged replay.
    /// Watch recovery cannot infer this set from inserted/revised counts.
    pub canonical_conversation_ids: BTreeSet<i64>,
    pub inserted_conversations: usize,
    pub inserted_messages: usize,
    pub updated_messages: usize,
    pub workspace_changes: usize,
    pub semantic_delta_max_message_id: Option<i64>,
    pub semantic_delta_inputs: Vec<EmbeddingInput>,
    pub lexical_update_deferred: bool,
    pub lexical_update_error: Option<String>,
}

impl PersistBatchOutcome {
    pub(super) fn record_insert_outcome(&mut self, outcome: &InsertOutcome) {
        self.canonical_conversation_ids
            .insert(outcome.conversation_id);
        self.inserted_conversations = self
            .inserted_conversations
            .saturating_add(usize::from(outcome.conversation_inserted));
        self.inserted_messages = self
            .inserted_messages
            .saturating_add(outcome.inserted_indices.len());
        self.updated_messages = self
            .updated_messages
            .saturating_add(outcome.updated_indices.len());
        if outcome.workspace_changed {
            self.workspace_changes = self.workspace_changes.saturating_add(1);
            self.lexical_update_deferred = true;
            self.lexical_update_error =
                Some("canonical provider title or workspace changed".to_string());
        }
    }

    pub(super) fn extend_semantic_delta(
        &mut self,
        inputs: Vec<EmbeddingInput>,
        max_message_id: Option<i64>,
    ) {
        self.semantic_delta_inputs.extend(inputs);
        if let Some(max_message_id) = max_message_id {
            self.semantic_delta_max_message_id = Some(
                self.semantic_delta_max_message_id
                    .map_or(max_message_id, |current| current.max(max_message_id)),
            );
        }
    }

    pub(in crate::indexer) fn record_deferred_lexical_update(&mut self, error: &anyhow::Error) {
        self.lexical_update_deferred = true;
        if self.lexical_update_error.is_none() {
            self.lexical_update_error = Some(error.to_string());
        }
    }

    pub(in crate::indexer) fn merge(&mut self, other: Self) {
        self.canonical_conversation_ids
            .extend(other.canonical_conversation_ids);
        self.updated_messages = self.updated_messages.saturating_add(other.updated_messages);
        self.workspace_changes = self
            .workspace_changes
            .saturating_add(other.workspace_changes);
        self.inserted_conversations = self
            .inserted_conversations
            .saturating_add(other.inserted_conversations);
        self.inserted_messages = self
            .inserted_messages
            .saturating_add(other.inserted_messages);
        self.extend_semantic_delta(
            other.semantic_delta_inputs,
            other.semantic_delta_max_message_id,
        );
        if other.lexical_update_deferred {
            self.lexical_update_deferred = true;
            if self.lexical_update_error.is_none() {
                self.lexical_update_error = other.lexical_update_error;
            }
        }
    }
}

pub(super) fn should_defer_incremental_lexical_update_after_error(error: &anyhow::Error) -> bool {
    crate::indexer::error_is_out_of_memory(error)
}

#[cfg(test)]
pub(super) fn should_inject_incremental_lexical_update_oom() -> bool {
    crate::indexer::WATCH_LEXICAL_TEST_FAULT.with(|fault| match fault.get() {
        Some(WatchLexicalTestFault::PublicationAfter(0)) => true,
        Some(WatchLexicalTestFault::PublicationAfter(remaining)) => {
            fault.set(Some(WatchLexicalTestFault::PublicationAfter(remaining - 1)));
            false
        }
        _ => crate::indexer::INCREMENTAL_LEXICAL_UPDATE_TEST_OOM.with(std::cell::Cell::get),
    })
}

#[cfg(not(test))]
pub(super) fn should_inject_incremental_lexical_update_oom() -> bool {
    false
}

pub(super) fn load_inserted_message_ids_by_idx(
    storage: &FrankenStorage,
    conversation_id: i64,
    inserted_indices: &[i64],
) -> Result<HashMap<i64, i64>> {
    if inserted_indices.is_empty() {
        return Ok(HashMap::new());
    }

    let mut sql = String::from(
        "SELECT id, idx
         FROM messages
         WHERE conversation_id = ?1
           AND idx IN (",
    );
    let mut params = Vec::with_capacity(inserted_indices.len() + 1);
    params.push(ParamValue::from(conversation_id));
    for (offset, idx) in inserted_indices.iter().enumerate() {
        if offset > 0 {
            sql.push_str(", ");
        }
        let _ = write!(sql, "?{}", offset + 2);
        params.push(ParamValue::from(*idx));
    }
    sql.push_str(") ORDER BY id ASC");

    let rows: Vec<(i64, i64)> = storage.raw().query_map_collect(&sql, &params, |row| {
        Ok((row.get_typed(0)?, row.get_typed(1)?))
    })?;
    Ok(rows.into_iter().map(|(id, idx)| (idx, id)).collect())
}

pub(super) fn packet_semantic_delta_for_outcome(
    storage: &FrankenStorage,
    outcome: &InsertOutcome,
) -> Result<(Vec<EmbeddingInput>, Option<i64>)> {
    let changed_indices = changed_message_indices(outcome);
    if changed_indices.is_empty() {
        return Ok((Vec::new(), None));
    }

    let message_ids_by_idx =
        load_inserted_message_ids_by_idx(storage, outcome.conversation_id, &changed_indices)?;
    if message_ids_by_idx.len() != changed_indices.len() {
        tracing::warn!(
            conversation_id = outcome.conversation_id,
            expected_changed_indices = changed_indices.len(),
            resolved_canonical_message_ids = message_ids_by_idx.len(),
            "skipping packet semantic delta rows without persisted canonical ids"
        );
    }
    let inserted_message_ids: HashSet<i64> = message_ids_by_idx.values().copied().collect();
    let max_message_id = inserted_message_ids.iter().copied().max();

    let inputs = packet_embedding_inputs_from_storage_for_message_ids(
        storage,
        &[outcome.conversation_id],
        &inserted_message_ids,
    )?;
    Ok((inputs, max_message_id))
}

fn raw_mirror_manifest_relative_path(conv: &NormalizedConversation) -> Option<&str> {
    conv.metadata
        .get("cass")
        .and_then(|cass| cass.get("raw_mirror"))
        .and_then(|raw_mirror| raw_mirror.get("manifest_relative_path"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn persisted_raw_mirror_db_link(
    conv: &NormalizedConversation,
    outcome: &InsertOutcome,
) -> crate::raw_mirror::RawMirrorDbLink {
    crate::raw_mirror::RawMirrorDbLink {
        conversation_id: Some(outcome.conversation_id),
        message_count: Some(conv.messages.len()),
        source_path: Some(conv.source_path.display().to_string()),
        started_at_ms: conv.started_at,
    }
}

pub(super) fn record_persisted_raw_mirror_db_link_groups<'a>(
    data_dir: &Path,
    conversations_and_outcomes: impl IntoIterator<
        Item = (&'a NormalizedConversation, &'a InsertOutcome),
    >,
) {
    let mut links_by_manifest: BTreeMap<String, Vec<crate::raw_mirror::RawMirrorDbLink>> =
        BTreeMap::new();
    for (conv, outcome) in conversations_and_outcomes {
        let Some(manifest_relative_path) = raw_mirror_manifest_relative_path(conv) else {
            continue;
        };
        links_by_manifest
            .entry(manifest_relative_path.to_string())
            .or_default()
            .push(persisted_raw_mirror_db_link(conv, outcome));
    }

    for (manifest_relative_path, links) in links_by_manifest {
        if let Err(error) =
            crate::raw_mirror::merge_manifest_db_links(data_dir, &manifest_relative_path, &links)
        {
            tracing::warn!(
                link_count = links.len(),
                manifest_relative_path,
                error = %error,
                "failed to record persisted raw mirror conversation links"
            );
        }
    }
}

pub(super) fn record_persisted_raw_mirror_db_links(
    data_dir: Option<&Path>,
    convs: &[NormalizedConversation],
    outcomes: &[InsertOutcome],
) {
    let Some(data_dir) = data_dir else {
        return;
    };
    record_persisted_raw_mirror_db_link_groups(data_dir, convs.iter().zip(outcomes.iter()));
}

/// The single raw-mirror link for a conversation persisted in slices,
/// carrying the whole parsed conversation's message count. Per-slice links
/// would each add a distinct (and misleading) message count to the
/// manifest and re-verify the mirror blob once per slice.
pub(super) fn record_sliced_raw_mirror_db_link(
    data_dir: Option<&Path>,
    conv: &NormalizedConversation,
    conversation_id: i64,
) {
    let (Some(data_dir), Some(manifest_relative_path)) =
        (data_dir, raw_mirror_manifest_relative_path(conv))
    else {
        return;
    };
    let link = crate::raw_mirror::RawMirrorDbLink {
        conversation_id: Some(conversation_id),
        message_count: Some(conv.messages.len()),
        source_path: Some(conv.source_path.display().to_string()),
        started_at_ms: conv.started_at,
    };
    if let Err(error) =
        crate::raw_mirror::merge_manifest_db_links(data_dir, manifest_relative_path, &[link])
    {
        tracing::warn!(
            manifest_relative_path,
            error = %error,
            "failed to record the raw mirror link of a conversation persisted in slices"
        );
    }
}
