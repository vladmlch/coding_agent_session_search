use std::collections::BTreeSet;

use anyhow::{Context, Result};
use frankensearch::quill::cass::{
    CassDocument as FsCassDocument, CassDocumentRef as FsCassDocumentRef,
};

use crate::connectors::{NormalizedConversation, NormalizedMessage};
use crate::model::conversation_packet::{ConversationPacket, ConversationPacketMessage};
use crate::search::canonicalize::is_hard_message_noise;

use super::{
    TantivyIndex, normalized_index_origin_host, normalized_index_origin_kind,
    normalized_index_source_id, tantivy_add_batch_max_chars, tantivy_add_batch_max_messages,
    tantivy_prebuilt_add_batch_max_messages,
};

#[derive(Clone)]
pub(super) struct CassDocContext {
    agent: String,
    workspace: Option<String>,
    workspace_original: Option<String>,
    pub(super) source_path: String,
    title: Option<String>,
    started_at_fallback: Option<i64>,
    pub(super) source_id: String,
    origin_kind: String,
    origin_host: Option<String>,
    pub(super) conversation_id: Option<i64>,
}

pub(super) fn cass_doc_context(
    conv: &NormalizedConversation,
    conversation_id: Option<i64>,
) -> CassDocContext {
    let cass_origin = conv.metadata.get("cass").and_then(|c| c.get("origin"));
    let raw_source_id = cass_origin
        .and_then(|o| o.get("source_id"))
        .and_then(|v| v.as_str());
    let raw_origin_kind = cass_origin
        .and_then(|o| o.get("kind"))
        .and_then(|v| v.as_str());
    let origin_host = normalized_index_origin_host(
        cass_origin
            .and_then(|o| o.get("host"))
            .and_then(|v| v.as_str()),
    );
    let source_id =
        normalized_index_source_id(raw_source_id, raw_origin_kind, origin_host.as_deref());
    let origin_kind = normalized_index_origin_kind(&source_id, raw_origin_kind);

    CassDocContext {
        agent: conv.agent_slug.clone(),
        workspace: conv
            .workspace
            .as_ref()
            .map(|ws| ws.to_string_lossy().to_string()),
        workspace_original: conv
            .metadata
            .get("cass")
            .and_then(|c| c.get("workspace_original"))
            .and_then(|v| v.as_str())
            .map(ToOwned::to_owned),
        source_path: conv.source_path.to_string_lossy().to_string(),
        title: conv.title.clone(),
        started_at_fallback: conv.started_at,
        source_id,
        origin_kind,
        origin_host,
        conversation_id,
    }
}

pub(super) fn cass_document_for_message(
    context: &CassDocContext,
    msg: &NormalizedMessage,
) -> Option<FsCassDocument> {
    if is_hard_message_noise(Some(msg.role.as_str()), &msg.content) {
        return None;
    }

    Some(FsCassDocument {
        agent: context.agent.clone(),
        workspace: context.workspace.clone(),
        workspace_original: context.workspace_original.clone(),
        source_path: context.source_path.clone(),
        msg_idx: msg.idx.max(0) as u64,
        created_at: msg.created_at.or(context.started_at_fallback),
        title: context.title.clone(),
        content: msg.content.clone(),
        conversation_id: context.conversation_id,
        source_id: context.source_id.clone(),
        origin_kind: context.origin_kind.clone(),
        origin_host: context.origin_host.clone(),
    })
}

fn push_cass_document_into_pending(
    docs: &mut Vec<FsCassDocument>,
    pending_chars: &mut usize,
    doc: FsCassDocument,
) {
    *pending_chars = pending_chars.saturating_add(doc.content.len());
    docs.push(doc);
}

/// Build the per-document context the lexical sink needs from a
/// [`ConversationPacket`]. Packet builders (raw scan + canonical replay)
/// already normalized identity, provenance, metadata, and timestamps, so
/// the lexical builder no longer has to re-walk the raw connector
/// payload (`coding_agent_session_search-ibuuh.32`). We still re-derive
/// `source_id`/`origin_kind`/`origin_host` from `metadata.cass.origin`
/// (rather than trusting `packet.payload.provenance` blindly) so the
/// packet pipeline produces byte-identical CassDocuments to the legacy
/// `cass_doc_context` path — that's the equivalence gate covered by
/// `packet_driven_lexical_pipeline_matches_legacy_for_normalized_conv`.
pub(super) fn cass_doc_context_from_packet(packet: &ConversationPacket) -> CassDocContext {
    let payload = &packet.payload;
    let metadata = &payload.metadata_json;
    let cass_origin = metadata.get("cass").and_then(|c| c.get("origin"));
    let raw_source_id = cass_origin
        .and_then(|o| o.get("source_id"))
        .and_then(|v| v.as_str());
    let raw_origin_kind = cass_origin
        .and_then(|o| o.get("kind"))
        .and_then(|v| v.as_str());
    let origin_host = normalized_index_origin_host(
        cass_origin
            .and_then(|o| o.get("host"))
            .and_then(|v| v.as_str()),
    );
    let source_id =
        normalized_index_source_id(raw_source_id, raw_origin_kind, origin_host.as_deref());
    let origin_kind = normalized_index_origin_kind(&source_id, raw_origin_kind);

    CassDocContext {
        agent: payload.identity.agent_slug.clone(),
        workspace: payload.identity.workspace.clone(),
        workspace_original: metadata
            .get("cass")
            .and_then(|c| c.get("workspace_original"))
            .and_then(|v| v.as_str())
            .map(ToOwned::to_owned),
        source_path: payload.identity.source_path.clone(),
        title: payload.identity.title.clone(),
        started_at_fallback: payload.timestamps.started_at,
        source_id,
        origin_kind,
        origin_host,
        conversation_id: payload.identity.conversation_id,
    }
}

/// Build a single CassDocument from a packet message, matching the
/// legacy `cass_document_for_message` filter and projection rules.
pub(super) fn cass_document_for_packet_message(
    context: &CassDocContext,
    msg: &ConversationPacketMessage,
) -> Option<FsCassDocument> {
    if is_hard_message_noise(Some(msg.role.as_str()), &msg.content) {
        return None;
    }

    Some(FsCassDocument {
        agent: context.agent.clone(),
        workspace: context.workspace.clone(),
        workspace_original: context.workspace_original.clone(),
        source_path: context.source_path.clone(),
        msg_idx: msg.idx.max(0) as u64,
        created_at: msg.created_at.or(context.started_at_fallback),
        title: context.title.clone(),
        content: msg.content.clone(),
        conversation_id: context.conversation_id,
        source_id: context.source_id.clone(),
        origin_kind: context.origin_kind.clone(),
        origin_host: context.origin_host.clone(),
    })
}

#[allow(clippy::too_many_arguments)]
fn push_packet_message_into_pending<F>(
    inner: &mut crate::search::quill_bridge::QuillCassIndex,
    context: &CassDocContext,
    msg: &ConversationPacketMessage,
    docs: &mut Vec<FsCassDocument>,
    pending_chars: &mut usize,
    max_messages: usize,
    max_chars: usize,
    on_batch_flushed: &mut F,
) -> Result<()>
where
    F: FnMut(usize) -> Result<()>,
{
    let Some(doc) = cass_document_for_packet_message(context, msg) else {
        return Ok(());
    };
    push_cass_document_into_pending(docs, pending_chars, doc);
    if docs.len() >= max_messages || *pending_chars >= max_chars {
        let flushed_docs = docs.len();
        inner.add_cass_documents(docs.as_slice())?;
        on_batch_flushed(flushed_docs)?;
        docs.clear();
        *pending_chars = 0;
    }
    Ok(())
}

impl TantivyIndex {
    pub fn add_messages(
        &mut self,
        conv: &NormalizedConversation,
        messages: &[NormalizedMessage],
    ) -> Result<()> {
        self.add_messages_with_conversation_id(conv, messages, None)
    }

    pub fn add_messages_with_conversation_id(
        &mut self,
        conv: &NormalizedConversation,
        messages: &[NormalizedMessage],
        conversation_id: Option<i64>,
    ) -> Result<()> {
        self.add_messages_with_conversation_id_and_batch_hook(
            conv,
            messages,
            conversation_id,
            |_| Ok(()),
        )
    }

    pub fn add_messages_with_conversation_id_and_batch_hook<F>(
        &mut self,
        conv: &NormalizedConversation,
        messages: &[NormalizedMessage],
        conversation_id: Option<i64>,
        mut on_batch_flushed: F,
    ) -> Result<()>
    where
        F: FnMut(usize) -> Result<()>,
    {
        let context = cass_doc_context(conv, conversation_id);
        let max_messages = tantivy_add_batch_max_messages();
        let max_chars = tantivy_add_batch_max_chars();
        let mut docs: Vec<FsCassDocument> = Vec::new();
        let mut pending_chars = 0usize;

        for msg in messages {
            let Some(doc) = cass_document_for_message(&context, msg) else {
                continue;
            };
            push_cass_document_into_pending(&mut docs, &mut pending_chars, doc);
            if docs.len() >= max_messages || pending_chars >= max_chars {
                let flushed_docs = docs.len();
                self.inner.add_cass_documents(&docs)?;
                on_batch_flushed(flushed_docs)?;
                docs.clear();
                pending_chars = 0;
            }
        }

        if docs.is_empty() {
            Ok(())
        } else {
            let flushed_docs = docs.len();
            self.inner.add_cass_documents(&docs)?;
            on_batch_flushed(flushed_docs)
        }
    }

    /// Packet-driven counterpart to
    /// [`Self::add_messages_with_conversation_id_and_batch_hook`].
    ///
    /// This is the entrypoint the ibuuh.32 migration uses to feed the
    /// lexical sink straight from a normalized [`ConversationPacket`].
    /// Callers that already hold a packet (e.g. the rebuild pipeline,
    /// or the in-tree convenience entrypoints `add_conversation` and
    /// `add_conversation_with_id`) avoid the second normalization pass
    /// the legacy `cass_doc_context` path performed against the raw
    /// `NormalizedConversation`.
    ///
    /// `message_indices` lets incremental callers project a subset of
    /// the packet's messages (e.g. only newly inserted indices) without
    /// rebuilding the packet — when `None`, every message is emitted.
    /// `conversation_id_override` lets callers stamp a canonical id
    /// without mutating the packet identity in place.
    pub fn add_messages_from_packet<F>(
        &mut self,
        packet: &ConversationPacket,
        message_indices: Option<&[usize]>,
        conversation_id_override: Option<i64>,
        mut on_batch_flushed: F,
    ) -> Result<()>
    where
        F: FnMut(usize) -> Result<()>,
    {
        let mut context = cass_doc_context_from_packet(packet);
        if let Some(id) = conversation_id_override {
            context.conversation_id = Some(id);
        }

        let max_messages = tantivy_add_batch_max_messages();
        let max_chars = tantivy_add_batch_max_chars();
        let mut docs: Vec<FsCassDocument> = Vec::new();
        let mut pending_chars = 0usize;

        let messages = &packet.payload.messages;
        let total = messages.len();
        if let Some(indices) = message_indices {
            for &i in indices {
                let Some(msg) = messages.get(i) else {
                    anyhow::bail!(
                        "packet message index {} out of range for packet with {} messages",
                        i,
                        total
                    );
                };
                push_packet_message_into_pending(
                    &mut self.inner,
                    &context,
                    msg,
                    &mut docs,
                    &mut pending_chars,
                    max_messages,
                    max_chars,
                    &mut on_batch_flushed,
                )?;
            }
        } else {
            for msg in messages {
                push_packet_message_into_pending(
                    &mut self.inner,
                    &context,
                    msg,
                    &mut docs,
                    &mut pending_chars,
                    max_messages,
                    max_chars,
                    &mut on_batch_flushed,
                )?;
            }
        }

        if docs.is_empty() {
            Ok(())
        } else {
            let flushed_docs = docs.len();
            self.inner.add_cass_documents(&docs)?;
            on_batch_flushed(flushed_docs)
        }
    }

    /// Reconcile selected canonical messages under their stable identities.
    /// A replacement filtered as noise removes its old live document. Each
    /// bounded engine batch publishes independently; errors propagate and a
    /// retry converges without promising whole-conversation atomicity.
    pub fn reconcile_messages_from_packet(
        &mut self,
        packet: &ConversationPacket,
        message_indices: &[usize],
        conversation_id_override: Option<i64>,
    ) -> Result<()> {
        self.reconcile_packet_messages_with_limits(
            packet,
            message_indices,
            conversation_id_override,
            tantivy_add_batch_max_messages(),
            tantivy_add_batch_max_chars(),
        )
    }

    pub(super) fn reconcile_packet_messages_with_limits(
        &mut self,
        packet: &ConversationPacket,
        message_indices: &[usize],
        conversation_id_override: Option<i64>,
        max_messages: usize,
        max_chars: usize,
    ) -> Result<()> {
        let messages = &packet.payload.messages;
        // Reject invalid selectors before publishing even the first batch.
        if let Some(index) = message_indices
            .iter()
            .find(|&&index| index >= messages.len())
        {
            anyhow::bail!(
                "packet message index {} out of range for packet with {} messages",
                index,
                messages.len()
            );
        }
        let mut context = cass_doc_context_from_packet(packet);
        if let Some(id) = conversation_id_override {
            context.conversation_id = Some(id);
        }
        let mut docs = Vec::new();
        let mut removals = Vec::new();
        let mut pending_indices = BTreeSet::new();
        let mut pending_bytes = 0usize;
        for &index in message_indices {
            let message = messages
                .get(index)
                .context("validated packet index disappeared")?;
            let msg_idx = message.idx.max(0) as u64;
            // Repeated selectors are valid replay, but one engine batch may
            // not contain a duplicate live identity.
            if pending_indices.contains(&msg_idx) {
                self.flush_packet_revisions(&mut docs, &mut removals)?;
                pending_indices.clear();
                pending_bytes = 0;
            }
            pending_indices.insert(msg_idx);
            if let Some(doc) = cass_document_for_packet_message(&context, message) {
                pending_bytes = pending_bytes.saturating_add(doc.content.len());
                docs.push(doc);
            } else {
                let identity = frankensearch::quill::cass::cass_document_identity(
                    &context.source_id,
                    frankensearch::quill::cass::CassConversationKey {
                        source_path: &context.source_path,
                        id: context.conversation_id,
                    },
                    msg_idx,
                );
                pending_bytes = pending_bytes.saturating_add(identity.len());
                removals.push(identity);
            }
            if pending_indices.len() >= max_messages.max(1) || pending_bytes >= max_chars.max(1) {
                self.flush_packet_revisions(&mut docs, &mut removals)?;
                pending_indices.clear();
                pending_bytes = 0;
            }
        }
        self.flush_packet_revisions(&mut docs, &mut removals)
    }

    fn flush_packet_revisions(
        &mut self,
        docs: &mut Vec<FsCassDocument>,
        removals: &mut Vec<String>,
    ) -> Result<()> {
        self.inner.upsert_cass_documents(docs)?;
        self.inner.delete_cass_document_ids(removals)?;
        docs.clear();
        removals.clear();
        Ok(())
    }

    /// Total live documents in the published snapshot.
    pub fn doc_count(&self) -> Result<u64> {
        self.inner.doc_count()
    }

    /// Task 5: stage one session's persisted search-region documents
    /// (thinking/tools/artifacts/subagents/meta scopes and noise-filtered
    /// chat regions) against `viewer_search_regions` on this uncommitted
    /// writer — deletes the queued stale identities, upserts the current
    /// region docs and queues the marks snapshot
    /// [`Self::commit_region_doc_stamps`] replays once the commit lands.
    /// The marks row stays untouched until then: a `published` stamp
    /// issued before the commit would certify documents a failed commit
    /// or crash never shipped. Called by the lexical ingest lanes that
    /// already own the writer. Returns documents upserted.
    pub fn stage_search_region_docs(
        &mut self,
        db: &impl crate::viewer_extensions::facts::FactsDb,
        conversation_id: i64,
    ) -> Result<usize> {
        // Idempotent within one writer generation: the per-session ingest
        // hook and the end-of-batch pending sweep both converge the same
        // session, and its marks still read `pending` until the post-commit
        // stamp. Re-staging here would re-upsert doc identities already
        // live in the uncommitted generation — a duplicate live id Quill
        // refuses — while the queued stage-time stamp snapshot still
        // certifies exactly the version that was staged. A marks row that
        // changed underneath the stage (republish/meta-edit) fails the
        // stamp's compare-and-swap and is re-converged by the next run.
        if self.staged_region_doc_sessions.contains(&conversation_id) {
            return Ok(0);
        }
        // Task 8: the first staged work on this writer captures the
        // coverage floor its eventual commit will certify.
        self.capture_publication_floor(db)?;
        let (docs, stamp) = crate::viewer_extensions::search_regions::stage_region_docs(
            &mut self.inner,
            db,
            conversation_id,
        )?;
        if let Some(stamp) = stamp {
            self.staged_region_doc_sessions.insert(conversation_id);
            self.pending_region_doc_stamps.push(stamp);
        }
        Ok(docs)
    }

    /// Incremental-lane staging: like [`Self::stage_search_region_docs`], but
    /// a session whose region documents are already committed in the live
    /// snapshot ([`region_docs_published_current`]) is skipped outright — no
    /// upsert, no queued stamp. Only lanes that write into the live index
    /// may use this; a lane that builds a fresh index must stage every
    /// session so the new snapshot carries the region documents.
    ///
    /// [`region_docs_published_current`]: crate::viewer_extensions::search_regions::region_docs_published_current
    pub fn stage_search_region_docs_unless_published(
        &mut self,
        db: &impl crate::viewer_extensions::facts::FactsDb,
        conversation_id: i64,
    ) -> Result<usize> {
        if crate::viewer_extensions::search_regions::region_docs_published_current(
            db,
            conversation_id,
        )? {
            return Ok(0);
        }
        self.stage_search_region_docs(db, conversation_id)
    }

    /// Test-only view of the queued region-doc stamps for the current
    /// uncommitted generation.
    #[cfg(test)]
    pub(crate) fn pending_region_doc_stamp_count(&self) -> usize {
        self.pending_region_doc_stamps.len()
    }

    /// Rescind a queued region-doc publication stamp for `conversation_id`
    /// in the current uncommitted generation. Used when the lexical lane
    /// skips a session's canonical staging after its region docs were
    /// staged (mid-batch OOM/deferred update): the docs may already be
    /// live via a peer session's replacement commit, but the persisted
    /// mark must stay `pending` so reads gate honestly and a later run
    /// re-converges the session.
    pub fn unstage_search_region_docs(&mut self, conversation_id: i64) {
        self.staged_region_doc_sessions.remove(&conversation_id);
        self.pending_region_doc_stamps
            .retain(|stamp| stamp.conversation_id != conversation_id);
    }

    /// Latch whole-archive republish deferral onto this writer: the
    /// incremental lane's per-batch region-doc prelude (the pending sweep
    /// and the per-session hooks) stages nothing until the lane's final
    /// drain — see the field docs for why per-batch convergence is
    /// quadratic during a REGION_VERSION republish. The lane owner calls
    /// this once, before the first persist batch, after counting the
    /// stale-version marks; nothing else may latch it.
    pub fn defer_region_doc_convergence_to_final_drain(&mut self) {
        self.region_doc_convergence_deferred = true;
    }

    /// Whether [`Self::defer_region_doc_convergence_to_final_drain`] was
    /// latched on this writer. The per-batch prelude consults this before
    /// staging; the final drain runs exactly when it is set.
    pub fn region_doc_convergence_deferred_to_final_drain(&self) -> bool {
        self.region_doc_convergence_deferred
    }

    /// Task 5: converge sessions whose region docs are still pending —
    /// ingest republication and annotation-edit meta replacement both mark
    /// `docs_state != 'published'`; the next index run reconciles them.
    /// Returns the number of sessions converged.
    ///
    /// A REGION_VERSION bump makes the incremental lane's ingest re-project
    /// every session of the archive, so this sweep re-stages them all at
    /// once onto a committed snapshot that already holds their (prior
    /// version's) documents. The upserts ride
    /// `Self::stage_region_doc_sessions_batched` — one bounded upsert per
    /// batch of sessions, not one per session: every upsert that replaces a
    /// live identity publishes a successor and validates every segment, so
    /// one upsert per session made the sweep quadratic in the session count
    /// (hours on a 6k-session archive; the same defect
    /// [`Self::converge_all_search_region_docs`] documents for the rebuild
    /// lane).
    pub fn converge_pending_search_region_docs(
        &mut self,
        db: &impl crate::viewer_extensions::facts::FactsDb,
        limit: i64,
    ) -> Result<usize> {
        // Task 8: capture the coverage floor before this pass touches any
        // pending marks — the eventual commit's certification binds at
        // most what was committed when this writer began covering work.
        self.capture_publication_floor(db)?;
        let pending =
            crate::viewer_extensions::search_regions::pending_region_doc_sessions(db, limit)?;
        self.stage_region_doc_sessions_batched(db, &pending)?;
        Ok(pending.len())
    }

    /// Stage the region documents of `sessions` in bounded batches — the
    /// ONE shared upsert/stamp loop of
    /// [`Self::converge_pending_search_region_docs`] and
    /// [`Self::converge_all_search_region_docs`].
    ///
    /// The documents of many sessions go to the writer in one bounded upsert
    /// (the prebuilt batch limits) because every upsert that replaces a live
    /// identity publishes a successor and validates every segment: one
    /// upsert per session made whole-archive passes quadratic in the
    /// session count. Each session's stamp queues only once its documents
    /// are upserted, and a session already staged in this writer generation
    /// is skipped — its stamp is already queued and a re-upsert would
    /// duplicate a live id. Returns the number of documents staged.
    fn stage_region_doc_sessions_batched(
        &mut self,
        db: &impl crate::viewer_extensions::facts::FactsDb,
        sessions: &[i64],
    ) -> Result<usize> {
        let max_docs = tantivy_prebuilt_add_batch_max_messages();
        let max_chars = tantivy_add_batch_max_chars();
        let mut batch: Vec<FsCassDocument> = Vec::new();
        let mut batch_stamps = Vec::new();
        let mut batch_chars = 0_usize;
        let mut docs = 0_usize;
        for (position, &conversation_id) in sessions.iter().enumerate() {
            // Same idempotence as `stage_search_region_docs`: a session
            // already staged in this writer generation is not re-upserted.
            if !self.staged_region_doc_sessions.contains(&conversation_id)
                && let Some((session_docs, stamp)) =
                    crate::viewer_extensions::search_regions::region_doc_stage(db, conversation_id)?
            {
                docs += session_docs.len();
                batch_chars = session_docs.iter().fold(batch_chars, |sum, doc| {
                    sum.saturating_add(doc.content.len())
                });
                batch.extend(session_docs);
                batch_stamps.push(stamp);
            }
            let last = position + 1 == sessions.len();
            if !batch_stamps.is_empty()
                && (last || batch.len() >= max_docs || batch_chars >= max_chars)
            {
                self.inner
                    .upsert_cass_documents(&batch)
                    .context("upserting search-region documents")?;
                // Stamps queue only once their documents are staged.
                for stamp in batch_stamps.drain(..) {
                    self.staged_region_doc_sessions
                        .insert(stamp.conversation_id);
                    self.pending_region_doc_stamps.push(stamp);
                }
                batch.clear();
                batch_chars = 0;
            }
        }
        Ok(docs)
    }

    /// Task 5: converge EVERY session carrying a marks row — the
    /// authoritative from-scratch rebuild owns no region documents, so
    /// `published` marks must be re-materialized alongside `pending` ones.
    /// Returns `(sessions_converged, documents_upserted)` so the rebuild's
    /// published-doc accounting binds exactly what ships.
    ///
    /// The documents of many sessions go to the writer in one bounded upsert
    /// (the prebuilt batch limits). A resumed terminal converges onto a
    /// candidate whose commit already holds these documents, so every upsert
    /// that replaces one publishes a successor and validates every segment;
    /// one upsert per session made that pass quadratic in the session count
    /// (hours on a 6,484-session archive, versus minutes from scratch).
    pub fn converge_all_search_region_docs(
        &mut self,
        db: &impl crate::viewer_extensions::facts::FactsDb,
        limit: i64,
    ) -> Result<(usize, usize)> {
        let sessions =
            crate::viewer_extensions::search_regions::marked_region_doc_sessions(db, limit)?;
        if sessions.is_empty() {
            return Ok((0, 0));
        }
        // Task 8: the first staged work on this writer captures the
        // coverage floor its eventual commit will certify.
        self.capture_publication_floor(db)?;
        let docs = self.stage_region_doc_sessions_batched(db, &sessions)?;
        Ok((sessions.len(), docs))
    }

    /// Build every lexical document for one packet without writing anything.
    ///
    /// The projection and noise filter are identical to
    /// [`Self::add_messages_from_packet`]; callers that need identity-stable
    /// writes (qhiv2 targeted reconcile) feed the result to
    /// [`Self::upsert_prebuilt_documents_slice`].
    #[must_use]
    pub fn build_packet_documents(
        packet: &ConversationPacket,
        conversation_id_override: Option<i64>,
    ) -> Vec<FsCassDocument> {
        let mut context = cass_doc_context_from_packet(packet);
        if let Some(id) = conversation_id_override {
            context.conversation_id = Some(id);
        }
        packet
            .payload
            .messages
            .iter()
            .filter_map(|msg| cass_document_for_packet_message(&context, msg))
            .collect()
    }

    /// Upsert prebuilt documents in bounded batches under their stable
    /// identities (see `QuillCassIndex::upsert_cass_documents`); a retry of
    /// the same set converges to exactly one live document per identity
    /// instead of appending duplicates.
    pub fn upsert_prebuilt_documents_slice(
        &mut self,
        documents: &[FsCassDocument],
    ) -> Result<usize> {
        let max_messages = tantivy_prebuilt_add_batch_max_messages();
        let max_chars = tantivy_add_batch_max_chars();
        let mut upserted_docs = 0usize;
        let mut batch_start = 0usize;
        let mut pending_chars = 0usize;

        for (idx, doc) in documents.iter().enumerate() {
            pending_chars = pending_chars.saturating_add(doc.content.len());
            let batch_len = idx + 1 - batch_start;
            if batch_len >= max_messages || pending_chars >= max_chars {
                let batch_end = idx + 1;
                upserted_docs = upserted_docs.saturating_add(batch_end - batch_start);
                let Some(batch) = documents.get(batch_start..batch_end) else {
                    anyhow::bail!(
                        "invalid Tantivy upsert document batch range {}..{} for {} documents",
                        batch_start,
                        batch_end,
                        documents.len()
                    );
                };
                self.inner.upsert_cass_documents(batch)?;
                batch_start = batch_end;
                pending_chars = 0;
            }
        }

        if batch_start < documents.len() {
            upserted_docs = upserted_docs.saturating_add(documents.len() - batch_start);
            let Some(batch) = documents.get(batch_start..) else {
                anyhow::bail!(
                    "invalid Tantivy upsert document tail range {}.. for {} documents",
                    batch_start,
                    documents.len()
                );
            };
            self.inner.upsert_cass_documents(batch)?;
        }
        Ok(upserted_docs)
    }

    pub fn add_prebuilt_documents_slice(&mut self, documents: &[FsCassDocument]) -> Result<usize> {
        let max_messages = tantivy_prebuilt_add_batch_max_messages();
        let max_chars = tantivy_add_batch_max_chars();
        let mut indexed_docs = 0usize;
        let mut batch_start = 0usize;
        let mut pending_chars = 0usize;

        for (idx, doc) in documents.iter().enumerate() {
            pending_chars = pending_chars.saturating_add(doc.content.len());
            let batch_len = idx + 1 - batch_start;
            if batch_len >= max_messages || pending_chars >= max_chars {
                let batch_end = idx + 1;
                indexed_docs = indexed_docs.saturating_add(batch_end - batch_start);
                let Some(batch) = documents.get(batch_start..batch_end) else {
                    anyhow::bail!(
                        "invalid Tantivy prebuilt document batch range {}..{} for {} documents",
                        batch_start,
                        batch_end,
                        documents.len()
                    );
                };
                self.inner.add_cass_documents(batch)?;
                batch_start = batch_end;
                pending_chars = 0;
            }
        }

        if batch_start < documents.len() {
            indexed_docs = indexed_docs.saturating_add(documents.len() - batch_start);
            let Some(batch) = documents.get(batch_start..) else {
                anyhow::bail!(
                    "invalid Tantivy prebuilt document tail range {}.. for {} documents",
                    batch_start,
                    documents.len()
                );
            };
            self.inner.add_cass_documents(batch)?;
        }

        Ok(indexed_docs)
    }

    /// #440: arm the identity-idempotent resume path. The next `docs`
    /// documents handed to [`Self::add_prebuilt_document_refs_slice`] are
    /// upserted instead of added, so a resumed rebuild whose published
    /// authority already holds them converges to exactly one live document
    /// per identity instead of being refused as a duplicate.
    pub fn arm_resume_reconcile(&mut self, docs: usize) {
        self.resume_reconcile_docs_remaining = docs;
    }

    /// Documents still owed to the upsert path by [`Self::arm_resume_reconcile`].
    #[must_use]
    pub fn resume_reconcile_docs_remaining(&self) -> usize {
        self.resume_reconcile_docs_remaining
    }

    /// Upsert prebuilt borrowed documents in bounded batches under their
    /// stable identities (see [`Self::upsert_prebuilt_documents_slice`]).
    pub fn upsert_prebuilt_document_refs_slice<'a>(
        &mut self,
        documents: &[FsCassDocumentRef<'a>],
    ) -> Result<usize> {
        let max_messages = tantivy_prebuilt_add_batch_max_messages();
        let max_chars = tantivy_add_batch_max_chars();
        let mut upserted_docs = 0usize;
        let mut batch_start = 0usize;
        let mut pending_chars = 0usize;

        for (idx, doc) in documents.iter().enumerate() {
            pending_chars = pending_chars.saturating_add(doc.content.len());
            let batch_len = idx + 1 - batch_start;
            if batch_len >= max_messages || pending_chars >= max_chars {
                let batch_end = idx + 1;
                upserted_docs = upserted_docs.saturating_add(batch_end - batch_start);
                let Some(batch) = documents.get(batch_start..batch_end) else {
                    anyhow::bail!(
                        "invalid Tantivy upsert document ref batch range {}..{} for {} documents",
                        batch_start,
                        batch_end,
                        documents.len()
                    );
                };
                self.inner.upsert_cass_document_refs(batch)?;
                batch_start = batch_end;
                pending_chars = 0;
            }
        }

        if batch_start < documents.len() {
            upserted_docs = upserted_docs.saturating_add(documents.len() - batch_start);
            let Some(batch) = documents.get(batch_start..) else {
                anyhow::bail!(
                    "invalid Tantivy upsert document ref tail range {}.. for {} documents",
                    batch_start,
                    documents.len()
                );
            };
            self.inner.upsert_cass_document_refs(batch)?;
        }
        Ok(upserted_docs)
    }

    pub fn add_prebuilt_document_refs_slice<'a>(
        &mut self,
        documents: &[FsCassDocumentRef<'a>],
    ) -> Result<usize> {
        // #440: a resumed rebuild owes its leading documents to the upsert
        // path (see `arm_resume_reconcile`). Split the slice at the owed
        // boundary so the reconcile window is exact rather than "this whole
        // batch", then fall through to the ordinary add for the remainder.
        if self.resume_reconcile_docs_remaining > 0 {
            let owed = self.resume_reconcile_docs_remaining.min(documents.len());
            let (reconcile, rest) = documents.split_at(owed);
            let upserted = self.upsert_prebuilt_document_refs_slice(reconcile)?;
            self.resume_reconcile_docs_remaining -= owed;
            if rest.is_empty() {
                return Ok(upserted);
            }
            return Ok(upserted.saturating_add(self.add_prebuilt_document_refs_slice(rest)?));
        }
        let max_messages = tantivy_prebuilt_add_batch_max_messages();
        let max_chars = tantivy_add_batch_max_chars();
        let mut indexed_docs = 0usize;
        let mut batch_start = 0usize;
        let mut pending_chars = 0usize;

        for (idx, doc) in documents.iter().enumerate() {
            pending_chars = pending_chars.saturating_add(doc.content.len());
            let batch_len = idx + 1 - batch_start;
            if batch_len >= max_messages || pending_chars >= max_chars {
                let batch_end = idx + 1;
                indexed_docs = indexed_docs.saturating_add(batch_end - batch_start);
                let Some(batch) = documents.get(batch_start..batch_end) else {
                    anyhow::bail!(
                        "invalid Tantivy prebuilt document ref batch range {}..{} for {} documents",
                        batch_start,
                        batch_end,
                        documents.len()
                    );
                };
                self.inner.add_cass_document_refs(batch)?;
                batch_start = batch_end;
                pending_chars = 0;
            }
        }

        if batch_start < documents.len() {
            indexed_docs = indexed_docs.saturating_add(documents.len() - batch_start);
            let Some(batch) = documents.get(batch_start..) else {
                anyhow::bail!(
                    "invalid Tantivy prebuilt document ref tail range {}.. for {} documents",
                    batch_start,
                    documents.len()
                );
            };
            self.inner.add_cass_document_refs(batch)?;
        }

        Ok(indexed_docs)
    }

    pub fn add_prebuilt_documents<I>(&mut self, documents: I) -> Result<usize>
    where
        I: IntoIterator<Item = FsCassDocument>,
    {
        let max_messages = tantivy_prebuilt_add_batch_max_messages();
        let max_chars = tantivy_add_batch_max_chars();
        let mut docs = Vec::new();
        let mut pending_chars = 0usize;
        let mut indexed_docs = 0usize;

        for doc in documents {
            pending_chars = pending_chars.saturating_add(doc.content.len());
            docs.push(doc);
            if docs.len() >= max_messages || pending_chars >= max_chars {
                indexed_docs = indexed_docs.saturating_add(docs.len());
                self.inner.add_cass_documents(&docs)?;
                docs.clear();
                pending_chars = 0;
            }
        }

        if !docs.is_empty() {
            indexed_docs = indexed_docs.saturating_add(docs.len());
            self.inner.add_cass_documents(&docs)?;
        }

        Ok(indexed_docs)
    }

    pub fn add_conversations_with_ids<'a, I>(&mut self, conversations: I) -> Result<usize>
    where
        I: IntoIterator<Item = (&'a NormalizedConversation, Option<i64>)>,
    {
        let max_messages = tantivy_add_batch_max_messages();
        let max_chars = tantivy_add_batch_max_chars();
        let mut docs: Vec<FsCassDocument> = Vec::new();
        let mut pending_chars = 0usize;
        let mut indexed_docs = 0usize;

        for (conv, conversation_id) in conversations {
            let context = cass_doc_context(conv, conversation_id);
            for msg in &conv.messages {
                let Some(doc) = cass_document_for_message(&context, msg) else {
                    continue;
                };
                push_cass_document_into_pending(&mut docs, &mut pending_chars, doc);
                if docs.len() >= max_messages || pending_chars >= max_chars {
                    indexed_docs = indexed_docs.saturating_add(docs.len());
                    self.inner.add_cass_documents(&docs)?;
                    docs.clear();
                    pending_chars = 0;
                }
            }
        }

        if !docs.is_empty() {
            indexed_docs = indexed_docs.saturating_add(docs.len());
            self.inner.add_cass_documents(&docs)?;
        }

        Ok(indexed_docs)
    }
}
