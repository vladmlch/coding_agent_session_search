use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};

use super::config::{
    CancelCheck, DEFAULT_SEMANTIC_MAX_BYTES_PER_CHECKPOINT,
    DEFAULT_SEMANTIC_MAX_MESSAGES_PER_CHECKPOINT,
    DEFAULT_SEMANTIC_RECONCILIATION_SCAN_CONVERSATIONS, resolved_env_u64, resolved_env_usize,
    saturating_u64_from_usize,
};
use super::types::EmbeddingInput;
use crate::franken_sync::{
    SqliteValue,
    compat::{ConnectionExt, ParamValue, RowExt},
};
use crate::indexer::semantic_progress::{
    SemanticProgressEvent, SemanticProgressFields, SemanticProgressSink,
};
use crate::model::conversation_packet::{ConversationPacket, ConversationPacketProvenance};
use crate::model::types::{Conversation, Message};
use crate::search::canonicalize::embedding_passages;
use crate::search::semantic_manifest::{SemanticManifest, TierKind};
use crate::search::tantivy::{
    normalized_index_origin_host, normalized_index_origin_kind, normalized_index_source_id,
};
use crate::search::vector_index::{ROLE_USER, role_code_from_str};
use crate::storage::sqlite::FrankenStorage;

pub(in crate::indexer::semantic::engine) struct CanonicalEmbeddingConversationRow {
    pub(in crate::indexer::semantic::engine) conversation_id: i64,
    pub(in crate::indexer::semantic::engine) agent_slug: String,
    pub(in crate::indexer::semantic::engine) agent_id: i64,
    pub(in crate::indexer::semantic::engine) workspace: Option<PathBuf>,
    pub(in crate::indexer::semantic::engine) workspace_id: Option<i64>,
    pub(in crate::indexer::semantic::engine) external_id: Option<String>,
    pub(in crate::indexer::semantic::engine) title: Option<String>,
    pub(in crate::indexer::semantic::engine) source_path: PathBuf,
    pub(in crate::indexer::semantic::engine) started_at: Option<i64>,
    pub(in crate::indexer::semantic::engine) ended_at: Option<i64>,
    pub(in crate::indexer::semantic::engine) source_id: Option<String>,
    pub(in crate::indexer::semantic::engine) origin_host: Option<String>,
}

pub(in crate::indexer::semantic::engine) struct CanonicalEmbeddingBatch {
    pub(in crate::indexer::semantic::engine) inputs: Vec<EmbeddingInput>,
    pub(in crate::indexer::semantic::engine) conversations_in_batch: u64,
    pub(in crate::indexer::semantic::engine) last_conversation_id: i64,
    pub(in crate::indexer::semantic::engine) total_conversations: u64,
    pub(in crate::indexer::semantic::engine) cursor_exhausted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::indexer::semantic::engine) struct SemanticCheckpointCaps {
    pub(in crate::indexer::semantic::engine) max_messages: usize,
    pub(in crate::indexer::semantic::engine) max_bytes: u64,
}

impl SemanticCheckpointCaps {
    pub(in crate::indexer::semantic::engine) fn unlimited() -> Self {
        Self {
            max_messages: 0,
            max_bytes: 0,
        }
    }

    pub(in crate::indexer::semantic::engine) fn from_env() -> Self {
        Self {
            max_messages: resolved_env_usize(
                "CASS_SEMANTIC_MAX_MESSAGES_PER_CHECKPOINT",
                DEFAULT_SEMANTIC_MAX_MESSAGES_PER_CHECKPOINT,
            ),
            max_bytes: resolved_env_u64(
                "CASS_SEMANTIC_MAX_BYTES_PER_CHECKPOINT",
                DEFAULT_SEMANTIC_MAX_BYTES_PER_CHECKPOINT,
            ),
        }
    }

    pub(in crate::indexer::semantic::engine) fn message_limited(self) -> bool {
        self.max_messages > 0
    }

    pub(in crate::indexer::semantic::engine) fn byte_limited(self) -> bool {
        self.max_bytes > 0
    }
}

pub struct CanonicalIncrementalEmbeddingBatch {
    pub inputs: Vec<EmbeddingInput>,
    pub conversations_in_batch: u64,
    pub raw_max_message_id: Option<i64>,
}

fn semantic_hot_tail_ids(storage: &FrankenStorage) -> Result<HashSet<i64>> {
    storage
        .raw()
        .query_map_collect(
            "SELECT conversation_id
             FROM conversation_tail_state
             WHERE last_message_idx IS NOT NULL
             ORDER BY conversation_id ASC",
            &[] as &[ParamValue],
            |row| row.get_typed(0),
        )
        .with_context(|| "listing semantic conversations from hot tail metadata")
        .map(|ids: Vec<i64>| ids.into_iter().collect())
}

/// #383 fast path: answer "does this conversation have a message past the
/// global cursor" from the conversation's tail instead of walking its history.
///
/// The range probe `conversation_id = ? AND id > ?` degrades to a full
/// per-conversation history walk under the `(conversation_id, idx)` autoindex,
/// which on multi-thousand-message conversations costs minutes per resume
/// checkpoint. The tail message is sufficient: `conversation_tail_state` is
/// written in the same transaction as the message rows it describes, and both
/// the append and rewrite ingest paths assign fresh, ascending rowids, so the
/// message at `last_message_idx` carries the conversation's maximum message
/// id. If the tail row or its message is missing (legacy/empty conversation,
/// interrupted migration), return `None` so the caller falls back to the
/// exhaustive probe.
pub(in crate::indexer::semantic::engine) fn semantic_tail_state_has_message_after(
    storage: &FrankenStorage,
    conversation_id: i64,
    after_message_id: i64,
) -> Result<Option<bool>> {
    let mut tail_idx: Option<i64> = None;
    storage
        .raw()
        .query_with_params_for_each(
            "SELECT last_message_idx FROM conversation_tail_state WHERE conversation_id = ?1",
            &[SqliteValue::from(conversation_id)],
            |row| {
                tail_idx = row.get_typed(0)?;
                Ok(())
            },
        )
        .with_context(|| {
            format!("reading conversation_tail_state for conversation {conversation_id}")
        })?;
    let Some(tail_idx) = tail_idx else {
        return Ok(None);
    };
    let mut tail_message_id: Option<i64> = None;
    storage
        .raw()
        .query_with_params_for_each(
            "SELECT id FROM messages WHERE conversation_id = ?1 AND idx = ?2",
            &[
                SqliteValue::from(conversation_id),
                SqliteValue::from(tail_idx),
            ],
            |row| {
                tail_message_id = Some(row.get_typed(0)?);
                Ok(())
            },
        )
        .with_context(|| {
            format!("reading tail message id for conversation {conversation_id} at idx {tail_idx}")
        })?;
    Ok(tail_message_id.map(|tail_id| tail_id > after_message_id))
}

pub(in crate::indexer::semantic::engine) fn semantic_conversation_has_message_after(
    storage: &FrankenStorage,
    conversation_id: i64,
    after_message_id: Option<i64>,
) -> Result<bool> {
    if let Some(cursor) = after_message_id
        && let Some(verdict) =
            semantic_tail_state_has_message_after(storage, conversation_id, cursor)?
    {
        return Ok(verdict);
    }
    let mut params = vec![SqliteValue::from(conversation_id)];
    let message_cursor_predicate = if let Some(after_message_id) = after_message_id {
        params.push(SqliteValue::from(after_message_id));
        " AND id > ?2"
    } else {
        ""
    };
    let hinted_probe = format!(
        "SELECT 1
         FROM messages INDEXED BY idx_messages_conv_idx
         WHERE conversation_id = ?1{message_cursor_predicate}
         LIMIT 1"
    );
    let fallback_probe = format!(
        "SELECT 1
         FROM messages
         WHERE conversation_id = ?1{message_cursor_predicate}
         LIMIT 1"
    );
    let mut has_message = false;
    storage
        .raw()
        .query_with_params_for_each(&hinted_probe, &params, |_| {
            has_message = true;
            Ok(())
        })
        .or_else(|err| {
            if !err
                .to_string()
                .contains("no such index: idx_messages_conv_idx")
            {
                return Err(err);
            }
            storage
                .raw()
                .query_with_params_for_each(&fallback_probe, &params, |_| {
                    has_message = true;
                    Ok(())
                })
        })
        .with_context(|| {
            let cursor = after_message_id.map_or_else(|| "none".to_string(), |id| id.to_string());
            format!(
                "probing messages for conversation {conversation_id} after message cursor {cursor}"
            )
        })?;
    Ok(has_message)
}

pub(in crate::indexer::semantic::engine) fn total_semantic_conversations(
    storage: &FrankenStorage,
) -> Result<u64> {
    total_semantic_conversations_with_cancel(storage, None)
}

fn check_semantic_scan_cancelled(cancelled: Option<&CancelCheck>) -> Result<()> {
    if cancelled.is_some_and(|check| check()) {
        bail!(
            "semantic_backfill_cancelled: canonical scan interrupted; resume the last durable checkpoint"
        );
    }
    Ok(())
}

pub(in crate::indexer::semantic::engine) fn total_semantic_conversations_with_cancel(
    storage: &FrankenStorage,
    cancelled: Option<&CancelCheck>,
) -> Result<u64> {
    // `conversation_tail_state.last_message_idx` is the maintained, schema-v18
    // hot cache for every normal non-empty conversation. Merge that compact
    // table with the legacy parent-row cache in Rust, then resolve only the
    // residual legacy/empty rows through point probes on the canonical
    // `(conversation_id, idx)` autoindex. Keeping the conversation id set in
    // memory is cheap (roughly one integer per conversation) and, critically,
    // never materializes message rows or bodies.
    //
    // This avoids all three hostile FrankenSQLite 0.1.13 paths found while
    // reproducing #343: correlated EXISTS under COUNT(*) misses the join-loop
    // memo; COUNT(DISTINCT ...) uses a linear seen-set; and GROUP BY retains
    // every source row in per-group vectors.
    check_semantic_scan_cancelled(cancelled)?;
    let hot_tail_ids = if cancelled.is_none() {
        semantic_hot_tail_ids(storage)?
    } else {
        let mut ids = HashSet::new();
        storage.raw().query_with_params_for_each(
            "SELECT conversation_id FROM conversation_tail_state WHERE last_message_idx IS NOT NULL ORDER BY conversation_id ASC",
            &[] as &[SqliteValue],
            |row| {
                check_semantic_scan_cancelled(cancelled)
                    .map_err(|error| crate::franken_sync::FrankenError::Internal(error.to_string()))?;
                ids.insert(row.get_typed::<i64>(0)?);
                Ok(())
            },
        )?;
        ids
    };
    let mut count = 0_u64;
    let mut missing_tail_ids = Vec::new();
    storage
        .raw()
        .query_with_params_for_each(
            "SELECT id, last_message_idx
             FROM conversations
             ORDER BY id ASC",
            &[] as &[SqliteValue],
            |row| {
                check_semantic_scan_cancelled(cancelled).map_err(|error| {
                    crate::franken_sync::FrankenError::Internal(error.to_string())
                })?;
                let conversation_id: i64 = row.get_typed(0)?;
                let legacy_last_message_idx: Option<i64> = row.get_typed(1)?;
                if legacy_last_message_idx.is_some() || hot_tail_ids.contains(&conversation_id) {
                    count = count.saturating_add(1);
                } else {
                    missing_tail_ids.push(conversation_id);
                }
                Ok(())
            },
        )
        .with_context(|| "listing conversations and legacy tail metadata")?;

    for conversation_id in missing_tail_ids {
        check_semantic_scan_cancelled(cancelled)?;
        if semantic_conversation_has_message_after(storage, conversation_id, None)? {
            count = count.saturating_add(1);
        }
    }
    Ok(count)
}

pub(in crate::indexer::semantic::engine) fn fetch_bounded_semantic_candidate_conversation_ids(
    storage: &FrankenStorage,
    after_conversation_id: i64,
    after_message_id: Option<i64>,
    max_candidates: usize,
) -> Result<(Vec<i64>, usize)> {
    let max_candidates = max_candidates.max(1);
    if let Some(after_message_id) = after_message_id {
        // Resume from the canonical global message cursor, not from the much
        // wider parent-table gap.  The previous implementation paged
        // `conversations` and executed one prepared message probe for every
        // parent.  A legitimate sparse checkpoint in #348 crossed 6,668
        // ineligible parents and retained more than 2 GiB before finding two
        // candidates.
        //
        // `messages.id` is the canonical INTEGER PRIMARY KEY cursor.  Force a
        // table/rowid traversal so FrankenSQLite can stream every eligible
        // post-cursor message through one statement.  A bounded BTreeSet keeps
        // only the smallest `max_candidates` conversation IDs.  This final
        // reconciliation is essential: messages may be appended to older
        // conversations, so global message-id order is not necessarily
        // conversation-id order, while the durable conversation checkpoint
        // must advance in ascending order without skipping an older candidate.
        //
        // The WHERE must stay the bare rowid range, with the conversation
        // bound applied in the row callback: FrankenSQLite seeks a rowid range
        // only when nothing else is in the WHERE. `AND conversation_id > ?2`
        // turned every batch into a full `messages` scan (on a 496,736-message
        // archive: about 1 s warm and 7 s cold per batch, versus 0.5 ms).
        let mut selected = BTreeSet::new();
        let mut rows_scanned = 0_usize;
        storage
            .raw()
            .query_with_params_for_each(
                "SELECT conversation_id
                 FROM messages NOT INDEXED
                 WHERE id > ?1",
                &[SqliteValue::from(after_message_id)],
                |row| {
                    let conversation_id: i64 = row.get_typed(0)?;
                    if conversation_id <= after_conversation_id {
                        return Ok(());
                    }
                    rows_scanned = rows_scanned.saturating_add(1);
                    if selected.contains(&conversation_id) {
                        return Ok(());
                    }
                    if selected.len() < max_candidates {
                        selected.insert(conversation_id);
                        return Ok(());
                    }
                    if selected
                        .last()
                        .is_some_and(|largest| conversation_id < *largest)
                    {
                        selected.insert(conversation_id);
                        if let Some(largest) = selected.last().copied() {
                            selected.remove(&largest);
                        }
                    }
                    Ok(())
                },
            )
            .with_context(|| {
                format!(
                    "streaming semantic candidates after conversation {after_conversation_id} and message {after_message_id}"
                )
            })?;
        return Ok((selected.into_iter().collect(), rows_scanned));
    }

    // Scan the narrow parent table in bounded pages, then issue an indexed
    // point probe for the message predicate. A correlated EXISTS looks tidy,
    // but FrankenSQLite 0.1.13's file-backed path clone/substitutes and executes
    // that subquery per outer row; on #343's 432k-message corpus it consumed
    // minutes and GiBs. Explicit probes make both bounds visible: at most one
    // small parent page resident and at most `max_candidates` selected IDs.
    let page_size = max_candidates.saturating_mul(4).clamp(256, 4_096);
    let page_size_i64 = i64::try_from(page_size).unwrap_or(i64::MAX);
    let hot_tail_ids = if after_message_id.is_none() {
        Some(semantic_hot_tail_ids(storage)?)
    } else {
        None
    };
    let mut selected = Vec::with_capacity(max_candidates.min(4_096));
    let mut rows_scanned = 0_usize;
    let mut page_cursor = after_conversation_id;

    loop {
        let page: Vec<(i64, Option<i64>)> = storage
            .raw()
            .query_map_collect(
                "SELECT id, last_message_idx
                 FROM conversations
                 WHERE id > ?1
                 ORDER BY id ASC
                 LIMIT ?2",
                &[
                    ParamValue::from(page_cursor),
                    ParamValue::from(page_size_i64),
                ],
                |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
            )
            .with_context(|| {
                format!("listing bounded semantic candidate page after conversation {page_cursor}")
            })?;
        if page.is_empty() {
            break;
        }
        let page_len = page.len();
        page_cursor = page
            .last()
            .map_or(page_cursor, |(conversation_id, _)| *conversation_id);

        for (conversation_id, legacy_last_message_idx) in page {
            rows_scanned = rows_scanned.saturating_add(1);
            let has_message = if let Some(hot_tail_ids) = hot_tail_ids.as_ref() {
                if legacy_last_message_idx.is_some() || hot_tail_ids.contains(&conversation_id) {
                    true
                } else {
                    semantic_conversation_has_message_after(storage, conversation_id, None)?
                }
            } else {
                semantic_conversation_has_message_after(storage, conversation_id, after_message_id)?
            };
            if has_message {
                selected.push(conversation_id);
                if selected.len() >= max_candidates {
                    return Ok((selected, rows_scanned));
                }
            }
        }

        if page_len < page_size {
            break;
        }
    }

    Ok((selected, rows_scanned))
}

pub(in crate::indexer::semantic::engine) fn cached_semantic_total_conversations(
    manifest: &SemanticManifest,
    tier: TierKind,
    embedder_id: &str,
    db_fingerprint: &str,
) -> Option<(u64, &'static str)> {
    manifest
        .checkpoint
        .as_ref()
        .filter(|checkpoint| {
            checkpoint.tier == tier
                && checkpoint.embedder_id == embedder_id
                && checkpoint.is_valid(db_fingerprint)
        })
        .map(|checkpoint| (checkpoint.total_conversations, "checkpoint"))
}

pub fn message_id_from_db(raw: i64) -> Option<u64> {
    u64::try_from(raw).ok()
}

pub fn saturating_u32_from_i64(raw: i64) -> u32 {
    match u32::try_from(raw) {
        Ok(value) => value,
        Err(_) if raw.is_negative() => 0,
        Err(_) => u32::MAX,
    }
}

fn canonical_embedding_created_at_ms(message_id: u64, created_at: Option<i64>) -> i64 {
    // `created_at_ms` feeds time-range filters in the vector index
    // (src/search/vector_index.rs range predicates) and contributes to
    // `stable_hit_hash`. Defaulting a NULL created_at to 0 silently
    // masquerades the message as Unix-epoch (1970), which is indistinguishable
    // from a legitimately-ancient row in downstream filters. Emit a warn
    // so operators see NULL-created_at rows in the logs instead of only
    // finding them by puzzling over 1970 timestamps in semantic hits.
    created_at.unwrap_or_else(|| {
        tracing::warn!(
            message_id,
            "semantic backfill: row has NULL created_at; defaulting to 0 (Unix epoch). \
             Downstream time-range filters will treat this message as 1970-01-01."
        );
        0
    })
}

pub(in crate::indexer::semantic::engine) fn canonical_embedding_packet_provenance(
    row: &CanonicalEmbeddingConversationRow,
) -> ConversationPacketProvenance {
    let source_id =
        normalized_index_source_id(row.source_id.as_deref(), None, row.origin_host.as_deref());
    ConversationPacketProvenance {
        origin_kind: normalized_index_origin_kind(&source_id, None),
        origin_host: normalized_index_origin_host(row.origin_host.as_deref()),
        source_id,
    }
}

pub(in crate::indexer::semantic::engine) fn canonical_embedding_conversation(
    row: &CanonicalEmbeddingConversationRow,
    provenance: &ConversationPacketProvenance,
    messages: Vec<Message>,
) -> Conversation {
    Conversation {
        id: Some(row.conversation_id),
        agent_slug: row.agent_slug.clone(),
        workspace: row.workspace.clone(),
        external_id: row.external_id.clone(),
        title: row.title.clone(),
        source_path: row.source_path.clone(),
        started_at: row.started_at,
        ended_at: row.ended_at,
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages,
        source_id: provenance.source_id.clone(),
        origin_host: provenance.origin_host.clone(),
    }
}

fn embedding_inputs_from_packet_message(
    conversation_id: i64,
    agent_id: u32,
    workspace_id: u32,
    source_id_hash: u32,
    message: &crate::model::conversation_packet::ConversationPacketMessage,
) -> Vec<EmbeddingInput> {
    let Some(raw_message_id) = message.message_id else {
        tracing::warn!(
            conversation_id,
            message_idx = message.idx,
            "skipping semantic backfill message without canonical id in ConversationPacket replay"
        );
        return Vec::new();
    };
    let Some(message_id) = message_id_from_db(raw_message_id) else {
        tracing::warn!(
            conversation_id,
            raw_message_id,
            "skipping out-of-range id during semantic backfill"
        );
        return Vec::new();
    };
    let created_at_ms = canonical_embedding_created_at_ms(message_id, message.created_at);
    embedding_passages(&message.content)
        .into_iter()
        .enumerate()
        .map(|(ordinal, content)| EmbeddingInput {
            message_id,
            created_at_ms,
            agent_id,
            workspace_id,
            source_id: source_id_hash,
            role: role_code_from_str(&message.role).unwrap_or(ROLE_USER),
            chunk_idx: ordinal as u8,
            content: content.to_owned(),
        })
        .collect()
}

fn embedding_inputs_from_conversation_packet(
    row: &CanonicalEmbeddingConversationRow,
    packet: &ConversationPacket,
) -> Vec<EmbeddingInput> {
    let agent_id = saturating_u32_from_i64(row.agent_id);
    let workspace_id = saturating_u32_from_i64(row.workspace_id.unwrap_or(0));
    let source_id_hash = crc32fast::hash(packet.payload.provenance.source_id.as_bytes());
    packet
        .projections
        .semantic
        .message_indices
        .iter()
        .flat_map(|message_index| {
            packet
                .payload
                .messages
                .get(*message_index)
                .map(|message| {
                    embedding_inputs_from_packet_message(
                        row.conversation_id,
                        agent_id,
                        workspace_id,
                        source_id_hash,
                        message,
                    )
                })
                .unwrap_or_default()
        })
        .collect()
}

pub(in crate::indexer::semantic::engine) fn fetch_canonical_embedding_conversations(
    storage: &FrankenStorage,
    conversation_ids: &[i64],
) -> Result<Vec<CanonicalEmbeddingConversationRow>> {
    let mut envelope_sql = String::from(
        "SELECT c.id,
                COALESCE(a.slug, 'unknown'),
                COALESCE(c.agent_id, 0),
                c.workspace_id,
                w.path,
                c.external_id,
                c.title,
                c.source_path,
                c.started_at,
                c.ended_at,
                c.source_id,
                c.origin_host
         FROM conversations c
         LEFT JOIN agents a ON a.id = c.agent_id
         LEFT JOIN workspaces w ON w.id = c.workspace_id
         WHERE c.id IN (",
    );
    let mut params = Vec::with_capacity(conversation_ids.len());
    for (idx, conversation_id) in conversation_ids.iter().enumerate() {
        if idx > 0 {
            envelope_sql.push_str(", ");
        }
        envelope_sql.push_str(&format!("?{}", idx + 1));
        params.push(ParamValue::from(*conversation_id));
    }
    envelope_sql.push_str(") ORDER BY c.id ASC");

    storage
        .raw()
        .query_map_collect(&envelope_sql, &params, |row| {
            let workspace_path: Option<String> = row.get_typed(4)?;
            Ok(CanonicalEmbeddingConversationRow {
                conversation_id: row.get_typed(0)?,
                agent_slug: row.get_typed(1)?,
                agent_id: row.get_typed(2)?,
                workspace_id: row.get_typed(3)?,
                workspace: workspace_path.map(PathBuf::from),
                external_id: row.get_typed(5)?,
                title: row.get_typed(6)?,
                source_path: PathBuf::from(row.get_typed::<String>(7)?),
                started_at: row.get_typed(8)?,
                ended_at: row.get_typed(9)?,
                source_id: row.get_typed(10)?,
                origin_host: row.get_typed(11)?,
            })
        })
        .with_context(|| {
            format!(
                "fetching semantic backfill conversation envelopes for {} conversations",
                conversation_ids.len()
            )
        })
}

/// Per-packet semantic context that supplies the database-internal
/// agent / workspace ids the canonical embedding row carries but the
/// `ConversationPacket` does not (those ids are storage-internal,
/// not part of the packet contract).
///
/// `coding_agent_session_search-ibuuh.32` (sink #3): when a caller
/// already holds packets (rebuild pipeline, salvage replay, repair
/// flows, etc.) it can pair them with their canonical
/// agent_id/workspace_id and drive the semantic preparation consumer
/// without a second storage round-trip.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub struct SemanticPacketContext {
    pub conversation_id: i64,
    pub agent_id: u32,
    pub workspace_id: u32,
}

/// Packet-driven counterpart to
/// [`packet_embedding_inputs_from_storage_with_progress`]: derives the same
/// `EmbeddingInput` list a fresh storage replay would produce, but
/// without re-querying canonical conversation rows.
///
/// Invariants:
/// - The `i`th element of `contexts` describes the `i`th packet.
/// - Returns `Err` if the lengths disagree, so a callsite cannot
///   silently mis-correlate ids.
/// - `source_id_hash` is derived from `packet.payload.provenance.source_id`
///   the same way `embedding_inputs_from_conversation_packet` derives
///   it from the canonical row, so the produced `EmbeddingInput.source_id`
///   matches both paths byte-for-byte.
///
/// The `semantic_inputs_from_packets_matches_storage_replay`
/// equivalence test pins every produced `EmbeddingInput` field is
/// identical to what the legacy storage-side replay returns for the
/// same canonical corpus, so callers that already hold packets can
/// switch to this helper without changing semantic-index output.
#[allow(dead_code)]
pub fn semantic_inputs_from_packets(
    packets: &[ConversationPacket],
    contexts: &[SemanticPacketContext],
) -> Result<Vec<EmbeddingInput>> {
    if packets.len() != contexts.len() {
        anyhow::bail!(
            "semantic_inputs_from_packets length mismatch: {} packets vs {} contexts",
            packets.len(),
            contexts.len()
        );
    }
    let mut inputs = Vec::new();
    for (packet, context) in packets.iter().zip(contexts.iter()) {
        let source_id_hash = crc32fast::hash(packet.payload.provenance.source_id.as_bytes());
        for &message_index in &packet.projections.semantic.message_indices {
            let Some(message) = packet.payload.messages.get(message_index) else {
                anyhow::bail!(
                    "packet semantic projection references missing message index {} \
                     (packet has {} messages)",
                    message_index,
                    packet.payload.messages.len()
                );
            };
            inputs.extend(embedding_inputs_from_packet_message(
                context.conversation_id,
                context.agent_id,
                context.workspace_id,
                source_id_hash,
                message,
            ));
        }
    }
    tracing::debug!(
        packets = packets.len(),
        packet_driven = true,
        semantic_inputs = inputs.len(),
        "built semantic inputs from in-memory ConversationPacket batch"
    );
    Ok(inputs)
}

#[cfg(test)]
pub(in crate::indexer::semantic::engine) fn fetch_canonical_embedding_batch(
    storage: &FrankenStorage,
    after_conversation_id: i64,
    max_conversations: usize,
) -> Result<CanonicalEmbeddingBatch> {
    fetch_canonical_embedding_batch_inner(storage, after_conversation_id, max_conversations, None)
}

/// Variant of [`fetch_canonical_embedding_batch`] that additionally
/// filters out canonical messages with `message_id <= after_message_id`
/// when set. This is how sub-fix 2 (`last_message_id` cursor) enforces
/// the "resume MUST advance past `last_message_id`" rule on a partially
/// embedded conversation.
#[cfg(test)]
pub(in crate::indexer::semantic::engine) fn fetch_canonical_embedding_batch_inner(
    storage: &FrankenStorage,
    after_conversation_id: i64,
    max_conversations: usize,
    after_message_id: Option<i64>,
) -> Result<CanonicalEmbeddingBatch> {
    fetch_canonical_embedding_batch_inner_with_caps(
        storage,
        after_conversation_id,
        max_conversations,
        after_message_id,
        SemanticCheckpointCaps::unlimited(),
    )
}

#[cfg(test)]
pub(in crate::indexer::semantic::engine) fn fetch_canonical_embedding_batch_inner_with_caps(
    storage: &FrankenStorage,
    after_conversation_id: i64,
    max_conversations: usize,
    after_message_id: Option<i64>,
    caps: SemanticCheckpointCaps,
) -> Result<CanonicalEmbeddingBatch> {
    let total_conversations = total_semantic_conversations(storage)?;
    fetch_canonical_embedding_batch_inner_with_caps_and_total(
        storage,
        after_conversation_id,
        max_conversations,
        after_message_id,
        caps,
        total_conversations,
        None,
    )
}

pub(in crate::indexer::semantic::engine) fn fetch_canonical_embedding_batch_inner_with_caps_and_total(
    storage: &FrankenStorage,
    after_conversation_id: i64,
    max_conversations: usize,
    after_message_id: Option<i64>,
    caps: SemanticCheckpointCaps,
    total_conversations: u64,
    sink: Option<&SemanticProgressSink>,
) -> Result<CanonicalEmbeddingBatch> {
    let max_conversations = max_conversations.max(1);
    let query_limit = max_conversations.saturating_add(1);
    if let Some(sink) = sink
        && sink.is_active()
    {
        sink.emit(
            SemanticProgressEvent::SelectionCandidatesStart,
            SemanticProgressFields {
                batch_rows: Some(saturating_u64_from_usize(query_limit)),
                rows_total: Some(saturating_u64_from_usize(max_conversations)),
                last_conversation_id: Some(after_conversation_id),
                last_message_id: after_message_id,
                ..Default::default()
            },
        );
    }

    let (mut conversation_ids, candidate_rows_scanned) =
        fetch_bounded_semantic_candidate_conversation_ids(
            storage,
            after_conversation_id,
            after_message_id,
            query_limit,
        )?;

    let candidate_rows_returned = conversation_ids.len();
    let has_more_from_candidate_limit = conversation_ids.len() > max_conversations;
    if has_more_from_candidate_limit {
        conversation_ids.truncate(max_conversations);
    }
    if let Some(sink) = sink
        && sink.is_active()
    {
        sink.emit(
            SemanticProgressEvent::SelectionCandidatesDone,
            SemanticProgressFields {
                batch_rows: Some(saturating_u64_from_usize(conversation_ids.len())),
                rows_processed: Some(saturating_u64_from_usize(candidate_rows_scanned)),
                rows_total: Some(saturating_u64_from_usize(max_conversations)),
                last_conversation_id: Some(
                    conversation_ids
                        .last()
                        .copied()
                        .unwrap_or(after_conversation_id),
                ),
                last_message_id: after_message_id,
                note: Some(format!(
                    "has_more={has_more_from_candidate_limit};eligible_rows={candidate_rows_returned}"
                )),
                ..Default::default()
            },
        );
    }

    if conversation_ids.is_empty() {
        return Ok(CanonicalEmbeddingBatch {
            inputs: Vec::new(),
            conversations_in_batch: 0,
            last_conversation_id: after_conversation_id,
            total_conversations,
            cursor_exhausted: true,
        });
    }

    let conversations = fetch_canonical_embedding_conversations(storage, &conversation_ids)?;

    let (selection, mut grouped_messages) = select_checkpoint_capped_conversations_lazily(
        conversations,
        after_message_id,
        caps,
        |conversation_id| {
            let mut loaded =
                storage.fetch_messages_for_lexical_rebuild_batch(&[conversation_id], None, None)?;
            Ok(loaded.remove(&conversation_id).unwrap_or_default())
        },
    )?;
    let CheckpointCappedSelection {
        conversations,
        last_conversation_id,
        stopped_before_candidate,
    } = selection;
    let (inputs, _) = packet_embedding_inputs_from_materialized_canonical_messages(
        &conversations,
        &mut grouped_messages,
        |_| true,
    );

    let conversations_in_batch = u64::try_from(conversations.len()).unwrap_or(u64::MAX);
    let cursor_exhausted = !has_more_from_candidate_limit && !stopped_before_candidate;
    tracing::debug!(
        conversations_in_batch,
        cursor_exhausted,
        packet_driven = true,
        semantic_inputs = inputs.len(),
        max_messages_per_checkpoint = caps.max_messages,
        max_bytes_per_checkpoint = caps.max_bytes,
        ?after_message_id,
        "built semantic backfill batch from ConversationPacket canonical replay"
    );

    Ok(CanonicalEmbeddingBatch {
        inputs,
        conversations_in_batch,
        last_conversation_id: last_conversation_id.unwrap_or(after_conversation_id),
        total_conversations,
        cursor_exhausted,
    })
}

pub(in crate::indexer::semantic::engine) struct CheckpointCappedSelection {
    pub(in crate::indexer::semantic::engine) conversations: Vec<CanonicalEmbeddingConversationRow>,
    pub(in crate::indexer::semantic::engine) last_conversation_id: Option<i64>,
    pub(in crate::indexer::semantic::engine) stopped_before_candidate: bool,
}

pub(in crate::indexer::semantic::engine) fn select_checkpoint_capped_conversations_lazily<F>(
    conversations: Vec<CanonicalEmbeddingConversationRow>,
    after_message_id: Option<i64>,
    caps: SemanticCheckpointCaps,
    mut load_messages: F,
) -> Result<(CheckpointCappedSelection, HashMap<i64, Vec<Message>>)>
where
    F: FnMut(i64) -> Result<Vec<Message>>,
{
    let mut selected = Vec::new();
    let mut grouped_messages = HashMap::new();
    let mut selected_messages = 0usize;
    let mut selected_bytes = 0u64;
    let mut last_conversation_id = None;
    let mut stopped_before_candidate = false;

    for conversation in conversations {
        let message_cap_full = caps.message_limited()
            && !selected.is_empty()
            && selected_messages >= caps.max_messages;
        let byte_cap_full =
            caps.byte_limited() && !selected.is_empty() && selected_bytes >= caps.max_bytes;
        if message_cap_full || byte_cap_full {
            stopped_before_candidate = true;
            break;
        }

        let mut messages = load_messages(conversation.conversation_id)?;
        if let Some(min_exclusive) = after_message_id {
            messages.retain(|message| message.id.is_some_and(|id| id > min_exclusive));
        }
        if messages.is_empty() {
            // This candidate was actually inspected. Advancing past it avoids
            // replaying an all-empty page selected through stale tail metadata.
            // Unread and over-budget nonempty candidates never reach this arm.
            last_conversation_id = Some(conversation.conversation_id);
            continue;
        }

        let message_count = messages.len();
        let byte_count = messages
            .iter()
            .map(|message| saturating_u64_from_usize(message.content.len()))
            .fold(0u64, u64::saturating_add);
        let would_exceed_messages = caps.message_limited()
            && !selected.is_empty()
            && selected_messages.saturating_add(message_count) > caps.max_messages;
        let would_exceed_bytes = caps.byte_limited()
            && !selected.is_empty()
            && selected_bytes.saturating_add(byte_count) > caps.max_bytes;
        if would_exceed_messages || would_exceed_bytes {
            stopped_before_candidate = true;
            tracing::debug!(
                conversation_id = conversation.conversation_id,
                selected_conversations = selected.len(),
                selected_messages,
                selected_bytes,
                candidate_messages = message_count,
                candidate_bytes = byte_count,
                max_messages_per_checkpoint = caps.max_messages,
                max_bytes_per_checkpoint = caps.max_bytes,
                "semantic checkpoint cap stopped this batch before the next full conversation"
            );
            break;
        }

        selected_messages = selected_messages.saturating_add(message_count);
        selected_bytes = selected_bytes.saturating_add(byte_count);
        last_conversation_id = Some(conversation.conversation_id);
        grouped_messages.insert(conversation.conversation_id, messages);
        selected.push(conversation);
    }

    Ok((
        CheckpointCappedSelection {
            conversations: selected,
            last_conversation_id,
            stopped_before_candidate,
        },
        grouped_messages,
    ))
}

#[cfg(test)]
pub fn packet_embedding_inputs_from_storage(
    storage: &FrankenStorage,
) -> Result<Vec<EmbeddingInput>> {
    Ok(fetch_canonical_embedding_batch(storage, 0, usize::MAX)?.inputs)
}

/// Collect every canonical semantic input while reporting bounded replay
/// progress in conversation units.
///
/// The direct `cass index --semantic` path ultimately needs the complete input
/// vector for embedding, but replaying the canonical database can itself take
/// long enough to deserve an honest phase counter. Reuse the same bounded
/// visitor as reconciliation so callers see progress after each fully
/// materialized conversation batch rather than one synthetic jump after an
/// unbounded query.
pub fn packet_embedding_inputs_from_storage_with_progress<F>(
    storage: &FrankenStorage,
    on_progress: F,
) -> Result<Vec<EmbeddingInput>>
where
    F: FnMut(usize, usize),
{
    let mut inputs = Vec::new();
    visit_packet_embedding_inputs_from_storage_with_limits_and_progress(
        storage,
        DEFAULT_SEMANTIC_RECONCILIATION_SCAN_CONVERSATIONS,
        SemanticCheckpointCaps {
            max_messages: DEFAULT_SEMANTIC_MAX_MESSAGES_PER_CHECKPOINT,
            max_bytes: DEFAULT_SEMANTIC_MAX_BYTES_PER_CHECKPOINT,
        },
        |input| {
            inputs.push(input);
            Ok(())
        },
        on_progress,
    )?;
    Ok(inputs)
}

/// Replay canonical semantic inputs in bounded conversation batches.
///
/// Reconciliation needs to inspect every canonical document identity, but it
/// must not retain every message body merely to discover a small repair delta.
/// The callback therefore receives each input by value while this helper keeps
/// only one bounded replay batch resident at a time.
pub fn visit_packet_embedding_inputs_from_storage<F>(
    storage: &FrankenStorage,
    visit: F,
) -> Result<()>
where
    F: FnMut(EmbeddingInput) -> Result<()>,
{
    visit_packet_embedding_inputs_from_storage_with_limits(
        storage,
        DEFAULT_SEMANTIC_RECONCILIATION_SCAN_CONVERSATIONS,
        SemanticCheckpointCaps {
            max_messages: DEFAULT_SEMANTIC_MAX_MESSAGES_PER_CHECKPOINT,
            max_bytes: DEFAULT_SEMANTIC_MAX_BYTES_PER_CHECKPOINT,
        },
        visit,
    )
}

pub(in crate::indexer::semantic::engine) fn visit_packet_embedding_inputs_from_storage_with_limits<
    F,
>(
    storage: &FrankenStorage,
    max_conversations: usize,
    caps: SemanticCheckpointCaps,
    visit: F,
) -> Result<()>
where
    F: FnMut(EmbeddingInput) -> Result<()>,
{
    visit_packet_embedding_inputs_from_storage_with_limits_and_progress(
        storage,
        max_conversations,
        caps,
        visit,
        |_, _| {},
    )
}

fn visit_packet_embedding_inputs_from_storage_with_limits_and_progress<F, P>(
    storage: &FrankenStorage,
    max_conversations: usize,
    caps: SemanticCheckpointCaps,
    mut visit: F,
    mut on_progress: P,
) -> Result<()>
where
    F: FnMut(EmbeddingInput) -> Result<()>,
    P: FnMut(usize, usize),
{
    let mut after_conversation_id = 0i64;
    let total_conversations = total_semantic_conversations(storage)?;
    let total_conversations_usize = usize::try_from(total_conversations).unwrap_or(usize::MAX);
    let mut processed_conversations = 0_u64;
    loop {
        let batch = fetch_canonical_embedding_batch_inner_with_caps_and_total(
            storage,
            after_conversation_id,
            max_conversations,
            None,
            caps,
            total_conversations,
            None,
        )?;
        for input in batch.inputs {
            visit(input)?;
        }
        processed_conversations =
            processed_conversations.saturating_add(batch.conversations_in_batch);
        on_progress(
            usize::try_from(processed_conversations).unwrap_or(usize::MAX),
            total_conversations_usize,
        );
        if batch.cursor_exhausted {
            return Ok(());
        }
        if batch.last_conversation_id <= after_conversation_id {
            bail!(
                "canonical semantic reconciliation scan did not advance beyond conversation \
                 {after_conversation_id}"
            );
        }
        after_conversation_id = batch.last_conversation_id;
    }
}

pub(in crate::indexer::semantic::engine) fn packet_embedding_inputs_from_selected_canonical_messages<
    F,
>(
    storage: &FrankenStorage,
    conversation_ids: &[i64],
    include_message: F,
) -> Result<(Vec<EmbeddingInput>, Option<i64>)>
where
    F: FnMut(&Message) -> bool,
{
    if conversation_ids.is_empty() {
        return Ok((Vec::new(), None));
    }

    let conversations = fetch_canonical_embedding_conversations(storage, conversation_ids)?;
    let mut grouped_messages =
        storage.fetch_messages_for_lexical_rebuild_batch(conversation_ids, None, None)?;
    Ok(
        packet_embedding_inputs_from_materialized_canonical_messages(
            &conversations,
            &mut grouped_messages,
            include_message,
        ),
    )
}

pub(in crate::indexer::semantic::engine) fn packet_embedding_inputs_from_materialized_canonical_messages<
    F,
>(
    conversations: &[CanonicalEmbeddingConversationRow],
    grouped_messages: &mut HashMap<i64, Vec<Message>>,
    mut include_message: F,
) -> (Vec<EmbeddingInput>, Option<i64>)
where
    F: FnMut(&Message) -> bool,
{
    let mut inputs = Vec::new();
    let mut raw_max_message_id: Option<i64> = None;

    for conversation in conversations {
        let mut messages = grouped_messages
            .remove(&conversation.conversation_id)
            .unwrap_or_default();
        messages.retain(|message| {
            let keep = include_message(message);
            if keep && let Some(message_id) = message.id {
                raw_max_message_id =
                    Some(raw_max_message_id.map_or(message_id, |current| current.max(message_id)));
            }
            keep
        });
        if messages.is_empty() {
            continue;
        }

        let provenance = canonical_embedding_packet_provenance(conversation);
        let canonical = canonical_embedding_conversation(conversation, &provenance, messages);
        let packet = ConversationPacket::from_canonical_replay(&canonical, provenance);
        inputs.extend(embedding_inputs_from_conversation_packet(
            conversation,
            &packet,
        ));
    }

    (inputs, raw_max_message_id)
}

pub fn packet_embedding_inputs_from_storage_since(
    storage: &FrankenStorage,
    since_message_id: i64,
) -> Result<CanonicalIncrementalEmbeddingBatch> {
    let conversation_ids: Vec<i64> = storage
        .raw()
        .query_map_collect(
            "SELECT DISTINCT m.conversation_id
             FROM messages m
             WHERE m.id > ?1
             ORDER BY m.conversation_id ASC",
            &[ParamValue::from(since_message_id)],
            |row| row.get_typed(0),
        )
        .with_context(|| {
            format!(
                "fetching canonical semantic catch-up conversation ids after message {since_message_id}"
            )
        })?;

    if conversation_ids.is_empty() {
        return Ok(CanonicalIncrementalEmbeddingBatch {
            inputs: Vec::new(),
            conversations_in_batch: 0,
            raw_max_message_id: None,
        });
    }

    let (inputs, raw_max_message_id) = packet_embedding_inputs_from_selected_canonical_messages(
        storage,
        &conversation_ids,
        |message| message.id.is_some_and(|id| id > since_message_id),
    )?;

    let conversations_in_batch = u64::try_from(conversation_ids.len()).unwrap_or(u64::MAX);
    tracing::debug!(
        since_message_id,
        conversations_in_batch,
        packet_driven = true,
        semantic_inputs = inputs.len(),
        "built semantic catch-up batch from ConversationPacket canonical replay"
    );

    Ok(CanonicalIncrementalEmbeddingBatch {
        inputs,
        conversations_in_batch,
        raw_max_message_id,
    })
}

pub fn packet_embedding_inputs_from_storage_for_message_ids(
    storage: &FrankenStorage,
    conversation_ids: &[i64],
    message_ids: &HashSet<i64>,
) -> Result<Vec<EmbeddingInput>> {
    if conversation_ids.is_empty() || message_ids.is_empty() {
        return Ok(Vec::new());
    }

    let (inputs, raw_max_message_id) = packet_embedding_inputs_from_selected_canonical_messages(
        storage,
        conversation_ids,
        |message| message.id.is_some_and(|id| message_ids.contains(&id)),
    )?;
    tracing::debug!(
        conversations_in_batch = conversation_ids.len(),
        selected_message_ids = message_ids.len(),
        semantic_inputs = inputs.len(),
        raw_max_message_id,
        packet_driven = true,
        "built selected semantic batch from ConversationPacket canonical replay"
    );

    Ok(inputs)
}
