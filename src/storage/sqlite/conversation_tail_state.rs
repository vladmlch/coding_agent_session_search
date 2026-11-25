use super::fts_rebuild::path_to_string;
use super::source_and_historical_import::SOURCE_PATH_MERGE_START_TOLERANCE_MS;
use super::open_and_locks::MESSAGE_LOOKUP_TRACE_ENABLED;
use super::reconcile_and_codebuff::{
    ConversationMergeEvidence, ExistingConversationNewMessages, ExistingConversationTailMetadata,
    ExistingConversationTailState, ExistingConversationWithTail, MessageMergeFingerprint,
    MessageReplayCore, MessageReplayFingerprintSet, PendingConversationKey,
    conversation_effective_started_at, conversation_external_lookup_key, message_replay_parts,
    reconciled_native_messages,
};
use super::tx_conversation_insert::{
    franken_find_existing_conversation_by_key, franken_find_external_conversation_tail_lookup,
    franken_promote_pi_family_external_identity_by_source_path,
};
use crate::franken_sync::compat::{
    OptionalExtension as FrankenOptionalExtension, RowExt as FrankenRowExt,
    Transaction as FrankenTransaction,
};
use crate::model::types::Conversation;
use anyhow::Result;
use std::collections::HashSet;
use std::sync::atomic::Ordering;

pub(super) fn franken_existing_conversation_append_tail_state(
    tx: &FrankenTransaction<'_>,
    conversation_id: i64,
) -> Result<Option<ExistingConversationTailState>> {
    let cached: Option<(Option<i64>, Option<i64>, Option<i64>)> = tx
        .query_row_map(
            "SELECT last_message_idx, last_message_created_at, ended_at
             FROM conversation_tail_state
             WHERE conversation_id = ?1",
            fparams![conversation_id],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?, row.get_typed(2)?)),
        )
        .optional()?;
    if let Some(cached) = cached {
        let (_, _, cached_ended_at) = cached;
        if let Some(tail_state) =
            existing_conversation_tail_state_from_cached(cached.0, cached.1, cached_ended_at)
        {
            return Ok(Some(tail_state));
        }
    }

    let legacy_cached: (Option<i64>, Option<i64>, Option<i64>) = tx.query_row_map(
        "SELECT last_message_idx, last_message_created_at, ended_at
         FROM conversations
         WHERE id = ?1",
        fparams![conversation_id],
        |row| Ok((row.get_typed(0)?, row.get_typed(1)?, row.get_typed(2)?)),
    )?;
    let (_, _, cached_ended_at) = legacy_cached;
    if let Some(tail_state) = existing_conversation_tail_state_from_cached(
        legacy_cached.0,
        legacy_cached.1,
        cached_ended_at,
    ) {
        franken_insert_conversation_tail_state(
            tx,
            conversation_id,
            cached_ended_at,
            Some(tail_state.last_message_idx),
            Some(tail_state.last_message_created_at),
        )?;
        return Ok(Some(tail_state));
    }

    let (max_idx, max_created_at): (Option<i64>, Option<i64>) = tx.query_row_map(
        "SELECT MAX(idx), MAX(created_at)
         FROM messages
         WHERE conversation_id = ?1",
        fparams![conversation_id],
        |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
    )?;
    if let Some((last_message_idx, last_message_created_at)) = max_idx.zip(max_created_at) {
        franken_update_conversation_tail_state(
            tx,
            conversation_id,
            None,
            Some(last_message_idx),
            Some(last_message_created_at),
        )?;
        return Ok(Some(ExistingConversationTailState {
            last_message_idx,
            last_message_created_at,
            ended_at: cached_ended_at,
        }));
    }
    Ok(None)
}

pub(super) fn franken_cached_existing_conversation_tail_metadata(
    tx: &FrankenTransaction<'_>,
    conversation_id: i64,
) -> Result<ExistingConversationTailMetadata> {
    let cached: Option<(Option<i64>, Option<i64>, Option<i64>)> = tx
        .query_row_map(
            "SELECT last_message_idx, last_message_created_at, ended_at
             FROM conversation_tail_state
             WHERE conversation_id = ?1",
            fparams![conversation_id],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?, row.get_typed(2)?)),
        )
        .optional()?;
    if let Some(cached) = cached {
        return Ok(ExistingConversationTailMetadata {
            last_message_idx: cached.0,
            last_message_created_at: cached.1,
            ended_at: cached.2,
        });
    }

    let legacy_cached: (Option<i64>, Option<i64>, Option<i64>) = tx.query_row_map(
        "SELECT last_message_idx, last_message_created_at, ended_at
         FROM conversations
         WHERE id = ?1",
        fparams![conversation_id],
        |row| Ok((row.get_typed(0)?, row.get_typed(1)?, row.get_typed(2)?)),
    )?;
    Ok(ExistingConversationTailMetadata {
        last_message_idx: legacy_cached.0,
        last_message_created_at: legacy_cached.1,
        ended_at: legacy_cached.2,
    })
}

pub(super) fn existing_conversation_tail_state_from_cached(
    last_message_idx: Option<i64>,
    last_message_created_at: Option<i64>,
    ended_at: Option<i64>,
) -> Option<ExistingConversationTailState> {
    let (last_message_idx, last_message_created_at) =
        last_message_idx.zip(last_message_created_at)?;
    Some(ExistingConversationTailState {
        last_message_idx,
        last_message_created_at,
        ended_at,
    })
}

pub(super) fn franken_find_existing_conversation_with_tail_by_key(
    tx: &FrankenTransaction<'_>,
    key: &PendingConversationKey,
    conv: Option<&Conversation>,
) -> Result<Option<ExistingConversationWithTail>> {
    if let PendingConversationKey::External {
        source_id,
        agent_id,
        external_id,
    } = key
    {
        let lookup_key = conversation_external_lookup_key(source_id, *agent_id, external_id);
        if let Some(existing) = franken_find_external_conversation_tail_lookup(tx, &lookup_key)? {
            return Ok(Some(existing));
        }
        if let Some(existing) = franken_promote_pi_family_external_identity_by_source_path(
            tx,
            source_id,
            *agent_id,
            external_id,
            conv,
        )? {
            return Ok(Some(existing));
        }
        return Ok(None);
    }

    let Some(id) = franken_find_existing_conversation_by_key(tx, key, conv)? else {
        return Ok(None);
    };
    let tail_state = franken_existing_conversation_append_tail_state(tx, id)?;
    Ok(Some(ExistingConversationWithTail { id, tail_state }))
}

pub(super) fn franken_insert_conversation_tail_state(
    tx: &FrankenTransaction<'_>,
    conversation_id: i64,
    ended_at: Option<i64>,
    last_message_idx: Option<i64>,
    last_message_created_at: Option<i64>,
) -> Result<()> {
    if ended_at.is_none() && last_message_idx.is_none() && last_message_created_at.is_none() {
        return Ok(());
    }
    tx.execute_compat(
        "INSERT OR REPLACE INTO conversation_tail_state (
             conversation_id, ended_at, last_message_idx, last_message_created_at
         ) VALUES (?1, ?2, ?3, ?4)",
        fparams![
            conversation_id,
            ended_at,
            last_message_idx,
            last_message_created_at
        ],
    )?;
    Ok(())
}

fn franken_update_conversation_tail_columns(
    tx: &FrankenTransaction<'_>,
    conversation_id: i64,
    ended_at_candidate: Option<i64>,
    last_message_idx_candidate: Option<i64>,
    last_message_created_at_candidate: Option<i64>,
) -> Result<()> {
    if ended_at_candidate.is_none()
        && last_message_idx_candidate.is_none()
        && last_message_created_at_candidate.is_none()
    {
        return Ok(());
    }

    tx.execute_compat(
        "UPDATE conversations
         SET ended_at = CASE
                 WHEN ?1 IS NULL THEN ended_at
                 WHEN ended_at IS NULL OR ended_at < ?1 THEN ?1
                 ELSE ended_at
             END,
             last_message_idx = CASE
                 WHEN ?2 IS NULL THEN last_message_idx
                 WHEN last_message_idx IS NULL OR last_message_idx < ?2 THEN ?2
                 ELSE last_message_idx
             END,
             last_message_created_at = CASE
                 WHEN ?3 IS NULL THEN last_message_created_at
                 WHEN last_message_created_at IS NULL OR last_message_created_at < ?3 THEN ?3
                 ELSE last_message_created_at
             END
         WHERE id = ?4",
        fparams![
            ended_at_candidate,
            last_message_idx_candidate,
            last_message_created_at_candidate,
            conversation_id
        ],
    )?;
    Ok(())
}

fn franken_tail_state_insert_ended_at(
    tx: &FrankenTransaction<'_>,
    conversation_id: i64,
    candidate: Option<i64>,
) -> Result<Option<i64>> {
    let canonical: Option<i64> = tx
        .query_row_map(
            "SELECT ended_at FROM conversations WHERE id = ?1",
            fparams![conversation_id],
            |row| row.get_typed(0),
        )
        .optional()?
        .flatten();
    Ok(canonical.max(candidate))
}

pub(super) fn franken_update_conversation_tail_state(
    tx: &FrankenTransaction<'_>,
    conversation_id: i64,
    ended_at_candidate: Option<i64>,
    last_message_idx_candidate: Option<i64>,
    last_message_created_at_candidate: Option<i64>,
) -> Result<()> {
    if ended_at_candidate.is_none()
        && last_message_idx_candidate.is_none()
        && last_message_created_at_candidate.is_none()
    {
        return Ok(());
    }

    let changed = tx.execute_compat(
        "UPDATE conversation_tail_state
         SET ended_at = CASE
                 WHEN ?1 IS NULL THEN ended_at
                 ELSE MAX(IFNULL(ended_at, 0), ?1)
             END,
             last_message_idx = CASE
                 WHEN ?2 IS NULL THEN last_message_idx
                 WHEN last_message_idx IS NULL OR last_message_idx < ?2 THEN ?2
                 ELSE last_message_idx
             END,
             last_message_created_at = CASE
                 WHEN ?3 IS NULL THEN last_message_created_at
                 WHEN last_message_created_at IS NULL OR last_message_created_at < ?3 THEN ?3
                 ELSE last_message_created_at
             END
         WHERE conversation_id = ?4",
        fparams![
            ended_at_candidate,
            last_message_idx_candidate,
            last_message_created_at_candidate,
            conversation_id
        ],
    )?;
    if changed == 0 {
        let insert_ended_at =
            franken_tail_state_insert_ended_at(tx, conversation_id, ended_at_candidate)?;
        franken_insert_conversation_tail_state(
            tx,
            conversation_id,
            insert_ended_at,
            last_message_idx_candidate,
            last_message_created_at_candidate,
        )?;
    }
    franken_update_conversation_tail_columns(
        tx,
        conversation_id,
        ended_at_candidate,
        last_message_idx_candidate,
        last_message_created_at_candidate,
    )?;
    Ok(())
}

pub(super) fn franken_set_conversation_tail_state_after_append(
    tx: &FrankenTransaction<'_>,
    conversation_id: i64,
    ended_at: i64,
    last_message_idx: i64,
    last_message_created_at: i64,
) -> Result<()> {
    let changed = tx.execute_compat(
        "UPDATE conversation_tail_state
         SET ended_at = ?1,
             last_message_idx = ?2,
             last_message_created_at = ?3
         WHERE conversation_id = ?4",
        fparams![
            ended_at,
            last_message_idx,
            last_message_created_at,
            conversation_id
        ],
    )?;
    if changed == 0 {
        let insert_ended_at =
            franken_tail_state_insert_ended_at(tx, conversation_id, Some(ended_at))?;
        franken_insert_conversation_tail_state(
            tx,
            conversation_id,
            insert_ended_at,
            Some(last_message_idx),
            Some(last_message_created_at),
        )?;
    }
    franken_update_conversation_tail_columns(
        tx,
        conversation_id,
        Some(ended_at),
        Some(last_message_idx),
        Some(last_message_created_at),
    )?;
    Ok(())
}

pub(super) fn collect_append_only_tail_messages<'a>(
    conv: &'a Conversation,
    existing_max_idx: i64,
    existing_max_created_at: i64,
) -> Option<ExistingConversationNewMessages<'a>> {
    if conv.agent_slug == "grok_bot" {
        return Some(reconciled_native_messages(conv));
    }
    if conv.messages.is_empty() {
        return Some(ExistingConversationNewMessages {
            messages: Vec::new(),
            new_chars: 0,
            idx_collision_count: 0,
            first_collision_idx: None,
        });
    }

    let mut split_idx = None;
    let mut prev_idx = None;
    for (pos, msg) in conv.messages.iter().enumerate() {
        if prev_idx.is_some_and(|prev| msg.idx < prev) {
            return None;
        }
        prev_idx = Some(msg.idx);
        if split_idx.is_none() && msg.idx > existing_max_idx {
            split_idx = Some(pos);
        }
    }
    let split_idx = split_idx?;
    if split_idx != 0 {
        return None;
    }

    let mut seen_tail_idx = HashSet::new();
    let mut seen_tail_replay = MessageReplayFingerprintSet::default();
    let mut new_chars = 0i64;
    let mut messages = Vec::new();
    for msg in &conv.messages[split_idx..] {
        let created_at = msg.created_at?;
        if created_at <= existing_max_created_at {
            return None;
        }

        if !seen_tail_idx.insert(msg.idx) {
            return None;
        }

        let (replay_core, replay_native_id) = message_replay_parts(msg);
        if seen_tail_replay.contains_parts(&replay_core, &replay_native_id) {
            return None;
        }
        seen_tail_replay.insert_parts(replay_core, replay_native_id);

        new_chars += msg.content.len() as i64;
        messages.push(msg);
    }

    Some(ExistingConversationNewMessages {
        messages,
        new_chars,
        idx_collision_count: 0,
        first_collision_idx: None,
    })
}

pub(super) fn collect_existing_conversation_noop_from_idx_tail<'a>(
    conv: &'a Conversation,
    _existing_max_idx: i64,
) -> Option<ExistingConversationNewMessages<'a>> {
    if conv.messages.is_empty() {
        return Some(ExistingConversationNewMessages {
            messages: Vec::new(),
            new_chars: 0,
            idx_collision_count: 0,
            first_collision_idx: None,
        });
    }

    // A max idx alone does not prove lower idx rows exist. Sparse historical
    // imports can have idx 2..3 first and later recover idx 0..1, so non-empty
    // no-op decisions must use the bounded message lookup.
    None
}

pub(super) fn collect_existing_conversation_noop_from_conversation_ended_at<'a>(
    conv: &'a Conversation,
    existing_ended_at: i64,
) -> Option<ExistingConversationNewMessages<'a>> {
    if conv.messages.is_empty()
        && conv
            .ended_at
            .is_none_or(|ended_at| ended_at <= existing_ended_at)
    {
        return Some(ExistingConversationNewMessages {
            messages: Vec::new(),
            new_chars: 0,
            idx_collision_count: 0,
            first_collision_idx: None,
        });
    }

    // A conversation-level ended_at says nothing about whether every earlier
    // message row was archived. Defer non-empty batches to the bounded lookup.
    None
}

pub(super) fn collect_existing_conversation_tail_from_ended_at<'a>(
    conv: &'a Conversation,
    existing_ended_at: i64,
) -> Option<ExistingConversationNewMessages<'a>> {
    if conv.messages.is_empty() {
        return Some(ExistingConversationNewMessages {
            messages: Vec::new(),
            new_chars: 0,
            idx_collision_count: 0,
            first_collision_idx: None,
        });
    }

    let mut prev_idx = None;
    for msg in conv.messages.iter() {
        if prev_idx.is_some_and(|prev| msg.idx <= prev) {
            return None;
        }
        prev_idx = Some(msg.idx);
        if msg.created_at? <= existing_ended_at {
            return None;
        }
    }

    let mut seen_tail_replay = MessageReplayFingerprintSet::default();
    let mut new_chars = 0i64;
    let mut messages = Vec::new();
    for msg in &conv.messages {
        let (replay_core, replay_native_id) = message_replay_parts(msg);
        if seen_tail_replay.contains_parts(&replay_core, &replay_native_id) {
            return None;
        }
        seen_tail_replay.insert_parts(replay_core, replay_native_id);

        new_chars += msg.content.len() as i64;
        messages.push(msg);
    }

    Some(ExistingConversationNewMessages {
        messages,
        new_chars,
        idx_collision_count: 0,
        first_collision_idx: None,
    })
}

pub(super) fn trace_existing_conversation_lookup_fallback(
    conversation_id: i64,
    conv: &Conversation,
    tail_state: Option<ExistingConversationTailState>,
    existing_ended_at: Option<i64>,
) {
    if !MESSAGE_LOOKUP_TRACE_ENABLED.load(Ordering::Relaxed) {
        return;
    }

    let mut prev_idx = None;
    let mut idx_order_violations = 0usize;
    let mut duplicate_idx_count = 0usize;
    let mut seen_idx = HashSet::new();
    let mut missing_created_at = 0usize;
    let mut min_idx = None;
    let mut max_idx = None;
    let mut min_created_at = None;
    let mut max_created_at = None;
    for msg in &conv.messages {
        if prev_idx.is_some_and(|prev| msg.idx < prev) {
            idx_order_violations = idx_order_violations.saturating_add(1);
        }
        prev_idx = Some(msg.idx);
        if !seen_idx.insert(msg.idx) {
            duplicate_idx_count = duplicate_idx_count.saturating_add(1);
        }
        min_idx = Some(min_idx.map_or(msg.idx, |current: i64| current.min(msg.idx)));
        max_idx = Some(max_idx.map_or(msg.idx, |current: i64| current.max(msg.idx)));
        if let Some(created_at) = msg.created_at {
            min_created_at =
                Some(min_created_at.map_or(created_at, |current: i64| current.min(created_at)));
            max_created_at =
                Some(max_created_at.map_or(created_at, |current: i64| current.max(created_at)));
        } else {
            missing_created_at = missing_created_at.saturating_add(1);
        }
    }

    let first_idx_after_tail = tail_state.and_then(|state| {
        conv.messages
            .iter()
            .find(|msg| msg.idx > state.last_message_idx)
            .map(|msg| msg.idx)
    });
    let first_created_after_tail = tail_state.and_then(|state| {
        conv.messages
            .iter()
            .find(|msg| {
                msg.created_at
                    .is_some_and(|created_at| created_at > state.last_message_created_at)
            })
            .and_then(|msg| msg.created_at)
    });
    let first_created_after_ended_at = existing_ended_at.and_then(|ended_at| {
        conv.messages
            .iter()
            .find(|msg| {
                msg.created_at
                    .is_some_and(|created_at| created_at > ended_at)
            })
            .and_then(|msg| msg.created_at)
    });

    let payload = serde_json::json!({
        "event": "existing_conversation_message_lookup_fallback",
        "conversation_id": conversation_id,
        "agent_slug": conv.agent_slug,
        "source_path": conv.source_path,
        "external_id": conv.external_id,
        "messages": conv.messages.len(),
        "min_idx": min_idx,
        "max_idx": max_idx,
        "missing_created_at": missing_created_at,
        "min_created_at": min_created_at,
        "max_created_at": max_created_at,
        "idx_order_violations": idx_order_violations,
        "duplicate_idx_count": duplicate_idx_count,
        "tail_state": tail_state.map(|state| {
            serde_json::json!({
                "last_message_idx": state.last_message_idx,
                "last_message_created_at": state.last_message_created_at,
                "ended_at": state.ended_at,
            })
        }),
        "existing_ended_at": existing_ended_at,
        "first_idx_after_tail": first_idx_after_tail,
        "first_created_after_tail": first_created_after_tail,
        "first_created_after_ended_at": first_created_after_ended_at,
    });
    if let Ok(line) = serde_json::to_string(&payload) {
        eprintln!("{line}");
    }
}

pub(super) fn franken_existing_conversation_ended_at(
    tx: &FrankenTransaction<'_>,
    conversation_id: i64,
) -> Result<Option<i64>> {
    let ended_at: Option<Option<i64>> = tx
        .query_row_map(
            "SELECT ended_at
             FROM conversations
             WHERE id = ?1",
            fparams![conversation_id],
            |row| row.get_typed(0),
        )
        .optional()?;
    Ok(ended_at.flatten())
}

pub(super) fn start_distance_ms(left: Option<i64>, right: Option<i64>) -> i64 {
    match (left, right) {
        (Some(left), Some(right)) => (i128::from(left) - i128::from(right))
            .abs()
            .try_into()
            .unwrap_or(i64::MAX),
        _ => i64::MAX,
    }
}

pub(super) fn conversation_merge_evidence(
    incoming_exact: &HashSet<MessageMergeFingerprint>,
    incoming_replay: &HashSet<MessageReplayCore>,
    existing_exact: &HashSet<MessageMergeFingerprint>,
    existing_replay: &HashSet<MessageReplayCore>,
    incoming_started_at: Option<i64>,
    existing_started_at: Option<i64>,
) -> Option<ConversationMergeEvidence> {
    let exact_overlap = incoming_exact.intersection(existing_exact).count();
    let replay_overlap = incoming_replay.intersection(existing_replay).count();
    if exact_overlap == 0 && replay_overlap == 0 {
        return None;
    }

    let smaller_replay_set = incoming_replay.len().min(existing_replay.len());
    let started_close = timestamps_within_tolerance(
        incoming_started_at,
        existing_started_at,
        SOURCE_PATH_MERGE_START_TOLERANCE_MS,
    );
    let full_replay_subset_match = smaller_replay_set >= 2 && replay_overlap == smaller_replay_set;

    let merge_allowed = if started_close {
        exact_overlap >= 1 || replay_overlap >= 2
    } else {
        exact_overlap >= 2 || full_replay_subset_match
    };

    merge_allowed.then_some(ConversationMergeEvidence {
        exact_overlap,
        replay_overlap,
        smaller_replay_set,
        started_close,
        start_distance_ms: start_distance_ms(incoming_started_at, existing_started_at),
    })
}

fn timestamps_within_tolerance(left: Option<i64>, right: Option<i64>, tolerance_ms: i64) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => {
            (i128::from(left) - i128::from(right)).abs() <= i128::from(tolerance_ms)
        }
        _ => false,
    }
}

pub(super) fn conversation_merge_key(agent_id: i64, conv: &Conversation) -> PendingConversationKey {
    if let Some(external_id) = conv.external_id.clone() {
        PendingConversationKey::External {
            source_id: conv.source_id.clone(),
            agent_id,
            external_id,
        }
    } else {
        PendingConversationKey::SourcePath {
            source_id: conv.source_id.clone(),
            agent_id,
            source_path: path_to_string(&conv.source_path),
            started_at: conversation_effective_started_at(conv),
        }
    }
}

/// Message data needed for semantic embedding generation.
pub struct MessageForEmbedding {
    pub message_id: i64,
    pub created_at: Option<i64>,
    pub agent_id: i64,
    pub workspace_id: Option<i64>,
    pub source_id_hash: u32,
    pub role: String,
    pub content: String,
}
