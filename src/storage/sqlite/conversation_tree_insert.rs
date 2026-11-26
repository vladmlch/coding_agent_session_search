use super::aggregators_and_pricing::StatsDelta;
use super::batch_ingest::{
    defer_storage_lexical_updates_enabled, normalized_conversation_for_storage,
};
use super::conversation_tail_state::{
    collect_append_only_tail_messages, conversation_merge_key,
    franken_find_existing_conversation_with_tail_by_key,
    franken_set_conversation_tail_state_after_append, franken_update_conversation_tail_state,
};
use super::fts_rebuild::{
    FTS_ENTRY_BATCH_MAX_CHARS, FTS_ENTRY_BATCH_MAX_DOCS, FtsEntry, flush_pending_fts_entries,
};
#[cfg(test)]
use super::reconcile_and_codebuff::InsertConversationTreePerfProfile;
use super::reconcile_and_codebuff::{
    ExistingConversationNewMessages, ExistingConversationTailState, InsertOutcome,
    MessageReplayFingerprintSet, borrowed_messages_tail_state,
    collect_new_messages_for_existing_conversation, conversation_effective_started_at,
    conversation_external_lookup_key_for_conv, conversation_tail_ended_at_candidate,
    franken_reconcile_native_message_indices, franken_reconcile_provider_workspace,
    message_merge_fingerprint, message_replay_parts,
};
use super::storage_core::FrankenStorage;
use super::tx_analytics_rollups::{
    franken_reassociate_provider_analytics_workspace, franken_update_daily_stats_in_tx,
};
#[cfg(test)]
use super::tx_conversation_insert::franken_find_existing_conversation_by_key;
use super::tx_conversation_insert::{
    ConversationInsertStatus, defer_analytics_updates_enabled,
    franken_insert_conversation_or_get_existing_after_miss,
    franken_update_external_conversation_tail_after_append,
    franken_update_external_conversation_tail_lookup_key,
};
use super::tx_message_insert::{
    ExistingMessageLookup, franken_append_insert_new_messages, franken_batch_insert_new_messages,
    franken_existing_message_lookup, franken_insert_snippets,
};
#[cfg(test)]
use super::tx_message_insert::{
    franken_append_insert_new_messages_with_profile, franken_batch_insert_new_messages_with_profile,
};
use crate::franken_sync::{
    TransactionBehavior,
    compat::{Transaction as FrankenTransaction, TransactionExt as FrankenTransactionExt},
};
use crate::model::types::Conversation;
#[cfg(test)]
use anyhow::anyhow;
use anyhow::{Context, Result};
use std::collections::HashMap;
#[cfg(test)]
use std::time::Instant;

impl FrankenStorage {
    /// Insert a conversation tree (conversation + messages + snippets + FTS).
    pub fn insert_conversation_tree(
        &self,
        agent_id: i64,
        workspace_id: Option<i64>,
        conv: &Conversation,
    ) -> Result<InsertOutcome> {
        self.insert_conversation_tree_with_analytics(
            agent_id,
            workspace_id,
            conv,
            defer_analytics_updates_enabled(),
        )
    }

    /// Internal form of [`Self::insert_conversation_tree`] with an explicit
    /// analytics disposition.
    ///
    /// The ambient disposition is process-global because an index run spans
    /// worker threads. Callers that assert on inline analytics side effects
    /// must pass `false` instead of reading state another thread can change.
    pub(crate) fn insert_conversation_tree_with_analytics(
        &self,
        agent_id: i64,
        workspace_id: Option<i64>,
        conv: &Conversation,
        defer_analytics_updates: bool,
    ) -> Result<InsertOutcome> {
        if conv.agent_slug == "codebuff" {
            return self
                .insert_conversations_batched_with_analytics(
                    &[(agent_id, workspace_id, conv)],
                    defer_analytics_updates,
                )?
                .pop()
                .context("Codebuff / Freebuff insert returned no outcome");
        }
        let normalized_conv = normalized_conversation_for_storage(conv);
        let conv = normalized_conv.as_ref();
        self.ensure_source_for_conversation(conv)?;
        let defer_lexical_updates = defer_storage_lexical_updates_enabled();
        let conversation_key = conversation_merge_key(agent_id, conv);
        let mut tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let reconciled_conv = franken_reconcile_native_message_indices(&tx, agent_id, conv)?;
        let conv = reconciled_conv.as_ref();
        let existing = franken_find_existing_conversation_with_tail_by_key(
            &tx,
            &conversation_key,
            Some(conv),
        )?;
        if let Some(existing) = existing {
            let mut outcome = self.franken_append_messages_with_tail_in_tx(
                &tx,
                agent_id,
                existing.id,
                conv,
                existing.tail_state,
                defer_lexical_updates,
                defer_analytics_updates,
            )?;
            outcome.workspace_changed = franken_reconcile_provider_workspace(
                &tx,
                agent_id,
                existing.id,
                workspace_id,
                conv,
            )?;
            franken_reassociate_provider_analytics_workspace(&tx, existing.id, conv)?;
            tx.commit()?;
            return Ok(outcome);
        }

        let conv_id = match franken_insert_conversation_or_get_existing_after_miss(
            &tx,
            agent_id,
            workspace_id,
            conv,
            &conversation_key,
        )? {
            ConversationInsertStatus::Inserted(conv_id) => conv_id,
            ConversationInsertStatus::Existing(existing_id) => {
                let ExistingMessageLookup {
                    by_idx: mut existing_messages,
                    replay: mut existing_replay_fingerprints,
                } = franken_existing_message_lookup(&tx, existing_id, &conv.messages)?;
                let ExistingConversationNewMessages {
                    messages: new_messages,
                    new_chars: _planned_new_chars,
                    idx_collision_count,
                    first_collision_idx,
                } = collect_new_messages_for_existing_conversation(
                    existing_id,
                    conv,
                    &mut existing_messages,
                    &mut existing_replay_fingerprints,
                    "skipping replay-equivalent recovered message with shifted idx",
                );
                let (inserted_last_idx, inserted_last_created_at) =
                    borrowed_messages_tail_state(&new_messages);
                let mut inserted_indices = Vec::new();
                let mut fts_entries = Vec::new();
                let mut fts_pending_chars = 0usize;
                let mut _fts_inserted_total = 0usize;
                let inserted_messages =
                    franken_append_insert_new_messages(&tx, existing_id, &new_messages, conv)?;
                let inserted_chars = inserted_messages
                    .iter()
                    .map(|(_, msg)| msg.content.len() as i64)
                    .sum::<i64>();
                for (msg_id, msg) in inserted_messages {
                    franken_insert_snippets(&tx, msg_id, &msg.snippets)?;
                    if !defer_lexical_updates {
                        fts_entries.push(FtsEntry::from_message(msg_id, msg, conv));
                        fts_pending_chars = fts_pending_chars.saturating_add(msg.content.len());
                        if fts_entries.len() >= FTS_ENTRY_BATCH_MAX_DOCS
                            || fts_pending_chars >= FTS_ENTRY_BATCH_MAX_CHARS
                        {
                            flush_pending_fts_entries(
                                self,
                                &tx,
                                &mut fts_entries,
                                &mut fts_pending_chars,
                                &mut _fts_inserted_total,
                            )?;
                        }
                    }
                    inserted_indices.push(msg.idx);
                }

                if idx_collision_count > 0 {
                    tracing::warn!(
                        conversation_id = existing_id,
                        collision_count = idx_collision_count,
                        first_idx = first_collision_idx,
                        source_path = %conv.source_path.display(),
                        "message idx collisions encountered while merging recovered conversation; retaining canonical message variants"
                    );
                }

                if !defer_lexical_updates {
                    flush_pending_fts_entries(
                        self,
                        &tx,
                        &mut fts_entries,
                        &mut fts_pending_chars,
                        &mut _fts_inserted_total,
                    )?;
                }

                let conv_last_ts = conversation_tail_ended_at_candidate(conv);
                franken_update_conversation_tail_state(
                    &tx,
                    existing_id,
                    conv_last_ts,
                    inserted_last_idx,
                    inserted_last_created_at,
                )?;
                if let Some(lookup_key) = conversation_external_lookup_key_for_conv(agent_id, conv)
                {
                    franken_update_external_conversation_tail_lookup_key(
                        &tx,
                        &lookup_key,
                        conv_last_ts,
                        inserted_last_idx,
                        inserted_last_created_at,
                    )?;
                }

                if !defer_analytics_updates && !inserted_indices.is_empty() {
                    franken_update_daily_stats_in_tx(
                        self,
                        &tx,
                        &conv.agent_slug,
                        &conv.source_id,
                        conversation_effective_started_at(conv),
                        StatsDelta {
                            session_count_delta: 0,
                            message_count_delta: inserted_indices.len() as i64,
                            total_chars_delta: inserted_chars,
                        },
                    )?;
                }

                let workspace_changed = franken_reconcile_provider_workspace(
                    &tx,
                    agent_id,
                    existing_id,
                    workspace_id,
                    conv,
                )?;
                franken_reassociate_provider_analytics_workspace(&tx, existing_id, conv)?;
                tx.commit()?;
                return Ok(InsertOutcome {
                    conversation_id: existing_id,
                    conversation_inserted: false,
                    inserted_indices,
                    updated_indices: Vec::new(),
                    workspace_changed,
                });
            }
        };
        let mut fts_entries = Vec::new();
        let mut fts_pending_chars = 0usize;
        let mut _fts_inserted_total = 0usize;
        let mut total_chars: i64 = 0;
        let mut inserted_indices = Vec::new();
        let mut pending_messages = HashMap::new();
        let mut pending_replay_fingerprints = MessageReplayFingerprintSet::default();
        let mut idx_collision_count = 0usize;
        let mut first_collision_idx: Option<i64> = None;
        let mut new_messages = Vec::new();
        for msg in &conv.messages {
            let incoming_fingerprint = message_merge_fingerprint(msg);
            if let Some(existing_fingerprint) = pending_messages.get(&msg.idx) {
                if existing_fingerprint != &incoming_fingerprint {
                    idx_collision_count = idx_collision_count.saturating_add(1);
                    first_collision_idx.get_or_insert(msg.idx);
                }
                continue;
            }
            let (incoming_core, incoming_native_id) = message_replay_parts(msg);
            if conv.agent_slug != "grok_bot"
                && pending_replay_fingerprints.contains_parts(&incoming_core, &incoming_native_id)
            {
                tracing::debug!(
                    conversation_id = conv_id,
                    idx = msg.idx,
                    source_path = %conv.source_path.display(),
                    "skipping replay-equivalent duplicate message within new conversation insert"
                );
                continue;
            }
            pending_messages.insert(msg.idx, incoming_fingerprint);
            pending_replay_fingerprints.insert_parts(incoming_core, incoming_native_id);
            new_messages.push(msg);
        }
        let inserted_message_ids = franken_batch_insert_new_messages(&tx, conv_id, &new_messages)?;
        for (msg_id, msg) in inserted_message_ids.into_iter().zip(new_messages) {
            franken_insert_snippets(&tx, msg_id, &msg.snippets)?;
            if !defer_lexical_updates {
                fts_entries.push(FtsEntry::from_message(msg_id, msg, conv));
                fts_pending_chars = fts_pending_chars.saturating_add(msg.content.len());
                if fts_entries.len() >= FTS_ENTRY_BATCH_MAX_DOCS
                    || fts_pending_chars >= FTS_ENTRY_BATCH_MAX_CHARS
                {
                    flush_pending_fts_entries(
                        self,
                        &tx,
                        &mut fts_entries,
                        &mut fts_pending_chars,
                        &mut _fts_inserted_total,
                    )?;
                }
            }
            total_chars += msg.content.len() as i64;
            inserted_indices.push(msg.idx);
        }
        if idx_collision_count > 0 {
            tracing::warn!(
                conversation_id = conv_id,
                collision_count = idx_collision_count,
                first_idx = first_collision_idx,
                source_path = %conv.source_path.display(),
                "message idx collisions encountered while inserting a new conversation; retaining the first canonical variant per idx"
            );
        }
        if !defer_lexical_updates {
            flush_pending_fts_entries(
                self,
                &tx,
                &mut fts_entries,
                &mut fts_pending_chars,
                &mut _fts_inserted_total,
            )?;
        }

        if !defer_analytics_updates {
            franken_update_daily_stats_in_tx(
                self,
                &tx,
                &conv.agent_slug,
                &conv.source_id,
                conversation_effective_started_at(conv),
                StatsDelta {
                    session_count_delta: 1,
                    message_count_delta: inserted_indices.len() as i64,
                    total_chars_delta: total_chars,
                },
            )?;
        }

        tx.commit()?;
        Ok(InsertOutcome {
            conversation_id: conv_id,
            conversation_inserted: true,
            inserted_indices,
            updated_indices: Vec::new(),
            workspace_changed: false,
        })
    }

    #[cfg(test)]
    pub(super) fn insert_conversation_tree_with_profile(
        &self,
        agent_id: i64,
        workspace_id: Option<i64>,
        conv: &Conversation,
        profile: &mut InsertConversationTreePerfProfile,
    ) -> Result<InsertOutcome> {
        let total_start = Instant::now();
        let normalized_conv = normalized_conversation_for_storage(conv);
        let conv = normalized_conv.as_ref();

        let source_start = Instant::now();
        self.ensure_source_for_conversation(conv)?;
        profile.source_duration += source_start.elapsed();

        let defer_lexical_updates = defer_storage_lexical_updates_enabled();
        let defer_analytics_updates = defer_analytics_updates_enabled();
        let conversation_key = conversation_merge_key(agent_id, conv);

        let tx_open_start = Instant::now();
        let mut tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        profile.tx_open_duration += tx_open_start.elapsed();

        let reconciled_conv = franken_reconcile_native_message_indices(&tx, agent_id, conv)?;
        let conv = reconciled_conv.as_ref();
        let existing_lookup_start = Instant::now();
        let existing =
            franken_find_existing_conversation_by_key(&tx, &conversation_key, Some(conv))?;
        profile.existing_lookup_duration += existing_lookup_start.elapsed();
        if let Some(existing_id) = existing {
            return Err(anyhow!(
                "profile helper expects new conversation path, found existing id {existing_id}"
            ));
        }

        let conversation_row_start = Instant::now();
        let conv_id = match franken_insert_conversation_or_get_existing_after_miss(
            &tx,
            agent_id,
            workspace_id,
            conv,
            &conversation_key,
        )? {
            ConversationInsertStatus::Inserted(conv_id) => conv_id,
            ConversationInsertStatus::Existing(existing_id) => {
                return Err(anyhow!(
                    "profile helper expected inserted conversation row, reused existing id {existing_id}"
                ));
            }
        };
        profile.conversation_row_duration += conversation_row_start.elapsed();

        let mut fts_entries = Vec::new();
        let mut fts_pending_chars = 0usize;
        let mut fts_inserted_total = 0usize;
        let mut total_chars: i64 = 0;
        let mut inserted_indices = Vec::new();
        let mut pending_messages = HashMap::new();
        let mut pending_replay_fingerprints = MessageReplayFingerprintSet::default();
        let mut idx_collision_count = 0usize;
        let mut first_collision_idx: Option<i64> = None;
        let mut new_messages = Vec::new();

        for msg in &conv.messages {
            let incoming_fingerprint = message_merge_fingerprint(msg);
            if let Some(existing_fingerprint) = pending_messages.get(&msg.idx) {
                if existing_fingerprint != &incoming_fingerprint {
                    idx_collision_count = idx_collision_count.saturating_add(1);
                    first_collision_idx.get_or_insert(msg.idx);
                }
                continue;
            }

            let (incoming_core, incoming_native_id) = message_replay_parts(msg);
            if conv.agent_slug != "grok_bot"
                && pending_replay_fingerprints.contains_parts(&incoming_core, &incoming_native_id)
            {
                tracing::debug!(
                    conversation_id = conv_id,
                    idx = msg.idx,
                    source_path = %conv.source_path.display(),
                    "skipping replay-equivalent duplicate message within profiled new conversation insert"
                );
                continue;
            }

            pending_messages.insert(msg.idx, incoming_fingerprint);
            pending_replay_fingerprints.insert_parts(incoming_core, incoming_native_id);
            new_messages.push(msg);
        }

        let message_insert_start = Instant::now();
        let inserted_message_ids = franken_batch_insert_new_messages_with_profile(
            &tx,
            conv_id,
            &new_messages,
            &mut profile.message_insert_breakdown,
        )?;
        profile.message_insert_duration += message_insert_start.elapsed();

        for (msg_id, msg) in inserted_message_ids.into_iter().zip(new_messages) {
            let snippet_insert_start = Instant::now();
            franken_insert_snippets(&tx, msg_id, &msg.snippets)?;
            profile.snippet_insert_duration += snippet_insert_start.elapsed();

            if !defer_lexical_updates {
                let fts_entry_start = Instant::now();
                fts_entries.push(FtsEntry::from_message(msg_id, msg, conv));
                fts_pending_chars = fts_pending_chars.saturating_add(msg.content.len());
                profile.fts_entry_duration += fts_entry_start.elapsed();
                if fts_entries.len() >= FTS_ENTRY_BATCH_MAX_DOCS
                    || fts_pending_chars >= FTS_ENTRY_BATCH_MAX_CHARS
                {
                    let fts_flush_start = Instant::now();
                    flush_pending_fts_entries(
                        self,
                        &tx,
                        &mut fts_entries,
                        &mut fts_pending_chars,
                        &mut fts_inserted_total,
                    )?;
                    profile.fts_flush_duration += fts_flush_start.elapsed();
                }
            }

            total_chars += msg.content.len() as i64;
            inserted_indices.push(msg.idx);
        }

        if idx_collision_count > 0 {
            tracing::warn!(
                conversation_id = conv_id,
                collision_count = idx_collision_count,
                first_idx = first_collision_idx,
                source_path = %conv.source_path.display(),
                "message idx collisions encountered while profiling a new conversation insert; retaining the first canonical variant per idx"
            );
        }

        if !defer_lexical_updates {
            let fts_flush_start = Instant::now();
            flush_pending_fts_entries(
                self,
                &tx,
                &mut fts_entries,
                &mut fts_pending_chars,
                &mut fts_inserted_total,
            )?;
            profile.fts_flush_duration += fts_flush_start.elapsed();
        }

        if !defer_analytics_updates {
            let analytics_start = Instant::now();
            franken_update_daily_stats_in_tx(
                self,
                &tx,
                &conv.agent_slug,
                &conv.source_id,
                conversation_effective_started_at(conv),
                StatsDelta {
                    session_count_delta: 1,
                    message_count_delta: inserted_indices.len() as i64,
                    total_chars_delta: total_chars,
                },
            )?;
            profile.analytics_duration += analytics_start.elapsed();
        }

        let commit_start = Instant::now();
        tx.commit()?;
        profile.commit_duration += commit_start.elapsed();
        profile.invocations += 1;
        profile.messages += conv.messages.len();
        profile.inserted_messages += inserted_indices.len();
        profile.total_duration += total_start.elapsed();

        Ok(InsertOutcome {
            conversation_id: conv_id,
            conversation_inserted: true,
            inserted_indices,
            updated_indices: Vec::new(),
            workspace_changed: false,
        })
    }

    #[cfg(test)]
    pub(super) fn append_existing_conversation_with_profile(
        &self,
        agent_id: i64,
        _workspace_id: Option<i64>,
        conv: &Conversation,
        profile: &mut InsertConversationTreePerfProfile,
    ) -> Result<InsertOutcome> {
        let total_start = Instant::now();
        let normalized_conv = normalized_conversation_for_storage(conv);
        let conv = normalized_conv.as_ref();

        let source_start = Instant::now();
        self.ensure_source_for_conversation(conv)?;
        profile.source_duration += source_start.elapsed();

        let defer_lexical_updates = defer_storage_lexical_updates_enabled();
        let defer_analytics_updates = defer_analytics_updates_enabled();
        let conversation_key = conversation_merge_key(agent_id, conv);

        let tx_open_start = Instant::now();
        let mut tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        profile.tx_open_duration += tx_open_start.elapsed();

        let reconciled_conv = franken_reconcile_native_message_indices(&tx, agent_id, conv)?;
        let conv = reconciled_conv.as_ref();
        let existing_lookup_start = Instant::now();
        let existing = franken_find_existing_conversation_with_tail_by_key(
            &tx,
            &conversation_key,
            Some(conv),
        )?;
        profile.existing_lookup_duration += existing_lookup_start.elapsed();
        let existing = existing.ok_or_else(|| {
            anyhow!("append profile helper expects existing conversation for {conversation_key:?}")
        })?;
        let existing_id = existing.id;

        let existing_idx_lookup_start = Instant::now();
        let append_tail_state = existing.tail_state;
        let append_tail_ended_at = append_tail_state.and_then(|state| state.ended_at);
        let existing_plan = append_tail_state.as_ref().and_then(|state| {
            collect_append_only_tail_messages(
                conv,
                state.last_message_idx,
                state.last_message_created_at,
            )
        });
        let used_append_tail_plan = existing_plan.is_some();
        profile.existing_idx_lookup_duration += existing_idx_lookup_start.elapsed();

        let dedupe_filter_start = Instant::now();
        let ExistingConversationNewMessages {
            messages: new_messages,
            new_chars,
            idx_collision_count,
            first_collision_idx,
        } = if let Some(existing_plan) = existing_plan {
            existing_plan
        } else {
            let ExistingMessageLookup {
                by_idx: mut existing_messages,
                replay: mut existing_replay_fingerprints,
            } = franken_existing_message_lookup(&tx, existing_id, &conv.messages)?;
            collect_new_messages_for_existing_conversation(
                existing_id,
                conv,
                &mut existing_messages,
                &mut existing_replay_fingerprints,
                "skipping replay-equivalent profiled append message with shifted idx",
            )
        };
        profile.dedupe_filter_duration += dedupe_filter_start.elapsed();

        let mut inserted_indices = Vec::new();
        let mut fts_entries = Vec::new();
        let mut fts_pending_chars = 0usize;
        let mut fts_inserted_total = 0usize;
        let (inserted_last_idx, inserted_last_created_at) =
            borrowed_messages_tail_state(&new_messages);

        let message_insert_start = Instant::now();
        let inserted_message_ids = franken_append_insert_new_messages_with_profile(
            &tx,
            existing_id,
            &new_messages,
            &mut profile.message_insert_breakdown,
        )?;
        profile.message_insert_duration += message_insert_start.elapsed();

        for (msg_id, msg) in inserted_message_ids.into_iter().zip(new_messages) {
            let snippet_insert_start = Instant::now();
            franken_insert_snippets(&tx, msg_id, &msg.snippets)?;
            profile.snippet_insert_duration += snippet_insert_start.elapsed();

            if !defer_lexical_updates {
                let fts_entry_start = Instant::now();
                fts_entries.push(FtsEntry::from_message(msg_id, msg, conv));
                fts_pending_chars = fts_pending_chars.saturating_add(msg.content.len());
                profile.fts_entry_duration += fts_entry_start.elapsed();
                if fts_entries.len() >= FTS_ENTRY_BATCH_MAX_DOCS
                    || fts_pending_chars >= FTS_ENTRY_BATCH_MAX_CHARS
                {
                    let fts_flush_start = Instant::now();
                    flush_pending_fts_entries(
                        self,
                        &tx,
                        &mut fts_entries,
                        &mut fts_pending_chars,
                        &mut fts_inserted_total,
                    )?;
                    profile.fts_flush_duration += fts_flush_start.elapsed();
                }
            }

            inserted_indices.push(msg.idx);
        }

        if idx_collision_count > 0 {
            tracing::warn!(
                conversation_id = existing_id,
                collision_count = idx_collision_count,
                first_idx = first_collision_idx,
                source_path = %conv.source_path.display(),
                "message idx collisions encountered while profiling append merge; retaining canonical message variants"
            );
        }

        if !defer_lexical_updates {
            let fts_flush_start = Instant::now();
            flush_pending_fts_entries(
                self,
                &tx,
                &mut fts_entries,
                &mut fts_pending_chars,
                &mut fts_inserted_total,
            )?;
            profile.fts_flush_duration += fts_flush_start.elapsed();
        }

        let conversation_row_start = Instant::now();
        let mut exact_append_tail_set = false;
        if used_append_tail_plan {
            if let (Some(last_message_idx), Some(last_message_created_at)) =
                (inserted_last_idx, inserted_last_created_at)
            {
                if append_tail_ended_at.is_none_or(|ended_at| ended_at <= last_message_created_at) {
                    franken_set_conversation_tail_state_after_append(
                        &tx,
                        existing_id,
                        last_message_created_at,
                        last_message_idx,
                        last_message_created_at,
                    )?;
                    exact_append_tail_set = true;
                } else {
                    franken_update_conversation_tail_state(
                        &tx,
                        existing_id,
                        Some(last_message_created_at),
                        inserted_last_idx,
                        inserted_last_created_at,
                    )?;
                }
            }
        } else {
            let conv_last_ts = conversation_tail_ended_at_candidate(conv);
            franken_update_conversation_tail_state(
                &tx,
                existing_id,
                conv_last_ts,
                inserted_last_idx,
                inserted_last_created_at,
            )?;
        }
        franken_update_external_conversation_tail_after_append(
            &tx,
            agent_id,
            conv,
            used_append_tail_plan,
            exact_append_tail_set,
            inserted_last_idx,
            inserted_last_created_at,
        )?;
        profile.conversation_row_duration += conversation_row_start.elapsed();

        if !defer_analytics_updates && !inserted_indices.is_empty() {
            let analytics_start = Instant::now();
            franken_update_daily_stats_in_tx(
                self,
                &tx,
                &conv.agent_slug,
                &conv.source_id,
                conversation_effective_started_at(conv),
                StatsDelta {
                    session_count_delta: 0,
                    message_count_delta: inserted_indices.len() as i64,
                    total_chars_delta: new_chars,
                },
            )?;
            profile.analytics_duration += analytics_start.elapsed();
        }

        let commit_start = Instant::now();
        tx.commit()?;
        profile.commit_duration += commit_start.elapsed();
        profile.invocations += 1;
        profile.messages += conv.messages.len();
        profile.inserted_messages += inserted_indices.len();
        profile.total_duration += total_start.elapsed();

        Ok(InsertOutcome {
            conversation_id: existing_id,
            conversation_inserted: false,
            inserted_indices,
            updated_indices: Vec::new(),
            workspace_changed: false,
        })
    }

    /// Append new messages to an existing conversation within an active transaction.
    #[allow(clippy::too_many_arguments)]
    fn franken_append_messages_with_tail_in_tx(
        &self,
        tx: &FrankenTransaction<'_>,
        agent_id: i64,
        conversation_id: i64,
        conv: &Conversation,
        append_tail_state: Option<ExistingConversationTailState>,
        defer_lexical_updates: bool,
        defer_analytics_updates: bool,
    ) -> Result<InsertOutcome> {
        let append_tail_ended_at = append_tail_state.and_then(|state| state.ended_at);
        let append_plan = append_tail_state.as_ref().and_then(|state| {
            collect_append_only_tail_messages(
                conv,
                state.last_message_idx,
                state.last_message_created_at,
            )
        });
        let used_append_tail_plan = append_plan.is_some();
        let ExistingConversationNewMessages {
            messages: new_messages,
            new_chars: _planned_new_chars,
            idx_collision_count,
            first_collision_idx,
        } = if let Some(append_plan) = append_plan {
            append_plan
        } else {
            let ExistingMessageLookup {
                by_idx: mut existing_messages,
                replay: mut existing_replay_fingerprints,
            } = franken_existing_message_lookup(tx, conversation_id, &conv.messages)?;
            collect_new_messages_for_existing_conversation(
                conversation_id,
                conv,
                &mut existing_messages,
                &mut existing_replay_fingerprints,
                "skipping replay-equivalent recovered message with shifted idx",
            )
        };

        let mut inserted_indices = Vec::new();
        let mut fts_entries = Vec::new();
        let mut fts_pending_chars = 0usize;
        let mut _fts_inserted_total = 0usize;
        let (inserted_last_idx, inserted_last_created_at) =
            borrowed_messages_tail_state(&new_messages);
        let inserted_messages =
            franken_append_insert_new_messages(tx, conversation_id, &new_messages, conv)?;
        let inserted_chars = inserted_messages
            .iter()
            .map(|(_, msg)| msg.content.len() as i64)
            .sum::<i64>();
        for (msg_id, msg) in inserted_messages {
            franken_insert_snippets(tx, msg_id, &msg.snippets)?;
            if !defer_lexical_updates {
                fts_entries.push(FtsEntry::from_message(msg_id, msg, conv));
                fts_pending_chars = fts_pending_chars.saturating_add(msg.content.len());
                if fts_entries.len() >= FTS_ENTRY_BATCH_MAX_DOCS
                    || fts_pending_chars >= FTS_ENTRY_BATCH_MAX_CHARS
                {
                    flush_pending_fts_entries(
                        self,
                        tx,
                        &mut fts_entries,
                        &mut fts_pending_chars,
                        &mut _fts_inserted_total,
                    )?;
                }
            }
            inserted_indices.push(msg.idx);
        }

        if idx_collision_count > 0 {
            tracing::warn!(
                conversation_id,
                collision_count = idx_collision_count,
                first_idx = first_collision_idx,
                source_path = %conv.source_path.display(),
                "message idx collisions encountered while appending to an existing conversation; retaining canonical message variants"
            );
        }

        if !defer_lexical_updates {
            flush_pending_fts_entries(
                self,
                tx,
                &mut fts_entries,
                &mut fts_pending_chars,
                &mut _fts_inserted_total,
            )?;
        }

        let mut exact_append_tail_set = false;
        if used_append_tail_plan {
            if let (Some(last_message_idx), Some(last_message_created_at)) =
                (inserted_last_idx, inserted_last_created_at)
            {
                if append_tail_ended_at.is_none_or(|ended_at| ended_at <= last_message_created_at) {
                    franken_set_conversation_tail_state_after_append(
                        tx,
                        conversation_id,
                        last_message_created_at,
                        last_message_idx,
                        last_message_created_at,
                    )?;
                    exact_append_tail_set = true;
                } else {
                    franken_update_conversation_tail_state(
                        tx,
                        conversation_id,
                        Some(last_message_created_at),
                        inserted_last_idx,
                        inserted_last_created_at,
                    )?;
                }
            }
        } else {
            let conv_last_ts = conversation_tail_ended_at_candidate(conv);
            franken_update_conversation_tail_state(
                tx,
                conversation_id,
                conv_last_ts,
                inserted_last_idx,
                inserted_last_created_at,
            )?;
        }
        franken_update_external_conversation_tail_after_append(
            tx,
            agent_id,
            conv,
            used_append_tail_plan,
            exact_append_tail_set,
            inserted_last_idx,
            inserted_last_created_at,
        )?;

        if !defer_analytics_updates && !inserted_indices.is_empty() {
            let message_count = inserted_indices.len() as i64;
            franken_update_daily_stats_in_tx(
                self,
                tx,
                &conv.agent_slug,
                &conv.source_id,
                conversation_effective_started_at(conv),
                StatsDelta {
                    session_count_delta: 0,
                    message_count_delta: message_count,
                    total_chars_delta: inserted_chars,
                },
            )?;
        }

        Ok(InsertOutcome {
            conversation_id,
            conversation_inserted: false,
            inserted_indices,
            updated_indices: Vec::new(),
            workspace_changed: false,
        })
    }
}
