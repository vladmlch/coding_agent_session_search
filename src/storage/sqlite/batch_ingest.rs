use super::aggregators_and_pricing::{
    AnalyticsRollupAggregator, MessageMetricsEntry, PricingDiagnostics, PricingTable,
    StatsAggregator, StatsDelta, TokenStatsAggregator, TokenUsageEntry, has_plan_for_role,
};
use super::claude_usage_dedup;
use super::conversation_tail_state::{
    conversation_merge_key, franken_update_conversation_tail_state,
};
use super::fts_rebuild::{
    FTS_ENTRY_BATCH_MAX_CHARS, FTS_ENTRY_BATCH_MAX_DOCS, FtsEntry, flush_pending_fts_entries,
    role_str,
};
use super::omp_and_semantic_meta::SemanticIdentityTier;
use super::reconcile_and_codebuff::{
    ExistingConversationNewMessages, InsertOutcome, MessageMergeFingerprint,
    MessageReplayFingerprintSet, PendingConversationKey, SourceIngestLedgerEntry,
    borrowed_messages_tail_state, codebuff_tool_count, conversation_effective_started_at,
    conversation_external_lookup_key_for_conv, conversation_tail_ended_at_candidate,
    franken_apply_codebuff_revision_projections, franken_reconcile_codebuff_messages,
    franken_reconcile_native_message_indices, franken_reconcile_provider_workspace,
    message_merge_fingerprint, message_replay_parts, provider_workspace_is_authoritative,
    shelley_metadata_is_authoritative,
};
use super::source_and_historical_import::historical_raw_json;
use super::storage_core::{EnsuredConversationSourceKey, FrankenStorage};
use super::tx_analytics_rollups::{
    TOKEN_DAILY_STATS_REBUILD_CURSOR_KEY, TOKEN_USAGE_REVISION_GENERATION_META_KEY,
    franken_flush_analytics_rollups_in_tx, franken_insert_message_metrics_batched_in_tx,
    franken_insert_token_usage_batched_in_tx, franken_reassociate_provider_analytics_workspace,
    franken_update_conversation_token_summaries_in_tx, franken_update_daily_stats_batched_in_tx,
    franken_update_token_daily_stats_batched_in_tx,
};
use super::tx_conversation_insert::{
    ConversationInsertStatus, defer_analytics_updates_enabled,
    franken_find_existing_conversation_by_key, franken_insert_conversation_or_get_existing,
    franken_update_external_conversation_tail_lookup_key,
};
use super::tx_message_insert::{
    franken_append_insert_new_messages, franken_batch_insert_new_messages,
    franken_collect_batched_existing_new_messages, franken_insert_snippets,
};
use crate::franken_sync::{
    TransactionBehavior,
    compat::{
        ConnectionExt as FrankenConnectionExt, RowExt as FrankenRowExt,
        Transaction as FrankenTransaction, TransactionExt as FrankenTransactionExt,
    },
};
use crate::model::types::{Conversation, Message};
use crate::sources::provenance::{LOCAL_SOURCE_ID, Source, SourceKind};
use anyhow::{Context, Result, bail};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

impl FrankenStorage {
    /// Batch insert multiple conversations with full analytics (token usage,
    /// message metrics, rollups).  Transaction-batched equivalent of
    /// `SqliteStorage::insert_conversations_batched`.
    pub fn insert_conversations_batched(
        &self,
        conversations: &[(i64, Option<i64>, &Conversation)],
    ) -> Result<Vec<InsertOutcome>> {
        self.insert_conversations_batched_with_analytics(
            conversations,
            defer_analytics_updates_enabled(),
        )
    }

    /// Internal form of [`Self::insert_conversations_batched`] with an
    /// explicit analytics disposition; see
    /// [`Self::insert_conversation_tree_with_analytics`] for why callers that
    /// assert on analytics side effects must pass `false` rather than reading
    /// the process-global ambient default.
    pub(crate) fn insert_conversations_batched_with_analytics(
        &self,
        conversations: &[(i64, Option<i64>, &Conversation)],
        defer_analytics_updates: bool,
    ) -> Result<Vec<InsertOutcome>> {
        self.insert_conversations_batched_with_completion(
            conversations,
            defer_analytics_updates,
            None,
        )
    }

    pub fn source_ingest_ledger_entries(&self) -> Result<HashMap<String, String>> {
        Ok(self.conn.query_map_collect(
            "SELECT key, value FROM meta WHERE key >= 'source_ingest_v1:' AND key < 'source_ingest_v1;'",
            fparams![],
            |row| Ok((row.get_typed::<String>(0)?, row.get_typed::<String>(1)?)),
        )?.into_iter().collect())
    }

    /// Record a fully observed source whose final batch was intentionally
    /// excluded by workspace policy or explicit forget tombstones. This is
    /// separate from canonical batch completion: an arbitrary empty/failed
    /// parser batch must still never advance a source's observation ledger.
    pub(crate) fn record_filtered_source_completion(
        &self,
        completion: &SourceIngestLedgerEntry,
    ) -> Result<()> {
        if !completion.key.starts_with("source_ingest_v1:")
            || completion.key.len() == "source_ingest_v1:".len()
        {
            bail!("source completion ledger key must name a source_ingest_v1 observation");
        }
        let mut tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute_compat(
            "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
            fparams![completion.key.as_str(), completion.observation.as_str()],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn insert_conversations_batched_with_source_completion(
        &self,
        conversations: &[(i64, Option<i64>, &Conversation)],
        completion: &SourceIngestLedgerEntry,
    ) -> Result<Vec<InsertOutcome>> {
        self.insert_conversations_batched_with_completion(
            conversations,
            defer_analytics_updates_enabled(),
            Some(completion),
        )
    }

    fn insert_conversations_batched_with_completion(
        &self,
        conversations: &[(i64, Option<i64>, &Conversation)],
        defer_analytics_updates: bool,
        completion: Option<&SourceIngestLedgerEntry>,
    ) -> Result<Vec<InsertOutcome>> {
        if let Some(completion) = completion
            && (!completion.key.starts_with("source_ingest_v1:")
                || completion.key.len() == "source_ingest_v1:".len())
        {
            bail!("source completion ledger key must name a source_ingest_v1 observation");
        }
        if conversations.is_empty() && completion.is_some() {
            bail!("source completion requires a final canonical conversation batch");
        }
        if conversations.is_empty() && completion.is_none() {
            return Ok(Vec::new());
        }

        self.ensure_sources_for_batch(conversations)?;

        let defer_lexical_updates = defer_storage_lexical_updates_enabled();
        let pricing_table = PricingTable::franken_load(&self.conn).unwrap_or_else(|e| {
            tracing::warn!(target: "cass::analytics::pricing", error = %e, "failed to load pricing table");
            PricingTable { entries: Vec::new() }
        });
        let mut pricing_diag = PricingDiagnostics::default();

        let mut tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;

        // Ensure all referenced agents, workspaces, and sources exist inside
        // the transaction so PRAGMA foreign_keys = ON checks pass even when the
        // in-memory ensured_* cache is stale (e.g. after purge/forget) or a
        // caller passes numeric IDs directly.
        ensure_agents_in_tx(&tx, conversations)?;
        ensure_workspaces_in_tx(&tx, conversations)?;
        ensure_sources_in_tx(&tx, conversations)?;

        let mut outcomes = Vec::with_capacity(conversations.len());
        let mut codebuff_revisions = Vec::new();
        let mut fts_entries = Vec::new();
        let mut fts_pending_chars = 0usize;
        let mut fts_inserted_total = 0usize;
        let mut fts_count_total = 0usize;
        let mut stats = StatsAggregator::new();
        let mut token_stats = TokenStatsAggregator::new();
        let mut token_entries: Vec<TokenUsageEntry> = Vec::new();
        let mut metrics_entries: Vec<MessageMetricsEntry> = Vec::new();
        let mut rollup_agg = AnalyticsRollupAggregator::new();
        let mut conv_ids_to_summarize: Vec<i64> = Vec::new();
        let mut pending_conversation_ids: HashMap<PendingConversationKey, i64> = HashMap::new();
        let mut pending_message_fingerprints: HashMap<i64, HashMap<i64, MessageMergeFingerprint>> =
            HashMap::new();
        let mut pending_message_replay_fingerprints: HashMap<i64, MessageReplayFingerprintSet> =
            HashMap::new();

        for &(agent_id, workspace_id, raw_conv) in conversations {
            let normalized_conv = normalized_conversation_for_storage(raw_conv);
            let revision_start = codebuff_revisions.len();
            let codebuff_conv = franken_reconcile_codebuff_messages(
                &tx,
                agent_id,
                normalized_conv.as_ref(),
                &mut codebuff_revisions,
            )?;
            let updated_indices = codebuff_revisions[revision_start..]
                .iter()
                .map(|revision| revision.message.idx)
                .collect();
            let reconciled_conv =
                franken_reconcile_native_message_indices(&tx, agent_id, codebuff_conv.as_ref())?;
            let conv = reconciled_conv.as_ref();
            let mut total_chars: i64 = 0;
            let mut inserted_indices = Vec::with_capacity(conv.messages.len());
            let mut inserted_messages: Vec<(i64, &Message)> =
                Vec::with_capacity(conv.messages.len());
            let mut session_count_delta = 1_i64;
            let conversation_key = conversation_merge_key(agent_id, conv);

            let existing_conv_id = if let Some(existing_id) =
                pending_conversation_ids.get(&conversation_key)
            {
                Some(*existing_id)
            } else {
                let existing_id =
                    franken_find_existing_conversation_by_key(&tx, &conversation_key, Some(conv))?;
                if let Some(existing_id) = existing_id {
                    pending_conversation_ids.insert(conversation_key.clone(), existing_id);
                }
                existing_id
            };

            let conv_id = if let Some(existing_id) = existing_conv_id {
                session_count_delta = 0;
                let (
                    ExistingConversationNewMessages {
                        messages: new_messages,
                        new_chars: _planned_new_chars,
                        idx_collision_count,
                        first_collision_idx,
                    },
                    existing_messages,
                    existing_replay_fingerprints,
                ) = franken_collect_batched_existing_new_messages(
                    &tx,
                    existing_id,
                    conv,
                    &mut pending_message_fingerprints,
                    &mut pending_message_replay_fingerprints,
                    "skipping replay-equivalent recovered message with shifted idx during batched merge",
                )?;
                let (inserted_last_idx, inserted_last_created_at) =
                    borrowed_messages_tail_state(&new_messages);
                let inserted_append_messages =
                    franken_append_insert_new_messages(&tx, existing_id, &new_messages, conv)?;
                total_chars += inserted_append_messages
                    .iter()
                    .map(|(_, msg)| msg.content.len() as i64)
                    .sum::<i64>();
                for (msg_id, msg) in inserted_append_messages {
                    franken_insert_snippets(&tx, msg_id, &msg.snippets)?;
                    if !defer_lexical_updates {
                        fts_entries.push(FtsEntry::from_message(msg_id, msg, conv));
                        fts_count_total += 1;
                        fts_pending_chars = fts_pending_chars.saturating_add(msg.content.len());
                        if fts_entries.len() >= FTS_ENTRY_BATCH_MAX_DOCS
                            || fts_pending_chars >= FTS_ENTRY_BATCH_MAX_CHARS
                        {
                            flush_pending_fts_entries(
                                self,
                                &tx,
                                &mut fts_entries,
                                &mut fts_pending_chars,
                                &mut fts_inserted_total,
                            )?;
                        }
                    }
                    inserted_indices.push(msg.idx);
                    inserted_messages.push((msg_id, msg));
                }

                if idx_collision_count > 0 {
                    tracing::warn!(
                        conversation_id = existing_id,
                        collision_count = idx_collision_count,
                        first_idx = first_collision_idx,
                        source_path = %conv.source_path.display(),
                        "message idx collisions encountered during batched conversation merge; retaining canonical message variants"
                    );
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

                pending_message_fingerprints.insert(existing_id, existing_messages);
                pending_message_replay_fingerprints
                    .insert(existing_id, existing_replay_fingerprints);

                existing_id
            } else {
                match franken_insert_conversation_or_get_existing(
                    &tx,
                    agent_id,
                    workspace_id,
                    conv,
                )? {
                    ConversationInsertStatus::Inserted(new_conv_id) => {
                        pending_conversation_ids.insert(conversation_key.clone(), new_conv_id);
                        let pending_messages =
                            pending_message_fingerprints.entry(new_conv_id).or_default();
                        let pending_replay_fingerprints = pending_message_replay_fingerprints
                            .entry(new_conv_id)
                            .or_default();
                        let mut new_messages = Vec::new();
                        for msg in &conv.messages {
                            let (incoming_core, incoming_native_id) = message_replay_parts(msg);
                            if pending_messages.contains_key(&msg.idx)
                                || (!matches!(conv.agent_slug.as_str(), "grok_bot" | "codebuff")
                                    && pending_replay_fingerprints
                                        .contains_parts(&incoming_core, &incoming_native_id))
                            {
                                continue;
                            }
                            pending_messages.insert(msg.idx, message_merge_fingerprint(msg));
                            pending_replay_fingerprints
                                .insert_parts(incoming_core, incoming_native_id);
                            new_messages.push(msg);
                        }
                        let inserted_message_ids =
                            franken_batch_insert_new_messages(&tx, new_conv_id, &new_messages)?;
                        for (msg_id, msg) in inserted_message_ids.into_iter().zip(new_messages) {
                            franken_insert_snippets(&tx, msg_id, &msg.snippets)?;
                            if !defer_lexical_updates {
                                fts_entries.push(FtsEntry::from_message(msg_id, msg, conv));
                                fts_count_total += 1;
                                fts_pending_chars =
                                    fts_pending_chars.saturating_add(msg.content.len());
                                if fts_entries.len() >= FTS_ENTRY_BATCH_MAX_DOCS
                                    || fts_pending_chars >= FTS_ENTRY_BATCH_MAX_CHARS
                                {
                                    flush_pending_fts_entries(
                                        self,
                                        &tx,
                                        &mut fts_entries,
                                        &mut fts_pending_chars,
                                        &mut fts_inserted_total,
                                    )?;
                                }
                            }
                            total_chars += msg.content.len() as i64;
                            inserted_indices.push(msg.idx);
                            inserted_messages.push((msg_id, msg));
                        }
                        new_conv_id
                    }
                    ConversationInsertStatus::Existing(existing_id) => {
                        session_count_delta = 0;
                        pending_conversation_ids.insert(conversation_key.clone(), existing_id);
                        let (
                            ExistingConversationNewMessages {
                                messages: new_messages,
                                new_chars: _planned_new_chars,
                                idx_collision_count,
                                first_collision_idx,
                            },
                            existing_messages,
                            existing_replay_fingerprints,
                        ) = franken_collect_batched_existing_new_messages(
                            &tx,
                            existing_id,
                            conv,
                            &mut pending_message_fingerprints,
                            &mut pending_message_replay_fingerprints,
                            "skipping replay-equivalent recovered message with shifted idx after duplicate conversation recovery",
                        )?;
                        let (inserted_last_idx, inserted_last_created_at) =
                            borrowed_messages_tail_state(&new_messages);
                        let inserted_append_messages = franken_append_insert_new_messages(
                            &tx,
                            existing_id,
                            &new_messages,
                            conv,
                        )?;
                        total_chars += inserted_append_messages
                            .iter()
                            .map(|(_, msg)| msg.content.len() as i64)
                            .sum::<i64>();
                        for (msg_id, msg) in inserted_append_messages {
                            franken_insert_snippets(&tx, msg_id, &msg.snippets)?;
                            if !defer_lexical_updates {
                                fts_entries.push(FtsEntry::from_message(msg_id, msg, conv));
                                fts_count_total += 1;
                                fts_pending_chars =
                                    fts_pending_chars.saturating_add(msg.content.len());
                                if fts_entries.len() >= FTS_ENTRY_BATCH_MAX_DOCS
                                    || fts_pending_chars >= FTS_ENTRY_BATCH_MAX_CHARS
                                {
                                    flush_pending_fts_entries(
                                        self,
                                        &tx,
                                        &mut fts_entries,
                                        &mut fts_pending_chars,
                                        &mut fts_inserted_total,
                                    )?;
                                }
                            }
                            inserted_indices.push(msg.idx);
                            inserted_messages.push((msg_id, msg));
                        }

                        if idx_collision_count > 0 {
                            tracing::warn!(
                                conversation_id = existing_id,
                                collision_count = idx_collision_count,
                                first_idx = first_collision_idx,
                                source_path = %conv.source_path.display(),
                                "message idx collisions encountered after duplicate conversation recovery; retaining canonical message variants"
                            );
                        }

                        let conv_last_ts = conversation_tail_ended_at_candidate(conv);
                        franken_update_conversation_tail_state(
                            &tx,
                            existing_id,
                            conv_last_ts,
                            inserted_last_idx,
                            inserted_last_created_at,
                        )?;
                        if let Some(lookup_key) =
                            conversation_external_lookup_key_for_conv(agent_id, conv)
                        {
                            franken_update_external_conversation_tail_lookup_key(
                                &tx,
                                &lookup_key,
                                conv_last_ts,
                                inserted_last_idx,
                                inserted_last_created_at,
                            )?;
                        }

                        pending_message_fingerprints.insert(existing_id, existing_messages);
                        pending_message_replay_fingerprints
                            .insert(existing_id, existing_replay_fingerprints);

                        existing_id
                    }
                }
            };

            // Codebuff analytics start on the canonical row's workspace. A
            // changed run-state workspace is reconciled below, and the
            // reassociation pass after the flush relocates these rows.
            let analytics_workspace_id = if conv.agent_slug == "codebuff" {
                tx.query_row_map(
                    "SELECT workspace_id FROM conversations WHERE id = ?1",
                    fparams![conv_id],
                    |row| row.get_typed::<Option<i64>>(0),
                )?
            } else {
                workspace_id
            };
            if !defer_analytics_updates
                && (conv.agent_slug != "codebuff"
                    || session_count_delta > 0
                    || !inserted_messages.is_empty())
            {
                let delta = StatsDelta {
                    session_count_delta,
                    message_count_delta: inserted_messages.len() as i64,
                    total_chars_delta: total_chars,
                };

                let effective_started_at = conversation_effective_started_at(conv);
                let day_id = effective_started_at
                    .map(FrankenStorage::day_id_from_millis)
                    .unwrap_or(0);
                stats.record_delta(
                    &conv.agent_slug,
                    &conv.source_id,
                    day_id,
                    delta.session_count_delta,
                    delta.message_count_delta,
                    delta.total_chars_delta,
                );

                let conv_day_id = day_id;
                let mut session_model_family = String::from("unknown");
                let mut has_any_tokens = false;

                // Local fix (review 2026-11-02, U1): count a Claude reply once.
                let mut claude_usage = claude_usage_dedup::ClaudeReplyUsageDedup::default();
                if conv.agent_slug == "claude_code" {
                    let inserted_idx: std::collections::HashSet<i64> =
                        inserted_messages.iter().map(|(_, m)| m.idx).collect();
                    for stored in conv
                        .messages
                        .iter()
                        .filter(|m| !inserted_idx.contains(&m.idx))
                    {
                        let stored_usage = crate::connectors::extract_tokens_for_agent(
                            &conv.agent_slug,
                            &stored.extra_json,
                            &stored.content,
                            &role_str(&stored.role),
                        );
                        claude_usage.observe_stored(
                            conv_id,
                            &stored.extra_json,
                            stored_usage.output_tokens,
                        );
                    }
                }

                for &(message_id, msg) in &inserted_messages {
                    let role_s = role_str(&msg.role);
                    let mut usage = if historical_raw_json(&msg.extra_json).is_some() {
                        crate::connectors::extract_tokens_for_agent(
                            &conv.agent_slug,
                            &serde_json::Value::Null,
                            &msg.content,
                            &role_s,
                        )
                    } else {
                        crate::connectors::extract_tokens_for_agent(
                            &conv.agent_slug,
                            &msg.extra_json,
                            &msg.content,
                            &role_s,
                        )
                    };
                    if conv.agent_slug == "codebuff" {
                        usage.tool_call_count = codebuff_tool_count(&msg.extra_json);
                        usage.has_tool_calls = usage.tool_call_count > 0;
                    }
                    if conv.agent_slug == "claude_code" {
                        claude_usage.apply(conv_id, &msg.extra_json, &mut usage);
                    }

                    let msg_ts = msg
                        .created_at
                        .or(conversation_effective_started_at(conv))
                        .unwrap_or(0);
                    let msg_day_id = if msg_ts > 0 {
                        FrankenStorage::day_id_from_millis(msg_ts)
                    } else {
                        conv_day_id
                    };

                    let model_info = usage
                        .model_name
                        .as_deref()
                        .map(crate::connectors::normalize_model);

                    let model_family = model_info
                        .as_ref()
                        .map(|i| i.family.clone())
                        .unwrap_or_else(|| "unknown".into());
                    let model_tier = model_info
                        .as_ref()
                        .map(|i| i.tier.clone())
                        .unwrap_or_else(|| "unknown".into());
                    let provider = usage
                        .provider
                        .clone()
                        .or_else(|| model_info.as_ref().map(|i| i.provider.clone()))
                        .unwrap_or_else(|| "unknown".into());

                    if model_family != "unknown" {
                        session_model_family = model_family.clone();
                    }

                    let estimated_cost = pricing_table.compute_cost(
                        usage.model_name.as_deref(),
                        msg_day_id,
                        usage.input_tokens,
                        usage.output_tokens,
                        usage.cache_read_tokens,
                        usage.cache_creation_tokens,
                    );
                    if estimated_cost.is_some() {
                        pricing_diag.record_priced();
                    } else if usage.has_token_data() {
                        pricing_diag.record_unpriced(usage.model_name.as_deref());
                    }

                    token_stats.record(
                        &conv.agent_slug,
                        &conv.source_id,
                        msg_day_id,
                        &model_family,
                        &role_s,
                        &usage,
                        msg.content.len() as i64,
                        estimated_cost.unwrap_or(0.0),
                    );

                    if usage.has_token_data() {
                        has_any_tokens = true;
                    }

                    let content_chars = msg.content.len() as i64;
                    let content_tokens_est = content_chars / 4;
                    let msg_hour_id = FrankenStorage::hour_id_from_millis(msg_ts);
                    let has_plan = has_plan_for_role(&role_s, &msg.content);

                    token_entries.push(TokenUsageEntry {
                        message_id,
                        conversation_id: conv_id,
                        agent_id,
                        workspace_id: analytics_workspace_id,
                        source_id: conv.source_id.clone(),
                        timestamp_ms: msg_ts,
                        day_id: msg_day_id,
                        model_name: usage.model_name.clone(),
                        model_family: Some(model_family.clone()),
                        model_tier: Some(model_tier.clone()),
                        service_tier: usage.service_tier.clone(),
                        provider: Some(provider.clone()),
                        input_tokens: usage.input_tokens,
                        output_tokens: usage.output_tokens,
                        cache_read_tokens: usage.cache_read_tokens,
                        cache_creation_tokens: usage.cache_creation_tokens,
                        thinking_tokens: usage.thinking_tokens,
                        total_tokens: usage.total_tokens(),
                        estimated_cost_usd: estimated_cost,
                        role: role_s.to_string(),
                        content_chars,
                        has_tool_calls: usage.has_tool_calls,
                        tool_call_count: usage.tool_call_count,
                        data_source: usage.data_source.as_str().to_string(),
                    });

                    let mm = MessageMetricsEntry {
                        message_id,
                        created_at_ms: msg_ts,
                        hour_id: msg_hour_id,
                        day_id: msg_day_id,
                        agent_slug: conv.agent_slug.clone(),
                        workspace_id: analytics_workspace_id.unwrap_or(0),
                        source_id: conv.source_id.clone(),
                        role: role_s.to_string(),
                        content_chars,
                        content_tokens_est,
                        model_name: usage.model_name.clone(),
                        model_family: model_family.clone(),
                        model_tier: model_tier.clone(),
                        provider,
                        api_input_tokens: usage.input_tokens,
                        api_output_tokens: usage.output_tokens,
                        api_cache_read_tokens: usage.cache_read_tokens,
                        api_cache_creation_tokens: usage.cache_creation_tokens,
                        api_thinking_tokens: usage.thinking_tokens,
                        api_service_tier: usage.service_tier.clone(),
                        api_data_source: usage.data_source.as_str().to_string(),
                        tool_call_count: usage.tool_call_count as i64,
                        has_tool_calls: usage.has_tool_calls,
                        has_plan,
                    };
                    rollup_agg.record(&mm);
                    metrics_entries.push(mm);
                }

                if session_count_delta > 0 {
                    token_stats.record_session(
                        &conv.agent_slug,
                        &conv.source_id,
                        conv_day_id,
                        &session_model_family,
                    );
                }

                if has_any_tokens {
                    conv_ids_to_summarize.push(conv_id);
                }
            }

            outcomes.push(InsertOutcome {
                conversation_id: conv_id,
                conversation_inserted: session_count_delta > 0,
                inserted_indices,
                updated_indices,
                // The packet as received, not `conv`: for an existing Codebuff
                // chat that carries the canonical source path, title and
                // workspace so its FTS rows stay canonical, which would hide
                // both a changed run-state and an alias source.
                workspace_changed: franken_reconcile_provider_workspace(
                    &tx,
                    agent_id,
                    conv_id,
                    workspace_id,
                    normalized_conv.as_ref(),
                )?,
            });
        }

        // Batch insert all FTS entries at once
        if !defer_lexical_updates {
            flush_pending_fts_entries(
                self,
                &tx,
                &mut fts_entries,
                &mut fts_pending_chars,
                &mut fts_inserted_total,
            )?;
        }
        if !defer_lexical_updates && fts_count_total > 0 {
            tracing::debug!(
                target: "cass::perf::fts5",
                total = fts_count_total,
                inserted = fts_inserted_total,
                conversations = conversations.len(),
                "franken_batch_fts_insert_complete"
            );
        }

        // Batched daily_stats update
        if !defer_analytics_updates && !stats.is_empty() {
            let entries = stats.expand();
            let affected = franken_update_daily_stats_batched_in_tx(&tx, &entries)?;
            tracing::debug!(
                target: "cass::perf::daily_stats",
                raw = stats.raw_entry_count(),
                expanded = entries.len(),
                affected = affected,
                "franken_batched_stats_update_complete"
            );
        }

        // Batch insert token_usage rows
        if !defer_analytics_updates && !token_entries.is_empty() {
            let token_count = token_entries.len();
            let inserted = franken_insert_token_usage_batched_in_tx(&tx, &token_entries)?;
            tracing::debug!(
                target: "cass::perf::token_usage",
                total = token_count,
                inserted = inserted,
                "franken_batch_token_usage_insert_complete"
            );
        }

        // Batched token_daily_stats update
        if !defer_analytics_updates && !token_stats.is_empty() {
            let entries = token_stats.expand();
            let affected = franken_update_token_daily_stats_batched_in_tx(&tx, &entries)?;
            tracing::debug!(
                target: "cass::perf::token_daily_stats",
                raw = token_stats.raw_entry_count(),
                expanded = entries.len(),
                affected = affected,
                "franken_batched_token_stats_update_complete"
            );
        }

        // Batch insert message_metrics rows
        if !defer_analytics_updates && !metrics_entries.is_empty() {
            let mm_count = metrics_entries.len();
            let inserted = franken_insert_message_metrics_batched_in_tx(&tx, &metrics_entries)?;
            tracing::debug!(
                target: "cass::perf::message_metrics",
                total = mm_count,
                inserted = inserted,
                "franken_batch_message_metrics_insert_complete"
            );
        }

        // Flush usage_hourly + usage_daily rollups
        if !defer_analytics_updates && !rollup_agg.is_empty() {
            let (hourly, daily, models_daily) =
                franken_flush_analytics_rollups_in_tx(&tx, &rollup_agg)?;
            tracing::debug!(
                target: "cass::perf::usage_rollups",
                hourly_buckets = rollup_agg.hourly_entry_count(),
                daily_buckets = rollup_agg.daily_entry_count(),
                models_daily_buckets = rollup_agg.models_daily_entry_count(),
                hourly_affected = hourly,
                daily_affected = daily,
                models_daily_affected = models_daily,
                "franken_batched_usage_rollups_complete"
            );
        }

        // Update conversation-level token summaries
        if !defer_analytics_updates {
            for conv_id in &conv_ids_to_summarize {
                franken_update_conversation_token_summaries_in_tx(&tx, *conv_id)?;
            }
        }

        // Canonical rows changed during planning so later packets saw their
        // latest payload. Apply derived revisions after first-packet buffers:
        // otherwise buffered FTS/analytics could resurrect the older content.
        for revision in &codebuff_revisions {
            franken_apply_codebuff_revision_projections(
                self,
                &tx,
                revision,
                defer_lexical_updates,
            )?;
        }
        if !codebuff_revisions.is_empty() {
            // Revisions do not advance the rowid watermark. A fresh generation
            // also invalidates checkpoints from an interrupted earlier revision.
            let generation = format!("message_revision:{:032x}", rand::random::<u128>());
            for tier in [SemanticIdentityTier::Fast, SemanticIdentityTier::Quality] {
                tx.execute_compat(
                    "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
                    fparams![tier.meta_key(), generation.as_str()],
                )?;
            }
            tx.execute("DELETE FROM meta WHERE key = 'last_embedded_message_id'")?;
            tx.execute_compat(
                "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
                fparams![
                    TOKEN_USAGE_REVISION_GENERATION_META_KEY,
                    generation.as_str()
                ],
            )?;
            tx.execute_compat(
                "DELETE FROM meta WHERE key = ?1",
                fparams![TOKEN_DAILY_STATS_REBUILD_CURSOR_KEY],
            )?;
        }

        // Repeated packets can change a conversation's workspace more than
        // once while its new analytics rows are still buffered. Relocate only
        // after those buffers are flushed, using the final canonical identity.
        let mut reassociated = HashSet::new();
        for ((_, _, conv), outcome) in conversations.iter().zip(&outcomes) {
            if conv.external_id.is_some()
                && (shelley_metadata_is_authoritative(conv)
                    || provider_workspace_is_authoritative(
                        &conv.agent_slug,
                        conv.workspace.as_deref(),
                        &conv.metadata_json,
                    ))
                && reassociated.insert(outcome.conversation_id)
            {
                franken_reassociate_provider_analytics_workspace(
                    &tx,
                    outcome.conversation_id,
                    conv,
                )?;
            }
        }

        if let Some(completion) = completion {
            tx.execute_compat(
                "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
                fparams![completion.key.as_str(), completion.observation.as_str()],
            )?;
        }
        tx.commit()?;

        pricing_diag.log_summary();

        Ok(outcomes)
    }
}

pub(super) fn normalized_storage_source_parts(
    source_id: Option<&str>,
    origin_kind: Option<&str>,
    origin_host: Option<&str>,
) -> (String, SourceKind, Option<String>) {
    let host_label = crate::search::tantivy::normalized_index_origin_host(origin_host);
    let source_id = crate::search::tantivy::normalized_index_source_id(
        source_id,
        origin_kind,
        host_label.as_deref(),
    );
    let normalized_kind =
        crate::search::tantivy::normalized_index_origin_kind(source_id.as_str(), origin_kind);
    if normalized_kind == LOCAL_SOURCE_ID {
        (source_id, SourceKind::Local, None)
    } else {
        (source_id, SourceKind::Ssh, host_label)
    }
}

fn conversation_origin_kind(conv: &Conversation) -> Option<&str> {
    conv.metadata_json
        .pointer("/cass/origin/kind")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|kind| !kind.is_empty())
}

pub(super) fn normalized_source_for_conversation(conv: &Conversation) -> Source {
    let (id, kind, host_label) = normalized_storage_source_parts(
        Some(conv.source_id.as_str()),
        conversation_origin_kind(conv),
        conv.origin_host.as_deref(),
    );
    Source {
        id,
        kind,
        host_label,
        machine_id: None,
        platform: None,
        config_json: None,
        created_at: None,
        updated_at: None,
    }
}

fn is_bootstrap_local_source(source: &Source) -> bool {
    source.id == LOCAL_SOURCE_ID
        && matches!(source.kind, SourceKind::Local)
        && source.host_label.is_none()
        && source.machine_id.is_none()
        && source.platform.is_none()
        && source.config_json.is_none()
        && source.created_at.is_none()
        && source.updated_at.is_none()
}

pub(super) fn normalized_conversation_for_storage<'a>(
    conv: &'a Conversation,
) -> Cow<'a, Conversation> {
    let normalized_source = normalized_source_for_conversation(conv);
    if normalized_source.id == conv.source_id && normalized_source.host_label == conv.origin_host {
        Cow::Borrowed(conv)
    } else {
        let mut normalized = conv.clone();
        normalized.source_id = normalized_source.id;
        normalized.origin_host = normalized_source.host_label;
        Cow::Owned(normalized)
    }
}

impl FrankenStorage {
    fn ensure_normalized_source_for_conversation(
        &self,
        conv: &Conversation,
        mut source: Source,
    ) -> Result<()> {
        if is_bootstrap_local_source(&source) {
            // `open()` and schema repair always seed the canonical local source row.
            // Avoid an autocommit UPDATE on every local conversation insert.
            return Ok(());
        }
        let cache_key = EnsuredConversationSourceKey::from_source(&source);
        if self.conversation_source_already_ensured(&cache_key) {
            return Ok(());
        }
        if let Some(existing) = self.get_source(source.id.as_str())? {
            if conversation_origin_kind(conv).is_none() {
                // A conversation without explicit provenance cannot supersede the
                // authoritative registry classification. This matters for legacy
                // archive imports whose `sources` rows carry kind but whose older
                // conversation metadata does not.
                self.mark_conversation_source_ensured(EnsuredConversationSourceKey::from_source(
                    &existing,
                ));
                return Ok(());
            }
            if source.machine_id.is_none() {
                source.machine_id = existing.machine_id.clone();
            }
            if source.platform.is_none() {
                source.platform = existing.platform.clone();
            }
            if source.config_json.is_none() {
                source.config_json = existing.config_json.clone();
            }
            if source.kind == existing.kind
                && source.host_label == existing.host_label
                && source.machine_id == existing.machine_id
                && source.platform == existing.platform
                && source.config_json == existing.config_json
            {
                self.mark_conversation_source_ensured(EnsuredConversationSourceKey::from_source(
                    &existing,
                ));
                return Ok(());
            }
        }
        self.upsert_source(&source)?;
        self.mark_conversation_source_ensured(EnsuredConversationSourceKey::from_source(&source));
        Ok(())
    }

    pub(super) fn ensure_source_for_conversation(&self, conv: &Conversation) -> Result<()> {
        let source = normalized_source_for_conversation(conv);
        self.ensure_normalized_source_for_conversation(conv, source)
    }

    fn ensure_sources_for_batch(
        &self,
        conversations: &[(i64, Option<i64>, &Conversation)],
    ) -> Result<()> {
        let mut seen = HashSet::with_capacity(conversations.len());
        for &(_, _, conv) in conversations {
            let source = normalized_source_for_conversation(conv);
            if seen.insert(source.id.clone()) {
                self.ensure_normalized_source_for_conversation(conv, source)?;
            }
        }
        Ok(())
    }
}

// =========================================================================
// FrankenStorage transaction helper functions
// =========================================================================

/// Get last_insert_rowid from a storage transaction.
pub(super) fn franken_last_rowid(tx: &FrankenTransaction<'_>) -> Result<i64> {
    tx.last_insert_rowid()
        .ok()
        .filter(|&id| id > 0)
        .with_context(|| "last_insert_rowid() returned NULL or 0 after INSERT")
}

/// Ensure all agents referenced by a batch exist within the transaction.
/// Acts as an in-tx guard for `PRAGMA foreign_keys = ON` when the caller's
/// cached IDs are stale (e.g. after purge/forget) or numeric IDs are passed
/// directly, checking each unique `agent_id` and inserting a stub row if
/// missing.
pub(super) fn ensure_agents_in_tx(
    tx: &FrankenTransaction<'_>,
    conversations: &[(i64, Option<i64>, &Conversation)],
) -> Result<()> {
    let mut seen = HashSet::new();
    let now = FrankenStorage::now_millis();
    for &(agent_id, _, conv) in conversations {
        if !seen.insert(agent_id) {
            continue;
        }
        let exists: i64 = tx.query_row_map(
            "SELECT COUNT(*) FROM agents WHERE id = ?1",
            fparams![agent_id],
            |row| row.get_typed(0),
        )?;
        if exists == 0 {
            tracing::debug!(
                target: "cass::fk_guard",
                agent_id,
                slug = %conv.agent_slug,
                "inserting agent row inside transaction to satisfy FK constraint"
            );
            // INSERT OR IGNORE: the slug might already exist with a different
            // id from a concurrent writer.  If the slug row exists, the FK
            // constraint is already satisfied (the caller just got a stale id).
            tx.execute_compat(
                "INSERT OR IGNORE INTO agents(id, slug, name, kind, created_at, updated_at)
                 VALUES(?1, ?2, ?3, 'cli', ?4, ?5)",
                fparams![
                    agent_id,
                    conv.agent_slug.as_str(),
                    conv.agent_slug.as_str(),
                    now,
                    now
                ],
            )?;
        }
    }
    Ok(())
}

/// Ensure all workspaces referenced by a batch exist within the
/// transaction.  Same rationale as `ensure_agents_in_tx`.
pub(super) fn ensure_workspaces_in_tx(
    tx: &FrankenTransaction<'_>,
    conversations: &[(i64, Option<i64>, &Conversation)],
) -> Result<()> {
    let mut seen = HashSet::new();
    for &(_, workspace_id, conv) in conversations {
        let ws_id = match workspace_id {
            Some(id) => id,
            None => continue,
        };
        if !seen.insert(ws_id) {
            continue;
        }
        let exists: i64 = tx.query_row_map(
            "SELECT COUNT(*) FROM workspaces WHERE id = ?1",
            fparams![ws_id],
            |row| row.get_typed(0),
        )?;
        if exists == 0 {
            let path_str = conv
                .workspace
                .as_ref()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_default();
            tracing::debug!(
                target: "cass::fk_guard",
                workspace_id = ws_id,
                path = %path_str,
                "inserting workspace row inside transaction to satisfy FK constraint"
            );
            tx.execute_compat(
                "INSERT OR IGNORE INTO workspaces(id, path) VALUES(?1, ?2)",
                fparams![ws_id, path_str.as_str()],
            )?;
        }
    }
    Ok(())
}

/// Ensure all sources referenced by a batch exist within the
/// transaction.  Same rationale as `ensure_agents_in_tx` — `source_id` is a
/// TEXT FK on the `conversations` table.
pub(super) fn ensure_sources_in_tx(
    tx: &FrankenTransaction<'_>,
    conversations: &[(i64, Option<i64>, &Conversation)],
) -> Result<()> {
    let mut seen = HashSet::new();
    for &(_, _, conv) in conversations {
        let (source_id, source_kind, host_label) = normalized_storage_source_parts(
            Some(conv.source_id.as_str()),
            conversation_origin_kind(conv),
            conv.origin_host.as_deref(),
        );
        if !seen.insert(source_id.clone()) {
            continue;
        }
        let exists: i64 = tx.query_row_map(
            "SELECT COUNT(*) FROM sources WHERE id = ?1",
            fparams![source_id.as_str()],
            |row| row.get_typed(0),
        )?;
        if exists == 0 {
            let kind_str = source_kind.to_string();
            let now = FrankenStorage::now_millis();
            tracing::debug!(
                target: "cass::fk_guard",
                source_id = %source_id,
                kind = kind_str.as_str(),
                "inserting source row inside transaction to satisfy FK constraint"
            );
            tx.execute_compat(
                "INSERT OR IGNORE INTO sources(id, kind, host_label, created_at, updated_at)
                 VALUES(?1, ?2, ?3, ?4, ?5)",
                fparams![
                    source_id.as_str(),
                    kind_str.as_str(),
                    host_label.as_deref(),
                    now,
                    now
                ],
            )?;
        }
    }
    Ok(())
}

pub(super) fn env_flag_enabled(name: &str) -> bool {
    dotenvy::var(name).ok().is_some_and(|v| {
        matches!(
            v.trim(),
            "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"
        )
    })
}

pub(super) fn defer_storage_lexical_updates_enabled() -> bool {
    env_flag_enabled("CASS_DEFER_LEXICAL_UPDATES")
}
