use super::*;

pub(in crate::indexer) const LEXICAL_SHARD_PLAN_VERSION: u8 = 1;

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub(crate) struct LexicalShardPlannerBudgets {
    pub max_conversations_per_shard: usize,
    pub max_messages_per_shard: usize,
    pub max_message_bytes_per_shard: usize,
}

impl LexicalShardPlannerBudgets {
    pub(in crate::indexer) fn normalized(self) -> Self {
        Self {
            max_conversations_per_shard: self.max_conversations_per_shard.max(1),
            max_messages_per_shard: self.max_messages_per_shard.max(1),
            max_message_bytes_per_shard: self.max_message_bytes_per_shard.max(1),
        }
    }
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub(crate) struct LexicalShardPlannerConversation {
    pub conversation_id: i64,
    pub message_count: usize,
    pub message_bytes: usize,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub(crate) struct LexicalShardPlanShard {
    pub shard_index: usize,
    pub first_conversation_id: i64,
    pub last_conversation_id: i64,
    pub conversation_count: usize,
    pub message_count: usize,
    pub message_bytes: usize,
    pub conversation_id_fingerprint: String,
    pub oversized_single_conversation: bool,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub(crate) struct LexicalShardPlan {
    pub planner_version: u8,
    pub plan_id: String,
    pub budgets: LexicalShardPlannerBudgets,
    pub total_conversations: usize,
    pub total_messages: usize,
    pub total_message_bytes: usize,
    pub oversized_conversation_ids: Vec<i64>,
    pub shards: Vec<LexicalShardPlanShard>,
}

pub(in crate::indexer) const LEXICAL_SHARD_UNKNOWN_MESSAGE_COUNT: usize = usize::MAX;

pub(in crate::indexer) fn lexical_shard_message_count_is_known(message_count: usize) -> bool {
    message_count != LEXICAL_SHARD_UNKNOWN_MESSAGE_COUNT
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn plan_lexical_rebuild_shards(
    conversations: &[LexicalShardPlannerConversation],
    budgets: LexicalShardPlannerBudgets,
) -> LexicalShardPlan {
    let budgets = budgets.normalized();
    let mut ordered = conversations.to_vec();
    ordered.sort_by_key(|conversation| conversation.conversation_id);

    let total_conversations = ordered.len();
    let total_messages = ordered
        .iter()
        .map(|conversation| conversation.message_count)
        .sum();
    let total_message_bytes = ordered
        .iter()
        .map(|conversation| conversation.message_bytes)
        .sum();

    let mut shards = Vec::new();
    let mut oversized_conversation_ids = Vec::new();

    let mut first_conversation_id = None;
    let mut last_conversation_id = None;
    let mut conversation_count = 0usize;
    let mut message_count = 0usize;
    let mut message_bytes = 0usize;
    let mut conversation_ids = Vec::new();

    let flush_current_shard = |shards: &mut Vec<LexicalShardPlanShard>,
                               first_conversation_id: &mut Option<i64>,
                               last_conversation_id: &mut Option<i64>,
                               conversation_count: &mut usize,
                               message_count: &mut usize,
                               message_bytes: &mut usize,
                               conversation_ids: &mut Vec<i64>,
                               oversized_single_conversation: bool| {
        if *conversation_count == 0 {
            return;
        }
        debug_assert_eq!(*conversation_count, conversation_ids.len());
        shards.push(LexicalShardPlanShard {
            shard_index: shards.len(),
            first_conversation_id: (*first_conversation_id)
                .expect("non-empty shard should have a first conversation id"),
            last_conversation_id: (*last_conversation_id)
                .expect("non-empty shard should have a last conversation id"),
            conversation_count: *conversation_count,
            message_count: *message_count,
            message_bytes: *message_bytes,
            conversation_id_fingerprint: lexical_shard_conversation_ids_fingerprint(
                conversation_ids,
            ),
            oversized_single_conversation,
        });
        *first_conversation_id = None;
        *last_conversation_id = None;
        *conversation_count = 0;
        *message_count = 0;
        *message_bytes = 0;
        conversation_ids.clear();
    };

    for conversation in ordered {
        let exceeds_budget = conversation.message_count > budgets.max_messages_per_shard
            || conversation.message_bytes > budgets.max_message_bytes_per_shard;
        let would_overflow_current = conversation_count > 0
            && (conversation_count.saturating_add(1) > budgets.max_conversations_per_shard
                || message_count.saturating_add(conversation.message_count)
                    > budgets.max_messages_per_shard
                || message_bytes.saturating_add(conversation.message_bytes)
                    > budgets.max_message_bytes_per_shard);

        if would_overflow_current {
            flush_current_shard(
                &mut shards,
                &mut first_conversation_id,
                &mut last_conversation_id,
                &mut conversation_count,
                &mut message_count,
                &mut message_bytes,
                &mut conversation_ids,
                false,
            );
        }

        if exceeds_budget {
            oversized_conversation_ids.push(conversation.conversation_id);
            flush_current_shard(
                &mut shards,
                &mut first_conversation_id,
                &mut last_conversation_id,
                &mut conversation_count,
                &mut message_count,
                &mut message_bytes,
                &mut conversation_ids,
                false,
            );
            first_conversation_id = Some(conversation.conversation_id);
            last_conversation_id = Some(conversation.conversation_id);
            conversation_count = 1;
            message_count = conversation.message_count;
            message_bytes = conversation.message_bytes;
            conversation_ids.push(conversation.conversation_id);
            flush_current_shard(
                &mut shards,
                &mut first_conversation_id,
                &mut last_conversation_id,
                &mut conversation_count,
                &mut message_count,
                &mut message_bytes,
                &mut conversation_ids,
                true,
            );
            continue;
        }

        if first_conversation_id.is_none() {
            first_conversation_id = Some(conversation.conversation_id);
        }
        last_conversation_id = Some(conversation.conversation_id);
        conversation_count = conversation_count.saturating_add(1);
        message_count = message_count.saturating_add(conversation.message_count);
        message_bytes = message_bytes.saturating_add(conversation.message_bytes);
        conversation_ids.push(conversation.conversation_id);
    }

    flush_current_shard(
        &mut shards,
        &mut first_conversation_id,
        &mut last_conversation_id,
        &mut conversation_count,
        &mut message_count,
        &mut message_bytes,
        &mut conversation_ids,
        false,
    );

    let plan_id = lexical_shard_plan_id(
        budgets,
        &shards,
        total_conversations,
        total_messages,
        total_message_bytes,
        &oversized_conversation_ids,
    );

    LexicalShardPlan {
        planner_version: LEXICAL_SHARD_PLAN_VERSION,
        plan_id,
        budgets,
        total_conversations,
        total_messages,
        total_message_bytes,
        oversized_conversation_ids,
        shards,
    }
}

pub(in crate::indexer) fn lexical_shard_conversation_ids_fingerprint(
    conversation_ids: &[i64],
) -> String {
    let mut hasher = blake3::Hasher::new();
    for conversation_id in conversation_ids {
        hasher.update(&conversation_id.to_le_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

pub(in crate::indexer) fn lexical_shard_plan_id(
    budgets: LexicalShardPlannerBudgets,
    shards: &[LexicalShardPlanShard],
    total_conversations: usize,
    total_messages: usize,
    total_message_bytes: usize,
    oversized_conversation_ids: &[i64],
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&[LEXICAL_SHARD_PLAN_VERSION]);
    hasher.update(&budgets.max_conversations_per_shard.to_le_bytes());
    hasher.update(&budgets.max_messages_per_shard.to_le_bytes());
    hasher.update(&budgets.max_message_bytes_per_shard.to_le_bytes());
    hasher.update(&total_conversations.to_le_bytes());
    hasher.update(&total_messages.to_le_bytes());
    hasher.update(&total_message_bytes.to_le_bytes());
    for shard in shards {
        hasher.update(&shard.shard_index.to_le_bytes());
        hasher.update(&shard.first_conversation_id.to_le_bytes());
        hasher.update(&shard.last_conversation_id.to_le_bytes());
        hasher.update(&shard.conversation_count.to_le_bytes());
        hasher.update(&shard.message_count.to_le_bytes());
        hasher.update(&shard.message_bytes.to_le_bytes());
        hasher.update(shard.conversation_id_fingerprint.as_bytes());
        hasher.update(&[u8::from(shard.oversized_single_conversation)]);
    }
    for conversation_id in oversized_conversation_ids {
        hasher.update(&conversation_id.to_le_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

// Dormant with the Quill backend: staged sharding is a Tantivy-ingest
// parallelization strategy whose merge step has no FSLX equivalent. Retained
// (not deleted) because the planner is engine-independent and would be reused
// if Quill ever grows an N-directory merge.
#[allow(dead_code)]
pub(in crate::indexer) fn lexical_rebuild_target_shard_count(
    worker_parallelism: usize,
    tantivy_writer_threads: usize,
) -> usize {
    worker_parallelism
        .max(tantivy_writer_threads)
        .clamp(1, 64)
        .saturating_mul(4)
        .clamp(1, 256)
}

// Dormant with the Quill backend: staged sharding is a Tantivy-ingest
// parallelization strategy whose merge step has no FSLX equivalent. Retained
// (not deleted) because the planner is engine-independent and would be reused
// if Quill ever grows an N-directory merge.
#[allow(dead_code)]
pub(in crate::indexer) fn lexical_rebuild_default_shard_budget(
    total: usize,
    target_shards: usize,
    min_budget: usize,
    max_budget: usize,
) -> usize {
    let min_budget = min_budget.max(1);
    let max_budget = max_budget.max(min_budget);
    total
        .max(1)
        .div_ceil(target_shards.max(1))
        .clamp(min_budget, max_budget)
}

pub(in crate::indexer) const LEXICAL_REBUILD_STAGED_SHARD_MESSAGE_BYTES_FLOOR: usize =
    16 * 1024 * 1024;
pub(in crate::indexer) const LEXICAL_REBUILD_STAGED_SHARD_MESSAGE_BYTES_DEFAULT: usize =
    64 * 1024 * 1024;
pub(in crate::indexer) const LEXICAL_REBUILD_STAGED_SHARD_MESSAGE_BYTES_CEILING: usize =
    128 * 1024 * 1024;
pub(in crate::indexer) const LEXICAL_REBUILD_STAGED_SHARD_MESSAGE_BYTES_MEMORY_FRACTION: u64 =
    2_048;

pub(in crate::indexer) fn lexical_rebuild_default_staged_shard_max_message_bytes_for_available_memory(
    available_memory_bytes: Option<u64>,
) -> usize {
    let Some(available_memory_bytes) = available_memory_bytes else {
        return LEXICAL_REBUILD_STAGED_SHARD_MESSAGE_BYTES_DEFAULT;
    };
    let scaled =
        available_memory_bytes / LEXICAL_REBUILD_STAGED_SHARD_MESSAGE_BYTES_MEMORY_FRACTION;
    let scaled =
        usize::try_from(scaled).unwrap_or(LEXICAL_REBUILD_STAGED_SHARD_MESSAGE_BYTES_CEILING);
    scaled.clamp(
        LEXICAL_REBUILD_STAGED_SHARD_MESSAGE_BYTES_FLOOR,
        LEXICAL_REBUILD_STAGED_SHARD_MESSAGE_BYTES_CEILING,
    )
}

pub(in crate::indexer) fn lexical_rebuild_staged_shard_max_message_bytes(
    settings: &LexicalRebuildPipelineSettingsSnapshot,
) -> usize {
    dotenvy::var("CASS_TANTIVY_REBUILD_STAGED_SHARD_MAX_MESSAGE_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or_else(|| {
            lexical_rebuild_default_staged_shard_max_message_bytes_for_available_memory(
                responsiveness::available_memory_bytes(),
            )
        })
        .min(settings.steady_commit_every_message_bytes.max(1))
        .max(1)
}

pub(in crate::indexer) fn lexical_rebuild_pending_shard_build_max_message_bytes(
    settings: &LexicalRebuildPipelineSettingsSnapshot,
) -> usize {
    dotenvy::var("CASS_TANTIVY_REBUILD_PENDING_SHARD_BUILD_MAX_MESSAGE_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or_else(|| {
            settings
                .pipeline_max_message_bytes_in_flight
                .max(lexical_rebuild_staged_shard_max_message_bytes(settings))
        })
        .max(1)
}

pub(in crate::indexer) fn lexical_rebuild_pending_shard_build_max_jobs(
    settings: &LexicalRebuildPipelineSettingsSnapshot,
) -> usize {
    dotenvy::var("CASS_TANTIVY_REBUILD_PENDING_SHARD_BUILD_MAX_JOBS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or_else(|| settings.staged_shard_builders.max(1).saturating_mul(32))
        .clamp(1, 256)
}

// Dormant with the Quill backend: staged sharding is a Tantivy-ingest
// parallelization strategy whose merge step has no FSLX equivalent. Retained
// (not deleted) because the planner is engine-independent and would be reused
// if Quill ever grows an N-directory merge.
#[allow(dead_code)]
pub(in crate::indexer) fn lexical_rebuild_default_shard_planner_budgets_for_totals(
    settings: &LexicalRebuildPipelineSettingsSnapshot,
    total_conversations: usize,
    total_messages: usize,
    total_message_bytes: usize,
) -> LexicalShardPlannerBudgets {
    let target_shards =
        lexical_rebuild_target_shard_count(settings.workers, settings.tantivy_writer_threads);
    let max_message_bytes_per_shard = lexical_rebuild_staged_shard_max_message_bytes(settings);
    let min_message_bytes_per_shard = settings
        .startup_commit_every_message_bytes
        .min(max_message_bytes_per_shard)
        .max(1);
    LexicalShardPlannerBudgets {
        max_conversations_per_shard: lexical_rebuild_default_shard_budget(
            total_conversations,
            target_shards,
            settings.startup_batch_fetch_conversations,
            settings.steady_commit_every_conversations,
        ),
        max_messages_per_shard: lexical_rebuild_default_shard_budget(
            total_messages,
            target_shards,
            settings.startup_commit_every_messages,
            settings.steady_commit_every_messages,
        ),
        max_message_bytes_per_shard: lexical_rebuild_default_shard_budget(
            total_message_bytes,
            target_shards,
            min_message_bytes_per_shard,
            max_message_bytes_per_shard,
        ),
    }
}

// Dormant with the Quill backend: staged sharding is a Tantivy-ingest
// parallelization strategy whose merge step has no FSLX equivalent. Retained
// (not deleted) because the planner is engine-independent and would be reused
// if Quill ever grows an N-directory merge.
#[allow(dead_code)]
pub(in crate::indexer) fn lexical_rebuild_shard_planner_conversations_from_storage(
    storage: &FrankenStorage,
) -> Result<Vec<LexicalShardPlannerConversation>> {
    Ok(storage
        .list_conversation_footprints_for_lexical_rebuild()
        .with_context(|| "listing canonical lexical rebuild conversation footprints")?
        .into_iter()
        .map(|footprint| LexicalShardPlannerConversation {
            conversation_id: footprint.conversation_id,
            message_count: footprint.message_count,
            message_bytes: footprint.message_bytes,
        })
        .collect())
}

#[allow(dead_code)]
pub(in crate::indexer) const LEXICAL_REBUILD_ID_ONLY_MAX_CONVERSATIONS_PER_SHARD: usize = 64;

// Dormant with the Quill backend: staged sharding is a Tantivy-ingest
// parallelization strategy whose merge step has no FSLX equivalent. Retained
// (not deleted) because the planner is engine-independent and would be reused
// if Quill ever grows an N-directory merge.
#[allow(dead_code)]
pub(in crate::indexer) fn plan_lexical_rebuild_shards_from_conversation_ids_with_settings(
    storage: &FrankenStorage,
    settings: &LexicalRebuildPipelineSettingsSnapshot,
    total_conversations: usize,
    total_messages: usize,
) -> Result<LexicalShardPlan> {
    let conversation_ids = storage
        .list_conversation_ids_for_lexical_rebuild()
        .with_context(|| "listing canonical conversation ids for id-only lexical shard planning")?;
    let exact_total_conversations = conversation_ids.len();
    let average_messages_per_conversation = if exact_total_conversations == 0 {
        0
    } else {
        total_messages.div_ceil(exact_total_conversations).max(1)
    };
    let estimated_message_bytes_per_conversation = average_messages_per_conversation
        .saturating_mul(LEXICAL_REBUILD_PLANNER_ESTIMATED_BYTES_PER_MESSAGE);
    let max_message_bytes_per_shard = lexical_rebuild_staged_shard_max_message_bytes(settings);
    let max_conversations_by_average_bytes = if estimated_message_bytes_per_conversation == 0 {
        LEXICAL_REBUILD_ID_ONLY_MAX_CONVERSATIONS_PER_SHARD
    } else {
        max_message_bytes_per_shard
            .saturating_div(estimated_message_bytes_per_conversation)
            .max(1)
    };
    let max_conversations_per_shard = settings
        .steady_commit_every_conversations
        .clamp(1, LEXICAL_REBUILD_ID_ONLY_MAX_CONVERSATIONS_PER_SHARD)
        .min(max_conversations_by_average_bytes.max(1))
        .max(1);
    let max_messages_per_shard = average_messages_per_conversation
        .saturating_mul(max_conversations_per_shard)
        .max(1);
    let estimated_message_bytes_per_shard = estimated_message_bytes_per_conversation
        .saturating_mul(max_conversations_per_shard)
        .max(1)
        .min(max_message_bytes_per_shard.max(1));

    tracing::info!(
        total_conversations,
        exact_total_conversations,
        total_messages,
        average_messages_per_conversation,
        estimated_message_bytes_per_conversation,
        max_conversations_per_shard,
        max_messages_per_shard,
        max_message_bytes_per_shard = estimated_message_bytes_per_shard,
        "using conservative conversation-id-only lexical shard plan because canonical DB has no tail footprint metadata"
    );

    let conversations = conversation_ids
        .into_iter()
        .map(|conversation_id| LexicalShardPlannerConversation {
            conversation_id,
            message_count: average_messages_per_conversation,
            message_bytes: estimated_message_bytes_per_conversation,
        })
        .collect::<Vec<_>>();
    let budgets = LexicalShardPlannerBudgets {
        max_conversations_per_shard,
        max_messages_per_shard,
        max_message_bytes_per_shard: estimated_message_bytes_per_shard,
    };
    let mut plan = plan_lexical_rebuild_shards(&conversations, budgets);
    for shard in &mut plan.shards {
        shard.message_count = LEXICAL_SHARD_UNKNOWN_MESSAGE_COUNT;
    }
    plan.total_conversations = exact_total_conversations;
    plan.total_messages = total_messages;
    plan.total_message_bytes =
        total_messages.saturating_mul(LEXICAL_REBUILD_PLANNER_ESTIMATED_BYTES_PER_MESSAGE);
    plan.plan_id = lexical_shard_plan_id(
        plan.budgets,
        &plan.shards,
        plan.total_conversations,
        plan.total_messages,
        plan.total_message_bytes,
        &plan.oversized_conversation_ids,
    );
    Ok(plan)
}

// Dormant with the Quill backend: staged sharding is a Tantivy-ingest
// parallelization strategy whose merge step has no FSLX equivalent. Retained
// (not deleted) because the planner is engine-independent and would be reused
// if Quill ever grows an N-directory merge.
#[allow(dead_code)]
pub(in crate::indexer) fn plan_lexical_rebuild_shards_from_storage_with_settings(
    storage: &FrankenStorage,
    settings: &LexicalRebuildPipelineSettingsSnapshot,
    total_conversations: usize,
) -> Result<LexicalShardPlan> {
    if !storage.lexical_rebuild_has_tail_footprint_metadata()? {
        let total_messages = count_total_messages_exact(storage)?;
        return plan_lexical_rebuild_shards_from_conversation_ids_with_settings(
            storage,
            settings,
            total_conversations,
            total_messages,
        );
    }

    let conversations = lexical_rebuild_shard_planner_conversations_from_storage(storage)?;
    let total_conversations = conversations.len();
    let total_messages = conversations
        .iter()
        .map(|conversation| conversation.message_count)
        .sum();
    let total_message_bytes = conversations
        .iter()
        .map(|conversation| conversation.message_bytes)
        .sum();
    let budgets = lexical_rebuild_default_shard_planner_budgets_for_totals(
        settings,
        total_conversations,
        total_messages,
        total_message_bytes,
    );
    let mut plan = plan_lexical_rebuild_shards(&conversations, budgets);
    for shard in &mut plan.shards {
        // Footprints are sizing estimates, not validation contracts: the
        // lexical sink can legitimately emit more than one Tantivy document
        // for a source message on snippet-heavy sessions. Keep the byte-aware
        // boundaries, but leave exact doc-count validation to the completed
        // rebuild accounting.
        shard.message_count = LEXICAL_SHARD_UNKNOWN_MESSAGE_COUNT;
    }
    plan.plan_id = lexical_shard_plan_id(
        plan.budgets,
        &plan.shards,
        plan.total_conversations,
        plan.total_messages,
        plan.total_message_bytes,
        &plan.oversized_conversation_ids,
    );
    Ok(plan)
}

#[derive(Debug, Clone)]
pub(in crate::indexer) struct LexicalRebuildPlannedShardCursor {
    pub(in crate::indexer) shards: Vec<LexicalShardPlanShard>,
    pub(in crate::indexer) next_shard_index: usize,
}

impl LexicalRebuildPlannedShardCursor {
    pub(in crate::indexer) fn for_resume(
        mut shards: Vec<LexicalShardPlanShard>,
        resume_after_conversation_id: Option<i64>,
    ) -> Option<Self> {
        if shards.is_empty() {
            return None;
        }
        shards.sort_by_key(|shard| shard.shard_index);
        let resume_after_conversation_id = resume_after_conversation_id.unwrap_or(0);
        let next_shard_index = shards
            .iter()
            .position(|shard| shard.last_conversation_id > resume_after_conversation_id)
            .unwrap_or(shards.len());
        if next_shard_index >= shards.len() {
            None
        } else {
            Some(Self {
                shards,
                next_shard_index,
            })
        }
    }

    pub(in crate::indexer) fn current(&self) -> Option<&LexicalShardPlanShard> {
        self.shards.get(self.next_shard_index)
    }

    pub(in crate::indexer) fn skip_completed(&mut self, last_conversation_id: i64) {
        while let Some(shard) = self.current() {
            if last_conversation_id >= shard.last_conversation_id {
                self.advance();
            } else {
                break;
            }
        }
    }

    pub(in crate::indexer) fn advance(&mut self) {
        self.next_shard_index = self.next_shard_index.saturating_add(1);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::indexer) struct LexicalRebuildShardBuilderSettings {
    pub(in crate::indexer) max_builders: usize,
    pub(in crate::indexer) writer_parallelism_budget: usize,
}

pub(in crate::indexer) const LEXICAL_REBUILD_STAGED_SHARD_BUILDER_MEMORY_SLOT_BYTES: u64 =
    32 * 1024 * 1024 * 1024;
pub(in crate::indexer) const LEXICAL_REBUILD_STAGED_SHARD_BUILDER_MEMORY_BUDGET_NUMERATOR: u64 = 2;
pub(in crate::indexer) const LEXICAL_REBUILD_STAGED_SHARD_BUILDER_MEMORY_BUDGET_DENOMINATOR: u64 =
    3;
pub(in crate::indexer) const LEXICAL_REBUILD_STAGED_SHARD_BUILD_RESERVE_FRACTION: u64 = 8;
pub(in crate::indexer) const LEXICAL_REBUILD_STAGED_SHARD_BUILD_RESERVE_FALLBACK_BYTES: u64 =
    16 * 1024 * 1024 * 1024;
pub(in crate::indexer) const LEXICAL_REBUILD_STAGED_SHARD_BUILD_RESERVE_FLOOR_BYTES: u64 =
    4 * 1024 * 1024 * 1024;
pub(in crate::indexer) const LEXICAL_REBUILD_STAGED_SHARD_BUILD_RESERVE_CEILING_BYTES: u64 =
    32 * 1024 * 1024 * 1024;
pub(in crate::indexer) const LEXICAL_REBUILD_STAGED_SHARD_BUILD_EMERGENCY_RESERVE_FRACTION: u64 =
    32;
pub(in crate::indexer) const LEXICAL_REBUILD_STAGED_SHARD_BUILD_EMERGENCY_FALLBACK_BYTES: u64 =
    4 * 1024 * 1024 * 1024;
pub(in crate::indexer) const LEXICAL_REBUILD_STAGED_SHARD_BUILD_EMERGENCY_FLOOR_BYTES: u64 =
    1024 * 1024 * 1024;
pub(in crate::indexer) const LEXICAL_REBUILD_STAGED_SHARD_BUILD_EMERGENCY_CEILING_BYTES: u64 =
    8 * 1024 * 1024 * 1024;
pub(in crate::indexer) const LEXICAL_REBUILD_STAGED_SHARD_BUILD_AMPLIFICATION_FLOOR_MILLI: u64 =
    8_000;
pub(in crate::indexer) const LEXICAL_REBUILD_STAGED_SHARD_BUILD_AMPLIFICATION_HEADROOM_MILLI: u64 =
    1_500;
pub(in crate::indexer) const LEXICAL_REBUILD_STAGED_SHARD_BUILD_MIN_ESTIMATED_BYTES: u64 =
    512 * 1024 * 1024;

pub(in crate::indexer) fn lexical_rebuild_default_staged_shard_builder_parallelism_for_workers_and_memory(
    workers: usize,
    available_memory_bytes: Option<u64>,
) -> usize {
    let cpu_ceiling = workers.clamp(1, 8);
    let Some(available_memory_bytes) = available_memory_bytes else {
        return cpu_ceiling;
    };
    let memory_budget = available_memory_bytes
        .saturating_mul(LEXICAL_REBUILD_STAGED_SHARD_BUILDER_MEMORY_BUDGET_NUMERATOR)
        / LEXICAL_REBUILD_STAGED_SHARD_BUILDER_MEMORY_BUDGET_DENOMINATOR.max(1);
    let memory_ceiling = usize::try_from(
        memory_budget / LEXICAL_REBUILD_STAGED_SHARD_BUILDER_MEMORY_SLOT_BYTES.max(1),
    )
    .unwrap_or(usize::MAX)
    .clamp(1, 8);
    cpu_ceiling.min(memory_ceiling).max(1)
}

pub(in crate::indexer) fn lexical_rebuild_default_staged_shard_builder_parallelism_for_workers(
    workers: usize,
) -> usize {
    lexical_rebuild_default_staged_shard_builder_parallelism_for_workers_and_memory(
        workers,
        responsiveness::available_memory_bytes(),
    )
}

pub(in crate::indexer) fn usize_from_u64_saturating(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

pub(in crate::indexer) fn lexical_rebuild_default_staged_shard_build_memory_reserve_bytes_for_total_memory(
    total_memory_bytes: Option<u64>,
) -> usize {
    let reserve = total_memory_bytes
        .map(|total| total / LEXICAL_REBUILD_STAGED_SHARD_BUILD_RESERVE_FRACTION.max(1))
        .unwrap_or(LEXICAL_REBUILD_STAGED_SHARD_BUILD_RESERVE_FALLBACK_BYTES)
        .clamp(
            LEXICAL_REBUILD_STAGED_SHARD_BUILD_RESERVE_FLOOR_BYTES,
            LEXICAL_REBUILD_STAGED_SHARD_BUILD_RESERVE_CEILING_BYTES,
        );
    usize_from_u64_saturating(reserve)
}

pub(in crate::indexer) fn lexical_rebuild_staged_shard_build_memory_reserve_bytes() -> usize {
    dotenvy::var("CASS_TANTIVY_REBUILD_STAGED_SHARD_BUILD_MEMORY_RESERVE_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or_else(|| {
            lexical_rebuild_default_staged_shard_build_memory_reserve_bytes_for_total_memory(
                responsiveness::total_memory_bytes(),
            )
        })
}

pub(in crate::indexer) fn lexical_rebuild_default_staged_shard_build_emergency_memory_reserve_bytes_for_total_memory(
    total_memory_bytes: Option<u64>,
) -> usize {
    let reserve = total_memory_bytes
        .map(|total| total / LEXICAL_REBUILD_STAGED_SHARD_BUILD_EMERGENCY_RESERVE_FRACTION.max(1))
        .unwrap_or(LEXICAL_REBUILD_STAGED_SHARD_BUILD_EMERGENCY_FALLBACK_BYTES)
        .clamp(
            LEXICAL_REBUILD_STAGED_SHARD_BUILD_EMERGENCY_FLOOR_BYTES,
            LEXICAL_REBUILD_STAGED_SHARD_BUILD_EMERGENCY_CEILING_BYTES,
        );
    usize_from_u64_saturating(reserve)
}

pub(in crate::indexer) fn lexical_rebuild_staged_shard_build_emergency_memory_reserve_bytes()
-> usize {
    dotenvy::var("CASS_TANTIVY_REBUILD_STAGED_SHARD_BUILD_EMERGENCY_MEMORY_RESERVE_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or_else(|| {
            lexical_rebuild_default_staged_shard_build_emergency_memory_reserve_bytes_for_total_memory(
                responsiveness::total_memory_bytes(),
            )
        })
}

pub(in crate::indexer) fn lexical_rebuild_staged_shard_builder_parallelism() -> usize {
    let raw = lexical_rebuild_staged_shard_builder_parallelism_configured();
    // Apply the responsiveness governor so shard-builder fanout shrinks
    // together with page-prep when the host is under pressure. On an idle
    // box this is a no-op; under pressure it preserves interactive headroom.
    responsiveness::effective_worker_count(raw).max(1)
}

pub(in crate::indexer) fn lexical_rebuild_staged_shard_builder_parallelism_configured() -> usize {
    dotenvy::var("CASS_TANTIVY_REBUILD_STAGED_SHARD_BUILDERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or_else(|| {
            lexical_rebuild_default_staged_shard_builder_parallelism_for_workers(
                lexical_rebuild_worker_parallelism(),
            )
        })
}

pub(in crate::indexer) fn lexical_rebuild_default_staged_merge_worker_parallelism_for_workers(
    workers: usize,
) -> usize {
    workers
        .saturating_sub(workers.saturating_mul(3).div_ceil(4))
        .clamp(1, 8)
}

pub(in crate::indexer) fn lexical_rebuild_staged_merge_worker_parallelism() -> usize {
    let raw = lexical_rebuild_staged_merge_worker_parallelism_configured();
    // Same governor wire-up as shard builders above: keep at most 1 merger on
    // tiny hosts, scale with measured machine responsiveness.
    responsiveness::effective_worker_count(raw).max(1)
}

pub(in crate::indexer) fn lexical_rebuild_staged_merge_worker_parallelism_configured() -> usize {
    dotenvy::var("CASS_TANTIVY_REBUILD_STAGED_MERGE_WORKERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or_else(|| {
            lexical_rebuild_default_staged_merge_worker_parallelism_for_workers(
                lexical_rebuild_worker_parallelism(),
            )
        })
}

pub(in crate::indexer) fn lexical_rebuild_staged_shard_builder_settings(
    settings: &LexicalRebuildPipelineSettingsSnapshot,
    planned_shard_count: usize,
) -> LexicalRebuildShardBuilderSettings {
    let planned_shard_count = planned_shard_count.max(1);
    let writer_parallelism_budget = settings.tantivy_writer_threads.max(1);
    let max_builders = planned_shard_count
        .min(settings.staged_shard_builders.max(1))
        .min(writer_parallelism_budget)
        .max(1);
    LexicalRebuildShardBuilderSettings {
        max_builders,
        writer_parallelism_budget,
    }
}

pub(in crate::indexer) fn lexical_rebuild_staged_shard_builder_writer_parallelism_for_dispatch(
    writer_parallelism_budget: usize,
    allowed_jobs: usize,
    dispatch_slot_index: usize,
) -> usize {
    let writer_parallelism_budget = writer_parallelism_budget.max(1);
    let allowed_jobs = allowed_jobs.max(1);
    if dispatch_slot_index >= allowed_jobs {
        return 1;
    }
    if allowed_jobs >= writer_parallelism_budget {
        return 1;
    }

    let base = writer_parallelism_budget / allowed_jobs;
    let remainder = writer_parallelism_budget % allowed_jobs;
    base.saturating_add(usize::from(dispatch_slot_index < remainder))
        .max(1)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::indexer) struct LexicalRebuildShardMergeSettings {
    pub(in crate::indexer) workers: usize,
}

pub(in crate::indexer) fn lexical_rebuild_staged_shard_merge_settings(
    settings: &LexicalRebuildPipelineSettingsSnapshot,
    planned_shard_count: usize,
) -> LexicalRebuildShardMergeSettings {
    let eager_merge_groups =
        planned_shard_count / LexicalRebuildShardMergeCoordinator::EAGER_MERGE_FAN_IN;
    let workers = if eager_merge_groups == 0 {
        1
    } else {
        settings.staged_merge_workers.min(eager_merge_groups).max(1)
    };
    LexicalRebuildShardMergeSettings { workers }
}
