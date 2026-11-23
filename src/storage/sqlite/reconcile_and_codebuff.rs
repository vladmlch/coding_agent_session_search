use super::aggregators_and_pricing::{
    AnalyticsRollupAggregator, MessageMetricsEntry, TokenStatsAggregator, TokenStatsDelta,
    has_plan_for_role,
};
use super::backup_and_integrity::{
    DAILY_STATS_CONTENT_REVISION_PENDING, DAILY_STATS_HEALTH_GENERATION_META_KEY,
    DAILY_STATS_HEALTH_META_KEY, FTS_FRANKEN_CONTENT_REVISION_PENDING,
    FTS_FRANKEN_REBUILD_FINGERPRINT_META_KEY, FTS_FRANKEN_REBUILD_META_KEY,
    franken_read_message_extra_compat, franken_read_metadata_compat,
};
use super::batch_ingest::ensure_workspaces_in_tx;
use super::conversation_tail_state::existing_conversation_tail_state_from_cached;
use super::fts_rebuild::{FtsEntry, path_to_string, role_str};
use super::schema_migrations::SemanticIdentityTier;
use super::storage_core::FrankenStorage;
use super::tx_analytics_rollups::{
    franken_flush_analytics_rollups_in_tx, franken_relocate_rollup_subtract,
    franken_update_conversation_token_summaries_in_tx,
};
use super::tx_message_insert::{
    franken_batch_insert_fts, franken_insert_snippets, franken_message_insert_payload,
    franken_metadata_insert_payload,
};
use crate::franken_sync::{
    Row as FrankenRow,
    compat::{
        OptionalExtension as FrankenOptionalExtension, ParamValue, RowExt as FrankenRowExt,
        Transaction as FrankenTransaction,
    },
};
use crate::model::types::{Conversation, Message, MessageRole, Snippet};
use anyhow::{Context, Result, bail};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::time::Duration;

pub struct InsertOutcome {
    pub conversation_id: i64,
    pub conversation_inserted: bool,
    pub inserted_indices: Vec<i64>,
    /// Native messages whose payload changed without allocating another row.
    pub updated_indices: Vec<i64>,
    /// Existing messages need new derived workspace associations or titles,
    /// not reinsertion. Provider metadata alone does not set this flag.
    pub workspace_changed: bool,
}

/// A completed source observation committed with its final canonical batch.
pub struct SourceIngestLedgerEntry {
    pub key: String,
    pub observation: String,
}

/// Whether a replayed conversation's workspace is the provider's current
/// attribution, so it replaces the canonical one. Cursor agent sessions say so
/// explicitly, including an explicit "unresolved". Codebuff re-emits a whole
/// chat with the workspace read from its run-state sidecar, so a present
/// workspace is current; an absent one (sidecar removed or unreadable) never
/// erases a known association.
pub(super) fn provider_workspace_is_authoritative(
    agent_slug: &str,
    workspace: Option<&Path>,
    metadata: &serde_json::Value,
) -> bool {
    let present = workspace.is_some_and(|path| !path.as_os_str().is_empty());
    match agent_slug {
        "cursor" => {
            metadata["cursor_format"] == "agent"
                && match metadata["cursor_workspace_attribution"].as_str() {
                    Some("workspace_trusted") => present,
                    Some("unresolved") => workspace.is_none(),
                    _ => false,
                }
        }
        "codebuff" => present,
        _ => false,
    }
}

/// A Codebuff chat can also be reached through another store's path (an
/// alias source sharing its lineage). Only a replay of the canonical row's
/// own source carries that chat's run-state; an alias keeps the canonical
/// source's attribution. A targeted `--watch-once` scans the canonicalized
/// path, so a replay can name the canonical file in another spelling (macOS
/// `/private/var`, a symlinked home, a Windows short name or mixed
/// separators): the paths are compared as paths and, when both exist, as
/// files.
pub(super) fn replay_owns_provider_workspace(
    agent_slug: &str,
    canonical_source: Option<&str>,
    source_path: &Path,
) -> bool {
    if agent_slug != "codebuff" {
        return true;
    }
    let Some(canonical_source) = canonical_source.map(Path::new) else {
        return false;
    };
    canonical_source == source_path
        || matches!(
            (
                std::fs::canonicalize(canonical_source),
                std::fs::canonicalize(source_path),
            ),
            (Ok(canonical), Ok(replayed)) if canonical == replayed
        )
}

/// Canonical rows were deleted (forget, dedup, agent purge). Their vectors
/// stay in the semantic artifact and SQLite reuses the freed top message ids,
/// so a watermark that covers them lets `cass index --semantic` take the #394
/// skip forever: the tier is never re-certified, and a reused id would resolve
/// to the deleted text's vector. Dropping the watermark sends the next semantic
/// run through the full re-embed, the same invalidation the reconcilers below
/// apply on identity changes (2l1b0.78).
pub(super) fn clear_semantic_embed_watermark_after_deletion(
    tx: &FrankenTransaction<'_>,
) -> Result<()> {
    tx.execute("DELETE FROM meta WHERE key = 'last_embedded_message_id'")?;
    Ok(())
}

/// Delete one duplicate conversation and its children inside `tx`. Messages,
/// snippets, conversation_tags, token_usage, etc. carry `ON DELETE CASCADE`;
/// the two external-lookup tables do not, so they go first (mirrors
/// `purge_agent_archive`). The caller clears the semantic watermark and
/// invalidates the FTS presence cache once for the whole batch.
pub(super) fn delete_duplicate_conversation_rows(
    tx: &FrankenTransaction<'_>,
    conversation_id: i64,
) -> Result<()> {
    tx.execute_compat(
        "DELETE FROM conversation_external_lookup WHERE conversation_id = ?1",
        fparams![conversation_id],
    )?;
    tx.execute_compat(
        "DELETE FROM conversation_external_tail_lookup WHERE conversation_id = ?1",
        fparams![conversation_id],
    )?;
    // Explicit message delete (do not rely solely on cascade) so the
    // operation is correct even if FK enforcement is toggled off.
    tx.execute_compat(
        "DELETE FROM messages WHERE conversation_id = ?1",
        fparams![conversation_id],
    )?;
    tx.execute_compat(
        "DELETE FROM conversation_tail_state WHERE conversation_id = ?1",
        fparams![conversation_id],
    )?;
    tx.execute_compat(
        "DELETE FROM token_usage WHERE conversation_id = ?1",
        fparams![conversation_id],
    )?;
    // NOTE: `embedding_jobs` is a per-(db_path, model_id) job table, NOT a
    // per-conversation table — it has no conversation_id column, so there is
    // nothing conversation-scoped to delete there.
    tx.execute_compat(
        "DELETE FROM conversations WHERE id = ?1",
        fparams![conversation_id],
    )?;
    Ok(())
}

/// Reconcile only the provider-owned attribution fields. A missing workspace
/// in an ordinary partial packet must never erase a known association.
pub(super) fn franken_reconcile_provider_workspace(
    tx: &FrankenTransaction<'_>,
    agent_id: i64,
    conversation_id: i64,
    workspace_id: Option<i64>,
    conv: &Conversation,
) -> Result<bool> {
    if shelley_metadata_is_authoritative(conv) {
        return franken_reconcile_shelley_metadata(
            tx,
            agent_id,
            conversation_id,
            workspace_id,
            conv,
        );
    }
    if conv.external_id.is_none()
        || !provider_workspace_is_authoritative(
            &conv.agent_slug,
            conv.workspace.as_deref(),
            &conv.metadata_json,
        )
    {
        return Ok(false);
    }
    let (previous_workspace, previous_source, mut metadata): (
        Option<i64>,
        Option<String>,
        serde_json::Value,
    ) = tx.query_row_map(
        "SELECT workspace_id, source_path, metadata_json, metadata_bin FROM conversations WHERE id = ?1",
        fparams![conversation_id],
        |row| {
            Ok((
                row.get_typed(0)?,
                row.get_typed(1)?,
                franken_read_metadata_compat(row, 2, 3),
            ))
        },
    )?;
    if !replay_owns_provider_workspace(
        &conv.agent_slug,
        previous_source.as_deref(),
        &conv.source_path,
    ) {
        return Ok(false);
    }
    let mut changed = previous_workspace != workspace_id;
    if !metadata.is_object() {
        // Preserve non-object legacy metadata rather than replacing it blindly.
        anyhow::bail!(
            "cannot reconcile the provider workspace for conversation {conversation_id}: canonical metadata is not an object"
        );
    }
    for field in ["cursor_workspace_attribution", "cursor_project_dir"] {
        if let Some(value) = conv.metadata_json.get(field)
            && metadata.get(field) != Some(value)
        {
            metadata[field] = value.clone();
            changed = true;
        }
    }
    if let Some(original) = conv.metadata_json.pointer("/cass/workspace_original") {
        let cass = metadata
            .as_object_mut()
            .expect("checked object")
            .entry("cass")
            .or_insert_with(|| serde_json::json!({}));
        if let Some(cass) = cass.as_object_mut()
            && cass.get("workspace_original") != Some(original)
        {
            cass.insert("workspace_original".to_string(), original.clone());
            changed = true;
        }
    }
    if changed {
        if previous_workspace != workspace_id {
            ensure_workspaces_in_tx(tx, &[(agent_id, workspace_id, conv)])?;
        }
        let (json, binary) = franken_metadata_insert_payload(&metadata)?;
        tx.execute_compat(
            "UPDATE conversations SET workspace_id = ?1, metadata_json = ?2, metadata_bin = ?3 WHERE id = ?4",
            fparams![workspace_id, json.as_deref(), binary.as_deref(), conversation_id],
        )?;
        if previous_workspace != workspace_id {
            // Vector doc IDs embed workspace_id. Counts and rowid watermarks
            // cannot detect this change. A fresh generation also prevents an
            // interrupted checkpoint from being reused after a change-back.
            let generation = format!("workspace:{:032x}", rand::random::<u128>());
            for tier in [SemanticIdentityTier::Fast, SemanticIdentityTier::Quality] {
                tx.execute_compat(
                    "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
                    fparams![tier.meta_key(), generation.as_str()],
                )?;
            }
            tx.execute("DELETE FROM meta WHERE key = 'last_embedded_message_id'")?;
        }
    }
    Ok(previous_workspace != workspace_id)
}

pub(super) fn shelley_metadata_is_authoritative(conv: &Conversation) -> bool {
    conv.agent_slug == "shelley"
        && conv.external_id.is_some()
        && conv.metadata_json["source"] == "shelley"
        && conv.metadata_json["shelley"].is_object()
}

/// Refresh provider-owned fields without replacing canonical messages or CASS metadata.
/// The return value requests republication only when searchable fields changed.
pub(super) fn franken_reconcile_shelley_metadata(
    tx: &FrankenTransaction<'_>,
    agent_id: i64,
    conversation_id: i64,
    workspace_id: Option<i64>,
    conv: &Conversation,
) -> Result<bool> {
    let (previous_workspace, previous_title, mut metadata): (
        Option<i64>,
        Option<String>,
        serde_json::Value,
    ) = tx.query_row_map(
        "SELECT workspace_id, title, metadata_json, metadata_bin FROM conversations WHERE id = ?1",
        fparams![conversation_id],
        |row| {
            Ok((
                row.get_typed(0)?,
                row.get_typed(1)?,
                franken_read_metadata_compat(row, 2, 3),
            ))
        },
    )?;
    if !metadata.is_object() {
        anyhow::bail!(
            "cannot reconcile Shelley metadata for conversation {conversation_id}: canonical metadata is not an object"
        );
    }
    let workspace_changed = previous_workspace != workspace_id;
    let title_changed = previous_title != conv.title;
    let mut changed = workspace_changed || title_changed;
    for field in ["source", "shelley"] {
        if metadata.get(field) != conv.metadata_json.get(field) {
            metadata[field] = conv.metadata_json[field].clone();
            changed = true;
        }
    }
    if let Some(original) = conv.metadata_json.pointer("/cass/workspace_original") {
        let cass = metadata
            .as_object_mut()
            .expect("checked object")
            .entry("cass")
            .or_insert_with(|| serde_json::json!({}));
        if let Some(cass) = cass.as_object_mut()
            && cass.get("workspace_original") != Some(original)
        {
            cass.insert("workspace_original".to_string(), original.clone());
            changed = true;
        }
    }
    if changed {
        if workspace_changed {
            ensure_workspaces_in_tx(tx, &[(agent_id, workspace_id, conv)])?;
        }
        let (json, binary) = franken_metadata_insert_payload(&metadata)?;
        tx.execute_compat(
            "UPDATE conversations SET workspace_id = ?1, title = ?2, metadata_json = ?3, metadata_bin = ?4 WHERE id = ?5",
            fparams![workspace_id, conv.title.as_deref(), json.as_deref(), binary.as_deref(), conversation_id],
        )?;
        if workspace_changed {
            let generation = format!("workspace:{:032x}", rand::random::<u128>());
            for tier in [SemanticIdentityTier::Fast, SemanticIdentityTier::Quality] {
                tx.execute_compat(
                    "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
                    fparams![tier.meta_key(), generation.as_str()],
                )?;
            }
            tx.execute("DELETE FROM meta WHERE key = 'last_embedded_message_id'")?;
        }
    }
    Ok(workspace_changed || title_changed)
}

#[cfg(test)]
#[derive(Debug, Clone, Default)]
pub(super) struct MessageInsertSubstageProfile {
    pub(super) single_row_calls: usize,
    pub(super) batch_calls: usize,
    pub(super) batch_rows: usize,
    pub(super) payload_duration: Duration,
    pub(super) sql_build_duration: Duration,
    pub(super) param_build_duration: Duration,
    pub(super) execute_duration: Duration,
    pub(super) rowid_duration: Duration,
}

#[cfg(test)]
#[derive(Debug, Clone, Default)]
pub(super) struct InsertConversationTreePerfProfile {
    pub(super) invocations: usize,
    pub(super) messages: usize,
    pub(super) inserted_messages: usize,
    pub(super) total_duration: Duration,
    pub(super) source_duration: Duration,
    pub(super) tx_open_duration: Duration,
    pub(super) existing_lookup_duration: Duration,
    pub(super) existing_idx_lookup_duration: Duration,
    pub(super) existing_replay_lookup_duration: Duration,
    pub(super) dedupe_filter_duration: Duration,
    pub(super) conversation_row_duration: Duration,
    pub(super) message_insert_duration: Duration,
    pub(super) message_insert_breakdown: MessageInsertSubstageProfile,
    pub(super) snippet_insert_duration: Duration,
    pub(super) fts_entry_duration: Duration,
    pub(super) fts_flush_duration: Duration,
    pub(super) analytics_duration: Duration,
    pub(super) commit_duration: Duration,
}

#[cfg(test)]
impl InsertConversationTreePerfProfile {
    fn millis(duration: Duration) -> f64 {
        duration.as_secs_f64() * 1000.0
    }

    pub(super) fn log_summary(&self, label: &str) {
        let calls = self.invocations.max(1) as f64;
        let accounted_duration = self.source_duration
            + self.tx_open_duration
            + self.existing_lookup_duration
            + self.existing_idx_lookup_duration
            + self.existing_replay_lookup_duration
            + self.dedupe_filter_duration
            + self.conversation_row_duration
            + self.message_insert_duration
            + self.snippet_insert_duration
            + self.fts_entry_duration
            + self.fts_flush_duration
            + self.analytics_duration
            + self.commit_duration;
        let residual_duration = self.total_duration.saturating_sub(accounted_duration);
        eprintln!(
            concat!(
                "CASS_INSERT_TREE_STAGE_PROFILE ",
                "label={} calls={} messages={} inserted_messages={} ",
                "total_ms={:.3} source_ms={:.3} tx_open_ms={:.3} existing_lookup_ms={:.3} ",
                "existing_idx_lookup_ms={:.3} existing_replay_lookup_ms={:.3} dedupe_filter_ms={:.3} ",
                "conversation_row_ms={:.3} message_insert_ms={:.3} snippet_insert_ms={:.3} ",
                "fts_entry_ms={:.3} fts_flush_ms={:.3} analytics_ms={:.3} commit_ms={:.3} ",
                "msg_payload_ms={:.3} msg_sql_ms={:.3} msg_param_ms={:.3} msg_execute_ms={:.3} msg_rowid_ms={:.3} ",
                "residual_ms={:.3} avg_total_ms={:.3} avg_message_insert_ms={:.3} ",
                "avg_msg_execute_ms={:.3} avg_msg_payload_ms={:.3} avg_snippet_insert_ms={:.3} avg_fts_entry_ms={:.3} avg_commit_ms={:.3}"
            ),
            label,
            self.invocations,
            self.messages,
            self.inserted_messages,
            Self::millis(self.total_duration),
            Self::millis(self.source_duration),
            Self::millis(self.tx_open_duration),
            Self::millis(self.existing_lookup_duration),
            Self::millis(self.existing_idx_lookup_duration),
            Self::millis(self.existing_replay_lookup_duration),
            Self::millis(self.dedupe_filter_duration),
            Self::millis(self.conversation_row_duration),
            Self::millis(self.message_insert_duration),
            Self::millis(self.snippet_insert_duration),
            Self::millis(self.fts_entry_duration),
            Self::millis(self.fts_flush_duration),
            Self::millis(self.analytics_duration),
            Self::millis(self.commit_duration),
            Self::millis(self.message_insert_breakdown.payload_duration),
            Self::millis(self.message_insert_breakdown.sql_build_duration),
            Self::millis(self.message_insert_breakdown.param_build_duration),
            Self::millis(self.message_insert_breakdown.execute_duration),
            Self::millis(self.message_insert_breakdown.rowid_duration),
            Self::millis(residual_duration),
            Self::millis(self.total_duration) / calls,
            Self::millis(self.message_insert_duration) / calls,
            Self::millis(self.message_insert_breakdown.execute_duration) / calls,
            Self::millis(self.message_insert_breakdown.payload_duration) / calls,
            Self::millis(self.snippet_insert_duration) / calls,
            Self::millis(self.fts_entry_duration) / calls,
            Self::millis(self.commit_duration) / calls,
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) enum PendingConversationKey {
    External {
        source_id: String,
        agent_id: i64,
        external_id: String,
    },
    SourcePath {
        source_id: String,
        agent_id: i64,
        source_path: String,
        started_at: Option<i64>,
    },
}

pub(super) fn conversation_external_lookup_key(
    source_id: &str,
    agent_id: i64,
    external_id: &str,
) -> String {
    format!(
        "{}:{source_id}:{agent_id}:{}:{external_id}",
        source_id.chars().count(),
        external_id.chars().count()
    )
}

pub(super) fn conversation_external_lookup_key_for_conv(
    agent_id: i64,
    conv: &Conversation,
) -> Option<String> {
    conv.external_id
        .as_deref()
        .map(|external_id| conversation_external_lookup_key(&conv.source_id, agent_id, external_id))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct MessageMergeFingerprint {
    pub(super) idx: i64,
    pub(super) created_at: Option<i64>,
    pub(super) role: MessageRole,
    pub(super) author: Option<String>,
    pub(super) content_hash: [u8; 32],
}

/// Replay-equivalence core: everything the replay guard compares except the
/// native record identity a connector carried onto the message.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct MessageReplayCore {
    pub(super) created_at: Option<i64>,
    pub(super) role: MessageRole,
    pub(super) author: Option<String>,
    pub(super) content_hash: [u8; 32],
}

/// Replay state for one conversation: the set of `MessageReplayCore`s already
/// stored (or pending in the same transaction), each with the native record
/// ids seen for that core.
///
/// A replay match needs an identity anchor: either a known `created_at` or a
/// shared native record id. A `None` id means "this copy carried no native
/// identity" — unknown, not absent. For timestamp-anchored cores an unknown id
/// on either side is a wildcard: connectors that surface no native id keep the
/// legacy four-tuple replay semantics, and codex records archived after
/// franken-agent-detection compacts their extras (>=32 MiB sessions) still
/// replay against the full copies stored earlier instead of duplicating them.
/// An untimestamped core has no anchor of its own, so a message without
/// `created_at` replays only through its native id: two unknown-id,
/// untimestamped messages sharing text are distinct records (Cursor
/// tool-call placeholders and repeated replies recur with equal flattened
/// text and different arguments), and an unknown stored id cannot anchor
/// another copy either.
#[derive(Debug, Clone, Default)]
pub(super) struct MessageReplayFingerprintSet {
    cores: HashMap<MessageReplayCore, HashSet<Option<String>>>,
}

impl MessageReplayFingerprintSet {
    pub(super) fn contains_parts(
        &self,
        core: &MessageReplayCore,
        native_id: &Option<String>,
    ) -> bool {
        let Some(native_ids) = self.cores.get(core) else {
            return false;
        };
        match (&core.created_at, native_id) {
            // No anchor at all: never a replay.
            (None, None) => false,
            // Anchored only by a native id: the same id on both sides is a
            // replay; anything else is a distinct record.
            (None, Some(_)) => native_ids.contains(native_id),
            // Timestamp-anchored: legacy wildcard semantics for unknown ids.
            (Some(_), _) => {
                native_id.is_none() || native_ids.contains(native_id) || native_ids.contains(&None)
            }
        }
    }

    pub(super) fn insert_parts(&mut self, core: MessageReplayCore, native_id: Option<String>) {
        self.cores.entry(core).or_default().insert(native_id);
    }

    pub(super) fn contains_message(&self, msg: &Message) -> bool {
        let (core, native_id) = message_replay_parts(msg);
        self.contains_parts(&core, &native_id)
    }

    pub(super) fn insert_message(&mut self, msg: &Message) {
        let (core, native_id) = message_replay_parts(msg);
        self.insert_parts(core, native_id);
    }

    pub(super) fn absorb(&mut self, other: Self) {
        for (core, native_ids) in other.cores {
            self.cores.entry(core).or_default().extend(native_ids);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ConversationMergeEvidence {
    pub(super) exact_overlap: usize,
    pub(super) replay_overlap: usize,
    pub(super) smaller_replay_set: usize,
    pub(super) started_close: bool,
    pub(super) start_distance_ms: i64,
}

pub(super) struct ExistingConversationNewMessages<'a> {
    pub(super) messages: Vec<&'a Message>,
    pub(super) new_chars: i64,
    pub(super) idx_collision_count: usize,
    pub(super) first_collision_idx: Option<i64>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ExistingConversationTailState {
    pub(super) last_message_idx: i64,
    pub(super) last_message_created_at: i64,
    pub(super) ended_at: Option<i64>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ExistingConversationTailMetadata {
    pub(super) last_message_idx: Option<i64>,
    pub(super) last_message_created_at: Option<i64>,
    pub(super) ended_at: Option<i64>,
}

impl ExistingConversationTailMetadata {
    pub(super) fn complete_tail_state(self) -> Option<ExistingConversationTailState> {
        existing_conversation_tail_state_from_cached(
            self.last_message_idx,
            self.last_message_created_at,
            self.ended_at,
        )
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ExistingConversationWithTail {
    pub(super) id: i64,
    pub(super) tail_state: Option<ExistingConversationTailState>,
}

pub(super) fn conversation_effective_started_at(conv: &Conversation) -> Option<i64> {
    conv.started_at
        .or_else(|| conv.messages.iter().filter_map(|msg| msg.created_at).min())
}

pub(super) fn conversation_tail_state(conv: &Conversation) -> (Option<i64>, Option<i64>) {
    (
        conv.messages.iter().map(|msg| msg.idx).max(),
        conv.messages.iter().filter_map(|msg| msg.created_at).max(),
    )
}

pub(super) fn conversation_tail_ended_at_candidate(conv: &Conversation) -> Option<i64> {
    let max_message_created_at = conv.messages.iter().filter_map(|msg| msg.created_at).max();
    max_message_created_at.max(conv.ended_at)
}

pub(super) fn borrowed_messages_tail_state(messages: &[&Message]) -> (Option<i64>, Option<i64>) {
    (
        messages.iter().map(|msg| msg.idx).max(),
        messages.iter().filter_map(|msg| msg.created_at).max(),
    )
}

pub(super) fn role_from_str(role: &str) -> MessageRole {
    match role {
        "user" => MessageRole::User,
        "agent" | "assistant" => MessageRole::Agent,
        "tool" => MessageRole::Tool,
        "system" => MessageRole::System,
        other => MessageRole::Other(other.to_string()),
    }
}

pub(super) fn message_merge_fingerprint(msg: &Message) -> MessageMergeFingerprint {
    MessageMergeFingerprint {
        idx: msg.idx,
        created_at: msg.created_at,
        role: msg.role.clone(),
        author: msg.author.clone(),
        content_hash: *blake3::hash(msg.content.as_bytes()).as_bytes(),
    }
}

/// The replay core of one message: the replay fingerprint fields without the
/// native record identity.
fn message_replay_core(msg: &Message) -> MessageReplayCore {
    MessageReplayCore {
        created_at: msg.created_at,
        role: msg.role.clone(),
        author: msg.author.clone(),
        content_hash: *blake3::hash(msg.content.as_bytes()).as_bytes(),
    }
}

/// Stable native record identity a connector archived onto the message.
///
/// The codex connector archives whole response_item envelopes in `extra`, and
/// tool outputs carry the Responses API call identity at `payload.call_id`,
/// falling back to `payload.id` — the same resolution order
/// franken-agent-detection applies to tool calls. Tool and skill call
/// messages additionally archive `cass.invocations[0].call_id` (indexer
/// invocation merge). Sparse tool-result connectors (devin, hermes,
/// openhands) archive the tool call identity as a top-level `tool_call_id` —
/// often the only per-record identity their rows carry. Any connector that
/// exposes one of these shapes participates in native-id replay
/// disambiguation; timestamped messages without a native id keep the legacy
/// four-tuple replay semantics, while untimestamped ones (see
/// [`MessageReplayFingerprintSet`]) replay only through a shared id.
///
/// Devin tool rows are turn-timestamped, so duplicate-text results of one
/// turn share the whole replay core; before the top-level `tool_call_id` was
/// read here they collapsed through the timestamped wildcard branch and the
/// archive silently lost the 2nd..Nth result (62 sessions, 507 unpaired
/// calls on the owner archive at the time).
pub(super) fn message_native_id(msg: &Message) -> Option<String> {
    native_id_from_extra(&msg.extra_json)
}

pub(super) fn native_id_from_extra(extra: &serde_json::Value) -> Option<String> {
    extra
        .pointer("/payload/call_id")
        .or_else(|| extra.pointer("/payload/id"))
        .or_else(|| extra.get("tool_call_id"))
        .or_else(|| extra.pointer("/cass/invocations/0/call_id"))
        .and_then(serde_json::Value::as_str)
        .filter(|native_id| !native_id.is_empty())
        .map(str::to_owned)
}

/// The replay core plus the native record identity of one message.
pub(super) fn message_replay_parts(msg: &Message) -> (MessageReplayCore, Option<String>) {
    (message_replay_core(msg), message_native_id(msg))
}

pub(super) fn conversation_message_fingerprints(
    conv: &Conversation,
) -> HashSet<MessageMergeFingerprint> {
    conv.messages
        .iter()
        .map(message_merge_fingerprint)
        .collect()
}

pub(super) fn conversation_message_replay_cores(conv: &Conversation) -> HashSet<MessageReplayCore> {
    conv.messages.iter().map(message_replay_core).collect()
}

fn replay_core_from_merge(fingerprint: &MessageMergeFingerprint) -> MessageReplayCore {
    MessageReplayCore {
        created_at: fingerprint.created_at,
        role: fingerprint.role.clone(),
        author: fingerprint.author.clone(),
        content_hash: fingerprint.content_hash,
    }
}

pub(super) fn replay_cores_from_merge_set(
    fingerprints: &HashSet<MessageMergeFingerprint>,
) -> HashSet<MessageReplayCore> {
    fingerprints.iter().map(replay_core_from_merge).collect()
}

pub(super) fn collect_new_messages_for_existing_conversation<'a>(
    conversation_id: i64,
    conv: &'a Conversation,
    existing_messages: &mut HashMap<i64, MessageMergeFingerprint>,
    existing_replay_fingerprints: &mut MessageReplayFingerprintSet,
    replay_skip_log: &'static str,
) -> ExistingConversationNewMessages<'a> {
    if matches!(conv.agent_slug.as_str(), "grok_bot" | "codebuff") {
        return reconciled_native_messages(conv);
    }
    let mut idx_collision_count = 0usize;
    let mut first_collision_idx: Option<i64> = None;
    let mut new_chars: i64 = 0;
    let mut messages = Vec::new();

    for msg in &conv.messages {
        let incoming_fingerprint = message_merge_fingerprint(msg);
        if let Some(existing_fingerprint) = existing_messages.get(&msg.idx) {
            if existing_fingerprint != &incoming_fingerprint {
                idx_collision_count = idx_collision_count.saturating_add(1);
                first_collision_idx.get_or_insert(msg.idx);
            }
            continue;
        }

        let (incoming_core, incoming_native_id) = message_replay_parts(msg);
        if existing_replay_fingerprints.contains_parts(&incoming_core, &incoming_native_id) {
            tracing::debug!(
                conversation_id,
                idx = msg.idx,
                source_path = %conv.source_path.display(),
                "{replay_skip_log}"
            );
            continue;
        }

        existing_messages.insert(msg.idx, incoming_fingerprint);
        existing_replay_fingerprints.insert_parts(incoming_core, incoming_native_id);
        new_chars += msg.content.len() as i64;
        messages.push(msg);
    }

    ExistingConversationNewMessages {
        messages,
        new_chars,
        idx_collision_count,
        first_collision_idx,
    }
}

/// Grok Bot packets have already been reconciled transactionally by native ID.
/// Every remaining message is new, regardless of timestamp or content equality.
pub(super) fn reconciled_native_messages(
    conv: &Conversation,
) -> ExistingConversationNewMessages<'_> {
    ExistingConversationNewMessages {
        messages: conv.messages.iter().collect(),
        new_chars: conv
            .messages
            .iter()
            .map(|msg| msg.content.len() as i64)
            .sum(),
        idx_collision_count: 0,
        first_collision_idx: None,
    }
}

/// Resolve rolling-window positions before the ordinary idx-based append path.
/// Canonical message envelopes are the durable native-ID map; no second mapping
/// table can drift from them. Read and append share the writer transaction,
/// including multiple overlapping windows submitted in one batch.
pub(super) fn franken_reconcile_native_message_indices<'a>(
    tx: &FrankenTransaction<'_>,
    agent_id: i64,
    conv: &'a Conversation,
) -> Result<Cow<'a, Conversation>> {
    if conv.agent_slug != "grok_bot" {
        return Ok(Cow::Borrowed(conv));
    }
    let external_id = conv
        .external_id
        .as_deref()
        .filter(|id| !id.is_empty())
        .context("Grok Bot rolling transcript requires its account/agent replica identity")?;
    let existing: Option<(i64, Option<i64>)> = tx.query_row_map(
        "SELECT id, started_at FROM conversations WHERE source_id = ?1 AND agent_id = ?2 AND external_id = ?3",
        fparams![conv.source_id.as_str(), agent_id, external_id],
        |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
    ).optional()?;
    let mut native_messages = HashMap::new();
    let mut max_idx = None::<i64>;
    if let Some((conversation_id, _)) = existing {
        for row in tx.query_params(
            "SELECT idx, role, author, created_at, content, extra_json, extra_bin FROM messages WHERE conversation_id = ?1",
            fparams![conversation_id],
        )? {
            let idx = row.get_typed::<i64>(0)?;
            let extra = franken_read_message_extra_compat(&row, 5, 6);
            let native_id = grok_bot_native_entry_id(&extra)?.to_owned();
            let identity = (
                row.get_typed::<String>(1)?,
                row.get_typed::<Option<String>>(2)?,
                row.get_typed::<Option<i64>>(3)?,
                row.get_typed::<String>(4)?,
                extra,
            );
            if native_messages.insert(native_id, identity).is_some() {
                bail!("Grok Bot canonical transcript contains duplicate native entry IDs");
            }
            max_idx = Some(max_idx.map_or(idx, |previous| previous.max(idx)));
        }
    }
    let mut reconciled = conv.clone();
    reconciled.messages.clear();
    if let Some((_, started_at)) = existing {
        reconciled.started_at = started_at;
    }
    for message in &conv.messages {
        let native_id = grok_bot_native_entry_id(&message.extra_json)?.to_owned();
        let identity = (
            role_str(&message.role),
            message.author.clone(),
            message.created_at,
            message.content.clone(),
            message.extra_json.clone(),
        );
        if let Some(canonical) = native_messages.get(&native_id) {
            if canonical != &identity {
                // Do not log native IDs or chat content in the diagnostic.
                bail!("Grok Bot native entry ID conflicts with its canonical message");
            }
            continue;
        }
        let idx = match max_idx {
            Some(previous) => previous
                .checked_add(1)
                .context("Grok Bot canonical message index exhausted")?,
            None => 0,
        };
        max_idx = Some(idx);
        native_messages.insert(native_id, identity);
        let mut message = message.clone();
        message.idx = idx;
        reconciled.messages.push(message);
    }
    Ok(Cow::Owned(reconciled))
}

fn grok_bot_native_entry_id(extra: &serde_json::Value) -> Result<&str> {
    extra
        .get("grok_bot_entry_id")
        .and_then(serde_json::Value::as_str)
        .filter(|id| !id.trim().is_empty())
        .context("Grok Bot message is missing its native entry ID")
}

pub(super) struct CodebuffRevision {
    conversation_id: i64,
    started_at: Option<i64>,
    source_id: String,
    previous_content_chars: i64,
    pub(super) message: Message,
    fts: FtsEntry,
}

pub(super) fn codebuff_native_message_id(extra: &serde_json::Value) -> Result<&str> {
    let id = extra
        .get("codebuff_message_id")
        .and_then(serde_json::Value::as_str)
        .filter(|id| !id.trim().is_empty())
        .context("Codebuff / Freebuff message requires its native ID")?;
    anyhow::ensure!(
        extra.get("id").and_then(serde_json::Value::as_str) == Some(id),
        "Codebuff / Freebuff native ID disagrees with its source envelope"
    );
    Ok(id)
}

/// The FAD shared-lineage transcript is a mutable snapshot. Preserve canonical
/// row IDs and positions while replacing same-ID payloads, never deduplicating
/// distinct IDs by content/time. Role, author and timestamp are immutable;
/// duplicates inside a packet are malformed even if their payloads match.
pub(super) fn franken_reconcile_codebuff_messages<'a>(
    tx: &FrankenTransaction<'_>,
    agent_id: i64,
    conv: &'a Conversation,
    revisions: &mut Vec<CodebuffRevision>,
) -> Result<Cow<'a, Conversation>> {
    if conv.agent_slug != "codebuff" {
        return Ok(Cow::Borrowed(conv));
    }
    let external_id = conv
        .external_id
        .as_deref()
        .filter(|id| !id.is_empty())
        .context("Codebuff / Freebuff transcript requires its store-scoped identity")?;
    let existing: Option<(i64, Option<i64>)> = tx.query_row_map(
        "SELECT id, started_at FROM conversations WHERE source_id = ?1 AND agent_id = ?2 AND external_id = ?3",
        fparams![conv.source_id.as_str(), agent_id, external_id],
        |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
    ).optional()?;
    let mut native = HashMap::new();
    let mut max_idx = None::<i64>;
    if let Some((conversation_id, _)) = existing {
        for row in tx.query_params(
            "SELECT id, idx, role, author, created_at, content, extra_json, extra_bin FROM messages WHERE conversation_id = ?1",
            fparams![conversation_id],
        )? {
            let extra_json = franken_read_message_extra_compat(&row, 6, 7);
            let id = codebuff_native_message_id(&extra_json)?.to_owned();
            let idx = row.get_typed::<i64>(1)?;
            let message = Message {
                id: Some(row.get_typed(0)?), idx,
                role: role_from_str(&row.get_typed::<String>(2)?),
                author: row.get_typed(3)?, created_at: row.get_typed(4)?,
                content: row.get_typed(5)?, extra_json, snippets: Vec::new(),
            };
            anyhow::ensure!(native.insert(id, message).is_none(),
                "Codebuff / Freebuff canonical transcript has duplicate native IDs");
            max_idx = Some(max_idx.map_or(idx, |old| old.max(idx)));
        }
    }
    let mut reconciled = conv.clone();
    reconciled.messages.clear();
    if let Some((conversation_id, started_at)) = existing {
        reconciled.started_at = started_at;
        let (source_path, title, workspace): (String, Option<String>, Option<String>) = tx.query_row_map(
            "SELECT c.source_path, c.title, (SELECT w.path FROM workspaces w WHERE w.id = c.workspace_id)
             FROM conversations c WHERE c.id = ?1",
            fparams![conversation_id],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?, row.get_typed(2)?)),
        )?;
        reconciled.source_path = source_path.into();
        reconciled.title = title;
        reconciled.workspace = workspace.map(PathBuf::from);
    }
    let mut packet_ids = HashSet::new();
    for message in &conv.messages {
        let native_id = codebuff_native_message_id(&message.extra_json)?;
        anyhow::ensure!(
            packet_ids.insert(native_id),
            "Codebuff / Freebuff packet has duplicate native IDs"
        );
        if let Some(old) = native.get(native_id) {
            anyhow::ensure!(
                old.role == message.role
                    && old.author == message.author
                    && old.created_at == message.created_at,
                "Codebuff / Freebuff native ID changed immutable role, author or timestamp"
            );
            let message_id = old.id.context("canonical message lacks row ID")?;
            if old.content == message.content
                && old.extra_json == message.extra_json
                && franken_codebuff_snippets_match(tx, message_id, &message.snippets)?
            {
                continue;
            }
            let mut revised = message.clone();
            revised.id = Some(message_id);
            revised.idx = old.idx;
            let (json, bin) = franken_message_insert_payload(&revised)?;
            tx.execute_compat(
                "UPDATE messages SET content = ?2, extra_json = ?3, extra_bin = ?4 WHERE id = ?1",
                fparams![
                    message_id,
                    revised.content.as_str(),
                    json.as_deref(),
                    bin.as_deref()
                ],
            )?;
            tx.execute_compat(
                "DELETE FROM snippets WHERE message_id = ?1",
                fparams![message_id],
            )?;
            franken_insert_snippets(tx, message_id, &revised.snippets)?;
            revisions.push(CodebuffRevision {
                conversation_id: existing.context("canonical message lacks conversation")?.0,
                started_at: reconciled.started_at,
                source_id: conv.source_id.clone(),
                previous_content_chars: i64::try_from(old.content.len())
                    .context("message length exceeds analytics range")?,
                fts: FtsEntry::from_message(message_id, &revised, &reconciled),
                message: revised,
            });
        } else {
            let idx = max_idx.map_or(Ok(0), |idx| {
                idx.checked_add(1)
                    .context("Codebuff / Freebuff canonical message index exhausted")
            })?;
            max_idx = Some(idx);
            let mut message = message.clone();
            message.id = None;
            message.idx = idx;
            reconciled.messages.push(message);
        }
    }
    Ok(Cow::Owned(reconciled))
}

fn franken_codebuff_snippets_match(
    tx: &FrankenTransaction<'_>,
    message_id: i64,
    incoming: &[Snippet],
) -> Result<bool> {
    let rows = tx.query_params(
        "SELECT file_path, start_line, end_line, language, snippet_text FROM snippets WHERE message_id = ?1 ORDER BY id",
        fparams![message_id],
    )?;
    codebuff_snippet_rows_match(&rows, incoming)
}

pub(super) fn codebuff_snippet_rows_match(
    rows: &[FrankenRow],
    incoming: &[Snippet],
) -> Result<bool> {
    if rows.len() != incoming.len() {
        return Ok(false);
    }
    for (row, snippet) in rows.iter().zip(incoming) {
        if row.get_typed::<Option<String>>(0)? != snippet.file_path.as_ref().map(path_to_string)
            || row.get_typed::<Option<i64>>(1)? != snippet.start_line
            || row.get_typed::<Option<i64>>(2)? != snippet.end_line
            || row.get_typed::<Option<String>>(3)? != snippet.language
            || row.get_typed::<Option<String>>(4)? != snippet.snippet_text
        {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn codebuff_tool_count(extra: &serde_json::Value) -> u32 {
    fn count(blocks: &[serde_json::Value]) -> u32 {
        blocks.iter().fold(0_u32, |total, block| {
            let own =
                u32::from(block.get("type").and_then(serde_json::Value::as_str) == Some("tool"));
            let nested = block
                .get("blocks")
                .and_then(serde_json::Value::as_array)
                .map_or(0, |blocks| count(blocks));
            total.saturating_add(own).saturating_add(nested)
        })
    }
    extra
        .get("blocks")
        .and_then(serde_json::Value::as_array)
        .map_or(0, |blocks| count(blocks))
}

/// Update only materialized analytics. A deferred initial insert has no metric
/// row and remains due for normal backfill; a later deferred revision still
/// corrects an already-published contribution instead of leaving it stale.
pub(super) fn franken_apply_codebuff_revision_projections(
    storage: &FrankenStorage,
    tx: &FrankenTransaction<'_>,
    revision: &CodebuffRevision,
    defer_lexical_updates: bool,
) -> Result<()> {
    let message_id = revision.fts.message_id;
    if !defer_lexical_updates
        && !storage.fts_inline_writes_suspended()
        && storage.fts_messages_present_cached(tx)
    {
        let mut fts = revision.fts.clone();
        let (source_path, title, workspace): (String, Option<String>, Option<String>) = tx.query_row_map(
            "SELECT c.source_path, c.title, (SELECT w.path FROM workspaces w WHERE w.id = c.workspace_id)
             FROM conversations c WHERE c.id = ?1",
            fparams![revision.conversation_id],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?, row.get_typed(2)?)),
        )?;
        fts.source_path = source_path;
        fts.title = title.unwrap_or_default();
        fts.workspace = workspace.unwrap_or_default();
        tx.execute_compat(
            "DELETE FROM fts_messages WHERE rowid = ?1",
            fparams![message_id],
        )?;
        franken_batch_insert_fts(storage, tx, std::slice::from_ref(&fts))?;
    } else {
        tx.execute_compat(
            "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
            fparams![
                FTS_FRANKEN_REBUILD_META_KEY,
                FTS_FRANKEN_CONTENT_REVISION_PENDING.to_string()
            ],
        )?;
        tx.execute_compat(
            "DELETE FROM meta WHERE key = ?1",
            fparams![FTS_FRANKEN_REBUILD_FINGERPRINT_META_KEY],
        )?;
    }
    let old: Option<MessageMetricsEntry> = tx
        .query_row_map(
            "SELECT message_id, created_at_ms, hour_id, day_id, agent_slug, workspace_id,
         source_id, role, content_chars, content_tokens_est, model_name, model_family,
         model_tier, provider, api_input_tokens, api_output_tokens, api_cache_read_tokens,
         api_cache_creation_tokens, api_thinking_tokens, api_service_tier, api_data_source,
         tool_call_count, has_tool_calls, has_plan FROM message_metrics WHERE message_id = ?1",
            fparams![message_id],
            |row| {
                Ok(MessageMetricsEntry {
                    message_id: row.get_typed(0)?,
                    created_at_ms: row.get_typed(1)?,
                    hour_id: row.get_typed(2)?,
                    day_id: row.get_typed(3)?,
                    agent_slug: row.get_typed(4)?,
                    workspace_id: row.get_typed(5)?,
                    source_id: row.get_typed(6)?,
                    role: row.get_typed(7)?,
                    content_chars: row.get_typed(8)?,
                    content_tokens_est: row.get_typed(9)?,
                    model_name: row.get_typed(10)?,
                    model_family: row.get_typed(11)?,
                    model_tier: row.get_typed(12)?,
                    provider: row.get_typed(13)?,
                    api_input_tokens: row.get_typed(14)?,
                    api_output_tokens: row.get_typed(15)?,
                    api_cache_read_tokens: row.get_typed(16)?,
                    api_cache_creation_tokens: row.get_typed(17)?,
                    api_thinking_tokens: row.get_typed(18)?,
                    api_service_tier: row.get_typed(19)?,
                    api_data_source: row.get_typed(20)?,
                    tool_call_count: row.get_typed(21)?,
                    has_tool_calls: row.get_typed::<i64>(22)? != 0,
                    has_plan: row.get_typed::<i64>(23)? != 0,
                })
            },
        )
        .optional()?;
    let message = &revision.message;
    let mut usage = crate::connectors::extract_tokens_for_agent(
        "codebuff",
        &message.extra_json,
        &message.content,
        &role_str(&message.role),
    );
    usage.tool_call_count = codebuff_tool_count(&message.extra_json);
    usage.has_tool_calls = usage.tool_call_count > 0;
    let content_chars =
        i64::try_from(message.content.len()).context("message length exceeds analytics range")?;
    if let Some(old) = old {
        // Track A may have been backfilled while Track B remains deferred.
        anyhow::ensure!(
            old.api_data_source == "estimated" && old.model_name.is_none(),
            "Codebuff / Freebuff revision has unsupported provider-usage analytics"
        );
        let mut new = old.clone();
        new.content_chars = content_chars;
        new.content_tokens_est = content_chars / 4;
        new.api_input_tokens = usage.input_tokens;
        new.api_output_tokens = usage.output_tokens;
        new.api_cache_read_tokens = usage.cache_read_tokens;
        new.api_cache_creation_tokens = usage.cache_creation_tokens;
        new.api_thinking_tokens = usage.thinking_tokens;
        new.tool_call_count = i64::from(usage.tool_call_count);
        new.has_tool_calls = usage.has_tool_calls;
        new.has_plan = has_plan_for_role(&new.role, &message.content);
        franken_replace_codebuff_rollup_contribution(tx, &old, &new)?;
        tx.execute_compat(
            "UPDATE message_metrics SET content_chars = ?2, content_tokens_est = ?3,
             api_input_tokens = ?4, api_output_tokens = ?5, tool_call_count = ?6,
             has_tool_calls = ?7, has_plan = ?8, api_cache_read_tokens = ?9,
             api_cache_creation_tokens = ?10, api_thinking_tokens = ?11 WHERE message_id = ?1",
            fparams![
                message_id,
                new.content_chars,
                new.content_tokens_est,
                new.api_input_tokens,
                new.api_output_tokens,
                new.tool_call_count,
                i64::from(new.has_tool_calls),
                i64::from(new.has_plan),
                new.api_cache_read_tokens,
                new.api_cache_creation_tokens,
                new.api_thinking_tokens
            ],
        )?;
    }
    // Track B uses its own persisted contribution, never the independently
    // rebuildable Track A metrics as a proxy for what was previously counted.
    let ledger = tx.query_params(
        "SELECT day_id, source_id, model_family, content_chars, input_tokens, output_tokens,
         cache_read_tokens, cache_creation_tokens, thinking_tokens, total_tokens,
         tool_call_count, estimated_cost_usd, data_source, model_name
         FROM token_usage WHERE message_id = ?1",
        fparams![message_id],
    )?;
    if let Some(old) = ledger.first() {
        anyhow::ensure!(
            old.get_typed::<String>(12)? == "estimated"
                && old.get_typed::<Option<String>>(13)?.is_none(),
            "Codebuff / Freebuff revision has unsupported provider-usage token ledger"
        );
        let delta = TokenStatsDelta {
            total_input_tokens: usage.input_tokens.unwrap_or(0)
                - old.get_typed::<Option<i64>>(4)?.unwrap_or(0),
            total_output_tokens: usage.output_tokens.unwrap_or(0)
                - old.get_typed::<Option<i64>>(5)?.unwrap_or(0),
            total_cache_read_tokens: usage.cache_read_tokens.unwrap_or(0)
                - old.get_typed::<Option<i64>>(6)?.unwrap_or(0),
            total_cache_creation_tokens: usage.cache_creation_tokens.unwrap_or(0)
                - old.get_typed::<Option<i64>>(7)?.unwrap_or(0),
            total_thinking_tokens: usage.thinking_tokens.unwrap_or(0)
                - old.get_typed::<Option<i64>>(8)?.unwrap_or(0),
            grand_total_tokens: usage.total_tokens().unwrap_or(0)
                - old.get_typed::<Option<i64>>(9)?.unwrap_or(0),
            total_content_chars: content_chars - old.get_typed::<i64>(3)?,
            total_tool_calls: i64::from(usage.tool_call_count) - old.get_typed::<i64>(10)?,
            estimated_cost_usd: -old.get_typed::<Option<f64>>(11)?.unwrap_or(0.0),
            ..TokenStatsDelta::default()
        };
        let mut tokens = TokenStatsAggregator::new();
        tokens.deltas.insert(
            (
                old.get_typed(0)?,
                "codebuff".into(),
                old.get_typed(1)?,
                old.get_typed::<Option<String>>(2)?
                    .unwrap_or_else(|| "unknown".into()),
            ),
            delta,
        );
        // Missing aggregate buckets are still due for backfill. Applying a
        // revision must not invent a bucket containing only its delta.
        for (day, agent, source, model, delta) in tokens.expand() {
            tx.execute_compat(
                "UPDATE token_daily_stats SET total_input_tokens = total_input_tokens + ?5,
                 total_output_tokens = total_output_tokens + ?6,
                 total_cache_read_tokens = total_cache_read_tokens + ?7,
                 total_cache_creation_tokens = total_cache_creation_tokens + ?8,
                 total_thinking_tokens = total_thinking_tokens + ?9,
                 grand_total_tokens = grand_total_tokens + ?10,
                 total_content_chars = total_content_chars + ?11,
                 total_tool_calls = total_tool_calls + ?12,
                 estimated_cost_usd = estimated_cost_usd + ?13, last_updated = ?14
                 WHERE day_id = ?1 AND agent_slug = ?2 AND source_id = ?3 AND model_family = ?4",
                fparams![
                    day,
                    agent.as_str(),
                    source.as_str(),
                    model.as_str(),
                    delta.total_input_tokens,
                    delta.total_output_tokens,
                    delta.total_cache_read_tokens,
                    delta.total_cache_creation_tokens,
                    delta.total_thinking_tokens,
                    delta.grand_total_tokens,
                    delta.total_content_chars,
                    delta.total_tool_calls,
                    delta.estimated_cost_usd,
                    FrankenStorage::now_millis()
                ],
            )?;
        }
        tx.execute_compat(
            "UPDATE token_usage SET content_chars = ?2, input_tokens = ?3, output_tokens = ?4,
             total_tokens = ?5, tool_call_count = ?6, has_tool_calls = ?7,
             cache_read_tokens = ?8, cache_creation_tokens = ?9, thinking_tokens = ?10,
             estimated_cost_usd = NULL WHERE message_id = ?1",
            fparams![
                message_id,
                content_chars,
                usage.input_tokens,
                usage.output_tokens,
                usage.total_tokens(),
                i64::from(usage.tool_call_count),
                i64::from(usage.has_tool_calls),
                usage.cache_read_tokens,
                usage.cache_creation_tokens,
                usage.thinking_tokens
            ],
        )?;
        franken_update_conversation_token_summaries_in_tx(tx, revision.conversation_id)?;
    }
    if !ledger.is_empty() {
        let day = revision
            .started_at
            .map(FrankenStorage::day_id_from_millis)
            .unwrap_or(0);
        tx.execute_compat(
            "UPDATE daily_stats SET total_chars = total_chars + ?3, last_updated = ?4
             WHERE day_id = ?1 AND agent_slug IN ('codebuff', 'all') AND source_id IN (?2, 'all')",
            fparams![
                day,
                revision.source_id.as_str(),
                content_chars - revision.previous_content_chars,
                FrankenStorage::now_millis()
            ],
        )?;
    } else {
        // Track A backfill does not prove this message was counted in daily
        // stats. Preserve the bucket and require its bounded canonical rebuild.
        tx.execute_compat(
            "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
            fparams![
                DAILY_STATS_HEALTH_GENERATION_META_KEY,
                DAILY_STATS_CONTENT_REVISION_PENDING.to_string()
            ],
        )?;
        tx.execute_compat(
            "DELETE FROM meta WHERE key = ?1",
            fparams![DAILY_STATS_HEALTH_META_KEY],
        )?;
    }
    Ok(())
}

pub(super) fn franken_replace_codebuff_rollup_contribution(
    tx: &FrankenTransaction<'_>,
    old: &MessageMetricsEntry,
    new: &MessageMetricsEntry,
) -> Result<()> {
    let mut previous = AnalyticsRollupAggregator::new();
    previous.record(old);
    let mut current = AnalyticsRollupAggregator::new();
    current.record(new);
    for (table, bucket_col, deltas) in [
        ("usage_hourly", "hour_id", &previous.hourly),
        ("usage_daily", "day_id", &previous.daily),
    ] {
        for ((bucket, agent, workspace, source), delta) in deltas {
            let present = tx.query_row_map(
                &format!("SELECT 1 FROM {table} WHERE {bucket_col} = ?1 AND agent_slug = ?2 AND workspace_id = ?3 AND source_id = ?4"),
                fparams![*bucket, agent.as_str(), *workspace, source.as_str()],
                |row| row.get_typed::<i64>(0),
            ).optional()?.is_some();
            if !present {
                let key = (*bucket, agent.clone(), *workspace, source.clone());
                if table == "usage_hourly" {
                    current.hourly.remove(&key);
                } else {
                    current.daily.remove(&key);
                }
                continue;
            }
            franken_relocate_rollup_subtract(
                tx,
                table,
                vec![
                    (bucket_col, ParamValue::from(*bucket)),
                    ("agent_slug", ParamValue::from(agent.as_str())),
                    ("workspace_id", ParamValue::from(*workspace)),
                    ("source_id", ParamValue::from(source.as_str())),
                ],
                *workspace,
                delta,
            )?;
        }
    }
    for ((day, agent, workspace, source, family, tier), delta) in &previous.models_daily {
        let present = tx
            .query_row_map(
                "SELECT 1 FROM usage_models_daily WHERE day_id = ?1 AND agent_slug = ?2
             AND workspace_id = ?3 AND source_id = ?4 AND model_family = ?5 AND model_tier = ?6",
                fparams![
                    *day,
                    agent.as_str(),
                    *workspace,
                    source.as_str(),
                    family.as_str(),
                    tier.as_str()
                ],
                |row| row.get_typed::<i64>(0),
            )
            .optional()?
            .is_some();
        if !present {
            current.models_daily.remove(&(
                *day,
                agent.clone(),
                *workspace,
                source.clone(),
                family.clone(),
                tier.clone(),
            ));
            continue;
        }
        franken_relocate_rollup_subtract(
            tx,
            "usage_models_daily",
            vec![
                ("day_id", ParamValue::from(*day)),
                ("agent_slug", ParamValue::from(agent.as_str())),
                ("workspace_id", ParamValue::from(*workspace)),
                ("source_id", ParamValue::from(source.as_str())),
                ("model_family", ParamValue::from(family.as_str())),
                ("model_tier", ParamValue::from(tier.as_str())),
            ],
            *workspace,
            delta,
        )?;
    }
    franken_flush_analytics_rollups_in_tx(tx, &current)?;
    Ok(())
}
