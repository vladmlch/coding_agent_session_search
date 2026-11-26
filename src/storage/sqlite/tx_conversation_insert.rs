use super::backup_and_integrity::{
    FTS_FALLBACK_REPAIR_PENDING_DETAIL_MAX_BYTES, FTS_SHADOW_NOT_VIABLE_ERROR_PREFIX,
};
use super::batch_ingest::{env_flag_enabled, franken_last_rowid};
use super::conversation_tail_state::{
    conversation_merge_evidence, conversation_merge_key,
    existing_conversation_tail_state_from_cached, franken_existing_conversation_append_tail_state,
    franken_insert_conversation_tail_state,
};
use super::fts_rebuild::path_to_string;
use super::open_and_locks::DEFER_ANALYTICS_UPDATES_GUARD_DEPTH;
use super::reconcile_and_codebuff::{
    ConversationMergeEvidence, ExistingConversationWithTail, PendingConversationKey,
    conversation_effective_started_at, conversation_external_lookup_key,
    conversation_external_lookup_key_for_conv, conversation_message_fingerprints,
    conversation_message_replay_cores, conversation_tail_state, replay_cores_from_merge_set,
};
use super::tx_message_insert::{
    franken_existing_message_fingerprints, franken_metadata_insert_payload,
};
use crate::franken_sync::{
    SqliteValue,
    compat::{
        OptionalExtension as FrankenOptionalExtension, RowExt as FrankenRowExt,
        Transaction as FrankenTransaction,
    },
};
use crate::model::types::Conversation;
use anyhow::{Context, Result, bail};
use std::sync::atomic::Ordering;
use std::time::Duration;

/// GH #413 follow-up: how much of one index run the inline `fts_messages`
/// shadow writes may cost before the rest of the run skips them.
///
/// Skipping the shadow keeps the canonical rows and the Quill index landing;
/// the shadow, which only the SQL search fallback reads, is left Partial with
/// the reason recorded for `doctor`. `CASS_FTS_INLINE_BUDGET_SECS` overrides;
/// `0` disables the budget.
const DEFAULT_FTS_INLINE_BUDGET_SECS: u64 = 300;

pub(super) fn fts_inline_flush_budget() -> Option<Duration> {
    let secs = dotenvy::var("CASS_FTS_INLINE_BUDGET_SECS")
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_FTS_INLINE_BUDGET_SECS);
    (secs > 0).then_some(Duration::from_secs(secs))
}

/// Per-page budget for paged shadow repair and rebuild. A page of
/// `CASS_FTS_REBUILD_BATCH_SIZE` rows that takes longer than this stops
/// maintenance with the reason instead of wedging the run for hours
/// (GH #345/#369/#413). `CASS_FTS_REPAIR_PAGE_BUDGET_SECS` overrides; `0`
/// disables the budget.
const DEFAULT_FTS_REPAIR_PAGE_BUDGET_SECS: u64 = 120;

pub(super) fn fts_repair_page_budget() -> Option<Duration> {
    let secs = dotenvy::var("CASS_FTS_REPAIR_PAGE_BUDGET_SECS")
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_FTS_REPAIR_PAGE_BUDGET_SECS);
    (secs > 0).then_some(Duration::from_secs(secs))
}

/// Largest canonical corpus (indexable message count) for which cass keeps
/// the derived `fts_messages` shadow at all.
///
/// Message count is the measure because it is exact and cheap
/// (`FTS_INDEXABLE_MESSAGE_COUNT_SQL`). When a positive bound is configured,
/// the shadow is dropped above it — it is derived data; Quill is the lexical
/// engine and the SQL search fallback scans `messages` when no shadow exists —
/// and it is not recreated until the corpus fits again.
/// `CASS_FTS_SHADOW_MAX_MESSAGES` overrides; `0` (the default on native SQLite)
/// disables the bound.
const DEFAULT_FTS_SHADOW_MAX_MESSAGES: u64 = 0;

pub(crate) fn fts_shadow_max_messages() -> Option<u64> {
    let messages = dotenvy::var("CASS_FTS_SHADOW_MAX_MESSAGES")
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_FTS_SHADOW_MAX_MESSAGES);
    (messages > 0).then_some(messages)
}

/// Whether the derived fts5 shadow may exist for the archive's corpus.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FtsShadowViability {
    /// No `fts_messages` table in the catalog.
    Absent,
    Viable {
        corpus_messages: u64,
    },
    NotViable {
        corpus_messages: u64,
        bound_messages: u64,
    },
}

/// The one sentence every surface (index log, status marker, doctor) uses
/// for a shadow the engine cannot materialize.
/// GH #374: legacy engine versions before the canonical-shadow fix created the
/// FTS5 `%_config`/`%_idx` shadow tables as ordinary rowid tables. Stock SQLite
/// then reports `wrong # of entries in index sqlite_autoindex_fts_messages_config_1`
/// while legacy `quick_check` could still return `ok`. Shared by the full
/// index preflight and residue classification so both agree on the shape.
pub(crate) fn incompatible_legacy_fts_shadow_ddl(table: &str, ddl: &str) -> Option<String> {
    let normalized = ddl
        .chars()
        .filter(|ch| !ch.is_whitespace() && *ch != '"' && *ch != '\'')
        .collect::<String>()
        .to_ascii_lowercase();
    (!normalized.contains("withoutrowid")).then(|| {
        format!(
            "legacy {table} schema is not WITHOUT ROWID; this pre-fix FTS5 shadow shape can carry a stale implicit autoindex and must be rebuilt before full indexing"
        )
    })
}

pub(crate) fn fts_shadow_not_viable_detail(corpus_messages: u64, bound_messages: u64) -> String {
    // GH #497 follow-up: the settled-state statement comes first because the
    // persisted markers keep only the first 400 characters of this text.
    format!(
        "{FTS_SHADOW_NOT_VIABLE_ERROR_PREFIX}settled state, not a pending repair: the canonical \
         corpus is {corpus_messages} messages, over the {bound_messages} message bound \
         (CASS_FTS_SHADOW_MAX_MESSAGES), so the derived shadow stays retired. Quill lexical \
         search is unaffected; the SQL search fallback scans messages; the derived FTS5 shadow \
         stays retired above CASS_FTS_SHADOW_MAX_MESSAGES (GH #413). `cass index --full` or \
         `cass doctor --rebuild-canonical-fts --yes` recreates it only after \
         CASS_FTS_SHADOW_MAX_MESSAGES is raised to at least the corpus size"
    )
}

/// The persisted form of a fallback-FTS marker detail: the first
/// `FTS_FALLBACK_REPAIR_PENDING_DETAIL_MAX_BYTES` characters.
pub(super) fn bounded_fts_marker_detail(detail: &str) -> String {
    detail
        .chars()
        .take(FTS_FALLBACK_REPAIR_PENDING_DETAIL_MAX_BYTES)
        .collect()
}

pub(crate) fn error_message_indicates_fts_shadow_not_viable(detail: &str) -> bool {
    detail.contains(FTS_SHADOW_NOT_VIABLE_ERROR_PREFIX)
}

pub(super) fn defer_analytics_updates_enabled() -> bool {
    resolve_defer_analytics_updates(
        env_flag_enabled("CASS_DEFER_ANALYTICS_UPDATES"),
        env_flag_enabled("CASS_INLINE_ANALYTICS_UPDATES"),
        DEFER_ANALYTICS_UPDATES_GUARD_DEPTH.load(Ordering::SeqCst) > 0,
    )
}

pub(crate) fn explicit_analytics_rebuild_deferred() -> bool {
    env_flag_enabled("CASS_DEFER_ANALYTICS_UPDATES")
}

pub(super) fn resolve_defer_analytics_updates(
    explicit_defer: bool,
    explicit_inline: bool,
    ambient_defer: bool,
) -> bool {
    if explicit_defer {
        return true;
    }
    if explicit_inline {
        return false;
    }
    ambient_defer
}

pub(super) enum ConversationInsertStatus {
    Inserted(i64),
    Existing(i64),
}

pub(super) fn franken_find_external_conversation_tail_lookup(
    tx: &FrankenTransaction<'_>,
    lookup_key: &str,
) -> Result<Option<ExistingConversationWithTail>> {
    let params = [SqliteValue::from(lookup_key)];
    let row = tx
        .query_row_with_params(
            "SELECT conversation_id, ended_at, last_message_idx, last_message_created_at
             FROM conversation_external_tail_lookup
             WHERE lookup_key = ?1",
            &params,
        )
        .optional()?;
    let Some(row) = row else {
        return Ok(None);
    };
    let id = row.get_typed(0)?;
    let ended_at = row.get_typed(1)?;
    let last_message_idx = row.get_typed(2)?;
    let last_message_created_at = row.get_typed(3)?;
    Ok(Some(ExistingConversationWithTail {
        id,
        tail_state: existing_conversation_tail_state_from_cached(
            last_message_idx,
            last_message_created_at,
            ended_at,
        ),
    }))
}

pub(super) fn franken_find_external_conversation_lookup(
    tx: &FrankenTransaction<'_>,
    lookup_key: &str,
) -> Result<Option<i64>> {
    Ok(franken_find_external_conversation_tail_lookup(tx, lookup_key)?.map(|existing| existing.id))
}

fn franken_insert_external_conversation_tail_lookup_key(
    tx: &FrankenTransaction<'_>,
    lookup_key: &str,
    conversation_id: i64,
    ended_at: Option<i64>,
    last_message_idx: Option<i64>,
    last_message_created_at: Option<i64>,
) -> Result<()> {
    let params = [
        SqliteValue::from(lookup_key),
        SqliteValue::from(conversation_id),
        SqliteValue::from(ended_at),
        SqliteValue::from(last_message_idx),
        SqliteValue::from(last_message_created_at),
    ];
    tx.execute_with_params(
        "INSERT OR REPLACE INTO conversation_external_tail_lookup(
             lookup_key, conversation_id, ended_at, last_message_idx, last_message_created_at
         ) VALUES(?1, ?2, ?3, ?4, ?5)",
        &params,
    )?;
    Ok(())
}

fn franken_insert_external_conversation_tail_lookup(
    tx: &FrankenTransaction<'_>,
    source_id: &str,
    agent_id: i64,
    external_id: &str,
    existing: ExistingConversationWithTail,
) -> Result<()> {
    let lookup_key = conversation_external_lookup_key(source_id, agent_id, external_id);
    let ended_at = existing.tail_state.and_then(|state| state.ended_at);
    let last_message_idx = existing.tail_state.map(|state| state.last_message_idx);
    let last_message_created_at = existing
        .tail_state
        .map(|state| state.last_message_created_at);
    franken_insert_external_conversation_tail_lookup_key(
        tx,
        &lookup_key,
        existing.id,
        ended_at,
        last_message_idx,
        last_message_created_at,
    )
}

/// Providers whose transcript path is a durable identity for the
/// conversation, so an external-id re-key may be reconciled by source path.
///
/// - `omp` / `pi_agent`: older connectors derived external ids from paths
///   relative to a discovery root; the same transcript acquires a different
///   external id when the connector starts preferring its embedded session id
///   or a newer detector selects a more-specific root.
/// - `antigravity`: the transcript lives at
///   `<base>/brain/<uuid>/.system_generated/logs/transcript.jsonl`, so the path
///   already embeds the conversation uuid and is never reused for a different
///   session. Conversations under the IDE store re-key from the bare `<uuid>`
///   to `ide/<uuid>` once the connector distinguishes the two stores (#454).
///
/// Other providers may legitimately reuse a source path for unrelated
/// external sessions and must stay out of this lane.
fn source_path_is_durable_conversation_identity(agent_slug: &str) -> bool {
    matches!(agent_slug, "omp" | "pi_agent" | "antigravity")
}

/// Recover the first ingest after a previously indexed transcript acquires a
/// different external id (see [`source_path_is_durable_conversation_identity`]
/// for the providers where the transcript path is a durable identity).
///
/// The transcript's source-qualified absolute path plus the normal source-path
/// merge evidence is the durable identity in that upgrade case.
pub(super) fn franken_promote_pi_family_external_identity_by_source_path(
    tx: &FrankenTransaction<'_>,
    source_id: &str,
    agent_id: i64,
    external_id: &str,
    conv: Option<&Conversation>,
) -> Result<Option<ExistingConversationWithTail>> {
    let Some(conv) =
        conv.filter(|conv| source_path_is_durable_conversation_identity(&conv.agent_slug))
    else {
        return Ok(None);
    };

    // A missing derived lookup must not make the path fallback select a
    // different row when the canonical external identity already exists.
    let exact_external_id = tx
        .query_row_map(
            "SELECT id
             FROM conversations
             WHERE source_id = ?1 AND agent_id = ?2 AND external_id = ?3",
            fparams![source_id, agent_id, external_id],
            |row| row.get_typed::<i64>(0),
        )
        .optional()?;

    let existing_id = if let Some(existing_id) = exact_external_id {
        existing_id
    } else {
        let source_path = path_to_string(&conv.source_path);
        let path_candidates: Vec<i64> = tx.query_map_collect(
            "SELECT id
             FROM conversations
             WHERE source_id = ?1 AND agent_id = ?2 AND source_path = ?3
             ORDER BY id
             LIMIT 2",
            fparams![source_id, agent_id, source_path.as_str()],
            |row| row.get_typed(0),
        )?;
        match path_candidates.as_slice() {
            [] => return Ok(None),
            [_] => {}
            _ => {
                bail!(
                    "cannot promote pi-family external identity for source_id={source_id} path={source_path}: multiple canonical conversations share the source-qualified path"
                );
            }
        }

        // Do not merge merely because a path was reused. Require the same
        // start/message evidence as the established no-external-id lane.
        let source_path_key = PendingConversationKey::SourcePath {
            source_id: source_id.to_owned(),
            agent_id,
            source_path,
            started_at: conversation_effective_started_at(conv),
        };
        let Some(existing_id) = franken_find_existing_conversation_by_key_impl(
            tx,
            &source_path_key,
            Some(conv),
            false,
        )?
        else {
            return Ok(None);
        };
        existing_id
    };

    let existing_external_id: Option<String> = tx.query_row_map(
        "SELECT external_id FROM conversations WHERE id = ?1",
        fparams![existing_id],
        |row| row.get_typed(0),
    )?;
    if existing_external_id.as_deref() != Some(external_id) {
        tx.execute_compat(
            "DELETE FROM conversation_external_lookup WHERE conversation_id = ?1",
            fparams![existing_id],
        )?;
        tx.execute_compat(
            "DELETE FROM conversation_external_tail_lookup WHERE conversation_id = ?1",
            fparams![existing_id],
        )?;
        tx.execute_compat(
            "UPDATE conversations SET external_id = ?1 WHERE id = ?2",
            fparams![external_id, existing_id],
        )?;
    }

    let existing = ExistingConversationWithTail {
        id: existing_id,
        tail_state: franken_existing_conversation_append_tail_state(tx, existing_id)?,
    };
    let lookup_key = conversation_external_lookup_key(source_id, agent_id, external_id);
    tx.execute_compat(
        "INSERT OR REPLACE INTO conversation_external_lookup(lookup_key, conversation_id)
         VALUES(?1, ?2)",
        fparams![lookup_key.as_str(), existing_id],
    )?;
    franken_insert_external_conversation_tail_lookup(
        tx,
        source_id,
        agent_id,
        external_id,
        existing,
    )?;
    Ok(Some(existing))
}

pub(super) fn franken_update_external_conversation_tail_lookup_key(
    tx: &FrankenTransaction<'_>,
    lookup_key: &str,
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
        "UPDATE conversation_external_tail_lookup
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
         WHERE lookup_key = ?4",
        fparams![
            ended_at_candidate,
            last_message_idx_candidate,
            last_message_created_at_candidate,
            lookup_key
        ],
    )?;
    Ok(())
}

fn franken_set_external_conversation_tail_lookup_after_append(
    tx: &FrankenTransaction<'_>,
    lookup_key: &str,
    ended_at: i64,
    last_message_idx: i64,
    last_message_created_at: i64,
) -> Result<()> {
    tx.execute_compat(
        "UPDATE conversation_external_tail_lookup
         SET ended_at = ?1,
             last_message_idx = ?2,
             last_message_created_at = ?3
         WHERE lookup_key = ?4",
        fparams![
            ended_at,
            last_message_idx,
            last_message_created_at,
            lookup_key
        ],
    )?;
    Ok(())
}

pub(super) fn franken_update_external_conversation_tail_after_append(
    tx: &FrankenTransaction<'_>,
    agent_id: i64,
    conv: &Conversation,
    used_append_tail_plan: bool,
    exact_append_set: bool,
    inserted_last_idx: Option<i64>,
    inserted_last_created_at: Option<i64>,
) -> Result<()> {
    let Some(lookup_key) = conversation_external_lookup_key_for_conv(agent_id, conv) else {
        return Ok(());
    };

    if exact_append_set
        && let (Some(last_message_idx), Some(last_message_created_at)) =
            (inserted_last_idx, inserted_last_created_at)
    {
        return franken_set_external_conversation_tail_lookup_after_append(
            tx,
            &lookup_key,
            last_message_created_at,
            last_message_idx,
            last_message_created_at,
        );
    }

    let ended_at_candidate = if used_append_tail_plan {
        inserted_last_created_at
    } else {
        conv.messages.iter().filter_map(|m| m.created_at).max()
    };
    franken_update_external_conversation_tail_lookup_key(
        tx,
        &lookup_key,
        ended_at_candidate,
        inserted_last_idx,
        inserted_last_created_at,
    )
}

pub(super) fn franken_find_existing_conversation_by_key(
    tx: &FrankenTransaction<'_>,
    key: &PendingConversationKey,
    conv: Option<&Conversation>,
) -> Result<Option<i64>> {
    franken_find_existing_conversation_by_key_impl(tx, key, conv, false)
}

fn franken_find_existing_conversation_by_key_after_conflict(
    tx: &FrankenTransaction<'_>,
    key: &PendingConversationKey,
    conv: Option<&Conversation>,
) -> Result<Option<i64>> {
    franken_find_existing_conversation_by_key_impl(tx, key, conv, true)
}

fn franken_find_existing_conversation_by_key_impl(
    tx: &FrankenTransaction<'_>,
    key: &PendingConversationKey,
    conv: Option<&Conversation>,
    allow_legacy_external_scan: bool,
) -> Result<Option<i64>> {
    match key {
        PendingConversationKey::External {
            source_id,
            agent_id,
            external_id,
        } => {
            let lookup_key = conversation_external_lookup_key(source_id, *agent_id, external_id);
            if let Some(existing_id) = franken_find_external_conversation_lookup(tx, &lookup_key)? {
                return Ok(Some(existing_id));
            }
            if let Some(existing) = franken_promote_pi_family_external_identity_by_source_path(
                tx,
                source_id,
                *agent_id,
                external_id,
                conv,
            )? {
                return Ok(Some(existing.id));
            }
            if !allow_legacy_external_scan {
                return Ok(None);
            }

            let existing_id = tx
                .query_row_map(
                    "SELECT id
                 FROM conversations
                 WHERE source_id = ?1 AND agent_id = ?2 AND external_id = ?3",
                    fparams![source_id.as_str(), *agent_id, external_id.as_str()],
                    |row| row.get_typed(0),
                )
                .optional()?;
            if let Some(existing_id) = existing_id {
                let tail_state = franken_existing_conversation_append_tail_state(tx, existing_id)?;
                franken_insert_external_conversation_tail_lookup_key(
                    tx,
                    &lookup_key,
                    existing_id,
                    tail_state.and_then(|state| state.ended_at),
                    tail_state.map(|state| state.last_message_idx),
                    tail_state.map(|state| state.last_message_created_at),
                )?;
                Ok(Some(existing_id))
            } else {
                Ok(None)
            }
        }
        PendingConversationKey::SourcePath {
            source_id,
            agent_id,
            source_path,
            started_at,
        } => {
            let exact_match = tx
                .query_row_map(
                    "SELECT c.id
                     FROM conversations c
                     WHERE c.source_id = ?1
                       AND c.agent_id = ?2
                       AND c.source_path = ?3
                       AND ((
                            COALESCE(
                                c.started_at,
                                (SELECT MIN(created_at)
                                 FROM messages
                                 WHERE conversation_id = c.id
                                   AND created_at IS NOT NULL)
                            ) IS NULL
                            AND ?4 IS NULL
                       ) OR COALESCE(
                            c.started_at,
                            (SELECT MIN(created_at)
                             FROM messages
                             WHERE conversation_id = c.id
                               AND created_at IS NOT NULL)
                       ) = ?4)
                     ORDER BY c.id
                     LIMIT 1",
                    fparams![
                        source_id.as_str(),
                        *agent_id,
                        source_path.as_str(),
                        *started_at
                    ],
                    |row| row.get_typed(0),
                )
                .optional()?;
            if exact_match.is_some() {
                return Ok(exact_match);
            }

            let Some(conv) = conv else {
                return Ok(None);
            };
            let incoming_fingerprints = conversation_message_fingerprints(conv);
            if incoming_fingerprints.is_empty() {
                return Ok(None);
            }
            let incoming_replay_fingerprints = conversation_message_replay_cores(conv);

            let candidates: Vec<(i64, Option<i64>)> = tx.query_map_collect(
                "SELECT
                     c.id,
                     COALESCE(
                         c.started_at,
                         (SELECT MIN(created_at)
                          FROM messages
                          WHERE conversation_id = c.id
                            AND created_at IS NOT NULL)
                     ) AS effective_started_at
                 FROM conversations c
                 WHERE c.source_id = ?1
                   AND c.agent_id = ?2
                   AND c.source_path = ?3
                 ORDER BY c.id",
                fparams![source_id.as_str(), *agent_id, source_path.as_str()],
                |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
            )?;

            let mut best_candidate: Option<(i64, ConversationMergeEvidence)> = None;
            for (candidate_id, candidate_started_at) in candidates {
                let existing_fingerprints =
                    franken_existing_message_fingerprints(tx, candidate_id)?;
                let existing_replay_fingerprints =
                    replay_cores_from_merge_set(&existing_fingerprints);
                let Some(evidence) = conversation_merge_evidence(
                    &incoming_fingerprints,
                    &incoming_replay_fingerprints,
                    &existing_fingerprints,
                    &existing_replay_fingerprints,
                    *started_at,
                    candidate_started_at,
                ) else {
                    continue;
                };

                let candidate_key = (
                    evidence.exact_overlap,
                    evidence.replay_overlap,
                    evidence.started_close,
                    evidence.smaller_replay_set,
                    std::cmp::Reverse(evidence.start_distance_ms),
                );
                let should_replace = best_candidate
                    .as_ref()
                    .map(|(_, best_evidence)| {
                        candidate_key
                            > (
                                best_evidence.exact_overlap,
                                best_evidence.replay_overlap,
                                best_evidence.started_close,
                                best_evidence.smaller_replay_set,
                                std::cmp::Reverse(best_evidence.start_distance_ms),
                            )
                    })
                    .unwrap_or(true);

                if should_replace {
                    best_candidate = Some((candidate_id, evidence));
                }
            }

            Ok(best_candidate.map(|(candidate_id, _)| candidate_id))
        }
    }
}

pub(super) fn franken_insert_conversation_or_get_existing(
    tx: &FrankenTransaction<'_>,
    agent_id: i64,
    workspace_id: Option<i64>,
    conv: &Conversation,
) -> Result<ConversationInsertStatus> {
    let conversation_key = conversation_merge_key(agent_id, conv);
    if let Some(existing_id) =
        franken_find_existing_conversation_by_key(tx, &conversation_key, Some(conv))?
    {
        return Ok(ConversationInsertStatus::Existing(existing_id));
    }

    franken_insert_conversation_or_get_existing_after_miss(
        tx,
        agent_id,
        workspace_id,
        conv,
        &conversation_key,
    )
}

pub(super) fn franken_insert_conversation_or_get_existing_after_miss(
    tx: &FrankenTransaction<'_>,
    agent_id: i64,
    workspace_id: Option<i64>,
    conv: &Conversation,
    conversation_key: &PendingConversationKey,
) -> Result<ConversationInsertStatus> {
    match franken_insert_conversation(tx, agent_id, workspace_id, conv) {
        Ok(Some(conv_id)) => Ok(ConversationInsertStatus::Inserted(conv_id)),
        Ok(None) => {
            // A concurrent writer won the unique-provenance race. Resolve the
            // canonical row so callers can merge messages into it.
            let existing_id =
                franken_find_existing_conversation_by_key_after_conflict(
                    tx,
                    conversation_key,
                    Some(conv),
                )?
                    .with_context(|| {
                        format!(
                            "conversation INSERT produced a duplicate conflict but existing row was not found for source_id={} agent_id={} external_id={:?} source_path={}",
                            conv.source_id,
                            agent_id,
                            conv.external_id,
                            conv.source_path.display()
                        )
                    })?;
            tracing::warn!(
                source_id = %conv.source_id,
                agent_id,
                external_id = ?conv.external_id,
                existing_id,
                source_path = %conv.source_path.display(),
                "conversation INSERT: duplicate gracefully recovered, reusing existing row"
            );
            Ok(ConversationInsertStatus::Existing(existing_id))
        }
        Err(error) => {
            tracing::error!(
                source_id = %conv.source_id,
                agent_id,
                external_id = ?conv.external_id,
                error = %error,
                source_path = %conv.source_path.display(),
                "franken_insert_conversation failed"
            );
            Err(error)
        }
    }
}

/// Insert a conversation into the DB within a storage transaction.
///
/// Uses a plain `INSERT` so the common miss path stays on the slim direct
/// insert lane. Duplicate provenance conflicts are converted into `Ok(None)`
/// so callers can recover the canonical row and merge messages into it.
pub(super) fn franken_insert_conversation(
    tx: &FrankenTransaction<'_>,
    agent_id: i64,
    workspace_id: Option<i64>,
    conv: &Conversation,
) -> Result<Option<i64>> {
    let (metadata_json_str, metadata_bin) = franken_metadata_insert_payload(&conv.metadata_json)?;
    let (last_message_idx, last_message_created_at) = conversation_tail_state(conv);
    let metadata_bin_bytes = metadata_bin.as_deref();

    match tx.execute_compat(
        "INSERT INTO conversations(
            agent_id, workspace_id, source_id, external_id, title, source_path,
            started_at, ended_at, approx_tokens, metadata_json, origin_host, metadata_bin,
            last_message_idx, last_message_created_at
        ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
        fparams![
            agent_id,
            workspace_id,
            conv.source_id.as_str(),
            conv.external_id.as_deref(),
            conv.title.as_deref(),
            path_to_string(&conv.source_path),
            conv.started_at,
            conv.ended_at,
            conv.approx_tokens,
            metadata_json_str.as_deref(),
            conv.origin_host.as_deref(),
            metadata_bin_bytes,
            last_message_idx,
            last_message_created_at
        ],
    ) {
        Ok(_) => {
            let conv_id = franken_last_rowid(tx)?;
            franken_insert_conversation_tail_state(
                tx,
                conv_id,
                conv.ended_at,
                last_message_idx,
                last_message_created_at,
            )?;
            if let Some(external_id) = conv.external_id.as_deref() {
                franken_insert_external_conversation_tail_lookup(
                    tx,
                    conv.source_id.as_str(),
                    agent_id,
                    external_id,
                    ExistingConversationWithTail {
                        id: conv_id,
                        tail_state: existing_conversation_tail_state_from_cached(
                            last_message_idx,
                            last_message_created_at,
                            conv.ended_at,
                        ),
                    },
                )?;
            }
            Ok(Some(conv_id))
        }
        Err(crate::franken_sync::FrankenError::UniqueViolation { .. }) => {
            tracing::debug!(
                source_id = %conv.source_id,
                agent_id,
                external_id = ?conv.external_id,
                source_path = %conv.source_path.display(),
                "conversation INSERT: duplicate provenance conflict"
            );
            Ok(None)
        }
        Err(error) => Err(error.into()),
    }
}
