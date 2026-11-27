use super::backup_and_integrity::{franken_read_message_extra_compat, serialize_json_to_msgpack};
use super::batch_ingest::franken_last_rowid;
use super::conversation_tail_state::{
    collect_append_only_tail_messages,
    collect_existing_conversation_noop_from_conversation_ended_at,
    collect_existing_conversation_noop_from_idx_tail,
    collect_existing_conversation_tail_from_ended_at,
    franken_cached_existing_conversation_tail_metadata, franken_existing_conversation_ended_at,
    trace_existing_conversation_lookup_fallback,
};
use super::fts_rebuild::{FTS5_BATCH_SIZE, FtsEntry, path_to_string, role_as_str};
use super::open_and_locks::{
    record_message_lookup_bounded_queries, record_message_lookup_full_scan_query,
};
#[cfg(test)]
use super::reconcile_and_codebuff::MessageInsertSubstageProfile;
use super::reconcile_and_codebuff::{
    ExistingConversationNewMessages, MessageMergeFingerprint, MessageReplayCore,
    MessageReplayFingerprintSet, collect_new_messages_for_existing_conversation,
    message_merge_fingerprint, message_native_id, native_id_from_extra, reconciled_native_messages,
    role_from_str,
};
use super::source_and_historical_import::historical_raw_json;
use super::storage_core::FrankenStorage;
use crate::franken_sync::{
    Connection as FrankenConnection, Row as FrankenRow, SqliteValue,
    compat::{RowExt as FrankenRowExt, Transaction as FrankenTransaction},
};
use crate::model::types::{Conversation, Message, Snippet};
use anyhow::{Context, Result};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
#[cfg(test)]
use std::time::Instant;

pub(super) type MetadataInsertPayload<'a> = (Option<Cow<'a, str>>, Option<Vec<u8>>);

pub(super) fn franken_metadata_insert_payload(
    value: &serde_json::Value,
) -> Result<MetadataInsertPayload<'_>> {
    if let Some(raw) = historical_raw_json(value) {
        Ok((Some(Cow::Borrowed(raw)), None))
    } else if value.is_null() {
        Ok((Some(Cow::Borrowed("null")), None))
    } else if value.as_object().is_some_and(|object| object.is_empty()) {
        Ok((None, None))
    } else if let Some(metadata_bin) = serialize_json_to_msgpack(value) {
        Ok((None, Some(metadata_bin)))
    } else {
        Ok((Some(Cow::Owned(serde_json::to_string(value)?)), None))
    }
}

fn franken_insert_new_message(
    tx: &FrankenTransaction<'_>,
    conversation_id: i64,
    msg: &Message,
) -> Result<i64> {
    let (extra_json_str, extra_bin) = franken_message_insert_payload(msg)?;
    let extra_bin_bytes = extra_bin.as_deref();

    tx.execute_compat(
        "INSERT INTO messages(conversation_id, idx, role, author, created_at, content, extra_json, extra_bin)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            fparams![
                conversation_id,
                msg.idx,
                role_as_str(&msg.role),
                msg.author.as_deref(),
                msg.created_at,
                msg.content.as_str(),
                extra_json_str.as_deref(),
                extra_bin_bytes
        ],
    )?;
    franken_last_rowid(tx)
}

fn franken_insert_new_message_ignore_duplicate(
    tx: &FrankenTransaction<'_>,
    conversation_id: i64,
    msg: &Message,
) -> Result<Option<i64>> {
    let (extra_json_str, extra_bin) = franken_message_insert_payload(msg)?;
    let extra_bin_bytes = extra_bin.as_deref();

    let changed = tx.execute_compat(
        "INSERT OR IGNORE INTO messages(conversation_id, idx, role, author, created_at, content, extra_json, extra_bin)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            fparams![
                conversation_id,
                msg.idx,
                role_as_str(&msg.role),
                msg.author.as_deref(),
                msg.created_at,
                msg.content.as_str(),
                extra_json_str.as_deref(),
                extra_bin_bytes
        ],
    )?;
    if changed == 0 {
        return Ok(None);
    }
    franken_last_rowid(tx).map(Some)
}

pub(super) type MessageInsertPayload<'a> = (Option<Cow<'a, str>>, Option<Vec<u8>>);

pub(super) fn franken_message_insert_payload(msg: &Message) -> Result<MessageInsertPayload<'_>> {
    if let Some(raw) = historical_raw_json(&msg.extra_json) {
        Ok((Some(Cow::Borrowed(raw)), None))
    } else if msg.extra_json.is_null() {
        Ok((None, None))
    } else {
        let extra_bin = serialize_json_to_msgpack(&msg.extra_json);
        if extra_bin.is_some() {
            Ok((None, extra_bin))
        } else {
            Ok((
                Some(Cow::Owned(serde_json::to_string(&msg.extra_json)?)),
                None,
            ))
        }
    }
}

/// Batch size for proven-new message inserts.
///
/// Each row binds 8 values, so 100 rows stays well under SQLite's default
/// `SQLITE_MAX_VARIABLE_NUMBER` limit of 999 while still amortizing parse cost.
const MESSAGE_INSERT_BATCH_SIZE: usize = 100;

/// Append workloads profile fastest with 50-row chunks on SQLite.
///
/// After the tail-state hot table removed conversation-row rewrites from the
/// append path, 50-row chunks beat the old 20-row setting on the append-merge
/// profile. 100-row chunks slightly regress the 20-message workload.
const APPEND_MESSAGE_INSERT_BATCH_SIZE: usize = 50;

fn message_insert_batch_sql(row_count: usize) -> &'static str {
    static MESSAGE_INSERT_BATCH_SQL: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();

    let max_batch_size = MESSAGE_INSERT_BATCH_SIZE.max(APPEND_MESSAGE_INSERT_BATCH_SIZE);
    let cached_sql = MESSAGE_INSERT_BATCH_SQL.get_or_init(|| {
        let mut sql_by_row_count = Vec::with_capacity(max_batch_size + 1);
        sql_by_row_count.push(String::new());
        for row_count in 1..=max_batch_size {
            let placeholders = (0..row_count)
                .map(|idx| {
                    let base = idx * 8;
                    format!(
                        "(?{},?{},?{},?{},?{},?{},?{},?{})",
                        base + 1,
                        base + 2,
                        base + 3,
                        base + 4,
                        base + 5,
                        base + 6,
                        base + 7,
                        base + 8
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            sql_by_row_count.push(format!(
                "INSERT INTO messages(conversation_id, idx, role, author, created_at, content, extra_json, extra_bin) VALUES {placeholders}"
            ));
        }
        sql_by_row_count
    });

    cached_sql
        .get(row_count)
        .map(String::as_str)
        .expect("message insert batch size must be covered by the cached SQL table")
}

pub(super) fn franken_batch_insert_new_messages(
    tx: &FrankenTransaction<'_>,
    conversation_id: i64,
    messages: &[&Message],
) -> Result<Vec<i64>> {
    franken_batch_insert_new_messages_with_batch_size(
        tx,
        conversation_id,
        messages,
        MESSAGE_INSERT_BATCH_SIZE,
    )
}

pub(super) fn franken_append_insert_new_messages<'a>(
    tx: &FrankenTransaction<'_>,
    conversation_id: i64,
    messages: &[&'a Message],
    conv: &Conversation,
) -> Result<Vec<(i64, &'a Message)>> {
    let mut inserted = Vec::with_capacity(messages.len());
    for msg in messages {
        if matches!(conv.agent_slug.as_str(), "grok_bot" | "codebuff") {
            // Native-ID reconciliation proved this index new in this
            // transaction. A conflicting writer must abort/retry the packet,
            // never silently discard its new native entry via OR IGNORE.
            inserted.push((franken_insert_new_message(tx, conversation_id, msg)?, *msg));
            continue;
        }
        if let Some(message_id) =
            franken_insert_new_message_ignore_duplicate(tx, conversation_id, msg)?
        {
            inserted.push((message_id, *msg));
        }
    }
    Ok(inserted)
}

fn franken_batch_insert_new_messages_with_batch_size(
    tx: &FrankenTransaction<'_>,
    conversation_id: i64,
    messages: &[&Message],
    batch_size: usize,
) -> Result<Vec<i64>> {
    let batch_size = batch_size.max(1);
    let mut inserted_ids = Vec::with_capacity(messages.len());
    for chunk in messages.chunks(batch_size) {
        if chunk.len() == 1 {
            inserted_ids.push(franken_insert_new_message(tx, conversation_id, chunk[0])?);
            continue;
        }
        let sql = message_insert_batch_sql(chunk.len());

        let mut param_values: Vec<SqliteValue> = Vec::with_capacity(chunk.len() * 8);
        for msg in chunk {
            let (extra_json_str, extra_bin) = franken_message_insert_payload(msg)?;
            param_values.push(SqliteValue::from(conversation_id));
            param_values.push(SqliteValue::from(msg.idx));
            param_values.push(SqliteValue::from(role_as_str(&msg.role)));
            param_values.push(SqliteValue::from(msg.author.as_deref()));
            param_values.push(SqliteValue::from(msg.created_at));
            param_values.push(SqliteValue::from(msg.content.as_str()));
            param_values.push(SqliteValue::from(extra_json_str.as_deref()));
            param_values.push(SqliteValue::from(extra_bin.as_deref()));
        }

        tx.execute_with_params(sql, &param_values)?;

        let last_id = franken_last_rowid(tx)?;
        let first_id = last_id
            .checked_sub((chunk.len() - 1) as i64)
            .with_context(|| {
                format!(
                    "inferring rowid range for {}-row message batch ending at {last_id}",
                    chunk.len()
                )
            })?;
        inserted_ids.extend((0..chunk.len()).map(|offset| first_id + offset as i64));
    }

    Ok(inserted_ids)
}

#[cfg(test)]
fn franken_insert_new_message_with_profile(
    tx: &FrankenTransaction<'_>,
    conversation_id: i64,
    msg: &Message,
    profile: &mut MessageInsertSubstageProfile,
) -> Result<i64> {
    profile.single_row_calls += 1;
    profile.batch_rows += 1;

    let payload_start = Instant::now();
    let (extra_json_str, extra_bin) = franken_message_insert_payload(msg)?;
    profile.payload_duration += payload_start.elapsed();
    let extra_bin_bytes = extra_bin.as_deref();

    let execute_start = Instant::now();
    tx.execute_compat(
        "INSERT INTO messages(conversation_id, idx, role, author, created_at, content, extra_json, extra_bin)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            fparams![
                conversation_id,
                msg.idx,
                role_as_str(&msg.role),
                msg.author.as_deref(),
                msg.created_at,
                msg.content.as_str(),
                extra_json_str.as_deref(),
                extra_bin_bytes
        ],
    )?;
    profile.execute_duration += execute_start.elapsed();

    let rowid_start = Instant::now();
    let rowid = franken_last_rowid(tx)?;
    profile.rowid_duration += rowid_start.elapsed();
    Ok(rowid)
}

#[cfg(test)]
pub(super) fn franken_batch_insert_new_messages_with_profile(
    tx: &FrankenTransaction<'_>,
    conversation_id: i64,
    messages: &[&Message],
    profile: &mut MessageInsertSubstageProfile,
) -> Result<Vec<i64>> {
    franken_batch_insert_new_messages_with_profile_batch_size(
        tx,
        conversation_id,
        messages,
        profile,
        MESSAGE_INSERT_BATCH_SIZE,
    )
}

#[cfg(test)]
pub(super) fn franken_append_insert_new_messages_with_profile(
    tx: &FrankenTransaction<'_>,
    conversation_id: i64,
    messages: &[&Message],
    profile: &mut MessageInsertSubstageProfile,
) -> Result<Vec<i64>> {
    franken_batch_insert_new_messages_with_profile_batch_size(
        tx,
        conversation_id,
        messages,
        profile,
        APPEND_MESSAGE_INSERT_BATCH_SIZE,
    )
}

#[cfg(test)]
fn franken_batch_insert_new_messages_with_profile_batch_size(
    tx: &FrankenTransaction<'_>,
    conversation_id: i64,
    messages: &[&Message],
    profile: &mut MessageInsertSubstageProfile,
    batch_size: usize,
) -> Result<Vec<i64>> {
    let batch_size = batch_size.max(1);
    let mut inserted_ids = Vec::with_capacity(messages.len());
    for chunk in messages.chunks(batch_size) {
        if chunk.len() == 1 {
            inserted_ids.push(franken_insert_new_message_with_profile(
                tx,
                conversation_id,
                chunk[0],
                profile,
            )?);
            continue;
        }

        profile.batch_calls += 1;
        profile.batch_rows += chunk.len();

        let sql_build_start = Instant::now();
        let sql = message_insert_batch_sql(chunk.len());
        profile.sql_build_duration += sql_build_start.elapsed();

        let mut param_values: Vec<SqliteValue> = Vec::with_capacity(chunk.len() * 8);
        for msg in chunk {
            let payload_start = Instant::now();
            let (extra_json_str, extra_bin) = franken_message_insert_payload(msg)?;
            profile.payload_duration += payload_start.elapsed();

            let param_build_start = Instant::now();
            param_values.push(SqliteValue::from(conversation_id));
            param_values.push(SqliteValue::from(msg.idx));
            param_values.push(SqliteValue::from(role_as_str(&msg.role)));
            param_values.push(SqliteValue::from(msg.author.as_deref()));
            param_values.push(SqliteValue::from(msg.created_at));
            param_values.push(SqliteValue::from(msg.content.as_str()));
            param_values.push(SqliteValue::from(extra_json_str.as_deref()));
            param_values.push(SqliteValue::from(extra_bin.as_deref()));
            profile.param_build_duration += param_build_start.elapsed();
        }

        let execute_start = Instant::now();
        tx.execute_with_params(sql, &param_values)?;
        profile.execute_duration += execute_start.elapsed();

        let rowid_start = Instant::now();
        let last_id = franken_last_rowid(tx)?;
        let first_id = last_id
            .checked_sub((chunk.len() - 1) as i64)
            .with_context(|| {
                format!(
                    "inferring rowid range for {}-row message batch ending at {last_id}",
                    chunk.len()
                )
            })?;
        inserted_ids.extend((0..chunk.len()).map(|offset| first_id + offset as i64));
        profile.rowid_duration += rowid_start.elapsed();
    }

    Ok(inserted_ids)
}

/// Insert snippets within a SQLite transaction.
pub(super) fn franken_insert_snippets(
    tx: &FrankenTransaction<'_>,
    message_id: i64,
    snippets: &[Snippet],
) -> Result<()> {
    for snip in snippets {
        let file_path_str = snip.file_path.as_ref().map(path_to_string);
        tx.execute_compat(
            "INSERT INTO snippets(message_id, file_path, start_line, end_line, language, snippet_text)
             VALUES(?1,?2,?3,?4,?5,?6)",
            fparams![
                message_id,
                file_path_str.as_deref(),
                snip.start_line,
                snip.end_line,
                snip.language.as_deref(),
                snip.snippet_text.as_deref()
            ],
        )?;
    }
    Ok(())
}

pub(super) fn franken_existing_message_fingerprints(
    tx: &FrankenTransaction<'_>,
    conversation_id: i64,
) -> Result<HashSet<MessageMergeFingerprint>> {
    let rows = tx.query_params(
        "SELECT idx, role, author, created_at, content
         FROM messages
         WHERE conversation_id = ?1",
        fparams![conversation_id],
    )?;
    let mut fingerprints = HashSet::with_capacity(rows.len());
    for row in rows {
        let role: String = row.get_typed(1)?;
        let content: String = row.get_typed(4)?;
        fingerprints.insert(MessageMergeFingerprint {
            idx: row.get_typed(0)?,
            created_at: row.get_typed(3)?,
            role: role_from_str(&role),
            author: row.get_typed(2)?,
            content_hash: *blake3::hash(content.as_bytes()).as_bytes(),
        });
    }
    Ok(fingerprints)
}

pub(super) struct ExistingMessageLookup {
    pub(super) by_idx: HashMap<i64, MessageMergeFingerprint>,
    pub(super) replay: MessageReplayFingerprintSet,
}

fn existing_message_lookup_from_rows(
    rows: Vec<FrankenRow>,
    min_idx: i64,
    max_idx: i64,
    created_bounds: Option<(i64, i64)>,
    replay_full_scan: bool,
    include_row_native_ids: bool,
) -> Result<ExistingMessageLookup> {
    let mut by_idx = HashMap::with_capacity(rows.len());
    let mut replay = MessageReplayFingerprintSet::default();
    for row in rows {
        let idx: i64 = row.get_typed(0)?;
        let role: String = row.get_typed(1)?;
        let author: Option<String> = row.get_typed(2)?;
        let created_at: Option<i64> = row.get_typed(3)?;
        let content: String = row.get_typed(4)?;
        let role = role_from_str(&role);
        let content_hash = *blake3::hash(content.as_bytes()).as_bytes();

        if idx >= min_idx && idx <= max_idx {
            by_idx.insert(
                idx,
                MessageMergeFingerprint {
                    idx,
                    created_at,
                    role: role.clone(),
                    author: author.clone(),
                    content_hash,
                },
            );
        }

        let replay_matches = if replay_full_scan {
            true
        } else if let Some((min_created_at, max_created_at)) = created_bounds {
            created_at.is_some_and(|ts| ts >= min_created_at && ts <= max_created_at)
        } else {
            true
        };
        if replay_matches {
            let native_id = include_row_native_ids
                .then(|| native_id_from_extra(&franken_read_message_extra_compat(&row, 5, 6)))
                .flatten();
            replay.insert_parts(
                MessageReplayCore {
                    created_at,
                    role,
                    author,
                    content_hash,
                },
                native_id,
            );
        }
    }
    Ok(ExistingMessageLookup { by_idx, replay })
}

pub(super) fn franken_existing_message_lookup(
    tx: &FrankenTransaction<'_>,
    conversation_id: i64,
    incoming_messages: &[Message],
) -> Result<ExistingMessageLookup> {
    if incoming_messages.is_empty() {
        return Ok(ExistingMessageLookup {
            by_idx: HashMap::new(),
            replay: MessageReplayFingerprintSet::default(),
        });
    }

    let has_conv_idx = tx
        .query_params("PRAGMA index_list(messages)", fparams![])
        .map(|rows| {
            rows.into_iter().any(|row| {
                row.get_typed::<String>(1).ok().as_deref() == Some("idx_messages_conv_idx")
            })
        })
        .unwrap_or(false);
    let index_hint = if has_conv_idx {
        " INDEXED BY idx_messages_conv_idx"
    } else {
        ""
    };

    let min_idx = incoming_messages
        .iter()
        .map(|msg| msg.idx)
        .min()
        .unwrap_or(0);
    let max_idx = incoming_messages
        .iter()
        .map(|msg| msg.idx)
        .max()
        .unwrap_or(min_idx);
    let idx_rows_sql = format!(
        "SELECT idx
         FROM messages{index_hint}
         WHERE conversation_id = ?1
           AND idx >= ?2
           AND idx <= ?3"
    );
    let idx_rows = tx.query_params(&idx_rows_sql, fparams![conversation_id, min_idx, max_idx])?;
    record_message_lookup_bounded_queries(1, idx_rows.len());

    let mut existing_indices = HashSet::with_capacity(idx_rows.len());
    for row in idx_rows {
        let idx: i64 = row.get_typed(0)?;
        existing_indices.insert(idx);
    }

    let mut by_idx = HashMap::with_capacity(incoming_messages.len().min(existing_indices.len()));
    let mut missing_messages = Vec::new();
    for msg in incoming_messages {
        if existing_indices.contains(&msg.idx) {
            // Same-idx messages are skipped by merge policy even when content has
            // diverged. Use the incoming fingerprint as a lightweight presence
            // marker so normal reprocessing does not need to read stored content.
            by_idx.insert(msg.idx, message_merge_fingerprint(msg));
        } else {
            missing_messages.push(msg);
        }
    }

    if missing_messages.is_empty() {
        return Ok(ExistingMessageLookup {
            by_idx,
            replay: MessageReplayFingerprintSet::default(),
        });
    }

    // Native-id replay disambiguation only pays off — and the stored-extra
    // read only costs — when the incoming batch itself carries native ids.
    let include_row_native_ids = missing_messages
        .iter()
        .any(|msg| message_native_id(msg).is_some());

    let requires_full_scan = missing_messages.iter().any(|msg| msg.created_at.is_none());
    let created_bounds = missing_messages
        .iter()
        .filter_map(|msg| msg.created_at)
        .fold(None, |bounds: Option<(i64, i64)>, created_at| {
            Some(match bounds {
                Some((min_created_at, max_created_at)) => (
                    min_created_at.min(created_at),
                    max_created_at.max(created_at),
                ),
                None => (created_at, created_at),
            })
        });

    let (full_scan_sql, bounded_sql) = if include_row_native_ids {
        (
            format!(
                "SELECT idx, role, author, created_at, content, extra_json, extra_bin
                 FROM messages{index_hint}
                 WHERE conversation_id = ?1"
            ),
            format!(
                "SELECT idx, role, author, created_at, content, extra_json, extra_bin
                 FROM messages{index_hint}
                 WHERE conversation_id = ?1
                   AND created_at IS NOT NULL
                   AND created_at >= ?2
                   AND created_at <= ?3"
            ),
        )
    } else {
        (
            format!(
                "SELECT idx, role, author, created_at, content
                 FROM messages{index_hint}
                 WHERE conversation_id = ?1"
            ),
            format!(
                "SELECT idx, role, author, created_at, content
                 FROM messages{index_hint}
                 WHERE conversation_id = ?1
                   AND created_at IS NOT NULL
                   AND created_at >= ?2
                   AND created_at <= ?3"
            ),
        )
    };

    let mut replay = MessageReplayFingerprintSet::default();
    if requires_full_scan {
        let rows = tx.query_params(&full_scan_sql, fparams![conversation_id])?;
        record_message_lookup_full_scan_query(rows.len());
        let content_lookup = existing_message_lookup_from_rows(
            rows,
            min_idx,
            max_idx,
            created_bounds,
            true,
            include_row_native_ids,
        )?;
        by_idx.extend(content_lookup.by_idx);
        replay.absorb(content_lookup.replay);
    } else if let Some((min_created_at, max_created_at)) = created_bounds {
        let rows = tx.query_params(
            &bounded_sql,
            fparams![conversation_id, min_created_at, max_created_at],
        )?;
        record_message_lookup_bounded_queries(1, rows.len());
        let created_lookup = existing_message_lookup_from_rows(
            rows,
            min_idx,
            max_idx,
            created_bounds,
            false,
            include_row_native_ids,
        )?;
        by_idx.extend(created_lookup.by_idx);
        replay.absorb(created_lookup.replay);
    }

    Ok(ExistingMessageLookup { by_idx, replay })
}

fn franken_existing_message_lookup_with_pending(
    tx: &FrankenTransaction<'_>,
    conversation_id: i64,
    incoming_messages: &[Message],
    pending_message_fingerprints: &mut HashMap<i64, HashMap<i64, MessageMergeFingerprint>>,
    pending_message_replay_fingerprints: &mut HashMap<i64, MessageReplayFingerprintSet>,
) -> Result<ExistingMessageLookup> {
    if let (Some(by_idx), Some(replay)) = (
        pending_message_fingerprints.get(&conversation_id),
        pending_message_replay_fingerprints.get(&conversation_id),
    ) {
        if incoming_messages
            .iter()
            .all(|msg| by_idx.contains_key(&msg.idx) || replay.contains_message(msg))
        {
            return Ok(ExistingMessageLookup {
                by_idx: by_idx.clone(),
                replay: replay.clone(),
            });
        }

        let fresh = franken_existing_message_lookup(tx, conversation_id, incoming_messages)?;
        let mut merged_by_idx = by_idx.clone();
        let mut merged_replay = replay.clone();
        merged_by_idx.extend(fresh.by_idx);
        merged_replay.absorb(fresh.replay);
        pending_message_fingerprints.insert(conversation_id, merged_by_idx.clone());
        pending_message_replay_fingerprints.insert(conversation_id, merged_replay.clone());
        return Ok(ExistingMessageLookup {
            by_idx: merged_by_idx,
            replay: merged_replay,
        });
    }

    let lookup = franken_existing_message_lookup(tx, conversation_id, incoming_messages)?;
    pending_message_fingerprints.insert(conversation_id, lookup.by_idx.clone());
    pending_message_replay_fingerprints.insert(conversation_id, lookup.replay.clone());
    Ok(lookup)
}

pub(super) fn franken_collect_batched_existing_new_messages<'a>(
    tx: &FrankenTransaction<'_>,
    conversation_id: i64,
    conv: &'a Conversation,
    pending_message_fingerprints: &mut HashMap<i64, HashMap<i64, MessageMergeFingerprint>>,
    pending_message_replay_fingerprints: &mut HashMap<i64, MessageReplayFingerprintSet>,
    replay_skip_log: &'static str,
) -> Result<(
    ExistingConversationNewMessages<'a>,
    HashMap<i64, MessageMergeFingerprint>,
    MessageReplayFingerprintSet,
)> {
    if matches!(conv.agent_slug.as_str(), "grok_bot" | "codebuff") {
        return Ok((
            reconciled_native_messages(conv),
            HashMap::new(),
            MessageReplayFingerprintSet::default(),
        ));
    }
    let tail_metadata = franken_cached_existing_conversation_tail_metadata(tx, conversation_id)?;
    let tail_state = tail_metadata.complete_tail_state();
    if let Some(tail_state) = tail_state
        && let Some(tail_plan) = collect_append_only_tail_messages(
            conv,
            tail_state.last_message_idx,
            tail_state.last_message_created_at,
        )
    {
        let mut by_idx = pending_message_fingerprints
            .remove(&conversation_id)
            .unwrap_or_default();
        let mut replay = pending_message_replay_fingerprints
            .remove(&conversation_id)
            .unwrap_or_default();
        for msg in &tail_plan.messages {
            by_idx.insert(msg.idx, message_merge_fingerprint(msg));
            replay.insert_message(msg);
        }
        return Ok((tail_plan, by_idx, replay));
    }

    let timestamp_data_incomplete = tail_metadata.last_message_created_at.is_none()
        || conv.messages.iter().any(|msg| msg.created_at.is_none());
    if timestamp_data_incomplete
        && let Some(existing_ended_at) = tail_metadata.ended_at
        && let Some(noop_plan) =
            collect_existing_conversation_noop_from_conversation_ended_at(conv, existing_ended_at)
    {
        let by_idx = pending_message_fingerprints
            .remove(&conversation_id)
            .unwrap_or_default();
        let replay = pending_message_replay_fingerprints
            .remove(&conversation_id)
            .unwrap_or_default();
        return Ok((noop_plan, by_idx, replay));
    }

    if timestamp_data_incomplete
        && let Some(last_message_idx) = tail_metadata.last_message_idx
        && let Some(tail_plan) =
            collect_existing_conversation_noop_from_idx_tail(conv, last_message_idx)
    {
        let mut by_idx = pending_message_fingerprints
            .remove(&conversation_id)
            .unwrap_or_default();
        let mut replay = pending_message_replay_fingerprints
            .remove(&conversation_id)
            .unwrap_or_default();
        for msg in &tail_plan.messages {
            by_idx.insert(msg.idx, message_merge_fingerprint(msg));
            replay.insert_message(msg);
        }
        return Ok((tail_plan, by_idx, replay));
    }

    let existing_ended_at = if tail_metadata.ended_at.is_some() {
        tail_metadata.ended_at
    } else {
        franken_existing_conversation_ended_at(tx, conversation_id)?
    };
    if let Some(existing_ended_at) = existing_ended_at
        && let Some(tail_plan) =
            collect_existing_conversation_tail_from_ended_at(conv, existing_ended_at)
    {
        let mut by_idx = pending_message_fingerprints
            .remove(&conversation_id)
            .unwrap_or_default();
        let mut replay = pending_message_replay_fingerprints
            .remove(&conversation_id)
            .unwrap_or_default();
        for msg in &tail_plan.messages {
            by_idx.insert(msg.idx, message_merge_fingerprint(msg));
            replay.insert_message(msg);
        }
        return Ok((tail_plan, by_idx, replay));
    }

    trace_existing_conversation_lookup_fallback(
        conversation_id,
        conv,
        tail_state,
        existing_ended_at,
    );

    let ExistingMessageLookup {
        by_idx: mut existing_messages,
        replay: mut existing_replay_fingerprints,
    } = franken_existing_message_lookup_with_pending(
        tx,
        conversation_id,
        &conv.messages,
        pending_message_fingerprints,
        pending_message_replay_fingerprints,
    )?;
    let new_messages = collect_new_messages_for_existing_conversation(
        conversation_id,
        conv,
        &mut existing_messages,
        &mut existing_replay_fingerprints,
        replay_skip_log,
    );
    Ok((
        new_messages,
        existing_messages,
        existing_replay_fingerprints,
    ))
}

/// Batch insert FTS5 entries within a SQLite transaction.
pub(super) fn franken_batch_insert_fts(
    storage: &FrankenStorage,
    tx: &FrankenTransaction<'_>,
    entries: &[FtsEntry],
) -> Result<usize> {
    if entries.is_empty() {
        return Ok(0);
    }

    let mut inserted = 0;

    for chunk in entries.chunks(FTS5_BATCH_SIZE) {
        let placeholders: String = chunk
            .iter()
            .enumerate()
            .map(|(i, _)| {
                let base = i * 3 + 1; // +1 for 1-indexed params
                format!("(?{},?{},?{})", base, base + 1, base + 2)
            })
            .collect::<Vec<_>>()
            .join(",");

        let sql = format!("INSERT INTO fts_messages(rowid, content, title) VALUES {placeholders}");

        let mut param_values: Vec<SqliteValue> = Vec::with_capacity(chunk.len() * 3);
        for entry in chunk {
            param_values.push(SqliteValue::from(entry.message_id));
            param_values.push(SqliteValue::from(entry.content.as_str()));
            param_values.push(SqliteValue::from(entry.title.as_str()));
        }

        // Derived FTS maintenance is best-effort within the enclosing batch
        // transaction: if a batch insert fails, the canonical message rows
        // still commit and post-run parity repair rebuilds missing FTS rows.
        match tx.execute_with_params(&sql, &param_values) {
            Ok(_) => {
                inserted += chunk.len();
            }
            Err(err) => {
                if err.to_string().contains("no such table") {
                    // Fresh DB / Tantivy-authoritative path (#295): the FTS5
                    // shadow tables are not materialized yet. db-resident FTS
                    // is best-effort here, so stop probing/inserting for the
                    // rest of this process instead of emitting a warning on
                    // every batch (which pegged a core on large corpora).
                    storage.set_fts_messages_present_cache(false);
                    tracing::debug!(
                        error = %err,
                        chunk_docs = chunk.len(),
                        "fts_messages not materialized yet; skipping db-resident FTS maintenance (Tantivy is authoritative)"
                    );
                } else {
                    tracing::warn!(
                        error = %err,
                        chunk_docs = chunk.len(),
                        "SQLite FTS batch insert failed; skipping db-resident FTS maintenance because Tantivy is authoritative"
                    );
                }
                return Ok(inserted);
            }
        }
    }

    Ok(inserted)
}

pub(super) fn franken_batch_insert_fts_on_connection(
    conn: &FrankenConnection,
    entries: &[FtsEntry],
) -> Result<usize> {
    if entries.is_empty() {
        return Ok(0);
    }

    let mut inserted = 0;

    for chunk in entries.chunks(FTS5_BATCH_SIZE) {
        let placeholders: String = chunk
            .iter()
            .enumerate()
            .map(|(i, _)| {
                let base = i * 3 + 1;
                format!("(?{},?{},?{})", base, base + 1, base + 2)
            })
            .collect::<Vec<_>>()
            .join(",");

        let sql = format!("INSERT INTO fts_messages(rowid, content, title) VALUES {placeholders}");

        let mut param_values: Vec<SqliteValue> = Vec::with_capacity(chunk.len() * 3);
        for entry in chunk {
            param_values.push(SqliteValue::from(entry.message_id));
            param_values.push(SqliteValue::from(entry.content.as_str()));
            param_values.push(SqliteValue::from(entry.title.as_str()));
        }

        conn.execute_with_params(&sql, &param_values)
            .with_context(|| {
                format!(
                    "inserting {} rows into fts_messages during streaming FTS maintenance",
                    chunk.len()
                )
            })?;
        inserted += chunk.len();
    }

    Ok(inserted)
}
