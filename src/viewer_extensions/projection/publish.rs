use std::collections::HashMap;

use anyhow::{Context, Result};
use serde_json::Value;

use crate::franken_sync::compat::{ParamValue, RowExt};
use crate::franken_sync::{Connection, TransactionBehavior};
use crate::model::conversation_packet::{ConversationPacket, ConversationPacketProvenance};
use crate::model::types::Conversation;
use crate::viewer_extensions::facts::{FactsDb, extract_session_facts};
use crate::viewer_extensions::providers::events;

use super::parts::{project_session_structure, source_key};
use super::{ProjectionPublishOutcome, StructuralPublication};

// ---------------------------------------------------------------------------
// Publication
// ---------------------------------------------------------------------------

pub(super) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

/// Idempotent structural upsert keyed on `(projection_version,
/// source_revision)` in `viewer_session_projection`. A changed projection is
/// a deterministic delete+replace inside the caller's transaction plus a
/// `projection_revision` bump — same logical-revision discipline as the
/// canonical observer triggers (never bumped on a skip).
pub fn publish_session_structure(
    db: &impl FactsDb,
    conversation_id: i64,
    publication: &StructuralPublication,
) -> Result<ProjectionPublishOutcome> {
    if db
        .facts_query_map(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='viewer_session_projection' LIMIT 1",
            &[],
            |_| Ok(()),
        )?
        .is_empty()
    {
        return Ok(ProjectionPublishOutcome::ExtensionNotInstalled);
    }

    let existing = db.facts_query_map(
        "SELECT projection_version, source_revision, extractor_version \
         FROM viewer_session_projection WHERE conversation_id = ?1",
        &[ParamValue::from(conversation_id)],
        |row| {
            Ok((
                row.get_typed::<i64>(0)?,
                row.get_typed::<i64>(1)?,
                row.get_typed::<i64>(2)?,
            ))
        },
    )?;
    if existing
        .first()
        .is_some_and(|&(version, revision, extractor)| {
            version == publication.projection_version
                && revision == publication.source_revision
                && extractor == publication.extractor_version
        })
    {
        return Ok(ProjectionPublishOutcome::SkippedUnchanged);
    }

    // Canonical anchor resolution: packet message ids when replayed,
    // else the idx→id join against committed rows. Unresolved anchors stay
    // NULL (source_idx remains the honest coordinate).
    let mut id_by_idx: HashMap<i64, i64> = HashMap::new();
    for row in db.facts_query_map(
        "SELECT id, idx FROM messages WHERE conversation_id = ?1",
        &[ParamValue::from(conversation_id)],
        |row| Ok((row.get_typed::<i64>(0)?, row.get_typed::<i64>(1)?)),
    )? {
        id_by_idx.entry(row.1).or_insert(row.0);
    }
    let resolve = |packet_id: Option<i64>, idx: i64| -> Option<i64> {
        packet_id.or_else(|| id_by_idx.get(&idx).copied())
    };
    let keyed =
        |packet_id: Option<i64>, idx: i64| -> String { source_key(resolve(packet_id, idx), idx) };

    for table in [
        "viewer_step_links",
        "viewer_turn_links",
        "viewer_tool_calls",
    ] {
        db.facts_execute(
            &format!("DELETE FROM {table} WHERE conversation_id = ?1"),
            &[ParamValue::from(conversation_id)],
        )?;
    }

    for link in &publication.steps {
        let message_id = resolve(link.message_id, link.source_idx);
        db.facts_execute(
            "INSERT INTO viewer_step_links (\
                conversation_id, step_index, turn_index, step_kind, link_index, \
                scope, block_index, call_id, source_idx, source_key, message_id, \
                part_key\
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
            &[
                ParamValue::from(conversation_id),
                ParamValue::from(link.step_index),
                ParamValue::from(link.turn_index),
                ParamValue::from(link.step_kind.as_str()),
                ParamValue::from(link.link_index),
                ParamValue::from(link.scope.as_str()),
                ParamValue::from(link.block_index),
                ParamValue::from(link.call_id.as_deref()),
                ParamValue::from(link.source_idx),
                ParamValue::from(keyed(link.message_id, link.source_idx)),
                ParamValue::from(message_id),
                ParamValue::from(link.part_key.as_deref()),
            ],
        )?;
    }
    for turn in &publication.turns {
        let message_id = turn
            .source_idx
            .and_then(|idx| resolve(turn.message_id, idx));
        db.facts_execute(
            "INSERT INTO viewer_turn_links (\
                conversation_id, turn_index, turn_kind, first_step_index, \
                step_count, source_idx, message_id, source_key\
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
            &[
                ParamValue::from(conversation_id),
                ParamValue::from(turn.turn_index),
                ParamValue::from(turn.turn_kind.as_str()),
                ParamValue::from(turn.first_step_index),
                ParamValue::from(turn.step_count),
                ParamValue::from(turn.source_idx),
                ParamValue::from(message_id),
                ParamValue::from(turn.source_idx.map(|idx| keyed(turn.message_id, idx))),
            ],
        )?;
    }
    for tool in &publication.tool_calls {
        let call_message_id = resolve(tool.call_message_id, tool.call_idx);
        let result_message_id = tool
            .result_idx
            .and_then(|idx| resolve(tool.result_message_id, idx));
        db.facts_execute(
            "INSERT INTO viewer_tool_calls (\
                conversation_id, call_id, raw_call_id, name, step_index, \
                turn_index, has_result, is_error, exit_code, file_path, \
                call_idx, call_message_id, result_idx, result_message_id, \
                result_block_index, ordinal\
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)",
            &[
                ParamValue::from(conversation_id),
                ParamValue::from(tool.call_id.as_str()),
                ParamValue::from(tool.raw_call_id.as_str()),
                ParamValue::from(tool.name.as_str()),
                ParamValue::from(tool.step_index),
                ParamValue::from(tool.turn_index),
                ParamValue::from(i64::from(tool.has_result)),
                ParamValue::from(tool.is_error.map(i64::from)),
                ParamValue::from(tool.exit_code),
                ParamValue::from(tool.file_path.as_deref()),
                ParamValue::from(tool.call_idx),
                ParamValue::from(call_message_id),
                ParamValue::from(tool.result_idx),
                ParamValue::from(result_message_id),
                ParamValue::from(tool.result_block_index),
                ParamValue::from(tool.ordinal),
            ],
        )?;
    }

    db.facts_execute(
        "INSERT OR REPLACE INTO viewer_session_projection (\
            conversation_id, projection_version, extractor_version, \
            source_revision, step_count, turn_count, tool_call_count, \
            projected_at_ms\
         ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
        &[
            ParamValue::from(conversation_id),
            ParamValue::from(publication.projection_version),
            ParamValue::from(publication.extractor_version),
            ParamValue::from(publication.source_revision),
            ParamValue::from(
                publication
                    .steps
                    .iter()
                    .map(|link| link.step_index)
                    .max()
                    .unwrap_or(0),
            ),
            ParamValue::from(publication.turns.len() as i64),
            ParamValue::from(publication.tool_calls.len() as i64),
            ParamValue::from(now_ms()),
        ],
    )?;

    // Logical projection revision: bumps only on a committed content
    // change, inside the same transaction.
    db.facts_execute(
        "INSERT OR IGNORE INTO viewer_revisions (id) VALUES (1)",
        &[],
    )?;
    db.facts_execute(
        "UPDATE viewer_revisions SET projection_revision = projection_revision + 1, \
         updated_at_ms = ?1 WHERE id = 1",
        &[ParamValue::from(now_ms())],
    )?;
    Ok(ProjectionPublishOutcome::Published)
}

pub(super) fn provenance_of(conv: &Conversation) -> ConversationPacketProvenance {
    let origin_kind = conv
        .metadata_json
        .pointer("/cass/origin/kind")
        .and_then(Value::as_str)
        .unwrap_or("local")
        .to_string();
    ConversationPacketProvenance {
        source_id: conv.source_id.clone(),
        origin_kind,
        origin_host: conv.origin_host.clone(),
    }
}

/// Structural publication for an internal canonical `Conversation` — the
/// same payload bytes that were committed.
pub fn structure_for_canonical(conv: &Conversation) -> StructuralPublication {
    let packet = ConversationPacket::from_canonical_replay(conv, provenance_of(conv));
    let facts = extract_session_facts(&packet, &conv.agent_slug);
    project_session_structure(&packet, &facts)
}

/// Adjacent-transaction structural publication used by the ingest hook:
/// one explicit transaction on the writer connection, idempotent on
/// `(projection_version, source_revision)`. Errors are logged, never
/// fatal — missing links read as `unavailable` sections rather than
/// aborting the canonical ingest commit that already succeeded.
pub fn publish_structure_for_canonical_best_effort(
    connection: &Connection,
    conv: &Conversation,
    conversation_id: i64,
) {
    let result = (|| -> Result<ProjectionPublishOutcome> {
        let mut tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .context("opening viewer structure projection transaction")?;
        let outcome =
            publish_session_structure(&tx, conversation_id, &structure_for_canonical(conv))?;
        tx.commit()
            .context("committing viewer structure projection transaction")?;
        Ok(outcome)
    })();
    if let Err(error) = result {
        tracing::warn!(
            conversation_id,
            error = %error,
            "viewer structure projection publication failed; detail structure remains unavailable"
        );
    }
}

/// Batched sibling of [`publish_structure_for_canonical_best_effort`]: one
/// adjacent transaction covers every `(conversation, conversation_id)`
/// pair so the serial ingest path pays a single commit.
pub fn publish_structure_for_canonical_batch_best_effort<'a>(
    connection: &Connection,
    pairs: impl IntoIterator<Item = (&'a Conversation, i64)>,
) {
    let result = (|| -> Result<()> {
        let mut tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .context("opening viewer structure projection batch transaction")?;
        for (conv, conversation_id) in pairs {
            publish_session_structure(&tx, conversation_id, &structure_for_canonical(conv))?;
        }
        tx.commit()
            .context("committing viewer structure projection batch transaction")?;
        Ok(())
    })();
    if let Err(error) = result {
        tracing::warn!(
            error = %error,
            "viewer structure projection batch publication failed; detail structure remains unavailable"
        );
    }
}

/// `events` re-export guard: keep the provider event normalization the
/// single source of `key/idx/message_id` mapping for future consumers.
#[allow(dead_code)]
fn _packet_events(packet: &ConversationPacket) -> usize {
    events(packet).len()
}
