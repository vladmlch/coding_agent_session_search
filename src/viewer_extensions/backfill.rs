//! Task 14: extension backfill for archives indexed before the profile ran.
//!
//! `viewer_session_facts` and the structural projection are populated inside
//! the canonical ingest pass (the `persist_conversation`/batch hooks call
//! the best-effort publishers on the just-committed `Conversation`). An
//! archive indexed BEFORE `--init-extensions` ever ran never saw that pass,
//! so its extension tables stayed empty forever and `search_sessions`/
//! `list_sessions` honestly refused `facts_pending`/`data_unavailable`.
//!
//! This module is the repair lane: it replays committed canonical rows
//! through the SAME packet reconstruction the ingest hooks use —
//! `facts::facts_for_canonical` / `projection::structure_for_canonical`
//! build the `ConversationPacket` via
//! `ConversationPacket::from_canonical_replay`, so the only work here is
//! faithful hydration of the committed `Conversation` (messages AND
//! snippets: snippet fields feed `semantic_hash`, so skipping them would
//! key the backfilled rows on a different `source_revision` than ingest
//! would have produced). No source JSONL is re-parsed.
//!
//! Guarantees:
//! * Bounded: one explicit transaction per `batch_size` conversations;
//!   each conversation publishes under a SAVEPOINT so a single bad row
//!   neither aborts the batch nor commits half a publication.
//! * Extension-tables only: canonical tables are read, never written, so
//!   `canonical_revision` cannot move; `projection_revision` bumps only on
//!   a genuinely changed projection (the publish functions' own rule).
//! * Idempotent/resumable: the publishers' `(extractor_version |
//!   projection_version, source_revision)` keys make an interrupted run
//!   safe to re-run — no cursor table; `--backfill-limit` merely bounds
//!   how many conversations one run scans.
//! * Honest: a conversation whose publish fails is rolled back to its
//!   savepoint, counted in [`BackfillStats::failures`], and re-derived on
//!   the next run.

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use serde_json::Value;

use crate::franken_sync::compat::{ParamValue, RowExt, TransactionExt};
use crate::franken_sync::{Connection, Row};
use crate::model::types::{Conversation, Message, MessageRole, Snippet};

use super::facts::{self, FactPublishOutcome, FactsDb};
use super::projection::{self, ProjectionPublishOutcome};

/// Conversations per backfill transaction. Matches the batching granularity
/// the serial ingest path already commits at, and bounds both the
/// transaction's WAL footprint and the work a crash has to redo.
pub const DEFAULT_BATCH_SIZE: usize = 256;

/// Per-run controls for [`backfill_viewer_extensions`].
#[derive(Debug, Clone)]
pub struct BackfillOptions {
    /// Maximum conversations hydrated + published per transaction.
    pub batch_size: usize,
    /// Optional cap on conversations scanned in this run (testing and
    /// operator control on very large archives). A capped run is fully
    /// resumable: committed rows keep their idempotence keys and the next
    /// run skips them as `SkippedUnchanged`.
    pub limit: Option<usize>,
}

impl Default for BackfillOptions {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_BATCH_SIZE,
            limit: None,
        }
    }
}

/// Outcome counters for one [`backfill_viewer_extensions`] run. A run that
/// published nothing new reports `*_skipped_unchanged` for every row — the
/// observable `SkippedUnchanged` semantics the re-run contract requires.
#[derive(Debug, Default)]
pub struct BackfillStats {
    /// Canonical conversations scanned (attempted), including failures.
    pub conversations_scanned: usize,
    /// `viewer_session_facts` rows inserted or deterministically replaced.
    pub facts_published: usize,
    /// Facts rows skipped because `(extractor_version, source_revision)`
    /// already matched — the idempotent re-run marker.
    pub facts_skipped_unchanged: usize,
    /// Projection row-sets inserted or deterministically replaced.
    pub structure_published: usize,
    /// Projections skipped on `(projection_version, source_revision,
    /// extractor_version)`.
    pub structure_skipped_unchanged: usize,
    /// Conversations whose publication failed and was rolled back to its
    /// savepoint — each also carries a durable `failed` ingest receipt in
    /// `viewer_meta` (the exact reason) until a later run reconciles it.
    /// They are retried by the next run.
    pub failures: usize,
    /// Canonical ids of the failed conversations (for operator review).
    pub failed_conversation_ids: Vec<i64>,
    /// Sessions that carried a NON-`complete` ingest receipt (`pending`
    /// or `failed`) before this run and are now durably `complete` — the
    /// reconciliation signal for interrupted ingest publications. A hard
    /// ingest crash (kill before the generation commit) leaves no receipt
    /// at all, so those sessions converge as ordinary `published` work
    /// through the same revision-keyed scan.
    pub reconciled: usize,
    /// `limit` was reached mid-archive; more work remains for a later run.
    pub limit_reached: bool,
    /// Browse sort tie keys (`viewer_session_sort.tie_key`) the end-of-run
    /// convergence resolved to a verified public identity.
    pub sort_ties_resolved: usize,
    /// Tie rows whose identity recomputation refused — the refusal kind is
    /// persisted in `tie_error` so browse surfaces the same typed refusal
    /// the oracle would.
    pub sort_ties_refused: usize,
}

// ---------------------------------------------------------------------------
// Canonical hydration: rebuild the `Conversation` the ingest commit wrote,
// column-for-column with `FrankenStorage::list_conversations` +
// `fetch_messages`, plus the `snippets` child rows the packet hash needs.
// ---------------------------------------------------------------------------

/// `conversation_tail_state` is a later-schema additive table; archives
/// migrated from pre-v15 may not have it. Probe once per run (mirrors
/// `FrankenStorage::conversation_tail_state_table_present`).
fn tail_state_present(db: &impl FactsDb) -> Result<bool> {
    Ok(!db
        .facts_query_map(
            "SELECT 1 FROM sqlite_master WHERE type='table' \
             AND name='conversation_tail_state' LIMIT 1",
            &[],
            |_| Ok(()),
        )?
        .is_empty())
}

fn table_exists(db: &impl FactsDb, name: &str) -> Result<bool> {
    Ok(!db
        .facts_query_map(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1 LIMIT 1",
            &[ParamValue::from(name)],
            |_| Ok(()),
        )?
        .is_empty())
}

/// `deserialize_msgpack_to_json` semantics from `storage/sqlite.rs`: a
/// binary column is the newer metadata/extra encoding; undecodable bytes
/// degrade to an empty object, never a fabricated payload.
fn msgpack_to_json(bytes: &[u8]) -> Value {
    if bytes.is_empty() {
        return Value::Object(serde_json::Map::new());
    }
    rmp_serde::from_slice(bytes).unwrap_or_else(|error| {
        tracing::debug!(
            error = %error,
            bytes_len = bytes.len(),
            "extension backfill: msgpack decode failed; using empty object"
        );
        Value::Object(serde_json::Map::new())
    })
}

/// `franken_read_metadata_compat`: prefer `metadata_bin`, fall back to the
/// `metadata_json` text column, else an empty object.
fn read_metadata(row: &Row, json_idx: usize, bin_idx: usize) -> Value {
    if let Ok(Some(bytes)) = row.get_typed::<Option<Vec<u8>>>(bin_idx)
        && !bytes.is_empty()
    {
        return msgpack_to_json(&bytes);
    }
    if let Ok(Some(text)) = row.get_typed::<Option<String>>(json_idx) {
        return serde_json::from_str(&text)
            .unwrap_or_else(|_| Value::Object(serde_json::Map::new()));
    }
    Value::Object(serde_json::Map::new())
}

/// `franken_read_message_extra_compat`: prefer `extra_bin`, fall back to
/// `extra_json`, else NULL.
fn read_extra(row: &Row, json_idx: usize, bin_idx: usize) -> Value {
    if let Ok(Some(bytes)) = row.get_typed::<Option<Vec<u8>>>(bin_idx)
        && !bytes.is_empty()
    {
        return msgpack_to_json(&bytes);
    }
    if let Ok(Some(text)) = row.get_typed::<Option<String>>(json_idx) {
        return serde_json::from_str(&text).unwrap_or(Value::Null);
    }
    Value::Null
}

/// The `map_to_internal` provenance derivation (`indexer::extract_
/// provenance`): `metadata.cass.origin.{source_id,kind,host}` normalized
/// through the shared index rules. Re-deriving from the COMMITTED metadata
/// reproduces exactly the `(source_id, origin_host)` pair the ingest-time
/// `Conversation` carried — including the local-source-with-host edge the
/// storage-normalized `conversations.source_id`/`origin_host` columns do
/// not retain verbatim.
fn provenance_from_metadata(metadata: &Value) -> (String, Option<String>) {
    let cass_origin = metadata.get("cass").and_then(|cass| cass.get("origin"));
    let raw_source_id = cass_origin
        .and_then(|origin| origin.get("source_id"))
        .and_then(Value::as_str);
    let raw_origin_kind = cass_origin
        .and_then(|origin| origin.get("kind"))
        .and_then(Value::as_str);
    let origin_host = crate::search::tantivy::normalized_index_origin_host(
        cass_origin
            .and_then(|origin| origin.get("host"))
            .and_then(Value::as_str),
    );
    let source_id = crate::search::tantivy::normalized_index_source_id(
        raw_source_id,
        raw_origin_kind,
        origin_host.as_deref(),
    );
    (source_id, origin_host)
}

/// `role_from_str` (storage/sqlite.rs): the committed role text decodes
/// back to the same `MessageRole` `map_role` produced at ingest.
fn role_from_str(role: &str) -> MessageRole {
    match role {
        "user" => MessageRole::User,
        "agent" | "assistant" => MessageRole::Agent,
        "tool" => MessageRole::Tool,
        "system" => MessageRole::System,
        other => MessageRole::Other(other.to_string()),
    }
}

/// Keyset-paged canonical conversation rows — same column mapping as
/// `FrankenStorage::list_conversations` (correlated dimension lookups
/// instead of a JOIN, tail-state-preferred `ended_at`, `unknown` agent
/// sentinel) but ordered by `id` so bounded batches page through the
/// archive without OFFSET.
fn list_conversation_page(
    db: &impl FactsDb,
    after_id: i64,
    limit: i64,
    tail_state: bool,
) -> Result<Vec<Conversation>> {
    let ended_at_projection = if tail_state {
        "COALESCE(\
             (SELECT ts.ended_at FROM conversation_tail_state ts \
              WHERE ts.conversation_id = c.id),\
             c.ended_at\
         )"
    } else {
        "c.ended_at"
    };
    db.facts_query_map(
        &format!(
            "SELECT c.id, \
                 COALESCE((SELECT a.slug FROM agents a WHERE a.id = c.agent_id), 'unknown'), \
                 (SELECT w.path FROM workspaces w WHERE w.id = c.workspace_id), \
                 c.external_id, c.title, c.source_path, c.started_at, \
                 {ended_at_projection}, c.approx_tokens, \
                 c.metadata_json, c.source_id, c.origin_host, c.metadata_bin \
             FROM conversations c \
             WHERE c.id > ?1 \
             ORDER BY c.id \
             LIMIT ?2"
        ),
        &[ParamValue::from(after_id), ParamValue::from(limit)],
        |row| {
            let workspace_path: Option<String> = row.get_typed(2)?;
            let source_path: String = row.get_typed(5)?;
            let metadata_json = read_metadata(row, 9, 12);
            let (source_id, origin_host) = provenance_from_metadata(&metadata_json);
            Ok(Conversation {
                id: Some(row.get_typed(0)?),
                agent_slug: row.get_typed(1)?,
                workspace: workspace_path.map(PathBuf::from),
                external_id: row.get_typed(3)?,
                title: row.get_typed(4)?,
                source_path: PathBuf::from(source_path),
                started_at: row.get_typed(6)?,
                ended_at: row.get_typed(7)?,
                approx_tokens: row.get_typed(8)?,
                metadata_json,
                messages: Vec::new(),
                source_id,
                origin_host,
            })
        },
    )
    .context("listing canonical conversations for extension backfill")
}

/// Committed messages for one conversation — the `fetch_messages` column
/// set (`id, idx, role, author, created_at, content, extra_json,
/// extra_bin`) in canonical `idx` order — joined with the `snippets`
/// child rows so the replayed packet hashes identically to the ingest one.
fn hydrate_messages(db: &impl FactsDb, conversation_id: i64) -> Result<Vec<Message>> {
    let mut messages = db.facts_query_map(
        "SELECT id, idx, role, author, created_at, content, extra_json, extra_bin \
         FROM messages WHERE conversation_id = ?1 ORDER BY idx",
        &[ParamValue::from(conversation_id)],
        |row| {
            Ok(Message {
                id: Some(row.get_typed(0)?),
                idx: row.get_typed(1)?,
                role: role_from_str(&row.get_typed::<String>(2)?),
                author: row.get_typed(3)?,
                created_at: row.get_typed(4)?,
                content: row.get_typed(5)?,
                extra_json: read_extra(row, 6, 7),
                snippets: Vec::new(),
            })
        },
    )?;
    if messages.is_empty() || !table_exists(db, "snippets")? {
        return Ok(messages);
    }
    let mut snippets_by_message: HashMap<i64, Vec<Snippet>> = HashMap::new();
    for row in db.facts_query_map(
        "SELECT s.message_id, s.id, s.file_path, s.start_line, s.end_line, \
                s.language, s.snippet_text \
         FROM snippets s JOIN messages m ON m.id = s.message_id \
         WHERE m.conversation_id = ?1 \
         ORDER BY s.message_id, s.id",
        &[ParamValue::from(conversation_id)],
        |row| {
            Ok((
                row.get_typed::<i64>(0)?,
                Snippet {
                    id: Some(row.get_typed(1)?),
                    file_path: row.get_typed::<Option<String>>(2)?.map(PathBuf::from),
                    start_line: row.get_typed(3)?,
                    end_line: row.get_typed(4)?,
                    language: row.get_typed(5)?,
                    snippet_text: row.get_typed(6)?,
                },
            ))
        },
    )? {
        snippets_by_message.entry(row.0).or_default().push(row.1);
    }
    for message in &mut messages {
        if let Some(id) = message.id
            && let Some(snippets) = snippets_by_message.remove(&id)
        {
            message.snippets = snippets;
        }
    }
    Ok(messages)
}

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

const CONVERSATION_SAVEPOINT: &str = "cass_viewer_backfill_conv";

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

/// Publish one coherent extension generation for a hydrated canonical
/// conversation under a savepoint, through the SAME
/// `publish_ingest_generation_in_tx` body the ingest hooks run — one
/// shared replay, one `source_revision`, facts + structure + parent fixup
/// + the `complete` receipt inside the generation unit. A durable
///   `complete` receipt at this exact generation is the resumable skip key.
///
/// A failed conversation rolls back to its savepoint and leaves a DURABLE
/// `failed` ingest receipt (written outside the savepoint but inside the
/// batch transaction) with the exact reason — the pending/failed receipt
/// set the next run reconciles.
fn publish_one(
    tx: &crate::franken_sync::compat::Transaction<'_>,
    conv: &Conversation,
    conversation_id: i64,
    stats: &mut BackfillStats,
) -> Result<()> {
    // Outer-scope so a failure mark can bind what was actually attempted.
    let mut attempted_revision = 0_i64;
    let mut attempted_attempts = 1_i64;
    tx.execute(&format!("SAVEPOINT {CONVERSATION_SAVEPOINT}"))?;
    let result = (|| -> Result<()> {
        // One shared replay → the coherent (source_revision) pair.
        let generation = projection::generation_for_canonical(tx, conv, conversation_id)?;
        let source_revision = generation.facts.source_revision;
        attempted_revision = source_revision;

        let previous = facts::ingest_receipt_for(tx, conversation_id)?;
        attempted_attempts = previous
            .as_ref()
            .map(|receipt| receipt.attempts + 1)
            .unwrap_or(1);
        let was_incomplete = previous
            .as_ref()
            .is_some_and(|receipt| receipt.state != facts::RECEIPT_COMPLETE);
        if let Some(receipt) = &previous
            && receipt.state == facts::RECEIPT_COMPLETE
            && receipt.source_revision == source_revision
            && receipt.extractor_version == facts::FACTS_EXTRACTOR_VERSION
            && receipt.projection_version == projection::PROJECTION_VERSION
            // `complete` is only honest while both lane markers are durably
            // visible — a receipt over deleted/lost rows is divergence the
            // republish below repairs in the same commit.
            && projection::generation_markers_visible(tx, conversation_id, source_revision)?
        {
            stats.facts_skipped_unchanged += 1;
            stats.structure_skipped_unchanged += 1;
            return Ok(());
        }

        let mut receipt = facts::IngestReceipt {
            conversation_id,
            state: facts::RECEIPT_PENDING.to_string(),
            source_revision,
            extractor_version: facts::FACTS_EXTRACTOR_VERSION,
            projection_version: projection::PROJECTION_VERSION,
            reason: None,
            attempts: attempted_attempts,
            updated_at_ms: now_ms(),
        };
        // The pending mark rides the savepoint: it is only durable when
        // the rest of the generation is (a batch crash leaves no false
        // state behind — this conversation simply retries next run).
        facts::write_ingest_receipt(tx, &receipt)?;
        receipt.state = facts::RECEIPT_COMPLETE.to_string();
        let (facts_outcome, structure_outcome) = projection::publish_ingest_generation_in_tx(
            tx,
            conv,
            conversation_id,
            &generation,
            &receipt,
        )?;
        match facts_outcome {
            FactPublishOutcome::Published => stats.facts_published += 1,
            FactPublishOutcome::SkippedUnchanged => stats.facts_skipped_unchanged += 1,
            FactPublishOutcome::ExtensionNotInstalled => {
                bail!("viewer_session_facts missing inside an installed profile")
            }
        }
        match structure_outcome {
            ProjectionPublishOutcome::Published => stats.structure_published += 1,
            ProjectionPublishOutcome::SkippedUnchanged => stats.structure_skipped_unchanged += 1,
            ProjectionPublishOutcome::ExtensionNotInstalled => {
                bail!("viewer_session_projection missing inside an installed profile")
            }
        }
        if was_incomplete {
            stats.reconciled += 1;
        }
        Ok(())
    })();
    match result {
        Ok(()) => {
            tx.execute(&format!("RELEASE SAVEPOINT {CONVERSATION_SAVEPOINT}"))?;
            Ok(())
        }
        Err(error) => {
            // Roll the failed conversation back; the batch and its other
            // conversations still commit. Then leave a durable `failed`
            // receipt — outside the rolled-back savepoint, inside the
            // batch transaction — so the interruption is auditable and
            // the next run's incomplete-receipt set names it.
            let cleanup = tx.execute(&format!(
                "ROLLBACK TO SAVEPOINT {CONVERSATION_SAVEPOINT}; \
                 RELEASE SAVEPOINT {CONVERSATION_SAVEPOINT}"
            ));
            if let Err(rollback_error) = cleanup {
                return Err(error.context(format!(
                    "backfill savepoint rollback also failed: {rollback_error}"
                )));
            }
            let failed = facts::IngestReceipt {
                conversation_id,
                state: facts::RECEIPT_FAILED.to_string(),
                source_revision: attempted_revision,
                extractor_version: facts::FACTS_EXTRACTOR_VERSION,
                projection_version: projection::PROJECTION_VERSION,
                reason: Some(format!("{error:#}")),
                attempts: attempted_attempts,
                updated_at_ms: now_ms(),
            };
            if let Err(mark_error) = facts::write_ingest_receipt(tx, &failed) {
                return Err(error.context(format!(
                    "failed to record the publication failure durably: {mark_error}"
                )));
            }
            Err(error)
        }
    }
}

/// Replay committed canonical conversations through the ingest-time facts
/// and structure publications. Requires the `viewer_*` profile to be
/// installed already (the `--backfill-viewer-extensions` flag handler runs
/// `schema::ensure_schema` + `revisions::ensure_observers` first).
///
/// `progress` runs once per committed batch with the cumulative stats so a
/// long run reports incrementally. The end-of-run journal
/// `notify_committed_change` is the CALLER's job — one pass for the whole
/// run, not one per batch (a watching bridge sees a single coherent
/// publication).
pub fn backfill_viewer_extensions(
    connection: &Connection,
    options: &BackfillOptions,
    mut progress: impl FnMut(&BackfillStats),
) -> Result<BackfillStats> {
    for table in ["viewer_session_facts", "viewer_session_projection"] {
        if !table_exists(connection, table)? {
            bail!(
                "viewer extension profile is not installed ({table} missing); \
                 run --init-extensions first"
            );
        }
    }
    connection
        .execute("PRAGMA foreign_keys = ON")
        .context("enabling foreign key enforcement for extension backfill")?;
    let batch_size = options.batch_size.max(1);
    let tail_state = tail_state_present(connection)?;

    let mut stats = BackfillStats::default();
    let mut after_id = 0_i64;
    loop {
        let remaining = options
            .limit
            .map(|limit| limit.saturating_sub(stats.conversations_scanned));
        if remaining == Some(0) {
            stats.limit_reached = true;
            break;
        }
        let fetch = remaining.map_or(batch_size, |rest| rest.min(batch_size)) as i64;
        let page = list_conversation_page(connection, after_id, fetch, tail_state)?;
        if page.is_empty() {
            break;
        }
        after_id = page.last().and_then(|conv| conv.id).unwrap_or(after_id);

        // Hydrate outside the write transaction: the reads pay no WAL
        // footprint and the commit window stays short.
        let mut batch = Vec::with_capacity(page.len());
        for mut conv in page {
            let conversation_id = conv.id.expect("paged rows carry canonical ids");
            conv.messages = hydrate_messages(connection, conversation_id).with_context(|| {
                format!("hydrating canonical messages for conversation {conversation_id}")
            })?;
            batch.push(conv);
        }

        let mut tx = connection
            .transaction()
            .context("opening extension backfill batch transaction")?;
        for conv in &batch {
            let conversation_id = conv.id.expect("paged rows carry canonical ids");
            if let Err(error) = publish_one(&tx, conv, conversation_id, &mut stats) {
                stats.failures += 1;
                stats.failed_conversation_ids.push(conversation_id);
                tracing::warn!(
                    conversation_id,
                    error = %error,
                    "extension backfill publication failed; \
                     the conversation is retried by the next run"
                );
            }
            stats.conversations_scanned += 1;
        }
        tx.commit()
            .context("committing extension backfill batch transaction")?;
        progress(&stats);
    }
    // The publication loop republishes facts/structure only — browse sort
    // mirrors are maintained by the schema-v5 triggers, but the public tie
    // key needs the identity recomputation the triggers cannot express.
    // Converge every NULL remainder here so a completed backfill leaves no
    // `facts_pending`/`identity_conflict` browse refusals undiagnosed.
    let sort =
        super::sort::repair_sort_keys(connection).context("converging browse sort tie keys")?;
    stats.sort_ties_resolved = sort.resolved;
    stats.sort_ties_refused = sort.refused;
    Ok(stats)
}
