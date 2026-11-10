//! Bounded replay journal for the CASS-owned Viewer change stream
//! (spec decision 7; plan Task 11).
//!
//! `viewer_change_log` is the truth. Canonical observer AFTER triggers
//! (`src/viewer_bridge/revisions.rs`, installed by `ensure_observers`)
//! append `session_created`/`session_updated`/`session_deleted` rows inside
//! the SAME commit that mutates canonical rows — including writers in other
//! processes, since triggers are schema objects. Extension-table triggers
//! on `viewer_annotations`/`viewer_tags` append `annotations_updated` inside
//! the annotation write transaction. `canonical_ready`, `index_ready` and
//! administrative `reset_required` are appended explicitly by post-commit
//! callers through [`append_change_event`].
//!
//! [`notify_committed_change`] is the post-commit maintenance pass — it is
//! callable ONLY after the writer's commit succeeded, never inside the
//! transaction it announces. It coalesces the committed burst to the latest
//! event per `(kind, conversation_id)`, stamps `canonical_ready` — the
//! canonical-progress marker, which asserts nothing about lexical
//! publication — and enforces bounded retention. A writer that crashes
//! after commit but before notify still left a fully replayable
//! (uncompacted) journal — never silently stale.
//!
//! Task 8 reserves `index_ready` for REAL lexical publication:
//! [`certify_lexical_publication`] is the ONLY writer of that kind, and it
//! runs only after a Quill commit has sealed a manifest generation. One
//! `BEGIN IMMEDIATE` transaction bumps `viewer_revisions.lexical_generation`,
//! persists the versioned [`PublicationReceipt`] into
//! `viewer_lexical_publication` and journals the `index_ready` event bound
//! to them — atomically, so a crash can never mint a generation the journal
//! cannot explain or a journal claim no durable receipt backs. Merely
//! observing canonical progress never advances `lexical_generation`.
//!
//! A separate stock indexer process MUST be able to notify a bridge: the
//! local IPC wakeup lands with Task 12; the SQLite journal alone is truth.
//! Logical revisions come from `viewer_revisions`, never from physical WAL
//! bytes.

use std::collections::HashMap;
use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};

use crate::franken_sync::compat::{ParamValue, RowExt};
use crate::franken_sync::{Connection, FrankenError, SqliteValue};
use crate::viewer_extensions::facts::FactsDb;
use crate::viewer_extensions::schema::rollback_owned_transaction;

/// Journal event kinds. Keep stable: they are the wire vocabulary the
/// Task-12 event stream and `changes_since` responses speak.
pub mod kind {
    /// A canonical `conversations` row appeared.
    pub const SESSION_CREATED: &str = "session_created";
    /// Served canonical content changed (`conversations`/`messages`/
    /// `conversation_tail_state`). A NULL `conversation_id` marks a coarse
    /// "something changed; refetch the list" signal synthesized when the
    /// journal cannot account for the committed `canonical_revision`.
    pub const SESSION_UPDATED: &str = "session_updated";
    /// A canonical `conversations` row disappeared (source deletion).
    pub const SESSION_DELETED: &str = "session_deleted";
    /// `viewer_annotations`/`viewer_tags` changed for one conversation.
    pub const ANNOTATIONS_UPDATED: &str = "annotations_updated";
    /// Post-commit marker: an ingest pass committed canonical changes and
    /// the journal is coalesced through it. `revision` binds the canonical
    /// revision the pass certifies. Canonical progress ONLY — it asserts
    /// nothing about lexical publication (Task 8).
    pub const CANONICAL_READY: &str = "canonical_ready";
    /// Lexical publication certification (Task 8): a real Quill commit
    /// sealed a manifest generation and
    /// [`certify_lexical_publication`](super::certify_lexical_publication)
    /// persisted the versioned [`PublicationReceipt`] covering it.
    /// `revision` binds the minted `lexical_generation` ordinal;
    /// `payload_json` carries the receipt itself.
    pub const INDEX_READY: &str = "index_ready";
    /// Coarse resync demand: cursor expired (bounded retention), the
    /// archive was replaced (seq regression), or an administrator
    /// explicitly appended it. Consumers refetch, then resume at
    /// `next_seq`.
    pub const RESET_REQUIRED: &str = "reset_required";
}

/// Bounded replay depth enforced by [`notify_committed_change`]. A cursor
/// older than the retained window is answered `truncated` +
/// `reset_required` — coarse refetch, never silently stale.
pub const JOURNAL_RETENTION: i64 = 10_000;

/// Hard cap on one `changes_since` page; the caller loops on `next_seq`.
pub const CHANGES_PAGE_LIMIT: usize = 1_024;

/// `viewer_meta` keys owned by this module.
const META_COMPACTED_THROUGH: &str = "viewer_change_log_compacted_through";
const META_FLOOR: &str = "viewer_change_log_floor";

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|span| span.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

/// One journal event. `seq` is `None` only on synthesized rows
/// (`reset_required`/`session_updated` coarse signals) which never advance
/// the resume cursor — only durable rows do.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct ChangeEvent {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seq: Option<i64>,
    pub kind: String,
    pub conversation_id: Option<i64>,
    /// The lane revision the event is bound to: post-bump
    /// `canonical_revision` for `session_*` and `canonical_ready`, the
    /// per-row annotation CAS revision for `annotations_updated`, the
    /// minted `lexical_generation` ordinal for `index_ready`, `0` for
    /// administrative `reset_required`.
    pub revision: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_json: Option<String>,
}

/// The four logical revisions from `viewer_revisions`, serialized in the
/// same shape as the bridge `Generation` (spec decision 12).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct JournalRevisions {
    pub canonical_revision: i64,
    pub annotation_revision: i64,
    pub projection_revision: i64,
    pub lexical_generation: i64,
}

/// One bounded replay page: `{events, next_seq, truncated,
/// current_revisions}` — the shape Task 12 binds to the `changes_since`
/// RPC.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct ChangePage {
    pub events: Vec<ChangeEvent>,
    /// Resume token: pass back as `after_seq` on the next call. Always a
    /// durable seq the caller has fully consumed (or the reset high-water),
    /// never a synthesized seq.
    pub next_seq: i64,
    /// `true` when the cursor could not be served completely — the event
    /// list then starts with a `reset_required` signal.
    pub truncated: bool,
    pub current_revisions: Option<JournalRevisions>,
}

fn journal_installed(connection: &Connection) -> Result<bool> {
    let rows = connection.query(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='viewer_change_log' LIMIT 1",
    )?;
    Ok(!rows.is_empty())
}

fn meta_int(connection: &Connection, key: &str) -> Result<Option<i64>> {
    let rows = connection.query_with_params(
        "SELECT value FROM viewer_meta WHERE key = ?1 LIMIT 1",
        &[SqliteValue::Text(key.to_owned().into())],
    )?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let text: Option<String> = row.get_typed(0)?;
    match text {
        Some(text) => text
            .parse::<i64>()
            .map(Some)
            .with_context(|| format!("viewer_meta.{key} is not an integer")),
        None => Ok(None),
    }
}

fn set_meta(connection: &Connection, key: &str, value: i64) -> Result<()> {
    connection.execute_with_params(
        "INSERT OR REPLACE INTO viewer_meta (key, value) VALUES (?1, ?2)",
        &[
            SqliteValue::Text(key.to_owned().into()),
            SqliteValue::Text(value.to_string().into()),
        ],
    )?;
    Ok(())
}

fn max_seq(connection: &Connection) -> Result<i64> {
    let value: Option<i64> = connection
        .query_row("SELECT MAX(seq) FROM viewer_change_log")?
        .get_typed(0)?;
    Ok(value.unwrap_or(0))
}

/// Highest journaled seq over CANONICAL event kinds — the `canonical_ready`
/// trigger. Session lifecycle rows are the only evidence of canonical
/// progress; annotation, marker and administrative rows above the boundary
/// coalesce without minting a canonical claim.
fn canonical_committed_seq(connection: &Connection) -> Result<i64> {
    let value: Option<i64> = connection
        .query_row(
            "SELECT MAX(seq) FROM viewer_change_log WHERE kind IN (\
                 'session_created','session_updated','session_deleted'\
             )",
        )?
        .get_typed(0)?;
    Ok(value.unwrap_or(0))
}

fn lane_revisions(connection: &Connection) -> Result<Option<JournalRevisions>> {
    let rows = connection.query(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='viewer_revisions' LIMIT 1",
    )?;
    if rows.is_empty() {
        return Ok(None);
    }
    let rows = connection.query(
        "SELECT canonical_revision,annotation_revision,projection_revision,lexical_generation \
         FROM viewer_revisions WHERE id = 1 LIMIT 2",
    )?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    Ok(Some(JournalRevisions {
        canonical_revision: row.get_typed(0)?,
        annotation_revision: row.get_typed(1)?,
        projection_revision: row.get_typed(2)?,
        lexical_generation: row.get_typed(3)?,
    }))
}

/// The highest canonical revision the journal can account for. Stamps are
/// written post-bump inside the same trigger, so a `canonical_revision`
/// higher than this watermark is a change the journal cannot explain —
/// answered with a coarse `session_updated`, never silence.
fn canonical_watermark(connection: &Connection) -> Result<i64> {
    let value: Option<i64> = connection
        .query_row(
            "SELECT MAX(revision) FROM viewer_change_log \
             WHERE kind IN ('session_created','session_updated','session_deleted')",
        )?
        .get_typed(0)?;
    Ok(value.unwrap_or(0))
}

/// Bounded replay of the committed journal.
///
/// * `after_seq` is the caller's resume token (`0` = from the retained
///   head). Rows are returned in commit order with repeated
///   `(kind, conversation_id)` events in a page coalesced to the latest —
///   a cursor never misses a changed key.
/// * `truncated = true` plus a `reset_required` event answers an expired
///   cursor (`after_seq` below the retention floor) or a seq regression
///   (archive replacement); `next_seq` is the current high-water mark so a
///   refetch can resume cleanly.
/// * `current_revisions` binds the committed logical generations at read
///   time; a canonical revision the journal cannot explain yields a
///   trailing coarse `session_updated` (`conversation_id = NULL`).
pub fn changes_since(connection: &Connection, after_seq: i64, limit: usize) -> Result<ChangePage> {
    let after = after_seq.max(0);
    let mut page = ChangePage {
        next_seq: after,
        ..ChangePage::default()
    };
    if !journal_installed(connection)? {
        return Ok(page);
    }
    page.current_revisions = lane_revisions(connection)?;
    let floor = meta_int(connection, META_FLOOR)?.unwrap_or(0);
    let high_water = max_seq(connection)?;

    if after > high_water || after < floor {
        // Expired cursor (retention floor passed it) or seq regression
        // (archive replaced: the journal restarted at a lower watermark).
        page.truncated = true;
        page.next_seq = high_water;
        page.events.push(ChangeEvent {
            seq: Some(high_water),
            kind: kind::RESET_REQUIRED.to_owned(),
            conversation_id: None,
            revision: page
                .current_revisions
                .map(|g| g.canonical_revision)
                .unwrap_or(0),
            payload_json: None,
        });
        return Ok(page);
    }

    let fetch = limit.clamp(1, CHANGES_PAGE_LIMIT) as i64;
    let rows = connection.query_with_params(
        "SELECT seq,kind,conversation_id,revision,payload_json \
         FROM viewer_change_log WHERE seq > ?1 ORDER BY seq LIMIT ?2",
        &[SqliteValue::Integer(after), SqliteValue::Integer(fetch)],
    )?;
    if let Some(last) = rows.last() {
        page.next_seq = last.get_typed(0)?;
    }

    // Coalesce within the page: keep the latest event per
    // (kind, conversation_id), preserving commit order. Refetch consumers
    // need the newest signal per key; `next_seq` already advanced past the
    // dropped rows, so nothing is re-read or skipped semantically.
    let mut latest: HashMap<(String, Option<i64>), usize> = HashMap::new();
    let mut events = Vec::with_capacity(rows.len());
    for row in &rows {
        let event = ChangeEvent {
            seq: Some(row.get_typed(0)?),
            kind: row.get_typed(1)?,
            conversation_id: row.get_typed(2)?,
            revision: row.get_typed(3)?,
            payload_json: row.get_typed(4)?,
        };
        let key = (event.kind.clone(), event.conversation_id);
        match latest.get(&key) {
            Some(&slot) => events[slot] = event,
            None => {
                latest.insert(key, events.len());
                events.push(event);
            }
        }
    }
    page.events = events;

    // Only at the journal tail can we certify coverage: if the committed
    // canonical revision outruns every canonical event the journal still
    // holds, emit a coarse refetch signal instead of silence.
    if rows.len() < fetch as usize
        && let Some(generation) = page.current_revisions
        && generation.canonical_revision > canonical_watermark(connection)?
    {
        page.events.push(ChangeEvent {
            seq: None,
            kind: kind::SESSION_UPDATED.to_owned(),
            conversation_id: None,
            revision: generation.canonical_revision,
            payload_json: None,
        });
    }
    Ok(page)
}

/// Append one event inside the caller's open transaction (or autocommit) —
/// the explicit lane for `index_ready`, `reset_required` and any event an
/// observer trigger cannot express. Returns the durable `seq`.
///
/// Commit semantics belong to the caller: a rollback drops the row with
/// the rest of the transaction, so a journal event is never observed ahead
/// of the change it announces.
pub fn append_change_event(
    connection: &Connection,
    event_kind: &str,
    conversation_id: Option<i64>,
    revision: i64,
    payload_json: Option<String>,
) -> Result<i64> {
    connection.execute_with_params(
        "INSERT INTO viewer_change_log (kind, conversation_id, revision, payload_json, created_at_ms) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        &[
            SqliteValue::Text(event_kind.to_owned().into()),
            conversation_id.map_or(SqliteValue::Null, SqliteValue::Integer),
            SqliteValue::Integer(revision),
            payload_json.map_or(SqliteValue::Null, |p| SqliteValue::Text(p.into())),
            SqliteValue::Integer(now_ms()),
        ],
    )?;
    Ok(connection.last_insert_rowid())
}

/// Post-commit journal maintenance — callable ONLY after the writer's
/// commit succeeded, never before and never inside the transaction it
/// announces. One `BEGIN IMMEDIATE` transaction:
///
/// 1. Coalesces newly committed rows (`seq` above the previous pass's
///    high-water) to the latest per `(kind, conversation_id)`.
/// 2. Stamps `canonical_ready` bound to `committed_revision` — but only
///    when the pass actually observed new events, so an idempotent
///    re-ingest emits no semantic event. Task 8: this marker certifies
///    canonical progress ONLY; `index_ready` is reserved for
///    [`certify_lexical_publication`].
/// 3. Enforces bounded retention: rows older than `high_water -
///    JOURNAL_RETENTION` are dropped and the floor recorded, so later
///    `changes_since` cursors below the floor get `reset_required`.
///
/// Returns the journal high-water `seq` after the pass — the wakeup
/// payload a Task-12 notifier may fan out over local IPC. `0` on an
/// unextended archive (nothing to maintain). `committed_revision` should
/// be the canonical revision the caller's commit produced; `0` re-reads
/// the current value.
pub fn notify_committed_change(connection: &Connection, committed_revision: i64) -> Result<i64> {
    notify_committed_change_with_retention(connection, committed_revision, JOURNAL_RETENTION)
}

/// [`notify_committed_change`] with an explicit retention bound so tests
/// can exercise the expiry path without filling the production window.
pub fn notify_committed_change_with_retention(
    connection: &Connection,
    committed_revision: i64,
    retention: i64,
) -> Result<i64> {
    if !journal_installed(connection)? {
        return Ok(0);
    }
    connection
        .execute("BEGIN IMMEDIATE")
        .context("opening change-journal notify transaction")?;
    let result = (|| -> Result<i64> {
        let boundary = meta_int(connection, META_COMPACTED_THROUGH)?.unwrap_or(0);
        let floor = meta_int(connection, META_FLOOR)?.unwrap_or(0);
        let committed = max_seq(connection)?;
        if committed > boundary {
            // A commit since the last pass journaled events: collapse the
            // burst to the latest per (kind, conversation_id). Every dropped
            // row has a kept successor with a higher seq, so this is
            // semantic-lossless for any cursor.
            connection.execute_with_params(
                "DELETE FROM viewer_change_log WHERE seq > ?1 AND seq NOT IN (\
                     SELECT MAX(seq) FROM viewer_change_log WHERE seq > ?1 \
                     GROUP BY kind, conversation_id\
                 )",
                &[SqliteValue::Integer(boundary)],
            )?;
            // Task 8: `canonical_ready` certifies CANONICAL progress only.
            // Annotation bursts (`annotations_updated`) and marker rows the
            // certification lane appended (`index_ready`, `reset_required`)
            // still coalesce above, but they must not mint a canonical
            // progress marker — the stamp is conditional on canonical
            // journal rows having landed since the boundary.
            if canonical_committed_seq(connection)? > boundary {
                // Mark the pass: canonical_ready binds the canonical
                // revision the commit certified (re-read when the caller
                // passes 0). index_ready is minted exclusively by
                // certify_lexical_publication.
                let revision = if committed_revision > 0 {
                    committed_revision
                } else {
                    lane_revisions(connection)?
                        .map(|g| g.canonical_revision)
                        .unwrap_or(0)
                };
                append_change_event(
                    connection,
                    kind::CANONICAL_READY,
                    None,
                    revision,
                    Some(format!("{{\"canonical_revision\":{revision}}}")),
                )?;
                // Only the newest canonical_ready marker is meaningful.
                connection.execute(
                    "DELETE FROM viewer_change_log WHERE kind = 'canonical_ready' AND seq < (\
                         SELECT MAX(seq) FROM viewer_change_log WHERE kind = 'canonical_ready'\
                     )",
                )?;
            }
        }
        let high_water = max_seq(connection)?;
        // Retention: expire the oldest rows past the bounded window and
        // record the floor so expired cursors reset instead of replaying
        // silently partial history.
        let expiry = high_water - retention.max(1);
        if expiry > floor {
            connection.execute_with_params(
                "DELETE FROM viewer_change_log WHERE seq <= ?1",
                &[SqliteValue::Integer(expiry)],
            )?;
            set_meta(connection, META_FLOOR, expiry)?;
        }
        set_meta(connection, META_COMPACTED_THROUGH, high_water)?;
        Ok(high_water)
    })();
    // The same ownership rule as the schema installer: a failed COMMIT can
    // leave this pass's transaction ACTIVE in the engine, and a body error
    // can too, so roll back while it is still open. Only this pass's
    // transaction can be open here — a pre-existing caller transaction
    // fails BEGIN IMMEDIATE above. The initiating error stays the cause; a
    // rollback failure is reported with it, never swallowed.
    let cleanup = |error: anyhow::Error| -> anyhow::Error {
        match rollback_owned_transaction(connection) {
            Ok(()) => error,
            Err(rollback) => error.context(format!(
                "rolling back the failed notify transaction also failed ({rollback}); \
                 the connection may still hold it open"
            )),
        }
    };
    match result {
        Ok(high_water) => match connection
            .execute("COMMIT")
            .context("committing change-journal notify transaction")
        {
            Ok(_) => Ok(high_water),
            Err(error) => Err(cleanup(error)),
        },
        Err(error) => Err(cleanup(error)),
    }
}

/// Ingest hook used by `src/indexer/mod.rs` (same style as the Task-7a
/// facts publication): runs [`notify_committed_change`] after the ingest
/// commit and never fails the pass — a missed pass only leaves the journal
/// uncompacted, which still replays correctly.
pub fn notify_committed_change_best_effort(connection: &Connection) {
    if let Err(error) = notify_committed_change(connection, 0) {
        tracing::warn!(
            error = %error,
            "viewer change-journal notify pass failed; journal remains replayable but uncompacted"
        );
    }
}

// ---------------------------------------------------------------------------
// Task 8: lexical publication certification — the ONLY `index_ready` writer.
// A certification exists exclusively when a real Quill commit already sealed
// a manifest generation; it binds that generation plus the covered revisions
// the caller's coverage floor names. Merely observing canonical progress or
// bumping a counter elsewhere never mints `lexical_generation`.
// ---------------------------------------------------------------------------

/// Version tag of the persisted [`PublicationReceipt`] payload.
pub const PUBLICATION_RECEIPT_VERSION: i64 = 1;

/// Versioned durable certification record for ONE real Quill publication.
/// Written inside the same `BEGIN IMMEDIATE` transaction that bumps
/// `viewer_revisions.lexical_generation` and journals `index_ready` — the
/// three artifacts commit atomically, so a crash can never produce a
/// generation the journal cannot explain nor a journal event no durable
/// receipt backs. `payload_json` of the `index_ready` journal row carries
/// this same serialization, so `changes_since` replay alone reconstructs
/// the certification — no side channel, no recomputation.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PublicationReceipt {
    /// Payload schema version ([`PUBLICATION_RECEIPT_VERSION`]).
    pub v: i64,
    /// Committed revisions the certified generation covers — the coverage
    /// floor captured when the publishing writer began staging, never a
    /// later wider snapshot.
    pub canonical_revision: i64,
    pub annotation_revision: i64,
    pub projection_revision: i64,
    /// Certification ordinal — `viewer_revisions.lexical_generation` after
    /// this certification's atomic bump.
    pub lexical_generation: i64,
    /// Committed Quill MANIFEST generation this receipt binds.
    pub quill_generation: i64,
    /// Live documents the certified manifest holds (durable evidence).
    pub live_doc_count: i64,
    pub certified_at_ms: i64,
}

/// The committed logical revisions — the `viewer_revisions` singleton read
/// through any [`FactsDb`] lane (`None` on an unextended archive). Writers
/// capture this as their coverage floor before staging work a later commit
/// will certify.
pub fn current_journal_revisions(db: &impl FactsDb) -> Result<Option<JournalRevisions>> {
    let rows = db.facts_query_map(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='viewer_revisions' LIMIT 1",
        &[],
        |_| Ok(()),
    )?;
    if rows.is_empty() {
        return Ok(None);
    }
    let revisions = db.facts_query_map(
        "SELECT canonical_revision,annotation_revision,projection_revision,lexical_generation \
         FROM viewer_revisions WHERE id = 1 LIMIT 2",
        &[],
        |row| {
            Ok(JournalRevisions {
                canonical_revision: row.get_typed(0)?,
                annotation_revision: row.get_typed(1)?,
                projection_revision: row.get_typed(2)?,
                lexical_generation: row.get_typed(3)?,
            })
        },
    )?;
    Ok(revisions.first().copied())
}

/// The durable [`PublicationReceipt`] singleton, if any certification has
/// committed (`None` before the first certified publication or on an
/// unextended archive). Deserialization failure surfaces as an error — a
/// receipt that cannot be decoded must never be silently certified away.
pub fn latest_lexical_publication(db: &impl FactsDb) -> Result<Option<PublicationReceipt>> {
    let rows = db.facts_query_map(
        "SELECT 1 FROM sqlite_master WHERE type='table' \
         AND name='viewer_lexical_publication' LIMIT 1",
        &[],
        |_| Ok(()),
    )?;
    if rows.is_empty() {
        return Ok(None);
    }
    let payloads = db.facts_query_map(
        "SELECT payload_json FROM viewer_lexical_publication WHERE id = 1 LIMIT 1",
        &[],
        |row| row.get_typed::<String>(0),
    )?;
    let Some(payload) = payloads.into_iter().next() else {
        return Ok(None);
    };
    serde_json::from_str(&payload)
        .map(Some)
        .context("decoding viewer_lexical_publication payload")
}

fn lexical_publication_table_present(db: &impl FactsDb) -> Result<bool> {
    let rows = db.facts_query_map(
        "SELECT 1 FROM sqlite_master WHERE type='table' \
         AND name='viewer_lexical_publication' LIMIT 1",
        &[],
        |_| Ok(()),
    )?;
    Ok(!rows.is_empty())
}

fn journal_installed_facts(db: &impl FactsDb) -> Result<bool> {
    let rows = db.facts_query_map(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='viewer_change_log' LIMIT 1",
        &[],
        |_| Ok(()),
    )?;
    Ok(!rows.is_empty())
}

/// A `ROLLBACK` that reports no open transaction still certifies the
/// connection is clean: the engine already closed the transaction the
/// caller began (some COMMIT-time failures unwind it themselves).
fn rollback_reports_no_active_transaction(error: &FrankenError) -> bool {
    match error {
        FrankenError::NoActiveTransaction => true,
        FrankenError::FunctionError(detail) => detail.contains("no transaction is active"),
        _ => false,
    }
}

/// [`rollback_owned_transaction`] for the `FactsDb` surface, which cannot
/// probe `in_transaction`: issue the rollback unconditionally and classify
/// a "no active transaction" refusal as already-clean. Only the call
/// site's own transaction is ever rolled back — a pre-existing caller
/// transaction fails `BEGIN IMMEDIATE` before this runs.
fn rollback_owned_facts_transaction(db: &impl FactsDb) -> std::result::Result<(), FrankenError> {
    match db.facts_execute("ROLLBACK", &[]) {
        Ok(_) => Ok(()),
        Err(error) if rollback_reports_no_active_transaction(&error) => Ok(()),
        Err(error) => Err(error),
    }
}

/// Certify one REAL committed Quill publication — the only lane that may
/// mint `lexical_generation` or journal `index_ready`.
///
/// Contract: the caller invokes this AFTER the Quill commit sealed the
/// manifest it names (`quill_generation`/`live_doc_count` read back from
/// the durable MANIFEST), never before and never speculatively. `floor` is
/// the coverage floor the publishing writer captured when it began
/// staging (`None` binds the revisions at certification time — the floor
/// for writers whose staging never touched the archive).
///
/// One `BEGIN IMMEDIATE` transaction commits three artifacts atomically:
/// the `viewer_revisions.lexical_generation` bump, the
/// `viewer_lexical_publication` receipt row, and the `index_ready`
/// journal event whose `revision`/`payload_json` bind them. A
/// certification identical to the durable receipt (same committed
/// generation AND same coverage) is idempotent: it mints no new ordinal
/// and appends no event — a publish retried after a crash cannot
/// fabricate a second readiness claim. Returns `None` on an unextended
/// archive (nothing to certify into).
pub fn certify_lexical_publication(
    db: &impl FactsDb,
    quill_generation: i64,
    live_doc_count: i64,
    floor: Option<JournalRevisions>,
) -> Result<Option<PublicationReceipt>> {
    if !journal_installed_facts(db)? || !lexical_publication_table_present(db)? {
        return Ok(None);
    }
    db.facts_execute("BEGIN IMMEDIATE", &[])
        .context("opening lexical certification transaction")?;
    let result = (|| -> Result<Option<PublicationReceipt>> {
        let Some(current) = current_journal_revisions(db)? else {
            return Ok(None);
        };
        // Covered revisions: the stage-time floor where one was captured,
        // clamped down to what is committed now — never a wider snapshot
        // than the floor proves this generation contains.
        let covered = floor.map_or(current, |floor| JournalRevisions {
            canonical_revision: floor.canonical_revision.min(current.canonical_revision),
            annotation_revision: floor.annotation_revision.min(current.annotation_revision),
            projection_revision: floor.projection_revision.min(current.projection_revision),
            lexical_generation: current.lexical_generation,
        });
        if let Some(previous) = latest_lexical_publication(db)?
            && previous.quill_generation == quill_generation
            && previous.canonical_revision == covered.canonical_revision
            && previous.annotation_revision == covered.annotation_revision
            && previous.projection_revision == covered.projection_revision
            && previous.live_doc_count == live_doc_count
        {
            // Same committed generation, same coverage: the certification
            // already happened — replay it, mint nothing.
            return Ok(Some(previous));
        }
        let lexical_generation = current.lexical_generation + 1;
        let now = now_ms();
        db.facts_execute(
            "UPDATE viewer_revisions \
             SET lexical_generation = ?1, updated_at_ms = ?2 WHERE id = 1",
            &[ParamValue::from(lexical_generation), ParamValue::from(now)],
        )?;
        let receipt = PublicationReceipt {
            v: PUBLICATION_RECEIPT_VERSION,
            canonical_revision: covered.canonical_revision,
            annotation_revision: covered.annotation_revision,
            projection_revision: covered.projection_revision,
            lexical_generation,
            quill_generation,
            live_doc_count,
            certified_at_ms: now,
        };
        let payload_json =
            serde_json::to_string(&receipt).context("serializing lexical publication receipt")?;
        db.facts_execute(
            "INSERT OR REPLACE INTO viewer_lexical_publication \
             (id, v, quill_generation, live_doc_count, canonical_revision, \
              annotation_revision, projection_revision, lexical_generation, \
              certified_at_ms, payload_json) \
             VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            &[
                ParamValue::from(receipt.v),
                ParamValue::from(receipt.quill_generation),
                ParamValue::from(receipt.live_doc_count),
                ParamValue::from(receipt.canonical_revision),
                ParamValue::from(receipt.annotation_revision),
                ParamValue::from(receipt.projection_revision),
                ParamValue::from(receipt.lexical_generation),
                ParamValue::from(receipt.certified_at_ms),
                ParamValue::from(payload_json.as_str()),
            ],
        )?;
        db.facts_execute(
            "INSERT INTO viewer_change_log \
             (kind, conversation_id, revision, payload_json, created_at_ms) \
             VALUES (?1, NULL, ?2, ?3, ?4)",
            &[
                ParamValue::from(kind::INDEX_READY),
                ParamValue::from(lexical_generation),
                ParamValue::from(payload_json.as_str()),
                ParamValue::from(now),
            ],
        )?;
        // Only the newest index_ready marker is meaningful — replay
        // coalesces to it anyway.
        db.facts_execute(
            "DELETE FROM viewer_change_log WHERE kind = 'index_ready' AND seq < (\
                 SELECT MAX(seq) FROM viewer_change_log WHERE kind = 'index_ready'\
             )",
            &[],
        )?;
        Ok(Some(receipt))
    })();
    // The same ownership rule as the notify pass, through the `FactsDb`
    // surface which cannot probe `in_transaction`: a failed COMMIT can
    // leave this transaction ACTIVE, so the rollback is unconditional and
    // a "no active transaction" refusal certifies the engine already
    // closed it. Only this certification's transaction can be open — a
    // pre-existing caller transaction fails BEGIN IMMEDIATE above. The
    // initiating error stays the cause; a rollback failure is reported
    // with it, never swallowed.
    let cleanup = |error: anyhow::Error| -> anyhow::Error {
        match rollback_owned_facts_transaction(db) {
            Ok(()) => error,
            Err(rollback) => error.context(format!(
                "rolling back the failed certification transaction also failed ({rollback}); \
                 the connection may still hold it open"
            )),
        }
    };
    match result {
        Ok(receipt) => match db
            .facts_execute("COMMIT", &[])
            .context("committing lexical certification transaction")
        {
            Ok(_) => Ok(receipt),
            Err(error) => Err(cleanup(error)),
        },
        Err(error) => Err(cleanup(error)),
    }
}

// ---------------------------------------------------------------------------
// Task 12: local IPC wakeup and endpoint discovery (wakeup-only — the SQLite
// journal stays the truth; a missed wakeup only delays the poll fallback).
// ---------------------------------------------------------------------------

/// Sidecar-file suffix a `cass-viewer-bridge --enable-viewer-events`
/// listener uses to publish its loopback endpoint next to the canonical
/// archive. The file lives beside the archive (not inside `data_dir`) so
/// ANY process holding the archive path — a stock `cass index` run, a
/// doctor pass — can find the wakeup door with no extra configuration.
pub const EVENT_ENDPOINT_SIDECAR_SUFFIX: &str = ".viewer_events.addr";

/// Wire `op` a wakeup sender writes on the event socket before closing.
/// The bridge event pump (`src/viewer_bridge/event_stream.rs`) treats any
/// received `notify` frame as "recheck the journal now" — it carries no
/// payload semantics, so a forged or stale wakeup costs one bounded
/// journal read, never a wrong event.
pub const EVENT_OP_NOTIFY: &str = "notify";

/// The loopback endpoint sidecar for the archive at `db`
/// (`<db>.viewer_events.addr`, JSON `{"addr","pid","epoch","protocol"}`).
pub fn event_endpoint_sidecar_path(db: &Path) -> PathBuf {
    PathBuf::from(format!("{}{}", db.display(), EVENT_ENDPOINT_SIDECAR_SUFFIX))
}

/// The journal high-water `seq` — `0` when the journal is absent or empty.
/// The Task-12 event pump binds this into `subscribed`/`heartbeat` frames.
pub fn journal_high_water(connection: &Connection) -> Result<i64> {
    if !journal_installed(connection)? {
        return Ok(0);
    }
    max_seq(connection)
}

/// Best-effort wakeup of a bridge event listener serving this archive.
/// Reads the endpoint sidecar, connects with a short bounded timeout,
/// writes one `{"op":"notify"}` frame and returns. Every failure mode is
/// swallowed: a missing/stale sidecar or refused connect only means the
/// listener's own poll fallback rechecks the journal on its next tick.
/// Called AFTER the caller's commit (same contract as
/// [`notify_committed_change_best_effort`]): a wakeup sent mid-transaction
/// could make a listener read the journal before the commit lands — the
/// journal still protects correctness (the event replays on the next
/// commit's wakeup or poll), so this is a latency issue only, never
/// a correctness one.
pub fn send_bridge_wakeup_best_effort(db: &Path) {
    let send = (|| -> Option<()> {
        let raw = std::fs::read(event_endpoint_sidecar_path(db)).ok()?;
        if raw.len() > 8 * 1024 {
            return None;
        }
        let value: serde_json::Value = serde_json::from_slice(&raw).ok()?;
        let loopback_addr: SocketAddr = value.get("addr")?.as_str()?.parse().ok()?;
        // The wakeup writer trusts only loopback endpoints: the sidecar is
        // published by the bridge's own 127.0.0.1 listener, and a tampered
        // file must not turn cass into a remote dialer.
        if !loopback_addr.ip().is_loopback() {
            return None;
        }
        let mut stream =
            TcpStream::connect_timeout(&loopback_addr, Duration::from_millis(150)).ok()?;
        let _ = stream.set_write_timeout(Some(Duration::from_millis(150)));
        stream
            .write_all(format!("{{\"op\":\"{EVENT_OP_NOTIFY}\"}}\n").as_bytes())
            .ok()?;
        Some(())
    })();
    let _ = send;
}

#[cfg(test)]
mod tests {
    //! Library-lane mirror of `tests/viewer_change_stream.rs`: the
    //! `cass-viewer-bridge` binary is not required here. The fixture
    //! installs literal observer triggers mirroring the
    //! `revisions::observer_ddl` shape (bump + journal append + body-WHERE
    //! guard); the production DDL itself is exercised end-to-end by the
    //! integration test through `ensure_observers`.

    use super::*;
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use crate::storage::sqlite::FrankenStorage;

    fn canned(id: i64, text: &str, count: i64) -> Conversation {
        Conversation {
            id: None,
            agent_slug: "codex".into(),
            workspace: None,
            external_id: Some(format!("fixture-{id}")),
            title: Some(format!("fixture-{id}")),
            source_path: format!("/missing/source-{id}.jsonl").into(),
            started_at: None,
            ended_at: None,
            approx_tokens: None,
            metadata_json: serde_json::json!({"fixture":true}),
            source_id: "local".into(),
            origin_host: None,
            messages: (0..count)
                .map(|idx| Message {
                    id: None,
                    idx,
                    role: MessageRole::Agent,
                    author: None,
                    created_at: Some(1_700_000_000_000 + idx),
                    content: text.into(),
                    extra_json: serde_json::json!({"fixture":idx}),
                    snippets: Vec::new(),
                })
                .collect(),
        }
    }

    /// Mirror of `revisions::observer_ddl`'s statement shapes: bump then a
    /// (optionally body-WHERE-guarded) journal append, plus an
    /// extension-table journal-only trigger. Kept literal so this unit lane
    /// never reaches into the binary module tree.
    fn install_mirror_observers(connection: &Connection) {
        connection
            .execute_batch(
                "CREATE TRIGGER mirror_conversations_ins AFTER INSERT ON conversations BEGIN
                     UPDATE viewer_revisions SET canonical_revision = canonical_revision + 1
                     WHERE id = 1;
                     INSERT INTO viewer_change_log (kind, conversation_id, revision, created_at_ms)
                     SELECT 'session_created', NEW.id,
                         COALESCE((SELECT canonical_revision FROM viewer_revisions WHERE id = 1), 0),
                         strftime('%s','now')*1000;
                 END;
                 CREATE TRIGGER mirror_conversations_del AFTER DELETE ON conversations BEGIN
                     UPDATE viewer_revisions SET canonical_revision = canonical_revision + 1
                     WHERE id = 1;
                     INSERT INTO viewer_change_log (kind, conversation_id, revision, created_at_ms)
                     SELECT 'session_deleted', OLD.id,
                         COALESCE((SELECT canonical_revision FROM viewer_revisions WHERE id = 1), 0),
                         strftime('%s','now')*1000;
                 END;
                 CREATE TRIGGER mirror_conversations_upd AFTER UPDATE ON conversations BEGIN
                     UPDATE viewer_revisions SET canonical_revision = canonical_revision + 1
                     WHERE id = 1 AND (OLD.source_path IS NOT NEW.source_path OR OLD.title IS NOT NEW.title);
                     INSERT INTO viewer_change_log (kind, conversation_id, revision, created_at_ms)
                     SELECT 'session_updated', NEW.id,
                         COALESCE((SELECT canonical_revision FROM viewer_revisions WHERE id = 1), 0),
                         strftime('%s','now')*1000
                     WHERE (OLD.source_path IS NOT NEW.source_path OR OLD.title IS NOT NEW.title);
                 END;
                 CREATE TRIGGER mirror_messages_ins AFTER INSERT ON messages BEGIN
                     UPDATE viewer_revisions SET canonical_revision = canonical_revision + 1
                     WHERE id = 1;
                     INSERT INTO viewer_change_log (kind, conversation_id, revision, created_at_ms)
                     SELECT 'session_updated', NEW.conversation_id,
                         COALESCE((SELECT canonical_revision FROM viewer_revisions WHERE id = 1), 0),
                         strftime('%s','now')*1000;
                 END;
                 CREATE TRIGGER mirror_annotations_ins AFTER INSERT ON viewer_annotations BEGIN
                     INSERT INTO viewer_change_log (kind, conversation_id, revision, created_at_ms)
                     SELECT 'annotations_updated', NEW.conversation_id, NEW.revision,
                         strftime('%s','now')*1000;
                 END;",
            )
            .expect("mirror observer triggers");
    }

    fn fixture() -> (tempfile::TempDir, std::path::PathBuf, FrankenStorage) {
        let temp = tempfile::tempdir().expect("tempdir");
        let db = temp.path().join("archive.db");
        let storage = FrankenStorage::open(&db).expect("canonical schema");
        crate::viewer_extensions::schema::ensure_schema(storage.raw()).expect("extension schema");
        install_mirror_observers(storage.raw());
        (temp, db, storage)
    }

    fn agent(storage: &FrankenStorage) -> i64 {
        storage
            .ensure_agent(&Agent {
                id: None,
                slug: "codex".into(),
                name: "Codex".into(),
                version: None,
                kind: AgentKind::Cli,
            })
            .expect("agent")
    }

    #[test]
    fn committed_canonical_write_journals_and_notifies() -> Result<()> {
        let (_temp, _db, storage) = fixture();
        let outcome =
            storage.insert_conversation_tree(agent(&storage), None, &canned(1, "alpha", 2))?;
        let reader = storage.raw();
        let page = changes_since(reader, 0, 64)?;
        assert!(
            !page.events.is_empty(),
            "canonical insert must journal events"
        );
        assert!(
            page.events.iter().any(|e| e.kind == kind::SESSION_CREATED
                && e.conversation_id == Some(outcome.conversation_id)),
            "expected session_created for the new conversation: {:?}",
            page.events
        );

        // Crash window: journal rows are committed with the canonical write;
        // notify then coalesces and stamps canonical_ready — Task 8: the
        // canonical pass is canonical progress only; index_ready is
        // reserved for a certified Quill publication.
        let high_water = notify_committed_change(reader, 0)?;
        let replay = changes_since(reader, 0, 64)?;
        assert!(
            replay
                .events
                .iter()
                .any(|e| e.kind == kind::CANONICAL_READY),
            "notify over real changes must stamp canonical_ready: {:?}",
            replay.events
        );
        assert!(
            replay.events.iter().all(|e| e.kind != kind::INDEX_READY),
            "a canonical notify must never emit index_ready: {:?}",
            replay.events
        );
        assert_eq!(replay.next_seq, high_water);

        // A quiet pass appends nothing.
        let quiet = notify_committed_change(reader, 0)?;
        assert_eq!(
            quiet, high_water,
            "quiet notify must not extend the journal"
        );
        let page = changes_since(reader, high_water, 64)?;
        assert!(page.events.is_empty());
        Ok(())
    }

    /// Task 8: `canonical_ready` certifies canonical progress only. A pass
    /// that observes ONLY annotation or marker rows above the boundary must
    /// coalesce them without minting a canonical claim — annotation-only
    /// activity and lexical certifications are not canonical commits.
    #[test]
    fn noncanonical_rows_do_not_mint_canonical_ready() -> Result<()> {
        let (_temp, _db, storage) = fixture();
        let reader = storage.raw();
        storage.insert_conversation_tree(agent(&storage), None, &canned(1, "base", 1))?;
        let baseline = notify_committed_change(reader, 0)?;

        // An annotation-only journal row above the boundary.
        append_change_event(reader, kind::ANNOTATIONS_UPDATED, Some(1), 9, None)?;
        let high_water = notify_committed_change(reader, 0)?;
        let page = changes_since(reader, baseline, 64)?;
        assert!(
            page.events
                .iter()
                .all(|e| e.kind != kind::CANONICAL_READY && e.kind != kind::INDEX_READY),
            "annotation-only activity must mint no readiness marker: {:?}",
            page.events
        );
        assert!(
            high_water > baseline,
            "the pass still advances the watermark"
        );

        // A certified publication above the boundary likewise mints no
        // canonical claim on the next notify.
        certify_lexical_publication(reader, 3, 10, None)?.expect("certify");
        notify_committed_change(reader, 0)?;
        let page = changes_since(reader, high_water, 64)?;
        assert!(
            page.events.iter().all(|e| e.kind != kind::CANONICAL_READY),
            "lexical certification must not fabricate canonical progress: {:?}",
            page.events
        );
        assert!(
            page.events.iter().any(|e| e.kind == kind::INDEX_READY),
            "the certification event itself replays: {:?}",
            page.events
        );
        Ok(())
    }

    #[test]
    fn rollback_and_same_values_leave_no_event() -> Result<()> {
        let (_temp, db, storage) = fixture();
        storage.insert_conversation_tree(agent(&storage), None, &canned(1, "base", 1))?;
        let cursor = changes_since(storage.raw(), 0, 64)?.next_seq;

        let writer = Connection::open(db.to_string_lossy().into_owned())?;
        writer.execute("BEGIN")?;
        writer.execute(
            "INSERT INTO messages (conversation_id, idx, role, content) \
             VALUES (1, 99, 'agent', 'vanished')",
        )?;
        writer.execute("ROLLBACK")?;
        drop(writer);
        let page = changes_since(storage.raw(), cursor, 64)?;
        assert!(
            page.events.is_empty() && page.next_seq == cursor,
            "rollback must be invisible to the journal: {page:?}"
        );

        let writer = Connection::open(db.to_string_lossy().into_owned())?;
        writer.execute("UPDATE conversations SET source_path = source_path WHERE id = 1")?;
        drop(writer);
        let page = changes_since(storage.raw(), cursor, 64)?;
        assert!(
            page.events.is_empty(),
            "same-values UPDATE must journal nothing: {:?}",
            page.events
        );
        Ok(())
    }

    #[test]
    fn retention_floor_requests_reset() -> Result<()> {
        let (_temp, _db, storage) = fixture();
        let agent = agent(&storage);
        for id in 1..=8 {
            storage.insert_conversation_tree(agent, None, &canned(id, "row", 1))?;
        }
        let first = changes_since(storage.raw(), 0, 64)?;
        notify_committed_change_with_retention(storage.raw(), 0, 4)?;
        let expired = changes_since(storage.raw(), first.next_seq.min(2), 64)?;
        assert!(
            expired.truncated
                && expired
                    .events
                    .iter()
                    .any(|e| e.kind == kind::RESET_REQUIRED),
            "expired cursor must truncate with reset_required: {expired:?}"
        );
        let regressed = changes_since(storage.raw(), expired.next_seq + 100_000, 64)?;
        assert!(
            regressed.truncated,
            "seq regression must reset: {regressed:?}"
        );
        Ok(())
    }

    #[test]
    fn explicit_append_commits_and_rolls_back_with_caller() -> Result<()> {
        let (_temp, _db, storage) = fixture();
        let connection = storage.raw();
        connection.execute("BEGIN")?;
        append_change_event(connection, kind::RESET_REQUIRED, None, 0, None)?;
        connection.execute("ROLLBACK")?;
        assert!(changes_since(connection, 0, 64)?.events.is_empty());

        connection.execute("BEGIN")?;
        let seq = append_change_event(connection, kind::RESET_REQUIRED, None, 7, None)?;
        connection.execute("COMMIT")?;
        let page = changes_since(connection, 0, 64)?;
        assert_eq!(page.events.len(), 1);
        assert_eq!(page.events[0].seq, Some(seq));
        assert_eq!(page.events[0].revision, 7);
        Ok(())
    }

    /// Task 8: `certify_lexical_publication` is the only writer of
    /// `index_ready`: one `BEGIN IMMEDIATE` transaction bumps
    /// `lexical_generation`, persists the versioned PublicationReceipt and
    /// journals the event bound to it — atomically, so a crash can never
    /// produce a journal claim the counters cannot explain. The receipt
    /// binds the caller's coverage floor (never the wider current
    /// revisions), and a second certification of the same generation and
    /// coverage is idempotent — no fabricated readiness.
    #[test]
    fn certify_lexical_publication_binds_receipt_to_journal() -> Result<()> {
        let (_temp, db, storage) = fixture();
        // Canonical progress first so covered revisions are nonzero.
        storage.insert_conversation_tree(agent(&storage), None, &canned(1, "alpha", 1))?;
        let current = current_journal_revisions(storage.raw())?.expect("revisions");
        let floor = JournalRevisions {
            canonical_revision: current.canonical_revision.saturating_sub(1),
            ..current
        };

        let receipt = certify_lexical_publication(storage.raw(), 7, 42, Some(floor))?
            .expect("certification must produce a receipt");
        assert_eq!(receipt.lexical_generation, 1);
        assert_eq!(receipt.quill_generation, 7);
        assert_eq!(receipt.live_doc_count, 42);
        assert_eq!(
            receipt.canonical_revision, floor.canonical_revision,
            "coverage binds the floor, never the wider current revision"
        );
        assert_eq!(receipt.v, PUBLICATION_RECEIPT_VERSION);

        // The durable singleton persisted the same receipt the journal
        // event carries.
        let persisted = latest_lexical_publication(storage.raw())?.expect("receipt row");
        assert_eq!(persisted, receipt);

        // The journal event replays on a FRESH connection (a restarted
        // bridge) with the receipt as its payload and lexical_generation
        // as its bound revision.
        let reader = Connection::open(db.to_string_lossy().into_owned())?;
        let page = changes_since(&reader, 0, 64)?;
        let event = page
            .events
            .iter()
            .find(|e| e.kind == kind::INDEX_READY)
            .expect("index_ready replays from the durable journal");
        assert_eq!(event.revision, 1, "event binds the certified ordinal");
        let payload: serde_json::Value =
            serde_json::from_str(event.payload_json.as_deref().expect("receipt payload"))?;
        assert_eq!(payload["quill_generation"], 7);
        assert_eq!(payload["canonical_revision"], floor.canonical_revision);
        assert_eq!(payload["v"], PUBLICATION_RECEIPT_VERSION);
        assert_eq!(
            page.current_revisions.map(|r| r.lexical_generation),
            Some(1),
            "viewer_revisions.lexical_generation advanced exactly once"
        );

        // Same generation + same coverage: idempotent — no second ordinal,
        // no second event.
        let again = certify_lexical_publication(storage.raw(), 7, 42, Some(floor))?
            .expect("repeat certification reads back");
        assert_eq!(again, receipt);
        assert_eq!(
            current_journal_revisions(storage.raw())?.map(|r| r.lexical_generation),
            Some(1),
            "idempotent certification must not mint a new lexical_generation"
        );

        // A NEW committed generation certifies a new ordinal.
        let next = certify_lexical_publication(storage.raw(), 8, 42, None)?
            .expect("second generation certification");
        assert_eq!(next.lexical_generation, 2);
        assert!(
            next.canonical_revision >= receipt.canonical_revision,
            "a floorless certification binds current coverage"
        );
        let page = changes_since(&reader, 0, 64)?;
        let ready_events: Vec<_> = page
            .events
            .iter()
            .filter(|e| e.kind == kind::INDEX_READY)
            .collect();
        assert_eq!(
            ready_events.len(),
            1,
            "read-side coalescing keeps only the newest index_ready: {:?}",
            page.events
        );
        Ok(())
    }

    /// Real COMMIT-failure injection: an `AFTER INSERT` trigger on
    /// `viewer_change_log` plants a `DEFERRABLE INITIALLY DEFERRED`
    /// foreign-key violation inside the transaction under test. The
    /// engine defers the check to COMMIT, where the surviving violation
    /// fails the COMMIT but — matching SQLite — leaves the transaction
    /// ACTIVE: exactly the ownership hole these tests guard.
    fn arm_commit_failure_on_journal_appends(connection: &Connection) {
        connection
            .execute_batch(
                "CREATE TABLE commit_poison (
                     id INTEGER PRIMARY KEY,
                     parent INTEGER REFERENCES conversations(id)
                         DEFERRABLE INITIALLY DEFERRED
                 );
                 CREATE TRIGGER poison_journal_append
                 AFTER INSERT ON viewer_change_log BEGIN
                     INSERT INTO commit_poison(parent) VALUES (999999);
                 END;",
            )
            .expect("commit-failure injection trigger");
    }

    /// A COMMIT that fails inside `notify_committed_change` must roll
    /// back the transaction the pass began: the initiating error stays
    /// the cause, `in_transaction()` proves the connection clean, the
    /// rolled-back pass leaves no partial state, and the same connection
    /// still runs an ordinary transaction.
    #[test]
    fn notify_commit_failure_rolls_back_owned_transaction() -> Result<()> {
        let (_temp, _db, storage) = fixture();
        let reader = storage.raw();
        reader.execute("PRAGMA foreign_keys = ON")?;
        storage.insert_conversation_tree(agent(&storage), None, &canned(1, "alpha", 1))?;
        arm_commit_failure_on_journal_appends(reader);

        let error = notify_committed_change(reader, 0)
            .expect_err("a notify COMMIT the deferred-FK recheck fails must surface");
        assert!(
            format!("{error:#}").contains("committing change-journal notify transaction"),
            "the initiating COMMIT failure must stay the cause: {error:#}"
        );
        assert!(
            !reader.as_async().in_transaction(),
            "the failed pass must not leave its own transaction open"
        );

        // The rolled-back pass left no partial state behind.
        let page = changes_since(reader, 0, 64)?;
        assert!(
            page.events.iter().all(|e| e.kind != kind::CANONICAL_READY),
            "a rolled-back notify must not leave its canonical_ready marker: {:?}",
            page.events
        );
        assert!(
            meta_int(reader, META_COMPACTED_THROUGH)?.is_none(),
            "a rolled-back notify must not persist the compaction watermark"
        );

        // The same connection still runs an ordinary transaction: the
        // injection stays armed, so the probe writes outside the journal
        // (a journal append would re-trigger the deferred violation).
        reader.execute("BEGIN")?;
        reader.execute("CREATE TABLE reuse_probe (id INTEGER)")?;
        reader.execute("COMMIT")?;
        assert_eq!(
            reader
                .query("SELECT 1 FROM sqlite_master WHERE name = 'reuse_probe'")?
                .len(),
            1,
            "the same connection must accept a later committed transaction"
        );
        Ok(())
    }

    /// The certification lane's COMMIT failure obeys the same ownership
    /// rule through the `FactsDb` surface: roll back the transaction the
    /// certification began, keep the initiating error as the cause,
    /// leave no receipt/generation/journal residue, and keep the
    /// connection usable.
    #[test]
    fn certify_commit_failure_rolls_back_owned_transaction() -> Result<()> {
        let (_temp, _db, storage) = fixture();
        let reader = storage.raw();
        reader.execute("PRAGMA foreign_keys = ON")?;
        storage.insert_conversation_tree(agent(&storage), None, &canned(1, "alpha", 1))?;
        arm_commit_failure_on_journal_appends(reader);

        let error = certify_lexical_publication(reader, 7, 42, None)
            .expect_err("a certification COMMIT the deferred-FK recheck fails must surface");
        assert!(
            format!("{error:#}").contains("committing lexical certification transaction"),
            "the initiating COMMIT failure must stay the cause: {error:#}"
        );
        assert!(
            !reader.as_async().in_transaction(),
            "the failed certification must not leave its own transaction open"
        );
        assert!(
            latest_lexical_publication(reader)?.is_none(),
            "a rolled-back certification must not persist a receipt"
        );
        assert_eq!(
            current_journal_revisions(reader)?.map(|r| r.lexical_generation),
            Some(0),
            "a rolled-back certification must not mint lexical_generation"
        );
        assert!(
            changes_since(reader, 0, 64)?
                .events
                .iter()
                .all(|e| e.kind != kind::INDEX_READY),
            "a rolled-back certification must not journal index_ready"
        );

        // The same connection still certifies once the injection is gone.
        reader.execute_batch("DROP TRIGGER poison_journal_append; DROP TABLE commit_poison;")?;
        let receipt = certify_lexical_publication(reader, 8, 42, None)?
            .expect("certification succeeds on the same connection after a handled failure");
        assert_eq!(receipt.lexical_generation, 1);
        Ok(())
    }

    /// A notify invoked while the CALLER already holds a transaction
    /// fails at BEGIN IMMEDIATE and must not roll back the caller's
    /// transaction — the ownership rule only ever rolls back what the
    /// function itself began.
    #[test]
    fn notify_preserves_caller_owned_transaction() -> Result<()> {
        let (_temp, _db, storage) = fixture();
        let reader = storage.raw();
        reader.execute("BEGIN")?;
        let error = notify_committed_change(reader, 0)
            .expect_err("notify inside a caller transaction must fail at BEGIN");
        assert!(
            format!("{error:#}").contains("opening change-journal notify transaction"),
            "the failure must be the nested BEGIN, not work inside it: {error:#}"
        );
        assert!(
            reader.as_async().in_transaction(),
            "a failed BEGIN must not roll back the caller's transaction"
        );
        reader.execute("INSERT INTO viewer_meta (key, value) VALUES ('caller_tx_probe', '1')")?;
        reader.execute("COMMIT")?;
        let probe: Option<String> = reader
            .query_row("SELECT value FROM viewer_meta WHERE key = 'caller_tx_probe'")?
            .get_typed(0)?;
        assert_eq!(
            probe.as_deref(),
            Some("1"),
            "the caller's transaction must still commit"
        );
        Ok(())
    }

    /// The same rule for the certification lane: a caller-held
    /// transaction fails BEGIN IMMEDIATE and is left untouched.
    #[test]
    fn certify_preserves_caller_owned_transaction() -> Result<()> {
        let (_temp, _db, storage) = fixture();
        let reader = storage.raw();
        storage.insert_conversation_tree(agent(&storage), None, &canned(1, "alpha", 1))?;
        reader.execute("BEGIN")?;
        let error = certify_lexical_publication(reader, 7, 42, None)
            .expect_err("certification inside a caller transaction must fail at BEGIN");
        assert!(
            format!("{error:#}").contains("opening lexical certification transaction"),
            "the failure must be the nested BEGIN, not work inside it: {error:#}"
        );
        assert!(
            reader.as_async().in_transaction(),
            "a failed BEGIN must not roll back the caller's transaction"
        );
        reader.execute("COMMIT")?;
        assert!(
            latest_lexical_publication(reader)?.is_none(),
            "the refused certification must leave no receipt"
        );
        Ok(())
    }
}
