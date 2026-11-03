//! Bounded replay journal for the CASS-owned Viewer change stream
//! (spec decision 7; plan Task 11).
//!
//! `viewer_change_log` is the truth. Canonical observer AFTER triggers
//! (`src/viewer_bridge/revisions.rs`, installed by `ensure_observers`)
//! append `session_created`/`session_updated`/`session_deleted` rows inside
//! the SAME commit that mutates canonical rows — including writers in other
//! processes, since triggers are schema objects. Extension-table triggers
//! on `viewer_annotations`/`viewer_tags` append `annotations_updated` inside
//! the annotation write transaction. `index_ready` and administrative
//! `reset_required` are appended explicitly by post-commit callers through
//! [`append_change_event`].
//!
//! [`notify_committed_change`] is the post-commit maintenance pass — it is
//! callable ONLY after the writer's commit succeeded, never inside the
//! transaction it announces. It coalesces the committed burst to the latest
//! event per `(kind, conversation_id)`, stamps `index_ready`, and enforces
//! bounded retention. A writer that crashes after commit but before notify
//! still left a fully replayable (uncompacted) journal — never silently
//! stale.
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

use crate::franken_sync::compat::RowExt;
use crate::franken_sync::{Connection, SqliteValue};

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
    /// revision the pass certifies.
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
    /// `canonical_revision` for `session_*`, the per-row annotation CAS
    /// revision for `annotations_updated`, the certified canonical revision
    /// for `index_ready`, `0` for administrative `reset_required`.
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
/// 2. Stamps `index_ready` bound to `committed_revision` — but only when
///    the pass actually observed new events, so an idempotent re-ingest
///    emits no semantic event.
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
            // Mark the pass: index_ready binds the canonical revision the
            // commit certified (re-read when the caller passes 0).
            let revision = if committed_revision > 0 {
                committed_revision
            } else {
                lane_revisions(connection)?
                    .map(|g| g.canonical_revision)
                    .unwrap_or(0)
            };
            append_change_event(
                connection,
                kind::INDEX_READY,
                None,
                revision,
                Some(format!("{{\"canonical_revision\":{revision}}}")),
            )?;
            // Only the newest index_ready marker is meaningful.
            connection.execute(
                "DELETE FROM viewer_change_log WHERE kind = 'index_ready' AND seq < (\
                     SELECT MAX(seq) FROM viewer_change_log WHERE kind = 'index_ready'\
                 )",
            )?;
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
    match result {
        Ok(high_water) => {
            connection
                .execute("COMMIT")
                .context("committing change-journal notify transaction")?;
            Ok(high_water)
        }
        Err(error) => {
            let _ = connection.execute("ROLLBACK");
            Err(error)
        }
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
        let addr: SocketAddr = value.get("addr")?.as_str()?.parse().ok()?;
        let mut stream = TcpStream::connect_timeout(&addr, Duration::from_millis(150)).ok()?;
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
        // notify then coalesces and stamps index_ready.
        let high_water = notify_committed_change(reader, 0)?;
        let replay = changes_since(reader, 0, 64)?;
        assert!(
            replay.events.iter().any(|e| e.kind == kind::INDEX_READY),
            "notify over real changes must stamp index_ready: {:?}",
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
}
