//! The sole writable lane of the bridge: one owned writable FrankenSQLite
//! connection, opened only under the explicit local `--enable-viewer-writes`
//! authorization, serializing every extension mutation behind a single
//! boundary type. `StrictArchive` remains a second, strictly read-only
//! connection; the two never share state and neither crosses a thread —
//! the session's single-threaded JSONL loop is the serializer.
//!
//! The writable surface is the narrow `cass_owned_queries_v1` op set
//! implemented here — never arbitrary client SQL. Every mutation is one
//! `BEGIN IMMEDIATE` transaction whose logical-revision bump commits
//! atomically with the payload change, so a revision is never observed
//! ahead of the write it announces (and vice versa).

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use coding_agent_search::franken_sync::compat::{
    OpenFlags, RowExt, open_with_flags, param_slice_to_values,
};
use coding_agent_search::franken_sync::{Connection, FrankenError, params};
use serde_json::{Value, json};

use super::annotations;
use super::protocol::{AnnotationPatch, Generation, Refusal, refuse};
use super::revisions;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|span| span.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

/// Roll back only while the writer's own transaction is still open: a
/// `COMMIT` that already released it — or a write that never reached
/// one — makes this a no-op, and a pre-existing transaction can never
/// reach it because `BEGIN IMMEDIATE` fails first.
fn rollback_owned_transaction(connection: &Connection) -> std::result::Result<(), FrankenError> {
    if !connection.as_async().in_transaction() {
        return Ok(());
    }
    connection.execute("ROLLBACK").map(|_| ())
}

/// Attach a failed rollback to the initiating error without masking it:
/// a `Refusal` keeps its protocol `kind` — `protocol::failure` reads
/// only the top error for `error.kind` — with the cleanup failure
/// folded into the message; any other error carries it as anyhow
/// context over the preserved cause.
fn with_cleanup_failure(error: anyhow::Error, rollback_error: &FrankenError) -> anyhow::Error {
    let note = format!(
        "rolling back the failed extension write also failed ({rollback_error}); \
         the writer connection may still hold the transaction open"
    );
    match error.downcast_ref::<Refusal>() {
        Some(refusal) => refuse(refusal.kind, format!("{}; {note}", refusal.message)),
        None => error.context(note),
    }
}

/// Owned writable extension connection. All `viewer_*` writes flow through
/// this type; nothing else in the process holds a writable handle.
pub struct OwnedWriter {
    connection: Connection,
}

impl OwnedWriter {
    /// Open the writable lane against an existing extended archive.
    /// `SQLITE_OPEN_READ_WRITE` without `CREATE` never fabricates a missing
    /// database. The extension profile must already be installed; canonical
    /// observer triggers are (re)ensured idempotently here.
    pub fn open(path: &Path) -> Result<Self> {
        let text = path.to_str().context("Archive path is not UTF-8")?;
        let connection = open_with_flags(text, OpenFlags::SQLITE_OPEN_READ_WRITE).map_err(|e| {
            refuse(
                "canonical_db_missing",
                format!("Cannot open the extension writer lane: {e}"),
            )
        })?;
        connection.execute("PRAGMA busy_timeout = 10000")?;
        if !revisions::extensions_installed(&connection)? {
            return Err(refuse(
                "viewer_extensions_missing",
                "The viewer extension profile is not installed; run --init-extensions first",
            ));
        }
        // Task 8: an archive whose extension profile predates schema v8
        // (no `viewer_lexical_publication`) would claim
        // `lexical_tracking: bound` while certification silently no-ops —
        // self-upgrade here. ensure_schema is additive idempotent DDL, so
        // a current archive pays one schema-version read.
        coding_agent_search::viewer_extensions::schema::ensure_schema(&connection).map_err(
            |e| {
                refuse(
                    "viewer_extensions_missing",
                    format!("Cannot ensure viewer extension schema: {e:#}"),
                )
            },
        )?;
        revisions::ensure_observers(&connection).map_err(|e| {
            refuse(
                "viewer_extensions_missing",
                format!("Cannot ensure canonical observers: {e:#}"),
            )
        })?;
        Ok(Self { connection })
    }

    /// Committed logical generation as seen by the writable lane.
    pub fn generation(&self) -> Result<Generation> {
        revisions::generation(&self.connection)?.ok_or_else(|| {
            refuse(
                "viewer_extensions_missing",
                "The viewer extension profile is not installed",
            )
        })
    }

    /// Serialized writer boundary: one immediate transaction per mutation;
    /// the logical-revision bump and the payload share the commit.
    fn transact<T>(&self, write: impl FnOnce(&Connection, i64) -> Result<T>) -> Result<T> {
        self.connection.execute("BEGIN IMMEDIATE").map_err(|e| {
            refuse(
                "writer_unavailable",
                format!("Extension writer lane is contended: {e}"),
            )
        })?;
        let now = now_ms();
        let outcome = match write(&self.connection, now) {
            Ok(value) => match self.connection.execute("COMMIT") {
                Ok(_) => return Ok(value),
                // A failed COMMIT can leave the transaction ACTIVE
                // (deferred-constraint recheck, concurrent-mode
                // conflict); the owned transaction is rolled back below
                // so one failed write cannot poison this persistent
                // lane for every later mutation.
                Err(commit_error) => Err(refuse(
                    "canonical_write_failed",
                    format!("Extension write commit failed: {commit_error}"),
                )),
            },
            Err(error) => Err(error),
        };
        match rollback_owned_transaction(&self.connection) {
            // The initiating error is the one to report; the rollback is
            // cleanup only.
            Ok(()) => outcome,
            Err(rollback_error) => {
                outcome.map_err(|error| with_cleanup_failure(error, &rollback_error))
            }
        }
    }

    /// Compare-and-swap one `viewer_annotations` row: the per-row
    /// `revision` is the CAS token, `expected_revision = 0` addresses an
    /// absent row. The row bump and the singleton `annotation_revision`
    /// bump commit atomically; canonical and lexical revisions are never
    /// touched by this lane.
    pub fn patch_annotations(
        &mut self,
        conversation_id: i64,
        expected_revision: i64,
        patch: &AnnotationPatch,
    ) -> Result<Value> {
        patch.validate()?;
        if conversation_id <= 0 || expected_revision < 0 {
            return Err(refuse(
                "invalid_request",
                "conversation_id must be positive and expected_revision nonnegative",
            ));
        }
        let annotation = self.transact(|connection, now| {
            let exists = connection.query_with_params(
                "SELECT 1 FROM conversations WHERE id = ?1 LIMIT 1",
                &param_slice_to_values(params![conversation_id]),
            )?;
            if exists.is_empty() {
                return Err(refuse(
                    "unknown_conversation",
                    "Annotation patch targets a conversation that does not exist",
                ));
            }
            let current = connection.query_with_params(
                "SELECT is_pinned,custom_title,notes,revision FROM viewer_annotations \
                     WHERE conversation_id = ?1 LIMIT 2",
                &param_slice_to_values(params![conversation_id]),
            )?;
            if current.len() > 1 {
                return Err(refuse(
                    "canonical_coordinates_invalid",
                    "Ambiguous annotation row",
                ));
            }
            let current = current.first();
            let current_revision: i64 = current
                .map(|row| row.get_typed(3))
                .transpose()?
                .unwrap_or(0);
            if current_revision != expected_revision {
                return Err(refuse(
                    "revision_conflict",
                    format!(
                        "Annotation revision {expected_revision} is stale; \
                         committed revision is {current_revision}"
                    ),
                ));
            }
            let read = |index: usize| -> Result<Option<String>> {
                Ok(current
                    .map(|row| row.get_typed::<Option<String>>(index))
                    .transpose()?
                    .flatten())
            };
            let is_pinned: i64 = match patch.is_pinned {
                Some(pinned) => i64::from(pinned),
                None => current
                    .map(|row| row.get_typed::<i64>(0))
                    .transpose()?
                    .unwrap_or(0),
            };
            let custom_title = match &patch.custom_title {
                Some(value) => value.clone(),
                None => read(1)?,
            };
            let notes = match &patch.notes {
                Some(value) => value.clone(),
                None => read(2)?,
            };
            // Tag set replacement (viewer `MetadataPatch` semantics: a
            // present list replaces the whole set inside the same CAS
            // transaction; the single `annotation_revision` bump below
            // covers annotations + tags atomically). The receipt reads the
            // committed set back in canonical order so `patch_annotations`,
            // `get_annotations` and `list_sessions` always agree.
            if let Some(raw) = &patch.tags {
                let clean = annotations::sanitize_tags(raw);
                connection.execute_with_params(
                    "DELETE FROM viewer_tags WHERE conversation_id = ?1",
                    &param_slice_to_values(params![conversation_id]),
                )?;
                for tag in &clean {
                    connection.execute_with_params(
                        "INSERT OR IGNORE INTO viewer_tags \
                         (conversation_id,tag,created_at_ms) VALUES (?1,?2,?3)",
                        &param_slice_to_values(params![conversation_id, tag.clone(), now]),
                    )?;
                }
            }
            let tags = annotations::tags_for(connection, &[conversation_id])?
                .remove(&conversation_id)
                .unwrap_or_default();
            let revision = current_revision + 1;
            connection.execute_with_params(
                "INSERT OR REPLACE INTO viewer_annotations \
                 (conversation_id,is_pinned,custom_title,notes,revision,updated_at_ms) \
                 VALUES (?1,?2,?3,?4,?5,?6)",
                &param_slice_to_values(params![
                    conversation_id,
                    is_pinned,
                    custom_title.clone(),
                    notes.clone(),
                    revision,
                    now
                ]),
            )?;
            // Task 5 targeted meta replacement: title/notes/tag region
            // rows re-derive inside the SAME transaction as the annotation
            // upsert — cleared values delete their rows here (a stale
            // Quill meta document can only produce an unverifiable hit,
            // never membership) — and `docs_state` flips to
            // `meta_pending` until the index lane replaces the docs.
            coding_agent_search::viewer_extensions::search_regions::replace_meta_regions(
                connection,
                conversation_id,
            )?;
            revisions::bump(connection, revisions::Counter::Annotation, now)?;
            Ok(json!({
                "conversation_id": conversation_id,
                "is_pinned": is_pinned != 0,
                "custom_title": custom_title,
                "notes": notes,
                "revision": revision,
                "updated_at_ms": now,
                "tags": tags,
            }))
        })?;
        // Read back AFTER commit: the receipt's generation is the committed
        // logical state, never an in-transaction estimate.
        Ok(json!({
            "annotation": annotation,
            "generation": self.generation()?,
        }))
    }

    /// Task 5 post-edit convergence (caller wraps this best-effort): fold
    /// every `docs_state != 'published'` marks row — including the meta
    /// replacement the annotation commit just staged — into the live Quill
    /// index and commit. When the published index cannot be written here
    /// (absent, contended by an ingest writer), the marks row stays
    /// `meta_pending` and search over the session degrades to honest
    /// `unknown` rather than answering from stale meta documents.
    ///
    /// A missing published index is skipped outright: `open_or_create`
    /// would fabricate a real index holding ONLY region documents —
    /// `searchable_index_fingerprint` would then report a published
    /// generation, `ReaderSet::open` would stop refusing
    /// `index_unavailable`, and chat literals would evaluate against an
    /// empty hit set as false negatives. `meta_pending` survives for the
    /// next ingest-time converge instead.
    pub fn converge_search_region_docs(&mut self, index_path: &Path) -> Result<()> {
        if coding_agent_search::search::tantivy::searchable_index_fingerprint(index_path)
            .context("reading the published index fingerprint for region-doc convergence")?
            .is_none()
        {
            return Ok(());
        }
        let mut index =
            coding_agent_search::search::tantivy::TantivyIndex::open_or_create(index_path)
                .context("opening the published lexical index for region-doc convergence")?;
        index.converge_pending_search_region_docs(&self.connection, i64::MAX)?;
        index
            .commit_with_region_doc_stamps(&self.connection)
            .context("committing converged search-region documents")
    }

    /// `true` while any `viewer_search_region_marks` row still awaits doc
    /// (re)publication — the bounded probe the deferred-converge drain
    /// loop and the pre-search flush share. One `LIMIT 1` read; the marks
    /// set itself remains the durable record of pending work.
    pub fn region_docs_pending(&self) -> Result<bool> {
        let rows = self
            .connection
            .query(
                "SELECT 1 FROM viewer_search_region_marks \
                 WHERE docs_state != 'published' LIMIT 1",
            )
            .context("probing pending search-region doc marks")?;
        Ok(!rows.is_empty())
    }
}

#[cfg(test)]
mod tests {
    //! Transaction-ownership regression lane for [`OwnedWriter::transact`]:
    //! a real COMMIT failure injected through the engine's deferred-FK
    //! recheck — the surviving violation fails the COMMIT but, matching
    //! SQLite, leaves the transaction ACTIVE — plus pre-existing
    //! transaction preservation. The wider protocol surface is exercised
    //! by `tests/viewer_bridge_extensions.rs`.

    use super::*;

    use crate::viewer_bridge::protocol::Refusal;
    use coding_agent_search::storage::sqlite::FrankenStorage;

    /// A writable lane over a real archive: the canonical schema comes
    /// from the storage open, the extension profile and observers are
    /// installed exactly as `OwnedWriter::open` does, then the writer is
    /// bound to its own read/write connection — the persistent lane a
    /// poisoned transaction would strand.
    fn fixture() -> Result<(tempfile::TempDir, OwnedWriter)> {
        let temp = tempfile::tempdir().context("tempdir")?;
        let db = temp.path().join("archive.db");
        let canonical = FrankenStorage::open(&db).context("canonical schema")?;
        drop(canonical);
        let path = db.to_str().context("archive path is not UTF-8")?;
        let connection = open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)
            .context("writer connection")?;
        connection.execute("PRAGMA busy_timeout = 10000")?;
        connection.execute("PRAGMA foreign_keys = ON")?;
        coding_agent_search::viewer_extensions::schema::ensure_schema(&connection)?;
        revisions::ensure_observers(&connection)?;
        connection.execute_batch(
            "INSERT OR IGNORE INTO agents (id, slug, name, kind, created_at, updated_at) \
             VALUES (1, 'fixture-agent', 'Fixture Agent', 'fixture', 0, 0);
             INSERT OR IGNORE INTO sources (id, kind, created_at, updated_at) \
             VALUES ('local', 'local', 0, 0);
             INSERT INTO conversations \
             (id, agent_id, source_id, external_id, source_path) \
             VALUES (1, 1, 'local', 'fixture-1', '/fixture/session-1.jsonl');",
        )?;
        Ok((temp, OwnedWriter { connection }))
    }

    fn patch() -> AnnotationPatch {
        AnnotationPatch {
            is_pinned: Some(true),
            ..AnnotationPatch::default()
        }
    }

    /// Real COMMIT-failure injection: an `AFTER INSERT` trigger on
    /// `viewer_annotations` plants a `DEFERRABLE INITIALLY DEFERRED`
    /// foreign-key violation inside the writer's own transaction. The
    /// engine defers the check to COMMIT, where the surviving violation
    /// fails the COMMIT but — matching SQLite — leaves the transaction
    /// ACTIVE: exactly the ownership hole this test guards.
    fn arm_commit_failure(writer: &OwnedWriter) -> Result<()> {
        writer.connection.execute_batch(
            "CREATE TABLE commit_poison (
                 id INTEGER PRIMARY KEY,
                 parent INTEGER REFERENCES conversations(id)
                     DEFERRABLE INITIALLY DEFERRED
             );
             CREATE TRIGGER poison_annotation_upsert
             AFTER INSERT ON viewer_annotations BEGIN
                 INSERT INTO commit_poison(parent) VALUES (999999);
             END;",
        )?;
        Ok(())
    }

    /// A COMMIT that fails inside `transact` must roll back the
    /// transaction it began: the `canonical_write_failed` refusal stays
    /// the top error (the JSONL `error.kind` contract reads only the top
    /// error), `in_transaction()` proves the connection clean, no
    /// annotation/journal/revision row survives, and the SAME persistent
    /// writer keeps serving mutations once the injection is gone — no
    /// reconnect.
    #[test]
    fn transact_commit_failure_rolls_back_owned_transaction() -> Result<()> {
        let (_temp, mut writer) = fixture()?;
        arm_commit_failure(&writer)?;
        // The fixture's own canonical seed already journals one row; the
        // assertion is that the failed mutation adds none.
        let journal_before: i64 = writer
            .connection
            .query_row("SELECT COUNT(*) FROM viewer_change_log")?
            .get_typed(0)?;

        let error = writer
            .patch_annotations(1, 0, &patch())
            .expect_err("a COMMIT the deferred-FK recheck fails must surface");
        let refusal = error
            .downcast_ref::<Refusal>()
            .expect("the top error must stay the canonical_write_failed refusal");
        assert_eq!(refusal.kind, "canonical_write_failed");
        assert!(
            !writer.connection.as_async().in_transaction(),
            "the failed mutation must not leave its own transaction open"
        );
        let annotations: i64 = writer
            .connection
            .query_row("SELECT COUNT(*) FROM viewer_annotations WHERE conversation_id = 1")?
            .get_typed(0)?;
        assert_eq!(
            annotations, 0,
            "the failed upsert must not partially commit"
        );
        let journal: i64 = writer
            .connection
            .query_row("SELECT COUNT(*) FROM viewer_change_log")?
            .get_typed(0)?;
        assert_eq!(
            journal, journal_before,
            "the observer journal row must roll back with the payload"
        );
        assert_eq!(
            writer.generation()?.annotation_revision,
            0,
            "the revision bump must roll back with the payload"
        );

        // Same persistent writer, injection removed: the lane is not
        // poisoned and needs no reconnect.
        writer
            .connection
            .execute_batch("DROP TRIGGER poison_annotation_upsert; DROP TABLE commit_poison;")?;
        let receipt = writer.patch_annotations(1, 0, &patch())?;
        assert_eq!(receipt["annotation"]["revision"], json!(1));
        assert_eq!(
            writer.generation()?.annotation_revision,
            1,
            "the same writer must still commit the retried mutation"
        );
        Ok(())
    }

    /// A mutation attempted while the connection ALREADY sits inside a
    /// transaction the writer did not begin must refuse at `BEGIN
    /// IMMEDIATE` and leave that transaction alone — the ownership rule
    /// only ever rolls back what `transact` itself began.
    #[test]
    fn transact_preserves_preexisting_transaction() -> Result<()> {
        let (_temp, mut writer) = fixture()?;
        writer.connection.execute("BEGIN")?;
        let error = writer
            .patch_annotations(1, 0, &patch())
            .expect_err("a mutation over a pre-existing transaction must refuse");
        let refusal = error
            .downcast_ref::<Refusal>()
            .expect("the top error must stay the writer_unavailable refusal");
        assert_eq!(refusal.kind, "writer_unavailable");
        assert!(
            writer.connection.as_async().in_transaction(),
            "a refused mutation must not roll back the pre-existing transaction"
        );
        writer.connection.execute("COMMIT")?;
        writer.patch_annotations(1, 0, &patch())?;
        Ok(())
    }
}
