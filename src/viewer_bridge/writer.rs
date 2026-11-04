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
use coding_agent_search::franken_sync::{Connection, params};
use serde_json::{Value, json};

use super::annotations;
use super::protocol::{AnnotationPatch, Generation, refuse};
use super::revisions;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|span| span.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
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
        match write(&self.connection, now) {
            Ok(value) => {
                self.connection.execute("COMMIT").map_err(|e| {
                    refuse(
                        "canonical_write_failed",
                        format!("Extension write commit failed: {e}"),
                    )
                })?;
                Ok(value)
            }
            Err(error) => {
                // Best-effort release; the write error is the one to report.
                let _ = self.connection.execute("ROLLBACK");
                Err(error)
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
}
