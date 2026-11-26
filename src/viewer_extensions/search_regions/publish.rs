use std::collections::HashMap;

use anyhow::{Context, Result};

use crate::franken_sync::compat::{ParamValue, RowExt};
use crate::model::conversation_packet::ConversationPacket;
use crate::viewer_extensions::facts::FactsDb;
use crate::viewer_extensions::projection::StructuralPublication;

use super::derive_message::derive_message_regions;
use super::derive_meta::derive_meta_regions;
use super::docs::{SessionScopeDoc, session_scope_docs};
use super::queries::region_mark;
use super::{
    DOCS_META_PENDING, DOCS_PENDING, REGION_DOC_BASE, REGION_VERSION, RegionRow, SESSION_DOC_BASE,
    SESSION_DOC_MAX_CHUNKS_PER_SCOPE, SESSION_DOC_SCOPE_SHIFT, session_scope_ordinal,
};

// ---------------------------------------------------------------------------
// Publication
// ---------------------------------------------------------------------------

fn insert_region(db: &impl FactsDb, conversation_id: i64, region: &RegionRow) -> Result<()> {
    db.facts_execute(
        "INSERT INTO viewer_search_regions \
         (conversation_id, region_ordinal, region_key, scope, scope_known, quill_idx, \
          message_idx, message_id, step_index, turn_index, tool_name, file_path, \
          exit_code, has_error, error_type, text_source, indexed_text, source_revision) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18)",
        &[
            ParamValue::from(conversation_id),
            ParamValue::from(region.region_ordinal),
            ParamValue::from(region.region_key.as_str()),
            ParamValue::from(region.scope.as_str()),
            ParamValue::from(region.scope_known as i64),
            ParamValue::from(region.quill_idx),
            ParamValue::from(region.message_idx),
            ParamValue::from(region.message_id),
            ParamValue::from(region.step_index),
            ParamValue::from(region.turn_index),
            ParamValue::from(region.tool_name.as_deref()),
            ParamValue::from(region.file_path.as_deref()),
            ParamValue::from(region.exit_code),
            ParamValue::from(region.has_error.map(|v| v as i64)),
            ParamValue::from(region.error_type.as_deref()),
            ParamValue::from(region.text_source.as_str()),
            ParamValue::from(region.indexed_text.as_deref()),
            ParamValue::from(region.source_revision),
        ],
    )
    .map(|_| ())
    .context("inserting persisted search region")
}

/// The `(source_id, source_path)` that [`super::docs::region_docs`] mints a session's
/// region documents under: the canonical conversation row, `source_id`
/// defaulting to `local`. `None` when the row is missing or has no path —
/// such a session owns no region documents.
fn region_doc_identity_parts(
    db: &impl FactsDb,
    conversation_id: i64,
) -> Result<Option<(String, String)>> {
    let row = db
        .facts_query_map(
            "SELECT source_id, source_path FROM conversations WHERE id = ?1 LIMIT 1",
            &[ParamValue::from(conversation_id)],
            |row| {
                Ok((
                    row.get_typed::<Option<String>>(0)?,
                    row.get_typed::<Option<String>>(1)?,
                ))
            },
        )?
        .into_iter()
        .next();
    Ok(row.and_then(|(source_id, source_path)| {
        source_path
            .filter(|path| !path.is_empty())
            .map(|path| (source_id.unwrap_or_else(|| "local".to_owned()), path))
    }))
}

fn stale_doc_ids_json(
    db: &impl FactsDb,
    conversation_id: i64,
    keep: &[String],
    session_keep: Option<&std::collections::HashSet<String>>,
) -> Result<String> {
    // Existing pending deletes ride forward minus ids the new set reclaims.
    let current: Vec<Option<String>> = db.facts_query_map(
        "SELECT pending_doc_deletes FROM viewer_search_region_marks \
         WHERE conversation_id = ?1",
        &[ParamValue::from(conversation_id)],
        |row| row.get_typed::<Option<String>>(0),
    )?;
    let mut pending: Vec<String> = current
        .first()
        .and_then(|raw| raw.as_deref().map(str::to_owned))
        .and_then(|raw| serde_json::from_str::<Vec<String>>(&raw).ok())
        .unwrap_or_default();
    // GH #457: the riding queue predates this publication, so its entries
    // were computed against a session×scope keep that may have MOVED — a
    // chunk stale when an earlier epoch queued it can be LIVE under the
    // current derivation (the conversation grew). Filter against BOTH
    // keeps: the region identities above, and the session×scope
    // identities the CURRENT derivation keeps — a live keep identity that
    // rides forward is deleted by the stamped commit AFTER the run counted
    // it, so a fresh reader sees fewer documents than the accounting
    // staged (the full-rebuild doc-count refusal). `None` (derivation
    // bail) keeps the old conservative ride-forward: the staging lane
    // bails on the same derivation, so no live session document can exist
    // for this conversation in a fresh candidate.
    pending.retain(|id| !keep.contains(id) && session_keep.is_none_or(|live| !live.contains(id)));
    Ok(serde_json::to_string(&pending).unwrap_or_else(|_| "[]".to_owned()))
}

/// Publish the session's persisted region inventory inside the coherent
/// ingest generation: same transaction, same `source_revision`, idempotent
/// on `(region_version, source_revision)`.
pub fn publish_search_regions(
    db: &impl FactsDb,
    conversation_id: i64,
    packet: &ConversationPacket,
    structure: &StructuralPublication,
) -> Result<()> {
    let source_revision = structure.source_revision;
    let mut regions = derive_message_regions(conversation_id, packet, structure);
    // `message_id` arrives None whenever the packet replays a pre-commit
    // `Conversation` (ingest's just-inserted rows carry no ids in the
    // struct). Resolve the canonical `(idx → id)` binding in the SAME
    // transaction the packet was committed in — a message-bound region
    // without `message_id` could never hydrate its text.
    if regions
        .iter()
        .any(|r| r.message_idx.is_some() && r.message_id.is_none())
    {
        let ids: HashMap<i64, i64> = db
            .facts_query_map(
                "SELECT idx, id FROM messages WHERE conversation_id = ?1",
                &[ParamValue::from(conversation_id)],
                |row| Ok((row.get_typed::<i64>(0)?, row.get_typed::<i64>(1)?)),
            )?
            .into_iter()
            .collect();
        for region in &mut regions {
            if region.message_id.is_none()
                && let Some(idx) = region.message_idx
            {
                region.message_id = ids.get(&idx).copied();
            }
        }
    }
    let first_meta = regions.len() as i64;
    regions.extend(derive_meta_regions(
        db,
        conversation_id,
        source_revision,
        first_meta,
    )?);

    // Doc-id diff for the index lane: every quill_idx slot the PREVIOUS row
    // set owned that the new set no longer does becomes a pending delete —
    // the Quill publish lane removes exactly those identities, so a shrunk
    // or renumbered inventory never leaves a live stale region document.
    // The `quill_idx >= REGION_DOC_BASE` bound is applied in Rust, not in
    // the WHERE clause: the bare `conversation_id` equality seeks the
    // index directly and returns only this session's own rows without
    // scanning a covering index on an equality + range pair. The filtered
    // set is identical, and the caller diffs it as a set.
    let previous_slots: Vec<i64> = db
        .facts_query_map(
            "SELECT quill_idx FROM viewer_search_regions WHERE conversation_id = ?1",
            &[ParamValue::from(conversation_id)],
            |row| row.get_typed::<i64>(0),
        )?
        .into_iter()
        .filter(|slot| *slot >= REGION_DOC_BASE as i64)
        .collect();
    // Stage 1: the previous generation's session×scope chunk count per
    // scope is bounded by its region count (region-boundary packing makes
    // at most one chunk per region). The OLD chat chunk count itself is
    // NOT derivable here — a re-ingest already replaced the message rows
    // this transaction reads — so the stale queue conservatively covers
    // every slot under that bound; the post-commit delete ignores
    // identities that never existed, and the keep filter below protects
    // every chunk the new generation owns. Read BEFORE the row DELETE.
    let old_scope_counts = old_region_scope_counts(db, conversation_id)?;
    // Region documents are minted from the canonical conversation row
    // (`region_docs`), so their queued identities must be too. A packet can
    // name the session by another path than the row it merged into; ids
    // built from it never match the live documents, so a reclaimed slot
    // survives the keep filter below and the post-commit delete removes a
    // live document.
    let (doc_source_id, doc_source_path) = region_doc_identity_parts(db, conversation_id)?
        .unwrap_or_else(|| {
            (
                packet.payload.provenance.source_id.clone(),
                packet.payload.identity.source_path.clone(),
            )
        });
    let doc_identity = |slot: i64| {
        frankensearch::quill::cass::cass_document_identity(
            &doc_source_id,
            frankensearch::quill::cass::CassConversationKey {
                source_path: &doc_source_path,
                id: Some(conversation_id),
            },
            slot as u64,
        )
    };
    let keep_ids: Vec<String> = regions
        .iter()
        .filter(|r| r.quill_idx >= REGION_DOC_BASE as i64)
        .map(|r| doc_identity(r.quill_idx))
        .collect();

    db.facts_execute(
        "DELETE FROM viewer_search_regions WHERE conversation_id = ?1",
        &[ParamValue::from(conversation_id)],
    )?;
    for region in &regions {
        insert_region(db, conversation_id, region)?;
    }

    // GH #457: the session×scope keep set derives from the JUST-INSERTED
    // rows through the same function the staging lane materializes
    // documents with, and the previous marks queue rides forward AFTER
    // the row replacement so the filter sees the CURRENT derivation: a
    // queued identity that was stale when an earlier epoch wrote the
    // queue can be LIVE now (the conversation grew), and the staged
    // commit would delete it after the run already counted it. `None`
    // (derivation bail) keeps the ride-forward conservative.
    let session_chunks = session_scope_docs(db, conversation_id)?;
    let session_keep: Option<std::collections::HashSet<String>> =
        session_chunks.as_deref().map(|chunks| {
            chunks
                .iter()
                .map(|chunk| doc_identity(chunk.slot as i64))
                .collect()
        });
    let mut pending = stale_doc_ids_json(db, conversation_id, &keep_ids, session_keep.as_ref())?;
    {
        let new_slots: std::collections::HashSet<i64> =
            regions.iter().map(|r| r.quill_idx).collect();
        let mut dropped: Vec<String> = previous_slots
            .into_iter()
            .filter(|slot| !new_slots.contains(slot))
            .map(doc_identity)
            .collect();
        let mut list: Vec<String> = serde_json::from_str(&pending).unwrap_or_default();
        list.append(&mut dropped);
        pending = serde_json::to_string(&list).unwrap_or_else(|_| "[]".to_owned());
    }
    // Stage 1: queue the session×scope stale identities. The keep set is
    // derived from the just-inserted rows through the SAME function the
    // staging lane materializes documents with, so a live keep identity
    // can never be queued. Old slots are conservatively bounded by the
    // pre-delete per-scope region counts (see above); identities that
    // never existed are ignored by the post-commit delete.
    let session_stale =
        session_doc_stale_identities(&old_scope_counts, &doc_identity, session_chunks.as_deref());
    if !session_stale.is_empty() {
        let mut list: Vec<String> = serde_json::from_str(&pending).unwrap_or_default();
        list.extend(session_stale);
        pending = serde_json::to_string(&list).unwrap_or_else(|_| "[]".to_owned());
    }
    db.facts_execute(
        "INSERT INTO viewer_search_region_marks \
         (conversation_id, region_version, source_revision, region_count, \
          docs_state, pending_doc_deletes, published_at_ms) \
         VALUES (?1,?2,?3,?4,?5,?6,?7) \
         ON CONFLICT(conversation_id) DO UPDATE SET \
           region_version=excluded.region_version, \
           source_revision=excluded.source_revision, \
           region_count=excluded.region_count, \
           docs_state=excluded.docs_state, \
           pending_doc_deletes=excluded.pending_doc_deletes, \
           published_at_ms=excluded.published_at_ms",
        &[
            ParamValue::from(conversation_id),
            ParamValue::from(REGION_VERSION),
            ParamValue::from(source_revision),
            ParamValue::from(regions.len() as i64),
            ParamValue::from(DOCS_PENDING),
            ParamValue::from(pending.as_str()),
            ParamValue::from(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0),
            ),
        ],
    )?;
    Ok(())
}

/// The previous generation's per-scope region row counts for one
/// conversation — the conservative old-slot bound the session×scope stale
/// queue uses. Grouped in Rust from an equality-prefix seek rather than
/// SQL `GROUP BY scope` + temp B-tree, so the bare `conversation_id = ?`
/// equality seeks an index and returns only the session's own rows. Sorting
/// before the run-length
/// fold reproduces SQL `GROUP BY`'s sorted output order (BINARY text
/// collation = byte order, same as Rust's `String` ordering), so the
/// produced `(scope, count)` list is element- and order-identical to the
/// SQL aggregation's.
fn old_region_scope_counts(db: &impl FactsDb, conversation_id: i64) -> Result<Vec<(String, i64)>> {
    let mut scopes = db.facts_query_map(
        "SELECT scope FROM viewer_search_regions WHERE conversation_id = ?1",
        &[ParamValue::from(conversation_id)],
        |row| row.get_typed::<String>(0),
    )?;
    scopes.sort();
    let mut counts: Vec<(String, i64)> = Vec::new();
    for scope in scopes {
        match counts.last_mut() {
            Some((scope_at, count)) if *scope_at == scope => *count += 1,
            _ => counts.push((scope, 1)),
        }
    }
    Ok(counts)
}

/// `region_key LIKE 'meta:%'` with SQLite's ASCII-case-insensitive LIKE
/// semantics — a five-byte prefix match. The targeted meta replacement
/// uses it to split a session's meta rows from its message-bound rows.
fn is_meta_region_key(region_key: &str) -> bool {
    region_key
        .get(..5)
        .is_some_and(|prefix| str::eq_ignore_ascii_case(prefix, "meta:"))
}

/// The conservative session×scope stale-identity queue for one
/// conversation (Stage 1): every session-doc slot under the pre-delete
/// per-scope region-count bound that the CURRENT rows' derivation does not
/// keep. Region-boundary packing makes at most one chunk per region, so
/// the old per-scope region count bounds the previous generation's chunk
/// count; the old chat chunk count itself is not derivable at publication
/// time (a re-ingest already replaced the message rows), which is why the
/// queue is conservative — the post-commit delete ignores identities that
/// never existed, and the keep filter (derived through the same function
/// the staging lane materializes with) protects every live chunk. The
/// caller passes the chunks it ALREADY derived for the ride-forward keep
/// set, so the publication's two session-doc views cannot disagree.
fn session_doc_stale_identities(
    old_scope_counts: &[(String, i64)],
    doc_identity: &dyn Fn(i64) -> String,
    chunks: Option<&[SessionScopeDoc]>,
) -> Vec<String> {
    let Some(chunks) = chunks else {
        return Vec::new();
    };
    let keep: std::collections::HashSet<String> = chunks
        .iter()
        .map(|chunk| doc_identity(chunk.slot as i64))
        .collect();
    let mut stale = Vec::new();
    for (scope, count) in old_scope_counts {
        let ordinal = session_scope_ordinal(scope);
        for seq in 0..(*count).max(0).min(SESSION_DOC_MAX_CHUNKS_PER_SCOPE as i64) as u64 {
            let slot = SESSION_DOC_BASE + (ordinal << SESSION_DOC_SCOPE_SHIFT) + seq;
            let identity = doc_identity(slot as i64);
            if !keep.contains(&identity) {
                stale.push(identity);
            }
        }
    }
    stale
}

/// Targeted metadata-region replacement for annotation/tag edits and
/// child-link fixups: only `meta:*` rows are re-derived, preserving the
/// message-bound ordinals. Marks the session's docs `meta_pending` until
/// the index lane replaces the meta documents. Reads canonical identity
/// itself; a session with no marks row yet simply has no region inventory
/// to patch (its next generation publishes everything).
pub fn replace_meta_regions(db: &impl FactsDb, conversation_id: i64) -> Result<()> {
    let mark = region_mark(db, conversation_id)?;
    let Some((_, source_revision, _, _)) = mark else {
        return Ok(());
    };
    // The `NOT LIKE 'meta:%'` residual is applied in Rust, not in the
    // WHERE clause, so the bare `conversation_id` equality seeks the
    // index directly instead of scanning the region table. The fold
    // reproduces `COALESCE(MAX(region_ordinal), -1)` exactly;
    // `is_meta_region_key` carries the LIKE pattern's
    // ASCII-case-insensitive semantics.
    let max_message_ordinal: i64 = db
        .facts_query_map(
            "SELECT region_key, region_ordinal FROM viewer_search_regions \
             WHERE conversation_id = ?1",
            &[ParamValue::from(conversation_id)],
            |row| Ok((row.get_typed::<String>(0)?, row.get_typed::<i64>(1)?)),
        )?
        .into_iter()
        .filter(|(region_key, _)| !is_meta_region_key(region_key))
        .map(|(_, region_ordinal)| region_ordinal)
        .max()
        .unwrap_or(-1);
    let old_meta: Vec<i64> = db.facts_query_map(
        "SELECT quill_idx FROM viewer_search_regions \
         WHERE conversation_id = ?1 AND region_key LIKE 'meta:%'",
        &[ParamValue::from(conversation_id)],
        |row| row.get_typed::<i64>(0),
    )?;
    // Stage 1: the meta replacement rewrites text that rides the
    // session×scope chunks (notes live in the chat scope, children in
    // subagents), so the session-doc stale queue needs the pre-delete
    // per-scope row counts as its conservative old-slot bound — read
    // BEFORE the meta DELETE, exactly like the full republication.
    let old_scope_counts = old_region_scope_counts(db, conversation_id)?;
    let meta = derive_meta_regions(
        db,
        conversation_id,
        source_revision,
        max_message_ordinal + 1,
    )?;
    db.facts_execute(
        "DELETE FROM viewer_search_regions WHERE conversation_id = ?1 \
         AND region_key LIKE 'meta:%'",
        &[ParamValue::from(conversation_id)],
    )?;
    for region in &meta {
        insert_region(db, conversation_id, region)?;
    }
    // Queue stale meta doc identities for the index lane.
    let keep: Vec<i64> = meta.iter().map(|r| r.quill_idx).collect();
    let mut pending: Vec<String> = db
        .facts_query_map(
            "SELECT pending_doc_deletes FROM viewer_search_region_marks \
             WHERE conversation_id = ?1",
            &[ParamValue::from(conversation_id)],
            |row| row.get_typed::<Option<String>>(0),
        )?
        .first()
        .and_then(|raw| raw.as_deref().map(str::to_owned))
        .and_then(|raw| serde_json::from_str::<Vec<String>>(&raw).ok())
        .unwrap_or_default();
    if let Some((source_id, source_path)) = region_doc_identity_parts(db, conversation_id)? {
        let doc_identity = |slot: i64| {
            frankensearch::quill::cass::cass_document_identity(
                &source_id,
                frankensearch::quill::cass::CassConversationKey {
                    source_path: &source_path,
                    id: Some(conversation_id),
                },
                slot as u64,
            )
        };
        // A queued delete for a slot the new meta inventory reclaims would
        // remove a live document after the commit counted it, as
        // `stale_doc_ids_json` already prevents on full republication.
        let keep_ids: Vec<String> = keep.iter().map(|slot| doc_identity(*slot)).collect();
        // GH #457: the session×scope keep set — the identities the CURRENT
        // derivation keeps — filters this ride-forward too: a queued
        // session chunk that was stale when an earlier epoch wrote the
        // queue can be LIVE now (the conversation grew), and the stamped
        // commit would delete a document the run already counted. `None`
        // (derivation bail) keeps the old conservative ride-forward: the
        // staging lane bails on the same derivation, so no live session
        // document can exist for this conversation in a fresh candidate.
        let session_chunks = session_scope_docs(db, conversation_id)?;
        let session_keep: Option<std::collections::HashSet<String>> =
            session_chunks.as_deref().map(|chunks| {
                chunks
                    .iter()
                    .map(|chunk| doc_identity(chunk.slot as i64))
                    .collect()
            });
        pending.retain(|id| {
            !keep_ids.contains(id) && session_keep.as_ref().is_none_or(|live| !live.contains(id))
        });
        for slot in old_meta.iter().filter(|slot| !keep.contains(slot)) {
            pending.push(doc_identity(*slot));
        }
        // Stage 1: session×scope stale queue — the keep set is derived from
        // the rows the replacement just wrote (the same function the
        // staging lane materializes with), the old slots are bounded by the
        // pre-delete per-scope counts, and identities that never existed
        // are ignored by the post-commit delete.
        let session_stale = session_doc_stale_identities(
            &old_scope_counts,
            &doc_identity,
            session_chunks.as_deref(),
        );
        pending.extend(session_stale);
    }
    db.facts_execute(
        "UPDATE viewer_search_region_marks SET docs_state = ?2, \
         pending_doc_deletes = ?3 WHERE conversation_id = ?1 \
         AND EXISTS (SELECT 1 FROM conversations c WHERE c.id = ?1)",
        &[
            ParamValue::from(conversation_id),
            ParamValue::from(DOCS_META_PENDING),
            ParamValue::from(serde_json::to_string(&pending).unwrap_or_default()),
        ],
    )?;
    Ok(())
}
