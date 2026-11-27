use std::collections::HashMap;
use std::fmt::Write as _;

use anyhow::{Context, Result};
use serde_json::Value;

use crate::franken_sync::Row;
use crate::franken_sync::compat::{ParamValue, RowExt};
use crate::viewer_extensions::facts::{FACTS_EXTRACTOR_VERSION, FactsDb};
use crate::viewer_extensions::projection::PROJECTION_VERSION;

use super::docs::pack_region_boundaries;
use super::{
    DOCS_PUBLISHED, REGION_DOC_BASE, REGION_VERSION, RegionRow, SESSION_DOC_CHUNK_CHAR_CAP,
};

// ---------------------------------------------------------------------------
// Quill region documents (read-side of the persisted inventory)
// ---------------------------------------------------------------------------

fn row_to_region(row: &Row) -> Result<RegionRow, crate::franken_sync::FrankenError> {
    let indexed_text: Option<String> = row.get_typed(15)?;
    Ok(RegionRow {
        region_ordinal: row.get_typed(0)?,
        region_key: row.get_typed(1)?,
        scope: row.get_typed(2)?,
        scope_known: row.get_typed::<i64>(3)? != 0,
        quill_idx: row.get_typed(4)?,
        message_idx: row.get_typed(5)?,
        message_id: row.get_typed(6)?,
        step_index: row.get_typed(7)?,
        turn_index: row.get_typed(8)?,
        tool_name: row.get_typed(9)?,
        file_path: row.get_typed(10)?,
        exit_code: row.get_typed(11)?,
        has_error: row.get_typed::<Option<i64>>(12)?.map(|v| v != 0),
        error_type: row.get_typed(13)?,
        text_source: row.get_typed(14)?,
        has_fragment: indexed_text.is_some(),
        indexed_text,
        source_revision: row.get_typed(16)?,
    })
}

/// Column-shifted `RegionRow` read for the STRUCTURAL universe load: same
/// column layout as [`region_row_shifted`] except position 16 carries the
/// `indexed_text IS NOT NULL` bit instead of the fragment itself — the
/// universe load never materializes fragment text (Stage 1 lazy fragments)
/// and only records whether a fragment exists to read on demand.
fn region_row_structural(row: &Row) -> Result<RegionRow, crate::franken_sync::FrankenError> {
    Ok(RegionRow {
        region_ordinal: row.get_typed(1)?,
        region_key: row.get_typed(2)?,
        scope: row.get_typed(3)?,
        scope_known: row.get_typed::<i64>(4)? != 0,
        quill_idx: row.get_typed(5)?,
        message_idx: row.get_typed(6)?,
        message_id: row.get_typed(7)?,
        step_index: row.get_typed(8)?,
        turn_index: row.get_typed(9)?,
        tool_name: row.get_typed(10)?,
        file_path: row.get_typed(11)?,
        exit_code: row.get_typed(12)?,
        has_error: row.get_typed::<Option<i64>>(13)?.map(|v| v != 0),
        error_type: row.get_typed(14)?,
        text_source: row.get_typed(15)?,
        has_fragment: row.get_typed::<i64>(16)? != 0,
        indexed_text: None,
        source_revision: row.get_typed(17)?,
    })
}

const REGION_SELECT: &str = "SELECT region_ordinal, region_key, scope, scope_known, quill_idx, \
     message_idx, message_id, step_index, turn_index, tool_name, file_path, \
     exit_code, has_error, error_type, text_source, indexed_text, source_revision \
     FROM viewer_search_regions WHERE conversation_id = ?1 ORDER BY region_ordinal";

/// All persisted region rows for one conversation (ordinal order).
pub fn regions_for_conversation(db: &impl FactsDb, conversation_id: i64) -> Result<Vec<RegionRow>> {
    db.facts_query_map(REGION_SELECT, &[ParamValue::from(conversation_id)], |row| {
        row_to_region(row)
    })
    .context("loading persisted search regions")
}

/// Region rows for a bounded candidate set (one indexed IN-scan).
pub fn regions_for_conversations(
    db: &impl FactsDb,
    conversation_ids: &[i64],
) -> Result<HashMap<i64, Vec<RegionRow>>> {
    let mut out: HashMap<i64, Vec<RegionRow>> = HashMap::new();
    if conversation_ids.is_empty() {
        return Ok(out);
    }
    const CHUNK: usize = 900;
    for chunk in conversation_ids.chunks(CHUNK) {
        let mut sql = String::from(
            "SELECT conversation_id, region_ordinal, region_key, scope, scope_known, \
             quill_idx, message_idx, message_id, step_index, turn_index, tool_name, \
             file_path, exit_code, has_error, error_type, text_source, indexed_text, \
             source_revision FROM viewer_search_regions WHERE conversation_id IN (",
        );
        let mut params: Vec<ParamValue> = Vec::with_capacity(chunk.len());
        for (i, cid) in chunk.iter().enumerate() {
            if i > 0 {
                sql.push(',');
            }
            let _ = write!(sql, "?{}", i + 1);
            params.push(ParamValue::from(*cid));
        }
        sql.push_str(") ORDER BY conversation_id, region_ordinal");
        for (cid, region) in db.facts_query_map(&sql, &params, |row| {
            Ok((row.get_typed::<i64>(0)?, region_row_shifted(row)?))
        })? {
            out.entry(cid).or_default().push(region);
        }
    }
    Ok(out)
}

/// Column-shifted `RegionRow` read for a `conversation_id`-prefixed
/// SELECT (columns 1..=17 of the batch loaders).
fn region_row_shifted(row: &Row) -> Result<RegionRow, crate::franken_sync::FrankenError> {
    let indexed_text: Option<String> = row.get_typed(16)?;
    Ok(RegionRow {
        region_ordinal: row.get_typed(1)?,
        region_key: row.get_typed(2)?,
        scope: row.get_typed(3)?,
        scope_known: row.get_typed::<i64>(4)? != 0,
        quill_idx: row.get_typed(5)?,
        message_idx: row.get_typed(6)?,
        message_id: row.get_typed(7)?,
        step_index: row.get_typed(8)?,
        turn_index: row.get_typed(9)?,
        tool_name: row.get_typed(10)?,
        file_path: row.get_typed(11)?,
        exit_code: row.get_typed(12)?,
        has_error: row.get_typed::<Option<i64>>(13)?.map(|v| v != 0),
        error_type: row.get_typed(14)?,
        text_source: row.get_typed(15)?,
        has_fragment: indexed_text.is_some(),
        indexed_text,
        source_revision: row.get_typed(17)?,
    })
}

/// Region rows for the whole filtered universe in one set-based read,
/// bound by the search lane's `IN (universe subquery)` so archives whose
/// id-list chunking cannot fit the request deadline still load in one
/// statement.
///
/// Stage 1 lazy fragments (§6.3): the load is STRUCTURAL-ONLY —
/// `indexed_text` never rides it. Position 16 carries the
/// `indexed_text IS NOT NULL` bit instead (`region_row_structural`), so
/// the universe load stops paying one fragment transfer per region
/// (~6 s on a 762k-region archive); the query lane reads fragments on
/// demand, only for the regions its witnesses and snippets actually need.
///
/// Takes `&Connection`/`&[SqliteValue]` (not `&impl FactsDb`/
/// `&[ParamValue]`) because the caller already holds the ordered universe
/// ids and subquery parameters in that form — no value-type conversion.
pub fn regions_for_universe_subquery(
    connection: &crate::franken_sync::Connection,
    id_subquery: &str,
    params: &[crate::franken_sync::SqliteValue],
) -> Result<HashMap<i64, Vec<RegionRow>>> {
    let sql = format!(
        "SELECT conversation_id, region_ordinal, region_key, scope, scope_known, \
         quill_idx, message_idx, message_id, step_index, turn_index, tool_name, \
         file_path, exit_code, has_error, error_type, text_source, \
         indexed_text IS NOT NULL, source_revision FROM viewer_search_regions \
         WHERE conversation_id IN ({id_subquery}) ORDER BY conversation_id, region_ordinal"
    );
    let rows = connection
        .query_with_params(&sql, params)
        .context("loading universe search regions")?;
    let mut out: HashMap<i64, Vec<RegionRow>> = HashMap::new();
    for row in &rows {
        let cid = row.get_typed::<i64>(0)?;
        let region = region_row_structural(row)?;
        out.entry(cid).or_default().push(region);
    }
    Ok(out)
}

/// The structural column list shared by the Stage 1b on-demand region
/// reads — the same 18 positions [`regions_for_universe_subquery`] selects,
/// with `indexed_text IS NOT NULL` at position 16 so the fragment TEXT
/// itself never rides a query-lane read (Stage 1 §6.3 lazy fragments).
const REGION_STRUCTURAL_COLUMNS: &str = "SELECT conversation_id, region_ordinal, region_key, \
     scope, scope_known, quill_idx, message_idx, message_id, step_index, turn_index, \
     tool_name, file_path, exit_code, has_error, error_type, text_source, \
     indexed_text IS NOT NULL, source_revision FROM viewer_search_regions";

/// Every structural region row of ONE conversation, ordinal order — the
/// Stage 1b fast lane's page-time load (`emitted_page_sessions` mode): one
/// indexed read per EMITTED session, the same rows in the same order the
/// universe load attached before Stage 1b, so page-time witness discovery
/// (Stage 1 §6.4) consults exactly the region set it always saw.
pub fn regions_for_conversation_structural(
    connection: &crate::franken_sync::Connection,
    conversation_id: i64,
) -> Result<Vec<RegionRow>> {
    let sql = format!(
        "{REGION_STRUCTURAL_COLUMNS} \
         WHERE conversation_id = ?1 ORDER BY region_ordinal"
    );
    let rows = connection
        .query_with_params(
            &sql,
            &[crate::franken_sync::SqliteValue::Integer(conversation_id)],
        )
        .context("loading one session's search regions")?;
    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        out.push(region_row_structural(row)?);
    }
    Ok(out)
}

/// Chunk bound for the gated per-conversation `quill_idx IN (...)` seeks —
/// 501 bound parameters per statement stays well inside the engine's
/// variable ceiling while keeping one indexed seek per conversation chunk.
const QUILL_SLOT_CHUNK: usize = 500;

/// Structural region rows for an explicit batch of (conversation,
/// `quill_idx`) slots — the Stage 1b legacy lane's gated read
/// (`gated_quill_seeks` mode). One `conversation_id = ? AND quill_idx IN
/// (...)` statement per conversation chunk seeks
/// `idx_viewer_search_regions_quill` directly; the caller's slot sets are
/// exactly the Quill hit gates the search lane already holds in memory, so
/// a session with no gated hits for any plan literal reads nothing at all.
/// Rows come back per conversation in `region_ordinal` order (chunk results
/// are re-sorted because chunks split the SORTED `quill_idx` list, not the
/// ordinal run), matching the iteration order the eager universe load
/// produced — byte-identical evaluation depends on that order.
pub fn regions_for_quill_slots(
    connection: &crate::franken_sync::Connection,
    slots: &HashMap<i64, Vec<i64>>,
) -> Result<HashMap<i64, Vec<RegionRow>>> {
    let mut out: HashMap<i64, Vec<RegionRow>> = HashMap::with_capacity(slots.len());
    // Deterministic conversation order (queries are independent; the sort
    // only stabilizes diagnostics-visible query sequencing).
    let mut cids: Vec<i64> = slots.keys().copied().collect();
    cids.sort_unstable();
    for cid in cids {
        let wanted: Vec<i64> = slots
            .get(&cid)
            .map(|slots| {
                slots
                    .iter()
                    .copied()
                    .collect::<std::collections::BTreeSet<i64>>()
            })
            .unwrap_or_default()
            .into_iter()
            .collect();
        let mut rows: Vec<RegionRow> = Vec::new();
        for chunk in wanted.chunks(QUILL_SLOT_CHUNK) {
            if chunk.is_empty() {
                continue;
            }
            let mut sql = format!(
                "{REGION_STRUCTURAL_COLUMNS} WHERE conversation_id = ?1 AND quill_idx IN (?1"
            );
            for index in 0..chunk.len() {
                let _ = write!(sql, ",?{}", index + 2);
            }
            sql.push_str(") ORDER BY region_ordinal");
            let mut params: Vec<crate::franken_sync::SqliteValue> =
                Vec::with_capacity(chunk.len() + 1);
            params.push(crate::franken_sync::SqliteValue::Integer(cid));
            for slot in chunk {
                params.push(crate::franken_sync::SqliteValue::Integer(*slot));
            }
            let chunk_rows = connection
                .query_with_params(&sql, &params)
                .context("loading gated search regions")?;
            for row in &chunk_rows {
                rows.push(region_row_structural(row)?);
            }
        }
        rows.sort_by_key(|row| row.region_ordinal);
        out.insert(cid, rows);
    }
    Ok(out)
}

/// Whether this archive carries the extension profile's marks table.
/// Canonical-only stores (minimal test schemas, archives opened before
/// the viewer profile ever installed) legitimately lack it; every
/// marks-listing helper treats that as an empty inventory rather than a
/// query error, matching [`live_region_doc_bound`]'s documented `0`.
/// The re-projection planner uses this as its archive gate: a
/// non-extension archive has no fast lane to converge toward.
pub fn has_region_marks_table(db: &impl FactsDb) -> Result<bool> {
    Ok(!db
        .facts_query_map(
            "SELECT 1 FROM sqlite_master WHERE type='table' \
             AND name='viewer_search_region_marks' LIMIT 1",
            &[],
            |row| row.get_typed::<i64>(0),
        )?
        .is_empty())
}

/// Upper bound on live NON-canonical documents across the archive: the
/// region documents plus the Stage 1 session×scope documents.
///
/// Region share: only persisted rows that actually own a document count —
/// the synthetic-slot rows (`quill_idx >= REGION_DOC_BASE`) carrying a
/// bounded `indexed_text` fragment, i.e. exactly the rows [`super::docs::region_docs`]
/// materializes. Chat regions (canonical `quill_idx`, `indexed_text`
/// NULL) map onto the message document and own none, so counting
/// `marks.region_count` here over-bounded the region share and
/// under-counted the observed canonical-message share, breaking watch
/// checkpoint refreshes and the early-exit equality.
///
/// Session×scope share (Stage 1): every (conversation, scope) chunk the
/// current rows pack into under `super::docs::pack_region_boundaries` — the same
/// greedy packer the staging lane materializes documents with, fed the
/// same per-region char contributions (`LENGTH(text) + 1`), so the bound
/// is exact-or-OVER at convergence, never exact (review M-3): the fold
/// counts every marked conversation's region rows, while staging also
/// requires a resolvable doc identity (`source_path`) — a marked
/// conversation without one contributes to the bound but publishes
/// nothing. Over is the safe direction for a bound (the gh473 watch
/// can only over-report), and that slack is exactly where a real
/// runaway could hide, so drift through it must be investigated, not
/// amortized. This streams one row per region and evaluates `LENGTH()`
/// over every chat region's message content — the one deliberate cost of
/// keeping the bound SQL-free of a persisted length column (no DDL is
/// allowed for Stage 1); it runs on indexer-side certification paths
/// only, never on the query lane.
///
/// `pending_doc_deletes` bounds stale identities still awaiting removal
/// (region drops and the conservative session-doc queue alike).
/// `0` on archives without the extension profile (no marks table). The
/// lexical publication receipt uses this to split the total indexed-doc
/// count back into its canonical-message share so the
/// `indexed_docs <= total_messages` certification stays exact.
pub fn live_region_doc_bound(db: &impl FactsDb) -> Result<u64> {
    if !has_region_marks_table(db)? {
        return Ok(0);
    }
    // Set-based form of the per-marks-row count, avoiding a correlated scalar
    // `COUNT(*)` subquery over `(conversation_id, quill_idx)`. The
    // JOIN + GROUP BY below yields exactly one row per marks row whose
    // conversation exists (`marks.conversation_id` is the primary key, and
    // `conversations.id` is INTEGER PRIMARY KEY, so the join cannot
    // duplicate it), and the LEFT JOIN keeps marks with no live regions at
    // a count of zero.
    let mut bound = 0_u64;
    for (deletes_json, doc_rows) in db.facts_query_map(
        "SELECT m.pending_doc_deletes, COUNT(r.conversation_id) \
         FROM viewer_search_region_marks m \
         JOIN conversations c ON c.id = m.conversation_id \
         LEFT JOIN viewer_search_regions r \
           ON r.conversation_id = m.conversation_id \
          AND r.quill_idx >= ?1 AND r.indexed_text IS NOT NULL \
         GROUP BY m.conversation_id, m.pending_doc_deletes",
        &[ParamValue::from(REGION_DOC_BASE as i64)],
        |row| Ok((row.get_typed::<String>(0)?, row.get_typed::<i64>(1)?)),
    )? {
        bound = bound.saturating_add(doc_rows.max(0) as u64);
        let deletes: Vec<Value> = serde_json::from_str(&deletes_json).unwrap_or_default();
        bound = bound.saturating_add(deletes.len() as u64);
    }
    // Session×scope share: stream every marked conversation's per-region
    // char contributions (region-ordinal order within each scope) and
    // pack them with the SAME greedy chunker the staging lane uses. The
    // INNER JOIN drops marks rows with no regions (they pack zero
    // chunks); the ORDER BY keeps each (conversation, scope) group
    // contiguous so the fold below never buffers more than one scope.
    let mut session_docs = 0_u64;
    let mut lengths: Vec<u64> = Vec::new();
    let mut current: Option<(i64, String)> = None;
    let flush = |key: &Option<(i64, String)>, lengths: &mut Vec<u64>, session_docs: &mut u64| {
        if key.is_some() {
            *session_docs = session_docs.saturating_add(
                pack_region_boundaries(lengths, SESSION_DOC_CHUNK_CHAR_CAP as u64).len() as u64,
            );
        }
        lengths.clear();
    };
    for (cid, scope, chars) in db.facts_query_map(
        "SELECT r.conversation_id, r.scope, \
                LENGTH(COALESCE(r.indexed_text, CASE WHEN r.text_source = 'chat' \
                 THEN COALESCE(m.content, '') ELSE '' END)) + 1 \
         FROM viewer_search_region_marks mk \
         JOIN conversations c ON c.id = mk.conversation_id \
         JOIN viewer_search_regions r ON r.conversation_id = mk.conversation_id \
         LEFT JOIN messages m ON m.id = r.message_id \
         ORDER BY r.conversation_id, r.scope, r.region_ordinal",
        &[],
        |row| {
            Ok((
                row.get_typed::<i64>(0)?,
                row.get_typed::<String>(1)?,
                row.get_typed::<i64>(2)?,
            ))
        },
    )? {
        let key = (cid, scope);
        if current.as_ref() != Some(&key) {
            flush(&current, &mut lengths, &mut session_docs);
            current = Some(key);
        }
        lengths.push(chars.max(0) as u64);
    }
    flush(&current, &mut lengths, &mut session_docs);
    Ok(bound.saturating_add(session_docs))
}

/// `(region_version, source_revision, region_count, docs_state)` for a
/// conversation's marks row; `None` when no publication has committed.
pub fn region_mark(
    db: &impl FactsDb,
    conversation_id: i64,
) -> Result<Option<(i64, i64, i64, String)>> {
    Ok(db
        .facts_query_map(
            "SELECT region_version, source_revision, region_count, docs_state \
             FROM viewer_search_region_marks WHERE conversation_id = ?1",
            &[ParamValue::from(conversation_id)],
            |row| {
                Ok((
                    row.get_typed::<i64>(0)?,
                    row.get_typed::<i64>(1)?,
                    row.get_typed::<i64>(2)?,
                    row.get_typed::<String>(3)?,
                ))
            },
        )?
        .into_iter()
        .next())
}

/// Marks rows for a bounded candidate set.
pub fn region_marks_for(
    db: &impl FactsDb,
    conversation_ids: &[i64],
) -> Result<HashMap<i64, (i64, i64, i64, String)>> {
    let mut out = HashMap::new();
    if conversation_ids.is_empty() {
        return Ok(out);
    }
    const CHUNK: usize = 900;
    for chunk in conversation_ids.chunks(CHUNK) {
        let mut sql = String::from(
            "SELECT conversation_id, region_version, source_revision, region_count, \
             docs_state FROM viewer_search_region_marks WHERE conversation_id IN (",
        );
        let mut params: Vec<ParamValue> = Vec::with_capacity(chunk.len());
        for (i, cid) in chunk.iter().enumerate() {
            if i > 0 {
                sql.push(',');
            }
            let _ = write!(sql, "?{}", i + 1);
            params.push(ParamValue::from(*cid));
        }
        sql.push(')');
        for row in db.facts_query_map(&sql, &params, |row| {
            Ok((
                row.get_typed::<i64>(0)?,
                (
                    row.get_typed::<i64>(1)?,
                    row.get_typed::<i64>(2)?,
                    row.get_typed::<i64>(3)?,
                    row.get_typed::<String>(4)?,
                ),
            ))
        })? {
            out.insert(row.0, row.1);
        }
    }
    Ok(out)
}

/// `docs_state = 'pending'` sessions — the index lane's dirty set after
/// ingest publication or meta replacement ran without a doc commit yet.
/// Empty on archives without the extension profile (no marks table).
pub fn pending_region_doc_sessions(db: &impl FactsDb, limit: i64) -> Result<Vec<i64>> {
    if !has_region_marks_table(db)? {
        return Ok(Vec::new());
    }
    db.facts_query_map(
        "SELECT conversation_id FROM viewer_search_region_marks m \
         WHERE m.docs_state != ?1 \
         AND EXISTS (SELECT 1 FROM conversations c WHERE c.id = m.conversation_id) \
         ORDER BY m.conversation_id LIMIT ?2",
        &[ParamValue::from(DOCS_PUBLISHED), ParamValue::from(limit)],
        |row| row.get_typed::<i64>(0),
    )
    .context("listing sessions with pending region documents")
}

/// EVERY session carrying a marks row — the authoritative rebuild lane's
/// converge set. A from-scratch index build owns no region documents at
/// all, so `published` marks must be re-materialized too; `pending`/
/// `meta_pending` states reconcile through the same pass. Empty on
/// archives without the extension profile (no marks table).
pub fn marked_region_doc_sessions(db: &impl FactsDb, limit: i64) -> Result<Vec<i64>> {
    if !has_region_marks_table(db)? {
        return Ok(Vec::new());
    }
    db.facts_query_map(
        "SELECT conversation_id FROM viewer_search_region_marks m \
         WHERE EXISTS (SELECT 1 FROM conversations c WHERE c.id = m.conversation_id) \
         ORDER BY m.conversation_id LIMIT ?1",
        &[ParamValue::from(limit)],
        |row| row.get_typed::<i64>(0),
    )
    .context("listing sessions carrying search-region marks")
}

/// Count of sessions whose region marks carry a stale
/// [`REGION_VERSION`] — the whole-archive republish set a version bump
/// creates. Stale marks still read `docs_state = 'published'` for their
/// OLD version, so the pending sweep does not see them: the lane
/// re-projects and re-converges them session by session as it re-persists
/// each one. The incremental lane counts this set ONCE per run to decide
/// whether region-doc convergence should ride the per-batch prelude (a
/// small set: ordinary incremental runs and watch events) or defer to the
/// run's final commit (a republish of hundreds of sessions publishing one
/// segment per persist batch is the quadratic the bounded batch staging
/// exists to avoid). Zero on archives without the extension profile.
pub fn stale_region_version_session_count(db: &impl FactsDb) -> Result<i64> {
    if !has_region_marks_table(db)? {
        return Ok(0);
    }
    let rows = db
        .facts_query_map(
            "SELECT COUNT(*) FROM viewer_search_region_marks m \
             WHERE m.region_version != ?1 \
             AND EXISTS (SELECT 1 FROM conversations c WHERE c.id = m.conversation_id)",
            &[ParamValue::from(REGION_VERSION)],
            |row| row.get_typed::<i64>(0),
        )
        .context("counting sessions with stale-region-version marks")?;
    Ok(rows.first().copied().unwrap_or(0))
}

/// The stale set for the incremental re-projection lane: every
/// conversation whose region marks are missing or at an older
/// [`REGION_VERSION`], or whose facts row is missing or at an older
/// [`crate::viewer_extensions::facts::FACTS_EXTRACTOR_VERSION`], or whose projection row is
/// missing or at an older [`crate::viewer_extensions::projection::PROJECTION_VERSION`] /
/// extractor version. The lane re-projects EXACTLY these conversations
/// from the PERSISTED archive (never from their sources), so the sweep
/// deliberately carries no source-path, source-id or attempts gating: a
/// session whose source file is gone converges through the archive
/// exactly like a live one, and the Stage-1 fast-lane gate counts every
/// conversation, so the lane must too.
///
/// Deliberately a LEFT JOIN over PK-anchored rows rather than correlated
/// subqueries (the same shape `live_region_doc_bound` uses at archive
/// scale); these joins use the index-seek shape that helper proved at
/// 6k sessions. Marks at the
/// CURRENT version with a non-`published` `docs_state` are NOT stale:
/// the pending sweep (the run's final drain) already converges those.
/// Empty on archives without the extension profile — a non-extension
/// archive has no fast lane to converge toward, and its first ingest
/// installs the profile anyway.
pub fn region_reproject_stale_session_ids(db: &impl FactsDb) -> Result<Vec<i64>> {
    if !has_region_marks_table(db)? {
        return Ok(Vec::new());
    }
    let sql = "SELECT c.id \
         FROM conversations c \
         LEFT JOIN viewer_search_region_marks m ON m.conversation_id = c.id \
         LEFT JOIN viewer_session_facts f ON f.conversation_id = c.id \
         LEFT JOIN viewer_session_projection p ON p.conversation_id = c.id \
         WHERE COALESCE(m.region_version, -1) != ?1 \
           OR COALESCE(f.extractor_version, -1) != ?2 \
           OR COALESCE(p.projection_version, -1) != ?3 \
           OR COALESCE(p.extractor_version, -1) != ?2 \
         ORDER BY c.id";
    let params = [
        ParamValue::from(REGION_VERSION),
        ParamValue::from(FACTS_EXTRACTOR_VERSION),
        ParamValue::from(PROJECTION_VERSION),
    ];
    db.facts_query_map(sql, &params, |row| row.get_typed::<i64>(0))
        .context("listing sessions needing region re-projection")
}
