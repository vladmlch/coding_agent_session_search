use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use frankensearch::quill::cass::CassDocument;

use crate::franken_sync::compat::{ParamValue, RowExt};
use crate::viewer_extensions::facts::FactsDb;

/// Index writer sink receiving staged search-region Quill documents.
pub trait RegionDocSink {
    /// Upsert one batch of search-region documents under their stable identities.
    fn upsert_cass_documents(&mut self, documents: &[CassDocument]) -> Result<()>;
}

use super::derive_helpers::bounded;
use super::queries::regions_for_conversation;
use super::{
    DOCS_PUBLISHED, REGION_DOC_BASE, REGION_VERSION, RegionRow, SESSION_DOC_BASE,
    SESSION_DOC_CHUNK_CHAR_CAP, SESSION_DOC_MAX_CHUNKS_PER_SCOPE, SESSION_DOC_SCOPE_SHIFT,
    TEXT_SOURCE_CHAT, session_scope_ordinal,
};

/// The canonical identity fields every synthetic-slot Quill document for a
/// conversation is minted from — shared by the per-region docs
/// ([`region_docs`]) and the session×scope docs ([`session_scope_docs`])
/// so both lanes agree on `source_path`/`source_id`/`title`/`created_at`.
struct RegionDocIdentity {
    source_path: String,
    source_id: String,
    title: Option<String>,
    started_at: Option<i64>,
    agent: String,
    workspace: Option<String>,
}

/// Read the identity a conversation's synthetic-slot documents are minted
/// from. `None` when the conversation row is missing or carries an empty
/// `source_path` — the same honesty guard [`region_docs`] always applied:
/// a session that cannot be named cannot own documents.
fn region_doc_identity(
    db: &impl FactsDb,
    conversation_id: i64,
) -> Result<Option<RegionDocIdentity>> {
    let identity_rows = db.facts_query_map(
        "SELECT c.source_path, c.source_id, c.title, c.started_at, \
                COALESCE((SELECT a.slug FROM agents a WHERE a.id = c.agent_id), 'unknown'), \
                (SELECT w.path FROM workspaces w WHERE w.id = c.workspace_id) \
         FROM conversations c WHERE c.id = ?1 LIMIT 1",
        &[ParamValue::from(conversation_id)],
        |row| {
            Ok((
                row.get_typed::<Option<String>>(0)?,
                row.get_typed::<Option<String>>(1)?,
                row.get_typed::<Option<String>>(2)?,
                row.get_typed::<Option<i64>>(3)?,
                row.get_typed::<Option<String>>(4)?,
                row.get_typed::<Option<String>>(5)?,
            ))
        },
    )?;
    let Some((Some(source_path), source_id_col, title, started_at, agent, workspace)) =
        identity_rows.into_iter().next()
    else {
        return Ok(None);
    };
    if source_path.is_empty() {
        return Ok(None);
    }
    Ok(Some(RegionDocIdentity {
        source_path,
        source_id: source_id_col.unwrap_or_else(|| "local".to_owned()),
        title,
        started_at,
        agent: agent.unwrap_or_else(|| "unknown".to_owned()),
        workspace,
    }))
}

/// One packed session×scope chunk: its synthetic slot and full text.
pub struct SessionScopeDoc {
    pub slot: u64,
    pub text: String,
}

/// Greedy region-boundary packing — the ONE authoritative session-doc
/// chunker, shared by staging ([`session_scope_docs`]), the publication
/// keep set and the live-doc bound, so the three can never disagree on a
/// slot. Whole regions accumulate into a chunk until the next region
/// would push it past `cap` characters; an oversized region takes a chunk
/// of its own. `lengths` are per-region char contributions INCLUDING the
/// `'\n'` separator ([`super::SESSION_REGION_TEXT_SQL`]'s `+ 1`); the returned
/// `(start, end)` ranges are half-open index ranges over `lengths`.
pub fn pack_region_boundaries(lengths: &[u64], cap: u64) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    let mut start = 0_usize;
    let mut total = 0_u64;
    for (index, &len) in lengths.iter().enumerate() {
        if index > start && total.saturating_add(len) > cap {
            ranges.push((start, index));
            start = index;
            total = 0;
        }
        total = total.saturating_add(len);
    }
    if start < lengths.len() {
        ranges.push((start, lengths.len()));
    }
    ranges
}

/// The authoritative session×scope doc derivation for one conversation
/// (Stage 1 §6.1/§6.2): reads the committed region inventory, groups the
/// regions by scope in region-ordinal order, and packs each scope's texts
/// into whole-region chunks of at most [`SESSION_DOC_CHUNK_CHAR_CAP`]
/// characters. Every region contributes its text (`indexed_text` when a
/// fragment exists — noise-chat and every derived/meta row — else the
/// anchored message content for `chat` rows, else the empty string) plus
/// one `'\n'` separator, so no token ever merges across a region
/// boundary. The same function runs at staging (documents to upsert) and
/// at publication (the keep set that filters the conservative stale
/// queue), which is what keeps their slot sets identical. `None` when the
/// conversation cannot own documents at all (missing row, empty
/// `source_path`).
pub fn session_scope_docs(
    db: &impl FactsDb,
    conversation_id: i64,
) -> Result<Option<Vec<SessionScopeDoc>>> {
    // The identity guard mirrors `region_docs`: a conversation that cannot
    // own region documents cannot own session documents either.
    if region_doc_identity(db, conversation_id)?.is_none() {
        return Ok(None);
    }
    let regions = regions_for_conversation(db, conversation_id)?;
    // Chat regions hydrate their anchored message content in one read.
    let mut message_texts: HashMap<i64, String> = HashMap::new();
    if regions
        .iter()
        .any(|r| r.text_source == TEXT_SOURCE_CHAT && r.indexed_text.is_none())
    {
        for (id, content) in db.facts_query_map(
            "SELECT id, content FROM messages WHERE conversation_id = ?1",
            &[ParamValue::from(conversation_id)],
            |row| {
                Ok((
                    row.get_typed::<i64>(0)?,
                    row.get_typed::<Option<String>>(1)?,
                ))
            },
        )? {
            if let Some(content) = content {
                message_texts.insert(id, content);
            }
        }
    }
    // Group texts by scope, preserving region-ordinal order.
    let mut scopes: Vec<(String, Vec<(String, u64)>)> = Vec::new();
    for region in &regions {
        let text: String = match region.indexed_text.as_ref() {
            Some(fragment) => fragment.clone(),
            None if region.text_source == TEXT_SOURCE_CHAT => region
                .message_id
                .and_then(|id| message_texts.get(&id).cloned())
                .unwrap_or_default(),
            None => String::new(),
        };
        let chars = (text.chars().count() + 1) as u64; // + '\n' separator
        match scopes.iter_mut().find(|(scope, _)| scope == &region.scope) {
            Some((_, texts)) => texts.push((text, chars)),
            None => scopes.push((region.scope.clone(), vec![(text, chars)])),
        }
    }
    let mut docs = Vec::new();
    for (scope, texts) in &scopes {
        let lengths: Vec<u64> = texts.iter().map(|(_, chars)| *chars).collect();
        let ordinal = session_scope_ordinal(scope);
        let ranges = pack_region_boundaries(&lengths, SESSION_DOC_CHUNK_CHAR_CAP as u64);
        if ranges.len() >= SESSION_DOC_MAX_CHUNKS_PER_SCOPE as usize {
            bail!(
                "conversation {conversation_id} scope {scope:?} packs {} session-doc chunks, \
                 beyond the 2^{} per-scope slot space (SESSION_DOC_BASE)",
                ranges.len(),
                SESSION_DOC_SCOPE_SHIFT
            );
        }
        for (chunk_seq, (start, end)) in ranges.iter().enumerate() {
            let mut text = String::new();
            for (piece, _) in &texts[*start..*end] {
                text.push_str(piece);
                text.push('\n');
            }
            docs.push(SessionScopeDoc {
                slot: SESSION_DOC_BASE + (ordinal << SESSION_DOC_SCOPE_SHIFT) + chunk_seq as u64,
                text,
            });
        }
    }
    Ok(Some(docs))
}

/// Build the Quill `CassDocument` set for one session's session×scope
/// chunks ([`session_scope_docs`]) — the Stage 1 fast path's membership
/// surface. `None` when the conversation cannot own documents.
pub fn session_scope_cass_documents(
    db: &impl FactsDb,
    conversation_id: i64,
) -> Result<Option<Vec<CassDocument>>> {
    let Some(identity) = region_doc_identity(db, conversation_id)? else {
        return Ok(None);
    };
    let Some(chunks) = session_scope_docs(db, conversation_id)? else {
        return Ok(None);
    };
    let docs = chunks
        .into_iter()
        .map(|chunk| CassDocument {
            agent: identity.agent.clone(),
            workspace: identity.workspace.clone(),
            workspace_original: None,
            source_path: identity.source_path.clone(),
            msg_idx: chunk.slot,
            created_at: identity.started_at,
            title: identity.title.clone(),
            content: chunk.text,
            source_id: identity.source_id.clone(),
            origin_kind: "local".to_owned(),
            origin_host: None,
            conversation_id: Some(conversation_id),
        })
        .collect();
    Ok(Some(docs))
}

/// Build the Quill `CassDocument` set for one session's persisted regions:
/// every synthetic-slot region (derived/meta/noise-chat) becomes one doc
/// whose content is the persisted `indexed_text` fragment.
pub fn region_docs(db: &impl FactsDb, conversation_id: i64) -> Result<Vec<CassDocument>> {
    let Some(identity) = region_doc_identity(db, conversation_id)? else {
        return Ok(Vec::new());
    };
    let regions = regions_for_conversation(db, conversation_id)?;
    let mut docs = Vec::new();
    for region in regions
        .iter()
        .filter(|r| r.quill_idx >= REGION_DOC_BASE as i64)
    {
        let Some(content) = region.indexed_text.as_deref() else {
            continue;
        };
        docs.push(CassDocument {
            agent: identity.agent.clone(),
            workspace: identity.workspace.clone(),
            workspace_original: None,
            source_path: identity.source_path.clone(),
            msg_idx: region.quill_idx as u64,
            created_at: identity.started_at,
            title: identity.title.clone(),
            content: content.to_owned(),
            source_id: identity.source_id.clone(),
            origin_kind: "local".to_owned(),
            origin_host: None,
            conversation_id: Some(conversation_id),
        });
    }
    Ok(docs)
}

/// The marks-row snapshot [`stage_region_docs`] read while materializing
/// a session's region documents. [`stamp_region_docs_published`] replays
/// it as a compare-and-swap after the index commit: if a concurrent
/// republish, meta replacement or delete-queue append landed between
/// staging and the commit, the row's `region_version`/`source_revision`/
/// `docs_state`/`pending_doc_deletes` no longer match and the stamp is
/// refused — the session stays honestly pending for the next converge
/// pass instead of certifying documents built against a stale inventory
/// or clobbering a newer pending delete list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionDocStamp {
    pub conversation_id: i64,
    pub region_version: i64,
    pub source_revision: i64,
    pub docs_state: String,
    /// Raw stored JSON — compared byte-for-byte so a republished delete
    /// queue (even a reordered one) is never silently overwritten.
    pub pending_doc_deletes: String,
    /// The identities this stage upserted — the keep set the post-commit
    /// replay never deletes. Every one of them is live in the candidate
    /// the stamp's commit publishes, so executing a queued delete against
    /// one is exactly the fresh-reader deficit GH #457's certification
    /// refuses to publish. A queue is data from ANOTHER epoch — a
    /// crash-left pending marks row no republish ever re-filters can
    /// still name a LIVE chunk — so the delete path guards by
    /// construction, whatever put a live identity in the queue. Empty
    /// unless the marks row carried queued deletes (nothing to filter
    /// then).
    pub keep_ids: Vec<String>,
}

/// Stage one session's region documents on the uncommitted index writer:
/// delete every queued stale identity, upsert the current docs, and
/// return the [`RegionDocStamp`] the caller replays through
/// [`stamp_region_docs_published`] AFTER the index commit lands. The
/// marks row is deliberately untouched here — stamping `published`
/// before the commit would certify documents a failed commit or crash
/// never shipped (a `published` mark is skipped by `converge_pending`,
/// so the absent docs would silently drop literals from search).
/// Returns `(upserted doc count, stamp)`; `(0, None)` when no region
/// inventory was ever published for the session — not a pending one, an
/// absent one — which the search lane treats as honest incomplete
/// evidence.
pub fn stage_region_docs(
    index: &mut impl RegionDocSink,
    db: &impl FactsDb,
    conversation_id: i64,
) -> Result<(usize, Option<RegionDocStamp>)> {
    let Some((docs, stamp)) = region_doc_stage(db, conversation_id)? else {
        return Ok((0, None));
    };
    let doc_count = docs.len();
    index
        .upsert_cass_documents(&docs)
        .context("upserting search-region documents")?;
    Ok((doc_count, Some(stamp)))
}

/// Read what [`stage_region_docs`] writes for one session without writing
/// it: the session's current region documents and the marks-row snapshot
/// that certifies them. `None` when the session has no marks row. Callers
/// that converge many sessions upsert the documents of many sessions in one
/// batch: replacing documents a committed candidate already holds costs one
/// publication per upsert call, and one call per session made a resumed
/// terminal converge quadratic in the session count.
pub fn region_doc_stage(
    db: &impl FactsDb,
    conversation_id: i64,
) -> Result<Option<(Vec<CassDocument>, RegionDocStamp)>> {
    let mark = db
        .facts_query_map(
            "SELECT region_version, source_revision, docs_state, pending_doc_deletes \
             FROM viewer_search_region_marks WHERE conversation_id = ?1",
            &[ParamValue::from(conversation_id)],
            |row| {
                Ok((
                    row.get_typed::<i64>(0)?,
                    row.get_typed::<i64>(1)?,
                    row.get_typed::<String>(2)?,
                    row.get_typed::<String>(3)?,
                ))
            },
        )?
        .into_iter()
        .next();
    let Some((region_version, source_revision, docs_state, pending_json)) = mark else {
        return Ok(None);
    };
    // Stale-identity deletes are NOT executed here: `delete_cass_document_ids`
    // requires a committed writer and would commit this writer mid-batch —
    // certifying every document staged so far (canonical AND region) ahead
    // of the real batch boundary. The stamp keeps the exact
    // `pending_doc_deletes` JSON so the post-commit stamp pass can retire
    // them and then compare-and-swap the queue away.
    //
    // Stage 1: the session×scope documents ride the same staging lane, so
    // the bridge writer, the indexer and the test harness all publish them
    // under the same marks discipline.
    let mut docs = region_docs(db, conversation_id)?;
    if let Some(session_docs) = session_scope_cass_documents(db, conversation_id)? {
        docs.extend(session_docs);
    }
    // GH #457 (Layer 2): snapshot the keep set — the identities this stage
    // is upserting — so the post-commit replay can refuse to delete a
    // queued identity the fresh candidate holds live. Built only when the
    // marks row carries queued deletes: nothing to filter otherwise.
    let keep_ids: Vec<String> =
        if serde_json::from_str::<Vec<String>>(&pending_json).is_ok_and(|q| !q.is_empty()) {
            docs.iter()
                .map(|doc| {
                    frankensearch::quill::cass::cass_document_identity(
                        &doc.source_id,
                        frankensearch::quill::cass::CassConversationKey {
                            source_path: &doc.source_path,
                            id: doc.conversation_id,
                        },
                        doc.msg_idx,
                    )
                })
                .collect()
        } else {
            Vec::new()
        };
    Ok(Some((
        docs,
        RegionDocStamp {
            conversation_id,
            region_version,
            source_revision,
            docs_state,
            pending_doc_deletes: pending_json,
            keep_ids,
        },
    )))
}

/// True when the session's region documents are already committed in the
/// live snapshot: its marks row is `published` at the current
/// `region_version`, carries no queued stale deletes, and binds the same
/// `source_revision` as its facts generation. An incremental lane skips
/// re-staging such a session — re-upserting an identity already live forces
/// Quill to publish a segment per session for no change. Anything else
/// (pending, meta-pending, a republished revision, a queued delete, or no
/// marks row) must stage. Rebuild lanes that write a fresh index never call
/// this; they re-materialize every marked session.
pub fn region_docs_published_current(db: &impl FactsDb, conversation_id: i64) -> Result<bool> {
    let mark = db
        .facts_query_map(
            "SELECT region_version, source_revision, docs_state, pending_doc_deletes \
             FROM viewer_search_region_marks WHERE conversation_id = ?1",
            &[ParamValue::from(conversation_id)],
            |row| {
                Ok((
                    row.get_typed::<i64>(0)?,
                    row.get_typed::<i64>(1)?,
                    row.get_typed::<String>(2)?,
                    row.get_typed::<String>(3)?,
                ))
            },
        )?
        .into_iter()
        .next();
    let Some((region_version, source_revision, docs_state, pending_json)) = mark else {
        return Ok(false);
    };
    if region_version != REGION_VERSION || docs_state != DOCS_PUBLISHED {
        return Ok(false);
    }
    let no_pending_deletes = serde_json::from_str::<Vec<String>>(&pending_json)
        .map(|ids| ids.is_empty())
        .unwrap_or(false);
    if !no_pending_deletes {
        return Ok(false);
    }
    let facts_revision = db
        .facts_query_map(
            "SELECT source_revision FROM viewer_session_facts \
             WHERE conversation_id = ?1 LIMIT 1",
            &[ParamValue::from(conversation_id)],
            |row| row.get_typed::<i64>(0),
        )?
        .into_iter()
        .next();
    Ok(facts_revision == Some(source_revision))
}

/// Stamp staged sessions `published` — callable ONLY after the index
/// commit that seals their documents. Each stamp replays the stage-time
/// marks snapshot as a compare-and-swap: `region_version`,
/// `source_revision`, `docs_state` and the exact `pending_doc_deletes`
/// JSON must still match or a concurrent republish/meta-edit queued work
/// this stamp never processed — those rows are left pending (returned
/// count excludes them) for the next converge pass. The `WHERE EXISTS`
/// guard shares the sort lane's honesty: a conversation deleted
/// mid-converge must never leave an orphan marks row stamped `published`.
pub fn stamp_region_docs_published(db: &impl FactsDb, stamps: &[RegionDocStamp]) -> Result<usize> {
    let mut stamped = 0_usize;
    for stamp in stamps {
        let updated = db.facts_execute(
            "UPDATE viewer_search_region_marks SET docs_state = 'published', \
             pending_doc_deletes = '[]' WHERE conversation_id = ?1 \
             AND region_version = ?2 AND source_revision = ?3 \
             AND docs_state = ?4 AND pending_doc_deletes = ?5 \
             AND EXISTS (SELECT 1 FROM conversations c WHERE c.id = ?1)",
            &[
                ParamValue::from(stamp.conversation_id),
                ParamValue::from(stamp.region_version),
                ParamValue::from(stamp.source_revision),
                ParamValue::from(stamp.docs_state.as_str()),
                ParamValue::from(stamp.pending_doc_deletes.as_str()),
            ],
        )?;
        stamped += usize::from(updated > 0);
    }
    Ok(stamped)
}

/// Hydrate one region's text for verification/snippets — the ONLY message
/// read the indexed query path performs, bounded by the hydrated region
/// set (`text_source='chat'` reads the canonical row it anchors; derived
/// and meta lanes return the persisted fragment).
pub fn hydrate_region_text(db: &impl FactsDb, region: &RegionRow) -> Result<Option<String>> {
    if region.text_source == TEXT_SOURCE_CHAT && region.indexed_text.is_none() {
        let Some(message_id) = region.message_id else {
            return Ok(None);
        };
        let rows = db.facts_query_map(
            "SELECT content FROM messages WHERE id = ?1 LIMIT 1",
            &[ParamValue::from(message_id)],
            |row| row.get_typed::<Option<String>>(0),
        )?;
        return Ok(rows.into_iter().next().flatten().map(|t| bounded(&t)));
    }
    Ok(region.indexed_text.clone())
}
