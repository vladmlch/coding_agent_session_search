//! Equivalence and edge-case coverage for [`live_region_doc_bound`].
//! The pre-rewrite correlated-subquery statement survives here only as
//! the oracle the set-based query must match.

use super::*;
use anyhow::Result;
use serde_json::Value;

use crate::franken_sync::compat::{ParamValue, RowExt};
use crate::model::types::{Agent, AgentKind};
use crate::storage::sqlite::FrankenStorage;
use crate::viewer_extensions::facts::FactsDb;

/// The original per-marks-row correlated statement, verbatim.
const CORRELATED_ORACLE_SQL: &str = "SELECT \
       (SELECT COUNT(*) FROM viewer_search_regions r \
        WHERE r.conversation_id = m.conversation_id \
          AND r.quill_idx >= ?1 AND r.indexed_text IS NOT NULL), \
       m.pending_doc_deletes \
     FROM viewer_search_region_marks m \
     WHERE EXISTS (SELECT 1 FROM conversations c WHERE c.id = m.conversation_id)";

/// Same accumulation as production: doc rows plus the length of the
/// `pending_doc_deletes` JSON array, unparseable JSON counting as zero,
/// plus the Stage 1 session×scope chunk count — here in the slow,
/// obviously-correct per-conversation form (one `session_scope_docs`
/// call per marked conversation), which the streaming GROUP-BY-fold
/// query in production must match exactly.
fn oracle_bound(db: &impl FactsDb) -> Result<u64> {
    let mut bound = 0_u64;
    for (doc_rows, deletes_json) in db.facts_query_map(
        CORRELATED_ORACLE_SQL,
        &[ParamValue::from(REGION_DOC_BASE as i64)],
        |row| Ok((row.get_typed::<i64>(0)?, row.get_typed::<String>(1)?)),
    )? {
        bound = bound.saturating_add(doc_rows.max(0) as u64);
        let deletes: Vec<Value> = serde_json::from_str(&deletes_json).unwrap_or_default();
        bound = bound.saturating_add(deletes.len() as u64);
    }
    for conversation_id in db.facts_query_map(
        "SELECT m.conversation_id FROM viewer_search_region_marks m \
         WHERE EXISTS (SELECT 1 FROM conversations c WHERE c.id = m.conversation_id)",
        &[],
        |row| row.get_typed::<i64>(0),
    )? {
        if let Some(chunks) = session_scope_docs(db, conversation_id)? {
            bound = bound.saturating_add(chunks.len() as u64);
        }
    }
    Ok(bound)
}

fn fixture() -> (tempfile::TempDir, FrankenStorage) {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = FrankenStorage::open(&temp.path().join("archive.db")).expect("canonical schema");
    crate::viewer_extensions::schema::ensure_schema(storage.raw()).expect("extension schema");
    (temp, storage)
}

fn agent_id(storage: &FrankenStorage) -> i64 {
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

fn insert_conversation(db: &impl FactsDb, id: i64, agent: i64) {
    db.facts_execute(
        "INSERT INTO conversations (id, agent_id, source_path) VALUES (?1, ?2, ?3)",
        &[
            ParamValue::from(id),
            ParamValue::from(agent),
            ParamValue::from(format!("/fixture/{id}.jsonl")),
        ],
    )
    .expect("conversation");
}

fn insert_marks(db: &impl FactsDb, conversation_id: i64, pending_doc_deletes: &str) {
    db.facts_execute(
        "INSERT INTO viewer_search_region_marks \
         (conversation_id, region_version, pending_doc_deletes) VALUES (?1, 1, ?2)",
        &[
            ParamValue::from(conversation_id),
            ParamValue::from(pending_doc_deletes),
        ],
    )
    .expect("marks");
}

fn insert_region(
    db: &impl FactsDb,
    conversation_id: i64,
    ordinal: i64,
    quill_idx: i64,
    indexed_text: Option<&str>,
) {
    db.facts_execute(
        "INSERT INTO viewer_search_regions \
         (conversation_id, region_ordinal, region_key, scope, quill_idx, indexed_text) \
         VALUES (?1, ?2, ?3, 'block', ?4, ?5)",
        &[
            ParamValue::from(conversation_id),
            ParamValue::from(ordinal),
            ParamValue::from(format!("region-{conversation_id}-{ordinal}")),
            ParamValue::from(quill_idx),
            ParamValue::from(indexed_text),
        ],
    )
    .expect("region");
}

fn base(offset: i64) -> i64 {
    REGION_DOC_BASE as i64 + offset
}

/// The session×scope chunk slot `session_doc_stale_identities`
/// conservatively covers for `scope`'s `chunk`-th old chunk.
fn session_slot(scope: &str, chunk: u64) -> i64 {
    (SESSION_DOC_BASE + (session_scope_ordinal(scope) << SESSION_DOC_SCOPE_SHIFT) + chunk) as i64
}

/// Review I-3(a): the ONE authoritative chunker multi-chunk packs.
/// Every case re-proves the invariants the three consumers (staging,
/// publication keep set, live-doc bound) all rely on: half-open
/// contiguous cover of the region list, multi-region chunks within
/// the cap, oversized regions isolated, no arithmetic overflow.
#[test]
fn pack_region_boundaries_multi_chunk_and_edge_cases() {
    fn assert_cover(lengths: &[u64], cap: u64) -> Vec<(usize, usize)> {
        let ranges = pack_region_boundaries(lengths, cap);
        let mut cursor = 0_usize;
        for &(start, end) in &ranges {
            assert_eq!(start, cursor, "ranges must be contiguous: {ranges:?}");
            assert!(start < end, "no empty ranges: {ranges:?}");
            cursor = end;
        }
        assert_eq!(
            cursor,
            lengths.len(),
            "the cover must end at the last region: {ranges:?}"
        );
        for &(start, end) in &ranges {
            let sum: u64 = lengths[start..end].iter().sum();
            if end - start > 1 {
                assert!(
                    sum <= cap,
                    "a multi-region chunk must fit the cap ({sum} > {cap}): {ranges:?}"
                );
            }
        }
        ranges
    }

    // Empty input: no chunks, no panic.
    assert_eq!(
        pack_region_boundaries(&[], 64 * 1024),
        Vec::<(usize, usize)>::new()
    );

    // Single region: exactly one chunk however small or oversized.
    assert_eq!(pack_region_boundaries(&[7], 10), vec![(0, 1)]);
    assert_eq!(pack_region_boundaries(&[100], 10), vec![(0, 1)]);

    // Multi-chunk packing: five regions of 5 chars under cap 12 pack
    // two-per-chunk (5+5=10 fits, +5 overflows), last chunk alone.
    let lengths = [5_u64, 5, 5, 5, 5];
    assert_eq!(assert_cover(&lengths, 12), vec![(0, 2), (2, 4), (4, 5)]);

    // Inclusive boundary: exactly `cap` characters still pack together
    // (the boundary is `> cap`, so 6+6=12 under cap 12 is ONE chunk).
    assert_eq!(assert_cover(&[6, 6], 12), vec![(0, 2)]);

    // Oversized region takes a chunk of its own and does not drag
    // neighbors into it: [5, 100, 5] under cap 12 isolates the 100.
    assert_eq!(assert_cover(&[5, 100, 5], 12), vec![(0, 1), (1, 2), (2, 3)]);
    // ...even in the middle of the run: [100, 5, 5] leaves the two
    // small regions packed together after it.
    assert_eq!(assert_cover(&[100, 5, 5], 12), vec![(0, 1), (1, 3)]);

    // Zero-length regions cost only their separator and pack freely.
    assert_eq!(assert_cover(&[0, 0, 0], 1), vec![(0, 3)]);

    // Saturation: near-u64::MAX contributions never overflow — the
    // oversized-isolation rule keeps saturating_add inaudible.
    assert_cover(&[u64::MAX, u64::MAX, 3], 10);

    // Realistic shape: 200 regions of 1000 chars under the production
    // 64-KiB cap → 65 regions per chunk (65_000 ≤ 65_536; 66_000
    // overflows), 4 chunks with a 5-region tail.
    let realistic: Vec<u64> = std::iter::repeat_n(1000, 200).collect();
    let ranges = assert_cover(&realistic, SESSION_DOC_CHUNK_CHAR_CAP as u64);
    assert_eq!(ranges.len(), 4, "65+65+65+5 regions: {ranges:?}");
    assert_eq!(ranges[0], (0, 65));
    assert_eq!(ranges[3], (195, 200));
}

#[test]
fn live_region_doc_bound_matches_correlated_oracle_and_hand_count() -> Result<()> {
    let (_temp, storage) = fixture();
    let db = storage.raw();
    let agent = agent_id(&storage);
    // Conversation 5 is deliberately never inserted (it was deleted).
    for id in [1, 2, 3, 4, 6, 7] {
        insert_conversation(db, id, agent);
    }

    // conv 1: two deletes; 3 live doc regions (incl. one exactly at the
    // base); a NULL-text synthetic row, and canonical rows below the
    // base whose text must not count. Hand count: 3 + 2 = 5.
    insert_marks(db, 1, r#"["a","b"]"#);
    insert_region(db, 1, 0, base(0), Some("x"));
    insert_region(db, 1, 1, base(1), Some("y"));
    insert_region(db, 1, 2, base(2), None);
    insert_region(db, 1, 3, 7, None);
    insert_region(db, 1, 4, 8, Some("z"));
    insert_region(db, 1, 5, base(3), Some("b"));

    // conv 2: marks row without any region rows. Hand count: 0.
    insert_marks(db, 2, "[]");

    // conv 3: canonical-only regions (below the base). Hand count: 1
    // (the single pending delete).
    insert_marks(db, 3, r#"["x"]"#);
    insert_region(db, 3, 0, 3, None);
    insert_region(db, 3, 1, 4, Some("c"));

    // conv 4: regions with no marks row are never counted. Hand count: 0.
    insert_region(db, 4, 0, base(0), Some("w"));
    insert_region(db, 4, 1, base(1), Some("v"));

    // conv 5: marks and regions of a deleted conversation (orphans; the
    // conversation-exists filter must drop the whole group). Hand count: 0.
    // Cascading foreign keys disallow seeding an orphan directly, so relax
    // enforcement strictly around the two orphan inserts and restore ON
    // before running the queries.
    db.execute("PRAGMA foreign_keys = OFF")?;
    insert_marks(db, 5, r#"["d1","d2","d3"]"#);
    insert_region(db, 5, 0, base(0), Some("o"));
    db.execute("PRAGMA foreign_keys = ON")?;

    // conv 6: a non-array JSON deletes payload contributes zero deletes,
    // but its live doc region still counts. Hand count: 1.
    insert_marks(db, 6, r#"{"not":"an array"}"#);
    insert_region(db, 6, 0, base(0), Some("p"));
    insert_region(db, 6, 1, base(1), None);

    // conv 7: unparseable deletes JSON contributes zero. Hand count: 0.
    insert_marks(db, 7, "not json");

    // Session×scope share (Stage 1): every marked conversation with
    // regions packs its 'block' rows into ONE chunk (all fixture texts
    // are far under the 65,536-char cap) — conv 1 (6 regions), conv 3
    // (2 regions) and conv 6 (2 regions) add one chunk each; conv 2 and
    // conv 7 own no regions and conv 4/conv 5 are excluded exactly as
    // their region share is. Hand count: 7 + 3 = 10.
    let oracle = oracle_bound(db)?;
    let set_based = live_region_doc_bound(db)?;
    assert_eq!(oracle, 10, "oracle drifted from the hand count");
    assert_eq!(set_based, oracle, "set-based bound must equal the oracle");
    assert_eq!(set_based, 10);
    Ok(())
}

#[test]
fn live_region_doc_bound_empty_tables_are_zero() -> Result<()> {
    let (_temp, storage) = fixture();
    assert_eq!(live_region_doc_bound(storage.raw())?, 0);
    assert_eq!(oracle_bound(storage.raw())?, 0);
    Ok(())
}

#[test]
fn live_region_doc_bound_marks_without_regions_and_regions_without_marks() -> Result<()> {
    let (_temp, storage) = fixture();
    let db = storage.raw();
    let agent = agent_id(&storage);
    insert_conversation(db, 1, agent);
    insert_conversation(db, 2, agent);

    // Marks alone: only the deletes count.
    insert_marks(db, 1, r#"["a","b"]"#);
    assert_eq!(live_region_doc_bound(db)?, 2);
    assert_eq!(oracle_bound(db)?, 2);

    // Regions alone (no marks row for conversation 2): nothing counts.
    insert_region(db, 2, 0, base(0), Some("q"));
    assert_eq!(live_region_doc_bound(db)?, 2);
    assert_eq!(oracle_bound(db)?, 2);

    // Only the marks row's own conversation contributes live regions.
    insert_marks(db, 2, "[]");
    // conv 2 now owns its region doc AND one session×scope chunk.
    assert_eq!(live_region_doc_bound(db)?, 4);
    assert_eq!(oracle_bound(db)?, 4);
    Ok(())
}

#[test]
fn live_region_doc_bound_without_marks_table_is_zero() -> Result<()> {
    // Canonical-only archive: no extension profile, so no marks table.
    let temp = tempfile::tempdir().expect("tempdir");
    let storage =
        FrankenStorage::open(&temp.path().join("canonical.db")).expect("canonical schema");
    assert_eq!(live_region_doc_bound(storage.raw())?, 0);
    Ok(())
}

fn fixture_doc_identity(conversation_id: i64, slot: i64) -> String {
    let source_path = format!("/fixture/{conversation_id}.jsonl");
    frankensearch::quill::cass::cass_document_identity(
        "local",
        frankensearch::quill::cass::CassConversationKey {
            source_path: &source_path,
            id: Some(conversation_id),
        },
        slot as u64,
    )
}

fn pending_deletes(db: &impl FactsDb, conversation_id: i64) -> Result<Vec<String>> {
    let raw: String = db
        .facts_query_map(
            "SELECT pending_doc_deletes FROM viewer_search_region_marks \
             WHERE conversation_id = ?1",
            &[ParamValue::from(conversation_id)],
            |row| row.get_typed::<String>(0),
        )?
        .into_iter()
        .next()
        .expect("marks row");
    Ok(serde_json::from_str(&raw)?)
}

/// A slot freed by an earlier republish and queued for deletion can be
/// reclaimed by the new meta inventory before the index lane runs. The
/// queued delete then targets a live document: the post-commit stamp
/// pass removes it after the commit already counted it, and a fresh
/// reader serves fewer documents than the rebuild published (GH #457).
#[test]
fn replace_meta_regions_drops_pending_deletes_its_new_slots_reclaim() -> Result<()> {
    let (_temp, storage) = fixture();
    let db = storage.raw();
    let agent = agent_id(&storage);
    insert_conversation(db, 1, agent);
    let reclaimed = fixture_doc_identity(1, base(0));
    let still_stale = fixture_doc_identity(1, base(5));
    insert_marks(
        db,
        1,
        &serde_json::to_string(&[reclaimed.as_str(), still_stale.as_str()])?,
    );
    // One tag yields exactly one meta region at ordinal 0: slot base(0).
    db.facts_execute(
        "INSERT INTO viewer_tags (conversation_id, tag) VALUES (1, 'release')",
        &[],
    )?;

    replace_meta_regions(db, 1)?;

    let docs = region_docs(db, 1)?;
    assert_eq!(docs.len(), 1, "the tag must materialize one meta doc");
    assert_eq!(docs[0].msg_idx, base(0) as u64);
    let pending = pending_deletes(db, 1)?;
    assert!(
        !pending.contains(&reclaimed),
        "a slot the new meta inventory owns must leave the delete queue: {pending:?}"
    );
    assert_eq!(
        pending,
        vec![still_stale],
        "unrelated stale ids stay queued"
    );
    Ok(())
}

/// A resumed terminal rebuild re-converges every marked session onto a
/// candidate whose commit already holds their region documents, so each
/// upsert replaces live documents and publishes a successor. One upsert
/// per session published one segment per session and validated every
/// segment each time: quadratic, hours on a real archive. The converge
/// must batch sessions, and the replacement must still leave exactly one
/// live document per identity.
#[test]
fn converge_all_onto_a_committed_candidate_publishes_per_batch_not_per_session() -> Result<()> {
    let (temp, storage) = fixture();
    let db = storage.raw();
    let agent = agent_id(&storage);
    let sessions = 6_i64;
    for id in 1..=sessions {
        insert_conversation(db, id, agent);
        insert_marks(db, id, "[]");
        insert_region(db, id, 0, base(0), Some("first region"));
        insert_region(db, id, 1, base(1), Some("second region"));
    }
    let index_path = temp.path().join("index");
    let mut index = crate::search::tantivy::TantivyIndex::open_or_create(&index_path)?;
    // Stage 1: each conversation now ships 2 region documents plus 1
    // session×scope chunk (its two 'block' regions pack into one
    // 28-char chunk) — 3 documents per session.
    assert_eq!(
        index.converge_all_search_region_docs(db, i64::MAX)?,
        (sessions as usize, 3 * sessions as usize)
    );
    index.commit()?;
    drop(index);

    // The resumed run opens a new writer on the committed candidate.
    let mut resumed = crate::search::tantivy::TantivyIndex::open_or_create(&index_path)?;
    let segments_before = resumed.segment_count();
    assert_eq!(
        resumed.converge_all_search_region_docs(db, i64::MAX)?,
        (sessions as usize, 3 * sessions as usize)
    );
    resumed.commit()?;
    let published = resumed.segment_count().saturating_sub(segments_before);
    assert!(
        published <= 1,
        "re-converging {sessions} sessions published {published} segments; \
         one batch must publish at most one"
    );
    assert_eq!(
        resumed.doc_count()?,
        3 * sessions as u64,
        "replacement must leave one live document per identity"
    );
    Ok(())
}

/// A REGION_VERSION bump re-ingests every session of the archive: the
/// generation republish rewrites each marks row `pending` while its
/// documents stay live in the committed snapshot, and the incremental
/// lane's pending sweep re-stages the whole set. One upsert per session
/// published a successor and validated every segment per session — the
/// same quadratic pass the twin test above shed for
/// [`TantivyIndex::converge_all_search_region_docs`], reached from the
/// incremental lane. The pending sweep must batch sessions the same
/// way, and the replacement must still leave exactly one live document
/// per identity.
///
/// [`TantivyIndex::converge_all_search_region_docs`]:
///     crate::search::tantivy::TantivyIndex::converge_all_search_region_docs
#[test]
fn converge_pending_onto_a_committed_candidate_publishes_per_batch_not_per_session() -> Result<()> {
    let (temp, storage) = fixture();
    let db = storage.raw();
    let agent = agent_id(&storage);
    let sessions = 6_i64;
    for id in 1..=sessions {
        insert_conversation(db, id, agent);
        insert_marks(db, id, "[]");
        insert_region(db, id, 0, base(0), Some("first region"));
        insert_region(db, id, 1, base(1), Some("second region"));
    }
    let index_path = temp.path().join("index");
    let mut index = crate::search::tantivy::TantivyIndex::open_or_create(&index_path)?;
    // Stage 1: each conversation ships 2 region documents plus 1
    // session×scope chunk (its two 'block' regions pack into one
    // 28-char chunk) — 3 documents per session — and the stamped
    // commit flips every marks row `published`.
    assert_eq!(
        index.converge_pending_search_region_docs(db, i64::MAX)?,
        sessions as usize
    );
    assert_eq!(
        index.pending_region_doc_stamp_count(),
        sessions as usize,
        "every converged session queues exactly one stamp"
    );
    index.commit_with_region_doc_stamps(db)?;
    let states = db.facts_query_map(
        "SELECT docs_state FROM viewer_search_region_marks ORDER BY conversation_id",
        &[],
        |row| row.get_typed::<String>(0),
    )?;
    assert!(
        states.iter().all(|state| state == "published"),
        "the stamped commit publishes every converged session: {states:?}"
    );
    drop(index);

    // The version-bump republish: every marks row flips `pending`
    // again while the same identities stay live in the committed
    // snapshot.
    db.facts_execute(
        "UPDATE viewer_search_region_marks SET docs_state = 'pending'",
        &[],
    )?;

    // The resumed incremental run opens a new writer on the committed
    // candidate.
    let mut resumed = crate::search::tantivy::TantivyIndex::open_or_create(&index_path)?;
    let segments_before = resumed.segment_count();
    assert_eq!(
        resumed.converge_pending_search_region_docs(db, i64::MAX)?,
        sessions as usize
    );
    resumed.commit_with_region_doc_stamps(db)?;
    let published = resumed.segment_count().saturating_sub(segments_before);
    assert!(
        published <= 1,
        "re-converging {sessions} pending sessions published {published} segments; \
         one batch must publish at most one"
    );
    assert_eq!(
        resumed.doc_count()?,
        3 * sessions as u64,
        "replacement must leave one live document per identity"
    );
    Ok(())
}

/// The sweep is idempotent within one writer generation: a session it
/// already staged (stamp queued, marks still `pending` until the
/// post-commit stamp) is never re-upserted — a duplicate live id — so a
/// second pass over the same pending set converges the same sessions
/// without touching the writer.
#[test]
fn converge_pending_is_idempotent_within_one_writer_generation() -> Result<()> {
    let (temp, storage) = fixture();
    let db = storage.raw();
    let agent = agent_id(&storage);
    let sessions = 3_i64;
    for id in 1..=sessions {
        insert_conversation(db, id, agent);
        insert_marks(db, id, "[]");
        insert_region(db, id, 0, base(0), Some("first region"));
        insert_region(db, id, 1, base(1), Some("second region"));
    }
    let index_path = temp.path().join("index");
    let mut index = crate::search::tantivy::TantivyIndex::open_or_create(&index_path)?;
    assert_eq!(
        index.converge_pending_search_region_docs(db, i64::MAX)?,
        sessions as usize
    );
    let stamps = index.pending_region_doc_stamp_count();
    let segments = index.segment_count();
    let docs = index.doc_count()?;

    // Marks are still `pending` (nothing committed yet), so the second
    // sweep lists the same sessions — but every one is already staged
    // on this writer, so nothing is upserted or queued again.
    assert_eq!(
        index.converge_pending_search_region_docs(db, i64::MAX)?,
        sessions as usize,
        "marks stay pending until the commit stamps them"
    );
    assert_eq!(
        index.pending_region_doc_stamp_count(),
        stamps,
        "no session stages twice in one writer generation"
    );
    assert_eq!(
        index.segment_count(),
        segments,
        "no re-upsert means no publish between the sweeps"
    );
    assert_eq!(index.doc_count()?, docs, "no duplicate live identities");
    index.commit_with_region_doc_stamps(db)?;
    assert_eq!(index.pending_region_doc_stamp_count(), 0);
    Ok(())
}

/// A crash mid-sweep — the writer dropped before its commit — must
/// leave every swept session's marks honestly `pending`: stamps queue
/// only after their documents are upserted and the marks row itself
/// only flips at the post-commit stamp, so the next run re-converges
/// the same sessions and the recovered commit publishes them.
#[test]
fn converge_pending_crash_before_commit_leaves_honest_pending_marks() -> Result<()> {
    let (temp, storage) = fixture();
    let db = storage.raw();
    let agent = agent_id(&storage);
    let sessions = 3_i64;
    for id in 1..=sessions {
        insert_conversation(db, id, agent);
        insert_marks(db, id, "[]");
        insert_region(db, id, 0, base(0), Some("first region"));
        insert_region(db, id, 1, base(1), Some("second region"));
    }
    let index_path = temp.path().join("index");
    {
        let mut index = crate::search::tantivy::TantivyIndex::open_or_create(&index_path)?;
        assert_eq!(
            index.converge_pending_search_region_docs(db, i64::MAX)?,
            sessions as usize
        );
        // Crash: the writer drops WITHOUT commit — staged docs are lost.
    }
    let states = db.facts_query_map(
        "SELECT docs_state FROM viewer_search_region_marks ORDER BY conversation_id",
        &[],
        |row| row.get_typed::<String>(0),
    )?;
    assert!(
        states.iter().all(|state| state == "pending"),
        "a crashed sweep must leave every mark pending: {states:?}"
    );

    // Recovery: a fresh writer re-converges every session and the
    // stamped commit publishes them.
    let mut index = crate::search::tantivy::TantivyIndex::open_or_create(&index_path)?;
    assert_eq!(
        index.converge_pending_search_region_docs(db, i64::MAX)?,
        sessions as usize
    );
    index.commit_with_region_doc_stamps(db)?;
    let states = db.facts_query_map(
        "SELECT docs_state FROM viewer_search_region_marks ORDER BY conversation_id",
        &[],
        |row| row.get_typed::<String>(0),
    )?;
    assert!(
        states.iter().all(|state| state == "published"),
        "the recovered commit publishes the re-converged sessions: {states:?}"
    );
    assert_eq!(
        index.doc_count()?,
        3 * sessions as u64,
        "recovery converges to one live document per identity"
    );
    Ok(())
}

/// A packet can name a session by another path than the conversation
/// row it merged into (a Cursor composer re-emitted under a second
/// path). Region documents are minted from the row, so the delete queue
/// must be too: ids built from the packet path never match the live
/// documents, a reclaimed slot survives the keep filter, and the
/// post-commit delete removes a live document (GH #457).
#[test]
fn publish_search_regions_queues_row_identities_when_packet_path_differs() -> Result<()> {
    let (_temp, storage) = fixture();
    let db = storage.raw();
    let agent = agent_id(&storage);
    insert_conversation(db, 1, agent);
    // An earlier republish freed slot base(0) and queued it; base(3) is
    // still owned by the previous inventory and is about to be dropped.
    let reclaimed = fixture_doc_identity(1, base(0));
    let still_stale = fixture_doc_identity(1, base(7));
    insert_marks(
        db,
        1,
        &serde_json::to_string(&[reclaimed.as_str(), still_stale.as_str()])?,
    );
    insert_region(db, 1, 3, base(3), Some("old meta"));
    // One tag and no messages: the new inventory is one meta region at
    // ordinal 0, which reclaims slot base(0).
    db.facts_execute(
        "INSERT INTO viewer_tags (conversation_id, tag) VALUES (1, 'release')",
        &[],
    )?;
    let conv = crate::model::types::Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: None,
        external_id: Some("composer-1".into()),
        title: None,
        source_path: "/elsewhere/composer-1.json".into(),
        started_at: None,
        ended_at: None,
        approx_tokens: None,
        metadata_json: Value::Null,
        source_id: "local".into(),
        origin_host: None,
        messages: Vec::new(),
    };
    let generation = crate::viewer_extensions::projection::generation_for_canonical(db, &conv, 1)?;
    assert_ne!(
        generation.packet.payload.identity.source_path, "/fixture/1.jsonl",
        "the fixture must exercise a packet path that differs from the row"
    );

    publish_search_regions(db, 1, &generation.packet, &generation.structure)?;

    let docs = region_docs(db, 1)?;
    assert_eq!(docs.len(), 1, "the tag must materialize one meta doc");
    assert_eq!(docs[0].msg_idx, base(0) as u64);
    let pending = pending_deletes(db, 1)?;
    assert_eq!(
        pending,
        vec![
            still_stale,
            fixture_doc_identity(1, base(3)),
            fixture_doc_identity(1, session_slot("block", 0)),
        ],
        "the reclaimed live slot leaves the queue, the dropped slot is \
         queued under the row identity its live document carries, and the \
         old inventory's one 'block' region conservatively bounds one old \
         session-doc chunk (unknown ordinal 6, chunk 0) that the new \
         chat-scope generation no longer keeps — it never held a live \
         document here, and the post-commit delete ignores absent \
         identities"
    );
    Ok(())
}
