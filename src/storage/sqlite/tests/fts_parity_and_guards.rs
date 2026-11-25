use super::*;
use crate::franken_sync::compat::{ConnectionExt as FrankenConnectionExt, RowExt as FrankenRowExt};
use std::collections::HashSet;
use std::fs;
use std::time::{Duration, Instant};
use tempfile::TempDir;

#[test]
fn fts_rebuild_page_planner_caps_bodies_before_materialization() {
    let headers = [(10, 4usize), (20, 6), (30, 1)];
    assert_eq!(
        plan_fts_rebuild_message_page(&headers, 10),
        Some(FtsRebuildPagePlan {
            last_rowid: 20,
            selected_rows: 2,
            body_bytes: 10,
        })
    );
    assert_eq!(plan_fts_rebuild_message_page(&[], 10), None);

    let oversized = plan_fts_rebuild_message_page(&[(40, 12), (50, 1)], 10)
        .expect("an oversized first row must still advance the cursor");
    assert_eq!(
        oversized,
        FtsRebuildPagePlan {
            last_rowid: 40,
            selected_rows: 1,
            body_bytes: 12,
        }
    );
}

#[test]
fn fts_rebuild_page_uses_coherent_snapshot_and_utf8_byte_lengths() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("fts-page-snapshot.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    seed_atomic_fts_rebuild_fixture(&storage);
    let unicode_body = "é🙂";
    storage
        .raw()
        .execute_compat(
            "UPDATE messages SET id = 100, content = ?1 WHERE idx = 0",
            fparams![unicode_body],
        )
        .unwrap();
    let conversation_id: i64 = storage
        .raw()
        .query_row_map(
            "SELECT conversation_id FROM messages WHERE id = 100",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    let sql_body_bytes: i64 = storage
        .raw()
        .query_row_map(
            "SELECT LENGTH(CAST(content AS BLOB)) FROM messages WHERE id = 100",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(
        usize::try_from(sql_body_bytes).unwrap(),
        unicode_body.len(),
        "SQL BLOB length must use the same UTF-8 byte metric as String::len"
    );

    let concurrent = FrankenStorage::open(&db_path).unwrap();
    arm_fts_page_after_plan_hook(move || {
        concurrent
            .raw()
            .execute("UPDATE messages SET content = 'this body grew after planning' WHERE id = 100")
            .expect("concurrent content growth must commit outside the read snapshot");
        concurrent
            .raw()
            .execute_compat(
                "INSERT INTO messages(id, conversation_id, idx, role, content)
                 VALUES(50, ?1, 1, 'assistant', 'inserted inside the planned rowid range')",
                fparams![conversation_id],
            )
            .expect("concurrent explicit-rowid insert must commit outside the read snapshot");
    });

    let page = storage
        .fetch_fts_rebuild_message_page_with_cap(0, 5_000, 8)
        .expect("fetch page from one coherent read snapshot");
    assert_eq!(page.planned_body_bytes, unicode_body.len());
    assert_eq!(page.rows.len(), 1);
    assert_eq!(page.rows[0].message_id, 100);
    assert_eq!(page.rows[0].content, unicode_body);
    assert!(
        page.planned_body_bytes <= 8,
        "a concurrent writer must not enlarge the materialized snapshot page"
    );

    let current_rows: i64 = storage
        .raw()
        .query_row_map("SELECT COUNT(*) FROM messages", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(
        current_rows, 2,
        "the writer did commit; its inserted row was merely excluded from the older page snapshot"
    );
}

#[test]
fn fts_batch_rowid_lookup_does_not_load_unrelated_sparse_range_rows() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("fts-sparse-rowid-lookup.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    seed_atomic_fts_rebuild_fixture(&storage);
    let first_id: i64 = storage
        .raw()
        .query_row_map(
            "SELECT id FROM fts_messages_docsize LIMIT 1",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    let unrelated_id = 500_000_i64;
    let sparse_requested_id = 1_000_000_i64;
    for id in [unrelated_id, sparse_requested_id] {
        storage
            .raw()
            .execute_compat(
                "INSERT INTO fts_messages_docsize(id, sz)
                 SELECT ?1, sz FROM fts_messages_docsize WHERE id = ?2",
                fparams![id, first_id],
            )
            .unwrap();
    }

    // Deliberately reverse numeric order. The unrelated row lies between
    // the requested IDs and would be loaded by the former MIN..MAX query.
    let rows = vec![
        FtsRebuildMessageRow {
            message_id: sparse_requested_id,
            conversation_id: 1,
            content: String::new(),
            created_at: None,
        },
        FtsRebuildMessageRow {
            message_id: first_id,
            conversation_id: 1,
            content: String::new(),
            created_at: None,
        },
    ];
    let found = storage
        .load_fts_message_rowid_set_for_batch(&rows)
        .expect("load exact sparse FTS rowids");
    let lookup_sql = format!(
        "EXPLAIN {}",
        fts_exact_rowid_probe_select_sql(&[sparse_requested_id, first_id])
    );
    let lookup_opcodes: Vec<(String, i64)> = storage
        .raw()
        .query_map_collect(&lookup_sql, fparams![], |row| {
            Ok((row.get_typed(1)?, row.get_typed(2)?))
        })
        .expect("explain the actual bounded IN-list FTS rowid probe");
    assert!(
        lookup_opcodes
            .iter()
            .any(|(opcode, cursor)| opcode == "SeekRowid" && *cursor == 0)
            && !lookup_opcodes
                .iter()
                .any(
                    |(opcode, cursor)| matches!(opcode.as_str(), "Rewind" | "Next") && *cursor == 0
                ),
        "the bounded IN-list probe must point-seek the large shadow, never scan it: {lookup_opcodes:?}"
    );

    assert_eq!(
        found,
        HashSet::from([first_id, sparse_requested_id]),
        "exact lookup must not materialize unrelated rowids inside the sparse numeric range"
    );
    assert!(!found.contains(&unrelated_id));
}

#[test]
fn fts_exact_rowid_probe_crosses_insert_chunks_and_seeks_the_shadow() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("fts-rowid-probe-plan.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    seed_atomic_fts_rebuild_fixture(&storage);
    let first_id: i64 = storage
        .raw()
        .query_row_map(
            "SELECT id FROM fts_messages_docsize LIMIT 1",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    let rows: Vec<FtsRebuildMessageRow> = (0..=FTS_EXACT_ROWID_PROBE_CHUNK_SIZE)
        .map(|offset| FtsRebuildMessageRow {
            message_id: first_id + i64::try_from(offset).unwrap(),
            conversation_id: 1,
            content: String::new(),
            created_at: None,
        })
        .collect();
    let found = storage
        .load_fts_message_rowid_set_for_batch(&rows)
        .expect("probe more IDs than one IN-list chunk");
    assert_eq!(found, HashSet::from([first_id]));

    let full_chunk_ids: Vec<i64> = (0..FTS_EXACT_ROWID_PROBE_CHUNK_SIZE)
        .map(|offset| first_id + i64::try_from(offset).unwrap())
        .collect();
    let explain_sql = format!(
        "EXPLAIN {}",
        fts_exact_rowid_probe_select_sql(&full_chunk_ids)
    );
    let opcodes: Vec<(String, i64)> = storage
        .raw()
        .query_map_collect(&explain_sql, fparams![], |row| {
            Ok((row.get_typed(1)?, row.get_typed(2)?))
        })
        .expect("explain full-chunk bounded IN-list exact-rowid probe");
    assert!(
        opcodes
            .iter()
            .filter(|(opcode, cursor)| opcode == "SeekRowid" && *cursor == 0)
            .count()
            >= 1,
        "a full chunk must compile to a direct SeekRowid on shadow cursor 0: {opcodes:?}"
    );
    assert!(
        !opcodes
            .iter()
            .any(|(opcode, cursor)| matches!(opcode.as_str(), "Rewind" | "Next") && *cursor == 0),
        "bounded exact-rowid probes must not scan the shadow cursor: {opcodes:?}"
    );
    let eqp_sql = format!(
        "EXPLAIN QUERY PLAN {}",
        fts_exact_rowid_probe_select_sql(&full_chunk_ids)
    );
    let eqp_details: Vec<String> = storage
        .raw()
        .query_map_collect(&eqp_sql, fparams![], |row| row.get_typed(3))
        .expect("explain query plan full-chunk bounded IN-list exact-rowid probe");
    assert!(
        eqp_details
            .iter()
            .any(|detail| detail.contains("SEARCH")),
        "bounded exact-rowid probe must use a SEARCH plan: {eqp_details:?}"
    );
}

#[test]
#[ignore = "explicit two-million-row FTS membership latency/RSS proof"]
fn fts_exact_rowid_probe_5k_is_bounded_against_two_million_unrelated_rows() {
    const UNRELATED_ROWS: i64 = 2_000_000;
    const REQUESTED_ROWS: usize = 5_000;
    const UNRELATED_BASE: i64 = 10_000_000;
    const REQUESTED_BASE: i64 = 20_000_000;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("fts-rowid-probe-perf.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    seed_atomic_fts_rebuild_fixture(&storage);

    // Grow the two synthetic ranges geometrically. FrankenSQLite caps a
    // recursive CTE at 1,000 iterations, so a recursive 2M-row fixture
    // silently measured the wrong corpus. This needs only O(log N) INSERT
    // statements and proves the actual requested cardinality below.
    let seed_geometric_range = |base: i64, count: i64, stride: i64| {
        storage
            .raw()
            .execute(&format!(
                "INSERT INTO fts_messages_docsize(id, sz)
                 SELECT {base}, sz FROM fts_messages_docsize ORDER BY id LIMIT 1"
            ))
            .expect("seed first synthetic FTS docsize row");
        let mut seeded = 1_i64;
        while seeded < count {
            let copied = seeded.min(count - seeded);
            storage
                .raw()
                .execute(&format!(
                    "INSERT INTO fts_messages_docsize(id, sz)
                         SELECT id + {}, sz
                         FROM fts_messages_docsize
                     WHERE id >= {base} AND id < {}",
                    seeded * stride,
                    base + copied * stride
                ))
                .expect("geometrically extend synthetic FTS docsize range");
            seeded += copied;
        }
        let actual: i64 = storage
            .raw()
            .query_row_map(
                &format!(
                    "SELECT COUNT(*) FROM fts_messages_docsize
                     WHERE id >= {base} AND id < {} AND (id - {base}) % {stride} = 0",
                    base + count * stride
                ),
                fparams![],
                |row| row.get_typed(0),
            )
            .expect("count synthetic FTS docsize range");
        assert_eq!(actual, count, "synthetic performance fixture cardinality");
    };
    seed_geometric_range(UNRELATED_BASE, UNRELATED_ROWS, 1);
    seed_geometric_range(
        REQUESTED_BASE,
        i64::try_from(REQUESTED_ROWS / 2).unwrap(),
        2,
    );

    let rows: Vec<FtsRebuildMessageRow> = (0..REQUESTED_ROWS)
        .map(|offset| FtsRebuildMessageRow {
            message_id: REQUESTED_BASE + i64::try_from(offset).unwrap(),
            conversation_id: 1,
            content: String::new(),
            created_at: None,
        })
        .collect();
    let rss_before = crate::indexer::responsiveness::process_resident_memory_bytes();
    let started = Instant::now();
    let found = storage
        .load_fts_message_rowid_set_for_batch(&rows)
        .expect("probe realistic 5k FTS repair page");
    let elapsed = started.elapsed();
    let rss_after = crate::indexer::responsiveness::process_resident_memory_bytes();

    assert_eq!(found.len(), REQUESTED_ROWS / 2);
    assert!(
        elapsed < Duration::from_secs(15),
        "5k exact-ID page took {elapsed:?} against {UNRELATED_ROWS} unrelated rows"
    );
    if let (Some(before), Some(after)) = (rss_before, rss_after) {
        let growth = after.saturating_sub(before);
        assert!(
            growth <= 32 * 1024 * 1024,
            "5k exact-ID page grew RSS by {growth} bytes against {UNRELATED_ROWS} unrelated rows"
        );
    }
    eprintln!(
        "FTS_EXACT_ROWID_PROBE_PERF unrelated_rows={UNRELATED_ROWS} requested_rows={REQUESTED_ROWS} matched_rows={} elapsed_ms={} rss_before={rss_before:?} rss_after={rss_after:?}",
        found.len(),
        elapsed.as_millis()
    );
}

#[test]
fn fts_shadow_parity_classifies_partial_shadow_exactly_without_marker_heuristics() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("fts-partial-parity.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    seed_atomic_fts_rebuild_fixture(&storage);
    let conversation_id: i64 = storage
        .raw()
        .query_row_map("SELECT id FROM conversations LIMIT 1", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    storage
        .raw()
        .execute_compat(
            "INSERT INTO messages(conversation_id, idx, role, content) VALUES(?1, 1, 'assistant', 'missing from the interrupted shadow')",
            fparams![conversation_id],
        )
        .unwrap();
    storage
        .raw()
        .execute_compat(
            "DELETE FROM meta WHERE key = ?1",
            fparams![FTS_FRANKEN_REBUILD_META_KEY],
        )
        .unwrap();

    let parity = storage.inspect_search_fallback_fts_parity().unwrap();
    assert_eq!(parity.status, FtsShadowParityStatus::Partial);
    assert_eq!(parity.canonical_messages, 2);
    assert_eq!(parity.indexable_messages, 2);
    assert_eq!(parity.indexed_messages, Some(1));
    assert_eq!(
        storage.count_fts_indexable_messages().unwrap(),
        reference_indexable_message_count(&storage),
        "partial-shadow classification must use an exact indexable-message count"
    );
}

#[test]
fn fts_shadow_parity_rejects_equal_counts_with_offsetting_rowids() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("fts-divergent-rowids.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    seed_atomic_fts_rebuild_fixture(&storage);
    let conversation_id: i64 = storage
        .raw()
        .query_row_map("SELECT id FROM conversations LIMIT 1", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    storage
        .raw()
        .execute_compat(
            "INSERT INTO messages(conversation_id, idx, role, content) VALUES(?1, 1, 'assistant', 'second canonical row')",
            fparams![conversation_id],
        )
        .unwrap();
    storage.rebuild_fts().unwrap();
    let message_ids: Vec<i64> = storage
        .raw()
        .query_map_collect("SELECT id FROM messages ORDER BY id", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(message_ids.len(), 2);
    storage
        .raw()
        .execute_compat(
            "DELETE FROM fts_messages_docsize WHERE id = ?1",
            fparams![message_ids[0]],
        )
        .unwrap();
    storage
        .raw()
        .execute_compat(
            "INSERT INTO fts_messages_docsize(id, sz)
             SELECT ?1, sz FROM fts_messages_docsize WHERE id = ?2",
            fparams![9_999_999_i64, message_ids[1]],
        )
        .unwrap();

    let parity = storage.inspect_search_fallback_fts_parity().unwrap();
    assert_eq!(parity.canonical_messages, 2);
    assert_eq!(parity.indexable_messages, 2);
    assert_eq!(parity.indexed_messages, Some(2));
    assert_eq!(parity.status, FtsShadowParityStatus::Divergent);
    assert_eq!(
        storage
            .inspect_fts_rowid_set_difference_with_page_rows(1)
            .expect("compare divergent FTS IDs across one-row pages"),
        (true, true),
        "offsetting rowids must report one missing and one excess ID even at every page boundary"
    );
    assert!(
        parity
            .detail
            .as_deref()
            .is_some_and(|detail| detail.contains("equal counts conceal rowid divergence"))
    );
}

#[test]
fn gh345_fts_dry_run_caps_rowid_work_without_claiming_late_divergence_is_healthy() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("fts-capped-dry-run.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    seed_atomic_fts_rebuild_fixture(&storage);

    let small = storage
        .inspect_search_fallback_fts_parity_dry_run(1)
        .expect("inspect a one-row shadow exactly at the dry-run cap");
    assert!(small.inspection_complete);
    assert_eq!(small.exact_status, Some(FtsShadowParityStatus::Healthy));

    let conversation_id: i64 = storage
        .raw()
        .query_row_map("SELECT id FROM conversations LIMIT 1", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    for idx in 1_i64..6 {
        storage
            .raw()
            .execute_compat(
                "INSERT INTO messages(conversation_id, idx, role, content)
                 VALUES(?1, ?2, 'assistant', ?3)",
                fparams![conversation_id, idx, format!("late divergence row {idx}")],
            )
            .unwrap();
    }
    storage.rebuild_fts().unwrap();
    let last_message_id: i64 = storage
        .raw()
        .query_row_map("SELECT MAX(id) FROM messages", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    storage
        .raw()
        .execute_compat(
            "DELETE FROM fts_messages_docsize WHERE id = ?1",
            fparams![last_message_id],
        )
        .unwrap();
    storage
        .raw()
        .execute_compat(
            "INSERT INTO fts_messages_docsize(id, sz)
             SELECT ?1, sz FROM fts_messages_docsize ORDER BY id LIMIT 1",
            fparams![9_999_999_i64],
        )
        .unwrap();

    let capped = storage
        .inspect_search_fallback_fts_parity_dry_run(2)
        .expect("bounded dry-run must stop before the planted late divergence");
    assert!(!capped.inspection_complete);
    assert_eq!(capped.exact_status, None);
    assert_eq!(capped.status_as_str(), "indeterminate");
    assert_eq!(capped.canonical_ids_examined, 2);
    assert_eq!(capped.indexed_ids_examined, 2);
    assert_eq!(capped.observed_missing_canonical_rowids_at_least, 0);
    assert_eq!(capped.observed_excess_fts_rowids_at_least, 0);

    let exact = storage
        .inspect_search_fallback_fts_parity()
        .expect("the mutating path's full preflight must find late divergence");
    assert_eq!(exact.status, FtsShadowParityStatus::Divergent);

    let completed = storage
        .inspect_search_fallback_fts_parity_dry_run(6)
        .expect("a cap covering both domains should classify them exactly");
    assert!(completed.inspection_complete);
    assert_eq!(
        completed.exact_status,
        Some(FtsShadowParityStatus::Divergent)
    );
    assert_eq!(completed.observed_missing_canonical_rowids_at_least, 1);
    assert_eq!(completed.observed_excess_fts_rowids_at_least, 1);
}

/// #345 plan of record: on a fixture whose row-ID domains EXCEED the cap,
/// a capped dry-run must stop at the cap and report the divergence it saw
/// as a ">= N divergent" floor — never an exact count, never "healthy".
#[test]
fn gh345_capped_dry_run_reports_divergence_floor_on_fixture_exceeding_cap() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("fts-divergence-floor.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    seed_atomic_fts_rebuild_fixture(&storage);

    let conversation_id: i64 = storage
        .raw()
        .query_row_map("SELECT id FROM conversations LIMIT 1", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    for idx in 1_i64..7 {
        storage
            .raw()
            .execute_compat(
                "INSERT INTO messages(conversation_id, idx, role, content)
                 VALUES(?1, ?2, 'assistant', ?3)",
                fparams![conversation_id, idx, format!("early divergence row {idx}")],
            )
            .unwrap();
    }
    storage.rebuild_fts().unwrap();
    // Plant divergence EARLY (the smallest rowid) so a capped scan
    // observes it before stopping.
    let first_message_id: i64 = storage
        .raw()
        .query_row_map("SELECT MIN(id) FROM messages", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    storage
        .raw()
        .execute_compat(
            "DELETE FROM fts_messages_docsize WHERE id = ?1",
            fparams![first_message_id],
        )
        .unwrap();

    // 7 indexable messages / 6 indexed rows, cap 3: both domains exceed
    // the cap, so the inspection is indeterminate — but the missing first
    // rowid was inside the inspected window and must surface as a floor.
    let capped = storage
        .inspect_search_fallback_fts_parity_dry_run(3)
        .expect("capped dry-run on a fixture exceeding the cap");
    assert!(!capped.inspection_complete);
    assert_eq!(capped.exact_status, None);
    assert_eq!(capped.status_as_str(), "indeterminate");
    assert_eq!(capped.canonical_ids_examined, 3);
    assert_eq!(capped.indexed_ids_examined, 3);
    assert_eq!(capped.observed_missing_canonical_rowids_at_least, 1);
    assert_eq!(capped.observed_excess_fts_rowids_at_least, 0);
    assert_eq!(capped.divergent_rowids_at_least(), 1);
    let detail = capped.detail.as_deref().expect("capped dry-run detail");
    assert!(
        detail.contains(">= 1 divergent row ID(s)"),
        "capped detail must report the divergence floor, got: {detail}"
    );
    assert!(
        detail.contains("deferred to --yes"),
        "capped detail must defer exact parity to the apply path, got: {detail}"
    );
}

#[test]
fn fts_shadow_parity_intersection_matches_exact_merge_for_excess_and_divergence() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("fts-intersection-cardinality.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    seed_atomic_fts_rebuild_fixture(&storage);

    let canonical_id: i64 = storage
        .raw()
        .query_row_map("SELECT id FROM messages LIMIT 1", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    let first_excess_id = canonical_id.saturating_add(1_000_000);
    storage
        .raw()
        .execute_compat(
            "INSERT INTO fts_messages_docsize(id, sz)
             SELECT ?1, sz FROM fts_messages_docsize WHERE id = ?2",
            fparams![first_excess_id, canonical_id],
        )
        .unwrap();

    let excess = storage.inspect_search_fallback_fts_parity().unwrap();
    assert_eq!(excess.indexable_messages, 1);
    assert_eq!(excess.indexed_messages, Some(2));
    assert_eq!(excess.status, FtsShadowParityStatus::Excess);
    assert_eq!(
        storage
            .inspect_fts_rowid_set_difference_bounded()
            .expect("compare one-row canonical set with a strict FTS superset"),
        (false, true),
        "the exact merge oracle must agree that only an excess rowid exists"
    );

    let conversation_id: i64 = storage
        .raw()
        .query_row_map("SELECT id FROM conversations LIMIT 1", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    storage
        .raw()
        .execute_compat(
            "INSERT INTO messages(conversation_id, idx, role, content)
             VALUES(?1, 1, 'assistant', 'missing from divergent shadow')",
            fparams![conversation_id],
        )
        .unwrap();
    let second_excess_id = first_excess_id.saturating_add(1);
    storage
        .raw()
        .execute_compat(
            "INSERT INTO fts_messages_docsize(id, sz)
             SELECT ?1, sz FROM fts_messages_docsize WHERE id = ?2",
            fparams![second_excess_id, canonical_id],
        )
        .unwrap();

    let divergent = storage.inspect_search_fallback_fts_parity().unwrap();
    assert_eq!(divergent.indexable_messages, 2);
    assert_eq!(divergent.indexed_messages, Some(3));
    assert_eq!(divergent.status, FtsShadowParityStatus::Divergent);
    assert_eq!(
        storage.count_fts_indexable_messages().unwrap(),
        reference_indexable_message_count(&storage),
        "divergent-shadow classification must use an exact indexable-message count"
    );
    assert!(
        divergent.detail.as_deref().is_some_and(|detail| {
            detail.contains("2 excess non-canonical rowids")
                && detail.contains("1 canonical rowids are missing")
                && detail.contains("intersection=1")
        }),
        "divergent detail must expose exact intersection-derived counts: {divergent:?}"
    );
    assert_eq!(
        storage
            .inspect_fts_rowid_set_difference_bounded()
            .expect("compare missing-plus-excess rowid domains"),
        (true, true),
        "the exact merge oracle must agree that both missing and excess rowids exist"
    );
}

#[test]
fn conversation_tail_ended_at_candidate_uses_latest_known_end() {
    let mut later_conversation_end = frontier_test_conversation(&[(0, Some(100)), (1, Some(110))]);
    later_conversation_end.ended_at = Some(250);
    assert_eq!(
        conversation_tail_ended_at_candidate(&later_conversation_end),
        Some(250),
        "conversation-level ended_at can be later than the final message timestamp"
    );

    let mut later_message_end = frontier_test_conversation(&[(0, Some(100)), (1, Some(300))]);
    later_message_end.ended_at = Some(250);
    assert_eq!(
        conversation_tail_ended_at_candidate(&later_message_end),
        Some(300),
        "message timestamps can be later than a stale conversation-level ended_at"
    );

    let mut no_message_timestamps = frontier_test_conversation(&[(0, None), (1, None)]);
    no_message_timestamps.ended_at = Some(200);
    assert_eq!(
        conversation_tail_ended_at_candidate(&no_message_timestamps),
        Some(200)
    );
}

#[test]
fn ended_at_shortcut_splits_safe_append_tail() {
    let covered = frontier_test_conversation(&[(0, Some(100)), (1, Some(110)), (2, Some(120))]);
    assert!(
        collect_existing_conversation_tail_from_ended_at(&covered, 120).is_none(),
        "ended_at coverage alone does not prove all lower idx rows exist"
    );

    let append = frontier_test_conversation(&[(0, Some(100)), (1, Some(110)), (2, Some(130))]);
    assert!(
        collect_existing_conversation_tail_from_ended_at(&append, 120).is_none(),
        "mixed covered-prefix plus append-tail input needs lookup to fill possible gaps"
    );

    let pure_append = frontier_test_conversation(&[(2, Some(130)), (3, Some(140))]);
    let plan = collect_existing_conversation_tail_from_ended_at(&pure_append, 120)
        .expect("all-new timestamp tail can append without message lookup");
    assert_eq!(plan.messages.len(), 2);
    assert_eq!(
        plan.messages.iter().map(|msg| msg.idx).collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert_eq!(
        plan.new_chars,
        ("message-2".len() + "message-3".len()) as i64
    );

    let unsorted = frontier_test_conversation(&[(1, Some(110)), (0, Some(100))]);
    assert!(
        collect_existing_conversation_tail_from_ended_at(&unsorted, 120).is_none(),
        "out-of-order input must not use the append/no-op shortcut"
    );

    let missing_timestamp = frontier_test_conversation(&[(0, Some(100)), (1, None)]);
    assert!(
        collect_existing_conversation_tail_from_ended_at(&missing_timestamp, 120).is_none(),
        "missing timestamps require replay-aware lookup"
    );

    let covered_after_append =
        frontier_test_conversation(&[(0, Some(100)), (1, Some(130)), (2, Some(110))]);
    assert!(
        collect_existing_conversation_tail_from_ended_at(&covered_after_append, 120).is_none(),
        "covered messages after the append split mean the input is not a safe tail"
    );

    let duplicate_idx = frontier_test_conversation(&[(0, Some(100)), (0, Some(130))]);
    assert!(
        collect_existing_conversation_tail_from_ended_at(&duplicate_idx, 120).is_none(),
        "duplicate idx values can collide with archived rows and require robust lookup"
    );
}

#[test]
fn idx_tail_shortcut_handles_no_timestamp_legacy_sources() {
    let covered = frontier_test_conversation(&[(0, None), (1, None)]);
    assert!(
        collect_existing_conversation_noop_from_idx_tail(&covered, 1).is_none(),
        "idx tail coverage alone does not prove all lower rows exist"
    );

    let append = frontier_test_conversation(&[(0, None), (1, None), (2, None)]);
    assert!(
        collect_existing_conversation_noop_from_idx_tail(&append, 1).is_none(),
        "partial timestamp tail metadata is not trusted for appends"
    );

    let unsorted = frontier_test_conversation(&[(1, None), (0, None), (2, None)]);
    assert!(
        collect_existing_conversation_noop_from_idx_tail(&unsorted, 1).is_none(),
        "out-of-order legacy messages need the robust lookup"
    );

    let duplicate_tail = frontier_test_conversation(&[(0, None), (2, None), (2, None)]);
    assert!(
        collect_existing_conversation_noop_from_idx_tail(&duplicate_tail, 1).is_none(),
        "duplicate tail idx values can collide and require robust lookup"
    );

    let duplicate_covered = frontier_test_conversation(&[(0, None), (1, None), (1, None)]);
    assert!(
        collect_existing_conversation_noop_from_idx_tail(&duplicate_covered, 1).is_none(),
        "duplicate covered idx values still need collision-aware lookup"
    );
}

#[test]
fn conversation_ended_at_shortcut_handles_stale_partial_idx_tail() {
    let mut covered = frontier_test_conversation(&[(0, Some(100)), (1, Some(110)), (2, Some(120))]);
    covered.ended_at = Some(120);
    assert!(
        collect_existing_conversation_noop_from_conversation_ended_at(&covered, 120).is_none(),
        "conversation ended_at coverage alone does not prove all message rows exist"
    );

    let mut missing_timestamp = frontier_test_conversation(&[(0, None), (1, None), (2, None)]);
    missing_timestamp.ended_at = Some(120);
    assert!(
        collect_existing_conversation_noop_from_conversation_ended_at(&missing_timestamp, 120)
            .is_none(),
        "no-timestamp messages need replay-aware lookup even when conversation ended_at is unchanged"
    );

    let mut newer = frontier_test_conversation(&[(0, Some(100)), (1, Some(110)), (2, Some(121))]);
    newer.ended_at = Some(121);
    assert!(
        collect_existing_conversation_noop_from_conversation_ended_at(&newer, 120).is_none(),
        "newer conversations need robust append handling"
    );

    let mut unsorted = frontier_test_conversation(&[(1, Some(110)), (0, Some(100))]);
    unsorted.ended_at = Some(120);
    assert!(
        collect_existing_conversation_noop_from_conversation_ended_at(&unsorted, 120).is_none(),
        "out-of-order unchanged conversations still use the robust path"
    );

    let mut duplicate =
        frontier_test_conversation(&[(0, Some(100)), (1, Some(110)), (1, Some(111))]);
    duplicate.ended_at = Some(120);
    assert!(
        collect_existing_conversation_noop_from_conversation_ended_at(&duplicate, 120).is_none(),
        "duplicate covered idx values still need collision-aware lookup"
    );
}

#[test]
fn populated_fts_shadow_without_rowid_reload_errors_are_classified() {
    assert!(
        error_message_indicates_populated_fts_shadow_without_rowid_reload(
            "not implemented: reloading populated WITHOUT ROWID table `fts_messages_config` into MemDatabase is not yet supported",
        )
    );
    assert!(
        error_message_indicates_populated_fts_shadow_without_rowid_reload(
            "not implemented: loading populated WITHOUT ROWID table fts_messages_data is not yet supported",
        )
    );
    assert!(
        !error_message_indicates_populated_fts_shadow_without_rowid_reload(
            "not implemented: reloading populated WITHOUT ROWID table `user_table` into MemDatabase is not yet supported",
        )
    );
}

#[test]
fn doctor_mutation_open_guard_only_targets_canonical_archive_db() {
    let dir = TempDir::new().unwrap();
    let canonical = dir.path().join("agent_search.db");
    let scratch = dir.path().join("scratch.db");

    assert_eq!(
        doctor_mutation_lock_path_for_db_open(&canonical),
        Some(dir.path().join("doctor/locks/doctor-repair.lock"))
    );
    assert_eq!(doctor_mutation_lock_path_for_db_open(&scratch), None);
}

#[test]
fn doctor_lock_metadata_pid_detection_is_exact() {
    let current = std::process::id();

    assert!(doctor_lock_metadata_pid_is_current_process(&format!(
        "schema_version=1\npid={current}\nmode=safe_auto_run\n"
    )));
    assert!(!doctor_lock_metadata_pid_is_current_process(
        "schema_version=1\npid=not-a-pid\n"
    ));
    assert!(!doctor_lock_metadata_pid_is_current_process(&format!(
        "pid={}\n",
        current.saturating_add(1)
    )));
}

#[test]
#[cfg(not(windows))]
fn doctor_storage_open_refuses_active_doctor_mutation_lock_from_other_process() {
    use std::io::Write as _;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("agent_search.db");
    {
        let storage = FrankenStorage::open(&db_path).unwrap();
        storage.close().unwrap();
    }

    let lock_path = doctor_mutation_lock_path_for_db_open(&db_path).unwrap();
    let mut lock_file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .unwrap();
    fs2::FileExt::try_lock_exclusive(&lock_file).unwrap();
    lock_file.set_len(0).unwrap();
    lock_file.write_all(b"schema_version=1\npid=1\n").unwrap();
    lock_file.sync_all().unwrap();

    let err =
        open_franken_raw_readonly_connection_with_timeout(&db_path, Duration::from_millis(25))
            .expect_err("active doctor mutation lock must block canonical DB opens");
    let message = err.to_string();
    assert!(
        message.contains("doctor mutation lock") && message.contains("active"),
        "error should identify the active doctor mutation lock: {message}"
    );

    let async_err =
        open_franken_async_readonly_connection_with_timeout(&db_path, Duration::from_millis(25))
            .expect_err("active doctor mutation lock must also block dedicated-owner opens");
    let async_message = async_err.to_string();
    assert!(
        async_message.contains("doctor mutation lock") && async_message.contains("active"),
        "dedicated-owner error should identify the active doctor mutation lock: {async_message}"
    );

    fs2::FileExt::unlock(&lock_file).unwrap();
}

#[test]
fn doctor_storage_open_allows_current_doctor_process_probe() {
    use std::io::Write as _;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("agent_search.db");
    {
        let storage = FrankenStorage::open(&db_path).unwrap();
        storage.close().unwrap();
    }

    let lock_path = doctor_mutation_lock_path_for_db_open(&db_path).unwrap();
    let mut lock_file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .unwrap();
    fs2::FileExt::try_lock_exclusive(&lock_file).unwrap();
    lock_file.set_len(0).unwrap();
    write!(lock_file, "schema_version=1\npid={}\n", std::process::id()).unwrap();
    lock_file.sync_all().unwrap();

    #[cfg(windows)]
    let _bypass = enter_doctor_mutation_db_open_bypass();

    let conn =
        open_franken_raw_readonly_connection_with_timeout(&db_path, Duration::from_millis(25))
            .expect(
                "doctor process must be able to run post-repair read probes under its own lock",
            );
    drop(conn);

    let mut async_conn =
        open_franken_async_readonly_connection_with_timeout(&db_path, Duration::from_millis(25))
            .expect("doctor process must also be able to open a dedicated-owner read probe");
    async_conn
        .close_without_checkpoint_sync()
        .expect("dedicated-owner doctor probe should close cleanly");

    fs2::FileExt::unlock(&lock_file).unwrap();
}

#[test]
fn autocommit_retain_disable_tries_compat_then_canonical_pragma() {
    let mut attempts = Vec::new();

    let selected = disable_autocommit_retain(|pragma| {
        attempts.push(pragma);
        if pragma == "PRAGMA fsqlite.autocommit_retain = OFF;" {
            Err("compat namespace unavailable")
        } else {
            Ok(())
        }
    })
    .expect("canonical pragma should disable autocommit retain");

    assert_eq!(selected, "PRAGMA autocommit_retain = OFF;");
    assert_eq!(attempts, AUTOCOMMIT_RETAIN_OFF_PRAGMAS);
}

#[test]
fn autocommit_retain_disable_fails_closed_when_no_pragma_works() {
    let mut attempts = Vec::new();

    let err = disable_autocommit_retain(|pragma| {
        attempts.push(pragma);
        Err("unsupported pragma")
    })
    .expect_err("unsupported autocommit retain controls should fail closed");

    assert_eq!(attempts, AUTOCOMMIT_RETAIN_OFF_PRAGMAS);
    let message = err.to_string();
    assert!(
        message.contains("refusing to keep a long-lived MVCC connection"),
        "error should force callers away from unbounded snapshot retention: {message}"
    );
    assert!(
        message.contains("PRAGMA fsqlite.autocommit_retain = OFF;")
            && message.contains("PRAGMA autocommit_retain = OFF;"),
        "error should preserve attempted PRAGMAs for diagnostics: {message}"
    );
}
