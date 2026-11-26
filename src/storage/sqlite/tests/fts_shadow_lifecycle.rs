use super::*;
use crate::franken_sync::SqliteValue;
use crate::franken_sync::compat::{ConnectionExt as FrankenConnectionExt, RowExt as FrankenRowExt};
use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use crate::sources::provenance::LOCAL_SOURCE_ID;
use serial_test::serial;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::TempDir;

#[test]
fn sqlite3_cli_launch_error_names_the_missing_tool() {
    let missing = sqlite3_cli_launch_error(
        std::io::Error::from(std::io::ErrorKind::NotFound),
        "recovering historical bundle /tmp/x",
    );
    let message = format!("{missing:#}");
    assert!(
        message.contains("needs the sqlite3 command-line tool")
            && message.contains("recovering historical bundle /tmp/x"),
        "{message}"
    );

    let denied = sqlite3_cli_launch_error(
        std::io::Error::from(std::io::ErrorKind::PermissionDenied),
        "importing recovered bundle /tmp/y",
    );
    let message = format!("{denied:#}");
    assert!(
        message.contains("launching sqlite3 for importing recovered bundle /tmp/y")
            && !message.contains("not installed"),
        "{message}"
    );
}

#[test]
#[serial]
fn storage_env_flags_are_truthy_only() {
    for value in ["1", "true", "TRUE", "yes", "YES", "on", "ON"] {
        let _guard = set_env_var("CASS_DEFER_LEXICAL_UPDATES", value);
        assert!(
            defer_storage_lexical_updates_enabled(),
            "{value:?} should enable the lexical defer toggle"
        );
    }

    for value in ["0", "false", "FALSE", "no", "NO", "", "maybe"] {
        let _guard = set_env_var("CASS_DEFER_LEXICAL_UPDATES", value);
        assert!(
            !defer_storage_lexical_updates_enabled(),
            "{value:?} should not enable the lexical defer toggle"
        );
    }
}

#[test]
fn analytics_defer_precedence_is_explicit() {
    assert!(!resolve_defer_analytics_updates(false, false, false));
    assert!(resolve_defer_analytics_updates(false, false, true));
    assert!(resolve_defer_analytics_updates(true, false, false));
    assert!(resolve_defer_analytics_updates(true, true, false));
    assert!(!resolve_defer_analytics_updates(false, true, true));
}

#[test]
#[serial]
fn legacy_omp_analytics_deferral_ignores_the_indexers_ambient_guard() {
    let _disabled = set_env_var("CASS_DEFER_ANALYTICS_UPDATES", "0");
    let ambient_guard = defer_analytics_updates_guard();
    assert!(
        !explicit_analytics_rebuild_deferred(),
        "the indexer's ambient ingest guard must not suppress its required post-publish OMP rebuild"
    );
    drop(ambient_guard);

    let _enabled = set_env_var("CASS_DEFER_ANALYTICS_UPDATES", "1");
    assert!(
        explicit_analytics_rebuild_deferred(),
        "the documented operator override must leave OMP analytics pending"
    );
}

#[test]
fn analytics_defer_guard_is_nesting_safe() {
    static TEST_DEPTH: AtomicUsize = AtomicUsize::new(0);

    assert_eq!(TEST_DEPTH.load(Ordering::SeqCst), 0);
    let outer = defer_analytics_updates_guard_for(&TEST_DEPTH);
    assert_eq!(TEST_DEPTH.load(Ordering::SeqCst), 1);
    {
        let _inner = defer_analytics_updates_guard_for(&TEST_DEPTH);
        assert_eq!(TEST_DEPTH.load(Ordering::SeqCst), 2);
    }
    assert_eq!(TEST_DEPTH.load(Ordering::SeqCst), 1);
    drop(outer);
    assert_eq!(TEST_DEPTH.load(Ordering::SeqCst), 0);
}

#[test]
#[serial]
fn primary_writer_discovers_virtual_fts_and_preserves_append_replay() {
    // The shadow writes asserted below are skipped while
    // CASS_DEFER_LEXICAL_UPDATES is truthy, and #[serial] tests set it to
    // "1" for their duration; pin it off and run serially with them.
    let _defer_guard = set_env_var("CASS_DEFER_LEXICAL_UPDATES", "0");
    let dir = TempDir::new().unwrap();
    let storage = FrankenStorage::open(&dir.path().join("primary-fts.db")).unwrap();
    storage.ensure_search_fallback_fts_consistency().unwrap();
    let rootpage: i64 = storage
        .raw()
        .query_row_map(
            "SELECT rootpage FROM sqlite_master WHERE name = 'fts_messages'",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(rootpage, 0, "virtual tables have no ordinary B-tree root");
    storage.invalidate_fts_messages_present_cache();
    let agent_id = storage
        .ensure_agent(&Agent {
            id: None,
            slug: "codex".into(),
            name: "Codex".into(),
            version: None,
            kind: AgentKind::Cli,
        })
        .unwrap();
    let mut conversation = frontier_test_conversation(&[(0, Some(1_700_000_000_000))]);
    conversation.messages[0].content = "primaryshadowfirst".into();
    storage
        .insert_conversation_tree_with_analytics(agent_id, None, &conversation, false)
        .unwrap();
    let first_ids = storage
        .raw()
        .query("SELECT id FROM messages ORDER BY id")
        .unwrap();
    assert_eq!(first_ids.len(), 1);
    let first_id: i64 = first_ids[0].get_typed(0).unwrap();
    let hits = storage
        .raw()
        .query("SELECT rowid FROM fts_messages WHERE fts_messages MATCH 'primaryshadowfirst'")
        .unwrap();
    assert_eq!(
        hits.len(),
        1,
        "the primary writer must populate the shadow without a rebuild"
    );
    assert_eq!(hits[0].get_typed::<i64>(0).unwrap(), first_id);

    let mut appended = conversation.messages[0].clone();
    appended.idx = 1;
    appended.created_at = Some(1_700_000_000_001);
    appended.content = "primaryshadowsecond".into();
    conversation.messages.push(appended);
    storage.invalidate_fts_messages_present_cache();
    storage
        .insert_conversation_tree_with_analytics(agent_id, None, &conversation, false)
        .unwrap();
    storage
        .insert_conversation_tree_with_analytics(agent_id, None, &conversation, false)
        .unwrap();
    let canonical = storage
        .raw()
        .query("SELECT id FROM messages ORDER BY id")
        .unwrap();
    assert_eq!(
        canonical.len(),
        2,
        "replay must not duplicate canonical messages"
    );
    assert_eq!(canonical[0].get_typed::<i64>(0).unwrap(), first_id);
    for (query, expected_id) in [
        ("primaryshadowfirst", first_id),
        (
            "primaryshadowsecond",
            canonical[1].get_typed::<i64>(0).unwrap(),
        ),
    ] {
        let rows = storage
            .raw()
            .query_with_params(
                "SELECT rowid FROM fts_messages WHERE fts_messages MATCH ?1",
                &[SqliteValue::from(query)],
            )
            .unwrap();
        assert_eq!(
            rows.len(),
            1,
            "each message must be searchable exactly once"
        );
        assert_eq!(rows[0].get_typed::<i64>(0).unwrap(), expected_id);
    }
}

#[test]
fn max_message_id_tracks_messages_table() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("max-message-id.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    assert_eq!(
        storage.max_message_id().unwrap(),
        None,
        "an empty archive must report no max message id"
    );

    seed_atomic_fts_rebuild_fixture(&storage);
    let max = storage
        .max_message_id()
        .unwrap()
        .expect("seeded archive must report a max message id");
    let expected: i64 = storage
        .raw()
        .query_row_map("SELECT MAX(id) FROM messages", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(max, expected);

    // The one-shot semantic skip (issue #394) compares this against the
    // embedding watermark; a watermark at max must read back intact.
    storage.set_last_embedded_message_id(max).unwrap();
    assert_eq!(storage.get_last_embedded_message_id().unwrap(), Some(max));
}

#[test]
fn fts_indexable_count_and_intersection_plans_are_bounded_and_exact() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("fts-indexable-count-plan.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    seed_atomic_fts_rebuild_fixture(&storage);

    let message_rootpage: i64 = storage
        .raw()
        .query_row_map(
            "SELECT rootpage FROM sqlite_master WHERE name = 'messages'",
            fparams![],
            |row| row.get_typed(0),
        )
        .expect("load messages table rootpage");
    let message_index_rootpage: i64 = storage
        .raw()
        .query_row_map(
            "SELECT rootpage FROM sqlite_master WHERE name = 'idx_messages_conv_idx'",
            fparams![],
            |row| row.get_typed(0),
        )
        .expect("load messages uniqueness-index rootpage");
    let conversation_rootpage: i64 = storage
        .raw()
        .query_row_map(
            "SELECT rootpage FROM sqlite_master WHERE name = 'conversations'",
            fparams![],
            |row| row.get_typed(0),
        )
        .expect("load conversations table rootpage");
    let docsize_rootpage: i64 = storage
        .raw()
        .query_row_map(
            "SELECT rootpage FROM sqlite_master WHERE name = 'fts_messages_docsize'",
            fparams![],
            |row| row.get_typed(0),
        )
        .expect("load FTS docsize rootpage");
    let count_opcodes: Vec<(String, i64)> = storage
        .raw()
        .query_map_collect(
            &format!("EXPLAIN {FTS_INDEXABLE_MESSAGE_COUNT_SQL}"),
            fparams![],
            |row| Ok((row.get_typed(1)?, row.get_typed(3)?)),
        )
        .expect("explain fused indexable-message parent-run count");
    assert!(
        count_opcodes.iter().any(|(opcode, _)| opcode == "SeekGE")
            && count_opcodes.iter().any(|(opcode, _)| opcode == "IdxGT"),
        "parity cardinality must range-scan the composite index: {count_opcodes:?}"
    );
    assert!(
        !count_opcodes
            .iter()
            .any(|(opcode, _)| matches!(opcode.as_str(), "OpenAutoindex" | "SorterOpen")),
        "parent-run cardinality must avoid autoindex/sorter materialization: {count_opcodes:?}"
    );
    let count_eqp: Vec<String> = storage
        .raw()
        .query_map_collect(
            &format!("EXPLAIN QUERY PLAN {FTS_INDEXABLE_MESSAGE_COUNT_SQL}"),
            fparams![],
            |row| row.get_typed(3),
        )
        .expect("explain query plan indexable-message parent-run count");
    assert!(
        count_eqp.iter().any(|detail| detail.contains("SEARCH"))
            && !count_eqp.iter().any(|detail| detail.contains("TEMP B-TREE")),
        "parity cardinality must use SEARCH without TEMP B-TREE: {count_eqp:?}"
    );
    let open_read_rootpages: Vec<i64> = count_opcodes
        .iter()
        .filter_map(|(opcode, rootpage)| (opcode == "OpenRead").then_some(*rootpage))
        .collect();
    assert!(
        open_read_rootpages.contains(&message_index_rootpage)
            && open_read_rootpages.contains(&conversation_rootpage)
            && !open_read_rootpages.contains(&message_rootpage)
            && open_read_rootpages.len() == 2,
        "parity cardinality must open only the composite index and parent table: roots={open_read_rootpages:?}, opcodes={count_opcodes:?}"
    );
    assert_eq!(
        storage.count_fts_indexable_messages().unwrap(),
        reference_indexable_message_count(&storage),
        "the forced access path must preserve the reference join's exact result"
    );

    assert_eq!(
        storage
            .count_fts_indexed_canonical_intersection()
            .expect("count the one-row healthy intersection"),
        1
    );
    let intersection_opcodes: Vec<(String, i64, i64)> = storage
        .raw()
        .query_map_collect(
            &format!("EXPLAIN {FTS_INDEXED_CANONICAL_INTERSECTION_SQL}"),
            fparams![],
            |row| Ok((row.get_typed(1)?, row.get_typed(2)?, row.get_typed(3)?)),
        )
        .expect("explain read-only shadow-driven canonical intersection");
    let cursor_for_rootpage = |rootpage: i64| {
        intersection_opcodes
            .iter()
            .find_map(|(opcode, p1, p2)| (opcode == "OpenRead" && *p2 == rootpage).then_some(*p1))
    };
    let docsize_cursor = cursor_for_rootpage(docsize_rootpage)
        .expect("intersection must open the FTS docsize table");
    let message_cursor =
        cursor_for_rootpage(message_rootpage).expect("intersection must open messages");
    assert!(
        intersection_opcodes
            .iter()
            .any(|(opcode, p1, _)| opcode == "Rewind" && *p1 == docsize_cursor)
            && intersection_opcodes
                .iter()
                .any(|(opcode, p1, _)| opcode == "SeekRowid" && *p1 == message_cursor)
            && !intersection_opcodes.iter().any(|(opcode, p1, _)| {
                matches!(opcode.as_str(), "Rewind" | "Next") && *p1 == message_cursor
            })
            && intersection_opcodes
                .iter()
                .any(|(opcode, _, _)| opcode == "ResultRow")
            && !intersection_opcodes
                .iter()
                .any(|(opcode, _, _)| matches!(opcode.as_str(), "SorterOpen" | "OpenEphemeral")),
        "intersection must scan only the derived shadow, point-seek canonical messages, and stream one compact parent ID without materialization: {intersection_opcodes:?}"
    );

    let conversation_id: i64 = storage
        .raw()
        .query_row_map("SELECT id FROM conversations LIMIT 1", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    for idx in 1_i64..5 {
        storage
            .raw()
            .execute_compat(
                "INSERT INTO messages(conversation_id, idx, role, content)
                     VALUES(?1, ?2, 'assistant', 'count-index equality-run regression')",
                fparams![conversation_id, idx],
            )
            .unwrap();
    }
    assert_eq!(
        storage.count_fts_indexable_messages().unwrap(),
        5,
        "a five-row equality run must count five rows, not the triangular sum 5+4+3+2+1"
    );
    assert_eq!(
        storage.count_fts_indexable_messages().unwrap(),
        reference_indexable_message_count(&storage),
        "fused equality-run counting must remain exact beyond a one-row proof"
    );

    storage.raw().execute("PRAGMA foreign_keys = OFF").unwrap();
    storage
        .raw()
        .execute(
            "INSERT INTO messages(id, conversation_id, idx, role, content)
                 VALUES(9999999, 9999999, 0, 'user', 'deliberate orphan')",
        )
        .unwrap();
    storage.raw().execute("PRAGMA foreign_keys = ON").unwrap();
    assert_eq!(
        storage
            .raw()
            .query_row_map("SELECT COUNT(*) FROM messages", fparams![], |row| {
                row.get_typed::<i64>(0)
            })
            .unwrap(),
        6,
        "the fixture must contain five canonical messages and one orphan"
    );
    assert_eq!(
        storage.count_fts_indexable_messages().unwrap(),
        reference_indexable_message_count(&storage),
        "prepared parent counts must exclude orphan IDs exactly"
    );
    let canonical_id: i64 = storage
        .raw()
        .query_row_map(
            "SELECT id FROM messages WHERE conversation_id != 9999999 LIMIT 1",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    storage
        .raw()
        .execute_compat(
            "INSERT INTO fts_messages_docsize(id, sz)
                 SELECT 9999999, sz FROM fts_messages_docsize WHERE id = ?1",
            fparams![canonical_id],
        )
        .unwrap();
    assert_eq!(
        storage.count_fts_indexed_canonical_intersection().unwrap(),
        1,
        "intersection must reject an indexed message whose parent conversation is absent"
    );
    drop(storage);
    let readonly = FrankenStorage::open_readonly(&db_path).unwrap();
    assert_eq!(
        readonly.count_fts_indexed_canonical_intersection().unwrap(),
        1,
        "exact intersection must require no TEMP or canonical writes on a read-only doctor connection"
    );
}

#[test]
fn transactional_fts_rebuild_interruptions_preserve_the_published_shadow() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("fts-atomic-interruption.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    seed_atomic_fts_rebuild_fixture(&storage);

    // Change canonical content without touching the already-published FTS
    // row so rollback can be distinguished from a leaked replacement.
    storage
        .raw()
        .execute("UPDATE messages SET content = 'replacement canonical content' WHERE idx = 0")
        .unwrap();
    let search_count = |term: &str| {
        storage
            .raw()
            .query_row_map(
                "SELECT COUNT(*) FROM fts_messages WHERE fts_messages MATCH ?1",
                fparams![term],
                |row| row.get_typed::<i64>(0),
            )
            .unwrap()
    };
    assert_eq!(search_count("survives"), 1);
    assert_eq!(search_count("replacement"), 0);

    for phase in [1, 2] {
        arm_fts_rebuild_interruption(phase);
        let error = storage
            .rebuild_fts()
            .expect_err("injected rebuild interruption must fail");
        let error_chain = format!("{error:#}");
        assert!(
            error_chain.contains("rolled back"),
            "rebuild failure must state rollback semantics: {error:#}"
        );
        assert!(
            error_chain.contains(&format!(
                "injected FTS rebuild interruption at test phase {phase}"
            )),
            "phase {phase} must reach the intended interruption point: {error:#}"
        );
        assert_eq!(
            search_count("survives"),
            1,
            "phase {phase} must preserve the previously published FTS content"
        );
        assert_eq!(
            search_count("replacement"),
            0,
            "phase {phase} must not leak the uncommitted replacement shadow"
        );
    }
}

/// GH #369 (bead cb0gl): a rebuild batch whose distinct terms and doclists
/// encode past one 64 KiB segment leaf failed with "segment leaf term
/// offset exceeds u16" and rolled back, leaving no fallback shadow. The
/// pinned engine now writes multi-leaf segments; the shadow must build and
/// answer terms from both ends of the batch.
#[test]
fn gh369_fts_rebuild_survives_a_batch_larger_than_one_segment_leaf() {
    let dir = TempDir::new().unwrap();
    let storage = FrankenStorage::open(&dir.path().join("fts-multileaf.db")).unwrap();
    let agent_id = storage
        .ensure_agent(&Agent {
            id: None,
            slug: "codex".into(),
            name: "Codex".into(),
            version: None,
            kind: AgentKind::Cli,
        })
        .unwrap();
    // 400 messages x 100 distinct terms: 40,000 terms, roughly 440 KB of
    // term text, several times one leaf even if the rebuild splits batches.
    let term = |message: usize, word: usize| format!("t{message:04}x{word:03}q");
    let messages = (0..400)
        .map(|message| Message {
            id: None,
            idx: message as i64,
            role: MessageRole::User,
            author: Some("user".into()),
            created_at: Some(1_700_000_000_000 + message as i64),
            content: (0..100)
                .map(|word| term(message, word))
                .collect::<Vec<_>>()
                .join(" "),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        })
        .collect();
    storage
        .insert_conversation_tree(
            agent_id,
            None,
            &Conversation {
                id: None,
                agent_slug: "codex".into(),
                workspace: Some(PathBuf::from("/tmp/fts-multileaf")),
                external_id: Some("fts-multileaf".into()),
                title: Some("FTS multi-leaf fixture".into()),
                source_path: PathBuf::from("/tmp/fts-multileaf.jsonl"),
                started_at: Some(1_700_000_000_000),
                ended_at: Some(1_700_000_000_400),
                approx_tokens: None,
                metadata_json: serde_json::Value::Null,
                messages,
                source_id: LOCAL_SOURCE_ID.into(),
                origin_host: None,
            },
        )
        .unwrap();

    storage
        .rebuild_fts()
        .expect("a batch past one segment leaf must rebuild");
    assert_eq!(
        storage.inspect_search_fallback_fts_parity().unwrap().status,
        FtsShadowParityStatus::Healthy
    );
    let matches = |needle: &str| {
        storage
            .raw()
            .query_row_map(
                "SELECT COUNT(*) FROM fts_messages WHERE fts_messages MATCH ?1",
                fparams![needle],
                |row| row.get_typed::<i64>(0),
            )
            .unwrap()
    };
    for needle in [term(0, 0), term(199, 50), term(399, 99)] {
        assert_eq!(matches(&needle), 1, "{needle}");
    }
    assert_eq!(matches("t9999x999q"), 0, "an absent term must not match");
}

#[test]
fn gh495_orphan_shadow_tables_are_residue_and_repair_converges() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("fts-orphan-shadow-residue.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    seed_atomic_fts_rebuild_fixture(&storage);
    storage.raw().execute("DROP TABLE fts_messages").unwrap();
    for ddl in [
        "CREATE TABLE fts_messages_config(k PRIMARY KEY, v) WITHOUT ROWID",
        "CREATE TABLE fts_messages_content(id INTEGER PRIMARY KEY, c0, c1, c2, c3, c4, c5, c6)",
        "CREATE TABLE fts_messages_data(id INTEGER PRIMARY KEY, block BLOB)",
        "CREATE TABLE fts_messages_docsize(id INTEGER PRIMARY KEY, sz BLOB)",
        "CREATE TABLE fts_messages_idx(segid, term, pgno, PRIMARY KEY(segid, term)) WITHOUT ROWID",
    ] {
        storage.raw().execute(ddl).unwrap();
    }

    let before = storage.inspect_search_fallback_fts_parity().unwrap();
    assert_eq!(before.status, FtsShadowParityStatus::Residue);
    assert!(before.detail.unwrap().contains("fts_messages_content"));
    let dry_run = storage
        .inspect_search_fallback_fts_parity_dry_run(10)
        .unwrap();
    assert_eq!(dry_run.exact_status, Some(FtsShadowParityStatus::Residue));

    assert!(matches!(
        storage.ensure_search_fallback_fts_consistency().unwrap(),
        FtsConsistencyRepair::Rebuilt { inserted_rows: 1 }
    ));
    assert_eq!(
        storage.inspect_search_fallback_fts_parity().unwrap().status,
        FtsShadowParityStatus::Healthy
    );
    assert!(matches!(
        storage.ensure_search_fallback_fts_consistency().unwrap(),
        FtsConsistencyRepair::AlreadyHealthy { rows: 1 }
    ));
}

#[test]
fn gh495_legacy_ddl_hidden_content_row_is_recreated_instead_of_caught_up() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("fts-legacy-ddl-residue.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    seed_atomic_fts_rebuild_fixture(&storage);
    storage.raw().execute("DROP TABLE fts_messages").unwrap();
    storage
        .raw()
        .execute(
            "CREATE VIRTUAL TABLE fts_messages USING fts5(
                    content, title, agent, workspace, source_path,
                    created_at UNINDEXED, message_id UNINDEXED, tokenize='porter'
                 )",
        )
        .unwrap();
    let message_id: i64 = storage
        .raw()
        .query_row_map("SELECT id FROM messages LIMIT 1", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    storage
        .raw()
        .execute_compat(
            "INSERT INTO fts_messages(
                    rowid, content, title, agent, workspace, source_path, created_at, message_id
                 ) VALUES(?1, 'stale hidden text', 'legacy', 'codex', '/tmp', '/tmp/legacy', 0, ?1)",
            fparams![message_id],
        )
        .unwrap();
    storage
        .raw()
        .execute_compat(
            "DELETE FROM fts_messages_docsize WHERE id = ?1",
            fparams![message_id],
        )
        .unwrap();

    assert_eq!(
        storage.inspect_search_fallback_fts_parity().unwrap().status,
        FtsShadowParityStatus::Residue,
        "legacy DDL must be classified before `_docsize` can misclassify it as Partial"
    );
    assert!(matches!(
        storage.ensure_search_fallback_fts_consistency().unwrap(),
        FtsConsistencyRepair::Rebuilt { inserted_rows: 1 }
    ));
    assert_eq!(
        storage.inspect_search_fallback_fts_parity().unwrap().status,
        FtsShadowParityStatus::Healthy
    );
    assert!(
        storage
            .fts_messages_schema_is_canonical_contentless()
            .unwrap()
    );
}

#[test]
fn gh495_legacy_contentless_ddl_without_delete_support_is_residue() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("fts-legacy-contentless-residue.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    seed_atomic_fts_rebuild_fixture(&storage);
    storage.raw().execute("DROP TABLE fts_messages").unwrap();
    storage
        .raw()
        .execute(
            "CREATE VIRTUAL TABLE fts_messages USING fts5(
                    content, title, agent, workspace, source_path,
                    created_at UNINDEXED, message_id UNINDEXED,
                    content='', tokenize='porter'
                 )",
        )
        .unwrap();
    let message_id: i64 = storage
        .raw()
        .query_row_map("SELECT id FROM messages LIMIT 1", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    storage
        .raw()
        .execute_compat(
            "INSERT INTO fts_messages(
                    rowid, content, title, agent, workspace, source_path, created_at, message_id
                 ) VALUES(?1, 'legacy contentless text', 'legacy', 'codex', '/tmp', '/tmp/legacy', 0, ?1)",
            fparams![message_id],
        )
        .unwrap();

    assert_eq!(
        storage.inspect_search_fallback_fts_parity().unwrap().status,
        FtsShadowParityStatus::Residue,
        "contentless DDL without contentless_delete=1 must not enter DELETE_ALL repair"
    );
    assert!(matches!(
        storage.ensure_search_fallback_fts_consistency().unwrap(),
        FtsConsistencyRepair::Rebuilt { inserted_rows: 1 }
    ));
    assert!(
        storage
            .fts_messages_schema_is_canonical_contentless()
            .unwrap()
    );
    assert!(matches!(
        storage.ensure_search_fallback_fts_consistency().unwrap(),
        FtsConsistencyRepair::AlreadyHealthy { rows: 1 }
    ));
}

/// GH #495 follow-up: residue classification must not strand the direct
/// `rebuild_fts()` callers (agent-exclusion purge, forget, dedup, reset).
/// Before the residue state existed, the legacy-DDL shape reached the
/// drop+recreate route; after it, the atomic rebuild bailed with "FTS
/// residue must be removed", so `sources agents exclude` exited 5 on every
/// legacy archive. Both residue shapes must now rebuild to one canonical,
/// healthy shadow without touching canonical rows. The orphan tables are
/// populated (the reporter's archive carried rows in its WITHOUT ROWID
/// `_config`/`_idx`), so a repair that drops only the `fts_messages`
/// registration fails here on "fts_messages_data already exists".
#[test]
fn gh495_direct_rebuild_callers_remove_residue_instead_of_failing() {
    type Shape = fn(&FrankenStorage);
    let orphan_populated: Shape = |storage| {
        storage.raw().execute("DROP TABLE fts_messages").unwrap();
        for sql in [
            "CREATE TABLE fts_messages_config(k PRIMARY KEY, v) WITHOUT ROWID",
            "INSERT INTO fts_messages_config VALUES('version', 4)",
            "CREATE TABLE fts_messages_content(id INTEGER PRIMARY KEY, c0, c1, c2, c3, c4, c5, c6)",
            "INSERT INTO fts_messages_content VALUES(99, 'orphan residue text', 't', 'codex', '/tmp', '/tmp/x', 0, 99)",
            "CREATE TABLE fts_messages_data(id INTEGER PRIMARY KEY, block BLOB)",
            "INSERT INTO fts_messages_data VALUES(1, x'00')",
            "CREATE TABLE fts_messages_docsize(id INTEGER PRIMARY KEY, sz BLOB)",
            "INSERT INTO fts_messages_docsize VALUES(99, x'01')",
            "CREATE TABLE fts_messages_idx(segid, term, pgno, PRIMARY KEY(segid, term)) WITHOUT ROWID",
            "INSERT INTO fts_messages_idx VALUES(1, 'orphan', 1)",
        ] {
            storage.raw().execute(sql).unwrap();
        }
    };
    let legacy_ddl: Shape = |storage| {
        storage.raw().execute("DROP TABLE fts_messages").unwrap();
        storage
            .raw()
            .execute(
                "CREATE VIRTUAL TABLE fts_messages USING fts5(
                        content, title, agent, workspace, source_path,
                        created_at UNINDEXED, message_id UNINDEXED, tokenize='porter'
                     )",
            )
            .unwrap();
    };
    for (name, shape) in [
        ("orphan-populated", orphan_populated),
        ("legacy-ddl", legacy_ddl),
    ] {
        let dir = TempDir::new().unwrap();
        let storage =
            FrankenStorage::open(&dir.path().join(format!("gh495-direct-{name}.db"))).unwrap();
        seed_atomic_fts_rebuild_fixture(&storage);
        shape(&storage);
        let canonical_rows = || {
            storage
                .raw()
                .query("SELECT id, conversation_id, idx, content FROM messages ORDER BY id")
                .unwrap()
                .iter()
                .map(|row| row.values().to_vec())
                .collect::<Vec<_>>()
        };
        let before_rows = canonical_rows();
        assert_eq!(
            storage.inspect_search_fallback_fts_parity().unwrap().status,
            FtsShadowParityStatus::Residue,
            "{name}: fixture must start as residue"
        );

        storage
            .rebuild_fts()
            .unwrap_or_else(|err| panic!("{name}: direct rebuild must converge: {err:#}"));

        assert_eq!(
            storage.inspect_search_fallback_fts_parity().unwrap().status,
            FtsShadowParityStatus::Healthy,
            "{name}: rebuild must publish exact parity"
        );
        assert!(
            storage
                .fts_messages_schema_is_canonical_contentless()
                .unwrap(),
            "{name}: only the canonical registration may survive"
        );
        let matches = |term: &str| {
            storage
                .raw()
                .query_row_map(
                    "SELECT COUNT(*) FROM fts_messages WHERE fts_messages MATCH ?1",
                    fparams![term],
                    |row| row.get_typed::<i64>(0),
                )
                .unwrap()
        };
        assert_eq!(
            matches("survives"),
            1,
            "{name}: canonical text is searchable"
        );
        assert_eq!(
            matches("orphan"),
            0,
            "{name}: residue text must not survive"
        );
        assert_eq!(
            canonical_rows(),
            before_rows,
            "{name}: canonical rows changed"
        );

        // Idempotent: a healthy canonical shadow takes the ordinary
        // transactional path and stays canonical.
        storage.rebuild_fts().unwrap();
        assert_eq!(
            storage.inspect_search_fallback_fts_parity().unwrap().status,
            FtsShadowParityStatus::Healthy
        );
        assert_eq!(matches("survives"), 1);
    }
}

/// GH #413 follow-up (iify0): the shadow bound drops an oversized shadow,
/// records both markers, refuses to recreate it while the corpus is over
/// the bound, and lets the ordinary repair recreate it once it fits.
#[test]
fn fts_shadow_bound_drops_and_refuses_to_recreate_until_the_corpus_fits() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("fts-shadow-bound.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    seed_atomic_fts_rebuild_fixture(&storage);
    let corpus = storage.fts_shadow_corpus_messages().unwrap();
    assert_eq!(
        corpus, 1,
        "the fixture conversation carries one indexable message"
    );
    assert!(
        corpus > 0,
        "the fixture conversation carries an indexable message"
    );

    // At the bound the shadow is viable; one byte under it is not.
    assert_eq!(
        storage
            .fts_shadow_viability_with_bound(Some(corpus))
            .unwrap(),
        FtsShadowViability::Viable {
            corpus_messages: corpus
        }
    );
    assert_eq!(
        storage.fts_shadow_viability_with_bound(None).unwrap(),
        FtsShadowViability::Viable {
            corpus_messages: corpus
        }
    );
    let not_viable = storage
        .fts_shadow_viability_with_bound(Some(corpus - 1))
        .unwrap();
    assert_eq!(
        not_viable,
        FtsShadowViability::NotViable {
            corpus_messages: corpus,
            bound_messages: corpus - 1
        }
    );

    let detail = fts_shadow_not_viable_detail(corpus, corpus - 1);
    assert!(error_message_indicates_fts_shadow_not_viable(&detail));
    storage.drop_fts_shadow_as_not_viable(&detail).unwrap();
    assert_eq!(
        storage.fts_shadow_viability_with_bound(Some(1)).unwrap(),
        FtsShadowViability::Absent
    );
    for table in std::iter::once("fts_messages").chain(FTS_MESSAGES_REQUIRED_SHADOW_TABLES) {
        let rows: i64 = storage
            .raw()
            .query_row_map(
                "SELECT COUNT(*) FROM sqlite_master WHERE name = ?1",
                fparams![table],
                |row| row.get_typed(0),
            )
            .unwrap();
        assert_eq!(rows, 0, "{table} must be gone after the drop");
    }
    assert!(
        storage
            .fts_shadow_not_viable_marker()
            .unwrap()
            .is_some_and(|marker| marker.contains("not viable on this engine"))
    );
    assert!(
        storage
            .read_fallback_fts_repair_pending()
            .unwrap()
            .is_some_and(|pending| pending.contains("not viable on this engine")),
        "status/doctor read the drop through the repair-pending marker"
    );

    // Still over the bound: recreation is refused and the marker stays.
    assert!(
        storage
            .fts_shadow_recreate_refused_with_bound(Some(corpus - 1))
            .unwrap()
            .is_some()
    );
    assert!(storage.fts_shadow_not_viable_marker().unwrap().is_some());
    // Fits again: the marker clears and the ordinary repair recreates it.
    assert!(
        storage
            .fts_shadow_recreate_refused_with_bound(Some(corpus))
            .unwrap()
            .is_none()
    );
    assert!(storage.fts_shadow_not_viable_marker().unwrap().is_none());
    storage.ensure_search_fallback_fts_consistency().unwrap();
    assert_eq!(
        storage.fts_shadow_viability_with_bound(None).unwrap(),
        FtsShadowViability::Viable {
            corpus_messages: corpus
        }
    );
}

/// GH #497 follow-up: once the recorded retirement describes the current
/// corpus exactly, a repeat repair must not write (the reporter saw every
/// doctor run and dry-run claim a mutation on a settled archive), while a
/// retirement whose marker no longer matches is still rewritten.
#[test]
fn gh497_current_fts_retirement_is_not_rewritten_but_a_stale_one_is() {
    const CHILD_ENV: &str = "CASS_TEST_FTS_RETIREMENT_NOOP_CHILD";
    let test_name = exact_child_test_name(
        module_path!(),
        "gh497_current_fts_retirement_is_not_rewritten_but_a_stale_one_is",
    );
    if !matches!(dotenvy::var(CHILD_ENV).as_deref(), Ok("1")) {
        let output = std::process::Command::new(
            std::env::current_exe().expect("current library test executable"),
        )
        .args(["--exact", &test_name, "--nocapture"])
        .env(CHILD_ENV, "1")
        .env("CASS_FTS_SHADOW_MAX_MESSAGES", "2")
        .output()
        .expect("run isolated FTS retirement regression");
        assert!(
            output.status.success(),
            "FTS retirement child failed: {}\nstdout: {}\nstderr: {}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("1 passed"),
            "the child must execute the regression, not silently filter it out"
        );
        return;
    }
    assert_eq!(fts_shadow_max_messages(), Some(2));

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("fts-retirement-noop.db");
    let storage = FrankenStorage::open(&db_path).expect("open retirement fixture");
    seed_atomic_fts_rebuild_fixture(&storage);
    let conversation_id: i64 = storage
        .raw()
        .query_row_map("SELECT id FROM conversations LIMIT 1", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    for idx in 1..3_i64 {
        storage
            .raw()
            .execute_compat(
                "INSERT INTO messages(conversation_id, idx, role, content) VALUES(?1, ?2, 'user', 'retired corpus message')",
                fparams![conversation_id, idx],
            )
            .unwrap();
    }
    assert_eq!(storage.fts_shadow_corpus_messages().unwrap(), 3);
    let wal_len = || {
        std::fs::metadata(database_sidecar_path(&db_path, "-wal"))
            .map(|meta| meta.len())
            .unwrap_or(0)
    };
    let markers = || {
        (
            storage.fts_shadow_not_viable_marker().unwrap(),
            storage.read_fallback_fts_repair_pending().unwrap(),
        )
    };
    let refuse = || {
        let error = storage
            .ensure_search_fallback_fts_consistency()
            .expect_err("over-bound recreation must be refused");
        assert!(error_message_indicates_fts_shadow_not_viable(&format!(
            "{error:#}"
        )));
    };

    // The first refusal drops the canonical shadow and records both markers.
    assert!(!storage.fts_shadow_retirement_is_current().unwrap());
    storage
        .drop_fts_shadow_as_not_viable("an older retirement text")
        .unwrap();
    assert!(
        !storage.fts_shadow_retirement_is_current().unwrap(),
        "a marker that does not describe this corpus is not current"
    );
    refuse();
    assert!(storage.fts_shadow_retirement_is_current().unwrap());
    let settled = markers();
    let expected = bounded_fts_marker_detail(&fts_shadow_not_viable_detail(3, 2));
    assert_eq!(settled.0.as_deref(), Some(expected.as_str()));
    assert_eq!(settled.1.as_deref(), Some(expected.as_str()));

    // Settled: repeat refusals write nothing at all.
    let settled_wal = wal_len();
    assert!(settled_wal > 0, "the fixture must write through the WAL");
    refuse();
    refuse();
    assert_eq!(
        wal_len(),
        settled_wal,
        "a settled retirement must not be rewritten"
    );
    assert_eq!(markers(), settled);

    // A stale marker (here: an index run's different pending reason) is
    // rewritten with the current retirement.
    storage
        .record_fallback_fts_repair_pending(Some("some other pending reason"))
        .unwrap();
    assert!(!storage.fts_shadow_retirement_is_current().unwrap());
    let stale_wal = wal_len();
    refuse();
    assert!(
        wal_len() > stale_wal,
        "a stale retirement must be rewritten"
    );
    assert!(storage.fts_shadow_retirement_is_current().unwrap());
    assert_eq!(markers(), settled);
}

#[test]
fn fts_shadow_recreation_enforces_bound_with_and_without_retirement_marker() {
    const CHILD_ENV: &str = "CASS_TEST_FTS_RECREATION_BOUND_CHILD";
    if !matches!(dotenvy::var(CHILD_ENV).as_deref(), Ok("1")) {
        let output = std::process::Command::new(
            std::env::current_exe().expect("current library test executable"),
        )
        .args([
            "--exact",
            &exact_child_test_name(
                module_path!(),
                "fts_shadow_recreation_enforces_bound_with_and_without_retirement_marker",
            ),
            "--nocapture",
        ])
        .env(CHILD_ENV, "1")
        .env("CASS_FTS_SHADOW_MAX_MESSAGES", "2")
        .output()
        .expect("run isolated FTS bound regression");
        assert!(
            output.status.success(),
            "FTS bound child failed: {}\nstdout: {}\nstderr: {}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("1 passed"),
            "the child must execute the regression, not silently filter it out"
        );
        return;
    }
    assert_eq!(fts_shadow_max_messages(), Some(2));

    for marker_present in [false, true] {
        for corpus_messages in [1_u64, 2, 3] {
            let dir = TempDir::new().unwrap();
            let storage = FrankenStorage::open(&dir.path().join("fts-bound-matrix.db"))
                .expect("open boundary fixture");
            seed_atomic_fts_rebuild_fixture(&storage);
            let conversation_id: i64 = storage
                .raw()
                .query_row_map("SELECT id FROM conversations LIMIT 1", fparams![], |row| {
                    row.get_typed(0)
                })
                .unwrap();
            for idx in 1..corpus_messages {
                storage
                    .raw()
                    .execute_compat(
                        "INSERT INTO messages(conversation_id, idx, role, content) VALUES(?1, ?2, 'user', 'marigold boundary message')",
                        fparams![conversation_id, i64::try_from(idx).unwrap()],
                    )
                    .unwrap();
            }
            let canonical_rows = || {
                [
                    "SELECT * FROM conversations ORDER BY id",
                    "SELECT * FROM messages ORDER BY id",
                ]
                .map(|sql| {
                    storage
                        .raw()
                        .query(sql)
                        .unwrap()
                        .iter()
                        .map(|row| row.values().to_vec())
                        .collect::<Vec<_>>()
                })
            };
            let before = canonical_rows();
            let historical_marker = "historical FTS size retirement";
            storage
                .drop_fts_shadow_as_not_viable(historical_marker)
                .unwrap();
            if !marker_present {
                storage.clear_fts_shadow_not_viable_marker().unwrap();
                storage.record_fallback_fts_repair_pending(None).unwrap();
            }
            assert_eq!(
                storage.fts_shadow_corpus_messages().unwrap(),
                corpus_messages
            );
            assert_eq!(
                storage.inspect_search_fallback_fts_parity().unwrap().status,
                FtsShadowParityStatus::Absent
            );

            let repair = storage.ensure_search_fallback_fts_consistency();
            if corpus_messages > 2 {
                let error = repair.expect_err("over-bound recreation must be refused");
                assert!(error_message_indicates_fts_shadow_not_viable(&format!(
                    "{error:#}"
                )));
                assert_eq!(
                    storage.inspect_search_fallback_fts_parity().unwrap().status,
                    FtsShadowParityStatus::Absent
                );
                // The refusal records the durable size-retirement marker
                // with the current counts whether or not one existed: the
                // next full index reads it as the terminal
                // `SkippedNotViable` state instead of retrying a repair
                // that must fail (GH #495, repair_fallback_fts_after_full_index_run).
                let marker = storage.fts_shadow_not_viable_marker().unwrap();
                assert!(
                    marker
                        .as_deref()
                        .is_some_and(|marker| marker != historical_marker
                            && error_message_indicates_fts_shadow_not_viable(marker)),
                    "over-bound refusal must record a fresh not-viable marker \
                     (marker_present={marker_present}): {marker:?}"
                );
            } else {
                assert!(matches!(
                    repair.expect("within-bound recreation must succeed"),
                    FtsConsistencyRepair::Rebuilt { inserted_rows }
                        if inserted_rows == usize::try_from(corpus_messages).unwrap()
                ));
                assert_eq!(
                    storage.inspect_search_fallback_fts_parity().unwrap().status,
                    FtsShadowParityStatus::Healthy
                );
                assert!(storage.fts_shadow_not_viable_marker().unwrap().is_none());
            }
            assert_eq!(
                canonical_rows(),
                before,
                "canonical rows changed: marker={marker_present}, corpus={corpus_messages}"
            );
        }
    }
}

#[test]
#[serial]
fn interrupted_partial_fts_catch_up_resumes_from_committed_batches() {
    let _batch_size = set_env_var("CASS_FTS_REBUILD_BATCH_SIZE", "2");
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("fts-partial-resume.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    seed_atomic_fts_rebuild_fixture(&storage);
    let conversation_id: i64 = storage
        .raw()
        .query_row_map("SELECT id FROM conversations LIMIT 1", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    for (idx, content) in [
        "resumecommitted sentinel",
        "resumependingtwo sentinel",
        "resumependingthree sentinel",
        "resumependingfour sentinel",
    ]
    .into_iter()
    .enumerate()
    {
        storage
            .raw()
            .execute_compat(
                "INSERT INTO messages(conversation_id, idx, role, content) VALUES(?1, ?2, 'assistant', ?3)",
                fparams![conversation_id, i64::try_from(idx + 1).unwrap(), content],
            )
            .unwrap();
    }
    assert_eq!(
        storage.inspect_search_fallback_fts_parity().unwrap().status,
        FtsShadowParityStatus::Partial
    );
    arm_fts_page_diagnostics(&db_path);

    // The first page contains the already-indexed row plus the first
    // missing row. Its autocommit INSERT is durable before the injected
    // interruption, which models a process dying between catch-up pages.
    arm_fts_rebuild_interruption(2);
    let error = storage
        .ensure_search_fallback_fts_consistency()
        .expect_err("partial catch-up interruption must surface");
    assert!(
        error.to_string().contains("resuming missing FTS rows"),
        "unexpected partial catch-up error: {error:#}"
    );
    emit_fts_page_diagnostics(&storage, "after_initial_interruption");
    drop(storage);

    let reopened = FrankenStorage::open_existing_schema_only_for_fts_repair(&db_path).unwrap();
    emit_fts_page_diagnostics(&reopened, "immediately_after_reopen");
    let search_count = |term: &str| {
        reopened
            .raw()
            .query_row_map(
                "SELECT COUNT(*) FROM fts_messages WHERE fts_messages MATCH ?1",
                fparams![term],
                |row| row.get_typed::<i64>(0),
            )
            .unwrap()
    };
    let interrupted_parity = reopened.inspect_search_fallback_fts_parity().unwrap();
    assert_eq!(interrupted_parity.status, FtsShadowParityStatus::Partial);
    assert_eq!(interrupted_parity.indexable_messages, 5);
    assert_eq!(interrupted_parity.indexed_messages, Some(2));
    assert_eq!(search_count("survives"), 1);
    assert_eq!(search_count("resumecommitted"), 1);
    assert_eq!(search_count("resumependingtwo"), 0);

    let repair = match reopened.ensure_search_fallback_fts_consistency() {
        Ok(repair) => repair,
        Err(error) => {
            emit_fts_page_diagnostics(&reopened, "after_resumed_repair_failure");
            let canonical_ids: Vec<i64> = reopened
                .raw()
                .query_map_collect("SELECT id FROM messages ORDER BY id", fparams![], |row| {
                    row.get_typed(0)
                })
                .unwrap();
            let indexed_ids: Vec<i64> = reopened
                .raw()
                .query_map_collect(
                    "SELECT id FROM fts_messages_docsize ORDER BY id",
                    fparams![],
                    |row| row.get_typed(0),
                )
                .unwrap();
            panic!(
                "resume partial catch-up after reopen failed: {error:#}; canonical={canonical_ids:?}; indexed={indexed_ids:?}"
            );
        }
    };
    assert_eq!(
        repair,
        FtsConsistencyRepair::IncrementalCatchUp {
            inserted_rows: 3,
            total_rows: 5,
        }
    );
    let repaired_parity = reopened.inspect_search_fallback_fts_parity().unwrap();
    let canonical_ids: Vec<i64> = reopened
        .raw()
        .query_map_collect("SELECT id FROM messages ORDER BY id", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    let indexed_ids: Vec<i64> = reopened
        .raw()
        .query_map_collect(
            "SELECT id FROM fts_messages_docsize ORDER BY id",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(
        repaired_parity.status,
        FtsShadowParityStatus::Healthy,
        "resumed repair must preserve exact rowids: parity={repaired_parity:?}, canonical={canonical_ids:?}, indexed={indexed_ids:?}"
    );
    assert_eq!(repaired_parity.indexed_messages, Some(5));
    assert_eq!(
        reopened
            .inspect_fts_rowid_set_difference_with_page_rows(2)
            .expect("compare exact FTS parity across multiple tiny pages"),
        (false, false),
        "equal five-row ID sets must remain equal across 2-row keyset page boundaries"
    );
    for term in [
        "survives",
        "resumecommitted",
        "resumependingtwo",
        "resumependingthree",
        "resumependingfour",
    ] {
        assert_eq!(search_count(term), 1, "missing or duplicate term {term}");
    }
    let distinct_rowids: i64 = reopened
        .raw()
        .query_row_map(
            "SELECT COUNT(DISTINCT id) FROM fts_messages_docsize",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(distinct_rowids, 5);

    let second_repair = reopened
        .ensure_search_fallback_fts_consistency()
        .expect("healthy repeat repair must be idempotent");
    assert_eq!(
        second_repair,
        FtsConsistencyRepair::AlreadyHealthy { rows: 5 }
    );
    assert_eq!(
        reopened
            .inspect_search_fallback_fts_parity()
            .unwrap()
            .indexed_messages,
        Some(5)
    );
    drop(reopened);

    let ordinary = FrankenStorage::open(&db_path)
        .expect("ordinary post-doctor open must retain lazy contentless FTS support");
    assert_eq!(
        ordinary
            .inspect_search_fallback_fts_parity()
            .expect("inspect parity after ordinary reopen")
            .status,
        FtsShadowParityStatus::Healthy
    );
    for term in [
        "survives",
        "resumecommitted",
        "resumependingtwo",
        "resumependingthree",
        "resumependingfour",
    ] {
        let count: i64 = ordinary
            .raw()
            .query_row_map(
                "SELECT COUNT(*) FROM fts_messages WHERE fts_messages MATCH ?1",
                fparams![term],
                |row| row.get_typed(0),
            )
            .unwrap();
        assert_eq!(count, 1, "ordinary reopen lost term {term}");
    }
}
