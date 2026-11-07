use crate::cli::commands::analytics::run_analytics::{
    close_franken_cli_read_db, open_franken_analytics_db, open_franken_cli_read_db,
};
use crate::cli::commands::search::effective_db_path_source::{
    append_source_filter_condition, source_filter_where_clause,
};
use crate::cli::commands::search::effective_db_path_source::{
    sort_stats_counts, source_filter_where_clause_with_columns, stats_agent_count_sql,
    stats_message_count_sql, stats_source_rows, stats_workspace_count_sql,
    unfiltered_conversation_stats,
};
use crate::cli::commands::sessions_context::CONTEXT_CANDIDATES_SQL;
use crate::cli::commands::sessions_context::{
    ContextCandidate, context_related_ids, find_context_source_conversation,
};
use crate::cli::commands::view::role_to_export_string::normalized_source_id_sql_expr;
use crate::cli::commands::view::role_to_export_string::{
    normalized_source_identity_sql_expr, normalized_source_origin_kind_sql_expr,
};
use crate::cli::contract_version::with_frankensqlite_connection;
use crate::franken_sync::compat::{ConnectionExt, RowExt};
use crate::sources::provenance::SourceFilter;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tempfile::TempDir;

pub(crate) fn make_legacy_local_db() -> (TempDir, PathBuf, PathBuf) {
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("agent_search.db");
    let shared_path = PathBuf::from("/legacy/shared/session.jsonl");

    with_frankensqlite_connection(&db_path, "legacy source test", |conn| {
        conn.execute("PRAGMA journal_mode = WAL;")?;
        conn.execute("CREATE TABLE agents (id INTEGER PRIMARY KEY, slug TEXT NOT NULL);")?;
        conn.execute(
            "CREATE TABLE workspaces (id INTEGER PRIMARY KEY, path TEXT, display_name TEXT);",
        )?;
        conn.execute("CREATE TABLE sources (id TEXT PRIMARY KEY, kind TEXT, host_label TEXT);")?;
        conn.execute(
            "CREATE TABLE conversations (
                    id INTEGER PRIMARY KEY,
                    agent_id INTEGER NOT NULL,
                    workspace_id INTEGER,
                    source_id TEXT,
                    origin_host TEXT,
                    title TEXT,
                    source_path TEXT NOT NULL,
                    started_at INTEGER,
                    ended_at INTEGER,
                    approx_tokens INTEGER,
                    external_id TEXT,
                    metadata_json TEXT,
                    metadata_bin BLOB
                );",
        )?;
        conn.execute(
            "CREATE TABLE messages (
                    id INTEGER PRIMARY KEY,
                    conversation_id INTEGER NOT NULL,
                    idx INTEGER,
                    role TEXT,
                    created_at INTEGER,
                    content TEXT
                );",
        )?;
        conn.execute("INSERT INTO agents(id, slug) VALUES (1, 'codex')")?;
        conn.execute(
            "INSERT INTO sources(id, kind, host_label) VALUES ('laptop', 'ssh', 'user@laptop')",
        )?;
        conn.execute(
            "INSERT INTO conversations(
                    id, agent_id, workspace_id, source_id, origin_host, title,
                    source_path, started_at, ended_at, approx_tokens, external_id,
                    metadata_json, metadata_bin
                ) VALUES (
                    1, 1, NULL, NULL, NULL, 'Local Session',
                    '/legacy/shared/session.jsonl', 1700000000000, NULL, 10,
                    'legacy-local', '{}', NULL
                )",
        )?;
        conn.execute(
            "INSERT INTO conversations(
                    id, agent_id, workspace_id, source_id, origin_host, title,
                    source_path, started_at, ended_at, approx_tokens, external_id,
                    metadata_json, metadata_bin
                ) VALUES (
                    2, 1, NULL, 'laptop', 'user@laptop', 'Remote Session',
                    '/legacy/shared/session.jsonl', 1700000100000, NULL, 10,
                    'remote-shared', '{}', NULL
                )",
        )?;
        conn.execute(
            "INSERT INTO messages(id, conversation_id, idx, role, created_at, content)
                 VALUES (1, 1, 0, 'user', 1700000000100, 'local body')",
        )?;
        conn.execute(
            "INSERT INTO messages(id, conversation_id, idx, role, created_at, content)
                 VALUES (2, 2, 0, 'user', 1700000100100, 'remote body')",
        )?;
        Ok(())
    })
    .expect("create legacy db");

    (tmp, db_path, shared_path)
}

pub(crate) fn open_legacy_local_db_for_update(db_path: &Path) -> crate::franken_sync::Connection {
    crate::franken_sync::Connection::open(db_path.to_string_lossy().to_string())
        .expect("open writable legacy source test db")
}

#[test]
pub(crate) fn source_filter_helpers_use_normalized_origin_kind_for_local_semantics() {
    let normalized_source_sql = normalized_source_identity_sql_expr("c.source_id", "c.origin_host");
    let normalized_where_origin_kind_sql = normalized_source_origin_kind_sql_expr(
        "c.source_id",
        "(SELECT s.kind FROM sources s WHERE s.id = c.source_id LIMIT 1)",
        "c.origin_host",
    );

    let (where_sql, param) = source_filter_where_clause(Some(&SourceFilter::Local));
    assert_eq!(
        where_sql,
        format!(
            " WHERE {normalized_where_origin_kind_sql} = '{local}'",
            local = crate::sources::provenance::LOCAL_SOURCE_ID,
        )
    );
    assert!(param.is_none());

    let (where_sql, param) =
        source_filter_where_clause(Some(&SourceFilter::SourceId("  LOCAL  ".to_string())));
    assert_eq!(where_sql, format!(" WHERE {normalized_source_sql} = ?"));
    assert_eq!(param.as_deref(), Some("local"));

    let mut sql = "SELECT 1 WHERE 1=1".to_string();
    let mut params = Vec::new();
    append_source_filter_condition(&mut sql, &mut params, &SourceFilter::Remote);
    let normalized_joined_origin_kind_sql =
        normalized_source_origin_kind_sql_expr("c.source_id", "s.kind", "c.origin_host");
    assert_eq!(
        sql,
        format!(
            "SELECT 1 WHERE 1=1 AND {normalized_joined_origin_kind_sql} != '{local}'",
            local = crate::sources::provenance::LOCAL_SOURCE_ID,
        )
    );
    assert!(params.is_empty());
}

#[test]
pub(crate) fn stats_source_rows_match_the_messages_join_with_and_without_the_autoindex() {
    // (conversation id, source_id, origin_host, messages)
    let seed: [(i64, Option<&str>, Option<&str>, i64); 6] = [
        (1, None, None, 3),
        (2, Some("local"), None, 0),
        (3, Some("   "), None, 2),
        (4, Some("laptop"), Some("user@laptop"), 4),
        (5, Some(" laptop "), Some("user@laptop"), 1),
        (6, Some(""), Some("srv"), 0),
    ];
    // Modern archives carry UNIQUE(conversation_id, idx), whose autoindex
    // the streaming pass reads; historical ones fall back to a table scan.
    for unique_index in [true, false] {
        let tmp = TempDir::new().expect("temp dir");
        let db_path = tmp.path().join("agent_search.db");
        with_frankensqlite_connection(&db_path, "stats source rows test", |conn| {
                conn.execute(
                    "CREATE TABLE conversations (id INTEGER PRIMARY KEY, source_id TEXT, origin_host TEXT)",
                )?;
                conn.execute(if unique_index {
                    "CREATE TABLE messages (id INTEGER PRIMARY KEY, conversation_id INTEGER NOT NULL, \
                     idx INTEGER NOT NULL, UNIQUE(conversation_id, idx))"
                } else {
                    "CREATE TABLE messages (id INTEGER PRIMARY KEY, conversation_id INTEGER NOT NULL, idx INTEGER)"
                })?;
                for (id, source_id, origin_host, messages) in seed {
                    conn.execute_compat(
                        "INSERT INTO conversations(id, source_id, origin_host) VALUES (?1, ?2, ?3)",
                        &[id.into(), source_id.into(), origin_host.into()],
                    )?;
                    for idx in 0..messages {
                        conn.execute_compat(
                            "INSERT INTO messages(conversation_id, idx) VALUES (?1, ?2)",
                            &[id.into(), idx.into()],
                        )?;
                    }
                }
                Ok(())
            })
            .expect("seed stats source rows db");

        let conn = open_franken_analytics_db(&None, Some(&db_path)).expect("open db");
        let normalized = normalized_source_identity_sql_expr("c.source_id", "c.origin_host");
        let mut oracle: Vec<(String, i64, i64)> = conn
            .query_map_collect(
                &format!(
                    "SELECT {normalized}, COUNT(DISTINCT c.id), COUNT(m.id) FROM conversations c \
                         LEFT JOIN messages m ON m.conversation_id = c.id GROUP BY {normalized}"
                ),
                &[],
                |r: &crate::franken_sync::Row| {
                    Ok((r.get_typed(0)?, r.get_typed(1)?, r.get_typed(2)?))
                },
            )
            .expect("old join query");
        oracle.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        let conversation_sources: Vec<(i64, String)> = conn
            .query_map_collect(
                &format!("SELECT c.id, {normalized} FROM conversations c"),
                &[],
                |r: &crate::franken_sync::Row| Ok((r.get_typed(0)?, r.get_typed(1)?)),
            )
            .expect("conversation sources");
        let rows = stats_source_rows(&conn, conversation_sources).expect("source rows");

        assert_eq!(rows, oracle, "unique_index={unique_index}");
        assert_eq!(
            rows,
            [
                ("local".to_string(), 3, 5),
                ("laptop".to_string(), 2, 5),
                ("srv".to_string(), 1, 0),
            ],
            "unique_index={unique_index}"
        );
        close_franken_cli_read_db(conn, &db_path, "stats source rows test").expect("close db");
    }
}

#[test]
pub(crate) fn stats_message_count_sql_skips_join_when_unfiltered() {
    assert_eq!(
        stats_message_count_sql("", ""),
        "SELECT COUNT(*) FROM messages",
        "unfiltered stats can count messages directly without a conversation join"
    );
    assert_eq!(
        stats_workspace_count_sql("", ""),
        "SELECT c.workspace_id, COUNT(*) FROM conversations c WHERE c.workspace_id IS NOT NULL GROUP BY c.workspace_id",
        "unfiltered workspace stats can aggregate workspace IDs before path lookup"
    );

    let source_join = " LEFT JOIN sources s ON s.id = c.source_id";
    let (where_sql, _param) = source_filter_where_clause_with_columns(
        Some(&SourceFilter::Local),
        "c.source_id",
        "s.kind",
        "c.origin_host",
    );
    let sql = stats_message_count_sql(source_join, &where_sql);
    assert!(
        sql.contains("JOIN conversations c ON m.conversation_id = c.id"),
        "source-filtered stats must retain the conversation join; sql={sql}"
    );
    assert!(
        sql.contains(source_join),
        "kind-filtered stats must join registered source metadata; sql={sql}"
    );
    assert!(
        sql.ends_with(&where_sql),
        "source filter should be appended unchanged; sql={sql}"
    );

    let workspace_sql = stats_workspace_count_sql(source_join, &where_sql);
    assert!(
        workspace_sql.contains(" AND c.workspace_id IS NOT NULL"),
        "source-filtered workspace stats append the non-null guard after the source WHERE; sql={workspace_sql}"
    );
}

#[test]
pub(crate) fn stats_grouping_preserves_sql_results_with_large_unused_payloads() {
    use crate::franken_sync::compat::{ConnectionExt as _, RowExt as _};

    fn assert_fold_matches_sql(conn: &crate::franken_sync::Connection) {
        let folded = unfiltered_conversation_stats(conn).expect("stream real statistics");
        let count: i64 = conn
            .query_row_map("SELECT COUNT(*) FROM conversations", &[], |row| {
                row.get_typed(0)
            })
            .expect("SQL count oracle");
        assert_eq!(folded.count, count);
        let dates: (Option<i64>, Option<i64>) = conn
                .query_row_map(
                    "SELECT MIN(started_at), MAX(started_at) FROM conversations WHERE started_at IS NOT NULL",
                    &[],
                    |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
                )
                .expect("SQL date oracle");
        assert_eq!((folded.oldest, folded.newest), dates);
        let mut agents: Vec<(Option<i64>, i64)> = conn
                .query_map_collect(
                    "SELECT agent_id, COUNT(*) FROM conversations GROUP BY agent_id ORDER BY COUNT(*) DESC",
                    &[],
                    |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
                )
                .expect("SQL agent oracle");
        let mut folded_agents: Vec<_> = folded.agents.into_iter().collect();
        sort_stats_counts(&mut agents);
        sort_stats_counts(&mut folded_agents);
        assert_eq!(folded_agents, agents);
        let mut workspaces: Vec<(i64, i64)> = conn
                .query_map_collect(
                    "SELECT workspace_id, COUNT(*) FROM conversations WHERE workspace_id IS NOT NULL GROUP BY workspace_id ORDER BY COUNT(*) DESC",
                    &[],
                    |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
                )
                .expect("SQL workspace oracle");
        let mut folded_workspaces: Vec<_> = folded.workspaces.into_iter().collect();
        sort_stats_counts(&mut workspaces);
        sort_stats_counts(&mut folded_workspaces);
        assert_eq!(folded_workspaces, workspaces);
    }

    let tmp = tempfile::tempdir().expect("temporary archive");
    let path = tmp.path().join("stats.db");
    let writer = crate::franken_sync::Connection::open(path.to_string_lossy().into_owned())
        .expect("create archive");
    writer.execute("CREATE TABLE conversations (id INTEGER PRIMARY KEY, agent_id INTEGER, workspace_id INTEGER, source_id TEXT, origin_host TEXT, payload TEXT, started_at INTEGER)").expect("conversations");
    assert_fold_matches_sql(&writer);
    writer
        .execute("CREATE TABLE sources (id TEXT PRIMARY KEY, kind TEXT)")
        .expect("sources");
    writer.execute("INSERT INTO sources VALUES ('local', 'local'), ('remote-a', 'ssh'), ('registered-local', 'local')").expect("source kinds");
    let payload = "unused archive payload ".repeat(8192);
    for id in 1..=24_i64 {
        writer
            .execute_compat(
                "INSERT INTO conversations VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                crate::franken_sync::params![
                    id,
                    (id % 4 != 0).then_some(id % 3),
                    (id % 5 != 0).then_some(id % 12),
                    match id % 3 {
                        0 => "local",
                        1 => "remote-a",
                        _ => "registered-local",
                    },
                    if id % 3 == 1 { Some("remote-a") } else { None },
                    payload.as_str(),
                    (id % 4 != 0).then_some((id - 12) * 1000)
                ],
            )
            .expect("payload-heavy conversation");
    }
    assert_fold_matches_sql(&writer);
    writer
        .execute("UPDATE conversations SET started_at = NULL")
        .expect("plant all-NULL dates");
    assert_fold_matches_sql(&writer);
    writer
            .execute("UPDATE conversations SET started_at = CASE WHEN id % 4 != 0 THEN (id - 12) * 1000 ELSE NULL END")
            .expect("restore mixed pre-epoch and NULL dates");
    writer.close().expect("publish archive");
    let conn = open_franken_cli_read_db(path, "stats parity", Duration::from_secs(5))
        .expect("same reader as stats");
    assert_fold_matches_sql(&conn);
    for (label, sql, expected_route) in [
        (
            "old",
            format!("{} ORDER BY COUNT(*) DESC", stats_agent_count_sql("", "")),
            "group_by_fallback",
        ),
        (
            "new",
            stats_agent_count_sql("", ""),
            "group_by_storage_substrate",
        ),
    ] {
        let trace_path = tmp.path().join(format!("{label}.log"));
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_ansi(false)
            .with_writer(std::sync::Mutex::new(
                std::fs::File::create(&trace_path).expect("trace file"),
            ))
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            conn.query(&sql).expect("trace actual grouped execution");
        });
        let trace = std::fs::read_to_string(trace_path).expect("read engine trace");
        assert!(
            trace.contains(expected_route),
            "expected {expected_route}: {trace}"
        );
        if label == "new" {
            assert!(
                !trace.contains("group_by_fallback"),
                "new query must stay on storage grouping: {trace}"
            );
        }
    }
    for filter in [
        None,
        Some(SourceFilter::Local),
        Some(SourceFilter::Remote),
        Some(SourceFilter::SourceId("remote-a".to_string())),
        Some(SourceFilter::SourceId("absent".to_string())),
    ] {
        let joined = matches!(filter, Some(SourceFilter::Local | SourceFilter::Remote));
        let join = if joined {
            " LEFT JOIN sources s ON s.id = c.source_id"
        } else {
            ""
        };
        let (condition, param) = source_filter_where_clause_with_columns(
            filter.as_ref(),
            "c.source_id",
            if joined { "s.kind" } else { "NULL" },
            "c.origin_host",
        );
        let params: Vec<crate::franken_sync::compat::ParamValue> =
            param.as_deref().map(Into::into).into_iter().collect();
        for sql in [
            stats_agent_count_sql(join, &condition),
            stats_workspace_count_sql(join, &condition),
        ] {
            let read = |query: &str| -> Vec<(Option<i64>, i64)> {
                conn.query_map_collect(query, &params, |row| {
                    Ok((row.get_typed(0)?, row.get_typed(1)?))
                })
                .expect("execute real aggregate")
            };
            let mut old = read(&format!("{sql} ORDER BY COUNT(*) DESC"));
            let mut new = read(&sql);
            sort_stats_counts(&mut old);
            sort_stats_counts(&mut new);
            assert_eq!(new, old, "source-filtered aggregate parity: {sql}");
        }
    }
    let mut ties = vec![(Some(3), 2), (None, 2), (Some(1), 2), (Some(9), 3)];
    sort_stats_counts(&mut ties);
    assert_eq!(
        ties,
        vec![(Some(9), 3), (None, 2), (Some(1), 2), (Some(3), 2)]
    );
}

#[test]
pub(crate) fn source_filter_helpers_match_blank_remote_source_id_via_origin_host() {
    let (_tmp, db_path, _shared_path) = make_legacy_local_db();
    let conn = open_legacy_local_db_for_update(&db_path);
    conn.execute("UPDATE conversations SET source_id = '   ' WHERE id = 2")
        .expect("blank remote source_id");

    let (where_sql, param) =
        source_filter_where_clause(Some(&SourceFilter::SourceId("user@laptop".to_string())));
    let sql = format!("SELECT COUNT(*) FROM conversations c{where_sql}");
    let params_vec = vec![param.expect("source id param").into()];
    let count: i64 = conn
        .query_row_map(&sql, &params_vec, |r: &crate::franken_sync::Row| {
            r.get_typed(0)
        })
        .expect("count blank remote source rows");
    assert_eq!(count, 1);

    let (where_sql, param) = source_filter_where_clause(Some(&SourceFilter::Remote));
    assert!(param.is_none());
    let sql = format!("SELECT COUNT(*) FROM conversations c{where_sql}");
    let count: i64 = conn
        .query_row_map(&sql, &[], |r: &crate::franken_sync::Row| r.get_typed(0))
        .expect("count remote source rows");
    assert_eq!(count, 1);

    conn.close().expect("close writable legacy source test db");
}

#[test]
pub(crate) fn source_filter_helpers_classify_registered_named_local_source_by_kind() {
    let (_tmp, db_path, _shared_path) = make_legacy_local_db();
    let conn = open_legacy_local_db_for_update(&db_path);
    conn.execute(
        "INSERT INTO sources(id, kind, host_label) VALUES ('backup-local', 'local', NULL)",
    )
    .expect("insert named local source");
    conn.execute(
        "UPDATE conversations SET source_id = 'backup-local', origin_host = NULL WHERE id = 2",
    )
    .expect("move second conversation to named local source");

    let count_for = |filter: SourceFilter| {
        let (where_sql, param) = source_filter_where_clause_with_columns(
            Some(&filter),
            "c.source_id",
            "s.kind",
            "c.origin_host",
        );
        let sql = format!(
            "SELECT COUNT(*) FROM conversations c \
                 LEFT JOIN sources s ON s.id = c.source_id{where_sql}"
        );
        let params = param
            .as_deref()
            .map(crate::franken_sync::compat::ParamValue::from)
            .into_iter()
            .collect::<Vec<_>>();
        conn.query_row_map(&sql, &params, |row: &crate::franken_sync::Row| {
            row.get_typed::<i64>(0)
        })
        .expect("count source-filtered conversations")
    };

    assert_eq!(count_for(SourceFilter::Local), 2);
    assert_eq!(count_for(SourceFilter::Remote), 0);
    assert_eq!(
        count_for(SourceFilter::SourceId("backup-local".to_string())),
        1
    );

    conn.close().expect("close writable legacy source test db");
}

#[test]
pub(crate) fn source_filter_helpers_match_trimmed_local_source_id_in_db() {
    let (_tmp, db_path, _shared_path) = make_legacy_local_db();
    let conn = open_legacy_local_db_for_update(&db_path);
    conn.execute("UPDATE conversations SET source_id = '  local  ' WHERE id = 1")
        .expect("update legacy local source_id");

    let (where_sql, param) =
        source_filter_where_clause(Some(&SourceFilter::SourceId("local".to_string())));
    let sql = format!("SELECT COUNT(*) FROM conversations c{where_sql}");
    let params_vec = vec![param.expect("source id param").into()];
    let count: i64 = conn
        .query_row_map(&sql, &params_vec, |r: &crate::franken_sync::Row| {
            r.get_typed(0)
        })
        .expect("count trimmed local source rows");
    assert_eq!(count, 1);

    let (where_sql, param) = source_filter_where_clause(Some(&SourceFilter::Local));
    assert!(param.is_none());
    let sql = format!("SELECT COUNT(*) FROM conversations c{where_sql}");
    let count: i64 = conn
        .query_row_map(&sql, &[], |r: &crate::franken_sync::Row| r.get_typed(0))
        .expect("count local source rows");
    assert_eq!(count, 1);

    conn.close().expect("close writable legacy source test db");
}

#[test]
pub(crate) fn context_candidates_covering_plan_limits_and_legacy_nulls() {
    use crate::franken_sync::compat::ConnectionExt as _;

    let conn = crate::franken_sync::Connection::open(":memory:").unwrap();
    conn.execute_batch(
        "CREATE TABLE conversations (
                 id INTEGER PRIMARY KEY, workspace_id INTEGER, agent_id INTEGER,
                 started_at INTEGER, metadata_bin BLOB
             );
             INSERT INTO conversations(id, workspace_id, agent_id, started_at) VALUES
                 (1, 1, 1, 100), (2, 1, 2, 300), (3, 2, 1, 200),
                 (4, 1, 1, NULL), (5, 2, 2, 86400000), (6, NULL, NULL, 250),
                 (7, 1, 1, 300), (8, NULL, NULL, NULL);",
    )
    .unwrap();
    let source = ContextCandidate {
        id: 1,
        workspace_id: Some(1),
        agent_id: Some(1),
        started_at: Some(100),
    };
    let expected = [vec![2, 7, 4], vec![2, 7, 6, 3], vec![7, 3, 4]];
    assert_eq!(
        context_related_ids(&conn, &source, usize::MAX).unwrap(),
        expected
    );
    conn.execute(
        "CREATE INDEX idx_conversations_context
             ON conversations(started_at DESC, workspace_id, agent_id)",
    )
    .unwrap();
    conn.execute("PRAGMA query_only = 1").unwrap();
    assert_eq!(
        context_related_ids(&conn, &source, usize::MAX).unwrap(),
        expected
    );
    assert_eq!(
        context_related_ids(&conn, &source, 1).unwrap(),
        [vec![2], vec![2], vec![7]]
    );
    assert_eq!(
        context_related_ids(&conn, &source, 0).unwrap(),
        [Vec::<i64>::new(), vec![], vec![]]
    );
    let null_source = ContextCandidate {
        id: 8,
        workspace_id: None,
        agent_id: None,
        started_at: None,
    };
    assert_eq!(
        context_related_ids(&conn, &null_source, 10).unwrap(),
        [Vec::<i64>::new(), vec![], vec![]]
    );

    let index_root: i64 = conn
        .query_row_map(
            "SELECT rootpage FROM sqlite_master WHERE name = 'idx_conversations_context'",
            &[],
            |row| row.get_typed(0),
        )
        .unwrap();
    let explain = conn
        .query(&format!("EXPLAIN {CONTEXT_CANDIDATES_SQL}"))
        .unwrap();
    let mut read_roots = Vec::new();
    for row in explain {
        let opcode: String = row.get_typed(1).unwrap();
        assert!(
            !matches!(opcode.as_str(), "SeekRowid" | "SorterOpen"),
            "{opcode}"
        );
        if opcode == "OpenRead" {
            read_roots.push(row.get_typed::<i64>(3).unwrap());
        }
    }
    assert_eq!(
        read_roots,
        vec![index_root],
        "candidate scan must open only the covering index"
    );
    // Negative control: the same projection without the hint reads the
    // conversation table, so narrow column names alone are not proof.
    let unhinted = conn
        .query("EXPLAIN SELECT id, workspace_id, agent_id, started_at FROM conversations")
        .unwrap();
    assert!(unhinted.iter().any(|row| {
        row.get_typed::<String>(1).unwrap() == "OpenRead"
            && row.get_typed::<i64>(3).unwrap() != index_root
    }));
}

#[test]
pub(crate) fn find_context_source_conversation_treats_null_source_as_local() {
    let (_tmp, db_path, shared_path) = make_legacy_local_db();
    let conn = open_franken_analytics_db(&None, Some(&db_path)).expect("open analytics db");

    let row =
        find_context_source_conversation(&conn, &shared_path.to_string_lossy(), Some("local"))
            .expect("legacy local row should resolve");
    assert_eq!(row.4, "Local Session");
    assert_eq!(row.6, "local");

    close_franken_cli_read_db(conn, &db_path, "legacy source test").expect("close db");
}

#[test]
pub(crate) fn find_context_source_conversation_normalizes_trimmed_source_id() {
    let (_tmp, db_path, shared_path) = make_legacy_local_db();
    let conn = open_franken_analytics_db(&None, Some(&db_path)).expect("open analytics db");

    let row =
        find_context_source_conversation(&conn, &shared_path.to_string_lossy(), Some("  LOCAL  "))
            .expect("trimmed local source should resolve");
    assert_eq!(row.4, "Local Session");
    assert_eq!(row.6, "local");

    close_franken_cli_read_db(conn, &db_path, "legacy source test").expect("close db");
}

#[test]
pub(crate) fn find_context_source_conversation_matches_blank_remote_source_id_via_origin_host() {
    let (_tmp, db_path, shared_path) = make_legacy_local_db();
    let conn = open_legacy_local_db_for_update(&db_path);
    conn.execute("UPDATE conversations SET source_id = '   ' WHERE id = 2")
        .expect("blank remote source_id");

    let row = find_context_source_conversation(
        &conn,
        &shared_path.to_string_lossy(),
        Some("user@laptop"),
    )
    .expect("blank remote source should resolve via origin host");
    assert_eq!(row.4, "Remote Session");
    assert_eq!(row.6, "user@laptop");

    conn.close().expect("close writable legacy source test db");
}

#[test]
pub(crate) fn find_context_source_conversation_matches_trimmed_local_source_id_in_db() {
    let (_tmp, db_path, shared_path) = make_legacy_local_db();
    let conn = open_legacy_local_db_for_update(&db_path);
    conn.execute("UPDATE conversations SET source_id = '  local  ' WHERE id = 1")
        .expect("update legacy local source_id");

    let row =
        find_context_source_conversation(&conn, &shared_path.to_string_lossy(), Some("local"))
            .expect("trimmed local source stored in db should resolve");
    assert_eq!(row.4, "Local Session");
    assert_eq!(row.6, "local");

    let preferred = find_context_source_conversation(&conn, &shared_path.to_string_lossy(), None)
        .expect("path-only lookup should still prefer normalized local row");
    assert_eq!(preferred.4, "Local Session");
    assert_eq!(preferred.6, "local");

    conn.close().expect("close writable legacy source test db");
}

#[test]
pub(crate) fn context_and_stats_queries_surface_null_source_id_as_local() {
    let (_tmp, db_path, shared_path) = make_legacy_local_db();
    let conn = open_franken_analytics_db(&None, Some(&db_path)).expect("open analytics db");

    let selected = find_context_source_conversation(&conn, &shared_path.to_string_lossy(), None)
        .expect("shared path should resolve");
    assert_eq!(selected.4, "Local Session");
    assert_eq!(selected.6, "local");

    let normalized_source_sql = normalized_source_id_sql_expr("c.source_id");
    let source_sql = format!(
        "SELECT {normalized_source_sql} as source_id, COUNT(DISTINCT c.id)
             FROM conversations c
             GROUP BY {normalized_source_sql}
             ORDER BY source_id"
    );
    let source_rows: Vec<(String, i64)> = conn
        .query_map_collect(&source_sql, &[], |r: &crate::franken_sync::Row| {
            Ok((r.get_typed(0)?, r.get_typed(1)?))
        })
        .expect("query source rows");
    assert!(
        source_rows
            .iter()
            .any(|(source_id, count)| source_id == "local" && *count == 1)
    );
    assert!(
        source_rows
            .iter()
            .any(|(source_id, count)| source_id == "laptop" && *count == 1)
    );

    let timeline_rows: Vec<String> = conn
        .query_map_collect(
            "SELECT COALESCE(c.source_id, 'local')
                 FROM conversations c
                 JOIN agents a ON c.agent_id = a.id
                 LEFT JOIN messages m ON m.conversation_id = c.id
                 WHERE c.started_at >= ?1 AND c.started_at <= ?2
                 GROUP BY c.id
                 ORDER BY c.started_at DESC",
            &[1_699_999_999_000_i64.into(), 1_700_000_200_000_i64.into()],
            |r: &crate::franken_sync::Row| r.get_typed(0),
        )
        .expect("query timeline rows");
    assert!(timeline_rows.iter().any(|source_id| source_id == "local"));

    close_franken_cli_read_db(conn, &db_path, "legacy source test").expect("close db");
}

#[test]
pub(crate) fn stats_source_grouping_uses_origin_host_when_remote_source_id_blank() {
    let (_tmp, db_path, _shared_path) = make_legacy_local_db();
    let conn = open_legacy_local_db_for_update(&db_path);
    conn.execute("UPDATE conversations SET source_id = '   ' WHERE id = 2")
        .expect("blank remote source_id");

    let normalized_source_sql = normalized_source_identity_sql_expr("c.source_id", "c.origin_host");
    let source_sql = format!(
        "SELECT {normalized_source_sql} as source_id, COUNT(DISTINCT c.id)
             FROM conversations c
             GROUP BY {normalized_source_sql}
             ORDER BY source_id"
    );
    let source_rows: Vec<(String, i64)> = conn
        .query_map_collect(&source_sql, &[], |r: &crate::franken_sync::Row| {
            Ok((r.get_typed(0)?, r.get_typed(1)?))
        })
        .expect("query normalized source rows");

    assert!(
        source_rows
            .iter()
            .any(|(source_id, count)| source_id == "user@laptop" && *count == 1),
        "expected blank remote source to group under origin host, got: {source_rows:?}"
    );

    conn.close().expect("close writable legacy source test db");
}

#[test]
pub(crate) fn stats_source_grouping_normalizes_trimmed_local_source_id_in_db() {
    let (_tmp, db_path, _shared_path) = make_legacy_local_db();
    let conn = open_legacy_local_db_for_update(&db_path);
    conn.execute("UPDATE conversations SET source_id = '  local  ' WHERE id = 1")
        .expect("update legacy local source_id");

    let normalized_source_sql = normalized_source_id_sql_expr("c.source_id");
    let source_sql = format!(
        "SELECT {normalized_source_sql} as source_id, COUNT(DISTINCT c.id)
             FROM conversations c
             GROUP BY {normalized_source_sql}
             ORDER BY source_id"
    );
    let source_rows: Vec<(String, i64)> = conn
        .query_map_collect(&source_sql, &[], |r: &crate::franken_sync::Row| {
            Ok((r.get_typed(0)?, r.get_typed(1)?))
        })
        .expect("query normalized source rows");

    assert!(
        source_rows
            .iter()
            .any(|(source_id, count)| source_id == "local" && *count == 1),
        "expected trimmed local source to group under local, got: {source_rows:?}"
    );
    assert!(
        !source_rows
            .iter()
            .any(|(source_id, _)| source_id.trim() != source_id || source_id.is_empty()),
        "expected no raw trimmed/blank source ids, got: {source_rows:?}"
    );

    conn.close().expect("close writable legacy source test db");
}
