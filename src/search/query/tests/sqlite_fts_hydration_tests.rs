use super::super::*;
use super::helpers::*;
use crate::connectors::{NormalizedConversation, NormalizedMessage};
use crate::franken_sync::Connection as FrankenConnection;
use crate::franken_sync::compat::{
    ConnectionExt as FrankenConnectionExt, OptionalExtension, RowExt,
};
use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use crate::search::tantivy::TantivyIndex;
use crate::storage::sqlite::FrankenStorage;
use tempfile::TempDir;

#[test]
fn search_with_fallback_marks_implicit_wildcard() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;

    let conv = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("handlers".into()),
        workspace: None,
        source_path: dir.path().join("h2.jsonl"),
        started_at: Some(1),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(1),
            content: "the request handler delegates".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    index.add_conversation(&conv)?;
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");

    // Base search for "andle" finds nothing; fallback "*andle*" should hit and mark implicit.
    let result = client.search_with_fallback(
        "andle",
        SearchFilters::default(),
        10,
        0,
        2,
        FieldMask::FULL,
    )?;
    assert!(result.wildcard_fallback);
    assert_eq!(result.hits.len(), 1);
    assert_eq!(result.hits[0].match_type, MatchType::ImplicitWildcard);

    Ok(())
}

/// 1t79z: a SQLite-only client no longer short-circuits leading-wildcard
/// queries to a false-empty result; they reach the bounded source scan.
#[test]
fn sqlite_backend_routes_wildcard_queries_to_the_scan_lane() -> Result<()> {
    let conn = SearchSqliteFixture::in_memory()?;
    conn.execute_batch(
        "CREATE TABLE sources (id TEXT PRIMARY KEY, kind TEXT);
         CREATE TABLE agents (id INTEGER PRIMARY KEY, slug TEXT NOT NULL);
         CREATE TABLE workspaces (id INTEGER PRIMARY KEY, path TEXT NOT NULL);
         CREATE TABLE conversations (
            id INTEGER PRIMARY KEY,
            agent_id INTEGER,
            workspace_id INTEGER,
            source_id TEXT,
            origin_host TEXT,
            title TEXT,
            source_path TEXT NOT NULL
         );
         CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            conversation_id INTEGER NOT NULL,
            idx INTEGER,
            content TEXT NOT NULL,
            created_at INTEGER
         );
         CREATE TABLE fts_messages (marker TEXT);
         INSERT INTO sources(id, kind) VALUES('local', 'local');
         INSERT INTO agents(id, slug) VALUES(1, 'codex');
         INSERT INTO workspaces(id, path) VALUES(1, '/workspace');
         INSERT INTO conversations(
            id, agent_id, workspace_id, source_id, origin_host, title, source_path
         ) VALUES(1, 1, 1, 'local', NULL, 'routing title', '/tmp/routing.jsonl');
         INSERT INTO messages(id, conversation_id, idx, content, created_at) VALUES
            (1, 1, 0, 'the error_handler fired', 1);",
    )?;
    let client = SearchClient {
        reader: LexicalReaderSlot::default(),
        sqlite: Mutex::new(Some(conn.into_connection())),
        sqlite_path: None,
        prefix_cache: Mutex::new(CacheShards::new(*CACHE_TOTAL_CAP, *CACHE_BYTE_CAP)),
        reload_on_search: true,
        strict_read_only: false,
        last_reload: Mutex::new(None),
        last_generation: Mutex::new(None),
        reload_epoch: Arc::new(AtomicU64::new(0)),
        warm_tx: None,
        _warm_handle: None,
        metrics: Metrics::default(),
        cache_namespace: format!("v{CACHE_KEY_VERSION}|schema:test"),
        semantic: Mutex::new(None),
        last_tantivy_total_count: Mutex::new(None),
        last_lexical_degrade_reason: Mutex::new(None),
        last_wildcard_fallback_skip: Mutex::new(None),
    };

    let hits = client.search("*handler", SearchFilters::default(), 5, 0, FieldMask::FULL)?;
    assert_eq!(
        hits.len(),
        1,
        "suffix wildcard must be answered by the scan lane"
    );
    assert!(hits[0].content.contains("error_handler"));

    Ok(())
}

#[test]
fn sqlite_backend_handles_null_workspace() -> Result<()> {
    let conn = SearchSqliteFixture::in_memory()?;
    conn.execute_batch(
        "CREATE TABLE sources (id TEXT PRIMARY KEY, kind TEXT);
         CREATE TABLE agents (id INTEGER PRIMARY KEY, slug TEXT NOT NULL UNIQUE);
         CREATE TABLE workspaces (id INTEGER PRIMARY KEY, path TEXT NOT NULL UNIQUE);
         CREATE TABLE conversations (
            id INTEGER PRIMARY KEY,
            agent_id INTEGER,
            workspace_id INTEGER,
            source_id TEXT,
            origin_host TEXT,
            title TEXT,
            source_path TEXT
         );
         CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            conversation_id INTEGER,
            idx INTEGER,
            content TEXT,
            created_at INTEGER
         );
         CREATE VIRTUAL TABLE fts_messages USING fts5(
            content,
            title,
            agent,
            workspace,
            source_path,
            created_at UNINDEXED,
            content='',
            tokenize='porter'
         );",
    )?;
    conn.execute("INSERT INTO sources(id, kind) VALUES('local', 'local')")?;
    conn.execute("INSERT INTO agents(id, slug) VALUES(1, 'codex')")?;
    conn.execute(
        "INSERT INTO conversations(id, agent_id, workspace_id, source_id, origin_host, title, source_path) VALUES(1, 1, NULL, 'local', NULL, 't', '/tmp/session.jsonl')",
    )?;
    conn.execute("INSERT INTO messages(id, conversation_id, idx, content, created_at) VALUES(1, 1, 0, 'auth token failure', 42)")?;
    conn.execute_compat(
        "INSERT INTO fts_messages(rowid, content, title, agent, workspace, source_path, created_at)
         VALUES(?1, ?2, ?3, ?4, NULL, ?5, ?6)",
        params![
            1_i64,
            "auth token failure",
            "t",
            "codex",
            "/tmp/session.jsonl",
            42_i64
        ],
    )?;

    let client = SearchClient {
        reader: LexicalReaderSlot::default(),
        sqlite: Mutex::new(Some(conn.into_connection())),
        sqlite_path: None,
        prefix_cache: Mutex::new(CacheShards::new(*CACHE_TOTAL_CAP, *CACHE_BYTE_CAP)),
        reload_on_search: true,
        strict_read_only: false,
        last_reload: Mutex::new(None),
        last_generation: Mutex::new(None),
        reload_epoch: Arc::new(AtomicU64::new(0)),
        warm_tx: None,
        _warm_handle: None,
        metrics: Metrics::default(),
        cache_namespace: format!("v{CACHE_KEY_VERSION}|schema:test"),
        semantic: Mutex::new(None),
        last_tantivy_total_count: Mutex::new(None),
        last_lexical_degrade_reason: Mutex::new(None),
        last_wildcard_fallback_skip: Mutex::new(None),
    };

    let hits = client.search("auth", SearchFilters::default(), 5, 0, FieldMask::FULL)?;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].workspace, "");
    assert_eq!(hits[0].line_number, Some(1));
    assert_eq!(hits[0].source_id, "local");
    assert_eq!(hits[0].origin_kind, "local");
    Ok(())
}

#[test]
fn sqlite_backend_supports_legacy_fts_message_id_schema() -> Result<()> {
    let conn = SearchSqliteFixture::in_memory()?;
    conn.execute_batch(
        "CREATE TABLE sources (id TEXT PRIMARY KEY, kind TEXT);
         CREATE TABLE agents (id INTEGER PRIMARY KEY, slug TEXT NOT NULL UNIQUE);
         CREATE TABLE workspaces (id INTEGER PRIMARY KEY, path TEXT NOT NULL UNIQUE);
         CREATE TABLE conversations (
            id INTEGER PRIMARY KEY,
            agent_id INTEGER,
            workspace_id INTEGER,
            source_id TEXT,
            origin_host TEXT,
            title TEXT,
            source_path TEXT
         );
         CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            conversation_id INTEGER,
            idx INTEGER,
            content TEXT,
            created_at INTEGER
         );
         CREATE VIRTUAL TABLE fts_messages USING fts5(
            content,
            title,
            agent,
            workspace,
            source_path,
            created_at UNINDEXED,
            message_id UNINDEXED,
            tokenize='porter'
         );",
    )?;
    conn.execute("INSERT INTO sources(id, kind) VALUES('local', 'local')")?;
    conn.execute("INSERT INTO agents(id, slug) VALUES(1, 'codex')")?;
    conn.execute("INSERT INTO workspaces(id, path) VALUES(1, '/legacy')")?;
    conn.execute(
        "INSERT INTO conversations(id, agent_id, workspace_id, source_id, origin_host, title, source_path)
         VALUES(1, 1, 1, 'local', NULL, 'legacy title', '/tmp/legacy.jsonl')",
    )?;
    conn.execute(
        "INSERT INTO messages(id, conversation_id, idx, content, created_at)
         VALUES(42, 1, 4, 'legacy auth token failure', 99)",
    )?;
    conn.execute_compat(
        "INSERT INTO fts_messages(rowid, content, title, agent, workspace, source_path, created_at, message_id)
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            1_i64,
            "legacy auth token failure",
            "legacy title",
            "codex",
            "/legacy",
            "/tmp/legacy.jsonl",
            99_i64,
            42_i64
        ],
    )?;

    let client = SearchClient {
        reader: LexicalReaderSlot::default(),
        sqlite: Mutex::new(Some(conn.into_connection())),
        sqlite_path: None,
        prefix_cache: Mutex::new(CacheShards::new(*CACHE_TOTAL_CAP, *CACHE_BYTE_CAP)),
        reload_on_search: true,
        strict_read_only: false,
        last_reload: Mutex::new(None),
        last_generation: Mutex::new(None),
        reload_epoch: Arc::new(AtomicU64::new(0)),
        warm_tx: None,
        _warm_handle: None,
        metrics: Metrics::default(),
        cache_namespace: format!("v{CACHE_KEY_VERSION}|schema:test"),
        semantic: Mutex::new(None),
        last_tantivy_total_count: Mutex::new(None),
        last_lexical_degrade_reason: Mutex::new(None),
        last_wildcard_fallback_skip: Mutex::new(None),
    };

    let hits = client.search("auth", SearchFilters::default(), 5, 0, FieldMask::FULL)?;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].title, "legacy title");
    assert_eq!(hits[0].source_path, "/tmp/legacy.jsonl");
    assert_eq!(hits[0].workspace, "/legacy");
    assert_eq!(hits[0].line_number, Some(5));
    assert_eq!(hits[0].content, "legacy auth token failure");
    Ok(())
}

#[test]
fn tantivy_reader_skips_sqlite_fallback_on_empty_lexical_results() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;
    index.commit()?;
    let reader = crate::search::quill_bridge::open_cass_reader(dir.path())
        .ok()
        .map(|reader| {
            (
                reader,
                crate::search::quill_bridge::QuillCassFields::compiled(),
            )
        });
    assert!(
        reader.is_some(),
        "test fixture should open a Tantivy reader even with an empty index"
    );

    let conn = SearchSqliteFixture::in_memory()?;
    conn.execute_batch(
        "CREATE TABLE sources (id TEXT PRIMARY KEY, kind TEXT);
         CREATE TABLE agents (id INTEGER PRIMARY KEY, slug TEXT NOT NULL UNIQUE);
         CREATE TABLE workspaces (id INTEGER PRIMARY KEY, path TEXT NOT NULL UNIQUE);
         CREATE TABLE conversations (
            id INTEGER PRIMARY KEY,
            agent_id INTEGER,
            workspace_id INTEGER,
            source_id TEXT,
            origin_host TEXT,
            title TEXT,
            source_path TEXT
         );
         CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            conversation_id INTEGER,
            idx INTEGER,
            content TEXT,
            created_at INTEGER
         );
         CREATE VIRTUAL TABLE fts_messages USING fts5(
            content,
            title,
            agent,
            workspace,
            source_path,
            created_at UNINDEXED,
            content='',
            tokenize='porter'
         );",
    )?;
    conn.execute("INSERT INTO sources(id, kind) VALUES('local', 'local')")?;
    conn.execute("INSERT INTO agents(id, slug) VALUES(1, 'codex')")?;
    conn.execute("INSERT INTO workspaces(id, path) VALUES(1, '/sqlite-only')")?;
    conn.execute(
        "INSERT INTO conversations(id, agent_id, workspace_id, source_id, origin_host, title, source_path)
         VALUES(1, 1, 1, 'local', NULL, 'sqlite fallback only', '/tmp/sqlite-only.jsonl')",
    )?;
    conn.execute(
        "INSERT INTO messages(id, conversation_id, idx, content, created_at)
         VALUES(1, 1, 0, 'sqliteonlytoken overflow candidate', 42)",
    )?;
    conn.execute_compat(
        "INSERT INTO fts_messages(rowid, content, title, agent, workspace, source_path, created_at)
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            1_i64,
            "sqliteonlytoken overflow candidate",
            "sqlite fallback only",
            "codex",
            "/sqlite-only",
            "/tmp/sqlite-only.jsonl",
            42_i64
        ],
    )?;

    let client = SearchClient {
        reader: LexicalReaderSlot::new(dir.path().to_path_buf(), reader),
        sqlite: Mutex::new(Some(conn.into_connection())),
        sqlite_path: None,
        prefix_cache: Mutex::new(CacheShards::new(*CACHE_TOTAL_CAP, *CACHE_BYTE_CAP)),
        reload_on_search: true,
        strict_read_only: false,
        last_reload: Mutex::new(None),
        last_generation: Mutex::new(None),
        reload_epoch: Arc::new(AtomicU64::new(0)),
        warm_tx: None,
        _warm_handle: None,
        metrics: Metrics::default(),
        cache_namespace: format!("v{CACHE_KEY_VERSION}|schema:test"),
        semantic: Mutex::new(None),
        last_tantivy_total_count: Mutex::new(None),
        last_lexical_degrade_reason: Mutex::new(None),
        last_wildcard_fallback_skip: Mutex::new(None),
    };

    let sqlite_hits = client.search_sqlite_fts5(
        Path::new(":memory:"),
        "sqliteonlytoken",
        SearchFilters::default(),
        5,
        0,
        FieldMask::FULL,
    )?;
    assert_eq!(
        sqlite_hits.len(),
        1,
        "fixture should prove sqlite fallback would have produced a hit"
    );

    let tantivy_authoritative_hits = client.search(
        "sqliteonlytoken",
        SearchFilters::default(),
        5,
        0,
        FieldMask::FULL,
    )?;
    assert!(
        tantivy_authoritative_hits.is_empty(),
        "a live Tantivy reader should prevent sqlite fallback from populating empty lexical results"
    );
    Ok(())
}

#[test]
fn sqlite_guard_does_not_repair_fts_when_generation_key_stale() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("stale-gen-fts.db");

    // Seed a DB with a conversation and indexed FTS content.
    {
        let storage = FrankenStorage::open(&db_path)?;
        let agent = Agent {
            id: None,
            slug: "codex".into(),
            name: "Codex".into(),
            version: None,
            kind: AgentKind::Cli,
        };
        let agent_id = storage.ensure_agent(&agent)?;
        let conversation = Conversation {
            id: None,
            agent_slug: "codex".into(),
            workspace: Some(PathBuf::from("/tmp/workspace")),
            external_id: Some("stale-gen-fts".into()),
            title: Some("Stale FTS generation".into()),
            source_path: PathBuf::from("/tmp/stale-gen-fts.jsonl"),
            started_at: Some(1_700_000_000_000),
            ended_at: Some(1_700_000_000_100),
            approx_tokens: Some(42),
            metadata_json: serde_json::Value::Null,
            messages: vec![Message {
                id: None,
                idx: 0,
                role: MessageRole::User,
                author: Some("user".into()),
                created_at: Some(1_700_000_000_050),
                content: "message that should remain queryable".into(),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            }],
            source_id: "local".into(),
            origin_host: None,
        };
        storage.insert_conversation_tree(agent_id, None, &conversation)?;
    }

    let count_before = sqlite_master_name_count(&db_path, "fts_messages")
        .context("count schema rows before generation key deletion")?;

    // Simulate a stale generation by deleting the rebuild marker.
    // This is the condition ensure_fts_consistency
    // detects to trigger a full FTS rebuild.
    {
        let conn = FrankenConnection::open(db_path.to_string_lossy())?;
        conn.execute_compat(
            "DELETE FROM meta WHERE key = ?1",
            &[ParamValue::from("fts_frankensqlite_rebuild_generation")],
        )?;
    }

    // Opening via sqlite_guard() must remain read-only. A search path
    // should not trigger heavyweight derived-index repair.
    let client = SearchClient {
        reader: LexicalReaderSlot::default(),
        sqlite: Mutex::new(None),
        sqlite_path: Some(db_path.clone()),
        prefix_cache: Mutex::new(CacheShards::new(*CACHE_TOTAL_CAP, *CACHE_BYTE_CAP)),
        reload_on_search: true,
        strict_read_only: false,
        last_reload: Mutex::new(None),
        last_generation: Mutex::new(None),
        reload_epoch: Arc::new(AtomicU64::new(0)),
        warm_tx: None,
        _warm_handle: None,
        metrics: Metrics::default(),
        cache_namespace: format!("v{CACHE_KEY_VERSION}|schema:test"),
        semantic: Mutex::new(None),
        last_tantivy_total_count: Mutex::new(None),
        last_lexical_degrade_reason: Mutex::new(None),
        last_wildcard_fallback_skip: Mutex::new(None),
    };

    let guard = client
        .sqlite_guard()
        .context("open sqlite guard for stale generation fixture")?;
    assert!(guard.is_some(), "sqlite guard should open the db");
    let conn = guard
        .as_ref()
        .expect("sqlite guard should hold a connection");
    let no_params: [ParamValue; 0] = [];
    let cache_size: i64 =
        conn.query_row_map("PRAGMA cache_size;", &no_params, |row| row.get_typed(0))?;
    assert_eq!(
        cache_size, -SEARCH_SQLITE_HYDRATION_CACHE_KIB,
        "search hydration should not inherit the general storage cache profile"
    );
    conn.execute_sync("DELETE FROM meta WHERE 1 = 0")
        .expect_err("search hydration connection must remain physically read-only");
    drop(guard);

    // The read-only open must not rewrite the rebuild-generation marker.
    let conn = FrankenConnection::open(db_path.to_string_lossy())?;
    let generation_after: Option<String> = conn
        .query_row_map(
            "SELECT value FROM meta WHERE key = ?1",
            &[ParamValue::from("fts_frankensqlite_rebuild_generation")],
            |row| row.get_typed(0),
        )
        .optional()?;
    assert!(
        generation_after.is_none(),
        "search sqlite guard must not mutate FTS rebuild metadata"
    );

    // Schema rows remain unchanged by the read-only open.
    let count_after = sqlite_master_name_count(&db_path, "fts_messages")
        .context("count schema rows after sqlite guard reopen")?;
    assert_eq!(
        count_after, count_before,
        "read-only reopen must leave FTS schema state unchanged"
    );

    Ok(())
}

#[test]
fn gh395_browse_by_date_hydrates_one_lazy_connection_from_multiple_workers() -> Result<()> {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<SearchSqliteConnection>();
    assert_send_sync::<SearchClient>();

    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("cross-worker-hydration.db");
    {
        let conn = FrankenConnection::open(db_path.to_string_lossy())?;
        conn.execute_batch(
            "PRAGMA application_id = 0x43415353;
             PRAGMA user_version = 1;
             CREATE TABLE sources (id TEXT PRIMARY KEY, kind TEXT NOT NULL);
             CREATE TABLE agents (id INTEGER PRIMARY KEY, slug TEXT NOT NULL);
             CREATE TABLE workspaces (id INTEGER PRIMARY KEY, path TEXT NOT NULL);
             CREATE TABLE conversations (
                 id INTEGER PRIMARY KEY,
                 agent_id INTEGER,
                 workspace_id INTEGER,
                 source_id TEXT,
                 origin_host TEXT,
                 title TEXT,
                 source_path TEXT NOT NULL,
                 started_at INTEGER,
                 ended_at INTEGER,
                 last_message_idx INTEGER,
                 last_message_created_at INTEGER
             );
             CREATE TABLE messages (
                 id INTEGER PRIMARY KEY,
                 conversation_id INTEGER NOT NULL,
                 idx INTEGER NOT NULL,
                 content TEXT NOT NULL,
                 created_at INTEGER,
                 UNIQUE(conversation_id, idx)
             );
             CREATE TABLE conversation_tail_state (
                 conversation_id INTEGER PRIMARY KEY,
                 ended_at INTEGER,
                 last_message_idx INTEGER,
                 last_message_created_at INTEGER
             );
             INSERT INTO sources(id, kind) VALUES('local', 'local');
             INSERT INTO agents(id, slug) VALUES(1, 'codex');
             INSERT INTO workspaces(id, path) VALUES(1, '/cross-worker');
             INSERT INTO conversations(
                 id, agent_id, workspace_id, source_id, origin_host, title, source_path
             ) VALUES(
                 1, 1, 1, 'local', NULL, 'worker-owned archive', '/tmp/cross-worker.jsonl'
             );
             INSERT INTO messages(id, conversation_id, idx, content, created_at)
             VALUES(1, 1, 0, 'cross worker hydration sentinel', 42);
             INSERT INTO conversation_tail_state(
                 conversation_id, ended_at, last_message_idx, last_message_created_at
             ) VALUES(1, 42, 0, 42);",
        )?;
    }

    let client = Arc::new(SearchClient {
        reader: LexicalReaderSlot::default(),
        sqlite: Mutex::new(None),
        sqlite_path: Some(db_path),
        prefix_cache: Mutex::new(CacheShards::new(*CACHE_TOTAL_CAP, *CACHE_BYTE_CAP)),
        reload_on_search: true,
        strict_read_only: false,
        last_reload: Mutex::new(None),
        last_generation: Mutex::new(None),
        reload_epoch: Arc::new(AtomicU64::new(0)),
        warm_tx: None,
        _warm_handle: None,
        metrics: Metrics::default(),
        cache_namespace: format!("v{CACHE_KEY_VERSION}|schema:cross-worker"),
        semantic: Mutex::new(None),
        last_tantivy_total_count: Mutex::new(None),
        last_lexical_degrade_reason: Mutex::new(None),
        last_wildcard_fallback_skip: Mutex::new(None),
    });
    let worker_count = 4;
    let start = Arc::new(std::sync::Barrier::new(worker_count + 1));
    let mut workers = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        let client = Arc::clone(&client);
        let start = Arc::clone(&start);
        workers.push(std::thread::spawn(move || -> Result<()> {
            start.wait();
            for _ in 0..3 {
                let hits =
                    client.browse_by_date(SearchFilters::default(), 1, 0, true, FieldMask::FULL)?;
                assert_eq!(hits.len(), 1);
                assert_eq!(hits[0].content, "cross worker hydration sentinel");
                assert_eq!(hits[0].source_path, "/tmp/cross-worker.jsonl");
            }
            Ok(())
        }));
    }
    start.wait();
    for worker in workers {
        worker
            .join()
            .map_err(|_| anyhow!("cross-worker hydration worker panicked"))??;
    }

    let mut guard = client.sqlite_guard()?;
    let mut conn = guard
        .take()
        .expect("the shared client should retain one dedicated-owner connection");
    drop(guard);
    conn.close_without_checkpoint_sync()
        .map_err(|error| anyhow!("closing cross-worker sqlite owner: {error}"))?;
    Ok(())
}

/// k0lo1: metadata-only text (agent / workspace / source_path) must not
/// match in the SQLite fallback lane; title/content text must.
#[test]
fn sqlite_fts_fallback_ignores_metadata_only_terms() -> Result<()> {
    fn fts_match_count(conn: &FrankenConnection, fts_query: &str) -> Result<Option<usize>> {
        let probe_params = [ParamValue::from("__cass_fts_probe_no_match__")];
        let match_mode = match conn.query_map_collect(
            "SELECT COUNT(*) FROM fts_messages WHERE fts_messages MATCH ?",
            &probe_params,
            |row| row.get_typed::<i64>(0),
        ) {
            Ok(_) => SqliteFtsMatchMode::Table,
            Err(err)
                if err
                    .to_string()
                    .contains("no such column: fts_messages in table fts_messages") =>
            {
                SqliteFtsMatchMode::IndexedColumns
            }
            Err(err) => return Err(err.into()),
        };
        let sql = format!(
            "SELECT COUNT(*) FROM fts_messages WHERE {}",
            SearchClient::sqlite_fts5_match_clause(match_mode)
        );
        let mut params = Vec::new();
        SearchClient::push_sqlite_fts5_match_params(&mut params, fts_query, match_mode);
        match conn.query_map_collect(&sql, &params, |row| row.get_typed::<i64>(0)) {
            Ok(rows) => {
                let count = rows.into_iter().next().unwrap_or(0);
                Ok(Some(usize::try_from(count.max(0)).unwrap_or(usize::MAX)))
            }
            Err(err) if err.to_string().contains("no such function: MATCH/2") => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("metadata-only-fallback.db");
    let conn = FrankenConnection::open(db_path.to_string_lossy().as_ref())?;
    conn.execute_batch(
        "PRAGMA application_id = 0x43415353;
         PRAGMA user_version = 1;
         CREATE VIRTUAL TABLE fts_messages USING fts5(
             content,
             title,
             agent,
             workspace,
             source_path,
             created_at UNINDEXED,
             tokenize='porter'
         );",
    )?;
    let seed = |rowid: i64, content: &str, title: &str, agent: &str, ws: &str, src: &str| {
        conn.execute_compat(
            "INSERT INTO fts_messages(rowid, content, title, agent, workspace, source_path, created_at)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            &[
                ParamValue::from(rowid),
                ParamValue::from(content),
                ParamValue::from(title),
                ParamValue::from(agent),
                ParamValue::from(ws),
                ParamValue::from(src),
                ParamValue::from(rowid),
            ],
        )
    };
    // Sentinel only in metadata fields — must NOT match.
    seed(
        1,
        "plain body text",
        "plain title",
        "zebraterm",
        "/ws/zebraterm",
        "/tmp/zebraterm.jsonl",
    )?;
    // Sentinel in content — must match.
    seed(
        2,
        "body mentions zebraterm here",
        "plain title",
        "codex",
        "/ws/alpha",
        "/tmp/a.jsonl",
    )?;
    // Sentinel in title — must match.
    seed(
        3,
        "plain body text",
        "zebraterm title",
        "codex",
        "/ws/alpha",
        "/tmp/b.jsonl",
    )?;

    let transpiled = transpile_to_fts5("zebraterm").expect("transpiled query");
    if let Some(count) = fts_match_count(&conn, transpiled.as_str())? {
        assert_eq!(
            count, 2,
            "only the content/title rows may match; metadata-only text is not query text"
        );
    }
    // Control: a term nowhere in the table matches nothing.
    let absent = transpile_to_fts5("quokkaterm").expect("transpiled query");
    if let Some(count) = fts_match_count(&conn, absent.as_str())? {
        assert_eq!(count, 0);
    }
    Ok(())
}

#[test]
fn sqlite_path_rusqlite_fallback_matches_hyphenated_ids_with_workspace_filter() -> Result<()> {
    fn fts_match_count_async(
        conn: &SearchSqliteConnection,
        fts_query: &str,
    ) -> Result<Option<usize>> {
        let match_mode = SearchClient::sqlite_fts_match_mode(conn)?;
        let sql = format!(
            "SELECT COUNT(*) FROM fts_messages WHERE {}",
            SearchClient::sqlite_fts5_match_clause(match_mode)
        );
        let mut params = Vec::new();
        SearchClient::push_sqlite_fts5_match_params(&mut params, fts_query, match_mode);
        match franken_query_map_collect_retry(conn, &sql, &params, |row| row.get_typed(0)) {
            Ok(rows) => {
                let count: i64 = rows.into_iter().next().unwrap_or(0);
                Ok(Some(usize::try_from(count.max(0)).unwrap_or(usize::MAX)))
            }
            Err(err) if err.to_string().contains("no such function: MATCH/2") => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    fn fts_match_count_raw(conn: &FrankenConnection, fts_query: &str) -> Result<Option<usize>> {
        let probe_params = [ParamValue::from("__cass_fts_probe_no_match__")];
        let match_mode = match conn.query_map_collect(
            "SELECT COUNT(*) FROM fts_messages WHERE fts_messages MATCH ?",
            &probe_params,
            |row| row.get_typed::<i64>(0),
        ) {
            Ok(_) => SqliteFtsMatchMode::Table,
            Err(err)
                if err
                    .to_string()
                    .contains("no such column: fts_messages in table fts_messages") =>
            {
                SqliteFtsMatchMode::IndexedColumns
            }
            Err(err) => return Err(err.into()),
        };
        let sql = format!(
            "SELECT COUNT(*) FROM fts_messages WHERE {}",
            SearchClient::sqlite_fts5_match_clause(match_mode)
        );
        let mut params = Vec::new();
        SearchClient::push_sqlite_fts5_match_params(&mut params, fts_query, match_mode);
        match conn.query_map_collect(&sql, &params, |row| row.get_typed::<i64>(0)) {
            Ok(rows) => {
                let count = rows.into_iter().next().unwrap_or(0);
                Ok(Some(usize::try_from(count.max(0)).unwrap_or(usize::MAX)))
            }
            Err(err) if err.to_string().contains("no such function: MATCH/2") => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("hyphenated-rusqlite-fallback.db");

    {
        let storage = FrankenStorage::open(&db_path)?;
        // Ensure the FTS shadow table is initialized so the direct
        // INSERT INTO fts_messages below can land.
        storage.ensure_search_fallback_fts_consistency()?;
        let conn = storage.raw();
        conn.execute(
            "INSERT INTO agents(id, slug, name, kind, created_at, updated_at)
             VALUES(1, 'codex', 'Codex', 'codex', 1, 1)",
        )?;
        conn.execute("INSERT INTO workspaces(id, path) VALUES(1, '/ws/alpha')")?;
        conn.execute("INSERT INTO workspaces(id, path) VALUES(2, '/ws/beta')")?;
        conn.execute(
            "INSERT INTO conversations(id, agent_id, workspace_id, source_id, origin_host, title, source_path)
             VALUES(1, 1, 1, 'local', NULL, 'alpha bead', '/tmp/alpha.jsonl')",
        )?;
        conn.execute(
            "INSERT INTO conversations(id, agent_id, workspace_id, source_id, origin_host, title, source_path)
             VALUES(2, 1, 2, 'local', NULL, 'beta bead', '/tmp/beta.jsonl')",
        )?;
        conn.execute(
            "INSERT INTO messages(id, conversation_id, idx, role, content, created_at)
             VALUES(11, 1, 0, 'user', 'Need follow-up on br-123 root cause', 100)",
        )?;
        conn.execute(
            "INSERT INTO messages(id, conversation_id, idx, role, content, created_at)
             VALUES(12, 2, 0, 'user', 'Need follow-up on br-123 user report', 101)",
        )?;
        conn.execute_compat(
            "INSERT INTO fts_messages(rowid, content, title)
             VALUES(?1, ?2, ?3)",
            &[
                ParamValue::from(11_i64),
                ParamValue::from("Need follow-up on br-123 root cause"),
                ParamValue::from("alpha bead"),
            ],
        )?;
        conn.execute_compat(
            "INSERT INTO fts_messages(rowid, content, title)
             VALUES(?1, ?2, ?3)",
            &[
                ParamValue::from(12_i64),
                ParamValue::from("Need follow-up on br-123 user report"),
                ParamValue::from("beta bead"),
            ],
        )?;
        let preclose_total_rows: i64 =
            conn.query_row_map("SELECT COUNT(*) FROM fts_messages", params![], |row| {
                row.get_typed(0)
            })?;
        assert_eq!(
            preclose_total_rows, 2,
            "freshly seeded file-backed FTS should retain the inserted rows"
        );
        let transpiled = transpile_to_fts5("br-123").expect("transpiled fallback query");
        if let Some(match_count) = fts_match_count_raw(conn, transpiled.as_str())? {
            assert_eq!(
                match_count, 2,
                "freshly seeded file-backed FTS should match the transpiled hyphenated query before reopen"
            );
        }
    }

    let client = SearchClient {
        reader: LexicalReaderSlot::default(),
        sqlite: Mutex::new(None),
        sqlite_path: Some(db_path),
        prefix_cache: Mutex::new(CacheShards::new(*CACHE_TOTAL_CAP, *CACHE_BYTE_CAP)),
        reload_on_search: true,
        strict_read_only: false,
        last_reload: Mutex::new(None),
        last_generation: Mutex::new(None),
        reload_epoch: Arc::new(AtomicU64::new(0)),
        warm_tx: None,
        _warm_handle: None,
        metrics: Metrics::default(),
        cache_namespace: format!("v{CACHE_KEY_VERSION}|schema:test"),
        semantic: Mutex::new(None),
        last_tantivy_total_count: Mutex::new(None),
        last_lexical_degrade_reason: Mutex::new(None),
        last_wildcard_fallback_skip: Mutex::new(None),
    };

    let guard = client.sqlite_guard()?;
    let conn = guard.as_ref().expect("sqlite guard should reopen file db");
    let reopened_total_rows: i64 =
        conn.query_row_map("SELECT COUNT(*) FROM fts_messages", params![], |row| {
            row.get_typed(0)
        })?;
    assert_eq!(
        reopened_total_rows, 2,
        "reopened file-backed FTS should still contain the seeded rows"
    );
    let transpiled = transpile_to_fts5("br-123").expect("transpiled fallback query");
    if let Some(match_count) = fts_match_count_async(conn, transpiled.as_str())? {
        assert_eq!(
            match_count, 2,
            "reopened file-backed FTS should still match the transpiled hyphenated query"
        );
    }
    drop(guard);

    let all_hits = client.search("br-123", SearchFilters::default(), 10, 0, FieldMask::FULL)?;
    assert_eq!(all_hits.len(), 2);
    assert!(
        all_hits.iter().all(|hit| hit.content.contains("br-123")),
        "hyphenated bead IDs should survive the file-backed sqlite fallback path"
    );

    let leading_or_hits = client.search(
        "OR br-123",
        SearchFilters::default(),
        10,
        0,
        FieldMask::FULL,
    )?;
    assert_eq!(leading_or_hits.len(), 2);

    let dotted_hits = client.search(
        "br-123.jsonl",
        SearchFilters::default(),
        10,
        0,
        FieldMask::FULL,
    )?;
    // k0lo1: `jsonl` exists only in source_path, which is a filter field,
    // not query text — the SQLite lane must agree with the primary index
    // (title/content only) and report no hits rather than a metadata match.
    assert_eq!(dotted_hits.len(), 0);

    let dotted_prefix_hits = client.search(
        "br-123.json*",
        SearchFilters::default(),
        10,
        0,
        FieldMask::FULL,
    )?;
    assert_eq!(dotted_prefix_hits.len(), 0);

    let prefix_hits = client.search("br-12*", SearchFilters::default(), 10, 0, FieldMask::FULL)?;
    assert_eq!(prefix_hits.len(), 2);

    let filtered_hits = client.search(
        "br-123",
        SearchFilters {
            workspaces: HashSet::from_iter(["/ws/beta".to_string()]),
            ..SearchFilters::default()
        },
        10,
        0,
        FieldMask::FULL,
    )?;
    assert_eq!(filtered_hits.len(), 1);
    assert_eq!(filtered_hits[0].workspace, "/ws/beta");
    assert_eq!(filtered_hits[0].source_path, "/tmp/beta.jsonl");
    assert!(filtered_hits[0].content.contains("br-123"));

    Ok(())
}

#[test]
fn sqlite_backend_orders_hits_by_bm25_score() -> Result<()> {
    let conn = SearchSqliteFixture::in_memory()?;
    conn.execute_batch(
        "CREATE TABLE conversations (
            id INTEGER PRIMARY KEY,
            agent_id INTEGER,
            workspace_id INTEGER,
            source_id TEXT,
            origin_host TEXT,
            title TEXT,
            source_path TEXT
         );
         CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            conversation_id INTEGER,
            idx INTEGER,
            content TEXT,
            created_at INTEGER
         );
         CREATE TABLE sources (id TEXT PRIMARY KEY, kind TEXT);
         CREATE TABLE agents (id INTEGER PRIMARY KEY, slug TEXT NOT NULL UNIQUE);
         CREATE TABLE workspaces (id INTEGER PRIMARY KEY, path TEXT NOT NULL UNIQUE);
         CREATE VIRTUAL TABLE fts_messages USING fts5(
            content,
            title,
            agent,
            workspace,
            source_path,
            created_at UNINDEXED,
            content='',
            tokenize='porter'
         );",
    )?;
    conn.execute("INSERT INTO sources(id, kind) VALUES('local', 'local')")?;
    conn.execute("INSERT INTO agents(id, slug) VALUES(1, 'codex')")?;
    conn.execute("INSERT INTO workspaces(id, path) VALUES(1, '/ws')")?;
    conn.execute(
        "INSERT INTO conversations(id, agent_id, workspace_id, source_id, origin_host, title, source_path) VALUES(1, 1, 1, 'local', NULL, 'best', '/tmp/best.jsonl')",
    )?;
    conn.execute(
        "INSERT INTO conversations(id, agent_id, workspace_id, source_id, origin_host, title, source_path) VALUES(2, 1, 1, 'local', NULL, 'worse', '/tmp/worse.jsonl')",
    )?;
    conn.execute("INSERT INTO messages(id, conversation_id, idx, content, created_at) VALUES(7, 1, 0, 'auth auth auth failure', 42)")?;
    conn.execute("INSERT INTO messages(id, conversation_id, idx, content, created_at) VALUES(8, 2, 0, 'auth failure', 43)")?;
    conn.execute_compat(
        "INSERT INTO fts_messages(rowid, content, title, agent, workspace, source_path, created_at)
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            7_i64,
            "auth auth auth failure",
            "best",
            "codex",
            "/ws",
            "/tmp/best.jsonl",
            42_i64
        ],
    )?;
    conn.execute_compat(
        "INSERT INTO fts_messages(rowid, content, title, agent, workspace, source_path, created_at)
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            8_i64,
            "auth failure",
            "worse",
            "codex",
            "/ws",
            "/tmp/worse.jsonl",
            43_i64
        ],
    )?;
    let client = SearchClient {
        reader: LexicalReaderSlot::default(),
        sqlite: Mutex::new(Some(conn.into_connection())),
        sqlite_path: None,
        prefix_cache: Mutex::new(CacheShards::new(*CACHE_TOTAL_CAP, *CACHE_BYTE_CAP)),
        reload_on_search: true,
        strict_read_only: false,
        last_reload: Mutex::new(None),
        last_generation: Mutex::new(None),
        reload_epoch: Arc::new(AtomicU64::new(0)),
        warm_tx: None,
        _warm_handle: None,
        metrics: Metrics::default(),
        cache_namespace: format!("v{CACHE_KEY_VERSION}|schema:test"),
        semantic: Mutex::new(None),
        last_tantivy_total_count: Mutex::new(None),
        last_lexical_degrade_reason: Mutex::new(None),
        last_wildcard_fallback_skip: Mutex::new(None),
    };
    let direct_hits = client.search_sqlite_fts5(
        Path::new(":memory:"),
        "auth",
        SearchFilters::default(),
        5,
        0,
        FieldMask::FULL,
    )?;
    assert_eq!(direct_hits.len(), 2);

    let hits = client.search("auth", SearchFilters::default(), 5, 0, FieldMask::FULL)?;
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].title, "best");
    assert_eq!(hits[1].title, "worse");
    assert!(hits[0].score > hits[1].score);

    Ok(())
}

#[test]
fn sqlite_fts5_ranked_phase_defers_content_decode_until_after_limit() {
    let (rank_sql, params) = SearchClient::sqlite_fts5_rank_query(
        "auth",
        &SearchFilters::default(),
        50,
        0,
        false,
        SqliteFtsMatchMode::Table,
    );
    let hydrate_sql =
        SearchClient::sqlite_fts5_hydrate_query(2, FieldMask::new(true, true, true, true), false);

    assert!(
        !rank_sql.contains("fts_messages.content"),
        "rank query must not decode large content rows before LIMIT"
    );
    assert!(
        hydrate_sql.contains("fts_messages.content"),
        "hydration query should still provide requested content"
    );
    assert!(
        rank_sql.contains("LIMIT ? OFFSET ?"),
        "rank query must apply page bounds before hydration"
    );
    assert_eq!(params.len(), 3, "fts query plus limit and offset params");
}

#[test]
fn sqlite_fts5_hydration_chunks_stay_below_bind_variable_limit() {
    let oversized_row_count = SQLITE_MAX_VARIABLE_NUMBER + 1;
    let unchunked_sql = SearchClient::sqlite_fts5_hydrate_query(
        oversized_row_count,
        FieldMask::new(true, true, true, true),
        false,
    );
    assert!(
        unchunked_sql.matches('?').count() > SQLITE_MAX_VARIABLE_NUMBER,
        "the pre-fix one-shot hydration query would exceed SQLite's bind limit"
    );

    let ranked_rows: Vec<(i64, f64)> = (0..(SQLITE_FTS5_HYDRATE_PARAM_CHUNK + 17))
        .map(|idx| (idx as i64, idx as f64))
        .collect();
    let chunk_sizes: Vec<usize> = SearchClient::sqlite_fts5_hydrate_row_chunks(&ranked_rows)
        .map(<[(i64, f64)]>::len)
        .collect();

    assert_eq!(
        chunk_sizes,
        vec![SQLITE_FTS5_HYDRATE_PARAM_CHUNK, 17],
        "large fallback pages must hydrate in bounded chunks while preserving rank windows"
    );
    assert!(
        chunk_sizes
            .iter()
            .all(|chunk_size| *chunk_size <= SQLITE_MAX_VARIABLE_NUMBER),
        "every hydration chunk must fit under SQLite's bind-variable ceiling"
    );
}

#[test]
fn tantivy_fallback_hydration_narrows_by_normalized_source_before_message_lookup() -> Result<()> {
    let conn = SearchSqliteFixture::in_memory()?;
    conn.execute_batch(
        "CREATE TABLE conversations (
            id INTEGER PRIMARY KEY,
            source_id TEXT,
            origin_host TEXT,
            source_path TEXT NOT NULL
         );
         CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            conversation_id INTEGER NOT NULL,
            idx INTEGER NOT NULL,
            content TEXT NOT NULL,
            UNIQUE(conversation_id, idx)
         );
         CREATE TABLE sources (id TEXT PRIMARY KEY, kind TEXT);",
    )?;
    conn.execute(
        "INSERT INTO conversations(id, source_id, origin_host, source_path)
         VALUES(1, '', 'devbox', '/tmp/shared-fallback.jsonl')",
    )?;
    conn.execute(
        "INSERT INTO conversations(id, source_id, origin_host, source_path)
         VALUES(2, 'local', NULL, '/tmp/shared-fallback.jsonl')",
    )?;
    conn.execute(
        "INSERT INTO messages(id, conversation_id, idx, content)
         VALUES(10, 1, 2, 'remote fallback content')",
    )?;
    conn.execute(
        "INSERT INTO messages(id, conversation_id, idx, content)
         VALUES(20, 2, 2, 'local content must not win')",
    )?;

    let client = SearchClient {
        reader: LexicalReaderSlot::default(),
        sqlite: Mutex::new(Some(conn.into_connection())),
        sqlite_path: None,
        prefix_cache: Mutex::new(CacheShards::new(*CACHE_TOTAL_CAP, *CACHE_BYTE_CAP)),
        reload_on_search: true,
        strict_read_only: false,
        last_reload: Mutex::new(None),
        last_generation: Mutex::new(None),
        reload_epoch: Arc::new(AtomicU64::new(0)),
        warm_tx: None,
        _warm_handle: None,
        metrics: Metrics::default(),
        cache_namespace: format!("v{CACHE_KEY_VERSION}|schema:test"),
        semantic: Mutex::new(None),
        last_tantivy_total_count: Mutex::new(None),
        last_lexical_degrade_reason: Mutex::new(None),
        last_wildcard_fallback_skip: Mutex::new(None),
    };

    let fallback_key = (
        "devbox".to_string(),
        "/tmp/shared-fallback.jsonl".to_string(),
        2,
    );
    let (_, hydrated_fallback) =
        client.hydrate_tantivy_hit_contents(&[], std::slice::from_ref(&fallback_key))?;

    assert_eq!(
        hydrated_fallback.get(&fallback_key).map(String::as_str),
        Some("remote fallback content")
    );

    Ok(())
}

#[test]
fn exact_content_hydration_returns_only_requested_message_indices() -> Result<()> {
    let conn = SearchSqliteFixture::in_memory()?;
    conn.execute_batch(
        "CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            conversation_id INTEGER NOT NULL,
            idx INTEGER NOT NULL,
            content TEXT NOT NULL,
            UNIQUE(conversation_id, idx)
         );",
    )?;

    for idx in 0..8 {
        conn.execute(&format!(
            "INSERT INTO messages(conversation_id, idx, content)
             VALUES(1, {idx}, 'conversation one row {idx}')"
        ))?;
    }
    conn.execute(
        "INSERT INTO messages(conversation_id, idx, content)
         VALUES(2, 0, 'conversation two row 0')",
    )?;

    let hydrated =
        hydrate_message_content_by_conversation(&conn, &[(1, 6), (1, 2), (2, 0), (1, 99)])?;

    assert_eq!(hydrated.len(), 3);
    assert_eq!(
        hydrated.get(&(1, 2)).map(String::as_str),
        Some("conversation one row 2")
    );
    assert_eq!(
        hydrated.get(&(1, 6)).map(String::as_str),
        Some("conversation one row 6")
    );
    assert_eq!(
        hydrated.get(&(2, 0)).map(String::as_str),
        Some("conversation two row 0")
    );
    assert!(!hydrated.contains_key(&(1, 99)));

    Ok(())
}

#[test]
fn sqlite_backend_generates_snippet_from_content() -> Result<()> {
    let conn = SearchSqliteFixture::in_memory()?;
    conn.execute_batch(
        "CREATE TABLE conversations (
            id INTEGER PRIMARY KEY,
            agent_id INTEGER,
            workspace_id INTEGER,
            source_id TEXT,
            origin_host TEXT,
            title TEXT,
            source_path TEXT
         );
         CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            conversation_id INTEGER,
            idx INTEGER,
            content TEXT,
            created_at INTEGER
         );
         CREATE TABLE sources (id TEXT PRIMARY KEY, kind TEXT);
         CREATE TABLE agents (id INTEGER PRIMARY KEY, slug TEXT NOT NULL UNIQUE);
         CREATE TABLE workspaces (id INTEGER PRIMARY KEY, path TEXT NOT NULL UNIQUE);
         CREATE VIRTUAL TABLE fts_messages USING fts5(
            content,
            title,
            agent,
            workspace,
            source_path,
            created_at UNINDEXED,
            content='',
            tokenize='porter'
         );",
    )?;
    conn.execute("INSERT INTO sources(id, kind) VALUES('local', 'local')")?;
    conn.execute("INSERT INTO agents(id, slug) VALUES(1, 'codex')")?;
    conn.execute("INSERT INTO workspaces(id, path) VALUES(1, '/ws')")?;
    conn.execute(
        "INSERT INTO conversations(id, agent_id, workspace_id, source_id, origin_host, title, source_path) VALUES(1, 1, 1, 'local', NULL, 'snippet title', '/tmp/snippet.jsonl')",
    )?;
    conn.execute("INSERT INTO messages(id, conversation_id, idx, content, created_at) VALUES(1, 1, 0, 'alpha beta gamma delta epsilon zeta eta theta', 42)")?;
    conn.execute_compat(
        "INSERT INTO fts_messages(rowid, content, title, agent, workspace, source_path, created_at)
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            1_i64,
            "alpha beta gamma delta epsilon zeta eta theta",
            "snippet title",
            "codex",
            "/ws",
            "/tmp/snippet.jsonl",
            42_i64
        ],
    )?;

    let client = SearchClient {
        reader: LexicalReaderSlot::default(),
        sqlite: Mutex::new(Some(conn.into_connection())),
        sqlite_path: None,
        prefix_cache: Mutex::new(CacheShards::new(*CACHE_TOTAL_CAP, *CACHE_BYTE_CAP)),
        reload_on_search: true,
        strict_read_only: false,
        last_reload: Mutex::new(None),
        last_generation: Mutex::new(None),
        reload_epoch: Arc::new(AtomicU64::new(0)),
        warm_tx: None,
        _warm_handle: None,
        metrics: Metrics::default(),
        cache_namespace: format!("v{CACHE_KEY_VERSION}|schema:test"),
        semantic: Mutex::new(None),
        last_tantivy_total_count: Mutex::new(None),
        last_lexical_degrade_reason: Mutex::new(None),
        last_wildcard_fallback_skip: Mutex::new(None),
    };

    let hits = client.search("delta", SearchFilters::default(), 5, 0, FieldMask::FULL)?;
    assert_eq!(hits.len(), 1);
    // With contentless FTS5, snippet is built from messages.content with term highlighting.
    assert_eq!(
        hits[0].snippet,
        super::super::fusion::snippet_from_content_for_query(&hits[0].content, "delta")
    );
    assert!(hits[0].snippet.contains("**delta**"));

    Ok(())
}

#[test]
fn sqlite_schema_v1_contentless_fts5_ranks_bm25_and_highlights_snippet() -> Result<()> {
    let dir = TempDir::new()?;
    let db_path = dir.path().join("schema-v1-fts5.db");
    let storage = FrankenStorage::open(&db_path)?;
    let agent_id = storage.ensure_agent(&Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    let workspace_id = storage.ensure_workspace(Path::new("/tmp/schema-v1-ws"), None)?;
    let make_conv =
        |external_id: &str, title: &str, source_path: &str, content: &str| Conversation {
            id: None,
            agent_slug: "codex".into(),
            workspace: Some(PathBuf::from("/tmp/schema-v1-ws")),
            external_id: Some(external_id.into()),
            title: Some(title.into()),
            source_path: PathBuf::from(source_path),
            started_at: Some(1_700_000_000_000),
            ended_at: Some(1_700_000_000_100),
            approx_tokens: None,
            metadata_json: serde_json::Value::Null,
            messages: vec![Message {
                id: None,
                idx: 0,
                role: MessageRole::User,
                author: Some("user".into()),
                created_at: Some(1_700_000_000_050),
                content: content.into(),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            }],
            source_id: "local".into(),
            origin_host: None,
        };
    storage.insert_conversation_tree(
        agent_id,
        Some(workspace_id),
        &make_conv(
            "bm25-best",
            "dense bm25 hit",
            "/tmp/bm25-best.jsonl",
            "needle appears needle once and needle again in schema v1 contentless body",
        ),
    )?;
    storage.insert_conversation_tree(
        agent_id,
        Some(workspace_id),
        &make_conv(
            "bm25-other",
            "single bm25 hit",
            "/tmp/bm25-other.jsonl",
            "prefix context before a single needle occurrence in schema v1 contentless body",
        ),
    )?;
    storage.rebuild_fts()?;
    drop(storage);

    let client = SearchClient {
        reader: LexicalReaderSlot::default(),
        sqlite: Mutex::new(None),
        sqlite_path: Some(db_path.clone()),
        prefix_cache: Mutex::new(CacheShards::new(*CACHE_TOTAL_CAP, *CACHE_BYTE_CAP)),
        reload_on_search: true,
        strict_read_only: false,
        last_reload: Mutex::new(None),
        last_generation: Mutex::new(None),
        reload_epoch: Arc::new(AtomicU64::new(0)),
        warm_tx: None,
        _warm_handle: None,
        metrics: Metrics::default(),
        cache_namespace: format!("v{CACHE_KEY_VERSION}|schema:test"),
        semantic: Mutex::new(None),
        last_tantivy_total_count: Mutex::new(None),
        last_lexical_degrade_reason: Mutex::new(None),
        last_wildcard_fallback_skip: Mutex::new(None),
    };

    let direct_hits = client.search_sqlite_fts5(
        &db_path,
        "needle",
        SearchFilters::default(),
        5,
        0,
        FieldMask::FULL,
    )?;
    assert_eq!(direct_hits.len(), 2);
    assert_eq!(direct_hits[0].title, "dense bm25 hit");
    assert_eq!(direct_hits[1].title, "single bm25 hit");
    assert!(
        direct_hits[0].score > direct_hits[1].score,
        "native bm25(fts_messages) must rank higher term-frequency match first: {} vs {}",
        direct_hits[0].score,
        direct_hits[1].score
    );
    assert!(!direct_hits[0].snippet.is_empty());
    assert!(direct_hits[0].snippet.contains("**needle**"));
    assert!(direct_hits[1].snippet.contains("**needle**"));
    assert_eq!(direct_hits[0].agent, "codex");
    assert_eq!(direct_hits[0].workspace, "/tmp/schema-v1-ws");
    assert_eq!(direct_hits[0].source_path, "/tmp/bm25-best.jsonl");

    let snippet_only_hits = client.search(
        "needle",
        SearchFilters::default(),
        5,
        0,
        FieldMask::new(true, true, false, false),
    )?;
    assert_eq!(snippet_only_hits.len(), 2);
    assert!(!snippet_only_hits[0].snippet.is_empty());
    assert!(snippet_only_hits[0].snippet.contains("**needle**"));

    Ok(())
}
