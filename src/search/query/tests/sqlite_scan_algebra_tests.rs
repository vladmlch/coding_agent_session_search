use super::super::*;
use super::helpers::*;
use crate::franken_sync::compat::RowExt;

#[test]
fn gh395_browse_by_date_respects_source_filter() -> Result<()> {
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
            source_path TEXT,
            started_at INTEGER,
            ended_at INTEGER,
            last_message_idx INTEGER,
            last_message_created_at INTEGER
         );
         CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            conversation_id INTEGER,
            idx INTEGER,
            content TEXT,
            created_at INTEGER,
            UNIQUE(conversation_id, idx)
         );
         CREATE TABLE conversation_tail_state (
            conversation_id INTEGER PRIMARY KEY,
            ended_at INTEGER,
            last_message_idx INTEGER,
            last_message_created_at INTEGER
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
    conn.execute("INSERT INTO sources(id, kind) VALUES('laptop', 'ssh')")?;
    conn.execute("INSERT INTO agents(id, slug) VALUES(1, 'codex')")?;
    conn.execute("INSERT INTO workspaces(id, path) VALUES(1, '/local')")?;
    conn.execute("INSERT INTO workspaces(id, path) VALUES(2, '/remote')")?;
    conn.execute(
        "INSERT INTO conversations(id, agent_id, workspace_id, source_id, origin_host, title, source_path) VALUES(1, 1, 1, '  local  ', NULL, 'local title', '/tmp/local.jsonl')",
    )?;
    conn.execute("INSERT INTO conversations(id, agent_id, workspace_id, source_id, origin_host, title, source_path) VALUES(2, 1, 2, 'laptop', 'dev@laptop', 'remote title', '/tmp/remote.jsonl')")?;
    conn.execute("INSERT INTO messages(id, conversation_id, idx, content, created_at) VALUES(1, 1, 0, 'auth token failure', 42)")?;
    conn.execute("INSERT INTO messages(id, conversation_id, idx, content, created_at) VALUES(2, 2, 0, 'auth token failure', 43)")?;
    conn.execute(
        "INSERT INTO conversation_tail_state(
             conversation_id, ended_at, last_message_idx, last_message_created_at
         ) VALUES
            (1, 42, 0, 42),
            (2, 43, 0, 43)",
    )?;
    conn.execute_compat(
        "INSERT INTO fts_messages(rowid, content, title, agent, workspace, source_path, created_at)
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            1_i64,
            "auth token failure",
            "local title",
            "codex",
            "/local",
            "/tmp/local.jsonl",
            42_i64
        ],
    )?;
    conn.execute_compat(
        "INSERT INTO fts_messages(rowid, content, title, agent, workspace, source_path, created_at)
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            2_i64,
            "auth token failure",
            "remote title",
            "codex",
            "/remote",
            "/tmp/remote.jsonl",
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

    let local_hits = client.browse_by_date(
        SearchFilters {
            source_filter: SourceFilter::Local,
            ..SearchFilters::default()
        },
        5,
        0,
        true,
        FieldMask::FULL,
    )?;
    assert_eq!(local_hits.len(), 1);
    assert_eq!(local_hits[0].source_id, "local");

    let remote_hits = client.browse_by_date(
        SearchFilters {
            source_filter: SourceFilter::SourceId("  LOCAL  ".to_string()),
            ..SearchFilters::default()
        },
        5,
        0,
        true,
        FieldMask::FULL,
    )?;
    assert_eq!(remote_hits.len(), 1);
    assert_eq!(remote_hits[0].source_id, "local");
    assert_eq!(remote_hits[0].origin_kind, "local");

    Ok(())
}

#[test]
fn sqlite_backend_remote_source_filter_matches_blank_source_id_with_origin_host() -> Result<()> {
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
    conn.execute("INSERT INTO agents(id, slug) VALUES(1, 'codex')")?;
    conn.execute(
        "INSERT INTO conversations(id, agent_id, workspace_id, source_id, origin_host, title, source_path)
         VALUES(1, 1, NULL, '   ', 'dev@laptop', 'remote title', '/tmp/remote-filter.jsonl')",
    )?;
    conn.execute(
        "INSERT INTO messages(id, conversation_id, idx, content, created_at)
         VALUES(1, 1, 0, 'remote filter proof', 42)",
    )?;
    conn.execute_compat(
        "INSERT INTO fts_messages(rowid, content, title, agent, workspace, source_path, created_at)
         VALUES(?1, ?2, ?3, ?4, NULL, ?5, ?6)",
        params![
            1_i64,
            "remote filter proof",
            "remote title",
            "codex",
            "/tmp/remote-filter.jsonl",
            42_i64
        ],
    )?;
    // A *named* local-kind source (backup root / chatgpt-import): local by
    // kind even though its id is not "local" (bead 5bf29).
    conn.execute("INSERT INTO sources(id, kind) VALUES('backup-local', 'local')")?;
    conn.execute(
        "INSERT INTO conversations(id, agent_id, workspace_id, source_id, origin_host, title, source_path)
         VALUES(2, 1, NULL, 'backup-local', NULL, 'backup title', '/tmp/backup-filter.jsonl')",
    )?;
    conn.execute(
        "INSERT INTO messages(id, conversation_id, idx, content, created_at)
         VALUES(2, 2, 0, 'remote filter proof from backup', 43)",
    )?;
    conn.execute_compat(
        "INSERT INTO fts_messages(rowid, content, title, agent, workspace, source_path, created_at)
         VALUES(?1, ?2, ?3, ?4, NULL, ?5, ?6)",
        params![
            2_i64,
            "remote filter proof from backup",
            "backup title",
            "codex",
            "/tmp/backup-filter.jsonl",
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

    let remote_hits = client.search(
        "remote",
        SearchFilters {
            source_filter: SourceFilter::Remote,
            ..Default::default()
        },
        5,
        0,
        FieldMask::FULL,
    )?;
    assert_eq!(remote_hits.len(), 1);
    assert_eq!(remote_hits[0].source_id, "dev@laptop");
    assert_eq!(remote_hits[0].origin_kind, "remote");
    assert_eq!(remote_hits[0].origin_host.as_deref(), Some("dev@laptop"));

    let source_hits = client.search(
        "remote",
        SearchFilters {
            source_filter: SourceFilter::SourceId("dev@laptop".into()),
            ..Default::default()
        },
        5,
        0,
        FieldMask::FULL,
    )?;
    assert_eq!(source_hits.len(), 1);
    assert_eq!(source_hits[0].source_id, "dev@laptop");
    assert_eq!(source_hits[0].origin_kind, "remote");

    let local_hits = client.search(
        "remote",
        SearchFilters {
            source_filter: SourceFilter::Local,
            ..Default::default()
        },
        5,
        0,
        FieldMask::FULL,
    )?;
    assert_eq!(local_hits.len(), 1, "the named local-kind source is local");
    assert_eq!(local_hits[0].source_id, "backup-local");
    assert_eq!(local_hits[0].origin_kind, "local");

    Ok(())
}

#[test]
fn sqlite_backend_workspace_filter_matches_null_workspace_as_empty_string() -> Result<()> {
    assert_sqlite_null_workspace_overrides_shadow(true)
}

#[test]
fn gh459_sqlite_null_workspace_overrides_legacy_content_bearing_shadow() -> Result<()> {
    assert_sqlite_null_workspace_overrides_shadow(false)
}

fn assert_sqlite_null_workspace_overrides_shadow(contentless: bool) -> Result<()> {
    let conn = SearchSqliteFixture::in_memory()?;
    let content_option = if contentless { "content=''," } else { "" };
    conn.execute_batch(&format!(
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
            {content_option}
            tokenize='porter'
         );",
    ))?;
    conn.execute("INSERT INTO sources(id, kind) VALUES('local', 'local')")?;
    conn.execute("INSERT INTO agents(id, slug) VALUES(1, 'codex')")?;
    conn.execute("INSERT INTO workspaces(id, path) VALUES(1, '/named')")?;
    // Conversation 1: no workspace (workspace_id=NULL)
    conn.execute(
        "INSERT INTO conversations(id, agent_id, workspace_id, source_id, origin_host, title, source_path) VALUES(1, 1, NULL, 'local', NULL, 'null workspace', '/tmp/null-workspace.jsonl')",
    )?;
    // Conversation 2: with workspace
    conn.execute(
        "INSERT INTO conversations(id, agent_id, workspace_id, source_id, origin_host, title, source_path) VALUES(2, 1, 1, 'local', NULL, 'named workspace', '/tmp/named-workspace.jsonl')",
    )?;
    conn.execute("INSERT INTO messages(id, conversation_id, idx, content, created_at) VALUES(1, 1, 0, 'auth token failure', 42)")?;
    conn.execute("INSERT INTO messages(id, conversation_id, idx, content, created_at) VALUES(2, 2, 0, 'auth token failure', 43)")?;
    conn.execute_compat(
        "INSERT INTO fts_messages(rowid, content, title, agent, workspace, source_path, created_at)
         VALUES(?1, ?2, ?3, ?4, '/old-guessed-workspace', ?5, ?6)",
        params![
            1_i64,
            "auth token failure",
            "null workspace",
            "codex",
            "/tmp/null-workspace.jsonl",
            42_i64
        ],
    )?;
    conn.execute_compat(
        "INSERT INTO fts_messages(rowid, content, title, agent, workspace, source_path, created_at)
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            2_i64,
            "auth token failure",
            "named workspace",
            "codex",
            "/named",
            "/tmp/named-workspace.jsonl",
            43_i64
        ],
    )?;
    let stored_workspace: Option<String> = conn.connection().query_row_map(
        "SELECT workspace FROM fts_messages WHERE rowid = 1",
        &[],
        |row| row.get_typed(0),
    )?;
    assert_eq!(
        stored_workspace.as_deref(),
        if contentless {
            Some("")
        } else {
            Some("/old-guessed-workspace")
        },
        "the shadow must retain obsolete attribution only when it stores content",
    );

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

    let hits = client.search(
        "auth",
        SearchFilters {
            workspaces: HashSet::from_iter([String::new()]),
            ..SearchFilters::default()
        },
        5,
        0,
        FieldMask::FULL,
    )?;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].workspace, "");
    assert_eq!(hits[0].source_path, "/tmp/null-workspace.jsonl");

    let stale_hits = client.search(
        "auth",
        SearchFilters {
            workspaces: HashSet::from_iter(["/old-guessed-workspace".to_string()]),
            ..SearchFilters::default()
        },
        5,
        0,
        FieldMask::FULL,
    )?;
    assert!(
        stale_hits.is_empty(),
        "canonical NULL must not resurrect the shadow workspace"
    );
    let all_hits = client.search("auth", SearchFilters::default(), 5, 0, FieldMask::FULL)?;
    assert_eq!(
        all_hits.len(),
        2,
        "workspace repair must retain both messages"
    );
    assert!(all_hits.iter().any(|hit| hit.workspace == "/named"));

    Ok(())
}

/// 2l1b0.52: the scan lane evaluates the lexical engine's standard
/// grammar (NOT, then AND, then OR; parentheses group). Negative controls:
/// the legacy OR-binds-tighter reading rejected "beta gamma" for the first
/// mixed query and "alpha" for the second.
#[test]
fn sqlite_message_scan_follows_standard_boolean_precedence() {
    fn score(haystack: &str, query: &SqliteMessageScanQuery) -> f32 {
        SearchClient::sqlite_message_scan_score(&[haystack.to_lowercase()], query)
    }

    let simple_or =
        SearchClient::sqlite_message_scan_query("alpha OR beta").expect("simple OR scan query");
    assert!(score("alpha", &simple_or) > 0.0);
    assert!(score("beta", &simple_or) > 0.0);
    assert_eq!(score("gamma", &simple_or), 0.0);

    // (alpha AND beta) OR gamma
    let and_then_or = SearchClient::sqlite_message_scan_query("alpha AND beta OR gamma")
        .expect("AND followed by OR scan query");
    assert!(score("alpha beta", &and_then_or) > 0.0);
    assert!(score("beta gamma", &and_then_or) > 0.0);
    assert_eq!(score("alpha", &and_then_or), 0.0);

    // alpha OR (beta AND gamma)
    let or_then_and = SearchClient::sqlite_message_scan_query("alpha OR beta AND gamma")
        .expect("OR followed by AND scan query");
    assert!(score("alpha", &or_then_and) > 0.0);
    assert!(score("beta gamma", &or_then_and) > 0.0);
    assert_eq!(score("beta", &or_then_and), 0.0);

    let grouped = SearchClient::sqlite_message_scan_query("(alpha OR beta) AND gamma")
        .expect("grouped scan query");
    assert_eq!(score("alpha", &grouped), 0.0);
    assert!(score("alpha gamma", &grouped) > 0.0);
    assert!(score("beta gamma", &grouped) > 0.0);

    let binary_not =
        SearchClient::sqlite_message_scan_query("alpha NOT beta").expect("NOT scan query");
    assert!(score("alpha", &binary_not) > 0.0);
    assert_eq!(score("alpha beta", &binary_not), 0.0);

    let excluded_group =
        SearchClient::sqlite_message_scan_query("alpha -(beta OR gamma)").expect("NOT group");
    assert!(score("alpha", &excluded_group) > 0.0);
    assert_eq!(score("alpha gamma", &excluded_group), 0.0);
}

#[test]
fn sqlite_message_scan_matches_primary_negated_or_truth_tables() {
    fn score(haystack: &str, query: &SqliteMessageScanQuery) -> f32 {
        SearchClient::sqlite_message_scan_score(&[haystack.to_lowercase()], query)
    }

    // (alpha AND NOT beta) OR gamma
    let nested = SearchClient::sqlite_message_scan_query("alpha NOT beta OR gamma")
        .expect("nested negated OR scan query");
    assert!(score("alpha", &nested) > 0.0);
    assert_eq!(score("alpha beta", &nested), 0.0);
    assert!(score("alpha beta gamma", &nested) > 0.0);
    assert!(score("gamma", &nested) > 0.0);

    let or_not =
        SearchClient::sqlite_message_scan_query("alpha OR NOT beta").expect("OR-NOT scan query");
    assert!(score("alpha beta", &or_not) > 0.0);
    assert_eq!(score("gamma beta", &or_not), 0.0);
    assert!(score("gamma delta", &or_not) > 0.0);

    let standalone_not =
        SearchClient::sqlite_message_scan_query("NOT beta").expect("standalone NOT query");
    assert!(score("alpha", &standalone_not) > 0.0);
    assert_eq!(score("alpha beta", &standalone_not), 0.0);

    // Negation is parity-based: NOT NOT beta is beta.
    let repeated_not = SearchClient::sqlite_message_scan_query("NOT NOT beta")
        .expect("repeated standalone NOT query");
    assert_eq!(score("alpha", &repeated_not), 0.0);
    assert!(score("alpha beta", &repeated_not) > 0.0);

    // A NOT with no operand is dropped: alpha OR beta.
    let permissive = SearchClient::sqlite_message_scan_query("alpha NOT OR beta")
        .expect("permissive NOT-before-OR query");
    assert!(score("alpha beta", &permissive) > 0.0);
    assert!(score("gamma beta", &permissive) > 0.0);
    assert_eq!(score("gamma delta", &permissive), 0.0);
}

/// Set-algebra reference for the fallback-lane differential (2l1b0.52).
#[derive(Debug)]
enum BoolReference {
    Term(usize),
    Not(Box<BoolReference>),
    And(Vec<BoolReference>),
    Or(Vec<BoolReference>),
}

impl BoolReference {
    const TERMS: [&'static str; 4] = ["kiwiword", "limeword", "mangoword", "plumword"];

    /// Deterministic xorshift, so every run generates the same queries.
    fn next(state: &mut u64) -> u64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }

    fn generate(state: &mut u64, depth: usize) -> Self {
        let roll = Self::next(state) % 10;
        if depth == 0 || roll < 3 {
            return Self::Term((Self::next(state) % Self::TERMS.len() as u64) as usize);
        }
        let children = |state: &mut u64| -> Vec<Self> {
            let count = 2 + (Self::next(state) % 2) as usize;
            (0..count)
                .map(|_| Self::generate(state, depth - 1))
                .collect()
        };
        match roll {
            3 | 4 => Self::Not(Box::new(Self::generate(state, depth - 1))),
            5..=7 => Self::And(children(state)),
            _ => Self::Or(children(state)),
        }
    }

    fn matches(&self, row: &[bool]) -> bool {
        match self {
            Self::Term(term) => row[*term],
            Self::Not(inner) => !inner.matches(row),
            Self::And(children) => children.iter().all(|child| child.matches(row)),
            Self::Or(children) => children.iter().any(|child| child.matches(row)),
        }
    }

    /// Query text that the standard grammar parses back into `self`, with
    /// the operator spelling (AND, `&&` or implied; OR or `||`; NOT or
    /// `-`) and any optional grouping chosen by `state`.
    fn render(&self, state: &mut u64) -> String {
        match self {
            Self::Term(term) => Self::TERMS[*term].to_string(),
            Self::Not(inner) => {
                let operand = inner.render_operand(state, true);
                if matches!(**inner, Self::Not(_)) || Self::next(state).is_multiple_of(2) {
                    format!("NOT {operand}")
                } else {
                    format!("-{operand}")
                }
            }
            Self::And(children) => {
                let separator = [" AND ", " && ", " "][(Self::next(state) % 3) as usize];
                children
                    .iter()
                    .map(|child| child.render_operand(state, matches!(child, Self::Or(_))))
                    .collect::<Vec<_>>()
                    .join(separator)
            }
            Self::Or(children) => {
                let separator = [" OR ", " || "][(Self::next(state) % 2) as usize];
                children
                    .iter()
                    .map(|child| child.render_operand(state, false))
                    .collect::<Vec<_>>()
                    .join(separator)
            }
        }
    }

    /// `self` as an operand: a compound expression is parenthesized when
    /// precedence requires it, and at random otherwise.
    fn render_operand(&self, state: &mut u64, required: bool) -> String {
        let compound = matches!(self, Self::And(_) | Self::Or(_));
        let text = self.render(state);
        if compound && (required || Self::next(state).is_multiple_of(2)) {
            format!("({text})")
        } else {
            text
        }
    }
}

/// 2l1b0.52: both fallback lanes, the SQLite FTS5 entry point and the
/// source-table scan, return exactly the set a set-algebra reference
/// computes for generated Boolean queries in every operator spelling,
/// with and without redundant grouping, including complements. The FTS5
/// entry point hands queries FTS5 cannot express to the scan, so the run
/// must exercise both routes. The legacy grammar (OR tighter than AND,
/// parentheses ignored) fails most mixed cases.
#[test]
fn fallback_lanes_match_set_algebra_for_generated_boolean_queries() -> Result<()> {
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
         );
         INSERT INTO sources(id, kind) VALUES('local', 'local');
         INSERT INTO agents(id, slug) VALUES(1, 'codex');
         INSERT INTO workspaces(id, path) VALUES(1, '/ws');
         INSERT INTO conversations(
            id, agent_id, workspace_id, source_id, origin_host, title, source_path
         ) VALUES(1, 1, 1, 'local', NULL, 'fixture', '/tmp/fixture.jsonl');",
    )?;
    let mut state = 0x21B0_0052_u64;
    let mut rows: Vec<Vec<bool>> = Vec::new();
    for id in 1..=48_i64 {
        let row: Vec<bool> = BoolReference::TERMS
            .iter()
            .map(|_| BoolReference::next(&mut state) % 100 < 45)
            .collect();
        let mut words = vec![format!("msgid{id}")];
        words.extend(
            BoolReference::TERMS
                .iter()
                .zip(&row)
                .filter(|(_, present)| **present)
                .map(|(term, _)| (*term).to_string()),
        );
        let content = words.join(" ");
        conn.execute_compat(
            "INSERT INTO messages(id, conversation_id, idx, content, created_at)
             VALUES(?1, 1, ?2, ?3, ?1)",
            params![id, id - 1, content.as_str()],
        )?;
        conn.execute_compat(
            "INSERT INTO fts_messages(rowid, content, title, agent, workspace, source_path, created_at)
             VALUES(?1, ?2, 'fixture', 'codex', '/ws', '/tmp/fixture.jsonl', ?1)",
            params![id, content.as_str()],
        )?;
        rows.push(row);
    }
    let client = cass_layer_b_test_client(Some(conn.into_connection()));
    let hit_ids = |hits: Vec<SearchHit>| -> Result<std::collections::BTreeSet<i64>> {
        hits.iter()
            .map(|hit| {
                hit.content
                    .split_whitespace()
                    .find_map(|word| word.strip_prefix("msgid"))
                    .and_then(|digits| digits.parse().ok())
                    .ok_or_else(|| anyhow!("hit without a msgid token: {:?}", hit.content))
            })
            .collect()
    };

    let (mut fts5_routed, mut scan_routed, mut proper_subsets) = (0, 0, 0);
    for case in 0..64 {
        let expr = BoolReference::generate(&mut state, 3);
        let query = expr.render_operand(&mut state, false);
        let expected: std::collections::BTreeSet<i64> = rows
            .iter()
            .zip(1_i64..)
            .filter(|(row, _)| expr.matches(row))
            .map(|(_, id)| id)
            .collect();
        if !expected.is_empty() && expected.len() < rows.len() {
            proper_subsets += 1;
        }
        let route = if transpile_to_fts5(&query).is_some() {
            fts5_routed += 1;
            "fts5"
        } else {
            scan_routed += 1;
            "scan"
        };
        let via_entry = hit_ids(client.search_sqlite_fts5(
            Path::new(":memory:"),
            &query,
            SearchFilters::default(),
            1000,
            0,
            FieldMask::FULL,
        )?)?;
        let via_scan = {
            let sqlite_guard = client.sqlite_guard()?;
            let conn = sqlite_guard
                .as_ref()
                .ok_or_else(|| anyhow!("fixture connection missing"))?;
            hit_ids(client.search_sqlite_message_scan(
                conn,
                SqliteMessageScanRequest {
                    raw_query: &query,
                    filters: &SearchFilters::default(),
                    limit: 1000,
                    offset: 0,
                    scan_page_rows: 16,
                    field_mask: FieldMask::FULL,
                    query_match_type: dominant_match_type(&query),
                },
            )?)?
        };
        eprintln!(
            "{}",
            serde_json::json!({
                "event": "fallback_differential",
                "case": case,
                "query": query,
                "route": route,
                "expected": expected.len(),
                "entry_hits": via_entry.len(),
                "scan_hits": via_scan.len(),
            })
        );
        assert_eq!(
            via_entry, expected,
            "case {case}: FTS5 entry point ({route}) for {query:?} = {expr:?}"
        );
        assert_eq!(
            via_scan, expected,
            "case {case}: source scan for {query:?} = {expr:?}"
        );
    }
    assert!(
        fts5_routed >= 16 && scan_routed >= 8,
        "both routes must be exercised: fts5={fts5_routed} scan={scan_routed}"
    );
    assert!(
        proper_subsets >= 32,
        "too few informative cases: {proper_subsets} of 64"
    );
    Ok(())
}

/// 1t79z: the scan lane honours suffix / substring / complex wildcards
/// on token boundaries, matching the primary index's regex expansion.
#[test]
fn sqlite_message_scan_matches_primary_wildcard_patterns() {
    fn score(haystack: &str, query: &SqliteMessageScanQuery) -> f32 {
        SearchClient::sqlite_message_scan_score(&[haystack.to_lowercase()], query)
    }

    let suffix = SearchClient::sqlite_message_scan_query("*handler").expect("suffix query");
    assert!(score("the error_handler fired", &suffix) > 0.0);
    assert!(score("handler", &suffix) > 0.0);
    assert_eq!(score("handlers everywhere", &suffix), 0.0);

    let substring = SearchClient::sqlite_message_scan_query("*andl*").expect("substring query");
    assert!(score("handlers everywhere", &substring) > 0.0);
    assert_eq!(score("hand over", &substring), 0.0);

    let complex = SearchClient::sqlite_message_scan_query("h*ler").expect("complex query");
    assert!(score("handler", &complex) > 0.0);
    assert!(score("hauler", &complex) > 0.0);
    assert_eq!(score("handlers", &complex), 0.0);
    assert_eq!(score("ler", &complex), 0.0);

    // Exact and prefix semantics are unchanged (the sanitizer splits
    // `error_handler` into `error` + `handler`, so exact `handler` still
    // matches it on the token boundary; `handlers` is a different token).
    let exact = SearchClient::sqlite_message_scan_query("handler").expect("exact query");
    assert!(score("handler", &exact) > 0.0);
    assert!(score("error_handler", &exact) > 0.0);
    assert_eq!(score("handlers", &exact), 0.0);
    let prefix = SearchClient::sqlite_message_scan_query("hand*").expect("prefix query");
    assert!(score("handlers", &prefix) > 0.0);
    assert_eq!(score("shand", &prefix), 0.0);

    // A bare star carries no term and yields no scan query.
    assert!(SearchClient::sqlite_message_scan_query("*").is_none());
}

#[test]
fn glob_token_matches_is_anchored_unless_the_pattern_says_otherwise() {
    assert!(glob_token_matches("h*ler", "handler"));
    assert!(!glob_token_matches("h*ler", "handlers"));
    assert!(glob_token_matches("*h*ler", "ahandler"));
    assert!(glob_token_matches("h*ler*", "handlers"));
    assert!(glob_token_matches("a*b*c", "axxbyyc"));
    assert!(glob_token_matches("a*b", "abxb"));
    assert!(!glob_token_matches("a*b*c", "acb"));
    assert!(glob_token_matches("*", "anything"));
}

#[test]
fn sqlite_message_scan_preserves_phrase_adjacency_within_one_field() {
    fn score(fields: &[&str], query: &SqliteMessageScanQuery) -> f32 {
        let fields = fields
            .iter()
            .map(|field| field.to_lowercase())
            .collect::<Vec<_>>();
        SearchClient::sqlite_message_scan_score(&fields, query)
    }

    let phrase =
        SearchClient::sqlite_message_scan_query("\"alpha beta\"").expect("phrase scan query");
    assert!(score(&["alpha beta"], &phrase) > 0.0);
    assert!(score(&["alpha, beta"], &phrase) > 0.0);
    assert_eq!(score(&["alpha x beta"], &phrase), 0.0);
    assert_eq!(
        score(&["alpha", "beta"], &phrase),
        0.0,
        "phrase must not bridge content and title"
    );

    let negated_phrase = SearchClient::sqlite_message_scan_query("NOT \"alpha beta\"")
        .expect("negated phrase scan query");
    assert_eq!(score(&["alpha beta"], &negated_phrase), 0.0);
    assert!(score(&["alpha x beta"], &negated_phrase) > 0.0);
}

#[test]
fn sqlite_message_scan_uses_token_exact_and_prefix_semantics() {
    fn score(haystack: &str, query: &SqliteMessageScanQuery) -> f32 {
        SearchClient::sqlite_message_scan_score(&[haystack.to_lowercase()], query)
    }

    let exact = SearchClient::sqlite_message_scan_query("row").expect("exact scan query");
    assert!(score("one row", &exact) > 0.0);
    assert_eq!(
        score("one arrow", &exact),
        0.0,
        "an exact term must not match inside another token"
    );

    let prefix = SearchClient::sqlite_message_scan_query("foo*").expect("prefix scan query");
    assert!(score("foobar", &prefix) > 0.0);
    assert_eq!(
        score("seafood", &prefix),
        0.0,
        "a prefix term must not degrade to an arbitrary substring"
    );
}

#[test]
fn sqlite_message_scan_pushes_all_filters_into_the_bounded_page() {
    let filters = SearchFilters {
        agents: HashSet::from(["codex".to_string()]),
        workspaces: HashSet::from(["/workspace".to_string()]),
        created_from: Some(10),
        created_to: Some(20),
        source_filter: SourceFilter::SourceId("remote-a".to_string()),
        session_paths: HashSet::from(["/tmp/session.jsonl".to_string()]),
    };
    let (sql, params) = SearchClient::sqlite_message_scan_query_sql(FieldMask::FULL, &filters, 7);
    // The page must be a bare rowid range: the query planner full-scans a
    // rowid range that shares its WHERE with anything else, which made
    // every page read the whole `messages` table.
    let page_end = sql
        .find("WHERE id <= ?) m")
        .expect("the page is a bare rowid range in a subquery");
    assert!(
        !sql.contains("LIMIT"),
        "the page is bounded by its id range, not by LIMIT: {sql}"
    );

    for predicate in [
        "COALESCE(a.slug, '') IN (?)",
        "COALESCE(w.path, '') IN (?)",
        "CAST(m.created_at AS INTEGER) >= ?",
        "CAST(m.created_at AS INTEGER) <= ?",
        "END = ?",
        "COALESCE(c.source_path, '') IN (?)",
    ] {
        let predicate_pos = sql.find(predicate).expect("source-scan filter predicate");
        assert!(
            predicate_pos > page_end,
            "filter predicate must apply outside the page subquery: {predicate}"
        );
    }
    assert_eq!(params.len(), 7, "the page bound plus six filter values");
    assert_eq!(
        params.first(),
        Some(&ParamValue::from(7_i64)),
        "the page bound binds first, as it comes first in the SQL text"
    );

    let (local_sql, _) = SearchClient::sqlite_message_scan_query_sql(
        FieldMask::FULL,
        &SearchFilters {
            source_filter: SourceFilter::Local,
            ..SearchFilters::default()
        },
        7,
    );
    let (remote_sql, _) = SearchClient::sqlite_message_scan_query_sql(
        FieldMask::FULL,
        &SearchFilters {
            source_filter: SourceFilter::Remote,
            ..SearchFilters::default()
        },
        7,
    );
    assert!(local_sql.contains("END = 'local'"));
    assert!(remote_sql.contains("END != 'local'"));
}

#[test]
fn sqlite_message_scan_filtering_precedes_low_scan_cap() -> Result<()> {
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
         INSERT INTO sources(id, kind) VALUES('remote-a', 'ssh');
         INSERT INTO agents(id, slug) VALUES(1, 'claude'), (2, 'codex');
         INSERT INTO workspaces(id, path) VALUES(1, '/excluded'), (2, '/target');
         INSERT INTO conversations(
            id, agent_id, workspace_id, source_id, origin_host, title, source_path
         ) VALUES
            (1, 1, 1, 'remote-a', 'dev@remote-a', 'excluded', '/tmp/excluded.jsonl'),
            (2, 2, 2, NULL, NULL, 'target', '/tmp/target.jsonl');
         INSERT INTO messages(id, conversation_id, idx, content, created_at) VALUES
            (1, 1, 0, 'needle in excluded row', 1),
            (2, 2, 0, 'needle in target row', 50);",
    )?;
    let filters = SearchFilters {
        agents: HashSet::from(["codex".to_string()]),
        workspaces: HashSet::from(["/target".to_string()]),
        created_from: Some(40),
        created_to: Some(60),
        source_filter: SourceFilter::SourceId("LOCAL".to_string()),
        session_paths: HashSet::from(["/tmp/target.jsonl".to_string()]),
    };
    let client = cass_layer_b_test_client(None);

    let hits = client.search_sqlite_message_scan(
        conn.connection(),
        SqliteMessageScanRequest {
            raw_query: "needle",
            filters: &filters,
            limit: 10,
            offset: 0,
            scan_page_rows: 1,
            field_mask: FieldMask::FULL,
            query_match_type: MatchType::Exact,
        },
    )?;

    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].agent, "codex");
    assert_eq!(hits[0].workspace, "/target");
    assert_eq!(hits[0].created_at, Some(50));
    assert_eq!(hits[0].source_path, "/tmp/target.jsonl");
    assert_eq!(hits[0].source_id, "local");

    let local_filters = SearchFilters {
        source_filter: SourceFilter::Local,
        ..SearchFilters::default()
    };
    let local_hits = client.search_sqlite_message_scan(
        conn.connection(),
        SqliteMessageScanRequest {
            raw_query: "needle",
            filters: &local_filters,
            limit: 10,
            offset: 0,
            scan_page_rows: 1,
            field_mask: FieldMask::FULL,
            query_match_type: MatchType::Exact,
        },
    )?;
    assert_eq!(local_hits.len(), 1);
    assert_eq!(local_hits[0].source_path, "/tmp/target.jsonl");
    assert_eq!(local_hits[0].source_id, "local");
    assert_eq!(local_hits[0].origin_kind, "local");
    Ok(())
}

#[test]
fn sqlite_message_scan_paginates_past_prefix_and_ranks_globally() -> Result<()> {
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
         INSERT INTO sources(id, kind) VALUES('local', 'local');
         INSERT INTO agents(id, slug) VALUES(1, 'codex');
         INSERT INTO workspaces(id, path) VALUES(1, '/workspace');
         INSERT INTO conversations(
            id, agent_id, workspace_id, source_id, origin_host, title, source_path
         ) VALUES(1, 1, 1, 'local', NULL, 'fallback title', '/tmp/fallback.jsonl');
         INSERT INTO messages(id, conversation_id, idx, content, created_at) VALUES
            (1, 1, 0, 'prefix noise', 1),
            (2, 1, 1, 'needle', 2),
            (3, 1, 2, 'needle needle', 3);",
    )?;
    let client = cass_layer_b_test_client(None);
    let filters = SearchFilters::default();

    let hits = client.search_sqlite_message_scan(
        conn.connection(),
        SqliteMessageScanRequest {
            raw_query: "needle",
            filters: &filters,
            limit: 10,
            offset: 0,
            scan_page_rows: 1,
            field_mask: FieldMask::FULL,
            query_match_type: MatchType::Exact,
        },
    )?;
    assert_eq!(
        hits.iter()
            .map(|hit| hit.content.as_str())
            .collect::<Vec<_>>(),
        vec!["needle needle", "needle"],
        "a one-row page must neither hide later matches nor truncate global ranking"
    );

    let offset_hit = client.search_sqlite_message_scan(
        conn.connection(),
        SqliteMessageScanRequest {
            raw_query: "needle",
            filters: &filters,
            limit: 1,
            offset: 1,
            scan_page_rows: 1,
            field_mask: FieldMask::FULL,
            query_match_type: MatchType::Exact,
        },
    )?;
    assert_eq!(offset_hit.len(), 1);
    assert_eq!(offset_hit[0].content, "needle");
    Ok(())
}

#[test]
fn sqlite_fts_fallback_scans_untranspilable_negated_or_query() -> Result<()> {
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
         ) VALUES(1, 1, 1, 'local', NULL, 'fallback title', '/tmp/fallback.jsonl');
         INSERT INTO messages(id, conversation_id, idx, content, created_at) VALUES
            (1, 1, 0, 'alpha beta', 1),
            (2, 1, 1, 'gamma beta', 2),
            (3, 1, 2, 'gamma delta', 3);",
    )?;
    let client = SearchClient {
        reader: LexicalReaderSlot::default(),
        sqlite: Mutex::new(Some(conn.into_connection())),
        sqlite_path: None,
        prefix_cache: Mutex::new(CacheShards::new(*CACHE_TOTAL_CAP, *CACHE_BYTE_CAP)),
        reload_on_search: false,
        strict_read_only: false,
        last_reload: Mutex::new(None),
        last_generation: Mutex::new(None),
        reload_epoch: Arc::new(AtomicU64::new(0)),
        warm_tx: None,
        _warm_handle: None,
        metrics: Metrics::default(),
        cache_namespace: format!("v{CACHE_KEY_VERSION}|schema:negated-or-fallback"),
        semantic: Mutex::new(None),
        last_tantivy_total_count: Mutex::new(None),
        last_lexical_degrade_reason: Mutex::new(None),
        last_wildcard_fallback_skip: Mutex::new(None),
    };

    let hits = client.search_sqlite_fts5(
        Path::new(":memory:"),
        "alpha OR NOT beta",
        SearchFilters::default(),
        10,
        0,
        FieldMask::FULL,
    )?;
    let contents = hits
        .iter()
        .map(|hit| hit.content.as_str())
        .collect::<HashSet<_>>();
    assert_eq!(contents, HashSet::from(["alpha beta", "gamma delta"]));
    Ok(())
}

/// 1t79z: through the public `search()` entry point, a SQLite-only client
/// must answer suffix / substring / complex wildcard queries from the
/// bounded source scan instead of a false-empty short-circuit.
#[test]
fn sqlite_only_search_answers_leading_and_interior_wildcards_via_scan() -> Result<()> {
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
         ) VALUES(1, 1, 1, 'local', NULL, 'wildcard title', '/tmp/wildcard.jsonl');
         INSERT INTO messages(id, conversation_id, idx, content, created_at) VALUES
            (1, 1, 0, 'the error_handler fired', 1),
            (2, 1, 1, 'handlers everywhere', 2),
            (3, 1, 2, 'plain hauler text', 3);",
    )?;
    let client = SearchClient {
        reader: LexicalReaderSlot::default(),
        sqlite: Mutex::new(Some(conn.into_connection())),
        sqlite_path: None,
        prefix_cache: Mutex::new(CacheShards::new(*CACHE_TOTAL_CAP, *CACHE_BYTE_CAP)),
        reload_on_search: false,
        strict_read_only: false,
        last_reload: Mutex::new(None),
        last_generation: Mutex::new(None),
        reload_epoch: Arc::new(AtomicU64::new(0)),
        warm_tx: None,
        _warm_handle: None,
        metrics: Metrics::default(),
        cache_namespace: format!("v{CACHE_KEY_VERSION}|schema:wildcard-scan-fallback"),
        semantic: Mutex::new(None),
        last_tantivy_total_count: Mutex::new(None),
        last_lexical_degrade_reason: Mutex::new(None),
        last_wildcard_fallback_skip: Mutex::new(None),
    };
    let contents = |query: &str| -> Result<HashSet<String>> {
        Ok(client
            .search(query, SearchFilters::default(), 10, 0, FieldMask::FULL)?
            .into_iter()
            .map(|hit| hit.content)
            .collect())
    };

    assert_eq!(
        contents("*handler")?,
        HashSet::from(["the error_handler fired".to_string()]),
        "suffix wildcard must match on token boundaries"
    );
    assert_eq!(
        contents("*andl*")?,
        HashSet::from([
            "the error_handler fired".to_string(),
            "handlers everywhere".to_string()
        ]),
        "substring wildcard must match every token containing the core"
    );
    assert_eq!(
        contents("h*ler")?,
        HashSet::from([
            "plain hauler text".to_string(),
            "the error_handler fired".to_string()
        ]),
        "complex wildcard is anchored at both token ends (`handler` and `hauler` \
         match, `handlers` does not)"
    );
    assert!(
        contents("*zzqx")?.is_empty(),
        "a pattern matching nothing stays empty"
    );
    Ok(())
}

#[test]
fn gh395_browse_by_date_treats_null_workspace_and_source_as_local() -> Result<()> {
    let conn = SearchSqliteFixture::in_memory()?;
    conn.execute_batch(
        "CREATE TABLE agents (id INTEGER PRIMARY KEY, slug TEXT NOT NULL);
         CREATE TABLE conversations (
            id INTEGER PRIMARY KEY,
            agent_id INTEGER NOT NULL,
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
         CREATE TABLE workspaces (id INTEGER PRIMARY KEY, path TEXT NOT NULL);
         CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            conversation_id INTEGER NOT NULL,
            idx INTEGER,
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
         CREATE TABLE sources (id TEXT PRIMARY KEY, kind TEXT);",
    )?;
    conn.execute("INSERT INTO agents(id, slug) VALUES(1, 'codex')")?;
    conn.execute(
        "INSERT INTO conversations(id, agent_id, workspace_id, source_id, origin_host, title, source_path)
         VALUES(1, 1, NULL, NULL, NULL, 'browse title', '/tmp/browse.jsonl')",
    )?;
    conn.execute(
        "INSERT INTO messages(id, conversation_id, idx, content, created_at)
         VALUES
            (1, 1, 0, 'older non-tail message', 122),
            (2, 1, 1, 'browse auth token failure', 123)",
    )?;
    conn.execute(
        "INSERT INTO conversation_tail_state(
             conversation_id, ended_at, last_message_idx, last_message_created_at
         ) VALUES(1, 123, 1, 123)",
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

    let hits = client.browse_by_date(
        SearchFilters {
            workspaces: HashSet::from_iter([String::new()]),
            source_filter: SourceFilter::Local,
            ..SearchFilters::default()
        },
        5,
        0,
        true,
        FieldMask::FULL,
    )?;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].workspace, "");
    assert_eq!(hits[0].source_id, "local");
    assert_eq!(hits[0].origin_kind, "local");
    assert_eq!(hits[0].content, "browse auth token failure");
    assert_eq!(hits[0].line_number, Some(2));

    Ok(())
}
