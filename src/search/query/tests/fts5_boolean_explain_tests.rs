use super::super::*;
use crate::connectors::{NormalizedConversation, NormalizedMessage, NormalizedSnippet};
use crate::search::tantivy::TantivyIndex;
use tempfile::TempDir;

/// `coding_agent_session_search-k0e5p` (ibuuh.24.2 sub-bead):
/// E2E equivalence gate for the rank+hydrate FTS5 fallback split
/// landed in peer commit c91ea038. The peer's existing unit test
/// pins the rank-SQL SHAPE (no content columns referenced) but
/// nothing pins the user-facing RESULT-SET equivalence. A
/// regression where the hydrate phase silently re-orders, drops,
/// or re-filters hits would slip past the SQL-shape check and
/// produce user-visible quality changes.
///
/// This test pins the prefix invariant (same pattern as bead
/// 1dd5u for the lexical search path): seed N ranked hits in the
/// FTS5 fallback DB, run search_sqlite_fts5 at limit=K and
/// limit=N, assert the smaller-limit result is a prefix of the
/// larger-limit result. A regression in either rank or hydrate
/// (re-order, drop, re-filter) trips immediately.
///
/// Pins three invariants:
/// 1. Smaller-limit hits are a strict prefix of larger-limit hits.
/// 2. Limit=N returns exactly N matches when ≥N candidates exist.
/// 3. Limit=0 returns empty (boundary case the rank+hydrate
///    split could break by hydrating before honoring the limit).
#[test]
fn search_sqlite_fts5_rank_and_hydrate_split_preserves_limit_prefix_invariant() -> Result<()> {
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
    conn.execute("INSERT INTO workspaces(id, path) VALUES(1, '/tmp/k0e5p')")?;

    // Seed N=6 messages all matching the same query token. Each
    // gets a distinct message_id + content shape so the prefix
    // assertion can pin specific ordering rather than just
    // counts. The bm25 score depends on per-row term frequency;
    // we vary `rankprobe` repetition (1×..6×) so the rank phase
    // produces a deterministic descending order.
    for (i, repeats) in (1..=6_i64).enumerate() {
        let conv_id = i as i64 + 1;
        let msg_id = (i as i64 + 1) * 10;
        conn.execute_compat(
            "INSERT INTO conversations(id, agent_id, workspace_id, source_id, \
             origin_host, title, source_path) \
             VALUES(?1, 1, 1, 'local', NULL, ?2, ?3)",
            params![
                conv_id,
                format!("k0e5p-{}", i),
                format!("/tmp/k0e5p/{}.jsonl", i),
            ],
        )?;
        let content = "rankprobe ".repeat(repeats as usize);
        conn.execute_compat(
            "INSERT INTO messages(id, conversation_id, idx, content, created_at) \
             VALUES(?1, ?2, ?3, ?4, ?5)",
            params![
                msg_id,
                conv_id,
                i as i64,
                content.as_str(),
                1_700_000_000_i64 + i as i64
            ],
        )?;
        conn.execute_compat(
            "INSERT INTO fts_messages(rowid, content, title, agent, workspace, \
             source_path, created_at, message_id) \
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                msg_id,
                content.as_str(),
                format!("k0e5p-{}", i),
                "codex",
                "/tmp/k0e5p",
                format!("/tmp/k0e5p/{}.jsonl", i),
                1_700_000_000_i64 + i as i64,
                msg_id,
            ],
        )?;
    }

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
        cache_namespace: format!("v{CACHE_KEY_VERSION}|schema:k0e5p"),
        semantic: Mutex::new(None),
        last_tantivy_total_count: Mutex::new(None),
        last_lexical_degrade_reason: Mutex::new(None),
        last_wildcard_fallback_skip: Mutex::new(None),
    };

    // Hit-key tuple: (source_path, line_number) is the stable
    // operator-visible identity. Two limits that share a prefix
    // must produce hits with the same identities in the same
    // order across that prefix.
    fn hit_keys(hits: &[SearchHit]) -> Vec<(String, Option<usize>)> {
        hits.iter()
            .map(|h| (h.source_path.clone(), h.line_number))
            .collect()
    }

    let large_hits = client.search_sqlite_fts5(
        Path::new(":memory:"),
        "rankprobe",
        SearchFilters::default(),
        6,
        0,
        FieldMask::FULL,
    )?;
    assert_eq!(
        large_hits.len(),
        6,
        "limit=N must return all N candidates when the corpus has exactly N matches"
    );

    let small_hits = client.search_sqlite_fts5(
        Path::new(":memory:"),
        "rankprobe",
        SearchFilters::default(),
        3,
        0,
        FieldMask::FULL,
    )?;
    assert_eq!(small_hits.len(), 3, "limit=3 must return exactly 3 hits");

    // Invariant 1: smaller-limit hits are a STRICT prefix of the
    // larger-limit hits — same identity, same order.
    let large_keys = hit_keys(&large_hits);
    let small_keys = hit_keys(&small_hits);
    assert_eq!(
        small_keys,
        large_keys[..3],
        "limit=3 hit keys MUST be the first 3 of limit=6 hit keys (rank+hydrate \
         split must not re-order or re-filter); small={small_keys:?} \
         large_prefix={:?}",
        &large_keys[..3]
    );

    // Invariant 2: hit content is also identical across the
    // shared prefix — the hydrate phase preserves the content
    // string the rank phase ranked. A regression where hydrate
    // pulled from a different DB row than rank pointed at would
    // trip this even if the keys aligned.
    for (idx, (small, large)) in small_hits.iter().zip(large_hits.iter()).enumerate() {
        assert_eq!(
            small.content, large.content,
            "hit[{idx}] content must agree across limit=3 and limit=6: \
             small={:?} large={:?}",
            small.content, large.content
        );
        assert_eq!(
            small.title, large.title,
            "hit[{idx}] title must agree across limit=3 and limit=6"
        );
    }

    // Invariant 3: limit=0 boundary. The rank+hydrate split could
    // break this by hydrating before honoring the limit; pinning
    // it directly catches that regression class.
    let zero_hits = client.search_sqlite_fts5(
        Path::new(":memory:"),
        "rankprobe",
        SearchFilters::default(),
        0,
        0,
        FieldMask::FULL,
    )?;
    assert!(
        zero_hits.is_empty(),
        "limit=0 must return zero hits even though the rank phase has candidates; \
         got {} hits",
        zero_hits.len()
    );

    Ok(())
}

// --- levenshtein_distance tests ---

#[test]
fn levenshtein_distance_identical_strings() {
    assert_eq!(levenshtein_distance("hello", "hello"), 0);
    assert_eq!(levenshtein_distance("", ""), 0);
}

#[test]
fn levenshtein_distance_insertions() {
    assert_eq!(levenshtein_distance("", "abc"), 3);
    assert_eq!(levenshtein_distance("cat", "cats"), 1);
}

#[test]
fn levenshtein_distance_deletions() {
    assert_eq!(levenshtein_distance("abc", ""), 3);
    assert_eq!(levenshtein_distance("cats", "cat"), 1);
}

#[test]
fn levenshtein_distance_substitutions() {
    assert_eq!(levenshtein_distance("cat", "bat"), 1);
    assert_eq!(levenshtein_distance("kitten", "sitten"), 1);
}

#[test]
fn levenshtein_distance_mixed_operations() {
    assert_eq!(levenshtein_distance("kitten", "sitting"), 3);
    assert_eq!(levenshtein_distance("saturday", "sunday"), 3);
}

// --- is_tool_invocation_noise tests ---

#[test]
fn is_tool_invocation_noise_allows_real_content() {
    assert!(!is_tool_invocation_noise("This is a normal message"));
    assert!(!is_tool_invocation_noise(
        "Let me use the Tool feature to accomplish this task. Here is the implementation..."
    ));
    // Long content that happens to start with [Tool: should be allowed if it's substantial
    let long_content = "[Tool: Read] Now here is a lot of useful content that explains the implementation details and provides context for the changes being made to the codebase.";
    assert!(!is_tool_invocation_noise(long_content));
}

#[test]
fn is_tool_invocation_noise_handles_short_tool_markers() {
    assert!(is_tool_invocation_noise("[tool: x]"));
    assert!(is_tool_invocation_noise("tool: bash"));
}

// --- Integration tests for boolean queries through search ---

#[test]
fn search_boolean_and_filters_results() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;

    // Create documents with different word combinations
    let conv1 = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("doc1".into()),
        workspace: None,
        source_path: dir.path().join("1.jsonl"),
        started_at: Some(1),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(1),
            content: "alpha beta gamma".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    let conv2 = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("doc2".into()),
        workspace: None,
        source_path: dir.path().join("2.jsonl"),
        started_at: Some(2),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(2),
            content: "alpha delta".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    index.add_conversation(&conv1)?;
    index.add_conversation(&conv2)?;
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");

    // "alpha AND beta" should only match doc1
    let hits = client.search(
        "alpha AND beta",
        SearchFilters::default(),
        10,
        0,
        FieldMask::FULL,
    )?;
    assert_eq!(hits.len(), 1);
    assert!(hits[0].content.contains("gamma"));

    // "alpha AND delta" should only match doc2
    let hits = client.search(
        "alpha AND delta",
        SearchFilters::default(),
        10,
        0,
        FieldMask::FULL,
    )?;
    assert_eq!(hits.len(), 1);
    assert!(hits[0].content.contains("delta"));

    Ok(())
}

#[test]
fn search_boolean_or_expands_results() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;

    let conv1 = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("doc1".into()),
        workspace: None,
        source_path: dir.path().join("1.jsonl"),
        started_at: Some(1),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(1),
            content: "unique xyzzy term".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    let conv2 = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("doc2".into()),
        workspace: None,
        source_path: dir.path().join("2.jsonl"),
        started_at: Some(2),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(2),
            content: "unique plugh term".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    index.add_conversation(&conv1)?;
    index.add_conversation(&conv2)?;
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");

    // "xyzzy OR plugh" should match both docs
    let hits = client.search(
        "xyzzy OR plugh",
        SearchFilters::default(),
        10,
        0,
        FieldMask::FULL,
    )?;
    assert_eq!(hits.len(), 2);

    Ok(())
}

#[test]
fn search_boolean_not_excludes_results() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;

    let conv1 = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("doc1".into()),
        workspace: None,
        source_path: dir.path().join("1.jsonl"),
        started_at: Some(1),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(1),
            content: "nottest keep this".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    let conv2 = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("doc2".into()),
        workspace: None,
        source_path: dir.path().join("2.jsonl"),
        started_at: Some(2),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(2),
            content: "nottest exclude this".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    index.add_conversation(&conv1)?;
    index.add_conversation(&conv2)?;
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");

    // "nottest NOT exclude" should only match doc1 (has nottest but NOT exclude)
    let hits = client.search(
        "nottest NOT exclude",
        SearchFilters::default(),
        10,
        0,
        FieldMask::FULL,
    )?;
    assert_eq!(hits.len(), 1);
    // Verify we got the right doc by checking it doesn't contain "exclude"
    assert!(
        !hits[0].content.contains("exclude"),
        "NOT exclude should filter out doc with 'exclude'"
    );

    // Prefix "-" exclusion should behave like NOT for simple queries.
    let hits = client.search(
        "nottest -exclude",
        SearchFilters::default(),
        10,
        0,
        FieldMask::FULL,
    )?;
    assert_eq!(hits.len(), 1);
    assert!(
        !hits[0].content.contains("exclude"),
        "Prefix -exclude should filter out doc with 'exclude'"
    );

    Ok(())
}

#[test]
fn search_phrase_query_matches_exact_sequence() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;

    let conv1 = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("doc1".into()),
        workspace: None,
        source_path: dir.path().join("1.jsonl"),
        started_at: Some(1),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(1),
            content: "the quick brown fox".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    let conv2 = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("doc2".into()),
        workspace: None,
        source_path: dir.path().join("2.jsonl"),
        started_at: Some(2),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(2),
            content: "the brown quick fox".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    index.add_conversation(&conv1)?;
    index.add_conversation(&conv2)?;
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");

    // "quick brown" (without quotes) should match both (words just need to be present)
    let hits = client.search(
        "quick brown",
        SearchFilters::default(),
        10,
        0,
        FieldMask::FULL,
    )?;
    assert_eq!(hits.len(), 2);

    // "\"quick brown\"" should match exact order only
    let hits = client.search(
        "\"quick brown\"",
        SearchFilters::default(),
        10,
        0,
        FieldMask::FULL,
    )?;
    assert_eq!(hits.len(), 1);
    assert!(hits[0].content.contains("quick brown"));

    Ok(())
}

#[test]
fn search_dot_punctuation_splits_terms_but_hyphens_preserve_compound_semantics() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;

    let conv = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("doc".into()),
        workspace: None,
        source_path: dir.path().join("3.jsonl"),
        started_at: Some(1),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(1),
            content: "foo bar baz".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    index.add_conversation(&conv)?;
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");

    let hits = client.search("foo.bar", SearchFilters::default(), 10, 0, FieldMask::FULL)?;
    assert_eq!(hits.len(), 1);

    let hits = client.search("foo-bar", SearchFilters::default(), 10, 0, FieldMask::FULL)?;
    assert_eq!(hits.len(), 0);

    Ok(())
}

// ========================================================================
// QueryExplanation tests
// ========================================================================

#[test]
fn explanation_classifies_simple_query() {
    let exp = QueryExplanation::analyze("hello", &SearchFilters::default());
    assert_eq!(exp.query_type, QueryType::Simple);
    assert_eq!(exp.index_strategy, IndexStrategy::EdgeNgram);
    assert_eq!(exp.estimated_cost, QueryCost::Low);
    assert!(exp.parsed.terms.len() == 1);
    assert_eq!(exp.parsed.terms[0].text, "hello");
    assert!(!exp.parsed.terms[0].subterms.is_empty());
    assert_eq!(exp.parsed.terms[0].subterms[0].pattern, "exact");
}

#[test]
fn explanation_classifies_wildcard_query() {
    let exp = QueryExplanation::analyze("*handler*", &SearchFilters::default());
    assert_eq!(exp.query_type, QueryType::Wildcard);
    assert_eq!(exp.index_strategy, IndexStrategy::RegexScan);
    assert_eq!(exp.estimated_cost, QueryCost::High);
    assert!(!exp.parsed.terms[0].subterms.is_empty());
    assert!(
        exp.parsed.terms[0].subterms[0]
            .pattern
            .contains("substring")
    );
    assert!(exp.warnings.iter().any(|w| w.contains("regex scan")));
}

#[test]
fn explanation_classifies_boolean_query() {
    let exp = QueryExplanation::analyze("foo AND bar", &SearchFilters::default());
    assert_eq!(exp.query_type, QueryType::Boolean);
    assert_eq!(exp.index_strategy, IndexStrategy::BooleanCombination);
    assert!(exp.parsed.operators.contains(&"AND".to_string()));
}

/// 2l1b0.52: `--explain` shows how the engine groups the query, so an
/// agent never infers precedence from the text. Negative control: the
/// legacy grammar grouped `a OR b c` as `(a OR b) AND c`.
#[test]
fn explanation_shows_the_grouping_the_engine_applies() {
    let structure = |raw: &str| {
        QueryExplanation::analyze(raw, &SearchFilters::default())
            .parsed
            .structure
    };
    assert_eq!(structure("a OR b c").as_deref(), Some("a OR (b AND c)"));
    assert_eq!(structure("(a OR b) c").as_deref(), Some("(a OR b) AND c"));
    assert_eq!(
        structure("a -(b OR \"c d\")").as_deref(),
        Some("a AND NOT (b OR \"c d\")")
    );
    assert_eq!(structure("NOT NOT a").as_deref(), Some("a"));
    assert_eq!(structure("").as_deref(), None);
}

/// Unbalanced parentheses are recovered, not rejected, and `--explain`
/// says how: the grouping shown is the one searched.
#[test]
fn explanation_reports_recovered_parentheses() {
    let explain = |raw: &str| QueryExplanation::analyze(raw, &SearchFilters::default());
    let recovery = |warnings: &[String]| {
        warnings
            .iter()
            .filter(|warning| warning.contains("'('") || warning.contains("'()'"))
            .cloned()
            .collect::<Vec<_>>()
    };

    let unclosed = explain("kiwi AND (lime OR mango");
    assert_eq!(
        unclosed.parsed.structure.as_deref(),
        Some("kiwi AND (lime OR mango)")
    );
    assert_eq!(
        recovery(&unclosed.warnings),
        ["1 unclosed '(' closed at the end of the query"]
    );

    let empty = explain("kiwi () lime");
    assert_eq!(empty.parsed.structure.as_deref(), Some("kiwi AND lime"));
    assert_eq!(recovery(&empty.warnings), ["1 empty '()' skipped"]);

    // Balanced groups, and parentheses inside a word, recover nothing.
    for balanced in ["(kiwi OR lime) mango", "call foo(bar) now", "kiwi lime)"] {
        assert!(
            recovery(&explain(balanced).warnings).is_empty(),
            "{balanced:?} needs no recovery"
        );
    }
}

#[test]
fn explanation_classifies_phrase_query() {
    let exp = QueryExplanation::analyze("\"exact phrase\"", &SearchFilters::default());
    assert_eq!(exp.query_type, QueryType::Phrase);
    assert!(exp.parsed.phrases.contains(&"exact phrase".to_string()));
}

#[test]
fn explanation_handles_filtered_query() {
    let mut filters = SearchFilters::default();
    filters.agents.insert("codex".to_string());

    let exp = QueryExplanation::analyze("test", &filters);
    assert_eq!(exp.query_type, QueryType::Filtered);
    assert_eq!(exp.filters_summary.agent_count, 1);
    assert!(
        exp.filters_summary
            .description
            .as_ref()
            .unwrap()
            .contains("1 agent")
    );
    assert!(exp.warnings.iter().any(|w| w.contains("codex")));
}

#[test]
fn explanation_handles_empty_query() {
    let exp = QueryExplanation::analyze("", &SearchFilters::default());
    assert_eq!(exp.query_type, QueryType::Empty);
    assert_eq!(exp.index_strategy, IndexStrategy::FullScan);
    assert_eq!(exp.estimated_cost, QueryCost::High);
    assert!(exp.warnings.iter().any(|w| w.contains("Empty query")));
}

#[test]
fn explanation_warns_short_terms() {
    let exp = QueryExplanation::analyze("a", &SearchFilters::default());
    assert!(exp.warnings.iter().any(|w| w.contains("Very short term")));
}

#[test]
fn explanation_with_wildcard_fallback() {
    let exp =
        QueryExplanation::analyze("test", &SearchFilters::default()).with_wildcard_fallback(true);
    assert!(exp.wildcard_applied);
    // Message starts with capital W: "Wildcard fallback was applied..."
    assert!(exp.warnings.iter().any(|w| w.contains("Wildcard fallback")));
}

#[test]
fn explanation_complex_query_has_higher_cost() {
    let exp = QueryExplanation::analyze(
        "foo AND bar OR baz NOT qux AND \"phrase here\"",
        &SearchFilters::default(),
    );
    assert_eq!(exp.query_type, QueryType::Boolean);
    // Complex query should have Medium or High cost
    assert!(matches!(
        exp.estimated_cost,
        QueryCost::Medium | QueryCost::High
    ));
}

#[test]
fn explanation_preserves_original_query() {
    let exp = QueryExplanation::analyze("Hello World!", &SearchFilters::default());
    assert_eq!(exp.original_query, "Hello World!");
    // Sanitized replaces special chars with spaces but preserves case
    assert!(exp.sanitized_query.contains("Hello"));
    // ! is replaced with space
    assert!(!exp.sanitized_query.contains("!"));
}

#[test]
fn explanation_detects_not_operator() {
    let exp = QueryExplanation::analyze("foo NOT bar", &SearchFilters::default());
    assert!(exp.parsed.operators.contains(&"NOT".to_string()));
    // Second term should be marked as negated
    assert!(
        exp.parsed
            .terms
            .iter()
            .any(|t| t.negated && t.text == "bar")
    );
}

#[test]
fn explanation_implicit_and() {
    let exp = QueryExplanation::analyze("foo bar", &SearchFilters::default());
    assert!(exp.parsed.implicit_and);
    assert_eq!(exp.parsed.terms.len(), 2);

    let term_and_phrase = QueryExplanation::analyze("foo \"bar baz\"", &SearchFilters::default());
    assert!(term_and_phrase.parsed.implicit_and);
    assert_eq!(term_and_phrase.parsed.terms.len(), 1);
    assert_eq!(term_and_phrase.parsed.phrases.len(), 1);

    let two_phrases =
        QueryExplanation::analyze("\"foo bar\" \"baz qux\"", &SearchFilters::default());
    assert!(two_phrases.parsed.implicit_and);
    assert_eq!(two_phrases.parsed.phrases.len(), 2);

    let one_phrase = QueryExplanation::analyze("\"foo bar\"", &SearchFilters::default());
    assert!(!one_phrase.parsed.implicit_and);

    let mixed_connectors = QueryExplanation::analyze("foo AND bar baz", &SearchFilters::default());
    assert!(mixed_connectors.parsed.implicit_and);

    let implicit_before_not = QueryExplanation::analyze("foo NOT bar", &SearchFilters::default());
    assert!(implicit_before_not.parsed.implicit_and);

    let explicit_before_not =
        QueryExplanation::analyze("foo AND NOT bar", &SearchFilters::default());
    assert!(!explicit_before_not.parsed.implicit_and);

    let explicit_or = QueryExplanation::analyze("foo OR bar", &SearchFilters::default());
    assert!(!explicit_or.parsed.implicit_and);
}

#[test]
fn explanation_serializes_to_json() {
    let exp = QueryExplanation::analyze("test query", &SearchFilters::default());
    let json = serde_json::to_value(&exp).expect("should serialize");
    assert!(json["original_query"].is_string());
    assert!(json["query_type"].is_string());
    assert!(json["index_strategy"].is_string());
    assert!(json["estimated_cost"].is_string());
    assert!(json["parsed"]["terms"].is_array());
}

// =========================================================================
// Multi-filter combination tests (bead yln.2)
// =========================================================================

#[test]
fn search_multi_filter_agent_workspace_time() -> Result<()> {
    // Test combining agent, workspace, and time range filters
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;

    // Create 4 conversations with different combinations
    let convs = [
        ("codex", "/ws/alpha", 100, "needle alpha codex"),
        ("claude", "/ws/alpha", 200, "needle alpha claude"),
        ("codex", "/ws/beta", 150, "needle beta codex"),
        ("codex", "/ws/alpha", 300, "needle alpha codex late"),
    ];

    for (i, (agent, ws, ts, content)) in convs.iter().enumerate() {
        let conv = NormalizedConversation {
            agent_slug: (*agent).into(),
            external_id: None,
            title: Some(format!("conv-{i}")),
            workspace: Some(std::path::PathBuf::from(*ws)),
            source_path: dir.path().join(format!("{i}.jsonl")),
            started_at: Some(*ts),
            ended_at: None,
            metadata: serde_json::json!({}),
            messages: vec![NormalizedMessage {
                idx: 0,
                role: "user".into(),
                author: None,
                created_at: Some(*ts),
                content: (*content).into(),
                extra: serde_json::json!({}),
                snippets: vec![],
                invocations: Vec::new(),
            }],
        };
        index.add_conversation(&conv)?;
    }
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");

    // Filter: codex + alpha + time 50-250
    let mut filters = SearchFilters::default();
    filters.agents.insert("codex".into());
    filters.workspaces.insert("/ws/alpha".into());
    filters.created_from = Some(50);
    filters.created_to = Some(250);

    let hits = client.search("needle", filters, 10, 0, FieldMask::FULL)?;
    assert_eq!(
        hits.len(),
        1,
        "Should match only one conv (codex + alpha + ts=100)"
    );
    assert_eq!(hits[0].agent, "codex");
    assert_eq!(hits[0].workspace, "/ws/alpha");
    assert!(hits[0].content.contains("alpha codex"));
    assert!(!hits[0].content.contains("late")); // Not the ts=300 one

    Ok(())
}

#[test]
fn search_multi_agent_filter() -> Result<()> {
    // Test filtering by multiple agents
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;

    for agent in ["codex", "claude", "cline", "gemini"] {
        let conv = NormalizedConversation {
            agent_slug: agent.into(),
            external_id: None,
            title: Some(format!("{agent}-conv")),
            workspace: Some(std::path::PathBuf::from("/ws")),
            source_path: dir.path().join(format!("{agent}.jsonl")),
            started_at: Some(100),
            ended_at: None,
            metadata: serde_json::json!({}),
            messages: vec![NormalizedMessage {
                idx: 0,
                role: "user".into(),
                author: None,
                created_at: Some(100),
                content: format!("needle from {agent}"),
                extra: serde_json::json!({}),
                snippets: vec![],
                invocations: Vec::new(),
            }],
        };
        index.add_conversation(&conv)?;
    }
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");

    // Filter for codex and claude only
    let mut filters = SearchFilters::default();
    filters.agents.insert("codex".into());
    filters.agents.insert("claude".into());

    let hits = client.search("needle", filters, 10, 0, FieldMask::FULL)?;
    assert_eq!(hits.len(), 2);
    let agents: Vec<_> = hits.iter().map(|h| h.agent.as_str()).collect();
    assert!(agents.contains(&"codex"));
    assert!(agents.contains(&"claude"));
    assert!(!agents.contains(&"cline"));
    assert!(!agents.contains(&"gemini"));

    Ok(())
}

// =========================================================================
// Cache metrics tests (bead yln.2)
// =========================================================================

#[test]
fn cache_metrics_incremented_on_operations() {
    let client = SearchClient {
        reader: LexicalReaderSlot::default(),
        sqlite: Mutex::new(None),
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

    // Initial metrics should be zero
    let (hits, outcomes, shortfall, reloads, _) = client.metrics.snapshot_all();
    assert_eq!((hits, outcomes.cold_miss, shortfall, reloads), (0, 0, 0, 0));

    // Simulate operations
    client.metrics.inc_cache_hits();
    client.metrics.inc_cache_hits();
    client.metrics.inc_cache_miss();
    client.metrics.inc_cache_shortfall();
    client.metrics.inc_reload();

    let (hits, outcomes, shortfall, reloads, _) = client.metrics.snapshot_all();
    assert_eq!(hits, 2);
    assert_eq!(outcomes.cold_miss, 1);
    assert_eq!(shortfall, 1);
    assert_eq!(reloads, 1);
}

#[test]
fn cache_shard_name_deterministic() {
    // Verify that shard name generation is deterministic for same filters
    let client = SearchClient {
        reader: LexicalReaderSlot::default(),
        sqlite: Mutex::new(None),
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

    let filters1 = SearchFilters::default();
    let mut filters2 = SearchFilters::default();
    filters2.agents.insert("codex".into());
    let mut filters3 = SearchFilters::default();
    filters3.workspaces.insert("/tmp/cass-workspace".into());

    // Same filters should always produce same shard name
    let shard1_first = client.shard_name(&filters1);
    let shard1_second = client.shard_name(&filters1);
    assert_eq!(
        shard1_first, shard1_second,
        "Same filters should produce same shard name"
    );

    // Different filters produce different shard names
    let shard2 = client.shard_name(&filters2);
    assert_ne!(
        shard1_first, shard2,
        "Different filters should produce different shard names"
    );

    // Shard name is deterministic
    assert_eq!(shard2, client.shard_name(&filters2));
    assert_eq!(
        client.shard_name(&filters3),
        "workspace:/tmp/cass-workspace"
    );
}

// =========================================================================
// Wildcard fallback edge cases (bead yln.2)
// =========================================================================

#[test]
fn wildcard_fallback_respects_filter_constraints() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;

    // Create conversations that would match wildcard but not filter
    let conv_match = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("match".into()),
        workspace: Some(std::path::PathBuf::from("/target")),
        source_path: dir.path().join("match.jsonl"),
        started_at: Some(100),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(100),
            content: "unique specific term here".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };

    let conv_other = NormalizedConversation {
        agent_slug: "claude".into(),
        external_id: None,
        title: Some("other".into()),
        workspace: Some(std::path::PathBuf::from("/other")),
        source_path: dir.path().join("other.jsonl"),
        started_at: Some(100),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(100),
            content: "unique specific also here".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };

    index.add_conversation(&conv_match)?;
    index.add_conversation(&conv_other)?;
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");

    // Search with filter that only matches conv_match
    let mut filters = SearchFilters::default();
    filters.agents.insert("codex".into());

    let result =
        client.search_with_fallback("unique", filters.clone(), 10, 0, 100, FieldMask::FULL)?;
    // Should only return the codex conversation, not claude
    assert!(result.hits.iter().all(|h| h.agent == "codex"));

    Ok(())
}

#[test]
fn wildcard_fallback_short_query_triggers_prefix() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;

    let conv = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("test".into()),
        workspace: None,
        source_path: dir.path().join("test.jsonl"),
        started_at: Some(100),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(100),
            content: "authentication authorization oauth".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    index.add_conversation(&conv)?;
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");

    // Short prefix "auth" should match "authentication" and "authorization"
    let result = client.search_with_fallback(
        "auth",
        SearchFilters::default(),
        10,
        0,
        100,
        FieldMask::FULL,
    )?;
    assert!(
        !result.hits.is_empty(),
        "Short prefix should match via prefix search"
    );
    assert!(result.hits[0].content.contains("auth"));

    Ok(())
}

// =========================================================================
// Real fixture tests with metrics (bead yln.2)
// =========================================================================

#[test]
fn search_real_fixture_multiple_messages() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;

    // Create a realistic conversation with multiple messages
    let conv = NormalizedConversation {
        agent_slug: "claude_code".into(),
        external_id: Some("conv-123".into()),
        title: Some("Implementing authentication".into()),
        workspace: Some(std::path::PathBuf::from("/home/user/project")),
        source_path: dir.path().join("session-1.jsonl"),
        started_at: Some(1700000000000),
        ended_at: Some(1700000060000),
        metadata: serde_json::json!({
            "model": "claude-3-sonnet",
            "tokens": 1500
        }),
        messages: vec![
            NormalizedMessage {
                idx: 0,
                role: "user".into(),
                author: Some("developer".into()),
                created_at: Some(1700000000000),
                content: "Help me implement JWT authentication for my Express API".into(),
                extra: serde_json::json!({}),
                snippets: vec![],
                invocations: Vec::new(),
            },
            NormalizedMessage {
                idx: 1,
                role: "assistant".into(),
                author: Some("claude".into()),
                created_at: Some(1700000010000),
                content: "I'll help you implement JWT authentication. First, let's install the required packages.".into(),
                extra: serde_json::json!({}),
                snippets: vec![NormalizedSnippet {
                    file_path: Some("package.json".into()),
                    start_line: Some(1),
                    end_line: Some(5),
                    language: Some("json".into()),
                    snippet_text: Some(r#"{"dependencies":{"jsonwebtoken":"^9.0.0"}}"#.into()),
                }],
                invocations: Vec::new(),
            },
            NormalizedMessage {
                idx: 2,
                role: "user".into(),
                author: Some("developer".into()),
                created_at: Some(1700000030000),
                content: "Can you also add refresh token support?".into(),
                extra: serde_json::json!({}),
                snippets: vec![],
                invocations: Vec::new(),
            },
        ],
    };
    index.add_conversation(&conv)?;
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");

    // Search for various terms that should match
    let hits = client.search(
        "JWT authentication",
        SearchFilters::default(),
        10,
        0,
        FieldMask::FULL,
    )?;
    assert!(!hits.is_empty(), "Should find JWT authentication");
    assert!(hits.iter().any(|h| h.agent == "claude_code"));
    assert!(
        hits.iter()
            .any(|h| h.snippet.contains("JWT") || h.snippet.contains("authentication"))
    );

    // Search for assistant response content
    let hits = client.search(
        "required packages",
        SearchFilters::default(),
        10,
        0,
        FieldMask::FULL,
    )?;
    assert!(
        !hits.is_empty(),
        "Should find 'required packages' in assistant response"
    );

    // Search for user question about refresh tokens
    let hits = client.search(
        "refresh token",
        SearchFilters::default(),
        10,
        0,
        FieldMask::FULL,
    )?;
    assert!(!hits.is_empty(), "Should find refresh token");
    assert!(hits.iter().any(|h| h.content.contains("refresh")));

    Ok(())
}

#[test]
fn search_deduplication_with_similar_content() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;

    // Create two conversations with very similar content
    for i in 0..2 {
        let conv = NormalizedConversation {
            agent_slug: "codex".into(),
            external_id: None,
            title: Some(format!("similar-{i}")),
            workspace: Some(std::path::PathBuf::from("/ws")),
            source_path: dir.path().join(format!("similar-{i}.jsonl")),
            started_at: Some(100 + i),
            ended_at: None,
            metadata: serde_json::json!({}),
            messages: vec![NormalizedMessage {
                idx: 0,
                role: "user".into(),
                author: None,
                created_at: Some(100 + i),
                // Exactly the same content
                content: "implement the sorting algorithm".into(),
                extra: serde_json::json!({}),
                snippets: vec![],
                invocations: Vec::new(),
            }],
        };
        index.add_conversation(&conv)?;
    }
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");
    let result = client.search_with_fallback(
        "sorting algorithm",
        SearchFilters::default(),
        10,
        0,
        100,
        FieldMask::FULL,
    )?;

    // Both should be returned (different source_paths mean different conversations)
    // but if they have exact same content from same source, dedup should apply
    assert!(!result.hits.is_empty());

    Ok(())
}
