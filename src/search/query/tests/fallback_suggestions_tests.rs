use super::super::*;
use crate::connectors::{NormalizedConversation, NormalizedMessage};
use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use crate::search::tantivy::TantivyIndex;
use crate::storage::sqlite::FrankenStorage;
use serde_json::json;
use tempfile::TempDir;

#[test]
fn wildcard_fallback_sparse_check_uses_effective_limit() {
    assert!(
        !should_try_wildcard_fallback(1, 1, 0, 3),
        "a filled one-result page is not sparse for fallback purposes"
    );
    assert!(
        !should_try_wildcard_fallback(2, 2, 0, 3),
        "a filled two-result page is not sparse for fallback purposes"
    );
    assert!(
        should_try_wildcard_fallback(0, 1, 0, 3),
        "zero hits should still trigger fallback even for tiny pages"
    );
    assert!(
        should_try_wildcard_fallback(1, 2, 0, 3),
        "a partially filled page should still trigger fallback"
    );
    assert!(
        !should_try_wildcard_fallback(0, 5, 10, 3),
        "pagination should not trigger wildcard fallback"
    );
    assert!(
        should_try_wildcard_fallback(1, 0, 0, 3),
        "limit zero preserves the legacy sparse-threshold semantics"
    );
}

#[test]
fn snippet_preview_fast_path_requires_snippet_only_match() {
    let snippet_only = FieldMask::new(false, true, false, false);
    let snippet = snippet_from_preview_without_full_content(
        snippet_only,
        "migration checks the database constraint before writing",
        "database",
    )
    .expect("preview should satisfy a snippet-only request when it contains the query");
    assert!(snippet.contains("**database**"));

    assert!(
        snippet_from_preview_without_full_content(
            FieldMask::FULL,
            "migration checks the database constraint before writing",
            "database",
        )
        .is_none(),
        "full-content requests must keep the sqlite hydration path"
    );
    assert!(
        snippet_from_preview_without_full_content(
            snippet_only,
            "migration checks constraints before writing",
            "database",
        )
        .is_none(),
        "snippet-only requests hydrate when the preview cannot show the match"
    );
}

#[test]
fn search_with_fallback_returns_exact_when_sufficient() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;

    // Add enough docs to exceed threshold - each with UNIQUE content to avoid dedup
    for i in 0..5 {
        let conv = NormalizedConversation {
            agent_slug: "codex".into(),
            external_id: None,
            title: Some(format!("doc-{i}")),
            workspace: Some(std::path::PathBuf::from("/ws")),
            source_path: dir.path().join(format!("{i}.jsonl")),
            started_at: Some(100 + i),
            ended_at: None,
            metadata: serde_json::json!({}),
            messages: vec![NormalizedMessage {
                idx: 0,
                role: "user".into(),
                author: None,
                created_at: Some(100 + i),
                // Each doc has unique content but shares "apple" keyword
                content: format!("apple fruit number {i} is delicious and healthy"),
                extra: serde_json::json!({}),
                snippets: vec![],
                invocations: Vec::new(),
            }],
        };
        index.add_conversation(&conv)?;
    }
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");

    // Search with low threshold - should not trigger fallback
    let result = client.search_with_fallback(
        "apple",
        SearchFilters::default(),
        10,
        0,
        3, // threshold of 3
        FieldMask::FULL,
    )?;

    assert!(!result.wildcard_fallback);
    assert!(result.hits.len() >= 3); // has enough results
    assert_eq!(result.total_count, Some(5));

    Ok(())
}

#[test]
fn search_with_fallback_triggers_on_sparse_results() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;

    // Add docs with substring that won't match exact prefix
    let conv = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("substring test".into()),
        workspace: Some(std::path::PathBuf::from("/ws")),
        source_path: dir.path().join("test.jsonl"),
        started_at: Some(100),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(100),
            content: "configuration management system".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    index.add_conversation(&conv)?;
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");

    // Search for "config" which should match "configuration" via prefix
    let result = client.search_with_fallback(
        "config",
        SearchFilters::default(),
        10,
        0,
        5, // high threshold
        FieldMask::FULL,
    )?;

    // Since we have only 1 result and threshold is 5, it may trigger fallback
    // but *config* would still match "configuration"
    assert!(!result.hits.is_empty());

    Ok(())
}

/// Snippet terms must be the words a reader would expect highlighted.
///
/// The `field:value` case is the one that matters: matched whole, the token
/// never occurs in the source and the hit renders with NO highlight at all.
#[test]
fn snippet_terms_drop_operators_and_field_prefixes() {
    assert_eq!(
        snippet_terms_for_query("hello world"),
        vec!["hello".to_string(), "world".to_string()]
    );
    assert_eq!(
        snippet_terms_for_query("agent:claude error"),
        vec!["claude".to_string(), "error".to_string()],
        "a field-qualified token must contribute its VALUE, not the whole token"
    );
    assert_eq!(
        snippet_terms_for_query("\"exact phrase\" here"),
        vec![
            "exact".to_string(),
            "phrase".to_string(),
            "here".to_string()
        ],
        "a quoted phrase contributes its words individually"
    );
    assert_eq!(
        snippet_terms_for_query("foo AND bar OR baz NOT qux"),
        vec![
            "foo".to_string(),
            "bar".to_string(),
            "baz".to_string(),
            "qux".to_string()
        ],
        "boolean operators are not literal words to highlight"
    );
    assert!(
        snippet_terms_for_query("   ").is_empty(),
        "a whitespace-only query yields no terms"
    );
    assert!(
        snippet_terms_for_query("agent:").is_empty(),
        "a bare field prefix with no value yields no terms"
    );
}

/// coding_agent_session_search-7hvlg (GH #518 follow-up): the automatic
/// `*term*` retry only refines a search that already succeeded. Its
/// dictionary walk can run out of the query fuel the plain lookup fit in;
/// the plain query's hits must come back, with `wildcard_fallback_skipped`
/// naming the cause, instead of the whole search failing.
#[test]
fn search_with_fallback_keeps_exact_hits_when_the_wildcard_retry_runs_out_of_fuel() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;
    let conversation = |id: usize, content: String| NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some(format!("doc-{id}")),
        workspace: Some(std::path::PathBuf::from("/ws")),
        source_path: dir.path().join(format!("{id}.jsonl")),
        started_at: Some(100),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(100),
            content,
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    // Many distinct terms over a few segments give the retry's
    // dictionary walk real work; one document holds the needle.
    for id in 0..400 {
        let words = (0..16)
            .map(|word| format!("filler{id}x{word}"))
            .collect::<Vec<_>>()
            .join(" ");
        index.add_conversation(&conversation(id, words))?;
        if id % 100 == 99 {
            index.commit()?;
        }
    }
    index.add_conversation(&conversation(400, "needleword appears once".into()))?;
    index.commit()?;

    let client_with_budget = |budget: u64| -> Result<SearchClient> {
        let reader = crate::search::quill_bridge::open_cass_reader_with_query_fuel_budget(
            dir.path(),
            budget,
        )?;
        SearchClient::from_opened_lexical_index(
            crate::search::tantivy::OpenedLexicalIndex {
                path: dir.path().to_path_buf(),
                reader: Some((
                    reader,
                    crate::search::quill_bridge::QuillCassFields::compiled(),
                )),
                federated_readers: None,
            },
            None,
            SearchClientOptions::default(),
        )?
        .context("a reader is present")
    };
    let plain = |budget: u64| {
        client_with_budget(budget)?.search(
            "needleword",
            SearchFilters::default(),
            10,
            0,
            FieldMask::FULL,
        )
    };
    let hi = (0..40)
        .map(|shift| 1_u64 << shift)
        .find(|budget| plain(*budget).is_ok())
        .expect("the plain lookup fits some budget");
    let mut lo = hi / 2;
    let mut hi_bound = hi;
    while lo < hi_bound {
        let mid = lo + (hi_bound - lo) / 2;
        if plain(mid).is_ok() {
            hi_bound = mid;
        } else {
            lo = mid + 1;
        }
    }
    let budget = hi_bound;
    // Precondition: at that budget the retry's walk does not fit.
    let refused = client_with_budget(budget)?
        .search(
            "*needleword*",
            SearchFilters::default(),
            10,
            0,
            FieldMask::FULL,
        )
        .expect_err("`*needleword*` must cost more fuel than the plain lookup");
    assert!(
        crate::search::quill_bridge::is_query_fuel_exhausted(&refused),
        "{refused:#}"
    );

    let client = client_with_budget(budget)?;
    let result = client.search_with_fallback(
        "needleword",
        SearchFilters::default(),
        10,
        0,
        5,
        FieldMask::FULL,
    )?;
    assert_eq!(result.hits.len(), 1, "budget {budget}: the plain hit");
    assert!(!result.wildcard_fallback);
    assert_eq!(
        client.wildcard_fallback_skipped_reason(),
        Some(WILDCARD_FALLBACK_SKIPPED_QUERY_FUEL_EXHAUSTED)
    );

    // Control: with fuel to spare the retry runs and nothing is skipped.
    let client = client_with_budget(budget.saturating_mul(1 << 20))?;
    let result = client.search_with_fallback(
        "needleword",
        SearchFilters::default(),
        10,
        0,
        5,
        FieldMask::FULL,
    )?;
    assert_eq!(result.hits.len(), 1);
    assert_eq!(client.wildcard_fallback_skipped_reason(), None);
    Ok(())
}

#[test]
fn search_with_fallback_skips_when_query_has_wildcards() -> Result<()> {
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
            content: "testing data".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    index.add_conversation(&conv)?;
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");

    // Query already has wildcards - should not trigger fallback
    let result = client.search_with_fallback(
        "*test*",
        SearchFilters::default(),
        10,
        0,
        10, // high threshold
        FieldMask::FULL,
    )?;

    assert!(!result.wildcard_fallback); // shouldn't trigger fallback for wildcard queries
    Ok(())
}

#[test]
fn search_with_fallback_prefers_wildcards_when_they_add_hits() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;

    // None of these documents contain the exact token "bet",
    // but they do contain it as a substring ("alphabet").
    for (i, body) in [
        "alphabet soup for coders",
        "mapping the alphabet city blocks",
    ]
    .iter()
    .enumerate()
    {
        let conv = NormalizedConversation {
            agent_slug: "codex".into(),
            external_id: None,
            title: Some(format!("alpha-{i}")),
            workspace: Some(std::path::PathBuf::from("/ws")),
            source_path: dir.path().join(format!("alpha-{i}.jsonl")),
            started_at: Some(100 + i as i64),
            ended_at: None,
            metadata: serde_json::json!({}),
            messages: vec![NormalizedMessage {
                idx: 0,
                role: "user".into(),
                author: None,
                created_at: Some(100 + i as i64),
                content: body.to_string(),
                extra: serde_json::json!({}),
                snippets: vec![],
                invocations: Vec::new(),
            }],
        };
        index.add_conversation(&conv)?;
    }
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");

    let result =
        client.search_with_fallback("bet", SearchFilters::default(), 10, 0, 2, FieldMask::FULL)?;

    assert!(
        result.wildcard_fallback,
        "should switch to wildcard fallback when it yields more hits"
    );
    assert_eq!(
        result.hits.len(),
        2,
        "fallback should surface all alphabet docs"
    );
    assert!(
        result
            .hits
            .iter()
            .all(|h| h.match_type == MatchType::ImplicitWildcard)
    );
    assert!(result.hits.iter().all(|h| h.content.contains("alphabet")));

    Ok(())
}

#[test]
fn automatic_wildcard_fallback_skips_long_zero_hit_token() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;

    let conv = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("fruit".into()),
        workspace: Some(std::path::PathBuf::from("/ws")),
        source_path: dir.path().join("fruit.jsonl"),
        started_at: Some(100),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(100),
            content: "apple pear banana".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    index.add_conversation(&conv)?;
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");

    let result = client.search_with_fallback(
        "zzzzzzunlikelyterm",
        SearchFilters::default(),
        10,
        0,
        1,
        FieldMask::FULL,
    )?;
    assert!(result.hits.is_empty());
    assert!(!result.wildcard_fallback);
    assert!(
        result
            .suggestions
            .iter()
            .any(|s| matches!(s.kind, SuggestionKind::WildcardQuery)),
        "manual wildcard suggestion should remain available"
    );

    let short_result =
        client.search_with_fallback("pple", SearchFilters::default(), 10, 0, 1, FieldMask::FULL)?;
    assert!(short_result.wildcard_fallback);
    assert_eq!(short_result.hits.len(), 1);
    assert_eq!(short_result.hits[0].match_type, MatchType::ImplicitWildcard);

    Ok(())
}

#[test]
fn nohit_suggestions_do_not_lazy_open_sqlite_when_tantivy_is_present() -> Result<()> {
    let dir = TempDir::new()?;
    let index_path = dir.path().join("index");
    let db_path = dir.path().join("cass.db");

    let storage = FrankenStorage::open(&db_path)?;
    storage.close()?;

    let mut index = TantivyIndex::open_or_create(&index_path)?;
    let conv = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("fruit".into()),
        workspace: Some(std::path::PathBuf::from("/ws")),
        source_path: dir.path().join("fruit.jsonl"),
        started_at: Some(100),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(100),
            content: "apple pear banana".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    index.add_conversation(&conv)?;
    index.commit()?;

    let client = SearchClient::open(&index_path, Some(&db_path))?.expect("index present");
    assert!(
        client
            .sqlite
            .lock()
            .map(|guard| guard.is_none())
            .unwrap_or(false),
        "sqlite should start closed"
    );

    let result = client.search_with_fallback(
        "zzzzzzunlikelyterm",
        SearchFilters::default(),
        10,
        0,
        1,
        FieldMask::FULL,
    )?;

    assert!(result.hits.is_empty());
    assert!(
        result
            .suggestions
            .iter()
            .any(|s| matches!(s.kind, SuggestionKind::WildcardQuery)),
        "manual wildcard suggestion should remain available"
    );
    assert!(
        result
            .suggestions
            .iter()
            .all(|s| !matches!(s.kind, SuggestionKind::AlternateAgent)),
        "alternate-agent suggestions should not force a SQLite open"
    );
    assert!(
        client
            .sqlite
            .lock()
            .map(|guard| guard.is_none())
            .unwrap_or(false),
        "sqlite should stay closed after Tantivy no-hit suggestions"
    );

    Ok(())
}

#[test]
fn search_with_fallback_emits_wildcard_suggestion_on_zero_hits() -> Result<()> {
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
        cache_namespace: "vtest|schema:none".into(),
        semantic: Mutex::new(None),
        last_tantivy_total_count: Mutex::new(None),
        last_lexical_degrade_reason: Mutex::new(None),
        last_wildcard_fallback_skip: Mutex::new(None),
    };

    let result =
        client.search_with_fallback("ghost", SearchFilters::default(), 5, 0, 3, FieldMask::FULL)?;

    assert!(
        result.hits.is_empty(),
        "no index/db means no hits should be returned"
    );
    assert!(
        !result.wildcard_fallback,
        "with zero baseline and fallback hits, we should keep baseline and mark fallback=false"
    );

    let wildcard = result
        .suggestions
        .iter()
        .find(|s| matches!(s.kind, SuggestionKind::WildcardQuery))
        .expect("should suggest adding wildcards");
    assert_eq!(wildcard.suggested_query.as_deref(), Some("*ghost*"));

    Ok(())
}

#[test]
fn search_with_fallback_skips_empty_query() -> Result<()> {
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
            content: "testing data".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    index.add_conversation(&conv)?;
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");

    // Empty query - should not trigger fallback
    let result =
        client.search_with_fallback("  ", SearchFilters::default(), 10, 0, 10, FieldMask::FULL)?;

    assert!(!result.wildcard_fallback);
    Ok(())
}

#[test]
fn search_with_fallback_skips_for_nonzero_offset() -> Result<()> {
    // Even with zero hits, fallback should not run when paginating (offset > 0)
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
        cache_namespace: "vtest|schema:none".into(),
        semantic: Mutex::new(None),
        last_tantivy_total_count: Mutex::new(None),
        last_lexical_degrade_reason: Mutex::new(None),
        last_wildcard_fallback_skip: Mutex::new(None),
    };

    let result = client.search_with_fallback(
        "ghost",
        SearchFilters::default(),
        5,
        10,
        3,
        FieldMask::FULL,
    )?;

    assert!(
        !result.wildcard_fallback,
        "fallback should not run on paginated searches"
    );
    // Suggestions still surface (wildcard suggestion expected)
    let wildcard = result
        .suggestions
        .iter()
        .find(|s| matches!(s.kind, SuggestionKind::WildcardQuery))
        .expect("wildcard suggestion present");
    assert_eq!(wildcard.suggested_query.as_deref(), Some("*ghost*"));

    Ok(())
}

#[test]
fn generate_suggestions_limits_and_sets_shortcuts() -> Result<()> {
    // Build a client without backends; suggestions are purely local heuristics
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
        cache_namespace: "vtest|schema:none".into(),
        semantic: Mutex::new(None),
        last_tantivy_total_count: Mutex::new(None),
        last_lexical_degrade_reason: Mutex::new(None),
        last_wildcard_fallback_skip: Mutex::new(None),
    };

    let mut filters = SearchFilters::default();
    filters.agents.insert("codex".into()); // triggers remove-agent suggestion

    let result = client.search_with_fallback("claud", filters, 5, 0, 3, FieldMask::FULL)?;

    // Should cap at 3 suggestions with shortcuts 1..=3
    assert_eq!(
        result.suggestions.len(),
        3,
        "should truncate to 3 suggestions"
    );
    for (idx, sugg) in result.suggestions.iter().enumerate() {
        assert_eq!(
            sugg.shortcut,
            Some((idx + 1) as u8),
            "shortcut should match position (1-based)"
        );
    }

    // Expect wildcard, remove filter, and spelling fix (claud -> claude)
    assert!(
        result
            .suggestions
            .iter()
            .any(|s| matches!(s.kind, SuggestionKind::WildcardQuery)),
        "should suggest wildcard search"
    );
    assert!(
        result
            .suggestions
            .iter()
            .any(|s| matches!(s.kind, SuggestionKind::RemoveFilter)),
        "should suggest removing agent filter"
    );
    assert!(
        result
            .suggestions
            .iter()
            .any(|s| matches!(s.kind, SuggestionKind::SpellingFix)),
        "should suggest spelling fix for nearby agent name"
    );

    Ok(())
}

#[test]
fn generate_suggestions_includes_recent_alternate_agents() -> Result<()> {
    let dir = TempDir::new()?;
    let db_path = dir.path().join("cass.db");
    let storage = FrankenStorage::open(&db_path)?;
    let workspace_id = storage.ensure_workspace(dir.path(), None)?;
    let base_ts = 1_700_000_010_000_i64;

    for (idx, slug) in ["claude_code", "codex"].iter().enumerate() {
        let agent = Agent {
            id: None,
            slug: (*slug).to_string(),
            name: (*slug).to_string(),
            version: None,
            kind: AgentKind::Cli,
        };
        let agent_id = storage.ensure_agent(&agent)?;
        let conversation = Conversation {
            id: None,
            agent_slug: (*slug).to_string(),
            workspace: Some(dir.path().to_path_buf()),
            external_id: Some(format!("alt-agent-{idx}")),
            title: Some(format!("alternate agent {idx}")),
            source_path: dir.path().join(format!("{slug}.jsonl")),
            started_at: Some(base_ts + idx as i64),
            ended_at: Some(base_ts + idx as i64),
            approx_tokens: Some(8),
            metadata_json: json!({}),
            messages: vec![Message {
                id: None,
                idx: 0,
                role: MessageRole::User,
                author: Some("user".into()),
                created_at: Some(base_ts + idx as i64),
                content: format!("content from {slug}"),
                extra_json: json!({}),
                snippets: Vec::new(),
            }],
            source_id: crate::sources::provenance::LOCAL_SOURCE_ID.to_string(),
            origin_host: None,
        };
        storage.insert_conversation_tree(agent_id, Some(workspace_id), &conversation)?;
    }
    drop(storage);

    let client = SearchClient::open(dir.path(), Some(&db_path))?.expect("db-backed client");
    let result =
        client.search_with_fallback("ghost", SearchFilters::default(), 5, 0, 3, FieldMask::FULL)?;

    let alternate_agents: HashSet<String> = result
        .suggestions
        .iter()
        .filter(|suggestion| matches!(suggestion.kind, SuggestionKind::AlternateAgent))
        .filter_map(|suggestion| suggestion.suggested_filters.as_ref())
        .flat_map(|filters| filters.agents.iter().cloned())
        .collect();

    assert!(
        alternate_agents.contains("claude_code"),
        "should suggest claude_code from normalized conversations schema"
    );
    assert!(
        alternate_agents.contains("codex"),
        "should suggest codex from normalized conversations schema"
    );

    Ok(())
}

#[test]
fn sanitize_query_preserves_wildcards() {
    // Wildcards should be preserved
    assert_eq!(fs_cass_sanitize_query("*foo*"), "*foo*");
    assert_eq!(fs_cass_sanitize_query("foo*"), "foo*");
    assert_eq!(fs_cass_sanitize_query("*bar"), "*bar");
    assert_eq!(fs_cass_sanitize_query("*config*"), "*config*");
}

#[test]
fn sanitize_query_strips_other_special_chars() {
    // Non-wildcard special chars become spaces
    assert_eq!(fs_cass_sanitize_query("foo.bar"), "foo bar");
    assert_eq!(fs_cass_sanitize_query("c++"), "c  ");
    assert_eq!(fs_cass_sanitize_query("foo-bar"), "foo-bar");
    assert_eq!(fs_cass_sanitize_query("test_case"), "test case");
}

#[test]
fn sanitize_query_combined() {
    // Mix of wildcards and special chars
    assert_eq!(fs_cass_sanitize_query("*foo.bar*"), "*foo bar*");
    assert_eq!(fs_cass_sanitize_query("test-*"), "test-*");
    assert_eq!(fs_cass_sanitize_query("*c++*"), "*c  *");
}

// Boolean query parsing tests
#[test]
fn parse_boolean_query_simple_terms() {
    let tokens = fs_cass_parse_boolean_query("foo bar baz");
    assert_eq!(tokens.len(), 3);
    assert_eq!(tokens[0], FsCassQueryToken::Term("foo".to_string()));
    assert_eq!(tokens[1], FsCassQueryToken::Term("bar".to_string()));
    assert_eq!(tokens[2], FsCassQueryToken::Term("baz".to_string()));
}

#[test]
fn parse_boolean_query_and_operator() {
    let tokens = fs_cass_parse_boolean_query("foo AND bar");
    assert_eq!(tokens.len(), 3);
    assert_eq!(tokens[0], FsCassQueryToken::Term("foo".to_string()));
    assert_eq!(tokens[1], FsCassQueryToken::And);
    assert_eq!(tokens[2], FsCassQueryToken::Term("bar".to_string()));

    // Also test && syntax
    let tokens2 = fs_cass_parse_boolean_query("foo && bar");
    assert_eq!(tokens2.len(), 3);
    assert_eq!(tokens2[1], FsCassQueryToken::And);
}

#[test]
fn parse_boolean_query_or_operator() {
    let tokens = fs_cass_parse_boolean_query("foo OR bar");
    assert_eq!(tokens.len(), 3);
    assert_eq!(tokens[0], FsCassQueryToken::Term("foo".to_string()));
    assert_eq!(tokens[1], FsCassQueryToken::Or);
    assert_eq!(tokens[2], FsCassQueryToken::Term("bar".to_string()));

    // Also test || syntax
    let tokens2 = fs_cass_parse_boolean_query("foo || bar");
    assert_eq!(tokens2.len(), 3);
    assert_eq!(tokens2[1], FsCassQueryToken::Or);
}

#[test]
fn parse_boolean_query_not_operator() {
    let tokens = fs_cass_parse_boolean_query("foo NOT bar");
    assert_eq!(tokens.len(), 3);
    assert_eq!(tokens[0], FsCassQueryToken::Term("foo".to_string()));
    assert_eq!(tokens[1], FsCassQueryToken::Not);
    assert_eq!(tokens[2], FsCassQueryToken::Term("bar".to_string()));
}

#[test]
fn parse_boolean_query_quoted_phrase() {
    let tokens = fs_cass_parse_boolean_query(r#"foo "exact phrase" bar"#);
    assert_eq!(tokens.len(), 3);
    assert_eq!(tokens[0], FsCassQueryToken::Term("foo".to_string()));
    assert_eq!(
        tokens[1],
        FsCassQueryToken::Phrase("exact phrase".to_string())
    );
    assert_eq!(tokens[2], FsCassQueryToken::Term("bar".to_string()));
}

#[test]
fn parse_boolean_query_complex() {
    let tokens = fs_cass_parse_boolean_query(r#"error OR warning NOT "false positive""#);
    assert_eq!(tokens.len(), 5);
    assert_eq!(tokens[0], FsCassQueryToken::Term("error".to_string()));
    assert_eq!(tokens[1], FsCassQueryToken::Or);
    assert_eq!(tokens[2], FsCassQueryToken::Term("warning".to_string()));
    assert_eq!(tokens[3], FsCassQueryToken::Not);
    assert_eq!(
        tokens[4],
        FsCassQueryToken::Phrase("false positive".to_string())
    );
}

#[test]
fn has_boolean_operators_detection() {
    assert!(!fs_cass_has_boolean_operators("foo bar"));
    assert!(fs_cass_has_boolean_operators("foo AND bar"));
    assert!(fs_cass_has_boolean_operators("foo OR bar"));
    assert!(fs_cass_has_boolean_operators("foo NOT bar"));
    assert!(fs_cass_has_boolean_operators(r#""exact phrase""#));
    assert!(fs_cass_has_boolean_operators("foo && bar"));
    assert!(fs_cass_has_boolean_operators("foo || bar"));
}

#[test]
fn parse_boolean_query_case_insensitive_operators() {
    // Operators should be case-insensitive
    let tokens = fs_cass_parse_boolean_query("foo and bar or baz not qux");
    assert_eq!(tokens.len(), 7);
    assert_eq!(tokens[1], FsCassQueryToken::And);
    assert_eq!(tokens[3], FsCassQueryToken::Or);
    assert_eq!(tokens[5], FsCassQueryToken::Not);
}

#[test]
fn parse_boolean_query_with_wildcards() {
    let tokens = fs_cass_parse_boolean_query("*config* OR env*");
    assert_eq!(tokens.len(), 3);
    assert_eq!(tokens[0], FsCassQueryToken::Term("*config*".to_string()));
    assert_eq!(tokens[1], FsCassQueryToken::Or);
    assert_eq!(tokens[2], FsCassQueryToken::Term("env*".to_string()));
}
