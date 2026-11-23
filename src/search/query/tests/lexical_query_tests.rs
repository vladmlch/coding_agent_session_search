use super::super::*;
use crate::connectors::{NormalizedConversation, NormalizedMessage, NormalizedSnippet};
use crate::search::tantivy::TantivyIndex;
use tempfile::TempDir;

#[test]
fn search_returns_results_with_filters_and_pagination() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;
    let conv = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("hello world convo".into()),
        workspace: Some(std::path::PathBuf::from("/tmp/workspace")),
        source_path: dir.path().join("rollout-1.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: Some("me".into()),
            created_at: Some(1_700_000_000_000),
            content: "hello rust world".into(),
            extra: serde_json::json!({}),
            snippets: vec![NormalizedSnippet {
                file_path: None,
                start_line: None,
                end_line: None,
                language: None,
                snippet_text: None,
            }],
            invocations: Vec::new(),
        }],
    };
    index.add_conversation(&conv)?;
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");
    let mut filters = SearchFilters::default();
    filters.agents.insert("codex".into());

    let hits = client.search("hello", filters, 10, 0, FieldMask::FULL)?;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].agent, "codex");
    assert!(hits[0].snippet.contains("hello"));
    Ok(())
}

#[test]
fn search_honors_created_range_and_workspace() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;

    let conv_a = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("needle one".into()),
        workspace: Some(std::path::PathBuf::from("/ws/a")),
        source_path: dir.path().join("a.jsonl"),
        started_at: Some(10),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(10),
            content: "alpha needle".into(),
            extra: serde_json::json!({}),
            snippets: vec![NormalizedSnippet {
                file_path: None,
                start_line: None,
                end_line: None,
                language: None,
                snippet_text: None,
            }],
            invocations: Vec::new(),
        }],
    };
    let conv_b = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("needle two".into()),
        workspace: Some(std::path::PathBuf::from("/ws/b")),
        source_path: dir.path().join("b.jsonl"),
        started_at: Some(20),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(20),
            content: "\nneedle second line".into(),
            extra: serde_json::json!({}),
            snippets: vec![NormalizedSnippet {
                file_path: None,
                start_line: None,
                end_line: None,
                language: None,
                snippet_text: None,
            }],
            invocations: Vec::new(),
        }],
    };
    index.add_conversation(&conv_a)?;
    index.add_conversation(&conv_b)?;
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");
    let mut filters = SearchFilters::default();
    filters.workspaces.insert("/ws/b".into());
    filters.created_from = Some(15);
    filters.created_to = Some(25);

    let hits = client.search("needle", filters, 10, 0, FieldMask::FULL)?;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].workspace, "/ws/b");
    assert!(hits[0].snippet.contains("second line"));
    Ok(())
}

#[test]
fn pagination_skips_results() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;
    for i in 0..3 {
        let conv = NormalizedConversation {
            agent_slug: "codex".into(),
            external_id: None,
            title: Some(format!("doc-{i}")),
            workspace: Some(std::path::PathBuf::from("/ws/p")),
            source_path: dir.path().join(format!("{i}.jsonl")),
            started_at: Some(100 + i),
            ended_at: None,
            metadata: serde_json::json!({}),
            messages: vec![NormalizedMessage {
                idx: 0,
                role: "user".into(),
                author: None,
                created_at: Some(100 + i),
                // Use unique content for each doc to avoid deduplication
                content: format!("pagination needle document number {i}"),
                extra: serde_json::json!({}),
                snippets: vec![NormalizedSnippet {
                    file_path: None,
                    start_line: None,
                    end_line: None,
                    language: None,
                    snippet_text: None,
                }],
                invocations: Vec::new(),
            }],
        };
        index.add_conversation(&conv)?;
    }
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");
    let hits = client.search(
        "pagination",
        SearchFilters::default(),
        1,
        1,
        FieldMask::FULL,
    )?;
    assert_eq!(hits.len(), 1);
    Ok(())
}

#[test]
fn search_matches_hyphenated_term() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;
    let conv = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("cma-es notes".into()),
        workspace: Some(std::path::PathBuf::from("/tmp/workspace")),
        source_path: dir.path().join("rollout-1.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: Some("me".into()),
            created_at: Some(1_700_000_000_000),
            content: "Need CMA-ES strategy and CMA ES variants".into(),
            extra: serde_json::json!({}),
            snippets: vec![NormalizedSnippet {
                file_path: None,
                start_line: None,
                end_line: None,
                language: None,
                snippet_text: None,
            }],
            invocations: Vec::new(),
        }],
    };
    index.add_conversation(&conv)?;
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");
    let hits = client.search("cma-es", SearchFilters::default(), 10, 0, FieldMask::FULL)?;
    assert_eq!(hits.len(), 1);
    assert!(hits[0].snippet.to_lowercase().contains("cma"));
    Ok(())
}

#[test]
fn search_matches_prefix_edge_ngram() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;
    let conv = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("math logic".into()),
        workspace: Some(std::path::PathBuf::from("/ws/m")),
        source_path: dir.path().join("math.jsonl"),
        started_at: Some(1000),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(1000),
            content: "please calculate the entropy".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    index.add_conversation(&conv)?;
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");

    // "cal" should match "calculate"
    let hits = client.search("cal", SearchFilters::default(), 10, 0, FieldMask::FULL)?;
    assert_eq!(hits.len(), 1);
    assert!(hits[0].content.contains("calculate"));

    // "entr" should match "entropy"
    let hits = client.search("entr", SearchFilters::default(), 10, 0, FieldMask::FULL)?;
    assert_eq!(hits.len(), 1);

    Ok(())
}

#[test]
fn search_matches_snake_case() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;
    let conv = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("code".into()),
        workspace: None,
        source_path: dir.path().join("c.jsonl"),
        started_at: Some(1),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(1),
            content: "check the my_variable_name please".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    index.add_conversation(&conv)?;
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");

    // "vari" should match "variable" inside "my_variable_name"
    let hits = client.search("vari", SearchFilters::default(), 10, 0, FieldMask::FULL)?;
    assert_eq!(hits.len(), 1);

    // "my_variable" should match "my_variable_name" (because it splits to "my variable")
    let hits = client.search(
        "my_variable",
        SearchFilters::default(),
        10,
        0,
        FieldMask::FULL,
    )?;
    assert_eq!(hits.len(), 1);

    Ok(())
}

#[test]
fn search_matches_symbols_stripped() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;
    let conv = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("symbols".into()),
        workspace: None,
        source_path: dir.path().join("s.jsonl"),
        started_at: Some(1),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(1),
            content: "working with c++ and foo.bar today".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    index.add_conversation(&conv)?;
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");

    // "c++" -> "c"
    let hits = client.search("c++", SearchFilters::default(), 10, 0, FieldMask::FULL)?;
    assert_eq!(hits.len(), 1);

    // "foo.bar" -> "foo", "bar"
    let hits = client.search("foo.bar", SearchFilters::default(), 10, 0, FieldMask::FULL)?;
    assert_eq!(hits.len(), 1);

    Ok(())
}

#[test]
fn search_sets_match_type_for_wildcards() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;

    let conv = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("handlers".into()),
        workspace: None,
        source_path: dir.path().join("h.jsonl"),
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

    let exact = client.search("handler", SearchFilters::default(), 10, 0, FieldMask::FULL)?;
    assert_eq!(exact[0].match_type, MatchType::Exact);

    let prefix = client.search("hand*", SearchFilters::default(), 10, 0, FieldMask::FULL)?;
    assert_eq!(prefix[0].match_type, MatchType::Prefix);

    let suffix = client.search("*handler", SearchFilters::default(), 10, 0, FieldMask::FULL)?;
    assert_eq!(suffix[0].match_type, MatchType::Suffix);

    let substring = client.search("*andle*", SearchFilters::default(), 10, 0, FieldMask::FULL)?;
    assert_eq!(substring[0].match_type, MatchType::Substring);

    Ok(())
}
