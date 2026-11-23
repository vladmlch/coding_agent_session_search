use super::super::*;
use super::helpers::*;
use crate::connectors::{NormalizedConversation, NormalizedMessage};
use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use crate::search::tantivy::TantivyIndex;
use crate::storage::sqlite::FrankenStorage;
use serde_json::json;
use tempfile::TempDir;

// ============================================================
// Filter Fidelity Property Tests (glt.9)
// Verify filters are never violated in search results
// ============================================================

#[test]
fn tantivy_search_hydrates_long_content_when_content_field_is_not_stored() -> Result<()> {
    let dir = TempDir::new()?;
    let db_path = dir.path().join("cass.db");
    let storage = FrankenStorage::open(&db_path)?;
    let workspace_id = storage.ensure_workspace(dir.path(), None)?;
    let agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: None,
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent)?;
    let long_content = format!(
        "{}needle appears past the preview boundary for hydration proof",
        "padding ".repeat(70)
    );
    let short_content = "shortneedle fits entirely inside the stored preview".to_string();
    let conversation = Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(dir.path().to_path_buf()),
        external_id: Some("hydrate-long-content".into()),
        title: Some("hydrated lexical doc".into()),
        source_path: dir.path().join("hydrate.jsonl"),
        started_at: Some(1_700_000_123_000),
        ended_at: Some(1_700_000_123_000),
        approx_tokens: Some(32),
        metadata_json: json!({}),
        messages: vec![
            Message {
                id: None,
                idx: 0,
                role: MessageRole::User,
                author: Some("user".into()),
                created_at: Some(1_700_000_123_000),
                content: long_content.clone(),
                extra_json: json!({}),
                snippets: Vec::new(),
            },
            Message {
                id: None,
                idx: 1,
                role: MessageRole::Agent,
                author: Some("assistant".into()),
                created_at: Some(1_700_000_124_000),
                content: short_content.clone(),
                extra_json: json!({}),
                snippets: Vec::new(),
            },
        ],
        source_id: crate::sources::provenance::LOCAL_SOURCE_ID.to_string(),
        origin_host: None,
    };
    storage.insert_conversation_tree(agent_id, Some(workspace_id), &conversation)?;
    storage.close()?;

    let index_path = dir.path().join("search-index");
    let mut index = TantivyIndex::open_or_create(&index_path)?;
    let normalized = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: Some("hydrate-long-content".into()),
        title: Some("hydrated lexical doc".into()),
        workspace: Some(dir.path().to_path_buf()),
        source_path: dir.path().join("hydrate.jsonl"),
        started_at: Some(1_700_000_123_000),
        ended_at: Some(1_700_000_123_000),
        metadata: json!({}),
        messages: vec![
            NormalizedMessage {
                idx: 0,
                role: "user".into(),
                author: Some("user".into()),
                created_at: Some(1_700_000_123_000),
                content: long_content.clone(),
                extra: json!({}),
                snippets: vec![],
                invocations: Vec::new(),
            },
            NormalizedMessage {
                idx: 1,
                role: "assistant".into(),
                author: Some("assistant".into()),
                created_at: Some(1_700_000_124_000),
                content: short_content.clone(),
                extra: json!({}),
                snippets: vec![],
                invocations: Vec::new(),
            },
        ],
    };
    index.add_conversation(&normalized)?;
    index.commit()?;

    let client = SearchClient::open(&index_path, Some(&db_path))?.expect("db-backed client");
    let hits = client.search("needle", SearchFilters::default(), 5, 0, FieldMask::FULL)?;

    assert_eq!(hits.len(), 1, "expected one lexical hit");
    assert_eq!(hits[0].title, "hydrated lexical doc");
    assert!(
        hits[0]
            .content
            .contains("needle appears past the preview boundary"),
        "lexical hit should hydrate full content from sqlite when Tantivy content is not stored"
    );
    assert!(
        hits[0].snippet.to_lowercase().contains("needle"),
        "snippet should still be rendered from hydrated content"
    );

    let bounded_hits = client.search(
        "needle",
        SearchFilters::default(),
        5,
        0,
        FieldMask::FULL.with_preview_content_limit(Some(200)),
    )?;

    assert_eq!(bounded_hits.len(), 1, "expected one lexical hit");
    assert!(
        bounded_hits[0].content.starts_with("padding padding"),
        "bounded content may be served from the stored preview prefix"
    );
    assert!(
        !bounded_hits[0]
            .content
            .contains("needle appears past the preview boundary"),
        "bounded preview content should not hydrate the full sqlite row"
    );

    let short_client = SearchClient::open(&index_path, Some(&db_path))?.expect("db-backed client");
    assert!(
        short_client
            .sqlite
            .lock()
            .map(|guard| guard.is_none())
            .unwrap_or(false),
        "sqlite should start closed for short preview hit"
    );

    let short_hits = short_client.search(
        "shortneedle",
        SearchFilters::default(),
        5,
        0,
        FieldMask::FULL,
    )?;

    assert_eq!(short_hits.len(), 1, "expected one short lexical hit");
    assert_eq!(
        short_hits[0].content, short_content,
        "untruncated stored preview is exact full content"
    );
    assert!(
        short_client
            .sqlite
            .lock()
            .map(|guard| guard.is_none())
            .unwrap_or(false),
        "short full-content hit should not lazy-open sqlite"
    );

    Ok(())
}

#[test]
fn filter_fidelity_agent_filter_respected() -> Result<()> {
    // Multiple agents; filter should return only matching agent
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;

    // Agent A (codex)
    let conv_a = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("alpha doc".into()),
        workspace: None,
        source_path: dir.path().join("a.jsonl"),
        started_at: Some(100),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(100),
            content: "hello world findme alpha".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    // Agent B (claude)
    let conv_b = NormalizedConversation {
        agent_slug: "claude".into(),
        external_id: None,
        title: Some("beta doc".into()),
        workspace: None,
        source_path: dir.path().join("b.jsonl"),
        started_at: Some(200),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(200),
            content: "hello world findme beta".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    index.add_conversation(&conv_a)?;
    index.add_conversation(&conv_b)?;
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");

    // Search with agent filter for codex only
    let mut filters = SearchFilters::default();
    filters.agents.insert("codex".into());

    let hits = client.search("findme", filters.clone(), 10, 0, FieldMask::FULL)?;

    // Property: all results must have agent == "codex"
    for hit in &hits {
        assert_eq!(
            hit.agent, "codex",
            "Agent filter violated: got agent '{}' instead of 'codex'",
            hit.agent
        );
    }
    assert!(!hits.is_empty(), "Should have found results");

    // Repeat search (should use cache) and verify same property
    let cached_hits = client.search("findme", filters, 10, 0, FieldMask::FULL)?;
    for hit in &cached_hits {
        assert_eq!(hit.agent, "codex", "Cached search violated agent filter");
    }

    Ok(())
}

#[test]
fn filter_fidelity_workspace_filter_respected() -> Result<()> {
    // Multiple workspaces; filter should return only matching workspace
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;

    // Workspace A
    let conv_a = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("ws_a doc".into()),
        workspace: Some(std::path::PathBuf::from("/workspace/alpha")),
        source_path: dir.path().join("a.jsonl"),
        started_at: Some(100),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(100),
            content: "workspace test needle".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    // Workspace B
    let conv_b = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("ws_b doc".into()),
        workspace: Some(std::path::PathBuf::from("/workspace/beta")),
        source_path: dir.path().join("b.jsonl"),
        started_at: Some(200),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(200),
            content: "workspace test needle".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    index.add_conversation(&conv_a)?;
    index.add_conversation(&conv_b)?;
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");

    // Search with workspace filter for beta only
    let mut filters = SearchFilters::default();
    filters.workspaces.insert("/workspace/beta".into());

    let hits = client.search("needle", filters.clone(), 10, 0, FieldMask::FULL)?;

    // Property: all results must have workspace == "/workspace/beta"
    for hit in &hits {
        assert_eq!(
            hit.workspace, "/workspace/beta",
            "Workspace filter violated: got '{}' instead of '/workspace/beta'",
            hit.workspace
        );
    }
    assert!(!hits.is_empty(), "Should have found results");

    // Repeat search (should use cache)
    let cached_hits = client.search("needle", filters, 10, 0, FieldMask::FULL)?;
    for hit in &cached_hits {
        assert_eq!(
            hit.workspace, "/workspace/beta",
            "Cached search violated workspace filter"
        );
    }

    Ok(())
}

#[test]
fn filter_fidelity_date_range_respected() -> Result<()> {
    // Multiple dates; filter should return only within range
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;

    // Early doc (ts=100)
    let conv_early = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("early".into()),
        workspace: None,
        source_path: dir.path().join("early.jsonl"),
        started_at: Some(100),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(100),
            content: "date range test".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    // Middle doc (ts=500)
    let conv_middle = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("middle".into()),
        workspace: None,
        source_path: dir.path().join("middle.jsonl"),
        started_at: Some(500),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(500),
            content: "date range test".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    // Late doc (ts=900)
    let conv_late = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("late".into()),
        workspace: None,
        source_path: dir.path().join("late.jsonl"),
        started_at: Some(900),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(900),
            content: "date range test".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    index.add_conversation(&conv_early)?;
    index.add_conversation(&conv_middle)?;
    index.add_conversation(&conv_late)?;
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");

    // Filter for middle range only (400-600)
    let filters = SearchFilters {
        created_from: Some(400),
        created_to: Some(600),
        ..Default::default()
    };

    let hits = client.search("range", filters.clone(), 10, 0, FieldMask::FULL)?;

    // Property: all results must have created_at within [400, 600]
    for hit in &hits {
        if let Some(ts) = hit.created_at {
            assert!(
                (400..=600).contains(&ts),
                "Date range filter violated: got ts={ts} outside [400, 600]"
            );
        }
    }
    // Should find only the middle doc
    assert_eq!(hits.len(), 1, "Should find exactly 1 doc in range");

    // Repeat search (cache)
    let cached_hits = client.search("range", filters, 10, 0, FieldMask::FULL)?;
    for hit in &cached_hits {
        if let Some(ts) = hit.created_at {
            assert!(
                (400..=600).contains(&ts),
                "Cached search violated date range filter"
            );
        }
    }

    Ok(())
}

#[test]
fn filter_fidelity_combined_filters_respected() -> Result<()> {
    // Combine agent + workspace + date filters
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;

    // Create 4 docs with different combinations
    let combinations = [
        ("codex", "/ws/prod", 100),  // wrong date
        ("claude", "/ws/prod", 500), // correct agent, correct ws, correct date
        ("claude", "/ws/dev", 500),  // correct agent, wrong ws, correct date
        ("claude", "/ws/prod", 900), // correct agent, correct ws, wrong date
    ];

    for (i, (agent, ws, ts)) in combinations.iter().enumerate() {
        let conv = NormalizedConversation {
            agent_slug: (*agent).into(),
            external_id: None,
            title: Some(format!("combo-{i}")),
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
                content: "hello world combotest query".into(),
                extra: serde_json::json!({}),
                snippets: vec![],
                invocations: Vec::new(),
            }],
        };
        index.add_conversation(&conv)?;
    }
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");

    // Filter: claude + /ws/prod + date 400-600
    let mut filters = SearchFilters::default();
    filters.agents.insert("claude".into());
    filters.workspaces.insert("/ws/prod".into());
    filters.created_from = Some(400);
    filters.created_to = Some(600);

    let hits = client.search("combotest", filters.clone(), 10, 0, FieldMask::FULL)?;

    // Should find exactly 1 doc (index 1 in combinations)
    assert_eq!(hits.len(), 1, "Combined filter should match exactly 1 doc");

    for hit in &hits {
        assert_eq!(hit.agent, "claude", "Agent filter violated");
        assert_eq!(hit.workspace, "/ws/prod", "Workspace filter violated");
        if let Some(ts) = hit.created_at {
            assert!((400..=600).contains(&ts), "Date filter violated: ts={ts}");
        }
    }

    // Cache hit
    let cached = client.search("combotest", filters, 10, 0, FieldMask::FULL)?;
    assert_eq!(cached.len(), 1, "Cached result count mismatch");

    Ok(())
}

#[test]
fn lexical_hits_normalize_trimmed_local_source_metadata() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;

    let conv = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("trimmed local doc".into()),
        workspace: None,
        source_path: dir.path().join("trimmed-local.jsonl"),
        started_at: Some(100),
        ended_at: None,
        metadata: serde_json::json!({
            "cass": {
                "origin": {
                    "source_id": "  LOCAL  ",
                    "kind": "local"
                }
            }
        }),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(100),
            content: "trimmed local lexical".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    index.add_conversation(&conv)?;
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");
    let hits = client.search("trimmed", SearchFilters::default(), 10, 0, FieldMask::FULL)?;

    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].source_id, "local");
    assert_eq!(hits[0].origin_kind, "local");

    Ok(())
}

#[test]
fn lexical_hits_normalize_remote_origin_kind_without_source_id() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;

    let conv = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("remote lexical doc".into()),
        workspace: None,
        source_path: dir.path().join("remote-lexical.jsonl"),
        started_at: Some(100),
        ended_at: None,
        metadata: serde_json::json!({
            "cass": {
                "origin": {
                    "source_id": "   ",
                    "kind": "ssh",
                    "host": "dev@laptop"
                }
            }
        }),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(100),
            content: "remote lexical".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    index.add_conversation(&conv)?;
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");
    let hits = client.search("remote", SearchFilters::default(), 10, 0, FieldMask::FULL)?;

    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].source_id, "dev@laptop");
    assert_eq!(hits[0].origin_kind, "remote");
    assert_eq!(hits[0].origin_host.as_deref(), Some("dev@laptop"));

    Ok(())
}

/// `--source remote` / `--source local` on the lexical lane must select by
/// origin kind. The index stores `origin_kind` as `local`/`remote`
/// (see `normalized_index_origin_kind`), while the Quill `Remote` filter
/// term is `ssh`, so `Remote` used to match nothing at all (bead 5bf29).
#[test]
fn lexical_source_filters_select_by_origin_kind() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;

    let conv_with_origin = |name: &str, origin: serde_json::Value| NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some(format!("{name} title")),
        workspace: None,
        source_path: dir.path().join(format!("{name}.jsonl")),
        started_at: Some(100),
        ended_at: None,
        metadata: serde_json::json!({ "cass": { "origin": origin } }),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(100),
            content: "sourcefilter proof".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    index.add_conversation(&conv_with_origin(
        "local-doc",
        serde_json::json!({ "source_id": "local", "kind": "local" }),
    ))?;
    index.add_conversation(&conv_with_origin(
        "ssh-doc",
        serde_json::json!({ "source_id": "laptop", "kind": "ssh", "host": "dev@laptop" }),
    ))?;
    index.add_conversation(&conv_with_origin(
        "backup-doc",
        serde_json::json!({ "source_id": "backup-local", "kind": "local" }),
    ))?;
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");
    let search = |filter: SourceFilter| -> Result<Vec<String>> {
        let hits = client.search(
            "sourcefilter",
            SearchFilters {
                source_filter: filter,
                ..Default::default()
            },
            10,
            0,
            FieldMask::FULL,
        )?;
        let mut ids: Vec<String> = hits.into_iter().map(|hit| hit.source_id).collect();
        ids.sort();
        Ok(ids)
    };

    assert_eq!(
        search(SourceFilter::All)?,
        vec!["backup-local", "laptop", "local"]
    );
    assert_eq!(
        search(SourceFilter::Remote)?,
        vec!["laptop"],
        "remote must select the ssh-origin document"
    );
    assert_eq!(
        search(SourceFilter::Local)?,
        vec!["backup-local", "local"],
        "local must select every local-kind origin, not only source_id == local"
    );
    assert_eq!(
        search(SourceFilter::SourceId("backup-local".into()))?,
        vec!["backup-local"]
    );
    Ok(())
}

#[test]
fn lexical_hits_infer_remote_origin_from_host_without_kind() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;

    let conv = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("legacy host-only lexical doc".into()),
        workspace: None,
        source_path: dir.path().join("legacy-host-only-lexical.jsonl"),
        started_at: Some(100),
        ended_at: None,
        metadata: serde_json::json!({
            "cass": {
                "origin": {
                    "source_id": "   ",
                    "host": "dev@laptop"
                }
            }
        }),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(100),
            content: "legacy remote lexical".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    index.add_conversation(&conv)?;
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");
    let hits = client.search("legacy", SearchFilters::default(), 10, 0, FieldMask::FULL)?;

    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].source_id, "dev@laptop");
    assert_eq!(hits[0].origin_kind, "remote");
    assert_eq!(hits[0].origin_host.as_deref(), Some("dev@laptop"));

    Ok(())
}

#[test]
fn filter_fidelity_source_filter_respected() -> Result<()> {
    // P3.1: Source filter should filter by origin_kind or source_id
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;

    // Local source doc
    let conv_local = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("local doc".into()),
        workspace: None,
        source_path: dir.path().join("local.jsonl"),
        started_at: Some(100),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(100),
            content: "source filter test local".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    // Remote source doc (would need to be indexed with ssh origin_kind)
    // For now, test that local filter returns local docs
    index.add_conversation(&conv_local)?;
    index.commit()?;

    let client = SearchClient::open(dir.path(), None)?.expect("index present");

    // Filter for local sources
    let filters = SearchFilters {
        source_filter: SourceFilter::Local,
        ..Default::default()
    };

    let hits = client.search("source", filters.clone(), 10, 0, FieldMask::FULL)?;

    // Property: all results should have source_id == "local"
    for hit in &hits {
        assert_eq!(
            hit.source_id, "local",
            "Source filter violated: got source_id '{}' instead of 'local'",
            hit.source_id
        );
    }
    assert!(!hits.is_empty(), "Should have found local results");

    // Filter for specific source ID
    let filters_id = SearchFilters {
        source_filter: SourceFilter::SourceId("  LOCAL  ".to_string()),
        ..Default::default()
    };

    let hits_id = client.search("source", filters_id, 10, 0, FieldMask::FULL)?;
    for hit in &hits_id {
        assert_eq!(
            hit.source_id, "local",
            "SourceId filter violated: got '{}' instead of 'local'",
            hit.source_id
        );
    }
    assert!(
        !hits_id.is_empty(),
        "Should have found results for source_id=local"
    );

    Ok(())
}

#[test]
fn filter_fidelity_cache_key_isolation() {
    // Different filters should have different cache keys
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

    let filters_empty = SearchFilters::default();
    let mut filters_agent = SearchFilters::default();
    filters_agent.agents.insert("codex".into());

    let mut filters_ws = SearchFilters::default();
    filters_ws.workspaces.insert("/ws".into());

    let key_empty = client.cache_key("test", &filters_empty, FieldMask::FULL);
    let key_agent = client.cache_key("test", &filters_agent, FieldMask::FULL);
    let key_ws = client.cache_key("test", &filters_ws, FieldMask::FULL);

    // All keys should be different
    assert_ne!(
        key_empty, key_agent,
        "Empty vs agent filter keys should differ"
    );
    assert_ne!(
        key_empty, key_ws,
        "Empty vs workspace filter keys should differ"
    );
    assert_ne!(
        key_agent, key_ws,
        "Agent vs workspace filter keys should differ"
    );

    // Same filter should produce same key
    let mut filters_agent2 = SearchFilters::default();
    filters_agent2.agents.insert("codex".into());
    let key_agent2 = client.cache_key("test", &filters_agent2, FieldMask::FULL);
    assert_eq!(key_agent, key_agent2, "Same filter should produce same key");
}

// ==========================================================================
// FTS5 Query Generation Tests (tst.srch.fts)
// Additional tests for SQL/FTS5 query generation edge cases
// ==========================================================================

// --- Additional sanitize_query tests (edge cases) ---

#[test]
fn sanitize_query_preserves_unicode_alphanumeric() {
    // Unicode letters and digits should be preserved
    assert_eq!(fs_cass_sanitize_query("こんにちは"), "こんにちは");
    assert_eq!(fs_cass_sanitize_query("café"), "café");
    assert_eq!(fs_cass_sanitize_query("日本語123"), "日本語123");
}

#[test]
fn sanitize_query_handles_multiple_consecutive_special_chars() {
    assert_eq!(fs_cass_sanitize_query("foo---bar"), "foo---bar");
    // a!@#$%^&()b has 9 special chars between a and b: ! @ # $ % ^ & ( )
    assert_eq!(fs_cass_sanitize_query("a!@#$%^&()b"), "a         b");
}

// --- Additional WildcardPattern::parse tests (edge cases) ---

#[test]
fn wildcard_pattern_empty_after_trim_returns_exact_empty() {
    assert_eq!(
        FsCassWildcardPattern::parse("*"),
        FsCassWildcardPattern::Exact(String::new())
    );
    assert_eq!(
        FsCassWildcardPattern::parse("**"),
        FsCassWildcardPattern::Exact(String::new())
    );
    assert_eq!(
        FsCassWildcardPattern::parse("***"),
        FsCassWildcardPattern::Exact(String::new())
    );
}

#[test]
fn wildcard_pattern_to_regex_generation() {
    // Exact and prefix patterns don't need regex
    assert_eq!(FsCassWildcardPattern::Exact("foo".into()).to_regex(), None);
    assert_eq!(FsCassWildcardPattern::Prefix("foo".into()).to_regex(), None);
    // Suffix and substring need regex. tantivy-fst regexes match the
    // complete term and reject zero-width assertions, so no `$` anchor.
    assert_eq!(
        FsCassWildcardPattern::Suffix("foo".into()).to_regex(),
        Some(".*foo".into())
    );
    assert_eq!(
        FsCassWildcardPattern::Substring("foo".into()).to_regex(),
        Some(".*foo.*".into())
    );
}

// --- Additional parse_boolean_query tests (edge cases) ---

#[test]
fn parse_boolean_query_prefix_minus_not() {
    // Prefix minus at start of query should trigger NOT
    let tokens = fs_cass_parse_boolean_query("-world");
    let expected = vec![
        FsCassQueryToken::Not,
        FsCassQueryToken::Term("world".into()),
    ];
    assert_eq!(tokens, expected);

    // Prefix minus after space should trigger NOT
    let tokens = fs_cass_parse_boolean_query("hello -world");
    let expected = vec![
        FsCassQueryToken::Term("hello".into()),
        FsCassQueryToken::Not,
        FsCassQueryToken::Term("world".into()),
    ];
    assert_eq!(tokens, expected);
}

#[test]
fn parse_boolean_query_empty_quoted_phrase_ignored() {
    let tokens = parse_boolean_query("\"\"");
    assert!(tokens.is_empty());

    let tokens = parse_boolean_query("foo \"\" bar");
    let expected: QueryTokenList = vec![
        QueryToken::Term("foo".into()),
        QueryToken::Term("bar".into()),
    ];
    assert_eq!(tokens, expected);
}

#[test]
fn parse_boolean_query_unclosed_quote() {
    // Unclosed quote should collect until end
    let tokens = parse_boolean_query("\"hello world");
    let expected: QueryTokenList = vec![QueryToken::Phrase("hello world".into())];
    assert_eq!(tokens, expected);
}

#[test]
fn transpile_to_fts5_rejects_leading_unary_not_queries() {
    assert_eq!(transpile_to_fts5("NOT foo"), None);
    assert_eq!(transpile_to_fts5("-foo"), None);
}

/// 2l1b0.52: the fallback follows the lexical parser's standard grammar,
/// parentheses included, and refuses only what FTS5 cannot express: a
/// complement (an OR operand, or a conjunction of NOTs alone).
#[test]
fn transpile_to_fts5_rejects_only_forms_fts5_cannot_express() {
    assert_eq!(transpile_to_fts5("foo OR NOT bar"), None);
    assert_eq!(transpile_to_fts5("NOT foo NOT bar"), None);
    assert_eq!(
        transpile_to_fts5("(foo OR bar) baz").as_deref(),
        Some("(foo OR bar) AND baz")
    );
    assert_eq!(
        transpile_to_fts5("foo -(bar OR baz)").as_deref(),
        Some("foo NOT (bar OR baz)")
    );
    assert_eq!(transpile_to_fts5("x&&(y)").as_deref(), Some("x AND y"));
    // Negative: without grouping this reads foo OR (bar AND baz).
    assert_eq!(
        transpile_to_fts5("foo OR bar baz").as_deref(),
        Some("foo OR (bar AND baz)")
    );
    assert_eq!(
        transpile_to_fts5("foo NOT bar OR baz").as_deref(),
        Some("(foo NOT bar) OR baz")
    );
    // A NOT with no operand is dropped, as the lexical parser drops it.
    assert_eq!(
        transpile_to_fts5("foo NOT OR bar").as_deref(),
        Some("foo OR bar")
    );
    // A `(` inside a word is a term character, not a group.
    assert_eq!(
        transpile_to_fts5("foo(bar)").as_deref(),
        Some("(foo AND bar)")
    );
}

#[test]
fn transpile_to_fts5_ignores_leading_or() {
    assert_eq!(transpile_to_fts5("OR test"), Some("test".to_string()));
    assert_eq!(
        transpile_to_fts5("OR foo-bar"),
        Some("(foo AND bar)".to_string())
    );
}

#[test]
fn transpile_to_fts5_splits_hyphenated_subterms_for_sqlite_fts() {
    assert_eq!(
        transpile_to_fts5("br-123.jsonl"),
        Some("(br AND 123 AND jsonl)".to_string())
    );
    assert_eq!(
        transpile_to_fts5("br-123.json*"),
        Some("(br AND 123 AND json*)".to_string())
    );
}

#[test]
fn transpile_to_fts5_preserves_supported_binary_not() {
    assert_eq!(
        transpile_to_fts5("foo NOT bar").as_deref(),
        Some("foo NOT bar")
    );
    assert_eq!(
        transpile_to_fts5("foo NOT bar-baz"),
        Some("foo NOT (bar AND baz)".to_string())
    );
    assert_eq!(
        transpile_to_fts5("foo OR bar NOT baz"),
        Some("foo OR (bar NOT baz)".to_string())
    );
}

#[test]
fn sqlite_only_unlimited_search_returns_all_matches_and_offset_suffixes() -> Result<()> {
    let fixture = build_semantic_test_fixture()?;
    assert!(!fixture.client.has_tantivy());
    assert_eq!(fixture.client.total_docs(), 0);
    assert!(transpile_to_fts5("*mantic").is_none());

    // Exercise both the ordinary SQLite query and the source-table scan
    // needed for a leading wildcard that FTS5 cannot represent.
    for query in ["semantic", "*mantic"] {
        let expected = fixture.client.search(
            query,
            SearchFilters::default(),
            fixture.doc_ids.len(),
            0,
            FieldMask::FULL,
        )?;
        assert_eq!(expected.len(), fixture.doc_ids.len(), "{query}");
        let mut expected_paths = expected
            .iter()
            .map(|hit| hit.source_path.clone())
            .collect::<Vec<_>>();
        expected_paths.sort();
        assert_eq!(expected_paths, fixture.source_paths, "{query}");

        for offset in [0, 1, expected.len()] {
            let actual = fixture.client.search(
                query,
                SearchFilters::default(),
                0,
                offset,
                FieldMask::FULL,
            )?;
            assert_eq!(
                serde_json::to_value(&actual)?,
                serde_json::to_value(&expected[offset..])?,
                "{query} with offset {offset} must retain the unlimited result suffix"
            );
        }
    }
    Ok(())
}

#[test]
fn sqlite_only_hybrid_preserves_complete_windows_and_empty_semantic_delegation() -> Result<()> {
    let fixture = build_semantic_test_fixture()?;
    assert!(!fixture.client.has_tantivy());
    let row_count = fixture.doc_ids.len();

    for semantic_query in ["semantic fixture query", ""] {
        let expected = fixture
            .client
            .search_hybrid(
                "semantic",
                semantic_query,
                SearchFilters::default(),
                row_count,
                0,
                0,
                FieldMask::FULL,
                false,
            )?
            .hits;
        assert_eq!(
            expected.len(),
            row_count,
            "semantic query {semantic_query:?}"
        );
        let mut expected_paths = expected
            .iter()
            .map(|hit| hit.source_path.clone())
            .collect::<Vec<_>>();
        expected_paths.sort();
        assert_eq!(expected_paths, fixture.source_paths);

        if semantic_query.is_empty() {
            let lexical = fixture.client.search(
                "semantic",
                SearchFilters::default(),
                row_count,
                0,
                FieldMask::FULL,
            )?;
            assert_eq!(
                serde_json::to_value(&expected)?,
                serde_json::to_value(&lexical)?,
                "an empty semantic query must preserve the lexical result"
            );
        }

        for limit in [row_count, 0] {
            for offset in [0, 1, row_count] {
                let actual = fixture
                    .client
                    .search_hybrid(
                        "semantic",
                        semantic_query,
                        SearchFilters::default(),
                        limit,
                        offset,
                        0,
                        FieldMask::FULL,
                        false,
                    )?
                    .hits;
                assert_eq!(
                    serde_json::to_value(&actual)?,
                    serde_json::to_value(&expected[offset..])?,
                    "semantic query {semantic_query:?}, limit {limit}, offset {offset}"
                );
            }
        }
    }
    Ok(())
}

#[test]
fn sqlite_only_result_cache_never_replays_forgotten_canonical_rows() -> Result<()> {
    let fixture = build_semantic_test_fixture()?;
    assert!(!fixture.client.has_tantivy());
    let db_path = fixture
        .client
        .sqlite_path
        .as_ref()
        .expect("fixture configured archive");
    let storage = FrankenStorage::open(db_path)?;
    let first =
        fixture
            .client
            .search("semantic", SearchFilters::default(), 1, 0, FieldMask::FULL)?;
    assert_eq!(first.len(), 1);
    let forgotten_path = first[0].source_path.clone();
    let forgotten = storage.forget_conversations_by_source_glob(&forgotten_path, false)?;
    assert_eq!(forgotten.conversations_deleted, 1);
    assert_eq!(forgotten.messages_matched, 1);
    storage.rebuild_fts()?;

    let cold = SearchClient::open_with_options(
        fixture._dir.path(),
        Some(db_path),
        SearchClientOptions {
            enable_reload: false,
            enable_warm: false,
            strict_read_only: true,
        },
    )?
    .expect("canonical fixture remains available");
    assert!(!cold.has_tantivy());
    let expected = cold.search("semantic", SearchFilters::default(), 1, 0, FieldMask::FULL)?;
    assert_eq!(expected.len(), 1);
    assert_ne!(expected[0].source_path, forgotten_path);
    for _ in 0..2 {
        let actual =
            fixture
                .client
                .search("semantic", SearchFilters::default(), 1, 0, FieldMask::FULL)?;
        assert_eq!(
            serde_json::to_value(&actual)?,
            serde_json::to_value(&expected)?
        );
        assert_eq!(fixture.client.cache_stats().cache_hits, 0);
    }
    Ok(())
}

#[test]
fn configured_archive_corruption_reaches_sqlite_search_and_date_browse() -> Result<()> {
    let dir = TempDir::new()?;
    let db_path = dir.path().join("corrupt-archive.db");
    let corrupt_bytes = vec![0xa5; 8192];
    std::fs::write(&db_path, &corrupt_bytes)?;

    for strict_read_only in [false, true] {
        let original_error =
            open_search_hydration_sqlite(&db_path, Duration::from_secs(1), strict_read_only)
                .expect_err("a non-SQLite file must fail the actual archive opener");
        let original_message = original_error.to_string();
        let original_cause = original_error.root_cause().to_string();
        let client = SearchClient::open_with_options(
            &dir.path().join("missing-lexical-index"),
            Some(&db_path),
            SearchClientOptions {
                enable_reload: false,
                enable_warm: false,
                strict_read_only,
            },
        )?
        .expect("the configured archive is opened lazily");
        assert!(!client.has_tantivy());

        let outcomes = [
            client.search("needle", SearchFilters::default(), 5, 0, FieldMask::FULL),
            client.search("*needle", SearchFilters::default(), 5, 0, FieldMask::FULL),
            client.browse_by_date(SearchFilters::default(), 5, 0, true, FieldMask::FULL),
        ];
        for outcome in outcomes {
            let error = outcome.expect_err(
                "a configured corrupt archive must not become a successful empty result",
            );
            let message = error.to_string();
            assert!(
                message.contains("opening configured search archive"),
                "{message}"
            );
            assert!(
                message.contains(&db_path.display().to_string()),
                "{message}"
            );
            assert!(message.contains(&original_message), "{message}");
            assert_eq!(error.root_cause().to_string(), original_cause);
        }
        assert_eq!(std::fs::read(&db_path)?, corrupt_bytes);
    }
    Ok(())
}

#[test]
fn configured_archive_disappearance_errors_and_a_restored_archive_can_retry() -> Result<()> {
    let mut fixture = build_semantic_test_fixture()?;
    fixture.client.strict_read_only = true;
    assert!(fixture.client.sqlite.lock().unwrap().is_none());
    let db_path = fixture
        .client
        .sqlite_path
        .as_ref()
        .expect("fixture configured archive");
    let original_bytes = std::fs::read(db_path)?;
    let parked_path = db_path.with_extension("parked");
    std::fs::rename(db_path, &parked_path)?;

    let outcomes = [
        fixture.client.search(
            "semantic fixture query",
            SearchFilters::default(),
            5,
            0,
            FieldMask::FULL,
        ),
        fixture
            .client
            .browse_by_date(SearchFilters::default(), 5, 0, true, FieldMask::FULL),
        fixture
            .client
            .search_semantic(
                "semantic fixture query",
                SearchFilters::default(),
                5,
                0,
                FieldMask::FULL,
                false,
            )
            .map(|(hits, _)| hits),
    ];
    for outcome in outcomes {
        let error = outcome.expect_err("a disappeared configured archive must remain an error");
        assert!(
            error
                .to_string()
                .contains("opening configured search archive"),
            "{error}"
        );
        assert!(error.chain().count() > 1, "the storage cause must survive");
    }
    assert!(
        !db_path.exists(),
        "search must not recreate the missing archive"
    );
    assert_eq!(std::fs::read(&parked_path)?, original_bytes);
    std::fs::rename(&parked_path, db_path)?;

    let (hits, _) = fixture.client.search_semantic(
        "semantic fixture query",
        SearchFilters::default(),
        5,
        0,
        FieldMask::FULL,
        false,
    )?;
    assert_eq!(hits.len(), fixture.doc_ids.len());
    assert_eq!(std::fs::read(db_path)?, original_bytes);
    Ok(())
}

#[test]
fn configured_archive_failure_keeps_complete_stored_lexical_hits_available() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;
    let content = "retained lexical evidence from the published generation";
    let conversation = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("retained evidence".into()),
        workspace: None,
        source_path: dir.path().join("retained.jsonl"),
        started_at: Some(1),
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(1),
            content: content.into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };
    index.add_conversation(&conversation)?;
    index.commit()?;
    let db_path = dir.path().join("corrupt-archive.db");
    let corrupt_bytes = vec![0xa5; 8192];
    std::fs::write(&db_path, &corrupt_bytes)?;

    for configured_db in [None, Some(db_path.as_path())] {
        let client = SearchClient::open_with_options(
            dir.path(),
            configured_db,
            SearchClientOptions {
                enable_reload: false,
                enable_warm: false,
                strict_read_only: true,
            },
        )?
        .expect("the published lexical index must remain usable");
        assert!(client.has_tantivy());
        if configured_db.is_some() {
            assert!(client.sqlite_guard().is_err());
            let (exact, fallback) = client.hydrate_tantivy_hit_contents(&[(1, 0)], &[])?;
            assert!(exact.is_empty() && fallback.is_empty());
        } else {
            assert!(client.sqlite_guard()?.is_none());
        }
        let hits = client.search("retained", SearchFilters::default(), 5, 0, FieldMask::FULL)?;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].content, content);
        assert_eq!(
            hits[0].source_path,
            conversation.source_path.to_string_lossy()
        );
    }
    assert_eq!(std::fs::read(&db_path)?, corrupt_bytes);
    Ok(())
}

#[test]
fn search_sqlite_fts5_returns_empty_when_sqlite_is_unavailable() {
    let client = SearchClient {
        reader: LexicalReaderSlot::default(),
        sqlite: Mutex::new(None),
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
        cache_namespace: "fts5-disabled".to_string(),
        semantic: Mutex::new(None),
        last_tantivy_total_count: Mutex::new(None),
        last_lexical_degrade_reason: Mutex::new(None),
        last_wildcard_fallback_skip: Mutex::new(None),
    };

    let hits = client.search_sqlite_fts5(
        Path::new("/nonexistent"),
        "test query",
        SearchFilters::default(),
        10,
        0,
        FieldMask::FULL,
    );

    assert!(hits.is_ok(), "disabled FTS5 path should stay non-fatal");
    assert!(
        hits.unwrap().is_empty(),
        "unavailable SQLite fallback should keep returning an empty result set"
    );
    let browse = client
        .browse_by_date(SearchFilters::default(), 10, 0, true, FieldMask::FULL)
        .expect("an absent optional SQLite backend stays non-fatal");
    assert!(browse.is_empty());
}
