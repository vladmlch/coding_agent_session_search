use super::super::*;
use super::helpers::*;
use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use crate::storage::sqlite::FrankenStorage;
use serde_json::json;
use tempfile::TempDir;

#[test]
fn hydrate_semantic_hits_with_ids_snippet_only_uses_full_content_for_snippets_and_identity()
-> Result<()> {
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
            started_at INTEGER
         );
         CREATE TABLE workspaces (id INTEGER PRIMARY KEY, path TEXT NOT NULL);
         CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            conversation_id INTEGER NOT NULL,
            idx INTEGER,
            role TEXT,
            content TEXT NOT NULL,
            created_at INTEGER
         );
         CREATE TABLE sources (id TEXT PRIMARY KEY, kind TEXT);",
    )?;
    conn.execute("INSERT INTO agents(id, slug) VALUES(1, 'codex')")?;
    conn.execute(
        "INSERT INTO conversations(id, agent_id, workspace_id, source_id, origin_host, title, source_path, started_at)
         VALUES(1, 1, NULL, 'local', NULL, 'semantic title', '/tmp/semantic.jsonl', 100)",
    )?;
    let shared_prefix = "shared-prefix ".repeat(32);
    let first = format!("{shared_prefix}first unique semantic tail");
    let second = format!("{shared_prefix}second unique semantic tail");
    conn.execute_with_params(
        "INSERT INTO messages(id, conversation_id, idx, role, content, created_at)
         VALUES(?1, 1, ?2, 'assistant', ?3, ?4)",
        &[
            crate::franken_sync::SqliteValue::Integer(1),
            crate::franken_sync::SqliteValue::Integer(0),
            crate::franken_sync::SqliteValue::Text(first.clone().into()),
            crate::franken_sync::SqliteValue::Integer(101),
        ],
    )?;
    conn.execute_with_params(
        "INSERT INTO messages(id, conversation_id, idx, role, content, created_at)
         VALUES(?1, 1, ?2, 'assistant', ?3, ?4)",
        &[
            crate::franken_sync::SqliteValue::Integer(2),
            crate::franken_sync::SqliteValue::Integer(1),
            crate::franken_sync::SqliteValue::Text(second.clone().into()),
            crate::franken_sync::SqliteValue::Integer(102),
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

    let hits = client.hydrate_semantic_hits_with_ids(
        &[
            VectorSearchResult {
                message_id: 1,
                chunk_idx: 0,
                score: 0.9,
            },
            VectorSearchResult {
                message_id: 2,
                chunk_idx: 0,
                score: 0.8,
            },
        ],
        FieldMask::new(false, true, true, true),
    )?;
    assert_eq!(hits.len(), 2);
    assert!(hits.iter().all(|(_, hit)| hit.content.is_empty()));
    assert!(hits.iter().all(|(_, hit)| !hit.snippet.is_empty()));
    assert_ne!(hits[0].1.content_hash, hits[1].1.content_hash);

    Ok(())
}

#[test]
fn cass_layer_b_projection_uses_alias_safe_real_sqlite_identity() -> Result<()> {
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

    let make_conversation =
        |external_id: &str, source_path: PathBuf, content: &str, timestamp: i64| Conversation {
            id: None,
            agent_slug: "codex".into(),
            workspace: Some(dir.path().to_path_buf()),
            external_id: Some(external_id.into()),
            title: Some(format!("Layer B {external_id}")),
            source_path,
            started_at: Some(timestamp),
            ended_at: Some(timestamp),
            approx_tokens: Some(8),
            metadata_json: json!({}),
            messages: vec![Message {
                id: None,
                idx: 0,
                role: MessageRole::User,
                author: Some("user".into()),
                created_at: Some(timestamp),
                content: content.into(),
                extra_json: json!({}),
                snippets: Vec::new(),
            }],
            source_id: crate::sources::provenance::LOCAL_SOURCE_ID.to_string(),
            origin_host: None,
        };

    let path_a = dir.path().join("conversation-a.jsonl");
    let path_b = dir.path().join("conversation-b.jsonl");
    let path_noise = dir.path().join("conversation-noise.jsonl");
    let outcome_a = storage.insert_conversation_tree(
        agent_id,
        Some(workspace_id),
        &make_conversation(
            "layer-b-a",
            path_a.clone(),
            "needle alpha from conversation a",
            1_700_000_001_000,
        ),
    )?;
    let outcome_b = storage.insert_conversation_tree(
        agent_id,
        Some(workspace_id),
        &make_conversation(
            "layer-b-b",
            path_b.clone(),
            "needle beta from conversation b",
            1_700_000_002_000,
        ),
    )?;
    let outcome_noise = storage.insert_conversation_tree(
        agent_id,
        Some(workspace_id),
        &make_conversation("layer-b-noise", path_noise, "ok", 1_700_000_003_000),
    )?;
    storage.close()?;

    let index_path = dir.path().join("absent-index");
    let client = SearchClient::open(&index_path, Some(&db_path))?
        .expect("SQLite-only client must be available");
    let candidates = [
        CassLexicalLayerBCandidate {
            conversation_id: outcome_b.conversation_id,
            message_index: 0,
            score_bits: 0.9_f32.to_bits(),
            rank: 0,
        },
        CassLexicalLayerBCandidate {
            conversation_id: outcome_noise.conversation_id,
            message_index: 0,
            score_bits: 0.85_f32.to_bits(),
            rank: 1,
        },
        CassLexicalLayerBCandidate {
            conversation_id: outcome_a.conversation_id,
            message_index: 0,
            score_bits: 0.8_f32.to_bits(),
            rank: 2,
        },
    ];
    let field_mask = FieldMask::new(true, false, true, false);
    let no_session_paths = HashSet::new();
    let second_visible = client.project_cass_lexical_layer_b_page(CassLexicalLayerBRequest {
        candidates: &candidates,
        raw_query: "needle*",
        session_paths: &no_session_paths,
        limit: 1,
        offset: 1,
        field_mask,
    })?;

    assert_eq!(second_visible.input_candidate_count, 3);
    assert_eq!(second_visible.hydrated_candidate_count, 3);
    assert_eq!(second_visible.available_hits, 2, "the 'ok' row is noise");
    assert_eq!(second_visible.hits.len(), 1);
    assert_eq!(second_visible.hits[0].input_rank, 2);
    assert_eq!(
        second_visible.hits[0].hit.score.to_bits(),
        0.8_f32.to_bits()
    );
    assert_eq!(second_visible.hits[0].hit.match_type, MatchType::Prefix);
    assert_eq!(
        second_visible.hits[0].hit.source_path,
        path_a.to_string_lossy()
    );
    assert!(
        second_visible.hits[0]
            .hit
            .content
            .contains("conversation a")
    );
    assert!(
        second_visible
            .no_claims
            .contains(&CassLexicalLayerBNoClaim::TotalCountPrecision)
    );

    // Both engine identities would historically render as `local#0`.
    // Exact conversation binding must nevertheless keep them distinct and
    // let the production session-path reducer select only conversation A.
    let only_a = HashSet::from([path_a.to_string_lossy().to_string()]);
    let filtered = client.project_cass_lexical_layer_b_page(CassLexicalLayerBRequest {
        candidates: &candidates,
        raw_query: "needle*",
        session_paths: &only_a,
        limit: 1,
        offset: 0,
        field_mask,
    })?;
    assert_eq!(filtered.available_hits, 1);
    assert_eq!(filtered.hits[0].input_rank, 2);
    assert_eq!(filtered.hits[0].hit.source_path, path_a.to_string_lossy());

    let missing = [CassLexicalLayerBCandidate {
        conversation_id: i64::MAX,
        message_index: 0,
        score_bits: 1.0_f32.to_bits(),
        rank: 0,
    }];
    let error = client
        .project_cass_lexical_layer_b_page(CassLexicalLayerBRequest {
            candidates: &missing,
            raw_query: "needle",
            session_paths: &no_session_paths,
            limit: 1,
            offset: 0,
            field_mask,
        })
        .expect_err("a missing exact identity must fail closed");
    assert!(matches!(
        error,
        CassLexicalLayerBError::UnresolvedIdentity {
            conversation_id: i64::MAX,
            message_index: 0
        }
    ));

    Ok(())
}

#[test]
fn cass_layer_b_projection_rejects_malformed_candidate_streams_before_sqlite() {
    let client = cass_layer_b_test_client(None);
    let no_session_paths = HashSet::new();
    let field_mask = FieldMask::new(true, false, true, false);
    let valid = CassLexicalLayerBCandidate {
        conversation_id: 1,
        message_index: 0,
        score_bits: 1.0_f32.to_bits(),
        rank: 0,
    };

    let oversized = vec![valid; CASS_LEXICAL_LAYER_B_CANDIDATE_MAX + 1];
    assert!(matches!(
        client.project_cass_lexical_layer_b_page(cass_layer_b_test_request(
            &oversized,
            &no_session_paths,
            1,
            0,
            field_mask,
        )),
        Err(CassLexicalLayerBError::CandidateBoundExceeded {
            actual,
            maximum: CASS_LEXICAL_LAYER_B_CANDIDATE_MAX,
        }) if actual == CASS_LEXICAL_LAYER_B_CANDIDATE_MAX + 1
    ));

    let noncanonical_rank = [CassLexicalLayerBCandidate { rank: 1, ..valid }];
    assert!(matches!(
        client.project_cass_lexical_layer_b_page(cass_layer_b_test_request(
            &noncanonical_rank,
            &no_session_paths,
            1,
            0,
            field_mask,
        )),
        Err(CassLexicalLayerBError::NonCanonicalRank {
            position: 0,
            actual_rank: 1,
        })
    ));

    let invalid_conversation = [CassLexicalLayerBCandidate {
        conversation_id: 0,
        ..valid
    }];
    assert!(matches!(
        client.project_cass_lexical_layer_b_page(cass_layer_b_test_request(
            &invalid_conversation,
            &no_session_paths,
            1,
            0,
            field_mask,
        )),
        Err(CassLexicalLayerBError::InvalidConversationId {
            rank: 0,
            conversation_id: 0,
        })
    ));

    let oversized_message_index = [CassLexicalLayerBCandidate {
        message_index: u64::MAX,
        ..valid
    }];
    assert!(matches!(
        client.project_cass_lexical_layer_b_page(cass_layer_b_test_request(
            &oversized_message_index,
            &no_session_paths,
            1,
            0,
            field_mask,
        )),
        Err(CassLexicalLayerBError::MessageIndexOutOfRange { rank: 0 })
    ));

    let non_finite_score = [CassLexicalLayerBCandidate {
        score_bits: f32::NAN.to_bits(),
        ..valid
    }];
    assert!(matches!(
        client.project_cass_lexical_layer_b_page(cass_layer_b_test_request(
            &non_finite_score,
            &no_session_paths,
            1,
            0,
            field_mask,
        )),
        Err(CassLexicalLayerBError::NonFiniteScore { rank: 0 })
    ));

    let duplicate_identity = [valid, CassLexicalLayerBCandidate { rank: 1, ..valid }];
    assert!(matches!(
        client.project_cass_lexical_layer_b_page(cass_layer_b_test_request(
            &duplicate_identity,
            &no_session_paths,
            1,
            0,
            field_mask,
        )),
        Err(CassLexicalLayerBError::DuplicateIdentity {
            conversation_id: 1,
            message_index: 0,
        })
    ));

    assert!(matches!(
        client.project_cass_lexical_layer_b_page(cass_layer_b_test_request(
            &[valid],
            &no_session_paths,
            0,
            0,
            field_mask,
        )),
        Err(CassLexicalLayerBError::ZeroLimit)
    ));
    assert!(matches!(
        client.project_cass_lexical_layer_b_page(cass_layer_b_test_request(
            &[valid],
            &no_session_paths,
            1,
            usize::MAX,
            field_mask,
        )),
        Err(CassLexicalLayerBError::PaginationOverflow)
    ));
    assert!(matches!(
        client.project_cass_lexical_layer_b_page(cass_layer_b_test_request(
            &[valid],
            &no_session_paths,
            1,
            0,
            FieldMask::new(true, true, true, false),
        )),
        Err(CassLexicalLayerBError::SnippetNotSupported)
    ));
    assert!(matches!(
        client.project_cass_lexical_layer_b_page(cass_layer_b_test_request(
            &[valid],
            &no_session_paths,
            1,
            0,
            field_mask.with_preview_content_limit(Some(32)),
        )),
        Err(CassLexicalLayerBError::BoundedPreviewNotSupported)
    ));
}

#[test]
fn cass_layer_b_projection_rejects_ambiguous_historical_identity() -> Result<()> {
    let conn = SearchSqliteFixture::in_memory()?;
    conn.execute_batch(
        "CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            conversation_id INTEGER NOT NULL,
            idx INTEGER NOT NULL
         );
         INSERT INTO messages(id, conversation_id, idx) VALUES(1, 7, 0);
         INSERT INTO messages(id, conversation_id, idx) VALUES(2, 7, 0);",
    )?;
    let client = cass_layer_b_test_client(Some(conn.into_connection()));
    let candidates = [CassLexicalLayerBCandidate {
        conversation_id: 7,
        message_index: 0,
        score_bits: 0.75_f32.to_bits(),
        rank: 0,
    }];
    let no_session_paths = HashSet::new();
    let error = client
        .project_cass_lexical_layer_b_page(cass_layer_b_test_request(
            &candidates,
            &no_session_paths,
            1,
            0,
            FieldMask::new(true, false, true, false),
        ))
        .expect_err("a historical duplicate identity must fail closed");
    assert!(matches!(
        error,
        CassLexicalLayerBError::UnresolvedIdentity {
            conversation_id: 7,
            message_index: 0,
        }
    ));

    Ok(())
}

#[test]
fn hydrate_semantic_hits_with_ids_normalizes_trimmed_local_source_metadata() -> Result<()> {
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
            started_at INTEGER
         );
         CREATE TABLE workspaces (id INTEGER PRIMARY KEY, path TEXT NOT NULL);
         CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            conversation_id INTEGER NOT NULL,
            idx INTEGER,
            role TEXT,
            content TEXT NOT NULL,
            created_at INTEGER
         );
         CREATE TABLE sources (id TEXT PRIMARY KEY, kind TEXT);",
    )?;
    conn.execute("INSERT INTO agents(id, slug) VALUES(1, 'codex')")?;
    conn.execute(
        "INSERT INTO conversations(id, agent_id, workspace_id, source_id, origin_host, title, source_path, started_at)
         VALUES(1, 1, NULL, '  local  ', NULL, 'trimmed local semantic', '/tmp/trimmed-local-semantic.jsonl', 100)",
    )?;
    conn.execute_with_params(
        "INSERT INTO messages(id, conversation_id, idx, role, content, created_at)
         VALUES(?1, 1, 0, 'assistant', ?2, 101)",
        &[
            crate::franken_sync::SqliteValue::Integer(1),
            crate::franken_sync::SqliteValue::Text("trimmed local semantic body".into()),
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

    let hits = client.hydrate_semantic_hits_with_ids(
        &[VectorSearchResult {
            message_id: 1,
            chunk_idx: 0,
            score: 0.9,
        }],
        FieldMask::new(false, true, true, true),
    )?;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].1.source_id, "local");
    assert_eq!(hits[0].1.origin_kind, "local");

    Ok(())
}

#[test]
fn hydrate_semantic_hits_with_ids_preserves_remote_origin_without_source_row() -> Result<()> {
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
            started_at INTEGER
         );
         CREATE TABLE workspaces (id INTEGER PRIMARY KEY, path TEXT NOT NULL);
         CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            conversation_id INTEGER NOT NULL,
            idx INTEGER,
            role TEXT,
            content TEXT NOT NULL,
            created_at INTEGER
         );
         CREATE TABLE sources (id TEXT PRIMARY KEY, kind TEXT);",
    )?;
    conn.execute("INSERT INTO agents(id, slug) VALUES(1, 'codex')")?;
    conn.execute(
        "INSERT INTO conversations(id, agent_id, workspace_id, source_id, origin_host, title, source_path, started_at)
         VALUES(1, 1, NULL, 'laptop', 'dev@laptop', 'remote semantic', '/tmp/remote-semantic.jsonl', 100)",
    )?;
    conn.execute_with_params(
        "INSERT INTO messages(id, conversation_id, idx, role, content, created_at)
         VALUES(?1, 1, 0, 'assistant', ?2, 101)",
        &[
            crate::franken_sync::SqliteValue::Integer(1),
            crate::franken_sync::SqliteValue::Text("remote semantic body".into()),
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

    let hits = client.hydrate_semantic_hits_with_ids(
        &[VectorSearchResult {
            message_id: 1,
            chunk_idx: 0,
            score: 0.9,
        }],
        FieldMask::new(false, true, true, true),
    )?;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].1.source_id, "laptop");
    assert_eq!(hits[0].1.origin_kind, "remote");
    assert_eq!(hits[0].1.origin_host.as_deref(), Some("dev@laptop"));

    Ok(())
}

#[test]
fn resolve_semantic_doc_ids_for_hits_distinguishes_same_source_path_line_by_content_hash()
-> Result<()> {
    let conn = SearchSqliteFixture::in_memory()?;
    conn.execute_batch(
        "CREATE TABLE agents (id INTEGER PRIMARY KEY, slug TEXT NOT NULL);
         CREATE TABLE sources (id TEXT PRIMARY KEY, kind TEXT);
         CREATE TABLE conversations (
            id INTEGER PRIMARY KEY,
            agent_id INTEGER NOT NULL,
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
            role TEXT,
            content TEXT NOT NULL,
            created_at INTEGER
         );",
    )?;
    conn.execute("INSERT INTO agents(id, slug) VALUES(1, 'codex')")?;
    conn.execute(
        "INSERT INTO conversations(id, agent_id, workspace_id, source_id, origin_host, title, source_path)
         VALUES(1, 1, NULL, 'local', NULL, 'Shared Session', '/tmp/progressive-shared.jsonl')",
    )?;
    conn.execute(
        "INSERT INTO conversations(id, agent_id, workspace_id, source_id, origin_host, title, source_path)
         VALUES(2, 1, NULL, 'local', NULL, 'Shared Session', '/tmp/progressive-shared.jsonl')",
    )?;
    let first = "same prefix first tail".to_string();
    let second = "same prefix second tail".to_string();
    conn.execute_with_params(
        "INSERT INTO messages(id, conversation_id, idx, role, content, created_at)
         VALUES(?1, ?2, 0, 'assistant', ?3, 100)",
        &[
            crate::franken_sync::SqliteValue::Integer(11),
            crate::franken_sync::SqliteValue::Integer(1),
            crate::franken_sync::SqliteValue::Text(first.clone().into()),
        ],
    )?;
    conn.execute_with_params(
        "INSERT INTO messages(id, conversation_id, idx, role, content, created_at)
         VALUES(?1, ?2, 0, 'assistant', ?3, 100)",
        &[
            crate::franken_sync::SqliteValue::Integer(22),
            crate::franken_sync::SqliteValue::Integer(2),
            crate::franken_sync::SqliteValue::Text(second.clone().into()),
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

    let first_hit = SearchHit {
        title: "Shared Session".into(),
        snippet: String::new(),
        content: String::new(),
        content_hash: stable_hit_hash(&first, "/tmp/progressive-shared.jsonl", Some(1), Some(100)),
        score: 0.0,
        source_path: "/tmp/progressive-shared.jsonl".into(),
        agent: "codex".into(),
        workspace: String::new(),
        workspace_original: None,
        created_at: Some(100),
        line_number: Some(1),
        match_type: MatchType::Exact,
        source_id: "local".into(),
        origin_kind: "local".into(),
        origin_host: None,
        conversation_id: None,
    };
    let second_hit = SearchHit {
        title: "Shared Session".into(),
        snippet: String::new(),
        content: String::new(),
        content_hash: stable_hit_hash(&second, "/tmp/progressive-shared.jsonl", Some(1), Some(100)),
        score: 0.0,
        source_path: "/tmp/progressive-shared.jsonl".into(),
        agent: "codex".into(),
        workspace: String::new(),
        workspace_original: None,
        created_at: Some(100),
        line_number: Some(1),
        match_type: MatchType::Exact,
        source_id: "local".into(),
        origin_kind: "local".into(),
        origin_host: None,
        conversation_id: None,
    };

    let resolved = client.resolve_semantic_doc_ids_for_hits(&[first_hit, second_hit])?;
    assert_eq!(resolved.len(), 2);
    assert_eq!(resolved[0].as_ref().map(|hit| hit.message_id), Some(11));
    assert_eq!(resolved[1].as_ref().map(|hit| hit.message_id), Some(22));
    assert_ne!(
        resolved[0].as_ref().map(|hit| hit.doc_id.as_str()),
        resolved[1].as_ref().map(|hit| hit.doc_id.as_str())
    );

    Ok(())
}

#[test]
fn hydrate_semantic_hits_with_ids_keeps_missing_title_empty() -> Result<()> {
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
            started_at INTEGER
         );
         CREATE TABLE workspaces (id INTEGER PRIMARY KEY, path TEXT NOT NULL);
         CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            conversation_id INTEGER NOT NULL,
            idx INTEGER,
            role TEXT,
            content TEXT NOT NULL,
            created_at INTEGER
         );
         CREATE TABLE sources (id TEXT PRIMARY KEY, kind TEXT);",
    )?;
    conn.execute("INSERT INTO agents(id, slug) VALUES(1, 'codex')")?;
    conn.execute(
        "INSERT INTO conversations(id, agent_id, workspace_id, source_id, origin_host, title, source_path, started_at)
         VALUES(1, 1, NULL, 'local', NULL, NULL, '/tmp/untitled-semantic.jsonl', 100)",
    )?;
    conn.execute_with_params(
        "INSERT INTO messages(id, conversation_id, idx, role, content, created_at)
         VALUES(?1, 1, 0, 'assistant', ?2, 101)",
        &[
            crate::franken_sync::SqliteValue::Integer(1),
            crate::franken_sync::SqliteValue::Text("untitled semantic body".into()),
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

    let hits = client.hydrate_semantic_hits_with_ids(
        &[VectorSearchResult {
            message_id: 1,
            chunk_idx: 0,
            score: 0.9,
        }],
        FieldMask::new(false, true, true, true),
    )?;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].1.title, "");

    Ok(())
}

#[test]
fn resolve_semantic_doc_ids_for_hits_prefers_conversation_id_over_ambiguous_provenance()
-> Result<()> {
    let conn = SearchSqliteFixture::in_memory()?;
    conn.execute_batch(
        "CREATE TABLE agents (id INTEGER PRIMARY KEY, slug TEXT NOT NULL);
         CREATE TABLE sources (id TEXT PRIMARY KEY, kind TEXT);
         CREATE TABLE conversations (
            id INTEGER PRIMARY KEY,
            agent_id INTEGER NOT NULL,
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
            role TEXT,
            content TEXT NOT NULL,
            created_at INTEGER
         );",
    )?;
    conn.execute("INSERT INTO agents(id, slug) VALUES(1, 'codex')")?;
    conn.execute(
        "INSERT INTO conversations(id, agent_id, workspace_id, source_id, origin_host, title, source_path)
         VALUES(1, 1, NULL, 'local', NULL, 'Shared Session', '/tmp/progressive-conversation-id.jsonl')",
    )?;
    conn.execute(
        "INSERT INTO conversations(id, agent_id, workspace_id, source_id, origin_host, title, source_path)
         VALUES(2, 1, NULL, 'local', NULL, 'Shared Session', '/tmp/progressive-conversation-id.jsonl')",
    )?;
    let content = "same ambiguous content".to_string();
    conn.execute_with_params(
        "INSERT INTO messages(id, conversation_id, idx, role, content, created_at)
         VALUES(?1, ?2, 0, 'assistant', ?3, 100)",
        &[
            crate::franken_sync::SqliteValue::Integer(11),
            crate::franken_sync::SqliteValue::Integer(1),
            crate::franken_sync::SqliteValue::Text(content.clone().into()),
        ],
    )?;
    conn.execute_with_params(
        "INSERT INTO messages(id, conversation_id, idx, role, content, created_at)
         VALUES(?1, ?2, 0, 'assistant', ?3, 100)",
        &[
            crate::franken_sync::SqliteValue::Integer(22),
            crate::franken_sync::SqliteValue::Integer(2),
            crate::franken_sync::SqliteValue::Text(content.clone().into()),
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

    let first_hit = SearchHit {
        title: "Shared Session".into(),
        snippet: String::new(),
        content: String::new(),
        content_hash: stable_hit_hash(
            &content,
            "/tmp/progressive-conversation-id.jsonl",
            Some(1),
            Some(100),
        ),
        score: 0.0,
        source_path: "/tmp/progressive-conversation-id.jsonl".into(),
        agent: "codex".into(),
        workspace: String::new(),
        workspace_original: None,
        created_at: Some(100),
        line_number: Some(1),
        match_type: MatchType::Exact,
        source_id: "local".into(),
        origin_kind: "local".into(),
        origin_host: None,
        conversation_id: Some(1),
    };
    let second_hit = SearchHit {
        conversation_id: Some(2),
        ..first_hit.clone()
    };

    let resolved = client.resolve_semantic_doc_ids_for_hits(&[first_hit, second_hit])?;
    assert_eq!(resolved.len(), 2);
    assert_eq!(resolved[0].as_ref().map(|hit| hit.message_id), Some(11));
    assert_eq!(resolved[1].as_ref().map(|hit| hit.message_id), Some(22));

    Ok(())
}

#[test]
fn resolve_semantic_doc_ids_for_hits_treats_null_source_as_local() -> Result<()> {
    let conn = SearchSqliteFixture::in_memory()?;
    conn.execute_batch(
        "CREATE TABLE agents (id INTEGER PRIMARY KEY, slug TEXT NOT NULL);
         CREATE TABLE sources (id TEXT PRIMARY KEY, kind TEXT);
         CREATE TABLE conversations (
            id INTEGER PRIMARY KEY,
            agent_id INTEGER NOT NULL,
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
            role TEXT,
            content TEXT NOT NULL,
            created_at INTEGER
         );",
    )?;
    conn.execute("INSERT INTO agents(id, slug) VALUES(1, 'codex')")?;
    conn.execute(
        "INSERT INTO conversations(id, agent_id, workspace_id, source_id, origin_host, title, source_path)
         VALUES(1, 1, NULL, NULL, NULL, 'Legacy Local', '/tmp/legacy-local.jsonl')",
    )?;
    let content = "legacy local semantic message".to_string();
    conn.execute_with_params(
        "INSERT INTO messages(id, conversation_id, idx, role, content, created_at)
         VALUES(?1, 1, 0, 'assistant', ?2, 100)",
        &[
            crate::franken_sync::SqliteValue::Integer(11),
            crate::franken_sync::SqliteValue::Text(content.clone().into()),
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

    let hit = SearchHit {
        title: "Legacy Local".into(),
        snippet: String::new(),
        content: String::new(),
        content_hash: stable_hit_hash(&content, "/tmp/legacy-local.jsonl", Some(1), Some(100)),
        score: 0.0,
        source_path: "/tmp/legacy-local.jsonl".into(),
        agent: "codex".into(),
        workspace: String::new(),
        workspace_original: None,
        created_at: Some(100),
        line_number: Some(1),
        match_type: MatchType::Exact,
        source_id: "local".into(),
        origin_kind: "local".into(),
        origin_host: None,
        conversation_id: None,
    };

    let resolved = client.resolve_semantic_doc_ids_for_hits(&[hit])?;
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].as_ref().map(|hit| hit.message_id), Some(11));

    Ok(())
}

#[test]
fn resolve_semantic_doc_ids_for_hits_matches_trimmed_local_source_id() -> Result<()> {
    let conn = SearchSqliteFixture::in_memory()?;
    conn.execute_batch(
        "CREATE TABLE agents (id INTEGER PRIMARY KEY, slug TEXT NOT NULL);
         CREATE TABLE sources (id TEXT PRIMARY KEY, kind TEXT);
         CREATE TABLE conversations (
            id INTEGER PRIMARY KEY,
            agent_id INTEGER NOT NULL,
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
            role TEXT,
            content TEXT NOT NULL,
            created_at INTEGER
         );",
    )?;
    conn.execute("INSERT INTO agents(id, slug) VALUES(1, 'codex')")?;
    conn.execute(
        "INSERT INTO conversations(id, agent_id, workspace_id, source_id, origin_host, title, source_path)
         VALUES(1, 1, NULL, '  local  ', NULL, 'Trimmed Local', '/tmp/trimmed-local.jsonl')",
    )?;
    let content = "trimmed local semantic message".to_string();
    conn.execute_with_params(
        "INSERT INTO messages(id, conversation_id, idx, role, content, created_at)
         VALUES(?1, 1, 0, 'assistant', ?2, 100)",
        &[
            crate::franken_sync::SqliteValue::Integer(11),
            crate::franken_sync::SqliteValue::Text(content.clone().into()),
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

    let hit = SearchHit {
        title: "Trimmed Local".into(),
        snippet: String::new(),
        content: String::new(),
        content_hash: stable_hit_hash(&content, "/tmp/trimmed-local.jsonl", Some(1), Some(100)),
        score: 0.0,
        source_path: "/tmp/trimmed-local.jsonl".into(),
        agent: "codex".into(),
        workspace: String::new(),
        workspace_original: None,
        created_at: Some(100),
        line_number: Some(1),
        match_type: MatchType::Exact,
        source_id: "local".into(),
        origin_kind: "local".into(),
        origin_host: None,
        conversation_id: None,
    };

    let resolved = client.resolve_semantic_doc_ids_for_hits(&[hit])?;
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].as_ref().map(|doc| doc.message_id), Some(11));

    Ok(())
}

#[test]
fn resolve_semantic_doc_ids_for_hits_normalizes_blank_local_source_id() -> Result<()> {
    let conn = SearchSqliteFixture::in_memory()?;
    conn.execute_batch(
        "CREATE TABLE agents (id INTEGER PRIMARY KEY, slug TEXT NOT NULL);
         CREATE TABLE sources (id TEXT PRIMARY KEY, kind TEXT);
         CREATE TABLE conversations (
            id INTEGER PRIMARY KEY,
            agent_id INTEGER NOT NULL,
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
            role TEXT,
            content TEXT NOT NULL,
            created_at INTEGER
         );",
    )?;
    conn.execute("INSERT INTO agents(id, slug) VALUES(1, 'codex')")?;
    conn.execute(
        "INSERT INTO conversations(id, agent_id, workspace_id, source_id, origin_host, title, source_path)
         VALUES(1, 1, NULL, 'local', NULL, 'Blank Local', '/tmp/blank-local.jsonl')",
    )?;
    let content = "blank local semantic message".to_string();
    conn.execute_with_params(
        "INSERT INTO messages(id, conversation_id, idx, role, content, created_at)
         VALUES(?1, 1, 0, 'assistant', ?2, 100)",
        &[
            crate::franken_sync::SqliteValue::Integer(11),
            crate::franken_sync::SqliteValue::Text(content.clone().into()),
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

    let hit = SearchHit {
        title: "Blank Local".into(),
        snippet: String::new(),
        content: String::new(),
        content_hash: stable_hit_hash(&content, "/tmp/blank-local.jsonl", Some(1), Some(100)),
        score: 0.0,
        source_path: "/tmp/blank-local.jsonl".into(),
        agent: "codex".into(),
        workspace: String::new(),
        workspace_original: None,
        created_at: Some(100),
        line_number: Some(1),
        match_type: MatchType::Exact,
        source_id: "   ".into(),
        origin_kind: "local".into(),
        origin_host: None,
        conversation_id: None,
    };

    let resolved = client.resolve_semantic_doc_ids_for_hits(&[hit])?;
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].as_ref().map(|doc| doc.message_id), Some(11));

    Ok(())
}

#[test]
fn resolve_semantic_doc_ids_for_hits_infers_remote_source_from_origin_host_when_source_id_blank()
-> Result<()> {
    let conn = SearchSqliteFixture::in_memory()?;
    conn.execute_batch(
        "CREATE TABLE agents (id INTEGER PRIMARY KEY, slug TEXT NOT NULL);
         CREATE TABLE sources (id TEXT PRIMARY KEY, kind TEXT);
         CREATE TABLE conversations (
            id INTEGER PRIMARY KEY,
            agent_id INTEGER NOT NULL,
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
            role TEXT,
            content TEXT NOT NULL,
            created_at INTEGER
         );",
    )?;
    conn.execute("INSERT INTO agents(id, slug) VALUES(1, 'codex')")?;
    conn.execute(
        "INSERT INTO conversations(id, agent_id, workspace_id, source_id, origin_host, title, source_path)
         VALUES(1, 1, NULL, '   ', 'dev@laptop', 'Legacy Remote', '/tmp/legacy-remote.jsonl')",
    )?;
    let content = "legacy remote semantic message".to_string();
    conn.execute_with_params(
        "INSERT INTO messages(id, conversation_id, idx, role, content, created_at)
         VALUES(?1, 1, 0, 'assistant', ?2, 100)",
        &[
            crate::franken_sync::SqliteValue::Integer(11),
            crate::franken_sync::SqliteValue::Text(content.clone().into()),
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

    let hit = SearchHit {
        title: "Legacy Remote".into(),
        snippet: String::new(),
        content: String::new(),
        content_hash: stable_hit_hash(&content, "/tmp/legacy-remote.jsonl", Some(1), Some(100)),
        score: 0.0,
        source_path: "/tmp/legacy-remote.jsonl".into(),
        agent: "codex".into(),
        workspace: String::new(),
        workspace_original: None,
        created_at: Some(100),
        line_number: Some(1),
        match_type: MatchType::Exact,
        source_id: "dev@laptop".into(),
        origin_kind: "remote".into(),
        origin_host: Some("dev@laptop".into()),
        conversation_id: None,
    };

    let resolved = client.resolve_semantic_doc_ids_for_hits(&[hit])?;
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].as_ref().map(|doc| doc.message_id), Some(11));

    Ok(())
}

#[test]
fn gh395_browse_by_date_snippet_only_uses_full_content_for_hit_identity() -> Result<()> {
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
         VALUES
            (1, 1, NULL, 'local', NULL, 'first browse title', '/tmp/browse-shared.jsonl'),
            (2, 1, NULL, 'local', NULL, 'second browse title', '/tmp/browse-shared.jsonl'),
            (3, 1, NULL, 'local', NULL, 'empty newest conversation', '/tmp/empty-browse.jsonl')",
    )?;
    // The newest candidate deliberately has no message. If LIMIT is
    // applied before excluding empty conversations, it consumes one of
    // the two page slots and the result is observably under-filled.
    let shared_prefix = "shared-prefix ".repeat(48);
    let first = format!("{shared_prefix}first browse-only tail");
    let second = format!("{shared_prefix}second browse-only tail");
    conn.execute_with_params(
        "INSERT INTO messages(id, conversation_id, idx, content, created_at)
         VALUES(?1, 1, ?2, ?3, ?4)",
        &[
            crate::franken_sync::SqliteValue::Integer(1),
            crate::franken_sync::SqliteValue::Integer(0),
            crate::franken_sync::SqliteValue::Text(first.clone().into()),
            crate::franken_sync::SqliteValue::Integer(101),
        ],
    )?;
    conn.execute_with_params(
        "INSERT INTO messages(id, conversation_id, idx, content, created_at)
         VALUES(?1, 2, ?2, ?3, ?4)",
        &[
            crate::franken_sync::SqliteValue::Integer(2),
            crate::franken_sync::SqliteValue::Integer(0),
            crate::franken_sync::SqliteValue::Text(second.clone().into()),
            crate::franken_sync::SqliteValue::Integer(102),
        ],
    )?;
    conn.execute(
        "INSERT INTO conversation_tail_state(
             conversation_id, ended_at, last_message_idx, last_message_created_at
         ) VALUES
            (1, 101, 0, 101),
            (2, 102, 0, 102),
            (3, 999, NULL, 999)",
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
        SearchFilters::default(),
        2,
        0,
        true,
        FieldMask::new(false, true, true, true),
    )?;
    assert_eq!(hits.len(), 2);
    assert!(hits.iter().all(|hit| hit.content.is_empty()));
    assert!(hits.iter().all(|hit| !hit.snippet.is_empty()));
    assert_eq!(hits[0].conversation_id, Some(2));
    assert_eq!(hits[1].conversation_id, Some(1));
    assert_ne!(hits[0].content_hash, hits[1].content_hash);

    Ok(())
}
