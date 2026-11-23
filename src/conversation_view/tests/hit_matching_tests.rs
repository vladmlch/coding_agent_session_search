use super::super::loader::{canonical_message_number, conversation_view_matches_hit};
use super::super::*;
use crate::franken_sync::compat::ConnectionExt;
use crate::model::types::{Conversation, Message, MessageRole};
use crate::search::query::{MatchType, SearchHit};
use crate::storage::sqlite::FrankenStorage;
use tempfile::tempdir;

#[test]
fn gh493_canonical_message_numbers_reject_invalid_indices_without_position_fallback() {
    assert_eq!(canonical_message_number(-1), None);
    assert_eq!(canonical_message_number(i64::MIN), None);
    assert_eq!(canonical_message_number(0), Some(1));
    assert_eq!(canonical_message_number(128), Some(129));
    assert_eq!(canonical_message_number(540), Some(541));
    assert_eq!(
        canonical_message_number(i64::MAX),
        usize::try_from(i64::MAX)
            .ok()
            .and_then(|idx| idx.checked_add(1))
    );
}

fn gh493_sparse_view_fixture() -> (tempfile::TempDir, FrankenStorage, SearchHit) {
    let root = tempdir().expect("tempdir");
    let storage = FrankenStorage::open(&root.path().join("cass.db")).expect("archive");
    storage.raw().execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'codex', 'Codex', 'local', 0, 0)").unwrap();
    storage.raw().execute("INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'sparse', 'Sparse', '/gh493/shared.db', 'local', 100)").unwrap();
    for (id, idx) in [(1_i64, 1_i64), (2, 16), (3, 89)] {
        storage.raw().execute_compat(
            "INSERT INTO messages (id, conversation_id, idx, role, created_at, content) VALUES (?1, 1, ?2, 'user', 101, 'identical repeated body')",
            crate::franken_sync::params![id, idx],
        ).unwrap();
    }
    let hit = SearchHit {
        title: "Sparse".into(),
        snippet: "identical repeated body".into(),
        content: "identical repeated body".into(),
        content_hash: 0,
        conversation_id: Some(1),
        score: 1.0,
        agent: "codex".into(),
        source_path: "/gh493/shared.db".into(),
        source_id: "local".into(),
        origin_kind: "local".into(),
        origin_host: None,
        workspace: String::new(),
        workspace_original: None,
        created_at: Some(101),
        line_number: Some(17),
        match_type: MatchType::Exact,
    };
    (root, storage, hit)
}

#[test]
fn gh493_hit_lookup_never_uses_dense_positions_or_ignores_source_identity() {
    let (_root, storage, hit) = gh493_sparse_view_fixture();
    let view = load_conversation_for_hit(&storage, &hit).unwrap().unwrap();
    for conversation_id in [Some(1), None] {
        for (number, valid) in [
            (0, false),
            (1, false),
            (2, true),
            (3, false),
            (17, true),
            (90, true),
            (91, false),
        ] {
            let mut request = hit.clone();
            request.conversation_id = conversation_id;
            request.line_number = Some(number);
            assert_eq!(
                conversation_view_matches_hit(&view, &request),
                valid,
                "stored ordinal {number}, conversation {conversation_id:?}"
            );
            assert_eq!(
                load_conversation_for_hit(&storage, &request)
                    .unwrap()
                    .is_some(),
                valid
            );
        }
    }
    for (source, path) in [("remote", "/gh493/shared.db"), ("local", "/gh493/other.db")] {
        let mut request = hit.clone();
        request.source_id = source.into();
        request.source_path = path.into();
        assert!(!conversation_view_matches_hit(&view, &request));
        assert!(
            load_conversation_for_hit(&storage, &request)
                .unwrap()
                .is_none()
        );
    }
    let mut duplicate_view = view.clone();
    duplicate_view.messages.push(view.messages[1].clone());
    assert!(!conversation_view_matches_hit(&duplicate_view, &hit));
    let mut truncated = hit;
    truncated.title = "old display title".into();
    truncated.content = "truncated display payload".into();
    assert!(
        conversation_view_matches_hit(&view, &truncated),
        "exact source/path/conversation/ordinal is independent of display text"
    );
}

#[test]
fn gh493_legacy_hit_ambiguity_cannot_be_hidden_by_a_path_cache() {
    let (_root, storage, mut hit) = gh493_sparse_view_fixture();
    // Populate the ordinary path cache before another session appears.
    load_conversation_for_source(&storage, "local", &hit.source_path)
        .unwrap()
        .unwrap();
    storage.raw().execute("INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (2, 1, 'twin', 'Sparse', '/gh493/shared.db', 'local', 200)").unwrap();
    storage.raw().execute("INSERT INTO messages (id, conversation_id, idx, role, created_at, content) VALUES (4, 2, 16, 'user', 101, 'identical repeated body')").unwrap();
    hit.conversation_id = None;
    assert!(
        load_conversation_for_hit(&storage, &hit).unwrap().is_none(),
        "matching text in two sessions is ambiguous, even with a warm cache"
    );
    for id in [1, 2] {
        hit.conversation_id = Some(id);
        assert_eq!(
            load_conversation_for_hit(&storage, &hit)
                .unwrap()
                .unwrap()
                .convo
                .id,
            Some(id)
        );
    }
    hit.conversation_id = Some(999);
    assert!(load_conversation_for_hit(&storage, &hit).unwrap().is_none());
}

#[test]
fn conversation_view_matches_hit_normalizes_blank_remote_source_id_via_origin_host() {
    let view = ConversationView {
        convo: Conversation {
            id: Some(1),
            agent_slug: "claude_code".to_string(),
            workspace: None,
            external_id: Some("ext-1".to_string()),
            title: Some("Session".to_string()),
            source_path: std::path::PathBuf::from("/shared/session.jsonl"),
            started_at: Some(100),
            ended_at: None,
            approx_tokens: None,
            metadata_json: serde_json::Value::Null,
            messages: Vec::new(),
            source_id: "user@laptop".to_string(),
            origin_host: Some("user@laptop".to_string()),
        },
        messages: vec![Message {
            id: Some(1),
            idx: 0,
            role: MessageRole::User,
            author: None,
            created_at: Some(101),
            content: "body".to_string(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        workspace: None,
    };

    let hit = SearchHit {
        title: "Session".to_string(),
        snippet: String::new(),
        content: String::new(),
        content_hash: 0,
        score: 0.0,
        conversation_id: None,
        source_path: "/shared/session.jsonl".to_string(),
        agent: "claude_code".to_string(),
        workspace: String::new(),
        workspace_original: None,
        created_at: None,
        line_number: None,
        match_type: Default::default(),
        source_id: "   ".to_string(),
        origin_kind: "remote".to_string(),
        origin_host: Some("user@laptop".to_string()),
    };

    assert!(conversation_view_matches_hit(&view, &hit));
}

#[test]
fn conversation_view_matches_hit_normalizes_local_source_id_variants() {
    let view = ConversationView {
        convo: Conversation {
            id: Some(1),
            agent_slug: "claude_code".to_string(),
            workspace: None,
            external_id: Some("ext-1".to_string()),
            title: Some("Session".to_string()),
            source_path: std::path::PathBuf::from("/shared/session.jsonl"),
            started_at: Some(100),
            ended_at: None,
            approx_tokens: None,
            metadata_json: serde_json::Value::Null,
            messages: Vec::new(),
            source_id: "local".to_string(),
            origin_host: None,
        },
        messages: vec![Message {
            id: Some(1),
            idx: 0,
            role: MessageRole::User,
            author: None,
            created_at: Some(101),
            content: "body".to_string(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        workspace: None,
    };

    let hit = SearchHit {
        title: "Session".to_string(),
        snippet: String::new(),
        content: String::new(),
        content_hash: 0,
        score: 0.0,
        conversation_id: None,
        source_path: "/shared/session.jsonl".to_string(),
        agent: "claude_code".to_string(),
        workspace: String::new(),
        workspace_original: None,
        created_at: None,
        line_number: None,
        match_type: Default::default(),
        source_id: "  LOCAL  ".to_string(),
        origin_kind: "local".to_string(),
        origin_host: None,
    };

    assert!(conversation_view_matches_hit(&view, &hit));
}

#[test]
fn conversation_view_matches_hit_refuses_stale_conversation_id_even_with_matching_hints() {
    let view = ConversationView {
        convo: Conversation {
            id: Some(1),
            agent_slug: "claude_code".to_string(),
            workspace: None,
            external_id: Some("ext-1".to_string()),
            title: Some("Session".to_string()),
            source_path: std::path::PathBuf::from("/shared/session.jsonl"),
            started_at: Some(100),
            ended_at: None,
            approx_tokens: None,
            metadata_json: serde_json::Value::Null,
            messages: Vec::new(),
            source_id: "local".to_string(),
            origin_host: None,
        },
        messages: vec![Message {
            id: Some(1),
            idx: 0,
            role: MessageRole::User,
            author: None,
            created_at: Some(101),
            content: "body".to_string(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        workspace: None,
    };

    let hit = SearchHit {
        title: "Session".to_string(),
        snippet: String::new(),
        content: "body".to_string(),
        content_hash: 0,
        score: 0.0,
        conversation_id: Some(999),
        source_path: "/shared/session.jsonl".to_string(),
        agent: "claude_code".to_string(),
        workspace: String::new(),
        workspace_original: None,
        created_at: Some(101),
        line_number: Some(1),
        match_type: Default::default(),
        source_id: "local".to_string(),
        origin_kind: "local".to_string(),
        origin_host: None,
    };

    assert!(!conversation_view_matches_hit(&view, &hit));
    let mut exact = hit.clone();
    exact.conversation_id = view.convo.id;
    assert!(conversation_view_matches_hit(&view, &exact));
    let mut legacy = hit;
    legacy.conversation_id = None;
    assert!(conversation_view_matches_hit(&view, &legacy));
}

#[test]
fn conversation_view_matches_hit_rejects_stale_conversation_id_without_other_hints() {
    let view = ConversationView {
        convo: Conversation {
            id: Some(1),
            agent_slug: "claude_code".to_string(),
            workspace: None,
            external_id: Some("ext-1".to_string()),
            title: Some("Session".to_string()),
            source_path: std::path::PathBuf::from("/shared/session.jsonl"),
            started_at: Some(100),
            ended_at: None,
            approx_tokens: None,
            metadata_json: serde_json::Value::Null,
            messages: vec![],
            source_id: "local".to_string(),
            origin_host: None,
        },
        messages: vec![Message {
            id: Some(1),
            idx: 0,
            role: MessageRole::User,
            author: None,
            created_at: Some(101),
            content: "body".to_string(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        workspace: None,
    };

    let hit = SearchHit {
        title: String::new(),
        snippet: String::new(),
        content: String::new(),
        content_hash: 0,
        score: 0.0,
        conversation_id: Some(999),
        source_path: "/shared/session.jsonl".to_string(),
        agent: "claude_code".to_string(),
        workspace: String::new(),
        workspace_original: None,
        created_at: None,
        line_number: None,
        match_type: Default::default(),
        source_id: "local".to_string(),
        origin_kind: "local".to_string(),
        origin_host: None,
    };

    assert!(!conversation_view_matches_hit(&view, &hit));
}

#[test]
fn load_conversation_for_hit_selects_exact_conversation_within_same_source_and_path() {
    use crate::storage::sqlite::FrankenStorage;

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("cass.db");
    let storage = FrankenStorage::open(&db_path).expect("open storage");
    let conn = storage.raw();
    let shared_path = "/shared/cursor.sqlite";

    conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'cursor', 'Cursor', 'local', 0, 0)")
        .expect("insert agent");
    conn.execute(
        "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'old-ext', 'Old Session', '/shared/cursor.sqlite', 'local', 100)",
    )
    .expect("insert old conversation");
    conn.execute(
        "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (2, 1, 'new-ext', 'New Session', '/shared/cursor.sqlite', 'local', 200)",
    )
    .expect("insert new conversation");
    conn.execute(
        "INSERT INTO messages (id, conversation_id, idx, role, created_at, content) VALUES (1, 1, 0, 'user', 101, 'old conversation body')",
    )
    .expect("insert old message");
    conn.execute(
        "INSERT INTO messages (id, conversation_id, idx, role, created_at, content) VALUES (2, 2, 0, 'user', 201, 'new conversation body')",
    )
    .expect("insert new message");

    let hit = SearchHit {
        title: "New Session".to_string(),
        snippet: "new conversation body".to_string(),
        content: "new conversation body".to_string(),
        content_hash: 0,
        conversation_id: None,
        score: 0.0,
        source_path: shared_path.to_string(),
        agent: "cursor".to_string(),
        workspace: String::new(),
        workspace_original: None,
        created_at: Some(201),
        line_number: Some(1),
        match_type: Default::default(),
        source_id: "local".to_string(),
        origin_kind: "local".to_string(),
        origin_host: None,
    };

    let loaded = load_conversation_for_hit(&storage, &hit)
        .expect("load exact conversation")
        .expect("matching conversation");

    assert_eq!(loaded.convo.external_id.as_deref(), Some("new-ext"));
    assert_eq!(loaded.convo.title.as_deref(), Some("New Session"));
    assert_eq!(loaded.messages[0].content, "new conversation body");
}

#[test]
fn load_conversation_for_hit_accepts_matching_timestamp_even_when_hit_content_is_stale() {
    use crate::storage::sqlite::FrankenStorage;

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("cass.db");
    let storage = FrankenStorage::open(&db_path).expect("open storage");
    let conn = storage.raw();
    let shared_path = "/shared/cursor.sqlite";

    conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'cursor', 'Cursor', 'local', 0, 0)")
        .expect("insert agent");
    conn.execute(
        "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'new-ext', 'New Session', '/shared/cursor.sqlite', 'local', 200)",
    )
    .expect("insert conversation");
    conn.execute(
        "INSERT INTO messages (id, conversation_id, idx, role, created_at, content) VALUES (1, 1, 0, 'user', 201, 'new conversation body')",
    )
    .expect("insert message");

    let hit = SearchHit {
        title: "New Session".to_string(),
        snippet: "rendered fragment".to_string(),
        content: "stale search fragment".to_string(),
        content_hash: 0,
        conversation_id: None,
        score: 0.0,
        source_path: shared_path.to_string(),
        agent: "cursor".to_string(),
        workspace: String::new(),
        workspace_original: None,
        created_at: Some(201),
        line_number: None,
        match_type: Default::default(),
        source_id: "local".to_string(),
        origin_kind: "local".to_string(),
        origin_host: None,
    };

    let loaded = load_conversation_for_hit(&storage, &hit)
        .expect("load exact conversation")
        .expect("matching conversation");

    assert_eq!(loaded.convo.external_id.as_deref(), Some("new-ext"));
    assert_eq!(loaded.messages[0].content, "new conversation body");
}

#[test]
fn load_conversation_for_hit_refuses_stale_id_without_substituting_matching_content() {
    let tmp = tempdir().expect("tempdir");
    let db_path = tmp.path().join("cass.db");
    let storage = FrankenStorage::open(&db_path).expect("open db");
    let conn = storage.raw();
    conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'claude_code', 'Claude Code', 'local', 0, 0)")
        .expect("insert agent");
    conn.execute(
        "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'exact-ext', 'Database Title', '/shared/cursor.sqlite', 'local', 200)",
    )
    .expect("insert conversation");
    conn.execute(
        "INSERT INTO messages (id, conversation_id, idx, role, created_at, content) VALUES (1, 1, 0, 'user', 201, 'db body')",
    )
    .expect("insert message");

    let hit = SearchHit {
        title: "Database Title".to_string(),
        snippet: "db body".to_string(),
        content: "db body".to_string(),
        content_hash: 0,
        conversation_id: Some(999),
        score: 1.0,
        source_path: "/shared/cursor.sqlite".to_string(),
        agent: "claude_code".to_string(),
        workspace: String::new(),
        workspace_original: None,
        created_at: Some(201),
        line_number: Some(1),
        match_type: MatchType::Exact,
        source_id: "local".to_string(),
        origin_kind: "local".to_string(),
        origin_host: None,
    };
    assert!(
        load_conversation_for_hit(&storage, &hit)
            .expect("load attempt succeeds")
            .is_none()
    );

    // The same uniquely identifying legacy hit remains usable when no ID
    // was supplied. An explicit stale ID is never silently erased.
    let mut legacy = hit;
    legacy.conversation_id = None;
    let loaded = load_conversation_for_hit(&storage, &legacy)
        .expect("load legacy hit")
        .expect("unique message remains discoverable");

    assert_eq!(loaded.convo.id, Some(1));
    assert_eq!(
        loaded.convo.source_path,
        std::path::Path::new("/shared/cursor.sqlite")
    );
}

#[test]
fn load_conversation_for_hit_uses_origin_host_when_db_source_id_is_blank_remote() {
    use crate::storage::sqlite::FrankenStorage;

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("cass.db");
    let storage = FrankenStorage::open(&db_path).expect("open storage");
    let conn = storage.raw();
    let shared_path = "/shared/remote.sqlite";

    conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'cursor', 'Cursor', 'local', 0, 0)")
        .expect("insert agent");
    conn.execute(
        "INSERT INTO sources (id, kind, host_label, created_at, updated_at) VALUES ('   ', 'ssh', 'user@laptop', 0, 0)",
    )
    .expect("insert blank-id remote source");
    conn.execute(
        "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, origin_host, started_at) VALUES (1, 1, 'remote-ext', 'Remote Session', '/shared/remote.sqlite', '   ', 'user@laptop', 200)",
    )
    .expect("insert conversation");
    conn.execute(
        "INSERT INTO messages (id, conversation_id, idx, role, created_at, content) VALUES (1, 1, 0, 'user', 201, 'db body')",
    )
    .expect("insert message");

    let hit = SearchHit {
        title: "Remote Session".to_string(),
        snippet: String::new(),
        content: String::new(),
        content_hash: 0,
        conversation_id: Some(1),
        score: 0.0,
        source_path: shared_path.to_string(),
        agent: "cursor".to_string(),
        workspace: String::new(),
        workspace_original: None,
        created_at: None,
        line_number: None,
        match_type: Default::default(),
        source_id: "   ".to_string(),
        origin_kind: "remote".to_string(),
        origin_host: Some("user@laptop".to_string()),
    };

    let loaded = load_conversation_for_hit(&storage, &hit)
        .expect("load exact conversation")
        .expect("matching conversation");

    assert_eq!(loaded.convo.id, Some(1));
    assert_eq!(loaded.convo.source_id, "user@laptop");
    assert_eq!(loaded.convo.origin_host.as_deref(), Some("user@laptop"));
}

#[test]
fn load_conversation_for_hit_rejects_conflicting_path_and_source_despite_matching_id() {
    use crate::storage::sqlite::FrankenStorage;

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("cass.db");
    let storage = FrankenStorage::open(&db_path).expect("open storage");
    let conn = storage.raw();

    conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'cursor', 'Cursor', 'local', 0, 0)")
        .expect("insert agent");
    conn.execute(
        "INSERT INTO sources (id, kind, host_label, created_at, updated_at) VALUES ('  local  ', 'local', 'local', 0, 0)",
    )
    .expect("insert local source");
    conn.execute(
        "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'exact-ext', 'Database Title', '/db/real/path.sqlite', '  local  ', 200)",
    )
    .expect("insert conversation");
    conn.execute(
        "INSERT INTO messages (id, conversation_id, idx, role, created_at, content) VALUES (1, 1, 0, 'user', 201, 'db body')",
    )
    .expect("insert message");

    let hit = SearchHit {
        title: "Stale Indexed Title".to_string(),
        snippet: String::new(),
        content: String::new(),
        content_hash: 0,
        conversation_id: Some(1),
        score: 0.0,
        source_path: "/stale/index/path.sqlite".to_string(),
        agent: "cursor".to_string(),
        workspace: String::new(),
        workspace_original: None,
        created_at: None,
        line_number: None,
        match_type: Default::default(),
        source_id: "remote-laptop".to_string(),
        origin_kind: "remote".to_string(),
        origin_host: Some("dev@laptop".to_string()),
    };

    assert!(load_conversation_for_hit(&storage, &hit).unwrap().is_none());
    let mut exact = hit;
    exact.source_path = "/db/real/path.sqlite".into();
    exact.source_id = "  LOCAL  ".into();
    exact.origin_kind = "local".into();
    exact.origin_host = None;
    let loaded = load_conversation_for_hit(&storage, &exact)
        .expect("load exact source/path/conversation")
        .expect("matching conversation");

    assert_eq!(loaded.convo.id, Some(1));
    assert_eq!(
        loaded.convo.source_path.to_string_lossy(),
        "/db/real/path.sqlite"
    );
    assert_eq!(loaded.convo.source_id, "local");
}

#[test]
fn load_conversation_for_hit_prefers_exact_conversation_id_over_stale_title() {
    use crate::storage::sqlite::FrankenStorage;

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("cass.db");
    let storage = FrankenStorage::open(&db_path).expect("open storage");
    let conn = storage.raw();
    let shared_path = "/shared/cursor.sqlite";

    conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'cursor', 'Cursor', 'local', 0, 0)")
        .expect("insert agent");
    conn.execute(
        "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'exact-ext', 'Database Title', '/shared/cursor.sqlite', 'local', 200)",
    )
    .expect("insert conversation");
    conn.execute(
        "INSERT INTO messages (id, conversation_id, idx, role, created_at, content) VALUES (1, 1, 0, 'user', 201, 'db body')",
    )
    .expect("insert message");

    let hit = SearchHit {
        title: "Stale Indexed Title".to_string(),
        snippet: String::new(),
        content: String::new(),
        content_hash: 0,
        conversation_id: Some(1),
        score: 0.0,
        source_path: shared_path.to_string(),
        agent: "cursor".to_string(),
        workspace: String::new(),
        workspace_original: None,
        created_at: None,
        line_number: None,
        match_type: Default::default(),
        source_id: "local".to_string(),
        origin_kind: "local".to_string(),
        origin_host: None,
    };

    let loaded = load_conversation_for_hit(&storage, &hit)
        .expect("load exact conversation")
        .expect("matching conversation");

    assert_eq!(loaded.convo.id, Some(1));
    assert_eq!(loaded.convo.title.as_deref(), Some("Database Title"));
}

#[test]
fn load_conversation_for_hit_ignores_stale_title_when_exact_content_identifies_match() {
    use crate::storage::sqlite::FrankenStorage;

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("cass.db");
    let storage = FrankenStorage::open(&db_path).expect("open storage");
    let conn = storage.raw();
    let shared_path = "/shared/cursor.sqlite";

    conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'cursor', 'Cursor', 'local', 0, 0)")
        .expect("insert agent");
    conn.execute(
        "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'old-ext', 'Old Session', '/shared/cursor.sqlite', 'local', 100)",
    )
    .expect("insert old conversation");
    conn.execute(
        "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (2, 1, 'new-ext', 'New Session', '/shared/cursor.sqlite', 'local', 200)",
    )
    .expect("insert new conversation");
    conn.execute(
        "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (1, 1, 0, 'user', 'old conversation body')",
    )
    .expect("insert old message");
    conn.execute(
        "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (2, 2, 0, 'user', 'new conversation body')",
    )
    .expect("insert new message");

    let hit = SearchHit {
        title: "Stale Indexed Title".to_string(),
        snippet: "new conversation body".to_string(),
        content: "new conversation body".to_string(),
        content_hash: 0,
        conversation_id: None,
        score: 0.0,
        source_path: shared_path.to_string(),
        agent: "cursor".to_string(),
        workspace: String::new(),
        workspace_original: None,
        created_at: None,
        line_number: Some(1),
        match_type: Default::default(),
        source_id: "local".to_string(),
        origin_kind: "local".to_string(),
        origin_host: None,
    };

    let loaded = load_conversation_for_hit(&storage, &hit)
        .expect("load exact conversation")
        .expect("matching conversation");

    assert_eq!(loaded.convo.external_id.as_deref(), Some("new-ext"));
    assert_eq!(loaded.convo.title.as_deref(), Some("New Session"));
    assert_eq!(loaded.messages[0].content, "new conversation body");
}

#[test]
fn load_conversation_for_hit_uses_title_only_identity_hint() {
    use crate::storage::sqlite::FrankenStorage;

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("cass.db");
    let storage = FrankenStorage::open(&db_path).expect("open storage");
    let conn = storage.raw();
    let shared_path = "/shared/cursor.sqlite";

    conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'cursor', 'Cursor', 'local', 0, 0)")
        .expect("insert agent");
    conn.execute(
        "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'old-ext', 'Old Session', '/shared/cursor.sqlite', 'local', 100)",
    )
    .expect("insert old conversation");
    conn.execute(
        "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (2, 1, 'new-ext', 'New Session', '/shared/cursor.sqlite', 'local', 200)",
    )
    .expect("insert new conversation");
    conn.execute(
        "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (1, 1, 0, 'user', 'old conversation body')",
    )
    .expect("insert old message");
    conn.execute(
        "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (2, 2, 0, 'user', 'new conversation body')",
    )
    .expect("insert new message");

    let hit = SearchHit {
        title: "Old Session".to_string(),
        snippet: String::new(),
        content: String::new(),
        content_hash: 0,
        conversation_id: None,
        score: 0.0,
        source_path: shared_path.to_string(),
        agent: "cursor".to_string(),
        workspace: String::new(),
        workspace_original: None,
        created_at: None,
        line_number: None,
        match_type: Default::default(),
        source_id: "local".to_string(),
        origin_kind: "local".to_string(),
        origin_host: None,
    };

    let loaded = load_conversation_for_hit(&storage, &hit)
        .expect("load attempt succeeds")
        .expect("matching conversation");

    assert_eq!(loaded.convo.external_id.as_deref(), Some("old-ext"));
    assert_eq!(loaded.convo.title.as_deref(), Some("Old Session"));
}

#[test]
fn load_conversation_for_hit_does_not_fall_back_to_wrong_conversation_when_identity_misses() {
    use crate::storage::sqlite::FrankenStorage;

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("cass.db");
    let storage = FrankenStorage::open(&db_path).expect("open storage");
    let conn = storage.raw();
    let shared_path = "/shared/cursor.sqlite";

    conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'cursor', 'Cursor', 'local', 0, 0)")
        .expect("insert agent");
    conn.execute(
        "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'old-ext', 'Old Session', '/shared/cursor.sqlite', 'local', 100)",
    )
    .expect("insert old conversation");
    conn.execute(
        "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (2, 1, 'new-ext', 'New Session', '/shared/cursor.sqlite', 'local', 200)",
    )
    .expect("insert new conversation");
    conn.execute(
        "INSERT INTO messages (id, conversation_id, idx, role, created_at, content) VALUES (1, 1, 0, 'user', 101, 'old conversation body')",
    )
    .expect("insert old message");
    conn.execute(
        "INSERT INTO messages (id, conversation_id, idx, role, created_at, content) VALUES (2, 2, 0, 'user', 201, 'new conversation body')",
    )
    .expect("insert new message");

    let hit = SearchHit {
        title: "Missing Session".to_string(),
        snippet: "missing conversation body".to_string(),
        content: "missing conversation body".to_string(),
        content_hash: 0,
        conversation_id: None,
        score: 0.0,
        source_path: shared_path.to_string(),
        agent: "cursor".to_string(),
        workspace: String::new(),
        workspace_original: None,
        created_at: Some(999),
        line_number: Some(1),
        match_type: Default::default(),
        source_id: "local".to_string(),
        origin_kind: "local".to_string(),
        origin_host: None,
    };

    let loaded = load_conversation_for_hit(&storage, &hit).expect("load attempt succeeds");
    assert!(
        loaded.is_none(),
        "identity-mismatched hits must not fall back to an arbitrary conversation"
    );
}
