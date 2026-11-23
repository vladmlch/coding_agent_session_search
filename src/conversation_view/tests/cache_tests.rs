use super::super::loader::storage_cache_scope;
use super::super::*;
use crate::model::types::{Conversation, Message, MessageRole, Workspace};
use std::path::PathBuf;
use std::sync::Arc;

fn make_test_view(id: i64) -> ConversationView {
    ConversationView {
        convo: Conversation {
            id: Some(id),
            agent_slug: "claude".to_string(),
            workspace: Some(PathBuf::from("/test/workspace")),
            external_id: Some(format!("ext-{}", id)),
            title: Some(format!("Test Conversation {}", id)),
            source_path: PathBuf::from(format!("/test/path/{}.jsonl", id)),
            started_at: Some(1704067200 + id),
            ended_at: None,
            approx_tokens: Some(1000),
            metadata_json: serde_json::json!({"test": true}),
            messages: Vec::new(),
            source_id: "local".to_string(),
            origin_host: None,
        },
        messages: vec![Message {
            id: Some(1),
            idx: 0,
            role: MessageRole::User,
            author: None,
            created_at: Some(1704067200),
            content: "Test message".to_string(),
            extra_json: serde_json::json!({}),
            snippets: Vec::new(),
        }],
        workspace: Some(Workspace {
            id: Some(1),
            path: PathBuf::from("/test/workspace"),
            display_name: None,
        }),
    }
}

#[test]
fn test_cache_insert_and_get() {
    let cache = ConversationCache::new(10);
    let view = make_test_view(1);
    let source_path = "/test/path/1.jsonl";

    // Insert into cache
    let arc = cache.insert(None, source_path, view.clone());
    assert_eq!(arc.convo.id, Some(1));

    // Get from cache
    let cached = cache.get(None, source_path);
    assert!(cached.is_some());
    assert_eq!(cached.unwrap().convo.id, Some(1));

    // Check stats
    let (hits, misses, _) = cache.stats().get();
    assert_eq!(hits, 1);
    assert_eq!(misses, 0);
}

#[test]
fn test_cache_distinguishes_same_path_across_sources() {
    let cache = ConversationCache::new(10);
    let source_path = "/test/shared/session.jsonl";

    let mut local = make_test_view(1);
    local.convo.source_path = PathBuf::from(source_path);
    local.convo.source_id = "local".to_string();
    let mut remote = make_test_view(2);
    remote.convo.source_path = PathBuf::from(source_path);
    remote.convo.source_id = "work-laptop".to_string();

    cache.insert(Some("local"), source_path, local);
    cache.insert(Some("work-laptop"), source_path, remote);

    let local_cached = cache.get(Some("local"), source_path).expect("local cached");
    let remote_cached = cache
        .get(Some("work-laptop"), source_path)
        .expect("remote cached");

    assert_eq!(local_cached.convo.source_id, "local");
    assert_eq!(remote_cached.convo.source_id, "work-laptop");
    assert_ne!(local_cached.convo.id, remote_cached.convo.id);
}

#[test]
fn load_conversation_cache_is_scoped_by_database_path() {
    use crate::storage::sqlite::FrankenStorage;

    let shared_path = "/shared/cross-db-session.jsonl";
    let tmp_a = tempfile::TempDir::new().expect("tempdir a");
    let db_path_a = tmp_a.path().join("cass-a.db");
    let storage_a = FrankenStorage::open(&db_path_a).expect("open storage a");
    let conn_a = storage_a.raw();
    let scope_a =
        storage_cache_scope(&storage_a).unwrap_or_else(|| db_path_a.display().to_string());

    let tmp_b = tempfile::TempDir::new().expect("tempdir b");
    let db_path_b = tmp_b.path().join("cass-b.db");
    let storage_b = FrankenStorage::open(&db_path_b).expect("open storage b");
    let conn_b = storage_b.raw();
    let scope_b =
        storage_cache_scope(&storage_b).unwrap_or_else(|| db_path_b.display().to_string());

    CONVERSATION_CACHE.invalidate_scoped(&scope_a, None, shared_path);
    CONVERSATION_CACHE.invalidate_scoped(&scope_b, None, shared_path);

    for conn in [&conn_a, &conn_b] {
        conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'claude_code', 'Claude Code', 'local', 0, 0)")
            .expect("insert agent");
    }

    {
        use crate::franken_sync::compat::{ParamValue, param_slice_to_values};
        let p = [ParamValue::from(shared_path.to_string())];
        conn_a.execute_with_params(
            "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'db-a', 'DB A Session', ?1, 'local', 100)",
            &param_slice_to_values(&p),
        )
        .expect("insert db a conversation");
        conn_b.execute_with_params(
            "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'db-b', 'DB B Session', ?1, 'local', 100)",
            &param_slice_to_values(&p),
        )
        .expect("insert db b conversation");
    }
    conn_a.execute(
        "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (1, 1, 0, 'user', 'db a body')",
    )
    .expect("insert db a message");
    conn_b.execute(
        "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (1, 1, 0, 'user', 'db b body')",
    )
    .expect("insert db b message");

    let from_a = load_conversation(&storage_a, shared_path)
        .expect("load from db a")
        .expect("db a conversation present");
    assert_eq!(from_a.convo.title.as_deref(), Some("DB A Session"));
    assert_eq!(from_a.messages[0].content, "db a body");

    let from_b = load_conversation(&storage_b, shared_path)
        .expect("load from db b")
        .expect("db b conversation present");
    assert_eq!(from_b.convo.title.as_deref(), Some("DB B Session"));
    assert_eq!(from_b.messages[0].content, "db b body");

    CONVERSATION_CACHE.invalidate_scoped(&scope_a, None, shared_path);
    CONVERSATION_CACHE.invalidate_scoped(&scope_b, None, shared_path);
}

#[test]
fn load_conversation_for_source_selects_blank_remote_source_id_via_origin_host() {
    use crate::storage::sqlite::FrankenStorage;

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("cass.db");
    let storage = FrankenStorage::open(&db_path).expect("open storage");
    let conn = storage.raw();
    let shared_path = "/shared/session.jsonl";

    conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'claude_code', 'Claude Code', 'local', 0, 0)")
        .expect("insert agent");
    conn.execute(
        "INSERT INTO sources (id, kind, host_label, created_at, updated_at) VALUES ('   ', 'ssh', 'user@laptop', 0, 0)",
    )
    .expect("insert blank-id remote source");
    {
        use crate::franken_sync::compat::{ParamValue, param_slice_to_values};
        let p = [ParamValue::from(shared_path.to_string())];
        conn.execute_with_params(
            "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, origin_host, started_at) VALUES (1, 1, 'remote-ext', 'Remote Session', ?1, '   ', 'user@laptop', 200)",
            &param_slice_to_values(&p),
        )
        .expect("insert remote conversation");
    }
    conn.execute(
        "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (1, 1, 0, 'user', 'remote body')",
    )
    .expect("insert remote message");

    let loaded = load_conversation_for_source(&storage, "user@laptop", shared_path)
        .expect("load conversation")
        .expect("conversation present");

    assert_eq!(loaded.convo.source_id, "user@laptop");
    assert_eq!(loaded.convo.origin_host.as_deref(), Some("user@laptop"));
    assert_eq!(loaded.convo.title.as_deref(), Some("Remote Session"));
    assert_eq!(loaded.messages[0].content, "remote body");
}

#[test]
fn load_conversation_for_source_selects_exact_source_id() {
    use crate::storage::sqlite::FrankenStorage;

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("cass.db");
    let storage = FrankenStorage::open(&db_path).expect("open storage");
    let conn = storage.raw();
    let shared_path = "/shared/session.jsonl";

    conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'claude_code', 'Claude Code', 'local', 0, 0)")
        .expect("insert agent");
    conn.execute(
        "INSERT INTO sources (id, kind, host_label, created_at, updated_at) VALUES ('  local  ', 'local', 'local', 0, 0)",
    )
    .expect("insert local source");
    conn.execute(
        "INSERT INTO sources (id, kind, host_label, created_at, updated_at) VALUES ('work-laptop', 'ssh', 'work-laptop', 0, 0)",
    )
    .expect("insert source");
    {
        use crate::franken_sync::compat::{ParamValue, param_slice_to_values};
        let p = [ParamValue::from(shared_path.to_string())];
        conn.execute_with_params(
            "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'local-ext', 'Local Session', ?1, '  local  ', 200)",
            &param_slice_to_values(&p),
        )
        .expect("insert local conversation");
        conn.execute_with_params(
            "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (2, 1, 'remote-ext', 'Remote Session', ?1, 'work-laptop', 100)",
            &param_slice_to_values(&p),
        )
        .expect("insert remote conversation");
    }
    conn.execute(
        "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (1, 1, 0, 'user', 'local body')",
    )
    .expect("insert local message");
    conn.execute(
        "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (2, 2, 0, 'user', 'remote body')",
    )
    .expect("insert remote message");

    let local = load_conversation_for_source(&storage, "local", shared_path)
        .expect("load local")
        .expect("local conversation");
    let remote = load_conversation_for_source(&storage, "work-laptop", shared_path)
        .expect("load remote")
        .expect("remote conversation");

    assert_eq!(local.convo.source_id, "local");
    assert_eq!(local.convo.title.as_deref(), Some("Local Session"));
    assert_eq!(local.messages[0].content, "local body");

    assert_eq!(remote.convo.source_id, "work-laptop");
    assert_eq!(remote.convo.title.as_deref(), Some("Remote Session"));
    assert_eq!(remote.messages[0].content, "remote body");
}

#[test]
fn load_conversation_for_source_invalidates_cache_when_newer_conversation_arrives() {
    use crate::storage::sqlite::FrankenStorage;

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("cass.db");
    let storage = FrankenStorage::open(&db_path).expect("open storage");
    let conn = storage.raw();
    let shared_path = "/shared/source-specific-session.jsonl";

    CONVERSATION_CACHE.invalidate(Some("local"), shared_path);

    conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'claude_code', 'Claude Code', 'local', 0, 0)")
        .expect("insert agent");
    {
        use crate::franken_sync::compat::{ParamValue, param_slice_to_values};
        let p = [ParamValue::from(shared_path.to_string())];
        conn.execute_with_params(
            "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'old-ext', 'Old Session', ?1, 'local', 100)",
            &param_slice_to_values(&p),
        )
        .expect("insert old conversation");
    }
    conn.execute(
        "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (1, 1, 0, 'user', 'old body')",
    )
    .expect("insert old message");

    let first = load_conversation_for_source(&storage, "local", shared_path)
        .expect("load old conversation")
        .expect("old conversation present");
    assert_eq!(first.convo.title.as_deref(), Some("Old Session"));
    assert_eq!(first.messages[0].content, "old body");

    {
        use crate::franken_sync::compat::{ParamValue, param_slice_to_values};
        let p = [ParamValue::from(shared_path.to_string())];
        conn.execute_with_params(
            "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (2, 1, 'new-ext', 'New Session', ?1, 'local', 200)",
            &param_slice_to_values(&p),
        )
        .expect("insert new conversation");
    }
    conn.execute(
        "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (2, 2, 0, 'user', 'new body')",
    )
    .expect("insert new message");

    let second = load_conversation_for_source(&storage, "local", shared_path)
        .expect("load new conversation")
        .expect("new conversation present");

    assert_eq!(second.convo.title.as_deref(), Some("New Session"));
    assert_eq!(second.messages[0].content, "new body");

    CONVERSATION_CACHE.invalidate(Some("local"), shared_path);
}

#[test]
fn load_conversation_prefers_named_local_source_for_shared_path() {
    use crate::storage::sqlite::FrankenStorage;

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("cass.db");
    let storage = FrankenStorage::open(&db_path).expect("open storage");
    let conn = storage.raw();
    let shared_path = "/shared/session.jsonl";

    conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'claude_code', 'Claude Code', 'local', 0, 0)")
        .expect("insert agent");
    conn.execute(
        "INSERT INTO sources (id, kind, host_label, created_at, updated_at) VALUES ('backup-local', 'local', NULL, 0, 0)",
    )
    .expect("insert named local source");
    conn.execute(
        "INSERT INTO sources (id, kind, host_label, created_at, updated_at) VALUES ('work-laptop', 'ssh', 'work-laptop', 0, 0)",
    )
    .expect("insert source");
    {
        use crate::franken_sync::compat::{ParamValue, param_slice_to_values};
        let p = [ParamValue::from(shared_path.to_string())];
        conn.execute_with_params(
            "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'local-ext', 'Local Session', ?1, 'backup-local', 100)",
            &param_slice_to_values(&p),
        )
        .expect("insert local conversation");
        conn.execute_with_params(
            "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (2, 1, 'remote-ext', 'Remote Session', ?1, 'work-laptop', 200)",
            &param_slice_to_values(&p),
        )
        .expect("insert remote conversation");
    }
    conn.execute(
        "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (1, 1, 0, 'user', 'local body')",
    )
    .expect("insert local message");
    conn.execute(
        "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (2, 2, 0, 'user', 'remote body')",
    )
    .expect("insert remote message");

    let loaded = load_conversation(&storage, shared_path)
        .expect("load conversation")
        .expect("conversation present");

    assert_eq!(loaded.convo.source_id, "backup-local");
    assert_eq!(loaded.convo.title.as_deref(), Some("Local Session"));
    assert_eq!(loaded.messages[0].content, "local body");
}

#[test]
fn load_conversation_uses_cached_value_when_validation_query_fails() {
    use crate::storage::sqlite::FrankenStorage;

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("cass.db");
    let storage = FrankenStorage::open(&db_path).expect("open storage");
    let conn = storage.raw();
    let shared_path = "/shared/cached-when-db-breaks.jsonl";

    CONVERSATION_CACHE.invalidate(None, shared_path);
    CONVERSATION_CACHE.invalidate(Some("local"), shared_path);

    conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'claude_code', 'Claude Code', 'local', 0, 0)")
        .expect("insert agent");
    {
        use crate::franken_sync::compat::{ParamValue, param_slice_to_values};
        let p = [ParamValue::from(shared_path.to_string())];
        conn.execute_with_params(
            "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'local-ext', 'Cached Session', ?1, 'local', 100)",
            &param_slice_to_values(&p),
        )
        .expect("insert local conversation");
    }
    conn.execute(
        "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (1, 1, 0, 'user', 'cached body')",
    )
    .expect("insert local message");

    let cached = load_conversation(&storage, shared_path)
        .expect("load initial conversation")
        .expect("conversation present");
    assert_eq!(cached.convo.title.as_deref(), Some("Cached Session"));
    assert_eq!(cached.messages[0].content, "cached body");

    conn.execute("DROP TABLE conversations")
        .expect("drop conversations to force validation failure");

    let still_cached = load_conversation(&storage, shared_path)
        .expect("use cached conversation after validation failure")
        .expect("cached conversation still present");

    assert_eq!(still_cached.convo.title.as_deref(), Some("Cached Session"));
    assert_eq!(still_cached.messages[0].content, "cached body");

    CONVERSATION_CACHE.invalidate(None, shared_path);
    CONVERSATION_CACHE.invalidate(Some("local"), shared_path);
}

#[test]
fn load_conversation_for_source_uses_cached_value_when_validation_query_fails() {
    use crate::storage::sqlite::FrankenStorage;

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("cass.db");
    let storage = FrankenStorage::open(&db_path).expect("open storage");
    let conn = storage.raw();
    let shared_path = "/shared/source-cache-when-db-breaks.jsonl";

    CONVERSATION_CACHE.invalidate(Some("local"), shared_path);

    conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'claude_code', 'Claude Code', 'local', 0, 0)")
        .expect("insert agent");
    {
        use crate::franken_sync::compat::{ParamValue, param_slice_to_values};
        let p = [ParamValue::from(shared_path.to_string())];
        conn.execute_with_params(
            "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'local-ext', 'Cached Session', ?1, 'local', 100)",
            &param_slice_to_values(&p),
        )
        .expect("insert local conversation");
    }
    conn.execute(
        "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (1, 1, 0, 'user', 'cached body')",
    )
    .expect("insert local message");

    let cached = load_conversation_for_source(&storage, "local", shared_path)
        .expect("load initial conversation")
        .expect("conversation present");
    assert_eq!(cached.convo.title.as_deref(), Some("Cached Session"));
    assert_eq!(cached.messages[0].content, "cached body");

    conn.execute("DROP TABLE conversations")
        .expect("drop conversations to force validation failure");

    let still_cached = load_conversation_for_source(&storage, "  LOCAL  ", shared_path)
        .expect("use cached conversation after validation failure")
        .expect("cached conversation still present");

    assert_eq!(still_cached.convo.title.as_deref(), Some("Cached Session"));
    assert_eq!(still_cached.messages[0].content, "cached body");

    CONVERSATION_CACHE.invalidate(Some("local"), shared_path);
}

#[test]
fn load_conversation_invalidates_path_only_cache_when_local_source_appears() {
    use crate::storage::sqlite::FrankenStorage;

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("cass.db");
    let storage = FrankenStorage::open(&db_path).expect("open storage");
    let conn = storage.raw();
    let shared_path = "/shared/late-local-session.jsonl";

    CONVERSATION_CACHE.invalidate(None, shared_path);
    CONVERSATION_CACHE.invalidate(Some("local"), shared_path);
    CONVERSATION_CACHE.invalidate(Some("work-laptop"), shared_path);

    conn.execute("INSERT INTO agents (id, slug, name, kind, created_at, updated_at) VALUES (1, 'claude_code', 'Claude Code', 'local', 0, 0)")
        .expect("insert agent");
    conn.execute(
        "INSERT INTO sources (id, kind, host_label, created_at, updated_at) VALUES ('work-laptop', 'ssh', 'work-laptop', 0, 0)",
    )
    .expect("insert source");
    {
        use crate::franken_sync::compat::{ParamValue, param_slice_to_values};
        let p = [ParamValue::from(shared_path.to_string())];
        conn.execute_with_params(
            "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (1, 1, 'remote-ext', 'Remote Session', ?1, 'work-laptop', 200)",
            &param_slice_to_values(&p),
        )
        .expect("insert remote conversation");
    }
    conn.execute(
        "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (1, 1, 0, 'user', 'remote body')",
    )
    .expect("insert remote message");

    let first = load_conversation(&storage, shared_path)
        .expect("load remote conversation")
        .expect("remote conversation present");
    assert_eq!(first.convo.source_id, "work-laptop");
    assert_eq!(first.messages[0].content, "remote body");

    {
        use crate::franken_sync::compat::{ParamValue, param_slice_to_values};
        let p = [ParamValue::from(shared_path.to_string())];
        conn.execute_with_params(
            "INSERT INTO conversations (id, agent_id, external_id, title, source_path, source_id, started_at) VALUES (2, 1, 'local-ext', 'Local Session', ?1, 'local', 100)",
            &param_slice_to_values(&p),
        )
        .expect("insert local conversation");
    }
    conn.execute(
        "INSERT INTO messages (id, conversation_id, idx, role, content) VALUES (2, 2, 0, 'user', 'local body')",
    )
    .expect("insert local message");

    let second = load_conversation(&storage, shared_path)
        .expect("load local conversation")
        .expect("local conversation present");

    assert_eq!(second.convo.source_id, "local");
    assert_eq!(second.convo.title.as_deref(), Some("Local Session"));
    assert_eq!(second.messages[0].content, "local body");

    CONVERSATION_CACHE.invalidate(None, shared_path);
    CONVERSATION_CACHE.invalidate(Some("local"), shared_path);
    CONVERSATION_CACHE.invalidate(Some("work-laptop"), shared_path);
}

#[test]
fn test_cache_miss() {
    let cache = ConversationCache::new(10);

    // Get from empty cache
    let cached = cache.get(None, "/nonexistent/path.jsonl");
    assert!(cached.is_none());

    // Check stats
    let (hits, misses, _) = cache.stats().get();
    assert_eq!(hits, 0);
    assert_eq!(misses, 1);
}

#[test]
fn test_cache_invalidation() {
    let cache = ConversationCache::new(10);
    let view = make_test_view(1);
    let source_path = "/test/path/1.jsonl";

    // Insert and verify
    cache.insert(None, source_path, view);
    assert!(cache.get(None, source_path).is_some());

    // Invalidate
    cache.invalidate(None, source_path);
    assert!(cache.get(None, source_path).is_none());
}

#[test]
fn test_cache_invalidate_all() {
    let cache = ConversationCache::new(10);

    // Insert multiple entries
    for i in 0..5 {
        let view = make_test_view(i);
        let source_path = format!("/test/path/{}.jsonl", i);
        cache.insert(None, &source_path, view);
    }

    assert_eq!(cache.len(), 5);

    // Invalidate all
    cache.invalidate_all();
    assert_eq!(cache.len(), 0);
    assert!(cache.is_empty());
}

#[test]
fn test_cache_lru_eviction() {
    let cache = ConversationCache::new(2); // 2 per shard, 32 total

    // Insert more entries than a single shard can hold
    // All entries go to same shard by using paths that hash to same shard
    // (in practice, FxHasher distributes well, so we insert many entries)
    for i in 0..100 {
        let view = make_test_view(i);
        let source_path = format!("/test/path/{}.jsonl", i);
        cache.insert(None, &source_path, view);
    }

    // Some early entries should have been evicted
    let (_, _, evictions) = cache.stats().get();
    assert!(evictions > 0, "Expected some evictions with small capacity");
}

#[test]
fn test_cache_hit_rate() {
    let cache = ConversationCache::new(10);
    let view = make_test_view(1);
    let source_path = "/test/path/1.jsonl";

    // Initial hit rate is 0
    assert_eq!(cache.stats().hit_rate(), 0.0);

    // Insert and access twice (1 miss on insert lookup, then 2 hits)
    cache.insert(None, source_path, view);
    let _ = cache.get(None, source_path);
    let _ = cache.get(None, source_path);

    // Hit rate should be positive (2 hits / 2 total)
    let hit_rate = cache.stats().hit_rate();
    assert!(
        hit_rate > 0.5,
        "Expected >50% hit rate, got {:.1}%",
        hit_rate * 100.0
    );
}

#[test]
fn test_cache_shard_distribution() {
    let cache = ConversationCache::new(100);

    // Insert 1000 entries
    for i in 0..1000 {
        let view = make_test_view(i);
        let source_path = format!("/various/paths/{}/session.jsonl", i);
        cache.insert(None, &source_path, view);
    }

    // All entries should be cached
    assert_eq!(cache.len(), 1000);
}

#[test]
fn test_cache_concurrent_access() {
    use std::thread;

    let cache = Arc::new(ConversationCache::new(100));
    let mut handles = vec![];

    // Spawn writers
    for t in 0..4 {
        let cache = Arc::clone(&cache);
        handles.push(thread::spawn(move || {
            for i in 0..250 {
                let id = t * 250 + i;
                let view = make_test_view(id);
                let source_path = format!("/test/path/{}.jsonl", id);
                cache.insert(None, &source_path, view);
            }
        }));
    }

    // Spawn readers
    for _ in 0..4 {
        let cache = Arc::clone(&cache);
        handles.push(thread::spawn(move || {
            for i in 0..1000 {
                let source_path = format!("/test/path/{}.jsonl", i);
                let _ = cache.get(None, &source_path);
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify cache is consistent
    let (hits, misses, _) = cache.stats().get();
    assert!(hits + misses > 0, "Expected some cache operations");
}
