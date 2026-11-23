use super::*;
use crate::franken_sync::compat::{ConnectionExt as FrankenConnectionExt, RowExt as FrankenRowExt};
use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use crate::sources::provenance::{LOCAL_SOURCE_ID, Source, SourceKind};
use std::path::{Path, PathBuf};
use tempfile::TempDir;

#[test]
fn ensure_agent_returns_existing_id() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: None,
        kind: AgentKind::Cli,
    };

    let id1 = storage.ensure_agent(&agent).unwrap();
    let id2 = storage.ensure_agent(&agent).unwrap();
    assert_eq!(id1, id2);
}

#[test]
fn ensure_agent_unchanged_preserves_updated_at() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: Some("1.0".into()),
        kind: AgentKind::Cli,
    };

    storage.ensure_agent(&agent).unwrap();
    let initial_updated_at: i64 = storage
        .conn
        .query_row_map(
            "SELECT updated_at FROM agents WHERE slug = ?1",
            fparams![agent.slug.as_str()],
            |row| row.get_typed(0),
        )
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));

    storage.ensure_agent(&agent).unwrap();
    let fetched_updated_at: i64 = storage
        .conn
        .query_row_map(
            "SELECT updated_at FROM agents WHERE slug = ?1",
            fparams![agent.slug.as_str()],
            |row| row.get_typed(0),
        )
        .unwrap();

    assert_eq!(fetched_updated_at, initial_updated_at);
}

#[test]
fn ensure_agent_changed_metadata_updates_cached_slug() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let mut agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: Some("1.0".into()),
        kind: AgentKind::Cli,
    };

    let id1 = storage.ensure_agent(&agent).unwrap();
    agent.name = "Codex CLI".into();
    agent.version = Some("1.1".into());
    let id2 = storage.ensure_agent(&agent).unwrap();

    let fetched: (String, Option<String>) = storage
        .conn
        .query_row_map(
            "SELECT name, version FROM agents WHERE slug = ?1",
            fparams![agent.slug.as_str()],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
        )
        .unwrap();

    assert_eq!(id1, id2);
    assert_eq!(fetched, ("Codex CLI".into(), Some("1.1".into())));
}

#[test]
fn list_agents_returns_inserted() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let agent = Agent {
        id: None,
        slug: "new_agent".into(),
        name: "New Agent".into(),
        version: None,
        kind: AgentKind::VsCode,
    };
    storage.ensure_agent(&agent).unwrap();

    let agents = storage.list_agents().unwrap();
    assert!(agents.iter().any(|a| a.slug == "new_agent"));
}

// =========================================================================
// Workspace storage tests (bead yln.4)
// =========================================================================

#[test]
fn ensure_workspace_creates_new() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let id = storage
        .ensure_workspace(Path::new("/home/user/project"), Some("My Project"))
        .unwrap();
    assert!(id > 0);
}

#[test]
fn ensure_workspace_returns_existing() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let path = Path::new("/home/user/myproject");
    let id1 = storage.ensure_workspace(path, None).unwrap();
    let id2 = storage.ensure_workspace(path, None).unwrap();
    assert_eq!(id1, id2);
}

#[test]
fn ensure_workspace_changed_display_name_updates_cached_path() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let path = Path::new("/home/user/myproject");
    let id1 = storage.ensure_workspace(path, Some("Before")).unwrap();
    let id2 = storage.ensure_workspace(path, Some("After")).unwrap();

    let display_name: Option<String> = storage
        .conn
        .query_row_map(
            "SELECT display_name FROM workspaces WHERE path = ?1",
            fparams![path.to_string_lossy().as_ref()],
            |row| row.get_typed(0),
        )
        .unwrap();

    assert_eq!(id1, id2);
    assert_eq!(display_name.as_deref(), Some("After"));
}

#[test]
fn list_workspaces_returns_inserted() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    storage
        .ensure_workspace(Path::new("/test/workspace"), Some("Test WS"))
        .unwrap();

    let workspaces = storage.list_workspaces().unwrap();
    assert!(
        workspaces
            .iter()
            .any(|w| w.path.to_str() == Some("/test/workspace"))
    );
}

// =========================================================================
// Source storage tests (bead yln.4)
// =========================================================================

#[test]
fn upsert_source_creates_new() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let source = Source {
        id: "test-laptop".into(),
        kind: SourceKind::Ssh,
        host_label: Some("test.local".into()),
        machine_id: Some("test-machine-id".into()),
        platform: None,
        config_json: None,
        created_at: Some(SqliteStorage::now_millis()),
        updated_at: None,
    };

    storage.upsert_source(&source).unwrap();
    let fetched = storage.get_source("test-laptop").unwrap();
    assert!(fetched.is_some());
    assert_eq!(fetched.unwrap().host_label, Some("test.local".into()));
}

#[test]
fn upsert_source_updates_existing() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let source1 = Source {
        id: "my-source".into(),
        kind: SourceKind::Ssh,
        host_label: Some("Original Label".into()),
        machine_id: None,
        platform: None,
        config_json: None,
        created_at: Some(SqliteStorage::now_millis()),
        updated_at: None,
    };
    storage.upsert_source(&source1).unwrap();

    let source2 = Source {
        id: "my-source".into(),
        kind: SourceKind::Ssh,
        host_label: Some("Updated Label".into()),
        machine_id: None,
        platform: Some("linux".into()),
        config_json: None,
        created_at: Some(SqliteStorage::now_millis()),
        updated_at: Some(SqliteStorage::now_millis()),
    };
    storage.upsert_source(&source2).unwrap();

    let fetched = storage.get_source("my-source").unwrap().unwrap();
    assert_eq!(fetched.host_label, Some("Updated Label".into()));
    assert!(fetched.platform.is_some());
}

#[test]
fn upsert_source_unchanged_preserves_updated_at() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let source = Source {
        id: "stable-source".into(),
        kind: SourceKind::Ssh,
        host_label: Some("builder.local".into()),
        machine_id: None,
        platform: Some("linux".into()),
        config_json: Some(serde_json::json!({"role": "bench"})),
        created_at: None,
        updated_at: None,
    };

    storage.upsert_source(&source).unwrap();
    let initial = storage.get_source("stable-source").unwrap().unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));

    storage.upsert_source(&source).unwrap();
    let fetched = storage.get_source("stable-source").unwrap().unwrap();

    assert_eq!(fetched.created_at, initial.created_at);
    assert_eq!(fetched.updated_at, initial.updated_at);
    assert_eq!(fetched.host_label, initial.host_label);
    assert_eq!(fetched.platform, initial.platform);
    assert_eq!(fetched.config_json, initial.config_json);
}

#[test]
fn ensure_source_for_conversation_recreates_remote_source_after_delete() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let conversation = Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: Some(PathBuf::from("/ws/cache-recreate")),
        external_id: Some("cache-recreate".into()),
        title: Some("Cache Recreate".into()),
        source_path: PathBuf::from("/log/cache-recreate.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_001),
        approx_tokens: Some(16),
        metadata_json: serde_json::json!({}),
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: Some("tester".into()),
            created_at: Some(1_700_000_000_000),
            content: "cache recreate".into(),
            extra_json: serde_json::json!({}),
            snippets: Vec::new(),
        }],
        source_id: "cache-remote-source".into(),
        origin_host: Some("builder-cache".into()),
    };

    storage
        .ensure_source_for_conversation(&conversation)
        .unwrap();
    assert!(storage.get_source("cache-remote-source").unwrap().is_some());

    let deleted = storage.delete_source("cache-remote-source", false).unwrap();
    assert!(deleted);
    assert!(storage.get_source("cache-remote-source").unwrap().is_none());

    storage
        .ensure_source_for_conversation(&conversation)
        .unwrap();
    let recreated = storage.get_source("cache-remote-source").unwrap();
    assert!(recreated.is_some());
    assert_eq!(
        recreated.unwrap().host_label.as_deref(),
        Some("builder-cache")
    );
}

#[test]
fn delete_source_removes_entry() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let source = Source {
        id: "to-delete".into(),
        kind: SourceKind::Local,
        host_label: None,
        machine_id: None,
        platform: None,
        config_json: None,
        created_at: Some(SqliteStorage::now_millis()),
        updated_at: None,
    };
    storage.upsert_source(&source).unwrap();

    let deleted = storage.delete_source("to-delete", false).unwrap();
    assert!(deleted);

    let fetched = storage.get_source("to-delete").unwrap();
    assert!(fetched.is_none());
}

#[test]
fn delete_source_cannot_delete_local() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let result = storage.delete_source(LOCAL_SOURCE_ID, false);
    assert!(result.is_err());
}

#[test]
fn list_sources_includes_local() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let sources = storage.list_sources().unwrap();
    assert!(sources.iter().any(|s| s.id == LOCAL_SOURCE_ID));
}

#[test]
fn storage_source_normalization_prefers_explicit_local_kind_over_named_id() {
    let (source_id, source_kind, host_label) = normalized_storage_source_parts(
        Some("backup-local"),
        Some("local"),
        Some("stale-host-label"),
    );

    assert_eq!(source_id, "backup-local");
    assert_eq!(source_kind, SourceKind::Local);
    assert_eq!(host_label, None);
}

#[test]
fn incomplete_conversation_metadata_does_not_overwrite_registered_local_kind() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();
    storage
        .upsert_source(&Source {
            id: "backup-local".into(),
            kind: SourceKind::Local,
            host_label: None,
            machine_id: Some("machine-a".into()),
            platform: Some("macos".into()),
            config_json: Some(serde_json::json!({"root": "/backup"})),
            created_at: None,
            updated_at: None,
        })
        .unwrap();

    let conversation = Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: None,
        external_id: Some("legacy-backup-local".into()),
        title: None,
        source_path: dir.path().join("backup.jsonl"),
        started_at: None,
        ended_at: None,
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: Vec::new(),
        source_id: "backup-local".into(),
        origin_host: None,
    };

    storage
        .ensure_source_for_conversation(&conversation)
        .unwrap();
    let preserved = storage
        .get_source("backup-local")
        .unwrap()
        .expect("registered named local source");
    assert_eq!(preserved.kind, SourceKind::Local);
    assert_eq!(preserved.machine_id.as_deref(), Some("machine-a"));
    assert_eq!(preserved.platform.as_deref(), Some("macos"));
    assert_eq!(
        preserved.config_json,
        Some(serde_json::json!({"root": "/backup"}))
    );
}

#[test]
fn insert_conversation_tree_blank_local_source_normalizes_to_local_id() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let agent_id = storage
        .ensure_agent(&Agent {
            id: None,
            slug: "codex".into(),
            name: "Codex".into(),
            version: None,
            kind: AgentKind::Cli,
        })
        .unwrap();

    let conversation = Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: None,
        external_id: Some("blank-local-source".into()),
        title: Some("Blank local source".into()),
        source_path: dir.path().join("blank-local.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_001),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: None,
            created_at: Some(1_700_000_000_000),
            content: "hello".into(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        source_id: "   ".into(),
        origin_host: None,
    };

    storage
        .insert_conversation_tree(agent_id, None, &conversation)
        .unwrap();

    assert!(storage.get_source("   ").unwrap().is_none());
    let source = storage
        .get_source(LOCAL_SOURCE_ID)
        .unwrap()
        .expect("local source row should exist");
    assert_eq!(source.kind, SourceKind::Local);
    assert_eq!(source.host_label, None);

    let conversations = storage.list_conversations(10, 0).unwrap();
    assert_eq!(conversations.len(), 1);
    assert_eq!(conversations[0].source_id, LOCAL_SOURCE_ID);
    assert_eq!(conversations[0].origin_host, None);
}

#[test]
fn repeated_local_inserts_do_not_touch_bootstrap_source_row() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let agent_id = storage
        .ensure_agent(&Agent {
            id: None,
            slug: "codex".into(),
            name: "Codex".into(),
            version: None,
            kind: AgentKind::Cli,
        })
        .unwrap();

    let bootstrap_updated_at: i64 = storage
        .conn
        .query_row_map(
            "SELECT updated_at FROM sources WHERE id = ?1",
            fparams![LOCAL_SOURCE_ID],
            |row| row.get_typed(0),
        )
        .unwrap();

    let make_conversation = |external_id: &str, suffix: &str| Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: None,
        external_id: Some(external_id.into()),
        title: Some(format!("Local source {suffix}")),
        source_path: dir.path().join(format!("local-{suffix}.jsonl")),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_001),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: None,
            created_at: Some(1_700_000_000_000),
            content: format!("hello-{suffix}"),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };

    std::thread::sleep(std::time::Duration::from_millis(5));
    storage
        .insert_conversation_tree(agent_id, None, &make_conversation("local-source-1", "one"))
        .unwrap();
    let after_first_insert: i64 = storage
        .conn
        .query_row_map(
            "SELECT updated_at FROM sources WHERE id = ?1",
            fparams![LOCAL_SOURCE_ID],
            |row| row.get_typed(0),
        )
        .unwrap();

    std::thread::sleep(std::time::Duration::from_millis(5));
    storage
        .insert_conversation_tree(agent_id, None, &make_conversation("local-source-2", "two"))
        .unwrap();
    let after_second_insert: i64 = storage
        .conn
        .query_row_map(
            "SELECT updated_at FROM sources WHERE id = ?1",
            fparams![LOCAL_SOURCE_ID],
            |row| row.get_typed(0),
        )
        .unwrap();

    assert_eq!(after_first_insert, bootstrap_updated_at);
    assert_eq!(after_second_insert, bootstrap_updated_at);
}

#[test]
fn insert_conversation_tree_blank_remote_source_normalizes_to_origin_host() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let agent_id = storage
        .ensure_agent(&Agent {
            id: None,
            slug: "codex".into(),
            name: "Codex".into(),
            version: None,
            kind: AgentKind::Cli,
        })
        .unwrap();

    let conversation = Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: None,
        external_id: Some("blank-remote-source".into()),
        title: Some("Blank remote source".into()),
        source_path: dir.path().join("blank-remote.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_001),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: None,
            created_at: Some(1_700_000_000_000),
            content: "hello".into(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        source_id: "   ".into(),
        origin_host: Some("user@work-laptop".into()),
    };

    storage
        .insert_conversation_tree(agent_id, None, &conversation)
        .unwrap();

    assert!(storage.get_source("   ").unwrap().is_none());
    let source = storage
        .get_source("user@work-laptop")
        .unwrap()
        .expect("normalized remote source row should exist");
    assert_eq!(source.kind, SourceKind::Ssh);
    assert_eq!(source.host_label.as_deref(), Some("user@work-laptop"));

    let conversations = storage.list_conversations(10, 0).unwrap();
    assert_eq!(conversations.len(), 1);
    assert_eq!(conversations[0].source_id, "user@work-laptop");
    assert_eq!(
        conversations[0].origin_host.as_deref(),
        Some("user@work-laptop")
    );
}

#[test]
fn insert_conversations_batched_normalizes_host_only_remote_source_id() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let agent_id = storage
        .ensure_agent(&Agent {
            id: None,
            slug: "codex".into(),
            name: "Codex".into(),
            version: None,
            kind: AgentKind::Cli,
        })
        .unwrap();

    let conversation = Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: None,
        external_id: Some("batched-blank-remote-source".into()),
        title: Some("Batched blank remote source".into()),
        source_path: dir.path().join("batched-blank-remote.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_001),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: None,
            created_at: Some(1_700_000_000_000),
            content: "hello".into(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        source_id: "   ".into(),
        origin_host: Some("user@batch-host".into()),
    };

    storage
        .insert_conversations_batched(&[(agent_id, None, &conversation)])
        .unwrap();

    assert!(storage.get_source("   ").unwrap().is_none());
    let source = storage
        .get_source("user@batch-host")
        .unwrap()
        .expect("normalized batched remote source row should exist");
    assert_eq!(source.kind, SourceKind::Ssh);
    assert_eq!(source.host_label.as_deref(), Some("user@batch-host"));

    let conversations = storage.list_conversations(10, 0).unwrap();
    assert_eq!(conversations.len(), 1);
    assert_eq!(conversations[0].source_id, "user@batch-host");
    assert_eq!(
        conversations[0].origin_host.as_deref(),
        Some("user@batch-host")
    );
}

#[test]
fn get_source_ids_excludes_local() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    // Add a non-local source
    let source = Source {
        id: "remote-1".into(),
        kind: SourceKind::Ssh,
        host_label: Some("server".into()),
        machine_id: None,
        platform: None,
        config_json: None,
        created_at: Some(SqliteStorage::now_millis()),
        updated_at: None,
    };
    storage.upsert_source(&source).unwrap();
    storage
        .upsert_source(&Source {
            id: "backup-local".into(),
            kind: SourceKind::Local,
            host_label: None,
            machine_id: None,
            platform: None,
            config_json: None,
            created_at: Some(SqliteStorage::now_millis()),
            updated_at: None,
        })
        .unwrap();

    let ids = storage.get_source_ids().unwrap();
    assert!(!ids.contains(&LOCAL_SOURCE_ID.to_string()));
    assert!(!ids.contains(&"backup-local".to_string()));
    assert!(ids.contains(&"remote-1".to_string()));
}

// =========================================================================
// Scan timestamp tests (bead yln.4)
// =========================================================================

#[test]
fn get_last_scan_ts_returns_none_initially() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let ts = storage.get_last_scan_ts().unwrap();
    assert!(ts.is_none());
}

#[test]
fn set_and_get_last_scan_ts() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let expected_ts = 1700000000000_i64;
    storage.set_last_scan_ts(expected_ts).unwrap();

    let actual_ts = storage.get_last_scan_ts().unwrap();
    assert_eq!(actual_ts, Some(expected_ts));
}

#[test]
fn connector_last_scan_ts_round_trip_normalizes_name() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path)?;

    assert_eq!(storage.get_connector_last_scan_ts(" Codex ")?, None);

    let expected_ts = 1_700_000_123_456_i64;
    storage.set_connector_last_scan_ts(" Codex ", expected_ts)?;

    assert_eq!(
        storage.get_connector_last_scan_ts("codex")?,
        Some(expected_ts)
    );
    assert_eq!(
        storage.get_connector_last_scan_ts("CODEX")?,
        Some(expected_ts)
    );
    assert_eq!(storage.get_connector_last_scan_ts("claude-code")?, None);
    Ok(())
}
