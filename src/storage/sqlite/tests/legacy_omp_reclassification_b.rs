use super::*;
use crate::franken_sync::compat::{ConnectionExt as FrankenConnectionExt, RowExt as FrankenRowExt};
use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use crate::sources::provenance::LOCAL_SOURCE_ID;
use serial_test::serial;
use std::path::PathBuf;
use tempfile::TempDir;

#[test]
#[serial]
fn legacy_omp_reclassification_preserves_distinct_remote_sources() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let _pi_sessions = set_env_var("PI_SESSIONS_DIR", "");
    let _omp_sessions = set_env_var("PI_CODING_AGENT_SESSION_DIR", "");
    let _shared_agent = set_env_var("PI_CODING_AGENT_DIR", "");
    let _omp_archive = set_env_var("CASS_OMP_DATA_ROOT", "");
    let _config_dir = set_env_var("PI_CONFIG_DIR", "");
    let _profile = set_env_var("OMP_PROFILE", "");
    let _legacy_profile = set_env_var("PI_PROFILE", "");
    let _xdg = set_env_var("XDG_DATA_HOME", "");
    let storage = SqliteStorage::open(&dir.path().join("test.db"))?;
    let pi_agent_id = storage.ensure_agent(&Agent {
        id: None,
        slug: "pi_agent".into(),
        name: "Pi Agent".into(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    let omp_agent_id = storage.ensure_agent(&Agent {
        id: None,
        slug: "omp".into(),
        name: "Oh My Pi".into(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    for source_id in ["remote-a", "remote-b"] {
        storage.conn.execute_compat(
            "INSERT INTO sources(id, kind, host_label, created_at, updated_at)
             VALUES(?1, 'ssh', ?1, 1000, 1000)",
            fparams![source_id],
        )?;
    }
    let shared_path = "/home/dev/.omp/profiles/work/agent/sessions/project/session.jsonl";
    storage.conn.execute_compat(
        "INSERT INTO conversations(
             agent_id, source_id, external_id, title, source_path, started_at
         ) VALUES(?1, 'remote-a', 'same-id', 'Remote A', ?2, 1000)",
        fparams![pi_agent_id, shared_path],
    )?;
    storage.conn.execute_compat(
        "INSERT INTO conversations(
             agent_id, source_id, external_id, title, source_path, started_at
         ) VALUES(?1, 'remote-b', 'same-id', 'Remote B', ?2, 1000)",
        fparams![omp_agent_id, shared_path],
    )?;

    assert_eq!(
        storage.reclassify_legacy_omp_conversations()?,
        LegacyOmpReclassificationResult {
            conversations_reclassified: 1,
            lexical_rebuild_required: true,
            analytics_rebuild_required: true,
        },
        "an identical remote path and external id on another source must not collapse provenance"
    );
    let rows = storage.conn.query_map_collect(
        "SELECT c.source_id, a.slug
         FROM conversations c
         JOIN agents a ON a.id = c.agent_id
         ORDER BY c.source_id",
        fparams![],
        |row| Ok((row.get_typed::<String>(0)?, row.get_typed::<String>(1)?)),
    )?;
    assert_eq!(
        rows,
        vec![
            ("remote-a".to_string(), "omp".to_string()),
            ("remote-b".to_string(), "omp".to_string()),
        ]
    );
    let remote_a_metadata: String = storage.conn.query_row_map(
        "SELECT metadata_json
         FROM conversations
         WHERE source_id = 'remote-a' AND agent_id = ?1",
        fparams![omp_agent_id],
        |row| row.get_typed(0),
    )?;
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&remote_a_metadata)?["profile"],
        "work",
        "path-derived profile provenance must survive a remote legacy migration"
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&remote_a_metadata)?["source"],
        "omp",
        "NULL legacy metadata must acquire the canonical provider identity"
    );

    let current_remote_a = Conversation {
        id: None,
        agent_slug: "omp".into(),
        workspace: None,
        external_id: Some("remote-a-current-id".into()),
        title: Some("Current Remote A".into()),
        source_path: PathBuf::from(shared_path),
        started_at: Some(1000),
        ended_at: Some(1001),
        approx_tokens: None,
        metadata_json: serde_json::json!({"source":"omp"}),
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: None,
            created_at: Some(1000),
            content: "remote A current scan".into(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        source_id: "remote-a".into(),
        origin_host: Some("remote-a".into()),
    };
    let outcome = storage
        .insert_conversations_batched(&[(omp_agent_id, None, &current_remote_a)])?
        .pop()
        .expect("one remote OMP insert outcome");
    assert!(
        !outcome.conversation_inserted,
        "a changed root-relative id must promote the reclassified remote row"
    );
    let remote_identities = storage.conn.query_map_collect(
        "SELECT source_id, external_id
         FROM conversations
         WHERE agent_id = ?1 AND source_path = ?2
         ORDER BY source_id",
        fparams![omp_agent_id, shared_path],
        |row| Ok((row.get_typed::<String>(0)?, row.get_typed::<String>(1)?)),
    )?;
    assert_eq!(
        remote_identities,
        vec![
            ("remote-a".to_string(), "remote-a-current-id".to_string()),
            ("remote-b".to_string(), "same-id".to_string()),
        ],
        "path recovery must never cross the source provenance boundary"
    );
    Ok(())
}

#[test]
#[serial]
fn legacy_omp_reclassification_normalizes_host_only_remote_provenance() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let _pi_sessions = set_env_var("PI_SESSIONS_DIR", "");
    let _omp_sessions = set_env_var("PI_CODING_AGENT_SESSION_DIR", "");
    let _shared_agent = set_env_var("PI_CODING_AGENT_DIR", "");
    let _omp_archive = set_env_var("CASS_OMP_DATA_ROOT", "");
    let _config_dir = set_env_var("PI_CONFIG_DIR", "");
    let _profile = set_env_var("OMP_PROFILE", "");
    let _legacy_profile = set_env_var("PI_PROFILE", "");
    let _xdg = set_env_var("XDG_DATA_HOME", "");
    let storage = SqliteStorage::open(&dir.path().join("test.db"))?;
    let pi_agent_id = storage.ensure_agent(&Agent {
        id: None,
        slug: "pi_agent".into(),
        name: "Pi Agent".into(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    storage.conn.execute_compat(
        "INSERT INTO sources(id, kind, host_label, created_at, updated_at)
         VALUES('   ', 'ssh', ' build-host ', 1000, 1000)",
        fparams![],
    )?;
    let source_path = "/home/dev/.omp/agent/sessions/project/remote.jsonl";
    storage.conn.execute_compat(
        "INSERT INTO conversations(
             agent_id, source_id, external_id, title, source_path, started_at,
             origin_host, metadata_json
         ) VALUES(
             ?1, '   ', 'remote-session', 'Remote legacy OMP', ?2, 1000,
             ' build-host ', '{\"source\":\"pi_agent\"}'
         )",
        fparams![pi_agent_id, source_path],
    )?;
    let conversation_id: i64 = storage.conn.query_row_map(
        "SELECT id FROM conversations WHERE external_id = 'remote-session'",
        fparams![],
        |row| row.get_typed(0),
    )?;
    storage.conn.execute_compat(
        "INSERT INTO messages(
             conversation_id, idx, role, created_at, content
         ) VALUES(?1, 0, 'user', 1000, 'legacy remote message')",
        fparams![conversation_id],
    )?;
    let message_id: i64 = storage.conn.query_row_map(
        "SELECT id FROM messages WHERE conversation_id = ?1",
        fparams![conversation_id],
        |row| row.get_typed(0),
    )?;
    storage.conn.execute_compat(
        "INSERT INTO token_usage(
             message_id, conversation_id, agent_id, source_id, timestamp_ms,
             day_id, role, content_chars
         ) VALUES(?1, ?2, ?3, '   ', 1000, 0, 'user', 21)",
        fparams![message_id, conversation_id, pi_agent_id],
    )?;

    assert_eq!(
        storage.reclassify_legacy_omp_conversations()?,
        LegacyOmpReclassificationResult {
            conversations_reclassified: 1,
            lexical_rebuild_required: true,
            analytics_rebuild_required: true,
        }
    );
    let (source_id, origin_host, agent_slug): (String, Option<String>, String) =
        storage.conn.query_row_map(
            "SELECT c.source_id, c.origin_host, a.slug
             FROM conversations c
             JOIN agents a ON a.id = c.agent_id
             WHERE c.id = ?1",
            fparams![conversation_id],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?, row.get_typed(2)?)),
        )?;
    assert_eq!(source_id, "build-host");
    assert_eq!(origin_host.as_deref(), Some("build-host"));
    assert_eq!(agent_slug, "omp");
    let (token_source_id, token_agent_id): (String, i64) = storage.conn.query_row_map(
        "SELECT source_id, agent_id FROM token_usage WHERE conversation_id = ?1",
        fparams![conversation_id],
        |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
    )?;
    let omp_agent_id: i64 = storage.conn.query_row_map(
        "SELECT id FROM agents WHERE slug = 'omp'",
        fparams![],
        |row| row.get_typed(0),
    )?;
    assert_eq!(token_source_id, "build-host");
    assert_eq!(token_agent_id, omp_agent_id);

    let incoming = Conversation {
        id: None,
        agent_slug: "omp".into(),
        workspace: None,
        external_id: Some("remote-session".into()),
        title: Some("Remote first-class OMP".into()),
        source_path: PathBuf::from(source_path),
        started_at: Some(1000),
        ended_at: Some(2000),
        approx_tokens: None,
        metadata_json: serde_json::json!({"source":"omp"}),
        messages: vec![Message {
            id: None,
            idx: 1,
            role: MessageRole::Agent,
            author: None,
            created_at: Some(2000),
            content: "first-class append".into(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        source_id: "build-host".into(),
        origin_host: Some("build-host".into()),
    };
    storage.insert_conversations_batched(&[(omp_agent_id, None, &incoming)])?;
    let canonical_count: i64 = storage.conn.query_row_map(
        "SELECT COUNT(*)
         FROM conversations
         WHERE source_id = 'build-host'
           AND agent_id = ?1
           AND external_id = 'remote-session'",
        fparams![omp_agent_id],
        |row| row.get_typed(0),
    )?;
    assert_eq!(
        canonical_count, 1,
        "the first current OMP scan must reuse the normalized legacy row"
    );
    Ok(())
}

#[test]
#[serial]
fn legacy_omp_plain_complete_marker_is_stale() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let _pi_sessions = set_env_var("PI_SESSIONS_DIR", "");
    let _omp_sessions = set_env_var("PI_CODING_AGENT_SESSION_DIR", "");
    let _shared_agent = set_env_var("PI_CODING_AGENT_DIR", "");
    let _omp_archive = set_env_var("CASS_OMP_DATA_ROOT", "");
    let _config_dir = set_env_var("PI_CONFIG_DIR", "");
    let _profile = set_env_var("OMP_PROFILE", "");
    let _legacy_profile = set_env_var("PI_PROFILE", "");
    let _xdg = set_env_var("XDG_DATA_HOME", "");
    let storage = SqliteStorage::open(&dir.path().join("test.db"))?;
    storage.conn.execute_compat(
        "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, 'complete')",
        fparams![LEGACY_OMP_RECLASSIFICATION_META_KEY],
    )?;

    assert_eq!(
        storage.reclassify_legacy_omp_conversations()?,
        LegacyOmpReclassificationResult::default()
    );
    let rebound: String = storage.conn.query_row_map(
        "SELECT value FROM meta WHERE key = ?1",
        fparams![LEGACY_OMP_RECLASSIFICATION_META_KEY],
        |row| row.get_typed(0),
    )?;
    assert!(
        rebound.starts_with("complete:") && rebound.len() > "complete:".len(),
        "a context-free complete marker must not survive the versioned ownership check: {rebound:?}"
    );
    Ok(())
}

#[test]
#[serial]
fn legacy_remote_omp_reclassification_rejects_non_mirror_lookalikes() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let _pi_sessions = set_env_var("PI_SESSIONS_DIR", "");
    let _omp_sessions = set_env_var("PI_CODING_AGENT_SESSION_DIR", "");
    let _shared_agent = set_env_var("PI_CODING_AGENT_DIR", "");
    let _omp_archive = set_env_var("CASS_OMP_DATA_ROOT", "");
    let _config_dir = set_env_var("PI_CONFIG_DIR", "");
    let _profile = set_env_var("OMP_PROFILE", "");
    let _legacy_profile = set_env_var("PI_PROFILE", "");
    let _xdg = set_env_var("XDG_DATA_HOME", "");
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path)?;
    let pi_agent_id = storage.ensure_agent(&Agent {
        id: None,
        slug: "pi_agent".into(),
        name: "Pi Agent".into(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    storage.conn.execute_compat(
        "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, 'complete')",
        fparams![PREVIOUS_LEGACY_OMP_RECLASSIFICATION_META_KEY],
    )?;

    let safe_name = crate::sources::provenance::path_to_safe_dirname("~/.omp/agent/sessions");
    let remote_external_id = "-projects-cass/remote-legacy-omp";
    let mut remote = Conversation {
        id: None,
        agent_slug: "pi_agent".into(),
        workspace: None,
        external_id: Some(remote_external_id.into()),
        title: Some("Remote legacy OMP".into()),
        source_path: dir
            .path()
            .join("cass/remotes/build-host/mirror")
            .join(&safe_name)
            .join("sessions/-projects-cass/remote-legacy-omp.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_001_000),
        approx_tokens: None,
        metadata_json: serde_json::json!({"source":"pi_agent"}),
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::Agent,
            author: Some("openrouter/stealth/ox-alpha".into()),
            created_at: Some(1_700_000_001_000),
            content: "remote legacy OMP answer".into(),
            extra_json: serde_json::json!({"message":{"model":"openrouter/stealth/ox-alpha"}}),
            snippets: Vec::new(),
        }],
        source_id: "build-host".into(),
        origin_host: Some("build-host.example".into()),
    };
    let mut lookalike = remote.clone();
    lookalike.external_id = Some("-projects-cass/incidental-lookalike".into());
    lookalike.title = Some("Incidental sanitized lookalike".into());
    lookalike.source_path = dir
        .path()
        .join("ordinary-cache")
        .join(&safe_name)
        .join("sessions/-projects-cass/incidental-lookalike.jsonl");
    let windows_external_id = "-projects-cass/windows-legacy-omp";
    let mut windows_legacy = remote.clone();
    windows_legacy.external_id = Some(windows_external_id.into());
    windows_legacy.title = Some("Windows legacy OMP".into());
    windows_legacy.source_path =
        PathBuf::from(r"C:\Users\dev\.omp\agent\sessions\-projects-cass\windows-legacy-omp.jsonl");
    windows_legacy.source_id = "windows-host".into();
    windows_legacy.origin_host = Some("windows-host.example".into());
    let windows_lookalike_external_id = "-projects-cass/windows-lookalike";
    let mut windows_lookalike = windows_legacy.clone();
    windows_lookalike.external_id = Some(windows_lookalike_external_id.into());
    windows_lookalike.title = Some("Windows sanitized lookalike".into());
    windows_lookalike.source_path = PathBuf::from(format!(
        r"C:\ordinary-cache\{safe_name}\sessions\-projects-cass\windows-lookalike.jsonl"
    ));
    storage.insert_conversations_batched(&[
        (pi_agent_id, None, &remote),
        (pi_agent_id, None, &lookalike),
        (pi_agent_id, None, &windows_legacy),
        (pi_agent_id, None, &windows_lookalike),
    ])?;

    let remote_conversation_id: i64 = storage.conn.query_row_map(
        "SELECT id FROM conversations WHERE external_id = ?1",
        fparams![remote_external_id],
        |row| row.get_typed(0),
    )?;
    let old_lookup =
        conversation_external_lookup_key(&remote.source_id, pi_agent_id, remote_external_id);
    // Identity resolution reads conversation_external_tail_lookup now —
    // franken_find_external_conversation_lookup delegates to it and the
    // legacy conversation_external_lookup table is retained only for
    // migration/purge SQL, so inserts populate the tail table alone.
    let old_lookup_exists: i64 = storage.conn.query_row_map(
        "SELECT EXISTS(
             SELECT 1 FROM conversation_external_tail_lookup WHERE lookup_key = ?1
         )",
        fparams![old_lookup.as_str()],
        |row| row.get_typed(0),
    )?;
    assert_eq!(old_lookup_exists, 1);

    assert_eq!(
        storage.reclassify_legacy_omp_conversations()?,
        LegacyOmpReclassificationResult {
            conversations_reclassified: 2,
            lexical_rebuild_required: true,
            analytics_rebuild_required: true,
        },
        "the v2 classifier must run even when the narrower v1 migration was complete"
    );
    // bet45: since the analytics split (d29f8825), reclassification
    // defers message_metrics to the analytics rebuild it flags via
    // analytics_rebuild_required — run it before asserting metrics rows.
    storage.rebuild_analytics()?;

    let omp_agent_id: i64 = storage.conn.query_row_map(
        "SELECT id FROM agents WHERE slug = 'omp'",
        fparams![],
        |row| row.get_typed(0),
    )?;
    let remote_slug: String = storage.conn.query_row_map(
        "SELECT a.slug
         FROM conversations c
         JOIN agents a ON a.id = c.agent_id
         WHERE c.id = ?1",
        fparams![remote_conversation_id],
        |row| row.get_typed(0),
    )?;
    let lookalike_slug: String = storage.conn.query_row_map(
        "SELECT a.slug
         FROM conversations c
         JOIN agents a ON a.id = c.agent_id
         WHERE c.external_id = ?1",
        fparams!["-projects-cass/incidental-lookalike"],
        |row| row.get_typed(0),
    )?;
    let windows_slug: String = storage.conn.query_row_map(
        "SELECT a.slug
         FROM conversations c
         JOIN agents a ON a.id = c.agent_id
         WHERE c.external_id = ?1",
        fparams![windows_external_id],
        |row| row.get_typed(0),
    )?;
    let windows_lookalike_slug: String = storage.conn.query_row_map(
        "SELECT a.slug
         FROM conversations c
         JOIN agents a ON a.id = c.agent_id
         WHERE c.external_id = ?1",
        fparams![windows_lookalike_external_id],
        |row| row.get_typed(0),
    )?;
    assert_eq!(remote_slug, "omp");
    assert_eq!(lookalike_slug, "pi_agent");
    assert_eq!(windows_slug, "omp");
    assert_eq!(windows_lookalike_slug, "pi_agent");

    let metadata_json: String = storage.conn.query_row_map(
        "SELECT metadata_json FROM conversations WHERE id = ?1",
        fparams![remote_conversation_id],
        |row| row.get_typed(0),
    )?;
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&metadata_json)?["source"],
        "omp"
    );
    let token_agent_id: i64 = storage.conn.query_row_map(
        "SELECT agent_id FROM token_usage WHERE conversation_id = ?1 LIMIT 1",
        fparams![remote_conversation_id],
        |row| row.get_typed(0),
    )?;
    assert_eq!(token_agent_id, omp_agent_id);
    let metrics_slug: String = storage.conn.query_row_map(
        "SELECT mm.agent_slug
             FROM message_metrics mm
             JOIN messages m ON m.id = mm.message_id
         WHERE m.conversation_id = ?1
         LIMIT 1",
        fparams![remote_conversation_id],
        |row| row.get_typed(0),
    )?;
    assert_eq!(metrics_slug, "omp");

    let new_lookup =
        conversation_external_lookup_key(&remote.source_id, omp_agent_id, remote_external_id);
    let stale_lookup_exists: i64 = storage.conn.query_row_map(
        "SELECT EXISTS(
             SELECT 1 FROM conversation_external_lookup WHERE lookup_key = ?1
         )",
        fparams![old_lookup.as_str()],
        |row| row.get_typed(0),
    )?;
    let canonical_lookup_exists: i64 = storage.conn.query_row_map(
        "SELECT EXISTS(
             SELECT 1 FROM conversation_external_lookup WHERE lookup_key = ?1
         )",
        fparams![new_lookup.as_str()],
        |row| row.get_typed(0),
    )?;
    let canonical_tail_lookup_exists: i64 = storage.conn.query_row_map(
        "SELECT EXISTS(
             SELECT 1 FROM conversation_external_tail_lookup WHERE lookup_key = ?1
         )",
        fparams![new_lookup.as_str()],
        |row| row.get_typed(0),
    )?;
    assert_eq!(stale_lookup_exists, 0);
    assert_eq!(canonical_lookup_exists, 1);
    assert_eq!(canonical_tail_lookup_exists, 1);

    remote.agent_slug = "omp".into();
    remote.metadata_json = serde_json::json!({"source":"omp"});
    storage.insert_conversations_batched(&[(omp_agent_id, None, &remote)])?;
    let canonical_count: i64 = storage.conn.query_row_map(
        "SELECT COUNT(*) FROM conversations WHERE external_id = ?1",
        fparams![remote_external_id],
        |row| row.get_typed(0),
    )?;
    assert_eq!(
        canonical_count, 1,
        "the first first-class OMP ingest must merge into the reclassified row"
    );
    Ok(())
}

#[test]
#[serial]
fn legacy_omp_reclassification_honors_current_custom_xdg_root() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let xdg_data_home = dir.path().join("custom-xdg");
    std::fs::create_dir_all(xdg_data_home.join("omp/sessions"))?;
    let _pi_sessions = set_env_var("PI_SESSIONS_DIR", "");
    let _omp_sessions = set_env_var("PI_CODING_AGENT_SESSION_DIR", "");
    let _shared_agent = set_env_var("PI_CODING_AGENT_DIR", "");
    let _omp_archive = set_env_var("CASS_OMP_DATA_ROOT", "");
    let _config_dir = set_env_var("PI_CONFIG_DIR", "");
    let _profile = set_env_var("OMP_PROFILE", "");
    let _legacy_profile = set_env_var("PI_PROFILE", "");
    let _xdg_without_evidence = set_env_var("XDG_DATA_HOME", "");

    let storage = SqliteStorage::open(&dir.path().join("test.db"))?;
    let pi_agent_id = storage.ensure_agent(&Agent {
        id: None,
        slug: "pi_agent".into(),
        name: "Pi Agent".into(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    let conversation = |external_id: &str, source_path: PathBuf| Conversation {
        id: None,
        agent_slug: "pi_agent".into(),
        workspace: None,
        external_id: Some(external_id.into()),
        title: Some(external_id.into()),
        source_path,
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_001_000),
        approx_tokens: None,
        metadata_json: serde_json::json!({"source":"pi_agent"}),
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: None,
            created_at: Some(1_700_000_000_000),
            content: external_id.into(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };
    let custom_xdg_id = "custom-xdg-legacy-omp";
    let unrelated_id = "unrelated-omp-directory";
    let custom_xdg = conversation(
        custom_xdg_id,
        xdg_data_home.join("omp/sessions/project/custom-xdg.jsonl"),
    );
    let unrelated = conversation(
        unrelated_id,
        dir.path().join("srv/omp/sessions/project/unrelated.jsonl"),
    );
    storage.insert_conversations_batched(&[
        (pi_agent_id, None, &custom_xdg),
        (pi_agent_id, None, &unrelated),
    ])?;

    let slug_for = |external_id: &str| -> anyhow::Result<String> {
        let slug = storage.conn.query_row_map(
            "SELECT a.slug
                 FROM conversations c
                 JOIN agents a ON a.id = c.agent_id
                 WHERE c.external_id = ?1",
            fparams![external_id],
            |row| row.get_typed(0),
        )?;
        Ok(slug)
    };

    assert_eq!(
        storage.reclassify_legacy_omp_conversations()?,
        LegacyOmpReclassificationResult::default(),
        "a historical custom-XDG-looking path stays Pi-owned without current provider-qualified evidence"
    );
    assert_eq!(slug_for(custom_xdg_id)?, "pi_agent");
    let unconfigured_complete_state: String = storage.conn.query_row_map(
        "SELECT value FROM meta WHERE key = ?1",
        fparams![LEGACY_OMP_RECLASSIFICATION_META_KEY],
        |row| row.get_typed(0),
    )?;

    let pending_state = {
        let _current_xdg = set_env_var("XDG_DATA_HOME", xdg_data_home.to_string_lossy());
        assert_eq!(
            storage.reclassify_legacy_omp_conversations()?,
            LegacyOmpReclassificationResult {
                conversations_reclassified: 1,
                lexical_rebuild_required: true,
                analytics_rebuild_required: true,
            },
            "a changed ownership context must invalidate the completed fast path"
        );
        storage.conn.query_row_map(
            "SELECT value FROM meta WHERE key = ?1",
            fparams![LEGACY_OMP_RECLASSIFICATION_META_KEY],
            |row| row.get_typed::<String>(0),
        )?
    };
    assert_eq!(slug_for(custom_xdg_id)?, "omp");
    assert_eq!(slug_for(unrelated_id)?, "pi_agent");

    // The current-XDG guard has restored the pre-scan empty value.
    // Completion must promote the stored scan context, not hash this
    // changed environment after the lexical publish.
    let _ = storage.rebuild_legacy_omp_analytics_with_progress(None, None, None)?;
    storage.mark_legacy_omp_lexical_publish_complete()?;
    let completed_state: String = storage.conn.query_row_map(
        "SELECT value FROM meta WHERE key = ?1",
        fparams![LEGACY_OMP_RECLASSIFICATION_META_KEY],
        |row| row.get_typed(0),
    )?;
    assert_eq!(
        completed_state,
        pending_state.replacen("analytics_pending:", "complete:", 1)
    );
    assert_ne!(completed_state, unconfigured_complete_state);
    assert_eq!(
        storage.reclassify_legacy_omp_conversations()?,
        LegacyOmpReclassificationResult::default()
    );
    let rebound_state: String = storage.conn.query_row_map(
        "SELECT value FROM meta WHERE key = ?1",
        fparams![LEGACY_OMP_RECLASSIFICATION_META_KEY],
        |row| row.get_typed(0),
    )?;
    assert_eq!(
        rebound_state, unconfigured_complete_state,
        "a context changed after publish must be rechecked rather than blessed by completion"
    );
    Ok(())
}

#[test]
#[serial]
fn legacy_omp_v2_preserves_a_pending_v1_derived_asset_rebuild() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let _pi_sessions = set_env_var("PI_SESSIONS_DIR", "");
    let _omp_sessions = set_env_var("PI_CODING_AGENT_SESSION_DIR", "");
    let _shared_agent = set_env_var("PI_CODING_AGENT_DIR", "");
    let _omp_archive = set_env_var("CASS_OMP_DATA_ROOT", "");
    let _config_dir = set_env_var("PI_CONFIG_DIR", "");
    let _profile = set_env_var("OMP_PROFILE", "");
    let _legacy_profile = set_env_var("PI_PROFILE", "");
    let _xdg = set_env_var("XDG_DATA_HOME", "");
    let storage = SqliteStorage::open(&dir.path().join("test.db"))?;
    storage.conn.execute_compat(
        "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, 'analytics_pending')",
        fparams![PREVIOUS_LEGACY_OMP_RECLASSIFICATION_META_KEY],
    )?;

    assert_eq!(
        storage.reclassify_legacy_omp_conversations()?,
        LegacyOmpReclassificationResult {
            conversations_reclassified: 0,
            lexical_rebuild_required: true,
            analytics_rebuild_required: true,
        },
        "versioning the path classifier must not forget a crash between the v1 identity swap and lexical publish"
    );
    let rebound_pending: String = storage.conn.query_row_map(
        "SELECT value FROM meta WHERE key = ?1",
        fparams![LEGACY_OMP_RECLASSIFICATION_META_KEY],
        |row| row.get_typed(0),
    )?;
    assert!(
        rebound_pending.starts_with("analytics_pending:")
            && rebound_pending.len() > "analytics_pending:".len(),
        "the context-free v1 pending state must be rebound before rebuilding: {rebound_pending:?}"
    );
    let rebound_context = rebound_pending
        .strip_prefix("analytics_pending:")
        .expect("pending marker has a context");
    for phase_key in [
        LEGACY_OMP_LEXICAL_PUBLISHED_META_KEY,
        LEGACY_OMP_ANALYTICS_REBUILT_META_KEY,
    ] {
        storage.conn.execute_compat(
            "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
            fparams![phase_key, rebound_context],
        )?;
    }
    storage.conn.execute_compat(
        "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
        fparams![
            LEGACY_OMP_ANALYTICS_CURSOR_CONTEXT_META_KEY,
            rebound_context
        ],
    )?;
    storage.conn.execute_compat(
        "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, 'start')",
        fparams![LEGACY_OMP_ANALYTICS_CURSOR_LAST_ID_META_KEY],
    )?;
    storage.conn.execute_compat(
        "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, '0')",
        fparams![LEGACY_OMP_ANALYTICS_CURSOR_PROCESSED_META_KEY],
    )?;
    assert_eq!(
        storage.reclassify_legacy_omp_conversations()?,
        LegacyOmpReclassificationResult::default(),
        "both phase markers must self-heal a missing aggregate completion marker"
    );
    let completed: String = storage.conn.query_row_map(
        "SELECT value FROM meta WHERE key = ?1",
        fparams![LEGACY_OMP_RECLASSIFICATION_META_KEY],
        |row| row.get_typed(0),
    )?;
    assert_eq!(
        completed,
        rebound_pending.replacen("analytics_pending:", "complete:", 1),
        "completion must promote the stored pending context exactly"
    );
    for cursor_key in [
        LEGACY_OMP_ANALYTICS_CURSOR_CONTEXT_META_KEY,
        LEGACY_OMP_ANALYTICS_CURSOR_LAST_ID_META_KEY,
        LEGACY_OMP_ANALYTICS_CURSOR_PROCESSED_META_KEY,
    ] {
        let rows: i64 = storage.conn.query_row_map(
            "SELECT COUNT(*) FROM meta WHERE key = ?1",
            fparams![cursor_key],
            |row| row.get_typed(0),
        )?;
        assert_eq!(rows, 0, "self-healing completion must retire {cursor_key}");
    }
    Ok(())
}

#[test]
fn connector_has_conversations_tracks_archived_agent_slug() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path)?;
    let agent_id = storage.ensure_agent(&Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: None,
        kind: AgentKind::Cli,
    })?;

    assert!(!storage.connector_has_conversations("codex")?);

    let conversation = Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: None,
        external_id: Some("connector-watermark-fixture".into()),
        title: Some("Connector watermark fixture".into()),
        source_path: PathBuf::from("/tmp/connector-watermark-fixture.jsonl"),
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
            content: "per-connector watermark regression".into(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };
    storage.insert_conversation_tree(agent_id, None, &conversation)?;

    assert!(storage.connector_has_conversations(" Codex ")?);
    assert!(!storage.connector_has_conversations("claude-code")?);
    assert!(!storage.connector_has_conversations(" ")?);
    Ok(())
}

#[test]
fn connector_scan_states_loads_watermarks_and_agent_presence() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path)?;
    let agent_id = storage.ensure_agent(&Agent {
        id: None,
        slug: "claude_code".into(),
        name: "Claude Code".into(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    storage.set_connector_last_scan_ts(" Codex ", 1_700_000_123_456)?;

    let conversation = Conversation {
        id: None,
        agent_slug: "claude_code".into(),
        workspace: None,
        external_id: Some("connector-scan-states-fixture".into()),
        title: Some("Connector scan states fixture".into()),
        source_path: PathBuf::from("/tmp/connector-scan-states-fixture.jsonl"),
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
            content: "bulk connector scan state regression".into(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };
    storage.insert_conversation_tree(agent_id, None, &conversation)?;

    let states = storage.connector_scan_states(&["codex", "claude", "gemini"])?;
    assert_eq!(
        states.get("codex").copied(),
        Some(ConnectorScanState {
            last_scan_ts: Some(1_700_000_123_456),
            ..ConnectorScanState::default()
        }),
        "bulk state should preserve connector-specific watermarks"
    );
    assert_eq!(
        states.get("claude").copied(),
        Some(ConnectorScanState {
            has_conversations: true,
            ..ConnectorScanState::default()
        }),
        "bulk state should honor known connector slug aliases"
    );
    assert_eq!(
        states.get("gemini").copied(),
        Some(ConnectorScanState::default()),
        "bulk state should identify newly enabled connectors with no archived rows"
    );
    Ok(())
}

#[test]
fn a_codex_watermark_from_before_its_scan_contract_leaves_the_contract_pending()
-> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let storage = SqliteStorage::open(&dir.path().join("test.db"))?;
    let pending = |storage: &SqliteStorage| -> anyhow::Result<bool> {
        Ok(storage.connector_scan_states(&["codex"])?["codex"].scan_contract_pending)
    };
    assert!(pending(&storage)?, "a fresh archive has not recorded it");

    // A watermark an older build wrote, without the contract.
    storage.conn.execute_compat(
        "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
        fparams!["last_scan_ts:connector:codex", "1700000000000"],
    )?;
    let state = storage.connector_scan_states(&["codex"])?["codex"];
    assert_eq!(state.last_scan_ts, Some(1_700_000_000_000));
    assert!(state.scan_contract_pending);

    // Another contract's marker does not satisfy this one.
    storage.conn.execute_compat(
        "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
        fparams!["scan_contract:connector:codex", "some-older-contract"],
    )?;
    assert!(pending(&storage)?);

    storage.record_connector_scan_contract("codex")?;
    assert!(!pending(&storage)?);

    // A watermark written by this build records the contract with it.
    let other = TempDir::new()?;
    let fresh = SqliteStorage::open(&other.path().join("test.db"))?;
    fresh.set_connector_last_scan_ts("CODEX", 1_700_000_000_001)?;
    assert!(!pending(&fresh)?);

    // A connector without a contract is never pending.
    storage.record_connector_scan_contract("claude")?;
    assert!(!storage.connector_scan_states(&["claude"])?["claude"].scan_contract_pending);
    Ok(())
}

#[test]
fn connector_has_conversations_checks_known_agent_slug_aliases() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path)?;
    let agent_id = storage.ensure_agent(&Agent {
        id: None,
        slug: "claude_code".into(),
        name: "Claude Code".into(),
        version: None,
        kind: AgentKind::Cli,
    })?;

    let conversation = Conversation {
        id: None,
        agent_slug: "claude_code".into(),
        workspace: None,
        external_id: Some("connector-watermark-claude-fixture".into()),
        title: Some("Claude connector watermark fixture".into()),
        source_path: PathBuf::from("/tmp/connector-watermark-claude-fixture.jsonl"),
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
            content: "claude connector alias regression".into(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };
    storage.insert_conversation_tree(agent_id, None, &conversation)?;

    assert!(
        storage.connector_has_conversations("claude")?,
        "the claude connector factory name must recognize legacy claude_code rows"
    );
    assert!(storage.connector_has_conversations("claude-code")?);
    assert!(storage.connector_has_conversations("claude_code")?);
    assert!(!storage.connector_has_conversations("codex")?);
    Ok(())
}
