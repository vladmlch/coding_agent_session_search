use super::*;
use crate::franken_sync::SqliteValue;
use crate::franken_sync::compat::{ConnectionExt as FrankenConnectionExt, RowExt as FrankenRowExt};
use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use crate::sources::provenance::LOCAL_SOURCE_ID;
use anyhow::Result;
use serial_test::serial;
use tempfile::TempDir;

#[test]
fn gh424_legacy_omp_pending_authority_requires_matching_completion() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let storage = SqliteStorage::open(&dir.path().join("authority.db"))?;
    assert!(!legacy_omp_analytics_pending(storage.raw())?);
    storage.raw().execute_compat(
        "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
        fparams![
            LEGACY_OMP_RECLASSIFICATION_META_KEY,
            "analytics_pending:context-a"
        ],
    )?;
    assert!(legacy_omp_analytics_pending(storage.raw())?);
    storage.raw().execute_compat(
        "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
        fparams![LEGACY_OMP_ANALYTICS_REBUILT_META_KEY, "context-b"],
    )?;
    assert!(legacy_omp_analytics_pending(storage.raw())?);
    storage.raw().execute_compat(
        "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
        fparams![LEGACY_OMP_ANALYTICS_REBUILT_META_KEY, "context-a"],
    )?;
    assert!(!legacy_omp_analytics_pending(storage.raw())?);
    storage.raw().execute_compat(
        "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
        fparams![LEGACY_OMP_RECLASSIFICATION_META_KEY, "analytics_pending"],
    )?;
    assert!(legacy_omp_analytics_pending(storage.raw())?);
    let error = storage
        .rebuild_legacy_omp_analytics_with_progress(None, None, None)
        .expect_err("unbound legacy authority requires canonical indexing first");
    assert!(format!("{error:#}").contains("unexpected marker state"));
    let metrics: i64 =
        storage
            .raw()
            .query_row_map("SELECT COUNT(*) FROM message_metrics", fparams![], |row| {
                row.get_typed(0)
            })?;
    assert_eq!(metrics, 0);
    storage.raw().execute_compat(
        "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
        fparams![LEGACY_OMP_RECLASSIFICATION_META_KEY, "broken-authority"],
    )?;
    assert!(legacy_omp_analytics_pending(storage.raw()).is_err());
    Ok(())
}

#[test]
fn literal_rowid_in_list_plans_as_primary_key_seek() -> anyhow::Result<()> {
    // GH #382 (semantic hydration stall): legacy engines
    // planned parameterized `WHERE id IN (?,?)` as a full messages SCAN,
    // so search hydration renders the ids as literals. Pin the plan the
    // literal form gets so an engine or query regression is caught here
    // rather than as a multi-minute hydration on a large archive.
    let dir = TempDir::new()?;
    let storage = SqliteStorage::open(&dir.path().join("test.db"))?;
    let plan_for = |sql: &str| -> anyhow::Result<String> {
        let rows: Vec<String> = storage.conn.query_map_collect(
            &format!("EXPLAIN QUERY PLAN {sql}"),
            fparams![],
            |row| {
                Ok(match row.get(3) {
                    Some(SqliteValue::Text(detail)) => detail.to_string(),
                    other => format!("{other:?}"),
                })
            },
        )?;
        Ok(rows.join(" | "))
    };
    let literal = plan_for(&format!(
        "SELECT id, conversation_id, content FROM messages WHERE id IN ({})",
        crate::storage::lexical_checkpoint_io::sql_rowid_literal_list(&[3, 99, 100_000])
    ))?;
    assert!(
        literal.contains("SEARCH") && literal.contains("INTEGER PRIMARY KEY"),
        "literal rowid IN-list must seek by primary key, got: {literal}"
    );
    assert!(
        !literal.contains("SCAN"),
        "literal rowid IN-list must not scan messages, got: {literal}"
    );
    let conversations = plan_for(&format!(
        "SELECT c.id FROM conversations c WHERE c.id IN ({})",
        crate::storage::lexical_checkpoint_io::sql_rowid_literal_list(&[1, 2])
    ))?;
    assert!(
        conversations.contains("SEARCH") && !conversations.contains("SCAN"),
        "literal conversation id IN-list must seek, got: {conversations}"
    );
    Ok(())
}

#[test]
fn literal_rowid_in_list_at_hydration_chunk_size_executes_and_seeks() -> anyhow::Result<()> {
    // GH #382: FTS5 hydration renders up to SQLITE_FTS5_HYDRATE_PARAM_CHUNK
    // (30_000) rowids as literals in ONE statement. A list that long goes
    // through a different parser/planner path than the three-element list
    // above (the parameterized form it replaced was bounded by the
    // 32_766 variable limit, which literals are not), so pin that
    // SQLite both executes it and still seeks by primary key.
    let dir = TempDir::new()?;
    let storage = SqliteStorage::open(&dir.path().join("test.db"))?;
    let ids: Vec<i64> = (1..=30_000_i64).map(|i| i * 7).collect();
    let list = crate::storage::lexical_checkpoint_io::sql_rowid_literal_list(&ids);
    let sql = format!("SELECT id FROM messages WHERE id IN ({list})");
    let plan: Vec<String> = storage.conn.query_map_collect(
        &format!("EXPLAIN QUERY PLAN {sql}"),
        fparams![],
        |row| {
            Ok(match row.get(3) {
                Some(SqliteValue::Text(detail)) => detail.to_string(),
                other => format!("{other:?}"),
            })
        },
    )?;
    let plan = plan.join(" | ");
    assert!(
        plan.contains("SEARCH") && !plan.contains("SCAN"),
        "a 30_000-literal rowid IN-list must still seek by primary key, got: {plan}"
    );
    let rows: Vec<bool> = storage
        .conn
        .query_map_collect(&sql, fparams![], |row| Ok(row.get(0).is_some()))?;
    assert!(rows.is_empty(), "empty messages table yields no rows");
    Ok(())
}

#[test]
fn antigravity_ide_rekey_promotes_external_identity_by_source_path() -> anyhow::Result<()> {
    // #454: conversations under the Antigravity IDE store were keyed by
    // the bare `<uuid>` until the connector learned to distinguish the IDE
    // and CLI stores; they now arrive as `ide/<uuid>`. The transcript path
    // embeds the uuid, so the re-key must reuse the existing row instead
    // of inserting a duplicate.
    let dir = TempDir::new()?;
    let storage = SqliteStorage::open(&dir.path().join("test.db"))?;
    let uuid = "00f0c687-1c8a-412b-a942-ec58f4b03c00";
    let source_path = dir
        .path()
        .join("home/.gemini/antigravity/brain")
        .join(uuid)
        .join(".system_generated/logs/transcript.jsonl");
    let started_at = 1_000_i64;
    let message = |idx: i64, content: &str| Message {
        id: None,
        idx,
        role: if idx == 0 {
            MessageRole::User
        } else {
            MessageRole::Agent
        },
        author: None,
        created_at: Some(started_at + idx),
        content: content.to_owned(),
        extra_json: serde_json::Value::Null,
        snippets: Vec::new(),
    };
    let conversation = |agent_slug: &str, external_id: &str, messages: Vec<Message>| Conversation {
        id: None,
        agent_slug: agent_slug.into(),
        workspace: None,
        external_id: Some(external_id.to_owned()),
        title: Some("Antigravity IDE session".into()),
        source_path: source_path.clone(),
        started_at: Some(started_at),
        ended_at: messages.iter().filter_map(|m| m.created_at).max(),
        approx_tokens: None,
        metadata_json: serde_json::json!({"source": agent_slug}),
        messages,
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };
    let agent_id = storage.ensure_agent(&Agent {
        id: None,
        slug: "antigravity".into(),
        name: "Antigravity".into(),
        version: None,
        kind: AgentKind::Cli,
    })?;

    // Legacy row: bare uuid.
    let legacy = conversation("antigravity", uuid, vec![message(0, "hello")]);
    let outcomes = storage.insert_conversations_batched(&[(agent_id, None, &legacy)])?;
    assert!(outcomes[0].conversation_inserted);

    // Re-scan with the store-qualified id and one appended message.
    let current = conversation(
        "antigravity",
        &format!("ide/{uuid}"),
        vec![message(0, "hello"), message(1, "world")],
    );
    let outcomes = storage.insert_conversations_batched(&[(agent_id, None, &current)])?;
    assert!(
        !outcomes[0].conversation_inserted,
        "the qualified id must reuse the legacy row, not insert a duplicate"
    );
    assert_eq!(outcomes[0].inserted_indices, vec![1]);

    let rows: Vec<(String, i64)> = storage.conn.query_map_collect(
        "SELECT c.external_id, COUNT(m.id)
         FROM conversations c LEFT JOIN messages m ON m.conversation_id = c.id
         WHERE c.agent_id = ?1
         GROUP BY c.id",
        fparams![agent_id],
        |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
    )?;
    assert_eq!(rows, vec![(format!("ide/{uuid}"), 2)]);

    // Control: the lane stays closed for providers whose source path may be
    // reused for unrelated sessions — a re-keyed codex row is a new row.
    let codex_id = storage.ensure_agent(&Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    let first = conversation("codex", "codex-a", vec![message(0, "hello")]);
    storage.insert_conversations_batched(&[(codex_id, None, &first)])?;
    let rekeyed = conversation(
        "codex",
        "codex-b",
        vec![message(0, "hello"), message(1, "world")],
    );
    let outcomes = storage.insert_conversations_batched(&[(codex_id, None, &rekeyed)])?;
    assert!(outcomes[0].conversation_inserted);
    let codex_rows: i64 = storage.conn.query_row_map(
        "SELECT COUNT(*) FROM conversations WHERE agent_id = ?1",
        fparams![codex_id],
        |row| row.get_typed(0),
    )?;
    assert_eq!(codex_rows, 2);
    Ok(())
}

#[test]
#[serial]
fn legacy_omp_first_scan_promotes_missing_or_changed_external_identity() -> anyhow::Result<()> {
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

    let cases = [
        ("missing.jsonl", None, "current-missing", 1_000_i64),
        (
            "changed.jsonl",
            Some("legacy-relative-id"),
            "current-relative-id",
            2_000_i64,
        ),
    ];
    let legacy = cases
        .iter()
        .map(
            |(filename, legacy_external_id, _, started_at)| Conversation {
                id: None,
                agent_slug: "pi_agent".into(),
                workspace: None,
                external_id: legacy_external_id.map(str::to_owned),
                title: Some(format!("Legacy {filename}")),
                source_path: dir
                    .path()
                    .join("home/.omp/agent/sessions/project")
                    .join(filename),
                started_at: Some(*started_at),
                ended_at: Some(*started_at + 1),
                approx_tokens: None,
                metadata_json: serde_json::json!({"source":"pi_agent"}),
                messages: vec![Message {
                    id: None,
                    idx: 0,
                    role: MessageRole::User,
                    author: None,
                    created_at: Some(*started_at),
                    content: format!("initial {filename}"),
                    extra_json: serde_json::Value::Null,
                    snippets: Vec::new(),
                }],
                source_id: LOCAL_SOURCE_ID.into(),
                origin_host: None,
            },
        )
        .collect::<Vec<_>>();
    let legacy_batch = legacy
        .iter()
        .map(|conversation| (pi_agent_id, None, conversation))
        .collect::<Vec<_>>();
    storage.insert_conversations_batched(&legacy_batch)?;

    assert_eq!(
        storage.reclassify_legacy_omp_conversations()?,
        LegacyOmpReclassificationResult {
            conversations_reclassified: 2,
            lexical_rebuild_required: true,
            analytics_rebuild_required: true,
        }
    );
    let omp_agent_id: i64 = storage.conn.query_row_map(
        "SELECT id FROM agents WHERE slug = 'omp'",
        fparams![],
        |row| row.get_typed(0),
    )?;

    let current = cases
        .iter()
        .map(
            |(filename, _, current_external_id, started_at)| Conversation {
                id: None,
                agent_slug: "omp".into(),
                workspace: None,
                external_id: Some((*current_external_id).to_owned()),
                title: Some(format!("Current {filename}")),
                source_path: dir
                    .path()
                    .join("home/.omp/agent/sessions/project")
                    .join(filename),
                started_at: Some(*started_at),
                ended_at: Some(*started_at + 2),
                approx_tokens: None,
                metadata_json: serde_json::json!({"source":"omp"}),
                messages: vec![
                    Message {
                        id: None,
                        idx: 0,
                        role: MessageRole::User,
                        author: None,
                        created_at: Some(*started_at),
                        content: format!("initial {filename}"),
                        extra_json: serde_json::Value::Null,
                        snippets: Vec::new(),
                    },
                    Message {
                        id: None,
                        idx: 1,
                        role: MessageRole::Agent,
                        author: None,
                        created_at: Some(*started_at + 2),
                        content: format!("appended {filename}"),
                        extra_json: serde_json::Value::Null,
                        snippets: Vec::new(),
                    },
                ],
                source_id: LOCAL_SOURCE_ID.into(),
                origin_host: None,
            },
        )
        .collect::<Vec<_>>();
    let current_batch = current
        .iter()
        .map(|conversation| (omp_agent_id, None, conversation))
        .collect::<Vec<_>>();
    let outcomes = storage.insert_conversations_batched(&current_batch)?;
    assert_eq!(outcomes.len(), 2);
    assert!(
        outcomes
            .iter()
            .all(|outcome| !outcome.conversation_inserted)
    );
    assert!(
        outcomes
            .iter()
            .all(|outcome| outcome.inserted_indices == vec![1])
    );

    let omp_conversation_count: i64 = storage.conn.query_row_map(
        "SELECT COUNT(*) FROM conversations WHERE agent_id = ?1",
        fparams![omp_agent_id],
        |row| row.get_typed(0),
    )?;
    assert_eq!(
        omp_conversation_count, 2,
        "the first current scan must reuse both reclassified rows"
    );
    for (filename, legacy_external_id, current_external_id, _) in cases {
        let source_path = dir
            .path()
            .join("home/.omp/agent/sessions/project")
            .join(filename);
        let (conversation_id, stored_external_id): (i64, String) = storage.conn.query_row_map(
            "SELECT id, external_id
                 FROM conversations
                 WHERE source_id = ?1 AND agent_id = ?2 AND source_path = ?3",
            fparams![
                LOCAL_SOURCE_ID,
                omp_agent_id,
                source_path.to_string_lossy().as_ref()
            ],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
        )?;
        assert_eq!(stored_external_id, current_external_id);
        let message_count: i64 = storage.conn.query_row_map(
            "SELECT COUNT(*) FROM messages WHERE conversation_id = ?1",
            fparams![conversation_id],
            |row| row.get_typed(0),
        )?;
        assert_eq!(message_count, 2);

        let current_lookup =
            conversation_external_lookup_key(LOCAL_SOURCE_ID, omp_agent_id, current_external_id);
        let current_lookup_id: i64 = storage.conn.query_row_map(
            "SELECT conversation_id
             FROM conversation_external_tail_lookup
             WHERE lookup_key = ?1",
            fparams![current_lookup.as_str()],
            |row| row.get_typed(0),
        )?;
        assert_eq!(current_lookup_id, conversation_id);

        if let Some(legacy_external_id) = legacy_external_id {
            let legacy_lookup =
                conversation_external_lookup_key(LOCAL_SOURCE_ID, omp_agent_id, legacy_external_id);
            let stale_lookup_count: i64 = storage.conn.query_row_map(
                "SELECT COUNT(*)
                 FROM conversation_external_tail_lookup
                 WHERE lookup_key = ?1",
                fparams![legacy_lookup.as_str()],
                |row| row.get_typed(0),
            )?;
            assert_eq!(stale_lookup_count, 0);
        }
    }
    Ok(())
}

#[test]
fn pi_agent_embedded_session_id_upgrade_rekeys_in_place() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let storage = SqliteStorage::open(&dir.path().join("test.db"))?;
    let pi_agent_id = storage.ensure_agent(&Agent {
        id: None,
        slug: "pi_agent".into(),
        name: "Pi Agent".into(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    let source_path = dir
        .path()
        .join("home/.pi/agent/sessions/project/session.jsonl");
    let first_message = Message {
        id: None,
        idx: 0,
        role: MessageRole::User,
        author: None,
        created_at: Some(1_000),
        content: "initial Pi message".into(),
        extra_json: serde_json::Value::Null,
        snippets: Vec::new(),
    };
    let legacy = Conversation {
        id: None,
        agent_slug: "pi_agent".into(),
        workspace: None,
        external_id: Some("project/session.jsonl".into()),
        title: Some("Legacy Pi session".into()),
        source_path: source_path.clone(),
        started_at: Some(1_000),
        ended_at: Some(1_000),
        approx_tokens: None,
        metadata_json: serde_json::json!({"source":"pi_agent"}),
        messages: vec![first_message.clone()],
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };
    let legacy_outcome = storage.insert_conversation_tree(pi_agent_id, None, &legacy)?;
    assert!(legacy_outcome.conversation_inserted);

    let current = Conversation {
        external_id: Some("stable-embedded-session-id".into()),
        title: Some("Current Pi session".into()),
        ended_at: Some(2_000),
        messages: vec![
            first_message,
            Message {
                id: None,
                idx: 1,
                role: MessageRole::Agent,
                author: None,
                created_at: Some(2_000),
                content: "appended Pi answer".into(),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            },
        ],
        ..legacy
    };
    let current_outcome = storage.insert_conversation_tree(pi_agent_id, None, &current)?;
    assert!(!current_outcome.conversation_inserted);
    assert_eq!(
        current_outcome.conversation_id,
        legacy_outcome.conversation_id
    );
    assert_eq!(current_outcome.inserted_indices, vec![1]);

    let rows: Vec<(i64, String)> = storage.conn.query_map_collect(
        "SELECT c.id, c.external_id
         FROM conversations c
         WHERE c.source_id = ?1 AND c.agent_id = ?2 AND c.source_path = ?3",
        fparams![
            LOCAL_SOURCE_ID,
            pi_agent_id,
            source_path.to_string_lossy().as_ref()
        ],
        |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
    )?;
    assert_eq!(
        rows,
        vec![(
            legacy_outcome.conversation_id,
            "stable-embedded-session-id".into()
        )],
        "the identity upgrade must not leave a path-keyed Pi twin"
    );
    let message_count: i64 = storage.conn.query_row_map(
        "SELECT COUNT(*) FROM messages WHERE conversation_id = ?1",
        fparams![legacy_outcome.conversation_id],
        |row| row.get_typed(0),
    )?;
    assert_eq!(message_count, 2);

    let stale_lookup_key =
        conversation_external_lookup_key(LOCAL_SOURCE_ID, pi_agent_id, "project/session.jsonl");
    let stale_lookup_count: i64 = storage.conn.query_row_map(
        "SELECT COUNT(*)
         FROM conversation_external_tail_lookup
         WHERE lookup_key = ?1",
        fparams![stale_lookup_key.as_str()],
        |row| row.get_typed(0),
    )?;
    assert_eq!(stale_lookup_count, 0);
    Ok(())
}

#[test]
#[serial]
fn legacy_omp_reclassification_rejects_same_source_path_with_different_identity()
-> anyhow::Result<()> {
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
    let shared_path = dir
        .path()
        .join("home/.omp/agent/sessions/project/session.jsonl");
    let shared_path = shared_path.to_string_lossy();
    storage.conn.execute_compat(
        "INSERT INTO conversations(
             agent_id, source_id, external_id, title, source_path, started_at
         ) VALUES(?1, 'local', NULL, 'Legacy OMP', ?2, 1000)",
        fparams![pi_agent_id, shared_path.as_ref()],
    )?;
    storage.conn.execute_compat(
        "INSERT INTO conversations(
             agent_id, source_id, external_id, title, source_path, started_at
         ) VALUES(?1, 'local', 'current-id', 'First-class OMP', ?2, 1000)",
        fparams![omp_agent_id, shared_path.as_ref()],
    )?;

    let error = storage
        .reclassify_legacy_omp_conversations()
        .expect_err("the exact same source path cannot acquire two OMP identities");
    assert!(
        error
            .to_string()
            .contains("cannot reclassify 1 legacy OMP conversation"),
        "unexpected collision diagnostic: {error:#}"
    );
    let slugs = storage.conn.query_map_collect(
        "SELECT a.slug
         FROM conversations c
         JOIN agents a ON a.id = c.agent_id
         ORDER BY c.id",
        fparams![],
        |row| row.get_typed::<String>(0),
    )?;
    assert_eq!(slugs, vec!["pi_agent", "omp"]);
    Ok(())
}

#[test]
#[serial]
fn legacy_omp_reclassification_preserves_undecodable_metadata() -> anyhow::Result<()> {
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
    let source_path = dir
        .path()
        .join("home/.omp/agent/sessions/project/corrupt-metadata.jsonl");
    let invalid_msgpack = vec![0xc1_u8];
    storage.conn.execute_compat(
        "INSERT INTO conversations(
             agent_id, source_id, external_id, title, source_path, started_at,
                 metadata_bin
             ) VALUES(?1, 'local', 'corrupt-metadata', 'Legacy OMP', ?2, 1000, ?3)",
        fparams![
            pi_agent_id,
            source_path.to_string_lossy().as_ref(),
            invalid_msgpack.as_slice()
        ],
    )?;

    let error = storage
        .reclassify_legacy_omp_conversations()
        .expect_err("invalid metadata must stop the identity rewrite without data loss");
    assert!(
        error
            .to_string()
            .contains("decoding MessagePack metadata for legacy OMP conversation"),
        "unexpected metadata diagnostic: {error:#}"
    );
    let (agent_slug, metadata_bin): (String, Vec<u8>) = storage.conn.query_row_map(
        "SELECT a.slug, c.metadata_bin
         FROM conversations c
         JOIN agents a ON a.id = c.agent_id
         WHERE c.external_id = 'corrupt-metadata'",
        fparams![],
        |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
    )?;
    assert_eq!(agent_slug, "pi_agent");
    assert_eq!(metadata_bin, invalid_msgpack);
    Ok(())
}
