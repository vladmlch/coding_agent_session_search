use super::*;
use crate::franken_sync::{
    Connection as FrankenConnection,
    compat::{
        ConnectionExt as FrankenConnectionExt, RowExt as FrankenRowExt,
        TransactionExt as FrankenTransactionExt,
    },
};
use crate::sources::provenance::LOCAL_SOURCE_ID;
use anyhow::{Context, Result};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use tempfile::TempDir;

#[test]
fn purge_agent_archive_data_removes_only_target_agent_and_rebuilds_derived_tables() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    fn seed_conversation(storage: &FrankenStorage, agent_slug: &str, marker: &str) {
        let agent = Agent {
            id: None,
            slug: agent_slug.into(),
            name: agent_slug.into(),
            version: None,
            kind: AgentKind::Cli,
        };
        let agent_id = storage.ensure_agent(&agent).unwrap();
        let conversation = Conversation {
            id: None,
            agent_slug: agent_slug.into(),
            workspace: Some(PathBuf::from("/tmp/workspace")),
            external_id: Some(format!("{agent_slug}-{marker}")),
            title: Some(format!("{agent_slug} {marker}")),
            source_path: PathBuf::from(format!("/tmp/{agent_slug}-{marker}.jsonl")),
            started_at: Some(1_700_000_000_000),
            ended_at: Some(1_700_000_000_100),
            approx_tokens: None,
            metadata_json: serde_json::Value::Null,
            messages: vec![
                Message {
                    id: None,
                    idx: 0,
                    role: MessageRole::User,
                    author: Some("user".into()),
                    created_at: Some(1_700_000_000_010),
                    content: format!("{agent_slug} {marker} user"),
                    extra_json: serde_json::Value::Null,
                    snippets: Vec::new(),
                },
                Message {
                    id: None,
                    idx: 1,
                    role: MessageRole::Agent,
                    author: Some("assistant".into()),
                    created_at: Some(1_700_000_000_020),
                    content: format!("{agent_slug} {marker} assistant"),
                    extra_json: serde_json::Value::Null,
                    snippets: Vec::new(),
                },
            ],
            source_id: LOCAL_SOURCE_ID.into(),
            origin_host: None,
        };
        storage
            .insert_conversation_tree(agent_id, None, &conversation)
            .unwrap();
    }

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("agent_search.db");
    let storage = FrankenStorage::open(&db_path).unwrap();

    seed_conversation(&storage, "openclaw", "purge-target");
    seed_conversation(&storage, "codex", "keep-target");
    let max_message_id = storage.max_message_id().unwrap().unwrap();
    storage
        .set_last_embedded_message_id(max_message_id)
        .unwrap();

    // An agent with no archived rows purges nothing and keeps the watermark.
    let noop = storage.purge_agent_archive_data("cursor").unwrap();
    assert_eq!(noop.conversations_deleted, 0);
    assert_eq!(
        storage.get_last_embedded_message_id().unwrap(),
        Some(max_message_id)
    );

    let purge = storage.purge_agent_archive_data("openclaw").unwrap();
    assert_eq!(purge.conversations_deleted, 1);
    assert_eq!(purge.messages_deleted, 2);
    // 2l1b0.78: the purged messages' vectors remain in the semantic artifact.
    assert_eq!(storage.get_last_embedded_message_id().unwrap(), None);

    storage.rebuild_fts().unwrap();
    storage.rebuild_analytics().unwrap();
    storage.rebuild_daily_stats().unwrap();
    storage.rebuild_token_daily_stats().unwrap();

    let agents = storage.list_agents().unwrap();
    assert_eq!(agents.len(), 1);
    assert_eq!(agents[0].slug, "codex");
    assert_eq!(storage.total_conversation_count().unwrap(), 1);
    assert_eq!(storage.total_message_count().unwrap(), 2);

    let fts_rows: i64 = storage
        .raw()
        .query_row_map("SELECT COUNT(*) FROM fts_messages", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(fts_rows, 2);

    let total_daily_sessions: i64 = storage
        .raw()
        .query_row_map(
            "SELECT COALESCE(SUM(session_count), 0)
             FROM daily_stats
             WHERE agent_slug = 'all' AND source_id = 'all'",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(total_daily_sessions, 1);

    let openclaw_token_rows: i64 = storage
        .raw()
        .query_row_map(
            "SELECT COUNT(*) FROM token_daily_stats WHERE agent_slug = 'openclaw'",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(openclaw_token_rows, 0);
}

/// #292 ask #2: `cass forget --source-glob` prunes the matching subset of
/// conversations from the canonical DB (dry-run reports, --apply deletes),
/// leaving non-matching conversations and their messages intact.
#[test]
fn forget_conversations_by_source_glob_dry_run_then_apply() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    fn seed(storage: &FrankenStorage, agent_slug: &str, source_path: &str, marker: &str) {
        let agent = Agent {
            id: None,
            slug: agent_slug.into(),
            name: agent_slug.into(),
            version: None,
            kind: AgentKind::Cli,
        };
        let agent_id = storage.ensure_agent(&agent).unwrap();
        let conversation = Conversation {
            id: None,
            agent_slug: agent_slug.into(),
            workspace: Some(PathBuf::from("/tmp/workspace")),
            external_id: Some(format!("{agent_slug}-{marker}")),
            title: Some(format!("{agent_slug} {marker}")),
            source_path: PathBuf::from(source_path),
            started_at: Some(1_700_000_000_000),
            ended_at: Some(1_700_000_000_100),
            approx_tokens: None,
            metadata_json: serde_json::Value::Null,
            messages: vec![Message {
                id: None,
                idx: 0,
                role: MessageRole::User,
                author: Some("user".into()),
                created_at: Some(1_700_000_000_010),
                content: format!("{marker} content"),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            }],
            source_id: LOCAL_SOURCE_ID.into(),
            origin_host: None,
        };
        storage
            .insert_conversation_tree(agent_id, None, &conversation)
            .unwrap();
    }

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("forget.db");
    let storage = FrankenStorage::open(&db_path).unwrap();

    // Two subagent transcripts (match the glob) + one top-level session.
    seed(
        &storage,
        "claude",
        "/home/u/.claude/projects/p/sess-1/subagents/agent-aaa.jsonl",
        "sub-a",
    );
    seed(
        &storage,
        "claude",
        "/home/u/.claude/projects/p/sess-2/subagents/agent-bbb.jsonl",
        "sub-b",
    );
    seed(
        &storage,
        "claude",
        "/home/u/.claude/projects/p/sess-3/session.jsonl",
        "top",
    );
    assert_eq!(storage.total_conversation_count().unwrap(), 3);

    let glob = "**/subagents/*.jsonl";
    // A semantic embed watermark that covers the whole corpus.
    let max_message_id = storage.max_message_id().unwrap().unwrap();
    storage
        .set_last_embedded_message_id(max_message_id)
        .unwrap();

    // Dry-run: reports matches, deletes nothing.
    let dry = storage
        .forget_conversations_by_source_glob(glob, true)
        .unwrap();
    assert!(dry.dry_run);
    assert_eq!(dry.conversations_matched, 2);
    assert_eq!(dry.messages_matched, 2);
    assert_eq!(dry.conversations_deleted, 0);
    assert_eq!(storage.total_conversation_count().unwrap(), 3);
    assert_eq!(
        storage.get_last_embedded_message_id().unwrap(),
        Some(max_message_id),
        "a dry run must not invalidate semantic assets"
    );
    assert!(
        storage.forgotten_source_stamps().unwrap().is_empty(),
        "a dry run must not tombstone anything"
    );

    // Apply: deletes the two subagent conversations, keeps the top-level one.
    let applied = storage
        .forget_conversations_by_source_glob(glob, false)
        .unwrap();
    assert!(!applied.dry_run);
    assert_eq!(applied.conversations_matched, 2);
    assert_eq!(applied.conversations_deleted, 2);
    assert_eq!(storage.total_conversation_count().unwrap(), 1);
    // The surviving top-level session's message is intact.
    assert_eq!(storage.total_message_count().unwrap(), 1);
    // 2l1b0.78: the deleted messages' vectors are still in the semantic
    // artifact, so the watermark that covered them must not survive.
    assert_eq!(storage.get_last_embedded_message_id().unwrap(), None);
    storage
        .set_last_embedded_message_id(max_message_id)
        .unwrap();
    // 2l1b0.50: each forgotten source is tombstoned with its file stamp
    // (these fixture paths do not exist, so the stamp is empty).
    let tombstones = storage.forgotten_source_stamps().unwrap();
    assert_eq!(
        tombstones
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "/home/u/.claude/projects/p/sess-1/subagents/agent-aaa.jsonl",
            "/home/u/.claude/projects/p/sess-2/subagents/agent-bbb.jsonl",
        ])
    );
    assert!(
        tombstones
            .values()
            .all(|stamp| *stamp == SourceFileStamp::default())
    );
    storage
        .clear_forgotten_sources(&[
            "/home/u/.claude/projects/p/sess-1/subagents/agent-aaa.jsonl".to_string(),
        ])
        .unwrap();
    assert_eq!(storage.forgotten_source_stamps().unwrap().len(), 1);

    // An empty pattern is rejected; a non-matching glob is a clean no-op.
    assert!(
        storage
            .forget_conversations_by_source_glob("   ", true)
            .is_err()
    );
    let none = storage
        .forget_conversations_by_source_glob("**/nope/*.jsonl", false)
        .unwrap();
    assert_eq!(none.conversations_matched, 0);
    assert_eq!(none.conversations_deleted, 0);
    assert_eq!(storage.total_conversation_count().unwrap(), 1);
    assert_eq!(
        storage.get_last_embedded_message_id().unwrap(),
        Some(max_message_id),
        "a no-op forget must not force a semantic re-embed"
    );
}

#[test]
fn forget_selectors_intersect_and_age_uses_known_message_timestamps() -> Result<()> {
    let directory = TempDir::new()?;
    let path = directory.path().join("selectors.db");
    let storage = FrankenStorage::open(&path)?;
    let base = 1_700_000_000_000;
    let fixtures = [
        (
            "retire-a",
            Some("/work/scratch"),
            vec![Some(base + 20), Some(base + 40)],
        ),
        (
            "recent",
            Some("/work/scratch"),
            vec![Some(base + 20), Some(base + 200)],
        ),
        ("boundary", Some("/work/scratch"), vec![Some(base + 100)]),
        ("unknown", Some("/work/scratch"), vec![None]),
        (
            "partly-unknown",
            Some("/work/scratch"),
            vec![Some(base + 20), None],
        ),
        ("empty", Some("/work/scratch"), vec![]),
        ("no-workspace", None, vec![Some(base + 20)]),
        (
            "production",
            Some("/work/production"),
            vec![Some(base + 20)],
        ),
    ];
    for (id, workspace, timestamps) in &fixtures {
        seed_forget_fixture(
            &storage,
            &forget_fixture_conversation(
                &format!("/fixtures/{id}.jsonl"),
                Some(id),
                *workspace,
                timestamps,
            ),
        )?;
    }
    assert_eq!(storage.total_conversation_count()?, 8);
    let source = ForgetConversationsFilter {
        source_glob: Some("**/retire-*.jsonl"),
        ..Default::default()
    };
    let workspace = ForgetConversationsFilter {
        workspace_glob: Some("/work/scr*"),
        ..Default::default()
    };
    let age = ForgetConversationsFilter {
        before_ms: Some(base + 100),
        ..Default::default()
    };
    assert_eq!(
        storage
            .forget_conversations(&source, true)?
            .conversations_matched,
        1
    );
    assert_eq!(
        storage
            .forget_conversations(&workspace, true)?
            .conversations_matched,
        6
    );
    assert_eq!(
        storage
            .forget_conversations(&age, true)?
            .conversations_matched,
        3
    );
    assert_eq!(
        storage
            .forget_conversations(
                &ForgetConversationsFilter {
                    workspace_glob: Some("*"),
                    ..Default::default()
                },
                true
            )?
            .conversations_matched,
        7,
        "an unknown workspace is not the empty string"
    );
    for filter in [
        ForgetConversationsFilter::default(),
        ForgetConversationsFilter {
            source_glob: Some("["),
            ..Default::default()
        },
        ForgetConversationsFilter {
            workspace_glob: Some("  "),
            ..Default::default()
        },
    ] {
        assert!(storage.forget_conversations(&filter, true).is_err());
    }
    let intersection = ForgetConversationsFilter {
        source_glob: source.source_glob,
        workspace_glob: workspace.workspace_glob,
        before_ms: age.before_ms,
    };
    let preview = storage.forget_conversations(&intersection, true)?;
    assert_eq!(preview.conversations_matched, 1);
    assert_eq!(preview.messages_matched, 2);
    assert_eq!(preview.conversations_deleted, 0);
    assert!(storage.forgotten_source_stamps()?.is_empty());
    assert!(storage.forgotten_conversation_keys()?.is_empty());
    assert_eq!(storage.total_conversation_count()?, 8);
    let applied = storage.forget_conversations(&intersection, false)?;
    assert_eq!(applied.conversations_deleted, 1);
    assert_eq!(applied.messages_matched, preview.messages_matched);
    assert_eq!(applied.before_ms, Some(base + 100));
    assert_eq!(applied.workspace_glob.as_deref(), Some("/work/scr*"));
    assert_eq!(storage.total_conversation_count()?, 7);
    assert_eq!(storage.total_message_count()?, 8);
    assert_eq!(storage.forgotten_source_stamps()?.len(), 1);
    assert!(storage.forgotten_conversation_keys()?.is_empty());
    Ok(())
}

#[test]
fn forget_derived_rebuild_marker_is_atomic_durable_and_survives_empty_retry() -> Result<()> {
    let directory = TempDir::new()?;
    let path = directory.path().join("forget-retry.db");
    let storage = FrankenStorage::open(&path)?;
    let source = "/fixture/retry.sqlite";
    let conversation_id = seed_forget_fixture(
        &storage,
        &forget_fixture_conversation(source, Some("retired"), Some("/work/old"), &[Some(10)]),
    )?;
    assert!(!storage.forget_derived_rebuild_pending()?);
    assert_eq!(
        storage
            .forget_conversations_by_source_glob(source, true)?
            .conversations_matched,
        1
    );
    assert!(!storage.forget_derived_rebuild_pending()?);

    // Force failure after the marker and session tombstone were written.
    // Their rollback must be atomic with the canonical deletion attempt.
    storage.raw().execute(
        "CREATE TABLE forget_retry_guard (
            conversation_id INTEGER PRIMARY KEY
                REFERENCES conversations(id) ON DELETE RESTRICT
         )",
    )?;
    storage.raw().execute_compat(
        "INSERT INTO forget_retry_guard(conversation_id) VALUES (?1)",
        fparams![conversation_id],
    )?;
    assert!(
        storage
            .forget_conversations_by_source_glob(source, false)
            .is_err()
    );
    assert_eq!(storage.total_conversation_count()?, 1);
    assert_eq!(storage.total_message_count()?, 1);
    assert!(storage.forgotten_conversation_keys()?.is_empty());
    assert!(storage.forgotten_source_stamps()?.is_empty());
    assert!(!storage.forget_derived_rebuild_pending()?);
    storage.raw().execute("DELETE FROM forget_retry_guard")?;

    assert_eq!(
        storage
            .forget_conversations_by_source_glob(source, false)?
            .conversations_deleted,
        1
    );
    assert!(storage.forget_derived_rebuild_pending()?);
    assert_eq!(storage.forgotten_conversation_keys()?.len(), 1);
    storage.close()?;

    let reopened = FrankenStorage::open(&path)?;
    assert!(reopened.forget_derived_rebuild_pending()?);
    for dry_run in [true, false] {
        let retry = reopened.forget_conversations_by_source_glob(source, dry_run)?;
        assert_eq!(retry.conversations_matched, 0);
        assert_eq!(retry.conversations_deleted, 0);
        assert!(
            reopened.forget_derived_rebuild_pending()?,
            "a no-match retry must preserve unfinished derived work"
        );
    }
    reopened.clear_forget_derived_rebuild_pending()?;
    assert!(!reopened.forget_derived_rebuild_pending()?);
    reopened.close()?;
    let reader = FrankenStorage::open_strict_readonly(&path)?;
    assert!(!reader.forget_derived_rebuild_pending()?);
    reader.close_without_checkpoint()?;
    Ok(())
}

#[test]
fn forgotten_source_batch_lookup_is_indexed_bounded_and_observes_changes() -> Result<()> {
    let directory = TempDir::new()?;
    let storage = FrankenStorage::open(&directory.path().join("batch-source-stamps.db"))?;
    // A nonempty request against an empty table must not cache absence:
    // this same path is inserted below and must become visible afterward.
    assert!(
        storage
            .forgotten_source_stamps_for(&["/fixture/history/rollout-7.jsonl".into()])?
            .is_empty()
    );
    let mut fixtures = Vec::new();
    let mut tx = storage.raw().transaction()?;
    for index in 0..512_i64 {
        let path = format!("/fixture/history/rollout-{index}.jsonl");
        let stamp = SourceFileStamp {
            size_bytes: (index % 3 != 0).then_some(index * 100),
            mtime_ms: (index % 5 != 0).then_some(1_700_000_000_000 + index),
        };
        tx.execute_compat(
            "INSERT INTO forgotten_sources
                (source_path, size_bytes, mtime_ms, forgotten_at_ms)
             VALUES (?1, ?2, ?3, 1)",
            fparams![path.as_str(), stamp.size_bytes, stamp.mtime_ms],
        )?;
        fixtures.push((path, stamp));
    }
    tx.execute_compat(
        "UPDATE forgotten_sources SET mtime_ms = 'unreadable unrelated stamp'
         WHERE source_path = ?1",
        fparams![fixtures[400].0.as_str()],
    )?;
    tx.commit()?;
    assert!(
        storage.forgotten_source_stamps().is_err(),
        "negative control: full-table decoding encounters the unrelated malformed stamp"
    );
    assert!(storage.forgotten_source_stamps_for(&[])?.is_empty());

    let request = [
        fixtures[0].0.clone(),
        fixtures[7].0.clone(),
        fixtures[511].0.clone(),
        fixtures[7].0.clone(),
        format!("{}.sibling", fixtures[7].0),
        "/fixture/history/not-archived.jsonl".into(),
    ];
    assert_eq!(
        storage.forgotten_source_stamps_for(&request)?,
        HashMap::from([
            fixtures[0].clone(),
            fixtures[7].clone(),
            fixtures[511].clone()
        ])
    );
    let many_paths: Vec<_> = fixtures[..260]
        .iter()
        .map(|(path, _)| path.clone())
        .collect();
    assert_eq!(
        storage.forgotten_source_stamps_for(&many_paths)?,
        fixtures[..260].iter().cloned().collect(),
        "large requests must retain every requested nullable stamp"
    );
    let plan: Vec<String> = storage.raw().query_map_collect(
        "EXPLAIN QUERY PLAN SELECT size_bytes, mtime_ms FROM forgotten_sources
         INDEXED BY sqlite_autoindex_forgotten_sources_1
         WHERE source_path = ?1 LIMIT 1",
        fparams![fixtures[7].0.as_str()],
        |row| row.get_typed(3),
    )?;
    assert!(
        plan.iter().any(|detail| detail.contains("SEARCH")
            && detail.contains("sqlite_autoindex_forgotten_sources_1")),
        "ordinary-file admission must use exact source-path seeks: {plan:?}"
    );

    // Changed-file re-inclusion clears a stamp. The next bounded lookup
    // must observe that durable removal, with no process-local cache.
    storage.clear_forgotten_sources(&[fixtures[7].0.clone()])?;
    assert_eq!(
        storage.forgotten_source_stamps_for(&request)?,
        HashMap::from([fixtures[0].clone(), fixtures[511].clone()])
    );
    assert!(!storage.forget_derived_rebuild_pending()?);
    Ok(())
}

#[test]
fn forgotten_conversation_batch_lookup_uses_exact_indexed_keys() -> Result<()> {
    let directory = TempDir::new()?;
    let storage = FrankenStorage::open(&directory.path().join("batch-exclusions.db"))?;
    assert!(
        storage
            .forgotten_conversation_keys_for(&[ForgottenConversationKey {
                source_id: "local".into(),
                agent_slug: "opencode".into(),
                source_path: "/fixture/store-0/sessions.db".into(),
                external_id: "session-7".into(),
            }])?
            .is_empty(),
        "an initially empty table must not hide the same key after insertion below"
    );
    let mut keys = Vec::new();
    let mut tx = storage.raw().transaction()?;
    for index in 0..512 {
        let key = ForgottenConversationKey {
            source_id: "local".into(),
            agent_slug: "opencode".into(),
            source_path: format!("/fixture/store-{}/sessions.db", index % 7),
            external_id: format!("session-{index}"),
        };
        tx.execute_compat(
            "INSERT INTO forgotten_conversations
                (source_id, agent_slug, source_path, external_id, forgotten_at_ms)
             VALUES (?1, ?2, ?3, ?4, 1)",
            fparams![
                key.source_id.as_str(),
                key.agent_slug.as_str(),
                key.source_path.as_str(),
                key.external_id.as_str()
            ],
        )?;
        keys.push(key);
    }
    // An unrelated malformed identity proves that admission does not
    // decode the rest of the exclusion table to answer a small batch.
    tx.execute(
        "INSERT INTO forgotten_conversations
            (source_id, agent_slug, source_path, external_id, forgotten_at_ms)
         VALUES (X'FF', 'opencode', '/unrelated/store.db', 'unrelated', 1)",
    )?;
    tx.commit()?;
    assert!(
        storage.forgotten_conversation_keys().is_err(),
        "negative control: a full-table decode reaches the malformed identity"
    );
    assert!(storage.forgotten_conversation_keys_for(&[])?.is_empty());

    let mut other_source = keys[7].clone();
    other_source.source_id = "remote-host".into();
    let mut other_agent = keys[7].clone();
    other_agent.agent_slug = "devin".into();
    let mut other_path = keys[7].clone();
    other_path.source_path.push_str("/sibling");
    let mut other_session = keys[7].clone();
    other_session.external_id = "not-archived".into();
    let request = [
        keys[7].clone(),
        keys[511].clone(),
        keys[7].clone(),
        other_source,
        other_agent,
        other_path,
        other_session,
    ];
    assert_eq!(
        storage.forgotten_conversation_keys_for(&request)?,
        HashSet::from([keys[7].clone(), keys[511].clone()])
    );
    assert_eq!(
        storage.forgotten_conversation_keys_for(&keys[..260])?,
        keys[..260].iter().cloned().collect()
    );
    let plan: Vec<String> = storage.raw().query_map_collect(
        "EXPLAIN QUERY PLAN SELECT 1 FROM forgotten_conversations
         INDEXED BY sqlite_autoindex_forgotten_conversations_1
         WHERE source_id = ?1 AND agent_slug = ?2
           AND source_path = ?3 AND external_id = ?4
         LIMIT 1",
        fparams![
            keys[7].source_id.as_str(),
            keys[7].agent_slug.as_str(),
            keys[7].source_path.as_str(),
            keys[7].external_id.as_str()
        ],
        |row| row.get_typed(3),
    )?;
    assert!(
        plan.iter().any(|detail| detail.contains("SEARCH")
            && detail.contains("sqlite_autoindex_forgotten_conversations_1")),
        "batch admission must use exact composite-key seeks: {plan:?}"
    );
    assert!(!storage.forget_derived_rebuild_pending()?);
    Ok(())
}

#[test]
fn reinclude_exact_session_preserves_provenance_siblings_and_pending_work() -> Result<()> {
    let directory = TempDir::new()?;
    let db_path = directory.path().join("reinclude.db");
    let provider = directory.path().join("project[1]/sessions.db");
    fs::create_dir_all(provider.parent().context("provider parent")?)?;
    let provider_bytes = b"unchanged provider evidence";
    fs::write(&provider, provider_bytes)?;
    let source = provider.to_string_lossy().into_owned();
    let storage = FrankenStorage::open(&db_path)?;
    let local = forget_fixture_conversation(
        &source,
        Some("shared-id"),
        Some("/work/retired"),
        &[Some(10)],
    );
    seed_forget_fixture(&storage, &local)?;
    let kept_id = seed_forget_fixture(
        &storage,
        &forget_fixture_conversation(&source, Some("kept-id"), Some("/work/current"), &[Some(20)]),
    )?;
    let mut remote = local.clone();
    remote.source_id = "remote-fixture".into();
    remote.origin_host = Some("remote-fixture".into());
    seed_forget_fixture(&storage, &remote)?;
    let mut other_agent = local.clone();
    other_agent.agent_slug = "devin".into();
    seed_forget_fixture(&storage, &other_agent)?;
    let mut other_path = local;
    other_path.source_path = directory.path().join("other.db");
    // The canonical merge identity is (source_id, agent, external_id) —
    // sharing `shared-id` under a different path now merges into `local`
    // instead of creating a sibling row. A distinct external id keeps this
    // fixture a real path sibling again.
    other_path.external_id = Some("other-path-id".into());
    other_path.title = other_path.external_id.clone();
    seed_forget_fixture(&storage, &other_path)?;
    assert_eq!(
        storage
            .forget_conversations(
                &ForgetConversationsFilter {
                    workspace_glob: Some("/work/retired"),
                    ..Default::default()
                },
                false,
            )?
            .conversations_deleted,
        4
    );
    for (key, path) in [
        ("source_ingest_v1:shared", source.as_str()),
        ("source_ingest_v1:other", "/fixture/unrelated.db"),
    ] {
        storage.raw().execute_compat(
            "INSERT INTO meta(key, value) VALUES (?1, ?2)",
            fparams![
                key,
                serde_json::json!({"primary": {"path": path}, "dependencies": []}).to_string()
            ],
        )?;
    }
    storage.set_last_scan_ts(123)?;
    storage.set_connector_last_scan_ts("opencode", 456)?;
    storage.set_connector_last_scan_ts("codex", 789)?;
    let before = storage.list_forget_exclusions(&ForgetExclusionSelector::default())?;
    assert_eq!(before.exclusions.len(), 4);
    assert!(before.legacy_sources.is_empty());
    let selector = ForgetExclusionSelector {
        source_path: Some(&source),
        source_id: Some(LOCAL_SOURCE_ID),
        agent_slug: Some("opencode"),
        external_id: Some("shared-id"),
        ..Default::default()
    };
    let listed = storage.list_forget_exclusions(&selector)?;
    assert_eq!(listed.exclusions.len(), 1);
    assert!(!listed.exclusions[0].blocked_by_source_stamp);
    assert_eq!(listed.exclusions[0].key.source_path, source);
    assert!(listed.exclusions[0].forgotten_at_ms > 0);
    let json = serde_json::to_value(&listed.exclusions[0])?;
    assert_eq!(json["source_id"], LOCAL_SOURCE_ID);
    assert!(json.get("workspace").is_none());
    assert!(json.get("last_message_at").is_none());
    assert!(
        storage
            .list_forget_exclusions(&ForgetExclusionSelector {
                source_glob: Some(&source),
                ..Default::default()
            })?
            .exclusions
            .is_empty(),
        "literal bracketed paths must use source-path rather than glob interpretation"
    );
    for invalid in [
        ForgetExclusionSelector::default(),
        ForgetExclusionSelector {
            source_glob: Some("*"),
            source_path: Some(&source),
            ..Default::default()
        },
        ForgetExclusionSelector {
            source_glob: Some("["),
            ..Default::default()
        },
        ForgetExclusionSelector {
            external_id: Some(" "),
            ..Default::default()
        },
    ] {
        for dry_run in [true, false] {
            assert!(
                storage
                    .reinclude_forget_exclusions(&invalid, dry_run)
                    .is_err()
            );
        }
    }
    let preview = storage.reinclude_forget_exclusions(&selector, true)?;
    assert_eq!(preview.exclusions_matched, 1);
    assert_eq!(preview.exclusions_removed, 0);
    assert_eq!(preview.source_observations_matched, 1);
    assert_eq!(preview.source_observations_removed, 0);
    assert_eq!(preview.sample_sessions, listed.exclusions);
    assert_eq!(
        storage.list_forget_exclusions(&ForgetExclusionSelector::default())?,
        before
    );
    assert_eq!(storage.source_ingest_ledger_entries()?.len(), 2);
    assert!(storage.forget_derived_rebuild_pending()?);

    let applied = storage.reinclude_forget_exclusions(&selector, false)?;
    assert_eq!(applied.exclusions_removed, 1);
    assert_eq!(applied.legacy_sources_removed, 0);
    assert_eq!(applied.source_observations_removed, 1);
    let remaining_ledger = storage.source_ingest_ledger_entries()?;
    assert_eq!(remaining_ledger.len(), 1);
    assert!(remaining_ledger.contains_key("source_ingest_v1:other"));
    assert!(
        storage
            .list_forget_exclusions(&selector)?
            .exclusions
            .is_empty()
    );
    assert_eq!(storage.forgotten_conversation_keys()?.len(), 3);
    assert_eq!(storage.total_conversation_count()?, 1);
    assert_eq!(storage.fetch_messages(kept_id)?.len(), 1);
    assert_eq!(storage.get_last_scan_ts()?, Some(123));
    assert_eq!(storage.get_connector_last_scan_ts("opencode")?, Some(0));
    assert_eq!(storage.get_connector_last_scan_ts("codex")?, Some(789));
    assert!(storage.forget_derived_rebuild_pending()?);
    storage
        .raw()
        .execute("INSERT INTO meta(key, value) VALUES ('source_ingest_v1:after-release', '{}')")?;
    let no_match = storage.reinclude_forget_exclusions(&selector, false)?;
    assert_eq!(no_match.exclusions_matched, 0);
    assert_eq!(no_match.exclusions_removed, 0);
    assert_eq!(no_match.source_observations_removed, 0);
    assert_eq!(storage.source_ingest_ledger_entries()?.len(), 2);
    storage.close()?;
    let reopened = FrankenStorage::open_strict_readonly(&db_path)?;
    assert_eq!(
        reopened
            .list_forget_exclusions(&ForgetExclusionSelector::default())?
            .exclusions
            .len(),
        3
    );
    assert!(reopened.forget_derived_rebuild_pending()?);
    reopened.close_without_checkpoint()?;
    assert_eq!(fs::read(provider)?, provider_bytes);
    Ok(())
}

#[test]
fn reinclude_logical_handles_replay_physical_stores_without_invalidating_siblings() -> Result<()> {
    for (name, present) in [
        ("opencode.db", true),
        ("extensionless-store", true),
        ("temporarily-missing-store", false),
    ] {
        let directory = TempDir::new()?;
        let storage = FrankenStorage::open(&directory.path().join("archive.db"))?;
        let source = directory.path().join(name);
        let source_bytes = b"SQLite format 3\0unchanged source evidence";
        if present {
            fs::write(&source, source_bytes)?;
        }
        for session in ["ses-restored", "ses-kept"] {
            storage.raw().execute_compat(
                "INSERT INTO forgotten_conversations
                 (source_id, agent_slug, source_path, external_id, forgotten_at_ms)
                 VALUES ('local', 'opencode', ?1, ?2, 1)",
                fparams![source.join(session).to_string_lossy().as_ref(), session],
            )?;
        }
        let sibling = directory.path().join(format!("{name}-sibling"));
        fs::write(&sibling, b"unrelated source")?;
        for (key, observation) in [
            (
                "physical-primary",
                serde_json::json!({"primary": {"path": source}, "dependencies": []}),
            ),
            (
                "physical-dependency",
                serde_json::json!({"primary": {"path": "/fixture/consumer"}, "dependencies": [{"path": source}]}),
            ),
            (
                "same-directory",
                serde_json::json!({"primary": {"path": sibling}, "dependencies": [{"path": directory.path()}]}),
            ),
            (
                "directory-primary",
                serde_json::json!({"primary": {"path": directory.path()}, "dependencies": []}),
            ),
        ] {
            storage.record_filtered_source_completion(&SourceIngestLedgerEntry {
                key: format!("source_ingest_v1:{key}"),
                observation: observation.to_string(),
            })?;
        }
        storage.set_last_scan_ts(900)?;
        storage.set_connector_last_scan_ts("opencode", 800)?;
        storage.set_connector_last_scan_ts("codex", 700)?;
        let logical_path = source.join("ses-restored");
        let selector = ForgetExclusionSelector {
            source_path: Some(logical_path.to_str().context("UTF-8 selected source")?),
            external_id: Some("ses-restored"),
            ..Default::default()
        };
        let before = storage.source_ingest_ledger_entries()?;
        let preview = storage.reinclude_forget_exclusions(&selector, true)?;
        assert_eq!(preview.exclusions_matched, 1);
        assert_eq!(
            preview.source_observations_matched,
            if present { 2 } else { 1 }
        );
        assert_eq!(preview.source_observations_removed, 0);
        assert_eq!(storage.source_ingest_ledger_entries()?, before);
        assert_eq!(storage.get_connector_last_scan_ts("opencode")?, Some(800));
        let result = storage.reinclude_forget_exclusions(&selector, false)?;
        assert_eq!(result.exclusions_removed, 1);
        assert_eq!(
            result.source_observations_removed,
            if present { 2 } else { 1 }
        );
        let ledger = storage.source_ingest_ledger_entries()?;
        assert!(!ledger.contains_key("source_ingest_v1:physical-primary"));
        assert_eq!(
            ledger.contains_key("source_ingest_v1:physical-dependency"),
            !present
        );
        assert!(ledger.contains_key("source_ingest_v1:same-directory"));
        assert!(ledger.contains_key("source_ingest_v1:directory-primary"));
        assert_eq!(storage.get_connector_last_scan_ts("opencode")?, Some(0));
        assert_eq!(storage.get_connector_last_scan_ts("codex")?, Some(700));
        assert_eq!(storage.get_last_scan_ts()?, Some(900));
        let retained = storage.forgotten_conversation_keys()?;
        assert_eq!(retained.len(), 1);
        assert!(retained.iter().all(|key| key.external_id == "ses-kept"));
        if present {
            assert_eq!(fs::read(&source)?, source_bytes);
        } else {
            assert!(!source.exists());
        }
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn reinclude_logical_alias_resets_provider_aliases_and_only_its_remote_mirror() -> Result<()> {
    use std::os::unix::fs::symlink;

    let directory = TempDir::new()?;
    let storage = FrankenStorage::open(&directory.path().join("archive.db"))?;
    let source = directory.path().join("physical-store");
    let alias = directory.path().join("discovered-store.db");
    fs::write(&source, b"SQLite format 3\0unchanged store")?;
    symlink(&source, &alias)?;
    let logical_path = alias.join("ses-restored");
    storage.raw().execute_compat(
        "INSERT INTO forgotten_conversations
         (source_id, agent_slug, source_path, external_id, forgotten_at_ms)
         VALUES ('remote-a', 'claude_code', ?1, 'ses-restored', 1)",
        fparams![logical_path.to_string_lossy().as_ref()],
    )?;
    storage.record_filtered_source_completion(&SourceIngestLedgerEntry {
        key: "source_ingest_v1:physical-store".into(),
        observation: serde_json::json!({
            "primary": {"path": source}, "dependencies": []
        })
        .to_string(),
    })?;
    storage.set_last_scan_ts(900)?;
    storage.set_connector_last_scan_ts("claude", 800)?;
    storage.set_connector_last_scan_ts("codex", 700)?;
    storage.record_mirror_scan_fingerprint("remote-a", "selected-mirror")?;
    storage.record_mirror_scan_fingerprint("remote-b", "unrelated-mirror")?;
    let selector = ForgetExclusionSelector {
        source_path: Some(logical_path.to_str().context("UTF-8 selected alias")?),
        external_id: Some("ses-restored"),
        ..Default::default()
    };
    let preview = storage.reinclude_forget_exclusions(&selector, true)?;
    assert_eq!(preview.source_observations_matched, 1);
    assert_eq!(storage.get_connector_last_scan_ts("claude")?, Some(800));
    assert_eq!(
        storage.read_mirror_scan_fingerprint("remote-a")?.as_deref(),
        Some("selected-mirror")
    );
    let result = storage.reinclude_forget_exclusions(&selector, false)?;
    assert_eq!(result.exclusions_removed, 1);
    assert_eq!(result.source_observations_removed, 1);
    assert!(storage.source_ingest_ledger_entries()?.is_empty());
    for provider in ["claude", "claude-code", "claude_code"] {
        assert_eq!(storage.get_connector_last_scan_ts(provider)?, Some(0));
    }
    assert_eq!(storage.get_connector_last_scan_ts("codex")?, Some(700));
    assert_eq!(storage.get_last_scan_ts()?, Some(900));
    assert_eq!(storage.read_mirror_scan_fingerprint("remote-a")?, None);
    assert_eq!(
        storage.read_mirror_scan_fingerprint("remote-b")?.as_deref(),
        Some("unrelated-mirror")
    );
    storage.set_connector_last_scan_ts("claude", 1000)?;
    storage.record_mirror_scan_fingerprint("remote-a", "new-completion")?;
    assert_eq!(
        storage
            .reinclude_forget_exclusions(&selector, false)?
            .exclusions_removed,
        0
    );
    assert_eq!(storage.get_connector_last_scan_ts("claude")?, Some(1000));
    assert_eq!(
        storage.read_mirror_scan_fingerprint("remote-a")?.as_deref(),
        Some("new-completion")
    );
    assert_eq!(fs::read(&alias)?, b"SQLite format 3\0unchanged store");
    Ok(())
}

#[test]
fn reinclude_identity_scope_refuses_source_stamp_overlap_before_any_mutation() -> Result<()> {
    let directory = TempDir::new()?;
    let storage = FrankenStorage::open(&directory.path().join("overlap.db"))?;
    for (source, session) in [
        ("/fixture/blocked.db", "blocked"),
        ("/fixture/free.db", "free"),
    ] {
        seed_forget_fixture(
            &storage,
            &forget_fixture_conversation(source, Some(session), Some("/work/old"), &[Some(10)]),
        )?;
    }
    storage.forget_conversations_by_source_glob("/fixture/*.db", false)?;
    storage.raw().execute(
        "INSERT INTO forgotten_sources(source_path, size_bytes, mtime_ms, forgotten_at_ms)
         VALUES ('/fixture/blocked.db', 10, 20, 30)",
    )?;
    storage.record_filtered_source_completion(&SourceIngestLedgerEntry {
        key: "source_ingest_v1:overlap".into(),
        observation: serde_json::json!({
            "primary": {"path": "/fixture/blocked.db"}, "dependencies": []
        })
        .to_string(),
    })?;
    storage.set_last_scan_ts(900)?;
    storage.set_connector_last_scan_ts("opencode", 800)?;
    storage.set_connector_last_scan_ts("codex", 700)?;
    storage.record_mirror_scan_fingerprint("remote-fixture", "before-release")?;
    let selector = ForgetExclusionSelector {
        agent_slug: Some("opencode"),
        ..Default::default()
    };
    let preview = storage.reinclude_forget_exclusions(&selector, true)?;
    assert_eq!(preview.exclusions_matched, 2);
    assert_eq!(preview.legacy_sources_matched, 0);
    assert_eq!(preview.blocked_by_source_stamps, ["/fixture/blocked.db"]);
    let before = storage.list_forget_exclusions(&ForgetExclusionSelector::default())?;
    assert_eq!(before.legacy_sources.len(), 1);
    assert!(
        storage
            .reinclude_forget_exclusions(&selector, false)
            .unwrap_err()
            .to_string()
            .contains("source-wide exclusion")
    );
    assert_eq!(
        storage.list_forget_exclusions(&ForgetExclusionSelector::default())?,
        before
    );
    assert_eq!(storage.source_ingest_ledger_entries()?.len(), 1);
    assert_eq!(storage.get_last_scan_ts()?, Some(900));
    assert_eq!(storage.get_connector_last_scan_ts("opencode")?, Some(800));
    let source_only = ForgetExclusionSelector {
        source_path: Some("/fixture/blocked.db"),
        ..Default::default()
    };
    let preview = storage.reinclude_forget_exclusions(&source_only, true)?;
    assert_eq!(preview.exclusions_matched, 1);
    assert_eq!(preview.legacy_sources_matched, 1);
    assert!(preview.blocked_by_source_stamps.is_empty());
    let applied = storage.reinclude_forget_exclusions(&source_only, false)?;
    assert_eq!(applied.exclusions_removed, 1);
    assert_eq!(applied.legacy_sources_removed, 1);
    assert_eq!(applied.source_observations_removed, 1);
    assert_eq!(storage.get_last_scan_ts()?, Some(0));
    assert_eq!(storage.get_connector_last_scan_ts("opencode")?, Some(0));
    assert_eq!(storage.get_connector_last_scan_ts("codex")?, Some(0));
    assert_eq!(
        storage.read_mirror_scan_fingerprint("remote-fixture")?,
        None
    );
    assert_eq!(storage.forgotten_conversation_keys()?.len(), 1);
    assert!(storage.forgotten_source_stamps()?.is_empty());
    assert!(storage.forget_derived_rebuild_pending()?);
    Ok(())
}
