use super::*;
use crate::franken_sync::compat::{
    ConnectionExt as FrankenConnectionExt, RowExt as FrankenRowExt,
    TransactionExt as FrankenTransactionExt,
};
use crate::model::types::{Agent, AgentKind};
use crate::sources::provenance::LOCAL_SOURCE_ID;
use anyhow::{Context, Result};
use std::collections::HashSet;
use std::fs;
use tempfile::TempDir;

#[test]
fn reinclude_ledger_failure_rolls_back_both_exclusion_kinds() -> Result<()> {
    let directory = TempDir::new()?;
    let storage = FrankenStorage::open(&directory.path().join("reinclude-rollback.db"))?;
    let source = "/fixture/rollback.db";
    seed_forget_fixture(
        &storage,
        &forget_fixture_conversation(source, Some("session"), None, &[Some(10)]),
    )?;
    storage.forget_conversations_by_source_glob(source, false)?;
    storage.raw().execute_compat(
        "INSERT INTO forgotten_sources(source_path, forgotten_at_ms) VALUES (?1, 1)",
        fparams![source],
    )?;
    storage.record_filtered_source_completion(&SourceIngestLedgerEntry {
        key: "source_ingest_v1:guarded".into(),
        observation: serde_json::json!({
            "primary": {"path": source}, "dependencies": []
        })
        .to_string(),
    })?;
    storage.raw().execute(
        "CREATE TABLE reinclude_ledger_guard (
            ledger_key TEXT PRIMARY KEY REFERENCES meta(key) ON DELETE RESTRICT
         )",
    )?;
    storage.raw().execute(
        "INSERT INTO reinclude_ledger_guard(ledger_key) VALUES ('source_ingest_v1:guarded')",
    )?;
    let selector = ForgetExclusionSelector {
        source_path: Some(source),
        ..Default::default()
    };
    let before = storage.list_forget_exclusions(&selector)?;
    assert_eq!(before.exclusions.len(), 1);
    assert_eq!(before.legacy_sources.len(), 1);
    let preview = storage.reinclude_forget_exclusions(&selector, true)?;
    assert_eq!(preview.exclusions_matched, 1);
    assert_eq!(preview.legacy_sources_matched, 1);
    assert_eq!(preview.source_observations_matched, 1);
    assert!(
        storage
            .reinclude_forget_exclusions(&selector, false)
            .is_err()
    );
    assert_eq!(storage.list_forget_exclusions(&selector)?, before);
    assert_eq!(storage.source_ingest_ledger_entries()?.len(), 1);
    assert!(storage.forget_derived_rebuild_pending()?);
    storage
        .raw()
        .execute("DELETE FROM reinclude_ledger_guard")?;
    let applied = storage.reinclude_forget_exclusions(&selector, false)?;
    assert_eq!(applied.exclusions_removed, 1);
    assert_eq!(applied.legacy_sources_removed, 1);
    assert_eq!(applied.source_observations_removed, 1);
    assert!(storage.source_ingest_ledger_entries()?.is_empty());
    assert_eq!(storage.schema_version()?, CURRENT_SCHEMA_VERSION);
    assert!(storage.forget_derived_rebuild_pending()?);
    Ok(())
}

#[test]
fn reinclude_preview_samples_do_not_limit_the_selected_release() -> Result<()> {
    let directory = TempDir::new()?;
    let storage = FrankenStorage::open(&directory.path().join("reinclude-samples.db"))?;
    let mut tx = storage
        .raw()
        .transaction_with_behavior(crate::franken_sync::TransactionBehavior::Immediate)?;
    for index in 0..25 {
        tx.execute_compat(
            "INSERT INTO forgotten_conversations
                (source_id, agent_slug, source_path, external_id, forgotten_at_ms)
             VALUES ('local', 'opencode', '/fixture/shared.db', ?1, 1)",
            fparams![format!("session-{index:02}")],
        )?;
    }
    tx.commit()?;
    let selector = ForgetExclusionSelector {
        source_glob: Some("/fixture/*.db"),
        ..Default::default()
    };
    assert_eq!(
        storage.list_forget_exclusions(&selector)?.exclusions.len(),
        25
    );
    let preview = storage.reinclude_forget_exclusions(&selector, true)?;
    assert_eq!(preview.exclusions_matched, 25);
    assert_eq!(preview.sample_sessions.len(), 20);
    assert_eq!(preview.exclusions_removed, 0);
    assert_eq!(storage.forgotten_conversation_keys()?.len(), 25);
    let applied = storage.reinclude_forget_exclusions(&selector, false)?;
    assert_eq!(applied.exclusions_removed, 25);
    assert!(storage.forgotten_conversation_keys()?.is_empty());
    assert!(!storage.forget_derived_rebuild_pending()?);
    Ok(())
}

#[test]
fn forget_shared_store_records_session_identity_without_suppressing_siblings() -> Result<()> {
    let directory = TempDir::new()?;
    let path = directory.path().join("shared-store.db");
    let storage = FrankenStorage::open(&path)?;
    let source = "/fixture/opencode.db";
    let forgotten =
        forget_fixture_conversation(source, Some("same-id"), Some("/work/retired"), &[Some(10)]);
    seed_forget_fixture(&storage, &forgotten)?;
    let sibling_id = seed_forget_fixture(
        &storage,
        &forget_fixture_conversation(source, Some("kept-id"), Some("/work/current"), &[Some(20)]),
    )?;
    let mut remote =
        forget_fixture_conversation(source, Some("same-id"), Some("/work/remote"), &[Some(30)]);
    remote.source_id = "remote-fixture".into();
    remote.origin_host = Some("remote-fixture".into());
    let remote_id = seed_forget_fixture(&storage, &remote)?;
    let filter = ForgetConversationsFilter {
        workspace_glob: Some("/work/retired"),
        ..Default::default()
    };
    let preview = storage.forget_conversations(&filter, true)?;
    assert_eq!(preview.conversations_matched, 1);
    assert_eq!(
        preview.sample_sessions,
        vec![ForgetSessionSample {
            source_path: Some(source.into()),
            external_id: Some("same-id".into()),
            workspace: Some("/work/retired".into()),
            last_message_at: Some(10),
        }]
    );
    assert!(storage.forgotten_conversation_keys()?.is_empty());
    assert_eq!(
        storage
            .forget_conversations(&filter, false)?
            .conversations_deleted,
        1
    );
    assert_eq!(storage.fetch_messages(sibling_id)?.len(), 1);
    assert_eq!(storage.fetch_messages(remote_id)?.len(), 1);
    assert!(
        storage.forgotten_source_stamps()?.is_empty(),
        "a whole-file stamp would also suppress kept sessions"
    );
    let expected = HashSet::from([ForgottenConversationKey {
        source_id: LOCAL_SOURCE_ID.into(),
        agent_slug: "opencode".into(),
        source_path: source.into(),
        external_id: "same-id".into(),
    }]);
    assert_eq!(storage.forgotten_conversation_keys()?, expected);
    storage.clear_forgotten_sources(&[source.into()])?;
    storage.close()?;
    let reopened = FrankenStorage::open(&path)?;
    assert_eq!(reopened.forgotten_conversation_keys()?, expected);
    assert_eq!(reopened.total_conversation_count()?, 2);
    Ok(())
}

#[test]
fn forget_shared_store_detection_checks_virtual_paths_and_sqlite_headers() -> Result<()> {
    let directory = TempDir::new()?;
    let database = directory.path().join("sessions.db");
    let header_only = directory.path().join("opaque-store");
    let ordinary_directory = directory.path().join("project.db");
    fs::create_dir(&ordinary_directory)?;
    let ordinary_file = ordinary_directory.join("rollout.jsonl");
    fs::write(&database, b"SQLite format 3\0fixture")?;
    fs::write(&header_only, b"SQLite format 3\0fixture")?;
    fs::write(&ordinary_file, b"ordinary session transcript")?;

    for source in [
        database.clone(),
        database.join("devin-session"),
        header_only.clone(),
        header_only.join("virtual-session"),
        directory.path().join("missing.SQLITE3"),
    ] {
        assert!(
            source_path_is_shared_store(source.to_str().context("UTF-8 source path")?),
            "{} must use session identity",
            source.display()
        );
    }
    for source in [
        ordinary_directory,
        ordinary_file,
        directory.path().join("missing.jsonl"),
    ] {
        assert!(
            !source_path_is_shared_store(source.to_str().context("UTF-8 source path")?),
            "{} must retain ordinary file behavior",
            source.display()
        );
    }
    Ok(())
}

#[test]
fn forget_virtual_and_header_only_stores_use_permanent_session_keys() -> Result<()> {
    let directory = TempDir::new()?;
    let storage = FrankenStorage::open(&directory.path().join("archive.db"))?;
    let database = directory.path().join("sessions.db");
    let header_only = directory.path().join("opaque-store");
    let source_bytes = b"SQLite format 3\0source evidence";
    fs::write(&database, source_bytes)?;
    fs::write(&header_only, source_bytes)?;
    let sources = [
        (database.join("session-devin"), "devin"),
        (header_only.clone(), "opencode"),
        (header_only.join("session-virtual"), "devin"),
    ];
    for (index, (source, agent)) in sources.iter().enumerate() {
        let external_id = format!("session-{index}");
        let mut conversation = forget_fixture_conversation(
            source.to_str().context("UTF-8 source path")?,
            Some(&external_id),
            Some("/work/retired"),
            &[Some(10)],
        );
        conversation.agent_slug = (*agent).into();
        seed_forget_fixture(&storage, &conversation)?;
    }
    let filter = ForgetConversationsFilter {
        workspace_glob: Some("/work/retired"),
        ..Default::default()
    };
    let preview = storage.forget_conversations(&filter, true)?;
    assert_eq!(preview.conversations_matched, 3);
    assert_eq!(preview.sample_sessions.len(), 3);
    assert!(storage.forgotten_source_stamps()?.is_empty());
    assert!(storage.forgotten_conversation_keys()?.is_empty());
    let applied = storage.forget_conversations(&filter, false)?;
    assert_eq!(applied.conversations_deleted, 3);
    assert_eq!(applied.sample_sessions, preview.sample_sessions);
    assert!(storage.forgotten_source_stamps()?.is_empty());
    let keys = storage.forgotten_conversation_keys()?;
    assert_eq!(keys.len(), 3);
    for (index, (source, agent)) in sources.iter().enumerate() {
        assert!(keys.contains(&ForgottenConversationKey {
            source_id: LOCAL_SOURCE_ID.into(),
            agent_slug: (*agent).into(),
            source_path: source.to_string_lossy().into_owned(),
            external_id: format!("session-{index}"),
        }));
    }
    assert_eq!(fs::read(database)?, source_bytes);
    assert_eq!(fs::read(header_only)?, source_bytes);
    Ok(())
}

#[test]
fn forget_session_preview_is_bounded_without_truncating_counts() -> Result<()> {
    let directory = TempDir::new()?;
    let storage = FrankenStorage::open(&directory.path().join("preview.db"))?;
    for index in 0..21 {
        seed_forget_fixture(
            &storage,
            &forget_fixture_conversation(
                "/fixture/sessions.db",
                Some(&format!("session-{index}")),
                Some("/work/retired"),
                &[Some(index), Some(index + 1)],
            ),
        )?;
    }
    let preview = storage.forget_conversations(
        &ForgetConversationsFilter {
            workspace_glob: Some("/work/retired"),
            ..Default::default()
        },
        true,
    )?;
    assert_eq!(preview.conversations_matched, 21);
    assert_eq!(preview.messages_matched, 42);
    assert_eq!(preview.sample_sessions.len(), 20);
    assert_eq!(
        preview.sample_sessions[0].external_id.as_deref(),
        Some("session-0")
    );
    assert_eq!(preview.sample_sessions[0].last_message_at, Some(1));
    assert_eq!(
        preview.sample_sessions[19].external_id.as_deref(),
        Some("session-19")
    );
    assert_eq!(preview.sample_sessions[19].last_message_at, Some(20));
    assert_eq!(storage.total_conversation_count()?, 21);
    assert!(storage.forgotten_conversation_keys()?.is_empty());
    Ok(())
}

#[test]
fn forget_recognizes_shared_json_even_after_only_one_session_remains() -> Result<()> {
    let directory = TempDir::new()?;
    let storage = FrankenStorage::open(&directory.path().join("shared-json.db"))?;
    let source = "/fixture/conversations.json";
    for (session, workspace) in [("one", "/work/one"), ("two", "/work/two")] {
        seed_forget_fixture(
            &storage,
            &forget_fixture_conversation(source, Some(session), Some(workspace), &[Some(10)]),
        )?;
    }
    storage.forget_conversations(
        &ForgetConversationsFilter {
            workspace_glob: Some("/work/one"),
            ..Default::default()
        },
        false,
    )?;
    assert_eq!(storage.forgotten_conversation_keys()?.len(), 1);
    assert_eq!(storage.total_conversation_count()?, 1);
    storage.forget_conversations_by_source_glob(source, false)?;
    assert_eq!(storage.forgotten_conversation_keys()?.len(), 2);
    assert!(storage.forgotten_source_stamps()?.is_empty());
    assert_eq!(storage.total_conversation_count()?, 0);
    Ok(())
}

#[test]
fn forget_partial_shared_source_without_identity_is_refused_atomically() -> Result<()> {
    let directory = TempDir::new()?;
    let storage = FrankenStorage::open(&directory.path().join("missing-identity.db"))?;
    let source = "/fixture/shared.sqlite";
    seed_forget_fixture(
        &storage,
        &forget_fixture_conversation(source, None, Some("/work/retired"), &[Some(10)]),
    )?;
    seed_forget_fixture(
        &storage,
        &forget_fixture_conversation(source, Some("kept"), Some("/work/current"), &[Some(20)]),
    )?;
    let filter = ForgetConversationsFilter {
        workspace_glob: Some("/work/retired"),
        ..Default::default()
    };
    for dry_run in [true, false] {
        let error = storage.forget_conversations(&filter, dry_run).unwrap_err();
        assert!(error.to_string().contains("no stable external ID"));
        assert_eq!(storage.total_conversation_count()?, 2);
        assert_eq!(storage.total_message_count()?, 2);
        assert!(storage.forgotten_source_stamps()?.is_empty());
        assert!(storage.forgotten_conversation_keys()?.is_empty());
    }
    assert_eq!(
        storage
            .forget_conversations_by_source_glob(source, false)?
            .conversations_deleted,
        2
    );
    assert_eq!(storage.forgotten_source_stamps()?.len(), 1);
    assert_eq!(storage.forgotten_conversation_keys()?.len(), 1);
    Ok(())
}

#[test]
fn forget_selection_continues_past_an_unmatched_page() -> Result<()> {
    let directory = TempDir::new()?;
    let storage = FrankenStorage::open(&directory.path().join("paged-forget.db"))?;
    let agent_id = storage.ensure_agent(&Agent {
        id: None,
        slug: "fixture".into(),
        name: "Fixture".into(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    let mut tx = storage
        .raw()
        .transaction_with_behavior(crate::franken_sync::TransactionBehavior::Immediate)?;
    for id in 1..=257_i64 {
        let source = if id == 257 {
            "/fixture/selected.jsonl"
        } else {
            "/fixture/kept.jsonl"
        };
        tx.execute_compat(
            "INSERT INTO conversations(id, agent_id, source_path, external_id) VALUES(?1, ?2, ?3, ?4)",
            fparams![id, agent_id, source, id.to_string()],
        )?;
        tx.execute_compat(
            "INSERT INTO messages(conversation_id, idx, role, created_at, content) VALUES(?1, 0, 'user', 1, 'retained fixture')",
            fparams![id],
        )?;
    }
    tx.commit()?;
    let preview = storage.forget_conversations_by_source_glob("**/selected.jsonl", true)?;
    assert_eq!(preview.conversations_matched, 1);
    assert_eq!(preview.messages_matched, 1);
    assert_eq!(storage.total_conversation_count()?, 257);
    let applied = storage.forget_conversations_by_source_glob("**/selected.jsonl", false)?;
    assert_eq!(applied.conversations_deleted, 1);
    assert_eq!(storage.total_conversation_count()?, 256);
    Ok(())
}

#[test]
fn forget_filtered_source_completion_is_durable_and_rejects_foreign_keys() -> Result<()> {
    let directory = TempDir::new()?;
    let path = directory.path().join("filtered-source.db");
    let storage = FrankenStorage::open(&path)?;
    let completion = SourceIngestLedgerEntry {
        key: "source_ingest_v1:excluded".into(),
        observation: "observed and intentionally excluded".into(),
    };
    assert!(
        storage
            .insert_conversations_batched_with_source_completion(&[], &completion)
            .is_err()
    );
    storage.record_filtered_source_completion(&completion)?;
    for key in ["schema_version", "source_ingest_v1:"] {
        assert!(
            storage
                .record_filtered_source_completion(&SourceIngestLedgerEntry {
                    key: key.into(),
                    observation: "invalid".into(),
                })
                .is_err()
        );
    }
    assert_eq!(storage.schema_version()?, CURRENT_SCHEMA_VERSION);
    storage.close()?;
    let reopened = FrankenStorage::open(&path)?;
    assert_eq!(
        reopened
            .source_ingest_ledger_entries()?
            .get(&completion.key),
        Some(&completion.observation)
    );
    assert_eq!(reopened.total_conversation_count()?, 0);
    Ok(())
}

/// Regression for cass#202: a `Connection` dropped mid-transaction can
/// leave child rows persisted without a matching parent. The next indexer
/// pass then trips `FOREIGN KEY constraint failed` on every write, the
/// session never gets marked indexed, and the pending backlog grows
/// without bound. `cleanup_orphan_fk_rows` is the indexer-startup
/// self-heal that breaks the cycle.
#[test]
fn cleanup_orphan_fk_rows_removes_orphans_and_is_noop_on_clean_db() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("orphan_fk_self_heal.db");
    let storage = FrankenStorage::open(&db_path).unwrap();

    // Plant orphan rows directly: rows whose FK parent does not exist.
    // FK enforcement is temporarily off so the planted rows can land.
    storage.raw().execute("PRAGMA foreign_keys = OFF").unwrap();

    // Seed a real conversation so a subset of children DO have valid
    // parents — we want the cleanup to be precise, not a table-flush.
    storage
        .raw()
        .execute_compat(
            "INSERT INTO agents(id, slug, name, kind, created_at, updated_at) \
                 VALUES(1, 'test-agent', 'Test Agent', 'cli', 0, 0)",
            fparams![],
        )
        .unwrap();
    storage
        .raw()
        .execute_compat(
            "INSERT INTO conversations(id, agent_id, source_id, source_path, started_at) \
             VALUES(1, 1, 'local', '/tmp/real.jsonl', 0)",
            fparams![],
        )
        .unwrap();
    storage
        .raw()
        .execute_compat(
            "INSERT INTO messages(id, conversation_id, idx, role, content) \
             VALUES(1, 1, 0, 'user', 'real message')",
            fparams![],
        )
        .unwrap();

    // Plant orphan messages referencing conversation_id=99999 (does not exist)
    // and conversation_id=0 (the specific shape reported in #202). Distinct
    // (conversation_id, idx) pairs are required by the UNIQUE constraint.
    for (mid, cid, idx) in [(101_i64, 99_999_i64, 0_i64), (102, 0, 0), (103, 0, 1)] {
        storage
            .raw()
            .execute_compat(
                "INSERT INTO messages(id, conversation_id, idx, role, content) \
                 VALUES(?1, ?2, ?3, 'user', 'orphan message')",
                fparams![mid, cid, idx],
            )
            .unwrap();
    }

    // Rows below are not directly orphaned because their immediate
    // `messages` parent exists, but that parent is itself orphaned. The
    // cleanup deletes them explicitly before deleting orphan messages so the
    // FK cascade engine does not have to run one delete program per orphan.
    for message_id in [1_i64, 101_i64, 102_i64] {
        storage
            .raw()
            .execute_compat(
                "INSERT INTO message_metrics(
                     message_id, created_at_ms, hour_id, day_id, agent_slug,
                     role, content_chars, content_tokens_est
                 ) VALUES(?1, 0, 0, 0, 'test-agent', 'user', 13, 2)",
                fparams![message_id],
            )
            .unwrap();
        storage
            .raw()
            .execute_compat(
                "INSERT INTO token_usage(
                     message_id, conversation_id, agent_id, timestamp_ms, day_id,
                     role, content_chars
                 ) VALUES(?1, 1, 1, 0, 0, 'user', 13)",
                fparams![message_id],
            )
            .unwrap();
    }

    // Plant a directly-orphan snippet — message_id=99999 does not exist
    // anywhere, so this exercises the snippets DELETE path rather than
    // riding on the cascade from the orphan-message DELETE.
    storage
        .raw()
        .execute_compat(
            "INSERT INTO snippets(message_id, file_path, start_line, end_line, language, snippet_text) \
             VALUES(99999, '/tmp/orphan-snippet.rs', 1, 2, 'rust', 'fn main() {}')",
            fparams![],
        )
        .unwrap();

    storage.raw().execute("PRAGMA foreign_keys = ON").unwrap();

    // Sanity: the planted orphans are visible.
    let messages_before: i64 = storage
        .raw()
        .query_row_map("SELECT COUNT(*) FROM messages", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(messages_before, 4); // 1 real + 3 orphans
    let snippets_before: i64 = storage
        .raw()
        .query_row_map("SELECT COUNT(*) FROM snippets", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(snippets_before, 1);
    let metrics_before: i64 = storage
        .raw()
        .query_row_map("SELECT COUNT(*) FROM message_metrics", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(metrics_before, 3);
    let token_usage_before: i64 = storage
        .raw()
        .query_row_map("SELECT COUNT(*) FROM token_usage", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(token_usage_before, 3);

    // Run the self-heal.
    let report = storage.cleanup_orphan_fk_rows().unwrap();

    // 3 orphan messages + 1 directly-orphan snippet = 4 primary orphans
    // reported. Dependent message_metrics/token_usage rows for orphan
    // messages are pruned too, but they are not double-counted because the
    // orphan message is the root row that made them invalid.
    let messages_after: i64 = storage
        .raw()
        .query_row_map("SELECT COUNT(*) FROM messages", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(messages_after, 1, "real message must be preserved");
    let snippets_after: i64 = storage
        .raw()
        .query_row_map("SELECT COUNT(*) FROM snippets", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(snippets_after, 0);
    let metrics_after: i64 = storage
        .raw()
        .query_row_map("SELECT COUNT(*) FROM message_metrics", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(metrics_after, 1, "real message metric must be preserved");
    let token_usage_after: i64 = storage
        .raw()
        .query_row_map("SELECT COUNT(*) FROM token_usage", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(token_usage_after, 1, "real token row must be preserved");

    assert_eq!(report.total, 4, "report total: {:?}", report);
    let messages_count = report
        .per_table
        .iter()
        .find(|(t, _)| *t == "messages")
        .map(|(_, c)| *c);
    assert_eq!(messages_count, Some(3));
    let snippets_count = report
        .per_table
        .iter()
        .find(|(t, _)| *t == "snippets")
        .map(|(_, c)| *c);
    assert_eq!(snippets_count, Some(1));

    // Second invocation on a now-clean DB must be a no-op.
    let second = storage.cleanup_orphan_fk_rows().unwrap();
    assert_eq!(second.total, 0);
    assert!(second.per_table.is_empty());
}

#[test]
fn cleanup_orphan_fk_rows_handles_more_than_one_delete_chunk() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("orphan_fk_chunked_self_heal.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    let orphan_count = ORPHAN_FK_ID_CHUNK_SIZE + 3;

    storage.raw().execute("PRAGMA foreign_keys = OFF").unwrap();
    {
        let mut tx = storage
            .raw()
            .transaction_with_behavior(crate::franken_sync::TransactionBehavior::Immediate)
            .unwrap();
        for idx in 0..orphan_count {
            let message_id = 10_000_i64 + i64::try_from(idx).unwrap();
            let conversation_id = 20_000_i64 + i64::try_from(idx).unwrap();
            tx.execute_compat(
                "INSERT INTO messages(id, conversation_id, idx, role, content) \
                     VALUES(?1, ?2, 0, 'user', 'orphan message')",
                fparams![message_id, conversation_id],
            )
            .unwrap();
            tx.execute_compat(
                "INSERT INTO message_metrics(
                     message_id, created_at_ms, hour_id, day_id, agent_slug,
                     role, content_chars, content_tokens_est
                 ) VALUES(?1, 0, 0, 0, 'test-agent', 'user', 14, 2)",
                fparams![message_id],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
    storage.raw().execute("PRAGMA foreign_keys = ON").unwrap();

    let report = storage.cleanup_orphan_fk_rows().unwrap();

    assert_eq!(report.total, i64::try_from(orphan_count).unwrap());
    let messages_count = report
        .per_table
        .iter()
        .find(|(table, _)| *table == "messages")
        .map(|(_, count)| *count);
    assert_eq!(messages_count, Some(i64::try_from(orphan_count).unwrap()));
    let messages_after: i64 = storage
        .raw()
        .query_row_map("SELECT COUNT(*) FROM messages", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(messages_after, 0);
    let metrics_after: i64 = storage
        .raw()
        .query_row_map("SELECT COUNT(*) FROM message_metrics", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(metrics_after, 0);
}

#[test]
fn cleanup_orphan_fk_rows_pages_direct_child_orphans() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("direct_orphan_fk_paged_self_heal.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    let orphan_count = (ORPHAN_FK_ID_CHUNK_SIZE * 2) + 5;

    storage.raw().execute("PRAGMA foreign_keys = OFF").unwrap();
    {
        let mut tx = storage
            .raw()
            .transaction_with_behavior(crate::franken_sync::TransactionBehavior::Immediate)
            .unwrap();
        for idx in 0..orphan_count {
            let message_id = 50_000_i64 + i64::try_from(idx).unwrap();
            tx.execute_compat(
                "INSERT INTO message_metrics(
                     message_id, created_at_ms, hour_id, day_id, agent_slug,
                     role, content_chars, content_tokens_est
                 ) VALUES(?1, 0, 0, 0, 'test-agent', 'user', 21, 3)",
                fparams![message_id],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
    storage.raw().execute("PRAGMA foreign_keys = ON").unwrap();

    let report = storage.cleanup_orphan_fk_rows().unwrap();

    assert_eq!(report.total, i64::try_from(orphan_count).unwrap());
    let metrics_count = report
        .per_table
        .iter()
        .filter(|(table, _)| *table == "message_metrics")
        .map(|(_, count)| *count)
        .sum::<i64>();
    assert_eq!(metrics_count, i64::try_from(orphan_count).unwrap());
    assert_eq!(
        report
            .per_table
            .iter()
            .filter(|(table, _)| *table == "message_metrics")
            .count(),
        1,
        "paged cleanup should aggregate report entries by table: {report:?}"
    );
    let metrics_after: i64 = storage
        .raw()
        .query_row_map("SELECT COUNT(*) FROM message_metrics", fparams![], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(metrics_after, 0);
}
