use super::*;
use crate::franken_sync::SqliteValue;
use crate::franken_sync::compat::{
    ConnectionExt as FrankenConnectionExt, OptionalExtension as FrankenOptionalExtension,
    RowExt as FrankenRowExt,
};
use crate::sources::provenance::LOCAL_SOURCE_ID;
use anyhow::{Context, Result};
use serial_test::serial;
use tempfile::TempDir;

/// GH #424: each message-metrics chunk and its cursor must commit as one
/// unit. An interrupted OMP migration resumes after the last committed
/// message without replaying additive usage rollups, while an unrelated
/// analytics rebuild invalidates the cursor and safely starts over.
#[test]
#[serial]
fn gh424_legacy_omp_payload_budget_cancellation_and_resume() -> anyhow::Result<()> {
    #[derive(Debug, thiserror::Error)]
    #[error("planted analytics cancellation")]
    struct Stop;

    let _lexical = set_env_var("CASS_DEFER_LEXICAL_UPDATES", "1");
    let dir = TempDir::new()?;
    let storage = FrankenStorage::open(&dir.path().join("bounded-analytics.db"))?;
    let agent = gh423_agent(&storage);
    let mut conv = gh423_storage_snapshot();
    // Two individually valid payloads exceed the combined read budget.
    // The third message is small and must survive the short first page.
    conv.messages[0].content = "a".repeat(5 * 1024 * 1024);
    conv.messages[1].content = "b".repeat(5 * 1024 * 1024);
    storage.insert_conversations_batched_with_analytics(&[(agent, None, &conv)], false)?;
    let context = "gh424-byte-budget-control";
    let cancel = || -> Result<()> { Err(Stop.into()) };
    let error = storage
        .rebuild_analytics_since_with_chunk_size_and_progress(
            None,
            128,
            None,
            None,
            Some(context),
            Some(&cancel),
        )
        .expect_err("cancellation before reset must refuse mutation");
    assert!(error.downcast_ref::<Stop>().is_some());
    let count = || -> Result<i64> {
        Ok(storage.raw().query_row_map(
            "SELECT COUNT(*) FROM message_metrics",
            fparams![],
            |row| row.get_typed(0),
        )?)
    };
    assert_eq!(count()?, 3);

    let stop = std::cell::Cell::new(false);
    let committed = std::cell::RefCell::new(Vec::new());
    let progress = |processed: i64, _total: i64| {
        if processed > 0 {
            committed.borrow_mut().push(processed);
            stop.set(true);
        }
    };
    let control = || -> Result<()> { if stop.get() { Err(Stop.into()) } else { Ok(()) } };
    let error = storage
        .rebuild_analytics_since_with_chunk_size_and_progress(
            None,
            128,
            Some(&progress),
            None,
            Some(context),
            Some(&control),
        )
        .context("legacy repair caller")
        .expect_err("stop after committed byte-bounded page");
    assert!(error.downcast_ref::<Stop>().is_some());
    assert_eq!(
        *committed.borrow(),
        vec![1],
        "byte limit applies before raw payload hydration"
    );
    assert_eq!(count()?, 1);
    let cursor: String = storage.raw().query_row_map(
        "SELECT value FROM meta WHERE key = ?1",
        fparams![LEGACY_OMP_ANALYTICS_CURSOR_PROCESSED_META_KEY],
        |row| row.get_typed(0),
    )?;
    assert_eq!(cursor, "1");
    stop.set(false);
    storage.rebuild_analytics_since_with_chunk_size_and_progress(
        None,
        128,
        None,
        None,
        Some(context),
        Some(&control),
    )?;
    assert_eq!(
        count()?,
        3,
        "short byte page must not hide the remaining tail"
    );
    let totals: (i64, i64) = storage.raw().query_row_map(
        "SELECT SUM(content_chars), SUM(tool_call_count) FROM message_metrics",
        fparams![],
        |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
    )?;
    assert_eq!(
        totals,
        (
            conv.messages
                .iter()
                .map(|message| message.content.len() as i64)
                .sum(),
            3
        )
    );
    for table in ["usage_hourly", "usage_daily", "usage_models_daily"] {
        let messages: i64 = storage.raw().query_row_map(
            &format!("SELECT SUM(message_count) FROM {table}"),
            fparams![],
            |row| row.get_typed(0),
        )?;
        assert_eq!(messages, 3, "resume must not double-count {table}");
    }
    Ok(())
}

#[test]
#[serial]
fn gh424_legacy_omp_cancellation_rolls_back_inflight_metrics_and_cursor() -> anyhow::Result<()> {
    #[derive(Debug, thiserror::Error)]
    #[error("planted in-transaction analytics cancellation")]
    struct Stop;

    let _lexical = set_env_var("CASS_DEFER_LEXICAL_UPDATES", "1");
    let dir = TempDir::new()?;
    let storage = FrankenStorage::open(&dir.path().join("cancel-analytics-write.db"))?;
    let agent = gh423_agent(&storage);
    let conv = gh423_storage_snapshot();
    storage.insert_conversations_batched_with_analytics(&[(agent, None, &conv)], false)?;
    let context = "gh424-write-cancellation";
    let control = || -> Result<()> {
        let count: i64 = storage.raw().query_row_map(
            "SELECT COUNT(*) FROM message_metrics",
            fparams![],
            |row| row.get_typed(0),
        )?;
        if count == 1 { Err(Stop.into()) } else { Ok(()) }
    };
    let error = storage
        .rebuild_analytics_since_with_chunk_size_and_progress(
            None,
            128,
            None,
            None,
            Some(context),
            Some(&control),
        )
        .expect_err("stop after a real metric insert must roll back its chunk");
    assert!(error.downcast_ref::<Stop>().is_some());
    for table in [
        "message_metrics",
        "usage_hourly",
        "usage_daily",
        "usage_models_daily",
    ] {
        let count: i64 = storage.raw().query_row_map(
            &format!("SELECT COUNT(*) FROM {table}"),
            fparams![],
            |row| row.get_typed(0),
        )?;
        assert_eq!(count, 0, "cancelled transaction must not publish {table}");
    }
    let cursor: String = storage.raw().query_row_map(
        "SELECT value FROM meta WHERE key = ?1",
        fparams![LEGACY_OMP_ANALYTICS_CURSOR_PROCESSED_META_KEY],
        |row| row.get_typed(0),
    )?;
    assert_eq!(cursor, "0");
    storage.rebuild_analytics_since_with_chunk_size_and_progress(
        None,
        128,
        None,
        None,
        Some(context),
        None,
    )?;
    let count: i64 =
        storage
            .raw()
            .query_row_map("SELECT COUNT(*) FROM message_metrics", fparams![], |row| {
                row.get_typed(0)
            })?;
    assert_eq!(count, 3);
    Ok(())
}

#[test]
fn gh424_legacy_omp_bounded_reset_reopens_at_each_boundary() -> anyhow::Result<()> {
    #[derive(Debug, thiserror::Error)]
    #[error("planted committed reset interruption")]
    struct Stop;

    fn rows(storage: &FrankenStorage, sql: &str) -> Result<Vec<Vec<SqliteValue>>> {
        Ok(storage
            .raw()
            .query(sql)?
            .iter()
            .map(|row| row.values().to_vec())
            .collect())
    }
    fn rollups(storage: &FrankenStorage) -> Result<Vec<Vec<Vec<SqliteValue>>>> {
        ["usage_hourly", "usage_daily", "usage_models_daily"]
            .into_iter()
            .map(|table| {
                rows(
                    storage,
                    &format!(
                        "SELECT agent_slug, workspace_id, source_id, message_count,
                        user_message_count, assistant_message_count, tool_call_count,
                        plan_message_count, api_coverage_message_count,
                        content_tokens_est_total, api_tokens_total
                 FROM {table} ORDER BY rowid"
                    ),
                )
            })
            .collect()
    }
    let dir = TempDir::new()?;
    let path = dir.path().join("bounded-reset.db");
    let storage = FrankenStorage::open(&path)?;
    let agent = gh423_agent(&storage);
    let mut conv = gh423_storage_snapshot();
    let template = conv.messages[0].clone();
    conv.messages = (0..130)
        .map(|idx| {
            let mut message = template.clone();
            message.idx = idx;
            message.created_at = Some(1_767_225_622_759 + idx * 86_400_000);
            message.extra_json["id"] = serde_json::json!(format!("reset-{idx}"));
            message.extra_json["codebuff_message_id"] = serde_json::json!(format!("reset-{idx}"));
            message
        })
        .collect();
    storage.insert_conversations_batched_with_analytics(&[(agent, None, &conv)], false)?;
    storage.rebuild_analytics_since_with_chunk_size(None, 128)?;
    let canonical = rows(&storage, "SELECT * FROM messages ORDER BY id")?;
    let metrics = rows(
        &storage,
        "SELECT * FROM message_metrics ORDER BY message_id",
    )?;
    let usage = rollups(&storage)?;
    assert_eq!(canonical.len(), 130);
    assert_eq!(metrics.len(), 130);
    assert!(usage.iter().all(|table| table.len() == 130));
    let context = "gh424-bounded-reset";
    for (key, value) in [
        (
            LEGACY_OMP_RECLASSIFICATION_META_KEY,
            format!("analytics_pending:{context}"),
        ),
        (LEGACY_OMP_ANALYTICS_REBUILT_META_KEY, context.to_string()),
        (
            LEGACY_OMP_ANALYTICS_CURSOR_CONTEXT_META_KEY,
            "obsolete-context".into(),
        ),
        (LEGACY_OMP_ANALYTICS_CURSOR_LAST_ID_META_KEY, "777".into()),
        (LEGACY_OMP_ANALYTICS_CURSOR_PROCESSED_META_KEY, "777".into()),
    ] {
        storage.raw().execute_compat(
            "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
            fparams![key, value],
        )?;
    }
    drop(storage);

    // Each boundary is observed only by a heartbeat outside the write
    // transaction, then the actual connection is closed and reopened.
    for boundary in 0..3 {
        let storage = FrankenStorage::open(&path)?;
        let stop = std::cell::Cell::new(false);
        let unknown_progress = std::cell::Cell::new(false);
        let progress = |current, total| {
            if current == 0 && total == 0 {
                unknown_progress.set(true);
            }
        };
        let heartbeat = || {
            let count: i64 = storage
                .raw()
                .query_row_map("SELECT COUNT(*) FROM message_metrics", fparams![], |row| {
                    row.get_typed(0)
                })
                .expect("read committed reset metrics");
            let hourly: i64 = storage
                .raw()
                .query_row_map("SELECT COUNT(*) FROM usage_hourly", fparams![], |row| {
                    row.get_typed(0)
                })
                .expect("read committed reset rollup");
            let cursor: Option<String> = storage
                .raw()
                .query_row_map(
                    "SELECT value FROM meta WHERE key = ?1",
                    fparams![LEGACY_OMP_ANALYTICS_CURSOR_LAST_ID_META_KEY],
                    |row| row.get_typed(0),
                )
                .optional()
                .expect("read committed reset cursor");
            if match boundary {
                0 => count == 2 && hourly == 130,
                1 => count == 0 && hourly == 130,
                _ => count == 0 && hourly == 0 && cursor.as_deref() == Some("start"),
            } {
                stop.set(true);
            }
        };
        let control = || -> Result<()> { if stop.get() { Err(Stop.into()) } else { Ok(()) } };
        let error = storage
            .rebuild_analytics_since_with_chunk_size_and_progress(
                None,
                128,
                Some(&progress),
                Some(&heartbeat),
                Some(context),
                Some(&control),
            )
            .expect_err("committed reset boundary must interrupt the real rebuild");
        assert!(error.downcast_ref::<Stop>().is_some());
        assert!(unknown_progress.get());
        assert_eq!(
            rows(&storage, "SELECT * FROM messages ORDER BY id")?,
            canonical
        );
        let marker: String = storage.raw().query_row_map(
            "SELECT value FROM meta WHERE key = ?1",
            fparams![LEGACY_OMP_RECLASSIFICATION_META_KEY],
            |row| row.get_typed(0),
        )?;
        assert_eq!(marker, format!("analytics_pending:{context}"));
        let complete: i64 = storage.raw().query_row_map(
            "SELECT COUNT(*) FROM meta WHERE key = ?1",
            fparams![LEGACY_OMP_ANALYTICS_REBUILT_META_KEY],
            |row| row.get_typed(0),
        )?;
        assert_eq!(
            complete, 0,
            "partially cleared analytics must lose completion authority"
        );
        for key in [
            LEGACY_OMP_ANALYTICS_CURSOR_CONTEXT_META_KEY,
            LEGACY_OMP_ANALYTICS_CURSOR_LAST_ID_META_KEY,
            LEGACY_OMP_ANALYTICS_CURSOR_PROCESSED_META_KEY,
        ] {
            let value: Option<String> = storage
                .raw()
                .query_row_map(
                    "SELECT value FROM meta WHERE key = ?1",
                    fparams![key],
                    |row| row.get_typed(0),
                )
                .optional()?;
            if boundary < 2 {
                assert_eq!(value, None, "clearing must not retain cursor {key}");
            } else {
                let expected = if key == LEGACY_OMP_ANALYTICS_CURSOR_CONTEXT_META_KEY {
                    context
                } else if key == LEGACY_OMP_ANALYTICS_CURSOR_LAST_ID_META_KEY {
                    "start"
                } else {
                    "0"
                };
                assert_eq!(value.as_deref(), Some(expected));
            }
        }
    }
    let storage = FrankenStorage::open(&path)?;
    let message_progress = std::cell::RefCell::new(Vec::new());
    let progress = |current, total| message_progress.borrow_mut().push((current, total));
    storage.rebuild_analytics_since_with_chunk_size_and_progress(
        None,
        128,
        Some(&progress),
        None,
        Some(context),
        None,
    )?;
    assert!(message_progress.borrow().contains(&(0, 130)));
    assert!(message_progress.borrow().contains(&(130, 130)));
    assert_eq!(
        rows(&storage, "SELECT * FROM messages ORDER BY id")?,
        canonical
    );
    assert_eq!(
        rows(
            &storage,
            "SELECT * FROM message_metrics ORDER BY message_id"
        )?,
        metrics
    );
    assert_eq!(rollups(&storage)?, usage);
    storage.rebuild_analytics_since_with_chunk_size_and_progress(
        None,
        128,
        None,
        None,
        Some(context),
        None,
    )?;
    assert_eq!(
        rows(
            &storage,
            "SELECT * FROM message_metrics ORDER BY message_id"
        )?,
        metrics
    );
    assert_eq!(
        rollups(&storage)?,
        usage,
        "completed cursor replay must not double-count"
    );
    Ok(())
}

#[test]
fn gh424_legacy_omp_reset_without_rowid_preserves_transactional_fallback() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let storage = FrankenStorage::open(&dir.path().join("alternate-layout-reset.db"))?;
    let agent = gh423_agent(&storage);
    storage.insert_conversations_batched_with_analytics(
        &[(agent, None, &gh423_storage_snapshot())],
        false,
    )?;
    // Retain the original table and all its actual columns/constraints;
    // the alternate layout changes only the presence of a hidden rowid.
    let original_schema: String = storage.raw().query_row_map(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'usage_models_daily'",
        fparams![],
        |row| row.get_typed(0),
    )?;
    storage
        .raw()
        .execute("ALTER TABLE usage_models_daily RENAME TO retained_usage_models_daily")?;
    storage.raw().execute(&format!(
        "{} WITHOUT ROWID",
        original_schema.trim().trim_end_matches(';')
    ))?;
    storage
        .raw()
        .execute("INSERT INTO usage_models_daily SELECT * FROM retained_usage_models_daily")?;
    let alternate_schema: String = storage.raw().query_row_map(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'usage_models_daily'",
        fparams![],
        |row| row.get_typed(0),
    )?;
    let without_rowid: i64 = storage.raw().query_row_map(
        "PRAGMA table_list('usage_models_daily')",
        fparams![],
        |row| row.get_typed(4),
    )?;
    assert_eq!(
        without_rowid, 1,
        "the warned fallback must use a real WITHOUT ROWID layout"
    );
    let context = "gh424-alternate-layout";
    let control = || -> Result<()> {
        let metrics: i64 = storage.raw().query_row_map(
            "SELECT COUNT(*) FROM message_metrics",
            fparams![],
            |row| row.get_typed(0),
        )?;
        anyhow::ensure!(metrics != 0, "planted fallback transaction stop");
        Ok(())
    };
    let error = storage
        .reset_legacy_omp_analytics(context, None, None, Some(&control))
        .expect_err("fallback cancellation must roll back the table reset");
    assert!(
        error
            .to_string()
            .contains("planted fallback transaction stop")
    );
    let metrics: i64 =
        storage
            .raw()
            .query_row_map("SELECT COUNT(*) FROM message_metrics", fparams![], |row| {
                row.get_typed(0)
            })?;
    assert_eq!(metrics, 3);
    let alternate: i64 = storage.raw().query_row_map(
        "SELECT message_count FROM usage_models_daily",
        fparams![],
        |row| row.get_typed(0),
    )?;
    assert_eq!(alternate, 3);
    let cursors: i64 = storage.raw().query_row_map(
        "SELECT COUNT(*) FROM meta WHERE key = ?1",
        fparams![LEGACY_OMP_ANALYTICS_CURSOR_CONTEXT_META_KEY],
        |row| row.get_typed(0),
    )?;
    assert_eq!(cursors, 0);
    storage.reset_legacy_omp_analytics(context, None, None, None)?;
    for table in [
        "message_metrics",
        "usage_hourly",
        "usage_daily",
        "usage_models_daily",
    ] {
        let remaining: i64 = storage.raw().query_row_map(
            &format!("SELECT COUNT(*) FROM {table}"),
            fparams![],
            |row| row.get_typed(0),
        )?;
        assert_eq!(remaining, 0, "fallback must clear {table}");
    }
    let cursor: String = storage.raw().query_row_map(
        "SELECT value FROM meta WHERE key = ?1",
        fparams![LEGACY_OMP_ANALYTICS_CURSOR_LAST_ID_META_KEY],
        |row| row.get_typed(0),
    )?;
    assert_eq!(cursor, "start");
    let canonical: i64 =
        storage
            .raw()
            .query_row_map("SELECT COUNT(*) FROM messages", fparams![], |row| {
                row.get_typed(0)
            })?;
    assert_eq!(canonical, 3);
    let retained_schema: String = storage.raw().query_row_map(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'usage_models_daily'",
        fparams![],
        |row| row.get_typed(0),
    )?;
    assert_eq!(
        retained_schema, alternate_schema,
        "reset must preserve the alternate schema"
    );
    Ok(())
}

#[test]
fn gh424_stale_initial_analytics_cursor_rebuilds_without_double_counting() -> anyhow::Result<()> {
    const TABLES: [&str; 4] = [
        "message_metrics",
        "usage_hourly",
        "usage_daily",
        "usage_models_daily",
    ];
    fn snapshot(storage: &FrankenStorage, table: &str) -> Result<Vec<Vec<SqliteValue>>> {
        // Compare every stored value except the rebuild's wall-clock stamp.
        let columns: Vec<String> = storage.raw().query_map_collect(
            &format!("PRAGMA table_info({table})"),
            fparams![],
            |row| row.get_typed(1),
        )?;
        let projection = columns
            .into_iter()
            .filter(|column| column != "last_updated")
            .collect::<Vec<_>>()
            .join(", ");
        Ok(storage
            .raw()
            .query(&format!("SELECT {projection} FROM {table} ORDER BY rowid"))?
            .iter()
            .map(|row| row.values().to_vec())
            .collect())
    }

    let dir = TempDir::new()?;
    let storage = FrankenStorage::open(&dir.path().join("stale-start.db"))?;
    let agent = gh423_agent(&storage);
    let conversation = gh423_storage_snapshot();
    storage.insert_conversations_batched_with_analytics(&[(agent, None, &conversation)], false)?;
    storage.rebuild_analytics_since_with_chunk_size(None, 128)?;
    let canonical = snapshot(&storage, "messages")?;
    assert!(!canonical.is_empty());
    let expected = TABLES
        .iter()
        .map(|table| snapshot(&storage, table))
        .collect::<Result<Vec<_>>>()?;
    assert!(expected.iter().all(|rows| !rows.is_empty()));

    // Each table alone must invalidate a stale start cursor, including
    // rollup-only states with no metrics. The final case is a genuine
    // empty start cursor and must resume without resetting again.
    for retained in TABLES.into_iter().map(Some).chain([None]) {
        storage.rebuild_analytics_since_with_chunk_size(None, 128)?;
        let context = "gh424-stale-initial-cursor";
        let mut tx = storage
            .raw()
            .transaction_with_behavior(crate::franken_sync::TransactionBehavior::Immediate)?;
        for table in TABLES {
            if retained != Some(table) {
                tx.execute(&format!("DELETE FROM {table}"))?;
            }
        }
        for (key, value) in [
            (LEGACY_OMP_ANALYTICS_CURSOR_CONTEXT_META_KEY, context),
            (LEGACY_OMP_ANALYTICS_CURSOR_LAST_ID_META_KEY, "start"),
            (LEGACY_OMP_ANALYTICS_CURSOR_PROCESSED_META_KEY, "0"),
        ] {
            tx.execute_compat(
                "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
                fparams![key, value],
            )?;
        }
        tx.commit()?;
        let reset_observed = std::cell::Cell::new(false);
        let progress = |current, total| {
            if current == 0 && total == 0 {
                reset_observed.set(true);
            }
        };
        storage.rebuild_analytics_since_with_chunk_size_and_progress(
            None,
            128,
            Some(&progress),
            None,
            Some(context),
            None,
        )?;
        assert_eq!(reset_observed.get(), retained.is_some());
        for replay in 0..2 {
            if replay != 0 {
                storage.rebuild_analytics_since_with_chunk_size_and_progress(
                    None,
                    128,
                    None,
                    None,
                    Some(context),
                    None,
                )?;
            }
            for (table, expected_rows) in TABLES.iter().zip(&expected) {
                assert_eq!(
                    snapshot(&storage, table)?,
                    *expected_rows,
                    "retained {retained:?}, replay {replay}, table {table}"
                );
            }
            assert_eq!(snapshot(&storage, "messages")?, canonical);
        }
    }
    Ok(())
}

#[test]
fn gh424_legacy_omp_analytics_cursor_resumes_without_double_counting() -> anyhow::Result<()> {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    let dir = TempDir::new()?;
    let storage = SqliteStorage::open(&dir.path().join("test.db"))?;
    let agent_id = storage.ensure_agent(&Agent {
        id: None,
        slug: "omp".into(),
        name: "Oh My Pi".into(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    let base_ts = 1_770_551_400_000_i64;
    let conversation = Conversation {
        id: None,
        agent_slug: "omp".into(),
        workspace: None,
        external_id: Some("gh424-resume".into()),
        title: Some("GH 424 resume fixture".into()),
        source_path: PathBuf::from("/tmp/gh424-resume.jsonl"),
        started_at: Some(base_ts),
        ended_at: Some(base_ts + 5_000),
        approx_tokens: None,
        metadata_json: serde_json::json!({"source":"omp"}),
        messages: (0..5)
            .map(|idx| Message {
                id: None,
                idx,
                role: if idx % 2 == 0 {
                    MessageRole::User
                } else {
                    MessageRole::Agent
                },
                author: None,
                created_at: Some(base_ts + idx * 1_000),
                content: format!("OMP analytics resume message {idx}"),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            })
            .collect(),
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };
    storage
        .insert_conversations_batched_with_analytics(&[(agent_id, None, &conversation)], false)?;
    let message_ids: Vec<i64> = storage.conn.query_map_collect(
        "SELECT id FROM messages ORDER BY id",
        fparams![],
        |row| row.get_typed(0),
    )?;
    assert_eq!(message_ids.len(), 5);

    let context = "gh424-test-context";
    let scoped_resume_error = storage
        .rebuild_analytics_since_with_chunk_size_and_progress(
            Some(base_ts),
            2,
            None,
            None,
            Some(context),
            None,
        )
        .expect_err("a full-archive cursor must not be applied to a scoped rebuild");
    assert!(
        scoped_resume_error
            .to_string()
            .contains("valid only for a full analytics rebuild"),
        "unexpected scoped-resume error: {scoped_resume_error:#}"
    );
    let interrupt_after_first_chunk = |processed: i64, _total: i64| {
        assert!(processed == 0 || processed >= 2);
        if processed >= 2 {
            panic!("GH #424 planted interruption after first committed chunk");
        }
    };
    let heartbeat_count = std::cell::Cell::new(0_usize);
    let heartbeat = || heartbeat_count.set(heartbeat_count.get().saturating_add(1));
    let interrupted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        storage.rebuild_analytics_since_with_chunk_size_and_progress(
            None,
            2,
            Some(&interrupt_after_first_chunk),
            Some(&heartbeat),
            Some(context),
            None,
        )
    }));
    assert!(
        interrupted.is_err(),
        "the planted interruption must stop the first rebuild"
    );
    assert!(
        heartbeat_count.get() >= 4,
        "the watchdog must be refreshed around the count and dimension-map phases before the first chunk completes"
    );

    let cursor_context: String = storage.conn.query_row_map(
        "SELECT value FROM meta WHERE key = ?1",
        fparams![LEGACY_OMP_ANALYTICS_CURSOR_CONTEXT_META_KEY],
        |row| row.get_typed(0),
    )?;
    let cursor_last_id: String = storage.conn.query_row_map(
        "SELECT value FROM meta WHERE key = ?1",
        fparams![LEGACY_OMP_ANALYTICS_CURSOR_LAST_ID_META_KEY],
        |row| row.get_typed(0),
    )?;
    let cursor_processed: String = storage.conn.query_row_map(
        "SELECT value FROM meta WHERE key = ?1",
        fparams![LEGACY_OMP_ANALYTICS_CURSOR_PROCESSED_META_KEY],
        |row| row.get_typed(0),
    )?;
    assert_eq!(cursor_context, context);
    assert_eq!(cursor_last_id, message_ids[1].to_string());
    assert_eq!(cursor_processed, "2");
    let partial_metrics: i64 =
        storage
            .conn
            .query_row_map("SELECT COUNT(*) FROM message_metrics", fparams![], |row| {
                row.get_typed(0)
            })?;
    assert_eq!(partial_metrics, 2);

    storage.rebuild_analytics_since_with_chunk_size_and_progress(
        None,
        2,
        None,
        Some(&heartbeat),
        Some(context),
        None,
    )?;
    let resumed_metrics: i64 =
        storage
            .conn
            .query_row_map("SELECT COUNT(*) FROM message_metrics", fparams![], |row| {
                row.get_typed(0)
            })?;
    assert_eq!(resumed_metrics, 5);

    type UsageSnapshotRow = (i64, String, i64, String, i64, i64, i64, i64);
    let usage_snapshot = || -> anyhow::Result<Vec<UsageSnapshotRow>> {
        Ok(storage.conn.query_map_collect(
            "SELECT day_id, agent_slug, workspace_id, source_id,
                    message_count, user_message_count,
                    assistant_message_count, content_tokens_est_total
             FROM usage_daily
             ORDER BY day_id, agent_slug, workspace_id, source_id",
            fparams![],
            |row| {
                Ok((
                    row.get_typed(0)?,
                    row.get_typed(1)?,
                    row.get_typed(2)?,
                    row.get_typed(3)?,
                    row.get_typed(4)?,
                    row.get_typed(5)?,
                    row.get_typed(6)?,
                    row.get_typed(7)?,
                ))
            },
        )?)
    };
    let resumed_usage = usage_snapshot()?;

    // A clean rebuild is the oracle for additive rollups. It must match
    // the interrupted/resumed result exactly and retire the foreign
    // migration cursor rather than leaving a future double-count trap.
    storage.rebuild_analytics_since_with_chunk_size(None, 2)?;
    assert_eq!(usage_snapshot()?, resumed_usage);
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
        assert_eq!(rows, 0, "ordinary rebuild must invalidate {cursor_key}");
    }

    // Recreate an interrupted prefix, then model a canonical prune plus a
    // later append before restart. `processed <= total_messages` still
    // holds, but the additive usage prefix no longer describes the live
    // message set. The cursor must reject that drift and restart cleanly.
    let drift_context = "gh424-prefix-drift-context";
    let interrupted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        storage.rebuild_analytics_since_with_chunk_size_and_progress(
            None,
            2,
            Some(&interrupt_after_first_chunk),
            Some(&heartbeat),
            Some(drift_context),
            None,
        )
    }));
    assert!(
        interrupted.is_err(),
        "the second planted interruption must leave a committed prefix"
    );
    storage.conn.execute_compat(
        "DELETE FROM message_metrics WHERE message_id = ?1",
        fparams![message_ids[0]],
    )?;
    storage.conn.execute_compat(
        "DELETE FROM messages WHERE id = ?1",
        fparams![message_ids[0]],
    )?;
    let conversation_id: i64 = storage.conn.query_row_map(
        "SELECT conversation_id FROM messages WHERE id = ?1",
        fparams![message_ids[1]],
        |row| row.get_typed(0),
    )?;
    storage.conn.execute_compat(
        "INSERT INTO messages(conversation_id, idx, role, created_at, content)
         VALUES(?1, 99, 'user', ?2, 'replacement after analytics interruption')",
        fparams![conversation_id, base_ts + 99_000],
    )?;

    storage.rebuild_analytics_since_with_chunk_size_and_progress(
        None,
        2,
        None,
        Some(&heartbeat),
        Some(drift_context),
        None,
    )?;
    let drift_resumed_usage = usage_snapshot()?;
    let drift_resumed_metrics: i64 =
        storage
            .conn
            .query_row_map("SELECT COUNT(*) FROM message_metrics", fparams![], |row| {
                row.get_typed(0)
            })?;
    assert_eq!(drift_resumed_metrics, 5);

    storage.rebuild_analytics_since_with_chunk_size(None, 2)?;
    assert_eq!(
        usage_snapshot()?,
        drift_resumed_usage,
        "prefix drift must restart rather than preserve deleted-message rollup deltas"
    );
    Ok(())
}

#[test]
fn rebuild_analytics_since_rebuilds_only_recent_days() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let agent = Agent {
        id: None,
        slug: "claude_code".into(),
        name: "Claude Code".into(),
        version: Some("1.0".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();

    // Day 1: 2026-02-06 10:30 UTC. Day 2: three days later, same time.
    let day1_ts = 1_770_551_400_000_i64;
    let day2_ts = day1_ts + 3 * 86_400_000;
    let day1 = SqliteStorage::day_id_from_millis(day1_ts);
    let day2 = SqliteStorage::day_id_from_millis(day2_ts);
    assert_eq!(day2 - day1, 3);

    let make_conv = |ext: &str, ts: i64, n: usize| Conversation {
        id: None,
        agent_slug: "claude_code".into(),
        workspace: None,
        external_id: Some(ext.into()),
        title: None,
        source_path: PathBuf::from(format!("/tmp/{ext}.jsonl")),
        started_at: Some(ts),
        ended_at: Some(ts + 60_000),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: (0..n)
            .map(|i| Message {
                id: None,
                idx: i as i64,
                role: if i % 2 == 0 {
                    MessageRole::User
                } else {
                    MessageRole::Agent
                },
                author: None,
                created_at: Some(ts + (i as i64) * 1_000),
                content: format!("message {i} of {ext}"),
                extra_json: serde_json::Value::Null,
                snippets: vec![],
            })
            .collect(),
        source_id: "local".into(),
        origin_host: None,
    };
    let conv1 = make_conv("since-day1", day1_ts, 2);
    let conv2 = make_conv("since-day2", day2_ts, 5);
    storage
        .insert_conversations_batched_with_analytics(
            &[(agent_id, None, &conv1), (agent_id, None, &conv2)],
            false,
        )
        .unwrap();

    let conn = storage.raw();
    let daily_count = |day: i64| -> i64 {
        conn.query_row_map(
            "SELECT COALESCE(SUM(message_count), 0) FROM usage_daily WHERE day_id = ?1",
            fparams![day],
            |row| row.get_typed(0),
        )
        .unwrap()
    };
    let metrics_count = |day: i64| -> i64 {
        conn.query_row_map(
            "SELECT COUNT(*) FROM message_metrics WHERE day_id = ?1",
            fparams![day],
            |row| row.get_typed(0),
        )
        .unwrap()
    };
    assert_eq!(daily_count(day1), 2);
    assert_eq!(daily_count(day2), 5);

    // Wipe every rollup, then rebuild only from the middle of day 2.
    conn.execute("DELETE FROM message_metrics").unwrap();
    conn.execute("DELETE FROM usage_hourly").unwrap();
    conn.execute("DELETE FROM usage_daily").unwrap();
    conn.execute("DELETE FROM usage_models_daily").unwrap();

    // A cutoff *after* the day-2 messages but on the same UTC day must
    // still rescan them: the window is widened to the start of that day.
    let result = storage
        .rebuild_analytics_since(Some(day2_ts + 6 * 3_600_000))
        .unwrap();
    assert_eq!(result.message_metrics_rows, 5);
    assert_eq!(metrics_count(day2), 5);
    assert_eq!(daily_count(day2), 5);
    // Day 1 was outside the window and stays untouched (still wiped).
    assert_eq!(metrics_count(day1), 0);
    assert_eq!(daily_count(day1), 0);

    // Re-running the same windowed rebuild must not double count.
    let again = storage.rebuild_analytics_since(Some(day2_ts)).unwrap();
    assert_eq!(again.message_metrics_rows, 5);
    assert_eq!(daily_count(day2), 5);
    let hourly_day2: i64 = conn
        .query_row_map(
            "SELECT COALESCE(SUM(message_count), 0) FROM usage_hourly WHERE hour_id = ?1",
            fparams![SqliteStorage::hour_id_from_millis(day2_ts)],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(hourly_day2, 5);

    // A window reaching back to day 1 restores it without disturbing day 2.
    let full_window = storage.rebuild_analytics_since(Some(day1_ts)).unwrap();
    assert_eq!(full_window.message_metrics_rows, 7);
    assert_eq!(daily_count(day1), 2);
    assert_eq!(daily_count(day2), 5);

    // And the unwindowed path still matches.
    let full = storage.rebuild_analytics().unwrap();
    assert_eq!(full.message_metrics_rows, 7);
    assert_eq!(daily_count(day1), 2);
    assert_eq!(daily_count(day2), 5);
}
