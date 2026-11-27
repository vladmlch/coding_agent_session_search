use super::super::*;
use super::helpers::*;
use crate::franken_sync::SqliteValue;
use crate::franken_sync::compat::{ConnectionExt, ParamValue, RowExt};
use serial_test::serial;
use tempfile::TempDir;

/// #298 gate (non-watch): a small conversation that hits a typed NoMem
/// with no real host memory pressure is deferred to the next index run —
/// never quarantined — because SQLite's bounded allocation guards
/// raise the same typed error without the host being out of memory.
#[test]
#[serial]
fn streaming_consumer_defers_small_non_watch_oom_without_quarantine() {
    for source_completion in [false, true] {
        let oom_guard = inject_non_watch_ingest_test_oom(1);
        // Pin the pressure probe to "never real pressure" so the size gate is
        // what decides, deterministically, on any host.
        let _reserve_guard = set_env("CASS_WATCH_OOM_REAL_PRESSURE_RESERVE_BYTES", "0");
        let tmp = TempDir::new().unwrap();
        let data_dir = tmp.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();

        let db_path = data_dir.join("db.sqlite");
        let storage = FrankenStorage::open(&db_path).unwrap();
        ensure_fts_schema(&storage);
        let mut index = TantivyIndex::open_or_create(&index_dir(&data_dir).unwrap()).unwrap();
        let progress = Arc::new(IndexingProgress::default());
        let conv = norm_conv(Some("defer-single"), vec![norm_msg(0, 1_700_000_000_000)]);
        let prior_watermark = 1_600_000_000_000;
        let scan_watermark = 1_700_000_123_456;
        storage
            .set_connector_last_scan_ts("codex", prior_watermark)
            .unwrap();
        let send_batch = |tx: &Sender<IndexMessage>| {
            if source_completion {
                tx.send(IndexMessage::SourceComplete {
                    connector_name: "codex",
                    conversations: vec![conv.clone()],
                    completion: crate::storage::sqlite::SourceIngestLedgerEntry {
                        key: "source_ingest_v1:consumer-oom".into(),
                        observation: "{}".into(),
                    },
                    byte_reservation: 0,
                })
                .unwrap();
            } else {
                send_conversation_batches(tx, "codex", vec![conv.clone()], true);
            }
        };

        let (tx, rx) = bounded(STREAMING_CHANNEL_SIZE);
        send_batch(&tx);
        send_done(&tx, "codex", true);
        send_done(&tx, "claude", true);
        drop(tx);

        let (_discovered, outcome) = run_streaming_consumer(
            rx,
            2,
            &storage,
            &data_dir,
            Some(&mut index),
            Arc::new(StreamingByteLimiter::new(STREAMING_MAX_BYTES_IN_FLIGHT)),
            &Some(progress.clone()),
            LexicalPopulationStrategy::IncrementalInline,
            true,
            Some(scan_watermark),
            None,
        )
        .expect("small-conversation NoMem without real pressure should defer, not fail");

        assert!(
            storage.source_ingest_ledger_entries().unwrap().is_empty(),
            "deferred canonical ingestion must not certify source completion"
        );
        assert_eq!(outcome.quarantined_conversations, 0);
        assert!(outcome.lexical_update_deferred);
        assert!(outcome.scan_had_errors);
        assert_eq!(
            outcome.deferred_sources,
            BTreeSet::from([conv.source_path.clone()])
        );
        assert_eq!(
            outcome.scanned_connectors,
            BTreeSet::from(["claude".to_string()])
        );
        assert_eq!(
            storage.get_connector_last_scan_ts("codex").unwrap(),
            Some(prior_watermark)
        );
        assert_eq!(
            storage.get_connector_last_scan_ts("claude").unwrap(),
            Some(scan_watermark)
        );
        assert!(
            !data_dir
                .join("quarantine/index_ingest_poison.jsonl")
                .exists(),
            "a small conversation with no real memory pressure must be deferred, not quarantined (#298)"
        );

        drop(oom_guard);
        for expected_inserted in [1, 0] {
            let (tx, rx) = bounded(STREAMING_CHANNEL_SIZE);
            send_batch(&tx);
            send_done(&tx, "codex", true);
            drop(tx);
            let (_, retried) = run_streaming_consumer(
                rx,
                1,
                &storage,
                &data_dir,
                Some(&mut index),
                Arc::new(StreamingByteLimiter::new(STREAMING_MAX_BYTES_IN_FLIGHT)),
                &Some(progress.clone()),
                LexicalPopulationStrategy::IncrementalInline,
                true,
                Some(scan_watermark),
                None,
            )
            .unwrap();
            assert!(!retried.scan_had_errors);
            assert!(retried.deferred_sources.is_empty());
            assert_eq!(retried.inserted_messages, expected_inserted);
            assert_eq!(
                storage.get_connector_last_scan_ts("codex").unwrap(),
                Some(scan_watermark)
            );
            assert_eq!(storage.total_message_count().unwrap(), 1);
            assert_eq!(
                storage.source_ingest_ledger_entries().unwrap().len(),
                usize::from(source_completion)
            );
            assert_eq!(
                progress.phase.load(Ordering::Relaxed),
                INDEX_PHASE_LEXICAL_INDEXING
            );
            assert_eq!(progress.total.load(Ordering::Relaxed), 1);
            assert_eq!(progress.current.load(Ordering::Relaxed), 1);
        }
    }
}

/// #290: a stable, already-triaged backlog of *same-version* irreducible
/// quarantines (recurs on every refresh, content already searchable from a
/// prior pass) must NOT trip the recent-burst circuit breaker / `critical`
/// health, even when its size exceeds the breaker limit. It stays a
/// `degraded` advisory.
#[test]
#[serial]
fn same_version_quarantine_backlog_does_not_trip_circuit_breaker() -> Result<()> {
    let _limit_guard = set_env("CASS_INGEST_QUARANTINE_CIRCUIT_LIMIT", "2");
    let _window_guard = set_env("CASS_INGEST_QUARANTINE_CIRCUIT_WINDOW_SECS", "86400");

    let tmp = TempDir::new()?;
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir)?;

    // Five same-version irreducible quarantines, all attempted "now" (recent).
    // record_attempt stamps the CURRENT cass version, so every entry is
    // same-version (IrreducibleSameVersion) — the already-triaged backlog.
    let now = chrono::Utc::now();
    let mut state = QuarantineState::default();
    for i in 0..5 {
        let key = QuarantineKey::new(format!("conv-stable-{i}"), 3);
        state.record_attempt(&key, "index-ingest-out-of-memory: out of memory", now);
    }
    state.save(&data_dir)?;

    let summary = conversation_ingest_quarantine_summary(&data_dir);
    anyhow::ensure!(
        summary.quarantined_conversations == 5,
        "all five conversations are still counted in the backlog total"
    );
    anyhow::ensure!(
        summary.recent_quarantined_conversations == 0,
        "same-version re-skips must not count toward the recent burst; got {}",
        summary.recent_quarantined_conversations
    );
    anyhow::ensure!(
        !summary.circuit_breaker_active,
        "a stable same-version backlog must not trip the breaker / critical health"
    );
    anyhow::ensure!(
        summary.status == "degraded",
        "stable searchable backlog is a degraded advisory, not critical; got {}",
        summary.status
    );
    Ok(())
}

/// #290 (contrapositive): a *fresh* recent burst of genuinely-new
/// (version-stale / never-triaged-under-this-binary) quarantines still trips
/// the circuit breaker / `critical` health, because those conversations'
/// content may be absent from the published index.
#[test]
#[serial]
fn fresh_version_stale_quarantine_burst_still_trips_circuit_breaker() -> Result<()> {
    let _limit_guard = set_env("CASS_INGEST_QUARANTINE_CIRCUIT_LIMIT", "2");
    let _window_guard = set_env("CASS_INGEST_QUARANTINE_CIRCUIT_WINDOW_SECS", "86400");

    let tmp = TempDir::new()?;
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir)?;

    let now = chrono::Utc::now();
    let mut state = QuarantineState::default();
    for i in 0..3 {
        let key = QuarantineKey::new(format!("conv-fresh-{i}"), 3);
        state.record_attempt(&key, "index-ingest-out-of-memory: out of memory", now);
    }
    // Rewrite each entry's version to an OLDER one so they are retry-eligible
    // (version-stale) — i.e. a genuinely-new burst this binary has not triaged.
    for record in state.entries.values_mut() {
        record.cass_version_at_quarantine = Some("0.0.1-old".to_string());
    }
    state.save(&data_dir)?;

    let summary = conversation_ingest_quarantine_summary(&data_dir);
    anyhow::ensure!(
        summary.recent_quarantined_conversations == 3,
        "fresh version-stale quarantines count toward the recent burst; got {}",
        summary.recent_quarantined_conversations
    );
    anyhow::ensure!(
        summary.circuit_breaker_active,
        "a fresh burst exceeding the limit must still trip the breaker"
    );
    anyhow::ensure!(
        summary.status == "critical",
        "a fresh burst is critical; got {}",
        summary.status
    );
    Ok(())
}

/// 2l1b0.74: the index-run lock and the source advisory-lock probe now use
/// std File locking. A holder that took the lock through fs2 (an older
/// cass, or any fs2 caller) must still read as busy, and a free lock as
/// free: on Linux both are flock, on Windows both are LockFileEx.
#[test]
fn std_lock_probes_see_an_fs2_holder_as_busy() -> Result<()> {
    let tmp = TempDir::new()?;
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir)?;
    let db_path = data_dir.join("agent_search.db");
    let lock_path = data_dir.join("index-run.lock");
    let holder = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)?;
    fs2::FileExt::try_lock_exclusive(&holder)?;

    let error = acquire_index_run_lock(&data_dir, &db_path, SearchMaintenanceMode::Index)
        .err()
        .ok_or_else(|| anyhow::anyhow!("a held index-run lock must refuse a second run"))?;
    let rendered = format!("{error:#}");
    anyhow::ensure!(
        rendered.contains("another cass index process already holds"),
        "contention must keep the index-busy message: {rendered}"
    );
    anyhow::ensure!(
        source_file_has_active_advisory_lock(&lock_path),
        "a file another handle holds must read as locked"
    );

    fs2::FileExt::unlock(&holder)?;
    anyhow::ensure!(
        !source_file_has_active_advisory_lock(&lock_path),
        "a released file must read as unlocked"
    );
    let _guard = acquire_index_run_lock(&data_dir, &db_path, SearchMaintenanceMode::Index)?;
    Ok(())
}

#[test]
fn quarantine_retry_apply_refuses_to_race_active_index_run() -> Result<()> {
    let tmp = TempDir::new()?;
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir)?;
    let db_path = data_dir.join("agent_search.db");
    let _active_index = acquire_index_run_lock(&data_dir, &db_path, SearchMaintenanceMode::Index)?;
    let config = quarantine_retry::RetryConfig::default();

    let dry_run = run_quarantine_retry(&data_dir, &config, false)?;
    anyhow::ensure!(
        dry_run.total_quarantined_before == 0,
        "read-only quarantine planning should remain available while indexing is active"
    );

    let error = run_quarantine_retry(&data_dir, &config, true)
        .expect_err("quarantine apply must not race the active index writer");
    let rendered = format!("{error:#}");
    anyhow::ensure!(
        rendered.contains("another cass index process already holds"),
        "quarantine apply should return the authoritative lock contention error: {rendered}"
    );
    Ok(())
}

#[test]
fn quarantine_retry_refuses_to_overwrite_malformed_operator_checkpoint() -> Result<()> {
    let tmp = TempDir::new()?;
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir)?;
    let state_path = QuarantineState::path(&data_dir);
    let malformed = b"not valid quarantine json";
    std::fs::write(&state_path, malformed)?;
    let config = quarantine_retry::RetryConfig::default();

    let plan_error = plan_quarantine_retry(&data_dir, &config)
        .expect_err("retry planning must surface a malformed checkpoint");
    anyhow::ensure!(
        format!("{plan_error:#}").contains("invalid quarantine state"),
        "unexpected retry-plan error: {plan_error:#}"
    );
    let apply_error = run_quarantine_retry(&data_dir, &config, true)
        .expect_err("retry apply must surface a malformed checkpoint");
    anyhow::ensure!(
        format!("{apply_error:#}").contains("invalid quarantine state"),
        "unexpected retry-apply error: {apply_error:#}"
    );
    anyhow::ensure!(
        std::fs::read(&state_path)?.as_slice() == malformed,
        "failed operator commands must preserve malformed checkpoint bytes"
    );
    Ok(())
}

/// xaztn: dry-run classifies without mutation; apply reparses exactly the
/// keyed source conversation and persists it before clearing quarantine.
#[test]
#[serial]
fn xaztn_targeted_quarantine_retry_reingests_exact_conversation() -> Result<()> {
    let tmp = TempDir::new()?;
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir)?;
    let source_path = tmp
        .path()
        .join(".codex/sessions/2026/07/rollout-retry.jsonl");
    std::fs::create_dir_all(source_path.parent().context("source parent")?)?;
    std::fs::write(
        &source_path,
        r#"{"timestamp":"2026-07-22T05:00:00.000Z","type":"session_meta","payload":{"id":"targeted-retry","cwd":"/data/projects/cass"}}
{"timestamp":"2026-07-22T05:00:01.000Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"xaztn targeted quarantine retry"}]}}
{"timestamp":"2026-07-22T05:00:02.000Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"targeted retry complete"}]}}
"#,
    )?;

    let connector = CodexConnector::new();
    let root = ScanRoot::local(source_path.clone());
    let context = crate::connectors::ScanContext::with_roots(source_path.clone(), vec![root], None);
    let mut scanned = connector.scan(&context)?;
    anyhow::ensure!(scanned.len() == 1, "fixture must parse one conversation");
    let scanned_conversation = scanned
        .first_mut()
        .context("missing fixture conversation")?;
    scanned_conversation.external_id = Some("sessions/2026/07/rollout-retry".to_string());
    let conversation_id = poison_conversation_id(scanned_conversation);
    let schema_version = u32::try_from(crate::storage::sqlite::CURRENT_SCHEMA_VERSION)?;

    let now = chrono::Utc::now();
    let mut state = QuarantineState::default();
    // One eligible (version-stale) entry + one irreducible (same-version).
    let eligible = QuarantineKey::new(&conversation_id, schema_version);
    let eligible_storage_key = format!("{conversation_id}::v{schema_version}");
    state.record_attempt(&eligible, "index-ingest-out-of-memory: out of memory", now);
    let irreducible_schema_version = schema_version.checked_add(1).context("schema version")?;
    let irreducible = QuarantineKey::new(&conversation_id, irreducible_schema_version);
    let irreducible_storage_key = format!("{conversation_id}::v{irreducible_schema_version}");
    state.record_attempt(
        &irreducible,
        "index-ingest-out-of-memory: out of memory",
        now,
    );
    // Demote the eligible entry to an older version so it is retry-eligible;
    // the irreducible entry keeps the current version.
    if let Some(record) = state.entries.get_mut(&eligible_storage_key) {
        record.cass_version_at_quarantine = Some("0.0.1-old".to_string());
    }
    state.save(&data_dir)?;
    let quarantine_dir = data_dir.join("quarantine");
    std::fs::create_dir_all(&quarantine_dir)?;
    std::fs::write(
        quarantine_dir.join(INDEX_INGEST_POISON_FILE),
        format!(
            "{}\n{}\n",
            serde_json::json!({
                "conversation_id": conversation_id,
                "schema_version_at_quarantine": schema_version,
                "cass_version_at_quarantine": "0.0.1-old",
                "reason": "index-ingest-out-of-memory",
                "agent_slug": "codex",
                "external_id": scanned_conversation.external_id,
                "source_path": source_path.display().to_string(),
                "workspace": scanned_conversation.workspace.as_ref().map(|path| path.display().to_string()),
                "started_at": scanned_conversation.started_at,
                "ended_at": scanned_conversation.ended_at,
                "message_count": scanned_conversation.messages.len(),
            }),
            serde_json::json!({
                "conversation_id": conversation_id,
                "schema_version_at_quarantine": irreducible_schema_version,
                "cass_version_at_quarantine": env!("CARGO_PKG_VERSION"),
                "reason": "index-ingest-out-of-memory",
                "agent_slug": "codex",
                "external_id": scanned_conversation.external_id,
                "source_path": source_path.display().to_string(),
                "workspace": scanned_conversation.workspace.as_ref().map(|path| path.display().to_string()),
                "started_at": scanned_conversation.started_at,
                "ended_at": scanned_conversation.ended_at,
                "message_count": scanned_conversation.messages.len(),
            }),
        ),
    )?;

    let config = quarantine_retry::RetryConfig::default();

    let dry = plan_quarantine_retry(&data_dir, &config)?;
    anyhow::ensure!(dry.total_quarantined == 2);
    anyhow::ensure!(dry.planned_attempts == 1);
    anyhow::ensure!(
        dry.skip_irreducible == 1,
        "the same-version entry is irreducible; got {}",
        dry.skip_irreducible
    );
    anyhow::ensure!(QuarantineState::load(&data_dir).len() == 2);

    let applied = run_quarantine_retry(&data_dir, &config, true)?;
    anyhow::ensure!(
        applied.cleared == 1,
        "apply re-ingests and clears the one eligible entry; got {}",
        applied.cleared
    );
    anyhow::ensure!(
        applied.remaining_quarantined == 1,
        "the irreducible entry remains; got {}",
        applied.remaining_quarantined
    );
    let after = QuarantineState::load(&data_dir);
    anyhow::ensure!(!after.entries.contains_key(&eligible_storage_key));
    anyhow::ensure!(after.entries.contains_key(&irreducible_storage_key));
    let poison_after =
        std::fs::read_to_string(data_dir.join("quarantine").join(INDEX_INGEST_POISON_FILE))?;
    anyhow::ensure!(!poison_after.contains(&format!(
        "\"schema_version_at_quarantine\":{schema_version}"
    )));
    anyhow::ensure!(poison_after.contains(&format!(
        "\"schema_version_at_quarantine\":{irreducible_schema_version}"
    )));
    let storage = FrankenStorage::open(&data_dir.join("agent_search.db"))?;
    let message_count: i64 =
        storage
            .raw()
            .query_row_map("SELECT COUNT(*) FROM messages", &[], |row| row.get_typed(0))?;
    anyhow::ensure!(
        message_count == 2,
        "targeted retry must persist two messages"
    );
    anyhow::ensure!(
        source_path.is_file(),
        "retry must not delete the source log"
    );
    Ok(())
}

/// #296: the best-effort pre-abort WAL checkpoint runs against a real
/// canonical DB and leaves a DB readable by a fresh connection (no panic,
/// no orphaned WAL). A missing DB is a clean no-op.
#[test]
#[serial]
fn best_effort_abort_wal_checkpoint_truncates_and_is_noop_when_absent() -> Result<()> {
    let tmp = TempDir::new()?;
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir)?;

    // No DB yet -> clean no-op (must not panic).
    best_effort_abort_wal_checkpoint(&data_dir);
    anyhow::ensure!(
        !data_dir.join("agent_search.db").exists(),
        "an absent archive must not be created by the abort checkpoint"
    );

    // Create a canonical DB with some content and an active WAL.
    let db_path = data_dir.join("agent_search.db");
    let storage = FrankenStorage::open(&db_path)?;
    storage.run_migrations()?;
    drop(storage);

    // Checkpoint should succeed and leave a readable DB.
    best_effort_abort_wal_checkpoint(&data_dir);
    anyhow::ensure!(
        db_path.exists(),
        "DB file should still exist after checkpoint"
    );
    // A fresh connection can open and query it.
    let conn = crate::franken_sync::Connection::open(db_path.to_string_lossy())?;
    let _ = conn.query("PRAGMA quick_check;");
    conn.close().ok();
    Ok(())
}

/// Checkpoint SQL stays under its caller's ownership. Every timeout
/// process is owned and reaped by this test; no delayed worker can escape.
#[test]
fn gh515_checkpoint_deadline_preserves_ownership_through_return_unwind_and_timeout() -> Result<()> {
    const CHILD: &str = "CASS_TEST_GH515_CHECKPOINT_CHILD";
    const ROOT: &str = "CASS_TEST_GH515_CHECKPOINT_ROOT";
    let test_filter = format!(
        "{}::gh515_checkpoint_deadline_preserves_ownership_through_return_unwind_and_timeout",
        module_path!().split_once("::").unwrap().1
    );
    if let Ok(mode) = dotenvy::var(CHILD) {
        let root = PathBuf::from(dotenvy::var(ROOT)?);
        let db_path = root.join("archive.db");
        let _owner = acquire_semantic_backfill_lock(&root.join("owner"), &db_path)?;
        fs::write(root.join("ready"), b"owned")?;
        let ready_deadline = Instant::now() + Duration::from_secs(10);
        while !root.join("go").exists() {
            anyhow::ensure!(
                Instant::now() < ready_deadline,
                "supervisor did not release child"
            );
            std::thread::sleep(Duration::from_millis(2));
        }
        let caller = std::thread::current().id();
        let local = std::rc::Rc::new(std::cell::Cell::new(0));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_bounded_abort_wal_checkpoint(
                db_path,
                Duration::from_millis(50),
                "unit deadline",
                |_| {
                    assert_eq!(std::thread::current().id(), caller);
                    local.set(1);
                    match mode.as_str() {
                        "timeout" => std::thread::sleep(Duration::from_secs(3)),
                        "panic" => panic!("planted checkpoint panic"),
                        "error" => anyhow::bail!("planted checkpoint error"),
                        "complete" => {}
                        _ => anyhow::bail!("unknown checkpoint child mode"),
                    }
                    Ok(FinalWalCheckpointOutcome::Completed)
                },
            )
        }));
        match mode.as_str() {
            "complete" => assert!(matches!(
                result,
                Ok(AbortWalCheckpointAttempt::Finished(Ok(_)))
            )),
            "error" => assert!(matches!(
                result,
                Ok(AbortWalCheckpointAttempt::Finished(Err(_)))
            )),
            "panic" => assert!(result.is_err()),
            _ => anyhow::bail!("a timed-out checkpoint returned to its owner"),
        }
        assert_eq!(
            local.get(),
            1,
            "the real caller-thread closure must execute"
        );
        fs::write(root.join("returned"), b"timer joined")?;
        // A forgotten timer must not kill an already-returned owner later.
        std::thread::sleep(Duration::from_millis(150));
        return Ok(());
    }

    struct Child(std::process::Child);
    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    for mode in ["complete", "error", "panic", "timeout"] {
        let tmp = TempDir::new()?;
        let root = tmp.path();
        let db_path = root.join("archive.db");
        fs::write(&db_path, b"canonical bytes")?;
        let stderr_path = root.join("stderr");
        let mut child = Child(
            std::process::Command::new(std::env::current_exe()?)
                .args(["--exact", &test_filter, "--nocapture"])
                .env(CHILD, mode)
                .env(ROOT, root)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::from(File::create(&stderr_path)?))
                .spawn()?,
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        while !root.join("ready").exists() {
            anyhow::ensure!(
                child.0.try_wait()?.is_none(),
                "checkpoint child exited before admission"
            );
            anyhow::ensure!(
                Instant::now() < deadline,
                "checkpoint child did not acquire ownership"
            );
            std::thread::sleep(Duration::from_millis(2));
        }
        assert!(try_acquire_semantic_backfill_lock(&root.join("contender"), &db_path)?.is_none());
        fs::write(root.join("go"), b"run")?;
        let status = loop {
            if let Some(status) = child.0.try_wait()? {
                break status;
            }
            anyhow::ensure!(Instant::now() < deadline, "checkpoint child did not stop");
            std::thread::sleep(Duration::from_millis(2));
        };
        if mode == "timeout" {
            assert_eq!(
                status.code(),
                Some(70),
                "{}",
                fs::read_to_string(&stderr_path)?
            );
            assert!(!root.join("returned").exists());
            let stderr = fs::read_to_string(&stderr_path)?;
            let event = stderr
                .lines()
                .find_map(|line| {
                    serde_json::from_str::<serde_json::Value>(line)
                        .ok()
                        .filter(|value| value["kind"] == "wal-checkpoint-timeout")
                })
                .context("checkpoint timeout must emit the structured failure")?;
            assert_eq!(event["success"], false);
            assert_eq!(event["code"], 70);
            assert_eq!(event["retryable"], true);
            assert_eq!(event["deadline_ms"], 50);
            assert_eq!(event["context"], "unit deadline");
        } else {
            assert!(status.success(), "{}", fs::read_to_string(&stderr_path)?);
            assert!(root.join("returned").exists());
        }
        let _retry = acquire_semantic_backfill_lock(&root.join("contender"), &db_path)?;
        assert_eq!(fs::read(&db_path)?, b"canonical bytes");
    }
    Ok(())
}

#[test]
fn gh515_doctor_checkpoint_refuses_busy_archive_before_timer_or_open() -> Result<()> {
    let tmp = TempDir::new()?;
    let db_path = tmp.path().join("archive.db");
    fs::write(&db_path, b"archive must not be opened")?;
    let owner_data = tmp.path().join("owner");
    let owner = acquire_semantic_backfill_lock(&owner_data, &db_path)?;
    let before = crate::search::asset_state::read_search_maintenance_snapshot(&owner_data);
    let error =
        checkpoint_wal_truncate_with_deadline(&db_path, "doctor --fix", Duration::from_secs(5))
            .expect_err("doctor checkpoint must respect another archive writer");
    assert!(error.is::<IndexRunBusy>(), "{error:#}");
    assert_eq!(fs::read(&db_path)?, b"archive must not be opened");
    assert_eq!(
        crate::search::asset_state::read_search_maintenance_snapshot(&owner_data).job_id,
        before.job_id,
    );
    drop(owner);
    let _retry = acquire_semantic_backfill_lock(&tmp.path().join("next"), &db_path)?;
    Ok(())
}

/// #321: a `wal_checkpoint(TRUNCATE)` that SQLite reports as blocked
/// (`busy != 0`) or that leaves frames un-backfilled did NOT truncate the
/// WAL, so it must never be classified as a completed checkpoint. The
/// canonical reproducer from the field is `busy=1, log_frames=289842,
/// checkpointed_frames=0` — the abort path used to log
/// "checkpointed canonical WAL before stall abort (#296)" success on
/// exactly that no-op, masking a 1.1 GB stranded WAL and a malformed DB.
#[test]
fn final_wal_checkpoint_blocked_status_is_not_completed() {
    // The exact field reproducer: blocked by an active reader, nothing moved.
    assert_eq!(
        classify_final_wal_checkpoint(1, 289_842, 0),
        FinalWalCheckpointOutcome::Blocked {
            busy: 1,
            log_frames: 289_842,
            checkpointed_frames: 0,
        },
        "busy=1 checkpoint must classify as Blocked, never Completed"
    );

    // A clean TRUNCATE: not busy, every logged frame backfilled.
    assert_eq!(
        classify_final_wal_checkpoint(0, 128, 128),
        FinalWalCheckpointOutcome::Completed,
        "busy=0 with all frames backfilled is the only Completed case"
    );

    // Not busy, but a partial backfill still means the WAL was not fully
    // truncated -> Blocked (do not report success).
    assert_eq!(
        classify_final_wal_checkpoint(0, 128, 40),
        FinalWalCheckpointOutcome::Blocked {
            busy: 0,
            log_frames: 128,
            checkpointed_frames: 40,
        },
        "a partial checkpoint must not be reported as completed"
    );

    // An empty WAL (nothing to do) is trivially completed.
    assert_eq!(
        classify_final_wal_checkpoint(0, 0, 0),
        FinalWalCheckpointOutcome::Completed,
        "an already-empty WAL is a completed no-op"
    );
}

#[test]
#[serial]
fn non_watch_ingest_chunk_size_defaults_and_clamps() -> Result<()> {
    {
        let _env = unset_env_var("CASS_NON_WATCH_INGEST_CHUNK_SIZE");
        anyhow::ensure!(
            non_watch_ingest_chunk_size() == NON_WATCH_INGEST_DEFAULT_CHUNK_SIZE,
            "unset chunk size should use the default"
        );
    }

    {
        let _env = set_env("CASS_NON_WATCH_INGEST_CHUNK_SIZE", "0");
        anyhow::ensure!(
            non_watch_ingest_chunk_size() == NON_WATCH_INGEST_DEFAULT_CHUNK_SIZE,
            "zero chunk size should use the default"
        );
    }

    {
        let _env = set_env("CASS_NON_WATCH_INGEST_CHUNK_SIZE", "2");
        anyhow::ensure!(
            non_watch_ingest_chunk_size() == 2,
            "valid explicit chunk size should be honored"
        );
    }

    {
        let _env = set_env("CASS_NON_WATCH_INGEST_CHUNK_SIZE", "99999");
        anyhow::ensure!(
            non_watch_ingest_chunk_size() == NON_WATCH_INGEST_CHUNK_SIZE_MAX,
            "oversized chunk size should clamp to the safety cap"
        );
    }
    Ok(())
}

#[test]
#[serial]
fn chunked_non_watch_ingest_bumps_run_lock_progress() -> Result<()> {
    let _chunk_guard = set_env("CASS_NON_WATCH_INGEST_CHUNK_SIZE", "2");
    let tmp = TempDir::new()?;
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir)?;

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path)?;
    ensure_fts_schema(&storage);
    let progress_tracker = Arc::new(IndexingProgress::default());
    let progress = Some(progress_tracker.clone());
    let progress_bump = Arc::new(AtomicI64::new(1));
    let conversations: Vec<_> = [
        ("chunked-non-watch-0", 0_i64),
        ("chunked-non-watch-1", 1_i64),
        ("chunked-non-watch-2", 2_i64),
        ("chunked-non-watch-3", 3_i64),
        ("chunked-non-watch-4", 4_i64),
    ]
    .iter()
    .map(|(external_id, offset)| {
        norm_conv(
            Some(external_id),
            vec![norm_msg(0, 1_700_000_000_000 + offset)],
        )
    })
    .collect();

    let outcome = ingest_non_watch_batch_with_oom_split(
        &storage,
        None,
        &data_dir,
        &conversations,
        &progress,
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
        true,
        Some(&progress_bump),
    )?;

    anyhow::ensure!(
        outcome.inserted_conversations == conversations.len(),
        "chunked ingest should insert every conversation"
    );
    anyhow::ensure!(
        outcome.inserted_messages == conversations.len(),
        "chunked ingest should insert every message"
    );
    anyhow::ensure!(
        progress_tracker.current.load(Ordering::Relaxed) == conversations.len(),
        "progress counter should reflect all chunked conversations"
    );
    anyhow::ensure!(
        progress_bump.load(Ordering::Relaxed) > 1,
        "chunked ingest must refresh the run-lock heartbeat before the enclosing scan completes"
    );
    let conversation_count: i64 =
        storage
            .raw()
            .query_row_map("SELECT COUNT(*) FROM conversations", &[], |row| {
                row.get_typed(0)
            })?;
    anyhow::ensure!(
        conversation_count == i64::try_from(conversations.len())?,
        "storage should contain every chunked conversation"
    );
    Ok(())
}

#[test]
fn stale_index_ingest_quarantine_version_retry_detects_legacy_records() -> Result<()> {
    let tmp = TempDir::new()?;
    let data_dir = tmp.path().join("data");
    let quarantine_dir = data_dir.join("quarantine");
    std::fs::create_dir_all(&quarantine_dir)?;

    let legacy = norm_conv(
        Some("legacy-index-poison"),
        vec![norm_msg(0, 1_700_000_000_000)],
    );
    let current = norm_conv(
        Some("current-index-poison"),
        vec![norm_msg(0, 1_700_000_000_001)],
    );
    let legacy_id = poison_conversation_id(&legacy);
    let current_id = poison_conversation_id(&current);
    std::fs::write(
        quarantine_dir.join(INDEX_INGEST_POISON_FILE),
        format!(
            "{}\n{}\n",
            serde_json::json!({
                "conversation_id": legacy_id,
                "schema_version_at_quarantine": crate::storage::sqlite::CURRENT_SCHEMA_VERSION,
                "reason": "index-ingest-out-of-memory"
            }),
            serde_json::json!({
                "conversation_id": current_id,
                "schema_version_at_quarantine": crate::storage::sqlite::CURRENT_SCHEMA_VERSION,
                "cass_version_at_quarantine": current_cass_version(),
                "reason": "index-ingest-out-of-memory"
            })
        ),
    )
    .context("write index-ingest poison fixture")?;

    let retry = stale_index_ingest_quarantine_version_retry(&data_dir)
        .context("load index-ingest poison retry state")?
        .context("legacy missing-version record should request one retry")?;
    assert_eq!(retry.stale_records, 1);
    assert_eq!(retry.legacy_records, 1);
    assert_eq!(retry.previous_versions, vec!["unknown".to_string()]);
    Ok(())
}

#[test]
fn stale_index_ingest_quarantine_version_retry_detects_structured_legacy_records() -> Result<()> {
    let tmp = TempDir::new()?;
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir)?;

    let schema_version = u32::try_from(crate::storage::sqlite::CURRENT_SCHEMA_VERSION)
        .context("current schema version fits in quarantine key")?;
    let mut state = QuarantineState::default();
    state.record_attempt(
        &QuarantineKey::new("structured-legacy-index-poison", schema_version),
        "index-ingest-out-of-memory: out of memory",
        chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_000, 0)
            .context("valid quarantine timestamp")?,
    );
    state
        .entries
        .values_mut()
        .next()
        .context("structured quarantine record exists")?
        .cass_version_at_quarantine = None;
    state.save(&data_dir)?;

    let retry = stale_index_ingest_quarantine_version_retry(&data_dir)
        .context("load structured index-ingest poison retry state")?
        .context("structured legacy record should request one retry")?;
    assert_eq!(retry.stale_records, 1);
    assert_eq!(retry.legacy_records, 1);
    assert_eq!(retry.previous_versions, vec!["unknown".to_string()]);
    Ok(())
}

#[test]
fn marking_stale_index_ingest_quarantine_retry_attempted_prevents_repeat() -> Result<()> {
    let tmp = TempDir::new()?;
    let data_dir = tmp.path().join("data");
    let quarantine_dir = data_dir.join("quarantine");
    std::fs::create_dir_all(&quarantine_dir)?;

    let conv = norm_conv(
        Some("stale-index-poison-missing-source"),
        vec![norm_msg(0, 1_700_000_000_000)],
    );
    let conversation_id = poison_conversation_id(&conv);
    let jsonl_path = quarantine_dir.join(INDEX_INGEST_POISON_FILE);
    std::fs::write(
        &jsonl_path,
        format!(
            "{}\n",
            serde_json::json!({
                "conversation_id": conversation_id,
                "schema_version_at_quarantine": crate::storage::sqlite::CURRENT_SCHEMA_VERSION,
                "reason": "index-ingest-out-of-memory",
                "cass_version_at_quarantine": "0.0.0"
            })
        ),
    )
    .context("write stale index-ingest poison fixture")?;

    let schema_version = u32::try_from(crate::storage::sqlite::CURRENT_SCHEMA_VERSION)
        .context("current schema version fits in quarantine key")?;
    let mut state = QuarantineState::default();
    state.record_attempt(
        &QuarantineKey::new(conversation_id, schema_version),
        "index-ingest-out-of-memory: out of memory",
        chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_000, 0)
            .context("valid quarantine timestamp")?,
    );
    state
        .entries
        .values_mut()
        .next()
        .context("structured quarantine record exists")?
        .cass_version_at_quarantine = None;
    state.save(&data_dir)?;

    let retry = stale_index_ingest_quarantine_version_retry(&data_dir)
        .context("load index-ingest poison retry state")?
        .context("stale records should request a retry before marking")?;
    assert_eq!(retry.stale_records, 1);
    assert_eq!(retry.legacy_records, 1);

    assert_eq!(
        mark_stale_index_ingest_quarantine_retry_attempted(&data_dir)
            .context("mark stale retry attempted")?,
        2,
        "both JSONL and structured quarantine surfaces should be marked"
    );
    assert!(
        stale_index_ingest_quarantine_version_retry(&data_dir)?.is_none(),
        "marking an attempted retry must prevent repeat full scans"
    );
    assert!(
        jsonl_path.exists(),
        "marking retry attempted must rewrite the quarantine file in place, not delete it"
    );
    let jsonl = std::fs::read_to_string(&jsonl_path)?;
    let record: serde_json::Value = serde_json::from_str(
        jsonl
            .lines()
            .next()
            .context("rewritten JSONL record remains")?,
    )?;
    assert_eq!(
        poison_record_cass_version(&record),
        Some(current_cass_version())
    );

    let structured_state = QuarantineState::load(&data_dir);
    assert_eq!(structured_state.len(), 1);
    let structured_record = structured_state
        .entries
        .values()
        .next()
        .context("structured quarantine record remains")?;
    assert_eq!(
        structured_record.cass_version_at_quarantine.as_deref(),
        Some(current_cass_version())
    );
    Ok(())
}

#[test]
fn successful_ingest_clears_matching_index_poison_records_without_deleting_file() -> Result<()> {
    let tmp = TempDir::new()?;
    let data_dir = tmp.path().join("data");
    let quarantine_dir = data_dir.join("quarantine");
    std::fs::create_dir_all(&quarantine_dir)?;

    let conv = norm_conv(
        Some("stale-index-poison-cleared"),
        vec![norm_msg(0, 1_700_000_000_000)],
    );
    let conversation_id = poison_conversation_id(&conv);
    let jsonl_path = quarantine_dir.join(INDEX_INGEST_POISON_FILE);
    std::fs::write(
        &jsonl_path,
        format!(
            "{}\n",
            serde_json::json!({
                "conversation_id": conversation_id,
                "schema_version_at_quarantine": crate::storage::sqlite::CURRENT_SCHEMA_VERSION,
                "reason": "index-ingest-out-of-memory",
                "cass_version_at_quarantine": "0.0.0"
            })
        ),
    )
    .context("write index-ingest poison fixture")?;

    let schema_version = u32::try_from(crate::storage::sqlite::CURRENT_SCHEMA_VERSION)
        .context("current schema version fits in quarantine key")?;
    let mut state = QuarantineState::default();
    state.record_attempt(
        &QuarantineKey::new(conversation_id.clone(), schema_version),
        "index-ingest-out-of-memory: out of memory",
        chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_000, 0)
            .context("valid quarantine timestamp")?,
    );
    state.save(&data_dir)?;

    clear_poison_conversations_after_successful_ingest(
        &data_dir,
        INDEX_INGEST_POISON_FILE,
        "index-ingest-out-of-memory",
        &[conv],
    );

    assert!(
        jsonl_path.exists(),
        "cleanup must truncate the quarantine file in place, not delete it"
    );
    assert!(
        std::fs::read_to_string(&jsonl_path)?.trim().is_empty(),
        "successful retry should remove the matching JSONL poison record"
    );
    assert!(
        QuarantineState::load(&data_dir).is_empty(),
        "successful retry should clear the matching structured quarantine record"
    );
    Ok(())
}

#[test]
fn operator_clear_removes_selected_key_from_both_poison_ledgers() -> Result<()> {
    let tmp = TempDir::new()?;
    let data_dir = tmp.path().join("data");
    let quarantine_dir = data_dir.join("quarantine");
    std::fs::create_dir_all(&quarantine_dir)?;

    for (file_name, reason) in [
        (WATCH_INGEST_POISON_FILE, "watch-ingest-out-of-memory"),
        (INDEX_INGEST_POISON_FILE, "index-ingest-out-of-memory"),
    ] {
        std::fs::write(
            quarantine_dir.join(file_name),
            format!(
                "{}\n{}\n",
                serde_json::json!({
                    "conversation_id": "selected",
                    "schema_version_at_quarantine": 1,
                    "reason": reason,
                    "cass_version_at_quarantine": if file_name == WATCH_INGEST_POISON_FILE {
                        current_cass_version()
                    } else {
                        "0.0.1-old"
                    },
                    "last_attempt_at_ms": if file_name == WATCH_INGEST_POISON_FILE {
                        10
                    } else {
                        20
                    },
                }),
                serde_json::json!({
                    "conversation_id": "retained",
                    "schema_version_at_quarantine": 1,
                    "reason": reason,
                }),
            ),
        )?;
    }

    let operator_records = operator_quarantine_poison_records(&data_dir)?;
    assert_eq!(operator_records.len(), 2);
    assert!(operator_records.contains_key(&("selected".to_string(), 1)));
    assert!(operator_records.contains_key(&("retained".to_string(), 1)));
    assert_eq!(
        operator_records
            .get(&("selected".to_string(), 1))
            .and_then(poison_record_cass_version),
        Some(current_cass_version()),
        "current-version evidence must win over a newer stale mirror"
    );

    let selected = BTreeSet::from([("selected".to_string(), 1_i64)]);
    assert_eq!(
        clear_operator_quarantine_poison_records(&data_dir, &selected)?,
        2
    );
    for file_name in [WATCH_INGEST_POISON_FILE, INDEX_INGEST_POISON_FILE] {
        let path = quarantine_dir.join(file_name);
        assert!(path.exists(), "operator clear must not delete {file_name}");
        let contents = std::fs::read_to_string(path)?;
        assert!(!contents.contains("selected"));
        assert!(contents.contains("retained"));
    }
    let sidecars = std::fs::read_dir(&quarantine_dir)?
        .filter_map(std::result::Result::ok)
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| name.contains(".tmp."))
        .collect::<Vec<_>>();
    assert!(
        sidecars.is_empty(),
        "successful atomic poison-ledger rewrites left temp files: {sidecars:?}"
    );
    Ok(())
}

/// Regression for cass#261: cross-surface storm guard.
///
/// If `mark_stale_index_ingest_jsonl_retry_attempted` succeeds for a
/// conversation (JSONL stamped with current_version) but
/// `mark_stale_index_ingest_structured_retry_attempted`'s `state.save()`
/// fails (leaving the structured record at `cass_version_at_quarantine =
/// None`), the next call to `stale_index_ingest_quarantine_version_retry`
/// must NOT signal a retry for that conversation even though the structured
/// record still looks legacy.  Without the `already_current_keys` guard
/// added to `StalePoisonVersionAccumulator`, the structured `None`
/// observation would win and trigger a full scan on every subsequent run —
/// an unbounded storm.
#[test]
fn cross_surface_current_version_suppresses_legacy_structured_entry() -> Result<()> {
    let tmp = TempDir::new()?;
    let data_dir = tmp.path().join("data");
    let quarantine_dir = data_dir.join("quarantine");
    std::fs::create_dir_all(&quarantine_dir)?;

    let conv = norm_conv(
        Some("cross-surface-storm-guard"),
        vec![norm_msg(0, 1_700_000_000_000)],
    );
    let conversation_id = poison_conversation_id(&conv);
    let current = current_cass_version();
    let schema_version = crate::storage::sqlite::CURRENT_SCHEMA_VERSION;

    // JSONL: already stamped with current_version (mark succeeded here)
    let jsonl_path = quarantine_dir.join(INDEX_INGEST_POISON_FILE);
    std::fs::write(
        &jsonl_path,
        format!(
            "{}\n",
            serde_json::json!({
                "conversation_id": &conversation_id,
                "schema_version_at_quarantine": schema_version,
                "reason": "index-ingest-out-of-memory",
                "cass_version_at_quarantine": current  // already marked
            })
        ),
    )
    .context("write JSONL fixture with current version")?;

    // Structured: still None (save failed during mark — the bug scenario)
    let schema_version_u32 = u32::try_from(schema_version).context("schema_version fits in u32")?;
    let mut state = QuarantineState::default();
    state.record_attempt(
        &QuarantineKey::new(&conversation_id, schema_version_u32),
        "index-ingest-out-of-memory: out of memory",
        chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_000, 0)
            .context("valid quarantine timestamp")?,
    );
    state
        .entries
        .values_mut()
        .next()
        .context("structured record exists")?
        .cass_version_at_quarantine = None; // simulate save failure leaving None
    state
        .save(&data_dir)
        .context("persist structured fixture")?;

    // Before the fix: this would return Some(retry) because the structured
    // None observation wins over the JSONL current-version observation.
    // After the fix: the JSONL current-version observation sets
    // already_current_keys, suppressing the structured None observation.
    let retry =
        stale_index_ingest_quarantine_version_retry(&data_dir).context("version retry check")?;
    assert!(
        retry.is_none(),
        "a conversation already marked current in JSONL must not trigger a retry \
         even if its structured record still has cass_version_at_quarantine=None \
         (cass#261 cross-surface storm guard)"
    );
    Ok(())
}

#[test]
fn stale_index_ingest_quarantine_retry_forces_scan_from_start() {
    assert_eq!(
        non_watch_scan_since_ts(false, false, false, Some(1234)),
        Some(1233)
    );
    assert_eq!(
        non_watch_scan_since_ts(false, false, true, Some(1234)),
        None
    );
    assert_eq!(
        non_watch_scan_since_ts(true, false, false, Some(1234)),
        None
    );
    assert_eq!(
        non_watch_scan_since_ts(false, true, false, Some(1234)),
        None
    );
}

#[test]
fn connector_local_scan_since_ts_protects_newly_enabled_connector_backlog() {
    let global_incremental_since_ts = Some(1233);

    assert_eq!(
        connector_local_scan_since_ts_from_state(global_incremental_since_ts, Some(2000), false),
        Some(1999),
        "an explicit connector watermark wins over the legacy global watermark"
    );
    assert_eq!(
        connector_local_scan_since_ts_from_state(global_incremental_since_ts, None, true),
        global_incremental_since_ts,
        "legacy databases keep incremental behavior for connectors that already have archived rows"
    );
    assert_eq!(
        connector_local_scan_since_ts_from_state(global_incremental_since_ts, None, false),
        None,
        "a connector with no rows and no connector watermark must scan from the beginning"
    );
    assert_eq!(
        connector_local_scan_since_ts_from_state(None, None, true),
        None,
        "full scans already have no cutoff"
    );
    for has_conversations in [false, true] {
        for watermark in [0, 2000, i64::MAX] {
            assert_eq!(
                connector_local_scan_since_ts_from_state(None, Some(watermark), has_conversations),
                None,
                "a saved connector watermark must not restrict a full or repair scan"
            );
        }
    }
    assert_eq!(
        connector_local_scan_since_ts_from_state(global_incremental_since_ts, Some(0), false),
        Some(0),
        "saturating subtraction must not underflow old or corrupt zero-valued markers"
    );
}

#[test]
fn devin_connector_watermark_includes_its_native_boundary_second() -> Result<()> {
    let tmp = TempDir::new()?;
    let storage = FrankenStorage::open(&tmp.path().join("precision.sqlite"))?;
    let watermark = 1_700_001_000_500;
    for name in ["devin", "claude"] {
        storage.set_connector_last_scan_ts(name, watermark)?;
    }
    let factories = get_connector_factories()
        .into_iter()
        .filter(|(name, _)| matches!(*name, "devin" | "claude"))
        .collect::<Vec<_>>();
    assert_eq!(factories.len(), 2);
    let cutoffs = connector_local_scan_since_ts_map(&storage, Some(watermark), &factories)?;
    assert_eq!(cutoffs["devin"], Some(1_700_001_000_000));
    assert_eq!(cutoffs["claude"], Some(watermark - 1));
    let full = connector_local_scan_since_ts_map(&storage, None, &factories)?;
    assert!(full.values().all(Option::is_none));
    for (input, expected) in [
        (0, 0),
        (999, 0),
        (1000, 1000),
        (-1, -1000),
        (-1000, -1000),
        (i64::MIN, i64::MIN),
        (i64::MAX, i64::MAX - i64::MAX.rem_euclid(1000)),
    ] {
        assert_eq!(
            connector_activity_time_lower_bound("devin", Some(input)),
            Some(expected)
        );
        assert_eq!(
            connector_activity_time_lower_bound("claude", Some(input)),
            Some(input)
        );
    }
    Ok(())
}

/// GH #513: compressed Codex rollouts keep old modification times, so an
/// archive whose Codex watermark predates the contract scans Codex
/// without a cutoff until a scan under the contract succeeds.
#[test]
fn a_pending_codex_scan_contract_lifts_only_the_codex_cutoff() -> Result<()> {
    let tmp = TempDir::new()?;
    let storage = FrankenStorage::open(&tmp.path().join("contract.sqlite"))?;
    let watermark = 1_700_001_000_500;
    storage.set_connector_last_scan_ts("claude", watermark)?;
    // The Codex watermark an older build wrote, without the contract.
    storage.raw().execute_compat(
        "INSERT OR REPLACE INTO meta(key, value) VALUES(?1, ?2)",
        &[
            ParamValue::from("last_scan_ts:connector:codex"),
            ParamValue::from(watermark.to_string()),
        ],
    )?;
    let factories = get_connector_factories()
        .into_iter()
        .filter(|(name, _)| matches!(*name, "codex" | "claude"))
        .collect::<Vec<_>>();
    assert_eq!(factories.len(), 2);

    let cutoffs = connector_local_scan_since_ts_map(&storage, Some(watermark), &factories)?;
    assert_eq!(cutoffs["codex"], None, "the backlog needs an uncut scan");
    assert_eq!(cutoffs["claude"], Some(watermark - 1));

    // A succeeded scan whose watermark was held back still settles it.
    record_scan_contract_with_watermark_held(&storage, "codex", false);
    let cutoffs = connector_local_scan_since_ts_map(&storage, Some(watermark), &factories)?;
    assert_eq!(cutoffs["codex"], Some(watermark - 1));
    Ok(())
}

#[test]
fn configured_scan_root_watermark_policy_matches_source_kind() -> Result<()> {
    ensure_since_ts_matches(
        explicit_scan_root_since_ts(
            &ScanRoot::remote(
                PathBuf::from("/remote/fixture/claude"),
                Origin::remote("fixture-host"),
                Some(Platform::Linux),
            ),
            Path::new("/tmp/cass-data"),
            Some(1234),
        ),
        None,
        "configured remote roots may contain already-transferred files older than the global scan watermark; canonical dedupe protects repeats, but the scan must not skip never-promoted mirror sessions",
    )?;
    ensure_since_ts_matches(
        explicit_scan_root_since_ts(
            &ScanRoot::local(PathBuf::from("/tmp/cass-data")),
            Path::new("/tmp/cass-data"),
            Some(1234),
        ),
        Some(1234),
        "the built-in local root should keep normal incremental filtering",
    )?;
    ensure_since_ts_matches(
        explicit_scan_root_since_ts(
            &ScanRoot::local(PathBuf::from("/tmp/backup-root")),
            Path::new("/tmp/cass-data"),
            Some(1234),
        ),
        Some(1234),
        "configured local roots are local history sources and must honor the normal incremental watermark",
    )?;
    ensure_since_ts_matches(
        explicit_scan_root_since_ts(
            &ScanRoot::local(PathBuf::from("/tmp/backup-root")),
            Path::new("/tmp/cass-data"),
            None,
        ),
        None,
        "full rebuilds still scan configured local roots from the beginning",
    )?;
    Ok(())
}

#[test]
#[serial]
fn streaming_consumer_handles_mixed_startup_batches_with_watch_checkpoint_policy() {
    let _wal_guard = set_env("CASS_INDEX_WRITER_WAL_AUTOCHECKPOINT_PAGES", "-1");
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    persist::apply_index_writer_busy_timeout(&storage);
    // Start from steady-state watch policy; startup ingest should flip the
    // connection to deferred checkpoints for the initial import batches.
    persist::apply_index_writer_checkpoint_policy(&storage, false);

    let progress = Arc::new(IndexingProgress::default());
    let flow_limiter = Arc::new(StreamingByteLimiter::new(STREAMING_MAX_BYTES_IN_FLIGHT));
    let (tx, rx) = bounded(STREAMING_CHANNEL_SIZE);

    let amp_convs: Vec<_> = (0..4)
        .map(|conv_idx| {
            large_startup_conv("amp", "amp-startup", conv_idx, 6, 256, 1_700_000_000_000)
        })
        .collect();
    let opencode_convs: Vec<_> = (0..2)
        .map(|conv_idx| {
            large_startup_conv(
                "opencode",
                "opencode-startup",
                conv_idx,
                4,
                256,
                1_700_100_000_000,
            )
        })
        .collect();

    let expected_conversations = (amp_convs.len() + opencode_convs.len()) as i64;
    let expected_messages = (4 * 6 + 2 * 4) as i64;

    send_conversation_batches(&tx, "amp", amp_convs, true);
    send_done(&tx, "amp", true);
    send_conversation_batches(&tx, "opencode", opencode_convs, true);
    send_done(&tx, "opencode", true);
    drop(tx);

    let (discovered, mutations) = run_streaming_consumer(
        rx,
        2,
        &storage,
        &data_dir,
        None,
        flow_limiter,
        &Some(progress.clone()),
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
        true,
        Some(FrankenStorage::now_millis()),
        None,
    )
    .expect("mixed startup ingest should not violate foreign keys");

    assert!(
        discovered.iter().any(|name| name == "amp"),
        "amp should remain marked as discovered"
    );
    assert!(
        discovered.iter().any(|name| name == "opencode"),
        "opencode should remain marked as discovered"
    );

    let conversation_count: i64 = storage
        .raw()
        .query_row_map("SELECT COUNT(*) FROM conversations", &[], |row| {
            row.get_typed(0)
        })
        .unwrap();
    let message_count: i64 = storage
        .raw()
        .query_row_map("SELECT COUNT(*) FROM messages", &[], |row| row.get_typed(0))
        .unwrap();
    let wal_autocheckpoint: i64 = storage
        .raw()
        .query_row_map("PRAGMA wal_autocheckpoint;", &[], |row| row.get_typed(0))
        .unwrap();

    assert_eq!(conversation_count, expected_conversations);
    assert_eq!(message_count, expected_messages);
    assert_eq!(
        mutations,
        CanonicalMutationCounts {
            inserted_conversations: expected_conversations as usize,
            inserted_messages: expected_messages as usize,
            updated_messages: 0,
        }
    );
    assert_eq!(
        wal_autocheckpoint,
        persist::BULK_IMPORT_WAL_AUTOCHECKPOINT_PAGES,
        "startup watch ingest should use the bounded bulk-import WAL checkpoint cadence"
    );
}

#[test]
#[serial]
fn ingest_batch_applies_checkpoint_policy_for_serial_writer_path() {
    let _guard = set_env("CASS_INDEX_WRITER_WAL_AUTOCHECKPOINT_PAGES", "-1");
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("checkpoint-policy.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    let mut index = TantivyIndex::open_or_create(&index_dir(&data_dir).unwrap()).unwrap();

    persist::apply_index_writer_checkpoint_policy(&storage, false);

    let first = vec![norm_conv(Some("checkpoint-a"), vec![norm_msg(0, 1_000)])];
    ingest_batch(
        &storage,
        Some(&mut index),
        &data_dir,
        &first,
        &None,
        LexicalPopulationStrategy::IncrementalInline,
        true,
    )
    .unwrap();

    let rows = storage.raw().query("PRAGMA wal_autocheckpoint;").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get(0).unwrap(),
        &SqliteValue::Integer(persist::BULK_IMPORT_WAL_AUTOCHECKPOINT_PAGES)
    );

    let second = vec![norm_conv(Some("checkpoint-b"), vec![norm_msg(0, 2_000)])];
    ingest_batch(
        &storage,
        Some(&mut index),
        &data_dir,
        &second,
        &None,
        LexicalPopulationStrategy::IncrementalInline,
        false,
    )
    .unwrap();

    let rows = storage.raw().query("PRAGMA wal_autocheckpoint;").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Integer(1000));
}
