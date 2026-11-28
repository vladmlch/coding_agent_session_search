use super::super::*;
use super::helpers::*;
use crate::connectors::ScanContext;
use crate::dependency_pin_correlation::build_metadata::current_for_unit_test;
use crate::franken_sync::SqliteValue;
use crate::franken_sync::compat::{ConnectionExt, ParamValue, RowExt};
use serial_test::serial;
use tempfile::TempDir;

#[test]
fn restore_watch_steady_state_checkpoint_policy_only_reenables_autocheckpoint_for_live_watch() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("watch-steady-state.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);

    persist::apply_index_writer_checkpoint_policy(&storage, true);
    restore_watch_steady_state_checkpoint_policy(&storage, true);

    let rows = storage.raw().query("PRAGMA wal_autocheckpoint;").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Integer(1000));

    persist::apply_index_writer_checkpoint_policy(&storage, true);
    restore_watch_steady_state_checkpoint_policy(&storage, false);

    let rows = storage.raw().query("PRAGMA wal_autocheckpoint;").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get(0).unwrap(),
        &SqliteValue::Integer(persist::BULK_IMPORT_WAL_AUTOCHECKPOINT_PAGES)
    );
}

#[test]
#[serial]
fn final_index_close_restores_checkpoint_policy_after_deferred_bulk_ingest() {
    let _guard = set_env("CASS_INDEX_WRITER_WAL_AUTOCHECKPOINT_PAGES", "-1");
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("final-checkpoint-policy.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);

    persist::apply_index_writer_checkpoint_policy(&storage, true);
    let rows = storage.raw().query("PRAGMA wal_autocheckpoint;").unwrap();
    assert_eq!(
        rows[0].get(0).unwrap(),
        &SqliteValue::Integer(persist::BULK_IMPORT_WAL_AUTOCHECKPOINT_PAGES)
    );

    prepare_storage_for_final_checkpoint(&storage, &db_path, "test index close");

    let rows = storage.raw().query("PRAGMA wal_autocheckpoint;").unwrap();
    assert_eq!(
        rows[0].get(0).unwrap(),
        &SqliteValue::Integer(1000),
        "final close should restore bounded auto-checkpoint policy after deferred bulk ingest"
    );
    assert_eq!(storage.index_writer_checkpoint_pages(), Some(1000));
}

#[test]
#[serial]
fn close_storage_after_index_checkpointing_close_does_not_leave_backfillable_wal_frames() {
    let _guard = set_env("CASS_INDEX_WRITER_WAL_AUTOCHECKPOINT_PAGES", "-1");
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("final-close-checkpoints.db");
    let db_path_str = db_path.to_string_lossy().to_string();
    let storage = FrankenStorage::open(&db_path).unwrap();
    storage.raw().execute("PRAGMA journal_mode = WAL;").unwrap();
    storage
        .raw()
        .execute("CREATE TABLE checkpoint_probe (x INTEGER);")
        .unwrap();
    persist::apply_index_writer_checkpoint_policy(&storage, true);
    storage
        .raw()
        .execute("INSERT INTO checkpoint_probe VALUES (42);")
        .unwrap();

    close_storage_after_index(storage, &db_path, "test index run").unwrap();

    let conn = crate::franken_sync::Connection::open(db_path_str).unwrap();
    let rows = conn.query("PRAGMA wal_checkpoint(FULL);").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get(2).unwrap(),
        &SqliteValue::Integer(0),
        "normal index close should already have checkpointed deferred WAL frames"
    );
    conn.close().unwrap();
}

// -----------------------------------------------------------------
// Card 3 (flat combining): tests that the combine drain preserves
// ordering, caps, backpressure, and Done-flushes-pending semantics.
// The existing streaming_consumer_* tests above verify the disabled
// (default) path byte-identically against the per-message code.
// -----------------------------------------------------------------

#[test]
#[serial]
fn flat_combine_preserves_order_and_counts() {
    let _guard = set_env("CASS_STREAMING_CONSUMER_COMBINE", "1");
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("combine.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    let mut index = TantivyIndex::open_or_create(&index_dir(&data_dir).unwrap()).unwrap();
    let progress = Arc::new(IndexingProgress::default());
    let flow_limiter = Arc::new(StreamingByteLimiter::new(STREAMING_MAX_BYTES_IN_FLIGHT));

    let (tx, rx) = bounded(STREAMING_CHANNEL_SIZE);

    // Pre-queue 3 batches from 2 producers so the drain path sees
    // enough extras to combine. num_producers=2 makes the combine
    // decision rule (num_producers >= 2) fire.
    send_conversation_batches(
        &tx,
        "codex",
        vec![norm_conv(
            Some("combine-a"),
            vec![norm_msg(0, 1_700_000_000_000)],
        )],
        true,
    );
    send_conversation_batches(
        &tx,
        "claude",
        vec![norm_conv(
            Some("combine-b"),
            vec![norm_msg(0, 1_700_000_000_100)],
        )],
        true,
    );
    send_conversation_batches(
        &tx,
        "codex",
        vec![norm_conv(
            Some("combine-c"),
            vec![norm_msg(0, 1_700_000_000_200)],
        )],
        false,
    );
    send_done(&tx, "codex", true);
    send_done(&tx, "claude", true);
    drop(tx);

    let (_discovered, mutations) = run_streaming_consumer(
        rx,
        2, // num_producers: enables combine-decision
        &storage,
        &data_dir,
        Some(&mut index),
        flow_limiter,
        &Some(progress.clone()),
        LexicalPopulationStrategy::IncrementalInline,
        true,
        None,
        None,
    )
    .unwrap();

    // All three conversations must end up persisted regardless of
    // how many drains coalesced them.
    assert_eq!(mutations.inserted_conversations, 3);

    let conversation_count: i64 = storage
        .raw()
        .query_row_map("SELECT COUNT(*) FROM conversations", &[], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(conversation_count, 3);

    // Ordering: external_ids must appear in the DB in the order they
    // were sent on the channel. The combine path concatenates, so
    // this is only true if the drain preserves FIFO semantics.
    use crate::franken_sync::compat::{ConnectionExt, RowExt};
    let external_ids: Vec<String> = storage
        .raw()
        .query_map_collect(
            "SELECT external_id FROM conversations WHERE external_id IS NOT NULL ORDER BY id",
            &[],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(
        external_ids,
        vec![
            "combine-a".to_string(),
            "combine-b".to_string(),
            "combine-c".to_string(),
        ],
    );

    // Per-connector stats: both connectors must be credited with
    // their correct conversation counts even though they were
    // coalesced into one ingest call.
    let stats = progress.stats.lock().unwrap_or_else(|e| e.into_inner());
    let codex_convs = stats
        .connectors
        .iter()
        .find(|c| c.name == "codex")
        .map(|c| c.conversations)
        .unwrap_or(0);
    let claude_convs = stats
        .connectors
        .iter()
        .find(|c| c.name == "claude")
        .map(|c| c.conversations)
        .unwrap_or(0);
    assert_eq!(codex_convs, 2);
    assert_eq!(claude_convs, 1);
}

#[test]
#[serial]
fn flat_combine_disabled_produces_identical_db_state_as_combine_enabled() {
    // PO-C3-1 ordering equivalence: for any interleaving, the final
    // DB state with combine=ON equals the DB state with combine=OFF.
    // We don't compare bit-for-bit at the SQLite level because
    // timestamps / autoincrement ids can differ; instead we compare
    // external_ids in-order, message counts, and message idx values.

    fn run_once(combine: &str) -> (i64, i64, Vec<(String, i64)>) {
        let _guard = set_env("CASS_STREAMING_CONSUMER_COMBINE", combine);
        let tmp = TempDir::new().unwrap();
        let data_dir = tmp.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();
        let db_path = data_dir.join("parity.db");
        let storage = FrankenStorage::open(&db_path).unwrap();
        ensure_fts_schema(&storage);
        let mut index = TantivyIndex::open_or_create(&index_dir(&data_dir).unwrap()).unwrap();
        let progress = Arc::new(IndexingProgress::default());
        let flow_limiter = Arc::new(StreamingByteLimiter::new(STREAMING_MAX_BYTES_IN_FLIGHT));
        let (tx, rx) = bounded(STREAMING_CHANNEL_SIZE);
        // A scripted 6-batch mix from 2 connectors.
        for i in 0..6 {
            let conn = if i % 2 == 0 { "codex" } else { "claude" };
            send_conversation_batches(
                &tx,
                conn,
                vec![norm_conv(
                    Some(&format!("parity-{i}")),
                    vec![norm_msg(0, 1_700_000_000_000 + i as i64)],
                )],
                i < 2,
            );
        }
        send_done(&tx, "codex", true);
        send_done(&tx, "claude", true);
        drop(tx);

        run_streaming_consumer(
            rx,
            2,
            &storage,
            &data_dir,
            Some(&mut index),
            flow_limiter,
            &Some(progress),
            LexicalPopulationStrategy::IncrementalInline,
            true,
            None,
            None,
        )
        .unwrap();

        use crate::franken_sync::compat::{ConnectionExt, RowExt};
        let conv_count: i64 = storage
            .raw()
            .query_row_map("SELECT COUNT(*) FROM conversations", &[], |row| {
                row.get_typed(0)
            })
            .unwrap();
        let msg_count: i64 = storage
            .raw()
            .query_row_map("SELECT COUNT(*) FROM messages", &[], |row| row.get_typed(0))
            .unwrap();
        let rows: Vec<(String, i64)> = storage
            .raw()
            .query_map_collect(
                "SELECT external_id, started_at FROM conversations ORDER BY id",
                &[],
                |row| {
                    let ext: Option<String> = row.get_typed(0)?;
                    let started: Option<i64> = row.get_typed(1)?;
                    Ok((ext.unwrap_or_default(), started.unwrap_or(0)))
                },
            )
            .unwrap();
        (conv_count, msg_count, rows)
    }

    let off = run_once("0");
    let on = run_once("1");
    assert_eq!(off.0, on.0, "conversation count must match");
    assert_eq!(off.1, on.1, "message count must match");
    assert_eq!(
        off.2, on.2,
        "per-conversation external_id + started_at must appear in the same order"
    );
}

#[test]
#[serial]
fn flat_combine_disabled_leaves_per_message_path_untouched() {
    // PO-C3-6: when env=0, the drain loop is never taken, so the
    // code path is byte-identical to the pre-Card-3 version. This
    // test just confirms the existing (pre-combine) semantics still
    // work when combine is explicitly disabled.
    let _guard = set_env("CASS_STREAMING_CONSUMER_COMBINE", "0");
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("disabled.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    let mut index = TantivyIndex::open_or_create(&index_dir(&data_dir).unwrap()).unwrap();
    let progress = Arc::new(IndexingProgress::default());
    let flow_limiter = Arc::new(StreamingByteLimiter::new(STREAMING_MAX_BYTES_IN_FLIGHT));
    let (tx, rx) = bounded(STREAMING_CHANNEL_SIZE);
    send_conversation_batches(
        &tx,
        "codex",
        vec![norm_conv(
            Some("single"),
            vec![norm_msg(0, 1_700_000_000_000)],
        )],
        true,
    );
    send_done(&tx, "codex", true);
    drop(tx);
    let (discovered, mutations) = run_streaming_consumer(
        rx,
        1,
        &storage,
        &data_dir,
        Some(&mut index),
        flow_limiter,
        &Some(progress.clone()),
        LexicalPopulationStrategy::IncrementalInline,
        true,
        None,
        None,
    )
    .unwrap();
    assert_eq!(discovered, vec!["codex".to_string()]);
    assert_eq!(mutations.inserted_conversations, 1);
}

#[test]
#[serial]
fn flat_combine_single_producer_skips_drain() {
    // The combine-decision rule requires num_producers >= 2. With
    // exactly one producer we must behave like the per-message
    // path even when env=1 is set.
    let _guard = set_env("CASS_STREAMING_CONSUMER_COMBINE", "1");
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("single-prod.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    let mut index = TantivyIndex::open_or_create(&index_dir(&data_dir).unwrap()).unwrap();
    let progress = Arc::new(IndexingProgress::default());
    let flow_limiter = Arc::new(StreamingByteLimiter::new(STREAMING_MAX_BYTES_IN_FLIGHT));
    let (tx, rx) = bounded(STREAMING_CHANNEL_SIZE);
    for i in 0..3 {
        send_conversation_batches(
            &tx,
            "codex",
            vec![norm_conv(
                Some(&format!("solo-{i}")),
                vec![norm_msg(0, 1_700_000_000_000 + i)],
            )],
            i == 0,
        );
    }
    send_done(&tx, "codex", true);
    drop(tx);
    let (_discovered, mutations) = run_streaming_consumer(
        rx,
        1, // single producer → drain loop body is bypassed
        &storage,
        &data_dir,
        Some(&mut index),
        flow_limiter,
        &Some(progress.clone()),
        LexicalPopulationStrategy::IncrementalInline,
        true,
        None,
        None,
    )
    .unwrap();
    assert_eq!(mutations.inserted_conversations, 3);
}

#[test]
#[serial]
fn streaming_combine_env_respects_truthy_parsing() {
    // Post-flip semantics: default is ON. Only explicit opt-out
    // keywords disable it. Anything else (unset, garbage, "1", …) is
    // treated as enabled. Operators who want the legacy per-message
    // path set CASS_STREAMING_CONSUMER_COMBINE=0.
    let prior = std::env::var("CASS_STREAMING_CONSUMER_COMBINE").ok();
    // SAFETY: test-local env mutation; one-shot and restored below.
    unsafe {
        std::env::remove_var("CASS_STREAMING_CONSUMER_COMBINE");
    }
    assert!(
        streaming_combine_enabled(),
        "unset env must default to enabled"
    );
    for truthy in ["1", "true", "yes", "on", "TRUE", "Yes"] {
        unsafe {
            std::env::set_var("CASS_STREAMING_CONSUMER_COMBINE", truthy);
        }
        assert!(
            streaming_combine_enabled(),
            "expected `{truthy}` to enable combine"
        );
    }
    for falsy in ["0", "false", "no", "off", "OFF"] {
        unsafe {
            std::env::set_var("CASS_STREAMING_CONSUMER_COMBINE", falsy);
        }
        assert!(
            !streaming_combine_enabled(),
            "expected `{falsy}` to disable combine"
        );
    }
    // Garbage / empty falls through to default-enabled.
    for pass_through in ["", "maybe", "idk"] {
        unsafe {
            std::env::set_var("CASS_STREAMING_CONSUMER_COMBINE", pass_through);
        }
        assert!(
            streaming_combine_enabled(),
            "non-off values fall through to default-enabled; `{pass_through}` did not"
        );
    }
    unsafe {
        std::env::remove_var("CASS_STREAMING_CONSUMER_COMBINE");
    }
    if let Some(v) = prior {
        unsafe {
            std::env::set_var("CASS_STREAMING_CONSUMER_COMBINE", v);
        }
    }
}

#[test]
#[serial]
fn streaming_combine_max_messages_clamps_to_valid_range() {
    let prior = std::env::var("CASS_STREAMING_COMBINE_MAX").ok();
    // SAFETY: test-local env mutation.
    unsafe {
        std::env::set_var("CASS_STREAMING_COMBINE_MAX", "0");
    }
    assert_eq!(
        streaming_combine_max_messages(),
        64,
        "zero must not be honored; default"
    );
    unsafe {
        std::env::set_var("CASS_STREAMING_COMBINE_MAX", "1000000");
    }
    assert_eq!(
        streaming_combine_max_messages(),
        1024,
        "upper clamp is 1024"
    );
    unsafe {
        std::env::set_var("CASS_STREAMING_COMBINE_MAX", "16");
    }
    assert_eq!(streaming_combine_max_messages(), 16);
    unsafe {
        std::env::remove_var("CASS_STREAMING_COMBINE_MAX");
    }
    assert_eq!(streaming_combine_max_messages(), 64);
    if let Some(v) = prior {
        unsafe {
            std::env::set_var("CASS_STREAMING_COMBINE_MAX", v);
        }
    }
}

#[test]
fn streaming_producer_records_remote_scan_errors_in_connector_stats() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    let mut index = TantivyIndex::open_or_create(&index_dir(&data_dir).unwrap()).unwrap();
    let progress = Arc::new(IndexingProgress::default());
    let (tx, rx) = bounded(STREAMING_CHANNEL_SIZE);
    let flow_limiter = Arc::new(StreamingByteLimiter::new(STREAMING_MAX_BYTES_IN_FLIGHT));
    let remote_root_path = PathBuf::from("/remote/fixture/claude");
    let handle = spawn_connector_producer(
        "claude",
        detected_remote_failure_connector_factory,
        tx,
        StreamingProducerConfig {
            source_ledger: Arc::new(HashMap::new()),
            raw_source_exclusions: Arc::new(RawSourceExclusions::default()),
            flow_limiter: flow_limiter.clone(),
            data_dir: data_dir.clone(),
            additional_scan_roots: vec![ScanRoot::remote(
                remote_root_path.clone(),
                Origin::remote("fixture-host"),
                Some(crate::sources::config::Platform::Linux),
            )],
            local_connector_roots: None,
            since_ts: None,
            local_since_ts_by_connector: Arc::new(HashMap::new()),
            progress: Some(progress.clone()),
            active_source_filter: Arc::new(ActiveSessionSourceFilter::default()),
        },
    );

    let (discovered, mutations) = run_streaming_consumer(
        rx,
        1,
        &storage,
        &data_dir,
        Some(&mut index),
        flow_limiter,
        &Some(progress.clone()),
        LexicalPopulationStrategy::IncrementalInline,
        true,
        None,
        None,
    )
    .unwrap();
    handle.join().unwrap();

    assert_eq!(discovered, vec!["claude".to_string()]);
    assert_eq!(mutations, CanonicalMutationCounts::default());

    let stats = progress.stats.lock().unwrap_or_else(|e| e.into_inner());
    let connector = stats
        .connectors
        .iter()
        .find(|connector| connector.name == "claude")
        .expect("claude connector stats should exist");
    assert_eq!(
        connector.error.as_deref(),
        Some("remote scan failed for /remote/fixture/claude: remote exploded")
    );
    assert!(
        mutations.scanned_connectors.is_empty(),
        "remote scan errors must prevent per-connector watermark advancement"
    );
    assert!(mutations.scan_had_errors);
}

#[test]
#[cfg(unix)]
fn gh426_source_observation_rejects_same_size_restored_mtime_rewrite() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("source.jsonl");
    std::fs::write(&path, b"before").unwrap();
    let modified = std::fs::metadata(&path).unwrap().modified().unwrap();
    let source = DiscoveredSourceFile::new(
        "claude_code",
        &ScanRoot::local(temp.path().to_path_buf()),
        path.clone(),
        crate::connectors::DiscoveredSourceRole::PrimarySessionLog,
        true,
    )
    .with_fs_metadata();
    let before = source_file_observation(&path).unwrap();
    let saved=serde_json::json!({"primary":before,"dependencies":[source_file_observation(temp.path()).unwrap()],
        "producer_contract":current_for_unit_test().ingest_contract}).to_string();
    std::thread::sleep(Duration::from_millis(2));
    std::fs::write(&path, b"after!").unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_times(std::fs::FileTimes::new().set_modified(modified))
        .unwrap();
    assert!(
        !source.fs_metadata_changed(),
        "planted rewrite must evade upstream millisecond/size guard"
    );
    assert_ne!(source_file_observation(&path).unwrap(), before);
    assert!(!source_ledger_matches(&saved, &source));
}

#[test]
fn gh426_source_observation_rejects_changed_primary_sidecar_and_new_dependency() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("source.jsonl");
    let sidecar = temp.path().join("source.json");
    std::fs::write(&path, b"source").unwrap();
    std::fs::write(&sidecar, b"sidecar").unwrap();
    let source = DiscoveredSourceFile::new(
        "kiro",
        &ScanRoot::local(temp.path().to_path_buf()),
        path.clone(),
        crate::connectors::DiscoveredSourceRole::PrimarySessionLog,
        true,
    )
    .with_fs_metadata();
    let snapshot = || {
        serde_json::json!({"primary":source_file_observation(&path).unwrap(),
        "producer_contract":current_for_unit_test().ingest_contract,
        "dependencies":[source_file_observation(temp.path()).unwrap(),source_file_observation(&sidecar).unwrap()]}).to_string()
    };
    let saved = snapshot();
    assert!(source_ledger_matches(&saved, &source));
    std::fs::write(&sidecar, b"changed sidecar").unwrap();
    assert!(!source_ledger_matches(&saved, &source));
    let saved = snapshot();
    std::fs::write(&path, b"changed primary").unwrap();
    assert!(!source_ledger_matches(&saved, &source));
    let saved = snapshot();
    std::fs::write(temp.path().join("new-required-sidecar"), b"new").unwrap();
    assert!(!source_ledger_matches(&saved, &source));
    assert!(!source_ledger_matches("{}", &source));
}

#[test]
fn streaming_configured_scan_roots_ignore_global_watermark() -> Result<()> {
    let tmp = TempDir::new()?;
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir)?;
    let remote_root_path = tmp.path().join("remote-mirror").join("claude");
    std::fs::create_dir_all(&remote_root_path)?;
    std::fs::write(
        remote_root_path.join("remote-watermark.jsonl"),
        b"{\"role\":\"user\",\"content\":\"remote\"}\n",
    )?;

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path)?;
    ensure_fts_schema(&storage);
    let index_path = index_dir(&data_dir)?;
    let mut index = TantivyIndex::open_or_create(&index_path)?;
    let progress = Arc::new(IndexingProgress::default());
    let opts = IndexOptions {
        full: false,
        force_rebuild: false,
        watch: false,
        watch_once_paths: None,
        db_path,
        data_dir: data_dir.clone(),
        semantic: false,
        build_hnsw: false,
        embedder: String::from("fastembed"),
        progress: Some(progress),
        watch_interval_secs: 30,
    };

    let mutations = run_streaming_index_with_connector_factories(
        &storage,
        Some(&mut index),
        &opts,
        Some(i64::MAX),
        LexicalPopulationStrategy::IncrementalInline,
        vec![ScanRoot::remote(
            remote_root_path,
            Origin::remote("fixture-host"),
            Some(Platform::Linux),
        )],
        None,
        vec![("claude", watermark_sensitive_remote_connector_factory)],
        Some(FrankenStorage::now_millis()),
        None,
    )
    .context("configured remote roots should scan from the root despite the global watermark")?;

    ensure_canonical_mutations(
        mutations.canonical_mutations,
        CanonicalMutationCounts {
            inserted_conversations: 1,
            inserted_messages: 1,
            updated_messages: 0,
        },
        "streaming configured scan roots",
    )?;
    Ok(())
}

#[test]
fn streaming_configured_local_scan_roots_honor_connector_watermark() -> Result<()> {
    let tmp = TempDir::new()?;
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir)?;
    let local_root_path = tmp.path().join("backup-local").join("claude");
    std::fs::create_dir_all(&local_root_path)?;
    std::fs::write(
        local_root_path.join("remote-watermark.jsonl"),
        b"{\"role\":\"user\",\"content\":\"local backup\"}\n",
    )?;

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path)?;
    ensure_fts_schema(&storage);
    storage.set_connector_last_scan_ts("claude", i64::MAX)?;
    let local_since_by_connector = connector_local_scan_since_ts_map(
        &storage,
        Some(1234),
        &[(
            "claude",
            watermark_sensitive_remote_connector_factory as ConnectorFactory,
        )],
    )?;
    assert_eq!(
        local_since_by_connector.get("claude").copied().flatten(),
        Some(i64::MAX - 1),
        "configured-root scan must see the connector-specific watermark"
    );
    assert_eq!(
        explicit_scan_root_since_ts(
            &configured_local_scan_root(local_root_path.clone()),
            &data_dir,
            local_since_by_connector.get("claude").copied().flatten(),
        ),
        Some(i64::MAX - 1),
        "configured local roots must preserve the connector-specific cutoff"
    );
    let index_path = index_dir(&data_dir)?;
    let mut index = TantivyIndex::open_or_create(&index_path)?;
    let progress = Arc::new(IndexingProgress::default());
    let opts = IndexOptions {
        full: false,
        force_rebuild: false,
        watch: false,
        watch_once_paths: None,
        db_path,
        data_dir: data_dir.clone(),
        semantic: false,
        build_hnsw: false,
        embedder: String::from("fastembed"),
        progress: Some(progress),
        watch_interval_secs: 30,
    };

    let mutations = run_streaming_index_with_connector_factories(
        &storage,
        Some(&mut index),
        &opts,
        Some(i64::MAX),
        LexicalPopulationStrategy::IncrementalInline,
        vec![configured_local_scan_root(local_root_path)],
        None,
        vec![("claude", watermark_sensitive_remote_connector_factory)],
        Some(FrankenStorage::now_millis()),
        None,
    )
    .context("configured local roots should keep the connector incremental watermark")?;

    ensure_canonical_mutations(
        mutations.canonical_mutations,
        CanonicalMutationCounts::default(),
        "streaming configured local scan roots",
    )?;
    Ok(())
}

#[test]
fn streaming_index_fails_closed_when_producer_panics() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    let mut index = TantivyIndex::open_or_create(&index_dir(&data_dir).unwrap()).unwrap();
    let progress = Arc::new(IndexingProgress::default());
    let opts = IndexOptions {
        full: false,
        force_rebuild: false,
        watch: false,
        watch_once_paths: None,
        db_path,
        data_dir,
        semantic: false,
        build_hnsw: false,
        embedder: "fastembed".to_string(),
        progress: Some(progress.clone()),
        watch_interval_secs: 30,
    };

    let error = run_streaming_index_with_connector_factories(
        &storage,
        Some(&mut index),
        &opts,
        None,
        LexicalPopulationStrategy::IncrementalInline,
        Vec::new(),
        None,
        vec![("claude", panic_connector_factory)],
        Some(FrankenStorage::now_millis()),
        None,
    )
    .expect_err("producer panic should abort streaming indexing");
    let message = error.to_string();
    assert!(
        message.contains("streaming producer thread panicked"),
        "panic should surface in the returned error: {message}"
    );
    assert!(
        message.contains("claude: connector panic during local scan"),
        "returned error should name the failing connector and panic: {message}"
    );
    assert_eq!(
        progress
            .last_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_deref(),
        Some(message.as_str()),
        "progress tracker should expose the real panic instead of pretending indexing succeeded"
    );
}

#[test]
fn gh426_batch_boundary_stats_survive_undiscovered_fallback() -> Result<()> {
    let temp = TempDir::new()?;
    let root = temp.path().join("claude/projects/resume");
    fs::create_dir_all(&root)?;
    let source = root.join("summary.jsonl");
    let messages = (0..2)
        .map(|idx| {
            serde_json::json!({
                "type":"user", "sessionId":"boundary-summary", "uuid":format!("summary-{idx}"),
                "timestamp":"2026-08-01T10:00:00Z", "cwd":"/work/resume",
                "message":{"role":"user", "content":format!("boundary summary message {idx}")}
            })
            .to_string()
        })
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(&source, format!("{messages}\n"))?;
    let factories: Vec<_> = configured_connector_factories()
        .into_iter()
        .filter(|(name, _)| matches!(*name, "claude" | "qwen"))
        .collect();
    assert_eq!(factories.len(), 2);
    for (name, factory) in &factories {
        assert_eq!(factory().supports_source_boundaries(), *name == "claude");
    }
    let data_dir = temp.path().join("data");
    fs::create_dir_all(&data_dir)?;
    let db_path = data_dir.join("archive.db");
    let storage = FrankenStorage::open(&db_path)?;
    let progress = Arc::new(IndexingProgress::default());
    let opts = IndexOptions {
        full: false,
        force_rebuild: false,
        watch: false,
        watch_once_paths: None,
        db_path,
        data_dir,
        semantic: false,
        build_hnsw: false,
        embedder: "fastembed".into(),
        progress: Some(Arc::clone(&progress)),
        watch_interval_secs: 30,
    };
    // Real Claude parsing and SourceComplete ingestion, followed by a
    // real Qwen factory with no roots in the closed scan universe.
    let outcome = run_batch_index_with_connector_factories(
        &storage,
        None,
        &opts,
        None,
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
        Vec::new(),
        Some(Arc::new(HashMap::from([
            ("claude".to_string(), vec![source]),
            ("qwen".to_string(), Vec::new()),
        ]))),
        factories,
        None,
        None,
    )?;
    assert_eq!(outcome.inserted_conversations, 1);
    assert_eq!(outcome.inserted_messages, 2);
    assert_eq!(storage.source_ingest_ledger_entries()?.len(), 1);
    assert_eq!(count_total_conversations_exact(&storage)?, 1);
    assert_eq!(count_total_messages_exact(&storage)?, 2);
    let stats = progress.stats.lock().unwrap();
    assert_eq!((stats.total_conversations, stats.total_messages), (1, 2));
    assert_eq!(stats.agents_discovered, vec!["claude"]);
    assert_eq!(stats.connectors.len(), 1);
    assert_eq!(stats.connectors[0].name, "claude");
    assert_eq!(
        (
            stats.connectors[0].conversations,
            stats.connectors[0].messages
        ),
        (1, 2)
    );
    assert!(stats.scan_ms >= stats.connectors[0].scan_ms);
    assert_eq!(stats.connector_summary["claude"].indexed, 1);
    drop(stats);
    assert_eq!(progress.discovered_agents.load(Ordering::Relaxed), 1);
    assert_eq!(
        *progress.discovered_agent_names.lock().unwrap(),
        vec!["claude"]
    );
    assert_eq!(
        progress.phase.load(Ordering::Relaxed),
        INDEX_PHASE_LEXICAL_INDEXING
    );
    assert_eq!(progress.total.load(Ordering::Relaxed), 1);
    assert_eq!(progress.current.load(Ordering::Relaxed), 1);
    assert!(progress.total_is_final.load(Ordering::Relaxed));
    Ok(())
}

#[test]
fn gh426_non_watch_batch_stop_preserves_committed_prefix_and_resumes() -> Result<()> {
    let temp = TempDir::new()?;
    let root = temp.path().join(".codex/sessions/2026/05/08");
    fs::create_dir_all(&root)?;
    for source in 0..3 {
        fs::write(
            root.join(format!("rollout-2026-05-08T00-00-0{source}-00000000-0000-4000-8000-00000000000{source}.jsonl")),
            format!(
                concat!(
                    "{{\"timestamp\":\"2026-05-08T23:09:00.000Z\",\"type\":\"session_meta\",\"payload\":{{\"id\":\"s-{s}\",\"cwd\":\"/tmp\"}}}}\n",
                    "{{\"timestamp\":\"2026-05-08T23:09:01.000Z\",\"type\":\"response_item\",\"payload\":{{\"type\":\"message\",\"role\":\"user\",\"content\":[{{\"type\":\"input_text\",\"text\":\"retained fallback prompt {s}\"}}]}}}}\n",
                    "{{\"timestamp\":\"2026-05-08T23:09:02.000Z\",\"type\":\"response_item\",\"payload\":{{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{{\"type\":\"output_text\",\"text\":\"retained fallback answer {s}\"}}]}}}}\n"
                ),
                s = source
            ),
        )?;
    }
    let data_dir = temp.path().join("data");
    fs::create_dir_all(&data_dir)?;
    let storage = FrankenStorage::open(&data_dir.join("archive.db"))?;
    let context = ScanContext::with_roots(data_dir.clone(), vec![ScanRoot::local(root)], None);
    let conversations = CodexConnector::new().scan(&context)?;
    assert_eq!(conversations.len(), 3);
    assert!(
        conversations
            .iter()
            .all(|conversation| conversation.messages.len() == 2)
    );
    let progress = Some(Arc::new(IndexingProgress::default()));
    let first = ingest_non_watch_batch_with_oom_split(
        &storage,
        None,
        &data_dir,
        &conversations[..1],
        &progress,
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
        true,
        None,
    )?;
    assert_eq!(
        (first.inserted_conversations, first.inserted_messages),
        (1, 2)
    );
    let saved = storage.list_conversations(10, 0)?;
    let id = saved[0].id.unwrap();
    let before: Vec<_> = storage
        .fetch_messages(id)?
        .iter()
        .map(|message| message.id)
        .collect();
    progress.as_ref().unwrap().request_stop();
    let error = ingest_non_watch_batch_with_oom_split(
        &storage,
        None,
        &data_dir,
        &conversations[1..],
        &progress,
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
        true,
        None,
    )
    .expect_err("stop at the next fallback batch must preserve the committed prefix");
    assert!(error.downcast_ref::<IndexInterrupted>().is_some());
    assert_eq!(count_total_conversations_exact(&storage)?, 1);
    assert_eq!(count_total_messages_exact(&storage)?, 2);
    assert_eq!(
        progress.as_ref().unwrap().current.load(Ordering::Relaxed),
        1
    );
    let resumed = ingest_non_watch_batch_with_oom_split(
        &storage,
        None,
        &data_dir,
        &conversations,
        &Some(Arc::new(IndexingProgress::default())),
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
        true,
        None,
    )?;
    assert_eq!(
        (resumed.inserted_conversations, resumed.inserted_messages),
        (2, 4)
    );
    assert_eq!(count_total_conversations_exact(&storage)?, 3);
    assert_eq!(count_total_messages_exact(&storage)?, 6);
    let after: Vec<_> = storage
        .fetch_messages(id)?
        .iter()
        .map(|message| message.id)
        .collect();
    assert_eq!(after, before);
    Ok(())
}

#[test]
fn batch_configured_scan_roots_ignore_global_watermark() -> Result<()> {
    let tmp = TempDir::new()?;
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir)?;
    let remote_root_path = tmp.path().join("remote-mirror").join("claude");
    std::fs::create_dir_all(&remote_root_path)?;
    std::fs::write(
        remote_root_path.join("remote-watermark.jsonl"),
        b"{\"role\":\"user\",\"content\":\"remote\"}\n",
    )?;

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path)?;
    ensure_fts_schema(&storage);
    let progress = Arc::new(IndexingProgress::default());
    let opts = IndexOptions {
        full: false,
        force_rebuild: false,
        watch: false,
        watch_once_paths: None,
        db_path,
        data_dir: data_dir.clone(),
        semantic: false,
        build_hnsw: false,
        embedder: String::from("fastembed"),
        progress: Some(progress),
        watch_interval_secs: 30,
    };

    let mutations = run_batch_index_with_connector_factories(
        &storage,
        None,
        &opts,
        Some(i64::MAX),
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
        vec![ScanRoot::remote(
            remote_root_path,
            Origin::remote("fixture-host"),
            Some(Platform::Linux),
        )],
        None,
        vec![("claude", watermark_sensitive_remote_connector_factory)],
        Some(FrankenStorage::now_millis()),
        None,
    )
    .context("configured remote roots should scan from the root despite the global watermark")?;

    ensure_canonical_mutations(
        mutations.canonical_mutations,
        CanonicalMutationCounts {
            inserted_conversations: 1,
            inserted_messages: 1,
            updated_messages: 0,
        },
        "batch configured scan roots",
    )?;
    Ok(())
}

#[test]
fn batch_configured_local_scan_roots_honor_connector_watermark() -> Result<()> {
    let tmp = TempDir::new()?;
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir)?;
    let local_root_path = tmp.path().join("backup-local").join("claude");
    std::fs::create_dir_all(&local_root_path)?;
    std::fs::write(
        local_root_path.join("remote-watermark.jsonl"),
        b"{\"role\":\"user\",\"content\":\"local backup\"}\n",
    )?;

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path)?;
    ensure_fts_schema(&storage);
    storage.set_connector_last_scan_ts("claude", i64::MAX)?;
    let local_since_by_connector = connector_local_scan_since_ts_map(
        &storage,
        Some(1234),
        &[(
            "claude",
            watermark_sensitive_remote_connector_factory as ConnectorFactory,
        )],
    )?;
    assert_eq!(
        local_since_by_connector.get("claude").copied().flatten(),
        Some(i64::MAX - 1),
        "configured-root scan must see the connector-specific watermark"
    );
    assert_eq!(
        explicit_scan_root_since_ts(
            &configured_local_scan_root(local_root_path.clone()),
            &data_dir,
            local_since_by_connector.get("claude").copied().flatten(),
        ),
        Some(i64::MAX - 1),
        "configured local roots must preserve the connector-specific cutoff"
    );
    let progress = Arc::new(IndexingProgress::default());
    let opts = IndexOptions {
        full: false,
        force_rebuild: false,
        watch: false,
        watch_once_paths: None,
        db_path,
        data_dir: data_dir.clone(),
        semantic: false,
        build_hnsw: false,
        embedder: String::from("fastembed"),
        progress: Some(progress),
        watch_interval_secs: 30,
    };

    let mutations = run_batch_index_with_connector_factories(
        &storage,
        None,
        &opts,
        Some(i64::MAX),
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
        vec![configured_local_scan_root(local_root_path)],
        None,
        vec![("claude", watermark_sensitive_remote_connector_factory)],
        Some(FrankenStorage::now_millis()),
        None,
    )
    .context("configured local roots should keep the connector incremental watermark")?;

    ensure_canonical_mutations(
        mutations.canonical_mutations,
        CanonicalMutationCounts::default(),
        "batch configured local scan roots",
    )?;
    Ok(())
}

#[test]
#[serial]
fn batch_index_does_not_watermark_failed_discovered_connector() -> Result<()> {
    let tmp = TempDir::new()?;
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir)?;
    let local_root = tmp.path().join("codex").join("sessions");
    std::fs::create_dir_all(&local_root)?;
    *FAILING_EXPLICIT_FILE_ROOT
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = Some(local_root);

    let result = (|| -> Result<()> {
        let db_path = data_dir.join("db.sqlite");
        let storage = FrankenStorage::open(&db_path)?;
        ensure_fts_schema(&storage);
        let progress = Arc::new(IndexingProgress::default());
        let opts = IndexOptions {
            full: false,
            force_rebuild: false,
            watch: false,
            watch_once_paths: None,
            db_path,
            data_dir,
            semantic: false,
            build_hnsw: false,
            embedder: String::from("fastembed"),
            progress: Some(progress.clone()),
            watch_interval_secs: 30,
        };

        let mutations = run_batch_index_with_connector_factories(
            &storage,
            None,
            &opts,
            None,
            LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
            Vec::new(),
            None,
            vec![("codex", failing_explicit_file_root_connector_factory)],
            Some(FrankenStorage::now_millis()),
            None,
        )?;

        assert_eq!(
            mutations.canonical_mutations,
            CanonicalMutationCounts::default()
        );
        assert!(
            mutations.scanned_connectors.is_empty(),
            "failed batch scans must not advance per-connector watermarks"
        );
        assert!(mutations.scan_had_errors);
        let stats = progress.stats.lock().unwrap_or_else(|e| e.into_inner());
        assert_eq!(stats.agents_discovered, vec!["codex".to_string()]);
        assert_eq!(
            stats
                .connectors
                .iter()
                .find(|connector| connector.name == "codex")
                .and_then(|connector| connector.error.as_deref()),
            Some("connector parse failed after source discovery")
        );
        Ok(())
    })();

    *FAILING_EXPLICIT_FILE_ROOT
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = None;
    result
}

#[test]
#[serial]
fn gh426_batch_watermarks_only_fully_persisted_connectors_without_active_sources() -> Result<()> {
    let _active_skip_reset = ActiveSessionSkipReset;
    let _fixture_reset = BatchWatermarkFixtureRootReset;
    let tmp = TempDir::new()?;
    let data_dir = tmp.path().join("data");
    let fixture_root = tmp.path().join("sessions");
    std::fs::create_dir_all(&data_dir)?;
    std::fs::create_dir_all(&fixture_root)?;
    let active_path = fixture_root.join("active.jsonl");
    let safe_path = fixture_root.join("safe.jsonl");
    std::fs::write(&active_path, b"active\n")?;
    std::fs::write(&safe_path, b"safe\n")?;
    *BATCH_WATERMARK_FIXTURE_ROOT
        .lock()
        .unwrap_or_else(|error| error.into_inner()) = Some(fixture_root);

    let active_paths = std::env::join_paths([active_path.as_os_str()])?;
    let _active_guard = set_env(
        "CASS_TEST_ACTIVE_SESSION_SOURCE_PATHS",
        active_paths.to_string_lossy().as_ref(),
    );
    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path)?;
    ensure_fts_schema(&storage);
    let opts = IndexOptions {
        full: false,
        force_rebuild: false,
        watch: false,
        watch_once_paths: None,
        db_path,
        data_dir,
        semantic: false,
        build_hnsw: false,
        embedder: "fastembed".to_string(),
        progress: None,
        watch_interval_secs: 30,
    };
    let scan_start_ts = 1_700_000_654_321_i64;

    let outcome = run_batch_index_with_connector_factories(
        &storage,
        None,
        &opts,
        None,
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
        Vec::new(),
        None,
        vec![
            (
                "claude",
                active_batch_watermark_connector_factory as ConnectorFactory,
            ),
            ("codex", safe_batch_watermark_connector_factory),
        ],
        Some(scan_start_ts),
        None,
    )?;

    assert_eq!(
        outcome.scanned_connectors,
        BTreeSet::from(["codex".to_string()]),
        "the active Claude source must not freeze the completed Codex connector"
    );
    assert_eq!(storage.get_connector_last_scan_ts("claude")?, None);
    assert_eq!(
        storage.get_connector_last_scan_ts("codex")?,
        Some(scan_start_ts),
        "batch mode must persist each safe connector watermark only after its rows commit"
    );
    let oom_guard = inject_non_watch_ingest_test_oom(1);
    let _pressure_guard = set_env("CASS_WATCH_OOM_REAL_PRESSURE_RESERVE_BYTES", "0");
    let next_scan_ts = scan_start_ts + 1;
    let deferred = run_batch_index_with_connector_factories(
        &storage,
        None,
        &opts,
        None,
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
        Vec::new(),
        None,
        vec![("codex", safe_batch_watermark_connector_factory)],
        Some(next_scan_ts),
        None,
    )?;
    assert!(deferred.scan_had_errors);
    assert!(deferred.scanned_connectors.is_empty());
    assert_eq!(deferred.deferred_sources, BTreeSet::from([safe_path]));
    assert_eq!(
        storage.get_connector_last_scan_ts("codex")?,
        Some(scan_start_ts)
    );
    drop(oom_guard);
    let retried = run_batch_index_with_connector_factories(
        &storage,
        None,
        &opts,
        None,
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
        Vec::new(),
        None,
        vec![("codex", safe_batch_watermark_connector_factory)],
        Some(next_scan_ts),
        None,
    )?;
    assert!(!retried.scan_had_errors);
    assert_eq!(
        retried.scanned_connectors,
        BTreeSet::from(["codex".to_string()])
    );
    assert_eq!(
        storage.get_connector_last_scan_ts("codex")?,
        Some(next_scan_ts)
    );
    assert_eq!(storage.total_message_count()?, 1);
    Ok(())
}

#[test]
fn batch_index_can_defer_authoritative_lexical_updates_without_tantivy_writer() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    let progress = Arc::new(IndexingProgress::default());
    let opts = IndexOptions {
        full: true,
        force_rebuild: false,
        watch: false,
        watch_once_paths: None,
        db_path,
        data_dir: data_dir.clone(),
        semantic: false,
        build_hnsw: false,
        embedder: "fastembed".to_string(),
        progress: Some(progress.clone()),
        watch_interval_secs: 30,
    };

    let mutations = run_batch_index_with_connector_factories(
        &storage,
        None,
        &opts,
        None,
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
        Vec::new(),
        None,
        vec![("codex", deferred_batch_connector_factory)],
        Some(FrankenStorage::now_millis()),
        None,
    )
    .expect("deferred batch ingest should not require a Tantivy writer");

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
    let stats = progress.stats.lock().unwrap_or_else(|e| e.into_inner());

    assert_eq!(
        mutations,
        CanonicalMutationCounts {
            inserted_conversations: 1,
            inserted_messages: 2,
            updated_messages: 0,
        }
    );
    assert_eq!(conversation_count, 1);
    assert_eq!(message_count, 2);
    assert_eq!(stats.total_conversations, 1);
    assert_eq!(stats.total_messages, 2);
    assert!(
        !index_dir(&data_dir).unwrap().join("meta.json").exists(),
        "deferred batch ingest should not materialize a live Tantivy index before the authoritative rebuild"
    );
}

#[test]
fn streaming_producer_stops_after_consumer_disconnect() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let counter = Arc::new(AtomicUsize::new(0));
    *DISCONNECT_TEST_COUNTER
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = Some(counter.clone());

    let (tx, rx) = bounded(STREAMING_CHANNEL_SIZE);
    drop(rx);

    let handle = spawn_connector_producer(
        "claude",
        disconnect_aware_connector_factory,
        tx,
        StreamingProducerConfig {
            source_ledger: Arc::new(HashMap::new()),
            raw_source_exclusions: Arc::new(RawSourceExclusions::default()),
            flow_limiter: Arc::new(StreamingByteLimiter::new(STREAMING_MAX_BYTES_IN_FLIGHT)),
            data_dir,
            additional_scan_roots: vec![ScanRoot::remote(
                PathBuf::from("/remote/fixture/claude"),
                Origin::remote("fixture-host"),
                Some(crate::sources::config::Platform::Linux),
            )],
            local_connector_roots: None,
            since_ts: None,
            local_since_ts_by_connector: Arc::new(HashMap::new()),
            progress: None,
            active_source_filter: Arc::new(ActiveSessionSourceFilter::default()),
        },
    );

    handle
        .join()
        .expect("producer should stop cleanly after consumer disconnect");
    assert_eq!(
        counter.load(Ordering::Relaxed),
        1,
        "producer should stop after the first failed batch send instead of chewing through local and remote scans"
    );

    *DISCONNECT_TEST_COUNTER
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = None;
}

#[test]
fn db_id_conversion_helpers_handle_invalid_ranges() {
    assert_eq!(message_id_from_db(-1), None);
    assert_eq!(message_id_from_db(0), Some(0));
    assert_eq!(message_id_from_db(42), Some(42));

    assert_eq!(saturating_u32_from_i64(-9), 0);
    assert_eq!(saturating_u32_from_i64(17), 17);
    assert_eq!(
        saturating_u32_from_i64(i64::from(u32::MAX) + 1234),
        u32::MAX
    );
}

#[test]
fn open_storage_for_index_resumes_zero_byte_interrupted_initialization() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("interrupted-initialization.db");
    std::fs::File::create(&db_path).unwrap();
    assert_eq!(std::fs::metadata(&db_path).unwrap().len(), 0);

    let (storage, rebuilt, opened_fresh_for_full) = open_storage_for_index(&db_path, true).unwrap();
    assert!(!rebuilt);
    assert!(opened_fresh_for_full);
    assert_eq!(
        storage.schema_version().unwrap(),
        crate::storage::sqlite::CURRENT_SCHEMA_VERSION
    );
    storage.close().unwrap();

    let reopened = FrankenStorage::open_readonly(&db_path).unwrap();
    assert_eq!(
        reopened.schema_version().unwrap(),
        crate::storage::sqlite::CURRENT_SCHEMA_VERSION,
        "resumed initialization must persist a read-only-openable canonical archive"
    );
    reopened.close_without_checkpoint().unwrap();
}

#[test]
fn open_storage_for_index_refuses_newer_schema_without_replacing_db() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("future-schema.db");

    {
        let storage = FrankenStorage::open(&db_path).unwrap();
        storage
            .raw()
            .execute_compat(
                "INSERT OR REPLACE INTO meta(key, value) VALUES('schema_version', ?1)",
                &[ParamValue::from(format!(
                    "{}",
                    crate::storage::sqlite::CURRENT_SCHEMA_VERSION + 1
                ))],
            )
            .unwrap();
    }

    let err = match open_storage_for_index(&db_path, false) {
        Ok(_) => panic!("newer schema must fail closed before indexing"),
        Err(err) => err.to_string(),
    };
    assert!(
        err.contains("newer than supported"),
        "diagnostic should name the schema mismatch: {err}"
    );
    assert!(db_path.exists(), "canonical DB must remain in place");

    // The index open path must not create backup-and-replace artifacts; a
    // real repair plan belongs to doctor or an explicit operator restore.
    let backup_count = std::fs::read_dir(tmp.path())
        .unwrap()
        .flatten()
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .map(|name| name.starts_with("future-schema.db.backup."))
                .unwrap_or(false)
        })
        .count();
    assert!(
        backup_count == 0,
        "index open must not backup-and-replace a future-schema archive"
    );
}
