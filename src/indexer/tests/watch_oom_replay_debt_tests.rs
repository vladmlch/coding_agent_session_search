use super::super::*;
use super::helpers::*;
use crate::franken_sync::compat::{ConnectionExt, ParamValue, RowExt};
use serial_test::serial;
use tempfile::TempDir;

#[test]
#[serial]
fn watch_reindex_splits_oom_batches_and_still_advances_state() {
    let tmp = TempDir::new().unwrap();
    let xdg = tmp.path().join("xdg_watch_oom_split");
    std::fs::create_dir_all(&xdg).unwrap();
    let prev = dotenvy::var("XDG_DATA_HOME").ok();
    unsafe { std::env::set_var("XDG_DATA_HOME", &xdg) };
    let _oom_guard = set_env("CASS_TEST_WATCH_INGEST_OOM_MIN_CONVS", "2");
    let _chunk_guard = set_env("CASS_WATCH_INGEST_CHUNK_SIZE", "4");
    // Disable the mtime fallback for this test — we just wrote the source
    // files, so they would all trip the active-source filter and never be
    // ingested. The OOM-split behavior we want to verify here is unrelated
    // to the active-source skip path.
    let _window_guard = set_env("CASS_ACTIVE_SESSION_RECENT_WRITE_WINDOW_SECS", "0");

    let data_dir = xdg.join("codex");
    std::fs::create_dir_all(&data_dir).unwrap();
    let codex_dir = data_dir.join("codex/sessions");
    std::fs::create_dir_all(&codex_dir).unwrap();
    let now_u128 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let base_ts = i64::try_from(now_u128)
        .unwrap_or(i64::MAX)
        .saturating_add(10_000);
    let mut paths = Vec::new();
    for idx in 0..4 {
        let path = codex_dir.join(format!("rollout-oom-{idx}.jsonl"));
        write_watch_codex_messages(
            &path,
            &format!("thread-oom-{idx}"),
            &[("user", &format!("p{idx}"), base_ts + i64::from(idx))],
        );
        paths.push(path);
    }

    let progress = Arc::new(crate::indexer::IndexingProgress::default());
    let opts = crate::indexer::IndexOptions {
        full: false,
        watch: true,
        force_rebuild: false,
        db_path: data_dir.join("agent_search.db"),
        data_dir: data_dir.clone(),
        semantic: false,
        build_hnsw: false,
        embedder: "fastembed".to_string(),
        progress: Some(progress.clone()),
        watch_once_paths: None,
        watch_interval_secs: 30,
    };

    let storage = FrankenStorage::open(&opts.db_path).unwrap();
    let index_path = index_dir(&opts.data_dir).unwrap();
    let t_index = TantivyIndex::open_or_create(&index_path).unwrap();
    let state = std::sync::Mutex::new(std::collections::HashMap::new());
    let storage = std::sync::Mutex::new(storage);
    let t_index = std::sync::Mutex::new(Some(t_index));
    let roots = vec![(ConnectorKind::Codex, ScanRoot::local(codex_dir))];

    let indexed = reindex_paths(
        &opts,
        paths,
        &roots,
        &state,
        &storage,
        &t_index,
        &index_path,
        false,
    )
    .unwrap();

    assert_eq!(indexed, 4);
    assert_eq!(progress.current.load(Ordering::Relaxed), 4);
    let conversation_rows: i64 = storage
        .lock()
        .unwrap()
        .raw()
        .query_row_map(
            "SELECT COUNT(*) FROM conversations",
            &[] as &[ParamValue],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(conversation_rows, 4);
    assert!(
        load_watch_state(&data_dir).contains_key(&ConnectorKind::Codex),
        "watch state should advance after split batches persist"
    );
    assert!(
        !data_dir
            .join("quarantine/watch_ingest_poison.jsonl")
            .exists(),
        "split batches should avoid quarantining conversations that fit singly"
    );

    if let Some(prev) = prev {
        unsafe { std::env::set_var("XDG_DATA_HOME", prev) };
    } else {
        unsafe { std::env::remove_var("XDG_DATA_HOME") };
    }
}

#[test]
#[serial]
fn watch_reindex_quarantine_preserves_watermark_for_retry() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("watch-quarantine-retry");
    let codex_dir = data_dir.join("codex/sessions");
    std::fs::create_dir_all(&codex_dir).unwrap();
    let now_u128 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let created_at = i64::try_from(now_u128)
        .unwrap_or(i64::MAX)
        .saturating_add(10_000);
    let codex_file = codex_dir.join("rollout-watch-quarantine-retry.jsonl");
    write_watch_codex_messages(
        &codex_file,
        "thread-watch-quarantine-retry",
        &[("user", "retry me", created_at)],
    );

    let _oom_guard = set_env("CASS_TEST_WATCH_INGEST_OOM_MIN_CONVS", "1");
    // #298: model a *genuinely* poisoned conversation — the solo isolate
    // retry also OOMs, and the size gate is disabled so even a small
    // conversation is allowed to quarantine. This preserves the original
    // intent of this test (watermark preserved + poison recorded) under the
    // new solo-retry-before-quarantine behavior.
    let _solo_oom_guard = set_env("CASS_TEST_WATCH_SOLO_RETRY_OOM", "1");
    let _small_gate_guard = set_env("CASS_WATCH_OOM_SMALL_CONVERSATION_BYTES", "0");
    // #364: quarantine now additionally requires *real* host memory
    // pressure; pin the probe to "always real pressure" so the poison
    // path stays reachable deterministically on any host.
    let _pressure_guard = set_env(
        "CASS_WATCH_OOM_REAL_PRESSURE_RESERVE_BYTES",
        "18446744073709551615",
    );
    let _window_guard = set_env("CASS_ACTIVE_SESSION_RECENT_WRITE_WINDOW_SECS", "0");
    let opts = crate::indexer::IndexOptions {
        full: false,
        watch: true,
        force_rebuild: false,
        db_path: data_dir.join("agent_search.db"),
        data_dir: data_dir.clone(),
        semantic: false,
        build_hnsw: false,
        embedder: "fastembed".to_string(),
        progress: None,
        watch_once_paths: None,
        watch_interval_secs: 30,
    };

    let storage = FrankenStorage::open(&opts.db_path).unwrap();
    storage.run_migrations().unwrap();
    let index_path = index_dir(&opts.data_dir).unwrap();
    let t_index = TantivyIndex::open_or_create(&index_path).unwrap();
    let state = std::sync::Mutex::new(std::collections::HashMap::new());
    let storage = std::sync::Mutex::new(storage);
    let t_index = std::sync::Mutex::new(Some(t_index));
    let roots = vec![(ConnectorKind::Codex, ScanRoot::local(codex_dir))];

    let indexed = reindex_paths(
        &opts,
        vec![codex_file],
        &roots,
        &state,
        &storage,
        &t_index,
        &index_path,
        false,
    )
    .unwrap();

    assert_eq!(indexed, 1);
    assert!(
        load_watch_state(&data_dir).is_empty(),
        "quarantined watch sources must remain eligible for later retry"
    );
    assert!(
        data_dir
            .join("quarantine/watch_ingest_poison.jsonl")
            .exists(),
        "watch OOM should still be recorded for operator visibility"
    );
}

/// #298 gate: a small conversation whose solo isolate-retry also OOMs must
/// be deferred — not quarantined — when there is no real host memory
/// pressure, so a bounded-allocation false positive can never poison a
/// small, replayable conversation.
#[test]
#[serial]
fn watch_reindex_defers_small_conversation_when_solo_retry_also_ooms() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("watch-oom-defer-small");
    let codex_dir = data_dir.join("codex/sessions");
    std::fs::create_dir_all(&codex_dir).unwrap();
    let now_u128 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let created_at = i64::try_from(now_u128)
        .unwrap_or(i64::MAX)
        .saturating_add(10_000);
    let codex_file = codex_dir.join("rollout-watch-oom-defer-small.jsonl");
    write_watch_codex_messages(
        &codex_file,
        "thread-watch-oom-defer-small",
        &[("user", "defer me", created_at)],
    );

    let _oom_guard = set_env("CASS_TEST_WATCH_INGEST_OOM_MIN_CONVS", "1");
    let _solo_oom_guard = set_env("CASS_TEST_WATCH_SOLO_RETRY_OOM", "1");
    // Pin the pressure probe to "never real pressure" so the size gate is
    // what decides, deterministically, on any host.
    let _reserve_guard = set_env("CASS_WATCH_OOM_REAL_PRESSURE_RESERVE_BYTES", "0");
    let _window_guard = set_env("CASS_ACTIVE_SESSION_RECENT_WRITE_WINDOW_SECS", "0");
    let opts = crate::indexer::IndexOptions {
        full: false,
        watch: true,
        force_rebuild: false,
        db_path: data_dir.join("agent_search.db"),
        data_dir: data_dir.clone(),
        semantic: false,
        build_hnsw: false,
        embedder: "fastembed".to_string(),
        progress: None,
        watch_once_paths: None,
        watch_interval_secs: 30,
    };

    let storage = FrankenStorage::open(&opts.db_path).unwrap();
    storage.run_migrations().unwrap();
    let index_path = index_dir(&opts.data_dir).unwrap();
    let t_index = TantivyIndex::open_or_create(&index_path).unwrap();
    let state = std::sync::Mutex::new(std::collections::HashMap::new());
    let storage = std::sync::Mutex::new(storage);
    let t_index = std::sync::Mutex::new(Some(t_index));
    let roots = vec![(ConnectorKind::Codex, ScanRoot::local(codex_dir))];

    let indexed = reindex_paths(
        &opts,
        vec![codex_file],
        &roots,
        &state,
        &storage,
        &t_index,
        &index_path,
        false,
    )
    .unwrap();

    assert_eq!(indexed, 1);
    assert!(
        load_watch_state(&data_dir).is_empty(),
        "deferred watch sources must remain eligible for later retry"
    );
    assert!(
        !data_dir
            .join("quarantine/watch_ingest_poison.jsonl")
            .exists(),
        "a small conversation with no real memory pressure must be deferred, not quarantined (#298)"
    );
}

/// GH#510: a deterministic bounded-guard NoMem used to re-run the split,
/// the solo retry and the deferral on every watch tick forever. A
/// deferred conversation now waits out an exponential backoff (counted in
/// a ledger) without an ingest attempt, keeps the watermark held, and is
/// forgotten once it ingests.
#[test]
#[serial]
fn watch_reindex_backs_off_a_repeatedly_deferred_conversation() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("watch-oom-defer-backoff");
    let codex_dir = data_dir.join("codex/sessions");
    std::fs::create_dir_all(&codex_dir).unwrap();
    let now_u128 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let created_at = i64::try_from(now_u128)
        .unwrap_or(i64::MAX)
        .saturating_add(10_000);
    let codex_file = codex_dir.join("rollout-watch-oom-defer-backoff.jsonl");
    write_watch_codex_messages(
        &codex_file,
        "thread-watch-oom-defer-backoff",
        &[("user", "back off", created_at)],
    );

    let oom_guard = set_env("CASS_TEST_WATCH_INGEST_OOM_MIN_CONVS", "1");
    let solo_oom_guard = set_env("CASS_TEST_WATCH_SOLO_RETRY_OOM", "1");
    let _reserve_guard = set_env("CASS_WATCH_OOM_REAL_PRESSURE_RESERVE_BYTES", "0");
    let _window_guard = set_env("CASS_ACTIVE_SESSION_RECENT_WRITE_WINDOW_SECS", "0");
    let opts = crate::indexer::IndexOptions {
        full: false,
        watch: true,
        force_rebuild: false,
        db_path: data_dir.join("agent_search.db"),
        data_dir: data_dir.clone(),
        semantic: false,
        build_hnsw: false,
        embedder: "fastembed".to_string(),
        progress: None,
        watch_once_paths: None,
        watch_interval_secs: 30,
    };

    let storage = FrankenStorage::open(&opts.db_path).unwrap();
    storage.run_migrations().unwrap();
    let index_path = index_dir(&opts.data_dir).unwrap();
    let t_index = TantivyIndex::open_or_create(&index_path).unwrap();
    let state = std::sync::Mutex::new(std::collections::HashMap::new());
    let storage = std::sync::Mutex::new(storage);
    let t_index = std::sync::Mutex::new(Some(t_index));
    let roots = vec![(ConnectorKind::Codex, ScanRoot::local(codex_dir))];
    let tick = || {
        reindex_paths_with_semantic_delta(
            &opts,
            vec![codex_file.clone()],
            &roots,
            &state,
            &storage,
            &t_index,
            &index_path,
            false,
            None,
            &ActiveSessionSourceFilter::new(false),
        )
        .unwrap()
    };
    let only_entry = || {
        let ledger = WatchNomemDeferrals::load(&data_dir);
        assert_eq!(ledger.entries.len(), 1, "one deferred conversation");
        ledger.entries.into_values().next().unwrap()
    };
    let expire_backoff = || {
        let mut ledger = WatchNomemDeferrals::load(&data_dir);
        for entry in ledger.entries.values_mut() {
            entry.next_retry_at_ms = 0;
        }
        ledger.save(&data_dir);
    };

    // Tick 1: attempted, deferred, scheduled one minute out.
    let first_tick = tick();
    assert_eq!(first_tick.indexed, 1);
    assert_eq!(
        first_tick.retry_sources,
        BTreeSet::from([roots[0].1.path.clone()])
    );
    let first = only_entry();
    assert_eq!(first.deferrals, 1);
    assert_eq!(first.next_retry_at_ms - first.last_deferred_at_ms, 60_000);
    assert!(load_watch_state(&data_dir).is_empty());

    // Tick 2, inside the backoff: no ingest attempt. An attempt would
    // NoMem again under the injection and count a second deferral.
    let held_tick = tick();
    assert_eq!(
        held_tick.indexed, 0,
        "a backed-off conversation is not ingested"
    );
    assert_eq!(held_tick.retry_sources, first_tick.retry_sources);
    assert_eq!(only_entry(), first, "no attempt inside the backoff");
    assert!(
        load_watch_state(&data_dir).is_empty(),
        "the watermark stays held for a backed-off conversation"
    );

    // Backoff elapsed: attempted again, and the backoff doubles.
    expire_backoff();
    let second_tick = tick();
    assert_eq!(second_tick.indexed, 1);
    assert_eq!(second_tick.retry_sources, first_tick.retry_sources);
    let second = only_entry();
    assert_eq!(second.deferrals, 2);
    assert_eq!(second.first_deferred_at_ms, first.first_deferred_at_ms);
    assert_eq!(
        second.next_retry_at_ms - second.last_deferred_at_ms,
        120_000
    );
    assert!(
        !data_dir
            .join("quarantine/watch_ingest_poison.jsonl")
            .exists(),
        "backoff never turns a bounded-guard NoMem into a quarantine"
    );

    // The trigger goes away: the next attempt ingests, the ledger forgets
    // the conversation and the watermark advances.
    drop(solo_oom_guard);
    drop(oom_guard);
    expire_backoff();
    let recovered_tick = tick();
    assert_eq!(recovered_tick.indexed, 1);
    assert!(recovered_tick.retry_sources.is_empty());
    assert!(WatchNomemDeferrals::load(&data_dir).entries.is_empty());
    assert!(
        load_watch_state(&data_dir).contains_key(&ConnectorKind::Codex),
        "the watermark advances once the deferred conversation ingests"
    );
}

/// GH#510: status/health surface watcher deferrals through the ingest
/// quarantine summary without calling them a quarantine.
#[test]
#[serial]
fn ingest_quarantine_summary_counts_watch_nomem_deferrals() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("deferral-summary");
    std::fs::create_dir_all(&data_dir).unwrap();
    let deferral = |source: &str| WatchNomemDeferral {
        agent: "amp".to_string(),
        source_path: source.to_string(),
        external_id: None,
        deferrals: 3,
        first_deferred_at_ms: 1_700_000_000_000,
        last_deferred_at_ms: 1_700_000_100_000,
        next_retry_at_ms: 1_700_000_340_000,
        indexed_text_bytes: 525,
    };
    let empty = conversation_ingest_quarantine_summary(&data_dir);
    assert_eq!(empty.deferred_conversations, 0);
    assert!(empty.recommended_action.is_none());

    let mut ledger = WatchNomemDeferrals::default();
    ledger
        .entries
        .insert("a".to_string(), deferral("/logs/a.json"));
    ledger
        .entries
        .insert("b".to_string(), deferral("/logs/b.json"));
    ledger.save(&data_dir);
    let deferred = conversation_ingest_quarantine_summary(&data_dir);
    assert_eq!(deferred.deferred_conversations, 2);
    assert_eq!(deferred.quarantined_conversations, 0);
    assert_eq!(deferred.status, "ok", "a deferral is not a quarantine");
    let action = deferred
        .recommended_action
        .expect("deferrals get an action");
    assert!(
        action.starts_with("2 conversation(s) are deferred"),
        "{action}"
    );
    assert!(action.contains("cass quarantine list --json"), "{action}");

    // A real quarantine keeps its own action; the deferral count stays.
    let mut state = crate::indexer::quarantine::QuarantineState::default();
    state.entries.insert(
        "conv-q::v1".to_string(),
        crate::indexer::quarantine::QuarantineRecord {
            first_attempt_at: chrono::Utc::now(),
            last_attempt_at: chrono::Utc::now(),
            attempt_count: 1,
            last_reason: "ingest_oom".to_string(),
            cass_version_at_quarantine: None,
        },
    );
    state.save(&data_dir).unwrap();
    let both = conversation_ingest_quarantine_summary(&data_dir);
    assert_eq!(both.deferred_conversations, 2);
    assert_eq!(both.status, "degraded");
    assert!(
        both.recommended_action
            .as_deref()
            .is_some_and(|action| action.starts_with("Run `cass quarantine list --json`")),
        "{:?}",
        both.recommended_action
    );
}

#[test]
fn watch_nomem_deferral_backoff_doubles_to_one_hour() {
    let backoffs: Vec<i64> = (1..=8).map(watch_nomem_deferral_backoff_ms).collect();
    assert_eq!(
        backoffs,
        [
            60_000, 120_000, 240_000, 480_000, 960_000, 1_920_000, 3_600_000, 3_600_000
        ]
    );
    assert_eq!(watch_nomem_deferral_backoff_ms(u32::MAX), 3_600_000);
}

/// #364 gate: a *large* conversation whose solo isolate-retry also OOMs
/// must still be deferred — not quarantined — when the host reports ample
/// available memory. Ample memory proves the typed NoMem is a
/// SQLite bounded-allocation artifact; conversation size makes a
/// guard more likely to trip but cannot turn it into host exhaustion, and
/// a quarantine here is a same-version-irreducible coverage hole for a
/// healthy, replayable source (the reported case: a 36 MB Codex session
/// under ~89 GiB MemAvailable).
///
/// Linux/macOS only: the reserve=0 pin yields `Some(false)` real
/// pressure only where `available_memory_bytes()` reports a value; on
/// other platforms the probe returns `None`, the (pinned) size gate
/// fires, and the disposition legitimately quarantines.
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
#[serial]
fn watch_reindex_defers_large_conversation_with_ample_memory() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("watch-oom-defer-large");
    let codex_dir = data_dir.join("codex/sessions");
    std::fs::create_dir_all(&codex_dir).unwrap();
    let now_u128 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let created_at = i64::try_from(now_u128)
        .unwrap_or(i64::MAX)
        .saturating_add(10_000);
    let codex_file = codex_dir.join("rollout-watch-oom-defer-large.jsonl");
    write_watch_codex_messages(
        &codex_file,
        "thread-watch-oom-defer-large",
        &[("user", "defer the large one", created_at)],
    );

    let _oom_guard = set_env("CASS_TEST_WATCH_INGEST_OOM_MIN_CONVS", "1");
    let _solo_oom_guard = set_env("CASS_TEST_WATCH_SOLO_RETRY_OOM", "1");
    // Classify the tiny fixture as "large" so the old size gate would
    // have quarantined it...
    let _small_gate_guard = set_env("CASS_WATCH_OOM_SMALL_CONVERSATION_BYTES", "1");
    // ...while the pressure probe deterministically reports ample memory.
    let _reserve_guard = set_env("CASS_WATCH_OOM_REAL_PRESSURE_RESERVE_BYTES", "0");
    let _window_guard = set_env("CASS_ACTIVE_SESSION_RECENT_WRITE_WINDOW_SECS", "0");
    let opts = crate::indexer::IndexOptions {
        full: false,
        watch: true,
        force_rebuild: false,
        db_path: data_dir.join("agent_search.db"),
        data_dir: data_dir.clone(),
        semantic: false,
        build_hnsw: false,
        embedder: "fastembed".to_string(),
        progress: None,
        watch_once_paths: None,
        watch_interval_secs: 30,
    };

    let storage = FrankenStorage::open(&opts.db_path).unwrap();
    storage.run_migrations().unwrap();
    let index_path = index_dir(&opts.data_dir).unwrap();
    let t_index = TantivyIndex::open_or_create(&index_path).unwrap();
    let state = std::sync::Mutex::new(std::collections::HashMap::new());
    let storage = std::sync::Mutex::new(storage);
    let t_index = std::sync::Mutex::new(Some(t_index));
    let roots = vec![(ConnectorKind::Codex, ScanRoot::local(codex_dir))];

    let indexed = reindex_paths(
        &opts,
        vec![codex_file],
        &roots,
        &state,
        &storage,
        &t_index,
        &index_path,
        false,
    )
    .unwrap();

    assert_eq!(indexed, 1);
    assert!(
        load_watch_state(&data_dir).is_empty(),
        "deferred watch sources must remain eligible for later retry"
    );
    assert!(
        !data_dir
            .join("quarantine/watch_ingest_poison.jsonl")
            .exists(),
        "a large conversation with ample host memory must be deferred, not \
         quarantined as same-version-irreducible (#364)"
    );
}

/// #298 differential: a conversation that OOMs in batch ingest but ingests
/// cleanly when isolated solo must NOT be quarantined. This is the exact
/// false-positive the reporter hit — small/medium conversations that index
/// fine solo were poisoned on the `--watch` batch path.
#[test]
#[serial]
fn watch_batch_oom_that_succeeds_solo_is_not_quarantined() {
    let tmp = TempDir::new().unwrap();
    let xdg = tmp.path().join("xdg_watch_batch_oom_solo_ok");
    std::fs::create_dir_all(&xdg).unwrap();
    let prev = dotenvy::var("XDG_DATA_HOME").ok();
    unsafe { std::env::set_var("XDG_DATA_HOME", &xdg) };

    // Inject a typed OOM on any batch of >= 2 conversations, but leave the
    // solo isolate-retry untouched (it disables injection), so each
    // conversation succeeds when re-ingested alone. Do NOT force the solo
    // retry to OOM — that is the whole point of the differential.
    let _oom_guard = set_env("CASS_TEST_WATCH_INGEST_OOM_MIN_CONVS", "2");
    let _chunk_guard = set_env("CASS_WATCH_INGEST_CHUNK_SIZE", "4");
    let _window_guard = set_env("CASS_ACTIVE_SESSION_RECENT_WRITE_WINDOW_SECS", "0");

    let data_dir = xdg.join("codex");
    std::fs::create_dir_all(&data_dir).unwrap();
    let codex_dir = data_dir.join("codex/sessions");
    std::fs::create_dir_all(&codex_dir).unwrap();
    let now_u128 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let base_ts = i64::try_from(now_u128)
        .unwrap_or(i64::MAX)
        .saturating_add(10_000);
    let mut paths = Vec::new();
    for idx in 0..4 {
        let path = codex_dir.join(format!("rollout-batch-oom-solo-ok-{idx}.jsonl"));
        write_watch_codex_messages(
            &path,
            &format!("thread-batch-oom-solo-ok-{idx}"),
            &[("user", &format!("p{idx}"), base_ts + i64::from(idx))],
        );
        paths.push(path);
    }

    let progress = Arc::new(crate::indexer::IndexingProgress::default());
    let opts = crate::indexer::IndexOptions {
        full: false,
        watch: true,
        force_rebuild: false,
        db_path: data_dir.join("agent_search.db"),
        data_dir: data_dir.clone(),
        semantic: false,
        build_hnsw: false,
        embedder: "fastembed".to_string(),
        progress: Some(progress.clone()),
        watch_once_paths: None,
        watch_interval_secs: 30,
    };

    let storage = FrankenStorage::open(&opts.db_path).unwrap();
    let index_path = index_dir(&opts.data_dir).unwrap();
    let t_index = TantivyIndex::open_or_create(&index_path).unwrap();
    let state = std::sync::Mutex::new(std::collections::HashMap::new());
    let storage = std::sync::Mutex::new(storage);
    let t_index = std::sync::Mutex::new(Some(t_index));
    let roots = vec![(ConnectorKind::Codex, ScanRoot::local(codex_dir))];

    let indexed = reindex_paths(
        &opts,
        paths,
        &roots,
        &state,
        &storage,
        &t_index,
        &index_path,
        false,
    )
    .unwrap();

    assert_eq!(indexed, 4);
    assert_eq!(progress.current.load(Ordering::Relaxed), 4);
    let conversation_rows: i64 = storage
        .lock()
        .unwrap()
        .raw()
        .query_row_map(
            "SELECT COUNT(*) FROM conversations",
            &[] as &[ParamValue],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(
        conversation_rows, 4,
        "all 4 conversations must persist via the solo isolate-retry"
    );
    assert!(
        !data_dir
            .join("quarantine/watch_ingest_poison.jsonl")
            .exists(),
        "conversations that ingest fine solo must not be quarantined on a batch-path bounded NoMem (#298)"
    );

    if let Some(prev) = prev {
        unsafe { std::env::set_var("XDG_DATA_HOME", prev) };
    } else {
        unsafe { std::env::remove_var("XDG_DATA_HOME") };
    }
}

#[test]
#[serial]
fn explicit_watch_once_defers_lexical_oom_without_quarantining_source() {
    let tmp = TempDir::new().unwrap();
    let xdg = tmp.path().join("xdg_watch_once_lexical_oom");
    std::fs::create_dir_all(&xdg).unwrap();
    let prev = dotenvy::var("XDG_DATA_HOME").ok();
    unsafe { std::env::set_var("XDG_DATA_HOME", &xdg) };
    let _lexical_oom_guard = inject_incremental_lexical_update_test_oom();

    let data_dir = xdg.join("codex");
    std::fs::create_dir_all(&data_dir).unwrap();
    let codex_dir = data_dir.join("codex/sessions");
    std::fs::create_dir_all(&codex_dir).unwrap();
    let codex_file = codex_dir.join("rollout-watch-once-lexical-oom.jsonl");
    write_watch_codex_messages(
        &codex_file,
        "thread-watch-once-lexical-oom",
        &[("user", "persist me", 1_700_000_000_100)],
    );

    let opts = crate::indexer::IndexOptions {
        full: false,
        watch: false,
        force_rebuild: false,
        db_path: data_dir.join("agent_search.db"),
        data_dir: data_dir.clone(),
        semantic: false,
        build_hnsw: false,
        embedder: "fastembed".to_string(),
        progress: None,
        watch_once_paths: Some(vec![codex_file.clone()]),
        watch_interval_secs: 30,
    };

    let storage = FrankenStorage::open(&opts.db_path).unwrap();
    let index_path = index_dir(&opts.data_dir).unwrap();
    let t_index = TantivyIndex::open_or_create(&index_path).unwrap();
    let state = std::sync::Mutex::new(std::collections::HashMap::new());
    let storage = std::sync::Mutex::new(storage);
    let t_index = std::sync::Mutex::new(Some(t_index));
    let roots = vec![(ConnectorKind::Codex, ScanRoot::local(codex_file.clone()))];

    let indexed = reindex_paths(
        &opts,
        vec![codex_file.clone()],
        &roots,
        &state,
        &storage,
        &t_index,
        &index_path,
        false,
    )
    .unwrap();

    assert_eq!(indexed, 1);
    let (conversation_rows, message_rows): (i64, i64) = {
        let storage = storage.lock().unwrap();
        let conversations = storage
            .raw()
            .query_row_map(
                "SELECT COUNT(*) FROM conversations",
                &[] as &[ParamValue],
                |row| row.get_typed(0),
            )
            .unwrap();
        let messages = storage
            .raw()
            .query_row_map(
                "SELECT COUNT(*) FROM messages",
                &[] as &[ParamValue],
                |row| row.get_typed(0),
            )
            .unwrap();
        (conversations, messages)
    };
    assert_eq!(conversation_rows, 1);
    assert_eq!(message_rows, 1);
    assert!(
        !data_dir
            .join("quarantine/watch_ingest_poison.jsonl")
            .exists(),
        "lexical update OOM must not quarantine a source conversation that persisted to SQLite"
    );
    assert!(
        t_index.lock().unwrap().is_none(),
        "dirty Tantivy writer should be dropped after deferred lexical update"
    );

    let canonical_before = watch_lexical_canonical_rows(&storage.lock().unwrap());
    assert_eq!(canonical_before.len(), 1);
    assert_eq!(canonical_before[0].3, "persist me");
    assert_eq!(
        watch_lexical_replay_debt_for_root(
            &storage.lock().unwrap(),
            ConnectorKind::Codex,
            &roots[0].1,
        )
        .unwrap()
        .len(),
        1
    );
    drop(_lexical_oom_guard);
    // Reopen the canonical handle and writer state, as a fresh process
    // would. A newer global timestamp must not hide this source's debt.
    let original_storage = storage.into_inner().unwrap();
    original_storage.close().unwrap();
    let storage = Mutex::new(FrankenStorage::open(&opts.db_path).unwrap());
    storage
        .lock()
        .unwrap()
        .set_last_indexed_at(FrankenStorage::now_millis().saturating_add(10_000))
        .unwrap();
    let t_index = Mutex::new(None);
    let state = Mutex::new(HashMap::new());
    let expected_id = frankensearch::quill::cass::cass_document_identity(
        "local",
        frankensearch::quill::cass::CassConversationKey {
            source_path: &codex_file.to_string_lossy(),
            id: Some(canonical_before[0].0),
        },
        canonical_before[0].2 as u64,
    );
    for (attempt, fail) in [true, false, false].into_iter().enumerate() {
        let fault =
            fail.then(|| WatchLexicalFaultGuard::set(WatchLexicalTestFault::PublicationAfter(0)));
        let indexed = reindex_paths(
            &opts,
            vec![codex_file.clone()],
            &roots,
            &state,
            &storage,
            &t_index,
            &index_path,
            false,
        )
        .unwrap();
        drop(fault);
        assert_eq!(indexed, [1, 1, 0][attempt]);
        assert_eq!(
            watch_lexical_canonical_rows(&storage.lock().unwrap()),
            canonical_before
        );
        let hits = watch_lexical_search_ids(&index_path, "persist");
        assert_eq!(hits.len(), usize::from(!fail));
        if !fail {
            assert_eq!(hits, BTreeSet::from([expected_id.clone()]));
        }
        assert_eq!(
            watch_lexical_replay_debt_for_root(
                &storage.lock().unwrap(),
                ConnectorKind::Codex,
                &roots[0].1,
            )
            .unwrap()
            .is_empty(),
            !fail
        );
        assert!(
            !data_dir
                .join("quarantine/watch_ingest_poison.jsonl")
                .exists()
        );
    }

    if let Some(prev) = prev {
        unsafe { std::env::set_var("XDG_DATA_HOME", prev) };
    } else {
        unsafe { std::env::remove_var("XDG_DATA_HOME") };
    }
}

#[test]
#[serial]
fn watch_storage_recycle_recovers_after_transient_open_failure() -> Result<()> {
    let _checkpoint = set_env("CASS_INDEX_WRITER_WAL_AUTOCHECKPOINT_PAGES", "-1");
    let tmp = TempDir::new()?;
    let data_dir = tmp.path().join("cass");
    let archive_dir = tmp.path().join("archive");
    let mut opts = watch_lexical_options(&data_dir);
    opts.db_path = archive_dir.join("agent_search.db");
    opts.watch = true;
    let progress = Arc::new(IndexingProgress::default());
    opts.progress = Some(Arc::clone(&progress));
    let storage = Mutex::new(FrankenStorage::open(&opts.db_path)?);
    let index_path = index_dir(&data_dir)?;
    let index = Mutex::new(Some(TantivyIndex::open_or_create(&index_path)?));
    let preserved = tmp.path().join("preserved/codex/rollout-preserved.jsonl");
    write_watch_lexical_source(&preserved, "thread-preserved", "recyclepreservedneedle");
    assert_eq!(
        run_watch_lexical_selection(&opts, &preserved, &storage, &index)?,
        1
    );
    let canonical_before = watch_lexical_canonical_rows(&storage.lock().unwrap());
    let preserved_ids = watch_lexical_search_ids(&index_path, "recyclepreservedneedle");
    assert_eq!(preserved_ids.len(), 1);

    let source = tmp.path().join("pending/codex/rollout-pending.jsonl");
    write_watch_lexical_source(&source, "thread-pending", "recyclerecoveredneedle");
    let source_before = fs::read(&source)?;
    let source_modified = fs::metadata(&source)?.modified()?;
    let roots = [(
        ConnectorKind::Codex,
        ScanRoot::local(source.parent().unwrap().to_path_buf()),
    )];
    let state = Mutex::new(HashMap::new());
    let reopen_pending = std::cell::Cell::new(false);
    let activity_before = progress.activity.load(Ordering::Relaxed);
    reopen_watch_storage_if_pending(
        &storage,
        &opts.db_path,
        &reopen_pending,
        true,
        Some(&progress),
    )?;
    assert_eq!(
        progress.activity.load(Ordering::Relaxed),
        activity_before,
        "normal callback admission must not manufacture progress or reopen storage"
    );
    close_watch_storage_for_recycle(&storage, &reopen_pending)?;
    assert!(reopen_pending.get());
    assert!(storage.lock().unwrap().raw().query("SELECT 1").is_err());

    // Cause a real filesystem open failure after the handle has closed.
    // Keep the entire database bundle intact, including committed WAL data.
    let parked_archive = tmp.path().join("parked-archive");
    fs::rename(&archive_dir, &parked_archive)?;
    fs::write(&archive_dir, b"temporarily unavailable archive directory")?;
    let database_before = fs::read(parked_archive.join("agent_search.db"))?;
    let mut pending = BTreeSet::from([source.clone()]);
    let callback = |paths, roots: &[(ConnectorKind, ScanRoot)], force_full| {
        reopen_watch_storage_if_pending(
            &storage,
            &opts.db_path,
            &reopen_pending,
            true,
            Some(&progress),
        )?;
        let outcome = reindex_paths_with_semantic_delta(
            &opts,
            paths,
            roots,
            &state,
            &storage,
            &index,
            &index_path,
            force_full,
            None,
            &ActiveSessionSourceFilter::new(false),
        )?;
        Ok(outcome.retry_sources)
    };
    for _ in 0..2 {
        assert!(!dispatch_watch_callback(
            &mut pending,
            &roots,
            false,
            &callback,
        ));
        assert_eq!(pending, BTreeSet::from([source.clone()]));
        assert!(
            reopen_pending.get(),
            "a failed open must retain its recovery obligation"
        );
        assert_eq!(
            progress.activity.load(Ordering::Relaxed),
            activity_before,
            "failed open attempts must not mask a stalled owner"
        );
        assert!(
            progress
                .last_error
                .lock()
                .unwrap()
                .as_deref()
                .is_some_and(|error| {
                    error.contains("reopening canonical db after watch recycle")
                })
        );
        assert_eq!(
            fs::read(parked_archive.join("agent_search.db"))?,
            database_before
        );
        assert!(watch_lexical_search_ids(&index_path, "recyclerecoveredneedle").is_empty());
    }

    fs::rename(&archive_dir, tmp.path().join("retired-path-blocker"))?;
    fs::rename(&parked_archive, &archive_dir)?;
    // Retry the retained source with no new filesystem notification.
    assert!(dispatch_watch_callback(
        &mut pending,
        &roots,
        false,
        &callback,
    ));
    assert!(pending.is_empty());
    assert!(!reopen_pending.get());
    assert!(progress.last_error.lock().unwrap().is_none());
    let canonical_after = watch_lexical_canonical_rows(&storage.lock().unwrap());
    assert_eq!(canonical_after.len(), canonical_before.len() + 1);
    assert!(
        canonical_before
            .iter()
            .all(|row| canonical_after.contains(row))
    );
    assert_eq!(
        watch_lexical_search_ids(&index_path, "recyclepreservedneedle"),
        preserved_ids
    );
    let recovered_ids = watch_lexical_search_ids(&index_path, "recyclerecoveredneedle");
    assert_eq!(recovered_ids.len(), 1);
    assert_eq!(
        storage.lock().unwrap().index_writer_checkpoint_pages(),
        Some(1000)
    );

    // Repeated delivery after recovery still has one canonical and lexical identity.
    run_watch_lexical_selection(&opts, &source, &storage, &index)?;
    assert_eq!(
        watch_lexical_canonical_rows(&storage.lock().unwrap()),
        canonical_after
    );
    assert_eq!(
        watch_lexical_search_ids(&index_path, "recyclerecoveredneedle"),
        recovered_ids
    );
    assert_eq!(fs::read(&source)?, source_before);
    assert_eq!(fs::metadata(&source)?.modified()?, source_modified);
    drop(index);
    storage.into_inner().unwrap().close()?;
    Ok(())
}

#[test]
fn watch_storage_recycle_honors_cancellation_before_reopen() -> Result<()> {
    let tmp = TempDir::new()?;
    let db_path = tmp.path().join("agent_search.db");
    let storage = Mutex::new(FrankenStorage::open(&db_path)?);
    let reopen_pending = std::cell::Cell::new(false);
    close_watch_storage_for_recycle(&storage, &reopen_pending)?;
    let progress = Arc::new(IndexingProgress::default());
    progress.request_stop();
    let error =
        reopen_watch_storage_if_pending(&storage, &db_path, &reopen_pending, true, Some(&progress))
            .expect_err("cancellation must prevent another storage open");
    assert!(error.downcast_ref::<IndexInterrupted>().is_some());
    assert!(reopen_pending.get());
    assert_eq!(progress.activity.load(Ordering::Relaxed), 0);
    assert!(storage.lock().unwrap().raw().query("SELECT 1").is_err());
    Ok(())
}

#[test]
#[serial]
fn gh538_watch_deferred_publication_retries_without_another_event() -> Result<()> {
    let tmp = TempDir::new()?;
    let data_dir = tmp.path().join("cass");
    fs::create_dir_all(&data_dir)?;
    let selected = tmp.path().join("codex/sessions");
    let source = selected.join("rollout-retry.jsonl");
    let unrelated = tmp.path().join("preserved/codex/rollout-preserved.jsonl");
    write_watch_lexical_source(&source, "thread-retry", "watchretryscheduledneedle");
    write_watch_lexical_source(&unrelated, "thread-preserved", "watchretrypreservedneedle");
    let source_before = fs::read(&source)?;
    let source_mtime = fs::metadata(&source)?.modified()?;
    let mut opts = watch_lexical_options(&data_dir);
    opts.watch = true;
    let roots = [(ConnectorKind::Codex, ScanRoot::local(selected.clone()))];
    let index_path = index_dir(&data_dir)?;
    let storage = Mutex::new(FrankenStorage::open(&opts.db_path)?);
    let index = Mutex::new(Some(TantivyIndex::open_or_create(&index_path)?));
    run_watch_lexical_selection(&opts, &unrelated, &storage, &index)?;
    let preserved_ids = watch_lexical_search_ids(&index_path, "watchretrypreservedneedle");
    assert_eq!(preserved_ids.len(), 1);
    let last_indexed_before = storage.lock().unwrap().get_last_indexed_at()?;
    let state = Mutex::new(HashMap::new());
    let progress = Arc::new(IndexingProgress::default());
    opts.progress = Some(Arc::clone(&progress));
    {
        // A failed attempt discovers work but completes none. A later
        // successful retry must not falsify that cumulative accounting
        // merely to make the watch watchdog recognize an idle owner.
        let _fault = WatchLexicalFaultGuard::set(WatchLexicalTestFault::DebtWrite);
        let failed = reindex_paths_with_semantic_delta(
            &opts,
            vec![source.clone()],
            &roots,
            &state,
            &storage,
            &index,
            &index_path,
            false,
            None,
            &ActiveSessionSourceFilter::new(false),
        );
        assert!(
            finalize_watch_reindex_result(
                failed.map(|outcome| outcome.indexed),
                &StaleDetector::from_env(),
                Some(&progress),
                "GH538 failed attempt",
            )
            .is_err()
        );
    }
    assert_eq!(progress.current.load(Ordering::Relaxed), 0);
    assert_eq!(progress.total.load(Ordering::Relaxed), 1);
    assert_eq!(progress.phase.load(Ordering::Relaxed), 0);
    assert!(!progress.watch_idle.load(Ordering::Acquire));
    let mut pending = WatchEventBacklog::default();
    assert!(pending.extend([source.clone()]));
    let mut pending_rescan = false;
    {
        let _fault = WatchLexicalFaultGuard::set(WatchLexicalTestFault::PublicationAfter(0));
        dispatch_pending_watch_callback(
            &mut pending,
            &mut pending_rescan,
            &roots,
            &|paths, roots, force_full| {
                let outcome = reindex_paths_with_semantic_delta(
                    &opts,
                    paths,
                    roots,
                    &state,
                    &storage,
                    &index,
                    &index_path,
                    force_full,
                    None,
                    &ActiveSessionSourceFilter::new(false),
                )?;
                assert_eq!(outcome.indexed, 1);
                Ok(outcome.retry_sources)
            },
        );
    }
    assert!(!pending_rescan);
    assert_eq!(pending.paths, BTreeSet::from([selected.clone()]));
    assert!(watch_lexical_search_ids(&index_path, "watchretryscheduledneedle").is_empty());
    assert_eq!(
        storage.lock().unwrap().get_last_indexed_at()?,
        last_indexed_before
    );
    assert!(load_watch_state(&data_dir).is_empty());
    let committed = watch_lexical_canonical_rows(&storage.lock().unwrap());
    assert_eq!(committed.len(), 2);
    assert_eq!(
        watch_lexical_search_ids(&index_path, "watchretrypreservedneedle"),
        preserved_ids
    );

    // Drop the in-memory event queue and handles exactly as a restart
    // does. Recovery is seeded from canonical debt, with no source write
    // or filesystem notification after the failed publication.
    drop(index);
    storage.into_inner().unwrap().close()?;
    pending.clear();
    let storage = Mutex::new(FrankenStorage::open(&opts.db_path)?);
    let index = Mutex::new(None);
    let recovery = watch_lexical_replay_sources(&storage.lock().unwrap(), &roots)?;
    assert_eq!(recovery, BTreeSet::from([selected]));
    assert!(pending.extend(recovery));
    dispatch_pending_watch_callback(
        &mut pending,
        &mut pending_rescan,
        &roots,
        &|paths, roots, force_full| {
            let outcome = reindex_paths_with_semantic_delta(
                &opts,
                paths,
                roots,
                &state,
                &storage,
                &index,
                &index_path,
                force_full,
                None,
                &ActiveSessionSourceFilter::new(false),
            )?;
            assert_eq!(outcome.indexed, 1);
            Ok(outcome.retry_sources)
        },
    );
    assert!(pending.paths.is_empty());
    assert!(!pending_rescan);
    assert!(watch_lexical_replay_sources(&storage.lock().unwrap(), &roots)?.is_empty());
    assert_eq!(
        watch_lexical_canonical_rows(&storage.lock().unwrap()),
        committed
    );
    assert_eq!(
        watch_lexical_search_ids(&index_path, "watchretryscheduledneedle").len(),
        1
    );
    assert_eq!(
        watch_lexical_search_ids(&index_path, "watchretrypreservedneedle"),
        preserved_ids
    );
    assert_eq!(fs::read(&source)?, source_before);
    assert_eq!(fs::metadata(&source)?.modified()?, source_mtime);
    assert_eq!(progress.current.load(Ordering::Relaxed), 2);
    assert_eq!(progress.total.load(Ordering::Relaxed), 3);
    assert_eq!(progress.phase.load(Ordering::Relaxed), 0);
    assert!(!progress.watch_idle.load(Ordering::Acquire));
    Ok(())
}

#[test]
#[serial]
fn gh538_watch_restart_debt_respects_source_connector_and_root_scope() -> Result<()> {
    let tmp = TempDir::new()?;
    let storage = FrankenStorage::open(&tmp.path().join("agent_search.db"))?;
    let root = ScanRoot::local(tmp.path().join("codex"));
    let child = ScanRoot::local(root.path.join("child"));
    let roots = [(ConnectorKind::Codex, root.clone())];
    assert!(watch_lexical_replay_sources(&storage, &roots)?.is_empty());
    let same_root = WatchLexicalReplayDebt::for_root(ConnectorKind::Codex, &root);
    let child_debt = WatchLexicalReplayDebt::for_root(ConnectorKind::Codex, &child);
    let another_connector = WatchLexicalReplayDebt::for_root(ConnectorKind::Claude, &root);
    let mut another_source = WatchLexicalReplayDebt::for_root(ConnectorKind::Codex, &root);
    another_source.source_id = "removed-source".into();
    let removed_root = WatchLexicalReplayDebt::for_root(
        ConnectorKind::Codex,
        &ScanRoot::local(tmp.path().join("removed-root")),
    );
    for debt in [
        &same_root,
        &child_debt,
        &another_connector,
        &another_source,
        &removed_root,
    ] {
        persist_watch_lexical_replay_debt(&storage, debt)?;
    }
    assert_eq!(
        watch_lexical_replay_sources(&storage, &roots)?,
        BTreeSet::from([root.path.clone()])
    );
    assert_eq!(
        watch_lexical_replay_sources(&storage, &[(ConnectorKind::Codex, child.clone())])?,
        BTreeSet::from([child.path]),
        "an ancestor debt may retry only the still-configured child"
    );
    clear_watch_lexical_replay_debt(&storage, &same_root)?;
    clear_watch_lexical_replay_debt(&storage, &child_debt)?;
    assert!(watch_lexical_replay_sources(&storage, &roots)?.is_empty());
    assert_eq!(load_watch_lexical_replay_debt(&storage)?.len(), 3);

    storage.raw().execute_compat(
        "INSERT INTO meta(key, value) VALUES(?1, ?2)",
        &[
            ParamValue::from(format!("{WATCH_LEXICAL_REPLAY_DEBT_PREFIX}malformed")),
            ParamValue::from("{broken"),
        ],
    )?;
    assert!(watch_lexical_replay_sources(&storage, &roots).is_err());
    let malformed: String = storage.raw().query_row_map(
        "SELECT value FROM meta WHERE key = ?1",
        &[ParamValue::from(format!(
            "{WATCH_LEXICAL_REPLAY_DEBT_PREFIX}malformed"
        ))],
        |row| row.get_typed(0),
    )?;
    assert_eq!(
        malformed, "{broken",
        "a failed load must not discard recovery debt"
    );
    Ok(())
}

#[test]
#[serial]
fn gh538_watch_excluded_debt_does_not_schedule_an_endless_rescan() -> Result<()> {
    let tmp = TempDir::new()?;
    let source = tmp.path().join("codex/rollout-excluded.jsonl");
    write_watch_lexical_source(&source, "thread-excluded", "excludedwatchneedle");
    let _exclusion = set_env("CASS_EXCLUDE_PATHS", &source.to_string_lossy());
    let opts = watch_lexical_options(tmp.path());
    let storage = Mutex::new(FrankenStorage::open(&opts.db_path)?);
    let root = ScanRoot::local(source.parent().unwrap().to_path_buf());
    persist_watch_lexical_replay_debt(
        &storage.lock().unwrap(),
        &WatchLexicalReplayDebt::for_root(ConnectorKind::Codex, &root),
    )?;
    let index = Mutex::new(None);
    let outcome = reindex_paths_with_semantic_delta(
        &opts,
        vec![root.path.clone()],
        &[(ConnectorKind::Codex, root.clone())],
        &Mutex::new(HashMap::new()),
        &storage,
        &index,
        &index_dir(tmp.path())?,
        false,
        None,
        &ActiveSessionSourceFilter::new(false),
    )?;
    assert_eq!(outcome.indexed, 0);
    assert!(outcome.retry_sources.is_empty());
    assert!(index.lock().unwrap().is_none());
    assert_eq!(
        load_watch_lexical_replay_debt(&storage.lock().unwrap())?.len(),
        1
    );
    assert_eq!(storage.lock().unwrap().get_last_indexed_at()?, None);
    Ok(())
}

#[test]
#[serial]
fn watch_notification_backlog_overflow_recovers_old_sources_without_resetting_archive() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("cass");
    fs::create_dir_all(&data_dir).unwrap();
    // A backdated Claude source must remain discoverable even after a
    // newer provider watermark and a notification backlog overflow.
    let selected = tmp.path().join("selected/claude");
    let missed = selected.join("projects/overflow/session-missed.jsonl");
    let unrelated = tmp.path().join("unrelated/codex/rollout-other.jsonl");
    fs::create_dir_all(missed.parent().unwrap()).unwrap();
    fs::write(
        &missed,
        r#"{"type":"user","timestamp":"2023-11-14T22:13:20.100Z","sessionId":"session-missed","message":{"role":"user","content":"overflowrecoveryneedle"}}
"#,
    )
    .unwrap();
    write_watch_lexical_source(&unrelated, "thread-other", "overflowpreservedneedle");
    fs::File::options()
        .write(true)
        .open(&missed)
        .unwrap()
        .set_times(
            fs::FileTimes::new().set_modified(UNIX_EPOCH + Duration::from_secs(1_700_000_000)),
        )
        .unwrap();
    let source_bytes = fs::read(&missed).unwrap();
    let source_mtime = fs::metadata(&missed).unwrap().modified().unwrap();
    let opts = watch_lexical_options(&data_dir);
    let storage = Mutex::new(FrankenStorage::open(&opts.db_path).unwrap());
    let index_path = index_dir(&data_dir).unwrap();
    let index = Mutex::new(Some(TantivyIndex::open_or_create(&index_path).unwrap()));
    assert_eq!(
        run_watch_lexical_selection(&opts, &unrelated, &storage, &index).unwrap(),
        1
    );
    let preserved_rows = watch_lexical_canonical_rows(&storage.lock().unwrap());
    let preserved_ids = watch_lexical_search_ids(&index_path, "overflowpreservedneedle");
    assert_eq!(preserved_rows.len(), 1);
    assert_eq!(preserved_ids.len(), 1);
    let roots = [(ConnectorKind::Claude, ScanRoot::local(selected.clone()))];
    let state = Mutex::new(HashMap::from([(ConnectorKind::Claude, i64::MAX / 4)]));

    // Source receipts now make the ordinary directory retry sufficient:
    // a provider timestamp cannot hide an unacknowledged backdated source.
    assert_eq!(
        reindex_paths(
            &opts,
            vec![selected.clone()],
            &roots,
            &state,
            &storage,
            &index,
            &index_path,
            false,
        )
        .unwrap(),
        1
    );
    let recovered_ids = watch_lexical_search_ids(&index_path, "overflowrecoveryneedle");
    assert_eq!(recovered_ids.len(), 1);
    let recovered_rows = watch_lexical_canonical_rows(&storage.lock().unwrap());
    assert_eq!(recovered_rows.len(), 2);

    let backlog = WatchNotificationMailbox::default();
    let rescan = AtomicBool::new(false);
    let (wake, _notifications) = bounded(1);
    for index in 0..=WATCH_PENDING_MAX_PATHS {
        enqueue_watch_notification(
            Ok(notify::Event::new(notify::EventKind::Any)
                .add_path(selected.join(format!("lost-notification-{index}")))),
            &backlog,
            &rescan,
            &wake,
        );
    }
    let mut pending = WatchEventBacklog::default();
    let mut pending_rescan = false;
    collect_watch_notifications(&backlog, &rescan, &mut pending, &mut pending_rescan);
    assert!(pending_rescan);
    dispatch_pending_watch_callback(
        &mut pending,
        &mut pending_rescan,
        &roots,
        &|paths, roots, rebuilding| {
            assert!(paths.is_empty());
            assert!(rebuilding);
            let indexed = reindex_paths(
                &opts,
                roots.iter().map(|(_, root)| root.path.clone()).collect(),
                roots,
                &state,
                &storage,
                &index,
                &index_path,
                rebuilding,
            )?;
            assert_eq!(indexed, 1);
            Ok(BTreeSet::new())
        },
    );
    assert!(!pending_rescan);
    let rows = watch_lexical_canonical_rows(&storage.lock().unwrap());
    assert_eq!(rows, recovered_rows);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], preserved_rows[0]);
    assert_eq!(
        watch_lexical_search_ids(&index_path, "overflowpreservedneedle"),
        preserved_ids
    );
    assert_eq!(
        watch_lexical_search_ids(&index_path, "overflowrecoveryneedle"),
        recovered_ids
    );
    assert_eq!(fs::read(&missed).unwrap(), source_bytes);
    assert_eq!(
        fs::metadata(&missed).unwrap().modified().unwrap(),
        source_mtime
    );
}

/// txc9h: a replay debt saved under another spelling of the same root
/// (here a symlink alias; on Windows the \\?\ form std::fs::canonicalize
/// produced) must still be found and cleared, not stranded by a plain
/// prefix comparison. An unrelated sibling root's debt must not match.
#[cfg(unix)]
#[test]
fn watch_lexical_replay_debt_matches_a_saved_alias_spelling_of_the_root() {
    let tmp = TempDir::new().unwrap();
    let real = tmp.path().join("real-root");
    let sibling = tmp.path().join("real-root-sibling");
    fs::create_dir_all(&real).unwrap();
    fs::create_dir_all(&sibling).unwrap();
    let alias = tmp.path().join("alias-root");
    std::os::unix::fs::symlink(&real, &alias).unwrap();
    let storage = FrankenStorage::open(&tmp.path().join("agent_search.db")).unwrap();
    let root = ScanRoot::local(real.clone());
    let saved = WatchLexicalReplayDebt {
        connector: ConnectorKind::Codex,
        source_id: root.origin.source_id.clone(),
        path: alias.clone(),
        discovery_only: false,
    };
    let unrelated = WatchLexicalReplayDebt {
        connector: ConnectorKind::Codex,
        source_id: root.origin.source_id.clone(),
        path: sibling.clone(),
        discovery_only: false,
    };
    persist_watch_lexical_replay_debt(&storage, &saved).unwrap();
    persist_watch_lexical_replay_debt(&storage, &unrelated).unwrap();
    assert!(
        !alias.starts_with(crate::normalize_path_identity(&real)),
        "the alias must be a different spelling, or this test proves nothing"
    );

    let pending =
        watch_lexical_replay_debt_for_root(&storage, ConnectorKind::Codex, &root).unwrap();
    assert_eq!(pending.len(), 1, "alias debt found; sibling debt not");
    assert_eq!(
        pending[0].path, alias,
        "returned as saved, so clearing hits its key"
    );
    clear_watch_lexical_replay_debt(&storage, &pending[0]).unwrap();
    assert!(
        watch_lexical_replay_debt_for_root(&storage, ConnectorKind::Codex, &root)
            .unwrap()
            .is_empty()
    );
    let sibling_root = ScanRoot::local(sibling.clone());
    assert_eq!(
        watch_lexical_replay_debt_for_root(&storage, ConnectorKind::Codex, &sibling_root)
            .unwrap()
            .len(),
        1,
        "clearing the alias debt left the sibling's debt in place"
    );
}

/// txc9h upgrade case: builds before txc9h saved debts with the
/// std::fs::canonicalize spelling (\\?\C:\... on Windows); identities are
/// now dunce-normalized (C:\...). Such a debt must still match its root.
/// On Unix both spellings agree, so this guards Windows.
#[test]
fn watch_lexical_replay_debt_saved_with_std_canonical_spelling_still_matches() {
    let tmp = TempDir::new().unwrap();
    let real = tmp.path().join("real-root");
    fs::create_dir_all(&real).unwrap();
    let storage = FrankenStorage::open(&tmp.path().join("agent_search.db")).unwrap();
    let root = ScanRoot::local(real.clone());
    let saved = WatchLexicalReplayDebt {
        connector: ConnectorKind::Codex,
        source_id: root.origin.source_id.clone(),
        path: std::fs::canonicalize(&real).unwrap(),
        discovery_only: false,
    };
    persist_watch_lexical_replay_debt(&storage, &saved).unwrap();
    let pending =
        watch_lexical_replay_debt_for_root(&storage, ConnectorKind::Codex, &root).unwrap();
    assert_eq!(
        pending.len(),
        1,
        "a pre-txc9h debt spelling must still match"
    );
    clear_watch_lexical_replay_debt(&storage, &pending[0]).unwrap();
    assert!(
        watch_lexical_replay_debt_for_root(&storage, ConnectorKind::Codex, &root)
            .unwrap()
            .is_empty()
    );
}
