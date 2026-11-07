use crate::cli::commands::analytics::run_analytics::{
    open_franken_analytics_db, open_franken_cli_read_db,
    receive_franken_cli_read_db_open_result_with_hard_timeout,
};
use crate::cli::commands::models::run_models_command::resolve_semantic_index_embedder;
use crate::cli::commands::view::role_to_export_string::index_result_counts_from_progress;
use crate::cli::state::probe_state_db_strict_worker::{
    format_timestamp_millis_rfc3339, probe_state_db,
};
use crate::cli::state::state_db_open_timeout::{
    STATE_DB_OPEN_TIMEOUT, STATUS_COUNT_SCAN_MAX_DB_BYTES, StateDbIntegrity,
    state_db_strict_open_error_message, unpublished_wal_sidecar_bytes,
};
use crate::cli::state::state_meta_json::{
    refresh_state_database_counts_if_needed, state_meta_json, state_meta_json_for_health,
    state_meta_json_for_status,
};
use crate::doctor_recover;
use crate::model::cli_error_kind::ErrorKind as CliErrorKind;
use crate::storage::sqlite::FrankenStorage;
use fs2::FileExt;
use serial_test::serial;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tempfile::TempDir;

pub(crate) fn seed_cli_db() -> (TempDir, PathBuf) {
    let temp = TempDir::new().expect("tempdir");
    let db_path = temp.path().join("agent_search.db");
    let storage = FrankenStorage::open(&db_path).expect("open cass db");
    storage
        .set_last_indexed_at(1_733_000_000_000)
        .expect("set last_indexed_at");
    storage
        .set_last_scan_ts(1_732_999_999_000)
        .expect("set last_scan_ts");
    drop(storage);
    (temp, db_path)
}

/// zn1xn: a full index run that leaves the canonical FTS shadow half-built
/// records a marker that `state_meta_json` (hence `index --json`/`status`)
/// surfaces additively; a later repair clears it and the field is omitted.
#[test]
pub(crate) fn state_meta_surfaces_pending_fallback_fts_repair_additively() {
    let (temp, db_path) = seed_cli_db();

    // No marker → the field is omitted (existing JSON is byte-identical).
    let clean = state_meta_json(temp.path(), &db_path, 3600, true);
    assert!(
        clean.pointer("/index/fallback_fts_repair").is_none(),
        "fallback_fts_repair must be omitted when no repair is pending"
    );

    {
        let storage = FrankenStorage::open(&db_path).expect("reopen cass db");
        storage
            .record_fallback_fts_repair_pending(Some(
                "inserting 4000 rows into fts_messages during streaming FTS maintenance: \
                     fts5: corrupt %_data record: segment leaf term offset exceeds u16",
            ))
            .expect("record pending marker");
        // Storage-level round-trip through the symmetric read accessor.
        assert!(
            storage
                .read_fallback_fts_repair_pending()
                .expect("read pending marker")
                .is_some_and(|d| d.contains("corrupt %_data record")),
            "the recorded marker must read back through read_fallback_fts_repair_pending"
        );
        drop(storage);
    }
    let pending = state_meta_json(temp.path(), &db_path, 3600, true);
    assert_eq!(
        pending.pointer("/index/fallback_fts_repair/pending"),
        Some(&serde_json::Value::Bool(true)),
        "a persisted marker must surface as fallback_fts_repair.pending=true"
    );
    assert_eq!(
        pending.pointer("/index/fallback_fts_repair/retired_not_viable"),
        Some(&serde_json::Value::Bool(false)),
        "a half-built shadow is not a retirement"
    );
    assert!(
        pending
            .pointer("/index/fallback_fts_repair/detail")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|d| d.contains("corrupt %_data record")),
        "the detail must carry the failure reason"
    );

    // GH #495: the marker a not-viable retirement leaves behind says why the
    // shadow is absent; status must not call that settled state pending.
    {
        let storage = FrankenStorage::open(&db_path).expect("reopen cass db");
        storage
            .record_fallback_fts_repair_pending(Some(
                &crate::storage::sqlite::fts_shadow_not_viable_detail(256_396, 100_000),
            ))
            .expect("record retirement marker");
        drop(storage);
    }
    let retired = state_meta_json(temp.path(), &db_path, 3600, true);
    assert_eq!(
        retired.pointer("/index/fallback_fts_repair/pending"),
        Some(&serde_json::Value::Bool(false)),
        "a settled retirement is not a pending repair: {retired}"
    );
    assert_eq!(
        retired.pointer("/index/fallback_fts_repair/retired_not_viable"),
        Some(&serde_json::Value::Bool(true))
    );
    assert!(
        retired
            .pointer("/index/fallback_fts_repair/detail")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|d| d.contains("settled state, not a pending repair")),
        "the retirement keeps its reason"
    );

    {
        let storage = FrankenStorage::open(&db_path).expect("reopen cass db");
        storage
            .record_fallback_fts_repair_pending(None)
            .expect("clear pending marker");
        drop(storage);
    }
    let cleared = state_meta_json(temp.path(), &db_path, 3600, true);
    assert!(
        cleared.pointer("/index/fallback_fts_repair").is_none(),
        "clearing the marker must omit the field again"
    );
}

/// zn1xn F4: a non-watch run that persistently defers inline lexical updates
/// tracks a consecutive-run streak, surfaced additively in status/index
/// JSON, and cleared once a run completes without deferring.
#[test]
pub(crate) fn state_meta_surfaces_persistent_lexical_repair_deferral_streak() {
    let (temp, db_path) = seed_cli_db();

    let clean = state_meta_json(temp.path(), &db_path, 3600, true);
    assert!(
        clean.pointer("/index/lexical_repair_deferred").is_none(),
        "lexical_repair_deferred must be omitted when no streak is active"
    );

    {
        let storage = FrankenStorage::open(&db_path).expect("reopen cass db");
        assert_eq!(
            storage
                .record_lexical_repair_deferred("deferred: streaming OOM on one source")
                .expect("record deferral"),
            1,
            "first deferral starts the streak at 1"
        );
        assert_eq!(
            storage
                .record_lexical_repair_deferred("deferred: streaming OOM again")
                .expect("record deferral"),
            2,
            "a consecutive deferral advances the streak"
        );
        assert_eq!(
            storage.read_lexical_repair_deferred().expect("read streak"),
            Some((2, "deferred: streaming OOM again".to_string()))
        );
        drop(storage);
    }
    let deferred = state_meta_json(temp.path(), &db_path, 3600, true);
    assert_eq!(
        deferred.pointer("/index/lexical_repair_deferred/consecutive_runs"),
        Some(&serde_json::json!(2)),
        "the streak length must surface as consecutive_runs"
    );
    assert!(
        deferred
            .pointer("/index/lexical_repair_deferred/reason")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|r| r.contains("OOM again")),
        "the last deferral reason must surface"
    );

    {
        let storage = FrankenStorage::open(&db_path).expect("reopen cass db");
        storage
            .clear_lexical_repair_deferred()
            .expect("clear streak");
        assert_eq!(
            storage.read_lexical_repair_deferred().expect("read streak"),
            None
        );
        drop(storage);
    }
    let cleared = state_meta_json(temp.path(), &db_path, 3600, true);
    assert!(
        cleared.pointer("/index/lexical_repair_deferred").is_none(),
        "clearing the streak must omit the field again"
    );
}

pub(crate) fn write_index_lock_metadata(
    lock_path: &Path,
    lock_file: &mut std::fs::File,
    metadata: &str,
) {
    use std::io::{Seek, SeekFrom, Write};

    lock_file.set_len(0).expect("truncate index lock metadata");
    lock_file
        .seek(SeekFrom::Start(0))
        .expect("rewind index lock metadata");
    lock_file
        .write_all(metadata.as_bytes())
        .expect("write lock metadata");
    lock_file.flush().expect("flush lock metadata");
    crate::search::asset_state::write_index_run_lock_metadata_sidecar(lock_path, metadata)
        .expect("write lock metadata sidecar");
}

#[test]
pub(crate) fn analytics_db_open_is_readonly() {
    let (temp, _db_path) = seed_cli_db();
    let data_dir = Some(temp.path().to_path_buf());
    let conn = open_franken_analytics_db(&data_dir, None).expect("open readonly analytics db");

    let err = conn
        .execute("CREATE TABLE cli_readonly_probe(id INTEGER PRIMARY KEY);")
        .expect_err("analytics reader must not accept writes");
    let message = err.to_string().to_lowercase();
    assert!(
        message.contains("readonly") || message.contains("query_only"),
        "unexpected readonly failure surface: {message}"
    );
}

#[test]
pub(crate) fn cli_read_db_hard_timeout_reports_open_timeout() {
    let (_tx, rx) = std::sync::mpsc::channel();
    let err = receive_franken_cli_read_db_open_result_with_hard_timeout(
        rx,
        "/tmp/agent_search.db".to_string(),
        "doctor database health".to_string(),
        Duration::from_millis(1),
    )
    .expect_err("open receiver timeout should produce a cli error");

    assert_eq!(err.kind, CliErrorKind::DbOpen.kind_str());
    assert!(err.retryable);
    assert!(
        err.message.contains("open timed out"),
        "timeout error should be actionable, got: {}",
        err.message
    );
}

#[test]
pub(crate) fn cli_read_db_refuses_corrupt_file_without_writable_fallback() {
    let temp = TempDir::new().expect("tempdir");
    let db_path = temp.path().join("agent_search.db");
    let corrupt_bytes = b"not a sqlite database";
    std::fs::write(&db_path, corrupt_bytes).expect("write corrupt db bytes");

    let err = match open_franken_cli_read_db(
        db_path.clone(),
        "corrupt preservation test",
        Duration::from_millis(250),
    ) {
        Ok(_) => panic!("corrupt db should not open"),
        Err(err) => err,
    };

    assert_eq!(err.kind, CliErrorKind::DbOpen.kind_str());
    assert!(
        err.message.contains("raw readonly open failed"),
        "error should report the readonly failure chain: {}",
        err.message
    );
    assert!(
        !err.message.contains("raw open failed"),
        "read helper must not use a writable fallback: {}",
        err.message
    );
    assert_eq!(
        std::fs::read(&db_path).expect("read db after failed open"),
        corrupt_bytes
    );
}

#[test]
pub(crate) fn probe_state_db_reads_meta_without_count_scan() {
    let (_temp, db_path) = seed_cli_db();
    let snapshot = probe_state_db(&db_path, "status", Duration::from_millis(250), false);

    assert!(snapshot.opened, "state probe should open the database");
    assert_eq!(snapshot.last_indexed_at, Some(1_733_000_000_000));
    assert_eq!(snapshot.last_scan_ts, Some(1_732_999_999_000));
    assert_eq!(snapshot.watch_lexical_replay_pending, Some(false));
    assert!(snapshot.counts_skipped, "count scan should remain disabled");
    assert_eq!(snapshot.conversation_count, 0);
    assert_eq!(snapshot.message_count, 0);
    assert!(
        snapshot.open_error.is_none(),
        "state probe should not report an error: {:?}",
        snapshot.open_error
    );
}

/// k2k20 ask #2: the status-class probe opens strictly read-only on the
/// dedicated-owner lane, still reads watermarks, counts and the bounded
/// integrity probe, and leaves the archive bytes untouched.
#[test]
pub(crate) fn status_state_probe_counts_and_integrity_without_mutating_archive() {
    let (_temp, db_path) = seed_cli_db();
    let before = std::fs::read(&db_path).expect("read db before probe");
    let snapshot = probe_state_db(&db_path, "status", STATE_DB_OPEN_TIMEOUT, true);

    assert!(
        snapshot.opened,
        "strict probe should open the seeded archive"
    );
    assert!(!snapshot.open_skipped);
    assert!(!snapshot.counts_skipped, "counts were requested");
    // The seeded archive carries watermarks only; the counts are read
    // (not skipped) and honestly zero.
    assert_eq!(snapshot.conversation_count, 0);
    assert_eq!(snapshot.message_count, 0);
    assert_eq!(snapshot.last_indexed_at, Some(1_733_000_000_000));
    assert_eq!(snapshot.integrity, Some(StateDbIntegrity::Ok));
    assert!(
        snapshot.open_error.is_none(),
        "strict probe should not report an error: {:?}",
        snapshot.open_error
    );
    assert_eq!(
        std::fs::read(&db_path).expect("read db after probe"),
        before,
        "a readiness read must not modify the archive"
    );
}

/// The strict lane never performs the ordinary opener's recovery writes;
/// a hard open failure next to an unpublished WAL sidecar names the
/// command that recovers it.
#[test]
pub(crate) fn strict_open_error_message_names_recovery_for_unpublished_wal() {
    let temp = TempDir::new().expect("tempdir");
    let db_path = temp.path().join("agent_search.db");
    std::fs::write(&db_path, b"db").expect("write db placeholder");
    std::fs::write(temp.path().join("agent_search.db-wal"), vec![0_u8; 4096])
        .expect("write wal placeholder");
    let err = anyhow::anyhow!("unable to open database file");
    let message = state_db_strict_open_error_message(&db_path, "status", &err, false);
    assert!(message.contains("4096-byte WAL sidecar"), "{message}");
    assert!(message.contains("cass index"), "{message}");

    let busy = state_db_strict_open_error_message(&db_path, "status", &err, true);
    assert!(!busy.contains("WAL sidecar"), "{busy}");
    assert!(unpublished_wal_sidecar_bytes(temp.path().join("missing.db").as_path()).is_none());
}

/// GH #503: the strict open's legacy-shadow refusal names the derived-shadow
/// repair; a refusal naming a canonical table does not.
#[test]
pub(crate) fn strict_open_error_message_is_truthful_about_the_503_legacy_shadow_catalog() {
    let temp = TempDir::new().expect("tempdir");
    let db_path = temp.path().join("agent_search.db");
    let legacy = anyhow::anyhow!(
            "database disk image is malformed: sqlite_master is missing implicit autoindex slot 1 for table `fts_messages_config`"
        )
        .context("strictly opening dedicated-owner frankensqlite db readonly");
    let message = state_db_strict_open_error_message(&db_path, "status", &legacy, false);
    assert!(
        message.contains("not the conversations or messages"),
        "{message}"
    );
    if doctor_recover::ENGINE_REPAIRS_MISSING_FTS5_SHADOW_AUTOINDEX {
        assert!(
            message.contains("cass doctor --rebuild-canonical-fts --dry-run --json"),
            "{message}"
        );
    } else {
        // GH #503: the pinned engine refuses this catalog even for the
        // shadow repair, so the hint must not send users there.
        assert!(!message.contains("rebuild-canonical-fts"), "{message}");
        assert!(message.contains("GH #503"), "{message}");
        assert!(
            message.contains("do not run 'cass doctor --fix'"),
            "{message}"
        );
    }

    // Other shadow-catalog refusals keep the shadow repair.
    let other_shadow = anyhow::anyhow!(
        "database disk image is malformed: sqlite_master row for `fts_messages_data` is invalid"
    );
    let message = state_db_strict_open_error_message(&db_path, "status", &other_shadow, false);
    assert!(
        message.contains("cass doctor --rebuild-canonical-fts --dry-run --json"),
        "{message}"
    );

    let canonical = anyhow::anyhow!(
        "database disk image is malformed: sqlite_master is missing implicit autoindex slot 1 for table `conversations`"
    );
    let message = state_db_strict_open_error_message(&db_path, "status", &canonical, false);
    assert!(!message.contains("rebuild-canonical-fts"), "{message}");
}

#[test]
pub(crate) fn strict_open_error_message_distinguishes_wal_recovery_from_plain_busy() {
    for wal_bytes in [None, Some(32), Some(4096)] {
        let temp = TempDir::new().expect("tempdir");
        let db_path = temp.path().join("agent_search.db");
        if let Some(bytes) = wal_bytes {
            std::fs::write(temp.path().join("agent_search.db-wal"), vec![0_u8; bytes])
                .expect("write isolated WAL shape");
        }
        let err = anyhow::Error::new(crate::franken_sync::FrankenError::BusyRecovery)
            .context("opening database through dedicated owner");
        let message = state_db_strict_open_error_message(&db_path, "status", &err, true);
        assert!(
            message.contains("WAL-index recovery is required"),
            "{message}"
        );
        assert!(message.contains("another connection"), "{message}");
        assert!(
            message.contains("leaves the archive unchanged"),
            "{message}"
        );
        assert!(message.contains("cass index"), "{message}");
        assert!(!message.contains("checkpoint"), "{message}");

        let err = anyhow::Error::new(crate::franken_sync::FrankenError::Busy)
            .context("opening database through dedicated owner");
        let message = state_db_strict_open_error_message(&db_path, "status", &err, true);
        assert!(!message.contains("WAL-index recovery"), "{message}");
        assert!(!message.contains("cass index"), "{message}");
    }
}

#[test]
pub(crate) fn status_state_probes_large_regular_db_instead_of_trusting_index_mtime() {
    let temp = TempDir::new().expect("tempdir");
    let db_path = temp.path().join("agent_search.db");
    let file = std::fs::File::create(&db_path).expect("create sparse db placeholder");
    file.set_len(STATUS_COUNT_SCAN_MAX_DB_BYTES + 4096)
        .expect("grow sparse placeholder");
    drop(file);

    let state = state_meta_json_for_status(temp.path(), &db_path, 60);
    let database = state
        .get("database")
        .and_then(serde_json::Value::as_object)
        .expect("database state");

    assert_eq!(database.get("exists").and_then(|v| v.as_bool()), Some(true));
    assert_eq!(
        database.get("open_skipped").and_then(|v| v.as_bool()),
        Some(false)
    );
    assert_eq!(
        database.get("opened").and_then(|v| v.as_bool()),
        Some(false)
    );
    assert!(
        database
            .get("open_error")
            .and_then(|v| v.as_str())
            .is_some_and(|err| !err.is_empty())
    );
}

#[test]
pub(crate) fn status_state_marks_scan_ahead_of_projection_stale() {
    let (temp, db_path) = seed_cli_db();
    {
        let storage = FrankenStorage::open(&db_path).expect("reopen cass db");
        storage
            .set_last_scan_ts(1_733_000_010_000)
            .expect("set scan ahead");
    }

    let state = state_meta_json_for_status(temp.path(), &db_path, 60);
    let index = state
        .get("index")
        .and_then(serde_json::Value::as_object)
        .expect("index state");

    assert_eq!(index.get("fresh").and_then(|v| v.as_bool()), Some(false));
    assert_eq!(index.get("stale").and_then(|v| v.as_bool()), Some(true));
    assert_eq!(index.get("status").and_then(|v| v.as_str()), Some("stale"));
    assert!(
        index
            .get("reason")
            .and_then(|v| v.as_str())
            .is_some_and(|reason| reason.contains("last_scan_ts is newer")),
        "status reason should identify the scan/projection gap: {index:?}"
    );
}

/// [coding_agent_session_search #301 truthfulness reconciliation]
/// `cass health` and `cass status` MUST agree on index staleness for
/// the SAME DB. Pre-fix, health passed skip_db_open=true and never
/// read `last_scan_ts`, so the scan-ahead-of-projection check could
/// not fire — health reported `fresh`/`status=ready` while status
/// reported `stale`. Now health does a watermarks-only probe so the
/// staleness inputs are identical. Guard both halves: the verdicts
/// match AND health still honors its gi4oy/d0rmo contract (the open
/// is assumed-good with null counts; only the watermarks are read).
#[test]
pub(crate) fn health_and_status_agree_on_scan_ahead_of_projection_stale() {
    let (temp, db_path) = seed_cli_db();
    {
        let storage = FrankenStorage::open(&db_path).expect("reopen cass db");
        storage
            .set_last_scan_ts(1_733_000_010_000)
            .expect("set scan ahead");
    }

    let status = state_meta_json_for_status(temp.path(), &db_path, 60);
    let health = state_meta_json_for_health(temp.path(), &db_path, 60);

    for (label, state) in [("status", &status), ("health", &health)] {
        let index = state
            .get("index")
            .and_then(serde_json::Value::as_object)
            .unwrap_or_else(|| panic!("{label} index state"));
        assert_eq!(
            index.get("fresh").and_then(|v| v.as_bool()),
            Some(false),
            "{label} must report fresh=false on scan-ahead-of-projection"
        );
        assert_eq!(
            index.get("stale").and_then(|v| v.as_bool()),
            Some(true),
            "{label} must report stale=true on scan-ahead-of-projection"
        );
        assert_eq!(
            index.get("status").and_then(|v| v.as_str()),
            Some("stale"),
            "{label} must report status=stale on scan-ahead-of-projection"
        );
    }

    // Health must STILL honor the gi4oy/d0rmo fast-surface contract:
    // the watermarks-only probe reads the meta watermarks but reports
    // open_skipped=true, counts_skipped=true, and null counts so the
    // 0-counts are an intentional skip signal, not missing data.
    let health_db = health
        .get("database")
        .and_then(serde_json::Value::as_object)
        .expect("health database state");
    assert_eq!(
        health_db.get("open_skipped").and_then(|v| v.as_bool()),
        Some(true),
        "health watermarks-only probe must still report open_skipped=true"
    );
    assert_eq!(
        health_db.get("counts_skipped").and_then(|v| v.as_bool()),
        Some(true),
        "health watermarks-only probe must still report counts_skipped=true"
    );
    assert!(
        health_db
            .get("conversations")
            .is_some_and(serde_json::Value::is_null),
        "health must still report conversations=null (counts skipped): {health_db:?}"
    );
    assert!(
        health_db
            .get("messages")
            .is_some_and(serde_json::Value::is_null),
        "health must still report messages=null (counts skipped): {health_db:?}"
    );
}

#[test]
pub(crate) fn gh538_watch_replay_debt_overrides_recent_publication_and_live_lease() {
    let (temp, db_path) = seed_cli_db();
    let now = FrankenStorage::now_millis();
    {
        let storage = FrankenStorage::open(&db_path).expect("reopen cass db");
        storage
            .set_last_indexed_at(now)
            .expect("recent publication");
        storage
            .set_last_scan_ts(now.saturating_sub(1_000))
            .expect("scan covered by publication");
        // Prefix neighbors must not count as watch debt.
        storage
            .raw()
            .execute_batch(
                "INSERT INTO meta(key, value) VALUES \
                     ('watch_lexical_replay_v0:old', '{}'), \
                     ('watch_lexical_replay_v1;neighbor', '{}');",
            )
            .expect("insert unrelated metadata");
    }
    let index_path = crate::search::tantivy::expected_index_dir(temp.path());
    let mut index = crate::search::tantivy::TantivyIndex::open_or_create(&index_path)
        .expect("create real lexical generation");
    index.commit().expect("publish lexical generation");
    drop(index);

    let lock_path = temp.path().join("index-run.lock");
    let mut lock_file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .expect("open watch lease");
    lock_file.try_lock_exclusive().expect("hold watch lease");
    write_index_lock_metadata(
        &lock_path,
        &mut lock_file,
        &format!(
            "pid={}\nstarted_at_ms={now}\nupdated_at_ms={now}\ndb_path={}\nmode=watch\n",
            std::process::id(),
            db_path.display()
        ),
    );

    let clean = state_meta_json_for_health(temp.path(), &db_path, 3600);
    assert_eq!(clean["index"]["status"], "ready", "{clean}");
    assert_eq!(clean["pending"]["lexical_replay"], false);
    assert_eq!(clean["pending"]["watch_active"], true);

    {
        let storage = FrankenStorage::open(&db_path).expect("open debt writer");
        // A corrupt payload is still outstanding work. Health must not
        // parse it away, count its roots as sessions, or certify readiness.
        storage
            .raw()
            .execute_batch(
                "INSERT INTO meta(key, value) \
                     VALUES('watch_lexical_replay_v1:pending-source', 'malformed debt');",
            )
            .expect("persist unresolved replay");
    }
    let before = std::fs::read(&db_path).expect("snapshot canonical bytes");
    for state in [
        state_meta_json_for_status(temp.path(), &db_path, 3600),
        state_meta_json_for_health(temp.path(), &db_path, 3600),
    ] {
        assert_eq!(state["pending"]["lexical_replay"], true, "{state}");
        assert_eq!(state["pending"]["watch_active"], true);
        assert_eq!(state["pending"]["sessions"], 0);
        assert_eq!(state["index"]["status"], "stale");
        assert_eq!(state["index"]["fresh"], false);
        assert_eq!(state["index"]["partial"], true);
        assert_eq!(
            state["index"]["last_indexed_at"],
            clean["index"]["last_indexed_at"]
        );
        assert!(
            state["index"]["reason"]
                .as_str()
                .is_some_and(|reason| reason.contains("durable watch"))
        );
    }
    assert_eq!(
        std::fs::read(&db_path).expect("canonical bytes after readiness reads"),
        before,
        "readiness probes cannot discharge or rewrite recovery debt"
    );

    {
        let storage = FrankenStorage::open(&db_path).expect("open completion writer");
        storage
            .raw()
            .execute_batch("DELETE FROM meta WHERE key = 'watch_lexical_replay_v1:pending-source';")
            .expect("complete exact replay debt");
    }
    let recovered = state_meta_json_for_health(temp.path(), &db_path, 3600);
    assert_eq!(recovered["pending"]["lexical_replay"], false);
    assert_eq!(recovered["index"]["status"], "ready", "{recovered}");
    assert_eq!(recovered["index"]["partial"], false);
}

#[test]
pub(crate) fn gh538_failed_watch_replay_probe_is_unknown_and_cannot_report_ready() {
    let temp = TempDir::new().expect("tempdir");
    let db_path = temp.path().join("agent_search.db");
    {
        let conn = crate::franken_sync::Connection::open(db_path.to_string_lossy().as_ref())
            .expect("open malformed metadata fixture");
        conn.execute_batch("CREATE TABLE meta (unexpected TEXT);")
            .expect("create metadata without its key column");
    }
    let index_path = crate::search::tantivy::expected_index_dir(temp.path());
    let mut index = crate::search::tantivy::TantivyIndex::open_or_create(&index_path)
        .expect("create real lexical generation");
    index.commit().expect("publish lexical generation");
    drop(index);

    let state = state_meta_json_for_health(temp.path(), &db_path, 3600);
    assert!(state["pending"]["lexical_replay"].is_null(), "{state}");
    assert_eq!(state["index"]["fresh"], false);
    assert_eq!(state["index"]["status"], "stale", "{state}");
    assert!(
        state["index"]["reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("could not be checked"))
    );
}

#[test]
pub(crate) fn status_state_still_probes_malformed_non_file_db_path() {
    let temp = TempDir::new().expect("tempdir");
    let db_path = temp.path().join("agent_search.db");
    std::fs::create_dir_all(&db_path).expect("create directory at db path");

    let state = state_meta_json_for_status(temp.path(), &db_path, 60);
    let database = state
        .get("database")
        .and_then(serde_json::Value::as_object)
        .expect("database state");

    assert_eq!(database.get("exists").and_then(|v| v.as_bool()), Some(true));
    assert_eq!(
        database.get("open_skipped").and_then(|v| v.as_bool()),
        Some(false)
    );
    assert_eq!(
        database.get("opened").and_then(|v| v.as_bool()),
        Some(false)
    );
    assert!(
        database
            .get("open_error")
            .and_then(|v| v.as_str())
            .is_some_and(|err| !err.is_empty())
    );
}

#[test]
pub(crate) fn status_state_surfaces_ingest_quarantine_without_marking_index_corrupt() {
    let temp = TempDir::new().expect("tempdir");
    let quarantine_dir = temp.path().join("quarantine");
    std::fs::create_dir_all(&quarantine_dir).expect("create quarantine dir");
    std::fs::write(
        quarantine_dir.join("index_ingest_poison.jsonl"),
        serde_json::json!({
            "schema_version": 1,
            "conversation_id": "tester|/logs/demo.jsonl|/workspace/demo|poison|1|2|1",
            "schema_version_at_quarantine": crate::storage::sqlite::CURRENT_SCHEMA_VERSION,
            "first_quarantined_at_ms": 10,
            "last_attempt_at_ms": 20,
            "attempt_count": 1,
            "reason": "index-ingest-out-of-memory",
            "agent_slug": "tester",
            "external_id": "poison",
            "source_path": "/logs/demo.jsonl",
            "workspace": "/workspace/demo",
            "started_at": 1,
            "ended_at": 2,
            "message_count": 1
        })
        .to_string(),
    )
    .expect("write quarantine record");

    let db_path = temp.path().join("agent_search.db");
    let state = state_meta_json_for_status(temp.path(), &db_path, 60);

    assert_eq!(
        state["index"]["quarantined_conversations"],
        serde_json::json!(1)
    );
    assert_eq!(
        state["ingest_quarantine"]["status"],
        serde_json::json!("degraded")
    );
    assert_eq!(
        state["ingest_quarantine"]["quarantined_conversations"],
        serde_json::json!(1)
    );
}

#[test]
pub(crate) fn refresh_state_database_counts_materializes_small_db_count_envelope()
-> anyhow::Result<()> {
    let (_temp, db_path) = seed_cli_db();
    let mut state = serde_json::json!({
        "database": {
            "opened": true,
            "conversations": serde_json::Value::Null,
            "messages": serde_json::Value::Null,
            "counts_skipped": true
        }
    });

    refresh_state_database_counts_if_needed(&mut state, &db_path, "status");

    let database = state
        .get("database")
        .and_then(|value| value.as_object())
        .ok_or_else(|| anyhow::anyhow!("database object missing"))?;
    anyhow::ensure!(
        database
            .get("counts_skipped")
            .and_then(|value| value.as_bool())
            == Some(false),
        "small DB count refresh should clear counts_skipped"
    );
    anyhow::ensure!(
        database
            .get("conversations")
            .and_then(|value| value.as_i64())
            == Some(0),
        "small DB count refresh should materialize conversation count"
    );
    anyhow::ensure!(
        database.get("messages").and_then(|value| value.as_i64()) == Some(0),
        "small DB count refresh should materialize message count"
    );
    Ok(())
}

#[test]
pub(crate) fn refresh_state_database_counts_keeps_large_db_counts_skipped() -> anyhow::Result<()> {
    let (_temp, db_path) = seed_cli_db();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&db_path)?
        .set_len(STATUS_COUNT_SCAN_MAX_DB_BYTES + 4096)?;
    let mut state = serde_json::json!({
        "database": {
            "opened": true,
            "conversations": serde_json::Value::Null,
            "messages": serde_json::Value::Null,
            "counts_skipped": true
        }
    });

    refresh_state_database_counts_if_needed(&mut state, &db_path, "status");

    let database = state
        .get("database")
        .and_then(|value| value.as_object())
        .ok_or_else(|| anyhow::anyhow!("database object missing"))?;
    anyhow::ensure!(
        database
            .get("counts_skipped")
            .and_then(|value| value.as_bool())
            == Some(true),
        "large DB count refresh should keep counts_skipped"
    );
    anyhow::ensure!(
        database
            .get("conversations")
            .is_some_and(serde_json::Value::is_null),
        "large DB count refresh should leave conversation count null"
    );
    anyhow::ensure!(
        database
            .get("messages")
            .is_some_and(serde_json::Value::is_null),
        "large DB count refresh should leave message count null"
    );
    Ok(())
}

#[test]
pub(crate) fn index_result_counts_from_progress_only_uses_exact_totals() {
    let progress = crate::indexer::IndexingProgress::default();
    {
        let mut stats = progress.stats.lock().expect("stats lock");
        stats.total_conversations = 7;
        stats.total_messages = 11;
        stats.total_counts_exact = true;
    }
    assert_eq!(index_result_counts_from_progress(&progress), Some((7, 11)));

    let progress = crate::indexer::IndexingProgress::default();
    {
        let mut stats = progress.stats.lock().expect("stats lock");
        stats.total_conversations = 7;
        stats.total_messages = 11;
    }
    assert_eq!(index_result_counts_from_progress(&progress), None);
}

/// Regression for CASS #192: when `total_counts_exact` is false the code
/// previously reopened the live DB through frankensqlite to collect
/// `SELECT COUNT(*)` values, which triggered `Connection::open ->
/// reload_memdb_from_pager_with_mode` and advanced the DB fingerprint past
/// the just-written lexical checkpoint. The post-#192 path derives counts
/// from `IndexingProgress::stats` directly (exact-or-observed), so the
/// success-path count collection cannot reopen the DB and cannot
/// invalidate the checkpoint fingerprint.
#[test]
pub(crate) fn index_result_counts_fall_back_to_observed_totals_without_reopen() {
    let progress = crate::indexer::IndexingProgress::default();
    {
        let mut stats = progress.stats.lock().expect("stats lock");
        stats.total_conversations = 5;
        stats.total_messages = 42;
        // deliberately leave total_counts_exact = false to mirror the
        // scenario the user reported where the reopen fallback used to fire
    }

    // Mirror the resolution logic in run_index_with_data so the fallback
    // chain is covered end-to-end without needing to stand up a full DB.
    let resolved = index_result_counts_from_progress(&progress)
        .or_else(|| {
            let stats = progress.stats.lock().ok()?;
            Some((
                i64::try_from(stats.total_conversations).unwrap_or(i64::MAX),
                i64::try_from(stats.total_messages).unwrap_or(i64::MAX),
            ))
        })
        .unwrap_or((0, 0));

    assert_eq!(
        resolved,
        (5, 42),
        "fallback must use observed progress stats, not reopen the live DB"
    );
}

pub(crate) struct EnvGuard {
    pub(crate) key: &'static str,
    pub(crate) previous: Option<String>,
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        if let Some(value) = &self.previous {
            unsafe {
                std::env::set_var(self.key, value);
            }
        } else {
            unsafe {
                std::env::remove_var(self.key);
            }
        }
    }
}

pub(crate) fn set_env(key: &'static str, value: &str) -> EnvGuard {
    let previous = dotenvy::var(key).ok();
    unsafe {
        std::env::set_var(key, value);
    }
    EnvGuard { key, previous }
}

#[test]
#[serial]
pub(crate) fn unsupported_semantic_policy_does_not_route_to_unloadable_embedder() {
    let _embedder = set_env("CASS_SEMANTIC_EMBEDDER", "snowflake-arctic-s");

    assert_eq!(resolve_semantic_index_embedder(None), "fastembed");
    assert_eq!(
        resolve_semantic_index_embedder(Some("fastembed")),
        "fastembed"
    );
    assert_eq!(resolve_semantic_index_embedder(Some("minilm")), "minilm");
    assert_eq!(resolve_semantic_index_embedder(Some("hash")), "hash");
}

/// GH #381 (ntxs1): status shows where an interrupted expected-docs scan
/// stopped, from the position file such a run leaves beside the index,
/// and nothing for a position file that belongs to another archive.
#[test]
#[serial]
pub(crate) fn state_meta_json_reports_interrupted_expected_docs_scan_position() {
    let (temp, db_path) = seed_cli_db();
    let index_root = temp.path().join("index");
    std::fs::create_dir_all(&index_root).expect("index root");
    let partial_path = index_root.join(".expected-lexical-docs.partial.json");
    // The on-disk shape an interrupted scan saves (memo schema 2).
    let write_partial = |db: &Path| {
        let partial = serde_json::json!({
            "schema_version": 2,
            "db_path": db.display().to_string(),
            "identity": {
                "total_conversations": 9,
                "max_conversation_id": 9,
                "max_message_id": 40,
                "total_messages": 40,
                "content_cap_bytes": 8_388_608
            },
            "last_conversation_id": 4,
            "expected_docs": 17
        });
        std::fs::write(&partial_path, serde_json::to_vec(&partial).unwrap())
            .expect("write scan position");
    };

    write_partial(&db_path);
    let state = state_meta_json(temp.path(), &db_path, 60, true);
    let scan = &state["rebuild"]["expected_docs_scan"];
    assert_eq!(scan["last_conversation_id"].as_i64(), Some(4), "{scan}");
    assert_eq!(scan["max_conversation_id"].as_i64(), Some(9), "{scan}");
    assert_eq!(scan["expected_docs_so_far"].as_u64(), Some(17), "{scan}");
    assert!(scan["saved_at"].is_string(), "{scan}");

    write_partial(&temp.path().join("another-archive.db"));
    let state = state_meta_json(temp.path(), &db_path, 60, true);
    assert!(
        state["rebuild"]["expected_docs_scan"].is_null(),
        "{}",
        state["rebuild"]
    );
}

#[test]
#[serial]
pub(crate) fn state_meta_json_reports_lexical_rebuild_pipeline_settings() {
    let _responsiveness = set_env("CASS_RESPONSIVENESS_DISABLE", "1");
    let _workers = set_env("CASS_TANTIVY_REBUILD_WORKERS", "9");
    let _reserved_cores = set_env("CASS_TANTIVY_REBUILD_RESERVED_CORES", "4");
    let _controller_mode = set_env("CASS_TANTIVY_REBUILD_CONTROLLER_MODE", "steady");
    let _controller_clear_samples =
        set_env("CASS_TANTIVY_REBUILD_CONTROLLER_RESTORE_CLEAR_SAMPLES", "5");
    let _controller_hold_ms = set_env("CASS_TANTIVY_REBUILD_CONTROLLER_RESTORE_HOLD_MS", "2345");
    let _controller_loadavg_high = set_env(
        "CASS_TANTIVY_REBUILD_CONTROLLER_LOADAVG_HIGH_WATERMARK_1M",
        "7.5",
    );
    let _controller_loadavg_low = set_env(
        "CASS_TANTIVY_REBUILD_CONTROLLER_LOADAVG_LOW_WATERMARK_1M",
        "6.25",
    );
    let _steady_fetch = set_env("CASS_TANTIVY_REBUILD_BATCH_FETCH_CONVERSATIONS", "210");
    let _startup_fetch = set_env(
        "CASS_TANTIVY_REBUILD_INITIAL_BATCH_FETCH_CONVERSATIONS",
        "21",
    );
    let _steady_conversations = set_env("CASS_TANTIVY_REBUILD_COMMIT_EVERY_CONVERSATIONS", "410");
    let _startup_conversations = set_env(
        "CASS_TANTIVY_REBUILD_INITIAL_COMMIT_EVERY_CONVERSATIONS",
        "41",
    );
    let _steady_messages = set_env("CASS_TANTIVY_REBUILD_COMMIT_EVERY_MESSAGES", "510");
    let _startup_messages = set_env("CASS_TANTIVY_REBUILD_INITIAL_COMMIT_EVERY_MESSAGES", "51");
    let _steady_message_bytes =
        set_env("CASS_TANTIVY_REBUILD_COMMIT_EVERY_MESSAGE_BYTES", "610000");
    let _startup_message_bytes = set_env(
        "CASS_TANTIVY_REBUILD_INITIAL_COMMIT_EVERY_MESSAGE_BYTES",
        "61000",
    );
    let _pipeline_channel = set_env("CASS_TANTIVY_REBUILD_PIPELINE_CHANNEL_SIZE", "4");
    let _page_prep_workers = set_env("CASS_TANTIVY_REBUILD_PAGE_PREP_WORKERS", "6");
    let _pipeline_bytes = set_env(
        "CASS_TANTIVY_REBUILD_PIPELINE_MAX_MESSAGE_BYTES_IN_FLIGHT",
        "888888",
    );
    let _writer_threads = set_env("CASS_TANTIVY_MAX_WRITER_THREADS", "2");
    let _shard_builders = set_env("CASS_TANTIVY_REBUILD_STAGED_SHARD_BUILDERS", "4");
    let _merge_workers = set_env("CASS_TANTIVY_REBUILD_STAGED_MERGE_WORKERS", "2");

    let (temp, db_path) = seed_cli_db();
    let state = state_meta_json(temp.path(), &db_path, 60, true);
    let pipeline = &state["rebuild"]["pipeline"];

    assert_eq!(pipeline["workers"].as_u64(), Some(9));
    let available_parallelism = pipeline["available_parallelism"]
        .as_u64()
        .expect("available_parallelism is reported");
    assert_eq!(
        pipeline["reserved_cores"].as_u64(),
        Some(4_u64.min(available_parallelism.saturating_sub(1)))
    );
    assert_eq!(pipeline["controller_mode"].as_str(), Some("steady"));
    assert_eq!(
        pipeline["controller_restore_clear_samples"].as_u64(),
        Some(5)
    );
    assert_eq!(pipeline["controller_restore_hold_ms"].as_u64(), Some(2345));
    assert_eq!(
        pipeline["controller_loadavg_high_watermark_1m"].as_f64(),
        Some(7.5)
    );
    assert_eq!(
        pipeline["controller_loadavg_low_watermark_1m"].as_f64(),
        Some(6.25)
    );
    assert_eq!(
        pipeline["tantivy_writer_threads"].as_u64(),
        Some(available_parallelism.min(2))
    );
    assert_eq!(
        pipeline["staged_shard_builders"].as_u64(),
        Some(crate::indexer::responsiveness::effective_worker_count(4).max(1) as u64)
    );
    assert_eq!(
        pipeline["staged_merge_workers"].as_u64(),
        Some(crate::indexer::responsiveness::effective_worker_count(2).max(1) as u64)
    );
    assert_eq!(
        pipeline["page_size"].as_i64(),
        Some(crate::indexer::LEXICAL_REBUILD_PAGE_SIZE_PUBLIC)
    );
    assert_eq!(
        pipeline["steady_batch_fetch_conversations"].as_u64(),
        Some(210)
    );
    assert_eq!(
        pipeline["startup_batch_fetch_conversations"].as_u64(),
        Some(21)
    );
    assert_eq!(
        pipeline["steady_commit_every_conversations"].as_u64(),
        Some(410)
    );
    assert_eq!(
        pipeline["startup_commit_every_conversations"].as_u64(),
        Some(41)
    );
    assert_eq!(pipeline["steady_commit_every_messages"].as_u64(), Some(510));
    assert_eq!(pipeline["startup_commit_every_messages"].as_u64(), Some(51));
    assert_eq!(
        pipeline["steady_commit_every_message_bytes"].as_u64(),
        Some(610000)
    );
    assert_eq!(
        pipeline["startup_commit_every_message_bytes"].as_u64(),
        Some(61000)
    );
    assert_eq!(pipeline["pipeline_channel_size"].as_u64(), Some(4));
    assert_eq!(
        pipeline["page_prep_workers"].as_u64(),
        Some(crate::indexer::responsiveness::effective_worker_count(6).max(1) as u64)
    );
    assert_eq!(
        pipeline["pipeline_max_message_bytes_in_flight"].as_u64(),
        Some(888888)
    );
    // GH #496: a page holds at most an equal share of the in-flight
    // budget across the channel slots plus the sink (888888 / 5): the
    // steady 610000-byte commit interval is clamped, the 61000-byte
    // startup one is not.
    assert_eq!(
        pipeline["startup_page_fetch_message_bytes"].as_u64(),
        Some(61000)
    );
    assert_eq!(
        pipeline["steady_page_fetch_message_bytes"].as_u64(),
        Some(888888 / 5)
    );
    assert_eq!(pipeline["runtime"], serde_json::Value::Null);
}

#[test]
pub(crate) fn state_meta_json_reports_active_rebuild_pipeline_runtime() {
    let (temp, db_path) = seed_cli_db();
    let index_path = crate::search::tantivy::index_dir(temp.path()).expect("index dir");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");
    std::fs::write(
            index_path.join(".lexical-rebuild-state.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "version": 3,
                "schema_hash": crate::search::tantivy::SCHEMA_HASH,
                "db": {
                    "db_path": db_path.display().to_string(),
                    "total_conversations": 10,
                    "storage_fingerprint": "10:42:0:0"
                },
                "page_size": crate::indexer::LEXICAL_REBUILD_PAGE_SIZE_PUBLIC,
                "committed_offset": 4,
                "committed_conversation_id": 4,
                "processed_conversations": 4,
                "indexed_docs": 20,
                "committed_meta_fingerprint": null,
                "pending": null,
                "completed": false,
                "updated_at_ms": 1_733_000_123_000_i64,
                "runtime": {
                    "queue_depth": 3,
                    "inflight_message_bytes": 65_536,
                    "max_message_bytes_in_flight": 131_072,
                    "pending_batch_conversations": 9,
                    "pending_batch_message_bytes": 131_072,
                    "page_prep_workers": 6,
                    "active_page_prep_jobs": 2,
                    "ordered_buffered_pages": 4,
                    "budget_generation": 1,
                    "producer_budget_wait_count": 2,
                    "producer_budget_wait_ms": 17,
                    "producer_handoff_wait_count": 1,
                    "producer_handoff_wait_ms": 9,
                    "host_loadavg_1m_milli": 7_250,
                    "controller_mode": "pressure_limited",
                    "controller_reason": "queue_depth_3_reached_pipeline_capacity_3",
                    "staged_merge_workers_max": 3,
                    "staged_merge_allowed_jobs": 1,
                    "staged_merge_active_jobs": 1,
                    "staged_merge_ready_artifacts": 5,
                    "staged_merge_ready_groups": 1,
                    "staged_merge_controller_reason": "page_prep_workers_saturated_6_of_6",
                    "staged_shard_build_workers_max": 6,
                    "staged_shard_build_allowed_jobs": 5,
                    "staged_shard_build_active_jobs": 4,
                    "staged_shard_build_pending_jobs": 2,
                    "staged_shard_build_controller_reason": "reserving_1_slots_for_staged_merge_active_jobs_1_ready_groups_1",
                    "updated_at_ms": 1_733_000_124_000_i64
                }
            }))
            .expect("serialize rebuild state"),
        )
        .expect("write rebuild state");

    let lock_path = temp.path().join("index-run.lock");
    let mut lock_file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .expect("open lock file");
    lock_file.try_lock_exclusive().expect("hold index lock");
    write_index_lock_metadata(
        &lock_path,
        &mut lock_file,
        &format!(
            "pid={}\nstarted_at_ms={}\ndb_path={}\nmode=index\n",
            std::process::id(),
            1_733_000_111_000_i64,
            db_path.display()
        ),
    );

    let state = state_meta_json(temp.path(), &db_path, 60, true);
    let runtime = &state["rebuild"]["pipeline"]["runtime"];
    let pipeline = &state["rebuild"]["pipeline"];

    assert_eq!(runtime["queue_depth"].as_u64(), Some(3));
    assert_eq!(
        runtime["queue_capacity"].as_u64(),
        pipeline["pipeline_channel_size"].as_u64()
    );
    assert_eq!(
        runtime["queue_headroom"].as_u64(),
        runtime["queue_capacity"]
            .as_u64()
            .map(|value| value.saturating_sub(3))
    );
    assert_eq!(runtime["inflight_message_bytes"].as_u64(), Some(65_536));
    assert_eq!(
        runtime["max_message_bytes_in_flight"].as_u64(),
        Some(131_072)
    );
    assert_eq!(
        runtime["inflight_message_bytes_headroom"].as_u64(),
        Some(65_536)
    );
    assert_eq!(runtime["pending_batch_conversations"].as_u64(), Some(9));
    assert_eq!(
        runtime["pending_batch_message_bytes"].as_u64(),
        Some(131_072)
    );
    assert_eq!(runtime["page_prep_workers"].as_u64(), Some(6));
    assert_eq!(runtime["active_page_prep_jobs"].as_u64(), Some(2));
    assert_eq!(runtime["ordered_buffered_pages"].as_u64(), Some(4));
    assert_eq!(runtime["budget_generation"].as_u64(), Some(1));
    assert_eq!(runtime["producer_budget_wait_count"].as_u64(), Some(2));
    assert_eq!(runtime["producer_budget_wait_ms"].as_u64(), Some(17));
    assert_eq!(runtime["producer_handoff_wait_count"].as_u64(), Some(1));
    assert_eq!(runtime["producer_handoff_wait_ms"].as_u64(), Some(9));
    assert_eq!(runtime["host_loadavg_1m"].as_f64(), Some(7.25));
    assert_eq!(
        runtime["controller_mode"].as_str(),
        Some("pressure_limited")
    );
    assert_eq!(
        runtime["controller_reason"].as_str(),
        Some("queue_depth_3_reached_pipeline_capacity_3")
    );
    assert_eq!(runtime["staged_merge_workers_max"].as_u64(), Some(3));
    assert_eq!(runtime["staged_merge_allowed_jobs"].as_u64(), Some(1));
    assert_eq!(runtime["staged_merge_active_jobs"].as_u64(), Some(1));
    assert_eq!(runtime["staged_merge_ready_artifacts"].as_u64(), Some(5));
    assert_eq!(runtime["staged_merge_ready_groups"].as_u64(), Some(1));
    assert_eq!(
        runtime["staged_merge_controller_reason"].as_str(),
        Some("page_prep_workers_saturated_6_of_6")
    );
    assert_eq!(runtime["staged_shard_build_workers_max"].as_u64(), Some(6));
    assert_eq!(runtime["staged_shard_build_allowed_jobs"].as_u64(), Some(5));
    assert_eq!(runtime["staged_shard_build_active_jobs"].as_u64(), Some(4));
    assert_eq!(runtime["staged_shard_build_pending_jobs"].as_u64(), Some(2));
    assert_eq!(
        runtime["staged_shard_build_controller_reason"].as_str(),
        Some("reserving_1_slots_for_staged_merge_active_jobs_1_ready_groups_1")
    );
    assert_eq!(
        runtime["updated_at"].as_str(),
        format_timestamp_millis_rfc3339(1_733_000_124_000_i64).as_deref()
    );
}

#[test]
pub(crate) fn state_meta_json_hides_empty_active_rebuild_pipeline_runtime_before_first_heartbeat() {
    let (temp, db_path) = seed_cli_db();
    let index_path = crate::search::tantivy::index_dir(temp.path()).expect("index dir");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");
    std::fs::write(
        index_path.join(".lexical-rebuild-state.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "version": 3,
            "schema_hash": crate::search::tantivy::SCHEMA_HASH,
            "db": {
                "db_path": db_path.display().to_string(),
                "total_conversations": 10,
                "storage_fingerprint": "10:42:0:0"
            },
            "page_size": crate::indexer::LEXICAL_REBUILD_PAGE_SIZE_PUBLIC,
            "committed_offset": 4,
            "committed_conversation_id": 4,
            "processed_conversations": 4,
            "indexed_docs": 20,
            "committed_meta_fingerprint": null,
            "pending": null,
            "completed": false,
            "updated_at_ms": 1_733_000_123_000_i64
        }))
        .expect("serialize rebuild state"),
    )
    .expect("write rebuild state");

    let lock_path = temp.path().join("index-run.lock");
    let mut lock_file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .expect("open lock file");
    lock_file.try_lock_exclusive().expect("hold index lock");
    write_index_lock_metadata(
        &lock_path,
        &mut lock_file,
        &format!(
            "pid={}\nstarted_at_ms={}\ndb_path={}\nmode=index\n",
            std::process::id(),
            1_733_000_111_000_i64,
            db_path.display()
        ),
    );

    let state = state_meta_json(temp.path(), &db_path, 60, true);
    assert_eq!(state["index"]["rebuilding"].as_bool(), Some(true));
    assert_eq!(state["rebuild"]["active"].as_bool(), Some(true));
    assert_eq!(
        state["rebuild"]["pipeline"]["runtime"],
        serde_json::Value::Null
    );
}

#[test]
pub(crate) fn state_meta_json_reports_active_rebuild() {
    let (temp, db_path) = seed_cli_db();
    let index_path = crate::search::tantivy::index_dir(temp.path()).expect("index dir");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");
    std::fs::write(
        index_path.join(".lexical-rebuild-state.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "version": 3,
            "schema_hash": crate::search::tantivy::SCHEMA_HASH,
            "db": {
                "db_path": db_path.display().to_string(),
                "total_conversations": 10,
                "storage_fingerprint": "10:42:0:0"
            },
            "page_size": crate::indexer::LEXICAL_REBUILD_PAGE_SIZE_PUBLIC,
            "committed_offset": 4,
            "committed_conversation_id": 4,
            "processed_conversations": 4,
            "indexed_docs": 20,
            "committed_meta_fingerprint": null,
            "pending": null,
            "completed": false,
            "updated_at_ms": 1_733_000_123_000_i64
        }))
        .expect("serialize rebuild state"),
    )
    .expect("write rebuild state");

    let lock_path = temp.path().join("index-run.lock");
    let mut lock_file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .expect("open lock file");
    lock_file.try_lock_exclusive().expect("hold index lock");
    write_index_lock_metadata(
        &lock_path,
        &mut lock_file,
        &format!(
            "pid={}\nstarted_at_ms={}\ndb_path={}\nmode=index\n",
            std::process::id(),
            1_733_000_111_000_i64,
            db_path.display()
        ),
    );

    let state = state_meta_json(temp.path(), &db_path, 60, true);
    assert_eq!(state["index"]["rebuilding"].as_bool(), Some(true));
    assert_eq!(state["rebuild"]["active"].as_bool(), Some(true));
    assert_eq!(state["pending"]["sessions"].as_u64(), Some(6));
    assert_eq!(
        state["rebuild"]["processed_conversations"].as_u64(),
        Some(4)
    );
    assert_eq!(state["rebuild"]["total_conversations"].as_u64(), Some(10));
}

#[test]
pub(crate) fn state_meta_json_prefers_pending_rebuild_progress_when_present() {
    let (temp, db_path) = seed_cli_db();
    let index_path = crate::search::tantivy::index_dir(temp.path()).expect("index dir");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");
    std::fs::write(
        index_path.join(".lexical-rebuild-state.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "version": 3,
            "schema_hash": crate::search::tantivy::SCHEMA_HASH,
            "db": {
                "db_path": db_path.display().to_string(),
                "total_conversations": 10,
                "storage_fingerprint": "10:42:0:0"
            },
            "page_size": crate::indexer::LEXICAL_REBUILD_PAGE_SIZE_PUBLIC,
            "committed_offset": 4,
            "committed_conversation_id": 4,
            "processed_conversations": 4,
            "indexed_docs": 20,
            "committed_meta_fingerprint": null,
            "pending": {
                "next_offset": 6,
                "next_conversation_id": 6,
                "processed_conversations": 6,
                "indexed_docs": 30,
                "base_meta_fingerprint": "stable-meta"
            },
            "completed": false,
            "updated_at_ms": 1_733_000_223_000_i64
        }))
        .expect("serialize rebuild state"),
    )
    .expect("write rebuild state");

    let lock_path = temp.path().join("index-run.lock");
    let mut lock_file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .expect("open lock file");
    lock_file.try_lock_exclusive().expect("hold index lock");
    write_index_lock_metadata(
        &lock_path,
        &mut lock_file,
        &format!(
            "pid={}\nstarted_at_ms={}\ndb_path={}\nmode=index\n",
            std::process::id(),
            1_733_000_111_000_i64,
            db_path.display()
        ),
    );

    let state = state_meta_json(temp.path(), &db_path, 60, true);
    assert_eq!(state["rebuild"]["active"].as_bool(), Some(true));
    assert_eq!(state["pending"]["sessions"].as_u64(), Some(4));
    assert_eq!(
        state["rebuild"]["processed_conversations"].as_u64(),
        Some(6)
    );
    assert_eq!(state["rebuild"]["indexed_docs"].as_u64(), Some(30));
}
