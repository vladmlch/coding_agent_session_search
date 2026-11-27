use super::compat::{
    self, ConnectionExt, OptionalExtension, ParamValue, RowExt, TransactionExt,
    param_slice_to_values, params_from_iter,
};
use super::connection::lock_taking_begin_verb;
use super::sidecar_probes::sidecar_path;
use super::*;
use rusqlite::types::ValueRef;
use std::time::{Duration, Instant};

    fn conn() -> Connection {
        Connection::open(":memory:").expect("open :memory:")
    }

    fn seed_files(conn: &Connection) {
        conn.execute_batch(
            "CREATE TABLE files(path TEXT NOT NULL, title TEXT NOT NULL);
             INSERT INTO files(path, title) VALUES
                ('/tmp/a.txt', 'alpha'),
                ('/tmp/b.txt', 'beta');",
        )
        .expect("seed");
    }

    #[test]
    fn interrupt_handle_cancels_active_recursive_query_promptly() {
        let conn = conn();
        let handle = conn.interrupt_handle();
        let interrupter = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            handle.interrupt();
        });
        let started = Instant::now();
        let result = conn.query(
            "WITH RECURSIVE cnt(x) AS (
                 SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 1000000000
             ) SELECT COUNT(x) FROM cnt",
        );
        interrupter.join().expect("interrupter thread");
        let err = result.expect_err("slow query must be interrupted");
        assert!(
            matches!(err, FrankenError::Interrupted | FrankenError::Abort),
            "expected interrupt-class error, got {err:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "interrupt should land promptly"
        );
    }

    #[test]
    fn shared_deadline_timer_fires_and_disarms_across_many_guards() {
        let conn = conn();
        // 1) A short deadline must fire mid-query and mark fired().
        let guard = conn.deadline_guard_after(Duration::from_millis(120));
        let result = conn.query(
            "WITH RECURSIVE cnt(x) AS (
                 SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 1000000000
             ) SELECT COUNT(x) FROM cnt",
        );
        let err = result.expect_err("deadline should interrupt the query");
        assert!(err.is_interrupt_class(), "got {err:?}");
        assert!(guard.fired(), "deadline slot should record the fire");

        // 2) 60 guards armed then disarmed: none may fire, all on the same
        //    reusable timer thread.
        let mut guards = Vec::new();
        for _ in 0..60 {
            guards.push(conn.deadline_guard_after(Duration::from_millis(40)));
        }
        for guard in &guards {
            guard.disarm();
        }
        std::thread::sleep(Duration::from_millis(150));
        for guard in &guards {
            assert!(!guard.fired(), "disarmed deadline must never fire");
        }
        drop(guards);
    }

    #[test]
    fn cancellable_begin_immediate_retry_honors_cancellation_handle() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("contended.db");
        let path_str = path.to_string_lossy().into_owned();

        let blocker = Connection::open(&path_str).expect("blocker open");
        blocker.execute("CREATE TABLE t(x)").expect("create");
        blocker.execute_batch("BEGIN IMMEDIATE").expect("lock");

        let cancellation = CancellationHandle::new();
        let cancel = cancellation.clone();
        let started = Instant::now();
        let worker = std::thread::spawn(move || {
            let conn = Connection::open(&path_str).expect("worker open");
            conn.execute("PRAGMA busy_timeout = 5000")
                .expect("busy timeout");
            let _scope = cancel.enter();
            conn.transaction_with_behavior(compat::TransactionBehavior::Immediate)
                .map(|_| ())
        });
        std::thread::sleep(Duration::from_millis(300));
        cancellation.cancel();
        let result = worker.join().expect("worker join");
        assert!(
            matches!(result, Err(FrankenError::Abort)),
            "cancelled BEGIN IMMEDIATE should Abort, got {result:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "cancellation should land promptly (elapsed {:?})",
            started.elapsed()
        );
        blocker.execute_batch("ROLLBACK").expect("release lock");
    }

    #[test]
    fn lock_taking_begin_verb_strips_trailing_semicolon() {
        assert_eq!(
            lock_taking_begin_verb("BEGIN IMMEDIATE;"),
            Some("BEGIN IMMEDIATE")
        );
        assert_eq!(
            lock_taking_begin_verb("BEGIN IMMEDIATE TRANSACTION;"),
            Some("BEGIN IMMEDIATE")
        );
        assert_eq!(
            lock_taking_begin_verb("  begin   exclusive   tran ;  "),
            Some("BEGIN EXCLUSIVE")
        );
        assert_eq!(
            lock_taking_begin_verb("BEGIN IMMEDIATE"),
            Some("BEGIN IMMEDIATE")
        );
        // Deferred BEGIN takes no lock; a `;` left inside the body means
        // a multi-statement batch that must stay on execute_batch.
        assert_eq!(lock_taking_begin_verb("BEGIN;"), None);
        assert_eq!(lock_taking_begin_verb("BEGIN TRANSACTION;"), None);
        assert_eq!(lock_taking_begin_verb("BEGIN IMMEDIATE; SELECT 1;"), None);
        assert_eq!(lock_taking_begin_verb("SELECT 1;"), None);
        assert_eq!(lock_taking_begin_verb(""), None);
    }

    #[test]
    fn semicolon_begin_immediate_routes_through_execute_entry_points() {
        let conn = conn();
        conn.execute("BEGIN IMMEDIATE;").expect("execute");
        assert!(conn.in_transaction());
        conn.execute_batch("ROLLBACK").expect("rollback");
        assert!(conn.is_autocommit());

        conn.execute_with_params("BEGIN IMMEDIATE;", &[])
            .expect("execute_with_params");
        assert!(conn.in_transaction());
        conn.execute_batch("ROLLBACK").expect("rollback");

        conn.execute_batch("BEGIN IMMEDIATE TRANSACTION;")
            .expect("execute_batch");
        assert!(conn.in_transaction());
        conn.execute_batch("ROLLBACK").expect("rollback");
        assert!(conn.is_autocommit());
    }

    #[test]
    fn begin_immediate_semicolon_restores_busy_timeout() {
        let conn = conn();
        conn.execute("PRAGMA busy_timeout = 4321").expect("set");
        conn.execute("BEGIN IMMEDIATE;").expect("begin");
        conn.execute_batch("ROLLBACK").expect("rollback");
        let row = conn.query_row("PRAGMA busy_timeout").expect("read");
        assert_eq!(row.get_typed::<i64>(0).unwrap(), 4321);
    }

    #[test]
    fn cancelled_scope_leaves_busy_timeout_untouched() {
        let conn = conn();
        conn.execute("PRAGMA busy_timeout = 4321").expect("set");
        let cancellation = CancellationHandle::new();
        {
            let _scope = cancellation.enter();
            cancellation.cancel();
            let err = conn
                .wal_checkpoint_truncate(Some(Instant::now() + Duration::from_secs(5)))
                .expect_err("cancelled scope must abort");
            assert!(matches!(err, FrankenError::Abort), "got {err:?}");
            let err = conn
                .transaction_with_deadline(compat::TransactionBehavior::Immediate, None)
                .expect_err("cancelled begin must abort");
            assert!(matches!(err, FrankenError::Abort), "got {err:?}");
        }
        // The sliced busy_timeout must never leak past BusyTimeoutRestore.
        let row = conn.query_row("PRAGMA busy_timeout").expect("read");
        assert_eq!(row.get_typed::<i64>(0).unwrap(), 4321);
    }

    #[test]
    fn busy_retry_paths_fail_cleanly_on_closed_connection() {
        let mut conn = conn();
        conn.close_in_place().expect("close");
        let err = conn
            .execute("BEGIN IMMEDIATE;")
            .expect_err("must not panic");
        assert!(matches!(err, FrankenError::Internal(_)), "got {err:?}");
        let err = conn
            .wal_checkpoint_truncate(None)
            .expect_err("must not panic");
        assert!(matches!(err, FrankenError::Internal(_)), "got {err:?}");
    }

    #[test]
    fn close_without_checkpoint_preserves_wal_for_later_recovery() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("wal.db");
        let path_str = path.to_string_lossy().into_owned();

        let conn = Connection::open(&path_str).expect("open");
        conn.execute("PRAGMA journal_mode = WAL").expect("wal mode");
        conn.execute("PRAGMA wal_autocheckpoint = 0")
            .expect("no auto ckpt");
        conn.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (1);")
            .expect("write");
        conn.close_without_checkpoint().expect("close w/o ckpt");

        let wal = sidecar_path(&path, "-wal");
        assert!(wal.exists(), "-wal must survive no-checkpoint close");
        let wal_len = std::fs::metadata(&wal).expect("wal meta").len();
        assert!(wal_len > 0, "-wal must retain frames");

        // A subsequent open still sees the WAL content (recovery).
        let reopened = Connection::open(&path_str).expect("reopen");
        let row = reopened.query_row("SELECT COUNT(*) FROM t").expect("count");
        assert_eq!(row.get_typed::<i64>(0).unwrap(), 1);
    }

    #[test]
    fn strict_readonly_refuses_damaged_or_absent_shm_without_mutation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("strict.db");
        let path_str = path.to_string_lossy().into_owned();

        // Seed a WAL database and close WITHOUT checkpoint so -wal/-shm stay.
        let conn = Connection::open(&path_str).expect("open");
        conn.execute("PRAGMA journal_mode = WAL").expect("wal");
        conn.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (7);")
            .expect("write");
        conn.close_without_checkpoint().expect("close");

        let shm = sidecar_path(&path, "-shm");
        let wal = sidecar_path(&path, "-wal");
        assert!(shm.exists(), "-shm must persist after no-checkpoint close");
        let wal_before = std::fs::read(&wal).expect("wal bytes");
        let strict = compat::OpenFlags::SQLITE_OPEN_READ_ONLY
            | compat::OpenFlags::SQLITE_OPEN_STRICT_READ_ONLY;

        // Case A: valid shm -> strict open succeeds, reads work, no mutation.
        let c = compat::open_with_flags(&path_str, strict).expect("strict open");
        let row = c.query_row("SELECT x FROM t").expect("row");
        assert_eq!(row.get_typed::<i64>(0).unwrap(), 7);
        c.close_without_checkpoint().expect("close ro");
        assert_eq!(std::fs::read(&wal).expect("wal after"), wal_before);

        // Case B: absent -shm -> BusyRecovery, and no sidecar is recreated.
        let bak = sidecar_path(&path, "-shm.keep");
        std::fs::rename(&shm, &bak).expect("hide shm");
        let err = compat::open_with_flags(&path_str, strict).expect_err("must refuse");
        assert!(
            matches!(err, FrankenError::BusyRecovery),
            "absent shm should be BusyRecovery, got {err:?}"
        );
        assert!(!shm.exists(), "strict open must not recreate -shm");
        assert_eq!(std::fs::read(&wal).expect("wal bytes"), wal_before);
        std::fs::rename(&bak, &shm).expect("restore shm");

        // Case C: damaged -shm -> BusyRecovery.
        std::fs::write(&shm, vec![0xABu8; 256]).expect("corrupt shm");
        let err = compat::open_with_flags(&path_str, strict).expect_err("must refuse");
        assert!(
            matches!(err, FrankenError::BusyRecovery),
            "damaged shm should be BusyRecovery, got {err:?}"
        );
        assert_eq!(std::fs::read(&wal).expect("wal bytes"), wal_before);
    }

    #[test]
    fn strict_readonly_plain_journal_needs_no_shm() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("legacy.db");
        let path_str = path.to_string_lossy().into_owned();
        let conn = Connection::open(&path_str).expect("open");
        conn.execute("PRAGMA journal_mode = DELETE")
            .expect("legacy mode");
        conn.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (3);")
            .expect("write");
        conn.close().expect("close");
        let strict = compat::OpenFlags::SQLITE_OPEN_READ_ONLY
            | compat::OpenFlags::SQLITE_OPEN_STRICT_READ_ONLY;
        let c = compat::open_with_flags(&path_str, strict).expect("strict open");
        let row = c.query_row("SELECT x FROM t").expect("row");
        assert_eq!(row.get_typed::<i64>(0).unwrap(), 3);
    }

    #[test]
    fn query_row_enforces_exactly_one_row() {
        let conn = conn();
        conn.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (1), (2);")
            .expect("seed");
        let err = conn.query_row("SELECT x FROM t WHERE x = 9").unwrap_err();
        assert!(matches!(err, FrankenError::QueryReturnedNoRows));
        let err = conn.query_row("SELECT x FROM t").unwrap_err();
        assert!(matches!(err, FrankenError::QueryReturnedMultipleRows));
        let row = conn.query_row("SELECT x FROM t WHERE x = 1").unwrap();
        assert_eq!(row.get_typed::<i64>(0).unwrap(), 1);
    }

    #[test]
    fn cancellation_scope_is_latched_and_restored() {
        let conn = conn();
        let cancellation = CancellationHandle::new();
        {
            let _scope = cancellation.enter();
            cancellation.cancel();
            assert!(cancellation.is_cancelled());
            let err = conn.query("SELECT 1").unwrap_err();
            assert!(matches!(err, FrankenError::Abort));
        }
        // Scope dropped: ops run again.
        let rows = conn.query("SELECT 1 AS one").expect("query");
        assert_eq!(rows[0].get_typed::<i64>(0).unwrap(), 1);
    }

    #[test]
    fn row_callback_error_propagates_once() {
        let conn = conn();
        seed_files(&conn);
        let mut calls = 0usize;
        let err = conn
            .query_for_each("SELECT path, title FROM files", |row| {
                calls += 1;
                if calls > 1 {
                    panic!("query must stop after callback error");
                }
                let _ = row.get_typed::<String>(1)?;
                Err(FrankenError::BusyRecovery)
            })
            .unwrap_err();
        assert!(matches!(err, FrankenError::BusyRecovery));
        assert_eq!(calls, 1);
    }

    #[test]
    fn multi_statement_execute_preserves_order() {
        let conn = conn();
        conn.execute(
            "CREATE TABLE side_effect_log(seq INTEGER PRIMARY KEY, tag TEXT);\n             INSERT INTO side_effect_log(tag) VALUES ('before');\n             THIS IS NOT VALID SQL;\n             INSERT INTO side_effect_log(tag) VALUES ('after');",
        )
        .expect_err("multi-statement execute must fail");
        let rows = conn
            .query("SELECT tag FROM side_effect_log ORDER BY seq")
            .expect("log");
        assert_eq!(
            rows.iter()
                .map(|row| row.get_typed::<String>(0).unwrap())
                .collect::<Vec<_>>(),
            vec!["before".to_string()],
            "no replay of prior statements on failure"
        );
    }

    #[test]
    fn transaction_and_savepoint_roundtrip() {
        let conn = conn();
        conn.execute("CREATE TABLE t(x)").expect("create");
        {
            let mut tx = conn.transaction().expect("begin");
            tx.execute("INSERT INTO t VALUES (1)").expect("insert");
            {
                let mut sp = tx.savepoint().expect("savepoint");
                sp.execute("INSERT INTO t VALUES (2)").expect("sp insert");
                sp.rollback().expect("sp rollback");
            }
            tx.commit().expect("commit");
        }
        let row = conn.query_row("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(row.get_typed::<i64>(0).unwrap(), 1);
        assert!(conn.as_async().is_autocommit());
    }

    #[test]
    fn compat_params_and_optional_extension_work() {
        let conn = conn();
        seed_files(&conn);
        let count: i64 = conn
            .query_row_map(
                "SELECT COUNT(*) FROM files WHERE title != ?1",
                params!["missing"],
                |row| row.get_typed::<i64>(0),
            )
            .expect("count");
        assert_eq!(count, 2);
        let none: Option<Row> = conn
            .query_row("SELECT path FROM files WHERE title = 'none'")
            .optional()
            .expect("optional");
        assert!(none.is_none());
        let mapped: Vec<String> = conn
            .query_map_collect("SELECT title FROM files ORDER BY title", params![], |row| {
                row.get_typed::<String>(0)
            })
            .expect("collect");
        assert_eq!(mapped, vec!["alpha".to_string(), "beta".to_string()]);

        let params = param_slice_to_values(params![1i64, "x", Option::<i64>::None]);
        assert_eq!(params.len(), 3);
        assert!(matches!(params[2], SqliteValue::Null));
        let from_iter = params_from_iter([ParamValue::from(7i64)]);
        assert_eq!(from_iter, vec![SqliteValue::Integer(7)]);
    }

    #[test]
    fn query_map_borrows_raw_rows() {
        let conn = conn();
        seed_files(&conn);
        let titles = conn
            .query_map("SELECT title FROM files ORDER BY title", &[], |row| {
                row.get_ref(0)
                    .map(|v| match v {
                        ValueRef::Text(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                        _ => String::new(),
                    })
                    .map_err(FrankenError::from)
            })
            .expect("map");
        assert_eq!(titles, vec!["alpha".to_string(), "beta".to_string()]);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn backup_exact_to_vacuum_into_roundtrip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let src = dir.path().join("src.db");
        let dst = dir.path().join("dst.db");
        let conn = Connection::open(src.to_string_lossy()).expect("open");
        conn.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (11);")
            .expect("seed");
        let report = conn.backup_exact_to(&dst).expect("backup");
        assert!(report.byte_len > 0);
        assert!(report.page_size >= 512);
        assert!(report.page_count > 0);
        // Refuses overwrite.
        assert!(matches!(
            conn.backup_exact_to(&dst),
            Err(FrankenError::CannotOpen { .. })
        ));
        let check = Connection::open(dst.to_string_lossy()).expect("open dst");
        let row = check.query_row("SELECT x FROM t").unwrap();
        assert_eq!(row.get_typed::<i64>(0).unwrap(), 11);
    }

    #[test]
    fn migration_runner_applies_ordered_steps() {
        let conn = conn();
        let runner = migrate::MigrationRunner::new()
            .add(1, "initial", "CREATE TABLE a(x);")
            .add(2, "second", "CREATE TABLE b(y);");
        let result = runner.run(&conn).expect("first run");
        assert_eq!(result.applied, vec![1, 2]);
        assert!(result.was_fresh, "virgin ledger should report fresh");
        let result = runner.run(&conn).expect("second run");
        assert!(result.applied.is_empty());
        assert!(!result.was_fresh);
        let row = conn
            .query_row("SELECT COUNT(*) FROM _schema_migrations")
            .unwrap();
        assert_eq!(row.get_typed::<i64>(0).unwrap(), 2);
    }

    #[test]
    fn franken_error_code_and_extended_codes_match() {
        let err = FrankenError::Abort;
        assert_eq!(err.extended_error_code(), 4);
        assert_eq!(format!("{:?}", err.error_code()), "Abort");
        let err = FrankenError::BusyRecovery;
        assert_eq!(err.extended_error_code(), 261);
        let err = FrankenError::OutOfMemory;
        assert_eq!(err.to_string(), "out of memory");
        let err = FrankenError::from_rusqlite(
            rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(1),
                Some("no such table: sessions".to_string()),
            ),
            None,
        );
        assert!(matches!(
            err,
            FrankenError::NoSuchTable { ref name } if name == "sessions"
        ));
    }

    #[test]
    fn shutdown_driver_is_a_noop_success() {
        assert!(shutdown_driver());
    }

    #[cfg(unix)]
    #[test]
    fn file_identity_detects_rename_and_preserves_writer_lock() {
        const CHILD_ENV: &str = "CASS_TEST_FRANKEN_SYNC_LOCK_PROBE_DB";
        const TEST_NAME: &str =
            "franken_sync::tests::file_identity_detects_rename_and_preserves_writer_lock";

        if let Some(probe_db) = std::env::var_os(CHILD_ENV) {
            let probe_str = probe_db.to_string_lossy().into_owned();
            let probe = Connection::open(&probe_str).expect("child open");
            probe.execute("PRAGMA busy_timeout = 0").expect("child timeout");
            match probe.execute("BEGIN IMMEDIATE") {
                Ok(_) => {
                    let _ = probe.execute_batch("ROLLBACK");
                    println!("\nLOCK_PROBE=ACQUIRED");
                }
                Err(error) if error.is_busy_class() => {
                    println!("\nLOCK_PROBE=HELD_BY_PARENT");
                }
                Err(error) => panic!("unexpected child lock probe error: {error:?}"),
            }
            return;
        }

        let probe_child = |db_path: &std::path::Path| -> String {
            let output = std::process::Command::new(std::env::current_exe().expect("current_exe"))
                .args(["--exact", TEST_NAME, "--test-threads=1", "--nocapture"])
                .env(CHILD_ENV, db_path)
                .output()
                .expect("spawn lock probe child");
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(
                output.status.success(),
                "child lock probe failed ({}):\nstdout:\n{stdout}\nstderr:\n{stderr}",
                output.status
            );
            stdout
                .lines()
                .find_map(|line| {
                    line.split_once("LOCK_PROBE=")
                        .and_then(|(_, rest)| rest.split_whitespace().next())
                })
                .map(str::to_owned)
                .unwrap_or_else(|| panic!("missing LOCK_PROBE line in stdout:\n{stdout}"))
        };

        let dir = tempfile::tempdir().expect("tempdir");

        // 1) rename() replacement detection: Connection::file_identity() stays
        //    pinned to the opened inode while FileIdentity::from_path() sees
        //    the new inode on disk.
        let path_a = dir.path().join("rename_a.db");
        let path_b = dir.path().join("rename_b.db");
        let conn_a = Connection::open(path_a.to_string_lossy()).expect("open a");
        conn_a
            .execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (1);")
            .expect("seed a");
        let id_a = conn_a.file_identity().expect("id a").expect("some id a");

        let conn_b = Connection::open(path_b.to_string_lossy()).expect("open b");
        conn_b
            .execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (2);")
            .expect("seed b");
        let id_b = conn_b.file_identity().expect("id b").expect("some id b");
        assert_ne!(id_a, id_b);
        conn_b.close().expect("close b");

        std::fs::rename(&path_b, &path_a).expect("atomic rename");
        assert_eq!(conn_a.file_identity().expect("cached id a"), Some(id_a));
        assert_eq!(
            FileIdentity::from_path(&path_a).expect("path id after rename"),
            Some(id_b)
        );
        conn_a.close().expect("close a");

        let strict = compat::OpenFlags::SQLITE_OPEN_READ_ONLY
            | compat::OpenFlags::SQLITE_OPEN_STRICT_READ_ONLY;

        // 2) Rollback (DELETE) mode: file_identity() and strict read-only open
        //    must preserve the parent writer's POSIX fcntl lock on `.db`.
        let rollback_db = dir.path().join("rollback_lock.db");
        let rollback_str = rollback_db.to_string_lossy().into_owned();
        let writer = Connection::open(&rollback_str).expect("open rollback writer");
        writer
            .execute("PRAGMA journal_mode = DELETE")
            .expect("delete mode");
        writer
            .execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (10);")
            .expect("seed rollback");
        writer.execute("BEGIN IMMEDIATE").expect("begin rollback");
        assert_eq!(probe_child(&rollback_db), "HELD_BY_PARENT");
        let _ = writer.file_identity().expect("file_identity under lock");
        let ro = compat::open_with_flags(&rollback_str, strict).expect("strict open under lock");
        assert_eq!(
            ro.query_row("SELECT COUNT(*) FROM t")
                .expect("ro rollback read")
                .get_typed::<i64>(0)
                .unwrap(),
            1
        );
        ro.close_without_checkpoint().expect("close ro");
        assert_eq!(
            probe_child(&rollback_db),
            "HELD_BY_PARENT",
            "file_identity/strict_open must not drop .db POSIX lock"
        );
        writer.execute_batch("ROLLBACK").expect("rollback writer");
        assert_eq!(probe_child(&rollback_db), "ACQUIRED");
        writer.close().expect("close rollback writer");

        // 3) WAL mode: file_identity() and strict read-only open must preserve
        //    the parent writer's POSIX fcntl locks on `.db` and `-shm`.
        let wal_db = dir.path().join("wal_lock.db");
        let wal_str = wal_db.to_string_lossy().into_owned();
        let wal_writer = Connection::open(&wal_str).expect("open wal writer");
        wal_writer
            .execute("PRAGMA journal_mode = WAL")
            .expect("wal mode");
        wal_writer
            .execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (20);")
            .expect("seed wal");
        wal_writer.execute("BEGIN IMMEDIATE").expect("begin wal");
        wal_writer.execute("INSERT INTO t VALUES (21)").expect("insert wal");
        assert_eq!(probe_child(&wal_db), "HELD_BY_PARENT");
        let _ = wal_writer.file_identity().expect("wal file_identity under lock");
        let ro_wal = compat::open_with_flags(&wal_str, strict).expect("strict wal open under lock");
        let row = ro_wal.query_row("SELECT COUNT(*) FROM t").expect("ro wal read");
        assert_eq!(row.get_typed::<i64>(0).unwrap(), 1);
        ro_wal.close_without_checkpoint().expect("close ro wal");
        assert_eq!(
            probe_child(&wal_db),
            "HELD_BY_PARENT",
            "file_identity/strict_open must not drop -shm/.db POSIX locks"
        );
        wal_writer.execute_batch("ROLLBACK").expect("rollback wal writer");
        assert_eq!(probe_child(&wal_db), "ACQUIRED");
        wal_writer.close().expect("close wal writer");
    }

    #[cfg(unix)]
    #[test]
    fn query_pragma_journal_mode_updates_live_wal_state_for_strict_readonly() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("switch_wal_via_query.db");
        let db_str = db_path.to_string_lossy().into_owned();
        let wal_path = sidecar_path(&db_path, "-wal");
        let shm_path = sidecar_path(&db_path, "-shm");
        let strict = compat::OpenFlags::SQLITE_OPEN_READ_ONLY
            | compat::OpenFlags::SQLITE_OPEN_STRICT_READ_ONLY;

        // Open a rollback DB (`known_wal = Some(false)`), seed a table, then
        // switch to WAL via `Connection::query("PRAGMA journal_mode = WAL;")`.
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t(x); INSERT INTO t VALUES (1);")
            .expect("seed");
        let id = conn.file_identity().expect("id").expect("some id");
        assert_eq!(
            super::sidecar_probes::live_connection_state(id),
            Some(Some(false))
        );

        let rows = conn.query("PRAGMA journal_mode = WAL;").expect("query wal");
        assert_eq!(
            rows[0].get_typed::<String>(0).unwrap().to_lowercase(),
            "wal"
        );
        assert_eq!(
            super::sidecar_probes::live_connection_state(id),
            Some(Some(true))
        );

        // If `-wal`/`-shm` were not created yet (or after removing clean empty
        // sidecars / reopening the WAL header without sidecars), strict
        // read-only open must refuse with BusyRecovery and create zero files.
        if !wal_path.exists() && !shm_path.exists() {
            let err = compat::open_with_flags(&db_str, strict)
                .expect_err("WAL header without sidecars must refuse");
            assert!(matches!(err, FrankenError::BusyRecovery), "got {err:?}");
            assert!(!wal_path.exists(), "strict open must not create -wal");
            assert!(!shm_path.exists(), "strict open must not create -shm");
        }

        // Also verify PreparedStatement refreshes `LIVE_DB_CONNECTIONS` back to
        // non-WAL when switching via prepared statement.
        let ps = PreparedStatement::new(&conn, "PRAGMA journal_mode = DELETE;")
            .expect("prepare delete");
        let _ = ps.query_row().expect("switch back to delete");
        assert_eq!(
            super::sidecar_probes::live_connection_state(id),
            Some(Some(false))
        );
        drop(ps);
        conn.close().expect("close");
    }

    #[cfg(all(unix, not(target_arch = "wasm32")))]
    #[test]
    fn backup_exact_to_rejects_dangling_symlink() {
        let dir = tempfile::tempdir().expect("tempdir");
        let src = dir.path().join("src.db");
        let dangling_target = dir.path().join("missing_target.db");
        let link = dir.path().join("dangling_link.db");
        std::os::unix::fs::symlink(&dangling_target, &link).expect("create dangling symlink");

        let conn = Connection::open(src.to_string_lossy()).expect("open");
        conn.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (42);")
            .expect("seed");

        let err = conn
            .backup_exact_to(&link)
            .expect_err("dangling symlink must be rejected");
        assert!(
            matches!(err, FrankenError::CannotOpen { ref path } if path == &link),
            "expected CannotOpen for dangling symlink, got {err:?}"
        );
        assert!(
            !dangling_target.exists(),
            "dangling symlink target must not be created"
        );
        assert!(std::fs::symlink_metadata(&link).is_ok());
    }

    #[cfg(unix)]
    fn recompute_shm_checksums(shm_bytes: &mut [u8]) {
        let mut s0 = 0_u32;
        let mut s1 = 0_u32;
        for pair in shm_bytes[..40].as_chunks::<8>().0 {
            let x0 = u32::from_ne_bytes([pair[0], pair[1], pair[2], pair[3]]);
            let x1 = u32::from_ne_bytes([pair[4], pair[5], pair[6], pair[7]]);
            s0 = s0.wrapping_add(x0).wrapping_add(s1);
            s1 = s1.wrapping_add(x1).wrapping_add(s0);
        }
        shm_bytes[40..44].copy_from_slice(&s0.to_ne_bytes());
        shm_bytes[44..48].copy_from_slice(&s1.to_ne_bytes());
        let first_hdr: [u8; 48] = shm_bytes[..48].try_into().unwrap();
        shm_bytes[48..96].copy_from_slice(&first_hdr);
    }

    #[test]
    fn clean_one_file_wal_strict_and_recovery_readonly_leave_zero_sidecars() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("clean_wal.db");
        let path_str = path.to_string_lossy().into_owned();
        let wal_path = sidecar_path(&path, "-wal");
        let shm_path = sidecar_path(&path, "-shm");

        {
            let conn = Connection::open(&path_str).expect("open");
            conn.execute("PRAGMA journal_mode = WAL;").expect("wal");
            conn.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (99);")
                .expect("seed");
            conn.close().expect("clean close checkpoints away sidecars");
        }
        assert!(!wal_path.exists(), "clean close must remove -wal");
        assert!(!shm_path.exists(), "clean close must remove -shm");
        let bytes_before = std::fs::read(&path).expect("db bytes before");
        let mtime_before = std::fs::metadata(&path)
            .expect("meta before")
            .modified()
            .expect("mtime before");

        let strict = compat::OpenFlags::SQLITE_OPEN_READ_ONLY
            | compat::OpenFlags::SQLITE_OPEN_STRICT_READ_ONLY;
        let ro_strict = compat::open_with_flags(&path_str, strict)
            .expect("P0-2: cleanly closed 1-file WAL must open via snapshot");
        assert_eq!(
            ro_strict
                .query_row("SELECT x FROM t")
                .expect("read")
                .get_typed::<i64>(0)
                .unwrap(),
            99
        );
        ro_strict.close().expect("close strict snapshot");
        assert!(!wal_path.exists(), "strict open must not create -wal");
        assert!(!shm_path.exists(), "strict open must not create -shm");
        assert_eq!(std::fs::read(&path).expect("db bytes"), bytes_before);
        assert_eq!(
            std::fs::metadata(&path).expect("meta").modified().unwrap(),
            mtime_before
        );

        let ro_recovery = Connection::open_schema_only_with_wal_index_recovery(&path_str)
            .expect("P0-3: recovery readonly must open cleanly closed 1-file WAL without sidecars");
        assert_eq!(
            ro_recovery
                .query_row("SELECT x FROM t")
                .expect("read")
                .get_typed::<i64>(0)
                .unwrap(),
            99
        );
        ro_recovery.close().expect("close recovery snapshot");
        assert!(!wal_path.exists(), "recovery open must not create -wal");
        assert!(!shm_path.exists(), "recovery open must not create -shm");
        assert_eq!(std::fs::read(&path).expect("db bytes"), bytes_before);

        // Also verify P0-3 snapshot fallback when `-wal` has frames and `-shm` is absent.
        {
            let conn = Connection::open(&path_str).expect("reopen");
            conn.execute("INSERT INTO t VALUES (100);").expect("insert");
            conn.close_without_checkpoint().expect("preserve wal");
        }
        std::fs::remove_file(&shm_path).expect("remove shm");
        let wal_before = std::fs::read(&wal_path).expect("wal before");
        let err = compat::open_with_flags(&path_str, strict)
            .expect_err("strict open with -wal but no -shm must still return BusyRecovery");
        assert!(matches!(err, FrankenError::BusyRecovery));
        assert!(!shm_path.exists(), "strict open must not create -shm");

        let ro_wal_only = Connection::open_schema_only_with_wal_index_recovery(&path_str)
            .expect("P0-3: recovery readonly must snapshot-read -wal without -shm");
        let count: i64 = ro_wal_only
            .query_row("SELECT COUNT(*) FROM t")
            .expect("count")
            .get_typed(0)
            .unwrap();
        assert_eq!(count, 2);
        ro_wal_only.close().expect("close wal-only snapshot");
        assert!(!shm_path.exists(), "snapshot fallback must not create -shm");
        assert_eq!(std::fs::read(&wal_path).expect("wal after"), wal_before);
    }

    #[cfg(unix)]
    #[test]
    fn strict_readonly_rejects_shm_salt_and_mx_frame_mismatch() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("coherent_wal.db");
        let path_str = path.to_string_lossy().into_owned();

        let conn = Connection::open(&path_str).expect("open");
        conn.execute("PRAGMA journal_mode = WAL;").expect("wal");
        conn.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (1);")
            .expect("seed");
        conn.close_without_checkpoint().expect("close");

        let shm_path = sidecar_path(&path, "-shm");
        let orig_shm = std::fs::read(&shm_path).expect("orig shm");
        let strict = compat::OpenFlags::SQLITE_OPEN_READ_ONLY
            | compat::OpenFlags::SQLITE_OPEN_STRICT_READ_ONLY;

        // 1) aSalt mismatch (valid header checksum, wrong salt[0]).
        let mut bad_salt = orig_shm.clone();
        bad_salt[32] ^= 0xFF;
        recompute_shm_checksums(&mut bad_salt);
        std::fs::write(&shm_path, &bad_salt).expect("write bad salt shm");
        let err = compat::open_with_flags(&path_str, strict)
            .expect_err("salt mismatch must return BusyRecovery");
        assert!(matches!(err, FrankenError::BusyRecovery), "got {err:?}");

        // 2) mxFrame beyond actual -wal byte length (valid header checksum).
        let mut bad_mx_frame = orig_shm.clone();
        let inflated_frames = 9999_u32;
        bad_mx_frame[16..20].copy_from_slice(&inflated_frames.to_ne_bytes());
        recompute_shm_checksums(&mut bad_mx_frame);
        std::fs::write(&shm_path, &bad_mx_frame).expect("write bad mxFrame shm");
        let err = compat::open_with_flags(&path_str, strict)
            .expect_err("mxFrame exceeding -wal size must return BusyRecovery");
        assert!(matches!(err, FrankenError::BusyRecovery), "got {err:?}");

        // Restore valid shm -> strict open succeeds again.
        std::fs::write(&shm_path, &orig_shm).expect("restore shm");
        let ok = compat::open_with_flags(&path_str, strict).expect("valid shm opens");
        assert_eq!(
            ok.query_row("SELECT x FROM t")
                .expect("read")
                .get_typed::<i64>(0)
                .unwrap(),
            1
        );
    }

    #[cfg(unix)]
    #[test]
    fn p0_8_stamp_probe_preserves_live_writer_posix_locks() {
        const CHILD_ENV: &str = "CASS_TEST_FRANKEN_SYNC_STAMP_LOCK_PROBE_DB";
        const TEST_NAME: &str =
            "franken_sync::tests::p0_8_stamp_probe_preserves_live_writer_posix_locks";

        if let Some(probe_db) = std::env::var_os(CHILD_ENV) {
            let probe_str = probe_db.to_string_lossy().into_owned();
            let probe = Connection::open(&probe_str).expect("child open");
            probe
                .execute("PRAGMA busy_timeout = 0")
                .expect("child timeout");
            match probe.execute("BEGIN IMMEDIATE") {
                Ok(_) => {
                    let _ = probe.execute_batch("ROLLBACK");
                    println!("\nLOCK_PROBE=ACQUIRED");
                }
                Err(error) if error.is_busy_class() => {
                    println!("\nLOCK_PROBE=HELD_BY_PARENT");
                }
                Err(error) => panic!("unexpected child lock probe error: {error:?}"),
            }
            return;
        }

        let probe_child = |db_path: &std::path::Path| -> String {
            let output = std::process::Command::new(std::env::current_exe().expect("current_exe"))
                .args(["--exact", TEST_NAME, "--test-threads=1", "--nocapture"])
                .env(CHILD_ENV, db_path)
                .output()
                .expect("spawn lock probe child");
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(
                output.status.success(),
                "child lock probe failed ({}):\nstdout:\n{stdout}\nstderr:\n{stderr}",
                output.status
            );
            stdout
                .lines()
                .find_map(|line| {
                    line.split_once("LOCK_PROBE=")
                        .and_then(|(_, rest)| rest.split_whitespace().next())
                })
                .map(str::to_owned)
                .unwrap_or_else(|| panic!("missing LOCK_PROBE line in stdout:\n{stdout}"))
        };

        let dir = tempfile::tempdir().expect("tempdir");

        // Rollback mode writer holding BEGIN IMMEDIATE.
        let rb_db = dir.path().join("rb_stamp.db");
        let rb_str = rb_db.to_string_lossy().into_owned();
        let rb_writer = Connection::open(&rb_str).expect("open rb");
        rb_writer
            .execute("PRAGMA journal_mode = DELETE;")
            .expect("delete");
        rb_writer
            .execute_batch(
                "PRAGMA application_id = 1128354643;
                 PRAGMA user_version = 1;
                 CREATE TABLE t(x);",
            )
            .expect("seed rb");
        rb_writer.execute("BEGIN IMMEDIATE;").expect("begin rb");
        assert_eq!(probe_child(&rb_db), "HELD_BY_PARENT");
        assert_eq!(
            super::probe_sqlite_header_stamp_safely(&rb_db).expect("probe rb"),
            super::SqliteHeaderStamp::Stamp {
                application_id: 0x4341_5353,
                user_version: 1,
            }
        );
        assert_eq!(probe_child(&rb_db), "HELD_BY_PARENT");
        rb_writer.execute_batch("ROLLBACK;").expect("rollback rb");
        assert_eq!(probe_child(&rb_db), "ACQUIRED");
        rb_writer.close().expect("close rb");

        // WAL mode writer holding BEGIN IMMEDIATE + uncommitted row.
        let wal_db = dir.path().join("wal_stamp.db");
        let wal_str = wal_db.to_string_lossy().into_owned();
        let wal_writer = Connection::open(&wal_str).expect("open wal");
        wal_writer
            .execute("PRAGMA journal_mode = WAL;")
            .expect("wal");
        wal_writer
            .execute_batch(
                "PRAGMA application_id = 1128354643;
                 PRAGMA user_version = 1;
                 CREATE TABLE t(x);
                 INSERT INTO t VALUES (1);",
            )
            .expect("seed wal");
        wal_writer.execute("BEGIN IMMEDIATE;").expect("begin wal");
        wal_writer
            .execute("INSERT INTO t VALUES (2);")
            .expect("insert wal");
        assert_eq!(probe_child(&wal_db), "HELD_BY_PARENT");
        assert_eq!(
            super::probe_sqlite_header_stamp_safely(&wal_db).expect("probe wal"),
            super::SqliteHeaderStamp::Stamp {
                application_id: 0x4341_5353,
                user_version: 1,
            }
        );
        assert_eq!(probe_child(&wal_db), "HELD_BY_PARENT");
        wal_writer.execute_batch("ROLLBACK;").expect("rollback wal");
        assert_eq!(probe_child(&wal_db), "ACQUIRED");
        wal_writer.close().expect("close wal");
    }

    #[test]
    fn multi_statement_execute_updates_live_wal_state_even_on_later_failure() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("multi_wal_state.db");
        let db_str = db_path.to_string_lossy().into_owned();

        let conn = Connection::open(&db_str).expect("open");
        let id = conn.file_identity().expect("id").expect("some id");
        assert_eq!(
            super::sidecar_probes::live_connection_state(id),
            Some(Some(false))
        );

        conn.execute("CREATE TABLE t(x); PRAGMA journal_mode = WAL; INSERT INTO t VALUES (1);")
            .expect("multi-statement wal switch");
        assert_eq!(
            super::sidecar_probes::live_connection_state(id),
            Some(Some(true))
        );
        let row = conn.query_row("SELECT x FROM t").expect("query row");
        assert_eq!(row.get_typed::<i64>(0).unwrap(), 1);

        conn.execute("PRAGMA journal_mode = DELETE; THIS IS NOT VALID SQL;")
            .expect_err("trailing invalid statement must fail");
        assert_eq!(
            super::sidecar_probes::live_connection_state(id),
            Some(Some(false))
        );
        conn.close().expect("close");
    }

    #[test]
    fn multi_statement_execute_with_params_binds_first_statement_only() {
        let conn = conn();
        conn.execute("CREATE TABLE t(x INTEGER);").expect("create");
        conn.execute_with_params(
            "INSERT INTO t(x) VALUES (?1); INSERT INTO t(x) VALUES (20);",
            &[SqliteValue::Integer(10)],
        )
        .expect("multi-statement with params");
        let rows = conn.query("SELECT x FROM t ORDER BY x").expect("select");
        assert_eq!(
            rows.iter()
                .map(|r| r.get_typed::<i64>(0).unwrap())
                .collect::<Vec<_>>(),
            vec![10, 20]
        );
    }

    #[test]
    fn multi_statement_execute_drains_multi_row_returning_statements() {
        let conn = conn();
        conn.execute(
            "CREATE TABLE t(x INTEGER);
             INSERT INTO t(x) VALUES (1), (2) RETURNING x;
             INSERT INTO t(x) VALUES (3);",
        )
        .expect("multi-statement returning");
        let rows = conn.query("SELECT x FROM t ORDER BY x").expect("select");
        assert_eq!(
            rows.iter()
                .map(|r| r.get_typed::<i64>(0).unwrap())
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn multi_statement_execute_stops_on_cancellation_scope() {
        let conn = conn();
        let cancellation = CancellationHandle::new();
        {
            let _scope = cancellation.enter();
            cancellation.cancel();
            let err = conn
                .execute("CREATE TABLE t(x); INSERT INTO t VALUES (1);")
                .expect_err("cancelled multi-statement execute must abort");
            assert!(matches!(err, FrankenError::Abort), "got {err:?}");
        }
    }

    #[test]
    fn rw_drop_checkpoints_while_ro_and_snapshot_drop_preserve_wal() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("drop_ckpt.db");
        let path_str = path.to_string_lossy().into_owned();
        let wal_path = sidecar_path(&path, "-wal");
        let shm_path = sidecar_path(&path, "-shm");

        {
            let rw = Connection::open(&path_str).expect("open rw");
            rw.execute("PRAGMA journal_mode = WAL;").expect("wal");
            rw.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (1);")
                .expect("seed");
            drop(rw);
        }
        assert!(
            !wal_path.exists() || std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0) == 0,
            "dropping RW connection must checkpoint/truncate WAL"
        );
        assert!(!shm_path.exists(), "dropping RW connection must clean up -shm");

        {
            let rw = Connection::open(&path_str).expect("reopen rw");
            rw.execute("INSERT INTO t VALUES (2);").expect("insert");
            rw.close_without_checkpoint().expect("close without ckpt");
        }
        let wal_before = std::fs::read(&wal_path).expect("wal before");
        assert!(wal_before.len() > 32, "precondition: dirty wal frames");

        {
            let snap = Connection::open_schema_only_with_wal_index_recovery(&path_str)
                .expect("recovery ro snapshot");
            let count: i64 = snap
                .query_row("SELECT COUNT(*) FROM t")
                .expect("count")
                .get_typed(0)
                .unwrap();
            assert_eq!(count, 2);
            drop(snap);
        }
        assert_eq!(
            std::fs::read(&wal_path).expect("wal after snapshot drop"),
            wal_before,
            "dropping RO snapshot must never checkpoint or modify original -wal"
        );
    }

    #[cfg(unix)]
    #[test]
    fn snapshot_copy_reopens_readonly_and_resolves_symlink_sidecars() {
        let dir = tempfile::tempdir().expect("tempdir");
        let real_dir = dir.path().join("real");
        std::fs::create_dir(&real_dir).expect("mkdir");
        let real_db = real_dir.join("archive.db");
        let real_str = real_db.to_string_lossy().into_owned();

        {
            let rw = Connection::open(&real_str).expect("open real");
            rw.execute("PRAGMA journal_mode = WAL;").expect("wal");
            rw.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (42);")
                .expect("seed");
            rw.close_without_checkpoint().expect("preserve wal");
        }

        let link_db = dir.path().join("link.db");
        std::os::unix::fs::symlink(&real_db, &link_db).expect("symlink");

        let snap =
            Connection::open_schema_only_with_wal_index_recovery(link_db.to_string_lossy().as_ref())
                .expect("snapshot via symlink");
        assert_eq!(
            snap.query_row("SELECT x FROM t")
                .expect("read via symlink")
                .get_typed::<i64>(0)
                .unwrap(),
            42
        );
        assert_eq!(
            snap.snapshot_source_identity(),
            FileIdentity::from_path(&real_db).expect("real id")
        );
        snap.execute("PRAGMA query_only = OFF;")
            .expect("query_only pragma executes");
        let write_err = snap
            .execute("CREATE TABLE should_fail(x);")
            .expect_err("snapshot handle must stay read-only at fd level");
        assert!(
            matches!(write_err, FrankenError::ReadOnly),
            "expected ReadOnly after PRAGMA query_only = OFF, got {write_err:?}"
        );
    }

    #[test]
    fn recovery_readonly_and_stamp_probe_do_not_poison_writer_shm() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("no_shm_poison.db");
        let db_str = db_path.to_string_lossy().into_owned();

        {
            let rw = Connection::open(&db_str).expect("open rw");
            rw.execute_batch(
                "PRAGMA application_id = 1128354643;
                 PRAGMA user_version = 1;
                 PRAGMA journal_mode = WAL;
                 CREATE TABLE t(x);
                 INSERT INTO t VALUES (1);",
            )
            .expect("seed");
            rw.close_without_checkpoint().expect("preserve wal+shm");
        }

        assert!(matches!(
            super::sidecar_probes::probe_sqlite_header_stamp_safely(&db_path)
                .expect("stamp probe"),
            super::sidecar_probes::SqliteHeaderStamp::Stamp {
                application_id: 1128354643,
                user_version: 1
            }
        ));

        let ro = Connection::open_schema_only_with_wal_index_recovery(&db_str)
            .expect("recovery ro");
        assert_eq!(
            ro.query_row("SELECT x FROM t")
                .expect("read")
                .get_typed::<i64>(0)
                .unwrap(),
            1
        );

        let rw2 = Connection::open(&db_str).expect("open writer after recovery ro");
        rw2.execute_batch("BEGIN IMMEDIATE; INSERT INTO t VALUES (2); COMMIT;")
            .expect("writer must not hit SQLITE_READONLY");
        drop(ro);
    }

    #[cfg(unix)]
    #[test]
    fn strict_readonly_accepts_zero_byte_wal_with_sz_page_zero_shm() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("empty_wal_shm.db");
        let db_str = db_path.to_string_lossy().into_owned();
        let wal_path = sidecar_path(&db_path, "-wal");
        let shm_path = sidecar_path(&db_path, "-shm");

        {
            let rw = Connection::open(&db_str).expect("open rw");
            rw.execute("PRAGMA journal_mode = WAL;").expect("wal");
            rw.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (7);")
                .expect("seed");
            rw.close().expect("clean close");
        }
        // Simulate a 0-byte `-wal` + valid `sz_page=0, mx_frame=0` `-shm` written
        // by C SQLite `walIndexRecover` on `nSize == 0`.
        std::fs::write(&wal_path, []).expect("create 0-byte wal");
        let mut shm = vec![0_u8; 32768];
        shm[0..4].copy_from_slice(&3_007_000_u32.to_ne_bytes());
        shm[12] = 1; // isInit
        shm[13] = 0; // bigEndCksum
        shm[14..16].copy_from_slice(&0_u16.to_ne_bytes()); // szPage = 0
        shm[16..20].copy_from_slice(&0_u32.to_ne_bytes()); // mxFrame = 0
        recompute_shm_checksums(&mut shm);
        std::fs::write(&shm_path, &shm).expect("write zero-sz_page shm");

        let strict = compat::OpenFlags::SQLITE_OPEN_READ_ONLY
            | compat::OpenFlags::SQLITE_OPEN_STRICT_READ_ONLY;
        let ro = compat::open_with_flags(&db_str, strict)
            .expect("0-byte wal with szPage=0,mxFrame=0 shm must be valid");
        assert_eq!(
            ro.query_row("SELECT x FROM t")
                .expect("read")
                .get_typed::<i64>(0)
                .unwrap(),
            7
        );
    }

    #[test]
    fn recovery_readonly_succeeds_while_live_writer_has_no_wal_sidecars_yet() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("live_wal_no_sidecars.db");
        let db_str = db_path.to_string_lossy().into_owned();
        let wal_path = sidecar_path(&db_path, "-wal");
        let shm_path = sidecar_path(&db_path, "-shm");

        {
            let seed = Connection::open(&db_str).expect("seed open");
            seed.execute("PRAGMA journal_mode = WAL;").expect("wal");
            seed.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (5);")
                .expect("seed");
            seed.close().expect("clean close");
        }
        assert!(!wal_path.exists());
        assert!(!shm_path.exists());

        let live_rw = Connection::open(&db_str).expect("reopen live rw");
        let ro = Connection::open_schema_only_with_wal_index_recovery(&db_str)
            .expect("live RW handle with !wal && !shm must allow in-place recovery ro");
        assert_eq!(
            ro.query_row("SELECT x FROM t")
                .expect("read")
                .get_typed::<i64>(0)
                .unwrap(),
            5
        );
        drop(ro);
        live_rw
            .execute_batch("BEGIN IMMEDIATE; INSERT INTO t VALUES (6); COMMIT;")
            .expect("live writer remains writable");
    }

    #[test]
    fn in_place_wal_index_recovery_fixes_missing_shm_without_mutating_db_or_wal() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("recover_shm_inplace.db");
        let db_str = db_path.to_string_lossy().into_owned();
        let wal_path = sidecar_path(&db_path, "-wal");
        let shm_path = sidecar_path(&db_path, "-shm");

        {
            let rw = Connection::open(&db_str).expect("open rw");
            rw.execute("PRAGMA journal_mode = WAL;").expect("wal");
            rw.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (21);")
                .expect("seed");
            rw.close_without_checkpoint().expect("preserve wal");
        }
        std::fs::remove_file(&shm_path).expect("remove shm");
        let main_before = std::fs::read(&db_path).expect("main before");
        let wal_before = std::fs::read(&wal_path).expect("wal before");

        let recovered = Connection::open_readonly_recovering_wal_index_in_place(&db_str)
            .expect("recover shm in place");
        assert_eq!(
            recovered
                .query_row("SELECT x FROM t")
                .expect("read")
                .get_typed::<i64>(0)
                .unwrap(),
            21
        );
        assert!(
            recovered.execute("INSERT INTO t VALUES (99);").is_err(),
            "in-place recovery handle must stay query_only"
        );
        drop(recovered);

        assert_eq!(std::fs::read(&db_path).expect("main after"), main_before);
        assert_eq!(std::fs::read(&wal_path).expect("wal after"), wal_before);
        assert!(shm_path.exists(), "-shm must be recovered in place");

        let strict = compat::OpenFlags::SQLITE_OPEN_READ_ONLY
            | compat::OpenFlags::SQLITE_OPEN_STRICT_READ_ONLY;
        let strict_ro =
            compat::open_with_flags(&db_str, strict).expect("strict readonly after shm recovery");
        assert_eq!(
            strict_ro
                .query_row("SELECT x FROM t")
                .expect("strict read")
                .get_typed::<i64>(0)
                .unwrap(),
            21
        );
        drop(strict_ro);

        let rw2 = Connection::open(&db_str).expect("writer after in-place shm recovery");
        rw2.execute_batch("BEGIN IMMEDIATE; INSERT INTO t VALUES (22); COMMIT;")
            .expect("writer must not hit SQLITE_READONLY");
    }
