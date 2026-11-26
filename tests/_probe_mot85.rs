//! Manual diagnostic for bead `coding_agent_session_search-mot85`.
//!
//! mot85 tracks the SQLite engine capability required to eliminate the
//! last two `rusqlite::Connection::open` call sites in cass
//! (`src/storage/sqlite/tests/mod.rs::rusqlite_test_fixture_conn`): supporting
//! `INSERT INTO sqlite_master` when `PRAGMA writable_schema = ON` is set.
//!
//! Run manually:
//!
//! ```bash
//! cargo test --test _probe_mot85 -- --ignored --nocapture
//! ```
//!
//! When this test *passes*, the SQLite facade accepts direct `sqlite_master`
//! writes under `PRAGMA writable_schema = ON` so the remaining test fixture
//! sites can migrate to `franken_sync::Connection`. When it fails with
//! `Err(Internal("no such table: sqlite_master"))` or a schema-write refusal,
//! direct `sqlite_master` mutation remains blocked.
//!
//! Do NOT run this as part of the regular test suite — it intentionally
//! panics on failure to surface the engine status, and the `#[ignore]`
//! attribute keeps it out of CI.
#[test]
#[ignore = "manual diagnostic for bead mot85; run with --ignored"]
fn probe_mot85_sqlite_writable_schema_writes() {
    use coding_agent_search::franken_sync::Connection as FrankenConnection;
    let tmpdir = tempfile::tempdir().unwrap();
    let db_path = tmpdir.path().join("probe.db");
    let conn = FrankenConnection::open(db_path.to_string_lossy().into_owned()).unwrap();
    conn.execute("CREATE TABLE foo (id INTEGER)").unwrap();
    let pragma = conn.execute("PRAGMA writable_schema = ON");
    eprintln!("[mot85-probe] writable_schema=ON result: {pragma:?}");
    let insert = conn.execute(
        "INSERT INTO sqlite_master(type, name, tbl_name, rootpage, sql) \
         VALUES('table', 'fixture_tbl', 'fixture_tbl', 0, 'CREATE TABLE fixture_tbl(x)')",
    );
    eprintln!("[mot85-probe] INSERT sqlite_master result: {insert:?}");
    assert!(
        insert.is_ok(),
        "mot85 is still blocked: INSERT INTO sqlite_master returned {insert:?}"
    );
}
