use crate::cli::state::state_db_open_timeout::{
    STATUS_COUNT_SCAN_MAX_DB_BYTES, bounded_canonical_db_corruption_probe,
};
use crate::doctor::forensic::doctor_sqlite_sidecar_path;
use std::path::Path;

/// Build a minimal canonical-shaped DB (the two tables the bounded probe
/// projects) with one real row each.
pub(crate) fn seed_probe_shaped_db(db_path: &Path) {
    let conn = crate::franken_sync::Connection::open(db_path.display().to_string())
        .expect("create probe fixture db");
    conn.execute_batch(
        "CREATE TABLE conversations (
                 id INTEGER PRIMARY KEY,
                 title TEXT,
                 source_path TEXT NOT NULL
             );
             CREATE TABLE messages (
                 id INTEGER PRIMARY KEY,
                 conversation_id INTEGER NOT NULL,
                 role TEXT NOT NULL
             );
             INSERT INTO conversations (id, title, source_path)
                 VALUES (1, 'probe fixture', '/tmp/fixture.jsonl');
             INSERT INTO messages (id, conversation_id, role)
                 VALUES (1, 1, 'user');",
    )
    .expect("seed probe fixture rows");
    // Explicit close runs the final passive WAL checkpoint so the table
    // pages land in the MAIN file — the corruption test smashes those
    // pages, and a dropped-not-closed connection leaves them in the WAL
    // sidecar (observed: a 4096-byte single-page main file).
    conn.close().expect("checkpoint fixture into the main file");
}

/// i6upe: the bounded probe reports nothing for a missing or healthy DB —
/// it must never manufacture a corruption verdict.
#[test]
pub(crate) fn bounded_corruption_probe_passes_missing_and_healthy_dbs() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("agent_search.db");
    assert_eq!(
        bounded_canonical_db_corruption_probe(&db_path, "i6upe-missing"),
        None,
        "a missing DB is not corrupt"
    );

    seed_probe_shaped_db(&db_path);
    assert_eq!(
        bounded_canonical_db_corruption_probe(&db_path, "i6upe-healthy"),
        None,
        "a healthy DB with real rows must pass the probe"
    );
}

/// GH #413: the pre-index corruption guard has a fixed 100-byte I/O
/// budget. Corrupt header fields must still fail closed without using a
/// SQL query whose physical work can scale with archive size.
#[test]
pub(crate) fn bounded_corruption_probe_flags_malformed_header() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("agent_search.db");
    seed_probe_shaped_db(&db_path);
    let mut bytes = std::fs::read(&db_path).expect("read fixture database");
    bytes[16] = 0;
    bytes[17] = 3;
    std::fs::write(&db_path, bytes).expect("write malformed page-size header");

    let verdict = bounded_canonical_db_corruption_probe(&db_path, "gh413-corrupt")
        .expect("invalid fixed header fields must produce a corruption verdict");
    assert!(
        verdict.contains("page size 3"),
        "unexpected verdict: {verdict}"
    );
}

/// GH #413: once the physical bundle exceeds the explicit ceiling, the
/// pre-index guard must stop after the fixed header read and never open
/// fsqlite. Namespace identity sidecars are a causal open marker.
#[test]
pub(crate) fn bounded_corruption_probe_does_not_open_large_archive() {
    use std::io::Write as _;

    let dir = tempfile::tempdir().expect("tempdir");
    let seed_path = dir.path().join("seed.db");
    seed_probe_shaped_db(&seed_path);
    let seed_header = std::fs::read(&seed_path).expect("read seed database");

    let db_path = dir.path().join("large-agent-search.db");
    let mut large = std::fs::File::create(&db_path).expect("create sparse large archive");
    large
        .write_all(&seed_header[..100])
        .expect("write valid SQLite header");
    let encoded_page_size = u16::from_be_bytes([seed_header[16], seed_header[17]]);
    let page_size = if encoded_page_size == 1 {
        65_536_u64
    } else {
        u64::from(encoded_page_size)
    };
    let first_page_boundary_above_limit = STATUS_COUNT_SCAN_MAX_DB_BYTES
        .saturating_add(page_size)
        .div_ceil(page_size)
        .saturating_mul(page_size);
    large
        .set_len(first_page_boundary_above_limit)
        .expect("extend sparse large archive beyond deep-probe ceiling");
    drop(large);

    assert_eq!(
        bounded_canonical_db_corruption_probe(&db_path, "gh413-large"),
        None,
        "a valid large-file header must not trigger a false corruption verdict"
    );
    for suffix in ["-fsqlite-ns-gate", "-fsqlite-ns-use"] {
        let sidecar = doctor_sqlite_sidecar_path(&db_path, suffix).expect("sidecar path");
        assert!(
            !sidecar.exists(),
            "large-archive guard opened fsqlite and created {}",
            sidecar.display()
        );
    }
}

/// i6upe: below the explicit archive-bundle byte ceiling, keep the
/// real-column edge probes that catch damage outside the SQLite header.
#[test]
pub(crate) fn bounded_corruption_probe_flags_smashed_table_pages_for_small_archive() {
    use std::io::{Seek as _, SeekFrom, Write as _};

    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("agent_search.db");
    seed_probe_shaped_db(&db_path);

    let len = std::fs::metadata(&db_path).expect("stat fixture").len();
    assert!(
        len > 4096,
        "fixture must span multiple pages for a mid-file smash (len={len})"
    );
    let mut db = std::fs::OpenOptions::new()
        .write(true)
        .open(&db_path)
        .expect("open fixture for corruption");
    db.seek(SeekFrom::Start(4096)).expect("seek past page 1");
    db.write_all(&vec![0xFF_u8; (len - 4096) as usize])
        .expect("smash table pages");
    drop(db);

    let wal_path = doctor_sqlite_sidecar_path(&db_path, "-wal").expect("WAL path");
    if let Ok(wal_len) = std::fs::metadata(&wal_path).map(|metadata| metadata.len())
        && wal_len > 0
    {
        let mut wal = std::fs::OpenOptions::new()
            .write(true)
            .open(&wal_path)
            .expect("open WAL sidecar for corruption");
        wal.write_all(&vec![0xFF_u8; wal_len as usize])
            .expect("smash WAL frames");
    }

    let verdict = bounded_canonical_db_corruption_probe(&db_path, "i6upe-corrupt")
        .expect("small smashed archive must produce a corruption verdict");
    let lower = verdict.to_ascii_lowercase();
    assert!(
        lower.contains("malformed") || lower.contains("corrupt") || lower.contains("disk image"),
        "unexpected smashed-page verdict: {verdict}"
    );
}
