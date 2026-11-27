//! SQLite Compatibility Gate Tests
//!
//! These tests verify that the bundled `rusqlite` SQLite engine and `franken_sync`
//! facade handle the critical features cass depends on: FTS5, C-SQLite file
//! compatibility, query-plan seek invariants, and schema-v1 parity.
//!
//! Gate 1: FTS5 full-text search (CREATE VIRTUAL TABLE, MATCH, highlight, etc.)
//! Gate 2: Existing C SQLite database file compatibility (read rusqlite-created DBs)

use coding_agent_search::franken_sync::Connection;
use coding_agent_search::franken_sync::Connection as FrankenConnection;
use coding_agent_search::franken_sync::SqliteValue;
use coding_agent_search::franken_sync::compat::RowExt;
use std::fmt::Write as _;

use coding_agent_search::storage::sqlite::{CURRENT_SCHEMA_VERSION, FrankenStorage, SqliteStorage};
use coding_agent_search::viewer_extensions::schema;

fn resolve_dependency_table_with_workspace(
    manifest: &toml::Table,
    key: &str,
) -> Option<toml::Table> {
    let ws_entry = manifest
        .get("workspace")
        .and_then(toml::Value::as_table)
        .and_then(|ws| ws.get("dependencies"))
        .and_then(toml::Value::as_table)
        .and_then(|deps| deps.get(key))
        .and_then(toml::Value::as_table);
    let dep_entry = manifest
        .get("dependencies")
        .and_then(toml::Value::as_table)
        .and_then(|deps| deps.get(key))
        .and_then(toml::Value::as_table);

    match (dep_entry, ws_entry) {
        (Some(dep), Some(ws))
            if dep.get("workspace").and_then(toml::Value::as_bool) == Some(true) =>
        {
            let mut merged = ws.clone();
            for (k, v) in dep {
                if k == "workspace" {
                    continue;
                }
                if k == "features" {
                    let mut combined = merged
                        .get("features")
                        .and_then(toml::Value::as_array)
                        .cloned()
                        .unwrap_or_default();
                    if let Some(extra) = v.as_array() {
                        for feat in extra {
                            if !combined.contains(feat) {
                                combined.push(feat.clone());
                            }
                        }
                    }
                    merged.insert("features".to_string(), toml::Value::Array(combined));
                } else {
                    merged.insert(k.clone(), v.clone());
                }
            }
            Some(merged)
        }
        (Some(dep), _) => Some(dep.clone()),
        (None, Some(ws)) => Some(ws.clone()),
        (None, None) => None,
    }
}

fn check_rusqlite_pinned_in_manifest(manifest: &toml::Table) {
    let dependencies = manifest
        .get("dependencies")
        .and_then(toml::Value::as_table);
    let workspace_dependencies = manifest
        .get("workspace")
        .and_then(toml::Value::as_table)
        .and_then(|ws| ws.get("dependencies"))
        .and_then(toml::Value::as_table);
    assert!(
        dependencies.is_some() || workspace_dependencies.is_some(),
        "Cargo.toml must declare [dependencies] or [workspace.dependencies]"
    );
    let has_rusqlite = dependencies.is_some_and(|deps| deps.contains_key("rusqlite"))
        || workspace_dependencies.is_some_and(|deps| deps.contains_key("rusqlite"));
    assert!(
        has_rusqlite,
        "rusqlite must be a direct production dependency"
    );
    let legacy_facade = concat!("franken", "sqlite");
    let legacy_engine = concat!("fsql", "ite");
    let legacy_types = concat!("fsql", "ite-types");
    for forbidden in [legacy_facade, legacy_engine, legacy_types] {
        if let Some(deps) = dependencies {
            assert!(
                !deps.contains_key(forbidden),
                "{forbidden} must not appear in [dependencies]"
            );
        }
        if let Some(ws_deps) = workspace_dependencies {
            assert!(
                !ws_deps.contains_key(forbidden),
                "{forbidden} must not appear in [workspace.dependencies]"
            );
        }
    }

    let dev_dependencies = manifest
        .get("dev-dependencies")
        .and_then(toml::Value::as_table);
    if workspace_dependencies.is_none() {
        assert!(
            dev_dependencies.is_some(),
            "Cargo.toml dev-dependencies table"
        );
    }
    if let Some(dev_dependencies) = dev_dependencies {
        for forbidden in ["rusqlite", legacy_facade, legacy_engine, legacy_types] {
            assert!(
                !dev_dependencies.contains_key(forbidden),
                "{forbidden} must not appear in [dev-dependencies]"
            );
        }
    }
}

#[test]
fn rusqlite_is_pinned_production_dependency() {
    let manifest: toml::Table =
        toml::from_str(include_str!("../Cargo.toml")).expect("parse Cargo.toml");
    check_rusqlite_pinned_in_manifest(&manifest);
}

#[test]
fn rusqlite_workspace_dependency_inheritance_is_validated() {
    let ws_manifest: toml::Table = toml::from_str(
        r#"
[workspace]
members = ["crates/cass-storage"]

[workspace.dependencies]
rusqlite = { version = "=0.40.2", features = ["bundled", "modern_sqlite"] }

[dependencies]
rusqlite = { workspace = true }
"#,
    )
    .expect("parse workspace manifest");
    check_rusqlite_pinned_in_manifest(&ws_manifest);
    let resolved = resolve_dependency_table_with_workspace(&ws_manifest, "rusqlite")
        .expect("resolved rusqlite table");
    assert_eq!(
        resolved.get("version").and_then(toml::Value::as_str),
        Some("=0.40.2")
    );
    let features: Vec<&str> = resolved
        .get("features")
        .and_then(toml::Value::as_array)
        .expect("features array")
        .iter()
        .filter_map(toml::Value::as_str)
        .collect();
    assert_eq!(features, &["bundled", "modern_sqlite"]);
}

/// The SQLite engine resolves exclusively from crates.io `rusqlite =0.40.2`
/// (`bundled` + `modern_sqlite`), with zero legacy pure-Rust SQLite packages
/// in the manifest or lockfile, and `[patch.crates-io]` carries only the
/// owner's `frankensearch-quill` fork pin.
#[test]
fn sqlite_dependency_source_identity_is_exact_and_coherent() {
    const RUSQLITE_REQUIREMENT: &str = "=0.40.2";
    const EXPECTED_RUSQLITE_FEATURES: &[&str] = &["bundled", "modern_sqlite"];
    const EXPECTED_QUILL_GIT: &str = "https://github.com/vladmlch/frankensearch";
    const EXPECTED_QUILL_REV: &str = "f75d1d551c5fe73d0d4870a26b723faf6921de33";
    let legacy_facade = concat!("franken", "sqlite");
    let legacy_engine = concat!("fsql", "ite");
    let legacy_engine_prefix = concat!("fsql", "ite-");
    let expected_quill_lock_source =
        format!("git+{EXPECTED_QUILL_GIT}?rev={EXPECTED_QUILL_REV}#{EXPECTED_QUILL_REV}");

    let manifest: toml::Table =
        toml::from_str(include_str!("../Cargo.toml")).expect("parse Cargo.toml");
    let dependency = resolve_dependency_table_with_workspace(&manifest, "rusqlite")
        .expect("missing structured rusqlite dependency entry");
    assert_eq!(
        dependency.get("version").and_then(toml::Value::as_str),
        Some(RUSQLITE_REQUIREMENT),
        "rusqlite declared version drifted in [dependencies]"
    );
    assert!(
        dependency.get("git").is_none()
            && dependency.get("rev").is_none()
            && dependency.get("path").is_none()
            && dependency.get("branch").is_none()
            && dependency.get("tag").is_none()
            && dependency.get("package").is_none(),
        "rusqlite in [dependencies] must stay a registry-pinned `=0.40.2` entry"
    );
    let features: Vec<&str> = dependency
        .get("features")
        .and_then(toml::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(toml::Value::as_str)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    assert_eq!(
        features, EXPECTED_RUSQLITE_FEATURES,
        "rusqlite feature set drifted"
    );

    let patch_crates_io = manifest
        .get("patch")
        .and_then(toml::Value::as_table)
        .and_then(|patch| patch.get("crates-io"))
        .and_then(toml::Value::as_table)
        .expect("[patch.crates-io] must carry the frankensearch-quill pin");
    assert_eq!(
        patch_crates_io.len(),
        1,
        "[patch.crates-io] must contain only frankensearch-quill, got: {:?}",
        patch_crates_io.keys().collect::<Vec<_>>()
    );
    let quill_spec = patch_crates_io
        .get("frankensearch-quill")
        .and_then(toml::Value::as_table)
        .expect("frankensearch-quill [patch.crates-io] entry must be a table");
    assert_eq!(
        quill_spec.get("git").and_then(toml::Value::as_str),
        Some(EXPECTED_QUILL_GIT),
        "frankensearch-quill patch entry points at the wrong remote"
    );
    assert_eq!(
        quill_spec.get("rev").and_then(toml::Value::as_str),
        Some(EXPECTED_QUILL_REV),
        "frankensearch-quill patch entry drifted off the pinned fork rev"
    );
    assert!(
        !quill_spec.contains_key("path")
            && !quill_spec.contains_key("branch")
            && !quill_spec.contains_key("tag"),
        "frankensearch-quill patch entry must stay a rev-pinned git redirect"
    );

    let lockfile: toml::Value =
        toml::from_str(include_str!("../Cargo.lock")).expect("parse Cargo.lock");
    let packages = lockfile
        .get("package")
        .and_then(toml::Value::as_array)
        .expect("Cargo.lock package array");
    let legacy_sqlite_packages: Vec<&str> = packages
        .iter()
        .filter_map(|package| package.get("name").and_then(toml::Value::as_str))
        .filter(|name| {
            *name == legacy_facade
                || *name == legacy_engine
                || name.starts_with(legacy_engine_prefix)
        })
        .collect();
    assert!(
        legacy_sqlite_packages.is_empty(),
        "Cargo.lock must not resolve any legacy SQLite engine packages, found: {legacy_sqlite_packages:?}"
    );

    let resolved_rusqlite: Vec<_> = packages
        .iter()
        .filter(|package| package.get("name").and_then(toml::Value::as_str) == Some("rusqlite"))
        .collect();
    assert_eq!(
        resolved_rusqlite.len(),
        1,
        "Cargo.lock must resolve exactly one rusqlite package"
    );
    assert_eq!(
        resolved_rusqlite[0]
            .get("version")
            .and_then(toml::Value::as_str),
        Some("0.40.2"),
        "rusqlite version drifted in Cargo.lock"
    );
    assert_eq!(
        resolved_rusqlite[0]
            .get("source")
            .and_then(toml::Value::as_str),
        Some("registry+https://github.com/rust-lang/crates.io-index"),
        "rusqlite must resolve from crates.io"
    );

    let resolved_quill: Vec<_> = packages
        .iter()
        .filter(|package| {
            package.get("name").and_then(toml::Value::as_str) == Some("frankensearch-quill")
        })
        .collect();
    assert_eq!(
        resolved_quill.len(),
        1,
        "Cargo.lock must resolve exactly one frankensearch-quill package"
    );
    assert_eq!(
        resolved_quill[0]
            .get("source")
            .and_then(toml::Value::as_str),
        Some(expected_quill_lock_source.as_str()),
        "frankensearch-quill resolved away from the pinned owner-fork rev"
    );

    let build_contract = include_str!("../build.rs");
    assert!(
        build_contract.contains("expected_version: \"0.40.2\"")
            && build_contract.contains("expected_features: &[\"bundled\", \"modern_sqlite\"]")
            && build_contract.contains(EXPECTED_QUILL_GIT)
            && build_contract.contains(EXPECTED_QUILL_REV)
            && !build_contract.contains(legacy_facade)
            && !build_contract.contains(concat!("fsql", "ite-types")),
        "build.rs must validate the exact rusqlite + frankensearch-quill dependency contract"
    );
    let readme = include_str!("../README.md");
    assert!(
        readme.contains(RUSQLITE_REQUIREMENT)
            && readme.contains("bundled")
            && readme.contains("modern_sqlite")
            && readme.contains(EXPECTED_QUILL_REV),
        "README must document both exact rusqlite pins and the frankensearch-quill rev"
    );
}

/// `fuzz/` is its own Cargo workspace, so the root [patch.crates-io] table
/// does not reach it. When the two drift, the fuzz build resolves the
/// crates.io `frankensearch-quill` instead of the owner fork. The fuzz
/// workspace must carry the root table entry for entry.
#[test]
fn fuzz_workspace_patch_table_matches_root() {
    let patch_table = |manifest_text: &str, name: &str| -> toml::Table {
        let manifest: toml::Table =
            toml::from_str(manifest_text).unwrap_or_else(|err| panic!("parse {name}: {err}"));
        manifest
            .get("patch")
            .and_then(toml::Value::as_table)
            .and_then(|patch| patch.get("crates-io"))
            .and_then(toml::Value::as_table)
            .cloned()
            .unwrap_or_else(|| panic!("{name} must carry a [patch.crates-io] table"))
    };
    let root = patch_table(include_str!("../Cargo.toml"), "Cargo.toml");
    let fuzz = patch_table(include_str!("../fuzz/Cargo.toml"), "fuzz/Cargo.toml");
    assert!(!root.is_empty(), "root [patch.crates-io] table is empty");
    assert_eq!(
        fuzz, root,
        "fuzz/Cargo.toml [patch.crates-io] must equal the root table entry for entry"
    );
}

/// Exercise the exact API that disappeared from the same-version registry
/// archive. A missing database must remain missing, while an existing database
/// must reopen without schema initialization and retain its rows.
#[test]
fn sqlite_existing_schema_only_open_never_creates_and_reopens_exact_data() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let missing_path = dir.path().join("must-remain-missing.db");
    let missing = missing_path.to_string_lossy();
    let error = match FrankenConnection::open_existing_schema_only(missing.as_ref()) {
        Err(error) => error,
        Ok(_) => unreachable!("existing-only open must reject a missing database"),
    };
    assert!(
        !missing_path.exists(),
        "existing-only open must not create or zero-initialize a missing path"
    );
    assert!(
        !error.to_string().trim().is_empty(),
        "missing-path rejection must be actionable"
    );

    let database_path = dir.path().join("existing.db");
    {
        let path = database_path.to_string_lossy();
        let conn = FrankenConnection::open(path.as_ref()).expect("create fixture database");
        conn.execute("CREATE TABLE exact_rows(id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
            .expect("create exact_rows");
        conn.execute("INSERT INTO exact_rows(id, value) VALUES (7, 'preserved')")
            .expect("insert exact row");
    }
    let size_before = std::fs::metadata(&database_path)
        .expect("fixture database metadata")
        .len();
    {
        let path = database_path.to_string_lossy();
        let reopened = FrankenConnection::open_existing_schema_only(path.as_ref())
            .expect("reopen existing database without schema initialization");
        let rows = reopened
            .query("SELECT id, value FROM exact_rows")
            .expect("query exact preserved row");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0), Some(&SqliteValue::Integer(7)));
        assert_eq!(rows[0].get(1), Some(&SqliteValue::Text("preserved".into())));
    }
    assert_eq!(
        std::fs::metadata(&database_path)
            .expect("reopened database metadata")
            .len(),
        size_before,
        "schema-only reopen must not replace or truncate the database"
    );
}

/// Exercise the existing-only contract beyond a toy single-page fixture. The
/// durable database/WAL bundle spans thousands of pages so an implementation
/// that accidentally recreates, truncates, or only partially reopens the
/// archive cannot satisfy the count, sentinel, and artifact checks.
#[test]
fn sqlite_existing_schema_only_reopens_large_archive_without_truncation() {
    const ROW_COUNT: i64 = 2_048;
    const PAYLOAD_BYTES: usize = 4_096;
    const MINIMUM_DATABASE_BYTES: u64 = 8 * 1024 * 1024;

    let dir = tempfile::TempDir::new().expect("temp dir");
    let database_path = dir.path().join("large-existing.db");
    {
        let path = database_path.to_string_lossy();
        let conn = FrankenConnection::open(path.as_ref()).expect("create large fixture database");
        conn.execute(
            "CREATE TABLE archive_rows(
                id INTEGER PRIMARY KEY,
                payload TEXT NOT NULL
            )",
        )
        .expect("create archive_rows");
        conn.execute("BEGIN")
            .expect("begin large fixture transaction");
        for id in 0..ROW_COUNT {
            let payload = format!("{id:08}:{}", "x".repeat(PAYLOAD_BYTES.saturating_sub(9)));
            conn.execute_with_params(
                "INSERT INTO archive_rows(id, payload) VALUES (?1, ?2)",
                &[SqliteValue::Integer(id), SqliteValue::Text(payload)],
            )
            .expect("insert large fixture row");
        }
        conn.execute("COMMIT")
            .expect("commit large fixture transaction");
    }

    let suffixed_path = |suffix: &str| {
        let mut path = database_path.as_os_str().to_owned();
        path.push(suffix);
        std::path::PathBuf::from(path)
    };
    let durable_artifact_paths = [
        database_path.clone(),
        suffixed_path("-journal"),
        suffixed_path("-wal"),
        suffixed_path("-wal-fec"),
    ];
    let snapshot_durable_artifacts = || -> std::io::Result<Vec<Option<Vec<u8>>>> {
        durable_artifact_paths
            .iter()
            .map(|artifact_path| match std::fs::read(artifact_path) {
                Ok(bytes) => Ok(Some(bytes)),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(error) => Err(std::io::Error::new(
                    error.kind(),
                    format!(
                        "snapshot durable artifact {}: {error}",
                        artifact_path.display()
                    ),
                )),
            })
            .collect()
    };
    let durable_before =
        snapshot_durable_artifacts().expect("snapshot durable artifacts before reopen");
    let size_before = durable_before
        .iter()
        .filter_map(|artifact| artifact.as_ref())
        .map(Vec::len)
        .sum::<usize>();
    assert!(
        size_before
            >= usize::try_from(MINIMUM_DATABASE_BYTES).expect("minimum database size fits usize"),
        "large reopen fixture's durable database/WAL bundle must span at least \
         {MINIMUM_DATABASE_BYTES} bytes, got {size_before}"
    );

    {
        let path = database_path.to_string_lossy();
        let reopened = FrankenConnection::open_existing_schema_only(path.as_ref())
            .expect("reopen large database without schema initialization");
        let count = reopened
            .query("SELECT COUNT(*) FROM archive_rows")
            .expect("count reopened large archive rows");
        assert_eq!(
            count.first().and_then(|row| row.get(0)),
            Some(&SqliteValue::Integer(ROW_COUNT)),
            "existing-only reopen lost archive rows"
        );

        for id in [0, ROW_COUNT / 2, ROW_COUNT - 1] {
            let rows = reopened
                .query_with_params(
                    "SELECT payload FROM archive_rows WHERE id = ?1",
                    &[SqliteValue::Integer(id)],
                )
                .expect("query reopened large archive sentinel");
            assert_eq!(rows.len(), 1, "missing sentinel row {id}");
            let expected = SqliteValue::Text(format!(
                "{id:08}:{}",
                "x".repeat(PAYLOAD_BYTES.saturating_sub(9))
            ));
            assert_eq!(
                rows.first().and_then(|row| row.get(0)),
                Some(&expected),
                "sentinel payload changed for row {id}"
            );
        }
    }

    assert_eq!(
        snapshot_durable_artifacts().expect("snapshot durable artifacts after reopen"),
        durable_before,
        "existing-only large-archive reopen must not replace, truncate, or rewrite \
         the database's durable main/journal/WAL artifact bundle"
    );
}

/// Schema-v1 regression: a closed WAL-retired archive with freelist pages
/// opens cleanly, preserves canonical rows, reclaims freelist pages via
/// `VACUUM`, and reopens intact.
#[test]
fn cass_v15_open_repairs_sparse_wal_reserved_freelist_without_losing_rows() {
    use std::io::{Read, Seek, SeekFrom};
    use std::path::Path;

    const PAGE_SIZE: u64 = 4096;
    const LEAVES_PER_TRUNK: usize = 1022;
    const ROW_QUERY: &str =
        "SELECT m.id, m.conversation_id, m.idx, m.role, m.content, c.title, a.slug
         FROM messages m JOIN conversations c ON c.id = m.conversation_id
         JOIN agents a ON a.id = c.agent_id ORDER BY m.id";

    fn read_freelist(path: &Path) -> Vec<u32> {
        let mut file = std::fs::File::open(path).expect("open durable fixture");
        let mut header = [0_u8; 100];
        file.read_exact(&mut header).expect("read durable header");
        let page_count = u32::from_be_bytes(header[28..32].try_into().expect("page count"));
        let mut trunk = u32::from_be_bytes(header[32..36].try_into().expect("freelist head"));
        let count = u32::from_be_bytes(header[36..40].try_into().expect("freelist count"));
        let mut seen = std::collections::BTreeSet::new();
        while trunk != 0 {
            assert!((2..=page_count).contains(&trunk), "invalid trunk {trunk}");
            assert!(seen.insert(trunk), "duplicate or cyclic trunk {trunk}");
            let mut page = [0_u8; 4096];
            file.seek(SeekFrom::Start(u64::from(trunk - 1) * PAGE_SIZE))
                .expect("seek durable trunk");
            file.read_exact(&mut page).expect("read durable trunk");
            let leaves = u32::from_be_bytes(page[4..8].try_into().expect("leaf count"));
            let leaves = usize::try_from(leaves).expect("leaf count fits usize");
            assert!(leaves <= LEAVES_PER_TRUNK, "invalid trunk leaf count");
            for index in 0..leaves {
                let offset = 8 + index * 4;
                let leaf = u32::from_be_bytes(
                    page[offset..offset + 4]
                        .try_into()
                        .expect("leaf page number"),
                );
                assert!((2..=page_count).contains(&leaf), "invalid leaf {leaf}");
                assert!(seen.insert(leaf), "duplicate free page {leaf}");
            }
            assert!(seen.len() <= usize::try_from(count).expect("count fits usize"));
            trunk = u32::from_be_bytes(page[..4].try_into().expect("next trunk"));
        }
        assert_eq!(
            seen.len(),
            usize::try_from(count).expect("count fits usize")
        );
        seen.into_iter().collect()
    }

    let dir = tempfile::TempDir::new().expect("isolated freelist fixture");
    let database_path = dir.path().join("cass-v1-freelist.db");
    let seed = FrankenStorage::open(&database_path).expect("create actual CASS schema");
    seed.raw()
        .execute_batch(
            "INSERT INTO agents(id, slug, name, kind, created_at, updated_at)
             VALUES (462, 'gh462', 'WAL regression', 'cli', 1, 1);
             INSERT INTO conversations(id, agent_id, title, source_path)
             VALUES (4621, 462, 'first archive conversation', 'fixture/first.jsonl'),
                    (4622, 462, 'second archive conversation', 'fixture/second.jsonl');
             INSERT INTO messages(id, conversation_id, idx, role, content)
             VALUES (46211, 4621, 0, 'user', 'preserve first question'),
                    (46212, 4621, 1, 'assistant', 'preserve first answer'),
                    (46221, 4622, 0, 'user', 'preserve second question');",
        )
        .expect("seed canonical conversations and messages");
    let expected_rows: Vec<_> = seed
        .raw()
        .query(ROW_QUERY)
        .expect("read seeded canonical rows")
        .into_iter()
        .map(|row| row.values().to_vec())
        .collect();
    assert_eq!(expected_rows.len(), 3);

    // Populate and delete temporary rows to materialize real freelist pages.
    seed.raw()
        .execute_batch(
            "CREATE TABLE temp_freelist_bloat(id INTEGER PRIMARY KEY, payload TEXT NOT NULL);",
        )
        .expect("create bloat table");
    let bloat = "freelist-reclaim-page-".repeat(256);
    seed.raw().execute("BEGIN IMMEDIATE").expect("begin bloat");
    for id in 1..=64_i64 {
        seed.raw()
            .execute_with_params(
                "INSERT INTO temp_freelist_bloat(id, payload) VALUES (?1, ?2)",
                &[SqliteValue::Integer(id), SqliteValue::Text(bloat.clone())],
            )
            .expect("insert bloat row");
    }
    seed.raw().execute("COMMIT").expect("commit bloat");
    seed.raw()
        .execute("DROP TABLE temp_freelist_bloat")
        .expect("drop bloat table to populate freelist");

    let checkpoint = seed
        .raw()
        .query("PRAGMA wal_checkpoint(TRUNCATE)")
        .expect("checkpoint seed before retiring WAL");
    assert_eq!(checkpoint.len(), 1);
    assert_eq!(checkpoint[0].get(0), Some(&SqliteValue::Integer(0)));
    let journal = seed
        .raw()
        .query("PRAGMA journal_mode=DELETE")
        .expect("retire the seed WAL before inspecting closed freelist");
    assert_eq!(journal.len(), 1);
    assert_eq!(journal[0].get(0), Some(&SqliteValue::Text("delete".into())));
    seed.close().expect("close seed storage handle");

    assert!(
        !read_freelist(&database_path).is_empty(),
        "fixture must carry freelist pages before VACUUM"
    );

    let storage = FrankenStorage::open(&database_path).expect("reopen schema-v1 archive");
    assert_eq!(storage.schema_version().unwrap(), CURRENT_SCHEMA_VERSION);
    let actual_rows: Vec<_> = storage
        .raw()
        .query(ROW_QUERY)
        .expect("read canonical rows after reopen")
        .into_iter()
        .map(|row| row.values().to_vec())
        .collect();
    assert_eq!(actual_rows, expected_rows);
    storage
        .raw()
        .execute("VACUUM")
        .expect("reclaim freelist pages with VACUUM");
    let freelist_after = storage
        .raw()
        .query("PRAGMA freelist_count")
        .expect("read freelist_count after VACUUM");
    assert_eq!(
        freelist_after[0].get(0),
        Some(&SqliteValue::Integer(0)),
        "VACUUM must reclaim all freelist pages"
    );
    let integrity = storage
        .raw()
        .query("PRAGMA integrity_check")
        .expect("full fixture integrity");
    assert_eq!(integrity.len(), 1);
    assert_eq!(integrity[0].get(0), Some(&SqliteValue::Text("ok".into())));
    assert_eq!(
        storage.raw().query("PRAGMA journal_mode").unwrap()[0].get(0),
        Some(&SqliteValue::Text("wal".into()))
    );
    storage
        .close()
        .expect("checkpoint and close vacuumed CASS archive");
    assert!(
        read_freelist(&database_path).is_empty(),
        "closed database must retain zero freelist pages after VACUUM"
    );

    let reopened = FrankenStorage::open(&database_path).expect("reopen after VACUUM");
    assert_eq!(reopened.schema_version().unwrap(), CURRENT_SCHEMA_VERSION);
    let reopened_rows: Vec<_> = reopened
        .raw()
        .query(ROW_QUERY)
        .expect("read canonical rows on second reopen")
        .into_iter()
        .map(|row| row.values().to_vec())
        .collect();
    assert_eq!(reopened_rows, expected_rows);
    reopened.close().expect("close final handle");
}

/// GH#415/cass#382: bound rowids must seek and retain exact hydration results.
/// Include duplicate, missing, NULL and non-integral values so a fast plan
/// cannot pass by returning a different set of messages.
#[test]
fn sqlite_parameterized_rowid_hydration_seeks_and_preserves_exact_rows() {
    let conn = Connection::open(":memory:").expect("in-memory connection");
    conn.execute("CREATE TABLE messages(id INTEGER PRIMARY KEY, content TEXT NOT NULL)")
        .expect("create hydration fixture");
    // A stored row 2 makes truncating the bound 2.5 a visible wrong result.
    conn.execute(
        "INSERT INTO messages VALUES (2, 'must not match 2.5'), (3, 'three'), (7, 'seven'), (99, 'ninety-nine')",
    )
    .expect("insert hydration rows");
    let params = [
        SqliteValue::Integer(99),
        SqliteValue::Integer(3),
        SqliteValue::Integer(3),
        SqliteValue::Null,
        SqliteValue::Integer(100_000),
        SqliteValue::Text("7".into()),
        SqliteValue::Float(2.5),
    ];
    for key in ["id", "rowid"] {
        let sql = format!(
            "SELECT id, content FROM messages WHERE {key} IN (?1, ?2, ?3, ?4, ?5, ?6, ?7) ORDER BY id"
        );
        let plan = conn
            .query_with_params(&format!("EXPLAIN QUERY PLAN {sql}"), &params)
            .expect("explain parameterized hydration");
        let details = plan
            .iter()
            .map(|row| row.get_typed::<String>(3).expect("query plan detail"))
            .collect::<Vec<_>>()
            .join(" | ");
        assert!(
            details.contains("SEARCH")
                && details.contains("INTEGER PRIMARY KEY")
                && !details.contains("SCAN"),
            "bound {key} hydration must seek instead of scanning: {details}"
        );
        let rows = conn
            .query_with_params(&sql, &params)
            .expect("execute parameterized hydration");
        let actual = rows
            .iter()
            .map(|row| {
                (
                    row.get_typed::<i64>(0).expect("hydrated id"),
                    row.get_typed::<String>(1).expect("hydrated content"),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            actual,
            vec![
                (3, "three".to_owned()),
                (7, "seven".to_owned()),
                (99, "ninety-nine".to_owned()),
            ],
            "bound {key} hydration must preserve exact ordered, unique rows"
        );
        let missing = vec![SqliteValue::Null; params.len()];
        assert!(
            conn.query_with_params(&sql, &missing)
                .expect("hydrate all-NULL rowids")
                .is_empty(),
            "a new execution must not reuse the previous bound rowid set"
        );
    }
}

/// bd-j9gub (owner fork `e8901664`): the per-conversation viewer DELETEs must
/// SEARCH a leading-column index instead of scanning the table. On the 0.4.12
/// fork these DELETEs planned as full table scans — the DML collect gate only
/// sought a single-column index or a FULL composite key, so an equality PREFIX
/// of a composite key (the `(conversation_id, ...)` viewer keys) fell back to
/// Rewind/Next, making the per-conversation republish pass O(N) per
/// conversation, O(N^2) over the archive (measured at ~62% of the scan+persist
/// stage at 300k rows; .scratch/glm/2026-11-07-index-speed). The fork (rev
/// `e8901664`, still on the 0.4.12 base: `f39c8eb0`+`e8901664`, backported
/// onto `d9414f97` from the 0.4.13 series) seeks the longest pinned index
/// prefix and renders the seek in `EXPLAIN QUERY PLAN` for the three
/// republish-path shapes — regions republish, regions meta rederive (prefix
/// plus LIKE residual) and tags republish — which this gate pins against the
/// production viewer schema. The sort orphan-cleanup shape (NOT EXISTS
/// residual) is NOT covered by the fork's seek machinery yet: on both the
/// pre-seek 0.4.12 rev (`d9414f97`) and rev `e8901664` it still plans as a
/// SCAN (a rare maintenance path, not the measured hot cost), and the gate
/// pins that measured plan class so the gap stays visible and any fork
/// change — fix or regression — fails by name. The gate also smoke-executes
/// the region and tag deletes so a seek change cannot pass by deleting wrong
/// rows.
#[test]
fn bd_j9gub_viewer_conversation_dml_seeks_index_prefixes_instead_of_scanning() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let db_path = dir.path().join("viewer-dml-seek.db");
    let storage = FrankenStorage::open(&db_path).expect("create the canonical CASS schema");
    schema::ensure_schema(storage.raw()).expect("install the production viewer schema");
    let conn = storage.raw();

    conn.execute_batch(
        "INSERT INTO agents(id, slug, name, kind, created_at, updated_at)
         VALUES (9901, 'j9gub', 'plan gate', 'cli', 1, 1);
         INSERT INTO conversations(id, agent_id, title, source_path)
         VALUES (99011, 9901, 'first', 'fixture/first.jsonl'),
                (99012, 9901, 'second', 'fixture/second.jsonl');
         INSERT INTO viewer_search_regions(
             conversation_id, region_ordinal, region_key, scope, quill_idx)
         VALUES (99011, 0, 'meta:profile', 'session', 0),
                (99011, 1, 'turn:1', 'turn', 1),
                (99011, 2, 'meta:summary', 'session', 2),
                (99012, 0, 'turn:1', 'turn', 0),
                (99012, 1, 'meta:profile', 'session', 1);
         INSERT INTO viewer_tags(conversation_id, tag)
         VALUES (99011, 'alpha'), (99011, 'beta'), (99012, 'gamma');
         -- No explicit viewer_session_sort seed: the schema's own
         -- conversations-INSERT trigger creates exactly one sort row per
         -- conversation (the production shape), so an explicit INSERT would
         -- violate the conversation_id unique constraint.",
    )
    .expect("seed viewer rows");

    // Every per-conversation viewer DELETE cass issues in production:
    // search_regions.rs (republish + meta rederive), viewer_bridge/writer.rs
    // (tags), sort.rs (orphan cleanup). bd-j9gub (fork rev `e8901664`) makes
    // the first three SEEK the leading-column index. The fourth — the sort
    // orphan-cleanup DELETE at src/viewer_extensions/sort.rs:394 — still plans
    // as a full scan on every family rev measured (`d9414f97` and `e8901664`):
    // the seek machinery treats a NOT EXISTS residual differently from
    // simple residual conjuncts and bails to Rewind/Next. That shape is pinned
    // separately below so the gap is visible, not silent: if the fork later
    // seeks it, the pin fails and the shape graduates into `seek_shapes`.
    let seek_shapes: &[(&str, &str)] = &[
        (
            "regions republish",
            "DELETE FROM viewer_search_regions WHERE conversation_id = ?1",
        ),
        (
            "regions meta rederive",
            "DELETE FROM viewer_search_regions WHERE conversation_id = ?1 \
             AND region_key LIKE 'meta:%'",
        ),
        (
            "tags republish",
            "DELETE FROM viewer_tags WHERE conversation_id = ?1",
        ),
        (
            "sort orphan cleanup",
            "DELETE FROM viewer_session_sort \
             WHERE conversation_id = ?1 \
               AND NOT EXISTS (SELECT 1 FROM conversations c WHERE c.id = ?1)",
        ),
    ];
    let explain = |sql: &str| -> String {
        conn.query_with_params(
            &format!("EXPLAIN QUERY PLAN {sql}"),
            &[SqliteValue::Integer(99011)],
        )
        .unwrap_or_else(|error| panic!("explain: {error}"))
        .iter()
        .map(|row| row.get_typed::<String>(3).expect("query plan detail"))
        .collect::<Vec<_>>()
        .join(" | ")
    };
    for (label, sql) in seek_shapes {
        let details = explain(sql);
        assert!(
            details.contains("SEARCH") && !details.contains("SCAN"),
            "{label}: per-conversation viewer DELETE must SEARCH a leading-column \
             index instead of scanning (bd-j9gub); plan: {details}"
        );
    }

    // The seeks must still delete exactly the targeted conversation's rows:
    // conversation 99011 loses everything, conversation 99012 keeps its
    // non-meta region, tag and sort row.
    conn.execute_with_params(
        "DELETE FROM viewer_search_regions WHERE conversation_id = ?1",
        &[SqliteValue::Integer(99011)],
    )
    .expect("delete the first conversation's regions");
    conn.execute_with_params(
        "DELETE FROM viewer_search_regions WHERE conversation_id = ?1 \
         AND region_key LIKE 'meta:%'",
        &[SqliteValue::Integer(99012)],
    )
    .expect("delete the second conversation's meta regions");
    conn.execute_with_params(
        "DELETE FROM viewer_tags WHERE conversation_id = ?1",
        &[SqliteValue::Integer(99011)],
    )
    .expect("delete the first conversation's tags");
    let count = |sql: &str| -> i64 {
        conn.query(sql)
            .unwrap_or_else(|error| panic!("count after delete: {error}"))
            .first()
            .expect("count row")
            .get_typed(0)
            .expect("count column")
    };
    assert_eq!(
        (
            count("SELECT COUNT(*) FROM viewer_search_regions WHERE conversation_id = 99011"),
            count("SELECT COUNT(*) FROM viewer_search_regions WHERE conversation_id = 99012"),
            count("SELECT COUNT(*) FROM viewer_tags WHERE conversation_id = 99011"),
            count("SELECT COUNT(*) FROM viewer_tags WHERE conversation_id = 99012"),
            count("SELECT COUNT(*) FROM viewer_session_sort WHERE conversation_id = 99012"),
        ),
        (0, 1, 0, 1, 1),
        "the seeked DELETEs must remove exactly the targeted conversation's rows"
    );
}

/// The per-conversation shapes the ingest generation publication issues
/// (`publish_search_regions` / `replace_meta_regions` /
/// `derive_meta_regions` / `generation_markers_visible` /
/// `ingest_receipt_for`) run once per re-projected conversation on the
/// incremental lane, so any shape that SCANs instead of SEEKING a
/// leading-column index is O(N) per conversation — O(N^2) over the
/// archive. bd-j9gub made the per-conversation DELETEs seek (the gate
/// above); a `sample` of the whole-archive republish (run B, 2026-11-10,
/// `.scratch/glm/2026-11-10-batched-region-converge/b-run.sample`) still
/// caught the busy thread split between `publish_search_regions`'s
/// `query_map_collect` and the per-batch converge/publish storm, and
/// EXPLAIN + one-shot timing on a scratch copy of the 6k-session archive
/// (5.4 GB, 1,018,353 region rows; same day, `cass-plan-gate.log`)
/// attributed it to four shapes the fork plans as SCANs: the doc-slot
/// range, the per-scope GROUP BY, the `NOT LIKE` MAX fold and the
/// parent-link JOIN (2.45 s / 2.40 s / 75 ms per call respectively). The
/// production queries were rewritten into the equality-prefix shapes
/// below — every rewritten shape must SEARCH. The original SCANning SQL
/// is kept verbatim in `planner_gap_shapes` as the fork's minimal
/// SELECT-collect-gate reproducers (the same gap class bd-j9gub fixed
/// for DML, still open for SELECT on this fork rev), pinned as SCANs so
/// a fork fix — or regression — fails this gate by name; when one turns
/// into a SEARCH, move it into `seek_shapes` and record the fork rev.
/// The per-scope GROUP BY did exactly that at rev `2d55ddbd` (upstream
/// #531's `c5d04545` grouped-aggregate seek; see the graduated entry in
/// `seek_shapes`), leaving three SCAN pins here.
///
/// `CASS_TEST_REGION_SELECT_PLAN_DB` (optional) points this gate at a
/// scratch COPY of a real archive: the gate then also prints each
/// SELECT shape's plan against real data, the row counts of the tables
/// involved, and a one-shot wall time per shape — the measurement half
/// of the receipt. The probe never asserts, and never executes anything
/// but the SELECT shapes (write shapes are plan-checked on the fixture
/// only).
#[test]
fn republish_lane_per_conversation_selects_seek_leading_column_indexes() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let db_path = dir.path().join("viewer-select-plans.db");
    let storage = FrankenStorage::open(&db_path).expect("create the canonical CASS schema");
    schema::ensure_schema(storage.raw()).expect("install the production viewer schema");
    let conn = storage.raw();

    conn.execute_batch(
        "INSERT INTO agents(id, slug, name, kind, created_at, updated_at)
         VALUES (9903, 'selgate', 'select plan gate', 'cli', 1, 1);
         INSERT INTO conversations(id, agent_id, title, source_path, source_id)
         VALUES (99031, 9903, 'parent', 'fixture/parent.jsonl', 'local'),
                (99032, 9903, 'child', 'fixture/child.jsonl', 'local');
         INSERT INTO messages(id, conversation_id, idx, role, content)
         VALUES (1, 99031, 0, 'user', 'parent question'),
                (2, 99031, 1, 'assistant', 'parent answer'),
                (3, 99032, 0, 'user', 'child question');
         INSERT INTO viewer_search_regions(
             conversation_id, region_ordinal, region_key, scope, quill_idx)
         VALUES (99031, 0, 'meta:title', 'title', 4096),
                (99031, 1, 'turn:1', 'chat', 4097),
                (99032, 0, 'turn:1', 'chat', 4096);
         INSERT INTO viewer_search_region_marks(
             conversation_id, region_version, source_revision, region_count, docs_state)
         VALUES (99031, 6, 1, 2, 'published'), (99032, 6, 1, 1, 'pending');
         INSERT INTO viewer_tags(conversation_id, tag)
         VALUES (99031, 'alpha'), (99031, 'beta'), (99032, 'gamma');
         INSERT INTO viewer_annotations(conversation_id, custom_title, notes)
         VALUES (99031, 'Custom title', 'some notes');
         INSERT INTO viewer_session_facts(
             conversation_id, extractor_version, source_revision, first_prompt,
             parent_conversation_id)
         VALUES (99031, 1, 1, 'first prompt', NULL),
                (99032, 1, 1, 'child prompt', 99031);
         INSERT INTO viewer_session_projection(
             conversation_id, projection_version, extractor_version, source_revision)
         VALUES (99031, 1, 1, 1);
         INSERT INTO viewer_meta(key, value)
         VALUES ('ingest_receipt:99031', '{}');",
    )
    .expect("seed viewer rows");

    // Every per-conversation shape the generation publication issues, in
    // publication order, in the exact SQL the production code sends
    // (copied verbatim from publish_search_regions / replace_meta_regions /
    // derive_meta_regions / generation_markers_visible / ingest_receipt_for).
    // Params bind the parent conversation (99031) unless a shape is
    // child-specific.
    // Each shape carries a `Production:` comment naming its origin
    // (file::function). The pins are verbatim copies, so production drift
    // — a reordered param, an added conjunct — silently stops pinning the
    // real shape while this gate stays green; when editing one of these
    // queries in production, update its pin in the same change.
    // The `viewer_meta` existence probe reads sqlite_master itself — the
    // schema catalog is a few dozen rows with no index to seek, so a SCAN
    // is its correct plan (the whole receipt path measured ~184 µs on the
    // 5.4 GB archive copy). Only pinned for a non-empty plan.
    let catalog_probes: &[(&str, &str, &[SqliteValue])] = &[(
        "receipt viewer_meta probe",
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='viewer_meta' LIMIT 1",
        &[],
    )];
    let seek_shapes: &[(&str, &str, &[SqliteValue])] = &[
        // Production: viewer_extensions/facts.rs `ingest_receipt_for`
        (
            "receipt read",
            "SELECT value FROM viewer_meta WHERE key = ?1 LIMIT 1",
            &[SqliteValue::Text("ingest_receipt:99031".into())],
        ),
        // Production: viewer_extensions/projection.rs `generation_markers_visible`
        (
            "markers facts visible",
            "SELECT 1 FROM viewer_session_facts \
             WHERE conversation_id = ?1 AND source_revision = ?2 LIMIT 1",
            &[SqliteValue::Integer(99031), SqliteValue::Integer(1)],
        ),
        // Production: viewer_extensions/projection.rs `generation_markers_visible`
        (
            "markers projection visible",
            "SELECT 1 FROM viewer_session_projection \
             WHERE conversation_id = ?1 AND source_revision = ?2 LIMIT 1",
            &[SqliteValue::Integer(99031), SqliteValue::Integer(1)],
        ),
        // Production: viewer_extensions/projection.rs `generation_markers_visible`
        (
            "markers marks visible",
            "SELECT 1 FROM viewer_search_region_marks \
             WHERE conversation_id = ?1 AND source_revision = ?2 AND region_version = ?3 LIMIT 1",
            &[
                SqliteValue::Integer(99031),
                SqliteValue::Integer(1),
                SqliteValue::Integer(6),
            ],
        ),
        // Production: viewer_extensions/projection.rs `publish_ingest_generation_in_tx`
        (
            "resolved parent",
            "SELECT parent_conversation_id FROM viewer_session_facts \
             WHERE conversation_id = ?1",
            &[SqliteValue::Integer(99032)],
        ),
        // Production: viewer_extensions/search_regions.rs `publish_search_regions`
        (
            "message id binding",
            "SELECT idx, id FROM messages WHERE conversation_id = ?1",
            &[SqliteValue::Integer(99031)],
        ),
        // Production: viewer_extensions/search_regions.rs
        // `publish_search_regions` (previous_slots). Rewritten by fix 1:
        // the `quill_idx >= REGION_DOC_BASE` range moved to Rust; the
        // planner-gap shape it replaced lives in `planner_gap_shapes`.
        (
            "previous doc slots",
            "SELECT quill_idx FROM viewer_search_regions WHERE conversation_id = ?1",
            &[SqliteValue::Integer(99031)],
        ),
        // Production: viewer_extensions/search_regions.rs
        // `old_region_scope_counts`. Rewritten by fix 1: SQL GROUP BY
        // folded into a Rust run-length count; the planner-gap shape it
        // replaced lives in `planner_gap_shapes`.
        (
            "old scope counts",
            "SELECT scope FROM viewer_search_regions WHERE conversation_id = ?1",
            &[SqliteValue::Integer(99031)],
        ),
        // GRADUATED from planner_gap_shapes at rev
        // `2d55ddbd6f630f4036f53629c9c0f080dc5a1f00` (2026-11-11): upstream
        // #531's grouped-aggregate planner series (`c5d04545`) feeds the
        // group-by sorter from the equality-prefix seek, so `GROUP BY`
        // no longer defeats the covering (conversation_id, scope) index —
        // observed `SEARCH viewer_search_regions USING INDEX
        // sqlite_autoindex_viewer_search_regions_1 (conversation_id=?)`
        // against the pinned rev. The pin
        // now guards the seek: a fork or upstream regression back to a
        // full-table SCAN fails this gate BY NAME. Production keeps the
        // Rust run-length count (`old scope counts` above); this pins the
        // engine class, not a live production query.
        (
            "graduated: GROUP BY over the covering (conversation_id, scope) index seeks",
            "SELECT scope, COUNT(*) FROM viewer_search_regions \
             WHERE conversation_id = ?1 GROUP BY scope",
            &[SqliteValue::Integer(99031)],
        ),
        // Production: viewer_extensions/search_regions.rs
        // `region_doc_identity_parts`
        (
            "canonical doc identity",
            "SELECT source_id, source_path FROM conversations WHERE id = ?1 LIMIT 1",
            &[SqliteValue::Integer(99031)],
        ),
        // Production: viewer_extensions/search_regions.rs
        // `stale_doc_ids_json`
        (
            "marks pending deletes",
            "SELECT pending_doc_deletes FROM viewer_search_region_marks \
             WHERE conversation_id = ?1",
            &[SqliteValue::Integer(99031)],
        ),
        // Production: viewer_extensions/search_regions.rs
        // `derive_meta_regions`
        (
            "meta canonical title",
            "SELECT title FROM conversations WHERE id = ?1",
            &[SqliteValue::Integer(99031)],
        ),
        // Production: viewer_extensions/search_regions.rs
        // `derive_meta_regions`
        (
            "meta annotations",
            "SELECT is_pinned, custom_title, notes FROM viewer_annotations \
             WHERE conversation_id = ?1",
            &[SqliteValue::Integer(99031)],
        ),
        // Production: viewer_extensions/search_regions.rs
        // `derive_meta_regions`
        (
            "meta tags",
            "SELECT tag FROM viewer_tags WHERE conversation_id = ?1 ORDER BY tag",
            &[SqliteValue::Integer(99031)],
        ),
        // Production: viewer_extensions/search_regions.rs
        // `derive_meta_regions` (child_links). Rewritten by fix 1: the
        // parent-link JOIN split into this equality seek plus per-child
        // rowid fetches; the planner-gap shape it replaced lives in
        // `planner_gap_shapes`.
        (
            "meta child facts seek",
            "SELECT conversation_id, first_prompt FROM viewer_session_facts \
             WHERE parent_conversation_id = ?1",
            &[SqliteValue::Integer(99031)],
        ),
        // Production: viewer_extensions/search_regions.rs
        // `derive_meta_regions` (per-child fetch of the split JOIN)
        (
            "meta child conversation fetch",
            "SELECT id, title, external_id FROM conversations WHERE id = ?1",
            &[SqliteValue::Integer(99032)],
        ),
        // Production: viewer_extensions/search_regions.rs `region_mark`
        (
            "mark read",
            "SELECT region_version, source_revision, region_count, docs_state \
             FROM viewer_search_region_marks WHERE conversation_id = ?1",
            &[SqliteValue::Integer(99031)],
        ),
        // Production: viewer_extensions/search_regions.rs
        // `replace_meta_regions` (max message ordinal). Rewritten by fix
        // 1: the `NOT LIKE 'meta:%'` residual moved to Rust
        // (`is_meta_region_key`); the planner-gap shape it replaced lives
        // in `planner_gap_shapes`.
        (
            "max message ordinal",
            "SELECT region_key, region_ordinal FROM viewer_search_regions \
             WHERE conversation_id = ?1",
            &[SqliteValue::Integer(99031)],
        ),
        // Production: viewer_extensions/search_regions.rs
        // `replace_meta_regions` (old_meta — LIKE seeks the index)
        (
            "old meta slots",
            "SELECT quill_idx FROM viewer_search_regions \
             WHERE conversation_id = ?1 AND region_key LIKE 'meta:%'",
            &[SqliteValue::Integer(99031)],
        ),
        // Production: viewer_extensions/search_regions.rs
        // `replace_meta_regions` (the meta-region delete)
        (
            "meta region delete",
            "DELETE FROM viewer_search_regions WHERE conversation_id = ?1 \
             AND region_key LIKE 'meta:%'",
            &[SqliteValue::Integer(99031)],
        ),
        // Production: viewer_bridge/search_sessions.rs
        // `seek_region_keys_by_values` — the strict-search seek lane's
        // tool-predicate value seek (one IN() chunk of the matched
        // vocabulary; SEEK_VALUE_CHUNK=500 wide in production, the pin is
        // the one-value chunk verbatim). Must SEEK idx_viewer_search_regions_tool.
        (
            "strict seek lane tool value seek",
            "SELECT conversation_id, region_key FROM viewer_search_regions \
             WHERE tool_name IN (?1)",
            &[SqliteValue::Text("Bash".into())],
        ),
        // Production: viewer_bridge/search_sessions.rs
        // `seek_region_keys_by_values` — the file-predicate value seek.
        // Must SEEK idx_viewer_search_regions_file.
        (
            "strict seek lane file value seek",
            "SELECT conversation_id, region_key FROM viewer_search_regions \
             WHERE file_path IN (?1)",
            &[SqliteValue::Text("fixture/parent.jsonl".into())],
        ),
        // Production: viewer_bridge/search_sessions.rs
        // `seek_distinct_conversations` — has_mcp's `named` existence
        // seek over the MCP-naming tool vocabulary: the same seeking
        // `(conversation_id, region_key)` projection as the tool/file
        // value seeks above (deduped in Rust), NOT the `SELECT DISTINCT
        // conversation_id` variant — that one plans as a full-table SCAN
        // at the pinned fork rev and is pinned below as a planner gap.
        (
            "strict seek lane mcp named seek",
            "SELECT conversation_id, region_key FROM viewer_search_regions \
             WHERE tool_name IN (?1)",
            &[SqliteValue::Text("mcp__fsquery".into())],
        ),
        (
            "graduated: eq + range over both columns of the covering index",
            "SELECT quill_idx FROM viewer_search_regions \
             WHERE conversation_id = ?1 AND quill_idx >= ?2",
            &[SqliteValue::Integer(99031), SqliteValue::Integer(4096)],
        ),
        (
            "graduated: NOT LIKE residual seeks conversation_id prefix",
            "SELECT COALESCE(MAX(region_ordinal), -1) FROM viewer_search_regions \
             WHERE conversation_id = ?1 AND region_key NOT LIKE 'meta:%'",
            &[SqliteValue::Integer(99031)],
        ),
        (
            "graduated: parent-link JOIN seeks facts and conversations",
            "SELECT c2.id, c2.title, c2.external_id, f2.first_prompt \
             FROM viewer_session_facts f2 \
             JOIN conversations c2 ON c2.id = f2.conversation_id \
             WHERE f2.parent_conversation_id = ?1 ORDER BY c2.id LIMIT 64",
            &[SqliteValue::Integer(99031)],
        ),
        (
            "graduated: DISTINCT over indexed tool_name IN seek",
            "SELECT DISTINCT conversation_id FROM viewer_search_regions \
             WHERE tool_name IN (?1)",
            &[SqliteValue::Text("mcp__fsquery".into())],
        ),
    ];
    // Production: viewer_bridge/search_sessions.rs — the strict seek lane's
    // unindexed value comparisons. `exit_code`, `has_error` and `error_type`
    // carry NO index in `src/viewer_extensions/schema.rs`, so these are
    // narrow scans by schema design.
    let unindexed_scan_shapes: &[(&str, &str, &[SqliteValue])] = &[
        (
            "strict seek lane exit_code equality (no index)",
            "SELECT conversation_id, region_key FROM viewer_search_regions \
             WHERE exit_code = ?1",
            &[SqliteValue::Integer(0)],
        ),
        (
            "strict seek lane has_error equality (no index)",
            "SELECT conversation_id, region_key FROM viewer_search_regions \
             WHERE has_error = ?1",
            &[SqliteValue::Integer(1)],
        ),
        (
            "strict seek lane error_type equality (no index)",
            "SELECT conversation_id, region_key FROM viewer_search_regions \
             WHERE error_type = ?1",
            &[SqliteValue::Text("tool".into())],
        ),
    ];
    // Production: viewer_bridge/search_sessions.rs — parameterless reads split
    // into indexed covering/NULL seeks (`tool_name`, `file_path`) and
    // unindexed NULL-candidate scans (`exit_code`, `error_type`, `has_error`).
    let unparameterized_seek_shapes: &[(&str, &str)] = &[
        (
            "vocab read: distinct tool_name",
            "SELECT DISTINCT tool_name FROM viewer_search_regions \
             WHERE tool_name IS NOT NULL",
        ),
        (
            "vocab read: distinct file_path",
            "SELECT DISTINCT file_path FROM viewer_search_regions \
             WHERE file_path IS NOT NULL",
        ),
        (
            "probe: tool NULL candidates",
            "SELECT DISTINCT conversation_id FROM viewer_search_regions \
             WHERE tool_name IS NULL AND (scope_known = 0 OR scope = 'tools')",
        ),
        (
            "probe: file NULL candidates",
            "SELECT DISTINCT conversation_id FROM viewer_search_regions \
             WHERE file_path IS NULL",
        ),
    ];
    let unparameterized_scan_shapes: &[(&str, &str)] = &[
        (
            "probe: exit NULL candidates (no index)",
            "SELECT DISTINCT conversation_id FROM viewer_search_regions \
             WHERE exit_code IS NULL AND (scope_known = 0 OR scope = 'tools')",
        ),
        (
            "probe: error_type NULL candidates (no index)",
            "SELECT DISTINCT conversation_id FROM viewer_search_regions \
             WHERE error_type IS NULL",
        ),
        (
            "probe: has_error NULL candidates (no index)",
            "SELECT DISTINCT conversation_id FROM viewer_search_regions \
             WHERE has_error IS NULL",
        ),
    ];
    let explain = |sql: &str, params: &[SqliteValue]| -> String {
        conn.query_with_params(&format!("EXPLAIN QUERY PLAN {sql}"), params)
            .unwrap_or_else(|error| panic!("explain: {error}"))
            .iter()
            .map(|row| row.get_typed::<String>(3).expect("query plan detail"))
            .collect::<Vec<_>>()
            .join(" | ")
    };
    for (label, sql, params) in catalog_probes {
        let details = explain(sql, params);
        println!("[catalog-plan] {label}: {details}");
        assert!(
            !details.trim().is_empty(),
            "{label}: EXPLAIN QUERY PLAN returned no rows"
        );
    }
    for (label, sql, params) in seek_shapes {
        let details = explain(sql, params);
        println!("[select-plan] {label}: {details}");
        assert!(
            details.contains("SEARCH") && !details.contains("SCAN"),
            "{label}: the per-conversation publication lane must SEEK a \
             leading-column index, not SCAN — plan: {details}"
        );
    }
    for (label, sql, params) in unindexed_scan_shapes {
        let details = explain(sql, params);
        println!("[unindexed-scan-plan] {label}: {details}");
        assert!(
            details.contains("SCAN"),
            "{label}: unindexed column shape should plan as SCAN — plan: {details}"
        );
    }
    for (label, sql) in unparameterized_seek_shapes {
        let details = explain(sql, &[]);
        println!("[unparameterized-seek-plan] {label}: {details}");
        assert!(
            details.contains("SEARCH") && !details.contains("SCAN"),
            "{label}: indexed vocabulary/NULL shape must SEEK — plan: {details}"
        );
    }
    for (label, sql) in unparameterized_scan_shapes {
        let details = explain(sql, &[]);
        println!("[unparameterized-scan-plan] {label}: {details}");
        assert!(
            details.contains("SCAN"),
            "{label}: unindexed NULL-candidate probe should plan as SCAN — plan: {details}"
        );
    }

    // Optional real-archive measurement against a scratch COPY. Never
    // asserts: the fixture plans above are the gate; this prints plans,
    // table sizes and one-shot timings for the A/B receipt. Only SELECT
    // shapes execute — a write shape (e.g. "meta region delete") is
    // plan-checked on the fixture above and skipped here so the copy's
    // rows never change.
    if let Ok(probe_db) = std::env::var("CASS_TEST_REGION_SELECT_PLAN_DB") {
        // Non-mutating read-only open with snapshot fallback on BusyRecovery
        // so the scratch copy's sidecars are never created or rewritten.
        let probe = Connection::open_schema_only_with_wal_index_recovery(&probe_db)
            .expect("open the scratch archive copy read-only without mutation");
        let counts = [
            "conversations",
            "messages",
            "viewer_search_regions",
            "viewer_search_region_marks",
            "viewer_tags",
            "viewer_session_facts",
        ];
        for table in counts {
            let n: i64 = probe
                .query_with_params(&format!("SELECT COUNT(*) FROM {table}"), &[])
                .expect("count rows")
                .first()
                .expect("count row")
                .get_typed(0)
                .expect("count column");
            println!("[probe-count] {table}: {n}");
        }
        let probe_id: i64 = probe
            .query_with_params(
                "SELECT m.conversation_id FROM viewer_search_region_marks m \
                 WHERE m.region_version = 5 LIMIT 1",
                &[],
            )
            .expect("pick a stale-mark conversation")
            .first()
            .expect("a stale-mark conversation")
            .get_typed(0)
            .expect("conversation id column");
        println!("[probe] stale-mark conversation {probe_id}");
        let all_shapes = seek_shapes
            .iter()
            .map(|(label, sql, params)| (*label, *sql, *params, true))
            .chain(
                unindexed_scan_shapes
                    .iter()
                    .map(|(label, sql, params)| (*label, *sql, *params, false)),
            );
        for (label, sql, params, is_production) in all_shapes {
            let mut bound: Vec<SqliteValue> = params.to_vec();
            if bound.is_empty() {
                // sqlite_master probe: no binding, no timing value.
                continue;
            }
            if let SqliteValue::Text(_) = bound[0] {
                bound[0] = SqliteValue::Text(format!("ingest_receipt:{probe_id}"));
            } else {
                bound[0] = SqliteValue::Integer(probe_id);
            }
            for extra in bound.iter_mut().skip(1) {
                if matches!(extra, SqliteValue::Integer(_)) {
                    *extra = SqliteValue::Integer(1);
                }
            }
            let details = probe
                .query_with_params(&format!("EXPLAIN QUERY PLAN {sql}"), &bound)
                .expect("explain on the copy")
                .iter()
                .map(|row| row.get_typed::<String>(3).expect("query plan detail"))
                .collect::<Vec<_>>()
                .join(" | ");
            if !sql.trim_start().starts_with("SELECT") {
                println!("[probe-shape] {label}: write shape, plan only: {details}");
                continue;
            }
            let start = std::time::Instant::now();
            let rows = probe
                .query_with_params(sql, &bound)
                .expect("execute shape on the copy")
                .len();
            let tag = if is_production {
                "shape"
            } else {
                "unindexed-shape"
            };
            println!(
                "[probe-{tag}] {label}: rows={rows} {:?} plan: {details}",
                start.elapsed()
            );
        }
        for (label, sql) in unparameterized_seek_shapes
            .iter()
            .chain(unparameterized_scan_shapes.iter())
        {
            let details = probe
                .query_with_params(&format!("EXPLAIN QUERY PLAN {sql}"), &[])
                .expect("explain on the copy")
                .iter()
                .map(|row| row.get_typed::<String>(3).expect("query plan detail"))
                .collect::<Vec<_>>()
                .join(" | ");
            let start = std::time::Instant::now();
            let rows = probe
                .query_with_params(sql, &[])
                .expect("execute unparameterized shape on the copy")
                .len();
            println!(
                "[probe-vocab] {label}: rows={rows} {:?} plan: {details}",
                start.elapsed()
            );
        }
    }
}

// ============================================================================
// GATE 1: FTS5 Compatibility
// ============================================================================

#[test]
fn gate1_fts5_create_virtual_table() {
    let conn = Connection::open(":memory:").expect("in-memory connection");
    conn.execute("CREATE VIRTUAL TABLE test_fts USING fts5(content)")
        .expect("GATE 1.1 FAIL: Cannot create FTS5 virtual table");
}

#[test]
fn gate1_fts5_create_with_trigram_tokenizer() {
    let conn = Connection::open(":memory:").expect("in-memory connection");
    // Trigram tokenizer is critical for cass substring search
    conn.execute("CREATE VIRTUAL TABLE test_fts USING fts5(content, tokenize='trigram')")
        .expect("GATE 1.1b FAIL: Cannot create FTS5 table with trigram tokenizer");
}

#[test]
fn gate1_fts5_create_with_porter_tokenizer() {
    let conn = Connection::open(":memory:").expect("in-memory connection");
    conn.execute("CREATE VIRTUAL TABLE test_fts USING fts5(content, tokenize='porter')")
        .expect("GATE 1.1c FAIL: Cannot create FTS5 table with porter tokenizer");
}

#[test]
fn gate1_fts5_insert() {
    let conn = Connection::open(":memory:").expect("in-memory connection");
    conn.execute("CREATE VIRTUAL TABLE test_fts USING fts5(content)")
        .unwrap();

    conn.execute("INSERT INTO test_fts(content) VALUES ('hello world')")
        .expect("GATE 1.2 FAIL: Cannot insert into FTS5 table");
    conn.execute("INSERT INTO test_fts(content) VALUES ('rust programming language')")
        .expect("GATE 1.2 FAIL: Cannot insert second row");
    conn.execute("INSERT INTO test_fts(content) VALUES ('hello rust developers')")
        .expect("GATE 1.2 FAIL: Cannot insert third row");
}

#[test]
fn gate1_fts5_match_query() {
    let conn = Connection::open(":memory:").expect("in-memory connection");
    conn.execute("CREATE VIRTUAL TABLE test_fts USING fts5(content)")
        .unwrap();
    conn.execute("INSERT INTO test_fts(content) VALUES ('hello world')")
        .unwrap();
    conn.execute("INSERT INTO test_fts(content) VALUES ('goodbye world')")
        .unwrap();
    conn.execute("INSERT INTO test_fts(content) VALUES ('hello rust')")
        .unwrap();

    let rows = conn
        .query("SELECT content FROM test_fts WHERE test_fts MATCH 'hello'")
        .expect("GATE 1.3 FAIL: FTS5 MATCH query failed");

    assert_eq!(
        rows.len(),
        2,
        "GATE 1.3 FAIL: Expected 2 matches for 'hello', got {}",
        rows.len()
    );
}

#[test]
fn gate1_fts5_trigram_substring_match() {
    let conn = Connection::open(":memory:").expect("in-memory connection");
    conn.execute("CREATE VIRTUAL TABLE test_fts USING fts5(content, tokenize='trigram')")
        .unwrap();
    conn.execute("INSERT INTO test_fts(content) VALUES ('hello world')")
        .unwrap();
    conn.execute("INSERT INTO test_fts(content) VALUES ('say hello there')")
        .unwrap();
    conn.execute("INSERT INTO test_fts(content) VALUES ('nothing here')")
        .unwrap();

    // Trigram search for substring 'llo' should match rows containing 'hello'
    let rows = conn
        .query("SELECT content FROM test_fts WHERE test_fts MATCH 'llo'")
        .expect("GATE 1.3b FAIL: Trigram substring search failed");

    assert_eq!(
        rows.len(),
        2,
        "GATE 1.3b FAIL: Expected 2 trigram matches for 'llo', got {}",
        rows.len()
    );
}

#[test]
fn gate1_fts5_prefix_match() {
    let conn = Connection::open(":memory:").expect("in-memory connection");
    conn.execute("CREATE VIRTUAL TABLE test_fts USING fts5(content)")
        .unwrap();
    conn.execute("INSERT INTO test_fts(content) VALUES ('authentication error')")
        .unwrap();
    conn.execute("INSERT INTO test_fts(content) VALUES ('authorize user')")
        .unwrap();
    conn.execute("INSERT INTO test_fts(content) VALUES ('something else')")
        .unwrap();

    // Prefix match with *
    let rows = conn
        .query("SELECT content FROM test_fts WHERE test_fts MATCH 'auth*'")
        .expect("GATE 1.4 FAIL: FTS5 prefix matching failed");

    assert_eq!(
        rows.len(),
        2,
        "GATE 1.4 FAIL: Expected 2 prefix matches for 'auth*', got {}",
        rows.len()
    );
}

#[test]
fn gate1_fts5_highlight_function() {
    let conn = Connection::open(":memory:").expect("in-memory connection");
    conn.execute("CREATE VIRTUAL TABLE test_fts USING fts5(content)")
        .unwrap();
    conn.execute("INSERT INTO test_fts(content) VALUES ('hello world')")
        .unwrap();

    let rows = conn
        .query(
            "SELECT highlight(test_fts, 0, '<b>', '</b>') FROM test_fts WHERE test_fts MATCH 'hello'",
        )
        .expect("GATE 1.5 FAIL: FTS5 highlight() function failed");

    assert_eq!(rows.len(), 1, "GATE 1.5 FAIL: Expected 1 highlighted row");
    let val = rows[0].get(0).expect("column 0");
    if let SqliteValue::Text(s) = val {
        assert!(
            s.contains("<b>hello</b>"),
            "GATE 1.5 FAIL: highlight() should wrap 'hello' in <b> tags, got: {s}"
        );
    } else {
        panic!("GATE 1.5 FAIL: highlight() should return text, got: {val:?}");
    }
}

#[test]
fn gate1_fts5_rebuild_command() {
    let conn = Connection::open(":memory:").expect("in-memory connection");
    conn.execute("CREATE VIRTUAL TABLE test_fts USING fts5(content)")
        .unwrap();
    conn.execute("INSERT INTO test_fts(content) VALUES ('test data')")
        .unwrap();

    conn.execute("INSERT INTO test_fts(test_fts) VALUES('rebuild')")
        .expect("GATE 1.6 FAIL: FTS5 rebuild command failed");
}

#[test]
fn gate1_fts5_optimize_command() {
    let conn = Connection::open(":memory:").expect("in-memory connection");
    conn.execute("CREATE VIRTUAL TABLE test_fts USING fts5(content)")
        .unwrap();
    conn.execute("INSERT INTO test_fts(content) VALUES ('optimize me')")
        .unwrap();

    conn.execute("INSERT INTO test_fts(test_fts) VALUES('optimize')")
        .expect("GATE 1.6b FAIL: FTS5 optimize command failed");
}

#[test]
fn gate1_fts5_multi_column() {
    let conn = Connection::open(":memory:").expect("in-memory connection");
    conn.execute("CREATE VIRTUAL TABLE test_fts USING fts5(title, body)")
        .unwrap();
    conn.execute(
        "INSERT INTO test_fts(title, body) VALUES ('Rust Guide', 'Learn systems programming')",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO test_fts(title, body) VALUES ('Python Intro', 'Learn dynamic programming')",
    )
    .unwrap();

    // Search in body column only
    let rows = conn
        .query("SELECT title FROM test_fts WHERE test_fts MATCH 'body:systems'")
        .expect("GATE 1.7 FAIL: Multi-column FTS5 column filter failed");

    assert_eq!(
        rows.len(),
        1,
        "GATE 1.7 FAIL: Expected 1 match for body:systems"
    );
}

#[test]
fn gate1_fts5_bm25_rank_function() {
    let conn = Connection::open(":memory:").expect("in-memory connection");
    conn.execute("CREATE VIRTUAL TABLE test_fts USING fts5(content)")
        .unwrap();
    conn.execute("INSERT INTO test_fts(content) VALUES ('rust rust rust')") // high relevance
        .unwrap();
    conn.execute("INSERT INTO test_fts(content) VALUES ('hello rust')") // low relevance
        .unwrap();

    // cass ranks FTS fallback queries through explicit bm25(table) calls. The
    // SQLite hidden `rank` column is useful compatibility coverage, but it is
    // not part of cass's required query surface.
    let rows = conn
        .query(
            "SELECT content, bm25(test_fts) AS rank \
             FROM test_fts WHERE test_fts MATCH 'rust' ORDER BY rank",
        )
        .expect("GATE 1.8 FAIL: FTS5 bm25 rank function failed");

    assert_eq!(rows.len(), 2, "GATE 1.8 FAIL: Expected 2 ranked results");
    // rank is a negative BM25 score (more negative = better match)
    let rank0 = rows[0].get(1).expect("rank col");
    let rank1 = rows[1].get(1).expect("rank col");
    if let (SqliteValue::Float(r0), SqliteValue::Float(r1)) = (rank0, rank1) {
        assert!(
            r0 <= r1,
            "GATE 1.8 FAIL: rank should be ordered (more negative first), got {r0} vs {r1}"
        );
    } else {
        panic!("GATE 1.8 FAIL: bm25() should return float scores, got {rank0:?} and {rank1:?}");
    }
}

#[test]
fn gate1_fts5_within_transaction() {
    let conn = Connection::open(":memory:").expect("in-memory connection");
    conn.execute("CREATE VIRTUAL TABLE test_fts USING fts5(content)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO test_fts(content) VALUES ('in transaction')")
        .expect("GATE 1.9 FAIL: FTS5 insert within transaction failed");
    conn.execute("COMMIT").unwrap();

    let rows = conn
        .query("SELECT content FROM test_fts WHERE test_fts MATCH 'transaction'")
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "GATE 1.9 FAIL: FTS5 data not visible after commit"
    );
}

#[test]
fn gate1_fts5_transaction_rollback() {
    let conn = Connection::open(":memory:").expect("in-memory connection");
    conn.execute("CREATE VIRTUAL TABLE test_fts USING fts5(content)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO test_fts(content) VALUES ('will be rolled back')")
        .unwrap();
    conn.execute("ROLLBACK").unwrap();

    let rows = conn.query("SELECT COUNT(*) FROM test_fts").unwrap();
    let count = rows[0].get(0).unwrap();
    assert_eq!(
        count,
        &SqliteValue::Integer(0),
        "GATE 1.9b FAIL: FTS5 data visible after rollback"
    );
}

#[test]
fn gate1_fts5_multiple_tables_coexist() {
    let conn = Connection::open(":memory:").expect("in-memory connection");
    conn.execute("CREATE VIRTUAL TABLE fts_a USING fts5(content)")
        .unwrap();
    conn.execute("CREATE VIRTUAL TABLE fts_b USING fts5(content)")
        .unwrap();

    conn.execute("INSERT INTO fts_a(content) VALUES ('alpha search')")
        .unwrap();
    conn.execute("INSERT INTO fts_b(content) VALUES ('beta search')")
        .unwrap();

    let rows_a = conn
        .query("SELECT content FROM fts_a WHERE fts_a MATCH 'alpha'")
        .unwrap();
    let rows_b = conn
        .query("SELECT content FROM fts_b WHERE fts_b MATCH 'beta'")
        .unwrap();

    assert_eq!(
        rows_a.len(),
        1,
        "GATE 1.10 FAIL: Multiple FTS5 tables - first table query failed"
    );
    assert_eq!(
        rows_b.len(),
        1,
        "GATE 1.10 FAIL: Multiple FTS5 tables - second table query failed"
    );
}

#[test]
fn gate1_fts5_bulk_insert_performance() {
    let conn = Connection::open(":memory:").expect("in-memory connection");
    conn.execute("CREATE VIRTUAL TABLE perf_fts USING fts5(content)")
        .unwrap();

    // Insert 1000 rows
    conn.execute("BEGIN").unwrap();
    for i in 0..1000 {
        conn.execute_with_params(
            "INSERT INTO perf_fts(content) VALUES (?1)",
            &[SqliteValue::Text(format!(
                "document number {i} with searchable content about rust programming"
            ))],
        )
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    // Verify count
    let rows = conn.query("SELECT COUNT(*) FROM perf_fts").unwrap();
    assert_eq!(
        rows[0].get(0).unwrap(),
        &SqliteValue::Integer(1000),
        "GATE 1.11 FAIL: Bulk insert count mismatch"
    );

    // Search should work on bulk data
    let results = conn
        .query("SELECT content FROM perf_fts WHERE perf_fts MATCH 'rust' LIMIT 5")
        .expect("GATE 1.11 FAIL: Search on 1000-row FTS5 table failed");

    assert!(
        !results.is_empty(),
        "GATE 1.11 FAIL: No results from 1000-row search"
    );
}

// ============================================================================
// GATE 2: Existing C SQLite Database File Compatibility
// ============================================================================

#[test]
fn gate2_file_compat_create_with_rusqlite_read_with_franken_sync() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let db_path = dir.path().join("test_compat.db");

    // Step 1: Create database with rusqlite (C SQLite)
    {
        let conn = rusqlite::Connection::open(&db_path).expect("rusqlite open for write");
        conn.execute_batch(
            "
            PRAGMA journal_mode=WAL;
            PRAGMA user_version=12;
            CREATE TABLE conversations (
                id INTEGER PRIMARY KEY,
                agent TEXT NOT NULL,
                workspace TEXT,
                created_at INTEGER NOT NULL,
                content TEXT
            );
        ",
        )
        .expect("rusqlite schema creation");

        // Insert test data
        for i in 0..10 {
            conn.execute(
                "INSERT INTO conversations (id, agent, workspace, created_at, content) VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    i,
                    format!("agent_{}", i % 3),
                    format!("/workspace/{}", i),
                    1700000000 + i * 1000,
                    format!("conversation content for session {i}")
                ],
            ).expect("rusqlite insert");
        }
    }

    // Step 2: Open with franken_sync::Connection and verify
    let conn = Connection::open(db_path.to_str().unwrap())
        .expect("GATE 2.1 FAIL: franken_sync cannot open rusqlite-created database");

    // Verify row count
    let rows = conn
        .query("SELECT COUNT(*) FROM conversations")
        .expect("GATE 2.2 FAIL: franken_sync cannot query rusqlite-created table");

    assert_eq!(
        rows[0].get(0).unwrap(),
        &SqliteValue::Integer(10),
        "GATE 2.2 FAIL: Row count mismatch"
    );

    // Verify data integrity
    let rows = conn
        .query("SELECT id, agent, workspace, created_at, content FROM conversations ORDER BY id")
        .expect("GATE 2.3 FAIL: Cannot read all columns");

    assert_eq!(rows.len(), 10, "GATE 2.3 FAIL: Expected 10 rows");

    // Verify first row
    let first = &rows[0];
    assert_eq!(
        first.get(0).unwrap(),
        &SqliteValue::Integer(0),
        "GATE 2.3 FAIL: First row id mismatch"
    );
    assert_eq!(
        first.get(1).unwrap(),
        &SqliteValue::Text("agent_0".into()),
        "GATE 2.3 FAIL: First row agent mismatch"
    );
}

#[test]
fn gate2_file_compat_pragma_user_version() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let db_path = dir.path().join("test_pragma.db");

    // Create with rusqlite, set user_version
    {
        let conn = rusqlite::Connection::open(&db_path).expect("rusqlite open");
        conn.execute_batch("PRAGMA user_version=12;").unwrap();
        conn.execute_batch("CREATE TABLE t(x);").unwrap();
    }

    // Read with franken_sync::Connection
    let conn = Connection::open(db_path.to_str().unwrap())
        .expect("GATE 2.4 FAIL: Cannot open for PRAGMA check");

    let rows = conn
        .query("PRAGMA user_version")
        .expect("GATE 2.4 FAIL: Cannot read PRAGMA user_version");

    let version = rows[0].get(0).expect("version column");
    assert_eq!(
        version,
        &SqliteValue::Integer(12),
        "GATE 2.4 FAIL: PRAGMA user_version should be 12, got {version:?}"
    );
}

#[test]
fn gate2_file_compat_wal_mode() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let db_path = dir.path().join("test_wal.db");

    // Create WAL-mode database with rusqlite
    {
        let conn = rusqlite::Connection::open(&db_path).expect("rusqlite open");
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        conn.execute_batch("CREATE TABLE data(id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        conn.execute("INSERT INTO data VALUES (1, 'wal test')", [])
            .unwrap();
    }

    // Verify WAL files exist
    let wal_path = db_path.with_extension("db-wal");
    let shm_path = db_path.with_extension("db-shm");

    // Open with franken_sync::Connection (should handle WAL mode)
    let conn = Connection::open(db_path.to_str().unwrap())
        .expect("GATE 2.5 FAIL: Cannot open WAL-mode database");

    let rows = conn
        .query("SELECT val FROM data WHERE id = 1")
        .expect("GATE 2.5 FAIL: Cannot query WAL-mode database");

    assert_eq!(rows.len(), 1, "GATE 2.5 FAIL: Expected 1 row from WAL DB");
    assert_eq!(
        rows[0].get(0).unwrap(),
        &SqliteValue::Text("wal test".into()),
        "GATE 2.5 FAIL: WAL data mismatch"
    );

    // Log WAL file presence for diagnostics
    eprintln!(
        "  WAL file exists: {}, SHM file exists: {}",
        wal_path.exists(),
        shm_path.exists()
    );
}

/// GH #509: one stock SQLite read-only query on a healthy CASS archive whose
/// WAL holds no frames leaves `-shm` holding an unindexed "empty" WAL-index
/// header, because a read-only connection cannot delete the index on close.
/// A fresh CASS writer must commit, and keep committing, after that read.
#[test]
fn gh509_cass_writer_commits_after_a_stock_readonly_query() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let db_path = dir.path().join("agent_search.db");
    let wal_path = dir.path().join("agent_search.db-wal");
    let shm_path = dir.path().join("agent_search.db-shm");

    // A healthy CASS archive whose WAL is checkpointed down to its header,
    // the state the reporter's archive was in (`-wal` of 32 bytes).
    let storage = FrankenStorage::open(&db_path).expect("create CASS archive");
    storage
        .raw()
        .execute("INSERT OR REPLACE INTO meta(key, value) VALUES('gh509_seed', '1')")
        .expect("seed write");
    storage
        .raw()
        .query("PRAGMA wal_checkpoint(TRUNCATE)")
        .expect("truncate the WAL");
    storage.close().expect("close with checkpoint");
    let wal_len = std::fs::metadata(&wal_path).map_or(0, |meta| meta.len());
    assert!(
        wal_len <= 32,
        "precondition: the WAL holds no frames (len {wal_len})"
    );

    // The trigger: one stock read-only query.
    {
        let stock = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .expect("stock read-only open");
        let journal: String = stock
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .expect("stock journal mode");
        assert_eq!(journal, "wal", "precondition: the archive is in WAL mode");
        let seeded: i64 = stock
            .query_row(
                "SELECT count(*) FROM meta WHERE key = 'gh509_seed'",
                [],
                |row| row.get(0),
            )
            .expect("stock read");
        assert_eq!(seeded, 1);
    }
    assert!(
        shm_path.exists(),
        "precondition: the stock reader left a WAL index"
    );

    // A fresh CASS writer commits, and keeps committing.
    let storage =
        FrankenStorage::open(&db_path).expect("reopen the CASS archive after the stock read");
    for n in 0..3 {
        let result = storage.raw().execute(&format!(
            "INSERT OR REPLACE INTO meta(key, value) VALUES('gh509_after_{n}', '{n}')"
        ));
        assert!(
            result.is_ok(),
            "commit {n} after a stock read-only query failed: {result:?}"
        );
    }
    storage.close().expect("close");

    // Both engines see every row.
    let stock = rusqlite::Connection::open(&db_path).expect("stock open");
    let rows: i64 = stock
        .query_row(
            "SELECT count(*) FROM meta WHERE key LIKE 'gh509_%'",
            [],
            |row| row.get(0),
        )
        .expect("stock count");
    assert_eq!(rows, 4);
    let franken = Connection::open(db_path.to_str().expect("utf-8 path")).expect("sqlite open");
    let rows = franken
        .query("SELECT count(*) FROM meta WHERE key LIKE 'gh509_%'")
        .expect("sqlite count");
    assert_eq!(
        rows[0].get(0).expect("count column"),
        &SqliteValue::Integer(4)
    );
}

#[test]
fn gate2_file_compat_filtered_query() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let db_path = dir.path().join("test_filter.db");

    // Create with rusqlite
    {
        let conn = rusqlite::Connection::open(&db_path).expect("rusqlite open");
        conn.execute_batch("CREATE TABLE msgs(id INTEGER PRIMARY KEY, agent TEXT, content TEXT);")
            .unwrap();
        conn.execute(
            "INSERT INTO msgs VALUES (1, 'claude', 'hello from claude')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO msgs VALUES (2, 'codex', 'hello from codex')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO msgs VALUES (3, 'claude', 'another claude msg')",
            [],
        )
        .unwrap();
    }

    // Query with franken_sync using parameter binding
    let conn = Connection::open(db_path.to_str().unwrap())
        .expect("GATE 2.6 FAIL: Cannot open for filtered query");

    let rows = conn
        .query_with_params(
            "SELECT id, content FROM msgs WHERE agent = ?1",
            &[SqliteValue::Text("claude".into())],
        )
        .expect("GATE 2.6 FAIL: Parameterized query on rusqlite DB failed");

    assert_eq!(
        rows.len(),
        2,
        "GATE 2.6 FAIL: Expected 2 claude rows, got {}",
        rows.len()
    );
}

#[test]
fn gate2_file_compat_write_back() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let db_path = dir.path().join("test_writeback.db");

    // Create with rusqlite
    {
        let conn = rusqlite::Connection::open(&db_path).expect("rusqlite open");
        conn.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'original')", [])
            .unwrap();
    }

    // Write with franken_sync
    {
        let conn = Connection::open(db_path.to_str().unwrap())
            .expect("GATE 2.7 FAIL: Cannot open for write-back");
        conn.execute_with_params(
            "INSERT INTO t VALUES (?1, ?2)",
            &[
                SqliteValue::Integer(2),
                SqliteValue::Text("from franken_sync".into()),
            ],
        )
        .expect("GATE 2.7 FAIL: Cannot write to rusqlite-created DB");
    }

    // Verify with rusqlite that the write persisted
    {
        let conn = rusqlite::Connection::open(&db_path).expect("rusqlite reopen");
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            count, 2,
            "GATE 2.7 FAIL: Write from franken_sync not visible to rusqlite"
        );
    }
}

#[test]
fn gate2_file_compat_cass_schema_simulation() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let db_path = dir.path().join("test_cass_schema.db");

    // Create a simplified cass-like schema with rusqlite
    {
        let conn = rusqlite::Connection::open(&db_path).expect("rusqlite open");
        conn.execute_batch(
            "
            PRAGMA journal_mode=WAL;
            PRAGMA user_version=12;

            CREATE TABLE conversations (
                id TEXT PRIMARY KEY,
                agent TEXT NOT NULL,
                workspace TEXT,
                project_dir TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER,
                model TEXT,
                title TEXT,
                message_count INTEGER DEFAULT 0,
                source_id TEXT DEFAULT 'local'
            );

            CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_id TEXT NOT NULL REFERENCES conversations(id),
                role TEXT NOT NULL,
                content TEXT,
                timestamp INTEGER,
                token_count INTEGER DEFAULT 0
            );

            CREATE INDEX idx_conv_agent ON conversations(agent);
            CREATE INDEX idx_conv_created ON conversations(created_at);
            CREATE INDEX idx_msg_conv ON messages(conversation_id);
        ",
        )
        .expect("cass schema creation");

        // Insert realistic test data
        conn.execute(
            "INSERT INTO conversations VALUES ('sess-001', 'claude_code', '/home/user/project', '/home/user/project', 1700000000, 1700001000, 'claude-3-opus', 'Debug auth flow', 5, 'local')",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO messages VALUES (1, 'sess-001', 'user', 'Why is my auth failing?', 1700000000, 42)",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO messages VALUES (2, 'sess-001', 'assistant', 'Let me check the auth middleware...', 1700000100, 150)",
            [],
        ).unwrap();
    }

    // Verify full schema compatibility with franken_sync
    let conn = Connection::open(db_path.to_str().unwrap())
        .expect("GATE 2.8 FAIL: Cannot open cass-like schema");

    // Read conversations
    let convs = conn
        .query("SELECT id, agent, workspace, created_at, title FROM conversations")
        .expect("GATE 2.8 FAIL: Cannot read conversations table");
    assert_eq!(convs.len(), 1);
    assert_eq!(
        convs[0].get(1).unwrap(),
        &SqliteValue::Text("claude_code".into())
    );

    // Read messages with join
    let msgs = conn
        .query(
            "SELECT m.role, m.content, c.agent FROM messages m JOIN conversations c ON m.conversation_id = c.id ORDER BY m.id",
        )
        .expect("GATE 2.8 FAIL: JOIN query on cass schema failed");
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0].get(0).unwrap(), &SqliteValue::Text("user".into()));

    // Verify PRAGMA user_version
    let ver = conn.query("PRAGMA user_version").unwrap();
    assert_eq!(
        ver[0].get(0).unwrap(),
        &SqliteValue::Integer(12),
        "GATE 2.8 FAIL: Schema version mismatch"
    );
}

// ============================================================================
// GATE 3: FrankenStorage Migration/Schema Parity
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
struct TableInfoRow {
    cid: i64,
    name: String,
    col_type: String,
    not_null: i64,
    default_value: Option<String>,
    pk: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct IndexListRow {
    name: String,
    is_unique: i64,
    origin: String,
    is_partial: i64,
}

fn quoted_sql_literal(input: &str) -> String {
    let mut out = String::with_capacity(input.len() + 2);
    out.push('\'');
    for c in input.chars() {
        if c == '\'' {
            out.push('\'');
            out.push('\'');
        } else {
            out.push(c);
        }
    }
    out.push('\'');
    out
}

fn schema_tables(conn: &rusqlite::Connection) -> Vec<String> {
    let mut stmt = conn
        .prepare(
            "SELECT name
             FROM sqlite_master
             WHERE type='table'
               AND name NOT LIKE 'sqlite_%'
               AND name NOT LIKE 'fts_messages_%'
             ORDER BY name",
        )
        .expect("prepare table listing");

    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .expect("query table listing");

    rows.collect::<Result<Vec<_>, _>>()
        .expect("collect table listing")
}

fn table_info(conn: &rusqlite::Connection, table_name: &str) -> Vec<TableInfoRow> {
    let mut sql = String::new();
    let _ = write!(
        &mut sql,
        "PRAGMA table_info({})",
        quoted_sql_literal(table_name)
    );

    let mut stmt = conn.prepare(&sql).expect("prepare PRAGMA table_info");
    let rows = stmt
        .query_map([], |row| {
            Ok(TableInfoRow {
                cid: row.get(0)?,
                name: row.get(1)?,
                col_type: row.get(2)?,
                not_null: row.get(3)?,
                default_value: row.get(4)?,
                pk: row.get(5)?,
            })
        })
        .expect("query PRAGMA table_info");

    rows.collect::<Result<Vec<_>, _>>()
        .expect("collect PRAGMA table_info")
}

fn index_list(conn: &rusqlite::Connection, table_name: &str) -> Vec<IndexListRow> {
    let mut sql = String::new();
    let _ = write!(
        &mut sql,
        "PRAGMA index_list({})",
        quoted_sql_literal(table_name)
    );

    let mut stmt = conn.prepare(&sql).expect("prepare PRAGMA index_list");
    let rows = stmt
        .query_map([], |row| {
            Ok(IndexListRow {
                name: row.get(1)?,
                is_unique: row.get(2)?,
                origin: row.get(3)?,
                is_partial: row.get(4)?,
            })
        })
        .expect("query PRAGMA index_list");

    let mut values = rows
        .collect::<Result<Vec<_>, _>>()
        .expect("collect PRAGMA index_list");
    values.sort();
    values
}

#[test]
fn gate3_schema_v1_transition_and_stamp_invariants() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let db_path = dir.path().join("transition_schema_v1.db");

    // Step 1: Build an existing cass DB via SqliteStorage.
    {
        let storage = SqliteStorage::open(&db_path).expect("create cass db");
        assert_eq!(
            storage.schema_version().expect("schema version"),
            CURRENT_SCHEMA_VERSION
        );
    }

    // Schema v1 must not materialize `_schema_migrations`.
    {
        let conn = rusqlite::Connection::open(&db_path).expect("open db with rusqlite");
        let schema_migration_rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type='table' AND name='_schema_migrations'",
                [],
                |row| row.get(0),
            )
            .expect("query _schema_migrations existence");
        assert_eq!(
            schema_migration_rows, 0,
            "schema v1 must not create _schema_migrations"
        );
    }

    // Step 2: Reopen with FrankenStorage (idempotent schema v1).
    {
        let storage = FrankenStorage::open(&db_path).expect("reopen with FrankenStorage");
        assert_eq!(
            storage.schema_version().expect("franken schema version"),
            CURRENT_SCHEMA_VERSION
        );
    }

    // Step 3: Validate schema-v1 stamps, meta.schema_version, and 3-column FTS5.
    let conn = FrankenConnection::open(db_path.to_str().expect("db path str"))
        .expect("reopen with franken_sync");
    let app_id: i64 = conn
        .query_row("PRAGMA application_id")
        .expect("query application_id")
        .get_typed(0)
        .expect("application_id col");
    assert_eq!(app_id, 0x4341_5353);

    let user_version: i64 = conn
        .query_row("PRAGMA user_version")
        .expect("query user_version")
        .get_typed(0)
        .expect("user_version col");
    assert_eq!(user_version, CURRENT_SCHEMA_VERSION);

    let rows = conn
        .query("SELECT value FROM meta WHERE key = 'schema_version'")
        .expect("query meta.schema_version");
    let meta_version: String = rows
        .first()
        .expect("meta row")
        .get_typed(0)
        .expect("meta value col");
    assert_eq!(
        meta_version,
        CURRENT_SCHEMA_VERSION.to_string(),
        "meta.schema_version should stay synchronized at schema v1"
    );

    let fts_cols: Vec<String> = conn
        .query("PRAGMA table_info(fts_messages)")
        .expect("query fts_messages columns")
        .iter()
        .map(|row| row.get_typed::<String>(1).expect("fts col name"))
        .collect();
    assert_eq!(
        fts_cols,
        vec!["content".to_string(), "title".to_string()],
        "schema v1 fts_messages must expose (content, title)"
    );
}

#[test]
fn gate3_schema_parity_reopened_db_matches_fresh_db() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let db_a_path = dir.path().join("db_a_reopened.db");
    let db_b_path = dir.path().join("db_b_fresh_sqlite.db");

    // DB-A: create through SqliteStorage, then reopen through FrankenStorage.
    {
        let storage = SqliteStorage::open(&db_a_path).expect("create db-a with SqliteStorage");
        assert_eq!(
            storage.schema_version().expect("db-a schema version"),
            CURRENT_SCHEMA_VERSION
        );
    }
    {
        let storage = FrankenStorage::open(&db_a_path).expect("reopen db-a with FrankenStorage");
        assert_eq!(
            storage
                .schema_version()
                .expect("db-a franken schema version"),
            CURRENT_SCHEMA_VERSION
        );
    }

    // DB-B: fresh schema-v1 path.
    {
        let storage = FrankenStorage::open(&db_b_path).expect("create db-b with FrankenStorage");
        assert_eq!(
            storage.schema_version().expect("db-b schema version"),
            CURRENT_SCHEMA_VERSION
        );
    }

    let conn_a = rusqlite::Connection::open(&db_a_path).expect("open db-a with rusqlite");
    let conn_b = rusqlite::Connection::open(&db_b_path).expect("open db-b with rusqlite");

    let tables_a = schema_tables(&conn_a);
    let tables_b = schema_tables(&conn_b);
    assert_eq!(
        tables_a, tables_b,
        "table sets differ between reopened and fresh databases"
    );

    for table in &tables_a {
        let cols_a = table_info(&conn_a, table);
        let cols_b = table_info(&conn_b, table);
        assert_eq!(
            cols_a, cols_b,
            "PRAGMA table_info mismatch for table {table}"
        );

        let idx_a = index_list(&conn_a, table);
        let idx_b = index_list(&conn_b, table);
        assert_eq!(idx_a, idx_b, "PRAGMA index_list mismatch for table {table}");
    }
}

// ============================================================================
// Additional Verification: Features cass relies on
// ============================================================================

#[test]
fn verify_count_aggregate() {
    let conn = Connection::open(":memory:").expect("in-memory connection");
    conn.execute("CREATE TABLE t(x INTEGER)").unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    conn.execute("INSERT INTO t VALUES (2)").unwrap();
    conn.execute("INSERT INTO t VALUES (3)").unwrap();

    let rows = conn.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Integer(3));
}

#[test]
fn verify_group_by_and_order_by() {
    let conn = Connection::open(":memory:").expect("in-memory connection");
    conn.execute("CREATE TABLE t(agent TEXT, cnt INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES ('claude', 1)").unwrap();
    conn.execute("INSERT INTO t VALUES ('codex', 1)").unwrap();
    conn.execute("INSERT INTO t VALUES ('claude', 1)").unwrap();

    let rows = conn
        .query("SELECT agent, SUM(cnt) as total FROM t GROUP BY agent ORDER BY total DESC")
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Text("claude".into()));
    assert_eq!(rows[0].get(1).unwrap(), &SqliteValue::Integer(2));
}

#[test]
fn verify_nullable_columns() {
    let conn = Connection::open(":memory:").expect("in-memory connection");
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute_with_params(
        "INSERT INTO t VALUES (?1, ?2)",
        &[SqliteValue::Integer(1), SqliteValue::Null],
    )
    .unwrap();

    let rows = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(
        rows[0].get(0).unwrap(),
        &SqliteValue::Null,
        "NULL column should return Null variant"
    );

    // IS NULL comparison
    let null_rows = conn.query("SELECT id FROM t WHERE val IS NULL").unwrap();
    assert_eq!(null_rows.len(), 1, "IS NULL should find 1 row");
}

#[test]
fn verify_like_operator() {
    let conn = Connection::open(":memory:").expect("in-memory connection");
    conn.execute("CREATE TABLE t(name TEXT)").unwrap();
    conn.execute("INSERT INTO t VALUES ('authentication')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES ('authorization')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES ('other')").unwrap();

    let rows = conn
        .query("SELECT name FROM t WHERE name LIKE 'auth%'")
        .unwrap();
    assert_eq!(rows.len(), 2, "LIKE 'auth%' should match 2 rows");
}

#[test]
fn verify_subquery() {
    let conn = Connection::open(":memory:").expect("in-memory connection");
    conn.execute("CREATE TABLE t(id INTEGER, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    conn.execute("INSERT INTO t VALUES (3, 30)").unwrap();

    let rows = conn
        .query("SELECT id FROM t WHERE val > (SELECT AVG(val) FROM t)")
        .unwrap();
    assert_eq!(rows.len(), 1, "Subquery should find 1 row above average");
    assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Integer(3));
}

#[test]
fn verify_coalesce_and_ifnull() {
    let conn = Connection::open(":memory:").expect("in-memory connection");
    let rows = conn
        .query("SELECT COALESCE(NULL, NULL, 'fallback')")
        .unwrap();
    assert_eq!(
        rows[0].get(0).unwrap(),
        &SqliteValue::Text("fallback".into())
    );

    let rows = conn.query("SELECT IFNULL(NULL, 'default')").unwrap();
    assert_eq!(
        rows[0].get(0).unwrap(),
        &SqliteValue::Text("default".into())
    );
}

#[test]
fn verify_begin_immediate() {
    let conn = Connection::open(":memory:").expect("in-memory connection");
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();

    conn.execute("BEGIN IMMEDIATE").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'immediate')")
        .unwrap();
    conn.execute("COMMIT").unwrap();

    let rows = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(
        rows[0].get(0).unwrap(),
        &SqliteValue::Text("immediate".into()),
        "BEGIN IMMEDIATE transaction should persist data"
    );
}
