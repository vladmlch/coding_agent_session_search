//! Real-binary proof for the schema-v1 logical-archive `--allow-compatible-schema`
//! contract: v1 backups restore cleanly and retry read-only under `--if-identical`,
//! while legacy pre-v1 or `_schema_migrations`-bearing backups are refused without
//! mutating the destination even when `--allow-compatible-schema` is passed.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::time::Duration;

use assert_cmd::Command;
use coding_agent_search::franken_sync::Connection;
use coding_agent_search::franken_sync::compat::RowExt;
use coding_agent_search::model::types::{Agent, AgentKind};
use coding_agent_search::storage::sqlite::{CURRENT_SCHEMA_VERSION, SqliteStorage};
use serde_json::Value;

fn command(home: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_cass"));
    command
        .current_dir(home)
        .env("HOME", home)
        .env("XDG_DATA_HOME", home.join("data"))
        .env("XDG_CONFIG_HOME", home.join("config"))
        .env("CASS_DATA_DIR", home.join("unused-default"))
        .env("CASS_IGNORE_SOURCES_CONFIG", "1")
        .env("CASS_AUTO_REFRESH", "0")
        .env_remove("CASS_OUTPUT_FORMAT")
        .env_remove("TOON_DEFAULT_FORMAT")
        .timeout(Duration::from_secs(90));
    command
}

fn json(output: Output) -> Value {
    assert!(
        output.status.success(),
        "archive command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("one JSON receipt")
}

fn database_files(path: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    ["", "-wal", "-shm", "-journal"]
        .into_iter()
        .filter_map(|suffix| {
            let mut name = path.as_os_str().to_os_string();
            name.push(suffix);
            let path = PathBuf::from(name);
            match fs::read(&path) {
                Ok(bytes) => Some((path, bytes)),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => panic!("cannot read test database image: {error}"),
            }
        })
        .collect()
}

fn make_v1_source(root: &Path, name: &str) -> PathBuf {
    assert_eq!(
        CURRENT_SCHEMA_VERSION, 1,
        "schema-v1 logical archive test must be revisited if CURRENT_SCHEMA_VERSION changes"
    );
    let source = root.join(name);
    let storage = SqliteStorage::open(&source).expect("create current canonical fixture");
    storage
        .ensure_agent(&Agent {
            id: None,
            slug: "schema-v1-agent".into(),
            name: "Schema v1 agent".into(),
            version: Some("preserve-me".into()),
            kind: AgentKind::Cli,
        })
        .expect("insert canonical source row");
    storage
        .raw()
        .execute(
            "INSERT INTO forgotten_sources(source_path, size_bytes, mtime_ms, forgotten_at_ms)
             VALUES('/fixture/retained-forget.jsonl', 12, 34, 56)",
        )
        .expect("seed existing source tombstone");
    drop(storage);
    source
}

fn export_source(home: &Path, source: &Path, backup: &Path, archive_id: &str) -> Value {
    json(
        command(home)
            .args(["archive", "export", "--db"])
            .arg(source)
            .args(["--archive-id", archive_id, "--include-private", "--output"])
            .arg(backup)
            .output()
            .expect("run archive export"),
    )
}

#[test]
fn schema_v1_backup_round_trips_with_allow_compatible_schema_and_if_identical() {
    let home = tempfile::tempdir().expect("temp home");
    let source = make_v1_source(home.path(), "source-v1.db");
    let archive_id = "schema-v1-compatible";
    let backup = home.path().join("v1.jsonl");
    let export_receipt = export_source(home.path(), &source, &backup, archive_id);
    assert_eq!(export_receipt["archive_id"], archive_id, "{export_receipt}");

    let destination = home.path().join("restored.db");
    let created = json(
        command(home.path())
            .args(["archive", "import"])
            .arg(&backup)
            .args([
                "--archive-id",
                archive_id,
                "--include-private",
                "--allow-compatible-schema",
                "--output",
            ])
            .arg(&destination)
            .output()
            .expect("run schema-v1 compatible import"),
    );
    assert_eq!(created["destination_status"], "created", "{created}");
    assert!(
        created.get("schema_migration").is_none_or(Value::is_null),
        "clean schema-v1 import must not emit legacy schema_migration: {created}"
    );

    let storage = SqliteStorage::open_readonly(&destination).expect("open restored archive");
    assert_eq!(
        storage.schema_version().expect("read current schema"),
        CURRENT_SCHEMA_VERSION
    );
    let user_version = storage
        .raw()
        .query_row("PRAGMA user_version")
        .expect("read PRAGMA user_version")
        .get_typed::<i64>(0)
        .expect("decode PRAGMA user_version");
    assert_eq!(user_version, CURRENT_SCHEMA_VERSION);
    let legacy_migrations = storage
        .raw()
        .query("PRAGMA table_info(\"_schema_migrations\")")
        .expect("inspect legacy _schema_migrations table");
    assert!(
        legacy_migrations.is_empty(),
        "restored schema-v1 archive must not carry _schema_migrations"
    );

    let agent = storage
        .raw()
        .query_row("SELECT slug, version FROM agents WHERE slug = 'schema-v1-agent'")
        .expect("read restored agent");
    assert_eq!(
        agent.get_typed::<String>(0).expect("agent slug"),
        "schema-v1-agent"
    );
    assert_eq!(
        agent.get_typed::<Option<String>>(1).expect("agent version"),
        Some("preserve-me".into())
    );
    let stamp = storage
        .raw()
        .query_row(
            "SELECT source_path, size_bytes, mtime_ms, forgotten_at_ms FROM forgotten_sources",
        )
        .expect("preserved source tombstone");
    assert_eq!(
        stamp.get_typed::<String>(0).expect("source path"),
        "/fixture/retained-forget.jsonl"
    );
    assert_eq!(stamp.get_typed::<i64>(1).expect("size"), 12);
    assert_eq!(stamp.get_typed::<i64>(2).expect("mtime"), 34);
    assert_eq!(stamp.get_typed::<i64>(3).expect("forgotten time"), 56);
    drop(storage);

    let before = database_files(&destination);
    let repeated = json(
        command(home.path())
            .args(["archive", "import"])
            .arg(&backup)
            .args([
                "--archive-id",
                archive_id,
                "--include-private",
                "--allow-compatible-schema",
                "--if-identical",
                "--output",
            ])
            .arg(&destination)
            .output()
            .expect("run identical retry with --allow-compatible-schema"),
    );
    assert_eq!(repeated["destination_status"], "unchanged", "{repeated}");
    assert!(
        repeated.get("schema_migration").is_none_or(Value::is_null),
        "identical schema-v1 retry must not emit legacy schema_migration: {repeated}"
    );
    assert_eq!(
        before,
        database_files(&destination),
        "identical retry must not mutate the restored database image"
    );
}

#[test]
fn pre_v1_and_legacy_migrations_backups_are_refused_without_mutating_destination() {
    let home = tempfile::tempdir().expect("temp home");
    let valid_source = make_v1_source(home.path(), "valid-v1.db");
    let valid_backup = home.path().join("valid-v1.jsonl");
    export_source(home.path(), &valid_source, &valid_backup, "valid-v1");
    let existing_destination = home.path().join("existing.db");
    let existing_receipt = json(
        command(home.path())
            .args(["archive", "import"])
            .arg(&valid_backup)
            .args(["--archive-id", "valid-v1", "--include-private", "--output"])
            .arg(&existing_destination)
            .output()
            .expect("seed existing destination"),
    );
    assert_eq!(existing_receipt["destination_status"], "created");
    let existing_before = database_files(&existing_destination);

    for (case_name, downgrade_sql) in [
        (
            "legacy-meta-v20",
            "PRAGMA user_version = 0;
             UPDATE meta SET value = '20' WHERE key = 'schema_version';",
        ),
        (
            "legacy-schema-migrations-table",
            "CREATE TABLE _schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL);
             INSERT INTO _schema_migrations VALUES (1, 'legacy');",
        ),
    ] {
        let source = make_v1_source(home.path(), &format!("{case_name}.db"));
        let connection =
            Connection::open(source.to_str().expect("UTF-8 fixture path")).expect("open fixture");
        connection
            .execute_batch(downgrade_sql)
            .expect("apply legacy schema marker");
        connection.close().expect("durably close legacy fixture");

        let archive_id = format!("refused-{case_name}");
        let backup = home.path().join(format!("{case_name}.jsonl"));
        export_source(home.path(), &source, &backup, &archive_id);

        for allow_compatible in [false, true] {
            let fresh_destination = home
                .path()
                .join(format!("refused-{case_name}-compat-{allow_compatible}.db"));
            let mut cmd = command(home.path());
            cmd.args(["archive", "import"]).arg(&backup).args([
                "--archive-id",
                &archive_id,
                "--include-private",
            ]);
            if allow_compatible {
                cmd.arg("--allow-compatible-schema");
            }
            let output = cmd
                .arg("--output")
                .arg(&fresh_destination)
                .output()
                .expect("run refused import");
            assert!(
                !output.status.success(),
                "{case_name} (allow_compatible={allow_compatible}) unexpectedly succeeded"
            );
            assert!(
                output.stdout.is_empty(),
                "{case_name}: failed import must emit no stdout receipt"
            );
            assert!(
                serde_json::from_slice::<Value>(&output.stderr).is_ok(),
                "{case_name}: failed import must emit JSON error on stderr"
            );
            assert!(
                !fresh_destination.exists(),
                "{case_name}: refused import must publish nothing"
            );
        }

        let overwrite_attempt = command(home.path())
            .args(["archive", "import"])
            .arg(&backup)
            .args([
                "--archive-id",
                &archive_id,
                "--include-private",
                "--allow-compatible-schema",
                "--if-identical",
                "--output",
            ])
            .arg(&existing_destination)
            .output()
            .expect("run refused import against existing destination");
        assert!(
            !overwrite_attempt.status.success(),
            "{case_name}: refused import against existing destination unexpectedly succeeded"
        );
        assert_eq!(
            database_files(&existing_destination),
            existing_before,
            "{case_name}: refused import must not mutate existing destination"
        );
    }
}
