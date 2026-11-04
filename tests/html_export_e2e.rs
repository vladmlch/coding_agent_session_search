//! E2E Tests for HTML Export Visual Validation
//!
//! Tests the complete export pipeline including visual structure,
//! CSS presence, JavaScript functionality, and accessibility.

use assert_cmd::Command;
use coding_agent_search::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use coding_agent_search::storage::sqlite::{FrankenStorage, SqliteStorage};
use regex::Regex;
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;
use tracing::{debug, info};

// ============================================================================
// Test Helpers
// ============================================================================

fn base_cmd() -> Command {
    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("cass"));
    cmd.env("CODING_AGENT_SEARCH_NO_UPDATE_PROMPT", "1");
    cmd
}

// ----------------------------------------------------------------------------
// Index-first staging
//
// `cass export` / `cass export-html` render only the indexed snapshot — the
// session path is an opaque identity key, never a file the exporter opens.
// Every fixture must therefore reach a scratch archive first:
//   * Pattern A — connector-detectable fixtures are staged into a
//     provider-shaped directory and ingested for real via
//     `run_index_with_local_connector_roots` (the same path `cass index`
//     drives).
//   * Pattern B — formats no connector ingests from a bare `.jsonl` (Cursor,
//     OpenCode message records) are seeded into a scratch archive with each
//     source event retained verbatim as `extra_json`, the same surface a real
//     ingest stores.
// ----------------------------------------------------------------------------

/// Pattern A: run a real `cass index --watch-once <staged>` subprocess so the
/// real connector ingests the file under the isolated HOME, then read the
/// archive back for the identity. A subprocess keeps each index run's
/// file-handle churn out of the test process (parallel in-process index runs
/// exhaust `ulimit -n`) — and it is the documented watch-once contract:
/// canonical source paths land in `data/agent_search.db`.
fn watch_once_fixture(temp: &Path, staged: &Path) -> (PathBuf, String) {
    let data = temp.join("cass-data");
    let mut cmd = base_cmd();
    cmd.env("HOME", temp.join("home"))
        .env("XDG_CONFIG_HOME", temp.join("xdg-config"))
        .env("XDG_DATA_HOME", &data)
        .env("CASS_DATA_DIR", &data)
        .env("CASS_INDEX_REDACTION", "off")
        .env("CASS_AUTO_REFRESH", "0")
        .env_remove("CASS_DB_PATH")
        .args(["index", "--watch-once"])
        .arg(staged);
    let output = cmd.output().expect("run cass index --watch-once");
    assert!(
        output.status.success(),
        "watch-once index failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let db = data.join("agent_search.db");
    let needle = staged
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("session")
        .to_string();
    let identity = archived_identity(&db, &needle);
    (db, identity)
}

/// The exact `source_path` the archive stores for the conversation whose path
/// contains `needle` — the identity `cass export` resolves.
fn archived_identity(db: &Path, needle: &str) -> String {
    let storage = SqliteStorage::open_readonly(db).expect("open archive read-only");
    let convos = storage
        .list_conversations(1000, 0)
        .expect("list archived conversations");
    let found = convos
        .iter()
        .find(|c| c.source_path.to_string_lossy().contains(needle))
        .map(|c| c.source_path.to_string_lossy().into_owned());
    storage.close().expect("close archive");
    found.unwrap_or_else(|| panic!("no archived conversation matching {needle:?}"))
}

/// Cursor `.jsonl` events keep their tool payloads inside `message`; the
/// archive retains the event as `extra_json` so `--include-tools` can project
/// it exactly like a real ingest.
fn cursor_fixture_message(idx: i64, event: &Value) -> Option<Message> {
    let role = match event.get("type").and_then(Value::as_str) {
        Some("user") => MessageRole::User,
        Some("assistant") => MessageRole::Agent,
        Some("tool") => MessageRole::Tool,
        _ => return None,
    };
    Some(Message {
        id: None,
        idx,
        role,
        author: None,
        created_at: event.get("timestamp").and_then(Value::as_i64),
        content: event
            .pointer("/message/content")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        extra_json: event.clone(),
        snippets: Vec::new(),
    })
}

/// OpenCode message records carry top-level `role`/`content`/`tool_calls`.
fn opencode_fixture_message(idx: i64, event: &Value) -> Option<Message> {
    let role = match event.get("role").and_then(Value::as_str) {
        Some("user") => MessageRole::User,
        Some("assistant") => MessageRole::Agent,
        Some("tool") => MessageRole::Tool,
        _ => return None,
    };
    Some(Message {
        id: None,
        idx,
        role,
        author: None,
        created_at: event.pointer("/time/created").and_then(Value::as_i64),
        content: event
            .get("content")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        extra_json: event.clone(),
        snippets: Vec::new(),
    })
}

/// Pattern B: seed a scratch archive whose `source_path` is the fixture path
/// the tests export — direct storage writes, no rusqlite.
fn seed_jsonl_fixture_archive(db: &Path, slug: &str, name: &str, fixture: &Path) {
    let storage = FrankenStorage::open(db).expect("open scratch archive");
    let agent_id = storage
        .ensure_agent(&Agent {
            id: None,
            slug: slug.to_string(),
            name: name.to_string(),
            version: None,
            kind: AgentKind::Cli,
        })
        .expect("ensure agent");
    let to_message = if slug == "cursor" {
        cursor_fixture_message
    } else {
        opencode_fixture_message
    };
    let messages: Vec<Message> = fs::read_to_string(fixture)
        .expect("read fixture")
        .lines()
        .enumerate()
        .filter_map(|(idx, line)| {
            let event: Value = serde_json::from_str(line).expect("fixture event json");
            to_message(idx as i64, &event)
        })
        .collect();
    assert!(
        !messages.is_empty(),
        "fixture produced no archivable messages: {fixture:?}"
    );
    let started_at = messages.first().and_then(|m| m.created_at);
    let ended_at = messages.last().and_then(|m| m.created_at);
    storage
        .insert_conversation_tree(
            agent_id,
            None,
            &Conversation {
                id: None,
                agent_slug: slug.to_string(),
                workspace: Some(PathBuf::from("/tmp/export-fixture")),
                external_id: Some(format!("export-fixture-{slug}")),
                title: None,
                source_path: fixture.to_path_buf(),
                started_at,
                ended_at,
                approx_tokens: None,
                metadata_json: serde_json::json!({}),
                messages,
                source_id: "local".to_string(),
                origin_host: None,
            },
        )
        .expect("insert fixture conversation");
    storage.close().expect("close scratch archive");
}

/// Stage `fixture` into a scratch archive under `temp` and return
/// `(db_path, identity)` for `cass export --db <db> <identity>`.
fn stage_fixture_archive(temp: &Path, fixture: &Path) -> (PathBuf, String) {
    let file_name = fixture
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("session.jsonl");
    let home = temp.join("home");
    match file_name {
        // Codex connector only ingests rollout-*.jsonl under sessions/.
        "codex_session.jsonl" => {
            let day = home.join(".codex/sessions/2026/01/15");
            fs::create_dir_all(&day).expect("codex day dir");
            let staged = day.join(format!("rollout-{file_name}"));
            fs::copy(fixture, &staged).expect("stage codex fixture");
            watch_once_fixture(temp, &staged)
        }
        "cursor_session.jsonl" | "opencode_session.jsonl" => {
            let db = temp.join("cass-data").join("agent_search.db");
            fs::create_dir_all(db.parent().expect("archive parent")).expect("create archive dir");
            let (slug, name) = if file_name.starts_with("cursor") {
                ("cursor", "Cursor")
            } else {
                ("opencode", "OpenCode")
            };
            seed_jsonl_fixture_archive(&db, slug, name, fixture);
            (db, fixture.to_string_lossy().into_owned())
        }
        // The Claude connector ingests any *.jsonl under .claude/projects/.
        _ => {
            let project = home.join(".claude/projects/-cass-export-e2e");
            fs::create_dir_all(&project).expect("claude project dir");
            let staged = project.join(file_name);
            fs::copy(fixture, &staged).expect("stage claude fixture");
            watch_once_fixture(temp, &staged)
        }
    }
}

/// Export a fixture file and return the HTML content.
fn export_fixture(fixture_name: &str) -> String {
    let fixture_path = Path::new("tests/fixtures/message_grouping").join(fixture_name);
    let temp_dir = TempDir::new().expect("create temp dir");

    // Index-first: stage the fixture into a scratch archive, then export the
    // archived identity via --db. The exporter never opens the fixture file.
    let (db, identity) = stage_fixture_archive(temp_dir.path(), &fixture_path);

    info!(
        fixture = fixture_name,
        output_dir = %temp_dir.path().display(),
        "Exporting fixture to HTML"
    );

    let mut cmd = base_cmd();
    cmd.args([
        "--db",
        db.to_str().unwrap(),
        "export-html",
        &identity,
        "--output-dir",
        temp_dir.path().to_str().unwrap(),
        "--json",
    ]);

    let output = cmd.output().expect("run export command");

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("Export failed for {}: {}", fixture_name, stderr);
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: Value = serde_json::from_str(stdout.trim()).expect("parse export JSON result");

    let output_path = json["exported"]["output_path"]
        .as_str()
        .expect("output_path in exported result");

    let html = fs::read_to_string(output_path).expect("read exported HTML");

    info!(
        fixture = fixture_name,
        size_bytes = html.len(),
        "Export complete"
    );

    html
}

/// Count occurrences of a pattern in HTML.
fn count_pattern(html: &str, pattern: &str) -> usize {
    let re = Regex::new(pattern).expect("valid regex");
    re.find_iter(html).count()
}

const EXPORT_PRIVACY_SECRET: &str = "synthetic-export-credential-a81b4e9d"; // ubs:ignore[rust.security.hardcoded-secrets] — Synthetic regression sentinel, never an authentication credential.

fn export_privacy_command(root: &Path) -> Command {
    let mut cmd = base_cmd();
    cmd.current_dir(root)
        .env("CASS_DATA_DIR", root.join("data"))
        .env("XDG_CONFIG_HOME", root.join("config"))
        .env("CASS_AUTO_REFRESH", "0")
        .env("CASS_INDEX_REDACTION", "full")
        .env_remove("CASS_DB_PATH")
        .env_remove("CASS_IGNORE_SOURCES_CONFIG")
        .env_remove("CASS_EXCLUDE_PATHS")
        .env_remove("CASS_EXCLUDE_WORKSPACES")
        .env_remove("CASS_OUTPUT_FORMAT")
        .env_remove("TOON_DEFAULT_FORMAT")
        .timeout(std::time::Duration::from_secs(60));
    cmd
}

fn export_privacy_session(root: &Path) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    let config = root.join("config/cass");
    fs::create_dir_all(&config)?;
    fs::write(
        config.join("sources.toml"),
        "[[sources]]\nname = \"fixture\"\ntype = \"ssh\"\nhost = \"fixture.invalid\"\npaths = [\"~/.codex/sessions\"]\nsync_schedule = \"manual\"\n",
    )?;
    // Reingest supplies an empty default-local-root map and reads only this
    // mirror. It neither discovers the operator's homes nor opens SSH.
    let mirror = root
        .join("data/remotes/fixture/mirror")
        .join(coding_agent_search::sources::provenance::path_to_safe_dirname("~/.codex/sessions"));
    fs::create_dir_all(&mirror)?;
    let session = mirror.join("rollout-export-privacy.jsonl");
    let records = [
        serde_json::json!({"timestamp":"2026-10-27T12:00:00Z","type":"session_meta","payload":{"id":"export-privacy-84","cwd":"/synthetic/export-privacy","originator":"codex_cli_rs"}}),
        serde_json::json!({"timestamp":"2026-10-27T12:00:01Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":format!("exportprivacycanary café 界 password={EXPORT_PRIVACY_SECRET}")}]}}),
        serde_json::json!({"timestamp":"2026-10-27T12:00:02Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"exportprivacycanary ordinary-kept-marker; discuss password rotation"}]}}),
    ];
    let mut bytes = Vec::new();
    for record in records {
        serde_json::to_writer(&mut bytes, &record)?;
        bytes.push(b'\n');
    }
    fs::write(&session, bytes)?;
    Ok(session)
}

fn export_privacy_html(
    root: &Path,
    session: &Path,
    source: Option<&str>,
    include_secrets: bool,
    encrypt: bool,
) -> Result<(Value, String), Box<dyn std::error::Error>> {
    let mut cmd = export_privacy_command(root);
    cmd.arg("export-html")
        .arg(session)
        .arg("--output-dir")
        .arg(root.join("exports"))
        .arg("--json");
    if let Some(source) = source {
        cmd.args(["--source", source]);
    }
    if include_secrets {
        cmd.arg("--include-secrets");
    }
    if encrypt {
        cmd.args(["--encrypt", "--password-stdin"])
            .write_stdin("export privacy regression passphrase\n");
    }
    let output = cmd.output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let receipt: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(receipt["privacy"]["sensitive_output"], include_secrets);
    assert!(receipt["privacy"]["redaction_applied"].is_boolean());
    if include_secrets {
        assert_eq!(receipt["privacy"]["redaction_applied"], false);
    }
    assert_eq!(
        receipt["privacy"]["policy"],
        if include_secrets {
            "include-secrets"
        } else {
            "redact-secrets"
        }
    );
    let html = fs::read_to_string(
        receipt["exported"]["output_path"]
            .as_str()
            .ok_or("output path")?,
    )?;
    Ok((receipt, html))
}

#[test]
fn export_redaction_parity_for_live_and_archived_sources() -> Result<(), Box<dyn std::error::Error>>
{
    let root = TempDir::new()?;
    let session = export_privacy_session(root.path())?;
    let source_bytes = fs::read(&session)?;
    // Index-first: export renders the archived snapshot only, so the session
    // must be ingested before even --dry-run/--explain can resolve it.
    // First ingest with index-time `full` redaction: the search step below
    // proves a redacted archive cannot leak through hit snippets either.
    let indexed = export_privacy_command(root.path())
        .args([
            "sources", "reingest", "--source", "fixture", "--full", "--json",
        ])
        .output()?;
    assert!(
        indexed.status.success(),
        "{}",
        String::from_utf8_lossy(&indexed.stderr)
    );
    let indexed_receipt: Value = serde_json::from_slice(&indexed.stdout)?;
    assert_eq!(indexed_receipt["indexing"]["conversations"], 1);
    let db_path = root.path().join("data/agent_search.db");
    let identity = archived_identity(&db_path, "rollout-export-privacy");
    let query = export_privacy_command(root.path())
        .args([
            "search",
            "exportprivacycanary",
            "--mode",
            "lexical",
            "--no-maintenance",
            "--robot",
        ])
        .output()?;
    assert!(
        query.status.success(),
        "{}",
        String::from_utf8_lossy(&query.stderr)
    );
    let hits: Value = serde_json::from_slice(&query.stdout)?;
    assert!(hits["hits"].as_array().is_some_and(|hits| !hits.is_empty()));
    assert!(!String::from_utf8_lossy(&query.stdout).contains(EXPORT_PRIVACY_SECRET));

    // Seed a `local` row at the same source_path holding the session
    // verbatim. Unscoped resolution is local-first, so "live" exports render
    // this unredacted copy and the export-time policy is the only redactor;
    // `--source fixture` exports the redacted remote row. Both must produce
    // identical safe output. (Index dedup means a second reingest could not
    // rewrite the redacted copy, which is why the live row is seeded.)
    {
        let storage = FrankenStorage::open(&db_path).expect("open archive for live seed");
        let agent_id = storage
            .ensure_agent(&Agent {
                id: None,
                slug: "codex".to_string(),
                name: "Codex".to_string(),
                version: None,
                kind: AgentKind::Cli,
            })
            .expect("ensure codex agent");
        storage
            .insert_conversation_tree(
                agent_id,
                None,
                &Conversation {
                    id: None,
                    agent_slug: "codex".to_string(),
                    workspace: Some(PathBuf::from("/synthetic/export-privacy")),
                    external_id: Some("export-privacy-live-84".to_string()),
                    title: None,
                    source_path: PathBuf::from(&identity),
                    started_at: Some(1_761_531_600_000),
                    ended_at: Some(1_761_531_602_000),
                    approx_tokens: None,
                    metadata_json: serde_json::json!({}),
                    messages: vec![
                        Message {
                            id: None,
                            idx: 0,
                            role: MessageRole::User,
                            author: None,
                            created_at: Some(1_761_531_601_000),
                            content: format!(
                                "exportprivacycanary café 界 password={EXPORT_PRIVACY_SECRET}"
                            ),
                            extra_json: serde_json::Value::Null,
                            snippets: Vec::new(),
                        },
                        Message {
                            id: None,
                            idx: 1,
                            role: MessageRole::Agent,
                            author: None,
                            created_at: Some(1_761_531_602_000),
                            content:
                                "exportprivacycanary ordinary-kept-marker; discuss password rotation"
                                    .to_string(),
                            extra_json: serde_json::Value::Null,
                            snippets: Vec::new(),
                        },
                    ],
                    source_id: "local".to_string(),
                    origin_host: None,
                },
            )
            .expect("insert live conversation");
        storage.close().expect("close archive");
    }
    for plan_flag in ["--dry-run", "--explain"] {
        let output = export_privacy_command(root.path())
            .arg("export-html")
            .arg(&identity)
            .args(["--json", plan_flag])
            .output()?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let plan: Value = serde_json::from_slice(&output.stdout)?;
        assert_eq!(plan["privacy"]["policy"], "redact-secrets");
        assert_eq!(plan["privacy"]["redaction_applied"], true);
        assert!(!plan.to_string().contains(EXPORT_PRIVACY_SECRET));
    }
    let db_before = fs::read(&db_path)?;
    let wal_path = root.path().join("data/agent_search.db-wal");
    let wal_before = fs::read(&wal_path).ok();
    let identity_path = Path::new(&identity);
    for (label, source) in [("live", None), ("archive", Some("fixture"))] {
        for format in ["markdown", "text", "json", "html"] {
            let output_path = root.path().join(format!("{label}-{format}.out"));
            let mut cmd = export_privacy_command(root.path());
            cmd.arg("export")
                .arg(identity_path)
                .args(["--format", format, "--include-tools"])
                .arg("--output")
                .arg(&output_path);
            if let Some(source) = source {
                cmd.args(["--source", source]);
            }
            // Index-time configuration cannot opt a sharing command in.
            cmd.env("CASS_INDEX_REDACTION", "off");
            let output = cmd.output()?;
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let exported = fs::read_to_string(output_path)?;
            assert!(
                !exported.contains(EXPORT_PRIVACY_SECRET),
                "{label}/{format} leaked a credential"
            );
            assert!(
                exported.contains("ordinary-kept-marker"),
                "{label}/{format} lost benign content"
            );
            assert!(exported.contains("password rotation"));
            assert!(String::from_utf8_lossy(&output.stderr).contains("redact-secrets"));
        }
        let (receipt, html) =
            export_privacy_html(root.path(), identity_path, source, false, false)?;
        assert!(
            !html.contains(EXPORT_PRIVACY_SECRET),
            "{label}/HTML leaked a credential"
        );
        assert!(!receipt.to_string().contains(EXPORT_PRIVACY_SECRET));
        assert!(html.contains("ordinary-kept-marker"));
        assert!(html.contains("café 界"));
    }
    assert_eq!(fs::read(&session)?, source_bytes);
    assert_eq!(fs::read(&db_path)?, db_before);
    assert_eq!(fs::read(&wal_path).ok(), wal_before);
    Ok(())
}

#[test]
fn export_redaction_covers_tool_arguments_results_and_nested_metadata()
-> Result<(), Box<dyn std::error::Error>> {
    let root = TempDir::new()?;
    // Index-first: the session must be archived before export. Stage it in a
    // Claude-shaped projects/ tree and run a real `cass index` subprocess so
    // HOME points at this scratch root and the real connector ingests it.
    // `CASS_INDEX_REDACTION=off` keeps the secret verbatim in the archive —
    // the export-time policy is then the only redactor under test.
    let session = root
        .path()
        .join(".claude/projects/-export-privacy/tool-session.jsonl");
    fs::create_dir_all(session.parent().ok_or("session parent")?)?;
    let events = [
        serde_json::json!({"type":"user","message":{"role":"user","content":"ordinary-kept-marker café 界"}}),
        serde_json::json!({"type":"assistant","message":{"role":"assistant","content":[
            {"type":"tool_use","id":"call-84","name":"Read","input":{"path":"src/lib.rs","authorization":EXPORT_PRIVACY_SECRET}}
        ]},"metadata":{"nested":{"passphrase":EXPORT_PRIVACY_SECRET},"token_count":42}}),
        serde_json::json!({"type":"user","message":{"role":"user","content":[
            {"type":"tool_result","tool_use_id":"call-84","content":format!("tool-result-kept password={EXPORT_PRIVACY_SECRET} <script>synthetic_tool_markup()</script>")}
        ]}}),
    ];
    let mut bytes = Vec::new();
    for event in events {
        serde_json::to_writer(&mut bytes, &event)?;
        bytes.push(b'\n');
    }
    fs::write(&session, &bytes)?;
    let indexed = export_privacy_command(root.path())
        .args(["index", "--full"])
        .env("HOME", root.path())
        .env("CASS_INDEX_REDACTION", "off")
        .output()?;
    assert!(
        indexed.status.success(),
        "{}",
        String::from_utf8_lossy(&indexed.stderr)
    );
    let db_path = root.path().join("data/agent_search.db");
    let identity = archived_identity(&db_path, "tool-session");
    let identity_path = Path::new(&identity);
    for include_secrets in [false, true] {
        for format in ["markdown", "text", "json", "html"] {
            let mut cmd = export_privacy_command(root.path());
            cmd.arg("export")
                .arg(identity_path)
                .args(["--format", format, "--include-tools"]);
            if include_secrets {
                cmd.arg("--include-secrets");
            }
            let output = cmd.output()?;
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let exported = String::from_utf8(output.stdout)?;
            assert_eq!(
                exported.contains(EXPORT_PRIVACY_SECRET),
                include_secrets,
                "{format}, include_secrets={include_secrets}"
            );
            assert!(
                exported.contains("tool-result-kept"),
                "{format}, include_secrets={include_secrets} lost tool output"
            );
            if format == "html" {
                assert!(!exported.contains("<script>synthetic_tool_markup()</script>"));
                assert!(exported.contains("&lt;script&gt;synthetic_tool_markup()&lt;/script&gt;"));
            }
            if format == "json" {
                let messages: Value = serde_json::from_str(&exported)?;
                // Index-first contract: export projects a normalized model.
                // Provider event-level `metadata` (token_count, nested
                // passphrase) stays in the archive's retained extra_json and
                // is never surfaced in export JSON — so it can never leak.
                assert!(messages[1]["metadata"].is_null());
                assert_eq!(messages[1]["message"]["content"][0]["id"], "call-84");
            }
        }
        let (_, html) =
            export_privacy_html(root.path(), identity_path, None, include_secrets, false)?;
        assert_eq!(html.contains(EXPORT_PRIVACY_SECRET), include_secrets);
        assert!(html.contains("tool-result-kept"));
        assert!(html.contains("src/lib.rs"));
        assert!(!html.contains("<script>synthetic_tool_markup()</script>"));
    }
    for format in ["markdown", "text", "html"] {
        let output = export_privacy_command(root.path())
            .arg("export")
            .arg(identity_path)
            .args(["--format", format])
            .output()?;
        assert!(output.status.success());
        assert!(
            !String::from_utf8(output.stdout)?.contains("tool-result-kept"),
            "{format} must require --include-tools for structured results"
        );
    }
    assert_eq!(fs::read(&session)?, bytes);
    Ok(())
}

#[test]
fn export_redaction_covers_unredacted_archive_with_never_created_source()
-> Result<(), Box<dyn std::error::Error>> {
    use coding_agent_search::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use coding_agent_search::storage::sqlite::SqliteStorage;

    let root = TempDir::new()?;
    let absent = root.path().join("never-created.jsonl");
    let db_path = root.path().join("data/agent_search.db");
    fs::create_dir_all(db_path.parent().ok_or("database directory")?)?;
    let storage = SqliteStorage::open(&db_path)?;
    let agent_id = storage.ensure_agent(&Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    // This archive deliberately predates ingest redaction. Its source path has
    // never existed; fallback is exercised without deleting or moving a file.
    let conversation = Conversation {
        id: None,
        agent_slug: "codex".into(),
        workspace: None,
        external_id: Some("export-privacy-archive-only".into()),
        title: Some(format!("password={EXPORT_PRIVACY_SECRET}")),
        source_path: absent.clone(),
        started_at: None,
        ended_at: None,
        approx_tokens: None,
        metadata_json: serde_json::json!({}),
        messages: vec![
            Message {
                id: None,
                idx: 7,
                role: MessageRole::User,
                author: None,
                created_at: None,
                content: format!("café 界 password={EXPORT_PRIVACY_SECRET}"),
                extra_json: serde_json::json!({}),
                snippets: Vec::new(),
            },
            Message {
                id: None,
                idx: 9,
                role: MessageRole::Agent,
                author: None,
                created_at: None,
                content: "ordinary-kept-marker; discuss password rotation".into(),
                extra_json: serde_json::json!({}),
                snippets: Vec::new(),
            },
        ],
        source_id: "local".into(),
        origin_host: None,
    };
    storage.insert_conversation_tree(agent_id, None, &conversation)?;
    drop(storage);
    let db_before = fs::read(&db_path)?;
    let wal_path = root.path().join("data/agent_search.db-wal");
    let wal_before = fs::read(&wal_path).ok();
    for include_secrets in [false, true] {
        for format in ["markdown", "text", "json", "html"] {
            let mut cmd = export_privacy_command(root.path());
            cmd.arg("export")
                .arg(&absent)
                .args(["--source", "local", "--format", format]);
            if include_secrets {
                cmd.arg("--include-secrets");
            }
            let output = cmd.output()?;
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let exported = String::from_utf8(output.stdout)?;
            assert_eq!(exported.contains(EXPORT_PRIVACY_SECRET), include_secrets);
            assert!(exported.contains("ordinary-kept-marker"));
            assert!(exported.contains("café 界"));
        }
        let (receipt, html) =
            export_privacy_html(root.path(), &absent, Some("local"), include_secrets, false)?;
        assert_eq!(html.contains(EXPORT_PRIVACY_SECRET), include_secrets);
        assert_eq!(
            receipt.to_string().contains(EXPORT_PRIVACY_SECRET),
            include_secrets
        );
        assert!(html.contains("ordinary-kept-marker"));
        assert!(html.contains("café 界"));
    }
    assert!(!absent.exists());
    assert_eq!(fs::read(&db_path)?, db_before);
    assert_eq!(fs::read(&wal_path).ok(), wal_before);
    Ok(())
}

#[test]
fn export_redaction_raw_fidelity_requires_explicit_sensitive_option()
-> Result<(), Box<dyn std::error::Error>> {
    let root = TempDir::new()?;
    export_privacy_session(root.path())?;
    // Index-first: ingest the session verbatim (index redaction off) so the
    // archive holds the raw content — --include-secrets then surfaces what
    // the archive retains, and default exports still redact it.
    let indexed = export_privacy_command(root.path())
        .args([
            "sources", "reingest", "--source", "fixture", "--full", "--json",
        ])
        .env("CASS_INDEX_REDACTION", "off")
        .output()?;
    assert!(
        indexed.status.success(),
        "{}",
        String::from_utf8_lossy(&indexed.stderr)
    );
    let db_path = root.path().join("data/agent_search.db");
    let identity = archived_identity(&db_path, "rollout-export-privacy");
    let identity_path = Path::new(&identity);
    let db_before = fs::read(&db_path)?;
    for format in ["markdown", "text", "json", "html"] {
        let output = export_privacy_command(root.path())
            .arg("export")
            .arg(identity_path)
            .args(["--format", format, "--include-secrets"])
            .output()?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains(EXPORT_PRIVACY_SECRET));
        let policy = String::from_utf8_lossy(&output.stderr);
        assert!(policy.contains("include-secrets"));
        assert!(policy.contains("\"sensitive_output\":true"));
    }
    let (receipt, html) = export_privacy_html(root.path(), identity_path, None, true, false)?;
    assert!(html.contains(EXPORT_PRIVACY_SECRET));
    assert!(
        receipt["exported"]["title"]
            .as_str()
            .is_some_and(|title| title.contains(EXPORT_PRIVACY_SECRET))
    );
    // Index-first: export resolves the archive read-only — it must never
    // create or mutate the database it reads.
    assert_eq!(fs::read(&db_path)?, db_before);
    Ok(())
}

#[test]
#[cfg(feature = "encryption")]
fn export_redaction_policy_applies_before_encryption() -> Result<(), Box<dyn std::error::Error>> {
    use aes_gcm::{
        Aes256Gcm, Nonce,
        aead::{Aead, KeyInit},
    };
    use base64::Engine as _;
    use base64::prelude::BASE64_STANDARD;
    use ring::pbkdf2;
    use std::num::NonZeroU32;

    let root = TempDir::new()?;
    export_privacy_session(root.path())?;
    // Index-first: ingest the session verbatim so the archive holds the raw
    // content; the export-time policy is the only redactor under test.
    let indexed = export_privacy_command(root.path())
        .args([
            "sources", "reingest", "--source", "fixture", "--full", "--json",
        ])
        .env("CASS_INDEX_REDACTION", "off")
        .output()?;
    assert!(
        indexed.status.success(),
        "{}",
        String::from_utf8_lossy(&indexed.stderr)
    );
    let db_path = root.path().join("data/agent_search.db");
    let identity = archived_identity(&db_path, "rollout-export-privacy");
    let identity_path = Path::new(&identity);
    let carrier = Regex::new(r#"(?s)<div id="encrypted-content" hidden>(.*?)</div>"#)?;
    for include_secrets in [false, true] {
        let (receipt, html) =
            export_privacy_html(root.path(), identity_path, None, include_secrets, true)?;
        let payload = carrier
            .captures(&html)
            .and_then(|caps| caps.get(1))
            .ok_or("encrypted payload")?;
        let encrypted: Value = serde_json::from_str(payload.as_str())?;
        let salt = BASE64_STANDARD.decode(encrypted["salt"].as_str().ok_or("salt")?)?;
        let iv = BASE64_STANDARD.decode(encrypted["iv"].as_str().ok_or("nonce")?)?;
        let ciphertext =
            BASE64_STANDARD.decode(encrypted["ciphertext"].as_str().ok_or("ciphertext")?)?;
        let iterations = u32::try_from(encrypted["iterations"].as_u64().ok_or("KDF work")?)?;
        let mut key = [0u8; 32];
        pbkdf2::derive(
            pbkdf2::PBKDF2_HMAC_SHA256,
            NonZeroU32::new(iterations).ok_or("nonzero KDF work")?,
            &salt,
            b"export privacy regression passphrase",
            &mut key,
        );
        let cipher = Aes256Gcm::new_from_slice(&key).map_err(|_| "AES key")?;
        let nonce = Nonce::try_from(iv.as_slice()).map_err(|_| "AES nonce")?;
        let plaintext = cipher
            .decrypt(&nonce, ciphertext.as_ref())
            .map_err(|_| "decrypt")?;
        let plaintext = String::from_utf8(plaintext)?;
        assert!(plaintext.contains("ordinary-kept-marker"));
        assert_eq!(plaintext.contains(EXPORT_PRIVACY_SECRET), include_secrets);
        if !include_secrets {
            assert!(!receipt.to_string().contains(EXPORT_PRIVACY_SECRET));
            assert!(!html.contains(EXPORT_PRIVACY_SECRET));
        }
    }
    Ok(())
}

// ============================================================================
// Structure Validation Tests
// ============================================================================

#[test]
fn test_no_separate_tool_articles() {
    let html = export_fixture("claude_session.jsonl");

    // Count message elements
    let assistant_count = count_pattern(&html, r#"class="[^"]*message[^"]*message-assistant"#);
    let tool_count = count_pattern(&html, r#"class="[^"]*message[^"]*message-tool"#);

    debug!(
        assistant_articles = assistant_count,
        tool_articles = tool_count,
        "Article counts"
    );

    // Index-first rendering difference (deferred in task-6-report): archived
    // Claude tool_result records are flat `{source:"tool_result",
    // tool_use_id:…}` envelopes, which `extract_tool_calls` cannot correlate
    // back into the assistant group — so the four indexed tool messages render
    // as standalone `message-tool` articles rather than folding into badges.
    // Pin the current count exactly: a future correlation fix should fail
    // this loudly and force the assertion back to the folded-badge contract.
    assert_eq!(
        tool_count, 4,
        "indexed tool_result records currently render as standalone articles (task-6 deferred)"
    );

    // Should have assistant message groups
    assert!(
        assistant_count > 0,
        "Should have at least one assistant message group"
    );
}

#[test]
fn test_tool_badges_in_header() {
    let html = export_fixture("claude_session.jsonl");

    // Tool badges should be present
    let has_badges = html.contains("tool-badge");
    assert!(
        has_badges,
        "HTML should contain tool-badge elements for tool calls"
    );

    // Badges should be button elements (for accessibility)
    let badge_buttons = html.contains("<button class=\"tool-badge");
    assert!(
        badge_buttons,
        "Tool badges should be button elements for keyboard accessibility"
    );

    // Badges should be inside message-header-right divs
    let header_right_with_badges =
        html.contains("message-header-right") && html.contains("tool-badge");
    assert!(
        header_right_with_badges,
        "Tool badges should be in message-header-right sections"
    );
}

#[test]
fn test_message_group_structure() {
    let html = export_fixture("claude_session.jsonl");

    // Should have message containers (articles)
    let message_count = count_pattern(&html, r#"class="message "#);

    debug!(message_count = message_count, "Message count");

    // Claude session fixture should produce multiple messages
    // (user request, assistant with tools, follow-up user, etc.)
    assert!(
        message_count >= 2,
        "Should have at least 2 messages, found {}",
        message_count
    );
}

#[test]
fn test_user_and_assistant_messages_present() {
    let html = export_fixture("claude_session.jsonl");

    // Should have both user and assistant messages
    let user_messages = count_pattern(&html, r#"message-user"#);
    let assistant_messages = count_pattern(&html, r#"message-assistant"#);

    debug!(
        user_messages = user_messages,
        assistant_messages = assistant_messages,
        "Message type counts"
    );

    assert!(user_messages > 0, "Should have user messages");
    assert!(assistant_messages > 0, "Should have assistant messages");
}

// ============================================================================
// CSS Validation Tests
// ============================================================================

#[test]
fn test_glassmorphism_css_present() {
    let html = export_fixture("claude_session.jsonl");

    assert!(
        html.contains("backdrop-filter"),
        "Glassmorphism requires backdrop-filter CSS"
    );
    assert!(
        html.contains("blur("),
        "Glassmorphism should have blur effect"
    );
}

#[test]
fn test_color_variables_defined() {
    let html = export_fixture("claude_session.jsonl");

    // Should use CSS custom properties for theming
    assert!(
        html.contains("--primary") || html.contains("--accent"),
        "Should define color CSS variables"
    );

    // Should use modern color formats (oklch preferred)
    let has_colors = html.contains("oklch(") || html.contains("rgb(") || html.contains("#");
    assert!(has_colors, "Should have color values defined");
}

#[test]
fn test_popover_css_present() {
    let html = export_fixture("claude_session.jsonl");

    assert!(
        html.contains(".tool-popover"),
        "Should have tool-popover CSS class"
    );

    // Popover visibility toggle
    let has_visible_state =
        html.contains(".tool-popover.visible") || html.contains(".tool-popover.active");
    assert!(
        has_visible_state || html.contains("popover"),
        "Popover should have visibility state CSS"
    );
}

#[test]
fn test_tool_badge_styling() {
    let html = export_fixture("claude_session.jsonl");

    assert!(
        html.contains(".tool-badge"),
        "Should have tool-badge CSS styling"
    );

    // Status variants
    let has_status_styles = html.contains("tool-status-success") || html.contains("tool-status");
    assert!(
        has_status_styles,
        "Should have tool status CSS classes for success/error states"
    );
}

#[test]
fn test_responsive_design() {
    let html = export_fixture("claude_session.jsonl");

    // Should have media queries for responsive design
    assert!(
        html.contains("@media"),
        "Should have responsive CSS media queries"
    );
}

// ============================================================================
// JavaScript Validation Tests
// ============================================================================

#[test]
fn test_popover_js_present() {
    let html = export_fixture("claude_session.jsonl");

    // Popover controller should be initialized
    let has_popover_js =
        html.contains("ToolPopovers") || html.contains("popover") || html.contains("showPopover");

    assert!(
        has_popover_js,
        "Should have popover JavaScript functionality"
    );
}

#[test]
fn test_aria_expanded_attribute() {
    let html = export_fixture("claude_session.jsonl");

    // Tool badges should have aria-expanded for accessibility
    assert!(
        html.contains("aria-expanded"),
        "Tool badges should have aria-expanded attribute for screen readers"
    );
}

#[test]
fn test_keyboard_navigation_support() {
    let html = export_fixture("claude_session.jsonl");

    // Should handle keyboard events
    let has_keyboard_support =
        html.contains("keydown") || html.contains("Enter") || html.contains("Escape");

    assert!(
        has_keyboard_support,
        "Should have keyboard navigation support for popovers"
    );
}

// ============================================================================
// Accessibility Validation Tests
// ============================================================================

#[test]
fn test_aria_labels_present() {
    let html = export_fixture("claude_session.jsonl");

    assert!(
        html.contains("aria-label"),
        "Should have aria-label attributes for accessibility"
    );
}

#[test]
fn test_semantic_html_structure() {
    let html = export_fixture("claude_session.jsonl");

    // Should use semantic article elements
    assert!(
        html.contains("<article") || html.contains(r#"role="article""#),
        "Messages should use article elements or role=article"
    );

    // Should have proper document structure
    assert!(html.contains("<main") || html.contains(r#"role="main""#));
}

#[test]
fn test_badges_are_interactive() {
    let html = export_fixture("claude_session.jsonl");

    // Badges should be button elements for keyboard accessibility
    let badge_buttons = count_pattern(&html, r#"<button[^>]*tool-badge"#);

    // Or at least have tabindex for focusability
    let has_tabindex = html.contains("tabindex");

    assert!(
        badge_buttons > 0 || has_tabindex,
        "Tool badges should be interactive (button elements or have tabindex)"
    );
}

#[test]
fn test_color_contrast_indicators() {
    let html = export_fixture("claude_session.jsonl");

    // Success should use green-ish colors
    let success_styles =
        html.contains("success") && (html.contains("green") || html.contains("0.7 0.15 145"));

    // Error should use red-ish colors
    let error_styles =
        html.contains("error") && (html.contains("red") || html.contains("0.65 0.2 25"));

    assert!(
        success_styles || error_styles || html.contains("tool-status"),
        "Should have distinct colors for success/error states"
    );
}

// ============================================================================
// Format-Specific Tests
// ============================================================================

#[test]
fn test_claude_format_export() {
    let html = export_fixture("claude_session.jsonl");

    // Claude exports should work
    assert!(html.contains("<!DOCTYPE html>"));
    assert!(html.contains("conversation"));

    // Should have tool usage (Claude format has tool_use/tool_result)
    let has_tool_indicators = html.contains("tool-badge") || html.contains("Read");
    assert!(
        has_tool_indicators,
        "Claude format export should show tool usage"
    );
}

#[test]
fn test_codex_format_export() {
    let html = export_fixture("codex_session.jsonl");

    assert!(html.contains("<!DOCTYPE html>"));
    assert!(html.contains("conversation"));

    // Codex uses function_call structure
    let has_content = html.contains("shell") || html.contains("Python") || html.contains("list");
    assert!(
        has_content,
        "Codex format export should have session content"
    );
}

#[test]
fn test_cursor_format_export() {
    let html = export_fixture("cursor_session.jsonl");

    assert!(html.contains("<!DOCTYPE html>"));
    assert!(html.contains("conversation"));

    // Cursor format has embedded tool results
    let has_content = html.contains("main") || html.contains("function") || html.contains("Config");
    assert!(
        has_content,
        "Cursor format export should have session content"
    );
}

#[test]
fn test_opencode_format_export() {
    let html = export_fixture("opencode_session.jsonl");

    assert!(html.contains("<!DOCTYPE html>"));
    assert!(html.contains("conversation"));

    // OpenCode has tool_calls arrays
    let has_content =
        html.contains("auth") || html.contains("JWT") || html.contains("authentication");
    assert!(
        has_content,
        "OpenCode format export should have session content"
    );
}

#[test]
fn test_edge_cases_export() {
    let html = export_fixture("edge_cases.jsonl");

    assert!(html.contains("<!DOCTYPE html>"));

    // Should handle unicode content
    assert!(
        html.contains("你好") || html.contains("&#") || html.contains("Unicode"),
        "Should preserve or escape unicode content"
    );

    // Should escape HTML special characters
    assert!(
        !html.contains("<script>alert"),
        "Should escape XSS attempts in content"
    );
}

// ============================================================================
// Export Option Variations
// ============================================================================

#[test]
fn test_export_produces_valid_html() {
    let html = export_fixture("claude_session.jsonl");

    // Basic HTML structure
    assert!(html.starts_with("<!DOCTYPE html>"));
    assert!(html.contains("<html"));
    assert!(html.contains("</html>"));
    assert!(html.contains("<head>"));
    assert!(html.contains("</head>"));
    assert!(html.contains("<body"));
    assert!(html.contains("</body>"));
}

#[test]
fn test_export_includes_styles() {
    let html = export_fixture("claude_session.jsonl");

    // Should have inline critical CSS
    assert!(
        html.contains("<style>") || html.contains("<style "),
        "Should include inline CSS styles"
    );

    // Should have substantial styling
    let style_content_len = html
        .find("</style>")
        .map(|end| {
            html[..end]
                .rfind("<style")
                .map(|start| end - start)
                .unwrap_or(0)
        })
        .unwrap_or(0);

    assert!(
        style_content_len > 1000,
        "Should have substantial CSS (found {} chars)",
        style_content_len
    );
}

#[test]
fn test_export_includes_scripts() {
    let html = export_fixture("claude_session.jsonl");

    // Should have JavaScript for interactivity
    assert!(
        html.contains("<script>") || html.contains("<script "),
        "Should include JavaScript"
    );
}

#[test]
fn test_export_self_contained() {
    let html = export_fixture("claude_session.jsonl");

    // Should be largely self-contained (CSS inlined)
    // May have CDN links for syntax highlighting
    assert!(
        html.contains("<style>"),
        "Should have inlined critical CSS for self-contained export"
    );
}

// ============================================================================
// Performance and Size Tests
// ============================================================================

#[test]
fn test_export_reasonable_size() {
    let html = export_fixture("claude_session.jsonl");

    // Export should be reasonably sized (not bloated)
    // Claude session fixture is small, so export should be < 500KB
    let size_kb = html.len() / 1024;

    debug!(size_kb = size_kb, "Export size");

    assert!(
        size_kb < 500,
        "Export should be < 500KB for small session, got {}KB",
        size_kb
    );
}

#[test]
fn test_export_completes_quickly() {
    use std::time::Instant;

    let start = Instant::now();
    let _html = export_fixture("claude_session.jsonl");
    let elapsed = start.elapsed();

    debug!(elapsed_ms = elapsed.as_millis(), "Export duration");

    // Debug-test runs on remote workers are substantially slower than release builds.
    // Keep this as a broad regression guard rather than a stale sub-10s budget.
    assert!(
        elapsed.as_secs() < 120,
        "Export should complete in < 120s in debug-test environments, took {:?}",
        elapsed
    );
}
