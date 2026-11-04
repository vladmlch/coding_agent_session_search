//! E2E tests for `cass sources` CLI commands.
//!
//! Tests the local-only sources subcommands end-to-end:
//! - sources add NAME --path PATH (local sources only)
//! - sources list / sources remove
//! - sources doctor (local checks only; legacy ssh entries warn)
//! - sources mappings / sources agents
//! - ordinary `cass index` over historical remote-mirror archives

use coding_agent_search::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use coding_agent_search::storage::sqlite::FrankenStorage;
use serde_json::Value;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::Stdio;

mod util;
use util::e2e_log::PhaseTracker;

fn tracker_for(test_name: &str) -> PhaseTracker {
    PhaseTracker::new("e2e_sources", test_name)
}

/// Helper: Create a sources.toml config file with given content.
fn create_sources_config(config_dir: &Path, toml_content: &str) {
    let config_file = config_dir.join("cass").join("sources.toml");
    fs::create_dir_all(config_file.parent().unwrap()).unwrap();
    fs::write(&config_file, toml_content).unwrap();
}

/// Helper: Read the sources.toml config file.
fn read_sources_config(config_dir: &Path) -> String {
    let config_file = config_dir.join("cass").join("sources.toml");
    fs::read_to_string(&config_file).unwrap_or_default()
}

fn cass_data_dir(data_root: &Path) -> std::path::PathBuf {
    data_root.join("coding-agent-search")
}

fn seed_archive_conversation(db_path: &Path, agent_slug: &str, marker: &str) {
    let storage = FrankenStorage::open(db_path).unwrap();
    let agent = Agent {
        id: None,
        slug: agent_slug.into(),
        name: agent_slug.into(),
        version: None,
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();
    let conversation = Conversation {
        id: None,
        agent_slug: agent_slug.into(),
        workspace: Some("/tmp/workspace".into()),
        external_id: Some(format!("{agent_slug}-{marker}")),
        title: Some(format!("{agent_slug} {marker}")),
        source_path: format!("/tmp/{agent_slug}-{marker}.jsonl").into(),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_100),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: vec![
            Message {
                id: None,
                idx: 0,
                role: MessageRole::User,
                author: Some("user".into()),
                created_at: Some(1_700_000_000_010),
                content: format!("{agent_slug} {marker} user"),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            },
            Message {
                id: None,
                idx: 1,
                role: MessageRole::Agent,
                author: Some("assistant".into()),
                created_at: Some(1_700_000_000_020),
                content: format!("{agent_slug} {marker} assistant"),
                extra_json: serde_json::Value::Null,
                snippets: Vec::new(),
            },
        ],
        source_id: "local".into(),
        origin_host: None,
    };
    storage
        .insert_conversation_tree(agent_id, None, &conversation)
        .unwrap();
}

// =============================================================================
// sources list tests
// =============================================================================

/// Historical remote mirrors under `<data-dir>/remotes/<name>/mirror/` are
/// indexed by ordinary `cass index` runs as read-only archives. This test
/// seeds such a mirror and asserts the archive indexes and searches under the
/// legacy source's identity.
#[test]
fn legacy_remote_mirror_indexes_and_searches_under_source_id() {
    use coding_agent_search::sources::provenance::path_to_safe_dirname;

    let tracker = tracker_for("legacy_remote_mirror_indexes_and_searches_under_source_id");
    let root = tempfile::tempdir().unwrap().keep();
    let home = root.join("home");
    let config = root.join("config");
    let data = root.join("data");
    fs::create_dir_all(&home).unwrap();
    fs::write(home.join(".env"), "").unwrap();
    let remote_path = "/synthetic/.codex/sessions";
    let mirror = data
        .join("remotes/workstation/mirror")
        .join(path_to_safe_dirname(remote_path));
    fs::create_dir_all(&mirror).unwrap();
    create_sources_config(
        &config,
        r#"
[[sources]]
name = "workstation"
type = "ssh"
host = "operator@workstation.invalid"
paths = ["/synthetic/.codex/sessions"]
sync_schedule = "manual"
"#,
    );
    fs::write(mirror.join("rollout-fleet.jsonl"), concat!(
        "{\"timestamp\":\"2026-09-01T00:00:00Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"fleet-json-contract\",\"cwd\":\"/synthetic/project\",\"cli_version\":\"0.42.0\"}}\n",
        "{\"timestamp\":\"2026-09-01T00:00:01Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"fleetjsoncontractmarker\"}]}}\n"
    )).unwrap();
    let command = || {
        let mut command = tracker.cass_std_command();
        command
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("HOME", &home)
            .env("XDG_CONFIG_HOME", &config)
            .env("XDG_DATA_HOME", root.join("xdg"))
            .env("CASS_DATA_DIR", &data)
            .env("CASS_AUTO_REFRESH", "0")
            .env("RUST_MIN_STACK", "134217728")
            .current_dir(&home);
        tracker.command_environment().apply_to_std(&mut command);
        command
    };

    let ingested = command().args(["index", "--json"]).output().unwrap();
    assert!(
        ingested.status.success(),
        "{}",
        String::from_utf8_lossy(&ingested.stderr)
    );
    let found = command()
        .args([
            "search",
            "fleetjsoncontractmarker",
            "--robot",
            "--mode",
            "lexical",
            "--source",
            "workstation",
            "--no-maintenance",
            "--no-daemon",
        ])
        .output()
        .unwrap();
    assert!(
        found.status.success(),
        "{}",
        String::from_utf8_lossy(&found.stderr)
    );
    let searched: Value = serde_json::from_slice(&found.stdout).unwrap();
    let hits = searched["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 1, "{searched}");
    assert_eq!(hits[0]["source_id"], "workstation");
    tracker.complete();
}

#[test]
fn legacy_mirrors_index_with_source_identity_streaming() {
    assert_legacy_mirrors_index_with_source_identity("1");
}

#[test]
fn legacy_mirrors_index_with_source_identity_batch() {
    assert_legacy_mirrors_index_with_source_identity("0");
}

fn assert_legacy_mirrors_index_with_source_identity(streaming: &str) {
    use coding_agent_search::sources::provenance::path_to_safe_dirname;

    let tracker = tracker_for(&format!(
        "legacy_mirrors_index_with_source_identity_{streaming}"
    ));
    let root = tempfile::tempdir().unwrap().keep();
    let home = root.join("home");
    let config = root.join("config");
    let data = root.join("data");
    fs::create_dir_all(&home).unwrap();
    fs::write(home.join(".env"), "").unwrap();
    // Windows home discovery need not honor HOME. Limit ordinary local
    // discovery to this fixture's provider and set its explicit data root.
    let disabled_agents: Vec<_> = coding_agent_search::connectors::get_connector_factories()
        .into_iter()
        .map(|(name, _)| name)
        .filter(|name| *name != "codex")
        .collect();
    let disabled_agents = toml::to_string(&std::collections::BTreeMap::from([(
        "disabled_agents",
        disabled_agents,
    )]))
    .unwrap();
    create_sources_config(
        &config,
        &format!(
            r#"{disabled_agents}
[[sources]]
name = "alpha"
type = "ssh"
host = "operator@alpha.invalid"
paths = ["/synthetic/.codex/sessions"]
[[sources]]
name = "beta"
type = "ssh"
host = "operator@beta.invalid"
paths = ["~"]
[[sources]]
name = "absent"
type = "ssh"
host = "operator@absent.invalid"
paths = ["/synthetic/.codex/sessions"]
"#
        ),
    );
    let seed = |path: &Path, id: &str| {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let meta = serde_json::json!({"timestamp":"2026-09-01T00:00:00Z", "type":"session_meta", "payload":{"id":id,"cwd":"/synthetic/project","cli_version":"0.42.0"}});
        let message = serde_json::json!({"timestamp":"2026-09-01T00:00:01Z", "type":"response_item", "payload":{"type":"message","role":"user","content":[{"type":"input_text","text":format!("mirrorscopeproof {id}")}]}});
        fs::write(path, format!("{meta}\n{message}\n")).unwrap();
    };
    let alpha = data
        .join("remotes/alpha/mirror")
        .join(path_to_safe_dirname("/synthetic/.codex/sessions"));
    let beta = data
        .join("remotes/beta/mirror")
        .join(path_to_safe_dirname("~"))
        .join(".codex/sessions");
    seed(&alpha.join("rollout-alpha.jsonl"), "alpha-first");
    seed(&beta.join("rollout-beta.jsonl"), "beta-first");
    seed(
        &home.join(".codex/sessions/rollout-local.jsonl"),
        "local-first",
    );
    let command = || {
        let mut command = tracker.cass_std_command();
        command
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("HOME", &home)
            .env("XDG_CONFIG_HOME", &config)
            .env("XDG_DATA_HOME", root.join("xdg"))
            .env("CASS_DATA_DIR", &data)
            .env("CASS_AUTO_REFRESH", "0")
            .env("RUST_MIN_STACK", "134217728")
            .env("CASS_STREAMING_INDEX", streaming)
            .env("CODEX_HOME", home.join(".codex"))
            .current_dir(&home);
        tracker.command_environment().apply_to_std(&mut command);
        command
    };
    let run = |args: &[&str], code| {
        let output = command().args(args).output().unwrap();
        assert_eq!(
            output.status.code(),
            Some(code),
            "{args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        output
    };
    let search_sources = |expected: &[&str]| {
        let output = run(
            &[
                "search",
                "mirrorscopeproof",
                "--robot",
                "--mode",
                "lexical",
                "--no-maintenance",
                "--no-daemon",
            ],
            0,
        );
        let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
        let mut actual: Vec<_> = payload["hits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|hit| hit["source_id"].as_str().unwrap())
            .collect();
        actual.sort_unstable();
        assert_eq!(
            actual, expected,
            "mirror/local history must be searchable under its source id: {payload}"
        );
    };
    // Ordinary `cass index` picks up every seeded mirror under its legacy
    // source id; a configured legacy source without a mirror is skipped, and
    // the locally seeded codex file lands under `local`.
    run(&["index", "--json"], 0);
    search_sources(&["alpha", "beta", "local"]);
    tracker.complete();
}

/// Test: sources list with no configured sources shows appropriate message.
#[test]
fn sources_list_empty() {
    let tracker = tracker_for("sources_list_empty");

    let start = tracker.start("setup", Some("Create temp config directory"));
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&config_dir).unwrap();
    fs::create_dir_all(&data_dir).unwrap();
    tracker.end("setup", Some("Create temp config directory"), start);

    let start = tracker.start("run_sources_list", Some("Run sources list with no config"));
    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "list"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources list command");
    tracker.end(
        "run_sources_list",
        Some("Run sources list with no config"),
        start,
    );

    let start = tracker.start("verify_output", Some("Verify empty sources message"));
    assert!(
        output.status.success(),
        "command failed: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("No sources configured") || stdout.contains("0 sources"),
        "Expected empty sources message, got: {stdout}"
    );
    tracker.end("verify_output", Some("Verify empty sources message"), start);

    tracker.complete();
}

/// Test: sources list with configured sources shows them.
#[test]
fn sources_list_with_sources() {
    let tracker = tracker_for("sources_list_with_sources");

    let start = tracker.start("setup", Some("Create config with one source"));
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();
    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]
sync_schedule = "manual"
"#,
    );
    tracker.end("setup", Some("Create config with one source"), start);

    let start = tracker.start("run_sources_list", Some("Run sources list"));
    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "list"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources list command");
    tracker.end("run_sources_list", Some("Run sources list"), start);

    let start = tracker.start("verify_output", Some("Verify source appears in output"));
    assert!(
        output.status.success(),
        "command failed: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("laptop"),
        "Expected source name in output, got: {stdout}"
    );
    tracker.end(
        "verify_output",
        Some("Verify source appears in output"),
        start,
    );

    tracker.complete();
}

#[test]
fn sources_agents_exclude_and_list_json() -> Result<(), Box<dyn std::error::Error>> {
    let tracker = tracker_for("sources_agents_exclude_and_list_json");

    let tmp = tempfile::TempDir::new()?;
    let config_dir = tmp.path().join("config");
    let data_dir = cass_data_dir(&tmp.path().join("data"));
    fs::create_dir_all(&config_dir)?;

    let start = tracker.start(
        "exclude_agent",
        Some("Exclude openclaw from future indexing runs"),
    );
    let mut command = tracker.cass_assert_command();
    let output = command
        .args([
            "sources",
            "agents",
            "exclude",
            "openclaw",
            "--keep-indexed-data",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .env("CASS_DATA_DIR", &data_dir)
        .output()?;
    tracker.end(
        "exclude_agent",
        Some("Exclude openclaw from future indexing runs"),
        start,
    );
    assert!(
        output.status.success(),
        "exclude failed: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    let config = read_sources_config(&config_dir);
    assert!(
        config.contains("disabled_agents") && config.contains("openclaw"),
        "expected disabled_agents entry in config, got: {config}"
    );

    let start = tracker.start("list_agents_json", Some("List excluded agents in JSON"));
    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "agents", "list", "--json"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .env("CASS_DATA_DIR", &data_dir)
        .output()?;
    tracker.end(
        "list_agents_json",
        Some("List excluded agents in JSON"),
        start,
    );
    assert!(
        output.status.success(),
        "list failed: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let json: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(json["disabled_agents"], serde_json::json!(["openclaw"]));

    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "list", "--json"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .env("CASS_DATA_DIR", &data_dir)
        .output()?;
    assert!(output.status.success(), "sources list --json failed");
    let json: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(json["disabled_agents"], serde_json::json!(["openclaw"]));

    tracker.complete();
    Ok(())
}

#[test]
fn sources_agents_include_removes_existing_exclusion() {
    let tracker = tracker_for("sources_agents_include_removes_existing_exclusion");

    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();
    create_sources_config(&config_dir, "disabled_agents = [\"openclaw\"]\n");

    let start = tracker.start(
        "include_agent",
        Some("Re-enable openclaw for future indexing runs"),
    );
    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "agents", "include", "openclaw"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources agents include command");
    tracker.end(
        "include_agent",
        Some("Re-enable openclaw for future indexing runs"),
        start,
    );
    assert!(
        output.status.success(),
        "include failed: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "agents", "list", "--json"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources agents list command");
    assert!(output.status.success(), "sources agents list failed");
    let json: Value = serde_json::from_slice(&output.stdout).expect("valid json");
    assert_eq!(json["disabled_agents"], serde_json::json!([]));

    tracker.complete();
}

#[test]
fn sources_agents_exclude_purges_local_archive_data_by_default() {
    let tracker = tracker_for("sources_agents_exclude_purges_local_archive_data_by_default");

    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    let data_root = tmp.path().join("data");
    let data_dir = cass_data_dir(&data_root);
    fs::create_dir_all(&config_dir).unwrap();
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("agent_search.db");

    seed_archive_conversation(&db_path, "openclaw", "purge-me");
    seed_archive_conversation(&db_path, "codex", "keep-me");

    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "agents", "exclude", "openclaw"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .env("CASS_DATA_DIR", &data_dir)
        .output()
        .expect("sources agents exclude command");
    assert!(
        output.status.success(),
        "exclude failed: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let storage = FrankenStorage::open(&db_path).unwrap();
    let conversations = storage.list_conversations(10, 0).unwrap();
    assert_eq!(conversations.len(), 1);
    assert_eq!(conversations[0].agent_slug, "codex");

    let mut command = tracker.cass_assert_command();
    let search_output = command
        .args(["search", "purge-me", "--robot", "--limit", "5"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .env("CASS_DATA_DIR", &data_dir)
        .output()
        .expect("search removed agent data");
    assert!(
        search_output.status.success(),
        "search after purge failed: {}\nstdout: {}\nstderr: {}",
        search_output.status,
        String::from_utf8_lossy(&search_output.stdout),
        String::from_utf8_lossy(&search_output.stderr)
    );
    let removed_hits: Value = serde_json::from_slice(&search_output.stdout).expect("valid json");
    let removed_total = removed_hits
        .get("total")
        .or_else(|| removed_hits.get("count"))
        .and_then(|value| value.as_i64())
        .or_else(|| {
            removed_hits
                .get("results")
                .and_then(|value| value.as_array())
                .map(|values| values.len() as i64)
        })
        .or_else(|| {
            removed_hits
                .get("hits")
                .and_then(|value| value.as_array())
                .map(|values| values.len() as i64)
        })
        .unwrap_or(-1);
    assert_eq!(
        removed_total,
        0,
        "expected removed agent search to have no hits: {}",
        String::from_utf8_lossy(&search_output.stdout)
    );

    let mut command = tracker.cass_assert_command();
    let search_output = command
        .args(["search", "keep-me", "--robot", "--limit", "5"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .env("CASS_DATA_DIR", &data_dir)
        .output()
        .expect("search retained agent data");
    assert!(search_output.status.success(), "search kept agent failed");
    let kept_hits: Value = serde_json::from_slice(&search_output.stdout).expect("valid json");
    let kept_total = kept_hits
        .get("total")
        .or_else(|| kept_hits.get("count"))
        .and_then(|value| value.as_i64())
        .or_else(|| {
            kept_hits
                .get("results")
                .and_then(|value| value.as_array())
                .map(|values| values.len() as i64)
        })
        .or_else(|| {
            kept_hits
                .get("hits")
                .and_then(|value| value.as_array())
                .map(|values| values.len() as i64)
        })
        .unwrap_or_default();
    assert!(
        kept_total >= 1,
        "expected retained codex data to remain searchable: {}",
        String::from_utf8_lossy(&search_output.stdout)
    );
}

/// Bead dndyv: an archive that exists but cannot be opened, and a path with
/// no archive at all, each used to print "No already archived data for that
/// agent was present" — telling the user there was nothing to purge while the
/// agent's rows could still be in the archive.
#[test]
fn sources_agents_exclude_distinguishes_unopenable_and_missing_archives() {
    let tracker =
        tracker_for("sources_agents_exclude_distinguishes_unopenable_and_missing_archives");
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();
    let exclude = |data_dir: &Path, agent: &str| {
        let output = tracker
            .cass_assert_command()
            .args(["sources", "agents", "exclude", agent])
            .env("XDG_CONFIG_HOME", &config_dir)
            .env("CASS_DATA_DIR", data_dir)
            .output()
            .expect("sources agents exclude command");
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        assert!(
            output.status.success(),
            "the exclusion itself succeeds: {}\nstdout: {stdout}\nstderr: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
        stdout
    };

    // An existing file that is not a SQLite database.
    let unreadable_dir = tmp.path().join("unreadable");
    fs::create_dir_all(&unreadable_dir).unwrap();
    let db_path = unreadable_dir.join("agent_search.db");
    let garbage = b"not a sqlite database\n".repeat(400);
    fs::write(&db_path, &garbage).unwrap();
    let stdout = exclude(&unreadable_dir, "openclaw");
    assert!(stdout.contains("could not be opened"), "{stdout}");
    assert!(stdout.contains("NOT purged"), "{stdout}");
    assert!(stdout.contains(&db_path.display().to_string()), "{stdout}");
    assert!(!stdout.contains("No already archived data"), "{stdout}");
    assert_eq!(
        fs::read(&db_path).unwrap(),
        garbage,
        "the archive is left as found"
    );

    // No archive at the resolved path.
    let empty_dir = tmp.path().join("empty");
    fs::create_dir_all(&empty_dir).unwrap();
    let stdout = exclude(&empty_dir, "codex");
    assert!(stdout.contains("No local archive exists at"), "{stdout}");
    assert!(!stdout.contains("No already archived data"), "{stdout}");
    assert!(!empty_dir.join("agent_search.db").exists());

    // Both exclusions were saved even though nothing was purged.
    let output = tracker
        .cass_assert_command()
        .args(["sources", "agents", "list", "--json"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources agents list command");
    assert!(output.status.success(), "sources agents list failed");
    let json: Value = serde_json::from_slice(&output.stdout).expect("valid json");
    let disabled = json["disabled_agents"].as_array().expect("disabled_agents");
    for agent in ["openclaw", "codex"] {
        assert!(disabled.iter().any(|value| value == agent), "{json}");
    }

    tracker.complete();
}

/// Test: sources list --verbose shows additional details.
#[test]
fn sources_list_verbose() {
    let tracker = tracker_for("sources_list_verbose");

    let start = tracker.start("setup", Some("Create config with verbose-testable source"));
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();
    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "workstation"
type = "ssh"
host = "dev@work.example.com"
paths = ["~/.claude/projects", "~/.codex/sessions"]
sync_schedule = "daily"
"#,
    );
    tracker.end(
        "setup",
        Some("Create config with verbose-testable source"),
        start,
    );

    let start = tracker.start(
        "run_sources_list_verbose",
        Some("Run sources list --verbose"),
    );
    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "list", "--verbose"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources list --verbose command");
    tracker.end(
        "run_sources_list_verbose",
        Some("Run sources list --verbose"),
        start,
    );

    let start = tracker.start(
        "verify_output",
        Some("Verify verbose output contains details"),
    );
    assert!(
        output.status.success(),
        "command failed: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("workstation"), "Missing source name");
    assert!(
        stdout.contains("work.example.com") || stdout.contains("dev@work"),
        "Missing host info in verbose output"
    );
    tracker.end(
        "verify_output",
        Some("Verify verbose output contains details"),
        start,
    );

    tracker.complete();
}

/// Test: sources list --json outputs valid JSON.
#[test]
fn sources_list_json() {
    let tracker = tracker_for("sources_list_json");

    let start = tracker.start("setup", Some("Create config for JSON output test"));
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&config_dir).unwrap();
    fs::create_dir_all(&data_dir).unwrap();
    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]
"#,
    );
    tracker.end("setup", Some("Create config for JSON output test"), start);

    let start = tracker.start("run_sources_list_json", Some("Run sources list --json"));
    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "list", "--json"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .env("XDG_DATA_HOME", &data_dir)
        .output()
        .expect("sources list --json command");
    tracker.end(
        "run_sources_list_json",
        Some("Run sources list --json"),
        start,
    );

    let start = tracker.start("verify_json", Some("Verify JSON structure and content"));
    assert!(
        output.status.success(),
        "command failed: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("valid JSON output");
    assert!(
        json.get("sources").is_some(),
        "Expected 'sources' field in JSON"
    );
    let sources = json["sources"].as_array().expect("sources should be array");
    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0]["name"], "laptop");
    assert_eq!(sources[0]["kind"], "legacy-remote");
    assert_eq!(sources[0]["supported"], false);
    assert_eq!(sources[0]["archived_conversations"], 0);
    tracker.end(
        "verify_json",
        Some("Verify JSON structure and content"),
        start,
    );

    tracker.complete();
}

// =============================================================================
// sources add tests
// =============================================================================

/// Test: sources add creates a local source from an explicit path.
#[test]
fn sources_add_local() {
    let tracker = tracker_for("sources_add_local");

    let start = tracker.start("setup", Some("Create temp config directory"));
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();
    tracker.end("setup", Some("Create temp config directory"), start);

    let start = tracker.start("run_sources_add", Some("Run sources add with a local path"));
    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "add", "myserver", "--path", "~/extra/sessions"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources add command");
    tracker.end(
        "run_sources_add",
        Some("Run sources add with a local path"),
        start,
    );

    let start = tracker.start(
        "verify_output",
        Some("Verify add success and config written"),
    );
    assert!(
        output.status.success(),
        "sources add failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Added local source 'myserver'"),
        "Expected success message, got: {stdout}"
    );
    let config_content = read_sources_config(&config_dir);
    assert!(
        config_content.contains("myserver"),
        "Source not in config file"
    );
    assert!(
        config_content.contains("extra/sessions"),
        "Path not in config file"
    );
    assert!(
        config_content.contains("local"),
        "Local type not in config file"
    );
    tracker.end(
        "verify_output",
        Some("Verify add success and config written"),
        start,
    );

    tracker.complete();
}

/// Test: `sources add --json` prints only the saved source as JSON, and a
/// refused duplicate add prints a JSON error envelope with no stdout data
/// (GH #531 follow-up, coding_agent_session_search-kw97g).
#[test]
fn sources_add_json_reports_the_saved_source_and_refuses_duplicates_as_json() {
    let tracker =
        tracker_for("sources_add_json_reports_the_saved_source_and_refuses_duplicates_as_json");
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();
    let add = || {
        tracker
            .cass_assert_command()
            .args([
                "sources",
                "add",
                "myserver",
                "--path",
                "~/extra/sessions",
                "--json",
            ])
            .env("XDG_CONFIG_HOME", &config_dir)
            .output()
            .expect("sources add --json")
    };

    let output = add();
    assert!(
        output.status.success(),
        "sources add --json failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&output.stdout).unwrap_or_else(|err| {
        panic!(
            "stdout must be exactly one JSON value ({err}): {}",
            String::from_utf8_lossy(&output.stdout)
        )
    });
    assert_eq!(result["status"], "added");
    assert_eq!(result["source"]["name"], "myserver");
    assert_eq!(result["source"]["kind"], "local");
    assert_eq!(result["source"]["type"], "local");
    let paths: Vec<&str> = result["source"]["paths"]
        .as_array()
        .expect("paths array")
        .iter()
        .filter_map(Value::as_str)
        .collect();
    assert_eq!(paths, ["~/extra/sessions"], "paths: {paths:?}");
    assert_eq!(result["next_steps"][0], "cass index");
    assert!(read_sources_config(&config_dir).contains("myserver"));

    let duplicate = add();
    assert_eq!(duplicate.status.code(), Some(10));
    assert!(
        duplicate.stdout.is_empty(),
        "a refused add must not print success data: {}",
        String::from_utf8_lossy(&duplicate.stdout)
    );
    let error: Value = serde_json::from_slice(&duplicate.stderr).unwrap_or_else(|err| {
        panic!(
            "stderr must be a JSON error envelope ({err}): {}",
            String::from_utf8_lossy(&duplicate.stderr)
        )
    });
    assert_eq!(error["error"]["kind"], "config");
    assert!(
        error["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("already exists")),
        "{error}"
    );
    assert_eq!(
        read_sources_config(&config_dir).matches("myserver").count(),
        1,
        "the refused add must leave the config unchanged"
    );

    tracker.complete();
}

/// Test: sources add with explicit paths.
#[test]
fn sources_add_explicit_paths() {
    let tracker = tracker_for("sources_add_explicit_paths");

    let start = tracker.start("setup", Some("Create temp config directory"));
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();
    tracker.end("setup", Some("Create temp config directory"), start);

    let start = tracker.start(
        "run_sources_add",
        Some("Run sources add with explicit paths"),
    );
    let mut command = tracker.cass_assert_command();
    let output = command
        .args([
            "sources",
            "add",
            "devbox",
            "--path",
            "~/.claude/projects",
            "--path",
            "~/.codex/sessions",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources add command");
    tracker.end(
        "run_sources_add",
        Some("Run sources add with explicit paths"),
        start,
    );

    let start = tracker.start("verify_config", Some("Verify paths in config file"));
    assert!(
        output.status.success(),
        "sources add failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let config_content = read_sources_config(&config_dir);
    assert!(
        config_content.contains("devbox"),
        "Source name not in config"
    );
    assert!(
        config_content.contains(".claude/projects"),
        "Path 1 not in config"
    );
    assert!(
        config_content.contains(".codex/sessions"),
        "Path 2 not in config"
    );
    tracker.end("verify_config", Some("Verify paths in config file"), start);

    tracker.complete();
}

/// Test: sources add fails without paths.
#[test]
fn sources_add_no_paths_error() {
    let tracker = tracker_for("sources_add_no_paths_error");

    let start = tracker.start("setup", Some("Create temp config directory"));
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();
    tracker.end("setup", Some("Create temp config directory"), start);

    let start = tracker.start("run_sources_add", Some("Run sources add without paths"));
    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "add", "server"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources add command");
    tracker.end(
        "run_sources_add",
        Some("Run sources add without paths"),
        start,
    );

    let start = tracker.start("verify_error", Some("Verify paths error reported"));
    assert!(
        !output.status.success(),
        "command should have failed but succeeded with: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("path"),
        "Expected paths error, got: {stderr}"
    );
    tracker.end("verify_error", Some("Verify paths error reported"), start);

    tracker.complete();
}

/// Test: sources add rejects duplicate source names.
#[test]
fn sources_add_duplicate_error() {
    let tracker = tracker_for("sources_add_duplicate_error");

    let start = tracker.start("setup", Some("Create config with existing source"));
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();
    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]
"#,
    );
    tracker.end("setup", Some("Create config with existing source"), start);

    let start = tracker.start(
        "run_sources_add_duplicate",
        Some("Add source with duplicate name"),
    );
    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "add", "laptop", "--path", "/tmp/other-sessions"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources add command");
    tracker.end(
        "run_sources_add_duplicate",
        Some("Add source with duplicate name"),
        start,
    );

    let start = tracker.start("verify_error", Some("Verify duplicate error"));
    assert!(
        !output.status.success(),
        "command should have failed but succeeded with: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("already exists") || stderr.contains("duplicate"),
        "Expected duplicate error, got: {stderr}"
    );
    tracker.end("verify_error", Some("Verify duplicate error"), start);

    tracker.complete();
}

/// Test: sources add rejects the reserved local source name.
#[test]
fn sources_add_reserved_local_name_error() {
    let tracker = tracker_for("sources_add_reserved_local_name_error");

    let start = tracker.start("setup", Some("Create temp config directory"));
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();
    tracker.end("setup", Some("Create temp config directory"), start);

    let start = tracker.start(
        "run_sources_add_reserved_local",
        Some("Attempt to add source named local"),
    );
    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "add", "local", "--path", "/tmp/sessions"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources add command");
    tracker.end(
        "run_sources_add_reserved_local",
        Some("Attempt to add source named local"),
        start,
    );

    let start = tracker.start("verify_error", Some("Verify reserved local error"));
    assert!(
        !output.status.success(),
        "command should have failed but succeeded with: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("reserved") || stderr.contains("built-in local source"),
        "Expected reserved-name error, got: {stderr}"
    );
    tracker.end("verify_error", Some("Verify reserved local error"), start);

    tracker.complete();
}

/// Test: sources add rejects duplicate names that differ only by case.
#[test]
fn sources_add_duplicate_error_case_insensitive() {
    let tracker = tracker_for("sources_add_duplicate_error_case_insensitive");

    let start = tracker.start(
        "setup",
        Some("Create config with existing mixed-case source"),
    );
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();
    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "Laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]
"#,
    );
    tracker.end(
        "setup",
        Some("Create config with existing mixed-case source"),
        start,
    );

    let start = tracker.start(
        "run_sources_add_duplicate",
        Some("Add source with duplicate name differing only by case"),
    );
    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "add", "laptop", "--path", "/tmp/other-sessions"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources add command");
    tracker.end(
        "run_sources_add_duplicate",
        Some("Add source with duplicate name differing only by case"),
        start,
    );

    let start = tracker.start(
        "verify_error",
        Some("Verify case-insensitive duplicate error"),
    );
    assert!(
        !output.status.success(),
        "command should have failed but succeeded with: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("already exists") || stderr.contains("duplicate"),
        "Expected duplicate error, got: {stderr}"
    );
    tracker.end(
        "verify_error",
        Some("Verify case-insensitive duplicate error"),
        start,
    );

    tracker.complete();
}

/// Test: the removed remote `sources add user@host` form is rejected through
/// the removed-command error envelope.
#[test]
fn sources_add_remote_url_rejected() {
    let tracker = tracker_for("sources_add_remote_url_rejected");

    let start = tracker.start("setup", Some("Create temp config directory"));
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();
    tracker.end("setup", Some("Create temp config directory"), start);

    let start = tracker.start(
        "run_sources_add_remote",
        Some("Attempt remote-style add user@host"),
    );
    let mut command = tracker.cass_assert_command();
    let output = command
        .args([
            "sources",
            "add",
            "user@laptop.local",
            "--path",
            "/tmp/sessions",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources add command");
    tracker.end(
        "run_sources_add_remote",
        Some("Attempt remote-style add user@host"),
        start,
    );

    let start = tracker.start(
        "verify_error",
        Some("Verify removed-command error for remote add form"),
    );
    assert_eq!(
        output.status.code(),
        Some(2),
        "removed commands exit 2: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("removed") && stderr.contains("local"),
        "Expected removed-command error, got: {stderr}"
    );
    assert!(
        !config_dir.join("cass/sources.toml").exists(),
        "a refused add must not create the config"
    );
    tracker.end(
        "verify_error",
        Some("Verify removed-command error for remote add form"),
        start,
    );

    tracker.complete();
}

// =============================================================================
// sources remove tests
// =============================================================================

/// Test: sources remove removes a configured source.
#[test]
fn sources_remove_basic() {
    let tracker = tracker_for("sources_remove_basic");

    let start = tracker.start("setup", Some("Create config with two sources"));
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]

[[sources]]
name = "workstation"
type = "ssh"
host = "dev@work.local"
paths = ["~/.claude/projects"]
"#,
    );

    tracker.end("setup", Some("Create config with two sources"), start);

    let start = tracker.start("run_sources_remove", Some("Remove laptop source"));
    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "remove", "laptop", "-y"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources remove command");
    tracker.end("run_sources_remove", Some("Remove laptop source"), start);

    let start = tracker.start(
        "verify_removal",
        Some("Verify laptop removed and workstation kept"),
    );
    assert!(
        output.status.success(),
        "sources remove failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify config was updated
    let config_content = read_sources_config(&config_dir);
    assert!(
        !config_content.contains("name = \"laptop\""),
        "Removed source still in config"
    );
    assert!(
        config_content.contains("workstation"),
        "Other source incorrectly removed"
    );
    tracker.end(
        "verify_removal",
        Some("Verify laptop removed and workstation kept"),
        start,
    );

    tracker.complete();
}

/// Test: sources remove with nonexistent source.
#[test]
fn sources_remove_nonexistent() {
    let tracker = tracker_for("sources_remove_nonexistent");

    let start = tracker.start("setup", Some("Create config with one source"));
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]
"#,
    );

    tracker.end("setup", Some("Create config with one source"), start);

    let start = tracker.start("run_sources_remove", Some("Remove nonexistent source"));
    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "remove", "nonexistent", "-y"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources remove command");
    tracker.end(
        "run_sources_remove",
        Some("Remove nonexistent source"),
        start,
    );

    let start = tracker.start("verify_error", Some("Verify not found error"));
    // Should fail gracefully
    assert!(
        !output.status.success(),
        "command should have failed but succeeded with: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not found") || stderr.contains("does not exist"),
        "Expected not found error, got: {stderr}"
    );
    tracker.end("verify_error", Some("Verify not found error"), start);

    tracker.complete();
}

/// Test: sources remove with --purge flag.
#[test]
fn sources_remove_with_purge() {
    let tracker = tracker_for("sources_remove_with_purge");

    let start = tracker.start(
        "setup",
        Some("Create config and data directory for purge test"),
    );
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&config_dir).unwrap();
    fs::create_dir_all(&data_dir).unwrap();

    // Create source data directory
    let source_data = cass_data_dir(&data_dir).join("remotes").join("laptop");
    fs::create_dir_all(&source_data).unwrap();
    fs::write(source_data.join("session.jsonl"), "test data").unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]
"#,
    );

    tracker.end(
        "setup",
        Some("Create config and data directory for purge test"),
        start,
    );

    let start = tracker.start(
        "run_sources_remove_purge",
        Some("Remove source with --purge"),
    );
    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "remove", "laptop", "--purge", "-y"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .env("XDG_DATA_HOME", &data_dir)
        .env_remove("CASS_DATA_DIR")
        .output()
        .expect("sources remove --purge command");
    tracker.end(
        "run_sources_remove_purge",
        Some("Remove source with --purge"),
        start,
    );

    let start = tracker.start("verify_removal", Some("Verify source removed from config"));
    assert!(
        output.status.success(),
        "sources remove --purge failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify config was updated
    let config_content = read_sources_config(&config_dir);
    assert!(
        !config_content.contains("laptop"),
        "Removed source still in config"
    );
    assert!(
        !source_data.exists(),
        "Synced source data should have been purged"
    );
    tracker.end(
        "verify_removal",
        Some("Verify source removed from config"),
        start,
    );

    tracker.complete();
}

/// Test: sources remove with --purge resolves the stored source name for data cleanup.
#[test]
fn sources_remove_with_purge_case_insensitive_uses_stored_name() {
    let tracker = tracker_for("sources_remove_with_purge_case_insensitive_uses_stored_name");

    let start = tracker.start(
        "setup",
        Some("Create mixed-case config and matching data directory for purge test"),
    );
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&config_dir).unwrap();
    fs::create_dir_all(&data_dir).unwrap();

    let source_data = cass_data_dir(&data_dir).join("remotes").join("Laptop");
    fs::create_dir_all(&source_data).unwrap();
    fs::write(source_data.join("session.jsonl"), "test data").unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "Laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]
"#,
    );

    tracker.end(
        "setup",
        Some("Create mixed-case config and matching data directory for purge test"),
        start,
    );

    let start = tracker.start(
        "run_sources_remove_purge",
        Some("Remove mixed-case source with lowercase filter and --purge"),
    );
    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "remove", "laptop", "--purge", "-y"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .env("XDG_DATA_HOME", &data_dir)
        .env_remove("CASS_DATA_DIR")
        .output()
        .expect("sources remove --purge command");
    tracker.end(
        "run_sources_remove_purge",
        Some("Remove mixed-case source with lowercase filter and --purge"),
        start,
    );

    let start = tracker.start(
        "verify_removal",
        Some("Verify mixed-case source config and mirror data were removed"),
    );
    assert!(
        output.status.success(),
        "sources remove --purge failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let config_content = read_sources_config(&config_dir);
    assert!(
        !config_content.contains("name = \"Laptop\""),
        "Removed source still in config"
    );
    assert!(
        !source_data.exists(),
        "Stored-name mirror directory should have been purged"
    );
    tracker.end(
        "verify_removal",
        Some("Verify mixed-case source config and mirror data were removed"),
        start,
    );

    tracker.complete();
}

/// Test: interactive remove prompt shows the canonical stored source name.
#[test]
fn sources_remove_prompt_uses_stored_name_case_insensitive() {
    let tracker = tracker_for("sources_remove_prompt_uses_stored_name_case_insensitive");

    let start = tracker.start("setup", Some("Create config with mixed-case source"));
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "Laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]
"#,
    );

    tracker.end("setup", Some("Create config with mixed-case source"), start);

    let start = tracker.start(
        "run_sources_remove_cancelled",
        Some("Run interactive remove with lowercase filter and cancel it"),
    );
    let mut cmd = tracker.cass_std_command();
    cmd.args(["sources", "remove", "laptop"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = cmd.spawn().expect("spawn sources remove command");
    child
        .stdin
        .as_mut()
        .expect("child stdin")
        .write_all(b"n\n")
        .expect("write cancel confirmation");
    let output = child.wait_with_output().expect("wait for sources remove");
    tracker.end(
        "run_sources_remove_cancelled",
        Some("Run interactive remove with lowercase filter and cancel it"),
        start,
    );

    let start = tracker.start(
        "verify_prompt",
        Some("Verify prompt used the stored canonical source name"),
    );
    assert!(
        output.status.success(),
        "cancelled remove should exit successfully: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Remove source 'Laptop' from configuration?"),
        "Expected canonical stored name in prompt, got: {stdout}"
    );
    assert!(
        stdout.contains("Cancelled."),
        "Expected cancellation output: {stdout}"
    );
    tracker.end(
        "verify_prompt",
        Some("Verify prompt used the stored canonical source name"),
        start,
    );

    tracker.complete();
}

// =============================================================================
// sources doctor tests
// =============================================================================

/// Test: sources doctor with no sources configured.
#[test]
fn sources_doctor_no_sources() {
    let tracker = tracker_for("sources_doctor_no_sources");

    let start = tracker.start("setup", Some("Create empty config directory"));
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    tracker.end("setup", Some("Create empty config directory"), start);

    let start = tracker.start(
        "run_sources_doctor",
        Some("Run sources doctor with no sources"),
    );
    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "doctor"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources doctor command");
    tracker.end(
        "run_sources_doctor",
        Some("Run sources doctor with no sources"),
        start,
    );

    let start = tracker.start("verify_output", Some("Verify no sources message"));
    // Should succeed but indicate no sources
    assert!(
        output.status.success(),
        "command failed: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("No") && stdout.contains("sources"),
        "Expected no sources message, got: {stdout}"
    );
    tracker.end("verify_output", Some("Verify no sources message"), start);

    tracker.complete();
}

/// Test: sources doctor --json outputs valid JSON.
#[test]
fn sources_doctor_json() {
    let tracker = tracker_for("sources_doctor_json");

    let start = tracker.start(
        "setup",
        Some("Create config with one source for doctor JSON"),
    );
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]
"#,
    );

    tracker.end(
        "setup",
        Some("Create config with one source for doctor JSON"),
        start,
    );

    let start = tracker.start("run_sources_doctor_json", Some("Run sources doctor --json"));
    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "doctor", "--json"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources doctor --json command");
    tracker.end(
        "run_sources_doctor_json",
        Some("Run sources doctor --json"),
        start,
    );

    let start = tracker.start(
        "verify_json",
        Some("Verify JSON array with laptop diagnostics"),
    );
    // Should output valid JSON. Local-only: the envelope is `diagnostics`
    // (per-source local checks), `budget`, and `config_validation`; a
    // configured remote host is a warning, not a probe target.
    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("valid JSON output");

    assert!(
        json.is_object(),
        "Expected sources doctor object, got: {}",
        String::from_utf8_lossy(&output.stdout)
    );

    let diagnostics = json["diagnostics"]
        .as_array()
        .expect("diagnostics array of raw checks");
    assert_eq!(
        diagnostics.len(),
        1,
        "raw checks retained under diagnostics"
    );
    assert_eq!(diagnostics[0]["source_id"], "laptop");
    let checks = diagnostics[0]["checks"]
        .as_array()
        .expect("per-source checks array");
    assert!(
        checks
            .iter()
            .any(|check| check["name"] == "Remote Host" && check["status"] == "warn"),
        "a configured remote host must be reported as a local-only warning: {diagnostics:?}"
    );
    assert!(json["budget"].is_object(), "robot budget block present");
    assert!(
        json["config_validation"].is_object(),
        "config validation report present"
    );
    tracker.end(
        "verify_json",
        Some("Verify JSON array with laptop diagnostics"),
        start,
    );

    tracker.complete();
}

/// Test: sources doctor --source filters to specific source.
#[test]
fn sources_doctor_single_source() {
    let tracker = tracker_for("sources_doctor_single_source");

    let start = tracker.start("setup", Some("Create config with two sources"));
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]

[[sources]]
name = "workstation"
type = "ssh"
host = "dev@work.local"
paths = ["~/.claude/projects"]
"#,
    );

    tracker.end("setup", Some("Create config with two sources"), start);

    let start = tracker.start(
        "run_sources_doctor_filtered",
        Some("Run doctor filtered to laptop"),
    );
    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "doctor", "--source", "laptop", "--json"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources doctor --source command");
    tracker.end(
        "run_sources_doctor_filtered",
        Some("Run doctor filtered to laptop"),
        start,
    );

    let start = tracker.start(
        "verify_filtered_output",
        Some("Verify only laptop in output"),
    );
    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("valid JSON output");

    // Filtered output contains only the laptop source, in the envelope's
    // `diagnostics` array (each entry keyed by `source_id`).
    let diagnostics = json["diagnostics"].as_array().expect("diagnostics array");
    assert_eq!(
        diagnostics.len(),
        1,
        "Should only have one source in output"
    );
    assert_eq!(diagnostics[0]["source_id"], "laptop");
    tracker.end(
        "verify_filtered_output",
        Some("Verify only laptop in output"),
        start,
    );

    tracker.complete();
}

// =============================================================================
// Integration workflow tests
// =============================================================================

/// Test: Complete workflow - add, list, remove.
#[test]
fn sources_workflow_add_list_remove() {
    let tracker = tracker_for("sources_workflow_add_list_remove");

    let start = tracker.start("setup", Some("Create temp config directory"));
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    tracker.end("setup", Some("Create temp config directory"), start);

    // 1. Add a source
    let start = tracker.start("add_source", Some("Add server source"));
    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "add", "server", "--path", "/tmp/sessions"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources add command");
    assert!(
        output.status.success(),
        "command failed: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    tracker.end("add_source", Some("Add server source"), start);

    // 2. List sources - should show the added source
    let start = tracker.start(
        "list_sources",
        Some("List sources and verify server present"),
    );
    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "list"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources list command");
    assert!(
        output.status.success(),
        "command failed: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("server"));
    tracker.end(
        "list_sources",
        Some("List sources and verify server present"),
        start,
    );

    // 3. Remove the source
    let start = tracker.start("remove_source", Some("Remove server source"));
    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "remove", "server", "-y"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources remove command");
    assert!(
        output.status.success(),
        "command failed: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    tracker.end("remove_source", Some("Remove server source"), start);

    // 4. List again - should be empty
    let start = tracker.start("verify_empty", Some("Verify source was removed"));
    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "list"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources list command");
    assert!(
        output.status.success(),
        "command failed: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.contains("server"),
        "Source should be removed, got: {stdout}"
    );
    tracker.end("verify_empty", Some("Verify source was removed"), start);

    tracker.complete();
}

/// Test: Add multiple sources and list them.
#[test]
fn sources_multiple_add_list() {
    let tracker = tracker_for("sources_multiple_add_list");

    let start = tracker.start("setup", Some("Create temp config directory"));
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    tracker.end("setup", Some("Create temp config directory"), start);

    // Add first source
    let start = tracker.start("add_laptop", Some("Add laptop source"));
    tracker
        .cass_assert_command()
        .args(["sources", "add", "laptop", "--path", "/tmp/laptop-sessions"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .assert()
        .success();
    tracker.end("add_laptop", Some("Add laptop source"), start);

    // Add second source
    let start = tracker.start("add_workstation", Some("Add workstation source"));
    tracker
        .cass_assert_command()
        .args([
            "sources",
            "add",
            "workstation",
            "--path",
            "/tmp/workstation-sessions",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .assert()
        .success();
    tracker.end("add_workstation", Some("Add workstation source"), start);

    // List all sources
    let start = tracker.start("verify_list", Some("List sources and verify both present"));
    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "list", "--json"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources list command");

    assert!(
        output.status.success(),
        "command failed: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("valid JSON");
    let sources = json["sources"].as_array().expect("sources array");

    assert_eq!(sources.len(), 2);
    let names: Vec<&str> = sources.iter().filter_map(|s| s["name"].as_str()).collect();
    assert!(names.contains(&"laptop"));
    assert!(names.contains(&"workstation"));
    tracker.end(
        "verify_list",
        Some("List sources and verify both present"),
        start,
    );

    tracker.complete();
}

// =============================================================================
// sources mappings list tests
// =============================================================================

/// Test: sources mappings list with no mappings configured.
#[test]
fn mappings_list_empty() {
    let tracker = tracker_for("mappings_list_empty");

    let start = tracker.start("setup", Some("Create config with source but no mappings"));
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]
"#,
    );

    tracker.end(
        "setup",
        Some("Create config with source but no mappings"),
        start,
    );

    let start = tracker.start("run_mappings_list", Some("Run mappings list for laptop"));
    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "mappings", "list", "laptop"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources mappings list command");
    tracker.end(
        "run_mappings_list",
        Some("Run mappings list for laptop"),
        start,
    );

    let start = tracker.start("verify_output", Some("Verify empty mappings message"));
    assert!(
        output.status.success(),
        "command failed: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("No") || stdout.contains("0 mapping"),
        "Expected no mappings message, got: {stdout}"
    );
    tracker.end(
        "verify_output",
        Some("Verify empty mappings message"),
        start,
    );

    tracker.complete();
}

/// Test: sources mappings list with mappings configured.
#[test]
fn mappings_list_with_mappings() {
    let tracker = tracker_for("mappings_list_with_mappings");

    let start = tracker.start("setup", Some("Create config with source and path mapping"));
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]

[[sources.path_mappings]]
from = "/home/user/projects"
to = "/Users/me/projects"
"#,
    );

    tracker.end(
        "setup",
        Some("Create config with source and path mapping"),
        start,
    );

    let start = tracker.start("run_mappings_list", Some("Run mappings list for laptop"));
    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "mappings", "list", "laptop"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources mappings list command");
    tracker.end(
        "run_mappings_list",
        Some("Run mappings list for laptop"),
        start,
    );

    let start = tracker.start("verify_output", Some("Verify mapping paths in output"));
    assert!(
        output.status.success(),
        "command failed: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("/home/user/projects") && stdout.contains("/Users/me/projects"),
        "Expected mapping paths in output, got: {stdout}"
    );
    tracker.end(
        "verify_output",
        Some("Verify mapping paths in output"),
        start,
    );

    tracker.complete();
}

/// Test: sources mappings list --json outputs valid JSON.
#[test]
fn mappings_list_json() {
    let tracker = tracker_for("mappings_list_json");

    let start = tracker.start("setup", Some("Create config with mapping for JSON test"));
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]

[[sources.path_mappings]]
from = "/home/user/projects"
to = "/Users/me/projects"
"#,
    );

    tracker.end(
        "setup",
        Some("Create config with mapping for JSON test"),
        start,
    );

    let start = tracker.start("run_mappings_list_json", Some("Run mappings list --json"));
    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "mappings", "list", "laptop", "--json"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources mappings list --json command");
    tracker.end(
        "run_mappings_list_json",
        Some("Run mappings list --json"),
        start,
    );

    let start = tracker.start("verify_json", Some("Verify JSON contains mappings field"));
    assert!(
        output.status.success(),
        "command failed: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("valid JSON output");

    assert!(
        json.get("mappings").is_some(),
        "Expected 'mappings' field in JSON"
    );
    tracker.end(
        "verify_json",
        Some("Verify JSON contains mappings field"),
        start,
    );

    tracker.complete();
}

/// Test: sources mappings list for nonexistent source.
#[test]
fn mappings_list_nonexistent_source() {
    let tracker = tracker_for("mappings_list_nonexistent_source");

    let start = tracker.start("setup", Some("Create config with laptop source"));
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]
"#,
    );

    tracker.end("setup", Some("Create config with laptop source"), start);

    let start = tracker.start(
        "run_mappings_list",
        Some("List mappings for nonexistent source"),
    );
    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "mappings", "list", "nonexistent"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources mappings list command");
    tracker.end(
        "run_mappings_list",
        Some("List mappings for nonexistent source"),
        start,
    );

    let start = tracker.start("verify_error", Some("Verify not found error"));
    assert!(
        !output.status.success(),
        "command should have failed but succeeded with: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not found") || stderr.contains("does not exist"),
        "Expected not found error, got: {stderr}"
    );
    tracker.end("verify_error", Some("Verify not found error"), start);

    tracker.complete();
}

// =============================================================================
// sources mappings add tests
// =============================================================================

/// Test: sources mappings add basic mapping.
#[test]
fn mappings_add_basic() {
    let tracker = tracker_for("mappings_add_basic");

    let start = tracker.start("setup", Some("Create config with laptop source"));
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]
"#,
    );

    tracker.end("setup", Some("Create config with laptop source"), start);

    let start = tracker.start("run_mappings_add", Some("Add basic path mapping"));
    let mut command = tracker.cass_assert_command();
    let output = command
        .args([
            "sources",
            "mappings",
            "add",
            "laptop",
            "--from",
            "/remote/path",
            "--to",
            "/local/path",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources mappings add command");
    tracker.end("run_mappings_add", Some("Add basic path mapping"), start);

    let start = tracker.start("verify_config", Some("Verify mapping in config file"));
    assert!(
        output.status.success(),
        "mappings add failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify config was updated
    let config_content = read_sources_config(&config_dir);
    assert!(
        config_content.contains("/remote/path") && config_content.contains("/local/path"),
        "Mapping not in config: {config_content}"
    );
    tracker.end(
        "verify_config",
        Some("Verify mapping in config file"),
        start,
    );

    tracker.complete();
}

/// Test: sources mappings add with agent filter.
#[test]
fn mappings_add_with_agents() {
    let tracker = tracker_for("mappings_add_with_agents");

    let start = tracker.start("setup", Some("Create config with laptop source"));
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]
"#,
    );

    tracker.end("setup", Some("Create config with laptop source"), start);

    let start = tracker.start("run_mappings_add", Some("Add mapping with agent filter"));
    let mut command = tracker.cass_assert_command();
    let output = command
        .args([
            "sources",
            "mappings",
            "add",
            "laptop",
            "--from",
            "/opt/work",
            "--to",
            "/Volumes/Work",
            "--agents",
            "claude_code,codex",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources mappings add command");
    tracker.end(
        "run_mappings_add",
        Some("Add mapping with agent filter"),
        start,
    );

    let start = tracker.start("verify_config", Some("Verify agent filter in config"));
    assert!(
        output.status.success(),
        "mappings add failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let config_content = read_sources_config(&config_dir);
    assert!(
        config_content.contains("claude_code") || config_content.contains("agents"),
        "Agent filter not in config: {config_content}"
    );
    tracker.end(
        "verify_config",
        Some("Verify agent filter in config"),
        start,
    );

    tracker.complete();
}

/// Test: sources mappings add multiple mappings.
#[test]
fn mappings_add_multiple() {
    let tracker = tracker_for("mappings_add_multiple");

    let start = tracker.start("setup", Some("Create config with laptop source"));
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]
"#,
    );

    tracker.end("setup", Some("Create config with laptop source"), start);

    // Add first mapping
    let start = tracker.start("add_first_mapping", Some("Add /home/user mapping"));
    tracker
        .cass_assert_command()
        .args([
            "sources",
            "mappings",
            "add",
            "laptop",
            "--from",
            "/home/user",
            "--to",
            "/Users/me",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .assert()
        .success();
    tracker.end("add_first_mapping", Some("Add /home/user mapping"), start);

    // Add second mapping
    let start = tracker.start("add_second_mapping", Some("Add /opt/projects mapping"));
    tracker
        .cass_assert_command()
        .args([
            "sources",
            "mappings",
            "add",
            "laptop",
            "--from",
            "/opt/projects",
            "--to",
            "/Projects",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .assert()
        .success();
    tracker.end(
        "add_second_mapping",
        Some("Add /opt/projects mapping"),
        start,
    );

    // Verify both mappings are in config
    let start = tracker.start("verify_config", Some("Verify both mappings in config"));
    let config_content = read_sources_config(&config_dir);
    assert!(
        config_content.contains("/home/user") && config_content.contains("/opt/projects"),
        "Both mappings not in config: {config_content}"
    );
    tracker.end(
        "verify_config",
        Some("Verify both mappings in config"),
        start,
    );

    tracker.complete();
}

/// Test: sources mappings add to nonexistent source.
#[test]
fn mappings_add_nonexistent_source() {
    let tracker = tracker_for("mappings_add_nonexistent_source");

    let start = tracker.start("setup", Some("Create config with laptop source"));
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]
"#,
    );

    tracker.end("setup", Some("Create config with laptop source"), start);

    let start = tracker.start(
        "run_mappings_add",
        Some("Add mapping to nonexistent source"),
    );
    let mut command = tracker.cass_assert_command();
    let output = command
        .args([
            "sources",
            "mappings",
            "add",
            "nonexistent",
            "--from",
            "/from",
            "--to",
            "/to",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources mappings add command");
    tracker.end(
        "run_mappings_add",
        Some("Add mapping to nonexistent source"),
        start,
    );

    let start = tracker.start("verify_error", Some("Verify not found error"));
    assert!(
        !output.status.success(),
        "command should have failed but succeeded with: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not found") || stderr.contains("does not exist"),
        "Expected not found error, got: {stderr}"
    );
    tracker.end("verify_error", Some("Verify not found error"), start);

    tracker.complete();
}

// =============================================================================
// sources mappings remove tests
// =============================================================================

/// Test: sources mappings remove by index.
#[test]
fn mappings_remove_by_index() {
    let tracker = tracker_for("mappings_remove_by_index");

    let start = tracker.start("setup", Some("Create config with two path mappings"));
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]

[[sources.path_mappings]]
from = "/home/user"
to = "/Users/me"

[[sources.path_mappings]]
from = "/opt/work"
to = "/Work"
"#,
    );

    tracker.end("setup", Some("Create config with two path mappings"), start);

    let start = tracker.start("run_mappings_remove", Some("Remove mapping at index 0"));
    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "mappings", "remove", "laptop", "0"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources mappings remove command");
    tracker.end(
        "run_mappings_remove",
        Some("Remove mapping at index 0"),
        start,
    );

    let start = tracker.start(
        "verify_removal",
        Some("Verify first mapping removed, second kept"),
    );
    assert!(
        output.status.success(),
        "mappings remove failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // First mapping should be gone, second should remain
    let config_content = read_sources_config(&config_dir);
    assert!(
        !config_content.contains("/home/user"),
        "Removed mapping still in config"
    );
    assert!(
        config_content.contains("/opt/work"),
        "Other mapping incorrectly removed"
    );
    tracker.end(
        "verify_removal",
        Some("Verify first mapping removed, second kept"),
        start,
    );

    tracker.complete();
}

/// Test: sources mappings remove with invalid index.
#[test]
fn mappings_remove_invalid_index() {
    let tracker = tracker_for("mappings_remove_invalid_index");

    let start = tracker.start("setup", Some("Create config with one path mapping"));
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]

[[sources.path_mappings]]
from = "/home/user"
to = "/Users/me"
"#,
    );

    tracker.end("setup", Some("Create config with one path mapping"), start);

    let start = tracker.start(
        "run_mappings_remove",
        Some("Remove mapping at invalid index 99"),
    );
    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "mappings", "remove", "laptop", "99"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources mappings remove command");
    tracker.end(
        "run_mappings_remove",
        Some("Remove mapping at invalid index 99"),
        start,
    );

    let start = tracker.start("verify_error", Some("Verify index out of range error"));
    assert!(
        !output.status.success(),
        "command should have failed but succeeded with: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("index") || stderr.contains("out of") || stderr.contains("range"),
        "Expected index error, got: {stderr}"
    );
    tracker.end(
        "verify_error",
        Some("Verify index out of range error"),
        start,
    );

    tracker.complete();
}

/// Test: sources mappings remove from empty mappings list.
#[test]
fn mappings_remove_from_empty() {
    let tracker = tracker_for("mappings_remove_from_empty");

    let start = tracker.start("setup", Some("Create config with no mappings"));
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]
"#,
    );

    tracker.end("setup", Some("Create config with no mappings"), start);

    let start = tracker.start(
        "run_mappings_remove",
        Some("Remove from empty mappings list"),
    );
    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "mappings", "remove", "laptop", "0"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources mappings remove command");
    tracker.end(
        "run_mappings_remove",
        Some("Remove from empty mappings list"),
        start,
    );

    let start = tracker.start("verify_error", Some("Verify empty mappings error"));
    assert!(
        !output.status.success(),
        "command should have failed but succeeded with: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("no mapping") || stderr.contains("empty") || stderr.contains("index"),
        "Expected no mappings error, got: {stderr}"
    );
    tracker.end("verify_error", Some("Verify empty mappings error"), start);

    tracker.complete();
}

// =============================================================================
// sources mappings test tests
// =============================================================================

/// Test: sources mappings test with matching path.
#[test]
fn mappings_test_match() {
    let tracker = tracker_for("mappings_test_match");

    let start = tracker.start("setup", Some("Create config with path mapping"));
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]

[[sources.path_mappings]]
from = "/home/user/projects"
to = "/Users/me/projects"
"#,
    );

    tracker.end("setup", Some("Create config with path mapping"), start);

    let start = tracker.start("run_mappings_test", Some("Test path that matches mapping"));
    let mut command = tracker.cass_assert_command();
    let output = command
        .args([
            "sources",
            "mappings",
            "test",
            "laptop",
            "/home/user/projects/myapp/src/main.rs",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources mappings test command");
    tracker.end(
        "run_mappings_test",
        Some("Test path that matches mapping"),
        start,
    );

    let start = tracker.start("verify_rewritten_path", Some("Verify path was rewritten"));
    assert!(
        output.status.success(),
        "command failed: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("/Users/me/projects/myapp/src/main.rs"),
        "Expected rewritten path, got: {stdout}"
    );
    tracker.end(
        "verify_rewritten_path",
        Some("Verify path was rewritten"),
        start,
    );

    tracker.complete();
}

/// Test: sources mappings test with non-matching path.
#[test]
fn mappings_test_no_match() {
    let tracker = tracker_for("mappings_test_no_match");

    let start = tracker.start("setup", Some("Create config with path mapping"));
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]

[[sources.path_mappings]]
from = "/home/user/projects"
to = "/Users/me/projects"
"#,
    );

    tracker.end("setup", Some("Create config with path mapping"), start);

    let start = tracker.start(
        "run_mappings_test",
        Some("Test path that does not match mapping"),
    );
    let mut command = tracker.cass_assert_command();
    let output = command
        .args([
            "sources",
            "mappings",
            "test",
            "laptop",
            "/opt/other/path/file.rs",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources mappings test command");
    tracker.end(
        "run_mappings_test",
        Some("Test path that does not match mapping"),
        start,
    );

    let start = tracker.start(
        "verify_unchanged_path",
        Some("Verify path unchanged or no match"),
    );
    assert!(
        output.status.success(),
        "command failed: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    // Path should be unchanged or indicate no match
    assert!(
        stdout.contains("/opt/other/path/file.rs") || stdout.contains("no match"),
        "Expected unchanged path or no match, got: {stdout}"
    );
    tracker.end(
        "verify_unchanged_path",
        Some("Verify path unchanged or no match"),
        start,
    );

    tracker.complete();
}

/// Test: sources mappings test with agent filter.
#[test]
fn mappings_test_with_agent() {
    let tracker = tracker_for("mappings_test_with_agent");

    let start = tracker.start("setup", Some("Create config with agent-filtered mapping"));
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]

[[sources.path_mappings]]
from = "/home/user"
to = "/Users/me"
agents = ["claude_code"]
"#,
    );

    tracker.end(
        "setup",
        Some("Create config with agent-filtered mapping"),
        start,
    );

    // Test with matching agent
    let start = tracker.start(
        "run_mappings_test",
        Some("Test mapping with matching agent"),
    );
    let mut command = tracker.cass_assert_command();
    let output = command
        .args([
            "sources",
            "mappings",
            "test",
            "laptop",
            "/home/user/file.rs",
            "--agent",
            "claude_code",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources mappings test command");
    tracker.end(
        "run_mappings_test",
        Some("Test mapping with matching agent"),
        start,
    );

    let start = tracker.start(
        "verify_rewritten_path",
        Some("Verify path rewritten for matching agent"),
    );
    assert!(
        output.status.success(),
        "command failed: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("/Users/me/file.rs"),
        "Expected rewritten path for matching agent, got: {stdout}"
    );
    tracker.end(
        "verify_rewritten_path",
        Some("Verify path rewritten for matching agent"),
        start,
    );

    tracker.complete();
}

// =============================================================================
// mappings workflow integration test
// =============================================================================

/// Test: Complete mappings workflow - add, list, test, remove.
#[test]
fn mappings_workflow_complete() {
    let tracker = tracker_for("mappings_workflow_complete");

    let start = tracker.start("setup", Some("Create config with laptop source"));
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]
"#,
    );

    tracker.end("setup", Some("Create config with laptop source"), start);

    // 1. Add a mapping
    let start = tracker.start("add_mapping", Some("Add path mapping"));
    tracker
        .cass_assert_command()
        .args([
            "sources",
            "mappings",
            "add",
            "laptop",
            "--from",
            "/remote/path",
            "--to",
            "/local/path",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .assert()
        .success();
    tracker.end("add_mapping", Some("Add path mapping"), start);

    // 2. List mappings - should show the added mapping
    let start = tracker.start("list_mappings", Some("List mappings and verify added"));
    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "mappings", "list", "laptop"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("list command");
    assert!(
        output.status.success(),
        "command failed: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("/remote/path"));
    tracker.end(
        "list_mappings",
        Some("List mappings and verify added"),
        start,
    );

    // 3. Test the mapping
    let start = tracker.start("test_mapping", Some("Test path rewriting"));
    let mut command = tracker.cass_assert_command();
    let output = command
        .args([
            "sources",
            "mappings",
            "test",
            "laptop",
            "/remote/path/subdir/file.rs",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("test command");
    assert!(
        output.status.success(),
        "command failed: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("/local/path/subdir/file.rs"));
    tracker.end("test_mapping", Some("Test path rewriting"), start);

    // 4. Remove the mapping
    let start = tracker.start("remove_mapping", Some("Remove the mapping"));
    tracker
        .cass_assert_command()
        .args(["sources", "mappings", "remove", "laptop", "0"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .assert()
        .success();
    tracker.end("remove_mapping", Some("Remove the mapping"), start);

    // 5. List again - should be empty
    let start = tracker.start("verify_empty", Some("Verify mapping was removed"));
    let mut command = tracker.cass_assert_command();
    let output = command
        .args(["sources", "mappings", "list", "laptop"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("list command");
    assert!(
        output.status.success(),
        "command failed: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    // After removal, should show "No path mappings" message
    assert!(
        stdout.contains("No") || !stdout.contains("/remote/path"),
        "Mapping should be removed, got: {stdout}"
    );
    tracker.end("verify_empty", Some("Verify mapping was removed"), start);

    tracker.complete();
}

/// `cass sources doctor` reports problems in `sources.toml` itself (bead
/// uojcg.8.5, adapted for local-only). A legacy `type = "ssh"` entry is an
/// inert archive record: it surfaces as a `legacy_remote_source` warning —
/// never probed, never an error. A `.sources.toml.tmp.*` left beside the
/// config by an interrupted write is a separate `interrupted_config_write`
/// warning, reported only when the file exists.
#[test]
#[cfg(unix)]
fn sources_doctor_reports_config_problems_the_host_probes_cannot_see() {
    let tracker = tracker_for("sources_doctor_reports_config_problems_the_host_probes_cannot_see");
    let diagnose = |leftover_temp: bool| -> (Option<i32>, Value) {
        let tmp = tempfile::TempDir::new().expect("create fixture");
        let config_dir = tmp.path().join("config");
        let data_dir = tmp.path().join("data");
        let empty_bin = tmp.path().join("bin");
        for dir in [&config_dir, &data_dir, &empty_bin] {
            fs::create_dir_all(dir).expect("create fixture directory");
        }
        create_sources_config(
            &config_dir,
            r#"
[[sources]]
name = "box"
type = "ssh"
host = "fixture@box.test"
paths = ["~/.claude/projects"]

[[sources.path_mappings]]
from = "/home/fixture/projects"
to = "/work/projects"
"#,
        );
        if leftover_temp {
            fs::write(config_dir.join("cass/.sources.toml.tmp.4242"), b"partial")
                .expect("write interrupted-write leftover");
        }
        let mut cmd = tracker.cass_std_command();
        cmd.args(["sources", "doctor", "--json"])
            .current_dir(tmp.path())
            .env("HOME", tmp.path())
            .env("XDG_CONFIG_HOME", &config_dir)
            .env("XDG_CACHE_HOME", tmp.path().join("cache"))
            .env("CASS_DATA_DIR", &data_dir)
            .env("PATH", &empty_bin)
            .env("NO_COLOR", "1");
        let output = util::timeout::spawn_with_timeout_or_diag(
            cmd,
            "sources_doctor_config_validation",
            Some(&data_dir),
            std::time::Duration::from_secs(30),
        );
        let json = serde_json::from_slice::<Value>(&output.stdout);
        assert!(
            json.is_ok(),
            "sources doctor --json stdout must be JSON; stdout={}; stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        (output.status.code(), json.expect("checked above"))
    };
    let kinds = |report: &Value| -> Vec<String> {
        report["config_validation"]["issues"]
            .as_array()
            .map(|issues| {
                issues
                    .iter()
                    .filter_map(|issue| issue["kind"].as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default()
    };

    let (code, report) = diagnose(false);
    assert_eq!(report["config_validation"]["source_count"], 1, "{report}");
    assert_eq!(report["config_validation"]["valid"], true, "{report}");
    assert_eq!(kinds(&report), ["legacy_remote_source"], "{report}");
    assert_eq!(code, Some(0), "a legacy archive record is not an error");

    let (code, report) = diagnose(true);
    let found = kinds(&report);
    assert!(
        found.iter().any(|kind| kind == "legacy_remote_source")
            && found.iter().any(|kind| kind == "interrupted_config_write"),
        "{report}"
    );
    assert_eq!(code, Some(0));
    tracker.complete();
}

/// GH #520, end to end: the archive layout keeps only a configured path's
/// basename inside its safe-name container (`~/.factory/sessions` arrives as
/// `<container>/sessions`). Factory, Vibe, Prime, Gemini and Cursor recognise
/// a store by its parent chain, so they indexed nothing from such a mirror.
/// The real binary indexes a mirror laid out exactly that way: every agent
/// must index, each marker must be searchable, and a second index must not
/// add or rekey a conversation.
#[test]
fn gh520_remote_mirrors_index_structural_agents_end_to_end() {
    use coding_agent_search::franken_sync::compat::{ConnectionExt, RowExt};
    use coding_agent_search::sources::provenance::path_to_safe_dirname;
    use std::collections::BTreeSet;

    let tracker = tracker_for("gh520_remote_mirrors_index_structural_agents_end_to_end");
    let tmp = tempfile::TempDir::new().unwrap();
    let root = tmp.path();
    let home = root.join("home");
    let config = root.join("config");
    let data = root.join("data");
    fs::create_dir_all(&home).unwrap();
    fs::write(home.join(".env"), "").unwrap();

    let paths = [
        "~/.claude/projects",
        "~/.codex/sessions",
        "~/.pi/agent/sessions",
        "~/.factory/sessions",
        "~/.vibe/logs/session",
        "~/.prime/agent/sessions",
        "~/.gemini/tmp",
        "~/.config/Cursor/User",
    ];
    create_sources_config(
        &config,
        &format!(
            "[[sources]]\nname = \"laptop\"\ntype = \"ssh\"\nhost = \"operator@laptop\"\npaths = {}\n",
            serde_json::to_string(&paths).unwrap()
        ),
    );

    // The historical mirror layout every sync method produced:
    // <container>/<basename>/...
    let mirror = data.join("remotes/laptop/mirror");
    let container = |path: &str| mirror.join(path_to_safe_dirname(path));
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let copy_tree = |from: &Path, to: &Path| {
        for entry in walkdir::WalkDir::new(from) {
            let entry = entry.unwrap();
            let target = to.join(entry.path().strip_prefix(from).unwrap());
            if entry.file_type().is_dir() {
                fs::create_dir_all(&target).unwrap();
            } else {
                fs::copy(entry.path(), &target).unwrap();
            }
        }
    };
    let write = |path: std::path::PathBuf, lines: &[String]| {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, lines.join("\n") + "\n").unwrap();
    };
    copy_tree(
        &fixtures.join("claude_code_real/projects"),
        &container("~/.claude/projects").join("projects"),
    );
    copy_tree(
        &fixtures.join("codex_real/sessions"),
        &container("~/.codex/sessions").join("sessions"),
    );
    copy_tree(
        &fixtures.join("pi_agent/sessions"),
        &container("~/.pi/agent/sessions").join("sessions"),
    );
    write(
        container("~/.factory/sessions").join("sessions/-home-operator-proj/fac-520.jsonl"),
        &[
            r#"{"type":"session_start","id":"fac-520","title":"Factory mirror","owner":"operator","cwd":"/home/operator/proj"}"#.into(),
            r#"{"type":"message","timestamp":"2026-10-01T10:00:00Z","message":{"role":"user","content":"factorymirrorprobe question"}}"#.into(),
            r#"{"type":"message","timestamp":"2026-10-01T10:00:05Z","message":{"role":"assistant","content":"factory answer"}}"#.into(),
        ],
    );
    write(
        container("~/.vibe/logs/session").join("session/vibe-520/messages.jsonl"),
        &[
            r#"{"role":"user","content":"vibemirrorprobe question","timestamp":"2026-10-01T10:00:00.000Z"}"#.into(),
            r#"{"role":"assistant","content":"vibe answer","timestamp":"2026-10-01T10:00:05.000Z"}"#.into(),
        ],
    );
    write(
        container("~/.prime/agent/sessions")
            .join("sessions/2026-10-01T10-00-00-000Z_0192aa11-0000-7000-8000-000000000520.jsonl"),
        &[
            r#"{"type":"session","version":3,"id":"0192aa11-0000-7000-8000-000000000520","timestamp":"2026-10-01T10:00:00.000Z","cwd":"/home/operator/proj"}"#.into(),
            r#"{"type":"message","id":"aaaa0001","parentId":null,"timestamp":"2026-10-01T10:00:01.000Z","message":{"role":"user","content":"primemirrorprobe question"}}"#.into(),
            r#"{"type":"message","id":"aaaa0002","parentId":"aaaa0001","timestamp":"2026-10-01T10:00:02.000Z","message":{"role":"assistant","content":[{"type":"text","text":"prime answer"}]}}"#.into(),
        ],
    );
    write(
        container("~/.gemini/tmp").join("tmp/hash520/chats/session-520.json"),
        &[serde_json::json!({
            "sessionId": "gemini-520",
            "projectHash": "hash520",
            "startTime": "2026-10-01T10:00:00Z",
            "lastUpdated": "2026-10-01T10:05:00Z",
            "messages": [
                {"type": "user", "content": "geminimirrorprobe question", "timestamp": "2026-10-01T10:00:00Z"},
                {"type": "model", "content": "gemini answer", "timestamp": "2026-10-01T10:01:00Z"}
            ]
        })
        .to_string()],
    );
    {
        let db_path = container("~/.config/Cursor/User").join("User/globalStorage/state.vscdb");
        fs::create_dir_all(db_path.parent().unwrap()).unwrap();
        let conn =
            coding_agent_search::franken_sync::Connection::open(db_path.to_string_lossy().as_ref())
                .unwrap();
        conn.execute("CREATE TABLE cursorDiskKV (key TEXT PRIMARY KEY, value TEXT)")
            .unwrap();
        conn.execute("CREATE TABLE ItemTable (key TEXT PRIMARY KEY, value TEXT)")
            .unwrap();
        conn.execute_compat(
            "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
            coding_agent_search::franken_sync::params![
                "composerData:cursor-520",
                r#"{"text":"cursormirrorprobe question"}"#
            ],
        )
        .unwrap();
    }

    let command = || {
        let mut command = tracker.cass_std_command();
        command
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("HOME", &home)
            .env("XDG_CONFIG_HOME", &config)
            .env("XDG_DATA_HOME", root.join("xdg"))
            .env("CASS_DATA_DIR", &data)
            .env("CASS_AUTO_REFRESH", "0")
            .env("RUST_MIN_STACK", "134217728")
            .current_dir(&home);
        tracker.command_environment().apply_to_std(&mut command);
        command
    };
    let run = |args: &[&str]| {
        let output = command().args(args).output().unwrap();
        (
            output.status.code(),
            String::from_utf8_lossy(&output.stdout).into_owned(),
            String::from_utf8_lossy(&output.stderr).into_owned(),
        )
    };
    let index = |args: &[&str]| {
        let (code, stdout, stderr) = run(args);
        assert_eq!(code, Some(0), "{args:?}\nstdout={stdout}\nstderr={stderr}");
    };
    let rows = || -> BTreeSet<(String, Option<String>)> {
        let storage = FrankenStorage::open_readonly(&data.join("agent_search.db")).unwrap();
        storage
            .raw()
            .query(
                "SELECT a.slug, c.external_id FROM conversations c \
                 JOIN agents a ON a.id = c.agent_id WHERE c.source_id = 'laptop'",
            )
            .unwrap()
            .into_iter()
            .map(|row| {
                (
                    row.get_typed::<String>(0).unwrap(),
                    row.get_typed::<Option<String>>(1).unwrap(),
                )
            })
            .collect()
    };
    let conversation_count = || -> i64 {
        let storage = FrankenStorage::open_readonly(&data.join("agent_search.db")).unwrap();
        storage
            .raw()
            .query_row_map(
                "SELECT COUNT(*) FROM conversations WHERE source_id = 'laptop'",
                &[],
                |row| row.get_typed(0),
            )
            .unwrap()
    };
    let hits = |marker: &str| -> usize {
        let (code, stdout, stderr) = run(&[
            "search",
            marker,
            "--robot",
            "--mode",
            "lexical",
            "--no-daemon",
        ]);
        assert_eq!(code, Some(0), "search {marker}\nstderr={stderr}");
        let payload: Value = serde_json::from_str(&stdout).unwrap();
        payload["hits"].as_array().unwrap().len()
    };

    index(&["index", "--full", "--json"]);
    let first = rows();
    eprintln!("gh520 indexed: {first:?}");
    let slugs: BTreeSet<String> = first.iter().map(|(slug, _)| slug.clone()).collect();
    for slug in [
        "claude_code",
        "codex",
        "pi_agent",
        "factory",
        "vibe",
        "prime_agent",
        "gemini",
        "cursor",
    ] {
        assert!(slugs.contains(slug), "{slug} missing: {first:?}");
    }
    for marker in [
        "factorymirrorprobe",
        "vibemirrorprobe",
        "primemirrorprobe",
        "geminimirrorprobe",
        "cursormirrorprobe",
    ] {
        assert!(hits(marker) > 0, "{marker} not searchable");
    }
    assert_eq!(conversation_count(), i64::try_from(first.len()).unwrap());

    // Each conversation belongs to the agent whose store its file was synced
    // from. Before mx520 the Claude connector also claimed the Factory, Pi,
    // Prime and Gemini files, and Pi claimed the Prime file.
    let expected_agent: std::collections::BTreeMap<String, &str> = [
        ("~/.claude/projects", "claude_code"),
        ("~/.codex/sessions", "codex"),
        ("~/.pi/agent/sessions", "pi_agent"),
        ("~/.factory/sessions", "factory"),
        ("~/.vibe/logs/session", "vibe"),
        ("~/.prime/agent/sessions", "prime_agent"),
        ("~/.gemini/tmp", "gemini"),
        ("~/.config/Cursor/User", "cursor"),
    ]
    .into_iter()
    .map(|(path, agent)| (path_to_safe_dirname(path), agent))
    .collect();
    let attributions: Vec<(String, String)> = {
        let storage = FrankenStorage::open_readonly(&data.join("agent_search.db")).unwrap();
        storage
            .raw()
            .query(
                "SELECT a.slug, c.source_path FROM conversations c \
                 JOIN agents a ON a.id = c.agent_id WHERE c.source_id = 'laptop'",
            )
            .unwrap()
            .into_iter()
            .map(|row| {
                (
                    row.get_typed::<String>(0).unwrap(),
                    row.get_typed::<String>(1).unwrap(),
                )
            })
            .collect()
    };
    let misattributed: Vec<&(String, String)> = attributions
        .iter()
        .filter(|(agent, source_path)| {
            let container = Path::new(source_path)
                .strip_prefix(&mirror)
                .ok()
                .and_then(|relative| relative.components().next())
                .map(|component| component.as_os_str().to_string_lossy().into_owned());
            container.and_then(|container| expected_agent.get(&container).copied())
                != Some(agent.as_str())
        })
        .collect();
    assert!(
        misattributed.is_empty(),
        "conversations attributed to the wrong agent: {misattributed:?}"
    );

    // A second, incremental index re-reads the remote roots: it must find the
    // same conversations under the same identities, with no duplicate rows.
    index(&["index", "--json"]);
    assert_eq!(
        rows(),
        first,
        "a second index changed remote conversation identity"
    );
    assert_eq!(conversation_count(), i64::try_from(first.len()).unwrap());
    tracker.complete();
}
