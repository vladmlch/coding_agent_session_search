//! Characterization test for the local-only migration (Task 0 baseline).
//!
//! A user's pre-migration configuration must be tolerated, not fatal and
//! never silently rewritten: a `sources.toml` still carrying remote-sync keys
//! (`type = "ssh"`, `host`, `sync_schedule`, `path_mappings`, `platform`) and
//! legacy env vars (`CASS_EXTERNAL_EMBEDDING_URL`, `CASS_SSH_CONFIG`,
//! `CASS_SWARM_AGENT_MAIL_URL`) must not break the everyday read-only and
//! index commands, and must not be mutated as a side effect.
//!
//! This test fixes the CURRENT contract; later tasks tighten the diagnostics
//! (the plan's "diagnose only" policy: stale keys are reported, never
//! executed). Byte-level SHA-256 snapshots pin the no-rewrite guarantee:
//! `sources.toml` must be identical after every command, and `agent_search.db`
//! must be identical across the read-only commands (`cass index` is the one
//! step allowed to change it — it is the archive's writer).

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use coding_agent_search::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use coding_agent_search::storage::sqlite::SqliteStorage;
use serde_json::Value;
use sha2::{Digest, Sha256};

mod util;
use util::cass_bin;

const MARK_LOCAL: &str = "zzlegacymarker51cf";

const SOURCES_TOML: &str = r#"[[sources]]
name = "legacy-laptop"
type = "ssh"
host = "fixture.invalid"
paths = ["~/.codex/sessions"]
sync_schedule = "hourly"
platform = "linux"

[[sources.path_mappings]]
from = "/home/fixture/projects"
to = "/work/fixture/projects"
"#;

fn sha256_hex(path: &Path) -> String {
    let bytes = fs::read(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let digest = Sha256::digest(&bytes);
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

struct Fixture {
    _tmp: tempfile::TempDir,
    home: PathBuf,
    config: PathBuf,
    xdg_data: PathBuf,
    data_dir: PathBuf,
    db_path: PathBuf,
    sources_toml: PathBuf,
}

impl Fixture {
    fn cass(&self, args: &[&str]) -> Output {
        Command::new(cass_bin())
            .args(args)
            .current_dir(&self.home)
            .env_clear()
            .env("PATH", "/usr/bin:/bin:/usr/local/bin")
            .env("TERM", "dumb")
            .env("HOME", &self.home)
            .env("XDG_CONFIG_HOME", &self.config)
            .env("XDG_DATA_HOME", &self.xdg_data)
            .env("CASS_DATA_DIR", &self.data_dir)
            .env("CASS_AUTO_REFRESH", "0")
            .env("CODING_AGENT_SEARCH_NO_UPDATE_PROMPT", "1")
            // The legacy environment this test characterizes. All three point
            // at unreachable loopback/config paths: if any of them is ever
            // dereferenced, the command fails fast instead of hanging.
            .env("CASS_EXTERNAL_EMBEDDING_URL", "http://127.0.0.1:9/embed")
            .env("CASS_SSH_CONFIG", self.config.join("ssh_config"))
            .env("CASS_SWARM_AGENT_MAIL_URL", "http://127.0.0.1:9/agent-mail")
            .output()
            .expect("spawn cass")
    }

    fn cass_ok(&self, args: &[&str]) -> Output {
        let out = self.cass(args);
        assert!(
            out.status.success(),
            "cass {:?} failed: status={:?}\nstdout={}\nstderr={}",
            args,
            out.status.code(),
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        out
    }
}

fn build_fixture() -> Fixture {
    let tmp = tempfile::tempdir().expect("fixture tempdir");
    let home = tmp.path().join("home");
    let config = tmp.path().join("config");
    let xdg_data = tmp.path().join("xdg-data");
    let data_dir = tmp.path().join("data");
    for dir in [&home, &config, &xdg_data, &data_dir] {
        fs::create_dir_all(dir).expect("create fixture dir");
    }

    let cass_config_dir = config.join("cass");
    fs::create_dir_all(&cass_config_dir).expect("create cass config dir");
    let sources_toml = cass_config_dir.join("sources.toml");
    fs::write(&sources_toml, SOURCES_TOML).expect("write legacy sources.toml");
    fs::write(
        config.join("ssh_config"),
        "Host legacy-laptop\n  HostName fixture.invalid\n  User fixture\n",
    )
    .expect("write CASS_SSH_CONFIG target");

    // A populated archive, like a real install carries: one local
    // conversation written through the project's storage API.
    let db_path = data_dir.join("agent_search.db");
    {
        let storage = SqliteStorage::open(&db_path).expect("create fixture archive db");
        let agent_id = storage
            .ensure_agent(&Agent {
                id: None,
                slug: "codex".into(),
                name: "Codex".into(),
                version: Some("fixture".into()),
                kind: AgentKind::Cli,
            })
            .expect("ensure fixture agent");
        let conv = Conversation {
            id: None,
            agent_slug: "codex".into(),
            workspace: Some(PathBuf::from("/fixture/workspace")),
            external_id: Some("legacy-settings-session".into()),
            title: Some("legacy settings session".into()),
            source_path: PathBuf::from("/fixture/.codex/sessions/rollout-legacy.jsonl"),
            started_at: Some(1_700_000_000_000),
            ended_at: Some(1_700_000_002_000),
            approx_tokens: Some(32),
            metadata_json: serde_json::json!({"fixture": true}),
            messages: vec![
                Message {
                    id: None,
                    idx: 0,
                    role: MessageRole::User,
                    author: Some("user".into()),
                    created_at: Some(1_700_000_000_000),
                    content: format!("legacy settings question {MARK_LOCAL}"),
                    extra_json: serde_json::json!({}),
                    snippets: vec![],
                },
                Message {
                    id: None,
                    idx: 1,
                    role: MessageRole::Agent,
                    author: Some("agent".into()),
                    created_at: Some(1_700_000_001_000),
                    content: "legacy settings answer".into(),
                    extra_json: serde_json::json!({}),
                    snippets: vec![],
                },
            ],
            source_id: "local".into(),
            origin_host: None,
        };
        storage
            .insert_conversation_tree(agent_id, None, &conv)
            .expect("insert fixture conversation");
    }

    Fixture {
        _tmp: tmp,
        home,
        config,
        xdg_data,
        data_dir,
        db_path,
        sources_toml,
    }
}

#[test]
fn legacy_settings_are_tolerated_and_never_rewritten() {
    let fixture = build_fixture();
    let data_dir = fixture.data_dir.to_string_lossy().into_owned();

    // Setup, not measurement: a user's existing archive has a lexical index.
    // The legacy env/config is already in force for this call too.
    fixture.cass_ok(&["index", "--data-dir", &data_dir]);

    let toml_sha_setup = sha256_hex(&fixture.sources_toml);
    let mut db_sha = sha256_hex(&fixture.db_path);

    // 1. Read-only search tolerates the legacy env and config, and still
    //    serves the archive.
    let out = fixture.cass_ok(&[
        "search",
        MARK_LOCAL,
        "--mode",
        "lexical",
        "--json",
        "--data-dir",
        &data_dir,
    ]);
    let payload: Value = serde_json::from_slice(&out.stdout).expect("search json");
    assert!(
        !payload["hits"].as_array().expect("hits array").is_empty(),
        "search must return the archived conversation under legacy settings: {payload}"
    );
    assert_eq!(
        sha256_hex(&fixture.sources_toml),
        toml_sha_setup,
        "search must not rewrite sources.toml"
    );
    let after = sha256_hex(&fixture.db_path);
    assert_eq!(
        after, db_sha,
        "read-only search must not modify agent_search.db"
    );
    db_sha = after;

    // 2. `sources list` still reads the legacy config verbatim — tolerated,
    //    not dropped.
    let out = fixture.cass_ok(&["sources", "list", "--json"]);
    let payload: Value = serde_json::from_slice(&out.stdout).expect("sources list json");
    let sources = payload["sources"].as_array().expect("sources array");
    let legacy = sources
        .iter()
        .find(|s| s["name"] == "legacy-laptop")
        .expect("legacy-laptop must still be listed");
    assert_eq!(legacy["type"], "ssh");
    assert_eq!(legacy["host"], "fixture.invalid");
    assert_eq!(legacy["sync_schedule"], "hourly");
    assert_eq!(legacy["platform"], "linux");
    assert_eq!(
        sha256_hex(&fixture.sources_toml),
        toml_sha_setup,
        "sources list must not rewrite sources.toml"
    );
    let after = sha256_hex(&fixture.db_path);
    assert_eq!(
        after, db_sha,
        "read-only sources list must not modify agent_search.db"
    );
    db_sha = after;

    // 3. `cass index` is the archive's writer: it may legitimately change the
    //    DB (configured-source upsert, index bookkeeping) but must succeed
    //    and must not touch sources.toml.
    fixture.cass_ok(&["index", "--json", "--data-dir", &data_dir]);
    assert_eq!(
        sha256_hex(&fixture.sources_toml),
        toml_sha_setup,
        "index must not rewrite sources.toml"
    );
    let after = sha256_hex(&fixture.db_path);
    eprintln!(
        "legacy_settings_tolerance: index changed agent_search.db: {}",
        after != db_sha
    );
    db_sha = after;

    // 4. `status` is a read-only health check.
    fixture.cass_ok(&["status", "--json", "--data-dir", &data_dir]);
    assert_eq!(
        sha256_hex(&fixture.sources_toml),
        toml_sha_setup,
        "status must not rewrite sources.toml"
    );
    let after = sha256_hex(&fixture.db_path);
    assert_eq!(
        after, db_sha,
        "read-only status must not modify agent_search.db"
    );
}
