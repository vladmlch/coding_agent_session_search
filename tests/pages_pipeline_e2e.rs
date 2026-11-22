use assert_cmd::cargo::cargo_bin_cmd;
use coding_agent_search::model::types::{Agent, AgentKind};
use coding_agent_search::pages::bundle::{
    BundleBuilder, BundleResult, UnencryptedConfig, UnencryptedPayload,
};
use coding_agent_search::pages::export::{ExportEngine, ExportFilter, PathMode};
use coding_agent_search::pages::verify::verify_bundle;
use coding_agent_search::storage::sqlite::SqliteStorage;
use serde_json::Value;
use std::fs;
use std::path::Path;
use tempfile::TempDir;

#[path = "util/mod.rs"]
mod util;

use util::ConversationFixtureBuilder;

/// Stage an unencrypted Pages export (`config.json` + `payload/<name>`) from
/// `payload_source`, the shape `cass pages --export-only` hands to the bundler.
fn stage_unencrypted_export(payload_source: &Path, export_dir: &Path) {
    let payload_name = payload_source
        .file_name()
        .and_then(|name| name.to_str())
        .expect("payload file name");
    let payload_dir = export_dir.join("payload");
    fs::create_dir_all(&payload_dir).expect("create payload dir");
    let size_bytes =
        fs::copy(payload_source, payload_dir.join(payload_name)).expect("copy payload");
    let config = UnencryptedConfig {
        encrypted: false,
        version: "1.0.0".to_string(),
        payload: UnencryptedPayload {
            path: format!("payload/{payload_name}"),
            format: "sqlite".to_string(),
            size_bytes: Some(size_bytes),
        },
        warning: Some("UNENCRYPTED".to_string()),
    };
    fs::write(
        export_dir.join("config.json"),
        serde_json::to_vec_pretty(&config).expect("serialize export config"),
    )
    .expect("write export config");
}

struct PipelineArtifacts {
    export_db_path: std::path::PathBuf,
    bundle: BundleResult,
}

fn build_pipeline(temp_dir: &TempDir) -> PipelineArtifacts {
    let data_dir = temp_dir.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();

    // 1. Setup: Create fixtures and populate DB
    setup_db(&data_dir);

    // 2. Export (simulating `cass pages --export-only`)
    let export_staging = temp_dir.path().join("export_staging");
    fs::create_dir_all(&export_staging).unwrap();
    let export_db_path = export_staging.join("export.db");
    let source_db_path = data_dir.join("agent_search.db");

    let filter = ExportFilter {
        agents: None,
        workspaces: None,
        since: None,
        until: None,
        path_mode: PathMode::Relative,
    };

    let export_engine = ExportEngine::new(&source_db_path, &export_db_path, filter);
    let stats = export_engine
        .execute(|_, _| {}, None)
        .expect("ExportEngine execution failed");
    assert_eq!(
        stats.conversations_processed, 1,
        "Should export 1 conversation"
    );
    assert!(export_db_path.exists(), "Export database should exist");

    // 3. Stage the unencrypted export (simulating the Wizard export step)
    let export_dir = temp_dir.path().join("export_dir");
    stage_unencrypted_export(&export_db_path, &export_dir);

    assert!(export_dir.join("config.json").exists());
    assert!(export_dir.join("payload").exists());

    // 4. Bundle (simulating Bundle Step)
    let bundle_dir = temp_dir.path().join("bundle");
    let builder = BundleBuilder::new()
        .title("E2E Test Archive")
        .description("Test archive for E2E pipeline");

    let bundle = builder
        .build(&export_dir, &bundle_dir, |_, _| {})
        .expect("Bundle failed");

    assert!(bundle.site_dir.join("index.html").exists());

    PipelineArtifacts {
        export_db_path,
        bundle,
    }
}

#[test]
fn test_pages_export_pipeline_e2e() {
    let temp_dir = TempDir::new().unwrap();
    let artifacts = build_pipeline(&temp_dir);
    let bundle_root = artifacts.bundle.site_dir.parent().expect("bundle root");

    // Verify (CLI)
    // Run `cass pages --verify <bundle_root>` to validate the bundle integrity and structure
    let mut cmd = cargo_bin_cmd!("cass");
    let assert = cmd
        .arg("pages")
        .arg("--verify")
        .arg(bundle_root)
        .arg("--json")
        .assert();

    assert.success();
}

#[test]
fn test_pages_pipeline_payload_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let artifacts = build_pipeline(&temp_dir);

    // The published site carries the export database byte-for-byte.
    let published = artifacts.bundle.site_dir.join("payload").join("export.db");
    assert_eq!(
        fs::read(&artifacts.export_db_path).unwrap(),
        fs::read(&published).unwrap()
    );
}

#[test]
fn test_pages_bundle_excludes_raw_mirror_artifacts_by_default() {
    let temp_dir = TempDir::new().unwrap();
    let input_path = temp_dir.path().join("export.db");
    fs::write(&input_path, b"export database bytes").expect("write export db fixture");

    let export_dir = temp_dir.path().join("export_dir");
    stage_unencrypted_export(&input_path, &export_dir);

    let raw_secret = b"PAGES_RAW_MIRROR_SECRET_SHOULD_NOT_LEAK";
    let raw_mirror_root = export_dir.join("raw-mirror").join("v1");
    let raw_mirror_blob = raw_mirror_root
        .join("blobs")
        .join("blake3")
        .join("aa")
        .join("secret.raw");
    fs::create_dir_all(raw_mirror_blob.parent().expect("raw mirror blob parent"))
        .expect("create raw mirror blob dir");
    fs::write(&raw_mirror_blob, raw_secret).expect("write raw mirror blob fixture");

    let raw_mirror_manifest = raw_mirror_root.join("manifests").join("secret.json");
    fs::create_dir_all(
        raw_mirror_manifest
            .parent()
            .expect("raw mirror manifest parent"),
    )
    .expect("create raw mirror manifest dir");
    fs::write(
        &raw_mirror_manifest,
        br#"{"original_path":"/secret/project/session.jsonl","content":"PAGES_RAW_MIRROR_SECRET_SHOULD_NOT_LEAK"}"#,
    )
    .expect("write raw mirror manifest fixture");

    let bundle_dir = temp_dir.path().join("bundle");
    let bundle = BundleBuilder::new()
        .title("Raw Mirror Privacy")
        .description("Raw mirror privacy regression")
        .build(&export_dir, &bundle_dir, |_, _| {})
        .expect("build pages bundle");

    assert!(
        !bundle.site_dir.join("raw-mirror").exists(),
        "public Pages site must not copy raw mirror directories"
    );
    assert!(
        !bundle.private_dir.join("raw-mirror").exists(),
        "private Pages helper artifacts must not copy raw mirror directories by default"
    );

    for entry in walkdir::WalkDir::new(&bundle_dir)
        .follow_links(false)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
    {
        let bytes = fs::read(entry.path()).expect("read generated bundle file");
        assert!(
            !bytes
                .windows(raw_secret.len())
                .any(|window| window == raw_secret),
            "raw mirror bytes leaked into {}",
            entry.path().display()
        );
    }
}

#[test]
fn test_pages_config_validate_cli() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("pages-config.json");
    let config = r#"{
  "filters": {
    "agents": ["claude-code"],
    "path_mode": "relative"
  },
  "bundle": {
    "title": "Test Archive",
    "description": "CLI config validation test",
    "hide_metadata": false
  },
  "deployment": {
    "target": "local",
    "output_dir": "./cass-export",
    "repo": null,
    "branch": null
  }
}"#;

    fs::write(&config_path, config).unwrap();

    let mut cmd = cargo_bin_cmd!("cass");
    let assert = cmd
        .arg("pages")
        .arg("--config")
        .arg(&config_path)
        .arg("--validate-config")
        .arg("--json")
        .assert()
        .success();

    let stdout = String::from_utf8(assert.get_output().stdout.clone()).unwrap();
    let payload: Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(payload["valid"].as_bool(), Some(true));
}

#[test]
fn test_pages_bundle_verify_detects_corruption() {
    let temp_dir = TempDir::new().unwrap();
    let artifacts = build_pipeline(&temp_dir);
    let site_dir = &artifacts.bundle.site_dir;

    // Baseline: verify passes
    let baseline = verify_bundle(site_dir, false).expect("verify baseline");
    assert_eq!(baseline.status, "valid");

    // Corrupt the payload file
    let payload_dir = site_dir.join("payload");
    let payload = fs::read_dir(&payload_dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|path| path.is_file())
        .expect("payload file");
    fs::write(&payload, b"corrupted payload").unwrap();

    let result = verify_bundle(site_dir, false).expect("verify after corruption");
    assert_eq!(result.status, "invalid");
}

#[test]
fn test_secret_scan_gating() {
    let temp_dir = TempDir::new().unwrap();

    // Setup XDG_DATA_HOME structure
    let xdg_data_home = temp_dir.path().join("xdg_data");
    let cass_data_dir = xdg_data_home.join("coding-agent-search");
    fs::create_dir_all(&cass_data_dir).unwrap();

    setup_db_with_secret(&cass_data_dir);

    // 1. Scan secrets (report only)
    let mut cmd = cargo_bin_cmd!("cass");
    let output = cmd
        .env("XDG_DATA_HOME", &xdg_data_home)
        .arg("pages")
        .arg("--scan-secrets")
        .arg("--json")
        .output()
        .unwrap();

    if !output.status.success() {
        eprintln!("Stderr: {}", String::from_utf8_lossy(&output.stderr));
    }
    assert!(
        output.status.success(),
        "Scan should succeed in report mode"
    );

    let stdout = String::from_utf8(output.stdout).unwrap();
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("valid json output");

    let findings = json.get("findings").expect("findings field in output");
    let findings_array = findings.as_array().expect("findings should be array");
    assert!(!findings_array.is_empty(), "Should detect inserted secret");

    // Check that we found the specific type of secret (openai_key pattern)
    let found_api_key = findings_array.iter().any(|f| {
        f.get("kind")
            .and_then(|k| k.as_str())
            .map(|s| s == "openai_key")
            .unwrap_or(false)
    });
    assert!(
        found_api_key,
        "Should detect the test API key (openai_key pattern)"
    );

    // 2. Fail on secrets
    let mut cmd_fail = cargo_bin_cmd!("cass");
    cmd_fail
        .env("XDG_DATA_HOME", &xdg_data_home)
        .arg("pages")
        .arg("--scan-secrets")
        .arg("--fail-on-secrets")
        .assert()
        .failure(); // Should exit with non-zero code
}

fn setup_db(data_dir: &Path) {
    setup_db_internal(data_dir, false);
}

fn setup_db_with_secret(data_dir: &Path) {
    setup_db_internal(data_dir, true);
}

fn setup_db_internal(data_dir: &Path, include_secret: bool) {
    let db_path = data_dir.join("agent_search.db");
    if let Some(p) = db_path.parent() {
        fs::create_dir_all(p).unwrap();
    }

    // Initialize DB with schema
    let storage = SqliteStorage::open(&db_path).expect("Failed to open storage");

    // Create Agent
    let agent = Agent {
        id: None,
        slug: "claude_code".to_string(),
        name: "Claude Code".to_string(),
        version: None,
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).expect("ensure agent");

    // Create Workspace
    let workspace_path = Path::new("/home/user/projects/test");
    let workspace_id = Some(
        storage
            .ensure_workspace(workspace_path, None)
            .expect("ensure workspace"),
    );

    let content = if include_secret {
        // Use a valid OpenAI key format (sk- followed by 20+ alphanumeric chars, no extra hyphens)
        "I accidentally pasted my key: sk-TESTabcdefghijklmnopqrstuvwxyz012345"
    } else {
        "Agent response 1"
    };

    // Create a fixture conversation
    let conversation = ConversationFixtureBuilder::new("claude_code")
        .title("Test Conversation")
        .workspace(workspace_path)
        .source_path("/home/user/.claude/projects/test/session.jsonl")
        .messages(5)
        .with_content(0, "User message 1")
        .with_content(1, content)
        .build_conversation();

    // Insert into DB
    storage
        .insert_conversation_tree(agent_id, workspace_id, &conversation)
        .expect("Failed to insert conversation");
}
