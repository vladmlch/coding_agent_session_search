//! Integration tests verifying franken_agent_detection (FAD) completeness in cass.
//!
//! Bead: coding_agent_session_search-3arih
//!
//! These tests ensure:
//! 1. Zero hardcoded agent paths remain in production code (all come from FAD)
//! 2. Connector detection round-trip works correctly
//! 3. FAD probe paths stay consistent with the connector set
//! 4. Agent counts are consistent across all APIs
//! 5. No detection-only connectors remain: every probed slug has a parser

use coding_agent_search::indexer::get_connector_factories;
use std::collections::HashSet;
use std::path::Path;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Map FAD internal slug → cass public slug.
fn public_slug(fad_slug: &str) -> &str {
    match fad_slug {
        "claude" => "claude_code",
        other => other,
    }
}

/// Map cass public slug → FAD internal slug.
fn fad_slug(public: &str) -> &str {
    match public {
        "claude_code" => "claude",
        other => other,
    }
}

/// Collect factory slugs as the FAD internal names (e.g. "claude", "codex").
fn factory_fad_slugs() -> HashSet<String> {
    get_connector_factories()
        .into_iter()
        .map(|(slug, _)| slug.to_string())
        .collect()
}

/// Collect probe path slugs from FAD.
fn probe_slugs() -> HashSet<String> {
    franken_agent_detection::default_probe_paths_tilde()
        .into_iter()
        .map(|(slug, _)| slug.to_string())
        .collect()
}

/// The connector registry under the FAD features cass enables, as FAD
/// internal slugs. Phase 0 kept exactly these ten; probe paths, the detection
/// report and the parser factories all enumerate this set.
const KEPT_FAD_SLUGS: &[&str] = &[
    "antigravity",
    "chatgpt",
    "claude",
    "codex",
    "cursor",
    "devin",
    "gemini",
    "opencode",
    "qoder",
    "qwen",
];

fn kept_fad_slugs() -> HashSet<String> {
    KEPT_FAD_SLUGS
        .iter()
        .map(|slug| (*slug).to_string())
        .collect()
}

/// Extract a function body from source code, including the braces.
fn extract_function_body(source: &str, fn_prefix: &str) -> String {
    let start = source
        .find(fn_prefix)
        .unwrap_or_else(|| panic!("function not found: {fn_prefix}"));
    let after = &source[start..];
    let open = after
        .find('{')
        .unwrap_or_else(|| panic!("no opening brace for: {fn_prefix}"));
    let mut depth = 0usize;
    let mut end_idx = None;
    for (i, ch) in after[open..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    end_idx = Some(open + i + 1);
                    break;
                }
            }
            _ => {}
        }
    }
    let end = end_idx.unwrap_or_else(|| panic!("no closing brace for: {fn_prefix}"));
    after[open..end].to_string()
}

// ---------------------------------------------------------------------------
// Test 1: Connector detection round-trip
// ---------------------------------------------------------------------------

/// Every connector factory must produce a valid connector that can run detect().
/// Root paths returned must be absolute or tilde-relative.
#[test]
fn connector_factories_all_instantiate_and_detect() {
    let factories = get_connector_factories();

    assert_eq!(
        factories.len(),
        KEPT_FAD_SLUGS.len(),
        "Expected {} connector factories, got {}",
        KEPT_FAD_SLUGS.len(),
        factories.len()
    );

    let mut slugs = Vec::new();
    for (slug, factory_fn) in &factories {
        let connector = factory_fn();
        let result = connector.detect();
        for root in &result.root_paths {
            let s = root.to_string_lossy();
            assert!(
                root.is_absolute() || s.starts_with("~/"),
                "connector {slug} returned non-absolute root path: {}",
                root.display()
            );
        }
        slugs.push(*slug);
        eprintln!(
            "  [OK] {slug}: detected={}, {} root path(s)",
            result.detected,
            result.root_paths.len()
        );
    }

    // No duplicate slugs
    let unique: HashSet<&str> = slugs.iter().copied().collect();
    assert_eq!(unique.len(), slugs.len(), "Duplicate factory slugs");

    // Required connectors always present
    for required in KEPT_FAD_SLUGS {
        assert!(
            unique.contains(required),
            "Required connector '{required}' missing"
        );
    }
}

/// Feature-gated connectors (chatgpt, cursor, opencode) are available because
/// cass enables those features in Cargo.toml.
#[test]
fn feature_gated_connectors_available() {
    let slugs = factory_fad_slugs();
    for gated in ["chatgpt", "cursor", "opencode"] {
        assert!(
            slugs.contains(gated),
            "Feature-gated connector '{gated}' not found. \
             Check Cargo.toml enables the feature for franken-agent-detection"
        );
    }
    assert_eq!(slugs.len(), 10, "Expected 10 connector factories");
    assert_eq!(slugs, kept_fad_slugs(), "connector factory set drifted");
}

// ---------------------------------------------------------------------------
// Test 2: Probe path coverage
// ---------------------------------------------------------------------------

/// Every factory connector must have a corresponding probe path entry, and
/// every probed slug must have a factory (no detection-only connectors remain).
#[test]
fn probe_paths_cover_all_factory_connectors() {
    let factory = factory_fad_slugs();
    let probes = probe_slugs();

    let missing: Vec<_> = factory.difference(&probes).cloned().collect();
    assert!(
        missing.is_empty(),
        "Factory connectors missing from probe paths: {missing:?}"
    );
    let detection_only: Vec<_> = probes.difference(&factory).cloned().collect();
    assert!(
        detection_only.is_empty(),
        "Probed connectors without a factory: {detection_only:?}"
    );

    eprintln!(
        "  Factory: {} connectors, Probes: {} entries",
        factory.len(),
        probes.len()
    );
}

/// All probe paths must use tilde-relative format (suitable for SSH).
#[test]
fn probe_paths_are_tilde_relative() {
    let paths = franken_agent_detection::default_probe_paths_tilde();
    for (slug, paths) in &paths {
        assert!(!paths.is_empty(), "Connector '{slug}' has no probe paths");
        for path in paths {
            assert!(
                path.starts_with("~/"),
                "Probe path for '{slug}' is not tilde-relative: {path}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Test 3: detect_installed_agents API
// ---------------------------------------------------------------------------

/// detect_installed_agents must return a valid report covering all KNOWN_CONNECTORS.
#[test]
fn detect_installed_agents_report_structure() {
    let opts = franken_agent_detection::AgentDetectOptions {
        include_undetected: true,
        ..Default::default()
    };
    let report = franken_agent_detection::detect_installed_agents(&opts)
        .expect("detect_installed_agents should not fail");

    // Must cover the full KNOWN_CONNECTORS set.
    assert_eq!(
        report.installed_agents.len(),
        KEPT_FAD_SLUGS.len(),
        "Expected {} agents in report, got {}",
        KEPT_FAD_SLUGS.len(),
        report.installed_agents.len()
    );
    assert_eq!(report.format_version, 1);
    // Bead 7k7pl: generated_at must be an ISO-8601-shaped timestamp,
    // not just any non-empty string. A regression that stored
    // `"unknown"` or a Unix-epoch integer as a string would slip past
    // `!is_empty()` while breaking downstream parsers that expect
    // RFC-3339. Check the canonical prefix shape: "YYYY-MM-DD" (10
    // chars, dashes at positions 4 and 7).
    assert!(
        report.generated_at.len() >= 10,
        "generated_at must be an ISO-8601 timestamp (>= 10 chars); got {:?}",
        report.generated_at
    );
    let bytes = report.generated_at.as_bytes();
    assert!(
        bytes[4] == b'-' && bytes[7] == b'-',
        "generated_at must have dashes at positions 4 and 7 (YYYY-MM-DD prefix); \
         got {:?}",
        report.generated_at
    );
    assert_eq!(report.summary.total_count, report.installed_agents.len());

    let slugs: HashSet<&str> = report
        .installed_agents
        .iter()
        .map(|e| e.slug.as_str())
        .collect();

    // Every kept connector must appear
    for slug in KEPT_FAD_SLUGS {
        assert!(
            slugs.contains(slug),
            "Connector '{slug}' missing from detection report"
        );
    }

    for entry in &report.installed_agents {
        assert!(!entry.slug.is_empty());
        eprintln!(
            "  [{}] {}: {} path(s)",
            if entry.detected { "YES" } else { " no" },
            entry.slug,
            entry.root_paths.len()
        );
    }
}

// ---------------------------------------------------------------------------
// Test 4: Agent count consistency
// ---------------------------------------------------------------------------

/// Detection, probe and factory APIs should enumerate the same set of
/// connectors (no detection-only connectors remain).
#[test]
fn agent_counts_consistent_across_apis() {
    let factory = factory_fad_slugs();
    let probes = probe_slugs();

    let opts = franken_agent_detection::AgentDetectOptions {
        include_undetected: true,
        ..Default::default()
    };
    let report = franken_agent_detection::detect_installed_agents(&opts)
        .expect("detect_installed_agents should not fail");
    let detection: HashSet<String> = report
        .installed_agents
        .iter()
        .map(|e| e.slug.clone())
        .collect();

    // Detection and probe should enumerate the same slugs
    assert_eq!(
        detection.len(),
        probes.len(),
        "Detection ({}) and probe ({}) counts differ",
        detection.len(),
        probes.len()
    );

    // Factory and detection enumerate the same slugs
    for slug in &factory {
        assert!(
            detection.contains(slug),
            "Factory connector '{slug}' not in detection report"
        );
    }
    let without_factory: Vec<_> = detection.difference(&factory).cloned().collect();
    assert!(
        without_factory.is_empty(),
        "Detected connectors without a factory: {without_factory:?}"
    );

    eprintln!(
        "  Factories: {}, Detection: {}, Probes: {}",
        factory.len(),
        detection.len(),
        probes.len()
    );
}

// ---------------------------------------------------------------------------
// Test 5: Source code audit — no hardcoded paths
// ---------------------------------------------------------------------------

fn is_src_test_only(rel: &str) -> bool {
    rel.ends_with("_tests.rs")
        || rel.ends_with(".tests.rs")
        || rel.contains("/tests/")
        || rel.ends_with("/tests.rs")
}

fn collect_non_test_rs_files(manifest_dir: &Path, dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    if !dir.is_dir() {
        return;
    }
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("should read {}: {e}", dir.display()))
        .filter_map(|e| e.ok())
        .collect();
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            collect_non_test_rs_files(manifest_dir, &path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            let rel = path
                .strip_prefix(manifest_dir)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            if !is_src_test_only(&rel) {
                out.push(path);
            }
        }
    }
}

fn lib_split_production_files() -> Vec<std::path::PathBuf> {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let src_dir = manifest_dir.join("src");
    let mut files = vec![src_dir.join("lib.rs")];
    for subdir in ["cli_root", "lib_doctor", "cli_app"] {
        collect_non_test_rs_files(manifest_dir, &src_dir.join(subdir), &mut files);
    }
    files
}

fn find_diagnostics_connector_paths_body() -> String {
    let mut found: Option<String> = None;
    for path in lib_split_production_files() {
        let src = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("should read {}: {e}", path.display()));
        let mut offset = 0usize;
        for line in src.split_inclusive('\n') {
            let trimmed = line.trim_start();
            let is_fn_decl = trimmed.starts_with("fn diagnostics_connector_paths(")
                || (trimmed.starts_with("pub")
                    && trimmed.contains(" fn diagnostics_connector_paths("));
            if is_fn_decl {
                assert!(
                    found.is_none(),
                    "multiple diagnostics_connector_paths definitions found (including {})",
                    path.display()
                );
                found = Some(extract_function_body(
                    &src[offset..],
                    "fn diagnostics_connector_paths(",
                ));
            }
            offset += line.len();
        }
    }
    found.expect("function not found: fn diagnostics_connector_paths(")
}

/// diagnostics_connector_paths() in the lib crate must use FAD's detect_installed_agents,
/// not hardcoded path lists.
#[test]
fn diagnostics_connector_paths_is_dynamic() {
    let body = find_diagnostics_connector_paths_body();

    assert!(
        body.contains("detect_installed_agents"),
        "diagnostics_connector_paths should call detect_installed_agents"
    );
    for banned in [
        ".claude/projects",
        ".codex/sessions",
        ".gemini",
        ".qwen/projects",
        ".qoder/projects",
        ".local/share/opencode",
        "com.openai.chat",
    ] {
        assert!(
            !body.contains(banned),
            "diagnostics_connector_paths still hardcodes: {banned}"
        );
    }
}

// ---------------------------------------------------------------------------
// Test 6: Slug mapping consistency
// ---------------------------------------------------------------------------

/// FAD uses "claude" internally; cass exposes "claude_code" publicly.
#[test]
fn slug_mappings_are_correct() {
    let factory = factory_fad_slugs();

    // FAD factory uses "claude", not "claude_code"
    assert!(factory.contains("claude"));
    assert!(!factory.contains("claude_code"));
    assert_eq!(public_slug("claude"), "claude_code");
    assert_eq!(fad_slug("claude_code"), "claude");
}

// ---------------------------------------------------------------------------
// Test 7: New agent auto-discovery mechanism
// ---------------------------------------------------------------------------

/// Documents and verifies the auto-discovery integration points.
/// When a new connector is added to FAD, cass picks it up automatically via:
/// - get_connector_factories() (indexing)
/// - detect_installed_agents() (diagnostics)
/// - default_probe_paths_tilde() (FAD probe-path table; cass no longer
///   consumes it — the remote probe script was removed with remote sync)
#[test]
fn new_agent_auto_discovery_documented() {
    let factories = get_connector_factories();
    let probes = franken_agent_detection::default_probe_paths_tilde();
    let report = franken_agent_detection::detect_installed_agents(
        &franken_agent_detection::AgentDetectOptions {
            include_undetected: true,
            ..Default::default()
        },
    )
    .expect("detection should work");

    assert!(!factories.is_empty());
    assert!(!probes.is_empty());
    assert!(!report.installed_agents.is_empty());

    eprintln!("\n  Auto-Discovery Verification:");
    eprintln!(
        "  - Factories: {} connectors (with parsers)",
        factories.len()
    );
    eprintln!("  - Probe paths: {} entries (all known)", probes.len());
    eprintln!(
        "  - Detection: {} entries ({} detected on this machine)",
        report.installed_agents.len(),
        report.summary.detected_count
    );
    eprintln!("  - Adding a connector to FAD auto-discovers in cass.");
}

/// GH449: the Devin factory exists even when its SQLite parser is compiled out.
/// Exercise the persisted provider format through the factory and real CLI so
/// slug enumeration alone cannot certify support again.
mod devin_ingestion {
    use super::*;
    use coding_agent_search::connectors::{ScanContext, ScanRoot};
    use coding_agent_search::franken_sync::compat::ConnectionExt;
    use coding_agent_search::franken_sync::{Connection, params};
    use serde_json::{Value, json};
    use std::fs;
    use std::time::Duration;

    fn seed_store(path: &Path) {
        let conn = Connection::open(path.to_string_lossy().as_ref()).expect("create Devin store");
        // Schema and JSON shapes from the published FAD 0.2.3 Devin connector,
        // independently populated here with branch, hidden and empty sessions.
        conn.execute_batch(
            "BEGIN;
             CREATE TABLE sessions (
                 id TEXT PRIMARY KEY, title TEXT, working_directory TEXT,
                 model TEXT, agent_mode TEXT, created_at INTEGER,
                 last_activity_at INTEGER, main_chain_id INTEGER,
                 hidden INTEGER NOT NULL DEFAULT 0
             );
             CREATE TABLE message_nodes (
                 node_id INTEGER PRIMARY KEY, session_id TEXT NOT NULL,
                 parent_node_id INTEGER, chat_message TEXT, created_at INTEGER
             );
             INSERT INTO sessions VALUES
                 ('kept', 'Devin branch repair', '/work/devin', 'model', 'agent',
                  1700000000, 1700000060, 5, 0),
                 ('hidden', 'Retired', NULL, NULL, NULL, 1700000000, 1700000060, 7, 1),
                 ('empty', NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0);
             COMMIT;",
        )
        .expect("seed Devin schema");
        for (session, id, parent, message) in [
            (
                "kept",
                1,
                None,
                json!({"role":"system", "content":"excludedpolicy"}),
            ),
            (
                "kept",
                2,
                Some(1),
                json!({"role":"user", "content":"devinneedle fix the branch",
                "images":[{"data":"excludedimagepayload", "mime_type":"image/png"}]}),
            ),
            (
                "kept",
                3,
                Some(2),
                json!({"role":"assistant", "content":"devinneedle inspect",
                "thinking":{"thinking":"follow the parent chain", "signature":"signature"},
                "tool_calls":[{"id":"call-1", "index":0, "kind":"function", "name":"shell",
                    "arguments":{"command":"git status"}}]}),
            ),
            (
                "kept",
                4,
                Some(3),
                json!({"role":"tool", "content":"devinneedle clean tree", "tool_call_id":"call-1"}),
            ),
            (
                "kept",
                5,
                Some(4),
                json!({"role":"assistant", "content":"devinneedle repaired"}),
            ),
            (
                "kept",
                6,
                Some(2),
                json!({"role":"assistant", "content":"excludedabandonedbranch"}),
            ),
            (
                "hidden",
                7,
                None,
                json!({"role":"user", "content":"excludedhiddensession"}),
            ),
        ] {
            conn.execute_compat(
                "INSERT INTO message_nodes
                 (session_id, node_id, parent_node_id, chat_message, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    session,
                    id,
                    parent,
                    message.to_string(),
                    1_700_000_000_i64 + i64::from(id)
                ],
            )
            .expect("insert Devin message node");
        }
    }

    fn source_bundle_bytes(db: &Path) -> Vec<Option<Vec<u8>>> {
        ["", "-wal", "-shm"]
            .into_iter()
            .map(|suffix| {
                let path = std::path::PathBuf::from(format!("{}{suffix}", db.display()));
                path.exists()
                    .then(|| fs::read(path).expect("source bundle bytes"))
            })
            .collect()
    }

    fn cass_command(home: &Path, data: &Path) -> std::process::Command {
        let mut cmd = std::process::Command::new(assert_cmd::cargo::cargo_bin!("cass"));
        cmd.env_clear()
            .env("HOME", home)
            .env("USERPROFILE", home)
            .env("PATH", "")
            .env("XDG_DATA_HOME", home.join(".local/share"))
            .env("XDG_CONFIG_HOME", home.join(".config"))
            .env("CASS_DEVIN_DATA_ROOT", home.join("sessions.db"))
            .env("CASS_IGNORE_SOURCES_CONFIG", "1")
            .env("CASS_AUTO_REFRESH", "0")
            .env("RUST_MIN_STACK", "134217728")
            .current_dir(home)
            .arg("--data-dir")
            .arg(data);
        if let Ok(system_root) = dotenvy::var("SystemRoot") {
            cmd.env("SystemRoot", system_root);
        }
        cmd
    }

    fn cass(home: &Path, data: &Path) -> assert_cmd::Command {
        let mut cmd = assert_cmd::Command::from_std(cass_command(home, data));
        cmd.timeout(Duration::from_secs(120));
        cmd
    }

    #[test]
    fn devin_factory_reads_main_chain_without_mutating_source() {
        let home = tempfile::tempdir().expect("isolated home");
        let db = home.path().join("sessions.db");
        seed_store(&db);
        let before = fs::read(&db).expect("source bytes");
        let (_, factory) = get_connector_factories()
            .into_iter()
            .find(|(slug, _)| *slug == "devin")
            .expect("Devin factory");
        let ctx = ScanContext::with_roots(
            home.path().to_path_buf(),
            vec![ScanRoot::local(db.clone())],
            None,
        )
        .without_local_default_stores();
        let conversations = factory().scan(&ctx).expect("real Devin scan");
        assert_eq!(
            conversations.len(),
            1,
            "disabled parser or wrong branch selection"
        );
        let conversation = &conversations[0];
        assert_eq!(conversation.agent_slug, "devin");
        assert_eq!(conversation.external_id.as_deref(), Some("kept"));
        assert_eq!(
            conversation.workspace.as_deref(),
            Some(Path::new("/work/devin"))
        );
        assert_eq!(conversation.source_path, db.join("kept"));
        assert_eq!(conversation.started_at, Some(1_700_000_000_000));
        assert_eq!(conversation.metadata["off_chain_nodes"], 1);
        assert_eq!(conversation.messages.len(), 4);
        assert_eq!(
            conversation
                .messages
                .iter()
                .map(|m| m.role.as_str())
                .collect::<Vec<_>>(),
            ["user", "assistant", "tool", "assistant"]
        );
        for message in &conversation.messages {
            assert!(!message.content.contains("excluded"));
        }
        assert_eq!(fs::read(&db).expect("source after scan"), before);
    }

    // review 2026-11-02 F5: a store that cannot be read must not look like an
    // empty successful scan (the host would then advance the watermark).
    #[test]
    fn devin_unreadable_database_fails_the_scan() {
        let home = tempfile::tempdir().expect("isolated home");
        let db = home.path().join("sessions.db");
        fs::write(&db, b"this is not a sqlite database").expect("write junk store");
        let (_, factory) = get_connector_factories()
            .into_iter()
            .find(|(slug, _)| *slug == "devin")
            .expect("Devin factory");
        let ctx = ScanContext::with_roots(
            home.path().to_path_buf(),
            vec![ScanRoot::local(db.clone())],
            None,
        )
        .without_local_default_stores();
        assert!(factory().scan(&ctx).is_err());
    }

    #[test]
    fn devin_cli_indexes_searches_and_reopens_without_duplicates() {
        let home = tempfile::tempdir().expect("isolated home");
        let db = home.path().join("sessions.db");
        let data = home.path().join("cass-data");
        seed_store(&db);
        let mut before = source_bundle_bytes(&db);
        let mut source_writer = None;
        for round in 0..4 {
            if round == 2 {
                let conn = Connection::open(db.to_string_lossy().as_ref()).expect("Devin writer");
                conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA wal_autocheckpoint = 0;")
                    .expect("live WAL store");
                let now = chrono::Utc::now().timestamp();
                conn.execute_compat(
                    "INSERT INTO message_nodes VALUES (8, 'kept', 5, ?1, ?2)",
                    params![
                        json!({"role":"user", "content":"devinneedle followup"}).to_string(),
                        now
                    ],
                )
                .expect("append provider turn");
                conn.execute_compat(
                    "UPDATE sessions SET main_chain_id = 8, last_activity_at = ?1 WHERE id = 'kept'",
                    params![now],
                )
                .expect("advance provider main chain");
                assert!(
                    fs::metadata(db.with_extension("db-wal"))
                        .expect("live WAL")
                        .len()
                        > 32
                );
                source_writer = Some(conn);
                before = source_bundle_bytes(&db);
            }
            cass(home.path(), &data)
                .args(if round < 2 {
                    vec!["index", "--full", "--json"]
                } else {
                    vec!["index", "--json"]
                })
                .assert()
                .success();
            let output = cass(home.path(), &data)
                .args([
                    "search",
                    "devinneedle",
                    "--mode",
                    "lexical",
                    "--agent",
                    "devin",
                    "--json",
                    "--limit",
                    "20",
                ])
                .assert()
                .success()
                .get_output()
                .stdout
                .clone();
            let result: Value = serde_json::from_slice(&output).expect("search JSON");
            let hits = result["hits"].as_array().expect("search hits");
            assert_eq!(hits.len(), if round < 2 { 4 } else { 5 }, "{result}");
            for hit in hits {
                assert_eq!(hit["agent"], "devin");
                assert_eq!(
                    hit["source_path"],
                    db.join("kept").to_string_lossy().as_ref()
                );
            }
            assert_eq!(
                source_bundle_bytes(&db),
                before,
                "source bundle changed in round {round}"
            );
        }
        drop(source_writer);
    }

    #[test]
    fn devin_same_second_wal_append_and_replay_preserve_five_messages() {
        use coding_agent_search::storage::sqlite::SqliteStorage;

        const SECOND: i64 = 1_700_001_000;
        for streaming in ["0", "1"] {
            let home = tempfile::tempdir().expect("isolated Devin precision home");
            let db = home.path().join("sessions.db");
            let data = home.path().join("cass-data");
            seed_store(&db);
            cass(home.path(), &data)
                .env("CASS_STREAMING_INDEX", streaming)
                .args(["index", "--full", "--json"])
                .assert()
                .success();

            let save_watermark = || {
                let storage = SqliteStorage::open(&data.join("agent_search.db"))
                    .expect("open canonical metadata");
                storage
                    .set_last_scan_ts(SECOND * 1000 + 500)
                    .expect("global watermark");
                storage
                    .set_connector_last_scan_ts("devin", SECOND * 1000 + 500)
                    .expect("Devin watermark");
                storage.close().expect("close canonical writer before CLI");
            };
            save_watermark();
            let writer = Connection::open(db.to_string_lossy().as_ref()).expect("Devin WAL writer");
            writer
                .execute_batch("PRAGMA journal_mode = WAL; PRAGMA wal_autocheckpoint = 0;")
                .expect("retain live WAL");
            writer
                .execute_compat(
                    "INSERT INTO message_nodes VALUES (8, 'kept', 5, ?1, ?2)",
                    params![
                        json!({"role":"user", "content":"devinneedle followup"}).to_string(),
                        SECOND
                    ],
                )
                .expect("append boundary-second turn");
            writer
                .execute_compat(
                    "UPDATE sessions SET main_chain_id = 8, last_activity_at = ?1 WHERE id = 'kept'",
                    params![SECOND],
                )
                .expect("advance boundary-second chain");
            writer
                .execute_compat(
                    "INSERT INTO sessions VALUES ('prior', 'prior second', NULL, NULL, NULL, ?1, ?1, 9, 0)",
                    params![SECOND - 1],
                )
                .expect("insert eligible older negative control");
            writer
                .execute_compat(
                    "INSERT INTO message_nodes VALUES (9, 'prior', NULL, ?1, ?2)",
                    params![
                        json!({"role":"user", "content":"priorsecondneedle"}).to_string(),
                        SECOND - 1
                    ],
                )
                .expect("insert prior-second control message");
            assert!(
                fs::metadata(db.with_extension("db-wal"))
                    .expect("live WAL")
                    .len()
                    > 32
            );
            let before = source_bundle_bytes(&db);
            let (_, factory) = get_connector_factories()
                .into_iter()
                .find(|(slug, _)| *slug == "devin")
                .expect("Devin factory");
            let mut ctx = ScanContext::with_roots(
                home.path().to_path_buf(),
                vec![ScanRoot::local(db.clone())],
                None,
            )
            .without_local_default_stores();
            assert_eq!(
                factory().scan(&ctx).expect("both visible controls").len(),
                2
            );
            // This is the cutoff selected from SECOND*1000+500 by the
            // production watermark map, independently covered by its unit test.
            ctx.since_ts = Some(SECOND * 1000);
            let eligible = factory().scan(&ctx).expect("native boundary cutoff");
            assert_eq!(eligible.len(), 1, "prior second must remain excluded");
            assert_eq!(eligible[0].external_id.as_deref(), Some("kept"));
            assert_eq!(eligible[0].messages.len(), 5);

            for replay in 0..2 {
                // Persist an exact half-second watermark, avoiding wall-clock
                // timing or sleeps. Reset it for replay so both invocations
                // must parse the same boundary-second provider state.
                if replay > 0 {
                    save_watermark();
                }
                cass(home.path(), &data)
                    .env("CASS_STREAMING_INDEX", streaming)
                    .args(["index", "--json"])
                    .assert()
                    .success();
                // FAD 0.3.11's source-boundary contract (upstream 27a5429)
                // routes devin through the durable-source lane: an unchanged
                // store is skipped and a changed one re-emits whole, so the
                // below-cutoff 'prior' control is indexed once on the first
                // re-scan too — the per-session watermark gate stays pinned
                // by the direct FAD scan above. Replay must duplicate neither.
                for (query, expected) in [("devinneedle", 5), ("priorsecondneedle", 1)] {
                    let output = cass(home.path(), &data)
                        .args([
                            "search", query, "--mode", "lexical", "--agent", "devin", "--json",
                            "--limit", "20",
                        ])
                        .assert()
                        .success()
                        .get_output()
                        .stdout
                        .clone();
                    let result: Value = serde_json::from_slice(&output).expect("search JSON");
                    assert_eq!(
                        result["budget"]["timed_out"].as_bool(),
                        Some(false),
                        "a timed-out search cannot prove the boundary-second counts: {result}"
                    );
                    assert_eq!(
                        result["hits"].as_array().expect("hits").len(),
                        expected,
                        "streaming={streaming} replay={replay}: {result}"
                    );
                }
                assert_eq!(
                    source_bundle_bytes(&db),
                    before,
                    "source conservation during replay"
                );
            }
            drop(writer);
        }
    }

    #[test]
    fn devin_file_override_watch_ingests_wal_only_commit_without_touching_source() {
        use std::process::{Child, Stdio};
        use std::time::Instant;

        struct WatchChild(Child);

        impl Drop for WatchChild {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }

        let home = tempfile::tempdir().expect("isolated Devin watch home");
        let db = home.path().join("sessions.db");
        let data = home.path().join("cass-data");
        seed_store(&db);
        // Keep the provider's writer open throughout startup, the commit, and
        // reader verification. Closing it could checkpoint the DB and conceal
        // a watcher that observes only the main database file.
        let writer = Connection::open(db.to_string_lossy().as_ref()).expect("live Devin writer");
        writer
            .execute_batch("PRAGMA journal_mode = WAL; PRAGMA wal_autocheckpoint = 0;")
            .expect("provider WAL mode before watcher startup");
        let initial_bundle = source_bundle_bytes(&db);
        let initial_modified = fs::metadata(&db).unwrap().modified().unwrap();
        let stdout_path = home.path().join("devin-watch.stdout");
        let stderr_path = home.path().join("devin-watch.stderr");
        let watch_logs = || {
            format!(
                "stdout:\n{}\nstderr:\n{}",
                fs::read_to_string(&stdout_path).unwrap_or_default(),
                fs::read_to_string(&stderr_path).unwrap_or_default(),
            )
        };
        let mut watch = WatchChild(
            cass_command(home.path(), &data)
                // JSON mode suppresses the INFO readiness signal unless requested.
                .arg("--verbose")
                .args(["index", "--watch", "--watch-interval", "1", "--json"])
                .env("RUST_LOG", "info")
                .stdout(Stdio::from(fs::File::create(&stdout_path).unwrap()))
                .stderr(Stdio::from(fs::File::create(&stderr_path).unwrap()))
                .spawn()
                .expect("start real Devin watcher"),
        );
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let logs = watch_logs();
            if logs.contains("watch mode: minimum interval between scan cycles") {
                break;
            }
            assert!(
                watch.0.try_wait().unwrap().is_none(),
                "watch exited before installing notifications: {logs}"
            );
            assert!(Instant::now() < deadline, "watch startup timed out: {logs}");
            std::thread::sleep(Duration::from_millis(100));
        }
        let search = |query: &str| {
            let output = cass(home.path(), &data)
                .args([
                    "search",
                    query,
                    "--mode",
                    "lexical",
                    "--agent",
                    "devin",
                    "--json",
                    "--no-maintenance",
                    "--timeout",
                    "3000",
                    "--limit",
                    "20",
                ])
                .timeout(Duration::from_secs(5))
                .output()
                .expect("read-only search while watching");
            assert!(
                output.status.success(),
                "search failed: {}\n{}",
                String::from_utf8_lossy(&output.stderr),
                watch_logs(),
            );
            let result: Value = serde_json::from_slice(&output.stdout).expect("search JSON");
            assert_ne!(
                result.pointer("/budget/timed_out").and_then(Value::as_bool),
                Some(true),
                "a timed-out search cannot prove absence: {result}"
            );
            assert!(result["hits"].is_array(), "search hits missing: {result}");
            result
        };
        assert_eq!(search("devinneedle")["hits"].as_array().unwrap().len(), 4);
        assert_eq!(source_bundle_bytes(&db), initial_bundle);
        assert_eq!(
            fs::metadata(&db).unwrap().modified().unwrap(),
            initial_modified
        );

        // Capture provider activity before a delayed commit. Its timestamp must
        // precede the WAL event by more than a whole second, otherwise rounding
        // the filesystem watermark could accidentally make this test pass.
        let now = chrono::Utc::now().timestamp();
        std::thread::sleep(Duration::from_millis(2100));
        assert!(chrono::Utc::now().timestamp() > now + 1);
        let mut committed_bundle = initial_bundle.clone();
        // The second commit deliberately retains the older activity timestamp
        // after the first callback has persisted its filesystem watermark.
        for (node_id, parent_id, needle) in [
            (8_i64, 5_i64, "devinwalneedle"),
            (9, 8, "devindelayedwalneedle"),
        ] {
            writer
                .execute_batch("BEGIN;")
                .expect("begin provider append");
            writer
                .execute_compat(
                    "INSERT INTO message_nodes VALUES (?1, 'kept', ?2, ?3, ?4)",
                    params![
                        node_id,
                        parent_id,
                        json!({"role":"user", "content":format!("{needle} new turn")}).to_string(),
                        now
                    ],
                )
                .expect("append provider node");
            writer
                .execute_compat(
                    "UPDATE sessions SET main_chain_id = ?1, last_activity_at = ?2 WHERE id = 'kept'",
                    params![node_id, now],
                )
                .expect("advance live main chain");
            writer
                .execute_batch("COMMIT;")
                .expect("commit provider WAL");
            let next_bundle = source_bundle_bytes(&db);
            assert_eq!(next_bundle[0], initial_bundle[0], "commit touched main DB");
            assert_eq!(
                fs::metadata(&db).unwrap().modified().unwrap(),
                initial_modified
            );
            assert_ne!(next_bundle[1], committed_bundle[1], "WAL did not change");
            assert!(next_bundle[1].as_ref().is_some_and(|wal| wal.len() > 32));
            committed_bundle = next_bundle;

            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                assert!(
                    watch.0.try_wait().unwrap().is_none(),
                    "watch exited: {}",
                    watch_logs(),
                );
                let result = search(needle);
                assert_eq!(
                    source_bundle_bytes(&db),
                    committed_bundle,
                    "reader mutated source"
                );
                assert_eq!(
                    fs::metadata(&db).unwrap().modified().unwrap(),
                    initial_modified
                );
                let hits = result["hits"].as_array().unwrap();
                if !hits.is_empty() {
                    assert_eq!(hits.len(), 1, "duplicate WAL ingestion: {result}");
                    assert_eq!(hits[0]["agent"], "devin");
                    assert_eq!(
                        hits[0]["source_path"],
                        db.join("kept").to_string_lossy().as_ref()
                    );
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "WAL-only commit never became searchable: {result}\n{}",
                    watch_logs(),
                );
                std::thread::sleep(Duration::from_millis(200));
            }
            if node_id == 8 {
                let deadline = Instant::now() + Duration::from_secs(10);
                loop {
                    let watermark = fs::read(data.join("watch_state.json"))
                        .ok()
                        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                        .and_then(|state| state.pointer("/m/dv").and_then(Value::as_i64));
                    if watermark.is_some_and(|timestamp| timestamp > (now + 1) * 1000) {
                        break;
                    }
                    assert!(
                        Instant::now() < deadline,
                        "first watch callback did not persist its event watermark: {watermark:?}\n{}",
                        watch_logs()
                    );
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }
        drop(watch);
        assert_eq!(source_bundle_bytes(&db), committed_bundle);
        assert_eq!(
            fs::metadata(&db).unwrap().modified().unwrap(),
            initial_modified
        );
        drop(writer);
    }

    #[test]
    fn devin_reads_older_nullable_schema_and_rejects_unreadable_stores() {
        use franken_agent_detection::connectors::devin::DevinConnector;

        let home = tempfile::tempdir().expect("isolated source stores");
        let db = home.path().join("sessions.db");
        let conn = Connection::open(db.to_string_lossy().as_ref()).expect("older Devin store");
        conn.execute_batch(
            r#"CREATE TABLE sessions (
                id TEXT PRIMARY KEY, title TEXT, working_directory TEXT,
                model TEXT, agent_mode TEXT, created_at INTEGER,
                last_activity_at INTEGER, main_chain_id INTEGER
             );
             CREATE TABLE message_nodes (
                node_id INTEGER PRIMARY KEY, session_id TEXT NOT NULL,
                parent_node_id INTEGER, chat_message TEXT, created_at INTEGER
             );
             INSERT INTO sessions VALUES ('legacy', NULL, NULL, NULL, NULL, NULL, NULL, 1);
             INSERT INTO message_nodes VALUES
                (1, 'legacy', NULL, '{"role":"user","content":"older Devin turn"}', 1700000000);"#,
        )
        .expect("older schema without hidden column");
        drop(conn);
        let before = source_bundle_bytes(&db);
        let conversations = DevinConnector::extract_from_sqlite(&db, None).expect("older scan");
        assert_eq!(conversations.len(), 1);
        assert_eq!(conversations[0].title.as_deref(), Some("older Devin turn"));
        assert_eq!(conversations[0].workspace, None);
        assert_eq!(conversations[0].started_at, Some(1_700_000_000_000));
        assert_eq!(source_bundle_bytes(&db), before);

        let missing = home.path().join("missing.db");
        assert!(DevinConnector::extract_from_sqlite(&missing, None).is_err());
        assert!(
            !missing.exists(),
            "read-only missing store must not be created"
        );
        let corrupt = home.path().join("corrupt.db");
        fs::write(&corrupt, b"invalid provider database").expect("corrupt fixture");
        assert!(DevinConnector::extract_from_sqlite(&corrupt, None).is_err());
        assert_eq!(
            fs::read(corrupt).expect("corrupt source preserved"),
            b"invalid provider database"
        );
    }
}
