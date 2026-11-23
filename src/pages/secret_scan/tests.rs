use super::detectors::{
    adjust_to_char_boundary, build_where_clause, collect_context_redactions, is_allowlisted,
    is_fully_allowlisted, redact_context, redact_report_provenance, redact_token, scan_text,
    shannon_entropy,
};
use super::report::write_human_report;
use super::scanner::{
    SecretScanCheckpoint, ensure_staged_export_has_no_sidecars, scan_database_with_cancel_check,
    table_exists,
};
use super::types::{
    BUILTIN_PATTERNS, CUSTOM_DENYLIST_PATTERN_ID, DEFAULT_ENTROPY_THRESHOLD, DEFAULT_MAX_FINDINGS,
    ENTROPY_BASE64_RE, ENTROPY_HEX_RE, REDACTED_CONTEXT, ScanContext,
};
use super::*;
use crate::franken_sync::compat::{ConnectionExt, RowExt};
use crate::franken_sync::params;
use crate::pages::sqlite_fixed_artifact_paths;
use anyhow::Result;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

fn redact_context_for_test(text: &str, start: usize, end: usize, window: usize) -> String {
    let config = SecretScanConfig::from_inputs_with_env(&[], &[], false)
        .expect("construct test secret-scan config");
    redact_context(text, start, end, window, &config)
}

// =========================================================================
// Shannon entropy tests
// =========================================================================

#[test]
fn shannon_entropy_empty_string_returns_zero() {
    assert_eq!(shannon_entropy(""), 0.0);
}

#[test]
fn shannon_entropy_single_repeated_char_returns_zero() {
    assert_eq!(shannon_entropy("aaaaaaaaaa"), 0.0);
}

#[test]
fn shannon_entropy_two_equal_chars_returns_one() {
    let e = shannon_entropy("ab");
    assert!((e - 1.0).abs() < 0.001, "expected ~1.0, got {}", e);
}

#[test]
fn shannon_entropy_high_entropy_base64() {
    // A string with many distinct chars should have high entropy
    let token = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let e = shannon_entropy(token);
    assert!(e > 4.0, "expected entropy > 4.0, got {}", e);
}

#[test]
fn shannon_entropy_hex_string() {
    let hex = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4";
    let e = shannon_entropy(hex);
    assert!(e > 3.0, "expected entropy > 3.0 for hex, got {}", e);
}

// =========================================================================
// Redact token tests
// =========================================================================

#[test]
fn redact_token_short_returns_redacted() {
    assert_eq!(redact_token("abcd"), "[redacted]");
    assert_eq!(redact_token("12345678"), "[redacted]");
}

#[test]
fn redact_token_long_is_fully_opaque() {
    let result = redact_token("sk-abcdefghijklmnop");
    assert_eq!(result, REDACTED_CONTEXT);
    assert!(!result.contains("sk"));
    assert!(!result.contains("op"));
}

#[test]
fn redact_token_nine_chars_is_fully_opaque() {
    let result = redact_token("123456789");
    assert_eq!(result, REDACTED_CONTEXT);
}

// =========================================================================
// Redact context tests
// =========================================================================

#[test]
fn redact_context_empty_text_returns_empty() {
    assert_eq!(redact_context_for_test("", 0, 0, 120), "");
}

#[test]
fn redact_context_replaces_match_with_replacement() {
    let text = "The key is sk-ABCDEFGHIJ and more";
    let start = 11;
    let end = 25;
    let result = redact_context_for_test(text, start, end, 120);
    assert!(result.contains(REDACTED_CONTEXT), "result: {}", result);
    assert!(
        !result.contains("sk-ABCDEFGHIJ"),
        "secret should be removed: {}",
        result
    );
}

#[test]
fn redact_context_match_at_start() {
    let text = "sk-SECRET rest of the text";
    let result = redact_context_for_test(text, 0, 9, 120);
    assert!(result.starts_with(REDACTED_CONTEXT), "result: {}", result);
}

#[test]
fn redact_context_match_at_end() {
    let text = "prefix sk-SECRET";
    let result = redact_context_for_test(text, 7, 16, 120);
    assert!(result.ends_with(REDACTED_CONTEXT), "result: {}", result);
}

#[test]
fn redact_context_start_beyond_text_returns_empty() {
    assert_eq!(redact_context_for_test("short", 10, 15, 120), "");
}

#[test]
fn redact_context_masks_private_key_body_and_adjacent_secret_classes() {
    let focal = "sk-ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789";
    let private_body = "b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAA";
    let jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.signature_123456789";
    let entropy = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let text = format!(
        "before {focal} middle -----BEGIN OPENSSH PRIVATE KEY-----\n{private_body}\n-----END OPENSSH PRIVATE KEY----- after {jwt} tail {entropy}"
    );
    let start = text.find(focal).expect("focal fixture offset");
    let result = redact_context_for_test(&text, start, start + focal.len(), text.len() * 2);

    for raw_secret in [focal, private_body, jwt, entropy] {
        assert!(
            !result.contains(raw_secret),
            "context leaked adjacent secret {raw_secret:?}: {result}"
        );
    }
    assert!(
        result.matches(REDACTED_CONTEXT).count() >= 3,
        "distinct secret ranges should be visibly masked: {result}"
    );
}

#[test]
fn redact_context_masks_allowlisted_and_custom_denylist_neighbors() {
    let allowlisted = "sk-ALLOWLISTabcdefghijklmnopqrstuvwxyz012345";
    let denied = "INTERNAL_SECRET_ABC123XYZ789";
    let focal = "AKIAIOSFODNN7EXAMPLE";
    let text = format!("{allowlisted} before {focal} after {denied}");
    let config = SecretScanConfig::from_inputs_with_env(
        &["sk-ALLOWLIST.*".to_string()],
        &["INTERNAL_SECRET_[A-Z0-9]+".to_string()],
        false,
    )
    .expect("construct test secret-scan config");
    let start = text.find(focal).expect("focal fixture offset");
    let result = redact_context(&text, start, start + focal.len(), text.len() * 2, &config);

    assert!(!result.contains(allowlisted), "allowlisted neighbor leaked");
    assert!(!result.contains(denied), "denylisted neighbor leaked");
}

#[test]
fn report_provenance_masks_custom_denylist_and_entropy_secrets() {
    let denied = "INTERNAL_SECRET_ABC123XYZ789";
    let entropy = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let value = Some(format!("/tmp/{denied}/safe.txt/{entropy}/session.jsonl"));
    let config = SecretScanConfig::from_inputs_with_env(
        &[],
        &["INTERNAL_SECRET_[A-Z0-9]+".to_string()],
        false,
    )
    .unwrap();

    let redacted = redact_report_provenance(&value, &config).unwrap();
    assert!(!redacted.contains(denied), "custom denylist value leaked");
    assert!(!redacted.contains(entropy), "entropy secret leaked");
    assert!(
        redacted.matches(REDACTED_CONTEXT).count() >= 2,
        "independent provenance secrets should be masked: {redacted}"
    );
}

#[test]
fn truncated_zero_finding_report_is_not_presented_as_clean() {
    let report = SecretScanReport {
        summary: SecretScanSummary {
            total: 0,
            by_severity: BTreeMap::new(),
            has_critical: false,
            truncated: true,
        },
        findings: Vec::new(),
    };
    let mut output = Vec::new();

    write_human_report(&mut output, &report, 3).unwrap();
    let output = String::from_utf8(output).unwrap();
    assert!(!output.contains("No secrets detected"), "{output}");
    assert!(output.contains("Results truncated"), "{output}");
}

// =========================================================================
// Allowlist tests
// =========================================================================

#[test]
fn is_allowlisted_returns_true_for_matching_pattern() {
    let config =
        SecretScanConfig::from_inputs_with_env(&["sk-test.*".to_string()], &[], false).unwrap();
    assert!(is_allowlisted("sk-test1234567890abcdef", &config));
}

#[test]
fn is_allowlisted_returns_false_when_no_match() {
    let config =
        SecretScanConfig::from_inputs_with_env(&["sk-test.*".to_string()], &[], false).unwrap();
    assert!(!is_allowlisted("sk-prod1234567890abcdef", &config));
}

#[test]
fn is_allowlisted_empty_list_returns_false() {
    let config = SecretScanConfig::from_inputs_with_env(&[], &[], false).unwrap();
    assert!(!is_allowlisted("anything", &config));
}

#[test]
fn structured_allowlist_requires_a_full_scalar_match() {
    let config = SecretScanConfig::from_inputs_with_env(&["SAFE".to_string()], &[], false).unwrap();
    assert!(is_fully_allowlisted("SAFE", &config));
    assert!(!is_fully_allowlisted("SAFE-real-secret", &config));
}

// =========================================================================
// Adjust to char boundary tests
// =========================================================================

#[test]
fn adjust_to_char_boundary_ascii() {
    let text = "hello";
    assert_eq!(adjust_to_char_boundary(text, 3, true), 3);
    assert_eq!(adjust_to_char_boundary(text, 3, false), 3);
}

#[test]
fn adjust_to_char_boundary_multibyte_forward() {
    let text = "héllo"; // 'é' is 2 bytes (0xC3 0xA9)
    // Index 2 is in the middle of 'é', forward should skip to next boundary
    let idx = adjust_to_char_boundary(text, 2, true);
    assert!(
        text.is_char_boundary(idx),
        "idx {} not a char boundary",
        idx
    );
}

#[test]
fn adjust_to_char_boundary_multibyte_backward() {
    let text = "héllo";
    let idx = adjust_to_char_boundary(text, 2, false);
    assert!(
        text.is_char_boundary(idx),
        "idx {} not a char boundary",
        idx
    );
}

#[test]
fn adjust_to_char_boundary_beyond_len() {
    let text = "abc";
    assert_eq!(adjust_to_char_boundary(text, 100, true), 3);
}

// =========================================================================
// Config construction tests
// =========================================================================

#[test]
fn config_from_inputs_with_valid_patterns() {
    let config = SecretScanConfig::from_inputs_with_env(
        &["allowed_.*".to_string()],
        &["denied_.*".to_string()],
        false,
    )
    .unwrap();
    assert_eq!(config.allowlist.len(), 1);
    assert_eq!(config.denylist.len(), 1);
    assert_eq!(config.entropy_threshold, DEFAULT_ENTROPY_THRESHOLD);
}

#[test]
fn config_from_inputs_with_invalid_regex_returns_error() {
    let result = SecretScanConfig::from_inputs_with_env(&["[invalid".to_string()], &[], false);
    assert!(result.is_err(), "invalid regex should return error");
}

#[test]
fn config_from_inputs_empty_lists() {
    let config = SecretScanConfig::from_inputs_with_env(&[], &[], false).unwrap();
    assert!(config.allowlist.is_empty());
    assert!(config.denylist.is_empty());
    assert_eq!(config.max_findings, DEFAULT_MAX_FINDINGS);
}

#[test]
fn table_exists_distinguishes_absence_from_invalid_schema_probe() -> Result<()> {
    let conn = crate::franken_sync::Connection::open(":memory:")?;
    conn.execute("CREATE TABLE snippets (id INTEGER PRIMARY KEY);")?;

    assert!(table_exists(&conn, "snippets")?);
    assert!(!table_exists(&conn, "not_present")?);

    let error = table_exists(&conn, "snippets; DROP TABLE snippets")
        .expect_err("invalid schema identifiers must fail closed");
    assert!(
        error.to_string().contains("Invalid SQLite identifier"),
        "unexpected invalid-identifier diagnostic: {error:#}"
    );
    Ok(())
}

#[test]
fn staged_export_scan_reads_flat_pages_schema_and_binds_digest() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let db_path = temp.path().join("export.db");
    // Match the production export flow: the staged scan target is a
    // VACUUM INTO image that is never opened read-write, so it owns no
    // WAL/journal sidecars. Building the fixture directly at `db_path`
    // would leave an `export.db-wal` and trip the sidecar refusal.
    let builder_path = temp.path().join("builder.db");
    let conn = crate::franken_sync::Connection::open(builder_path.to_string_lossy().as_ref())?;
    conn.execute_batch(
        r#"
        PRAGMA journal_mode = 'delete';
        CREATE TABLE conversations (
            id INTEGER PRIMARY KEY,
            agent TEXT NOT NULL,
            workspace TEXT,
            title TEXT,
            source_path TEXT NOT NULL,
            started_at INTEGER,
            ended_at INTEGER,
            message_count INTEGER,
            metadata_json TEXT
        );
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            conversation_id INTEGER NOT NULL,
            idx INTEGER NOT NULL,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at INTEGER,
            updated_at INTEGER,
            model TEXT,
            attachment_refs TEXT
        );
        CREATE TABLE snippets (
            id INTEGER PRIMARY KEY,
            message_id INTEGER NOT NULL,
            file_path TEXT,
            start_line INTEGER,
            end_line INTEGER,
            language TEXT,
            snippet_text TEXT
        );
        INSERT INTO conversations (
            id, agent, workspace, title, source_path, started_at,
            message_count, metadata_json
        ) VALUES (
            1, 'codex', '/tmp/project', 'safe title',
            'session.jsonl', 1700000000000, 2, '{}'
        );
        INSERT INTO messages (
            id, conversation_id, idx, role, content, created_at
        ) VALUES (
            7, 1, 0, 'user', 'credential AKIAIOSFODNN7EXAMPLE',
            1700000000000
        );
        INSERT INTO messages (
            id, conversation_id, idx, role, content, created_at, model,
            attachment_refs
        ) VALUES (
            8, 1, 1, 'assistant', 'safe content', 1700000000001,
            'safe safe safe safe safe safe safe safe safe safe AUX_MODEL_CREDENTIAL',
            'AUX_ATTACHMENT_CREDENTIAL'
        );
        INSERT INTO snippets (
            id, message_id, file_path, language, snippet_text
        ) VALUES (
            9, 8,
            'safe safe safe safe safe safe safe safe safe safe safe safe safe safe safe safe safe safe safe safe AUX_PATH_CREDENTIAL',
            'safe safe safe safe safe safe safe safe safe safe safe safe safe safe safe safe safe safe safe safe safe safe safe safe safe safe safe safe safe safe AUX_LANGUAGE_CREDENTIAL',
            'safe snippet'
        );
        "#,
    )?;
    conn.execute_batch(&format!(
        "VACUUM INTO '{}';",
        db_path.to_string_lossy().replace('\'', "''")
    ))?;
    conn.close()
        .map_err(|error| anyhow::anyhow!("close staged export builder: {error}"))?;

    let config = SecretScanConfig::from_inputs_with_env(
        &[],
        &["AUX_(?:MODEL|ATTACHMENT|PATH|LANGUAGE)_CREDENTIAL".to_string()],
        false,
    )?;
    let attestation = scan_staged_export_database(&db_path, &config)?;

    assert_eq!(attestation.artifact_sha256.len(), 64);
    assert!(!attestation.report.summary.truncated);
    let finding = attestation
        .report
        .findings
        .iter()
        .find(|finding| finding.kind == "aws_access_key_id")
        .expect("message content credential must be detected");
    assert_eq!(finding.agent.as_deref(), Some("codex"));
    assert_eq!(finding.workspace.as_deref(), Some("/tmp/project"));
    assert_eq!(finding.message_id, Some(7));
    let auxiliary_findings: Vec<_> = attestation
        .report
        .findings
        .iter()
        .filter(|finding| finding.pattern == CUSTOM_DENYLIST_PATTERN_ID)
        .collect();
    assert_eq!(
        auxiliary_findings.len(),
        4,
        "model, attachment, snippet path, and snippet language must all be scanned"
    );
    assert!(auxiliary_findings.iter().all(|finding| {
        finding.message_id == Some(8) && finding.location == SecretLocation::MessageMetadata
    }));
    Ok(())
}

#[test]
fn staged_export_scan_rejects_unbound_sqlite_sidecars() -> Result<()> {
    let config = SecretScanConfig::from_inputs_with_env(&[], &[], false)?;
    let artifact_paths = sqlite_fixed_artifact_paths(Path::new("export.db"))
        .into_iter()
        .filter(|path| !crate::pages::is_fsqlite_namespace_identity_record(path));
    for relative_path in artifact_paths {
        let temp = tempfile::tempdir()?;
        let db_path = temp.path().join("export.db");
        let sentinel_path = temp.path().join(relative_path);
        let artifact_label = sentinel_path.display().to_string();
        std::fs::write(&db_path, b"main-file sentinel")?;
        std::fs::write(&sentinel_path, b"unbound sidecar sentinel")?;

        let error = scan_staged_export_database(&db_path, &config)
            .expect_err("a main-file digest must not attest an unbound SQLite sidecar");
        let diagnostic = format!("{error:#}");
        assert!(
            diagnostic.contains(&artifact_label),
            "diagnostic omitted rejected artifact {artifact_label}: {diagnostic}"
        );
        assert!(
            diagnostic.contains("main-file-only artifact attestation"),
            "unexpected error for {artifact_label}: {diagnostic}"
        );
        assert_eq!(
            std::fs::read(&sentinel_path)?,
            b"unbound sidecar sentinel",
            "attestation rejection mutated sentinel {artifact_label}"
        );
    }

    // FrankenSQLite namespace identity records are unavoidable runtime
    // droppings, not payload: their presence alone must not block a
    // main-file-only attestation.
    for suffix in ["-fsqlite-ns-gate", "-fsqlite-ns-use"] {
        let temp = tempfile::tempdir()?;
        let db_path = temp.path().join("export.db");
        std::fs::write(&db_path, b"main-file sentinel")?;
        std::fs::write(
            temp.path().join(format!("export.db{suffix}")),
            b"identity record",
        )?;
        ensure_staged_export_has_no_sidecars(&db_path, "before verification")?;
    }
    Ok(())
}

#[test]
fn staged_export_scan_rejects_parallel_wal_segments_without_mutation() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let db_path = temp.path().join("export.db");
    let segment_path = temp.path().join("export.db-wal-seg-not-an-epoch");
    let config = SecretScanConfig::from_inputs_with_env(&[], &[], false)?;
    std::fs::write(&db_path, b"main-file sentinel")?;
    std::fs::write(&segment_path, b"parallel WAL segment sentinel")?;

    let error = scan_staged_export_database(&db_path, &config)
        .expect_err("an unbound parallel WAL segment must block attestation");
    let diagnostic = format!("{error:#}");
    assert!(
        diagnostic.contains(&segment_path.display().to_string()),
        "diagnostic omitted rejected WAL segment: {diagnostic}"
    );
    assert_eq!(std::fs::read(&db_path)?, b"main-file sentinel");
    assert_eq!(
        std::fs::read(&segment_path)?,
        b"parallel WAL segment sentinel"
    );
    Ok(())
}

#[test]
fn cancellation_at_conversation_page_boundary_stops_before_message_schema() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let db_path = temp.path().join("scan.db");
    let conn = crate::franken_sync::Connection::open(db_path.to_string_lossy().as_ref())?;
    conn.execute_batch(
        r#"
        CREATE TABLE agents (
            id INTEGER PRIMARY KEY,
            slug TEXT NOT NULL
        );
        CREATE TABLE workspaces (
            id INTEGER PRIMARY KEY,
            path TEXT NOT NULL
        );
        CREATE TABLE conversations (
            id INTEGER PRIMARY KEY,
            agent_id INTEGER,
            workspace_id INTEGER,
            title TEXT,
            source_path TEXT NOT NULL,
            started_at INTEGER,
            metadata_json TEXT
        );
        INSERT INTO agents (id, slug) VALUES (1, 'codex');
        INSERT INTO workspaces (id, path) VALUES (1, '/tmp/project');
        INSERT INTO conversations (
            id, agent_id, workspace_id, title, source_path, started_at, metadata_json
        ) VALUES (
            1, 1, 1, 'safe title', '/tmp/project/session.jsonl', 1700000000000, '{}'
        );
        "#,
    )?;
    drop(conn);

    let filters = SecretScanFilters {
        agents: None,
        workspaces: None,
        since_ts: None,
        until_ts: None,
    };
    let config = SecretScanConfig::from_inputs_with_env(&[], &[], false)?;
    let error = scan_database_with_cancel_check(&db_path, &filters, &config, None, |checkpoint| {
        checkpoint == SecretScanCheckpoint::AfterConversations
    })
    .expect_err("page-boundary cancellation must abort instead of probing messages");

    assert!(
        error.to_string().contains("Secret scan cancelled"),
        "the missing messages table must never be probed after cancellation: {error:#}"
    );
    Ok(())
}

#[test]
fn database_scan_uses_one_snapshot_across_all_payload_surfaces() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let db_path = temp.path().join("scan.db");
    let setup = crate::franken_sync::Connection::open(db_path.to_string_lossy().as_ref())?;
    setup.execute_batch(
        r#"
        PRAGMA journal_mode = WAL;
        CREATE TABLE conversations (
            id INTEGER PRIMARY KEY,
            agent TEXT NOT NULL,
            workspace TEXT,
            title TEXT,
            source_path TEXT NOT NULL,
            metadata_json TEXT
        );
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            conversation_id INTEGER NOT NULL,
            idx INTEGER NOT NULL,
            role TEXT NOT NULL,
            content TEXT NOT NULL
        );
        INSERT INTO conversations (
            id, agent, workspace, title, source_path, metadata_json
        ) VALUES (
            1, 'codex', '/tmp/project', 'ordinary title',
            '/tmp/project/session.jsonl', '{}'
        );
        INSERT INTO messages (id, conversation_id, idx, role, content)
        VALUES (7, 1, 0, 'user', 'ordinary message');
        "#,
    )?;
    drop(setup);

    let writer = crate::franken_sync::Connection::open(db_path.to_string_lossy().as_ref())?;
    let mutation_committed = std::cell::Cell::new(false);
    let filters = SecretScanFilters {
        agents: None,
        workspaces: None,
        since_ts: None,
        until_ts: None,
    };
    let config = SecretScanConfig::from_inputs_with_env(&[], &[], false)?;

    let report = scan_database_with_cancel_check(
        &db_path,
        &filters,
        &config,
        None,
        |checkpoint| {
            if checkpoint == SecretScanCheckpoint::BeforeMessagePage && !mutation_committed.get() {
                writer
                    .execute(
                        "UPDATE messages SET content = 'credential AKIAIOSFODNN7EXAMPLE' WHERE id = 7",
                    )
                    .expect("concurrent WAL update must commit during the scan");
                mutation_committed.set(true);
            }
            false
        },
    )?;

    assert!(
        mutation_committed.get(),
        "the test must mutate the database after the scan snapshot is established"
    );
    assert_eq!(
        report.summary.total, 0,
        "one scan must not combine pre-update conversations with post-update messages"
    );
    let live_content: String = writer.query_row_map(
        "SELECT content FROM messages WHERE id = 7",
        params![],
        |row| row.get_typed(0),
    )?;
    assert_eq!(live_content, "credential AKIAIOSFODNN7EXAMPLE");
    Ok(())
}

// =========================================================================
// Scan text tests (via scan_database with crafted DB)
// =========================================================================

#[test]
fn builtin_patterns_aws_access_key_detected() -> Result<(), String> {
    let pattern = &BUILTIN_PATTERNS[0]; // aws_access_key_id
    let cases = [
        ("Found key AKIAIOSFODNN7EXAMPLE in config", true),
        ("Found temporary key ASIAIOSFODNN7EXAMPLE in config", true),
        ("ASIAIOSFODNN7EXAMPL", false),
        ("asiaiosfodnn7example", false),
    ];
    match cases
        .into_iter()
        .find(|(input, expected)| pattern.regex.is_match(input) != *expected)
    {
        Some((input, expected)) => Err(format!(
            "AWS access-key scanner returned {} for {input:?}, expected {expected}",
            pattern.regex.is_match(input)
        )),
        None => Ok(()),
    }
}

#[test]
fn builtin_patterns_github_pat_detected() {
    let text = "token ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij";
    let pattern = &BUILTIN_PATTERNS[2]; // github_pat
    assert!(pattern.regex.is_match(text), "should detect GitHub PAT");
}

#[test]
fn builtin_patterns_anthropic_key_detected() {
    let text = "sk-ant-ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefgh";
    let pattern = &BUILTIN_PATTERNS[4]; // anthropic_key
    assert!(pattern.regex.is_match(text), "should detect Anthropic key");
}

#[test]
fn builtin_patterns_jwt_detected() {
    let text = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abc123";
    let pattern = &BUILTIN_PATTERNS[5]; // jwt
    assert!(pattern.regex.is_match(text), "should detect JWT");
}

#[test]
fn builtin_patterns_private_key_detected() -> Result<(), String> {
    let pattern = &BUILTIN_PATTERNS[6]; // private_key
    for kind in [
        "RSA PRIVATE KEY",
        "EC PRIVATE KEY",
        "DSA PRIVATE KEY",
        "OPENSSH PRIVATE KEY",
        "PRIVATE KEY",
        "ENCRYPTED PRIVATE KEY",
        "PGP PRIVATE KEY BLOCK",
    ] {
        let text = format!("-----BEGIN {kind}-----\nMIIE...");
        if !pattern.regex.is_match(&text) {
            return Err(format!("post-hoc scanner missed {kind}"));
        }
    }
    Ok(())
}

#[test]
fn builtin_patterns_database_url_detected() {
    let text = "database_url=postgres://user:pass@host:5432/db";
    let pattern = &BUILTIN_PATTERNS[7]; // database_url
    assert!(pattern.regex.is_match(text), "should detect database URL");
}

#[test]
fn builtin_patterns_generic_api_key_detected() {
    let text = "api_key=abcdefgh12345678";
    let pattern = &BUILTIN_PATTERNS[8]; // generic_api_key
    assert!(
        pattern.regex.is_match(text),
        "should detect generic API key"
    );
}

#[test]
fn builtin_patterns_safe_text_not_detected() {
    let safe_text = "This is a normal message about Rust programming.";
    for pattern in BUILTIN_PATTERNS.iter() {
        assert!(
            !pattern.regex.is_match(safe_text),
            "pattern {} should not match safe text",
            pattern.id,
        );
    }
}

// =========================================================================
// Severity ranking tests
// =========================================================================

#[test]
fn severity_rank_ordering() {
    assert!(SecretSeverity::Critical.rank() < SecretSeverity::High.rank());
    assert!(SecretSeverity::High.rank() < SecretSeverity::Medium.rank());
    assert!(SecretSeverity::Medium.rank() < SecretSeverity::Low.rank());
}

#[test]
fn severity_label_values() {
    assert_eq!(SecretSeverity::Critical.label(), "critical");
    assert_eq!(SecretSeverity::High.label(), "high");
    assert_eq!(SecretSeverity::Medium.label(), "medium");
    assert_eq!(SecretSeverity::Low.label(), "low");
}

#[test]
fn severity_summary_serializes_in_stable_rank_order() {
    let mut by_severity = BTreeMap::new();
    by_severity.insert(SecretSeverity::Low, 1);
    by_severity.insert(SecretSeverity::Critical, 1);
    by_severity.insert(SecretSeverity::Medium, 1);
    by_severity.insert(SecretSeverity::High, 1);
    let encoded = serde_json::to_string(&SecretScanSummary {
        total: 4,
        by_severity,
        has_critical: true,
        truncated: false,
    })
    .unwrap();

    let positions = ["critical", "high", "medium", "low"]
        .map(|label| encoded.find(label).expect("serialized severity label"));
    assert!(
        positions.windows(2).all(|pair| pair[0] < pair[1]),
        "{encoded}"
    );
}

// =========================================================================
// SecretLocation label tests
// =========================================================================

#[test]
fn location_labels() {
    assert_eq!(
        SecretLocation::ConversationTitle.label(),
        "conversation.title"
    );
    assert_eq!(
        SecretLocation::ConversationMetadata.label(),
        "conversation.metadata"
    );
    assert_eq!(SecretLocation::MessageContent.label(), "message.content");
    assert_eq!(SecretLocation::MessageMetadata.label(), "message.metadata");
}

// =========================================================================
// Build where clause tests
// =========================================================================

#[test]
fn build_where_clause_empty_filters() {
    let filters = SecretScanFilters {
        agents: None,
        workspaces: None,
        since_ts: None,
        until_ts: None,
    };
    let (clause, params) = build_where_clause(&filters).unwrap();
    assert!(clause.is_empty(), "empty filters should give empty clause");
    assert!(params.is_empty());
}

#[test]
fn build_where_clause_with_agent_filter() {
    let filters = SecretScanFilters {
        agents: Some(vec!["claude".to_string(), "codex".to_string()]),
        workspaces: None,
        since_ts: None,
        until_ts: None,
    };
    let (clause, params) = build_where_clause(&filters).unwrap();
    assert!(
        clause.contains("COALESCE(a.slug, 'unknown') IN"),
        "clause: {}",
        clause
    );
    assert_eq!(params.len(), 2);
}

#[test]
fn build_where_clause_with_time_range() {
    let filters = SecretScanFilters {
        agents: None,
        workspaces: None,
        since_ts: Some(1000),
        until_ts: Some(2000),
    };
    let (clause, params) = build_where_clause(&filters).unwrap();
    assert!(clause.contains("c.started_at >="), "clause: {}", clause);
    assert!(clause.contains("c.started_at <="), "clause: {}", clause);
    assert_eq!(params.len(), 2);
}

#[test]
fn build_where_clause_with_workspace_filter() {
    let filters = SecretScanFilters {
        agents: None,
        workspaces: Some(vec![PathBuf::from("/home/user/project")]),
        since_ts: None,
        until_ts: None,
    };
    let (clause, params) = build_where_clause(&filters).unwrap();
    assert!(clause.contains("w.path IN"), "clause: {}", clause);
    assert_eq!(params.len(), 1);
}

#[test]
fn build_where_clause_empty_agent_list_matches_nothing() {
    let filters = SecretScanFilters {
        agents: Some(vec![]),
        workspaces: None,
        since_ts: None,
        until_ts: None,
    };
    let (clause, _) = build_where_clause(&filters).unwrap();
    assert!(
        clause.contains("1=0"),
        "empty agent list should match nothing: {}",
        clause
    );
}

#[test]
fn build_where_clause_empty_workspace_list_matches_nothing() {
    let filters = SecretScanFilters {
        agents: None,
        workspaces: Some(vec![]),
        since_ts: None,
        until_ts: None,
    };
    let (clause, _) = build_where_clause(&filters).unwrap();
    assert!(
        clause.contains("1=0"),
        "empty workspace list should match nothing: {}",
        clause
    );
}

// =========================================================================
// Entropy regex tests
// =========================================================================

#[test]
fn entropy_base64_regex_matches_long_strings() {
    assert!(ENTROPY_BASE64_RE.is_match("ABCDEFGHIJKLMNOPQRSTuv"));
    assert!(!ENTROPY_BASE64_RE.is_match("short"));
}

#[test]
fn entropy_hex_regex_matches_32_plus_chars() {
    assert!(ENTROPY_HEX_RE.is_match("a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4"));
    assert!(!ENTROPY_HEX_RE.is_match("a1b2c3d4"));
}

// =========================================================================
// Edge case tests — malformed input robustness (br-ig84)
// =========================================================================

#[test]
fn scan_text_empty_text_no_findings() {
    let config = SecretScanConfig::from_inputs_with_env(&[], &[], false).unwrap();
    let ctx = ScanContext {
        agent: None,
        workspace: None,
        source_path: None,
        conversation_id: None,
        message_id: None,
        message_idx: None,
    };
    let mut findings = Vec::new();
    let mut seen = Vec::new();
    let mut truncated = false;

    scan_text(
        "",
        SecretLocation::MessageContent,
        &ctx,
        &config,
        &mut findings,
        &mut seen,
        &mut truncated,
    );
    assert!(findings.is_empty());
    assert!(!truncated);
}

#[test]
fn scan_text_already_truncated_skips() {
    let config = SecretScanConfig::from_inputs_with_env(&[], &[], false).unwrap();
    let ctx = ScanContext {
        agent: None,
        workspace: None,
        source_path: None,
        conversation_id: None,
        message_id: None,
        message_idx: None,
    };
    let mut findings = Vec::new();
    let mut seen = Vec::new();
    let mut truncated = true; // pre-set

    scan_text(
        "sk-test1234567890abcdefghijklmnopqr",
        SecretLocation::MessageContent,
        &ctx,
        &config,
        &mut findings,
        &mut seen,
        &mut truncated,
    );
    assert!(findings.is_empty(), "should skip when already truncated");
}

#[test]
fn scan_text_denylist_always_critical() {
    let config =
        SecretScanConfig::from_inputs_with_env(&[], &["FORBIDDEN_TOKEN_.*".to_string()], false)
            .unwrap();
    let ctx = ScanContext {
        agent: Some("test".to_string()),
        workspace: None,
        source_path: None,
        conversation_id: Some(1),
        message_id: Some(1),
        message_idx: Some(0),
    };
    let mut findings = Vec::new();
    let mut seen = Vec::new();
    let mut truncated = false;

    scan_text(
        "prefix FORBIDDEN_TOKEN_abc suffix",
        SecretLocation::MessageContent,
        &ctx,
        &config,
        &mut findings,
        &mut seen,
        &mut truncated,
    );

    assert_eq!(findings.len(), 1);
    assert_eq!(findings[0].severity, SecretSeverity::Critical);
    assert_eq!(findings[0].kind, "denylist");
}

#[test]
fn scan_text_allowlist_suppresses_builtin_match() {
    let config =
        SecretScanConfig::from_inputs_with_env(&["sk-test.*".to_string()], &[], false).unwrap();
    let ctx = ScanContext {
        agent: None,
        workspace: None,
        source_path: None,
        conversation_id: Some(1),
        message_id: Some(1),
        message_idx: Some(0),
    };
    let mut findings = Vec::new();
    let mut seen = Vec::new();
    let mut truncated = false;

    scan_text(
        "sk-testABCDEFGHIJKLMNOPQRSTUVWXYZ12345",
        SecretLocation::MessageContent,
        &ctx,
        &config,
        &mut findings,
        &mut seen,
        &mut truncated,
    );

    // The openai_key pattern should match but be suppressed by allowlist
    assert!(
        !findings.iter().any(|f| f.kind == "openai_key"),
        "allowlisted key should be suppressed"
    );
}

#[test]
fn scan_text_deduplicates_findings() {
    let config = SecretScanConfig::from_inputs_with_env(&[], &[], false).unwrap();
    let ctx = ScanContext {
        agent: None,
        workspace: None,
        source_path: None,
        conversation_id: Some(1),
        message_id: Some(1),
        message_idx: Some(0),
    };
    let mut findings = Vec::new();
    let mut seen = Vec::new();
    let mut truncated = false;

    // Scan same text twice — same context, so duplicates should be skipped
    let text = "sk-ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789";
    scan_text(
        text,
        SecretLocation::MessageContent,
        &ctx,
        &config,
        &mut findings,
        &mut seen,
        &mut truncated,
    );
    let count_after_first = findings.len();

    scan_text(
        text,
        SecretLocation::MessageContent,
        &ctx,
        &config,
        &mut findings,
        &mut seen,
        &mut truncated,
    );
    assert_eq!(
        findings.len(),
        count_after_first,
        "duplicate findings should be skipped"
    );
}

#[test]
fn scan_text_preserves_distinct_occurrences_with_identical_display_masks() {
    let config =
        SecretScanConfig::from_inputs_with_env(&[], &[r"AA[0-9]{8}ZZ".to_string()], false).unwrap();
    let ctx = ScanContext {
        agent: None,
        workspace: None,
        source_path: Some("fixture.jsonl".to_string()),
        conversation_id: Some(1),
        message_id: Some(1),
        message_idx: Some(0),
    };
    let mut findings = Vec::new();
    let mut seen = Vec::new();
    let mut truncated = false;

    scan_text(
        "AA11111111ZZ AA22222222ZZ",
        SecretLocation::MessageContent,
        &ctx,
        &config,
        &mut findings,
        &mut seen,
        &mut truncated,
    );

    assert_eq!(findings.len(), 2);
    assert_eq!(findings[0].match_redacted, findings[1].match_redacted);
}

#[test]
fn scan_text_reports_overlapping_detectors_as_one_occurrence() {
    let config = SecretScanConfig::from_inputs_with_env(&[], &[], false).unwrap();
    let ctx = ScanContext {
        agent: None,
        workspace: None,
        source_path: Some("fixture.jsonl".to_string()),
        conversation_id: Some(1),
        message_id: Some(1),
        message_idx: Some(0),
    };
    let mut findings = Vec::new();
    let mut seen = Vec::new();
    let mut truncated = false;

    scan_text(
        "token=ghp_abcdefghijklmnopqrstuvwxyz0123456789",
        SecretLocation::MessageContent,
        &ctx,
        &config,
        &mut findings,
        &mut seen,
        &mut truncated,
    );

    assert_eq!(findings.len(), 1);
    assert_eq!(findings[0].kind, "github_pat");
}

#[test]
fn overlapping_later_detector_upgrades_to_highest_severity_at_capacity() {
    let mut config = SecretScanConfig::from_inputs_with_env(&[], &[], false).unwrap();
    config.max_findings = 1;
    let ctx = ScanContext {
        agent: None,
        workspace: None,
        source_path: Some("fixture.jsonl".to_string()),
        conversation_id: Some(1),
        message_id: Some(1),
        message_idx: Some(0),
    };
    let mut findings = Vec::new();
    let mut seen = Vec::new();
    let mut truncated = false;

    scan_text(
        r#"authorization="Bearer abcdefgh12345678""#,
        SecretLocation::MessageContent,
        &ctx,
        &config,
        &mut findings,
        &mut seen,
        &mut truncated,
    );

    assert_eq!(findings.len(), 1);
    assert_eq!(findings[0].kind, "bearer_token");
    assert_eq!(findings[0].severity, SecretSeverity::High);
    assert!(
        !truncated,
        "an overlapping upgrade must not consume capacity"
    );
}

#[test]
fn context_range_storage_excludes_matches_outside_the_window() {
    let config =
        SecretScanConfig::from_inputs_with_env(&[], &[r"SECRET[0-9]{4}".to_string()], false)
            .unwrap();
    let text = (0..1_000)
        .map(|index| format!("SECRET{index:04} "))
        .collect::<String>();
    let focal = text.find("SECRET0500").unwrap();
    let ranges = collect_context_redactions(
        &text,
        &config,
        focal.saturating_sub(1),
        focal + "SECRET0500".len() + 1,
    );

    assert_eq!(ranges.len(), 1, "only the bounded context span is retained");
    assert_eq!(ranges[0].start, focal);
}

#[test]
fn scan_text_max_findings_truncates() {
    let mut config =
        SecretScanConfig::from_inputs_with_env(&[], &["LONG_SECRET_\\d+".to_string()], false)
            .unwrap();
    config.max_findings = 3;

    let ctx = ScanContext {
        agent: None,
        workspace: None,
        source_path: None,
        conversation_id: Some(1),
        message_id: Some(1),
        message_idx: Some(0),
    };
    let mut findings = Vec::new();
    let mut seen = Vec::new();
    let mut truncated = false;

    // Distinct source spans remain distinct findings even though report
    // redaction intentionally gives every match the same opaque form.
    let text = "LONG_SECRET_001 LONG_SECRET_002 LONG_SECRET_003 LONG_SECRET_004 LONG_SECRET_005";
    scan_text(
        text,
        SecretLocation::MessageContent,
        &ctx,
        &config,
        &mut findings,
        &mut seen,
        &mut truncated,
    );

    assert!(
        findings.len() <= 3,
        "should cap at max_findings: {}",
        findings.len()
    );
    assert!(truncated, "should set truncated flag");
}

#[test]
fn scan_text_pure_alphabetic_base64_skipped() {
    // Pure alphabetic strings (CamelCase identifiers) should NOT trigger entropy detection
    let config = SecretScanConfig::from_inputs_with_env(&[], &[], false).unwrap();
    let ctx = ScanContext {
        agent: None,
        workspace: None,
        source_path: None,
        conversation_id: Some(1),
        message_id: Some(1),
        message_idx: Some(0),
    };
    let mut findings = Vec::new();
    let mut seen = Vec::new();
    let mut truncated = false;

    // This is a pure alphabetic string — should be skipped by the heuristic
    let text = "SecretScanConfigFromInputsWithEnvTest";
    scan_text(
        text,
        SecretLocation::MessageContent,
        &ctx,
        &config,
        &mut findings,
        &mut seen,
        &mut truncated,
    );

    assert!(
        !findings.iter().any(|f| f.kind == "high_entropy_base64"),
        "pure alphabetic strings should not trigger entropy detection"
    );
}
