use super::*;
use crate::connectors::{DiscoveredSourceRole, ScanRoot};
use serde_json::json;
use tempfile::tempdir;

type TestResult = Result<(), Box<dyn std::error::Error>>;

macro_rules! verify {
    ($condition:expr $(,)?) => {
        if !$condition {
            return Err(format!("verification failed: {}", stringify!($condition)).into());
        }
    };
    ($condition:expr, $($arg:tt)+) => {
        if !$condition {
            return Err(format!($($arg)+).into());
        }
    };
}

macro_rules! verify_eq {
    ($left:expr, $right:expr $(,)?) => {{
        let left = &$left;
        let right = &$right;
        if !std::cmp::PartialEq::eq(left, right) {
            return Err(format!(
                "verification failed: {} equals {}; left={left:?}, right={right:?}",
                stringify!($left),
                stringify!($right),
            )
            .into());
        }
    }};
    ($left:expr, $right:expr, $($arg:tt)+) => {{
        let left = &$left;
        let right = &$right;
        if !std::cmp::PartialEq::eq(left, right) {
            return Err(format!($($arg)+).into());
        }
    }};
}

macro_rules! verify_ne {
    ($left:expr, $right:expr $(,)?) => {{
        let left = &$left;
        let right = &$right;
        if std::cmp::PartialEq::eq(left, right) {
            return Err(format!(
                "verification failed: {} differs from {}; both={left:?}",
                stringify!($left),
                stringify!($right),
            )
            .into());
        }
    }};
    ($left:expr, $right:expr, $($arg:tt)+) => {{
        let left = &$left;
        let right = &$right;
        if std::cmp::PartialEq::eq(left, right) {
            return Err(format!($($arg)+).into());
        }
    }};
}

// --- per-provider classification --------------------------------------

#[test]
fn cursor_lock_is_retryable_lock_busy_not_a_zero_import() -> TestResult {
    let d = classify(
        "cursor",
        "~/Library/.../state.vscdb",
        IngestFailureKind::SourceLocked,
    );
    verify_eq!(d.disposition, SourceIngestDisposition::Locked);
    verify!(d.retryable, "a live-app lock is transient and retryable");
    verify!(d.disposition.is_explicit_limitation());
    verify!(
        d.safe_next_action.contains("NOT a zero-message import"),
        "a lock must never read as a successful empty import: {}",
        d.safe_next_action
    );
    Ok(())
}

#[test]
fn chatgpt_encrypted_is_unsupported_not_silent_and_legacy_still_indexes() -> TestResult {
    let d = classify(
        "chatgpt",
        "~/.../com.openai.chat",
        IngestFailureKind::EncryptedUnsupported,
    );
    verify_eq!(d.disposition, SourceIngestDisposition::UnsupportedEncrypted);
    verify!(!d.retryable);
    verify!(d.disposition.is_explicit_limitation());
    verify!(d.safe_next_action.contains("legacy unencrypted"));
    verify!(d.safe_next_action.contains("never a fake success"));
    Ok(())
}

#[test]
fn keychain_unavailable_is_retryable_from_interactive_session() -> TestResult {
    let d = classify(
        "chatgpt",
        "~/.../com.openai.chat",
        IngestFailureKind::KeychainUnavailable,
    );
    verify_eq!(d.disposition, SourceIngestDisposition::UnsupportedEncrypted);
    verify!(d.retryable);
    verify!(d.safe_next_action.contains("headless"));
    Ok(())
}

#[test]
fn malformed_json_line_quarantines_the_line_and_preserves_the_rest() -> TestResult {
    let d = classify(
        "codex",
        "rollout-x.jsonl",
        IngestFailureKind::MalformedJsonLine,
    )
    .with_line(42);
    verify_eq!(d.disposition, SourceIngestDisposition::PartiallyIndexed);
    verify_eq!(d.line_or_row, Some(42));
    verify!(
        d.safe_next_action.contains("never aborted"),
        "a malformed line must not abort the whole file"
    );
    verify_eq!(d.severity, IngestSeverity::Warning);
    Ok(())
}

#[test]
fn truncated_session_indexes_up_to_truncation() -> TestResult {
    let d = classify(
        "claude_code",
        "session.jsonl",
        IngestFailureKind::TruncatedSession,
    );
    verify_eq!(d.disposition, SourceIngestDisposition::PartiallyIndexed);
    Ok(())
}

#[test]
fn amp_filename_assumption_violation_is_skipped_not_misparsed() -> TestResult {
    let d = classify(
        "amp",
        "weird-名前.json",
        IngestFailureKind::FilenameAssumptionViolated,
    );
    verify_eq!(d.disposition, SourceIngestDisposition::Skipped);
    verify!(d.disposition.is_explicit_limitation());
    verify!(d.safe_next_action.contains("non-ASCII"));
    Ok(())
}

#[test]
fn unreadable_source_is_an_error_but_retryable() -> TestResult {
    let d = classify(
        "gemini",
        "~/.gemini/tmp",
        IngestFailureKind::UnreadableSource,
    );
    verify_eq!(d.severity, IngestSeverity::Error);
    verify_eq!(d.disposition, SourceIngestDisposition::Skipped);
    verify!(d.retryable);
    Ok(())
}

#[test]
fn every_failure_kind_has_a_nonempty_safe_action_and_no_destructive_op() -> TestResult {
    let kinds = [
        IngestFailureKind::ExternalIdCollisionRisk,
        IngestFailureKind::MalformedJsonLine,
        IngestFailureKind::TruncatedSession,
        IngestFailureKind::SourceLocked,
        IngestFailureKind::EncryptedUnsupported,
        IngestFailureKind::KeychainUnavailable,
        IngestFailureKind::FilenameAssumptionViolated,
        IngestFailureKind::UnreadableSource,
        IngestFailureKind::UnparseableSource,
        IngestFailureKind::SourceOverReadBudget,
        IngestFailureKind::UnsupportedStoreFormat,
    ];
    let destructive = ["rm ", "--delete", "--purge", "reset", "drop ", "rm -rf"];
    for kind in kinds {
        let d = classify("aider", "/some/path", kind);
        verify!(
            !d.safe_next_action.is_empty(),
            "{} has no action",
            kind.as_str()
        );
        let lower = d.safe_next_action.to_ascii_lowercase();
        for bad in destructive {
            verify!(
                !lower.contains(bad),
                "{} safe action references a destructive op {bad:?}: {}",
                kind.as_str(),
                d.safe_next_action
            );
        }
    }
    Ok(())
}

// --- Aider external-id disambiguation ---------------------------------

#[test]
fn distinct_workspaces_never_collide_on_the_same_base_id() -> TestResult {
    let a = disambiguated_external_id("aider-history", "/home/me/project-a");
    let b = disambiguated_external_id("aider-history", "/home/me/project-b");
    verify_ne!(
        a,
        b,
        "distinct project workspaces must not merge under one id"
    );
    verify!(a.starts_with("aider-history@"));
    Ok(())
}

#[test]
fn same_workspace_and_base_id_is_stable() -> TestResult {
    let a = disambiguated_external_id("aider-history", "/home/me/project-a");
    let b = disambiguated_external_id("aider-history", "/home/me/project-a");
    verify_eq!(a, b, "disambiguation must be deterministic");
    Ok(())
}

#[test]
fn empty_workspace_returns_base_id_unchanged() -> TestResult {
    verify_eq!(disambiguated_external_id("base", ""), "base");
    Ok(())
}

#[test]
fn diagnostic_can_carry_disambiguated_ids() -> TestResult {
    let canonical = disambiguated_external_id("h", "/ws/proj");
    let d = classify(
        "aider",
        "/ws/proj/.aider.chat.history.md",
        IngestFailureKind::ExternalIdCollisionRisk,
    )
    .with_workspace("/ws/proj")
    .with_ids("h", canonical.clone());
    verify_eq!(d.external_id.as_deref(), Some("h"));
    verify_eq!(d.canonical_id.as_deref(), Some(canonical.as_str()));
    verify_eq!(d.workspace.as_deref(), Some("/ws/proj"));
    Ok(())
}

// --- provider summary -------------------------------------------------

#[test]
fn summary_distinguishes_every_disposition() -> TestResult {
    let mut s = ProviderIngestSummary::default();
    for disp in [
        SourceIngestDisposition::Discovered,
        SourceIngestDisposition::Indexed,
        SourceIngestDisposition::Indexed,
        SourceIngestDisposition::PartiallyIndexed,
        SourceIngestDisposition::Quarantined,
        SourceIngestDisposition::Skipped,
        SourceIngestDisposition::Locked,
        SourceIngestDisposition::UnsupportedEncrypted,
    ] {
        s.record(disp);
    }
    verify_eq!(s.discovered, 1);
    verify_eq!(s.indexed, 2);
    verify_eq!(s.partially_indexed, 1);
    verify_eq!(s.quarantined, 1);
    verify_eq!(s.skipped, 1);
    verify_eq!(s.locked, 1);
    verify_eq!(s.unsupported_encrypted, 1);
    verify_eq!(s.total(), 8);
    verify_eq!(s.with_content(), 3); // indexed + partially-indexed
    Ok(())
}

#[test]
fn a_locked_or_encrypted_only_run_has_no_content_but_is_not_silent() -> TestResult {
    // The anti-pattern guard: a run where every source is locked/encrypted
    // must report zero content AND surface the explicit limitations.
    let mut s = ProviderIngestSummary::default();
    s.record(SourceIngestDisposition::Locked);
    s.record(SourceIngestDisposition::UnsupportedEncrypted);
    verify_eq!(s.with_content(), 0, "no searchable content");
    verify!(
        s.total() > 0,
        "but the sources are accounted for, not silently dropped"
    );
    verify!(SourceIngestDisposition::Locked.is_explicit_limitation());
    verify!(SourceIngestDisposition::UnsupportedEncrypted.is_explicit_limitation());
    Ok(())
}

// --- serialization stability ------------------------------------------

#[test]
fn diagnostic_serializes_with_stable_fields_and_round_trips() -> TestResult {
    let d =
        classify("cursor", "/p/state.vscdb", IngestFailureKind::SourceLocked).with_workspace("/p");
    let value = serde_json::to_value(&d)?;
    verify_eq!(
        value["schema_version"],
        CONNECTOR_INGEST_DIAGNOSTIC_SCHEMA_VERSION
    );
    verify_eq!(value["provider"], "cursor");
    verify_eq!(value["failure_kind"], "source-locked");
    verify_eq!(value["severity"], "warning");
    verify_eq!(value["disposition"], "locked");
    verify_eq!(value["retryable"], true);
    let back: ConnectorIngestDiagnostic = serde_json::from_value(value)?;
    verify_eq!(back, d);
    Ok(())
}

#[test]
fn summary_serializes_with_stable_snake_case_keys() -> TestResult {
    let mut s = ProviderIngestSummary::default();
    s.record(SourceIngestDisposition::UnsupportedEncrypted);
    let value = serde_json::to_value(s)?;
    verify_eq!(value["unsupported_encrypted"], 1);
    verify_eq!(value["indexed"], 0);
    Ok(())
}

#[test]
fn wire_labels_are_stable_kebab() -> TestResult {
    let failure_kinds = [
        IngestFailureKind::ExternalIdCollisionRisk,
        IngestFailureKind::MalformedJsonLine,
        IngestFailureKind::SourceLocked,
        IngestFailureKind::EncryptedUnsupported,
        IngestFailureKind::KeychainUnavailable,
        IngestFailureKind::FilenameAssumptionViolated,
        IngestFailureKind::UnreadableSource,
        IngestFailureKind::UnparseableSource,
        IngestFailureKind::TruncatedSession,
        IngestFailureKind::SourceOverReadBudget,
    ];
    verify_eq!(
        serde_json::to_value(failure_kinds)?,
        json!([
            "external-id-collision-risk",
            "malformed-json-line",
            "source-locked",
            "encrypted-unsupported",
            "keychain-unavailable",
            "filename-assumption-violated",
            "unreadable-source",
            "unparseable-source",
            "truncated-session",
            "source-over-read-budget"
        ])
    );
    for (k, w) in [
        (
            IngestFailureKind::ExternalIdCollisionRisk,
            "external-id-collision-risk",
        ),
        (IngestFailureKind::MalformedJsonLine, "malformed-json-line"),
        (IngestFailureKind::SourceLocked, "source-locked"),
        (
            IngestFailureKind::EncryptedUnsupported,
            "encrypted-unsupported",
        ),
        (
            IngestFailureKind::KeychainUnavailable,
            "keychain-unavailable",
        ),
        (
            IngestFailureKind::FilenameAssumptionViolated,
            "filename-assumption-violated",
        ),
        (IngestFailureKind::UnreadableSource, "unreadable-source"),
        (IngestFailureKind::UnparseableSource, "unparseable-source"),
        (IngestFailureKind::TruncatedSession, "truncated-session"),
        (
            IngestFailureKind::SourceOverReadBudget,
            "source-over-read-budget",
        ),
    ] {
        verify_eq!(k.as_str(), w);
    }
    let dispositions = [
        SourceIngestDisposition::Indexed,
        SourceIngestDisposition::PartiallyIndexed,
        SourceIngestDisposition::UnsupportedEncrypted,
        SourceIngestDisposition::Locked,
    ];
    verify_eq!(
        serde_json::to_value(dispositions)?,
        json!([
            "indexed",
            "partially-indexed",
            "unsupported-encrypted",
            "locked"
        ])
    );
    for (d, w) in [
        (SourceIngestDisposition::Indexed, "indexed"),
        (
            SourceIngestDisposition::PartiallyIndexed,
            "partially-indexed",
        ),
        (
            SourceIngestDisposition::UnsupportedEncrypted,
            "unsupported-encrypted",
        ),
        (SourceIngestDisposition::Locked, "locked"),
    ] {
        verify_eq!(d.as_str(), w);
    }
    verify_eq!(IngestSeverity::Warning.as_str(), "warning");
    Ok(())
}

#[test]
fn severity_orders_for_a_max_rollup() -> TestResult {
    verify!(IngestSeverity::Info < IngestSeverity::Warning);
    verify!(IngestSeverity::Warning < IngestSeverity::Error);
    let worst = [
        IngestSeverity::Info,
        IngestSeverity::Error,
        IngestSeverity::Warning,
    ]
    .into_iter()
    .max()
    .ok_or_else(|| std::io::Error::other("severity sample must be non-empty"))?;
    verify_eq!(worst, IngestSeverity::Error);
    Ok(())
}

#[test]
fn live_run_quarantines_only_bad_jsonl_line_and_indexes_valid_content() -> TestResult {
    let temp = tempdir()?;
    let path = temp.path().join("mixed.jsonl");
    std::fs::write(&path, b"{\"ok\":1}\n{broken}\n{\"ok\":2}\n")?;
    let root = ScanRoot::local(temp.path().to_path_buf());
    let source = DiscoveredSourceFile::new(
        "codex",
        &root,
        path.clone(),
        DiscoveredSourceRole::PrimarySessionLog,
        true,
    );
    let ctx = ScanContext::with_roots(temp.path().to_path_buf(), vec![root], None);
    let mut run = ConnectorIngestRun::begin("codex", temp.path(), &ctx, &[source]);
    let mut conversation = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: Some("mixed".into()),
        title: None,
        workspace: None,
        source_path: path,
        started_at: None,
        ended_at: None,
        metadata: json!({}),
        messages: Vec::new(),
    };
    run.observe_conversation(&mut conversation);
    let report = run.finish();
    verify_eq!(report.summary.partially_indexed, 1);
    verify_eq!(report.diagnostics.len(), 1);
    verify_eq!(report.diagnostics[0].line_or_row, Some(2));
    verify_eq!(
        report.diagnostics[0].failure_kind,
        IngestFailureKind::MalformedJsonLine
    );
    let quarantine =
        std::fs::read_to_string(temp.path().join("quarantine/connector_ingest_lines.json"))?;
    verify!(!quarantine.contains("{broken}"), "payload must never leak");
    verify!(quarantine.contains("payload_blake3"));
    Ok(())
}

#[test]
fn live_aider_run_disambiguates_same_external_id_by_workspace() -> TestResult {
    fn conversation(workspace: &str) -> NormalizedConversation {
        NormalizedConversation {
            agent_slug: "aider".into(),
            external_id: Some("history".into()),
            title: None,
            workspace: Some(PathBuf::from(workspace)),
            source_path: PathBuf::from(workspace).join(".aider.chat.history.md"),
            started_at: None,
            ended_at: None,
            metadata: json!({}),
            messages: Vec::new(),
        }
    }
    let ctx = ScanContext::local_default(PathBuf::from("/tmp"), None);
    let mut first = ConnectorIngestRun::begin("aider", Path::new("/tmp"), &ctx, &[]);
    let mut second = ConnectorIngestRun::begin("aider", Path::new("/tmp"), &ctx, &[]);
    let mut a = conversation("/workspace/alpha");
    let mut b = conversation("/workspace/beta");
    first.observe_conversation(&mut a);
    second.observe_conversation(&mut b);
    verify_ne!(a.external_id, b.external_id);
    verify_eq!(first.finish().diagnostics.len(), 1);
    verify_eq!(second.finish().summary.indexed, 1);
    Ok(())
}

#[test]
fn live_chatgpt_run_reports_encrypted_but_keeps_legacy_source() -> TestResult {
    let temp = tempdir()?;
    let legacy_dir = temp.path().join("conversations-legacy");
    let encrypted_dir = temp.path().join("conversations-v3-secret");
    std::fs::create_dir_all(&legacy_dir)?;
    std::fs::create_dir_all(&encrypted_dir)?;
    let legacy = legacy_dir.join("conversation.json");
    std::fs::write(&legacy, "{}")?;
    let root = ScanRoot::local(temp.path().to_path_buf());
    let source = DiscoveredSourceFile::new(
        "chatgpt",
        &root,
        legacy,
        DiscoveredSourceRole::PrimarySessionLog,
        true,
    );
    let ctx = ScanContext::with_roots(temp.path().to_path_buf(), vec![root], None);
    let report = ConnectorIngestRun::begin("chatgpt", temp.path(), &ctx, &[source]).finish();
    verify_eq!(report.summary.discovered, 1);
    verify_eq!(report.summary.unsupported_encrypted, 1);
    let expected_kind = if cfg!(target_os = "macos") {
        IngestFailureKind::KeychainUnavailable
    } else {
        IngestFailureKind::EncryptedUnsupported
    };
    let expected_kind_seen = report.diagnostics.iter().any(|diagnostic| {
        // ubs:ignore — compares a public diagnostic enum, never secret material.
        diagnostic.failure_kind == expected_kind
    });
    verify!(expected_kind_seen);
    Ok(())
}

#[test]
fn live_cursor_busy_error_is_retryable_and_not_zero_message_success() -> TestResult {
    let ctx = ScanContext::local_default(PathBuf::from("/tmp"), None);
    let mut run = ConnectorIngestRun::begin("cursor", Path::new("/tmp"), &ctx, &[]);
    run.observe_scan_error(
        Path::new("/cursor/state.vscdb"),
        "database is locked (SQLITE_BUSY)",
    );
    let report = run.finish();
    verify_eq!(report.summary.locked, 1);
    verify_eq!(report.summary.with_content(), 0);
    verify!(report.diagnostics[0].retryable);
    verify_eq!(
        report.diagnostics[0].failure_kind,
        IngestFailureKind::SourceLocked
    );
    Ok(())
}

#[test]
fn indexing_stats_robot_shape_exposes_summary_and_diagnostics() -> TestResult {
    let value = serde_json::to_value(crate::indexer::IndexingStats::default())?;
    verify_eq!(value["connector_summary"], json!({}));
    verify_eq!(value["connector_diagnostics"], json!([]));
    Ok(())
}

/// A Codex scan that stopped only because rollouts exceeded the read budget
/// is diagnosed per rejected rollout with the setting that admits it, even
/// through added context; it used to read as one "unreadable, check
/// permissions" entry naming the cass data directory. An I/O failure keeps
/// the unreadable entry.
#[test]
fn codex_over_budget_scan_error_is_diagnosed_per_rollout_not_as_the_data_dir() -> TestResult {
    let error = crate::connectors::codex::over_budget_scan_error(&[(
        "/codex/sessions/rollout-big.jsonl",
        200 * 1024 * 1024,
    )])
    .context("scanning codex");
    let ctx = ScanContext::local_default(PathBuf::from("/data"), None);

    let mut run = ConnectorIngestRun::begin("codex", Path::new("/data"), &ctx, &[]);
    run.observe_connector_scan_error(Path::new("/data"), &error);
    let report = run.finish();
    verify_eq!(report.diagnostics.len(), 1);
    let diagnostic = &report.diagnostics[0];
    verify_eq!(
        diagnostic.failure_kind,
        IngestFailureKind::SourceOverReadBudget
    );
    verify_eq!(diagnostic.source_path, "/codex/sessions/rollout-big.jsonl");
    verify!(
        diagnostic
            .safe_next_action
            .contains("CASS_CODEX_MAX_SOURCE_BYTES")
    );
    verify_eq!(report.summary.skipped, 1);

    let mut run = ConnectorIngestRun::begin("codex", Path::new("/data"), &ctx, &[]);
    let disk_gone = anyhow::Error::new(std::io::Error::other("disk went away"));
    run.observe_connector_scan_error(Path::new("/data"), &disk_gone);
    let report = run.finish();
    verify_eq!(report.diagnostics.len(), 1);
    verify_eq!(
        report.diagnostics[0].failure_kind,
        IngestFailureKind::UnreadableSource
    );
    verify_eq!(report.diagnostics[0].source_path, "/data");
    Ok(())
}

/// GH #511: a connector that read a source but rejected a record value
/// ("invalid shared CLI timestamp at record 0", with context and no I/O
/// cause) is unparseable, not unreadable: no permissions advice, not
/// retryable on the same bytes, at the root it scanned. An I/O cause still
/// reads as unreadable and a lock as locked.
#[test]
fn content_failures_are_unparseable_and_io_failures_stay_unreadable() -> TestResult {
    let ctx = ScanContext::local_default(PathBuf::from("/data"), None);
    let root = Path::new("/home/me/.config/manicode");
    let parse_error = anyhow::anyhow!("premature end of input")
        .context("invalid shared CLI timestamp at record 0");

    let mut run = ConnectorIngestRun::begin("codebuff", Path::new("/data"), &ctx, &[]);
    run.observe_connector_scan_error(root, &parse_error);
    let report = run.finish();
    verify_eq!(report.diagnostics.len(), 1);
    let diagnostic = &report.diagnostics[0];
    verify_eq!(
        diagnostic.failure_kind,
        IngestFailureKind::UnparseableSource
    );
    verify_eq!(diagnostic.source_path, root.display().to_string());
    verify!(!diagnostic.retryable);
    verify_eq!(diagnostic.disposition, SourceIngestDisposition::Skipped);
    verify!(!diagnostic.safe_next_action.contains("check permissions"));
    verify!(
        diagnostic
            .safe_next_action
            .contains("indexing_stats.connectors[].error")
    );
    verify_eq!(report.summary.skipped, 1);

    let denied = anyhow::Error::new(std::io::Error::new(
        std::io::ErrorKind::PermissionDenied,
        "permission denied",
    ))
    .context("reading chat-messages.json");
    let mut run = ConnectorIngestRun::begin("codebuff", Path::new("/data"), &ctx, &[]);
    run.observe_connector_scan_error(root, &denied);
    let report = run.finish();
    verify_eq!(
        report.diagnostics[0].failure_kind,
        IngestFailureKind::UnreadableSource
    );
    verify!(report.diagnostics[0].retryable);

    let locked = anyhow::anyhow!("database is locked").context("scanning state.vscdb");
    let mut run = ConnectorIngestRun::begin("cursor", Path::new("/data"), &ctx, &[]);
    run.observe_connector_scan_error(Path::new("/cursor"), &locked);
    let report = run.finish();
    verify_eq!(
        report.diagnostics[0].failure_kind,
        IngestFailureKind::SourceLocked
    );
    Ok(())
}

/// FAD 0.3.5 includes the failing transcript path in its top message.
/// Project names are not evidence of a database lock, even when a host
/// has flattened the contextual error into one display string.
#[test]
fn gh511_project_names_cannot_turn_parse_failures_into_locks() -> TestResult {
    let ctx = ScanContext::local_default(PathBuf::from("/data"), None);
    for project in ["a-busy-project", "unlocked-project"] {
        let transcript = PathBuf::from("/home/me/.config/manicode/projects")
            .join(project)
            .join("chats/2026-03-21T17-14-03.768Z/chat-messages.json");
        let parse_error =
            serde_json::from_str::<serde_json::Value>(r#"[{"id":"user-1774113351457""#)
                .expect_err("truncated native transcript must fail JSON parsing");
        let error =
            anyhow::Error::new(parse_error).context("invalid Codebuff / Freebuff transcript JSON");
        let summary = format!("{}: {error:#}", transcript.display());
        let error = error.context(summary.clone());
        for error in [error, anyhow::anyhow!(summary)] {
            let mut run = ConnectorIngestRun::begin("codebuff", Path::new("/data"), &ctx, &[]);
            run.observe_connector_scan_error(&transcript, &error);
            let report = run.finish();
            verify_eq!(report.diagnostics.len(), 1);
            let diagnostic = &report.diagnostics[0];
            verify_eq!(
                diagnostic.failure_kind,
                IngestFailureKind::UnparseableSource
            );
            verify_eq!(diagnostic.source_path, transcript.display().to_string());
            verify!(!diagnostic.retryable);
            verify_eq!(report.summary.skipped, 1);
            verify_eq!(report.summary.locked, 0);
        }
    }
    Ok(())
}

#[test]
fn gh511_permission_errors_under_busy_paths_stay_unreadable() -> TestResult {
    let ctx = ScanContext::local_default(PathBuf::from("/data"), None);
    let transcript = Path::new(
        "/home/me/.config/manicode/projects/a-busy-project/chats/unlocked/chat-messages.json",
    );
    let error = anyhow::Error::new(std::io::Error::new(
        std::io::ErrorKind::PermissionDenied,
        "permission denied",
    ));
    let summary = format!("{}: {error:#}", transcript.display());
    let error = error.context(summary);
    let mut run = ConnectorIngestRun::begin("codebuff", Path::new("/data"), &ctx, &[]);
    run.observe_connector_scan_error(transcript, &error);
    let report = run.finish();
    verify_eq!(
        report.diagnostics[0].failure_kind,
        IngestFailureKind::UnreadableSource
    );
    verify!(report.diagnostics[0].retryable);
    verify_eq!(report.summary.skipped, 1);
    verify_eq!(report.summary.locked, 0);
    Ok(())
}

#[test]
fn gh511_json_error_content_cannot_claim_a_database_lock() -> TestResult {
    let ctx = ScanContext::local_default(PathBuf::from("/data"), None);
    let error = <serde_json::Error as serde::de::Error>::custom("database is locked");
    let error = anyhow::Error::new(error).context("invalid Codebuff / Freebuff transcript JSON");
    let mut run = ConnectorIngestRun::begin("codebuff", Path::new("/data"), &ctx, &[]);
    run.observe_connector_scan_error(Path::new("/codebuff/chat-messages.json"), &error);
    let report = run.finish();
    verify_eq!(
        report.diagnostics[0].failure_kind,
        IngestFailureKind::UnparseableSource
    );
    verify!(!report.diagnostics[0].retryable);
    verify_eq!(report.summary.locked, 0);
    Ok(())
}

#[test]
fn gh511_database_lock_causes_remain_retryable() -> TestResult {
    use crate::franken_sync::FrankenError;

    let ctx = ScanContext::local_default(PathBuf::from("/data"), None);
    let path = Path::new("/cursor/state.vscdb");
    for error in [
        anyhow::Error::new(FrankenError::Busy),
        anyhow::Error::new(FrankenError::BusyRecovery),
        anyhow::Error::new(FrankenError::BusySnapshot {
            conflicting_pages: "12".to_string(),
        }),
        anyhow::Error::new(FrankenError::DatabaseLocked {
            path: path.to_path_buf(),
        }),
        anyhow::Error::new(FrankenError::LockFailed {
            detail: "F_SETLK contention".to_string(),
        }),
        anyhow::anyhow!("database is locked (SQLITE_BUSY)"),
        anyhow::anyhow!("database is busy (recovery in progress)"),
        anyhow::anyhow!("database table is locked"),
        anyhow::anyhow!("SQLITE_BUSY"),
        anyhow::anyhow!("SQLITE_BUSY_SNAPSHOT"),
        anyhow::anyhow!("SQLITE_LOCKED"),
        anyhow::anyhow!("SQLITE_LOCKED_SHAREDCACHE"),
    ] {
        let error = error.context("reading /cursor/state.vscdb");
        let mut run = ConnectorIngestRun::begin("cursor", Path::new("/data"), &ctx, &[]);
        run.observe_connector_scan_error(path, &error);
        let report = run.finish();
        verify_eq!(
            report.diagnostics[0].failure_kind,
            IngestFailureKind::SourceLocked
        );
        verify!(report.diagnostics[0].retryable);
        verify_eq!(report.summary.locked, 1);
    }
    Ok(())
}

#[test]
fn a_reused_source_missing_from_the_discovery_inventory_counts_as_indexed() -> TestResult {
    // Self-contained connectors (Claude Code, Codex) reuse ledger rows
    // without listing the unchanged sources first.
    let temp = tempdir()?;
    let ctx = ScanContext::with_roots(temp.path().to_path_buf(), Vec::new(), None);
    let mut run = ConnectorIngestRun::begin("claude", temp.path(), &ctx, &[]);
    let reused = temp.path().join("projects/p/session-0.jsonl");
    run.observe_reused_sources([reused.clone(), reused]);
    let report = run.finish();
    verify!(report.diagnostics.is_empty(), "{:?}", report.diagnostics);
    verify_eq!(report.summary.indexed, 1);
    verify_eq!(report.summary.discovered, 0);
    Ok(())
}

// ---- quarantine ledger publication batching (one write per source file) ----

/// Path of the connector-line quarantine ledger inside a scan data dir.
fn connector_ledger(data_dir: &Path) -> PathBuf {
    data_dir
        .join("quarantine")
        .join("connector_ingest_lines.json")
}

/// Attempted ledger publications whose destination lives in `data_dir`'s
/// quarantine directory, as counted by `quarantine::test_support`.
fn ledger_save_count(data_dir: &Path) -> u64 {
    crate::indexer::quarantine::test_support::save_count(&data_dir.join("quarantine"))
}

fn reset_ledger_save_count(data_dir: &Path) {
    crate::indexer::quarantine::test_support::reset_save_count(&data_dir.join("quarantine"));
}

/// The exact BTreeMap key the quarantine ledger uses for one observation.
fn ledger_key(provider: &str, source: &Path, line_number: u64, payload: &[u8]) -> String {
    format!(
        "{}|{}|{}|{}",
        provider,
        source.display(),
        line_number,
        blake3::hash(payload).to_hex()
    )
}

/// Parse the on-disk ledger into a JSON value, failing with the raw text.
fn read_ledger(data_dir: &Path) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let text = std::fs::read_to_string(connector_ledger(data_dir))?;
    Ok(serde_json::from_str(&text)
        .unwrap_or_else(|error| panic!("ledger is not valid JSON: {error}: {text}")))
}

fn scan_source(data_dir: &Path, provider: &str, file: &Path) -> ConnectorIngestRun {
    let root = ScanRoot::local(file.parent().unwrap().to_path_buf());
    let source = DiscoveredSourceFile::new(
        provider,
        &root,
        file.to_path_buf(),
        DiscoveredSourceRole::PrimarySessionLog,
        true,
    );
    let ctx = ScanContext::with_roots(data_dir.to_path_buf(), vec![root], None);
    ConnectorIngestRun::begin(provider, data_dir, &ctx, &[source])
}

/// N distinct malformed lines in one file must publish the ledger O(1)
/// times — once for the file — instead of once per malformed line.
#[test]
fn quarantine_ledger_publications_are_o1_per_source_file() -> TestResult {
    let temp = tempdir()?;
    let data_dir = temp.path();
    let file = data_dir.join("rollout-many-bad-lines.jsonl");
    let malformed_lines = 64usize;
    let mut body = String::from("{\"ok\":1}\n");
    for i in 0..malformed_lines {
        body.push_str(&format!("{{broken synthetic line {i}}}\n"));
    }
    // Final line has no trailing newline: it is a truncated session, a
    // different failure kind that must land in the same single batch.
    body.push_str("{broken truncated tail");
    std::fs::write(&file, body)?;

    reset_ledger_save_count(data_dir);
    let report = scan_source(data_dir, "codex", &file).finish();
    let saves = ledger_save_count(data_dir);

    verify_eq!(
        saves,
        1,
        "{malformed_lines} malformed lines in one source file must publish the \
         ledger exactly once (attempted publications: {saves})",
    );
    verify_eq!(report.diagnostics.len(), malformed_lines + 1);

    let ledger = read_ledger(data_dir)?;
    let entries = ledger["entries"].as_object().unwrap();
    verify_eq!(entries.len(), malformed_lines + 1);
    for i in 0..malformed_lines {
        let payload = format!("{{broken synthetic line {i}}}");
        let key = ledger_key("codex", &file, (i + 2) as u64, payload.as_bytes());
        let record = entries
            .get(&key)
            .ok_or_else(|| format!("entry for line {} missing from the ledger", i + 2))?;
        verify_eq!(record["failure_kind"], "malformed-json-line");
        verify_eq!(record["attempt_count"], 1);
    }
    let tail_key = ledger_key("codex", &file, 66, b"{broken truncated tail");
    let tail = entries
        .get(&tail_key)
        .ok_or_else(|| "truncated tail entry missing from the ledger".to_string())?;
    verify_eq!(tail["failure_kind"], "truncated-session");
    Ok(())
}

/// A mixed batch (new entries, repeats of entries from an earlier scan,
/// two source files) must leave the ledger with exactly the content the
/// per-line writer produced: same fields, same attempt counts, first
/// observation timestamps preserved.
#[test]
fn quarantine_ledger_content_matches_per_line_writes_for_a_mixed_batch() -> TestResult {
    let temp = tempdir()?;
    let data_dir = temp.path();
    let file_a = data_dir.join("rollout-a.jsonl");
    let file_b = data_dir.join("rollout-b.jsonl");
    std::fs::write(&file_a, "{broken alpha}\n{\"ok\":1}\n{broken gamma}\n")?;

    // Scan 1: file A alone. Two fresh entries, one publication per line
    // under the old contract — capture the first-observation timestamp
    // that every later scan must preserve.
    reset_ledger_save_count(data_dir);
    let _ = scan_source(data_dir, "codex", &file_a).finish();
    let ledger = read_ledger(data_dir)?;
    let alpha_key = ledger_key("codex", &file_a, 1, b"{broken alpha}");
    let alpha_first_observed = ledger["entries"]
        .as_object()
        .unwrap()
        .get(&alpha_key)
        .ok_or("alpha entry missing after scan 1")?["first_observed_at"]
        .as_str()
        .unwrap()
        .to_string();

    // Scan 2: file A grows one new malformed line while its two earlier
    // lines repeat byte-for-byte; file B is a second source file.
    std::fs::write(
        &file_a,
        "{broken alpha}\n{\"ok\":1}\n{broken gamma}\n{broken delta}\n",
    )?;
    std::fs::write(&file_b, "{broken beta}\n")?;
    reset_ledger_save_count(data_dir);
    {
        let root = ScanRoot::local(data_dir.to_path_buf());
        let sources = [
            DiscoveredSourceFile::new(
                "codex",
                &root,
                file_a.clone(),
                DiscoveredSourceRole::PrimarySessionLog,
                true,
            ),
            DiscoveredSourceFile::new(
                "codex",
                &root,
                file_b.clone(),
                DiscoveredSourceRole::PrimarySessionLog,
                true,
            ),
        ];
        let ctx = ScanContext::with_roots(data_dir.to_path_buf(), vec![root], None);
        let _ = ConnectorIngestRun::begin("codex", data_dir, &ctx, &sources).finish();
    }
    let saves = ledger_save_count(data_dir);
    verify_eq!(
        saves,
        2,
        "two source files must publish the ledger once per file (got {saves})"
    );

    let ledger = read_ledger(data_dir)?;
    let entries = ledger["entries"].as_object().unwrap();
    verify_eq!(entries.len(), 4, "2 repeats + 2 new entries");
    for (key, expected_attempts, payload) in [
        (&alpha_key, 2u64, "{broken alpha}"),
        (
            &ledger_key("codex", &file_a, 3, b"{broken gamma}"),
            2,
            "{broken gamma}",
        ),
        (
            &ledger_key("codex", &file_a, 4, b"{broken delta}"),
            1,
            "{broken delta}",
        ),
        (
            &ledger_key("codex", &file_b, 1, b"{broken beta}"),
            1,
            "{broken beta}",
        ),
    ] {
        let record = entries
            .get(key)
            .ok_or_else(|| format!("missing ledger entry {key}"))?;
        verify_eq!(record["attempt_count"], expected_attempts);
        verify_eq!(record["provider"], "codex");
        verify_eq!(record["failure_kind"], "malformed-json-line");
        verify_eq!(record["payload_bytes"].as_u64(), Some(payload.len() as u64));
        verify_eq!(
            record["payload_blake3"],
            blake3::hash(payload.as_bytes()).to_hex().to_string()
        );
        let first = record["first_observed_at"].as_str().unwrap();
        let last = record["last_observed_at"].as_str().unwrap();
        verify!(first <= last, "first_observed_at must not move backwards");
    }
    // The repeated entry keeps its first sighting from scan 1 while its
    // attempt count advanced — the dedup contract of the ledger.
    verify_eq!(
        entries
            .get(&alpha_key)
            .ok_or("alpha entry missing after scan 2")?["first_observed_at"],
        alpha_first_observed.as_str(),
        "repeated observation must not reset first_observed_at"
    );
    // No payload text may ever reach the ledger.
    let raw = std::fs::read_to_string(connector_ledger(data_dir))?;
    for payload in ["{broken alpha}", "{broken beta}", "{broken delta}"] {
        verify!(
            !raw.contains(payload),
            "payload text leaked into the ledger"
        );
    }
    Ok(())
}

/// A re-scan that observes no new malformed line must not touch the
/// ledger file at all, while still reporting every line as a diagnostic.
#[test]
fn a_repeat_scan_of_unchanged_malformed_files_writes_nothing() -> TestResult {
    let temp = tempdir()?;
    let data_dir = temp.path();
    let file = data_dir.join("rollout-repeat.jsonl");
    std::fs::write(&file, "{broken one}\n{\"ok\":1}\n{broken two}\n")?;

    reset_ledger_save_count(data_dir);
    let first = scan_source(data_dir, "codex", &file).finish();
    verify_eq!(first.diagnostics.len(), 2);
    let ledger = connector_ledger(data_dir);
    let bytes_before = std::fs::read(&ledger)?;
    let mtime_before = std::fs::metadata(&ledger)?.modified()?;

    reset_ledger_save_count(data_dir);
    let second = scan_source(data_dir, "codex", &file).finish();
    let saves = ledger_save_count(data_dir);
    verify_eq!(
        saves,
        0,
        "a re-scan with no new malformed lines must not publish the ledger (got {saves})"
    );
    verify_eq!(
        std::fs::read(&ledger)?,
        bytes_before,
        "ledger bytes must be untouched by a no-change re-scan"
    );
    verify_eq!(
        std::fs::metadata(&ledger)?.modified()?,
        mtime_before,
        "ledger mtime must be untouched by a no-change re-scan"
    );
    // Diagnostics are per-line observations and stay equivalent.
    verify_eq!(second.diagnostics.len(), 2, "{:?}", second.diagnostics);
    verify_eq!(
        second.diagnostics[0].failure_kind,
        IngestFailureKind::MalformedJsonLine
    );
    verify_eq!(second.diagnostics[0].line_or_row, Some(1));
    verify_eq!(second.diagnostics[1].line_or_row, Some(3));
    Ok(())
}

/// Manual before/after cost probe for the ledger publication policy. Not
/// part of the suite: run with
/// `cargo test --lib quarantine_ledger_write_cost_probe -- --ignored --nocapture`
/// and size the synthetic file with `CASS_QUARANTINE_TIMING_LINES`
/// (default 15,000, the measured fresh-index magnitude).
#[test]
#[ignore = "manual perf probe; see .scratch/glm/2026-11-07-quarantine-batch/report.md"]
fn quarantine_ledger_write_cost_probe() -> TestResult {
    let malformed_lines: usize = std::env::var("CASS_QUARANTINE_TIMING_LINES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(15_000);
    let temp = tempdir()?;
    let data_dir = temp.path();
    let file = data_dir.join("rollout-probe.jsonl");
    let mut body = String::with_capacity(malformed_lines * 64);
    body.push_str(
        "{\"timestamp\":\"2026-01-01T00:00:00.000Z\",\"type\":\"session_meta\",\
         \"payload\":{\"id\":\"synthetic-probe\"}}\n",
    );
    for i in 0..malformed_lines {
        body.push_str(&format!(
            "{{broken synthetic payload {i} for the quarantine probe}}\n"
        ));
    }
    std::fs::write(&file, body)?;

    reset_ledger_save_count(data_dir);
    let started = std::time::Instant::now();
    let report = scan_source(data_dir, "codex", &file).finish();
    let elapsed = started.elapsed();
    let saves = ledger_save_count(data_dir);
    println!(
        "QUARANTINE_PROBE_BATCHED malformed_lines={malformed_lines} elapsed_ms={} \
         ledger_publications={saves} diagnostics={}",
        elapsed.as_millis(),
        report.diagnostics.len(),
    );
    verify_eq!(report.diagnostics.len(), malformed_lines);
    Ok(())
}

/// Manual cost probe for the incumbent per-line publication policy: the
/// retained single-observation entry point performs exactly the per-line
/// work the old scan loop did (lock, read and parse the whole ledger,
/// upsert one line, serialize and fsync-publish the whole ledger), so
/// looping it N times measures the pre-batching cost in the same binary
/// as `quarantine_ledger_write_cost_probe`.
#[test]
#[ignore = "manual perf probe; see .scratch/glm/2026-11-07-quarantine-batch/report.md"]
fn quarantine_ledger_per_line_write_cost_probe() -> TestResult {
    let malformed_lines: usize = std::env::var("CASS_QUARANTINE_TIMING_LINES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(15_000);
    let temp = tempdir()?;
    let data_dir = temp.path();
    let file = data_dir.join("rollout-probe.jsonl");
    reset_ledger_save_count(data_dir);
    let started = std::time::Instant::now();
    for i in 0..malformed_lines {
        let payload = format!("{{broken synthetic payload {i} for the quarantine probe}}\n");
        crate::indexer::quarantine::record_connector_line(
            data_dir,
            "codex",
            &file,
            (i + 2) as u64,
            payload.trim_end_matches('\n').as_bytes(),
            "malformed-json-line",
        )?;
    }
    let elapsed = started.elapsed();
    let saves = ledger_save_count(data_dir);
    let ledger = read_ledger(data_dir)?;
    let entries = ledger["entries"].as_object().unwrap().len();
    println!(
        "QUARANTINE_PROBE_PER_LINE malformed_lines={malformed_lines} elapsed_ms={} \
         ledger_publications={saves} entries={entries}",
        elapsed.as_millis(),
    );
    verify_eq!(saves as usize, malformed_lines);
    verify_eq!(entries, malformed_lines);
    Ok(())
}
