#[allow(unused_imports)]
use super::*;
#[allow(unused_imports)]
use crate::franken_sync::compat::{
    ConnectionExt, OpenFlags as FrankenOpenFlags, RowExt,
    open_with_flags as open_franken_with_flags,
};
#[allow(unused_imports)]
use crate::indexer::IndexOptions;
#[allow(unused_imports)]
use crate::model::cli_error_kind::ErrorKind as CliErrorKind;
#[allow(unused_imports)]
use crate::robot_budget_envelope::BudgetSnapshot;
#[allow(unused_imports)]
use crate::{cli_app::*, cli_root::*, lib_doctor::*, *};
#[allow(unused_imports)]
use anyhow::Result;
#[allow(unused_imports)]
use base64::prelude::*;
#[allow(unused_imports)]
use chrono::Utc;
#[allow(unused_imports)]
use clap::{Arg, ArgAction, Command, CommandFactory, Parser, Subcommand, ValueEnum, ValueHint};
#[allow(unused_imports)]
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
#[allow(unused_imports)]
use std::fs::{File, OpenOptions};
#[allow(unused_imports)]
use std::io::{self, BufRead, BufReader, IsTerminal, Read, Seek, SeekFrom, Write};
#[allow(unused_imports)]
use std::path::{Path, PathBuf};
#[allow(unused_imports)]
use std::sync::{Arc, Mutex};
#[allow(unused_imports)]
use std::time::{Duration, Instant};
#[allow(unused_imports)]
use tracing::{info, warn};

#[test]
fn doctor_forensic_bundle_copy_refuses_path_traversal_targets() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let bundle_dir = data_dir
        .join("doctor")
        .join("forensic-bundles")
        .join("unit");
    std::fs::create_dir_all(&bundle_dir).expect("create bundle dir");
    let source_path = data_dir.join("source.txt");
    std::fs::write(&source_path, b"source bytes").expect("write source");
    let mut artifacts = Vec::new();

    let err = doctor_forensic_copy_file_artifact(
        &mut artifacts,
        &bundle_dir,
        &data_dir,
        DoctorForensicCopySpec {
            artifact_kind: "unsafe_test_source",
            source_path: &source_path,
            relative_path: Path::new("../escape.txt"),
            required: true,
            sidecar_group: None,
        },
    )
    .expect_err("path traversal relative targets must be refused");

    assert!(err.contains("unsafe forensic bundle relative path"));
    assert_eq!(artifacts.len(), 1);
    assert!(!artifacts[0].copied);
    assert!(
        artifacts[0]
            .error
            .as_deref()
            .is_some_and(|error| error.contains("unsafe bundle relative path"))
    );
}

#[test]
fn doctor_forensic_bundle_copy_fails_when_required_artifact_is_missing() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let bundle_dir = data_dir
        .join("doctor")
        .join("forensic-bundles")
        .join("unit");
    std::fs::create_dir_all(&bundle_dir).expect("create bundle dir");
    let missing_source = data_dir.join("required-before-mutation.lock");
    let mut artifacts = Vec::new();

    let err = doctor_forensic_copy_file_artifact(
        &mut artifacts,
        &bundle_dir,
        &data_dir,
        DoctorForensicCopySpec {
            artifact_kind: "required_before_mutation",
            source_path: &missing_source,
            relative_path: Path::new("operation/required-before-mutation.lock"),
            required: true,
            sidecar_group: None,
        },
    )
    .expect_err("missing required pre-mutation artifacts must fail the bundle");

    assert!(err.contains("required forensic source artifact is missing"));
    assert_eq!(artifacts.len(), 1);
    assert_eq!(artifacts[0].artifact_kind, "required_before_mutation");
    assert!(artifacts[0].required);
    assert!(!artifacts[0].exists);
    assert!(!artifacts[0].copied);
    assert_eq!(
        artifacts[0].error.as_deref(),
        Some("required source artifact is missing")
    );
}

#[cfg(unix)]
#[test]
fn doctor_forensic_bundle_capture_refuses_symlinked_bundle_root() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index").join("tantivy");
    std::fs::create_dir_all(data_dir.join("doctor")).expect("create doctor dir");
    std::fs::write(&db_path, b"sqlite bytes").expect("write db");
    let outside = temp.path().join("outside-bundles");
    std::fs::create_dir_all(&outside).expect("create outside bundle target");
    std::os::unix::fs::symlink(&outside, doctor_forensic_bundle_root(&data_dir))
        .expect("create symlinked bundle root");

    let bundle = capture_doctor_forensic_bundle(DoctorForensicBundleCaptureInput {
        operation_id: "unit-symlink-refusal",
        data_dir: &data_dir,
        db_path: &db_path,
        index_path: &index_path,
        plan: None,
        quarantine_report: None,
        extra_file_artifacts: &[],
    });

    assert_eq!(bundle.status, "failed");
    assert!(
        bundle
            .blocked_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("unsafe forensic bundle root")),
        "symlinked bundle root should fail closed before copying: {bundle:#?}"
    );
}

#[test]
fn doctor_cleanup_event_log_orders_hash_chain_and_correlates_receipts() {
    let data_dir = Path::new("/tmp/cass");
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    let mut result = DiagCleanupApplyResult {
        mode: DoctorRepairMode::CleanupApply,
        approval_requirement: DoctorApprovalRequirement::ApprovalFingerprint,
        requested: true,
        operation_started_at_ms: Some(1_700_000_002_000),
        operation_finished_at_ms: Some(1_700_000_002_075),
        approval_fingerprint: "cleanup-v1-events".to_string(),
        pruned_asset_count: 1,
        reclaimed_bytes: 42,
        actions: vec![DiagCleanupApplyAction {
            asset_safety: doctor_asset_safety(DoctorAssetClass::RetainedPublishBackup),
            artifact_kind: "retained_publish_backup".to_string(),
            path: "/tmp/cass/index/.lexical-publish-backups/old".to_string(),
            generation_id: None,
            shard_id: None,
            disposition: None,
            reason: "outside retention cap".to_string(),
            planned_reclaimable_bytes: 42,
            reclaimed_bytes: 42,
            applied: true,
            skipped: false,
            skip_reason: None,
            mutation_receipt: None,
        }],
        ..DiagCleanupApplyResult::default()
    };

    finalize_cleanup_apply_contract(&mut result, data_dir, &db_path, &index_path);

    let plan = result.plan.as_ref().expect("cleanup apply plan");
    assert_eq!(plan.event_log.status, "embedded_planned_events");
    assert_eq!(plan.event_log.event_count, 3);
    assert_eq!(
        plan.event_log.events[1].phase, "action_planned",
        "event invariant failed: planned cleanup event stream should record planned action phase before receipt execution"
    );
    assert!(
        plan.event_log.events[1].receipt_correlation_id.is_none(),
        "event invariant failed: dry-run/plan events must not claim receipt correlation before apply"
    );

    let event_log = &result.receipt.event_log;
    assert_eq!(event_log.status, "embedded_receipt_events");
    assert_eq!(
        event_log.event_count, 3,
        "event invariant failed: one cleanup action should produce start/action/finish events"
    );
    assert_eq!(
        event_log.hash_chain_tip.as_deref(),
        event_log.events.last().map(|event| event.event_id.as_str()),
        "event invariant failed: hash_chain_tip must point at the final event"
    );
    assert!(
        event_log
            .checksum_blake3
            .as_deref()
            .is_some_and(|checksum| { checksum.starts_with("doctor-event-log-v1-") }),
        "event invariant failed: receipt event log must include a deterministic checksum"
    );

    let start = &event_log.events[0];
    assert_eq!(start.phase, "operation_started");
    assert!(start.previous_event_hash.is_none());
    assert_eq!(start.operation_id, result.receipt.plan_fingerprint);
    assert_eq!(
        doctor_event_id(start),
        start.event_id,
        "event invariant failed: operation start event id must be canonical and reproducible"
    );

    let action_event = &event_log.events[1];
    let receipt_action = &result.receipt.actions[0];
    assert_eq!(
        action_event.previous_event_hash.as_deref(),
        Some(start.event_id.as_str()),
        "event invariant failed: action event must chain from operation start"
    );
    assert_eq!(action_event.phase, "action_applied");
    assert_eq!(
        action_event.action_id.as_deref(),
        Some(receipt_action.action_id.as_str())
    );
    assert_eq!(
        action_event.redacted_target_path.as_deref(),
        Some("[cass-data]/index/.lexical-publish-backups/old"),
        "redaction invariant failed: event logs must store redacted target paths, not full sensitive paths"
    );
    assert!(
        !serde_json::to_string(action_event)
            .expect("serialize event")
            .contains("/tmp/cass/index"),
        "redaction invariant failed: event log leaked the full cleanup target path"
    );
    assert_eq!(action_event.safety_gate_passed, Some(true));
    assert!(action_event.blocked_reasons.is_empty());
    assert_eq!(
        action_event.receipt_correlation_id.as_deref(),
        Some("doctor_cleanup_apply_v1")
    );
    assert_eq!(
        action_event.artifact_ids,
        vec![receipt_action.artifacts[0].artifact_id.clone()],
        "event invariant failed: action event must correlate with receipt artifact ids"
    );
    assert_eq!(
        doctor_event_id(action_event),
        action_event.event_id,
        "event invariant failed: action event id must be canonical and reproducible"
    );

    let finish = &event_log.events[2];
    assert_eq!(
        finish.previous_event_hash.as_deref(),
        Some(action_event.event_id.as_str()),
        "event invariant failed: finish event must chain from the action event"
    );
    assert_eq!(finish.phase, "operation_finished");
    assert_eq!(finish.elapsed_ms, Some(75));
    assert_eq!(
        doctor_event_id(finish),
        finish.event_id,
        "event invariant failed: operation finish event id must be canonical and reproducible"
    );
}

#[test]
fn doctor_operation_event_log_records_checks_without_sensitive_paths() {
    let check_reports = vec![doctor_check_report(
        "operation_state",
        "warn",
        "Doctor mutation blocked: /tmp/cass/doctor/events/pending.lock",
        false,
        false,
    )];
    let outcome = doctor_operation_outcome_with_details(
        DoctorOperationOutcomeKind::OkReadOnlyDiagnosed,
        "read-only doctor surfaced an operation-state warning".to_string(),
        "recorded diagnostic event log".to_string(),
        "no mutation was attempted".to_string(),
        DoctorDataLossRisk::Low,
        Some("cass doctor --json".to_string()),
        None,
    );
    let operation_id = doctor_check_operation_id(&check_reports, false, &outcome, false);
    let event_log = doctor_operation_event_log_for_checks(
        &operation_id,
        &check_reports,
        false,
        25,
        "embedded_operation_events",
    );

    assert_eq!(event_log.status, "embedded_operation_events");
    assert_eq!(event_log.event_count, 3);
    assert_eq!(
        event_log.events[0].phase, "operation_started",
        "event invariant failed: read-only doctor event log should begin with an operation start"
    );
    assert_eq!(
        event_log.events[1].phase, "check_warn",
        "event invariant failed: warning checks need a stable branchable phase"
    );
    assert_eq!(
        event_log.events[1].mode,
        DoctorRepairMode::Check,
        "event invariant failed: read-only doctor checks must not masquerade as mutating repairs"
    );
    assert_eq!(
        event_log.events[1].asset_class,
        Some(DoctorAssetClass::OperationReceipt),
        "event invariant failed: check events should preserve the affected asset class for triage"
    );
    assert!(
        event_log.events[1]
            .blocked_reasons
            .iter()
            .any(|reason| reason == "check_status:warn"),
        "event invariant failed: warning check event should carry a machine-readable status blocker"
    );
    assert_eq!(event_log.events[2].elapsed_ms, Some(25));
    assert_eq!(
        event_log.hash_chain_tip.as_deref(),
        event_log.events.last().map(|event| event.event_id.as_str())
    );
    let mut retimed_events = event_log.events.clone();
    retimed_events[2].elapsed_ms = Some(999);
    assert_eq!(
        doctor_event_id(&retimed_events[2]),
        event_log.events[2].event_id,
        "event invariant failed: volatile elapsed telemetry must not change event identity"
    );
    assert_eq!(
        doctor_event_log_checksum(&retimed_events),
        event_log.checksum_blake3.clone().expect("event checksum"),
        "event invariant failed: volatile elapsed telemetry must not change event-log checksum"
    );
    let encoded = serde_json::to_string(&event_log).expect("event log json");
    assert!(
        !encoded.contains("/tmp/cass"),
        "redaction invariant failed: check event log must not leak sensitive paths from diagnostic messages"
    );
}

#[test]
fn doctor_artifact_manifest_reports_checksum_mismatch_and_missing_artifacts() {
    let matched = DoctorArtifact {
        artifact_id: "matched".to_string(),
        artifact_kind: "receipt".to_string(),
        asset_class: DoctorAssetClass::OperationReceipt,
        path: "/tmp/cass/receipts/matched.json".to_string(),
        redacted_path: "[cass-data]/receipts/matched.json".to_string(),
        exists: true,
        size_bytes: Some(8),
        descriptor_blake3: "descriptor".to_string(),
        expected_content_blake3: Some("abc".to_string()),
        actual_content_blake3: Some("abc".to_string()),
        checksum_status: doctor_artifact_checksum_status(true, Some("abc"), Some("abc")),
    };
    let mismatched = DoctorArtifact {
        artifact_id: "mismatched".to_string(),
        actual_content_blake3: Some("def".to_string()),
        checksum_status: doctor_artifact_checksum_status(true, Some("abc"), Some("def")),
        ..matched.clone()
    };
    let missing = DoctorArtifact {
        artifact_id: "missing".to_string(),
        exists: false,
        checksum_status: doctor_artifact_checksum_status(false, Some("abc"), None),
        ..matched.clone()
    };

    assert_eq!(
        doctor_artifact_manifest(vec![matched.clone()]).drift_detection_status,
        DoctorDriftDetectionStatus::Verified
    );
    assert_eq!(
        doctor_artifact_manifest(vec![matched, mismatched]).drift_detection_status,
        DoctorDriftDetectionStatus::ChecksumMismatch
    );
    assert_eq!(
        doctor_artifact_manifest(vec![missing]).drift_detection_status,
        DoctorDriftDetectionStatus::MissingArtifact
    );
}

#[test]
fn doctor_redacted_path_hides_cass_data_dir_prefix() {
    let data_dir = Path::new("/home/example/.local/share/cass");
    assert_eq!(
        doctor_redacted_path(
            "/home/example/.local/share/cass/index/.lexical-publish-backups/old",
            data_dir
        ),
        "[cass-data]/index/.lexical-publish-backups/old"
    );
    assert_eq!(
        doctor_redacted_path("/var/tmp/external-session.jsonl", data_dir),
        "[external]/external-session.jsonl"
    );
}

#[test]
fn doctor_sources_config_path_uses_sources_config_path_result() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let config_path = temp
        .path()
        .join("xdg-config")
        .join("cass")
        .join("sources.toml");

    assert_eq!(
        doctor_sources_config_path_from_config_result(&data_dir, Ok(config_path.clone())),
        config_path,
        "doctor must inspect the same sources.toml location as sources commands"
    );
}

#[test]
fn doctor_sources_config_path_falls_back_to_data_dir_when_config_dir_is_unavailable() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");

    assert_eq!(
        doctor_sources_config_path_from_config_result(
            &data_dir,
            Err(crate::sources::config::ConfigError::NoConfigDir)
        ),
        data_dir.join("cass").join("sources.toml")
    );
}

#[test]
fn doctor_sources_config_check_validates_sources_not_just_toml_syntax() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let sources_path = temp.path().join("sources.toml");
    std::fs::write(
        &sources_path,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@host"
paths = [" ~/.claude/projects"]
"#,
    )
    .expect("write sources config");

    let check = doctor_sources_config_check(&sources_path);
    assert_eq!(check.status, "warn");
    assert!(
        check.message.contains("Sources config validation warning"),
        "{check:?}"
    );
    assert!(
        check.message.contains("paths[0] cannot have leading"),
        "{check:?}"
    );
}

#[test]
fn doctor_sources_config_check_accepts_valid_sources_config() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let sources_path = temp.path().join("sources.toml");
    std::fs::write(
        &sources_path,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@host"
paths = ["~/.claude/projects"]
"#,
    )
    .expect("write sources config");

    let check = doctor_sources_config_check(&sources_path);
    assert_eq!(check.status, "pass");
    assert_eq!(check.message, "Sources config valid");
}

#[test]
fn doctor_inventory_queries_stream_metadata_and_count_messages_in_one_narrow_pass() {
    use crate::franken_sync::compat::ConnectionExt as _;

    let temp = tempfile::TempDir::new().expect("tempdir");
    let db_path = temp.path().join("doctor-inventory-streaming.db");
    let source_path = temp.path().join("session.jsonl");
    std::fs::write(&source_path, b"{}\n").expect("write source fixture");
    let conn = crate::franken_sync::Connection::open(db_path.to_string_lossy().as_ref())
        .expect("open fixture database");
    conn.execute_batch(&format!(
        "CREATE TABLE agents (id INTEGER PRIMARY KEY, slug TEXT NOT NULL UNIQUE);
         CREATE TABLE sources (id TEXT PRIMARY KEY, kind TEXT NOT NULL);
         CREATE TABLE conversations (
             id INTEGER PRIMARY KEY,
             agent_id INTEGER,
             source_id TEXT,
             source_path TEXT,
             origin_host TEXT,
             started_at INTEGER
         );
         CREATE TABLE messages (
             id INTEGER PRIMARY KEY,
             conversation_id INTEGER NOT NULL,
             idx INTEGER NOT NULL,
             content TEXT NOT NULL
         );
         CREATE UNIQUE INDEX idx_messages_conv_idx ON messages(conversation_id, idx);
         INSERT INTO agents(id, slug) VALUES(1, 'codex');
         INSERT INTO sources(id, kind) VALUES('local', 'local');
         INSERT INTO conversations(id, agent_id, source_id, source_path, started_at)
         VALUES
             (10, 1, 'local', '{source}', 1000),
             (20, 1, 'local', '{source}', 2000),
             (30, NULL, 'local', '{source}', 3000);
         INSERT INTO messages(id, conversation_id, idx, content)
         VALUES
             (101, 10, 0, 'one'),
             (901, 10, 100, 'two'),
             (5001, 30, 2, 'three'),
             (7001, 30, 50, 'four'),
             (9001, 30, 99, 'five');",
        source = source_path.display(),
    ))
    .expect("seed inventory fixture");

    let inventory_rows =
        query_doctor_source_inventory_db_rows(&conn).expect("query source inventory");
    assert_eq!(inventory_rows.len(), 3);
    assert!(
        inventory_rows.iter().all(|row| row.conversation_count == 1),
        "the database query must emit unit rows for the bounded Rust aggregation: {inventory_rows:#?}"
    );
    let inventory =
        build_doctor_source_inventory_report(temp.path(), true, None, inventory_rows, Vec::new());
    assert_eq!(inventory.total_indexed_conversations, 3);
    assert_eq!(inventory.provider_counts.get("codex"), Some(&2));
    assert_eq!(inventory.provider_counts.get("unknown"), Some(&1));

    let candidates =
        query_doctor_raw_mirror_backfill_candidates(&conn).expect("query raw mirror candidates");
    let counts: Vec<(i64, usize)> = candidates
        .iter()
        .map(|candidate| (candidate.conversation_id, candidate.message_count))
        .collect();
    assert_eq!(counts, vec![(10, 2), (20, 0), (30, 3)]);

    let inventory_plan: Vec<String> = conn
        .query_map_collect(
            "EXPLAIN QUERY PLAN
             SELECT COALESCE(a.slug, 'unknown'), c.source_path,
                    COALESCE(c.source_id, 'local'), c.origin_host, s.kind, 1
             FROM conversations c
             LEFT JOIN agents a ON c.agent_id = a.id
             LEFT JOIN sources s ON c.source_id = s.id",
            &[],
            |row: &crate::franken_sync::Row| row.get_typed(3),
        )
        .expect("explain source inventory metadata scan");
    assert!(
        !inventory_plan
            .iter()
            .any(|detail| detail.contains("TEMP B-TREE")),
        "source inventory must not materialize GROUP BY/ORDER BY state: {inventory_plan:?}"
    );
    let message_plan: Vec<String> = conn
        .query_map_collect(
            "EXPLAIN QUERY PLAN
             SELECT conversation_id
             FROM messages INDEXED BY idx_messages_conv_idx",
            &[],
            |row: &crate::franken_sync::Row| row.get_typed(3),
        )
        .expect("explain narrow message scan");
    // FrankenSQLite currently renders a full covering-index walk as the
    // generic `SCAN messages`; the production SQL's INDEXED BY clause is
    // the enforceable index contract (and would fail preparation if the
    // index were absent). The plan still must remain a single scan with
    // no temp materialization.
    assert_eq!(
        message_plan.len(),
        1,
        "unexpected scan plan: {message_plan:?}"
    );
    assert!(
        !message_plan
            .iter()
            .any(|detail| detail.contains("TEMP B-TREE")),
        "message counts must not materialize grouped state: {message_plan:?}"
    );
}

#[test]
fn doctor_raw_mirror_message_count_falls_back_to_one_streaming_legacy_table_scan() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let db_path = temp.path().join("doctor-inventory-legacy.db");
    let conn = crate::franken_sync::Connection::open(db_path.to_string_lossy().as_ref())
        .expect("open legacy fixture database");
    conn.execute_batch(
        "CREATE TABLE conversations (
             id INTEGER PRIMARY KEY,
             source_path TEXT,
             message_count INTEGER
         );
         CREATE TABLE messages (
             id INTEGER PRIMARY KEY,
             conversation_id INTEGER,
             idx INTEGER,
             content TEXT
         );
         INSERT INTO conversations(id, source_path, message_count)
         VALUES(1, '/legacy/one.jsonl', 999), (2, '/legacy/two.jsonl', 999);
         INSERT INTO messages(id, conversation_id, idx, content)
         VALUES(5, 1, 8, 'one'), (99, 1, 2, 'two'), (1000, 2, 400, 'three');",
    )
    .expect("seed legacy inventory fixture");

    let candidates = query_doctor_raw_mirror_backfill_candidates(&conn)
        .expect("query legacy raw mirror candidates");
    let counts: Vec<(i64, usize)> = candidates
        .iter()
        .map(|candidate| (candidate.conversation_id, candidate.message_count))
        .collect();
    assert_eq!(
        counts,
        vec![(1, 2), (2, 1)],
        "a legacy table without the modern autoindex must use exact streamed counts, not stale conversation metadata"
    );
}

#[test]
fn doctor_source_inventory_counts_missing_sources_without_calling_them_lost() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let visible_root = data_dir.join("provider-roots/codex");
    std::fs::create_dir_all(&visible_root).expect("create visible root");
    let visible_source = data_dir.join("sessions/visible.jsonl");
    std::fs::create_dir_all(visible_source.parent().unwrap()).expect("create sessions dir");
    std::fs::write(&visible_source, "{}\n").expect("write visible source");
    let missing_source = data_dir.join("sessions/pruned.jsonl");

    let rows = vec![
        DoctorSourceInventoryDbRow {
            provider: "codex".to_string(),
            source_path: Some(visible_source.display().to_string()),
            source_id: "local".to_string(),
            origin_host: None,
            origin_kind: Some("local".to_string()),
            conversation_count: 1,
        },
        DoctorSourceInventoryDbRow {
            provider: "claude".to_string(),
            source_path: Some(missing_source.display().to_string()),
            source_id: "local".to_string(),
            origin_host: None,
            origin_kind: Some("local".to_string()),
            conversation_count: 2,
        },
        DoctorSourceInventoryDbRow {
            provider: "  ".to_string(),
            source_path: None,
            source_id: "local".to_string(),
            origin_host: None,
            origin_kind: Some("local".to_string()),
            conversation_count: 1,
        },
        DoctorSourceInventoryDbRow {
            provider: "codex".to_string(),
            source_path: Some("/remote/codex/session.jsonl".to_string()),
            source_id: "work-laptop".to_string(),
            origin_host: Some("user@work-laptop".to_string()),
            origin_kind: Some("ssh".to_string()),
            conversation_count: 3,
        },
    ];

    let first = build_doctor_source_inventory_report(
        &data_dir,
        true,
        None,
        rows.clone(),
        vec![
            ("codex".to_string(), visible_root.clone()),
            ("Claude".to_string(), PathBuf::from("/outside/claude")),
        ],
    );
    let second = build_doctor_source_inventory_report(
        &data_dir,
        true,
        None,
        rows,
        vec![
            ("codex".to_string(), visible_root),
            ("Claude".to_string(), PathBuf::from("/outside/claude")),
        ],
    );

    assert_eq!(first.total_indexed_conversations, 7);
    assert_eq!(first.provider_counts.get("codex"), Some(&4));
    assert_eq!(first.provider_counts.get("claude_code"), Some(&2));
    assert_eq!(first.provider_counts.get("unknown"), Some(&1));
    assert_eq!(first.missing_current_source_count, 2);
    assert_eq!(first.unknown_mapping_count, 1);
    assert_eq!(first.remote_source_count, 3);
    assert_eq!(first.local_source_count, 4);

    let codex_root = first
        .detected_roots
        .iter()
        .find(|root| root.provider == "codex")
        .expect("codex root");
    assert!(codex_root.exists);
    assert_eq!(codex_root.redacted_path, "[cass-data]/provider-roots/codex");

    assert_eq!(
        first.detected_roots[0].stable_source_id, second.detected_roots[0].stable_source_id,
        "stable source identifiers must be deterministic across identical inventories"
    );
    assert!(
        first.sources.iter().any(|source| {
            source.is_remote
                && source.source_id == "work-laptop"
                && source.origin_kind == "remote"
                && source.origin_host.as_deref() == Some("user@work-laptop")
        }),
        "remote/source identity should be preserved when known: {:#?}",
        first.sources
    );
    let local_source = first
        .sources
        .iter()
        .find(|source| !source.is_remote && source.source_id == "local")
        .expect("local source identity");
    assert_eq!(local_source.conversation_count, 4);
    assert_eq!(
        local_source.providers,
        vec![
            "claude_code".to_string(),
            "codex".to_string(),
            "unknown".to_string(),
        ],
        "source identities should aggregate providers for the same origin"
    );
    assert!(
        first
            .notes
            .iter()
            .any(|note| note.contains("not proof") || note.contains("archive database")),
        "source_inventory notes should explain that missing upstream files are not archive data loss"
    );

    let payload = serde_json::json!({ "source_inventory": first });
    assert_eq!(
        payload["source_inventory"]["missing_current_source_count"].as_u64(),
        Some(2)
    );
    assert!(
        payload["source_inventory"]["providers"].is_array(),
        "source_inventory JSON should remain parseable and array-backed"
    );
}

#[test]
fn doctor_source_inventory_classifies_named_local_id_by_kind() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = data_dir.join("sessions/backup.jsonl");
    std::fs::create_dir_all(source_path.parent().expect("source parent"))
        .expect("create source parent");
    std::fs::write(&source_path, "{}\n").expect("write named local source");

    let report = build_doctor_source_inventory_report(
        &data_dir,
        true,
        None,
        vec![DoctorSourceInventoryDbRow {
            provider: "codex".to_string(),
            source_path: Some(source_path.display().to_string()),
            source_id: "backup-local".to_string(),
            origin_host: None,
            origin_kind: Some("local".to_string()),
            conversation_count: 2,
        }],
        Vec::new(),
    );

    assert_eq!(report.local_source_count, 2);
    assert_eq!(report.remote_source_count, 0);
    let source = report.sources.first().expect("named local inventory row");
    assert_eq!(source.source_id, "backup-local");
    assert_eq!(source.origin_kind, "local");
    assert!(!source.is_remote);
}

#[test]
fn doctor_structured_details_are_bounded_without_weakening_summary_counts() {
    let detail_count = DOCTOR_STRUCTURED_DETAIL_DEFAULT_LIMIT + 7;
    let mut bounded_backfill = DoctorRawMirrorBackfillReport {
        total_candidate_count: detail_count,
        source_missing_count: detail_count,
        receipts: (0..detail_count)
            .map(|index| DoctorRawMirrorBackfillReceipt {
                stable_record_id: format!("receipt-{index:04}"),
                source_missing: true,
                ..DoctorRawMirrorBackfillReceipt::default()
            })
            .collect(),
        ..DoctorRawMirrorBackfillReport::default()
    };
    let mut bounded_warnings = (0..detail_count)
        .map(|index| DoctorSoleCopyWarning {
            stable_warning_id: format!("warning-{index:04}"),
            ..DoctorSoleCopyWarning::default()
        })
        .collect::<Vec<_>>();

    let bounded_meta = bounded_doctor_structured_details(
        &mut bounded_backfill,
        &mut bounded_warnings,
        false,
        "cass doctor check --json --verbose",
    );
    assert_eq!(
        bounded_backfill.receipts.len(),
        DOCTOR_STRUCTURED_DETAIL_DEFAULT_LIMIT
    );
    assert_eq!(
        bounded_warnings.len(),
        DOCTOR_STRUCTURED_DETAIL_DEFAULT_LIMIT
    );
    assert_eq!(bounded_backfill.total_candidate_count, detail_count);
    assert_eq!(bounded_backfill.source_missing_count, detail_count);
    assert_eq!(bounded_meta["details_truncated"], true);
    assert_eq!(
        bounded_meta["raw_mirror_backfill_receipts_total"],
        detail_count
    );
    assert_eq!(
        bounded_meta["raw_mirror_backfill_receipts_omitted"],
        detail_count - DOCTOR_STRUCTURED_DETAIL_DEFAULT_LIMIT
    );
    assert_eq!(bounded_meta["sole_copy_warnings_total"], detail_count);
    assert_eq!(
        bounded_meta["full_details_command"],
        "cass doctor check --json --verbose"
    );

    let mut full_backfill = DoctorRawMirrorBackfillReport {
        receipts: (0..detail_count)
            .map(|index| DoctorRawMirrorBackfillReceipt {
                stable_record_id: format!("receipt-{index:04}"),
                ..DoctorRawMirrorBackfillReceipt::default()
            })
            .collect(),
        ..DoctorRawMirrorBackfillReport::default()
    };
    let mut full_warnings = (0..detail_count)
        .map(|index| DoctorSoleCopyWarning {
            stable_warning_id: format!("warning-{index:04}"),
            ..DoctorSoleCopyWarning::default()
        })
        .collect::<Vec<_>>();
    let full_meta = bounded_doctor_structured_details(
        &mut full_backfill,
        &mut full_warnings,
        true,
        "cass doctor check --json --verbose",
    );
    assert_eq!(full_backfill.receipts.len(), detail_count);
    assert_eq!(full_warnings.len(), detail_count);
    assert_eq!(full_meta["details_truncated"], false);
    assert_eq!(full_meta["limit_per_collection"], serde_json::Value::Null);
    assert_eq!(full_meta["full_details_command"], serde_json::Value::Null);
}

/// f3grf: archives indexed before the fix hold, for every Claude source, a
/// pre-parse manifest under provider `claude` beside the linked
/// `claude_code` one. Same bytes at the same path are not unlinked
/// evidence; a different file, or the same path with other bytes, is.
#[test]
fn f3grf_coverage_counts_alias_captures_apart_from_unlinked_manifests() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let sessions_dir = data_dir.join("sessions");
    std::fs::create_dir_all(&sessions_dir).expect("create sessions dir");
    let claude_source = sessions_dir.join("claude-session.jsonl");
    let claude_bytes = b"{\"type\":\"user\",\"text\":\"claude\"}\n";
    let link = DoctorRawMirrorDbLink {
        conversation_id: Some(1),
        message_count: Some(2),
        source_path: Some(claude_source.display().to_string()),
        started_at_ms: Some(1_700_000_000_000),
    };
    let write = |provider: &str, path: &Path, bytes: &[u8], links| {
        let manifest = raw_mirror_test_manifest(&data_dir, provider, "local", path, bytes, links);
        write_raw_mirror_test_manifest(&data_dir, &manifest, bytes);
    };
    write("claude_code", &claude_source, claude_bytes, vec![link]);
    write("claude", &claude_source, claude_bytes, Vec::new());
    // Unlinked evidence that must still count: the same path with older
    // bytes, and a file no archive row names.
    write(
        "claude",
        &claude_source,
        b"{\"type\":\"user\",\"text\":\"earlier\"}\n",
        Vec::new(),
    );
    write(
        "codex",
        &sessions_dir.join("orphan.jsonl"),
        b"{\"type\":\"message\",\"text\":\"orphan\"}\n",
        Vec::new(),
    );
    let raw_mirror = collect_doctor_raw_mirror_report(&data_dir);
    assert_eq!(raw_mirror.manifests.len(), 4);
    let source_inventory =
        build_doctor_source_inventory_report(&data_dir, true, None, Vec::new(), Vec::new());

    let summary = build_doctor_coverage_summary(
        &source_inventory,
        &raw_mirror,
        &DoctorRawMirrorBackfillReport::default(),
        &[],
    );

    assert_eq!(summary.raw_mirror_db_link_count, 1);
    assert_eq!(summary.mirror_alias_of_linked_count, 1);
    assert_eq!(summary.mirror_without_db_link_count, 2);
    assert!(summary.coverage_reducing_live_source_rebuild_refused);
}

#[test]
fn doctor_coverage_summary_classifies_ledger_gaps_and_confidence_tiers() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let sessions_dir = data_dir.join("sessions");
    std::fs::create_dir_all(&sessions_dir).expect("create sessions dir");
    let live_source = sessions_dir.join("live.jsonl");
    let missing_source = sessions_dir.join("pruned.jsonl");
    std::fs::write(&live_source, b"live bytes").expect("write live source");

    let rows = vec![
        DoctorSourceInventoryDbRow {
            provider: "codex".to_string(),
            source_path: Some(live_source.display().to_string()),
            source_id: "local".to_string(),
            origin_host: None,
            origin_kind: Some("local".to_string()),
            conversation_count: 1,
        },
        DoctorSourceInventoryDbRow {
            provider: "codex".to_string(),
            source_path: Some(missing_source.display().to_string()),
            source_id: "local".to_string(),
            origin_host: None,
            origin_kind: Some("local".to_string()),
            conversation_count: 1,
        },
        DoctorSourceInventoryDbRow {
            provider: "unknown".to_string(),
            source_path: None,
            source_id: "local".to_string(),
            origin_host: None,
            origin_kind: Some("local".to_string()),
            conversation_count: 1,
        },
        DoctorSourceInventoryDbRow {
            provider: "claude_code".to_string(),
            source_path: Some("/remote/claude/session.jsonl".to_string()),
            source_id: "workstation-a".to_string(),
            origin_host: Some("user@workstation-a".to_string()),
            origin_kind: Some("ssh".to_string()),
            conversation_count: 1,
        },
    ];
    let source_inventory =
        build_doctor_source_inventory_report(&data_dir, true, None, rows, Vec::new());

    let mirrored_bytes = b"{\"type\":\"message\",\"text\":\"mirrored\"}\n";
    let mirrored_manifest = raw_mirror_test_manifest(
        &data_dir,
        "codex",
        "local",
        &missing_source,
        mirrored_bytes,
        vec![DoctorRawMirrorDbLink {
            conversation_id: Some(2),
            message_count: Some(3),
            source_path: Some(missing_source.display().to_string()),
            started_at_ms: Some(1_700_000_000_000),
        }],
    );
    write_raw_mirror_test_manifest(&data_dir, &mirrored_manifest, mirrored_bytes);
    let unlinked_bytes = b"{\"type\":\"message\",\"text\":\"orphan mirror\"}\n";
    let unlinked_manifest = raw_mirror_test_manifest(
        &data_dir,
        "codex",
        "local",
        &sessions_dir.join("orphan.jsonl"),
        unlinked_bytes,
        Vec::new(),
    );
    write_raw_mirror_test_manifest(&data_dir, &unlinked_manifest, unlinked_bytes);
    let raw_mirror = collect_doctor_raw_mirror_report(&data_dir);

    let backfill = DoctorRawMirrorBackfillReport {
        schema_version: 1,
        db_available: true,
        total_candidate_count: 3,
        source_missing_count: 1,
        db_projection_only_count: 1,
        receipts: vec![
            DoctorRawMirrorBackfillReceipt {
                stable_record_id: "live-row".to_string(),
                conversation_id: Some(1),
                provider: "codex".to_string(),
                source_id: "local".to_string(),
                origin_kind: "local".to_string(),
                message_count: 2,
                started_at_ms: Some(1_700_000_000_000),
                redacted_source_path: Some("[cass-data]/sessions/live.jsonl".to_string()),
                source_path_blake3: Some(doctor_raw_mirror_backfill_source_path_blake3(
                    &live_source.display().to_string(),
                )),
                source_stat_snapshot: Some(DoctorRawMirrorBackfillSourceStatSnapshot {
                    exists: true,
                    file_type: "file".to_string(),
                    size_bytes: Some(10),
                    modified_at_ms: Some(1_700_000_000_001),
                    content_blake3: Some("live-hash".to_string()),
                    stat_error: None,
                }),
                ..DoctorRawMirrorBackfillReceipt::default()
            },
            DoctorRawMirrorBackfillReceipt {
                stable_record_id: "missing-mirrored-row".to_string(),
                conversation_id: Some(2),
                provider: "codex".to_string(),
                source_id: "local".to_string(),
                origin_kind: "local".to_string(),
                message_count: 3,
                started_at_ms: Some(1_700_000_000_000),
                redacted_source_path: Some("[cass-data]/sessions/pruned.jsonl".to_string()),
                source_path_blake3: Some(doctor_raw_mirror_backfill_source_path_blake3(
                    &missing_source.display().to_string(),
                )),
                raw_source_captured: true,
                raw_mirror_db_linked: true,
                source_missing: true,
                parse_loss_unknown: false,
                raw_mirror_manifest_id: Some(mirrored_manifest.manifest_id.clone()),
                raw_mirror_blob_blake3: Some(mirrored_manifest.blob_blake3.clone()),
                raw_mirror_blob_size_bytes: Some(mirrored_bytes.len() as u64),
                source_stat_snapshot: Some(DoctorRawMirrorBackfillSourceStatSnapshot {
                    exists: false,
                    file_type: "missing".to_string(),
                    stat_error: Some("not found".to_string()),
                    ..DoctorRawMirrorBackfillSourceStatSnapshot::default()
                }),
                ..DoctorRawMirrorBackfillReceipt::default()
            },
            DoctorRawMirrorBackfillReceipt {
                stable_record_id: "unknown-row".to_string(),
                conversation_id: Some(3),
                provider: "unknown".to_string(),
                source_id: "local".to_string(),
                origin_kind: "local".to_string(),
                message_count: 5,
                db_projection_only: true,
                parse_loss_unknown: true,
                ..DoctorRawMirrorBackfillReceipt::default()
            },
        ],
        ..DoctorRawMirrorBackfillReport::default()
    };

    let sole_copy_warnings = build_doctor_sole_copy_warnings(&backfill);
    let summary = build_doctor_coverage_summary(
        &source_inventory,
        &raw_mirror,
        &backfill,
        &sole_copy_warnings,
    );

    assert_eq!(sole_copy_warnings.len(), 1);
    assert_eq!(sole_copy_warnings[0].confidence_tier, "verified_raw_mirror");
    assert!(sole_copy_warnings[0].raw_source_captured);
    assert!(!sole_copy_warnings[0].db_projection_only);
    let rendered_warning =
        serde_json::to_string(&sole_copy_warnings[0]).expect("sole-copy warning json");
    assert!(
        !rendered_warning.contains(&missing_source.display().to_string()),
        "sole-copy warnings must stay redacted and hash-addressed"
    );

    assert_eq!(summary.archive_conversation_count, 4);
    assert_eq!(summary.archived_message_count, 10);
    assert_eq!(summary.provider_count, 3);
    assert_eq!(summary.visible_current_source_count, 1);
    assert_eq!(summary.visible_current_source_bytes, 10);
    assert_eq!(summary.raw_mirror_manifest_count, 2);
    assert_eq!(summary.raw_mirror_db_link_count, 1);
    assert_eq!(summary.db_without_raw_mirror_count, 2);
    assert_eq!(summary.db_projection_only_count, 1);
    assert_eq!(summary.mirror_without_db_link_count, 1);
    assert_eq!(summary.missing_current_source_count, 1);
    assert_eq!(summary.sole_copy_candidate_count, 1);
    assert_eq!(summary.current_source_newer_than_archive_count, 1);
    assert_eq!(summary.remote_source_count, 1);
    assert_eq!(summary.unknown_mapping_count, 1);
    assert_eq!(summary.earliest_started_at_ms, Some(1_700_000_000_000));
    assert_eq!(summary.latest_started_at_ms, Some(1_700_000_000_000));
    assert!(summary.coverage_reducing_live_source_rebuild_refused);

    let risk = doctor_coverage_risk_summary(&summary, sole_copy_warnings.len(), false);
    assert_eq!(risk.status, "sole_copy_risk");
    assert_eq!(risk.sole_copy_warning_count, 1);
    assert_eq!(risk.raw_mirror_db_link_count, 1);
    assert_eq!(risk.db_without_raw_mirror_count, 2);
    assert_eq!(risk.mirror_without_db_link_count, 1);
    assert_eq!(risk.current_source_newer_than_archive_count, 1);

    assert_eq!(
        doctor_coverage_confidence_tier(1, 0, 0, 1, 1, 0, 0),
        "sole_copy_verified_raw_mirror"
    );
    assert_eq!(
        doctor_coverage_confidence_tier(1, 1, 1, 1, 0, 0, 0),
        "sole_copy_db_projection"
    );
    assert_eq!(
        doctor_coverage_confidence_tier(1, 0, 0, 0, 1, 0, 1),
        "current_source_newer_than_archive"
    );
    assert_eq!(
        doctor_coverage_confidence_tier(0, 0, 0, 0, 0, 1, 0),
        "raw_mirror_unlinked"
    );
    let mirror_only_risk = doctor_coverage_risk_summary(
        &DoctorCoverageSummary {
            confidence_tier: "raw_mirror_unlinked".to_string(),
            mirror_without_db_link_count: 1,
            recommended_action: "inspect mirror".to_string(),
            ..DoctorCoverageSummary::default()
        },
        0,
        false,
    );
    assert_eq!(mirror_only_risk.status, "raw_mirror_unlinked");
    assert_eq!(mirror_only_risk.mirror_without_db_link_count, 1);
}

/// GH #506: with capture switched off, archive rows without a raw copy are
/// disclosed as the operator's choice (low risk, not a degraded archive),
/// while a sole copy still reports high risk.
#[test]
fn gh506_capture_disabled_unmirrored_rows_are_disclosed_not_a_gap() {
    let unmirrored = DoctorCoverageSummary {
        confidence_tier: "archive_db_without_raw_mirror".to_string(),
        archive_conversation_count: 3,
        db_without_raw_mirror_count: 3,
        recommended_action: "inspect".to_string(),
        ..DoctorCoverageSummary::default()
    };

    let enabled = doctor_coverage_risk_summary(&unmirrored, 0, false);
    assert_eq!(enabled.status, "raw_mirror_backfill_available");
    assert!(!enabled.raw_mirror_capture_disabled);
    assert_eq!(doctor_summary_risk_level(&enabled), "medium");
    assert_eq!(
        doctor_summary_health_class(&enabled, None, true),
        "degraded-archive-risk"
    );
    assert_eq!(doctor_risk_level_for_reports(&enabled, &[]), "medium");

    let disabled = doctor_coverage_risk_summary(&unmirrored, 0, true);
    assert_eq!(disabled.status, "raw_mirror_capture_disabled");
    assert!(disabled.raw_mirror_capture_disabled);
    assert_eq!(
        doctor_summary_coverage_state(&disabled),
        "raw_mirror_capture_disabled"
    );
    assert_eq!(
        doctor_summary_source_mirror_state(&disabled),
        "archive_rows_without_raw_mirror"
    );
    assert_eq!(doctor_summary_risk_level(&disabled), "low");
    assert_eq!(
        doctor_summary_health_class(&disabled, None, true),
        "healthy"
    );
    assert_eq!(doctor_risk_level_for_reports(&disabled, &[]), "none");

    // The switch does not hide a sole copy or a missing source.
    let sole_copy = doctor_coverage_risk_summary(
        &DoctorCoverageSummary {
            missing_current_source_count: 1,
            ..unmirrored.clone()
        },
        1,
        true,
    );
    assert_eq!(sole_copy.status, "sole_copy_risk");
    assert_eq!(doctor_summary_risk_level(&sole_copy), "high");
    assert_eq!(
        doctor_summary_health_class(&sole_copy, None, true),
        "degraded-archive-risk"
    );
}

#[test]
fn doctor_coverage_comparison_gate_blocks_data_reducing_candidates() {
    let coverage_summary = DoctorCoverageSummary {
        confidence_tier: "sole_copy_verified_raw_mirror".to_string(),
        archive_conversation_count: 3,
        archived_message_count: 9,
        provider_count: 2,
        source_identity_count: 2,
        visible_current_source_count: 1,
        raw_mirror_db_link_count: 3,
        missing_current_source_count: 1,
        db_without_raw_mirror_count: 1,
        sole_copy_candidate_count: 1,
        earliest_started_at_ms: Some(1_700_000_000_000),
        latest_started_at_ms: Some(1_700_000_005_000),
        ..DoctorCoverageSummary::default()
    };

    let gate = build_doctor_coverage_comparison_gate(
        &coverage_summary,
        &doctor_test_source_authority_report(),
        Some(2),
        Some(8),
        Some(7),
        None,
    );

    assert_eq!(gate.status, "blocked");
    assert!(!gate.promote_allowed);
    assert!(gate.safe_to_inspect);
    assert_eq!(gate.conversation_delta, Some(-1));
    assert_eq!(gate.message_delta, Some(-1));
    assert_eq!(gate.lexical_document_delta, Some(-2));
    assert!(
        gate.blocking_reasons
            .iter()
            .any(|reason| reason.contains("conversation")),
        "canonical conversation loss must block promotion: {gate:#?}"
    );
    assert!(
        gate.blocking_reasons
            .iter()
            .any(|reason| reason.contains("message")),
        "canonical message loss must block promotion: {gate:#?}"
    );
    assert!(
        gate.warning_reasons
            .iter()
            .any(|reason| reason.contains("lexical index")),
        "derived lexical mismatch should be a warning rather than the canonical blocker: {gate:#?}"
    );
    let rendered = serde_json::to_string(&gate).expect("gate json");
    assert!(
        !rendered.contains("/tmp/"),
        "coverage gate details should be count and evidence based, not exact local paths"
    );
}

#[test]
fn doctor_coverage_comparison_gate_warns_on_derived_only_mismatches() {
    let coverage_summary = DoctorCoverageSummary {
        confidence_tier: "verified_raw_mirror_coverage".to_string(),
        archive_conversation_count: 1,
        archived_message_count: 4,
        raw_mirror_db_link_count: 1,
        ..DoctorCoverageSummary::default()
    };

    let gate = build_doctor_coverage_comparison_gate(
        &coverage_summary,
        &doctor_test_source_authority_report(),
        Some(1),
        Some(4),
        Some(3),
        Some(2),
    );

    assert_eq!(gate.status, "warn");
    assert!(gate.promote_allowed);
    assert!(gate.blocking_reasons.is_empty());
    assert_eq!(gate.conversation_delta, Some(0));
    assert_eq!(gate.message_delta, Some(0));
    assert_eq!(gate.lexical_document_delta, Some(-1));
    assert_eq!(gate.semantic_vector_delta, Some(-2));
    assert!(
        gate.warning_reasons
            .iter()
            .any(|reason| reason.contains("semantic index")),
        "semantic gaps are opportunistic derived warnings: {gate:#?}"
    );
}

#[test]
fn doctor_source_authority_matrix_covers_every_kind_and_refuses_derived_outputs() {
    let policy_kinds: HashSet<_> = DOCTOR_SOURCE_AUTHORITY_POLICY_TABLE
        .iter()
        .map(|policy| policy.authority)
        .collect();
    let expected_kinds: HashSet<_> = ALL_DOCTOR_SOURCE_AUTHORITIES.iter().copied().collect();
    assert_eq!(
        policy_kinds, expected_kinds,
        "every source authority kind must have an explicit precedence policy"
    );
    assert_eq!(
        DOCTOR_SOURCE_AUTHORITY_POLICY_TABLE.len(),
        policy_kinds.len(),
        "source authority policy table must not contain duplicate authorities"
    );

    let lexical = doctor_source_authority_policy(DoctorSourceAuthorityKind::DerivedLexicalIndex);
    assert!(lexical.repairs.is_empty());
    assert_eq!(
        lexical.decision_when_valid,
        DoctorSourceAuthorityDecision::Refused,
        "derived lexical indexes are repair targets, not archive authorities"
    );

    let candidate =
        doctor_source_authority_policy(DoctorSourceAuthorityKind::VerifiedCandidateArchive);
    assert!(
        candidate
            .required_evidence
            .contains(&"coverage-nondecreasing"),
        "candidate promotion must require non-decreasing coverage"
    );
    assert_eq!(
        candidate.decision_when_valid,
        DoctorSourceAuthorityDecision::Promotable
    );
}

#[test]
fn doctor_source_authority_report_rejects_pruned_live_source_but_keeps_verified_mirror() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let db_path = data_dir.join("agent_search.db");
    std::fs::write(&db_path, b"db placeholder").expect("write db placeholder");
    let missing_source = data_dir.join("sessions/pruned.jsonl");
    let rows = vec![DoctorSourceInventoryDbRow {
        provider: "codex".to_string(),
        source_path: Some(missing_source.display().to_string()),
        source_id: "local".to_string(),
        origin_host: None,
        origin_kind: Some("local".to_string()),
        conversation_count: 1,
    }];
    let source_inventory =
        build_doctor_source_inventory_report(&data_dir, true, None, rows, Vec::new());
    let bytes = b"{\"type\":\"message\",\"content\":\"preserved\"}\n";
    let manifest = raw_mirror_test_manifest(
        &data_dir,
        "codex",
        "local",
        &missing_source,
        bytes,
        vec![DoctorRawMirrorDbLink {
            conversation_id: Some(1),
            message_count: Some(1),
            source_path: Some(missing_source.display().to_string()),
            started_at_ms: Some(1_733_000_000_000),
        }],
    );
    write_raw_mirror_test_manifest(&data_dir, &manifest, bytes);
    let raw_mirror = collect_doctor_raw_mirror_report(&data_dir);
    let report = build_doctor_source_authority_report(&db_path, &source_inventory, &raw_mirror);

    assert_eq!(
        report.selected_authority,
        Some(DoctorSourceAuthorityKind::CanonicalArchiveDb)
    );
    assert!(
        report.selected_authorities.iter().any(|candidate| {
            candidate.authority == DoctorSourceAuthorityKind::VerifiedRawMirror
                && candidate.decision == DoctorSourceAuthorityDecision::CandidateOnly
                && candidate.coverage_delta == 0
        }),
        "verified raw mirror should remain a candidate authority when upstream is pruned: {report:#?}"
    );
    assert!(
        report.rejected_authorities.iter().any(|candidate| {
            candidate.authority == DoctorSourceAuthorityKind::LiveUpstreamSource
                && candidate.reason.contains("missing local conversation")
                && candidate.coverage_delta < 0
                && candidate
                    .evidence
                    .contains(&"coverage-shrinks-relative-to-archive".to_string())
        }),
        "pruned live source must be rejected with stable coverage evidence: {report:#?}"
    );
    assert_eq!(
        report.checksum_evidence.summary_status,
        DoctorArtifactChecksumStatus::Matched
    );
    assert_eq!(report.coverage_delta.raw_mirror_db_link_count, 1);
}

#[test]
fn doctor_source_authority_prefers_verified_mirror_over_unreadable_archive() {
    // #374 Issue 1: an unopenable/malformed archive DB must not be selected
    // as repair authority over a checksum-verified raw mirror, and stale
    // capture debris (interrupted_capture) must not veto an otherwise
    // blob-clean mirror.
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let db_path = data_dir.join("agent_search.db");
    std::fs::write(&db_path, b"malformed db").expect("write db placeholder");
    let source_path = data_dir.join("sessions/live.jsonl");

    // The archive exists but the read-only open/query failed: db_available
    // is false and the concrete cause is carried as db_query_error.
    let source_inventory = build_doctor_source_inventory_report(
        &data_dir,
        false,
        Some("database disk image is malformed".to_string()),
        Vec::new(),
        Vec::new(),
    );

    let bytes = b"{\"type\":\"message\",\"content\":\"preserved\"}\n";
    let manifest = raw_mirror_test_manifest(
        &data_dir,
        "codex",
        "local",
        &source_path,
        bytes,
        vec![DoctorRawMirrorDbLink {
            conversation_id: Some(1),
            message_count: Some(1),
            source_path: Some(source_path.display().to_string()),
            started_at_ms: Some(1_733_000_000_000),
        }],
    );
    write_raw_mirror_test_manifest(&data_dir, &manifest, bytes);

    // Stale interrupted-capture debris: a leftover file under the mirror's
    // tmp/ dir downgrades status to "warn" without harming blob integrity.
    let mirror_tmp = doctor_raw_mirror_root(&data_dir).join("tmp/op-123");
    std::fs::create_dir_all(&mirror_tmp).expect("create mirror tmp");
    std::fs::write(mirror_tmp.join("model.safetensors.tmp"), b"partial").expect("write debris");

    let raw_mirror = collect_doctor_raw_mirror_report(&data_dir);
    assert_eq!(
        raw_mirror.status, "warn",
        "stale capture debris should mark aggregate status warn"
    );
    assert_eq!(raw_mirror.summary.missing_blob_count, 0);
    assert_eq!(raw_mirror.summary.checksum_mismatch_count, 0);

    let report = build_doctor_source_authority_report(&db_path, &source_inventory, &raw_mirror);

    assert_eq!(
        report.selected_authority,
        Some(DoctorSourceAuthorityKind::VerifiedRawMirror),
        "blob-clean mirror must outrank an unreadable archive despite warn status: {report:#?}"
    );
    assert!(
        !report
            .selected_authorities
            .iter()
            .any(|c| c.authority == DoctorSourceAuthorityKind::CanonicalArchiveDb),
        "an unreadable archive must not be a selected authority: {report:#?}"
    );
    assert!(
        !report
            .selected_authorities
            .iter()
            .chain(report.rejected_authorities.iter())
            .any(|c| c.evidence.iter().any(|e| e == "archive-db-opened")),
        "a failed open must not emit archive-db-opened evidence: {report:#?}"
    );
}

#[test]
fn doctor_read_only_next_command_routes_by_failing_asset_class() {
    // #374 Defect 2B: a read-only `doctor check` must not recommend `--fix`
    // for a failing canonical-archive or source-authority check, since
    // SafeAutoRun (`--fix`) can only mutate derived-cleanup asset classes.
    let fail_db = doctor_check_report("database", "fail", "archive unreadable", false, false);
    assert_eq!(
        doctor_read_only_next_command(std::slice::from_ref(&fail_db)),
        "cass doctor repair --dry-run --json"
    );

    let fail_source =
        doctor_check_report("source_coverage", "fail", "coverage shrank", false, false);
    assert_eq!(
        doctor_read_only_next_command(std::slice::from_ref(&fail_source)),
        "cass doctor archive-scan --json"
    );

    // All-pass (or only derived-cleanup findings) keeps the safe auto-fix path.
    let pass_db = doctor_check_report("database", "pass", "Database OK", true, true);
    assert_eq!(
        doctor_read_only_next_command(std::slice::from_ref(&pass_db)),
        "cass doctor --fix --json"
    );

    // An archive fault outranks an incidental cleanup failure.
    let cleanup = doctor_check_report("derivative_cleanup", "fail", "stale cache", true, true);
    assert_eq!(
        doctor_read_only_next_command(&[cleanup, fail_db]),
        "cass doctor repair --dry-run --json"
    );

    // 2l1b0.73: a leak-only integrity failure routes to its in-place
    // repair, even beside another failing archive check...
    let leak_message = format!(
        "Database failed frankensqlite integrity_check: database disk image is malformed: page 8196 is never used (2715 conversations, 3683202 messages){}",
        doctor_leaked_pages_remedy_suffix(true)
    );
    let fail_leak = doctor_check_report("database", "fail", &leak_message, false, false);
    let fail_staging = doctor_check_report("candidate_staging", "fail", "blocked", false, false);
    assert_eq!(
        doctor_read_only_next_command(&[fail_staging, fail_leak]),
        "cass doctor --repair-leaked-pages --dry-run --json"
    );
    // ...while any other integrity failure keeps the generic planner.
    let other_message = format!(
        "Database failed frankensqlite integrity_check: database disk image is malformed: page 7 is referenced multiple times (1 conversations, 2 messages){}",
        doctor_leaked_pages_remedy_suffix(false)
    );
    let fail_other = doctor_check_report("database", "fail", &other_message, false, false);
    assert_eq!(
        doctor_read_only_next_command(std::slice::from_ref(&fail_other)),
        "cass doctor repair --dry-run --json"
    );
}

#[test]
fn doctor_source_authority_report_refuses_unverified_mirror_when_archive_missing() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let db_path = data_dir.join("agent_search.db");
    let source_inventory =
        build_doctor_source_inventory_report(&data_dir, false, None, Vec::new(), Vec::new());
    let raw_mirror = collect_doctor_raw_mirror_report(&data_dir);
    let report = build_doctor_source_authority_report(&db_path, &source_inventory, &raw_mirror);

    assert_eq!(report.decision, DoctorSourceAuthorityDecision::Refused);
    assert_eq!(report.selected_authority, None);
    assert!(
        report.rejected_authorities.iter().any(|candidate| {
            candidate.authority == DoctorSourceAuthorityKind::VerifiedRawMirror
                && candidate.reason.contains("not trusted")
                && candidate.checksum_status == DoctorArtifactChecksumStatus::NotRecorded
        }),
        "missing mirror should fail closed instead of pretending live sources are authoritative: {report:#?}"
    );
}

#[test]
fn doctor_source_authority_report_rejects_remote_sync_copy_without_identity_and_checksums() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let db_path = data_dir.join("agent_search.db");
    std::fs::write(&db_path, b"db placeholder").expect("write db placeholder");
    let rows = vec![DoctorSourceInventoryDbRow {
        provider: "cline".to_string(),
        source_path: Some("~/.cline/tasks/session.jsonl".to_string()),
        source_id: "work-laptop".to_string(),
        origin_host: Some("work-laptop".to_string()),
        origin_kind: Some("ssh".to_string()),
        conversation_count: 1,
    }];
    let source_inventory =
        build_doctor_source_inventory_report(&data_dir, true, None, rows, Vec::new());
    let raw_mirror = collect_doctor_raw_mirror_report(&data_dir);
    let report = build_doctor_source_authority_report(&db_path, &source_inventory, &raw_mirror);

    assert_eq!(report.coverage_delta.remote_source_count, 1);
    assert!(
        report.rejected_authorities.iter().any(|candidate| {
            candidate.authority == DoctorSourceAuthorityKind::RemoteSyncCopy
                && candidate.decision == DoctorSourceAuthorityDecision::Refused
                && candidate.checksum_status == DoctorArtifactChecksumStatus::NotRecorded
                && candidate
                    .reason
                    .contains("matched source identity, matched origin host")
                && candidate
                    .evidence
                    .contains(&"remote-identity-ambiguous".to_string())
                && candidate
                    .evidence
                    .contains(&"remote-generation-unverified".to_string())
                && candidate
                    .evidence
                    .contains(&"remote-copy-coverage-unknown".to_string())
        }),
        "remote sync copies must stay refused until identity, generation/checksum, and coverage evidence are all verified: {report:#?}"
    );
}
