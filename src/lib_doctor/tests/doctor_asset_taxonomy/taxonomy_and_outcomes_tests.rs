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

/// GH#324: crash-orphaned lexical staging dirs must classify as
/// reclaimable derived cache (safe_to_gc, auto-delete allowed) instead of
/// falling into a protected class that shields the debris from cleanup.
#[test]
fn doctor_classifies_orphaned_lexical_staging_dirs_as_reclaimable() {
    for relative in [
        "index/cass-lexical-shards.k1eHZJ/shard-0/meta.json",
        "index/cass-lexical-merge.2qwt8l",
        "index/cass-lexical-merge.2qwt8l.reclaim-42-0/index/meta.json",
        "index/cass-federated-materialize-r4nd0m/index/meta.json",
        "index/cass-empty-lexical-repair-q1w2e3/file",
        // Historical top-level layout.
        "cass-lexical-shards.abc123/shard-0/meta.json",
        "cass-lexical-merge.abc123",
    ] {
        assert_eq!(
            doctor_classify_storage_relative_path(Path::new(relative)),
            DoctorAssetClass::ReclaimableDerivedCache,
            "{relative} should classify as reclaimable staging debris"
        );
    }
    let reclaimable_safety = doctor_asset_safety(DoctorAssetClass::ReclaimableDerivedCache);
    assert!(reclaimable_safety.safe_to_gc_allowed);
    assert!(reclaimable_safety.auto_delete_allowed);
    assert!(!reclaimable_safety.precious);

    // Live/published assets must keep their protective classes.
    for (relative, expected) in [
        ("index/v8/meta.json", DoctorAssetClass::DerivedLexicalIndex),
        (
            "index/.lexical-publish-backups/gen-1/meta.json",
            DoctorAssetClass::RetainedPublishBackup,
        ),
        ("agent_search.db", DoctorAssetClass::CanonicalArchiveDb),
        // Bare prefixes (no tempfile suffix) are not cass-created names;
        // they stay in the fail-closed lexical-index bucket.
        (
            "index/cass-lexical-shards.",
            DoctorAssetClass::DerivedLexicalIndex,
        ),
    ] {
        assert_eq!(
            doctor_classify_storage_relative_path(Path::new(relative)),
            expected,
            "{relative} must not be classified as reclaimable"
        );
    }
}

#[test]
fn doctor_asset_taxonomy_explicitly_covers_every_class_and_operation() {
    let policy_classes: HashSet<_> = DOCTOR_ASSET_POLICY_TABLE
        .iter()
        .map(|policy| policy.asset_class)
        .collect();
    let expected_classes: HashSet<_> = ALL_DOCTOR_ASSET_CLASSES.iter().copied().collect();
    assert_eq!(
        policy_classes, expected_classes,
        "every doctor asset class must be represented in the central policy table"
    );

    for &asset_class in ALL_DOCTOR_ASSET_CLASSES {
        let policy = doctor_asset_policy(asset_class);
        for &operation in DOCTOR_ASSET_ALL_OPERATIONS {
            let policy_says_allowed = policy.allowed_operations.contains(&operation);
            assert_eq!(
                doctor_asset_allows_operation(asset_class, operation),
                policy_says_allowed,
                "operation matrix drifted for {asset_class:?} / {operation:?}"
            );
        }
    }
}

#[test]
fn precious_archive_evidence_is_never_pruneable_or_safe_to_gc() {
    let precious_classes = [
        DoctorAssetClass::SourceSessionLog,
        DoctorAssetClass::RawMirrorBlob,
        DoctorAssetClass::CanonicalArchiveDb,
        DoctorAssetClass::ArchiveDbSidecar,
        DoctorAssetClass::SourceCoverageLedger,
        DoctorAssetClass::UserConfig,
        DoctorAssetClass::BookmarkStore,
        DoctorAssetClass::OperationReceipt,
        DoctorAssetClass::EventLog,
        DoctorAssetClass::ForensicBundle,
        DoctorAssetClass::SupportBundle,
        DoctorAssetClass::BackupBundle,
        DoctorAssetClass::ExternalUpstreamSource,
    ];

    for asset_class in precious_classes {
        let policy = doctor_asset_policy(asset_class);
        assert!(
            policy.precious,
            "{asset_class:?} should remain marked as precious evidence"
        );
        assert!(
            !policy.auto_delete_allowed,
            "{asset_class:?} must not be auto-deleted"
        );
        assert!(
            !doctor_asset_safe_to_gc(asset_class, true),
            "{asset_class:?} must fail closed even when a caller claims it is safe"
        );
        assert!(
            !doctor_asset_allows_operation(asset_class, DoctorAssetOperation::PruneReclaim),
            "{asset_class:?} must not allow prune/reclaim"
        );
    }
}

#[test]
fn quarantine_artifacts_remain_inspection_only() {
    for asset_class in [
        DoctorAssetClass::FailedSeedBundle,
        DoctorAssetClass::QuarantinedLexicalGeneration,
        DoctorAssetClass::QuarantinedLexicalShard,
    ] {
        let policy = doctor_asset_policy(asset_class);
        assert_eq!(
            policy.safety_classification,
            DoctorAssetSafetyClassification::DiagnosticEvidence
        );
        assert!(
            doctor_asset_allows_operation(asset_class, DoctorAssetOperation::Read),
            "{asset_class:?} should be readable for inspection"
        );
        assert!(
            !doctor_asset_safe_to_gc(asset_class, true),
            "{asset_class:?} must never become safe_to_gc without an explicit reclassification"
        );
        assert!(
            !doctor_asset_allows_operation(asset_class, DoctorAssetOperation::PruneReclaim),
            "{asset_class:?} must not be pruneable while quarantined"
        );
    }
}

#[test]
fn support_bundle_policy_allows_redaction_but_not_pruning() {
    assert!(doctor_asset_allows_operation(
        DoctorAssetClass::SupportBundle,
        DoctorAssetOperation::Redact
    ));
    assert!(doctor_asset_allows_operation(
        DoctorAssetClass::SupportBundle,
        DoctorAssetOperation::Export
    ));
    assert!(!doctor_asset_allows_operation(
        DoctorAssetClass::SupportBundle,
        DoctorAssetOperation::PruneReclaim
    ));
    assert!(!doctor_asset_safe_to_gc(
        DoctorAssetClass::SupportBundle,
        true
    ));
}

#[test]
fn only_derived_reclaimable_classes_can_be_safe_to_gc() {
    for policy in DOCTOR_ASSET_POLICY_TABLE {
        if policy.safe_to_gc_allowed || policy.auto_delete_allowed {
            assert!(
                policy.derived,
                "{:?} cannot be auto-reclaimable unless it is derived",
                policy.asset_class
            );
            assert!(
                !policy.precious,
                "{:?} cannot be both precious and auto-reclaimable",
                policy.asset_class
            );
            assert!(
                doctor_asset_allows_operation(
                    policy.asset_class,
                    DoctorAssetOperation::PruneReclaim
                ),
                "{:?} has safe_to_gc/auto-delete enabled but lacks prune permission",
                policy.asset_class
            );
        }
    }

    assert!(!doctor_asset_safe_to_gc(
        DoctorAssetClass::DerivedLexicalIndex,
        true
    ));
    assert!(doctor_asset_safe_to_gc(
        DoctorAssetClass::ReclaimableDerivedCache,
        true
    ));
    assert!(doctor_asset_safe_to_gc(
        DoctorAssetClass::RetainedPublishBackup,
        true
    ));
}

#[test]
fn unknown_asset_class_fails_closed() {
    let policy = doctor_asset_policy(DoctorAssetClass::Unknown);
    assert_eq!(
        policy.safety_classification,
        DoctorAssetSafetyClassification::Unknown
    );
    assert!(policy.allowed_operations.is_empty());
    assert!(!policy.auto_delete_allowed);
    assert!(!doctor_asset_safe_to_gc(DoctorAssetClass::Unknown, true));
    for &operation in DOCTOR_ASSET_ALL_OPERATIONS {
        assert!(!doctor_asset_allows_operation(
            DoctorAssetClass::Unknown,
            operation
        ));
    }
}

#[test]
fn doctor_anomaly_taxonomy_explicitly_covers_every_class() {
    let policy_anomalies: HashSet<_> = DOCTOR_ANOMALY_POLICY_TABLE
        .iter()
        .map(|policy| policy.anomaly_class)
        .collect();
    let expected_anomalies: HashSet<_> = ALL_DOCTOR_ANOMALIES.iter().copied().collect();
    assert_eq!(
        policy_anomalies, expected_anomalies,
        "every DoctorAnomaly variant needs a central policy row"
    );
    assert_eq!(
        DOCTOR_ANOMALY_POLICY_TABLE.len(),
        expected_anomalies.len(),
        "DoctorAnomaly policy table must not contain duplicate rows"
    );

    let policy_health_classes: HashSet<_> = DOCTOR_ANOMALY_POLICY_TABLE
        .iter()
        .map(|policy| policy.health_class)
        .collect();
    let expected_health_classes: HashSet<_> = ALL_DOCTOR_HEALTH_CLASSES.iter().copied().collect();
    assert_eq!(
        policy_health_classes, expected_health_classes,
        "every DoctorHealth class should be reachable from at least one anomaly"
    );

    for &anomaly in ALL_DOCTOR_ANOMALIES {
        let value = serde_json::to_value(anomaly).expect("serialize DoctorAnomaly");
        let serialized = value
            .as_str()
            .expect("DoctorAnomaly serializes as a string");
        assert!(
            serialized
                .chars()
                .all(|ch| ch.is_ascii_lowercase() || ch == '-'),
            "{anomaly:?} must serialize as stable kebab-case, got {serialized:?}"
        );
        assert!(
            !serialized.contains('_'),
            "{anomaly:?} must not use snake_case in robot contracts"
        );
    }

    for &health_class in ALL_DOCTOR_HEALTH_CLASSES {
        let value = serde_json::to_value(health_class).expect("serialize DoctorHealth");
        let serialized = value.as_str().expect("DoctorHealth serializes as a string");
        assert!(
            serialized
                .chars()
                .all(|ch| ch.is_ascii_lowercase() || ch == '-'),
            "{health_class:?} must serialize as stable kebab-case, got {serialized:?}"
        );
        assert!(
            !serialized.contains('_'),
            "{health_class:?} must not use snake_case in robot contracts"
        );
    }
}

#[test]
fn doctor_anomaly_policy_fails_closed_for_precious_assets() {
    for policy in DOCTOR_ANOMALY_POLICY_TABLE {
        let asset_policy = doctor_asset_policy(policy.affected_asset_class);
        if policy.safe_for_auto_repair {
            assert_eq!(
                policy.health_class,
                DoctorHealth::DegradedDerivedAssets,
                "{:?} must not be auto-repair-safe unless it is derived-only",
                policy.anomaly_class
            );
            assert_eq!(
                policy.data_loss_risk,
                DoctorDataLossRisk::None,
                "{:?} must not be auto-repair-safe while carrying data-loss risk",
                policy.anomaly_class
            );
            assert!(
                !asset_policy.precious,
                "{:?} must not be auto-repair-safe for precious {:?}",
                policy.anomaly_class, policy.affected_asset_class
            );
        }

        if asset_policy.precious
            || matches!(
                policy.health_class,
                DoctorHealth::DegradedArchiveRisk | DoctorHealth::SourceAuthorityUnsafe
            )
        {
            assert!(
                !policy.safe_for_auto_repair,
                "{:?} must fail closed by default",
                policy.anomaly_class
            );
        }
    }
}

#[test]
fn doctor_check_report_serializes_stable_robot_fields() {
    let stale_index = doctor_check_report("index", "fail", "Search index not found", true, false);
    let value = serde_json::to_value(&stale_index).expect("serialize check report");
    assert_eq!(value["anomaly_class"], "derived-lexical-stale");
    assert_eq!(value["health_class"], "degraded-derived-assets");
    assert_eq!(value["severity"], "warn");
    assert_eq!(value["affected_asset_class"], "derived_lexical_index");
    assert_eq!(value["data_loss_risk"], "none");
    assert_eq!(value["recommended_action"], "rebuild-derived-lexical-index");
    assert_eq!(value["safe_for_auto_repair"], true);
    assert_eq!(value["default_outcome_kind"], "planned");
    assert_eq!(value["fix_available"], true);
    assert_eq!(value["fix_applied"], false);

    let source_pruned = doctor_check_report(
        "source_inventory",
        "warn",
        "Source coverage risk: 1 indexed local conversation(s) no longer have a visible upstream file; 0 indexed conversation(s) have incomplete source mapping",
        false,
        false,
    );
    let value = serde_json::to_value(&source_pruned).expect("serialize source report");
    assert_eq!(value["anomaly_class"], "upstream-source-pruned");
    assert_eq!(value["health_class"], "degraded-archive-risk");
    assert_eq!(value["severity"], "warn");
    assert_eq!(value["affected_asset_class"], "external_upstream_source");
    assert_eq!(value["data_loss_risk"], "high");
    assert_eq!(value["safe_for_auto_repair"], false);

    let pass = doctor_check_report("database", "pass", "Database OK", true, true);
    let value = serde_json::to_value(&pass).expect("serialize pass report");
    assert_eq!(value["anomaly_class"], "healthy");
    assert_eq!(value["health_class"], "healthy");
    assert_eq!(value["severity"], "info");
    assert_eq!(value["safe_for_auto_repair"], false);
}

#[test]
fn doctor_check_report_auto_repair_is_gated_by_issue_and_fix_plan() {
    let planned = doctor_check_report("index", "warn", "Index may be stale", true, false);
    assert!(planned.safe_for_auto_repair);

    let no_plan = doctor_check_report("index", "warn", "Index may be stale", false, false);
    assert!(!no_plan.safe_for_auto_repair);

    let passing = doctor_check_report("index", "pass", "Index OK", true, false);
    assert!(!passing.safe_for_auto_repair);

    let precious = doctor_check_report(
        "database",
        "fail",
        "Cannot open database: simulated archive issue",
        true,
        false,
    );
    assert_eq!(
        precious.affected_asset_class,
        DoctorAssetClass::CanonicalArchiveDb
    );
    assert!(!precious.safe_for_auto_repair);
}

#[test]
fn doctor_root_cause_incidents_group_source_pruning_with_mirror_evidence() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path();
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    let checks = vec![
        doctor_check_report(
            "source_inventory",
            "warn",
            "Source coverage risk: 2 indexed local conversation(s) no longer have a visible upstream file; 0 indexed conversation(s) have incomplete source mapping",
            false,
            false,
        ),
        doctor_check_report(
            "raw_mirror_backfill",
            "warn",
            "2 DB-only archive rows appear to be the sole remaining copy but verified raw mirror evidence exists",
            false,
            false,
        ),
    ];
    let coverage_risk = DoctorCoverageRiskSummary {
        schema_version: 1,
        status: "sole_copy_risk".to_string(),
        confidence_tier: "sole_copy_verified_raw_mirror".to_string(),
        archive_conversation_count: 2,
        missing_current_source_count: 2,
        raw_mirror_db_link_count: 2,
        sole_copy_warning_count: 2,
        recommended_action: "Back up the cass data directory.".to_string(),
        ..DoctorCoverageRiskSummary::default()
    };
    let operation_state = doctor_test_operation_state(data_dir, &db_path, &index_path);
    let incidents = build_doctor_root_cause_incidents(
        &checks,
        &coverage_risk,
        &operation_state,
        &DoctorCandidateStagingReport::default(),
        "lexical",
        None,
    );

    assert_eq!(incidents.len(), 1, "{incidents:#?}");
    let incident = &incidents[0];
    assert_eq!(
        incident.root_cause_kind,
        DoctorIncidentRootCauseKind::SourcePrunedWithMirrorIntact
    );
    assert_eq!(incident.confidence, "high");
    assert_eq!(incident.archive_risk_level, DoctorDataLossRisk::High);
    assert_eq!(
        incident.evidence_check_ids,
        vec![
            "raw_mirror_backfill".to_string(),
            "source_inventory".to_string()
        ]
    );
    assert!(
        incident
            .blocked_actions
            .contains(&"source-only-rebuild".to_string()),
        "{incident:#?}"
    );
}

#[test]
fn doctor_root_cause_incidents_collect_lock_paths_and_next_action() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path();
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    let lock_path = data_dir.join("doctor/locks/doctor-repair.lock");
    let operation_state = build_doctor_operation_state_report(
        data_dir,
        &db_path,
        &index_path,
        &crate::search::asset_state::SearchMaintenanceSnapshot::default(),
        &DoctorMutationLockObservation::Active {
            path: lock_path,
            metadata: BTreeMap::new(),
        },
    );
    let checks = vec![
        doctor_check_report(
            "operation_state",
            "fail",
            "Doctor mutation blocked: another doctor repair is active",
            false,
            false,
        ),
        doctor_check_report(
            "lock_file",
            "warn",
            "Stale lock file found but owner could not be proven stale",
            false,
            false,
        ),
    ];
    let incidents = build_doctor_root_cause_incidents(
        &checks,
        &DoctorCoverageRiskSummary {
            schema_version: 1,
            status: "ok".to_string(),
            confidence_tier: "archive_db_coverage".to_string(),
            recommended_action: "none".to_string(),
            ..DoctorCoverageRiskSummary::default()
        },
        &operation_state,
        &DoctorCandidateStagingReport::default(),
        "lexical",
        None,
    );

    assert_eq!(incidents.len(), 1, "{incidents:#?}");
    let incident = &incidents[0];
    assert_eq!(
        incident.root_cause_kind,
        DoctorIncidentRootCauseKind::ActiveLockBlockingRepair
    );
    assert_eq!(incident.severity, DoctorSeverity::Warn);
    assert_eq!(
        incident.evidence_check_ids,
        vec!["lock_file".to_string(), "operation_state".to_string()]
    );
    assert!(
        incident
            .blocked_actions
            .contains(&"mutating-doctor-repair".to_string()),
        "{incident:#?}"
    );
    assert!(
        incident
            .redacted_evidence_paths
            .iter()
            .any(|path| path.contains("[cass-data]")),
        "{incident:#?}"
    );
    assert!(
        incident
            .safe_next_actions
            .iter()
            .any(|action| action.contains("wait") || action.contains("inspect")),
        "{incident:#?}"
    );
}

#[test]
fn doctor_root_cause_incidents_sort_archive_candidate_recovery_before_derived_work() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path();
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    let checks = vec![
        doctor_check_report("index", "warn", "Search index missing", true, false),
        doctor_check_report(
            "database",
            "fail",
            "Cannot open database: simulated archive issue",
            false,
            false,
        ),
    ];
    let candidate_staging = DoctorCandidateStagingReport {
        completed_candidate_count: 1,
        redacted_root_path: "[cass-data]/doctor/candidates".to_string(),
        candidates: vec![DoctorCandidateSummary {
            candidate_id: "candidate-1".to_string(),
            lifecycle_status: "completed".to_string(),
            path: data_dir
                .join("doctor/candidates/candidate-1")
                .display()
                .to_string(),
            redacted_path: "[cass-data]/doctor/candidates/candidate-1".to_string(),
            redacted_manifest_path: Some(
                "[cass-data]/doctor/candidates/candidate-1/manifest.json".to_string(),
            ),
            safe_to_delete_automatically: false,
            ..DoctorCandidateSummary::default()
        }],
        ..DoctorCandidateStagingReport::default()
    };
    let operation_state = doctor_test_operation_state(data_dir, &db_path, &index_path);
    let incidents = build_doctor_root_cause_incidents(
        &checks,
        &DoctorCoverageRiskSummary {
            schema_version: 1,
            status: "ok".to_string(),
            confidence_tier: "archive_db_coverage".to_string(),
            recommended_action: "none".to_string(),
            ..DoctorCoverageRiskSummary::default()
        },
        &operation_state,
        &candidate_staging,
        "lexical",
        None,
    );

    assert_eq!(incidents.len(), 2, "{incidents:#?}");
    assert_eq!(
        incidents[0].root_cause_kind,
        DoctorIncidentRootCauseKind::ArchiveDbUnreadableWithValidCandidate
    );
    assert_eq!(
        incidents[1].root_cause_kind,
        DoctorIncidentRootCauseKind::DerivedIndexStale
    );
    assert_eq!(incidents[0].archive_risk_level, DoctorDataLossRisk::High);
    assert_eq!(incidents[1].derived_risk_level, DoctorDataLossRisk::Low);
}

#[test]
fn doctor_health_class_uses_highest_risk_report() {
    let derived = doctor_check_report("index", "fail", "Search index not found", true, false);
    let source_unsafe = doctor_check_report(
        "sessions",
        "warn",
        "No agent sessions found in configured search paths",
        false,
        false,
    );
    let archive_risk = doctor_check_report(
        "database",
        "fail",
        "Cannot open database: simulated archive issue",
        false,
        false,
    );
    let repair_failed =
        doctor_check_report("rebuild", "fail", "Index rebuild failed: boom", true, false);

    assert_eq!(
        doctor_health_class_for_checks(std::slice::from_ref(&derived)),
        DoctorHealth::DegradedDerivedAssets
    );
    assert_eq!(
        doctor_health_class_for_checks(&[derived.clone(), source_unsafe]),
        DoctorHealth::SourceAuthorityUnsafe
    );
    assert_eq!(
        doctor_health_class_for_checks(&[derived.clone(), archive_risk.clone()]),
        DoctorHealth::DegradedArchiveRisk,
        "archive-risk checks must not be masked by derived-asset failures"
    );
    assert_eq!(
        doctor_health_class_for_checks(&[archive_risk, repair_failed]),
        DoctorHealth::RepairPreviouslyFailed,
        "a previous failed repair should be the top-level state until inspected"
    );
}

#[test]
fn doctor_operation_outcome_contract_covers_every_kind() {
    let policy_kinds: HashSet<_> = DOCTOR_OPERATION_OUTCOME_POLICY_TABLE
        .iter()
        .map(|policy| policy.kind)
        .collect();
    let expected_kinds: HashSet<_> = ALL_DOCTOR_OPERATION_OUTCOMES.iter().copied().collect();
    assert_eq!(
        policy_kinds, expected_kinds,
        "every DoctorOperationOutcomeKind needs an explicit policy row"
    );
    assert_eq!(
        DOCTOR_OPERATION_OUTCOME_POLICY_TABLE.len(),
        expected_kinds.len(),
        "DoctorOperationOutcome policy table must not contain duplicates"
    );

    for &kind in ALL_DOCTOR_OPERATION_OUTCOMES {
        let value = serde_json::to_value(kind).expect("serialize operation outcome kind");
        let serialized = value
            .as_str()
            .expect("DoctorOperationOutcomeKind serializes as a string");
        assert!(
            serialized
                .chars()
                .all(|ch| ch.is_ascii_lowercase() || ch == '-'),
            "{kind:?} must serialize as stable kebab-case, got {serialized:?}"
        );
        assert!(
            !serialized.contains('_'),
            "{kind:?} must not use snake_case in robot contracts"
        );
        let policy = doctor_operation_outcome_policy(kind);
        assert!(
            !policy.reason.is_empty()
                && !policy.action_taken.is_empty()
                && !policy.action_not_taken.is_empty(),
            "{kind:?} must be self-documenting for humans and robots"
        );
    }
}

#[test]
fn doctor_operation_outcome_report_serializes_branchable_fields() {
    let report = doctor_operation_outcome_with_details(
        DoctorOperationOutcomeKind::RepairBlocked,
        "active doctor lock is still owned by pid 123".to_string(),
        "recorded lock contention".to_string(),
        "did not run repair while another owner is active".to_string(),
        DoctorDataLossRisk::Unknown,
        Some("cass doctor --json".to_string()),
        Some("receipt.artifact_manifest".to_string()),
    );
    let value = serde_json::to_value(&report).expect("serialize operation outcome");
    assert_eq!(value["kind"], "repair-blocked");
    assert_eq!(value["safe_to_retry"], false);
    assert_eq!(value["requires_override"], false);
    assert_eq!(value["data_loss_risk"], "unknown");
    assert_eq!(value["next_command"], "cass doctor --json");
    assert_eq!(value["artifact_manifest_path"], "receipt.artifact_manifest");
    assert_eq!(value["exit_code_kind"], "lock-busy");
    assert!(
        value["action_not_taken"]
            .as_str()
            .unwrap_or_default()
            .contains("did not run repair"),
        "operation outcomes must explain the skipped action"
    );
}

#[test]
fn doctor_top_level_operation_outcome_distinguishes_read_only_and_autorun() {
    let post_repair_probes = DoctorPostRepairProbeSuiteReport::default();
    let archive_risk = doctor_check_report(
        "database",
        "fail",
        "Cannot open database: simulated archive issue",
        false,
        false,
    );
    let read_only = doctor_top_level_operation_outcome(
        std::slice::from_ref(&archive_risk),
        false,
        1,
        0,
        false,
        &post_repair_probes,
        None,
    );
    assert_eq!(
        read_only.kind,
        DoctorOperationOutcomeKind::OkReadOnlyDiagnosed
    );
    assert_eq!(read_only.data_loss_risk, DoctorDataLossRisk::High);
    // 2l1b0.73: `--fix` runs derived cleanup only, so a failing archive
    // check must be routed to the repair planner, as the safe-auto-run
    // report already did (#374). This assertion used to pin `--fix`.
    assert_eq!(
        read_only.next_command.as_deref(),
        Some("cass doctor repair --dry-run --json")
    );
    let derived_only = doctor_check_report(
        "lock_file",
        "fail",
        "stale index-run lock left by a dead process",
        true,
        false,
    );
    let derived_read_only = doctor_top_level_operation_outcome(
        std::slice::from_ref(&derived_only),
        false,
        1,
        0,
        false,
        &post_repair_probes,
        None,
    );
    assert_eq!(
        derived_read_only.next_command.as_deref(),
        Some("cass doctor --fix --json"),
        "failures the safe auto-fix path handles still point at --fix"
    );
    let uninitialized = doctor_top_level_operation_outcome(
        std::slice::from_ref(&archive_risk),
        false,
        1,
        0,
        true,
        &post_repair_probes,
        None,
    );
    assert_eq!(
        uninitialized.next_command.as_deref(),
        Some("cass index --full")
    );

    let fully_fixed =
        doctor_top_level_operation_outcome(&[], true, 0, 1, false, &post_repair_probes, None);
    assert_eq!(fully_fixed.kind, DoctorOperationOutcomeKind::Fixed);
    assert_eq!(fully_fixed.exit_code_kind, DoctorExitCodeKind::Success);
    assert_eq!(fully_fixed.data_loss_risk, DoctorDataLossRisk::None);

    let partially_fixed = doctor_top_level_operation_outcome(
        std::slice::from_ref(&archive_risk),
        true,
        1,
        1,
        false,
        &post_repair_probes,
        None,
    );
    assert_eq!(
        partially_fixed.kind,
        DoctorOperationOutcomeKind::PartiallyFixed
    );
    assert_eq!(
        partially_fixed.exit_code_kind,
        DoctorExitCodeKind::RepairFailure
    );
    assert_eq!(partially_fixed.data_loss_risk, DoctorDataLossRisk::High);

    let auto_skipped = doctor_top_level_operation_outcome(
        std::slice::from_ref(&archive_risk),
        true,
        1,
        0,
        false,
        &post_repair_probes,
        None,
    );
    assert_eq!(
        auto_skipped.kind,
        DoctorOperationOutcomeKind::AutoRunSkipped
    );
    assert_eq!(
        auto_skipped.exit_code_kind,
        DoctorExitCodeKind::HealthFailure
    );

    let previous_failure = doctor_check_report(
        "rebuild",
        "fail",
        "Index rebuild failed: verification marker still present",
        true,
        false,
    );
    let verification_failed = doctor_top_level_operation_outcome(
        &[previous_failure],
        true,
        1,
        0,
        false,
        &post_repair_probes,
        None,
    );
    assert_eq!(
        verification_failed.kind,
        DoctorOperationOutcomeKind::VerificationFailed
    );
    assert_eq!(
        verification_failed.exit_code_kind,
        DoctorExitCodeKind::RepairFailure
    );
}

#[test]
fn doctor_archive_post_repair_probe_rolls_back_scratch_namespace() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let db_path = data_dir.join("agent_search.db");
    let storage = crate::storage::sqlite::FrankenStorage::open(&db_path).expect("create test db");
    storage.close().expect("close seeded db");

    let probe = run_doctor_archive_db_post_repair_probe(&data_dir, &db_path);

    assert_eq!(probe.status, "pass", "probe should pass: {probe:#?}");
    assert!(probe.rollback_confirmed);
    assert!(
        probe
            .steps
            .iter()
            .any(|step| step == "write_probe_sentinel")
    );
    let conn = open_franken_cli_read_db(
        db_path.clone(),
        "post-repair probe test",
        Duration::from_secs(5),
    )
    .expect("reopen db");
    let durable_probe_tables: i64 = conn
        .query_row_map(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name LIKE 'cass_doctor_probe_%'",
            crate::franken_sync::params![],
            |row: &crate::franken_sync::Row| row.get_typed(0),
        )
        .expect("count probe tables");
    let _ = close_franken_cli_read_db(conn, &db_path, "post-repair probe test");
    assert_eq!(
        durable_probe_tables, 0,
        "rollback-only probe must not leave scratch tables behind"
    );
}

#[test]
fn doctor_archive_post_repair_probe_fails_closed_on_unopenable_db_path() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let db_path = data_dir.join("agent_search.db");
    std::fs::create_dir_all(&db_path).expect("create unopenable db path directory");

    let probe = run_doctor_archive_db_post_repair_probe(&data_dir, &db_path);

    assert_eq!(probe.status, "fail");
    assert!(probe.blocks_promotion);
    assert!(probe.blocks_cleanup);
    assert!(
        probe
            .failure_reason
            .as_deref()
            .unwrap_or_default()
            .contains("production frankensqlite manager"),
        "failure should name production open path: {probe:#?}"
    );
}

#[test]
fn doctor_post_repair_probe_suite_writes_failure_context_and_blocks_success() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let db_path = data_dir.join("agent_search.db");
    let index_path = crate::search::tantivy::expected_index_dir(&data_dir);
    std::fs::create_dir_all(&db_path).expect("create unopenable db path directory");
    let readiness_snapshot = serde_json::json!({
        "semantic": {
            "can_search": false,
            "vector_index_path": serde_json::Value::Null,
            "hnsw_path": serde_json::Value::Null,
        }
    });

    let suite = collect_doctor_post_repair_probes(DoctorPostRepairProbeRunContext {
        data_dir: &data_dir,
        db_path: &db_path,
        index_path: &index_path,
        readiness_snapshot: &readiness_snapshot,
        fix_can_mutate: true,
        auto_fix_applied: true,
        fs_mutation_receipts: &[],
        cleanup_apply_result: None,
    });

    assert!(suite.requested);
    assert_eq!(suite.status, "fail");
    assert!(suite.blocks_success);
    assert_eq!(suite.failed_count, 1);
    assert!(suite.manifest_path.is_some());
    let failed_probe = suite
        .probes
        .iter()
        .find(|probe| probe.status == "fail")
        .expect("failed probe");
    let failure_context_path = failed_probe
        .failure_context_path
        .as_ref()
        .expect("failure context path");
    assert!(
        Path::new(failure_context_path).exists(),
        "failed probe should write artifact-backed context"
    );
    let failure_context: serde_json::Value =
        serde_json::from_slice(&std::fs::read(failure_context_path).expect("read failure context"))
            .expect("failure context json");
    assert_eq!(
        failure_context["context_kind"].as_str(),
        Some("cass_doctor_post_repair_probe_failure_context")
    );
    assert_eq!(
        failure_context["failed_phase"].as_str(),
        Some("post_repair_probe")
    );
    assert_eq!(
        failure_context["failed_check"].as_str(),
        Some("archive-db-rollback-write-read")
    );
    assert_eq!(
        failure_context["repro"]["safety"].as_str(),
        Some("read-only-redacted-template")
    );
    assert_eq!(
        failure_context["repro"]["mutates_live_archive"].as_bool(),
        Some(false)
    );
    assert!(
        failure_context["repro"]["command_json"]
            .as_array()
            .expect("repro argv")
            .iter()
            .any(|arg| arg.as_str() == Some("[cass-data]")),
        "failure context should target a redacted data-dir placeholder: {failure_context:#}"
    );
    assert!(
        !failure_context
            .to_string()
            .contains(temp.path().to_string_lossy().as_ref()),
        "shareable failure context must not leak temp paths: {failure_context:#}"
    );
    assert_eq!(
        failure_context["redaction_status"].as_str(),
        Some(
            "redacted_by_default; exact local paths are omitted from the shareable failure context"
        )
    );
    assert!(
        failed_probe
            .redacted_failure_context_path
            .as_deref()
            .unwrap_or_default()
            .contains("[cass-data]"),
        "robot metadata should include redacted failure context path"
    );
}

#[test]
fn doctor_post_repair_probe_failure_overrides_partial_success_outcome() {
    let target = DoctorPostRepairProbeTarget {
        probe_id: "archive-db-rollback-write-read",
        target_asset_class: DoctorAssetClass::CanonicalArchiveDb,
        target_path: Path::new("/tmp/cass-data/agent_search.db"),
        data_dir: Path::new("/tmp/cass-data"),
    };
    let post_repair_probes = DoctorPostRepairProbeSuiteReport {
        schema_version: 1,
        requested: true,
        status: "fail".to_string(),
        probe_count: 1,
        failed_count: 1,
        blocks_success: true,
        redacted_manifest_path: Some(
            "[cass-data]/doctor/post-repair-probes/probes.json".to_string(),
        ),
        probes: vec![doctor_post_repair_probe_report(
            target,
            doctor_post_repair_probe_outcome(
                "fail",
                1,
                Some("read-after-write mismatch".to_string()),
                true,
                true,
                false,
                vec!["read_probe_sentinel".to_string()],
            ),
        )],
        ..DoctorPostRepairProbeSuiteReport::default()
    };
    let probe_check = doctor_check_report(
        "post_repair_probes",
        "fail",
        "Post-repair verification failed",
        false,
        false,
    );

    let outcome = doctor_top_level_operation_outcome(
        &[probe_check],
        true,
        1,
        1,
        false,
        &post_repair_probes,
        None,
    );

    assert_eq!(outcome.kind, DoctorOperationOutcomeKind::VerificationFailed);
    assert_eq!(outcome.exit_code_kind, DoctorExitCodeKind::RepairFailure);
    assert_eq!(
        outcome.artifact_manifest_path.as_deref(),
        Some("[cass-data]/doctor/post-repair-probes/probes.json")
    );
}
