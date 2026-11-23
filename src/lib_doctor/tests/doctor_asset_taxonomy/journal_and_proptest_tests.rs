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
fn doctor_plan_receipt_schema_report_names_required_contract_fields() {
    let schema = doctor_plan_receipt_schema_report();
    assert!(
        schema
            .plan_fingerprint_includes
            .contains(&"artifact_manifest")
    );
    assert!(schema.receipt_required_fields.contains(&"plan_fingerprint"));
    assert!(schema.receipt_required_fields.contains(&"finished_at_ms"));
    assert!(
        schema
            .artifact_checksum_statuses
            .contains(&DoctorArtifactChecksumStatus::Mismatched)
    );
    assert!(
        schema
            .drift_detection_statuses
            .contains(&DoctorDriftDetectionStatus::MissingArtifact)
    );
}

#[test]
fn doctor_verification_contract_covers_required_scenarios_and_logs() {
    let contract = doctor_verification_contract_report();
    for required_field in [
        "scenario_id",
        "command_line",
        "file_inventory_before",
        "inventory_before",
        "inventory_after",
        "checksums",
        "stdout",
        "stderr",
        "parsed_json",
        "receipts",
        "coverage_deltas",
        "redacted_paths",
    ] {
        let found = contract
            .required_manifest_fields
            .iter()
            .chain(contract.required_step_log_fields.iter())
            .any(|field| field.contains(required_field) || *field == required_field);
        assert!(found, "verification contract missing {required_field}");
    }

    for scenario_id in [
        "no_delete_default_check",
        "upstream_pruned_archive_survives",
        "corrupt_db_repair_plan",
        "stale_lock_and_active_rebuild",
        "restore_rehearsal_then_apply",
        "derived_cleanup_fingerprint_apply",
        "semantic_fallback_no_archive_damage",
        "local_mirror_source_coverage",
    ] {
        let scenario = contract
            .matrix
            .iter()
            .find(|entry| entry.scenario_id == scenario_id)
            .unwrap_or_else(|| panic!("missing doctor verification scenario {scenario_id}"));
        assert!(
            scenario.proof_layers.len() >= 2,
            "{scenario_id} should not rely on a single proof layer"
        );
        assert!(
            scenario.required_artifacts.contains(&"manifest.json"),
            "{scenario_id} must emit a durable manifest"
        );
    }

    let no_delete = contract
        .matrix
        .iter()
        .find(|entry| entry.scenario_id == "no_delete_default_check")
        .expect("no-delete scenario");
    assert!(no_delete.mutation_audit_required);
    assert!(
        no_delete
            .archive_preservation_assertion
            .contains("no archive")
    );

    let semantic = contract
        .matrix
        .iter()
        .find(|entry| entry.scenario_id == "semantic_fallback_no_archive_damage")
        .expect("semantic fallback scenario");
    assert!(!semantic.mutation_audit_required);
    assert!(
        semantic
            .archive_preservation_assertion
            .contains("never imply archive corruption")
    );
}

// -----------------------------------------------------------------------
// Pass-13 structural refactor: journal_fs_mutation_receipt_to_actions_log
//
// These tests cover the new bridge between the FsMutationExecutor's
// `DoctorFsMutationReceipt` and the per-run `actions.jsonl` journal owned
// by `crate::doctor_runs`. The bridge is what makes `cass doctor --undo`,
// `cass doctor --diff`, and `cass doctor --ls` see real production
// cleanup-apply mutations (not just mutations from new fixers that go
// through `doctor_chokepoint::mutate()`).
// -----------------------------------------------------------------------

fn pass13_synthetic_receipt(
    kind: DoctorFsMutationKind,
    status: DoctorActionStatus,
    target_path: &str,
) -> DoctorFsMutationReceipt {
    DoctorFsMutationReceipt {
        schema_version: 1,
        operation_id: "op-pass13-test".to_string(),
        action_id: "act-pass13-test".to_string(),
        mutation_kind: kind,
        fallback_kind: None,
        mode: DoctorRepairMode::CleanupApply,
        asset_class: DoctorAssetClass::QuarantinedLexicalGeneration,
        source_path: None,
        redacted_source_path: None,
        target_path: target_path.to_string(),
        redacted_target_path: target_path.to_string(),
        staging_root: None,
        redacted_staging_root: None,
        expected_source_blake3: None,
        actual_source_blake3: Some("aa".repeat(32)),
        actual_target_blake3: Some("bb".repeat(32)),
        planned_bytes: 0,
        affected_bytes: 0,
        status,
        blocked_reasons: Vec::new(),
        precondition_checks: Vec::new(),
        forensic_bundle: doctor_forensic_bundle_uncaptured("test"),
    }
}

#[test]
fn pass13_journal_bridge_writes_actions_jsonl_entry_for_applied_receipt() {
    let temp = tempfile::tempdir().expect("tempdir");
    let run_dir = temp.path().join("run-dir");
    std::fs::create_dir_all(&run_dir).expect("create run dir");

    let receipt = pass13_synthetic_receipt(
        DoctorFsMutationKind::PruneCleanupTarget,
        DoctorActionStatus::Applied,
        "/data/doctor/x.idx",
    );
    journal_fs_mutation_receipt_to_actions_log(&run_dir, "run-id-1", &receipt, 100, 200)
        .expect("journal must succeed");

    let actions_path = run_dir.join("actions.jsonl");
    let body = std::fs::read_to_string(&actions_path).expect("read actions.jsonl");
    assert!(body.contains("\"kind\":\"mutation\""));
    assert!(body.contains("\"path\":\"/data/doctor/x.idx\""));
    assert!(body.contains("\"op\":\"prune-cleanup-target\""));
    assert!(body.contains("\"run_id\":\"run-id-1\""));
    assert!(
        body.ends_with('\n'),
        "must end with newline (atomic append)"
    );
}

#[test]
fn pass13_journal_bridge_silently_skips_non_applied_receipts() {
    let temp = tempfile::tempdir().expect("tempdir");
    let run_dir = temp.path().join("run-dir");
    std::fs::create_dir_all(&run_dir).expect("create run dir");

    for status in [
        DoctorActionStatus::Planned,
        DoctorActionStatus::Skipped,
        DoctorActionStatus::Blocked,
        DoctorActionStatus::Failed,
        DoctorActionStatus::Refused,
    ] {
        let receipt = pass13_synthetic_receipt(
            DoctorFsMutationKind::PruneCleanupTarget,
            status,
            "/data/doctor/y.idx",
        );
        journal_fs_mutation_receipt_to_actions_log(&run_dir, "run-id-2", &receipt, 100, 200)
            .expect("journal must succeed (silent skip)");
    }

    // No actions.jsonl entry should be written for any non-Applied status.
    let actions_path = run_dir.join("actions.jsonl");
    if actions_path.exists() {
        let body = std::fs::read_to_string(&actions_path).expect("read actions.jsonl");
        assert!(
            body.is_empty(),
            "non-Applied receipts must NOT emit journal entries; got: {body:?}"
        );
    }
}

#[test]
fn pass13_journal_bridge_emits_correct_op_labels_per_mutation_kind() {
    let cases = [
        (
            DoctorFsMutationKind::PruneCleanupTarget,
            "prune-cleanup-target",
        ),
        (
            DoctorFsMutationKind::RemoveStaleLegacyIndexLock,
            "remove-stale-legacy-index-lock",
        ),
        (
            DoctorFsMutationKind::CopyFileToStaging,
            "copy-file-to-staging",
        ),
        (
            DoctorFsMutationKind::WriteFileToStaging,
            "write-file-to-staging",
        ),
        (
            DoctorFsMutationKind::PromoteStagedFile,
            "promote-staged-file",
        ),
        (
            DoctorFsMutationKind::RestoreStagedFile,
            "restore-staged-file",
        ),
        (
            DoctorFsMutationKind::MoveFileToQuarantine,
            "move-file-to-quarantine",
        ),
    ];
    for (kind, expected_op_label) in cases {
        let temp = tempfile::tempdir().expect("tempdir");
        let run_dir = temp.path().join("run-dir");
        std::fs::create_dir_all(&run_dir).expect("create run dir");
        let receipt =
            pass13_synthetic_receipt(kind, DoctorActionStatus::Applied, "/data/doctor/probe.bin");
        journal_fs_mutation_receipt_to_actions_log(&run_dir, "run-id-op-label", &receipt, 1, 2)
            .expect("journal must succeed");
        let body =
            std::fs::read_to_string(run_dir.join("actions.jsonl")).expect("read actions.jsonl");
        assert!(
            body.contains(&format!("\"op\":\"{expected_op_label}\"")),
            "expected op label `{expected_op_label}` for {kind:?}; body: {body}"
        );
    }
}

#[test]
fn pass16_cleanup_apply_exit_code_kind_covers_all_four_branches() {
    // success: cleanup applied wholesale
    assert_eq!(
        cleanup_apply_exit_code_kind(true, 5, false),
        "success",
        "applied=true must always map to success regardless of other fields"
    );
    assert_eq!(
        cleanup_apply_exit_code_kind(true, 0, true),
        "success",
        "applied=true wins even if blocked_reasons are non-empty (defense-in-depth)"
    );

    // partial-success: some pruning happened but gate didn't flip applied
    assert_eq!(
        cleanup_apply_exit_code_kind(false, 1, false),
        "partial-success"
    );
    assert_eq!(
        cleanup_apply_exit_code_kind(false, 7, true),
        "partial-success",
        "partial-success wins over blocked when at least one asset was pruned"
    );

    // blocked: nothing pruned + at least one blocker reason
    assert_eq!(cleanup_apply_exit_code_kind(false, 0, true), "blocked");

    // no-op: nothing pruned + nothing blocked
    assert_eq!(cleanup_apply_exit_code_kind(false, 0, false), "no-op");
}

#[test]
fn pass13_journal_bridge_records_before_hash_for_destructive_kinds_only() {
    // Prune / RemoveStaleLock / MoveToQuarantine all act on existing files;
    // their actual_source_blake3 IS the file's pre-mutation hash.
    // Copy/Write/Promote/Restore write into previously-empty targets; their
    // destination has no pre-image, so before_blake3 must be `null`.
    let destructive = [
        DoctorFsMutationKind::PruneCleanupTarget,
        DoctorFsMutationKind::RemoveStaleLegacyIndexLock,
        DoctorFsMutationKind::MoveFileToQuarantine,
    ];
    for kind in destructive {
        let temp = tempfile::tempdir().expect("tempdir");
        let run_dir = temp.path().join("run-dir");
        std::fs::create_dir_all(&run_dir).expect("create run dir");
        let receipt = pass13_synthetic_receipt(kind, DoctorActionStatus::Applied, "/x");
        journal_fs_mutation_receipt_to_actions_log(&run_dir, "rid", &receipt, 1, 2)
            .expect("journal");
        let body = std::fs::read_to_string(run_dir.join("actions.jsonl")).expect("read");
        assert!(
            body.contains(&format!("\"before_blake3\":\"{}\"", "aa".repeat(32))),
            "destructive {kind:?} must record before_blake3 from actual_source_blake3"
        );
    }

    let non_destructive = [
        DoctorFsMutationKind::CopyFileToStaging,
        DoctorFsMutationKind::WriteFileToStaging,
        DoctorFsMutationKind::PromoteStagedFile,
        DoctorFsMutationKind::RestoreStagedFile,
    ];
    for kind in non_destructive {
        let temp = tempfile::tempdir().expect("tempdir");
        let run_dir = temp.path().join("run-dir");
        std::fs::create_dir_all(&run_dir).expect("create run dir");
        let receipt = pass13_synthetic_receipt(kind, DoctorActionStatus::Applied, "/x");
        journal_fs_mutation_receipt_to_actions_log(&run_dir, "rid", &receipt, 1, 2)
            .expect("journal");
        let body = std::fs::read_to_string(run_dir.join("actions.jsonl")).expect("read");
        assert!(
            body.contains("\"before_blake3\":null"),
            "non-destructive {kind:?} must record before_blake3=null; got: {body}"
        );
    }
}

// -----------------------------------------------------------------------
// Pass-17 property-based fuzz tests for the journal bridge.
//
// The pass-13 unit tests cover 4 hand-picked cases. proptest exercises the
// bridge across many randomly-generated receipts, pinning contract-level
// invariants the bridge MUST hold regardless of input:
//   (1) Applied receipts → exactly one well-formed JSON line, newline-
//       terminated, parseable as ActionRecord::Mutation.
//   (2) Non-Applied receipts → no on-disk side effects.
//   (3) op_label is bijective with mutation_kind (canonical mapping).
//   (4) before_blake3 follows the destructive-vs-non-destructive rule.
//   (5) after_blake3 always equals receipt.actual_target_blake3.
//   (6) N sequential Applied receipts → exactly N journal entries with
//       preserved ordering and per-line atomicity (no torn writes).
// -----------------------------------------------------------------------

fn arb_mutation_kind() -> proptest::strategy::BoxedStrategy<DoctorFsMutationKind> {
    use proptest::prelude::*;
    prop_oneof![
        Just(DoctorFsMutationKind::PruneCleanupTarget),
        Just(DoctorFsMutationKind::RemoveStaleLegacyIndexLock),
        Just(DoctorFsMutationKind::CopyFileToStaging),
        Just(DoctorFsMutationKind::WriteFileToStaging),
        Just(DoctorFsMutationKind::PromoteStagedFile),
        Just(DoctorFsMutationKind::RestoreStagedFile),
        Just(DoctorFsMutationKind::MoveFileToQuarantine),
    ]
    .boxed()
}

fn arb_non_applied_status() -> proptest::strategy::BoxedStrategy<DoctorActionStatus> {
    use proptest::prelude::*;
    prop_oneof![
        Just(DoctorActionStatus::Planned),
        Just(DoctorActionStatus::Skipped),
        Just(DoctorActionStatus::Blocked),
        Just(DoctorActionStatus::Failed),
        Just(DoctorActionStatus::Refused),
    ]
    .boxed()
}

fn arb_safe_path_segment() -> proptest::strategy::BoxedStrategy<String> {
    use proptest::prelude::*;
    // ASCII alphanumeric with hyphens / underscores / dots; non-empty;
    // bounded length. Avoids serde-JSON escaping surprises in the
    // canonical-mapping assertions while still ranging widely.
    proptest::collection::vec(
        prop_oneof![
            proptest::char::range('a', 'z'),
            proptest::char::range('A', 'Z'),
            proptest::char::range('0', '9'),
            Just('-'),
            Just('_'),
            Just('.'),
        ],
        1..32usize,
    )
    .prop_map(|chars| chars.into_iter().collect::<String>())
    .boxed()
}

fn arb_blake3_hex_or_none() -> proptest::strategy::BoxedStrategy<Option<String>> {
    use proptest::prelude::*;
    prop_oneof![
        Just::<Option<String>>(None),
        Just(Some("a".repeat(64))),
        Just(Some("b".repeat(64))),
        Just(Some("0".repeat(64))),
        Just(Some(format!("{}{}", "deadbeef".repeat(7), "deadbeef"))),
    ]
    .boxed()
}

fn pass17_arbitrary_receipt(
    kind: DoctorFsMutationKind,
    status: DoctorActionStatus,
    target_path: String,
    actual_source_blake3: Option<String>,
    actual_target_blake3: Option<String>,
) -> DoctorFsMutationReceipt {
    DoctorFsMutationReceipt {
        schema_version: 1,
        operation_id: "op-prop".to_string(),
        action_id: "act-prop".to_string(),
        mutation_kind: kind,
        fallback_kind: None,
        mode: DoctorRepairMode::CleanupApply,
        asset_class: DoctorAssetClass::QuarantinedLexicalGeneration,
        source_path: None,
        redacted_source_path: None,
        target_path,
        redacted_target_path: String::new(),
        staging_root: None,
        redacted_staging_root: None,
        expected_source_blake3: None,
        actual_source_blake3,
        actual_target_blake3,
        planned_bytes: 0,
        affected_bytes: 0,
        status,
        blocked_reasons: Vec::new(),
        precondition_checks: Vec::new(),
        forensic_bundle: doctor_forensic_bundle_uncaptured("prop"),
    }
}

fn is_destructive_kind(kind: DoctorFsMutationKind) -> bool {
    matches!(
        kind,
        DoctorFsMutationKind::PruneCleanupTarget
            | DoctorFsMutationKind::RemoveStaleLegacyIndexLock
            | DoctorFsMutationKind::MoveFileToQuarantine
    )
}

fn canonical_op_label(kind: DoctorFsMutationKind) -> &'static str {
    match kind {
        DoctorFsMutationKind::PruneCleanupTarget => "prune-cleanup-target",
        DoctorFsMutationKind::RemoveStaleLegacyIndexLock => "remove-stale-legacy-index-lock",
        DoctorFsMutationKind::CopyFileToStaging => "copy-file-to-staging",
        DoctorFsMutationKind::WriteFileToStaging => "write-file-to-staging",
        DoctorFsMutationKind::PromoteStagedFile => "promote-staged-file",
        DoctorFsMutationKind::RestoreStagedFile => "restore-staged-file",
        DoctorFsMutationKind::MoveFileToQuarantine => "move-file-to-quarantine",
    }
}

#[test]
fn pass18_journal_mutation_schema_fingerprint_is_stable() -> Result<(), Box<dyn std::error::Error>>
{
    // Pass-18 golden-style schema fingerprint: pin the exact byte form
    // of an ActionRecord::Mutation line so any silent field add / remove /
    // reorder fails visibly. The proptest in pass-17 checks parseability +
    // canonical op mapping; this test pins the literal wire format. If
    // this test fires, either (a) the doctor_runs::ActionRecord::Mutation
    // schema legitimately changed and downstream consumers (`--diff`,
    // `--ls`, `--undo`) must be updated, or (b) someone introduced an
    // accidental schema regression.
    let temp = tempfile::tempdir()?;
    let run_dir = temp.path().join("run-dir");
    std::fs::create_dir_all(&run_dir)?;

    let receipt = DoctorFsMutationReceipt {
        schema_version: 1,
        operation_id: "op-fingerprint".to_string(),
        action_id: "act-fingerprint".to_string(),
        mutation_kind: DoctorFsMutationKind::PruneCleanupTarget,
        fallback_kind: None,
        mode: DoctorRepairMode::CleanupApply,
        asset_class: DoctorAssetClass::QuarantinedLexicalGeneration,
        source_path: None,
        redacted_source_path: None,
        target_path: "/data/doctor/example.idx".to_string(),
        redacted_target_path: String::new(),
        staging_root: None,
        redacted_staging_root: None,
        expected_source_blake3: None,
        actual_source_blake3: Some(
            "1111111111111111111111111111111111111111111111111111111111111111".to_string(),
        ),
        actual_target_blake3: Some(
            "2222222222222222222222222222222222222222222222222222222222222222".to_string(),
        ),
        planned_bytes: 0,
        affected_bytes: 0,
        status: DoctorActionStatus::Applied,
        blocked_reasons: Vec::new(),
        precondition_checks: Vec::new(),
        forensic_bundle: doctor_forensic_bundle_uncaptured("fingerprint"),
    };
    journal_fs_mutation_receipt_to_actions_log(&run_dir, "rid-fp", &receipt, 100, 200)?;
    let body = std::fs::read_to_string(run_dir.join("actions.jsonl"))?;

    // Expected exact wire form. Field order is determined by serde's
    // serialization order of `ActionRecord::Mutation` fields, plus the
    // `kind` discriminant tag from `#[serde(tag = "kind")]`. Any change
    // here is a SCHEMA EVENT and must be coordinated with downstream
    // consumers (cass doctor --diff/--ls/--undo, undo_run, etc.).
    let expected = concat!(
        "{",
        "\"kind\":\"mutation\",",
        "\"run_id\":\"rid-fp\",",
        "\"fm_id\":\"op-fingerprint\",",
        "\"path\":\"/data/doctor/example.idx\",",
        "\"op\":\"prune-cleanup-target\",",
        "\"before_blake3\":\"1111111111111111111111111111111111111111111111111111111111111111\",",
        "\"after_blake3\":\"2222222222222222222222222222222222222222222222222222222222222222\",",
        "\"started_at_ms\":100,",
        "\"ended_at_ms\":200",
        "}\n",
    );
    if body.as_bytes().cmp(expected.as_bytes()).is_ne() {
        return Err(
            "Mutation record wire format drifted. If this is intentional, update the expected literal and bump RUN_ARTIFACT_SCHEMA_VERSION so consumers know to refresh their parsers."
                .into(),
        );
    }

    Ok(())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(128))]

    #[test]
    fn pass17_applied_receipts_always_produce_exactly_one_parseable_journal_line(
        kind in arb_mutation_kind(),
        target_path in arb_safe_path_segment(),
        actual_source_blake3 in arb_blake3_hex_or_none(),
        actual_target_blake3 in arb_blake3_hex_or_none(),
        started_at_ms in proptest::prelude::any::<i64>(),
        ended_at_ms in proptest::prelude::any::<i64>(),
    ) {
        let temp = tempfile::tempdir().expect("tempdir");
        let run_dir = temp.path().join("run-dir");
        std::fs::create_dir_all(&run_dir).expect("create run dir");
        let receipt = pass17_arbitrary_receipt(
            kind,
            DoctorActionStatus::Applied,
            target_path.clone(),
            actual_source_blake3.clone(),
            actual_target_blake3.clone(),
        );
        journal_fs_mutation_receipt_to_actions_log(
            &run_dir,
            "rid-prop",
            &receipt,
            started_at_ms,
            ended_at_ms,
        )
        .expect("journal must succeed for Applied receipts");
        let body = std::fs::read_to_string(run_dir.join("actions.jsonl"))
            .expect("read actions.jsonl");
        // (1) Exactly one JSON line, newline-terminated.
        proptest::prop_assert!(body.ends_with('\n'),
            "journal line must be newline-terminated: {body:?}");
        let lines: Vec<&str> = body.lines().collect();
        proptest::prop_assert_eq!(lines.len(), 1,
            "Applied receipt must produce exactly one journal line; got {}", lines.len());
        // Parseable as ActionRecord — schema invariant.
        let parsed: crate::doctor_runs::ActionRecord =
            serde_json::from_str(lines[0]).expect("journal line must parse as ActionRecord");
        // (3) op_label is the canonical mapping for the kind.
        // (4) before_blake3 follows the destructive rule.
        // (5) after_blake3 == receipt.actual_target_blake3.
        match parsed {
            crate::doctor_runs::ActionRecord::Mutation {
                op,
                path,
                before_blake3,
                after_blake3,
                ..
            } => {
                proptest::prop_assert_eq!(&op, canonical_op_label(kind),
                    "op label must be canonical mapping for {:?}", kind);
                proptest::prop_assert_eq!(&path, &target_path);
                if is_destructive_kind(kind) {
                    proptest::prop_assert_eq!(&before_blake3, &actual_source_blake3,
                        "destructive {:?} must record actual_source_blake3 as before_blake3", kind);
                } else {
                    proptest::prop_assert!(before_blake3.is_none(),
                        "non-destructive {:?} must record before_blake3 = None", kind);
                }
                proptest::prop_assert_eq!(&after_blake3, &actual_target_blake3,
                    "after_blake3 must always equal receipt.actual_target_blake3");
            }
            other => {
                return Err(proptest::test_runner::TestCaseError::fail(format!(
                    "expected Mutation record, got {other:?}"
                )));
            }
        }
    }

    #[test]
    fn pass17_non_applied_receipts_produce_no_journal_entries(
        kind in arb_mutation_kind(),
        status in arb_non_applied_status(),
        target_path in arb_safe_path_segment(),
        actual_source_blake3 in arb_blake3_hex_or_none(),
        actual_target_blake3 in arb_blake3_hex_or_none(),
    ) {
        let temp = tempfile::tempdir().expect("tempdir");
        let run_dir = temp.path().join("run-dir");
        std::fs::create_dir_all(&run_dir).expect("create run dir");
        let receipt = pass17_arbitrary_receipt(
            kind,
            status,
            target_path,
            actual_source_blake3,
            actual_target_blake3,
        );
        journal_fs_mutation_receipt_to_actions_log(&run_dir, "rid-skip", &receipt, 0, 0)
            .expect("journal must Ok(()) silently for non-Applied receipts");
        // (2) No on-disk side effects for non-Applied receipts.
        let actions_path = run_dir.join("actions.jsonl");
        if actions_path.exists() {
            let body = std::fs::read_to_string(&actions_path).expect("read");
            proptest::prop_assert!(body.is_empty(),
                "non-Applied receipt must not emit any journal content; got: {body:?}");
        }
    }

    #[test]
    fn pass17_sequential_applied_receipts_produce_exactly_n_journal_entries(
        n in 2usize..16usize,
        kind in arb_mutation_kind(),
    ) {
        let temp = tempfile::tempdir().expect("tempdir");
        let run_dir = temp.path().join("run-dir");
        std::fs::create_dir_all(&run_dir).expect("create run dir");
        // Vary target_path per iteration so each Mutation is distinguishable.
        for i in 0..n {
            let receipt = pass17_arbitrary_receipt(
                kind,
                DoctorActionStatus::Applied,
                format!("/seq/path-{i}.bin"),
                Some("c".repeat(64)),
                Some("d".repeat(64)),
            );
            journal_fs_mutation_receipt_to_actions_log(
                &run_dir,
                "rid-seq",
                &receipt,
                i as i64,
                (i as i64) + 1,
            )
            .expect("each journal append must succeed");
        }
        let body = std::fs::read_to_string(run_dir.join("actions.jsonl"))
            .expect("read actions.jsonl");
        let lines: Vec<&str> = body.lines().collect();
        proptest::prop_assert_eq!(lines.len(), n,
            "N sequential Applied receipts must yield exactly N journal lines; got {}", lines.len());
        // Each line is parseable + has its iteration's path in order.
        for (i, line) in lines.iter().enumerate() {
            let parsed: crate::doctor_runs::ActionRecord = serde_json::from_str(line)
                .expect("each line must parse as ActionRecord");
            if let crate::doctor_runs::ActionRecord::Mutation { path, .. } = parsed {
                proptest::prop_assert_eq!(path, format!("/seq/path-{i}.bin"),
                    "journal entries must preserve append order");
            } else {
                return Err(proptest::test_runner::TestCaseError::fail(
                    "expected Mutation variant".to_string(),
                ));
            }
        }
    }
}
