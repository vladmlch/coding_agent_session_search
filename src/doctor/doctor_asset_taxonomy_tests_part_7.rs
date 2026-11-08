use crate::doctor::archive::doctor_archive_coverage_snapshot::{
    apply_doctor_archive_normalize_plan, build_doctor_archive_normalize_plan,
    build_doctor_archive_scan_context,
};
use crate::doctor::doctor_asset_taxonomy_tests_part_6::{
    raw_mirror_test_manifest, write_raw_mirror_test_manifest,
};
use crate::doctor::forensic::doctor_forensic_bundle_uncaptured;
use crate::doctor::fs_safety::cleanup_apply::{
    DoctorFsMutationKind, DoctorFsMutationReceipt, cleanup_apply_exit_code_kind,
    journal_fs_mutation_receipt_to_actions_log,
};
use crate::doctor::policy::types::DoctorAssetClass;
use crate::doctor::policy::{
    DoctorActionStatus, DoctorArtifactChecksumStatus, DoctorDriftDetectionStatus, DoctorRepairMode,
};
use crate::doctor::raw_mirror::collect_doctor_raw_mirror_report_with_thresholds_and::collect_doctor_raw_mirror_report_with_thresholds_and_mode;
use crate::doctor::raw_mirror::doctorrawmirrorverificationmode::{
    DoctorRawMirrorVerificationMode, collect_doctor_raw_mirror_report,
    doctor_raw_mirror_manifest_blake3, doctor_raw_mirror_manifest_relative_path,
    doctor_raw_mirror_root,
};
use crate::doctor::repair_state::types::DOCTOR_RAW_MIRROR_MANIFEST_MAX_BYTES;
use crate::doctor::report::human::{
    doctor_plan_receipt_schema_report, doctor_verification_contract_report,
};
use anyhow::Result;

#[test]
pub(crate) fn gh430_bounded_doctor_caps_logical_verification_work_for_shared_chunks() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("growing-source.jsonl");
    let first_bytes = vec![b'a'; 64 * 1024];
    std::fs::write(&source_path, &first_bytes).expect("write first source version");
    crate::raw_mirror::capture_source_file_with_chunk_policy(
        crate::raw_mirror::RawMirrorCaptureInput {
            data_dir: &data_dir,
            provider: "codex",
            source_id: "local",
            origin_kind: "local",
            origin_host: None,
            source_path: &source_path,
            db_links: &[],
        },
        1,
        32 * 1024,
    )
    .expect("capture first chunked version");
    let mut second_bytes = first_bytes;
    second_bytes.push(b'\n');
    std::fs::write(&source_path, &second_bytes).expect("write appended source version");
    crate::raw_mirror::capture_source_file_with_chunk_policy(
        crate::raw_mirror::RawMirrorCaptureInput {
            data_dir: &data_dir,
            provider: "codex",
            source_id: "local",
            origin_kind: "local",
            origin_host: None,
            source_path: &source_path,
            db_links: &[],
        },
        1,
        32 * 1024,
    )
    .expect("capture second chunked version");

    let physical_storage_bytes = crate::raw_mirror::physical_storage_bytes(&data_dir);
    let report = collect_doctor_raw_mirror_report_with_thresholds_and_mode(
        &data_dir,
        u64::MAX,
        u64::MAX,
        u64::MAX,
        DoctorRawMirrorVerificationMode::Bounded {
            manifest_limit: 100,
            byte_limit: physical_storage_bytes,
        },
    );

    assert_eq!(report.status, "verification_deferred");
    assert_eq!(report.summary.manifest_count, 2);
    assert!(report.manifests.is_empty());
    assert!(report.warnings.iter().any(|warning| {
        warning.contains("estimated full verification work is")
            && warning.contains("estimated verification-work bytes")
            && !warning.contains("not computed")
    }));
}

#[test]
pub(crate) fn raw_mirror_full_report_rejects_oversized_manifest_before_parsing() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = data_dir.join("sessions/source.jsonl");
    let bytes = b"{\"type\":\"message\",\"text\":\"bounded\"}\n";
    let manifest =
        raw_mirror_test_manifest(&data_dir, "codex", "local", &source_path, bytes, Vec::new());
    let (_, manifest_path) = write_raw_mirror_test_manifest(&data_dir, &manifest, bytes);
    std::fs::OpenOptions::new()
        .write(true)
        .open(&manifest_path)
        .expect("open manifest fixture")
        .set_len(DOCTOR_RAW_MIRROR_MANIFEST_MAX_BYTES + 1)
        .expect("plant oversized sparse manifest");

    let report = collect_doctor_raw_mirror_report(&data_dir);
    assert_eq!(report.status, "warn");
    assert_eq!(report.summary.manifest_count, 1);
    assert_eq!(report.summary.invalid_manifest_count, 1);
    assert!(
        report.manifests[0]
            .invalid_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("16777216-byte validation limit"))
    );
}

#[cfg(unix)]
#[test]
pub(crate) fn raw_mirror_report_rejects_symlink_manifest_entries() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let root = doctor_raw_mirror_root(&data_dir);
    let manifest_dir = root.join("manifests");
    std::fs::create_dir_all(&manifest_dir).expect("create manifest dir");
    let outside_target = temp.path().join("outside-manifest.json");
    std::fs::write(&outside_target, "{}").expect("write external manifest");
    std::os::unix::fs::symlink(&outside_target, manifest_dir.join("linked.json"))
        .expect("create manifest symlink");

    let report = collect_doctor_raw_mirror_report(&data_dir);
    assert_eq!(report.status, "warn");
    assert_eq!(report.summary.manifest_count, 1);
    assert_eq!(report.summary.invalid_manifest_count, 1);
    assert_eq!(
        report.manifests[0].invalid_reason.as_deref(),
        Some("manifest path is a symlink")
    );
}

#[cfg(unix)]
#[test]
pub(crate) fn raw_mirror_report_rejects_blob_paths_with_symlinked_ancestors() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let root = doctor_raw_mirror_root(&data_dir);
    std::fs::create_dir_all(&root).expect("create raw mirror root");

    let source_path = data_dir.join("sessions/source.jsonl");
    let bytes = b"{\"type\":\"message\",\"text\":\"outside blob\"}\n";
    let manifest =
        raw_mirror_test_manifest(&data_dir, "codex", "local", &source_path, bytes, Vec::new());

    let outside_blob_root = temp.path().join("outside-blob-root");
    let outside_blob_path = outside_blob_root.join(
        manifest
            .blob_relative_path
            .strip_prefix("blobs/")
            .expect("blob relative path prefix"),
    );
    std::fs::create_dir_all(outside_blob_path.parent().expect("outside blob parent"))
        .expect("create outside blob parent");
    std::fs::write(&outside_blob_path, bytes).expect("write outside blob");
    std::os::unix::fs::symlink(&outside_blob_root, root.join("blobs"))
        .expect("create symlinked blob root");

    let manifest_path = root.join(doctor_raw_mirror_manifest_relative_path(
        &manifest.manifest_id,
    ));
    std::fs::create_dir_all(manifest_path.parent().expect("manifest parent"))
        .expect("create manifest parent");
    std::fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).expect("manifest json"),
    )
    .expect("write raw mirror manifest");

    let report = collect_doctor_raw_mirror_report(&data_dir);
    assert_eq!(report.status, "warn");
    assert_eq!(report.summary.manifest_count, 1);
    assert_eq!(report.summary.invalid_manifest_count, 1);
    assert_eq!(
        report.manifests[0].invalid_reason.as_deref(),
        Some("blob path contains a symlinked component"),
        "raw mirror verification must not follow symlinked blob ancestors"
    );
    assert_eq!(
        report.manifests[0].manifest_id, manifest.manifest_id,
        "symlink rejection should preserve manifest identity for diagnostics"
    );
    assert!(
        !report.manifests[0].blob_path.is_empty(),
        "symlink rejection should keep the rejected blob path visible in the report"
    );
    assert_eq!(
        report.summary.verified_blob_count, 0,
        "a blob reached through a symlinked parent must not count as verified evidence"
    );
}

#[test]
pub(crate) fn raw_mirror_report_counts_missing_blob_without_relabeling_manifest_invalid() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = data_dir.join("sessions/source.jsonl");
    let bytes = b"{\"type\":\"message\",\"text\":\"missing blob\"}\n";
    let manifest =
        raw_mirror_test_manifest(&data_dir, "codex", "local", &source_path, bytes, Vec::new());
    let root = doctor_raw_mirror_root(&data_dir);
    let manifest_path = root.join(doctor_raw_mirror_manifest_relative_path(
        &manifest.manifest_id,
    ));
    std::fs::create_dir_all(manifest_path.parent().expect("manifest parent"))
        .expect("create manifest parent");
    std::fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).expect("manifest json"),
    )
    .expect("write raw mirror manifest without blob");

    let report = collect_doctor_raw_mirror_report(&data_dir);
    assert_eq!(report.status, "warn");
    assert_eq!(report.summary.manifest_count, 1);
    assert_eq!(report.summary.missing_blob_count, 1);
    assert_eq!(
        report.summary.invalid_manifest_count, 0,
        "a correctly formed manifest with an absent blob should diagnose missing evidence, not manifest corruption"
    );
    assert_eq!(report.manifests[0].status, "missing_blob");
    assert_eq!(
        report.manifests[0].blob_checksum_status,
        DoctorArtifactChecksumStatus::Missing
    );
}

#[test]
pub(crate) fn archive_scan_classifies_normalizable_metadata_and_critical_archive_risks() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let bytes = b"{\"type\":\"message\",\"text\":\"dedup\"}\n";

    let first_path = data_dir.join("sessions/first.jsonl");
    let first_manifest =
        raw_mirror_test_manifest(&data_dir, "codex", "local", &first_path, bytes, Vec::new());
    write_raw_mirror_test_manifest(&data_dir, &first_manifest, bytes);

    let second_path = data_dir.join("sessions/second.jsonl");
    let second_manifest =
        raw_mirror_test_manifest(&data_dir, "codex", "local", &second_path, bytes, Vec::new());
    write_raw_mirror_test_manifest(&data_dir, &second_manifest, bytes);

    let legacy_path = data_dir.join("sessions/legacy.jsonl");
    let legacy_bytes = b"{\"type\":\"message\",\"text\":\"legacy\"}\n";
    let mut legacy_manifest = raw_mirror_test_manifest(
        &data_dir,
        "codex",
        "local",
        &legacy_path,
        legacy_bytes,
        Vec::new(),
    );
    legacy_manifest.manifest_blake3 = None;
    write_raw_mirror_test_manifest(&data_dir, &legacy_manifest, legacy_bytes);

    let optional_path = data_dir.join("sessions/optional.jsonl");
    let optional_bytes = b"{\"type\":\"message\",\"text\":\"optional\"}\n";
    let mut optional_manifest = raw_mirror_test_manifest(
        &data_dir,
        "codex",
        "local",
        &optional_path,
        optional_bytes,
        Vec::new(),
    );
    optional_manifest.compression.state = "legacy-zstd".to_string();
    optional_manifest.manifest_blake3 = Some(doctor_raw_mirror_manifest_blake3(&optional_manifest));
    write_raw_mirror_test_manifest(&data_dir, &optional_manifest, optional_bytes);

    let missing_path = data_dir.join("sessions/missing.jsonl");
    let missing_bytes = b"{\"type\":\"message\",\"text\":\"missing\"}\n";
    let missing_manifest = raw_mirror_test_manifest(
        &data_dir,
        "codex",
        "local",
        &missing_path,
        missing_bytes,
        Vec::new(),
    );
    let root = doctor_raw_mirror_root(&data_dir);
    let missing_manifest_path = root.join(doctor_raw_mirror_manifest_relative_path(
        &missing_manifest.manifest_id,
    ));
    std::fs::create_dir_all(missing_manifest_path.parent().expect("manifest parent"))
        .expect("create missing manifest parent");
    std::fs::write(
        &missing_manifest_path,
        serde_json::to_vec_pretty(&missing_manifest).expect("manifest json"),
    )
    .expect("write missing-blob manifest without blob bytes");

    let context = build_doctor_archive_scan_context(&data_dir, &data_dir.join("agent_search.db"));
    let finding_kinds: Vec<_> = context
        .scan
        .findings
        .iter()
        .map(|finding| finding.finding_kind.as_str())
        .collect();

    assert!(finding_kinds.contains(&"duplicate_metadata"));
    assert!(finding_kinds.contains(&"legacy_schema_annotation"));
    assert!(finding_kinds.contains(&"malformed_optional_fields"));
    assert!(finding_kinds.contains(&"missing_raw_mirror_blob"));
    assert!(!context.scan.mutation_performed);
    assert_eq!(context.scan.issues_fixed, 0);
    assert!(
        context
            .scan
            .findings
            .iter()
            .filter(|finding| finding.safe_to_normalize)
            .all(|finding| finding.recommendation.contains("archive-normalize")),
        "safe findings should route to archive-normalize: {:#?}",
        context.scan.findings
    );
    let missing = context
        .scan
        .findings
        .iter()
        .find(|finding| finding.finding_kind == "missing_raw_mirror_blob")
        .expect("missing blob finding");
    assert_eq!(missing.severity, "critical");
    assert!(!missing.safe_to_normalize);
    assert!(
        missing.recommendation.contains("restore")
            || missing.recommendation.contains("reconstruct")
    );

    let plan = build_doctor_archive_normalize_plan(
        &data_dir,
        &data_dir.join("agent_search.db"),
        &context.scan,
        false,
        None,
    );
    assert_eq!(
        plan.action_count, context.scan.safe_normalization_candidate_count,
        "normalize should plan only safe additive metadata annotations"
    );
    assert!(
        plan.skipped_actions
            .iter()
            .any(|action| action.finding_kind == "missing_raw_mirror_blob"),
        "critical findings must be skipped rather than normalized away: {plan:#?}"
    );
    assert!(!plan.coverage_reduced);
}

#[test]
pub(crate) fn archive_normalize_apply_writes_only_additive_idempotent_annotations() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = data_dir.join("sessions/source.jsonl");
    let bytes = b"{\"type\":\"message\",\"text\":\"legacy annotation\"}\n";
    let mut manifest =
        raw_mirror_test_manifest(&data_dir, "codex", "local", &source_path, bytes, Vec::new());
    manifest.manifest_blake3 = None;
    write_raw_mirror_test_manifest(&data_dir, &manifest, bytes);

    let before_raw_mirror = collect_doctor_raw_mirror_report(&data_dir);
    let context = build_doctor_archive_scan_context(&data_dir, &data_dir.join("agent_search.db"));
    let mut plan = build_doctor_archive_normalize_plan(
        &data_dir,
        &data_dir.join("agent_search.db"),
        &context.scan,
        true,
        None,
    );
    plan.apply_authorized = true;
    plan.approval_status = "matched".to_string();
    assert_eq!(plan.action_count, 1);

    let first = apply_doctor_archive_normalize_plan(&data_dir, &plan);
    assert_eq!(first.status, "applied");
    assert_eq!(first.applied_action_count, 1);
    assert_eq!(first.refused_action_count, 0);
    assert!(first.mutation_performed);
    assert!(!first.coverage_reduced);
    let action_path = data_dir.join(&plan.actions[0].target_relative_path);
    assert!(
        action_path.exists(),
        "annotation should be written additively"
    );

    let after_raw_mirror = collect_doctor_raw_mirror_report(&data_dir);
    assert_eq!(
        before_raw_mirror.summary.manifest_count,
        after_raw_mirror.summary.manifest_count
    );
    assert_eq!(
        before_raw_mirror.summary.total_blob_bytes,
        after_raw_mirror.summary.total_blob_bytes
    );
    assert_eq!(
        before_raw_mirror.manifests[0].manifest_id,
        after_raw_mirror.manifests[0].manifest_id
    );

    let second = apply_doctor_archive_normalize_plan(&data_dir, &plan);
    assert_eq!(second.status, "idempotent");
    assert_eq!(second.applied_action_count, 0);
    assert_eq!(second.already_present_count, 1);
    assert_eq!(second.refused_action_count, 0);
    assert!(!second.mutation_performed);
}

#[test]
pub(crate) fn archive_normalize_refuses_hostile_annotation_paths() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = data_dir.join("sessions/source.jsonl");
    let bytes = b"{\"type\":\"message\",\"text\":\"legacy annotation\"}\n";
    let mut manifest =
        raw_mirror_test_manifest(&data_dir, "codex", "local", &source_path, bytes, Vec::new());
    manifest.manifest_blake3 = None;
    write_raw_mirror_test_manifest(&data_dir, &manifest, bytes);

    let context = build_doctor_archive_scan_context(&data_dir, &data_dir.join("agent_search.db"));
    let mut plan = build_doctor_archive_normalize_plan(
        &data_dir,
        &data_dir.join("agent_search.db"),
        &context.scan,
        true,
        None,
    );
    plan.apply_authorized = true;
    plan.approval_status = "matched".to_string();
    plan.actions[0].target_relative_path = "../escape.json".to_string();

    let receipt = apply_doctor_archive_normalize_plan(&data_dir, &plan);
    assert_eq!(receipt.status, "partial");
    assert_eq!(receipt.applied_action_count, 0);
    assert_eq!(receipt.refused_action_count, 1);
    assert!(!receipt.mutation_performed);
    assert!(
        !temp.path().join("escape.json").exists(),
        "hostile annotation path must not be created outside data_dir"
    );
}

#[cfg(unix)]
#[test]
pub(crate) fn archive_normalize_refuses_symlinked_annotation_targets_and_data_dirs() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let real_data_dir = temp.path().join("cass-data-real");
    let source_path = real_data_dir.join("sessions/source.jsonl");
    let bytes = b"{\"type\":\"message\",\"text\":\"legacy annotation\"}\n";
    let mut manifest = raw_mirror_test_manifest(
        &real_data_dir,
        "codex",
        "local",
        &source_path,
        bytes,
        Vec::new(),
    );
    manifest.manifest_blake3 = None;
    write_raw_mirror_test_manifest(&real_data_dir, &manifest, bytes);

    let context =
        build_doctor_archive_scan_context(&real_data_dir, &real_data_dir.join("agent_search.db"));
    let mut plan = build_doctor_archive_normalize_plan(
        &real_data_dir,
        &real_data_dir.join("agent_search.db"),
        &context.scan,
        true,
        None,
    );
    plan.apply_authorized = true;
    plan.approval_status = "matched".to_string();
    let target_path = real_data_dir.join(&plan.actions[0].target_relative_path);
    std::fs::create_dir_all(target_path.parent().expect("annotation parent"))
        .expect("create annotation parent");
    let outside = temp.path().join("outside-annotation.json");
    std::fs::write(&outside, b"{\"outside\":true}\n").expect("write outside target");
    std::os::unix::fs::symlink(&outside, &target_path).expect("create annotation target symlink");

    let receipt = apply_doctor_archive_normalize_plan(&real_data_dir, &plan);
    assert_eq!(receipt.status, "partial");
    assert_eq!(receipt.applied_action_count, 0);
    assert_eq!(receipt.refused_action_count, 1);
    assert!(
        receipt
            .event_log
            .iter()
            .any(|event| event.message.contains("symlink")),
        "symlink target should be refused explicitly: {receipt:#?}"
    );

    let symlinked_data_dir = temp.path().join("cass-data-link");
    std::os::unix::fs::symlink(&real_data_dir, &symlinked_data_dir)
        .expect("create data dir symlink");
    let context = build_doctor_archive_scan_context(
        &symlinked_data_dir,
        &symlinked_data_dir.join("agent_search.db"),
    );
    let plan = build_doctor_archive_normalize_plan(
        &symlinked_data_dir,
        &symlinked_data_dir.join("agent_search.db"),
        &context.scan,
        true,
        None,
    );
    assert_eq!(plan.status, "blocked");
    assert!(
        plan.safety_gates
            .iter()
            .any(|gate| gate.gate_id == "data-dir-root-safe" && gate.status == "fail"),
        "symlinked data dir root should fail the root safety gate: {plan:#?}"
    );
    let receipt = apply_doctor_archive_normalize_plan(&symlinked_data_dir, &plan);
    assert_eq!(receipt.status, "blocked");
    assert!(!receipt.mutation_performed);
}

#[test]
pub(crate) fn doctor_plan_receipt_schema_report_names_required_contract_fields() {
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
pub(crate) fn doctor_verification_contract_covers_required_scenarios_and_logs() {
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

pub(crate) fn pass13_synthetic_receipt(
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
pub(crate) fn pass13_journal_bridge_writes_actions_jsonl_entry_for_applied_receipt() {
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
pub(crate) fn pass13_journal_bridge_silently_skips_non_applied_receipts() {
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
pub(crate) fn pass13_journal_bridge_emits_correct_op_labels_per_mutation_kind() {
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
pub(crate) fn pass16_cleanup_apply_exit_code_kind_covers_all_four_branches() {
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
pub(crate) fn pass13_journal_bridge_records_before_hash_for_destructive_kinds_only() {
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

pub(crate) fn arb_mutation_kind() -> proptest::strategy::BoxedStrategy<DoctorFsMutationKind> {
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

pub(crate) fn arb_non_applied_status() -> proptest::strategy::BoxedStrategy<DoctorActionStatus> {
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

pub(crate) fn arb_safe_path_segment() -> proptest::strategy::BoxedStrategy<String> {
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

pub(crate) fn arb_blake3_hex_or_none() -> proptest::strategy::BoxedStrategy<Option<String>> {
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

pub(crate) fn pass17_arbitrary_receipt(
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

pub(crate) fn is_destructive_kind(kind: DoctorFsMutationKind) -> bool {
    matches!(
        kind,
        DoctorFsMutationKind::PruneCleanupTarget
            | DoctorFsMutationKind::RemoveStaleLegacyIndexLock
            | DoctorFsMutationKind::MoveFileToQuarantine
    )
}

pub(crate) fn canonical_op_label(kind: DoctorFsMutationKind) -> &'static str {
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
pub(crate) fn pass18_journal_mutation_schema_fingerprint_is_stable()
-> Result<(), Box<dyn std::error::Error>> {
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
