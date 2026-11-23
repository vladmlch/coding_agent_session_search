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
fn raw_mirror_candidate_fallback_key_groups_versions_and_provider_reclassification() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let report = test_doctor_raw_mirror_report_for_candidate(&data_dir);
    let mut first = report.manifests[0].clone();
    first.db_links.clear();
    let mut later = first.clone();
    later.provider = "omp".to_string();
    later.manifest_id = "later-manifest".to_string();
    later.blob_blake3 = "22".repeat(32);
    later.source_content_blake3 = "33".repeat(32);
    later.captured_at_ms += 1;

    assert_eq!(
        doctor_candidate_raw_mirror_key(&first),
        doctor_candidate_raw_mirror_key(&later),
        "content changes and provider reclassification must remain versions of one physical source"
    );
    later.original_path_blake3 = "different-path".to_string();
    assert_ne!(
        doctor_candidate_raw_mirror_key(&first),
        doctor_candidate_raw_mirror_key(&later),
        "distinct physical source paths must remain separate recovery candidates"
    );
}

#[test]
fn doctor_candidate_staging_refuses_roots_overlapping_live_archive_or_index() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    std::fs::create_dir_all(&data_dir).expect("create data dir");

    assert!(
        doctor_candidate_root_is_safe(
            &data_dir,
            &db_path,
            &index_path,
            &doctor_candidate_root(&data_dir),
        )
        .is_ok()
    );
    assert!(
        doctor_candidate_root_is_safe(&data_dir, &db_path, &index_path, &db_path).is_err(),
        "candidate staging must never target the live DB path"
    );
    assert!(
        doctor_candidate_root_is_safe(&data_dir, &db_path, &index_path, &index_path).is_err(),
        "candidate staging must never target the live index path"
    );
}

#[test]
fn doctor_candidate_build_creates_manifest_logs_checksums_and_preserves_live_inventory() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    seed_candidate_source_db(&db_path);
    let raw_mirror = test_doctor_raw_mirror_report_for_candidate(&data_dir);
    let source_inventory = collect_doctor_source_inventory(&data_dir, &db_path);
    let source_authority =
        build_doctor_source_authority_report(&db_path, &source_inventory, &raw_mirror);
    let coverage_summary = DoctorCoverageSummary {
        schema_version: 1,
        confidence_tier: "verified_raw_mirror_coverage".to_string(),
        archive_conversation_count: 1,
        archived_message_count: 1,
        raw_mirror_manifest_count: 2,
        raw_mirror_db_link_count: 1,
        recommended_action: "fixture".to_string(),
        ..DoctorCoverageSummary::default()
    };
    let live_before = doctor_candidate_live_inventory(&db_path, &index_path);

    let build = build_doctor_reconstruct_candidate(
        &data_dir,
        &db_path,
        &index_path,
        &raw_mirror,
        &source_authority,
        &coverage_summary,
    );

    assert_eq!(build.status, "completed", "{build:#?}");
    assert!(build.frankensqlite_open_ok);
    assert!(build.frankensqlite_write_ok);
    assert_eq!(build.candidate_conversation_count, Some(1));
    assert_eq!(build.candidate_message_count, Some(1));
    assert_eq!(build.skipped_record_count, 1);
    assert_eq!(build.parse_error_count, 1);
    assert!(build.manifest_blake3.is_some());
    assert!(build.live_inventory_unchanged);
    assert_eq!(build.live_inventory_before, live_before);
    assert_eq!(build.live_inventory_after, live_before);
    assert!(
        build
            .fs_mutation_receipts
            .iter()
            .all(|receipt| receipt.status == DoctorActionStatus::Applied),
        "all candidate writes/copies should be audited and applied: {build:#?}"
    );

    let candidate_dir = PathBuf::from(build.path.as_deref().expect("candidate path"));
    let manifest_path = PathBuf::from(build.manifest_path.as_deref().expect("manifest path"));
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&manifest_path).expect("read manifest"))
            .expect("parse manifest");
    assert_eq!(manifest["lifecycle_status"], "completed");
    assert!(
        manifest["checksum_set"]
            .as_object()
            .expect("checksum set")
            .contains_key("database/candidate.db")
    );
    assert!(
        candidate_dir.join("logs/skipped-records.jsonl").exists(),
        "skipped-record log should be durable"
    );
    assert!(
        candidate_dir.join("logs/parse-errors.jsonl").exists(),
        "parse-error log should be durable"
    );
    assert_eq!(
        std::fs::read_to_string(candidate_dir.join("logs/parse-errors.jsonl"))
            .expect("read parse errors")
            .lines()
            .count(),
        1
    );
    assert!(
        candidate_dir
            .join("index/lexical/candidate-generation.json")
            .exists(),
        "lexical candidate metadata should be staged away from the live index"
    );
    assert!(
        candidate_dir.join("index/semantic/metadata.json").exists(),
        "semantic candidate metadata should be staged away from live semantic assets"
    );
    assert!(db_path.exists(), "live DB must remain in place");
    assert!(
        !index_path.exists(),
        "candidate build must not create the live index path"
    );
}

#[test]
fn doctor_candidate_build_reconstructs_from_verified_raw_mirror_when_live_db_is_corrupt() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    let corrupt_bytes = b"not a sqlite database";
    std::fs::write(&db_path, corrupt_bytes).expect("write corrupt live db");

    let original_source = temp.path().join("pruned-session.jsonl");
    let raw_bytes =
        b"{\"role\":\"user\",\"content\":\"recover me\"}\nnot-json-but-still-archive-data\n";
    let manifest = raw_mirror_test_manifest(
        &data_dir,
        "codex",
        "local",
        &original_source,
        raw_bytes,
        vec![DoctorRawMirrorDbLink {
            conversation_id: Some(7),
            message_count: Some(2),
            source_path: Some(original_source.display().to_string()),
            started_at_ms: Some(1_700_000_000_000),
        }],
    );
    write_raw_mirror_test_manifest(&data_dir, &manifest, raw_bytes);
    let raw_mirror = collect_doctor_raw_mirror_report(&data_dir);
    assert_eq!(raw_mirror.status, "verified");
    let source_inventory =
        build_doctor_source_inventory_report(&data_dir, false, None, Vec::new(), Vec::new());
    let source_authority =
        build_doctor_source_authority_report(&db_path, &source_inventory, &raw_mirror);
    assert_eq!(
        source_authority.selected_authority,
        Some(DoctorSourceAuthorityKind::VerifiedRawMirror)
    );
    let coverage_summary = DoctorCoverageSummary {
        schema_version: 1,
        confidence_tier: "verified_raw_mirror_coverage".to_string(),
        raw_mirror_manifest_count: 1,
        raw_mirror_db_link_count: 1,
        recommended_action: "fixture".to_string(),
        ..DoctorCoverageSummary::default()
    };
    let live_before = doctor_candidate_live_inventory(&db_path, &index_path);

    let build = build_doctor_reconstruct_candidate(
        &data_dir,
        &db_path,
        &index_path,
        &raw_mirror,
        &source_authority,
        &coverage_summary,
    );

    assert_eq!(build.status, "completed", "{build:#?}");
    assert!(build.frankensqlite_open_ok);
    assert!(build.frankensqlite_write_ok);
    assert_eq!(build.confidence, "verified_raw_mirror_reconstruction");
    assert_eq!(build.candidate_conversation_count, Some(1));
    assert_eq!(build.candidate_message_count, Some(2));
    assert_eq!(build.parse_error_count, 1);
    assert!(
        build
            .evidence_sources
            .iter()
            .any(|source| source.starts_with("verified_raw_mirror:manifest_id=")),
        "verified raw mirror evidence should be explicit: {build:#?}"
    );
    assert_eq!(
        build.coverage_after.coverage_source,
        "verified_raw_mirror_candidate_archive"
    );
    assert!(build.live_inventory_unchanged);
    assert_eq!(build.live_inventory_before, live_before);
    assert_eq!(build.live_inventory_after, live_before);
    assert_eq!(
        std::fs::read(&db_path).expect("read live corrupt db"),
        corrupt_bytes,
        "candidate reconstruction must leave the corrupt live DB untouched"
    );

    let candidate_dir = PathBuf::from(build.path.as_deref().expect("candidate path"));
    let staged_blob_relative = doctor_raw_mirror_blob_relative_path(&manifest.blob_blake3)
        .expect("canonical staged blob path");
    assert!(
        candidate_dir
            .join("evidence/raw-mirror/blobs")
            .join(
                staged_blob_relative
                    .strip_prefix("blobs/")
                    .expect("blob path prefix"),
            )
            .exists(),
        "candidate should stage raw mirror blob evidence"
    );
    assert!(
        candidate_dir
            .join("evidence/raw-mirror/manifests")
            .join(format!("{}.json", manifest.manifest_id))
            .exists(),
        "candidate should stage raw mirror manifest evidence"
    );
    let parse_log = std::fs::read_to_string(candidate_dir.join("logs/parse-errors.jsonl"))
        .expect("read parse log");
    assert!(parse_log.contains("\"line_number\":2"));
    assert!(
        !parse_log.contains("not-json-but-still-archive-data"),
        "parse logs should preserve diagnostics without raw session content"
    );
    let manifest_path = PathBuf::from(build.manifest_path.as_deref().expect("manifest path"));
    let manifest_json: serde_json::Value =
        serde_json::from_slice(&std::fs::read(manifest_path).expect("read manifest"))
            .expect("parse candidate manifest");
    assert_eq!(
        manifest_json["coverage_after"]["coverage_source"].as_str(),
        Some("verified_raw_mirror_candidate_archive")
    );
    assert_eq!(
        manifest_json["confidence"].as_str(),
        Some("verified_raw_mirror_reconstruction")
    );
}

#[test]
fn doctor_candidate_raw_mirror_reconstruction_suppresses_duplicate_db_links() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    std::fs::write(&db_path, b"corrupt").expect("write corrupt db");

    let source_a = temp.path().join("session-a.jsonl");
    let source_b = temp.path().join("session-b.jsonl");
    let bytes_a = b"{\"role\":\"user\",\"content\":\"first\"}\n";
    let bytes_b = b"{\"role\":\"assistant\",\"content\":\"duplicate\"}\n";
    let duplicate_link = DoctorRawMirrorDbLink {
        conversation_id: Some(42),
        message_count: Some(1),
        source_path: Some(source_a.display().to_string()),
        started_at_ms: Some(1_700_000_000_000),
    };
    let mut manifest_a = raw_mirror_test_manifest(
        &data_dir,
        "codex",
        "local",
        &source_a,
        bytes_a,
        vec![duplicate_link.clone()],
    );
    let mut manifest_b = raw_mirror_test_manifest(
        &data_dir,
        "codex",
        "local",
        &source_b,
        bytes_b,
        vec![duplicate_link],
    );
    manifest_a.captured_at_ms = 1_733_000_000_100;
    manifest_a.source_mtime_ms = Some(1_733_000_000_100);
    manifest_a.manifest_blake3 = Some(doctor_raw_mirror_manifest_blake3(&manifest_a));
    manifest_b.captured_at_ms = 1_733_000_000_200;
    manifest_b.source_mtime_ms = Some(1_733_000_000_200);
    manifest_b.manifest_blake3 = Some(doctor_raw_mirror_manifest_blake3(&manifest_b));
    write_raw_mirror_test_manifest(&data_dir, &manifest_a, bytes_a);
    write_raw_mirror_test_manifest(&data_dir, &manifest_b, bytes_b);
    let raw_mirror = collect_doctor_raw_mirror_report(&data_dir);
    let source_inventory =
        build_doctor_source_inventory_report(&data_dir, false, None, Vec::new(), Vec::new());
    let source_authority =
        build_doctor_source_authority_report(&db_path, &source_inventory, &raw_mirror);
    let coverage_summary = DoctorCoverageSummary {
        schema_version: 1,
        confidence_tier: "verified_raw_mirror_coverage".to_string(),
        raw_mirror_manifest_count: 2,
        raw_mirror_db_link_count: 2,
        ..DoctorCoverageSummary::default()
    };

    let build = build_doctor_reconstruct_candidate(
        &data_dir,
        &db_path,
        &index_path,
        &raw_mirror,
        &source_authority,
        &coverage_summary,
    );

    assert_eq!(build.status, "completed", "{build:#?}");
    assert_eq!(build.candidate_conversation_count, Some(1));
    assert_eq!(build.candidate_message_count, Some(1));
    assert!(
        build
            .evidence_sources
            .iter()
            .any(|source| source.contains(&manifest_b.manifest_id)),
        "newest verified capture must win duplicate-conversation recovery: {build:#?}"
    );
    assert!(
        build
            .evidence_sources
            .iter()
            .all(|source| !source.contains(&manifest_a.manifest_id)),
        "older duplicate evidence must not become the reconstructed conversation: {build:#?}"
    );
    assert!(
        build.skipped_record_count >= 1,
        "duplicate manifest should be logged as skipped: {build:#?}"
    );
    let candidate_dir = PathBuf::from(build.path.as_deref().expect("candidate path"));
    let skipped_log = std::fs::read_to_string(candidate_dir.join("logs/skipped-records.jsonl"))
        .expect("read skipped log");
    assert!(
        skipped_log.contains("duplicate_raw_mirror_conversation_key"),
        "duplicate suppression reason should be durable: {skipped_log}"
    );
    assert!(
        skipped_log.contains(&manifest_a.manifest_id),
        "the skipped record must identify the older capture: {skipped_log}"
    );

    let newest_blob_path = doctor_raw_mirror_root(&data_dir).join(&manifest_b.blob_relative_path);
    std::fs::write(&newest_blob_path, vec![b'X'; bytes_b.len()])
        .expect("plant same-size post-verification corruption in newest blob");
    let fallback_build = build_doctor_reconstruct_candidate(
        &data_dir,
        &db_path,
        &index_path,
        &raw_mirror,
        &source_authority,
        &coverage_summary,
    );
    assert_eq!(fallback_build.status, "completed", "{fallback_build:#?}");
    assert_eq!(fallback_build.candidate_conversation_count, Some(1));
    assert!(
        fallback_build
            .evidence_sources
            .iter()
            .any(|source| source.contains(&manifest_a.manifest_id)),
        "an unreadable newest version must fall back to older verified evidence: {fallback_build:#?}"
    );
    assert!(
        fallback_build
            .evidence_sources
            .iter()
            .all(|source| !source.contains(&manifest_b.manifest_id)),
        "post-verification corruption must not become recovery authority: {fallback_build:#?}"
    );
    let fallback_candidate_dir = PathBuf::from(
        fallback_build
            .path
            .as_deref()
            .expect("fallback candidate path"),
    );
    let fallback_skipped_log =
        std::fs::read_to_string(fallback_candidate_dir.join("logs/skipped-records.jsonl"))
            .expect("read fallback skipped log");
    assert!(fallback_skipped_log.contains(&manifest_b.manifest_id));
    assert!(fallback_skipped_log.contains("source verification changed"));
}

#[test]
fn doctor_candidate_staging_reports_orphaned_and_interrupted_without_gc() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    let orphan = doctor_candidate_root(&data_dir).join("orphan-no-manifest");
    std::fs::create_dir_all(&orphan).expect("create orphan candidate");
    std::fs::write(orphan.join("partial.db"), b"partial bytes").expect("write partial");
    let in_progress = doctor_candidate_root(&data_dir).join("in-progress");
    std::fs::create_dir_all(&in_progress).expect("create in-progress candidate");
    let manifest = serde_json::json!({
        "schema_version": 1,
        "manifest_kind": DOCTOR_CANDIDATE_MANIFEST_KIND,
        "candidate_id": "in-progress",
        "lifecycle_status": "in_progress",
        "artifact_count": 0,
        "checksum_set": {},
        "selected_authority": "verified_raw_mirror",
        "created_at_ms": 1,
        "updated_at_ms": 2
    });
    std::fs::write(
        in_progress.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest).expect("manifest json"),
    )
    .expect("write in-progress manifest");

    let report = collect_doctor_candidate_staging_report(&data_dir, &db_path, &index_path);

    assert_eq!(report.status, "needs_inspection");
    assert_eq!(report.orphaned_candidate_count, 1);
    assert_eq!(report.interrupted_candidate_count, 2);
    assert!(
        report
            .candidates
            .iter()
            .all(|candidate| !candidate.safe_to_delete_automatically),
        "candidate evidence must never be marked as automatically deletable"
    );
    assert!(orphan.join("partial.db").exists());
    assert!(in_progress.join("manifest.json").exists());
}

#[test]
fn doctor_candidate_staging_target_guard_refuses_live_paths() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    let candidate_dir = doctor_candidate_root(&data_dir).join("candidate");
    std::fs::create_dir_all(&candidate_dir).expect("create candidate dir");
    std::fs::create_dir_all(candidate_dir.join("database")).expect("create candidate db dir");
    std::fs::create_dir_all(&index_path).expect("create index path");
    std::fs::write(&db_path, b"live db").expect("write db");

    assert!(!doctor_staging_target_path_is_safe(
        &db_path,
        &candidate_dir,
        &data_dir
    ));
    assert!(!doctor_staging_target_path_is_safe(
        &index_path.join("live-index-file"),
        &candidate_dir,
        &data_dir
    ));
    assert!(doctor_staging_target_path_is_safe(
        &candidate_dir.join("database/candidate.db"),
        &candidate_dir,
        &data_dir
    ));
}

#[test]
fn doctor_candidate_live_inventory_reports_wal_and_shm_sidecars() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    std::fs::write(&db_path, b"db").expect("write db");
    std::fs::write(data_dir.join("agent_search.db-wal"), b"wal").expect("write wal");
    std::fs::write(data_dir.join("agent_search.db-shm"), b"shm").expect("write shm");

    let inventory = doctor_candidate_live_inventory(&db_path, &index_path);

    assert!(inventory.db_exists);
    assert!(inventory.db_wal_exists);
    assert!(inventory.db_shm_exists);
    assert_eq!(inventory.db_wal_size_bytes, 3);
    assert_eq!(inventory.db_shm_size_bytes, 3);
    assert!(inventory.db_wal_blake3.is_some());
    assert!(inventory.db_shm_blake3.is_some());
}

#[test]
fn doctor_repair_plan_selects_single_completed_candidate_for_fingerprint_promotion() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    std::fs::write(&db_path, b"corrupt live archive").expect("write corrupt live db");
    let candidate_dir = doctor_candidate_root(&data_dir).join("candidate-plan-promote");
    let manifest_path = write_candidate_promotion_test_manifest(
        &db_path,
        &index_path,
        &candidate_dir,
        "candidate-plan-promote",
        b"candidate archive bytes",
        true,
    );
    let manifest_blake3 = file_blake3_hex(&manifest_path).expect("manifest hash");
    let candidate_staging =
        collect_doctor_candidate_staging_report(&data_dir, &db_path, &index_path);

    let plan = doctor_test_repair_plan_for_candidate_staging(
        &data_dir,
        &db_path,
        &index_path,
        &candidate_staging,
        false,
        true,
        true,
    );

    assert_eq!(plan.planned_action_count, 2, "{plan:#?}");
    assert!(doctor_repair_plan_has_action(
        &plan,
        DOCTOR_REPAIR_ACTION_PROMOTE_RECONSTRUCT_CANDIDATE
    ));
    assert!(doctor_repair_plan_has_action(
        &plan,
        DOCTOR_REPAIR_ACTION_REBUILD_FROM_PROMOTED_CANDIDATE
    ));
    assert!(
        !plan
            .branchable_blocker_codes
            .iter()
            .any(|code| code == "archive-risk"),
        "a verified candidate should replace the unsafe source-session rebuild blocker: {plan:#?}"
    );
    assert_eq!(
        plan.fingerprint_inputs
            .pointer("/candidate_staging/selected_completed_candidate/manifest_blake3")
            .and_then(serde_json::Value::as_str),
        Some(manifest_blake3.as_str())
    );
    assert!(
        plan.exact_apply_command.contains("--plan-fingerprint"),
        "plan must remain fingerprint-gated: {plan:#?}"
    );
}

#[test]
fn doctor_repair_plan_blocks_ambiguous_completed_candidate_promotion() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    std::fs::write(&db_path, b"corrupt live archive").expect("write corrupt live db");
    for candidate_id in ["candidate-a", "candidate-b"] {
        let candidate_dir = doctor_candidate_root(&data_dir).join(candidate_id);
        write_candidate_promotion_test_manifest(
            &db_path,
            &index_path,
            &candidate_dir,
            candidate_id,
            format!("{candidate_id} archive bytes").as_bytes(),
            true,
        );
    }
    let candidate_staging =
        collect_doctor_candidate_staging_report(&data_dir, &db_path, &index_path);

    let plan = doctor_test_repair_plan_for_candidate_staging(
        &data_dir,
        &db_path,
        &index_path,
        &candidate_staging,
        false,
        true,
        true,
    );

    assert!(
        !doctor_repair_plan_has_action(&plan, DOCTOR_REPAIR_ACTION_PROMOTE_RECONSTRUCT_CANDIDATE),
        "ambiguous candidates must not produce a promotion action: {plan:#?}"
    );
    assert!(
        plan.branchable_blocker_codes
            .iter()
            .any(|code| code == "candidate-selection-ambiguous"),
        "ambiguous candidate blocker should be branchable for robot callers: {plan:#?}"
    );
    assert_eq!(
        plan.fingerprint_inputs
            .pointer("/candidate_staging/completed_candidates")
            .and_then(serde_json::Value::as_array)
            .map(Vec::len),
        Some(2)
    );
}

#[test]
fn doctor_repair_plan_does_not_replace_readable_archive_with_candidate() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    std::fs::write(&db_path, b"readable archive placeholder").expect("write live db");
    let candidate_dir = doctor_candidate_root(&data_dir).join("candidate-readable-db");
    write_candidate_promotion_test_manifest(
        &db_path,
        &index_path,
        &candidate_dir,
        "candidate-readable-db",
        b"candidate archive bytes",
        true,
    );
    let candidate_staging =
        collect_doctor_candidate_staging_report(&data_dir, &db_path, &index_path);

    let plan = doctor_test_repair_plan_for_candidate_staging(
        &data_dir,
        &db_path,
        &index_path,
        &candidate_staging,
        true,
        false,
        true,
    );

    assert!(doctor_repair_plan_has_action(
        &plan,
        DOCTOR_REPAIR_ACTION_REBUILD_FROM_ARCHIVE_DB
    ));
    assert!(!doctor_repair_plan_has_action(
        &plan,
        DOCTOR_REPAIR_ACTION_PROMOTE_RECONSTRUCT_CANDIDATE
    ));
    assert!(
        plan.warnings
            .iter()
            .any(|warning| warning.contains("candidate-promotion-skipped")),
        "candidate skip warning should make the conservative choice explicit: {plan:#?}"
    );
}

#[test]
fn doctor_repair_plan_never_promotes_candidate_over_deferred_queryable_archive() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    std::fs::write(&db_path, b"bounded-queryable archive placeholder").expect("write live db");
    let candidate_dir = doctor_candidate_root(&data_dir).join("candidate-deferred-db");
    write_candidate_promotion_test_manifest(
        &db_path,
        &index_path,
        &candidate_dir,
        "candidate-deferred-db",
        b"candidate archive bytes",
        true,
    );
    let candidate_staging =
        collect_doctor_candidate_staging_report(&data_dir, &db_path, &index_path);

    let plan = doctor_test_repair_plan_for_candidate_staging_with_queryability(
        &data_dir,
        &db_path,
        &index_path,
        &candidate_staging,
        false,
        true,
        false,
        false,
        true,
    );

    assert!(doctor_repair_plan_has_action(
        &plan,
        DOCTOR_REPAIR_ACTION_REBUILD_FROM_ARCHIVE_DB
    ));
    assert!(!doctor_repair_plan_has_action(
        &plan,
        DOCTOR_REPAIR_ACTION_PROMOTE_RECONSTRUCT_CANDIDATE
    ));
    assert_eq!(
        plan.fingerprint_inputs
            .pointer("/canonical_archive_queryable_for_non_destructive_derived_rebuild")
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );
    assert!(
        plan.fingerprint_inputs
            .pointer("/candidate_staging/selected_completed_candidate")
            .is_some_and(serde_json::Value::is_null),
        "the staged candidate must not be selected when canonical bounded reads succeeded: {plan:#?}"
    );
    assert!(
        plan.warnings
            .iter()
            .any(|warning| warning.contains("candidate-promotion-skipped")),
        "the fail-closed selection must be explicit in the plan: {plan:#?}"
    );
}

#[test]
fn doctor_repair_plan_never_promotes_candidate_after_timeout_unknown_archive() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    std::fs::write(&db_path, b"health unknown after timeout").expect("write live db");
    let candidate_dir = doctor_candidate_root(&data_dir).join("candidate-timeout-db");
    write_candidate_promotion_test_manifest(
        &db_path,
        &index_path,
        &candidate_dir,
        "candidate-timeout-db",
        b"candidate archive bytes",
        true,
    );
    let candidate_staging =
        collect_doctor_candidate_staging_report(&data_dir, &db_path, &index_path);

    let plan = doctor_test_repair_plan_for_candidate_staging_with_queryability(
        &data_dir,
        &db_path,
        &index_path,
        &candidate_staging,
        false,
        false,
        false,
        false,
        true,
    );

    assert!(!doctor_repair_plan_has_action(
        &plan,
        DOCTOR_REPAIR_ACTION_PROMOTE_RECONSTRUCT_CANDIDATE
    ));
    assert!(
        plan.branchable_blocker_codes
            .iter()
            .any(|code| code == "canonical-archive-failure-unproven"),
        "timeout/unverified state must block replacement even with a completed candidate: {plan:#?}"
    );
    assert_eq!(
        plan.fingerprint_inputs
            .pointer("/canonical_candidate_replacement_authorized")
            .and_then(serde_json::Value::as_bool),
        Some(false)
    );
    assert!(
        plan.fingerprint_inputs
            .pointer("/candidate_staging/selected_completed_candidate")
            .is_some_and(serde_json::Value::is_null)
    );
}

#[test]
fn doctor_deferred_archive_collectors_are_explicitly_unknown_and_select_no_authority() {
    let reason = doctor_archive_wide_collectors_deferred_reason(
        Some("frankensqlite_full_page_integrity_probe_deferred: large archive"),
        false,
        false,
        false,
        false,
        false,
    )
    .expect("large archive must defer archive-wide collectors");
    let inventory = doctor_source_inventory_deferred(&reason);
    assert!(!inventory.db_available);
    assert!(
        inventory
            .db_query_error
            .as_deref()
            .is_some_and(|error| error.contains("archive_wide_collectors_deferred"))
    );
    let backfill =
        doctor_raw_mirror_backfill_deferred(false, &reason, "archive_wide_collectors_deferred");
    assert_eq!(backfill.status, "deferred");
    assert_eq!(backfill.total_candidate_count, 0);
    let coverage = doctor_coverage_summary_deferred(Some(17), Some(101), 3, &reason);
    assert_eq!(coverage.confidence_tier, "unchecked");
    assert!(coverage.coverage_reducing_live_source_rebuild_refused);
    let authority = doctor_refuse_source_authority_when_collectors_deferred(
        doctor_test_source_authority_report(),
        &reason,
        false,
    );
    assert_eq!(authority.decision, DoctorSourceAuthorityDecision::Refused);
    assert_eq!(authority.selected_authority, None);
    assert!(authority.selected_authorities.is_empty());
}

#[test]
fn doctor_affirmative_corruption_preserves_verified_mirror_candidate_only_authority()
-> Result<(), String> {
    let reason =
        doctor_archive_wide_collectors_deferred_reason(None, false, true, false, true, false)
            .ok_or_else(|| {
                "affirmative corrupt-open must defer archive-wide collectors".to_string()
            })?;
    let mut report = doctor_test_source_authority_report();
    report
        .selected_authorities
        .push(doctor_source_authority_candidate(
            DoctorSourceAuthorityKind::VerifiedRawMirror,
            DoctorSourceAuthorityDecision::CandidateOnly,
            "verified raw mirror fixture".to_string(),
            1,
            None,
            DoctorArtifactChecksumStatus::Matched,
            vec!["verified-blob-count=1".to_string()],
        ));

    let authority = doctor_refuse_source_authority_when_collectors_deferred(report, &reason, true);

    if !matches!(
        authority.decision,
        DoctorSourceAuthorityDecision::CandidateOnly
    ) {
        return Err(format!(
            "unexpected aggregate authority decision: {:?}",
            authority.decision
        ));
    }
    if !authority
        .selected_authority
        .is_some_and(|selected| matches!(selected, DoctorSourceAuthorityKind::VerifiedRawMirror))
    {
        return Err(format!(
            "unexpected selected authority: {:?}",
            authority.selected_authority
        ));
    }
    if !authority.selected_authorities.len().cmp(&1).is_eq() {
        return Err(format!(
            "expected one preserved authority, found {}",
            authority.selected_authorities.len()
        ));
    }
    let Some(selected) = authority.selected_authorities.first() else {
        return Err("preserved authority disappeared after length check".to_string());
    };
    if !matches!(
        selected.decision,
        DoctorSourceAuthorityDecision::CandidateOnly
    ) {
        return Err(format!(
            "unexpected preserved authority decision: {:?}",
            selected.decision
        ));
    }
    if !selected.evidence.iter().any(|evidence| {
        evidence
            .as_str()
            .eq("candidate-only-not-promotion-authority")
    }) {
        return Err(format!(
            "candidate-only boundary evidence is missing: {:?}",
            selected.evidence
        ));
    }
    Ok(())
}

#[test]
fn doctor_candidate_promotion_preserves_backup_and_replaces_single_file_archive() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    let live_bytes = b"prior live archive bytes";
    let candidate_bytes = b"verified promoted candidate bytes";
    std::fs::write(&db_path, live_bytes).expect("write live db");
    let candidate_dir = doctor_candidate_root(&data_dir).join("candidate-promote-ok");
    let manifest_path = write_candidate_promotion_test_manifest(
        &db_path,
        &index_path,
        &candidate_dir,
        "candidate-promote-ok",
        candidate_bytes,
        true,
    );

    let report = promote_doctor_reconstruct_candidate_bundle(
        &data_dir,
        &db_path,
        &index_path,
        &candidate_dir,
    );

    assert_eq!(report.status, "applied", "{report:#?}");
    assert_eq!(
        report.reader_consistency_guarantee,
        "single-file-atomic-replace-no-sqlite-sidecars"
    );
    assert_eq!(
        std::fs::read(&db_path).expect("read live db"),
        candidate_bytes
    );
    assert!(
        !candidate_dir.join("database/candidate.db").exists(),
        "successful promotion should consume the staged candidate path after backup preservation"
    );
    let backup_dir = PathBuf::from(&report.backup_dir);
    assert_eq!(
        std::fs::read(backup_dir.join("live/agent_search.db")).expect("read live backup"),
        live_bytes
    );
    assert_eq!(
        std::fs::read(backup_dir.join("candidate/candidate.db")).expect("read candidate backup"),
        candidate_bytes
    );
    assert!(Path::new(&report.receipt_path).exists());
    let promotion_marker_path = report
        .promotion_marker_path
        .as_deref()
        .map(Path::new)
        .expect("promotion marker path");
    assert!(promotion_marker_path.exists());
    assert_eq!(
        file_blake3_hex(promotion_marker_path).expect("hash promotion marker"),
        report
            .promotion_marker_blake3
            .clone()
            .expect("promotion marker blake3")
    );
    assert_eq!(report.event_log.event_count, 3);
    assert!(report.event_log.hash_chain_tip.is_some());
    assert_eq!(
        report.derived_assets_consistency_status,
        "no-live-derived-index-to-promote"
    );
    assert!(!report.derived_lexical_rebuild_required);
    assert!(!report.derived_semantic_rebuild_required);
    assert!(
        report.candidate_lexical_metadata_blake3.is_some(),
        "promotion should preserve the candidate lexical metadata checksum: {report:#?}"
    );
    assert!(
        report.candidate_semantic_metadata_blake3.is_some(),
        "promotion should preserve the candidate semantic metadata checksum: {report:#?}"
    );
    assert!(
        report.event_log.events.iter().any(|event| event
            .artifact_ids
            .iter()
            .any(|artifact_id| artifact_id.starts_with("candidate_archive_db_backup:"))),
        "event log should expose descriptive artifact ids, not bare checksums: {report:#?}"
    );
    let backup_manifest_path = report
        .backup_manifest_path
        .as_deref()
        .map(Path::new)
        .expect("backup manifest path");
    assert!(backup_manifest_path.exists());
    assert_eq!(
        file_blake3_hex(backup_manifest_path).expect("hash backup manifest"),
        report
            .backup_manifest_blake3
            .clone()
            .expect("backup manifest blake3")
    );
    let backup_manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(backup_manifest_path).expect("read backup manifest"))
            .expect("parse backup manifest");
    assert_eq!(
        backup_manifest
            .get("manifest_kind")
            .and_then(serde_json::Value::as_str),
        Some("cass_doctor_candidate_promotion_backup_manifest_v1")
    );
    assert_eq!(
        backup_manifest
            .get("derived_assets_consistency_status")
            .and_then(serde_json::Value::as_str),
        Some("no-live-derived-index-to-promote")
    );
    let promotion_marker: serde_json::Value = serde_json::from_slice(
        &std::fs::read(promotion_marker_path).expect("read promotion marker"),
    )
    .expect("parse promotion marker");
    assert_eq!(
        promotion_marker
            .get("derived_assets_consistency_status")
            .and_then(serde_json::Value::as_str),
        Some("no-live-derived-index-to-promote")
    );
    assert!(
        report.fs_mutation_receipts.iter().any(|receipt| {
            receipt.status == DoctorActionStatus::Applied
                && receipt.fallback_kind.as_deref() == Some("none")
                && receipt
                    .precondition_checks
                    .iter()
                    .any(|check| check == "prior_live_bundle_backup_preserved")
                && receipt
                    .precondition_checks
                    .iter()
                    .any(|check| check == "candidate_bundle_backup_preserved")
        }),
        "promotion receipt should prove both rollback inputs were preserved: {report:#?}"
    );
    assert!(
        manifest_path.exists(),
        "promotion must not delete the candidate manifest evidence"
    );
}

#[test]
fn doctor_candidate_promotion_refuses_stale_live_inventory_without_mutation() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    std::fs::write(&db_path, b"live bytes before manifest").expect("write live db");
    let candidate_dir = doctor_candidate_root(&data_dir).join("candidate-stale-live");
    write_candidate_promotion_test_manifest(
        &db_path,
        &index_path,
        &candidate_dir,
        "candidate-stale-live",
        b"candidate bytes",
        true,
    );
    std::fs::write(&db_path, b"live bytes after manifest").expect("mutate live db");

    let report = promote_doctor_reconstruct_candidate_bundle(
        &data_dir,
        &db_path,
        &index_path,
        &candidate_dir,
    );

    assert_eq!(report.status, "blocked");
    assert!(
        report
            .blocked_reasons
            .iter()
            .any(|reason| reason.contains("inventory changed")),
        "stale coverage/live inventory gate should be explicit: {report:#?}"
    );
    assert_eq!(
        std::fs::read(&db_path).expect("read live db"),
        b"live bytes after manifest"
    );
    assert!(
        Path::new(&report.receipt_path).exists(),
        "blocked stale promotion should still leave an inspectable receipt"
    );
    assert!(
        report.backup_manifest_path.is_none(),
        "blocked promotion must not advertise an unwritten backup manifest: {report:#?}"
    );
    assert!(
        candidate_dir.join("database/candidate.db").exists(),
        "blocked stale promotion must leave candidate evidence in place"
    );
}

#[test]
fn doctor_candidate_promotion_promotes_sqlite_sidecar_bundle_together() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    std::fs::write(&db_path, b"live db").expect("write live db");
    std::fs::write(data_dir.join("agent_search.db-wal"), b"live wal").expect("write live wal");
    std::fs::write(data_dir.join("agent_search.db-shm"), b"live shm").expect("write live shm");
    let candidate_dir = doctor_candidate_root(&data_dir).join("candidate-sidecar");
    write_candidate_promotion_test_manifest_with_sidecars(
        &db_path,
        &index_path,
        &candidate_dir,
        "candidate-sidecar",
        b"candidate db",
        &[("-wal", b"candidate wal"), ("-shm", b"candidate shm")],
        true,
    );

    let report = promote_doctor_reconstruct_candidate_bundle(
        &data_dir,
        &db_path,
        &index_path,
        &candidate_dir,
    );

    assert_eq!(report.status, "applied", "{report:#?}");
    assert!(report.sidecar_bundle_complete);
    assert_eq!(
        report.reader_consistency_guarantee,
        "sqlite-bundle-components-verified-sequentially-promoted-with-rollback-receipts"
    );
    assert_eq!(
        std::fs::read(&db_path).expect("read live db"),
        b"candidate db"
    );
    assert_eq!(
        std::fs::read(data_dir.join("agent_search.db-wal")).expect("read live wal"),
        b"candidate wal"
    );
    assert_eq!(
        std::fs::read(data_dir.join("agent_search.db-shm")).expect("read live shm"),
        b"candidate shm"
    );
    let backup_dir = PathBuf::from(&report.backup_dir);
    assert_eq!(
        std::fs::read(backup_dir.join("live/agent_search.db-wal")).expect("read live wal backup"),
        b"live wal"
    );
    assert_eq!(
        std::fs::read(backup_dir.join("live/agent_search.db-shm")).expect("read live shm backup"),
        b"live shm"
    );
    assert!(
        !candidate_dir.join("database/candidate.db-wal").exists(),
        "applied sidecar promotion should consume staged WAL after backup preservation"
    );
}

#[test]
fn doctor_candidate_promotion_reuses_live_derived_assets_when_archive_identity_is_unchanged() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    std::fs::create_dir_all(&index_path).expect("create live derived index");
    let archive_bytes = b"same archive identity";
    std::fs::write(&db_path, archive_bytes).expect("write live db");
    let candidate_dir = doctor_candidate_root(&data_dir).join("candidate-derived-reuse");
    write_candidate_promotion_test_manifest(
        &db_path,
        &index_path,
        &candidate_dir,
        "candidate-derived-reuse",
        archive_bytes,
        true,
    );

    let report = promote_doctor_reconstruct_candidate_bundle(
        &data_dir,
        &db_path,
        &index_path,
        &candidate_dir,
    );

    assert_eq!(report.status, "applied", "{report:#?}");
    assert_eq!(
        report.derived_assets_consistency_status,
        "live-derived-assets-reusable-same-archive-identity"
    );
    assert!(!report.derived_lexical_rebuild_required);
    assert!(!report.derived_semantic_rebuild_required);
    assert_eq!(
        std::fs::read(&db_path).expect("read promoted live db"),
        archive_bytes
    );
}

#[test]
fn doctor_candidate_promotion_marks_derived_assets_stale_when_archive_identity_changes() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    std::fs::create_dir_all(&index_path).expect("create live derived index");
    std::fs::write(&db_path, b"prior archive identity").expect("write live db");
    let candidate_dir = doctor_candidate_root(&data_dir).join("candidate-derived-stale");
    write_candidate_promotion_test_manifest(
        &db_path,
        &index_path,
        &candidate_dir,
        "candidate-derived-stale",
        b"replacement archive identity",
        true,
    );

    let report = promote_doctor_reconstruct_candidate_bundle(
        &data_dir,
        &db_path,
        &index_path,
        &candidate_dir,
    );

    assert_eq!(report.status, "applied", "{report:#?}");
    assert_eq!(
        report.derived_assets_consistency_status,
        "live-derived-assets-stale-after-archive-promotion-rebuild-required"
    );
    assert!(report.derived_lexical_rebuild_required);
    assert!(report.derived_semantic_rebuild_required);
    let marker_path = report
        .promotion_marker_path
        .as_deref()
        .map(Path::new)
        .expect("promotion marker path");
    let marker: serde_json::Value =
        serde_json::from_slice(&std::fs::read(marker_path).expect("read marker"))
            .expect("parse marker");
    assert_eq!(
        marker
            .get("derived_lexical_rebuild_required")
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );
}

#[test]
fn doctor_candidate_promotion_records_derived_followup_after_lexical_rebuild() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    std::fs::create_dir_all(&index_path).expect("create live derived index");
    std::fs::write(&db_path, b"prior archive identity").expect("write live db");
    let candidate_dir = doctor_candidate_root(&data_dir).join("candidate-derived-followup");
    write_candidate_promotion_test_manifest(
        &db_path,
        &index_path,
        &candidate_dir,
        "candidate-derived-followup",
        b"replacement archive identity",
        true,
    );

    let mut report = promote_doctor_reconstruct_candidate_bundle(
        &data_dir,
        &db_path,
        &index_path,
        &candidate_dir,
    );
    assert_eq!(report.status, "applied", "{report:#?}");
    assert!(report.derived_lexical_rebuild_required);
    assert!(report.derived_semantic_rebuild_required);

    doctor_candidate_promotion_record_derived_followup_artifact(
        &data_dir,
        &mut report,
        42,
        "lexical",
    );

    assert_eq!(
        report.derived_assets_consistency_status,
        "promoted-archive-derived-followup-completed"
    );
    assert!(!report.derived_lexical_rebuild_required);
    assert!(!report.derived_semantic_rebuild_required);
    assert_eq!(report.derived_lexical_followup_status, "rebuild-completed");
    assert_eq!(
        report.derived_semantic_followup_status,
        "skipped-lexical-fallback-active-no-auto-download"
    );
    assert_eq!(
        report.derived_vector_followup_status,
        "skipped-lexical-fallback-active-no-auto-download"
    );
    assert_eq!(
        report.derived_memo_followup_status,
        "not-mutated-rebuildable-cache-does-not-block-archive-recovery"
    );
    let followup_path = report
        .derived_followup_artifact_path
        .as_deref()
        .map(Path::new)
        .expect("derived follow-up artifact path");
    assert!(
        followup_path.exists(),
        "follow-up artifact should be durable"
    );
    let followup: serde_json::Value =
        serde_json::from_slice(&std::fs::read(followup_path).expect("read followup"))
            .expect("parse followup");
    assert_eq!(
        followup
            .get("manifest_kind")
            .and_then(serde_json::Value::as_str),
        Some("cass_doctor_candidate_promotion_derived_followup_v1")
    );
    assert_eq!(
        followup
            .get("lexical_rebuild_message_count")
            .and_then(serde_json::Value::as_u64),
        Some(42)
    );
    assert_eq!(
        followup
            .get("auto_download_attempted")
            .and_then(serde_json::Value::as_bool),
        Some(false)
    );
    assert_eq!(
        followup
            .pointer("/derived_asset_actions/semantic_vector_index/blocks_archive_recovery")
            .and_then(serde_json::Value::as_bool),
        Some(false)
    );
    assert_eq!(
        followup
            .pointer("/derived_asset_actions/memoization_cache/status")
            .and_then(serde_json::Value::as_str),
        Some("not-mutated-rebuildable-cache-does-not-block-archive-recovery")
    );
}

#[test]
fn doctor_candidate_promotion_refuses_incomplete_sqlite_sidecar_bundle_without_mutation() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    std::fs::write(&db_path, b"live db").expect("write live db");
    std::fs::write(data_dir.join("agent_search.db-wal"), b"live wal").expect("write live wal");
    let candidate_dir = doctor_candidate_root(&data_dir).join("candidate-incomplete-sidecar");
    write_candidate_promotion_test_manifest(
        &db_path,
        &index_path,
        &candidate_dir,
        "candidate-incomplete-sidecar",
        b"candidate db",
        true,
    );

    let report = promote_doctor_reconstruct_candidate_bundle(
        &data_dir,
        &db_path,
        &index_path,
        &candidate_dir,
    );

    assert_eq!(report.status, "blocked");
    assert!(!report.sidecar_bundle_complete);
    assert!(
        report
            .blocked_reasons
            .iter()
            .any(|reason| reason.contains("live sqlite bundle component")),
        "incomplete bundle should explain the missing sidecar replacement: {report:#?}"
    );
    assert_eq!(std::fs::read(&db_path).expect("read live db"), b"live db");
    assert_eq!(
        std::fs::read(data_dir.join("agent_search.db-wal")).expect("read live wal"),
        b"live wal"
    );
    assert!(candidate_dir.join("database/candidate.db").exists());
    assert!(Path::new(&report.receipt_path).exists());
}

#[test]
fn doctor_candidate_promotion_missing_manifest_writes_blocked_receipt_without_mutation() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    std::fs::write(&db_path, b"live db").expect("write live db");
    let candidate_dir = doctor_candidate_root(&data_dir).join("candidate-missing-manifest");
    std::fs::create_dir_all(candidate_dir.join("database")).expect("create candidate db dir");
    std::fs::write(candidate_dir.join("database/candidate.db"), b"candidate")
        .expect("write candidate db");

    let report = promote_doctor_reconstruct_candidate_bundle(
        &data_dir,
        &db_path,
        &index_path,
        &candidate_dir,
    );

    assert_eq!(report.status, "blocked");
    assert!(
        report
            .blocked_reasons
            .iter()
            .any(|reason| reason.contains("failed to read candidate manifest")),
        "missing manifest should be an explicit blocked reason: {report:#?}"
    );
    assert_eq!(std::fs::read(&db_path).expect("read live db"), b"live db");
    assert!(candidate_dir.join("database/candidate.db").exists());
    let receipt_path = Path::new(&report.receipt_path);
    assert!(receipt_path.exists());
    let receipt: serde_json::Value =
        serde_json::from_slice(&std::fs::read(receipt_path).expect("read receipt"))
            .expect("parse receipt");
    assert_eq!(
        receipt.get("status").and_then(serde_json::Value::as_str),
        Some("blocked")
    );
}

#[test]
fn doctor_candidate_promotion_rolls_back_prior_live_on_post_replace_sync_failure() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    let live_bytes = b"prior live archive bytes";
    let candidate_bytes = b"candidate bytes that fail sync";
    std::fs::write(&db_path, live_bytes).expect("write live db");
    let candidate_dir = doctor_candidate_root(&data_dir).join("candidate-rollback");
    write_candidate_promotion_test_manifest(
        &db_path,
        &index_path,
        &candidate_dir,
        "candidate-rollback",
        candidate_bytes,
        true,
    );

    doctor_test_inject_next_file_sync_failure();
    let report = promote_doctor_reconstruct_candidate_bundle(
        &data_dir,
        &db_path,
        &index_path,
        &candidate_dir,
    );

    assert_eq!(report.status, "rolled_back", "{report:#?}");
    assert!(report.rollback_applied);
    assert!(
        report.rollback_reference.is_some(),
        "rollback receipt must point to the prior-live backup"
    );
    assert_eq!(
        std::fs::read(&db_path).expect("read rolled back live db"),
        live_bytes
    );
    let backup_dir = PathBuf::from(&report.backup_dir);
    assert_eq!(
        std::fs::read(backup_dir.join("candidate/candidate.db"))
            .expect("read preserved candidate backup"),
        candidate_bytes
    );
    assert!(
        report.fs_mutation_receipts.iter().any(|receipt| {
            receipt.status == DoctorActionStatus::Failed
                && receipt
                    .precondition_checks
                    .iter()
                    .any(|check| check == "rollback_restored_prior_live_sqlite_bundle")
        }),
        "failed promotion receipt should prove rollback ran: {report:#?}"
    );
}

#[test]
fn doctor_candidate_promotion_uses_cross_device_replace_fallback_with_receipt() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    std::fs::write(&db_path, b"prior live").expect("write live db");
    let candidate_dir = doctor_candidate_root(&data_dir).join("candidate-cross-device");
    write_candidate_promotion_test_manifest(
        &db_path,
        &index_path,
        &candidate_dir,
        "candidate-cross-device",
        b"candidate across device",
        true,
    );

    doctor_test_inject_next_cross_device_rename_failure();
    let report = promote_doctor_reconstruct_candidate_bundle(
        &data_dir,
        &db_path,
        &index_path,
        &candidate_dir,
    );

    assert_eq!(report.status, "applied", "{report:#?}");
    assert_eq!(
        std::fs::read(&db_path).expect("read promoted live db"),
        b"candidate across device"
    );
    assert!(
        !candidate_dir.join("database/candidate.db").exists(),
        "cross-device fallback should consume the staged candidate after preserving backup"
    );
    assert!(
        report.fs_mutation_receipts.iter().any(|receipt| {
            receipt.status == DoctorActionStatus::Applied
                && receipt.fallback_kind.as_deref() == Some("cross_device_copy_replace")
                && receipt
                    .precondition_checks
                    .iter()
                    .any(|check| check == "filesystem_cross_device_copy_replace_completed:")
        }),
        "receipt should expose the cross-device replacement fallback: {report:#?}"
    );
}

#[test]
fn doctor_candidate_promotion_rolls_back_permission_denied_rename_without_consuming_candidate() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let db_path = data_dir.join("agent_search.db");
    let index_path = data_dir.join("index");
    std::fs::write(&db_path, b"prior live").expect("write live db");
    let candidate_dir = doctor_candidate_root(&data_dir).join("candidate-permission-denied");
    write_candidate_promotion_test_manifest(
        &db_path,
        &index_path,
        &candidate_dir,
        "candidate-permission-denied",
        b"candidate blocked by rename",
        true,
    );

    doctor_test_inject_next_permission_denied_rename_failure();
    let report = promote_doctor_reconstruct_candidate_bundle(
        &data_dir,
        &db_path,
        &index_path,
        &candidate_dir,
    );

    assert_eq!(report.status, "rolled_back", "{report:#?}");
    assert!(report.rollback_applied);
    assert_eq!(
        std::fs::read(&db_path).expect("read rolled back live db"),
        b"prior live"
    );
    assert!(
        candidate_dir.join("database/candidate.db").exists(),
        "failed non-cross-device rename should not consume the staged candidate"
    );
    assert!(
        report.fs_mutation_receipts.iter().any(|receipt| {
            receipt.status == DoctorActionStatus::Failed
                && receipt.blocked_reasons.iter().any(|reason| {
                    reason.contains("failed to atomically promote sqlite bundle component")
                        && reason.contains("permission denied")
                })
        }),
        "receipt should preserve the rename failure root cause: {report:#?}"
    );
}
