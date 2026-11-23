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
fn raw_mirror_layout_rejects_hostile_paths_and_documents_permission_intent() {
    assert!(doctor_raw_mirror_validate_relative_path("../escape").is_err());
    assert!(doctor_raw_mirror_validate_relative_path("/tmp/escape").is_err());
    assert!(doctor_raw_mirror_validate_relative_path("blobs/blake3/ab/hash.raw").is_ok());

    let policy = doctor_raw_mirror_policy_report();
    assert!(policy.append_only);
    assert!(policy.global_dedup_by_content_hash);
    assert!(policy.never_overwrite_different_bytes);
    assert_eq!(policy.directory_mode_octal, "0700");
    assert_eq!(policy.file_mode_octal, "0600");
    assert!(policy.enforce_private_files);
    assert!(!policy.fsync_required);
    assert!(
        policy.path_traversal_defense.contains("dot-dot")
            && policy.symlink_defense.contains("refuses symlinked")
    );
    assert!(policy.local_only_by_default);
    assert!(
        policy
            .storage_scope
            .contains("local cass data directory only")
    );
    assert!(!policy.sensitive_output_policy.default_includes_exact_paths);
    assert!(!policy.sensitive_output_policy.default_includes_raw_content);
    assert!(
        policy
            .sensitive_output_policy
            .redacted_by_default_fields
            .contains(&"original_path")
    );
    assert!(policy.support_bundle_policy.include_manifest_metadata);
    assert!(!policy.support_bundle_policy.include_blob_bytes);
    assert!(!policy.support_bundle_policy.include_exact_paths);
    assert!(!policy.public_export_policy.pages_exports_include_raw_mirror);
    assert!(!policy.public_export_policy.html_exports_include_raw_mirror);
    assert_eq!(policy.compression_policy.default_state, "none");
    assert!(policy.compression_policy.optional);
    assert_eq!(policy.encryption_policy.default_state, "none");
    assert!(policy.encryption_policy.optional);

    let layout = doctor_raw_mirror_layout_report();
    assert_eq!(layout.root_relative_path, "raw-mirror/v1");
    assert!(
        layout
            .case_insensitive_collision_behavior
            .contains("path identity is hashed")
    );
}

#[test]
fn raw_mirror_interrupted_capture_count_ignores_empty_temp_dirs() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let tmp_root = doctor_raw_mirror_root(&data_dir).join("tmp");
    std::fs::create_dir_all(tmp_root.join("capture.empty")).expect("create empty temp dir");

    assert_eq!(
        doctor_raw_mirror_count_interrupted_captures(&doctor_raw_mirror_root(&data_dir)),
        0
    );

    std::fs::write(tmp_root.join("capture.failed.tmp"), b"partial")
        .expect("write interrupted temp file");
    assert_eq!(
        doctor_raw_mirror_count_interrupted_captures(&doctor_raw_mirror_root(&data_dir)),
        1
    );
}

#[test]
fn raw_mirror_report_serializes_redacted_manifest_metadata_only_by_default() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp
        .path()
        .join("workspace-with-secrets")
        .join("private-session-with-token.jsonl");
    let raw_bytes = b"RAW_MIRROR_SECRET_PROMPT_SHOULD_NOT_LEAK";
    let mut manifest = raw_mirror_test_manifest(
        &data_dir,
        "codex",
        "local",
        &source_path,
        raw_bytes,
        Vec::new(),
    );
    manifest.compression = DoctorRawMirrorCompressionEnvelope {
        state: "compressed".to_string(),
        algorithm: Some("zstd".to_string()),
        uncompressed_size_bytes: Some(raw_bytes.len() as u64),
    };
    manifest.encryption = DoctorRawMirrorEncryptionEnvelope {
        state: "encrypted".to_string(),
        algorithm: Some("aes-256-gcm".to_string()),
        key_id: Some("local-key-1".to_string()),
        envelope_version: Some(1),
    };
    // The manifest checksum is public integrity metadata, not an
    // authentication boundary. A crafted but internally consistent
    // manifest must not be able to smuggle an exact path through a field
    // whose name merely claims it is redacted.
    manifest.redacted_original_path = source_path.display().to_string();
    manifest.manifest_blake3 = Some(doctor_raw_mirror_manifest_blake3(&manifest));
    write_raw_mirror_test_manifest(&data_dir, &manifest, raw_bytes);

    let report = collect_doctor_raw_mirror_report(&data_dir);
    let payload = serde_json::to_value(&report).expect("raw mirror report json");
    let rendered = serde_json::to_string(&payload).expect("raw mirror rendered json");

    assert_eq!(payload["sensitive_paths_included"].as_bool(), Some(false));
    assert_eq!(payload["raw_content_included"].as_bool(), Some(false));
    assert!(
        payload.get("root_path").is_none(),
        "exact raw mirror root path must not serialize by default: {payload:#}"
    );
    let manifest_payload = &payload["manifests"][0];
    assert!(manifest_payload.get("manifest_path").is_none());
    assert!(manifest_payload.get("blob_path").is_none());
    assert!(manifest_payload.get("original_path").is_none());
    assert_eq!(
        manifest_payload["redacted_original_path"].as_str(),
        Some("[external]/private-session-with-token.jsonl")
    );
    assert_eq!(
        manifest_payload["compression"]["state"].as_str(),
        Some("compressed")
    );
    assert_eq!(
        manifest_payload["compression"]["algorithm"].as_str(),
        Some("zstd")
    );
    assert_eq!(
        manifest_payload["encryption"]["state"].as_str(),
        Some("encrypted")
    );
    assert_eq!(
        manifest_payload["encryption"]["algorithm"].as_str(),
        Some("aes-256-gcm")
    );
    assert!(!rendered.contains("RAW_MIRROR_SECRET_PROMPT_SHOULD_NOT_LEAK"));
    assert!(
        !rendered.contains(&source_path.display().to_string()),
        "default raw mirror report must not serialize the exact source path"
    );
}

#[test]
fn raw_mirror_manifest_identity_supports_dedup_and_distinguishes_byte_changes() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = data_dir.join("sessions/source.jsonl");
    let duplicate_link = DoctorRawMirrorDbLink {
        conversation_id: Some(7),
        message_count: Some(2),
        source_path: Some(source_path.display().to_string()),
        started_at_ms: Some(1_733_000_000_000),
    };
    let merged =
        doctor_raw_mirror_unique_db_links(&[duplicate_link.clone(), duplicate_link.clone()]);
    assert_eq!(
        merged.len(),
        1,
        "same-hash metadata merge must deduplicate identical db_links"
    );

    let first = raw_mirror_test_manifest(
        &data_dir,
        "codex",
        "local",
        &source_path,
        b"same bytes",
        vec![duplicate_link],
    );
    let second_provider = raw_mirror_test_manifest(
        &data_dir,
        "claude_code",
        "local",
        &source_path,
        b"same bytes",
        Vec::new(),
    );
    assert_eq!(
        first.blob_relative_path, second_provider.blob_relative_path,
        "identical bytes should deduplicate to the same content-addressed blob"
    );
    assert_ne!(
        first.manifest_id, second_provider.manifest_id,
        "provider/source metadata remains independently addressable even when blob bytes dedup"
    );

    let changed_bytes = raw_mirror_test_manifest(
        &data_dir,
        "codex",
        "local",
        &source_path,
        b"different bytes",
        Vec::new(),
    );
    assert_ne!(
        first.blob_relative_path, changed_bytes.blob_relative_path,
        "different bytes at the same source identity must never overwrite the existing blob"
    );
    assert_ne!(first.manifest_id, changed_bytes.manifest_id);
}

#[test]
fn raw_mirror_report_verifies_and_reconstructs_chunked_sources() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("large-source-fixture.jsonl");
    let source_bytes = b"0123456789abcdefABCDEFGHIJKLMNOPqrstuvwxyz!@#$%^tail!";
    std::fs::write(&source_path, source_bytes).expect("write chunked source fixture");
    let captured = crate::raw_mirror::capture_source_file_with_chunk_policy(
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
        16,
    )
    .expect("capture chunked source");

    let report = collect_doctor_raw_mirror_report_with_threshold(&data_dir, u64::MAX);
    assert_eq!(report.status, "verified");
    assert_eq!(report.summary.manifest_count, 1);
    assert_eq!(report.summary.verified_blob_count, 5);
    let manifest = report.manifests.first().expect("chunked manifest report");
    assert_eq!(manifest.status, "verified");
    assert_eq!(manifest.storage_kind, "fixed_chunks_v1");
    assert_eq!(manifest.chunk_count, 4);
    assert_eq!(
        manifest.source_content_blake3,
        blake3::hash(source_bytes).to_hex().to_string()
    );
    assert_eq!(manifest.source_size_bytes, Some(source_bytes.len() as u64));
    assert_eq!(
        doctor_candidate_read_verified_raw_mirror_blob(&data_dir, manifest)
            .expect("reconstruct doctor candidate bytes"),
        source_bytes
    );

    let payload = serde_json::to_value(&report).expect("raw mirror report JSON");
    assert_eq!(
        payload["manifests"][0]["storage_kind"].as_str(),
        Some("fixed_chunks_v1")
    );
    assert_eq!(payload["manifests"][0]["chunk_count"].as_u64(), Some(4));
    assert!(
        !serde_json::to_string(&payload)
            .expect("render report")
            .contains("0123456789abcdef"),
        "doctor metadata must not serialize reconstructed source bytes"
    );
    assert_eq!(
        std::fs::read(&source_path).expect("source remains untouched"),
        source_bytes
    );
    assert_eq!(captured.storage_kind, "fixed_chunks_v1");
}

#[test]
fn raw_mirror_report_rejects_whole_blob_source_size_drift() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = data_dir.join("sessions/source-size-drift.jsonl");
    let bytes = b"{\"type\":\"message\",\"text\":\"hello\"}\n";
    let mut manifest =
        raw_mirror_test_manifest(&data_dir, "codex", "local", &source_path, bytes, Vec::new());
    manifest.source_size_bytes = Some(bytes.len() as u64 + 1);
    manifest.manifest_blake3 = Some(doctor_raw_mirror_manifest_blake3(&manifest));
    write_raw_mirror_test_manifest(&data_dir, &manifest, bytes);

    let report = collect_doctor_raw_mirror_report_with_threshold(&data_dir, u64::MAX);
    let manifest_report = report.manifests.first().expect("manifest report");
    assert_eq!(report.status, "warn");
    assert_eq!(manifest_report.status, "invalid_manifest");
    assert_eq!(
        manifest_report.invalid_reason.as_deref(),
        Some("whole-blob source_size_bytes does not match blob_size_bytes")
    );
    assert!(!doctor_raw_mirror_manifest_is_verified(manifest_report));
}

#[test]
fn raw_mirror_backfill_uses_newest_source_digest_for_chunked_evidence() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("mutable-source.jsonl");
    let db_link = crate::raw_mirror::RawMirrorDbLink {
        conversation_id: Some(7),
        message_count: Some(1),
        source_path: Some(source_path.display().to_string()),
        started_at_ms: Some(1_733_000_000_000),
    };
    let first_bytes = b"0123456789abcdefABCDEFGHIJKLMNOP";
    std::fs::write(&source_path, first_bytes).expect("write first source version");
    crate::raw_mirror::capture_source_file_with_chunk_policy(
        crate::raw_mirror::RawMirrorCaptureInput {
            data_dir: &data_dir,
            provider: "codex",
            source_id: "local",
            origin_kind: "local",
            origin_host: None,
            source_path: &source_path,
            db_links: std::slice::from_ref(&db_link),
        },
        1,
        16,
    )
    .expect("capture first chunked source version");

    std::thread::sleep(std::time::Duration::from_millis(2));
    let mut second_bytes = first_bytes.to_vec();
    second_bytes.extend_from_slice(b"tail");
    std::fs::write(&source_path, &second_bytes).expect("write second source version");
    crate::raw_mirror::capture_source_file_with_chunk_policy(
        crate::raw_mirror::RawMirrorCaptureInput {
            data_dir: &data_dir,
            provider: "codex",
            source_id: "local",
            origin_kind: "local",
            origin_host: None,
            source_path: &source_path,
            db_links: std::slice::from_ref(&db_link),
        },
        1,
        16,
    )
    .expect("capture second chunked source version");

    let report = collect_doctor_raw_mirror_report_with_threshold(&data_dir, u64::MAX);
    let (by_conversation_id, by_source_key) = doctor_raw_mirror_existing_evidence_maps(&report);
    let expected_source_blake3 = blake3::hash(&second_bytes).to_hex().to_string();
    let linked = by_conversation_id
        .get(&(7, source_path.display().to_string()))
        .expect("newest conversation-linked evidence");
    assert_eq!(linked.source_content_blake3, expected_source_blake3);
    assert_ne!(
        linked.source_content_blake3, linked.blob_blake3,
        "chunked evidence must distinguish the source digest from the descriptor digest"
    );
    let source_key = doctor_raw_mirror_backfill_source_key(
        "local",
        "local",
        None,
        &source_path.display().to_string(),
    );
    assert_eq!(
        by_source_key
            .get(&source_key)
            .expect("newest source-key evidence")
            .source_content_blake3,
        expected_source_blake3
    );

    let reclassified_candidate = DoctorRawMirrorBackfillCandidate {
        conversation_id: 8,
        provider: "omp".to_string(),
        source_path: Some(source_path.display().to_string()),
        source_id: "local".to_string(),
        origin_host: None,
        origin_kind: Some("local".to_string()),
        started_at_ms: Some(1_733_000_000_001),
        message_count: 1,
    };
    let reclassified_receipt = doctor_raw_mirror_backfill_candidate_receipt(
        &data_dir,
        &reclassified_candidate,
        &by_conversation_id,
        &by_source_key,
        &mut HashMap::new(),
        false,
    );
    assert_eq!(reclassified_receipt.provider, "omp");
    assert_eq!(
        reclassified_receipt.action, "existing_raw_manifest_needs_db_link",
        "provider reclassification must reuse the verified physical-source evidence"
    );
    assert!(reclassified_receipt.raw_source_captured);
    assert!(!reclassified_receipt.raw_mirror_db_linked);
    assert_eq!(
        reclassified_receipt.raw_mirror_manifest_id,
        Some(linked.manifest_id.clone())
    );
    assert_eq!(
        reclassified_receipt
            .raw_mirror_source_content_blake3
            .as_deref(),
        Some(expected_source_blake3.as_str())
    );
    assert_eq!(
        reclassified_receipt.raw_mirror_source_size_bytes,
        Some(second_bytes.len() as u64)
    );
    assert_eq!(
        reclassified_receipt.raw_mirror_storage_kind.as_deref(),
        Some("fixed_chunks_v1")
    );
    assert_eq!(reclassified_receipt.raw_mirror_chunk_count, Some(3));

    let mut receipt = DoctorRawMirrorBackfillReceipt {
        source_stat_snapshot: Some(DoctorRawMirrorBackfillSourceStatSnapshot {
            exists: true,
            file_type: "file".to_string(),
            size_bytes: Some(second_bytes.len() as u64),
            content_blake3: Some(expected_source_blake3),
            ..DoctorRawMirrorBackfillSourceStatSnapshot::default()
        }),
        ..DoctorRawMirrorBackfillReceipt::default()
    };
    assert!(
        !doctor_raw_mirror_backfill_live_source_changed(&mut receipt, linked),
        "byte-identical chunked sources must not be misreported as changed"
    );
    receipt
        .source_stat_snapshot
        .as_mut()
        .expect("source stat")
        .content_blake3 = Some(blake3::hash(first_bytes).to_hex().to_string());
    assert!(doctor_raw_mirror_backfill_live_source_changed(
        &mut receipt,
        linked
    ));
}

/// zp0fp: a reconstruct promotion numbers conversations anew, so a raw
/// mirror manifest keeps a db link to its source's id in the replaced
/// archive while that id now names another source. Evidence is matched
/// by conversation id and source path, so neither source is compared with
/// the other's digest (`live_source_changed`), whichever was captured
/// last. Keyed by id alone, conversation 2 (source A) got source B's
/// newer evidence and was reported as a changed live source.
#[test]
fn raw_mirror_backfill_ignores_db_links_whose_id_now_names_another_source() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let capture = |name: &str, bytes: &[u8], old_id: i64, new_id: i64| {
        let path = temp.path().join(name);
        std::fs::write(&path, bytes).expect("write source");
        let links = [old_id, new_id].map(|id| crate::raw_mirror::RawMirrorDbLink {
            conversation_id: Some(id),
            message_count: Some(1),
            source_path: Some(path.display().to_string()),
            started_at_ms: Some(1_733_000_000_000),
        });
        let record = crate::raw_mirror::capture_source_file_with_chunk_policy(
            crate::raw_mirror::RawMirrorCaptureInput {
                data_dir: &data_dir,
                provider: "codex",
                source_id: "local",
                origin_kind: "local",
                origin_host: None,
                source_path: &path,
                db_links: &links,
            },
            1 << 20,
            1 << 16,
        )
        .expect("capture source");
        (path, record.manifest_id)
    };
    // Source A was conversation 1 in the replaced archive and is 2 now;
    // source B was 2 and is 1. B is captured last, so it is the newer
    // evidence for both ids.
    let (path_a, manifest_a) = capture("a.jsonl", b"alpha source bytes", 1, 2);
    std::thread::sleep(std::time::Duration::from_millis(2));
    let (path_b, manifest_b) = capture("b.jsonl", b"bravo source bytes", 2, 1);

    let report = collect_doctor_raw_mirror_report_with_threshold(&data_dir, u64::MAX);
    let (by_conversation_id, by_source_key) = doctor_raw_mirror_existing_evidence_maps(&report);
    for (conversation_id, path, manifest_id) in
        [(1, &path_b, &manifest_b), (2, &path_a, &manifest_a)]
    {
        let candidate = DoctorRawMirrorBackfillCandidate {
            conversation_id,
            provider: "codex".to_string(),
            source_path: Some(path.display().to_string()),
            source_id: "local".to_string(),
            origin_host: None,
            origin_kind: Some("local".to_string()),
            started_at_ms: Some(1_733_000_000_000),
            message_count: 1,
        };
        let receipt = doctor_raw_mirror_backfill_candidate_receipt(
            &data_dir,
            &candidate,
            &by_conversation_id,
            &by_source_key,
            &mut HashMap::new(),
            false,
        );
        assert_eq!(
            receipt.action, "already_raw_source_captured",
            "conversation {conversation_id}"
        );
        assert_eq!(
            receipt.raw_mirror_manifest_id.as_deref(),
            Some(manifest_id.as_str()),
            "conversation {conversation_id}"
        );
    }
}

#[test]
fn raw_mirror_backfill_hashes_each_distinct_source_path_once_per_report() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("shared-source.jsonl");
    let first_bytes = b"first source snapshot";
    let first_blake3 = blake3::hash(first_bytes).to_hex().to_string();
    std::fs::write(&source_path, first_bytes).expect("write first source snapshot");
    let candidate = |conversation_id| DoctorRawMirrorBackfillCandidate {
        conversation_id,
        provider: "codex".to_string(),
        source_path: Some(source_path.display().to_string()),
        source_id: "local".to_string(),
        origin_host: None,
        origin_kind: Some("local".to_string()),
        started_at_ms: Some(1_733_000_000_000),
        message_count: 1,
    };
    let by_conversation_id = HashMap::new();
    let by_source_key = HashMap::new();
    let mut source_stat_cache = HashMap::new();

    let first = doctor_raw_mirror_backfill_candidate_receipt(
        &data_dir,
        &candidate(1),
        &by_conversation_id,
        &by_source_key,
        &mut source_stat_cache,
        false,
    );
    assert_eq!(
        first
            .source_stat_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.content_blake3.as_deref()),
        Some(first_blake3.as_str())
    );

    let second_bytes = b"a different source snapshot";
    let second_blake3 = blake3::hash(second_bytes).to_hex().to_string();
    std::fs::write(&source_path, second_bytes).expect("plant a later source mutation");
    let second = doctor_raw_mirror_backfill_candidate_receipt(
        &data_dir,
        &candidate(2),
        &by_conversation_id,
        &by_source_key,
        &mut source_stat_cache,
        false,
    );
    assert_eq!(source_stat_cache.len(), 1);
    assert_eq!(
        second
            .source_stat_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.content_blake3.as_deref()),
        first
            .source_stat_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.content_blake3.as_deref()),
        "a report must reuse one source snapshot instead of rehashing the same large provider file per conversation"
    );
    assert_ne!(
        second
            .source_stat_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.content_blake3.as_deref()),
        Some(second_blake3.as_str()),
        "the planted mutation proves the second candidate did not perform another source hash"
    );
}

#[test]
fn gh415_doctor_sensitive_container_never_becomes_repairable_mirror() {
    let temp = tempfile::TempDir::new().unwrap();
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("sensitive-container");
    let bytes = b"security boundary probe: must never be read by doctor";
    std::fs::write(&source_path, bytes).unwrap();
    let before = std::fs::metadata(&source_path).unwrap().modified().unwrap();
    let mut cache = HashMap::new();
    for provider in ["shelley", "grok_bot", "pi_durable"] {
        for origin in ["local", "ssh"] {
            for apply in [false, true] {
                let candidate = DoctorRawMirrorBackfillCandidate {
                    conversation_id: 17,
                    provider: provider.into(),
                    source_path: Some(source_path.display().to_string()),
                    source_id: if origin == "local" { "local" } else { "remote" }.into(),
                    origin_host: None,
                    origin_kind: Some(origin.into()),
                    started_at_ms: None,
                    message_count: 1,
                };
                let receipt = doctor_raw_mirror_backfill_candidate_receipt(
                    &data_dir,
                    &candidate,
                    &HashMap::new(),
                    &HashMap::new(),
                    &mut cache,
                    apply,
                );
                assert_eq!(receipt.action, "disabled_sensitive_container");
                assert!(!doctor_raw_mirror_backfill_receipt_would_mutate(&receipt));
                assert!(receipt.source_stat_snapshot.is_none());
                assert!(!receipt.raw_source_captured);
                assert!(!receipt.raw_mirror_db_linked);
                assert!(!receipt.db_projection_only);
                let variable = match provider {
                    "shelley" => "CASS_SHELLEY_DB",
                    "grok_bot" => "CASS_GROK_BOT_DATA_ROOT",
                    _ => "PI_CODING_AGENT_DIR",
                };
                assert!(
                    receipt
                        .warnings
                        .iter()
                        .any(|message| message.contains(variable))
                );
                let mut report = DoctorRawMirrorBackfillReport::default();
                accumulate_doctor_raw_mirror_backfill_receipt(&mut report, receipt);
                assert_eq!(report.eligible_live_source_count, 0);
                assert_eq!(report.existing_raw_manifest_link_count, 0);
                assert_eq!(report.capture_failure_count, 0);
            }
        }
    }
    assert!(
        cache.is_empty(),
        "denial must precede source reads and hashing"
    );
    assert!(!data_dir.exists());
    assert_eq!(std::fs::read(&source_path).unwrap(), bytes);
    assert_eq!(
        std::fs::metadata(&source_path).unwrap().modified().unwrap(),
        before
    );
    // Ordinary source files remain eligible; do not disable all backfill.
    let control = DoctorRawMirrorBackfillCandidate {
        conversation_id: 18,
        provider: "codex".into(),
        source_path: Some(source_path.display().to_string()),
        source_id: "local".into(),
        origin_host: None,
        origin_kind: Some("local".into()),
        started_at_ms: None,
        message_count: 1,
    };
    let receipt = doctor_raw_mirror_backfill_candidate_receipt(
        &data_dir,
        &control,
        &HashMap::new(),
        &HashMap::new(),
        &mut cache,
        false,
    );
    assert_eq!(receipt.action, "would_capture_live_source");
    assert!(doctor_raw_mirror_backfill_receipt_would_mutate(&receipt));
}

#[test]
fn raw_mirror_backfill_refuses_capture_when_source_content_hash_is_unavailable() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("unverified-source.jsonl");
    let source_bytes = b"source bytes that must not be captured without a digest";
    std::fs::write(&source_path, source_bytes).expect("write source");
    let candidate = DoctorRawMirrorBackfillCandidate {
        conversation_id: 17,
        provider: "codex".to_string(),
        source_path: Some(source_path.display().to_string()),
        source_id: "local".to_string(),
        origin_host: None,
        origin_kind: Some("local".to_string()),
        started_at_ms: Some(1_733_000_000_000),
        message_count: 1,
    };
    let mut source_stat_cache = HashMap::from([(
        source_path.clone(),
        DoctorRawMirrorBackfillSourceStatSnapshot {
            exists: true,
            file_type: "file".to_string(),
            size_bytes: Some(source_bytes.len() as u64),
            content_blake3: None,
            stat_error: Some("injected source read failure".to_string()),
            ..DoctorRawMirrorBackfillSourceStatSnapshot::default()
        },
    )]);

    let receipt = doctor_raw_mirror_backfill_candidate_receipt(
        &data_dir,
        &candidate,
        &HashMap::new(),
        &HashMap::new(),
        &mut source_stat_cache,
        true,
    );

    assert_eq!(
        receipt.action,
        "source_content_unverified_db_projection_only"
    );
    assert!(receipt.db_projection_only);
    assert!(!receipt.raw_source_captured);
    assert!(
        receipt
            .warnings
            .iter()
            .any(|warning| warning.contains("refuses raw-mirror capture")),
        "receipt must explain why unverified bytes were not captured: {:?}",
        receipt.warnings
    );
    assert!(
        !data_dir.join("raw-mirror").exists(),
        "the refusal path must not create raw-mirror state"
    );
    assert_eq!(
        std::fs::read(&source_path).expect("read untouched source"),
        source_bytes
    );
}

#[test]
fn raw_mirror_backfill_does_not_report_an_unopenable_archive_as_available() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let db_path = data_dir.join("agent_search.db");
    std::fs::write(&db_path, b"not a sqlite database").expect("write malformed archive");
    let raw_mirror = collect_doctor_raw_mirror_report(&data_dir);

    let report = collect_doctor_raw_mirror_backfill_report(&data_dir, &db_path, &raw_mirror, false);

    assert!(!report.db_available);
    assert_eq!(report.status, "warn");
    assert!(report.db_query_error.is_some());
    assert!(report.receipts.is_empty());
}

#[test]
fn raw_mirror_report_verifies_blobs_flags_manifest_drift_and_interrupted_captures() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = data_dir.join("sessions/source.jsonl");
    let bytes = b"{\"type\":\"message\",\"text\":\"hello\"}\n";
    let good_manifest =
        raw_mirror_test_manifest(&data_dir, "codex", "local", &source_path, bytes, Vec::new());
    let (blob_path, _) = write_raw_mirror_test_manifest(&data_dir, &good_manifest, bytes);

    let drift_path = data_dir.join("sessions/source-drift.jsonl");
    let mut drift_manifest =
        raw_mirror_test_manifest(&data_dir, "codex", "local", &drift_path, bytes, Vec::new());
    drift_manifest.manifest_blake3 = Some("doctor-raw-mirror-manifest-v1-bad".to_string());
    write_raw_mirror_test_manifest(&data_dir, &drift_manifest, bytes);

    let unverified_bytes = b"{\"type\":\"message\",\"text\":\"needs manifest checksum\"}\n";
    let unverified_path = data_dir.join("sessions/source-unverified.jsonl");
    let mut unverified_manifest = raw_mirror_test_manifest(
        &data_dir,
        "codex",
        "local",
        &unverified_path,
        unverified_bytes,
        Vec::new(),
    );
    unverified_manifest.manifest_blake3 = None;
    write_raw_mirror_test_manifest(&data_dir, &unverified_manifest, unverified_bytes);

    let tmp_dir = doctor_raw_mirror_root(&data_dir).join("tmp/op-1");
    std::fs::create_dir_all(&tmp_dir).expect("create tmp dir");
    std::fs::write(tmp_dir.join("capture.tmp"), b"partial").expect("write tmp");

    let report = collect_doctor_raw_mirror_report(&data_dir);
    assert_eq!(report.status, "warn");
    assert_eq!(report.summary.manifest_count, 3);
    assert_eq!(report.summary.verified_blob_count, 2);
    assert_eq!(report.summary.manifest_checksum_mismatch_count, 1);
    assert_eq!(report.summary.manifest_checksum_not_recorded_count, 1);
    assert_eq!(report.summary.interrupted_capture_count, 1);
    assert_eq!(report.summary.duplicate_blob_reference_count, 1);
    assert_eq!(
        report.summary.total_blob_bytes,
        (bytes.len() + unverified_bytes.len()) as u64,
        "summary byte count should reflect unique deduplicated blob storage"
    );
    assert!(blob_path.exists());

    let good_report = report
        .manifests
        .iter()
        .find(|manifest| manifest.manifest_id == good_manifest.manifest_id)
        .expect("good manifest report");
    assert_eq!(
        good_report.blob_checksum_status,
        DoctorArtifactChecksumStatus::Matched
    );
    assert_eq!(
        good_report.redacted_original_path,
        "[cass-data]/sessions/source.jsonl"
    );
    assert_eq!(good_report.upstream_path_exists, Some(false));

    let drift_report = report
        .manifests
        .iter()
        .find(|manifest| manifest.manifest_id == drift_manifest.manifest_id)
        .expect("drift manifest report");
    assert_eq!(drift_report.status, "manifest_drift");
    assert_eq!(
        drift_report.manifest_checksum_status,
        DoctorArtifactChecksumStatus::Mismatched
    );

    let unverified_report = report
        .manifests
        .iter()
        .find(|manifest| manifest.manifest_id == unverified_manifest.manifest_id)
        .expect("unverified manifest report");
    assert_eq!(unverified_report.status, "manifest_unverified");
    assert_eq!(
        unverified_report.manifest_checksum_status,
        DoctorArtifactChecksumStatus::NotRecorded
    );
}

#[test]
fn raw_mirror_report_warns_when_verified_blob_bytes_cross_threshold() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = data_dir.join("sessions/large-source.jsonl");
    let bytes = b"{\"type\":\"message\",\"text\":\"size warning\"}\n";
    let manifest =
        raw_mirror_test_manifest(&data_dir, "codex", "local", &source_path, bytes, Vec::new());
    write_raw_mirror_test_manifest(&data_dir, &manifest, bytes);

    let report = collect_doctor_raw_mirror_report_with_threshold(&data_dir, 1);

    assert_eq!(report.status, "warn");
    assert_eq!(report.summary.total_blob_bytes, bytes.len() as u64);
    assert!(
        report
            .warnings
            .iter()
            .any(|warning| warning.contains("raw_mirror.size")
                && warning.contains("cass mirror prune --keep-latest 1 --json")),
        "raw mirror size warning should include the canonical prune dry-run: {report:#?}"
    );
}

#[test]
fn gh430_raw_mirror_report_warns_on_per_source_full_copy_amplification() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = data_dir.join("sessions/growing.jsonl");
    std::fs::create_dir_all(source_path.parent().expect("source parent"))
        .expect("create source parent");
    std::fs::write(&source_path, b"x").expect("write current source");

    let first_bytes = b"first complete historical source version";
    let second_bytes = b"second complete historical source version with appended bytes";
    let first = raw_mirror_test_manifest(
        &data_dir,
        "pi_agent",
        "backup-local",
        &source_path,
        first_bytes,
        Vec::new(),
    );
    let second = raw_mirror_test_manifest(
        &data_dir,
        "omp",
        "backup-local",
        &source_path,
        second_bytes,
        Vec::new(),
    );
    write_raw_mirror_test_manifest(&data_dir, &first, first_bytes);
    write_raw_mirror_test_manifest(&data_dir, &second, second_bytes);

    // A remote machine can have the same path text. Its historical bytes
    // must not be compared with this host's current file metadata.
    for remote_bytes in [
        &b"first remote source version"[..],
        &b"second much larger remote source version"[..],
    ] {
        let remote = raw_mirror_test_manifest_with_origin(
            &data_dir,
            "codex",
            "remote-workstation",
            "ssh",
            Some("remote-workstation"),
            &source_path,
            remote_bytes,
            Vec::new(),
        );
        write_raw_mirror_test_manifest(&data_dir, &remote, remote_bytes);
    }

    let report =
        collect_doctor_raw_mirror_report_with_amplification_thresholds(&data_dir, 2_000, 1);
    let expected_referenced_bytes = (first_bytes.len() + second_bytes.len()) as u64;

    assert_eq!(report.status, "warn");
    assert_eq!(
        report.summary.amplified_source_count, 1,
        "remote manifests must not be measured against an unrelated local path"
    );
    assert_eq!(
        report.summary.amplified_source_referenced_bytes,
        expected_referenced_bytes
    );
    assert_eq!(
        report.summary.amplified_source_excess_bytes,
        expected_referenced_bytes - 1
    );
    let amplified = report
        .amplified_sources
        .first()
        .expect("source amplification detail");
    assert_eq!(amplified.evidence_kind, "full_blob_checksum");
    assert_eq!(amplified.source_id, "backup-local");
    assert_eq!(
        amplified.providers,
        vec!["omp".to_string(), "pi_agent".to_string()],
        "provider reclassification must not split one physical source path into separate amplification groups"
    );
    assert_eq!(amplified.version_count, 2);
    assert_eq!(amplified.referenced_blob_bytes, expected_referenced_bytes);
    assert_eq!(amplified.current_source_size_bytes, 1);
    assert_eq!(
        amplified.amplification_ratio_milli,
        Some(expected_referenced_bytes * 1_000)
    );
    assert!(
        report.warnings.iter().any(
            |warning| warning.contains("raw_mirror.source_amplification")
                && warning.contains("sole-copy evidence")
        ),
        "source amplification warning must explain the archive-safety boundary: {report:#?}"
    );
    let rendered = serde_json::to_string(&report).expect("serialize raw mirror report");
    assert!(
        !rendered.contains(&source_path.display().to_string()),
        "source amplification diagnostics must not expose exact source paths"
    );
}

#[test]
fn gh430_bounded_doctor_still_reports_amplification_without_claiming_blob_verification() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = data_dir.join("sessions/growing.jsonl");
    std::fs::create_dir_all(source_path.parent().expect("source parent"))
        .expect("create source parent");
    std::fs::write(&source_path, b"x").expect("write current source");

    let versions: [(&str, &[u8]); 3] = [
        ("pi_agent", b"first complete source version"),
        ("omp", b"second complete source version with appended bytes"),
        (
            "omp",
            b"third complete source version with still more appended bytes",
        ),
    ];
    let mut valid_manifest_entries = Vec::new();
    for (provider, bytes) in versions {
        let manifest = raw_mirror_test_manifest(
            &data_dir,
            provider,
            "local",
            &source_path,
            bytes,
            Vec::new(),
        );
        let (_, manifest_path) = write_raw_mirror_test_manifest(&data_dir, &manifest, bytes);
        valid_manifest_entries.push((manifest_path, false));
    }

    let rejected_bytes = b"forged fourth version must not influence the warning";
    let mut rejected = raw_mirror_test_manifest(
        &data_dir,
        "omp",
        "local",
        &source_path,
        rejected_bytes,
        Vec::new(),
    );
    rejected.manifest_blake3 = Some("doctor-raw-mirror-manifest-v1-forged".to_string());
    write_raw_mirror_test_manifest(&data_dir, &rejected, rejected_bytes);

    let report = collect_doctor_raw_mirror_report_with_thresholds_and_mode(
        &data_dir,
        0,
        2_000,
        1,
        DoctorRawMirrorVerificationMode::Bounded {
            manifest_limit: 1,
            byte_limit: u64::MAX,
        },
    );

    assert_eq!(report.status, "verification_deferred");
    assert_eq!(report.summary.manifest_count, 4);
    assert_eq!(report.summary.verified_blob_count, 0);
    assert!(
        report.manifests.is_empty(),
        "bounded metadata diagnostics must not masquerade as fully verified manifest reports"
    );
    let amplified = report
        .amplified_sources
        .first()
        .expect("bounded source amplification detail");
    assert_eq!(
        amplified.evidence_kind,
        "manifest_checksum_and_blob_metadata"
    );
    assert_eq!(
        amplified.version_count, 3,
        "the forged manifest must be excluded from the bounded amplification evidence"
    );
    assert_eq!(
        amplified.providers,
        vec!["omp".to_string(), "pi_agent".to_string()]
    );
    assert!(report.warnings.iter().any(|warning| {
        warning.contains("raw_mirror.source_amplification")
            && warning.contains("evidence=manifest_checksum_and_blob_metadata")
    }));
    let rendered = serde_json::to_string(&report).expect("serialize bounded report");
    assert!(!rendered.contains(&source_path.display().to_string()));

    let capped_scan = doctor_raw_mirror_bounded_amplification_reports_with_limits(
        &data_dir,
        &doctor_raw_mirror_root(&data_dir),
        &valid_manifest_entries,
        false,
        2_000,
        1,
        DoctorRawMirrorBoundedScanLimits {
            manifest_count: 2,
            byte_count: u64::MAX,
        },
    );
    assert_eq!(capped_scan.scanned_manifest_count, 2);
    assert!(capped_scan.truncated);
    assert_eq!(capped_scan.reports.len(), 1);
    assert_eq!(capped_scan.reports[0].version_count, 2);

    let byte_limited = collect_doctor_raw_mirror_report_with_thresholds_and_mode(
        &data_dir,
        0,
        2_000,
        1,
        DoctorRawMirrorVerificationMode::Bounded {
            manifest_limit: 100,
            byte_limit: 1,
        },
    );
    assert_eq!(byte_limited.status, "verification_deferred");
    assert_eq!(byte_limited.summary.manifest_count, 4);
    assert!(byte_limited.manifests.is_empty());
    assert!(byte_limited.warnings.iter().any(|warning| {
        warning.contains(
            "physical bytes across blobs, manifests, logs, and interrupted temp artifacts",
        ) && warning.contains("1 physical bytes")
    }));

    let poisoned_db_path = data_dir.join("agent_search.db");
    std::fs::write(&poisoned_db_path, b"not a sqlite database")
        .expect("plant a DB that would fail if bounded doctor queried backfill");
    let deferred_backfill = collect_doctor_raw_mirror_backfill_report_for_doctor(
        &data_dir,
        &poisoned_db_path,
        &byte_limited,
        false,
        None,
    );
    assert_eq!(deferred_backfill.status, "deferred");
    assert!(!deferred_backfill.db_available);
    assert!(deferred_backfill.warnings.iter().any(|warning| {
        warning.contains("unverified manifests must not be treated as absent evidence")
    }));
}

#[test]
fn gh430_bounded_doctor_caps_logical_verification_work_for_shared_chunks() {
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
fn raw_mirror_full_report_rejects_oversized_manifest_before_parsing() {
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
fn raw_mirror_report_rejects_symlink_manifest_entries() {
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
fn raw_mirror_report_rejects_blob_paths_with_symlinked_ancestors() {
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
fn raw_mirror_report_counts_missing_blob_without_relabeling_manifest_invalid() {
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
fn archive_scan_classifies_normalizable_metadata_and_critical_archive_risks() {
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
fn archive_normalize_apply_writes_only_additive_idempotent_annotations() {
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
fn archive_normalize_refuses_hostile_annotation_paths() {
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
fn archive_normalize_refuses_symlinked_annotation_targets_and_data_dirs() {
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
