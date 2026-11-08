use crate::doctor::forensic::doctor_redacted_path;
use crate::doctor::policy::DoctorArtifactChecksumStatus;
use crate::doctor::raw_mirror::doctor_raw_mirror_root_dir::{
    CASS_DOCTOR_RAW_MIRROR_FULL_VERIFY, DOCTOR_RAW_MIRROR_SCHEMA_VERSION,
};
use crate::doctor::raw_mirror::doctorrawmirrorverificationmode::{
    DoctorRawMirrorReport, DoctorRawMirrorSummary, DoctorRawMirrorVerificationMode,
    doctor_raw_mirror_apply_source_amplification_reports,
    doctor_raw_mirror_bounded_amplification_reports, doctor_raw_mirror_count_interrupted_captures,
    doctor_raw_mirror_estimated_verification_bytes, doctor_raw_mirror_invalid_manifest_report,
    doctor_raw_mirror_layout_report, doctor_raw_mirror_policy_report, doctor_raw_mirror_root,
    doctor_raw_mirror_size_warning, doctor_raw_mirror_source_amplification_reports,
    doctor_verify_raw_mirror_manifest,
};
use crate::doctor::repair_state::types::{
    DOCTOR_RAW_MIRROR_BOUNDED_AMPLIFICATION_MANIFEST_LIMIT, DOCTOR_RAW_MIRROR_MANIFEST_MAX_BYTES,
};
use crate::doctor::report::inventory::{
    DoctorCoverageRiskSummary, DoctorCoverageSummary, DoctorRawMirrorBackfillReport,
    DoctorSoleCopyWarning, DoctorSourceAuthorityReport, DoctorSourceInventoryReport,
};
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub(crate) fn collect_doctor_raw_mirror_report_with_thresholds_and_mode(
    data_dir: &Path,
    size_warn_threshold_bytes: u64,
    source_amplification_ratio_warn_threshold_milli: u64,
    source_amplification_excess_warn_threshold_bytes: u64,
    verification_mode: DoctorRawMirrorVerificationMode,
) -> DoctorRawMirrorReport {
    let root = doctor_raw_mirror_root(data_dir);
    let root_path = root.display().to_string();
    let mut report = DoctorRawMirrorReport {
        schema_version: DOCTOR_RAW_MIRROR_SCHEMA_VERSION,
        status: "absent".to_string(),
        root_path: root_path.clone(),
        redacted_root_path: doctor_redacted_path(&root_path, data_dir),
        exists: root.exists(),
        sensitive_paths_included: false,
        raw_content_included: false,
        layout: doctor_raw_mirror_layout_report(),
        policy: doctor_raw_mirror_policy_report(),
        summary: DoctorRawMirrorSummary::default(),
        manifests: Vec::new(),
        amplified_sources: Vec::new(),
        warnings: Vec::new(),
        notes: vec![
            "Raw mirror blobs are precious evidence and are never automatic cleanup candidates.".to_string(),
            "A verified mirror blob remains useful when the upstream provider file has been pruned.".to_string(),
        ],
    };

    if !root.exists() {
        return report;
    }
    if std::fs::symlink_metadata(&root)
        .map(|metadata| metadata.file_type().is_symlink())
        .unwrap_or(false)
    {
        report.status = "warn".to_string();
        report
            .warnings
            .push("raw mirror root is a symlink and will not be trusted for repair".to_string());
        return report;
    }

    let physical_inventory = crate::raw_mirror::storage_summary(data_dir);
    report.summary.orphan_blob_count = physical_inventory.orphan_blob_count;
    report.summary.orphan_blob_bytes = physical_inventory.orphan_blob_bytes;
    if report.summary.orphan_blob_count > 0 {
        report.warnings.push(format!(
            "raw_mirror.orphan_blobs: {} unreferenced content-addressed blob(s) use {} bytes; inspect `cass mirror prune --older-than 7d --json` before applying cleanup",
            report.summary.orphan_blob_count, report.summary.orphan_blob_bytes
        ));
    }

    report.summary.interrupted_capture_count = doctor_raw_mirror_count_interrupted_captures(&root);
    if report.summary.interrupted_capture_count > 0 {
        report.warnings.push(format!(
            "{} interrupted raw mirror capture artifact(s) remain under tmp",
            report.summary.interrupted_capture_count
        ));
    }

    let manifest_root = root.join("manifests");
    let (bounded_manifest_limit, bounded_byte_limit) = match verification_mode {
        DoctorRawMirrorVerificationMode::Full => (None, None),
        DoctorRawMirrorVerificationMode::Bounded {
            manifest_limit,
            byte_limit,
        } => (Some(manifest_limit), Some(byte_limit)),
    };
    let mut manifest_entries: Vec<(PathBuf, bool)> = Vec::new();
    let mut manifest_entry_count = 0usize;
    let mut full_verification_deferred = false;
    let bounded_inventory_entry_limit = bounded_manifest_limit
        .map(|limit| limit.max(DOCTOR_RAW_MIRROR_BOUNDED_AMPLIFICATION_MANIFEST_LIMIT));
    if manifest_root.exists() {
        for entry in walkdir::WalkDir::new(&manifest_root)
            .follow_links(false)
            .into_iter()
        {
            match entry {
                Ok(entry) if entry.file_type().is_file() || entry.file_type().is_symlink() => {
                    let path = entry.path();
                    if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                        continue;
                    }
                    manifest_entry_count += 1;
                    if bounded_manifest_limit.is_some_and(|limit| manifest_entry_count > limit) {
                        full_verification_deferred = true;
                    }
                    if bounded_inventory_entry_limit
                        .is_none_or(|limit| manifest_entries.len() < limit)
                    {
                        manifest_entries.push((path.to_path_buf(), entry.file_type().is_symlink()));
                    }
                }
                Ok(_) => {}
                Err(err) => report
                    .warnings
                    .push(format!("failed to scan raw mirror manifest entry: {err}")),
            }
        }
    }

    let physical_storage_bytes = bounded_byte_limit
        .map(|_| crate::raw_mirror::physical_storage_bytes(data_dir))
        .unwrap_or(0);
    let physical_limit_exceeded =
        bounded_byte_limit.is_some_and(|limit| physical_storage_bytes > limit);
    let verification_work_computed =
        bounded_byte_limit.is_some() && !physical_limit_exceeded && !full_verification_deferred;
    let estimated_verification_bytes = if verification_work_computed {
        doctor_raw_mirror_estimated_verification_bytes(&manifest_entries)
    } else {
        0
    };
    let verification_work_limit_exceeded =
        bounded_byte_limit.is_some_and(|limit| estimated_verification_bytes > limit);
    if physical_limit_exceeded || verification_work_limit_exceeded {
        full_verification_deferred = true;
    }

    if full_verification_deferred {
        report.summary.manifest_count = manifest_entry_count;
        report.status = "verification_deferred".to_string();
        let amplification_scan = doctor_raw_mirror_bounded_amplification_reports(
            data_dir,
            &root,
            &manifest_entries,
            manifest_entries.len() < manifest_entry_count,
            source_amplification_ratio_warn_threshold_milli,
            source_amplification_excess_warn_threshold_bytes,
        );
        doctor_raw_mirror_apply_source_amplification_reports(
            &mut report,
            amplification_scan.reports,
        );
        let limit = bounded_manifest_limit.unwrap_or(0);
        let byte_limit = bounded_byte_limit.unwrap_or(0);
        let verification_work = if physical_limit_exceeded {
            "not computed because the physical-byte limit was already exceeded".to_string()
        } else if !verification_work_computed {
            "not computed because the manifest-count limit was already exceeded".to_string()
        } else {
            format!("{estimated_verification_bytes} bytes")
        };
        report.warnings.push(format!(
            "Raw mirror verification deferred: inventory has {manifest_entry_count} manifest(s) and {physical_storage_bytes} physical bytes across blobs, manifests, logs, and interrupted temp artifacts; estimated full verification work is {verification_work}. Bounded doctor limits are {limit} manifests, {byte_limit} physical bytes, and {byte_limit} estimated verification-work bytes. Set {CASS_DOCTOR_RAW_MIRROR_FULL_VERIFY}=1 to run a full blob checksum verification."
        ));
        report.notes.push(
            "Bounded doctor mode does not claim raw mirror blobs are verified until full checksum verification runs."
                .to_string(),
        );
        report.notes.push(
            "Per-source amplification remains available in bounded mode using checksum-verified manifest metadata plus regular-file blob sizes; this evidence measures storage growth but is not blob-content or restore-authority proof."
                .to_string(),
        );
        report.notes.push(format!(
            "Bounded amplification diagnostics scanned {} of {} manifest entries{}.",
            amplification_scan.scanned_manifest_count,
            manifest_entry_count,
            if amplification_scan.truncated {
                "; the evidence is partial because the metadata scan cap was reached"
            } else {
                ""
            }
        ));
        return report;
    }

    for (path, is_symlink) in manifest_entries {
        let report_entry = if is_symlink {
            doctor_raw_mirror_invalid_manifest_report(
                data_dir,
                &path,
                "manifest path is a symlink".to_string(),
            )
        } else if std::fs::symlink_metadata(&path)
            .is_ok_and(|metadata| metadata.len() > DOCTOR_RAW_MIRROR_MANIFEST_MAX_BYTES)
        {
            doctor_raw_mirror_invalid_manifest_report(
                data_dir,
                &path,
                format!(
                    "manifest exceeds the {}-byte validation limit",
                    DOCTOR_RAW_MIRROR_MANIFEST_MAX_BYTES
                ),
            )
        } else {
            match std::fs::read_to_string(&path)
                .ok()
                .and_then(|content| serde_json::from_str(&content).ok())
            {
                Some(manifest) => {
                    doctor_verify_raw_mirror_manifest(data_dir, &root, &path, manifest)
                }
                None => doctor_raw_mirror_invalid_manifest_report(
                    data_dir,
                    &path,
                    "manifest is not parseable JSON".to_string(),
                ),
            }
        };
        report.manifests.push(report_entry);
    }

    report
        .manifests
        .sort_by(|left, right| left.manifest_path.cmp(&right.manifest_path));
    report.summary.manifest_count = report.manifests.len();
    let mut blob_reference_counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut verified_blob_bytes: BTreeMap<String, u64> = BTreeMap::new();
    for manifest in &report.manifests {
        if !manifest.verified_stored_blobs.is_empty() {
            for (blob_blake3, blob_size_bytes) in &manifest.verified_stored_blobs {
                *blob_reference_counts
                    .entry(blob_blake3.clone())
                    .or_default() += 1;
                verified_blob_bytes
                    .entry(blob_blake3.clone())
                    .or_insert(*blob_size_bytes);
            }
        } else if !manifest.blob_blake3.is_empty() {
            *blob_reference_counts
                .entry(manifest.blob_blake3.clone())
                .or_default() += 1;
        }
        if manifest.blob_checksum_status == DoctorArtifactChecksumStatus::Missing {
            report.summary.missing_blob_count += 1;
        }
        if manifest.blob_checksum_status == DoctorArtifactChecksumStatus::Mismatched {
            report.summary.checksum_mismatch_count += 1;
        }
        if manifest.manifest_checksum_status == DoctorArtifactChecksumStatus::Mismatched {
            report.summary.manifest_checksum_mismatch_count += 1;
        }
        if manifest.manifest_checksum_status == DoctorArtifactChecksumStatus::NotRecorded {
            report.summary.manifest_checksum_not_recorded_count += 1;
        }
        if manifest.status == "invalid_manifest" {
            report.summary.invalid_manifest_count += 1;
        }
    }
    report.summary.duplicate_blob_reference_count = blob_reference_counts
        .values()
        .map(|count| count.saturating_sub(1))
        .sum();
    report.summary.verified_blob_count = verified_blob_bytes.len();
    report.summary.total_blob_bytes = verified_blob_bytes.values().copied().sum();
    if let Some(warning) =
        doctor_raw_mirror_size_warning(report.summary.total_blob_bytes, size_warn_threshold_bytes)
    {
        report.warnings.push(warning);
    }
    let amplified_sources = doctor_raw_mirror_source_amplification_reports(
        &report.manifests,
        source_amplification_ratio_warn_threshold_milli,
        source_amplification_excess_warn_threshold_bytes,
    );
    doctor_raw_mirror_apply_source_amplification_reports(&mut report, amplified_sources);

    report.status = if report.summary.invalid_manifest_count > 0
        || report.summary.missing_blob_count > 0
        || report.summary.checksum_mismatch_count > 0
        || report.summary.manifest_checksum_mismatch_count > 0
        || report.summary.manifest_checksum_not_recorded_count > 0
        || report.summary.interrupted_capture_count > 0
        || report.summary.orphan_blob_count > 0
        || report.summary.amplified_source_count > 0
        || doctor_raw_mirror_size_warning(
            report.summary.total_blob_bytes,
            size_warn_threshold_bytes,
        )
        .is_some()
    {
        "warn".to_string()
    } else if report.summary.manifest_count == 0 {
        "empty".to_string()
    } else {
        "verified".to_string()
    };

    report
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct DoctorArchiveCoverageSnapshot {
    pub(crate) schema_version: u32,
    pub(crate) archive_conversation_count: usize,
    pub(crate) archived_message_count: usize,
    pub(crate) source_identity_count: usize,
    pub(crate) raw_mirror_manifest_count: usize,
    pub(crate) raw_mirror_verified_blob_count: usize,
    pub(crate) raw_mirror_db_link_count: usize,
    pub(crate) db_without_raw_mirror_count: usize,
    pub(crate) mirror_without_db_link_count: usize,
    pub(crate) missing_current_source_count: usize,
    pub(crate) unknown_mapping_count: usize,
    pub(crate) sole_copy_candidate_count: usize,
    pub(crate) precious_raw_mirror_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorArchiveFinding {
    pub(crate) finding_id: String,
    pub(crate) severity: String,
    pub(crate) scope: String,
    pub(crate) dedupe_key: String,
    pub(crate) asset_class: String,
    pub(crate) finding_kind: String,
    pub(crate) evidence: Vec<String>,
    pub(crate) confidence: String,
    pub(crate) recommendation: String,
    pub(crate) safe_to_normalize: bool,
    pub(crate) normalization_kind: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorArchiveScanReport {
    pub(crate) schema_version: u32,
    pub(crate) status: String,
    pub(crate) mutation_performed: bool,
    pub(crate) auto_fix_applied: bool,
    pub(crate) issues_fixed: usize,
    pub(crate) finding_count: usize,
    pub(crate) critical_finding_count: usize,
    pub(crate) safe_normalization_candidate_count: usize,
    pub(crate) coverage_before: DoctorArchiveCoverageSnapshot,
    pub(crate) coverage_after: DoctorArchiveCoverageSnapshot,
    pub(crate) coverage_reduced: bool,
    pub(crate) findings: Vec<DoctorArchiveFinding>,
    pub(crate) recommendations: Vec<String>,
    pub(crate) notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorArchiveNormalizeAction {
    pub(crate) action_id: String,
    pub(crate) action_kind: String,
    pub(crate) finding_id: String,
    pub(crate) finding_kind: String,
    pub(crate) scope: String,
    pub(crate) asset_class: String,
    pub(crate) target_relative_path: String,
    pub(crate) redacted_target_path: String,
    pub(crate) content_blake3: String,
    pub(crate) description: String,
    pub(crate) additive_only: bool,
    pub(crate) idempotent: bool,
    pub(crate) precious_asset_mutation: bool,
    #[serde(skip_serializing)]
    pub(crate) annotation_content: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorArchiveNormalizeSkippedAction {
    pub(crate) finding_id: String,
    pub(crate) finding_kind: String,
    pub(crate) severity: String,
    pub(crate) scope: String,
    pub(crate) asset_class: String,
    pub(crate) reason: String,
    pub(crate) recommendation: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorArchiveNormalizeSafetyGate {
    pub(crate) gate_id: String,
    pub(crate) status: String,
    pub(crate) message: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorArchiveNormalizePlan {
    pub(crate) schema_version: u32,
    pub(crate) status: String,
    pub(crate) dry_run: bool,
    pub(crate) apply_requested: bool,
    pub(crate) apply_authorized: bool,
    pub(crate) approval_status: String,
    pub(crate) will_mutate: bool,
    pub(crate) plan_fingerprint: String,
    pub(crate) exact_apply_argv: Vec<String>,
    pub(crate) exact_apply_command: String,
    pub(crate) action_count: usize,
    pub(crate) skipped_action_count: usize,
    pub(crate) coverage_before: DoctorArchiveCoverageSnapshot,
    pub(crate) expected_coverage_after: DoctorArchiveCoverageSnapshot,
    pub(crate) coverage_reduced: bool,
    pub(crate) actions: Vec<DoctorArchiveNormalizeAction>,
    pub(crate) skipped_actions: Vec<DoctorArchiveNormalizeSkippedAction>,
    pub(crate) safety_gates: Vec<DoctorArchiveNormalizeSafetyGate>,
    pub(crate) notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorArchiveNormalizeEvent {
    pub(crate) event: String,
    pub(crate) action_id: Option<String>,
    pub(crate) target_relative_path: Option<String>,
    pub(crate) message: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorArchiveNormalizeReceipt {
    pub(crate) schema_version: u32,
    pub(crate) status: String,
    pub(crate) plan_fingerprint: String,
    pub(crate) mutation_performed: bool,
    pub(crate) applied_action_count: usize,
    pub(crate) already_present_count: usize,
    pub(crate) skipped_action_count: usize,
    pub(crate) refused_action_count: usize,
    pub(crate) coverage_before: DoctorArchiveCoverageSnapshot,
    pub(crate) coverage_after: DoctorArchiveCoverageSnapshot,
    pub(crate) coverage_reduced: bool,
    pub(crate) event_log: Vec<DoctorArchiveNormalizeEvent>,
    pub(crate) notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorArchiveScanContext {
    pub(crate) scan: DoctorArchiveScanReport,
    pub(crate) source_inventory: DoctorSourceInventoryReport,
    pub(crate) raw_mirror: DoctorRawMirrorReport,
    pub(crate) raw_mirror_backfill: DoctorRawMirrorBackfillReport,
    pub(crate) coverage_summary: DoctorCoverageSummary,
    pub(crate) sole_copy_warnings: Vec<DoctorSoleCopyWarning>,
    pub(crate) coverage_risk: DoctorCoverageRiskSummary,
    pub(crate) source_authority: DoctorSourceAuthorityReport,
}
