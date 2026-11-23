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

#[derive(Debug, Default)]
pub(crate) struct DoctorRawMirrorSourceAmplificationAccumulator {
    pub(crate) providers: BTreeSet<String>,
    pub(crate) source_id: String,
    pub(crate) original_path: String,
    pub(crate) redacted_original_path: String,
    pub(crate) original_path_blake3: String,
    pub(crate) snapshots: BTreeSet<String>,
    pub(crate) blobs: BTreeMap<String, u64>,
}

#[derive(Debug)]
pub(crate) struct DoctorRawMirrorBoundedAmplificationScan {
    pub(crate) reports: Vec<DoctorRawMirrorSourceAmplificationReport>,
    pub(crate) scanned_manifest_count: usize,
    pub(crate) truncated: bool,
}

pub(crate) fn doctor_raw_mirror_source_amplification_reports(
    manifests: &[DoctorRawMirrorManifestReport],
    ratio_warn_threshold_milli: u64,
    excess_warn_threshold_bytes: u64,
) -> Vec<DoctorRawMirrorSourceAmplificationReport> {
    let versions = manifests
        .iter()
        .filter(|manifest| doctor_raw_mirror_manifest_is_verified(manifest))
        .map(|manifest| DoctorRawMirrorSourceAmplificationVersion {
            provider: manifest.provider.clone(),
            source_id: manifest.source_id.clone(),
            origin_kind: manifest.origin_kind.clone(),
            origin_host: manifest.origin_host.clone(),
            original_path: manifest.original_path.clone(),
            redacted_original_path: manifest.redacted_original_path.clone(),
            original_path_blake3: manifest.original_path_blake3.clone(),
            snapshot_blake3: manifest.blob_blake3.clone(),
            stored_blobs: manifest.verified_stored_blobs.clone(),
        })
        .collect::<Vec<_>>();
    doctor_raw_mirror_source_amplification_reports_from_versions(
        &versions,
        "full_blob_checksum",
        ratio_warn_threshold_milli,
        excess_warn_threshold_bytes,
    )
}

pub(crate) fn doctor_raw_mirror_source_amplification_reports_from_versions(
    versions: &[DoctorRawMirrorSourceAmplificationVersion],
    evidence_kind: &'static str,
    ratio_warn_threshold_milli: u64,
    excess_warn_threshold_bytes: u64,
) -> Vec<DoctorRawMirrorSourceAmplificationReport> {
    let mut sources: BTreeMap<(String, String), DoctorRawMirrorSourceAmplificationAccumulator> =
        BTreeMap::new();
    for version in versions {
        // `original_path` is meaningful only on the machine that captured a
        // local source. A remote manifest can carry the same path text as an
        // unrelated local file, so comparing its historical bytes with local
        // filesystem metadata would manufacture an amplification warning.
        let normalized_source_id = normalized_provenance_source_id(
            version.source_id.as_str(),
            Some(version.origin_kind.as_str()),
            version.origin_host.as_deref(),
        );
        let normalized_origin_kind = normalized_provenance_origin_kind(
            normalized_source_id.as_str(),
            Some(version.origin_kind.as_str()),
        );
        if normalized_origin_kind != crate::sources::provenance::LOCAL_SOURCE_ID {
            continue;
        }
        let key = (
            version.source_id.clone(),
            version.original_path_blake3.clone(),
        );
        let source =
            sources
                .entry(key)
                .or_insert_with(|| DoctorRawMirrorSourceAmplificationAccumulator {
                    providers: BTreeSet::new(),
                    source_id: version.source_id.clone(),
                    original_path: version.original_path.clone(),
                    redacted_original_path: version.redacted_original_path.clone(),
                    original_path_blake3: version.original_path_blake3.clone(),
                    snapshots: BTreeSet::new(),
                    blobs: BTreeMap::new(),
                });
        source.providers.insert(version.provider.clone());
        source.snapshots.insert(version.snapshot_blake3.clone());
        for (blob_blake3, blob_size_bytes) in &version.stored_blobs {
            source
                .blobs
                .entry(blob_blake3.clone())
                .or_insert(*blob_size_bytes);
        }
    }

    let mut reports = Vec::new();
    for source in sources.into_values() {
        let version_count = source.snapshots.len();
        if version_count < 2 {
            continue;
        }
        let current_source_size_bytes = match std::fs::symlink_metadata(&source.original_path) {
            Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                metadata.len()
            }
            _ => continue,
        };
        let referenced_blob_bytes = source
            .blobs
            .values()
            .copied()
            .fold(0_u64, u64::saturating_add);
        let excess_bytes = referenced_blob_bytes.saturating_sub(current_source_size_bytes);
        let amplification_ratio_milli = (current_source_size_bytes > 0)
            .then(|| referenced_blob_bytes.saturating_mul(1_000) / current_source_size_bytes);
        let ratio_exceeded = amplification_ratio_milli
            .is_some_and(|ratio| ratio >= ratio_warn_threshold_milli)
            || (current_source_size_bytes == 0 && referenced_blob_bytes > 0);
        if !ratio_exceeded || excess_bytes < excess_warn_threshold_bytes {
            continue;
        }
        reports.push(DoctorRawMirrorSourceAmplificationReport {
            evidence_kind,
            providers: source.providers.into_iter().collect(),
            source_id: source.source_id,
            redacted_original_path: source.redacted_original_path,
            original_path_blake3: source.original_path_blake3,
            version_count,
            referenced_blob_bytes,
            current_source_size_bytes,
            excess_bytes,
            amplification_ratio_milli,
        });
    }
    reports.sort_by(|left, right| {
        right
            .excess_bytes
            .cmp(&left.excess_bytes)
            .then_with(|| left.providers.cmp(&right.providers))
            .then_with(|| left.source_id.cmp(&right.source_id))
            .then_with(|| left.original_path_blake3.cmp(&right.original_path_blake3))
    });
    reports
}

/// Read the cheap, integrity-bound subset needed for a storage-amplification
/// warning without hashing the entire raw blob. This is deliberately not a
/// repair-authority proof: callers may use it only for diagnostics, and the
/// serialized report labels the weaker evidence class explicitly.
pub(crate) fn doctor_raw_mirror_amplification_version_from_manifest_path(
    data_dir: &Path,
    root: &Path,
    manifest_path: &Path,
) -> Option<DoctorRawMirrorSourceAmplificationVersion> {
    let metadata = std::fs::symlink_metadata(manifest_path).ok()?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() > DOCTOR_RAW_MIRROR_AMPLIFICATION_MANIFEST_MAX_BYTES
        || path_has_symlink_below_root(manifest_path, root)
    {
        return None;
    }
    let manifest: DoctorRawMirrorManifestFile =
        serde_json::from_slice(&std::fs::read(manifest_path).ok()?).ok()?;
    if manifest.schema_version != DOCTOR_RAW_MIRROR_SCHEMA_VERSION
        || manifest.manifest_kind != DOCTOR_RAW_MIRROR_MANIFEST_KIND
        || manifest.blob_hash_algorithm != DOCTOR_RAW_MIRROR_HASH_ALGORITHM
    {
        return None;
    }

    let expected_blob_relative = doctor_raw_mirror_blob_relative_path(&manifest.blob_blake3)?;
    if manifest.blob_relative_path != expected_blob_relative {
        return None;
    }
    let relative_blob =
        doctor_raw_mirror_validate_relative_path(&manifest.blob_relative_path).ok()?;
    let blob_path = root.join(relative_blob);
    if blob_path
        .parent()
        .is_none_or(|parent| existing_path_has_symlink_below_root(parent, root))
    {
        return None;
    }
    let blob_metadata = std::fs::symlink_metadata(&blob_path).ok()?;
    if blob_metadata.file_type().is_symlink()
        || !blob_metadata.is_file()
        || blob_metadata.len() != manifest.blob_size_bytes
    {
        return None;
    }

    let expected_original_path_blake3 =
        doctor_raw_mirror_original_path_blake3(&manifest.original_path);
    if expected_original_path_blake3 != manifest.original_path_blake3 {
        return None;
    }
    let expected_manifest_id = doctor_raw_mirror_manifest_id(
        &manifest.provider,
        &manifest.source_id,
        &manifest.origin_kind,
        manifest.origin_host.as_deref(),
        &manifest.original_path_blake3,
        &manifest.blob_blake3,
    );
    if manifest.manifest_id != expected_manifest_id {
        return None;
    }
    let expected_manifest_relative =
        doctor_raw_mirror_manifest_relative_path(&manifest.manifest_id);
    if manifest_path.strip_prefix(root).ok()? != Path::new(&expected_manifest_relative) {
        return None;
    }
    let computed_manifest_blake3 = doctor_raw_mirror_manifest_blake3(&manifest);
    if manifest.manifest_blake3.as_deref() != Some(computed_manifest_blake3.as_str()) {
        return None;
    }
    let mut stored_blobs = vec![(manifest.blob_blake3.clone(), manifest.blob_size_bytes)];
    let expected_content_blake3 = if let Some(storage) = manifest.content_storage.as_ref() {
        if storage.get("kind").and_then(serde_json::Value::as_str) != Some("fixed_chunks_v1")
            || storage
                .get("content_hash_algorithm")
                .and_then(serde_json::Value::as_str)
                != Some(DOCTOR_RAW_MIRROR_HASH_ALGORITHM)
        {
            return None;
        }
        let content_blake3 = storage
            .get("content_blake3")
            .and_then(serde_json::Value::as_str)?;
        doctor_raw_mirror_blob_relative_path(content_blake3)?;
        let content_size_bytes = storage
            .get("content_size_bytes")
            .and_then(serde_json::Value::as_u64)?;
        let declared_chunk_size_bytes = storage
            .get("chunk_size_bytes")
            .and_then(serde_json::Value::as_u64)?;
        if declared_chunk_size_bytes == 0 {
            return None;
        }
        if manifest.source_size_bytes != Some(content_size_bytes) {
            return None;
        }
        let chunks = storage
            .get("chunks")
            .and_then(serde_json::Value::as_array)?;
        if chunks.is_empty() {
            return None;
        }
        let mut reconstructed_size_bytes = 0_u64;
        for (index, chunk) in chunks.iter().enumerate() {
            let chunk_blake3 = chunk
                .get("blob_blake3")
                .and_then(serde_json::Value::as_str)?;
            let chunk_relative_path = chunk
                .get("blob_relative_path")
                .and_then(serde_json::Value::as_str)?;
            let chunk_blob_size_bytes = chunk
                .get("blob_size_bytes")
                .and_then(serde_json::Value::as_u64)?;
            let is_last = index + 1 == chunks.len();
            if chunk_blob_size_bytes == 0
                || chunk_blob_size_bytes > declared_chunk_size_bytes
                || (!is_last && chunk_blob_size_bytes != declared_chunk_size_bytes)
            {
                return None;
            }
            reconstructed_size_bytes =
                reconstructed_size_bytes.checked_add(chunk_blob_size_bytes)?;
            if doctor_raw_mirror_blob_relative_path(chunk_blake3)?.as_str() != chunk_relative_path {
                return None;
            }
            let relative_chunk =
                doctor_raw_mirror_validate_relative_path(chunk_relative_path).ok()?;
            let chunk_path = root.join(relative_chunk);
            if chunk_path
                .parent()
                .is_none_or(|parent| existing_path_has_symlink_below_root(parent, root))
            {
                return None;
            }
            let chunk_metadata = std::fs::symlink_metadata(&chunk_path).ok()?;
            if chunk_metadata.file_type().is_symlink()
                || !chunk_metadata.is_file()
                || chunk_metadata.len() != chunk_blob_size_bytes
            {
                return None;
            }
            stored_blobs.push((chunk_blake3.to_string(), chunk_blob_size_bytes));
        }
        if reconstructed_size_bytes != content_size_bytes
            || manifest.verification.content_blake3.as_deref() != Some(content_blake3)
        {
            return None;
        }
        content_blake3
    } else {
        if manifest.source_size_bytes != Some(manifest.blob_size_bytes) {
            return None;
        }
        manifest.blob_blake3.as_str()
    };
    if manifest
        .verification
        .content_blake3
        .as_deref()
        .is_some_and(|verified| verified != expected_content_blake3)
    {
        return None;
    }

    Some(DoctorRawMirrorSourceAmplificationVersion {
        provider: doctor_normalized_provider_slug(&manifest.provider),
        source_id: manifest.source_id,
        origin_kind: manifest.origin_kind,
        origin_host: manifest.origin_host,
        original_path: manifest.original_path.clone(),
        redacted_original_path: doctor_redacted_path(&manifest.original_path, data_dir),
        original_path_blake3: manifest.original_path_blake3,
        snapshot_blake3: manifest.blob_blake3,
        stored_blobs,
    })
}

pub(crate) fn doctor_raw_mirror_bounded_amplification_reports(
    data_dir: &Path,
    root: &Path,
    manifest_entries: &[(PathBuf, bool)],
    inventory_truncated: bool,
    ratio_warn_threshold_milli: u64,
    excess_warn_threshold_bytes: u64,
) -> DoctorRawMirrorBoundedAmplificationScan {
    doctor_raw_mirror_bounded_amplification_reports_with_limits(
        data_dir,
        root,
        manifest_entries,
        inventory_truncated,
        ratio_warn_threshold_milli,
        excess_warn_threshold_bytes,
        DoctorRawMirrorBoundedScanLimits {
            manifest_count: DOCTOR_RAW_MIRROR_BOUNDED_AMPLIFICATION_MANIFEST_LIMIT,
            byte_count: DOCTOR_RAW_MIRROR_BOUNDED_AMPLIFICATION_BYTE_LIMIT,
        },
    )
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DoctorRawMirrorBoundedScanLimits {
    pub(crate) manifest_count: usize,
    pub(crate) byte_count: u64,
}

pub(crate) fn doctor_raw_mirror_bounded_amplification_reports_with_limits(
    data_dir: &Path,
    root: &Path,
    manifest_entries: &[(PathBuf, bool)],
    inventory_truncated: bool,
    ratio_warn_threshold_milli: u64,
    excess_warn_threshold_bytes: u64,
    limits: DoctorRawMirrorBoundedScanLimits,
) -> DoctorRawMirrorBoundedAmplificationScan {
    let mut versions = Vec::new();
    let mut scanned_manifest_count = 0_usize;
    let mut scanned_manifest_bytes = 0_u64;
    let mut truncated = inventory_truncated;
    for (path, is_symlink) in manifest_entries {
        if *is_symlink {
            continue;
        }
        let Ok(metadata) = std::fs::symlink_metadata(path) else {
            continue;
        };
        if !metadata.is_file()
            || metadata.len() > DOCTOR_RAW_MIRROR_AMPLIFICATION_MANIFEST_MAX_BYTES
        {
            continue;
        }
        let Some(next_scanned_bytes) = scanned_manifest_bytes.checked_add(metadata.len()) else {
            truncated = true;
            break;
        };
        if scanned_manifest_count >= limits.manifest_count || next_scanned_bytes > limits.byte_count
        {
            truncated = true;
            break;
        }
        scanned_manifest_count += 1;
        scanned_manifest_bytes = next_scanned_bytes;
        if let Some(version) =
            doctor_raw_mirror_amplification_version_from_manifest_path(data_dir, root, path)
        {
            versions.push(version);
        }
    }
    let reports = doctor_raw_mirror_source_amplification_reports_from_versions(
        &versions,
        "manifest_checksum_and_blob_metadata",
        ratio_warn_threshold_milli,
        excess_warn_threshold_bytes,
    );
    DoctorRawMirrorBoundedAmplificationScan {
        reports,
        scanned_manifest_count,
        truncated,
    }
}

pub(crate) fn doctor_raw_mirror_apply_source_amplification_reports(
    report: &mut DoctorRawMirrorReport,
    amplified_sources: Vec<DoctorRawMirrorSourceAmplificationReport>,
) {
    report.amplified_sources = amplified_sources;
    report.summary.amplified_source_count = report.amplified_sources.len();
    report.summary.amplified_source_referenced_bytes = report
        .amplified_sources
        .iter()
        .map(|source| source.referenced_blob_bytes)
        .fold(0_u64, u64::saturating_add);
    report.summary.amplified_source_excess_bytes = report
        .amplified_sources
        .iter()
        .map(|source| source.excess_bytes)
        .fold(0_u64, u64::saturating_add);
    report.summary.max_source_amplification_ratio_milli = report
        .amplified_sources
        .iter()
        .filter_map(|source| source.amplification_ratio_milli)
        .max();
    if let Some(worst) = report.amplified_sources.first() {
        let ratio = worst
            .amplification_ratio_milli
            .map(|ratio| format!("{:.1}x", ratio as f64 / 1_000.0))
            .unwrap_or_else(|| "unbounded (current source is empty)".to_string());
        report.warnings.push(format!(
            "raw_mirror.source_amplification: {} source path(s) materially exceed their current file size; evidence={} worst={} providers={:?} versions={} referenced_bytes={} current_bytes={} excess_bytes={} ratio={}. Review `amplified_sources` before applying any prune because an older version can be sole-copy evidence after provider rotation.",
            report.summary.amplified_source_count,
            worst.evidence_kind,
            worst.redacted_original_path,
            worst.providers,
            worst.version_count,
            worst.referenced_blob_bytes,
            worst.current_source_size_bytes,
            worst.excess_bytes,
            ratio,
        ));
    }
}

pub(crate) fn doctor_raw_mirror_full_verify_requested() -> bool {
    dotenvy::var(CASS_DOCTOR_RAW_MIRROR_FULL_VERIFY)
        .ok()
        .is_some_and(|raw| {
            matches!(
                raw.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
}

pub(crate) fn doctor_raw_mirror_full_verify_manifest_limit() -> usize {
    dotenvy::var(CASS_DOCTOR_RAW_MIRROR_FULL_VERIFY_MANIFEST_LIMIT)
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|&limit| limit > 0)
        .unwrap_or(DOCTOR_RAW_MIRROR_DEFAULT_FULL_VERIFY_MANIFEST_LIMIT)
}

pub(crate) fn doctor_raw_mirror_full_verify_byte_limit() -> u64 {
    dotenvy::var(CASS_DOCTOR_RAW_MIRROR_FULL_VERIFY_BYTE_LIMIT)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|&limit| limit > 0)
        .unwrap_or(DOCTOR_RAW_MIRROR_DEFAULT_FULL_VERIFY_BYTE_LIMIT)
}

pub(crate) fn doctor_raw_mirror_estimated_verification_bytes(
    manifest_entries: &[(PathBuf, bool)],
) -> u64 {
    let mut estimated_bytes = 0_u64;
    for (path, is_symlink) in manifest_entries {
        if *is_symlink {
            continue;
        }
        let Ok(metadata) = std::fs::symlink_metadata(path) else {
            continue;
        };
        if !metadata.is_file() || metadata.len() > DOCTOR_RAW_MIRROR_MANIFEST_MAX_BYTES {
            continue;
        }
        estimated_bytes = estimated_bytes.saturating_add(metadata.len());
        let Ok(bytes) = std::fs::read(path) else {
            continue;
        };
        let Ok(manifest) = serde_json::from_slice::<DoctorRawMirrorManifestFile>(&bytes) else {
            continue;
        };
        estimated_bytes = estimated_bytes.saturating_add(manifest.blob_size_bytes);
        if manifest.content_storage.is_some() {
            // Chunked verification reads the manifest a second time through the
            // raw-mirror validator. The descriptor is read three times total:
            // once by doctor, then once for its raw checksum and once for typed
            // descriptor parsing. Chunk bytes are streamed once in source order.
            estimated_bytes = estimated_bytes
                .saturating_add(metadata.len())
                .saturating_add(manifest.blob_size_bytes.saturating_mul(2))
                .saturating_add(manifest.source_size_bytes.unwrap_or_default());
        }
    }
    estimated_bytes
}

pub(crate) fn collect_doctor_raw_mirror_report_for_doctor(
    data_dir: &Path,
    mutating_repair_requested: bool,
) -> DoctorRawMirrorReport {
    let mode = if mutating_repair_requested || doctor_raw_mirror_full_verify_requested() {
        DoctorRawMirrorVerificationMode::Full
    } else {
        DoctorRawMirrorVerificationMode::Bounded {
            manifest_limit: doctor_raw_mirror_full_verify_manifest_limit(),
            byte_limit: doctor_raw_mirror_full_verify_byte_limit(),
        }
    };
    collect_doctor_raw_mirror_report_with_threshold_and_mode(
        data_dir,
        doctor_raw_mirror_size_warn_threshold_bytes(),
        mode,
    )
}

pub(crate) fn collect_doctor_raw_mirror_report(data_dir: &Path) -> DoctorRawMirrorReport {
    collect_doctor_raw_mirror_report_with_threshold_and_mode(
        data_dir,
        doctor_raw_mirror_size_warn_threshold_bytes(),
        DoctorRawMirrorVerificationMode::Full,
    )
}

#[cfg(test)]
pub(crate) fn collect_doctor_raw_mirror_report_with_threshold(
    data_dir: &Path,
    size_warn_threshold_bytes: u64,
) -> DoctorRawMirrorReport {
    collect_doctor_raw_mirror_report_with_threshold_and_mode(
        data_dir,
        size_warn_threshold_bytes,
        DoctorRawMirrorVerificationMode::Full,
    )
}

pub(crate) fn collect_doctor_raw_mirror_report_with_threshold_and_mode(
    data_dir: &Path,
    size_warn_threshold_bytes: u64,
    verification_mode: DoctorRawMirrorVerificationMode,
) -> DoctorRawMirrorReport {
    collect_doctor_raw_mirror_report_with_thresholds_and_mode(
        data_dir,
        size_warn_threshold_bytes,
        DOCTOR_RAW_MIRROR_SOURCE_AMPLIFICATION_WARN_RATIO_MILLI_DEFAULT,
        DOCTOR_RAW_MIRROR_SOURCE_AMPLIFICATION_WARN_EXCESS_BYTES_DEFAULT,
        verification_mode,
    )
}

#[cfg(test)]
pub(crate) fn collect_doctor_raw_mirror_report_with_amplification_thresholds(
    data_dir: &Path,
    ratio_warn_threshold_milli: u64,
    excess_warn_threshold_bytes: u64,
) -> DoctorRawMirrorReport {
    collect_doctor_raw_mirror_report_with_thresholds_and_mode(
        data_dir,
        0,
        ratio_warn_threshold_milli,
        excess_warn_threshold_bytes,
        DoctorRawMirrorVerificationMode::Full,
    )
}

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
