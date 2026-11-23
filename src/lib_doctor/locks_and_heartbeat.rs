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

pub(crate) fn doctor_mutation_lock_path(data_dir: &Path) -> PathBuf {
    data_dir
        .join("doctor")
        .join("locks")
        .join("doctor-repair.lock")
}

pub(crate) fn doctor_parse_lock_metadata(raw: &str) -> BTreeMap<String, String> {
    raw.lines()
        .filter_map(|line| {
            let (key, value) = line.split_once('=')?;
            let key = key.trim();
            if key.is_empty() {
                return None;
            }
            Some((key.to_string(), value.trim().to_string()))
        })
        .collect()
}

pub(crate) fn doctor_read_lock_metadata(file: &std::fs::File) -> BTreeMap<String, String> {
    use std::io::Read as _;

    const MAX_LOCK_FILE_READ: u64 = 64 * 1024;
    let mut raw = String::new();
    let _ = file.take(MAX_LOCK_FILE_READ).read_to_string(&mut raw);
    doctor_parse_lock_metadata(&raw)
}

pub(crate) fn doctor_lock_metadata_u32(
    metadata: &BTreeMap<String, String>,
    key: &str,
) -> Option<u32> {
    metadata.get(key).and_then(|value| value.parse().ok())
}

pub(crate) fn doctor_lock_metadata_i64(
    metadata: &BTreeMap<String, String>,
    key: &str,
) -> Option<i64> {
    metadata.get(key).and_then(|value| value.parse().ok())
}

pub(crate) fn doctor_redact_lock_command(
    command: Option<&String>,
    data_dir: &Path,
) -> Option<String> {
    let command = command?;
    let data_dir_text = data_dir.display().to_string();
    if data_dir_text.is_empty() {
        return Some(command.clone());
    }
    Some(command.replace(&data_dir_text, "[cass-data]"))
}

pub(crate) fn doctor_probe_mutation_lock(data_dir: &Path) -> DoctorMutationLockObservation {
    let path = doctor_mutation_lock_path(data_dir);
    if !path.exists() {
        return DoctorMutationLockObservation::Absent { path };
    }

    let file = match OpenOptions::new().read(true).write(true).open(&path) {
        Ok(file) => file,
        Err(err) => {
            return DoctorMutationLockObservation::Unavailable {
                path,
                reason: format!("failed to open doctor mutation lock for inspection: {err}"),
            };
        }
    };
    let metadata = doctor_read_lock_metadata(&file);
    match fs2::FileExt::try_lock_exclusive(&file) {
        Ok(()) => {
            let _ = fs2::FileExt::unlock(&file);
            DoctorMutationLockObservation::Available { path, metadata }
        }
        Err(err) if doctor_lock_probe_error_is_active(&err) => {
            DoctorMutationLockObservation::Active { path, metadata }
        }
        Err(err) => DoctorMutationLockObservation::Unavailable {
            path,
            reason: format!("failed to probe doctor mutation lock ownership: {err}"),
        },
    }
}

/// fs2 reports a contended try-lock as its `lock_contended_error()`:
/// EWOULDBLOCK on unix, but raw ERROR_LOCK_VIOLATION on Windows, which no
/// `ErrorKind` matches, so an active doctor lock read as unavailable
/// (2l1b0.74).
pub(crate) fn doctor_lock_probe_error_is_active(err: &std::io::Error) -> bool {
    err.kind() == std::io::ErrorKind::WouldBlock
        || (err.raw_os_error().is_some()
            && err.raw_os_error() == fs2::lock_contended_error().raw_os_error())
}

#[cfg(test)]
mod doctor_lock_probe_error_tests {
    use super::*;

    /// fs2's own contended error must read as an active lock on every
    /// platform (on Windows it is raw ERROR_LOCK_VIOLATION, not WouldBlock),
    /// and unrelated failures must not (2l1b0.74).
    #[test]
    fn contended_lock_error_is_active_and_other_errors_are_not() {
        assert!(doctor_lock_probe_error_is_active(
            &fs2::lock_contended_error()
        ));
        assert!(doctor_lock_probe_error_is_active(&std::io::Error::from(
            std::io::ErrorKind::WouldBlock
        )));
        assert!(!doctor_lock_probe_error_is_active(&std::io::Error::from(
            std::io::ErrorKind::NotFound
        )));
        assert!(!doctor_lock_probe_error_is_active(&std::io::Error::other(
            "permission denied"
        )));
    }
}

pub(crate) fn doctor_acquire_mutation_lock(
    data_dir: &Path,
    db_path: &Path,
) -> Result<(DoctorMutationLockGuard, DoctorMutationLockObservation), DoctorMutationLockObservation>
{
    let path = doctor_mutation_lock_path(data_dir);
    if let Some(parent) = path.parent()
        && let Err(err) = std::fs::create_dir_all(parent)
    {
        return Err(DoctorMutationLockObservation::Unavailable {
            path,
            reason: format!("failed to create doctor lock directory: {err}"),
        });
    }

    let mut file = match OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)
    {
        Ok(file) => file,
        Err(err) => {
            return Err(DoctorMutationLockObservation::Unavailable {
                path,
                reason: format!("failed to open doctor mutation lock: {err}"),
            });
        }
    };

    if let Err(err) = fs2::FileExt::try_lock_exclusive(&file) {
        let metadata = doctor_read_lock_metadata(&file);
        if doctor_lock_probe_error_is_active(&err) {
            return Err(DoctorMutationLockObservation::Active { path, metadata });
        }
        return Err(DoctorMutationLockObservation::Unavailable {
            path,
            reason: format!("failed to acquire doctor mutation lock: {err}"),
        });
    }

    let started_at_ms = doctor_now_ms();
    let mut metadata = BTreeMap::new();
    metadata.insert("schema_version".to_string(), "1".to_string());
    metadata.insert("pid".to_string(), std::process::id().to_string());
    metadata.insert("started_at_ms".to_string(), started_at_ms.to_string());
    metadata.insert("updated_at_ms".to_string(), started_at_ms.to_string());
    metadata.insert("db_path".to_string(), db_path.display().to_string());
    metadata.insert("mode".to_string(), "safe_auto_run".to_string());
    metadata.insert("command".to_string(), "cass doctor --fix".to_string());

    let encoded = metadata
        .iter()
        .map(|(key, value)| format!("{key}={value}\n"))
        .collect::<String>();
    if let Err(err) = file.set_len(0) {
        let _ = fs2::FileExt::unlock(&file);
        return Err(DoctorMutationLockObservation::Unavailable {
            path,
            reason: format!("failed to clear doctor mutation lock metadata: {err}"),
        });
    }
    if let Err(err) = file
        .write_all(encoded.as_bytes())
        .and_then(|_| file.sync_all())
    {
        let _ = fs2::FileExt::unlock(&file);
        return Err(DoctorMutationLockObservation::Unavailable {
            path,
            reason: format!("failed to write doctor mutation lock metadata: {err}"),
        });
    }

    Ok((
        DoctorMutationLockGuard { file },
        DoctorMutationLockObservation::Acquired { path, metadata },
    ))
}

pub(crate) fn doctor_acquire_existing_mutation_lock_without_metadata(
    data_dir: &Path,
) -> Result<(DoctorMutationLockGuard, DoctorMutationLockObservation), DoctorMutationLockObservation>
{
    let path = doctor_mutation_lock_path(data_dir);
    let file = match OpenOptions::new().read(true).write(true).open(&path) {
        Ok(file) => file,
        Err(err) => {
            return Err(DoctorMutationLockObservation::Unavailable {
                path,
                reason: format!("failed to open existing doctor mutation lock: {err}"),
            });
        }
    };

    if let Err(err) = fs2::FileExt::try_lock_exclusive(&file) {
        let metadata = doctor_read_lock_metadata(&file);
        if doctor_lock_probe_error_is_active(&err) {
            return Err(DoctorMutationLockObservation::Active { path, metadata });
        }
        return Err(DoctorMutationLockObservation::Unavailable {
            path,
            reason: format!("failed to acquire existing doctor mutation lock: {err}"),
        });
    }

    Ok((
        DoctorMutationLockGuard { file },
        DoctorMutationLockObservation::Acquired {
            path,
            metadata: BTreeMap::new(),
        },
    ))
}

pub(crate) fn doctor_operation_owner_from_maintenance(
    data_dir: &Path,
    db_path: &Path,
    snapshot: &crate::search::asset_state::SearchMaintenanceSnapshot,
) -> Option<DoctorOperationOwnerReport> {
    if !snapshot.active && !snapshot.orphaned {
        return None;
    }

    let lock_path = data_dir.join("index-run.lock");
    let pid = snapshot.pid;
    let started_at_ms = snapshot.started_at_ms;
    let updated_at_ms = snapshot.updated_at_ms;
    let mode = snapshot.mode.map(|mode| mode.as_lock_value().to_string());
    let job_kind = snapshot
        .job_kind
        .map(|kind| kind.as_lock_value().to_string());
    let db_path_text = snapshot
        .db_path
        .as_ref()
        .map(|path| path.display().to_string());
    let db_path_matches_requested = snapshot
        .db_path
        .as_deref()
        .map(|lock_db_path| path_identities_match(lock_db_path, db_path));
    let owned_by_current_process = pid == Some(std::process::id());
    let owner_confidence = if snapshot.active && owned_by_current_process {
        DoctorOperationOwnerConfidence::CurrentProcess
    } else if snapshot.active
        && (pid.is_some() || started_at_ms.is_some() || snapshot.db_path.is_some())
    {
        DoctorOperationOwnerConfidence::ActiveAdvisoryLock
    } else if snapshot.active {
        DoctorOperationOwnerConfidence::ActiveMissingMetadata
    } else {
        DoctorOperationOwnerConfidence::StaleMetadataOnly
    };
    let lock_kind = if snapshot
        .mode
        .is_some_and(crate::search::asset_state::SearchMaintenanceMode::watch_active)
    {
        DoctorOperationLockKind::WatchIngestion
    } else {
        DoctorOperationLockKind::IndexRun
    };
    let mut evidence = Vec::new();
    evidence.push("advisory-flock-state-from-index-run-lock".to_string());
    if snapshot.orphaned {
        evidence.push("metadata-without-active-owner".to_string());
    }
    if db_path_matches_requested == Some(false) {
        evidence.push("lock-db-path-differs-from-requested-db".to_string());
    }

    Some(DoctorOperationOwnerReport {
        lock_kind,
        lock_path: lock_path.display().to_string(),
        redacted_lock_path: doctor_redacted_path(&lock_path.display().to_string(), data_dir),
        active: snapshot.active,
        owned_by_current_process,
        owner_confidence,
        pid,
        owner_command: None,
        started_at_ms,
        started_at: started_at_ms.and_then(format_timestamp_millis_rfc3339),
        updated_at_ms,
        updated_at: updated_at_ms.and_then(format_timestamp_millis_rfc3339),
        mode,
        job_id: snapshot.job_id.clone(),
        job_kind,
        phase: snapshot.phase.clone(),
        db_path: db_path_text,
        db_path_matches_requested,
        evidence,
        next_action: if snapshot.active {
            "wait for the active index/watch owner to finish, then rerun cass doctor --json"
                .to_string()
        } else {
            "stale metadata is advisory only; do not delete evidence manually".to_string()
        },
    })
}

pub(crate) fn doctor_operation_owner_from_doctor_lock(
    data_dir: &Path,
    db_path: &Path,
    observation: &DoctorMutationLockObservation,
) -> Option<DoctorOperationOwnerReport> {
    let empty_metadata = BTreeMap::new();
    let (active, owned_by_current_process, owner_confidence, metadata, unavailable_reason) =
        match observation {
            DoctorMutationLockObservation::Absent { .. } => return None,
            DoctorMutationLockObservation::Available { metadata, .. } if metadata.is_empty() => {
                return None;
            }
            DoctorMutationLockObservation::Available { metadata, .. } => (
                false,
                false,
                DoctorOperationOwnerConfidence::StaleMetadataOnly,
                metadata,
                None,
            ),
            DoctorMutationLockObservation::Active { metadata, .. } => {
                let pid = doctor_lock_metadata_u32(metadata, "pid");
                let owned_by_current_process = pid == Some(std::process::id());
                let confidence = if metadata.is_empty() {
                    DoctorOperationOwnerConfidence::ActiveMissingMetadata
                } else {
                    DoctorOperationOwnerConfidence::ActiveAdvisoryLock
                };
                (true, owned_by_current_process, confidence, metadata, None)
            }
            DoctorMutationLockObservation::Acquired { metadata, .. } => (
                true,
                true,
                DoctorOperationOwnerConfidence::CurrentProcess,
                metadata,
                None,
            ),
            DoctorMutationLockObservation::Unavailable { reason, .. } => (
                false,
                false,
                DoctorOperationOwnerConfidence::Unavailable,
                &empty_metadata,
                Some(reason.as_str()),
            ),
        };

    let lock_path = observation.path();
    let pid = doctor_lock_metadata_u32(metadata, "pid");
    let started_at_ms = doctor_lock_metadata_i64(metadata, "started_at_ms");
    let updated_at_ms = doctor_lock_metadata_i64(metadata, "updated_at_ms");
    let lock_db_path = metadata.get("db_path").cloned();
    let db_path_matches_requested = lock_db_path
        .as_deref()
        .map(|lock_db_path| path_identities_match(Path::new(lock_db_path), db_path));
    let mut evidence = Vec::new();
    if let Some(reason) = unavailable_reason {
        evidence.push(format!("lock-unavailable={reason}"));
    } else if active {
        evidence.push("doctor-mutation-lock-held".to_string());
    } else {
        evidence.push("doctor-mutation-lock-metadata-without-owner".to_string());
    }
    if metadata.get("schema_version").is_none() && unavailable_reason.is_none() {
        evidence.push("missing-schema-version".to_string());
    }

    Some(DoctorOperationOwnerReport {
        lock_kind: DoctorOperationLockKind::DoctorRepair,
        lock_path: lock_path.display().to_string(),
        redacted_lock_path: doctor_redacted_path(&lock_path.display().to_string(), data_dir),
        active,
        owned_by_current_process,
        owner_confidence,
        pid,
        owner_command: doctor_redact_lock_command(metadata.get("command"), data_dir),
        started_at_ms,
        started_at: started_at_ms.and_then(format_timestamp_millis_rfc3339),
        updated_at_ms,
        updated_at: updated_at_ms.and_then(format_timestamp_millis_rfc3339),
        mode: metadata.get("mode").cloned(),
        job_id: metadata.get("job_id").cloned(),
        job_kind: metadata.get("job_kind").cloned(),
        phase: metadata.get("phase").cloned(),
        db_path: lock_db_path,
        db_path_matches_requested,
        evidence,
        next_action: if active && !owned_by_current_process {
            "wait for the active cass doctor --fix owner to finish, then rerun cass doctor --json"
                .to_string()
        } else if unavailable_reason.is_some() {
            "inspect lock path permissions before attempting a mutating doctor repair".to_string()
        } else if active {
            "current doctor process owns the mutation lock".to_string()
        } else {
            "stale doctor lock metadata is advisory; do not delete it — cass doctor --fix \
             reclaims it with a receipt under doctor/reclaimed/ once the dead owner's \
             heartbeat exceeds the self-heal bound"
                .to_string()
        },
    })
}

pub(crate) fn doctor_interrupted_kind_for_path(path: &Path) -> DoctorInterruptedOperationKind {
    let text = path.to_string_lossy().to_ascii_lowercase();
    if text.contains("promot") {
        DoctorInterruptedOperationKind::Promotion
    } else if text.contains("restore") {
        DoctorInterruptedOperationKind::Restore
    } else if text.contains("cleanup") {
        DoctorInterruptedOperationKind::Cleanup
    } else if text.contains("backup") || text.contains("verify") {
        DoctorInterruptedOperationKind::BackupVerification
    } else if text.contains("receipt") {
        DoctorInterruptedOperationKind::ReceiptIncomplete
    } else if text.contains("event") {
        DoctorInterruptedOperationKind::EventLogIncomplete
    } else if text.contains("raw-mirror") || text.contains("raw_mirror") {
        DoctorInterruptedOperationKind::RawMirrorCapture
    } else if text.ends_with(".lock") {
        DoctorInterruptedOperationKind::StaleLock
    } else if text.contains("candidate") || text.contains("interrupted-repair") {
        DoctorInterruptedOperationKind::CandidateBuild
    } else {
        DoctorInterruptedOperationKind::Unknown
    }
}

pub(crate) fn doctor_interrupted_report_for_path(
    data_dir: &Path,
    path: &Path,
    kind: DoctorInterruptedOperationKind,
    disposition: DoctorInterruptedOperationDisposition,
    evidence: Vec<String>,
) -> DoctorInterruptedOperationReport {
    let blocks_mutation = true;
    let next_action = match disposition {
        DoctorInterruptedOperationDisposition::NeedsInspection => {
            "inspect interrupted doctor artifacts and receipts before running cass doctor --fix"
        }
        DoctorInterruptedOperationDisposition::RecoverOnNextIndexRun => {
            "run cass status --json or cass index --full to let lexical publish recovery finalize"
        }
    };
    DoctorInterruptedOperationReport {
        kind,
        path: path.display().to_string(),
        redacted_path: doctor_redacted_path(&path.display().to_string(), data_dir),
        disposition,
        blocks_mutation,
        safe_to_delete_automatically: false,
        evidence,
        next_action: next_action.to_string(),
    }
}

pub(crate) fn doctor_push_interrupted_scan_entries(
    reports: &mut Vec<DoctorInterruptedOperationReport>,
    data_dir: &Path,
    root: &Path,
    evidence_label: &str,
) {
    const MAX_INTERRUPTED_OPERATION_REPORTS: usize = 64;
    if reports.len() >= MAX_INTERRUPTED_OPERATION_REPORTS || !root.exists() {
        return;
    }

    for entry in walkdir::WalkDir::new(root).max_depth(4).follow_links(false) {
        if reports.len() >= MAX_INTERRUPTED_OPERATION_REPORTS {
            break;
        }
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        if entry.path() == root {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_ascii_lowercase();
        let suspicious = entry.file_type().is_dir()
            || name.contains("interrupted")
            || name.contains("incomplete")
            || name.contains("partial")
            || name.contains("pending")
            || name.ends_with(".tmp")
            || name.ends_with(".lock");
        if !suspicious {
            continue;
        }
        let kind = doctor_interrupted_kind_for_path(entry.path());
        let mut report = doctor_interrupted_report_for_path(
            data_dir,
            entry.path(),
            kind,
            DoctorInterruptedOperationDisposition::NeedsInspection,
            vec![evidence_label.to_string()],
        );
        if doctor_interrupted_artifact_is_quarantinable(data_dir, entry.path()) {
            report.next_action = DOCTOR_CLEANUP_INTERRUPTED_ARTIFACTS_NEXT_ACTION.to_string();
        }
        reports.push(report);
    }
}

/// The staging root `cass doctor --cleanup-interrupted-artifacts --yes`
/// quarantines (every child, renamed under `doctor/quarantine/`, never deleted).
pub(crate) fn doctor_raw_mirror_capture_staging_root(data_dir: &Path) -> PathBuf {
    data_dir.join("raw-mirror").join("v1").join("tmp")
}

/// GH #497: whether the supported cleanup command clears this interrupted
/// artifact, so doctor can name that command instead of asking for manual
/// inspection. Decided by location, not by the path's words, because the
/// cleanup command moves exactly the children of the staging root.
pub(crate) fn doctor_interrupted_artifact_is_quarantinable(data_dir: &Path, path: &Path) -> bool {
    path.starts_with(doctor_raw_mirror_capture_staging_root(data_dir))
}

pub(crate) const DOCTOR_CLEANUP_INTERRUPTED_ARTIFACTS_NEXT_ACTION: &str = "run cass doctor --cleanup-interrupted-artifacts --yes to quarantine interrupted raw-mirror capture staging (moved under doctor/quarantine/interrupted-artifacts, never deleted), then rerun cass doctor --json";

pub(crate) fn doctor_lexical_publish_in_progress_backup_path(index_path: &Path) -> Option<PathBuf> {
    let file_name = index_path.file_name()?.to_string_lossy();
    Some(index_path.with_file_name(format!(".{file_name}.publish-in-progress.bak")))
}

pub(crate) fn collect_doctor_interrupted_operation_states(
    data_dir: &Path,
    index_path: &Path,
) -> Vec<DoctorInterruptedOperationReport> {
    let mut reports = Vec::new();
    doctor_push_interrupted_scan_entries(
        &mut reports,
        data_dir,
        &data_dir.join("doctor").join("tmp"),
        "doctor-tmp-artifact",
    );
    doctor_push_interrupted_scan_entries(
        &mut reports,
        data_dir,
        &data_dir.join("doctor").join("receipts"),
        "doctor-receipt-artifact",
    );
    doctor_push_interrupted_scan_entries(
        &mut reports,
        data_dir,
        &data_dir.join("doctor").join("events"),
        "doctor-event-artifact",
    );
    doctor_push_interrupted_scan_entries(
        &mut reports,
        data_dir,
        &doctor_raw_mirror_capture_staging_root(data_dir),
        "raw-mirror-interrupted-capture",
    );
    doctor_push_interrupted_scan_entries(
        &mut reports,
        data_dir,
        &data_dir.join("locks"),
        "legacy-doctor-lock-artifact",
    );

    if let Some(sidecar) = doctor_lexical_publish_in_progress_backup_path(index_path)
        && sidecar.exists()
    {
        reports.push(doctor_interrupted_report_for_path(
            data_dir,
            &sidecar,
            DoctorInterruptedOperationKind::ParkedPublishBackup,
            DoctorInterruptedOperationDisposition::RecoverOnNextIndexRun,
            vec!["lexical-publish-in-progress-backup".to_string()],
        ));
    }

    reports.sort_by(|left, right| left.path.cmp(&right.path));
    reports
}

pub(crate) fn build_doctor_operation_state_report(
    data_dir: &Path,
    db_path: &Path,
    index_path: &Path,
    maintenance_snapshot: &crate::search::asset_state::SearchMaintenanceSnapshot,
    doctor_lock: &DoctorMutationLockObservation,
) -> DoctorOperationStateReport {
    let mut owners = Vec::new();
    if let Some(owner) =
        doctor_operation_owner_from_maintenance(data_dir, db_path, maintenance_snapshot)
    {
        owners.push(owner);
    }
    if let Some(owner) = doctor_operation_owner_from_doctor_lock(data_dir, db_path, doctor_lock) {
        owners.push(owner);
    }

    let interrupted_states = collect_doctor_interrupted_operation_states(data_dir, index_path);
    let maintenance_targets_current_db = maintenance_snapshot
        .db_path
        .as_ref()
        .is_none_or(|lock_db_path| crate::path_identities_match(lock_db_path, db_path));
    let active_index_maintenance = maintenance_snapshot.active && maintenance_targets_current_db;
    let active_rebuild = maintenance_snapshot.active
        && maintenance_targets_current_db
        && maintenance_snapshot
            .mode
            .map(crate::search::asset_state::SearchMaintenanceMode::rebuild_active)
            .unwrap_or(true);
    let active_watch = maintenance_snapshot.active
        && maintenance_targets_current_db
        && maintenance_snapshot
            .mode
            .is_some_and(crate::search::asset_state::SearchMaintenanceMode::watch_active);
    let active_doctor_repair = matches!(
        doctor_lock,
        DoctorMutationLockObservation::Active { .. }
            | DoctorMutationLockObservation::Acquired { .. }
    );
    let external_doctor_repair_active =
        matches!(doctor_lock, DoctorMutationLockObservation::Active { .. });
    let stale_owner_count = owners
        .iter()
        .filter(|owner| owner.owner_confidence == DoctorOperationOwnerConfidence::StaleMetadataOnly)
        .count();
    let interrupted_blocker_count = interrupted_states
        .iter()
        .filter(|state| state.blocks_mutation)
        .count();
    let quarantinable_blocker_count = interrupted_states
        .iter()
        .filter(|state| {
            state.blocks_mutation
                && doctor_interrupted_artifact_is_quarantinable(data_dir, Path::new(&state.path))
        })
        .count();
    let mutation_blocked_reason = if active_index_maintenance {
        Some("active index/watch maintenance lock blocks mutating doctor repair".to_string())
    } else if external_doctor_repair_active {
        Some("another cass doctor --fix owner holds the mutation lock".to_string())
    } else if doctor_lock.blocks_mutation() {
        Some("doctor mutation lock could not be inspected or acquired safely".to_string())
    } else if interrupted_blocker_count > 0 {
        Some(format!(
            "{interrupted_blocker_count} interrupted doctor artifact(s) require inspection before mutation"
        ))
    } else {
        None
    };
    let mutating_doctor_allowed = mutation_blocked_reason.is_none();
    let next_action = match mutation_blocked_reason.as_deref() {
        Some(reason) if reason.contains("index/watch") => {
            "wait for cass status --json to report rebuild.active=false, then rerun cass doctor --json"
        }
        Some(reason) if reason.contains("another cass doctor") => {
            "wait for the active cass doctor --fix process to finish, then rerun cass doctor --json"
        }
        // GH #497: name the supported command that clears the blocker when
        // one exists, rather than only asking for inspection.
        Some(reason)
            if reason.contains("interrupted")
                && quarantinable_blocker_count == interrupted_blocker_count =>
        {
            DOCTOR_CLEANUP_INTERRUPTED_ARTIFACTS_NEXT_ACTION
        }
        Some(reason) if reason.contains("interrupted") && quarantinable_blocker_count > 0 => {
            "run cass doctor --cleanup-interrupted-artifacts --yes to quarantine the interrupted raw-mirror capture staging, then inspect the remaining operation_state.interrupted_states before running cass doctor --fix"
        }
        Some(reason) if reason.contains("interrupted") => {
            "inspect operation_state.interrupted_states before running cass doctor --fix"
        }
        Some(_) => "inspect operation_state.owners before running cass doctor --fix",
        None => "mutating doctor repair is allowed by the current lock and interrupted-state model",
    };

    DoctorOperationStateReport {
        schema_version: 1,
        read_only_check_allowed: true,
        mutating_doctor_allowed,
        active_index_maintenance,
        active_rebuild,
        active_watch,
        active_doctor_repair,
        owner_count: owners.len(),
        stale_owner_count,
        interrupted_state_count: interrupted_states.len(),
        mutation_blocked_reason,
        owners,
        interrupted_states,
        next_action: next_action.to_string(),
        notes: vec![
            "Read-only doctor inspection remains allowed while repair/index owners are active.".to_string(),
            "PID and timestamp metadata are advisory; advisory lock ownership and receipts are the authority.".to_string(),
            "Interrupted artifacts are never deleted by this state model; they block mutation until inspected.".to_string(),
        ],
    }
}

pub(crate) fn doctor_elapsed_since_ms(now_ms: i64, timestamp_ms: Option<i64>) -> Option<u64> {
    let timestamp_ms = timestamp_ms?;
    Some(now_ms.saturating_sub(timestamp_ms).max(0) as u64)
}

pub(crate) fn doctor_lock_pid_metadata_status(owner: &DoctorOperationOwnerReport) -> String {
    if owner
        .evidence
        .iter()
        .any(|entry| entry.to_ascii_lowercase().contains("permission denied"))
    {
        return "permission-denied".to_string();
    }
    if owner.pid.is_none() {
        return "pid-missing".to_string();
    }
    if owner.owned_by_current_process {
        return "current-process".to_string();
    }
    match owner.owner_confidence {
        DoctorOperationOwnerConfidence::ActiveAdvisoryLock => {
            "not-queried-pid-reuse-ambiguous".to_string()
        }
        DoctorOperationOwnerConfidence::ActiveMissingMetadata => "metadata-missing".to_string(),
        DoctorOperationOwnerConfidence::CurrentProcess => "current-process".to_string(),
        DoctorOperationOwnerConfidence::StaleMetadataOnly => "metadata-only".to_string(),
        DoctorOperationOwnerConfidence::Unavailable => "unavailable".to_string(),
    }
}

pub(crate) fn doctor_lock_pid_reuse_ambiguous(owner: &DoctorOperationOwnerReport) -> bool {
    owner.pid.is_some()
        && !owner.owned_by_current_process
        && matches!(
            owner.owner_confidence,
            DoctorOperationOwnerConfidence::ActiveAdvisoryLock
                | DoctorOperationOwnerConfidence::StaleMetadataOnly
        )
}

pub(crate) fn doctor_lock_stale_suspected(
    owner: &DoctorOperationOwnerReport,
    last_heartbeat_age_ms: Option<u64>,
) -> Option<bool> {
    match owner.owner_confidence {
        DoctorOperationOwnerConfidence::StaleMetadataOnly => Some(true),
        DoctorOperationOwnerConfidence::Unavailable
        | DoctorOperationOwnerConfidence::ActiveMissingMetadata => None,
        DoctorOperationOwnerConfidence::CurrentProcess
        | DoctorOperationOwnerConfidence::ActiveAdvisoryLock => Some(
            last_heartbeat_age_ms.is_some_and(|age| age > DOCTOR_LOCK_HEARTBEAT_STALE_AFTER_MS),
        ),
    }
}

pub(crate) fn doctor_lock_retry_policy(
    owner: &DoctorOperationOwnerReport,
) -> (&'static str, Option<u64>) {
    match owner.owner_confidence {
        DoctorOperationOwnerConfidence::CurrentProcess => ("current-process-owned", None),
        DoctorOperationOwnerConfidence::ActiveAdvisoryLock
        | DoctorOperationOwnerConfidence::ActiveMissingMetadata => ("wait-and-retry", Some(30_000)),
        DoctorOperationOwnerConfidence::StaleMetadataOnly => ("inspect-receipts", None),
        DoctorOperationOwnerConfidence::Unavailable => ("inspect-permissions", None),
    }
}

pub(crate) fn build_doctor_lock_diagnostics(
    operation_state: &DoctorOperationStateReport,
    now_ms: i64,
) -> Vec<DoctorLockDiagnosticReport> {
    operation_state
        .owners
        .iter()
        .map(|owner| {
            let age_ms = doctor_elapsed_since_ms(now_ms, owner.started_at_ms);
            let last_heartbeat_age_ms = doctor_elapsed_since_ms(now_ms, owner.updated_at_ms);
            let stale_suspected = doctor_lock_stale_suspected(owner, last_heartbeat_age_ms);
            let (retry_policy, retry_after_ms) = doctor_lock_retry_policy(owner);
            let mut evidence = owner.evidence.clone();
            if owner.owner_command.is_some() {
                doctor_add_unique_string(&mut evidence, "owner-command-from-lock-metadata");
            }
            if owner.pid.is_none() {
                doctor_add_unique_string(&mut evidence, "pid-missing");
            }
            if doctor_lock_pid_reuse_ambiguous(owner) {
                doctor_add_unique_string(&mut evidence, "pid-reuse-ambiguous");
            }
            if last_heartbeat_age_ms.is_some_and(|age| age > DOCTOR_LOCK_HEARTBEAT_STALE_AFTER_MS) {
                doctor_add_unique_string(&mut evidence, "heartbeat-older-than-stale-threshold");
            }

            DoctorLockDiagnosticReport {
                schema_version: 1,
                lock_kind: owner.lock_kind,
                lock_path: owner.lock_path.clone(),
                redacted_lock_path: owner.redacted_lock_path.clone(),
                active: owner.active,
                owned_by_current_process: owner.owned_by_current_process,
                owner_confidence: owner.owner_confidence,
                pid: owner.pid,
                owner_command: owner.owner_command.clone(),
                pid_metadata_status: doctor_lock_pid_metadata_status(owner),
                pid_reuse_ambiguous: doctor_lock_pid_reuse_ambiguous(owner),
                age_ms,
                last_heartbeat_age_ms,
                stale_suspected,
                wait_duration_ms: retry_after_ms,
                retry_policy: retry_policy.to_string(),
                safe_to_wait: matches!(retry_policy, "wait-and-retry" | "current-process-owned"),
                manual_delete_allowed: false,
                recommended_action: owner.next_action.clone(),
                evidence,
            }
        })
        .collect()
}

/// siekg: last doctor phase that completed, read by the stderr liveness
/// heartbeat so a multi-minute run says *where* it is, not just that it is
/// alive. Process-global on purpose: doctor runs once per process and the
/// value is advisory message text only.
pub(crate) static DOCTOR_HEARTBEAT_LAST_PHASE: Mutex<String> = Mutex::new(String::new());

/// Default liveness cadence for long doctor runs; `CASS_DOCTOR_HEARTBEAT_SECS`
/// overrides it (`0` disables).
pub(crate) const DOCTOR_HEARTBEAT_DEFAULT_SECS: u64 = 15;

pub(crate) fn doctor_heartbeat_interval() -> Option<std::time::Duration> {
    doctor_heartbeat_interval_from(std::env::var("CASS_DOCTOR_HEARTBEAT_SECS").ok().as_deref())
}

/// Pure resolver for the heartbeat cadence: unset/unparseable → default,
/// `0` → disabled (`None`).
pub(crate) fn doctor_heartbeat_interval_from(raw: Option<&str>) -> Option<std::time::Duration> {
    let secs = raw
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(DOCTOR_HEARTBEAT_DEFAULT_SECS);
    (secs > 0).then(|| std::time::Duration::from_secs(secs))
}

#[cfg(test)]
mod doctor_heartbeat_tests {
    use super::{DOCTOR_HEARTBEAT_DEFAULT_SECS, doctor_heartbeat_interval_from};
    use std::time::Duration;

    #[test]
    fn unset_or_garbage_uses_default_cadence() {
        let default = Duration::from_secs(DOCTOR_HEARTBEAT_DEFAULT_SECS);
        assert!(doctor_heartbeat_interval_from(None).is_some_and(|d| d.cmp(&default).is_eq()));
        assert!(
            doctor_heartbeat_interval_from(Some("nope")).is_some_and(|d| d.cmp(&default).is_eq())
        );
    }

    #[test]
    fn zero_disables_and_positive_overrides() {
        assert!(doctor_heartbeat_interval_from(Some("0")).is_none());
        assert!(
            doctor_heartbeat_interval_from(Some(" 3 "))
                .is_some_and(|d| d.as_secs().cmp(&3).is_eq())
        );
    }
}

/// siekg (bead `doctor-check-no-progress-output-siekg`) + k2k20 ask #1: `cass doctor --check`
/// legitimately runs for 3–16 minutes on multi-GB or corrupt archives and
/// used to emit *nothing* until it finished, so with stdout redirected it was
/// indistinguishable from a hang and operators killed it. This guard prints a
/// one-line heartbeat to stderr (the diagnostics channel in every output
/// mode; stdout stays data-only) at a fixed cadence while the run is live,
/// naming elapsed time and the last completed phase. First tick fires only
/// after one full interval, so quick runs stay byte-identical on stderr.
pub(crate) struct CliHeartbeat {
    /// Dropping the sender wakes the thread immediately, so retiring the
    /// heartbeat never adds latency to a fast `status`/`doctor` exit.
    pub(crate) stop: Option<std::sync::mpsc::Sender<()>>,
    pub(crate) handle: Option<std::thread::JoinHandle<()>>,
}

impl CliHeartbeat {
    pub(crate) fn start(surface: &str) -> Option<Self> {
        let interval = doctor_heartbeat_interval()?;
        if let Ok(mut last) = DOCTOR_HEARTBEAT_LAST_PHASE.lock() {
            last.clear();
        }
        let (stop, wake) = std::sync::mpsc::channel::<()>();
        let surface = surface.to_string();
        let handle = std::thread::Builder::new()
            .name("cass-doctor-heartbeat".to_string())
            .spawn(move || {
                let started = std::time::Instant::now();
                let mut next = interval;
                loop {
                    // Sleep until the next tick or until the owner drops the
                    // sender (disconnect), whichever comes first.
                    let remaining = next.saturating_sub(started.elapsed());
                    match wake.recv_timeout(remaining) {
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                        Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                    }
                    let elapsed = started.elapsed();
                    if elapsed < next {
                        continue;
                    }
                    next += interval;
                    let last_phase = DOCTOR_HEARTBEAT_LAST_PHASE
                        .lock()
                        .map(|p| p.clone())
                        .unwrap_or_default();
                    let phase_note = if last_phase.is_empty() {
                        String::new()
                    } else {
                        format!("; last completed phase: {last_phase}")
                    };
                    eprintln!(
                        "[cass {surface}] still running ({}s elapsed{phase_note}). Multi-minute runs are normal on multi-GB or corrupt archives; set CASS_DOCTOR_HEARTBEAT_SECS=0 to silence.",
                        elapsed.as_secs()
                    );
                }
            })
            .ok()?;
        Some(Self {
            stop: Some(stop),
            handle: Some(handle),
        })
    }
}

impl Drop for CliHeartbeat {
    fn drop(&mut self) {
        // Disconnect first so a sleeping thread wakes at once, then join.
        drop(self.stop.take());
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

pub(crate) fn doctor_push_timing_span(
    spans: &mut Vec<DoctorTimingSpanReport>,
    name: &str,
    phase: &str,
    source: &str,
    started: std::time::Instant,
    threshold_ms: u64,
    notes: Vec<String>,
) {
    if let Ok(mut last) = DOCTOR_HEARTBEAT_LAST_PHASE.lock() {
        last.clear();
        last.push_str(name);
    }
    let elapsed_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
    spans.push(DoctorTimingSpanReport {
        schema_version: 1,
        name: name.to_string(),
        phase: phase.to_string(),
        source: source.to_string(),
        elapsed_ms,
        threshold_ms,
        slow: elapsed_ms > threshold_ms,
        event_log_correlation_id: format!("doctor_timing:{name}"),
        notes,
    });
}

pub(crate) fn build_doctor_slow_operations(
    spans: &[DoctorTimingSpanReport],
) -> Vec<DoctorSlowOperationReport> {
    let mut slow = spans
        .iter()
        .filter(|span| span.slow)
        .map(|span| DoctorSlowOperationReport {
            schema_version: 1,
            name: span.name.clone(),
            phase: span.phase.clone(),
            elapsed_ms: span.elapsed_ms,
            threshold_ms: span.threshold_ms,
            severity: if span.elapsed_ms > span.threshold_ms.saturating_mul(5) {
                "warn".to_string()
            } else {
                "info".to_string()
            },
            event_log_correlation_id: span.event_log_correlation_id.clone(),
            recommended_action: format!(
                "Inspect doctor timing span `{}` and rerun with CASS_TRACE_FILE=<path> if it remains slow.",
                span.name
            ),
        })
        .collect::<Vec<_>>();
    slow.sort_by(|left, right| {
        right
            .elapsed_ms
            .cmp(&left.elapsed_ms)
            .then_with(|| left.name.cmp(&right.name))
    });
    slow
}

pub(crate) fn build_doctor_timing_summary(
    spans: &[DoctorTimingSpanReport],
    elapsed_ms: u64,
) -> DoctorTimingSummaryReport {
    let slow_operation_count = spans.iter().filter(|span| span.slow).count();
    let slowest = spans
        .iter()
        .max_by(|left, right| left.elapsed_ms.cmp(&right.elapsed_ms));
    DoctorTimingSummaryReport {
        schema_version: 1,
        elapsed_ms,
        measured_operation_count: spans.len(),
        slow_operation_count,
        default_threshold_ms: DOCTOR_SLOW_OPERATION_DEFAULT_THRESHOLD_MS,
        slowest_operation: slowest.map(|span| span.name.clone()),
        slowest_elapsed_ms: slowest.map(|span| span.elapsed_ms),
        notes: vec![
            "Timing spans are advisory wall-clock measurements for the current doctor process."
                .to_string(),
            "Health/status should consume cached summaries rather than rerunning expensive doctor collectors."
                .to_string(),
        ],
    }
}

pub(crate) fn build_doctor_retry_recommendation(
    operation_state: &DoctorOperationStateReport,
    operation_outcome: &DoctorOperationOutcomeReport,
    locks: &[DoctorLockDiagnosticReport],
    recommended_action: Option<&String>,
) -> DoctorRetryRecommendationReport {
    if let Some(lock) = locks.iter().find(|lock| {
        lock.active || lock.owner_confidence == DoctorOperationOwnerConfidence::Unavailable
    }) {
        return DoctorRetryRecommendationReport {
            schema_version: 1,
            policy: lock.retry_policy.clone(),
            safe_to_retry: true,
            retry_after_ms: lock.wait_duration_ms,
            command: Some("cass doctor --json".to_string()),
            reason: lock.recommended_action.clone(),
            notes: vec![
                format!("lock_kind={}", doctor_serde_label(lock.lock_kind)),
                "Do not delete lock files manually; use doctor/status output and receipts as evidence."
                    .to_string(),
            ],
        };
    }

    if operation_state.interrupted_state_count > 0 {
        return DoctorRetryRecommendationReport {
            schema_version: 1,
            policy: "inspect-interrupted-state".to_string(),
            safe_to_retry: false,
            retry_after_ms: None,
            command: Some("cass doctor --json".to_string()),
            reason: operation_state.next_action.clone(),
            notes: vec![
                "Interrupted artifacts are evidence and are not automatically deleted.".to_string(),
            ],
        };
    }

    DoctorRetryRecommendationReport {
        schema_version: 1,
        policy: if operation_outcome.safe_to_retry {
            "retry-allowed"
        } else {
            "no-retry-needed"
        }
        .to_string(),
        safe_to_retry: operation_outcome.safe_to_retry,
        retry_after_ms: None,
        command: operation_outcome
            .next_command
            .clone()
            .or_else(|| recommended_action.cloned()),
        reason: operation_outcome.reason.clone(),
        notes: vec!["No active doctor/index lock owner was reported in this payload.".to_string()],
    }
}
