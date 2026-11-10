use crate::doctor::policy::types::{DoctorAssetClass, DoctorAssetSafetyClassification};
use crate::doctor::report::storage::DoctorFullRebuildReadinessReport;
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DoctorOperationLockKind {
    IndexRun,
    WatchIngestion,
    DoctorRepair,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DoctorOperationOwnerConfidence {
    ActiveAdvisoryLock,
    ActiveMissingMetadata,
    CurrentProcess,
    StaleMetadataOnly,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DoctorInterruptedOperationKind {
    CandidateBuild,
    Promotion,
    Restore,
    Cleanup,
    BackupVerification,
    ReceiptIncomplete,
    EventLogIncomplete,
    ParkedPublishBackup,
    RawMirrorCapture,
    StaleLock,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DoctorInterruptedOperationDisposition {
    NeedsInspection,
    RecoverOnNextIndexRun,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorOperationOwnerReport {
    pub(crate) lock_kind: DoctorOperationLockKind,
    pub(crate) lock_path: String,
    pub(crate) redacted_lock_path: String,
    pub(crate) active: bool,
    pub(crate) owned_by_current_process: bool,
    pub(crate) owner_confidence: DoctorOperationOwnerConfidence,
    pub(crate) pid: Option<u32>,
    pub(crate) owner_command: Option<String>,
    pub(crate) started_at_ms: Option<i64>,
    pub(crate) started_at: Option<String>,
    pub(crate) updated_at_ms: Option<i64>,
    pub(crate) updated_at: Option<String>,
    pub(crate) mode: Option<String>,
    pub(crate) job_id: Option<String>,
    pub(crate) job_kind: Option<String>,
    pub(crate) phase: Option<String>,
    pub(crate) db_path: Option<String>,
    pub(crate) db_path_matches_requested: Option<bool>,
    pub(crate) evidence: Vec<String>,
    pub(crate) next_action: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorLockDiagnosticReport {
    pub(crate) schema_version: u32,
    pub(crate) lock_kind: DoctorOperationLockKind,
    pub(crate) lock_path: String,
    pub(crate) redacted_lock_path: String,
    pub(crate) active: bool,
    pub(crate) owned_by_current_process: bool,
    pub(crate) owner_confidence: DoctorOperationOwnerConfidence,
    pub(crate) pid: Option<u32>,
    pub(crate) owner_command: Option<String>,
    pub(crate) pid_metadata_status: String,
    pub(crate) pid_reuse_ambiguous: bool,
    pub(crate) age_ms: Option<u64>,
    pub(crate) last_heartbeat_age_ms: Option<u64>,
    pub(crate) stale_suspected: Option<bool>,
    pub(crate) wait_duration_ms: Option<u64>,
    pub(crate) retry_policy: String,
    pub(crate) safe_to_wait: bool,
    pub(crate) manual_delete_allowed: bool,
    pub(crate) recommended_action: String,
    pub(crate) evidence: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorTimingSpanReport {
    pub(crate) schema_version: u32,
    pub(crate) name: String,
    pub(crate) phase: String,
    pub(crate) source: String,
    pub(crate) elapsed_ms: u64,
    pub(crate) threshold_ms: u64,
    pub(crate) slow: bool,
    pub(crate) event_log_correlation_id: String,
    pub(crate) notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorSlowOperationReport {
    pub(crate) schema_version: u32,
    pub(crate) name: String,
    pub(crate) phase: String,
    pub(crate) elapsed_ms: u64,
    pub(crate) threshold_ms: u64,
    pub(crate) severity: String,
    pub(crate) event_log_correlation_id: String,
    pub(crate) recommended_action: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorTimingSummaryReport {
    pub(crate) schema_version: u32,
    pub(crate) elapsed_ms: u64,
    pub(crate) measured_operation_count: usize,
    pub(crate) slow_operation_count: usize,
    pub(crate) default_threshold_ms: u64,
    pub(crate) slowest_operation: Option<String>,
    pub(crate) slowest_elapsed_ms: Option<u64>,
    pub(crate) notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorRetryRecommendationReport {
    pub(crate) schema_version: u32,
    pub(crate) policy: String,
    pub(crate) safe_to_retry: bool,
    pub(crate) retry_after_ms: Option<u64>,
    pub(crate) command: Option<String>,
    pub(crate) reason: String,
    pub(crate) notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorInterruptedOperationReport {
    pub(crate) kind: DoctorInterruptedOperationKind,
    pub(crate) path: String,
    pub(crate) redacted_path: String,
    pub(crate) disposition: DoctorInterruptedOperationDisposition,
    pub(crate) blocks_mutation: bool,
    pub(crate) safe_to_delete_automatically: bool,
    pub(crate) evidence: Vec<String>,
    pub(crate) next_action: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorOperationStateReport {
    pub(crate) schema_version: u32,
    pub(crate) read_only_check_allowed: bool,
    pub(crate) mutating_doctor_allowed: bool,
    pub(crate) active_index_maintenance: bool,
    pub(crate) active_rebuild: bool,
    pub(crate) active_watch: bool,
    pub(crate) active_doctor_repair: bool,
    pub(crate) owner_count: usize,
    pub(crate) stale_owner_count: usize,
    pub(crate) interrupted_state_count: usize,
    pub(crate) mutation_blocked_reason: Option<String>,
    pub(crate) owners: Vec<DoctorOperationOwnerReport>,
    pub(crate) interrupted_states: Vec<DoctorInterruptedOperationReport>,
    pub(crate) next_action: String,
    pub(crate) notes: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct DoctorMutationLockGuard {
    pub(crate) file: std::fs::File,
}

impl Drop for DoctorMutationLockGuard {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.file);
    }
}

#[derive(Debug, Clone)]
pub(crate) enum DoctorMutationLockObservation {
    Absent {
        path: PathBuf,
    },
    Available {
        path: PathBuf,
        metadata: BTreeMap<String, String>,
    },
    Active {
        path: PathBuf,
        metadata: BTreeMap<String, String>,
    },
    Acquired {
        path: PathBuf,
        metadata: BTreeMap<String, String>,
    },
    Unavailable {
        path: PathBuf,
        reason: String,
    },
}

impl DoctorMutationLockObservation {
    pub(crate) fn path(&self) -> &Path {
        match self {
            Self::Absent { path }
            | Self::Available { path, .. }
            | Self::Active { path, .. }
            | Self::Acquired { path, .. }
            | Self::Unavailable { path, .. } => path,
        }
    }

    pub(crate) fn blocks_mutation(&self) -> bool {
        matches!(self, Self::Active { .. } | Self::Unavailable { .. })
    }
}

pub(crate) const DOCTOR_STORAGE_MIN_FREE_BYTES: u64 = 1024 * 1024 * 1024;

pub(crate) const CASS_TEST_DOCTOR_STORAGE_AVAILABLE_BYTES: &str =
    "CASS_TEST_DOCTOR_STORAGE_AVAILABLE_BYTES";

pub(crate) const CASS_RAW_MIRROR_SIZE_WARN_THRESHOLD_BYTES: &str =
    "CASS_RAW_MIRROR_SIZE_WARN_THRESHOLD_BYTES";

pub(crate) const DOCTOR_RAW_MIRROR_SIZE_WARN_THRESHOLD_BYTES_DEFAULT: u64 = 100 * 1_000_000_000;

pub(crate) const DOCTOR_RAW_MIRROR_SOURCE_AMPLIFICATION_WARN_RATIO_MILLI_DEFAULT: u64 = 4_000;

pub(crate) const DOCTOR_RAW_MIRROR_SOURCE_AMPLIFICATION_WARN_EXCESS_BYTES_DEFAULT: u64 =
    256 * 1024 * 1024;

pub(crate) const DOCTOR_RAW_MIRROR_MANIFEST_MAX_BYTES: u64 = 16 * 1024 * 1024;

pub(crate) const DOCTOR_RAW_MIRROR_AMPLIFICATION_MANIFEST_MAX_BYTES: u64 = 1024 * 1024;

pub(crate) const DOCTOR_RAW_MIRROR_BOUNDED_AMPLIFICATION_MANIFEST_LIMIT: usize = 4_096;

pub(crate) const DOCTOR_RAW_MIRROR_BOUNDED_AMPLIFICATION_BYTE_LIMIT: u64 = 64 * 1024 * 1024;

pub(crate) const CASS_TEST_DOCTOR_CANDIDATE_PROMOTION_FAILPOINT: &str =
    "CASS_TEST_DOCTOR_CANDIDATE_PROMOTION_FAILPOINT";

pub(crate) const CASS_TEST_DOCTOR_RENAME_FAILURE: &str = "CASS_TEST_DOCTOR_RENAME_FAILURE";

pub(crate) const DOCTOR_SLOW_OPERATION_DEFAULT_THRESHOLD_MS: u64 = 500;

/// World-class-doctor pass-4: lowered from 1 hour to 5 minutes per the
/// safety-envelope hardening pass, so a stale lock stops blocking repairs
/// after 5 minutes instead of an hour. While the holder is live, a second
/// `cass doctor --fix` reports repair-blocked (exit 7 `index-busy` when failed
/// checks remain).
///
/// The stale-detection threshold is the lock-metadata `started_at_ms` /
/// `updated_at_ms` age in ms.
///
/// **Pass-11 fix:** an earlier docstring claimed
/// `CASS_DOCTOR_LOCK_HEARTBEAT_STALE_AFTER_MS` could be set as an env var
/// override. That env var was never wired in code — it was false advertising.
/// The capabilities surface previously echoed the same false claim; both have
/// been corrected. Future passes may wire an env override; for now this is a
/// compile-time constant.
pub(crate) const DOCTOR_LOCK_HEARTBEAT_STALE_AFTER_MS: u64 = 5 * 60 * 1000;

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorStoragePressureReport {
    pub(crate) schema_version: u32,
    pub(crate) status: String,
    pub(crate) data_dir_exists: bool,
    pub(crate) probe_path: String,
    pub(crate) available_bytes: Option<u64>,
    pub(crate) min_recommended_free_bytes: u64,
    pub(crate) low_disk_risk: String,
    /// GH#442: whether an authoritative rebuild (`cass index --full` /
    /// `--force-rebuild`) would pass the indexer's disk-headroom preflight,
    /// computed by the indexer's own rule against the same probe paths.
    pub(crate) full_rebuild_readiness: DoctorFullRebuildReadinessReport,
    pub(crate) total_accounted_bytes: u64,
    pub(crate) reclaimable_derived_bytes: u64,
    pub(crate) precious_evidence_bytes: u64,
    pub(crate) protected_non_reclaimable_bytes: u64,
    pub(crate) unknown_bytes: u64,
    pub(crate) reclaimable_bytes_by_class: BTreeMap<String, u64>,
    pub(crate) precious_bytes_by_class: BTreeMap<String, u64>,
    pub(crate) protected_bytes_by_class: BTreeMap<String, u64>,
    pub(crate) bytes_by_class: BTreeMap<String, DoctorStorageAssetClassBreakdown>,
    pub(crate) safe_cleanup_scope: Vec<String>,
    pub(crate) precious_evidence_classes: Vec<String>,
    pub(crate) classification_warnings: Vec<String>,
    pub(crate) recommended_action: String,
    pub(crate) notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorStorageAssetClassBreakdown {
    pub(crate) bytes: u64,
    pub(crate) file_count: usize,
    pub(crate) safety_classification: DoctorAssetSafetyClassification,
    pub(crate) derived: bool,
    pub(crate) precious: bool,
    pub(crate) reclaimable: bool,
    pub(crate) example_paths: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct DoctorConfigExclusionRiskReport {
    pub(crate) schema_version: u32,
    pub(crate) path: String,
    pub(crate) risk_kind: String,
    pub(crate) evidence: Vec<String>,
    pub(crate) confidence: String,
    pub(crate) recommended_action: String,
    pub(crate) auto_fix_available: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct DoctorConfigExclusionTarget {
    pub(crate) path: PathBuf,
    pub(crate) asset_class: DoctorAssetClass,
    pub(crate) label: &'static str,
    pub(crate) tokens: &'static [&'static str],
}

#[derive(Debug, Clone)]
pub(crate) struct DoctorConfigPatternMatch {
    pub(crate) config_path: PathBuf,
    pub(crate) line_number: usize,
    pub(crate) pattern: String,
    pub(crate) include: bool,
}
