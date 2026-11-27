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
#[allow(unused_imports)]
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, Clone)]
pub(crate) struct DoctorSourceInventoryDbRow {
    pub(crate) provider: String,
    pub(crate) source_path: Option<String>,
    pub(crate) source_id: String,
    pub(crate) origin_host: Option<String>,
    pub(crate) origin_kind: Option<String>,
    pub(crate) conversation_count: usize,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct DoctorSourceInventoryReport {
    pub(crate) schema_version: u32,
    pub(crate) db_available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) db_query_error: Option<String>,
    pub(crate) total_indexed_conversations: usize,
    pub(crate) provider_counts: BTreeMap<String, usize>,
    pub(crate) missing_current_source_count: usize,
    pub(crate) unknown_mapping_count: usize,
    pub(crate) remote_source_count: usize,
    pub(crate) local_source_count: usize,
    pub(crate) detected_provider_root_count: usize,
    pub(crate) providers: Vec<DoctorProviderSourceInventory>,
    pub(crate) sources: Vec<DoctorSourceIdentityInventory>,
    pub(crate) detected_roots: Vec<DoctorDetectedProviderRoot>,
    pub(crate) notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct DoctorProviderSourceInventory {
    pub(crate) provider: String,
    pub(crate) stable_source_id: String,
    pub(crate) indexed_conversation_count: usize,
    pub(crate) missing_current_source_count: usize,
    pub(crate) unknown_mapping_count: usize,
    pub(crate) remote_source_count: usize,
    pub(crate) local_source_count: usize,
    pub(crate) detected_root_count: usize,
    pub(crate) detected_roots: Vec<DoctorDetectedProviderRoot>,
    pub(crate) prune_risk: DoctorProviderPruneRisk,
    pub(crate) notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct DoctorProviderPruneRisk {
    pub(crate) level: &'static str,
    pub(crate) note: &'static str,
}

impl Default for DoctorProviderPruneRisk {
    fn default() -> Self {
        doctor_provider_prune_risk("unknown")
    }
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct DoctorSourceIdentityInventory {
    pub(crate) stable_source_id: String,
    pub(crate) source_id: String,
    pub(crate) origin_kind: String,
    pub(crate) origin_host: Option<String>,
    pub(crate) is_remote: bool,
    pub(crate) providers: Vec<String>,
    pub(crate) conversation_count: usize,
    pub(crate) missing_current_source_count: usize,
    pub(crate) unknown_mapping_count: usize,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct DoctorDetectedProviderRoot {
    pub(crate) provider: String,
    pub(crate) stable_source_id: String,
    pub(crate) path: String,
    pub(crate) redacted_path: String,
    pub(crate) exists: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct DoctorRawMirrorBackfillCandidate {
    pub(crate) conversation_id: i64,
    pub(crate) provider: String,
    pub(crate) source_path: Option<String>,
    pub(crate) source_id: String,
    pub(crate) origin_host: Option<String>,
    pub(crate) origin_kind: Option<String>,
    pub(crate) started_at_ms: Option<i64>,
    pub(crate) message_count: usize,
}

/// Maximum number of per-record doctor details emitted by default on robot
/// surfaces. The full collections remain in memory for coverage/risk decisions;
/// only the serialized projection is capped. `--verbose` removes this output
/// cap for operators who explicitly need the complete evidence listing.
pub(crate) const DOCTOR_STRUCTURED_DETAIL_DEFAULT_LIMIT: usize = 64;

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct DoctorRawMirrorBackfillReport {
    pub(crate) schema_version: u32,
    pub(crate) backfill_generation: u32,
    pub(crate) mode: String,
    pub(crate) status: String,
    pub(crate) db_available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) db_query_error: Option<String>,
    pub(crate) total_candidate_count: usize,
    pub(crate) already_raw_source_captured_count: usize,
    pub(crate) eligible_live_source_count: usize,
    pub(crate) existing_raw_manifest_link_count: usize,
    pub(crate) captured_live_source_count: usize,
    pub(crate) already_present_capture_count: usize,
    pub(crate) source_missing_count: usize,
    pub(crate) db_projection_only_count: usize,
    pub(crate) remote_or_unknown_mapping_count: usize,
    pub(crate) changed_source_hash_count: usize,
    pub(crate) capture_failure_count: usize,
    pub(crate) external_source_mutation_count: usize,
    pub(crate) read_only_external_source_dirs: bool,
    pub(crate) forensic_bundle: DoctorForensicBundleMetadata,
    pub(crate) receipts: Vec<DoctorRawMirrorBackfillReceipt>,
    pub(crate) warnings: Vec<String>,
    pub(crate) notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct DoctorRawMirrorBackfillReceipt {
    pub(crate) stable_record_id: String,
    pub(crate) conversation_id: Option<i64>,
    pub(crate) provider: String,
    pub(crate) source_id: String,
    pub(crate) origin_kind: String,
    pub(crate) origin_host: Option<String>,
    pub(crate) message_count: usize,
    pub(crate) started_at_ms: Option<i64>,
    pub(crate) redacted_source_path: Option<String>,
    pub(crate) source_path_blake3: Option<String>,
    pub(crate) action: String,
    pub(crate) raw_source_captured: bool,
    pub(crate) raw_mirror_db_linked: bool,
    pub(crate) source_missing: bool,
    pub(crate) db_projection_only: bool,
    pub(crate) parse_loss_unknown: bool,
    pub(crate) captured_at_ms: Option<i64>,
    pub(crate) source_stat_snapshot: Option<DoctorRawMirrorBackfillSourceStatSnapshot>,
    pub(crate) raw_mirror_manifest_id: Option<String>,
    pub(crate) raw_mirror_manifest_relative_path: Option<String>,
    pub(crate) raw_mirror_blob_blake3: Option<String>,
    pub(crate) raw_mirror_blob_size_bytes: Option<u64>,
    pub(crate) raw_mirror_source_content_blake3: Option<String>,
    pub(crate) raw_mirror_source_size_bytes: Option<u64>,
    pub(crate) raw_mirror_storage_kind: Option<String>,
    pub(crate) raw_mirror_chunk_count: Option<usize>,
    pub(crate) backfill_generation: u32,
    pub(crate) forensic_bundle: DoctorForensicBundleMetadata,
    pub(crate) warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct DoctorRawMirrorBackfillSourceStatSnapshot {
    pub(crate) exists: bool,
    pub(crate) file_type: String,
    pub(crate) size_bytes: Option<u64>,
    pub(crate) modified_at_ms: Option<i64>,
    pub(crate) content_blake3: Option<String>,
    pub(crate) stat_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Default)]
pub(crate) struct DoctorCandidateLiveInventory {
    pub(crate) db_exists: bool,
    pub(crate) db_size_bytes: u64,
    pub(crate) db_blake3: Option<String>,
    pub(crate) db_wal_exists: bool,
    pub(crate) db_wal_size_bytes: u64,
    pub(crate) db_wal_blake3: Option<String>,
    pub(crate) db_shm_exists: bool,
    pub(crate) db_shm_size_bytes: u64,
    pub(crate) db_shm_blake3: Option<String>,
    pub(crate) index_exists: bool,
    pub(crate) index_size_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorCandidateArtifact {
    pub(crate) artifact_kind: String,
    pub(crate) asset_class: DoctorAssetClass,
    pub(crate) relative_path: String,
    pub(crate) path: String,
    pub(crate) redacted_path: String,
    pub(crate) exists: bool,
    pub(crate) size_bytes: u64,
    pub(crate) checksum_blake3: Option<String>,
    pub(crate) checksum_status: DoctorArtifactChecksumStatus,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct DoctorCandidateManifest {
    pub(crate) schema_version: u32,
    pub(crate) manifest_kind: String,
    pub(crate) candidate_id: String,
    pub(crate) lifecycle_status: String,
    pub(crate) created_at_ms: i64,
    pub(crate) updated_at_ms: i64,
    pub(crate) operation_id: String,
    pub(crate) staging_root: String,
    pub(crate) redacted_staging_root: String,
    pub(crate) manifest_path: String,
    pub(crate) redacted_manifest_path: String,
    pub(crate) selected_authority: String,
    pub(crate) selected_authority_decision: String,
    pub(crate) selected_authority_evidence: Vec<String>,
    pub(crate) evidence_sources: Vec<String>,
    pub(crate) coverage_before: DoctorCandidateCoverageSnapshot,
    pub(crate) coverage_after: DoctorCandidateCoverageSnapshot,
    pub(crate) confidence: String,
    pub(crate) skipped_record_log: String,
    pub(crate) parse_error_log: String,
    pub(crate) artifact_count: usize,
    pub(crate) checksum_set: BTreeMap<String, String>,
    pub(crate) artifacts: Vec<DoctorCandidateArtifact>,
    pub(crate) coverage_gate: DoctorCoverageComparisonGateReport,
    pub(crate) live_inventory_before: DoctorCandidateLiveInventory,
    pub(crate) live_inventory_after: DoctorCandidateLiveInventory,
    pub(crate) live_inventory_unchanged: bool,
    pub(crate) notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct DoctorCandidateCoverageSnapshot {
    pub(crate) coverage_source: String,
    pub(crate) conversation_count: Option<usize>,
    pub(crate) message_count: Option<usize>,
    pub(crate) raw_mirror_manifest_count: usize,
    pub(crate) raw_mirror_db_link_count: usize,
    pub(crate) missing_current_source_count: usize,
    pub(crate) confidence_tier: String,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct DoctorCandidateSummary {
    pub(crate) candidate_id: String,
    pub(crate) lifecycle_status: String,
    pub(crate) path: String,
    pub(crate) redacted_path: String,
    pub(crate) manifest_path: Option<String>,
    pub(crate) redacted_manifest_path: Option<String>,
    pub(crate) manifest_blake3: Option<String>,
    pub(crate) artifact_count: usize,
    pub(crate) checksum_count: usize,
    pub(crate) selected_authority: Option<String>,
    pub(crate) created_at_ms: Option<i64>,
    pub(crate) updated_at_ms: Option<i64>,
    pub(crate) orphaned: bool,
    pub(crate) interrupted: bool,
    pub(crate) safe_to_delete_automatically: bool,
    pub(crate) evidence: Vec<String>,
    pub(crate) next_action: String,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct DoctorCandidateBuildReport {
    pub(crate) requested: bool,
    pub(crate) status: String,
    pub(crate) candidate_id: Option<String>,
    pub(crate) path: Option<String>,
    pub(crate) redacted_path: Option<String>,
    pub(crate) manifest_path: Option<String>,
    pub(crate) redacted_manifest_path: Option<String>,
    pub(crate) manifest_blake3: Option<String>,
    pub(crate) selected_authority: String,
    pub(crate) selected_authority_decision: String,
    pub(crate) selected_authority_evidence: Vec<String>,
    pub(crate) evidence_sources: Vec<String>,
    pub(crate) coverage_before: DoctorCandidateCoverageSnapshot,
    pub(crate) coverage_after: DoctorCandidateCoverageSnapshot,
    pub(crate) confidence: String,
    pub(crate) artifact_count: usize,
    pub(crate) checksum_count: usize,
    pub(crate) skipped_record_count: usize,
    pub(crate) parse_error_count: usize,
    pub(crate) frankensqlite_open_ok: bool,
    pub(crate) frankensqlite_write_ok: bool,
    pub(crate) candidate_conversation_count: Option<usize>,
    pub(crate) candidate_message_count: Option<usize>,
    pub(crate) coverage_gate: DoctorCoverageComparisonGateReport,
    pub(crate) live_inventory_before: DoctorCandidateLiveInventory,
    pub(crate) live_inventory_after: DoctorCandidateLiveInventory,
    pub(crate) live_inventory_unchanged: bool,
    pub(crate) fs_mutation_receipts: Vec<DoctorFsMutationReceipt>,
    pub(crate) warnings: Vec<String>,
    pub(crate) notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct DoctorCandidateStagingReport {
    pub(crate) schema_version: u32,
    pub(crate) status: String,
    pub(crate) root_path: String,
    pub(crate) redacted_root_path: String,
    pub(crate) exists: bool,
    pub(crate) total_candidate_count: usize,
    pub(crate) completed_candidate_count: usize,
    pub(crate) in_progress_candidate_count: usize,
    pub(crate) interrupted_candidate_count: usize,
    pub(crate) blocked_candidate_count: usize,
    pub(crate) orphaned_candidate_count: usize,
    pub(crate) latest_build: Option<DoctorCandidateBuildReport>,
    pub(crate) candidates: Vec<DoctorCandidateSummary>,
    pub(crate) warnings: Vec<String>,
    pub(crate) notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorCandidatePromotionArtifact {
    pub(crate) artifact_kind: String,
    pub(crate) asset_class: DoctorAssetClass,
    pub(crate) source_path: Option<String>,
    pub(crate) redacted_source_path: Option<String>,
    pub(crate) backup_path: Option<String>,
    pub(crate) redacted_backup_path: Option<String>,
    pub(crate) target_path: String,
    pub(crate) redacted_target_path: String,
    pub(crate) size_bytes: u64,
    pub(crate) checksum_blake3: Option<String>,
    pub(crate) copied_to_backup: bool,
    pub(crate) promoted_to_live: bool,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct DoctorCandidatePromotionReport {
    pub(crate) schema_version: u32,
    pub(crate) promotion_id: String,
    pub(crate) candidate_id: String,
    pub(crate) requested: bool,
    pub(crate) status: String,
    pub(crate) candidate_path: String,
    pub(crate) redacted_candidate_path: String,
    pub(crate) manifest_path: String,
    pub(crate) redacted_manifest_path: String,
    pub(crate) backup_dir: String,
    pub(crate) redacted_backup_dir: String,
    pub(crate) backup_manifest_path: Option<String>,
    pub(crate) redacted_backup_manifest_path: Option<String>,
    pub(crate) receipt_path: String,
    pub(crate) redacted_receipt_path: String,
    pub(crate) promotion_marker_path: Option<String>,
    pub(crate) redacted_promotion_marker_path: Option<String>,
    pub(crate) promotion_marker_blake3: Option<String>,
    pub(crate) plan_fingerprint: String,
    pub(crate) coverage_gate_status: String,
    pub(crate) coverage_promote_allowed: bool,
    pub(crate) live_inventory_before: DoctorCandidateLiveInventory,
    pub(crate) expected_live_inventory: DoctorCandidateLiveInventory,
    pub(crate) live_inventory_after: DoctorCandidateLiveInventory,
    pub(crate) sidecar_bundle_complete: bool,
    pub(crate) reader_consistency_guarantee: String,
    pub(crate) derived_assets_consistency_status: String,
    pub(crate) derived_lexical_rebuild_required: bool,
    pub(crate) derived_semantic_rebuild_required: bool,
    pub(crate) derived_lexical_followup_status: String,
    pub(crate) derived_semantic_followup_status: String,
    pub(crate) derived_vector_followup_status: String,
    pub(crate) derived_memo_followup_status: String,
    pub(crate) derived_followup_artifact_path: Option<String>,
    pub(crate) redacted_derived_followup_artifact_path: Option<String>,
    pub(crate) derived_followup_artifact_blake3: Option<String>,
    pub(crate) candidate_lexical_metadata_path: Option<String>,
    pub(crate) redacted_candidate_lexical_metadata_path: Option<String>,
    pub(crate) candidate_lexical_metadata_blake3: Option<String>,
    pub(crate) candidate_semantic_metadata_path: Option<String>,
    pub(crate) redacted_candidate_semantic_metadata_path: Option<String>,
    pub(crate) candidate_semantic_metadata_blake3: Option<String>,
    pub(crate) backup_manifest_blake3: Option<String>,
    pub(crate) rollback_reference: Option<String>,
    pub(crate) rollback_applied: bool,
    pub(crate) event_log: DoctorEventLogMetadata,
    pub(crate) artifacts: Vec<DoctorCandidatePromotionArtifact>,
    pub(crate) fs_mutation_receipts: Vec<DoctorFsMutationReceipt>,
    pub(crate) blocked_reasons: Vec<String>,
    pub(crate) warnings: Vec<String>,
    pub(crate) notes: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct DoctorRawMirrorExistingEvidence {
    pub(crate) manifest_id: String,
    pub(crate) manifest_relative_path: String,
    pub(crate) blob_blake3: String,
    pub(crate) blob_size_bytes: u64,
    pub(crate) source_content_blake3: String,
    pub(crate) source_size_bytes: Option<u64>,
    pub(crate) storage_kind: String,
    pub(crate) chunk_count: usize,
    pub(crate) captured_at_ms: i64,
    pub(crate) source_mtime_ms: Option<i64>,
    pub(crate) db_linked: bool,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct DoctorCoverageSummary {
    pub(crate) schema_version: u32,
    pub(crate) confidence_tier: String,
    pub(crate) archive_conversation_count: usize,
    pub(crate) archived_message_count: usize,
    pub(crate) provider_count: usize,
    pub(crate) source_identity_count: usize,
    pub(crate) visible_current_source_count: usize,
    pub(crate) visible_current_source_bytes: u64,
    pub(crate) raw_mirror_manifest_count: usize,
    pub(crate) raw_mirror_db_link_count: usize,
    pub(crate) db_without_raw_mirror_count: usize,
    pub(crate) db_projection_only_count: usize,
    pub(crate) mirror_without_db_link_count: usize,
    /// Unlinked verified manifests whose exact source bytes, at the same path
    /// and source identity, are DB-linked through another manifest: a second
    /// capture under a different provider slug (f3grf). Not counted in
    /// `mirror_without_db_link_count`.
    pub(crate) mirror_alias_of_linked_count: usize,
    pub(crate) missing_current_source_count: usize,
    pub(crate) sole_copy_candidate_count: usize,
    pub(crate) current_source_newer_than_archive_count: usize,
    pub(crate) remote_source_count: usize,
    pub(crate) unknown_mapping_count: usize,
    pub(crate) earliest_started_at_ms: Option<i64>,
    pub(crate) latest_started_at_ms: Option<i64>,
    pub(crate) coverage_reducing_live_source_rebuild_refused: bool,
    pub(crate) recommended_action: String,
    pub(crate) notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct DoctorCoverageComparisonGateReport {
    pub(crate) schema_version: u32,
    pub(crate) status: String,
    pub(crate) promote_allowed: bool,
    pub(crate) safe_to_inspect: bool,
    pub(crate) confidence_tier: String,
    pub(crate) selected_authority: String,
    pub(crate) selected_authority_decision: String,
    pub(crate) archive_conversation_count: usize,
    pub(crate) candidate_conversation_count: Option<usize>,
    pub(crate) conversation_delta: Option<i64>,
    pub(crate) archived_message_count: usize,
    pub(crate) candidate_message_count: Option<usize>,
    pub(crate) message_delta: Option<i64>,
    pub(crate) candidate_lexical_document_count: Option<usize>,
    pub(crate) lexical_document_delta: Option<i64>,
    pub(crate) candidate_semantic_vector_count: Option<usize>,
    pub(crate) semantic_vector_delta: Option<i64>,
    pub(crate) provider_count: usize,
    pub(crate) source_identity_count: usize,
    pub(crate) visible_current_source_count: usize,
    pub(crate) raw_mirror_db_link_count: usize,
    pub(crate) missing_current_source_count: usize,
    pub(crate) db_without_raw_mirror_count: usize,
    pub(crate) db_projection_only_count: usize,
    pub(crate) mirror_without_db_link_count: usize,
    pub(crate) sole_copy_candidate_count: usize,
    pub(crate) current_source_newer_than_archive_count: usize,
    pub(crate) earliest_started_at_ms: Option<i64>,
    pub(crate) latest_started_at_ms: Option<i64>,
    pub(crate) blocking_reasons: Vec<String>,
    pub(crate) warning_reasons: Vec<String>,
    pub(crate) evidence: Vec<String>,
    pub(crate) notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct DoctorSoleCopyWarning {
    pub(crate) stable_warning_id: String,
    pub(crate) conversation_id: Option<i64>,
    pub(crate) provider: String,
    pub(crate) source_id: String,
    pub(crate) origin_kind: String,
    pub(crate) origin_host: Option<String>,
    pub(crate) redacted_source_path: Option<String>,
    pub(crate) source_path_blake3: Option<String>,
    pub(crate) message_count: usize,
    pub(crate) raw_source_captured: bool,
    pub(crate) db_projection_only: bool,
    pub(crate) confidence_tier: String,
    pub(crate) reason: String,
    pub(crate) recommended_action: String,
}

pub(crate) fn bounded_doctor_structured_details(
    raw_mirror_backfill: &mut DoctorRawMirrorBackfillReport,
    sole_copy_warnings: &mut Vec<DoctorSoleCopyWarning>,
    verbose: bool,
    full_details_command: &str,
) -> serde_json::Value {
    let receipt_total = raw_mirror_backfill.receipts.len();
    let warning_total = sole_copy_warnings.len();

    if !verbose {
        raw_mirror_backfill
            .receipts
            .truncate(DOCTOR_STRUCTURED_DETAIL_DEFAULT_LIMIT);
        sole_copy_warnings.truncate(DOCTOR_STRUCTURED_DETAIL_DEFAULT_LIMIT);
    }

    let receipt_returned = raw_mirror_backfill.receipts.len();
    let warning_returned = sole_copy_warnings.len();
    let details_truncated = receipt_returned < receipt_total || warning_returned < warning_total;
    serde_json::json!({
        "mode": if verbose { "full_verbose" } else { "bounded_summary" },
        "limit_per_collection": if verbose {
            serde_json::Value::Null
        } else {
            serde_json::json!(DOCTOR_STRUCTURED_DETAIL_DEFAULT_LIMIT)
        },
        "details_truncated": details_truncated,
        "raw_mirror_backfill_receipts_total": receipt_total,
        "raw_mirror_backfill_receipts_returned": receipt_returned,
        "raw_mirror_backfill_receipts_omitted": receipt_total.saturating_sub(receipt_returned),
        "sole_copy_warnings_total": warning_total,
        "sole_copy_warnings_returned": warning_returned,
        "sole_copy_warnings_omitted": warning_total.saturating_sub(warning_returned),
        "full_details_command": details_truncated.then_some(full_details_command),
        "note": "Counts and safety decisions use the complete internal collections; only per-record robot details are capped unless --verbose is requested.",
    })
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct DoctorCoverageRiskSummary {
    pub(crate) schema_version: u32,
    pub(crate) status: String,
    pub(crate) confidence_tier: String,
    pub(crate) archive_conversation_count: usize,
    pub(crate) missing_current_source_count: usize,
    pub(crate) db_without_raw_mirror_count: usize,
    pub(crate) db_projection_only_count: usize,
    pub(crate) mirror_without_db_link_count: usize,
    pub(crate) current_source_newer_than_archive_count: usize,
    pub(crate) raw_mirror_db_link_count: usize,
    pub(crate) sole_copy_warning_count: usize,
    /// `CASS_RAW_MIRROR` turned raw capture off (GH #506): archive rows
    /// without a raw copy are then expected rather than a backfill gap.
    pub(crate) raw_mirror_capture_disabled: bool,
    pub(crate) recommended_action: String,
}

#[derive(Debug, Default)]
pub(crate) struct DoctorProviderInventoryBuilder {
    pub(crate) indexed_conversation_count: usize,
    pub(crate) missing_current_source_count: usize,
    pub(crate) unknown_mapping_count: usize,
    pub(crate) remote_source_count: usize,
    pub(crate) local_source_count: usize,
    pub(crate) detected_roots: Vec<DoctorDetectedProviderRoot>,
}

#[derive(Debug, Default)]
pub(crate) struct DoctorSourceIdentityInventoryBuilder {
    pub(crate) source_id: String,
    pub(crate) origin_kind: String,
    pub(crate) origin_host: Option<String>,
    pub(crate) is_remote: bool,
    pub(crate) providers: HashSet<String>,
    pub(crate) conversation_count: usize,
    pub(crate) missing_current_source_count: usize,
    pub(crate) unknown_mapping_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DoctorSourceAuthorityKind {
    CanonicalArchiveDb,
    VerifiedRawMirror,
    VerifiedBackupBundle,
    VerifiedCandidateArchive,
    LiveUpstreamSource,
    RemoteSyncCopy,
    DerivedLexicalIndex,
    DerivedSemanticIndex,
    SupportBundle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DoctorSourceAuthorityDecision {
    ReadOnly,
    CandidateOnly,
    Promotable,
    Refused,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DoctorSourceAuthorityPolicy {
    pub(crate) authority: DoctorSourceAuthorityKind,
    pub(crate) repairs: &'static [DoctorAssetClass],
    pub(crate) required_evidence: &'static [&'static str],
    pub(crate) refuses_when: &'static [&'static str],
    pub(crate) decision_when_valid: DoctorSourceAuthorityDecision,
    pub(crate) notes: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorSourceAuthorityMatrixEntry {
    pub(crate) authority: DoctorSourceAuthorityKind,
    pub(crate) repairs: Vec<DoctorAssetClass>,
    pub(crate) required_evidence: Vec<&'static str>,
    pub(crate) refuses_when: Vec<&'static str>,
    pub(crate) decision_when_valid: DoctorSourceAuthorityDecision,
    pub(crate) notes: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorSourceAuthorityCandidate {
    pub(crate) authority: DoctorSourceAuthorityKind,
    pub(crate) decision: DoctorSourceAuthorityDecision,
    pub(crate) reason: String,
    pub(crate) repairs: Vec<DoctorAssetClass>,
    pub(crate) coverage_delta: i64,
    pub(crate) freshness_delta_ms: Option<i64>,
    pub(crate) checksum_status: DoctorArtifactChecksumStatus,
    pub(crate) evidence: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct DoctorSourceAuthorityCoverageDelta {
    pub(crate) archive_conversation_count: usize,
    pub(crate) visible_local_source_conversation_count: usize,
    pub(crate) missing_current_source_count: usize,
    pub(crate) remote_source_count: usize,
    pub(crate) unknown_mapping_count: usize,
    pub(crate) raw_mirror_db_link_count: usize,
    pub(crate) visible_local_source_minus_archive: i64,
    pub(crate) raw_mirror_links_minus_archive: i64,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct DoctorSourceAuthorityFreshnessDelta {
    pub(crate) archive_db_modified_at_ms: Option<i64>,
    pub(crate) newest_raw_mirror_capture_at_ms: Option<i64>,
    pub(crate) newest_raw_mirror_source_mtime_ms: Option<i64>,
    pub(crate) raw_mirror_capture_minus_archive_db_ms: Option<i64>,
    pub(crate) freshness_state: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorSourceAuthorityChecksumEvidence {
    pub(crate) raw_mirror_status: String,
    pub(crate) raw_mirror_manifest_count: usize,
    pub(crate) raw_mirror_verified_blob_count: usize,
    pub(crate) raw_mirror_missing_blob_count: usize,
    pub(crate) raw_mirror_checksum_mismatch_count: usize,
    pub(crate) raw_mirror_manifest_checksum_mismatch_count: usize,
    pub(crate) raw_mirror_manifest_checksum_not_recorded_count: usize,
    pub(crate) raw_mirror_invalid_manifest_count: usize,
    pub(crate) summary_status: DoctorArtifactChecksumStatus,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorSourceAuthorityReport {
    pub(crate) schema_version: u32,
    pub(crate) decision: DoctorSourceAuthorityDecision,
    pub(crate) selected_authority: Option<DoctorSourceAuthorityKind>,
    pub(crate) selected_authorities: Vec<DoctorSourceAuthorityCandidate>,
    pub(crate) rejected_authorities: Vec<DoctorSourceAuthorityCandidate>,
    pub(crate) coverage_delta: DoctorSourceAuthorityCoverageDelta,
    pub(crate) freshness_delta: DoctorSourceAuthorityFreshnessDelta,
    pub(crate) checksum_evidence: DoctorSourceAuthorityChecksumEvidence,
    pub(crate) matrix: Vec<DoctorSourceAuthorityMatrixEntry>,
    pub(crate) notes: Vec<String>,
}

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
