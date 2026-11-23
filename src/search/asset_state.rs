//! Shared search asset state evaluation for status, health, and fail-open search planning.
//!
//! This module centralizes coarse-grained asset truth so callers stop inferring
//! lexical freshness, active maintenance, and semantic readiness from ad hoc
//! file checks spread across the CLI.
//!
//! The maintenance coordination layer (`evaluate_maintenance_coordination`,
//! `decide_maintenance_action`, `poll_maintenance_until_idle`) provides
//! single-flight semantics: foreground cass actors share one coherent truth
//! for repair/acquisition work and never duplicate basic maintenance jobs.

mod coordination;
mod lexical;
mod maintenance;
mod semantic;
mod types;

#[cfg(test)]
mod tests;

use anyhow::Result;

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) use coordination::{
    MaintenanceCoordinationOutcome, MaintenanceDecision, MaintenanceEvent, MaintenanceEventKind,
    PollResult, UnifiedMaintenanceView, YieldRequest, append_maintenance_event,
    check_yield_requested, clear_yield_signal, decide_maintenance_action,
    decide_maintenance_action_from_snapshot, decide_search_failopen,
    evaluate_maintenance_coordination, evaluate_maintenance_coordination_from_snapshot,
    poll_maintenance_until_idle, read_maintenance_events, request_yield,
    truncate_maintenance_event_log, unified_maintenance_view,
};
pub(crate) use lexical::{
    CORRUPT_MANIFEST_STATUS_REASON, LEGACY_ENGINE_STATUS_REASON,
    LEXICAL_HOLLOW_LIVE_DOC_FLOOR_PERCENT, LexicalHollowVerdict,
    completed_lexical_checkpoint_indexed_docs, lexical_generation_hollow_verdict,
    lexical_storage_fingerprints_match,
};
pub(crate) use maintenance::{
    REBUILD_STALL_DETECT_SECS_DEFAULT, SearchMaintenanceJobKind, SearchMaintenanceMode,
    SearchMaintenanceSnapshot, clear_index_run_lock_metadata_sidecar,
    index_run_lock_metadata_sidecar_path, index_run_lock_path, maintenance_stall_age_ms,
    read_search_maintenance_snapshot, read_search_maintenance_snapshot_reaping,
    rebuild_stall_detect_threshold_ms, windows_lock_conflict,
    write_index_run_lock_metadata_sidecar,
};
pub(crate) use semantic::{inspect_semantic_assets, semantic_state_from_availability};
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) use types::SemanticPreference;
pub(crate) use types::{
    InspectSearchAssetsInput, LexicalAssetState, LexicalCheckpointState, LexicalFingerprintState,
    SearchAssetSnapshot, SemanticAssetState, SemanticBacklogProgressState,
    SemanticCheckpointProgressState, SemanticTierAssetState,
};

#[cfg(test)]
use coordination::MAX_EVENT_LOG_ENTRIES;
#[cfg(test)]
use lexical::{LexicalObservationInput, lexical_state_from_observations};
#[cfg(test)]
use semantic::{
    apply_semantic_identity_invalidation, bounded_ann_presence, semantic_preference_surface,
};

use lexical::{InspectLexicalAssetsInput, inspect_lexical_assets};
use semantic::semantic_state_not_inspected;

pub(crate) fn inspect_search_assets(
    input: InspectSearchAssetsInput<'_>,
) -> Result<SearchAssetSnapshot> {
    let InspectSearchAssetsInput {
        data_dir,
        db_path,
        stale_threshold,
        last_indexed_at_ms,
        now_ms,
        maintenance,
        semantic_preference,
        db_available,
        compute_lexical_fingerprint,
        inspect_semantic,
    } = input;

    let lexical = inspect_lexical_assets(InspectLexicalAssetsInput {
        data_dir,
        db_path,
        stale_threshold,
        last_indexed_at_ms,
        now_ms,
        maintenance,
        db_available,
        compute_lexical_fingerprint,
    })?;
    let current_db_fingerprint = lexical.fingerprint.current_db_fingerprint.as_deref();
    let semantic = if inspect_semantic {
        inspect_semantic_assets(
            data_dir,
            db_path,
            semantic_preference,
            current_db_fingerprint,
            db_available,
        )
    } else {
        semantic_state_not_inspected(data_dir, semantic_preference, current_db_fingerprint)
    };

    Ok(SearchAssetSnapshot { lexical, semantic })
}
